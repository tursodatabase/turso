//! Replays traces of the Quint specs in quint/mvcc-passive-checkpoint/ on a
//! real database. Each step of a trace runs one engine operation, and after
//! each step the test compares the engine state with the state of the trace.
//! quint/mvcc-passive-checkpoint/HARNESS.md describes the design.

mod itf;
mod rows;
mod schema;

use std::sync::Arc;

use super::*;
use crate::alloc::DynAllocator;
use crate::mvcc::clock::MvccClock;
use crate::mvcc::database::checkpoint_state_machine::{
    CheckpointState, CheckpointStateMachine, CheckpointYieldPoint,
};
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
use crate::state_machine::{StateTransition, TransitionResult};
use crate::storage::wal::CheckpointMode;
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::Mutex;
use crate::{Connection, StepResult, IO};

use itf::Step;

/// What a replay must show.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// Every check holds at every step.
    Conforms,
    /// The engine fails a check at this step, with this kind of failure.
    Violation { step: usize, kind: Failure },
}

/// The kinds of differences between the engine and the trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Failure {
    /// A read returned another value than the spec oracle.
    WrongRead,
    /// `durable_txid_max` is ordered differently from the spec value.
    Durable,
    /// The checkpoint snapshot is ordered differently from the spec value.
    Snapshot,
    /// The checkpoint collected other rows than the spec write set.
    Collect,
    /// The engine wrote a WAL frame where the spec did not, or the other way.
    Frame,
    /// The backfilled frame or `backfill_floor` differs from the spec.
    Backfill,
    /// The logical log is empty where the spec has records, or the other way.
    LogicalLog,
    /// A crash now recovers other rows than the spec.
    RecoveryRows,
    /// A crash now makes the database fail to open or to read.
    RecoveryError,
    /// A crash now makes recovery panic.
    RecoveryPanic,
    /// The schema cookie in the global header is below the shared schema.
    CookieBehind,
}

/// Why a replay stopped before the end of the trace.
#[derive(Debug)]
enum Stop {
    /// The trace splits an engine step that the test cannot split.
    Refused(String),
    Failed {
        kind: Failure,
        detail: String,
    },
}

type StepOutcome = std::result::Result<(), Stop>;

fn refuse(reason: impl Into<String>) -> StepOutcome {
    Err(Stop::Refused(reason.into()))
}

fn fail(kind: Failure, detail: impl Into<String>) -> StepOutcome {
    Err(Stop::Failed {
        kind,
        detail: detail.into(),
    })
}

/// A trace driver for one spec. `run_step` runs the action of `step` and
/// checks the state after it.
trait Driver {
    fn run_step(&mut self, index: usize, step: &Step) -> StepOutcome;
}

#[derive(Debug, PartialEq, Eq)]
enum Replay {
    Done,
    Refused,
}

/// Replays `trace` with `driver` and checks the result against `expect`.
fn replay(name: &str, trace: &str, expect: Expect, driver: &mut dyn Driver) -> Replay {
    let steps = itf::parse_trace(trace);
    let mut stop = None;
    for (index, step) in steps.iter().enumerate().skip(1) {
        if let Err(s) = driver.run_step(index, step) {
            stop = Some((index, s));
            break;
        }
    }
    match (expect, stop) {
        (Expect::Conforms, None) => Replay::Done,
        (_, Some((index, Stop::Refused(reason)))) => {
            assert_eq!(
                expect,
                Expect::Conforms,
                "{name}: a trace that must show a violation was refused at step {index}: {reason}"
            );
            println!("{name}: refused at step {index}: {reason}");
            Replay::Refused
        }
        (Expect::Conforms, Some((index, Stop::Failed { kind, detail }))) => panic!(
            "{name}: the engine differs from the spec at step {index} ({}): {kind:?}: {detail}",
            steps[index].action
        ),
        (Expect::Violation { step, kind }, None) => panic!(
            "{name}: expected {kind:?} at step {step}, but every step conforms. \
             If a fix made this pass, change the test to Expect::Conforms."
        ),
        (Expect::Violation { step, kind }, Some((index, Stop::Failed { kind: got, detail }))) => {
            assert_eq!(
                (index, got),
                (step, kind),
                "{name}: expected {kind:?} at step {step}, got {got:?} at step {index}: {detail}"
            );
            Replay::Done
        }
    }
}

/// A yield injector that yields once, at one yield point and one selection
/// key. The target is set before each engine operation that must stop. A
/// target key of None matches any key.
#[derive(Debug, Default)]
struct StopAt {
    target: Mutex<Option<(YieldPoint, Option<u64>)>>,
    fired: AtomicBool,
}

impl StopAt {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn set(&self, point: YieldPoint, selection_key: Option<u64>) {
        *self.target.lock() = Some((point, selection_key));
        self.fired.store(false, Ordering::Release);
    }

    fn clear(&self) {
        *self.target.lock() = None;
        self.fired.store(false, Ordering::Release);
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }
}

impl YieldInjector for StopAt {
    fn should_yield(&self, _instance_id: u64, selection_key: u64, point: YieldPoint) -> bool {
        let mut target = self.target.lock();
        let hit = matches!(*target, Some((p, key)) if p == point && key.is_none_or(|k| k == selection_key));
        if hit {
            *target = None;
            self.fired.store(true, Ordering::Release);
            return true;
        }
        false
    }
}

/// Opens a connection with its own `StopAt` injector.
fn connect_with_stop(db: &MvccTestDbNoConn) -> (Arc<Connection>, Arc<StopAt>) {
    let conn = db.connect();
    let stop = StopAt::new();
    conn.set_yield_injector(Some(stop.clone()));
    (conn, stop)
}

const STEP_LIMIT: usize = 100_000;

/// Steps `stmt` until `stop` fires or the statement is done. Returns true
/// when it stopped at the target.
fn step_statement(stmt: &mut crate::Statement, stop: &StopAt, io: &Arc<dyn IO>) -> bool {
    for _ in 0..STEP_LIMIT {
        match stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield => {
                if stop.fired() {
                    return true;
                }
                io.step().unwrap();
            }
            StepResult::Row => {}
            StepResult::Done => return false,
            other => panic!("unexpected statement step result {other:?}"),
        }
    }
    panic!("statement did not stop in {STEP_LIMIT} steps");
}

type Store = MvStore<MvccClock, DynAllocator>;

/// A passive checkpoint that the test steps on its own connection.
struct Checkpointer {
    conn: Arc<Connection>,
    stop: Arc<StopAt>,
    io: Arc<dyn IO>,
    mv: Arc<Store>,
    sm: Option<CheckpointStateMachine<MvccClock, DynAllocator>>,
}

impl Checkpointer {
    fn new(db: &MvccTestDbNoConn, mv: Arc<Store>) -> Self {
        let (conn, stop) = connect_with_stop(db);
        let io = conn.pager.load().io.clone();
        Self {
            conn,
            stop,
            io,
            mv,
            sm: None,
        }
    }

    /// Starts a pass and steps it to the collection of table rows. The
    /// collection yields after `collect_threshold` keys.
    fn start(&mut self, collect_threshold: usize) {
        let mut sm = CheckpointStateMachine::new(
            self.conn.pager.load().clone(),
            self.mv.clone(),
            self.conn.clone(),
            false,
            self.conn.get_sync_mode(),
            crate::MAIN_DB_ID,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        sm.set_collect_preemption_threshold_for_test(collect_threshold);
        self.sm = Some(sm);
        self.step_until(|state| state == CheckpointState::CollectTableRows);
    }

    fn sm(&self) -> &CheckpointStateMachine<MvccClock, DynAllocator> {
        self.sm.as_ref().expect("no checkpoint is running")
    }

    /// Steps to the publish window, after the pager commit. Returns false
    /// when the pass had nothing to write, because it then skips the pager
    /// commit and the publish.
    fn commit(&mut self) -> bool {
        self.stop
            .set(CheckpointYieldPoint::BeforePublishWindow.point(), None);
        self.step_until(|state| state == CheckpointState::CheckpointWal);
        let committed = self.stop.fired();
        self.stop.clear();
        committed
    }

    fn finish(&mut self) {
        self.step_until(|_| false);
        self.sm = None;
    }

    /// Steps until `done(state)` holds, until the pass stops at the target
    /// of `stop`, or until it finishes.
    fn step_until(&mut self, done: impl Fn(CheckpointState) -> bool) {
        let sm = self.sm.as_mut().expect("no checkpoint is running");
        for _ in 0..STEP_LIMIT {
            if done(sm.state_for_test()) {
                return;
            }
            match sm.step(&()).unwrap() {
                TransitionResult::Continue => {}
                TransitionResult::Io(io) => {
                    if self.stop.fired() {
                        return;
                    }
                    io.wait(self.io.as_ref()).unwrap();
                }
                TransitionResult::Done(_) => return,
            }
        }
        panic!("checkpoint did not reach the state in {STEP_LIMIT} steps");
    }
}

/// What a process crash at this moment recovers.
#[derive(Debug)]
enum Recovery<T> {
    Read(T),
    Error(String),
    Panic(String),
}

/// Copies the database file, the WAL and the logical log to a new folder,
/// as a process crash leaves them, then opens the copy and reads it with
/// `read`. The running database does not change.
fn crash_probe<T>(
    db_path: &str,
    read: impl FnOnce(&Arc<Connection>) -> crate::Result<T>,
) -> Recovery<T> {
    let source = std::path::Path::new(db_path);
    let name = source.file_name().unwrap().to_str().unwrap();
    let folder = tempfile::TempDir::new().unwrap();
    for entry in std::fs::read_dir(source.parent().unwrap()).unwrap() {
        let entry = entry.unwrap();
        let file = entry.file_name().into_string().unwrap();
        if file.starts_with(name) && !file.ends_with("-tshm") {
            std::fs::copy(entry.path(), folder.path().join(&file)).unwrap();
        }
    }
    let copy = folder.path().join(name);
    let opened = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> crate::Result<_> {
        let io = Arc::new(crate::io::PlatformIO::new()?);
        let db = crate::Database::open_file_with_flags(
            io,
            copy.to_str().unwrap(),
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true),
            None,
            Arc::new(crate::EngineDialect),
        )?;
        let copy = MvccTestDbNoConn {
            db: Some(db),
            path: None,
            opts: crate::DatabaseOpts::new(),
            enc_opts: None,
            _temp_dir: None,
        };
        read(&copy.connect())
    }));
    match opened {
        Ok(Ok(value)) => Recovery::Read(value),
        Ok(Err(err)) => Recovery::Error(err.to_string()),
        Err(payload) => Recovery::Panic(
            payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default(),
        ),
    }
}

fn query_rows(conn: &Arc<Connection>, sql: &str) -> crate::Result<Vec<Vec<crate::Value>>> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect());
        Ok(())
    })?;
    Ok(rows)
}
