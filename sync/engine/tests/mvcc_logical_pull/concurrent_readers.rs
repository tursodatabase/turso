//! What reads on other connections return while pulls and checkpoints run. Pulls run without the
//! SDK's SyncBusyGate unless a test says so. The SDK takes the gate for every checkpoint, so each
//! checkpoint here waits for the other connections' statements to finish and blocks new ones until
//! it ends (`SyncBusyGate` below).
//!
//! Rows are only ever inserted, locally and on the remote:
//! - a read outside a transaction must contain every row an earlier such read saw;
//! - every read inside one transaction must return the same rows;
//! - at the end, every local and remote row is there, and every local row is still waiting
//!   to be pushed.
//!
//! Busy is allowed for the clients: turso_core returns it when another connection holds a lock,
//! and every client here retries on it. A pull waits for the locks it needs (up to the sync busy
//! timeout), so it is not retried. A checkpoint has no other connection running, so it must not
//! return Busy. Each client sleeps 1 ms between operations, so pulls and checkpoints can acquire
//! their locks. Without the sleep, a client that is always in a transaction makes every pull fail
//! with Busy, as it would any other writer.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use turso_core::{Connection, LimboError, Value};
use turso_sync_engine::errors::Error;
use turso_sync_engine::Result;

use crate::harness::{database_opts, InProcessServer, Remote, Replica};

const ROUNDS: usize = 50;
const PAUSE: Duration = Duration::from_millis(1);
/// Every pull replays all unpushed local rows, so more rows make each pull slower.
const LOCAL_ROWS: usize = 500;

#[test]
fn readers_during_logical_pull() -> Result<()> {
    let remote = remote()?;
    let replica = remote.bootstrap_replica()?;
    run(&remote, &replica, false, false)
}

#[test]
fn readers_during_logical_pull_and_checkpoint() -> Result<()> {
    let remote = remote()?;
    let replica = remote.bootstrap_replica()?;
    run(&remote, &replica, true, false)
}

#[test]
fn readers_during_wal_page_pull() -> Result<()> {
    wal_page_pull(false)
}

#[test]
fn readers_during_wal_page_pull_and_checkpoint() -> Result<()> {
    wal_page_pull(true)
}

/// A page apply to an MVCC database commits the remote pages and replays local changes in a
/// later commit, so the SDK holds its SyncBusyGate while it runs
/// (`DatabaseSyncEngine::apply_needs_sync_busy_gate`).
#[test]
fn readers_with_sync_busy_gate_during_mvcc_page_pull() -> Result<()> {
    page_pull(remote()?, "mvcc", false, true)
}

#[test]
#[ignore = "without the SyncBusyGate, every pull succeeds but no remote row is in the table after it, or a plain INSERT keeps failing with SchemaUpdated"]
fn readers_during_mvcc_page_pull() -> Result<()> {
    page_pull(remote()?, "mvcc", false, false)
}

fn wal_page_pull(checkpoint: bool) -> Result<()> {
    let remote = Remote::new_wal(&[TABLE])?;
    page_pull(remote, "wal", checkpoint, false)
}

fn page_pull(
    remote: Arc<Remote>,
    journal_mode: &str,
    checkpoint: bool,
    gate_pulls: bool,
) -> Result<()> {
    let dir = Arc::new(tempfile::tempdir()?);
    let replica = Replica::open(
        &dir,
        InProcessServer::pages_only(remote.clone()),
        database_opts(),
    )
    .map_err(|e| step_error("open", 0, e))?;
    let mode = replica
        .connect()?
        .prepare("PRAGMA journal_mode")?
        .run_collect_rows()?;
    match &mode[0][0] {
        Value::Text(mode) => assert_eq!(mode.as_str(), journal_mode),
        other => panic!("unexpected journal mode {other:?}"),
    }
    run(&remote, &replica, checkpoint, gate_pulls)
}

// No index: every pull replays all unpushed local rows, and replaying them into an MVCC index
// gets slower with each row (tursodatabase/turso#11871).
const TABLE: &str = "CREATE TABLE t(x TEXT)";

fn remote() -> Result<Arc<Remote>> {
    Remote::new(&[TABLE])
}

/// This test's replacement for the SDK's SyncBusyGate. Statements on the other connections hold
/// it for reading; a checkpoint, and a pull when the test gates pulls, holds it for writing.
type SyncBusyGate = Arc<RwLock<()>>;

#[derive(Default)]
struct Flags {
    stop: AtomicBool,
    /// Set while a pull or checkpoint runs.
    pull_or_checkpoint_running: AtomicBool,
}

impl Flags {
    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    fn pull_or_checkpoint_running(&self) -> bool {
        self.pull_or_checkpoint_running.load(Ordering::SeqCst)
    }

    fn run_pull_or_checkpoint(&self, operation: impl FnOnce() -> Result<()>) -> Result<()> {
        self.pull_or_checkpoint_running
            .store(true, Ordering::SeqCst);
        let result = operation();
        self.pull_or_checkpoint_running
            .store(false, Ordering::SeqCst);
        result
    }
}

fn run(remote: &Remote, replica: &Replica, checkpoint: bool, gate_pulls: bool) -> Result<()> {
    let flags = Arc::new(Flags::default());
    let sync_busy_gate = SyncBusyGate::default();
    let writer = spawn_writer(replica.connect()?, flags.clone(), sync_busy_gate.clone());
    let autocommit =
        spawn_autocommit_reader(replica.connect()?, flags.clone(), sync_busy_gate.clone());
    let in_txn =
        spawn_transaction_reader(replica.connect()?, flags.clone(), sync_busy_gate.clone());

    let mut checkpoints = 0;
    for round in 0..ROUNDS {
        remote.execute_transaction(&[&format!("INSERT INTO t VALUES ('remote-{round}')")])?;
        flags
            .run_pull_or_checkpoint(|| {
                let _sync_busy_gate = gate_pulls.then(|| sync_busy_gate.write().unwrap());
                replica.pull()
            })
            .map_err(|e| step_error("pull", round, e))?;
        if checkpoint {
            // Waiting for the gate is outside the flag: nothing else runs while the checkpoint does.
            let _sync_busy_gate = sync_busy_gate.write().unwrap();
            flags
                .run_pull_or_checkpoint(|| replica.checkpoint())
                .map_err(|e| step_error("checkpoint", round, e))?;
            checkpoints += 1;
        }
    }
    flags.stop.store(true, Ordering::Relaxed);

    let writer = writer.join().unwrap()?;
    let autocommit = autocommit.join().unwrap();
    let in_txn = in_txn.join().unwrap();
    let report = format!(
        "checkpoints: {checkpoints}\nwriter: {writer:#?}\nautocommit reader: {autocommit:#?}\ntransaction reader: {in_txn:#?}"
    );
    if checkpoint {
        assert_eq!(checkpoints, ROUNDS, "{report}");
    }
    let final_rows = read(&replica.connect()?)?;
    assert_eq!(
        final_rows.len(),
        writer.inserted + ROUNDS,
        "{final_rows:?}\n{report}"
    );
    // Nothing is pushed, so every local insert must still be waiting to be pushed.
    assert_eq!(
        replica.pending_local_changes()?,
        writer.inserted as i64,
        "local changes waiting to be pushed"
    );
    assert!(
        autocommit.anomalies.is_empty() && in_txn.anomalies.is_empty(),
        "{report}"
    );
    assert!(autocommit.reads > 0 && in_txn.reads > 0, "{report}");
    if !gate_pulls {
        // Each client must have finished work while a pull or checkpoint was running,
        // or the test did not check what happens during one.
        assert!(
            writer.during_pull_or_checkpoint > 0
                && autocommit.during_pull_or_checkpoint > 0
                && in_txn.during_pull_or_checkpoint > 0,
            "{report}"
        );
    }
    Ok(())
}

#[derive(Debug, Default)]
struct WriterReport {
    inserted: usize,
    /// Inserts that started and committed while a pull or checkpoint was running.
    during_pull_or_checkpoint: usize,
}

#[derive(Debug, Default)]
struct ReaderReport {
    reads: usize,
    /// Reads that started and finished while a pull or checkpoint was running.
    during_pull_or_checkpoint: usize,
    busy: usize,
    /// Each kind of anomaly, with how many times it happened.
    anomalies: BTreeMap<String, usize>,
}

impl ReaderReport {
    fn anomaly(&mut self, description: String) {
        *self.anomalies.entry(description).or_default() += 1;
    }
}

/// Inserts up to [LOCAL_ROWS] local rows, or until stopped.
fn spawn_writer(
    conn: Arc<Connection>,
    flags: Arc<Flags>,
    sync_busy_gate: SyncBusyGate,
) -> thread::JoinHandle<Result<WriterReport>> {
    thread::spawn(move || {
        let mut report = WriterReport::default();
        while !flags.stopped() && report.inserted < LOCAL_ROWS {
            let inserted = report.inserted;
            retry_on_busy(BUSY_LIMIT, || {
                let _sync_busy_gate = sync_busy_gate.read().unwrap();
                let started_during_pull_or_checkpoint = flags.pull_or_checkpoint_running();
                conn.execute(format!("INSERT INTO t VALUES ('local-{inserted}')"))?;
                if started_during_pull_or_checkpoint && flags.pull_or_checkpoint_running() {
                    report.during_pull_or_checkpoint += 1;
                }
                Ok(())
            })
            .map_err(|e| step_error("local insert", inserted, e))?;
            report.inserted += 1;
            thread::sleep(PAUSE);
        }
        Ok(report)
    })
}

fn spawn_autocommit_reader(
    conn: Arc<Connection>,
    flags: Arc<Flags>,
    sync_busy_gate: SyncBusyGate,
) -> thread::JoinHandle<ReaderReport> {
    thread::spawn(move || {
        let mut report = ReaderReport::default();
        let mut seen = BTreeSet::new();
        while !flags.stopped() {
            let started_during_pull_or_checkpoint = flags.pull_or_checkpoint_running();
            let result = {
                let _sync_busy_gate = sync_busy_gate.read().unwrap();
                read(&conn)
            };
            match result {
                Ok(rows) => {
                    report.reads += 1;
                    if started_during_pull_or_checkpoint && flags.pull_or_checkpoint_running() {
                        report.during_pull_or_checkpoint += 1;
                    }
                    let missing: Vec<_> = seen.difference(&rows).cloned().collect();
                    if !missing.is_empty() {
                        report.anomaly(format!(
                            "rows disappeared: {missing:?} (saw {} rows, had {})",
                            rows.len(),
                            seen.len()
                        ));
                    }
                    seen = rows;
                }
                Err(error) if is_busy(&error) => report.busy += 1,
                Err(error) => report.anomaly(format!("read failed: {error}")),
            }
            thread::sleep(PAUSE);
        }
        report
    })
}

fn spawn_transaction_reader(
    conn: Arc<Connection>,
    flags: Arc<Flags>,
    sync_busy_gate: SyncBusyGate,
) -> thread::JoinHandle<ReaderReport> {
    thread::spawn(move || {
        let mut report = ReaderReport::default();
        while !flags.stopped() {
            let sync_busy_gate_guard = sync_busy_gate.read().unwrap();
            match conn.execute("BEGIN") {
                Ok(()) => {}
                Err(LimboError::Busy) => {
                    report.busy += 1;
                    drop(sync_busy_gate_guard);
                    thread::sleep(PAUSE);
                    continue;
                }
                Err(error) => {
                    report.anomaly(format!("BEGIN failed: {error}"));
                    break;
                }
            }
            let mut snapshot = None;
            for _ in 0..20 {
                let started_during_pull_or_checkpoint = flags.pull_or_checkpoint_running();
                match read(&conn) {
                    Ok(rows) => {
                        report.reads += 1;
                        if started_during_pull_or_checkpoint && flags.pull_or_checkpoint_running() {
                            report.during_pull_or_checkpoint += 1;
                        }
                        match &snapshot {
                            None => snapshot = Some(rows),
                            Some(first) if *first == rows => {}
                            Some(first) => report.anomaly(format!(
                                "rows changed inside a transaction: {} rows, then {}",
                                first.len(),
                                rows.len()
                            )),
                        }
                    }
                    Err(error) if is_busy(&error) => report.busy += 1,
                    Err(error) => report.anomaly(format!("read in a transaction failed: {error}")),
                }
            }
            if let Err(error) = conn.execute("COMMIT") {
                report.anomaly(format!("COMMIT failed: {error}"));
                break;
            }
            drop(sync_busy_gate_guard);
            thread::sleep(PAUSE);
        }
        report
    })
}

const BUSY_LIMIT: Duration = Duration::from_secs(10);

fn retry_on_busy(limit: Duration, mut operation: impl FnMut() -> Result<()>) -> Result<()> {
    let started = Instant::now();
    loop {
        match operation() {
            Err(error) if is_busy(&error) && started.elapsed() < limit => {
                thread::sleep(Duration::from_millis(1));
            }
            result => return result,
        }
    }
}

fn step_error(step: &str, round: usize, error: Error) -> Error {
    Error::DatabaseSyncEngineError(format!("{step} in round {round}: {error:?}"))
}

fn is_busy(error: &Error) -> bool {
    match error {
        Error::TursoError(LimboError::Busy) => true,
        other => other.to_string().contains("Database is busy"),
    }
}

fn read(conn: &Arc<Connection>) -> Result<BTreeSet<String>> {
    let rows = conn.prepare("SELECT x FROM t")?.run_collect_rows()?;
    Ok(rows
        .into_iter()
        .map(|row| match &row[0] {
            Value::Text(text) => text.as_str().to_string(),
            other => panic!("unexpected value {other:?}"),
        })
        .collect())
}
