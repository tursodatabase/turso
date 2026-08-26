use std::num::NonZero;
use std::sync::Arc;
use std::time::Instant;

use turso_core::{Connection, LimboError, Statement, StepResult, Value};

use crate::spec::{BenchError, INSERT_SQL, PAYLOAD};

/// Global worker id. Cooperative uses `0..N`. Threads use `thread * workers_per_thread + local`.
#[derive(Clone, Copy)]
pub(crate) struct WorkerId(pub u32);

/// Interleaved unique PKs: `id = local * concurrency + worker_index`.
pub(crate) struct PkCursor {
    next_local: i64,
    worker_index: i64,
    concurrency: i64,
}

impl PkCursor {
    fn for_worker(id: WorkerId, concurrency: usize) -> Self {
        Self {
            next_local: 0,
            worker_index: i64::from(id.0),
            concurrency: concurrency as i64,
        }
    }

    fn next_pk(&mut self) -> i64 {
        let pk = self.next_local * self.concurrency + self.worker_index;
        self.next_local += 1;
        pk
    }
}

/// SQL lifecycle of one in-flight Turso transaction. Park is not a phase.
/// `Statement` stays suspended across `io.step()`. Driving the same phase again
/// is the resume.
#[derive(Clone, Copy)]
pub(crate) enum Phase {
    Begin,
    Insert { left: usize },
    Commit,
    Rollback { then: AfterRollback },
}

#[derive(Clone, Copy)]
pub(crate) enum AfterRollback {
    RetryBegin,
}

pub(crate) enum StepOut {
    /// More CPU work this sweep (phase advanced, or INSERT row finished).
    Ready,
    /// `StepResult::IO` / `Yield` / `Sleep`. Wait for the shared `io.step()`.
    Parked,
}

/// One connection, one in-flight tx. Reused across commits. Re-prepare on
/// `SchemaUpdated`. Never share a connection across workers.
pub(crate) struct Worker {
    conn: Arc<Connection>,
    begin: Statement,
    insert: Statement,
    commit: Statement,
    rollback: Statement,
    pub(crate) phase: Phase,
    pks: PkCursor,
    batch_size: usize,
    /// Committed rows. Bumped only after COMMIT Done, not per INSERT.
    pub(crate) inserts_ok: u64,
    pub(crate) txns_ok: u64,
    pub(crate) busy: u64,
    pub(crate) busy_snapshots: u64,
    pub(crate) schema_updated: u64,
    /// Rows committed in the current tx, so BusySnapshot can uncount them.
    tx_inserts: u64,
    txn_started: Option<Instant>,
    pub(crate) latencies_ns: Vec<u64>,
}

impl Worker {
    pub(crate) fn new(
        id: WorkerId,
        conn: Arc<Connection>,
        batch_size: usize,
        concurrency: usize,
        busy_timeout: std::time::Duration,
    ) -> Result<Self, BenchError> {
        conn.set_busy_timeout(busy_timeout);
        conn.execute("PRAGMA synchronous = FULL")?;
        let begin = conn.prepare("BEGIN CONCURRENT")?;
        let insert = conn.prepare(INSERT_SQL)?;
        let commit = conn.prepare("COMMIT")?;
        let rollback = conn.prepare("ROLLBACK")?;
        Ok(Self {
            conn,
            begin,
            insert,
            commit,
            rollback,
            phase: Phase::Begin,
            pks: PkCursor::for_worker(id, concurrency),
            batch_size,
            inserts_ok: 0,
            txns_ok: 0,
            busy: 0,
            busy_snapshots: 0,
            schema_updated: 0,
            tx_inserts: 0,
            txn_started: None,
            latencies_ns: Vec::new(),
        })
    }

    pub(crate) fn reset_counters(&mut self) {
        self.inserts_ok = 0;
        self.txns_ok = 0;
        self.busy = 0;
        self.busy_snapshots = 0;
        self.schema_updated = 0;
        self.tx_inserts = 0;
        self.txn_started = None;
        self.latencies_ns.clear();
    }

    fn bind_insert(&mut self) -> Result<(), BenchError> {
        let pk = self.pks.next_pk();
        self.insert
            .bind_at(NonZero::new(1).unwrap(), Value::from_i64(pk))?;
        self.insert
            .bind_at(NonZero::new(2).unwrap(), Value::build_text(PAYLOAD))?;
        Ok(())
    }

    fn reset_current(&mut self) {
        let stmt = match self.phase {
            Phase::Begin => &mut self.begin,
            Phase::Insert { .. } => &mut self.insert,
            Phase::Commit => &mut self.commit,
            Phase::Rollback { .. } => &mut self.rollback,
        };
        stmt.reset_best_effort();
    }

    fn reprepare(&mut self) -> Result<(), BenchError> {
        self.begin = self.conn.prepare("BEGIN CONCURRENT")?;
        self.insert = self.conn.prepare(INSERT_SQL)?;
        self.commit = self.conn.prepare("COMMIT")?;
        self.rollback = self.conn.prepare("ROLLBACK")?;
        Ok(())
    }

    fn on_busy_snapshot(&mut self) -> Result<StepOut, BenchError> {
        self.busy_snapshots += 1;
        self.tx_inserts = 0;
        self.txn_started = None;
        self.reset_current();
        self.phase = Phase::Rollback {
            then: AfterRollback::RetryBegin,
        };
        Ok(StepOut::Ready)
    }

    fn on_schema_updated(&mut self) -> Result<StepOut, BenchError> {
        self.schema_updated += 1;
        self.tx_inserts = 0;
        self.txn_started = None;
        self.reset_current();
        self.reprepare()?;
        self.phase = Phase::Rollback {
            then: AfterRollback::RetryBegin,
        };
        Ok(StepOut::Ready)
    }

    pub(crate) fn drive(&mut self) -> Result<StepOut, BenchError> {
        if matches!(self.phase, Phase::Begin) && self.txn_started.is_none() {
            self.txn_started = Some(Instant::now());
        }
        let stepped = match self.phase {
            Phase::Begin => self.begin.step(),
            Phase::Insert { .. } => self.insert.step(),
            Phase::Commit => self.commit.step(),
            Phase::Rollback { .. } => self.rollback.step(),
        };
        match stepped {
            Ok(StepResult::Done) => match self.phase {
                Phase::Begin => {
                    self.begin.reset()?;
                    self.tx_inserts = 0;
                    self.phase = Phase::Insert {
                        left: self.batch_size,
                    };
                    self.bind_insert()?;
                    Ok(StepOut::Ready)
                }
                Phase::Insert { left } => {
                    self.tx_inserts += 1;
                    let left = left - 1;
                    self.insert.reset()?;
                    if left == 0 {
                        self.phase = Phase::Commit;
                    } else {
                        self.phase = Phase::Insert { left };
                        self.bind_insert()?;
                    }
                    Ok(StepOut::Ready)
                }
                Phase::Commit => {
                    self.inserts_ok += self.tx_inserts;
                    self.txns_ok += 1;
                    self.tx_inserts = 0;
                    if let Some(t0) = self.txn_started.take() {
                        self.latencies_ns.push(t0.elapsed().as_nanos() as u64);
                    }
                    self.commit.reset()?;
                    self.phase = Phase::Begin;
                    Ok(StepOut::Ready)
                }
                Phase::Rollback {
                    then: AfterRollback::RetryBegin,
                } => {
                    self.txn_started = None;
                    self.rollback.reset()?;
                    self.phase = Phase::Begin;
                    Ok(StepOut::Ready)
                }
            },
            Ok(StepResult::IO | StepResult::Yield | StepResult::Sleep { .. }) => {
                Ok(StepOut::Parked)
            }
            Ok(StepResult::Row) => Ok(StepOut::Ready),
            Ok(StepResult::Busy) => {
                self.busy += 1;
                Ok(StepOut::Ready)
            }
            Ok(StepResult::Interrupt) => Err(BenchError::engine("statement interrupted")),
            Err(LimboError::BusySnapshot) => self.on_busy_snapshot(),
            Err(LimboError::SchemaUpdated) => self.on_schema_updated(),
            Err(LimboError::Busy) => {
                self.busy += 1;
                Ok(StepOut::Ready)
            }
            Err(err) => Err(err.into()),
        }
    }
}
