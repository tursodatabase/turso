//! Fixed-iteration MVCC workloads for instruction profiling.
//!
//! Run this through `scripts/mvcc-icount-callgrind.sh` on macOS. The script
//! subtracts runs with two iteration counts so database setup is not charged to
//! each operation.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use turso_core::{
    Connection, Database, MemoryIO, SqliteDialect, Statement, StatementStatusCounter, StepResult,
    Value,
};

const POINT_ROWS: usize = 2_048;
const SCAN_ROWS: usize = 128;
const DELETE_ROWS: usize = 8_192;
const BATCH_INSERT_ROWS: usize = 32;

fn main() {
    let iterations = env_usize("ICOUNT_ITERS", 100);
    let samples = env_usize("WALLCLOCK_SAMPLES", 1);
    let scenario = std::env::var("ICOUNT_SCENARIO").unwrap_or_else(|_| "point_read".into());
    let mut observed_rows = 0usize;

    for sample in 0..samples {
        let mut harness = Harness::new(&scenario);
        let started = Instant::now();
        for iteration in 0..iterations {
            observed_rows += harness.run(iteration);
        }
        let elapsed = started.elapsed();
        println!(
            "mvcc-wallclock: sample={sample} elapsed_ns={} ns_per_operation={}",
            elapsed.as_nanos(),
            elapsed.as_nanos() / iterations as u128,
        );
        println!(
            "mvcc-workload-reprepares: sample={sample} counts={:?}",
            harness
                .statements
                .iter()
                .map(|statement| statement.stmt_status(StatementStatusCounter::Reprepare))
                .collect::<Vec<_>>()
        );
    }

    println!(
        "mvcc-workload: scenario={scenario} iterations={iterations} samples={samples} observed_rows={observed_rows}"
    );
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|error| panic!("invalid {name}={value:?}: {error}"))
        })
        .unwrap_or(default)
}

struct Harness {
    db: Arc<Database>,
    statements: Vec<Statement>,
    scenario: Scenario,
}

impl Harness {
    fn new(name: &str) -> Self {
        let scenario = Scenario::from_name(name);
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        conn.wal_auto_actions_disable();
        setup(&conn, scenario);
        if scenario.reads_checkpointed_rows() {
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        }
        let statements = scenario
            .sql()
            .iter()
            .map(|sql| conn.prepare(sql).unwrap())
            .collect();

        Self {
            db,
            statements,
            scenario,
        }
    }

    fn run(&mut self, iteration: usize) -> usize {
        match self.scenario {
            Scenario::PointUpdateCommit | Scenario::InsertCommit | Scenario::DeleteCommit => {
                let rowid = match self.scenario {
                    Scenario::PointUpdateCommit => {
                        i64::try_from(iteration % POINT_ROWS + 1).unwrap()
                    }
                    Scenario::InsertCommit => i64::try_from(iteration).unwrap() + 3_000_000,
                    Scenario::DeleteCommit => i64::try_from(iteration % DELETE_ROWS + 1).unwrap(),
                    _ => unreachable!(),
                };
                bind_rowid(&mut self.statements[0], rowid);
            }
            Scenario::BatchInsertCommit => return self.run_batch_insert(iteration),
            Scenario::PointRead
            | Scenario::IndexRead
            | Scenario::Scan128
            | Scenario::PointReadBtree
            | Scenario::IndexReadBtree
            | Scenario::Scan128Btree
            | Scenario::IndexScan128
            | Scenario::PointUpdateRollback
            | Scenario::InsertRollback => {}
        }

        self.statements
            .iter_mut()
            .map(|statement| run_statement(&self.db, statement))
            .sum()
    }

    fn run_batch_insert(&mut self, iteration: usize) -> usize {
        let [begin, insert, commit] = self.statements.as_mut_slice() else {
            unreachable!("batch insert uses BEGIN, INSERT, and COMMIT statements");
        };
        let mut rows = run_statement(&self.db, begin);
        for row in 0..BATCH_INSERT_ROWS {
            let rowid = i64::try_from(iteration * BATCH_INSERT_ROWS + row).unwrap() + 3_000_000;
            bind_rowid(insert, rowid);
            rows += run_statement(&self.db, insert);
        }
        rows + run_statement(&self.db, commit)
    }
}

fn bind_rowid(statement: &mut Statement, rowid: i64) {
    statement
        .bind_at(NonZeroUsize::new(1).unwrap(), Value::from_i64(rowid))
        .unwrap();
}

#[derive(Clone, Copy)]
enum Scenario {
    PointRead,
    IndexRead,
    Scan128,
    PointReadBtree,
    IndexReadBtree,
    Scan128Btree,
    IndexScan128,
    PointUpdateRollback,
    InsertRollback,
    PointUpdateCommit,
    InsertCommit,
    BatchInsertCommit,
    DeleteCommit,
}

impl Scenario {
    fn from_name(name: &str) -> Self {
        match name {
            "point_read" => Self::PointRead,
            "index_read" => Self::IndexRead,
            "scan_128" => Self::Scan128,
            "point_read_btree" => Self::PointReadBtree,
            "index_read_btree" => Self::IndexReadBtree,
            "scan_128_btree" => Self::Scan128Btree,
            "index_scan_128" => Self::IndexScan128,
            "point_update_rollback" => Self::PointUpdateRollback,
            "insert_rollback" => Self::InsertRollback,
            "point_update_commit" => Self::PointUpdateCommit,
            "insert_commit" => Self::InsertCommit,
            "batch_insert_commit" => Self::BatchInsertCommit,
            "delete_commit" => Self::DeleteCommit,
            other => panic!("unknown ICOUNT_SCENARIO: {other}"),
        }
    }

    fn reads_checkpointed_rows(self) -> bool {
        matches!(
            self,
            Self::PointReadBtree | Self::IndexReadBtree | Self::Scan128Btree
        )
    }

    fn sql(self) -> &'static [&'static str] {
        match self {
            Self::PointRead | Self::PointReadBtree => {
                &["SELECT payload FROM bench WHERE id = 1024"]
            }
            Self::IndexRead | Self::IndexReadBtree => {
                &["SELECT payload FROM bench WHERE key = 'key-01024'"]
            }
            Self::Scan128 | Self::Scan128Btree => {
                &["SELECT id, key, payload FROM bench ORDER BY id"]
            }
            Self::IndexScan128 => {
                &["SELECT key FROM bench WHERE key >= 'key-01000' ORDER BY key LIMIT 128"]
            }
            Self::PointUpdateRollback => &[
                "BEGIN CONCURRENT",
                "UPDATE bench SET payload = 'changed' WHERE id = 1024",
                "ROLLBACK",
            ],
            Self::InsertRollback => &[
                "BEGIN CONCURRENT",
                "INSERT INTO bench VALUES (3000000, 'temporary', 'temporary')",
                "ROLLBACK",
            ],
            Self::PointUpdateCommit => &["UPDATE bench SET payload = 'changed' WHERE id = ?1"],
            Self::InsertCommit => {
                &["INSERT INTO bench VALUES (?1, printf('insert-%d', ?1), 'inserted payload')"]
            }
            Self::BatchInsertCommit => &[
                "BEGIN CONCURRENT",
                "INSERT INTO bench VALUES (?1, printf('insert-%d', ?1), 'inserted payload')",
                "COMMIT",
            ],
            Self::DeleteCommit => &["DELETE FROM bench WHERE id = ?1"],
        }
    }
}

fn setup(conn: &Arc<Connection>, scenario: Scenario) {
    conn.execute(
        "CREATE TABLE bench(\
            id INTEGER PRIMARY KEY, \
            key TEXT NOT NULL UNIQUE, \
            payload TEXT NOT NULL\
        )",
    )
    .unwrap();
    let rows = match scenario {
        Scenario::Scan128 | Scenario::Scan128Btree => SCAN_ROWS,
        Scenario::DeleteCommit => DELETE_ROWS,
        _ => POINT_ROWS,
    };
    conn.execute(format!(
        "WITH RECURSIVE generate(i) AS (\
            VALUES(1) UNION ALL SELECT i + 1 FROM generate WHERE i < {rows}\
        ) \
        INSERT INTO bench \
        SELECT i, printf('key-%05d', i), printf('payload-%05d', i) FROM generate"
    ))
    .unwrap();
}

fn run_statement(db: &Arc<Database>, statement: &mut Statement) -> usize {
    let mut rows = 0;
    loop {
        match statement.step().unwrap() {
            StepResult::Row => rows += 1,
            StepResult::Done => break,
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                db.io.step().unwrap();
            }
            StepResult::Busy => panic!("MVCC instruction workload returned Busy"),
            StepResult::Interrupt => panic!("MVCC instruction workload was interrupted"),
        }
    }
    statement.reset().unwrap();
    rows
}
