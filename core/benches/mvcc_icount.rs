//! Fixed-iteration MVCC workloads for instruction profiling.
//!
//! Run this through `scripts/mvcc-icount-callgrind.sh` on macOS. The script
//! subtracts runs with two iteration counts so database setup is not charged to
//! each operation.

use std::num::NonZeroUsize;
use std::sync::Arc;

use turso_core::{Connection, Database, MemoryIO, SqliteDialect, Statement, StepResult, Value};

const POINT_ROWS: usize = 2_048;
const SCAN_ROWS: usize = 128;

fn main() {
    let iterations = env_usize("ICOUNT_ITERS", 100);
    let scenario = std::env::var("ICOUNT_SCENARIO").unwrap_or_else(|_| "point_read".into());
    let mut harness = Harness::new(&scenario);
    let mut observed_rows = 0usize;

    for iteration in 0..iterations {
        observed_rows += harness.run(iteration);
    }

    println!(
        "mvcc-icount: scenario={scenario} iterations={iterations} observed_rows={observed_rows}"
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
            Scenario::PointUpdateCommit => {
                let rowid = i64::try_from(iteration % POINT_ROWS + 1).unwrap();
                self.statements[0]
                    .bind_at(NonZeroUsize::new(1).unwrap(), Value::from_i64(rowid))
                    .unwrap();
            }
            Scenario::PointRead
            | Scenario::IndexRead
            | Scenario::Scan128
            | Scenario::PointUpdateRollback
            | Scenario::InsertRollback => {}
        }

        self.statements
            .iter_mut()
            .map(|statement| run_statement(&self.db, statement))
            .sum()
    }
}

#[derive(Clone, Copy)]
enum Scenario {
    PointRead,
    IndexRead,
    Scan128,
    PointUpdateRollback,
    InsertRollback,
    PointUpdateCommit,
}

impl Scenario {
    fn from_name(name: &str) -> Self {
        match name {
            "point_read" => Self::PointRead,
            "index_read" => Self::IndexRead,
            "scan_128" => Self::Scan128,
            "point_update_rollback" => Self::PointUpdateRollback,
            "insert_rollback" => Self::InsertRollback,
            "point_update_commit" => Self::PointUpdateCommit,
            other => panic!("unknown ICOUNT_SCENARIO: {other}"),
        }
    }

    fn sql(self) -> &'static [&'static str] {
        match self {
            Self::PointRead => &["SELECT payload FROM bench WHERE id = 1024"],
            Self::IndexRead => &["SELECT payload FROM bench WHERE key = 'key-01024'"],
            Self::Scan128 => &["SELECT id, key, payload FROM bench ORDER BY id"],
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
        Scenario::Scan128 => SCAN_ROWS,
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
