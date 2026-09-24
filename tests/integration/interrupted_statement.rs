use std::sync::Arc;

use crate::common::{limbo_exec_rows, limbo_exec_rows_fallible, sqlite_exec_rows, TempDatabase};
use turso_core::vdbe::StepResult;

const ROW_COUNT: i64 = 2000;
const INTERRUPT_AFTER_OPS: u64 = 100;

#[derive(Debug, PartialEq)]
struct Outcome {
    interrupted: bool,
    autocommit: bool,
    rows: Vec<Vec<rusqlite::types::Value>>,
}

struct Case {
    begin: bool,
    sql: &'static str,
}

const CASES: [Case; 6] = [
    Case {
        begin: true,
        sql: "DELETE FROM t WHERE id < 1500",
    },
    Case {
        begin: true,
        sql: "UPDATE t SET v = -1 WHERE id < 1500",
    },
    Case {
        begin: true,
        sql: "INSERT INTO t SELECT id + 5000, v FROM t",
    },
    Case {
        begin: true,
        sql: "SELECT count(*) FROM t a, t b",
    },
    Case {
        begin: false,
        sql: "DELETE FROM t WHERE id < 1500",
    },
    Case {
        begin: false,
        sql: "UPDATE t SET v = -1 WHERE id < 1500",
    },
];

#[turso_macros::test(mvcc)]
fn interrupted_statement_ends_transaction_like_sqlite(tmp_db: TempDatabase) {
    let turso = tmp_db.connect_limbo();
    turso
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    turso
        .execute(format!(
            "INSERT INTO t SELECT value, value FROM generate_series(1, {ROW_COUNT})"
        ))
        .unwrap();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite
        .execute_batch(&format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
             WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {ROW_COUNT})
             INSERT INTO t SELECT x, x FROM c;"
        ))
        .unwrap();

    let mut differences = Vec::new();
    for case in CASES {
        let expected = sqlite_outcome(&sqlite, &case);
        let actual = turso_outcome(&turso, &case);
        assert!(
            expected.interrupted,
            "SQLite did not interrupt {}",
            case.sql
        );
        if actual != expected {
            differences.push(format!(
                "begin={} sql={}\n  turso:  {actual:?}\n  sqlite: {expected:?}",
                case.begin, case.sql
            ));
        }
    }
    assert!(
        differences.is_empty(),
        "Turso differs from SQLite:\n{}",
        differences.join("\n")
    );
}

const ROWS_QUERY: &str = "SELECT count(*), sum(v) FROM t";
const EARLIER_WRITE: &str = "INSERT INTO t VALUES (100000, 7)";

fn turso_outcome(conn: &Arc<turso_core::Connection>, case: &Case) -> Outcome {
    if case.begin {
        conn.execute("BEGIN").unwrap();
        conn.execute(EARLIER_WRITE).unwrap();
    }
    let interrupted = turso_step_with_interrupt(conn, case.sql);
    let outcome = Outcome {
        interrupted,
        autocommit: conn.get_auto_commit(),
        rows: limbo_exec_rows(conn, ROWS_QUERY),
    };
    if !outcome.autocommit {
        conn.execute("ROLLBACK").unwrap();
    }
    outcome
}

fn sqlite_outcome(conn: &rusqlite::Connection, case: &Case) -> Outcome {
    if case.begin {
        conn.execute_batch("BEGIN").unwrap();
        conn.execute_batch(EARLIER_WRITE).unwrap();
    }
    let interrupted = sqlite_step_with_interrupt(conn, case.sql);
    let outcome = Outcome {
        interrupted,
        autocommit: conn.is_autocommit(),
        rows: sqlite_exec_rows(conn, ROWS_QUERY),
    };
    if !outcome.autocommit {
        conn.execute_batch("ROLLBACK").unwrap();
    }
    outcome
}

struct Scenario {
    name: &'static str,
    setup: &'static [&'static str],
    before: &'static [&'static str],
    interrupted: &'static str,
    after: &'static [&'static str],
}

const SCENARIOS: [Scenario; 6] = [
    Scenario {
        name: "savepoint inside BEGIN",
        setup: &[],
        before: &[
            "BEGIN",
            "INSERT INTO t VALUES (100000, 7)",
            "SAVEPOINT s",
            "INSERT INTO t VALUES (100001, 7)",
        ],
        interrupted: "DELETE FROM t WHERE id < 1500",
        after: &[
            "ROLLBACK TO s",
            "RELEASE s",
            "SAVEPOINT a",
            "INSERT INTO t VALUES (100002, 7)",
            "RELEASE a",
            ROWS_QUERY,
        ],
    },
    Scenario {
        name: "savepoint without BEGIN",
        setup: &[],
        before: &["SAVEPOINT s", "INSERT INTO t VALUES (100003, 7)"],
        interrupted: "UPDATE t SET v = -1 WHERE id < 1500",
        after: &[
            "RELEASE s",
            "SAVEPOINT s",
            "INSERT INTO t VALUES (100004, 7)",
            "ROLLBACK TO s",
            "RELEASE s",
            ROWS_QUERY,
        ],
    },
    Scenario {
        name: "savepoint, then writes to a temp table",
        setup: &["CREATE TEMP TABLE tw(x)"],
        before: &["BEGIN", "SAVEPOINT s", "INSERT INTO tw VALUES (1)"],
        interrupted: "DELETE FROM t WHERE id < 1500",
        after: &[
            "INSERT INTO tw VALUES (2)",
            "SAVEPOINT s",
            "INSERT INTO tw VALUES (3)",
            "ROLLBACK TO s",
            "RELEASE s",
            "SELECT x FROM tw",
            "DROP TABLE tw",
        ],
    },
    Scenario {
        name: "temp table created in the transaction",
        setup: &[],
        before: &[
            "BEGIN",
            "CREATE TEMP TABLE tt(x)",
            "INSERT INTO tt VALUES (1)",
        ],
        interrupted: "DELETE FROM t WHERE id < 1500",
        after: &[
            "SELECT name FROM temp.sqlite_master",
            "CREATE TEMP TABLE tt(x)",
            "SELECT count(*) FROM tt",
            "DROP TABLE tt",
        ],
    },
    Scenario {
        name: "AUTOINCREMENT",
        setup: &[
            "CREATE TABLE a(id INTEGER PRIMARY KEY AUTOINCREMENT, v INTEGER)",
            "INSERT INTO a(v) VALUES (1), (2), (3)",
        ],
        before: &["BEGIN", "INSERT INTO a(v) VALUES (4)"],
        interrupted: "INSERT INTO a(v) SELECT v FROM t",
        after: &[
            "SELECT seq FROM sqlite_sequence WHERE name = 'a'",
            "INSERT INTO a(v) VALUES (5)",
            "SELECT id, v FROM a ORDER BY id",
        ],
    },
    Scenario {
        name: "deferred foreign key",
        setup: &[
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
            "CREATE TABLE child(p INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
        ],
        before: &["BEGIN", "INSERT INTO child VALUES (1)"],
        interrupted: "DELETE FROM t WHERE id < 1500",
        after: &[
            "BEGIN",
            "INSERT INTO parent VALUES (5)",
            "COMMIT",
            "SELECT id FROM parent",
            "SELECT p FROM child",
        ],
    },
];

const DURABLE_STATE_QUERIES: [&str; 5] = [
    ROWS_QUERY,
    "SELECT id, v FROM a ORDER BY id",
    "SELECT name, seq FROM sqlite_sequence",
    "SELECT id FROM parent",
    "SELECT p FROM child",
];

#[derive(Debug, PartialEq)]
struct ScenarioOutcome {
    interrupted: bool,
    autocommit_after_interrupt: bool,
    after: Vec<Result<Vec<Vec<rusqlite::types::Value>>, ()>>,
    autocommit_at_end: bool,
}

#[turso_macros::test(mvcc)]
fn interrupted_statement_leaves_connection_like_sqlite(tmp_db: TempDatabase) {
    let turso = tmp_db.connect_limbo();
    turso
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    turso
        .execute(format!(
            "INSERT INTO t SELECT value, value FROM generate_series(1, {ROW_COUNT})"
        ))
        .unwrap();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    sqlite
        .execute_batch(&format!(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
             WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {ROW_COUNT})
             INSERT INTO t SELECT x, x FROM c;"
        ))
        .unwrap();

    let mut differences = Vec::new();
    for scenario in SCENARIOS {
        let expected = sqlite_scenario_outcome(&sqlite, &scenario);
        let actual = turso_scenario_outcome(&tmp_db, &turso, &scenario);
        assert!(
            expected.interrupted,
            "SQLite did not interrupt {}",
            scenario.name
        );
        if actual != expected {
            differences.push(format!(
                "{}\n  turso:  {actual:?}\n  sqlite: {expected:?}",
                scenario.name
            ));
        }
    }
    assert!(
        differences.is_empty(),
        "Turso differs from SQLite:\n{}",
        differences.join("\n")
    );

    let expected_durable_state: Vec<_> = DURABLE_STATE_QUERIES
        .iter()
        .map(|query| sqlite_exec_rows(&sqlite, query))
        .collect();
    assert_eq!(integrity_check(&turso), "ok");
    turso.close().unwrap();
    let path = tmp_db.path.clone();
    let opts = tmp_db.db_opts;
    drop(tmp_db);

    let reopened = TempDatabase::new_with_existent_with_opts(&path, opts);
    let conn = reopened.connect_limbo();
    let durable_state: Vec<_> = DURABLE_STATE_QUERIES
        .iter()
        .map(|query| limbo_exec_rows(&conn, query))
        .collect();
    assert_eq!(durable_state, expected_durable_state);
    assert_eq!(integrity_check(&conn), "ok");
}

fn turso_scenario_outcome(
    tmp_db: &TempDatabase,
    conn: &Arc<turso_core::Connection>,
    scenario: &Scenario,
) -> ScenarioOutcome {
    for sql in scenario.setup.iter().chain(scenario.before) {
        conn.execute(sql).unwrap();
    }
    let interrupted = turso_step_with_interrupt(conn, scenario.interrupted);
    let autocommit_after_interrupt = conn.get_auto_commit();
    let after = scenario
        .after
        .iter()
        .map(|sql| limbo_exec_rows_fallible(tmp_db, conn, sql).map_err(|_| ()))
        .collect();
    let outcome = ScenarioOutcome {
        interrupted,
        autocommit_after_interrupt,
        after,
        autocommit_at_end: conn.get_auto_commit(),
    };
    if !outcome.autocommit_at_end {
        conn.execute("ROLLBACK").unwrap();
    }
    outcome
}

fn sqlite_scenario_outcome(conn: &rusqlite::Connection, scenario: &Scenario) -> ScenarioOutcome {
    for sql in scenario.setup.iter().chain(scenario.before) {
        conn.execute_batch(sql).unwrap();
    }
    let interrupted = sqlite_step_with_interrupt(conn, scenario.interrupted);
    let autocommit_after_interrupt = conn.is_autocommit();
    let after = scenario
        .after
        .iter()
        .map(|sql| sqlite_try_exec_rows(conn, sql))
        .collect();
    let outcome = ScenarioOutcome {
        interrupted,
        autocommit_after_interrupt,
        after,
        autocommit_at_end: conn.is_autocommit(),
    };
    if !outcome.autocommit_at_end {
        conn.execute_batch("ROLLBACK").unwrap();
    }
    outcome
}

fn integrity_check(conn: &Arc<turso_core::Connection>) -> String {
    let rows = limbo_exec_rows(conn, "PRAGMA integrity_check");
    match &rows[..] {
        [row] => match &row[..] {
            [rusqlite::types::Value::Text(result)] => result.clone(),
            _ => panic!("unexpected integrity_check row {row:?}"),
        },
        _ => panic!("unexpected integrity_check rows {rows:?}"),
    }
}

fn sqlite_try_exec_rows(
    conn: &rusqlite::Connection,
    sql: &str,
) -> Result<Vec<Vec<rusqlite::types::Value>>, ()> {
    let mut stmt = conn.prepare(sql).map_err(|_| ())?;
    let column_count = stmt.column_count();
    let mut rows = stmt.raw_query();
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(|_| ())? {
        result.push(
            (0..column_count)
                .map(|i| row.get::<_, rusqlite::types::Value>(i).unwrap())
                .collect(),
        );
    }
    Ok(result)
}

fn turso_step_with_interrupt(conn: &Arc<turso_core::Connection>, sql: &str) -> bool {
    conn.set_progress_handler(INTERRUPT_AFTER_OPS, Some(Box::new(|| true)));
    let mut stmt = conn.prepare(sql).unwrap();
    let interrupted = loop {
        match stmt.step().unwrap() {
            StepResult::IO => stmt._io().step().unwrap(),
            StepResult::Row | StepResult::Yield => continue,
            StepResult::Interrupt => break true,
            StepResult::Done => break false,
            other => panic!("unexpected step result {other:?} for {sql}"),
        }
    };
    drop(stmt);
    conn.set_progress_handler(0, None);
    interrupted
}

fn sqlite_step_with_interrupt(conn: &rusqlite::Connection, sql: &str) -> bool {
    set_sqlite_progress_handler(conn, INTERRUPT_AFTER_OPS as i32, Some(always_interrupt));
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = stmt.raw_query();
    let interrupted = loop {
        match rows.next() {
            Ok(Some(_)) => continue,
            Ok(None) => break false,
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == rusqlite::ErrorCode::OperationInterrupted =>
            {
                break true
            }
            Err(err) => panic!("unexpected SQLite error {err} for {sql}"),
        }
    };
    drop(rows);
    drop(stmt);
    set_sqlite_progress_handler(conn, 0, None);
    interrupted
}

type SqliteProgressHandler = unsafe extern "C" fn(*mut std::ffi::c_void) -> std::ffi::c_int;

fn set_sqlite_progress_handler(
    conn: &rusqlite::Connection,
    ops: i32,
    handler: Option<SqliteProgressHandler>,
) {
    unsafe {
        rusqlite::ffi::sqlite3_progress_handler(conn.handle(), ops, handler, std::ptr::null_mut())
    };
}

unsafe extern "C" fn always_interrupt(_: *mut std::ffi::c_void) -> std::ffi::c_int {
    1
}
