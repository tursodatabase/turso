//! How explicit transactions behave when a statement inside them fails,
//! checked against SQLite via rusqlite.

use std::sync::Arc;

use crate::common::{limbo_exec_rows, sqlite_exec_rows};
use turso_core::{ConnectOpts, Database, SqliteDialect, IO};

fn memory_database(mvcc: bool) -> Arc<Database> {
    let io = Arc::new(turso_core::MemoryIO::new()) as Arc<dyn IO>;
    Database::open_file_with_flags(
        io,
        ":memory:",
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_mvcc(mvcc),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

fn memory_connection(mvcc: bool) -> Arc<turso_core::Connection> {
    memory_database(mvcc).connect(ConnectOpts::new()).unwrap()
}

#[test]
fn explicit_transaction_survives_runtime_error() {
    // Run the same sequence against `execute` (returns true on success)
    // and `is_autocommit`. The critical assertion is that after a runtime
    // error the connection is still inside the explicit transaction.
    fn check(execute: impl Fn(&str) -> bool, is_autocommit: impl Fn() -> bool) {
        assert!(execute("BEGIN"));
        assert!(!is_autocommit());
        assert!(!execute("SELECT abs(-9223372036854775808)"));
        assert!(
            !is_autocommit(),
            "transaction must stay alive after a runtime error"
        );
        assert!(execute("ROLLBACK"));
        assert!(is_autocommit());
    }

    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    check(
        |sql| sqlite.execute_batch(sql).is_ok(),
        || sqlite.is_autocommit(),
    );

    for mvcc in [false, true] {
        let conn = memory_connection(mvcc);
        check(|sql| conn.execute(sql).is_ok(), || conn.get_auto_commit());
    }
}

#[test]
fn ordinary_error_in_a_writer_keeps_the_transaction() {
    // These statements get no statement savepoint, but an ordinary runtime
    // error is raised before they write anything, so only the statement is
    // lost and the transaction survives — SQLite behaves the same way.
    for failing in [
        "INSERT INTO t VALUES (abs(-9223372036854775808))",
        "UPDATE t SET x = abs(-9223372036854775808)",
    ] {
        fn check(
            failing: &str,
            execute: impl Fn(&str) -> bool,
            count: impl Fn() -> i64,
            is_autocommit: impl Fn() -> bool,
        ) {
            assert!(execute("CREATE TABLE t (x)"));
            assert!(execute("BEGIN"));
            assert!(execute("INSERT INTO t VALUES (1)"));
            assert!(!execute(failing), "{failing}");
            assert!(
                !is_autocommit(),
                "transaction must survive an ordinary error in a writer: {failing}"
            );
            assert!(execute("COMMIT"), "{failing}");
            assert_eq!(count(), 1, "{failing}");
        }

        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        check(
            failing,
            |sql| sqlite.execute_batch(sql).is_ok(),
            || rows_as_i64(&sqlite_exec_rows(&sqlite, "SELECT count(*) FROM t")),
            || sqlite.is_autocommit(),
        );

        for mvcc in [false, true] {
            let conn = memory_connection(mvcc);
            check(
                failing,
                |sql| conn.execute(sql).is_ok(),
                || rows_as_i64(&limbo_exec_rows(&conn, "SELECT count(*) FROM t")),
                || conn.get_auto_commit(),
            );
        }
    }
}

#[test]
fn error_in_a_returning_expression_undoes_the_statement() {
    // RETURNING expressions evaluate after the row is written, so these
    // statements need a statement savepoint: on error the write is undone
    // and the transaction survives — SQLite behaves the same way (it
    // implements RETURNING as an internal trigger).
    for failing in [
        "INSERT INTO t VALUES (2) RETURNING abs(-9223372036854775808)",
        "UPDATE t SET x = x + 10 WHERE x = 1 RETURNING abs(-9223372036854775808)",
        "DELETE FROM t WHERE x = 1 RETURNING abs(-9223372036854775808)",
    ] {
        fn check(
            failing: &str,
            execute: impl Fn(&str) -> bool,
            count: impl Fn(&str) -> i64,
            is_autocommit: impl Fn() -> bool,
        ) {
            assert!(execute("CREATE TABLE t (x)"));
            assert!(execute("BEGIN"));
            assert!(execute("INSERT INTO t VALUES (1)"));
            assert!(!execute(failing), "{failing}");
            assert!(
                !is_autocommit(),
                "transaction must survive a RETURNING expression error: {failing}"
            );
            assert!(execute("COMMIT"), "{failing}");
            assert_eq!(count("SELECT count(*) FROM t"), 1, "{failing}");
            assert_eq!(
                count("SELECT sum(x) FROM t"),
                1,
                "the failed statement's write must be undone: {failing}"
            );
        }

        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        check(
            failing,
            |sql| sqlite.execute_batch(sql).is_ok(),
            |sql| rows_as_i64(&sqlite_exec_rows(&sqlite, sql)),
            || sqlite.is_autocommit(),
        );

        for mvcc in [false, true] {
            let conn = memory_connection(mvcc);
            check(
                failing,
                |sql| conn.execute(sql).is_ok(),
                |sql| rows_as_i64(&limbo_exec_rows(&conn, sql)),
                || conn.get_auto_commit(),
            );
        }
    }
}

#[test]
fn out_of_space_without_statement_savepoint_rolls_back_the_transaction() {
    // Running out of space strikes mid-write. A single-row INSERT has no
    // statement savepoint to undo the partial write, so the whole
    // transaction must roll back — SQLite behaves the same way.
    fn check(execute: impl Fn(&str) -> bool, count: impl Fn(&str) -> i64) {
        assert!(execute("PRAGMA max_page_count=10"));
        assert!(execute("CREATE TABLE a (x)"));
        assert!(execute("CREATE TABLE b (x)"));
        assert!(execute("BEGIN"));
        assert!(execute("INSERT INTO a VALUES (1)"));
        assert!(!execute("INSERT INTO b VALUES (randomblob(100000))"));
        assert!(
            !execute("COMMIT"),
            "the transaction must already be rolled back"
        );
        assert_eq!(count("SELECT count(*) FROM a"), 0);
        assert_eq!(count("SELECT count(*) FROM b"), 0);
    }

    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    check(
        |sql| sqlite.execute_batch(sql).is_ok(),
        |sql| rows_as_i64(&sqlite_exec_rows(&sqlite, sql)),
    );

    // WAL mode only: under MVCC the page limit acts when the checkpointer
    // writes the B-tree, not while the statement runs.
    let conn = memory_connection(false);
    check(
        |sql| conn.execute(sql).is_ok(),
        |sql| rows_as_i64(&limbo_exec_rows(&conn, sql)),
    );
}

#[test]
fn out_of_space_with_statement_savepoint_keeps_the_transaction() {
    // INSERT ... SELECT gets a statement savepoint, so running out of space
    // undoes only the statement and the transaction survives — SQLite
    // behaves the same way.
    fn check(execute: impl Fn(&str) -> bool, count: impl Fn(&str) -> i64) {
        assert!(execute("PRAGMA max_page_count=12"));
        assert!(execute("CREATE TABLE a (x)"));
        assert!(execute("CREATE TABLE src (x)"));
        assert!(execute(
            "INSERT INTO src VALUES (randomblob(2000)), (randomblob(2000)), (randomblob(2000))"
        ));
        assert!(execute("BEGIN"));
        assert!(execute("INSERT INTO a VALUES (1)"));
        assert!(!execute(
            "INSERT INTO a SELECT randomblob(10000) FROM src, src, src"
        ));
        assert!(execute("COMMIT"), "the transaction must still be open");
        assert_eq!(count("SELECT count(*) FROM a"), 1);
    }

    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    check(
        |sql| sqlite.execute_batch(sql).is_ok(),
        |sql| rows_as_i64(&sqlite_exec_rows(&sqlite, sql)),
    );

    // WAL mode only: under MVCC the page limit acts when the checkpointer
    // writes the B-tree, not while the statement runs.
    let conn = memory_connection(false);
    check(
        |sql| conn.execute(sql).is_ok(),
        |sql| rows_as_i64(&limbo_exec_rows(&conn, sql)),
    );
}

#[test]
fn explicit_transaction_survives_failed_multi_row_write() {
    // A multi-row write runs under a statement savepoint, so when it fails
    // mid-statement only that statement is undone: earlier writes stay and
    // the transaction can still commit.
    fn check(
        execute: impl Fn(&str) -> bool,
        count: impl Fn() -> i64,
        is_autocommit: impl Fn() -> bool,
    ) {
        assert!(execute("CREATE TABLE t (x INTEGER UNIQUE)"));
        assert!(execute("BEGIN"));
        assert!(execute("INSERT INTO t VALUES (1), (2), (3)"));
        assert!(!execute("INSERT INTO t SELECT x + 2 FROM t"));
        assert!(
            !is_autocommit(),
            "transaction must stay alive after a failed multi-row write"
        );
        assert_eq!(
            count(),
            3,
            "the failed statement must be fully undone and earlier writes kept"
        );
        assert!(execute("COMMIT"));
        assert_eq!(count(), 3);
    }

    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    check(
        |sql| sqlite.execute_batch(sql).is_ok(),
        || rows_as_i64(&sqlite_exec_rows(&sqlite, "SELECT count(*) FROM t")),
        || sqlite.is_autocommit(),
    );

    for mvcc in [false, true] {
        let conn = memory_connection(mvcc);
        check(
            |sql| conn.execute(sql).is_ok(),
            || rows_as_i64(&limbo_exec_rows(&conn, "SELECT count(*) FROM t")),
            || conn.get_auto_commit(),
        );
    }
}

fn rows_as_i64(rows: &[Vec<rusqlite::types::Value>]) -> i64 {
    match &rows[0][0] {
        rusqlite::types::Value::Integer(n) => *n,
        other => panic!("expected an integer, got {other:?}"),
    }
}

#[test]
fn mvcc_checkpoint_activity_does_not_disturb_a_failing_transaction() {
    // Another connection's commits drive MVCC checkpoints, which write the
    // B-tree and dirty pager pages. An open transaction whose statement then
    // fails with an ordinary error must survive: its write activity is
    // measured against its own write set, not the shared pager.
    let db = memory_database(true);
    let conn1 = db.connect(ConnectOpts::new()).unwrap();
    let conn2 = db.connect(ConnectOpts::new()).unwrap();
    conn1
        .execute("CREATE TABLE t (x INTEGER PRIMARY KEY)")
        .unwrap();
    conn1
        .execute("PRAGMA mvcc_checkpoint_threshold = 1")
        .unwrap();
    conn1.execute("BEGIN CONCURRENT").unwrap();
    conn1.execute("INSERT INTO t VALUES (1)").unwrap();
    for i in 100..130 {
        conn2
            .execute(format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }
    assert!(conn1
        .execute("INSERT INTO t VALUES (abs(-9223372036854775808))")
        .is_err());
    assert!(
        !conn1.get_auto_commit(),
        "the transaction must survive an ordinary error despite checkpoint activity"
    );
    conn1.execute("COMMIT").unwrap();
    assert_eq!(
        rows_as_i64(&limbo_exec_rows(&conn1, "SELECT count(*) FROM t")),
        31
    );
}

#[test]
fn error_in_materialized_view_maintenance_undoes_the_statement() {
    // Maintaining a materialized view evaluates its expressions after the
    // base-table write, so the statement needs a statement savepoint: on
    // error the base write is undone and the transaction survives, and the
    // view stays consistent with the table.
    let tmp_db = crate::common::TempDatabase::builder()
        .with_views(true)
        .build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (x)").unwrap();
    conn.execute("CREATE MATERIALIZED VIEW v AS SELECT abs(x) AS a FROM t")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("BEGIN").unwrap();
    assert!(conn
        .execute("UPDATE t SET x = -9223372036854775808 WHERE rowid = 1")
        .is_err());
    assert!(
        !conn.get_auto_commit(),
        "the transaction must survive the view-maintenance error"
    );
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT x FROM t ORDER BY rowid"),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT a FROM v ORDER BY a"),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ],
        "the view must stay consistent with the base table"
    );
}

#[test]
fn multi_row_statement_without_savepoint_keeps_partial_writes() {
    // The may-abort analysis gives this UPDATE no statement savepoint (no
    // constraints, triggers, FKs, or RETURNING), so when its subquery
    // overflows on the second row, the first row's write stays and the
    // transaction survives — SQLite behaves the same way: no statement
    // journal means no statement atomicity.
    fn check(
        execute: impl Fn(&str) -> bool,
        rows: impl Fn() -> Vec<Vec<rusqlite::types::Value>>,
        is_autocommit: impl Fn() -> bool,
    ) {
        assert!(execute("CREATE TABLE t (x)"));
        assert!(execute("INSERT INTO t VALUES (-1), (1)"));
        assert!(execute("BEGIN"));
        assert!(!execute(
            "UPDATE t SET x = (SELECT sum(v) FROM \
             (SELECT 9223372036854775807 v UNION ALL SELECT t.x))"
        ));
        assert!(!is_autocommit(), "the transaction must stay alive");
        assert!(execute("COMMIT"));
        assert_eq!(
            rows(),
            vec![
                vec![rusqlite::types::Value::Integer(9223372036854775806)],
                vec![rusqlite::types::Value::Integer(1)],
            ],
            "the row written before the error must stay"
        );
    }

    let sqlite = rusqlite::Connection::open_in_memory().unwrap();
    check(
        |sql| sqlite.execute_batch(sql).is_ok(),
        || sqlite_exec_rows(&sqlite, "SELECT x FROM t ORDER BY rowid"),
        || sqlite.is_autocommit(),
    );

    for mvcc in [false, true] {
        let conn = memory_connection(mvcc);
        check(
            |sql| conn.execute(sql).is_ok(),
            || limbo_exec_rows(&conn, "SELECT x FROM t ORDER BY rowid"),
            || conn.get_auto_commit(),
        );
    }
}
