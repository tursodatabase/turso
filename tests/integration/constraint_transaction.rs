//! Tests for constraint violation behavior within transactions.
//!
//! Verifies that tursodb matches SQLite's ABORT semantics:
//! - Default (ABORT): only the failing statement is rolled back; prior
//!   successful statements in the same transaction are preserved.
//! - OR ROLLBACK: the entire transaction is rolled back.
//! - OR FAIL: changes made by the failing statement *before* the error persist.
//!
//! These are differential tests that run the same SQL against both tursodb
//! (via turso_core) and SQLite (via rusqlite) and compare results.

use crate::common::{ExecRows, TempDatabase};

/// Helper: run SQL against rusqlite, ignoring errors from individual statements.
fn sqlite_exec_ignore_errors(conn: &rusqlite::Connection, sql: &str) {
    let _ = conn.execute_batch(sql);
}

/// Helper: run SQL against rusqlite, expecting success.
fn sqlite_exec(conn: &rusqlite::Connection, sql: &str) {
    conn.execute_batch(sql).unwrap();
}

/// Helper: query rows from rusqlite.
fn sqlite_query_i64(conn: &rusqlite::Connection, sql: &str) -> Vec<i64> {
    let mut stmt = conn.prepare(sql).unwrap();
    let rows = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<i64>, _>>()
        .unwrap();
    rows
}

/// CHECK constraint failure with default ABORT in explicit transaction:
/// successful statements before the failure are preserved and committed.
#[turso_macros::test]
fn test_check_abort_explicit_txn_data_survives(db: TempDatabase) {
    let limbo = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "CREATE TABLE t(val INT CHECK(val > 0));";
    limbo.execute(setup).unwrap();
    sqlite_exec(&sqlite, setup);

    // Run statements one by one so we can ignore the error
    for sql in [
        "BEGIN;",
        "INSERT INTO t VALUES (10);",
        "INSERT INTO t VALUES (20);",
    ] {
        limbo.execute(sql).unwrap();
        sqlite_exec(&sqlite, sql);
    }

    // This should fail
    let limbo_err = limbo.execute("INSERT INTO t VALUES (-5);");
    assert!(limbo_err.is_err(), "Expected CHECK constraint error");
    sqlite_exec_ignore_errors(&sqlite, "INSERT INTO t VALUES (-5);");

    // Commit
    limbo.execute("COMMIT;").unwrap();
    sqlite_exec(&sqlite, "COMMIT;");

    // Verify: both should have rows 10 and 20
    let limbo_rows: Vec<(i64,)> = limbo.exec_rows("SELECT val FROM t ORDER BY val;");
    let sqlite_rows = sqlite_query_i64(&sqlite, "SELECT val FROM t ORDER BY val;");

    assert_eq!(
        limbo_rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        sqlite_rows,
        "tursodb and SQLite should return same rows after CHECK ABORT in explicit txn"
    );
    assert_eq!(sqlite_rows, vec![10, 20]);
}

/// After CHECK constraint failure, transaction stays active and more
/// statements can be executed successfully.
#[turso_macros::test]
fn test_check_abort_continue_after_error(db: TempDatabase) {
    let limbo = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "CREATE TABLE t(val INT CHECK(val > 0));";
    limbo.execute(setup).unwrap();
    sqlite_exec(&sqlite, setup);

    for sql in ["BEGIN;", "INSERT INTO t VALUES (10);"] {
        limbo.execute(sql).unwrap();
        sqlite_exec(&sqlite, sql);
    }

    // Fail
    let _ = limbo.execute("INSERT INTO t VALUES (-5);");
    sqlite_exec_ignore_errors(&sqlite, "INSERT INTO t VALUES (-5);");

    // Continue inserting
    limbo.execute("INSERT INTO t VALUES (30);").unwrap();
    sqlite_exec(&sqlite, "INSERT INTO t VALUES (30);");

    limbo.execute("COMMIT;").unwrap();
    sqlite_exec(&sqlite, "COMMIT;");

    let limbo_rows: Vec<(i64,)> = limbo.exec_rows("SELECT val FROM t ORDER BY val;");
    let sqlite_rows = sqlite_query_i64(&sqlite, "SELECT val FROM t ORDER BY val;");

    assert_eq!(
        limbo_rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        sqlite_rows
    );
    assert_eq!(sqlite_rows, vec![10, 30]);
}

/// Multi-value INSERT where one value fails CHECK: the entire statement
/// is rolled back (ABORT), but prior statements in the transaction survive.
#[turso_macros::test]
fn test_check_abort_multi_value_statement_rollback(db: TempDatabase) {
    let limbo = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "CREATE TABLE t(val INT CHECK(val > 0));";
    limbo.execute(setup).unwrap();
    sqlite_exec(&sqlite, setup);

    limbo.execute("BEGIN;").unwrap();
    sqlite_exec(&sqlite, "BEGIN;");

    limbo.execute("INSERT INTO t VALUES (10);").unwrap();
    sqlite_exec(&sqlite, "INSERT INTO t VALUES (10);");

    // Multi-value INSERT: 20 succeeds within the statement, then -5 fails,
    // so the entire statement (including 20) is rolled back.
    let _ = limbo.execute("INSERT INTO t VALUES (20), (-5), (30);");
    sqlite_exec_ignore_errors(&sqlite, "INSERT INTO t VALUES (20), (-5), (30);");

    limbo.execute("COMMIT;").unwrap();
    sqlite_exec(&sqlite, "COMMIT;");

    let limbo_rows: Vec<(i64,)> = limbo.exec_rows("SELECT val FROM t ORDER BY val;");
    let sqlite_rows = sqlite_query_i64(&sqlite, "SELECT val FROM t ORDER BY val;");

    assert_eq!(
        limbo_rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        sqlite_rows
    );
    // Only row 10 from the first INSERT survives
    assert_eq!(sqlite_rows, vec![10]);
}

/// INSERT OR ROLLBACK: the entire transaction is rolled back on constraint failure.
#[turso_macros::test]
fn test_check_or_rollback_entire_txn(db: TempDatabase) {
    let limbo = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "CREATE TABLE t(val INT CHECK(val > 0));";
    limbo.execute(setup).unwrap();
    sqlite_exec(&sqlite, setup);

    for sql in [
        "BEGIN;",
        "INSERT INTO t VALUES (10);",
        "INSERT INTO t VALUES (20);",
    ] {
        limbo.execute(sql).unwrap();
        sqlite_exec(&sqlite, sql);
    }

    // OR ROLLBACK: this rolls back the entire transaction
    let limbo_err = limbo.execute("INSERT OR ROLLBACK INTO t VALUES (-5);");
    assert!(limbo_err.is_err());
    sqlite_exec_ignore_errors(&sqlite, "INSERT OR ROLLBACK INTO t VALUES (-5);");

    // Table should be empty (transaction was rolled back)
    let limbo_rows: Vec<(i64,)> = limbo.exec_rows("SELECT val FROM t ORDER BY val;");
    let sqlite_rows = sqlite_query_i64(&sqlite, "SELECT val FROM t ORDER BY val;");

    assert_eq!(
        limbo_rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        sqlite_rows
    );
    assert!(sqlite_rows.is_empty());
}

/// ROLLBACK after CHECK failure: all changes in the transaction are undone.
#[turso_macros::test]
fn test_check_abort_then_explicit_rollback(db: TempDatabase) {
    let limbo = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "CREATE TABLE t(val INT CHECK(val > 0));";
    limbo.execute(setup).unwrap();
    sqlite_exec(&sqlite, setup);

    for sql in [
        "BEGIN;",
        "INSERT INTO t VALUES (10);",
        "INSERT INTO t VALUES (20);",
    ] {
        limbo.execute(sql).unwrap();
        sqlite_exec(&sqlite, sql);
    }

    let _ = limbo.execute("INSERT INTO t VALUES (-5);");
    sqlite_exec_ignore_errors(&sqlite, "INSERT INTO t VALUES (-5);");

    // Explicit ROLLBACK
    limbo.execute("ROLLBACK;").unwrap();
    sqlite_exec(&sqlite, "ROLLBACK;");

    let limbo_rows: Vec<(i64,)> = limbo.exec_rows("SELECT val FROM t ORDER BY val;");
    let sqlite_rows = sqlite_query_i64(&sqlite, "SELECT val FROM t ORDER BY val;");

    assert_eq!(
        limbo_rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        sqlite_rows
    );
    assert!(sqlite_rows.is_empty());
}

/// UNIQUE constraint failure with default ABORT in explicit transaction.
#[turso_macros::test]
fn test_unique_abort_explicit_txn(db: TempDatabase) {
    let limbo = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "CREATE TABLE t(val INT UNIQUE);";
    limbo.execute(setup).unwrap();
    sqlite_exec(&sqlite, setup);

    for sql in [
        "BEGIN;",
        "INSERT INTO t VALUES (10);",
        "INSERT INTO t VALUES (20);",
    ] {
        limbo.execute(sql).unwrap();
        sqlite_exec(&sqlite, sql);
    }

    let limbo_err = limbo.execute("INSERT INTO t VALUES (10);");
    assert!(limbo_err.is_err());
    sqlite_exec_ignore_errors(&sqlite, "INSERT INTO t VALUES (10);");

    limbo.execute("COMMIT;").unwrap();
    sqlite_exec(&sqlite, "COMMIT;");

    let limbo_rows: Vec<(i64,)> = limbo.exec_rows("SELECT val FROM t ORDER BY val;");
    let sqlite_rows = sqlite_query_i64(&sqlite, "SELECT val FROM t ORDER BY val;");

    assert_eq!(
        limbo_rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        sqlite_rows
    );
    assert_eq!(sqlite_rows, vec![10, 20]);
}

/// NOT NULL constraint failure with default ABORT in explicit transaction.
#[turso_macros::test]
fn test_notnull_abort_explicit_txn(db: TempDatabase) {
    let limbo = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();

    let setup = "CREATE TABLE t(val INT NOT NULL);";
    limbo.execute(setup).unwrap();
    sqlite_exec(&sqlite, setup);

    for sql in [
        "BEGIN;",
        "INSERT INTO t VALUES (10);",
        "INSERT INTO t VALUES (20);",
    ] {
        limbo.execute(sql).unwrap();
        sqlite_exec(&sqlite, sql);
    }

    let limbo_err = limbo.execute("INSERT INTO t VALUES (NULL);");
    assert!(limbo_err.is_err());
    sqlite_exec_ignore_errors(&sqlite, "INSERT INTO t VALUES (NULL);");

    limbo.execute("COMMIT;").unwrap();
    sqlite_exec(&sqlite, "COMMIT;");

    let limbo_rows: Vec<(i64,)> = limbo.exec_rows("SELECT val FROM t ORDER BY val;");
    let sqlite_rows = sqlite_query_i64(&sqlite, "SELECT val FROM t ORDER BY val;");

    assert_eq!(
        limbo_rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        sqlite_rows
    );
    assert_eq!(sqlite_rows, vec![10, 20]);
}

/// UPDATE with CHECK failure in explicit transaction: the UPDATE statement
/// is rolled back but prior changes survive.
#[turso_macros::test]
fn test_update_check_abort_explicit_txn(db: TempDatabase) {
    let limbo = db.connect_limbo();
    let sqlite = rusqlite::Connection::open_in_memory().unwrap();

    for sql in [
        "CREATE TABLE t(val INT CHECK(val > 0));",
        "INSERT INTO t VALUES (10);",
        "INSERT INTO t VALUES (20);",
    ] {
        limbo.execute(sql).unwrap();
        sqlite_exec(&sqlite, sql);
    }

    limbo.execute("BEGIN;").unwrap();
    sqlite_exec(&sqlite, "BEGIN;");

    limbo.execute("INSERT INTO t VALUES (30);").unwrap();
    sqlite_exec(&sqlite, "INSERT INTO t VALUES (30);");

    // UPDATE that fails CHECK
    let limbo_err = limbo.execute("UPDATE t SET val = -1 WHERE val = 10;");
    assert!(limbo_err.is_err());
    sqlite_exec_ignore_errors(&sqlite, "UPDATE t SET val = -1 WHERE val = 10;");

    limbo.execute("COMMIT;").unwrap();
    sqlite_exec(&sqlite, "COMMIT;");

    let limbo_rows: Vec<(i64,)> = limbo.exec_rows("SELECT val FROM t ORDER BY val;");
    let sqlite_rows = sqlite_query_i64(&sqlite, "SELECT val FROM t ORDER BY val;");

    assert_eq!(
        limbo_rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        sqlite_rows
    );
    // Original 10 and 20, plus new 30. The failed UPDATE didn't change 10.
    assert_eq!(sqlite_rows, vec![10, 20, 30]);
}
