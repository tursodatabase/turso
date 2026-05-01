//! Regression tests for AUTOINCREMENT when sqlite_sequence is missing.
//!
//! See https://github.com/tursodatabase/turso/issues/3764
//! and SQLite fix d8dc2b3a58cd5dc29.
//!
//! In SQLite, `PRAGMA writable_schema=ON` lets a user delete the
//! sqlite_sequence row from sqlite_schema and then INSERT into an
//! AUTOINCREMENT table. Without an existence check this segfaults SQLite.
//!
//! Turso does not yet support `PRAGMA writable_schema`, so we cannot
//! reproduce the scenario from SQL. Instead we mutate the connection's
//! in-memory schema cache to drop sqlite_sequence and then exercise the
//! same AUTOINCREMENT code paths to verify they fail with a corrupt-db
//! error rather than panicking.
//!
//! Each test covers a distinct emit path that opens sqlite_sequence:
//! - implicit rowid INSERT (init_autoincrement)
//! - explicit rowid INSERT (reload_autoincrement_state)
//! - multi-row INSERT...SELECT (ensure_sequence_initialized)

use crate::common::{limbo_exec_rows, try_limbo_exec_rows, TempDatabase};

fn assert_missing_sequence_error(err: turso_core::LimboError, ctx: &str) {
    let msg = format!("{err}");
    let is_corrupt = matches!(err, turso_core::LimboError::Corrupt(_));
    assert!(
        is_corrupt && msg.contains("sqlite_sequence"),
        "{ctx}: expected LimboError::Corrupt mentioning sqlite_sequence, got: {err}"
    );
}

#[test]
fn autoincrement_implicit_rowid_with_missing_sqlite_sequence() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
        .unwrap();

    // Sanity check: sqlite_sequence was auto-created.
    let rows = limbo_exec_rows(
        &conn,
        "SELECT name FROM sqlite_schema WHERE name='sqlite_sequence'",
    );
    assert_eq!(rows.len(), 1);

    // Simulate writable_schema deleting the sqlite_sequence schema row.
    conn.with_schema_mut(|schema| {
        schema.remove_table("sqlite_sequence");
    });

    let err = try_limbo_exec_rows(&db, &conn, "INSERT INTO t(val) VALUES ('hello')")
        .expect_err("INSERT should fail when sqlite_sequence is missing");
    assert_missing_sequence_error(err, "implicit rowid INSERT");
}

#[test]
fn autoincrement_explicit_rowid_with_missing_sqlite_sequence() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
        .unwrap();

    conn.with_schema_mut(|schema| {
        schema.remove_table("sqlite_sequence");
    });

    // Explicit rowid takes a different code path: it calls reload_autoincrement_state
    // and emit_update_sqlite_sequence so the sequence reflects user-supplied keys.
    let err = try_limbo_exec_rows(&db, &conn, "INSERT INTO t(id, val) VALUES (10, 'x')")
        .expect_err("INSERT with explicit rowid should fail when sqlite_sequence is missing");
    assert_missing_sequence_error(err, "explicit rowid INSERT");
}

#[test]
fn autoincrement_multirow_insert_with_missing_sqlite_sequence() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE src(id INTEGER, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    conn.execute("CREATE TABLE dst(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
        .unwrap();

    conn.with_schema_mut(|schema| {
        schema.remove_table("sqlite_sequence");
    });

    // INSERT...SELECT for AUTOINCREMENT routes through ensure_sequence_initialized,
    // which is a third caller of get_valid_sqlite_sequence_table.
    let err = try_limbo_exec_rows(&db, &conn, "INSERT INTO dst(val) SELECT val FROM src")
        .expect_err("multi-row INSERT should fail when sqlite_sequence is missing");
    assert_missing_sequence_error(err, "multi-row INSERT...SELECT");
}

#[test]
fn autoincrement_works_normally_when_sqlite_sequence_present() {
    // Companion happy-path test so a regression that breaks the corrupt
    // detection by accident doesn't pass these tests trivially.
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t(val) VALUES ('a'), ('b')")
        .unwrap();

    let rows = limbo_exec_rows(&conn, "SELECT id, val FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);

    let seq = limbo_exec_rows(&conn, "SELECT seq FROM sqlite_sequence WHERE name='t'");
    assert_eq!(seq.len(), 1);
}
