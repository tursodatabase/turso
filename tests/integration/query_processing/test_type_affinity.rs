//! Tests for type affinity and comparison behavior
//!
//! This test module specifically targets issues related to type coercion
//! and comparison between different storage classes.

use crate::common::{limbo_exec_rows, sqlite_exec_rows};
use rusqlite::types::Value;

/// Test that TEXT columns can be compared with numeric literals.
///
/// SQLite performs numeric coercion when comparing TEXT values with numeric
/// literals. For example, `WHERE text_col = 0.0` should match rows where
/// text_col contains "0.0".
///
/// This test reproduces the fuzz failure from issue #4745.
#[turso_macros::test(init_sql = "CREATE TABLE t (c TEXT);")]
fn test_text_column_with_numeric_literal_float(tmp_db: crate::common::TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let sqlite_conn = rusqlite::Connection::open(&tmp_db.path)?;
    let limbo_conn = tmp_db.connect_limbo();

    // Insert a value that looks like a float string
    sqlite_conn.execute("INSERT INTO t VALUES ('0.0')", [])?;
    limbo_conn.execute("INSERT INTO t VALUES ('0.0')")?;

    // Query with numeric literal - should match the string "0.0"
    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, "SELECT c FROM t WHERE c = 0.0");
    let limbo_rows = limbo_exec_rows(&limbo_conn, "SELECT c FROM t WHERE c = 0.0");

    assert_eq!(
        sqlite_rows, limbo_rows,
        "Mismatch when comparing TEXT column with float literal. \
         SQLite returned {} rows, Limbo returned {} rows.",
        sqlite_rows.len(),
        limbo_rows.len()
    );
    assert_eq!(sqlite_rows.len(), 1, "Expected exactly 1 row from SQLite");
    assert_eq!(sqlite_rows[0], vec![Value::Text("0.0".to_string())]);
    Ok(())
}

/// Test that TEXT columns can be compared with integer literals.
#[turso_macros::test(init_sql = "CREATE TABLE t (c TEXT);")]
fn test_text_column_with_numeric_literal_int(tmp_db: crate::common::TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let sqlite_conn = rusqlite::Connection::open(&tmp_db.path)?;
    let limbo_conn = tmp_db.connect_limbo();

    // Insert a value that looks like an integer string
    sqlite_conn.execute("INSERT INTO t VALUES ('42')", [])?;
    limbo_conn.execute("INSERT INTO t VALUES ('42')")?;

    // Query with integer literal - should match the string "42"
    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, "SELECT c FROM t WHERE c = 42");
    let limbo_rows = limbo_exec_rows(&limbo_conn, "SELECT c FROM t WHERE c = 42");

    assert_eq!(
        sqlite_rows, limbo_rows,
        "Mismatch when comparing TEXT column with integer literal"
    );
    assert_eq!(sqlite_rows.len(), 1, "Expected exactly 1 row from SQLite");
    assert_eq!(sqlite_rows[0], vec![Value::Text("42".to_string())]);
    Ok(())
}

/// Test comparison with negative numeric literals.
#[turso_macros::test(init_sql = "CREATE TABLE t (c TEXT);")]
fn test_text_column_with_negative_numeric(tmp_db: crate::common::TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let sqlite_conn = rusqlite::Connection::open(&tmp_db.path)?;
    let limbo_conn = tmp_db.connect_limbo();

    // Insert negative number strings
    sqlite_conn.execute("INSERT INTO t VALUES ('-5.5')", [])?;
    limbo_conn.execute("INSERT INTO t VALUES ('-5.5')")?;

    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, "SELECT c FROM t WHERE c = -5.5");
    let limbo_rows = limbo_exec_rows(&limbo_conn, "SELECT c FROM t WHERE c = -5.5");

    assert_eq!(sqlite_rows, limbo_rows);
    assert_eq!(sqlite_rows.len(), 1);
    Ok(())
}

/// Test that NUMERIC affinity columns work correctly with text comparisons.
#[turso_macros::test(init_sql = "CREATE TABLE t (c NUMERIC);")]
fn test_numeric_column_with_text_literal(tmp_db: crate::common::TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let sqlite_conn = rusqlite::Connection::open(&tmp_db.path)?;
    let limbo_conn = tmp_db.connect_limbo();

    // Insert text representation of numbers into NUMERIC column
    sqlite_conn.execute("INSERT INTO t VALUES ('123.45')", [])?;
    limbo_conn.execute("INSERT INTO t VALUES ('123.45')")?;

    // Query with float literal
    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, "SELECT c FROM t WHERE c = 123.45");
    let limbo_rows = limbo_exec_rows(&limbo_conn, "SELECT c FROM t WHERE c = 123.45");

    assert_eq!(sqlite_rows, limbo_rows);
    Ok(())
}

/// Test with ORDER BY on a TEXT column using numeric comparison.
#[turso_macros::test(init_sql = "CREATE TABLE t (id INTEGER, c TEXT);")]
fn test_text_column_order_by_with_numeric_filter(tmp_db: crate::common::TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let sqlite_conn = rusqlite::Connection::open(&tmp_db.path)?;
    let limbo_conn = tmp_db.connect_limbo();

    // Insert rows
    sqlite_conn.execute("INSERT INTO t VALUES (1, '0.0')", [])?;
    sqlite_conn.execute("INSERT INTO t VALUES (2, '1.0')", [])?;
    sqlite_conn.execute("INSERT INTO t VALUES (3, '0.0')", [])?;

    limbo_conn.execute("INSERT INTO t VALUES (1, '0.0')")?;
    limbo_conn.execute("INSERT INTO t VALUES (2, '1.0')")?;
    limbo_conn.execute("INSERT INTO t VALUES (3, '0.0')")?;

    // This is the exact query from the fuzz failure
    let sqlite_rows = sqlite_exec_rows(
        &sqlite_conn,
        "SELECT id, c FROM t WHERE c = 0.0 ORDER BY c ASC, rowid ASC",
    );
    let limbo_rows = limbo_exec_rows(
        &limbo_conn,
        "SELECT id, c FROM t WHERE c = 0.0 ORDER BY c ASC, rowid ASC",
    );

    assert_eq!(
        sqlite_rows, limbo_rows,
        "Mismatch on query with ORDER BY. SQLite: {:?}, Limbo: {:?}",
        sqlite_rows, limbo_rows
    );
    Ok(())
}
