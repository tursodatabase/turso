//! Regression tests for issue #8695: an UPDATE that fails partway through
//! must undo the rows it already rewrote, even inside an explicit
//! transaction. SQLite leaves the table unchanged in this case.
use crate::common::TempDatabase;
use std::sync::Arc;

fn query_rows(conn: &Arc<turso_core::Connection>, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let vals: Vec<String> = row.get_values().map(|v| format!("{v}")).collect();
        rows.push(vals.join("|"));
        Ok(())
    })
    .unwrap();
    rows
}

const IDS: &str = "SELECT group_concat(id) FROM (SELECT id FROM t ORDER BY id)";

#[turso_macros::test(init_sql = "CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT);")]
fn failed_rowid_update_outside_tx_leaves_table_unchanged(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(10,'j')")?;

    let result = conn.execute("UPDATE t SET id = id + 7");
    assert!(result.is_err(), "UPDATE should fail with UNIQUE constraint");

    assert_eq!(query_rows(&conn, IDS), vec!["1,2,3,10"]);
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT);")]
fn failed_rowid_update_inside_tx_leaves_table_unchanged(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(10,'j')")?;

    conn.execute("BEGIN")?;
    let result = conn.execute("UPDATE t SET id = id + 7");
    assert!(result.is_err(), "UPDATE should fail with UNIQUE constraint");
    conn.execute("COMMIT")?;

    assert_eq!(query_rows(&conn, IDS), vec!["1,2,3,10"]);
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t(id INTEGER PRIMARY KEY, k TEXT UNIQUE);")]
fn failed_rowid_update_with_unique_column_inside_tx_leaves_table_unchanged(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c'),(10,'j')")?;

    conn.execute("BEGIN")?;
    let result = conn.execute("UPDATE t SET id = id + 7");
    assert!(result.is_err(), "UPDATE should fail with UNIQUE constraint");
    conn.execute("COMMIT")?;

    assert_eq!(query_rows(&conn, IDS), vec!["1,2,3,10"]);
    Ok(())
}
