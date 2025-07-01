use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;

#[test]
fn test_insert_returning_single_row() -> anyhow::Result<()> {
    // Initialize temporary database and connection
    let tmp_db = TempDatabase::new_empty(false);
    let conn = tmp_db.connect_limbo();

    // Create table
    let ret = limbo_exec_rows(&tmp_db, &conn, "CREATE TABLE t(x INTEGER);");
    assert!(ret.is_empty());

    // Insert with RETURNING and verify the returned row
    let ret = limbo_exec_rows(&tmp_db, &conn, "INSERT INTO t VALUES (1) RETURNING x;");
    assert_eq!(ret, vec![vec![Value::Integer(1)]]);

    let ret = limbo_exec_rows(&tmp_db, &conn, "INSERT INTO t VALUES (1) RETURNING x;");
    assert_eq!(ret, vec![vec![Value::Integer(1)]]);

    Ok(())
}

#[test]
fn test_insert_returning_multiple_rows() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty(false);
    let conn = tmp_db.connect_limbo();
    limbo_exec_rows(&tmp_db, &conn, "CREATE TABLE t(x INTEGER);");

    // Multiple-row insert using VALUES list
    let ret = limbo_exec_rows(&tmp_db, &conn, "INSERT INTO t VALUES (2), (3) RETURNING x;");
    assert_eq!(ret, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);

    let ret = limbo_exec_rows(&tmp_db, &conn, "INSERT INTO t VALUES (4), (5) RETURNING *;");
    assert_eq!(ret, vec![vec![Value::Integer(4)], vec![Value::Integer(5)]]);

    Ok(())
}
