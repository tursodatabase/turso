use crate::common::{limbo_exec_rows, rusqlite_integrity_check, TempDatabase};
use rusqlite::types::Value;
use tempfile::TempDir;

#[test]
fn tables_after_fts5_table_are_visible() {
    let (_dir, path) = create_sqlite_file(
        "CREATE TABLE a(x); CREATE VIRTUAL TABLE f USING fts5(b); CREATE TABLE z(y);",
    );
    let db = TempDatabase::new_with_existent(&path);
    let conn = db.connect_limbo();
    let rows = limbo_exec_rows(&conn, "SELECT count(*) FROM z");
    assert_eq!(rows, vec![vec![Value::Integer(0)]]);
}

#[test]
fn writes_update_indexes_when_file_has_rtree_table() {
    let (_dir, path) = create_sqlite_file(
        "CREATE TABLE a(x); CREATE INDEX ax ON a(x); CREATE VIRTUAL TABLE f USING rtree(id, x0, x1);",
    );
    let db = TempDatabase::new_with_existent(&path);
    let conn = db.connect_limbo();
    conn.execute("INSERT INTO a VALUES(1)").unwrap();
    let rows = limbo_exec_rows(&conn, "PRAGMA integrity_check");
    assert_eq!(rows, vec![vec![Value::Text("ok".to_string())]]);
    conn.close().unwrap();
    drop(db);
    rusqlite_integrity_check(&path).unwrap();
}

fn create_sqlite_file(sql: &str) -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("vtab.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(sql).unwrap();
    drop(conn);
    (dir, path)
}
