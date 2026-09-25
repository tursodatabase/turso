use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;

#[test]
fn mvcc_update_to_nocase_equal_value_replaces_index_key() {
    let tmp_db = TempDatabase::builder().with_mvcc(true).build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT COLLATE NOCASE)")
        .unwrap();
    conn.execute("CREATE INDEX users_email ON users(email)")
        .unwrap();
    conn.execute("INSERT INTO users VALUES(1, 'bob@example.com')")
        .unwrap();
    conn.execute("UPDATE users SET email = 'Bob@Example.com' WHERE id = 1")
        .unwrap();

    let rows = limbo_exec_rows(
        &conn,
        "SELECT email FROM users WHERE email = 'bob@example.com'",
    );
    assert_eq!(rows, vec![vec![Value::Text("Bob@Example.com".into())]]);

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let rows = limbo_exec_rows(
        &conn,
        "SELECT email FROM users WHERE email = 'bob@example.com'",
    );
    assert_eq!(rows, vec![vec![Value::Text("Bob@Example.com".into())]]);
}

#[test]
fn mvcc_update_integer_to_equal_real_replaces_index_key() {
    let tmp_db = TempDatabase::builder().with_mvcc(true).build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, a)")
        .unwrap();
    conn.execute("CREATE INDEX ta ON t(a)").unwrap();
    conn.execute("INSERT INTO t VALUES(1, 1)").unwrap();
    conn.execute("UPDATE t SET a = 1.0 WHERE id = 1").unwrap();

    let rows = limbo_exec_rows(&conn, "SELECT a, typeof(a) FROM t WHERE a > 0");
    assert_eq!(
        rows,
        vec![vec![Value::Real(1.0), Value::Text("real".into())]]
    );

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT a, typeof(a) FROM t WHERE a > 0");
    assert_eq!(
        rows,
        vec![vec![Value::Real(1.0), Value::Text("real".into())]]
    );
}
