use std::{path::Path, sync::Arc};

use turso_core::{ConnectOpts, Database, OpenOptions, PlatformIO, SqliteDialect, Value};

fn read_rowid_pair(path: &Path) -> Vec<Vec<Value>> {
    let io = Arc::new(PlatformIO::new().unwrap());
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let db = Database::open(io, path.to_str().unwrap(), options).unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    let mut stmt = conn.prepare("SELECT a, rowid FROM t").unwrap();
    stmt.run_collect_rows().unwrap()
}

#[test]
fn sqlite_quoted_integer_primary_key_is_loaded_as_rowid_alias() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("quoted-integer.db");
    {
        let sqlite = rusqlite::Connection::open(&path).unwrap();
        sqlite
            .execute_batch(
                "CREATE TABLE t(a \"INTEGER\" PRIMARY KEY, b); INSERT INTO t VALUES(5, 1);",
            )
            .unwrap();
    }

    assert_eq!(
        read_rowid_pair(&path),
        vec![vec![Value::from_i64(5), Value::from_i64(5)]]
    );
}

#[test]
fn sqlite_quoted_integer_primary_key_keeps_other_unique_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("quoted-integer-with-unique.db");
    {
        let sqlite = rusqlite::Connection::open(&path).unwrap();
        sqlite
            .execute_batch(
                "CREATE TABLE t(a \"INTEGER\" PRIMARY KEY, b UNIQUE); INSERT INTO t VALUES(5, 1);",
            )
            .unwrap();
    }

    assert_eq!(
        read_rowid_pair(&path),
        vec![vec![Value::from_i64(5), Value::from_i64(5)]]
    );
}

#[test]
fn legacy_quoted_integer_primary_key_with_autoindex_keeps_stored_value() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy-quoted-integer.db");
    {
        let sqlite = rusqlite::Connection::open(&path).unwrap();
        sqlite
            .execute_batch(
                r#"CREATE TABLE t(a INT PRIMARY KEY, b);
                   INSERT INTO t VALUES(5, 1);
                   UPDATE t SET rowid = 7;
                   PRAGMA writable_schema = ON;
                   UPDATE sqlite_schema
                   SET sql = 'CREATE TABLE t(a "INTEGER" PRIMARY KEY, b)'
                   WHERE name = 't';
                   PRAGMA schema_version = 2;"#,
            )
            .unwrap();
    }

    assert_eq!(
        read_rowid_pair(&path),
        vec![vec![Value::from_i64(5), Value::from_i64(7)]]
    );
}
