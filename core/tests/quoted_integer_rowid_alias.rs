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
fn sqlite_quoted_integer_primary_key_desc_forms() {
    for (suffix, expected) in [("PRIMARY KEY(a DESC)", 5), ("", 1)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("quoted-integer-desc.db");
        {
            let sqlite = rusqlite::Connection::open(&path).unwrap();
            let definition = if suffix.is_empty() {
                "a 'INTEGER' PRIMARY KEY DESC, b".to_owned()
            } else {
                format!("a 'INTEGER', b, {suffix}")
            };
            sqlite
                .execute_batch(&format!(
                    "CREATE TABLE t({definition}); INSERT INTO t VALUES(5, 1);"
                ))
                .unwrap();
        }
        assert_eq!(
            read_rowid_pair(&path),
            vec![vec![Value::from_i64(5), Value::from_i64(expected)]],
            "{suffix}"
        );
    }
}

#[test]
fn legacy_quoted_integer_with_additional_unique_index_preserves_values() {
    for written in ["\"INTEGER\"", "'INTEGER'", "`INTEGER`"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy-with-unique.db");
        {
            let sqlite = rusqlite::Connection::open(&path).unwrap();
            sqlite.execute_batch("CREATE TABLE t(a INT PRIMARY KEY, b UNIQUE); INSERT INTO t VALUES(5, 1); UPDATE t SET rowid = 7; PRAGMA writable_schema = ON;").unwrap();
            sqlite
                .execute(
                    "UPDATE sqlite_schema SET sql = ?1 WHERE name = 't'",
                    [format!("CREATE TABLE t(a {written} PRIMARY KEY, b UNIQUE)")],
                )
                .unwrap();
            sqlite.execute_batch("PRAGMA schema_version = 2;").unwrap();
        }
        assert_eq!(
            read_rowid_pair(&path),
            vec![vec![Value::from_i64(5), Value::from_i64(7)]],
            "{written}"
        );
    }
}

#[test]
fn turso_created_quoted_integer_primary_key_is_rowid_alias() {
    for written in ["\"INTEGER\"", "'INTEGER'", "`INTEGER`", "[INTEGER]"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("turso-quoted-integer.db");
        {
            let io = Arc::new(PlatformIO::new().unwrap());
            let db = Database::open(
                io,
                path.to_str().unwrap(),
                OpenOptions::new(Arc::new(SqliteDialect)),
            )
            .unwrap();
            let conn = db.connect(ConnectOpts::new()).unwrap();
            conn.execute(format!("CREATE TABLE t(a {written} PRIMARY KEY, b)"))
                .unwrap();
            conn.execute("INSERT INTO t VALUES(5, 1)").unwrap();
            let mut stmt = conn.prepare("SELECT a, rowid FROM t").unwrap();
            assert_eq!(
                stmt.run_collect_rows().unwrap(),
                vec![vec![Value::from_i64(5), Value::from_i64(5)]],
                "{written}"
            );
        }
        assert_eq!(
            read_rowid_pair(&path),
            vec![vec![Value::from_i64(5), Value::from_i64(5)]],
            "reopen {written}"
        );
    }
}

#[test]
fn sqlite_integer_primary_key_all_quoting_styles() {
    for written in ["\"INTEGER\"", "'INTEGER'", "`INTEGER`", "[INTEGER]"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("quoted-integer.db");
        {
            let sqlite = rusqlite::Connection::open(&path).unwrap();
            sqlite
                .execute_batch(&format!(
                    "CREATE TABLE t(a {written} PRIMARY KEY, b); INSERT INTO t VALUES(5, 1);"
                ))
                .unwrap();
        }
        assert_eq!(
            read_rowid_pair(&path),
            vec![vec![Value::from_i64(5), Value::from_i64(5)]],
            "{written}"
        );
    }
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
fn legacy_integer_primary_key_all_quoting_styles_keeps_stored_value() {
    for written in ["\"INTEGER\"", "'INTEGER'", "`INTEGER`"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy-quoted-integer.db");
        {
            let sqlite = rusqlite::Connection::open(&path).unwrap();
            sqlite.execute_batch("CREATE TABLE t(a INT PRIMARY KEY, b); INSERT INTO t VALUES(5, 1); UPDATE t SET rowid = 7; PRAGMA writable_schema = ON;").unwrap();
            sqlite
                .execute(
                    "UPDATE sqlite_schema SET sql = ?1 WHERE name = 't'",
                    [format!("CREATE TABLE t(a {written} PRIMARY KEY, b)")],
                )
                .unwrap();
            sqlite.execute_batch("PRAGMA schema_version = 2;").unwrap();
        }
        assert_eq!(
            read_rowid_pair(&path),
            vec![vec![Value::from_i64(5), Value::from_i64(7)]],
            "{written}"
        );
    }
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
