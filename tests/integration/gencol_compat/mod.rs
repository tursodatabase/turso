use std::{fs, path::Path};

use rusqlite::types::Value;
use tempfile::TempDir;
use turso_core::DatabaseOpts;

use crate::common::{limbo_exec_rows, TempDatabase};

#[test]
fn broken_table_name_blocks_materialized_view() {
    check_broken_table_create_name("CREATE MATERIALIZED VIEW BAD AS SELECT * FROM healthy");
}

#[test]
fn broken_table_name_blocks_virtual_table() {
    check_broken_table_create_name("CREATE VIRTUAL TABLE BAD USING csv(data='1')");
}

fn check_broken_table_create_name(sql: &str) {
    let dir = TempDir::new_in("/tmp").unwrap();
    let path = dir.path().join("create.db");
    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("integration/gencol_compat/fixtures/gencol_dup_as_v0.8.0-pre.9.db"),
        &path,
    )
    .unwrap();
    let db = TempDatabase::builder()
        .with_db_path(&path)
        .with_views(true)
        .build();
    let conn = db.connect_limbo();
    let mut api = unsafe { conn._build_turso_ext() };
    let rc = unsafe { limbo_csv::register_extension_static(&mut api) };
    unsafe { conn._free_extension_ctx(api) };
    assert_eq!(rc, turso_ext::ResultCode::OK);
    let err = conn
        .prepare(sql)
        .err()
        .expect("creation must fail before modifying sqlite_schema")
        .to_string();
    assert!(err.contains("already exists"), "{err}");
    conn.execute(&sql.replace(" BAD ", " IF NOT EXISTS BAD "))
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT count(*) FROM sqlite_schema WHERE name='bad'"),
        vec![vec![Value::from(1)]]
    );
    conn.execute("DROP TABLE bad").unwrap();
    conn.execute(sql).unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "PRAGMA integrity_check"),
        vec![vec![Value::Text("ok".into())]]
    );
}

#[test]
fn broken_parent_drop_checks_healthy_child_rows() {
    check_broken_parent_drop(false);
}

#[test]
fn broken_parent_drop_allows_null_child_keys() {
    check_broken_parent_drop(true);
}

fn check_broken_parent_drop(null_only: bool) {
    let dir = TempDir::new_in("/tmp").unwrap();
    let path = dir.path().join("foreign_keys.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite.execute_batch(
        "CREATE TABLE bad(a PRIMARY KEY, b); INSERT INTO bad VALUES (1, 2);
         CREATE TABLE child(x REFERENCES bad(a)); INSERT INTO child VALUES (NULL), (1);
         PRAGMA writable_schema=ON;
         UPDATE sqlite_schema SET sql='CREATE TABLE bad(a PRIMARY KEY, b AS (a) AS (a))' WHERE name='bad';",
    ).unwrap();
    drop(sqlite);
    let db = TempDatabase::builder().with_db_path(&path).build();
    let conn = db.connect_limbo();
    conn.execute("PRAGMA foreign_keys=ON").unwrap();
    let err = conn.execute("DROP TABLE bad").unwrap_err().to_string();
    assert!(err.contains("FOREIGN KEY constraint failed"), "{err}");
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT * FROM child ORDER BY x"),
        vec![vec![Value::Null], vec![Value::from(1)]]
    );
    if null_only {
        conn.execute("DELETE FROM child WHERE x IS NOT NULL")
            .unwrap();
    } else {
        conn.execute("DELETE FROM child").unwrap();
    }
    conn.execute("DROP TABLE BAD").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "PRAGMA integrity_check"),
        vec![vec![Value::Text("ok".into())]]
    );
}

#[test]
fn broken_table_can_be_dropped_under_mvcc() {
    let dir = TempDir::new_in("/tmp").unwrap();
    let path = dir.path().join("mvcc.db");
    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("integration/gencol_compat/fixtures/gencol_dup_as_v0.8.0-pre.9.db"),
        &path,
    )
    .unwrap();
    {
        let db = TempDatabase::builder().with_db_path(&path).build();
        let conn = db.connect_limbo();
        conn.execute("PRAGMA journal_mode=mvcc").unwrap();
        conn.execute("DROP TABLE bad").unwrap();
        assert_eq!(
            limbo_exec_rows(&conn, "SELECT * FROM healthy"),
            vec![vec![Value::from(1)]]
        );
        assert_eq!(
            limbo_exec_rows(&conn, "PRAGMA integrity_check"),
            vec![vec![Value::Text("ok".into())]]
        );
        conn.execute("PRAGMA journal_mode=wal").unwrap();
    }
    let db = TempDatabase::builder().with_db_path(&path).build();
    let conn = db.connect_limbo();
    assert!(
        limbo_exec_rows(&conn, "SELECT name FROM sqlite_schema WHERE tbl_name='bad'").is_empty()
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT * FROM healthy"),
        vec![vec![Value::from(1)]]
    );
    assert_eq!(
        limbo_exec_rows(&conn, "PRAGMA integrity_check"),
        vec![vec![Value::Text("ok".into())]]
    );
}

#[test]
fn broken_table_index_without_root_can_be_dropped() {
    let dir = TempDir::new_in("/tmp").unwrap();
    let path = dir.path().join("rootless.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite
        .execute_batch(
            "CREATE TABLE healthy(x); INSERT INTO healthy VALUES (1);
         CREATE TABLE bad(a, b);
         PRAGMA writable_schema=ON;
         UPDATE sqlite_schema SET sql='CREATE TABLE bad(a, b AS (a) AS (a))' WHERE name='bad';
         INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql)
         VALUES ('index','bad_fts','bad',0,'CREATE INDEX bad_fts ON bad(a)');",
        )
        .unwrap();
    drop(sqlite);
    let db = TempDatabase::builder().with_db_path(&path).build();
    let conn = db.connect_limbo();
    conn.with_schema_mut(|schema| {
        assert!(schema.broken_tables["bad"].index_root_pages.is_empty());
        assert_eq!(schema.broken_tables["bad"].index_names, vec!["bad_fts"]);
    })
    .unwrap();
    conn.execute("DROP TABLE bad").unwrap();
    assert!(
        limbo_exec_rows(&conn, "SELECT name FROM sqlite_schema WHERE tbl_name='bad'").is_empty()
    );
    assert_eq!(
        limbo_exec_rows(&conn, "PRAGMA integrity_check"),
        vec![vec![Value::Text("ok".into())]]
    );
}

#[test]
fn broken_autoincrement_table_removes_sequence_backing_table() {
    let dir = TempDir::new_in("/tmp").unwrap();
    let path = dir.path().join("autoincrement.db");
    {
        let db = TempDatabase::builder().with_db_path(&path).build();
        let conn = db.connect_limbo();
        conn.execute("CREATE TABLE bad(id INTEGER PRIMARY KEY AUTOINCREMENT, a)")
            .unwrap();
        conn.execute("INSERT INTO bad(a) VALUES (1)").unwrap();
        assert_eq!(
            limbo_exec_rows(
                &conn,
                "SELECT count(*) FROM sqlite_schema WHERE name LIKE '%autoincrement_bad%'"
            ),
            vec![vec![Value::from(1)]]
        );
    }
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite.execute_batch(
        "PRAGMA writable_schema=ON;
         UPDATE sqlite_schema SET sql='CREATE TABLE bad(id INTEGER PRIMARY KEY AUTOINCREMENT, a AS (id) AS (id))' WHERE name='bad';",
    ).unwrap();
    drop(sqlite);
    let db = TempDatabase::builder().with_db_path(&path).build();
    let conn = db.connect_limbo();
    conn.execute("DROP TABLE bad").unwrap();
    assert!(limbo_exec_rows(
        &conn,
        "SELECT name FROM sqlite_schema WHERE name LIKE '%autoincrement_bad%'"
    )
    .is_empty());
    assert_eq!(
        limbo_exec_rows(&conn, "PRAGMA integrity_check"),
        vec![vec![Value::Text("ok".into())]]
    );
}

#[test]
fn duplicate_as_table_can_be_dropped() {
    check_fixture("gencol_dup_as_v0.8.0-pre.9.db");
}

#[test]
fn default_on_generated_table_can_be_dropped() {
    check_fixture("gencol_default_on_generated_v0.8.0-pre.8.db");
}

#[test]
fn broken_table_indexes_are_removed() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("indexed.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE healthy(x); INSERT INTO healthy VALUES (1);
         CREATE TABLE bad(a UNIQUE, b); INSERT INTO bad VALUES (1, 2);
         CREATE INDEX bad_b ON bad(b);
         PRAGMA writable_schema=ON;
         UPDATE sqlite_schema SET sql='CREATE TABLE bad(a UNIQUE, b AS (a) AS (a))' WHERE name='bad';",
    )
    .unwrap();
    drop(conn);
    check_database(&path);
}

#[test]
fn broken_table_index_names_are_reserved() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("reserved.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite.execute_batch(
        "CREATE TABLE healthy(x); INSERT INTO healthy VALUES (1);
         CREATE TABLE bad(a UNIQUE, b); CREATE INDEX bad_b ON bad(b);
         INSERT INTO bad VALUES (1, 2), (2, zeroblob(20000));
         DELETE FROM bad WHERE a=2;
         PRAGMA writable_schema=ON;
         UPDATE sqlite_schema SET sql='CREATE TABLE bad(a UNIQUE, b AS (a) AS (a))' WHERE name='bad';",
    ).unwrap();
    assert!(
        sqlite
            .query_row("PRAGMA freelist_count", [], |row| row.get::<_, i64>(0))
            .unwrap()
            > 0
    );
    drop(sqlite);
    let db = TempDatabase::builder().with_db_path(&path).build();
    let conn = db.connect_limbo();
    for sql in [
        "CREATE INDEX bad_b ON healthy(x)",
        "CREATE INDEX BAD_B ON healthy(x)",
    ] {
        let err = conn.execute(sql).unwrap_err().to_string();
        assert!(err.contains("already exists"), "{err}");
    }
    conn.execute("CREATE INDEX IF NOT EXISTS bad_b ON healthy(x)")
        .unwrap();
    for (sql, message) in [
        ("CREATE TABLE bad_b(x)", "already an index"),
        (
            "ALTER TABLE healthy RENAME TO bad_b",
            "already another table or index",
        ),
    ] {
        let err = conn.execute(sql).unwrap_err().to_string();
        assert!(err.contains(message), "{err}");
    }
    conn.execute("DROP TABLE bad").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT * FROM healthy"),
        vec![vec![Value::from(1)]]
    );
    assert_eq!(
        limbo_exec_rows(&conn, "PRAGMA integrity_check"),
        vec![vec![Value::Text("ok".into())]]
    );
}

#[test]
fn broken_table_index_roots_do_not_grow_on_reparse() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("reparse.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite.execute_batch(
        "CREATE TABLE bad(a UNIQUE, b); CREATE INDEX bad_b ON bad(b);
         PRAGMA writable_schema=ON;
         UPDATE sqlite_schema SET sql='CREATE TABLE bad(a UNIQUE, b AS (a) AS (a))' WHERE name='bad';",
    ).unwrap();
    drop(sqlite);
    let db = TempDatabase::builder().with_db_path(&path).build();
    let conn = db.connect_limbo();
    conn.with_schema_mut(|schema| {
        let roots = schema.broken_tables["bad"].index_root_pages.clone();
        let names = schema.broken_tables["bad"].index_names.clone();
        assert_eq!(names.len(), 2);
        for _ in 0..3 {
            let mut indexes = Vec::new();
            let mut automatic = Default::default();
            schema
                .handle_schema_row(
                    "index",
                    "bad_b",
                    "bad",
                    roots[0],
                    Some("CREATE INDEX bad_b ON bad(b)"),
                    &Default::default(),
                    &mut indexes,
                    &mut automatic,
                    &mut Default::default(),
                    &mut Default::default(),
                    &mut Default::default(),
                    &|_| None,
                    &turso_core::dialect::sqlite::SqliteDialect,
                )
                .unwrap();
            automatic.insert(
                "bad".into(),
                vec![("sqlite_autoindex_bad_1".into(), roots[1])],
            );
            schema
                .populate_indices(&Default::default(), indexes, automatic, false)
                .unwrap();
            assert_eq!(schema.broken_tables["bad"].index_root_pages, roots);
            assert_eq!(schema.broken_tables["bad"].index_names, names);
        }
    })
    .unwrap();
}

#[test]
fn broken_table_drop_preserves_shared_root() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("shared.db");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite
        .execute_batch(
            "CREATE TABLE healthy(x); INSERT INTO healthy VALUES (1), (2), (3);
         PRAGMA writable_schema=ON;
         INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql)
         SELECT 'table','bad','bad',rootpage,'CREATE TABLE bad(a AS (1) AS (2))'
         FROM sqlite_schema WHERE name='healthy';",
        )
        .unwrap();
    drop(sqlite);
    let db = TempDatabase::builder().with_db_path(&path).build();
    let conn = db.connect_limbo();
    conn.execute("DROP TABLE bad").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT * FROM healthy ORDER BY x"),
        vec![
            vec![Value::from(1)],
            vec![Value::from(2)],
            vec![Value::from(3)]
        ]
    );
    assert_eq!(
        limbo_exec_rows(&conn, "PRAGMA integrity_check"),
        vec![vec![Value::Text("ok".into())]]
    );
}

#[test]
fn broken_table_non_parse_error_is_propagated() {
    let mut schema = turso_core::schema::Schema::new();
    let err = schema
        .handle_schema_row(
            "table",
            "bad",
            "bad",
            2,
            Some("SELECT 1"),
            &Default::default(),
            &mut Default::default(),
            &mut Default::default(),
            &mut Default::default(),
            &mut Default::default(),
            &mut Default::default(),
            &|_| None,
            &turso_core::dialect::sqlite::SqliteDialect,
        )
        .unwrap_err();
    assert!(matches!(err, turso_core::LimboError::Corrupt(_)), "{err}");
}

#[test]
fn strict_any_index_can_be_rebuilt() {
    for recovery in [
        "REINDEX main.tg",
        "REINDEX t",
        "REINDEX tg",
        "DROP INDEX tg; CREATE INDEX tg ON t(g)",
    ] {
        with_fixture("gencol_strict_any_index_v0.8.0-pre.9.db", |db| {
            let conn = db.connect_limbo();
            assert_eq!(
                limbo_exec_rows(&conn, "PRAGMA integrity_check"),
                vec![vec![Value::Text("row 1 missing from index tg".into())]]
            );
            for sql in recovery.split(';') {
                conn.execute(sql).unwrap();
            }
            assert_eq!(
                limbo_exec_rows(&conn, "PRAGMA integrity_check"),
                vec![vec![Value::Text("ok".into())]],
                "{recovery}"
            );
            assert_eq!(
                limbo_exec_rows(&conn, "SELECT id FROM t WHERE g='42'"),
                vec![vec![Value::from(1)]]
            );
        });
    }
}

#[test]
fn stale_unique_index_key_is_detected() {
    with_fixture("gencol_replace_corrupt_unique_v0.8.0-pre.9.db", |db| {
        let conn = db.connect_limbo();
        let expected = vec![vec![Value::from(1), Value::from(5), Value::from(6)]];
        assert_eq!(limbo_exec_rows(&conn, "SELECT rowid,a,b FROM t"), expected);
        assert_eq!(
            limbo_exec_rows(&conn, "PRAGMA integrity_check"),
            vec![vec![Value::Text(
                "row 1 missing from index sqlite_autoindex_t_1".into()
            )]]
        );
        assert_eq!(limbo_exec_rows(&conn, "SELECT rowid,a,b FROM t"), expected);
    });
}

#[test]
fn rowvalue_collate_indexes_pass_integrity_check() {
    with_fixture("gencol_rowvalue_collate_index_v0.8.0-pre.9.db", |db| {
        let conn = db.connect_limbo();
        assert_eq!(
            limbo_exec_rows(&conn, "PRAGMA integrity_check"),
            vec![vec![Value::Text("ok".into())]]
        );
        assert_eq!(
            limbo_exec_rows(&conn, "SELECT count(*) FROM t"),
            vec![vec![Value::from(1)]]
        );
    });
}

#[test]
fn strict_illtyped_generated_row_can_be_repaired_or_deleted() {
    with_fixture("gencol_strict_illtyped_row_v0.8.0-pre.9.db", |db| {
        let conn = db.connect_limbo();
        assert_eq!(
            limbo_exec_rows(&conn, "SELECT id,a FROM t"),
            vec![vec![Value::from(1), Value::Text("abc".into())]]
        );
        assert_eq!(
            limbo_exec_rows(&conn, "PRAGMA integrity_check"),
            vec![vec![Value::Text("non-INTEGER value in t.b".into())]]
        );
        let err = conn
            .execute("UPDATE t SET other=1")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("cannot store TEXT value in INTEGER column"),
            "{err}"
        );
    });
    for recovery in ["UPDATE t SET a='42'", "DELETE FROM t WHERE id=1"] {
        with_fixture("gencol_strict_illtyped_row_v0.8.0-pre.9.db", |db| {
            let conn = db.connect_limbo();
            conn.execute(recovery).unwrap();
            assert_eq!(
                limbo_exec_rows(&conn, "PRAGMA integrity_check"),
                vec![vec![Value::Text("ok".into())]],
                "{recovery}"
            );
        });
    }
}

fn with_fixture(name: &str, check: impl FnOnce(&TempDatabase)) {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("integration/gencol_compat/fixtures")
        .join(name);
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("compat.db");
    fs::copy(fixture, &path).unwrap();
    let db = TempDatabase::builder()
        .with_db_path(&path)
        .with_opts(DatabaseOpts::new().with_generated_columns(true))
        .build();
    check(&db);
}

fn check_fixture(name: &str) {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("integration/gencol_compat/fixtures")
        .join(name);
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("compat.db");
    fs::copy(fixture, &path).unwrap();
    check_database(&path);
}

fn check_database(path: &Path) {
    let db = TempDatabase::builder()
        .with_db_path(path)
        .with_opts(DatabaseOpts::new().with_generated_columns(true))
        .build();
    let conn = db.connect_limbo();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ),
        vec![
            vec![Value::Text("bad".into())],
            vec![Value::Text("healthy".into())]
        ]
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT * FROM healthy"),
        vec![vec![Value::from(1)]]
    );
    for sql in [
        "SELECT * FROM bad",
        "INSERT INTO bad VALUES (1, 2)",
        "UPDATE bad SET a=2",
        "DELETE FROM bad",
        "ALTER TABLE bad RENAME TO other",
        "CREATE INDEX bad_a ON bad(a)",
        "CREATE TRIGGER bad_trigger AFTER INSERT ON bad BEGIN SELECT 1; END",
        "ANALYZE bad",
    ] {
        let err = conn.execute(sql).unwrap_err().to_string();
        assert!(
            err.contains("could not be loaded") && err.contains("DROP TABLE"),
            "{sql}: {err}"
        );
    }
    for (sql, message) in [
        ("CREATE TABLE bad(z)", "already exists"),
        (
            "ALTER TABLE healthy RENAME TO bad",
            "already another table or index",
        ),
    ] {
        let err = conn.execute(sql).unwrap_err().to_string();
        assert!(err.contains(message), "{sql}: {err}");
    }
    // The quarantined table still owns its pages; integrity_check must not
    // report them as unused while it awaits the DROP.
    assert_eq!(
        limbo_exec_rows(&conn, "PRAGMA integrity_check"),
        vec![vec![Value::Text("ok".into())]]
    );
    conn.execute("DROP TABLE bad").unwrap();
    conn.execute("CREATE TABLE bad(z)").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "PRAGMA integrity_check"),
        vec![vec![Value::Text("ok".into())]]
    );
    conn.execute("DROP TABLE bad").unwrap();
    drop(conn);
    drop(db);
    let db = TempDatabase::builder()
        .with_db_path(path)
        .with_opts(DatabaseOpts::new().with_generated_columns(true))
        .build();
    let conn = db.connect_limbo();
    assert!(
        limbo_exec_rows(&conn, "SELECT name FROM sqlite_master WHERE tbl_name='bad'").is_empty()
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT * FROM healthy"),
        vec![vec![Value::from(1)]]
    );
    assert_eq!(
        limbo_exec_rows(&conn, "PRAGMA integrity_check"),
        vec![vec![Value::Text("ok".into())]]
    );
}
