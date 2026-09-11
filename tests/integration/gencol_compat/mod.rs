use std::{fs, path::Path};

use rusqlite::types::Value;
use tempfile::TempDir;
use turso_core::DatabaseOpts;

use crate::common::{limbo_exec_rows, TempDatabase};

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
