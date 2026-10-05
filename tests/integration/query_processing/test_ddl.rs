use crate::common::{rusqlite_integrity_check, ExecRows, TempDatabase};
use asserting::prelude::*;

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn test_fail_drop_indexed_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE INDEX i ON t (a)")?;
    assert_that!(conn.execute("ALTER TABLE t DROP COLUMN a")).is_err();
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a UNIQUE, b);")]
fn test_fail_drop_unique_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    assert_that!(conn.execute("ALTER TABLE t DROP COLUMN a")).is_err();
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b, UNIQUE(a, b));")]
fn test_fail_drop_compound_unique_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    assert_that!(conn.execute("ALTER TABLE t DROP COLUMN a")).is_err();
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a PRIMARY KEY, b);")]
fn test_fail_drop_primary_key_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    assert_that!(conn.execute("ALTER TABLE t DROP COLUMN a")).is_err();
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b, PRIMARY KEY(a, b));")]
fn test_fail_drop_compound_primary_key_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    assert_that!(conn.execute("ALTER TABLE t DROP COLUMN a")).is_err();
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn test_fail_drop_partial_index_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE INDEX i ON t (b) WHERE a > 0")?;
    assert_that!(conn.execute("ALTER TABLE t DROP COLUMN a")).is_err();
    Ok(())
}

#[test]
fn test_drop_sqlite_created_mixed_case_index() -> anyhow::Result<()> {
    for drop_sql in [
        "DROP INDEX Idx_A",
        "DROP INDEX idx_a",
        "DROP INDEX \"Idx_A\"",
        "DROP INDEX IF EXISTS Idx_A",
    ] {
        let tmp_dir = tempfile::TempDir::new()?;
        let db_path = tmp_dir.path().join("test.db");
        rusqlite::Connection::open(&db_path)?.execute_batch(
            "CREATE TABLE t(a); CREATE INDEX Idx_A ON t(a); INSERT INTO t VALUES(7)",
        )?;

        let db = TempDatabase::new_with_existent(&db_path);
        db.connect_limbo().execute(drop_sql)?;
        drop(db);

        let sqlite = rusqlite::Connection::open(&db_path)?;
        let indexes: i64 = sqlite.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'index'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(indexes, 0, "{drop_sql}");
        sqlite.execute_batch("CREATE TABLE t2(b); INSERT INTO t2 VALUES(9)")?;
        drop(sqlite);
        crate::common::rusqlite_integrity_check(&db_path)?;
    }
    Ok(())
}

#[turso_macros::test]
fn test_alter_column_rewrites_indexed_affinity_change(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA journal_mode = WAL")?;
    conn.execute("CREATE TABLE t(x NUMERIC)")?;
    conn.execute("CREATE INDEX idx_x ON t(x)")?;
    conn.execute("INSERT INTO t VALUES (10), (2), (30)")?;

    conn.execute("ALTER TABLE t ALTER COLUMN x TO y TEXT")?;

    let rows: Vec<(String, String)> = conn.exec_rows("SELECT y, typeof(y) FROM t ORDER BY rowid");
    assert_eq!(
        rows,
        vec![
            ("10".to_string(), "text".to_string()),
            ("2".to_string(), "text".to_string()),
            ("30".to_string(), "text".to_string()),
        ]
    );

    let indexed_rows: Vec<(String,)> =
        conn.exec_rows("SELECT y FROM t INDEXED BY idx_x WHERE y >= '0' ORDER BY y");
    assert_eq!(
        indexed_rows,
        vec![("10".to_string(),), ("2".to_string(),), ("30".to_string(),),]
    );

    let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    assert_eq!(integrity, vec![("ok".to_string(),)]);

    assert_that!(conn.execute("SELECT x FROM t")).is_err();
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT) STRICT;")]
fn test_alter_column_strict_type_mismatch_keeps_table(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES (1, 'abc')")?;

    assert_that!(conn.execute("ALTER TABLE t ALTER COLUMN v TO v2 INTEGER"))
        .err()
        .display_string()
        .contains("cannot store TEXT value in INTEGER column t.v2");

    let schema: Vec<(String,)> = conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 't'");
    assert_eq!(
        schema,
        vec![("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT) STRICT".to_string(),)]
    );
    let rows: Vec<(i64, String, String)> = conn.exec_rows("SELECT id, v, typeof(v) FROM t");
    assert_eq!(rows, vec![(1, "abc".to_string(), "text".to_string())]);
    let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    assert_eq!(integrity, vec![("ok".to_string(),)]);
    Ok(())
}

#[turso_macros::test(
    mvcc,
    init_sql = "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT) STRICT;"
)]
fn test_alter_column_strict_int_to_integer_keeps_rows_and_index(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE INDEX t_v ON t(v)")?;
    conn.execute("INSERT INTO t VALUES (1, 5)")?;

    conn.execute("ALTER TABLE t ALTER COLUMN v TO v2 INTEGER")?;

    let rows: Vec<(i64, i64, String)> =
        conn.exec_rows("SELECT id, v2, typeof(v2) FROM t WHERE v2 = 5");
    assert_eq!(rows, vec![(1, 5, "integer".to_string())]);
    let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    assert_eq!(integrity, vec![("ok".to_string(),)]);
    Ok(())
}

#[turso_macros::test(mvcc, init_sql = "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);")]
fn test_alter_column_rejects_rows_failing_new_constraints(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES (1, -1), (2, NULL)")?;

    assert_that!(conn.execute("ALTER TABLE t ALTER COLUMN v TO v2 INTEGER CHECK(v2 > 0)"))
        .err()
        .display_string()
        .contains("CHECK constraint failed");
    assert_that!(conn.execute("ALTER TABLE t ALTER COLUMN v TO v2 INTEGER NOT NULL"))
        .err()
        .display_string()
        .contains("NOT NULL constraint failed");

    let schema: Vec<(String,)> = conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 't'");
    assert_eq!(
        schema,
        vec![("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)".to_string(),)]
    );
    let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    assert_eq!(integrity, vec![("ok".to_string(),)]);

    conn.execute("UPDATE t SET v = 5 WHERE id = 2")?;
    conn.execute("ALTER TABLE t ALTER COLUMN v TO v2 INTEGER NOT NULL CHECK(v2 < id + 10)")?;
    let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, v2 FROM t");
    assert_eq!(rows, vec![(1, -1), (2, 5)]);
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);")]
fn test_alter_column_rejects_check_on_missing_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    assert_that!(conn.execute("ALTER TABLE t ALTER COLUMN v TO v2 INTEGER CHECK(nosuch > 0)"))
        .err()
        .display_string()
        .contains("no such column: nosuch");
    assert_that!(conn.execute("ALTER TABLE t ALTER COLUMN v TO v2 INTEGER CHECK(v > 0)"))
        .err()
        .display_string()
        .contains("no such column: v");

    let schema: Vec<(String,)> = conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 't'");
    assert_eq!(
        schema,
        vec![("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)".to_string(),)]
    );
    conn.execute("INSERT INTO t VALUES (1, 1)")?;

    conn.execute("ALTER TABLE t ALTER COLUMN v TO v2 INTEGER CHECK(v2 > 0)")?;
    assert_that!(conn.execute("INSERT INTO t VALUES (2, 0)"))
        .err()
        .display_string()
        .contains("CHECK constraint failed");
    conn.execute("INSERT INTO t VALUES (3, 3)")?;

    let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, v2 FROM t ORDER BY id");
    assert_eq!(rows, vec![(1, 1), (3, 3)]);
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn test_fail_drop_view_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE VIEW v AS SELECT a, b FROM t")?;
    assert_that!(conn.execute("ALTER TABLE t DROP COLUMN a")).is_err();
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn test_rename_view_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE VIEW v AS SELECT a, b FROM t")?;
    conn.execute("INSERT INTO t VALUES (1, 2)")?;
    conn.execute("ALTER TABLE t RENAME a TO c")?;
    let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT * FROM v");
    assert_eq!(rows, vec![(1, 2)]);
    let sql: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type = 'view' AND name = 'v'");
    assert_eq!(
        sql,
        vec![("CREATE VIEW v AS SELECT c, b FROM t".to_string(),)]
    );
    Ok(())
}

#[turso_macros::test(
    init_sql = "CREATE TABLE t (pk INTEGER PRIMARY KEY, indexed INTEGER, viewed INTEGER, partial INTEGER, compound1 INTEGER, compound2 INTEGER, unused1 INTEGER, unused2 INTEGER, unused3 INTEGER);"
)]
fn test_allow_drop_unreferenced_columns(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE INDEX idx ON t(indexed)")?;
    conn.execute("CREATE VIEW v AS SELECT viewed FROM t")?;
    conn.execute("CREATE INDEX partial_idx ON t(compound1) WHERE partial > 0")?;
    conn.execute("CREATE INDEX compound_idx ON t(compound1, compound2)")?;

    // Should be able to drop unused columns
    conn.execute("ALTER TABLE t DROP COLUMN unused1")?;
    conn.execute("ALTER TABLE t DROP COLUMN unused2")?;
    conn.execute("ALTER TABLE t DROP COLUMN unused3")?;

    Ok(())
}

#[turso_macros::test]
fn test_create_table_without_rowid_supported(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(b INTEGER, a TEXT PRIMARY KEY, c TEXT) WITHOUT ROWID")?;

    let sql: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 't'");
    assert_eq!(
        sql,
        vec![("CREATE TABLE t (b INTEGER, a TEXT PRIMARY KEY, c TEXT) WITHOUT ROWID".to_string(),)]
    );
    Ok(())
}

#[turso_macros::test]
fn test_create_table_without_rowid_composite_pk_supported(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(a TEXT, b INT, PRIMARY KEY(a, b)) WITHOUT ROWID")?;
    Ok(())
}

#[turso_macros::test]
fn test_create_table_without_rowid_requires_primary_key(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    assert_that!(conn.execute("CREATE TABLE t(a TEXT, b INT) WITHOUT ROWID"))
        .err()
        .display_string()
        .contains("PRIMARY KEY");
    Ok(())
}

#[turso_macros::test]
fn test_create_table_without_rowid_rejects_secondary_unique(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    assert_that!(
        conn.execute("CREATE TABLE t(a TEXT PRIMARY KEY, b INT UNIQUE, c TEXT) WITHOUT ROWID")
    )
    .err()
    .display_string()
    .contains("secondary UNIQUE constraints on WITHOUT ROWID tables are not supported");
    Ok(())
}

#[turso_macros::test]
fn test_create_table_without_rowid_rejects_autoincrement(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    assert_that!(conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT) WITHOUT ROWID"))
        .err()
        .display_string()
        .contains("AUTOINCREMENT is not allowed on WITHOUT ROWID tables");
    Ok(())
}

#[turso_macros::test]
fn test_fail_not_null_in_upsert(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER NOT NULL, c TEXT NOT NULL);")?;
    conn.execute("INSERT INTO t VALUES (1, 10, 'first');")?;

    assert_that!(conn.execute("INSERT INTO t VALUES (1, NULL, 'second') ON CONFLICT(a) DO UPDATE SET b = excluded.b, c = excluded.c;"))
        .err()
        .display_string()
        .contains("t.b");
    Ok(())
}

/// test which simulation situation when prepared statement is used within a transaction which changed schema itself
/// in this case DB must not use database schema - but instead use connection schema
#[turso_macros::test]
fn test_prepared_stmt_reprepare_ddl_change_txn(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(x);")?;
    let mut stmt = conn.prepare("INSERT INTO t VALUES (1)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("CREATE TABLE q(x)").unwrap();
    stmt.run_ignore_rows().unwrap();
    conn.execute("COMMIT").unwrap();

    Ok(())
}

/// Older Turso versions stored CREATE VIEW column lists without identifier
/// quoting, leaving sqlite_schema rows whose SQL no longer parses (and which
/// real SQLite refuses to load entirely). Such rows are tolerated at schema
/// load and must be removable with DROP VIEW so affected databases can be
/// cleaned up and the name reused.
///
/// The fixture was generated by a pre-fix tursodb running
/// `CREATE VIEW v([col one]) AS SELECT a FROM t`. Read-only assertions on
/// the same fixture live in
/// sqlite/conformance/turso-sqltests/legacy-unquoted-view-columns.sqltest.
#[test]
fn test_drop_broken_legacy_view_row() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../sqlite/conformance/database/testing_legacy_unquoted_view_columns.db");
    let tmp_dir = tempfile::TempDir::new()?;
    let db_path = tmp_dir.path().join("legacy_view.db");
    std::fs::copy(&fixture, &db_path)?;

    let db = TempDatabase::builder().with_db_path(&db_path).build();
    let conn = db.connect_limbo();

    // The table is readable; the broken view is unavailable with a
    // diagnosable error; CREATE VIEW over the name is blocked.
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT a FROM t");
    assert_eq!(rows, vec![(42,)]);
    assert_that!(conn.execute("SELECT * FROM v"))
        .err()
        .display_string()
        .contains("could not be loaded");
    assert_that!(conn.execute("CREATE VIEW v AS SELECT a FROM t"))
        .err()
        .display_string()
        .contains("already exists");

    // DROP VIEW removes the orphaned row and frees the name.
    conn.execute("DROP VIEW v")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM sqlite_master WHERE name = 'v'");
    assert_eq!(rows, vec![(0,)]);
    conn.execute("CREATE VIEW v(\"col one\") AS SELECT a FROM t")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT \"col one\" FROM v");
    assert_eq!(rows, vec![(42,)]);
    Ok(())
}

#[test]
fn test_drop_sqlite_created_mixed_case_table() -> anyhow::Result<()> {
    let cases = [
        (
            "CREATE TABLE T1(a); INSERT INTO T1 VALUES(7)",
            "DROP TABLE T1",
        ),
        ("CREATE TABLE T1(a)", "DROP TABLE t1"),
        ("CREATE TABLE T1(a)", "DROP TABLE \"T1\""),
        ("CREATE TABLE T1(a)", "DROP TABLE IF EXISTS T1"),
        (
            "CREATE TABLE T1(a); CREATE INDEX I1 ON T1(a); \
             CREATE TRIGGER Tr1 AFTER INSERT ON T1 BEGIN SELECT 1; END; \
             INSERT INTO T1 VALUES(7)",
            "DROP TABLE T1",
        ),
    ];

    for (setup, drop_sql) in cases {
        let (_tmp_dir, db_path) = create_database_with_sqlite(setup)?;
        let db = TempDatabase::builder().with_db_path(&db_path).build();
        db.connect_limbo().execute(drop_sql)?;
        drop(db);

        let sqlite = rusqlite::Connection::open(&db_path)?;
        let remaining: i64 =
            sqlite.query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))?;
        assert_eq!(remaining, 0, "{drop_sql}");
        sqlite.execute_batch("CREATE TABLE t2(b); INSERT INTO t2 VALUES(9)")?;
        drop(sqlite);
        rusqlite_integrity_check(&db_path)?;
    }
    Ok(())
}

#[test]
fn test_drop_sqlite_created_mixed_case_autoincrement_table() -> anyhow::Result<()> {
    let (_tmp_dir, db_path) = create_database_with_sqlite(
        "CREATE TABLE T1(a INTEGER PRIMARY KEY AUTOINCREMENT, b); \
         INSERT INTO T1(a, b) VALUES(99, 1)",
    )?;
    let db = TempDatabase::builder().with_db_path(&db_path).build();
    let conn = db.connect_limbo();
    conn.execute("INSERT INTO T1(b) VALUES(2)")?;
    conn.execute("DROP TABLE T1")?;
    drop(conn);
    drop(db);

    let sqlite = rusqlite::Connection::open(&db_path)?;
    let remaining: i64 =
        sqlite.query_row("SELECT count(*) FROM sqlite_sequence", [], |row| row.get(0))?;
    assert_eq!(remaining, 0);
    drop(sqlite);

    let db = TempDatabase::builder().with_db_path(&db_path).build();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t1(a INTEGER PRIMARY KEY AUTOINCREMENT, b)")?;
    conn.execute("INSERT INTO t1(b) VALUES(3)")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT a FROM t1");
    assert_eq!(rows, vec![(1,)]);
    Ok(())
}

fn create_database_with_sqlite(
    sql: &str,
) -> anyhow::Result<(tempfile::TempDir, std::path::PathBuf)> {
    let tmp_dir = tempfile::TempDir::new()?;
    let db_path = tmp_dir.path().join("test.db");
    rusqlite::Connection::open(&db_path)?.execute_batch(sql)?;
    Ok((tmp_dir, db_path))
}

#[test]
fn test_unknown_collation_fails_only_writes_that_need_it() -> anyhow::Result<()> {
    let (_tmp_dir, db) = copy_unknown_collation_fixture()?;
    let conn = db.connect_limbo();

    assert_that!(conn.execute("INSERT INTO t1 VALUES ('c', 3)"))
        .is_err()
        .err()
        .display_string()
        .contains("no such collation sequence: reverse");
    assert_that!(conn.execute("DELETE FROM t1 WHERE b = 2"))
        .is_err()
        .err()
        .display_string()
        .contains("no such collation sequence: reverse");
    assert_that!(conn.execute("INSERT INTO t3 VALUES ('c')"))
        .is_err()
        .err()
        .display_string()
        .contains("no such collation sequence: reverse");
    assert_that!(conn.execute("UPDATE t1 SET a = 'z' WHERE b = 2"))
        .is_err()
        .err()
        .display_string()
        .contains("no such collation sequence: reverse");
    assert_that!(conn.execute("INSERT INTO t2 VALUES ('c', 3)"))
        .is_err()
        .err()
        .display_string()
        .contains("no such collation sequence: reverse");
    assert_that!(conn.execute("DELETE FROM t2 WHERE b = 1"))
        .is_err()
        .err()
        .display_string()
        .contains("no such collation sequence: reverse");
    conn.execute("UPDATE t1 SET b = 3 WHERE b = 1")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT b FROM t1 ORDER BY b");
    assert_eq!(rows, vec![(2,), (3,)]);
    Ok(())
}

#[test]
fn test_alter_table_keeps_unknown_collation() -> anyhow::Result<()> {
    let (_tmp_dir, db) = copy_unknown_collation_fixture()?;
    let conn = db.connect_limbo();

    conn.execute("ALTER TABLE t1 ADD COLUMN c")?;
    let rows: Vec<(String,)> = conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 't1'");
    assert_that!(rows[0].0.to_lowercase()).contains("collate reverse");

    conn.execute("ALTER TABLE t3 ADD COLUMN b")?;
    let rows: Vec<(String,)> = conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 't3'");
    assert_that!(rows[0].0.to_lowercase()).contains("unique (a collate reverse)");
    Ok(())
}

#[test]
fn test_registered_collation_in_schema_is_used_only_outside_indexes() -> anyhow::Result<()> {
    let (_tmp_dir, db) = copy_unknown_collation_fixture()?;
    let conn = db.connect_limbo();
    conn.register_external_collation("reverse".to_string(), 0, reverse_collation, None);

    let rows: Vec<(String,)> = conn.exec_rows("SELECT a FROM t1 ORDER BY a");
    assert_eq!(rows, vec![("b".to_string(),), ("a".to_string(),)]);
    for sql in [
        "INSERT INTO t1 VALUES ('c', 3)",
        "SELECT a FROM t1 UNION SELECT a FROM t1",
    ] {
        assert_that!(conn.execute(sql))
            .is_err()
            .err()
            .display_string()
            .contains("custom collations are not supported in indexes or WITHOUT ROWID tables");
    }
    Ok(())
}

#[test]
fn test_without_rowid_table_with_unknown_collation_cannot_be_used() -> anyhow::Result<()> {
    let tmp_dir = tempfile::TempDir::new()?;
    let db_path = tmp_dir.path().join("without_rowid.db");
    let sqlite = rusqlite::Connection::open(&db_path)?;
    sqlite.execute_batch(
        "CREATE TABLE w(a TEXT PRIMARY KEY COLLATE nocase, b) WITHOUT ROWID;
         INSERT INTO w VALUES ('a', 1), ('b', 2);
         CREATE TABLE wc(a TEXT, b, PRIMARY KEY(a COLLATE nocase)) WITHOUT ROWID;
         INSERT INTO wc VALUES ('a', 1), ('b', 2);
         PRAGMA writable_schema = ON;
         UPDATE sqlite_schema
            SET sql = replace(sql, 'nocase', 'reverse')
          WHERE name IN ('w', 'wc');",
    )?;
    drop(sqlite);
    let db = TempDatabase::builder().with_db_path(&db_path).build();
    let conn = db.connect_limbo();

    for table in ["w", "wc"] {
        assert_that!(conn.execute(format!("SELECT * FROM {table}")))
            .is_err()
            .err()
            .display_string()
            .contains("no such collation sequence: reverse");
        assert_that!(conn.execute(format!("INSERT INTO {table} VALUES ('c', 3)")))
            .is_err()
            .err()
            .display_string()
            .contains("no such collation sequence: reverse");
    }

    conn.register_external_collation("reverse".to_string(), 0, reverse_collation, None);
    for table in ["w", "wc"] {
        assert_that!(conn.execute(format!("INSERT INTO {table} VALUES ('c', 3)")))
            .is_err()
            .err()
            .display_string()
            .contains("custom collations are not supported in indexes or WITHOUT ROWID tables");
    }
    Ok(())
}

#[test]
fn test_without_rowid_key_uses_primary_key_collation_over_column_collation() -> anyhow::Result<()> {
    let tmp_dir = tempfile::TempDir::new()?;
    let db_path = tmp_dir.path().join("without_rowid.db");
    let sqlite = rusqlite::Connection::open(&db_path)?;
    sqlite.execute_batch(
        "CREATE TABLE w(a TEXT COLLATE nocase, b, PRIMARY KEY(a COLLATE binary)) WITHOUT ROWID;
         INSERT INTO w VALUES ('a', 1), ('b', 2);
         PRAGMA writable_schema = ON;
         UPDATE sqlite_schema SET sql = replace(sql, 'nocase', 'reverse') WHERE name = 'w';",
    )?;
    drop(sqlite);
    let db = TempDatabase::builder().with_db_path(&db_path).build();
    let conn = db.connect_limbo();

    conn.execute("INSERT INTO w VALUES ('c', 3)")?;
    let rows: Vec<(String, i64)> = conn.exec_rows("SELECT a, b FROM w");
    assert_eq!(
        rows,
        vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("c".to_string(), 3)
        ]
    );
    Ok(())
}

unsafe extern "C" fn reverse_collation(
    _context: usize,
    left_ptr: *const u8,
    left_len: usize,
    right_ptr: *const u8,
    right_len: usize,
) -> i32 {
    let left = unsafe { std::slice::from_raw_parts(left_ptr, left_len) };
    let right = unsafe { std::slice::from_raw_parts(right_ptr, right_len) };
    match right.cmp(left) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

fn copy_unknown_collation_fixture() -> anyhow::Result<(tempfile::TempDir, TempDatabase)> {
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../sqlite/conformance/database/unknown_collation.db");
    let tmp_dir = tempfile::TempDir::new()?;
    let db_path = tmp_dir.path().join("unknown_collation.db");
    std::fs::copy(&fixture, &db_path)?;
    let db = TempDatabase::builder().with_db_path(&db_path).build();
    Ok((tmp_dir, db))
}
