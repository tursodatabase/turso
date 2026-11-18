use crate::common::{rusqlite_integrity_check, ExecRows, TempDatabase};
use crate::queued_io::QueuedIo;
use asserting::prelude::*;
use std::sync::Arc;
use turso_core::{ConnectOpts, Database, LimboError, SessionRole, StepResult, IO};
use turso_sqlite::SqliteDialect;

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

#[turso_macros::test(mvcc)]
fn test_drop_fails_while_another_statement_is_reading(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, x, b BLOB)")?;
    conn.execute("CREATE INDEX ux ON u(x)")?;
    for id in 1..=32 {
        conn.execute(format!("INSERT INTO u VALUES({id}, {id}, zeroblob(60))"))?;
    }

    for (select_sql, drop_sql) in [
        ("SELECT id FROM u", "DROP TABLE u"),
        ("SELECT x FROM u INDEXED BY ux WHERE x > 0", "DROP INDEX ux"),
        // SQLite refuses any DROP while another statement is reading,
        // even one on a table that statement does not read.
        ("SELECT id FROM u", "DROP TABLE other"),
    ] {
        conn.execute("CREATE TABLE IF NOT EXISTS other(a)")?;
        for in_transaction in [false, true] {
            if in_transaction {
                conn.execute("BEGIN")?;
            }
            let mut select = conn.prepare(select_sql)?;
            let mut ids = vec![next_int(&mut select)?.unwrap()];

            let err = conn.execute(drop_sql).unwrap_err();
            assert!(
                matches!(err, LimboError::TableLocked),
                "{drop_sql} during {select_sql}: {err:?}"
            );
            conn.execute("CREATE TABLE reuse(id INTEGER PRIMARY KEY, b BLOB)")?;
            conn.execute("INSERT INTO reuse VALUES(16, zeroblob(5000))")?;

            while let Some(id) = next_int(&mut select)? {
                ids.push(id);
            }
            assert_eq!(ids, (1..=32).collect::<Vec<_>>(), "{select_sql}");
            drop(select);
            conn.execute("DROP TABLE reuse")?;
            if in_transaction {
                conn.execute("COMMIT")?;
            }

            let objects: Vec<(String,)> = conn.exec_rows(
                "SELECT name FROM sqlite_schema WHERE name NOT LIKE '__turso%' ORDER BY name",
            );
            assert_eq!(
                objects,
                vec![
                    ("other".to_string(),),
                    ("u".to_string(),),
                    ("ux".to_string(),)
                ],
                "{drop_sql} in_transaction={in_transaction}"
            );
            let count: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM u");
            assert_eq!(count, vec![(32,)]);
        }
    }

    conn.execute("DROP INDEX ux")?;
    conn.execute("DROP TABLE u")?;
    let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    assert_eq!(integrity, vec![("ok".to_string(),)]);
    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_drop_runs_while_a_statement_that_reads_no_table_is_active(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    // Like SQLite, only statements that read a database file block a DROP.
    for sql in [
        "VALUES(1),(2),(3)",
        "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3",
    ] {
        for drop_sql in ["DROP INDEX oi", "DROP TABLE o"] {
            conn.execute("CREATE TABLE IF NOT EXISTS o(a)")?;
            conn.execute("CREATE INDEX IF NOT EXISTS oi ON o(a)")?;
            let mut stmt = conn.prepare(sql)?;
            let mut values = vec![next_int(&mut stmt)?.unwrap()];
            conn.execute(drop_sql)?;
            while let Some(value) = next_int(&mut stmt)? {
                values.push(value);
            }
            assert_eq!(values, vec![1, 2, 3], "{sql} with {drop_sql}");
        }
        let objects: Vec<(String,)> = conn.exec_rows(
            "SELECT name FROM sqlite_schema WHERE name NOT LIKE '__turso%' ORDER BY name",
        );
        assert!(objects.is_empty(), "{sql}: {objects:?}");
    }
    Ok(())
}

#[test]
fn test_select_fails_while_drop_waits_for_io() -> anyhow::Result<()> {
    // A DROP prepared before its table exists is reprepared on its first
    // step, and must be refused the same way once it is running.
    for (drop_sql, prepare_before_create) in [
        ("DROP TABLE u", false),
        ("DROP INDEX ux", false),
        ("DROP TABLE IF EXISTS u", true),
        ("DROP INDEX IF EXISTS ux", true),
    ] {
        let io = Arc::new(QueuedIo::new());
        let dir = tempfile::TempDir::new()?;
        let path = dir.path().join("select-during-drop-io.db");
        let db = Database::open_file(io.clone(), path.to_str().unwrap(), Arc::new(SqliteDialect))?;
        let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
        conn.execute("PRAGMA page_size=512")?;
        conn.execute("PRAGMA cache_size=9")?;
        conn.execute("PRAGMA journal_mode='wal'")?;
        let early_drop_stmt = if prepare_before_create {
            Some(conn.prepare(drop_sql)?)
        } else {
            None
        };
        conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, x, b BLOB)")?;
        conn.execute("CREATE INDEX ux ON u(x)")?;
        conn.execute("CREATE TABLE other(a)")?;
        for id in 1..=300 {
            conn.execute(format!("INSERT INTO u VALUES({id}, {id}, zeroblob(300))"))?;
        }
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;

        let mut drop_stmt = match early_drop_stmt {
            Some(stmt) => stmt,
            None => conn.prepare(drop_sql)?,
        };
        for _ in 0..6 {
            assert!(matches!(drop_stmt.step()?, StepResult::IO), "{drop_sql}");
            io.step()?;
        }
        // Both a reader of the dropped b-tree and a reader of another table
        // are refused, as a DROP refuses to start while any statement reads.
        for select_sql in [
            "SELECT count(*), sum(length(b)) FROM u",
            "SELECT count(*), sum(x) FROM u INDEXED BY ux WHERE x > 0",
            "SELECT count(*) FROM other",
        ] {
            let err = conn.prepare(select_sql)?.step().unwrap_err();
            assert!(
                matches!(err, LimboError::TableLocked),
                "{select_sql} during {drop_sql}: {err:?}"
            );
        }
        loop {
            match drop_stmt.step()? {
                StepResult::IO => io.step()?,
                StepResult::Done => break,
                r => panic!("unexpected step result: {r:?}"),
            }
        }
        drop(drop_stmt);

        let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
        assert_eq!(integrity, vec![("ok".to_string(),)], "{drop_sql}");
        let count: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM other");
        assert_eq!(count, vec![(0,)]);
    }
    Ok(())
}

#[turso_macros::test]
fn test_unstarted_drop_does_not_block_readers(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, x)")?;
    conn.execute("CREATE INDEX ux ON u(x)")?;
    conn.execute("CREATE TABLE other(a)")?;
    for id in 1..=32 {
        conn.execute(format!("INSERT INTO u VALUES({id}, {id})"))?;
    }

    // EXPLAIN lists the program of a DROP without running it.
    for explain_sql in ["EXPLAIN DROP TABLE other", "EXPLAIN DROP INDEX ux"] {
        let mut explain = conn.prepare(explain_sql)?;
        assert!(matches!(explain.step()?, StepResult::Row), "{explain_sql}");
        let count: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM u");
        assert_eq!(count, vec![(32,)], "{explain_sql}");
        conn.execute("INSERT INTO other VALUES(1)")?;
    }

    // A DROP that returned busy before starting its transaction has freed
    // nothing, and a reader may run while the caller holds it.
    let writer = tmp_db.connect_limbo();
    writer.execute("BEGIN IMMEDIATE")?;
    writer.execute("INSERT INTO other VALUES(9)")?;
    let mut drop_stmt = conn.prepare("DROP TABLE other")?;
    assert!(matches!(drop_stmt.step()?, StepResult::Busy));
    let count: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM u");
    assert_eq!(count, vec![(32,)]);
    writer.execute("COMMIT")?;
    drop_stmt.reset()?;
    loop {
        match drop_stmt.step()? {
            StepResult::IO => drop_stmt._io().step()?,
            StepResult::Done => break,
            r => panic!("unexpected step result: {r:?}"),
        }
    }
    drop(drop_stmt);
    let objects: Vec<(String,)> =
        conn.exec_rows("SELECT name FROM sqlite_schema WHERE name = 'other'");
    assert!(objects.is_empty());
    Ok(())
}

#[test]
fn test_drop_fails_while_select_waits_for_its_first_page() -> anyhow::Result<()> {
    let io = Arc::new(QueuedIo::new());
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().join("drop-during-select-io.db");
    let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect))?;
    let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    conn.execute("PRAGMA page_size=512")?;
    conn.execute("PRAGMA cache_size=9")?;
    conn.execute("PRAGMA cache_spill=ON")?;
    conn.execute("PRAGMA journal_mode='wal'")?;
    conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, b BLOB)")?;
    for id in 1..=32 {
        conn.execute(format!("INSERT INTO u VALUES({id}, zeroblob(60))"))?;
    }
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;

    let mut select = conn.prepare("SELECT id, length(b) FROM u WHERE id = 16")?;
    assert!(matches!(select.step()?, StepResult::IO));
    select._io().step()?;

    let err = conn.execute("DROP TABLE u").unwrap_err();
    assert!(matches!(err, LimboError::TableLocked), "{err:?}");
    conn.execute("CREATE TABLE reuse(id INTEGER PRIMARY KEY, b BLOB)")?;
    conn.execute("INSERT INTO reuse VALUES(16, zeroblob(5000))")?;

    let mut rows = Vec::new();
    loop {
        match select.step()? {
            StepResult::Row => {
                let row = select.row().unwrap();
                rows.push((row.get::<i64>(0)?, row.get::<i64>(1)?));
            }
            StepResult::IO => select._io().step()?,
            StepResult::Done => break,
            r => panic!("unexpected step result: {r:?}"),
        }
    }
    assert_eq!(rows, vec![(16, 60)]);
    drop(select);
    conn.execute("DROP TABLE u")?;
    Ok(())
}

#[test]
fn test_drop_fails_while_insert_waits_for_io_after_abandoned_delete() -> anyhow::Result<()> {
    let io = Arc::new(QueuedIo::new());
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().join("drop-during-insert-io.db");
    let db = Database::open_file(io.clone(), path.to_str().unwrap(), Arc::new(SqliteDialect))?;
    let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
    conn.execute("PRAGMA page_size=512")?;
    conn.execute("PRAGMA cache_size=9")?;
    conn.execute("PRAGMA cache_spill=ON")?;
    conn.execute("PRAGMA journal_mode='wal'")?;
    conn.execute("CREATE TABLE seed(id INTEGER PRIMARY KEY, x, b BLOB)")?;
    conn.execute("CREATE INDEX seed_x ON seed(x)")?;
    for id in 1..=10 {
        conn.execute(format!(
            "INSERT INTO seed VALUES({id}, {id}, zeroblob(20000))"
        ))?;
    }
    conn.execute("DELETE FROM seed WHERE id=1")?;
    conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, b BLOB)")?;
    conn.execute("INSERT INTO u VALUES(1, zeroblob(60))")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    conn.execute("BEGIN")?;

    let mut delete = conn.prepare("DELETE FROM seed WHERE id IN (2,3)")?;
    for _ in 0..9 {
        assert!(matches!(delete.step()?, StepResult::IO));
        io.step()?;
    }
    drop(delete);

    let mut insert = conn.prepare("INSERT INTO u VALUES(101, zeroblob(800))")?;
    let mut insert_io = 0;
    loop {
        match insert.step()? {
            StepResult::IO => {
                insert_io += 1;
                io.step()?;
                if insert_io == 2 {
                    assert!(conn.execute("DROP TABLE u").is_err());
                }
            }
            StepResult::Done => break,
            r => panic!("unexpected step result: {r:?}"),
        }
    }
    assert!(insert_io >= 2, "INSERT finished before its second I/O");
    drop(insert);
    conn.execute("ROLLBACK")?;

    let ids: Vec<(i64,)> = conn.exec_rows("SELECT id FROM u");
    assert_eq!(ids, vec![(1,)]);
    let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    assert_eq!(integrity, vec![("ok".to_string(),)]);
    Ok(())
}

fn next_int(stmt: &mut turso_core::Statement) -> anyhow::Result<Option<i64>> {
    loop {
        match stmt.step()? {
            StepResult::Row => return Ok(Some(stmt.row().unwrap().get(0)?)),
            StepResult::IO => stmt._io().step()?,
            StepResult::Done => return Ok(None),
            r => panic!("unexpected step result: {r:?}"),
        }
    }
}

fn create_database_with_sqlite(
    sql: &str,
) -> anyhow::Result<(tempfile::TempDir, std::path::PathBuf)> {
    let tmp_dir = tempfile::TempDir::new()?;
    let db_path = tmp_dir.path().join("test.db");
    rusqlite::Connection::open(&db_path)?.execute_batch(sql)?;
    Ok((tmp_dir, db_path))
}
