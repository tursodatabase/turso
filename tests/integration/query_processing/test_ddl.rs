use std::sync::Arc;

use crate::common::{ExecRows, TempDatabase};
use crate::queued_io::QueuedIo;
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, StepResult};

fn open_queued_conn(io: Arc<QueuedIo>, path: &str) -> anyhow::Result<Arc<Connection>> {
    let db =
        Database::open_file_with_flags(io, path, OpenFlags::default(), DatabaseOpts::new(), None)?;
    Ok(db.connect()?)
}

fn collect_rows(stmt: &mut turso_core::Statement) -> anyhow::Result<Vec<(i64, i64)>> {
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            StepResult::Yield => {}
            StepResult::Row => {
                let row = stmt.row().expect("row should be available after Row");
                rows.push((row.get::<i64>(0)?, row.get::<i64>(1)?));
            }
            StepResult::Done => return Ok(rows),
            StepResult::Interrupt | StepResult::Busy => {
                anyhow::bail!("unexpected non-progress result while draining statement")
            }
        }
    }
}

/// SQLite's OP_Destroy refuses to free a root page while any other statement
/// is active on the connection (SQLITE_LOCKED "database table is locked"),
/// because active cursors may hold references to pages that DROP TABLE would
/// send to the freelist for reuse. A SELECT parked at its first I/O yield must
/// therefore block DROP TABLE, and resume against intact table pages instead
/// of a recycled root page.
///
/// Reference behavior verified against SQLite via rusqlite: with a stepped,
/// unfinished SELECT on the same connection, DROP TABLE fails with
/// SQLITE_LOCKED "database table is locked" while CREATE TABLE and INSERT
/// still succeed, and DROP TABLE succeeds once the reader finishes.
#[test]
fn active_table_seek_after_drop_reuse_must_not_use_recycled_root_page() -> anyhow::Result<()> {
    let io = Arc::new(QueuedIo::new());
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().join("table-interior-drop-reuse.db");
    let path = path.to_str().unwrap();
    let conn = open_queued_conn(io, path)?;

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
    match select.step()? {
        StepResult::IO => select._io().step()?,
        other => anyhow::bail!("SELECT did not yield at the root-page read: {other:?}"),
    }

    let err = conn
        .execute("DROP TABLE u")
        .expect_err("DROP TABLE must fail while a statement is active on the connection");
    assert!(
        matches!(err, turso_core::LimboError::TableLocked),
        "expected TableLocked, got {err:?}"
    );
    conn.execute("CREATE TABLE reuse(id INTEGER PRIMARY KEY, b BLOB)")?;
    conn.execute("INSERT INTO reuse VALUES(16, zeroblob(5000))")?;

    let rows = collect_rows(&mut select)?;
    assert_eq!(rows, vec![(16, 60)]);

    conn.execute("DROP TABLE u")?;
    let remaining: Vec<(String,)> =
        conn.exec_rows("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name");
    assert_eq!(remaining, vec![("reuse".to_string(),)]);
    let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    assert_eq!(integrity, vec![("ok".to_string(),)]);

    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn test_fail_drop_indexed_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE INDEX i ON t (a)")?;
    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(res.is_err(), "Expected error when dropping indexed column");
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a UNIQUE, b);")]
fn test_fail_drop_unique_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(res.is_err(), "Expected error when dropping UNIQUE column");
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b, UNIQUE(a, b));")]
fn test_fail_drop_compound_unique_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping column in compound UNIQUE"
    );
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a PRIMARY KEY, b);")]
fn test_fail_drop_primary_key_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping PRIMARY KEY column"
    );
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b, PRIMARY KEY(a, b));")]
fn test_fail_drop_compound_primary_key_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping column in compound PRIMARY KEY"
    );
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn test_fail_drop_partial_index_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE INDEX i ON t (b) WHERE a > 0")?;
    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping column referenced by partial index"
    );
    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE t (a, b);")]
fn test_fail_drop_view_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE VIEW v AS SELECT a, b FROM t")?;
    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping column referenced by view"
    );
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

    let res = conn.execute("CREATE TABLE t(a TEXT, b INT) WITHOUT ROWID");
    assert!(
        res.is_err(),
        "Expected error when creating WITHOUT ROWID table without a primary key"
    );
    assert!(
        res.unwrap_err().to_string().contains("PRIMARY KEY"),
        "Expected error message about a required primary key"
    );
    Ok(())
}

#[turso_macros::test]
fn test_create_table_without_rowid_rejects_secondary_unique(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res =
        conn.execute("CREATE TABLE t(a TEXT PRIMARY KEY, b INT UNIQUE, c TEXT) WITHOUT ROWID");
    assert!(
        res.is_err(),
        "Expected error when creating WITHOUT ROWID table with secondary UNIQUE"
    );
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("secondary UNIQUE constraints on WITHOUT ROWID tables are not supported"),
        "Expected error message about secondary UNIQUE constraints"
    );
    Ok(())
}

#[turso_macros::test]
fn test_create_table_without_rowid_rejects_autoincrement(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT) WITHOUT ROWID");
    assert!(
        res.is_err(),
        "Expected error when creating WITHOUT ROWID table with AUTOINCREMENT"
    );
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("AUTOINCREMENT is not allowed on WITHOUT ROWID tables"),
        "Expected error message about AUTOINCREMENT"
    );
    Ok(())
}

#[turso_macros::test]
fn test_fail_not_null_in_upsert(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER NOT NULL, c TEXT NOT NULL);")?;
    conn.execute("INSERT INTO t VALUES (1, 10, 'first');")?;

    let res = conn.execute("INSERT INTO t VALUES (1, NULL, 'second') ON CONFLICT(a) DO UPDATE SET b = excluded.b, c = excluded.c;");
    assert!(res.is_err(), "Expected NOT NULL constraint error");
    assert!(
        res.unwrap_err().to_string().contains("t.b"),
        "Expected NOT NULL error message to contain 't.b'"
    );
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
/// testing/sqltests/turso-tests/legacy-unquoted-view-columns.sqltest.
#[test]
fn test_drop_broken_legacy_view_row() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../testing/sqltests/database/testing_legacy_unquoted_view_columns.db");
    let tmp_dir = tempfile::TempDir::new()?;
    let db_path = tmp_dir.path().join("legacy_view.db");
    std::fs::copy(&fixture, &db_path)?;

    let db = TempDatabase::builder().with_db_path(&db_path).build();
    let conn = db.connect_limbo();

    // The table is readable; the broken view is unavailable with a
    // diagnosable error; CREATE VIEW over the name is blocked.
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT a FROM t");
    assert_eq!(rows, vec![(42,)]);
    let err = conn.execute("SELECT * FROM v").unwrap_err();
    assert!(err.to_string().contains("could not be loaded"), "{err}");
    let err = conn
        .execute("CREATE VIEW v AS SELECT a FROM t")
        .unwrap_err();
    assert!(err.to_string().contains("already exists"), "{err}");

    // DROP VIEW removes the orphaned row and frees the name.
    conn.execute("DROP VIEW v")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM sqlite_master WHERE name = 'v'");
    assert_eq!(rows, vec![(0,)]);
    conn.execute("CREATE VIEW v(\"col one\") AS SELECT a FROM t")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT \"col one\" FROM v");
    assert_eq!(rows, vec![(42,)]);
    Ok(())
}
