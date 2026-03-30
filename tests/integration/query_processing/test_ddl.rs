use crate::common::{ExecRows, TempDatabase};

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

/// WITHOUT ROWID tables are not supported
#[turso_macros::test]
fn test_create_table_without_rowid_not_supported(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID");
    assert!(
        res.is_err(),
        "Expected error when creating WITHOUT ROWID table"
    );
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("WITHOUT ROWID tables are not supported"),
        "Expected error message about WITHOUT ROWID not being supported"
    );
    Ok(())
}

#[turso_macros::test]
fn test_create_table_without_rowid_composite_pk(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("CREATE TABLE t(a TEXT, b INT, PRIMARY KEY(a, b)) WITHOUT ROWID");
    assert!(
        res.is_err(),
        "Expected error when creating WITHOUT ROWID table with composite primary key"
    );
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("WITHOUT ROWID tables are not supported"),
        "Expected error message about WITHOUT ROWID not being supported"
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
