use crate::common::TempDatabase;

#[test]
fn test_fail_drop_indexed_column() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (a, b);", true);
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE INDEX i ON t (a)")?;
    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(res.is_err(), "Expected error when dropping indexed column");
    Ok(())
}

#[test]
fn test_fail_drop_unique_column() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (a UNIQUE, b);", true);
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(res.is_err(), "Expected error when dropping UNIQUE column");
    Ok(())
}

#[test]
fn test_fail_drop_compound_unique_column() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (a, b, UNIQUE(a, b));", true);
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping column in compound UNIQUE"
    );
    Ok(())
}

#[test]
fn test_fail_drop_primary_key_column() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (a PRIMARY KEY, b);", true);
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping PRIMARY KEY column"
    );
    Ok(())
}

#[test]
fn test_fail_drop_compound_primary_key_column() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (a, b, PRIMARY KEY(a, b));", true);
    let conn = tmp_db.connect_limbo();

    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping column in compound PRIMARY KEY"
    );
    Ok(())
}

#[test]
fn test_fail_drop_partial_index_column() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (a, b);", true);
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE INDEX i ON t (b) WHERE a > 0")?;
    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping column referenced by partial index"
    );
    Ok(())
}

#[test]
fn test_fail_drop_view_column() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (a, b);", true);
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE VIEW v AS SELECT a, b FROM t")?;
    let res = conn.execute("ALTER TABLE t DROP COLUMN a");
    assert!(
        res.is_err(),
        "Expected error when dropping column referenced by view"
    );
    Ok(())
}

// FIXME: this should rewrite the view to reference the new column name
#[test]
fn test_fail_rename_view_column() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE t (a, b);", true);
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE VIEW v AS SELECT a, b FROM t")?;
    let res = conn.execute("ALTER TABLE t RENAME a TO c");
    assert!(
        res.is_err(),
        "Expected error when renaming column referenced by view"
    );
    Ok(())
}

#[test]
fn test_allow_drop_unreferenced_columns() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite(
        "CREATE TABLE t (pk INTEGER PRIMARY KEY, indexed INTEGER, viewed INTEGER, partial INTEGER, compound1 INTEGER, compound2 INTEGER, unused1 INTEGER, unused2 INTEGER, unused3 INTEGER);",
        true
    );
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
