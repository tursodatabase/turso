use turso_sync_engine::Result;

use crate::harness::Remote;

/// A table with a STORED column that reads a VIRTUAL generated column.
const STORED_READER: &[&str] = &[
    "CREATE TABLE t (id TEXT PRIMARY KEY, a INTEGER, v INTEGER AS (a + 1), s INTEGER AS (v * 2) STORED)",
    "INSERT INTO t (id, a) VALUES ('x', 10)",
];

/// The remote drops a STORED column that reads another generated column. The
/// server then writes the table text again without the VIRTUAL keyword of
/// that column.
#[test]
fn drop_stored_column_that_reads_column_with_virtual_keyword() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id TEXT PRIMARY KEY, a INTEGER, v INTEGER AS (a + 1) VIRTUAL, s INTEGER AS (v * 2) STORED)",
        "INSERT INTO t (id, a) VALUES ('x', 10)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["ALTER TABLE t DROP COLUMN s"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote drops a STORED column, and then it changes the generated column
/// that the STORED column read. The server refuses that change while the
/// STORED column exists.
#[test]
fn drop_stored_column_and_redefine_column_it_reads() -> Result<()> {
    let remote = Remote::new(STORED_READER)?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t DROP COLUMN s",
        "ALTER TABLE t ALTER COLUMN v TO v INTEGER AS (a + 2)",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote replaces a STORED column with a VIRTUAL column of the same name.
/// It also changes the generated column that both columns read.
#[test]
fn replace_stored_column_and_redefine_column_it_reads() -> Result<()> {
    let remote = Remote::new(STORED_READER)?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t DROP COLUMN s",
        "ALTER TABLE t ALTER COLUMN v TO v INTEGER AS (a + 2)",
        "ALTER TABLE t ADD COLUMN s INTEGER AS (v * 2)",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote replaces a STORED column with a VIRTUAL column of the same name.
#[test]
fn replace_stored_column_with_virtual_column() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, s TEXT AS (upper(a)) STORED)",
        "INSERT INTO t (id, a) VALUES ('x', 'Hello'), ('y', 'World')",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t DROP COLUMN s",
        "ALTER TABLE t ADD COLUMN s TEXT AS (lower(a))",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote replaces its last column, a STORED column, with a VIRTUAL column
/// of another name. The replica matches columns by position, so it first
/// renames the STORED column.
#[test]
fn replace_last_stored_column_with_virtual_column_of_another_name() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id TEXT PRIMARY KEY, price INTEGER, qty INTEGER, total_stored INTEGER AS (price * qty) STORED)",
        "INSERT INTO t (id, price, qty) VALUES ('x', 3, 4), ('y', 5, 6)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t DROP COLUMN total_stored",
        "ALTER TABLE t ADD COLUMN total INTEGER AS (price * qty)",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote redefines a STORED column of an empty table. ADD COLUMN accepts
/// a STORED column only while the table is empty.
#[test]
fn redefine_stored_column_of_empty_table() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, s TEXT AS (upper(a)) STORED)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t DROP COLUMN s",
        "ALTER TABLE t ADD COLUMN s TEXT AS (lower(a)) STORED",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote turns a VIRTUAL column of an empty table into a STORED column.
#[test]
fn make_virtual_column_of_empty_table_stored() -> Result<()> {
    let remote =
        Remote::new(&["CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, v TEXT AS (upper(a)))"])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t DROP COLUMN v",
        "ALTER TABLE t ADD COLUMN v TEXT AS (lower(a)) STORED",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote drops a column from a table whose STORED column has NOT NULL
/// after AS. The server then writes the table text again with NOT NULL before
/// AS. The table has no rows, because a pull cannot replay the rows of a table
/// with a STORED column yet.
#[test]
fn drop_column_beside_stored_column_with_not_null_after_as() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, c INT, g INT AS (a + 1) STORED NOT NULL)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["ALTER TABLE t DROP COLUMN c"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}
