use turso_sync_engine::Result;

use crate::harness::Remote;

/// A table whose generated column has the VIRTUAL keyword and an index. ADD
/// COLUMN and DROP COLUMN on the server write the table text again without
/// that keyword.
const INDEXED_COLUMN_WITH_VIRTUAL_KEYWORD: &[&str] = &[
    "CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, c INT, g INT AS (a + 1) VIRTUAL)",
    "CREATE INDEX ig ON t (g)",
    "INSERT INTO t (id, a, c) VALUES (1, 10, 20)",
];

/// The remote drops another column. In MVCC mode the replica cannot ALTER the
/// indexed column, so it must not treat the lost keyword as a change.
#[test]
fn drop_column_beside_indexed_column_with_virtual_keyword() -> Result<()> {
    let remote = Remote::new(INDEXED_COLUMN_WITH_VIRTUAL_KEYWORD)?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["ALTER TABLE t DROP COLUMN c"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote adds a column. The replica must not treat the lost keyword as a
/// change.
#[test]
fn add_column_beside_indexed_column_with_virtual_keyword() -> Result<()> {
    let remote = Remote::new(INDEXED_COLUMN_WITH_VIRTUAL_KEYWORD)?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["ALTER TABLE t ADD COLUMN d INT"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}
