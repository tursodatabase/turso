use turso_sync_engine::Result;

use crate::harness::Remote;

const TABLE_WITH_INDEXED_GENERATED_COLUMN: &[&str] = &[
    "CREATE TABLE core (id TEXT PRIMARY KEY, sort TEXT, search TEXT AS (CAST (sort AS TEXT)))",
    "CREATE INDEX idx ON core (search)",
    "INSERT INTO core (id, sort) VALUES ('a', 'Hello')",
];

#[test]
fn drop_indexed_generated_column_and_its_index_in_one_transaction() -> Result<()> {
    let remote = Remote::new(TABLE_WITH_INDEXED_GENERATED_COLUMN)?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["DROP INDEX idx", "ALTER TABLE core DROP COLUMN search"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn redefine_indexed_generated_column_and_its_index_in_one_transaction() -> Result<()> {
    let remote = Remote::new(TABLE_WITH_INDEXED_GENERATED_COLUMN)?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "DROP INDEX idx",
        "ALTER TABLE core DROP COLUMN search",
        "ALTER TABLE core ADD COLUMN search TEXT AS (UPPER (sort))",
        "CREATE INDEX idx ON core (search)",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn rename_column_used_by_index_and_trigger() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (x, y)",
        "CREATE INDEX t_x ON t (x)",
        "CREATE TRIGGER t_ins AFTER INSERT ON t BEGIN SELECT new.x; END",
        "INSERT INTO t VALUES (1, 2)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["ALTER TABLE t RENAME COLUMN x TO z"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn create_table_with_primary_key() -> Result<()> {
    let remote = Remote::new(&["CREATE TABLE kept (x)"])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "CREATE TABLE created (id TEXT PRIMARY KEY, y)",
        "INSERT INTO created VALUES ('a', 1)",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn drop_table_with_primary_key() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE kept (x)",
        "CREATE TABLE dropped (id TEXT PRIMARY KEY, y)",
        "INSERT INTO dropped VALUES ('a', 1)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["DROP TABLE dropped"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
#[ignore = "https://github.com/tursodatabase/turso/issues/9529"]
fn rename_indexed_table() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (x, y)",
        "CREATE INDEX t_x ON t (x)",
        "INSERT INTO t VALUES (1, 2)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["ALTER TABLE t RENAME TO u"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn drop_newest_table_and_create_index_in_one_transaction() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE kept (x)",
        "CREATE TABLE dropped (y)",
        "INSERT INTO kept VALUES (1)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["DROP TABLE dropped", "CREATE INDEX kept_x ON kept (x)"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn drop_newest_index_and_create_table_in_one_transaction() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE kept (x)",
        "CREATE INDEX dropped ON kept (x)",
        "INSERT INTO kept VALUES (1)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["DROP INDEX dropped", "CREATE TABLE created (y)"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn drop_newest_table_and_create_another_table_in_one_transaction() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE kept (x)",
        "CREATE TABLE dropped (y)",
        "INSERT INTO dropped VALUES (1)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "DROP TABLE dropped",
        "CREATE TABLE created (z)",
        "INSERT INTO created VALUES (2)",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}
