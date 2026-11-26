use std::sync::Arc;

use turso_sync_engine::Result;

use crate::harness::{database_opts, InProcessServer, Remote, Replica};

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

/// The remote changes a generated column and the generated column that uses
/// it, with a regular column between them. It drops both and adds them again at
/// the end. The replica must not take the last generated column as renamed to
/// the name of the first one, which the replica still has until it drops it.
#[test]
fn redefine_generated_columns_around_regular_column_in_one_transaction() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, k TEXT AS (lower(a)), x TEXT, u TEXT AS (k || '!'))",
        "INSERT INTO t (id, a, x) VALUES (1, 'Aa', 'x1')",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t DROP COLUMN u",
        "ALTER TABLE t DROP COLUMN k",
        "ALTER TABLE t ADD COLUMN k TEXT AS (upper(a))",
        "ALTER TABLE t ADD COLUMN u TEXT AS (k || '!')",
    ])?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);

    // A pull matches the values of a remote row with the columns by position.
    // This insert shows that the replica has the column order of the server.
    remote.execute_transaction(&["INSERT INTO t (id, a, x) VALUES (2, 'Bb', 'x2')"])?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// A new replica starts from a checkpoint of the server before the change of
/// `redefine_generated_columns_around_regular_column_in_one_transaction`, and
/// replays the change from the log. It must be the same as the server.
#[test]
fn new_replica_replays_generated_columns_re_added_around_regular_column() -> Result<()> {
    let setup = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, k TEXT AS (lower(a)), x TEXT, u TEXT AS (k || '!'))",
        "INSERT INTO t (id, a, x) VALUES (1, 'Aa', 'x1')",
    ];
    let checkpoint = Remote::new(&setup)?;
    let remote = Remote::new(&setup)?;
    // A bootstrap checkpoints the remote, so its log holds only the change.
    remote.bootstrap_replica()?;
    remote.execute_transaction(&[
        "ALTER TABLE t DROP COLUMN u",
        "ALTER TABLE t DROP COLUMN k",
        "ALTER TABLE t ADD COLUMN k TEXT AS (upper(a))",
        "ALTER TABLE t ADD COLUMN u TEXT AS (k || '!')",
    ])?;

    let replica = open_new_replica(&remote, checkpoint)?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// Opens a new replica that starts from `checkpoint`, a database with the state
/// of the server at its last checkpoint, and replays the log of `remote`.
fn open_new_replica(remote: &Arc<Remote>, checkpoint: Arc<Remote>) -> Result<Replica> {
    Replica::open(
        &Arc::new(tempfile::tempdir()?),
        InProcessServer::with_bootstrap_base(remote.clone(), checkpoint),
        database_opts(),
    )
}
