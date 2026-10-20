use turso_sync_engine::Result;

use crate::harness::{Remote, Replica, Snapshot};

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

#[test]
fn pull_keeps_unpushed_local_drops() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE r (x)",
        "CREATE INDEX \"r x\" ON r (x)",
        "CREATE TABLE t (x, \"select\", z)",
        "CREATE TABLE \"order\" (x)",
        "CREATE VIEW \"v-1\" AS SELECT x FROM r",
        "CREATE TRIGGER \"from\" AFTER INSERT ON r BEGIN SELECT new.x; END",
    ])?;
    let replica = remote.bootstrap_replica()?;

    replica.execute("DROP INDEX \"r x\"")?;
    replica.execute("DROP VIEW \"v-1\"")?;
    replica.execute("ALTER TABLE t DROP COLUMN z")?;
    replica.execute("ALTER TABLE t DROP COLUMN \"select\"")?;
    replica.execute("DROP TABLE \"order\"")?;
    replica.execute("DROP TRIGGER \"from\"")?;
    remote.execute_transaction(&["INSERT INTO r VALUES (1)"])?;
    replica.pull()?;
    remote.execute_transaction(&["INSERT INTO r VALUES (2)"])?;
    replica.pull()?;
    replica.pull()?;

    let expected = Remote::new(&[
        "CREATE TABLE r (x)",
        "CREATE TABLE t (x)",
        "INSERT INTO r VALUES (1)",
        "INSERT INTO r VALUES (2)",
    ])?;
    assert_eq!(replica.snapshot()?, expected.snapshot()?);
    Ok(())
}

#[test]
fn pull_skips_remote_writes_to_table_dropped_locally() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE r (x)",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x)",
        "CREATE TABLE u (id TEXT PRIMARY KEY, x)",
        "CREATE TABLE w (x)",
        "INSERT INTO t VALUES (1, 'a'), (2, 'b')",
        "INSERT INTO u VALUES ('a', 1), ('b', 2)",
        "INSERT INTO w VALUES (1), (2)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    replica.execute("DROP TABLE t")?;
    replica.execute("DROP TABLE u")?;
    replica.execute("DROP TABLE w")?;
    remote.execute_transaction(&[
        "INSERT INTO t VALUES (3, 'c')",
        "UPDATE t SET x = 'z' WHERE id = 1",
        "DELETE FROM t WHERE id = 2",
        "INSERT INTO u VALUES ('c', 3)",
        "UPDATE u SET x = 9 WHERE id = 'a'",
        "DELETE FROM u WHERE id = 'b'",
        "INSERT INTO w VALUES (3)",
        "UPDATE w SET x = 9 WHERE x = 1",
        "DELETE FROM w WHERE x = 2",
        "INSERT INTO r VALUES (1)",
    ])?;
    replica.pull()?;
    remote.execute_transaction(&["INSERT INTO r VALUES (2)"])?;
    replica.pull()?;

    let expected = Remote::new(&[
        "CREATE TABLE r (x)",
        "INSERT INTO r VALUES (1)",
        "INSERT INTO r VALUES (2)",
    ])?;
    assert_eq!(replica.snapshot()?, expected.snapshot()?);
    Ok(())
}

#[test]
fn pull_skips_remote_values_for_column_dropped_locally() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x, y, z)",
        "INSERT INTO t VALUES (1, 'x1', 'y1', 'z1')",
        "CREATE TABLE g (id INTEGER PRIMARY KEY, double AS (id * 2), y, z)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    replica.execute("ALTER TABLE t DROP COLUMN y")?;
    replica.execute("ALTER TABLE g DROP COLUMN y")?;
    remote.execute_transaction(&[
        "INSERT INTO g (id, y, z) VALUES (1, 'y1', 'z1')",
        "INSERT INTO t VALUES (2, 'x2', 'y2', 'z2')",
        "UPDATE t SET y = 'y9', z = 'z9' WHERE id = 1",
        "UPDATE t SET y = 'y8' WHERE id = 2",
    ])?;
    replica.pull()?;
    remote.execute_transaction(&["INSERT INTO t (id, x, y) VALUES (3, 'x3', 'y3')"])?;
    replica.pull()?;

    let expected = Remote::new(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x, z)",
        "CREATE TABLE g (id INTEGER PRIMARY KEY, double AS (id * 2), z)",
        "INSERT INTO g (id, z) VALUES (1, 'z1')",
        "INSERT INTO t VALUES (1, 'x1', 'z9')",
        "INSERT INTO t VALUES (2, 'x2', 'z2')",
        "INSERT INTO t VALUES (3, 'x3', NULL)",
    ])?;
    assert_eq!(replica.snapshot()?, expected.snapshot()?);
    Ok(())
}

#[test]
fn pull_skips_local_writes_made_before_a_local_drop() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE r (x)",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x)",
        "CREATE TABLE c (id INTEGER PRIMARY KEY, x, y, z)",
        "INSERT INTO c VALUES (1, 'x1', 'y1', 'z1')",
    ])?;
    let replica = remote.bootstrap_replica()?;

    replica.execute("INSERT INTO t VALUES (1, 'a')")?;
    replica.execute("INSERT INTO c VALUES (2, 'x2', 'y2', 'z2')")?;
    replica.execute("UPDATE c SET y = 'y9' WHERE id = 1")?;
    replica.execute("UPDATE c SET y = 'y8', z = 'z8' WHERE id = 2")?;
    replica.execute("DROP TABLE t")?;
    replica.execute("ALTER TABLE c DROP COLUMN y")?;
    replica.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x)")?;
    replica.execute("INSERT INTO t VALUES (2, 'b')")?;
    remote.execute_transaction(&["INSERT INTO r VALUES (1)"])?;
    replica.pull()?;
    remote.execute_transaction(&["INSERT INTO r VALUES (2)"])?;
    replica.pull()?;

    let expected = Remote::new(&[
        "CREATE TABLE r (x)",
        "CREATE TABLE c (id INTEGER PRIMARY KEY, x, z)",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x)",
        "INSERT INTO r VALUES (1)",
        "INSERT INTO r VALUES (2)",
        "INSERT INTO c VALUES (1, 'x1', 'z1')",
        "INSERT INTO c VALUES (2, 'x2', 'z8')",
        "INSERT INTO t VALUES (2, 'b')",
    ])?;
    assert_eq!(replica.snapshot()?, expected.snapshot()?);
    Ok(())
}

fn assert_pull_fails_on_remote_schema_change_after_local_column_drop(
    remote_schema_change: &[&str],
    remote_insert: &str,
) -> Result<()> {
    let remote = Remote::new(&["CREATE TABLE t (id INTEGER PRIMARY KEY, x, y, z)"])?;
    let replica = remote.bootstrap_replica()?;

    replica.execute("ALTER TABLE t DROP COLUMN y")?;
    let before_pull = replica.snapshot()?;
    remote.execute_transaction(remote_schema_change)?;
    assert_pull_fails_and_keeps(&replica, &before_pull);
    remote.execute_transaction(&[remote_insert])?;
    assert_pull_fails_and_keeps(&replica, &before_pull);
    Ok(())
}

fn assert_pull_fails_and_keeps(replica: &Replica, before_pull: &Snapshot) {
    let error = replica.pull().unwrap_err();
    assert!(
        error.to_string().contains("push the local changes"),
        "{error}"
    );
    assert_eq!(&replica.snapshot().unwrap(), before_pull);
}

#[test]
fn pull_fails_when_remote_drops_another_column_of_a_table_with_an_unpushed_column_drop(
) -> Result<()> {
    assert_pull_fails_on_remote_schema_change_after_local_column_drop(
        &["ALTER TABLE t DROP COLUMN x"],
        "INSERT INTO t VALUES (2, 'y2', 'z2')",
    )
}

#[test]
fn pull_fails_when_remote_drops_the_same_column_as_an_unpushed_column_drop() -> Result<()> {
    assert_pull_fails_on_remote_schema_change_after_local_column_drop(
        &["ALTER TABLE t DROP COLUMN y"],
        "INSERT INTO t VALUES (2, 'x2', 'z2')",
    )
}

#[test]
fn pull_fails_when_remote_recreates_a_table_with_an_unpushed_column_drop() -> Result<()> {
    assert_pull_fails_on_remote_schema_change_after_local_column_drop(
        &[
            "DROP TABLE t",
            "CREATE TABLE t (id INTEGER PRIMARY KEY, x, z, y)",
        ],
        "INSERT INTO t VALUES (2, 'x2', 'z2', 'y2')",
    )
}

#[test]
fn pull_fails_when_remote_adds_a_column_to_a_table_with_an_unpushed_column_drop() -> Result<()> {
    assert_pull_fails_on_remote_schema_change_after_local_column_drop(
        &["ALTER TABLE t ADD COLUMN w"],
        "INSERT INTO t VALUES (2, 'x2', 'y2', 'z2', 'w2')",
    )
}
