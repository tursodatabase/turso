use turso_core::Value;
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
fn rename_table_referenced_by_foreign_key_and_change_it_in_one_transaction() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (x PRIMARY KEY, y)",
        "CREATE TABLE child (r REFERENCES t (x))",
        "INSERT INTO t VALUES (1, 2)",
        "INSERT INTO child VALUES (1)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t RENAME TO u",
        "ALTER TABLE u ADD COLUMN z",
        "UPDATE u SET z = 3 WHERE x = 1",
        "INSERT INTO u VALUES (4, 5, 6)",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn rename_table_to_the_old_name_of_another_renamed_table() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE a (x)",
        "CREATE TABLE b (y)",
        "INSERT INTO a VALUES (1)",
        "INSERT INTO b VALUES (2)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["ALTER TABLE b RENAME TO c", "ALTER TABLE a RENAME TO b"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn swap_table_names() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE a (x)",
        "CREATE TABLE b (y)",
        "INSERT INTO a VALUES (1)",
        "INSERT INTO b VALUES (2)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE a RENAME TO tmp",
        "ALTER TABLE b RENAME TO a",
        "ALTER TABLE tmp RENAME TO b",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn swap_table_names_and_change_a_table_named_like_the_temporary_name() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE a (x)",
        "CREATE TABLE b (y)",
        "CREATE TABLE turso_sync_rename_1 (z)",
        "INSERT INTO a VALUES (1)",
        "INSERT INTO b VALUES (2)",
        "INSERT INTO turso_sync_rename_1 VALUES (3)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE a RENAME TO tmp",
        "ALTER TABLE b RENAME TO a",
        "ALTER TABLE tmp RENAME TO b",
        "ALTER TABLE turso_sync_rename_1 ADD COLUMN w",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn rename_table_next_to_a_table_named_like_a_temporary_name() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE a (x)",
        "CREATE TABLE turso_sync_rename_2 (q)",
        "INSERT INTO a VALUES (1)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["ALTER TABLE a RENAME TO b"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn rename_table_to_a_name_like_a_temporary_name() -> Result<()> {
    let remote = Remote::new(&["CREATE TABLE a (x)", "INSERT INTO a VALUES (1)"])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["ALTER TABLE a RENAME TO turso_sync_rename_2"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn rename_table_to_a_name_with_uppercase_letters() -> Result<()> {
    let remote = Remote::new(&["CREATE TABLE t (x)", "INSERT INTO t VALUES (1)"])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["ALTER TABLE t RENAME TO U"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn swap_table_names_when_the_replica_has_a_table_named_like_the_temporary_name() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE a (x)",
        "CREATE TABLE b (y)",
        "INSERT INTO a VALUES (1)",
        "INSERT INTO b VALUES (2)",
    ])?;
    let replica = remote.bootstrap_replica()?;
    replica.execute("CREATE TABLE turso_sync_rename_1 (z)")?;

    remote.execute_transaction(&[
        "ALTER TABLE a RENAME TO c",
        "ALTER TABLE b RENAME TO a",
        "ALTER TABLE c RENAME TO b",
    ])?;
    replica.pull()?;
    remote.execute_transaction(&["CREATE TABLE turso_sync_rename_1 (z)"])?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn rename_table_with_unpushed_local_changes() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (x PRIMARY KEY, y)",
        "INSERT INTO t VALUES (1, 2)",
        "INSERT INTO t VALUES (3, 4)",
    ])?;
    let replica = remote.bootstrap_replica()?;
    replica.execute("INSERT INTO t VALUES (5, 6)")?;
    replica.execute("UPDATE t SET y = 7 WHERE x = 1")?;
    replica.execute("DELETE FROM t WHERE x = 3")?;

    remote.execute_transaction(&["ALTER TABLE t RENAME TO u"])?;
    replica.pull()?;
    replica.pull()?;

    assert_eq!(
        replica.query("SELECT name FROM sqlite_schema WHERE name IN ('t', 'u')")?,
        vec![vec![Value::build_text("u")]]
    );
    assert_eq!(
        replica.query("SELECT * FROM u ORDER BY x")?,
        vec![
            vec![Value::from_i64(1), Value::from_i64(7)],
            vec![Value::from_i64(5), Value::from_i64(6)],
        ]
    );
    Ok(())
}

#[test]
fn pull_remote_changes_after_a_rename_with_unpushed_local_changes() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (x PRIMARY KEY, y)",
        "INSERT INTO t VALUES (1, 2)",
    ])?;
    let replica = remote.bootstrap_replica()?;
    replica.execute("INSERT INTO t VALUES (5, 6)")?;

    remote.execute_transaction(&["ALTER TABLE t RENAME TO u"])?;
    replica.pull()?;
    remote.execute_transaction(&["INSERT INTO u VALUES (10, 10)"])?;
    replica.pull()?;
    remote.execute_transaction(&["ALTER TABLE u RENAME TO v"])?;
    replica.pull()?;
    remote.execute_transaction(&["INSERT INTO v VALUES (11, 11)"])?;
    replica.pull()?;

    assert_eq!(
        replica.query("SELECT * FROM v ORDER BY x")?,
        vec![
            vec![Value::from_i64(1), Value::from_i64(2)],
            vec![Value::from_i64(5), Value::from_i64(6)],
            vec![Value::from_i64(10), Value::from_i64(10)],
            vec![Value::from_i64(11), Value::from_i64(11)],
        ]
    );
    Ok(())
}

#[test]
fn rename_table_right_after_a_replace_base_snapshot_with_unpushed_local_changes() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (x PRIMARY KEY, y)",
        "INSERT INTO t VALUES (1, 2)",
    ])?;
    let replica = remote.bootstrap_replica()?;
    replica.execute("INSERT INTO t VALUES (5, 6)")?;

    remote.answer_next_pull_with_replace_base_snapshot()?;
    remote.execute_transaction(&["ALTER TABLE t RENAME TO u"])?;
    replica.pull()?;
    remote.execute_transaction(&["INSERT INTO u VALUES (10, 10)"])?;
    replica.pull()?;

    assert_eq!(
        replica.query("SELECT * FROM u ORDER BY x")?,
        vec![
            vec![Value::from_i64(1), Value::from_i64(2)],
            vec![Value::from_i64(5), Value::from_i64(6)],
            vec![Value::from_i64(10), Value::from_i64(10)],
        ]
    );
    Ok(())
}

#[test]
fn swap_table_names_with_unpushed_local_changes() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE a (x PRIMARY KEY)",
        "CREATE TABLE b (y PRIMARY KEY)",
        "INSERT INTO a VALUES (1)",
        "INSERT INTO b VALUES (2)",
    ])?;
    let replica = remote.bootstrap_replica()?;
    replica.execute("INSERT INTO a VALUES (3)")?;
    replica.execute("INSERT INTO b VALUES (4)")?;

    remote.execute_transaction(&[
        "ALTER TABLE a RENAME TO c",
        "ALTER TABLE b RENAME TO a",
        "ALTER TABLE c RENAME TO b",
    ])?;
    replica.pull()?;
    remote.execute_transaction(&["INSERT INTO a VALUES (6)", "INSERT INTO b VALUES (7)"])?;
    replica.pull()?;

    assert_eq!(
        replica.query("SELECT * FROM a ORDER BY y")?,
        vec![
            vec![Value::from_i64(2)],
            vec![Value::from_i64(4)],
            vec![Value::from_i64(6)]
        ]
    );
    assert_eq!(
        replica.query("SELECT * FROM b ORDER BY x")?,
        vec![
            vec![Value::from_i64(1)],
            vec![Value::from_i64(3)],
            vec![Value::from_i64(7)]
        ]
    );
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
