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

const TABLE_WITH_GENERATED_COLUMN_AND_INDEXED_USER: &[&str] = &[
    "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, k TEXT AS (lower(a)), u TEXT AS (k || '!'))",
    "CREATE INDEX t_u ON t (u)",
    "INSERT INTO t (id, a) VALUES (1, 'Aa')",
];

/// Changes the expression of `k`. Core refuses to drop `k` while `u` uses it,
/// so the remote drops the index of `u` and `u` first, and adds them again
/// last, with the same definitions.
const REDEFINE_GENERATED_COLUMN_AND_ITS_INDEXED_USER: &[&str] = &[
    "DROP INDEX t_u",
    "ALTER TABLE t DROP COLUMN u",
    "ALTER TABLE t DROP COLUMN k",
    "ALTER TABLE t ADD COLUMN k TEXT AS (upper(a))",
    "ALTER TABLE t ADD COLUMN u TEXT AS (k || '!')",
    "CREATE INDEX t_u ON t (u)",
];

/// The remote changes a generated column that an indexed generated column
/// uses. The replica keeps `u`, which did not change, but it must drop `u`
/// before it drops `k`, and add it again.
#[test]
fn redefine_generated_column_and_its_indexed_user_in_one_transaction() -> Result<()> {
    let remote = Remote::new(TABLE_WITH_GENERATED_COLUMN_AND_INDEXED_USER)?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(REDEFINE_GENERATED_COLUMN_AND_ITS_INDEXED_USER)?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);

    // A pull matches the values of a remote row with the columns by position.
    // This insert shows that the replica has the column order of the server.
    remote.execute_transaction(&["INSERT INTO t (id, a) VALUES (2, 'Bb')"])?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// As `redefine_generated_column_and_its_indexed_user_in_one_transaction`, for
/// a new replica that starts from a checkpoint of the server before the change
/// and replays the change from the log.
#[test]
fn new_replica_replays_redefined_generated_column_and_its_indexed_user() -> Result<()> {
    let checkpoint = Remote::new(TABLE_WITH_GENERATED_COLUMN_AND_INDEXED_USER)?;
    let remote = Remote::new(TABLE_WITH_GENERATED_COLUMN_AND_INDEXED_USER)?;
    // A bootstrap checkpoints the remote, so its log holds only the change.
    remote.bootstrap_replica()?;
    remote.execute_transaction(REDEFINE_GENERATED_COLUMN_AND_ITS_INDEXED_USER)?;

    let replica = open_new_replica(&remote, checkpoint)?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote drops a column and its generated helper column, and gives the
/// indexed generated column that used the helper a new expression. It drops
/// the index and that column first, and adds them again last. The replica
/// keeps that column by its name, so it must drop it before the helper, and
/// add it again.
#[test]
fn drop_column_and_its_helper_and_redefine_their_indexed_user_in_one_transaction() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, k TEXT AS (lower(a)), b TEXT, l TEXT AS (lower(b)), u TEXT AS (k || '!'))",
        "CREATE INDEX t_u ON t (u)",
        "INSERT INTO t (id, a, b) VALUES (1, 'Aa', 'Bb')",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "DROP INDEX t_u",
        "ALTER TABLE t DROP COLUMN u",
        "ALTER TABLE t DROP COLUMN k",
        "ALTER TABLE t DROP COLUMN a",
        "ALTER TABLE t ADD COLUMN u TEXT AS (l || '!')",
        "CREATE INDEX t_u ON t (u)",
    ])?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);

    remote.execute_transaction(&["INSERT INTO t (id, b) VALUES (2, 'Cc')"])?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote drops `motto`, `wand_key` and `wand`, which `motto` uses,
/// renames `house` to `patronus`, and adds `motto` again with a new
/// expression. The replica runs the rename first, so it must read its columns
/// again before it drops `motto` and checks the columns that stay.
#[test]
fn rename_column_and_redefine_generated_column_in_one_transaction() -> Result<()> {
    let remote = Remote::new(&[
        r#"CREATE TABLE "muggles-in-london" (
            id TEXT PRIMARY KEY,
            house TEXT,
            wand TEXT,
            wand_key TEXT AS (lower(wand)),
            motto TEXT AS (wand || '!')
        )"#,
        r#"INSERT INTO "muggles-in-london" (id, house, wand) VALUES ('a', 'Gryffindor', 'Holly')"#,
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        r#"ALTER TABLE "muggles-in-london" DROP COLUMN motto"#,
        r#"ALTER TABLE "muggles-in-london" DROP COLUMN wand_key"#,
        r#"ALTER TABLE "muggles-in-london" DROP COLUMN wand"#,
        r#"ALTER TABLE "muggles-in-london" RENAME COLUMN house TO patronus"#,
        r#"ALTER TABLE "muggles-in-london" ADD COLUMN motto TEXT AS (patronus || '!')"#,
    ])?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);

    remote.execute_transaction(&[
        r#"INSERT INTO "muggles-in-london" (id, patronus) VALUES ('b', 'Stag')"#,
    ])?;
    replica.pull()?;
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

/// The remote renames a column, and gives its old name to a generated column
/// that a regular column follows. The replica must rename both columns. It must
/// not drop the generated column, because then it takes the regular column as
/// renamed to the old name.
#[test]
fn rename_generated_column_to_name_of_renamed_column() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, x INT, l INT AS (id * 2), m INT)",
        "INSERT INTO t (id, x, m) VALUES (1, 10, 20)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t RENAME COLUMN x TO y",
        "ALTER TABLE t RENAME COLUMN l TO x",
    ])?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote renames `wand` to `wand_wood`, and then `house` to `wand`. The
/// replica matches the columns by position, so it takes `house` as dropped,
/// and `motto`, which uses `house`, as renamed to `wand_wood`.
#[test]
fn rename_chain_beside_generated_column_keeps_values() -> Result<()> {
    assert_pulls_keep_values(
        r#"CREATE TABLE "muggles-in-london" (
            id TEXT PRIMARY KEY,
            house TEXT,
            wand TEXT,
            motto TEXT AS (house || '!')
        )"#,
        &[
            r#"ALTER TABLE "muggles-in-london" RENAME COLUMN wand TO wand_wood"#,
            r#"ALTER TABLE "muggles-in-london" RENAME COLUMN house TO wand"#,
        ],
    )
}

/// The remote renames `wand` to `wand_key`, which was a generated column, and
/// then `house` to `wand`. The replica matches the columns by position, so it
/// takes `house` as dropped and keeps the generated `wand_key` where the
/// remote has the regular `wand_key`.
#[test]
fn rename_chain_onto_generated_column_keeps_values() -> Result<()> {
    assert_pulls_keep_values(
        r#"CREATE TABLE "muggles-in-london" (
            id TEXT PRIMARY KEY,
            house TEXT,
            wand TEXT,
            wand_key TEXT AS (id || '-key'),
            motto TEXT AS (upper(house))
        )"#,
        &[
            r#"ALTER TABLE "muggles-in-london" DROP COLUMN motto"#,
            r#"ALTER TABLE "muggles-in-london" DROP COLUMN wand_key"#,
            r#"ALTER TABLE "muggles-in-london" RENAME COLUMN wand TO wand_key"#,
            r#"ALTER TABLE "muggles-in-london" RENAME COLUMN house TO wand"#,
            r#"ALTER TABLE "muggles-in-london" ADD COLUMN motto TEXT AS (upper(wand))"#,
        ],
    )
}

/// The remote drops every column but `id`, and adds `wand` again as a
/// generated column. The replica matches `wand` by name, so it keeps the
/// regular `wand` where the remote has the generated `wand`.
#[test]
fn regular_column_added_again_as_generated_column_keeps_values() -> Result<()> {
    assert_pulls_keep_values(
        r#"CREATE TABLE "muggles-in-london" (
            id TEXT PRIMARY KEY,
            house TEXT,
            wand TEXT,
            motto TEXT AS (upper(house))
        )"#,
        &[
            r#"ALTER TABLE "muggles-in-london" DROP COLUMN motto"#,
            r#"ALTER TABLE "muggles-in-london" DROP COLUMN house"#,
            r#"ALTER TABLE "muggles-in-london" DROP COLUMN wand"#,
            r#"ALTER TABLE "muggles-in-london" ADD COLUMN wand TEXT AS (id || '-wand')"#,
            r#"ALTER TABLE "muggles-in-london" ADD COLUMN motto TEXT AS (id || '!')"#,
        ],
    )
}

/// The remote drops every column but `id`, and adds a new generated column
/// `wand_key`. The replica matches the columns by position, so it takes
/// `house` as renamed to the regular `wand_key`.
#[test]
fn new_generated_column_matched_by_position_keeps_values() -> Result<()> {
    assert_pulls_keep_values(
        r#"CREATE TABLE "muggles-in-london" (
            id TEXT PRIMARY KEY,
            house TEXT,
            wand TEXT,
            motto TEXT AS (upper(wand))
        )"#,
        &[
            r#"ALTER TABLE "muggles-in-london" DROP COLUMN motto"#,
            r#"ALTER TABLE "muggles-in-london" DROP COLUMN wand"#,
            r#"ALTER TABLE "muggles-in-london" DROP COLUMN house"#,
            r#"ALTER TABLE "muggles-in-london" ADD COLUMN wand_key TEXT AS (id || '-key')"#,
            r#"ALTER TABLE "muggles-in-london" ADD COLUMN motto TEXT AS (id || '!')"#,
        ],
    )
}

/// The remote drops `wand`, and renames `house` to `wand`. The replica matches
/// the columns by position, so it takes `house` as dropped and keeps its own
/// `wand`, which has another type than the remote `wand`.
#[test]
fn rename_used_column_to_dropped_name_keeps_values() -> Result<()> {
    assert_pulls_keep_values(
        r#"CREATE TABLE "muggles-in-london" (
            id TEXT PRIMARY KEY,
            house TEXT,
            wand INT,
            motto TEXT AS (house || '!')
        )"#,
        &[
            r#"ALTER TABLE "muggles-in-london" DROP COLUMN wand"#,
            r#"ALTER TABLE "muggles-in-london" RENAME COLUMN house TO wand"#,
        ],
    )
}

/// Bootstraps a replica of a remote that created the table with `create_table`
/// and inserted a row. Then the remote runs `transaction`, and the replica
/// pulls. Then the remote inserts another row, and the replica pulls again.
/// A pull can fail, but after a pull that succeeds the replica must have the
/// schema and the values of the remote.
fn assert_pulls_keep_values(create_table: &str, transaction: &[&str]) -> Result<()> {
    let remote = Remote::new(&[
        create_table,
        r#"INSERT INTO "muggles-in-london" (id, house, wand) VALUES ('a', 'Gryffindor', 'Holly')"#,
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(transaction)?;
    if replica.pull().is_ok() {
        assert_eq!(replica.snapshot()?, remote.snapshot()?);
    }
    remote.execute_transaction(&[r#"INSERT INTO "muggles-in-london" (id) VALUES ('b')"#])?;
    if replica.pull().is_ok() {
        assert_eq!(replica.snapshot()?, remote.snapshot()?);
    }
    Ok(())
}

#[test]
fn pull_after_unpushed_drop_table() -> Result<()> {
    let remote = Remote::new(&["CREATE TABLE r (x)", "CREATE TABLE t (x)"])?;
    let replica = remote.bootstrap_replica()?;

    replica.execute("DROP TABLE t")?;
    remote.execute_transaction(&["INSERT INTO r VALUES (1)"])?;
    replica.pull()?;
    replica.pull()?;

    let expected = Remote::new(&["CREATE TABLE r (x)", "INSERT INTO r VALUES (1)"])?;
    assert_eq!(replica.snapshot()?, expected.snapshot()?);
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
