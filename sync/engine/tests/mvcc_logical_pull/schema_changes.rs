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

/// `tag` is a generated column that uses another generated column,
/// `surname_norm`.
const MUGGLES_IN_LONDON: &[&str] = &[
    r#"CREATE TABLE "muggles-in-london" (
        id TEXT PRIMARY KEY,
        surname TEXT,
        surname_norm TEXT AS (CAST (surname AS TEXT)),
        tag TEXT AS (LOWER (surname_norm))
    )"#,
    r#"INSERT INTO "muggles-in-london" (id, surname) VALUES ('a', 'Smith')"#,
];

/// Changes the expression of `surname_norm`. `tag` uses `surname_norm`, so
/// `tag` is dropped first and added again at the end with the same
/// expression.
const REDEFINE_SURNAME_NORM: &[&str] = &[
    r#"ALTER TABLE "muggles-in-london" DROP COLUMN tag"#,
    r#"ALTER TABLE "muggles-in-london" DROP COLUMN surname_norm"#,
    r#"ALTER TABLE "muggles-in-london" ADD COLUMN surname_norm TEXT AS (UPPER (surname))"#,
    r#"ALTER TABLE "muggles-in-london" ADD COLUMN tag TEXT AS (LOWER (surname_norm))"#,
];

#[test]
fn redefine_generated_column_used_by_another_in_one_transaction() -> Result<()> {
    let remote = Remote::new(MUGGLES_IN_LONDON)?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(REDEFINE_SURNAME_NORM)?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// `house` has two generated columns, `house_key` and `house_len`. `motto` is
/// a generated column that uses `house_key`.
const MUGGLES_WITH_HOUSE: &[&str] = &[
    r#"CREATE TABLE "muggles-in-london" (
        id TEXT PRIMARY KEY,
        house TEXT,
        house_key TEXT AS (lower(house)),
        house_len INT AS (length(house)),
        wand TEXT,
        wand_key TEXT AS (lower(wand)),
        motto TEXT AS (house_key || '!')
    )"#,
    r#"INSERT INTO "muggles-in-london" (id, house, wand) VALUES ('a', 'Gryffindor', 'Holly')"#,
];

/// Drops `house` and its generated columns. `motto` uses `house_key`, so
/// `motto` is dropped first. Then `motto` is added again at the end with an
/// expression that uses `wand_key`.
const DROP_HOUSE_AND_REDEFINE_MOTTO: &[&str] = &[
    r#"ALTER TABLE "muggles-in-london" DROP COLUMN motto"#,
    r#"ALTER TABLE "muggles-in-london" DROP COLUMN house_key"#,
    r#"ALTER TABLE "muggles-in-london" DROP COLUMN house_len"#,
    r#"ALTER TABLE "muggles-in-london" DROP COLUMN house"#,
    r#"ALTER TABLE "muggles-in-london" ADD COLUMN motto TEXT AS (wand_key || '!')"#,
];

#[test]
fn drop_column_with_generated_columns_and_redefine_their_user_in_one_transaction() -> Result<()> {
    let remote = Remote::new(MUGGLES_WITH_HOUSE)?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(DROP_HOUSE_AND_REDEFINE_MOTTO)?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);

    // A pull matches the values of a remote row to the columns by position.
    // This insert shows that the replica kept the column order of the server.
    remote.execute_transaction(&[
        r#"INSERT INTO "muggles-in-london" (id, wand) VALUES ('b', 'Elder')"#,
    ])?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// Changes the expression of `house_key`. `motto` uses `house_key`, and
/// `motto_len` uses `motto`, so the remote drops both first and adds them
/// again at the end with the same expressions.
#[test]
fn redefine_generated_column_used_through_another_in_one_transaction() -> Result<()> {
    let remote = Remote::new(&[
        r#"CREATE TABLE "muggles-in-london" (
            id TEXT PRIMARY KEY,
            house TEXT,
            house_key TEXT AS (lower(house)),
            motto TEXT AS (house_key || '!'),
            motto_len INT AS (length(motto))
        )"#,
        r#"INSERT INTO "muggles-in-london" (id, house) VALUES ('a', 'Gryffindor')"#,
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        r#"ALTER TABLE "muggles-in-london" DROP COLUMN motto_len"#,
        r#"ALTER TABLE "muggles-in-london" DROP COLUMN motto"#,
        r#"ALTER TABLE "muggles-in-london" DROP COLUMN house_key"#,
        r#"ALTER TABLE "muggles-in-london" ADD COLUMN house_key TEXT AS (upper(house))"#,
        r#"ALTER TABLE "muggles-in-london" ADD COLUMN motto TEXT AS (house_key || '!')"#,
        r#"ALTER TABLE "muggles-in-london" ADD COLUMN motto_len INT AS (length(motto))"#,
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote drops a column and the generated column that uses it. The
/// generated column comes before that column in the table, so the replica
/// must drop the columns in another order than from the last to the first.
#[test]
fn drop_column_and_generated_column_declared_before_it() -> Result<()> {
    let remote = Remote::new(&[
        r#"CREATE TABLE "muggles-in-london" (
            id TEXT PRIMARY KEY,
            house_key TEXT AS (lower(house)),
            house TEXT,
            wand TEXT
        )"#,
        r#"INSERT INTO "muggles-in-london" (id, house, wand) VALUES ('a', 'Gryffindor', 'Holly')"#,
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        r#"ALTER TABLE "muggles-in-london" DROP COLUMN house_key"#,
        r#"ALTER TABLE "muggles-in-london" DROP COLUMN house"#,
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote drops a generated column and one of the columns that it uses.
/// It renames the other column, and it adds the generated column again on the
/// renamed column.
#[test]
fn drop_and_rename_columns_of_generated_column_in_one_transaction() -> Result<()> {
    let remote = Remote::new(&[
        r#"CREATE TABLE "muggles-in-london" (
            id TEXT PRIMARY KEY,
            house TEXT,
            wand TEXT,
            motto TEXT AS (house || wand)
        )"#,
        r#"INSERT INTO "muggles-in-london" (id, house, wand) VALUES ('a', 'Gryffindor', 'Holly')"#,
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        r#"ALTER TABLE "muggles-in-london" DROP COLUMN motto"#,
        r#"ALTER TABLE "muggles-in-london" DROP COLUMN house"#,
        r#"ALTER TABLE "muggles-in-london" RENAME COLUMN wand TO wand_wood"#,
        r#"ALTER TABLE "muggles-in-london" ADD COLUMN motto TEXT AS (upper(wand_wood))"#,
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
