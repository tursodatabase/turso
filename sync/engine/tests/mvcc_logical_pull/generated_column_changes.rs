use turso_sync_engine::Result;

use crate::harness::Remote;

/// The remote drops its last generated column and adds it again with another
/// expression. The server writes the table text again, without the quotes that
/// the table name does not need. The replica must store the same text.
#[test]
fn redefine_last_generated_column_of_table_with_quoted_name() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE \"muggles\" (id TEXT PRIMARY KEY, surname VARCHAR(40), tag TEXT AS (LOWER (surname)))",
        "INSERT INTO \"muggles\" (id, surname) VALUES ('a', 'Smith')",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE muggles DROP COLUMN tag",
        "ALTER TABLE muggles ADD COLUMN tag TEXT AS (UPPER (surname))",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote adds a column to a table whose generated column has NOT NULL
/// after AS. The server writes the table text again with NOT NULL before AS.
/// The replica must not take that as a changed generated column, so the
/// generated column must keep its position.
#[test]
fn add_column_beside_generated_column_with_not_null_after_as() -> Result<()> {
    assert_add_column_keeps_table(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, g INT AS (a * 2) NOT NULL, b TEXT)",
        "INSERT INTO t (id, a, b) VALUES (1, 1, 'b1')",
    ])
}

/// The remote adds a column to a table whose generated column has a foreign
/// key. The server writes the foreign key again as a table constraint. Core
/// refuses to drop a column that a foreign key uses, so the replica must not
/// try to drop the generated column.
#[test]
fn add_column_beside_generated_column_with_foreign_key() -> Result<()> {
    assert_add_column_keeps_table(&[
        "CREATE TABLE p (id INTEGER PRIMARY KEY)",
        "INSERT INTO p VALUES (1)",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, b INTEGER, g INTEGER AS (b + 0) REFERENCES p (id), c TEXT)",
        "INSERT INTO t (id, b, c) VALUES (1, 1, 'c1')",
    ])
}

/// The remote adds a foreign key to its last generated column with ALTER
/// COLUMN. The replica must add the foreign key too. Core stores the foreign
/// key in the text of the table, but does not add it to the schema that the
/// connection of the remote uses. So the test compares the foreign keys of the
/// replica with those of a database made from the stored texts of the remote.
#[test]
fn add_foreign_key_to_last_generated_column() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE p (id INTEGER PRIMARY KEY)",
        "INSERT INTO p VALUES (1)",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, b INTEGER, c TEXT, g INTEGER AS (b + 0))",
        "INSERT INTO t (id, b, c) VALUES (1, 1, 'c1')",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t ALTER COLUMN g TO g INTEGER AS (b + 0) REFERENCES p (id)",
    ])?;
    replica.pull()?;

    let stored_texts = remote
        .query("SELECT sql FROM sqlite_schema WHERE name IN ('p', 't') ORDER BY name")?
        .into_iter()
        .map(|row| row[0].to_string())
        .collect::<Vec<_>>();
    let stored = Remote::new(&stored_texts.iter().map(String::as_str).collect::<Vec<_>>())?;
    let foreign_keys = "PRAGMA foreign_key_list(t)";
    assert_eq!(stored.query(foreign_keys)?.len(), 1);
    assert_eq!(replica.query(foreign_keys)?, stored.query(foreign_keys)?);
    let rows = "SELECT * FROM t ORDER BY id";
    assert_eq!(replica.query(rows)?, remote.query(rows)?);
    Ok(())
}

/// The remote adds a column to a table whose generated column has UNIQUE after
/// AS. The server writes the table text again with UNIQUE before AS. Core
/// refuses to drop a UNIQUE column, so the replica must not try to drop the
/// generated column.
#[test]
fn add_column_beside_unique_generated_column() -> Result<()> {
    assert_add_column_keeps_table(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, g INT AS (a * 2) UNIQUE, b TEXT)",
        "INSERT INTO t (id, a, b) VALUES (1, 1, 'b1')",
    ])
}

/// The remote changes only the case of the name of a generated column that is
/// not the last column, and that another generated column and an index use.
/// The replica must rename the column too, and not take it as a changed
/// generated column, which would move it.
#[test]
fn rename_middle_generated_column_in_case_only() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, g TEXT AS (upper(a)), h TEXT AS (g || '!'), b TEXT)",
        "CREATE INDEX t_g ON t (g)",
        "INSERT INTO t (id, a, b) VALUES (1, 'x', 'y')",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["ALTER TABLE t RENAME COLUMN g TO G"])?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);

    remote.execute_transaction(&["INSERT INTO t (id, a, b) VALUES (2, 'p', 'q')"])?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// Bootstraps a replica of a remote that ran `setup`. Then the remote adds a
/// column to table `t`, and the replica pulls. The replica must then be the
/// same as the remote.
fn assert_add_column_keeps_table(setup: &[&str]) -> Result<()> {
    let remote = Remote::new(setup)?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&["ALTER TABLE t ADD COLUMN x TEXT"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}
