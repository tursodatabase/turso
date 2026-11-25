use std::sync::Arc;

use turso_sync_engine::Result;

use crate::harness::{database_opts, InProcessServer, Remote, Replica};

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

/// The remote changes a generated column in place, and the column is not the
/// last one. The replica must keep the column in its place: the next change
/// is matched by position, and with a moved column it loses the values of
/// `note`.
#[test]
fn alter_middle_generated_column_in_place() -> Result<()> {
    assert_alter_column_keeps_table(
        &["CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, g TEXT AS (upper(a)), note TEXT)"],
        &["ALTER TABLE t ALTER COLUMN g TO g TEXT AS (lower(a))"],
    )
}

/// The remote changes a generated column in place, and another generated
/// column uses it. Neither is the last column.
#[test]
fn alter_middle_generated_column_that_another_uses_in_place() -> Result<()> {
    assert_alter_column_keeps_table(
        &["CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, g TEXT AS (upper(a)), h TEXT AS (g || '!'), note TEXT)"],
        &["ALTER TABLE t ALTER COLUMN g TO g TEXT AS (lower(a))"],
    )
}

/// The remote drops the index of a generated column, changes the column in
/// place, and creates the index again, in one transaction.
#[test]
fn alter_indexed_middle_generated_column_in_place() -> Result<()> {
    assert_alter_column_keeps_table(
        &[
            "CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, g TEXT AS (upper(a)), note TEXT)",
            "CREATE INDEX t_g ON t (g)",
        ],
        &[
            "DROP INDEX t_g",
            "ALTER TABLE t ALTER COLUMN g TO g TEXT AS (lower(a))",
            "CREATE INDEX t_g ON t (g)",
        ],
    )
}

/// The remote reverses the dependency between two generated columns in one
/// transaction.
#[test]
fn reverse_dependency_between_generated_columns() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, v INT AS (a + 1), w INT AS (v * 10))",
        "INSERT INTO t (id, a) VALUES (1, 5)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t ALTER COLUMN w TO w INT AS (a * 10)",
        "ALTER TABLE t ALTER COLUMN v TO v INT AS (w + 1)",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote moves a column to the end of the table, and a generated column
/// then reads it, in one transaction.
#[test]
fn move_column_that_generated_column_then_reads() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id TEXT PRIMARY KEY, x TEXT, y TEXT, g TEXT AS (y))",
        "INSERT INTO t (id, x, y) VALUES ('k', 'x1', 'y1')",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t DROP COLUMN x",
        "ALTER TABLE t ADD COLUMN x TEXT",
        "ALTER TABLE t ALTER COLUMN g TO g TEXT AS (x)",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote changes a generated column in the middle of the table in place,
/// and drops and adds again its last column, a STORED generated column. Core
/// cannot change a STORED column in place, so the replica must drop and add
/// it again too. ADD COLUMN accepts a STORED column only while the table is
/// empty.
#[test]
fn alter_middle_generated_column_and_redefine_last_stored_column() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, g TEXT AS (upper(a)), note TEXT, s TEXT AS (lower(a)) STORED)",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t ALTER COLUMN g TO g TEXT AS (lower(a))",
        "ALTER TABLE t DROP COLUMN s",
        "ALTER TABLE t ADD COLUMN s TEXT AS (upper(a)) STORED",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The replica has indexes that the remote does not have: on a generated
/// column that the remote changes in place, on a generated column that uses
/// it, and on an expression of it with a WHERE clause. In MVCC mode core
/// cannot build an index again during ALTER COLUMN, so the replica must drop
/// them first and create them again.
#[test]
fn alter_middle_generated_column_with_local_indexes() -> Result<()> {
    let setup = [
        "CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, g TEXT AS (upper(a)), h TEXT AS (g || '!'), note TEXT)",
        "INSERT INTO t (id, a, note) VALUES ('x', 'Hi', 'n')",
    ];
    let local = [
        "CREATE INDEX t_g ON t (g)",
        "CREATE INDEX t_h ON t (h)",
        "CREATE INDEX t_g_lower ON t (lower(g)) WHERE g IS NOT NULL",
    ];
    let remote_change = "ALTER TABLE t ALTER COLUMN g TO g TEXT AS (lower(a))";
    let remote = Remote::new(&setup)?;
    let replica = remote.bootstrap_replica()?;
    for sql in local {
        replica.execute(sql)?;
    }

    remote.execute_transaction(&[remote_change])?;
    replica.pull()?;

    let expected = setup
        .iter()
        .chain([remote_change].iter())
        .chain(local.iter())
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(replica.snapshot()?, Remote::new(&expected)?.snapshot()?);
    Ok(())
}

/// The remote changes in place a generated column that has a foreign key, and
/// the column is not the last one.
#[test]
fn alter_middle_generated_column_with_foreign_key() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE p (id INTEGER PRIMARY KEY)",
        "INSERT INTO p VALUES (1), (2)",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, g INT AS (a + 0) REFERENCES p (id), note TEXT)",
        "INSERT INTO t (id, a, note) VALUES (1, 1, 'n')",
    ])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[
        "ALTER TABLE t ALTER COLUMN g TO g INT AS (a + 1) REFERENCES p (id)",
    ])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote changes a middle generated column that has a CHECK, and adds a
/// column. Core prints a CHECK in another form after ALTER COLUMN than after
/// ADD COLUMN, so the column of the replica still differs in form from the
/// remote one after the change in place. The replica must not take that as
/// another change and move the column. The texts differ in form, so the test
/// compares the columns and the rows.
#[test]
fn alter_middle_generated_column_with_check_and_add_column() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, g INT AS (length(a)) CHECK(g>0), note TEXT)",
        "INSERT INTO t (id, a, note) VALUES ('x', 'Hi', 'n')",
    ])?;
    let replica = remote.bootstrap_replica()?;

    for transaction in [
        &[
            "ALTER TABLE t ALTER COLUMN g TO g INT AS (length(a) + 1) CHECK(g>0)",
            "ALTER TABLE t ADD COLUMN c TEXT",
        ][..],
        &["ALTER TABLE t RENAME COLUMN note TO memo"],
    ] {
        remote.execute_transaction(transaction)?;
        replica.pull()?;
        for query in ["PRAGMA table_xinfo(t)", "SELECT * FROM t"] {
            assert_eq!(replica.query(query)?, remote.query(query)?, "{query}");
        }
    }
    Ok(())
}

/// The remote adds a foreign key to a generated column that is not the last
/// column. The replica must change the column in place and add the key.
#[test]
fn add_foreign_key_to_middle_generated_column() -> Result<()> {
    assert_alter_column_keeps_table(
        &[
            "CREATE TABLE p (id TEXT PRIMARY KEY)",
            "CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, g TEXT AS (upper(a)), note TEXT)",
        ],
        &["ALTER TABLE t ALTER COLUMN g TO g TEXT AS (upper(a)) REFERENCES p (id)"],
    )
}

/// The remote changes the foreign key of its last generated column. Core
/// refuses to drop a column that a foreign key uses, so the replica must change
/// the column in place.
#[test]
fn change_foreign_key_of_last_generated_column() -> Result<()> {
    assert_alter_column_keeps_table(
        &[
            "CREATE TABLE p (id TEXT PRIMARY KEY)",
            "CREATE TABLE q (id TEXT PRIMARY KEY)",
            "CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, note TEXT, g TEXT AS (upper(a)) REFERENCES p (id))",
        ],
        &["ALTER TABLE t ALTER COLUMN g TO g TEXT AS (upper(a)) REFERENCES q (id)"],
    )
}

/// The replica has an index on the last generated column, and the remote
/// changes that column. Core refuses to drop an indexed column, so the replica
/// must change the column in place, and drop and create the index again.
#[test]
fn alter_last_generated_column_with_local_index() -> Result<()> {
    let setup = [
        "CREATE TABLE t (id TEXT PRIMARY KEY, a TEXT, note TEXT, g TEXT AS (upper(a)))",
        "INSERT INTO t (id, a, note) VALUES ('x', 'Hi', 'n')",
    ];
    let local = "CREATE INDEX t_g ON t (g)";
    let remote_change = "ALTER TABLE t ALTER COLUMN g TO g TEXT AS (lower(a))";
    let remote = Remote::new(&setup)?;
    let replica = remote.bootstrap_replica()?;
    replica.execute(local)?;

    remote.execute_transaction(&[remote_change])?;
    replica.pull()?;

    let expected = Remote::new(&[setup[0], setup[1], remote_change, local])?;
    assert_eq!(replica.snapshot()?, expected.snapshot()?);
    Ok(())
}

/// The remote renames a column, and later adds one. Core then writes the
/// expression of the middle generated column again, without the quotes. The
/// replica must change the column in place to the new text, and not move it.
#[test]
fn add_column_after_rename_beside_generated_column_with_quoted_name() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, g TEXT AS (upper(\"a\")), b TEXT)",
        "INSERT INTO t (id, a, b) VALUES (1, 'a1', 'b1')",
    ])?;
    let replica = remote.bootstrap_replica()?;

    for change in [
        "ALTER TABLE t RENAME COLUMN b TO c",
        "ALTER TABLE t ADD COLUMN d TEXT",
    ] {
        remote.execute_transaction(&[change])?;
        replica.pull()?;
        assert_eq!(replica.snapshot()?, remote.snapshot()?);
    }
    Ok(())
}

/// The replica changes a middle generated column and does not push the change:
/// core writes no change row for ALTER COLUMN, so it never reaches the server.
/// Then the remote adds a column. The replica must change the column in place
/// to the definition of the server.
#[test]
fn local_alter_of_middle_generated_column_takes_server_definition() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, g INT AS (a + 1), b INT)",
        "INSERT INTO t (id, a, b) VALUES (1, 10, 20)",
    ])?;
    let replica = remote.bootstrap_replica()?;
    replica.execute("ALTER TABLE t ALTER COLUMN g TO g INT AS (a + 5)")?;

    remote.execute_transaction(&["ALTER TABLE t ADD COLUMN c INT"])?;
    replica.pull()?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// A new replica starts from the last checkpoint of the server and replays the
/// log after it: an ALTER COLUMN of a middle generated column, and then an ADD
/// COLUMN. The new replica must be the same as the server.
#[test]
fn new_replica_replays_alter_of_middle_generated_column() -> Result<()> {
    let setup = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, v TEXT AS (upper(a)) VIRTUAL, c TEXT, d TEXT)",
        "INSERT INTO t (id, a, c, d) VALUES (1, 'aa', 'cc', 'dd')",
    ];
    let checkpoint = Remote::new(&setup)?;
    let remote = Remote::new(&setup)?;
    // A bootstrap checkpoints the remote, so its log holds only the changes
    // after it.
    remote.bootstrap_replica()?;
    remote
        .execute_transaction(&["ALTER TABLE t ALTER COLUMN v TO v TEXT AS (lower(a)) VIRTUAL"])?;
    remote.execute_transaction(&["ALTER TABLE t ADD COLUMN e TEXT"])?;

    let replica = Replica::open(
        &Arc::new(tempfile::tempdir()?),
        InProcessServer::with_bootstrap_base(remote.clone(), checkpoint),
        database_opts(),
    )?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// The remote renames a column, and later adds one. Core then writes the
/// expression of the middle STORED generated column again, without the quotes.
/// Core cannot change a STORED column in place, and dropping and adding it
/// again would move it. So the pull must fail with an error that names the
/// column, and the replica must keep its values.
#[test]
fn change_of_middle_stored_generated_column_fails_the_pull() -> Result<()> {
    let remote = Remote::new(&[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, s TEXT AS (upper(\"a\")) STORED, b TEXT)",
        "INSERT INTO t (id, a, b) VALUES (1, 'a1', 'b1')",
    ])?;
    let replica = remote.bootstrap_replica()?;
    remote.execute_transaction(&["ALTER TABLE t RENAME COLUMN b TO c"])?;
    replica.pull()?;
    let before = replica.snapshot()?;

    remote.execute_transaction(&["ALTER TABLE t ADD COLUMN d TEXT"])?;
    let error = replica.pull().expect_err("the pull must fail").to_string();
    assert!(
        error.contains(
            "the pull cannot change the generated column s of table t to its definition on the server"
        ),
        "{error}"
    );
    assert_eq!(replica.snapshot()?, before);
    Ok(())
}

/// Bootstraps a replica of a remote that ran `setup` and inserted a row into
/// table `t`. Then the remote runs `transaction`, and later it renames `note`.
/// After each pull, the replica must be the same as the remote.
fn assert_alter_column_keeps_table(setup: &[&str], transaction: &[&str]) -> Result<()> {
    let remote = Remote::new(setup)?;
    remote.execute_transaction(&["INSERT INTO t (id, a, note) VALUES ('x', 'Hi', 'n')"])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(transaction)?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);

    remote.execute_transaction(&["ALTER TABLE t RENAME COLUMN note TO memo"])?;
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
