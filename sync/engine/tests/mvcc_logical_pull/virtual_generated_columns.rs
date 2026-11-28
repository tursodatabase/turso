use std::sync::Arc;

use turso_sync_engine::Result;

use crate::harness::{database_opts, InProcessServer, Remote, Replica};

/// ADD COLUMN and DROP COLUMN on the server write the SQL text of this table
/// again, and the new text does not have the VIRTUAL keyword.
const CREATE_WITH_VIRTUAL_KEYWORD: &str = "CREATE TABLE \"muggles-in-london\" (id TEXT PRIMARY KEY, surname TEXT, surname_key TEXT AS (LOWER (surname)) VIRTUAL, house_rank INTEGER)";

const INSERT_ROW: &str =
    "INSERT INTO \"muggles-in-london\" (id, surname, house_rank) VALUES ('a', 'Smith', 7)";
const ADD_COLUMN: &str = "ALTER TABLE \"muggles-in-london\" ADD COLUMN \"wand-length\" TEXT";
const DROP_COLUMN: &str = "ALTER TABLE \"muggles-in-london\" DROP COLUMN \"wand-length\"";

/// Checks a new replica that replays the whole log. After the ADD COLUMN, the
/// replica must keep the column order of the server. If it does not, the
/// DROP COLUMN renames the wrong column and the pull fails with "duplicate
/// column name".
#[test]
fn replay_log_add_and_drop_column_with_virtual_keyword() -> Result<()> {
    let remote = Remote::new(&[
        CREATE_WITH_VIRTUAL_KEYWORD,
        INSERT_ROW,
        ADD_COLUMN,
        DROP_COLUMN,
    ])?;
    let replica = replay_log(&remote)?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// Checks a replica that starts from a snapshot of the table with the VIRTUAL
/// keyword, and then pulls each change.
#[test]
fn pull_add_and_drop_column_with_virtual_keyword() -> Result<()> {
    let remote = Remote::new(&[CREATE_WITH_VIRTUAL_KEYWORD, INSERT_ROW])?;
    let replica = remote.bootstrap_replica()?;

    remote.execute_transaction(&[ADD_COLUMN])?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);

    remote.execute_transaction(&[DROP_COLUMN])?;
    replica.pull()?;
    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// Checks that an ADD COLUMN alone keeps the column order of the server on a
/// new replica that replays the whole log.
#[test]
fn replay_log_add_column_with_virtual_keyword() -> Result<()> {
    let remote = Remote::new(&[CREATE_WITH_VIRTUAL_KEYWORD, INSERT_ROW, ADD_COLUMN])?;
    let replica = replay_log(&remote)?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// Checks a RENAME COLUMN after the ADD COLUMN on a new replica that replays
/// the whole log. If the column order on the replica is wrong, the replica
/// renames the wrong column and loses the value of house_rank without an
/// error.
#[test]
fn replay_log_add_and_rename_column_with_virtual_keyword() -> Result<()> {
    let remote = Remote::new(&[
        CREATE_WITH_VIRTUAL_KEYWORD,
        INSERT_ROW,
        ADD_COLUMN,
        "ALTER TABLE \"muggles-in-london\" RENAME COLUMN house_rank TO house_order",
    ])?;
    let replica = replay_log(&remote)?;

    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

/// Opens a new replica that starts from an empty database and replays the
/// whole log of `remote`. A new replica does this when the server created the
/// table after its last checkpoint.
fn replay_log(remote: &Arc<Remote>) -> Result<Replica> {
    let dir = Arc::new(tempfile::tempdir()?);
    Replica::open(
        &dir,
        InProcessServer::with_empty_base(remote.clone()),
        database_opts(),
    )
}
