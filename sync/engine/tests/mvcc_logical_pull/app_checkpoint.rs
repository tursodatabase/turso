//! The app runs `PRAGMA wal_checkpoint` on its own connection to a synced WAL database.

use std::sync::Arc;

use turso_core::{Connection, Value};
use turso_sync_engine::Result;

use crate::harness::{database_opts, InProcessServer, Remote, Replica};

/// A checkpoint by the app stops at the last WAL frame the sync engine knows is synced, so the
/// unpushed local row stays in the WAL and the next pull can roll it back and reapply it.
/// PASSIVE reports a partial checkpoint. FULL, RESTART and TRUNCATE report busy, as they do when a
/// reader blocks them.
#[test]
fn app_checkpoint_keeps_unpushed_frames_in_wal() -> Result<()> {
    let remote = Remote::new_wal(&["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('first')"])?;
    let dir = Arc::new(tempfile::tempdir()?);
    let replica = Replica::open(
        &dir,
        InProcessServer::pages_only(remote.clone()),
        database_opts(),
    )?;
    let conn = replica.connect()?;
    conn.execute("INSERT INTO t VALUES ('pre')")?;
    let [busy, log, checkpointed] = app_checkpoint(&conn, "PASSIVE")?;
    assert_eq!(busy, Some(0));
    assert!(log > Some(0), "log {log:?}");
    assert_eq!(checkpointed, Some(0));
    for mode in ["FULL", "RESTART", "TRUNCATE"] {
        assert_eq!(
            app_checkpoint(&conn, mode)?,
            [Some(1), None, None],
            "{mode}"
        );
    }

    remote.execute_transaction(&["INSERT INTO t VALUES ('second')"])?;
    replica.pull()?;

    assert_eq!(rows(&replica)?, ["first", "pre", "second"]);
    assert_eq!(replica.pending_local_changes()?, 1);
    Ok(())
}

/// The remote pages that a pull wrote are synced, so the app's PASSIVE checkpoint copies them. The
/// frames written after them (the sync engine's own bookkeeping, then a local row) stay in the WAL.
#[test]
fn app_checkpoint_copies_pulled_frames() -> Result<()> {
    let remote = Remote::new_wal(&["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('first')"])?;
    let dir = Arc::new(tempfile::tempdir()?);
    let replica = Replica::open(
        &dir,
        InProcessServer::pages_only(remote.clone()),
        database_opts(),
    )?;
    remote.execute_transaction(&["INSERT INTO t VALUES ('second')"])?;
    replica.pull()?;

    let conn = replica.connect()?;
    let [busy, log, pulled] = app_checkpoint(&conn, "PASSIVE")?;
    assert_eq!(busy, Some(0));
    assert!(pulled > Some(0), "checkpointed {pulled:?}");
    assert!(pulled < log, "checkpointed {pulled:?}, log {log:?}");

    conn.execute("INSERT INTO t VALUES ('pre')")?;
    let [busy, log_after_insert, checkpointed] = app_checkpoint(&conn, "PASSIVE")?;
    assert_eq!(busy, Some(0));
    assert!(log_after_insert > log);
    assert_eq!(checkpointed, pulled);

    remote.execute_transaction(&["INSERT INTO t VALUES ('third')"])?;
    replica.pull()?;
    assert_eq!(rows(&replica)?, ["first", "pre", "second", "third"]);
    assert_eq!(replica.pending_local_changes()?, 1);
    Ok(())
}

/// The sync checkpoint empties the WAL, so the next frames belong to a new WAL with new salts.
/// None of those frames is synced yet, so the app's checkpoint copies none of them.
#[test]
fn app_checkpoint_after_wal_restart_copies_nothing() -> Result<()> {
    let remote = Remote::new_wal(&["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('first')"])?;
    let dir = Arc::new(tempfile::tempdir()?);
    let replica = Replica::open(
        &dir,
        InProcessServer::pages_only(remote.clone()),
        database_opts(),
    )?;
    let conn = replica.connect()?;
    conn.execute("INSERT INTO t VALUES ('pre')")?;
    replica.checkpoint()?;

    conn.execute("INSERT INTO t VALUES ('pre2')")?;
    let [busy, log, checkpointed] = app_checkpoint(&conn, "PASSIVE")?;
    assert_eq!(busy, Some(0));
    assert!(log > Some(0), "log {log:?}");
    assert_eq!(checkpointed, Some(0));

    remote.execute_transaction(&["INSERT INTO t VALUES ('second')"])?;
    replica.pull()?;
    assert_eq!(rows(&replica)?, ["first", "pre", "pre2", "second"]);
    assert_eq!(replica.pending_local_changes()?, 2);
    Ok(())
}

/// Runs `PRAGMA wal_checkpoint(mode)` and returns its row: busy, log frames, checkpointed frames.
/// `None` is NULL.
fn app_checkpoint(conn: &Arc<Connection>, mode: &str) -> Result<[Option<i64>; 3]> {
    let rows = conn
        .prepare(format!("PRAGMA wal_checkpoint({mode})"))?
        .run_collect_rows()?;
    let [row] = rows.as_slice() else {
        panic!("{mode}: expected one row, got {rows:?}");
    };
    Ok(std::array::from_fn(|i| match &row[i] {
        Value::Null => None,
        value => Some(
            value
                .as_int()
                .unwrap_or_else(|| panic!("{mode}: unexpected value {value:?}")),
        ),
    }))
}

fn rows(replica: &Replica) -> Result<Vec<String>> {
    let rows = replica
        .connect()?
        .prepare("SELECT x FROM t ORDER BY x")?
        .run_collect_rows()?;
    Ok(rows
        .into_iter()
        .map(|row| match &row[0] {
            Value::Text(text) => text.as_str().to_string(),
            other => panic!("unexpected value {other:?}"),
        })
        .collect())
}
