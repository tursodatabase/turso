//! The app runs `PRAGMA wal_checkpoint` on its own connection to a synced WAL database.

use std::sync::Arc;

use turso_core::Value;
use turso_sync_engine::Result;

use crate::harness::{database_opts, InProcessServer, Remote, Replica};

/// A checkpoint by the app would copy its unpushed local frames into the DB file, and every later
/// pull would fail to roll them back. So the app's checkpoint fails, and the pull applies the
/// remote changes and keeps the local row.
#[test]
fn app_checkpoint_fails_and_pulls_keep_working() -> Result<()> {
    let remote = Remote::new_wal(&["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('first')"])?;
    let dir = Arc::new(tempfile::tempdir()?);
    let replica = Replica::open(
        &dir,
        InProcessServer::pages_only(remote.clone()),
        database_opts(),
    )?;
    let conn = replica.connect()?;
    conn.execute("INSERT INTO t VALUES ('pre')")?;
    for mode in ["PASSIVE", "FULL", "RESTART", "TRUNCATE"] {
        let error = conn
            .prepare(format!("PRAGMA wal_checkpoint({mode})"))?
            .run_collect_rows()
            .expect_err("the app's checkpoint must fail");
        assert!(
            error.to_string().contains("only the sync engine runs"),
            "{mode}: {error}"
        );
    }

    remote.execute_transaction(&["INSERT INTO t VALUES ('second')"])?;
    replica.pull()?;
    // The sync API's checkpoint, which the error points to, still works.
    replica.checkpoint()?;

    let rows = replica
        .connect()?
        .prepare("SELECT x FROM t ORDER BY x")?
        .run_collect_rows()?;
    let rows: Vec<String> = rows
        .into_iter()
        .map(|row| match &row[0] {
            Value::Text(text) => text.as_str().to_string(),
            other => panic!("unexpected value {other:?}"),
        })
        .collect();
    assert_eq!(rows, ["first", "pre", "second"]);
    assert_eq!(replica.pending_local_changes()?, 1);
    Ok(())
}
