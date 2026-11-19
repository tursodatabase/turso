//! An app on a plain connection (the JS binding, which never takes the SDK's SyncBusyGate) with
//! a transaction open while a logical MVCC pull runs. The pull needs the gate, so it does not
//! wait for the app: the app could commit during the wait, and the pull would then lose that
//! commit.

use std::sync::Arc;

use turso_core::{Connection, Value};
use turso_sync_engine::Result;

use crate::harness::Remote;

/// The app commits the first time the pull returns control to its caller, which is where a
/// pull that waits for a lock would wait. The pull must not wait: it fails with Busy without
/// returning control, so the app's commit comes after the pull. The app's row must stay in the
/// table and stay waiting to be pushed, and the next pull must apply the remote row.
#[test]
fn app_commit_while_a_logical_pull_runs_is_not_lost() -> Result<()> {
    let remote = Remote::new(&["CREATE TABLE t(x TEXT)"])?;
    let replica = remote.bootstrap_replica()?;
    let app = replica.connect()?;
    app.execute("BEGIN")?;
    app.execute("INSERT INTO t VALUES ('app')")?;
    remote.execute_transaction(&["INSERT INTO t VALUES ('remote')"])?;

    let mut committed_during_pull = false;
    let (yields, pull_result) = replica.pull_calling_on_yield(|| {
        if !committed_during_pull {
            app.execute("COMMIT").unwrap();
            committed_during_pull = true;
        }
    });
    assert!(
        !committed_during_pull && yields == 0,
        "the pull waited for the app's transaction: {yields} yields, result {pull_result:?}"
    );
    let error = pull_result.expect_err("the pull must fail while the app holds its transaction");
    assert!(
        error.to_string().to_lowercase().contains("busy"),
        "unexpected error: {error}"
    );

    app.execute("COMMIT")?;
    assert_eq!(rows(&app)?, ["app"]);
    assert_eq!(
        replica.pending_local_changes()?,
        1,
        "the app's row is not waiting to be pushed"
    );

    replica.pull()?;
    assert_eq!(rows(&app)?, ["app", "remote"]);
    assert_eq!(replica.pending_local_changes()?, 1);
    Ok(())
}

fn rows(conn: &Arc<Connection>) -> Result<Vec<String>> {
    let rows = conn
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
