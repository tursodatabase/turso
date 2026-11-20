use turso_sync_engine::Result;

use crate::harness::Remote;

/// A remote that writes plain frames gives a pull no row changes to replay,
/// so the pull must fail instead of reporting success with rows missing.
#[test]
fn pull_of_plain_frames_fails() -> Result<()> {
    let remote = Remote::without_portable_changes(&["CREATE TABLE t (x)"])?;
    let replica = remote.bootstrap_replica()?;
    remote.execute_transaction(&["INSERT INTO t VALUES ('b')"])?;

    let err = replica.pull().expect_err("pull of plain frames must fail");
    assert!(
        err.to_string().contains("Corrupt database")
            && err.to_string().contains("has no portable changes")
            && err.to_string().contains("PRAGMA journal_mode = 'mvcc'"),
        "unexpected error: {err}"
    );
    Ok(())
}
