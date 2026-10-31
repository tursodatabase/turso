use std::collections::BTreeSet;

use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use turso_core::Value;
use turso_sync_engine::Result;

use crate::harness::{Remote, Replica};

fn text_ids(rows: Vec<Vec<Value>>) -> Vec<String> {
    rows.into_iter()
        .map(|row| {
            row[0]
                .to_text()
                .expect("expected TEXT id column")
                .to_string()
        })
        .collect()
}

#[test]
fn restored_backup_rejects_push_instead_of_silently_dropping_writes() -> Result<()> {
    let remote = Remote::new(&["CREATE TABLE t (id TEXT PRIMARY KEY)"])?;
    let mut replica = remote.bootstrap_replica()?;

    replica.execute("INSERT INTO t VALUES ('one')")?;
    replica.push()?;

    let backup = replica.backup_all_files()?;

    replica.execute("INSERT INTO t VALUES ('two')")?;
    replica.push()?;

    replica.restore_all_files(backup.path())?;
    replica.execute("INSERT INTO t VALUES ('three')")?;

    let err = replica
        .push()
        .expect_err("push after backup restore must fail instead of dropping writes");
    assert!(
        err.to_string().contains("protocol error"),
        "unexpected error: {err}"
    );
    assert_eq!(
        text_ids(replica.query_rows("SELECT id FROM t ORDER BY id")?),
        vec!["one", "three"],
        "local write after restore must remain intact locally when push is rejected"
    );
    Ok(())
}

#[test]
fn restored_backup_with_many_offline_writes_still_rejects_push() -> Result<()> {
    let remote = Remote::new(&["CREATE TABLE t (id TEXT PRIMARY KEY)"])?;
    let mut replica = remote.bootstrap_replica()?;

    replica.execute("INSERT INTO t VALUES ('w01')")?;
    replica.push()?;

    let backup = replica.backup_all_files()?;

    replica.execute("INSERT INTO t VALUES ('w02')")?;
    replica.push()?;

    // Restore and write enough rows offline that MAX(change_id) locally exceeds
    // the server watermark; push must still detect the rewind via last_push_attempt.
    replica.restore_all_files(backup.path())?;
    for idx in 3..=12 {
        replica.execute(&format!("INSERT INTO t VALUES ('w{idx:02}')"))?;
    }

    let err = replica
        .push()
        .expect_err("offline writes above remote watermark must not mask a backup rewind");
    assert!(
        err.to_string().contains("protocol error"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[test]
fn restored_backup_rejects_pull_without_clobbering_post_restore_writes() -> Result<()> {
    let remote = Remote::new(&["CREATE TABLE t (id TEXT PRIMARY KEY)"])?;
    let mut replica = remote.bootstrap_replica()?;

    replica.execute("INSERT INTO t VALUES ('one')")?;
    replica.push()?;

    let backup = replica.backup_all_files()?;

    replica.execute("INSERT INTO t VALUES ('two')")?;
    replica.push()?;

    replica.restore_all_files(backup.path())?;
    replica.execute("INSERT INTO t VALUES ('three')")?;

    let err = replica
        .pull()
        .expect_err("pull after backup restore must fail instead of skipping self-origin txns");
    assert!(
        err.to_string().contains("protocol error"),
        "unexpected error: {err}"
    );
    assert_eq!(
        text_ids(replica.query_rows("SELECT id FROM t ORDER BY id")?),
        vec!["one", "three"]
    );
    Ok(())
}

#[test]
fn restored_db_without_info_sidecar_rejects_push_and_pull() -> Result<()> {
    let remote = Remote::new(&["CREATE TABLE t (id TEXT PRIMARY KEY)"])?;
    let mut replica = remote.bootstrap_replica()?;

    replica.execute("INSERT INTO t VALUES ('one')")?;
    replica.push()?;

    let backup = replica.backup_all_files()?;

    replica.execute("INSERT INTO t VALUES ('two')")?;
    replica.execute("INSERT INTO t VALUES ('three')")?;
    replica.push()?;

    // Restore only the database/log files while keeping the newer -info sidecar.
    replica.restore_db_files_only(backup.path())?;
    replica.execute("INSERT INTO t VALUES ('four')")?;

    let push_err = replica
        .push()
        .expect_err("db-only restore must be caught via local CDC sequence watermark");
    assert!(push_err.to_string().contains("protocol error"));

    let pull_err = replica
        .pull()
        .expect_err("db-only restore pull must also fail via local CDC sequence watermark");
    assert!(pull_err.to_string().contains("protocol error"));
    Ok(())
}

#[test]
fn cloned_replica_files_are_rejected_once_original_pushes_ahead() -> Result<()> {
    let remote = Remote::new(&["CREATE TABLE t (id TEXT PRIMARY KEY)"])?;
    let primary = remote.bootstrap_replica()?;

    primary.execute("INSERT INTO t VALUES ('one')")?;
    primary.push()?;

    let snapshot_dir = primary.backup_all_files()?;
    let cloned = Replica::clone_from_backup(remote.clone(), snapshot_dir.path())?;

    primary.execute("INSERT INTO t VALUES ('two')")?;
    primary.push()?;

    cloned.execute("INSERT INTO t VALUES ('from-clone')")?;
    let err = cloned
        .push()
        .expect_err("cloned replica sharing client_unique_id must be rejected once behind remote");
    assert!(err.to_string().contains("protocol error"));
    Ok(())
}

#[test]
fn crash_after_remote_commit_before_post_push_meta_recovers_cleanly() -> Result<()> {
    let remote = Remote::new(&["CREATE TABLE t (id TEXT PRIMARY KEY)"])?;
    let mut replica = remote.bootstrap_replica()?;

    replica.execute("INSERT INTO t VALUES ('one')")?;
    replica.push()?;

    replica.execute("INSERT INTO t VALUES ('two')")?;
    replica.fail_next_push_after_remote_commit();
    let _ = replica
        .push()
        .expect_err("simulated transport drop after remote commit must surface error");

    // Reopen from disk as after a process crash before post-push update_meta.
    replica.reopen()?;
    replica.execute("INSERT INTO t VALUES ('three')")?;
    replica.push()?;
    replica.pull()?;

    let expected = vec!["one".to_string(), "three".to_string(), "two".to_string()];
    assert_eq!(
        text_ids(remote.query_rows("SELECT id FROM t ORDER BY id")?),
        expected
    );
    assert_eq!(replica.snapshot()?, remote.snapshot()?);
    Ok(())
}

#[test]
fn deterministic_sync_backup_restore_and_clone_simulation() -> Result<()> {
    for seed in 0..24u64 {
        let mut rng = ChaCha8Rng::seed_from_u64(0x8129_0000 + seed);
        let remote = Remote::new(&["CREATE TABLE sim_items (id TEXT PRIMARY KEY)"])?;
        let mut replica = remote.bootstrap_replica()?;

        let mut next_row_seq = 1usize;
        let mut pushed_to_remote = BTreeSet::<String>::new();
        let mut pending_local = BTreeSet::<String>::new();
        let mut saved_backup: Option<(tempfile::TempDir, usize)> = None;
        let mut remote_push_epoch = 0usize;
        let mut rewound = false;

        for _step in 0..20 {
            match rng.random_range(0..7) {
                0 | 1 => {
                    let key = format!("s{seed}_r{next_row_seq:03}");
                    next_row_seq += 1;
                    replica.execute(&format!("INSERT INTO sim_items VALUES ('{key}')"))?;
                    pending_local.insert(key);
                }
                2 => {
                    if rewound {
                        assert!(
                            replica.push().is_err(),
                            "seed {seed}: rewound replica push must never succeed silently"
                        );
                    } else {
                        replica.push()?;
                        if !pending_local.is_empty() {
                            remote_push_epoch += 1;
                            pushed_to_remote.extend(std::mem::take(&mut pending_local));
                        }
                    }
                }
                3 => {
                    if !rewound && !pending_local.is_empty() {
                        replica.fail_next_push_after_remote_commit();
                        assert!(replica.push().is_err());
                        remote_push_epoch += 1;
                        pushed_to_remote.extend(std::mem::take(&mut pending_local));
                        replica.reopen()?;
                        replica.push()?;
                    }
                }
                4 => {
                    if !rewound {
                        remote.execute_transaction(&[&format!(
                            "INSERT INTO sim_items VALUES ('remote_{seed}_{next_row_seq:03}')"
                        )])?;
                        let key = format!("remote_{seed}_{next_row_seq:03}");
                        next_row_seq += 1;
                        pushed_to_remote.insert(key);
                        replica.pull()?;
                    } else {
                        let _ = replica.pull();
                    }
                }
                5 => {
                    if !rewound && pending_local.is_empty() {
                        saved_backup = Some((replica.backup_all_files()?, remote_push_epoch));
                    }
                }
                _ => {
                    if let Some((backup_dir, _)) = saved_backup
                        .as_ref()
                        .filter(|(_, backup_epoch)| remote_push_epoch > *backup_epoch)
                    {
                        replica.restore_all_files(backup_dir.path())?;
                        pending_local.clear();
                        rewound = true;
                    }
                }
            }
        }

        if !rewound {
            replica.push()?;
            pushed_to_remote.extend(std::mem::take(&mut pending_local));
            replica.pull()?;
            assert_eq!(
                replica.snapshot()?,
                remote.snapshot()?,
                "seed {seed}: healthy replica and remote must converge"
            );
        } else {
            let key = format!("post_rewind_{seed}");
            replica.execute(&format!("INSERT INTO sim_items VALUES ('{key}')"))?;
            assert!(
                replica.push().is_err(),
                "seed {seed}: post-restore push must fail instead of dropping {key}"
            );
        }

        let actual_remote: BTreeSet<String> =
            text_ids(remote.query_rows("SELECT id FROM sim_items ORDER BY id")?)
                .into_iter()
                .collect();
        assert_eq!(
            actual_remote, pushed_to_remote,
            "seed {seed}: every acknowledged push must remain on the remote server"
        );
    }
    Ok(())
}
