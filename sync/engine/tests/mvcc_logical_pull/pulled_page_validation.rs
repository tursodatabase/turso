use super::harness::{database_opts, InProcessServer, Remote, Replica};
use std::sync::atomic::Ordering;
use std::sync::Arc;

const LOCAL_KEY: [u8; 32] = [22; 32];

fn remote(encrypted: bool) -> Arc<Remote> {
    let setup = [
        "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
        "INSERT INTO items VALUES (1, 'base')",
    ];
    if encrypted {
        Remote::new_encrypted(&setup).unwrap()
    } else {
        Remote::new(&setup).unwrap()
    }
}

/// Checks that a tampered pull fails with `expected` in its error and leaves
/// the replica as it was.
fn assert_tampered_pull_fails(
    replica: &Replica,
    tamper: &std::sync::atomic::AtomicU8,
    code: u8,
    expected: &str,
) {
    let before = replica.snapshot().unwrap();
    tamper.store(code, Ordering::SeqCst);
    let error = replica.pull().expect_err("tampered pull must fail");
    assert!(error.to_string().contains(expected), "{code}: {error}");
    assert_eq!(replica.snapshot().unwrap(), before);
}

#[test]
fn incremental_page_pull_rejects_malformed_pages() {
    for (encrypted, code, expected) in [
        (true, 2, "non-empty encryption reserve"),
        (true, 3, "protected page layout"),
        (false, 4, "pulled page id is out of range"),
        (true, 4, "pulled page id is out of range"),
        (false, 5, "pulled page id is out of range"),
    ] {
        let remote = remote(encrypted);
        let directory = Arc::new(tempfile::tempdir().unwrap());
        let mut server = InProcessServer::new(remote.clone());
        server.force_page_updates = true;
        let tamper = server.bootstrap_failure.clone();
        let replica = if encrypted {
            Replica::open_encrypted(&directory, server, Some(LOCAL_KEY))
        } else {
            Replica::open(&directory, server, database_opts())
        }
        .unwrap();
        remote
            .execute_transaction(&["INSERT INTO items VALUES (2, 'remote')"])
            .unwrap();
        assert_tampered_pull_fails(&replica, &tamper, code, expected);
        replica.pull().unwrap();
        assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
    }
}

#[test]
fn replace_base_page_pull_rejects_malformed_pages() {
    for (code, expected) in [
        (3, "protected page layout"),
        (4, "pulled page id is out of range"),
    ] {
        let remote = remote(true);
        let directory = Arc::new(tempfile::tempdir().unwrap());
        drop(
            Replica::open_encrypted(
                &directory,
                InProcessServer::new(remote.clone()),
                Some(LOCAL_KEY),
            )
            .unwrap(),
        );
        remote
            .execute_transaction(&["INSERT INTO items VALUES (2, 'remote')"])
            .unwrap();
        let server = InProcessServer::with_replacement(remote.clone());
        let tamper = server.bootstrap_failure.clone();
        let replica = Replica::open_encrypted(&directory, server, Some(LOCAL_KEY)).unwrap();
        assert_tampered_pull_fails(&replica, &tamper, code, expected);
        drop(replica);
        // The test server checkpoints the remote when it serves pages, so the
        // retry needs another replace-base pull to see the new row.
        let server = InProcessServer::with_replacement(remote.clone());
        let replica = Replica::open_encrypted(&directory, server, Some(LOCAL_KEY)).unwrap();
        replica.pull().unwrap();
        assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
    }
}
