use super::harness::{InProcessServer, Remote, Replica};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use turso_core::io::clock::{MonotonicInstant, WallClockInstant};
use turso_core::io::{Clock, FileSyncType};
use turso_core::{Buffer, Completion, File, OpenFlags, PlatformIO, IO};

const PRIVATE_MARKER: &[u8] = b"encrypted bootstrap failure marker";

// Observe writes before they reach storage, and stop bootstrap on either side
// of its main-file sync. Host metadata publication is injected separately.
struct BootstrapIo {
    inner: Arc<PlatformIO>,
    failure: Arc<AtomicU8>,
}

impl Clock for BootstrapIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for BootstrapIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let file = self.inner.open_file(path, flags, direct)?;
        Ok(Arc::new(BootstrapFile {
            inner: file,
            io: self.inner.clone(),
            failure: self.failure.clone(),
            main: path.ends_with("replica.db"),
        }))
    }
    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }
    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }
}

struct BootstrapFile {
    inner: Arc<dyn File>,
    io: Arc<PlatformIO>,
    failure: Arc<AtomicU8>,
    main: bool,
}

impl File for BootstrapFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        assert!(!buffer
            .as_slice()
            .windows(PRIVATE_MARKER.len())
            .any(|b| b == PRIVATE_MARKER));
        self.inner.pwrite(pos, buffer, c)
    }
    fn sync(&self, c: Completion, kind: FileSyncType) -> turso_core::Result<Completion> {
        let failure = if self.main {
            self.failure.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if failure == 1 {
            return Err(turso_core::LimboError::InternalError(
                "injected before bootstrap sync".into(),
            ));
        }
        let result = self.inner.sync(c, kind)?;
        if failure == 2 {
            self.io.wait_for_completion(result)?;
            return Err(turso_core::LimboError::InternalError(
                "injected after bootstrap sync".into(),
            ));
        }
        Ok(result)
    }
    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.truncate(len, c)
    }
}

#[test]
fn encrypted_creation_rejects_orphaned_recovery_artifacts_without_writes() {
    let remote = Remote::new_encrypted(&[]).unwrap();
    let mut artifacts = vec![("-replace-base-apply".to_string(), Vec::new())];
    for name in ["main-db", "main-wal", "main-log", "revert-wal", "metadata"] {
        for contents in [Vec::new(), b"old recovery bytes".to_vec()] {
            artifacts.push((format!("-replace-base-apply-{name}.backup"), contents));
        }
    }
    // Hosts can store recovery sidecars separately from core's filesystem.
    for host_storage in [false, true] {
        for (suffix, contents) in &artifacts {
            let directory = Arc::new(tempfile::tempdir().unwrap());
            let path = directory.path().join(format!("replica.db{suffix}"));
            let path = path.to_string_lossy().into_owned();
            let files = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
            let mut server = InProcessServer::new(remote.clone());
            if host_storage {
                files.lock().unwrap().insert(path.clone(), contents.clone());
                server.host_files = Some(files.clone());
            } else {
                std::fs::write(&path, contents).unwrap();
            }
            let error = Replica::open_encrypted(&directory, server, Some([22; 32]))
                .err()
                .unwrap_or_else(|| panic!("creation accepted {suffix}, host={host_storage}"));
            assert!(
                error.to_string().contains("existing recovery artifact"),
                "{error}"
            );
            assert!(error.to_string().contains(&path), "{error}");
            let entries = std::fs::read_dir(directory.path()).unwrap().count();
            if host_storage {
                assert_eq!(entries, 0, "creation wrote core files");
                let files = files.lock().unwrap();
                assert_eq!(files.len(), 1, "creation wrote host files");
                assert_eq!(&files[&path], contents);
            } else {
                assert_eq!(entries, 1, "creation wrote files");
                assert_eq!(&std::fs::read(&path).unwrap(), contents);
            }
        }
    }
}

#[test]
fn encrypted_reopen_preserves_replica_and_orphaned_backups() {
    let remote = Remote::new_encrypted(&["CREATE TABLE items(value TEXT)"]).unwrap();
    let directory = Arc::new(tempfile::tempdir().unwrap());
    let replica = Replica::open_encrypted(
        &directory,
        InProcessServer::new(remote.clone()),
        Some([22; 32]),
    )
    .unwrap();
    replica
        .execute("INSERT INTO items VALUES ('unpushed local row')")
        .unwrap();
    let expected = replica.snapshot().unwrap();
    drop(replica);
    let backups: Vec<_> = ["main-db", "main-wal", "main-log", "revert-wal", "metadata"]
        .into_iter()
        .map(|name| {
            directory
                .path()
                .join(format!("replica.db-replace-base-apply-{name}.backup"))
        })
        .collect();
    for path in &backups {
        std::fs::write(path, b"non-authoritative backup").unwrap();
    }
    let replica =
        Replica::open_encrypted(&directory, InProcessServer::new(remote), Some([22; 32])).unwrap();
    assert_eq!(replica.snapshot().unwrap(), expected);
    for path in backups {
        assert_eq!(std::fs::read(path).unwrap(), b"non-authoritative backup");
    }
}

#[test]
fn encrypted_empty_bootstrap_pull_reopen_and_auth_failure() {
    let remote = Remote::new_encrypted(&[]).unwrap();
    let directory = Arc::new(tempfile::tempdir().unwrap());
    let replica = Replica::open_encrypted(
        &directory,
        InProcessServer::new(remote.clone()),
        Some([22; 32]),
    )
    .unwrap();
    remote
        .execute_transaction(&[
            "CREATE TABLE secrets(id INTEGER PRIMARY KEY, value TEXT)",
            "INSERT INTO secrets VALUES (1, 'private sync marker')",
            "INSERT INTO secrets VALUES (2, hex(zeroblob(40000)))",
        ])
        .unwrap();
    replica.pull().unwrap();
    assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
    replica.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    for entry in std::fs::read_dir(directory.path()).unwrap() {
        let bytes = std::fs::read(entry.unwrap().path()).unwrap();
        assert!(!bytes
            .windows(b"private sync marker".len())
            .any(|window| window == b"private sync marker"));
    }
    drop(replica);
    let replica = Replica::open_encrypted(
        &directory,
        InProcessServer::new(remote.clone()),
        Some([22; 32]),
    )
    .unwrap();
    assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
    assert!(Replica::open_encrypted(
        &directory,
        InProcessServer::new(remote.clone()),
        Some([33; 32])
    )
    .is_err());
    assert!(
        Replica::open_encrypted(&directory, InProcessServer::new(remote.clone()), None).is_err()
    );
    remote
        .execute_transaction(&["INSERT INTO secrets VALUES (3, 'next private marker')"])
        .unwrap();
    let before = std::fs::read(directory.path().join("replica.db-info")).unwrap();
    assert!(replica.pull_with_tampered_staging().is_err());
    assert_eq!(
        std::fs::read(directory.path().join("replica.db-info")).unwrap(),
        before
    );
    replica.pull().unwrap();
    assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
    replica.pull().unwrap(); // authenticated empty download
}

#[test]
fn encrypted_source_supports_a_plaintext_replica() {
    let remote = Remote::new_encrypted(&["CREATE TABLE items(value TEXT)"]).unwrap();
    let directory = Arc::new(tempfile::tempdir().unwrap());
    let replica =
        Replica::open_encrypted(&directory, InProcessServer::new(remote.clone()), None).unwrap();
    remote
        .execute_transaction(&["INSERT INTO items VALUES ('plaintext local replica')"])
        .unwrap();
    replica.pull().unwrap();
    assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
    assert!(std::fs::read(directory.path().join("replica.db-info"))
        .unwrap()
        .starts_with(b"{"));
}

#[test]
fn encrypted_creation_refuses_existing_files_and_incompatible_pages() {
    let remote = Remote::new_encrypted(&[]).unwrap();
    let directory = Arc::new(tempfile::tempdir().unwrap());
    let original = b"existing database must not be overwritten";
    std::fs::write(directory.path().join("replica.db"), original).unwrap();
    assert!(
        Replica::open_encrypted(&directory, InProcessServer::new(remote), Some([22; 32])).is_err()
    );
    assert_eq!(
        std::fs::read(directory.path().join("replica.db")).unwrap(),
        original
    );
    let plaintext_remote = Remote::new(&[]).unwrap();
    let fresh = Arc::new(tempfile::tempdir().unwrap());
    let error = Replica::open_encrypted(
        &fresh,
        InProcessServer::new(plaintext_remote),
        Some([22; 32]),
    )
    .err()
    .unwrap();
    assert!(
        error.to_string().contains("page header is incompatible"),
        "{error}"
    );
    assert!(!fresh.path().join("replica.db-info").exists());
    // The bootstrap may reserve its extent before validating individual pages,
    // but incompatible plaintext pages must never be installed.
    assert!(std::fs::read(fresh.path().join("replica.db"))
        .unwrap()
        .iter()
        .all(|&byte| byte == 0));
}

#[test]
fn encrypted_replacement_preserves_local_changes_and_reload_keys() {
    let remote = Remote::new_encrypted(&[
        "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
        "INSERT INTO items VALUES (1, 'base')",
    ])
    .unwrap();
    let directory = Arc::new(tempfile::tempdir().unwrap());
    let replica = Replica::open_encrypted(
        &directory,
        InProcessServer::new(remote.clone()),
        Some([22; 32]),
    )
    .unwrap();
    replica
        .execute("INSERT INTO items VALUES (2, 'local')")
        .unwrap();
    drop(replica);
    remote
        .execute_transaction(&["INSERT INTO items VALUES (3, 'remote')"])
        .unwrap();
    let replica = Replica::open_encrypted(
        &directory,
        InProcessServer::with_replacement(remote.clone()),
        Some([22; 32]),
    )
    .unwrap();
    replica.pull().unwrap();
    remote
        .execute_transaction(&["INSERT INTO items VALUES (2, 'local')"])
        .unwrap();
    assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
    replica
        .execute("INSERT INTO items VALUES (4, 'after reload')")
        .unwrap();
    replica.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    drop(replica);
    let reopened = Replica::open_encrypted(
        &directory,
        InProcessServer::new(remote.clone()),
        Some([22; 32]),
    )
    .unwrap();
    remote
        .execute_transaction(&["INSERT INTO items VALUES (4, 'after reload')"])
        .unwrap();
    assert_eq!(reopened.snapshot().unwrap(), remote.snapshot().unwrap());
}

#[test]
fn encrypted_zero_page_generation_base_starts_in_mvcc_mode() {
    let remote = Remote::new_encrypted(&[
        "CREATE TABLE items(value TEXT)",
        "INSERT INTO items VALUES ('log-only value')",
    ])
    .unwrap();
    let directory = Arc::new(tempfile::tempdir().unwrap());
    let replica = Replica::open_encrypted(
        &directory,
        InProcessServer::with_empty_base(remote.clone()),
        Some([22; 32]),
    )
    .unwrap();
    assert!(replica.is_mvcc());
    assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
}
