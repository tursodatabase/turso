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
fn encrypted_bootstrap_failures_around_sync_and_metadata_publication() {
    for stage in 1..=4 {
        let remote = Remote::new_encrypted(&[
            "CREATE TABLE items(value TEXT)",
            "INSERT INTO items VALUES ('encrypted bootstrap failure marker')",
        ])
        .unwrap();
        let directory = Arc::new(tempfile::tempdir().unwrap());
        let server = InProcessServer::new(remote.clone());
        if stage >= 3 {
            server
                .metadata_write_failure
                .store(stage - 2, Ordering::SeqCst);
        }
        let io = Arc::new(BootstrapIo {
            inner: Arc::new(PlatformIO::new().unwrap()),
            failure: Arc::new(AtomicU8::new(if stage <= 2 { stage } else { 0 })),
        });
        let error = Replica::open_encrypted_with_io(&directory, server, Some([22; 32]), io)
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("injected"),
            "stage {stage}: {error}"
        );
        let main = std::fs::read(directory.path().join("replica.db")).unwrap();
        assert!(!main.is_empty());
        let crypto = turso_core::EncryptionContext::new(
            turso_core::CipherMode::Aes256Gcm,
            &turso_core::EncryptionKey::new_256([22; 32]),
            4096,
        )
        .unwrap();
        for (index, page) in main.chunks_exact(4096).enumerate() {
            crypto.decrypt_page(page, index + 1).unwrap();
        }
        let reopened = Replica::open_encrypted(
            &directory,
            InProcessServer::new(remote.clone()),
            Some([22; 32]),
        );
        if stage == 4 {
            assert_eq!(
                reopened.unwrap().snapshot().unwrap(),
                remote.snapshot().unwrap()
            );
        } else {
            assert!(!directory.path().join("replica.db-info").exists());
            assert!(reopened.is_err());
            assert_eq!(
                std::fs::read(directory.path().join("replica.db")).unwrap(),
                main
            );
        }
    }
}

#[test]
fn encrypted_pull_metadata_failure_keeps_published_progress() {
    let remote = Remote::new_encrypted(&["CREATE TABLE items(value TEXT)"]).unwrap();
    let directory = Arc::new(tempfile::tempdir().unwrap());
    let server = InProcessServer::new(remote.clone());
    let failure = server.metadata_write_failure.clone();
    let replica = Replica::open_encrypted(&directory, server, Some([22; 32])).unwrap();
    let before = std::fs::read(directory.path().join("replica.db-info")).unwrap();
    remote
        .execute_transaction(&["INSERT INTO items VALUES ('remote')"])
        .unwrap();
    failure.store(1, Ordering::SeqCst);
    assert!(replica
        .pull()
        .unwrap_err()
        .to_string()
        .contains("metadata publication"));
    assert_eq!(
        std::fs::read(directory.path().join("replica.db-info")).unwrap(),
        before
    );
    drop(replica);
    let replica = Replica::open_encrypted(
        &directory,
        InProcessServer::new(remote.clone()),
        Some([22; 32]),
    )
    .unwrap();
    replica.pull().unwrap();
    assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
}

#[test]
fn encrypted_startup_restores_pending_replacement_and_can_write_again() {
    let remote = Remote::new_encrypted(&[
        "CREATE TABLE items(value TEXT)",
        "INSERT INTO items VALUES ('original')",
    ])
    .unwrap();
    let directory = Arc::new(tempfile::tempdir().unwrap());
    let replica = Replica::open_encrypted(
        &directory,
        InProcessServer::new(remote.clone()),
        Some([22; 32]),
    )
    .unwrap();
    let expected = replica.snapshot().unwrap();
    drop(replica);
    let main = directory
        .path()
        .join("replica.db")
        .to_string_lossy()
        .into_owned();
    let mut files = Vec::new();
    for (suffix, name) in [
        ("", "main-db"),
        ("-wal", "main-wal"),
        ("-log", "main-log"),
        ("-wal-revert", "revert-wal"),
        ("-info", "metadata"),
    ] {
        let path = format!("{main}{suffix}");
        let backup = format!("{main}-replace-base-apply-{name}.backup");
        let present = std::fs::metadata(&path).is_ok_and(|meta| meta.len() != 0);
        if present {
            std::fs::copy(&path, &backup).unwrap();
        }
        files.push(serde_json::json!({"path":path,"backup_path":backup,"present":present}));
    }
    // Reproduce the on-disk state after the guard publishes its manifest and
    // an interrupted replacement has damaged the main database and metadata.
    std::fs::write(
        format!("{main}-replace-base-apply"),
        serde_json::to_vec(&serde_json::json!({
            "version":1,"operation":"replace_base_apply","main_db_path":main,
            "previous_synced_revision":null,"files":files,
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(&main, [0x91; 4096]).unwrap();
    std::fs::write(format!("{main}-info"), [0x91; 20]).unwrap();
    let replica = Replica::open_encrypted(
        &directory,
        InProcessServer::new(remote.clone()),
        Some([22; 32]),
    )
    .unwrap();
    assert_eq!(replica.snapshot().unwrap(), expected);
    assert!(!std::path::Path::new(&format!("{main}-replace-base-apply")).exists());
    replica
        .execute("INSERT INTO items VALUES ('after recovery')")
        .unwrap();
    let expected = replica.snapshot().unwrap();
    drop(replica);
    let replica =
        Replica::open_encrypted(&directory, InProcessServer::new(remote), Some([22; 32])).unwrap();
    assert_eq!(replica.snapshot().unwrap(), expected);
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

#[test]
fn encrypted_replacement_replay_failure_restores_database_and_progress() {
    let remote = Remote::new_encrypted(&[
        "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
        "INSERT INTO items VALUES (1, 'duplicate')",
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
        .execute("CREATE UNIQUE INDEX local_unique ON items(value)")
        .unwrap();
    let before = replica.snapshot().unwrap();
    drop(replica);
    remote
        .execute_transaction(&["INSERT INTO items VALUES (2, 'duplicate')"])
        .unwrap();
    let replica = Replica::open_encrypted(
        &directory,
        InProcessServer::with_replacement(remote.clone()),
        Some([22; 32]),
    )
    .unwrap();
    let progress = std::fs::read(directory.path().join("replica.db-info")).unwrap();
    assert!(
        replica.pull().is_err(),
        "replaying the local unique index must fail"
    );
    assert_eq!(replica.snapshot().unwrap(), before);
    assert_eq!(
        std::fs::read(directory.path().join("replica.db-info")).unwrap(),
        progress
    );
    replica
        .execute("INSERT INTO items VALUES (3, 'after recovery')")
        .unwrap();
    replica.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let recovered = replica.snapshot().unwrap();
    drop(replica);
    let reopened =
        Replica::open_encrypted(&directory, InProcessServer::new(remote), Some([22; 32])).unwrap();
    assert_eq!(reopened.snapshot().unwrap(), recovered);
}

#[test]
fn encrypted_metadata_uses_host_existence_when_storage_differs_from_core() {
    let remote = Remote::new_encrypted(&["CREATE TABLE items(value TEXT)"]).unwrap();
    let directory = Arc::new(tempfile::tempdir().unwrap());
    let metadata_path = directory.path().join("replica.db-info");
    // OPFS registration creates an empty file, while BrowserIO keeps metadata
    // separately in localStorage. The empty OPFS file is not the metadata.
    std::fs::write(&metadata_path, []).unwrap();
    let files = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let server = || {
        let mut server = InProcessServer::new(remote.clone());
        server.host_files = Some(files.clone());
        server
    };
    let replica = Replica::open_encrypted(&directory, server(), Some([22; 32])).unwrap();
    assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
    drop(replica);
    let path = metadata_path.to_string_lossy().into_owned();
    assert!(files.lock().unwrap()[&path].starts_with(b"TSYNMETA"));
    assert_eq!(std::fs::metadata(&metadata_path).unwrap().len(), 0);
    let replica = Replica::open_encrypted(&directory, server(), Some([22; 32])).unwrap();
    assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
    drop(replica);
    files.lock().unwrap().insert(path, Vec::new());
    assert!(Replica::open_encrypted(&directory, server(), Some([22; 32])).is_err());
}
