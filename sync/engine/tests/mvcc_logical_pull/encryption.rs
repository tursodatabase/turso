use super::harness::{InProcessServer, Remote, Replica};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use turso_core::io::clock::{MonotonicInstant, WallClockInstant};
use turso_core::io::{Clock, FileSyncType};
use turso_core::{Buffer, Completion, File, OpenFlags, PlatformIO, IO};

const PRIVATE_MARKER: &[u8] = b"encrypted bootstrap failure marker";

#[test]
fn encrypted_bootstrap_retries_after_interrupted_download_and_page_writes() {
    for failure in [1, 4, 5] {
        let remote = Remote::new_encrypted(&[
            "CREATE TABLE items(value TEXT)",
            "INSERT INTO items VALUES (hex(zeroblob(12000)))",
        ])
        .unwrap();
        let directory = Arc::new(tempfile::tempdir().unwrap());
        let server = InProcessServer::new(remote.clone());
        server
            .bootstrap_failure
            .store(u8::from(failure == 1), Ordering::SeqCst);
        let io = Arc::new(BootstrapIo {
            inner: Arc::new(PlatformIO::new().unwrap()),
            failure: Arc::new(AtomicU8::new(if failure == 1 { 0 } else { failure })),
        });
        let error = Replica::open_encrypted_with_io(&directory, server, Some([22; 32]), io)
            .err()
            .expect("injected bootstrap failure");
        assert!(
            error.to_string().contains(if failure == 1 {
                "interrupted bootstrap"
            } else {
                "write"
            }),
            "{error}"
        );
        assert!(!directory.path().join("replica.db-info").exists());
        let bytes = std::fs::read(directory.path().join("replica.db")).unwrap();
        let crypto = turso_core::EncryptionContext::new(
            turso_core::CipherMode::Aes256Gcm,
            &turso_core::EncryptionKey::new_256([22; 32]),
            4096,
        )
        .unwrap();
        let page = crypto.decrypt_page(&bytes[..4096], 1).unwrap();
        assert!(page.starts_with(b"SQLite format 3\0"));
        let retry = Replica::open_encrypted(
            &directory,
            InProcessServer::new(remote.clone()),
            Some([22; 32]),
        )
        .unwrap();
        assert_eq!(retry.snapshot().unwrap(), remote.snapshot().unwrap());
    }
}

#[test]
fn encrypted_bootstrap_rejects_nonzero_reserved_bytes_before_installing_page() {
    let remote = Remote::new_encrypted(&["CREATE TABLE items(value TEXT)"]).unwrap();
    let directory = Arc::new(tempfile::tempdir().unwrap());
    let server = InProcessServer::new(remote.clone());
    server.bootstrap_failure.store(2, Ordering::SeqCst);
    let error = Replica::open_encrypted(&directory, server, Some([22; 32]))
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("encryption reserve is not empty"),
        "{error}"
    );
    assert!(!directory.path().join("replica.db-info").exists());
    assert!(std::fs::read(directory.path().join("replica.db"))
        .unwrap()
        .iter()
        .all(|b| *b == 0));
    let replica = Replica::open_encrypted(
        &directory,
        InProcessServer::new(remote.clone()),
        Some([22; 32]),
    )
    .unwrap();
    assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
}

#[test]
fn encrypted_incremental_pull_replays_unpushed_local_changes() {
    for pages in [false, true] {
        let remote = Remote::new_encrypted(&[
            "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
            "INSERT INTO items VALUES (1, 'base')",
        ])
        .unwrap();
        let directory = Arc::new(tempfile::tempdir().unwrap());
        let mut server = InProcessServer::new(remote.clone());
        server.force_page_updates = pages;
        let replica = Replica::open_encrypted(&directory, server, Some([22; 32])).unwrap();
        replica
            .execute("UPDATE items SET value='local' WHERE id=1")
            .unwrap();
        replica
            .execute("INSERT INTO items VALUES (2, 'unpushed')")
            .unwrap();
        remote
            .execute_transaction(&["INSERT INTO items VALUES (3, 'remote')"])
            .unwrap();
        replica.pull().unwrap();
        remote
            .execute_transaction(&[
                "UPDATE items SET value='local' WHERE id=1",
                "INSERT INTO items VALUES (2, 'unpushed')",
            ])
            .unwrap();
        assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
        replica.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let expected = replica.snapshot().unwrap();
        drop(replica);
        let reopened =
            Replica::open_encrypted(&directory, InProcessServer::new(remote), Some([22; 32]))
                .unwrap();
        assert_eq!(reopened.snapshot().unwrap(), expected);
    }
}

#[test]
fn encrypted_late_staging_failure_does_not_publish_progress_or_delete_replica() {
    for pages in [false, true] {
        let remote =
            Remote::new_encrypted(&["CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)"])
                .unwrap();
        let directory = Arc::new(tempfile::tempdir().unwrap());
        let mut server = InProcessServer::new(remote.clone());
        server.force_page_updates = pages;
        let replica = Replica::open_encrypted(&directory, server, Some([22; 32])).unwrap();
        let before = replica.snapshot().unwrap();
        let info = std::fs::read(directory.path().join("replica.db-info")).unwrap();
        remote
            .execute_transaction(&["INSERT INTO items VALUES (1, 'first valid record')"])
            .unwrap();
        remote
            .execute_transaction(&["INSERT INTO items VALUES (2, hex(zeroblob(40000)))"])
            .unwrap();
        let error = replica.pull_with_tampered_staging_at(None).unwrap_err();
        assert!(
            matches!(
                error,
                turso_sync_engine::errors::Error::AuthenticationFailed
            ),
            "{error}"
        );
        assert_eq!(
            std::fs::read(directory.path().join("replica.db-info")).unwrap(),
            info
        );
        assert!(directory.path().join("replica.db").exists());
        if !pages {
            assert_eq!(replica.snapshot().unwrap(), before);
            replica.pull().unwrap();
            assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
        }
    }
}

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
        if self.main && pos >= 4096 {
            match self.failure.load(Ordering::SeqCst) {
                4 => {
                    self.failure.store(0, Ordering::SeqCst);
                    let partial =
                        Arc::new(Buffer::new(buffer.as_slice()[..buffer.len() - 1].to_vec()));
                    return self.inner.pwrite(pos, partial, c);
                }
                5 => {
                    self.failure.store(0, Ordering::SeqCst);
                    c.error(turso_core::CompletionError::ShortWrite);
                    return Ok(c);
                }
                _ => {}
            }
        }
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
        if failure == 3 {
            self.io.wait_for_completion(result)?;
            std::process::exit(86);
        }
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
    for stage in 1..=5 {
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
        let meta_path = directory.path().join("replica.db-info");
        let meta_before_retry = std::fs::read(&meta_path).ok();
        let reopened = Replica::open_encrypted(
            &directory,
            InProcessServer::new(remote.clone()),
            Some([22; 32]),
        );
        if stage == 5 {
            // Nonempty partial metadata must remain an authentication error.
            let meta = meta_before_retry.unwrap();
            assert!(!meta.is_empty());
            assert!(reopened.is_err());
            assert_eq!(
                std::fs::read(directory.path().join("replica.db")).unwrap(),
                main
            );
            assert_eq!(std::fs::read(meta_path).unwrap(), meta);
        } else {
            assert_eq!(
                reopened
                    .expect("retry after bootstrap failure")
                    .snapshot()
                    .unwrap(),
                remote.snapshot().unwrap()
            );
        }
        assert!(!directory.path().join("replica.db-bootstrap").exists());
    }
}

#[test]
fn encrypted_bootstrap_retries_after_failed_metadata_write() {
    let remote = Remote::new_encrypted(&[
        "CREATE TABLE items(value TEXT)",
        "INSERT INTO items VALUES ('retry after failure')",
    ])
    .expect("remote");
    for local_key in [None, Some([22; 32])] {
        for host_storage in [false, true] {
            for empty_metadata in [false, true] {
                let directory = Arc::new(tempfile::tempdir().expect("directory"));
                let files = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
                let server = || {
                    let mut server = InProcessServer::new(remote.clone());
                    if host_storage {
                        server.host_files = Some(files.clone());
                    }
                    server
                };
                let first = server();
                first.metadata_write_failure.store(1, Ordering::SeqCst);
                assert!(Replica::open_encrypted(&directory, first, local_key).is_err());
                assert!(
                    std::fs::metadata(directory.path().join("replica.db"))
                        .unwrap()
                        .len()
                        > 0
                );
                if empty_metadata {
                    let path = directory.path().join("replica.db-info");
                    if host_storage {
                        files
                            .lock()
                            .unwrap()
                            .insert(path.to_string_lossy().into_owned(), Vec::new());
                    } else {
                        std::fs::write(path, []).unwrap();
                    }
                }
                let retry = Replica::open_encrypted(&directory, server(), local_key)
                    .unwrap_or_else(|error| panic!(
                        "retry failed: local_key={}, host_storage={host_storage}, empty_metadata={empty_metadata}: {error}",
                        local_key.is_some()
                    ));
                assert_eq!(retry.snapshot().unwrap(), remote.snapshot().unwrap());
                assert!(!directory.path().join("replica.db-bootstrap").exists());
            }
        }
    }
}

#[test]
fn encrypted_bootstrap_recovers_after_process_exit() {
    const CHILD_PATH: &str = "TURSO_TEST_BOOTSTRAP_CRASH_PATH";
    let remote = Remote::new_encrypted(&[
        "CREATE TABLE items(value TEXT)",
        "INSERT INTO items VALUES ('crash retry')",
    ])
    .expect("remote");
    if let Ok(path) = std::env::var(CHILD_PATH) {
        let directory = Arc::new(tempfile::tempdir().expect("child resources"));
        let io = Arc::new(BootstrapIo {
            inner: Arc::new(PlatformIO::new().expect("IO")),
            failure: Arc::new(AtomicU8::new(3)),
        });
        let _ = Replica::open_encrypted_at_path(
            &directory,
            InProcessServer::new(remote),
            Some([22; 32]),
            io,
            &path,
        );
        panic!("child did not exit after the database fsync");
    }
    let directory = Arc::new(tempfile::tempdir().expect("directory"));
    let path = directory.path().join("replica.db");
    let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "encryption::encrypted_bootstrap_recovers_after_process_exit",
            "--nocapture",
        ])
        .env(CHILD_PATH, &path)
        .env("TMPDIR", directory.path())
        .output()
        .expect("crash subprocess");
    assert_eq!(output.status.code(), Some(86), "{output:?}");
    assert!(!directory.path().join("replica.db-info").exists());
    assert!(!directory.path().join("replica.db-bootstrap").exists());
    let replica = Replica::open_encrypted(
        &directory,
        InProcessServer::new(remote.clone()),
        Some([22; 32]),
    )
    .expect("retry after process exit");
    assert_eq!(
        replica.snapshot().expect("replica"),
        remote.snapshot().expect("remote")
    );
}

#[test]
fn encrypted_bootstrap_preserves_metadata_on_authentication_failure() {
    let remote = Remote::new_encrypted(&["CREATE TABLE items(value TEXT)"]).expect("remote");
    let directory = Arc::new(tempfile::tempdir().expect("directory"));
    let server = InProcessServer::new(remote.clone());
    // Metadata is complete, but publication reports an error before core opens.
    server.metadata_write_failure.store(2, Ordering::SeqCst);
    assert!(Replica::open_encrypted(&directory, server, Some([22; 32])).is_err());
    let db_path = directory.path().join("replica.db");
    let meta_path = directory.path().join("replica.db-info");
    let original_db = std::fs::read(&db_path).expect("database");
    let original_meta = std::fs::read(&meta_path).expect("metadata");
    assert!(Replica::open_encrypted(
        &directory,
        InProcessServer::new(remote.clone()),
        Some([23; 32])
    )
    .is_err());
    assert_eq!(std::fs::read(&db_path).unwrap(), original_db);
    assert_eq!(std::fs::read(&meta_path).unwrap(), original_meta);
    let mut damaged = original_meta.clone();
    *damaged.last_mut().unwrap() ^= 1;
    std::fs::write(&meta_path, &damaged).unwrap();
    assert!(Replica::open_encrypted(
        &directory,
        InProcessServer::new(remote.clone()),
        Some([22; 32])
    )
    .is_err());
    assert_eq!(std::fs::read(&db_path).unwrap(), original_db);
    assert_eq!(std::fs::read(&meta_path).unwrap(), damaged);
    std::fs::write(&meta_path, original_meta).unwrap();
    let replica = Replica::open_encrypted(&directory, InProcessServer::new(remote), Some([22; 32]))
        .expect("reopen with valid metadata and correct key");
    replica
        .execute("INSERT INTO items VALUES ('local row')")
        .unwrap();
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
fn encrypted_creation_refuses_conflicting_sidecars_and_incompatible_pages() {
    let remote = Remote::new_encrypted(&[]).unwrap();
    for suffix in ["-wal", "-wal-revert", "-log", "-changes"] {
        let directory = Arc::new(tempfile::tempdir().unwrap());
        let original = b"existing replica data must not be overwritten";
        let main = directory.path().join("replica.db");
        let sidecar = directory.path().join(format!("replica.db{suffix}"));
        std::fs::write(&main, original).unwrap();
        std::fs::write(&sidecar, original).unwrap();
        let error = Replica::open_encrypted(
            &directory,
            InProcessServer::new(remote.clone()),
            Some([22; 32]),
        )
        .err()
        .expect("conflicting sidecar must block bootstrap");
        assert!(
            error.to_string().contains("existing replica file"),
            "{error}"
        );
        assert!(
            error.to_string().contains(sidecar.to_str().unwrap()),
            "{error}"
        );
        assert_eq!(std::fs::read(main).unwrap(), original);
        assert_eq!(std::fs::read(sidecar).unwrap(), original);
        assert!(!directory.path().join("replica.db-info").exists());
    }
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
fn plaintext_empty_host_metadata_bootstraps_and_reopens() {
    let remote = Remote::new(&[
        "CREATE TABLE items(value TEXT)",
        "INSERT INTO items VALUES ('from remote')",
    ])
    .unwrap();
    for empty in [false, true] {
        let directory = Arc::new(tempfile::tempdir().unwrap());
        let path = directory
            .path()
            .join("replica.db-info")
            .to_string_lossy()
            .into_owned();
        let files = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        if empty {
            files.lock().unwrap().insert(path.clone(), Vec::new());
        }
        let server = || {
            let mut server = InProcessServer::new(remote.clone());
            server.host_files = Some(files.clone());
            server
        };
        let opts = super::harness::database_opts();
        let replica = Replica::open(&directory, server(), opts).unwrap();
        assert_eq!(replica.snapshot().unwrap(), remote.snapshot().unwrap());
        drop(replica);
        assert!(serde_json::from_slice::<serde_json::Value>(&files.lock().unwrap()[&path]).is_ok());
        let reopened = Replica::open(&directory, server(), opts).unwrap();
        assert_eq!(reopened.snapshot().unwrap(), remote.snapshot().unwrap());
    }
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
    let damaged = b"nonempty invalid metadata".to_vec();
    files.lock().unwrap().insert(path.clone(), damaged.clone());
    let main = std::fs::read(directory.path().join("replica.db")).unwrap();
    assert!(Replica::open_encrypted(&directory, server(), Some([22; 32])).is_err());
    assert_eq!(files.lock().unwrap()[&path], damaged);
    assert_eq!(
        std::fs::read(directory.path().join("replica.db")).unwrap(),
        main
    );
}
