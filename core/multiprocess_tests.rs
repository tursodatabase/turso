use super::*;
use crate::database::remove_file_from_database_registry;
use crate::dialect::cannot_parse::DialectThatCannotParse;
use crate::security::roles::SessionRole;
use crate::storage::pager::test_writes::{commit_new_pages, commit_user_version};
use crate::sync::atomic::Ordering;
use std::process::Command;

#[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
use crate::WindowsIOCP;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

const MULTIPROCESS_SHM_HOLD_READ_TX_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_shm_hold_read_tx_child_process";
const MULTIPROCESS_HOLD_OPEN_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_hold_open_child_process";

fn multiprocess_test_io() -> Arc<dyn IO> {
    #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
    {
        Arc::new(WindowsIOCP::new().unwrap())
    }

    #[cfg(not(all(target_os = "windows", feature = "experimental_win_iocp")))]
    {
        Arc::new(PlatformIO::new().unwrap())
    }
}

fn run_checkpoint(conn: &Arc<Connection>, mode: CheckpointMode) -> CheckpointResult {
    let pager = conn.pager.load();
    pager
        .io
        .block(|| pager.checkpoint(mode, SyncMode::Full, true))
        .unwrap()
}

fn wal_max_frame(conn: &Arc<Connection>) -> u64 {
    conn.pager
        .load()
        .wal
        .as_ref()
        .expect("wal should be present")
        .get_max_frame_in_wal()
}

fn wait_for_file(path: &std::path::Path) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("timed out waiting for {}", path.display());
}

fn multiprocess_wal_db_opts() -> DatabaseOpts {
    DatabaseOpts::new().with_multiprocess_wal(true)
}

fn open_multiprocess_db(io: Arc<dyn IO>, path: &str) -> Result<Arc<Database>> {
    Database::open_file_with_flags(
        io,
        path,
        OpenFlags::default(),
        multiprocess_wal_db_opts(),
        None,
        Arc::new(DialectThatCannotParse),
    )
}

fn open_multiprocess_db_with_flags(
    io: Arc<dyn IO>,
    path: &str,
    flags: OpenFlags,
) -> Result<Arc<Database>> {
    Database::open_file_with_flags(
        io,
        path,
        flags,
        multiprocess_wal_db_opts(),
        None,
        Arc::new(DialectThatCannotParse),
    )
}

#[test]
fn open_db_async_state_drop_clears_opening_registry_entry() {
    let key = DatabaseKey::SharedMemory(
        "open-db-async-state-drop-clears-opening-registry-entry".to_string(),
    );
    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.insert(key.clone(), RegistryEntry::Opening);
    }

    let mut state = OpenDbAsyncState::new();
    state.registry_key = Some(key.clone());
    drop(state);

    let manager = DATABASE_MANAGER.lock();
    assert!(
        !manager.contains_key(&key),
        "dropping an incomplete async open must clear the Opening sentinel"
    );
}

fn flip_db_header_reserved_byte(path: &std::path::Path) {
    use std::io::{Read, Seek, SeekFrom, Write};

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();

    #[cfg(feature = "checksum")]
    {
        let mut page = vec![0u8; 4096];
        file.read_exact(&mut page).unwrap();
        page[72] ^= 0x01;
        crate::storage::checksum::ChecksumContext::new()
            .add_checksum_to_page(&mut page, 1)
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&page).unwrap();
        file.sync_all().unwrap();
    }

    #[cfg(not(feature = "checksum"))]
    {
        file.seek(SeekFrom::Start(72)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0x01;
        file.seek(SeekFrom::Start(72)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();
    }
}

#[test]
#[cfg(any(target_os = "linux", target_os = "android"))]
fn shared_wal_coordination_rejects_remote_filesystem_magic_values() {
    assert!(!Database::filesystem_magic_allows_shared_wal(0x6969));
    assert!(!Database::filesystem_magic_allows_shared_wal(
        0xFF53_4D42u32 as libc::c_long,
    ));
    assert!(!Database::filesystem_magic_allows_shared_wal(0x0102_1997));
    assert!(Database::filesystem_magic_allows_shared_wal(0xEF53));
    assert!(Database::filesystem_magic_allows_shared_wal(0x0102_1994));
}

#[test]
#[cfg(host_shared_wal)]
fn shared_wal_coordination_path_probe_accepts_nonexistent_relative_paths() {
    let result = Database::path_allows_shared_wal_coordination(std::path::Path::new(
        "nonexistent-relative-multiprocess-wal.db",
    ));
    assert!(
        result.is_ok(),
        "nonexistent relative paths should probe the current directory instead of erroring: {result:?}"
    );
}

#[test]
fn database_open_without_experimental_multiprocess_wal_uses_in_process_backend() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-default-off.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = Database::open_file(io, db_path_str, Arc::new(DialectThatCannotParse)).unwrap();

    let last_checksum_and_max_frame = db.shared_wal.read().last_checksum_and_max_frame();
    let wal = db
        .build_wal(last_checksum_and_max_frame, db.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "in_process");
    assert!(db.shared_wal_coordination().unwrap().is_none());

    let shm_path = storage::wal::coordination_path_for_wal_path(&format!("{db_path_str}-wal"));
    assert!(!std::path::Path::new(&shm_path).exists());
}

/// Child half of `reject_live_multiprocess_probe_uses_configured_wal_path`:
/// open the multiprocess database, hold it open (keeping the authority in
/// multiprocess mode), signal readiness, and wait for release before exiting.
#[test]
fn multiprocess_hold_open_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };
    let ready_file = std::env::var_os("TURSO_MULTIPROCESS_READY_FILE")
        .map(std::path::PathBuf::from)
        .unwrap();
    let release_file = std::env::var_os("TURSO_MULTIPROCESS_RELEASE_FILE")
        .map(std::path::PathBuf::from)
        .unwrap();

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    let last = db.shared_wal.read().last_checksum_and_max_frame();
    let wal = db.build_wal(last, db.buffer_pool.clone()).unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_open_mode_name(), Some("multiprocess"));

    std::fs::write(&ready_file, b"ready").unwrap();
    wait_for_file(&release_file);
}

/// The legacy/multiprocess probe keys the coordination file off the configured
/// WAL path, not a hard-coded `{path}-wal`. With a live multiprocess authority
/// held open by a child process, the probe rejects a legacy open only when it
/// inspects the WAL path that actually backs the authority; a probe pointed at
/// a different WAL path finds nothing. Before the fix, the probe hard-coded
/// `{path}-wal` and so missed the authority whenever a custom WAL path was in
/// use.
#[test]
fn reject_live_multiprocess_probe_uses_configured_wal_path() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("probe-wal-path.db");
    let db_path_str = db_path.to_str().unwrap();
    let ready_file = dir.path().join("child-ready");
    let release_file = dir.path().join("child-release");
    let io: Arc<dyn IO> = multiprocess_test_io();

    // Parent keeps the multiprocess database open so a second (child) opener
    // flips the authority into multiprocess mode.
    let _db = open_multiprocess_db(io.clone(), db_path_str).unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let mut child = Command::new(&current_exe)
        .arg(MULTIPROCESS_HOLD_OPEN_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .env("TURSO_MULTIPROCESS_READY_FILE", &ready_file)
        .env("TURSO_MULTIPROCESS_RELEASE_FILE", &release_file)
        .spawn()
        .unwrap();
    wait_for_file(&ready_file);

    // Probing the WAL path that actually backs the authority finds it and
    // rejects a legacy open.
    let default_wal = format!("{db_path_str}-wal");
    let err = Database::reject_live_multiprocess_wal_for_legacy_open(
        &io,
        db_path_str,
        Some(&default_wal),
        DatabaseOpts::new(),
    )
    .expect_err("legacy open over a live multiprocess authority must be rejected");
    assert!(
        matches!(err, LimboError::LockingError(_)),
        "expected LockingError from the multiprocess probe, got {err:?}"
    );

    // Probing an unrelated WAL path must NOT reject: the coordination file is
    // keyed on the WAL path, so the probe must honor its argument.
    let other_wal = format!("{db_path_str}-other-wal");
    Database::reject_live_multiprocess_wal_for_legacy_open(
        &io,
        db_path_str,
        Some(&other_wal),
        DatabaseOpts::new(),
    )
    .expect("probe over an unrelated wal path must not reject");

    std::fs::write(&release_file, b"release").unwrap();
    assert!(
        child.wait().unwrap().success(),
        "hold-open child should exit cleanly after release"
    );
}

#[test]
fn database_open_selects_shm_wal_backend() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();

    let last_checksum_and_max_frame = db.shared_wal.read().last_checksum_and_max_frame();
    let wal = db
        .build_wal(last_checksum_and_max_frame, db.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "tshm");

    let shm_path = storage::wal::coordination_path_for_wal_path(&format!("{db_path_str}-wal"));
    assert!(std::path::Path::new(&shm_path).exists());
}

#[test]
fn database_open_rebuilds_from_disk_scan_when_exclusive_shm_snapshot_is_stale() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-stale-reopen.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db_a = open_multiprocess_db(io.clone(), db_path_str).unwrap();
    let conn_a = db_a
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    commit_new_pages(&conn_a, 1);
    commit_user_version(&conn_a, 1);

    let authority = db_a.shared_wal_coordination().unwrap().unwrap();
    let stale_snapshot = authority.snapshot();

    commit_user_version(&conn_a, 2);
    let header_before = read_header(&conn_a);

    authority.install_snapshot(stale_snapshot);

    drop(conn_a);
    drop(db_a);
    remove_file_from_database_registry(db_path_str);

    let db_b = open_multiprocess_db(io, db_path_str).unwrap();
    assert!(
        db_b.shared_wal
            .read()
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire),
        "cold reopen should fall back to a WAL disk scan when tshm snapshot is stale"
    );

    let conn_b = db_b
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(read_header(&conn_b), header_before);
}

#[test]
fn database_open_reuses_trusted_tshm_snapshot_without_disk_scan_when_no_backfill_proof_is_needed() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-trusted-tail-reopen.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db = open_multiprocess_db(io.clone(), db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    commit_new_pages(&conn, 1);
    commit_user_version(&conn, 1);
    commit_user_version(&conn, 2);
    let header_before = read_header(&conn);

    let authority = db.shared_wal_coordination().unwrap().unwrap();
    let snapshot_before = authority.snapshot();
    assert!(
        snapshot_before.max_frame > 0,
        "trusted-tail reopen coverage requires committed WAL frames before reopen"
    );
    assert_eq!(
        snapshot_before.nbackfills, 0,
        "this coverage only applies when no positive backfill proof is required"
    );
    let frames_before = authority.iter_latest_frames(0, snapshot_before.max_frame);
    assert!(
        !frames_before.is_empty(),
        "trusted-tail reopen coverage requires a populated durable frame index"
    );

    drop(conn);
    drop(db);
    remove_file_from_database_registry(db_path_str);

    let reopened = open_multiprocess_db(io, db_path_str).unwrap();
    assert!(
        !reopened
            .shared_wal
            .read()
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire),
        "reopen should trust the persisted tshm snapshot when no backfill-proof validation is needed"
    );

    let reopened_authority = reopened.shared_wal_coordination().unwrap().unwrap();
    assert_eq!(
        reopened_authority.snapshot(),
        snapshot_before,
        "trusted reopen must preserve the authoritative WAL snapshot"
    );
    assert_eq!(
        reopened_authority.iter_latest_frames(0, snapshot_before.max_frame),
        frames_before,
        "trusted reopen must preserve durable frame-index content"
    );

    let reopened_conn = reopened
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(read_header(&reopened_conn), header_before);
}

#[test]
fn database_open_rebuilds_from_disk_scan_after_partial_checkpoint_without_backfill_proof() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir
        .path()
        .join("coordination-partial-checkpoint-reopen-no-proof.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db = open_multiprocess_db(io.clone(), db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.wal_auto_actions_disable();
    commit_new_pages(&conn, 32);
    let header_before = read_header(&conn);
    assert!(
        wal_max_frame(&conn) > 1,
        "partial-checkpoint reopen coverage requires more than one WAL frame before checkpointing"
    );

    run_checkpoint(
        &conn,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
    );

    let authority = db.shared_wal_coordination().unwrap().unwrap();
    let snapshot_before = authority.snapshot();
    assert!(
        snapshot_before.nbackfills > 0,
        "partial-checkpoint reopen coverage requires persisted positive nbackfills"
    );
    assert!(
        snapshot_before.max_frame > snapshot_before.nbackfills,
        "partial-checkpoint reopen coverage requires live WAL frames beyond the backfill point"
    );
    authority.clear_backfill_proof();

    drop(conn);
    drop(db);
    remove_file_from_database_registry(db_path_str);

    let reopened = open_multiprocess_db(io, db_path_str).unwrap();
    let mut expected_snapshot = snapshot_before;
    expected_snapshot.nbackfills = 0;
    assert!(
        reopened
            .shared_wal
            .read()
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire),
        "reopen must rebuild shared WAL state from disk when positive nbackfills are not durably provable"
    );

    let reopened_authority = reopened.shared_wal_coordination().unwrap().unwrap();
    assert_eq!(
        reopened_authority.snapshot(),
        expected_snapshot,
        "disk-scan reopen must preserve the WAL generation while clearing untrusted positive nbackfills"
    );

    let reopened_conn = reopened
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(
        read_header(&reopened_conn),
        header_before,
        "partial-checkpoint reopen should preserve committed rows after the conservative disk scan"
    );
}

#[test]
fn database_open_rebuilds_from_disk_scan_after_wal_append_invalidates_backfill_proof() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir
        .path()
        .join("coordination-partial-checkpoint-reopen-stale-proof.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db = open_multiprocess_db(io.clone(), db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.wal_auto_actions_disable();
    commit_new_pages(&conn, 32);

    run_checkpoint(
        &conn,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
    );

    let authority = db.shared_wal_coordination().unwrap().unwrap();
    let snapshot_after_checkpoint = authority.snapshot();
    assert!(
        snapshot_after_checkpoint.nbackfills > 0,
        "stale-proof coverage requires a partial checkpoint that publishes positive nbackfills"
    );
    assert!(
        snapshot_after_checkpoint.max_frame > snapshot_after_checkpoint.nbackfills,
        "stale-proof coverage requires live WAL frames beyond the backfill point"
    );

    commit_new_pages(&conn, 1);
    let header_before = read_header(&conn);
    let snapshot_after_append = authority.snapshot();
    assert!(
        snapshot_after_append.max_frame > snapshot_after_checkpoint.max_frame,
        "stale-proof coverage requires a WAL append after proof installation"
    );

    drop(conn);
    drop(db);
    remove_file_from_database_registry(db_path_str);

    let reopened = open_multiprocess_db(io, db_path_str).unwrap();
    assert!(
        reopened
            .shared_wal
            .read()
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire),
        "reopen must rebuild from disk after a WAL append invalidates the tshm backfill proof"
    );

    let reopened_conn = reopened
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(
        read_header(&reopened_conn),
        header_before,
        "stale-proof reopen should preserve rows committed after the partial checkpoint"
    );
}

#[test]
fn database_open_rebuilds_from_disk_scan_after_db_header_mismatch_invalidates_backfill_proof() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir
        .path()
        .join("coordination-partial-checkpoint-reopen-db-header-mismatch.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db = open_multiprocess_db(io.clone(), db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.wal_auto_actions_disable();
    commit_new_pages(&conn, 32);
    let header_before = read_header(&conn);

    run_checkpoint(
        &conn,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
    );

    let authority = db.shared_wal_coordination().unwrap().unwrap();
    let snapshot_before = authority.snapshot();
    assert!(
        snapshot_before.nbackfills > 0,
        "DB-header mismatch coverage requires a partial checkpoint that publishes positive nbackfills"
    );
    assert!(
        snapshot_before.max_frame > snapshot_before.nbackfills,
        "DB-header mismatch coverage requires live WAL frames beyond the backfill point"
    );

    drop(conn);
    drop(db);
    flip_db_header_reserved_byte(&db_path);
    remove_file_from_database_registry(db_path_str);

    let reopened = open_multiprocess_db(io, db_path_str).unwrap();
    let mut expected_snapshot = snapshot_before;
    expected_snapshot.nbackfills = 0;
    assert!(
        reopened
            .shared_wal
            .read()
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire),
        "reopen must rebuild from disk when the DB header fingerprint no longer matches the tshm backfill proof"
    );

    let reopened_authority = reopened.shared_wal_coordination().unwrap().unwrap();
    assert_eq!(
        reopened_authority.snapshot(),
        expected_snapshot,
        "disk-scan reopen must preserve the WAL generation while clearing untrusted positive nbackfills"
    );

    let reopened_conn = reopened
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(
        read_header(&reopened_conn),
        header_before,
        "DB-header-mismatch reopen should preserve committed rows after the conservative disk scan"
    );
}

#[test]
fn multiprocess_shm_hold_read_tx_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };
    let ready_file = std::env::var_os("TURSO_MULTIPROCESS_READY_FILE")
        .map(std::path::PathBuf::from)
        .unwrap();
    let release_file = std::env::var_os("TURSO_MULTIPROCESS_RELEASE_FILE")
        .map(std::path::PathBuf::from)
        .unwrap();
    let readonly = std::env::var_os("TURSO_MULTIPROCESS_READONLY").is_some();
    let expect_disk_scan = std::env::var_os("TURSO_MULTIPROCESS_EXPECT_DISK_SCAN").is_some();

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = if readonly {
        open_multiprocess_db_with_flags(io, db_path.to_str().unwrap(), OpenFlags::ReadOnly).unwrap()
    } else {
        open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap()
    };
    let last_checksum_and_max_frame = db.shared_wal.read().last_checksum_and_max_frame();
    let wal = db
        .build_wal(last_checksum_and_max_frame, db.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "tshm");
    assert_eq!(wal_file.coordination_open_mode_name(), Some("multiprocess"));
    assert_eq!(
        db.shared_wal
            .read()
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire),
        expect_disk_scan,
        "child reopen path did not match expected disk-scan behavior"
    );

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let pager = conn.pager.load();
    let wal = pager.wal.as_ref().unwrap();
    wal.begin_read_tx().unwrap();
    std::fs::write(&ready_file, b"ready").unwrap();
    wait_for_file(&release_file);
    wal.end_read_tx();
}

#[test]
fn subprocess_readonly_child_reader_blocks_restart_and_truncate_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir
        .path()
        .join("coordination-readonly-reader-blocks-checkpoint.db");
    let ready_file = dir.path().join("child-ready");
    let release_file = dir.path().join("child-release");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.wal_auto_actions_disable();
    commit_new_pages(&conn, 32);
    assert!(
        wal_max_frame(&conn) > 0,
        "read-only reader coverage requires committed WAL frames"
    );

    let authority = db.shared_wal_coordination().unwrap().unwrap();
    assert_eq!(
        authority.min_active_reader_frame(),
        None,
        "setup should start without active shared readers"
    );

    let current_exe = std::env::current_exe().unwrap();
    let mut child = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_HOLD_READ_TX_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .env("TURSO_MULTIPROCESS_READY_FILE", &ready_file)
        .env("TURSO_MULTIPROCESS_RELEASE_FILE", &release_file)
        .env("TURSO_MULTIPROCESS_READONLY", "1")
        .spawn()
        .unwrap();

    wait_for_file(&ready_file);
    let reader_frame = authority
        .min_active_reader_frame()
        .expect("read-only child should publish an active shared reader slot");
    assert!(
        reader_frame > 0,
        "read-only child should pin a positive WAL frame while its read transaction is active"
    );

    let restart_err = conn.checkpoint(CheckpointMode::Restart).unwrap_err();
    assert!(
        matches!(restart_err, LimboError::Busy),
        "restart checkpoint should fail with Busy while the read-only child holds a WAL snapshot: {restart_err:?}",
    );

    let truncate_err = conn
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap_err();
    assert!(
        matches!(truncate_err, LimboError::Busy),
        "truncate checkpoint should fail with Busy while the read-only child holds a WAL snapshot: {truncate_err:?}",
    );

    std::fs::write(&release_file, b"release").unwrap();
    let child_status = child.wait().unwrap();
    assert!(
        child_status.success(),
        "read-only child should exit cleanly after releasing its read transaction: {child_status:?}"
    );

    let checkpoint = conn
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
    assert!(
        checkpoint.everything_backfilled(),
        "truncate checkpoint should succeed once the read-only child releases its WAL snapshot"
    );
    assert_eq!(
        authority.min_active_reader_frame(),
        None,
        "shared reader state should clear once the read-only child releases its WAL snapshot"
    );
}

#[test]
fn subprocess_readonly_disk_scan_child_reader_stays_in_shared_coordination() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir
        .path()
        .join("coordination-readonly-disk-scan-reader-blocks-checkpoint.db");
    let ready_file = dir.path().join("child-ready");
    let release_file = dir.path().join("child-release");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db = open_multiprocess_db(io.clone(), db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.wal_auto_actions_disable();
    commit_new_pages(&conn, 32);
    let header_before = read_header(&conn);
    assert!(
        wal_max_frame(&conn) > 1,
        "disk-scan read-only coverage requires more than one WAL frame before partial checkpoint"
    );

    run_checkpoint(
        &conn,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
    );

    let authority = db.shared_wal_coordination().unwrap().unwrap();
    let snapshot_before = authority.snapshot();
    assert!(
        snapshot_before.nbackfills > 0,
        "disk-scan read-only coverage requires persisted positive nbackfills before proof removal"
    );
    assert!(
        snapshot_before.max_frame > snapshot_before.nbackfills,
        "disk-scan read-only coverage requires a live WAL tail beyond the backfill point"
    );
    authority.clear_backfill_proof();

    drop(conn);
    drop(db);
    remove_file_from_database_registry(db_path_str);

    let current_exe = std::env::current_exe().unwrap();
    let mut child = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_HOLD_READ_TX_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .env("TURSO_MULTIPROCESS_READY_FILE", &ready_file)
        .env("TURSO_MULTIPROCESS_RELEASE_FILE", &release_file)
        .env("TURSO_MULTIPROCESS_READONLY", "1")
        .env("TURSO_MULTIPROCESS_EXPECT_DISK_SCAN", "1")
        .spawn()
        .unwrap();

    wait_for_file(&ready_file);
    let reader_frame = authority
        .min_active_reader_frame()
        .expect("disk-scan read-only child should publish an active shared reader slot");
    assert!(
        reader_frame > snapshot_before.nbackfills,
        "disk-scan read-only child should protect the live WAL tail, not the checkpointed prefix"
    );

    let reopened = open_multiprocess_db(io, db_path_str).unwrap();
    let reopened_conn = reopened
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    let restart_err = reopened_conn
        .checkpoint(CheckpointMode::Restart)
        .unwrap_err();
    assert!(
        matches!(restart_err, LimboError::Busy),
        "restart checkpoint should fail with Busy while the disk-scan read-only child holds a WAL snapshot: {restart_err:?}",
    );

    let truncate_err = reopened_conn
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap_err();
    assert!(
        matches!(truncate_err, LimboError::Busy),
        "truncate checkpoint should fail with Busy while the disk-scan read-only child holds a WAL snapshot: {truncate_err:?}",
    );

    drop(reopened_conn);
    drop(reopened);
    remove_file_from_database_registry(db_path_str);

    let wal_len = std::fs::metadata(format!("{db_path_str}-wal"))
        .unwrap_or_else(|_| panic!("expected WAL file at {db_path_str}-wal"))
        .len();
    assert!(
        wal_len > 0,
        "closing the other process must not run shutdown checkpoint while the disk-scan read-only child still holds a shared reader slot"
    );

    std::fs::write(&release_file, b"release").unwrap();
    let child_status = child.wait().unwrap();
    assert!(
        child_status.success(),
        "disk-scan read-only child should exit cleanly after releasing its read transaction: {child_status:?}"
    );

    let reopened = open_multiprocess_db(multiprocess_test_io(), db_path_str).unwrap();
    let reopened_conn = reopened
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let checkpoint = reopened_conn
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
    assert!(
        checkpoint.everything_backfilled(),
        "truncate checkpoint should succeed once the disk-scan read-only child releases its WAL snapshot"
    );
    assert_eq!(
        read_header(&reopened_conn),
        header_before,
        "disk-scan read-only reopen should preserve committed rows"
    );
}

#[test]
fn subprocess_database_truncate_checkpoint_reclaims_dead_child_reader_slot() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-dead-reader-slot.db");
    let ready_file = dir.path().join("child-ready");
    let release_file = dir.path().join("child-release");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    commit_new_pages(&conn, 1);
    commit_user_version(&conn, 1);

    let authority = db.shared_wal_coordination().unwrap().unwrap();
    assert_eq!(
        authority.min_active_reader_frame(),
        None,
        "setup should start without active shared reader slots"
    );

    let current_exe = std::env::current_exe().unwrap();
    let mut child = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_HOLD_READ_TX_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .env("TURSO_MULTIPROCESS_READY_FILE", &ready_file)
        .env("TURSO_MULTIPROCESS_RELEASE_FILE", &release_file)
        .spawn()
        .unwrap();

    wait_for_file(&ready_file);
    let reader_frame = authority
        .min_active_reader_frame()
        .expect("child read transaction should publish an active shared reader slot");

    commit_user_version(&conn, 2);
    let header_before = read_header(&conn);
    assert!(
        wal_max_frame(&conn) > reader_frame,
        "parent should append newer WAL frames after the child pins its read snapshot"
    );

    let err = conn
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, LimboError::Busy),
        "truncate checkpoint should fail with Busy while child process holds a shared reader slot: {err:?}",
    );

    child.kill().unwrap();
    let child_status = child.wait().unwrap();
    #[cfg(unix)]
    assert_eq!(
        child_status.signal(),
        Some(libc::SIGKILL),
        "expected killed child process to exit via SIGKILL, got {child_status:?}"
    );
    #[cfg(windows)]
    assert!(
        !child_status.success(),
        "expected killed child process to exit unsuccessfully on Windows, got {child_status:?}"
    );

    let checkpoint = conn
        .checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
    assert!(
        checkpoint.everything_backfilled(),
        "truncate checkpoint should succeed after reclaiming the dead child shared reader slot"
    );
    assert_eq!(
        authority.min_active_reader_frame(),
        None,
        "successful checkpoint should reclaim the dead child shared reader slot"
    );

    let wal_len = std::fs::metadata(format!("{db_path_str}-wal"))
        .map(|meta| meta.len())
        .unwrap_or(0);
    assert_eq!(wal_len, 0, "truncate checkpoint should leave the WAL empty");

    drop(conn);
    drop(db);
    remove_file_from_database_registry(db_path_str);

    let io: Arc<dyn IO> = multiprocess_test_io();
    let reopened = open_multiprocess_db(io, db_path_str).unwrap();
    let reopened_conn = reopened
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(
        read_header(&reopened_conn),
        header_before,
        "cold reopen after reader-slot reclamation should preserve committed rows"
    );
}

#[test]
fn database_open_reopen_with_live_child_reader_does_not_clobber_authority() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-live-reader-reopen.db");
    let ready_file = dir.path().join("child-ready");
    let release_file = dir.path().join("child-release");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db = open_multiprocess_db(io.clone(), db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.wal_auto_actions_disable();
    commit_new_pages(&conn, 32);
    let header_before = read_header(&conn);
    assert!(
        wal_max_frame(&conn) > 1,
        "disk-scan reopen coverage requires more than one WAL frame before partial checkpoint"
    );

    run_checkpoint(
        &conn,
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
    );

    let authority = db.shared_wal_coordination().unwrap().unwrap();
    let snapshot_before = authority.snapshot();
    assert!(
        snapshot_before.nbackfills > 0,
        "disk-scan reopen coverage requires a persisted authority snapshot with positive nbackfills"
    );
    assert!(
        snapshot_before.max_frame > snapshot_before.nbackfills,
        "disk-scan reopen coverage requires live WAL frames beyond the backfill point"
    );
    let frames_before = authority.iter_latest_frames(0, snapshot_before.max_frame);
    assert!(
        !frames_before.is_empty(),
        "setup should publish durable frame-index entries before reopen repair"
    );

    let current_exe = std::env::current_exe().unwrap();
    let mut child = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_HOLD_READ_TX_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .env("TURSO_MULTIPROCESS_READY_FILE", &ready_file)
        .env("TURSO_MULTIPROCESS_RELEASE_FILE", &release_file)
        .spawn()
        .unwrap();

    wait_for_file(&ready_file);
    let reader_frame = authority
        .min_active_reader_frame()
        .expect("child read transaction should publish an active shared reader slot");

    drop(conn);
    drop(db);
    remove_file_from_database_registry(db_path_str);

    let reopened = open_multiprocess_db(io, db_path_str).unwrap();
    let reopened_authority = reopened.shared_wal_coordination().unwrap().unwrap();
    assert_eq!(reopened_authority.snapshot(), snapshot_before);
    assert_eq!(
        reopened_authority.iter_latest_frames(0, snapshot_before.max_frame),
        frames_before,
        "reopen repair must preserve durable frame-index content while the child reader is still alive"
    );
    assert_eq!(
        reopened_authority.min_active_reader_frame(),
        Some(reader_frame),
        "reopen repair must not reclaim the live child reader slot"
    );

    std::fs::write(&release_file, b"release").unwrap();
    let child_status = child.wait().unwrap();
    assert!(
        child_status.success(),
        "child reader should exit cleanly after reopen repair: {child_status:?}"
    );

    let reopened_conn = reopened
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(
        read_header(&reopened_conn),
        header_before,
        "reopen repair should preserve committed rows while leaving the child reader intact"
    );
}

#[test]
fn database_open_rebuilds_from_disk_scan_when_shared_frame_index_overflowed() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir
        .path()
        .join("coordination-overflowed-frame-index-reopen.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db_a = open_multiprocess_db(io.clone(), db_path_str).unwrap();
    let conn_a = db_a
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    commit_new_pages(&conn_a, 1);
    commit_user_version(&conn_a, 1);
    commit_user_version(&conn_a, 2);
    let header_before = read_header(&conn_a);

    let authority = db_a.shared_wal_coordination().unwrap().unwrap();
    let snapshot = authority.snapshot();
    assert!(
        snapshot.max_frame > 0,
        "overflow reopen coverage requires committed WAL frames before the authority is marked incomplete"
    );
    assert!(
        !authority
            .iter_latest_frames(0, snapshot.max_frame)
            .is_empty(),
        "overflow reopen coverage requires a populated durable frame index before it is discarded"
    );

    authority.discard_durable_frame_index_for_exclusive_rebuild();
    authority.mark_frame_index_overflowed_for_tests();

    assert!(
        authority
            .iter_latest_frames(0, snapshot.max_frame)
            .is_empty(),
        "test setup should clear durable frame-index entries before reopen"
    );
    assert!(
        authority.frame_index_overflowed(),
        "test setup should mark the shared frame index incomplete before reopen"
    );

    drop(conn_a);
    drop(db_a);
    remove_file_from_database_registry(db_path_str);

    let db_b = open_multiprocess_db(io, db_path_str).unwrap();
    assert!(
        db_b.shared_wal
            .read()
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire),
        "overflowed shared frame index must force a conservative WAL disk scan on reopen"
    );

    let reopened_authority = db_b.shared_wal_coordination().unwrap().unwrap();
    assert!(
        !reopened_authority.frame_index_overflowed(),
        "disk-scan reopen should rebuild the durable frame index instead of leaving it overflowed"
    );
    assert!(
        !reopened_authority
            .iter_latest_frames(0, snapshot.max_frame)
            .is_empty(),
        "disk-scan reopen should repopulate the durable frame index from the WAL"
    );

    let conn_b = db_b
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(read_header(&conn_b), header_before);
}

#[test]
fn memory_database_keeps_in_process_wal_backend() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(DialectThatCannotParse)).unwrap();

    let last_checksum_and_max_frame = db.shared_wal.read().last_checksum_and_max_frame();
    let wal = db
        .build_wal(last_checksum_and_max_frame, db.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "in_process");
}

/// The database size in pages and the user version that a new read
/// transaction of `conn` sees.
fn read_header(conn: &Arc<Connection>) -> (u32, i32) {
    let pager = conn.pager.load();
    pager.begin_read_tx().unwrap();
    let header = pager
        .io
        .block(|| {
            pager.with_header(|header| (header.database_size.get(), header.user_version.get()))
        })
        .unwrap();
    pager.end_read_tx();
    header
}

#[test]
fn database_open_reads_main_file_when_wal_is_empty_but_tshm_claims_frames() {
    reopen_after_wal_lost_frames("coordination-empty-wal-reopen.db", false, false);
}

#[test]
fn database_open_reads_main_file_when_wal_is_empty_but_overflowed_tshm_claims_frames() {
    reopen_after_wal_lost_frames("coordination-empty-wal-overflowed-reopen.db", true, false);
}

#[test]
fn database_open_reads_wal_frames_that_remain_when_tshm_claims_more_frames() {
    reopen_after_wal_lost_frames("coordination-short-wal-reopen.db", false, true);
}

/// Commits two transactions after a checkpoint, closes the database, truncates
/// the WAL file to drop both transactions (or only the last one when
/// `keep_first_commit` is set) while the -tshm file still describes them, and
/// checks that a reopen reads only what the WAL file and the main file have.
fn reopen_after_wal_lost_frames(file_name: &str, mark_overflowed: bool, keep_first_commit: bool) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join(file_name);
    let db_path_str = db_path.to_str().unwrap();
    let wal_path = format!("{db_path_str}-wal");
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db_a = open_multiprocess_db(io.clone(), db_path_str).unwrap();
    let conn_a = db_a
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    commit_new_pages(&conn_a, 1);
    commit_user_version(&conn_a, 1);
    run_checkpoint(
        &conn_a,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    let header_in_main_file = read_header(&conn_a);
    commit_new_pages(&conn_a, 3);
    let header_after_first_commit = read_header(&conn_a);
    let frames_of_first_commit = wal_max_frame(&conn_a);
    commit_user_version(&conn_a, 2);
    assert!(wal_max_frame(&conn_a) > frames_of_first_commit);
    assert_ne!(read_header(&conn_a), header_after_first_commit);
    assert_ne!(header_after_first_commit, header_in_main_file);
    if mark_overflowed {
        db_a.shared_wal_coordination()
            .unwrap()
            .unwrap()
            .mark_frame_index_overflowed_for_tests();
    }

    drop(conn_a);
    drop(db_a);
    remove_file_from_database_registry(db_path_str);

    let (wal_len, expected_frames, expected_header) = if keep_first_commit {
        let frame_size = (storage::sqlite3_ondisk::WAL_FRAME_HEADER_SIZE + 4096) as u64;
        (
            storage::sqlite3_ondisk::WAL_HEADER_SIZE as u64 + frames_of_first_commit * frame_size,
            frames_of_first_commit,
            header_after_first_commit,
        )
    } else {
        (0, 0, header_in_main_file)
    };
    std::fs::OpenOptions::new()
        .write(true)
        .open(&wal_path)
        .unwrap()
        .set_len(wal_len)
        .unwrap();

    let db_b = open_multiprocess_db(io, db_path_str).unwrap();
    let conn_b = db_b
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert_eq!(wal_max_frame(&conn_b), expected_frames);
    assert_eq!(read_header(&conn_b), expected_header);
}

const MULTIPROCESS_DEAD_WRITER_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_dead_writer_child_process";

/// Child roles for `new_writer_after_killed_writer_with_uncommitted_frames_commits`.
/// `spill` appends uncommitted frames (the page cache spills them to the WAL),
/// reports readiness and waits until the parent terminates it. `commit` is the
/// next writer: one write transaction that must commit.
#[test]
fn multiprocess_dead_writer_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };
    let role = std::env::var("TURSO_MULTIPROCESS_ROLE").unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    match role.as_str() {
        "spill" => {
            let ready_file = std::env::var_os("TURSO_MULTIPROCESS_READY_FILE")
                .map(std::path::PathBuf::from)
                .unwrap();
            let pager = conn.pager.load();
            pager.begin_read_tx().unwrap();
            pager
                .io
                .block(|| pager.begin_write_tx(conn.wal_auto_actions()))
                .unwrap();
            // More new pages than the page cache holds, so it spills dirty
            // pages to the WAL before any commit.
            for _ in 0..8000 {
                pager.io.block(|| pager.allocate_page()).unwrap();
            }
            std::fs::write(&ready_file, b"ready").unwrap();
            // Never released: the parent terminates this process.
            wait_for_file(&ready_file.with_extension("never"));
        }
        "commit" => commit_new_pages(&conn, 1),
        other => panic!("unknown role {other}"),
    }
}

/// A writer terminated after its page cache spilled uncommitted frames leaves
/// their entries in the shared frame index, above the published max_frame. The
/// next writer must discard them instead of tripping the monotonic-frame assert.
#[test]
fn new_writer_after_killed_writer_with_uncommitted_frames_commits() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("mp-dead-writer.db");
    let db_path_str = db_path.to_str().unwrap();
    let ready_file = dir.path().join("spilled");

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    commit_new_pages(&conn, 1);
    let header_before = read_header(&conn);
    let wal_len_before = std::fs::metadata(format!("{db_path_str}-wal"))
        .unwrap()
        .len();

    let spawn = |role: &str| {
        Command::new(std::env::current_exe().unwrap())
            .arg(MULTIPROCESS_DEAD_WRITER_CHILD_TEST)
            .arg("--exact")
            .arg("--nocapture")
            .env("TURSO_MULTIPROCESS_DB_PATH", &db_path)
            .env("TURSO_MULTIPROCESS_READY_FILE", &ready_file)
            .env("TURSO_MULTIPROCESS_ROLE", role)
            .spawn()
            .unwrap()
    };

    let mut victim = spawn("spill");
    wait_for_file(&ready_file);
    victim.kill().unwrap();
    let status = victim.wait().unwrap();
    #[cfg(unix)]
    assert_eq!(status.signal(), Some(libc::SIGKILL), "got {status:?}");
    #[cfg(not(unix))]
    assert!(!status.success());
    let wal_len_after = std::fs::metadata(format!("{db_path_str}-wal"))
        .unwrap()
        .len();
    assert!(
        wal_len_after > wal_len_before,
        "precondition: the terminated writer must have appended uncommitted frames"
    );

    let status = spawn("commit").wait().unwrap();
    assert!(status.success(), "next writer failed: {status:?}");
    // The next writer's page is the only growth: none of the terminated
    // writer's pages became visible.
    assert_eq!(
        read_header(&conn),
        (header_before.0 + 1, header_before.1),
        "database size and user version after the next writer"
    );
}
