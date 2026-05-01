use super::*;
use std::process::Command;

#[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
use crate::WindowsIOCP;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

const MULTIPROCESS_SHM_INSERT_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_shm_insert_child_process";
const MULTIPROCESS_SHM_COUNT_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_shm_count_child_process";
const MULTIPROCESS_SHM_HOLD_READ_TX_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_shm_hold_read_tx_child_process";
const MULTIPROCESS_SHM_SCHEMA_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_shm_schema_child_process";
const MULTIPROCESS_SHM_INSERT_AND_CLOSE_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_shm_insert_and_close_child_process";
const MULTIPROCESS_SHM_EXPECT_OPEN_FAILURE_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_shm_expect_open_failure_child_process";
const DEFAULT_LOCKED_DB_CHILD_TEST: &str = "multiprocess_tests::default_locked_db_child_process";

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

fn count_test_rows(conn: &Arc<Connection>) -> i64 {
    let mut stmt = conn.prepare("select count(*) from test").unwrap();
    let mut count = 0;
    stmt.run_with_row_callback(|row| {
        count = row.get(0).unwrap();
        Ok(())
    })
    .unwrap();
    count
}

fn get_rows(conn: &Arc<Connection>, query: &str) -> Vec<Vec<Value>> {
    for _attempt in 0..3 {
        let mut stmt = match conn.prepare(query) {
            Ok(s) => s,
            Err(LimboError::SchemaUpdated) => {
                conn.maybe_reparse_schema().unwrap();
                continue;
            }
            Err(e) => panic!("prepare failed: {e}"),
        };
        let mut rows = Vec::new();
        match stmt.run_with_row_callback(|row| {
            rows.push(row.get_values().cloned().collect::<Vec<_>>());
            Ok(())
        }) {
            Ok(()) => return rows,
            Err(LimboError::SchemaUpdated) => {
                drop(stmt);
                conn.maybe_reparse_schema().unwrap();
                continue;
            }
            Err(e) => panic!("run_with_row_callback failed: {e}"),
        }
    }
    panic!("get_rows: exhausted SchemaUpdated retries for: {query}");
}

fn get_rows_without_schema_retry(conn: &Arc<Connection>, query: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get_values().cloned().collect::<Vec<_>>());
        Ok(())
    })
    .unwrap();
    rows
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
    )
}

fn open_multiprocess_db_with_flags(
    io: Arc<dyn IO>,
    path: &str,
    flags: OpenFlags,
) -> Result<Arc<Database>> {
    Database::open_file_with_flags(io, path, flags, multiprocess_wal_db_opts(), None)
}

#[test]
fn open_db_async_state_drop_clears_opening_registry_entry() {
    let key = DatabaseKey::SharedMemory(
        "open-db-async-state-drop-clears-opening-registry-entry".to_string(),
    );
    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.clear();
        manager.insert(key.clone(), RegistryEntry::Opening);
    }

    let mut state = OpenDbAsyncState::new();
    state.registry_key = Some(key.clone());
    drop(state);

    let mut manager = DATABASE_MANAGER.lock();
    assert!(
        !manager.contains_key(&key),
        "dropping an incomplete async open must clear the Opening sentinel"
    );
    manager.clear();
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
    let db = Database::open_file(io, db_path_str).unwrap();

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

#[test]
fn database_open_without_experimental_multiprocess_wal_rejects_second_process() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-default-locked.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let _db = Database::open_file(io, db_path_str).unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let child_output = Command::new(&current_exe)
        .arg(DEFAULT_LOCKED_DB_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        child_output.status.success(),
        "second default-open child process unexpectedly succeeded: stdout={}; stderr={}",
        String::from_utf8_lossy(&child_output.stdout),
        String::from_utf8_lossy(&child_output.stderr)
    );
}

#[test]
fn database_open_with_experimental_multiprocess_wal_rejects_unsupported_io_backend() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let err = Database::open_file_with_flags(
        io,
        "unsupported-multiprocess.db",
        OpenFlags::default(),
        multiprocess_wal_db_opts(),
        None,
    )
    .expect_err("multiprocess WAL should reject IO backends without shared coordination");
    assert!(
        matches!(err, LimboError::InvalidArgument(ref message) if message.contains("active IO backend")),
        "expected InvalidArgument about unsupported IO backend, got {err:?}"
    );
}

#[test]
fn readonly_open_with_experimental_multiprocess_wal_allows_missing_coordination_file() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("readonly-multiprocess-no-tshm.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    {
        let db = Database::open_file(io.clone(), db_path_str).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
    }

    let readonly = open_multiprocess_db_with_flags(io, db_path_str, OpenFlags::ReadOnly)
        .expect("read-only open should degrade gracefully when no .tshm exists");
    assert!(readonly.shared_wal_coordination().unwrap().is_none());
}

#[test]
fn database_open_with_experimental_multiprocess_wal_rejects_second_default_process() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir
        .path()
        .join("coordination-multiprocess-parent-default-child.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let _db = open_multiprocess_db(io, db_path_str).unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let child_output = Command::new(&current_exe)
        .arg(DEFAULT_LOCKED_DB_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        child_output.status.success(),
        "default-open child process unexpectedly succeeded against multiprocess parent: stdout={}; stderr={}",
        String::from_utf8_lossy(&child_output.stdout),
        String::from_utf8_lossy(&child_output.stderr)
    );
}

#[test]
fn database_open_without_experimental_multiprocess_wal_rejects_second_multiprocess_process() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir
        .path()
        .join("coordination-default-parent-multiprocess-child.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let _db = Database::open_file(io, db_path_str).unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let child_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_EXPECT_OPEN_FAILURE_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        child_output.status.success(),
        "multiprocess child process unexpectedly opened against default parent: stdout={}; stderr={}",
        String::from_utf8_lossy(&child_output.stdout),
        String::from_utf8_lossy(&child_output.stderr)
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
    let conn_a = db_a.connect().unwrap();
    conn_a
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn_a
        .execute("insert into test(value) values ('before-stale')")
        .unwrap();

    let authority = db_a.shared_wal_coordination().unwrap().unwrap();
    let stale_snapshot = authority.snapshot();

    conn_a
        .execute("insert into test(value) values ('after-stale')")
        .unwrap();

    authority.install_snapshot(stale_snapshot);

    drop(conn_a);
    drop(db_a);
    let mut manager = DATABASE_MANAGER.lock();
    manager.clear();
    drop(manager);

    let db_b = open_multiprocess_db(io, db_path_str).unwrap();
    assert!(
        db_b.shared_wal
            .read()
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire),
        "cold reopen should fall back to a WAL disk scan when tshm snapshot is stale"
    );

    let conn_b = db_b.connect().unwrap();
    let rows = get_rows(&conn_b, "select id, value from test order by id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
    assert_eq!(rows[0][1].to_string(), "before-stale");
    assert_eq!(rows[1][0].as_int().unwrap(), 2);
    assert_eq!(rows[1][1].to_string(), "after-stale");
}

#[test]
fn database_open_reuses_trusted_tshm_snapshot_without_disk_scan_when_no_backfill_proof_is_needed() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-trusted-tail-reopen.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();

    let db = open_multiprocess_db(io.clone(), db_path_str).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn.execute("insert into test(value) values ('before-reopen')")
        .unwrap();
    conn.execute("insert into test(value) values ('after-reopen')")
        .unwrap();

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
    let mut manager = DATABASE_MANAGER.lock();
    manager.clear();
    drop(manager);

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

    let reopened_conn = reopened.connect().unwrap();
    assert_eq!(count_test_rows(&reopened_conn), 2);
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
    let conn = db.connect().unwrap();
    conn.wal_auto_checkpoint_disable();
    conn.execute("create table test(id integer primary key, value blob)")
        .unwrap();
    conn.execute("begin immediate").unwrap();
    for _ in 0..32 {
        conn.execute("insert into test(value) values (randomblob(2048))")
            .unwrap();
    }
    conn.execute("commit").unwrap();
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
    let mut manager = DATABASE_MANAGER.lock();
    manager.clear();
    drop(manager);

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

    let reopened_conn = reopened.connect().unwrap();
    assert_eq!(
        count_test_rows(&reopened_conn),
        32,
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
    let conn = db.connect().unwrap();
    conn.wal_auto_checkpoint_disable();
    conn.execute("create table test(id integer primary key, value blob)")
        .unwrap();
    conn.execute("begin immediate").unwrap();
    for _ in 0..32 {
        conn.execute("insert into test(value) values (randomblob(2048))")
            .unwrap();
    }
    conn.execute("commit").unwrap();

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

    conn.execute("insert into test(value) values (randomblob(2048))")
        .unwrap();
    let snapshot_after_append = authority.snapshot();
    assert!(
        snapshot_after_append.max_frame > snapshot_after_checkpoint.max_frame,
        "stale-proof coverage requires a WAL append after proof installation"
    );

    drop(conn);
    drop(db);
    let mut manager = DATABASE_MANAGER.lock();
    manager.clear();
    drop(manager);

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

    let reopened_conn = reopened.connect().unwrap();
    assert_eq!(
        count_test_rows(&reopened_conn),
        33,
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
    let conn = db.connect().unwrap();
    conn.wal_auto_checkpoint_disable();
    conn.execute("create table test(id integer primary key, value blob)")
        .unwrap();
    conn.execute("begin immediate").unwrap();
    for _ in 0..32 {
        conn.execute("insert into test(value) values (randomblob(2048))")
            .unwrap();
    }
    conn.execute("commit").unwrap();

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
    let mut manager = DATABASE_MANAGER.lock();
    manager.clear();
    drop(manager);

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

    let reopened_conn = reopened.connect().unwrap();
    assert_eq!(
        count_test_rows(&reopened_conn),
        32,
        "DB-header-mismatch reopen should preserve committed rows after the conservative disk scan"
    );
}

#[test]
fn default_locked_db_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = multiprocess_test_io();
    let err = Database::open_file(io, db_path.to_str().unwrap())
        .expect_err("default non-multiprocess open should stay DB-file locked across processes");
    assert!(
        matches!(err, LimboError::LockingError(_)),
        "expected LockingError from second default open, got {err:?}"
    );
}

#[test]
fn multiprocess_shm_expect_open_failure_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = multiprocess_test_io();
    let err = open_multiprocess_db(io, db_path.to_str().unwrap())
        .expect_err("multiprocess open should fail when a legacy opener already owns the DB");
    assert!(
        matches!(err, LimboError::LockingError(_)),
        "expected LockingError from incompatible multiprocess open, got {err:?}"
    );
}

#[test]
fn multiprocess_shm_insert_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    let last_checksum_and_max_frame = db.shared_wal.read().last_checksum_and_max_frame();
    let wal = db
        .build_wal(last_checksum_and_max_frame, db.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "tshm");
    assert_eq!(wal_file.coordination_open_mode_name(), Some("multiprocess"));

    let conn = db.connect().unwrap();
    conn.execute("insert into test(value) values ('child')")
        .unwrap();
}

#[test]
fn multiprocess_shm_insert_and_close_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    let last_checksum_and_max_frame = db.shared_wal.read().last_checksum_and_max_frame();
    let wal = db
        .build_wal(last_checksum_and_max_frame, db.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "tshm");
    assert_eq!(wal_file.coordination_open_mode_name(), Some("multiprocess"));

    let conn = db.connect().unwrap();
    conn.execute("insert into test(value) values ('child-close')")
        .unwrap();
    conn.close().unwrap();
}

#[test]
fn multiprocess_shm_count_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };
    let expected_count = std::env::var("TURSO_MULTIPROCESS_EXPECTED_COUNT")
        .unwrap()
        .parse::<i64>()
        .unwrap();

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    let last_checksum_and_max_frame = db.shared_wal.read().last_checksum_and_max_frame();
    let wal = db
        .build_wal(last_checksum_and_max_frame, db.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "tshm");
    assert_eq!(wal_file.coordination_open_mode_name(), Some("multiprocess"));

    let conn = db.connect().unwrap();
    assert_eq!(count_test_rows(&conn), expected_count);
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

    let conn = db.connect().unwrap();
    let pager = conn.pager.load();
    let wal = pager.wal.as_ref().unwrap();
    wal.begin_read_tx().unwrap();
    std::fs::write(&ready_file, b"ready").unwrap();
    wait_for_file(&release_file);
    wal.end_read_tx();
}

#[test]
fn multiprocess_shm_schema_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    let last_checksum_and_max_frame = db.shared_wal.read().last_checksum_and_max_frame();
    let wal = db
        .build_wal(last_checksum_and_max_frame, db.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "tshm");
    assert_eq!(wal_file.coordination_open_mode_name(), Some("multiprocess"));

    let conn = db.connect().unwrap();
    conn.execute("create table child_table(id integer primary key, value text)")
        .unwrap();
    conn.execute("insert into child_table(value) values ('child-schema')")
        .unwrap();
}

#[test]
fn subprocess_database_open_selects_multiprocess_shm_backend() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-subprocess.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let last_checksum_and_max_frame = db.shared_wal.read().last_checksum_and_max_frame();
    let wal = db
        .build_wal(last_checksum_and_max_frame, db.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "tshm");
    assert_eq!(wal_file.coordination_open_mode_name(), Some("exclusive"));

    let conn = db.connect().unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn.execute("insert into test(value) values ('parent')")
        .unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let insert_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_INSERT_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        insert_output.status.success(),
        "child process failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&insert_output.stdout),
        String::from_utf8_lossy(&insert_output.stderr)
    );

    let count_output = Command::new(current_exe)
        .arg(MULTIPROCESS_SHM_COUNT_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .env("TURSO_MULTIPROCESS_EXPECTED_COUNT", "2")
        .output()
        .unwrap();
    assert!(
        count_output.status.success(),
        "count child process failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&count_output.stdout),
        String::from_utf8_lossy(&count_output.stderr)
    );
}

#[test]
fn plain_vacuum_rejects_multiprocess_wal_database() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("vacuum-multiprocess.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn.execute("insert into test(value) values ('parent')")
        .unwrap();

    let err = conn
        .execute("VACUUM")
        .expect_err("VACUUM should reject on a multiprocess-WAL database");
    assert!(
        matches!(err, LimboError::ParseError(ref msg) if msg.contains("experimental multiprocess WAL")),
        "expected explicit multiprocess VACUUM rejection, got {err:?}"
    );
    assert_eq!(
        count_test_rows(&conn),
        1,
        "rejecting VACUUM on a multiprocess-WAL database must not disturb the existing connection"
    );
}

#[test]
fn subprocess_child_close_skips_shutdown_checkpoint_in_multiprocess_wal_mode() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("close-skip-shutdown-checkpoint.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db.connect().unwrap();

    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    run_checkpoint(
        &conn,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    let current_exe = std::env::current_exe().unwrap();
    let child_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_INSERT_AND_CLOSE_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        child_output.status.success(),
        "child process failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&child_output.stdout),
        String::from_utf8_lossy(&child_output.stderr)
    );

    let wal_path = format!("{db_path_str}-wal");
    let wal_len = std::fs::metadata(&wal_path)
        .unwrap_or_else(|_| panic!("expected WAL file at {wal_path}"))
        .len();
    assert!(
        wal_len > 0,
        "child close should not run shutdown checkpoint while another process still has the DB open"
    );

    assert_eq!(
        count_test_rows(&conn),
        1,
        "parent connection should still see the child commit via WAL"
    );
}

#[test]
fn subprocess_database_open_survives_truncate_rewrite_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-truncate-rewrite.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn.execute("insert into test(value) values ('parent-0')")
        .unwrap();

    let current_exe = std::env::current_exe().unwrap();
    for expected_count in [2_i64, 3_i64] {
        let insert_output = Command::new(&current_exe)
            .arg(MULTIPROCESS_SHM_INSERT_CHILD_TEST)
            .arg("--exact")
            .arg("--nocapture")
            .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
            .output()
            .unwrap();
        assert!(
            insert_output.status.success(),
            "child insert failed: stdout={}; stderr={}",
            String::from_utf8_lossy(&insert_output.stdout),
            String::from_utf8_lossy(&insert_output.stderr)
        );
        assert_eq!(count_test_rows(&conn), expected_count);

        let checkpoint = run_checkpoint(
            &conn,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );
        assert!(
            checkpoint.everything_backfilled(),
            "truncate checkpoint should fully backfill before WAL rewrite"
        );

        let count_output = Command::new(&current_exe)
            .arg(MULTIPROCESS_SHM_COUNT_CHILD_TEST)
            .arg("--exact")
            .arg("--nocapture")
            .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
            .env(
                "TURSO_MULTIPROCESS_EXPECTED_COUNT",
                expected_count.to_string(),
            )
            .output()
            .unwrap();
        assert!(
            count_output.status.success(),
            "child count after truncate failed: stdout={}; stderr={}",
            String::from_utf8_lossy(&count_output.stdout),
            String::from_utf8_lossy(&count_output.stderr)
        );
    }

    assert_eq!(count_test_rows(&conn), 3);
}

#[test]
fn subprocess_database_open_peer_refreshes_remote_schema_without_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-peer-schema-refresh.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("create table t(value integer, next_value integer)")
        .unwrap();

    let series_rows = get_rows(&conn, "select value from generate_series(1, 3)");
    assert_eq!(series_rows.len(), 3);
    assert_eq!(series_rows[0][0].to_string(), "1");
    assert_eq!(series_rows[1][0].to_string(), "2");
    assert_eq!(series_rows[2][0].to_string(), "3");

    let initial_schema_rows = get_rows(
        &conn,
        "select name, type from sqlite_schema where name = 'child_table'",
    );
    assert!(
        initial_schema_rows.is_empty(),
        "fresh parent connection should not see child_table before child creates it"
    );

    let current_exe = std::env::current_exe().unwrap();
    let schema_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_SCHEMA_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        schema_output.status.success(),
        "child schema process failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&schema_output.stdout),
        String::from_utf8_lossy(&schema_output.stderr)
    );

    let schema_rows = get_rows_without_schema_retry(
        &conn,
        "select name, type from sqlite_schema where name = 'child_table'",
    );
    assert_eq!(schema_rows.len(), 1);
    assert_eq!(schema_rows[0][0].to_string(), "child_table");
    assert_eq!(schema_rows[0][1].to_string(), "table");

    let child_rows = get_rows_without_schema_retry(&conn, "select value from child_table");
    assert_eq!(child_rows.len(), 1);
    assert_eq!(child_rows[0][0].to_string(), "child-schema");

    conn.execute("insert into t select value, value + 1 from generate_series(1, 3)")
        .unwrap();
    let inserted_rows = get_rows(&conn, "select value, next_value from t order by value");
    assert_eq!(inserted_rows.len(), 3);
    assert_eq!(inserted_rows[0][0].to_string(), "1");
    assert_eq!(inserted_rows[0][1].to_string(), "2");
    assert_eq!(inserted_rows[2][0].to_string(), "3");
    assert_eq!(inserted_rows[2][1].to_string(), "4");
}

#[test]
fn subprocess_database_open_parent_directly_uses_child_created_table() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-direct-child-table-use.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("create table t(value integer)").unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let schema_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_SCHEMA_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        schema_output.status.success(),
        "child schema process failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&schema_output.stdout),
        String::from_utf8_lossy(&schema_output.stderr)
    );

    let mut stmt = conn
        .prepare("insert into child_table(value) values ('parent-schema')")
        .unwrap();
    stmt.run_ignore_rows().unwrap();

    let child_rows =
        get_rows_without_schema_retry(&conn, "select value from child_table order by rowid");
    assert_eq!(child_rows.len(), 2);
    assert_eq!(child_rows[0][0].to_string(), "child-schema");
    assert_eq!(child_rows[1][0].to_string(), "parent-schema");
}

#[test]
fn subprocess_database_open_parent_execute_uses_child_created_table() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-execute-child-table-use.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("create table t(value integer)").unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let schema_output = Command::new(&current_exe)
        .arg(MULTIPROCESS_SHM_SCHEMA_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .output()
        .unwrap();
    assert!(
        schema_output.status.success(),
        "child schema process failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&schema_output.stdout),
        String::from_utf8_lossy(&schema_output.stderr)
    );

    conn.execute("insert into child_table(value) values ('parent-schema')")
        .unwrap();

    let child_rows =
        get_rows_without_schema_retry(&conn, "select value from child_table order by rowid");
    assert_eq!(child_rows.len(), 2);
    assert_eq!(child_rows[0][0].to_string(), "child-schema");
    assert_eq!(child_rows[1][0].to_string(), "parent-schema");
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
    let conn = db.connect().unwrap();
    conn.wal_auto_checkpoint_disable();
    conn.execute("create table test(id integer primary key, value blob)")
        .unwrap();
    conn.execute("begin immediate").unwrap();
    for _ in 0..32 {
        conn.execute("insert into test(value) values (randomblob(2048))")
            .unwrap();
    }
    conn.execute("commit").unwrap();
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
    let conn = db.connect().unwrap();
    conn.wal_auto_checkpoint_disable();
    conn.execute("create table test(id integer primary key, value blob)")
        .unwrap();
    conn.execute("begin immediate").unwrap();
    for _ in 0..32 {
        conn.execute("insert into test(value) values (randomblob(2048))")
            .unwrap();
    }
    conn.execute("commit").unwrap();
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
    let mut manager = DATABASE_MANAGER.lock();
    manager.clear();
    drop(manager);

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
    let reopened_conn = reopened.connect().unwrap();

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
    let mut manager = DATABASE_MANAGER.lock();
    manager.clear();
    drop(manager);

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
    let reopened_conn = reopened.connect().unwrap();
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
        count_test_rows(&reopened_conn),
        32,
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
    let conn = db.connect().unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn.execute("insert into test(value) values ('before-reader')")
        .unwrap();

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

    conn.execute("insert into test(value) values ('after-reader')")
        .unwrap();
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
    let mut manager = DATABASE_MANAGER.lock();
    manager.clear();
    drop(manager);

    let io: Arc<dyn IO> = multiprocess_test_io();
    let reopened = open_multiprocess_db(io, db_path_str).unwrap();
    let reopened_conn = reopened.connect().unwrap();
    assert_eq!(
        count_test_rows(&reopened_conn),
        2,
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
    let conn = db.connect().unwrap();
    conn.wal_auto_checkpoint_disable();
    conn.execute("create table test(id integer primary key, value blob)")
        .unwrap();
    conn.execute("begin immediate").unwrap();
    for _ in 0..32 {
        conn.execute("insert into test(value) values (randomblob(2048))")
            .unwrap();
    }
    conn.execute("commit").unwrap();
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
    let mut manager = DATABASE_MANAGER.lock();
    manager.clear();
    drop(manager);

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

    let reopened_conn = reopened.connect().unwrap();
    assert_eq!(
        count_test_rows(&reopened_conn),
        32,
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
    let conn_a = db_a.connect().unwrap();
    conn_a
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn_a
        .execute("insert into test(value) values ('before-overflow')")
        .unwrap();
    conn_a
        .execute("insert into test(value) values ('after-overflow')")
        .unwrap();

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
    let mut manager = DATABASE_MANAGER.lock();
    manager.clear();
    drop(manager);

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

    let conn_b = db_b.connect().unwrap();
    let rows = get_rows(&conn_b, "select value from test order by id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].to_string(), "before-overflow");
    assert_eq!(rows[1][0].to_string(), "after-overflow");
}

#[test]
fn memory_database_keeps_in_process_wal_backend() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:").unwrap();

    let last_checksum_and_max_frame = db.shared_wal.read().last_checksum_and_max_frame();
    let wal = db
        .build_wal(last_checksum_and_max_frame, db.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "in_process");
}

#[test]
fn memory_database_query_can_close_without_checkpointing() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:").unwrap();
    let conn = db.connect().unwrap();

    conn.query("VALUES ('ok')").unwrap();
    conn.close().unwrap();
}
