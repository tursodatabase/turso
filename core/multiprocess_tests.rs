use super::*;
use crate::database::remove_file_from_database_registry;
use crate::dialect::cannot_parse::DialectThatCannotParse;
use crate::security::roles::SessionRole;
use crate::storage::pager::test_writes::{commit_new_pages, commit_user_version};
use crate::storage::shared_wal_coordination::{
    MappedSharedWalCoordination, SharedWalCoordinationOpenMode,
};
use crate::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::process::Command;

#[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
use crate::WindowsIOCP;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

const MULTIPROCESS_SHM_HOLD_READ_TX_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_shm_hold_read_tx_child_process";
const MULTIPROCESS_HOLD_OPEN_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_hold_open_child_process";
const MULTIPROCESS_DB_FILE_LOCK_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_db_file_lock_child_process";
const DISABLED_FILE_LOCK_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_open_with_disabled_file_lock_child_process";
const MULTIPROCESS_PAUSED_CREATOR_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_paused_creator_child_process";
const MULTIPROCESS_FIRST_OPENER_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_first_opener_child_process";
const MULTIPROCESS_NEW_DB_CREATOR_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_new_db_creator_child_process";

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

#[test]
fn multiprocess_open_reinitializes_unpublished_coordination_file() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("unpublished-header.db");
    let db_path_str = db_path.to_str().unwrap();
    let shm_path = storage::wal::coordination_path_for_wal_path(&format!("{db_path_str}-wal"));
    // A creator that died after sizing the file but before publishing the header.
    std::fs::File::create(&shm_path)
        .unwrap()
        .set_len(1 << 16)
        .unwrap();

    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    commit_new_pages(&conn, 1);
}

/// Where a paused creator of the coordination file stalls.
#[derive(Clone, Copy)]
enum CreatorPause {
    /// After probing the lifetime byte, before taking it: the creator has not
    /// yet been seen by any other process.
    AfterProbe,
    /// File sized, lifetime lock held, header not published.
    BeforeInitialize,
}

/// Start a child that stalls as the creator of the coordination file of
/// `db_path` at `pause` and wait until it has. It finishes creating the file
/// once `release_file` exists.
fn spawn_paused_creator(
    db_path: &std::path::Path,
    ready_file: &std::path::Path,
    release_file: &std::path::Path,
    pause: CreatorPause,
) -> std::process::Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg(MULTIPROCESS_PAUSED_CREATOR_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path)
        .env("TURSO_MULTIPROCESS_READY_FILE", ready_file)
        .env("TURSO_MULTIPROCESS_RELEASE_FILE", release_file);
    if matches!(pause, CreatorPause::AfterProbe) {
        command.env("TURSO_MULTIPROCESS_PAUSE_AFTER_PROBE", "1");
    }
    let child = command.spawn().unwrap();
    wait_for_file(ready_file);
    child
}

/// Child half of the creator-in-flight tests.
#[test]
fn multiprocess_paused_creator_child_process() {
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
    let wal_path = format!("{}-wal", db_path.to_str().unwrap());
    let shm_path = storage::wal::coordination_path_for_wal_path(&wal_path);
    let pause = || {
        std::fs::write(&ready_file, b"ready").unwrap();
        wait_for_file(&release_file);
    };
    let noop = || {};
    let pause_fn: &dyn Fn() = &pause;
    let noop_fn: &dyn Fn() = &noop;
    let (after_probe, before_initialize) =
        if std::env::var_os("TURSO_MULTIPROCESS_PAUSE_AFTER_PROBE").is_some() {
            (pause_fn, noop_fn)
        } else {
            (noop_fn, pause_fn)
        };
    let _authority = MappedSharedWalCoordination::create_or_open_paused_for_tests(
        &io,
        std::path::Path::new(&shm_path),
        64,
        after_probe,
        before_initialize,
    )
    .unwrap();
}

/// Open an existing database with `flags` while a child process is paused at
/// `pause` as the creator of its coordination file, and return the open's
/// result.
///
/// The creator stays paused until the open has had ample time to fail, so the
/// open cannot finish before the release unless it fails: an opener without
/// the wait returns its error immediately.
fn open_while_creator_paused(
    name: &str,
    flags: OpenFlags,
    pause: CreatorPause,
) -> std::thread::Result<Result<(bool, i64)>> {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join(name);
    let db_path_str = db_path.to_str().unwrap().to_string();
    let ready_file = dir.path().join("creator-ready");
    let release_file = dir.path().join("creator-release");
    let io: Arc<dyn IO> = multiprocess_test_io();

    {
        let db = Database::open_file(io.clone(), &db_path_str, Arc::new(EngineDialect)).unwrap();
        let conn = db.connect(ConnectOpts::new()).unwrap();
        conn.execute("create table test(id integer primary key, value text)")
            .unwrap();
        conn.execute("insert into test(value) values ('seed')")
            .unwrap();
    }
    remove_file_from_database_registry(&db_path_str);

    let mut creator = spawn_paused_creator(&db_path, &ready_file, &release_file, pause);
    let opener = std::thread::spawn(move || -> Result<(bool, i64)> {
        let db = open_multiprocess_db_with_flags(io, &db_path_str, flags)?;
        let attached = db.shared_wal_coordination()?.is_some();
        let conn = db.connect(ConnectOpts::new())?;
        Ok((attached, count_test_rows(&conn)))
    });
    std::thread::sleep(Duration::from_millis(200));
    let finished_early = opener.is_finished();
    std::fs::write(&release_file, b"release").unwrap();
    assert!(
        !finished_early,
        "open must wait for the creator that holds the init lock"
    );
    let result = opener.join();
    assert!(creator.wait().unwrap().success());
    result
}

#[test]
fn readonly_multiprocess_open_waits_for_concurrent_creator() {
    let (attached, rows) = open_while_creator_paused(
        "readonly-waits-for-creator.db",
        OpenFlags::ReadOnly,
        CreatorPause::BeforeInitialize,
    )
    .unwrap()
    .unwrap();
    assert!(
        attached,
        "reader must attach to the published coordination file"
    );
    assert_eq!(rows, 1);
}

#[test]
fn writable_multiprocess_open_waits_for_concurrent_creator() {
    let (attached, rows) = open_while_creator_paused(
        "writable-waits-for-creator.db",
        OpenFlags::default(),
        CreatorPause::BeforeInitialize,
    )
    .unwrap()
    .unwrap();
    assert!(
        attached,
        "opener must attach to the published coordination file"
    );
    assert_eq!(rows, 1);
}

/// A read-only open must wait for a creator that has probed the lifetime byte
/// but not yet taken it. Without the init lock the reader probes the byte as
/// free in that window, finds the file still short and reports it as corrupt
/// because it sees no live creator.
#[test]
fn readonly_multiprocess_open_waits_for_creator_that_has_not_locked_yet() {
    let (attached, rows) = open_while_creator_paused(
        "readonly-waits-for-unlocked-creator.db",
        OpenFlags::ReadOnly,
        CreatorPause::AfterProbe,
    )
    .unwrap()
    .unwrap();
    assert!(
        attached,
        "reader must attach to the published coordination file"
    );
    assert_eq!(rows, 1);
}

/// A writable open must not decide the open mode while a creator is between
/// probing the lifetime byte and taking it. Without the init lock it probes
/// the byte as free too, decides Exclusive and initializes the file under
/// the creator.
#[test]
fn writable_multiprocess_open_waits_for_creator_that_has_not_locked_yet() {
    let (attached, rows) = open_while_creator_paused(
        "writable-waits-for-unlocked-creator.db",
        OpenFlags::default(),
        CreatorPause::AfterProbe,
    )
    .unwrap()
    .unwrap();
    assert!(
        attached,
        "opener must attach to the published coordination file"
    );
    assert_eq!(rows, 1);
}

/// A child that opens the coordination file as one of two concurrent first
/// openers, paused after probing the lifetime byte until released, then records
/// one commit in it and holds the file open until told to exit.
struct FirstOpener {
    child: std::process::Child,
    probe_ready_file: std::path::PathBuf,
    probe_release_file: std::path::PathBuf,
    opened_file: std::path::PathBuf,
    done_file: std::path::PathBuf,
}

impl FirstOpener {
    fn spawn(db_path: &std::path::Path, dir: &std::path::Path, role: &str) -> Self {
        let probe_ready_file = dir.join(format!("{role}-probe-ready"));
        let probe_release_file = dir.join(format!("{role}-probe-release"));
        let opened_file = dir.join(format!("{role}-opened"));
        let done_file = dir.join(format!("{role}-done"));
        let child = Command::new(std::env::current_exe().unwrap())
            .arg(MULTIPROCESS_FIRST_OPENER_CHILD_TEST)
            .arg("--exact")
            .arg("--nocapture")
            .env("TURSO_MULTIPROCESS_DB_PATH", db_path)
            .env("TURSO_MULTIPROCESS_OPENER_ROLE", role)
            .env("TURSO_MULTIPROCESS_PROBE_READY_FILE", &probe_ready_file)
            .env("TURSO_MULTIPROCESS_PROBE_RELEASE_FILE", &probe_release_file)
            .env("TURSO_MULTIPROCESS_OPENED_FILE", &opened_file)
            .env("TURSO_MULTIPROCESS_DONE_FILE", &done_file)
            .spawn()
            .unwrap();
        Self {
            child,
            probe_ready_file,
            probe_release_file,
            opened_file,
            done_file,
        }
    }

    fn release_probe(&self) {
        std::fs::write(&self.probe_release_file, b"release").unwrap();
    }
}

impl Drop for FirstOpener {
    /// Unblock and reap the child so a failed assertion leaves none behind.
    fn drop(&mut self) {
        let _ = std::fs::write(&self.probe_release_file, b"release");
        let _ = std::fs::write(&self.done_file, b"done");
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Two flagged first openers must not both initialize the coordination file.
/// The first is paused after probing the lifetime byte as free, before taking
/// it. The second must not get as far as probing: without the init lock it
/// probes the byte as free too, and both can decide Exclusive and initialize
/// the file, the later one resetting state the earlier one already uses. With
/// it the second waits, then opens in MultiProcess mode over the file the
/// first initialized, and both commits are visible afterwards.
#[test]
fn concurrent_first_openers_initialize_the_coordination_file_once() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("first-openers.db");
    let shm_path =
        storage::wal::coordination_path_for_wal_path(&format!("{}-wal", db_path.to_str().unwrap()));

    let mut first = FirstOpener::spawn(&db_path, dir.path(), "first");
    wait_for_file(&first.probe_ready_file);
    let mut second = FirstOpener::spawn(&db_path, dir.path(), "second");
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        !second.probe_ready_file.exists(),
        "the second opener probed the lifetime byte while the first was still deciding the open mode"
    );

    first.release_probe();
    wait_for_file(&first.opened_file);
    wait_for_file(&second.probe_ready_file);
    second.release_probe();
    wait_for_file(&second.opened_file);

    let io: Arc<dyn IO> = multiprocess_test_io();
    let observer =
        MappedSharedWalCoordination::open_existing(&io, std::path::Path::new(&shm_path), 64)
            .unwrap()
            .expect("coordination file exists");
    let snapshot = observer.snapshot();
    assert_eq!(snapshot.max_frame, 2);
    assert_eq!(snapshot.transaction_count, 2);
    assert_eq!(observer.find_frame(7, 0, 2, None), Some(1));
    assert_eq!(observer.find_frame(9, 0, 2, None), Some(2));
    drop(observer);

    std::fs::write(&first.done_file, b"done").unwrap();
    std::fs::write(&second.done_file, b"done").unwrap();
    assert!(first.child.wait().unwrap().success());
    assert!(second.child.wait().unwrap().success());
}

/// Child half of `concurrent_first_openers_initialize_the_coordination_file_once`.
#[test]
fn multiprocess_first_opener_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };
    let env_path = |name: &str| std::path::PathBuf::from(std::env::var_os(name).unwrap());
    let role = std::env::var("TURSO_MULTIPROCESS_OPENER_ROLE").unwrap();
    let probe_ready_file = env_path("TURSO_MULTIPROCESS_PROBE_READY_FILE");
    let probe_release_file = env_path("TURSO_MULTIPROCESS_PROBE_RELEASE_FILE");
    let opened_file = env_path("TURSO_MULTIPROCESS_OPENED_FILE");
    let done_file = env_path("TURSO_MULTIPROCESS_DONE_FILE");

    let io: Arc<dyn IO> = multiprocess_test_io();
    let wal_path = format!("{}-wal", db_path.to_str().unwrap());
    let shm_path = storage::wal::coordination_path_for_wal_path(&wal_path);
    let authority = MappedSharedWalCoordination::create_or_open_paused_for_tests(
        &io,
        std::path::Path::new(&shm_path),
        64,
        &|| {
            std::fs::write(&probe_ready_file, b"ready").unwrap();
            wait_for_file(&probe_release_file);
        },
        &|| {},
    )
    .unwrap();

    let (page_id, frame_id) = if role == "first" {
        assert_eq!(
            authority.open_mode(),
            SharedWalCoordinationOpenMode::Exclusive
        );
        (7, 1)
    } else {
        assert_eq!(
            authority.open_mode(),
            SharedWalCoordinationOpenMode::MultiProcess
        );
        // The first opener's commit must have survived this open.
        assert_eq!(authority.snapshot().max_frame, 1);
        assert_eq!(authority.find_frame(7, 0, 1, None), Some(1));
        (9, 2)
    };
    let owner = authority.owner_record();
    assert!(authority.try_acquire_writer(owner));
    authority.record_frame(page_id, frame_id);
    authority.publish_commit(frame_id, 0, 0, frame_id);
    authority.release_writer(owner);
    std::fs::write(&opened_file, b"opened").unwrap();
    wait_for_file(&done_file);
}

/// A process that opened a new database and has not written yet must notice
/// that another process created it. Its in-memory page 1 describes an empty
/// database; building the first write on it would write a second page 1 and a
/// second sqlite_schema over the other process's committed table.
#[test]
fn process_holding_an_empty_database_adopts_one_created_by_another_process() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("created-by-other-process.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path_str).unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();

    let child_output = Command::new(std::env::current_exe().unwrap())
        .arg(MULTIPROCESS_NEW_DB_CREATOR_CHILD_TEST)
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

    // Prepared while this process still believes the database is empty.
    conn.execute("create table if not exists t(x)").unwrap();
    conn.execute("insert into t values (2)").unwrap();

    let xs = |conn: &Arc<Connection>| {
        get_rows(conn, "select x from t order by x")
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>()
    };
    let expected = vec![Value::from_i64(1), Value::from_i64(2)];
    assert_eq!(xs(&conn), expected);
    let fresh = db.connect(ConnectOpts::new()).unwrap();
    assert_eq!(xs(&fresh), expected);
}

/// Child half of `process_holding_an_empty_database_adopts_one_created_by_another_process`.
#[test]
fn multiprocess_new_db_creator_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };
    let io: Arc<dyn IO> = multiprocess_test_io();
    let db = open_multiprocess_db(io, db_path.to_str().unwrap()).unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("create table t(x)").unwrap();
    conn.execute("insert into t values (1)").unwrap();
    conn.close().unwrap();
}

/// A read-only open never creates or initializes the coordination file. A
/// writer creates it empty before it takes the init lock, so a short file with
/// no live creator is a creator that has not started yet (or died): the open
/// reports retryable `Busy`, not `Corrupt`, and leaves the file alone.
#[test]
fn readonly_open_reports_an_empty_coordination_file_as_busy() {
    let dir = tempfile::tempdir().unwrap();
    let shm_path = dir.path().join("short.db-tshm");
    std::fs::File::create(&shm_path).unwrap();

    let io: Arc<dyn IO> = multiprocess_test_io();
    let err = MappedSharedWalCoordination::open_existing(&io, &shm_path, 64).err();
    assert!(
        matches!(err, Some(LimboError::Busy)),
        "empty file without a creator: {err:?}"
    );
    assert_eq!(std::fs::metadata(&shm_path).unwrap().len(), 0);
}

/// Same for a file that was sized but whose header was never published, such
/// as one left by a creator that died: a writable opener re-initializes it, a
/// read-only opener reports `Busy` and does not touch it.
#[test]
fn readonly_open_reports_an_unpublished_coordination_file_as_busy() {
    let dir = tempfile::tempdir().unwrap();
    let shm_path = dir.path().join("unpublished.db-tshm");
    // Larger than the header for 64 reader slots, all zero: no magic.
    let len = 1 << 20;
    std::fs::File::create(&shm_path)
        .unwrap()
        .set_len(len)
        .unwrap();

    let io: Arc<dyn IO> = multiprocess_test_io();
    let err = MappedSharedWalCoordination::open_existing(&io, &shm_path, 64).err();
    assert!(
        matches!(err, Some(LimboError::Busy)),
        "unpublished file without a creator: {err:?}"
    );
    assert_eq!(std::fs::metadata(&shm_path).unwrap().len(), len);
}

/// A flagged open must not be rejected as a legacy open while another process
/// holds a shared lock on the database file, as another flagged opener does
/// while it probes.
#[test]
fn multiprocess_open_succeeds_while_another_process_holds_shared_db_lock() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("shared-db-lock.db");
    let ready_file = dir.path().join("lock-ready");
    let release_file = dir.path().join("lock-release");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg(MULTIPROCESS_DB_FILE_LOCK_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", &db_path)
        .env("TURSO_MULTIPROCESS_READY_FILE", &ready_file)
        .env("TURSO_MULTIPROCESS_RELEASE_FILE", &release_file)
        .spawn()
        .unwrap();
    wait_for_file(&ready_file);

    let io: Arc<dyn IO> = multiprocess_test_io();
    let opened = open_multiprocess_db(io, db_path.to_str().unwrap()).map(drop);
    std::fs::write(&release_file, b"release").unwrap();
    opened.expect("flagged open must coexist with a shared lock on the database file");
    assert!(child.wait().unwrap().success());
}

/// Child half of `multiprocess_open_succeeds_while_another_process_holds_shared_db_lock`.
#[test]
fn multiprocess_db_file_lock_child_process() {
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
    let file = io
        .open_file(
            db_path.to_str().unwrap(),
            OpenFlags::Create | OpenFlags::NoLock,
            true,
        )
        .unwrap();
    file.lock_file(false).unwrap();
    std::fs::write(&ready_file, b"ready").unwrap();
    wait_for_file(&release_file);
}

/// With `LIMBO_DISABLE_FILE_LOCK` set, a flagged open must not probe the database
/// file lock, as `open_file` takes none.
#[test]
fn multiprocess_open_ignores_legacy_lock_when_file_locks_are_disabled() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("disabled-file-lock.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = multiprocess_test_io();
    let _db = Database::open_file(io, db_path_str, Arc::new(EngineDialect)).unwrap();

    let current_exe = std::env::current_exe().unwrap();
    let child_output = Command::new(&current_exe)
        .arg(DISABLED_FILE_LOCK_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_MULTIPROCESS_DB_PATH", db_path_str)
        .env(crate::io::common::ENV_DISABLE_FILE_LOCK, "1")
        .output()
        .unwrap();
    assert!(
        child_output.status.success(),
        "flagged child open unexpectedly failed with file locks disabled: stdout={}; stderr={}",
        String::from_utf8_lossy(&child_output.stdout),
        String::from_utf8_lossy(&child_output.stderr)
    );
}

/// Child half of `multiprocess_open_ignores_legacy_lock_when_file_locks_are_disabled`.
#[test]
fn multiprocess_open_with_disabled_file_lock_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = multiprocess_test_io();
    open_multiprocess_db(io, db_path.to_str().unwrap())
        .expect("flagged open must not probe the database file lock when file locks are disabled");
}

/// The real IO, except that every `sleep` moves the monotonic clock forward by
/// an hour instead of blocking, so bounded waits expire at once.
struct FastForwardIO {
    inner: Arc<dyn IO>,
    skipped_nanos: AtomicU64,
    sleeps: AtomicUsize,
}

impl Clock for FastForwardIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
            + Duration::from_nanos(self.skipped_nanos.load(Ordering::Acquire))
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for FastForwardIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        self.inner.open_file(path, flags, direct)
    }

    fn open_shared_wal_file(&self, path: &str) -> Result<Arc<dyn File>> {
        self.inner.open_shared_wal_file(path)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        self.inner.supports_shared_wal_coordination()
    }

    fn sleep(&self, _duration: Duration) {
        self.sleeps.fetch_add(1, Ordering::AcqRel);
        self.skipped_nanos.fetch_add(
            Duration::from_secs(3600).as_nanos() as u64,
            Ordering::AcqRel,
        );
    }
}

/// An opener that finds a creator stalled past the bounded wait gets the
/// retryable `Busy`, not `Corrupt`, for the writable and the read-only path,
/// and the wait runs on the IO's clock and sleep. A retry succeeds once the
/// creator finishes.
#[test]
fn stalled_creator_makes_openers_return_busy() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("stalled-creator.db");
    let wal_path = format!("{}-wal", db_path.to_str().unwrap());
    let shm_path =
        std::path::PathBuf::from(storage::wal::coordination_path_for_wal_path(&wal_path));
    let ready_file = dir.path().join("creator-ready");
    let release_file = dir.path().join("creator-release");
    let mut creator = spawn_paused_creator(
        &db_path,
        &ready_file,
        &release_file,
        CreatorPause::BeforeInitialize,
    );

    let fast_forward = Arc::new(FastForwardIO {
        inner: multiprocess_test_io(),
        skipped_nanos: AtomicU64::new(0),
        sleeps: AtomicUsize::new(0),
    });
    let io: Arc<dyn IO> = fast_forward.clone();
    let writable_err = MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64).err();
    let readonly_err = MappedSharedWalCoordination::open_existing(&io, &shm_path, 64).err();
    std::fs::write(&release_file, b"release").unwrap();
    assert!(
        matches!(writable_err, Some(LimboError::Busy)),
        "writable open: {writable_err:?}"
    );
    assert!(
        matches!(readonly_err, Some(LimboError::Busy)),
        "read-only open: {readonly_err:?}"
    );
    assert!(
        fast_forward.sleeps.load(Ordering::Acquire) >= 2,
        "each open must wait through the IO's sleep"
    );

    assert!(creator.wait().unwrap().success());
    let io = multiprocess_test_io();
    MappedSharedWalCoordination::create_or_open(&io, &shm_path, 64)
        .map(drop)
        .expect("a retry after the creator finished must succeed");
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
