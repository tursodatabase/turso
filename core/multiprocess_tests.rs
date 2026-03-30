use super::*;
#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
use std::os::unix::process::ExitStatusExt;
use std::process::Command;

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
const MULTIPROCESS_SHM_INSERT_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_shm_insert_child_process";
#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
const MULTIPROCESS_SHM_COUNT_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_shm_count_child_process";
#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
const MULTIPROCESS_SHM_HOLD_READ_TX_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_shm_hold_read_tx_child_process";
#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
const MULTIPROCESS_SHM_SCHEMA_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_shm_schema_child_process";
#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
const MULTIPROCESS_SHM_INSERT_AND_CLOSE_CHILD_TEST: &str =
    "multiprocess_tests::multiprocess_shm_insert_and_close_child_process";

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
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

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
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

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
fn run_checkpoint(conn: &Arc<Connection>, mode: CheckpointMode) -> CheckpointResult {
    let pager = conn.pager.load();
    pager
        .io
        .block(|| pager.checkpoint(mode, SyncMode::Full, true))
        .unwrap()
}

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
fn wal_max_frame(conn: &Arc<Connection>) -> u64 {
    conn.pager
        .load()
        .wal
        .as_ref()
        .expect("wal should be present")
        .get_max_frame_in_wal()
}

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
fn grow_wal_past_frame_boundary(conn: &Arc<Connection>, target_frame: u64) {
    conn.wal_auto_checkpoint_disable();

    for _ in 0..64 {
        if wal_max_frame(conn) > target_frame {
            return;
        }

        conn.execute("begin immediate").unwrap();
        for _ in 0..128 {
            conn.execute("insert into test(value) values (randomblob(8192))")
                .unwrap();
        }
        conn.execute("commit").unwrap();
    }
    panic!(
        "failed to grow WAL beyond frame boundary {target_frame}; max_frame={}",
        wal_max_frame(conn)
    );
}

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
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

#[cfg(all(feature = "fs", target_os = "linux", target_pointer_width = "64"))]
#[test]
fn shared_wal_coordination_rejects_remote_filesystem_magic_values() {
    assert!(!Database::filesystem_magic_allows_shared_wal(0x6969));
    assert!(!Database::filesystem_magic_allows_shared_wal(
        0xFF53_4D42u32 as libc::c_long,
    ));
    assert!(!Database::filesystem_magic_allows_shared_wal(0x0102_1997));
    assert!(Database::filesystem_magic_allows_shared_wal(0xEF53));
    assert!(Database::filesystem_magic_allows_shared_wal(0x0102_1994));
}

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn database_open_selects_shm_wal_backend() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path_str).unwrap();

    let last_checksum_and_max_frame = db.shared_wal.read().last_checksum_and_max_frame();
    let wal = db
        .build_wal(last_checksum_and_max_frame, db.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "tshm");

    let shm_path = storage::wal::coordination_path_for_wal_path(&format!("{db_path_str}-wal"));
    assert!(std::path::Path::new(&shm_path).exists());
}

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn database_open_reuses_valid_exclusive_shm_authority_without_disk_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-reopen.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

    let db_a = Database::open_file(io.clone(), db_path_str).unwrap();
    let conn_a = db_a.connect().unwrap();
    conn_a
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn_a
        .execute("insert into test(value) values ('parent')")
        .unwrap();

    let db_b = Database::open_file(io, db_path_str).unwrap();
    assert!(!db_b
        .shared_wal
        .read()
        .metadata
        .loaded_from_disk_scan
        .load(Ordering::Acquire));

    let last_checksum_and_max_frame = db_b.shared_wal.read().last_checksum_and_max_frame();
    let wal = db_b
        .build_wal(last_checksum_and_max_frame, db_b.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "tshm");

    let conn_b = db_b.connect().unwrap();
    assert_eq!(count_test_rows(&conn_b), 1);
}

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn database_open_rebuilds_from_disk_scan_when_exclusive_shm_snapshot_is_stale() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-stale-reopen.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

    let db_a = Database::open_file(io.clone(), db_path_str).unwrap();
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

    let db_b = Database::open_file(io, db_path_str).unwrap();
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

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn database_open_reuses_valid_shm_authority_after_large_wal_crosses_frame_index_block_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-large-wal-reopen.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let tshm_path = storage::wal::coordination_path_for_wal_path(&format!("{db_path_str}-wal"));

    let db_a = Database::open_file(io.clone(), db_path_str).unwrap();
    let conn_a = db_a.connect().unwrap();
    conn_a
        .execute("create table test(id integer primary key, value blob)")
        .unwrap();
    wait_for_file(std::path::Path::new(&tshm_path));
    let initial_tshm_len = std::fs::metadata(&tshm_path).unwrap().len();

    grow_wal_past_frame_boundary(&conn_a, 4096);
    assert!(
        wal_max_frame(&conn_a) > 4096,
        "test setup should cross the first shared frame-index block boundary"
    );
    let grown_tshm_len = std::fs::metadata(&tshm_path).unwrap().len();
    assert!(
        grown_tshm_len > initial_tshm_len,
        "shared WAL coordination map should grow when the frame index crosses one block boundary"
    );
    let authority = db_a.shared_wal_coordination().unwrap().unwrap();
    let snapshot = authority.snapshot();
    assert_eq!(
        snapshot.nbackfills, 0,
        "large-WAL reopen coverage requires uncheckpointed WAL frames"
    );
    let wal_bytes = std::fs::read(format!("{db_path_str}-wal")).unwrap();
    let frame_size =
        crate::storage::sqlite3_ondisk::WAL_FRAME_HEADER_SIZE + snapshot.page_size as usize;
    let expected_wal_len =
        crate::storage::sqlite3_ondisk::WAL_HEADER_SIZE + snapshot.max_frame as usize * frame_size;
    assert_eq!(
        wal_bytes.len(),
        expected_wal_len,
        "authority snapshot should match on-disk WAL length before reopen"
    );
    let last_frame_offset = crate::storage::sqlite3_ondisk::WAL_HEADER_SIZE
        + (snapshot.max_frame as usize - 1) * frame_size;
    let (last_frame_header, _) =
        crate::storage::sqlite3_ondisk::parse_wal_frame_header(&wal_bytes[last_frame_offset..]);
    assert!(
        last_frame_header.is_commit_frame(),
        "large-WAL setup should end on a commit frame"
    );
    assert_eq!(last_frame_header.salt_1, snapshot.salt_1);
    assert_eq!(last_frame_header.salt_2, snapshot.salt_2);
    assert_eq!(last_frame_header.checksum_1, snapshot.checksum_1);
    assert_eq!(last_frame_header.checksum_2, snapshot.checksum_2);

    drop(conn_a);
    drop(db_a);
    let mut manager = DATABASE_MANAGER.lock();
    manager.clear();
    drop(manager);

    let db_b = Database::open_file(io, db_path_str).unwrap();
    assert!(
        !db_b
            .shared_wal
            .read()
            .metadata
            .loaded_from_disk_scan
            .load(Ordering::Acquire),
        "cold reopen should still trust tshm after the shared frame index grows past one block"
    );

    let conn_b = db_b.connect().unwrap();
    let rows = get_rows(&conn_b, "select count(*) from test");
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0][0].as_int().unwrap() > 0,
        "large WAL reopen should preserve committed rows across the frame-index block boundary"
    );
}

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn multiprocess_shm_insert_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path.to_str().unwrap()).unwrap();
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

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn multiprocess_shm_insert_and_close_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path.to_str().unwrap()).unwrap();
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

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn multiprocess_shm_count_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };
    let expected_count = std::env::var("TURSO_MULTIPROCESS_EXPECTED_COUNT")
        .unwrap()
        .parse::<i64>()
        .unwrap();

    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path.to_str().unwrap()).unwrap();
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

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
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

    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path.to_str().unwrap()).unwrap();
    let last_checksum_and_max_frame = db.shared_wal.read().last_checksum_and_max_frame();
    let wal = db
        .build_wal(last_checksum_and_max_frame, db.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "tshm");
    assert_eq!(wal_file.coordination_open_mode_name(), Some("multiprocess"));

    let conn = db.connect().unwrap();
    let pager = conn.pager.load();
    let wal = pager.wal.as_ref().unwrap();
    wal.begin_read_tx().unwrap();
    std::fs::write(&ready_file, b"ready").unwrap();
    wait_for_file(&release_file);
    wal.end_read_tx();
}

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn multiprocess_shm_schema_child_process() {
    let Some(db_path) = std::env::var_os("TURSO_MULTIPROCESS_DB_PATH") else {
        return;
    };

    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path.to_str().unwrap()).unwrap();
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
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path_str).unwrap();
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

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn subprocess_child_close_skips_shutdown_checkpoint_in_multiprocess_wal_mode() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("close-skip-shutdown-checkpoint.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path_str).unwrap();
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

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn subprocess_database_open_survives_truncate_rewrite_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-truncate-rewrite.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path_str).unwrap();
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

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn subprocess_database_open_peer_refreshes_remote_schema_without_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-peer-schema-refresh.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path_str).unwrap();
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

    let schema_rows = get_rows(
        &conn,
        "select name, type from sqlite_schema where name = 'child_table'",
    );
    assert_eq!(schema_rows.len(), 1);
    assert_eq!(schema_rows[0][0].to_string(), "child_table");
    assert_eq!(schema_rows[0][1].to_string(), "table");

    let child_rows = get_rows(&conn, "select value from child_table");
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

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn subprocess_database_truncate_checkpoint_reclaims_dead_child_reader_slot() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-dead-reader-slot.db");
    let ready_file = dir.path().join("child-ready");
    let release_file = dir.path().join("child-release");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path_str).unwrap();
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
    assert_eq!(
        child_status.signal(),
        Some(libc::SIGKILL),
        "expected killed child process to exit via SIGKILL, got {child_status:?}"
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

    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let reopened = Database::open_file(io, db_path_str).unwrap();
    let reopened_conn = reopened.connect().unwrap();
    assert_eq!(
        count_test_rows(&reopened_conn),
        2,
        "cold reopen after reader-slot reclamation should preserve committed rows"
    );
}

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn database_open_reuses_valid_shm_authority_after_truncate_rewrite_without_disk_scan() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("coordination-truncate-rewrite-reopen.db");
    let db_path_str = db_path.to_str().unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());

    let db_a = Database::open_file(io.clone(), db_path_str).unwrap();
    let conn_a = db_a.connect().unwrap();
    conn_a
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    conn_a
        .execute("insert into test(value) values ('before-truncate')")
        .unwrap();

    let checkpoint = run_checkpoint(
        &conn_a,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );
    assert!(
        checkpoint.everything_backfilled(),
        "truncate checkpoint should fully backfill before WAL rewrite"
    );

    conn_a
        .execute("insert into test(value) values ('after-truncate')")
        .unwrap();

    let db_b = Database::open_file(io, db_path_str).unwrap();
    assert!(!db_b
        .shared_wal
        .read()
        .metadata
        .loaded_from_disk_scan
        .load(Ordering::Acquire));

    let last_checksum_and_max_frame = db_b.shared_wal.read().last_checksum_and_max_frame();
    let wal = db_b
        .build_wal(last_checksum_and_max_frame, db_b.buffer_pool.clone())
        .unwrap();
    let wal_file = wal.as_any().downcast_ref::<WalFile>().unwrap();
    assert_eq!(wal_file.coordination_backend_name(), "tshm");

    let conn_b = db_b.connect().unwrap();
    assert_eq!(count_test_rows(&conn_b), 2);
}

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
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

#[cfg(all(feature = "fs", unix, target_pointer_width = "64"))]
#[test]
fn memory_database_query_can_close_without_checkpointing() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:").unwrap();
    let conn = db.connect().unwrap();

    conn.query("VALUES ('ok')").unwrap();
    conn.close().unwrap();
}
