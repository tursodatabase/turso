use turso_core::security::roles::SessionRole;
use turso_core::storage::sqlite3_ondisk;
use turso_core::sync::Arc;
use turso_core::CheckpointMode;
use turso_core::CheckpointResult;
use turso_core::ConnectOpts;
use turso_core::Connection;
use turso_core::Database;
use turso_core::IOExt;
use turso_core::PlatformIO;
use turso_core::IO;
use turso_sqlite::SqliteDialect;

/// Returns an IO backend that supports shared WAL coordination on the host.
/// On Windows the default `PlatformIO` (`WindowsIO`) lacks the byte-locking
/// and mapping primitives, so the experimental IOCP backend is used when
/// the `experimental_win_iocp` feature is enabled.
fn shared_wal_test_io() -> Arc<dyn IO> {
    #[cfg(all(target_os = "windows", feature = "win_iocp"))]
    {
        Arc::new(turso_core::WindowsIOCP::new().unwrap())
    }
    #[cfg(not(all(target_os = "windows", feature = "win_iocp")))]
    {
        Arc::new(PlatformIO::new().unwrap())
    }
}

/// The returned `TempDir` deletes the database directory when it drops, so
/// callers must hold it for as long as they use the database.
#[allow(clippy::arc_with_non_send_sync)]
pub(crate) fn get_database() -> (Arc<Database>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("test.db");
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "wal")
            .unwrap();
    }
    let io = shared_wal_test_io();
    let db = Database::open_file_with_flags(
        io.clone(),
        path.to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_multiprocess_wal(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    // db + tmp directory
    (db, temp_dir)
}

#[test]
fn test_wal_truncate_checkpoint() {
    let (db, path) = get_database();
    let walpath = path.path().join("test.db-wal");

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test (id integer primary key, value text)")
        .unwrap();
    for _i in 0..25 {
        let _ = conn.execute("insert into test (value) values (randomblob(1024)), (randomblob(1024)), (randomblob(1024))");
    }
    let pager = conn.get_pager();
    let _ = pager.cacheflush();

    let stat = std::fs::metadata(&walpath).unwrap();
    let meta_before = std::fs::metadata(&walpath).unwrap();
    let bytes_before = meta_before.len();
    run_checkpoint_until_done(
        &pager,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    assert_eq!(pager.wal_state().unwrap().max_frame, 0);

    tracing::debug!("wal filepath: {walpath:?}, size: {}", stat.len());
    let meta_after = std::fs::metadata(&walpath).unwrap();
    let bytes_after = meta_after.len();
    assert_ne!(
        bytes_before, bytes_after,
        "WAL file should not have been empty before checkpoint"
    );
    assert_eq!(
        bytes_after, 0,
        "WAL file should be truncated to 0 bytes, but is {bytes_after} bytes",
    );
}

#[test]
#[cfg_attr(
    all(target_os = "windows", not(feature = "win_iocp")),
    ignore = "shared WAL coordination requires the experimental Windows IOCP backend"
)]
fn test_shutdown_checkpoint_truncates_after_restart() {
    let (db, path) = get_database();
    let walpath = path.path().join("test.db-wal");

    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test (id integer primary key, value text)")
        .unwrap();
    conn.execute("insert into test (value) values ('v1'), ('v2')")
        .unwrap();

    let pager = conn.get_pager();
    run_checkpoint_until_done(&pager, CheckpointMode::Restart);

    let bytes_before = std::fs::metadata(&walpath).unwrap().len();
    assert!(
        bytes_before > 0,
        "WAL should still have data after RESTART checkpoint"
    );

    conn.close().unwrap();

    let bytes_after = std::fs::metadata(&walpath).unwrap().len();
    assert_eq!(
        bytes_after, 0,
        "Shutdown checkpoint should truncate WAL after RESTART, but WAL is {bytes_after} bytes",
    );
}

fn bulk_inserts(conn: &Arc<Connection>, n_txns: usize, rows_per_txn: usize) {
    for _ in 0..n_txns {
        conn.execute("begin transaction").unwrap();
        for i in 0..rows_per_txn {
            conn.execute(format!("insert into test(value) values ('v{i}')"))
                .unwrap();
        }
        conn.execute("commit").unwrap();
    }
}

fn count_test_table(conn: &Arc<Connection>) -> i64 {
    let mut stmt = conn.prepare("select count(*) from test").unwrap();
    let mut count: i64 = 0;
    stmt.run_with_row_callback(|row| {
        count = row.get(0).unwrap();
        Ok(())
    })
    .unwrap();
    count
}

fn run_checkpoint_until_done(pager: &turso_core::Pager, mode: CheckpointMode) -> CheckpointResult {
    // Use pager.checkpoint() instead of wal.checkpoint() directly because
    // WAL truncation (for TRUNCATE mode) now happens in pager's TruncateWalFile phase.
    pager
        .io
        .block(|| pager.checkpoint(mode, turso_core::SyncMode::Full, true))
        .unwrap()
}

#[test]
fn test_checkpoint_truncate_reset_handling() {
    let (db, path) = get_database();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    let walpath = path.path().join("test.db-wal");

    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 10, 10);

    // Get size before checkpoint
    let size_before = std::fs::metadata(&walpath).unwrap().len();
    assert!(size_before > 0, "WAL file should have content");

    // Do a TRUNCATE checkpoint
    {
        let pager = conn.get_pager();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );
    }

    // Check file size after truncate
    let size_after = std::fs::metadata(&walpath).unwrap().len();
    assert_eq!(size_after, 0, "WAL file should be truncated to 0 bytes");

    // Verify we can still write to the database
    conn.execute("INSERT INTO test VALUES (1001, 'after-truncate')")
        .unwrap();

    // Check WAL has new content
    let new_size = std::fs::metadata(&walpath).unwrap().len();
    assert!(new_size >= 32, "WAL file too small");
    let hdr = read_wal_header(&walpath);
    let expected_magic = if cfg!(target_endian = "big") {
        sqlite3_ondisk::WAL_MAGIC_BE
    } else {
        sqlite3_ondisk::WAL_MAGIC_LE
    };
    assert!(
        hdr.magic == expected_magic,
        "bad WAL magic: {:#X}, expected: {:#X}",
        hdr.magic,
        sqlite3_ondisk::WAL_MAGIC_BE
    );
    assert_eq!(hdr.file_format, 3007000);
    assert_eq!(hdr.page_size, 4096, "invalid page size");
    assert_eq!(hdr.checkpoint_seq, 1, "invalid checkpoint_seq");
}

#[test]
fn test_wal_checkpoint_truncate_db_file_contains_data() {
    let (db, path) = get_database();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    let walpath = path.path().join("test.db-wal");

    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 10, 100);

    // Get size before checkpoint
    let size_before = std::fs::metadata(&walpath).unwrap().len();
    assert!(size_before > 0, "WAL file should have content");

    // Do a TRUNCATE checkpoint
    {
        let pager = conn.get_pager();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        );
    }

    // Check file size after truncate
    let size_after = std::fs::metadata(&walpath).unwrap().len();
    assert_eq!(size_after, 0, "WAL file should be truncated to 0 bytes");

    // Verify we can still write to the database
    conn.execute("INSERT INTO test VALUES (1001, 'after-truncate')")
        .unwrap();

    // Check WAL has new content
    let new_size = std::fs::metadata(&walpath).unwrap().len();
    assert!(new_size >= 32, "WAL file too small");
    let hdr = read_wal_header(&walpath);
    let expected_magic = if cfg!(target_endian = "big") {
        sqlite3_ondisk::WAL_MAGIC_BE
    } else {
        sqlite3_ondisk::WAL_MAGIC_LE
    };
    assert!(
        hdr.magic == expected_magic,
        "bad WAL magic: {:#X}, expected: {:#X}",
        hdr.magic,
        sqlite3_ondisk::WAL_MAGIC_BE
    );
    assert_eq!(hdr.file_format, 3007000);
    assert_eq!(hdr.page_size, 4096, "invalid page size");
    assert_eq!(hdr.checkpoint_seq, 1, "invalid checkpoint_seq");
    {
        let pager = conn.get_pager();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
    }
    // delete the WAL file so we can read right from db and assert
    // that everything was backfilled properly
    std::fs::remove_file(&walpath).unwrap();

    let count = count_test_table(&conn);
    assert_eq!(
        count, 1001,
        "we should have 1001 rows in the table all together"
    );
}

fn read_wal_header(path: &std::path::Path) -> sqlite3_ondisk::WalHeader {
    use std::{fs::File, io::Read};
    let mut hdr = [0u8; 32];
    File::open(path).unwrap().read_exact(&mut hdr).unwrap();
    let be = |i| u32::from_be_bytes(hdr[i..i + 4].try_into().unwrap());
    sqlite3_ondisk::WalHeader {
        magic: be(0x00),
        file_format: be(0x04),
        page_size: be(0x08),
        checkpoint_seq: be(0x0C),
        salt_1: be(0x10),
        salt_2: be(0x14),
        checksum_1: be(0x18),
        checksum_2: be(0x1C),
    }
}

#[test]
fn test_checkpoint_succeeds_after_rollback() {
    let (db, _path) = get_database();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 5, 3);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO test(value) VALUES('rollback_me')")
        .unwrap();
    conn.execute("ROLLBACK").unwrap();

    let pager = conn.get_pager();
    let result = run_checkpoint_until_done(&pager, CheckpointMode::Restart);
    assert!(
        result.everything_backfilled(),
        "checkpoint must succeed after rollback, not return Busy"
    );
}
