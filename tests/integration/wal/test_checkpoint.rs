use turso_core::security::roles::SessionRole;
use turso_core::storage::sqlite3_ondisk;
use turso_core::sync::Arc;
use turso_core::CheckpointMode;
use turso_core::CheckpointResult;
use turso_core::ConnectOpts;
use turso_core::Connection;
use turso_core::Database;
use turso_core::IOExt;
use turso_core::IOResult;
use turso_core::LimboError;
use turso_core::PlatformIO;
use turso_core::SyncMode;
use turso_core::WalAutoActions;
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

#[test]
fn test_wal_checkpoint_defers_backfill_publication_until_db_sync() {
    let (db, _path) = get_database();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 8, 2);

    let pager = conn.get_pager();
    let result = run_wal_checkpoint_until_done(&db, &pager, CheckpointMode::Full);
    assert!(
        result.wal_total_backfilled > 0,
        "checkpoint setup should backfill frames before DB sync"
    );
    assert_eq!(
        pager.wal_backfill_frame().unwrap(),
        0,
        "wal.checkpoint() must not publish positive nbackfills before DB sync completes"
    );
}

#[test]
fn test_checkpoint_sync_mode_off_leaves_backfill_unpublished() {
    let (db, _path) = get_database();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 8, 2);

    let pager = conn.get_pager();
    let result = pager
        .io
        .block(|| pager.checkpoint(CheckpointMode::Full, SyncMode::Off, true))
        .unwrap();
    assert!(
        result.wal_total_backfilled > 0,
        "sync-mode-off checkpoint setup should still backfill frames into the DB file"
    );
    assert_eq!(
        pager.wal_backfill_frame().unwrap(),
        0,
        "SyncMode::Off must not publish positive nbackfills as durable shared state"
    );
}

#[test]
fn test_wal_passive_partial_then_complete() {
    let (db, _tmp) = get_database();
    let conn1 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let conn2 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn1
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn1, 15, 2);
    let IOResult::Done(completions) = conn1.get_pager().cacheflush().unwrap() else {
        panic!()
    };
    for c in completions {
        db.io.wait_for_completion(c).unwrap();
    }

    // Force a read transaction that will freeze a lower read mark
    let readmark = {
        let pager = conn2.get_pager();
        let wal2 = pager.wal().unwrap();
        wal2.begin_read_tx().unwrap();
        wal2.get_max_frame()
    };

    // generate more frames that the reader will not see.
    bulk_inserts(&conn1, 15, 2);
    let IOResult::Done(completions) = conn1.get_pager().cacheflush().unwrap() else {
        panic!()
    };
    for c in completions {
        db.io.wait_for_completion(c).unwrap();
    }

    // Run passive checkpoint, expect partial
    let (res1, max_before) = {
        let pager = conn1.get_pager();
        let res = run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );
        let maxf = pager.wal_max_frame().unwrap();
        (res, maxf)
    };
    assert_eq!(res1.wal_max_frame, max_before);
    assert!(
        res1.wal_total_backfilled < res1.wal_max_frame,
        "Partial backfill expected, {} : {}",
        res1.wal_total_backfilled,
        res1.wal_max_frame
    );
    assert_eq!(
        res1.wal_total_backfilled, readmark,
        "Checkpointed frames should match read mark"
    );
    // Release reader
    {
        let pager = conn2.get_pager();
        let wal2 = pager.wal().unwrap();
        wal2.end_read_tx();
    }

    // Second passive checkpoint should finish
    let pager = conn1.get_pager();
    let res2 = run_checkpoint_until_done(
        &pager,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );
    assert_eq!(
        res2.wal_total_backfilled, res2.wal_max_frame,
        "Second checkpoint completes remaining frames"
    );
}

#[test]
fn test_wal_restart_blocks_readers() {
    let (db, _temp_dir) = get_database();
    let conn1 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let conn2 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    // Start a read transaction
    conn2
        .get_pager()
        .wal()
        .as_ref()
        .unwrap()
        .begin_read_tx()
        .unwrap();

    // checkpoint should succeed here because the wal is fully checkpointed (empty)
    // so the reader is using readmark0 to read directly from the db file.
    let p = conn1.get_pager();
    let w = p.wal().unwrap();
    loop {
        match w.checkpoint(&p, CheckpointMode::Restart, SyncMode::Full) {
            Ok(IOResult::IO(io)) => {
                io.wait(db.io.as_ref()).unwrap();
            }
            e => {
                assert!(
                    matches!(&e, Err(err) if matches!(**err, LimboError::Busy)),
                    "reader is holding readmark0 we should return Busy"
                );
                break;
            }
        }
    }
    conn2.get_pager().end_read_tx();

    conn1
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    for i in 0..10 {
        conn1
            .execute(format!("insert into test(value) values ('value{i}')"))
            .unwrap();
    }
    // now that we have some frames to checkpoint, try again
    conn2.get_pager().begin_read_tx().unwrap();
    let p = conn1.get_pager();
    let w = p.wal().unwrap();
    loop {
        match w.checkpoint(&p, CheckpointMode::Restart, SyncMode::Full) {
            Ok(IOResult::IO(io)) => {
                io.wait(db.io.as_ref()).unwrap();
            }
            Ok(IOResult::Done(_)) => {
                panic!("Checkpoint should not have succeeded");
            }
            Err(e) => {
                assert!(
                    matches!(*e, LimboError::Busy),
                    "should return busy if we have readers"
                );
                break;
            }
        }
    }
}

#[test]
fn test_wal_concurrent_readers_during_checkpoint() {
    let (db, _path) = get_database();
    let conn_writer = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn_writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn_writer, 5, 10);

    // Start multiple readers at different points
    let conn_r1 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let conn_r2 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    // R1 starts reading
    let r1_max_frame = {
        let pager = conn_r1.get_pager();
        let wal = pager.wal().unwrap();
        wal.begin_read_tx().unwrap();
        wal.get_max_frame()
    };
    bulk_inserts(&conn_writer, 5, 10);

    // R2 starts reading, sees more frames than R1
    let r2_max_frame = {
        let pager = conn_r2.get_pager();
        let wal = pager.wal().unwrap();
        wal.begin_read_tx().unwrap();
        wal.get_max_frame()
    };

    // try passive checkpoint, should only checkpoint up to R1's position
    let checkpoint_result = {
        let pager = conn_writer.get_pager();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        )
    };

    assert!(
        checkpoint_result.wal_total_backfilled < checkpoint_result.wal_max_frame,
        "Should not checkpoint all frames when readers are active"
    );
    assert_eq!(
        checkpoint_result.wal_total_backfilled, r1_max_frame,
        "Should have checkpointed up to R1's max frame"
    );

    // Verify R2 still sees its frames
    assert_eq!(
        conn_r2.get_pager().wal().unwrap().get_max_frame(),
        r2_max_frame,
        "Reader should maintain its snapshot"
    );
}

#[test]
fn test_wal_writer_blocks_restart_checkpoint() {
    let (db, _path) = get_database();
    let conn1 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let conn2 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn1
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn1, 5, 5);

    // start a write transaction
    {
        let pager = conn2.get_pager();
        let wal = pager.wal().unwrap();
        let _ = wal.begin_read_tx().unwrap();
        wal.begin_write_tx(WalAutoActions::all_enabled()).unwrap();
    }

    // should fail because writer lock is held
    let result = {
        let pager = conn1.get_pager();
        let wal = pager.wal().unwrap();
        wal.checkpoint(&pager, CheckpointMode::Restart, SyncMode::Full)
    };

    assert!(
        matches!(&result, Err(err) if matches!(**err, LimboError::Busy)),
        "Restart checkpoint should fail when write lock is held"
    );

    conn2.get_pager().wal().unwrap().end_read_tx();
    // release write lock
    conn2.get_pager().wal().unwrap().end_write_tx();

    // now restart should succeed
    let result = {
        let pager = conn1.get_pager();
        run_checkpoint_until_done(&pager, CheckpointMode::Restart)
    };

    assert!(result.everything_backfilled());
}

#[test]
#[should_panic(expected = "must have a read transaction to begin a write transaction")]
fn test_wal_read_transaction_required_before_write() {
    let (db, _path) = get_database();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();

    // Attempt to start a write transaction without a read transaction
    let pager = conn.get_pager();
    let wal = pager.wal().unwrap();
    let _ = wal.begin_write_tx(WalAutoActions::all_enabled());
}

#[test]
fn test_wal_multiple_readers_at_different_frames() {
    let (db, _path) = get_database();
    let conn_writer = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn_writer
        .execute("CREATE TABLE test(id INTEGER PRIMARY KEY, value TEXT)")
        .unwrap();

    fn start_reader(conn: &Arc<Connection>) -> (u64, turso_core::Statement) {
        conn.execute("BEGIN").unwrap();
        let mut stmt = conn.prepare("SELECT * FROM test").unwrap();
        stmt.step().unwrap();
        let frame = conn.get_pager().wal().unwrap().get_max_frame();
        (frame, stmt)
    }

    bulk_inserts(&conn_writer, 3, 5);

    let conn1 = &db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let (r1_frame, _stmt) = start_reader(conn1); // reader 1

    bulk_inserts(&conn_writer, 3, 5);

    let conn_r2 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let (r2_frame, _stmt2) = start_reader(&conn_r2); // reader 2

    bulk_inserts(&conn_writer, 3, 5);

    let conn_r3 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let (r3_frame, _stmt3) = start_reader(&conn_r3); // reader 3

    assert!(r1_frame < r2_frame && r2_frame < r3_frame);

    // passive checkpoint #1
    let result1 = {
        let pager = conn_writer.get_pager();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        )
    };
    assert_eq!(result1.wal_total_backfilled, r1_frame);

    // finish reader‑1
    conn1.execute("COMMIT").unwrap();

    // passive checkpoint #2
    let result2 = {
        let pager = conn_writer.get_pager();
        run_checkpoint_until_done(
            &pager,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        )
    };
    assert_eq!(
        result1.wal_checkpoint_backfilled + result2.wal_checkpoint_backfilled,
        r2_frame
    );

    // verify visible rows
    let r2_cnt = count_test_table(&conn_r2);
    let r3_cnt = count_test_table(&conn_r3);

    assert_eq!(r2_cnt, 30);
    assert_eq!(r3_cnt, 45);
}

#[test]
fn test_wal_stale_snapshot_in_write_transaction() {
    let (db, _path) = get_database();
    let conn1 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let conn2 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn1
        .execute("create table test(id integer primary key, value text)")
        .unwrap();
    // Start a read transaction on conn2
    {
        let pager = conn2.get_pager();
        let wal = pager.wal().unwrap();
        wal.begin_read_tx().unwrap();
    }
    // Make changes using conn1
    bulk_inserts(&conn1, 5, 5);
    // Try to start a write transaction on conn2 with a stale snapshot
    let result = {
        let pager = conn2.get_pager();
        let wal = pager.wal().unwrap();
        wal.begin_write_tx(WalAutoActions::all_enabled())
    };
    // Should get BusySnapShot due to stale snapshot
    assert!(matches!(result, Err(LimboError::BusySnapshot)));

    // End read transaction and start a fresh one
    {
        let pager = conn2.get_pager();
        let wal = pager.wal().unwrap();
        wal.end_read_tx();
        wal.begin_read_tx().unwrap();
    }
    // Now write transaction should work
    let result = {
        let pager = conn2.get_pager();
        let wal = pager.wal().unwrap();
        wal.begin_write_tx(WalAutoActions::all_enabled())
    };
    assert!(matches!(result, Ok(())));
}

#[test]
fn test_wal_full_backfills_all() {
    let (db, _tmp) = get_database();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    // Write some data to put frames in the WAL
    conn.execute("create table test(id integer primary key, value text)")
        .unwrap();
    bulk_inserts(&conn, 8, 4);

    // Ensure frames are flushed to the WAL
    let IOResult::Done(completions) = conn.get_pager().cacheflush().unwrap() else {
        panic!()
    };
    for c in completions {
        db.io.wait_for_completion(c).unwrap();
    }

    // Snapshot the current mxFrame before running FULL
    let mx_before = conn.get_pager().wal_max_frame().unwrap();
    assert!(mx_before > 0, "expected frames in WAL before FULL");

    // Run FULL checkpoint - must backfill *all* frames up to mx_before
    let result = {
        let pager = conn.get_pager();
        run_checkpoint_until_done(&pager, CheckpointMode::Full)
    };

    assert_eq!(result.wal_checkpoint_backfilled, mx_before);
    assert_eq!(result.wal_total_backfilled, mx_before);
}

#[test]
fn test_wal_full_waits_for_old_reader_then_succeeds() {
    let (db, _tmp) = get_database();
    let writer = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let reader = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    writer
        .execute("create table test(id integer primary key, value text)")
        .unwrap();

    // First commit some data and flush (reader will snapshot here)
    bulk_inserts(&writer, 2, 3);
    let IOResult::Done(completions) = writer.get_pager().cacheflush().unwrap() else {
        panic!()
    };
    for c in completions {
        db.io.wait_for_completion(c).unwrap();
    }

    // Start a read transaction pinned at the current snapshot
    {
        let pager = reader.get_pager();
        let wal = pager.wal().unwrap();
        wal.begin_read_tx().unwrap();
    }
    let r_snapshot = {
        let pager = reader.get_pager();
        let wal = pager.wal().unwrap();
        wal.get_max_frame()
    };

    // Advance WAL beyond the reader's snapshot
    bulk_inserts(&writer, 3, 4);
    let IOResult::Done(completions) = writer.get_pager().cacheflush().unwrap() else {
        panic!()
    };
    for c in completions {
        db.io.wait_for_completion(c).unwrap();
    }
    let mx_now = writer.get_pager().wal_max_frame().unwrap();
    assert!(mx_now > r_snapshot);

    // FULL must return Busy while a reader is stuck behind
    {
        let pager = writer.get_pager();
        let wal = pager.wal().unwrap();
        loop {
            match wal.checkpoint(&pager, CheckpointMode::Full, SyncMode::Full) {
                Ok(IOResult::IO(io)) => {
                    // Drive any pending IO (should quickly become Busy or Done)
                    io.wait(db.io.as_ref()).unwrap();
                }
                Err(err) if matches!(*err, LimboError::Busy) => {
                    break;
                }
                other => panic!("expected Busy from FULL with old reader, got {other:?}"),
            }
        }
    }
    assert_eq!(
        writer.get_pager().wal_backfill_frame().unwrap(),
        0,
        "a FULL checkpoint that returns Busy must not publish positive nbackfills before DB sync"
    );

    // Release the reader, now full mode should succeed and backfill everything
    {
        let pager = reader.get_pager();
        let wal = pager.wal().unwrap();
        wal.end_read_tx();
    }

    let result = {
        let pager = writer.get_pager();
        run_checkpoint_until_done(&pager, CheckpointMode::Full)
    };

    assert_eq!(
        result.wal_checkpoint_backfilled, mx_now,
        "the successful FULL reruns from the last durable backfill point because the Busy attempt did not publish progress"
    );
    assert!(result.everything_backfilled());
}

#[test]
fn test_rollback_releases_read_lock() {
    let (db, _path) = get_database();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES(1)").unwrap();

    {
        let pager = conn.get_pager();
        let wal = pager.wal().unwrap();
        assert!(
            wal.holds_read_lock(),
            "read lock must be held during write tx"
        );
    }

    conn.execute("ROLLBACK").unwrap();

    {
        let pager = conn.get_pager();
        let wal = pager.wal().unwrap();
        assert!(
            !wal.holds_read_lock(),
            "read lock must be released after ROLLBACK"
        );
    }
}

#[test]
fn test_savepoint_rollback_preserves_read_lock() {
    let (db, _path) = get_database();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES(1)").unwrap();

    // Trigger a statement failure that causes savepoint rollback.
    // A duplicate primary key on the second INSERT will fail the
    // statement, rolling back to the anonymous savepoint while
    // keeping the write transaction open.
    let res = conn.execute("INSERT INTO t VALUES(1)");
    assert!(res.is_err(), "duplicate PK insert must fail");

    {
        let pager = conn.get_pager();
        let wal = pager.wal().unwrap();
        assert!(
            wal.holds_read_lock(),
            "read lock must still be held after savepoint rollback"
        );
        assert!(
            wal.holds_write_lock(),
            "write lock must still be held after savepoint rollback"
        );
    }

    // The transaction should still be usable: commit succeeds and
    // the first insert is preserved.
    conn.execute("COMMIT").unwrap();

    let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
    let mut count: i64 = 0;
    stmt.run_with_row_callback(|row| {
        count = row.get(0).unwrap();
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 1, "first insert should survive savepoint rollback");
}

fn run_wal_checkpoint_until_done(
    db: &Database,
    pager: &turso_core::Pager,
    mode: CheckpointMode,
) -> CheckpointResult {
    let wal = pager.wal().expect("wal should be present");
    loop {
        match wal.checkpoint(pager, mode, SyncMode::Full) {
            Ok(IOResult::IO(io)) => io.wait(db.io.as_ref()).unwrap(),
            Ok(IOResult::Done(result)) => return result,
            Err(err) => panic!("checkpoint should succeed: {err:?}"),
        }
    }
}
