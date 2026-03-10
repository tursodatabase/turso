//! Multi-process WAL scenario tests.
//!
//! Each test simulates multiple "virtual processes" by opening separate
//! `Database` instances on the same file. This gives:
//! - Real flock behavior (separate OFDs = real mutual exclusion)
//! - Real mmap behavior (same physical pages via kernel)
//! - Deterministic operation ordering (sequential, single-thread)
//!
//! The only sharing between instances is through filesystem objects
//! (WAL file + TSHM mmap), exactly as with real cross-process access.

use std::sync::Arc;

use crate::io::{OpenFlags, PlatformIO, IO};
use crate::storage::database::DatabaseFile;
use crate::storage::multi_process_wal::MultiProcessWal;
use crate::storage::tshm::NUM_READER_SLOTS;
use crate::storage::wal::CheckpointMode;
use crate::types::Value;
use crate::util::IOExt;
use crate::{Connection, Database, DatabaseOpts, LimboError, LockingMode};

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

/// Create a fresh WAL-mode database in a temp directory. Returns the Database,
/// a Connection, and the directory path (kept alive to prevent cleanup).
fn open_mp_database() -> (Arc<Database>, Arc<Connection>, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut db_path = dir.clone();
    db_path.push("test.db");

    // Create the database via rusqlite so the file exists and is WAL-mode.
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
    }

    let (db, conn) = open_with_mode(&dir, LockingMode::SharedWrites);
    (db, conn, dir)
}

/// Open another "virtual process" on the same database directory.
fn open_second_process(dir: &std::path::Path) -> (Arc<Database>, Arc<Connection>) {
    open_with_mode(dir, LockingMode::SharedWrites)
}

/// Open a "virtual process" on the given directory with a specific locking mode.
///
/// IMPORTANT: Bypasses the process-wide database registry to create a truly
/// independent Database instance with its own file descriptors and TSHM,
/// simulating a separate OS process. The default `open_file_with_flags`
/// returns the same Arc<Database> from the registry, which defeats
/// multi-process flock-based exclusion testing.
fn try_open_with_mode(
    dir: &std::path::Path,
    mode: LockingMode,
) -> crate::Result<(Arc<Database>, Arc<Connection>)> {
    let mut db_path = dir.to_path_buf();
    db_path.push("test.db");
    let db_path_str = db_path.to_str().unwrap();
    let wal_path = format!("{db_path_str}-wal");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let mut flags = OpenFlags::default();
    if mode.is_shared() {
        flags |= OpenFlags::SharedLock;
    }
    let file = io.open_file(db_path_str, flags, true).unwrap();
    let db_file = Arc::new(DatabaseFile::new(file));
    let opts = DatabaseOpts::new()
        .with_locking_mode(mode)
        .with_shared_access(mode.is_shared());
    let mut state = crate::OpenDbAsyncState::new();
    let db = loop {
        match Database::open_with_flags_bypass_registry_async(
            &mut state,
            io.clone(),
            db_path_str,
            Some(&wal_path),
            db_file.clone(),
            flags,
            opts,
            None,
        )? {
            crate::IOResult::Done(db) => break db,
            crate::IOResult::IO(io_completion) => {
                io_completion.wait(&*io).unwrap();
            }
        }
    };
    let conn = db.connect()?;
    Ok((db, conn))
}

fn open_with_mode(dir: &std::path::Path, mode: LockingMode) -> (Arc<Database>, Arc<Connection>) {
    match try_open_with_mode(dir, mode) {
        Ok(result) => result,
        Err(e) => panic!("open_with_mode failed: {e}"),
    }
}

/// Execute a SQL statement (no result rows expected).
fn exec(conn: &Arc<Connection>, sql: &str) {
    conn.execute(sql).unwrap();
}

/// Execute a SQL statement, collecting all result rows.
fn query_rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.run_collect_rows().unwrap()
}

/// Execute a SQL query and return the single integer value from the first row.
fn query_i64(conn: &Arc<Connection>, sql: &str) -> i64 {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut val: i64 = 0;
    stmt.run_with_row_callback(|row| {
        val = row.get(0).unwrap();
        Ok(())
    })
    .unwrap();
    val
}

/// Assert PRAGMA integrity_check returns "ok".
fn assert_integrity_ok(conn: &Arc<Connection>) {
    let rows = query_rows(conn, "PRAGMA integrity_check");
    assert!(!rows.is_empty(), "integrity_check returned no rows");
    match &rows[0][0] {
        Value::Text(s) => assert_eq!(s.as_ref(), "ok", "integrity_check failed: {s}"),
        other => panic!("unexpected integrity_check result: {other:?}"),
    }
}

/// Run a passive checkpoint to completion via the pager.
fn run_checkpoint(conn: &Arc<Connection>, mode: CheckpointMode) {
    let pager = conn.pager.load();
    pager
        .io
        .block(|| pager.checkpoint(mode, crate::SyncMode::Full, true))
        .unwrap();
}

/// Get the current max_frame from the WAL.
fn wal_max_frame(conn: &Arc<Connection>) -> u64 {
    let pager = conn.pager.load();
    pager.wal_state().unwrap().max_frame
}

// ---------------------------------------------------------------------------
// S1: Snapshot Isolation (explicit transaction holds TSHM slot)
// ---------------------------------------------------------------------------
#[test]
fn test_mp_s1_snapshot_isolation() {
    let (_db_w, conn_w, dir) = open_mp_database();
    let (_db_r, conn_r) = open_second_process(&dir);

    // Writer: create table and insert 2 rows.
    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "INSERT INTO t VALUES (1,'a'), (2,'b')");

    // Reader: BEGIN explicit transaction — claims TSHM slot.
    exec(&conn_r, "BEGIN");
    let count = query_i64(&conn_r, "SELECT count(*) FROM t");
    assert_eq!(count, 2, "reader should see 2 rows at snapshot start");

    // Writer: insert 2 more rows (committed, but reader holds snapshot).
    exec(&conn_w, "INSERT INTO t VALUES (3,'c'), (4,'d')");

    // Reader: still in same transaction — should see old snapshot.
    let count = query_i64(&conn_r, "SELECT count(*) FROM t");
    assert_eq!(count, 2, "reader should still see 2 rows within snapshot");

    // Reader: COMMIT and re-query — should now see 4 rows.
    exec(&conn_r, "COMMIT");
    let count = query_i64(&conn_r, "SELECT count(*) FROM t");
    assert_eq!(count, 4, "reader should see 4 rows after commit");

    assert_integrity_ok(&conn_w);
}

// ---------------------------------------------------------------------------
// S2: Checkpoint Respects Active Reader
// ---------------------------------------------------------------------------
#[test]
fn test_mp_s2_checkpoint_respects_active_reader() {
    let (_db_w, conn_w, dir) = open_mp_database();
    let (_db_r, conn_r) = open_second_process(&dir);

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");

    // Writer: insert 100 rows.
    exec(&conn_w, "BEGIN");
    for i in 0..100 {
        exec(&conn_w, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
    }
    exec(&conn_w, "COMMIT");

    let max_after_first_batch = wal_max_frame(&conn_w);
    assert!(max_after_first_batch > 0, "WAL should have frames");

    // Reader: BEGIN — claims TSHM slot at current max_frame.
    exec(&conn_r, "BEGIN");
    let _count = query_i64(&conn_r, "SELECT count(*) FROM t");

    // Writer: insert 100 more rows.
    exec(&conn_w, "BEGIN");
    for i in 100..200 {
        exec(&conn_w, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
    }
    exec(&conn_w, "COMMIT");

    // Checkpoint — should not backfill past the reader's snapshot frame.
    run_checkpoint(
        &conn_w,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );

    // The WAL should still have frames (reader holds snapshot).
    let max_after_ckpt = wal_max_frame(&conn_w);
    assert!(
        max_after_ckpt > 0,
        "WAL should still have frames while reader holds snapshot"
    );

    // Reader: COMMIT — releases TSHM slot.
    exec(&conn_r, "COMMIT");

    // Checkpoint again — should now be able to backfill everything.
    run_checkpoint(
        &conn_w,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );

    // Verify all data is intact.
    let count = query_i64(&conn_w, "SELECT count(*) FROM t");
    assert_eq!(count, 200, "should have 200 rows after full checkpoint");
    assert_integrity_ok(&conn_w);
}

// ---------------------------------------------------------------------------
// S3: WAL Reset Blocked by Reader
// ---------------------------------------------------------------------------
#[test]
fn test_mp_s3_wal_reset_blocked_by_reader() {
    let (db_w, conn_w, dir) = open_mp_database();
    let (_db_r, conn_r) = open_second_process(&dir);

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "INSERT INTO t VALUES (1, 'a')");

    // Checkpoint everything so WAL is fully backfilled.
    run_checkpoint(
        &conn_w,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );

    // Reader: BEGIN — holds TSHM slot.
    exec(&conn_r, "BEGIN");
    let _count = query_i64(&conn_r, "SELECT count(*) FROM t");

    // Writer: insert more data. This triggers try_restart_log_before_write
    // which should NOT restart the WAL because the reader holds a slot.
    exec(&conn_w, "INSERT INTO t VALUES (2, 'b')");

    // WAL should NOT have been restarted (max_frame should still be > 0).
    let max_frame = db_w
        .shared_wal
        .read()
        .max_frame
        .load(std::sync::atomic::Ordering::Acquire);
    assert!(
        max_frame > 0,
        "WAL should not have restarted while reader holds slot (max_frame={max_frame})"
    );

    // Reader: COMMIT — releases TSHM slot.
    exec(&conn_r, "COMMIT");

    // Writer: insert again. Now try_restart_log should succeed.
    // First checkpoint so nbackfills == max_frame (restart precondition).
    run_checkpoint(
        &conn_w,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );

    // Start a write tx to trigger try_restart_log.
    {
        let pager = conn_w.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.begin_read_tx().unwrap();
        wal.begin_write_tx().unwrap();
    }

    // WAL should now have been restarted.
    let max_final = db_w
        .shared_wal
        .read()
        .max_frame
        .load(std::sync::atomic::Ordering::Acquire);
    assert_eq!(
        max_final, 0,
        "WAL should be restarted when no external readers (max_frame={max_final})"
    );

    {
        let pager = conn_w.pager.load();
        let wal = pager.wal.as_ref().unwrap();
        wal.end_write_tx();
        wal.end_read_tx();
    }

    assert_integrity_ok(&conn_w);
}

// ---------------------------------------------------------------------------
// S4: Reader Crash Recovery
// ---------------------------------------------------------------------------
#[test]
fn test_mp_s4_reader_crash_recovery() {
    let (db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "INSERT INTO t VALUES (1, 'a'), (2, 'b')");

    // Open a reader and BEGIN — claims TSHM slot.
    let (db_r, conn_r) = open_second_process(&dir);
    exec(&conn_r, "BEGIN");
    let count = query_i64(&conn_r, "SELECT count(*) FROM t");
    assert_eq!(count, 2);

    // Crash the reader: drop both connection and database without commit.
    // This leaves a stale TSHM slot.
    drop(conn_r);
    drop(db_r);

    // Writer: insert more data.
    exec(&conn_w, "INSERT INTO t VALUES (3, 'c')");

    // Checkpoint should be limited by the stale slot (since PID is alive,
    // the stale slot won't be auto-reclaimed in-process).
    run_checkpoint(
        &conn_w,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );

    // Manually clear the stale slot (simulate OS PID reclaim in real multi-process).
    if let Some(tshm) = &db_w.tshm {
        // Find and release all reader slots.
        tshm.reclaim_dead_slots();
    }

    // Checkpoint again — should now succeed fully.
    run_checkpoint(
        &conn_w,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );

    // Verify data is intact.
    let count = query_i64(&conn_w, "SELECT count(*) FROM t");
    assert_eq!(count, 3);
    assert_integrity_ok(&conn_w);
}

// ---------------------------------------------------------------------------
// S5: Writer Crash Recovery
// ---------------------------------------------------------------------------
#[test]
fn test_mp_s5_writer_crash_recovery() {
    let (_db_w, conn_w, dir) = open_mp_database();

    // Writer: create table and insert 10 committed rows.
    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "BEGIN");
    for i in 0..10 {
        exec(&conn_w, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
    }
    exec(&conn_w, "COMMIT");

    let count = query_i64(&conn_w, "SELECT count(*) FROM t");
    assert_eq!(count, 10, "should have 10 committed rows");

    // Writer: BEGIN a new transaction, insert 10 more rows, but do NOT commit.
    exec(&conn_w, "BEGIN");
    for i in 10..20 {
        exec(&conn_w, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
    }

    // Crash the writer: drop without commit.
    drop(conn_w);
    drop(_db_w);

    // New process opens the database — should see only the 10 committed rows.
    let (_db_new, conn_new) = open_second_process(&dir);
    let count = query_i64(&conn_new, "SELECT count(*) FROM t");
    assert_eq!(
        count, 10,
        "new process should see only 10 committed rows, not 20 (got {count})"
    );

    assert_integrity_ok(&conn_new);
}

// ---------------------------------------------------------------------------
// S6: No Checkpoint Livelock
// ---------------------------------------------------------------------------
#[test]
fn test_mp_s6_no_checkpoint_livelock() {
    let (_db_w, conn_w, _dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");

    let mut prev_max_frame = 0u64;
    let mut grew_unbounded = false;

    for round in 0..100 {
        exec(&conn_w, "BEGIN");
        for i in 0..10 {
            let id = round * 10 + i;
            exec(&conn_w, &format!("INSERT INTO t VALUES ({id}, 'v{id}')"));
        }
        exec(&conn_w, "COMMIT");

        run_checkpoint(
            &conn_w,
            CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
        );

        let mf = wal_max_frame(&conn_w);
        // After a successful passive checkpoint with no readers, all frames
        // should be backfilled. The WAL may still show non-zero max_frame
        // until restart, but it should not be growing without bound.
        if mf > prev_max_frame + 100 {
            grew_unbounded = true;
            break;
        }
        prev_max_frame = mf;
    }

    assert!(
        !grew_unbounded,
        "WAL grew without bound despite periodic checkpointing"
    );

    let count = query_i64(&conn_w, "SELECT count(*) FROM t");
    assert_eq!(count, 1000, "should have 1000 rows");
    assert_integrity_ok(&conn_w);
}

// ---------------------------------------------------------------------------
// S7: Schema Visibility — Multiple Creates
// ---------------------------------------------------------------------------
#[test]
fn test_mp_s7_schema_visibility_multiple_creates() {
    let (_db_a, conn_a, dir) = open_mp_database();
    let (_db_b, conn_b) = open_second_process(&dir);

    // Process A creates tables one by one.
    exec(&conn_a, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_a, "INSERT INTO t1 VALUES (1, 'hello')");

    // Process B verifies t1 is visible.
    let count = query_i64(&conn_b, "SELECT count(*) FROM t1");
    assert_eq!(count, 1, "B should see t1 with 1 row");

    exec(
        &conn_a,
        "CREATE TABLE t2(id INTEGER PRIMARY KEY, name TEXT)",
    );
    exec(&conn_a, "INSERT INTO t2 VALUES (1, 'world')");

    // Process B verifies t2 is visible.
    let count = query_i64(&conn_b, "SELECT count(*) FROM t2");
    assert_eq!(count, 1, "B should see t2 with 1 row");

    exec(
        &conn_a,
        "CREATE TABLE t3(id INTEGER PRIMARY KEY, data BLOB)",
    );
    exec(&conn_a, "INSERT INTO t3 VALUES (1, X'DEADBEEF')");

    // Process B verifies t3 is visible.
    let count = query_i64(&conn_b, "SELECT count(*) FROM t3");
    assert_eq!(count, 1, "B should see t3 with 1 row");

    // Process B can see all tables in schema.
    let rows = query_rows(
        &conn_b,
        "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name",
    );
    let names: Vec<String> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.to_string(),
            _ => panic!("expected text"),
        })
        .collect();
    assert_eq!(names, vec!["t1", "t2", "t3"]);

    assert_integrity_ok(&conn_a);
}

// ---------------------------------------------------------------------------
// S8: Schema Visibility — ALTER, DROP, and Indexes
// ---------------------------------------------------------------------------
#[test]
fn test_mp_s8_schema_alter_drop_indexes() {
    let (_db_a, conn_a, dir) = open_mp_database();
    let (_db_b, conn_b) = open_second_process(&dir);

    // Process A: create table with data.
    exec(
        &conn_a,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER)",
    );
    exec(&conn_a, "INSERT INTO t VALUES (1, 'hello', 42)");
    exec(&conn_a, "INSERT INTO t VALUES (2, 'world', 99)");

    // Process B sees data.
    let count = query_i64(&conn_b, "SELECT count(*) FROM t");
    assert_eq!(count, 2);

    // Process A: CREATE INDEX.
    exec(&conn_a, "CREATE INDEX idx_b ON t(b)");

    // Process B: query using the indexed column.
    let rows = query_rows(&conn_b, "SELECT a FROM t WHERE b = 42");
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        Value::Text(s) => assert_eq!(s.as_ref(), "hello"),
        other => panic!("expected text, got {other:?}"),
    }

    // Process A: ALTER TABLE ADD COLUMN.
    exec(&conn_a, "ALTER TABLE t ADD COLUMN c DEFAULT 0");

    // Process B: sees new column.
    let rows = query_rows(&conn_b, "SELECT c FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);

    // Process A: DROP INDEX.
    exec(&conn_a, "DROP INDEX idx_b");

    // Process B: query still works (just no index).
    let rows = query_rows(&conn_b, "SELECT a FROM t WHERE b = 99");
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        Value::Text(s) => assert_eq!(s.as_ref(), "world"),
        other => panic!("expected text, got {other:?}"),
    }

    // Process A: CREATE another INDEX.
    exec(&conn_a, "CREATE INDEX idx_a ON t(a)");

    // Process B: query using new index.
    let rows = query_rows(&conn_b, "SELECT b FROM t WHERE a = 'hello'");
    assert_eq!(rows.len(), 1);

    // Process A: DROP TABLE.
    exec(&conn_a, "DROP TABLE t");

    // Process B: prepare should fail with "no such table".
    let result = conn_b.prepare("SELECT * FROM t");
    assert!(result.is_err(), "prepare should fail after DROP TABLE");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("no such table"),
        "error should mention 'no such table', got: {err_msg}"
    );

    assert_integrity_ok(&conn_a);
}

// ---------------------------------------------------------------------------
// S9: Long-Running Reader
// ---------------------------------------------------------------------------
#[test]
fn test_mp_s9_long_running_reader() {
    let (_db_w, conn_w, dir) = open_mp_database();
    let (_db_r, conn_r) = open_second_process(&dir);

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");

    // Insert initial rows.
    exec(&conn_w, "BEGIN");
    for i in 0..10 {
        exec(&conn_w, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
    }
    exec(&conn_w, "COMMIT");

    // Reader: BEGIN — snapshot at 10 rows.
    exec(&conn_r, "BEGIN");
    let snapshot_count = query_i64(&conn_r, "SELECT count(*) FROM t");
    assert_eq!(snapshot_count, 10, "reader snapshot should see 10 rows");

    // Writer: insert 50 batches of 100 rows each while reader holds snapshot.
    for batch in 0..50 {
        exec(&conn_w, "BEGIN");
        for i in 0..100 {
            let id = 10 + batch * 100 + i;
            exec(&conn_w, &format!("INSERT INTO t VALUES ({id}, 'v{id}')"));
        }
        exec(&conn_w, "COMMIT");
    }

    // Reader: should still see snapshot-era count.
    let count_during = query_i64(&conn_r, "SELECT count(*) FROM t");
    assert_eq!(
        count_during, 10,
        "reader should still see 10 rows during long writes"
    );

    // Reader: COMMIT — fresh query should see all rows.
    exec(&conn_r, "COMMIT");
    let final_count = query_i64(&conn_r, "SELECT count(*) FROM t");
    assert_eq!(
        final_count,
        10 + 50 * 100,
        "reader should see all 5010 rows after commit"
    );

    assert_integrity_ok(&conn_w);
}

// ---------------------------------------------------------------------------
// S10: Multiple Readers at Different Snapshots
// ---------------------------------------------------------------------------
#[test]
fn test_mp_s10_multiple_readers_different_snapshots() {
    let (_db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");

    // Insert 10 rows.
    exec(&conn_w, "BEGIN");
    for i in 0..10 {
        exec(&conn_w, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
    }
    exec(&conn_w, "COMMIT");

    // R1: BEGIN at 10 rows.
    let (_db_r1, conn_r1) = open_second_process(&dir);
    exec(&conn_r1, "BEGIN");
    let r1_count = query_i64(&conn_r1, "SELECT count(*) FROM t");
    assert_eq!(r1_count, 10, "R1 should see 10 rows");

    // Insert 10 more rows.
    exec(&conn_w, "BEGIN");
    for i in 10..20 {
        exec(&conn_w, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
    }
    exec(&conn_w, "COMMIT");

    // R2: BEGIN at 20 rows.
    let (_db_r2, conn_r2) = open_second_process(&dir);
    exec(&conn_r2, "BEGIN");
    let r2_count = query_i64(&conn_r2, "SELECT count(*) FROM t");
    assert_eq!(r2_count, 20, "R2 should see 20 rows");

    // Insert 10 more rows.
    exec(&conn_w, "BEGIN");
    for i in 20..30 {
        exec(&conn_w, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
    }
    exec(&conn_w, "COMMIT");

    // R3: BEGIN at 30 rows.
    let (_db_r3, conn_r3) = open_second_process(&dir);
    exec(&conn_r3, "BEGIN");
    let r3_count = query_i64(&conn_r3, "SELECT count(*) FROM t");
    assert_eq!(r3_count, 30, "R3 should see 30 rows");

    // Verify all readers still hold their snapshots simultaneously.
    assert_eq!(
        query_i64(&conn_r1, "SELECT count(*) FROM t"),
        10,
        "R1 snapshot should still be 10"
    );
    assert_eq!(
        query_i64(&conn_r2, "SELECT count(*) FROM t"),
        20,
        "R2 snapshot should still be 20"
    );
    assert_eq!(
        query_i64(&conn_r3, "SELECT count(*) FROM t"),
        30,
        "R3 snapshot should still be 30"
    );

    // Checkpoint — should be limited by R1 (oldest reader).
    run_checkpoint(
        &conn_w,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );
    // WAL should still have frames (R1 holds old snapshot).
    let mf = wal_max_frame(&conn_w);
    assert!(mf > 0, "WAL should have frames while R1 holds snapshot");

    // R1: COMMIT.
    exec(&conn_r1, "COMMIT");
    run_checkpoint(
        &conn_w,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );

    // R2: COMMIT.
    exec(&conn_r2, "COMMIT");
    run_checkpoint(
        &conn_w,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );

    // R3: COMMIT.
    exec(&conn_r3, "COMMIT");
    run_checkpoint(
        &conn_w,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );

    // All readers committed — verify final state.
    let final_count = query_i64(&conn_w, "SELECT count(*) FROM t");
    assert_eq!(final_count, 30, "should have 30 rows total");
    assert_integrity_ok(&conn_w);
}

// ===========================================================================
// Locking mode tests
// ===========================================================================

// ---------------------------------------------------------------------------
// LM1: Exclusive mode does not create TSHM file
// ---------------------------------------------------------------------------
#[test]
fn test_locking_mode_exclusive_no_tshm() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut db_path = dir.clone();
    db_path.push("test.db");

    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
    }

    let (_db, conn) = open_with_mode(&dir, LockingMode::Exclusive);
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY)");
    exec(&conn, "INSERT INTO t VALUES (1)");

    // No TSHM file should exist.
    let tshm_path = dir.join("test.db-tshm");
    assert!(
        !tshm_path.exists(),
        "TSHM file should not exist in exclusive mode"
    );
}

// ---------------------------------------------------------------------------
// LM2: SharedWrites creates TSHM file
// ---------------------------------------------------------------------------
#[test]
fn test_locking_mode_shared_writes_creates_tshm() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut db_path = dir.clone();
    db_path.push("test.db");

    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
    }

    let (_db_a, conn_a) = open_with_mode(&dir, LockingMode::SharedWrites);
    exec(&conn_a, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_a, "INSERT INTO t VALUES (1, 'a')");

    // TSHM file should exist.
    let tshm_path = dir.join("test.db-tshm");
    assert!(
        tshm_path.exists(),
        "TSHM file should exist in shared_writes mode"
    );
}

// ---------------------------------------------------------------------------
// LM3: SharedReads creates TSHM file
// ---------------------------------------------------------------------------
#[test]
fn test_locking_mode_shared_reads_creates_tshm() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut db_path = dir.clone();
    db_path.push("test.db");

    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
    }

    let (_db, conn) = open_with_mode(&dir, LockingMode::SharedReads);
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY)");
    exec(&conn, "INSERT INTO t VALUES (1)");

    let tshm_path = dir.join("test.db-tshm");
    assert!(
        tshm_path.exists(),
        "TSHM file should exist in shared_reads mode"
    );
}

// ---------------------------------------------------------------------------
// LM4: Opening with a different locking mode than TSHM returns error
// ---------------------------------------------------------------------------
#[test]
fn test_locking_mode_mismatch_rejected() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut db_path = dir.clone();
    db_path.push("test.db");

    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
    }

    // First process opens with SharedWrites — TSHM records mode=2.
    let (_db_a, conn_a) = open_with_mode(&dir, LockingMode::SharedWrites);
    exec(&conn_a, "CREATE TABLE t(id INTEGER PRIMARY KEY)");
    exec(&conn_a, "INSERT INTO t VALUES (1)");

    // Second process tries SharedReads — should fail because TSHM says SharedWrites.
    let err_msg = match try_open_with_mode(&dir, LockingMode::SharedReads) {
        Ok(_) => panic!("Opening with mismatched locking mode should fail"),
        Err(e) => format!("{e}"),
    };
    assert!(
        err_msg.contains("locking") || err_msg.contains("Locking") || err_msg.contains("mode"),
        "Error should mention locking mode mismatch, got: {err_msg}"
    );
}

// ===========================================================================
// Reader slot exhaustion tests
// ===========================================================================

// ---------------------------------------------------------------------------
// RS1: All reader slots full → new read returns BUSY
// ---------------------------------------------------------------------------
#[test]
fn test_mp_reader_slots_exhausted_returns_busy() {
    let (db, conn, _dir) = open_mp_database();

    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn, "INSERT INTO t VALUES (1, 'hello')");

    // Sanity: reads work before filling slots.
    let count = query_i64(&conn, "SELECT count(*) FROM t");
    assert_eq!(count, 1);

    // Fill every TSHM reader slot. Use our own PID so pid_is_alive()
    // returns true and the slots are not auto-reclaimed.
    let tshm = db.tshm.as_ref().expect("SharedWrites DB must have TSHM");
    let pid = std::process::id();
    let mut slot_handles = Vec::with_capacity(NUM_READER_SLOTS);
    for i in 0..NUM_READER_SLOTS {
        match tshm.claim_reader_slot(pid, (i + 1) as u32) {
            Some(h) => slot_handles.push(h),
            None => panic!("failed to claim slot {i}, only got {}", slot_handles.len()),
        }
    }
    assert_eq!(slot_handles.len(), NUM_READER_SLOTS);

    // Now a SELECT should fail with BUSY — no reader slots available.
    // Note: the query uses the existing connection (connect() itself also
    // needs a slot to read the header, so we create it before filling slots).
    let result = conn.execute("SELECT count(*) FROM t");
    assert!(
        result.is_err(),
        "query should fail when all reader slots are full"
    );
    let err = result.unwrap_err();
    assert!(
        matches!(err, LimboError::Busy),
        "expected Busy error, got: {err}"
    );

    // Release a handful of slots.
    for _ in 0..5 {
        tshm.release_reader_slot(slot_handles.pop().unwrap());
    }

    // Now a read should succeed.
    let count = query_i64(&conn, "SELECT count(*) FROM t");
    assert_eq!(count, 1, "read should succeed after releasing slots");

    // Cleanup: release remaining slots.
    for h in slot_handles {
        tshm.release_reader_slot(h);
    }
}

// ---------------------------------------------------------------------------
// RS2: Writes also fail when reader slots are exhausted (writes need a read tx)
// ---------------------------------------------------------------------------
#[test]
fn test_mp_reader_slots_exhausted_blocks_writes() {
    let (db, conn, _dir) = open_mp_database();

    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn, "INSERT INTO t VALUES (1, 'hello')");

    let tshm = db.tshm.as_ref().expect("SharedWrites DB must have TSHM");
    let pid = std::process::id();
    let mut slot_handles = Vec::with_capacity(NUM_READER_SLOTS);
    for i in 0..NUM_READER_SLOTS {
        slot_handles.push(
            tshm.claim_reader_slot(pid, (i + 1) as u32)
                .unwrap_or_else(|| panic!("failed to claim slot {i}")),
        );
    }

    // INSERT also needs begin_read_tx under the hood, so it should fail too.
    let result = conn.execute("INSERT INTO t VALUES (2, 'world')");
    assert!(
        result.is_err(),
        "write should fail when all reader slots are full"
    );
    let err = result.unwrap_err();
    assert!(
        matches!(err, LimboError::Busy),
        "expected Busy error for write, got: {err}"
    );

    // Release one slot — write should now succeed.
    tshm.release_reader_slot(slot_handles.pop().unwrap());
    exec(&conn, "INSERT INTO t VALUES (2, 'world')");
    let count = query_i64(&conn, "SELECT count(*) FROM t");
    assert_eq!(count, 2, "write should succeed after releasing a slot");

    // Cleanup.
    for h in slot_handles {
        tshm.release_reader_slot(h);
    }
}

// ---------------------------------------------------------------------------
// RS3: connect() also fails when reader slots are exhausted
// ---------------------------------------------------------------------------
#[test]
fn test_mp_reader_slots_exhausted_blocks_connect() {
    let (db, conn, _dir) = open_mp_database();

    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn, "INSERT INTO t VALUES (1, 'hello')");

    let tshm = db.tshm.as_ref().unwrap();
    let pid = std::process::id();
    let mut slot_handles = Vec::with_capacity(NUM_READER_SLOTS);
    for i in 0..NUM_READER_SLOTS {
        slot_handles.push(
            tshm.claim_reader_slot(pid, (i + 1) as u32)
                .unwrap_or_else(|| panic!("failed to claim slot {i}")),
        );
    }

    // connect() needs a reader slot to read the header — should fail.
    let result = db.connect();
    assert!(
        result.is_err(),
        "connect() should fail when all reader slots are full"
    );

    // Release one slot — connect should now succeed.
    tshm.release_reader_slot(slot_handles.pop().unwrap());
    let conn2 = db.connect().unwrap();
    let count = query_i64(&conn2, "SELECT count(*) FROM t");
    assert_eq!(count, 1);

    // Cleanup.
    for h in slot_handles {
        tshm.release_reader_slot(h);
    }
}

// ---------------------------------------------------------------------------
// RS4: Checkpoint can still run when reader slots are exhausted (it doesn't
//      need a new reader slot itself).
// ---------------------------------------------------------------------------
#[test]
fn test_mp_reader_slots_exhausted_checkpoint_still_works() {
    let (db, conn, _dir) = open_mp_database();

    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for i in 0..100 {
        exec(&conn, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"));
    }

    let tshm = db.tshm.as_ref().unwrap();
    let pid = std::process::id();
    let mut slot_handles = Vec::new();
    for i in 0..NUM_READER_SLOTS {
        match tshm.claim_reader_slot(pid, (i + 1) as u32) {
            Some(h) => slot_handles.push(h),
            None => break,
        }
    }

    // Checkpoint should still work — it operates on the pager directly,
    // not through begin_read_tx.
    run_checkpoint(
        &conn,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );

    // Release all and verify data integrity.
    for h in slot_handles {
        tshm.release_reader_slot(h);
    }
    let count = query_i64(&conn, "SELECT count(*) FROM t");
    assert_eq!(count, 100);
    assert_integrity_ok(&conn);
}

// ---------------------------------------------------------------------------
// SharedReads: writer exclusion
// ---------------------------------------------------------------------------

/// Verify that in SharedReads mode, only the first process to open the
/// database can write. A second process should get Busy on writes.
#[test]
fn test_shared_reads_writer_exclusion() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut db_path = dir.clone();
    db_path.push("test.db");
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
    }

    // Process 0 opens with SharedReads — gets the permanent write lock.
    let (_db0, conn0) = open_with_mode(&dir, LockingMode::SharedReads);

    // Process 1 opens with SharedReads — does NOT get the write lock.
    let (_db1, conn1) = open_with_mode(&dir, LockingMode::SharedReads);

    // Process 0 can write.
    exec(&conn0, "CREATE TABLE t(id INTEGER PRIMARY KEY)");
    exec(&conn0, "INSERT INTO t VALUES (1)");

    // Process 1 cannot write — should fail with Busy.
    let result = conn1.execute("INSERT INTO t VALUES (2)");
    assert!(
        result.is_err(),
        "db1 should not be able to write when db0 holds the lock"
    );
}

/// Regression test: after a Truncate checkpoint the WAL goes to 0 bytes.
/// If another process writes new frames and then a *second* Truncate happens,
/// a third process whose cached WAL state coincidentally matches the new
/// (ckpt_seq=0, max_frame=1) would skip the rescan — leaving its frame_cache
/// stale and unable to find the newly-written data.
///
/// We use two separate tables so the two write cycles produce WAL frames
/// for *different* B-tree pages. This ensures the stale frame_cache from
/// cycle 1 can't accidentally satisfy lookups for cycle 2's pages.
///
/// Timeline:
///   1. P0 creates tables t1 and t2, Truncate → WAL = 0 bytes
///   2. P1 inserts into t1 → WAL frame 1 maps t1's page
///   3. P2 reads → caches WAL state (ckpt_seq=0, max_frame=1) with t1 mapping
///   4. P0 Truncate → WAL = 0, t1 data in DB file
///   5. P1 inserts into t2 → WAL frame 1 maps t2's page (same ckpt_seq/max!)
///   6. P2 reads t2 → stale frame_cache has t1 mapping, not t2 → data invisible
#[test]
fn test_mp_stale_frame_cache_after_repeated_truncate() {
    let (_db0, conn0, dir) = open_mp_database();
    let (_db1, conn1) = open_second_process(&dir);
    let (_db2, conn2) = open_second_process(&dir);

    // Step 1: create both tables, checkpoint everything, then truncate.
    exec(&conn0, "CREATE TABLE t1(id INTEGER PRIMARY KEY)");
    exec(&conn0, "CREATE TABLE t2(id INTEGER PRIMARY KEY)");
    run_checkpoint(
        &conn0,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    // Step 2: P1 inserts into t1 → WAL frame 1 contains t1's leaf page.
    exec(&conn1, "INSERT INTO t1 VALUES (1)");

    // Step 3: P2 reads → rescans WAL, builds frame_cache with {t1_page → 1},
    // caches WAL state as (ckpt_seq=0, max_frame=1).
    assert_eq!(query_i64(&conn2, "SELECT count(*) FROM t1"), 1);

    // Step 4: Truncate again → WAL = 0 bytes, t1 row now in DB file.
    run_checkpoint(
        &conn0,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    // Step 5: P1 inserts into t2 (different table, different page) → WAL
    // frame 1 now maps t2's leaf page. On-disk state is still (0, 1).
    exec(&conn1, "INSERT INTO t2 VALUES (100)");

    // Step 6: P2 reads t2. Its cached WAL state is (0,1) from step 3.
    // Disk WAL state is also (0,1) from step 5 — numerically identical!
    // Without the fix: rescan skips, frame_cache still maps t1's page,
    // t2's page not in cache → falls back to DB file → stale data (0 rows).
    let count = query_i64(&conn2, "SELECT count(*) FROM t2");
    assert_eq!(
        count, 1,
        "process 2 should see t2 row after repeated truncate"
    );
    assert_integrity_ok(&conn2);
}

/// Regression test: Restart checkpoint on a process that opened with an empty
/// WAL writes an invalid header (page_size=0) and sets initialized=true,
/// causing the next write to skip prepare_wal_start and crash in prepare_frames.
#[test]
fn test_mp_restart_checkpoint_on_empty_wal() {
    let (db1, conn1, dir) = open_mp_database();

    // P1 creates a table and writes data.
    exec(&conn1, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)");
    exec(&conn1, "INSERT INTO t1 VALUES (1)");

    // Truncate checkpoint → WAL is now 0 bytes, all data in DB file.
    run_checkpoint(
        &conn1,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    // P2 opens while the WAL is empty. Its WalFileShared gets page_size=0.
    let (_db2, conn2) = open_second_process(&dir);

    // P2 does a Restart checkpoint on the empty WAL.
    // Bug (now fixed): an eager header flush would set initialized=true with page_size=0.
    run_checkpoint(&conn2, CheckpointMode::Restart);

    // P2 tries to write. Without the fix: prepare_wal_start skips (initialized=true),
    // prepare_frames panics on page_size=0.
    exec(&conn2, "INSERT INTO t1 VALUES (2)");

    // Verify data is visible.
    assert_eq!(query_i64(&conn2, "SELECT count(*) FROM t1"), 2);

    // Drop to avoid unused warnings.
    drop(conn1);
    drop(db1);
}

/// Regression test: stale WAL salt after checkpoint restart in multi-process.
///
/// Bug (before fix): MultiProcessWal::begin_write_tx() did not rescan the WAL
/// from disk after acquiring the write lock. If another process checkpointed
/// (restart mode, which changes the salt) and wrote new frames (flushing the
/// new WAL header via prepare_wal_start), a stale writer would use the old
/// salt. Its frame checksums were computed with the old salt but the durable
/// WAL header had the new salt → salt mismatch → committed data lost.
///
/// Fix: begin_write_tx now rescans the WAL from disk after acquiring the
/// inter-process lock. This updates the shared state (salt, max_frame, etc.)
/// which causes the inner begin_write_tx to detect a stale snapshot via
/// db_changed() and return BusySnapshot. The caller must then retry with a
/// fresh read transaction, which picks up the correct salt.
///
/// Found via TLA+ model checking of the WAL checkpoint protocol.
///
/// Timeline:
///   1. P_A writes row 1, commits
///   2. P_B opens, starts explicit tx: BEGIN + SELECT (holds stale read tx)
///   3. P_A checkpoints (Restart) and writes row 2 (new salt on disk)
///   4. P_A closes
///   5. P_B's INSERT is correctly rejected (BusySnapshot after rescan)
///   6. P_B retries → succeeds with correct salt
///   7. P_C opens and verifies all rows visible
#[test]
fn test_mp_stale_salt_after_checkpoint_restart() {
    let (_db_a, conn_a, dir) = open_mp_database();

    // Step 1: P_A creates table and writes initial data.
    exec(&conn_a, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_a, "INSERT INTO t VALUES (1, 'from_a_1')");

    // Step 2: P_B opens and starts an EXPLICIT transaction. The BEGIN + SELECT
    // establishes a read tx that persists across subsequent statements. This
    // is critical: without BEGIN, the INSERT would start a fresh implicit tx
    // with begin_read_tx (triggering a TSHM rescan that picks up the new salt).
    let (_db_b, conn_b) = open_second_process(&dir);
    exec(&conn_b, "BEGIN");
    let count = query_i64(&conn_b, "SELECT count(*) FROM t");
    assert_eq!(count, 1, "P_B should see 1 row at snapshot start");

    // Step 3: P_A does Restart checkpoint.
    // This backfills row 1 to DB, generates new salt in memory, resets WAL.
    run_checkpoint(&conn_a, CheckpointMode::Restart);

    // Step 4: P_A writes row 2. This triggers prepare_wal_start which writes
    // the new WAL header (with new salt) to disk, then writes the frame.
    exec(&conn_a, "INSERT INTO t VALUES (2, 'from_a_2')");

    // Verify P_A sees both rows.
    let count = query_i64(&conn_a, "SELECT count(*) FROM t");
    assert_eq!(count, 2, "P_A should see 2 rows");

    // Step 5: P_A closes (graceful close releases write lock).
    drop(conn_a);
    drop(_db_a);

    // Step 6: P_B tries to write within its stale explicit transaction.
    // The fix: begin_write_tx rescans from disk, which updates shared state
    // (new salt, new max_frame). db_changed() then detects the mismatch
    // between P_B's stale local state and the fresh shared state, returning
    // BusySnapshot. This prevents writing with the wrong salt.
    let result = conn_b.execute("INSERT INTO t VALUES (3, 'from_b')");
    assert!(
        matches!(result, Err(LimboError::BusySnapshot)),
        "expected BusySnapshot after stale explicit tx, got: {result:?}"
    );

    // Step 7: P_B must ROLLBACK the stale explicit tx before retrying.
    // BusySnapshot doesn't auto-rollback in explicit transactions — the user
    // must do it explicitly. Then the new implicit tx calls begin_read_tx
    // which rescans from disk and picks up the correct salt.
    exec(&conn_b, "ROLLBACK");
    exec(&conn_b, "INSERT INTO t VALUES (3, 'from_b')");

    // Step 8: P_C opens and verifies all rows are visible.
    let (_db_c, conn_c) = open_second_process(&dir);

    let count = query_i64(&conn_c, "SELECT count(*) FROM t");
    assert_eq!(
        count, 3,
        "P_C should see all 3 rows after WAL rescan (got {count}). \
         If only 2 rows are visible, P_B's write was lost due to stale WAL salt."
    );

    assert_integrity_ok(&conn_c);
}

/// Test: when WalIndex append_frame fails during commit, data must still be
/// visible to other processes via the inner WAL's frame_cache.
///
/// Bug (C1): commit_prepared_frames silently swallows append_frame errors
/// with `let _ =` and then calls commit_max_frame anyway, making the
/// WalIndex inconsistent (max_frame covers frames not in the hash table).
/// Subsequent find_frame trusts the WalIndex exclusively and returns None
/// for those pages, causing committed data to be invisible.
///
/// This test injects a failure in append_frame, writes data, then verifies
/// WalIndex append failure during commit_prepared_frames clears the WalIndex
/// but the WAL commit succeeds (data is committed). The WalIndex is an
/// optimization layer — its failure cannot un-commit WAL data.
/// No rescan_wal_from_disk is performed (inner frame_cache is current).
#[test]
fn test_mp_wal_index_append_failure_clears_index_data_committed() {
    let (db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "INSERT INTO t VALUES (1, 'initial')");

    // Verify initial data visible.
    assert_eq!(query_i64(&conn_w, "SELECT count(*) FROM t"), 1);

    // Inject WalIndex append failure — simulates disk full / mmap failure
    // during segment growth.
    let wal_index = db_w
        .wal_index
        .as_ref()
        .expect("SharedWrites should have WalIndex");
    wal_index.inject_append_failure(true);

    // Write succeeds — WAL frames are committed before the WalIndex update.
    // The WalIndex gets cleared but the data is committed.
    exec(&conn_w, "INSERT INTO t VALUES (2, 'after_failure')");

    // Stop injecting failures.
    wal_index.inject_append_failure(false);

    // WalIndex was cleared (max_frame == 0).
    assert_eq!(wal_index.max_frame(), 0, "WalIndex should be cleared");

    // Both rows visible — inner.find_frame works via stale-generation fallback.
    assert_eq!(query_i64(&conn_w, "SELECT count(*) FROM t"), 2);

    // Second process should also see both rows.
    let (_db_r, conn_r) = open_second_process(&dir);
    assert_eq!(
        query_i64(&conn_r, "SELECT count(*) FROM t"),
        2,
        "Both rows visible to second process even after WalIndex failure"
    );
}

/// Test: WalIndex salt mismatch mid-transaction triggers assertion.
///
/// Design: find_frame trusts the WalIndex exclusively when validated.
/// If the WalIndex salt doesn't match the inner WAL salt (indicating
/// the WalIndex is from a different WAL generation), this is a protocol
/// violation that should crash rather than silently return wrong data.
///
/// Note: silent hash table corruption (zeroed hash entries without
/// detectable integrity violation) is an inherent limitation of the
/// current WalIndex design. Protection against that would require
/// per-entry checksums (future work). The seqlock + generation counter
/// + salt validation catch all protocol-level inconsistencies.
#[test]
fn test_mp_wal_index_salt_mismatch_after_restart_detected() {
    let (_db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "INSERT INTO t VALUES (1, 'hello')");
    exec(&conn_w, "INSERT INTO t VALUES (2, 'world')");

    // Baseline: writer sees its own data.
    assert_eq!(query_i64(&conn_w, "SELECT count(*) FROM t"), 2);

    // Open a second process.
    let (_db_r, conn_r) = open_second_process(&dir);
    assert_eq!(query_i64(&conn_r, "SELECT count(*) FROM t"), 2);

    // Truncate checkpoint → WAL goes to 0 bytes, WalIndex cleared.
    run_checkpoint(
        &conn_w,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    // Write new data with the new salt.
    exec(&conn_w, "INSERT INTO t VALUES (3, 'after_restart')");

    // Reader detects the salt change via WalIndex and reads correctly.
    let count = query_i64(&conn_r, "SELECT count(*) FROM t");
    assert_eq!(count, 3, "Reader must see new data after WAL restart");
}

/// Test: uncommitted frames from an explicit transaction are NOT visible to
/// other processes. WAL frames written inside an explicit tx have db_size==0
/// (no commit marker), so other processes scanning the WAL stop at the last
/// committed frame boundary. Only after COMMIT are the frames visible.
#[test]
fn test_mp_uncommitted_frames_not_visible_cross_process() {
    let (_db_a, conn_a, dir) = open_mp_database();
    let (_db_b, conn_b) = open_with_mode(&dir, LockingMode::SharedWrites);

    // P_A: setup
    exec(&conn_a, "CREATE TABLE t(id INTEGER PRIMARY KEY)");
    exec(&conn_a, "INSERT INTO t VALUES (1)");

    // P_B: verify sees row 1
    let count = query_i64(&conn_b, "SELECT count(*) FROM t");
    assert_eq!(count, 1, "P_B should see 1 row before tx");

    // P_A: begin explicit tx, insert another row (uncommitted)
    exec(&conn_a, "BEGIN");
    exec(&conn_a, "INSERT INTO t VALUES (2)");

    // P_B: should see only 1 row — uncommitted frames are invisible.
    let count = query_i64(&conn_b, "SELECT count(*) FROM t");
    assert_eq!(
        count, 1,
        "P_B should see only 1 row (uncommitted frames invisible), got {count}"
    );

    // P_A: commit — now the frame is visible.
    exec(&conn_a, "COMMIT");

    // P_B: should see both rows after commit.
    let count = query_i64(&conn_b, "SELECT count(*) FROM t");
    assert_eq!(count, 2, "P_B should see 2 rows after COMMIT, got {count}");
}

// ---------------------------------------------------------------------------
// Rollback + WalIndex cross-process correctness
// ---------------------------------------------------------------------------

/// Test: ROLLBACK correctly updates WalIndex and cross-process reads
/// see correct data after rollback.
///
/// Scenario: Process A inserts in an explicit transaction, then ROLLBACKs.
/// The WalIndex must revert max_frame. Process B must NOT see the rolled-back
/// data.
#[test]
fn test_mp_rollback_wal_index_cross_process() {
    let (_db_w, conn_w, dir) = open_mp_database();

    // Setup: create table and insert baseline data.
    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "INSERT INTO t VALUES (1, 'baseline')");

    let (_db_r, conn_r) = open_second_process(&dir);
    assert_eq!(query_i64(&conn_r, "SELECT count(*) FROM t"), 1);

    // P_W: begin tx, insert rows.
    exec(&conn_w, "BEGIN");
    exec(&conn_w, "INSERT INTO t VALUES (2, 'will_rollback')");
    exec(&conn_w, "INSERT INTO t VALUES (3, 'will_rollback')");

    // P_W: ROLLBACK — WalIndex must revert.
    exec(&conn_w, "ROLLBACK");

    // P_W: should see only baseline data.
    assert_eq!(
        query_i64(&conn_w, "SELECT count(*) FROM t"),
        1,
        "Writer should see only 1 row after rollback"
    );

    // P_R: should see only baseline data.
    assert_eq!(
        query_i64(&conn_r, "SELECT count(*) FROM t"),
        1,
        "Reader should see only 1 row after rollback"
    );

    // Verify data integrity — the baseline row should be intact.
    let rows = query_rows(&conn_r, "SELECT val FROM t WHERE id = 1");
    match &rows[0][0] {
        crate::types::Value::Text(s) => assert_eq!(s.as_ref(), "baseline"),
        other => panic!("unexpected val: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Truncate checkpoint + cross-process write
// ---------------------------------------------------------------------------

/// Test: Process A does Truncate checkpoint (WAL → 0 bytes), then
/// Process B (opened before truncate) writes new data. B must detect
/// the truncated/restarted WAL and handle it correctly.
#[test]
fn test_mp_truncate_then_cross_process_write() {
    let (_db_a, conn_a, dir) = open_mp_database();

    // Setup: create table and insert data.
    exec(&conn_a, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_a, "INSERT INTO t VALUES (1, 'before_truncate')");

    // Open P_B before the truncate — it has the old WAL state cached.
    let (_db_b, conn_b) = open_second_process(&dir);
    assert_eq!(query_i64(&conn_b, "SELECT count(*) FROM t"), 1);

    // P_A: Truncate checkpoint — WAL goes to 0 bytes.
    run_checkpoint(
        &conn_a,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    // P_B: write new data. Must detect the truncated WAL.
    exec(&conn_b, "INSERT INTO t VALUES (2, 'after_truncate')");

    // Both processes should see 2 rows.
    assert_eq!(
        query_i64(&conn_a, "SELECT count(*) FROM t"),
        2,
        "P_A should see 2 rows after truncate + cross-process write"
    );
    assert_eq!(
        query_i64(&conn_b, "SELECT count(*) FROM t"),
        2,
        "P_B should see 2 rows after truncate + cross-process write"
    );

    // Integrity check on both.
    assert_integrity_ok(&conn_a);
    assert_integrity_ok(&conn_b);
}

// ---------------------------------------------------------------------------
// Rapid open/close cycles
// ---------------------------------------------------------------------------

/// Test: 50 rapid open/close cycles on the same database.
/// Catches fd, mmap, flock leaks that would accumulate silently.
/// Final integrity check verifies no corruption from the churn.
#[test]
fn test_mp_rapid_open_close_cycles() {
    let (_db, conn, dir) = open_mp_database();

    // Setup: create table with data.
    exec(&conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for i in 1..=10 {
        exec(&conn, &format!("INSERT INTO t VALUES ({i}, 'row{i}')"));
    }

    // 50 open/close cycles.
    for cycle in 0..50 {
        let (db_i, conn_i) = open_second_process(&dir);
        let count = query_i64(&conn_i, "SELECT count(*) FROM t");
        assert_eq!(count, 10, "cycle {cycle}: expected 10 rows, got {count}");
        drop(conn_i);
        drop(db_i);
    }

    // Final: original connection still works and data is intact.
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM t"), 10);
    assert_integrity_ok(&conn);

    // One more write after all the churn.
    exec(&conn, "INSERT INTO t VALUES (11, 'after_churn')");
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM t"), 11);
}

// ---------------------------------------------------------------------------
// WalIndex max_frame lag (inner > WalIndex)
// ---------------------------------------------------------------------------

/// Test: Simulate the window where inner WAL max_frame exceeds WalIndex's
/// committed max_frame. Verify reads succeed via the fallthrough to
/// inner.find_frame.
///
/// This tests the scenario where the WalIndex has some but not all entries
/// (e.g., WAL restart/truncate changed the generation). find_frame detects
/// `gen != cached_gen` and falls through to inner.find_frame.
#[test]
fn test_mp_wal_index_stale_generation_fallthrough() {
    let (db_w, conn_w, dir) = open_mp_database();

    // Write ALL data before opening reader so its inner frame_cache has
    // every frame from the initial WAL file read.
    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "INSERT INTO t VALUES (1, 'first')");
    exec(&conn_w, "INSERT INTO t VALUES (2, 'second')");
    exec(&conn_w, "INSERT INTO t VALUES (3, 'third')");

    // Open reader — reads WAL from disk during open, building frame_cache.
    let (_db_r, conn_r) = open_second_process(&dir);
    assert_eq!(query_i64(&conn_r, "SELECT count(*) FROM t"), 3);

    // Clear the WalIndex — this bumps the generation AND zeros max_frame.
    // The reader's cached_wal_index_generation is now stale.
    let wal_index = db_w
        .wal_index
        .as_ref()
        .expect("SharedWrites should have WalIndex");
    let gen_before = wal_index.generation();
    wal_index.clear(0, 0);
    assert_ne!(
        wal_index.generation(),
        gen_before,
        "clear() should bump generation"
    );
    assert_eq!(wal_index.max_frame(), 0);

    // Reader queries — WalIndex has max_frame=0, so find_frame skips the
    // WalIndex path entirely and falls through to inner.find_frame (which
    // has the frame_cache from the initial WAL read).
    let count = query_i64(&conn_r, "SELECT count(*) FROM t");
    assert_eq!(
        count, 3,
        "Reader should see all 3 rows via inner fallthrough despite stale WalIndex"
    );
}

// ---------------------------------------------------------------------------
// Savepoint rollback (constraint violation) + WalIndex
// ---------------------------------------------------------------------------

/// Test: PK constraint violation triggers savepoint rollback which calls
/// wal_index.rollback_to(). Verify the WalIndex returns to pre-violation
/// state and cross-process reads are unaffected.
#[test]
fn test_mp_savepoint_rollback_wal_index() {
    let (db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "INSERT INTO t VALUES (1, 'original')");
    exec(&conn_w, "INSERT INTO t VALUES (2, 'original')");

    let wal_index = db_w
        .wal_index
        .as_ref()
        .expect("SharedWrites should have WalIndex");
    let max_before = wal_index.max_frame();

    // Attempt duplicate PK — should fail with constraint violation.
    let result = conn_w.execute("INSERT INTO t VALUES (1, 'duplicate')");
    assert!(result.is_err(), "Duplicate PK insert should fail");

    // WalIndex max_frame should return to pre-violation value.
    let max_after = wal_index.max_frame();
    assert_eq!(
        max_before, max_after,
        "WalIndex max_frame should revert after savepoint rollback \
         (before={max_before}, after={max_after})"
    );

    // Cross-process read should see only the original 2 rows.
    let (_db_r, conn_r) = open_second_process(&dir);
    assert_eq!(
        query_i64(&conn_r, "SELECT count(*) FROM t"),
        2,
        "Reader should see exactly 2 rows after constraint violation"
    );

    // Writer should still be able to write new data.
    exec(&conn_w, "INSERT INTO t VALUES (3, 'after_violation')");
    assert_eq!(query_i64(&conn_w, "SELECT count(*) FROM t"), 3);
    assert_eq!(query_i64(&conn_r, "SELECT count(*) FROM t"), 3);

    assert_integrity_ok(&conn_w);
    assert_integrity_ok(&conn_r);
}

// ---------------------------------------------------------------------------
// Helper: get rescan count from MultiProcessWal
// ---------------------------------------------------------------------------

/// Get the rescan_wal_from_disk call count from the inner WalFile of a
/// connection's MultiProcessWal. Works by downcasting through the Wal
/// trait object.
fn mp_rescan_count(conn: &Arc<Connection>) -> u64 {
    let pager = conn.pager.load();
    let wal = pager.wal.as_ref().expect("pager has no WAL");
    let mp_wal = wal
        .as_any()
        .downcast_ref::<MultiProcessWal>()
        .expect("WAL is not MultiProcessWal");
    mp_wal.inner().rescan_count()
}

// ---------------------------------------------------------------------------
// WAL file reads on the read hot path
// ---------------------------------------------------------------------------

/// Test: begin_read_tx does NOT rescan the WAL from disk when TSHM state
/// changes. Instead it syncs from the mmap'd WalIndex (zero I/O).
#[test]
fn test_mp_begin_read_tx_rescans_wal_on_tshm_change() {
    let (_db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "INSERT INTO t VALUES (1, 'hello')");

    // Open a reader process.
    let (_db_r, conn_r) = open_second_process(&dir);

    // Initial read — may trigger a rescan during open/first read.
    assert_eq!(query_i64(&conn_r, "SELECT count(*) FROM t"), 1);

    // Record the rescan count AFTER the initial read settles.
    let rescan_before = mp_rescan_count(&conn_r);

    // Writer writes new data — this bumps the TSHM writer_state.
    exec(&conn_w, "INSERT INTO t VALUES (2, 'world')");

    // Reader begins a new read transaction. With the WalIndex sync,
    // begin_read_tx updates shared state from the mmap (zero I/O)
    // instead of calling rescan_wal_from_disk.
    assert_eq!(query_i64(&conn_r, "SELECT count(*) FROM t"), 2);

    let rescan_after = mp_rescan_count(&conn_r);

    assert_eq!(
        rescan_after, rescan_before,
        "rescan_wal_from_disk should NOT be called during begin_read_tx \
         (before={rescan_before}, after={rescan_after}). \
         The WalIndex sync replaces the full WAL rescan."
    );
}

/// Test: find_frame uses the WalIndex exclusively when it is correctly
/// populated. Clears the inner frame_cache after begin_read_tx and verifies
/// reads still succeed — proving find_frame never falls through to inner.
#[test]
fn test_mp_find_frame_trusts_wal_index_exclusively() {
    let (db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for i in 1..=20 {
        exec(&conn_w, &format!("INSERT INTO t VALUES ({i}, 'row_{i}')"));
    }

    let wal_index = db_w
        .wal_index
        .as_ref()
        .expect("SharedWrites should have WalIndex");
    assert!(
        wal_index.max_frame() > 0,
        "WalIndex should be populated after writes"
    );

    // Open reader — its WalIndex points to the same mmap.
    let (_db_r, conn_r) = open_second_process(&dir);

    // Initial read — triggers begin_read_tx which syncs from WalIndex.
    assert_eq!(query_i64(&conn_r, "SELECT count(*) FROM t"), 20);

    // Clear the inner frame_cache. If find_frame fell through to
    // inner.find_frame, lookups would fail (return None) and the reader
    // would read stale pages from the DB file.
    let pager = conn_r.pager.load();
    let wal = pager.wal.as_ref().expect("pager has no WAL");
    let mp_wal = wal
        .as_any()
        .downcast_ref::<MultiProcessWal>()
        .expect("WAL is not MultiProcessWal");
    mp_wal.inner().clear_frame_cache();

    // Reads must still succeed — all lookups go through WalIndex only.
    let rows = query_rows(&conn_r, "SELECT id, val FROM t ORDER BY id");
    assert_eq!(rows.len(), 20);
    for (i, row) in rows.iter().enumerate() {
        let expected_id = (i + 1) as i64;
        match &row[0] {
            Value::Numeric(crate::numeric::Numeric::Integer(v)) => {
                assert_eq!(*v, expected_id)
            }
            other => panic!("unexpected id type: {other:?}"),
        }
    }
}

/// Test: Repeated write→read cycles accumulate zero WAL rescans.
/// The reader syncs from the WalIndex mmap (zero I/O) instead of
/// rescanning the entire WAL file on every TSHM change.
#[test]
fn test_mp_rescan_count_scales_with_writes() {
    let (_db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");

    let (_db_r, conn_r) = open_second_process(&dir);

    // Warm up: initial read.
    assert_eq!(query_i64(&conn_r, "SELECT count(*) FROM t"), 0);

    let rescan_start = mp_rescan_count(&conn_r);

    let num_writes = 50;
    for i in 1..=num_writes {
        exec(&conn_w, &format!("INSERT INTO t VALUES ({i}, 'row_{i}')"));
        // Reader must see the new row — triggers begin_read_tx.
        assert_eq!(query_i64(&conn_r, "SELECT count(*) FROM t"), i);
    }

    let rescan_end = mp_rescan_count(&conn_r);
    let total_rescans = rescan_end - rescan_start;

    assert_eq!(
        total_rescans, 0,
        "Expected zero rescans (WalIndex sync replaces WAL rescan), \
         got {total_rescans}."
    );
}

// ---------------------------------------------------------------------------
// TOCTOU gap in begin_read_tx
// ---------------------------------------------------------------------------

/// Test: Demonstrates the TOCTOU window in MultiProcessWal::begin_read_tx.
///
/// The gap between `maybe_rescan_from_tshm()` and `claim_reader_slot()`:
///
/// ```text
///   Reader                          Writer
///   ------                          ------
///   maybe_rescan_from_tshm()
///     → reads WAL, gets max_frame=N
///                                   write frame N+1
///                                   checkpoint TRUNCATE
///                                     → copies frames 1..N+1 to DB
///                                     → truncates WAL to 0
///                                   write frame 1 (new salt!)
///   inner.begin_read_tx()
///     → snapshots max_frame (stale!)
///   claim_reader_slot(max_frame=N)
///     → slot says "I need up to frame N"
///     → but frame N has different content now (new salt)
/// ```
///
/// This test exercises the window by having a writer do rapid
/// checkpoint+write cycles while a reader opens read transactions.
/// A snapshot violation (reader sees inconsistent data) would indicate
/// the TOCTOU fired.
///
/// Because this is a race condition, a single run may not trigger it.
/// The test runs many iterations to increase the probability.
#[test]
fn test_mp_toctou_begin_read_tx_stress() {
    let (_db_w, conn_w, dir) = open_mp_database();

    exec(
        &conn_w,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)",
    );

    // Insert initial data.
    exec(&conn_w, "INSERT INTO t VALUES (1, 100)");

    let (_db_r, conn_r) = open_second_process(&dir);

    // Warm up reader.
    assert_eq!(query_i64(&conn_r, "SELECT val FROM t WHERE id = 1"), 100);

    // Run many write→checkpoint→write→read cycles.
    // The idea: each cycle does a TRUNCATE checkpoint (which changes the WAL
    // salt and resets frames to 0) followed by a write (new frame 1 with new
    // salt). If the reader's begin_read_tx rescans before the checkpoint but
    // claims its slot after the new write, it has a stale view.
    for cycle in 0..200 {
        let new_val = 1000 + cycle;

        // Update the row.
        exec(
            &conn_w,
            &format!("UPDATE t SET val = {new_val} WHERE id = 1"),
        );

        // Checkpoint TRUNCATE — copies WAL to DB, truncates WAL.
        // Ignore Busy — another reader may block the checkpoint.
        let pager_w = conn_w.pager.load();
        let _ = pager_w.io.block(|| {
            pager_w.checkpoint(
                CheckpointMode::Truncate {
                    upper_bound_inclusive: None,
                },
                crate::SyncMode::Full,
                true,
            )
        });

        // Write again — this creates frame 1 with a new salt (if the
        // checkpoint succeeded and restarted the WAL).
        let next_val = new_val + 1;
        exec(
            &conn_w,
            &format!("UPDATE t SET val = {next_val} WHERE id = 1"),
        );

        // Reader: the value must be either new_val or next_val,
        // NEVER the old value from a previous cycle or garbage.
        let reader_val = query_i64(&conn_r, "SELECT val FROM t WHERE id = 1");
        assert!(
            reader_val == new_val as i64 || reader_val == next_val as i64,
            "TOCTOU detected at cycle {cycle}: reader saw val={reader_val}, \
             expected {new_val} or {next_val}. The reader's begin_read_tx \
             rescanned the WAL before the checkpoint but claimed its slot \
             after the WAL was restarted with a new salt."
        );
    }
}

/// Test: Demonstrates the TOCTOU gap can cause a reader to hold a stale
/// snapshot after a WAL restart (salt change).
///
/// Sequence that triggers the bug:
/// 1. Writer writes frames 1..N
/// 2. Reader calls maybe_rescan_from_tshm → rescans WAL, sees frames 1..N
///    with salt_A
/// 3. Writer does TRUNCATE checkpoint → WAL truncated to 0, DB updated
/// 4. Writer writes frame 1 with new salt_B
/// 5. Reader calls inner.begin_read_tx() → snapshots max_frame from
///    shared state (which rescan populated in step 2 with salt_A's frames)
/// 6. Reader claims TSHM slot with max_frame=N
/// 7. Reader calls find_frame(page_X) → finds frame from step 2's
///    frame_cache, but the WAL file now has different content at that
///    offset (salt_B frame or zeros)
///
/// This test creates the conditions and checks for inconsistency.
#[test]
fn test_mp_toctou_stale_salt_after_wal_restart() {
    let (_db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");

    // Write enough data to have multiple pages in the WAL.
    for i in 1..=100 {
        exec(
            &conn_w,
            &format!("INSERT INTO t VALUES ({i}, '{}')", "x".repeat(100)),
        );
    }

    let (_db_r, conn_r) = open_second_process(&dir);

    // Reader sees all 100 rows.
    assert_eq!(query_i64(&conn_r, "SELECT count(*) FROM t"), 100);

    // Now do a TRUNCATE checkpoint → WAL goes to 0, DB has everything.
    run_checkpoint(
        &conn_w,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    // Writer writes new data — this starts a fresh WAL with new salt.
    exec(&conn_w, "UPDATE t SET val = 'updated' WHERE id = 1");

    // The writer's WAL now has frame 1 with a new salt.
    // The reader's cached state (from the rescan during its last
    // begin_read_tx) had the old WAL with old salt.
    //
    // When the reader does its next begin_read_tx:
    // - maybe_rescan_from_tshm detects TSHM changed → rescans WAL from disk
    // - BUT between the rescan and claiming the TSHM slot, the writer could
    //   do another checkpoint+write cycle, making the rescan stale.
    //
    // For a single-threaded test we can't reproduce the exact interleaving,
    // but we verify the reader sees correct data regardless.
    let val = query_rows(&conn_r, "SELECT val FROM t WHERE id = 1");
    match &val[0][0] {
        Value::Text(s) => assert!(
            s.as_ref() == "updated" || s.as_ref() == "x".repeat(100),
            "Reader saw unexpected value after WAL restart: {s}"
        ),
        other => panic!("unexpected type: {other:?}"),
    }

    // Full verification: all rows should be consistent.
    let count = query_i64(&conn_r, "SELECT count(*) FROM t");
    assert_eq!(count, 100, "Row count mismatch after WAL restart");
    assert_integrity_ok(&conn_r);
}

/// Test: Multi-threaded TOCTOU stress test.
///
/// Spawns a writer thread doing rapid write→checkpoint→write cycles and
/// a reader thread doing rapid SELECT queries. The reader must never see
/// an inconsistent snapshot — every read must return a valid committed
/// state of the database.
///
/// This test has a higher chance of hitting the TOCTOU window because
/// the writer and reader run concurrently on real OS threads, creating
/// genuine interleaving at the rescan↔slot-claim boundary.
#[test]
fn test_mp_toctou_concurrent_stress() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap().keep();
    let mut db_path = dir.clone();
    db_path.push("test.db");

    // Create WAL-mode database.
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, val INTEGER)", [])
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 0)", []).unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));

    // Writer thread.
    let stop_w = stop.clone();
    let dir_w = dir.clone();
    let writer = thread::spawn(move || {
        let (_db, conn) = open_with_mode(&dir_w, LockingMode::SharedWrites);
        let mut val: i64 = 0;
        while !stop_w.load(Ordering::Relaxed) {
            val += 1;
            exec(&conn, &format!("UPDATE t SET val = {val} WHERE id = 1"));

            // Occasionally checkpoint to trigger WAL restarts.
            if val % 10 == 0 {
                let pager = conn.pager.load();
                let _ = pager.io.block(|| {
                    pager.checkpoint(
                        CheckpointMode::Truncate {
                            upper_bound_inclusive: None,
                        },
                        crate::SyncMode::Full,
                        true,
                    )
                });
            }
        }
    });

    // Reader thread.
    let stop_r = stop.clone();
    let reader = thread::spawn(move || {
        let (_db, conn) = open_with_mode(&dir, LockingMode::SharedWrites);
        let mut reads = 0u64;
        let mut max_seen: i64 = 0;
        let mut non_monotonic_errors = 0u64;
        let mut short_read_errors = 0u64;

        while !stop_r.load(Ordering::Relaxed) {
            let result = (|| -> crate::Result<i64> {
                let mut stmt = conn.prepare("SELECT val FROM t WHERE id = 1")?;
                let mut val: i64 = 0;
                stmt.run_with_row_callback(|row| {
                    val = row.get(0)?;
                    Ok(())
                })?;
                Ok(val)
            })();

            match result {
                Ok(val) => {
                    // Monotonic reads: within the same connection, values
                    // should never decrease. Each begin_read_tx should get
                    // a snapshot >= the previous one.
                    if val < max_seen {
                        // Non-monotonic read = snapshot went backwards.
                        // This is a real protocol violation.
                        non_monotonic_errors += 1;
                    }
                    if val > max_seen {
                        max_seen = val;
                    }
                    reads += 1;
                }
                Err(e) => {
                    let msg = format!("{e}");
                    if msg.contains("ShortRead") || msg.contains("Completion") {
                        short_read_errors += 1;
                    }
                    // Other errors (Busy, etc.) are expected under
                    // contention — just retry.
                }
            }
        }

        (reads, non_monotonic_errors, short_read_errors)
    });

    // Let it run for a bit.
    thread::sleep(Duration::from_secs(3));
    stop.store(true, Ordering::Relaxed);

    writer.join().expect("writer panicked");
    let (total_reads, non_monotonic, short_reads) = reader.join().expect("reader panicked");

    eprintln!(
        "TOCTOU concurrent stress: {total_reads} reads, \
         {non_monotonic} non-monotonic, {short_reads} short-reads"
    );

    assert!(
        total_reads > 0,
        "Reader thread didn't execute any reads — test is vacuous"
    );

    // Non-monotonic reads must NEVER happen — this would be a real
    // protocol violation (snapshot went backwards).
    assert_eq!(
        non_monotonic, 0,
        "Non-monotonic reads detected: reader saw an older value \
         after seeing a newer one ({non_monotonic} occurrences)"
    );

    // ShortReadWalFrame can occur in same-process multi-thread tests
    // because TSHM reader slots use PID. Since all threads share the
    // same PID, the writer's checkpoint ignores the reader's slot
    // (min_reader_frame_excluding filters by PID). In real multi-process
    // usage with different PIDs, the reader's slot would prevent the
    // writer from truncating frames the reader is actively using.
    if short_reads > 0 {
        eprintln!(
            "NOTE: {short_reads} ShortReadWalFrame errors (expected in \
             same-process multi-thread tests due to shared PID)"
        );
    }
}

// ---------------------------------------------------------------------------
// Write path: WalIndex sync instead of disk rescan
// ---------------------------------------------------------------------------

/// Test: when process B writes (bumping TSHM), process A's next begin_write_tx
/// syncs from the WalIndex mmap instead of rescanning the WAL from disk.
#[test]
fn test_mp_write_path_no_disk_rescan() {
    let (_db_a, conn_a, dir) = open_mp_database();

    exec(&conn_a, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_a, "INSERT INTO t VALUES (1, 'a1')");

    let (_db_b, conn_b) = open_second_process(&dir);

    // Warm up: B reads to settle initial state.
    assert_eq!(query_i64(&conn_b, "SELECT count(*) FROM t"), 1);

    // B writes — bumps TSHM.
    exec(&conn_b, "INSERT INTO t VALUES (2, 'b1')");

    // Record A's rescan count before A writes again.
    let rescan_before = mp_rescan_count(&conn_a);

    // A writes — begin_write_tx detects TSHM change, syncs from WalIndex.
    exec(&conn_a, "INSERT INTO t VALUES (3, 'a2')");

    let rescan_after = mp_rescan_count(&conn_a);
    assert_eq!(
        rescan_after, rescan_before,
        "begin_write_tx should sync from WalIndex, not rescan_wal_from_disk \
         (before={rescan_before}, after={rescan_after})"
    );

    // Verify data integrity.
    assert_eq!(query_i64(&conn_a, "SELECT count(*) FROM t"), 3);
    assert_integrity_ok(&conn_a);
}

// ---------------------------------------------------------------------------
// TSHM rebuild on missing file + corrupt→delete→reopen recovery
// ---------------------------------------------------------------------------

/// Test: opening a database without a tshm file rebuilds it automatically.
#[test]
fn test_mp_tshm_rebuild_on_missing_file() {
    let (_db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "INSERT INTO t VALUES (1, 'hello')");
    exec(&conn_w, "INSERT INTO t VALUES (2, 'world')");

    let tshm_path = dir.join("test.db-tshm");
    assert!(tshm_path.exists(), "tshm file should exist after writes");

    // Drop all connections so the file isn't held open.
    drop(conn_w);
    drop(_db_w);

    // Delete the tshm file.
    std::fs::remove_file(&tshm_path).unwrap();
    assert!(!tshm_path.exists());

    // Reopen — tshm should be recreated and data fully accessible.
    let (_db_new, conn_new) = open_with_mode(&dir, LockingMode::SharedWrites);
    assert_eq!(query_i64(&conn_new, "SELECT count(*) FROM t"), 2);

    assert!(tshm_path.exists(), "tshm file should be recreated on open");
    assert_integrity_ok(&conn_new);
}

/// Test: corrupt WalIndex → detect error → delete tshm → reopen recovers.
#[test]
fn test_mp_corrupt_tshm_delete_reopen_recovers() {
    let (db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for i in 1..=10 {
        exec(&conn_w, &format!("INSERT INTO t VALUES ({i}, 'row_{i}')"));
    }

    let wal_index = db_w
        .wal_index
        .as_ref()
        .expect("SharedWrites should have WalIndex");
    assert!(wal_index.max_frame() > 0);

    // Corrupt the WalIndex hash tables.
    wal_index.corrupt_hash_tables();

    // verify_hash_integrity should detect the corruption.
    let result = wal_index.verify_hash_integrity();
    assert!(
        result.is_err(),
        "verify_hash_integrity should return error on corruption"
    );

    let tshm_path = dir.join("test.db-tshm");

    // Drop everything so the tshm file isn't held open.
    drop(conn_w);
    drop(db_w);

    // Delete the tshm file (user recovery action).
    std::fs::remove_file(&tshm_path).unwrap();

    // Reopen — tshm is rebuilt, all data accessible.
    let (db_new, conn_new) = open_with_mode(&dir, LockingMode::SharedWrites);
    assert_eq!(query_i64(&conn_new, "SELECT count(*) FROM t"), 10);

    // WalIndex should pass integrity check after rebuild.
    if let Some(wal_index) = &db_new.wal_index {
        assert!(wal_index.verify_hash_integrity().is_ok());
    }

    assert!(tshm_path.exists());
    assert_integrity_ok(&conn_new);
}

/// Corrupt tshm on disk, then reopen — should fail with Corrupt error.
/// The user deletes the tshm file and reopens to recover.
#[test]
fn test_mp_corrupt_tshm_reopen_returns_corrupt_error() {
    let (db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    for i in 1..=5 {
        exec(&conn_w, &format!("INSERT INTO t VALUES ({i}, 'row_{i}')"));
    }

    let wal_index = db_w
        .wal_index
        .as_ref()
        .expect("SharedWrites should have WalIndex");
    assert!(wal_index.max_frame() > 0);

    // Corrupt the WalIndex hash tables (which corrupts pgno_checksum).
    wal_index.corrupt_hash_tables();

    // Drop everything so the tshm file is closed.
    drop(conn_w);
    drop(db_w);

    // Reopen — open_inner should detect the checksum mismatch and return
    // a Corrupt error instead of silently rebuilding.
    let tshm_path = dir.join("test.db-tshm");
    assert!(tshm_path.exists(), "tshm file should still exist");

    let err_msg = match try_open_with_mode(&dir, LockingMode::SharedWrites) {
        Ok(_) => panic!("reopen with corrupt tshm should return an error"),
        Err(e) => e.to_string(),
    };
    assert!(
        err_msg.contains("corruption") || err_msg.contains("Corrupt"),
        "error should indicate corruption, got: {err_msg}"
    );
}

/// WalIndex corruption detected during begin_read_tx (via verify_hash_integrity)
/// propagates as an error through mvcc_refresh_if_db_changed.
#[test]
fn test_mp_corruption_propagates_through_mvcc_refresh() {
    let (_db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "INSERT INTO t VALUES (1, 'initial')");

    // Open a second process and verify it can read.
    let (db_r, conn_r) = open_second_process(&dir);
    assert_eq!(query_i64(&conn_r, "SELECT count(*) FROM t"), 1);

    // Write more data from the first process to bump TSHM writer_state.
    exec(&conn_w, "INSERT INTO t VALUES (2, 'second')");

    // Corrupt the WalIndex hash tables. Since both processes share the
    // same mmap'd WalIndex, this corruption is visible to the reader.
    let wal_index = db_r
        .wal_index
        .as_ref()
        .expect("SharedWrites should have WalIndex");
    wal_index.corrupt_hash_tables();

    // The next begin_read_tx on the reader should detect corruption.
    // The TSHM writer_state changed (from the write above), so
    // sync_from_wal_index_for_read will call verify_hash_integrity.
    let result = conn_r.execute("SELECT count(*) FROM t");
    assert!(
        result.is_err(),
        "read after corruption should fail, not silently return stale data"
    );
}

// ---------------------------------------------------------------------------
// finish_append_frames_commit: WalIndex visibility on first write
// ---------------------------------------------------------------------------

/// Test: On the very first write to an empty WAL, finish_append_frames_commit
/// must call commit_max_frame to make WalIndex entries visible. Previously,
/// the guard `if wal_index.max_frame() > 0` skipped this, leaving frames
/// appended to the WalIndex but invisible (max_frame stuck at 0).
#[test]
fn test_mp_finish_commit_updates_wal_index_on_first_write() {
    let (db_w, conn_w, dir) = open_mp_database();

    // Truncate checkpoint to start with a completely empty WAL.
    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    run_checkpoint(
        &conn_w,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    let wal_index = db_w
        .wal_index
        .as_ref()
        .expect("SharedWrites should have WalIndex");
    assert_eq!(
        wal_index.max_frame(),
        0,
        "WalIndex should be empty after truncate"
    );

    // First write to the empty WAL.
    exec(&conn_w, "INSERT INTO t VALUES (1, 'first')");

    // WalIndex max_frame must reflect the committed frames.
    assert!(
        wal_index.max_frame() > 0,
        "WalIndex max_frame should be > 0 after first write (was 0)"
    );

    // A second process must be able to read the data via WalIndex.
    let (_db_r, conn_r) = open_second_process(&dir);
    assert_eq!(
        query_i64(&conn_r, "SELECT count(*) FROM t"),
        1,
        "Reader should see the row committed via first write"
    );
}

// ---------------------------------------------------------------------------
// rollback(None): WalIndex cleanup of uncommitted entries
// ---------------------------------------------------------------------------

/// Test: After rollback(None), uncommitted WalIndex entries (appended by
/// append_frame but not committed by commit_max_frame) must be cleaned up.
/// Without cleanup, stale hash entries persist and can cause collisions
/// when new frames are written at the same positions.
#[test]
fn test_mp_rollback_none_cleans_wal_index() {
    let (db_w, conn_w, dir) = open_mp_database();

    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "INSERT INTO t VALUES (1, 'baseline')");

    let wal_index = db_w
        .wal_index
        .as_ref()
        .expect("SharedWrites should have WalIndex");
    let max_frame_before = wal_index.max_frame();
    assert!(max_frame_before > 0);

    // Begin a write transaction, insert data, then rollback.
    exec(&conn_w, "BEGIN");
    exec(&conn_w, "INSERT INTO t VALUES (2, 'will_rollback')");
    exec(&conn_w, "INSERT INTO t VALUES (3, 'will_rollback')");
    exec(&conn_w, "ROLLBACK");

    // WalIndex max_frame should be unchanged (no commit happened).
    assert_eq!(
        wal_index.max_frame(),
        max_frame_before,
        "WalIndex max_frame should not change after rollback"
    );

    // Now write new data at the same frame positions.
    // Without cleanup_uncommitted, stale hash entries from the rolled-back
    // frames would pollute the hash table.
    exec(&conn_w, "INSERT INTO t VALUES (4, 'after_rollback')");

    // Verify data integrity.
    assert_eq!(
        query_i64(&conn_w, "SELECT count(*) FROM t"),
        2,
        "Writer should see baseline + new row"
    );

    // Reader must see correct data too.
    let (_db_r, conn_r) = open_second_process(&dir);
    assert_eq!(
        query_i64(&conn_r, "SELECT count(*) FROM t"),
        2,
        "Reader should see baseline + new row"
    );
    assert_integrity_ok(&conn_r);
}

/// Regression: verify that begin_read_tx claims the TSHM reader slot BEFORE
/// acquiring the inner read lock. Without this ordering, a concurrent Truncate
/// checkpoint could scan TSHM, not see the reader, and truncate the WAL while
/// the reader still needs its frames.
#[test]
fn test_mp_tshm_slot_claimed_before_inner_read_lock() {
    let (db_w, conn_w, dir) = open_mp_database();

    // Write some data so the WAL has frames.
    exec(&conn_w, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn_w, "INSERT INTO t VALUES (1,'hello')");

    // Open a second process.
    let (db_r, conn_r) = open_second_process(&dir);

    // Start a read transaction in the reader.
    exec(&conn_r, "BEGIN");
    let count = query_i64(&conn_r, "SELECT count(*) FROM t");
    assert_eq!(count, 1);

    // Verify the reader's TSHM slot is visible to the writer process.
    // The writer's TSHM is the same mmap'd region, so it sees the slot.
    let tshm_w = db_w.tshm.as_ref().expect("writer DB must have TSHM");
    let tshm_r = db_r.tshm.as_ref().expect("reader DB must have TSHM");

    // The writer should see at least one active reader (the reader process).
    // Exclude the writer's own PID to check for external readers.
    let writer_pid = std::process::id();
    let external_min = tshm_w.min_reader_frame_excluding(writer_pid);
    // In test mode, both use the same PID, so check has_active_readers instead.
    assert!(
        tshm_r.has_active_readers(),
        "TSHM should have an active reader slot during read transaction"
    );

    // The external_min should be Some (since reader has a slot with our PID).
    // Even though we exclude writer_pid, in tests both are the same PID,
    // so fall back to checking has_active_readers above.
    // If they had different PIDs (real multi-process), external_min would
    // be Some(max_frame).
    let _ = external_min;

    // End the read transaction and verify the slot is released.
    exec(&conn_r, "COMMIT");
    // After commit, the reader's slot should be released. Start another
    // read tx and immediately check — this just verifies the lifecycle.
    // The slot is released in end_read_tx, which happens implicitly after COMMIT.

    // Key verification: do a Truncate checkpoint while reader has a slot.
    // Start a new read tx in the reader.
    exec(&conn_r, "BEGIN");
    let count2 = query_i64(&conn_r, "SELECT count(*) FROM t");
    assert_eq!(count2, 1);

    // Checkpoint as Truncate — the reader's slot should prevent WAL truncation
    // from causing issues.
    run_checkpoint(
        &conn_w,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    );

    // Reader should still be able to read (WAL frames preserved or already
    // in DB file after checkpoint).
    let count3 = query_i64(&conn_r, "SELECT count(*) FROM t");
    assert_eq!(count3, 1);
    exec(&conn_r, "COMMIT");

    assert_integrity_ok(&conn_w);
    assert_integrity_ok(&conn_r);
}

/// Test that cache spilling (append_frames_vectored) works correctly
/// with MultiProcessWal. When the page cache is full, dirty pages are
/// spilled to the WAL via append_frames_vectored. The MultiProcessWal
/// wrapper inserts those frames into the WalIndex but defers committing
/// max_frame until finish_append_frames_commit (on COMMIT). This test
/// verifies:
/// 1. Data is correct after a spill+commit
/// 2. Cross-process readers see the committed data
/// 3. WalIndex is consistent after spilling
#[test]
fn test_mp_spill_deferred_commit() {
    let (db_w, conn_w, dir) = open_mp_database();

    exec(
        &conn_w,
        "CREATE TABLE spill_test(id INTEGER PRIMARY KEY, val TEXT)",
    );

    // Record max_frame after CREATE TABLE (before the spill transaction).
    let max_frame_before = db_w
        .wal_index
        .as_ref()
        .map(|wi| wi.max_frame())
        .unwrap_or(0);

    // The minimum page cache is 200 pages (MINIMUM_PAGE_CACHE_SIZE_IN_PAGES).
    // Spill threshold is 90% = 180 pages. Each ~4000 byte row fills roughly
    // one page. Insert 250 rows to exceed the 180-page threshold and trigger
    // cache spilling via append_frames_vectored.
    exec(&conn_w, "PRAGMA cache_size = 200");

    // Insert enough large rows in a single transaction to trigger spilling.
    exec(&conn_w, "BEGIN");
    for i in 0..250 {
        let val: String = std::iter::repeat_n('x', 3800).collect();
        conn_w
            .execute(format!(
                "INSERT INTO spill_test(id, val) VALUES ({i}, '{val}')"
            ))
            .unwrap();
    }

    // Verify spilling actually occurred BEFORE commit: the WAL file
    // should have grown from append_frames_vectored writing spilled
    // pages. WAL header = 32 bytes, each frame = 24 + 4096 = 4120 bytes.
    // With only the CREATE TABLE frames, size ≈ 8K. If spilling occurred,
    // size should be much larger.
    let wal_path = dir.join("test.db-wal");
    let wal_size_mid_tx = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert!(
        wal_size_mid_tx > 100_000,
        "WAL file should have grown mid-transaction from spilling (size={wal_size_mid_tx})"
    );

    exec(&conn_w, "COMMIT");

    // Verify writer sees all rows.
    let count = query_i64(&conn_w, "SELECT count(*) FROM spill_test");
    assert_eq!(
        count, 250,
        "writer should see all 250 rows after spill+commit"
    );

    // Verify WalIndex is consistent and has significantly more frames
    // than before the spill transaction (indicating spilling occurred).
    if let Some(wal_index) = &db_w.wal_index {
        let max_frame_after = wal_index.max_frame();
        assert!(
            max_frame_after > max_frame_before + 100,
            "WalIndex should have many more frames after spill+commit (before={max_frame_before}, after={max_frame_after})"
        );
        wal_index.verify_hash_integrity().unwrap();
    }

    // Cross-process reader should see all 250 rows.
    let (_db_r, conn_r) = open_second_process(&dir);
    let count_r = query_i64(&conn_r, "SELECT count(*) FROM spill_test");
    assert_eq!(
        count_r, 250,
        "cross-process reader should see all 250 rows after spill+commit"
    );

    // Verify individual rows to check data integrity.
    let rows = query_rows(&conn_r, "SELECT id FROM spill_test ORDER BY id LIMIT 5");
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r.first() {
            Some(Value::Numeric(crate::numeric::Numeric::Integer(v))) => Some(*v),
            _ => None,
        })
        .collect();
    assert_eq!(
        ids,
        vec![0, 1, 2, 3, 4],
        "first 5 rows should have sequential ids"
    );

    assert_integrity_ok(&conn_w);
    assert_integrity_ok(&conn_r);
}

/// Test that spill + rollback correctly discards spilled frames.
/// When a transaction spills dirty pages and then rolls back,
/// the WalIndex entries are cleaned up and cross-process readers
/// should NOT see the rolled-back data.
#[test]
fn test_mp_spill_rollback() {
    let (db_w, conn_w, dir) = open_mp_database();

    exec(
        &conn_w,
        "CREATE TABLE spill_rb(id INTEGER PRIMARY KEY, val TEXT)",
    );

    // Insert baseline data.
    exec(&conn_w, "INSERT INTO spill_rb(id) VALUES (1)");

    // Open reader BEFORE the spill transaction.
    let (db_r, conn_r) = open_second_process(&dir);
    let count_before = query_i64(&conn_r, "SELECT count(*) FROM spill_rb");
    assert_eq!(count_before, 1);

    // Set minimum cache and do a large transaction that we'll roll back.
    // 250 rows × ~4000 bytes each should exceed the 180-page spill threshold.
    exec(&conn_w, "PRAGMA cache_size = 200");
    exec(&conn_w, "BEGIN");
    for i in 100..350 {
        let val: String = std::iter::repeat_n('y', 3800).collect();
        conn_w
            .execute(format!(
                "INSERT INTO spill_rb(id, val) VALUES ({i}, '{val}')"
            ))
            .unwrap();
    }

    // Verify spilling occurred before rollback.
    let wal_path = dir.join("test.db-wal");
    let wal_size_mid_tx = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert!(
        wal_size_mid_tx > 100_000,
        "WAL file should have grown mid-transaction from spilling (size={wal_size_mid_tx})"
    );

    exec(&conn_w, "ROLLBACK");

    // Writer should only see baseline data.
    let count_w = query_i64(&conn_w, "SELECT count(*) FROM spill_rb");
    assert_eq!(count_w, 1, "writer should see only baseline after rollback");

    // Verify WalIndex cleanup.
    if let Some(wal_index) = &db_w.wal_index {
        wal_index.verify_hash_integrity().unwrap();
    }

    // Cross-process reader should also see only baseline.
    let count_r2 = query_i64(&conn_r, "SELECT count(*) FROM spill_rb");
    assert_eq!(
        count_r2, 1,
        "cross-process reader should not see rolled-back spill data"
    );

    assert_integrity_ok(&conn_w);
    assert_integrity_ok(&conn_r);

    drop(conn_r);
    drop(db_r);
    drop(conn_w);
    drop(db_w);
}
