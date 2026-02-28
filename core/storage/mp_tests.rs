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
fn open_with_mode(dir: &std::path::Path, mode: LockingMode) -> (Arc<Database>, Arc<Connection>) {
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
    let opts = DatabaseOpts::new().with_locking_mode(mode);
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
        )
        .unwrap()
        {
            crate::IOResult::Done(db) => break db,
            crate::IOResult::IO(io_completion) => {
                io_completion.wait(&*io).unwrap();
            }
        }
    };
    let conn = db.connect().unwrap();
    (db, conn)
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
    let db_path_str = db_path.to_str().unwrap();
    let wal_path = format!("{db_path_str}-wal");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let mut flags = OpenFlags::default();
    flags |= OpenFlags::SharedLock;
    let file = io.open_file(db_path_str, flags, true).unwrap();
    let db_file = Arc::new(DatabaseFile::new(file));
    let opts = DatabaseOpts::new().with_locking_mode(LockingMode::SharedReads);
    let mut state = crate::OpenDbAsyncState::new();
    let result = loop {
        match Database::open_with_flags_bypass_registry_async(
            &mut state,
            io.clone(),
            db_path_str,
            Some(&wal_path),
            db_file.clone(),
            flags,
            opts,
            None,
        ) {
            Ok(crate::IOResult::Done(db)) => break Ok(db),
            Ok(crate::IOResult::IO(io_completion)) => {
                io_completion.wait(&*io).unwrap();
            }
            Err(e) => break Err(e),
        }
    };

    assert!(
        result.is_err(),
        "Opening with mismatched locking mode should fail"
    );
    let err_msg = format!("{}", result.unwrap_err());
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

// ===========================================================================
// MVCC multi-process tests
// ===========================================================================

/// Create a fresh MVCC-mode database in a temp directory.
///
/// First creates a WAL-mode database via rusqlite, then switches to MVCC
/// via PRAGMA. The PRAGMA handler does a Truncate checkpoint + writes the
/// MVCC version header to the WAL. A subsequent explicit checkpoint flushes
/// everything (including the Mvcc header page) to the DB file so that
/// second opens auto-detect MVCC mode from the on-disk header.
fn open_mp_mvcc_database() -> (Arc<Database>, Arc<Connection>, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut db_path = dir.clone();
    db_path.push("test.db");

    // Create the database via rusqlite so the file exists and is WAL-mode.
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
    }

    let (db, conn) = open_with_mode(&dir, LockingMode::SharedWrites);
    // Switch to MVCC mode (writes Mvcc version header to WAL).
    exec(&conn, "PRAGMA journal_mode = 'experimental_mvcc'");
    (db, conn, dir)
}

/// Open a "virtual process" on a directory where MVCC mode is already
/// established (DB file header says Mvcc). The MvStore is auto-initialized
/// during open_with_flags_bypass_registry_async.
fn open_second_process_mvcc(dir: &std::path::Path) -> (Arc<Database>, Arc<Connection>) {
    open_with_mode(dir, LockingMode::SharedWrites)
}

// ---------------------------------------------------------------------------
// MVCC1: Basic checkpoint — data visible to second process
// ---------------------------------------------------------------------------

/// Verify that MVCC writes checkpointed by P1 are visible to P2.
///
/// In MVCC mode, writes go to SkipMap + .db-log (not WAL). Checkpoint
/// flushes SkipMap data through the pager/WAL to the DB file, then
/// truncates the WAL and .db-log. A second process opening the DB reads
/// from the clean DB file.
#[test]
fn test_mp_mvcc_basic_checkpoint() {
    let (_db1, conn1, dir) = open_mp_mvcc_database();

    // P1: create table and insert data in MVCC mode.
    exec(&conn1, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn1, "INSERT INTO t VALUES (1, 'hello')");
    exec(&conn1, "INSERT INTO t VALUES (2, 'world')");

    // P1: checkpoint to flush everything (including Mvcc header page) to DB file.
    exec(&conn1, "PRAGMA wal_checkpoint(TRUNCATE)");

    // P2: open the same DB — auto-detects MVCC from header.
    let (_db2, conn2) = open_second_process_mvcc(&dir);

    // P2: verify it sees P1's checkpointed data.
    let count = query_i64(&conn2, "SELECT count(*) FROM t");
    assert_eq!(count, 2, "P2 should see 2 rows from P1's checkpoint");

    assert_integrity_ok(&conn1);
    assert_integrity_ok(&conn2);
}

// ---------------------------------------------------------------------------
// MVCC2: Cross-process visibility after checkpoint with concurrent opens
// ---------------------------------------------------------------------------

/// Both processes are open simultaneously. P1 writes + checkpoints,
/// P2 reads and sees the checkpointed data. Exercises the TSHM-gated
/// rescan path in begin_read_tx + mvcc_refresh_if_db_changed.
#[test]
fn test_mp_mvcc_cross_process_visibility_after_checkpoint() {
    let (_db1, conn1, dir) = open_mp_mvcc_database();

    // Create table and checkpoint so P2 sees the schema.
    exec(&conn1, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn1, "PRAGMA wal_checkpoint(TRUNCATE)");

    // P2: open while P1 is still alive.
    let (_db2, conn2) = open_second_process_mvcc(&dir);

    // P2: initial read — should see empty table.
    let count = query_i64(&conn2, "SELECT count(*) FROM t");
    assert_eq!(count, 0, "P2 should see empty table initially");

    // P1: insert rows and checkpoint. This bumps TSHM writer_state.
    exec(&conn1, "INSERT INTO t VALUES (1, 'alpha')");
    exec(&conn1, "INSERT INTO t VALUES (2, 'beta')");
    exec(&conn1, "PRAGMA wal_checkpoint(TRUNCATE)");

    // P2: read again — should see P1's data.
    // The autocommit path calls begin_read_tx (TSHM check → rescan)
    // followed by mvcc_refresh_if_db_changed (page cache invalidation).
    let count = query_i64(&conn2, "SELECT count(*) FROM t");
    assert_eq!(
        count, 2,
        "P2 should see 2 rows after P1's checkpoint, got {count}"
    );

    // P1: insert more rows and checkpoint again.
    exec(&conn1, "INSERT INTO t VALUES (3, 'gamma')");
    exec(&conn1, "PRAGMA wal_checkpoint(TRUNCATE)");

    // P2: should see all 3 rows.
    let count = query_i64(&conn2, "SELECT count(*) FROM t");
    assert_eq!(
        count, 3,
        "P2 should see 3 rows after P1's second checkpoint, got {count}"
    );

    assert_integrity_ok(&conn2);
}

// ---------------------------------------------------------------------------
// MVCC3: Flock coalescing during checkpoint in SharedReads mode
// ---------------------------------------------------------------------------

/// In SharedReads mode, header_validation acquires a permanent write lock
/// (refcount=1). MVCC checkpoint's begin_write_tx bumps to refcount=2.
/// end_write_tx drops back to 1 (flock NOT released). Verify no deadlock
/// and correct refcount behavior.
#[test]
fn test_mp_mvcc_flock_coalescing_during_checkpoint() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut db_path = dir.clone();
    db_path.push("test.db");

    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "journal_mode", "wal").unwrap();
    }

    // Open with SharedReads — acquires permanent write lock (refcount=1).
    let (db1, conn1) = open_with_mode(&dir, LockingMode::SharedReads);

    // Verify permanent lock is held (refcount=1).
    if let Some(tshm) = &db1.tshm {
        assert_eq!(
            tshm.active_writers(),
            1,
            "SharedReads should hold permanent write lock (refcount=1)"
        );
    }

    // Switch to MVCC mode.
    exec(&conn1, "PRAGMA journal_mode = 'experimental_mvcc'");

    // Create table and insert data.
    exec(&conn1, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn1, "INSERT INTO t VALUES (1, 'a')");
    exec(&conn1, "INSERT INTO t VALUES (2, 'b')");

    // Checkpoint — this triggers:
    //   begin_write_tx → tshm.write_lock() → refcount 1→2
    //   commit_tx → end_write_tx → tshm.write_unlock() → refcount 2→1 (flock NOT released)
    //   TRUNCATE checkpoint → ...
    // Must not deadlock.
    exec(&conn1, "PRAGMA wal_checkpoint(TRUNCATE)");

    // Verify refcount is still 1 (permanent lock retained, not 0).
    if let Some(tshm) = &db1.tshm {
        assert_eq!(
            tshm.active_writers(),
            1,
            "After checkpoint, refcount should still be 1 (permanent lock)"
        );
    }

    // Insert more and checkpoint again — verify repeated coalescing works.
    exec(&conn1, "INSERT INTO t VALUES (3, 'c')");
    exec(&conn1, "PRAGMA wal_checkpoint(TRUNCATE)");

    if let Some(tshm) = &db1.tshm {
        assert_eq!(
            tshm.active_writers(),
            1,
            "After second checkpoint, refcount should still be 1"
        );
    }

    // Verify data integrity.
    let count = query_i64(&conn1, "SELECT count(*) FROM t");
    assert_eq!(count, 3, "should have 3 rows");
    assert_integrity_ok(&conn1);

    // Close P1 — Drop should release the permanent lock.
    drop(conn1);
    drop(db1);

    // P2 (SharedReads) should now be able to acquire the write lock.
    let (db2, conn2) = open_with_mode(&dir, LockingMode::SharedReads);
    if let Some(tshm) = &db2.tshm {
        assert_eq!(
            tshm.active_writers(),
            1,
            "P2 should acquire permanent write lock after P1 closes"
        );
    }

    let count = query_i64(&conn2, "SELECT count(*) FROM t");
    assert_eq!(count, 3, "P2 should see all 3 rows");
    assert_integrity_ok(&conn2);
}

// ---------------------------------------------------------------------------
// MVCC4: mvcc_refresh_if_db_changed detects cross-process checkpoint
// ---------------------------------------------------------------------------

/// Test that MultiProcessWal.mvcc_refresh_if_db_changed detects when another
/// process has checkpointed, by checking TSHM writer_state before delegating
/// to the inner WalFile.
///
/// Without the TSHM check fix, the inner WalFile only sees its own
/// per-process WalFileShared state, which isn't updated by other processes'
/// checkpoints. The fix adds a TSHM-gated rescan so the inner state is
/// updated before the comparison.
#[test]
fn test_mp_mvcc_refresh_detects_cross_process_checkpoint() {
    let (_db1, conn1, dir) = open_mp_mvcc_database();

    // P1: create table, insert data, checkpoint to establish schema on disk.
    exec(&conn1, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn1, "PRAGMA wal_checkpoint(TRUNCATE)");

    // P2: open and do an initial read to establish cached WAL state.
    let (_db2, conn2) = open_second_process_mvcc(&dir);
    let count = query_i64(&conn2, "SELECT count(*) FROM t");
    assert_eq!(count, 0, "P2 should see empty table initially");

    // P1: insert data and checkpoint. This bumps TSHM writer_state.
    exec(&conn1, "INSERT INTO t VALUES (1, 'x')");
    exec(&conn1, "INSERT INTO t VALUES (2, 'y')");
    exec(&conn1, "PRAGMA wal_checkpoint(TRUNCATE)");

    // P2: directly call mvcc_refresh_if_db_changed via the WAL trait.
    // With the TSHM fix: detects the cross-process change, rescans,
    // returns true → page cache would be invalidated.
    // Without the fix: inner WalFile state is stale, returns false.
    let pager2 = conn2.pager.load();
    let wal2 = pager2.wal.as_ref().expect("MVCC should have WAL");
    let changed = wal2.mvcc_refresh_if_db_changed();
    assert!(
        changed,
        "mvcc_refresh_if_db_changed should detect cross-process checkpoint"
    );

    // Note: full data visibility (P2 seeing P1's rows) depends on the
    // MVCC read path falling through from MvStore to pager, which is a
    // separate cross-process MVCC concern. The key invariant verified
    // here is that mvcc_refresh_if_db_changed correctly detects the
    // cross-process checkpoint via TSHM.
}

// ---------------------------------------------------------------------------
// MVCC5: Stale salt after checkpoint (MVCC variant)
// ---------------------------------------------------------------------------

/// MVCC variant of the WAL stale-salt regression test. Verifies that when
/// P1 checkpoints (TRUNCATE → new WAL salt) and then P2 writes and
/// checkpoints, P2 picks up the new salt via rescan in begin_write_tx.
///
/// In MVCC mode, writes go through SkipMap + .db-log, but checkpoint
/// writes go through the pager/WAL. So the WAL salt coordination is
/// critical during MVCC checkpoint.
#[test]
fn test_mp_mvcc_stale_salt_after_checkpoint() {
    let (_db1, conn1, dir) = open_mp_mvcc_database();

    // P1: create table, insert data, checkpoint.
    exec(&conn1, "CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT)");
    exec(&conn1, "INSERT INTO t VALUES (1, 'from_p1')");
    exec(&conn1, "PRAGMA wal_checkpoint(TRUNCATE)");

    // Close P1 to release write lock.
    drop(conn1);
    drop(_db1);

    // P2: open, write, checkpoint. Must detect new salt from P1's checkpoint.
    let (_db2, conn2) = open_with_mode(&dir, LockingMode::SharedWrites);
    exec(&conn2, "INSERT INTO t VALUES (2, 'from_p2')");
    exec(&conn2, "PRAGMA wal_checkpoint(TRUNCATE)");

    // Close P2.
    drop(conn2);
    drop(_db2);

    // P3: open and verify all data is visible.
    let (_db3, conn3) = open_with_mode(&dir, LockingMode::SharedWrites);
    let count = query_i64(&conn3, "SELECT count(*) FROM t");
    assert_eq!(
        count, 2,
        "P3 should see both rows after sequential MVCC checkpoints, got {count}. \
         If only 1 row visible, P2 may have used a stale WAL salt."
    );

    // Verify specific values.
    let rows = query_rows(&conn3, "SELECT val FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    match &rows[0][0] {
        Value::Text(s) => assert_eq!(s.as_ref(), "from_p1"),
        other => panic!("expected text, got {other:?}"),
    }
    match &rows[1][0] {
        Value::Text(s) => assert_eq!(s.as_ref(), "from_p2"),
        other => panic!("expected text, got {other:?}"),
    }

    assert_integrity_ok(&conn3);
}
