//! Torn-write recovery tests for the pager WAL (#1327).
//!
//! A torn write happens when power is lost while a page/frame write is only
//! partially persisted: the tail of the file may hold a prefix of the data,
//! zeroed sectors, or garbage, while the application was never told anything.
//! These tests damage the in-memory WAL image at 512-byte sector granularity
//! between "crash" (drop of the last connection) and reopen, then assert the
//! SQLite torn-tail semantics: recovery keeps every commit up to the last
//! valid commit frame, drops everything after the damage, and never panics.

use std::sync::Arc;

use anyhow::Result;
use turso_core::{Connection, Database, DatabaseOpts, IO, OpenFlags, StepResult};

use crate::runner::memory::io::MemorySimIO;

/// Model spinning-rust / flash sector size: writes tear on these boundaries.
const SECTOR_SIZE: usize = 512;

fn make_io(seed: u64) -> Arc<MemorySimIO> {
    Arc::new(MemorySimIO::new(seed, 4096, 100, 1, 5))
}

fn open_conn(io: Arc<MemorySimIO>, path: &str) -> Result<Arc<Connection>> {
    let db = Database::open_file_with_flags(
        io as Arc<dyn IO>,
        path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    let conn = db.connect()?;
    Ok(conn)
}

fn query_rows(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id")?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                rows.push((
                    row.get::<i64>(0).expect("id column should exist"),
                    row.get::<String>(1).expect("v column should exist"),
                ));
            }
            StepResult::Done => return Ok(rows),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

fn assert_integrity_ok(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<()> {
    let mut stmt = conn.prepare("PRAGMA integrity_check")?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("integrity_check should return a row");
                let verdict = row.get::<String>(0).expect("verdict column should exist");
                assert_eq!(verdict, "ok", "integrity_check failed after recovery");
                return Ok(());
            }
            StepResult::Done => panic!("integrity_check returned no rows"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

fn find_file_path_by_suffix(io: &MemorySimIO, suffix: &str) -> String {
    io.files
        .borrow()
        .keys()
        .find(|path| path.ends_with(suffix))
        .cloned()
        .unwrap_or_else(|| panic!("expected file with suffix {suffix}"))
}

fn file_len_by_suffix(io: &MemorySimIO, suffix: &str) -> usize {
    let path = find_file_path_by_suffix(io, suffix);
    let files = io.files.borrow();
    let len = files.get(&path).unwrap().buffer.borrow().len();
    len
}

fn mutate_file_by_suffix(io: &MemorySimIO, suffix: &str, mutator: impl FnOnce(&mut Vec<u8>)) {
    let path = find_file_path_by_suffix(io, suffix);
    let files = io.files.borrow();
    let file = files
        .get(&path)
        .unwrap_or_else(|| panic!("missing file for path {path}"));
    mutator(&mut file.buffer.borrow_mut());
}

/// Round `len` down to a sector boundary, modeling the largest prefix of a
/// write that can survive a power cut.
fn sector_floor(len: usize) -> usize {
    len - (len % SECTOR_SIZE)
}

/// What this test checks: A WAL tail truncated mid-frame on a sector boundary
/// only loses the commit whose frames were torn; every prior commit survives.
/// Why this matters: This is the canonical torn write - power dies while the
/// tail of the WAL is being extended - and it must degrade to "last durable
/// commit wins", never to a corrupt database.
#[test]
fn sim_wal_torn_tail_truncated_mid_frame_keeps_committed_prefix() -> Result<()> {
    let seed = 301;
    let io = make_io(seed);
    let path = format!("sim_wal_torn_tail_truncated_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;
    let wal_len_after_first_insert = file_len_by_suffix(io.as_ref(), "-wal");
    conn.execute("INSERT INTO t VALUES (2, 'b')")?;
    drop(conn);

    // Persist only the first sector of the second insert's frames.
    mutate_file_by_suffix(io.as_ref(), "-wal", |buf| {
        assert!(buf.len() > wal_len_after_first_insert, "second insert must have appended frames");
        buf.truncate(sector_floor(wal_len_after_first_insert) + SECTOR_SIZE);
    });

    let conn = open_conn(io.clone(), &path)?;
    assert_eq!(query_rows(&conn, io.as_ref())?, vec![(1, "a".to_string())]);
    assert_integrity_ok(&conn, io.as_ref())?;
    Ok(())
}

/// What this test checks: A torn sector that persisted garbage inside the last
/// frame is caught by the WAL frame checksum and the damaged commit is dropped.
/// Why this matters: Disks may persist arbitrary bytes in a torn sector, not
/// just a clean prefix; detection must not depend on the tail being zeroed.
#[test]
fn sim_wal_torn_garbage_sector_dropped_by_checksum() -> Result<()> {
    let seed = 302;
    let io = make_io(seed);
    let path = format!("sim_wal_torn_garbage_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;
    conn.execute("INSERT INTO t VALUES (2, 'b')")?;
    drop(conn);

    // The last sector of the WAL persisted garbage instead of frame bytes.
    mutate_file_by_suffix(io.as_ref(), "-wal", |buf| {
        let len = buf.len();
        assert!(len > SECTOR_SIZE, "WAL must be longer than one sector");
        for byte in &mut buf[len - SECTOR_SIZE..] {
            *byte = 0xAB;
        }
    });

    let conn = open_conn(io.clone(), &path)?;
    assert_eq!(query_rows(&conn, io.as_ref())?, vec![(1, "a".to_string())]);
    assert_integrity_ok(&conn, io.as_ref())?;
    Ok(())
}

/// What this test checks: Tearing a sector in the middle of a multi-frame
/// transaction drops the whole transaction, not a prefix of its frames.
/// Why this matters: Commit atomicity across frames is the core WAL guarantee;
/// a torn write inside a big transaction must not leave half its pages applied.
#[test]
fn sim_wal_torn_sector_mid_multiframe_transaction_drops_whole_txn() -> Result<()> {
    let seed = 303;
    let io = make_io(seed);
    let path = format!("sim_wal_torn_multiframe_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;
    let wal_len_before_txn = file_len_by_suffix(io.as_ref(), "-wal");

    // Multi-page transaction: several ~2KB rows force multiple WAL frames.
    conn.execute("BEGIN")?;
    let big = "x".repeat(2000);
    for id in 10..15 {
        conn.execute(format!("INSERT INTO t VALUES ({id}, '{big}')"))?;
    }
    conn.execute("COMMIT")?;
    drop(conn);

    // Corrupt one sector early in the transaction's frame range: everything
    // from the damaged frame onward (including the commit frame) becomes
    // invalid. Flip bits rather than zeroing, because a b-tree page fills from
    // its tail and a sector of free space is legitimately all zeroes already.
    mutate_file_by_suffix(io.as_ref(), "-wal", |buf| {
        let start = sector_floor(wal_len_before_txn) + SECTOR_SIZE;
        assert!(buf.len() >= start + SECTOR_SIZE, "transaction must span multiple sectors");
        for byte in &mut buf[start..start + SECTOR_SIZE] {
            *byte ^= 0xFF;
        }
    });

    let conn = open_conn(io.clone(), &path)?;
    assert_eq!(query_rows(&conn, io.as_ref())?, vec![(1, "a".to_string())]);
    assert_integrity_ok(&conn, io.as_ref())?;
    Ok(())
}

/// What this test checks: A torn WAL header write invalidates the whole WAL
/// and the database falls back to its last checkpointed state without panicking.
/// Why this matters: The 32-byte WAL header is itself written through the same
/// torn-write-prone path; a half-written header must read as "no WAL", not as
/// an error or a crash.
#[test]
fn sim_wal_torn_header_falls_back_to_checkpointed_state() -> Result<()> {
    let seed = 304;
    let io = make_io(seed);
    let path = format!("sim_wal_torn_header_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;
    // Push the committed state into the main DB file and reset the WAL.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    conn.execute("INSERT INTO t VALUES (2, 'b')")?;
    drop(conn);

    // The fresh WAL header (first 32 bytes) persisted only half a sector's
    // worth of valid bytes; garbage follows.
    mutate_file_by_suffix(io.as_ref(), "-wal", |buf| {
        assert!(buf.len() >= 32, "WAL must have a header after the second insert");
        for byte in &mut buf[16..32] {
            *byte = 0xCD;
        }
    });

    let conn = open_conn(io.clone(), &path)?;
    assert_eq!(query_rows(&conn, io.as_ref())?, vec![(1, "a".to_string())]);
    assert_integrity_ok(&conn, io.as_ref())?;
    Ok(())
}
