//! Tests for `MemoryYieldIO` that need a full `Database`, so they live in
//! the main crate rather than in `turso_core_io`.

use crate::io::*;
use crate::io::clock::{MonotonicInstant, WallClockInstant};
use crate::sync::Arc;
use crate::vdbe::StepResult;
use crate::SqliteDialect;
use crate::{Database, IOResult, OpenFlags, Result};
use std::sync::atomic::{AtomicBool, Ordering};

struct StepGuardedIO {
    inner: MemoryYieldIO,
    step_allowed: AtomicBool,
}

impl StepGuardedIO {
    fn new() -> Self {
        Self {
            inner: MemoryYieldIO::new(),
            step_allowed: AtomicBool::new(true),
        }
    }

    fn set_step_allowed(&self, allowed: bool) {
        self.step_allowed.store(allowed, Ordering::SeqCst);
    }
}

impl Clock for StepGuardedIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for StepGuardedIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        self.inner.open_file(path, flags, direct)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }

    fn file_id(&self, path: &str) -> Result<crate::io::FileId> {
        self.inner.file_id(path)
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        self.inner.supports_shared_wal_coordination()
    }

    fn step(&self) -> Result<()> {
        assert!(
            self.step_allowed.load(Ordering::SeqCst),
            "IO::step must only be called by the test driver"
        );
        self.inner.step()
    }
}

fn drive_guarded_io<T>(
    io: &StepGuardedIO,
    mut action: impl FnMut() -> Result<IOResult<T>>,
) -> (T, usize) {
    let mut io_yields = 0usize;
    loop {
        io.set_step_allowed(false);
        let step = action();
        io.set_step_allowed(true);

        match step.unwrap() {
            IOResult::Done(result) => return (result, io_yields),
            IOResult::IO(io_result) => {
                io_yields += 1;
                io_result.wait(io).unwrap();
            }
        }
    }
}

/// Every op must come back unfinished and only complete once `step()` runs.
#[test]
fn completions_are_deferred_until_step() {
    let io = MemoryYieldIO::new();
    let file = io.open_file("t", OpenFlags::Create, false).unwrap();

    let buf = Arc::new(Buffer::new(vec![0xAB; 64]));
    let wc = file.pwrite(0, buf, Completion::new_write(|_| {})).unwrap();
    assert!(
        !wc.finished(),
        "pwrite completion must not finish before step()"
    );
    io.step().unwrap();
    assert!(wc.finished() && wc.succeeded());

    let sc = file
        .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
        .unwrap();
    assert!(
        !sc.finished(),
        "sync completion must not finish before step()"
    );
    io.step().unwrap();
    assert!(sc.succeeded());

    let tc = file.truncate(16, Completion::new_trunc(|_| {})).unwrap();
    assert!(
        !tc.finished(),
        "truncate completion must not finish before step()"
    );
    io.step().unwrap();
    assert!(tc.succeeded());
    assert_eq!(file.size().unwrap(), 16);
}

/// Data written before a `step()` is visible to a later read, exactly as
/// with the synchronous `MemoryIO` — only the signalling is deferred.
#[test]
fn read_returns_written_bytes_after_step() {
    let io = MemoryYieldIO::new();
    let file = io.open_file("t", OpenFlags::Create, false).unwrap();

    let wbuf = Arc::new(Buffer::new(vec![0x42; 100]));
    let wc = file.pwrite(0, wbuf, Completion::new_write(|_| {})).unwrap();
    io.step().unwrap();
    assert!(wc.succeeded());

    let rbuf = Arc::new(Buffer::new_temporary(100));
    let rc = file
        .pread(0, Completion::new_read(rbuf.clone(), |_| None))
        .unwrap();
    assert!(
        !rc.finished(),
        "pread completion must not finish before step()"
    );
    io.step().unwrap();
    assert!(rc.succeeded());
    assert!(rbuf.as_slice().iter().all(|&b| b == 0x42));
}

/// Driving real SQL through the engine on this backend must force at least
/// one `StepResult::IO` (i.e. a genuine yield + re-entry), which the
/// synchronous `MemoryIO` fast-paths away, while still producing correct
/// results.
#[test]
fn engine_yields_and_round_trips() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io: Arc<dyn IO> = Arc::new(MemoryYieldIO::new());
    let db =
        Database::open_file(io.clone(), "memory_yield_test.db", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();

    // Step statements manually so we can observe the cooperative-yield path.
    // Returns (rows, number of StepResult::IO yields).
    let run = |sql: &str| -> (Vec<crate::Value>, usize) {
        let mut stmt = conn.prepare(sql).unwrap();
        let mut io_yields = 0usize;
        let mut rows = Vec::new();
        loop {
            match stmt.step().unwrap() {
                StepResult::IO => {
                    io_yields += 1;
                    io.step().unwrap();
                }
                StepResult::Row => {
                    let v = stmt.row().unwrap().get_values().next().unwrap().clone();
                    rows.push(v);
                }
                StepResult::Done => break,
                other => panic!("unexpected step result: {other:?}"),
            }
        }
        (rows, io_yields)
    };

    // A write transaction must flush pages, so it is guaranteed to defer at
    // least one completion and surface a StepResult::IO. The synchronous
    // MemoryIO would fast-path right past this.
    let (_, create_yields) = run("CREATE TABLE t(x)");
    assert!(
        create_yields > 0,
        "memory_yield backend must force at least one StepResult::IO on a write"
    );
    run("INSERT INTO t VALUES (1), (2), (3)");

    // And the round trip still produces correct results.
    let (rows, _) = run("SELECT x FROM t ORDER BY x");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], crate::Value::from_i64(1));
    assert_eq!(rows[2], crate::Value::from_i64(3));
}

#[test]
fn journal_mode_mvcc_bootstrap_yields_without_internal_step() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(StepGuardedIO::new());
    let db = Database::open_file(
        io.clone(),
        "journal_mode_mvcc_yield.db",
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    let mut stmt = conn.prepare("PRAGMA journal_mode = 'mvcc'").unwrap();

    let mut rows = Vec::new();
    let mut io_yields = 0usize;
    loop {
        io.set_step_allowed(false);
        let step = stmt.step();
        io.set_step_allowed(true);

        match step.unwrap() {
            StepResult::IO => {
                io_yields += 1;
                io.step().unwrap();
            }
            StepResult::Row => {
                let value = stmt.row().unwrap().get_values().next().unwrap().clone();
                rows.push(value);
            }
            StepResult::Done => break,
            other => panic!("unexpected step result: {other:?}"),
        }
    }

    assert!(
        io_yields > 0,
        "MemoryYieldIO should force PRAGMA journal_mode=mvcc through cooperative yields"
    );
    assert_eq!(rows, vec![crate::Value::build_text("mvcc")]);
    assert!(conn.mvcc_enabled());
}

#[test]
fn fresh_mvcc_attach_yields_without_internal_step() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(StepGuardedIO::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        "attach_main_yield.db",
        OpenFlags::Create,
        crate::DatabaseOpts::new().with_attach(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    let run = |sql: &str| {
        let mut stmt = conn.prepare(sql).unwrap();
        let mut io_yields = 0usize;
        loop {
            io.set_step_allowed(false);
            let step = stmt.step();
            io.set_step_allowed(true);

            match step.unwrap() {
                StepResult::IO => {
                    io_yields += 1;
                    io.step().unwrap();
                }
                StepResult::Row => {}
                StepResult::Done => break,
                other => panic!("unexpected step result: {other:?}"),
            }
        }
        io_yields
    };

    run("PRAGMA journal_mode = 'mvcc'");
    let attach_yields = run("ATTACH 'attach_aux_yield.db' AS aux");

    assert!(
        attach_yields > 0,
        "MemoryYieldIO should force ATTACH through cooperative yields"
    );
    assert!(conn.get_database_id_by_name("aux").is_ok());
    assert!(conn
        .mv_store_for_db(conn.get_database_id_by_name("aux").unwrap())
        .is_some());
}

#[test]
fn pager_allocate_and_free_yield_for_header_reads_without_internal_step() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(StepGuardedIO::new());
    let db =
        Database::open_file(io.clone(), "pager_header_yield.db", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();

    let pager = conn.get_pager();
    conn.execute("BEGIN IMMEDIATE").unwrap();
    pager.clear_page_cache(false);
    let (_page, allocate_yields) = drive_guarded_io(io.as_ref(), || pager.allocate_page());
    assert!(
        allocate_yields > 0,
        "allocate_page should yield when reading page 1 from storage"
    );
    conn.execute("ROLLBACK").unwrap();

    conn.execute("BEGIN IMMEDIATE").unwrap();
    pager.clear_page_cache(false);
    let ((), free_yields) = drive_guarded_io(io.as_ref(), || pager.free_page(None, 2));
    assert!(
        free_yields > 0,
        "free_page should yield when reading page 1 from storage"
    );
    conn.execute("ROLLBACK").unwrap();
}

#[test]
fn overflow_delete_yields_for_header_validation_without_internal_step() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(StepGuardedIO::new());
    let db = Database::open_file(
        io.clone(),
        "overflow_delete_yield.db",
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(x BLOB)").unwrap();
    conn.execute("INSERT INTO t VALUES (zeroblob(20000))")
        .unwrap();

    conn.get_pager().clear_page_cache(false);
    let mut stmt = conn.prepare("DELETE FROM t").unwrap();
    let mut io_yields = 0usize;
    loop {
        io.set_step_allowed(false);
        let step = stmt.step();
        io.set_step_allowed(true);

        match step.unwrap() {
            StepResult::IO => {
                io_yields += 1;
                io.step().unwrap();
            }
            StepResult::Done => break,
            other => panic!("unexpected step result: {other:?}"),
        }
    }

    assert!(
        io_yields > 0,
        "overflow DELETE should yield while clearing overflow pages"
    );
}

/// Local reproduction of the OPFS/WASM "insert hang" reported against
/// `@tursodatabase/database-wasm`: seeding a database stalls while adding
/// records, and a larger page size avoids it.
///
/// This needs no WASM build or browser — it models the exact browser
/// constraint. On the browser main thread OPFS completions are delivered
/// only when control returns to the JS event loop, and `IO::step()` is a
/// no-op there. `StepGuardedIO` enforces that: `io.step()` may run only from
/// the driver loop below (mirroring the JS `stepSync()`/`await io()` loop),
/// never from inside `stmt.step()`. If the engine drives IO internally, the
/// assertion in `StepGuardedIO::step` fires — that is the browser hang.
///
/// The trigger is mid-transaction cache spilling: a small page cache with
/// spilling enabled and a single transaction that dirties far more pages
/// than the cache can hold. The pager spills dirty pages to the WAL via
/// `WalFile::append_frames_vectored`, which awaits the write with a blocking
/// `io.drain_completions(...)` (core/storage/wal.rs) instead of yielding
/// `StepResult::IO`. That blocking wait is the bug.
///
/// Run it with:
///   cargo test -p turso_core --features io_memory_yield \
///       memory_yield::tests::wasm_opfs_cache_spill_insert_hang
#[test]
fn wasm_opfs_cache_spill_insert_hang() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(StepGuardedIO::new());
    let db = Database::open_file(
        io.clone(),
        "wasm_opfs_spill_hang.db",
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    // Drive one statement to completion exactly like the WASM/OPFS JS
    // binding does: step, and on StepResult::IO hand control back to the
    // "event loop" (the driver's io.step()). The engine must only ever
    // *yield* IO here, never drive it itself.
    let run = |sql: &str| {
        let mut stmt = conn.prepare(sql).unwrap();
        loop {
            io.set_step_allowed(false);
            let step = stmt.step();
            io.set_step_allowed(true);
            match step.unwrap() {
                StepResult::IO => io.step().unwrap(),
                StepResult::Row => {}
                StepResult::Done => break,
                other => panic!("unexpected step result: {other:?}"),
            }
        }
    };

    // Small cache + spilling enabled (this is the WASM default for spill,
    // but we set it explicitly so the test is platform-independent).
    run("PRAGMA cache_size=200");
    run("PRAGMA cache_spill=ON");
    run("CREATE TABLE t(x)");

    // One transaction that dirties far more than 200 pages, forcing the
    // pager to spill dirty pages to the WAL before COMMIT.
    run("BEGIN");
    for _ in 0..500 {
        run("INSERT INTO t VALUES (randomblob(4000))");
    }
    run("COMMIT");

    let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
    io.set_step_allowed(false);
    loop {
        let step = stmt.step();
        match step.unwrap() {
            StepResult::IO => {
                io.set_step_allowed(true);
                io.step().unwrap();
                io.set_step_allowed(false);
            }
            StepResult::Row => {
                let n = stmt.row().unwrap().get_values().next().unwrap().clone();
                assert_eq!(n, crate::Value::from_i64(500));
            }
            StepResult::Done => break,
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}
