//! Per-database memory leak detection.
//!
//! This test target uses a counting `#[global_allocator]` (hence the
//! separate binary) to verify that repeatedly opening a database, running
//! a workload, and dropping the database does not accumulate heap memory
//! in process-global structures (e.g. the database registry, completion
//! groups, MVCC skiplist garbage).
//!
//! It models a server that serves many databases over its lifetime,
//! creating and dropping them one at a time. Regression caught here in
//! the past: the `CompletionGroup` self-reference `Arc` cycle leaking
//! every completion group ever built (one per WAL checkpoint / commit
//! batch / sorter spill).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use turso_core::{Database, OpenFlags, StepResult};

struct CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = System.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            LIVE_BYTES.fetch_add(new_size, Ordering::Relaxed);
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn live_bytes() -> usize {
    LIVE_BYTES.load(Ordering::Relaxed)
}

/// Drive crossbeam-epoch garbage collection to completion. The MVCC
/// skiplists defer destruction of removed entries through epoch-based
/// reclamation, so right after a database is dropped its garbage may
/// still sit in the (process-global) epoch queues. The collector only
/// frees a few bags per `pin()`, so pin repeatedly until the live byte
/// count stops shrinking.
fn drain_epoch_garbage() {
    let mut last = live_bytes();
    loop {
        for _ in 0..1024 {
            crossbeam_epoch::pin().flush();
        }
        let now = live_bytes();
        if now >= last {
            return;
        }
        last = now;
    }
}

/// One full database lifecycle: open at `path`, run a small workload,
/// close and drop every handle, delete the files.
fn database_lifecycle(io: &Arc<dyn turso_core::IO + Send>, path: &std::path::Path, mvcc: bool) {
    let db = Database::open_file_with_flags(
        io.clone(),
        path.to_str().unwrap(),
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
    )
    .unwrap();

    let conn = db.connect().unwrap();
    if mvcc {
        conn.pragma_update("journal_mode", "'mvcc'").unwrap();
    }

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute(if mvcc { "BEGIN CONCURRENT" } else { "BEGIN" })
        .unwrap();
    for i in 0..100 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'payload-{i}')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("UPDATE t SET b = b || '-updated' WHERE a % 2 = 0")
        .unwrap();

    let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {}
            StepResult::IO => stmt.get_pager().io.step().unwrap(),
            _ => break,
        }
    }
    drop(stmt);

    conn.close().unwrap();
    drop(conn);
    drop(db);

    for ext in ["", "-wal", "-log", "-shm"] {
        let f = path.with_file_name(format!(
            "{}{}",
            path.file_name().unwrap().to_str().unwrap(),
            ext
        ));
        if f.exists() {
            std::fs::remove_file(f).unwrap();
        }
    }
}

const WARMUP_CYCLES: usize = 20;
const MEASURED_CYCLES: usize = 100;
/// A database lifecycle should retain ~0 bytes once globals are warmed
/// up. The budget leaves slack for allocator-internal noise (hashmap
/// rehashes, thread-local caches) without masking real leaks: the
/// smallest leak found so far (a single `Completion` per database,
/// ~280 bytes/cycle) blows through it.
const PER_CYCLE_BUDGET_BYTES: isize = 128;

fn assert_no_per_database_leak(mvcc: bool) {
    let label = if mvcc { "MVCC" } else { "WAL" };
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let mut n = 0usize;
    let cycle = |n: &mut usize| {
        let path = tmp_dir.path().join(format!("leak-{n}.db"));
        database_lifecycle(&io, &path, mvcc);
        *n += 1;
    };

    // Warmup: populate lazy statics, allocator caches, thread-local
    // buffer caches, etc. so the measured phase only sees per-cycle cost.
    for _ in 0..WARMUP_CYCLES {
        cycle(&mut n);
    }
    drain_epoch_garbage();
    let baseline = live_bytes() as isize;

    for _ in 0..MEASURED_CYCLES {
        cycle(&mut n);
    }
    drain_epoch_garbage();
    let growth = live_bytes() as isize - baseline;

    let per_cycle = growth / MEASURED_CYCLES as isize;
    eprintln!(
        "{label}: heap growth over {MEASURED_CYCLES} database lifecycles: \
         {growth} bytes ({per_cycle} bytes/cycle)"
    );
    assert!(
        per_cycle <= PER_CYCLE_BUDGET_BYTES,
        "{label}: heap grew by {per_cycle} bytes per database lifecycle \
         (total {growth} bytes over {MEASURED_CYCLES} cycles, budget \
         {PER_CYCLE_BUDGET_BYTES} bytes/cycle) — something process-global is \
         retaining per-database memory after the database is dropped"
    );
}

// `serial`: the two tests share the process-global LIVE_BYTES counter, so
// they must not run on concurrent threads when executed by `cargo test`
// (nextest runs them in separate processes, where this is a no-op).
#[test]
#[serial_test::serial]
fn database_lifecycle_does_not_leak_mvcc() {
    assert_no_per_database_leak(true);
}

#[test]
#[serial_test::serial]
fn database_lifecycle_does_not_leak_wal() {
    assert_no_per_database_leak(false);
}
