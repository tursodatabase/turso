//! What readahead is worth on a table scan.
//!
//! Readahead does not make the CPU do less work -- it makes the database ask
//! storage for the same bytes in far fewer requests. So the thing to measure
//! is time spent per request, and the benchmark has to be run against storage
//! where a request costs something.
//!
//! Two shapes are measured:
//!
//! * `local_file` -- an ordinary file. The only per-request cost is the
//!   syscall, and the operating system already has the data in memory, so
//!   this is close to the smallest win readahead can produce. If it is
//!   positive here it is positive everywhere.
//!
//! * `per_request_cost` -- the same scan against an IO that spends a fixed
//!   amount of work on every request before serving it. This is a *model*, and
//!   worth being clear about: it stands in for storage where a request costs
//!   much more than the bytes it moves -- a network disk, a remote page
//!   server, an object store. The cost is CPU work rather than sleeping so
//!   that both a wall-clock run and an instruction-count run measure the same
//!   thing. It does not claim to be any particular device; it says "if a
//!   request costs C, readahead removes most of the Cs."
//!
//! Run:  cargo bench -p turso_core --bench readahead_benchmark

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use std::hint::black_box as hint_black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use turso_core::{
    io::FileSyncType, Buffer, Clock, Completion, Database, DatabaseOpts, File, MonotonicInstant,
    OpenFlags, PlatformIO, SqliteDialect, StepResult, WallClockInstant, IO,
};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Rows in the benchmark table. At ~200 bytes a row this is a few thousand
/// pages: big enough that a scan crosses many of them and cannot sit entirely
/// in the page cache below.
const ROWS: usize = 120_000;

/// Page cache for the scanning connection, in KiB (the negative form of
/// `PRAGMA cache_size`). Deliberately far smaller than the table: a scan that
/// fits in memory never goes to storage and has nothing to read ahead.
const CACHE_KIB: i64 = 2_000;

/// Work spent on each storage request in the modelled-cost benchmark.
///
/// Calibrated to roughly 10 microseconds, which is the order of a read from a
/// local NVMe drive that is not already in memory. Slower storage only makes
/// the gap wider: network block storage is ten to a hundred times this, and an
/// object store is a thousand times it.
const WORK_PER_REQUEST: u64 = 45_000;

/// An IO that charges a fixed amount of work for every read request,
/// regardless of size. See the module docs.
struct CostPerRequestIo {
    inner: Arc<dyn IO>,
    work: u64,
}

impl Clock for CostPerRequestIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for CostPerRequestIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        Ok(Arc::new(CostPerRequestFile {
            inner: self.inner.open_file(path, flags, direct)?,
            work: self.work,
        }))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }

    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }
}

struct CostPerRequestFile {
    inner: Arc<dyn File>,
    work: u64,
}

/// Deterministic busy work. Not a sleep, so an instruction-counting run sees
/// it too.
#[inline(never)]
fn spend(units: u64) {
    let mut acc = 0x9e3779b97f4a7c15u64;
    for i in 0..units {
        acc = acc.wrapping_mul(6364136223846793005).wrapping_add(i);
    }
    hint_black_box(acc);
}

impl File for CostPerRequestFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        spend(self.work);
        self.inner.pread(pos, c)
    }

    fn preadv(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        // One request, whatever its size: that is the whole point.
        spend(self.work);
        self.inner.preadv(pos, buffers, c)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.inner.pwrite(pos, buffer, c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        self.inner.sync(c, sync_type)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.truncate(len, c)
    }
}

/// Build the table once, with rusqlite, so the benchmark measures reads only.
fn seed_db() -> TempDir {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("readahead.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=DELETE;
         PRAGMA page_size=4096;
         CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT, v INTEGER);",
    )
    .unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO t(id, payload, v) VALUES(?1, ?2, ?3)")
            .unwrap();
        let payload = "x".repeat(180);
        for i in 0..ROWS {
            stmt.execute(rusqlite::params![i as i64, payload, (i % 977) as i64])
                .unwrap();
        }
    }
    tx.commit().unwrap();
    dir
}

/// Open the seeded database on `io` and set the scan up: a small page cache
/// and the readahead window under test.
fn connect(io: Arc<dyn IO>, path: &str, prefetch_pages: u32) -> Arc<turso_core::Connection> {
    let db = Database::open_file_with_flags(
        io,
        path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute(format!("PRAGMA cache_size = -{CACHE_KIB}"))
        .unwrap();
    conn.execute(format!("PRAGMA prefetch_pages = {prefetch_pages}"))
        .unwrap();
    conn
}

/// A whole-table scan that touches every row's payload, so it cannot be
/// answered from b-tree counts alone.
const SCAN: &str = "SELECT sum(v), count(payload) FROM t";

fn run_scan(io: &Arc<dyn IO>, conn: &Arc<turso_core::Connection>) {
    let mut stmt = conn.prepare(SCAN).unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row().unwrap());
            }
            StepResult::Done => break,
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => io.step().unwrap(),
            StepResult::Interrupt | StepResult::Busy => panic!("unexpected scan result"),
        }
    }
}

/// Empty the page cache between iterations so every run starts cold and
/// really goes to storage. Resizing down and back is the cheapest way to do
/// that from SQL.
fn drop_cache(conn: &Arc<turso_core::Connection>) {
    conn.execute("PRAGMA cache_size = -20").unwrap();
    conn.execute(format!("PRAGMA cache_size = -{CACHE_KIB}"))
        .unwrap();
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_readahead(criterion: &mut Criterion) {
    report_request_counts();
    let dir = seed_db();
    let path = dir.path().join("readahead.db");
    let path = path.to_str().unwrap();

    let mut group = criterion.benchmark_group("readahead");
    group.measurement_time(Duration::from_secs(10));

    // An ordinary file: the per-request cost is a syscall and nothing else.
    for prefetch in [0u32, 32] {
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let conn = connect(io.clone(), path, prefetch);
        group.bench_with_input(
            BenchmarkId::new("local_file", label(prefetch)),
            &prefetch,
            |b, _| {
                b.iter(|| {
                    drop_cache(&conn);
                    run_scan(&io, &conn);
                })
            },
        );
    }

    // Storage where a request costs much more than the bytes it moves.
    for prefetch in [0u32, 32] {
        let io: Arc<dyn IO> = Arc::new(CostPerRequestIo {
            inner: Arc::new(PlatformIO::new().unwrap()),
            work: WORK_PER_REQUEST,
        });
        let conn = connect(io.clone(), path, prefetch);
        group.bench_with_input(
            BenchmarkId::new("per_request_cost", label(prefetch)),
            &prefetch,
            |b, _| {
                b.iter(|| {
                    drop_cache(&conn);
                    run_scan(&io, &conn);
                })
            },
        );
    }

    group.finish();
}

fn label(prefetch_pages: u32) -> &'static str {
    if prefetch_pages == 0 {
        "readahead_off"
    } else {
        "readahead_on"
    }
}

/// Counts requests rather than timing them, so a run reports the thing
/// readahead actually changes. Printed alongside the timings.
fn report_request_counts() {
    struct CountingIo {
        inner: Arc<dyn IO>,
        reads: Arc<AtomicU64>,
        bytes: Arc<AtomicU64>,
    }
    struct CountingFile {
        inner: Arc<dyn File>,
        reads: Arc<AtomicU64>,
        bytes: Arc<AtomicU64>,
    }
    impl Clock for CountingIo {
        fn current_time_monotonic(&self) -> MonotonicInstant {
            self.inner.current_time_monotonic()
        }
        fn current_time_wall_clock(&self) -> WallClockInstant {
            self.inner.current_time_wall_clock()
        }
    }
    impl IO for CountingIo {
        fn open_file(
            &self,
            path: &str,
            flags: OpenFlags,
            direct: bool,
        ) -> turso_core::Result<Arc<dyn File>> {
            Ok(Arc::new(CountingFile {
                inner: self.inner.open_file(path, flags, direct)?,
                reads: self.reads.clone(),
                bytes: self.bytes.clone(),
            }))
        }
        fn remove_file(&self, path: &str) -> turso_core::Result<()> {
            self.inner.remove_file(path)
        }
        fn step(&self) -> turso_core::Result<()> {
            self.inner.step()
        }
    }
    impl File for CountingFile {
        fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
            self.inner.lock_file(exclusive)
        }
        fn unlock_file(&self) -> turso_core::Result<()> {
            self.inner.unlock_file()
        }
        fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.bytes
                .fetch_add(c.as_read().buf().len() as u64, Ordering::Relaxed);
            self.inner.pread(pos, c)
        }

        fn preadv(
            &self,
            pos: u64,
            buffers: Vec<Arc<Buffer>>,
            c: Completion,
        ) -> turso_core::Result<Completion> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(
                buffers.iter().map(|b| b.len() as u64).sum::<u64>(),
                Ordering::Relaxed,
            );
            self.inner.preadv(pos, buffers, c)
        }
        fn pwrite(
            &self,
            pos: u64,
            buffer: Arc<Buffer>,
            c: Completion,
        ) -> turso_core::Result<Completion> {
            self.inner.pwrite(pos, buffer, c)
        }
        fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
            self.inner.sync(c, sync_type)
        }
        fn size(&self) -> turso_core::Result<u64> {
            self.inner.size()
        }
        fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
            self.inner.truncate(len, c)
        }
    }

    let dir = seed_db();
    let path = dir.path().join("readahead.db");
    let path = path.to_str().unwrap();
    println!("\nrequests made by `{SCAN}`:");
    for prefetch in [0u32, 32] {
        let reads = Arc::new(AtomicU64::new(0));
        let bytes = Arc::new(AtomicU64::new(0));
        let io: Arc<dyn IO> = Arc::new(CountingIo {
            inner: Arc::new(PlatformIO::new().unwrap()),
            reads: reads.clone(),
            bytes: bytes.clone(),
        });
        let conn = connect(io.clone(), path, prefetch);
        // Warm up once so the count reflects steady state rather than the
        // first-touch of the file.
        drop_cache(&conn);
        run_scan(&io, &conn);
        drop_cache(&conn);
        reads.store(0, Ordering::Relaxed);
        bytes.store(0, Ordering::Relaxed);
        run_scan(&io, &conn);
        println!(
            "  prefetch_pages={prefetch:<3} {:>8} requests  {:>8} KiB",
            reads.load(Ordering::Relaxed),
            bytes.load(Ordering::Relaxed) / 1024,
        );
    }
    println!();
}

criterion_group!(benches, bench_readahead);
criterion_main!(benches);
