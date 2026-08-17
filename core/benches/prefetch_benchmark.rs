//! Benchmark for scan readahead (`PRAGMA prefetch_pages`).
//!
//! Readahead wins by overlapping storage latency, so this benchmark models a
//! storage device with a fixed per-read latency: every read completes only
//! after the IO layer has been stepped a fixed number of times. Without
//! readahead a scan pays the full latency for every leaf page in sequence;
//! with readahead the waits overlap. The model is deterministic, so the
//! effect shows up both in wall-clock time and in instruction-count based
//! runners.
//!
//! Run:  cargo bench -p turso_core --bench prefetch_benchmark

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use turso_core::{
    io::{FileId, FileSyncType},
    Buffer, Clock, Completion, Connection, Database, DatabaseOpts, File, MonotonicInstant,
    OpenFlags, SqliteDialect, StepResult, WallClockInstant, IO,
};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// How many IO steps a read takes to complete: the "device latency".
const READ_DELAY_TICKS: u64 = 16;

/// Busy-work per IO step so a step costs time (roughly a microsecond) the
/// way waiting on a real device would. Without this the whole latency model
/// is free and the benchmark only measures row-processing CPU, hiding
/// exactly the stalls readahead removes. A spin, unlike a sleep, is also
/// visible to instruction-count based runners.
const SPIN_PER_TICK: u64 = 2000;

/// Rows in the scanned table; roughly 600 leaf pages at 48-byte payloads.
const ROWS: usize = 20_000;

const DB_PATH: &str = "prefetch_bench.db";

struct DelayedOp {
    due_tick: u64,
    submit: Box<dyn FnOnce() -> turso_core::Result<()> + Send>,
}

struct LatencyIoState {
    tick: u64,
    pending: VecDeque<DelayedOp>,
}

/// In-memory IO where every read completes `read_delay_ticks` IO steps after
/// it was submitted. Writes complete immediately so building the fixture
/// stays cheap.
struct LatencyIo {
    inner: Arc<dyn IO>,
    state: Arc<Mutex<LatencyIoState>>,
    read_delay_ticks: u64,
}

impl LatencyIo {
    fn new(read_delay_ticks: u64) -> Self {
        Self {
            inner: Arc::new(turso_core::MemoryIO::new()),
            state: Arc::new(Mutex::new(LatencyIoState {
                tick: 0,
                pending: VecDeque::new(),
            })),
            read_delay_ticks,
        }
    }

    fn step_once(&self) -> turso_core::Result<()> {
        for _ in 0..SPIN_PER_TICK {
            std::hint::spin_loop();
        }
        let mut due = Vec::new();
        {
            let mut state = self.state.lock().unwrap();
            state.tick += 1;
            let tick = state.tick;
            while state.pending.front().is_some_and(|op| op.due_tick <= tick) {
                due.push(state.pending.pop_front().unwrap());
            }
        }
        for op in due {
            (op.submit)()?;
        }
        Ok(())
    }
}

impl Clock for LatencyIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for LatencyIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        Ok(Arc::new(LatencyFile {
            inner,
            state: self.state.clone(),
            read_delay_ticks: self.read_delay_ticks,
        }))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }

    fn step(&self) -> turso_core::Result<()> {
        self.step_once()
    }

    fn drain_completions(&self, completions: &[Completion]) -> turso_core::Result<()> {
        while completions.iter().any(|c| !c.finished()) {
            self.step_once()?;
        }
        Ok(())
    }

    fn cancel(&self, completions: &[Completion]) -> turso_core::Result<()> {
        for completion in completions {
            completion.abort();
        }
        Ok(())
    }

    fn file_id(&self, path: &str) -> turso_core::Result<FileId> {
        self.inner.file_id(path)
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        self.inner.fill_bytes(dest);
    }

    fn generate_random_number(&self) -> i64 {
        self.inner.generate_random_number()
    }
}

struct LatencyFile {
    inner: Arc<dyn File>,
    state: Arc<Mutex<LatencyIoState>>,
    read_delay_ticks: u64,
}

impl File for LatencyFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, completion: Completion) -> turso_core::Result<Completion> {
        let inner = self.inner.clone();
        let c = completion.clone();
        let mut state = self.state.lock().unwrap();
        let due_tick = state.tick + self.read_delay_ticks;
        state.pending.push_back(DelayedOp {
            due_tick,
            submit: Box::new(move || {
                drop(inner.pread(pos, c)?);
                Ok(())
            }),
        });
        Ok(completion)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        self.inner.pwrite(pos, buffer, completion)
    }

    fn sync(
        &self,
        completion: Completion,
        sync_type: FileSyncType,
    ) -> turso_core::Result<Completion> {
        self.inner.sync(completion, sync_type)
    }

    fn truncate(&self, len: u64, completion: Completion) -> turso_core::Result<Completion> {
        self.inner.truncate(len, completion)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
}

fn open_db(io: Arc<LatencyIo>) -> Arc<Database> {
    Database::open_file_with_flags(
        io,
        DB_PATH,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

fn execute(conn: &Arc<Connection>, io: &Arc<LatencyIo>, sql: &str) {
    let mut stmt = conn.prepare(sql).unwrap();
    run_to_completion(&mut stmt, io);
}

fn run_to_completion(stmt: &mut turso_core::Statement, io: &Arc<LatencyIo>) {
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row());
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
}

fn build_fixture(io: &Arc<LatencyIo>) {
    let db = open_db(io.clone());
    let conn = db.connect().unwrap();
    execute(&conn, io, "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)");
    execute(
        &conn,
        io,
        &format!("INSERT INTO t SELECT value, hex(zeroblob(48)) FROM generate_series(1, {ROWS})"),
    );
    execute(&conn, io, "PRAGMA wal_checkpoint(TRUNCATE)");
}

/// Opens a fresh database instance (cold page cache) and scans the table.
fn scan_cold(io: &Arc<LatencyIo>, prefetch_pages: usize) {
    let db = open_db(io.clone());
    let conn = db.connect().unwrap();
    execute(
        &conn,
        io,
        &format!("PRAGMA prefetch_pages = {prefetch_pages}"),
    );
    let mut stmt = conn.prepare("SELECT count(*), sum(a) FROM t").unwrap();
    run_to_completion(&mut stmt, io);
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_scan_readahead(criterion: &mut Criterion) {
    let io = Arc::new(LatencyIo::new(READ_DELAY_TICKS));
    build_fixture(&io);

    if std::env::var("PREFETCH_BENCH_DEBUG").is_ok() {
        for pages in [0usize, 8, 32, 128] {
            let t0 = io.state.lock().unwrap().tick;
            scan_cold(&io, pages);
            let t1 = io.state.lock().unwrap().tick;
            eprintln!("prefetch_pages={pages}: ticks={}", t1 - t0);
        }
    }

    let mut group = criterion.benchmark_group("scan_readahead_latency_device");
    group.sample_size(10);
    for prefetch_pages in [0usize, 8, 32, 128] {
        group.bench_with_input(
            BenchmarkId::new("prefetch_pages", prefetch_pages),
            &prefetch_pages,
            |b, &prefetch_pages| {
                b.iter(|| scan_cold(&io, prefetch_pages));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_scan_readahead);
criterion_main!(benches);
