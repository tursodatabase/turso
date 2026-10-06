#[cfg(not(feature = "codspeed"))]
use criterion::{criterion_group, criterion_main, Criterion};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::hint::black_box;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;
use turso_core::{
    io::FileSyncType, Buffer, Clock, Completion, Connection, Database, DatabaseOpts, File,
    MonotonicInstant, OpenFlags, PlatformIO, SqliteDialect, WallClockInstant, IO,
};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const BASE_ROWS: i64 = 200_000;
const ID_SPACING: i64 = 1024;
const VALUE_LEN: usize = 100;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Wal,
    SkipWal,
}

#[derive(Clone, Copy, Debug)]
enum Workload {
    Append,
    Update,
    RandomInsert,
}

#[derive(Clone, Copy, Debug)]
enum Sync {
    Full,
    Off,
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_mvcc_checkpoint_skip_wal(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("mvcc-checkpoint-skip-wal");
    let mut out = std::env::var("MVCC_CHECKPOINT_SKIP_WAL_OUT")
        .ok()
        .map(|path| {
            let mut file = std::fs::File::create(path).expect("MVCC_CHECKPOINT_SKIP_WAL_OUT");
            writeln!(file, "{}", Sample::CSV_HEADER).unwrap();
            file
        });
    let mut samples = Vec::new();
    for workload in workloads() {
        for sync in syncs() {
            let mut wal = Bench::open(Mode::Wal, workload, sync);
            let mut skip_wal = Bench::open(Mode::SkipWal, workload, sync);
            eprintln!(
                "atomic write units of {}: {:?}",
                skip_wal.db_path,
                atomic_write_units(&skip_wal.db_path)
            );
            for rows in rows_per_checkpoint() {
                for round in 0..rounds(rows) {
                    for bench in [&mut wal, &mut skip_wal] {
                        let sample = bench.change_rows_and_checkpoint(rows, round);
                        eprintln!("{}", sample.to_csv());
                        if let Some(file) = out.as_mut() {
                            writeln!(file, "{}", sample.to_csv()).unwrap();
                            file.flush().unwrap();
                        }
                        samples.push(sample);
                    }
                }
            }
            wal.assert_same_rows_as(&skip_wal);
        }
    }
    print_summary(&samples);

    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_millis(100));
    group.measurement_time(std::time::Duration::from_secs(1));
    group.bench_function("report", |b| {
        b.iter(|| black_box(1u8));
    });
    group.finish();
}

fn workloads() -> Vec<Workload> {
    if cfg!(feature = "codspeed") {
        return vec![Workload::Append];
    }
    [Workload::Append, Workload::Update, Workload::RandomInsert]
        .into_iter()
        .filter(|workload| selected("MVCC_CHECKPOINT_SKIP_WAL_WORKLOADS", workload))
        .collect()
}

fn syncs() -> Vec<Sync> {
    if cfg!(feature = "codspeed") {
        return vec![Sync::Off];
    }
    [Sync::Full, Sync::Off]
        .into_iter()
        .filter(|sync| selected("MVCC_CHECKPOINT_SKIP_WAL_SYNCS", sync))
        .collect()
}

fn rows_per_checkpoint() -> Vec<i64> {
    if cfg!(feature = "codspeed") {
        return vec![1_000];
    }
    [100, 1_000, 10_000, 100_000]
        .into_iter()
        .filter(|rows| selected("MVCC_CHECKPOINT_SKIP_WAL_ROWS", rows))
        .collect()
}

fn selected(env_var: &str, value: &impl std::fmt::Debug) -> bool {
    std::env::var(env_var).map_or(true, |list| {
        list.split(',')
            .any(|item| item.trim().eq_ignore_ascii_case(&format!("{value:?}")))
    })
}

fn rounds(rows: i64) -> usize {
    match rows {
        rows if rows <= 100 => 20,
        rows if rows <= 1_000 => 10,
        rows if rows <= 10_000 => 5,
        _ => 3,
    }
}

struct Bench {
    mode: Mode,
    workload: Workload,
    sync: Sync,
    _db: Arc<Database>,
    conn: Arc<Connection>,
    counters: Arc<IoCounters>,
    rng: ChaCha8Rng,
    next_append_id: i64,
    db_path: String,
    _dir: TempDir,
}

impl Bench {
    fn open(mode: Mode, workload: Workload, sync: Sync) -> Self {
        let dir =
            tempfile::tempdir_in(std::env::var("BENCH_DIR").unwrap_or(".".to_string())).unwrap();
        let db_path = dir.path().join("bench.db").to_str().unwrap().to_string();
        let counters = Arc::new(IoCounters::default());
        let io = Arc::new(CountingIo {
            inner: Arc::new(PlatformIO::new().unwrap()),
            counters: counters.clone(),
        });
        let db = Database::open_file_with_flags(
            io,
            &db_path,
            OpenFlags::default(),
            DatabaseOpts::new().with_experimental_mvcc_checkpoint_skip_wal(mode == Mode::SkipWal),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        if let Ok(cache_size) = std::env::var("MVCC_CHECKPOINT_SKIP_WAL_CACHE_SIZE") {
            conn.execute(format!("PRAGMA cache_size = {cache_size}"))
                .unwrap();
        }
        conn.execute(match sync {
            Sync::Full => "PRAGMA synchronous = FULL",
            Sync::Off => "PRAGMA synchronous = OFF",
        })
        .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
            .unwrap();
        let mut bench = Self {
            mode,
            workload,
            sync,
            _db: db,
            conn,
            counters,
            rng: ChaCha8Rng::seed_from_u64(42),
            next_append_id: BASE_ROWS * ID_SPACING,
            db_path,
            _dir: dir,
        };
        bench.insert_rows((0..BASE_ROWS).map(|k| k * ID_SPACING));
        bench
            .conn
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        bench
    }

    fn change_rows_and_checkpoint(&mut self, rows: i64, round: usize) -> Sample {
        match self.workload {
            Workload::Append => {
                let first = self.next_append_id;
                self.next_append_id += rows;
                self.insert_rows(first..first + rows);
            }
            Workload::Update => {
                let ids: Vec<i64> = (0..rows)
                    .map(|_| self.rng.random_range(0..BASE_ROWS) * ID_SPACING)
                    .collect();
                self.update_rows(&ids);
            }
            Workload::RandomInsert => {
                let ids: Vec<i64> = (0..rows)
                    .map(|_| {
                        self.rng.random_range(0..BASE_ROWS) * ID_SPACING
                            + self.rng.random_range(1..ID_SPACING)
                    })
                    .collect();
                self.insert_rows(ids);
            }
        }
        let db_size_before = std::fs::metadata(&self.db_path).unwrap().len();
        let io_before = self.counters.snapshot();
        let started = Instant::now();
        self.conn
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        let took = started.elapsed();
        let io = self.counters.snapshot().minus(&io_before);
        let db_size_after = std::fs::metadata(&self.db_path).unwrap().len();
        Sample {
            mode: self.mode,
            workload: self.workload,
            sync: self.sync,
            rows,
            round,
            ms: took.as_secs_f64() * 1e3,
            io,
            db_size_before,
            db_size_after,
        }
    }

    fn insert_rows(&mut self, ids: impl IntoIterator<Item = i64>) {
        self.conn.execute("BEGIN CONCURRENT").unwrap();
        let mut stmt = self
            .conn
            .prepare("INSERT OR IGNORE INTO t VALUES (?, ?)")
            .unwrap();
        for id in ids {
            stmt.bind_at(1.try_into().unwrap(), turso_core::Value::from_i64(id))
                .unwrap();
            stmt.bind_at(2.try_into().unwrap(), self.value()).unwrap();
            stmt.run_ignore_rows().unwrap();
            stmt.reset().unwrap();
        }
        drop(stmt);
        self.conn.execute("COMMIT").unwrap();
    }

    fn update_rows(&mut self, ids: &[i64]) {
        self.conn.execute("BEGIN CONCURRENT").unwrap();
        let mut stmt = self
            .conn
            .prepare("UPDATE t SET v = ? WHERE id = ?")
            .unwrap();
        for id in ids {
            stmt.bind_at(1.try_into().unwrap(), self.value()).unwrap();
            stmt.bind_at(2.try_into().unwrap(), turso_core::Value::from_i64(*id))
                .unwrap();
            stmt.run_ignore_rows().unwrap();
            stmt.reset().unwrap();
        }
        drop(stmt);
        self.conn.execute("COMMIT").unwrap();
    }

    fn value(&mut self) -> turso_core::Value {
        let value: String = (0..VALUE_LEN)
            .map(|_| self.rng.random_range(b'a'..=b'z') as char)
            .collect();
        turso_core::Value::build_text(value)
    }

    fn assert_same_rows_as(&self, other: &Bench) {
        let summary = |bench: &Bench| {
            let mut stmt = bench
                .conn
                .prepare("SELECT count(*), sum(id), sum(length(v)) FROM t")
                .unwrap();
            let mut summary = Vec::new();
            stmt.run_with_row_callback(|row| {
                summary = row.get_values().map(|value| value.to_string()).collect();
                Ok(())
            })
            .unwrap();
            summary
        };
        assert_eq!(summary(self), summary(other));
    }
}

#[cfg(target_os = "linux")]
fn atomic_write_units(path: &str) -> Option<(u32, u32)> {
    const STATX_WRITE_ATOMIC: u32 = 0x0001_0000;
    #[repr(C)]
    struct StatxWithAtomicWrite {
        mask: u32,
        fields_before_atomic_write: [u8; 0xa8 - 4],
        atomic_write_unit_min: u32,
        atomic_write_unit_max: u32,
        fields_after_atomic_write: [u8; 0x100 - 0xb0],
    }
    let path = std::ffi::CString::new(path).ok()?;
    let mut statx = std::mem::MaybeUninit::<StatxWithAtomicWrite>::zeroed();
    let rc = unsafe {
        libc::statx(
            libc::AT_FDCWD,
            path.as_ptr(),
            0,
            STATX_WRITE_ATOMIC,
            statx.as_mut_ptr().cast(),
        )
    };
    if rc != 0 {
        return None;
    }
    let statx = unsafe { statx.assume_init() };
    (statx.mask & STATX_WRITE_ATOMIC != 0 && statx.atomic_write_unit_max > 0)
        .then_some((statx.atomic_write_unit_min, statx.atomic_write_unit_max))
}

#[cfg(not(target_os = "linux"))]
fn atomic_write_units(_path: &str) -> Option<(u32, u32)> {
    None
}

struct Sample {
    mode: Mode,
    workload: Workload,
    sync: Sync,
    rows: i64,
    round: usize,
    ms: f64,
    io: IoSnapshot,
    db_size_before: u64,
    db_size_after: u64,
}

impl Sample {
    const CSV_HEADER: &str = "workload,sync,rows,mode,round,ms,db_bytes,db_writes,db_syncs,wal_bytes,wal_writes,wal_syncs,wal_truncates,log_syncs,log_truncates,db_size_before,db_size_after";

    fn to_csv(&self) -> String {
        let db = &self.io.files[FileKind::Db as usize];
        let wal = &self.io.files[FileKind::Wal as usize];
        let log = &self.io.files[FileKind::Log as usize];
        format!(
            "{:?},{:?},{},{:?},{},{:.3},{},{},{},{},{},{},{},{},{},{},{}",
            self.workload,
            self.sync,
            self.rows,
            self.mode,
            self.round,
            self.ms,
            db.bytes_written,
            db.writes,
            db.syncs,
            wal.bytes_written,
            wal.writes,
            wal.syncs,
            wal.truncates,
            log.syncs,
            log.truncates,
            self.db_size_before,
            self.db_size_after,
        )
    }
}

fn print_summary(samples: &[Sample]) {
    eprintln!();
    eprintln!("workload,sync,rows,wal_p50_ms,skip_wal_p50_ms,speedup,wal_mib_written,skip_wal_mib_written,wal_fsyncs,skip_wal_fsyncs,db_pages_written,new_db_pages");
    let mut keys: Vec<(String, String, i64)> = samples
        .iter()
        .map(|s| (format!("{:?}", s.workload), format!("{:?}", s.sync), s.rows))
        .collect();
    keys.dedup();
    for (workload, sync, rows) in keys {
        let of_mode = |mode: Mode| -> Vec<&Sample> {
            samples
                .iter()
                .filter(|s| {
                    s.mode == mode
                        && format!("{:?}", s.workload) == workload
                        && format!("{:?}", s.sync) == sync
                        && s.rows == rows
                })
                .collect()
        };
        let wal = of_mode(Mode::Wal);
        let skip_wal = of_mode(Mode::SkipWal);
        let p50 = |samples: &[&Sample]| {
            let mut ms: Vec<f64> = samples.iter().map(|s| s.ms).collect();
            ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
            ms[ms.len() / 2]
        };
        let mean = |samples: &[&Sample], f: &dyn Fn(&Sample) -> f64| {
            samples.iter().map(|s| f(s)).sum::<f64>() / samples.len() as f64
        };
        let mib = |s: &Sample| s.io.total_bytes_written() as f64 / (1024.0 * 1024.0);
        let fsyncs = |s: &Sample| s.io.total_syncs() as f64;
        let db_pages = |s: &Sample| s.io.files[FileKind::Db as usize].bytes_written as f64 / 4096.0;
        let new_pages = |s: &Sample| (s.db_size_after - s.db_size_before) as f64 / 4096.0;
        let (wal_p50, skip_wal_p50) = (p50(&wal), p50(&skip_wal));
        eprintln!(
            "{workload},{sync},{rows},{wal_p50:.2},{skip_wal_p50:.2},{:.2},{:.2},{:.2},{:.1},{:.1},{:.0},{:.0}",
            wal_p50 / skip_wal_p50,
            mean(&wal, &mib),
            mean(&skip_wal, &mib),
            mean(&wal, &fsyncs),
            mean(&skip_wal, &fsyncs),
            mean(&skip_wal, &db_pages),
            mean(&skip_wal, &new_pages),
        );
    }
}

#[derive(Clone, Copy)]
enum FileKind {
    Db = 0,
    Wal = 1,
    Log = 2,
}

impl FileKind {
    fn of(path: &str) -> Self {
        if path.ends_with("-wal") {
            FileKind::Wal
        } else if path.ends_with("-log") {
            FileKind::Log
        } else {
            FileKind::Db
        }
    }
}

#[derive(Default)]
struct FileCounters {
    bytes_written: AtomicU64,
    writes: AtomicU64,
    syncs: AtomicU64,
    truncates: AtomicU64,
}

#[derive(Default)]
struct IoCounters {
    files: [FileCounters; 3],
}

#[derive(Clone, Copy, Default)]
struct FileIo {
    bytes_written: u64,
    writes: u64,
    syncs: u64,
    truncates: u64,
}

#[derive(Clone, Copy, Default)]
struct IoSnapshot {
    files: [FileIo; 3],
}

impl IoCounters {
    fn snapshot(&self) -> IoSnapshot {
        let mut snapshot = IoSnapshot::default();
        for (counters, file) in self.files.iter().zip(snapshot.files.iter_mut()) {
            *file = FileIo {
                bytes_written: counters.bytes_written.load(Ordering::Relaxed),
                writes: counters.writes.load(Ordering::Relaxed),
                syncs: counters.syncs.load(Ordering::Relaxed),
                truncates: counters.truncates.load(Ordering::Relaxed),
            };
        }
        snapshot
    }
}

impl IoSnapshot {
    fn minus(&self, earlier: &IoSnapshot) -> IoSnapshot {
        let mut delta = IoSnapshot::default();
        for ((now, before), out) in self
            .files
            .iter()
            .zip(earlier.files.iter())
            .zip(delta.files.iter_mut())
        {
            *out = FileIo {
                bytes_written: now.bytes_written - before.bytes_written,
                writes: now.writes - before.writes,
                syncs: now.syncs - before.syncs,
                truncates: now.truncates - before.truncates,
            };
        }
        delta
    }

    fn total_bytes_written(&self) -> u64 {
        self.files.iter().map(|file| file.bytes_written).sum()
    }

    fn total_syncs(&self) -> u64 {
        self.files.iter().map(|file| file.syncs).sum()
    }
}

struct CountingIo {
    inner: Arc<dyn IO>,
    counters: Arc<IoCounters>,
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
            kind: FileKind::of(path),
            inner: self.inner.open_file(path, flags, direct)?,
            counters: self.counters.clone(),
        }))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.inner.remove_file(path)
    }

    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }

    fn cancel(&self, completions: &[Completion]) -> turso_core::Result<()> {
        self.inner.cancel(completions)
    }

    fn drain_completions(&self, completions: &[Completion]) -> turso_core::Result<()> {
        self.inner.drain_completions(completions)
    }

    fn file_id(&self, path: &str) -> turso_core::Result<turso_core::io::FileId> {
        self.inner.file_id(path)
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        self.inner.fill_bytes(dest);
    }

    fn generate_random_number(&self) -> i64 {
        self.inner.generate_random_number()
    }
}

struct CountingFile {
    kind: FileKind,
    inner: Arc<dyn File>,
    counters: Arc<IoCounters>,
}

impl CountingFile {
    fn counters(&self) -> &FileCounters {
        &self.counters.files[self.kind as usize]
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
        self.inner.pread(pos, c)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        let counters = self.counters();
        counters
            .bytes_written
            .fetch_add(buffer.len() as u64, Ordering::Relaxed);
        counters.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.pwrite(pos, buffer, c)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        let counters = self.counters();
        let bytes: usize = buffers.iter().map(|buffer| buffer.len()).sum();
        counters
            .bytes_written
            .fetch_add(bytes as u64, Ordering::Relaxed);
        counters.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.pwritev(pos, buffers, c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        self.counters().syncs.fetch_add(1, Ordering::Relaxed);
        self.inner.sync(c, sync_type)
    }

    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.counters().truncates.fetch_add(1, Ordering::Relaxed);
        self.inner.truncate(len, c)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
}

criterion_group! {
    name = mvcc_checkpoint_skip_wal_benches;
    config = Criterion::default();
    targets = bench_mvcc_checkpoint_skip_wal
}

criterion_main!(mvcc_checkpoint_skip_wal_benches);
