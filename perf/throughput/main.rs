//! Concurrent write throughput benchmark.
//!
//! A fixed number of connections each run small write transactions back
//! to back, closed loop: the next transaction starts the moment the previous
//! one commits. Throughput is how many of them commit per second over a
//! fixed measured window, and it is reported next to the CPU the process
//! spent per transaction and what the disk did, because throughput on its
//! own says nothing about what it cost. Every transaction's service time
//! is kept too, so the latency under saturation can be looked at.

mod sqlite_engine;
mod turso_engine;

use clap::{Parser, ValueEnum};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, OnceLock,
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Engine {
    Sqlite,
    Turso,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TxnMode {
    /// WAL journal, one writer at a time, started with `BEGIN IMMEDIATE`.
    Immediate,
    /// MVCC journal with `BEGIN CONCURRENT` and passive checkpointing.
    Concurrent,
}

impl TxnMode {
    fn label(self) -> &'static str {
        match self {
            TxnMode::Immediate => "immediate",
            TxnMode::Concurrent => "concurrent",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CheckpointMode {
    /// Checkpoint without blocking other connections. Row versions already
    /// written to the B-tree stay in memory until a later GC pass.
    Passive,
    /// Blocking checkpoint that drops row versions once they are in the B-tree.
    Truncate,
}

#[derive(Parser)]
#[command(name = "txn-throughput")]
#[command(about = "Concurrent write throughput benchmark for SQLite and Turso")]
struct Args {
    #[arg(short = 'e', long = "engine")]
    engine: Engine,

    #[arg(
        short = 'c',
        long = "connections",
        default_value = "1",
        help = "Connections writing at once, each on its own thread, each starting its \
                next transaction as soon as the previous one commits"
    )]
    connections: usize,

    #[arg(
        short = 'b',
        long = "batch-size",
        default_value = "100",
        help = "Rows inserted per transaction"
    )]
    batch_size: usize,

    #[arg(
        short = 'd',
        long = "duration",
        default_value = "30",
        help = "Measurement time in seconds"
    )]
    duration: u64,

    #[arg(
        long = "warmup",
        default_value = "3",
        help = "Warmup time in seconds; its transactions are recorded but flagged"
    )]
    warmup: u64,

    #[arg(
        long = "timeout",
        default_value = "60000",
        help = "Busy timeout in milliseconds"
    )]
    timeout: u64,

    #[arg(
        short = 'm',
        long = "mode",
        help = "Defaults to immediate for SQLite and concurrent for Turso"
    )]
    mode: Option<TxnMode>,

    #[arg(
        long = "checkpoint-mode",
        default_value = "passive",
        help = "Turso MVCC auto-checkpoint mode in concurrent mode. SQLite ignores this"
    )]
    checkpoint_mode: CheckpointMode,

    #[arg(
        long = "mvcc-checkpoint-threshold",
        help = "Bytes of logical log between Turso MVCC auto-checkpoints (default 4120000). SQLite ignores this"
    )]
    mvcc_checkpoint_threshold: Option<i64>,

    #[arg(
        long = "checkpointer",
        value_name = "MS",
        default_value = "1000",
        help = "Checkpoint from a separate connection every MS milliseconds instead of on the \
                writers' commit path, for both engines. 0 lets each writer auto-checkpoint \
                itself, which is what the engines do out of the box"
    )]
    checkpointer: u64,

    #[arg(
        long = "run",
        default_value = "1",
        help = "Number of this run among repeats of the same configuration; it names the \
                database and output files"
    )]
    run: usize,

    #[arg(
        long = "db-dir",
        default_value = ".",
        help = "Directory for the database files. Every run gets its own file there and a \
                run refuses to start on a file that already exists. Nothing is ever deleted"
    )]
    db_dir: std::path::PathBuf,

    #[arg(
        long = "out-dir",
        default_value = ".",
        help = "Directory for the results. Every run writes its own files there and \
                refuses to overwrite ones that exist"
    )]
    out_dir: std::path::PathBuf,

    #[arg(
        long = "io",
        default_value = DEFAULT_IO,
        help = "Turso IO backend: io_uring (default on Linux) or syscall. SQLite ignores this"
    )]
    io: String,
}

#[cfg(target_os = "linux")]
const DEFAULT_IO: &str = "io_uring";
#[cfg(not(target_os = "linux"))]
const DEFAULT_IO: &str = "syscall";

/// One transaction, as its connection saw it.
pub struct Sample {
    /// When it started, from the start of the run.
    pub started_ns: u64,
    /// True during warmup. Kept so the start of a run can be looked at; left
    /// out of every summary and plot.
    pub warmup: bool,
    /// How many times it had to start over on a stale snapshot.
    pub restarts: u32,
    pub begin_ns: u64,
    pub work_ns: u64,
    pub commit_ns: u64,
    /// `BEGIN` to successful `COMMIT`, restarts included.
    pub total_ns: u64,
}

pub struct Config {
    pub db_path: String,
    pub run: usize,
    pub connections: usize,
    pub batch_size: usize,
    pub warmup: Duration,
    pub duration: Duration,
    pub timeout: Duration,
    pub mode: TxnMode,
    pub io: String,
    pub checkpoint_mode: CheckpointMode,
    pub mvcc_checkpoint_threshold: Option<i64>,
    pub checkpointer: Option<Duration>,
}

impl Config {
    fn stop_at(&self) -> Duration {
        self.warmup + self.duration
    }
}

pub struct Checkpoint {
    /// When it started, from the start of the run.
    pub at: Duration,
    pub took: Duration,
}

pub struct Run {
    pub per_connection: Vec<Vec<Sample>>,
    pub checkpoints: Vec<Checkpoint>,
    /// Wall time from the first transaction to the last commit.
    pub elapsed: Duration,
    /// What the table held when the connections were done, read back through
    /// a fresh connection.
    pub rows_in_table: u64,
}

/// The run's clock, shared by every connection. It starts when the first
/// one asks for work, after every connection is open and prepared, so setup
/// does not eat into the measured time.
pub struct Clock {
    start: OnceLock<Instant>,
    stop_at: Duration,
    warmup: Duration,
}

pub struct Start {
    pub at: Instant,
    /// The same, from the start of the run.
    pub offset: Duration,
    pub warmup: bool,
}

impl Clock {
    pub fn new(config: &Config) -> Self {
        Self {
            start: OnceLock::new(),
            stop_at: config.stop_at(),
            warmup: config.warmup,
        }
    }

    pub fn started_at(&self) -> Instant {
        *self.start.get_or_init(Instant::now)
    }

    pub fn elapsed(&self) -> Duration {
        self.start.get().map(Instant::elapsed).unwrap_or_default()
    }

    /// The start of the next transaction, or `None` once the run is over.
    pub fn next(&self) -> Option<Start> {
        let offset = self.started_at().elapsed();
        if offset >= self.stop_at {
            return None;
        }
        Some(Start {
            at: Instant::now(),
            offset,
            warmup: offset < self.warmup,
        })
    }
}

/// Row ids, unique across connections and transactions within a run.
pub static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Transactions committed so far, over every connection. The sampler reads it
/// once a second.
pub static COMMITTED: AtomicU64 = AtomicU64::new(0);

/// One second of the run, as the sampler saw it.
struct Tick {
    second: u64,
    warmup: bool,
    transactions: u64,
    cpu: CpuTime,
}

/// Samples the commit count and the process's CPU time once a second while
/// the run goes, so the run has a time series and not only totals: a box
/// of CPU utilization needs a distribution, and a throughput that sagged
/// halfway through needs to be visible as such.
fn sample(stop: Arc<AtomicBool>, warmup: Duration) -> Vec<Tick> {
    let mut ticks = Vec::new();
    let started = Instant::now();
    let (mut last_transactions, mut last_cpu) = (COMMITTED.load(Ordering::Relaxed), cpu_time());
    let mut second = 0u64;
    while !stop.load(Ordering::Relaxed) {
        second += 1;
        let due = started + Duration::from_secs(second);
        while Instant::now() < due {
            if stop.load(Ordering::Relaxed) {
                return ticks;
            }
            thread::sleep(Duration::from_millis(10));
        }
        let (transactions, cpu) = (COMMITTED.load(Ordering::Relaxed), cpu_time());
        ticks.push(Tick {
            second,
            warmup: Duration::from_secs(second) <= warmup,
            transactions: transactions - last_transactions,
            cpu: cpu - last_cpu,
        });
        (last_transactions, last_cpu) = (transactions, cpu);
    }
    ticks
}

fn main() {
    let args = Args::parse();

    let mode = args.mode.unwrap_or(match args.engine {
        Engine::Sqlite => TxnMode::Immediate,
        Engine::Turso => TxnMode::Concurrent,
    });
    assert!(
        args.engine != Engine::Sqlite || mode == TxnMode::Immediate,
        "SQLite only supports --mode immediate"
    );
    assert!(args.connections > 0, "--connections must be at least 1");

    let engine_label = match args.engine {
        Engine::Sqlite => "sqlite",
        Engine::Turso => "turso",
    };
    let tag = format!("{engine_label}/{}", mode.label());

    std::fs::create_dir_all(&args.db_dir).expect("cannot create the database directory");
    std::fs::create_dir_all(&args.out_dir).expect("cannot create the output directory");
    let run_name = format!("{engine_label}-c{}-r{}", args.connections, args.run);
    let db_path = args.db_dir.join(format!("{run_name}.db"));
    let samples_path = args.out_dir.join(format!("{run_name}.csv"));
    let checkpoints_path = args.out_dir.join(format!("{run_name}-checkpoints.csv"));
    let timeline_path = args.out_dir.join(format!("{run_name}-timeline.csv"));
    let result_path = args.out_dir.join(format!("{run_name}-result.csv"));
    for (path, flag) in [
        (&db_path, "--db-dir"),
        (&samples_path, "--out-dir"),
        (&checkpoints_path, "--out-dir"),
        (&timeline_path, "--out-dir"),
        (&result_path, "--out-dir"),
    ] {
        if path.exists() {
            eprintln!(
                "[{tag}] {} already exists; remove it or pass another {flag}",
                path.display()
            );
            std::process::exit(1);
        }
    }

    let config = Config {
        db_path: db_path.to_string_lossy().into_owned(),
        run: args.run,
        connections: args.connections,
        batch_size: args.batch_size,
        warmup: Duration::from_secs(args.warmup),
        duration: Duration::from_secs(args.duration),
        timeout: Duration::from_millis(args.timeout),
        mode,
        io: args.io,
        checkpoint_mode: args.checkpoint_mode,
        mvcc_checkpoint_threshold: args.mvcc_checkpoint_threshold,
        checkpointer: (args.checkpointer > 0).then(|| Duration::from_millis(args.checkpointer)),
    };

    let cpu_before = cpu_time();
    let disk_before = DiskStats::for_path(&args.db_dir);
    let stop_sampling = Arc::new(AtomicBool::new(false));
    let sampler = {
        let stop = Arc::clone(&stop_sampling);
        let warmup = config.warmup;
        thread::spawn(move || sample(stop, warmup))
    };
    let run = match args.engine {
        Engine::Sqlite => sqlite_engine::run(&config),
        Engine::Turso => turso_engine::run(&config),
    };
    stop_sampling.store(true, Ordering::Relaxed);
    let ticks = sampler.join().expect("sampler thread panicked");
    let cpu = cpu_time() - cpu_before;
    let disk = DiskStats::for_path(&args.db_dir)
        .zip(disk_before)
        .map(|(a, b)| a - b);

    // Every committed transaction, warmup included, must be in the table.
    let committed: u64 = run.per_connection.iter().map(|s| s.len() as u64).sum();
    let expected_rows = committed * config.batch_size as u64;
    if run.rows_in_table != expected_rows {
        eprintln!(
            "[{tag}] the table holds {} rows but {committed} transactions of {} rows committed",
            run.rows_in_table, config.batch_size
        );
        std::process::exit(1);
    }

    let result = Outcome::from(&config, &run, cpu, disk.as_ref());
    result.report(&tag, &run);
    write_samples(&samples_path, engine_label, &config, &run);
    write_checkpoints(&checkpoints_path, engine_label, &config, &run);
    write_timeline(&timeline_path, engine_label, &config, &ticks);
    result.write(&result_path, engine_label, &config);
    eprintln!("[{tag}] wrote {}", result_path.display());
}

/// The numbers one run boils down to.
struct Outcome {
    transactions: u64,
    rows: u64,
    seconds: f64,
    transactions_per_s: f64,
    cpu: CpuTime,
    cpu_us_per_transaction: f64,
    restarts: u64,
    p50_ms: f64,
    p99_ms: f64,
    p999_ms: f64,
    max_ms: f64,
    disk: Option<DiskStats>,
    checkpoints: usize,
    checkpoint_p50_ms: f64,
    checkpoint_max_ms: f64,
}

impl Outcome {
    fn from(config: &Config, run: &Run, cpu: CpuTime, disk: Option<&DiskStats>) -> Self {
        // Only transactions that started inside the measured window count,
        // over exactly that window: the same wall time for every engine.
        let measured: Vec<&Sample> = run
            .per_connection
            .iter()
            .flatten()
            .filter(|s| !s.warmup)
            .collect();
        let transactions = measured.len() as u64;
        let seconds = config.duration.as_secs_f64();
        let mut totals: Vec<u64> = measured.iter().map(|s| s.total_ns).collect();
        totals.sort_unstable();
        let quantile = |q: f64| {
            if totals.is_empty() {
                return 0.0;
            }
            let idx = ((totals.len() as f64 - 1.0) * q).round() as usize;
            totals[idx] as f64 / 1e6
        };
        let mut checkpoint_ms: Vec<f64> = run
            .checkpoints
            .iter()
            .map(|c| c.took.as_secs_f64() * 1e3)
            .collect();
        checkpoint_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let busy = (cpu.user + cpu.system).as_secs_f64();
        Self {
            transactions,
            rows: transactions * config.batch_size as u64,
            seconds,
            transactions_per_s: transactions as f64 / seconds,
            cpu,
            // The whole process over the whole run, warmup and checkpointer
            // included, per measured transaction: what a transaction costs
            // when the engine is the only thing running.
            cpu_us_per_transaction: if transactions > 0 {
                busy * 1e6 / transactions as f64
            } else {
                0.0
            },
            restarts: measured.iter().map(|s| s.restarts as u64).sum(),
            p50_ms: quantile(0.5),
            p99_ms: quantile(0.99),
            p999_ms: quantile(0.999),
            max_ms: quantile(1.0),
            disk: disk.cloned(),
            checkpoints: run.checkpoints.len(),
            checkpoint_p50_ms: checkpoint_ms
                .get(checkpoint_ms.len() / 2)
                .copied()
                .unwrap_or(0.0),
            checkpoint_max_ms: checkpoint_ms.last().copied().unwrap_or(0.0),
        }
    }

    fn report(&self, tag: &str, run: &Run) {
        eprintln!(
            "[{tag}] {} transactions, {} rows in {:.1}s: {:.0} transactions/s, {:.0} rows/s",
            self.transactions,
            self.rows,
            self.seconds,
            self.transactions_per_s,
            self.rows as f64 / self.seconds
        );
        eprintln!(
            "[{tag}] service time p50 {:.2}ms  p99 {:.2}ms  p99.9 {:.2}ms  max {:.2}ms, {} restarts",
            self.p50_ms, self.p99_ms, self.p999_ms, self.max_ms, self.restarts
        );
        let hardware_threads = std::thread::available_parallelism().map_or(1, |n| n.get());
        let wall = run.elapsed.as_secs_f64().max(f64::EPSILON);
        let busy = (self.cpu.user + self.cpu.system).as_secs_f64();
        eprintln!(
            "[{tag}] cpu: user {:.1}s  sys {:.1}s  {:.0}% of one core over {wall:.1}s, \
             {:.1}% of {hardware_threads} hardware threads, {:.0}us per transaction",
            self.cpu.user.as_secs_f64(),
            self.cpu.system.as_secs_f64(),
            busy / wall * 100.0,
            busy / wall / hardware_threads as f64 * 100.0,
            self.cpu_us_per_transaction
        );
        if let Some(disk) = &self.disk {
            eprintln!(
                "[{tag}] disk {}: {} writes, {:.1} MB written, {:.2}ms per write, busy {:.0}% of the run",
                disk.device,
                disk.writes,
                disk.megabytes(),
                disk.ms_per_write(),
                disk.busy_ms as f64 / wall / 10.0
            );
        }
        if !run.checkpoints.is_empty() {
            eprintln!(
                "[{tag}] checkpointer: {} checkpoints, p50 {:.1}ms  max {:.1}ms",
                run.checkpoints.len(),
                self.checkpoint_p50_ms,
                self.checkpoint_max_ms
            );
        }
    }

    fn write(&self, path: &std::path::Path, engine_label: &str, config: &Config) {
        use std::io::Write;
        let mut out = std::fs::File::create(path).expect("cannot create the result file");
        writeln!(
            out,
            "engine,mode,connections,batch_size,run,transactions,rows,seconds,\
             transactions_per_s,rows_per_s,cpu_user_s,cpu_sys_s,cpu_us_per_transaction,\
             hardware_threads,restarts,\
             p50_ms,p99_ms,p999_ms,max_ms,disk_writes,disk_mb,disk_ms_per_write,\
             checkpoints,checkpoint_p50_ms,checkpoint_max_ms"
        )
        .unwrap();
        let (disk_writes, disk_mb, disk_ms_per_write) = match &self.disk {
            Some(d) => (d.writes, d.megabytes(), d.ms_per_write()),
            None => (0, 0.0, 0.0),
        };
        writeln!(
            out,
            "{engine_label},{},{},{},{},{},{},{:.3},{:.2},{:.2},{:.3},{:.3},{:.2},{},{},\
             {:.3},{:.3},{:.3},{:.3},{disk_writes},{disk_mb:.1},{disk_ms_per_write:.3},\
             {},{:.2},{:.2}",
            config.mode.label(),
            config.connections,
            config.batch_size,
            config.run,
            self.transactions,
            self.rows,
            self.seconds,
            self.transactions_per_s,
            self.rows as f64 / self.seconds,
            self.cpu.user.as_secs_f64(),
            self.cpu.system.as_secs_f64(),
            self.cpu_us_per_transaction,
            std::thread::available_parallelism().map_or(1, |n| n.get()),
            self.restarts,
            self.p50_ms,
            self.p99_ms,
            self.p999_ms,
            self.max_ms,
            self.checkpoints,
            self.checkpoint_p50_ms,
            self.checkpoint_max_ms
        )
        .unwrap();
    }
}

fn write_samples(path: &std::path::Path, engine_label: &str, config: &Config, run: &Run) {
    use std::io::Write;
    let file = std::fs::File::create(path).expect("cannot create the samples file");
    let mut out = std::io::BufWriter::new(file);
    writeln!(
        out,
        "engine,mode,connections,run,connection,started_ns,warmup,restarts,begin_ns,work_ns,commit_ns,total_ns"
    )
    .unwrap();
    for (connection, samples) in run.per_connection.iter().enumerate() {
        for s in samples {
            writeln!(
                out,
                "{engine_label},{},{},{},{connection},{},{},{},{},{},{},{}",
                config.mode.label(),
                config.connections,
                config.run,
                s.started_ns,
                s.warmup as u8,
                s.restarts,
                s.begin_ns,
                s.work_ns,
                s.commit_ns,
                s.total_ns
            )
            .unwrap();
        }
    }
    out.flush().unwrap();
}

fn write_checkpoints(path: &std::path::Path, engine_label: &str, config: &Config, run: &Run) {
    use std::io::Write;
    let file = std::fs::File::create(path).expect("cannot create the checkpoints file");
    let mut out = std::io::BufWriter::new(file);
    writeln!(out, "engine,mode,connections,run,at_ns,took_ns").unwrap();
    for c in &run.checkpoints {
        writeln!(
            out,
            "{engine_label},{},{},{},{},{}",
            config.mode.label(),
            config.connections,
            config.run,
            c.at.as_nanos(),
            c.took.as_nanos()
        )
        .unwrap();
    }
    out.flush().unwrap();
}

fn write_timeline(path: &std::path::Path, engine_label: &str, config: &Config, ticks: &[Tick]) {
    use std::io::Write;
    let file = std::fs::File::create(path).expect("cannot create the timeline file");
    let mut out = std::io::BufWriter::new(file);
    let hardware_threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    writeln!(
        out,
        "engine,mode,connections,run,second,warmup,transactions,cpu_user_s,cpu_sys_s,cpu_percent"
    )
    .unwrap();
    for t in ticks {
        let busy = (t.cpu.user + t.cpu.system).as_secs_f64();
        writeln!(
            out,
            "{engine_label},{},{},{},{},{},{},{:.3},{:.3},{:.2}",
            config.mode.label(),
            config.connections,
            config.run,
            t.second,
            t.warmup as u8,
            t.transactions,
            t.cpu.user.as_secs_f64(),
            t.cpu.system.as_secs_f64(),
            busy / hardware_threads as f64 * 100.0
        )
        .unwrap();
    }
    out.flush().unwrap();
}

/// CPU time this process has used so far, user and system.
#[derive(Debug, Clone, Copy)]
struct CpuTime {
    user: Duration,
    system: Duration,
}

impl std::ops::Sub for CpuTime {
    type Output = CpuTime;
    fn sub(self, earlier: CpuTime) -> CpuTime {
        CpuTime {
            user: self.user.saturating_sub(earlier.user),
            system: self.system.saturating_sub(earlier.system),
        }
    }
}

fn cpu_time() -> CpuTime {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: RUSAGE_SELF is always valid and getrusage fills the struct on
    // success, which is the only way it returns zero.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(rc, 0, "getrusage failed");
    let usage = unsafe { usage.assume_init() };
    let duration = |t: libc::timeval| Duration::new(t.tv_sec as u64, (t.tv_usec as u32) * 1000);
    CpuTime {
        user: duration(usage.ru_utime),
        system: duration(usage.ru_stime),
    }
}

/// The kernel's counters for the block device a path lives on, from
/// /proc/diskstats. `None` off Linux or when the device is not listed.
#[derive(Debug, Clone)]
struct DiskStats {
    device: String,
    writes: u64,
    sectors_written: u64,
    /// Time spent writing, summed over writes in flight, so per-write it is
    /// the average wait a write saw.
    write_ms: u64,
    /// Time the device had any I/O in flight.
    busy_ms: u64,
}

impl DiskStats {
    fn for_path(path: &std::path::Path) -> Option<DiskStats> {
        use std::os::unix::fs::MetadataExt;
        let dev = std::fs::metadata(path).ok()?.dev();
        let (major, minor) = (libc::major(dev), libc::minor(dev));
        let stats = std::fs::read_to_string("/proc/diskstats").ok()?;
        stats.lines().find_map(|line| {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() < 14 || f[0] != major.to_string() || f[1] != minor.to_string() {
                return None;
            }
            Some(DiskStats {
                device: f[2].to_string(),
                writes: f[7].parse().ok()?,
                sectors_written: f[9].parse().ok()?,
                write_ms: f[10].parse().ok()?,
                busy_ms: f[12].parse().ok()?,
            })
        })
    }

    fn megabytes(&self) -> f64 {
        self.sectors_written as f64 * 512.0 / 1e6
    }

    fn ms_per_write(&self) -> f64 {
        if self.writes == 0 {
            0.0
        } else {
            self.write_ms as f64 / self.writes as f64
        }
    }
}

impl std::ops::Sub for DiskStats {
    type Output = DiskStats;
    fn sub(self, earlier: DiskStats) -> DiskStats {
        DiskStats {
            device: self.device,
            writes: self.writes.saturating_sub(earlier.writes),
            sectors_written: self.sectors_written.saturating_sub(earlier.sectors_written),
            write_ms: self.write_ms.saturating_sub(earlier.write_ms),
            busy_ms: self.busy_ms.saturating_sub(earlier.busy_ms),
        }
    }
}
