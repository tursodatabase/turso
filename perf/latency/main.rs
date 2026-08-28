//! Write transaction latency benchmark.
//!
//! Writers hammer one database with small write transactions and every
//! transaction's latency is written out as a raw sample, so the whole
//! distribution can be plotted instead of a handful of quantiles.
//!
//! Transactions arrive on a fixed schedule that does not bend when the database
//! is slow. A transaction's latency is measured from the time it was *supposed*
//! to start, so time spent waiting for a writer ahead of it lands in the
//! sample. Without that, a blocking database looks fast: it simply stops being
//! asked for work while it is busy, and only the transactions it was ready for
//! get timed. That is coordinated omission, and it hides exactly the stalls
//! this benchmark exists to measure.

mod sqlite_engine;
mod turso_engine;

use clap::{Parser, ValueEnum};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        OnceLock,
    },
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
pub enum Arrivals {
    /// Exponential gaps between arrivals, a Poisson process at the rate. No
    /// period of its own, so it cannot line up with anything periodic in the
    /// engine, such as SQLite's millisecond busy-handler sleeps.
    Poisson,
    /// Arrivals exactly 1/rate apart.
    Fixed,
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
#[command(name = "turso-txn-latency")]
#[command(about = "Write transaction latency benchmark for SQLite and Turso")]
struct Args {
    #[arg(short = 'e', long = "engine")]
    engine: Engine,

    #[arg(
        short = 'c',
        long = "connections",
        default_value = "1",
        help = "Connections serving the arriving transactions. One connection isolates \
                what the engine does to a writer's latency; more adds lock contention"
    )]
    connections: usize,

    #[arg(
        short = 'r',
        long = "rate",
        default_value = "200",
        help = "Transactions offered per second, held fixed no matter how slow the database is"
    )]
    rate: f64,

    #[arg(
        long = "arrivals",
        default_value = "poisson",
        help = "How arrivals are spaced: poisson draws exponential gaps around 1/rate, \
                fixed puts them exactly 1/rate apart"
    )]
    arrivals: Arrivals,

    #[arg(
        long = "seed",
        default_value = "1",
        help = "Seed for the poisson arrival schedule. The same seed gives both engines \
                and every repeat the same schedule"
    )]
    seed: u64,

    #[arg(
        short = 'b',
        long = "batch-size",
        default_value = "10",
        help = "Rows inserted per transaction"
    )]
    batch_size: usize,

    #[arg(
        short = 'd',
        long = "duration",
        default_value = "20",
        help = "Measurement time in seconds"
    )]
    duration: u64,

    #[arg(
        long = "warmup",
        default_value = "3",
        help = "Warmup time in seconds, not recorded"
    )]
    warmup: u64,

    #[arg(
        long = "timeout",
        default_value = "60000",
        help = "Busy timeout in milliseconds"
    )]
    timeout: u64,

    #[arg(
        long = "max-overrun",
        default_value = "3",
        help = "Give up once the run takes this many times longer than planned"
    )]
    max_overrun: f64,

    #[arg(
        short = 'm',
        long = "mode",
        help = "Defaults to immediate for SQLite and concurrent for Turso"
    )]
    mode: Option<TxnMode>,

    #[arg(
        long = "db-dir",
        default_value = ".",
        help = "Directory for the database files. Every run gets its own file there, \
                named after the engine, connection count and repeat, and a run refuses \
                to start on a file that already exists. Nothing is ever deleted"
    )]
    db_dir: std::path::PathBuf,

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
                writer's commit path, for both engines. 0 lets each writer auto-checkpoint \
                itself, which is what the engines do out of the box"
    )]
    checkpointer: u64,

    #[arg(
        long = "run",
        default_value = "1",
        help = "Number of this run among repeats of the same configuration; it names the \
                database and samples files"
    )]
    run: usize,

    #[arg(
        long = "out-dir",
        default_value = ".",
        help = "Directory for the samples. Every run writes one CSV there, named like its \
                database file, and refuses to overwrite one that exists"
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

/// One transaction's latency, split into the phases a writer goes through.
pub struct Sample {
    /// When the transaction was due, from the start of the run.
    pub scheduled_ns: u64,
    /// True during warmup. Kept so the start of a run can be looked at; left
    /// out of every summary and plot.
    pub warmup: bool,
    /// How many times the transaction had to start over on a stale snapshot.
    pub restarts: u32,
    /// From the scheduled arrival to the moment a connection picked it up. This
    /// is the backlog: transactions queued behind a database that is busy.
    pub queue_ns: u64,
    /// How long `BEGIN` took. For a blocking writer this is the wait for the
    /// write lock.
    pub begin_ns: u64,
    pub work_ns: u64,
    pub commit_ns: u64,
    /// Scheduled arrival to successful `COMMIT`, which is what a caller waits.
    pub total_ns: u64,
}

pub struct Config {
    pub db_path: String,
    pub connections: usize,
    pub batch_size: usize,
    pub warmup: Duration,
    pub duration: Duration,
    pub timeout: Duration,
    pub mode: TxnMode,
    /// Turso IO backend name, as accepted by `turso::Builder::with_io`.
    pub io: String,
    pub checkpoint_mode: CheckpointMode,
    pub mvcc_checkpoint_threshold: Option<i64>,
    /// Interval of the separate checkpointer connection, or `None` to let
    /// the writer auto-checkpoint on its commit path.
    pub checkpointer: Option<Duration>,
    pub rate: f64,
    pub arrivals: Arrivals,
    pub seed: u64,
    pub max_overrun: f64,
}

impl Config {
    fn stop_at(&self) -> Duration {
        self.warmup + self.duration
    }
}

/// What one engine's run produced.
pub struct Run {
    pub per_thread: Vec<Vec<Sample>>,
    /// True if the run was cut short because the database fell too far behind.
    pub overran: bool,
    /// Wall time from the first scheduled arrival to the last commit.
    pub elapsed: Duration,
    /// Every run of the separate checkpointer. Empty without `--checkpointer`.
    pub checkpoints: Vec<Checkpoint>,
}

pub struct Checkpoint {
    /// When it started, from the start of the run.
    pub at: Duration,
    pub took: Duration,
}

/// Hands out transaction slots from a schedule shared by every connection.
///
/// The schedule is fixed before the run starts: slot `k` is due at
/// `start + due[k]` whether or not anyone is free to run it, and a connection
/// that takes its slot late carries that lateness into the sample.
pub struct Pacer {
    start: OnceLock<Instant>,
    due: Vec<Duration>,
    warmup: Duration,
    wall_deadline: Duration,
    next_slot: AtomicU64,
    overran: AtomicBool,
}

pub struct Arrival {
    /// When this transaction was supposed to start.
    pub scheduled: Instant,
    /// The same, from the start of the run.
    pub offset: Duration,
    /// True during warmup.
    pub warmup: bool,
}

impl Pacer {
    pub fn new(config: &Config) -> Self {
        Self {
            start: OnceLock::new(),
            due: schedule(config),
            warmup: config.warmup,
            wall_deadline: config.stop_at().mul_f64(config.max_overrun.max(1.0)),
            next_slot: AtomicU64::new(0),
            overran: AtomicBool::new(false),
        }
    }

    /// The clock starts when the first connection asks for work, which is
    /// after every connection is open and prepared, so setup does not eat into
    /// the first slots.
    pub fn started_at(&self) -> Instant {
        *self.start.get_or_init(Instant::now)
    }

    pub fn elapsed(&self) -> Duration {
        self.start.get().map(Instant::elapsed).unwrap_or_default()
    }

    /// The next transaction to run, or `None` once the run is over.
    pub fn next(&self) -> Option<Arrival> {
        let start = self.started_at();
        if start.elapsed() >= self.wall_deadline {
            self.overran.store(true, Ordering::Relaxed);
            return None;
        }
        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed) as usize;
        let offset = *self.due.get(slot)?;
        Some(Arrival {
            scheduled: start + offset,
            offset,
            warmup: offset < self.warmup,
        })
    }

    pub fn overran(&self) -> bool {
        self.overran.load(Ordering::Relaxed)
    }
}

/// Every arrival's offset from the start of the run, in order, up to the
/// end of the measured time.
fn schedule(config: &Config) -> Vec<Duration> {
    let stop_at = config.stop_at().as_secs_f64();
    let gap = 1.0 / config.rate;
    let mut due = Vec::with_capacity((stop_at * config.rate) as usize + 1);
    let mut rng = SplitMix64(config.seed);
    let mut t = 0.0;
    while t < stop_at {
        due.push(Duration::from_secs_f64(t));
        t += match config.arrivals {
            Arrivals::Fixed => gap,
            Arrivals::Poisson => -rng.unit().ln() * gap,
        };
    }
    due
}

/// A small seeded generator, so the schedule is the same for every engine and
/// repeat and nothing has to be pulled in for it.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in (0, 1], so its logarithm is always finite.
    fn unit(&mut self) -> f64 {
        ((self.next() >> 11) as f64 + 1.0) / ((1u64 << 53) as f64)
    }
}

/// Waits until `deadline`, spinning over the last stretch. Thread sleeps land
/// within a millisecond or so of where they were aimed, which is wider than a
/// fast commit and would show up as latency the database never caused.
pub fn wait_until(deadline: Instant) {
    const SPIN: Duration = Duration::from_micros(300);
    let now = Instant::now();
    if deadline <= now {
        return;
    }
    let remaining = deadline - now;
    if remaining > SPIN {
        std::thread::sleep(remaining - SPIN);
    }
    spin_until(deadline);
}

pub fn spin_until(deadline: Instant) {
    while Instant::now() < deadline {
        std::hint::spin_loop();
    }
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
    assert!(args.rate > 0.0, "--rate must be positive");

    let mut config = Config {
        db_path: String::new(),
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
        rate: args.rate,
        arrivals: args.arrivals,
        seed: args.seed,
        max_overrun: args.max_overrun,
    };

    let engine_label = match args.engine {
        Engine::Sqlite => "sqlite",
        Engine::Turso => "turso",
    };
    let tag = format!("{engine_label}/{}", config.mode.label());

    std::fs::create_dir_all(&args.db_dir).expect("cannot create the database directory");
    std::fs::create_dir_all(&args.out_dir).expect("cannot create the output directory");
    {
        let repeat = args.run;
        // Every run gets a fresh file of its own. A leftover from an earlier
        // run would give this one a head start of rows, so it is an error,
        // and it is the user's file to remove, not ours.
        let run_name = format!("{engine_label}-c{}-r{repeat}", config.connections);
        let db_path = args.db_dir.join(format!("{run_name}.db"));
        let out_path = args.out_dir.join(format!("{run_name}.csv"));
        for (path, flag) in [(&db_path, "--db-dir"), (&out_path, "--out-dir")] {
            if path.exists() {
                eprintln!(
                    "[{tag}] {} already exists; remove it or pass another {flag}",
                    path.display()
                );
                std::process::exit(1);
            }
        }
        config.db_path = db_path.to_string_lossy().into_owned();
        let cpu_before = cpu_time();
        let disk_before = DiskStats::for_path(&args.db_dir);
        let run = match args.engine {
            Engine::Sqlite => sqlite_engine::run(&config),
            Engine::Turso => turso_engine::run(&config),
        };
        let cpu = cpu_time() - cpu_before;
        let disk = DiskStats::for_path(&args.db_dir)
            .zip(disk_before)
            .map(|(a, b)| a - b);
        let mut totals: Vec<u64> = run
            .per_thread
            .iter()
            .flatten()
            .filter(|s| !s.warmup)
            .map(|s| s.total_ns)
            .collect();
        report(&tag, &config, &mut totals, &run, args.rate, cpu, disk);
        write_samples(&out_path, engine_label, &config, repeat, &run);
        let checkpoints_path = args.out_dir.join(format!("{run_name}-checkpoints.csv"));
        write_checkpoints(&checkpoints_path, engine_label, &config, repeat, &run);
        eprintln!(
            "[{tag}] wrote {} and {}",
            out_path.display(),
            checkpoints_path.display()
        );
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

fn write_samples(
    path: &std::path::Path,
    engine_label: &str,
    config: &Config,
    run_index: usize,
    run: &Run,
) {
    use std::io::Write;
    let file = std::fs::File::create(path).expect("cannot create the samples file");
    let mut out = std::io::BufWriter::new(file);
    writeln!(
        out,
        "engine,mode,connections,run,thread_id,scheduled_ns,warmup,restarts,queue_ns,begin_ns,work_ns,commit_ns,total_ns"
    )
    .unwrap();
    for (thread_id, samples) in run.per_thread.iter().enumerate() {
        for s in samples {
            writeln!(
                out,
                "{engine_label},{},{},{run_index},{thread_id},{},{},{},{},{},{},{},{}",
                config.mode.label(),
                config.connections,
                s.scheduled_ns,
                s.warmup as u8,
                s.restarts,
                s.queue_ns,
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

fn write_checkpoints(
    path: &std::path::Path,
    engine_label: &str,
    config: &Config,
    run_index: usize,
    run: &Run,
) {
    use std::io::Write;
    let file = std::fs::File::create(path).expect("cannot create the checkpoints file");
    let mut out = std::io::BufWriter::new(file);
    writeln!(out, "engine,mode,connections,run,at_ns,took_ns").unwrap();
    for c in &run.checkpoints {
        writeln!(
            out,
            "{engine_label},{},{},{run_index},{},{}",
            config.mode.label(),
            config.connections,
            c.at.as_nanos(),
            c.took.as_nanos()
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

fn report(
    tag: &str,
    config: &Config,
    totals: &mut [u64],
    run: &Run,
    target_rate: f64,
    cpu: CpuTime,
    disk: Option<DiskStats>,
) {
    if totals.is_empty() {
        eprintln!("[{tag}] no transactions recorded");
        return;
    }
    totals.sort_unstable();

    let quantile = |q: f64| {
        let idx = ((totals.len() as f64 - 1.0) * q).round() as usize;
        totals[idx] as f64 / 1e6
    };

    // The measurement window is wall time minus warmup, which runs longer than
    // the requested duration when the database could not keep up.
    let measured = run.elapsed.saturating_sub(config.warmup).as_secs_f64();
    let achieved = totals.len() as f64 / measured.max(f64::EPSILON);

    eprintln!(
        "[{tag}] {} transactions in {measured:.1}s, {achieved:.0}/s achieved",
        totals.len()
    );
    eprintln!(
        "[{tag}] p50 {:.2}ms  p99 {:.2}ms  p99.9 {:.2}ms  max {:.2}ms",
        quantile(0.5),
        quantile(0.99),
        quantile(0.999),
        quantile(1.0)
    );
    // The benchmark's own CPU use, so a run that burnt cores waiting is
    // visible. Includes warmup and, on Linux, the io_uring SQPOLL thread.
    let busy = cpu.user + cpu.system;
    let hardware_threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let wall = run.elapsed.as_secs_f64().max(f64::EPSILON);
    eprintln!(
        "[{tag}] cpu: user {:.1}s  sys {:.1}s  {:.0}% of one core over {wall:.1}s, \
         {:.1}% of {hardware_threads} hardware threads",
        cpu.user.as_secs_f64(),
        cpu.system.as_secs_f64(),
        busy.as_secs_f64() / wall * 100.0,
        busy.as_secs_f64() / wall / hardware_threads as f64 * 100.0
    );
    // What the disk under the database did during the run, from the kernel's
    // counters for the whole device. Other traffic on the device is in here
    // too, which is the point: a slow disk shows up as a slow disk.
    if let Some(disk) = disk {
        let per_write = if disk.writes > 0 {
            disk.write_ms as f64 / disk.writes as f64
        } else {
            0.0
        };
        eprintln!(
            "[{tag}] disk {}: {} writes, {:.1} MB written, {per_write:.2}ms per write, busy {:.0}% of the run",
            disk.device,
            disk.writes,
            disk.sectors_written as f64 * 512.0 / 1e6,
            disk.busy_ms as f64 / wall / 10.0
        );
    }
    if !run.checkpoints.is_empty() {
        let mut ms: Vec<f64> = run
            .checkpoints
            .iter()
            .map(|c| c.took.as_secs_f64() * 1e3)
            .collect();
        ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        eprintln!(
            "[{tag}] checkpointer: {} checkpoints, p50 {:.1}ms  max {:.1}ms  total {:.0}ms",
            ms.len(),
            ms[ms.len() / 2],
            ms[ms.len() - 1],
            ms.iter().sum::<f64>()
        );
    }
    {
        if run.overran {
            eprintln!(
                "[{tag}] WARNING: gave up after running {:.0}x past the planned time. \
                 This database cannot sustain {target_rate:.0} transactions/s, so the \
                 tail here is cut short rather than measured.",
                config.max_overrun
            );
        } else if achieved < target_rate * 0.95 {
            eprintln!(
                "[{tag}] WARNING: only reached {achieved:.0}/s of the {target_rate:.0}/s \
                 offered. The latencies are real, but this database is past saturation."
            );
        }
    }
}
