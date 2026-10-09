//! TATP (Telecom Application Transaction Processing) benchmark.
//!
//! `populate` builds a fresh database of a given number of subscribers;
//! `run` points a fixed number of connections at it, each running TATP
//! transactions back to back, closed loop, and reports the Mean Qualified
//! Throughput (MQTh): transactions completed per second over a fixed
//! measured window. It also reports the response time of every
//! transaction kind, the CPU the process spent and what the disk did.

mod sqlite_engine;
mod turso_engine;
mod workload;

use clap::{Args, Parser, Subcommand, ValueEnum};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, OnceLock,
    },
    thread,
    time::{Duration, Instant},
};
use workload::{Kind, Mix};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Engine {
    Sqlite,
    Turso,
}

impl Engine {
    fn label(self) -> &'static str {
        match self {
            Engine::Sqlite => "sqlite",
            Engine::Turso => "turso",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TxnMode {
    /// WAL journal, one writer at a time: writes start with `BEGIN IMMEDIATE`.
    Immediate,
    /// MVCC journal, every transaction starts with `BEGIN CONCURRENT`.
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
enum MixArg {
    Standard,
    Read,
}

#[derive(Parser)]
#[command(name = "tatp")]
#[command(about = "TATP benchmark for SQLite and Turso")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a database and fill it with the initial TATP rows.
    Populate(PopulateArgs),
    /// Run the transaction mix against a populated database.
    Run(RunArgs),
}

#[derive(Args)]
struct EngineArgs {
    #[arg(short = 'e', long = "engine")]
    engine: Engine,

    #[arg(long = "db", help = "Database file")]
    db: PathBuf,

    #[arg(
        short = 'm',
        long = "mode",
        help = "Defaults to immediate for SQLite and concurrent for Turso"
    )]
    mode: Option<TxnMode>,

    #[arg(
        long = "io",
        default_value = DEFAULT_IO,
        help = "Turso IO backend: io_uring (default on Linux) or syscall. SQLite ignores this"
    )]
    io: String,

    #[arg(
        long = "timeout",
        default_value = "60000",
        help = "Busy timeout in milliseconds"
    )]
    timeout: u64,
}

impl EngineArgs {
    fn mode(&self) -> TxnMode {
        let mode = self.mode.unwrap_or(match self.engine {
            Engine::Sqlite => TxnMode::Immediate,
            Engine::Turso => TxnMode::Concurrent,
        });
        assert!(
            self.engine != Engine::Sqlite || mode == TxnMode::Immediate,
            "SQLite only supports --mode immediate"
        );
        mode
    }
}

#[derive(Args)]
struct PopulateArgs {
    #[command(flatten)]
    engine: EngineArgs,

    #[arg(
        short = 's',
        long = "subscribers",
        default_value = "100000",
        help = "Rows in the subscriber table; the other tables are sized from it"
    )]
    subscribers: u64,

    #[arg(
        long = "seed",
        default_value = "1",
        help = "Seed of the generated rows, so both engines get the same data"
    )]
    seed: u64,
}

#[derive(Args)]
struct RunArgs {
    #[command(flatten)]
    engine: EngineArgs,

    #[arg(
        short = 's',
        long = "subscribers",
        help = "Rows the database's subscriber table holds. It names the output files, \
                and the run stops if the database holds another number"
    )]
    subscribers: u64,

    #[arg(
        long = "cache-size-mb",
        default_value = "1024",
        help = "Page cache of every connection in MiB, for both engines. The default \
                matches the 1 GB the SQLite VLDB paper gives its engines"
    )]
    cache_size_mb: u64,

    #[arg(
        short = 'c',
        long = "connections",
        default_value = "1",
        help = "Connections running transactions at once, each on its own thread, each \
                starting its next transaction as soon as the previous one ends"
    )]
    connections: usize,

    #[arg(long = "mix", default_value = "standard", help = "Transaction mix")]
    mix: MixArg,

    #[arg(
        long = "uniform",
        help = "Draw subscriber ids uniformly instead of with the specification's \
                non-uniform distribution"
    )]
    uniform: bool,

    #[arg(
        short = 'd',
        long = "duration",
        default_value = "60",
        help = "Measurement time in seconds"
    )]
    duration: u64,

    #[arg(
        long = "warmup",
        default_value = "10",
        help = "Warmup time in seconds; its transactions are recorded but flagged"
    )]
    warmup: u64,

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
        long = "no-group-commit",
        help = "Turn off Turso MVCC group commit. SQLite ignores this"
    )]
    no_group_commit: bool,

    #[arg(
        long = "seed",
        default_value = "1",
        help = "Seed of the transaction stream; connection i draws from seed + i"
    )]
    seed: u64,

    #[arg(
        long = "run",
        default_value = "1",
        help = "Number of this run among repeats of the same configuration; it names the \
                output files"
    )]
    run: usize,

    #[arg(
        long = "out-dir",
        default_value = ".",
        help = "Directory for the results. Every run writes its own files there and \
                refuses to overwrite ones that exist"
    )]
    out_dir: PathBuf,
}

#[cfg(target_os = "linux")]
const DEFAULT_IO: &str = "io_uring";
#[cfg(not(target_os = "linux"))]
const DEFAULT_IO: &str = "syscall";

pub struct PopulateConfig {
    pub db_path: String,
    pub subscribers: u64,
    pub seed: u64,
    pub mode: TxnMode,
    pub io: String,
    pub timeout: Duration,
}

/// What populating wrote, counted back through a fresh connection.
pub struct TableCounts {
    pub subscriber: u64,
    pub access_info: u64,
    pub special_facility: u64,
    pub call_forwarding: u64,
}

pub struct Config {
    pub db_path: String,
    pub subscribers: u64,
    pub cache_size_mb: u64,
    pub run: usize,
    pub connections: usize,
    pub mix: Mix,
    pub uniform: bool,
    pub seed: u64,
    pub warmup: Duration,
    pub duration: Duration,
    pub timeout: Duration,
    pub mode: TxnMode,
    pub io: String,
    pub checkpointer: Option<Duration>,
    pub group_commit: bool,
}

impl Config {
    fn stop_at(&self) -> Duration {
        self.warmup + self.duration
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Completed and found the rows it looked for.
    Found,
    /// Completed, but the rows were not there. The specification counts
    /// it in the throughput all the same.
    NotFound,
    /// Rolled back on an error the specification allows: a constraint
    /// violation in INSERT_CALL_FORWARDING. Not counted in the throughput.
    Rejected,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Outcome::Found => "found",
            Outcome::NotFound => "not_found",
            Outcome::Rejected => "rejected",
        }
    }
}

/// One transaction, as its connection saw it.
pub struct Sample {
    /// When it started, from the start of the run.
    pub started_ns: u64,
    /// True during warmup. Kept so the start of a run can be looked at; left
    /// out of every summary and plot.
    pub warmup: bool,
    pub kind: Kind,
    pub outcome: Outcome,
    /// How many times it had to start over on a conflict with another
    /// transaction.
    pub restarts: u32,
    /// `BEGIN` to the end of `COMMIT` or `ROLLBACK`, restarts included.
    pub total_ns: u64,
}

pub struct Checkpoint {
    /// When it started, from the start of the run.
    pub at: Duration,
    pub took: Duration,
}

pub struct Run {
    pub subscribers: u64,
    pub per_connection: Vec<Vec<Sample>>,
    pub checkpoints: Vec<Checkpoint>,
    /// Wall time from the first transaction to the last one ending.
    pub elapsed: Duration,
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

/// Transactions completed so far, over every connection. The sampler reads
/// it once a second.
pub static COMPLETED: AtomicU64 = AtomicU64::new(0);

fn main() {
    match Cli::parse().command {
        Command::Populate(args) => populate(args),
        Command::Run(args) => run(args),
    }
}

fn populate(args: PopulateArgs) {
    let mode = args.engine.mode();
    let tag = format!("{}/{}", args.engine.engine.label(), mode.label());
    if args.engine.db.exists() {
        eprintln!(
            "[{tag}] {} already exists; populating needs a new file",
            args.engine.db.display()
        );
        std::process::exit(1);
    }
    if let Some(dir) = args.engine.db.parent() {
        std::fs::create_dir_all(dir).expect("cannot create the database directory");
    }
    assert!(args.subscribers > 0, "--subscribers must be at least 1");
    let config = PopulateConfig {
        db_path: args.engine.db.to_string_lossy().into_owned(),
        subscribers: args.subscribers,
        seed: args.seed,
        mode,
        io: args.engine.io,
        timeout: Duration::from_millis(args.engine.timeout),
    };
    let started = Instant::now();
    let counts = match args.engine.engine {
        Engine::Sqlite => sqlite_engine::populate(&config),
        Engine::Turso => turso_engine::populate(&config),
    };
    let took = started.elapsed().as_secs_f64();
    assert_eq!(
        counts.subscriber, config.subscribers,
        "the subscriber table does not hold every subscriber"
    );
    let n = counts.subscriber as f64;
    eprintln!(
        "[{tag}] populated {} in {took:.1}s: {} subscribers, {} access_info ({:.2} each), \
         {} special_facility ({:.2} each), {} call_forwarding ({:.2} each)",
        args.engine.db.display(),
        counts.subscriber,
        counts.access_info,
        counts.access_info as f64 / n,
        counts.special_facility,
        counts.special_facility as f64 / n,
        counts.call_forwarding,
        counts.call_forwarding as f64 / n,
    );
}

fn run(args: RunArgs) {
    let mode = args.engine.mode();
    assert!(args.connections > 0, "--connections must be at least 1");
    let engine_label = args.engine.engine.label();
    let tag = format!("{engine_label}/{}", mode.label());
    if !args.engine.db.exists() {
        eprintln!(
            "[{tag}] {} does not exist; run `tatp populate` first",
            args.engine.db.display()
        );
        std::process::exit(1);
    }

    std::fs::create_dir_all(&args.out_dir).expect("cannot create the output directory");
    let run_name = format!(
        "{engine_label}-s{}-c{}-r{}",
        args.subscribers, args.connections, args.run
    );
    let samples_path = args.out_dir.join(format!("{run_name}.csv"));
    let checkpoints_path = args.out_dir.join(format!("{run_name}-checkpoints.csv"));
    let timeline_path = args.out_dir.join(format!("{run_name}-timeline.csv"));
    let kinds_path = args.out_dir.join(format!("{run_name}-transactions.csv"));
    let result_path = args.out_dir.join(format!("{run_name}-result.csv"));
    for path in [
        &samples_path,
        &checkpoints_path,
        &timeline_path,
        &kinds_path,
        &result_path,
    ] {
        if path.exists() {
            eprintln!(
                "[{tag}] {} already exists; remove it or pass another --out-dir",
                path.display()
            );
            std::process::exit(1);
        }
    }

    let config = Config {
        db_path: args.engine.db.to_string_lossy().into_owned(),
        subscribers: args.subscribers,
        cache_size_mb: args.cache_size_mb,
        run: args.run,
        connections: args.connections,
        mix: match args.mix {
            MixArg::Standard => Mix::Standard,
            MixArg::Read => Mix::Read,
        },
        uniform: args.uniform,
        seed: args.seed,
        warmup: Duration::from_secs(args.warmup),
        duration: Duration::from_secs(args.duration),
        timeout: Duration::from_millis(args.engine.timeout),
        mode,
        io: args.engine.io,
        checkpointer: (args.checkpointer > 0).then(|| Duration::from_millis(args.checkpointer)),
        group_commit: !args.no_group_commit,
    };

    let db_dir = args
        .engine
        .db
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let cpu_before = cpu_time();
    let disk_before = DiskStats::for_path(&db_dir);
    let stop_sampling = Arc::new(AtomicBool::new(false));
    let sampler = {
        let stop = Arc::clone(&stop_sampling);
        let warmup = config.warmup;
        thread::spawn(move || sample(stop, warmup))
    };
    let run = match args.engine.engine {
        Engine::Sqlite => sqlite_engine::run(&config),
        Engine::Turso => turso_engine::run(&config),
    };
    stop_sampling.store(true, Ordering::Relaxed);
    let ticks = sampler.join().expect("sampler thread panicked");
    let cpu = cpu_time() - cpu_before;
    let disk = DiskStats::for_path(&db_dir)
        .zip(disk_before)
        .map(|(a, b)| a - b);

    let result = Summary::from(&config, &run, cpu, disk.as_ref());
    result.report(&tag, &run);
    write_samples(&samples_path, engine_label, &config, &run);
    write_checkpoints(&checkpoints_path, engine_label, &config, &run);
    write_timeline(&timeline_path, engine_label, &config, &ticks);
    result.write_kinds(&kinds_path, engine_label, &config);
    result.write(&result_path, engine_label, &config);
    eprintln!("[{tag}] wrote {}", result_path.display());
}

/// One second of the run, as the sampler saw it.
struct Tick {
    second: u64,
    warmup: bool,
    transactions: u64,
    cpu: CpuTime,
}

/// Samples the completed transaction count and the process's CPU time once
/// a second while the run goes, so a throughput that sagged halfway through
/// shows up as such.
fn sample(stop: Arc<AtomicBool>, warmup: Duration) -> Vec<Tick> {
    let mut ticks = Vec::new();
    let started = Instant::now();
    let (mut last_transactions, mut last_cpu) = (COMPLETED.load(Ordering::Relaxed), cpu_time());
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
        let (transactions, cpu) = (COMPLETED.load(Ordering::Relaxed), cpu_time());
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

/// The numbers of one transaction kind over the measured window.
struct KindSummary {
    kind: Kind,
    found: u64,
    not_found: u64,
    rejected: u64,
    restarts: u64,
    p50_ms: f64,
    p90_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

impl KindSummary {
    fn completed(&self) -> u64 {
        self.found + self.not_found
    }
}

/// The numbers one run boils down to.
struct Summary {
    subscribers: u64,
    /// Transactions that completed, found or not, in the measured window.
    completed: u64,
    rejected: u64,
    seconds: f64,
    mqth: f64,
    cpu: CpuTime,
    cpu_us_per_transaction: f64,
    restarts: u64,
    p50_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    kinds: Vec<KindSummary>,
    disk: Option<DiskStats>,
    checkpoints: usize,
    checkpoint_p50_ms: f64,
    checkpoint_max_ms: f64,
}

impl Summary {
    fn from(config: &Config, run: &Run, cpu: CpuTime, disk: Option<&DiskStats>) -> Self {
        // Only transactions that started inside the measured window count,
        // over exactly that window: the same wall time for every engine.
        let measured: Vec<&Sample> = run
            .per_connection
            .iter()
            .flatten()
            .filter(|s| !s.warmup)
            .collect();
        let completed_samples: Vec<&Sample> = measured
            .iter()
            .copied()
            .filter(|s| s.outcome != Outcome::Rejected)
            .collect();
        let completed = completed_samples.len() as u64;
        let rejected = measured.len() as u64 - completed;
        let seconds = config.duration.as_secs_f64();
        let mut all_ms = response_times_ms(completed_samples.iter().copied());
        all_ms.sort_by(f64::total_cmp);
        let kinds = Kind::ALL
            .iter()
            .map(|&kind| {
                let of_kind: Vec<&Sample> = measured
                    .iter()
                    .copied()
                    .filter(|s| s.kind == kind)
                    .collect();
                let count =
                    |outcome| of_kind.iter().filter(|s| s.outcome == outcome).count() as u64;
                let mut ms = response_times_ms(
                    of_kind
                        .iter()
                        .copied()
                        .filter(|s| s.outcome != Outcome::Rejected),
                );
                ms.sort_by(f64::total_cmp);
                KindSummary {
                    kind,
                    found: count(Outcome::Found),
                    not_found: count(Outcome::NotFound),
                    rejected: count(Outcome::Rejected),
                    restarts: of_kind.iter().map(|s| s.restarts as u64).sum(),
                    p50_ms: quantile(&ms, 0.5),
                    p90_ms: quantile(&ms, 0.9),
                    p99_ms: quantile(&ms, 0.99),
                    max_ms: quantile(&ms, 1.0),
                }
            })
            .collect();
        let mut checkpoint_ms: Vec<f64> = run
            .checkpoints
            .iter()
            .map(|c| c.took.as_secs_f64() * 1e3)
            .collect();
        checkpoint_ms.sort_by(f64::total_cmp);
        let busy = (cpu.user + cpu.system).as_secs_f64();
        Self {
            subscribers: run.subscribers,
            completed,
            rejected,
            seconds,
            mqth: completed as f64 / seconds,
            cpu,
            // The whole process over the whole run, warmup and checkpointer
            // included, per measured transaction.
            cpu_us_per_transaction: if completed > 0 {
                busy * 1e6 / completed as f64
            } else {
                0.0
            },
            restarts: measured.iter().map(|s| s.restarts as u64).sum(),
            p50_ms: quantile(&all_ms, 0.5),
            p99_ms: quantile(&all_ms, 0.99),
            max_ms: quantile(&all_ms, 1.0),
            kinds,
            disk: disk.cloned(),
            checkpoints: run.checkpoints.len(),
            checkpoint_p50_ms: quantile(&checkpoint_ms, 0.5),
            checkpoint_max_ms: quantile(&checkpoint_ms, 1.0),
        }
    }

    fn report(&self, tag: &str, run: &Run) {
        eprintln!(
            "[{tag}] {} subscribers, {} transactions completed in {:.1}s: MQTh {:.0} \
             transactions/s, {} rejected, {} restarts",
            self.subscribers, self.completed, self.seconds, self.mqth, self.rejected, self.restarts
        );
        eprintln!(
            "[{tag}] response time p50 {:.3}ms  p99 {:.3}ms  max {:.3}ms",
            self.p50_ms, self.p99_ms, self.max_ms
        );
        for k in &self.kinds {
            if k.completed() + k.rejected == 0 {
                continue;
            }
            let found_percent = k.found as f64 / (k.completed() + k.rejected).max(1) as f64 * 100.0;
            eprintln!(
                "[{tag}]   {:<23} {:>9} done  {:>5.1}% found  {:>6} rejected  {:>6} restarts  \
                 p50 {:.3}ms  p90 {:.3}ms  p99 {:.3}ms  max {:.3}ms",
                k.kind.name(),
                k.completed(),
                found_percent,
                k.rejected,
                k.restarts,
                k.p50_ms,
                k.p90_ms,
                k.p99_ms,
                k.max_ms
            );
        }
        let hardware_threads = hardware_threads();
        let wall = run.elapsed.as_secs_f64().max(f64::EPSILON);
        let busy = (self.cpu.user + self.cpu.system).as_secs_f64();
        eprintln!(
            "[{tag}] cpu: user {:.1}s  sys {:.1}s  {:.0}% of one core over {wall:.1}s, \
             {:.1}% of {hardware_threads} hardware threads, {:.1}us per transaction",
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

    fn write(&self, path: &Path, engine_label: &str, config: &Config) {
        use std::io::Write;
        let mut out = std::fs::File::create(path).expect("cannot create the result file");
        writeln!(
            out,
            "engine,mode,mix,distribution,subscribers,cache_size_mb,connections,run,transactions,\
             rejected,\
             seconds,mqth,cpu_user_s,cpu_sys_s,cpu_us_per_transaction,hardware_threads,restarts,\
             p50_ms,p99_ms,max_ms,disk_writes,disk_mb,disk_ms_per_write,\
             checkpoints,checkpoint_p50_ms,checkpoint_max_ms"
        )
        .unwrap();
        let (disk_writes, disk_mb, disk_ms_per_write) = match &self.disk {
            Some(d) => (d.writes, d.megabytes(), d.ms_per_write()),
            None => (0, 0.0, 0.0),
        };
        writeln!(
            out,
            "{engine_label},{},{},{},{},{},{},{},{},{},{:.3},{:.2},{:.3},{:.3},{:.2},{},{},\
             {:.4},{:.4},{:.4},{disk_writes},{disk_mb:.1},{disk_ms_per_write:.3},\
             {},{:.2},{:.2}",
            config.mode.label(),
            config.mix.label(),
            distribution_label(config),
            self.subscribers,
            config.cache_size_mb,
            config.connections,
            config.run,
            self.completed,
            self.rejected,
            self.seconds,
            self.mqth,
            self.cpu.user.as_secs_f64(),
            self.cpu.system.as_secs_f64(),
            self.cpu_us_per_transaction,
            hardware_threads(),
            self.restarts,
            self.p50_ms,
            self.p99_ms,
            self.max_ms,
            self.checkpoints,
            self.checkpoint_p50_ms,
            self.checkpoint_max_ms
        )
        .unwrap();
    }

    fn write_kinds(&self, path: &Path, engine_label: &str, config: &Config) {
        use std::io::Write;
        let mut out = std::fs::File::create(path).expect("cannot create the transactions file");
        writeln!(
            out,
            "engine,mode,mix,subscribers,connections,run,transaction,completed,found,not_found,\
             rejected,restarts,per_s,p50_ms,p90_ms,p99_ms,max_ms"
        )
        .unwrap();
        for k in &self.kinds {
            writeln!(
                out,
                "{engine_label},{},{},{},{},{},{},{},{},{},{},{},{:.2},{:.4},{:.4},{:.4},{:.4}",
                config.mode.label(),
                config.mix.label(),
                self.subscribers,
                config.connections,
                config.run,
                k.kind.name(),
                k.completed(),
                k.found,
                k.not_found,
                k.rejected,
                k.restarts,
                k.completed() as f64 / self.seconds,
                k.p50_ms,
                k.p90_ms,
                k.p99_ms,
                k.max_ms
            )
            .unwrap();
        }
    }
}

fn response_times_ms<'a>(samples: impl Iterator<Item = &'a Sample>) -> Vec<f64> {
    samples.map(|s| s.total_ns as f64 / 1e6).collect()
}

fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() as f64 - 1.0) * q).round() as usize]
}

fn distribution_label(config: &Config) -> &'static str {
    if config.uniform {
        "uniform"
    } else {
        "nurand"
    }
}

fn hardware_threads() -> usize {
    thread::available_parallelism().map_or(1, |n| n.get())
}

fn write_samples(path: &Path, engine_label: &str, config: &Config, run: &Run) {
    use std::io::Write;
    let file = std::fs::File::create(path).expect("cannot create the samples file");
    let mut out = std::io::BufWriter::new(file);
    writeln!(
        out,
        "engine,mode,connections,run,connection,started_ns,warmup,transaction,outcome,restarts,total_ns"
    )
    .unwrap();
    for (connection, samples) in run.per_connection.iter().enumerate() {
        for s in samples {
            writeln!(
                out,
                "{engine_label},{},{},{},{connection},{},{},{},{},{},{}",
                config.mode.label(),
                config.connections,
                config.run,
                s.started_ns,
                s.warmup as u8,
                s.kind.name(),
                s.outcome.label(),
                s.restarts,
                s.total_ns
            )
            .unwrap();
        }
    }
    out.flush().unwrap();
}

fn write_checkpoints(path: &Path, engine_label: &str, config: &Config, run: &Run) {
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

fn write_timeline(path: &Path, engine_label: &str, config: &Config, ticks: &[Tick]) {
    use std::io::Write;
    let file = std::fs::File::create(path).expect("cannot create the timeline file");
    let mut out = std::io::BufWriter::new(file);
    let hardware_threads = hardware_threads();
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

/// CPU time this process has used so far, user and system. Zero where
/// getrusage is not available.
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

#[cfg(not(unix))]
fn cpu_time() -> CpuTime {
    CpuTime {
        user: Duration::ZERO,
        system: Duration::ZERO,
    }
}

#[cfg(unix)]
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
    #[cfg(not(target_os = "linux"))]
    fn for_path(_path: &Path) -> Option<DiskStats> {
        None
    }

    #[cfg(target_os = "linux")]
    fn for_path(path: &Path) -> Option<DiskStats> {
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
