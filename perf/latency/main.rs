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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

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
#[command(name = "txn-latency")]
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
        long = "closed-loop",
        help = "Offer the next transaction only once the previous one finishes. Measures service time, and undercounts stalls"
    )]
    closed_loop: bool,

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

    #[arg(long = "db", help = "Database path")]
    db_path: Option<String>,

    #[arg(
        long = "checkpoint-mode",
        default_value = "passive",
        help = "Turso MVCC auto-checkpoint mode in concurrent mode. SQLite ignores this"
    )]
    checkpoint_mode: CheckpointMode,

    #[arg(
        long = "io",
        default_value = "syscall",
        help = "Turso IO backend: syscall or io_uring (Linux only). SQLite ignores this"
    )]
    io: String,
}

/// One transaction's latency, split into the phases a writer goes through.
pub struct Sample {
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
    /// Time between arrivals, or `None` in closed-loop mode.
    pub period: Option<Duration>,
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
}

/// Hands out transaction slots on a fixed schedule shared by every connection.
///
/// Slot `k` is due at `start + k * period` whether or not anyone is free to run
/// it. A connection that takes its slot late carries that lateness into the
/// sample.
pub struct Pacer {
    start: OnceLock<Instant>,
    period: Option<Duration>,
    stop_at: Duration,
    warmup: Duration,
    wall_deadline: Duration,
    next_slot: AtomicU64,
    overran: AtomicBool,
}

pub struct Arrival {
    /// When this transaction was supposed to start.
    pub scheduled: Instant,
    /// False during warmup.
    pub record: bool,
}

impl Pacer {
    pub fn new(config: &Config) -> Self {
        Self {
            start: OnceLock::new(),
            period: config.period,
            stop_at: config.stop_at(),
            warmup: config.warmup,
            wall_deadline: config.stop_at().mul_f64(config.max_overrun.max(1.0)),
            next_slot: AtomicU64::new(0),
            overran: AtomicBool::new(false),
        }
    }

    /// The clock starts when the first connection asks for work, which is
    /// after every connection is open and prepared, so setup does not eat into
    /// the first slots.
    fn started_at(&self) -> Instant {
        *self.start.get_or_init(Instant::now)
    }

    pub fn elapsed(&self) -> Duration {
        self.start.get().map(Instant::elapsed).unwrap_or_default()
    }

    /// The next transaction to run, or `None` once the run is over.
    pub fn next(&self) -> Option<Arrival> {
        let start = self.started_at();
        let elapsed = start.elapsed();
        if elapsed >= self.wall_deadline {
            self.overran.store(true, Ordering::Relaxed);
            return None;
        }
        let Some(period) = self.period else {
            if elapsed >= self.stop_at {
                return None;
            }
            return Some(Arrival {
                scheduled: Instant::now(),
                record: elapsed >= self.warmup,
            });
        };
        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
        let offset = period.mul_f64(slot as f64);
        if offset >= self.stop_at {
            return None;
        }
        Some(Arrival {
            scheduled: start + offset,
            record: offset >= self.warmup,
        })
    }

    pub fn overran(&self) -> bool {
        self.overran.load(Ordering::Relaxed)
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
    assert!(
        args.closed_loop || args.rate > 0.0,
        "--rate must be positive, or pass --closed-loop"
    );

    let db_path = args.db_path.unwrap_or_else(|| match args.engine {
        Engine::Sqlite => "txn_latency_sqlite.db".to_string(),
        Engine::Turso => "txn_latency_turso.db".to_string(),
    });

    let config = Config {
        db_path,
        connections: args.connections,
        batch_size: args.batch_size,
        warmup: Duration::from_secs(args.warmup),
        duration: Duration::from_secs(args.duration),
        timeout: Duration::from_millis(args.timeout),
        mode,
        io: args.io,
        checkpoint_mode: args.checkpoint_mode,
        period: if args.closed_loop {
            None
        } else {
            Some(Duration::from_secs_f64(1.0 / args.rate))
        },
        max_overrun: args.max_overrun,
    };

    remove_db_tree(&config.db_path);

    let engine_label = match args.engine {
        Engine::Sqlite => "sqlite",
        Engine::Turso => "turso",
    };

    let run = match args.engine {
        Engine::Sqlite => sqlite_engine::run(&config),
        Engine::Turso => turso_engine::run(&config),
    };

    println!("engine,mode,connections,thread_id,queue_ns,begin_ns,work_ns,commit_ns,total_ns");
    let mut totals = Vec::new();
    for (thread_id, samples) in run.per_thread.iter().enumerate() {
        for s in samples {
            println!(
                "{engine_label},{},{},{thread_id},{},{},{},{},{}",
                config.mode.label(),
                config.connections,
                s.queue_ns,
                s.begin_ns,
                s.work_ns,
                s.commit_ns,
                s.total_ns
            );
            totals.push(s.total_ns);
        }
    }

    report(engine_label, &config, &mut totals, &run, args.rate);
}

fn report(engine_label: &str, config: &Config, totals: &mut [u64], run: &Run, target_rate: f64) {
    let tag = format!("{engine_label}/{}", config.mode.label());
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
    if config.period.is_some() {
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

fn remove_db_tree(db_path: &str) {
    let path = std::path::Path::new(db_path);
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => std::path::Path::new("."),
    };
    let Some(file) = path.file_name().and_then(|f| f.to_str()) else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name == file || name.starts_with(&format!("{file}-")) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}
