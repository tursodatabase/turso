use clap::{Parser, ValueEnum};
use std::time::{Duration, Instant};

mod sqlite_engine;
mod turso_engine;

pub const DB_PATH: &str = "write_throughput_test.db";

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Engine {
    Turso,
    Sqlite,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TransactionMode {
    Legacy,
    Mvcc,
    Concurrent,
    LogicalLog,
    /// MVCC with `BEGIN CONCURRENT`; auto-checkpoint uses blocking TRUNCATE (flag off).
    MvccTruncate,
    /// MVCC with `BEGIN CONCURRENT`; auto-checkpoint uses experimental passive/off-lock path.
    MvccPassive,
}

impl TransactionMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Mvcc => "mvcc",
            Self::Concurrent => "concurrent",
            Self::LogicalLog => "logical-log",
            Self::MvccTruncate => "mvcc-truncate",
            Self::MvccPassive => "mvcc-passive",
        }
    }

    pub fn needs_mvcc(self) -> bool {
        !matches!(self, Self::Legacy)
    }

    pub fn begin_statement(self) -> &'static str {
        match self {
            Self::Legacy | Self::Mvcc => "BEGIN",
            Self::Concurrent | Self::LogicalLog | Self::MvccTruncate | Self::MvccPassive => {
                "BEGIN CONCURRENT"
            }
        }
    }

    /// Whether transactions are started concurrently and joined at the end,
    /// rather than run one after another.
    pub fn overlaps_transactions(self) -> bool {
        matches!(
            self,
            Self::Concurrent | Self::MvccTruncate | Self::MvccPassive
        )
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum IoOption {
    IoUring,
}

#[derive(Parser)]
#[command(name = "write-throughput")]
#[command(about = "Write throughput benchmark for Turso and SQLite")]
pub struct Args {
    #[arg(short = 'e', long = "engine", default_value = "turso")]
    pub engine: Engine,

    #[arg(short = 't', long = "threads", default_value = "1")]
    pub threads: usize,

    #[arg(short = 'b', long = "batch-size", default_value = "100")]
    pub batch_size: usize,

    #[arg(short = 'i', long = "iterations", default_value = "10")]
    pub iterations: usize,

    #[arg(short = 'm', long = "mode", default_value = "legacy", help = "Turso transaction mode")]
    pub mode: TransactionMode,

    #[arg(
        long = "compute",
        default_value = "0",
        help = "Per transaction compute time (us)"
    )]
    pub compute: u64,

    #[arg(
        long = "timeout",
        default_value = "30000",
        help = "Busy timeout in milliseconds"
    )]
    pub timeout: u64,

    #[arg(long = "io", help = "IO backend (Turso only)")]
    pub io: Option<IoOption>,
}

impl Args {
    pub fn busy_timeout(&self) -> Duration {
        Duration::from_millis(self.timeout)
    }
}

pub struct Measurement {
    pub rows: u64,
    pub elapsed: Duration,
}

fn main() -> anyhow::Result<()> {
    #[cfg(feature = "console")]
    let console_layer = console_subscriber::spawn();
    {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        use tracing_subscriber::{EnvFilter, Layer};

        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_thread_ids(true)
            .with_filter(EnvFilter::from_default_env());
        let registry = tracing_subscriber::registry();
        #[cfg(feature = "console")]
        let registry = registry.with(console_layer);
        registry.with(fmt_layer).init();
    }

    let args = Args::parse();
    remove_db_tree(DB_PATH);

    let (system, mode) = match args.engine {
        Engine::Turso => ("Turso", args.mode.label()),
        Engine::Sqlite => ("SQLite", "wal"),
    };

    let measurement = match args.engine {
        Engine::Turso => turso_engine::run(&args)?,
        Engine::Sqlite => sqlite_engine::run(&args)?,
    };

    let throughput = measurement.rows as f64 / measurement.elapsed.as_secs_f64();
    println!(
        "{system},{mode},{},{},{},{throughput:.2}",
        args.threads, args.batch_size, args.compute
    );

    Ok(())
}

fn remove_db_tree(db_path: &str) {
    if let Ok(entries) = std::fs::read_dir(".") {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name == db_path || name.starts_with(&format!("{db_path}-")) {
                let _ = std::fs::remove_file(name);
            }
        }
    }
}

/// Row id that is unique across every thread, iteration and batch position.
pub fn row_id(
    thread_id: usize,
    iterations: usize,
    batch_size: usize,
    iteration: usize,
    index: usize,
) -> usize {
    thread_id * iterations * batch_size + iteration * batch_size + index
}

// Busy loop to simulate CPU or GPU bound computation (for example, parsing,
// data aggregation or ML inference).
pub fn perform_compute(thread_id: usize, usec: u64) -> u64 {
    if usec == 0 {
        return 0;
    }
    let start = Instant::now();
    let mut sum: u64 = 0;
    while start.elapsed().as_micros() < usec as u128 {
        sum = sum.wrapping_add(thread_id as u64);
    }
    sum
}
