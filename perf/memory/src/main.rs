use anyhow::Result;
use clap::{Parser, ValueEnum};
use memory_benchmark::measure::{MemoryReport, MemorySnapshot, file_size, take_snapshot};
use memory_benchmark::profile::Phase;
use memory_benchmark::workload::{
    JournalMode, WorkloadConfig, WorkloadObserver, WorkloadProfile, clean_db_files, run_workload,
};
use std::time::{Duration, Instant};

// Workspace Clippy runs with `--all-features`, which enables `turso`'s
// mimalloc-backed global allocator. Skip the benchmark-only dhat allocator
// under Clippy so the lint build does not try to link two allocators.
#[cfg(not(clippy))]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Human,
    Json,
    Csv,
}

#[derive(Parser)]
#[command(name = "memory-benchmark")]
#[command(about = "Memory usage benchmark for Turso SQL workloads")]
struct Args {
    /// Journal mode
    #[arg(short = 'm', long = "mode", default_value = "wal")]
    mode: JournalMode,

    /// Built-in workload profile
    #[arg(short = 'w', long = "workload", default_value = "insert-heavy")]
    workload: WorkloadProfile,

    /// Number of iterations for the workload
    #[arg(short = 'i', long = "iterations", default_value = "1000")]
    iterations: usize,

    /// Batch size (rows per transaction)
    #[arg(short = 'b', long = "batch-size", default_value = "100")]
    batch_size: usize,

    /// SQLite page cache size (in pages, negative = KiB)
    #[arg(long = "cache-size")]
    cache_size: Option<i64>,

    /// Number of concurrent connections
    #[arg(long = "connections", default_value = "1")]
    connections: usize,

    /// Busy timeout in milliseconds
    #[arg(long = "timeout", default_value = "30000")]
    timeout: u64,

    /// Output format
    #[arg(long = "format", default_value = "human")]
    format: OutputFormat,

    /// Run a final checkpoint after the workload completes
    #[arg(long)]
    checkpoint: bool,

    /// MVCC only: set `PRAGMA mvcc_checkpoint_threshold` (bytes; -1 disables
    /// auto-checkpoint). Use -1 to isolate the inline-GC effect.
    #[arg(long = "mvcc-checkpoint-threshold")]
    mvcc_checkpoint_threshold: Option<i64>,

    /// MVCC only: set `PRAGMA mvcc_gc_threshold` (live-version growth per
    /// inline GC pass; -1 disables inline GC). Toggle this for A/B runs.
    #[arg(long = "mvcc-gc-threshold")]
    mvcc_gc_threshold: Option<i64>,
}

/// Takes RSS snapshots at phase transitions and tracks the RSS peak after
/// every batch.
struct SnapshotObserver {
    start: Instant,
    snapshots: Vec<MemorySnapshot>,
    peak_bytes: usize,
}

impl WorkloadObserver for SnapshotObserver {
    fn on_phase(&mut self, phase: Phase) {
        let label = match phase {
            Phase::Setup => "setup",
            Phase::Run => "run-start",
            Phase::Checkpoint => "checkpoint",
            Phase::Done => unreachable!(),
        };
        self.snapshots.push(take_snapshot(self.start, label));
    }

    fn after_batch(&mut self) {
        let current = take_snapshot(self.start, "periodic");
        if current.rss_bytes > self.peak_bytes {
            self.peak_bytes = current.rss_bytes;
        }
    }
}

fn main() -> Result<()> {
    #[cfg(not(clippy))]
    let _profiler = dhat::Profiler::new_heap();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.connections.max(1))
        .build()?;

    rt.block_on(async_main(args))
}

async fn async_main(args: Args) -> Result<()> {
    let db_path = "memory_benchmark.db";
    clean_db_files(db_path);

    let cfg = WorkloadConfig {
        mode: args.mode,
        workload: args.workload,
        iterations: args.iterations,
        batch_size: args.batch_size,
        connections: args.connections,
        timeout: Duration::from_millis(args.timeout),
        cache_size: args.cache_size,
        checkpoint: args.checkpoint,
        mvcc_checkpoint_threshold: args.mvcc_checkpoint_threshold,
        mvcc_gc_threshold: args.mvcc_gc_threshold,
    };

    let start = Instant::now();

    // Baseline snapshot before any DB work
    let baseline_snapshot = take_snapshot(start, "baseline");
    let baseline = baseline_snapshot.rss_bytes;
    let mut observer = SnapshotObserver {
        start,
        snapshots: vec![baseline_snapshot],
        peak_bytes: baseline,
    };

    let workload_name = run_workload(db_path, &cfg, &mut observer).await?;

    // Final snapshot
    let final_snap = take_snapshot(start, "final");
    let peak_bytes = observer.peak_bytes.max(final_snap.rss_bytes);
    let mut snapshots = observer.snapshots;
    snapshots.push(final_snap.clone());

    let dhat_stats = dhat::HeapStats::get();
    let report = MemoryReport {
        mode: args.mode.to_string(),
        workload: workload_name,
        iterations: args.iterations,
        batch_size: args.batch_size,
        connections: args.connections,
        baseline_bytes: baseline,
        peak_bytes,
        final_bytes: final_snap.rss_bytes,
        net_growth_bytes: final_snap.rss_bytes.saturating_sub(baseline),
        heap_current_bytes: dhat_stats.curr_bytes,
        heap_peak_bytes: dhat_stats.max_bytes,
        total_allocs: dhat_stats.total_blocks,
        total_bytes_allocated: dhat_stats.total_bytes,
        snapshots,
        db_file_bytes: file_size(db_path),
        wal_file_bytes: {
            let wal_path = format!("{db_path}-wal");
            let size = file_size(&wal_path);
            if size > 0 { Some(size) } else { None }
        },
        log_file_bytes: {
            let log_path = format!("{db_path}-log");
            let size = file_size(&log_path);
            if size > 0 { Some(size) } else { None }
        },
    };

    match args.format {
        OutputFormat::Human => report.print_human(),
        OutputFormat::Json => report.print_json(),
        OutputFormat::Csv => {
            MemoryReport::print_csv_header();
            report.print_csv();
        }
    }

    Ok(())
}
