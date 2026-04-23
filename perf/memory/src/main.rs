mod measure;
mod profile;

use std::time::{Duration, Instant};

use anyhow::Result;
use clap::{Parser, ValueEnum};
use profile::{
    Phase, Profile, WorkItem, checkpoint::Checkpoint, insert::InsertHeavy, mixed::Mixed,
    read::ReadHeavy, scan::ScanHeavy, series_blob::SeriesBlob,
};
use turso::Connection;
use turso::params::Params;

use crate::measure::{MemoryReport, MemorySnapshot, file_size, take_snapshot};

// Workspace Clippy runs with `--all-features`, which enables `turso`'s
// mimalloc-backed global allocator. Skip the benchmark-only dhat allocator
// under Clippy so the lint build does not try to link two allocators.
#[cfg(not(clippy))]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum JournalMode {
    Wal,
    Mvcc,
}

impl std::fmt::Display for JournalMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalMode::Wal => write!(f, "wal"),
            JournalMode::Mvcc => write!(f, "mvcc"),
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum WorkloadProfile {
    InsertHeavy,
    ReadHeavy,
    Mixed,
    ScanHeavy,
    SeriesBlob,
}

impl std::fmt::Display for WorkloadProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkloadProfile::InsertHeavy => write!(f, "insert-heavy"),
            WorkloadProfile::ReadHeavy => write!(f, "read-heavy"),
            WorkloadProfile::Mixed => write!(f, "mixed"),
            WorkloadProfile::ScanHeavy => write!(f, "scan-heavy"),
            WorkloadProfile::SeriesBlob => write!(f, "series-blob"),
        }
    }
}

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

    let mut profile = create_profile(
        args.workload,
        args.iterations,
        args.batch_size,
        args.checkpoint,
    );
    let timeout = Duration::from_millis(args.timeout);

    let start = Instant::now();
    let mut snapshots: Vec<MemorySnapshot> = Vec::new();

    // Baseline snapshot before any DB work
    snapshots.push(take_snapshot(start, "baseline"));

    let db = turso::Builder::new_local(db_path).build().await?;

    // Setup connection for schema/seeding and journal mode
    let setup_conn = db.connect()?;
    setup_conn.busy_timeout(timeout)?;

    let mode_str = match args.mode {
        JournalMode::Wal => "'wal'",
        JournalMode::Mvcc => "'mvcc'",
    };
    setup_conn.pragma_update("journal_mode", mode_str).await?;

    if let Some(cache_size) = args.cache_size {
        setup_conn
            .pragma_update("cache_size", &cache_size.to_string())
            .await?;
    }

    let begin_stmt = match args.mode {
        JournalMode::Wal => "BEGIN",
        JournalMode::Mvcc => "BEGIN CONCURRENT",
    };

    let mut last_phase = None;
    let mut peak_bytes = snapshots[0].rss_bytes;

    loop {
        let (phase, batches) = profile.next_batch(args.connections);

        if phase == Phase::Done {
            break;
        }

        // Take snapshot on phase transition
        if last_phase != Some(phase) {
            let label = match phase {
                Phase::Setup => "setup",
                Phase::Run => "run-start",
                Phase::Checkpoint => "checkpoint",
                Phase::Done => unreachable!(),
            };
            snapshots.push(take_snapshot(start, label));
            last_phase = Some(phase);
        }

        if batches.is_empty() {
            continue;
        }

        match phase {
            Phase::Setup => {
                // Setup runs sequentially on a single connection.
                let items = batches.into_iter().next().unwrap_or_default();
                if !items.is_empty() {
                    setup_conn.execute("BEGIN", ()).await?;
                    execute_items(&setup_conn, items).await?;
                    setup_conn.execute("COMMIT", ()).await?;
                }
            }
            Phase::Run => {
                // Run phase: dispatch batches concurrently across connections.
                let mut handles = Vec::with_capacity(batches.len());
                for items in batches {
                    if items.is_empty() {
                        continue;
                    }
                    let conn = db.connect()?;
                    conn.busy_timeout(timeout)?;
                    let begin = begin_stmt.to_string();
                    handles.push(tokio::spawn(async move {
                        conn.execute(&begin, ()).await?;
                        execute_items(&conn, items).await?;
                        conn.execute("COMMIT", ()).await?;
                        Ok::<_, turso::Error>(())
                    }));
                }
                for handle in handles {
                    handle.await??;
                }
            }
            Phase::Checkpoint => {
                let items = batches.into_iter().next().unwrap_or_default();
                if !items.is_empty() {
                    execute_checkpoint_items(&setup_conn, items).await?;
                }
            }
            Phase::Done => unreachable!(),
        }

        // Track peak
        let current = take_snapshot(start, "periodic");
        if current.rss_bytes > peak_bytes {
            peak_bytes = current.rss_bytes;
        }
    }

    // Final snapshot
    let final_snap = take_snapshot(start, "final");
    peak_bytes = peak_bytes.max(final_snap.rss_bytes);
    snapshots.push(final_snap.clone());

    let baseline = snapshots[0].rss_bytes;
    let dhat_stats = dhat::HeapStats::get();
    let report = MemoryReport {
        mode: args.mode.to_string(),
        workload: profile.name().to_string(),
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

async fn execute_items(conn: &Connection, items: Vec<WorkItem>) -> Result<(), turso::Error> {
    for item in items {
        let is_query = item
            .sql
            .trim_start()
            .get(..6)
            .is_some_and(|s| s.eq_ignore_ascii_case("SELECT"));
        if is_query {
            let mut rows = conn
                .query(&item.sql, Params::Positional(item.params))
                .await?;
            while rows.next().await?.is_some() {}
        } else if item.params.is_empty() {
            conn.execute(&item.sql, ()).await?;
        } else {
            let mut stmt = conn.prepare(&item.sql).await?;
            stmt.execute(Params::Positional(item.params)).await?;
        }
    }
    Ok(())
}

fn create_profile(
    workload: WorkloadProfile,
    iterations: usize,
    batch_size: usize,
    checkpoint: bool,
) -> Box<dyn Profile> {
    let profile: Box<dyn Profile> = match workload {
        WorkloadProfile::InsertHeavy => Box::new(InsertHeavy::new(iterations, batch_size)),
        WorkloadProfile::ReadHeavy => Box::new(ReadHeavy::new(iterations, batch_size)),
        WorkloadProfile::Mixed => Box::new(Mixed::new(iterations, batch_size)),
        WorkloadProfile::ScanHeavy => Box::new(ScanHeavy::new(iterations, batch_size)),
        WorkloadProfile::SeriesBlob => Box::new(SeriesBlob::new(iterations, batch_size)),
    };

    if checkpoint {
        Box::new(Checkpoint::new(profile))
    } else {
        profile
    }
}

async fn execute_checkpoint_items(
    conn: &Connection,
    items: Vec<WorkItem>,
) -> Result<(), turso::Error> {
    for item in items {
        let mut rows = conn
            .query(&item.sql, Params::Positional(item.params))
            .await?;
        while rows.next().await?.is_some() {}
    }
    Ok(())
}

fn clean_db_files(db_path: &str) {
    for suffix in ["", "-wal", "-shm", "-journal", "-log"] {
        let path = if suffix.is_empty() {
            db_path.to_string()
        } else {
            format!("{db_path}{suffix}")
        };
        if std::path::Path::new(&path).exists() {
            let _ = std::fs::remove_file(&path);
        }
    }
}
