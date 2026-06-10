use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};

mod generators;
mod plot;
mod runner;
mod setup;
mod stats;
mod workload;

use generators::GenState;
use plot::{render_plot_html, PlotMeta};
use runner::{compile_ops, worker, WorkerCfg};
use setup::{hydrate_pools, setup_database};
use stats::{print_summary, OutputFormat};
use workload::{load_workload, needed_pools, total_seed_rows};

#[derive(Debug, Parser)]
#[command(name = "turso-load")]
#[command(about = "Run YAML-defined load tests against a local Turso database")]
struct Args {
    /// Workload YAML file.
    workload: PathBuf,

    /// Database path. Use :memory: for an in-memory database.
    #[arg(long, default_value = "turso-load.db")]
    db: String,

    /// Number of concurrent database connections.
    #[arg(short = 'c', long, default_value_t = 1)]
    connections: usize,

    /// Measured run duration in seconds.
    #[arg(short = 'd', long, default_value_t = 30.0)]
    duration: f64,

    /// Warmup duration in seconds. Warmup operations are not counted.
    #[arg(long, default_value_t = 0.0)]
    warmup: f64,

    /// Target total operations per second. Omit to run as fast as possible.
    #[arg(long)]
    rate: Option<f64>,

    /// RNG seed for reproducible generated values.
    #[arg(long, default_value_t = 1)]
    seed: u64,

    /// Skip schema creation and data seeding; hydrate ref pools from the database.
    #[arg(long)]
    no_setup: bool,

    /// Enable MVCC journal mode before setup and load.
    #[arg(long)]
    mvcc: bool,

    /// Busy timeout in milliseconds for every connection.
    #[arg(long, default_value_t = 30_000)]
    busy_timeout_ms: u64,

    /// Output format for the terminal summary.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    output: OutputFormat,

    /// Write a self-contained HTML latency report to this path.
    #[arg(long)]
    plot: Option<PathBuf>,

    /// Print unexpected operation errors as they occur.
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.connections == 0 {
        anyhow::bail!("--connections must be greater than zero");
    }
    if !(args.duration > 0.0) {
        anyhow::bail!("--duration must be greater than zero");
    }
    if args.warmup < 0.0 {
        anyhow::bail!("--warmup must not be negative");
    }
    if let Some(rate) = args.rate {
        if !(rate > 0.0) {
            anyhow::bail!("--rate must be greater than zero");
        }
    }

    let workload = load_workload(&args.workload)?;
    let pool_keys = needed_pools(&workload);
    let mut gen = GenState::new(args.seed, pool_keys);
    if args.no_setup {
        gen.seqs.offset = total_seed_rows(&workload) as i64;
    }

    let db = turso::Builder::new_local(&args.db)
        .experimental_attach(true)
        .experimental_custom_types(true)
        .experimental_generated_columns(true)
        .experimental_index_method(true)
        .experimental_materialized_views(true)
        .experimental_multiprocess_wal(true)
        .experimental_vacuum(true)
        .experimental_without_rowid(true)
        .build()
        .await
        .with_context(|| format!("opening database {}", args.db))?;
    let setup_conn = db.connect()?;
    setup_conn.busy_timeout(Duration::from_millis(args.busy_timeout_ms))?;
    if args.mvcc {
        setup_conn.pragma_update("journal_mode", "'mvcc'").await?;
    }

    if args.no_setup {
        hydrate_pools(&setup_conn, &workload, &mut gen).await?;
    } else {
        let progress = seed_progress(total_seed_rows(&workload));
        setup_database(&setup_conn, &workload, &mut gen, &progress).await?;
        progress.finish_and_clear();
    }

    let (ops, total_weight) = compile_ops(&workload)?;
    let ops = Arc::new(ops);
    let gen = Arc::new(Mutex::new(gen));
    let concurrent_ok = Arc::new(AtomicBool::new(true));
    let rate_per_connection = args.rate.map(|rate| rate / args.connections as f64);
    let cfg = Arc::new(WorkerCfg {
        rate: rate_per_connection,
        verbose: args.verbose,
    });

    let warmup = Duration::from_secs_f64(args.warmup);
    let duration = Duration::from_secs_f64(args.duration);
    let start = Instant::now();
    let measure_from = start + warmup;
    let deadline = measure_from + duration;

    let mut handles = Vec::with_capacity(args.connections);
    for _ in 0..args.connections {
        let conn = db.connect()?;
        conn.busy_timeout(Duration::from_millis(args.busy_timeout_ms))?;
        handles.push(tokio::spawn(worker(
            conn,
            Arc::clone(&ops),
            total_weight,
            Arc::clone(&gen),
            Arc::clone(&concurrent_ok),
            Arc::clone(&cfg),
            measure_from,
            deadline,
        )));
    }

    let mut stats = stats::Stats::new();
    for handle in handles {
        stats.merge(handle.await.context("worker task panicked")?);
    }

    let samples = stats.samples();
    let summary = stats.summary(duration.as_secs_f64());
    print_summary(&summary, args.output);

    if let Some(path) = args.plot {
        let html = render_plot_html(
            &summary,
            samples,
            &PlotMeta {
                workload: workload.name.clone(),
                description: workload.description.clone(),
                db: args.db,
                connections: args.connections,
            },
        );
        std::fs::write(&path, html)
            .with_context(|| format!("writing plot report {}", path.display()))?;
        eprintln!("wrote {}", path.display());
    }

    Ok(())
}

fn seed_progress(rows: u64) -> ProgressBar {
    if rows == 0 {
        return ProgressBar::hidden();
    }
    let progress = ProgressBar::new(rows);
    progress.set_style(
        ProgressStyle::with_template(
            "seeding [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} rows",
        )
        .unwrap()
        .progress_chars("=> "),
    );
    progress
}
