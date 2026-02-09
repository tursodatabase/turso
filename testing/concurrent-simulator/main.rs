/// Whopper CLI - The Turso deterministic simulator
use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use rand::{Rng, RngCore};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use turso_whopper::{StepResult, Whopper, WhopperOpts, properties::*, workloads::*};

/// Elle consistency model to use
#[derive(Debug, Clone, Copy, ValueEnum)]
enum ElleModel {
    /// List-append model: transactions append to and read from lists
    ListAppend,
    // TODO: rw-register model
}

#[derive(Parser)]
#[command(name = "turso_whopper")]
#[command(about = "The Turso Whopper Simulator")]
struct Args {
    /// Simulation mode (fast, chaos, ragnarök/ragnarok)
    #[arg(long, default_value = "fast")]
    mode: String,
    /// Max connections
    #[arg(long, default_value_t = 4)]
    max_connections: usize,
    #[arg(long, default_value_t = 0.0)]
    reopen_probability: f64,
    /// Max steps
    #[arg(long)]
    max_steps: Option<usize>,
    /// Keep mmap I/O files on disk after run
    #[arg(long)]
    keep: bool,
    /// Enable MVCC (Multi-Version Concurrency Control)
    #[arg(long)]
    enable_mvcc: bool,
    /// Enable database encryption
    #[arg(long)]
    enable_encryption: bool,
    /// Enable Elle consistency checking with specified model (uses only Elle workloads)
    #[arg(long, value_enum)]
    elle: Option<ElleModel>,
    /// Output path for Elle history EDN file
    #[arg(long, default_value = "elle-history.edn")]
    elle_output: String,
    /// Dump database files to simulator-output directory after run
    #[arg(long)]
    dump_db: bool,
}

fn main() -> anyhow::Result<()> {
    init_logger();

    let args = Args::parse();

    let seed = std::env::var("SEED")
        .ok()
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<u64>().expect("SEED must be a valid u64"))
        .unwrap_or_else(|| {
            let mut rng = rand::rng();
            rng.next_u64()
        });

    println!("mode = {}", args.mode);
    println!("seed = {seed}");

    let opts = build_opts(&args, seed)?;

    if opts.cosmic_ray_probability > 0.0 {
        println!("cosmic ray probability = {}", opts.cosmic_ray_probability);
    }

    let mut whopper = Whopper::new(opts)?;

    let max_steps = whopper.max_steps;
    let progress_interval = max_steps / 10;
    let progress_stages = [
        "       .             I/U/D/C",
        "       .             ",
        "       .             ",
        "       |             ",
        "       |             ",
        "      ╱|╲            ",
        "     ╱╲|╱╲           ",
        "    ╱╲╱|╲╱╲          ",
        "   ╱╲╱╲|╱╲╱╲         ",
        "  ╱╲╱╲╱|╲╱╲╱╲        ",
        " ╱╲╱╲╱╲|╱╲╱╲╱╲       ",
    ];
    let mut progress_index = 0;
    println!("{}", progress_stages[progress_index]);
    progress_index += 1;

    while !whopper.is_done() {
        if whopper.rng.random_bool(args.reopen_probability) {
            whopper.reopen().unwrap();
        }
        match whopper.step()? {
            StepResult::Ok => {}
            StepResult::WalSizeLimitExceeded => break,
        }

        if progress_interval > 0 && whopper.current_step % progress_interval == 0 {
            let stats = &whopper.stats;
            println!(
                "{}{}/{}/{}/{}",
                progress_stages[progress_index],
                stats.inserts,
                stats.updates,
                stats.deletes,
                stats.integrity_checks
            );
            progress_index += 1;
        }
    }

    // Finalize properties (e.g., export Elle history)
    whopper.finalize_properties()?;

    // Dump database files if requested
    if args.dump_db {
        whopper.dump_db_files()?;
    }

    // Print Elle analysis instructions if enabled
    if args.elle.is_some() {
        let output_path = &args.elle_output;
        println!("\nElle history exported to: {output_path}");
    }

    Ok(())
}

fn build_opts(args: &Args, seed: u64) -> anyhow::Result<WhopperOpts> {
    let mut base_opts = match args.mode.as_str() {
        "fast" => WhopperOpts::fast(),
        "chaos" => WhopperOpts::chaos(),
        "ragnarök" | "ragnarok" => WhopperOpts::ragnarok(),
        mode => return Err(anyhow::anyhow!("Unknown mode: {}", mode)),
    };

    if let Some(max_steps) = args.max_steps {
        base_opts = base_opts.with_max_steps(max_steps);
    }

    // Build workloads and properties based on Elle mode
    let (workloads, properties) = if let Some(elle_model) = args.elle {
        // Elle mode: only Elle workloads + transactions
        let w: Vec<(u32, Box<dyn Workload>)> = match elle_model {
            ElleModel::ListAppend => vec![
                // Elle list-append workloads (single pre-created table)
                (40, Box::new(ElleAppendWorkload::new())),
                (30, Box::new(ElleReadWorkload)),
                // Transaction control
                (30, Box::new(BeginWorkload)),
                (15, Box::new(CommitWorkload)),
                (5, Box::new(RollbackWorkload)),
            ],
        };

        let output_path = PathBuf::from(&args.elle_output);
        let p: Vec<Box<dyn Property>> = vec![Box::new(ElleHistoryRecorder::new(output_path))];

        (w, p)
    } else {
        // Normal mode: all workloads
        let w: Vec<(u32, Box<dyn Workload>)> = vec![
            // Idle-only workloads
            (10, Box::new(IntegrityCheckWorkload)),
            (5, Box::new(WalCheckpointWorkload)),
            (10, Box::new(CreateSimpleTableWorkload)),
            (20, Box::new(SimpleSelectWorkload)),
            (20, Box::new(SimpleInsertWorkload)),
            // Index workloads
            (2, Box::new(CreateIndexWorkload)),
            (2, Box::new(DropIndexWorkload)),
            // Transaction workloads
            (30, Box::new(BeginWorkload)),
            (10, Box::new(CommitWorkload)),
            (10, Box::new(RollbackWorkload)),
        ];

        let p: Vec<Box<dyn Property>> = vec![
            Box::new(IntegrityCheckProperty),
            Box::new(SimpleKeysDoNotDisappear::new()),
        ];

        (w, p)
    };

    let opts = base_opts
        .with_seed(seed)
        .with_max_connections(args.max_connections)
        .with_keep_files(args.keep)
        .with_enable_mvcc(args.enable_mvcc)
        .with_enable_encryption(args.enable_encryption)
        .with_elle_enabled(args.elle.is_some())
        .with_workloads(workloads)
        .with_properties(properties);

    Ok(opts)
}

fn init_logger() {
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_line_number(true)
                .without_time()
                .with_thread_ids(false),
        )
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .try_init();
}
