/// Whopper CLI - The Turso deterministic simulator
use std::path::PathBuf;

use clap::Parser;
use rand::{Rng, RngCore};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use turso_whopper::{StepResult, Whopper, WhopperOpts, properties::*, workloads::*};

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
    /// Enable Elle history recording for transactional consistency checking
    #[arg(long)]
    enable_elle: bool,
    /// Output path for Elle history EDN file
    #[arg(long, default_value = "elle-history.edn")]
    elle_output: String,
}

fn main() -> anyhow::Result<()> {
    init_logger();

    let args = Args::parse();

    let seed = std::env::var("SEED")
        .ok()
        .map(|s| s.parse::<u64>().unwrap())
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

    // Print Elle analysis instructions if enabled
    if args.enable_elle {
        let output_path = &args.elle_output;
        println!("\nElle history exported to: {output_path}");
        println!("\nTo analyze with elle-cli:");
        println!(
            "  clojure -Sdeps '{{:deps {{elle-cli/elle-cli {{:mvn/version \"0.2.1\"}}}}}}' \\"
        );
        println!("    -M -m elle-cli.core --model list-append {output_path}");
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

    // Build workloads list
    let mut workloads: Vec<(u32, Box<dyn Workload>)> = vec![
        // Idle-only workloads
        (10, Box::new(IntegrityCheckWorkload)),
        (5, Box::new(WalCheckpointWorkload)),
        (10, Box::new(CreateSimpleTableWorkload)),
        (20, Box::new(SimpleSelectWorkload)),
        (20, Box::new(SimpleInsertWorkload)),
        // DML workloads (work in both Idle and InTx)
        // (1, Box::new(SelectWorkload)),
        // (30, Box::new(InsertWorkload)),
        // (20, Box::new(UpdateWorkload)),
        // (10, Box::new(DeleteWorkload)),
        (2, Box::new(CreateIndexWorkload)),
        (2, Box::new(DropIndexWorkload)),
        // InTx-only workloads
        (30, Box::new(BeginWorkload)),
        (10, Box::new(CommitWorkload)),
        (10, Box::new(RollbackWorkload)),
    ];

    // Build properties list
    let mut properties: Vec<Box<dyn Property>> = vec![
        Box::new(IntegrityCheckProperty),
        Box::new(SimpleKeysDoNotDisappear::new()),
    ];

    // Add Elle workloads and property if enabled
    if args.enable_elle {
        // Add Elle workloads
        workloads.push((10, Box::new(CreateElleTableWorkload)));
        workloads.push((30, Box::new(ElleAppendWorkload::new())));
        workloads.push((20, Box::new(ElleReadWorkload)));

        // Create Elle history recorder (writes incrementally to file)
        let output_path = PathBuf::from(&args.elle_output);
        properties.push(Box::new(ElleHistoryRecorder::new(output_path)));
    }

    let opts = base_opts
        .with_seed(seed)
        .with_max_connections(args.max_connections)
        .with_keep_files(args.keep)
        .with_enable_mvcc(args.enable_mvcc)
        .with_enable_encryption(args.enable_encryption)
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
