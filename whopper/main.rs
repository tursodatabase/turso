/// Whopper CLI - The Turso deterministic simulator
use clap::Parser;
use rand::{Rng, RngCore};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use turso_whopper::{StepResult, Whopper, WhopperOpts};

#[derive(Parser)]
#[command(name = "turso_whopper")]
#[command(about = "The Turso Whopper Simulator")]
struct Args {
    /// Simulation mode (fast, chaos, ragnarök/ragnarok)
    #[arg(long, default_value = "fast")]
    mode: String,
    /// Keep mmap I/O files on disk after run
    #[arg(long)]
    keep: bool,
    /// Enable MVCC (Multi-Version Concurrency Control)
    #[arg(long)]
    enable_mvcc: bool,
    /// Enable database encryption
    #[arg(long)]
    enable_encryption: bool,
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

    Ok(())
}

fn build_opts(args: &Args, seed: u64) -> anyhow::Result<WhopperOpts> {
    let mut rng = rand::rng();
    let max_connections = rng.random_range(1..=8) as usize;

    let base_opts = match args.mode.as_str() {
        "fast" => WhopperOpts::fast(),
        "chaos" => WhopperOpts::chaos(),
        "ragnarök" | "ragnarok" => WhopperOpts::ragnarok(),
        mode => return Err(anyhow::anyhow!("Unknown mode: {}", mode)),
    };

    Ok(base_opts
        .with_seed(seed)
        .with_max_connections(max_connections)
        .with_keep_files(args.keep)
        .with_enable_mvcc(args.enable_mvcc)
        .with_enable_encryption(args.enable_encryption))
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
