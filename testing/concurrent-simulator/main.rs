/// Whopper CLI - The Turso deterministic simulator
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand, ValueEnum};
use rand::{Rng, RngCore};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
use turso_whopper::multiprocess::{MultiprocessOpts, MultiprocessWhopper};
use turso_whopper::{
    StepResult, Whopper, WhopperOpts,
    chaotic_elle::{ChaoticElleProfile, ChaoticWorkloadProfile, ElleModelKind},
    properties::*,
    workloads::*,
};

/// Elle consistency model to use
#[derive(Debug, Clone, Copy, ValueEnum)]
enum ElleModel {
    /// List-append model: transactions append to and read from lists
    ListAppend,
    /// Rw-register model: transactions write and read single values
    RwRegister,
}

#[derive(Parser)]
#[command(name = "turso_whopper")]
#[command(about = "The Turso Whopper Simulator")]
struct Args {
    #[command(subcommand)]
    subcommand: Option<SubCmd>,

    /// Simulation mode (fast, chaos, ragnarök/ragnarok)
    #[arg(long, default_value = "fast")]
    mode: String,
    /// Max connections
    #[arg(long, default_value_t = 4)]
    max_connections: usize,
    /// Number of worker processes in multiprocess mode. Defaults to `max_connections`.
    #[arg(long)]
    processes: Option<usize>,
    /// Number of connections opened inside each worker process in multiprocess mode.
    #[arg(long, default_value_t = 1)]
    connections_per_process: usize,
    #[arg(long, default_value_t = 0.0)]
    reopen_probability: f64,
    /// Max steps
    #[arg(long)]
    max_steps: Option<usize>,
    /// Keep files on disk after run
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
    /// Run in multiprocess mode (spawns OS processes instead of in-process fibers)
    #[arg(long)]
    multiprocess: bool,
    /// Probability of killing a worker process per step (multiprocess mode only)
    #[arg(long, default_value_t = 0.0)]
    kill_probability: f64,
    /// Probability of restarting the full worker cohort per step (multiprocess mode only)
    #[arg(long, default_value_t = 0.0)]
    restart_probability: f64,
    /// Stream multiprocess operation/lifecycle history as JSONL for deterministic debugging
    #[arg(long)]
    history_output: Option<PathBuf>,
}

#[derive(Subcommand)]
enum SubCmd {
    /// Run as a worker process (internal, called by multiprocess coordinator)
    Worker {
        /// Path to the database file
        #[arg(long)]
        db_path: String,
        /// Enable MVCC mode
        #[arg(long)]
        enable_mvcc: bool,
        /// Number of connections to open inside this worker process
        #[arg(long, default_value_t = 1)]
        connections_per_process: usize,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Dispatch to worker BEFORE init_logger so the worker can install its own
    // stderr-only subscriber without the coordinator's logger polluting stdout.
    if let Some(SubCmd::Worker {
        db_path,
        enable_mvcc,
        connections_per_process,
    }) = &args.subcommand
    {
        #[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
        {
            return turso_whopper::worker::run_worker(
                db_path,
                *enable_mvcc,
                *connections_per_process,
            );
        }
        #[cfg(not(all(any(unix, target_os = "windows"), target_pointer_width = "64")))]
        {
            return Err(anyhow::anyhow!(
                "worker mode is only supported on 64-bit Unix and Windows hosts"
            ));
        }
    }

    init_logger();

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

    if args.multiprocess {
        return run_multiprocess(&args, seed);
    }

    run_inprocess(&args, seed)
}

#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
fn run_multiprocess(args: &Args, seed: u64) -> anyhow::Result<()> {
    if args.enable_mvcc {
        eprintln!("MVCC mode not yet supported with multiprocess mode");
        std::process::exit(1);
    }
    let base_max_steps = match args.mode.as_str() {
        "fast" => 100_000,
        "chaos" => 10_000_000,
        "ragnarök" | "ragnarok" => 1_000_000,
        mode => return Err(anyhow::anyhow!("Unknown mode: {}", mode)),
    };
    let max_steps = args.max_steps.unwrap_or(base_max_steps);

    let (workloads, properties, elle_tables, chaotic_profiles) =
        build_workloads_and_properties(args);
    let process_count = args.processes.unwrap_or(args.max_connections);

    let opts = MultiprocessOpts {
        seed: Some(seed),
        process_count,
        connections_per_process: args.connections_per_process,
        max_steps,
        enable_mvcc: args.enable_mvcc,
        elle_tables,
        workloads,
        properties,
        chaotic_profiles,
        kill_probability: args.kill_probability,
        restart_probability: args.restart_probability,
        history_output: args.history_output.clone(),
        keep_files: args.keep,
    };

    println!(
        "multiprocess = true ({} processes, {} connections/process, {} total connections)",
        process_count,
        args.connections_per_process,
        process_count.saturating_mul(args.connections_per_process)
    );
    if args.kill_probability > 0.0 {
        println!("kill_probability = {}", args.kill_probability);
    }
    if args.restart_probability > 0.0 {
        println!("restart_probability = {}", args.restart_probability);
    }
    if let Some(path) = &args.history_output {
        println!("history_output = {}", path.display());
    }

    let mut whopper = MultiprocessWhopper::new(opts)?;

    let progress_interval = max_steps / 10;
    let elle_mode = args.elle.is_some();
    let progress_stages = progress_art(elle_mode);
    let mut progress_index = 0;
    println!("{}", progress_stages[progress_index]);
    progress_index += 1;

    while !whopper.is_done() {
        whopper.step()?;

        if progress_interval > 0 && whopper.current_step % progress_interval == 0 {
            let stats = &whopper.stats;
            let counts = format_stats(stats, elle_mode);
            println!("{}{}", progress_stages[progress_index], counts);
            progress_index += 1;
        }
    }

    whopper.finalize()?;

    if whopper.stats.corruption_events > 0 {
        println!(
            "\nWARNING: {} corruption events detected during simulation",
            whopper.stats.corruption_events
        );
    }

    if args.elle.is_some() {
        println!("\nElle history exported to: {}", args.elle_output);
    }

    Ok(())
}

#[cfg(not(all(any(unix, target_os = "windows"), target_pointer_width = "64")))]
fn run_multiprocess(_args: &Args, _seed: u64) -> anyhow::Result<()> {
    Err(anyhow::anyhow!(
        "multiprocess mode is only supported on 64-bit Unix and Windows hosts"
    ))
}

fn run_inprocess(args: &Args, seed: u64) -> anyhow::Result<()> {
    let opts = build_inprocess_opts(args, seed)?;

    if opts.cosmic_ray_probability > 0.0 {
        println!("cosmic ray probability = {}", opts.cosmic_ray_probability);
    }

    let mut whopper = Whopper::new(opts)?;

    let max_steps = whopper.max_steps;
    let progress_interval = max_steps / 10;
    let elle_mode = args.elle.is_some();
    let progress_stages = progress_art(elle_mode);
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
            let counts = format_stats(stats, elle_mode);
            println!("{}{}", progress_stages[progress_index], counts);
            progress_index += 1;
        }
    }

    whopper.finalize_properties()?;

    if args.dump_db {
        whopper.dump_db_files()?;
    }

    if args.elle.is_some() {
        println!("\nElle history exported to: {}", args.elle_output);
    }

    Ok(())
}

fn build_workloads_and_properties(args: &Args) -> BuildArtifacts {
    if let Some(elle_model) = args.elle {
        let elle_counter = Arc::new(std::sync::atomic::AtomicI64::new(1));

        let (table_name, create_sql) = match elle_model {
            ElleModel::ListAppend => (
                "elle_lists",
                "CREATE TABLE IF NOT EXISTS elle_lists (key TEXT PRIMARY KEY, vals TEXT DEFAULT '')",
            ),
            ElleModel::RwRegister => (
                "elle_rw",
                "CREATE TABLE IF NOT EXISTS elle_rw (key TEXT PRIMARY KEY, val INTEGER)",
            ),
        };

        let model_kind = match elle_model {
            ElleModel::ListAppend => ElleModelKind::ListAppend,
            ElleModel::RwRegister => ElleModelKind::RwRegister,
        };

        let chaotic: Vec<(f64, &'static str, Box<dyn ChaoticWorkloadProfile>)> = vec![(
            0.3,
            "chaotic-elle",
            Box::new(ChaoticElleProfile::new(
                table_name.to_string(),
                model_kind,
                elle_counter.clone(),
                args.enable_mvcc,
            )),
        )];

        let w: Vec<(u32, Box<dyn Workload>)> = match elle_model {
            ElleModel::ListAppend => vec![
                (40, Box::new(ElleAppendWorkload::with_counter(elle_counter))),
                (30, Box::new(ElleReadWorkload)),
                (30, Box::new(BeginWorkload)),
                (15, Box::new(CommitWorkload)),
                (5, Box::new(RollbackWorkload)),
            ],
            ElleModel::RwRegister => vec![
                (
                    40,
                    Box::new(ElleRwWriteWorkload::with_counter(elle_counter)),
                ),
                (30, Box::new(ElleRwReadWorkload)),
                (30, Box::new(BeginWorkload)),
                (15, Box::new(CommitWorkload)),
                (5, Box::new(RollbackWorkload)),
            ],
        };

        let output_path = PathBuf::from(&args.elle_output);
        let p: Vec<Box<dyn Property>> = vec![Box::new(ElleHistoryRecorder::new(output_path))];
        let et = vec![(table_name.to_string(), create_sql.to_string())];

        (w, p, et, chaotic)
    } else {
        let w: Vec<(u32, Box<dyn Workload>)> = vec![
            (10, Box::new(IntegrityCheckWorkload)),
            (5, Box::new(WalCheckpointWorkload)),
            (10, Box::new(CreateSimpleTableWorkload)),
            (20, Box::new(SimpleSelectWorkload)),
            (20, Box::new(SimpleInsertWorkload)),
            (15, Box::new(UpdateWorkload)),
            (15, Box::new(DeleteWorkload)),
            (2, Box::new(CreateIndexWorkload)),
            (2, Box::new(DropIndexWorkload)),
            (30, Box::new(BeginWorkload)),
            (10, Box::new(CommitWorkload)),
            (10, Box::new(RollbackWorkload)),
        ];

        let p: Vec<Box<dyn Property>> = vec![
            Box::new(IntegrityCheckProperty),
            Box::new(SimpleKeysDoNotDisappear::new()),
        ];

        (w, p, vec![], vec![])
    }
}

type WorkerWorkloads = Vec<(u32, Box<dyn Workload>)>;
type PropertyList = Vec<Box<dyn Property>>;
type TableSchemas = Vec<(String, String)>;
type ChaosProfiles = Vec<(f64, &'static str, Box<dyn ChaoticWorkloadProfile>)>;
type BuildArtifacts = (WorkerWorkloads, PropertyList, TableSchemas, ChaosProfiles);

fn build_inprocess_opts(args: &Args, seed: u64) -> anyhow::Result<WhopperOpts> {
    let mut base_opts = match args.mode.as_str() {
        "fast" => WhopperOpts::fast(),
        "chaos" => WhopperOpts::chaos(),
        "ragnarök" | "ragnarok" => WhopperOpts::ragnarok(),
        mode => return Err(anyhow::anyhow!("Unknown mode: {}", mode)),
    };

    if let Some(max_steps) = args.max_steps {
        base_opts = base_opts.with_max_steps(max_steps);
    }

    let (workloads, properties, elle_tables, chaotic_profiles) =
        build_workloads_and_properties(args);

    let opts = base_opts
        .with_seed(seed)
        .with_max_connections(args.max_connections)
        .with_keep_files(args.keep)
        .with_enable_mvcc(args.enable_mvcc)
        .with_enable_encryption(args.enable_encryption)
        .with_elle_tables(elle_tables)
        .with_workloads(workloads)
        .with_properties(properties)
        .with_chaotic_profiles(chaotic_profiles);

    Ok(opts)
}

fn format_stats(stats: &turso_whopper::Stats, elle_mode: bool) -> String {
    if elle_mode {
        format!("{}/{}", stats.elle_writes, stats.elle_reads)
    } else {
        format!(
            "{}/{}/{}/{}",
            stats.inserts, stats.updates, stats.deletes, stats.integrity_checks
        )
    }
}

fn progress_art(elle_mode: bool) -> [&'static str; 11] {
    [
        if elle_mode {
            "       .             W/R"
        } else {
            "       .             I/U/D/C"
        },
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
    ]
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
