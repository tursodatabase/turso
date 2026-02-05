//! Diferential Fuzzer for Turso.
//!
//! This binary runs a differential testing fuzzer that compares Turso
//! results against SQLite for generated SQL statements.

use std::{
    io::{IsTerminal, stdin},
    panic::{self},
    sync::Arc,
};

use anyhow::Result;
use clap::{Parser, Subcommand};
use differential_fuzzer::{Fuzzer, GeneratorKind, SimConfig};
use parking_lot::Mutex;
use rand::RngCore;

/// SQLancer-style differential testing fuzzer for Turso.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Random seed for deterministic execution (only used without subcommand).
    #[arg(short, long, default_value_t = rand::rng().next_u64())]
    seed: u64,

    /// Number of tables to create.
    #[arg(short = 't', long, default_value_t = 2)]
    num_tables: usize,

    /// Number of columns per table.
    #[arg(short = 'c', long, default_value_t = 5)]
    columns_per_table: usize,

    /// Number of statements to generate and execute.
    #[arg(short = 'n', long, default_value_t = 100)]
    num_statements: usize,

    /// Enable verbose output.
    #[arg(short, long)]
    verbose: bool,

    /// Persist database files to disk after simulation.
    #[arg(short, long)]
    keep_files: bool,

    /// SQL generator backend to use.
    #[arg(short = 'g', long, default_value = "sql-gen", value_enum)]
    generator: GeneratorKind,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the fuzzer in a loop with random seeds.
    Loop {
        /// Number of iterations to run (0 for infinite).
        #[arg(default_value_t = 0)]
        iterations: u64,
    },
}

fn main() -> Result<()> {
    // Initialize tracing
    let mut subscriber = tracing_subscriber::fmt().with_env_filter(
        tracing_subscriber::EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()),
    );

    if !stdin().is_terminal() {
        subscriber = subscriber.with_ansi(false)
    }
    subscriber.init();

    let mut args = Args::parse();

    match args.command {
        Some(Commands::Loop { iterations }) => {
            let mut iteration = 0u64;
            loop {
                args.seed = rand::rng().next_u64();
                tracing::info!("Iteration {}: seed {}", iteration + 1, args.seed);
                run_single(&args)?;

                iteration += 1;
                if iterations > 0 && iteration >= iterations {
                    tracing::info!("Completed {} iterations successfully", iterations);
                    break;
                }
            }
            Ok(())
        }
        None => run_single(&args),
    }
}

fn run_single(args: &Args) -> Result<()> {
    let config = SimConfig {
        seed: args.seed,
        num_tables: args.num_tables,
        columns_per_table: args.columns_per_table,
        num_statements: args.num_statements,
        verbose: args.verbose,
        keep_files: args.keep_files,
        generator: args.generator,
    };

    tracing::info!("Starting differential_fuzzer with config: {:?}", config);

    let fuzzer = Fuzzer::new(config)?;

    let panic_info = Arc::new(Mutex::new(None::<String>));
    let info_clone = Arc::clone(&panic_info);

    let prev_hook = panic::take_hook();

    panic::set_hook(Box::new(move |info| {
        *info_clone.lock() = Some(info.to_string());
    }));

    let stats = std::panic::catch_unwind(|| fuzzer.run()).map_err(|err| {
        let msg = if let Some(s) = err.downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = err.downcast_ref::<String>() {
            s.clone()
        } else {
            "Unknown panic".to_string()
        };
        let mut err = anyhow::anyhow!("{msg}");
        if let Some(msg) = panic_info.lock().take() {
            err = err.context(msg);
        }

        err
    });
    let stats = match stats {
        Ok(inner) => inner,
        Err(e) => Err(e),
    };

    panic::set_hook(prev_hook);

    if args.keep_files {
        tracing::info!("Persisting database files to disk...");
        fuzzer.persist_files()?;
    }

    // Write schema to JSON file
    match fuzzer.get_schema() {
        Ok(schema) => {
            let json = serde_json::to_string_pretty(&schema)?;
            let full_path = fuzzer.out_dir.join("schema.json");
            std::fs::write(full_path.clone(), &json)?;
            tracing::info!("Wrote schema to {}", full_path.display());
        }
        Err(e) => {
            tracing::warn!("Failed to get schema for JSON dump: {e}");
        }
    }

    let stats = stats?;

    if stats.oracle_failures > 0 {
        std::process::exit(1);
    }

    Ok(())
}
