//! SQLancer-style simulator for Turso.
//!
//! This binary runs a differential testing simulator that compares Turso
//! results against SQLite for generated SQL statements.

use anyhow::Result;
use clap::Parser;
use rand::RngCore;
use sim_redo::{SimConfig, Simulator};

/// SQLancer-style differential testing simulator for Turso.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Random seed for deterministic execution.
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

    let args = Args::parse();

    let config = SimConfig {
        seed: args.seed,
        num_tables: args.num_tables,
        columns_per_table: args.columns_per_table,
        num_statements: args.num_statements,
        verbose: args.verbose,
    };

    tracing::info!("Starting sim_redo with config: {:?}", config);

    let mut simulator = Simulator::new(config)?;
    let stats = simulator.run()?;

    tracing::info!("Final stats: {:?}", stats);

    if args.keep_files {
        tracing::info!("Persisting database files to disk...");
        simulator.persist_files()?;
    }

    if stats.oracle_failures > 0 {
        std::process::exit(1);
    }

    Ok(())
}
