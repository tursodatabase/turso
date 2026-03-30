//! Diferential Fuzzer for Turso.
//!
//! This binary runs a differential testing fuzzer that compares Turso
//! results against SQLite for generated SQL statements.

use std::io::{IsTerminal, stdin};
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use differential_fuzzer::{Fuzzer, GeneratorKind, SimConfig, TreeMode};
use rand::RngCore;
use serde::Serialize;

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

    /// Write a coverage report to simulator-output/coverage.txt.
    #[arg(long)]
    coverage: bool,

    /// Use full hierarchical tree in the coverage report instead of simplified flat view.
    #[arg(long)]
    full_tree: bool,

    /// Enable experimental MVCC mode.
    #[arg(long)]
    mvcc: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the fuzzer in a loop with random seeds.
    Loop {
        /// Number of iterations to run (0 for infinite).
        #[arg(default_value_t = 0)]
        iterations: u64,

        /// Collect errors and write a JSON report to this path instead of stopping on first failure.
        #[arg(long)]
        report: Option<PathBuf>,
    },
}

/// A single failure recorded during a loop run.
#[derive(Debug, Serialize)]
struct FailureRecord {
    iteration: u64,
    seed: u64,
    error: String,
    statements_executed: usize,
    oracle_failures: usize,
    warnings: usize,
    config: ConfigRecord,
}

/// Serializable snapshot of the run configuration.
#[derive(Debug, Serialize)]
struct ConfigRecord {
    num_tables: usize,
    columns_per_table: usize,
    num_statements: usize,
    generator: String,
    mvcc: bool,
}

/// Summary written to the JSON report file.
#[derive(Debug, Serialize)]
struct LoopReport {
    total_iterations: u64,
    total_failures: u64,
    failures: Vec<FailureRecord>,
}

impl ConfigRecord {
    fn from_args(args: &Args) -> Self {
        Self {
            num_tables: args.num_tables,
            columns_per_table: args.columns_per_table,
            num_statements: args.num_statements,
            generator: format!("{:?}", args.generator),
            mvcc: args.mvcc,
        }
    }
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
        Some(Commands::Loop {
            iterations,
            ref report,
        }) => {
            let mut iteration = 0u64;
            let mut failures: Vec<FailureRecord> = Vec::new();
            let collecting = report.is_some();

            loop {
                args.seed = rand::rng().next_u64();
                tracing::info!("Iteration {}: seed {}", iteration + 1, args.seed);

                match run_single_inner(&args) {
                    Ok(stats) if stats.oracle_failures > 0 => {
                        let record = FailureRecord {
                            iteration: iteration + 1,
                            seed: args.seed,
                            error: format!("{} oracle failure(s) detected", stats.oracle_failures),
                            statements_executed: stats.statements_executed,
                            oracle_failures: stats.oracle_failures,
                            warnings: stats.warnings,
                            config: ConfigRecord::from_args(&args),
                        };
                        tracing::error!(
                            "Iteration {} failed (seed {}): {}",
                            iteration + 1,
                            args.seed,
                            record.error
                        );
                        failures.push(record);
                        if !collecting {
                            break;
                        }
                    }
                    Err(e) => {
                        let record = FailureRecord {
                            iteration: iteration + 1,
                            seed: args.seed,
                            error: format!("{e:#}"),
                            statements_executed: 0,
                            oracle_failures: 0,
                            warnings: 0,
                            config: ConfigRecord::from_args(&args),
                        };
                        tracing::error!(
                            "Iteration {} errored (seed {}): {e:#}",
                            iteration + 1,
                            args.seed
                        );
                        failures.push(record);
                        if !collecting {
                            break;
                        }
                    }
                    Ok(_) => {}
                }

                iteration += 1;
                if iterations > 0 && iteration >= iterations {
                    tracing::info!("Completed {} iterations", iterations);
                    break;
                }
            }

            if let Some(path) = report {
                let report = LoopReport {
                    total_iterations: iteration,
                    total_failures: failures.len() as u64,
                    failures,
                };
                let json = serde_json::to_string_pretty(&report)?;
                std::fs::write(path, &json)?;
                tracing::info!(
                    "Wrote report ({} failures / {} iterations) to {}",
                    report.total_failures,
                    report.total_iterations,
                    path.display()
                );
                if report.total_failures > 0 {
                    std::process::exit(1);
                }
            } else if !failures.is_empty() {
                std::process::exit(1);
            }

            Ok(())
        }
        None => run_single(&args),
    }
}

/// Run a single fuzzer iteration, returning stats on success.
/// Does NOT call `process::exit` — the caller decides what to do with failures.
fn run_single_inner(args: &Args) -> Result<differential_fuzzer::SimStats> {
    let config = SimConfig {
        seed: args.seed,
        num_tables: args.num_tables,
        columns_per_table: args.columns_per_table,
        num_statements: args.num_statements,
        verbose: args.verbose,
        keep_files: args.keep_files,
        generator: args.generator,
        coverage: args.coverage,
        tree_mode: if args.full_tree {
            TreeMode::Full
        } else {
            TreeMode::Simplified
        },
        mvcc: args.mvcc,
    };

    tracing::info!("Starting differential_fuzzer with config: {:?}", config);

    let fuzzer = Fuzzer::new(config)?;
    let stats = fuzzer.run()?;

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

    Ok(stats)
}

fn run_single(args: &Args) -> Result<()> {
    let stats = run_single_inner(args)?;
    if stats.oracle_failures > 0 {
        std::process::exit(1);
    }
    Ok(())
}
