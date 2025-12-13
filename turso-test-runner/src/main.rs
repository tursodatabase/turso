use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "turso-test-runner")]
#[command(about = "SQL test runner for Turso/Limbo")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run tests
    Run {
        /// Test files or directories
        #[arg(required = true)]
        paths: Vec<PathBuf>,

        /// Path to tursodb binary
        #[arg(long, default_value = "tursodb")]
        binary: PathBuf,

        /// Filter tests by name pattern
        #[arg(short, long)]
        filter: Option<String>,

        /// Number of parallel jobs
        #[arg(short, long, default_value = "4")]
        jobs: usize,

        /// Output format (pretty, json)
        #[arg(short, long, default_value = "pretty")]
        output: String,
    },

    /// Validate test file syntax
    Check {
        /// Test files or directories
        paths: Vec<PathBuf>,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            paths,
            binary,
            filter,
            jobs,
            output,
        } => {
            println!("Running tests from {:?}", paths);
            println!("  Binary: {:?}", binary);
            println!("  Filter: {:?}", filter);
            println!("  Jobs: {}", jobs);
            println!("  Output: {}", output);
            // TODO: Implement test runner
        }
        Commands::Check { paths } => {
            println!("Checking test files: {:?}", paths);

            for path in paths {
                match std::fs::read_to_string(&path) {
                    Ok(content) => match turso_test_runner::parse(&content) {
                        Ok(file) => {
                            println!(
                                "  {} - OK ({} databases, {} setups, {} tests)",
                                path.display(),
                                file.databases.len(),
                                file.setups.len(),
                                file.tests.len()
                            );
                        }
                        Err(e) => {
                            eprintln!("  {} - ERROR: {}", path.display(), e);
                        }
                    },
                    Err(e) => {
                        eprintln!("  {} - ERROR: {}", path.display(), e);
                    }
                }
            }
        }
    }
}
