use std::path::PathBuf;

use clap::Parser;

pub mod runner;
mod testing;

/// Test Runner for Limbo DSL
#[derive(Parser, Debug)]
#[command(name = "dsl_runner")]
#[command(version, about, long_about)]
pub struct Args {
    /// File path to run test
    #[arg(short, long)]
    pub path: Option<PathBuf>,

    /// Default Databases to run tests
    #[arg(short, long = "dbs", value_delimiter = ',', num_args = 1.., required = true)]
    pub databases: Vec<PathBuf>,
}
