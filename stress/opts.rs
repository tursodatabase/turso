use std::path::PathBuf;

use clap::{command, Parser};

#[derive(Parser)]
#[command(name = "turso_stress")]
#[command(author, version, about, long_about = None)]
pub struct Opts {
    /// Verbose mode
    #[clap(short = 'v', long, help = "verbose mode")]
    pub verbose: bool,

    /// Silent mode
    #[clap(long, help = "silent mode")]
    pub silent: bool,

    /// Number of threads to run
    #[clap(short = 't', long, help = "the number of threads", default_value_t = 1)]
    pub nr_threads: usize,

    /// Number of iterations per thread
    #[clap(
        short = 'i',
        long,
        help = "the number of iterations",
        default_value_t = normal_or_miri(100_000, 1000)
    )]
    pub nr_iterations: usize,

    /// Log file for SQL statements
    #[clap(
        short = 'l',
        long,
        help = "log file for SQL statements",
        default_value = "limbostress.log"
    )]
    pub log_file: String,

    /// Load log file instead of creating a new one
    #[clap(
        short = 'L',
        long = "load-log",
        help = "load log file instead of creating a new one",
        default_value_t = false
    )]
    pub load_log: bool,

    /// Skip writing to log file
    #[clap(
        short = 's',
        long = "skip-log",
        help = "load log file instead of creating a new one",
        default_value_t = false
    )]
    pub skip_log: bool,

    /// Database file
    #[clap(short = 'd', long, help = "database file")]
    pub db_file: Option<String>,

    /// Select VFS
    #[clap(
        long,
        help = "Select VFS. options are io_uring (if feature enabled), memory, and syscall"
    )]
    pub vfs: Option<String>,

    /// Number of tables to use
    #[clap(long, help = "Select number of tables to create")]
    pub tables: Option<usize>,

    /// Busy timeout in milliseconds
    #[clap(
        long,
        help = "Set busy timeout in milliseconds",
        default_value_t = 5000
    )]
    pub busy_timeout: u64,

    /// Random seed for reproducibility
    #[clap(long, help = "Random seed for reproducibility")]
    pub seed: Option<u64>,

    #[clap(long, help = "Reference DB to take schema and initial state")]
    pub db_ref: Option<PathBuf>,
}

const fn normal_or_miri<T: Copy>(normal_val: T, miri_val: T) -> T {
    if cfg!(miri) {
        miri_val
    } else {
        normal_val
    }
}
