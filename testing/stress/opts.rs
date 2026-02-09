use std::fmt;
use std::path::PathBuf;

use clap::{command, Parser};

/// Transaction mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum TxMode {
    /// SQLite transaction mode with single-writer, multiple-reader semantics.
    SQLite,
    /// Concurrent transaction mode with multiple-writer, multiple-reader semantics.
    Concurrent,
}

impl fmt::Display for TxMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TxMode::SQLite => write!(f, "sqlite"),
            TxMode::Concurrent => write!(f, "concurrent"),
        }
    }
}

#[derive(Parser, Clone)]
#[command(name = "turso_stress")]
#[command(author, version, about, long_about = None)]
pub struct Opts {
    /// Transaction mode
    #[clap(long, help = "transaction mode", default_value_t = TxMode::SQLite)]
    pub tx_mode: TxMode,

    /// Number of threads to run
    #[clap(short = 't', long, help = "the number of threads", default_value_t = 1)]
    pub nr_threads: usize,

    /// Number of iterations per thread
    #[clap(
        short = 'i',
        long,
        help = "the number of iterations",
        default_value_t = normal_or_constrained(100_000, 1000)
    )]
    pub nr_iterations: usize,

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

    #[clap(
        long,
        help = "If true, this will run a modified minimal version of turso_stress that ensure the DB is deterministic (no sources of randomness that are not controlled by the seed)"
    )]
    pub check_uncontrolled_nondeterminism: bool,
}

/// Returns a constrained value when running under miri or shuttle,
/// since these tools have much higher overhead and explore execution states.
const fn normal_or_constrained<T: Copy>(normal_val: T, constrained_val: T) -> T {
    if cfg!(miri) || cfg!(shuttle) {
        constrained_val
    } else {
        normal_val
    }
}
