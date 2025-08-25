use clap::Parser;
use std::path::PathBuf;

/// Configuration for the TursoDB benchmark tool
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct BenchConfig {
    /// Database path
    #[arg(short = 'p', long, default_value = "./turso-bench")]
    pub path: String,

    /// File size per thread in KB
    #[arg(short = 'f', long, default_value = "1024")]
    pub file_size_kb: u64,

    /// Record size in KB  
    #[arg(short = 'r', long, default_value = "4")]
    pub record_size_kb: u64,

    /// Access mode: 0=Insert, 1=Update, 2=Delete
    #[arg(short = 'a', long, default_value = "0")]
    pub access_mode: u8,

    /// Number of threads
    #[arg(short = 't', long, default_value = "1")]
    pub num_threads: u32,

    /// Number of database transactions per thread
    #[arg(short = 'n', long, default_value = "10")]
    pub transactions: u32,

    /// Number of tables to create
    #[arg(short = 'T', long, default_value = "3")]
    pub num_tables: u32,

    /// Number of databases
    #[arg(short = 'D', long, default_value = "1")]
    pub num_databases: u32,

    /// Time interval between transactions in milliseconds
    #[arg(short = 'i', long, default_value = "0")]
    pub interval_ms: u64,

    /// Enable quiet mode (no progress output)
    #[arg(short = 'q', long)]
    pub quiet: bool,

    /// Enable latency measurement
    #[arg(short = 'L', long)]
    pub latency_file: Option<String>,

    /// Enable IOPS measurement
    #[arg(short = 'k', long)]
    pub iops_file: Option<String>,

    /// Overlap ratio for random operations (0-100%)
    #[arg(short = 'v', long, default_value = "0")]
    pub overlap_ratio: u8,

    /// Use random insert order
    #[arg(short = 'R', long)]
    pub random_insert: bool,

    /// Enable MVCC (Multi-Version Concurrency Control)
    #[arg(long)]
    pub enable_mvcc: bool,

    /// Enable indexes
    #[arg(long, default_value = "true")]
    pub enable_indexes: bool,

    /// Enable views
    #[arg(long)]
    pub enable_views: bool,
}

impl BenchConfig {
    /// Validate the configuration
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.access_mode > 2 {
            anyhow::bail!("Access mode must be 0 (insert), 1 (update), or 2 (delete)");
        }

        if self.num_threads == 0 {
            anyhow::bail!("Number of threads must be greater than 0");
        }

        if self.transactions == 0 {
            anyhow::bail!("Number of transactions must be greater than 0");
        }

        if self.overlap_ratio > 100 {
            anyhow::bail!("Overlap ratio must be between 0 and 100");
        }

        if self.num_tables == 0 || self.num_tables > 20 {
            anyhow::bail!("Number of tables must be between 1 and 20");
        }

        if self.num_databases == 0 || self.num_databases > 20 {
            anyhow::bail!("Number of databases must be between 1 and 20");
        }

        Ok(())
    }

    /// Get the database path for a specific thread and database index
    pub fn get_db_path(&self, thread_id: u32, db_index: u32) -> String {
        if self.path == ":memory:" {
            format!(":memory:-{}-{}", thread_id, db_index)
        } else {
            format!("{}/test.db{}_{}", self.path, thread_id, db_index)
        }
    }

    /// Get table name for a specific table index
    pub fn get_table_name(&self, table_index: u32) -> String {
        format!("tblMyList{}", table_index)
    }
}