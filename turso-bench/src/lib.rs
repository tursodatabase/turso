//! TursoDB Benchmark Tool
//!
//! A benchmarking tool for TursoDB that's compatible with mobibench functionality
//! but uses TursoDB's core database engine instead of SQLite.

pub mod bench;
pub mod config;
pub mod database;
pub mod metrics;
pub mod utils;

pub use bench::BenchmarkRunner;
pub use config::BenchConfig;
pub use database::{DatabaseManager, OperationMode};
pub use metrics::BenchmarkMetrics;

/// Result type for benchmark operations
pub type Result<T> = anyhow::Result<T>;