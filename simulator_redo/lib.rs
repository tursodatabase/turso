//! Simplified SQLancer-style simulator for Turso.
//!
//! This simulator uses schema introspection (rather than shadow state) to detect
//! the current database schema and generates statements based on it. It uses a
//! differential oracle to compare Turso results with SQLite.

pub mod memory;
pub mod oracle;
pub mod runner;
pub mod schema;

pub use memory::{MemorySimFile, MemorySimIO, SimIO};
pub use oracle::{DifferentialOracle, Oracle, OracleResult, check_differential};
pub use runner::{SimConfig, SimStats, Simulator};
pub use schema::SchemaIntrospector;
