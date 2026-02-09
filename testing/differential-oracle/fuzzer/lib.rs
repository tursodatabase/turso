//! This fuzzer uses schema introspection (rather than shadow state) to detect
//! the current database schema and generates statements based on it. It uses a
//! differential oracle to compare Turso results with SQLite.

pub mod generate;
pub mod memory;
pub mod oracle;
pub mod printf_gen;
pub mod runner;
pub mod schema;

pub use generate::{GeneratedStatement, GeneratorKind, SqlGenerator};
pub use memory::{MemorySimFile, MemorySimIO, SimIO};
pub use oracle::{DifferentialOracle, Oracle, OracleResult, check_differential};
pub use runner::{Fuzzer, SimConfig, SimStats, TreeMode};
pub use schema::SchemaIntrospector;
