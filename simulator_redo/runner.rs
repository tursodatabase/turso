//! Main simulation runner.
//!
//! This module orchestrates the simulation by:
//! 1. Creating both Turso and SQLite databases
//! 2. Generating and executing CREATE TABLE statements
//! 3. Generating statements using sql_gen_prop
//! 4. Executing them on both databases
//! 5. Checking the differential oracle

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use turso_core::Database;

use crate::oracle::{OracleResult, check_differential};
use crate::schema::SchemaIntrospector;

/// Configuration for the simulator.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Random seed for deterministic execution.
    pub seed: u64,
    /// Number of tables to create.
    pub num_tables: usize,
    /// Number of columns per table.
    pub columns_per_table: usize,
    /// Number of statements to generate and execute.
    pub num_statements: usize,
    /// Whether to print verbose output.
    pub verbose: bool,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: rand::rng().next_u64(),
            num_tables: 2,
            columns_per_table: 5,
            num_statements: 100,
            verbose: false,
        }
    }
}

/// Statistics from a simulation run.
#[derive(Debug, Default)]
pub struct SimStats {
    /// Number of statements executed.
    pub statements_executed: usize,
    /// Number of oracle failures.
    pub oracle_failures: usize,
    /// Number of errors encountered.
    pub errors: usize,
}

/// The main simulator.
pub struct Simulator {
    config: SimConfig,
    rng: ChaCha8Rng,
    turso_conn: Arc<turso_core::Connection>,
    sqlite_conn: rusqlite::Connection,
    #[expect(dead_code)]
    turso_db: Arc<Database>,
}

impl Simulator {
    /// Create a new simulator with in-memory databases.
    ///
    /// Uses `turso_core::MemoryIO` for deterministic in-memory storage.
    pub fn new(config: SimConfig) -> Result<Self> {
        let rng = ChaCha8Rng::seed_from_u64(config.seed);

        // Create Turso in-memory database using MemoryIO
        let io = Arc::new(turso_core::MemoryIO::new());
        let turso_db = Database::open_file(io, ":memory:")?;
        let turso_conn = turso_db.connect()?;

        // Create SQLite in-memory database
        let sqlite_conn = rusqlite::Connection::open_in_memory()
            .context("Failed to open SQLite in-memory database")?;

        Ok(Self {
            config,
            rng,
            turso_conn,
            sqlite_conn,
            turso_db,
        })
    }

    /// Run the simulation.
    pub fn run(&mut self) -> Result<SimStats> {
        let mut stats = SimStats::default();

        tracing::info!(
            "Starting simulation with seed={}, tables={}, statements={}",
            self.config.seed,
            self.config.num_tables,
            self.config.num_statements
        );

        // Step 1: Generate and execute CREATE TABLE statements
        self.create_tables()?;

        // Step 2: Introspect the schema
        let schema = SchemaIntrospector::from_turso(&self.turso_conn)
            .context("Failed to introspect Turso schema")?;

        if schema.tables.is_empty() {
            bail!("No tables found after CREATE TABLE");
        }

        tracing::info!("Schema introspected: {} tables", schema.tables.len());

        // Step 3: Generate and execute statements
        // Create a deterministic seed for proptest
        let seed_bytes: [u8; 32] = {
            let mut bytes = [0u8; 32];
            self.rng.fill_bytes(&mut bytes);
            bytes
        };

        let mut test_runner = TestRunner::new_with_rng(
            proptest::test_runner::Config::default(),
            proptest::test_runner::TestRng::from_seed(
                proptest::test_runner::RngAlgorithm::ChaCha,
                &seed_bytes,
            ),
        );

        for i in 0..self.config.num_statements {
            // Generate a statement
            let strategy = sql_gen_prop::strategies::statement_for_schema(&schema);
            let value_tree = strategy
                .new_tree(&mut test_runner)
                .map_err(|e| anyhow::anyhow!("Failed to generate statement: {}", e))?;

            let stmt = value_tree.current();
            let sql = stmt.to_string();

            if self.config.verbose {
                tracing::info!("Statement {}: {}", i, sql);
            }

            // Execute on both databases and check oracle
            match check_differential(&self.turso_conn, &self.sqlite_conn, &sql) {
                Ok(OracleResult::Pass) => {
                    stats.statements_executed += 1;
                }
                Ok(OracleResult::Fail(reason)) => {
                    stats.oracle_failures += 1;
                    tracing::error!("Oracle failure at statement {}: {}", i, reason);
                    if !self.config.verbose {
                        // Print the failing statement
                        tracing::error!("Failing SQL: {}", sql);
                    }
                    return Err(anyhow::anyhow!("Oracle failure: {}", reason));
                }
                Err(e) => {
                    stats.errors += 1;
                    tracing::warn!("Error executing statement {}: {}", i, e);
                }
            }
        }

        tracing::info!(
            "Simulation complete: {} statements executed, {} failures, {} errors",
            stats.statements_executed,
            stats.oracle_failures,
            stats.errors
        );

        Ok(stats)
    }

    /// Create tables in both databases.
    fn create_tables(&mut self) -> Result<()> {
        for table_idx in 0..self.config.num_tables {
            let table_name = format!("t{table_idx}");
            let create_sql = self.generate_create_table(&table_name);

            tracing::debug!("Creating table: {create_sql}");

            // Execute on Turso
            self.turso_conn
                .execute(&create_sql)
                .context(format!("Failed to create table {table_name} in Turso"))?;

            // Execute on SQLite
            self.sqlite_conn
                .execute(&create_sql, [])
                .context(format!("Failed to create table {table_name} in SQLite"))?;
        }

        Ok(())
    }

    /// Generate a CREATE TABLE statement.
    fn generate_create_table(&mut self, table_name: &str) -> String {
        use rand::Rng;

        let mut columns = Vec::new();

        // Always add an integer primary key
        columns.push("\"id\" INTEGER PRIMARY KEY".to_string());

        // Add additional columns
        for col_idx in 1..self.config.columns_per_table {
            let col_name = format!("c{col_idx}");
            let col_type = match self.rng.random_range(0..4) {
                0 => "INTEGER",
                1 => "REAL",
                2 => "TEXT",
                _ => "BLOB",
            };
            let nullable = if self.rng.random_bool(0.5) {
                ""
            } else {
                " NOT NULL"
            };
            columns.push(format!("\"{col_name}\" {col_type}{nullable}"));
        }

        format!("CREATE TABLE \"{table_name}\" ({})", columns.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sim_config_default() {
        let config = SimConfig::default();
        assert_eq!(config.seed, 42);
        assert_eq!(config.num_tables, 2);
        assert_eq!(config.num_statements, 100);
    }

    #[test]
    fn test_simulator_creation() {
        let config = SimConfig {
            seed: 12345,
            num_tables: 1,
            columns_per_table: 3,
            num_statements: 10,
            verbose: false,
        };
        let sim = Simulator::new(config);
        assert!(sim.is_ok());
    }
}
