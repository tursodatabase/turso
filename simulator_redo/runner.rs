//! Main simulation runner.
//!
//! This module orchestrates the simulation by:
//! 1. Creating both Turso and SQLite databases
//! 2. Generating and executing CREATE TABLE statements
//! 3. Generating statements (DML and DDL) using sql_gen_prop
//! 4. Executing them on both databases
//! 5. Checking the differential oracle
//! 6. Re-introspecting schemas after DDL statements

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sql_gen_prop::{Schema, SqlStatement, StatementKind};
use turso_core::Database;

use crate::memory::{MemorySimIO, SimIO};
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
    /// In-memory IO for the Turso database.
    io: Arc<MemorySimIO>,
}

impl Simulator {
    /// Create a new simulator with in-memory databases.
    ///
    /// Uses `MemorySimIO` for deterministic in-memory storage.
    pub fn new(config: SimConfig) -> Result<Self> {
        let rng = ChaCha8Rng::seed_from_u64(config.seed);

        // Create Turso in-memory database using MemorySimIO
        let io = Arc::new(MemorySimIO::new(config.seed));
        let turso_db = Database::open_file(io.clone(), "test.db")?;
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
            io,
        })
    }

    /// Persist the in-memory database files to disk.
    ///
    /// Writes `.db`, `.wal`, and `.lg` files to the filesystem.
    pub fn persist_files(&self) -> Result<()> {
        self.io.persist_files()?;
        Ok(())
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

        let mut schema = self.introspect_and_verify_schemas()?;

        for i in 0..self.config.num_statements {
            // Generate a statement (DML or DDL)
            let strategy = sql_gen_prop::strategies::statement_for_schema(&schema, None);
            let value_tree = strategy
                .new_tree(&mut test_runner)
                .map_err(|e| anyhow::anyhow!("Failed to generate statement: {e}"))?;

            let stmt = value_tree.current();
            let sql = stmt.to_string();
            let is_ddl = Self::is_ddl_statement(&stmt);

            if self.config.verbose {
                let stmt_type = if is_ddl { "DDL" } else { "DML" };
                tracing::info!("Statement {} [{}]: {}", i, stmt_type, sql);
            }

            // Execute on both databases and check oracle
            match check_differential(&self.turso_conn, &self.sqlite_conn, &sql) {
                Ok(OracleResult::Pass) => {
                    stats.statements_executed += 1;

                    // After DDL statements, re-introspect both databases and verify schemas match
                    if is_ddl {
                        schema = self.introspect_and_verify_schemas().map_err(|e| {
                            anyhow::anyhow!("Schema mismatch after DDL statement {i} ({sql}): {e}")
                        })?;
                        tracing::debug!(
                            "Schema updated after DDL: {} tables, {} indexes",
                            schema.tables.len(),
                            schema.indexes.len()
                        );
                    }
                }
                Ok(OracleResult::Fail(reason)) => {
                    stats.oracle_failures += 1;
                    tracing::error!("Oracle failure at statement {i}: {reason}");
                    if !self.config.verbose {
                        tracing::error!("Failing SQL: {sql}");
                    }
                    return Err(anyhow::anyhow!("Oracle failure: {reason}"));
                }
                Err(e) => {
                    stats.errors += 1;
                    tracing::warn!("Error executing statement {i}: {e}");
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

    /// Check if a statement is a DDL statement (modifies schema).
    fn is_ddl_statement(stmt: &SqlStatement) -> bool {
        StatementKind::from(stmt).is_ddl()
    }

    /// Introspect schemas from both databases and verify they match.
    fn introspect_and_verify_schemas(&self) -> Result<Schema> {
        let turso_schema = SchemaIntrospector::from_turso(&self.turso_conn)
            .context("Failed to introspect Turso schema")?;
        let sqlite_schema = SchemaIntrospector::from_sqlite(&self.sqlite_conn)
            .context("Failed to introspect SQLite schema")?;

        // Verify table names match
        let turso_tables: std::collections::HashSet<_> =
            turso_schema.tables.iter().map(|t| &t.name).collect();
        let sqlite_tables: std::collections::HashSet<_> =
            sqlite_schema.tables.iter().map(|t| &t.name).collect();

        if turso_tables != sqlite_tables {
            bail!(
                "Table mismatch: Turso has {:?}, SQLite has {:?}",
                turso_tables,
                sqlite_tables
            );
        }

        // Verify each table's columns match
        for turso_table in turso_schema.tables.iter() {
            let sqlite_table = sqlite_schema
                .tables
                .iter()
                .find(|t| t.name == turso_table.name)
                .expect("Table should exist in SQLite schema");

            let turso_cols: Vec<_> = turso_table.columns.iter().map(|c| &c.name).collect();
            let sqlite_cols: Vec<_> = sqlite_table.columns.iter().map(|c| &c.name).collect();

            if turso_cols != sqlite_cols {
                bail!(
                    "Column mismatch in table '{}': Turso has {:?}, SQLite has {:?}",
                    turso_table.name,
                    turso_cols,
                    sqlite_cols
                );
            }
        }

        Ok(turso_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sim_config_default() {
        let config = SimConfig::default();
        // seed is now randomly generated by default
        assert!(config.seed > 0);
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
