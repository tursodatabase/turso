//! Operations that can be executed on the database.

use rand_chacha::ChaCha8Rng;
use std::sync::Arc;
use turso_core::{Connection, LimboError, Statement, Value};

use crate::{SamplesContainer, SimulatorFiber, SimulatorState, Stats};

/// Maximum number of keys to remember per table
const MAX_SAMPLE_KEYS_PER_TABLE: usize = 1000;

/// State of a simulator fiber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiberState {
    Idle,
    InTx,
}

/// An operation that can be executed on the database.
/// Operations are produced by workloads and contain all data needed for execution.
#[derive(Debug, Clone)]
pub enum Operation {
    /// Begin a transaction
    Begin { mode: String },
    /// Commit current transaction
    Commit,
    /// Rollback current transaction
    Rollback,
    /// Run PRAGMA integrity_check
    IntegrityCheck,
    /// Run WAL checkpoint with specified mode
    WalCheckpoint { mode: String },
    /// Create a simple key-value table
    CreateSimpleTable { table_name: String },
    /// Select from a simple table by key
    SimpleSelect { table_name: String, key: String },
    /// Insert into a simple table
    SimpleInsert {
        table_name: String,
        key: String,
        value_hex: String,
    },
    /// Generic SELECT query
    Select { sql: String },
    /// Generic INSERT query
    Insert { sql: String },
    /// Generic UPDATE query
    Update { sql: String },
    /// Generic DELETE query
    Delete { sql: String },
    /// Create an index
    CreateIndex {
        sql: String,
        index_name: String,
        table_name: String,
    },
    /// Drop an index
    DropIndex { sql: String, index_name: String },
}

/// Context passed to Operation::start_op and Operation::finish_op.
pub struct OpContext<'a> {
    pub fiber: &'a mut SimulatorFiber,
    pub sim_state: &'a mut SimulatorState,
    pub stats: &'a mut Stats,
    pub rng: &'a mut ChaCha8Rng,
}

impl Operation {
    /// Get the SQL string for this operation
    pub fn sql(&self) -> String {
        match self {
            Operation::Begin { mode } => mode.clone(),
            Operation::Commit => "COMMIT".to_string(),
            Operation::Rollback => "ROLLBACK".to_string(),
            Operation::IntegrityCheck => "PRAGMA integrity_check".to_string(),
            Operation::WalCheckpoint { mode } => format!("PRAGMA wal_checkpoint({mode})"),
            Operation::CreateSimpleTable { table_name } => {
                format!(
                    "CREATE TABLE IF NOT EXISTS {table_name} (key TEXT PRIMARY KEY, value BLOB)"
                )
            }
            Operation::SimpleSelect { table_name, key } => {
                format!("SELECT value FROM {table_name} WHERE key = '{key}'")
            }
            Operation::SimpleInsert {
                table_name,
                key,
                value_hex,
            } => {
                format!(
                    "INSERT OR REPLACE INTO {table_name} (key, value) VALUES ('{key}', X'{value_hex}')"
                )
            }
            Operation::Select { sql } => sql.clone(),
            Operation::Insert { sql } => sql.clone(),
            Operation::Update { sql } => sql.clone(),
            Operation::Delete { sql } => sql.clone(),
            Operation::CreateIndex { sql, .. } => sql.clone(),
            Operation::DropIndex { sql, .. } => sql.clone(),
        }
    }

    /// Prepare this operation on a connection.
    /// Returns Ok(Statement) on success, or an error.
    pub fn init_op(&self, ctx: &mut OpContext) -> Result<(), turso_core::LimboError> {
        let stmt = ctx.fiber.connection.prepare(self.sql())?;
        ctx.fiber.statement.replace(Some(stmt));
        Ok(())
    }

    /// Called when an operation finishes execution.
    /// Applies state changes based on operation type and result.
    pub fn finish_op(&self, ctx: &mut OpContext, result: &OpResult) {
        // Only apply state changes on success
        if let OpResult::Error { .. } = result {
            return;
        }

        match self {
            Operation::Begin { .. } => {
                ctx.fiber.state = FiberState::InTx;
                ctx.fiber.txn_id = Some(ctx.sim_state.gen_txn_id());
            }
            Operation::Commit | Operation::Rollback => {
                ctx.fiber.state = FiberState::Idle;
                ctx.fiber.txn_id = None;
            }
            Operation::CreateSimpleTable { table_name } => {
                ctx.sim_state.simple_tables.insert(table_name.clone(), ());
            }
            Operation::SimpleInsert {
                table_name, key, ..
            } => {
                let table_name = table_name.clone();
                let keys = &mut ctx.sim_state.simple_tables_keys;
                let container = keys
                    .entry(table_name)
                    .or_insert_with(|| SamplesContainer::new(MAX_SAMPLE_KEYS_PER_TABLE));
                container.add(key.clone(), ctx.rng);
            }
            Operation::CreateIndex {
                index_name,
                table_name,
                ..
            } => {
                let index_name = index_name.clone();
                let table_name = table_name.clone();
                ctx.sim_state.indexes.insert(index_name, table_name);
            }
            Operation::DropIndex { index_name, .. } => {
                ctx.sim_state.indexes.remove(index_name);
            }
            _ => {}
        }
    }
}

/// Result of an operation execution
#[derive(Debug, Clone)]
pub enum OpResult {
    /// Operation completed successfully with fetched rows
    Success { rows: Vec<Vec<Value>> },
    /// Operation failed with an error
    Error { error: LimboError },
}
