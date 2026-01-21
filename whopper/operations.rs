//! Operations that can be executed on the database.

use std::sync::Arc;
use turso_core::{Connection, Statement, Value};

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
    DropIndex {
        sql: String,
        index_name: String,
    },
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
    pub fn prepare(
        &self,
        connection: &Arc<Connection>,
    ) -> Result<Statement, turso_core::LimboError> {
        connection.prepare(self.sql())
    }
}

/// Result of an operation execution
#[derive(Debug, Clone)]
pub enum OpResult {
    /// Operation completed successfully with fetched rows
    Success { rows: Vec<Vec<Value>> },
    /// Operation failed with an error
    Error { error: String },
}
