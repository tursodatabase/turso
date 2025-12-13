pub mod cli;

use crate::parser::ast::DatabaseConfig;
use async_trait::async_trait;
use std::time::Duration;

/// Result from executing SQL
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Rows returned, each row is a vector of string-formatted columns
    pub rows: Vec<Vec<String>>,
    /// Error message if the query failed
    pub error: Option<String>,
}

impl QueryResult {
    /// Create a successful result with rows
    pub fn success(rows: Vec<Vec<String>>) -> Self {
        Self { rows, error: None }
    }

    /// Create an error result
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            rows: Vec::new(),
            error: Some(message.into()),
        }
    }

    /// Check if this result is an error
    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }
}

/// Backend trait for executing SQL against a target
#[async_trait]
pub trait SqlBackend: Send + Sync {
    /// Name of this backend (for filtering and display)
    fn name(&self) -> &str;

    /// Create a new isolated database instance
    async fn create_database(
        &self,
        config: &DatabaseConfig,
    ) -> Result<Box<dyn DatabaseInstance>, BackendError>;
}

/// An isolated database instance
#[async_trait]
pub trait DatabaseInstance: Send + Sync {
    /// Execute SQL and return results
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError>;

    /// Close and cleanup the database
    async fn close(self: Box<Self>) -> Result<(), BackendError>;
}

/// Errors that can occur in backends
#[derive(Debug, Clone, thiserror::Error)]
pub enum BackendError {
    #[error("failed to create database: {0}")]
    CreateDatabase(String),

    #[error("failed to execute SQL: {0}")]
    Execute(String),

    #[error("failed to close database: {0}")]
    Close(String),

    #[error("backend not available: {0}")]
    NotAvailable(String),

    #[error("timeout after {0:?}")]
    Timeout(Duration),
}
