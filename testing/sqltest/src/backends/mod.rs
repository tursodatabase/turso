pub mod cli;
pub mod js;
pub mod pg_connection;
pub mod postgres;
pub mod tursopg;

use crate::parser::ast::{Backend, Capability, DatabaseConfig, DatabaseLocation};
use async_trait::async_trait;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::NamedTempFile;

/// Provides resolved paths for default databases
pub trait DefaultDatabaseResolver: Send + Sync {
    /// Resolve a database location to an actual path
    /// Returns Some(path) for Default/DefaultNoRowidAlias, None otherwise
    fn resolve(&self, location: &DatabaseLocation) -> Option<PathBuf>;
}

/// Marker used to separate setup output from query output
/// This marker is inserted between setup SQL and query SQL, then filtered out
pub const SETUP_END_MARKER: &str = "__SETUP_END_MARKER_7f3a9b2c__";

/// SQL to output the setup end marker
pub const SETUP_END_MARKER_SQL: &str = "SELECT '__SETUP_END_MARKER_7f3a9b2c__';";

/// Result from executing SQL
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Rows returned, each row is a vector of string-formatted columns
    pub rows: Vec<Vec<String>>,
    /// Error message if the query failed
    pub error: Option<String>,
    /// SQLSTATE of the error, from backends that speak the PostgreSQL wire protocol
    pub sqlstate: Option<String>,
}

impl QueryResult {
    /// Create a successful result with rows
    pub fn success(rows: Vec<Vec<String>>) -> Self {
        Self {
            rows,
            error: None,
            sqlstate: None,
        }
    }

    /// Create an error result
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            rows: Vec::new(),
            error: Some(message.into()),
            sqlstate: None,
        }
    }

    /// Create an error result with the SQLSTATE that the server sent.
    /// `None` means that the server did not send one.
    pub fn error_with_sqlstate(message: impl Into<String>, sqlstate: Option<String>) -> Self {
        Self {
            sqlstate,
            ..Self::error(message)
        }
    }

    /// Check if this result is an error
    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }

    /// Filter out setup output by removing all rows up to and including the marker row.
    /// If no marker is found, returns self unchanged (for backward compatibility).
    pub fn filter_setup_output(mut self) -> Self {
        if self.is_error() {
            return self;
        }

        // Find the marker row (single column containing the marker)
        if let Some(marker_idx) = self
            .rows
            .iter()
            .position(|row| row.len() == 1 && row[0] == SETUP_END_MARKER)
        {
            // Remove all rows up to and including the marker
            self.rows = self.rows.split_off(marker_idx + 1);
        }

        self
    }
}

/// Handle returned from closing a database that preserves the file on disk.
/// For temp file databases, the file is deleted when this handle is dropped.
/// For memory databases, the path is None.
pub struct DatabaseFileHandle {
    /// Path to the database file on disk, if any
    pub path: Option<PathBuf>,
    /// Keeps the temp file alive until this handle is dropped
    _temp_file: Option<NamedTempFile>,
}

impl DatabaseFileHandle {
    /// Create a handle for a temp file database
    pub fn temp(temp_file: NamedTempFile) -> Self {
        let path = Some(temp_file.path().to_path_buf());
        Self {
            path,
            _temp_file: Some(temp_file),
        }
    }

    /// Create a handle with no file (memory databases)
    pub fn none() -> Self {
        Self {
            path: None,
            _temp_file: None,
        }
    }
}

/// Backend trait for executing SQL against a target
#[async_trait]
pub trait SqlBackend: Send + Sync {
    /// Name of this backend (for filtering and display)
    fn name(&self) -> &str;

    /// Backend type enum variant
    fn backend_type(&self) -> Backend;

    /// Return the set of capabilities this backend supports
    fn capabilities(&self) -> HashSet<Capability>;

    /// Whether this backend is the sqlite CLI backend
    fn is_sqlite(&self) -> bool {
        false
    }

    /// Whether this backend supports snapshot tests (EXPLAIN output comparison).
    /// Returns false by default; only the Rust backend enables this.
    fn supports_snapshots(&self) -> bool {
        false
    }

    /// Whether the errors of this backend have a SQLSTATE. The runner rejects
    /// a test with `@sqlstate` on a backend whose errors have none.
    fn reports_sqlstate(&self) -> bool {
        false
    }

    /// Whether this backend can compare typed materialized-view results with
    /// their defining SELECT after each statement.
    fn supports_materialized_view_verification(&self) -> bool {
        false
    }

    /// Create a new isolated database instance
    async fn create_database(
        &self,
        config: &DatabaseConfig,
    ) -> Result<Box<dyn DatabaseInstance>, BackendError>;
}

/// An isolated database instance
#[async_trait]
pub trait DatabaseInstance: Send + Sync {
    /// Execute setup SQL (DDL, inserts, etc.)
    /// For in-memory databases, this may buffer SQL for later execution.
    /// For file-based databases, this executes immediately.
    async fn execute_setup(&mut self, sql: &str) -> Result<(), BackendError> {
        // For file-based databases, execute immediately
        let result = self.execute(sql).await?;
        if result.is_error() {
            Err(BackendError::Execute(
                result.error.unwrap_or_else(|| "unknown error".to_string()),
            ))
        } else {
            Ok(())
        }
    }

    /// Execute SQL and return results
    /// For in-memory databases, this will combine any buffered setup SQL with the query.
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError>;

    /// Execute SQL while checking every live materialized view after each
    /// successful statement. Only called when the backend advertises support.
    async fn execute_with_materialized_view_verification(
        &mut self,
        _sql: &str,
    ) -> Result<QueryResult, BackendError> {
        Err(BackendError::NotAvailable(
            "typed materialized-view verification is not implemented by this backend".to_string(),
        ))
    }

    /// Close and cleanup the database, returning a handle that keeps the file alive
    async fn close(self: Box<Self>) -> Result<DatabaseFileHandle, BackendError>;
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

    /// The backend cannot open a database of this kind. The runner skips the test.
    #[error("{0}")]
    Unsupported(String),

    #[error("timeout after {0:?}")]
    Timeout(Duration),
}

/// Parse pipe-separated list output into rows
pub fn parse_list_output(output: &str) -> Vec<Vec<String>> {
    output
        .lines()
        .map(|line| {
            if line.is_empty() {
                // Empty line represents a row with a single empty cell (NULL)
                vec!["".to_string()]
            } else {
                line.split('|').map(|s| s.to_string()).collect()
            }
        })
        .collect()
}

/// Return an error when a child process crashed instead of exiting: a Rust
/// panic exits with status 101, and a panic with `panic = "abort"` (the release
/// profile) or a segfault kills the process with a signal.
///
/// The exit status decides this, not the stderr text: sqlite3 and tursodb write
/// every SQL error to stderr, and an error message can quote any text, including
/// text that looks like a panic message.
pub fn check_process_crash(
    status: std::process::ExitStatus,
    stderr: &str,
) -> Result<(), BackendError> {
    #[cfg(unix)]
    let signal = std::os::unix::process::ExitStatusExt::signal(&status);
    #[cfg(not(unix))]
    let signal: Option<i32> = None;
    if status.code() == Some(101) || signal.is_some() {
        return Err(BackendError::Execute(format!(
            "process crashed ({status}): {}",
            stderr.trim()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_list_output_empty() {
        let output = "";
        let rows = parse_list_output(output);
        assert!(rows.is_empty());
    }

    #[test]
    fn test_parse_list_output_single_column() {
        let output = "1\n2\n3";
        let rows = parse_list_output(output);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec!["1"]);
        assert_eq!(rows[1], vec!["2"]);
        assert_eq!(rows[2], vec!["3"]);
    }

    #[test]
    fn test_parse_list_output_multiple_columns() {
        let output = "1|Alice|30\n2|Bob|25";
        let rows = parse_list_output(output);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec!["1", "Alice", "30"]);
        assert_eq!(rows[1], vec!["2", "Bob", "25"]);
    }

    #[test]
    fn test_parse_list_output_empty_values() {
        let output = "1||3";
        let rows = parse_list_output(output);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec!["1", "", "3"]);
    }

    #[test]
    fn test_parse_list_output_trailing_newline() {
        let output = "1|Alice\n2|Bob\n";
        let rows = parse_list_output(output);
        assert_eq!(rows.len(), 2);
    }

    #[cfg(unix)]
    fn wait_status(raw: i32) -> std::process::ExitStatus {
        std::os::unix::process::ExitStatusExt::from_raw(raw)
    }

    const PANIC_STDERR: &str = "thread 'main' panicked at core/translate/expr/expr_type.rs:760:21:\n\
        internal error: entered unreachable code: type \"varchar\" requires 1 parameter(s), got 2";

    #[cfg(unix)]
    #[test]
    fn test_check_process_crash_panic_exit_status() {
        // A wait status holds the exit code in bits 8..16.
        let err = check_process_crash(wait_status(101 << 8), PANIC_STDERR).unwrap_err();
        assert!(err.to_string().contains("process crashed"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn test_check_process_crash_killed_by_signal() {
        // SIGABRT, which `panic = "abort"` raises.
        let err = check_process_crash(wait_status(6), PANIC_STDERR).unwrap_err();
        assert!(err.to_string().contains("process crashed"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn test_check_process_crash_sql_error_quoting_panic_text() {
        // sqlite3 exits with status 1 after an SQL error whose message can quote
        // text that looks like a panic message.
        let stderr = "Parse error near line 1: no such table: a\nthread 'x' panicked at b";
        assert!(check_process_crash(wait_status(1 << 8), stderr).is_ok());
        assert!(check_process_crash(wait_status(0), stderr).is_ok());
    }
}
