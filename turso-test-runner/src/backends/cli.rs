use super::{BackendError, DatabaseInstance, QueryResult, SqlBackend};
use crate::parser::ast::{DatabaseConfig, DatabaseLocation};
use async_trait::async_trait;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::timeout;

/// Provides resolved paths for default databases
pub trait DefaultDatabaseResolver: Send + Sync {
    /// Resolve a database location to an actual path
    /// Returns Some(path) for Default/DefaultNoRowidAlias, None otherwise
    fn resolve(&self, location: &DatabaseLocation) -> Option<PathBuf>;
}

/// CLI backend that executes SQL via the tursodb CLI tool
pub struct CliBackend {
    /// Path to the tursodb binary
    binary_path: PathBuf,
    /// Working directory for the CLI
    working_dir: Option<PathBuf>,
    /// Timeout for query execution
    timeout: Duration,
    /// Resolver for default database paths
    default_db_resolver: Option<Arc<dyn DefaultDatabaseResolver>>,
}

impl CliBackend {
    /// Create a new CLI backend with the given binary path
    pub fn new(binary_path: impl Into<PathBuf>) -> Self {
        Self {
            binary_path: binary_path.into(),
            working_dir: None,
            timeout: Duration::from_secs(30),
            default_db_resolver: None,
        }
    }

    /// Set the working directory for the CLI
    pub fn with_working_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.working_dir = Some(dir.into());
        self
    }

    /// Set the timeout for query execution
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the default database resolver
    pub fn with_default_db_resolver(mut self, resolver: Arc<dyn DefaultDatabaseResolver>) -> Self {
        self.default_db_resolver = Some(resolver);
        self
    }
}

#[async_trait]
impl SqlBackend for CliBackend {
    fn name(&self) -> &str {
        "cli"
    }

    async fn create_database(
        &self,
        config: &DatabaseConfig,
    ) -> Result<Box<dyn DatabaseInstance>, BackendError> {
        let (db_path, temp_file, is_memory) = match &config.location {
            DatabaseLocation::Memory => (":memory:".to_string(), None, true),
            DatabaseLocation::TempFile => {
                let temp = NamedTempFile::new()
                    .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
                let path = temp.path().to_string_lossy().to_string();
                (path, Some(temp), false)
            }
            DatabaseLocation::Path(path) => (path.to_string_lossy().to_string(), None, false),
            DatabaseLocation::Default | DatabaseLocation::DefaultNoRowidAlias => {
                // Resolve the path using the resolver
                let resolved = self
                    .default_db_resolver
                    .as_ref()
                    .and_then(|r| r.resolve(&config.location))
                    .ok_or_else(|| {
                        BackendError::CreateDatabase(
                            "default database not generated - no resolver configured".to_string(),
                        )
                    })?;
                (resolved.to_string_lossy().to_string(), None, false)
            }
        };

        Ok(Box::new(CliDatabaseInstance {
            binary_path: self.binary_path.clone(),
            working_dir: self.working_dir.clone(),
            db_path,
            readonly: config.readonly,
            timeout: self.timeout,
            _temp_file: temp_file,
            is_memory,
            setup_buffer: Vec::new(),
        }))
    }
}

/// A database instance that executes SQL via CLI subprocess
pub struct CliDatabaseInstance {
    binary_path: PathBuf,
    working_dir: Option<PathBuf>,
    db_path: String,
    readonly: bool,
    timeout: Duration,
    /// Keep temp file alive - it's deleted when this is dropped
    _temp_file: Option<NamedTempFile>,
    /// Whether this is an in-memory database (needs buffering)
    is_memory: bool,
    /// Buffer of setup SQL (for memory databases)
    setup_buffer: Vec<String>,
}

impl CliDatabaseInstance {
    /// Execute SQL by spawning a CLI process
    async fn run_sql(&self, sql: &str) -> Result<QueryResult, BackendError> {
        let mut cmd = Command::new(&self.binary_path);

        let file_name = self
            .binary_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                BackendError::Execute(format!("binary path does not contain a file name"))
            })?;
        let is_sqlite = file_name.starts_with("sqlite");
        let is_turso_cli = file_name.starts_with("tursodb") || file_name.starts_with("turso");

        // Set working directory if specified
        if let Some(dir) = &self.working_dir {
            cmd.current_dir(dir);
        }

        if is_sqlite {
            cmd.arg(format!("file:{}?immutable=1", self.db_path));
        }

        // Only add -q flag for tursodb/turso (not sqlite3 or other CLIs)
        if is_turso_cli {
            cmd.arg(&self.db_path);
            cmd.arg("-q"); // Quiet mode - suppress banner
            cmd.arg("-m").arg("list"); // List mode for pipe-separated output
            cmd.arg("--experimental-views");
            cmd.arg("--experimental-strict");
            cmd.arg("--experimental-triggers");
        }

        if self.readonly {
            cmd.arg("--readonly");
        }

        // Set up pipes
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        // Spawn the process
        let mut child = cmd
            .spawn()
            .map_err(|e| BackendError::Execute(format!("failed to spawn tursodb: {}", e)))?;

        // Write SQL to stdin
        if let Some(stdin) = child.stdin.as_mut() {
            stdin
                .write_all(sql.as_bytes())
                .await
                .map_err(|e| BackendError::Execute(format!("failed to write to stdin: {}", e)))?;
        }
        child.stdin.take(); // Close stdin to signal end of input

        // Wait for output with timeout
        let output = timeout(self.timeout, child.wait_with_output())
            .await
            .map_err(|_| BackendError::Timeout(self.timeout))?
            .map_err(|e| BackendError::Execute(format!("failed to read output: {}", e)))?;

        // Parse stdout
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        // Check for errors in stderr
        if !stderr.is_empty() && (stderr.contains("Error") || stderr.contains("error")) {
            return Ok(QueryResult::error(stderr.trim().to_string()));
        }

        // Check for errors in stdout (tursodb outputs errors like "× Parse error: ...")
        if stdout.contains("× ") || stdout.contains("error:") || stdout.contains("Error:") {
            return Ok(QueryResult::error(stdout.trim().to_string()));
        }

        if !output.status.success() {
            let stderr = stderr.trim();
            if !stderr.is_empty() {
                return Ok(QueryResult::error(stderr.to_string()));
            }
            if !stdout.trim().is_empty() {
                return Ok(QueryResult::error(stdout.trim().to_string()));
            }
            return Ok(QueryResult::error(format!(
                "command exited with status {}",
                output.status
            )));
        }

        let rows = parse_list_output(&stdout);

        Ok(QueryResult::success(rows))
    }
}

#[async_trait]
impl DatabaseInstance for CliDatabaseInstance {
    async fn execute_setup(&mut self, sql: &str) -> Result<(), BackendError> {
        if self.is_memory {
            // For memory databases, buffer the setup SQL for later
            self.setup_buffer.push(sql.to_string());
            Ok(())
        } else {
            // For file-based databases, execute immediately
            let result = self.run_sql(sql).await?;
            if result.is_error() {
                Err(BackendError::Execute(
                    result.error.unwrap_or_else(|| "unknown error".to_string()),
                ))
            } else {
                Ok(())
            }
        }
    }

    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError> {
        if self.is_memory && !self.setup_buffer.is_empty() {
            // Combine buffered setup SQL with the query
            let mut combined = self.setup_buffer.join("\n");
            combined.push('\n');
            combined.push_str(sql);
            self.run_sql(&combined).await
        } else {
            // Execute directly
            self.run_sql(sql).await
        }
    }

    async fn close(self: Box<Self>) -> Result<(), BackendError> {
        // Temp file will be automatically deleted when self is dropped
        Ok(())
    }
}

/// Parse pipe-separated list output from tursodb
fn parse_list_output(output: &str) -> Vec<Vec<String>> {
    output
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| line.split('|').map(|s| s.to_string()).collect())
        .collect()
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
}
