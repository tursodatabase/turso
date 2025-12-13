use super::{BackendError, DatabaseInstance, QueryResult, SqlBackend};
use crate::parser::ast::{DatabaseConfig, DatabaseLocation};
use async_trait::async_trait;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::timeout;

/// CLI backend that executes SQL via the tursodb CLI tool
pub struct CliBackend {
    /// Path to the tursodb binary
    binary_path: PathBuf,
    /// Working directory for the CLI
    working_dir: Option<PathBuf>,
    /// Timeout for query execution
    timeout: Duration,
}

impl CliBackend {
    /// Create a new CLI backend with the given binary path
    pub fn new(binary_path: impl Into<PathBuf>) -> Self {
        Self {
            binary_path: binary_path.into(),
            working_dir: None,
            timeout: Duration::from_secs(30),
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
        let (db_path, temp_file) = match &config.location {
            DatabaseLocation::Memory => (":memory:".to_string(), None),
            DatabaseLocation::TempFile => {
                let temp = NamedTempFile::new()
                    .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
                let path = temp.path().to_string_lossy().to_string();
                (path, Some(temp))
            }
            DatabaseLocation::Path(path) => (path.to_string_lossy().to_string(), None),
        };

        Ok(Box::new(CliDatabaseInstance {
            binary_path: self.binary_path.clone(),
            working_dir: self.working_dir.clone(),
            db_path,
            readonly: config.readonly,
            timeout: self.timeout,
            _temp_file: temp_file, // Keep temp file alive
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
}

#[async_trait]
impl DatabaseInstance for CliDatabaseInstance {
    async fn execute(&mut self, sql: &str) -> Result<QueryResult, BackendError> {
        let mut cmd = Command::new(&self.binary_path);

        // Set working directory if specified
        if let Some(dir) = &self.working_dir {
            cmd.current_dir(dir);
        }

        // Build command arguments
        cmd.arg(&self.db_path);
        cmd.arg("-m").arg("list"); // List mode for pipe-separated output

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

        // Check for errors
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.is_empty() && stderr.contains("Error") {
            return Ok(QueryResult::error(stderr.trim().to_string()));
        }

        if !output.status.success() {
            let stderr = stderr.trim();
            if !stderr.is_empty() {
                return Ok(QueryResult::error(stderr.to_string()));
            }
            return Ok(QueryResult::error(format!(
                "command exited with status {}",
                output.status
            )));
        }

        // Parse stdout
        let stdout = String::from_utf8_lossy(&output.stdout);
        let rows = parse_list_output(&stdout);

        Ok(QueryResult::success(rows))
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
