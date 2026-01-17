use super::{BackendError, DatabaseInstance, QueryResult, SqlBackend, parse_list_output};
use crate::backends::DefaultDatabaseResolver;
use crate::parser::ast::{Backend, DatabaseConfig, DatabaseLocation};
use async_trait::async_trait;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::timeout;

/// JavaScript backend that executes SQL via Node.js and @tursodatabase/database
pub struct JsBackend {
    /// Path to the Node.js binary
    node_path: PathBuf,
    /// Path to the runner script
    script_path: PathBuf,
    /// Timeout for query execution
    timeout: Duration,
    /// Resolver for default database paths
    default_db_resolver: Option<Arc<dyn DefaultDatabaseResolver>>,
    /// Enable MVCC mode
    mvcc: bool,
}

impl JsBackend {
    /// Create a new JavaScript backend with the given node and script paths
    pub fn new(node_path: impl Into<PathBuf>, script_path: impl Into<PathBuf>) -> Self {
        Self {
            node_path: node_path.into(),
            script_path: script_path.into(),
            timeout: Duration::from_secs(30),
            default_db_resolver: None,
            mvcc: false,
        }
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

    /// Enable MVCC mode (experimental journal mode)
    pub fn with_mvcc(mut self, mvcc: bool) -> Self {
        self.mvcc = mvcc;
        self
    }
}

#[async_trait]
impl SqlBackend for JsBackend {
    fn name(&self) -> &str {
        "js"
    }

    fn backend_type(&self) -> Backend {
        Backend::Js
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

        Ok(Box::new(JsDatabaseInstance {
            node_path: self.node_path.clone(),
            script_path: self.script_path.clone(),
            db_path,
            readonly: config.readonly,
            timeout: self.timeout,
            _temp_file: temp_file,
            is_memory,
            setup_buffer: Vec::new(),
            mvcc: self.mvcc,
        }))
    }
}

/// A database instance that executes SQL via Node.js subprocess
pub struct JsDatabaseInstance {
    node_path: PathBuf,
    script_path: PathBuf,
    db_path: String,
    readonly: bool,
    timeout: Duration,
    /// Keep temp file alive - it's deleted when this is dropped
    _temp_file: Option<NamedTempFile>,
    /// Whether this is an in-memory database (needs buffering)
    is_memory: bool,
    /// Buffer of setup SQL (for memory databases)
    setup_buffer: Vec<String>,
    /// Enable MVCC mode
    mvcc: bool,
}

impl JsDatabaseInstance {
    /// Execute SQL by spawning a Node.js process
    async fn run_sql(&self, sql: &str) -> Result<QueryResult, BackendError> {
        let mut cmd = Command::new(&self.node_path);

        // Arguments: script path, database path, optional --readonly
        cmd.arg(&self.script_path);
        cmd.arg(&self.db_path);

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
            .map_err(|e| BackendError::Execute(format!("failed to spawn node: {e}")))?;

        // Prepend MVCC pragma if enabled (skip for readonly databases).
        // FIXME: readonly default DB tests do not exercise MVCC code paths.
        let sql_to_execute = if self.mvcc && !self.readonly {
            format!("PRAGMA journal_mode = 'experimental_mvcc';\n{sql}")
        } else {
            sql.to_string()
        };

        // Write SQL to stdin
        if let Some(stdin) = child.stdin.as_mut() {
            stdin
                .write_all(sql_to_execute.as_bytes())
                .await
                .map_err(|e| BackendError::Execute(format!("failed to write to stdin: {e}")))?;
        }
        child.stdin.take(); // Close stdin to signal end of input

        // Wait for output with timeout
        let output = timeout(self.timeout, child.wait_with_output())
            .await
            .map_err(|_| BackendError::Timeout(self.timeout))?
            .map_err(|e| BackendError::Execute(format!("failed to read output: {e}")))?;

        // Parse stdout
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        // Check for errors in stderr
        if !stderr.is_empty() && (stderr.contains("Error") || stderr.contains("error")) {
            return Ok(QueryResult::error(stderr.trim().to_string()));
        }

        // Check for errors in stdout (the runner outputs "Error: ...")
        if stdout.starts_with("Error:") || stdout.contains("\nError:") {
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

        let mut rows = parse_list_output(&stdout);

        // Filter out MVCC pragma output if present
        if self.mvcc && !rows.is_empty() {
            if let Some(first_row) = rows.first() {
                if first_row.len() == 1 && first_row[0] == "experimental_mvcc" {
                    rows.remove(0);
                }
            }
        }

        Ok(QueryResult::success(rows))
    }
}

#[async_trait]
impl DatabaseInstance for JsDatabaseInstance {
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
