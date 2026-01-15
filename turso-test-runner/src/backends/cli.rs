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

/// CLI backend that executes SQL via the tursodb or sqlite3 CLI tools
pub struct CliBackend {
    /// Path to the tursodb binary
    binary_path: PathBuf,
    /// Working directory for the CLI
    working_dir: Option<PathBuf>,
    /// Timeout for query execution
    timeout: Duration,
    /// Resolver for default database paths
    default_db_resolver: Option<Arc<dyn DefaultDatabaseResolver>>,
    /// Enable MVCC mode
    mvcc: bool,
}

impl CliBackend {
    /// Create a new CLI backend with the given binary path
    pub fn new(binary_path: impl Into<PathBuf>) -> Self {
        Self {
            binary_path: binary_path.into(),
            working_dir: None,
            timeout: Duration::from_secs(30),
            default_db_resolver: None,
            mvcc: false,
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

    /// Enable MVCC mode (experimental journal mode)
    pub fn with_mvcc(mut self, mvcc: bool) -> Self {
        self.mvcc = mvcc;
        self
    }

    pub fn set_default_db_resolver(mut self, resolver: Arc<dyn DefaultDatabaseResolver>) -> Self {
        self.default_db_resolver = Some(resolver);
        self
    }
}

#[async_trait]
impl SqlBackend for CliBackend {
    fn name(&self) -> String {
        let file_name = self
            .binary_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| BackendError::Execute("binary path does not contain a file name".into()))
            .unwrap_or("cli");
        let is_sqlite = file_name.starts_with("sqlite");
        format!("{}-cli", if is_sqlite { "sqlite" } else { "tursodb" })
    }

    fn backend_type(&self) -> Backend {
        Backend::Cli
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
            mvcc: self.mvcc,
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
    /// Enable MVCC mode
    mvcc: bool,
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
                BackendError::Execute("binary path does not contain a file name".into())
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
            .map_err(|e| BackendError::Execute(format!("failed to spawn tursodb: {e}")))?;

        // Prepend MVCC pragma if enabled (skip for readonly databases)
        let sql_to_execute = if self.mvcc && is_turso_cli && !self.readonly {
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
