use super::{
    BackendError, DatabaseFileHandle, DatabaseInstance, QueryResult, SqlBackend, parse_list_output,
};
use crate::backends::DefaultDatabaseResolver;
use crate::parser::ast::{Backend, Capability, DatabaseConfig, DatabaseLocation};
use async_trait::async_trait;
use std::collections::HashSet;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
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
    /// Resolver for default database paths
    default_db_resolver: Option<Arc<dyn DefaultDatabaseResolver>>,
    /// Enable MVCC mode
    mvcc: bool,
    /// Whether the binary is sqlite3 (detected from binary name)
    is_sqlite: bool,
}

impl CliBackend {
    /// Create a new CLI backend with the given binary path
    pub fn new(binary_path: impl Into<PathBuf>) -> Self {
        let binary_path = binary_path.into();
        let is_sqlite = binary_path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|name| name.starts_with("sqlite"))
            .unwrap_or(false);
        Self {
            binary_path,
            working_dir: None,
            timeout: Duration::from_secs(30),
            default_db_resolver: None,
            mvcc: false,
            is_sqlite,
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
    fn name(&self) -> &str {
        "cli"
    }

    fn backend_type(&self) -> Backend {
        Backend::Cli
    }

    fn capabilities(&self) -> HashSet<Capability> {
        Capability::all_set()
    }

    fn is_sqlite(&self) -> bool {
        self.is_sqlite
    }

    async fn create_database(
        &self,
        config: &DatabaseConfig,
    ) -> Result<Box<dyn DatabaseInstance>, BackendError> {
        let (db_path, temp_file, buffer_setups) = match &config.location {
            DatabaseLocation::Memory => (":memory:".to_string(), None, true),
            DatabaseLocation::TempFile => {
                let temp = NamedTempFile::new()
                    .map_err(|e| BackendError::CreateDatabase(e.to_string()))?;
                let path = temp.path().to_string_lossy().to_string();
                (path, Some(temp), true)
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
            buffer_setups,
            setup_buffer: Vec::new(),
            mvcc: self.mvcc,
            is_sqlite: self.is_sqlite,
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
    /// Whether to buffer setups and send them with the test SQL in one subprocess.
    /// True for per-test databases (memory, temp); false for shared databases (file, default).
    buffer_setups: bool,
    /// Buffered setup SQL to prepend to the first query
    setup_buffer: Vec<String>,
    /// Enable MVCC mode
    mvcc: bool,
    is_sqlite: bool,
}

impl CliDatabaseInstance {
    const DOT_COMMANDS: &[&str] = &[".schema", ".tables"];

    fn final_supported_sqlite_dot_command(sql: &str) -> Option<&str> {
        sql.lines().rev().map(str::trim).find(|line| {
            !line.is_empty()
                && Self::DOT_COMMANDS.iter().any(|cmd| {
                    *line == *cmd
                        || line
                            .strip_prefix(cmd)
                            .is_some_and(|rest| rest.is_empty() || rest.starts_with(' '))
                })
        })
    }

    fn normalize_sqlite_dot_command_script(sql: &str) -> String {
        let final_dot_command = Self::final_supported_sqlite_dot_command(sql);

        sql.lines()
            .map(|line| {
                if let Some(dot_command) = final_dot_command.filter(|cmd| line.trim() == *cmd) {
                    dot_command.to_string()
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn collapse_repeated_spaces(input: &str) -> String {
        let mut normalized = input.to_string();
        while normalized.contains("  ") {
            normalized = normalized.replace("  ", " ");
        }
        normalized
    }

    fn normalize_sqlite_schema_line(line: &str) -> String {
        const PREFIXES: &[&str] = &["CREATE TABLE ", "CREATE TABLE IF NOT EXISTS "];

        for prefix in PREFIXES {
            if let Some(rest) = line.strip_prefix(prefix) {
                if let Some(paren_idx) = rest.find('(') {
                    let name_part = &rest[..paren_idx];
                    if !name_part.ends_with(' ') {
                        return format!("{prefix}{name_part} {}", &rest[paren_idx..]);
                    }
                }
            }
        }

        line.to_string()
    }

    fn normalize_sqlite_dot_command_output(dot_command: &str, stdout: &str) -> Vec<Vec<String>> {
        stdout
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| match dot_command {
                ".tables" => vec![Self::collapse_repeated_spaces(line)],
                ".schema" => vec![Self::normalize_sqlite_schema_line(line)],
                _ => vec![line.to_string()],
            })
            .collect()
    }

    /// Execute SQL by spawning a CLI process
    async fn run_sql(&self, sql: &str) -> Result<QueryResult, BackendError> {
        let mut cmd = Command::new(&self.binary_path);
        let sqlite_dot_command = self
            .is_sqlite
            .then(|| Self::final_supported_sqlite_dot_command(sql))
            .flatten();
        let is_sqlite_dot_command = sqlite_dot_command.is_some();

        let file_name = self
            .binary_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                BackendError::Execute("binary path does not contain a file name".to_string())
            })?;
        let is_turso_cli = file_name.starts_with("tursodb") || file_name.starts_with("turso");

        // Set working directory if specified
        if let Some(dir) = &self.working_dir {
            cmd.current_dir(dir);
        }

        if self.is_sqlite {
            if self.readonly {
                cmd.arg(format!("file:{}?immutable=1", self.db_path));
            } else {
                cmd.arg(&self.db_path);
            }
        }

        // Only add -q flag for tursodb/turso (not sqlite3 or other CLIs)
        if is_turso_cli {
            cmd.arg(&self.db_path);
            cmd.arg("-q"); // Quiet mode - suppress banner
            cmd.arg("-m").arg("list"); // List mode for pipe-separated output
            cmd.arg("--experimental-views");
            cmd.arg("--experimental-custom-types");
            cmd.arg("--experimental-attach");
            cmd.arg("--experimental-index-method");
            cmd.arg("--experimental-generated-columns");
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

        // Prepend MVCC pragma if enabled (skip for readonly databases; the generated readonly DBs are already in MVCC mode).
        let sql_to_execute = if self.mvcc && is_turso_cli && !self.readonly {
            format!("PRAGMA journal_mode = 'mvcc';\n{sql}")
        } else {
            sql.to_string()
        };
        let sql_to_execute = if is_sqlite_dot_command {
            Self::normalize_sqlite_dot_command_script(&sql_to_execute)
        } else {
            sql_to_execute
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

        // Parse stdout/stderr and check exit code
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let exit_success = output.status.success();

        // Detect errors from output text
        let has_stderr_error =
            !stderr.is_empty() && (stderr.contains("Error") || stderr.contains("error"));
        let has_stdout_error =
            stdout.contains("× ") || stdout.contains("error:") || stdout.contains("Error:");

        if has_stderr_error || has_stdout_error || !exit_success {
            // Extract the best error message available
            let error_msg = if has_stderr_error {
                stderr.trim().to_string()
            } else if has_stdout_error {
                stdout.trim().to_string()
            } else if !stderr.trim().is_empty() {
                stderr.trim().to_string()
            } else if !stdout.trim().is_empty() {
                stdout.trim().to_string()
            } else {
                format!("command exited with status {}", output.status)
            };
            return Ok(QueryResult::error(error_msg));
        }

        if is_sqlite_dot_command {
            let rows = Self::normalize_sqlite_dot_command_output(
                sqlite_dot_command.expect("checked above"),
                &stdout,
            );

            Ok(QueryResult::success(rows))
        } else {
            let mut rows = parse_list_output(&stdout);

            // Filter out MVCC pragma output if present
            if self.mvcc && !rows.is_empty() {
                if let Some(first_row) = rows.first() {
                    if first_row.len() == 1 && first_row[0] == "mvcc" {
                        rows.remove(0);
                    }
                }
            }

            Ok(QueryResult::success(rows))
        }
    }
}

#[async_trait]
impl DatabaseInstance for CliDatabaseInstance {
    async fn execute_setup(&mut self, sql: &str) -> Result<(), BackendError> {
        if self.buffer_setups {
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
        if self.buffer_setups && !self.setup_buffer.is_empty() {
            // Combine buffered setup SQL with the query, using a marker to separate them
            let mut combined = self.setup_buffer.join("\n");
            combined.push('\n');
            // Add marker to identify where setup ends and query begins
            combined.push_str(super::SETUP_END_MARKER_SQL);
            combined.push('\n');
            combined.push_str(sql);
            let result = self.run_sql(&combined).await?;
            // Filter out setup output (everything before and including the marker)
            Ok(result.filter_setup_output())
        } else {
            // Execute directly
            self.run_sql(sql).await
        }
    }

    async fn close(self: Box<Self>) -> Result<DatabaseFileHandle, BackendError> {
        match self._temp_file {
            Some(tf) => Ok(DatabaseFileHandle::temp(tf)),
            None => Ok(DatabaseFileHandle::none()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CliDatabaseInstance;

    #[test]
    fn final_supported_sqlite_dot_command_detects_last_line() {
        let sql = "CREATE TABLE t1(a);\nSELECT '__SETUP_END_MARKER_7f3a9b2c__';\n    .schema";
        assert_eq!(
            CliDatabaseInstance::final_supported_sqlite_dot_command(sql),
            Some(".schema")
        );
    }

    #[test]
    fn normalize_sqlite_dot_command_script_left_aligns_final_command_only() {
        let sql = "CREATE TABLE t1(a);\n    .schema";
        assert_eq!(
            CliDatabaseInstance::normalize_sqlite_dot_command_script(sql),
            "CREATE TABLE t1(a);\n.schema"
        );
    }

    #[test]
    fn normalize_sqlite_tables_output_preserves_single_spaces() {
        assert_eq!(
            CliDatabaseInstance::normalize_sqlite_dot_command_output(
                ".tables",
                "t1  v1\nuser logs"
            ),
            vec![vec!["t1 v1".to_string()], vec!["user logs".to_string()]]
        );
    }

    #[test]
    fn normalize_sqlite_schema_output_only_adjusts_create_table_prefix() {
        assert_eq!(
            CliDatabaseInstance::normalize_sqlite_dot_command_output(
                ".schema",
                "CREATE TABLE t1(a CHECK(abs(a) > 0));\nCREATE TRIGGER trg AFTER INSERT ON t1 BEGIN SELECT 1; END;"
            ),
            vec![
                vec!["CREATE TABLE t1 (a CHECK(abs(a) > 0));".to_string()],
                vec!["CREATE TRIGGER trg AFTER INSERT ON t1 BEGIN SELECT 1; END;".to_string()]
            ]
        );
    }
}
