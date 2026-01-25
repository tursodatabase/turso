//! Snapshot testing support for SQL EXPLAIN output.
//!
//! This module provides insta-compatible snapshot testing capabilities
//! for verifying SQL EXPLAIN output remains consistent.
//!
//! Uses serde_yaml for serialization.
//!
//! ## Snapshot Update Modes
//!
//! Similar to cargo-insta, this module supports multiple update modes:
//!
//! - `Auto`: Default mode. Behaves like `No` in CI environments (no files written),
//!   or `New` otherwise (writes `.snap.new` files for review).
//! - `New`: Writes new/changed snapshots to `.snap.new` files for review.
//! - `Always`: Writes snapshots directly to `.snap` files (like `--update-snapshots`).
//! - `No`: Never writes snapshot files; just reports pass/fail.
//!
//! ## CI Detection
//!
//! CI environments are detected by checking for common environment variables:
//! `CI`, `GITHUB_ACTIONS`, `TRAVIS`, `CIRCLECI`, `GITLAB_CI`, `JENKINS_URL`, etc.

use clap::ValueEnum;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use tokio::fs;

/// Snapshot update mode, similar to cargo-insta's INSTA_UPDATE values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum SnapshotUpdateMode {
    /// Default mode: `No` in CI, `New` otherwise.
    #[default]
    Auto,
    /// Write new/changed snapshots to `.snap.new` files for review.
    New,
    /// Write snapshots directly to `.snap` files (always update).
    Always,
    /// Never write snapshot files; just report pass/fail.
    No,
}

impl SnapshotUpdateMode {
    /// Resolve the effective mode, taking CI detection into account.
    pub fn resolve(self) -> SnapshotUpdateMode {
        match self {
            SnapshotUpdateMode::Auto => {
                if is_ci() {
                    SnapshotUpdateMode::No
                } else {
                    SnapshotUpdateMode::New
                }
            }
            other => other,
        }
    }
}

impl std::fmt::Display for SnapshotUpdateMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            SnapshotUpdateMode::Auto => "auto",
            SnapshotUpdateMode::New => "new",
            SnapshotUpdateMode::Always => "always",
            SnapshotUpdateMode::No => "no",
        };
        write!(f, "{s}")
    }
}

impl std::str::FromStr for SnapshotUpdateMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(SnapshotUpdateMode::Auto),
            "new" => Ok(SnapshotUpdateMode::New),
            "always" => Ok(SnapshotUpdateMode::Always),
            "no" => Ok(SnapshotUpdateMode::No),
            _ => Err(format!(
                "Invalid snapshot update mode: '{s}'. Valid options: auto, new, always, no"
            )),
        }
    }
}

/// Check if we're running in a CI environment.
pub fn is_ci() -> bool {
    // Check common CI environment variables
    std::env::var("CI").is_ok()
        || std::env::var("GITHUB_ACTIONS").is_ok()
        || std::env::var("TRAVIS").is_ok()
        || std::env::var("CIRCLECI").is_ok()
        || std::env::var("GITLAB_CI").is_ok()
        || std::env::var("JENKINS_URL").is_ok()
        || std::env::var("BUILDKITE").is_ok()
        || std::env::var("TF_BUILD").is_ok() // Azure Pipelines
        || std::env::var("CODEBUILD_BUILD_ID").is_ok() // AWS CodeBuild
}

/// Snapshot file metadata (YAML frontmatter)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Source test file name
    pub source: String,
    /// The SQL expression being snapshotted
    pub expression: String,
    /// Structured info section
    pub info: SnapshotInfo,
}

/// Structured info section in snapshot metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotInfo {
    /// Type of SQL statement (SELECT, INSERT, etc.)
    pub statement_type: String,
    /// Tables referenced in the query
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<String>,
    /// Setup blocks that were used
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub setup_blocks: Vec<String>,
    /// Database location
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// Line number in test file
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
}

impl SnapshotInfo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_setups(mut self, setups: Vec<String>) -> Self {
        self.setup_blocks = setups;
        self
    }

    pub fn with_database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }

    pub fn with_line_number(mut self, line: u32) -> Self {
        self.line = Some(line);
        self
    }

    pub fn with_statement_type(mut self, stmt_type: String) -> Self {
        self.statement_type = stmt_type;
        self
    }

    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }
}

/// A complete parsed snapshot with metadata and content
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The YAML metadata
    pub metadata: SnapshotMetadata,
    /// The actual snapshot content (EXPLAIN output)
    pub content: String,
}

impl Snapshot {
    /// Parse a snapshot file from its contents
    pub fn parse(file_contents: &str) -> Result<Self, SnapshotParseError> {
        let (yaml_str, content) = split_frontmatter(file_contents)?;
        let metadata: SnapshotMetadata =
            serde_yaml::from_str(&yaml_str).map_err(SnapshotParseError::Yaml)?;

        Ok(Snapshot {
            metadata,
            content: content.to_string(),
        })
    }

    /// Serialize to the snapshot file format
    pub fn to_string(&self) -> Result<String, SnapshotParseError> {
        let yaml = serde_yaml::to_string(&self.metadata).map_err(SnapshotParseError::Yaml)?;
        Ok(format!("---\n{}---\n{}\n", yaml, self.content))
    }
}

/// Error type for snapshot parsing
#[derive(Debug)]
pub enum SnapshotParseError {
    /// Invalid frontmatter format
    InvalidFormat(String),
    /// YAML parsing error
    Yaml(serde_yaml::Error),
}

impl std::fmt::Display for SnapshotParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidFormat(msg) => write!(f, "Invalid snapshot format: {msg}"),
            Self::Yaml(e) => write!(f, "YAML error: {e}"),
        }
    }
}

impl std::error::Error for SnapshotParseError {}

/// Split a snapshot file into YAML frontmatter and content
fn split_frontmatter(contents: &str) -> Result<(String, String), SnapshotParseError> {
    let contents = contents.trim_start();

    if !contents.starts_with("---") {
        return Err(SnapshotParseError::InvalidFormat(
            "File must start with ---".to_string(),
        ));
    }

    let after_first = &contents[3..];
    let end_idx = after_first.find("\n---").ok_or_else(|| {
        SnapshotParseError::InvalidFormat("Missing closing --- for frontmatter".to_string())
    })?;

    let yaml = after_first[..end_idx].trim().to_string();
    let content = after_first[end_idx + 4..]
        .trim_start_matches('\n')
        .trim_end()
        .to_string();

    Ok((yaml, content))
}

/// Result of comparing a snapshot
#[derive(Debug, Clone)]
pub enum SnapshotResult {
    /// Snapshot matched
    Match,
    /// Snapshot differs
    Mismatch {
        expected: String,
        actual: String,
        diff: String,
    },
    /// No snapshot exists yet
    New { content: String },
    /// Snapshot was updated (when in update mode)
    Updated { old: String, new: String },
    /// Error occurred
    Error { msg: String },
}

/// Manages snapshot files for a test file
pub struct SnapshotManager {
    /// Path to the test file (used to derive snapshot directory)
    test_file_path: PathBuf,
    /// Snapshot update mode (resolved from Auto if needed)
    update_mode: SnapshotUpdateMode,
}

impl SnapshotManager {
    /// Create a new snapshot manager for a test file.
    ///
    /// The `mode` is automatically resolved: `Auto` becomes `No` in CI, `New` otherwise.
    pub fn new(test_file_path: &Path, mode: SnapshotUpdateMode) -> Self {
        Self {
            test_file_path: test_file_path.to_path_buf(),
            update_mode: mode.resolve(),
        }
    }

    /// Get the snapshots directory path
    fn snapshots_dir(&self) -> PathBuf {
        self.test_file_path
            .parent()
            .unwrap_or(Path::new("."))
            .join("snapshots")
    }

    /// Get the snapshot file path for a given snapshot name
    pub fn snapshot_path(&self, name: &str) -> PathBuf {
        let file_stem = self
            .test_file_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        self.snapshots_dir()
            .join(format!("{file_stem}__{name}.snap"))
    }

    /// Get the pending snapshot file path (for new/changed snapshots)
    pub fn pending_path(&self, name: &str) -> PathBuf {
        let file_stem = self
            .test_file_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        self.snapshots_dir()
            .join(format!("{file_stem}__{name}.snap.new"))
    }

    /// Read and parse a snapshot file with full metadata
    pub async fn read_snapshot(&self, name: &str) -> anyhow::Result<Option<Snapshot>> {
        let path = self.snapshot_path(name);
        match fs::read_to_string(&path).await {
            Ok(contents) => Ok(Some(Snapshot::parse(&contents)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Write a snapshot file
    pub async fn write_snapshot(
        &self,
        name: &str,
        sql: &str,
        content: &str,
        info: &SnapshotInfo,
    ) -> anyhow::Result<()> {
        let path = self.snapshot_path(name);

        // Ensure snapshots directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let snapshot = create_snapshot(&self.test_file_path, sql, content, info);
        let formatted = snapshot.to_string()?;

        fs::write(&path, formatted).await?;
        Ok(())
    }

    /// Write a pending snapshot file (.snap.new)
    pub async fn write_pending(
        &self,
        name: &str,
        sql: &str,
        content: &str,
        info: &SnapshotInfo,
    ) -> anyhow::Result<()> {
        let path = self.pending_path(name);

        // Ensure snapshots directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let snapshot = create_snapshot(&self.test_file_path, sql, content, info);
        let formatted = snapshot.to_string()?;

        fs::write(&path, formatted).await?;
        Ok(())
    }

    /// Remove a pending snapshot file (.snap.new) if it exists.
    /// Called when a snapshot is accepted (written to .snap).
    pub async fn remove_pending(&self, name: &str) -> anyhow::Result<()> {
        let path = self.pending_path(name);
        if path.exists() {
            fs::remove_file(&path).await?;
        }
        Ok(())
    }

    /// Compare actual output against stored snapshot.
    ///
    /// Behavior depends on the update mode:
    /// - `Always`: Write directly to `.snap` files
    /// - `New`: Write to `.snap.new` files for review
    /// - `No`: Don't write any files, just report results
    pub async fn compare(
        &self,
        name: &str,
        sql: &str,
        actual: &str,
        info: &SnapshotInfo,
    ) -> SnapshotResult {
        // Try to read existing snapshot
        match self.read_snapshot(name).await {
            Ok(Some(snapshot)) => {
                let expected = &snapshot.content;

                if expected.trim() == actual.trim() {
                    // In Always mode, clean up any stale .snap.new file
                    if self.update_mode == SnapshotUpdateMode::Always {
                        let _ = self.remove_pending(name).await;
                    }
                    SnapshotResult::Match
                } else {
                    // Snapshot mismatch - behavior depends on mode
                    match self.update_mode {
                        SnapshotUpdateMode::Always => {
                            // Update the snapshot directly
                            if let Err(e) = self.write_snapshot(name, sql, actual, info).await {
                                return SnapshotResult::Error {
                                    msg: format!("Failed to update snapshot: {e}"),
                                };
                            }
                            // Clean up any pending .snap.new file
                            let _ = self.remove_pending(name).await;
                            SnapshotResult::Updated {
                                old: expected.clone(),
                                new: actual.to_string(),
                            }
                        }
                        SnapshotUpdateMode::New => {
                            // Write to .snap.new for review, then report mismatch
                            let _ = self.write_pending(name, sql, actual, info).await;
                            let diff = generate_diff(expected, actual);
                            SnapshotResult::Mismatch {
                                expected: expected.clone(),
                                actual: actual.to_string(),
                                diff,
                            }
                        }
                        SnapshotUpdateMode::No | SnapshotUpdateMode::Auto => {
                            // Auto should already be resolved, but handle it as No
                            let diff = generate_diff(expected, actual);
                            SnapshotResult::Mismatch {
                                expected: expected.clone(),
                                actual: actual.to_string(),
                                diff,
                            }
                        }
                    }
                }
            }
            Ok(None) => {
                // No existing snapshot - behavior depends on mode
                match self.update_mode {
                    SnapshotUpdateMode::Always => {
                        // Create the snapshot directly
                        if let Err(e) = self.write_snapshot(name, sql, actual, info).await {
                            return SnapshotResult::Error {
                                msg: format!("Failed to create snapshot: {e}"),
                            };
                        }
                        // Clean up any pending .snap.new file
                        let _ = self.remove_pending(name).await;
                        SnapshotResult::New {
                            content: actual.to_string(),
                        }
                    }
                    SnapshotUpdateMode::New => {
                        // Write to .snap.new for review
                        let _ = self.write_pending(name, sql, actual, info).await;
                        SnapshotResult::New {
                            content: actual.to_string(),
                        }
                    }
                    SnapshotUpdateMode::No | SnapshotUpdateMode::Auto => {
                        // Don't write anything, just report new
                        SnapshotResult::New {
                            content: actual.to_string(),
                        }
                    }
                }
            }
            Err(e) => SnapshotResult::Error {
                msg: format!("Failed to parse snapshot: {e}"),
            },
        }
    }

    /// Get the current update mode
    pub fn update_mode(&self) -> SnapshotUpdateMode {
        self.update_mode
    }

    /// Check if we're in a mode that writes snapshots directly (Always mode)
    pub fn is_update_mode(&self) -> bool {
        self.update_mode == SnapshotUpdateMode::Always
    }
}

/// Find all pending snapshot files (`.snap.new`) in a directory.
///
/// This is useful for `--check` mode to detect stale pending snapshots.
pub async fn find_pending_snapshots(dir: &Path) -> Vec<PathBuf> {
    let mut pending = Vec::new();
    let snapshots_dir = dir.join("snapshots");

    if !snapshots_dir.exists() {
        return pending;
    }

    if let Ok(mut entries) = fs::read_dir(&snapshots_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "new") {
                // Check if it's a .snap.new file
                if let Some(stem) = path.file_stem() {
                    if stem.to_string_lossy().ends_with(".snap") {
                        pending.push(path);
                    }
                }
            }
        }
    }

    pending
}

/// Find all pending snapshot files recursively in a directory tree.
pub async fn find_all_pending_snapshots(base_dir: &Path) -> Vec<PathBuf> {
    let mut pending = Vec::new();

    // Helper to recursively scan
    async fn scan_dir(dir: &Path, pending: &mut Vec<PathBuf>) {
        // Check for snapshots directory
        let snapshots_dir = dir.join("snapshots");
        if snapshots_dir.exists() {
            if let Ok(mut entries) = fs::read_dir(&snapshots_dir).await {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "new") {
                        if let Some(stem) = path.file_stem() {
                            if stem.to_string_lossy().ends_with(".snap") {
                                pending.push(path);
                            }
                        }
                    }
                }
            }
        }

        // Recurse into subdirectories
        if let Ok(mut entries) = fs::read_dir(dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if path.is_dir() && path.file_name().is_some_and(|n| n != "snapshots") {
                    Box::pin(scan_dir(&path, pending)).await;
                }
            }
        }
    }

    Box::pin(scan_dir(base_dir, &mut pending)).await;
    pending
}

/// Create a Snapshot from components
fn create_snapshot(source_path: &Path, sql: &str, content: &str, info: &SnapshotInfo) -> Snapshot {
    let source = source_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    // Extract tables from SQL
    let tables: Vec<String> = extract_tables(sql).into_iter().collect();

    // Detect statement type
    let statement_type = detect_statement_type(sql).to_string();

    let metadata = SnapshotMetadata {
        source,
        expression: sql.to_string(),
        info: SnapshotInfo {
            statement_type,
            tables,
            setup_blocks: info.setup_blocks.clone(),
            database: info.database.clone(),
            line: info.line,
        },
    };

    Snapshot {
        metadata,
        content: content.to_string(),
    }
}

// Regex patterns for extracting table names from SQL
static TABLE_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        // FROM clause
        Regex::new(r"(?i)\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // JOIN clause
        Regex::new(r"(?i)\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // INSERT INTO
        Regex::new(r"(?i)\bINSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // UPDATE
        Regex::new(r"(?i)\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // DELETE FROM
        Regex::new(r"(?i)\bDELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // CREATE TABLE
        Regex::new(r"(?i)\bCREATE\s+(?:TEMP\s+|TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // DROP TABLE
        Regex::new(r"(?i)\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // ALTER TABLE
        Regex::new(r"(?i)\bALTER\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // CREATE INDEX ... ON
        Regex::new(r"(?i)\bON\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
    ]
});

/// Extract table names referenced in a SQL query
fn extract_tables(sql: &str) -> BTreeSet<String> {
    let mut tables = BTreeSet::new();

    for pattern in TABLE_PATTERNS.iter() {
        for cap in pattern.captures_iter(sql) {
            if let Some(table) = cap.get(1) {
                let table_name = table.as_str().to_lowercase();
                if !is_sql_keyword(&table_name) {
                    tables.insert(table_name);
                }
            }
        }
    }

    tables
}

/// Check if a string is a SQL keyword
fn is_sql_keyword(s: &str) -> bool {
    matches!(
        s.to_uppercase().as_str(),
        "SELECT"
            | "FROM"
            | "WHERE"
            | "AND"
            | "OR"
            | "NOT"
            | "IN"
            | "EXISTS"
            | "BETWEEN"
            | "LIKE"
            | "IS"
            | "NULL"
            | "TRUE"
            | "FALSE"
            | "AS"
            | "ON"
            | "USING"
            | "GROUP"
            | "ORDER"
            | "BY"
            | "HAVING"
            | "LIMIT"
            | "OFFSET"
            | "UNION"
            | "INTERSECT"
            | "EXCEPT"
            | "ALL"
            | "DISTINCT"
            | "VALUES"
            | "SET"
            | "DEFAULT"
    )
}

/// Detect the type of SQL statement
fn detect_statement_type(sql: &str) -> &'static str {
    let sql_upper = sql.trim().to_uppercase();
    if sql_upper.starts_with("SELECT") {
        "SELECT"
    } else if sql_upper.starts_with("INSERT") {
        "INSERT"
    } else if sql_upper.starts_with("UPDATE") {
        "UPDATE"
    } else if sql_upper.starts_with("DELETE") {
        "DELETE"
    } else if sql_upper.starts_with("CREATE TABLE") {
        "CREATE TABLE"
    } else if sql_upper.starts_with("CREATE INDEX") {
        "CREATE INDEX"
    } else if sql_upper.starts_with("CREATE") {
        "CREATE"
    } else if sql_upper.starts_with("DROP") {
        "DROP"
    } else if sql_upper.starts_with("ALTER") {
        "ALTER"
    } else if sql_upper.starts_with("WITH") {
        "WITH (CTE)"
    } else {
        "OTHER"
    }
}

/// Generate a unified diff between expected and actual content
fn generate_diff(expected: &str, actual: &str) -> String {
    use similar::{ChangeTag, TextDiff};

    let diff = TextDiff::from_lines(expected, actual);
    let mut result = String::new();

    for change in diff.iter_all_changes() {
        let sign = match change.tag() {
            ChangeTag::Delete => "-",
            ChangeTag::Insert => "+",
            ChangeTag::Equal => " ",
        };
        result.push_str(sign);
        result.push_str(change.value());
        if !change.value().ends_with('\n') {
            result.push('\n');
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_snapshot_path() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("my-test.sqltest");
        let manager = SnapshotManager::new(&test_file, SnapshotUpdateMode::No);

        assert_eq!(
            manager.snapshot_path("select-users"),
            temp.path().join("snapshots/my-test__select-users.snap")
        );
    }

    #[test]
    fn test_pending_path() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("my-test.sqltest");
        let manager = SnapshotManager::new(&test_file, SnapshotUpdateMode::No);

        assert_eq!(
            manager.pending_path("select-users"),
            temp.path().join("snapshots/my-test__select-users.snap.new")
        );
    }

    #[test]
    fn test_create_and_parse_snapshot() {
        let path = PathBuf::from("tests/my-test.sqltest");
        let sql = "SELECT * FROM users";
        let content = "addr  opcode\n0     Init";
        let info = SnapshotInfo::new()
            .with_setups(vec!["schema".to_string()])
            .with_database(":memory:".to_string());

        let snapshot = create_snapshot(&path, sql, content, &info);
        let serialized = snapshot.to_string().unwrap();

        // Parse it back
        let parsed = Snapshot::parse(&serialized).unwrap();

        assert_eq!(parsed.metadata.source, "my-test.sqltest");
        assert_eq!(parsed.metadata.expression, "SELECT * FROM users");
        assert_eq!(parsed.metadata.info.statement_type, "SELECT");
        assert!(parsed.metadata.info.tables.contains(&"users".to_string()));
        assert_eq!(parsed.metadata.info.setup_blocks, vec!["schema"]);
        assert_eq!(parsed.metadata.info.database, Some(":memory:".to_string()));
        assert_eq!(parsed.content, content);
    }

    #[test]
    fn test_extract_tables() {
        // Simple SELECT
        let tables = extract_tables("SELECT * FROM users");
        assert!(tables.contains("users"));

        // JOIN
        let tables = extract_tables("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));

        // INSERT
        let tables = extract_tables("INSERT INTO products VALUES (1, 'test')");
        assert!(tables.contains("products"));

        // UPDATE
        let tables = extract_tables("UPDATE inventory SET qty = 10 WHERE id = 1");
        assert!(tables.contains("inventory"));

        // Subquery
        let tables = extract_tables("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)");
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));
    }

    #[test]
    fn test_detect_statement_type() {
        assert_eq!(detect_statement_type("SELECT * FROM users"), "SELECT");
        assert_eq!(
            detect_statement_type("INSERT INTO users VALUES (1)"),
            "INSERT"
        );
        assert_eq!(
            detect_statement_type("UPDATE users SET name = 'x'"),
            "UPDATE"
        );
        assert_eq!(detect_statement_type("DELETE FROM users"), "DELETE");
        assert_eq!(
            detect_statement_type("CREATE TABLE foo (id INT)"),
            "CREATE TABLE"
        );
        assert_eq!(
            detect_statement_type("WITH cte AS (SELECT 1) SELECT * FROM cte"),
            "WITH (CTE)"
        );
    }

    #[tokio::test]
    async fn test_compare_match() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("test.sqltest");
        let info = SnapshotInfo::new();

        // Create snapshot using Always mode (writes directly)
        let manager = SnapshotManager::new(&test_file, SnapshotUpdateMode::Always);
        manager
            .write_snapshot("snap1", "SELECT 1", "content here", &info)
            .await
            .unwrap();

        // Compare using No mode (doesn't write files)
        let manager2 = SnapshotManager::new(&test_file, SnapshotUpdateMode::No);
        match manager2
            .compare("snap1", "SELECT 1", "content here", &info)
            .await
        {
            SnapshotResult::Match => {}
            other => panic!("Expected Match, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_compare_mismatch() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("test.sqltest");
        let info = SnapshotInfo::new();

        // Create snapshot using Always mode
        let manager = SnapshotManager::new(&test_file, SnapshotUpdateMode::Always);
        manager
            .write_snapshot("snap1", "SELECT 1", "original", &info)
            .await
            .unwrap();

        // Compare with different content using No mode (won't write .snap.new)
        let manager2 = SnapshotManager::new(&test_file, SnapshotUpdateMode::No);
        match manager2
            .compare("snap1", "SELECT 1", "modified", &info)
            .await
        {
            SnapshotResult::Mismatch {
                expected, actual, ..
            } => {
                assert_eq!(expected, "original");
                assert_eq!(actual, "modified");
            }
            other => panic!("Expected Mismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_compare_mismatch_writes_pending_in_new_mode() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("test.sqltest");
        let info = SnapshotInfo::new();

        // Create snapshot using Always mode
        let manager = SnapshotManager::new(&test_file, SnapshotUpdateMode::Always);
        manager
            .write_snapshot("snap1", "SELECT 1", "original", &info)
            .await
            .unwrap();

        // Compare with different content using New mode (writes .snap.new)
        let manager2 = SnapshotManager::new(&test_file, SnapshotUpdateMode::New);
        match manager2
            .compare("snap1", "SELECT 1", "modified", &info)
            .await
        {
            SnapshotResult::Mismatch {
                expected, actual, ..
            } => {
                assert_eq!(expected, "original");
                assert_eq!(actual, "modified");
            }
            other => panic!("Expected Mismatch, got {other:?}"),
        }

        // Check that .snap.new was created
        assert!(manager2.pending_path("snap1").exists());
    }

    #[tokio::test]
    async fn test_compare_new_in_new_mode() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("test.sqltest");
        let info = SnapshotInfo::new();

        // Use New mode - should write .snap.new
        let manager = SnapshotManager::new(&test_file, SnapshotUpdateMode::New);
        match manager
            .compare("new-snap", "SELECT 1", "new content", &info)
            .await
        {
            SnapshotResult::New { content } => {
                assert_eq!(content, "new content");
            }
            other => panic!("Expected New, got {other:?}"),
        }

        // Check that .snap.new was created
        assert!(manager.pending_path("new-snap").exists());
        // Check that .snap was NOT created
        assert!(!manager.snapshot_path("new-snap").exists());
    }

    #[tokio::test]
    async fn test_compare_new_in_no_mode() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("test.sqltest");
        let info = SnapshotInfo::new();

        // Use No mode - should NOT write any files
        let manager = SnapshotManager::new(&test_file, SnapshotUpdateMode::No);
        match manager
            .compare("new-snap", "SELECT 1", "new content", &info)
            .await
        {
            SnapshotResult::New { content } => {
                assert_eq!(content, "new content");
            }
            other => panic!("Expected New, got {other:?}"),
        }

        // Check that no files were created
        assert!(!manager.pending_path("new-snap").exists());
        assert!(!manager.snapshot_path("new-snap").exists());
    }

    #[tokio::test]
    async fn test_compare_new_in_always_mode() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("test.sqltest");
        let info = SnapshotInfo::new();

        // Use Always mode - should write .snap directly
        let manager = SnapshotManager::new(&test_file, SnapshotUpdateMode::Always);
        match manager
            .compare("new-snap", "SELECT 1", "new content", &info)
            .await
        {
            SnapshotResult::New { content } => {
                assert_eq!(content, "new content");
            }
            other => panic!("Expected New, got {other:?}"),
        }

        // Check that .snap was created (not .snap.new)
        assert!(manager.snapshot_path("new-snap").exists());
        assert!(!manager.pending_path("new-snap").exists());
    }
}
