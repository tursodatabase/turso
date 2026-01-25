//! Snapshot testing support for SQL EXPLAIN output.
//!
//! This module provides insta-compatible snapshot testing capabilities
//! for verifying SQL EXPLAIN output remains consistent.
//!
//! Uses the `insta` crate for reading snapshots and the `similar` crate for diffs.

use insta::Snapshot;
use insta::internals::SnapshotContents;
use regex::Regex;
use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

/// Additional metadata for a snapshot
#[derive(Debug, Clone, Default)]
pub struct SnapshotInfo {
    /// Setup blocks that were executed before this snapshot
    pub setups: Vec<String>,
    /// Database type (e.g., ":memory:", "file.db")
    pub database: Option<String>,
    /// Line number in the test file where this snapshot is defined
    pub line_number: Option<u32>,
}

impl SnapshotInfo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_setups(mut self, setups: Vec<String>) -> Self {
        self.setups = setups;
        self
    }

    pub fn with_database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }

    pub fn with_line_number(mut self, line: u32) -> Self {
        self.line_number = Some(line);
        self
    }
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
    New {
        content: String,
    },
    /// Snapshot was updated (when in update mode)
    Updated {
        old: String,
        new: String,
    },
    Error {
        msg: String,
    },
}

/// Manages snapshot files for a test file
pub struct SnapshotManager {
    /// Path to the test file (used to derive snapshot directory)
    test_file_path: PathBuf,
    /// Whether to automatically update snapshots
    update_mode: bool,
}

impl SnapshotManager {
    /// Create a new snapshot manager for a test file
    pub fn new(test_file_path: &Path, update: bool) -> Self {
        Self {
            test_file_path: test_file_path.to_path_buf(),
            update_mode: update,
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

    /// Read an existing snapshot file using insta's Snapshot type
    pub fn read_snapshot(&self, name: &str) -> Option<String> {
        let path = self.snapshot_path(name);

        // Use insta's Snapshot::from_file to load the snapshot
        Snapshot::from_file(&path).ok().map(|snapshot| {
            // Extract text content from SnapshotContents
            snapshot_contents_to_string(snapshot.contents())
        })
    }

    /// Write a snapshot file in insta-compatible format
    pub fn write_snapshot(
        &self,
        name: &str,
        sql: &str,
        content: &str,
        info: &SnapshotInfo,
    ) -> io::Result<()> {
        let path = self.snapshot_path(name);

        // Ensure snapshots directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let formatted = format_snapshot(&self.test_file_path, name, sql, content, info);
        fs::write(&path, formatted)
    }

    /// Write a pending snapshot file (.snap.new) in insta-compatible format
    pub fn write_pending(
        &self,
        name: &str,
        sql: &str,
        content: &str,
        info: &SnapshotInfo,
    ) -> io::Result<()> {
        let path = self.pending_path(name);

        // Ensure snapshots directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let formatted = format_snapshot(&self.test_file_path, name, sql, content, info);
        fs::write(&path, formatted)
    }

    /// Compare actual output against stored snapshot
    pub fn compare(
        &self,
        name: &str,
        sql: &str,
        actual: &str,
        info: &SnapshotInfo,
    ) -> SnapshotResult {
        let path = self.snapshot_path(name);

        // Try to load existing snapshot using insta
        match Snapshot::from_file(&path) {
            Ok(snapshot) => {
                let expected = snapshot_contents_to_string(snapshot.contents());

                if expected.trim() == actual.trim() {
                    SnapshotResult::Match
                } else if self.update_mode {
                    // Update the snapshot
                    if let Err(e) = self.write_snapshot(name, sql, actual, info) {
                        return SnapshotResult::Error {
                            msg: format!("Failed to update snapshot: {e}"),
                        };
                    }
                    SnapshotResult::Updated {
                        old: expected,
                        new: actual.to_string(),
                    }
                } else {
                    // Generate diff using similar
                    let diff = generate_diff(&expected, actual);
                    SnapshotResult::Mismatch {
                        expected,
                        actual: actual.to_string(),
                        diff,
                    }
                }
            }
            Err(_) => {
                // No existing snapshot
                if self.update_mode {
                    // Create the snapshot directly
                    if let Err(e) = self.write_snapshot(name, sql, actual, info) {
                        return SnapshotResult::Error {
                            msg: format!("Failed to create snapshot: {e}"),
                        };
                    }
                    SnapshotResult::New {
                        content: actual.to_string(),
                    }
                } else {
                    // Write to .snap.new for review
                    let _ = self.write_pending(name, sql, actual, info);
                    SnapshotResult::New {
                        content: actual.to_string(),
                    }
                }
            }
        }
    }

    /// Check if update mode is enabled
    pub fn is_update_mode(&self) -> bool {
        self.update_mode
    }
}

/// Extract string content from insta's SnapshotContents
fn snapshot_contents_to_string(contents: &SnapshotContents) -> String {
    match contents {
        SnapshotContents::Text(text) => text.to_string(),
        SnapshotContents::Binary(bytes) => {
            // Convert binary to string (lossy) - binary snapshots are rare for EXPLAIN output
            String::from_utf8_lossy(bytes).to_string()
        }
    }
}

/// Format snapshot content in insta-compatible format with YAML frontmatter
fn format_snapshot(
    source_path: &Path,
    snapshot_name: &str,
    sql: &str,
    content: &str,
    info: &SnapshotInfo,
) -> String {
    let source = source_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");

    // Format SQL for YAML expression field
    let expression_yaml = format_yaml_string(sql);

    // Extract tables referenced in the SQL
    let tables = extract_tables(sql);

    // Determine the SQL statement type
    let statement_type = detect_statement_type(sql);

    // Build the info section
    let info_yaml = format_info_section(&tables, &statement_type, info);

    // Build the YAML frontmatter with insta-compatible fields
    format!(
        "---
source: {source}
expression: {expression_yaml}
snapshot_name: {snapshot_name}
{info_yaml}---
{content}
"
    )
}

/// Format the info section as YAML
fn format_info_section(
    tables: &BTreeSet<String>,
    statement_type: &str,
    info: &SnapshotInfo,
) -> String {
    let mut lines = vec!["info:".to_string()];

    // Statement type
    lines.push(format!("  statement_type: {statement_type}"));

    // Tables referenced
    if !tables.is_empty() {
        lines.push("  tables:".to_string());
        for table in tables {
            lines.push(format!("    - {table}"));
        }
    }

    // Setup blocks used
    if !info.setups.is_empty() {
        lines.push("  setup_blocks:".to_string());
        for setup in &info.setups {
            lines.push(format!("    - {setup}"));
        }
    }

    // Database configuration
    if let Some(ref db) = info.database {
        lines.push(format!("  database: \"{db}\""));
    }

    // Line number in test file
    if let Some(line) = info.line_number {
        lines.push(format!("  line: {line}"));
    }

    lines.push(String::new()); // trailing newline before ---
    lines.join("\n")
}

/// Format a string for YAML, using block scalar for complex strings
fn format_yaml_string(s: &str) -> String {
    // Use YAML literal block scalar (|) for multi-line or strings with special chars
    if s.contains('\n') || s.contains(':') || s.contains('"') || s.contains('\'') {
        let indented = s
            .lines()
            .map(|line| format!("  {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        format!("|\n{indented}")
    } else {
        // Simple single-line strings can be quoted
        format!("\"{s}\"")
    }
}

// Regex patterns for extracting table names from SQL
static TABLE_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        // FROM clause: FROM table, FROM schema.table
        Regex::new(r"(?i)\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // JOIN clause: JOIN table, LEFT JOIN table, etc.
        Regex::new(r"(?i)\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // INSERT INTO table
        Regex::new(r"(?i)\bINSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // UPDATE table
        Regex::new(r"(?i)\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // DELETE FROM table
        Regex::new(r"(?i)\bDELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // CREATE TABLE table
        Regex::new(r"(?i)\bCREATE\s+(?:TEMP\s+|TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // DROP TABLE table
        Regex::new(r"(?i)\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // ALTER TABLE table
        Regex::new(r"(?i)\bALTER\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap(),
        // CREATE INDEX ... ON table
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
                // Filter out SQL keywords that might be matched
                if !is_sql_keyword(&table_name) {
                    tables.insert(table_name);
                }
            }
        }
    }

    tables
}

/// Check if a string is a SQL keyword (to filter false positives)
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

/// Generate a unified diff between expected and actual content using similar
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
        let manager = SnapshotManager::new(&test_file, false);

        assert_eq!(
            manager.snapshot_path("select-users"),
            temp.path().join("snapshots/my-test__select-users.snap")
        );
    }

    #[test]
    fn test_pending_path() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("my-test.sqltest");
        let manager = SnapshotManager::new(&test_file, false);

        assert_eq!(
            manager.pending_path("select-users"),
            temp.path().join("snapshots/my-test__select-users.snap.new")
        );
    }

    #[test]
    fn test_format_snapshot() {
        let path = PathBuf::from("tests/my-test.sqltest");
        let sql = "SELECT * FROM users";
        let content = "addr  opcode\n0     Init";
        let info = SnapshotInfo::new();
        let formatted = format_snapshot(&path, "test-name", sql, content, &info);

        assert!(formatted.contains("source: my-test.sqltest"));
        assert!(formatted.contains("expression: \"SELECT * FROM users\""));
        assert!(formatted.contains("snapshot_name: test-name"));
        assert!(formatted.contains("statement_type: SELECT"));
        assert!(formatted.contains("tables:"));
        assert!(formatted.contains("- users"));
        assert!(formatted.contains("addr  opcode"));
    }

    #[test]
    fn test_format_snapshot_multiline_sql() {
        let path = PathBuf::from("tests/my-test.sqltest");
        let sql = "SELECT *\nFROM users\nWHERE id = 1";
        let content = "0|Init";
        let info = SnapshotInfo::new();
        let formatted = format_snapshot(&path, "test-name", sql, content, &info);

        // Multi-line SQL should use YAML block scalar in expression
        assert!(formatted.contains("expression: |"));
        assert!(formatted.contains("  SELECT *"));
        assert!(formatted.contains("  FROM users"));
        assert!(formatted.contains("snapshot_name: test-name"));
    }

    #[test]
    fn test_format_snapshot_with_info() {
        let path = PathBuf::from("tests/my-test.sqltest");
        let sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";
        let content = "0|Init";
        let info = SnapshotInfo::new()
            .with_setups(vec!["schema".to_string(), "data".to_string()])
            .with_database(":memory:".to_string())
            .with_line_number(42);
        let formatted = format_snapshot(&path, "test-name", sql, content, &info);

        // Check info section
        assert!(formatted.contains("info:"));
        assert!(formatted.contains("statement_type: SELECT"));
        assert!(formatted.contains("tables:"));
        assert!(formatted.contains("- users"));
        assert!(formatted.contains("- orders"));
        assert!(formatted.contains("setup_blocks:"));
        assert!(formatted.contains("- schema"));
        assert!(formatted.contains("- data"));
        assert!(formatted.contains("database: \":memory:\""));
        assert!(formatted.contains("line: 42"));
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
        let tables =
            extract_tables("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)");
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

    #[test]
    fn test_compare_match() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("test.sqltest");
        let info = SnapshotInfo::new();

        // Create snapshot
        let manager = SnapshotManager::new(&test_file, true);
        manager
            .write_snapshot("snap1", "SELECT 1", "content here", &info)
            .unwrap();

        // Compare
        let manager2 = SnapshotManager::new(&test_file, false);
        match manager2.compare("snap1", "SELECT 1", "content here", &info) {
            SnapshotResult::Match => {}
            other => panic!("Expected Match, got {other:?}"),
        }
    }

    #[test]
    fn test_compare_mismatch() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("test.sqltest");
        let info = SnapshotInfo::new();

        // Create snapshot
        let manager = SnapshotManager::new(&test_file, true);
        manager
            .write_snapshot("snap1", "SELECT 1", "original", &info)
            .unwrap();

        // Compare with different content
        let manager2 = SnapshotManager::new(&test_file, false);
        match manager2.compare("snap1", "SELECT 1", "modified", &info) {
            SnapshotResult::Mismatch {
                expected, actual, ..
            } => {
                assert_eq!(expected, "original");
                assert_eq!(actual, "modified");
            }
            other => panic!("Expected Mismatch, got {other:?}"),
        }
    }

    #[test]
    fn test_compare_new() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("test.sqltest");
        let info = SnapshotInfo::new();

        let manager = SnapshotManager::new(&test_file, false);
        match manager.compare("new-snap", "SELECT 1", "new content", &info) {
            SnapshotResult::New { content } => {
                assert_eq!(content, "new content");
            }
            other => panic!("Expected New, got {other:?}"),
        }

        // Check that .snap.new was created
        assert!(manager.pending_path("new-snap").exists());
    }
}
