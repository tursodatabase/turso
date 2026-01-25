//! Snapshot testing support for SQL EXPLAIN output.
//!
//! This module provides insta-compatible snapshot testing capabilities
//! for verifying SQL EXPLAIN output remains consistent.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

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

    /// Read an existing snapshot file
    pub fn read_snapshot(&self, name: &str) -> Option<String> {
        let path = self.snapshot_path(name);
        fs::read_to_string(&path).ok().map(|content| {
            // Parse insta format: skip YAML frontmatter
            extract_snapshot_content(&content)
        })
    }

    /// Write a snapshot file (either .snap or .snap.new depending on mode)
    pub fn write_snapshot(&self, name: &str, content: &str) -> io::Result<()> {
        let path = self.snapshot_path(name);

        // Ensure snapshots directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let formatted = format_snapshot(&self.test_file_path, name, content);
        fs::write(&path, formatted)
    }

    /// Write a pending snapshot file (.snap.new)
    pub fn write_pending(&self, name: &str, content: &str) -> io::Result<()> {
        let path = self.pending_path(name);

        // Ensure snapshots directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let formatted = format_snapshot(&self.test_file_path, name, content);
        fs::write(&path, formatted)
    }

    /// Compare actual output against stored snapshot
    pub fn compare(&self, name: &str, actual: &str) -> SnapshotResult {
        let existing = self.read_snapshot(name);

        match existing {
            Some(expected) => {
                if expected.trim() == actual.trim() {
                    SnapshotResult::Match
                } else if self.update_mode {
                    // Update the snapshot
                    if let Err(e) = self.write_snapshot(name, actual) {
                        // Return mismatch with error info if we couldn't update
                        return SnapshotResult::Mismatch {
                            expected: expected.clone(),
                            actual: actual.to_string(),
                            diff: format!("Failed to update snapshot: {e}"),
                        };
                    }
                    SnapshotResult::Updated {
                        old: expected,
                        new: actual.to_string(),
                    }
                } else {
                    // Generate diff
                    let diff = generate_diff(&expected, actual);
                    SnapshotResult::Mismatch {
                        expected,
                        actual: actual.to_string(),
                        diff,
                    }
                }
            }
            None => {
                // No existing snapshot
                if self.update_mode {
                    // Create the snapshot directly
                    if let Err(e) = self.write_snapshot(name, actual) {
                        return SnapshotResult::New {
                            content: format!("Failed to create snapshot: {e}"),
                        };
                    }
                    SnapshotResult::New {
                        content: actual.to_string(),
                    }
                } else {
                    // Write to .snap.new for review
                    let _ = self.write_pending(name, actual);
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

/// Format snapshot content in insta-compatible format with YAML frontmatter
fn format_snapshot(source_path: &Path, snapshot_name: &str, content: &str) -> String {
    let source = source_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");

    format!(
        r#"---
source: {source}
snapshot: {snapshot_name}
---
{content}
"#
    )
}

/// Extract the actual content from an insta-format snapshot file
fn extract_snapshot_content(file_content: &str) -> String {
    // Skip YAML frontmatter (content between --- markers)
    let mut lines = file_content.lines();
    let mut in_frontmatter = false;
    let mut found_start = false;
    let mut content_lines = Vec::new();

    for line in lines.by_ref() {
        if line == "---" {
            if !found_start {
                found_start = true;
                in_frontmatter = true;
            } else if in_frontmatter {
                in_frontmatter = false;
            }
            continue;
        }

        if !in_frontmatter && found_start {
            content_lines.push(line);
        }
    }

    content_lines.join("\n")
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
        let content = "addr  opcode\n0     Init";
        let formatted = format_snapshot(&path, "test-name", content);

        assert!(formatted.contains("source: my-test.sqltest"));
        assert!(formatted.contains("snapshot: test-name"));
        assert!(formatted.contains("addr  opcode"));
    }

    #[test]
    fn test_extract_snapshot_content() {
        let file = r#"---
source: test.sqltest
snapshot: my-snap
---
addr  opcode
0     Init
"#;

        let content = extract_snapshot_content(file);
        assert_eq!(content, "addr  opcode\n0     Init");
    }

    #[test]
    fn test_compare_match() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("test.sqltest");

        // Create snapshot
        let manager = SnapshotManager::new(&test_file, true);
        manager.write_snapshot("snap1", "content here").unwrap();

        // Compare
        let manager2 = SnapshotManager::new(&test_file, false);
        match manager2.compare("snap1", "content here") {
            SnapshotResult::Match => {}
            other => panic!("Expected Match, got {other:?}"),
        }
    }

    #[test]
    fn test_compare_mismatch() {
        let temp = TempDir::new().unwrap();
        let test_file = temp.path().join("test.sqltest");

        // Create snapshot
        let manager = SnapshotManager::new(&test_file, true);
        manager.write_snapshot("snap1", "original").unwrap();

        // Compare with different content
        let manager2 = SnapshotManager::new(&test_file, false);
        match manager2.compare("snap1", "modified") {
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

        let manager = SnapshotManager::new(&test_file, false);
        match manager.compare("new-snap", "new content") {
            SnapshotResult::New { content } => {
                assert_eq!(content, "new content");
            }
            other => panic!("Expected New, got {other:?}"),
        }

        // Check that .snap.new was created
        assert!(manager.pending_path("new-snap").exists());
    }
}
