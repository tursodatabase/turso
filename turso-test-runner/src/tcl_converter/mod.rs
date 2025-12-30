//! Converter from Tcl .test files to .sqltest format
//!
//! This module provides an error-recoverable parser that converts the legacy
//! Tcl-based test format to the new `.sqltest` DSL format.

mod parser;

pub use parser::{
    ConversionResult, ConversionWarning, TclParser, TclTest, TclTestKind, WarningKind,
};

use std::fmt::Write;

/// Convert a Tcl test file content to the .sqltest format
pub fn convert(content: &str, source_file: &str) -> ConversionResult {
    let parser = TclParser::new(content, source_file);
    parser.parse()
}

/// Generate the .sqltest output from parsed tests
pub fn generate_sqltest(result: &ConversionResult) -> String {
    let mut output = String::new();

    // Collect all unique databases used by tests
    let mut databases: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut needs_memory = false;
    let mut has_ddl = false;

    for test in &result.tests {
        if test.has_ddl {
            has_ddl = true;
        }
        match &test.db {
            Some(db) if db == ":memory:" => {
                needs_memory = true;
            }
            Some(db) => {
                // Specific database path
                databases.insert(db.clone());
            }
            None => {
                // Uses default test_dbs
            }
        }
    }

    // Determine database configuration
    if has_ddl || needs_memory {
        // Need writable database
        writeln!(output, "@database :memory:").unwrap();
    } else if !databases.is_empty() {
        // Use specific databases mentioned in tests (readonly)
        for db in &databases {
            writeln!(output, "@database {} readonly", db).unwrap();
        }
    } else {
        // Default: uses test_dbs, output readonly
        writeln!(output, "@database testing/testing.db readonly").unwrap();
    }

    writeln!(output).unwrap();

    // Write test cases
    for test in &result.tests {
        // Write comments before the test
        for comment in &test.comments {
            writeln!(output, "{}", comment).unwrap();
        }

        // Write test header
        writeln!(output, "test {} {{", test.name).unwrap();

        // Write SQL (preserving structure including comments)
        for line in test.sql.lines() {
            if line.trim().is_empty() {
                writeln!(output).unwrap();
            } else {
                writeln!(output, "    {}", line.trim()).unwrap();
            }
        }

        writeln!(output, "}}").unwrap();

        // Write expectation
        match &test.kind {
            TclTestKind::Exact(expected) => {
                // Check if any line has significant trailing whitespace
                let needs_raw = expected.lines().any(|line| {
                    let trimmed = line.trim_end();
                    trimmed != line && !trimmed.is_empty()
                });

                if needs_raw {
                    // Raw mode: no indentation, preserves whitespace exactly
                    writeln!(output, "expect raw {{").unwrap();
                    for line in expected.lines() {
                        if !line.is_empty() {
                            writeln!(output, "{}", line).unwrap();
                        }
                    }
                } else {
                    writeln!(output, "expect {{").unwrap();
                    for line in expected.lines() {
                        if !line.is_empty() {
                            writeln!(output, "    {}", line).unwrap();
                        }
                    }
                }
                writeln!(output, "}}").unwrap();
            }
            TclTestKind::Pattern(pattern) => {
                writeln!(output, "expect pattern {{").unwrap();
                writeln!(output, "    {}", pattern.trim()).unwrap();
                writeln!(output, "}}").unwrap();
            }
            TclTestKind::Error(pattern) => {
                writeln!(output, "expect error {{").unwrap();
                if let Some(p) = pattern {
                    writeln!(output, "    {}", p.trim()).unwrap();
                }
                writeln!(output, "}}").unwrap();
            }
            TclTestKind::AnyError => {
                writeln!(output, "expect error {{").unwrap();
                writeln!(output, "}}").unwrap();
            }
        }

        writeln!(output).unwrap();
    }

    // Write remaining comments
    for comment in &result.pending_comments {
        writeln!(output, "{}", comment).unwrap();
    }

    output
}
