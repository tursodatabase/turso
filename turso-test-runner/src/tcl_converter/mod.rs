//! Converter from Tcl .test files to .sqltest format
//!
//! This module provides an error-recoverable parser that converts the legacy
//! Tcl-based test format to the new `.sqltest` DSL format.

mod parser;

pub use parser::{
    ConversionResult, ConversionWarning, TclParser, TclTest, TclTestKind, WarningKind,
};

use std::collections::HashMap;
use std::fmt::Write;

/// Convert a Tcl test file content to the .sqltest format
pub fn convert(content: &str, source_file: &str) -> ConversionResult {
    let parser = TclParser::new(content, source_file);
    parser.parse()
}

/// A single output file with its content and test count
pub struct OutputFile {
    pub content: String,
    pub test_count: usize,
}

/// Output from generating sqltest files - may produce multiple files
pub struct GeneratedOutput {
    /// Map of filename suffix to output content
    /// e.g., "memory" -> content, "small" -> content, "" -> content (default)
    pub files: HashMap<String, OutputFile>,
    /// Total test count
    pub total_tests: usize,
}

/// Get the suffix name for a database (for filename)
fn db_to_suffix(db: &Option<String>) -> String {
    match db {
        None => "default".to_string(),
        Some(db) if db == ":memory:" => "memory".to_string(),
        Some(db) => {
            // Extract meaningful name from path like "testing/testing_small.db"
            let path = std::path::Path::new(db);
            let stem = path.file_stem().unwrap_or_default().to_string_lossy();
            // Remove "testing_" prefix if present
            let name = stem.strip_prefix("testing_").unwrap_or(&stem);
            if name == "testing" {
                "default".to_string()
            } else {
                name.to_string()
            }
        }
    }
}

/// Get the database declaration for a group
fn get_db_declaration(db_key: &str, has_ddl: bool) -> String {
    if db_key == "memory" || has_ddl {
        "@database :memory:".to_string()
    } else {
        // Map suffix back to database path
        match db_key {
            "default" => ["testing", "testing_norowidalias"]
                .map(|key| format!("@database database/{key}.db readonly"))
                .join("\n"),
            "small" | "norowidalias" | "user_version_10" => {
                format!("@database database/testing_{db_key}.db readonly")
            }
            other => {
                // For unknown databases, try to construct a path
                return format!("@database database/{}.db readonly", other);
            }
        }
    }
}

/// Generate the .sqltest output from parsed tests
/// Returns separate outputs for each database type
pub fn generate_sqltest(result: &ConversionResult) -> GeneratedOutput {
    // Group tests by database
    let mut groups: HashMap<String, Vec<&TclTest>> = HashMap::new();

    for test in &result.tests {
        let key = if test.has_ddl || test.db.as_deref() == Some(":memory:") {
            "memory".to_string()
        } else {
            db_to_suffix(&test.db)
        };
        groups.entry(key).or_default().push(test);
    }

    let mut files = HashMap::new();
    let mut total_tests = 0;

    for (key, tests) in groups {
        if tests.is_empty() {
            continue;
        }

        let has_ddl = tests.iter().any(|t| t.has_ddl);
        let content = generate_tests_output(&tests, &key, has_ddl);
        let test_count = tests.len();
        total_tests += test_count;

        files.insert(
            key,
            OutputFile {
                content,
                test_count,
            },
        );
    }

    GeneratedOutput { files, total_tests }
}

/// Generate output for a set of tests with the same database
fn generate_tests_output(tests: &[&TclTest], db_key: &str, has_ddl: bool) -> String {
    let mut output = String::new();

    writeln!(output, "{}", get_db_declaration(db_key, has_ddl)).unwrap();
    writeln!(output).unwrap();

    // Write test cases
    for test in tests {
        write_test(&mut output, test);
    }

    output
}

/// Write a single test to the output
fn write_test(output: &mut String, test: &TclTest) {
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
