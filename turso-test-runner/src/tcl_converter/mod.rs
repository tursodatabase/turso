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

    // Determine database type based on tests
    let needs_memory = result.tests.iter().any(|t| t.uses_memory_db());
    let has_ddl = result.tests.iter().any(|t| t.has_ddl);
    let uses_test_dbs = result.tests.iter().any(|t| t.uses_test_dbs());

    // If we have DDL statements or memory db tests, we need a writable database
    if has_ddl || needs_memory {
        writeln!(output, "@database :memory:").unwrap();
    } else if uses_test_dbs {
        // Tests that use test_dbs without DDL need readonly databases
        writeln!(output, "@database testing/testing.db readonly").unwrap();
    } else {
        // Default to memory for safety
        writeln!(output, "@database :memory:").unwrap();
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
                writeln!(output, "expect {{").unwrap();
                for line in expected.lines() {
                    let trimmed = line.trim();
                    if !trimmed.is_empty() {
                        writeln!(output, "    {}", trimmed).unwrap();
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

    output
}
