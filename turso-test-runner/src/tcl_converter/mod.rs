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

    // Determine if any test has setup SQL (requires writable database)
    let has_setups = result.tests.iter().any(|t| t.setup_sql.is_some());
    let needs_memory = result.tests.iter().any(|t| t.uses_memory_db());
    let uses_test_dbs = result.tests.iter().any(|t| t.uses_test_dbs());

    // If we have setup blocks, we need a writable database
    if has_setups || needs_memory {
        writeln!(output, "@database :memory:").unwrap();
    } else if uses_test_dbs {
        // Tests that use test_dbs without setup need readonly databases
        writeln!(output, "@database testing/testing.db readonly").unwrap();
    } else {
        // Default to memory for safety
        writeln!(output, "@database :memory:").unwrap();
    }

    writeln!(output).unwrap();

    // Collect all unique setups
    let mut setups_written: std::collections::HashSet<String> = std::collections::HashSet::new();

    for test in &result.tests {
        if let Some(setup_name) = &test.setup_name {
            if !setups_written.contains(setup_name) {
                if let Some(setup_sql) = &test.setup_sql {
                    writeln!(output, "setup {} {{", setup_name).unwrap();
                    for line in setup_sql.lines() {
                        writeln!(output, "    {}", line.trim()).unwrap();
                    }
                    writeln!(output, "}}").unwrap();
                    writeln!(output).unwrap();
                    setups_written.insert(setup_name.clone());
                }
            }
        }
    }

    // Write test cases
    for test in &result.tests {
        // Write setup reference if needed
        if let Some(setup_name) = &test.setup_name {
            if setups_written.contains(setup_name) {
                writeln!(output, "@setup {}", setup_name).unwrap();
            }
        }

        // Write test header
        writeln!(output, "test {} {{", test.name).unwrap();

        // Write SQL
        for line in test.sql.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                writeln!(output, "    {}", trimmed).unwrap();
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
