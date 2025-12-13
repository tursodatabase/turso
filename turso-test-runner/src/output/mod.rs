pub mod json;
pub mod pretty;

use crate::runner::{FileResult, RunSummary, TestOutcome, TestResult};

/// Output format trait
pub trait OutputFormat {
    /// Write results for a single test (for streaming output)
    fn write_test(&mut self, result: &TestResult);

    /// Write results for a completed file
    fn write_file(&mut self, result: &FileResult);

    /// Write the final summary
    fn write_summary(&mut self, summary: &RunSummary);

    /// Flush any buffered output
    fn flush(&mut self);
}

/// Available output formats
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Format {
    Pretty,
    Json,
}

impl std::str::FromStr for Format {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pretty" => Ok(Format::Pretty),
            "json" => Ok(Format::Json),
            _ => Err(format!("unknown output format: {}", s)),
        }
    }
}

/// Create an output formatter for the given format
pub fn create_output(format: Format) -> Box<dyn OutputFormat> {
    match format {
        Format::Pretty => Box::new(pretty::PrettyOutput::new()),
        Format::Json => Box::new(json::JsonOutput::new()),
    }
}

/// Helper to get outcome symbol for display
pub fn outcome_symbol(outcome: &TestOutcome) -> &'static str {
    match outcome {
        TestOutcome::Passed => "PASS",
        TestOutcome::Failed { .. } => "FAIL",
        TestOutcome::Skipped { .. } => "SKIP",
        TestOutcome::Error { .. } => "ERROR",
    }
}
