use super::{outcome_symbol, OutputFormat};
use crate::runner::{FileResult, RunSummary, TestOutcome, TestResult};
use colored::Colorize;
use std::io::{self, Write};

/// Pretty human-readable output
pub struct PrettyOutput {
    current_file: Option<String>,
}

impl PrettyOutput {
    pub fn new() -> Self {
        Self { current_file: None }
    }

    fn outcome_colored(&self, outcome: &TestOutcome) -> colored::ColoredString {
        let symbol = outcome_symbol(outcome);
        match outcome {
            TestOutcome::Passed => symbol.green(),
            TestOutcome::Failed { .. } => symbol.red(),
            TestOutcome::Skipped { .. } => symbol.yellow(),
            TestOutcome::Error { .. } => symbol.red().bold(),
        }
    }
}

impl Default for PrettyOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl OutputFormat for PrettyOutput {
    fn write_test(&mut self, result: &TestResult) {
        let file_str = result.file.display().to_string();

        // Print file header if new file
        if self.current_file.as_ref() != Some(&file_str) {
            if self.current_file.is_some() {
                println!();
            }
            println!("{}", file_str.bold());
            self.current_file = Some(file_str);
        }

        // Format duration
        let duration_ms = result.duration.as_millis();
        let duration_str = format!("({}ms)", duration_ms);

        // Print test result
        print!(
            "  [{}] {:<40} {}",
            self.outcome_colored(&result.outcome),
            result.name,
            duration_str.dimmed()
        );

        // Print additional info for non-passing tests
        match &result.outcome {
            TestOutcome::Passed => {
                println!();
            }
            TestOutcome::Failed { reason } => {
                println!();
                for line in reason.lines() {
                    println!("         {}", line.red());
                }
            }
            TestOutcome::Skipped { reason } => {
                println!(" {}", format!("({})", reason).dimmed());
            }
            TestOutcome::Error { message } => {
                println!();
                for line in message.lines() {
                    println!("         {}", line.red());
                }
            }
        }
    }

    fn write_file(&mut self, result: &FileResult) {
        // Write all test results for this file
        for test_result in &result.results {
            self.write_test(test_result);
        }
    }

    fn write_summary(&mut self, summary: &RunSummary) {
        println!();
        println!("{}", "Summary:".bold());

        let mut parts = Vec::new();

        if summary.passed > 0 {
            parts.push(format!("{} passed", summary.passed).green().to_string());
        }
        if summary.failed > 0 {
            parts.push(format!("{} failed", summary.failed).red().to_string());
        }
        if summary.skipped > 0 {
            parts.push(format!("{} skipped", summary.skipped).yellow().to_string());
        }
        if summary.errors > 0 {
            parts.push(format!("{} errors", summary.errors).red().to_string());
        }

        println!("  {}", parts.join(", "));
        println!(
            "  {}",
            format!("Total time: {}ms", summary.duration.as_millis()).dimmed()
        );

        // Print overall status
        if summary.is_success() {
            println!();
            println!("{}", "All tests passed!".green().bold());
        } else {
            println!();
            println!("{}", "Some tests failed.".red().bold());
        }
    }

    fn flush(&mut self) {
        let _ = io::stdout().flush();
    }
}
