use super::OutputFormat;
use crate::runner::{FileResult, RunSummary, TestOutcome, TestResult};
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;

/// JSON output for machine consumption
pub struct JsonOutput {
    results: HashMap<PathBuf, Vec<JsonTestResult>>,
}

impl JsonOutput {
    pub fn new() -> Self {
        Self {
            results: HashMap::new(),
        }
    }
}

impl Default for JsonOutput {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize)]
struct JsonTestResult {
    name: String,
    outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    duration_ms: u128,
}

#[derive(Serialize)]
struct JsonFileResult {
    path: String,
    results: Vec<JsonTestResult>,
    duration_ms: u128,
}

#[derive(Serialize)]
struct JsonSummary {
    total: usize,
    passed: usize,
    failed: usize,
    skipped: usize,
    errors: usize,
    duration_ms: u128,
}

#[derive(Serialize)]
struct JsonReport {
    files: Vec<JsonFileResult>,
    summary: JsonSummary,
}

impl OutputFormat for JsonOutput {
    fn write_test(&mut self, result: &TestResult) {
        let (outcome, reason) = match &result.outcome {
            TestOutcome::Passed => ("passed".to_string(), None),
            TestOutcome::Failed { reason } => ("failed".to_string(), Some(reason.clone())),
            TestOutcome::Skipped { reason } => ("skipped".to_string(), Some(reason.clone())),
            TestOutcome::Error { message } => ("error".to_string(), Some(message.clone())),
            TestOutcome::SnapshotNew { .. } => ("snapshot_new".to_string(), None),
            TestOutcome::SnapshotUpdated { .. } => ("snapshot_updated".to_string(), None),
            TestOutcome::SnapshotMismatch { diff, .. } => {
                ("snapshot_mismatch".to_string(), Some(diff.clone()))
            }
        };

        let json_result = JsonTestResult {
            name: result.name.clone(),
            outcome,
            reason,
            duration_ms: result.duration.as_millis(),
        };

        self.results
            .entry(result.file.clone())
            .or_default()
            .push(json_result);
    }

    fn write_file(&mut self, result: &FileResult) {
        for test_result in &result.results {
            self.write_test(test_result);
        }
    }

    fn write_summary(&mut self, summary: &RunSummary) {
        // Build the complete report
        let files: Vec<JsonFileResult> = self
            .results
            .drain()
            .map(|(path, results)| {
                let duration_ms: u128 = results.iter().map(|r| r.duration_ms).sum();
                JsonFileResult {
                    path: path.display().to_string(),
                    results,
                    duration_ms,
                }
            })
            .collect();

        let report = JsonReport {
            files,
            summary: JsonSummary {
                total: summary.total,
                passed: summary.passed,
                failed: summary.failed,
                skipped: summary.skipped,
                errors: summary.errors,
                duration_ms: summary.duration.as_millis(),
            },
        };

        // Output the JSON
        match serde_json::to_string_pretty(&report) {
            Ok(json) => println!("{json}"),
            Err(e) => eprintln!("Error serializing JSON: {e}"),
        }
    }

    fn flush(&mut self) {
        // JSON output is done all at once in write_summary
    }
}
