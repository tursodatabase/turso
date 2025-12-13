mod executor;

pub use executor::TestExecutor;

use crate::backends::{BackendError, SqlBackend};
use crate::comparison::{compare, ComparisonResult};
use crate::parser::ast::{DatabaseConfig, TestCase, TestFile};
use futures::stream::{FuturesUnordered, StreamExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Result of running a single test
#[derive(Debug, Clone)]
pub struct TestResult {
    /// Test name
    pub name: String,
    /// Source file
    pub file: PathBuf,
    /// Database config used
    pub database: DatabaseConfig,
    /// Outcome
    pub outcome: TestOutcome,
    /// Execution duration
    pub duration: Duration,
}

/// Outcome of a test
#[derive(Debug, Clone)]
pub enum TestOutcome {
    /// Test passed
    Passed,
    /// Test failed (assertion)
    Failed { reason: String },
    /// Test was skipped
    Skipped { reason: String },
    /// Test encountered an error
    Error { message: String },
}

impl TestOutcome {
    pub fn is_passed(&self) -> bool {
        matches!(self, TestOutcome::Passed)
    }

    pub fn is_failed(&self) -> bool {
        matches!(self, TestOutcome::Failed { .. })
    }

    pub fn is_skipped(&self) -> bool {
        matches!(self, TestOutcome::Skipped { .. })
    }

    pub fn is_error(&self) -> bool {
        matches!(self, TestOutcome::Error { .. })
    }
}

/// Result of running all tests in a file
#[derive(Debug)]
pub struct FileResult {
    /// Source file path
    pub file: PathBuf,
    /// Individual test results
    pub results: Vec<TestResult>,
    /// Total duration
    pub duration: Duration,
}

/// Aggregated results from a test run
#[derive(Debug, Default)]
pub struct RunSummary {
    /// Total tests run
    pub total: usize,
    /// Tests passed
    pub passed: usize,
    /// Tests failed
    pub failed: usize,
    /// Tests skipped
    pub skipped: usize,
    /// Tests with errors
    pub errors: usize,
    /// Total duration
    pub duration: Duration,
}

impl RunSummary {
    pub fn add(&mut self, outcome: &TestOutcome) {
        self.total += 1;
        match outcome {
            TestOutcome::Passed => self.passed += 1,
            TestOutcome::Failed { .. } => self.failed += 1,
            TestOutcome::Skipped { .. } => self.skipped += 1,
            TestOutcome::Error { .. } => self.errors += 1,
        }
    }

    pub fn is_success(&self) -> bool {
        self.failed == 0 && self.errors == 0
    }
}

/// Test runner configuration
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// Maximum concurrent jobs
    pub max_jobs: usize,
    /// Test name filter (glob pattern)
    pub filter: Option<String>,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            max_jobs: num_cpus::get(),
            filter: None,
        }
    }
}

impl RunnerConfig {
    pub fn with_max_jobs(mut self, jobs: usize) -> Self {
        self.max_jobs = jobs;
        self
    }

    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }
}

/// Main test runner
pub struct TestRunner<B: SqlBackend> {
    backend: Arc<B>,
    config: RunnerConfig,
    semaphore: Arc<Semaphore>,
}

impl<B: SqlBackend + 'static> TestRunner<B> {
    pub fn new(backend: B) -> Self {
        let config = RunnerConfig::default();
        let semaphore = Arc::new(Semaphore::new(config.max_jobs));
        Self {
            backend: Arc::new(backend),
            config,
            semaphore,
        }
    }

    pub fn with_config(mut self, config: RunnerConfig) -> Self {
        self.semaphore = Arc::new(Semaphore::new(config.max_jobs));
        self.config = config;
        self
    }

    /// Spawn all test tasks for a parsed file, returning futures
    fn spawn_file_tests(
        &self,
        path: &Path,
        test_file: &TestFile,
    ) -> FuturesUnordered<tokio::task::JoinHandle<TestResult>> {
        let futures = FuturesUnordered::new();

        // For each database configuration
        for db_config in &test_file.databases {
            // For each test
            for test in &test_file.tests {
                // Apply filter if present
                if let Some(ref filter) = self.config.filter {
                    if !matches_filter(&test.name, filter) {
                        continue;
                    }
                }

                let backend = Arc::clone(&self.backend);
                let semaphore = Arc::clone(&self.semaphore);
                let test = test.clone();
                let db_config = db_config.clone();
                let setups = test_file.setups.clone();
                let file_path = path.to_path_buf();

                futures.push(tokio::spawn(async move {
                    let _permit = semaphore.acquire_owned().await.unwrap();
                    run_single_test(backend, file_path, db_config, test, setups).await
                }));
            }
        }

        futures
    }

    /// Run all tests in a file
    pub async fn run_file(
        &self,
        path: &Path,
        test_file: &TestFile,
    ) -> Result<FileResult, BackendError> {
        let start = Instant::now();
        let mut futures = self.spawn_file_tests(path, test_file);

        let mut results = Vec::new();
        while let Some(result) = futures.next().await {
            match result {
                Ok(test_result) => results.push(test_result),
                Err(e) => {
                    // JoinError - task panicked
                    results.push(TestResult {
                        name: "unknown".to_string(),
                        file: path.to_path_buf(),
                        database: DatabaseConfig {
                            location: crate::parser::ast::DatabaseLocation::Memory,
                            readonly: false,
                        },
                        outcome: TestOutcome::Error {
                            message: format!("task panicked: {}", e),
                        },
                        duration: Duration::ZERO,
                    });
                }
            }
        }

        Ok(FileResult {
            file: path.to_path_buf(),
            results,
            duration: start.elapsed(),
        })
    }

    /// Run tests from multiple paths (files or directories) - all in parallel
    pub async fn run_paths(&self, paths: &[PathBuf]) -> Result<Vec<FileResult>, BackendError> {
        let start = Instant::now();

        // Collect all test files first
        let mut test_files: Vec<(PathBuf, TestFile)> = Vec::new();
        let mut parse_errors: Vec<FileResult> = Vec::new();

        for path in paths {
            if path.is_dir() {
                let pattern = path.join("**/*.sqltest");
                let pattern_str = pattern.to_string_lossy();

                for entry in glob::glob(&pattern_str).map_err(|e| {
                    BackendError::Execute(format!("invalid glob pattern: {}", e))
                })? {
                    match entry {
                        Ok(file_path) => {
                            match std::fs::read_to_string(&file_path) {
                                Ok(content) => match crate::parse(&content) {
                                    Ok(test_file) => {
                                        test_files.push((file_path, test_file));
                                    }
                                    Err(e) => {
                                        parse_errors.push(FileResult {
                                            file: file_path.clone(),
                                            results: vec![TestResult {
                                                name: "parse".to_string(),
                                                file: file_path,
                                                database: DatabaseConfig {
                                                    location: crate::parser::ast::DatabaseLocation::Memory,
                                                    readonly: false,
                                                },
                                                outcome: TestOutcome::Error {
                                                    message: format!("parse error: {}", e),
                                                },
                                                duration: Duration::ZERO,
                                            }],
                                            duration: Duration::ZERO,
                                        });
                                    }
                                },
                                Err(e) => {
                                    parse_errors.push(FileResult {
                                        file: file_path.clone(),
                                        results: vec![TestResult {
                                            name: "read".to_string(),
                                            file: file_path,
                                            database: DatabaseConfig {
                                                location: crate::parser::ast::DatabaseLocation::Memory,
                                                readonly: false,
                                            },
                                            outcome: TestOutcome::Error {
                                                message: format!("read error: {}", e),
                                            },
                                            duration: Duration::ZERO,
                                        }],
                                        duration: Duration::ZERO,
                                    });
                                }
                            }
                        }
                        Err(e) => {
                            return Err(BackendError::Execute(format!("glob error: {}", e)));
                        }
                    }
                }
            } else if path.is_file() {
                match std::fs::read_to_string(path) {
                    Ok(content) => match crate::parse(&content) {
                        Ok(test_file) => {
                            test_files.push((path.clone(), test_file));
                        }
                        Err(e) => {
                            parse_errors.push(FileResult {
                                file: path.clone(),
                                results: vec![TestResult {
                                    name: "parse".to_string(),
                                    file: path.clone(),
                                    database: DatabaseConfig {
                                        location: crate::parser::ast::DatabaseLocation::Memory,
                                        readonly: false,
                                    },
                                    outcome: TestOutcome::Error {
                                        message: format!("parse error: {}", e),
                                    },
                                    duration: Duration::ZERO,
                                }],
                                duration: Duration::ZERO,
                            });
                        }
                    },
                    Err(e) => {
                        parse_errors.push(FileResult {
                            file: path.clone(),
                            results: vec![TestResult {
                                name: "read".to_string(),
                                file: path.clone(),
                                database: DatabaseConfig {
                                    location: crate::parser::ast::DatabaseLocation::Memory,
                                    readonly: false,
                                },
                                outcome: TestOutcome::Error {
                                    message: format!("read error: {}", e),
                                },
                                duration: Duration::ZERO,
                            }],
                            duration: Duration::ZERO,
                        });
                    }
                }
            }
        }

        // Spawn ALL test tasks from ALL files at once into a single FuturesUnordered
        let mut all_futures: FuturesUnordered<_> = FuturesUnordered::new();

        for (path, test_file) in &test_files {
            let file_futures = self.spawn_file_tests(path, test_file);
            for future in file_futures {
                let path = path.clone();
                all_futures.push(async move { (path, future.await) });
            }
        }

        // Collect results, grouped by file
        let mut results_by_file: std::collections::HashMap<PathBuf, Vec<TestResult>> =
            std::collections::HashMap::new();

        while let Some((path, result)) = all_futures.next().await {
            let test_result = match result {
                Ok(r) => r,
                Err(e) => TestResult {
                    name: "unknown".to_string(),
                    file: path.clone(),
                    database: DatabaseConfig {
                        location: crate::parser::ast::DatabaseLocation::Memory,
                        readonly: false,
                    },
                    outcome: TestOutcome::Error {
                        message: format!("task panicked: {}", e),
                    },
                    duration: Duration::ZERO,
                },
            };
            results_by_file
                .entry(path)
                .or_default()
                .push(test_result);
        }

        // Convert to FileResults
        let mut all_results = parse_errors;
        let total_duration = start.elapsed();

        for (path, results) in results_by_file {
            all_results.push(FileResult {
                file: path,
                results,
                duration: total_duration, // Approximate - all ran in parallel
            });
        }

        Ok(all_results)
    }
}

/// Run a single test
async fn run_single_test<B: SqlBackend>(
    backend: Arc<B>,
    file_path: PathBuf,
    db_config: DatabaseConfig,
    test: TestCase,
    setups: std::collections::HashMap<String, String>,
) -> TestResult {
    let start = Instant::now();

    // Check if skipped
    if let Some(reason) = &test.skip {
        return TestResult {
            name: test.name,
            file: file_path,
            database: db_config,
            outcome: TestOutcome::Skipped {
                reason: reason.clone(),
            },
            duration: start.elapsed(),
        };
    }

    // Create database instance
    let mut db = match backend.create_database(&db_config).await {
        Ok(db) => db,
        Err(e) => {
            return TestResult {
                name: test.name,
                file: file_path,
                database: db_config,
                outcome: TestOutcome::Error {
                    message: format!("failed to create database: {}", e),
                },
                duration: start.elapsed(),
            };
        }
    };

    // Run setups
    for setup_name in &test.setups {
        if let Some(setup_sql) = setups.get(setup_name) {
            if let Err(e) = db.execute(setup_sql).await {
                let _ = db.close().await;
                return TestResult {
                    name: test.name,
                    file: file_path,
                    database: db_config,
                    outcome: TestOutcome::Error {
                        message: format!("setup '{}' failed: {}", setup_name, e),
                    },
                    duration: start.elapsed(),
                };
            }
        } else {
            let _ = db.close().await;
            return TestResult {
                name: test.name,
                file: file_path,
                database: db_config,
                outcome: TestOutcome::Error {
                    message: format!("setup '{}' not found", setup_name),
                },
                duration: start.elapsed(),
            };
        }
    }

    // Execute test SQL
    let result = match db.execute(&test.sql).await {
        Ok(r) => r,
        Err(e) => {
            let _ = db.close().await;
            return TestResult {
                name: test.name,
                file: file_path,
                database: db_config,
                outcome: TestOutcome::Error {
                    message: format!("execution failed: {}", e),
                },
                duration: start.elapsed(),
            };
        }
    };

    // Close database
    let _ = db.close().await;

    // Compare result
    let comparison = compare(&result, &test.expectation);
    let outcome = match comparison {
        ComparisonResult::Match => TestOutcome::Passed,
        ComparisonResult::Mismatch { reason } => TestOutcome::Failed { reason },
    };

    TestResult {
        name: test.name,
        file: file_path,
        database: db_config,
        outcome,
        duration: start.elapsed(),
    }
}

/// Check if test name matches filter pattern
fn matches_filter(name: &str, pattern: &str) -> bool {
    // Simple glob matching: * matches anything
    if pattern.contains('*') {
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            // prefix* or *suffix or prefix*suffix
            let prefix = parts[0];
            let suffix = parts[1];
            name.starts_with(prefix) && name.ends_with(suffix)
        } else {
            // Multiple * - just check contains for now
            parts.iter().all(|part| part.is_empty() || name.contains(part))
        }
    } else {
        // Exact match
        name == pattern
    }
}

/// Compute summary from file results
pub fn summarize(results: &[FileResult]) -> RunSummary {
    let mut summary = RunSummary::default();
    let total_duration: Duration = results.iter().map(|r| r.duration).sum();

    for file_result in results {
        for test_result in &file_result.results {
            summary.add(&test_result.outcome);
        }
    }

    summary.duration = total_duration;
    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matches_filter_exact() {
        assert!(matches_filter("select-test", "select-test"));
        assert!(!matches_filter("select-test", "other-test"));
    }

    #[test]
    fn test_matches_filter_prefix() {
        assert!(matches_filter("select-test", "select-*"));
        assert!(matches_filter("select-anything", "select-*"));
        assert!(!matches_filter("other-test", "select-*"));
    }

    #[test]
    fn test_matches_filter_suffix() {
        assert!(matches_filter("select-test", "*-test"));
        assert!(matches_filter("other-test", "*-test"));
        assert!(!matches_filter("select-thing", "*-test"));
    }

    #[test]
    fn test_matches_filter_contains() {
        assert!(matches_filter("select-join-test", "*join*"));
        assert!(!matches_filter("select-test", "*join*"));
    }

    #[test]
    fn test_summary_add() {
        let mut summary = RunSummary::default();
        summary.add(&TestOutcome::Passed);
        summary.add(&TestOutcome::Passed);
        summary.add(&TestOutcome::Failed {
            reason: "mismatch".to_string(),
        });
        summary.add(&TestOutcome::Skipped {
            reason: "skip".to_string(),
        });

        assert_eq!(summary.total, 4);
        assert_eq!(summary.passed, 2);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.skipped, 1);
        assert!(!summary.is_success());
    }
}
