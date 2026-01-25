use crate::backends::{BackendError, SqlBackend};
use crate::comparison::{ComparisonResult, compare};
use crate::parser::ast::{
    Capability, DatabaseConfig, Requirement, SnapshotCase, TestCase, TestFile,
};
use crate::snapshot::{SnapshotInfo, SnapshotManager, SnapshotResult};
use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashSet;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Result of loading test files
pub struct LoadedTests {
    /// Successfully parsed test files with their paths
    pub files: Vec<(PathBuf, TestFile)>,
    /// Parse/read errors as FileResults (for reporting)
    pub errors: Vec<FileResult>,
}

impl LoadedTests {
    /// Get just the TestFile references for scanning (e.g., for default database needs)
    pub fn test_files(&self) -> impl Iterator<Item = &TestFile> {
        self.files.iter().map(|(_, tf)| tf)
    }
}

/// Load and parse test files from paths
///
/// This function handles:
/// - Resolving directories (globbing for .sqltest files)
/// - Reading and parsing each file
/// - Collecting parse errors separately for reporting
pub async fn load_test_files(paths: &[PathBuf]) -> Result<LoadedTests, BackendError> {
    let mut files = Vec::new();
    let mut errors = Vec::new();

    for path in paths {
        if path.is_dir() {
            let pattern = path.join("**/*.sqltest");
            let pattern_str = pattern.to_string_lossy();

            for entry in glob::glob(&pattern_str)
                .map_err(|e| BackendError::Execute(format!("invalid glob pattern: {e}")))?
            {
                match entry {
                    Ok(file_path) => {
                        load_single_file(&file_path, &mut files, &mut errors).await;
                    }
                    Err(e) => {
                        return Err(BackendError::Execute(format!("glob error: {e}")));
                    }
                }
            }
        } else if path.is_file() {
            load_single_file(path, &mut files, &mut errors).await;
        }
    }

    Ok(LoadedTests { files, errors })
}

async fn load_single_file(
    path: &PathBuf,
    files: &mut Vec<(PathBuf, TestFile)>,
    errors: &mut Vec<FileResult>,
) {
    match tokio::fs::read_to_string(path).await {
        Ok(content) => match crate::parse(&content) {
            Ok(test_file) => {
                files.push((path.clone(), test_file));
            }
            Err(e) => {
                errors.push(FileResult {
                    file: path.clone(),
                    results: vec![TestResult {
                        name: "parse".to_string(),
                        file: path.clone(),
                        database: DatabaseConfig {
                            location: crate::parser::ast::DatabaseLocation::Memory,
                            readonly: false,
                        },
                        outcome: TestOutcome::Error {
                            message: format!("parse error: {e}"),
                        },
                        duration: Duration::ZERO,
                    }],
                    duration: Duration::ZERO,
                });
            }
        },
        Err(e) => {
            errors.push(FileResult {
                file: path.clone(),
                results: vec![TestResult {
                    name: "read".to_string(),
                    file: path.clone(),
                    database: DatabaseConfig {
                        location: crate::parser::ast::DatabaseLocation::Memory,
                        readonly: false,
                    },
                    outcome: TestOutcome::Error {
                        message: format!("read error: {e}"),
                    },
                    duration: Duration::ZERO,
                }],
                duration: Duration::ZERO,
            });
        }
    }
}

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
    /// Snapshot test: new snapshot created
    SnapshotNew { content: String },
    /// Snapshot test: snapshot updated
    SnapshotUpdated { old: String, new: String },
    /// Snapshot test: snapshot mismatch
    SnapshotMismatch {
        expected: String,
        actual: String,
        diff: String,
    },
}

impl TestOutcome {
    pub fn is_passed(&self) -> bool {
        matches!(self, TestOutcome::Passed)
    }

    pub fn is_failed(&self) -> bool {
        matches!(
            self,
            TestOutcome::Failed { .. } | TestOutcome::SnapshotMismatch { .. }
        )
    }

    pub fn is_skipped(&self) -> bool {
        matches!(self, TestOutcome::Skipped { .. })
    }

    pub fn is_error(&self) -> bool {
        matches!(self, TestOutcome::Error { .. })
    }

    pub fn is_snapshot_new(&self) -> bool {
        matches!(self, TestOutcome::SnapshotNew { .. })
    }

    pub fn is_snapshot_updated(&self) -> bool {
        matches!(self, TestOutcome::SnapshotUpdated { .. })
    }

    pub fn is_snapshot_mismatch(&self) -> bool {
        matches!(self, TestOutcome::SnapshotMismatch { .. })
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
    /// Snapshots created (new)
    pub snapshots_new: usize,
    /// Snapshots updated
    pub snapshots_updated: usize,
    /// Snapshot mismatches
    pub snapshots_mismatch: usize,
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
            TestOutcome::SnapshotNew { .. } => {
                self.snapshots_new += 1;
                // New snapshots without update mode don't fail
            }
            TestOutcome::SnapshotUpdated { .. } => {
                self.snapshots_updated += 1;
                self.passed += 1; // Count as passed since it was updated
            }
            TestOutcome::SnapshotMismatch { .. } => {
                self.snapshots_mismatch += 1;
                self.failed += 1;
            }
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
    /// Whether MVCC mode is enabled
    pub mvcc: bool,
    /// Whether to automatically update snapshots
    pub update_snapshots: bool,
    /// Snapshot name filter (glob pattern)
    pub snapshot_filter: Option<String>,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            max_jobs: num_cpus::get(),
            filter: None,
            mvcc: false,
            update_snapshots: false,
            snapshot_filter: None,
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

    pub fn with_mvcc(mut self, mvcc: bool) -> Self {
        self.mvcc = mvcc;
        self
    }

    pub fn with_update_snapshots(mut self, update: bool) -> Self {
        self.update_snapshots = update;
        self
    }

    pub fn with_snapshot_filter(mut self, filter: impl Into<String>) -> Self {
        self.snapshot_filter = Some(filter.into());
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
        let backend_type = self.backend.backend_type();
        let backend_capabilities = self.backend.capabilities();

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

                // Skip tests that don't match the current backend
                if let Some(required_backend) = test.backend {
                    if required_backend != backend_type {
                        continue;
                    }
                }

                let backend = Arc::clone(&self.backend);
                let semaphore = Arc::clone(&self.semaphore);
                let test = test.clone();
                let db_config = db_config.clone();
                let setups = test_file.setups.clone();
                let file_path = path.to_path_buf();
                let mvcc = self.config.mvcc;
                let global_skip = test_file.global_skip.clone();
                let global_requires = test_file.global_requires.clone();
                let capabilities = backend_capabilities.clone();

                futures.push(tokio::spawn(async move {
                    let _permit = semaphore.acquire_owned().await.unwrap();
                    run_single_test(
                        backend,
                        file_path,
                        db_config,
                        test,
                        setups,
                        mvcc,
                        global_skip,
                        global_requires,
                        capabilities,
                    )
                    .await
                }));
            }
        }

        futures
    }

    /// Spawn all snapshot test tasks for a parsed file, returning futures
    fn spawn_snapshot_tests(
        &self,
        path: &Path,
        test_file: &TestFile,
    ) -> FuturesUnordered<tokio::task::JoinHandle<TestResult>> {
        let futures = FuturesUnordered::new();

        // For each database configuration (snapshots use the first one)
        if let Some(db_config) = test_file.databases.first() {
            // For each snapshot
            for snapshot in &test_file.snapshots {
                // Apply snapshot filter if present
                if let Some(ref filter) = self.config.snapshot_filter {
                    if !matches_filter(&snapshot.name, filter) {
                        continue;
                    }
                }

                // Also check the regular filter (it applies to both tests and snapshots)
                if let Some(ref filter) = self.config.filter {
                    if !matches_filter(&snapshot.name, filter) {
                        continue;
                    }
                }

                let backend = Arc::clone(&self.backend);
                let semaphore = Arc::clone(&self.semaphore);
                let snapshot = snapshot.clone();
                let db_config = db_config.clone();
                let setups = test_file.setups.clone();
                let file_path = path.to_path_buf();
                let mvcc = self.config.mvcc;
                let global_skip = test_file.global_skip.clone();
                let update_snapshots = self.config.update_snapshots;

                futures.push(tokio::spawn(async move {
                    let _permit = semaphore.acquire_owned().await.unwrap();
                    run_single_snapshot_test(
                        backend,
                        file_path,
                        db_config,
                        snapshot,
                        setups,
                        mvcc,
                        global_skip,
                        update_snapshots,
                    )
                    .await
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
                            message: format!("task panicked: {e}"),
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
    /// The callback is called for each test result as it completes
    ///
    /// This is a convenience method that loads and runs tests in one call.
    /// For more control (e.g., to inspect files before running), use
    /// `load_test_files` followed by `run_loaded_tests`.
    pub async fn run_paths<F>(
        &self,
        paths: &[PathBuf],
        on_result: F,
    ) -> Result<Vec<FileResult>, BackendError>
    where
        F: FnMut(&TestResult),
    {
        let loaded = load_test_files(paths).await?;
        self.run_loaded_tests(loaded, on_result).await
    }

    /// Run pre-loaded test files
    ///
    /// Use this when you need to inspect the test files before running
    /// (e.g., to check for default database needs).
    pub async fn run_loaded_tests<F>(
        &self,
        loaded: LoadedTests,
        mut on_result: F,
    ) -> Result<Vec<FileResult>, BackendError>
    where
        F: FnMut(&TestResult),
    {
        let start = Instant::now();

        // Report any parse/read errors
        for error_result in &loaded.errors {
            for result in &error_result.results {
                on_result(result);
            }
        }

        // Spawn ALL test tasks from ALL files at once into a single FuturesUnordered
        let mut all_futures: FuturesUnordered<tokio::task::JoinHandle<TestResult>> =
            FuturesUnordered::new();

        for (path, test_file) in &loaded.files {
            // Spawn regular tests
            let file_futures = self.spawn_file_tests(path, test_file);
            for future in file_futures {
                all_futures.push(future);
            }

            // Spawn snapshot tests
            let snapshot_futures = self.spawn_snapshot_tests(path, test_file);
            for future in snapshot_futures {
                all_futures.push(future);
            }
        }

        // Collect results, grouped by file
        let mut results_by_file: std::collections::HashMap<PathBuf, Vec<TestResult>> =
            std::collections::HashMap::new();

        while let Some(result) = all_futures.next().await {
            let test_result = match result {
                Ok(r) => r,
                Err(e) => {
                    // JoinError doesn't tell us which file, but this is rare
                    // Just report as an error without grouping
                    let result = TestResult {
                        name: "unknown".to_string(),
                        file: PathBuf::from("unknown"),
                        database: DatabaseConfig {
                            location: crate::parser::ast::DatabaseLocation::Memory,
                            readonly: false,
                        },
                        outcome: TestOutcome::Error {
                            message: format!("task panicked: {e}"),
                        },
                        duration: Duration::ZERO,
                    };
                    on_result(&result);
                    continue;
                }
            };

            // Call the callback with each result as it completes
            on_result(&test_result);

            // Use test_result.file for grouping
            let file_path = test_result.file.clone();
            results_by_file
                .entry(file_path)
                .or_default()
                .push(test_result);
        }

        // Convert to FileResults
        let mut all_results = loaded.errors;
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
#[allow(clippy::too_many_arguments)]
async fn run_single_test<B: SqlBackend>(
    backend: Arc<B>,
    file_path: PathBuf,
    db_config: DatabaseConfig,
    test: TestCase,
    setups: std::collections::HashMap<String, String>,
    mvcc: bool,
    global_skip: Option<crate::parser::ast::Skip>,
    global_requires: Vec<Requirement>,
    backend_capabilities: HashSet<Capability>,
) -> TestResult {
    let start = Instant::now();

    // Capture test name and other info for panic recovery
    let test_name = test.name.clone();
    let file_path_for_panic = file_path.clone();
    let db_config_for_panic = db_config.clone();

    let test_future = async move {
        // Check if skipped (per-test skip overrides global skip)
        let effective_skip = test.skip.as_ref().or(global_skip.as_ref());
        if let Some(skip) = effective_skip {
            let should_skip = match &skip.condition {
                None => true, // Unconditional skip
                Some(crate::parser::ast::SkipCondition::Mvcc) => mvcc,
            };
            if should_skip {
                return TestResult {
                    name: test.name,
                    file: file_path,
                    database: db_config,
                    outcome: TestOutcome::Skipped {
                        reason: skip.reason.clone(),
                    },
                    duration: start.elapsed(),
                };
            }
        }

        // Check required capabilities (global + per-test)
        for req in global_requires.iter().chain(test.requires.iter()) {
            if !backend_capabilities.contains(&req.capability) {
                return TestResult {
                    name: test.name,
                    file: file_path,
                    database: db_config,
                    outcome: TestOutcome::Skipped {
                        reason: format!("requires {}: {}", req.capability, req.reason),
                    },
                    duration: start.elapsed(),
                };
            }
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
                        message: format!("failed to create database: {e}"),
                    },
                    duration: start.elapsed(),
                };
            }
        };

        // Run setups (using execute_setup which buffers for memory databases)
        for setup_ref in &test.setups {
            if let Some(setup_sql) = setups.get(&setup_ref.name) {
                if let Err(e) = db.execute_setup(setup_sql).await {
                    let _ = db.close().await;
                    return TestResult {
                        name: test.name,
                        file: file_path,
                        database: db_config,
                        outcome: TestOutcome::Error {
                            message: format!("setup '{}' failed: {}", setup_ref.name, e),
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
                        message: format!("setup '{}' not found", setup_ref.name),
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
                        message: format!("execution failed: {e}"),
                    },
                    duration: start.elapsed(),
                };
            }
        };

        // Close database
        let _ = db.close().await;

        // Compare result (select expectation based on backend)
        let expectation = test.expectations.for_backend(backend.backend_type());
        let comparison = compare(&result, expectation);
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
    };

    match AssertUnwindSafe(test_future).catch_unwind().await {
        Ok(result) => result,
        Err(panic_info) => {
            // Extract panic message
            let panic_message = if let Some(s) = panic_info.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = panic_info.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };

            TestResult {
                name: test_name,
                file: file_path_for_panic,
                database: db_config_for_panic,
                outcome: TestOutcome::Error {
                    message: format!("panic: {panic_message}"),
                },
                duration: start.elapsed(),
            }
        }
    }
}

/// Run a single snapshot test
#[allow(clippy::too_many_arguments)]
async fn run_single_snapshot_test<B: SqlBackend>(
    backend: Arc<B>,
    file_path: PathBuf,
    db_config: DatabaseConfig,
    snapshot: SnapshotCase,
    setups: std::collections::HashMap<String, String>,
    mvcc: bool,
    global_skip: Option<crate::parser::ast::Skip>,
    update_snapshots: bool,
) -> TestResult {
    let start = Instant::now();

    // Capture test name and other info for panic recovery
    let test_name = snapshot.name.clone();
    let file_path_for_panic = file_path.clone();
    let db_config_for_panic = db_config.clone();

    let test_future = async move {
        // Check if skipped (per-snapshot skip overrides global skip)
        let effective_skip = snapshot.skip.as_ref().or(global_skip.as_ref());
        if let Some(skip) = effective_skip {
            let should_skip = match &skip.condition {
                None => true, // Unconditional skip
                Some(crate::parser::ast::SkipCondition::Mvcc) => mvcc,
            };
            if should_skip {
                return TestResult {
                    name: snapshot.name,
                    file: file_path,
                    database: db_config,
                    outcome: TestOutcome::Skipped {
                        reason: skip.reason.clone(),
                    },
                    duration: start.elapsed(),
                };
            }
        }

        // Create database instance
        let mut db = match backend.create_database(&db_config).await {
            Ok(db) => db,
            Err(e) => {
                return TestResult {
                    name: snapshot.name,
                    file: file_path,
                    database: db_config,
                    outcome: TestOutcome::Error {
                        message: format!("failed to create database: {e}"),
                    },
                    duration: start.elapsed(),
                };
            }
        };

        // Run setups (using execute_setup which buffers for memory databases)
        for setup_ref in &snapshot.setups {
            if let Some(setup_sql) = setups.get(&setup_ref.name) {
                if let Err(e) = db.execute_setup(setup_sql).await {
                    let _ = db.close().await;
                    return TestResult {
                        name: snapshot.name,
                        file: file_path,
                        database: db_config,
                        outcome: TestOutcome::Error {
                            message: format!("setup '{}' failed: {}", setup_ref.name, e),
                        },
                        duration: start.elapsed(),
                    };
                }
            } else {
                let _ = db.close().await;
                return TestResult {
                    name: snapshot.name,
                    file: file_path,
                    database: db_config,
                    outcome: TestOutcome::Error {
                        message: format!("setup '{}' not found", setup_ref.name),
                    },
                    duration: start.elapsed(),
                };
            }
        }

        // Execute EXPLAIN <sql>
        let explain_sql = format!("EXPLAIN {}", snapshot.sql);
        let result = match db.execute(&explain_sql).await {
            Ok(r) => r,
            Err(e) => {
                let _ = db.close().await;
                return TestResult {
                    name: snapshot.name,
                    file: file_path,
                    database: db_config,
                    outcome: TestOutcome::Error {
                        message: format!("EXPLAIN execution failed: {e}"),
                    },
                    duration: start.elapsed(),
                };
            }
        };

        // Close database
        let _ = db.close().await;

        // Format EXPLAIN output for snapshot
        let actual_output = result
            .rows
            .iter()
            .map(|row| row.join("|"))
            .collect::<Vec<_>>()
            .join("\n");

        // Build snapshot info with metadata
        let db_location_str = match &db_config.location {
            crate::parser::ast::DatabaseLocation::Memory => ":memory:".to_string(),
            crate::parser::ast::DatabaseLocation::TempFile => ":temp:".to_string(),
            crate::parser::ast::DatabaseLocation::Path(p) => p.display().to_string(),
            crate::parser::ast::DatabaseLocation::Default => ":default:".to_string(),
            crate::parser::ast::DatabaseLocation::DefaultNoRowidAlias => {
                ":default-no-rowidalias:".to_string()
            }
        };
        let snapshot_info = SnapshotInfo::new()
            .with_setups(snapshot.setups.iter().map(|s| s.name.clone()).collect())
            .with_database(db_location_str);

        // Compare with snapshot
        let snapshot_manager = SnapshotManager::new(&file_path, update_snapshots);
        let snapshot_result = snapshot_manager
            .compare(&snapshot.name, &snapshot.sql, &actual_output, &snapshot_info)
            .await;

        let outcome = match snapshot_result {
            SnapshotResult::Match => TestOutcome::Passed,
            SnapshotResult::Mismatch {
                expected,
                actual,
                diff,
            } => TestOutcome::SnapshotMismatch {
                expected,
                actual,
                diff,
            },
            SnapshotResult::New { content } => TestOutcome::SnapshotNew { content },
            SnapshotResult::Updated { old, new } => TestOutcome::SnapshotUpdated { old, new },
            SnapshotResult::Error { msg } => TestOutcome::Error { message: msg },
        };

        TestResult {
            name: snapshot.name,
            file: file_path,
            database: db_config,
            outcome,
            duration: start.elapsed(),
        }
    };

    match AssertUnwindSafe(test_future).catch_unwind().await {
        Ok(result) => result,
        Err(panic_info) => {
            // Extract panic message
            let panic_message = if let Some(s) = panic_info.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = panic_info.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };

            TestResult {
                name: test_name,
                file: file_path_for_panic,
                database: db_config_for_panic,
                outcome: TestOutcome::Error {
                    message: format!("panic: {panic_message}"),
                },
                duration: start.elapsed(),
            }
        }
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
            parts
                .iter()
                .all(|part| part.is_empty() || name.contains(part))
        }
    } else {
        // Exact match
        name == pattern
    }
}

/// Compute summary from file results
pub fn summarize(start: Instant, results: &[FileResult]) -> RunSummary {
    let mut summary = RunSummary::default();

    for file_result in results {
        for test_result in &file_result.results {
            summary.add(&test_result.outcome);
        }
    }

    summary.duration = start.elapsed();
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
