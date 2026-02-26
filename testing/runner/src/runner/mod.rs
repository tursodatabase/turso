use crate::backends::{BackendError, QueryResult, SqlBackend};
use crate::comparison::{ComparisonResult, compare};
use crate::parser::ast::{
    Backend, Capability, DatabaseConfig, DatabaseLocation, Expectation, Requirement, SetupRef,
    Skip, SkipCondition, SnapshotCase, TestCase, TestFile,
};
use crate::snapshot::{SnapshotInfo, SnapshotManager, SnapshotResult, SnapshotUpdateMode};
use async_trait::async_trait;
use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

// ============================================================================
// Runnable trait and RunOptions
// ============================================================================

/// Options for running a test or snapshot
#[derive(Clone)]
pub struct RunOptions {
    /// Path to the source file
    pub file_path: PathBuf,
    /// Database configuration
    pub db_config: DatabaseConfig,
    /// Available setup blocks (name -> SQL)
    pub setups: HashMap<String, String>,
    /// Whether MVCC mode is enabled
    pub mvcc: bool,
    /// Whether release mode is enabled
    pub release: bool,
    /// Global skip directives
    pub global_skip: Vec<Skip>,
    /// Global capability requirements (used by tests)
    pub global_requires: Vec<Requirement>,
    /// Backend capabilities (used by tests)
    pub backend_capabilities: HashSet<Capability>,
    /// Backend type (used by tests)
    pub backend_type: Backend,
    /// Whether this is the sqlite CLI backend
    pub backend_is_sqlite: bool,
    /// Snapshot update mode (used by snapshots)
    pub snapshot_update_mode: SnapshotUpdateMode,
    /// Path to binary used for cross-checking integrity (None = disabled)
    pub cross_check_binary: Option<PathBuf>,
}

/// Trait for runnable test items (tests and snapshots)
#[async_trait]
pub trait Runnable: Clone + Send + 'static {
    /// Get the name of this test item
    fn name(&self) -> &str;

    /// Get the skip configuration, if any
    fn skip(&self) -> &[Skip];

    /// Get setup references
    fn setups(&self) -> &[SetupRef];

    /// Check if this test should be skipped due to additional conditions (e.g., capabilities)
    /// Returns Some(reason) if should skip, None if can proceed
    fn check_skip_conditions(&self, options: &RunOptions) -> Option<String>;

    /// Whether this test expects an error outcome (skip cross-check for these).
    fn expects_error(&self) -> bool {
        false
    }

    /// Whether this test should be cross-checked with another binary's integrity_check.
    fn cross_check_integrity(&self) -> bool {
        false
    }

    /// Whether this test requires release mode to run.
    fn release(&self) -> bool {
        false
    }

    /// Get all SQL queries to execute.
    fn queries_to_execute(&self) -> Vec<String>;

    /// Evaluate the execution results and return the outcome.
    /// The results correspond to the queries returned by `queries_to_execute()`.
    async fn evaluate_results(
        &self,
        results: Vec<QueryResult>,
        options: &RunOptions,
    ) -> TestOutcome;
}

// ============================================================================
// Runnable implementations
// ============================================================================

#[async_trait]
impl Runnable for TestCase {
    fn name(&self) -> &str {
        &self.name
    }

    fn skip(&self) -> &[Skip] {
        &self.modifiers.skip
    }

    fn setups(&self) -> &[SetupRef] {
        &self.modifiers.setups
    }

    fn check_skip_conditions(&self, options: &RunOptions) -> Option<String> {
        // Check required capabilities (global + per-test)
        for req in options
            .global_requires
            .iter()
            .chain(self.modifiers.requires.iter())
        {
            if !options.backend_capabilities.contains(&req.capability) {
                return Some(format!("requires {}: {}", req.capability, req.reason));
            }
        }
        None
    }

    fn expects_error(&self) -> bool {
        matches!(self.expectations.default, Expectation::Error(_))
    }

    fn cross_check_integrity(&self) -> bool {
        self.modifiers.cross_check_integrity
    }

    fn release(&self) -> bool {
        self.modifiers.release
    }

    fn queries_to_execute(&self) -> Vec<String> {
        vec![self.sql.clone()]
    }

    async fn evaluate_results(
        &self,
        results: Vec<QueryResult>,
        options: &RunOptions,
    ) -> TestOutcome {
        // Tests use only the first (and only) result
        assert!(results.len() == 1);
        let result = results.first().unwrap();
        let expectation = self.expectations.for_backend(options.backend_type);
        let comparison = compare(result, expectation);
        match comparison {
            ComparisonResult::Match => TestOutcome::Passed,
            ComparisonResult::Mismatch { reason } => TestOutcome::Failed { reason },
        }
    }
}

#[async_trait]
impl Runnable for SnapshotCase {
    fn name(&self) -> &str {
        &self.name
    }

    fn skip(&self) -> &[Skip] {
        &self.modifiers.skip
    }

    fn setups(&self) -> &[SetupRef] {
        &self.modifiers.setups
    }

    fn check_skip_conditions(&self, options: &RunOptions) -> Option<String> {
        // Check required capabilities (global + per-snapshot)
        for req in options
            .global_requires
            .iter()
            .chain(self.modifiers.requires.iter())
        {
            if !options.backend_capabilities.contains(&req.capability) {
                return Some(format!("requires {}: {}", req.capability, req.reason));
            }
        }
        None
    }

    fn release(&self) -> bool {
        self.modifiers.release
    }

    fn queries_to_execute(&self) -> Vec<String> {
        if self.eqp_only {
            vec![format!("EXPLAIN QUERY PLAN {}", self.sql)]
        } else {
            vec![
                format!("EXPLAIN QUERY PLAN {}", self.sql),
                format!("EXPLAIN {}", self.sql),
            ]
        }
    }

    async fn evaluate_results(
        &self,
        results: Vec<QueryResult>,
        options: &RunOptions,
    ) -> TestOutcome {
        let eqp_result = results.first().expect("should have EQP result");

        if let Some(err) = &eqp_result.error {
            return TestOutcome::Error {
                message: format!("EXPLAIN QUERY PLAN failed: {err}"),
            };
        }

        let actual_output = if self.eqp_only {
            format_eqp_snapshot_content(&eqp_result.rows)
        } else {
            let explain_result = results.get(1).expect("should have EXPLAIN result");
            if let Some(err) = &explain_result.error {
                return TestOutcome::Error {
                    message: format!("EXPLAIN failed: {err}"),
                };
            }
            format_snapshot_content(&eqp_result.rows, &explain_result.rows)
        };

        // Build snapshot info with metadata
        let db_location_str = options.db_config.location.to_string();
        let snapshot_info = SnapshotInfo::new()
            .with_setups(
                self.modifiers
                    .setups
                    .iter()
                    .map(|s| s.name.clone())
                    .collect(),
            )
            .with_database(db_location_str);

        // Compare with snapshot
        let snapshot_manager =
            SnapshotManager::new(&options.file_path, options.snapshot_update_mode);
        let snapshot_result = snapshot_manager
            .compare(&self.name, &self.sql, &actual_output, &snapshot_info)
            .await;

        match snapshot_result {
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
        }
    }
}

/// Format EQP-only snapshot content (no bytecode).
fn format_eqp_snapshot_content(eqp_rows: &[Vec<String>]) -> String {
    use crate::snapshot::format_explain_query_plan_output;

    let mut output = String::new();
    output.push_str("QUERY PLAN\n");
    output.push_str(&format_explain_query_plan_output(eqp_rows));
    output
}

/// Format the combined snapshot content with both EXPLAIN QUERY PLAN and EXPLAIN output.
fn format_snapshot_content(eqp_rows: &[Vec<String>], explain_rows: &[Vec<String>]) -> String {
    use crate::snapshot::{format_explain_output, format_explain_query_plan_output};

    let mut output = String::new();

    // EXPLAIN QUERY PLAN section
    output.push_str("QUERY PLAN\n");
    output.push_str(&format_explain_query_plan_output(eqp_rows));
    output.push_str("\n\n");

    // EXPLAIN section
    output.push_str("BYTECODE\n");
    output.push_str(&format_explain_output(explain_rows));

    output
}

// ============================================================================
// Generic test runner
// ============================================================================

/// Run a single test item (test or snapshot)
async fn run_single<B: SqlBackend, R: Runnable>(
    backend: Arc<B>,
    test: R,
    options: RunOptions,
) -> TestResult {
    let start = Instant::now();

    // Capture info for panic recovery
    let item_name = test.name().to_string();
    let file_path_for_panic = options.file_path.clone();
    let db_config_for_panic = options.db_config.clone();

    let test_future = async move {
        // Check if skipped (per-item skip overrides global skip)
        for skip in test.skip().iter().chain(options.global_skip.iter()) {
            let should_skip = match &skip.condition {
                None => true, // Unconditional skip
                Some(SkipCondition::Mvcc) => options.mvcc,
                Some(SkipCondition::Sqlite) => options.backend_is_sqlite,
            };
            if should_skip {
                return TestResult {
                    name: test.name().to_string(),
                    file: options.file_path,
                    database: options.db_config,
                    outcome: TestOutcome::Skipped {
                        reason: skip.reason.clone(),
                    },
                    duration: start.elapsed(),
                };
            }
        }

        // Check if test requires release mode
        if test.release() && !options.release {
            return TestResult {
                name: test.name().to_string(),
                file: options.file_path,
                database: options.db_config,
                outcome: TestOutcome::Skipped {
                    reason: "requires release mode".to_string(),
                },
                duration: start.elapsed(),
            };
        }

        // Check additional skip conditions (e.g., capabilities)
        if let Some(reason) = test.check_skip_conditions(&options) {
            return TestResult {
                name: test.name().to_string(),
                file: options.file_path,
                database: options.db_config,
                outcome: TestOutcome::Skipped { reason },
                duration: start.elapsed(),
            };
        }

        // Create database instance
        let mut db = match backend.create_database(&options.db_config).await {
            Ok(db) => db,
            Err(e) => {
                return TestResult {
                    name: test.name().to_string(),
                    file: options.file_path,
                    database: options.db_config,
                    outcome: TestOutcome::Error {
                        message: format!("failed to create database: {e}"),
                    },
                    duration: start.elapsed(),
                };
            }
        };

        // Run setups
        for setup_ref in test.setups() {
            if let Some(setup_sql) = options.setups.get(&setup_ref.name) {
                if let Err(e) = db.execute_setup(setup_sql).await {
                    let _ = db.close().await;
                    return TestResult {
                        name: test.name().to_string(),
                        file: options.file_path,
                        database: options.db_config,
                        outcome: TestOutcome::Error {
                            message: format!("setup '{}' failed: {}", setup_ref.name, e),
                        },
                        duration: start.elapsed(),
                    };
                }
            } else {
                let _ = db.close().await;
                return TestResult {
                    name: test.name().to_string(),
                    file: options.file_path,
                    database: options.db_config,
                    outcome: TestOutcome::Error {
                        message: format!("setup '{}' not found", setup_ref.name),
                    },
                    duration: start.elapsed(),
                };
            }
        }

        // Execute all SQL queries
        let queries = test.queries_to_execute();
        let mut results = Vec::with_capacity(queries.len());
        for sql in queries {
            match db.execute(&sql).await {
                Ok(r) => results.push(r),
                Err(e) => {
                    let _ = db.close().await;
                    return TestResult {
                        name: test.name().to_string(),
                        file: options.file_path,
                        database: options.db_config,
                        outcome: TestOutcome::Error {
                            message: format!("execution failed: {e}"),
                        },
                        duration: start.elapsed(),
                    };
                }
            }
        }

        // Close database - capture the file handle to keep temp files alive
        let file_handle = match db.close().await {
            Ok(handle) => handle,
            Err(e) => {
                return TestResult {
                    name: test.name().to_string(),
                    file: options.file_path,
                    database: options.db_config,
                    outcome: TestOutcome::Error {
                        message: format!("failed to close database: {e}"),
                    },
                    duration: start.elapsed(),
                };
            }
        };

        // Evaluate results
        let mut outcome = test.evaluate_results(results, &options).await;

        // Run cross-check integrity if:
        // - test has @cross-check-integrity annotation
        // - cross-check binary is configured
        // - test passed
        // - database is not readonly (readonly DBs are pre-generated)
        // - test doesn't expect an error
        // - MVCC mode is off (MVCC databases have turso-specific format)
        if test.cross_check_integrity()
            && matches!(outcome, TestOutcome::Passed)
            && !options.db_config.readonly
            && !test.expects_error()
            && !options.mvcc
        {
            if let Some(ref binary) = options.cross_check_binary {
                if let Some(ref db_path) = file_handle.path {
                    match run_cross_check_integrity(binary, db_path).await {
                        Ok(()) => {} // integrity check passed
                        Err(msg) => {
                            outcome = TestOutcome::Failed {
                                reason: format!("cross-check integrity_check failed: {msg}"),
                            };
                        }
                    }
                }
            }
        }

        // file_handle is dropped here, cleaning up temp files
        drop(file_handle);

        TestResult {
            name: test.name().to_string(),
            file: options.file_path,
            database: options.db_config,
            outcome,
            duration: start.elapsed(),
        }
    };

    match AssertUnwindSafe(test_future).catch_unwind().await {
        Ok(result) => result,
        Err(panic_info) => {
            let panic_message = if let Some(s) = panic_info.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = panic_info.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };

            TestResult {
                name: item_name,
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
    /// Whether release mode is enabled
    pub release: bool,
    /// Snapshot update mode (Auto, New, Always, No)
    pub snapshot_update_mode: SnapshotUpdateMode,
    /// Snapshot name filter (glob pattern)
    pub snapshot_filter: Option<String>,
    /// Path to binary for cross-checking integrity (None = disabled).
    /// Used by tests annotated with `@cross-check-integrity`.
    pub cross_check_binary: Option<PathBuf>,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            max_jobs: num_cpus::get(),
            filter: None,
            mvcc: false,
            release: false,
            snapshot_update_mode: SnapshotUpdateMode::Auto,
            snapshot_filter: None,
            cross_check_binary: None,
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

    pub fn with_release(mut self, release: bool) -> Self {
        self.release = release;
        self
    }

    pub fn with_snapshot_update_mode(mut self, mode: SnapshotUpdateMode) -> Self {
        self.snapshot_update_mode = mode;
        self
    }

    pub fn with_snapshot_filter(mut self, filter: impl Into<String>) -> Self {
        self.snapshot_filter = Some(filter.into());
        self
    }

    pub fn with_cross_check_binary(mut self, binary: PathBuf) -> Self {
        self.cross_check_binary = Some(binary);
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
                if let Some(required_backend) = test.modifiers.backend {
                    if required_backend != backend_type {
                        continue;
                    }
                }

                let backend = Arc::clone(&self.backend);
                let semaphore = Arc::clone(&self.semaphore);
                let test = test.clone();

                // Upgrade :memory: to :temp: when cross-check is enabled for this test
                let effective_db_config = maybe_upgrade_memory_to_temp(
                    db_config,
                    test.modifiers.cross_check_integrity,
                    &self.config.cross_check_binary,
                );

                let options = RunOptions {
                    file_path: path.to_path_buf(),
                    db_config: effective_db_config,
                    setups: test_file.setups.clone(),
                    mvcc: self.config.mvcc,
                    release: self.config.release,
                    global_skip: test_file.global_skip.clone(),
                    global_requires: test_file.global_requires.clone(),
                    backend_capabilities: backend_capabilities.clone(),
                    backend_type,
                    backend_is_sqlite: self.backend.is_sqlite(),
                    // Tests don't use snapshots, but we still need the field
                    snapshot_update_mode: SnapshotUpdateMode::No,
                    cross_check_binary: self.config.cross_check_binary.clone(),
                };

                futures.push(tokio::spawn(async move {
                    let _permit = semaphore.acquire_owned().await.unwrap();
                    run_single(backend, test, options).await
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

        // Skip all snapshots if backend doesn't support them (e.g., sqlite3 CLI)
        if !self.backend.supports_snapshots() {
            return futures;
        }

        let backend_type = self.backend.backend_type();

        let backend_capabilities = self.backend.capabilities();

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

                // Skip snapshots that don't match the current backend
                if let Some(required_backend) = snapshot.modifiers.backend {
                    if required_backend != backend_type {
                        continue;
                    }
                }

                let backend = Arc::clone(&self.backend);
                let semaphore = Arc::clone(&self.semaphore);
                let snapshot = snapshot.clone();

                let options = RunOptions {
                    file_path: path.to_path_buf(),
                    db_config: db_config.clone(),
                    setups: test_file.setups.clone(),
                    mvcc: self.config.mvcc,
                    release: self.config.release,
                    global_skip: test_file.global_skip.clone(),
                    global_requires: test_file.global_requires.clone(),
                    backend_capabilities: backend_capabilities.clone(),
                    backend_type,
                    backend_is_sqlite: self.backend.is_sqlite(),
                    snapshot_update_mode: self.config.snapshot_update_mode,
                    cross_check_binary: self.config.cross_check_binary.clone(),
                };

                futures.push(tokio::spawn(async move {
                    let _permit = semaphore.acquire_owned().await.unwrap();
                    run_single(backend, snapshot, options).await
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

/// Upgrade :memory: databases to :temp: when a test has `@cross-check-integrity`
/// and a cross-check binary is configured, so there is a file on disk to check.
fn maybe_upgrade_memory_to_temp(
    config: &DatabaseConfig,
    cross_check_integrity: bool,
    cross_check_binary: &Option<PathBuf>,
) -> DatabaseConfig {
    if cross_check_integrity
        && cross_check_binary.is_some()
        && config.location == DatabaseLocation::Memory
    {
        DatabaseConfig {
            location: DatabaseLocation::TempFile,
            readonly: config.readonly,
        }
    } else {
        config.clone()
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

/// Run `PRAGMA integrity_check` on a database file using an external binary.
/// Returns Ok(()) if the check passes ("ok"), or Err(message) with details.
async fn run_cross_check_integrity(binary: &Path, db_path: &Path) -> Result<(), String> {
    use tokio::process::Command;

    let mut cmd = Command::new(binary);

    // Detect if binary is tursodb (needs -q and list mode flags)
    let is_turso = binary
        .file_name()
        .and_then(|n| n.to_str())
        .map(|name| name.starts_with("tursodb") || name.starts_with("turso"))
        .unwrap_or(false);

    cmd.arg(db_path);

    if is_turso {
        cmd.arg("-q");
        cmd.arg("-m").arg("list");
        cmd.arg("--experimental-triggers");
    }

    cmd.arg("PRAGMA integrity_check;");

    let output = cmd
        .output()
        .await
        .map_err(|e| format!("failed to run {}: {e}", binary.display()))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        return Err(format!(
            "{} exited with {}: {}{}",
            binary.display(),
            output.status,
            stderr.trim(),
            if !stdout.trim().is_empty() {
                format!("\n{}", stdout.trim())
            } else {
                String::new()
            }
        ));
    }

    let result = stdout.trim();
    if result == "ok" {
        Ok(())
    } else {
        Err(result.to_string())
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
