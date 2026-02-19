pub mod backends;
pub mod comparison;
pub mod generator;
pub mod output;
pub mod parser;
pub mod runner;
pub mod snapshot;
pub mod tcl_converter;

pub use backends::{
    BackendError, DatabaseInstance, DefaultDatabaseResolver, QueryResult, SqlBackend,
};
pub use comparison::{ComparisonResult, compare};
pub use generator::{
    DefaultDatabaseNeeds, DefaultDatabases, GeneratorConfig,
    INTEGRITY_FIXTURE_CHECK_CONSTRAINT_VIOLATION_REL_PATH, INTEGRITY_FIXTURE_RELATIVE_PATHS,
    generate_database, generate_integrity_fixture, generate_integrity_missing_index_entry_fixture,
};
pub use output::{Format, OutputFormat, create_output};
pub use parser::ast::{
    DatabaseConfig, DatabaseLocation, Expectation, SetupRef, SnapshotCase, TestCase, TestFile,
};
pub use parser::{ParseError, parse};
pub use runner::{
    FileResult, LoadedTests, RunSummary, RunnerConfig, TestOutcome, TestResult, TestRunner,
    load_test_files, summarize,
};
pub use snapshot::{
    SnapshotManager, SnapshotResult, SnapshotUpdateMode, find_all_pending_snapshots,
    find_pending_snapshots, is_ci,
};
