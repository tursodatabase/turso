pub mod backends;
pub mod cli;
pub mod comparison;
pub mod format_real;
pub mod gap;
pub mod gap_removals;
pub mod matrix_oracle;
pub mod output;
pub mod parser;
pub mod runner;
pub mod snapshot;
pub mod tcl_converter;

pub use backends::{
    BackendError, DatabaseInstance, DefaultDatabaseResolver, QueryResult, SqlBackend,
};
pub use comparison::{ComparisonResult, compare};
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
