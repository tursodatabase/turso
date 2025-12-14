pub mod backends;
pub mod comparison;
pub mod output;
pub mod parser;
pub mod runner;

pub use backends::{BackendError, DatabaseInstance, QueryResult, SqlBackend};
pub use comparison::{ComparisonResult, compare};
pub use output::{Format, OutputFormat, create_output};
pub use parser::ast::{
    DatabaseConfig, DatabaseLocation, Expectation, SetupRef, TestCase, TestFile,
};
pub use parser::{ParseError, parse};
pub use runner::{
    FileResult, RunSummary, RunnerConfig, TestOutcome, TestResult, TestRunner, summarize,
};
