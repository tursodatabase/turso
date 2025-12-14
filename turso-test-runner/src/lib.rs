pub mod backends;
pub mod comparison;
pub mod output;
pub mod parser;
pub mod runner;

pub use backends::{BackendError, DatabaseInstance, QueryResult, SqlBackend};
pub use comparison::{compare, ComparisonResult};
pub use output::{create_output, Format, OutputFormat};
pub use parser::ast::{DatabaseConfig, DatabaseLocation, Expectation, SetupRef, TestCase, TestFile};
pub use parser::{parse, ParseError};
pub use runner::{
    summarize, FileResult, RunSummary, RunnerConfig, TestExecutor, TestOutcome, TestResult,
    TestRunner,
};
