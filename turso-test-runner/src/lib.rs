pub mod backends;
pub mod comparison;
pub mod parser;
pub mod runner;

pub use backends::{BackendError, DatabaseInstance, QueryResult, SqlBackend};
pub use comparison::{compare, ComparisonResult};
pub use parser::ast::{DatabaseConfig, DatabaseLocation, Expectation, TestCase, TestFile};
pub use parser::{parse, ParseError};
pub use runner::{
    summarize, FileResult, RunSummary, RunnerConfig, TestExecutor, TestOutcome, TestResult,
    TestRunner,
};
