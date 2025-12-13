use std::collections::HashMap;
use std::path::PathBuf;

/// A complete test file parsed from `.sqltest` format
#[derive(Debug, Clone, PartialEq)]
pub struct TestFile {
    /// Database configurations to run tests against
    pub databases: Vec<DatabaseConfig>,
    /// Named setup blocks that can be composed
    pub setups: HashMap<String, String>,
    /// Test cases
    pub tests: Vec<TestCase>,
}

/// A single test case
#[derive(Debug, Clone, PartialEq)]
pub struct TestCase {
    /// Unique name for this test
    pub name: String,
    /// SQL to execute
    pub sql: String,
    /// Expected result
    pub expectation: Expectation,
    /// Names of setups to apply before the test (in order)
    pub setups: Vec<String>,
    /// If set, skip this test with the given reason
    pub skip: Option<String>,
}

/// What we expect from executing the SQL
#[derive(Debug, Clone, PartialEq)]
pub enum Expectation {
    /// Exact row-by-row match (pipe-separated columns)
    Exact(Vec<String>),
    /// Match output against regex pattern
    Pattern(String),
    /// Compare as sets (order doesn't matter)
    Unordered(Vec<String>),
    /// Expect an error with optional pattern match
    Error(Option<String>),
}

/// Database configuration
#[derive(Debug, Clone, PartialEq)]
pub struct DatabaseConfig {
    /// Where the database is located
    pub location: DatabaseLocation,
    /// Whether the database is read-only
    pub readonly: bool,
}

/// Database location type
#[derive(Debug, Clone, PartialEq)]
pub enum DatabaseLocation {
    /// In-memory database (`:memory:`)
    Memory,
    /// Temporary file database (`:temp:`)
    TempFile,
    /// Path to an existing database file
    Path(PathBuf),
}

impl DatabaseConfig {
    /// Check if this is a writable database (memory or temp)
    pub fn is_writable(&self) -> bool {
        !self.readonly
            && matches!(
                self.location,
                DatabaseLocation::Memory | DatabaseLocation::TempFile
            )
    }
}

impl TestFile {
    /// Check if this file has only writable databases
    pub fn is_writable_file(&self) -> bool {
        self.databases.iter().all(|db| db.is_writable())
    }

    /// Check if this file has only readonly databases
    pub fn is_readonly_file(&self) -> bool {
        self.databases.iter().all(|db| db.readonly)
    }
}
