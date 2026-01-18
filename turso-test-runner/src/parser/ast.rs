use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Range;
use std::path::PathBuf;
use std::str::FromStr;

/// Backend types for running tests
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Backend {
    /// Rust bindings backend
    Rust,
    /// CLI backend
    Cli,
    /// JavaScript bindings backend
    Js,
}

impl Backend {
    /// All known backend variants
    pub const ALL: &'static [Backend] = &[Backend::Rust, Backend::Cli, Backend::Js];
}

impl Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Backend::Rust => write!(f, "rust"),
            Backend::Cli => write!(f, "cli"),
            Backend::Js => write!(f, "js"),
        }
    }
}

impl FromStr for Backend {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "rust" => Ok(Backend::Rust),
            "cli" => Ok(Backend::Cli),
            "js" => Ok(Backend::Js),
            _ => Err(format!(
                "unknown backend '{s}', valid backends are: rust, cli, js"
            )),
        }
    }
}

/// A complete test file parsed from `.sqltest` format
#[derive(Debug, Clone, PartialEq)]
pub struct TestFile {
    /// Database configurations to run tests against
    pub databases: Vec<DatabaseConfig>,
    /// Named setup blocks that can be composed
    pub setups: HashMap<String, String>,
    /// Test cases
    pub tests: Vec<TestCase>,
    /// Global skip directive that applies to all tests in the file
    pub global_skip: Option<Skip>,
}

/// A setup reference with its span in the source
#[derive(Debug, Clone, PartialEq)]
pub struct SetupRef {
    /// Name of the setup
    pub name: String,
    /// Span of the @setup directive in the source (includes @setup and the name)
    pub span: Range<usize>,
}

/// A single test case
#[derive(Debug, Clone, PartialEq)]
pub struct TestCase {
    /// Unique name for this test
    pub name: String,
    /// Span of the test name in the source
    pub name_span: Range<usize>,
    /// SQL to execute
    pub sql: String,
    /// Expected results (with optional backend-specific overrides)
    pub expectations: Expectations,
    /// Setup references with their spans
    pub setups: Vec<SetupRef>,
    /// If set, skip this test (unconditionally or conditionally)
    pub skip: Option<Skip>,
    /// If set, only run this test on the specified backend
    pub backend: Option<Backend>,
}

/// Skip configuration for a test
#[derive(Debug, Clone, PartialEq)]
pub struct Skip {
    /// The reason for skipping
    pub reason: String,
    /// Optional condition for skipping (if None, always skip)
    pub condition: Option<SkipCondition>,
}

/// Conditions for skipping a test
#[derive(Debug, Clone, PartialEq)]
pub enum SkipCondition {
    /// Skip when MVCC mode is enabled
    Mvcc,
    /// Skip run with SQLite backend
    SQLite,
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

/// Collection of expectations with optional backend-specific overrides
#[derive(Debug, Clone, PartialEq)]
pub struct Expectations {
    /// Default expectation used when no backend-specific override exists
    pub default: Expectation,
    /// Backend-specific expectation overrides
    pub overrides: HashMap<Backend, Expectation>,
}

impl Expectations {
    /// Create expectations with just a default (no overrides)
    pub fn new(default: Expectation) -> Self {
        Self {
            default,
            overrides: HashMap::new(),
        }
    }

    /// Get the expectation for a specific backend
    pub fn for_backend(&self, backend: Backend) -> &Expectation {
        self.overrides.get(&backend).unwrap_or(&self.default)
    }
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
    /// Generated default database with INTEGER PRIMARY KEY (`:default:`)
    Default,
    /// Generated default database with INT PRIMARY KEY - no rowid alias (`:default-no-rowidalias:`)
    DefaultNoRowidAlias,
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

impl Display for DatabaseLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseLocation::Memory => f.write_str(":memory:"),
            DatabaseLocation::TempFile => f.write_str(":temp:"),
            DatabaseLocation::Path(path_buf) => write!(f, "{}", path_buf.display()),
            DatabaseLocation::Default => f.write_str(":default:"),
            DatabaseLocation::DefaultNoRowidAlias => f.write_str(":default-no-rowidalias:"),
        }
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
