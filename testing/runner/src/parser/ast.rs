use std::collections::{HashMap, HashSet};
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

/// Backend capabilities that tests can require
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {
    /// Support for CREATE TRIGGER
    Trigger,
    /// Support for STRICT tables
    Strict,
    /// Support for MATERIALIZED VIEW (experimental)
    MaterializedViews,
}

impl Capability {
    /// All known capability variants
    pub const ALL: &'static [Capability] = &[
        Capability::Trigger,
        Capability::Strict,
        Capability::MaterializedViews,
    ];

    /// Get all capabilities as a HashSet (convenience for backends that support everything)
    pub fn all_set() -> HashSet<Capability> {
        Self::ALL.iter().copied().collect()
    }
}

impl Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Capability::Trigger => write!(f, "trigger"),
            Capability::Strict => write!(f, "strict"),
            Capability::MaterializedViews => write!(f, "materialized_views"),
        }
    }
}

impl FromStr for Capability {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "trigger" => Ok(Capability::Trigger),
            "strict" => Ok(Capability::Strict),
            "materialized_views" => Ok(Capability::MaterializedViews),
            _ => Err(format!(
                "unknown capability '{s}', valid capabilities are: trigger, strict, materialized_views"
            )),
        }
    }
}

/// A capability requirement with a reason
#[derive(Debug, Clone, PartialEq)]
pub struct Requirement {
    /// The required capability
    pub capability: Capability,
    /// Reason why this capability is required
    pub reason: String,
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
    /// Snapshot test cases (for EXPLAIN output)
    pub snapshots: Vec<SnapshotCase>,
    /// Global skip directive that applies to all tests in the file
    pub global_skip: Option<Skip>,
    /// Global capability requirements that apply to all tests in the file
    pub global_requires: Vec<Requirement>,
}

/// A setup reference with its span in the source
#[derive(Debug, Clone, PartialEq)]
pub struct SetupRef {
    /// Name of the setup
    pub name: String,
    /// Span of the @setup directive in the source (includes @setup and the name)
    pub span: Range<usize>,
}

/// Common modifiers shared by test and snapshot cases
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CaseModifiers {
    /// Setup references with their spans
    pub setups: Vec<SetupRef>,
    /// If set, skip this case (unconditionally or conditionally)
    pub skip: Option<Skip>,
    /// If set, only run this case on the specified backend
    pub backend: Option<Backend>,
    /// Required capabilities for this case
    pub requires: Vec<Requirement>,
}

/// A snapshot test case (for EXPLAIN output)
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotCase {
    /// Unique name for this snapshot test
    pub name: String,
    /// Span of the snapshot name in the source
    pub name_span: Range<usize>,
    /// SQL to execute (EXPLAIN will be prepended)
    pub sql: String,
    /// Common modifiers (setups, skip, backend, requires)
    pub modifiers: CaseModifiers,
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
    /// Common modifiers (setups, skip, backend, requires)
    pub modifiers: CaseModifiers,
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
    /// Skip when running against the sqlite CLI backend
    Sqlite,
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
