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
    /// PostgreSQL wire-protocol backend (tursopg server)
    Pg,
}

impl Backend {
    /// All known backend variants
    pub const ALL: &'static [Backend] = &[Backend::Rust, Backend::Cli, Backend::Js, Backend::Pg];
}

impl Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Backend::Rust => write!(f, "rust"),
            Backend::Cli => write!(f, "cli"),
            Backend::Js => write!(f, "js"),
            Backend::Pg => write!(f, "pg"),
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
            "pg" => Ok(Backend::Pg),
            _ => Err(format!(
                "unknown backend '{s}', valid backends are: rust, cli, js, pg"
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
    /// Support for custom types (CREATE TYPE / DROP TYPE)
    CustomTypes,
}

impl Capability {
    /// All known capability variants
    pub const ALL: &'static [Capability] = &[
        Capability::Trigger,
        Capability::Strict,
        Capability::MaterializedViews,
        Capability::CustomTypes,
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
            Capability::CustomTypes => write!(f, "custom_types"),
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
            "custom_types" => Ok(Capability::CustomTypes),
            _ => Err(format!(
                "unknown capability '{s}', valid capabilities are: trigger, strict, materialized_views, custom_types"
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
    /// Matrix test cases (cross-product expansions with blessed results)
    pub matrices: Vec<MatrixCase>,
    /// Global skip directives that apply to all tests in the file
    pub global_skip: Vec<Skip>,
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
    pub skip: Vec<Skip>,
    /// If set, only run this case on the specified backend
    pub backend: Option<Backend>,
    /// Required capabilities for this case
    pub requires: Vec<Requirement>,
    /// If true, cross-check the resulting database file with another binary's
    /// `PRAGMA integrity_check` after the test passes.
    pub cross_check_integrity: bool,
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
    /// If true, only run EXPLAIN QUERY PLAN (no bytecode).
    /// Set by the `snapshot-eqp` directive.
    pub eqp_only: bool,
    /// Common modifiers (setups, skip, backend, requires)
    pub modifiers: CaseModifiers,
}

/// A matrix variable: a substitution name plus the values it expands over.
#[derive(Debug, Clone, PartialEq)]
pub struct MatrixVar {
    /// Name referenced in the SQL template as `$name`
    pub name: String,
    /// Values the variable iterates over (may include the empty string)
    pub values: Vec<String>,
}

/// A matrix test case: one SQL template expanded over the cross-product of
/// its `@var` decorators. Each expansion becomes an individual case named
/// `<matrix>[<slug>]...[<slug>]` and is verified differentially at run
/// time: Turso and the bundled SQLite oracle must produce the same rows,
/// or both must reject the statement.
#[derive(Debug, Clone, PartialEq)]
pub struct MatrixCase {
    /// Base name for the expansions
    pub name: String,
    /// Span of the matrix name in the source
    pub name_span: Range<usize>,
    /// SQL template with `$name` substitution points
    pub sql_template: String,
    /// Variables, in declaration order (also the order of name suffixes)
    pub vars: Vec<MatrixVar>,
    /// Common modifiers (setups, skip, backend, requires)
    pub modifiers: CaseModifiers,
}

/// One expansion of a matrix case: a concrete name and SQL.
#[derive(Debug, Clone, PartialEq)]
pub struct MatrixExpansion {
    /// Generated case name, e.g. `frame-sum[rows][1-preceding][current-row]`
    pub name: String,
    /// SQL with all variables substituted
    pub sql: String,
}

impl MatrixCase {
    /// Expand the cross-product of all variables into concrete cases.
    /// Substitution replaces longer variable names first so `$start` is
    /// never clobbered by a variable named `$s`.
    pub fn expand(&self) -> Vec<MatrixExpansion> {
        let mut order: Vec<usize> = (0..self.vars.len()).collect();
        order.sort_by_key(|&i| std::cmp::Reverse(self.vars[i].name.len()));

        let mut expansions = Vec::new();
        let mut indices = vec![0usize; self.vars.len()];
        loop {
            let mut sql = self.sql_template.clone();
            for &vi in &order {
                let var = &self.vars[vi];
                sql = sql.replace(&format!("${}", var.name), &var.values[indices[vi]]);
            }
            let mut name = self.name.clone();
            for (vi, var) in self.vars.iter().enumerate() {
                name.push('[');
                name.push_str(&slug(&var.values[indices[vi]]));
                name.push(']');
            }
            expansions.push(MatrixExpansion { name, sql });

            // Advance the odometer.
            let mut pos = self.vars.len();
            loop {
                if pos == 0 {
                    return expansions;
                }
                pos -= 1;
                indices[pos] += 1;
                if indices[pos] < self.vars[pos].values.len() {
                    break;
                }
                indices[pos] = 0;
            }
        }
    }

    /// Total number of expansions this matrix produces.
    pub fn expansion_count(&self) -> usize {
        self.vars.iter().map(|v| v.values.len().max(1)).product()
    }
}

/// Turn a variable value into a stable, readable name fragment.
fn slug(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut last_dash = true;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        "none".to_string()
    } else {
        out
    }
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
