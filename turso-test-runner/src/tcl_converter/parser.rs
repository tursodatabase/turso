//! Error-recoverable parser for Tcl .test files
//!
//! This parser attempts to extract test cases from Tcl test files,
//! skipping constructs it doesn't understand and reporting warnings.

use chumsky::prelude::*;

use crate::tcl_converter::utils::{
    arg, braced, check_has_ddl, clean_name, clean_sql, comment, hpad, hspace, skip_line,
};

#[macro_export]
macro_rules! text_parser {
    ($lt:lifetime, $ty:ty) => {
        impl Parser<$lt, &$lt str, $ty, extra::Err<Rich<$lt, char>>>
    };
}

/// Result of converting a Tcl test file
#[derive(Debug, Clone)]
pub struct ConversionResult {
    /// Successfully parsed tests
    pub tests: Vec<TclTest>,
    /// Warnings about skipped or problematic content
    pub warnings: Vec<ConversionWarning>,
    /// Source file name for reporting
    pub source_file: String,
    /// Comments that were left at the end of the file
    pub pending_comments: Vec<String>,
}

/// A warning generated during conversion
#[derive(Debug, Clone)]
pub struct ConversionWarning {
    /// Line number where the issue was found
    pub line: usize,
    /// Kind of warning
    pub kind: WarningKind,
    /// Description of what was skipped or problematic
    pub message: String,
    /// The source line content
    pub source: String,
}

/// Types of warnings
#[derive(Debug, Clone, PartialEq)]
pub enum WarningKind {
    /// A foreach loop was skipped (generates multiple tests)
    ForeachSkipped,
    /// An if block was skipped (conditional tests)
    IfBlockSkipped,
    /// An unknown test function was encountered
    UnknownFunction,
    /// Failed to parse test arguments
    ParseError,
    /// A proc definition was skipped
    ProcSkipped,
    /// Extension loading was skipped
    ExtensionSkipped,
    /// Test uses tolerance comparison (not supported)
    ToleranceTest,
    /// Test uses skip_lines (not supported)
    SkipLinesTest,
    /// Small database test (different db)
    SmallDbTest,
}

/// A parsed test case from Tcl
#[derive(Debug, Clone)]
pub struct TclTest {
    /// Test name
    pub name: String,
    /// SQL to execute (including any DDL statements)
    pub sql: String,
    /// Type of test and expected output
    pub kind: TclTestKind,
    /// Database specifier if specific
    pub db: Option<String>,
    /// Whether the test contains DDL statements (CREATE, DROP, etc.)
    pub has_ddl: bool,
    /// Comments that appeared before this test
    pub comments: Vec<String>,
}

/// The type of test expectation
#[derive(Debug, Clone)]
pub enum TclTestKind {
    /// Exact match
    Exact(String),
    /// Regex pattern match
    Pattern(String),
    /// Error with optional pattern
    Error(Option<String>),
    /// Any error
    AnyError,
}

impl TclTest {
    /// Check if this test uses :memory: database
    pub fn uses_memory_db(&self) -> bool {
        matches!(&self.db, Some(db) if db == ":memory:")
    }

    /// Check if this test uses the default test_dbs
    pub fn uses_test_dbs(&self) -> bool {
        self.db.is_none()
    }
}

/// Configuration for building a test from parsed arguments
#[derive(Debug, Clone)]
struct TestConfig {
    /// Database override (None = use default, Some = specific db)
    db: Option<String>,
    /// How to interpret the result argument
    result_kind: ResultKind,
    /// Argument layout
    layout: ArgLayout,
}

/// How to interpret the result/expected argument
#[derive(Debug, Clone, Copy)]
enum ResultKind {
    /// Exact match expected output
    Exact,
    /// Regex pattern match
    Pattern,
    /// Error with optional pattern
    Error,
    /// Any error (no result arg needed)
    AnyError,
}

/// Argument layout for different test functions
#[derive(Debug, Clone, Copy)]
enum ArgLayout {
    /// name {sql} {expected?}
    Standard,
    /// db name {sql} {expected?}
    WithDb,
}

impl TestConfig {
    fn standard() -> Self {
        Self {
            db: None,
            result_kind: ResultKind::Exact,
            layout: ArgLayout::Standard,
        }
    }

    fn with_db(db: impl Into<String>) -> Self {
        Self {
            db: Some(db.into()),
            result_kind: ResultKind::Exact,
            layout: ArgLayout::Standard,
        }
    }

    fn memory() -> Self {
        Self::with_db(":memory:")
    }

    fn error(self) -> Self {
        Self {
            result_kind: ResultKind::Error,
            ..self
        }
    }

    fn any_error(self) -> Self {
        Self {
            result_kind: ResultKind::AnyError,
            ..self
        }
    }

    fn pattern(self) -> Self {
        Self {
            result_kind: ResultKind::Pattern,
            ..self
        }
    }

    fn db_from_arg(self) -> Self {
        Self {
            layout: ArgLayout::WithDb,
            ..self
        }
    }
}

/// Parser for Tcl test files
pub struct TclParser<'a> {
    source_file: &'a str,
    content: &'a str,
}

impl<'a> TclParser<'a> {
    pub fn new(content: &'a str, source_file: &'a str) -> Self {
        Self {
            source_file,
            content,
        }
    }

    /// Parse the entire file using the chumsky-based parser
    pub fn parse(self) -> ConversionResult {
        parse_tcl_file(self.content, self.source_file)
    }
}

/// Build a TclTest from parsed arguments using the given config
fn build_test(args: Vec<String>, config: TestConfig) -> Result<TclTest, String> {
    let (db, name_idx) = match config.layout {
        ArgLayout::Standard => (config.db, 0),
        ArgLayout::WithDb => {
            if args.is_empty() {
                return Err("Expected db argument".to_string());
            }
            (Some(args[0].trim().to_string()), 1)
        }
    };

    let min_args = name_idx + 2; // name + sql minimum
    if args.len() < min_args {
        return Err(format!(
            "Expected at least {} arguments, got {}",
            min_args,
            args.len()
        ));
    }

    let name = clean_name(&args[name_idx]);
    let sql = clean_sql(&args[name_idx + 1]);
    let result_arg = args.get(name_idx + 2).map(|s| clean_expected(s));
    let has_ddl = check_has_ddl(&sql);

    let kind = match config.result_kind {
        ResultKind::Exact => TclTestKind::Exact(result_arg.unwrap_or_default()),
        ResultKind::Pattern => TclTestKind::Pattern(result_arg.unwrap_or_else(|| ".*".to_string())),
        ResultKind::Error => TclTestKind::Error(result_arg),
        ResultKind::AnyError => TclTestKind::AnyError,
    };

    Ok(TclTest {
        name,
        sql,
        kind,
        db,
        has_ddl,
        comments: Vec::new(),
    })
}

/// Create a parser for a specific test function
fn test_fn<'a>(keyword: &'a str, config: TestConfig) -> text_parser!('a, TclTest) {
    just(keyword)
        .then_ignore(hspace())
        .ignore_then(hpad(arg()).repeated().collect::<Vec<String>>())
        .try_map(move |args, span| {
            build_test(args, config.clone()).map_err(|msg| Rich::custom(span, msg))
        })
}

/// Parser for all test function types
/// Order matters - longer/more specific keywords first!
pub fn test_parser<'a>() -> text_parser!('a, TclTest) {
    choice((
        // Specific DB tests
        test_fn(
            "do_execsql_test_skip_lines_on_specific_db",
            TestConfig::standard().db_from_arg(),
        ),
        test_fn(
            "do_execsql_test_on_specific_db",
            TestConfig::standard().db_from_arg(),
        ),
        // In-memory error tests
        test_fn(
            "do_execsql_test_in_memory_error_content",
            TestConfig::memory().error(),
        ),
        test_fn(
            "do_execsql_test_in_memory_any_error",
            TestConfig::memory().any_error(),
        ),
        test_fn(
            "do_execsql_test_in_memory_error",
            TestConfig::memory().error(),
        ),
        // Error tests
        test_fn(
            "do_execsql_test_error_content",
            TestConfig::standard().error(),
        ),
        test_fn(
            "do_execsql_test_any_error",
            TestConfig::standard().any_error(),
        ),
        test_fn("do_execsql_test_error", TestConfig::standard().error()),
        // Special tests
        test_fn("do_execsql_test_regex", TestConfig::standard().pattern()),
        test_fn(
            "do_execsql_test_tolerance",
            TestConfig::standard(), // Will need special handling
        ),
        test_fn(
            "do_execsql_test_small",
            TestConfig::with_db("testing/testing_small.db"),
        ),
        // Basic test (must be last - shortest keyword)
        test_fn("do_execsql_test", TestConfig::standard()),
    ))
}

/// Type of skipped content
#[derive(Debug, Clone)]
pub enum SkipKind {
    Foreach,
    If,
    Proc,
    Line, // set, source, etc.
    Unknown,
}

/// Item parsed from Tcl file
#[derive(Debug, Clone)]
pub enum TclItem {
    Test(TclTest),
    Comment(String),
    Skip(SkipKind, String), // kind and preview of content
}

impl TclItem {
    #[inline]
    fn comment(s: impl Into<String>) -> Self {
        Self::Comment(s.into())
    }
}

/// Full file parser
pub fn tcl_file_parser<'a>() -> text_parser!('a, Vec<TclItem>) {
    // Skip block parsers that capture preview text
    let foreach_block = just("foreach")
        .ignore_then(braced())
        .map(|preview| TclItem::Skip(SkipKind::Foreach, format!("foreach{}", preview)));

    let if_block = just("if")
        .ignore_then(braced())
        .map(|preview| TclItem::Skip(SkipKind::If, format!("if{}", preview)));

    let proc_block = just("proc")
        .ignore_then(braced())
        .map(|preview| TclItem::Skip(SkipKind::Proc, format!("proc{}", preview)));

    let item = choice((
        test_parser().map(TclItem::Test),
        comment().map(TclItem::comment),
        foreach_block,
        if_block,
        proc_block,
        skip_line().to(TclItem::Skip(SkipKind::Line, String::new())),
        // Skip unknown lines
        none_of('\n')
            .repeated()
            .at_least(1)
            .collect::<String>()
            .map(|s| TclItem::Skip(SkipKind::Unknown, s)),
    ));

    item.separated_by(just('\n').repeated())
        .allow_leading()
        .allow_trailing()
        .collect()
}

/// Parse a Tcl file and extract tests with comments
pub fn parse_tcl_file(content: &str, source_file: &str) -> ConversionResult {
    let (items, errors) = tcl_file_parser().parse(content).into_output_errors();

    let mut tests = Vec::new();
    let mut warnings = Vec::new();
    let mut pending_comments = Vec::new();
    let mut used_names = std::collections::HashSet::new();

    if let Some(items) = items {
        for item in items {
            match item {
                TclItem::Test(mut test) => {
                    // Attach pending comments
                    test.comments = std::mem::take(&mut pending_comments);

                    // Deduplicate names
                    let base_name = test.name.clone();
                    let mut final_name = base_name.clone();
                    let mut counter = 2;
                    while used_names.contains(&final_name) {
                        final_name = format!("{}-{}", base_name, counter);
                        counter += 1;
                    }
                    test.name = final_name.clone();
                    used_names.insert(final_name);

                    tests.push(test);
                }
                TclItem::Comment(c) => {
                    pending_comments.push(c);
                }
                TclItem::Skip(kind, preview) => {
                    // Generate warning for skipped content
                    let (warning_kind, message) = match kind {
                        SkipKind::Foreach => (
                            WarningKind::ForeachSkipped,
                            "foreach loop skipped - generates parameterized tests".to_string(),
                        ),
                        SkipKind::If => (
                            WarningKind::IfBlockSkipped,
                            "if block skipped - conditional tests need manual review".to_string(),
                        ),
                        SkipKind::Proc => (
                            WarningKind::ProcSkipped,
                            "Proc definition skipped".to_string(),
                        ),
                        SkipKind::Line | SkipKind::Unknown => {
                            // Don't generate warnings for simple skips
                            continue;
                        }
                    };

                    warnings.push(ConversionWarning {
                        line: 0,
                        kind: warning_kind,
                        message,
                        source: preview.chars().take(100).collect(),
                    });
                }
            }
        }
    }

    // Add parse errors as warnings
    for e in errors {
        warnings.push(ConversionWarning {
            line: 0,
            kind: WarningKind::ParseError,
            message: e.to_string(),
            source: String::new(),
        });
    }

    ConversionResult {
        tests,
        warnings,
        source_file: source_file.to_string(),
        pending_comments,
    }
}

/// Parse a Tcl list string into its elements
/// Handles: {element1} {element2}, plain words, multiline braced elements
fn parse_tcl_list<'a>() -> text_parser!('a, Vec<String>) {
    // Braced: {content with {nested} braces}
    let braced = braced();

    // Line of unbraced text (newline-terminated)
    let unbraced = none_of("{}\n")
        .repeated()
        .at_least(1)
        .collect::<String>()
        .map(|s| s.trim().to_string());

    // An element on a line
    let line_element = hspace().ignore_then(choice((braced, unbraced)));

    // Multiple elements can be on same line or different lines
    line_element
        .separated_by(one_of(" \t\n").repeated().at_least(1))
        .allow_leading()
        .allow_trailing()
        .collect::<Vec<_>>()
        .map(|v| v.into_iter().filter(|s| !s.is_empty()).collect())
}

/// Clean up expected output (convert Tcl list format to pipe-separated)
fn clean_expected(s: &str) -> String {
    // First, strip outer quotes if the entire content is wrapped in them
    // This handles {"content"} where we get "content" after brace extraction
    let s = s.trim();
    let was_quoted = s.starts_with('"') && s.ends_with('"') && s.len() >= 2;
    let s = if was_quoted { &s[1..s.len() - 1] } else { s };

    // Parse the whole content as a Tcl list (handles multiline braced elements)
    let elements = parse_tcl_list().parse(s).into_result().unwrap_or_default();

    let mut result = String::new();

    for element in elements {
        // Each element may contain newlines - split into rows
        for line in element.lines() {
            let trimmed = if was_quoted {
                line.trim_start()
            } else {
                line.trim()
            };

            if trimmed.is_empty() {
                continue;
            }

            if !result.is_empty() {
                result.push('\n');
            }

            // Handle Tcl escapes
            let unescaped = trimmed
                .replace("\\\"", "\"")
                .replace("\\{", "{")
                .replace("\\}", "}")
                .replace("\\[", "[")
                .replace("\\]", "]")
                .replace("\\$", "$")
                .replace("\\\\", "\\");
            result.push_str(&unescaped);
        }
    }

    result
}

impl std::fmt::Display for WarningKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WarningKind::ForeachSkipped => write!(f, "FOREACH"),
            WarningKind::IfBlockSkipped => write!(f, "IF_BLOCK"),
            WarningKind::UnknownFunction => write!(f, "UNKNOWN"),
            WarningKind::ParseError => write!(f, "PARSE_ERROR"),
            WarningKind::ProcSkipped => write!(f, "PROC"),
            WarningKind::ExtensionSkipped => write!(f, "EXTENSION"),
            WarningKind::ToleranceTest => write!(f, "TOLERANCE"),
            WarningKind::SkipLinesTest => write!(f, "SKIP_LINES"),
            WarningKind::SmallDbTest => write!(f, "SMALL_DB"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tcl_converter::convert;

    use super::*;

    #[test]
    fn test_parse_basic_test() {
        let content = r#"
do_execsql_test select-const-1 {
  SELECT 1
} {1}
"#;

        let result = convert(content, "test.test");
        assert!(result.warnings.is_empty());
        assert_eq!(result.tests.len(), 1);
        assert_eq!(result.tests[0].name, "select-const-1");
        assert_eq!(result.tests[0].sql.trim(), "SELECT 1");
    }

    #[test]
    fn test_parse_error_test() {
        let content = r#"
do_execsql_test_error select-missing-table {
    SELECT * FROM nonexistent;
} {.*no such table.*}
"#;

        let result = convert(content, "test.test");
        assert!(result.warnings.is_empty());
        assert_eq!(result.tests.len(), 1);
        assert!(matches!(result.tests[0].kind, TclTestKind::Error(_)));
    }

    #[test]
    fn test_skip_foreach() {
        let content = r#"
foreach {testname lhs ans} {
  int-1           1        0
  int-2           2        0
} {
  do_execsql_test boolean-not-$testname "SELECT not $lhs" $::ans
}
"#;

        let result = convert(content, "test.test");
        assert!(!result.warnings.is_empty());
        assert_eq!(result.warnings[0].kind, WarningKind::ForeachSkipped);
    }
}
