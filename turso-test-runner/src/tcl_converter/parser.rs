//! Error-recoverable parser for Tcl .test files
//!
//! This parser attempts to extract test cases from Tcl test files,
//! skipping constructs it doesn't understand and reporting warnings.

use chumsky::prelude::*;

use crate::tcl_converter::utils::{
    arg, braced, check_has_ddl, clean_name, clean_sql, comment, hpad, hspace, skip_block,
    skip_line,
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
    /// skip_lines db name {sql} {expected?}
    SkipLinesWithDb,
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

    fn skip_lines_db_from_arg(self) -> Self {
        Self {
            layout: ArgLayout::SkipLinesWithDb,
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
        ArgLayout::SkipLinesWithDb => {
            // skip_lines db name {sql} {expected}
            if args.len() < 2 {
                return Err("Expected skip_lines and db arguments".to_string());
            }
            // args[0] is skip_lines (we ignore it), args[1] is db
            (Some(args[1].trim().to_string()), 2)
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
        // Specific DB tests with skip_lines (format: skip_lines db name sql expected)
        test_fn(
            "do_execsql_test_skip_lines_on_specific_db",
            TestConfig::standard().skip_lines_db_from_arg(),
        ),
        // Specific DB tests (format: db name sql expected)
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
    // Skip block parsers that capture the entire block content
    let foreach_block =
        skip_block("foreach").map(|preview| TclItem::Skip(SkipKind::Foreach, preview));

    let if_block = skip_block("if").map(|preview| TclItem::Skip(SkipKind::If, preview));

    let proc_block = skip_block("proc").map(|preview| TclItem::Skip(SkipKind::Proc, preview));

    // Each item can have leading horizontal whitespace
    let item = hspace().ignore_then(choice((
        test_parser().map(TclItem::Test),
        comment().map(TclItem::comment),
        foreach_block,
        if_block,
        proc_block,
        skip_line().to(TclItem::Skip(SkipKind::Line, String::new())),
        // Skip unknown lines (but only non-empty ones after whitespace)
        none_of(" \t\n")
            .then(none_of('\n').repeated())
            .to_slice()
            .map(|s: &str| TclItem::Skip(SkipKind::Unknown, s.to_string())),
    )));

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

    // ==================== test_parser() tests ====================

    #[test]
    fn test_parser_do_execsql_test() {
        let input = "do_execsql_test mytest {SELECT 1} {1}";
        let result = test_parser().parse(input).into_result();
        assert!(result.is_ok());
        let test = result.unwrap();
        assert_eq!(test.name, "mytest");
        assert_eq!(test.sql.trim(), "SELECT 1");
        assert!(matches!(test.kind, TclTestKind::Exact(ref s) if s == "1"));
    }

    #[test]
    fn test_parser_do_execsql_test_error() {
        let input = "do_execsql_test_error mytest {SELECT * FROM missing} {.*no such table.*}";
        let result = test_parser().parse(input).into_result();
        assert!(result.is_ok());
        let test = result.unwrap();
        assert!(matches!(test.kind, TclTestKind::Error(Some(ref p)) if p.contains("no such table")));
    }

    #[test]
    fn test_parser_do_execsql_test_any_error() {
        let input = "do_execsql_test_any_error mytest {SELECT * FROM missing}";
        let result = test_parser().parse(input).into_result();
        assert!(result.is_ok());
        let test = result.unwrap();
        assert!(matches!(test.kind, TclTestKind::AnyError));
    }

    #[test]
    fn test_parser_do_execsql_test_in_memory() {
        let input = "do_execsql_test_in_memory_error mytest {CREATE TABLE t(x)} {.*}";
        let result = test_parser().parse(input).into_result();
        assert!(result.is_ok());
        let test = result.unwrap();
        assert_eq!(test.db, Some(":memory:".to_string()));
    }

    #[test]
    fn test_parser_do_execsql_test_regex() {
        let input = "do_execsql_test_regex mytest {SELECT date('now')} {\\d{4}-\\d{2}-\\d{2}}";
        let result = test_parser().parse(input).into_result();
        assert!(result.is_ok());
        let test = result.unwrap();
        assert!(matches!(test.kind, TclTestKind::Pattern(_)));
    }

    #[test]
    fn test_parser_do_execsql_test_small() {
        let input = "do_execsql_test_small mytest {SELECT 1} {1}";
        let result = test_parser().parse(input).into_result();
        assert!(result.is_ok());
        let test = result.unwrap();
        assert!(test.db.as_ref().unwrap().contains("small"));
    }

    #[test]
    fn test_parser_do_execsql_test_on_specific_db() {
        let input = "do_execsql_test_on_specific_db mydb mytest {SELECT 1} {1}";
        let result = test_parser().parse(input).into_result();
        assert!(result.is_ok());
        let test = result.unwrap();
        assert_eq!(test.db, Some("mydb".to_string()));
    }

    #[test]
    fn test_parser_multiline_sql() {
        let input = r#"do_execsql_test mytest {
  SELECT *
  FROM users
  WHERE id = 1
} {1 alice}"#;
        let result = test_parser().parse(input).into_result();
        assert!(result.is_ok());
        let test = result.unwrap();
        assert!(test.sql.contains("SELECT"));
        assert!(test.sql.contains("FROM users"));
    }

    #[test]
    fn test_parser_nested_braces_in_sql() {
        let input = "do_execsql_test mytest {SELECT json_object('a', 1)} {{\"a\":1}}";
        let result = test_parser().parse(input).into_result();
        assert!(result.is_ok());
    }

    // ==================== tcl_file_parser() tests ====================

    #[test]
    fn test_file_parser_single_test() {
        let input = "do_execsql_test test1 {SELECT 1} {1}";
        let result = tcl_file_parser().parse(input).into_result();
        assert!(result.is_ok());
        let items = result.unwrap();
        assert_eq!(items.len(), 1);
        assert!(matches!(items[0], TclItem::Test(_)));
    }

    #[test]
    fn test_file_parser_multiple_tests() {
        let input = r#"do_execsql_test test1 {SELECT 1} {1}
do_execsql_test test2 {SELECT 2} {2}
do_execsql_test test3 {SELECT 3} {3}"#;
        let result = tcl_file_parser().parse(input).into_result();
        assert!(result.is_ok());
        let items = result.unwrap();
        let tests: Vec<_> = items.iter().filter(|i| matches!(i, TclItem::Test(_))).collect();
        assert_eq!(tests.len(), 3);
    }

    #[test]
    fn test_file_parser_with_comments() {
        let input = r#"# This is a comment
do_execsql_test test1 {SELECT 1} {1}
# Another comment"#;
        let result = tcl_file_parser().parse(input).into_result();
        assert!(result.is_ok());
        let items = result.unwrap();
        let comments: Vec<_> = items.iter().filter(|i| matches!(i, TclItem::Comment(_))).collect();
        assert_eq!(comments.len(), 2);
    }

    #[test]
    fn test_file_parser_foreach_skipped() {
        let input = r#"foreach {a b} {1 2 3 4} {
  do_execsql_test test-$a {SELECT $b} {$b}
}"#;
        let result = tcl_file_parser().parse(input).into_result();
        assert!(result.is_ok());
        let items = result.unwrap();
        let skips: Vec<_> = items.iter().filter(|i| matches!(i, TclItem::Skip(SkipKind::Foreach, _))).collect();
        assert!(!skips.is_empty());
    }

    #[test]
    fn test_file_parser_if_skipped() {
        let input = r#"if {$condition} {
  do_execsql_test test1 {SELECT 1} {1}
}"#;
        let result = tcl_file_parser().parse(input).into_result();
        assert!(result.is_ok());
        let items = result.unwrap();
        let skips: Vec<_> = items.iter().filter(|i| matches!(i, TclItem::Skip(SkipKind::If, _))).collect();
        assert!(!skips.is_empty());
    }

    #[test]
    fn test_file_parser_proc_skipped() {
        let input = r#"proc myproc {args} {
  return $args
}"#;
        let result = tcl_file_parser().parse(input).into_result();
        assert!(result.is_ok());
        let items = result.unwrap();
        let skips: Vec<_> = items.iter().filter(|i| matches!(i, TclItem::Skip(SkipKind::Proc, _))).collect();
        assert!(!skips.is_empty());
    }

    #[test]
    fn test_file_parser_set_skipped() {
        let input = "set testdir [file dirname $argv0]";
        let result = tcl_file_parser().parse(input).into_result();
        assert!(result.is_ok());
        let items = result.unwrap();
        let skips: Vec<_> = items.iter().filter(|i| matches!(i, TclItem::Skip(SkipKind::Line, _))).collect();
        assert!(!skips.is_empty());
    }

    #[test]
    fn test_file_parser_source_skipped() {
        let input = "source $testdir/tester.tcl";
        let result = tcl_file_parser().parse(input).into_result();
        assert!(result.is_ok());
    }

    // ==================== parse_tcl_file() integration tests ====================

    #[test]
    fn test_parse_tcl_file_basic() {
        let content = r#"
do_execsql_test select-const-1 {
  SELECT 1
} {1}
"#;

        let result = convert(content, "test.test");
        assert!(result.warnings.is_empty(), "Got warnings: {:?}", result.warnings);
        assert_eq!(result.tests.len(), 1);
        assert_eq!(result.tests[0].name, "select-const-1");
        assert_eq!(result.tests[0].sql.trim(), "SELECT 1");
    }

    #[test]
    fn test_parse_tcl_file_error_test() {
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
    fn test_parse_tcl_file_foreach_warning() {
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

    #[test]
    fn test_parse_tcl_file_comment_attachment() {
        let content = r#"# This test checks something
do_execsql_test test1 {SELECT 1} {1}"#;

        let result = convert(content, "test.test");
        assert_eq!(result.tests.len(), 1);
        assert_eq!(result.tests[0].comments.len(), 1);
        assert!(result.tests[0].comments[0].contains("This test checks"));
    }

    #[test]
    fn test_parse_tcl_file_name_deduplication() {
        let content = r#"do_execsql_test test1 {SELECT 1} {1}
do_execsql_test test1 {SELECT 2} {2}
do_execsql_test test1 {SELECT 3} {3}"#;

        let result = convert(content, "test.test");
        assert_eq!(result.tests.len(), 3);
        assert_eq!(result.tests[0].name, "test1");
        assert_eq!(result.tests[1].name, "test1-2");
        assert_eq!(result.tests[2].name, "test1-3");
    }

    #[test]
    fn test_parse_tcl_file_ddl_detection() {
        let content = r#"do_execsql_test test1 {
  CREATE TABLE t1(x);
  INSERT INTO t1 VALUES(1);
  SELECT * FROM t1;
} {1}"#;

        let result = convert(content, "test.test");
        assert_eq!(result.tests.len(), 1);
        assert!(result.tests[0].has_ddl);
    }

    #[test]
    fn test_parse_tcl_file_no_ddl() {
        let content = r#"do_execsql_test test1 {SELECT 1 + 1} {2}"#;

        let result = convert(content, "test.test");
        assert_eq!(result.tests.len(), 1);
        assert!(!result.tests[0].has_ddl);
    }

    #[test]
    fn test_parse_tcl_file_mixed_content() {
        let content = r#"#!/usr/bin/tclsh
set testdir [file dirname $argv0]
source $testdir/tester.tcl

# Test basic select
do_execsql_test basic-1 {SELECT 1} {1}

# Test arithmetic
do_execsql_test arith-1 {SELECT 1 + 1} {2}
"#;

        let result = convert(content, "test.test");
        assert_eq!(result.tests.len(), 2);
        assert_eq!(result.tests[0].name, "basic-1");
        assert_eq!(result.tests[1].name, "arith-1");
    }

    #[test]
    fn test_parse_tcl_file_multiline_expected() {
        let content = r#"do_execsql_test test1 {
  SELECT * FROM (VALUES (1, 'a'), (2, 'b'))
} {1 a
2 b}"#;

        let result = convert(content, "test.test");
        assert_eq!(result.tests.len(), 1);
        if let TclTestKind::Exact(expected) = &result.tests[0].kind {
            assert!(expected.contains("1 a"));
            assert!(expected.contains("2 b"));
        } else {
            panic!("Expected Exact kind");
        }
    }

    // ==================== build_test() tests ====================

    #[test]
    fn test_build_test_standard() {
        let args = vec!["test-name".to_string(), "SELECT 1".to_string(), "1".to_string()];
        let result = build_test(args, TestConfig::standard());
        assert!(result.is_ok());
        let test = result.unwrap();
        assert_eq!(test.name, "test-name");
        assert!(test.db.is_none());
    }

    #[test]
    fn test_build_test_with_db() {
        let args = vec!["mydb".to_string(), "test-name".to_string(), "SELECT 1".to_string(), "1".to_string()];
        let result = build_test(args, TestConfig::standard().db_from_arg());
        assert!(result.is_ok());
        let test = result.unwrap();
        assert_eq!(test.db, Some("mydb".to_string()));
    }

    #[test]
    fn test_build_test_memory() {
        let args = vec!["test-name".to_string(), "SELECT 1".to_string(), "1".to_string()];
        let result = build_test(args, TestConfig::memory());
        assert!(result.is_ok());
        let test = result.unwrap();
        assert_eq!(test.db, Some(":memory:".to_string()));
    }

    #[test]
    fn test_build_test_error_kind() {
        let args = vec!["test-name".to_string(), "SELECT * FROM missing".to_string(), ".*error.*".to_string()];
        let result = build_test(args, TestConfig::standard().error());
        assert!(result.is_ok());
        let test = result.unwrap();
        assert!(matches!(test.kind, TclTestKind::Error(_)));
    }

    #[test]
    fn test_build_test_any_error_kind() {
        let args = vec!["test-name".to_string(), "SELECT * FROM missing".to_string()];
        let result = build_test(args, TestConfig::standard().any_error());
        assert!(result.is_ok());
        let test = result.unwrap();
        assert!(matches!(test.kind, TclTestKind::AnyError));
    }

    #[test]
    fn test_build_test_insufficient_args() {
        let args = vec!["test-name".to_string()];
        let result = build_test(args, TestConfig::standard());
        assert!(result.is_err());
    }

    // ==================== Edge cases ====================

    #[test]
    fn test_empty_file() {
        let result = convert("", "test.test");
        assert!(result.tests.is_empty());
    }

    #[test]
    fn test_only_comments() {
        let content = "# comment 1\n# comment 2\n# comment 3";
        let result = convert(content, "test.test");
        assert!(result.tests.is_empty());
        // Pending comments should be captured
        assert!(!result.pending_comments.is_empty());
    }

    #[test]
    fn test_whitespace_handling() {
        // Tests with leading horizontal whitespace (common in indented files)
        let content = "  do_execsql_test test1 {SELECT 1} {1}";
        let result = convert(content, "test.test");
        assert_eq!(result.tests.len(), 1);
    }

    #[test]
    fn test_on_specific_db_parsing() {
        let content = r#"do_execsql_test_on_specific_db {:memory:} test-name {
  SELECT 1
} {1}"#;
        let result = convert(content, "test.test");
        println!("Tests: {:?}", result.tests);
        println!("Warnings: {:?}", result.warnings);
        assert_eq!(result.tests.len(), 1, "Expected 1 test");
        assert_eq!(result.tests[0].db, Some(":memory:".to_string()));
        assert_eq!(result.tests[0].name, "test-name");
    }

    #[test]
    fn test_on_specific_db_indented() {
        let content = r#"  do_execsql_test_on_specific_db {:memory:} select-union-1 {
  CREATE TABLE t (x TEXT);
  select * from t;
  } {x|x
  y|y}"#;
        let result = convert(content, "test.test");
        println!("Tests: {:?}", result.tests);
        println!("Warnings: {:?}", result.warnings);
        assert_eq!(result.tests.len(), 1, "Expected 1 test");
        assert_eq!(result.tests[0].db, Some(":memory:".to_string()));
    }

    #[test]
    fn test_multiple_on_specific_db() {
        let content = r#"do_execsql_test_on_specific_db {:memory:} test1 {
  SELECT 1
} {1}

do_execsql_test_on_specific_db {:memory:} test2 {
  SELECT 2
} {2}

do_execsql_test_on_specific_db {:memory:} test3 {
  SELECT 3
} {3}"#;
        let result = convert(content, "test.test");
        println!("Tests: {:?}", result.tests.len());
        for t in &result.tests {
            println!("  Test: {} db={:?}", t.name, t.db);
        }
        println!("Warnings: {:?}", result.warnings);
        assert_eq!(result.tests.len(), 3, "Expected 3 tests");
    }

    #[test]
    fn test_on_specific_db_extra_whitespace() {
        // Two spaces after {:memory:}
        let content = r#"do_execsql_test_on_specific_db {:memory:}  limit-expr-can-be-cast-losslessly-1 {
  SELECT 1 LIMIT 1.1 + 2.9;
} {1}"#;
        let result = convert(content, "test.test");
        println!("Tests: {:?}", result.tests);
        println!("Warnings: {:?}", result.warnings);
        assert_eq!(result.tests.len(), 1, "Expected 1 test");
        assert_eq!(result.tests[0].name, "limit-expr-can-be-cast-losslessly-1");
    }

    #[test]
    fn test_in_memory_error_content() {
        let content = r#"do_execsql_test_in_memory_error_content  limit-expr-cannot-be-cast-losslessly-1 {
  SELECT 1 LIMIT '1';
} {cannot be losslessly converted}"#;
        let result = convert(content, "test.test");
        println!("Tests: {:?}", result.tests);
        println!("Warnings: {:?}", result.warnings);
        assert_eq!(result.tests.len(), 1, "Expected 1 test");
    }

    #[test]
    fn test_multiline_expected_indented() {
        let content = r#"  do_execsql_test_on_specific_db {:memory:} select-intersect-union-with-limit {
    CREATE TABLE t (x TEXT, y TEXT);
    select * from t limit 3;
  } {a|a
  b|b
  z|z}"#;
        let result = convert(content, "test.test");
        println!("Tests: {:?}", result.tests);
        println!("Warnings: {:?}", result.warnings);
        assert_eq!(result.tests.len(), 1, "Expected 1 test");
        assert_eq!(result.tests[0].name, "select-intersect-union-with-limit");
    }

    #[test]
    fn test_consecutive_indented_tests() {
        let content = r#"  do_execsql_test_on_specific_db {:memory:} test1 {
    SELECT 1;
  } {1}

  do_execsql_test_on_specific_db {:memory:} test2 {
    SELECT 2;
  } {2}"#;
        let result = convert(content, "test.test");
        println!("Tests: {:?}", result.tests);
        println!("Warnings: {:?}", result.warnings);
        assert_eq!(result.tests.len(), 2, "Expected 2 tests");
    }

    #[test]
    fn test_full_select_file() {
        let content = std::fs::read_to_string("../testing/select.test").unwrap();
        let result = convert(&content, "select.test");
        println!("Total tests: {}", result.tests.len());
        println!("Warnings: {}", result.warnings.len());
        for w in &result.warnings {
            println!("  {:?}: {}", w.kind, w.message);
        }
        // 172 total - 2 commented - 1 in foreach = 169
        assert!(result.tests.len() > 100, "Should have over 100 tests");
    }

    #[test]
    fn test_raw_file_parsing() {
        let content = std::fs::read_to_string("../testing/select.test").unwrap();
        let (items, errors) = tcl_file_parser().parse(&content).into_output_errors();

        if let Some(items) = items {
            let mut tests = 0;
            let mut comments = 0;
            let mut skips = 0;
            let mut skip_foreach = 0;
            let mut skip_if = 0;
            let mut skip_proc = 0;
            let mut skip_line = 0;
            let mut skip_unknown = 0;
            let mut unknown_previews = Vec::new();

            for item in &items {
                match item {
                    TclItem::Test(_) => tests += 1,
                    TclItem::Comment(_) => comments += 1,
                    TclItem::Skip(kind, preview) => {
                        skips += 1;
                        match kind {
                            SkipKind::Foreach => skip_foreach += 1,
                            SkipKind::If => skip_if += 1,
                            SkipKind::Proc => skip_proc += 1,
                            SkipKind::Line => skip_line += 1,
                            SkipKind::Unknown => {
                                skip_unknown += 1;
                                if preview.len() > 50 {
                                    unknown_previews.push(preview.chars().take(80).collect::<String>());
                                }
                            }
                        }
                    }
                }
            }

            println!("Total items: {}", items.len());
            println!("Tests: {}", tests);
            println!("Comments: {}", comments);
            println!("Skips total: {}", skips);
            println!("  Foreach: {}", skip_foreach);
            println!("  If: {}", skip_if);
            println!("  Proc: {}", skip_proc);
            println!("  Line: {}", skip_line);
            println!("  Unknown: {}", skip_unknown);

            println!("\nLarge unknown skips:");
            for (i, p) in unknown_previews.iter().enumerate() {
                println!("  {}: {}", i, p);
            }

            // Print first and last tests
            println!("\nFirst 10 tests:");
            let tests_vec: Vec<_> = items.iter().filter_map(|i| {
                if let TclItem::Test(t) = i { Some(t) } else { None }
            }).collect();
            for t in tests_vec.iter().take(10) {
                println!("  {}", t.name);
            }
            println!("\nLast 10 tests:");
            for t in tests_vec.iter().rev().take(10).rev() {
                println!("  {}", t.name);
            }
        }

        println!("Parse errors: {}", errors.len());
        for e in errors.iter().take(5) {
            println!("  Error: {}", e);
        }
    }
}
