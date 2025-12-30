//! Error-recoverable parser for Tcl .test files
//!
//! This parser attempts to extract test cases from Tcl test files,
//! skipping constructs it doesn't understand and reporting warnings.

use chumsky::prelude::*;

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
    /// Line number in source
    pub line: usize,
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

macro_rules! text_parser {
    ($lt:lifetime, $ty:ty) => {
        impl Parser<$lt, &$lt str, $ty, extra::Err<Simple<'a, char>>>
    };
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

    /// Check if SQL contains DDL statements
    fn check_has_ddl(sql: &str) -> bool {
        let upper = sql.to_uppercase();
        upper.contains("CREATE ")
            || upper.contains("DROP ")
            || upper.contains("ALTER ")
            || upper.contains("INSERT ")
            || upper.contains("UPDATE ")
            || upper.contains("DELETE ")
    }
}

/// Parser for Tcl test files
pub struct TclParser<'a> {
    source_file: &'a str,
    lines: Vec<&'a str>,
    pos: usize,
}

impl<'a> TclParser<'a> {
    pub fn new(content: &'a str, source_file: &'a str) -> Self {
        Self {
            source_file,
            lines: content.lines().collect(),
            pos: 0,
        }
    }

    /// Parse the entire file
    pub fn parse(&mut self) -> ConversionResult {
        let mut tests = Vec::new();
        let mut warnings = Vec::new();
        let mut used_names: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut pending_comments: Vec<String> = Vec::new();

        while self.pos < self.lines.len() {
            let line = self.lines[self.pos].trim();
            let line_num = self.pos + 1;

            // Skip empty lines (but keep pending comments - they might be for the next test)
            if line.is_empty() {
                self.pos += 1;
                continue;
            }

            // Collect comments (except shebang)
            if line.starts_with('#') && !line.starts_with("#!") {
                pending_comments.push(line.to_string());
                self.pos += 1;
                continue;
            }

            // Skip shebang
            if line.starts_with("#!/") {
                self.pos += 1;
                continue;
            }

            // Skip source and set directives
            if line.starts_with("set ") || line.starts_with("source ") {
                self.pos += 1;
                continue;
            }

            // Skip proc definitions
            if line.starts_with("proc ") {
                warnings.push(ConversionWarning {
                    line: line_num,
                    kind: WarningKind::ProcSkipped,
                    message: "Proc definition skipped".to_string(),
                    source: line.to_string(),
                });
                self.skip_braced_block();
                continue;
            }

            // Handle load_extension
            if line.starts_with("load_extension") {
                warnings.push(ConversionWarning {
                    line: line_num,
                    kind: WarningKind::ExtensionSkipped,
                    message: "Extension loading not supported".to_string(),
                    source: line.to_string(),
                });
                self.pos += 1;
                continue;
            }

            // Handle foreach (generates multiple tests - skip with warning)
            if line.starts_with("foreach ") {
                warnings.push(ConversionWarning {
                    line: line_num,
                    kind: WarningKind::ForeachSkipped,
                    message: "foreach loop skipped - generates parameterized tests".to_string(),
                    source: self.get_foreach_preview(),
                });
                self.skip_braced_block();
                continue;
            }

            // Handle if blocks (conditional tests - skip with warning)
            if line.starts_with("if ") {
                warnings.push(ConversionWarning {
                    line: line_num,
                    kind: WarningKind::IfBlockSkipped,
                    message: "if block skipped - conditional tests need manual review".to_string(),
                    source: self.get_foreach_preview(),
                });
                self.skip_braced_block();
                continue;
            }

            // Try to parse test functions
            if let Some(result) = self.try_parse_test(line, line_num) {
                match result {
                    Ok(mut test) => {
                        // Attach pending comments to this test
                        test.comments = std::mem::take(&mut pending_comments);

                        // Deduplicate test names
                        let base_name = test.name.clone();
                        let mut final_name = base_name.clone();
                        let mut counter = 2;

                        while used_names.contains(&final_name) {
                            final_name = format!("{}-{}", base_name, counter);
                            counter += 1;
                        }

                        if final_name != test.name {
                            test.name = final_name.clone();
                        }

                        used_names.insert(final_name);
                        tests.push(test);
                    }
                    Err(warning) => {
                        warnings.push(warning);
                    }
                }
                continue;
            }

            // Unknown construct
            if !line.is_empty()
                && !line.starts_with("}")
                && !line.starts_with("{")
                && !line.starts_with("]")
            {
                // Only warn about substantial unknown content
                if line.len() > 3 {
                    warnings.push(ConversionWarning {
                        line: line_num,
                        kind: WarningKind::UnknownFunction,
                        message: "Unknown construct".to_string(),
                        source: line.chars().take(80).collect(),
                    });
                }
            }

            self.pos += 1;
        }

        ConversionResult {
            tests,
            warnings,
            source_file: self.source_file.to_string(),
            pending_comments,
        }
    }

    /// Try to parse a test function call
    fn try_parse_test(
        &mut self,
        line: &str,
        line_num: usize,
    ) -> Option<Result<TclTest, ConversionWarning>> {
        // Match different test function patterns (order matters - more specific first)
        if line.starts_with("do_execsql_test_on_specific_db") {
            Some(self.parse_specific_db_test(line_num))
        } else if line.starts_with("do_execsql_test_in_memory_error_content") {
            Some(self.parse_memory_error_content_test(line_num))
        } else if line.starts_with("do_execsql_test_in_memory_any_error") {
            Some(self.parse_memory_any_error_test(line_num))
        } else if line.starts_with("do_execsql_test_in_memory_error") {
            Some(self.parse_memory_error_test(line_num))
        } else if line.starts_with("do_execsql_test_error_content") {
            Some(self.parse_error_content_test(line_num))
        } else if line.starts_with("do_execsql_test_any_error") {
            Some(self.parse_any_error_test(line_num))
        } else if line.starts_with("do_execsql_test_error") {
            Some(self.parse_error_test(line_num))
        } else if line.starts_with("do_execsql_test_regex") {
            Some(self.parse_regex_test(line_num))
        } else if line.starts_with("do_execsql_test_tolerance") {
            Some(self.parse_tolerance_test(line_num))
        } else if line.starts_with("do_execsql_test_small") {
            Some(self.parse_small_test(line_num))
        } else if line.starts_with("do_execsql_test_skip_lines_on_specific_db") {
            Some(self.parse_skip_lines_test(line_num))
        } else if line.starts_with("do_execsql_test") {
            Some(self.parse_basic_test(line_num))
        } else {
            None
        }
    }

    /// Parse: do_execsql_test name { sql } { expected }
    fn parse_basic_test(&mut self, line_num: usize) -> Result<TclTest, ConversionWarning> {
        let content = self.collect_test_content();
        let parts = Self::parse_test_args(&content, 2)?;

        let name = Self::clean_name(&parts[0]);
        let sql = Self::clean_sql(&parts[1]);
        let expected = if parts.len() > 2 {
            Self::clean_expected(&parts[2])
        } else {
            String::new()
        };

        let has_ddl = TclTest::check_has_ddl(&sql);

        Ok(TclTest {
            name,
            sql,
            kind: TclTestKind::Exact(expected),
            db: None,
            has_ddl,
            comments: Vec::new(),
            line: line_num,
        })
    }

    /// Parse: do_execsql_test_small name { sql } { expected }
    fn parse_small_test(&mut self, line_num: usize) -> Result<TclTest, ConversionWarning> {
        // Parse same as basic but mark as small db
        let result = self.parse_basic_test(line_num);
        match result {
            Ok(mut test) => {
                test.db = Some("testing/testing_small.db".to_string());
                Ok(test)
            }
            Err(e) => Err(e),
        }
    }

    /// Parse: do_execsql_test_on_specific_db db name { sql } { expected }
    fn parse_specific_db_test(&mut self, line_num: usize) -> Result<TclTest, ConversionWarning> {
        let content = self.collect_test_content();
        let parts = Self::parse_test_args(&content, 3)?;

        // Don't use clean_name for db path - just trim it
        let db = parts[0].trim().to_string();
        let name = Self::clean_name(&parts[1]);
        let sql = Self::clean_sql(&parts[2]);
        let expected = if parts.len() > 3 {
            Self::clean_expected(&parts[3])
        } else {
            String::new()
        };

        let has_ddl = TclTest::check_has_ddl(&sql);

        Ok(TclTest {
            name,
            sql,
            kind: TclTestKind::Exact(expected),
            db: Some(db),
            has_ddl,
            comments: Vec::new(),
            line: line_num,
        })
    }

    /// Parse: do_execsql_test_error name { sql } { pattern }
    fn parse_error_test(&mut self, line_num: usize) -> Result<TclTest, ConversionWarning> {
        let content = self.collect_test_content();
        let parts = Self::parse_test_args(&content, 2)?;

        let name = Self::clean_name(&parts[0]);
        let sql = Self::clean_sql(&parts[1]);
        let pattern = if parts.len() > 2 {
            Some(Self::clean_expected(&parts[2]))
        } else {
            None
        };

        let has_ddl = TclTest::check_has_ddl(&sql);

        Ok(TclTest {
            name,
            sql,
            kind: TclTestKind::Error(pattern),
            db: None,
            has_ddl,
            comments: Vec::new(),
            line: line_num,
        })
    }

    /// Parse: do_execsql_test_error_content name { sql } { error_text }
    fn parse_error_content_test(&mut self, line_num: usize) -> Result<TclTest, ConversionWarning> {
        self.parse_error_test(line_num)
    }

    /// Parse: do_execsql_test_any_error name { sql }
    fn parse_any_error_test(&mut self, line_num: usize) -> Result<TclTest, ConversionWarning> {
        let content = self.collect_test_content();
        let parts = Self::parse_test_args(&content, 2)?;

        let name = Self::clean_name(&parts[0]);
        let sql = Self::clean_sql(&parts[1]);
        let has_ddl = TclTest::check_has_ddl(&sql);

        Ok(TclTest {
            name,
            sql,
            kind: TclTestKind::AnyError,
            db: None,
            has_ddl,
            comments: Vec::new(),
            line: line_num,
        })
    }

    /// Parse: do_execsql_test_in_memory_error name { sql } { pattern }
    fn parse_memory_error_test(&mut self, line_num: usize) -> Result<TclTest, ConversionWarning> {
        let mut result = self.parse_error_test(line_num)?;
        result.db = Some(":memory:".to_string());
        Ok(result)
    }

    /// Parse: do_execsql_test_in_memory_error_content name { sql } { error_text }
    fn parse_memory_error_content_test(
        &mut self,
        line_num: usize,
    ) -> Result<TclTest, ConversionWarning> {
        self.parse_memory_error_test(line_num)
    }

    /// Parse: do_execsql_test_in_memory_any_error name { sql }
    fn parse_memory_any_error_test(
        &mut self,
        line_num: usize,
    ) -> Result<TclTest, ConversionWarning> {
        let mut result = self.parse_any_error_test(line_num)?;
        result.db = Some(":memory:".to_string());
        Ok(result)
    }

    /// Parse: do_execsql_test_regex name { sql } { pattern }
    fn parse_regex_test(&mut self, line_num: usize) -> Result<TclTest, ConversionWarning> {
        let content = self.collect_test_content();
        let parts = Self::parse_test_args(&content, 2)?;

        let name = Self::clean_name(&parts[0]);
        let sql = Self::clean_sql(&parts[1]);
        let pattern = if parts.len() > 2 {
            Self::clean_expected(&parts[2])
        } else {
            ".*".to_string()
        };

        let has_ddl = TclTest::check_has_ddl(&sql);

        Ok(TclTest {
            name,
            sql,
            kind: TclTestKind::Pattern(pattern),
            db: None,
            has_ddl,
            comments: Vec::new(),
            line: line_num,
        })
    }

    /// Parse: do_execsql_test_tolerance name { sql } { expected } tolerance
    fn parse_tolerance_test(&mut self, line_num: usize) -> Result<TclTest, ConversionWarning> {
        Err(ConversionWarning {
            line: line_num,
            kind: WarningKind::ToleranceTest,
            message: "Tolerance tests not supported - needs manual conversion".to_string(),
            source: self.lines.get(self.pos - 1).unwrap_or(&"").to_string(),
        })
    }

    /// Parse: do_execsql_test_skip_lines_on_specific_db skip db name { sql } { expected }
    fn parse_skip_lines_test(&mut self, line_num: usize) -> Result<TclTest, ConversionWarning> {
        let _ = self.collect_test_content();
        Err(ConversionWarning {
            line: line_num,
            kind: WarningKind::SkipLinesTest,
            message: "Skip lines tests not supported - needs manual conversion".to_string(),
            source: self.lines.get(self.pos - 1).unwrap_or(&"").to_string(),
        })
    }

    /// Collect all content for a test spanning multiple lines
    fn collect_test_content() -> text_parser!('a, &'a str) {
        any().delimited_by(just('{'), just('}'))
    }

    /// Skip a braced block (foreach, proc, etc.)
    fn skip_braced_block(&mut self) {
        let mut brace_depth = 0;
        let mut started = false;

        while self.pos < self.lines.len() {
            let line = self.lines[self.pos];

            for ch in line.chars() {
                if ch == '{' {
                    brace_depth += 1;
                    started = true;
                } else if ch == '}' {
                    brace_depth -= 1;
                }
            }

            self.pos += 1;

            if started && brace_depth == 0 {
                break;
            }
        }
    }

    /// Get a preview of a foreach block for the warning
    fn get_foreach_preview(&self) -> String {
        let mut preview = String::new();
        let start = self.pos;
        let end = (start + 5).min(self.lines.len());

        for i in start..end {
            if !preview.is_empty() {
                preview.push('\n');
            }
            preview.push_str(self.lines[i]);
        }

        if end < self.lines.len() {
            preview.push_str("\n...");
        }

        preview
    }

    /// Parse test arguments from collected content using chumsky
    fn parse_test_args() -> text_parser!('a, Vec<String>) {
        // Parser for whitespace
        let ws = any::<&str, extra::Err<Simple<char>>>()
            .filter(|c: &char| c.is_whitespace())
            .repeated();

        // Parser for braced content with nested braces
        let braced = recursive(|braced| {
            let not_brace = any().filter(|c: &char| *c != '{' && *c != '}');
            let inner = choice((
                braced.map(|s: String| format!("{{{}}}", s)),
                not_brace.map(|c| c.to_string()),
            ))
            .repeated()
            .collect::<Vec<_>>()
            .map(|v| v.join(""));

            just('{').ignore_then(inner).then_ignore(just('}'))
        });

        // Parser for quoted strings
        let quoted = just('"')
            .ignore_then(
                any()
                    .filter(|c: &char| *c != '"')
                    .repeated()
                    .collect::<String>(),
            )
            .then_ignore(just('"'));

        // Parser for unbraced words (no whitespace, braces, or quotes)
        let word = any()
            .filter(|c: &char| !c.is_whitespace() && *c != '{' && *c != '}' && *c != '"')
            .repeated()
            .at_least(1)
            .collect::<String>();

        // Parser for a single argument
        let arg = choice((braced, quoted, word));

        // Parser for function name (skip it)
        let func_name = any()
            .filter(|c: &char| !c.is_whitespace() && *c != '{')
            .repeated();

        // Full parser: skip whitespace, function name, whitespace, then collect args
        let parser = ws
            .ignore_then(func_name)
            .ignore_then(ws)
            .ignore_then(arg.padded().repeated().collect::<Vec<_>>());

        parser
    }

    /// Clean up a test name (remove quotes, convert invalid chars)
    fn clean_name(s: &str) -> String {
        let name = s
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .trim_matches('{')
            .trim_matches('}');

        // Replace characters not allowed in identifiers
        let mut result = String::new();
        for ch in name.chars() {
            match ch {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' => result.push(ch),
                '.' => result.push('_'), // Replace dots with underscore
                ':' => result.push('_'), // Replace colons with underscore
                ' ' => result.push('-'), // Replace spaces with hyphen
                _ => {}                  // Skip other invalid chars
            }
        }

        // Ensure name starts with a letter or underscore
        if result.is_empty() {
            return "unnamed-test".to_string();
        }

        if result
            .chars()
            .next()
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false)
        {
            result = format!("test-{}", result);
        }

        result
    }

    /// Clean up SQL (normalize whitespace, preserve comments)
    fn clean_sql(s: &str) -> String {
        let mut result = String::new();
        let mut prev_was_empty = false;

        for line in s.lines() {
            let trimmed = line.trim();

            if trimmed.is_empty() {
                // Collapse multiple empty lines into one
                if !prev_was_empty && !result.is_empty() {
                    result.push('\n');
                    prev_was_empty = true;
                }
            } else {
                if !result.is_empty() && !prev_was_empty {
                    result.push('\n');
                }
                result.push_str(trimmed);
                prev_was_empty = false;
            }
        }

        result
    }

    /// Clean up expected output (convert Tcl list format to pipe-separated)
    fn clean_expected(s: &str) -> String {
        // First, strip outer quotes if the entire content is wrapped in them
        // This handles {"content"} where we get "content" after brace extraction
        let s = s.trim();
        let was_quoted = s.starts_with('"') && s.ends_with('"') && s.len() >= 2;
        let s = if was_quoted { &s[1..s.len() - 1] } else { s };

        // Parse the whole content as a Tcl list (handles multiline braced elements)
        let elements = Self::parse_tcl_list(s);

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

    /// Parse a Tcl list string into its elements
    /// Handles: {element1} {element2}, plain words, multiline braced elements
    fn parse_tcl_list(s: &str) -> Vec<String> {
        let mut elements = Vec::new();
        let mut chars = s.chars().peekable();
        let mut current = String::new();

        while let Some(ch) = chars.next() {
            match ch {
                '{' => {
                    // Start of braced element - find matching close brace
                    let mut depth = 1;
                    let mut content = String::new();
                    while let Some(inner) = chars.next() {
                        if inner == '{' {
                            depth += 1;
                            content.push(inner);
                        } else if inner == '}' {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                            content.push(inner);
                        } else {
                            content.push(inner);
                        }
                    }
                    // Push any accumulated non-braced content first
                    if !current.trim().is_empty() {
                        elements.push(current.trim().to_string());
                        current = String::new();
                    }
                    elements.push(content);
                }
                ' ' | '\t' if current.is_empty() => {
                    // Skip leading whitespace between elements
                }
                ' ' | '\t' => {
                    // End of unbraced word - but only if not inside a line
                    // For simple values, treat the whole line as one element
                    current.push(ch);
                }
                '\n' => {
                    // Newline separates elements in unbraced content
                    if !current.trim().is_empty() {
                        elements.push(current.trim().to_string());
                    }
                    current = String::new();
                }
                _ => {
                    current.push(ch);
                }
            }
        }

        // Don't forget remaining content
        if !current.trim().is_empty() {
            elements.push(current.trim().to_string());
        }

        // If no elements were found, return the whole string as one element
        if elements.is_empty() && !s.trim().is_empty() {
            elements.push(s.trim().to_string());
        }

        elements
    }
}

/// Parse test arguments from collected content using chumsky
fn parse_test_args<'a>() -> text_parser!('a, Vec<String>) {
    // Parser for whitespace
    let ws = any::<&str, extra::Err<Simple<char>>>()
        .filter(|c: &char| c.is_whitespace())
        .repeated();

    // Parser for braced content with nested braces
    let braced = recursive(|braced| {
        let not_brace = any().filter(|c: &char| *c != '{' && *c != '}');
        let inner = choice((
            braced.map(|s: String| format!("{{{}}}", s)),
            not_brace.map(|c| c.to_string()),
        ))
        .repeated()
        .collect::<Vec<_>>()
        .map(|v| v.join(""));

        just('{').ignore_then(inner).then_ignore(just('}'))
    });

    // Parser for quoted strings
    let quoted = just('"')
        .ignore_then(
            any()
                .filter(|c: &char| *c != '"')
                .repeated()
                .collect::<String>(),
        )
        .then_ignore(just('"'));

    // Parser for unbraced words (no whitespace, braces, or quotes)
    let word = any()
        .filter(|c: &char| !c.is_whitespace() && *c != '{' && *c != '}' && *c != '"')
        .repeated()
        .at_least(1)
        .collect::<String>();

    // Parser for a single argument
    let arg = choice((braced, quoted, word));

    // Parser for function name (skip it)
    let func_name = any()
        .filter(|c: &char| !c.is_whitespace() && *c != '{')
        .repeated();

    // Full parser: skip whitespace, function name, whitespace, then collect args
    let parser = ws
        .ignore_then(func_name)
        .ignore_then(ws)
        .ignore_then(arg.padded().repeated().collect::<Vec<_>>());

    parser
}

/// Collect all content for a test spanning multiple lines
fn collect_test_content<'a>() -> text_parser!('a, &'a str) {
    any().delimited_by(just('{'), just('}')).to_slice()
}

/// Parse a Tcl list string into its elements
/// Handles: {element1} {element2}, plain words, multiline braced elements
fn parse_tcl_list<'a>() -> text_parser!('a, Vec<String>) {
    // Braced: {content with {nested} braces}
    let braced = recursive_brace();

    // Line of unbraced text (newline-terminated)
    let unbraced = none_of("{}\n")
        .repeated()
        .at_least(1)
        .collect::<String>()
        .map(|s| s.trim().to_string());

    // Horizontal whitespace only
    let hspace = one_of(" \t").repeated();

    // An element on a line
    let line_element = hspace.ignore_then(choice((braced, unbraced)));

    // Multiple elements can be on same line or different lines
    line_element
        .separated_by(one_of(" \t\n").repeated().at_least(1))
        .allow_leading()
        .allow_trailing()
        .collect::<Vec<_>>()
        .map(|v| v.into_iter().filter(|s| !s.is_empty()).collect())
}

/// Parser that parses content that can be recursively delimted by braces
fn recursive_brace<'a>() -> text_parser!('a, String) {
    recursive(|braced| {
        choice((
            braced.map(|s: String| format!("{{{}}}", s)),
            none_of("{}").map(|c: char| c.to_string()),
        ))
        .repeated()
        .collect::<Vec<_>>()
        .map(|v| v.join(""))
        .delimited_by(just('{'), just('}'))
    })
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
