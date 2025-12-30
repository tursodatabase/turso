//! Error-recoverable parser for Tcl .test files
//!
//! This parser attempts to extract test cases from Tcl test files,
//! skipping constructs it doesn't understand and reporting warnings.

/// Result of converting a Tcl test file
#[derive(Debug, Clone)]
pub struct ConversionResult {
    /// Successfully parsed tests
    pub tests: Vec<TclTest>,
    /// Warnings about skipped or problematic content
    pub warnings: Vec<ConversionWarning>,
    /// Source file name for reporting
    pub source_file: String,
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
    /// SQL to execute
    pub sql: String,
    /// Type of test and expected output
    pub kind: TclTestKind,
    /// Database specifier if specific
    pub db: Option<String>,
    /// Setup SQL to run before the test (extracted from sql)
    pub setup_sql: Option<String>,
    /// Setup name if extracted
    pub setup_name: Option<String>,
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
    pub fn parse(mut self) -> ConversionResult {
        let mut tests = Vec::new();
        let mut warnings = Vec::new();
        let mut used_names: std::collections::HashSet<String> = std::collections::HashSet::new();

        while self.pos < self.lines.len() {
            let line = self.lines[self.pos].trim();
            let line_num = self.pos + 1;

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
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

            // Try to parse test functions
            if let Some(result) = self.try_parse_test(line, line_num) {
                match result {
                    Ok(mut test) => {
                        // Deduplicate test names
                        let base_name = test.name.clone();
                        let mut final_name = base_name.clone();
                        let mut counter = 2;

                        while used_names.contains(&final_name) {
                            final_name = format!("{}-{}", base_name, counter);
                            counter += 1;
                        }

                        if final_name != test.name {
                            // Also update setup name if present
                            if let Some(ref mut setup_name) = test.setup_name {
                                let base_setup = setup_name.clone();
                                *setup_name = format!(
                                    "{}-{}",
                                    base_setup.trim_end_matches("-setup"),
                                    counter - 1
                                );
                                setup_name.push_str("-setup");
                            }
                            test.name = final_name.clone();
                        }

                        used_names.insert(final_name);
                        tests.push(test);
                    }
                    Err(warning) => warnings.push(warning),
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

        Ok(TclTest {
            name,
            sql,
            kind: TclTestKind::Exact(expected),
            db: None,
            setup_sql: None,
            setup_name: None,
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

        let db = Self::clean_name(&parts[0]);
        let name = Self::clean_name(&parts[1]);
        let sql = Self::clean_sql(&parts[2]);
        let expected = if parts.len() > 3 {
            Self::clean_expected(&parts[3])
        } else {
            String::new()
        };

        // Extract setup SQL from tests that create tables
        let (setup_sql, test_sql, setup_name) = Self::extract_setup(&sql, &name);

        Ok(TclTest {
            name,
            sql: test_sql,
            kind: TclTestKind::Exact(expected),
            db: Some(db),
            setup_sql,
            setup_name,
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

        Ok(TclTest {
            name,
            sql,
            kind: TclTestKind::Error(pattern),
            db: None,
            setup_sql: None,
            setup_name: None,
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

        Ok(TclTest {
            name,
            sql,
            kind: TclTestKind::AnyError,
            db: None,
            setup_sql: None,
            setup_name: None,
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

        Ok(TclTest {
            name,
            sql,
            kind: TclTestKind::Pattern(pattern),
            db: None,
            setup_sql: None,
            setup_name: None,
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
    fn collect_test_content(&mut self) -> String {
        let mut content = String::new();
        let mut brace_depth = 0;
        let mut started = false;

        while self.pos < self.lines.len() {
            let line = self.lines[self.pos];
            content.push_str(line);
            content.push('\n');

            // Count braces
            for ch in line.chars() {
                if ch == '{' {
                    brace_depth += 1;
                    started = true;
                } else if ch == '}' {
                    brace_depth -= 1;
                }
            }

            self.pos += 1;

            // Stop when braces are balanced (and we've seen at least one)
            if started && brace_depth == 0 {
                break;
            }
        }

        content
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

    /// Parse test arguments from collected content
    fn parse_test_args(
        content: &str,
        min_args: usize,
    ) -> Result<Vec<String>, ConversionWarning> {
        let mut args = Vec::new();
        let mut current = String::new();
        let mut brace_depth = 0;
        let mut in_string = false;
        let mut chars = content.chars().peekable();

        // Skip the function name
        while let Some(&ch) = chars.peek() {
            if ch.is_whitespace() || ch == '{' {
                break;
            }
            chars.next();
        }

        // Skip whitespace
        while let Some(&ch) = chars.peek() {
            if !ch.is_whitespace() {
                break;
            }
            chars.next();
        }

        while let Some(ch) = chars.next() {
            match ch {
                '{' if !in_string => {
                    if brace_depth > 0 {
                        current.push(ch);
                    }
                    brace_depth += 1;
                }
                '}' if !in_string => {
                    brace_depth -= 1;
                    if brace_depth > 0 {
                        current.push(ch);
                    } else if brace_depth == 0 {
                        args.push(current.trim().to_string());
                        current = String::new();
                    }
                }
                '"' => {
                    in_string = !in_string;
                    if brace_depth == 0 {
                        // Quoted argument outside braces
                        if in_string {
                            current = String::new();
                        } else {
                            args.push(current.clone());
                            current = String::new();
                        }
                    } else {
                        current.push(ch);
                    }
                }
                _ if brace_depth > 0 => {
                    current.push(ch);
                }
                _ if brace_depth == 0 && !ch.is_whitespace() => {
                    // Unbraced argument
                    current.push(ch);
                }
                _ if brace_depth == 0 && ch.is_whitespace() && !current.is_empty() => {
                    args.push(current.clone());
                    current = String::new();
                }
                _ => {}
            }
        }

        if !current.is_empty() {
            args.push(current);
        }

        if args.len() < min_args {
            return Err(ConversionWarning {
                line: 0,
                kind: WarningKind::ParseError,
                message: format!(
                    "Expected at least {} arguments, got {}",
                    min_args,
                    args.len()
                ),
                source: content.chars().take(100).collect(),
            });
        }

        Ok(args)
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
                _ => {} // Skip other invalid chars
            }
        }

        // Ensure name starts with a letter or underscore
        if result.is_empty() {
            return "unnamed-test".to_string();
        }

        if result.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            result = format!("test-{}", result);
        }

        result
    }

    /// Clean up SQL (normalize whitespace)
    fn clean_sql(s: &str) -> String {
        let mut result = String::new();
        for line in s.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                if !result.is_empty() {
                    result.push('\n');
                }
                result.push_str(trimmed);
            }
        }
        result
    }

    /// Clean up expected output (convert Tcl list format to pipe-separated)
    fn clean_expected(s: &str) -> String {
        let mut result = String::new();

        for line in s.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
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

    /// Extract setup SQL from test SQL that contains CREATE TABLE etc.
    fn extract_setup(sql: &str, test_name: &str) -> (Option<String>, String, Option<String>) {
        let mut setup_lines = Vec::new();
        let mut test_lines = Vec::new();
        let mut in_setup = true;

        for line in sql.lines() {
            let upper = line.to_uppercase();
            let trimmed = upper.trim();

            // Setup statements
            if in_setup
                && (trimmed.starts_with("CREATE ")
                    || trimmed.starts_with("INSERT ")
                    || trimmed.starts_with("DROP "))
            {
                setup_lines.push(line);
            } else {
                in_setup = false;
                test_lines.push(line);
            }
        }

        if setup_lines.is_empty() {
            (None, sql.to_string(), None)
        } else {
            // Generate a setup name from test name
            let setup_name = format!("{}-setup", test_name.replace(' ', "-"));
            (
                Some(setup_lines.join("\n")),
                test_lines.join("\n"),
                Some(setup_name),
            )
        }
    }
}

impl std::fmt::Display for WarningKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WarningKind::ForeachSkipped => write!(f, "FOREACH"),
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
