pub mod ast;
pub mod lexer;

use ast::*;
use lexer::{SpannedToken, Token, tokenize};
use miette::{Diagnostic, SourceSpan};
use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;

/// Parse a `.sqltest` file from source
pub fn parse(input: &str) -> Result<TestFile, ParseError> {
    let tokens = tokenize(input)?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

struct Parser {
    tokens: Vec<SpannedToken>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<SpannedToken>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn parse(&mut self) -> Result<TestFile, ParseError> {
        let mut databases = Vec::new();
        let mut setups = HashMap::new();
        let mut tests = Vec::new();
        let mut global_skip = None;
        let mut global_requires = Vec::new();

        while !self.is_at_end() {
            self.skip_newlines_and_comments();

            if self.is_at_end() {
                break;
            }

            match self.peek() {
                Some(Token::AtDatabase) => {
                    databases.push(self.parse_database()?);
                }
                // Global @skip-file or @skip-file-if: applies to all tests in the file
                Some(Token::AtSkipFile) => {
                    global_skip = Some(self.parse_global_skip()?);
                }
                Some(Token::AtSkipFileIf) => {
                    global_skip = Some(self.parse_global_skip_if()?);
                }
                // Global @requires-file: applies to all tests in the file
                Some(Token::AtRequiresFile) => {
                    global_requires.push(self.parse_global_requires()?);
                }
                Some(Token::Setup) => {
                    let (name, sql) = self.parse_setup()?;
                    if setups.contains_key(&name) {
                        return Err(self.error(format!("duplicate setup name: {name}")));
                    }
                    setups.insert(name, sql);
                }
                Some(
                    Token::AtSetup
                    | Token::AtSkip
                    | Token::AtSkipIf
                    | Token::AtRequires
                    | Token::AtBackend
                    | Token::Test,
                ) => {
                    tests.push(self.parse_test()?);
                }
                Some(token) => {
                    return Err(self.error(format!("unexpected token: {token}")));
                }
                None => break,
            }
        }

        let test_file = TestFile {
            databases,
            setups,
            tests,
            global_skip,
            global_requires,
        };

        self.validate(&test_file)?;

        Ok(test_file)
    }

    fn parse_database(&mut self) -> Result<DatabaseConfig, ParseError> {
        self.expect_token(Token::AtDatabase)?;

        match self.peek() {
            Some(Token::Memory) => {
                self.advance();
                Ok(DatabaseConfig {
                    location: DatabaseLocation::Memory,
                    readonly: false,
                })
            }
            Some(Token::TempFile) => {
                self.advance();
                Ok(DatabaseConfig {
                    location: DatabaseLocation::TempFile,
                    readonly: false,
                })
            }
            Some(Token::Default) => {
                self.advance();
                Ok(DatabaseConfig {
                    location: DatabaseLocation::Default,
                    readonly: true,
                })
            }
            Some(Token::DefaultNoRowidAlias) => {
                self.advance();
                Ok(DatabaseConfig {
                    location: DatabaseLocation::DefaultNoRowidAlias,
                    readonly: true,
                })
            }
            Some(Token::Path(path)) => {
                let path = path.clone();
                self.advance();

                let readonly = if matches!(self.peek(), Some(Token::Readonly)) {
                    self.advance();
                    true
                } else {
                    false
                };

                Ok(DatabaseConfig {
                    location: DatabaseLocation::Path(PathBuf::from(path)),
                    readonly,
                })
            }
            Some(token) => Err(self.error(format!(
                "expected database specifier (:memory:, :temp:, :default:, :default-no-rowidalias:, or path), got {token}"
            ))),
            None => Err(self.error("expected database specifier, got EOF".to_string())),
        }
    }

    fn parse_global_skip(&mut self) -> Result<ast::Skip, ParseError> {
        self.expect_token(Token::AtSkipFile)?;
        let reason = self.expect_string()?;
        Ok(ast::Skip {
            reason,
            condition: None,
        })
    }

    fn parse_global_skip_if(&mut self) -> Result<ast::Skip, ParseError> {
        self.expect_token(Token::AtSkipFileIf)?;
        let condition = self.parse_skip_condition()?;
        let reason = self.expect_string()?;
        Ok(ast::Skip {
            reason,
            condition: Some(condition),
        })
    }

    fn parse_global_requires(&mut self) -> Result<ast::Requirement, ParseError> {
        self.expect_token(Token::AtRequiresFile)?;
        let capability = self.parse_capability()?;
        let reason = self.expect_string()?;
        Ok(ast::Requirement { capability, reason })
    }

    fn parse_setup(&mut self) -> Result<(String, String), ParseError> {
        self.expect_token(Token::Setup)?;

        let name = self.expect_identifier()?;
        let content = self.expect_block_content()?.trim().to_string();

        Ok((name, content))
    }

    fn parse_test(&mut self) -> Result<TestCase, ParseError> {
        let mut test_setups = Vec::new();
        let mut skip = None;
        let mut backend = None;
        let mut requires = Vec::new();

        // Parse decorators
        loop {
            match self.peek() {
                Some(Token::AtSetup) => {
                    let at_setup_span_start = self.current_span().start;
                    self.advance();
                    let (setup_name, name_span) = self.expect_identifier_with_span()?;
                    test_setups.push(SetupRef {
                        name: setup_name,
                        span: at_setup_span_start..name_span.end,
                    });
                    self.skip_newlines_and_comments();
                }
                Some(Token::AtSkip) => {
                    self.advance();
                    let reason = self.expect_string()?;
                    skip = Some(ast::Skip {
                        reason,
                        condition: None,
                    });
                    self.skip_newlines_and_comments();
                }
                Some(Token::AtSkipIf) => {
                    self.advance();
                    let condition = self.parse_skip_condition()?;
                    let reason = self.expect_string()?;
                    skip = Some(ast::Skip {
                        reason,
                        condition: Some(condition),
                    });
                    self.skip_newlines_and_comments();
                }
                Some(Token::AtRequires) => {
                    self.advance();
                    let capability = self.parse_capability()?;
                    let reason = self.expect_string()?;
                    requires.push(ast::Requirement { capability, reason });
                    self.skip_newlines_and_comments();
                }
                Some(Token::AtBackend) => {
                    self.advance();
                    let backend_name = self.expect_identifier()?;
                    backend = Some(
                        backend_name
                            .parse::<ast::Backend>()
                            .map_err(|e| self.error(e))?,
                    );
                    self.skip_newlines_and_comments();
                }
                _ => break,
            }
        }

        // Parse test
        self.expect_token(Token::Test)?;
        let (name, name_span) = self.expect_identifier_with_span()?;
        let sql = self.expect_block_content()?.trim().to_string();

        self.skip_newlines_and_comments();

        // Parse expect blocks (at least one required, with optional backend-specific overrides)
        let mut default_expectation: Option<Expectation> = None;
        let mut overrides: HashMap<ast::Backend, Expectation> = HashMap::new();

        while matches!(self.peek(), Some(Token::Expect)) {
            self.expect_token(Token::Expect)?;

            // Check for backend qualifier: expect @js { ... }
            let backend_qualifier = if let Some(Token::AtIdentifier(backend_name)) = self.peek() {
                let backend_name = backend_name.clone();
                self.advance();
                let backend = backend_name
                    .parse::<ast::Backend>()
                    .map_err(|e| self.error(e))?;
                Some(backend)
            } else {
                None
            };

            let expectation = self.parse_expectation()?;

            if let Some(backend) = backend_qualifier {
                if overrides.contains_key(&backend) {
                    return Err(
                        self.error(format!("duplicate expect block for backend '{backend}'"))
                    );
                }
                overrides.insert(backend, expectation);
            } else {
                if default_expectation.is_some() {
                    return Err(
                        self.error("multiple default expect blocks (use @backend qualifier for backend-specific expectations)".to_string()),
                    );
                }
                default_expectation = Some(expectation);
            }

            self.skip_newlines_and_comments();
        }

        // Validate at least one default expectation
        let default = default_expectation.ok_or_else(|| {
            self.error(
                "at least one default expect block (without @backend qualifier) is required"
                    .to_string(),
            )
        })?;

        Ok(TestCase {
            name,
            name_span,
            sql,
            expectations: Expectations { default, overrides },
            setups: test_setups,
            skip,
            backend,
            requires,
        })
    }

    fn parse_expectation(&mut self) -> Result<Expectation, ParseError> {
        match self.peek() {
            Some(Token::Error) => {
                self.advance();
                let content = self.expect_block_content()?.trim().to_string();
                let pattern = if content.is_empty() {
                    None
                } else {
                    Some(content)
                };
                Ok(Expectation::Error(pattern))
            }
            Some(Token::Pattern) => {
                self.advance();
                let content = self.expect_block_content()?.trim().to_string();
                Ok(Expectation::Pattern(content))
            }
            Some(Token::Unordered) => {
                self.advance();
                let content = self.expect_block_content()?;
                // Trim each line to handle indentation in expect blocks
                let rows = content
                    .trim()
                    .lines()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                Ok(Expectation::Unordered(rows))
            }
            Some(Token::Raw) => {
                self.advance();
                let content = self.expect_block_content()?;
                // Raw mode: preserve whitespace exactly, only split on newlines
                // We still strip the leading/trailing newlines from the block itself
                let content = content.strip_prefix('\n').unwrap_or(&content);
                let content = content.strip_suffix('\n').unwrap_or(content);
                let rows = content.lines().map(|s| s.to_string()).collect();
                Ok(Expectation::Exact(rows))
            }
            Some(Token::BlockContent(_)) => {
                let content = self.expect_block_content()?;
                // Trim each line to handle indentation in expect blocks
                let rows = content
                    .trim()
                    .lines()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                Ok(Expectation::Exact(rows))
            }
            Some(token) => {
                Err(self.error(format!("expected expect modifier or block, got {token}")))
            }
            None => Err(self.error("expected expect block, got EOF".to_string())),
        }
    }

    fn parse_skip_condition(&mut self) -> Result<ast::SkipCondition, ParseError> {
        match self.peek() {
            Some(Token::Mvcc) => {
                self.advance();
                Ok(ast::SkipCondition::Mvcc)
            }
            Some(token) => Err(self.error(format!("expected skip condition (mvcc), got {token}"))),
            None => Err(self.error("expected skip condition, got EOF".to_string())),
        }
    }

    fn parse_capability(&mut self) -> Result<ast::Capability, ParseError> {
        match self.peek() {
            Some(Token::Trigger) => {
                self.advance();
                Ok(ast::Capability::Trigger)
            }
            Some(Token::Strict) => {
                self.advance();
                Ok(ast::Capability::Strict)
            }
            Some(token) => Err(self.error(format!(
                "expected capability (trigger, strict), got {token}"
            ))),
            None => Err(self.error("expected capability, got EOF".to_string())),
        }
    }

    fn expect_block_content(&mut self) -> Result<String, ParseError> {
        match self.peek() {
            Some(Token::BlockContent(content)) => {
                let content = content.clone();
                self.advance();
                Ok(content)
            }
            Some(token) => Err(self.error(format!("expected block {{...}}, got {token}"))),
            None => Err(self.error("expected block, got EOF".to_string())),
        }
    }

    fn expect_token(&mut self, expected: Token) -> Result<(), ParseError> {
        match self.peek() {
            Some(token) if std::mem::discriminant(token) == std::mem::discriminant(&expected) => {
                self.advance();
                Ok(())
            }
            Some(token) => Err(self.error(format!("expected {expected}, got {token}"))),
            None => Err(self.error(format!("expected {expected}, got EOF"))),
        }
    }

    fn expect_identifier(&mut self) -> Result<String, ParseError> {
        match self.peek() {
            Some(Token::Identifier(name)) => {
                let name = name.clone();
                self.advance();
                Ok(name)
            }
            Some(token) => Err(self.error(format!("expected identifier, got {token}"))),
            None => Err(self.error("expected identifier, got EOF".to_string())),
        }
    }

    fn expect_identifier_with_span(&mut self) -> Result<(String, Range<usize>), ParseError> {
        match self.peek() {
            Some(Token::Identifier(name)) => {
                let name = name.clone();
                let span = self.current_span();
                self.advance();
                Ok((name, span))
            }
            Some(token) => Err(self.error(format!("expected identifier, got {token}"))),
            None => Err(self.error("expected identifier, got EOF".to_string())),
        }
    }

    fn expect_string(&mut self) -> Result<String, ParseError> {
        match self.peek() {
            Some(Token::String(s)) => {
                let s = s.clone();
                self.advance();
                Ok(s)
            }
            Some(token) => Err(self.error(format!("expected string, got {token}"))),
            None => Err(self.error("expected string, got EOF".to_string())),
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos).map(|t| &t.token)
    }

    fn current_span(&self) -> Range<usize> {
        self.tokens
            .get(self.pos)
            .map(|t| t.span.clone())
            .unwrap_or(0..0)
    }

    fn advance(&mut self) {
        if !self.is_at_end() {
            self.pos += 1;
        }
    }

    fn is_at_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn skip_newlines_and_comments(&mut self) {
        while matches!(self.peek(), Some(Token::Newline | Token::Comment(_))) {
            self.advance();
        }
    }

    fn error(&self, message: String) -> ParseError {
        let span = self
            .tokens
            .get(self.pos)
            .map(|token| SourceSpan::new(token.span.start.into(), token.span.len()));

        ParseError::SyntaxError {
            message,
            span,
            help: None,
        }
    }

    fn validate(&self, file: &TestFile) -> Result<(), ParseError> {
        // Rule 1: At least one database required
        if file.databases.is_empty() {
            return Err(ParseError::ValidationError {
                message: "at least one @database declaration is required".to_string(),
                span: None,
                help: Some(
                    "Add a @database directive at the top of the file, e.g.: @database :memory:"
                        .to_string(),
                ),
            });
        }

        // Rule 2: Cannot mix readonly and writable databases
        let has_readonly = file.databases.iter().any(|db| db.readonly);
        let has_writable = file.databases.iter().any(|db| !db.readonly);

        if has_readonly && has_writable {
            return Err(ParseError::ValidationError {
                message: "cannot mix readonly and writable databases in the same file".to_string(),
                span: None,
                help: Some(
                    "Use either all readonly databases or all writable databases".to_string(),
                ),
            });
        }

        // Rule 3: Setup blocks not allowed in readonly database files
        if has_readonly && !file.setups.is_empty() {
            return Err(ParseError::ValidationError {
                message: "setup blocks are not allowed in readonly database files".to_string(),
                span: None,
                help: Some("Remove setup blocks or use a writable database".to_string()),
            });
        }

        // Rule 4: All referenced setup names must exist
        for test in &file.tests {
            for setup_ref in &test.setups {
                if !file.setups.contains_key(&setup_ref.name) {
                    let available: Vec<_> = file.setups.keys().collect();
                    let help = if available.is_empty() {
                        "No setup blocks are defined in this file".to_string()
                    } else {
                        format!(
                            "Available setups: {}",
                            available
                                .iter()
                                .map(|s| s.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    };
                    return Err(ParseError::ValidationError {
                        message: format!(
                            "test '{}' references undefined setup '{}'",
                            test.name, setup_ref.name
                        ),
                        span: Some(SourceSpan::new(
                            setup_ref.span.start.into(),
                            setup_ref.span.len(),
                        )),
                        help: Some(help),
                    });
                }
            }
        }

        // Rule 5: Test names must be unique
        let mut seen_names: std::collections::HashMap<&str, Range<usize>> =
            std::collections::HashMap::new();
        for test in &file.tests {
            if let Some(first_span) = seen_names.get(test.name.as_str()) {
                return Err(ParseError::ValidationError {
                    message: format!("duplicate test name: {}", test.name),
                    span: Some(SourceSpan::new(
                        test.name_span.start.into(),
                        test.name_span.len(),
                    )),
                    help: Some(format!("First defined at offset {}", first_span.start)),
                });
            }
            seen_names.insert(&test.name, test.name_span.clone());
        }

        Ok(())
    }
}

#[derive(Debug, Clone, thiserror::Error, Diagnostic)]
pub enum ParseError {
    #[error(transparent)]
    #[diagnostic(transparent)]
    LexerError(#[from] lexer::LexerError),

    #[error("{message}")]
    #[diagnostic(code(sqltest::syntax))]
    SyntaxError {
        message: String,
        #[label("here")]
        span: Option<SourceSpan>,
        #[help]
        help: Option<String>,
    },

    #[error("{message}")]
    #[diagnostic(code(sqltest::validation))]
    ValidationError {
        message: String,
        #[label("here")]
        span: Option<SourceSpan>,
        #[help]
        help: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_file() {
        let input = r#"
@database :memory:

test select-1 {
    SELECT 1;
}
expect {
    1
}
"#;

        let file = parse(input).unwrap();
        assert_eq!(file.databases.len(), 1);
        assert_eq!(file.databases[0].location, DatabaseLocation::Memory);
        assert_eq!(file.tests.len(), 1);
        assert_eq!(file.tests[0].name, "select-1");
    }

    #[test]
    fn test_parse_with_setup() {
        let input = r#"
@database :memory:

setup users {
    CREATE TABLE users (id INTEGER);
}

@setup users
test select-users {
    SELECT * FROM users;
}
expect {
}
"#;

        let file = parse(input).unwrap();
        assert_eq!(file.setups.len(), 1);
        assert!(file.setups.contains_key("users"));
        assert_eq!(file.tests[0].setups.len(), 1);
        assert_eq!(file.tests[0].setups[0].name, "users");
    }

    #[test]
    fn test_parse_readonly_database() {
        let input = r#"
@database testing/test.db readonly

test select-count {
    SELECT COUNT(*) FROM users;
}
expect {
    100
}
"#;

        let file = parse(input).unwrap();
        assert!(file.databases[0].readonly);
        assert!(matches!(
            file.databases[0].location,
            DatabaseLocation::Path(_)
        ));
    }

    #[test]
    fn test_parse_expect_error() {
        let input = r#"
@database :memory:

test select-error {
    SELECT * FROM nonexistent;
}
expect error {
    no such table
}
"#;

        let file = parse(input).unwrap();
        assert!(matches!(
            file.tests[0].expectations.default,
            Expectation::Error(Some(_))
        ));
    }

    #[test]
    fn test_parse_expect_pattern() {
        let input = r#"
@database :memory:

test select-random {
    SELECT random();
}
expect pattern {
    ^-?\d+$
}
"#;

        let file = parse(input).unwrap();
        assert!(matches!(
            file.tests[0].expectations.default,
            Expectation::Pattern(_)
        ));
    }

    #[test]
    fn test_parse_skip() {
        let input = r#"
@database :memory:

@skip "known bug"
test buggy {
    SELECT buggy();
}
expect {
    1
}
"#;

        let file = parse(input).unwrap();
        assert_eq!(
            file.tests[0].skip,
            Some(ast::Skip {
                reason: "known bug".to_string(),
                condition: None,
            })
        );
    }

    #[test]
    fn test_parse_skip_if_mvcc() {
        let input = r#"
@database :memory:

@skip-if mvcc "total_changes not supported in MVCC"
test total-changes {
    SELECT total_changes();
}
expect {
    1
}
"#;

        let file = parse(input).unwrap();
        assert_eq!(
            file.tests[0].skip,
            Some(ast::Skip {
                reason: "total_changes not supported in MVCC".to_string(),
                condition: Some(ast::SkipCondition::Mvcc),
            })
        );
    }

    #[test]
    fn test_parse_expect_raw() {
        // Using explicit string to control whitespace precisely
        // The content "  hello  " has 2 leading and 2 trailing spaces
        let input = "@database :memory:\n\ntest select-spaces {\n    SELECT 1;\n}\nexpect raw {\n  hello  \n}\n";

        let file = parse(input).unwrap();
        // Raw mode preserves leading/trailing whitespace
        assert!(matches!(
            &file.tests[0].expectations.default,
            Expectation::Exact(rows) if rows == &vec!["  hello  ".to_string()]
        ));
    }

    #[test]
    fn test_parse_expect_raw_vs_normal() {
        // Normal mode trims whitespace
        let input_normal = r#"
@database :memory:

test select-1 {
    SELECT 1;
}
expect {
    hello world
}
"#;
        let file_normal = parse(input_normal).unwrap();
        assert!(matches!(
            &file_normal.tests[0].expectations.default,
            Expectation::Exact(rows) if rows == &vec!["hello world".to_string()]
        ));

        // Raw mode preserves whitespace (4 leading spaces, 2 trailing)
        let input_raw = "@database :memory:\n\ntest select-1 {\n    SELECT 1;\n}\nexpect raw {\n    hello world  \n}\n";
        let file_raw = parse(input_raw).unwrap();
        assert!(matches!(
            &file_raw.tests[0].expectations.default,
            Expectation::Exact(rows) if rows == &vec!["    hello world  ".to_string()]
        ));
    }

    #[test]
    fn test_validation_no_database() {
        let input = r#"
test select-1 {
    SELECT 1;
}
expect {
    1
}
"#;

        let result = parse(input);
        assert!(matches!(result, Err(ParseError::ValidationError { .. })));
    }

    #[test]
    fn test_validation_mixed_databases() {
        let input = r#"
@database :memory:
@database testing/test.db readonly

test select-1 {
    SELECT 1;
}
expect {
    1
}
"#;

        let result = parse(input);
        assert!(matches!(result, Err(ParseError::ValidationError { .. })));
    }

    #[test]
    fn test_validation_setup_in_readonly() {
        let input = r#"
@database testing/test.db readonly

setup users {
    CREATE TABLE users (id INTEGER);
}

test select-1 {
    SELECT 1;
}
expect {
    1
}
"#;

        let result = parse(input);
        assert!(matches!(result, Err(ParseError::ValidationError { .. })));
    }

    #[test]
    fn test_validation_undefined_setup() {
        let input = r#"
@database :memory:

@setup nonexistent
test select-1 {
    SELECT 1;
}
expect {
    1
}
"#;

        let result = parse(input);
        assert!(matches!(result, Err(ParseError::ValidationError { .. })));
    }

    #[test]
    fn test_parse_global_skip() {
        let input = r#"
@database :memory:
@skip-file "all tests skipped"

test select-1 {
    SELECT 1;
}
expect {
    1
}
"#;

        let file = parse(input).unwrap();
        assert_eq!(
            file.global_skip,
            Some(ast::Skip {
                reason: "all tests skipped".to_string(),
                condition: None,
            })
        );
        // Per-test skip should be None since we're using global skip
        assert!(file.tests[0].skip.is_none());
    }

    #[test]
    fn test_parse_global_skip_if_mvcc() {
        let input = r#"
@database :memory:
@skip-file-if mvcc "MVCC not supported for this file"

test select-1 {
    SELECT 1;
}
expect {
    1
}

test select-2 {
    SELECT 2;
}
expect {
    2
}
"#;

        let file = parse(input).unwrap();
        assert_eq!(
            file.global_skip,
            Some(ast::Skip {
                reason: "MVCC not supported for this file".to_string(),
                condition: Some(ast::SkipCondition::Mvcc),
            })
        );
        // All tests should have no per-test skip
        assert!(file.tests[0].skip.is_none());
        assert!(file.tests[1].skip.is_none());
    }

    #[test]
    fn test_parse_backend_specific_expectations() {
        let input = r#"
@database :memory:

test float-literal {
    SELECT 1.0;
}
expect {
    1.0
}
expect @js {
    1
}
"#;

        let file = parse(input).unwrap();
        assert_eq!(file.tests.len(), 1);

        // Check default expectation
        assert!(matches!(
            &file.tests[0].expectations.default,
            Expectation::Exact(rows) if rows == &vec!["1.0".to_string()]
        ));

        // Check JS-specific override
        assert!(matches!(
            file.tests[0].expectations.for_backend(ast::Backend::Js),
            Expectation::Exact(rows) if rows == &vec!["1".to_string()]
        ));

        // Check Rust backend gets default (no override)
        assert!(matches!(
            file.tests[0].expectations.for_backend(ast::Backend::Rust),
            Expectation::Exact(rows) if rows == &vec!["1.0".to_string()]
        ));
    }

    #[test]
    fn test_parse_backend_specific_error_expectations() {
        let input = r#"
@database :memory:

test error-test {
    SELECT * FROM nonexistent;
}
expect error {
    no such table
}
expect @cli error {
    table not found
}
"#;

        let file = parse(input).unwrap();

        // Check default is Error
        assert!(matches!(
            &file.tests[0].expectations.default,
            Expectation::Error(Some(s)) if s.contains("no such table")
        ));

        // Check CLI-specific override
        assert!(matches!(
            file.tests[0].expectations.for_backend(ast::Backend::Cli),
            Expectation::Error(Some(s)) if s.contains("table not found")
        ));
    }

    #[test]
    fn test_parse_invalid_backend_name() {
        let input = r#"
@database :memory:

test invalid-backend {
    SELECT 1;
}
expect {
    1
}
expect @invalid {
    1
}
"#;

        let result = parse(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_duplicate_backend_expectation() {
        let input = r#"
@database :memory:

test duplicate-backend {
    SELECT 1;
}
expect {
    1
}
expect @js {
    1
}
expect @js {
    2
}
"#;

        let result = parse(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_missing_default_expectation() {
        let input = r#"
@database :memory:

test no-default {
    SELECT 1;
}
expect @js {
    1
}
"#;

        let result = parse(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_global_skip_with_per_test_override() {
        let input = r#"
@database :memory:
@skip-file-if mvcc "global skip reason"

test test-with-override {
    SELECT 1;
}
expect {
    1
}

@skip "per-test skip"
test test-overridden {
    SELECT 2;
}
expect {
    2
}
"#;

        let file = parse(input).unwrap();
        // Global skip should be set
        assert_eq!(
            file.global_skip,
            Some(ast::Skip {
                reason: "global skip reason".to_string(),
                condition: Some(ast::SkipCondition::Mvcc),
            })
        );
        // First test has no per-test skip (uses global)
        assert!(file.tests[0].skip.is_none());
        // Second test has per-test skip (overrides global)
        assert_eq!(
            file.tests[1].skip,
            Some(ast::Skip {
                reason: "per-test skip".to_string(),
                condition: None,
            })
        );
    }

    #[test]
    fn test_parse_requires() {
        let input = r#"
@database :memory:

@requires trigger "test needs trigger support"
test trigger-test {
    CREATE TRIGGER test_trigger AFTER INSERT ON foo BEGIN SELECT 1; END;
}
expect {
    1
}
"#;

        let file = parse(input).unwrap();
        assert_eq!(file.tests[0].requires.len(), 1);
        assert_eq!(
            file.tests[0].requires[0].capability,
            ast::Capability::Trigger
        );
        assert_eq!(
            file.tests[0].requires[0].reason,
            "test needs trigger support"
        );
    }

    #[test]
    fn test_parse_requires_strict() {
        let input = r#"
@database :memory:

@requires strict "test needs strict tables"
test strict-test {
    CREATE TABLE foo (id INT) STRICT;
}
expect {
}
"#;

        let file = parse(input).unwrap();
        assert_eq!(file.tests[0].requires.len(), 1);
        assert_eq!(
            file.tests[0].requires[0].capability,
            ast::Capability::Strict
        );
        assert_eq!(file.tests[0].requires[0].reason, "test needs strict tables");
    }

    #[test]
    fn test_parse_requires_file() {
        let input = r#"
@database :memory:
@requires-file trigger "all tests need triggers"

test trigger-test-1 {
    SELECT 1;
}
expect {
    1
}

test trigger-test-2 {
    SELECT 2;
}
expect {
    2
}
"#;

        let file = parse(input).unwrap();
        assert_eq!(file.global_requires.len(), 1);
        assert_eq!(file.global_requires[0].capability, ast::Capability::Trigger);
        assert_eq!(file.global_requires[0].reason, "all tests need triggers");
        // Per-test requires should be empty
        assert!(file.tests[0].requires.is_empty());
        assert!(file.tests[1].requires.is_empty());
    }

    #[test]
    fn test_parse_multiple_requires() {
        let input = r#"
@database :memory:

@requires trigger "needs triggers"
@requires strict "needs strict tables"
test multi-require {
    SELECT 1;
}
expect {
    1
}
"#;

        let file = parse(input).unwrap();
        assert_eq!(file.tests[0].requires.len(), 2);
        assert!(
            file.tests[0]
                .requires
                .iter()
                .any(|r| r.capability == ast::Capability::Trigger)
        );
        assert!(
            file.tests[0]
                .requires
                .iter()
                .any(|r| r.capability == ast::Capability::Strict)
        );
    }

    #[test]
    fn test_parse_requires_file_and_per_test() {
        let input = r#"
@database :memory:
@requires-file trigger "file needs triggers"

test basic {
    SELECT 1;
}
expect {
    1
}

@requires strict "this test also needs strict"
test strict-test {
    SELECT 2;
}
expect {
    2
}
"#;

        let file = parse(input).unwrap();
        // Global requires
        assert_eq!(file.global_requires.len(), 1);
        assert_eq!(file.global_requires[0].capability, ast::Capability::Trigger);
        // First test has no per-test requires
        assert!(file.tests[0].requires.is_empty());
        // Second test has per-test requires for strict
        assert_eq!(file.tests[1].requires.len(), 1);
        assert_eq!(
            file.tests[1].requires[0].capability,
            ast::Capability::Strict
        );
    }
}
