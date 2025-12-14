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

        while !self.is_at_end() {
            self.skip_newlines_and_comments();

            if self.is_at_end() {
                break;
            }

            match self.peek() {
                Some(Token::AtDatabase) => {
                    databases.push(self.parse_database()?);
                }
                Some(Token::Setup) => {
                    let (name, sql) = self.parse_setup()?;
                    if setups.contains_key(&name) {
                        return Err(self.error(format!("duplicate setup name: {name}")));
                    }
                    setups.insert(name, sql);
                }
                Some(Token::AtSetup | Token::AtSkip | Token::Test) => {
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
                "expected database specifier (:memory:, :temp:, or path), got {token}"
            ))),
            None => Err(self.error("expected database specifier, got EOF".to_string())),
        }
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
                    skip = Some(self.expect_string()?);
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

        // Parse expect
        self.expect_token(Token::Expect)?;

        let expectation = self.parse_expectation()?;

        Ok(TestCase {
            name,
            name_span,
            sql,
            expectation,
            setups: test_setups,
            skip,
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
                        test.name_span.len().into(),
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
            file.tests[0].expectation,
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
        assert!(matches!(file.tests[0].expectation, Expectation::Pattern(_)));
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
        assert_eq!(file.tests[0].skip, Some("known bug".to_string()));
    }

    #[test]
    fn test_parse_expect_raw() {
        // Using explicit string to control whitespace precisely
        // The content "  hello  " has 2 leading and 2 trailing spaces
        let input = "@database :memory:\n\ntest select-spaces {\n    SELECT 1;\n}\nexpect raw {\n  hello  \n}\n";

        let file = parse(input).unwrap();
        // Raw mode preserves leading/trailing whitespace
        assert!(matches!(
            &file.tests[0].expectation,
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
            &file_normal.tests[0].expectation,
            Expectation::Exact(rows) if rows == &vec!["hello world".to_string()]
        ));

        // Raw mode preserves whitespace (4 leading spaces, 2 trailing)
        let input_raw = "@database :memory:\n\ntest select-1 {\n    SELECT 1;\n}\nexpect raw {\n    hello world  \n}\n";
        let file_raw = parse(input_raw).unwrap();
        assert!(matches!(
            &file_raw.tests[0].expectation,
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
}
