pub mod ast;
pub mod lexer;

use ast::*;
use lexer::{SpannedToken, Token, line_col, tokenize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Parse a `.sqltest` file from source
pub fn parse(input: &str) -> Result<TestFile, ParseError> {
    let tokens = tokenize(input).map_err(|e| ParseError::LexerError(e.to_string()))?;
    let mut parser = Parser::new(input, tokens);
    parser.parse()
}

struct Parser<'a> {
    input: &'a str,
    tokens: Vec<SpannedToken>,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str, tokens: Vec<SpannedToken>) -> Self {
        Self {
            input,
            tokens,
            pos: 0,
        }
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
        let content = self.expect_block_content()?;

        Ok((name, content))
    }

    fn parse_test(&mut self) -> Result<TestCase, ParseError> {
        let mut test_setups = Vec::new();
        let mut skip = None;

        // Parse decorators
        loop {
            match self.peek() {
                Some(Token::AtSetup) => {
                    self.advance();
                    let setup_name = self.expect_identifier()?;
                    test_setups.push(setup_name);
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
        let name = self.expect_identifier()?;
        let sql = self.expect_block_content()?;

        self.skip_newlines_and_comments();

        // Parse expect
        self.expect_token(Token::Expect)?;

        let expectation = self.parse_expectation()?;

        Ok(TestCase {
            name,
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
                let content = self.expect_block_content()?;
                let pattern = if content.is_empty() {
                    None
                } else {
                    Some(content)
                };
                Ok(Expectation::Error(pattern))
            }
            Some(Token::Pattern) => {
                self.advance();
                let content = self.expect_block_content()?;
                Ok(Expectation::Pattern(content))
            }
            Some(Token::Unordered) => {
                self.advance();
                let content = self.expect_block_content()?;
                let rows = content.lines().map(|s| s.to_string()).collect();
                Ok(Expectation::Unordered(rows))
            }
            Some(Token::BlockContent(_)) => {
                let content = self.expect_block_content()?;
                let rows = content.lines().map(|s| s.to_string()).collect();
                Ok(Expectation::Exact(rows))
            }
            Some(token) => Err(self.error(format!(
                "expected expect modifier or block, got {token}"
            ))),
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
        let (line, column) = if let Some(token) = self.tokens.get(self.pos) {
            line_col(self.input, token.span.start)
        } else {
            (0, 0)
        };

        ParseError::SyntaxError {
            message,
            line,
            column,
        }
    }

    fn validate(&self, file: &TestFile) -> Result<(), ParseError> {
        // Rule 1: At least one database required
        if file.databases.is_empty() {
            return Err(ParseError::ValidationError(
                "at least one @database declaration is required".to_string(),
            ));
        }

        // Rule 2: Cannot mix readonly and writable databases
        let has_readonly = file.databases.iter().any(|db| db.readonly);
        let has_writable = file.databases.iter().any(|db| !db.readonly);

        if has_readonly && has_writable {
            return Err(ParseError::ValidationError(
                "cannot mix readonly and writable databases in the same file".to_string(),
            ));
        }

        // Rule 3: Setup blocks not allowed in readonly database files
        if has_readonly && !file.setups.is_empty() {
            return Err(ParseError::ValidationError(
                "setup blocks are not allowed in readonly database files".to_string(),
            ));
        }

        // Rule 4: All referenced setup names must exist
        for test in &file.tests {
            for setup_name in &test.setups {
                if !file.setups.contains_key(setup_name) {
                    return Err(ParseError::ValidationError(format!(
                        "test '{}' references undefined setup '{}'",
                        test.name, setup_name
                    )));
                }
            }
        }

        // Rule 5: Test names must be unique
        let mut seen_names = std::collections::HashSet::new();
        for test in &file.tests {
            if !seen_names.insert(&test.name) {
                return Err(ParseError::ValidationError(format!(
                    "duplicate test name: {}",
                    test.name
                )));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseError {
    #[error("lexer error: {0}")]
    LexerError(String),

    #[error("syntax error at line {line}, column {column}: {message}")]
    SyntaxError {
        message: String,
        line: usize,
        column: usize,
    },

    #[error("validation error: {0}")]
    ValidationError(String),
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
        assert_eq!(file.tests[0].setups, vec!["users"]);
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
        assert!(matches!(result, Err(ParseError::ValidationError(_))));
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
        assert!(matches!(result, Err(ParseError::ValidationError(_))));
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
        assert!(matches!(result, Err(ParseError::ValidationError(_))));
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
        assert!(matches!(result, Err(ParseError::ValidationError(_))));
    }
}
