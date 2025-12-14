use logos::{Lexer, Logos};
use miette::{Diagnostic, SourceSpan};
use std::fmt;

/// Extract block content between braces, handling nested braces
/// Note: Does NOT trim content - trimming is handled by the parser based on context
fn extract_block_content(lexer: &mut Lexer<'_, Token>) -> Option<String> {
    let remainder = lexer.remainder();
    let mut depth = 1;

    for (idx, ch) in remainder.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    let content = remainder[..idx].to_string();
                    // Bump past the content and the closing brace
                    lexer.bump(idx + 1);
                    return Some(content);
                }
            }
            _ => {}
        }
    }

    // Unterminated block - return None to signal error
    None
}

/// Token types for the `.sqltest` DSL
#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t\r]+")]
pub enum Token {
    /// `@database`
    #[token("@database")]
    AtDatabase,

    /// `@setup`
    #[token("@setup")]
    AtSetup,

    /// `@skip`
    #[token("@skip")]
    AtSkip,

    /// `setup` keyword
    #[token("setup")]
    Setup,

    /// `test` keyword
    #[token("test")]
    Test,

    /// `expect` keyword
    #[token("expect")]
    Expect,

    /// `error` modifier
    #[token("error")]
    Error,

    /// `pattern` modifier
    #[token("pattern")]
    Pattern,

    /// `unordered` modifier
    #[token("unordered")]
    Unordered,

    /// `raw` modifier (preserves whitespace in expect blocks)
    #[token("raw")]
    Raw,

    /// `readonly` modifier
    #[token("readonly")]
    Readonly,

    /// `:memory:`
    #[token(":memory:")]
    Memory,

    /// `:temp:`
    #[token(":temp:")]
    TempFile,

    /// `{` followed by content until matching `}`
    #[token("{", extract_block_content)]
    BlockContent(String),

    /// An identifier (setup name, test name)
    /// Starts with letter or underscore, followed by alphanumeric, underscore, or hyphen
    #[regex(r"[a-zA-Z_][a-zA-Z0-9_-]*", |lex| lex.slice().to_string())]
    Identifier(String),

    /// A quoted string
    #[regex(r#""([^"\\]|\\.)*""#, |lex| {
        let s = lex.slice();
        // Remove surrounding quotes
        s[1..s.len()-1].to_string()
    })]
    String(String),

    /// A path (for database files) - matches file paths
    #[regex(r"[a-zA-Z0-9_./-]+\.[a-zA-Z0-9]+", |lex| lex.slice().to_string())]
    Path(String),

    /// Comment (starts with #)
    #[regex(r"#[^\n]*", |lex| lex.slice()[1..].trim().to_string(), allow_greedy = true)]
    Comment(String),

    /// Newline
    #[token("\n")]
    Newline,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::AtDatabase => write!(f, "@database"),
            Token::AtSetup => write!(f, "@setup"),
            Token::AtSkip => write!(f, "@skip"),
            Token::Setup => write!(f, "setup"),
            Token::Test => write!(f, "test"),
            Token::Expect => write!(f, "expect"),
            Token::Error => write!(f, "error"),
            Token::Pattern => write!(f, "pattern"),
            Token::Unordered => write!(f, "unordered"),
            Token::Raw => write!(f, "raw"),
            Token::Readonly => write!(f, "readonly"),
            Token::Memory => write!(f, ":memory:"),
            Token::TempFile => write!(f, ":temp:"),
            Token::BlockContent(_) => write!(f, "{{...}}"),
            Token::Identifier(s) => write!(f, "{s}"),
            Token::String(s) => write!(f, "\"{s}\""),
            Token::Path(s) => write!(f, "{s}"),
            Token::Comment(s) => write!(f, "# {s}"),
            Token::Newline => write!(f, "\\n"),
        }
    }
}

/// A token with its span in the source
#[derive(Debug, Clone)]
pub struct SpannedToken {
    pub token: Token,
    pub span: std::ops::Range<usize>,
}

/// Tokenize input and collect all tokens with their spans
pub fn tokenize(input: &str) -> Result<Vec<SpannedToken>, LexerError> {
    let mut lexer = Token::lexer(input);
    let mut tokens = Vec::new();

    while let Some(result) = lexer.next() {
        match result {
            Ok(token) => {
                tokens.push(SpannedToken {
                    token,
                    span: lexer.span(),
                });
            }
            Err(()) => {
                let span = lexer.span();
                let slice = input[span.clone()].to_string();
                let help = suggest_fix(&slice);
                return Err(LexerError::InvalidToken {
                    span: SourceSpan::new(span.start.into(), span.len()),
                    slice,
                    help,
                });
            }
        }
    }

    Ok(tokens)
}

/// Suggest a fix for an invalid token
fn suggest_fix(slice: &str) -> Option<String> {
    if slice.starts_with('@') {
        Some(format!(
            "Valid directives are: @database, @setup, @skip. Did you mean one of these?"
        ))
    } else if slice.starts_with(':') {
        Some("Database specifiers are :memory: or :temp:".to_string())
    } else {
        None
    }
}

/// Calculate line and column from a byte offset
pub fn line_col(input: &str, offset: usize) -> (usize, usize) {
    let mut line = 1;
    let mut col = 1;

    for (i, ch) in input.char_indices() {
        if i >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }

    (line, col)
}

#[derive(Debug, Clone, thiserror::Error, Diagnostic)]
pub enum LexerError {
    #[error("invalid token '{slice}'")]
    #[diagnostic(code(sqltest::lexer::invalid_token))]
    InvalidToken {
        #[label("unrecognized token")]
        span: SourceSpan,
        slice: String,
        #[help]
        help: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_database_memory() {
        let input = "@database :memory:";
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].token, Token::AtDatabase);
        assert_eq!(tokens[1].token, Token::Memory);
    }

    #[test]
    fn test_tokenize_database_temp() {
        let input = "@database :temp:";
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].token, Token::AtDatabase);
        assert_eq!(tokens[1].token, Token::TempFile);
    }

    #[test]
    fn test_tokenize_readonly_database() {
        let input = "@database testing/test.db readonly";
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].token, Token::AtDatabase);
        assert_eq!(tokens[1].token, Token::Path("testing/test.db".to_string()));
        assert_eq!(tokens[2].token, Token::Readonly);
    }

    #[test]
    fn test_tokenize_setup_block() {
        let input = "setup users { CREATE TABLE users (id INTEGER); }";
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].token, Token::Setup);
        assert_eq!(tokens[1].token, Token::Identifier("users".to_string()));
        // Block content is not trimmed by the lexer (parser handles trimming)
        assert_eq!(
            tokens[2].token,
            Token::BlockContent(" CREATE TABLE users (id INTEGER); ".to_string())
        );
    }

    #[test]
    fn test_tokenize_test_with_decorators() {
        let input = "@setup users\n@skip \"known bug\"\ntest select-1 { SELECT 1; }";
        let tokens = tokenize(input).unwrap();

        let non_newline: Vec<_> = tokens
            .iter()
            .filter(|t| !matches!(t.token, Token::Newline))
            .collect();

        assert_eq!(non_newline[0].token, Token::AtSetup);
        assert_eq!(
            non_newline[1].token,
            Token::Identifier("users".to_string())
        );
        assert_eq!(non_newline[2].token, Token::AtSkip);
        assert_eq!(non_newline[3].token, Token::String("known bug".to_string()));
        assert_eq!(non_newline[4].token, Token::Test);
        assert_eq!(
            non_newline[5].token,
            Token::Identifier("select-1".to_string())
        );
        // Block content is not trimmed by the lexer (parser handles trimming)
        assert_eq!(
            non_newline[6].token,
            Token::BlockContent(" SELECT 1; ".to_string())
        );
    }

    #[test]
    fn test_tokenize_expect_modifiers() {
        let tokens = tokenize("expect error { no such table }").unwrap();
        assert_eq!(tokens[0].token, Token::Expect);
        assert_eq!(tokens[1].token, Token::Error);
        // Block content is not trimmed by the lexer (parser handles trimming)
        assert_eq!(
            tokens[2].token,
            Token::BlockContent(" no such table ".to_string())
        );

        let tokens = tokenize("expect pattern { ^\\d+$ }").unwrap();
        assert_eq!(tokens[1].token, Token::Pattern);

        let tokens = tokenize("expect unordered { 1\n2\n3 }").unwrap();
        assert_eq!(tokens[1].token, Token::Unordered);
    }

    #[test]
    fn test_tokenize_nested_braces() {
        let input = "test nested { SELECT json_object('a', 1); }";
        let tokens = tokenize(input).unwrap();

        assert_eq!(tokens[0].token, Token::Test);
        assert_eq!(tokens[1].token, Token::Identifier("nested".to_string()));
        // The json_object call has parens but no braces, should work fine
        // Block content is not trimmed by the lexer (parser handles trimming)
        assert_eq!(
            tokens[2].token,
            Token::BlockContent(" SELECT json_object('a', 1); ".to_string())
        );
    }

    #[test]
    fn test_tokenize_comment() {
        let input = "# This is a comment\n@database :memory:";
        let tokens = tokenize(input).unwrap();

        assert_eq!(
            tokens[0].token,
            Token::Comment("This is a comment".to_string())
        );
        assert_eq!(tokens[1].token, Token::Newline);
        assert_eq!(tokens[2].token, Token::AtDatabase);
    }

    #[test]
    fn test_line_col() {
        let input = "line1\nline2\nline3";
        assert_eq!(line_col(input, 0), (1, 1));
        assert_eq!(line_col(input, 5), (1, 6));
        assert_eq!(line_col(input, 6), (2, 1));
        assert_eq!(line_col(input, 12), (3, 1));
    }
}
