//! PostgreSQL lexer implementation
//!
//! This lexer tokenizes PostgreSQL SQL input, handling PostgreSQL-specific
//! features like dollar quoting, :: type casts, and $n parameters.

use crate::error::{Error, Result};
use crate::token::{Token, TokenType};
use std::collections::HashMap;

pub struct Lexer<'a> {
    input: &'a [u8],
    pub(crate) position: usize,
    keywords: HashMap<String, TokenType>,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        let mut lexer = Lexer {
            input,
            position: 0,
            keywords: HashMap::new(),
        };
        lexer.init_keywords();
        lexer
    }

    fn init_keywords(&mut self) {
        use TokenType::*;
        let keywords = vec![
            ("ABORT", Abort),
            ("ADD", Add),
            ("ALL", All),
            ("ALTER", Alter),
            ("ANALYZE", Analyze),
            ("AND", And),
            ("ARRAY", Array),
            ("AS", As),
            ("ATTACH", Attach),
            ("ASC", Asc),
            ("BEGIN", Begin),
            ("BETWEEN", Between),
            ("BIGINT", Bigint),
            ("BIGSERIAL", Bigserial),
            ("BOOLEAN", Boolean),
            ("BUFFERS", Buffers),
            ("BY", By),
            ("CASCADE", Cascade),
            ("CASE", Case),
            ("CAST", Cast),
            ("CHAR", Char),
            ("CHECK", Check),
            ("CHECKPOINT", Checkpoint),
            ("CLUSTER", Cluster),
            ("COLLATE", Collate),
            ("COLUMN", Column),
            ("COMMIT", Commit),
            ("CONFLICT", Conflict),
            ("CONSTRAINT", Constraint),
            ("COPY", Copy),
            ("COSTS", Costs),
            ("CREATE", Create),
            ("CROSS", Cross),
            ("CSV", Csv),
            ("CURRENT", Current),
            ("DATABASE", Database),
            ("DATE", Date),
            ("DEALLOCATE", Deallocate),
            ("DECIMAL", Decimal),
            ("DEFAULT", Default),
            ("DELETE", Delete),
            ("DELIMITER", Delimiter),
            ("DESC", Desc),
            ("DISCARD", Discard),
            ("DISTINCT", Distinct),
            ("DO", Do),
            ("DOUBLE", Double),
            ("DROP", Drop),
            ("ELSE", Else),
            ("END", End),
            ("EXCEPT", Except),
            ("EXISTS", Exists),
            ("EXPLAIN", Explain),
            ("EXTENSION", Extension),
            ("FALSE", False),
            ("FIRST", First),
            ("FLOAT", Float),
            ("FOLLOWING", Following),
            ("FOR", For),
            ("FOREIGN", Foreign),
            ("FREEZE", Freeze),
            ("FROM", From),
            ("FULL", Full),
            ("FUNCTION", Function),
            ("GRANT", Grant),
            ("GROUP", Group),
            ("GROUPS", Groups),
            ("HAVING", Having),
            ("HEADER", Header),
            ("IF", If),
            ("ILIKE", Ilike),
            ("IN", In),
            ("INDEX", Index),
            ("INET", Inet),
            ("INNER", Inner),
            ("INSERT", Insert),
            ("INT", Int),
            ("INTEGER", Integer),
            ("INTERSECT", Intersect),
            ("INTERVAL", Interval),
            ("INTO", Into),
            ("IS", Is),
            ("ISOLATION", Isolation),
            ("ISNULL", Isnull),
            ("JOIN", Join),
            ("JSON", Json),
            ("JSONB", Jsonb),
            ("KEY", Key),
            ("LAST", Last),
            ("LATERAL", Lateral),
            ("LEFT", Left),
            ("LEVEL", Level),
            ("LIKE", Like),
            ("LIMIT", Limit),
            ("LISTEN", Listen),
            ("LOCK", Lock),
            ("LOCKED", Locked),
            ("MATERIALIZED", Materialized),
            ("NATURAL", Natural),
            ("NO", No),
            ("NORMALIZE", Normalize),
            ("NOT", Not),
            ("NOTHING", Nothing),
            ("NOTIFY", Notify),
            ("NOTNULL", Notnull),
            ("NOWAIT", Nowait),
            ("NULL", Null),
            ("NULLS", Nulls),
            ("NUMERIC", Numeric),
            ("OF", Of),
            ("OFFSET", Offset),
            ("ON", On),
            ("ONLY", Only),
            ("OR", Or),
            ("ORDER", Order),
            ("ORDINALITY", Ordinality),
            ("OUTER", Outer),
            ("OVER", Over),
            ("PARTITION", Partition),
            ("PLANS", Plans),
            ("PRECEDING", Preceding),
            ("PRECISION", Precision),
            ("PREPARE", Prepare),
            ("PRIMARY", Primary),
            ("PROCEDURE", Procedure),
            ("RANGE", Range),
            ("READ", Read),
            ("REAL", Real),
            ("RECURSIVE", Recursive),
            ("REFERENCES", References),
            ("REINDEX", Reindex),
            ("RELEASE", Release),
            ("RESET", Reset),
            ("RETURNING", Returning),
            ("REVOKE", Revoke),
            ("RIGHT", Right),
            ("ROLE", Role),
            ("ROLLBACK", Rollback),
            ("ROLLUP", Rollup),
            ("ROW", Row),
            ("ROWS", Rows),
            ("SAVEPOINT", Savepoint),
            ("SCHEMA", Schema),
            ("SELECT", Select),
            ("SEQUENCES", Sequences),
            ("SERIAL", Serial),
            ("SESSION", Session),
            ("SET", Set),
            ("SHARE", Share),
            ("SHOW", Show),
            ("SIMILAR", Similar),
            ("SKIP", Skip),
            ("SMALLINT", Smallint),
            ("SMALLSERIAL", Smallserial),
            ("STDIN", Stdin),
            ("STDOUT", Stdout),
            ("SYSTEM", System),
            ("TABLE", Table),
            ("TABLESPACE", Tablespace),
            ("TABLESAMPLE", Tablesample),
            ("TEMP", Temp),
            ("TEMPORARY", Temporary),
            ("TEXT", Text),
            ("THEN", Then),
            ("TIME", Time),
            ("TIMESTAMP", Timestamp),
            ("TIMESTAMPTZ", Timestamptz),
            ("TO", To),
            ("TRANSACTION", Transaction),
            ("TRIGGER", Trigger),
            ("TRUE", True),
            ("TRUNCATE", Truncate),
            ("TYPE", Type),
            ("UNBOUNDED", Unbounded),
            ("UNION", Union),
            ("UNIQUE", Unique),
            ("UPDATE", Update),
            ("USER", User),
            ("USING", Using),
            ("UUID", Uuid),
            ("VACUUM", Vacuum),
            ("VALUES", Values),
            ("VARCHAR", Varchar),
            ("VERBOSE", Verbose),
            ("VIEW", View),
            ("WHEN", When),
            ("WHERE", Where),
            ("WINDOW", Window),
            ("WITH", With),
            ("WRITE", Write),
            ("ZONE", Zone),
        ];

        for (kw, token) in keywords {
            self.keywords.insert(kw.to_string(), token);
        }
    }

    pub fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace_and_comments()?;

        if self.position >= self.input.len() {
            return Ok(Token::new(TokenType::Eof, String::new(), self.position));
        }

        let _start = self.position;
        let ch = self.current_char();

        let token = match ch {
            b'(' => self.single_char_token(TokenType::LeftParen),
            b')' => self.single_char_token(TokenType::RightParen),
            b'[' => self.single_char_token(TokenType::LeftBracket),
            b']' => self.single_char_token(TokenType::RightBracket),
            b'{' => self.single_char_token(TokenType::LeftBrace),
            b'}' => self.single_char_token(TokenType::RightBrace),
            b',' => self.single_char_token(TokenType::Comma),
            b';' => self.single_char_token(TokenType::Semicolon),
            b'+' => self.single_char_token(TokenType::Plus),
            b'*' => self.single_char_token(TokenType::Star),
            b'/' => self.single_char_token(TokenType::Slash),
            b'%' => self.single_char_token(TokenType::Percent),
            b'.' => self.single_char_token(TokenType::Dot),
            b':' => self.scan_colon()?,
            b'=' => self.single_char_token(TokenType::Equal),
            b'<' => self.scan_less()?,
            b'>' => self.scan_greater()?,
            b'!' => self.scan_exclamation()?,
            b'|' => self.scan_pipe()?,
            b'&' => self.scan_ampersand()?,
            b'?' => self.scan_question()?,
            b'#' => self.scan_hash()?,
            b'@' => self.scan_at()?,
            b'~' => self.scan_tilde()?,
            b'-' => self.scan_minus()?,
            b'$' => self.scan_dollar()?,
            b'\'' => self.scan_string()?,
            b'"' => self.scan_quoted_identifier()?,
            b'0'..=b'9' => self.scan_number()?,
            _ if is_identifier_start(ch) => self.scan_identifier()?,
            _ => {
                return Err(Error::UnexpectedToken {
                    token: format!("{}", ch as char),
                    position: self.position,
                })
            }
        };

        Ok(token)
    }

    fn current_char(&self) -> u8 {
        self.input[self.position]
    }

    fn peek_char(&self) -> Option<u8> {
        if self.position + 1 < self.input.len() {
            Some(self.input[self.position + 1])
        } else {
            None
        }
    }

    fn advance(&mut self) {
        self.position += 1;
    }

    fn single_char_token(&mut self, token_type: TokenType) -> Token {
        let pos = self.position;
        let ch = self.current_char();
        self.advance();
        Token::new(token_type, (ch as char).to_string(), pos)
    }

    fn skip_whitespace_and_comments(&mut self) -> Result<()> {
        while self.position < self.input.len() {
            match self.current_char() {
                b' ' | b'\t' | b'\n' | b'\r' => self.advance(),
                b'-' if self.peek_char() == Some(b'-') => {
                    // Skip line comment
                    self.advance();
                    self.advance();
                    while self.position < self.input.len() && self.current_char() != b'\n' {
                        self.advance();
                    }
                }
                b'/' if self.peek_char() == Some(b'*') => {
                    // Skip block comment
                    self.advance();
                    self.advance();
                    let mut depth = 1;
                    while depth > 0 && self.position < self.input.len() - 1 {
                        if self.current_char() == b'*' && self.peek_char() == Some(b'/') {
                            depth -= 1;
                            self.advance();
                            self.advance();
                        } else if self.current_char() == b'/' && self.peek_char() == Some(b'*') {
                            depth += 1;
                            self.advance();
                            self.advance();
                        } else {
                            self.advance();
                        }
                    }
                    if depth > 0 {
                        return Err(Error::SyntaxError("Unterminated comment".to_string()));
                    }
                }
                _ => break,
            }
        }
        Ok(())
    }

    fn scan_colon(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance();
        if self.position < self.input.len() && self.current_char() == b':' {
            self.advance();
            Ok(Token::new(TokenType::TypeCast, "::".to_string(), pos))
        } else {
            Ok(Token::new(TokenType::Colon, ":".to_string(), pos))
        }
    }

    fn scan_less(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance();
        if self.position < self.input.len() {
            match self.current_char() {
                b'=' => {
                    self.advance();
                    Ok(Token::new(TokenType::LessEqual, "<=".to_string(), pos))
                }
                b'>' => {
                    self.advance();
                    Ok(Token::new(TokenType::NotEqual, "<>".to_string(), pos))
                }
                b'@' => {
                    self.advance();
                    Ok(Token::new(TokenType::ContainedBy, "<@".to_string(), pos))
                }
                _ => Ok(Token::new(TokenType::Less, "<".to_string(), pos)),
            }
        } else {
            Ok(Token::new(TokenType::Less, "<".to_string(), pos))
        }
    }

    fn scan_greater(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance();
        if self.position < self.input.len() && self.current_char() == b'=' {
            self.advance();
            Ok(Token::new(TokenType::GreaterEqual, ">=".to_string(), pos))
        } else {
            Ok(Token::new(TokenType::Greater, ">".to_string(), pos))
        }
    }

    fn scan_exclamation(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance();
        if self.position < self.input.len() {
            match self.current_char() {
                b'=' => {
                    self.advance();
                    Ok(Token::new(TokenType::NotEqual, "!=".to_string(), pos))
                }
                b'~' => {
                    self.advance();
                    if self.position < self.input.len() && self.current_char() == b'*' {
                        self.advance();
                        Ok(Token::new(TokenType::NotTildeAny, "!~*".to_string(), pos))
                    } else {
                        Ok(Token::new(TokenType::NotTilde, "!~".to_string(), pos))
                    }
                }
                _ => Err(Error::UnexpectedToken {
                    token: format!("!{}", self.current_char() as char),
                    position: pos,
                }),
            }
        } else {
            Err(Error::UnexpectedToken {
                token: "!".to_string(),
                position: pos,
            })
        }
    }

    fn scan_pipe(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance();
        if self.position < self.input.len() && self.current_char() == b'|' {
            self.advance();
            Ok(Token::new(TokenType::Concat, "||".to_string(), pos))
        } else {
            Err(Error::UnexpectedToken {
                token: "|".to_string(),
                position: pos,
            })
        }
    }

    fn scan_ampersand(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance();
        if self.position < self.input.len() && self.current_char() == b'&' {
            self.advance();
            Ok(Token::new(TokenType::Overlap, "&&".to_string(), pos))
        } else {
            Err(Error::UnexpectedToken {
                token: "&".to_string(),
                position: pos,
            })
        }
    }

    fn scan_question(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance();
        if self.position < self.input.len() {
            match self.current_char() {
                b'&' => {
                    self.advance();
                    Ok(Token::new(TokenType::QuestionAnd, "?&".to_string(), pos))
                }
                b'|' => {
                    self.advance();
                    Ok(Token::new(TokenType::QuestionPipe, "?|".to_string(), pos))
                }
                _ => Ok(Token::new(TokenType::Question, "?".to_string(), pos)),
            }
        } else {
            Ok(Token::new(TokenType::Question, "?".to_string(), pos))
        }
    }

    fn scan_hash(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance();
        if self.position < self.input.len() && self.current_char() == b'>' {
            self.advance();
            if self.position < self.input.len() && self.current_char() == b'>' {
                self.advance();
                Ok(Token::new(TokenType::HashLongArrow, "#>>".to_string(), pos))
            } else {
                Ok(Token::new(TokenType::HashArrow, "#>".to_string(), pos))
            }
        } else {
            Err(Error::UnexpectedToken {
                token: "#".to_string(),
                position: pos,
            })
        }
    }

    fn scan_at(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance();
        if self.position < self.input.len() {
            match self.current_char() {
                b'@' => {
                    self.advance();
                    Ok(Token::new(TokenType::AtAt, "@@".to_string(), pos))
                }
                b'>' => {
                    self.advance();
                    Ok(Token::new(TokenType::Contains, "@>".to_string(), pos))
                }
                _ => Err(Error::UnexpectedToken {
                    token: format!("@{}", self.current_char() as char),
                    position: pos,
                }),
            }
        } else {
            Err(Error::UnexpectedToken {
                token: "@".to_string(),
                position: pos,
            })
        }
    }

    fn scan_tilde(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance();
        if self.position < self.input.len() && self.current_char() == b'*' {
            self.advance();
            Ok(Token::new(TokenType::TildeAny, "~*".to_string(), pos))
        } else {
            Ok(Token::new(TokenType::Tilde, "~".to_string(), pos))
        }
    }

    fn scan_minus(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance();
        if self.position < self.input.len() && self.current_char() == b'>' {
            self.advance();
            if self.position < self.input.len() && self.current_char() == b'>' {
                self.advance();
                Ok(Token::new(TokenType::LongArrow, "->>".to_string(), pos))
            } else {
                Ok(Token::new(TokenType::Arrow, "->".to_string(), pos))
            }
        } else {
            Ok(Token::new(TokenType::Minus, "-".to_string(), pos))
        }
    }

    fn scan_dollar(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance();

        // Check for dollar parameter ($1, $2, etc.)
        if self.position < self.input.len() && self.current_char().is_ascii_digit() {
            let mut num = String::new();
            while self.position < self.input.len() && self.current_char().is_ascii_digit() {
                num.push(self.current_char() as char);
                self.advance();
            }
            return Ok(Token::new(
                TokenType::DollarParameter,
                format!("${num}"),
                pos,
            ));
        }

        // Check for dollar-quoted string
        let tag_start = self.position;
        while self.position < self.input.len() {
            let ch = self.current_char();
            if ch == b'$' {
                let tag = if tag_start == self.position {
                    String::new()
                } else {
                    String::from_utf8_lossy(&self.input[tag_start..self.position]).to_string()
                };
                self.advance();

                // Now scan for the matching closing tag
                let content_start = self.position;
                while self.position < self.input.len() {
                    if self.current_char() == b'$' {
                        // Check if this is our closing tag
                        let potential_close_start = self.position + 1;
                        let potential_close_end = potential_close_start + tag.len();
                        if potential_close_end < self.input.len()
                            && self.input[potential_close_start..potential_close_end]
                                == tag.as_bytes()[..]
                            && self.input[potential_close_end] == b'$'
                        {
                            let content =
                                String::from_utf8_lossy(&self.input[content_start..self.position])
                                    .to_string();
                            self.position = potential_close_end + 1;
                            return Ok(Token::new(TokenType::DollarQuotedString, content, pos));
                        }
                    }
                    self.advance();
                }
                return Err(Error::UnterminatedDollarQuote { tag });
            }
            if !ch.is_ascii_alphanumeric() && ch != b'_' {
                break;
            }
            self.advance();
        }

        Err(Error::UnexpectedToken {
            token: "$".to_string(),
            position: pos,
        })
    }

    fn scan_string(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance(); // Skip opening '
        let mut value = String::new();

        while self.position < self.input.len() {
            let ch = self.current_char();
            if ch == b'\'' {
                self.advance();
                if self.position < self.input.len() && self.current_char() == b'\'' {
                    // Escaped quote
                    value.push('\'');
                    self.advance();
                } else {
                    return Ok(Token::new(TokenType::String, value, pos));
                }
            } else {
                value.push(ch as char);
                self.advance();
            }
        }

        Err(Error::UnterminatedString)
    }

    fn scan_quoted_identifier(&mut self) -> Result<Token> {
        let pos = self.position;
        self.advance(); // Skip opening "
        let mut value = String::new();

        while self.position < self.input.len() {
            let ch = self.current_char();
            if ch == b'"' {
                self.advance();
                if self.position < self.input.len() && self.current_char() == b'"' {
                    // Escaped quote
                    value.push('"');
                    self.advance();
                } else {
                    return Ok(Token::new(TokenType::QuotedIdentifier, value, pos));
                }
            } else {
                value.push(ch as char);
                self.advance();
            }
        }

        Err(Error::SyntaxError(
            "Unterminated quoted identifier".to_string(),
        ))
    }

    fn scan_number(&mut self) -> Result<Token> {
        let pos = self.position;
        let mut value = String::new();
        let mut has_dot = false;
        let mut has_e = false;

        while self.position < self.input.len() {
            let ch = self.current_char();
            match ch {
                b'0'..=b'9' => {
                    value.push(ch as char);
                    self.advance();
                }
                b'.' if !has_dot && !has_e => {
                    if self.peek_char().is_some_and(|c| c.is_ascii_digit()) {
                        has_dot = true;
                        value.push('.');
                        self.advance();
                    } else {
                        break;
                    }
                }
                b'e' | b'E' if !has_e => {
                    has_e = true;
                    value.push(ch as char);
                    self.advance();
                    if self.position < self.input.len() {
                        let next = self.current_char();
                        if next == b'+' || next == b'-' {
                            value.push(next as char);
                            self.advance();
                        }
                    }
                }
                b'_' => {
                    // PostgreSQL allows underscores in numbers
                    self.advance();
                }
                _ => break,
            }
        }

        if has_dot || has_e {
            Ok(Token::new(TokenType::FloatLiteral, value, pos))
        } else {
            Ok(Token::new(TokenType::IntegerLiteral, value, pos))
        }
    }

    fn scan_identifier(&mut self) -> Result<Token> {
        let pos = self.position;
        let start = self.position;

        while self.position < self.input.len() {
            let ch = self.current_char();
            if is_identifier_continue(ch) {
                self.advance();
            } else {
                break;
            }
        }

        let value = String::from_utf8_lossy(&self.input[start..self.position]).to_string();
        let upper = value.to_uppercase();

        let token_type = self
            .keywords
            .get(&upper)
            .copied()
            .unwrap_or(TokenType::Identifier);

        Ok(Token::new(token_type, value, pos))
    }

    /// Check if we're at end of input
    pub fn is_eof(&self) -> bool {
        self.position >= self.input.len()
    }

    /// Peek at the next token without consuming it
    pub fn peek_token(&mut self) -> Result<Token> {
        let saved_pos = self.position;
        let token = self.next_token()?;
        self.position = saved_pos;
        Ok(token)
    }
}

fn is_identifier_start(ch: u8) -> bool {
    ch.is_ascii_alphabetic() || ch == b'_'
}

fn is_identifier_continue(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'$'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_tokens() {
        let input = b"SELECT * FROM users WHERE id = 1";
        let mut lexer = Lexer::new(input);

        assert_eq!(lexer.next_token().unwrap().token_type, TokenType::Select);
        assert_eq!(lexer.next_token().unwrap().token_type, TokenType::Star);
        assert_eq!(lexer.next_token().unwrap().token_type, TokenType::From);
        assert_eq!(
            lexer.next_token().unwrap().token_type,
            TokenType::Identifier
        );
        assert_eq!(lexer.next_token().unwrap().token_type, TokenType::Where);
        assert_eq!(
            lexer.next_token().unwrap().token_type,
            TokenType::Identifier
        );
        assert_eq!(lexer.next_token().unwrap().token_type, TokenType::Equal);
        assert_eq!(
            lexer.next_token().unwrap().token_type,
            TokenType::IntegerLiteral
        );
    }

    #[test]
    fn test_dollar_parameters() {
        let input = b"SELECT * FROM users WHERE id = $1 AND name = $2";
        let mut lexer = Lexer::new(input);

        // Skip to the dollar parameters
        for _ in 0..7 {
            lexer.next_token().unwrap();
        }

        let token = lexer.next_token().unwrap();
        assert_eq!(token.token_type, TokenType::DollarParameter);
        assert_eq!(token.value, "$1");
    }

    #[test]
    fn test_type_cast() {
        let input = b"SELECT '123'::integer";
        let mut lexer = Lexer::new(input);

        lexer.next_token().unwrap(); // SELECT
        let string_token = lexer.next_token().unwrap();
        assert_eq!(string_token.token_type, TokenType::String);
        assert_eq!(string_token.value, "123");

        let cast_token = lexer.next_token().unwrap();
        assert_eq!(cast_token.token_type, TokenType::TypeCast);
        assert_eq!(cast_token.value, "::");
    }

    #[test]
    fn test_dollar_quoted_string() {
        let input = b"$$Hello World$$";
        let mut lexer = Lexer::new(input);

        let token = lexer.next_token().unwrap();
        assert_eq!(token.token_type, TokenType::DollarQuotedString);
        assert_eq!(token.value, "Hello World");
    }

    #[test]
    fn test_dollar_quoted_string_with_tag() {
        let input = b"$tag$Content with $ and ' characters$tag$";
        let mut lexer = Lexer::new(input);

        let token = lexer.next_token().unwrap();
        assert_eq!(token.token_type, TokenType::DollarQuotedString);
        assert_eq!(token.value, "Content with $ and ' characters");
    }

    #[test]
    fn test_json_operators() {
        let tests = vec![
            (&b"->"[..], TokenType::Arrow),
            (&b"->>"[..], TokenType::LongArrow),
            (&b"#>"[..], TokenType::HashArrow),
            (&b"#>>"[..], TokenType::HashLongArrow),
            (&b"@>"[..], TokenType::Contains),
            (&b"<@"[..], TokenType::ContainedBy),
        ];

        for (input, expected) in tests {
            let mut lexer = Lexer::new(input);
            let token = lexer.next_token().unwrap();
            assert_eq!(token.token_type, expected);
        }
    }
}
