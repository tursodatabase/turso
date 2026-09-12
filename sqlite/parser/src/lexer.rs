use crate::{error::Error, token::TokenType, Result};
use turso_macros::match_ignore_ascii_case;

/// Returns true if the given identifier (case-insensitive) is a SQL keyword.
/// This is used to determine whether an identifier needs to be quoted when
/// rendered back to SQL text.
pub fn is_quotable_keyword(input: &[u8]) -> bool {
    let token = keyword_or_id_token(input);
    token != TokenType::TK_ID && token != TokenType::TK_TYPE
}

fn keyword_or_id_token(input: &[u8]) -> TokenType {
    match_ignore_ascii_case!(match input {
        b"ABORT" => TokenType::TK_ABORT,
        b"ACTION" => TokenType::TK_ACTION,
        b"ADD" => TokenType::TK_ADD,
        b"AFTER" => TokenType::TK_AFTER,
        b"ALL" => TokenType::TK_ALL,
        b"ALTER" => TokenType::TK_ALTER,
        b"ALWAYS" => TokenType::TK_ALWAYS,
        b"ANALYZE" => TokenType::TK_ANALYZE,
        b"AND" => TokenType::TK_AND,
        b"AS" => TokenType::TK_AS,
        b"ASC" => TokenType::TK_ASC,
        b"ATTACH" => TokenType::TK_ATTACH,
        b"AUTOINCREMENT" => TokenType::TK_AUTOINCR,
        b"BEFORE" => TokenType::TK_BEFORE,
        b"BEGIN" => TokenType::TK_BEGIN,
        b"BETWEEN" => TokenType::TK_BETWEEN,
        b"BY" => TokenType::TK_BY,
        b"CASCADE" => TokenType::TK_CASCADE,
        b"CASE" => TokenType::TK_CASE,
        b"CAST" => TokenType::TK_CAST,
        b"CHECK" => TokenType::TK_CHECK,
        b"COLLATE" => TokenType::TK_COLLATE,
        b"COLUMN" => TokenType::TK_COLUMNKW,
        b"COMMIT" => TokenType::TK_COMMIT,
        b"CONCURRENT" => TokenType::TK_CONCURRENT,
        b"CONFLICT" => TokenType::TK_CONFLICT,
        b"CONSTRAINT" => TokenType::TK_CONSTRAINT,
        b"CREATE" => TokenType::TK_CREATE,
        b"CROSS" => TokenType::TK_JOIN_KW,
        b"CURRENT" => TokenType::TK_CURRENT,
        b"CURRENT_DATE" => TokenType::TK_CTIME_KW,
        b"CURRENT_TIME" => TokenType::TK_CTIME_KW,
        b"CURRENT_TIMESTAMP" => TokenType::TK_CTIME_KW,
        b"DATABASE" => TokenType::TK_DATABASE,
        b"DEFAULT" => TokenType::TK_DEFAULT,
        b"DEFERRABLE" => TokenType::TK_DEFERRABLE,
        b"DEFERRED" => TokenType::TK_DEFERRED,
        b"DELETE" => TokenType::TK_DELETE,
        b"DESC" => TokenType::TK_DESC,
        b"DETACH" => TokenType::TK_DETACH,
        b"DISTINCT" => TokenType::TK_DISTINCT,
        b"DO" => TokenType::TK_DO,
        b"DROP" => TokenType::TK_DROP,
        b"EACH" => TokenType::TK_EACH,
        b"ELSE" => TokenType::TK_ELSE,
        b"END" => TokenType::TK_END,
        b"ESCAPE" => TokenType::TK_ESCAPE,
        b"EXCEPT" => TokenType::TK_EXCEPT,
        b"EXCLUDE" => TokenType::TK_EXCLUDE,
        b"EXCLUSIVE" => TokenType::TK_EXCLUSIVE,
        b"EXISTS" => TokenType::TK_EXISTS,
        b"EXPLAIN" => TokenType::TK_EXPLAIN,
        b"FAIL" => TokenType::TK_FAIL,
        b"FILTER" => TokenType::TK_FILTER,
        b"FIRST" => TokenType::TK_FIRST,
        b"FOLLOWING" => TokenType::TK_FOLLOWING,
        b"FOR" => TokenType::TK_FOR,
        b"FOREIGN" => TokenType::TK_FOREIGN,
        b"FROM" => TokenType::TK_FROM,
        b"FULL" => TokenType::TK_JOIN_KW,
        b"GENERATED" => TokenType::TK_GENERATED,
        b"GLOB" => TokenType::TK_LIKE_KW,
        b"GROUP" => TokenType::TK_GROUP,
        b"GROUPS" => TokenType::TK_GROUPS,
        b"HAVING" => TokenType::TK_HAVING,
        b"IF" => TokenType::TK_IF,
        b"IGNORE" => TokenType::TK_IGNORE,
        b"IMMEDIATE" => TokenType::TK_IMMEDIATE,
        b"IN" => TokenType::TK_IN,
        b"INDEX" => TokenType::TK_INDEX,
        b"INDEXED" => TokenType::TK_INDEXED,
        b"INITIALLY" => TokenType::TK_INITIALLY,
        b"INNER" => TokenType::TK_JOIN_KW,
        b"INSERT" => TokenType::TK_INSERT,
        b"INSTEAD" => TokenType::TK_INSTEAD,
        b"INTERSECT" => TokenType::TK_INTERSECT,
        b"INTO" => TokenType::TK_INTO,
        b"IS" => TokenType::TK_IS,
        b"ISNULL" => TokenType::TK_ISNULL,
        b"JOIN" => TokenType::TK_JOIN,
        b"KEY" => TokenType::TK_KEY,
        b"LAST" => TokenType::TK_LAST,
        b"LEFT" => TokenType::TK_JOIN_KW,
        b"LIKE" => TokenType::TK_LIKE_KW,
        b"LIMIT" => TokenType::TK_LIMIT,
        b"MATCH" => TokenType::TK_MATCH,
        b"MATERIALIZED" => TokenType::TK_MATERIALIZED,
        b"NATURAL" => TokenType::TK_JOIN_KW,
        b"NO" => TokenType::TK_NO,
        b"NOT" => TokenType::TK_NOT,
        b"NOTHING" => TokenType::TK_NOTHING,
        b"NOTNULL" => TokenType::TK_NOTNULL,
        b"NULL" => TokenType::TK_NULL,
        b"NULLS" => TokenType::TK_NULLS,
        b"OF" => TokenType::TK_OF,
        b"OFFSET" => TokenType::TK_OFFSET,
        b"ON" => TokenType::TK_ON,
        b"OR" => TokenType::TK_OR,
        b"ORDER" => TokenType::TK_ORDER,
        b"OPTIMIZE" => TokenType::TK_OPTIMIZE,
        b"OTHERS" => TokenType::TK_OTHERS,
        b"OUTER" => TokenType::TK_JOIN_KW,
        b"OVER" => TokenType::TK_OVER,
        b"PARTITION" => TokenType::TK_PARTITION,
        b"PLAN" => TokenType::TK_PLAN,
        b"PRAGMA" => TokenType::TK_PRAGMA,
        b"PRECEDING" => TokenType::TK_PRECEDING,
        b"PRIMARY" => TokenType::TK_PRIMARY,
        b"QUERY" => TokenType::TK_QUERY,
        b"RAISE" => TokenType::TK_RAISE,
        b"RANGE" => TokenType::TK_RANGE,
        b"RECURSIVE" => TokenType::TK_RECURSIVE,
        b"REFERENCES" => TokenType::TK_REFERENCES,
        b"REGEXP" => TokenType::TK_LIKE_KW,
        b"REINDEX" => TokenType::TK_REINDEX,
        b"RELEASE" => TokenType::TK_RELEASE,
        b"RENAME" => TokenType::TK_RENAME,
        b"REPLACE" => TokenType::TK_REPLACE,
        b"RETURNING" => TokenType::TK_RETURNING,
        b"RESTRICT" => TokenType::TK_RESTRICT,
        b"RIGHT" => TokenType::TK_JOIN_KW,
        b"ROLLBACK" => TokenType::TK_ROLLBACK,
        b"ROW" => TokenType::TK_ROW,
        b"ROWS" => TokenType::TK_ROWS,
        b"SAVEPOINT" => TokenType::TK_SAVEPOINT,
        b"SELECT" => TokenType::TK_SELECT,
        b"SET" => TokenType::TK_SET,
        b"TABLE" => TokenType::TK_TABLE,
        b"TEMP" => TokenType::TK_TEMP,
        b"TEMPORARY" => TokenType::TK_TEMP,
        b"THEN" => TokenType::TK_THEN,
        b"TIES" => TokenType::TK_TIES,
        b"TO" => TokenType::TK_TO,
        b"TRANSACTION" => TokenType::TK_TRANSACTION,
        b"TRIGGER" => TokenType::TK_TRIGGER,
        b"TYPE" => TokenType::TK_TYPE,
        b"UNBOUNDED" => TokenType::TK_UNBOUNDED,
        b"UNION" => TokenType::TK_UNION,
        b"UNIQUE" => TokenType::TK_UNIQUE,
        b"UPDATE" => TokenType::TK_UPDATE,
        b"USING" => TokenType::TK_USING,
        b"VACUUM" => TokenType::TK_VACUUM,
        b"VALUES" => TokenType::TK_VALUES,
        b"VIEW" => TokenType::TK_VIEW,
        b"VIRTUAL" => TokenType::TK_VIRTUAL,
        b"WHEN" => TokenType::TK_WHEN,
        b"WHERE" => TokenType::TK_WHERE,
        b"WINDOW" => TokenType::TK_WINDOW,
        b"WITH" => TokenType::TK_WITH,
        b"WITHIN" => TokenType::TK_WITHIN,
        b"WITHOUT" => TokenType::TK_WITHOUT,
        _ => TokenType::TK_ID,
    })
}

#[inline(always)]
pub const fn is_identifier_start(b: u8) -> bool {
    b.is_ascii_uppercase() || b == b'_' || b.is_ascii_lowercase() || b > b'\x7F'
}

#[inline(always)]
pub const fn is_identifier_continue(b: u8) -> bool {
    b == b'$'
        || b.is_ascii_digit()
        || b.is_ascii_uppercase()
        || b == b'_'
        || b.is_ascii_lowercase()
        || b > b'\x7F'
}

#[derive(Clone, PartialEq, Eq, Debug)] // do not derive Copy for Token, just use .clone() when needed
pub struct Token<'a> {
    pub value: &'a [u8],
    pub token_type: TokenType, // None means Token is whitespaces or comments
}

impl<'a> Token<'a> {
    #[inline]
    pub const fn new(value: &'a [u8], token_type: TokenType) -> Self {
        Token { value, token_type }
    }
    #[inline]
    pub fn to_utf8(&self) -> String {
        String::from_utf8_lossy(self.as_bytes()).to_string()
    }
    /// # Safety
    /// Same as `String::from_utf8_unchecked`,
    /// the caller must ensure that token bytes are valid UTF-8.
    #[inline]
    pub unsafe fn to_utf8_unchecked(&self) -> String {
        String::from_utf8_unchecked(self.as_bytes().to_vec())
    }
    #[inline]
    pub const fn as_bytes(&self) -> &[u8] {
        self.value
    }
}

pub struct Lexer<'a> {
    pub(crate) offset: usize,
    pub(crate) input: &'a [u8],
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token<'a>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.peek() {
            None => None, // End of file
            Some(b) if b.is_ascii_whitespace() => Some(Ok(self.eat_white_space())),
            // matching logic
            Some(b) => match b {
                b'-' => Some(Ok(self.eat_minus_or_comment_or_ptr())),
                b'(' => Some(Ok(self.eat_one_token(TokenType::TK_LP))),
                b')' => Some(Ok(self.eat_one_token(TokenType::TK_RP))),
                b';' => Some(Ok(self.eat_one_token(TokenType::TK_SEMI))),
                b'+' => Some(Ok(self.eat_one_token(TokenType::TK_PLUS))),
                b'*' => Some(Ok(self.eat_one_token(TokenType::TK_STAR))),
                b'/' => Some(self.mark(|l| l.eat_slash_or_comment())),
                b'%' => Some(Ok(self.eat_one_token(TokenType::TK_REM))),
                b'=' => Some(Ok(self.eat_eq())),
                b'<' => Some(Ok(self.eat_le_or_ne_or_lshift_or_lt())),
                b'>' => Some(Ok(self.eat_ge_or_gt_or_rshift())),
                b'!' => Some(self.mark(|l| l.eat_ne())),
                b'|' => Some(Ok(self.eat_concat_or_bitor())),
                b',' => Some(Ok(self.eat_one_token(TokenType::TK_COMMA))),
                b'&' => Some(Ok(self.eat_overlap_or_bitand())),
                b'~' => Some(Ok(self.eat_one_token(TokenType::TK_BITNOT))),
                b'\'' | b'"' | b'`' => Some(self.mark(|l| l.eat_lit_or_id())),
                b'.' => Some(self.mark(|l| l.eat_dot_or_frac(false))),
                b'0'..=b'9' => Some(self.mark(|l| l.eat_number())),
                b'[' => Some(Ok(self.eat_one_token(TokenType::TK_LBRACKET))),
                b']' => Some(Ok(self.eat_one_token(TokenType::TK_RBRACKET))),
                b'@' => {
                    // @> is array contains operator; bare @ starts a variable
                    if self.input.get(self.offset + 1) == Some(&b'>') {
                        Some(Ok(self.eat_array_contains()))
                    } else {
                        Some(self.mark(|l| l.eat_var()))
                    }
                }
                b'?' | b'$' => Some(self.mark(|l| l.eat_var())),
                b'#' => {
                    let start = self.offset;
                    self.eat(); // consume '#'
                    self.eat_while(is_identifier_continue);
                    Some(Ok(Token::new(
                        &self.input[start..self.offset],
                        TokenType::TK_ILLEGAL,
                    )))
                }
                b':' => {
                    // `:name` is a named parameter, and so is `:::name` (the
                    // name may start with a "::" pair, as in SQLite). Any other
                    // `:` — before a digit, a space, `]`, a lone `:` — is a
                    // standalone colon, which the slice syntax `a[1:2]` needs.
                    // SQLite has no slices and reads `:2` as a parameter; that
                    // is the one place we deviate from its tokenizer.
                    match (
                        self.input.get(self.offset + 1),
                        self.input.get(self.offset + 2),
                    ) {
                        (Some(&b), _) if is_identifier_start(b) => Some(self.mark(|l| l.eat_var())),
                        (Some(&b':'), Some(&b':')) => Some(self.mark(|l| l.eat_var())),
                        _ => Some(Ok(self.eat_one_token(TokenType::TK_COLON))),
                    }
                }
                b if is_identifier_start(b) => Some(self.mark(|l| l.eat_blob_or_id())),
                _ => Some(self.eat_unrecognized()),
            },
        }
    }
}

#[cold]
const fn cold() {}

impl<'a> Lexer<'a> {
    #[inline(always)]
    pub const fn new(input: &'a [u8]) -> Self {
        Lexer { input, offset: 0 }
    }

    #[inline(always)]
    pub fn remaining(&self) -> &'a [u8] {
        self.input.get(self.offset..).unwrap_or(&[])
    }

    #[inline]
    pub fn mark<F, R>(&mut self, exc: F) -> Result<R>
    where
        F: FnOnce(&mut Self) -> Result<R>,
    {
        let start_offset = self.offset;
        let result = exc(self);
        if result.is_err() {
            self.offset = start_offset; // Reset to the start offset if an error occurs
        }
        result
    }

    /// Returns the current offset in the input without consuming.
    #[inline(always)]
    pub const fn peek(&self) -> Option<u8> {
        if self.offset < self.input.len() {
            Some(self.input[self.offset])
        } else {
            None // End of file
        }
    }

    /// Returns the current offset in the input and consumes it.
    #[inline(always)]
    pub const fn eat(&mut self) -> Option<u8> {
        if let Some(b) = self.peek() {
            self.offset += 1;
            Some(b)
        } else {
            None
        }
    }

    #[inline(always)]
    fn eat_and_assert<F>(&mut self, f: F)
    where
        F: Fn(u8) -> bool,
    {
        let _value = self.eat();
        debug_assert!(f(_value.unwrap()))
    }

    #[inline]
    // Eats up to but not including the specified byte, returns true if found
    fn eat_until(&mut self, byte: u8) -> bool {
        match memchr::memchr(byte, self.remaining()) {
            Some(pos) => {
                self.offset += pos;
                true
            }
            None => {
                cold();
                self.offset = self.input.len();
                false
            }
        }
    }

    #[inline]
    // Eats up to and including the specified byte, returns true if found
    fn eat_past(&mut self, byte: u8) -> bool {
        match memchr::memchr(byte, self.remaining()) {
            Some(pos) => {
                self.offset += pos + 1;
                true
            }
            None => {
                cold();
                self.offset = self.input.len();
                false
            }
        }
    }

    #[inline]
    fn eat_while<F>(&mut self, f: F)
    where
        F: Fn(u8) -> bool,
    {
        loop {
            if let Some(b) = self.peek() {
                if !f(b) {
                    cold();
                    return;
                }
            } else {
                cold();
                return;
            }

            self.eat();
        }
    }

    fn eat_while_number_digit(&mut self) -> Result<()> {
        loop {
            let start = self.offset;
            self.eat_while(|b| b.is_ascii_digit());
            match self.peek() {
                Some(b'_') => {
                    self.eat_and_assert(|b| b == b'_');

                    if start == self.offset {
                        // before the underscore, there was no digit
                        let token_text =
                            String::from_utf8_lossy(&self.input[start..self.offset]).to_string();
                        return Err(Error::BadNumber {
                            span: (start, self.offset - start).into(),
                            token_text,
                            offset: start,
                        });
                    }

                    match self.peek() {
                        Some(b) if b.is_ascii_digit() => continue, // Continue if next is a digit
                        _ => {
                            // after the underscore, there is no digit
                            let token_text =
                                String::from_utf8_lossy(&self.input[start..self.offset])
                                    .to_string();
                            return Err(Error::BadNumber {
                                span: (start, self.offset - start).into(),
                                token_text,
                                offset: start,
                            });
                        }
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    fn eat_while_number_hexdigit(&mut self) -> Result<()> {
        loop {
            let start = self.offset;
            self.eat_while(|b| b.is_ascii_hexdigit());
            match self.peek() {
                Some(b'_') => {
                    if start == self.offset {
                        // before the underscore, there was no digit
                        let token_text =
                            String::from_utf8_lossy(&self.input[start..self.offset]).to_string();
                        return Err(Error::BadNumber {
                            span: (start, self.offset - start).into(),
                            token_text,
                            offset: start,
                        });
                    }

                    self.eat_and_assert(|b| b == b'_');
                    match self.peek() {
                        Some(b) if b.is_ascii_hexdigit() => continue, // Continue if next is a digit
                        _ => {
                            // after the underscore, there is no digit
                            let token_text =
                                String::from_utf8_lossy(&self.input[start..self.offset])
                                    .to_string();
                            return Err(Error::BadNumber {
                                span: (start, self.offset - start).into(),
                                token_text,
                                offset: start,
                            });
                        }
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    #[inline]
    fn eat_one_token(&mut self, typ: TokenType) -> Token<'a> {
        debug_assert!(!self.remaining().is_empty());

        let tok = Token::new(self.remaining().get(..1).unwrap_or("".as_bytes()), typ);
        self.offset += 1;
        tok
    }

    #[inline]
    fn eat_white_space(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b.is_ascii_whitespace());
        self.eat_while(|b| b.is_ascii_whitespace());
        // This is whitespace
        Token::new(&self.input[start..self.offset], TokenType::TK_NONE)
    }

    fn eat_minus_or_comment_or_ptr(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'-');

        match self.peek() {
            Some(b'-') => {
                self.eat_and_assert(|b| b == b'-');
                if self.eat_until(b'\n') {
                    self.eat_and_assert(|b| b == b'\n');
                }

                Token::new(&self.input[start..self.offset], TokenType::TK_NONE)
            }
            Some(b'>') => {
                self.eat_and_assert(|b| b == b'>');
                if self.peek() == Some(b'>') {
                    self.eat_and_assert(|b| b == b'>');
                }

                Token::new(&self.input[start..self.offset], TokenType::TK_PTR)
            }
            _ => Token::new(b"-", TokenType::TK_MINUS),
        }
    }

    fn eat_slash_or_comment(&mut self) -> Result<Token<'a>> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'/');
        match self.peek() {
            // C-style comments begin with "/*" and extend up to and
            // including the next "*/" character pair or until
            // the end of input, whichever comes first.
            Some(b'*') => {
                self.eat_and_assert(|b| b == b'*');
                loop {
                    if self.eat_past(b'*') {
                        match self.peek() {
                            Some(b'/') => {
                                self.eat_and_assert(|b| b == b'/');
                                break; // End of block comment
                            }
                            None => break,
                            _ => {}
                        }
                    } else {
                        cold();
                        break;
                    }
                }

                Ok(Token::new(
                    &self.input[start..self.offset],
                    TokenType::TK_NONE, // This is a comment
                ))
            }
            _ => Ok(Token::new(b"/", TokenType::TK_SLASH)),
        }
    }

    fn eat_eq(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'=');
        if self.peek() == Some(b'=') {
            self.eat_and_assert(|b| b == b'=');
        }

        Token::new(&self.input[start..self.offset], TokenType::TK_EQ)
    }

    fn eat_le_or_ne_or_lshift_or_lt(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'<');
        match self.peek() {
            Some(b'=') => {
                self.eat_and_assert(|b| b == b'=');
                Token::new(&self.input[start..self.offset], TokenType::TK_LE)
            }
            Some(b'<') => {
                self.eat_and_assert(|b| b == b'<');
                Token::new(&self.input[start..self.offset], TokenType::TK_LSHIFT)
            }
            Some(b'>') => {
                self.eat_and_assert(|b| b == b'>');
                Token::new(&self.input[start..self.offset], TokenType::TK_NE)
            }
            _ => Token::new(b"<", TokenType::TK_LT),
        }
    }

    fn eat_ge_or_gt_or_rshift(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'>');
        match self.peek() {
            Some(b'=') => {
                self.eat_and_assert(|b| b == b'=');
                Token::new(&self.input[start..self.offset], TokenType::TK_GE)
            }
            Some(b'>') => {
                self.eat_and_assert(|b| b == b'>');
                Token::new(&self.input[start..self.offset], TokenType::TK_RSHIFT)
            }
            _ => Token::new(b">", TokenType::TK_GT),
        }
    }

    fn eat_ne(&mut self) -> Result<Token<'a>> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'!');
        match self.peek() {
            Some(b'=') => {
                self.eat_and_assert(|b| b == b'=');
            }
            _ => {
                let token_text =
                    String::from_utf8_lossy(&self.input[start..self.offset]).to_string();
                return Err(Error::ExpectedEqualsSign {
                    span: (start, self.offset - start).into(),
                    token_text,
                    offset: start,
                });
            }
        }

        Ok(Token::new(
            &self.input[start..self.offset],
            TokenType::TK_NE,
        ))
    }

    fn eat_concat_or_bitor(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'|');

        if self.peek() == Some(b'|') {
            self.eat_and_assert(|b| b == b'|');
            return Token::new(&self.input[start..self.offset], TokenType::TK_CONCAT);
        }

        // Otherwise it is a bitwise OR operator
        Token::new(&self.input[start..self.offset], TokenType::TK_BITOR)
    }

    /// Tokenize `&&` (array overlap) or single `&` (bitwise AND).
    fn eat_overlap_or_bitand(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'&');
        if self.peek() == Some(b'&') {
            self.eat_and_assert(|b| b == b'&');
            return Token::new(&self.input[start..self.offset], TokenType::TK_ARRAY_OVERLAP);
        }
        Token::new(&self.input[start..self.offset], TokenType::TK_BITAND)
    }

    /// Tokenize `@>` (array contains).
    fn eat_array_contains(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'@');
        self.eat_and_assert(|b| b == b'>');
        Token::new(
            &self.input[start..self.offset],
            TokenType::TK_ARRAY_CONTAINS,
        )
    }

    fn eat_lit_or_id(&mut self) -> Result<Token<'a>> {
        let start = self.offset;
        let quote = self.eat().unwrap();
        debug_assert!(quote == b'\'' || quote == b'"' || quote == b'`');
        let tt = if quote == b'\'' {
            TokenType::TK_STRING
        } else {
            TokenType::TK_ID
        };

        loop {
            if self.eat_past(quote) {
                match self.peek() {
                    Some(b) if b == quote => {
                        self.eat_and_assert(|b| b == quote);
                        continue;
                    }
                    _ => break,
                }
            } else {
                cold();
                let token_text =
                    String::from_utf8_lossy(&self.input[start..self.offset]).to_string();
                return Err(Error::UnterminatedLiteral {
                    span: (start, self.offset - start).into(),
                    token_text,
                    offset: start,
                });
            }
        }

        Ok(Token::new(&self.input[start..self.offset], tt))
    }

    fn eat_dot_or_frac(&mut self, has_digit_prefix: bool) -> Result<Token<'a>> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'.');

        match self.peek() {
            Some(b)
                if b.is_ascii_digit() || (has_digit_prefix && b.eq_ignore_ascii_case(&b'e')) =>
            {
                self.eat_while_number_digit()?;
                match self.peek() {
                    Some(b'e') | Some(b'E') => {
                        _ = self.eat_expo()?;
                        Ok(Token::new(
                            &self.input[start..self.offset],
                            TokenType::TK_FLOAT,
                        ))
                    }
                    Some(b) if is_identifier_start(b) => {
                        let token_text =
                            String::from_utf8_lossy(&self.input[start..self.offset]).to_string();
                        Err(Error::BadFractionalPart {
                            span: (start, self.offset - start).into(),
                            token_text,
                            offset: start,
                        })
                    }
                    _ => Ok(Token::new(
                        &self.input[start..self.offset],
                        TokenType::TK_FLOAT,
                    )),
                }
            }
            _ => Ok(Token::new(
                &self.input[start..self.offset],
                TokenType::TK_DOT,
            )),
        }
    }

    fn eat_expo(&mut self) -> Result<Token<'a>> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'e' || b == b'E');
        match self.peek() {
            Some(b'+') | Some(b'-') => {
                self.eat_and_assert(|b| b == b'+' || b == b'-');
            }
            _ => {}
        }

        let start_num = self.offset;
        self.eat_while_number_digit()?;
        if start_num == self.offset {
            let token_text = String::from_utf8_lossy(&self.input[start..self.offset]).to_string();
            return Err(Error::BadExponentPart {
                span: (start, self.offset - start).into(),
                token_text,
                offset: start,
            });
        }

        if self.peek().is_some() && is_identifier_start(self.peek().unwrap()) {
            let token_text = String::from_utf8_lossy(&self.input[start..self.offset]).to_string();
            return Err(Error::BadExponentPart {
                span: (start, self.offset - start).into(),
                token_text,
                offset: start,
            });
        }

        // This is a number
        Ok(Token::new(
            &self.input[start..self.offset],
            TokenType::TK_FLOAT,
        ))
    }

    fn eat_number(&mut self) -> Result<Token<'a>> {
        let start = self.offset;
        let first_digit = self.eat().unwrap();
        debug_assert!(first_digit.is_ascii_digit());

        // hex int
        if first_digit == b'0' {
            match self.peek() {
                Some(b'x') | Some(b'X') => {
                    self.eat_and_assert(|b| b == b'x' || b == b'X');
                    let start_hex = self.offset;
                    self.eat_while_number_hexdigit()?;

                    if start_hex == self.offset {
                        let token_text =
                            String::from_utf8_lossy(&self.input[start..self.offset]).to_string();
                        return Err(Error::MalformedHexInteger {
                            span: (start, self.offset - start).into(),
                            token_text,
                            offset: start,
                        });
                    }

                    if self.peek().is_some() && is_identifier_start(self.peek().unwrap()) {
                        let token_text =
                            String::from_utf8_lossy(&self.input[start..self.offset]).to_string();
                        return Err(Error::BadNumber {
                            span: (start, self.offset - start).into(),
                            token_text,
                            offset: start,
                        });
                    }

                    return Ok(Token::new(
                        &self.input[start..self.offset],
                        TokenType::TK_INTEGER,
                    ));
                }
                _ => {}
            }
        }

        self.eat_while_number_digit()?;
        match self.peek() {
            Some(b'.') => {
                self.eat_dot_or_frac(true)?;
                Ok(Token::new(
                    &self.input[start..self.offset],
                    TokenType::TK_FLOAT,
                ))
            }
            Some(b'e') | Some(b'E') => {
                self.eat_expo()?;
                Ok(Token::new(
                    &self.input[start..self.offset],
                    TokenType::TK_FLOAT,
                ))
            }
            Some(b) if is_identifier_start(b) => {
                let token_text =
                    String::from_utf8_lossy(&self.input[start..self.offset]).to_string();
                Err(Error::BadNumber {
                    span: (start, self.offset - start).into(),
                    token_text,
                    offset: start,
                })
            }
            _ => Ok(Token::new(
                &self.input[start..self.offset],
                TokenType::TK_INTEGER,
            )),
        }
    }

    /// Lex a parameter marker: `?` or `?NNN`, or a named parameter that
    /// starts with `$`, `@` or `:`.
    fn eat_var(&mut self) -> Result<Token<'a>> {
        let start = self.offset;
        let tok = self.eat().unwrap();
        debug_assert!(tok == b'?' || tok == b'$' || tok == b'@' || tok == b':');

        match tok {
            b'?' => {
                self.eat_while(|b| b.is_ascii_digit());

                Ok(Token::new(
                    &self.input[start..self.offset],
                    TokenType::TK_VARIABLE,
                ))
            }
            _ => self.eat_named_var(start),
        }
    }

    /// Lex the name of a named parameter. `start` is the offset of the
    /// prefix byte (`$`, `@` or `:`), which the caller has already eaten.
    fn eat_named_var(&mut self, start: usize) -> Result<Token<'a>> {
        // Same rule as SQLite's tokenizer (sqlite3GetToken, CC_VARALPHA):
        // the name is identifier bytes, and a "::" pair anywhere in it —
        // leading, middle or trailing — is part of the name. That is what
        // lets the TCL binding pass namespace-qualified variables ($::var,
        // $ns::var) as one parameter. After at least one identifier byte,
        // one "(...)" suffix ends the name, for TCL array elements like
        // $arr(elem); it must be closed and may not contain whitespace.
        // At least one identifier byte is required, so a bare `$` or `$::`
        // is still an error.
        let mut n_id = 0usize;
        let mut bad_suffix = false;
        loop {
            match self.peek() {
                Some(b) if is_identifier_continue(b) => {
                    n_id += 1;
                    self.eat();
                }
                Some(b':') if self.input.get(self.offset + 1) == Some(&b':') => {
                    self.eat();
                    self.eat();
                }
                Some(b'(') if n_id > 0 => {
                    self.eat();
                    self.eat_while(|b| b != b')' && !b.is_ascii_whitespace());
                    if self.peek() == Some(b')') {
                        self.eat();
                    } else {
                        // Unclosed, or whitespace inside the suffix.
                        bad_suffix = true;
                    }
                    break;
                }
                _ => break,
            }
        }

        if n_id == 0 || bad_suffix {
            // SQLite marks these TK_ILLEGAL and reports "unrecognized
            // token", not a variable-specific error.
            let token_text = String::from_utf8_lossy(&self.input[start..self.offset]).to_string();
            return Err(Error::UnrecognizedToken {
                span: (start, self.offset - start).into(),
                token_text,
                offset: start,
            });
        }

        Ok(Token::new(
            &self.input[start..self.offset],
            TokenType::TK_VARIABLE,
        ))
    }

    #[inline]
    fn eat_blob_or_id(&mut self) -> Result<Token<'a>> {
        let start = self.offset;
        let start_char = self.eat().unwrap();
        debug_assert!(is_identifier_start(start_char));

        match start_char {
            b'x' | b'X' if self.peek() == Some(b'\'') => {
                self.eat_and_assert(|b| b == b'\'');
                let start_hex = self.offset;
                self.eat_while(|b| b.is_ascii_hexdigit());

                match self.peek() {
                    Some(b'\'') => {
                        let end_hex = self.offset;
                        debug_assert!(end_hex >= start_hex);
                        self.eat_and_assert(|b| b == b'\'');

                        if ((end_hex - start_hex) & 1) != 0 {
                            let token_text =
                                String::from_utf8_lossy(&self.input[start..self.offset])
                                    .to_string();
                            return Err(Error::UnrecognizedToken {
                                span: (start, self.offset - start).into(),
                                token_text,
                                offset: start,
                            });
                        }
                        Ok(Token::new(
                            &self.input[start..self.offset],
                            TokenType::TK_BLOB,
                        ))
                    }
                    _ => {
                        let token_text =
                            String::from_utf8_lossy(&self.input[start..self.offset]).to_string();
                        Err(Error::UnterminatedLiteral {
                            span: (start, self.offset - start).into(),
                            token_text,
                            offset: start,
                        })
                    }
                }
            }
            _ => {
                self.eat_while(is_identifier_continue);
                let result = &self.input[start..self.offset];
                Ok(Token::new(result, keyword_or_id_token(result)))
            }
        }
    }

    fn eat_unrecognized(&mut self) -> Result<Token<'a>> {
        let start = self.offset;
        self.eat_while(|b| !b.is_ascii_whitespace());
        let token_text = String::from_utf8_lossy(&self.input[start..self.offset]).to_string();
        Err(Error::UnrecognizedToken {
            span: (start, self.offset - start).into(),
            token_text,
            offset: start,
        })
    }
}

#[cfg(test)]
#[path = "../tests/unit/lexer/tests.rs"]
mod tests;
