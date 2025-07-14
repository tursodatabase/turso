//! Adaptation/port of [`SQLite` tokenizer](http://www.sqlite.org/src/artifact?ci=trunk&filename=src/tokenize.c)
use fallible_iterator::FallibleIterator;
use memchr::memchr;

pub use crate::dialect::TokenType;
use crate::dialect::TokenType::*;
use crate::dialect::{is_identifier_continue, is_identifier_start, keyword_token, sentinel};
use crate::parser::ast::Cmd;
use crate::parser::parse::{yyParser, YYCODETYPE};
use crate::parser::Context;

mod error;
#[cfg(test)]
mod test;

use crate::lexer::scan::ScanError;
use crate::lexer::scan::Splitter;
use crate::lexer::Scanner;
pub use crate::parser::ParserError;
pub use error::Error;

// TODO Extract scanning stuff and move this into the parser crate
// to make possible to use the tokenizer without depending on the parser...

/// SQL parser
pub struct Parser<'input> {
    input: &'input [u8],
    scanner: Scanner<Tokenizer>,
    /// lemon parser
    parser: yyParser<'input>,
    had_error: bool,
}

impl<'input> Parser<'input> {
    /// Constructor
    pub fn new(input: &'input [u8]) -> Self {
        let lexer = Tokenizer::new();
        let scanner = Scanner::new(lexer);
        let ctx = Context::new(input);
        let parser = yyParser::new(ctx);
        Parser {
            input,
            scanner,
            parser,
            had_error: false,
        }
    }
    /// Parse new `input`
    pub fn reset(&mut self, input: &'input [u8]) {
        self.input = input;
        self.scanner.reset();
        self.had_error = false;
    }
    /// Current line position in input
    pub fn line(&self) -> u64 {
        self.scanner.line()
    }
    /// Current column position in input
    pub fn column(&self) -> usize {
        self.scanner.column()
    }

    /// Current byte offset in input
    pub fn offset(&self) -> usize {
        self.scanner.offset()
    }

    /// Public API for sqlite3ParserFinalize()
    pub fn finalize(&mut self) {
        self.parser.sqlite3ParserFinalize();
    }
}

/*
 ** Return the id of the next token in input.
 */
fn get_token(scanner: &mut Scanner<Tokenizer>, input: &[u8]) -> Result<TokenType, Error> {
    let mut t = {
        let (_, token_type) = match scanner.scan(input)? {
            (_, None, _) => {
                return Ok(TK_EOF);
            }
            (_, Some(tuple), _) => tuple,
        };
        token_type
    };
    if t == TK_ID
        || t == TK_STRING
        || t == TK_JOIN_KW
        || t == TK_WINDOW
        || t == TK_OVER
        || yyParser::parse_fallback(t as YYCODETYPE) == TK_ID as YYCODETYPE
    {
        t = TK_ID;
    }
    Ok(t)
}

/*
 ** The following three functions are called immediately after the tokenizer
 ** reads the keywords WINDOW, OVER and FILTER, respectively, to determine
 ** whether the token should be treated as a keyword or an SQL identifier.
 ** This cannot be handled by the usual lemon %fallback method, due to
 ** the ambiguity in some constructions. e.g.
 **
 **   SELECT sum(x) OVER ...
 **
 ** In the above, "OVER" might be a keyword, or it might be an alias for the
 ** sum(x) expression. If a "%fallback ID OVER" directive were added to
 ** grammar, then SQLite would always treat "OVER" as an alias, making it
 ** impossible to call a window-function without a FILTER clause.
 **
 ** WINDOW is treated as a keyword if:
 **
 **   * the following token is an identifier, or a keyword that can fallback
 **     to being an identifier, and
 **   * the token after than one is TK_AS.
 **
 ** OVER is a keyword if:
 **
 **   * the previous token was TK_RP, and
 **   * the next token is either TK_LP or an identifier.
 **
 ** FILTER is a keyword if:
 **
 **   * the previous token was TK_RP, and
 **   * the next token is TK_LP.
 */
fn analyze_window_keyword(
    scanner: &mut Scanner<Tokenizer>,
    input: &[u8],
) -> Result<TokenType, Error> {
    let t = get_token(scanner, input)?;
    if t != TK_ID {
        return Ok(TK_ID);
    };
    let t = get_token(scanner, input)?;
    if t != TK_AS {
        return Ok(TK_ID);
    };
    Ok(TK_WINDOW)
}
fn analyze_over_keyword(
    scanner: &mut Scanner<Tokenizer>,
    input: &[u8],
    last_token: TokenType,
) -> Result<TokenType, Error> {
    if last_token == TK_RP {
        let t = get_token(scanner, input)?;
        if t == TK_LP || t == TK_ID {
            return Ok(TK_OVER);
        }
    }
    Ok(TK_ID)
}
fn analyze_filter_keyword(
    scanner: &mut Scanner<Tokenizer>,
    input: &[u8],
    last_token: TokenType,
) -> Result<TokenType, Error> {
    if last_token == TK_RP && get_token(scanner, input)? == TK_LP {
        return Ok(TK_FILTER);
    }
    Ok(TK_ID)
}

macro_rules! try_with_position {
    ($scanner:expr, $expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => {
                let mut err = Error::from(err);
                err.position($scanner.line(), $scanner.column(), $scanner.offset() - 1);
                return Err(err);
            }
        }
    };
}

impl FallibleIterator for Parser<'_> {
    type Item = Cmd;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Cmd>, Error> {
        //print!("line: {}, column: {}: ", self.scanner.line(), self.scanner.column());
        // if we have already encountered an error, return None to signal that to fallible_iterator that we are done parsing
        if self.had_error {
            return Ok(None);
        }
        self.parser.ctx.reset();
        let mut last_token_parsed = TK_EOF;
        let mut eof = false;
        loop {
            let (start, (value, mut token_type), end) = match self.scanner.scan(self.input)? {
                (_, None, _) => {
                    eof = true;
                    break;
                }
                (start, Some(tuple), end) => (start, tuple, end),
            };

            if token_type == TK_ILLEGAL {
                //  break out of parsing loop and return error
                self.parser.sqlite3ParserFinalize();
                self.had_error = true;
                return Err(Error::UnrecognizedToken(
                    Some((self.scanner.line(), self.scanner.column())),
                    Some(start.into()),
                ));
            }

            let token = if token_type >= TK_WINDOW {
                debug_assert!(
                    token_type == TK_OVER || token_type == TK_FILTER || token_type == TK_WINDOW
                );
                self.scanner.mark();
                if token_type == TK_WINDOW {
                    token_type = analyze_window_keyword(&mut self.scanner, self.input)?;
                } else if token_type == TK_OVER {
                    token_type =
                        analyze_over_keyword(&mut self.scanner, self.input, last_token_parsed)?;
                } else if token_type == TK_FILTER {
                    token_type =
                        analyze_filter_keyword(&mut self.scanner, self.input, last_token_parsed)?;
                }
                self.scanner.reset_to_mark();
                token_type.to_token(start, value, end)
            } else {
                token_type.to_token(start, value, end)
            };
            //println!("({:?}, {:?})", token_type, token);
            try_with_position!(self.scanner, self.parser.sqlite3Parser(token_type, token));
            last_token_parsed = token_type;
            if self.parser.ctx.done() {
                //println!();
                break;
            }
        }
        if last_token_parsed == TK_EOF {
            return Ok(None); // empty input
        }
        /* Upon reaching the end of input, call the parser two more times
        with tokens TK_SEMI and 0, in that order. */
        if eof && self.parser.ctx.is_ok() {
            if last_token_parsed != TK_SEMI {
                try_with_position!(
                    self.scanner,
                    self.parser
                        .sqlite3Parser(TK_SEMI, sentinel(self.input.len()))
                );
                if self.parser.ctx.error().is_some() {
                    self.had_error = true;
                }
            }
            try_with_position!(
                self.scanner,
                self.parser
                    .sqlite3Parser(TK_EOF, sentinel(self.input.len()))
            );
            if self.parser.ctx.error().is_some() {
                self.had_error = true;
            }
        }
        self.parser.sqlite3ParserFinalize();
        if let Some(e) = self.parser.ctx.error() {
            let err = Error::ParserError(
                e,
                Some((self.scanner.line(), self.scanner.column())),
                Some((self.offset() - 1).into()),
            );
            self.had_error = true;
            return Err(err);
        }
        let cmd = self.parser.ctx.cmd();
        if let Some(ref cmd) = cmd {
            if let Err(e) = cmd.check() {
                let err = Error::ParserError(
                    e,
                    Some((self.scanner.line(), self.scanner.column())),
                    Some((self.offset() - 1).into()),
                );
                self.had_error = true;
                return Err(err);
            }
        }
        Ok(cmd)
    }
}

/// SQL token
pub type Token<'input> = (&'input [u8], TokenType);

/// SQL lexer
#[derive(Default)]
pub struct Tokenizer {}

impl Tokenizer {
    /// Constructor
    pub fn new() -> Self {
        Self {}
    }
}

/// ```rust
/// use turso_sqlite3_parser::lexer::sql::Tokenizer;
/// use turso_sqlite3_parser::lexer::Scanner;
///
/// let tokenizer = Tokenizer::new();
/// let input = b"PRAGMA parser_trace=ON;";
/// let mut s = Scanner::new(tokenizer);
/// let Ok((_, Some((token1, _)), _)) = s.scan(input) else { panic!() };
/// s.scan(input).unwrap();
/// assert!(b"PRAGMA".eq_ignore_ascii_case(token1));
/// ```
impl Splitter for Tokenizer {
    type Error = Error;
    type TokenType = TokenType;

    fn split<'input>(
        &mut self,
        data: &'input [u8],
    ) -> Result<(Option<Token<'input>>, usize), Error> {
        if data[0].is_ascii_whitespace() {
            // eat as much space as possible
            return Ok((
                None,
                match data.iter().skip(1).position(|&b| !b.is_ascii_whitespace()) {
                    Some(i) => i + 1,
                    _ => data.len(),
                },
            ));
        }
        match data[0] {
            b'-' => {
                if let Some(b) = data.get(1) {
                    if *b == b'-' {
                        // eat comment
                        if let Some(i) = memchr(b'\n', data) {
                            Ok((None, i + 1))
                        } else {
                            Ok((None, data.len()))
                        }
                    } else if *b == b'>' {
                        if let Some(b) = data.get(2) {
                            if *b == b'>' {
                                return Ok((Some((&data[..3], TK_PTR)), 3));
                            }
                        }
                        Ok((Some((&data[..2], TK_PTR)), 2))
                    } else {
                        Ok((Some((&data[..1], TK_MINUS)), 1))
                    }
                } else {
                    Ok((Some((&data[..1], TK_MINUS)), 1))
                }
            }
            b'(' => Ok((Some((&data[..1], TK_LP)), 1)),
            b')' => Ok((Some((&data[..1], TK_RP)), 1)),
            b';' => Ok((Some((&data[..1], TK_SEMI)), 1)),
            b'+' => Ok((Some((&data[..1], TK_PLUS)), 1)),
            b'*' => Ok((Some((&data[..1], TK_STAR)), 1)),
            b'/' => {
                if let Some(b) = data.get(1) {
                    if *b == b'*' {
                        // eat comment
                        let mut pb = 0;
                        let mut end = None;
                        for (i, b) in data.iter().enumerate().skip(2) {
                            if *b == b'/' && pb == b'*' {
                                end = Some(i);
                                break;
                            }
                            pb = *b;
                        }
                        if let Some(i) = end {
                            Ok((None, i + 1))
                        } else {
                            Err(Error::UnterminatedBlockComment(None, None))
                        }
                    } else {
                        Ok((Some((&data[..1], TK_SLASH)), 1))
                    }
                } else {
                    Ok((Some((&data[..1], TK_SLASH)), 1))
                }
            }
            b'%' => Ok((Some((&data[..1], TK_REM)), 1)),
            b'=' => {
                if let Some(b) = data.get(1) {
                    Ok(if *b == b'=' {
                        (Some((&data[..2], TK_EQ)), 2)
                    } else {
                        (Some((&data[..1], TK_EQ)), 1)
                    })
                } else {
                    Ok((Some((&data[..1], TK_EQ)), 1))
                }
            }
            b'<' => {
                if let Some(b) = data.get(1) {
                    Ok(match *b {
                        b'=' => (Some((&data[..2], TK_LE)), 2),
                        b'>' => (Some((&data[..2], TK_NE)), 2),
                        b'<' => (Some((&data[..2], TK_LSHIFT)), 2),
                        _ => (Some((&data[..1], TK_LT)), 1),
                    })
                } else {
                    Ok((Some((&data[..1], TK_LT)), 1))
                }
            }
            b'>' => {
                if let Some(b) = data.get(1) {
                    Ok(match *b {
                        b'=' => (Some((&data[..2], TK_GE)), 2),
                        b'>' => (Some((&data[..2], TK_RSHIFT)), 2),
                        _ => (Some((&data[..1], TK_GT)), 1),
                    })
                } else {
                    Ok((Some((&data[..1], TK_GT)), 1))
                }
            }
            b'!' => {
                if let Some(b) = data.get(1) {
                    if *b == b'=' {
                        Ok((Some((&data[..2], TK_NE)), 2))
                    } else {
                        Err(Error::ExpectedEqualsSign(None, None))
                    }
                } else {
                    Err(Error::ExpectedEqualsSign(None, None))
                }
            }
            b'|' => {
                if let Some(b) = data.get(1) {
                    Ok(if *b == b'|' {
                        (Some((&data[..2], TK_CONCAT)), 2)
                    } else {
                        (Some((&data[..1], TK_BITOR)), 1)
                    })
                } else {
                    Ok((Some((&data[..1], TK_BITOR)), 1))
                }
            }
            b',' => Ok((Some((&data[..1], TK_COMMA)), 1)),
            b'&' => Ok((Some((&data[..1], TK_BITAND)), 1)),
            b'~' => Ok((Some((&data[..1], TK_BITNOT)), 1)),
            quote @ (b'`' | b'\'' | b'"') => literal(data, quote),
            b'.' => {
                if let Some(b) = data.get(1) {
                    if b.is_ascii_digit() {
                        fractional_part(data, 0)
                    } else {
                        Ok((Some((&data[..1], TK_DOT)), 1))
                    }
                } else {
                    Ok((Some((&data[..1], TK_DOT)), 1))
                }
            }
            b'0'..=b'9' => number(data),
            b'[' => {
                if let Some(i) = memchr(b']', data) {
                    // Keep original quotes / '[' ... ’]'
                    Ok((Some((&data[0..=i], TK_ID)), i + 1))
                } else {
                    Err(Error::UnterminatedBracket(None, None))
                }
            }
            b'?' => {
                match data.iter().skip(1).position(|&b| !b.is_ascii_digit()) {
                    Some(i) => {
                        // do not include the '?' in the token
                        Ok((Some((&data[1..=i], TK_VARIABLE)), i + 1))
                    }
                    None => {
                        if !data[1..].is_empty() && data[1..].iter().all(|ch| *ch == b'0') {
                            return Err(Error::BadVariableName(None, None));
                        }
                        Ok((Some((&data[1..], TK_VARIABLE)), data.len()))
                    }
                }
            }
            b'$' | b'@' | b'#' | b':' => {
                match data
                    .iter()
                    .skip(1)
                    .position(|&b| !is_identifier_continue(b))
                {
                    Some(0) => Err(Error::BadVariableName(None, None)),
                    Some(i) => {
                        // '$' is included as part of the name
                        Ok((Some((&data[..=i], TK_VARIABLE)), i + 1))
                    }
                    None => {
                        if data.len() == 1 {
                            return Err(Error::BadVariableName(None, None));
                        }
                        Ok((Some((data, TK_VARIABLE)), data.len()))
                    }
                }
            }
            b if is_identifier_start(b) => {
                if b == b'x' || b == b'X' {
                    if let Some(&b'\'') = data.get(1) {
                        blob_literal(data)
                    } else {
                        Ok(self.identifierish(data))
                    }
                } else {
                    Ok(self.identifierish(data))
                }
            }
            // Return TK_ILLEGAL
            _ => handle_unrecognized(data),
        }
    }
}

fn handle_unrecognized(data: &[u8]) -> Result<(Option<Token<'_>>, usize), Error> {
    let mut end = 1;
    while end < data.len() && !data[end].is_ascii_whitespace() {
        end += 1;
    }

    Ok((Some((&data[..end], TokenType::TK_ILLEGAL)), end))
}

fn literal(data: &[u8], quote: u8) -> Result<(Option<Token<'_>>, usize), Error> {
    debug_assert_eq!(data[0], quote);
    let tt = if quote == b'\'' { TK_STRING } else { TK_ID };
    let mut pb = 0;
    let mut end = None;
    // data[0] == quote => skip(1)
    for (i, b) in data.iter().enumerate().skip(1) {
        if *b == quote {
            if pb == quote {
                // escaped quote
                pb = 0;
                continue;
            }
        } else if pb == quote {
            end = Some(i);
            break;
        }
        pb = *b;
    }
    if end.is_some() || pb == quote {
        let i = match end {
            Some(i) => i,
            _ => data.len(),
        };
        // keep original quotes in the token
        Ok((Some((&data[0..i], tt)), i))
    } else {
        Err(Error::UnterminatedLiteral(None, None))
    }
}

fn blob_literal(data: &[u8]) -> Result<(Option<Token<'_>>, usize), Error> {
    debug_assert!(data[0] == b'x' || data[0] == b'X');
    debug_assert_eq!(data[1], b'\'');

    let mut end = 2;
    let mut valid = true;
    while end < data.len() && data[end] != b'\'' {
        if !data[end].is_ascii_hexdigit() {
            valid = false;
        }
        end += 1;
    }

    let total_len = if end < data.len() { end + 1 } else { end };

    if !valid || (end - 2) % 2 != 0 || end >= data.len() {
        return Ok((Some((&data[..total_len], TokenType::TK_ILLEGAL)), total_len));
    }

    Ok((Some((&data[2..end], TokenType::TK_BLOB)), total_len))
}

fn number(data: &[u8]) -> Result<(Option<Token<'_>>, usize), Error> {
    debug_assert!(data[0].is_ascii_digit());
    if data[0] == b'0' {
        if let Some(b) = data.get(1) {
            if *b == b'x' || *b == b'X' {
                return hex_integer(data);
            }
        } else {
            return Ok((Some((data, TK_INTEGER)), data.len()));
        }
    }
    if let Some((i, b)) = find_end_of_number(data, 1, u8::is_ascii_digit)? {
        if b == b'.' {
            return fractional_part(data, i);
        } else if b == b'e' || b == b'E' {
            return exponential_part(data, i);
        } else if is_identifier_start(b) {
            return Err(Error::BadNumber(None, None, Some(i + 1), unsafe {
                String::from_utf8_unchecked(data[..i + 1].to_vec())
            }));
        }
        Ok((Some((&data[..i], TK_INTEGER)), i))
    } else {
        Ok((Some((data, TK_INTEGER)), data.len()))
    }
}

fn hex_integer(data: &[u8]) -> Result<(Option<Token<'_>>, usize), Error> {
    debug_assert_eq!(data[0], b'0');
    debug_assert!(data[1] == b'x' || data[1] == b'X');
    if let Some((i, b)) = find_end_of_number(data, 2, u8::is_ascii_hexdigit)? {
        // Must not be empty (Ox is invalid)
        if i == 2 || is_identifier_start(b) {
            let (len, help) = if i == 2 && !is_identifier_start(b) {
                (i, "Did you forget to add digits after '0x' or '0X'?")
            } else {
                (i + 1, "There are some invalid digits after '0x' or '0X'")
            };
            return Err(Error::MalformedHexInteger(
                None,
                None,
                Some(len),  // Length of the malformed hex
                Some(help), // Help Message
            ));
        }
        Ok((Some((&data[..i], TK_INTEGER)), i))
    } else {
        // Must not be empty (Ox is invalid)
        if data.len() == 2 {
            return Err(Error::MalformedHexInteger(
                None,
                None,
                Some(2), // Length of the malformed hex
                Some("Did you forget to add digits after '0x' or '0X'?"), // Help Message
            ));
        }
        Ok((Some((data, TK_INTEGER)), data.len()))
    }
}

fn fractional_part(data: &[u8], i: usize) -> Result<(Option<Token<'_>>, usize), Error> {
    debug_assert_eq!(data[i], b'.');
    if let Some((i, b)) = find_end_of_number(data, i + 1, u8::is_ascii_digit)? {
        if b == b'e' || b == b'E' {
            return exponential_part(data, i);
        } else if is_identifier_start(b) {
            return Err(Error::BadNumber(None, None, Some(i + 1), unsafe {
                String::from_utf8_unchecked(data[..i + 1].to_vec())
            }));
        }
        Ok((Some((&data[..i], TK_FLOAT)), i))
    } else {
        Ok((Some((data, TK_FLOAT)), data.len()))
    }
}

fn exponential_part(data: &[u8], i: usize) -> Result<(Option<Token<'_>>, usize), Error> {
    debug_assert!(data[i] == b'e' || data[i] == b'E');
    // data[i] == 'e'|'E'
    if let Some(b) = data.get(i + 1) {
        let i = if *b == b'+' || *b == b'-' { i + 1 } else { i };
        if let Some((j, b)) = find_end_of_number(data, i + 1, u8::is_ascii_digit)? {
            if j == i + 1 || is_identifier_start(b) {
                let len = if is_identifier_start(b) { j + 1 } else { j };
                return Err(Error::BadNumber(None, None, Some(len), unsafe {
                    String::from_utf8_unchecked(data[..len].to_vec())
                }));
            }
            Ok((Some((&data[..j], TK_FLOAT)), j))
        } else {
            if data.len() == i + 1 {
                return Err(Error::BadNumber(None, None, Some(i + 1), unsafe {
                    String::from_utf8_unchecked(data[..i + 1].to_vec())
                }));
            }
            Ok((Some((data, TK_FLOAT)), data.len()))
        }
    } else {
        Err(Error::BadNumber(None, None, Some(data.len()), unsafe {
            String::from_utf8_unchecked(data.to_vec())
        }))
    }
}

fn find_end_of_number(
    data: &[u8],
    i: usize,
    test: fn(&u8) -> bool,
) -> Result<Option<(usize, u8)>, Error> {
    for (j, &b) in data.iter().enumerate().skip(i) {
        if test(&b) {
            continue;
        } else if b == b'_' {
            if j >= 1 && data.get(j - 1).is_some_and(test) && data.get(j + 1).is_some_and(test) {
                continue;
            }
            return Err(Error::BadNumber(None, None, Some(j), unsafe {
                String::from_utf8_unchecked(data[..j].to_vec())
            }));
        } else {
            return Ok(Some((j, b)));
        }
    }
    Ok(None)
}

impl Tokenizer {
    fn identifierish<'input>(&mut self, data: &'input [u8]) -> (Option<Token<'input>>, usize) {
        debug_assert!(is_identifier_start(data[0]));
        // data[0] is_identifier_start => skip(1)
        let end = data
            .iter()
            .skip(1)
            .position(|&b| !is_identifier_continue(b));
        let i = match end {
            Some(i) => i + 1,
            _ => data.len(),
        };
        let word = &data[..i];
        (Some((word, keyword_token(word).unwrap_or(TK_ID))), i)
    }
}

#[cfg(test)]
mod tests {
    use super::Tokenizer;
    use crate::dialect::TokenType;
    use crate::lexer::sql::Error;
    use crate::lexer::Scanner;

    #[test]
    fn fallible_iterator() -> Result<(), Error> {
        let tokenizer = Tokenizer::new();
        let input = b"PRAGMA parser_trace=ON;";
        let mut s = Scanner::new(tokenizer);
        expect_token(&mut s, input, b"PRAGMA", TokenType::TK_PRAGMA)?;
        expect_token(&mut s, input, b"parser_trace", TokenType::TK_ID)?;
        Ok(())
    }

    #[test]
    fn invalid_number_literal() -> Result<(), Error> {
        let tokenizer = Tokenizer::new();
        let input = b"SELECT 1E;";
        let mut s = Scanner::new(tokenizer);
        expect_token(&mut s, input, b"SELECT", TokenType::TK_SELECT)?;
        let err = s.scan(input).unwrap_err();
        assert!(matches!(err, Error::BadNumber(_, _, _, _)));
        Ok(())
    }

    fn expect_token(
        s: &mut Scanner<Tokenizer>,
        input: &[u8],
        token: &[u8],
        token_type: TokenType,
    ) -> Result<(), Error> {
        let (t, tt) = s.scan(input)?.1.unwrap();
        assert_eq!(token, t);
        assert_eq!(token_type, tt);
        Ok(())
    }
}
