use crate::parser::{error::Error, token::TokenType};

include!(concat!(env!("OUT_DIR"), "/keywords.rs"));

#[inline(always)]
pub(crate) fn is_identifier_start(b: u8) -> bool {
    b.is_ascii_uppercase() || b == b'_' || b.is_ascii_lowercase() || b > b'\x7F'
}

#[inline(always)]
pub(crate) fn is_identifier_continue(b: u8) -> bool {
    b == b'$'
        || b.is_ascii_digit()
        || b.is_ascii_uppercase()
        || b == b'_'
        || b.is_ascii_lowercase()
        || b > b'\x7F'
}

#[derive(Clone, PartialEq, Eq)] // do not derive Copy for Token, just use .clone() when needed
pub struct Token<'a> {
    pub value: &'a [u8],
    pub token_type: Option<TokenType>, // None means Token is whitespaces or comments
}

pub struct Lexer<'a> {
    pub(crate) offset: usize,
    input: &'a [u8],
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token<'a>, Error>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match self.peek() {
            None => None, // End of file
            Some(b) if b.is_ascii_whitespace() => Some(Ok(self.eat_white_space())),
            // matching logic
            Some(b) => match b {
                b'-' => Some(Ok(self.eat_minus_or_comment_or_ptr())),
                b'(' => Some(Ok(self.eat_token(1, TokenType::TK_LP))),
                b')' => Some(Ok(self.eat_token(1, TokenType::TK_RP))),
                b';' => Some(Ok(self.eat_token(1, TokenType::TK_SEMI))),
                b'+' => Some(Ok(self.eat_token(1, TokenType::TK_PLUS))),
                b'*' => Some(Ok(self.eat_token(1, TokenType::TK_STAR))),
                b'/' => Some(self.mark(|l| l.eat_slash_or_comment())),
                b'%' => Some(Ok(self.eat_token(1, TokenType::TK_REM))),
                b'=' => Some(Ok(self.eat_eq())),
                b'<' => Some(Ok(self.eat_le_or_ne_or_lshift_or_lt())),
                b'>' => Some(Ok(self.eat_ge_or_gt_or_rshift())),
                b'!' => Some(self.mark(|l| l.eat_ne())),
                b'|' => Some(Ok(self.eat_concat_or_bitor())),
                b',' => Some(Ok(self.eat_token(1, TokenType::TK_COMMA))),
                b'&' => Some(Ok(self.eat_token(1, TokenType::TK_BITAND))),
                b'~' => Some(Ok(self.eat_token(1, TokenType::TK_BITNOT))),
                b'\'' | b'"' | b'`' => Some(self.mark(|l| l.eat_lit_or_id())),
                b'.' => Some(self.mark(|l| l.eat_dot_or_frac())),
                b'0'..=b'9' => Some(self.mark(|l| l.eat_number())),
                b'[' => Some(self.mark(|l| l.eat_bracket())),
                b'?' | b'$' | b'@' | b'#' | b':' => Some(self.mark(|l| l.eat_var())),
                b if is_identifier_start(b) => Some(self.mark(|l| l.eat_blob_or_id())),
                _ => Some(Ok(self.eat_unrecognized())),
            },
        }
    }
}

impl<'a> Lexer<'a> {
    #[inline(always)]
    pub fn new(input: &'a [u8]) -> Self {
        Lexer { input, offset: 0 }
    }

    #[inline(always)]
    pub fn remaining(&self) -> &'a [u8] {
        &self.input[self.offset..]
    }

    #[inline(always)]
    pub fn mark<F, R>(&mut self, exc: F) -> Result<R, Error>
    where
        F: FnOnce(&mut Self) -> Result<R, Error>,
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
    pub fn peek(&self) -> Option<u8> {
        if self.offset < self.input.len() {
            Some(self.input[self.offset])
        } else {
            None // End of file
        }
    }

    /// Returns the current offset in the input and consumes it.
    #[inline(always)]
    pub fn eat(&mut self) -> Option<u8> {
        let result = self.peek();
        if result.is_some() {
            self.offset += 1;
        }

        result
    }

    #[inline(always)]
    fn eat_and_assert<F>(&mut self, f: F)
    where
        F: Fn(u8) -> bool,
    {
        let _value = self.eat();
        debug_assert!(f(_value.unwrap()))
    }

    #[inline(always)]
    fn eat_while<F>(&mut self, f: F)
    where
        F: Fn(Option<u8>) -> bool,
    {
        loop {
            if !f(self.peek()) {
                return;
            }

            self.eat();
        }
    }

    #[inline(always)]
    fn eat_while_number_digit(&mut self) -> Result<(), Error> {
        loop {
            let start = self.offset;
            self.eat_while(|b| b.is_some() && b.unwrap().is_ascii_digit());
            match self.peek() {
                Some(b'_') => {
                    if start == self.offset {
                        // before the underscore, there was no digit
                        return Err(Error::BadNumber(self.offset, None, unsafe {
                            String::from_utf8_unchecked(self.input[start..self.offset].to_vec())
                        }));
                    }

                    self.eat_and_assert(|b| b == b'_');
                    match self.peek() {
                        Some(b) if b.is_ascii_digit() => continue, // Continue if next is a digit
                        _ => {
                            // after the underscore, there is no digit
                            return Err(Error::BadNumber(self.offset, None, unsafe {
                                String::from_utf8_unchecked(self.input[start..self.offset].to_vec())
                            }));
                        }
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    #[inline(always)]
    fn eat_while_number_hexdigit(&mut self) -> Result<(), Error> {
        loop {
            let start = self.offset;
            self.eat_while(|b| b.is_some() && b.unwrap().is_ascii_hexdigit());
            match self.peek() {
                Some(b'_') => {
                    if start == self.offset {
                        // before the underscore, there was no digit
                        return Err(Error::BadNumber(self.offset, None, unsafe {
                            String::from_utf8_unchecked(self.input[start..self.offset].to_vec())
                        }));
                    }

                    self.eat_and_assert(|b| b == b'_');
                    match self.peek() {
                        Some(b) if b.is_ascii_hexdigit() => continue, // Continue if next is a digit
                        _ => {
                            // after the underscore, there is no digit
                            return Err(Error::BadNumber(self.offset, None, unsafe {
                                String::from_utf8_unchecked(self.input[start..self.offset].to_vec())
                            }));
                        }
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    #[inline(always)]
    fn eat_token(&mut self, size: usize, typ: TokenType) -> Token<'a> {
        debug_assert!(size > 0);
        debug_assert!(self.remaining().len() >= size);

        let tok = Token {
            value: &self.remaining()[..size],
            token_type: Some(typ),
        };
        self.offset += size;
        tok
    }

    #[inline(always)]
    fn eat_white_space(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b.is_ascii_whitespace());
        self.eat_while(|b| b.is_some() && b.unwrap().is_ascii_whitespace());
        Token {
            value: &self.input[start..self.offset],
            token_type: None, // This is a whitespace
        }
    }

    #[inline(always)]
    fn eat_minus_or_comment_or_ptr(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'-');

        match self.peek() {
            Some(b'-') => {
                self.eat_and_assert(|b| b == b'-');
                self.eat_while(|b| b.is_some() && b.unwrap() != b'\n');
                if self.peek() == Some(b'\n') {
                    self.eat_and_assert(|b| b == b'\n');
                }

                Token {
                    value: &self.input[start..self.offset],
                    token_type: None, // This is a comment
                }
            }
            Some(b'>') => {
                self.eat_and_assert(|b| b == b'>');
                if self.peek() == Some(b'>') {
                    self.eat_and_assert(|b| b == b'>');
                }

                Token {
                    value: &self.input[start..self.offset],
                    token_type: Some(TokenType::TK_RP),
                }
            }
            _ => Token {
                value: &self.input[start..self.offset],
                token_type: Some(TokenType::TK_MINUS),
            },
        }
    }

    #[inline(always)]
    fn eat_slash_or_comment(&mut self) -> Result<Token<'a>, Error> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'/');
        match self.peek() {
            Some(b'*') => {
                self.eat_and_assert(|b| b == b'*');
                loop {
                    self.eat_while(|b| b.is_some() && b.unwrap() != b'*');
                    match self.peek() {
                        Some(b'*') => {
                            self.eat_and_assert(|b| b == b'*');
                            match self.peek() {
                                Some(b'/') => {
                                    self.eat_and_assert(|b| b == b'/');
                                    break; // End of block comment
                                }
                                None => {
                                    return Err(Error::UnterminatedBlockComment(self.offset, None))
                                }
                                _ => {}
                            }
                        }
                        None => return Err(Error::UnterminatedBlockComment(self.offset, None)),
                        _ => unreachable!(), // We should not reach here
                    }
                }

                Ok(Token {
                    value: &self.input[start..self.offset],
                    token_type: None, // This is a comment
                })
            }
            _ => Ok(Token {
                value: &self.input[start..self.offset],
                token_type: Some(TokenType::TK_SLASH),
            }),
        }
    }

    #[inline(always)]
    fn eat_eq(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'=');
        if self.peek() == Some(b'=') {
            self.eat_and_assert(|b| b == b'=');
        }

        Token {
            value: &self.input[start..self.offset],
            token_type: Some(TokenType::TK_EQ),
        }
    }

    #[inline(always)]
    fn eat_le_or_ne_or_lshift_or_lt(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'<');
        match self.peek() {
            Some(b'=') => {
                self.eat_and_assert(|b| b == b'=');
                Token {
                    value: &self.input[start..self.offset],
                    token_type: Some(TokenType::TK_LE),
                }
            }
            Some(b'<') => {
                self.eat_and_assert(|b| b == b'<');
                Token {
                    value: &self.input[start..self.offset],
                    token_type: Some(TokenType::TK_LSHIFT),
                }
            }
            Some(b'>') => {
                self.eat_and_assert(|b| b == b'>');
                Token {
                    value: &self.input[start..self.offset],
                    token_type: Some(TokenType::TK_NE),
                }
            }
            _ => Token {
                value: &self.input[start..self.offset],
                token_type: Some(TokenType::TK_LT),
            },
        }
    }

    #[inline(always)]
    fn eat_ge_or_gt_or_rshift(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'>');
        match self.peek() {
            Some(b'=') => {
                self.eat_and_assert(|b| b == b'=');
                Token {
                    value: &self.input[start..self.offset],
                    token_type: Some(TokenType::TK_GE),
                }
            }
            Some(b'>') => {
                self.eat_and_assert(|b| b == b'>');
                Token {
                    value: &self.input[start..self.offset],
                    token_type: Some(TokenType::TK_RSHIFT),
                }
            }
            _ => Token {
                value: &self.input[start..self.offset],
                token_type: Some(TokenType::TK_GT),
            },
        }
    }

    #[inline(always)]
    fn eat_ne(&mut self) -> Result<Token<'a>, Error> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'!');
        match self.peek() {
            Some(b'=') => {
                self.eat_and_assert(|b| b == b'=');
            }
            _ => return Err(Error::ExpectedEqualsSign(self.offset, None)),
        }

        Ok(Token {
            value: &self.input[start..self.offset],
            token_type: Some(TokenType::TK_NE),
        })
    }

    #[inline(always)]
    fn eat_concat_or_bitor(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'|');
        if self.peek() == Some(b'|') {
            self.eat_and_assert(|b| b == b'|');
            return Token {
                value: &self.input[start..self.offset],
                token_type: Some(TokenType::TK_CONCAT),
            };
        }

        Token {
            value: &self.input[start..self.offset],
            token_type: Some(TokenType::TK_BITOR),
        }
    }

    #[inline(always)]
    fn eat_lit_or_id(&mut self) -> Result<Token<'a>, Error> {
        let start = self.offset;
        let quote = self.eat().unwrap();
        debug_assert!(quote == b'\'' || quote == b'"' || quote == b'`');
        let tt = if quote == b'\'' {
            TokenType::TK_STRING
        } else {
            TokenType::TK_ID
        };

        loop {
            self.eat_while(|b| b.is_some() && b.unwrap() != quote);
            match self.peek() {
                Some(b) if b == quote => {
                    self.eat_and_assert(|b| b == quote);
                    match self.peek() {
                        Some(b) if b == quote => {
                            self.eat_and_assert(|b| b == quote);
                            continue;
                        }
                        _ => break,
                    }
                }
                None => return Err(Error::UnterminatedLiteral(self.offset, None)),
                _ => unreachable!(),
            };
        }

        Ok(Token {
            value: &self.input[start..self.offset],
            token_type: Some(tt),
        })
    }

    #[inline(always)]
    fn eat_dot_or_frac(&mut self) -> Result<Token<'a>, Error> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'.');

        match self.peek() {
            Some(b) if b.is_ascii_digit() => {
                self.eat_while_number_digit()?;
                match self.peek() {
                    Some(b'e') | Some(b'E') => {
                        _ = self.eat_expo()?;
                        Ok(Token {
                            value: &self.input[start..self.offset],
                            token_type: Some(TokenType::TK_FLOAT),
                        })
                    }
                    Some(b) if is_identifier_start(b) => {
                        Err(Error::BadFractionalPart(self.offset, None, unsafe {
                            String::from_utf8_unchecked(self.input[start..self.offset + 1].to_vec())
                        }))
                    }
                    _ => Ok(Token {
                        value: &self.input[start..self.offset],
                        token_type: Some(TokenType::TK_FLOAT),
                    }),
                }
            }
            _ => Ok(Token {
                value: &self.input[start..self.offset],
                token_type: Some(TokenType::TK_DOT),
            }),
        }
    }

    #[inline(always)]
    fn eat_expo(&mut self) -> Result<Token<'a>, Error> {
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
            return Err(Error::BadExponentPart(self.offset, None, unsafe {
                String::from_utf8_unchecked(self.input[start..self.offset].to_vec())
            }));
        }

        if self.peek().is_some() && is_identifier_start(self.peek().unwrap()) {
            return Err(Error::BadExponentPart(self.offset, None, unsafe {
                String::from_utf8_unchecked(self.input[start..self.offset + 1].to_vec())
            }));
        }

        Ok(Token {
            value: &self.input[start..self.offset],
            token_type: Some(TokenType::TK_FLOAT), // This is a number
        })
    }

    #[inline(always)]
    fn eat_number(&mut self) -> Result<Token<'a>, Error> {
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
                        return Err(Error::MalformedHexInteger(
                            self.offset,
                            None,
                            Some("Did you forget to add digits after '0x' or '0X'?"), // Help Message
                        ));
                    }

                    if self.peek().is_some() && is_identifier_start(self.peek().unwrap()) {
                        return Err(Error::BadNumber(self.offset, None, unsafe {
                            String::from_utf8_unchecked(self.input[start..self.offset + 1].to_vec())
                        }));
                    }

                    return Ok(Token {
                        value: &self.input[start..self.offset],
                        token_type: Some(TokenType::TK_INTEGER),
                    });
                }
                _ => {}
            }
        }

        self.eat_while_number_digit()?;
        match self.peek() {
            Some(b'.') => {
                self.eat_dot_or_frac()?;
                Ok(Token {
                    value: &self.input[start..self.offset],
                    token_type: Some(TokenType::TK_FLOAT),
                })
            }
            Some(b'e') | Some(b'E') => {
                self.eat_expo()?;
                Ok(Token {
                    value: &self.input[start..self.offset],
                    token_type: Some(TokenType::TK_FLOAT),
                })
            }
            Some(b) if is_identifier_start(b) => Err(Error::BadNumber(self.offset, None, unsafe {
                String::from_utf8_unchecked(self.input[start..self.offset + 1].to_vec())
            })),
            _ => Ok(Token {
                value: &self.input[start..self.offset],
                token_type: Some(TokenType::TK_INTEGER),
            }),
        }
    }

    #[inline(always)]
    fn eat_bracket(&mut self) -> Result<Token<'a>, Error> {
        let start = self.offset;
        self.eat_and_assert(|b| b == b'[');
        self.eat_while(|b| b.is_some() && b.unwrap() != b']');
        match self.peek() {
            Some(b']') => {
                self.eat_and_assert(|b| b == b']');
                Ok(Token {
                    value: &self.input[start..self.offset],
                    token_type: Some(TokenType::TK_ID),
                })
            }
            None => Err(Error::UnterminatedBracket(self.offset, None)),
            _ => unreachable!(), // We should not reach here
        }
    }

    #[inline(always)]
    fn eat_var(&mut self) -> Result<Token<'a>, Error> {
        let start = self.offset;
        let tok = self.eat().unwrap();
        debug_assert!(tok == b'?' || tok == b'$' || tok == b'@' || tok == b'#' || tok == b':');

        match tok {
            b'?' => {
                let start_digit = self.offset;
                self.eat_while(|b| b.is_some() && b.unwrap().is_ascii_digit());

                // empty variable name
                if start_digit == self.offset {
                    return Err(Error::BadVariableName(self.offset, None));
                }

                Ok(Token {
                    value: &self.input[start + 1..self.offset], // do not include '? in the value
                    token_type: Some(TokenType::TK_VARIABLE),
                })
            }
            _ => {
                let start_id = self.offset;
                self.eat_while(|b| b.is_some() && is_identifier_continue(b.unwrap()));

                // empty variable name
                if start_id == self.offset {
                    return Err(Error::BadVariableName(self.offset, None));
                }

                Ok(Token {
                    value: &self.input[start..self.offset],
                    token_type: Some(TokenType::TK_VARIABLE),
                })
            }
        }
    }

    #[inline(always)]
    fn eat_blob_or_id(&mut self) -> Result<Token<'a>, Error> {
        let start = self.offset;
        let start_char = self.eat().unwrap();
        debug_assert!(is_identifier_start(start_char));

        match start_char {
            b'x' | b'X' if self.peek() == Some(b'\'') => {
                self.eat_and_assert(|b| b == b'\'');
                let start_hex = self.offset;
                self.eat_while(|b| b.is_some() && b.unwrap().is_ascii_hexdigit());

                match self.peek() {
                    Some(b'\'') => {
                        let end_hex = self.offset;
                        debug_assert!(end_hex >= start_hex);
                        self.eat_and_assert(|b| b == b'\'');

                        if (end_hex - start_hex) % 2 != 0 {
                            return Err(Error::UnrecognizedToken(self.offset, None));
                        }

                        Ok(Token {
                            value: &self.input[start + 2..self.offset - 1], // do not include 'x' or 'X' and the last '
                            token_type: Some(TokenType::TK_BLOB),
                        })
                    }
                    _ => Err(Error::UnterminatedLiteral(self.offset, None)),
                }
            }
            _ => {
                self.eat_while(|b| b.is_some() && is_identifier_continue(b.unwrap()));
                let result = &self.input[start..self.offset];
                Ok(Token {
                    value: result,
                    token_type: Some(keyword_token(result).unwrap_or(TokenType::TK_ID)),
                })
            }
        }
    }

    #[inline(always)]
    fn eat_unrecognized(&mut self) -> Token<'a> {
        let start = self.offset;
        self.eat_while(|b| b.is_some() && !b.unwrap().is_ascii_whitespace());
        Token {
            value: &self.input[start..self.offset],
            token_type: Some(TokenType::TK_ILLEGAL),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_lexer_one_tok() {
        let test_cases = vec![
            (
                b"    ".as_slice(),
                Token {
                    value: b"    ".as_slice(),
                    token_type: None,
                },
            ),
            (
                b"-- This is a comment\n".as_slice(),
                Token {
                    value: b"-- This is a comment\n".as_slice(),
                    token_type: None, // This is a comment
                },
            ),
            (
                b"-".as_slice(),
                Token {
                    value: b"-".as_slice(),
                    token_type: Some(TokenType::TK_MINUS),
                },
            ),
            (
                b"->".as_slice(),
                Token {
                    value: b"->".as_slice(),
                    token_type: Some(TokenType::TK_RP),
                },
            ),
            (
                b"->>".as_slice(),
                Token {
                    value: b"->>".as_slice(),
                    token_type: Some(TokenType::TK_RP),
                },
            ),
            (
                b"(".as_slice(),
                Token {
                    value: b"(".as_slice(),
                    token_type: Some(TokenType::TK_LP),
                },
            ),
            (
                b")".as_slice(),
                Token {
                    value: b")".as_slice(),
                    token_type: Some(TokenType::TK_RP),
                },
            ),
            (
                b";".as_slice(),
                Token {
                    value: b";".as_slice(),
                    token_type: Some(TokenType::TK_SEMI),
                },
            ),
            (
                b"+".as_slice(),
                Token {
                    value: b"+".as_slice(),
                    token_type: Some(TokenType::TK_PLUS),
                },
            ),
            (
                b"*".as_slice(),
                Token {
                    value: b"*".as_slice(),
                    token_type: Some(TokenType::TK_STAR),
                },
            ),
            (
                b"/".as_slice(),
                Token {
                    value: b"/".as_slice(),
                    token_type: Some(TokenType::TK_SLASH),
                },
            ),
            (
                b"/* This is a block comment */".as_slice(),
                Token {
                    value: b"/* This is a block comment */".as_slice(),
                    token_type: None, // This is a comment
                },
            ),
            (
                b"/* This is a\n\n block comment */".as_slice(),
                Token {
                    value: b"/* This is a\n\n block comment */".as_slice(),
                    token_type: None, // This is a comment
                },
            ),
            (
                b"/* This is a** block* comment */".as_slice(),
                Token {
                    value: b"/* This is a** block* comment */".as_slice(),
                    token_type: None, // This is a comment
                },
            ),
            (
                b"=".as_slice(),
                Token {
                    value: b"=".as_slice(),
                    token_type: Some(TokenType::TK_EQ),
                },
            ),
            (
                b"==".as_slice(),
                Token {
                    value: b"==".as_slice(),
                    token_type: Some(TokenType::TK_EQ),
                },
            ),
            (
                b"<".as_slice(),
                Token {
                    value: b"<".as_slice(),
                    token_type: Some(TokenType::TK_LT),
                },
            ),
            (
                b"<>".as_slice(),
                Token {
                    value: b"<>".as_slice(),
                    token_type: Some(TokenType::TK_NE),
                },
            ),
            (
                b"<=".as_slice(),
                Token {
                    value: b"<=".as_slice(),
                    token_type: Some(TokenType::TK_LE),
                },
            ),
            (
                b"<<".as_slice(),
                Token {
                    value: b"<<".as_slice(),
                    token_type: Some(TokenType::TK_LSHIFT),
                },
            ),
            (
                b">".as_slice(),
                Token {
                    value: b">".as_slice(),
                    token_type: Some(TokenType::TK_GT),
                },
            ),
            (
                b">=".as_slice(),
                Token {
                    value: b">=".as_slice(),
                    token_type: Some(TokenType::TK_GE),
                },
            ),
            (
                b">>".as_slice(),
                Token {
                    value: b">>".as_slice(),
                    token_type: Some(TokenType::TK_RSHIFT),
                },
            ),
            (
                b"!=".as_slice(),
                Token {
                    value: b"!=".as_slice(),
                    token_type: Some(TokenType::TK_NE),
                },
            ),
            (
                b"|".as_slice(),
                Token {
                    value: b"|".as_slice(),
                    token_type: Some(TokenType::TK_BITOR),
                },
            ),
            (
                b"||".as_slice(),
                Token {
                    value: b"||".as_slice(),
                    token_type: Some(TokenType::TK_CONCAT),
                },
            ),
            (
                b",".as_slice(),
                Token {
                    value: b",".as_slice(),
                    token_type: Some(TokenType::TK_COMMA),
                },
            ),
            (
                b"&".as_slice(),
                Token {
                    value: b"&".as_slice(),
                    token_type: Some(TokenType::TK_BITAND),
                },
            ),
            (
                b"~".as_slice(),
                Token {
                    value: b"~".as_slice(),
                    token_type: Some(TokenType::TK_BITNOT),
                },
            ),
            (
                b"'string'".as_slice(),
                Token {
                    value: b"'string'".as_slice(),
                    token_type: Some(TokenType::TK_STRING),
                },
            ),
            (
                b"`identifier`".as_slice(),
                Token {
                    value: b"`identifier`".as_slice(),
                    token_type: Some(TokenType::TK_ID),
                },
            ),
            (
                b"\"quoted string\"".as_slice(),
                Token {
                    value: b"\"quoted string\"".as_slice(),
                    token_type: Some(TokenType::TK_ID),
                },
            ),
            (
                b"\"\"\"triple \"\"quoted string\"\"\"".as_slice(),
                Token {
                    value: b"\"\"\"triple \"\"quoted string\"\"\"".as_slice(),
                    token_type: Some(TokenType::TK_ID),
                },
            ),
            (
                b"```triple ``quoted string```".as_slice(),
                Token {
                    value: b"```triple ``quoted string```".as_slice(),
                    token_type: Some(TokenType::TK_ID),
                },
            ),
            (
                b"'''triple ''quoted string'''".as_slice(),
                Token {
                    value: b"'''triple ''quoted string'''".as_slice(),
                    token_type: Some(TokenType::TK_STRING),
                },
            ),
            (
                b".".as_slice(),
                Token {
                    value: b".".as_slice(),
                    token_type: Some(TokenType::TK_DOT),
                },
            ),
            (
                b".123".as_slice(),
                Token {
                    value: b".123".as_slice(),
                    token_type: Some(TokenType::TK_FLOAT),
                },
            ),
            (
                b".456".as_slice(),
                Token {
                    value: b".456".as_slice(),
                    token_type: Some(TokenType::TK_FLOAT),
                },
            ),
            (
                b".456e789".as_slice(),
                Token {
                    value: b".456e789".as_slice(),
                    token_type: Some(TokenType::TK_FLOAT),
                },
            ),
            (
                b".456E-789".as_slice(),
                Token {
                    value: b".456E-789".as_slice(),
                    token_type: Some(TokenType::TK_FLOAT),
                },
            ),
            (
                b"123".as_slice(),
                Token {
                    value: b"123".as_slice(),
                    token_type: Some(TokenType::TK_INTEGER),
                },
            ),
            (
                b"123.456".as_slice(),
                Token {
                    value: b"123.456".as_slice(),
                    token_type: Some(TokenType::TK_FLOAT),
                },
            ),
            (
                b"123e456".as_slice(),
                Token {
                    value: b"123e456".as_slice(),
                    token_type: Some(TokenType::TK_FLOAT),
                },
            ),
            (
                b"123E-456".as_slice(),
                Token {
                    value: b"123E-456".as_slice(),
                    token_type: Some(TokenType::TK_FLOAT),
                },
            ),
            (
                b"0x1A3F".as_slice(),
                Token {
                    value: b"0x1A3F".as_slice(),
                    token_type: Some(TokenType::TK_INTEGER),
                },
            ),
            (
                b"0x1A3F_5678".as_slice(),
                Token {
                    value: b"0x1A3F_5678".as_slice(),
                    token_type: Some(TokenType::TK_INTEGER),
                },
            ),
            (
                b"0x1A3F_5678e9".as_slice(),
                Token {
                    value: b"0x1A3F_5678e9".as_slice(),
                    token_type: Some(TokenType::TK_INTEGER),
                },
            ),
            (
                b"[identifier]".as_slice(),
                Token {
                    value: b"[identifier]".as_slice(),
                    token_type: Some(TokenType::TK_ID),
                },
            ),
            (
                b"?123".as_slice(),
                Token {
                    value: b"123".as_slice(), // '?' is not included in the value
                    token_type: Some(TokenType::TK_VARIABLE),
                },
            ),
            (
                b"$var_name".as_slice(),
                Token {
                    value: b"$var_name".as_slice(),
                    token_type: Some(TokenType::TK_VARIABLE),
                },
            ),
            (
                b"@param".as_slice(),
                Token {
                    value: b"@param".as_slice(),
                    token_type: Some(TokenType::TK_VARIABLE),
                },
            ),
            (
                b"#comment".as_slice(),
                Token {
                    value: b"#comment".as_slice(),
                    token_type: Some(TokenType::TK_VARIABLE),
                },
            ),
            (
                b":named_param".as_slice(),
                Token {
                    value: b":named_param".as_slice(),
                    token_type: Some(TokenType::TK_VARIABLE),
                },
            ),
            (
                b"x'1234567890abcdef'".as_slice(),
                Token {
                    value: b"1234567890abcdef".as_slice(), // 'x' is not included in the value
                    token_type: Some(TokenType::TK_BLOB),
                },
            ),
            (
                b"X'1234567890abcdef'".as_slice(),
                Token {
                    value: b"1234567890abcdef".as_slice(), // 'X' is not included in the value
                    token_type: Some(TokenType::TK_BLOB),
                },
            ),
            (
                b"x''".as_slice(),
                Token {
                    value: b"".as_slice(), // 'x' is not included in the value
                    token_type: Some(TokenType::TK_BLOB),
                },
            ),
            (
                b"X''".as_slice(),
                Token {
                    value: b"".as_slice(), // 'X' is not included in the value
                    token_type: Some(TokenType::TK_BLOB),
                },
            ),
            (
                b"wHeRe".as_slice(),
                Token {
                    value: b"wHeRe".as_slice(), // 'X' is not included in the value
                    token_type: Some(TokenType::TK_WHERE),
                },
            ),
            (
                b"wHeRe123".as_slice(),
                Token {
                    value: b"wHeRe123".as_slice(), // 'X' is not included in the value
                    token_type: Some(TokenType::TK_ID),
                },
            ),
            (
                b"wHeRe_123".as_slice(),
                Token {
                    value: b"wHeRe_123".as_slice(), // 'X' is not included in the value
                    token_type: Some(TokenType::TK_ID),
                },
            ),
        ];

        for (input, expected) in test_cases {
            let mut lexer = Lexer::new(input);
            let token = lexer.next().unwrap().unwrap();
            let expect_value = unsafe { String::from_utf8_unchecked(expected.value.to_vec()) };
            let got_value = unsafe { String::from_utf8_unchecked(token.value.to_vec()) };
            print!(
                "Input: {:?}, Expected: {:?}, Got: {:?}\n",
                input, expect_value, got_value
            );
            assert_eq!(got_value, expect_value);
            assert_eq!(token.token_type, expected.token_type);
        }
    }

    #[test]
    fn test_keyword_token() {
        let values = HashMap::from([
            ("ABORT", TokenType::TK_ABORT),
            ("ACTION", TokenType::TK_ACTION),
            ("ADD", TokenType::TK_ADD),
            ("AFTER", TokenType::TK_AFTER),
            ("ALL", TokenType::TK_ALL),
            ("ALTER", TokenType::TK_ALTER),
            ("ALWAYS", TokenType::TK_ALWAYS),
            ("ANALYZE", TokenType::TK_ANALYZE),
            ("AND", TokenType::TK_AND),
            ("AS", TokenType::TK_AS),
            ("ASC", TokenType::TK_ASC),
            ("ATTACH", TokenType::TK_ATTACH),
            ("AUTOINCREMENT", TokenType::TK_AUTOINCR),
            ("BEFORE", TokenType::TK_BEFORE),
            ("BEGIN", TokenType::TK_BEGIN),
            ("BETWEEN", TokenType::TK_BETWEEN),
            ("BY", TokenType::TK_BY),
            ("CASCADE", TokenType::TK_CASCADE),
            ("CASE", TokenType::TK_CASE),
            ("CAST", TokenType::TK_CAST),
            ("CHECK", TokenType::TK_CHECK),
            ("COLLATE", TokenType::TK_COLLATE),
            ("COLUMN", TokenType::TK_COLUMNKW),
            ("COMMIT", TokenType::TK_COMMIT),
            ("CONFLICT", TokenType::TK_CONFLICT),
            ("CONSTRAINT", TokenType::TK_CONSTRAINT),
            ("CREATE", TokenType::TK_CREATE),
            ("CROSS", TokenType::TK_JOIN_KW),
            ("CURRENT", TokenType::TK_CURRENT),
            ("CURRENT_DATE", TokenType::TK_CTIME_KW),
            ("CURRENT_TIME", TokenType::TK_CTIME_KW),
            ("CURRENT_TIMESTAMP", TokenType::TK_CTIME_KW),
            ("DATABASE", TokenType::TK_DATABASE),
            ("DEFAULT", TokenType::TK_DEFAULT),
            ("DEFERRABLE", TokenType::TK_DEFERRABLE),
            ("DEFERRED", TokenType::TK_DEFERRED),
            ("DELETE", TokenType::TK_DELETE),
            ("DESC", TokenType::TK_DESC),
            ("DETACH", TokenType::TK_DETACH),
            ("DISTINCT", TokenType::TK_DISTINCT),
            ("DO", TokenType::TK_DO),
            ("DROP", TokenType::TK_DROP),
            ("EACH", TokenType::TK_EACH),
            ("ELSE", TokenType::TK_ELSE),
            ("END", TokenType::TK_END),
            ("ESCAPE", TokenType::TK_ESCAPE),
            ("EXCEPT", TokenType::TK_EXCEPT),
            ("EXCLUDE", TokenType::TK_EXCLUDE),
            ("EXCLUSIVE", TokenType::TK_EXCLUSIVE),
            ("EXISTS", TokenType::TK_EXISTS),
            ("EXPLAIN", TokenType::TK_EXPLAIN),
            ("FAIL", TokenType::TK_FAIL),
            ("FILTER", TokenType::TK_FILTER),
            ("FIRST", TokenType::TK_FIRST),
            ("FOLLOWING", TokenType::TK_FOLLOWING),
            ("FOR", TokenType::TK_FOR),
            ("FOREIGN", TokenType::TK_FOREIGN),
            ("FROM", TokenType::TK_FROM),
            ("FULL", TokenType::TK_JOIN_KW),
            ("GENERATED", TokenType::TK_GENERATED),
            ("GLOB", TokenType::TK_LIKE_KW),
            ("GROUP", TokenType::TK_GROUP),
            ("GROUPS", TokenType::TK_GROUPS),
            ("HAVING", TokenType::TK_HAVING),
            ("IF", TokenType::TK_IF),
            ("IGNORE", TokenType::TK_IGNORE),
            ("IMMEDIATE", TokenType::TK_IMMEDIATE),
            ("IN", TokenType::TK_IN),
            ("INDEX", TokenType::TK_INDEX),
            ("INDEXED", TokenType::TK_INDEXED),
            ("INITIALLY", TokenType::TK_INITIALLY),
            ("INNER", TokenType::TK_JOIN_KW),
            ("INSERT", TokenType::TK_INSERT),
            ("INSTEAD", TokenType::TK_INSTEAD),
            ("INTERSECT", TokenType::TK_INTERSECT),
            ("INTO", TokenType::TK_INTO),
            ("IS", TokenType::TK_IS),
            ("ISNULL", TokenType::TK_ISNULL),
            ("JOIN", TokenType::TK_JOIN),
            ("KEY", TokenType::TK_KEY),
            ("LAST", TokenType::TK_LAST),
            ("LEFT", TokenType::TK_JOIN_KW),
            ("LIKE", TokenType::TK_LIKE_KW),
            ("LIMIT", TokenType::TK_LIMIT),
            ("MATCH", TokenType::TK_MATCH),
            ("MATERIALIZED", TokenType::TK_MATERIALIZED),
            ("NATURAL", TokenType::TK_JOIN_KW),
            ("NO", TokenType::TK_NO),
            ("NOT", TokenType::TK_NOT),
            ("NOTHING", TokenType::TK_NOTHING),
            ("NOTNULL", TokenType::TK_NOTNULL),
            ("NULL", TokenType::TK_NULL),
            ("NULLS", TokenType::TK_NULLS),
            ("OF", TokenType::TK_OF),
            ("OFFSET", TokenType::TK_OFFSET),
            ("ON", TokenType::TK_ON),
            ("OR", TokenType::TK_OR),
            ("ORDER", TokenType::TK_ORDER),
            ("OTHERS", TokenType::TK_OTHERS),
            ("OUTER", TokenType::TK_JOIN_KW),
            ("OVER", TokenType::TK_OVER),
            ("PARTITION", TokenType::TK_PARTITION),
            ("PLAN", TokenType::TK_PLAN),
            ("PRAGMA", TokenType::TK_PRAGMA),
            ("PRECEDING", TokenType::TK_PRECEDING),
            ("PRIMARY", TokenType::TK_PRIMARY),
            ("QUERY", TokenType::TK_QUERY),
            ("RAISE", TokenType::TK_RAISE),
            ("RANGE", TokenType::TK_RANGE),
            ("RECURSIVE", TokenType::TK_RECURSIVE),
            ("REFERENCES", TokenType::TK_REFERENCES),
            ("REGEXP", TokenType::TK_LIKE_KW),
            ("REINDEX", TokenType::TK_REINDEX),
            ("RELEASE", TokenType::TK_RELEASE),
            ("RENAME", TokenType::TK_RENAME),
            ("REPLACE", TokenType::TK_REPLACE),
            ("RETURNING", TokenType::TK_RETURNING),
            ("RESTRICT", TokenType::TK_RESTRICT),
            ("RIGHT", TokenType::TK_JOIN_KW),
            ("ROLLBACK", TokenType::TK_ROLLBACK),
            ("ROW", TokenType::TK_ROW),
            ("ROWS", TokenType::TK_ROWS),
            ("SAVEPOINT", TokenType::TK_SAVEPOINT),
            ("SELECT", TokenType::TK_SELECT),
            ("SET", TokenType::TK_SET),
            ("TABLE", TokenType::TK_TABLE),
            ("TEMP", TokenType::TK_TEMP),
            ("TEMPORARY", TokenType::TK_TEMP),
            ("THEN", TokenType::TK_THEN),
            ("TIES", TokenType::TK_TIES),
            ("TO", TokenType::TK_TO),
            ("TRANSACTION", TokenType::TK_TRANSACTION),
            ("TRIGGER", TokenType::TK_TRIGGER),
            ("UNBOUNDED", TokenType::TK_UNBOUNDED),
            ("UNION", TokenType::TK_UNION),
            ("UNIQUE", TokenType::TK_UNIQUE),
            ("UPDATE", TokenType::TK_UPDATE),
            ("USING", TokenType::TK_USING),
            ("VACUUM", TokenType::TK_VACUUM),
            ("VALUES", TokenType::TK_VALUES),
            ("VIEW", TokenType::TK_VIEW),
            ("VIRTUAL", TokenType::TK_VIRTUAL),
            ("WHEN", TokenType::TK_WHEN),
            ("WHERE", TokenType::TK_WHERE),
            ("WINDOW", TokenType::TK_WINDOW),
            ("WITH", TokenType::TK_WITH),
            ("WITHOUT", TokenType::TK_WITHOUT),
        ]);

        for (key, value) in &values {
            assert!(keyword_token(key.as_bytes()).unwrap() == *value);
            assert!(
                keyword_token(key.as_bytes().to_ascii_lowercase().as_slice()).unwrap() == *value
            );
        }

        assert!(keyword_token(b"").is_none());
        assert!(keyword_token(b"wrong").is_none());
        assert!(keyword_token(b"super wrong").is_none());
        assert!(keyword_token(b"super_wrong").is_none());
        assert!(keyword_token(b"aae26e78-3ba7-4627-8f8f-02623302495a").is_none());
        assert!(keyword_token("Crème Brulée".as_bytes()).is_none());
        assert!(keyword_token("fróm".as_bytes()).is_none());
    }

    #[test]
    fn test_lexer_multi_tok() {
        let test_cases = vec![
            (
                b"    SELECT 1".as_slice(),
                vec![
                    Token {
                        value: b"    ".as_slice(),
                        token_type: None,
                    },
                    Token {
                        value: b"SELECT".as_slice(),
                        token_type: Some(TokenType::TK_SELECT),
                    },
                    Token {
                        value: b" ".as_slice(),
                        token_type: None,
                    },
                    Token {
                        value: b"1".as_slice(),
                        token_type: Some(TokenType::TK_INTEGER),
                    },
                ],
            ),
            (
                b"INSERT INTO users VALUES (1,2,3)".as_slice(),
                vec![
                    Token {
                        value: b"INSERT".as_slice(),
                        token_type: Some(TokenType::TK_INSERT),
                    },
                    Token {
                        value: b" ".as_slice(),
                        token_type: None,
                    },
                    Token {
                        value: b"INTO".as_slice(),
                        token_type: Some(TokenType::TK_INTO),
                    },
                    Token {
                        value: b" ".as_slice(),
                        token_type: None,
                    },
                    Token {
                        value: b"users".as_slice(),
                        token_type: Some(TokenType::TK_ID),
                    },
                    Token {
                        value: b" ".as_slice(),
                        token_type: None,
                    },
                    Token {
                        value: b"VALUES".as_slice(),
                        token_type: Some(TokenType::TK_VALUES),
                    },
                    Token {
                        value: b" ".as_slice(),
                        token_type: None,
                    },
                    Token {
                        value: b"(".as_slice(),
                        token_type: Some(TokenType::TK_LP),
                    },
                    Token {
                        value: b"1".as_slice(),
                        token_type: Some(TokenType::TK_INTEGER),
                    },
                    Token {
                        value: b",".as_slice(),
                        token_type: Some(TokenType::TK_COMMA),
                    },
                    Token {
                        value: b"2".as_slice(),
                        token_type: Some(TokenType::TK_INTEGER),
                    },
                    Token {
                        value: b",".as_slice(),
                        token_type: Some(TokenType::TK_COMMA),
                    },
                    Token {
                        value: b"3".as_slice(),
                        token_type: Some(TokenType::TK_INTEGER),
                    },
                    Token {
                        value: b")".as_slice(),
                        token_type: Some(TokenType::TK_RP),
                    },
                ],
            ),
        ];

        for (input, expected_tokens) in test_cases {
            let lexer = Lexer::new(input);
            let mut tokens = Vec::new();

            for token in lexer {
                tokens.push(token.unwrap());
            }

            assert_eq!(tokens.len(), expected_tokens.len());

            for (i, token) in tokens.iter().enumerate() {
                let expect_value =
                    unsafe { String::from_utf8_unchecked(expected_tokens[i].value.to_vec()) };
                let got_value = unsafe { String::from_utf8_unchecked(token.value.to_vec()) };
                assert_eq!(got_value, expect_value);
                assert_eq!(token.token_type, expected_tokens[i].token_type);
            }
        }
    }
}
