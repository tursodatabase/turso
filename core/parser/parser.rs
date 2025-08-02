use crate::parser::ast::{Cmd, Name, Stmt, TransactionType};
use crate::parser::error::Error;
use crate::parser::lexer::{Lexer, Token};
use crate::parser::token::TokenType;

fn from_bytes(bytes: &[u8]) -> String {
    unsafe { str::from_utf8_unchecked(bytes).to_owned() }
}

pub struct Parser<'a> {
    lexer: Lexer<'a>,
    /// The current token being processed
    peek_mark: Option<Token<'a>>,
}

impl<'a> Iterator for Parser<'a> {
    type Item = Result<Cmd, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // consumes prefix SEMI
        while let Some(Ok(token)) = self.peek() {
            if token.token_type == Some(TokenType::TK_SEMI) {
                self.eat_assert(&[TokenType::TK_SEMI]);
            } else {
                break;
            }
        }

        let result = match self.peek() {
            None => None, // EOF
            Some(Ok(token)) => match token.token_type {
                Some(TokenType::TK_EXPLAIN) => {
                    self.eat_assert(&[TokenType::TK_EXPLAIN]);

                    let mut is_query_plan = false;
                    match self.peek_no_eof() {
                        Ok(tok) if tok.token_type == Some(TokenType::TK_QUERY) => {
                            self.eat_assert(&[TokenType::TK_QUERY]);

                            if let Err(err) = self.eat_expect(&[TokenType::TK_PLAN]) {
                                return Some(Err(err));
                            }

                            is_query_plan = true;
                        }
                        Err(err) => return Some(Err(err)),
                        _ => {}
                    }

                    let stmt = self.parse_stmt();
                    if let Err(err) = stmt {
                        return Some(Err(err));
                    }

                    if is_query_plan {
                        Some(Ok(Cmd::ExplainQueryPlan(stmt.unwrap())))
                    } else {
                        Some(Ok(Cmd::Explain(stmt.unwrap())))
                    }
                }
                _ => {
                    let stmt = self.parse_stmt();
                    if let Err(err) = stmt {
                        return Some(Err(err));
                    }

                    Some(Ok(Cmd::Stmt(stmt.unwrap())))
                }
            },
            Some(Err(err)) => return Some(Err(err)),
        };

        // consumes suffix SEMI
        let mut found_semi = false;
        loop {
            match self.peek_ignore_eof() {
                Ok(None) => break,
                Ok(Some(token)) if token.token_type == Some(TokenType::TK_SEMI) => {
                    found_semi = true;
                    self.eat_assert(&[TokenType::TK_SEMI]);
                }
                Ok(Some(token)) => {
                    if !found_semi {
                        return Some(Err(Error::ParseUnexpectedToken {
                            expected: &[TokenType::TK_SEMI],
                            got: token.token_type.unwrap(),
                        }));
                    }

                    break;
                }
                Err(err) => return Some(Err(err)),
            }
        }

        result
    }
}

impl<'a> Parser<'a> {
    #[inline(always)]
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            lexer: Lexer::new(input),
            peek_mark: None,
        }
    }

    /// Get the next token from the lexer
    #[inline(always)]
    fn eat(&mut self) -> Option<Result<Token<'a>, Error>> {
        if let Some(token) = self.peek_mark.take() {
            return Some(Ok(token));
        }

        loop {
            let tok = self.lexer.next();
            if let Some(Ok(ref token)) = tok {
                if token.token_type.is_none() {
                    continue; // white space or comment
                }
            }

            return tok;
        }
    }

    #[inline(always)]
    fn eat_no_eof(&mut self) -> Result<Token<'a>, Error> {
        match self.eat() {
            None => Err(Error::ParseUnexpectedEOF),
            Some(Ok(token)) => Ok(token),
            Some(Err(err)) => Err(err),
        }
    }

    #[inline(always)]
    fn eat_expect(&mut self, expected: &'static [TokenType]) -> Result<Token<'a>, Error> {
        self.peek_expect(expected)?;
        Ok(self.eat_assert(expected))
    }

    #[inline(always)]
    fn eat_assert(&mut self, expected: &'static [TokenType]) -> Token<'a> {
        let token = self.eat_no_eof().unwrap();

        #[cfg(debug_assertions)]
        {
            for expected in expected {
                if token.token_type == Some(*expected) {
                    return token;
                }
            }

            panic!(
                "Expected token {:?}, got {:?}",
                expected,
                token.token_type.unwrap()
            );
        }

        #[cfg(not(debug_assertions))]
        token // in release mode, we assume the caller has checked the token type
    }

    /// Peek at the next token without consuming it
    #[inline(always)]
    fn peek(&mut self) -> Option<Result<Token<'a>, Error>> {
        if let Some(ref token) = self.peek_mark {
            return Some(Ok(token.clone()));
        }

        match self.eat() {
            None => None, // EOF
            Some(Ok(token)) => {
                self.peek_mark = Some(token.clone());
                Some(Ok(token))
            }
            Some(Err(err)) => Some(Err(err)),
        }
    }

    #[inline(always)]
    fn peek_no_eof(&mut self) -> Result<Token<'a>, Error> {
        match self.peek() {
            None => Err(Error::ParseUnexpectedEOF),
            Some(Ok(token)) => Ok(token),
            Some(Err(err)) => Err(err),
        }
    }

    #[inline(always)]
    fn peek_ignore_eof(&mut self) -> Result<Option<Token<'a>>, Error> {
        match self.peek() {
            None => Ok(None),
            Some(Ok(token)) => Ok(Some(token)),
            Some(Err(err)) => Err(err),
        }
    }

    #[inline(always)]
    fn peek_expect(&mut self, expected: &'static [TokenType]) -> Result<Token<'a>, Error> {
        let token = self.peek_no_eof()?;
        for expected in expected {
            if token.token_type == Some(*expected) {
                return Ok(token);
            }
        }

        Err(Error::ParseUnexpectedToken {
            expected: expected,
            got: token.token_type.unwrap(), // no whitespace or comment tokens here
        })
    }

    #[inline(always)]
    fn parse_stmt(&mut self) -> Result<Stmt, Error> {
        let tok = self.peek_expect(&[
            TokenType::TK_BEGIN,
            TokenType::TK_COMMIT,
            TokenType::TK_END,
            TokenType::TK_ROLLBACK,
            TokenType::TK_SAVEPOINT,
            TokenType::TK_RELEASE,
            // add more
        ])?;

        match tok.token_type.unwrap() {
            TokenType::TK_BEGIN => self.parse_begin(),
            TokenType::TK_COMMIT | TokenType::TK_END => self.parse_commit(),
            TokenType::TK_ROLLBACK => self.parse_rollback(),
            TokenType::TK_SAVEPOINT => self.parse_savepoint(),
            TokenType::TK_RELEASE => self.parse_release(),
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    fn peek_nm(&mut self) -> Result<Token<'a>, Error> {
        self.peek_expect(&[
            TokenType::TK_ID,
            TokenType::TK_STRING,
            TokenType::TK_INDEXED,
            TokenType::TK_JOIN_KW,
        ])
    }

    #[inline(always)]
    fn parse_nm(&mut self) -> Name {
        let tok = self.eat_assert(&[
            TokenType::TK_ID,
            TokenType::TK_STRING,
            TokenType::TK_INDEXED,
            TokenType::TK_JOIN_KW,
        ]);

        let first_char = tok.value[0]; // no need to check empty
        match first_char {
            b'[' | b'\'' | b'`' | b'"' => Name::Quoted(from_bytes(tok.value)),
            _ => Name::Ident(from_bytes(tok.value)),
        }
    }

    #[inline(always)]
    fn parse_transopt(&mut self) -> Result<Option<Name>, Error> {
        match self.peek_ignore_eof()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_TRANSACTION => {
                    self.eat_assert(&[TokenType::TK_TRANSACTION]);
                    if self.peek_nm().ok().is_some() {
                        Ok(Some(self.parse_nm()))
                    } else {
                        Ok(None)
                    }
                }
                _ => Ok(None),
            },
        }
    }

    #[inline(always)]
    fn parse_begin(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_BEGIN]);

        let transtype = match self.peek_ignore_eof()? {
            None => None,
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_DEFERRED => {
                    self.eat_assert(&[TokenType::TK_DEFERRED]);
                    Some(TransactionType::Deferred)
                }
                TokenType::TK_IMMEDIATE => {
                    self.eat_assert(&[TokenType::TK_IMMEDIATE]);
                    Some(TransactionType::Immediate)
                }
                TokenType::TK_EXCLUSIVE => {
                    self.eat_assert(&[TokenType::TK_EXCLUSIVE]);
                    Some(TransactionType::Exclusive)
                }
                _ => None,
            },
        };

        Ok(Stmt::Begin {
            typ: transtype,
            name: self.parse_transopt()?,
        })
    }

    #[inline(always)]
    fn parse_commit(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_COMMIT, TokenType::TK_END]);
        Ok(Stmt::Commit {
            name: self.parse_transopt()?,
        })
    }

    #[inline(always)]
    fn parse_rollback(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_ROLLBACK]);

        let tx_name = self.parse_transopt()?;

        let savepoint_name = match self.peek_ignore_eof()? {
            None => None,
            Some(tok) => {
                if tok.token_type == Some(TokenType::TK_TO) {
                    self.eat_assert(&[TokenType::TK_TO]);

                    if self.peek_no_eof()?.token_type == Some(TokenType::TK_SAVEPOINT) {
                        self.eat_assert(&[TokenType::TK_SAVEPOINT]);
                    }

                    self.peek_nm()?;
                    Some(self.parse_nm())
                } else {
                    None
                }
            }
        };

        Ok(Stmt::Rollback {
            tx_name,
            savepoint_name,
        })
    }

    #[inline(always)]
    fn parse_savepoint(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_SAVEPOINT]);
        self.peek_nm()?;
        Ok(Stmt::Savepoint {
            name: self.parse_nm(),
        })
    }

    #[inline(always)]
    fn parse_release(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_RELEASE]);

        if self.peek_no_eof()?.token_type == Some(TokenType::TK_SAVEPOINT) {
            self.eat_assert(&[TokenType::TK_SAVEPOINT]);
        }

        self.peek_nm()?;
        Ok(Stmt::Release {
            name: self.parse_nm(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser() {
        let test_cases = vec![
            // begin
            (
                b"BEGIN".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: None,
                    name: None,
                })],
            ),
            (
                b"EXPLAIN BEGIN".as_slice(),
                vec![Cmd::Explain(Stmt::Begin {
                    typ: None,
                    name: None,
                })],
            ),
            (
                b"EXPLAIN QUERY PLAN BEGIN".as_slice(),
                vec![Cmd::ExplainQueryPlan(Stmt::Begin {
                    typ: None,
                    name: None,
                })],
            ),
            (
                b"BEGIN TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: None,
                    name: None,
                })],
            ),
            (
                b"BEGIN DEFERRED TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Deferred),
                    name: None,
                })],
            ),
            (
                b"BEGIN IMMEDIATE TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Immediate),
                    name: None,
                })],
            ),
            (
                b"BEGIN EXCLUSIVE TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Exclusive),
                    name: None,
                })],
            ),
            (
                b"BEGIN DEFERRED TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Deferred),
                    name: Some(Name::Ident("my_transaction".to_string())),
                })],
            ),
            (
                b"BEGIN IMMEDIATE TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Immediate),
                    name: Some(Name::Ident("my_transaction".to_string())),
                })],
            ),
            (
                b"BEGIN EXCLUSIVE TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Exclusive),
                    name: Some(Name::Ident("my_transaction".to_string())),
                })],
            ),
            (
                b"BEGIN EXCLUSIVE TRANSACTION 'my_transaction'".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Exclusive),
                    name: Some(Name::Quoted("'my_transaction'".to_string())),
                })],
            ),
            (
                ";;;BEGIN;BEGIN;;;;;;BEGIN".as_bytes(),
                vec![
                    Cmd::Stmt(Stmt::Begin {
                        typ: None,
                        name: None,
                    }),
                    Cmd::Stmt(Stmt::Begin {
                        typ: None,
                        name: None,
                    }),
                    Cmd::Stmt(Stmt::Begin {
                        typ: None,
                        name: None,
                    }),
                ],
            ),
            // commit
            (
                b"COMMIT".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit { name: None })],
            ),
            (
                b"END".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit { name: None })],
            ),
            (
                b"COMMIT TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit { name: None })],
            ),
            (
                b"END TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit { name: None })],
            ),
            (
                b"COMMIT TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit {
                    name: Some(Name::Ident("my_transaction".to_string())),
                })],
            ),
            (
                b"END TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit {
                    name: Some(Name::Ident("my_transaction".to_string())),
                })],
            ),
            // Rollback
            (
                b"ROLLBACK".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: None,
                    savepoint_name: None,
                })],
            ),
            (
                b"ROLLBACK TO SAVEPOINT my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: None,
                    savepoint_name: Some(Name::Ident("my_savepoint".to_string())),
                })],
            ),
            (
                b"ROLLBACK TO my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: None,
                    savepoint_name: Some(Name::Ident("my_savepoint".to_string())),
                })],
            ),
            (
                b"ROLLBACK TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: Some(Name::Ident("my_transaction".to_string())),
                    savepoint_name: None,
                })],
            ),
            (
                b"ROLLBACK TRANSACTION my_transaction TO my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: Some(Name::Ident("my_transaction".to_string())),
                    savepoint_name: Some(Name::Ident("my_savepoint".to_string())),
                })],
            ),
            // savepoint
            (
                b"SAVEPOINT my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Savepoint {
                    name: Name::Ident("my_savepoint".to_string()),
                })],
            ),
            (
                b"SAVEPOINT 'my_savepoint'".as_slice(),
                vec![Cmd::Stmt(Stmt::Savepoint {
                    name: Name::Quoted("'my_savepoint'".to_string()),
                })],
            ),
            // release
            (
                b"RELEASE my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Release {
                    name: Name::Ident("my_savepoint".to_string()),
                })],
            ),
            (
                b"RELEASE SAVEPOINT my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Release {
                    name: Name::Ident("my_savepoint".to_string()),
                })],
            ),
            (
                b"RELEASE SAVEPOINT 'my_savepoint'".as_slice(),
                vec![Cmd::Stmt(Stmt::Release {
                    name: Name::Quoted("'my_savepoint'".to_string()),
                })],
            ),
        ];

        for (input, expected) in test_cases {
            println!("Testing input: {:?}", from_bytes(input));
            let mut parser = Parser::new(input);
            let mut results = Vec::new();
            while let Some(cmd) = parser.next() {
                match cmd {
                    Ok(cmd) => results.push(cmd),
                    Err(err) => panic!("Parse error: {}", err),
                }
            }

            assert_eq!(results, expected, "Input: {:?}", input);
        }
    }
}
