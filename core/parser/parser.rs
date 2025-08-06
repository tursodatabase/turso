use crate::parser::ast::{
    As, Cmd, CommonTableExpr, CompoundOperator, CompoundSelect, CreateTableBody, Distinctness,
    Expr, FrameBound, FrameClause, FrameExclude, FrameMode, FromClause, FunctionTail, GroupBy,
    Indexed, IndexedColumn, JoinConstraint, JoinOperator, JoinType, JoinedSelectTable,
    LikeOperator, Limit, Literal, Materialized, Name, NullsOrder, OneSelect, Operator, Over,
    QualifiedName, ResultColumn, Select, SelectBody, SelectTable, SortOrder, SortedColumn, Stmt,
    TransactionType, Type, TypeSize, UnaryOperator, Window, WindowDef, With,
};
use crate::parser::error::Error;
use crate::parser::lexer::{Lexer, Token};
use crate::parser::token::TokenType;

#[inline(always)]
fn from_bytes_as_str(bytes: &[u8]) -> &str {
    unsafe { str::from_utf8_unchecked(bytes) }
}

#[inline(always)]
fn from_bytes(bytes: &[u8]) -> String {
    unsafe { str::from_utf8_unchecked(bytes).to_owned() }
}

#[inline(always)]
fn join_type_from_bytes(s: &[u8]) -> Result<JoinType, Error> {
    if b"CROSS".eq_ignore_ascii_case(s) {
        Ok(JoinType::INNER | JoinType::CROSS)
    } else if b"FULL".eq_ignore_ascii_case(s) {
        Ok(JoinType::LEFT | JoinType::RIGHT | JoinType::OUTER)
    } else if b"INNER".eq_ignore_ascii_case(s) {
        Ok(JoinType::INNER)
    } else if b"LEFT".eq_ignore_ascii_case(s) {
        Ok(JoinType::LEFT | JoinType::OUTER)
    } else if b"NATURAL".eq_ignore_ascii_case(s) {
        Ok(JoinType::NATURAL)
    } else if b"RIGHT".eq_ignore_ascii_case(s) {
        Ok(JoinType::RIGHT | JoinType::OUTER)
    } else if b"OUTER".eq_ignore_ascii_case(s) {
        Ok(JoinType::OUTER)
    } else {
        Err(Error::Custom(format!(
            "unsupported JOIN type: {:?}",
            str::from_utf8(s)
        )))
    }
}

#[inline(always)]
fn new_join_type(n0: &[u8], n1: Option<&[u8]>, n2: Option<&[u8]>) -> Result<JoinType, Error> {
    let mut jt = join_type_from_bytes(n0)?;

    if let Some(n1) = n1 {
        jt |= join_type_from_bytes(n1)?;
    }

    if let Some(n2) = n2 {
        jt |= join_type_from_bytes(n2)?;
    }

    if (jt & (JoinType::INNER | JoinType::OUTER)) == (JoinType::INNER | JoinType::OUTER)
        || (jt & (JoinType::OUTER | JoinType::LEFT | JoinType::RIGHT)) == JoinType::OUTER
    {
        return Err(Error::Custom(format!(
            "unsupported JOIN type: {:?} {:?} {:?}",
            from_bytes_as_str(n0),
            from_bytes_as_str(n1.unwrap_or(&[])),
            from_bytes_as_str(n2.unwrap_or(&[])),
        )));
    }

    Ok(jt)
}

pub struct Parser<'a> {
    lexer: Lexer<'a>,

    /// The current token being processed
    current_token: Token<'a>,
    peekable: bool,
}

impl<'a> Iterator for Parser<'a> {
    type Item = Result<Cmd, Error>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match self.mark(|p| p.next_cmd()) {
            Ok(None) => None, // EOF
            Ok(Some(cmd)) => Some(Ok(cmd)),
            Err(err) => Some(Err(err)),
        }
    }
}

impl<'a> Parser<'a> {
    #[inline(always)]
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            lexer: Lexer::new(input),
            peekable: false,
            current_token: Token {
                value: b"",
                token_type: None,
            },
        }
    }

    // entrypoint of parsing
    #[inline(always)]
    fn next_cmd(&mut self) -> Result<Option<Cmd>, Error> {
        // consumes prefix SEMI
        while let Some(token) = self.peek()? {
            if token.token_type == Some(TokenType::TK_SEMI) {
                self.eat_assert(&[TokenType::TK_SEMI]);
            } else {
                break;
            }
        }

        let result = match self.peek()? {
            None => None, // EOF
            Some(token) => match token.token_type.unwrap() {
                TokenType::TK_EXPLAIN => {
                    self.eat_assert(&[TokenType::TK_EXPLAIN]);

                    let mut is_query_plan = false;
                    if self.peek_no_eof()?.token_type == Some(TokenType::TK_QUERY) {
                        self.eat_assert(&[TokenType::TK_QUERY]);
                        self.eat_expect(&[TokenType::TK_PLAN])?;
                        is_query_plan = true;
                    }

                    let stmt = self.parse_stmt()?;
                    if is_query_plan {
                        Some(Cmd::ExplainQueryPlan(stmt))
                    } else {
                        Some(Cmd::Explain(stmt))
                    }
                }
                _ => {
                    let stmt = self.parse_stmt()?;
                    Some(Cmd::Stmt(stmt))
                }
            },
        };

        let mut found_semi = false;
        loop {
            match self.peek()? {
                None => break,
                Some(token) if token.token_type == Some(TokenType::TK_SEMI) => {
                    found_semi = true;
                    self.eat_assert(&[TokenType::TK_SEMI]);
                }
                Some(token) => {
                    if !found_semi {
                        return Err(Error::ParseUnexpectedToken {
                            expected: &[TokenType::TK_SEMI],
                            got: token.token_type.unwrap(),
                        });
                    }

                    break;
                }
            }
        }

        Ok(result)
    }

    #[inline(always)]
    fn consume_lexer_without_whitespaces_or_comments(
        &mut self,
    ) -> Option<Result<Token<'a>, Error>> {
        debug_assert!(!self.peekable);
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
    fn next_token(&mut self) -> Result<Option<Token<'a>>, Error> {
        debug_assert!(!self.peekable);
        let mut next = self.consume_lexer_without_whitespaces_or_comments();

        fn get_token(tt: TokenType) -> TokenType {
            match tt {
                TokenType::TK_INDEX
                | TokenType::TK_STRING
                | TokenType::TK_JOIN_KW
                | TokenType::TK_WINDOW
                | TokenType::TK_OVER => TokenType::TK_ID,
                _ => tt.fallback_id_if_ok(),
            }
        }

        if let Some(Ok(ref mut tok)) = next {
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
            match tok.token_type.unwrap() {
                TokenType::TK_WINDOW => {
                    let can_be_window = self.try_parse(|p| {
                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => return Ok(false),
                            Some(tok) => match get_token(tok?.token_type.unwrap()) {
                                TokenType::TK_ID => {}
                                _ => return Ok(false),
                            },
                        }

                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => return Ok(false),
                            Some(tok) => match get_token(tok?.token_type.unwrap()) {
                                TokenType::TK_AS => Ok(true),
                                _ => Ok(false),
                            },
                        }
                    })?;

                    if !can_be_window {
                        tok.token_type = Some(TokenType::TK_ID);
                    }
                }
                TokenType::TK_OVER => {
                    let prev_tt = self.current_token.token_type.unwrap_or(TokenType::TK_EOF);
                    let can_be_over = {
                        if prev_tt == TokenType::TK_RP {
                            self.try_parse(|p| {
                                match p.consume_lexer_without_whitespaces_or_comments() {
                                    None => return Ok(false),
                                    Some(tok) => match get_token(tok?.token_type.unwrap()) {
                                        TokenType::TK_LP | TokenType::TK_ID => Ok(true),
                                        _ => Ok(false),
                                    },
                                }
                            })?
                        } else {
                            false
                        }
                    };

                    if !can_be_over {
                        tok.token_type = Some(TokenType::TK_ID);
                    }
                }
                TokenType::TK_FILTER => {
                    let prev_tt = self.current_token.token_type.unwrap_or(TokenType::TK_EOF);
                    let can_be_filter = {
                        if prev_tt == TokenType::TK_RP {
                            self.try_parse(|p| {
                                match p.consume_lexer_without_whitespaces_or_comments() {
                                    None => return Ok(false),
                                    Some(tok) => match get_token(tok?.token_type.unwrap()) {
                                        TokenType::TK_LP => Ok(true),
                                        _ => Ok(false),
                                    },
                                }
                            })?
                        } else {
                            false
                        }
                    };

                    if !can_be_filter {
                        tok.token_type = Some(TokenType::TK_ID);
                    }
                }
                _ => {}
            }
        }

        match next {
            None => Ok(None), // EOF
            Some(Ok(tok)) => {
                self.current_token = tok.clone();
                self.peekable = true;
                Ok(Some(tok))
            }
            Some(Err(err)) => Err(err),
        }
    }

    #[inline(always)]
    fn mark<F, R>(&mut self, exc: F) -> Result<R, Error>
    where
        F: FnOnce(&mut Self) -> Result<R, Error>,
    {
        let old_peekable = self.peekable;
        let old_current_token = self.current_token.clone();
        let start_offset = self.lexer.offset;
        let result = exc(self);
        if result.is_err() {
            self.peekable = old_peekable;
            self.current_token = old_current_token;
            self.lexer.offset = start_offset;
        }
        result
    }

    #[inline(always)]
    fn try_parse<F, R>(&mut self, exc: F) -> R
    where
        F: FnOnce(&mut Self) -> R,
    {
        debug_assert!(!self.peekable);
        let start_offset = self.lexer.offset;
        let result = exc(self);
        self.peekable = false;
        self.lexer.offset = start_offset;
        result
    }

    /// Get the next token from the lexer
    #[inline(always)]
    fn eat(&mut self) -> Result<Option<Token<'a>>, Error> {
        let result = self.peek()?;
        self.peekable = false; // Clear the peek mark after consuming
        Ok(result)
    }

    /// Peek at the next token without consuming it
    #[inline(always)]
    fn peek(&mut self) -> Result<Option<Token<'a>>, Error> {
        if self.peekable {
            return Ok(Some(self.current_token.clone()));
        }

        self.next_token()
    }

    #[inline(always)]
    fn eat_no_eof(&mut self) -> Result<Token<'a>, Error> {
        match self.eat()? {
            None => Err(Error::ParseUnexpectedEOF),
            Some(token) => Ok(token),
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

                if *expected == TokenType::TK_ID
                    && token.token_type.unwrap().fallback_id_if_ok() == TokenType::TK_ID
                {
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

    #[inline(always)]
    fn peek_no_eof(&mut self) -> Result<Token<'a>, Error> {
        match self.peek()? {
            None => Err(Error::ParseUnexpectedEOF),
            Some(token) => Ok(token),
        }
    }

    #[inline(always)]
    fn peek_expect(&mut self, expected: &'static [TokenType]) -> Result<Token<'a>, Error> {
        let token = self.peek_no_eof()?;
        for expected in expected {
            if token.token_type == Some(*expected) {
                return Ok(token);
            }

            if *expected == TokenType::TK_ID
                && token.token_type.unwrap().fallback_id_if_ok() == TokenType::TK_ID
            {
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
            TokenType::TK_CREATE,
            TokenType::TK_SELECT,
            TokenType::TK_VALUES,
            // add more
        ])?;

        match tok.token_type.unwrap() {
            TokenType::TK_BEGIN => self.parse_begin(),
            TokenType::TK_COMMIT | TokenType::TK_END => self.parse_commit(),
            TokenType::TK_ROLLBACK => self.parse_rollback(),
            TokenType::TK_SAVEPOINT => self.parse_savepoint(),
            TokenType::TK_RELEASE => self.parse_release(),
            TokenType::TK_CREATE => self.parse_create_stmt(),
            TokenType::TK_SELECT | TokenType::TK_VALUES => Ok(Stmt::Select(self.parse_select()?)),
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

    fn parse_transopt(&mut self) -> Result<Option<Name>, Error> {
        match self.peek()? {
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

    fn parse_begin(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_BEGIN]);

        let transtype = match self.peek()? {
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

    fn parse_commit(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_COMMIT, TokenType::TK_END]);
        Ok(Stmt::Commit {
            name: self.parse_transopt()?,
        })
    }

    fn parse_rollback(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_ROLLBACK]);

        let tx_name = self.parse_transopt()?;

        let savepoint_name = match self.peek()? {
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

    fn parse_savepoint(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_SAVEPOINT]);
        self.peek_nm()?;
        Ok(Stmt::Savepoint {
            name: self.parse_nm(),
        })
    }

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

    fn parse_create_stmt(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_CREATE]);
        let mut first_tok = self.peek_expect(&[
            TokenType::TK_TEMP,
            TokenType::TK_TABLE,
            TokenType::TK_VIRTUAL,
            TokenType::TK_VIEW,
            TokenType::TK_INDEX,
            TokenType::TK_UNIQUE,
            TokenType::TK_TRIGGER,
        ])?;
        let mut temp = false;
        if first_tok.token_type == Some(TokenType::TK_TEMP) {
            temp = true;
            first_tok = self.peek_expect(&[
                TokenType::TK_TABLE,
                TokenType::TK_VIEW,
                TokenType::TK_TRIGGER,
            ])?;
        }

        match first_tok.token_type.unwrap() {
            TokenType::TK_TABLE => self.parse_create_table(temp),
            TokenType::TK_VIRTUAL => todo!(),
            TokenType::TK_VIEW => todo!(),
            TokenType::TK_INDEX => todo!(),
            TokenType::TK_UNIQUE => todo!(),
            TokenType::TK_TRIGGER => todo!(),
            _ => unreachable!(),
        }
    }

    fn parse_if_not_exists(&mut self) -> Result<bool, Error> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TokenType::TK_IF) {
                self.eat_assert(&[TokenType::TK_IF]);
            } else {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }

        self.eat_expect(&[TokenType::TK_NOT])?;
        self.eat_expect(&[TokenType::TK_EXISTS])?;
        Ok(true)
    }

    fn parse_fullname(&mut self, allow_alias: bool) -> Result<QualifiedName, Error> {
        self.peek_nm()?;
        let first_name = self.parse_nm();

        let secone_name = if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TokenType::TK_DOT) {
                self.eat_assert(&[TokenType::TK_DOT]);
                self.peek_nm()?;
                Some(self.parse_nm())
            } else {
                None
            }
        } else {
            None
        };

        let alias_name = if allow_alias {
            if let Some(tok) = self.peek()? {
                if tok.token_type == Some(TokenType::TK_AS) {
                    self.eat_assert(&[TokenType::TK_AS]);
                    self.peek_nm()?;
                    Some(self.parse_nm())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if secone_name.is_some() {
            Ok(QualifiedName {
                db_name: Some(first_name),
                name: secone_name.unwrap(),
                alias: alias_name,
            })
        } else {
            Ok(QualifiedName {
                db_name: None,
                name: first_name,
                alias: alias_name,
            })
        }
    }

    fn parse_signed(&mut self) -> Result<Box<Expr>, Error> {
        self.peek_expect(&[
            TokenType::TK_FLOAT,
            TokenType::TK_INTEGER,
            TokenType::TK_PLUS,
            TokenType::TK_MINUS,
        ])?;

        let expr = self.parse_expr_operand()?;
        match expr.as_ref() {
            Expr::Unary(_, inner) => match inner.as_ref() {
                Expr::Literal(Literal::Numeric(_)) => Ok(expr),
                _ => Err(Error::Custom(
                    "Expected a numeric literal after unary operator".to_string(),
                )),
            },
            _ => Ok(expr),
        }
    }

    fn parse_type(&mut self) -> Result<Option<Type>, Error> {
        let mut type_name = if let Some(tok) = self.peek()? {
            match tok.token_type.unwrap().fallback_id_if_ok() {
                TokenType::TK_ID | TokenType::TK_STRING => {
                    self.eat_assert(&[TokenType::TK_ID, TokenType::TK_STRING]);
                    from_bytes(tok.value)
                }
                _ => return Ok(None),
            }
        } else {
            return Ok(None);
        };

        loop {
            if let Some(tok) = self.peek()? {
                match tok.token_type.unwrap().fallback_id_if_ok() {
                    TokenType::TK_ID | TokenType::TK_STRING => {
                        self.eat_assert(&[TokenType::TK_ID, TokenType::TK_STRING]);
                        type_name.push_str(" ");
                        type_name.push_str(from_bytes_as_str(tok.value));
                    }
                    _ => break,
                }
            } else {
                break;
            }
        }

        let size = if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TokenType::TK_LP) {
                self.eat_assert(&[TokenType::TK_LP]);
                let first_size = self.parse_signed()?;
                let tok = self.eat_expect(&[TokenType::TK_RP, TokenType::TK_COMMA])?;
                match tok.token_type.unwrap() {
                    TokenType::TK_RP => Some(TypeSize::MaxSize(first_size)),
                    TokenType::TK_COMMA => {
                        let second_size = self.parse_signed()?;
                        self.eat_expect(&[TokenType::TK_RP])?;
                        Some(TypeSize::TypeSize(first_size, second_size))
                    }
                    _ => unreachable!(),
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(Some(Type {
            name: type_name,
            size,
        }))
    }

    /// SQLite understands these operators, listed in precedence1 order
    /// (top to bottom / highest to lowest):
    ///
    /// Operators 2
    /// 11 -> ~ [expr]    + [expr]    - [expr]
    /// 10 -> [expr] COLLATE (collation-name) 3
    /// 9 -> ||   ->   ->>
    /// 8 -> *   /   %
    /// 7 -> +   -
    /// 6 -> &  |   <<  >>
    /// 5 -> [expr] ESCAPE [escape-character-expr] 4
    /// 4 -> <  >  <=  >=
    /// 3 -> =  ==  <>  !=  IS   IS NOT
    ///     IS DISTINCT FROM   IS NOT DISTINCT FROM
    ///     [expr] BETWEEN5 [expr] AND [expr]
    ///     IN5  MATCH5  LIKE5  REGEXP5  GLOB5
    ///     [expr] ISNULL  [expr] NOTNULL   [expr] NOT NULL
    /// 2   NOT [expr]
    /// 1 -> AND
    /// 0 -> OR
    ///
    /// this function detect precedence by peeking first token of operator
    /// after parsing a operand (binary operator)

    fn current_token_precedence(&mut self) -> Result<Option<u8>, Error> {
        let tok = self.peek()?;
        if tok.is_none() {
            return Ok(None);
        }

        match tok.unwrap().token_type.unwrap() {
            TokenType::TK_OR => Ok(Some(0)),
            TokenType::TK_AND => Ok(Some(1)),
            TokenType::TK_NOT => Ok(Some(3)), // NOT is 3 because of binary operator
            TokenType::TK_EQ
            | TokenType::TK_NE
            | TokenType::TK_IS
            | TokenType::TK_BETWEEN
            | TokenType::TK_IN
            | TokenType::TK_MATCH
            | TokenType::TK_LIKE_KW
            | TokenType::TK_ISNULL
            | TokenType::TK_NOTNULL => Ok(Some(3)),
            TokenType::TK_LT | TokenType::TK_GT | TokenType::TK_LE | TokenType::TK_GE => {
                Ok(Some(4))
            }
            TokenType::TK_ESCAPE => Ok(Some(5)),
            TokenType::TK_BITAND
            | TokenType::TK_BITOR
            | TokenType::TK_LSHIFT
            | TokenType::TK_RSHIFT => Ok(Some(6)),
            TokenType::TK_PLUS | TokenType::TK_MINUS => Ok(Some(7)),
            TokenType::TK_STAR | TokenType::TK_SLASH | TokenType::TK_REM => Ok(Some(8)),
            TokenType::TK_CONCAT | TokenType::TK_PTR => Ok(Some(9)),
            TokenType::TK_COLLATE => Ok(Some(10)),
            // no need 11 because its for unary operators
            _ => Ok(None),
        }
    }

    fn parse_distinct(&mut self) -> Result<Option<Distinctness>, Error> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_DISTINCT => {
                    self.eat_assert(&[TokenType::TK_DISTINCT]);
                    Ok(Some(Distinctness::Distinct))
                }
                TokenType::TK_ALL => {
                    self.eat_assert(&[TokenType::TK_ALL]);
                    Ok(Some(Distinctness::All))
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_filter_clause(&mut self) -> Result<Option<Box<Expr>>, Error> {
        match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_FILTER => {
                    self.eat_assert(&[TokenType::TK_FILTER]);
                }
                _ => return Ok(None),
            },
        }

        self.eat_expect(&[TokenType::TK_LP])?;
        self.eat_expect(&[TokenType::TK_WHERE])?;
        let expr = self.parse_expr(0)?;
        self.eat_expect(&[TokenType::TK_RP])?;
        Ok(Some(expr))
    }

    fn parse_frame_opt(&mut self) -> Result<Option<FrameClause>, Error> {
        let range_or_rows = match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_RANGE => {
                    self.eat_assert(&[TokenType::TK_RANGE]);
                    FrameMode::Range
                }
                TokenType::TK_ROWS => {
                    self.eat_assert(&[TokenType::TK_ROWS]);
                    FrameMode::Rows
                }
                TokenType::TK_GROUPS => {
                    self.eat_assert(&[TokenType::TK_GROUPS]);
                    FrameMode::Groups
                }
                _ => return Ok(None),
            },
        };

        let has_end = match self.peek_no_eof()?.token_type.unwrap() {
            TokenType::TK_BETWEEN => {
                self.eat_assert(&[TokenType::TK_BETWEEN]);
                true
            }
            _ => false,
        };

        let start = match self.peek_no_eof()?.token_type.unwrap() {
            TokenType::TK_UNBOUNDED => {
                self.eat_assert(&[TokenType::TK_UNBOUNDED]);
                self.eat_expect(&[TokenType::TK_PRECEDING])?;
                FrameBound::UnboundedPreceding
            }
            TokenType::TK_CURRENT => {
                self.eat_assert(&[TokenType::TK_CURRENT]);
                self.eat_expect(&[TokenType::TK_ROW])?;
                FrameBound::CurrentRow
            }
            _ => {
                let expr = self.parse_expr(0)?;
                let tok = self.eat_expect(&[TokenType::TK_PRECEDING, TokenType::TK_FOLLOWING])?;
                match tok.token_type.unwrap() {
                    TokenType::TK_PRECEDING => FrameBound::Preceding(expr),
                    TokenType::TK_FOLLOWING => FrameBound::Following(expr),
                    _ => unreachable!(),
                }
            }
        };

        let end = if has_end {
            self.eat_expect(&[TokenType::TK_AND])?;

            Some(match self.peek_no_eof()?.token_type.unwrap() {
                TokenType::TK_UNBOUNDED => {
                    self.eat_assert(&[TokenType::TK_UNBOUNDED]);
                    self.eat_expect(&[TokenType::TK_FOLLOWING])?;
                    FrameBound::UnboundedFollowing
                }
                TokenType::TK_CURRENT => {
                    self.eat_assert(&[TokenType::TK_CURRENT]);
                    self.eat_expect(&[TokenType::TK_ROW])?;
                    FrameBound::CurrentRow
                }
                _ => {
                    let expr = self.parse_expr(0)?;
                    let tok =
                        self.eat_expect(&[TokenType::TK_PRECEDING, TokenType::TK_FOLLOWING])?;
                    match tok.token_type.unwrap() {
                        TokenType::TK_PRECEDING => FrameBound::Preceding(expr),
                        TokenType::TK_FOLLOWING => FrameBound::Following(expr),
                        _ => unreachable!(),
                    }
                }
            })
        } else {
            None
        };

        let exclude = match self.peek()? {
            None => None,
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_EXCLUDE => {
                    self.eat_assert(&[TokenType::TK_EXCLUDE]);
                    let tok = self.eat_expect(&[
                        TokenType::TK_NO,
                        TokenType::TK_CURRENT,
                        TokenType::TK_GROUP,
                        TokenType::TK_TIES,
                    ])?;
                    match tok.token_type.unwrap() {
                        TokenType::TK_NO => {
                            self.eat_expect(&[TokenType::TK_OTHERS])?;
                            Some(FrameExclude::NoOthers)
                        }
                        TokenType::TK_CURRENT => {
                            self.eat_expect(&[TokenType::TK_ROW])?;
                            Some(FrameExclude::CurrentRow)
                        }
                        TokenType::TK_GROUP => Some(FrameExclude::Group),
                        TokenType::TK_TIES => Some(FrameExclude::Ties),
                        _ => unreachable!(),
                    }
                }
                _ => None,
            },
        };

        Ok(Some(FrameClause {
            mode: range_or_rows,
            start,
            end,
            exclude,
        }))
    }

    fn parse_window(&mut self) -> Result<Window, Error> {
        let name = match self.peek()? {
            None => None,
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_PARTITION
                | TokenType::TK_ORDER
                | TokenType::TK_RANGE
                | TokenType::TK_ROWS
                | TokenType::TK_GROUPS => None,
                tt => match tt.fallback_id_if_ok() {
                    TokenType::TK_ID
                    | TokenType::TK_STRING
                    | TokenType::TK_INDEXED
                    | TokenType::TK_JOIN_KW => Some(self.parse_nm()),
                    _ => None,
                },
            },
        };

        let partition_by = match self.peek()? {
            None => vec![],
            Some(tok) if tok.token_type == Some(TokenType::TK_PARTITION) => {
                self.eat_assert(&[TokenType::TK_PARTITION]);
                self.eat_expect(&[TokenType::TK_BY])?;
                self.parse_nexpr_list()?
            }
            _ => vec![],
        };

        let order_by = self.parse_order_by()?;
        let frame_clause = self.parse_frame_opt()?;
        Ok(Window {
            base: name,
            partition_by,
            order_by,
            frame_clause,
        })
    }

    fn parse_over_clause(&mut self) -> Result<Option<Over>, Error> {
        match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_OVER => {
                    self.eat_assert(&[TokenType::TK_OVER]);
                }
                _ => return Ok(None),
            },
        }

        let tok = self.peek_expect(&[
            TokenType::TK_LP,
            TokenType::TK_ID,
            TokenType::TK_STRING,
            TokenType::TK_INDEXED,
            TokenType::TK_JOIN_KW,
        ])?;
        match tok.token_type.unwrap() {
            TokenType::TK_LP => {
                self.eat_assert(&[TokenType::TK_LP]);
                let window = self.parse_window()?;
                self.eat_expect(&[TokenType::TK_LP])?;
                Ok(Some(Over::Window(window)))
            }
            _ => {
                let name = self.parse_nm();
                Ok(Some(Over::Name(name)))
            }
        }
    }

    fn parse_filter_over(&mut self) -> Result<FunctionTail, Error> {
        let filter_clause = self.parse_filter_clause()?;
        let over_clause = self.parse_over_clause()?;
        Ok(FunctionTail {
            filter_clause,
            over_clause,
        })
    }


    #[inline(always)] // this function is hot :)
    fn parse_expr_operand(&mut self) -> Result<Box<Expr>, Error> {
        let tok = self.peek_expect(&[
            TokenType::TK_LP,
            TokenType::TK_CAST,
            TokenType::TK_CTIME_KW,
            TokenType::TK_ID,
            TokenType::TK_STRING,
            TokenType::TK_INDEXED,
            TokenType::TK_JOIN_KW,
            TokenType::TK_NULL,
            TokenType::TK_BLOB,
            TokenType::TK_FLOAT,
            TokenType::TK_INTEGER,
            TokenType::TK_VARIABLE,
            TokenType::TK_NOT,
            TokenType::TK_BITNOT,
            TokenType::TK_PLUS,
            TokenType::TK_MINUS,
            TokenType::TK_EXISTS,
            TokenType::TK_CASE,
        ])?;

        match tok.token_type.unwrap() {
            TokenType::TK_LP => {
                self.eat_assert(&[TokenType::TK_LP]);
                let exprs = self.parse_nexpr_list()?;
                self.eat_expect(&[TokenType::TK_RP])?;
                Ok(Box::new(Expr::Parenthesized(exprs)))
            }
            TokenType::TK_NULL => {
                self.eat_assert(&[TokenType::TK_NULL]);
                Ok(Box::new(Expr::Literal(Literal::Null)))
            }
            TokenType::TK_BLOB => {
                self.eat_assert(&[TokenType::TK_BLOB]);
                Ok(Box::new(Expr::Literal(Literal::Blob(from_bytes(
                    tok.value,
                )))))
            }
            TokenType::TK_FLOAT => {
                self.eat_assert(&[TokenType::TK_FLOAT]);
                Ok(Box::new(Expr::Literal(Literal::Numeric(from_bytes(
                    tok.value,
                )))))
            }
            TokenType::TK_INTEGER => {
                self.eat_assert(&[TokenType::TK_INTEGER]);
                Ok(Box::new(Expr::Literal(Literal::Numeric(from_bytes(
                    tok.value,
                )))))
            }
            TokenType::TK_VARIABLE => {
                self.eat_assert(&[TokenType::TK_VARIABLE]);
                Ok(Box::new(Expr::Variable(from_bytes(tok.value))))
            }
            TokenType::TK_CAST => {
                self.eat_assert(&[TokenType::TK_CAST]);
                self.eat_expect(&[TokenType::TK_LP])?;
                let expr = self.parse_expr(0)?;
                self.eat_expect(&[TokenType::TK_AS])?;
                let typ = self.parse_type()?;
                self.eat_expect(&[TokenType::TK_RP])?;
                return Ok(Box::new(Expr::Cast {
                    expr,
                    type_name: typ,
                }));
            }
            TokenType::TK_CTIME_KW => {
                let tok = self.eat_assert(&[TokenType::TK_CTIME_KW]);
                if b"CURRENT_DATE".eq_ignore_ascii_case(&tok.value) {
                    Ok(Box::new(Expr::Literal(Literal::CurrentDate)))
                } else if b"CURRENT_TIME".eq_ignore_ascii_case(&tok.value) {
                    Ok(Box::new(Expr::Literal(Literal::CurrentTime)))
                } else if b"CURRENT_TIMESTAMP".eq_ignore_ascii_case(&tok.value) {
                    Ok(Box::new(Expr::Literal(Literal::CurrentTimestamp)))
                } else {
                    unreachable!()
                }
            }
            TokenType::TK_NOT => {
                self.eat_assert(&[TokenType::TK_NOT]);
                let expr = self.parse_expr(2)?; // NOT precedence is 2
                Ok(Box::new(Expr::Unary(UnaryOperator::Not, expr)))
            }
            TokenType::TK_BITNOT => {
                self.eat_assert(&[TokenType::TK_BITNOT]);
                let expr = self.parse_expr(11)?; // BITNOT precedence is 11
                Ok(Box::new(Expr::Unary(UnaryOperator::BitwiseNot, expr)))
            }
            TokenType::TK_PLUS => {
                self.eat_assert(&[TokenType::TK_PLUS]);
                let expr = self.parse_expr(11)?; // PLUS precedence is 11
                Ok(Box::new(Expr::Unary(UnaryOperator::Positive, expr)))
            }
            TokenType::TK_MINUS => {
                self.eat_assert(&[TokenType::TK_MINUS]);
                let expr = self.parse_expr(11)?; // MINUS precedence is 11
                Ok(Box::new(Expr::Unary(UnaryOperator::Negative, expr)))
            }
            TokenType::TK_EXISTS => {
                self.eat_assert(&[TokenType::TK_EXISTS]);
                self.eat_expect(&[TokenType::TK_LP])?;
                let select = self.parse_select()?;
                self.eat_expect(&[TokenType::TK_RP])?;
                Ok(Box::new(Expr::Exists(select)))
            }
            TokenType::TK_CASE => {
                self.eat_assert(&[TokenType::TK_CASE]);
                let base = if self.peek_no_eof()?.token_type.unwrap() != TokenType::TK_WHEN {
                    Some(self.parse_expr(0)?)
                } else {
                    None
                };

                let mut when_then_pairs = vec![];
                loop {
                    if let Some(tok) = self.peek()? {
                        if tok.token_type.unwrap() != TokenType::TK_WHEN {
                            break;
                        }
                    } else {
                        break;
                    }

                    self.eat_assert(&[TokenType::TK_WHEN]);
                    let when = self.parse_expr(0)?;
                    self.eat_expect(&[TokenType::TK_THEN])?;
                    let then = self.parse_expr(0)?;
                    when_then_pairs.push((when, then));
                }

                let else_expr = if let Some(ok) = self.peek()? {
                    if ok.token_type == Some(TokenType::TK_ELSE) {
                        self.eat_assert(&[TokenType::TK_ELSE]);
                        Some(self.parse_expr(0)?)
                    } else {
                        None
                    }
                } else {
                    None
                };

                Ok(Box::new(Expr::Case {
                    base,
                    when_then_pairs,
                    else_expr,
                }))
            }
            _ => {
                let can_be_lit_str = tok.token_type == Some(TokenType::TK_STRING);
                debug_assert!(self.peek_nm().is_ok(), "Expected a name token");
                let name = self.parse_nm();

                let second_name = if let Some(tok) = self.peek()? {
                    if tok.token_type == Some(TokenType::TK_DOT) {
                        self.eat_assert(&[TokenType::TK_DOT]);
                        self.peek_nm()?;
                        Some(self.parse_nm())
                    } else if tok.token_type == Some(TokenType::TK_LP) {
                        if can_be_lit_str {
                            return Err(Error::ParseUnexpectedToken {
                                got: TokenType::TK_STRING,
                                expected: &[
                                    TokenType::TK_ID,
                                    TokenType::TK_INDEXED,
                                    TokenType::TK_JOIN_KW,
                                ],
                            });
                        } // can not be literal string in function name

                        self.eat_assert(&[TokenType::TK_LP]);
                        let tok = self.peek_no_eof()?;
                        match tok.token_type.unwrap() {
                            TokenType::TK_STAR => {
                                self.eat_assert(&[TokenType::TK_STAR]);
                                self.eat_expect(&[TokenType::TK_RP])?;
                                return Ok(Box::new(Expr::FunctionCallStar {
                                    name: name,
                                    filter_over: self.parse_filter_over()?,
                                }));
                            }
                            _ => {
                                let distinct = self.parse_distinct()?;
                                let exprs = self.parse_expr_list()?;
                                self.eat_expect(&[TokenType::TK_RP])?;
                                let order_by = self.parse_order_by()?;
                                let filter_over = self.parse_filter_over()?;
                                return Ok(Box::new(Expr::FunctionCall {
                                    name,
                                    distinctness: distinct,
                                    args: exprs,
                                    order_by,
                                    filter_over,
                                }));
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                let third_name = if let Some(tok) = self.peek()? {
                    if tok.token_type == Some(TokenType::TK_DOT) {
                        self.eat_assert(&[TokenType::TK_DOT]);
                        self.peek_nm()?;
                        Some(self.parse_nm())
                    } else {
                        None
                    }
                } else {
                    None
                };

                if second_name.is_some() && third_name.is_some() {
                    Ok(Box::new(Expr::DoublyQualified(
                        name,
                        second_name.unwrap(),
                        third_name.unwrap(),
                    )))
                } else if second_name.is_some() {
                    Ok(Box::new(Expr::Qualified(name, second_name.unwrap())))
                } else if can_be_lit_str {
                    Ok(Box::new(Expr::Literal(match name {
                        Name::Quoted(s) => Literal::String(s),
                        Name::Ident(s) => Literal::String(s),
                    })))
                } else {
                    Ok(Box::new(Expr::Id(name)))
                }
            }
        }
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Box<Expr>>, Error> {
        let mut exprs = vec![];
        loop {
            match self.peek()? {
                Some(tok) => match tok.token_type.unwrap().fallback_id_if_ok() {
                    TokenType::TK_LP
                    | TokenType::TK_CAST
                    | TokenType::TK_ID
                    | TokenType::TK_STRING
                    | TokenType::TK_INDEXED
                    | TokenType::TK_JOIN_KW
                    | TokenType::TK_NULL
                    | TokenType::TK_BLOB
                    | TokenType::TK_FLOAT
                    | TokenType::TK_INTEGER
                    | TokenType::TK_VARIABLE
                    | TokenType::TK_CTIME_KW
                    | TokenType::TK_NOT
                    | TokenType::TK_BITNOT
                    | TokenType::TK_PLUS
                    | TokenType::TK_MINUS
                    | TokenType::TK_EXISTS
                    | TokenType::TK_CASE => {}
                    _ => break,
                },
                None => break,
            }

            exprs.push(self.parse_expr(0)?);
            match self.peek_no_eof()?.token_type.unwrap() {
                TokenType::TK_COMMA => {
                    self.eat_assert(&[TokenType::TK_COMMA]);
                }
                _ => break,
            }
        }

        Ok(exprs)
    }

    fn parse_expr(&mut self, precedence: u8) -> Result<Box<Expr>, Error> {
        let mut result = self.parse_expr_operand()?;

        loop {
            let pre = match self.current_token_precedence()? {
                Some(pre) if pre < precedence => break,
                None => break, // no more ops
                Some(pre) => pre,
            };

            let mut tok = self.peek_no_eof()?;
            let mut not = false;
            if tok.token_type.unwrap() == TokenType::TK_NOT {
                self.eat_assert(&[TokenType::TK_NOT]);
                tok = self.peek_expect(&[
                    TokenType::TK_BETWEEN,
                    TokenType::TK_IN,
                    TokenType::TK_MATCH,
                    TokenType::TK_LIKE_KW,
                ])?;
                not = true;
            }

            result = match tok.token_type.unwrap() {
                TokenType::TK_OR => {
                    self.eat_assert(&[TokenType::TK_OR]);
                    Box::new(Expr::Binary(result, Operator::Or, self.parse_expr(pre)?))
                }
                TokenType::TK_AND => {
                    self.eat_assert(&[TokenType::TK_AND]);
                    Box::new(Expr::Binary(result, Operator::And, self.parse_expr(pre)?))
                }
                TokenType::TK_EQ => {
                    self.eat_assert(&[TokenType::TK_EQ]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Equals,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_NE => {
                    self.eat_assert(&[TokenType::TK_NE]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::NotEquals,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_BETWEEN => {
                    self.eat_assert(&[TokenType::TK_BETWEEN]);
                    let start = self.parse_expr(pre)?;
                    self.eat_expect(&[TokenType::TK_AND])?;
                    let end = self.parse_expr(pre)?;
                    Box::new(Expr::Between {
                        lhs: result,
                        not,
                        start,
                        end,
                    })
                }
                TokenType::TK_IN => {
                    self.eat_assert(&[TokenType::TK_IN]);
                    let tok = self.peek_no_eof()?;
                    match tok.token_type.unwrap() {
                        TokenType::TK_LP => {
                            self.eat_assert(&[TokenType::TK_LP]);
                            let tok = self.peek_no_eof()?;
                            match tok.token_type.unwrap() {
                                TokenType::TK_SELECT
                                | TokenType::TK_WITH
                                | TokenType::TK_VALUES => {
                                    let select = self.parse_select()?;
                                    self.eat_expect(&[TokenType::TK_RP])?;
                                    Box::new(Expr::InSelect {
                                        lhs: result,
                                        not,
                                        rhs: select,
                                    })
                                }
                                _ => {
                                    let exprs = self.parse_expr_list()?;
                                    self.eat_expect(&[TokenType::TK_RP])?;
                                    Box::new(Expr::InList {
                                        lhs: result,
                                        not,
                                        rhs: exprs,
                                    })
                                }
                            }
                        }
                        _ => {
                            let name = self.parse_fullname(false)?;
                            let mut exprs = vec![];
                            if let Some(tok) = self.peek()? {
                                if tok.token_type == Some(TokenType::TK_LP) {
                                    self.eat_assert(&[TokenType::TK_LP]);
                                    exprs = self.parse_expr_list()?;
                                    self.eat_expect(&[TokenType::TK_RP])?;
                                }
                            }

                            Box::new(Expr::InTable {
                                lhs: result,
                                not,
                                rhs: name,
                                args: exprs,
                            })
                        }
                    }
                }
                TokenType::TK_MATCH | TokenType::TK_LIKE_KW => {
                    let tok = self.eat_assert(&[TokenType::TK_MATCH, TokenType::TK_LIKE_KW]);
                    let op = match tok.token_type.unwrap() {
                        TokenType::TK_MATCH => LikeOperator::Match,
                        TokenType::TK_LIKE_KW => {
                            if b"LIKE".eq_ignore_ascii_case(tok.value) {
                                LikeOperator::Like
                            } else if b"GLOB".eq_ignore_ascii_case(tok.value) {
                                LikeOperator::Glob
                            } else if b"REGEXP".eq_ignore_ascii_case(tok.value) {
                                LikeOperator::Regexp
                            } else {
                                unreachable!()
                            }
                        }
                        _ => unreachable!(),
                    };

                    let expr = self.parse_expr(5)?; // do not consume ESCAPE
                    let escape = if let Some(tok) = self.peek()? {
                        if tok.token_type == Some(TokenType::TK_ESCAPE) {
                            self.eat_assert(&[TokenType::TK_ESCAPE]);
                            Some(self.parse_expr(5)?)
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    Box::new(Expr::Like {
                        lhs: result,
                        not,
                        op,
                        rhs: expr,
                        escape,
                    })
                }
                TokenType::TK_ISNULL => {
                    self.eat_assert(&[TokenType::TK_ISNULL]);
                    Box::new(Expr::IsNull(result))
                }
                TokenType::TK_NOTNULL => {
                    self.eat_assert(&[TokenType::TK_NOTNULL]);
                    Box::new(Expr::NotNull(result))
                }
                TokenType::TK_LT => {
                    self.eat_assert(&[TokenType::TK_LT]);
                    Box::new(Expr::Binary(result, Operator::Less, self.parse_expr(pre)?))
                }
                TokenType::TK_GT => {
                    self.eat_assert(&[TokenType::TK_GT]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Greater,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_LE => {
                    self.eat_assert(&[TokenType::TK_LE]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::LessEquals,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_GE => {
                    self.eat_assert(&[TokenType::TK_GE]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::GreaterEquals,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_ESCAPE => unreachable!(),
                TokenType::TK_BITAND => {
                    self.eat_assert(&[TokenType::TK_BITAND]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::BitwiseAnd,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_BITOR => {
                    self.eat_assert(&[TokenType::TK_BITOR]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::BitwiseOr,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_LSHIFT => {
                    self.eat_assert(&[TokenType::TK_LSHIFT]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::LeftShift,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_RSHIFT => {
                    self.eat_assert(&[TokenType::TK_RSHIFT]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::RightShift,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_PLUS => {
                    self.eat_assert(&[TokenType::TK_PLUS]);
                    Box::new(Expr::Binary(result, Operator::Add, self.parse_expr(pre)?))
                }
                TokenType::TK_MINUS => {
                    self.eat_assert(&[TokenType::TK_MINUS]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Subtract,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_STAR => {
                    self.eat_assert(&[TokenType::TK_STAR]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Multiply,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_SLASH => {
                    self.eat_assert(&[TokenType::TK_SLASH]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Divide,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_REM => {
                    self.eat_assert(&[TokenType::TK_REM]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Modulus,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_CONCAT => {
                    self.eat_assert(&[TokenType::TK_CONCAT]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Concat,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_PTR => {
                    let tok = self.eat_assert(&[TokenType::TK_PTR]);
                    let op = if tok.value.len() == 2 {
                        Operator::ArrowRight
                    } else {
                        Operator::ArrowRightShift
                    };

                    Box::new(Expr::Binary(result, op, self.parse_expr(pre)?))
                }
                TokenType::TK_COLLATE => {
                    Box::new(Expr::Collate(result, self.parse_collate()?.unwrap()))
                }
                _ => unreachable!(),
            }
        }

        Ok(result)
    }

    fn parse_collate(&mut self) -> Result<Option<Name>, Error> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TokenType::TK_COLLATE) {
                self.eat_assert(&[TokenType::TK_COLLATE]);
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        let tok = self.eat_expect(&[TokenType::TK_ID, TokenType::TK_STRING])?;
        let first_char = tok.value[0]; // no need to check empty
        match first_char {
            b'[' | b'\'' | b'`' | b'"' => Ok(Some(Name::Quoted(from_bytes(tok.value)))),
            _ => Ok(Some(Name::Ident(from_bytes(tok.value)))),
        }
    }

    fn parse_sort_order(&mut self) -> Result<Option<SortOrder>, Error> {
        match self.peek()? {
            Some(tok) if tok.token_type == Some(TokenType::TK_ASC) => {
                self.eat_assert(&[TokenType::TK_ASC]);
                Ok(Some(SortOrder::Asc))
            }
            Some(tok) if tok.token_type == Some(TokenType::TK_DESC) => {
                self.eat_assert(&[TokenType::TK_DESC]);
                Ok(Some(SortOrder::Desc))
            }
            _ => Ok(None),
        }
    }

    fn parse_eid_list(&mut self) -> Result<Vec<IndexedColumn>, Error> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TokenType::TK_LP) {
                self.eat_assert(&[TokenType::TK_LP]);
            } else {
                return Ok(vec![]);
            }
        } else {
            return Ok(vec![]);
        }

        let mut columns = vec![];
        loop {
            if self.peek_no_eof()?.token_type == Some(TokenType::TK_RP) {
                self.eat_assert(&[TokenType::TK_RP]);
                break;
            }

            self.peek_nm()?;
            let nm = self.parse_nm();
            let collate = self.parse_collate()?;
            let sort_order = self.parse_sort_order()?;
            columns.push(IndexedColumn {
                col_name: nm,
                collation_name: collate,
                order: sort_order,
            });

            let tok = self.eat_expect(&[TokenType::TK_COMMA, TokenType::TK_RP])?;
            if tok.token_type == Some(TokenType::TK_RP) {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_common_table_expr(&mut self) -> Result<CommonTableExpr, Error> {
        self.peek_nm()?;
        let nm = self.parse_nm();
        let eid_list = self.parse_eid_list()?;
        self.eat_expect(&[TokenType::TK_AS])?;
        let wqas = match self.peek_no_eof()?.token_type.unwrap() {
            TokenType::TK_MATERIALIZED => {
                self.eat_assert(&[TokenType::TK_MATERIALIZED]);
                Materialized::Yes
            }
            TokenType::TK_NOT => {
                self.eat_assert(&[TokenType::TK_NOT]);
                self.eat_expect(&[TokenType::TK_MATERIALIZED])?;
                Materialized::No
            }
            _ => Materialized::Any,
        };
        self.eat_expect(&[TokenType::TK_LP])?;
        let select = self.parse_select()?;
        self.eat_expect(&[TokenType::TK_RP])?;
        Ok(CommonTableExpr {
            tbl_name: nm,
            columns: eid_list,
            materialized: wqas,
            select,
        })
    }

    fn parse_with(&mut self) -> Result<Option<With>, Error> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TokenType::TK_WITH) {
                self.eat_assert(&[TokenType::TK_WITH]);
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        let recursive = if self.peek_no_eof()?.token_type == Some(TokenType::TK_RECURSIVE) {
            self.eat_assert(&[TokenType::TK_RECURSIVE]);
            true
        } else {
            false
        };

        let mut ctes = vec![self.parse_common_table_expr()?];
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TokenType::TK_COMMA) {
                self.eat_assert(&[TokenType::TK_COMMA]);
                loop {
                    ctes.push(self.parse_common_table_expr()?);
                    match self.peek_no_eof()?.token_type.unwrap() {
                        TokenType::TK_COMMA => {
                            self.eat_assert(&[TokenType::TK_COMMA]);
                        }
                        _ => break,
                    }
                }
            }
        }

        Ok(Some(With { recursive, ctes }))
    }

    fn parse_as(&mut self) -> Result<Option<As>, Error> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.unwrap().fallback_id_if_ok() {
                TokenType::TK_AS => {
                    self.eat_assert(&[TokenType::TK_AS]);
                    self.peek_nm()?;
                    Ok(Some(As::As(self.parse_nm())))
                }
                TokenType::TK_STRING | TokenType::TK_ID => Ok(Some(As::Elided(self.parse_nm()))),
                _ => Ok(None),
            },
        }
    }

    fn parse_window_defn(&mut self) -> Result<WindowDef, Error> {
        self.peek_nm()?;
        let name = self.parse_nm();
        self.eat_expect(&[TokenType::TK_AS])?;
        self.eat_expect(&[TokenType::TK_LP])?;
        let window = self.parse_window()?;
        self.eat_expect(&[TokenType::TK_RP])?;
        Ok(WindowDef { name, window })
    }

    fn parse_window_clause(&mut self) -> Result<Vec<WindowDef>, Error> {
        match self.peek()? {
            None => return Ok(vec![]),
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_WINDOW => {
                    self.eat_assert(&[TokenType::TK_WINDOW]);
                }
                _ => return Ok(vec![]),
            },
        }

        let mut result = vec![self.parse_window_defn()?];
        loop {
            match self.peek_no_eof()?.token_type.unwrap() {
                TokenType::TK_COMMA => {
                    self.eat_assert(&[TokenType::TK_COMMA]);
                    result.push(self.parse_window_defn()?);
                }
                _ => break,
            }
        }

        Ok(result)
    }

    fn parse_group_by(&mut self) -> Result<Option<GroupBy>, Error> {
        match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_GROUP => {
                    self.eat_assert(&[TokenType::TK_GROUP]);
                    self.eat_expect(&[TokenType::TK_BY])?;
                }
                _ => return Ok(None),
            },
        }

        let exprs = self.parse_nexpr_list()?;
        let having = match self.peek()? {
            Some(tok) if tok.token_type == Some(TokenType::TK_HAVING) => {
                self.eat_assert(&[TokenType::TK_HAVING]);
                Some(self.parse_expr(0)?)
            }
            _ => None,
        };

        Ok(Some(GroupBy { exprs, having }))
    }

    fn parse_where(&mut self) -> Result<Option<Box<Expr>>, Error> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_WHERE => {
                    self.eat_assert(&[TokenType::TK_WHERE]);
                    let expr = self.parse_expr(0)?;
                    Ok(Some(expr))
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_indexed(&mut self) -> Result<Option<Indexed>, Error> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_INDEXED => {
                    self.eat_assert(&[TokenType::TK_INDEXED]);
                    self.eat_expect(&[TokenType::TK_BY])?;
                    self.peek_nm()?;
                    Ok(Some(Indexed::IndexedBy(self.parse_nm())))
                }
                TokenType::TK_NOT => {
                    self.eat_assert(&[TokenType::TK_NOT]);
                    self.eat_expect(&[TokenType::TK_INDEXED])?;
                    Ok(Some(Indexed::NotIndexed))
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_nm_list(&mut self) -> Result<Vec<Name>, Error> {
        self.peek_nm()?;
        let mut names = vec![self.parse_nm()];

        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == Some(TokenType::TK_COMMA) => {
                    self.eat_assert(&[TokenType::TK_COMMA]);
                    self.peek_nm()?;
                    names.push(self.parse_nm());
                }
                _ => break,
            }
        }

        Ok(names)
    }

    fn parse_on_using(&mut self) -> Result<Option<JoinConstraint>, Error> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_ON => {
                    self.eat_assert(&[TokenType::TK_ON]);
                    let expr = self.parse_expr(0)?;
                    Ok(Some(JoinConstraint::On(expr)))
                }
                TokenType::TK_USING => {
                    self.eat_assert(&[TokenType::TK_USING]);
                    self.eat_expect(&[TokenType::TK_LP])?;
                    let names = self.parse_nm_list()?;
                    self.eat_expect(&[TokenType::TK_RP])?;
                    Ok(Some(JoinConstraint::Using(names)))
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_joined_tables(&mut self) -> Result<Vec<JoinedSelectTable>, Error> {
        let mut result = vec![];
        loop {
            let op = match self.peek()? {
                Some(tok) => match tok.token_type.unwrap() {
                    TokenType::TK_COMMA => {
                        self.eat_assert(&[TokenType::TK_COMMA]);
                        JoinOperator::Comma
                    }
                    TokenType::TK_JOIN => {
                        self.eat_assert(&[TokenType::TK_JOIN]);
                        JoinOperator::TypedJoin(None)
                    }
                    TokenType::TK_JOIN_KW => {
                        let jkw = self.eat_assert(&[TokenType::TK_JOIN_KW]);
                        let tok = self.eat_expect(&[
                            TokenType::TK_JOIN,
                            TokenType::TK_ID,
                            TokenType::TK_STRING,
                            TokenType::TK_INDEXED,
                            TokenType::TK_JOIN_KW,
                        ])?;

                        match tok.token_type.unwrap() {
                            TokenType::TK_JOIN => {
                                JoinOperator::TypedJoin(Some(new_join_type(jkw.value, None, None)?))
                            }
                            _ => {
                                let tok_name_1 = self.eat_assert(&[
                                    TokenType::TK_ID,
                                    TokenType::TK_STRING,
                                    TokenType::TK_INDEXED,
                                    TokenType::TK_JOIN_KW,
                                ]);

                                let tok = self.eat_expect(&[
                                    TokenType::TK_JOIN,
                                    TokenType::TK_ID,
                                    TokenType::TK_STRING,
                                    TokenType::TK_INDEXED,
                                    TokenType::TK_JOIN_KW,
                                ])?;

                                match tok.token_type.unwrap() {
                                    TokenType::TK_JOIN => JoinOperator::TypedJoin(Some(
                                        new_join_type(jkw.value, Some(tok_name_1.value), None)?,
                                    )),
                                    _ => {
                                        let tok_name_2 = self.eat_assert(&[
                                            TokenType::TK_ID,
                                            TokenType::TK_STRING,
                                            TokenType::TK_INDEXED,
                                            TokenType::TK_JOIN_KW,
                                        ]);
                                        self.eat_expect(&[TokenType::TK_JOIN])?;
                                        JoinOperator::TypedJoin(Some(new_join_type(
                                            jkw.value,
                                            Some(tok_name_1.value),
                                            Some(tok_name_2.value),
                                        )?))
                                    }
                                }
                            }
                        }
                    }
                    _ => break,
                },
                None => break,
            };

            let tok = self.peek_expect(&[
                TokenType::TK_ID,
                TokenType::TK_STRING,
                TokenType::TK_INDEXED,
                TokenType::TK_JOIN_KW,
                TokenType::TK_LP,
            ])?;

            match tok.token_type.unwrap().fallback_id_if_ok() {
                TokenType::TK_ID
                | TokenType::TK_STRING
                | TokenType::TK_INDEXED
                | TokenType::TK_JOIN_KW => {
                    let name = self.parse_fullname(false)?;
                    match self.peek()? {
                        None => {
                            result.push(JoinedSelectTable {
                                operator: op,
                                table: Box::new(SelectTable::Table(name, None, None)),
                                constraint: None,
                            });
                        }
                        Some(tok) => match tok.token_type.unwrap() {
                            TokenType::TK_LP => {
                                self.eat_assert(&[TokenType::TK_LP]);
                                let exprs = self.parse_expr_list()?;
                                self.eat_assert(&[TokenType::TK_RP]);
                                let alias = self.parse_as()?;
                                let on_using = self.parse_on_using()?;
                                result.push(JoinedSelectTable {
                                    operator: op,
                                    table: Box::new(SelectTable::TableCall(name, exprs, alias)),
                                    constraint: on_using,
                                });
                            }
                            _ => {
                                let alias = self.parse_as()?;
                                let indexed = self.parse_indexed()?;
                                let on_using = self.parse_on_using()?;
                                result.push(JoinedSelectTable {
                                    operator: op,
                                    table: Box::new(SelectTable::Table(name, alias, indexed)),
                                    constraint: on_using,
                                });
                            }
                        },
                    }
                }
                TokenType::TK_LP => {
                    self.eat_assert(&[TokenType::TK_LP]);
                    match self.peek_no_eof()?.token_type.unwrap() {
                        TokenType::TK_SELECT | TokenType::TK_WITH | TokenType::TK_VALUES => {
                            let select = self.parse_select()?;
                            self.eat_expect(&[TokenType::TK_RP])?;
                            let alias = self.parse_as()?;
                            let on_using = self.parse_on_using()?;
                            result.push(JoinedSelectTable {
                                operator: op,
                                table: Box::new(SelectTable::Select(select, alias)),
                                constraint: on_using,
                            });
                        }
                        _ => {
                            let fr = self.parse_from_clause()?;
                            self.eat_expect(&[TokenType::TK_RP])?;
                            let alias = self.parse_as()?;
                            let on_using = self.parse_on_using()?;
                            result.push(JoinedSelectTable {
                                operator: op,
                                table: Box::new(SelectTable::Sub(fr, alias)),
                                constraint: on_using,
                            });
                        }
                    }
                }
                _ => unreachable!(),
            }
        }

        Ok(result)
    }

    fn parse_from_clause(&mut self) -> Result<FromClause, Error> {
        let tok = self.peek_expect(&[
            TokenType::TK_ID,
            TokenType::TK_STRING,
            TokenType::TK_INDEXED,
            TokenType::TK_JOIN_KW,
            TokenType::TK_LP,
        ])?;

        match tok.token_type.unwrap().fallback_id_if_ok() {
            TokenType::TK_ID
            | TokenType::TK_STRING
            | TokenType::TK_INDEXED
            | TokenType::TK_JOIN_KW => {
                let name = self.parse_fullname(false)?;
                match self.peek()? {
                    None => Ok(FromClause {
                        select: Box::new(SelectTable::Table(name, None, None)),
                        joins: vec![],
                    }),
                    Some(tok) => match tok.token_type.unwrap() {
                        TokenType::TK_LP => {
                            self.eat_assert(&[TokenType::TK_LP]);
                            let exprs = self.parse_expr_list()?;
                            self.eat_assert(&[TokenType::TK_RP]);
                            let alias = self.parse_as()?;
                            Ok(FromClause {
                                select: Box::new(SelectTable::TableCall(name, exprs, alias)),
                                joins: self.parse_joined_tables()?,
                            })
                        }
                        _ => {
                            let alias = self.parse_as()?;
                            let indexed = self.parse_indexed()?;
                            Ok(FromClause {
                                select: Box::new(SelectTable::Table(name, alias, indexed)),
                                joins: self.parse_joined_tables()?,
                            })
                        }
                    },
                }
            }
            TokenType::TK_LP => {
                self.eat_assert(&[TokenType::TK_LP]);
                match self.peek_no_eof()?.token_type.unwrap() {
                    TokenType::TK_SELECT | TokenType::TK_WITH | TokenType::TK_VALUES => {
                        let select = self.parse_select()?;
                        self.eat_expect(&[TokenType::TK_RP])?;
                        let alias = self.parse_as()?;
                        Ok(FromClause {
                            select: Box::new(SelectTable::Select(select, alias)),
                            joins: self.parse_joined_tables()?,
                        })
                    }
                    _ => {
                        let fr = self.parse_from_clause()?;
                        self.eat_expect(&[TokenType::TK_RP])?;
                        let alias = self.parse_as()?;
                        Ok(FromClause {
                            select: Box::new(SelectTable::Sub(fr, alias)),
                            joins: self.parse_joined_tables()?,
                        })
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    fn parse_from_clause_opt(&mut self) -> Result<Option<FromClause>, Error> {
        match self.peek()? {
            None => return Ok(None),
            Some(tok) if tok.token_type == Some(TokenType::TK_FROM) => {
                self.eat_assert(&[TokenType::TK_FROM]);
            }
            _ => return Ok(None),
        }

        Ok(Some(self.parse_from_clause()?))
    }

    fn parse_select_column(&mut self) -> Result<ResultColumn, Error> {
        match self.peek_no_eof()?.token_type.unwrap().fallback_id_if_ok() {
            TokenType::TK_STAR => {
                self.eat_assert(&[TokenType::TK_STAR]);
                Ok(ResultColumn::Star)
            }
            tt => {
                // dot STAR case
                if tt == TokenType::TK_ID
                    || tt == TokenType::TK_STRING
                    || tt == TokenType::TK_INDEXED
                    || tt == TokenType::TK_JOIN_KW
                {
                    if let Ok(res) = self.mark(|p| -> Result<ResultColumn, Error> {
                        let name = p.parse_nm();
                        p.eat_expect(&[TokenType::TK_DOT])?;
                        p.eat_expect(&[TokenType::TK_STAR])?;
                        Ok(ResultColumn::TableStar(name))
                    }) {
                        return Ok(res);
                    }
                }

                let expr = self.parse_expr(0)?;
                let alias = self.parse_as()?;
                Ok(ResultColumn::Expr(expr, alias))
            }
        }
    }

    fn parse_select_columns(&mut self) -> Result<Vec<ResultColumn>, Error> {
        let mut result = vec![self.parse_select_column()?];

        loop {
            if let Some(tok) = self.peek()? {
                if tok.token_type == Some(TokenType::TK_COMMA) {
                    self.eat_assert(&[TokenType::TK_COMMA]);
                } else {
                    break;
                }
            } else {
                break;
            }

            result.push(self.parse_select_column()?);
        }

        Ok(result)
    }

    fn parse_nexpr_list(&mut self) -> Result<Vec<Box<Expr>>, Error> {
        let mut result = vec![self.parse_expr(0)?];
        loop {
            if let Some(tok) = self.peek()? {
                if tok.token_type == Some(TokenType::TK_COMMA) {
                    self.eat_assert(&[TokenType::TK_COMMA]);
                } else {
                    break;
                }
            } else {
                break;
            }

            result.push(self.parse_expr(0)?);
        }

        Ok(result)
    }

    fn parse_one_select(&mut self) -> Result<OneSelect, Error> {
        let tok = self.eat_expect(&[TokenType::TK_SELECT, TokenType::TK_VALUES])?;
        match tok.token_type.unwrap() {
            TokenType::TK_SELECT => {
                let distinct = self.parse_distinct()?;
                let collist = self.parse_select_columns()?;
                let from = self.parse_from_clause_opt()?;
                let where_clause = self.parse_where()?;
                let group_by = self.parse_group_by()?;
                let window_clause = self.parse_window_clause()?;
                Ok(OneSelect::Select {
                    distinctness: distinct,
                    columns: collist,
                    from,
                    where_clause,
                    group_by,
                    window_clause,
                })
            }
            TokenType::TK_VALUES => {
                self.eat_expect(&[TokenType::TK_LP])?;
                let mut values = vec![self.parse_nexpr_list()?];
                self.eat_expect(&[TokenType::TK_RP])?;

                loop {
                    if let Some(tok) = self.peek()? {
                        if tok.token_type == Some(TokenType::TK_COMMA) {
                            self.eat_assert(&[TokenType::TK_COMMA]);
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }

                    self.eat_expect(&[TokenType::TK_LP])?;
                    values.push(self.parse_nexpr_list()?);
                    self.eat_expect(&[TokenType::TK_RP])?;
                }

                Ok(OneSelect::Values(values))
            }
            _ => unreachable!(),
        }
    }

    fn parse_select_body(&mut self) -> Result<SelectBody, Error> {
        let select = self.parse_one_select()?;
        let mut compounds = vec![];
        loop {
            let op = match self.peek()? {
                Some(tok) => match tok.token_type.unwrap() {
                    TokenType::TK_UNION => {
                        self.eat_assert(&[TokenType::TK_UNION]);
                        if self.peek_no_eof()?.token_type == Some(TokenType::TK_ALL) {
                            self.eat_assert(&[TokenType::TK_ALL]);
                            CompoundOperator::UnionAll
                        } else {
                            CompoundOperator::Union
                        }
                    }
                    TokenType::TK_EXCEPT => {
                        self.eat_assert(&[TokenType::TK_EXCEPT]);
                        CompoundOperator::Except
                    }
                    TokenType::TK_INTERSECT => {
                        self.eat_assert(&[TokenType::TK_INTERSECT]);
                        CompoundOperator::Intersect
                    }
                    _ => break,
                },
                None => break,
            };

            compounds.push(CompoundSelect {
                operator: op,
                select: self.parse_one_select()?,
            });
        }

        Ok(SelectBody { select, compounds })
    }

    fn parse_sorted_column(&mut self) -> Result<SortedColumn, Error> {
        let expr = self.parse_expr(0)?;
        let sort_order = self.parse_sort_order()?;

        let nulls = match self.peek()? {
            Some(tok) if tok.token_type == Some(TokenType::TK_NULLS) => {
                self.eat_assert(&[TokenType::TK_NULLS]);
                let tok = self.eat_expect(&[TokenType::TK_FIRST, TokenType::TK_LAST])?;
                match tok.token_type.unwrap() {
                    TokenType::TK_FIRST => Some(NullsOrder::First),
                    TokenType::TK_LAST => Some(NullsOrder::Last),
                    _ => unreachable!(),
                }
            }
            _ => None,
        };

        Ok(SortedColumn {
            expr,
            order: sort_order,
            nulls,
        })
    }

    fn parse_order_by(&mut self) -> Result<Vec<SortedColumn>, Error> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TokenType::TK_ORDER) {
                self.eat_assert(&[TokenType::TK_ORDER]);
            } else {
                return Ok(vec![]);
            }
        } else {
            return Ok(vec![]);
        }

        self.eat_expect(&[TokenType::TK_BY])?;
        let mut columns = vec![self.parse_sorted_column()?];
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TokenType::TK_COMMA) {
                self.eat_assert(&[TokenType::TK_COMMA]);
                loop {
                    columns.push(self.parse_sorted_column()?);
                    match self.peek_no_eof()?.token_type.unwrap() {
                        TokenType::TK_COMMA => {
                            self.eat_assert(&[TokenType::TK_COMMA]);
                        }
                        _ => break,
                    };
                }
            }
        }

        Ok(columns)
    }

    fn parse_limit(&mut self) -> Result<Option<Limit>, Error> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TokenType::TK_LIMIT) {
                self.eat_assert(&[TokenType::TK_LIMIT]);
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        let limit = self.parse_expr(0)?;
        let offset = match self.peek()? {
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_OFFSET | TokenType::TK_COMMA => {
                    self.eat_assert(&[TokenType::TK_OFFSET, TokenType::TK_COMMA]);
                    Some(self.parse_expr(0)?)
                }
                _ => None,
            },
            _ => None,
        };

        Ok(Some(Limit {
            expr: limit,
            offset,
        }))
    }

    fn parse_select_without_cte(&mut self, with: Option<With>) -> Result<Select, Error> {
        let body = self.parse_select_body()?;
        let order_by = self.parse_order_by()?;
        let limit = self.parse_limit()?;
        Ok(Select {
            with,
            body,
            order_by,
            limit,
        })
    }

    fn parse_select(&mut self) -> Result<Select, Error> {
        let with = self.parse_with()?;
        self.parse_select_without_cte(with)
    }

    fn parse_create_table_args(&mut self) -> Result<CreateTableBody, Error> {
        let tok = self.eat_expect(&[TokenType::TK_LP, TokenType::TK_AS])?;
        match tok.token_type.unwrap() {
            TokenType::TK_AS => Ok(CreateTableBody::AsSelect(self.parse_select()?)),
            TokenType::TK_LP => todo!(),
            _ => unreachable!(),
        }
    }

    fn parse_create_table(&mut self, temporary: bool) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_TABLE]);
        let if_not_exists = self.parse_if_not_exists()?;
        let tbl_name = self.parse_fullname(false)?;
        let body = self.parse_create_table_args()?;
        Ok(Stmt::CreateTable {
            temporary,
            if_not_exists,
            tbl_name,
            body,
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
            (
                b"RELEASE SAVEPOINT ABORT".as_slice(),
                vec![Cmd::Stmt(Stmt::Release {
                    name: Name::Ident("ABORT".to_string()),
                })],
            ),
            // test exprs
            (
                b"SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: None,
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
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
