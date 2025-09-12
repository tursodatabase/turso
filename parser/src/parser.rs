use crate::ast::{
    check::ColumnCount, AlterTable, AlterTableBody, As, Cmd, ColumnConstraint, ColumnDefinition,
    CommonTableExpr, CompoundOperator, CompoundSelect, CreateTableBody, CreateVirtualTable,
    DeferSubclause, Distinctness, Expr, ForeignKeyClause, FrameBound, FrameClause, FrameExclude,
    FrameMode, FromClause, FunctionTail, GroupBy, Indexed, IndexedColumn, InitDeferredPred,
    InsertBody, JoinConstraint, JoinOperator, JoinType, JoinedSelectTable, LikeOperator, Limit,
    Literal, Materialized, Name, NamedColumnConstraint, NamedTableConstraint, NullsOrder,
    OneSelect, Operator, Over, PragmaBody, PragmaValue, QualifiedName, RefAct, RefArg, ResolveType,
    ResultColumn, Select, SelectBody, SelectTable, Set, SortOrder, SortedColumn, Stmt,
    TableConstraint, TableOptions, TransactionType, TriggerCmd, TriggerEvent, TriggerTime, Type,
    TypeSize, UnaryOperator, Update, Upsert, UpsertDo, UpsertIndex, Window, WindowDef, With,
};
use crate::error::Error;
use crate::lexer::{Lexer, Token};
use crate::token::TokenType::{self, *};
use crate::Result;
use turso_macros::match_ignore_ascii_case;

macro_rules! peek_expect {
    ( $parser:expr, $( $x:ident ),* $(,)?) => {
        {
            let token = $parser.peek_no_eof()?;
            match token.token_type.unwrap() {
                $($x => token,)*
                tt => {
                    // handle fallback TK_ID
                    match (TK_ID, tt.fallback_id_if_ok()) {
                        $(($x, TK_ID) => token,)*
                        _ => {
                            return Err(Error::ParseUnexpectedToken {
                                parsed_offset: ($parser.offset(), 1).into(),
                                expected: &[
                                    $($x,)*
                                ],
                                got: tt,
                            })
                        }
                    }
                }
            }
        }
    };
}

macro_rules! eat_assert {
    ( $parser:expr, $( $x:ident ),* $(,)?) => {
        {
            let token = $parser.eat().unwrap().unwrap();

            #[cfg(debug_assertions)]
            match token.token_type.unwrap() {
                $($x => token,)*
                tt => {
                    // handle fallback TK_ID
                    match (TK_ID, tt.fallback_id_if_ok()) {
                        $(($x, TK_ID) => token,)*
                        _ => {
                            panic!(
                                "Expected token {:?}, got {:?}",
                                &[ $($x,)* ],
                                tt
                            );
                        }
                    }
                }
            }

            #[cfg(not(debug_assertions))]
            token // in release mode, we assume the caller has checked the token type
        }
    };
}

macro_rules! eat_expect {
    ( $parser:expr, $( $x:ident ),* $(,)?) => {
        {
            peek_expect!($parser, $( $x ),*);
            eat_assert!($parser, $( $x ),*)
        }
    };
}

#[inline(always)]
fn from_bytes_as_str(bytes: &[u8]) -> &str {
    unsafe { str::from_utf8_unchecked(bytes) }
}

#[inline(always)]
fn from_bytes(bytes: &[u8]) -> String {
    unsafe { str::from_utf8_unchecked(bytes).to_owned() }
}

#[inline]
fn join_type_from_bytes(s: &[u8]) -> Result<JoinType> {
    match_ignore_ascii_case!(match s {
        b"CROSS" => Ok(JoinType::INNER | JoinType::CROSS),
        b"FULL" => Ok(JoinType::LEFT | JoinType::RIGHT | JoinType::OUTER),
        b"INNER" => Ok(JoinType::INNER),
        b"LEFT" => Ok(JoinType::LEFT | JoinType::OUTER),
        b"NATURAL" => Ok(JoinType::NATURAL),
        b"RIGHT" => Ok(JoinType::RIGHT | JoinType::OUTER),
        b"OUTER" => Ok(JoinType::OUTER),
        _ => Err(Error::Custom(format!(
            "unsupported JOIN type: {:?}",
            str::from_utf8(s)
        ))),
    })
}

#[inline]
fn new_join_type(n0: &[u8], n1: Option<&[u8]>, n2: Option<&[u8]>) -> Result<JoinType> {
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
    type Item = Result<Cmd>;

    #[inline]
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
                value: &input[..0],
                token_type: None,
            },
        }
    }

    #[inline(always)]
    pub fn offset(&self) -> usize {
        if !self.peekable {
            // not peekable means current token already consumed
            // so just take lexer offset
            self.lexer.offset
        } else {
            self.lexer.offset - self.current_token.value.len()
        }
    }

    // entrypoint of parsing
    pub fn next_cmd(&mut self) -> Result<Option<Cmd>> {
        // consumes prefix SEMI
        while let Some(token) = self.peek()? {
            if token.token_type == Some(TK_SEMI) {
                eat_assert!(self, TK_SEMI);
            } else {
                break;
            }
        }

        let result = match self.peek()? {
            None => None, // EOF
            Some(token) => match token.token_type.unwrap() {
                TK_EXPLAIN => {
                    eat_assert!(self, TK_EXPLAIN);

                    if self.peek_no_eof()?.token_type == Some(TK_QUERY) {
                        eat_assert!(self, TK_QUERY);
                        eat_expect!(self, TK_PLAN);
                        Some(Cmd::ExplainQueryPlan(self.parse_stmt()?))
                    } else {
                        Some(Cmd::Explain(self.parse_stmt()?))
                    }
                }
                _ => Some(Cmd::Stmt(self.parse_stmt()?)),
            },
        };

        let mut found_semi = false;
        loop {
            match self.peek()? {
                None => break,
                Some(token) if token.token_type == Some(TK_SEMI) => {
                    found_semi = true;
                    eat_assert!(self, TK_SEMI);
                }
                Some(token) => {
                    if !found_semi {
                        let tt = token.token_type.unwrap();
                        return Err(Error::ParseUnexpectedToken {
                            parsed_offset: (self.offset(), 1).into(),
                            expected: &[TK_SEMI],
                            got: tt,
                        });
                    }

                    break;
                }
            }
        }

        Ok(result)
    }

    #[inline(always)]
    fn consume_lexer_without_whitespaces_or_comments(&mut self) -> Option<Result<Token<'a>>> {
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

    fn next_token(&mut self) -> Result<Option<&Token<'a>>> {
        debug_assert!(!self.peekable);
        let mut next = self.consume_lexer_without_whitespaces_or_comments();

        fn get_token(tt: TokenType) -> TokenType {
            match tt {
                TK_ID | TK_STRING | TK_JOIN_KW | TK_UNION | TK_EXCEPT | TK_INTERSECT
                | TK_GENERATED | TK_WITHOUT | TK_COLUMNKW | TK_WINDOW | TK_FILTER | TK_OVER => {
                    TK_ID
                }
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
             **
             ** UNION is a keyword if:
             **
             **   * the next token is TK_ALL|TK_SELECT|TK_VALUES.
             **
             ** EXCEPT is a keyword if:
             **
             **   * the next token is TK_SELECT|TK_VALUES.
             **
             ** INTERSECT is a keyword if:
             **
             **   * the next token is TK_SELECT|TK_VALUES.
             **
             ** COLUMNKW is a keyword if:
             **
             **   * the previous token is TK_ADD|TK_RENAME|TK_DROP.
             **
             ** GENERATED is a keyword if:
             **
             **   * the next token is TK_ALWAYS.
             **   * the token after than one is TK_AS.
             **
             ** WITHOUT is a keyword if:
             **
             **   * the previous token is TK_RP|TK_COMMA.
             **   * the next token is TK_ID.
             */
            match tok.token_type.unwrap() {
                TK_WINDOW => {
                    let can_be_window = self.try_parse(|p| {
                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => return Ok(false),
                            Some(tok) => match get_token(tok?.token_type.unwrap()) {
                                TK_ID => {}
                                _ => return Ok(false),
                            },
                        }

                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => Ok(false),
                            Some(tok) => match tok?.token_type.unwrap() {
                                TK_AS => Ok(true),
                                _ => Ok(false),
                            },
                        }
                    })?;

                    if !can_be_window {
                        tok.token_type = Some(TK_ID);
                    }
                }
                TK_OVER => {
                    let prev_tt = self.current_token.token_type.unwrap_or(TK_EOF);
                    let can_be_over = {
                        if prev_tt == TK_RP {
                            self.try_parse(|p| {
                                match p.consume_lexer_without_whitespaces_or_comments() {
                                    None => Ok(false),
                                    Some(tok) => match get_token(tok?.token_type.unwrap()) {
                                        TK_LP | TK_ID => Ok(true),
                                        _ => Ok(false),
                                    },
                                }
                            })?
                        } else {
                            false
                        }
                    };

                    if !can_be_over {
                        tok.token_type = Some(TK_ID);
                    }
                }
                TK_FILTER => {
                    let prev_tt = self.current_token.token_type.unwrap_or(TK_EOF);
                    let can_be_filter = {
                        if prev_tt == TK_RP {
                            self.try_parse(|p| {
                                match p.consume_lexer_without_whitespaces_or_comments() {
                                    None => Ok(false),
                                    Some(tok) => match tok?.token_type.unwrap() {
                                        TK_LP => Ok(true),
                                        _ => Ok(false),
                                    },
                                }
                            })?
                        } else {
                            false
                        }
                    };

                    if !can_be_filter {
                        tok.token_type = Some(TK_ID);
                    }
                }
                TK_UNION => {
                    let can_be_union = self.try_parse(|p| {
                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => Ok(false),
                            Some(tok) => match tok?.token_type.unwrap() {
                                TK_ALL | TK_SELECT | TK_VALUES => Ok(true),
                                _ => Ok(false),
                            },
                        }
                    })?;

                    if !can_be_union {
                        tok.token_type = Some(TK_ID);
                    }
                }
                TK_EXCEPT | TK_INTERSECT => {
                    let can_be_except = self.try_parse(|p| {
                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => Ok(false),
                            Some(tok) => match tok?.token_type.unwrap() {
                                TK_SELECT | TK_VALUES => Ok(true),
                                _ => Ok(false),
                            },
                        }
                    })?;

                    if !can_be_except {
                        tok.token_type = Some(TK_ID);
                    }
                }
                TK_COLUMNKW => {
                    let prev_tt = self.current_token.token_type.unwrap_or(TK_EOF);
                    let can_be_columnkw =
                        matches!(prev_tt, TK_ADD | TK_RENAME | TK_DROP | TK_ALTER);

                    if !can_be_columnkw {
                        tok.token_type = Some(TK_ID);
                    }
                }
                TK_GENERATED => {
                    let can_be_generated = self.try_parse(|p| {
                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => return Ok(false),
                            Some(tok) => match tok?.token_type.unwrap() {
                                TK_ALWAYS => {}
                                _ => return Ok(false),
                            },
                        }

                        match p.consume_lexer_without_whitespaces_or_comments() {
                            None => Ok(false),
                            Some(tok) => match tok?.token_type.unwrap() {
                                TK_AS => Ok(true),
                                _ => Ok(false),
                            },
                        }
                    })?;

                    if !can_be_generated {
                        tok.token_type = Some(TK_ID);
                    }
                }
                TK_WITHOUT => {
                    let prev_tt = self.current_token.token_type.unwrap_or(TK_EOF);
                    let can_be_without = match prev_tt {
                        TK_RP | TK_COMMA => self.try_parse(|p| {
                            match p.consume_lexer_without_whitespaces_or_comments() {
                                None => Ok(false),
                                Some(tok) => match get_token(tok?.token_type.unwrap()) {
                                    TK_ID => Ok(true),
                                    _ => Ok(false),
                                },
                            }
                        })?,
                        _ => false,
                    };

                    if !can_be_without {
                        tok.token_type = Some(TK_ID);
                    }
                }
                _ => {}
            }
        }

        match next {
            None => Ok(None), // EOF
            Some(Ok(tok)) => {
                self.current_token = tok;
                self.peekable = true;
                Ok(Some(&self.current_token))
            }
            Some(Err(err)) => Err(err),
        }
    }

    #[inline]
    fn mark<F, R>(&mut self, exc: F) -> Result<R>
    where
        F: FnOnce(&mut Self) -> Result<R>,
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

    #[inline]
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
    #[inline]
    fn eat(&mut self) -> Result<Option<Token<'a>>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => {
                let result = tok.clone();
                self.peekable = false; // Clear the peek mark after consuming
                Ok(Some(result))
            }
        }
    }

    /// Peek at the next token without consuming it
    #[inline]
    fn peek(&mut self) -> Result<Option<&Token<'a>>> {
        if self.peekable {
            return Ok(Some(&self.current_token));
        }

        self.next_token()
    }

    #[inline]
    fn eat_no_eof(&mut self) -> Result<Token<'a>> {
        match self.eat()? {
            None => Err(Error::ParseUnexpectedEOF),
            Some(token) => Ok(token),
        }
    }

    #[inline]
    fn peek_no_eof(&mut self) -> Result<&Token<'a>> {
        match self.peek()? {
            None => Err(Error::ParseUnexpectedEOF),
            Some(token) => Ok(token),
        }
    }

    fn parse_stmt(&mut self) -> Result<Stmt> {
        let tok = peek_expect!(
            self,
            TK_BEGIN,
            TK_COMMIT,
            TK_END,
            TK_ROLLBACK,
            TK_SAVEPOINT,
            TK_RELEASE,
            TK_CREATE,
            TK_SELECT,
            TK_VALUES,
            TK_WITH,
            TK_ANALYZE,
            TK_ATTACH,
            TK_DETACH,
            TK_PRAGMA,
            TK_VACUUM,
            TK_ALTER,
            TK_DELETE,
            TK_DROP,
            TK_INSERT,
            TK_REPLACE,
            TK_UPDATE,
            TK_REINDEX
        );

        match tok.token_type.unwrap() {
            TK_BEGIN => self.parse_begin(),
            TK_COMMIT | TK_END => self.parse_commit(),
            TK_ROLLBACK => self.parse_rollback(),
            TK_SAVEPOINT => self.parse_savepoint(),
            TK_RELEASE => self.parse_release(),
            TK_CREATE => self.parse_create_stmt(),
            TK_SELECT | TK_VALUES => Ok(Stmt::Select(self.parse_select()?)),
            TK_WITH => self.parse_with_stmt(),
            TK_ANALYZE => self.parse_analyze(),
            TK_ATTACH => self.parse_attach(),
            TK_DETACH => self.parse_detach(),
            TK_PRAGMA => self.parse_pragma(),
            TK_VACUUM => self.parse_vacuum(),
            TK_ALTER => self.parse_alter(),
            TK_DELETE => self.parse_delete(),
            TK_DROP => self.parse_drop_stmt(),
            TK_INSERT | TK_REPLACE => self.parse_insert(),
            TK_UPDATE => self.parse_update(),
            TK_REINDEX => self.parse_reindex(),
            _ => unreachable!(),
        }
    }

    fn parse_nm(&mut self) -> Result<Name> {
        let tok = eat_expect!(self, TK_ID, TK_STRING, TK_INDEXED, TK_JOIN_KW);

        let first_char = tok.value[0]; // no need to check empty
        match first_char {
            b'[' | b'\'' | b'`' | b'"' => Ok(Name::Quoted(from_bytes(tok.value))),
            _ => Ok(Name::Ident(from_bytes(tok.value))),
        }
    }

    fn parse_transopt(&mut self) -> Result<Option<Name>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TK_TRANSACTION => {
                    eat_assert!(self, TK_TRANSACTION);
                    match self.peek()? {
                        Some(tok) => match tok.token_type.unwrap() {
                            TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW => {
                                Ok(Some(self.parse_nm()?))
                            }
                            _ => Ok(None),
                        },
                        _ => Ok(None),
                    }
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_begin(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_BEGIN);

        let transtype = match self.peek()? {
            None => None,
            Some(tok) => match tok.token_type.unwrap() {
                TK_DEFERRED => {
                    eat_assert!(self, TK_DEFERRED);
                    Some(TransactionType::Deferred)
                }
                TK_IMMEDIATE => {
                    eat_assert!(self, TK_IMMEDIATE);
                    Some(TransactionType::Immediate)
                }
                TK_EXCLUSIVE => {
                    eat_assert!(self, TK_EXCLUSIVE);
                    Some(TransactionType::Exclusive)
                }
                TK_CONCURRENT => {
                    eat_assert!(self, TK_CONCURRENT);
                    Some(TransactionType::Concurrent)
                }
                _ => None,
            },
        };

        Ok(Stmt::Begin {
            typ: transtype,
            name: self.parse_transopt()?,
        })
    }

    fn parse_commit(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_COMMIT, TK_END);
        Ok(Stmt::Commit {
            name: self.parse_transopt()?,
        })
    }

    fn parse_rollback(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_ROLLBACK);

        let tx_name = self.parse_transopt()?;

        let savepoint_name = match self.peek()? {
            None => None,
            Some(tok) => {
                if tok.token_type == Some(TK_TO) {
                    eat_assert!(self, TK_TO);

                    if self.peek_no_eof()?.token_type == Some(TK_SAVEPOINT) {
                        eat_assert!(self, TK_SAVEPOINT);
                    }

                    Some(self.parse_nm()?)
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

    fn parse_savepoint(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_SAVEPOINT);
        Ok(Stmt::Savepoint {
            name: self.parse_nm()?,
        })
    }

    fn parse_release(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_RELEASE);

        if self.peek_no_eof()?.token_type == Some(TK_SAVEPOINT) {
            eat_assert!(self, TK_SAVEPOINT);
        }

        Ok(Stmt::Release {
            name: self.parse_nm()?,
        })
    }

    fn parse_create_view(&mut self, temporary: bool) -> Result<Stmt> {
        eat_assert!(self, TK_VIEW);
        let if_not_exists = self.parse_if_not_exists()?;
        let view_name = self.parse_fullname(false)?;
        let columns = self.parse_eid_list(true)?;
        eat_expect!(self, TK_AS);
        let select = self.parse_select()?;
        if let (ColumnCount::Fixed(n), false) = (select.column_count(), columns.is_empty()) {
            if n != columns.len() {
                return Err(Error::Custom(format!(
                    "expected {} columns for {} but got {}",
                    columns.len(),
                    view_name,
                    n
                )));
            }
        }

        Ok(Stmt::CreateView {
            temporary,
            if_not_exists,
            view_name,
            columns,
            select,
        })
    }

    fn parse_create_materialized_view(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_MATERIALIZED);
        eat_assert!(self, TK_VIEW);
        let if_not_exists = self.parse_if_not_exists()?;
        let view_name = self.parse_fullname(false)?;
        let columns = self.parse_eid_list(false)?;
        eat_expect!(self, TK_AS);
        let select = self.parse_select()?;
        Ok(Stmt::CreateMaterializedView {
            if_not_exists,
            view_name,
            columns,
            select,
        })
    }

    fn parse_vtab_arg(&mut self) -> Result<String> {
        let start_idx = self.offset();

        loop {
            match self.peek_no_eof()?.token_type.unwrap() {
                TK_LP => {
                    let mut lp_count: usize = 0;
                    loop {
                        let tok = self.eat_no_eof()?;
                        match tok.token_type.unwrap() {
                            TK_LP => {
                                lp_count += 1;
                            }
                            TK_RP => {
                                lp_count -= 1; // FIXME: no need to check underflow
                                if lp_count == 0 {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                TK_COMMA | TK_RP => break,
                _ => {
                    self.eat_no_eof()?;
                }
            }
        }

        // minus 1 because lexer already consumed TK_COMMA or TK_RP
        let end_idx = self.offset();
        if start_idx == end_idx {
            return Err(Error::Custom("unexpected COMMA in vtab args".to_owned()));
        }

        Ok(from_bytes(&self.lexer.input[start_idx..end_idx]))
    }

    fn parse_create_virtual(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_VIRTUAL);
        eat_expect!(self, TK_TABLE);
        let if_not_exists = self.parse_if_not_exists()?;
        let tbl_name = self.parse_fullname(false)?;
        eat_expect!(self, TK_USING);
        let module_name = self.parse_nm()?;
        let args = match self.peek()? {
            Some(tok) => match tok.token_type.unwrap() {
                TK_LP => {
                    eat_assert!(self, TK_LP);
                    let mut result = vec![];
                    loop {
                        if self.peek_no_eof()?.token_type == Some(TK_RP) {
                            break; // handle empty args case
                        }

                        result.push(self.parse_vtab_arg()?);
                        let tok = peek_expect!(self, TK_COMMA, TK_RP);
                        match tok.token_type.unwrap() {
                            TK_COMMA => {
                                eat_assert!(self, TK_COMMA);
                            }
                            TK_RP => break,
                            _ => unreachable!(),
                        }
                    }

                    eat_assert!(self, TK_RP); // already checks in loop
                    result
                }
                _ => vec![],
            },
            _ => vec![],
        };

        Ok(Stmt::CreateVirtualTable(CreateVirtualTable {
            if_not_exists,
            tbl_name,
            module_name,
            args,
        }))
    }

    fn parse_create_stmt(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_CREATE);
        let mut first_tok = peek_expect!(
            self,
            TK_TEMP,
            TK_TABLE,
            TK_VIRTUAL,
            TK_VIEW,
            TK_INDEX,
            TK_UNIQUE,
            TK_TRIGGER,
            TK_MATERIALIZED
        );
        let mut temp = false;
        if first_tok.token_type == Some(TK_TEMP) {
            eat_assert!(self, TK_TEMP);
            temp = true;
            first_tok = peek_expect!(self, TK_TABLE, TK_VIEW, TK_TRIGGER);
        }

        match first_tok.token_type.unwrap() {
            TK_TABLE => self.parse_create_table(temp),
            TK_VIEW => self.parse_create_view(temp),
            TK_MATERIALIZED => self.parse_create_materialized_view(),
            TK_TRIGGER => self.parse_create_trigger(temp),
            TK_VIRTUAL => self.parse_create_virtual(),
            TK_INDEX | TK_UNIQUE => self.parse_create_index(),
            _ => unreachable!(),
        }
    }

    fn parse_with_stmt(&mut self) -> Result<Stmt> {
        let with = self.parse_with()?;
        debug_assert!(with.is_some());
        let first_tok =
            peek_expect!(self, TK_SELECT, TK_VALUES, TK_UPDATE, TK_DELETE, TK_INSERT, TK_REPLACE,);

        match first_tok.token_type.unwrap() {
            TK_SELECT | TK_VALUES => Ok(Stmt::Select(self.parse_select_without_cte(with)?)),
            TK_UPDATE => self.parse_update_without_cte(with),
            TK_DELETE => self.parse_delete_without_cte(with),
            TK_INSERT | TK_REPLACE => self.parse_insert_without_cte(with),
            _ => unreachable!(),
        }
    }

    fn parse_if_not_exists(&mut self) -> Result<bool> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TK_IF) {
                eat_assert!(self, TK_IF);
            } else {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }

        eat_expect!(self, TK_NOT);
        eat_expect!(self, TK_EXISTS);
        Ok(true)
    }

    fn parse_fullname(&mut self, allow_alias: bool) -> Result<QualifiedName> {
        let first_name = self.parse_nm()?;

        let secone_name = if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TK_DOT) {
                eat_assert!(self, TK_DOT);
                Some(self.parse_nm()?)
            } else {
                None
            }
        } else {
            None
        };

        let alias_name = if allow_alias {
            if let Some(tok) = self.peek()? {
                if tok.token_type == Some(TK_AS) {
                    eat_assert!(self, TK_AS);
                    Some(self.parse_nm()?)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(secone_name) = secone_name {
            Ok(QualifiedName {
                db_name: Some(first_name),
                name: secone_name,
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

    fn parse_signed(&mut self) -> Result<Box<Expr>> {
        peek_expect!(self, TK_FLOAT, TK_INTEGER, TK_PLUS, TK_MINUS);

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

    fn parse_type(&mut self) -> Result<Option<Type>> {
        let mut type_name = if let Some(tok) = self.peek()? {
            match tok.token_type.unwrap().fallback_id_if_ok() {
                TK_ID | TK_STRING => {
                    let tok = eat_assert!(self, TK_ID, TK_STRING);
                    from_bytes(tok.value)
                }
                _ => return Ok(None),
            }
        } else {
            return Ok(None);
        };

        while let Some(tok) = self.peek()? {
            match tok.token_type.unwrap().fallback_id_if_ok() {
                TK_ID | TK_STRING => {
                    let tok = eat_assert!(self, TK_ID, TK_STRING);
                    type_name.push(' ');
                    type_name.push_str(from_bytes_as_str(tok.value));
                }
                _ => break,
            }
        }

        let size = if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TK_LP) {
                eat_assert!(self, TK_LP);
                let first_size = self.parse_signed()?;
                let tok = eat_expect!(self, TK_RP, TK_COMMA);
                match tok.token_type.unwrap() {
                    TK_RP => Some(TypeSize::MaxSize(first_size)),
                    TK_COMMA => {
                        let second_size = self.parse_signed()?;
                        eat_expect!(self, TK_RP);
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
    fn current_token_precedence(&mut self) -> Result<Option<u8>> {
        let tok = self.peek()?;
        if tok.is_none() {
            return Ok(None);
        }

        match tok.unwrap().token_type.unwrap() {
            TK_OR => Ok(Some(0)),
            TK_AND => Ok(Some(1)),
            TK_NOT => Ok(Some(3)), // NOT is 3 because of binary operator
            TK_EQ | TK_NE | TK_IS | TK_BETWEEN | TK_IN | TK_MATCH | TK_LIKE_KW | TK_ISNULL
            | TK_NOTNULL => Ok(Some(3)),
            TK_LT | TK_GT | TK_LE | TK_GE => Ok(Some(4)),
            TK_ESCAPE => Ok(None), // ESCAPE will be consumed after parsing MATCH|LIKE_KW
            TK_BITAND | TK_BITOR | TK_LSHIFT | TK_RSHIFT => Ok(Some(6)),
            TK_PLUS | TK_MINUS => Ok(Some(7)),
            TK_STAR | TK_SLASH | TK_REM => Ok(Some(8)),
            TK_CONCAT | TK_PTR => Ok(Some(9)),
            TK_COLLATE => Ok(Some(10)),
            // no need 11 because its for unary operators
            _ => Ok(None),
        }
    }

    fn parse_distinct(&mut self) -> Result<Option<Distinctness>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TK_DISTINCT => {
                    eat_assert!(self, TK_DISTINCT);
                    Ok(Some(Distinctness::Distinct))
                }
                TK_ALL => {
                    eat_assert!(self, TK_ALL);
                    Ok(Some(Distinctness::All))
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_filter_clause(&mut self) -> Result<Option<Box<Expr>>> {
        match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TK_FILTER => {
                    eat_assert!(self, TK_FILTER);
                }
                _ => return Ok(None),
            },
        }

        eat_expect!(self, TK_LP);
        eat_expect!(self, TK_WHERE);
        let expr = self.parse_expr(0)?;
        eat_expect!(self, TK_RP);
        Ok(Some(expr))
    }

    fn parse_frame_opt(&mut self) -> Result<Option<FrameClause>> {
        let range_or_rows = match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TK_RANGE => {
                    eat_assert!(self, TK_RANGE);
                    FrameMode::Range
                }
                TK_ROWS => {
                    eat_assert!(self, TK_ROWS);
                    FrameMode::Rows
                }
                TK_GROUPS => {
                    eat_assert!(self, TK_GROUPS);
                    FrameMode::Groups
                }
                _ => return Ok(None),
            },
        };

        let has_end = match self.peek_no_eof()?.token_type.unwrap() {
            TK_BETWEEN => {
                eat_assert!(self, TK_BETWEEN);
                true
            }
            _ => false,
        };

        let start = match self.peek_no_eof()?.token_type.unwrap() {
            TK_UNBOUNDED => {
                eat_assert!(self, TK_UNBOUNDED);
                eat_expect!(self, TK_PRECEDING);
                FrameBound::UnboundedPreceding
            }
            TK_CURRENT => {
                eat_assert!(self, TK_CURRENT);
                eat_expect!(self, TK_ROW);
                FrameBound::CurrentRow
            }
            _ => {
                let expr = self.parse_expr(0)?;
                let tok = eat_expect!(self, TK_PRECEDING, TK_FOLLOWING);
                match tok.token_type.unwrap() {
                    TK_PRECEDING => FrameBound::Preceding(expr),
                    TK_FOLLOWING => FrameBound::Following(expr),
                    _ => unreachable!(),
                }
            }
        };

        let end = if has_end {
            eat_expect!(self, TK_AND);

            Some(match self.peek_no_eof()?.token_type.unwrap() {
                TK_UNBOUNDED => {
                    eat_assert!(self, TK_UNBOUNDED);
                    eat_expect!(self, TK_FOLLOWING);
                    FrameBound::UnboundedFollowing
                }
                TK_CURRENT => {
                    eat_assert!(self, TK_CURRENT);
                    eat_expect!(self, TK_ROW);
                    FrameBound::CurrentRow
                }
                _ => {
                    let expr = self.parse_expr(0)?;
                    let tok = eat_expect!(self, TK_PRECEDING, TK_FOLLOWING);
                    match tok.token_type.unwrap() {
                        TK_PRECEDING => FrameBound::Preceding(expr),
                        TK_FOLLOWING => FrameBound::Following(expr),
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
                TK_EXCLUDE => {
                    eat_assert!(self, TK_EXCLUDE);
                    let tok = eat_expect!(self, TK_NO, TK_CURRENT, TK_GROUP, TK_TIES);
                    match tok.token_type.unwrap() {
                        TK_NO => {
                            eat_expect!(self, TK_OTHERS);
                            Some(FrameExclude::NoOthers)
                        }
                        TK_CURRENT => {
                            eat_expect!(self, TK_ROW);
                            Some(FrameExclude::CurrentRow)
                        }
                        TK_GROUP => Some(FrameExclude::Group),
                        TK_TIES => Some(FrameExclude::Ties),
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

    fn parse_window(&mut self) -> Result<Window> {
        let name = match self.peek()? {
            None => None,
            Some(tok) => match tok.token_type.unwrap() {
                TK_PARTITION | TK_ORDER | TK_RANGE | TK_ROWS | TK_GROUPS => None,
                tt => match tt.fallback_id_if_ok() {
                    TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW => Some(self.parse_nm()?),
                    _ => None,
                },
            },
        };

        let partition_by = match self.peek()? {
            Some(tok) if tok.token_type == Some(TK_PARTITION) => {
                eat_assert!(self, TK_PARTITION);
                eat_expect!(self, TK_BY);
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

    fn parse_over_clause(&mut self) -> Result<Option<Over>> {
        match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TK_OVER => {
                    eat_assert!(self, TK_OVER);
                }
                _ => return Ok(None),
            },
        }

        let tok = peek_expect!(self, TK_LP, TK_ID, TK_STRING, TK_INDEXED, TK_JOIN_KW);
        match tok.token_type.unwrap() {
            TK_LP => {
                eat_assert!(self, TK_LP);
                let window = self.parse_window()?;
                eat_expect!(self, TK_RP);
                Ok(Some(Over::Window(window)))
            }
            _ => Ok(Some(Over::Name(self.parse_nm()?))),
        }
    }

    fn parse_filter_over(&mut self) -> Result<FunctionTail> {
        let filter_clause = self.parse_filter_clause()?;
        let over_clause = self.parse_over_clause()?;
        Ok(FunctionTail {
            filter_clause,
            over_clause,
        })
    }

    fn parse_raise_type(&mut self) -> Result<ResolveType> {
        let tok = eat_expect!(self, TK_ROLLBACK, TK_ABORT, TK_FAIL);

        match tok.token_type.unwrap() {
            TK_ROLLBACK => Ok(ResolveType::Rollback),
            TK_ABORT => Ok(ResolveType::Abort),
            TK_FAIL => Ok(ResolveType::Fail),
            _ => unreachable!(),
        }
    }

    fn parse_expr_operand(&mut self) -> Result<Box<Expr>> {
        let tok = peek_expect!(
            self,
            TK_LP,
            TK_CAST,
            TK_CTIME_KW,
            TK_RAISE,
            TK_ID,
            TK_STRING,
            TK_INDEXED,
            TK_JOIN_KW,
            TK_NULL,
            TK_BLOB,
            TK_FLOAT,
            TK_INTEGER,
            TK_VARIABLE,
            TK_NOT,
            TK_BITNOT,
            TK_PLUS,
            TK_MINUS,
            TK_EXISTS,
            TK_CASE,
        );

        match tok.token_type.unwrap() {
            TK_LP => {
                eat_assert!(self, TK_LP);
                match self.peek_no_eof()?.token_type.unwrap() {
                    TK_WITH | TK_SELECT | TK_VALUES => {
                        let select = self.parse_select()?;
                        eat_expect!(self, TK_RP);
                        Ok(Box::new(Expr::Subquery(select)))
                    }
                    _ => {
                        let exprs = self.parse_nexpr_list()?;
                        eat_expect!(self, TK_RP);
                        Ok(Box::new(Expr::Parenthesized(exprs)))
                    }
                }
            }
            TK_NULL => {
                eat_assert!(self, TK_NULL);
                Ok(Box::new(Expr::Literal(Literal::Null)))
            }
            TK_BLOB => {
                let tok = eat_assert!(self, TK_BLOB);
                Ok(Box::new(Expr::Literal(Literal::Blob(from_bytes(
                    tok.value,
                )))))
            }
            TK_FLOAT => {
                let tok = eat_assert!(self, TK_FLOAT);
                Ok(Box::new(Expr::Literal(Literal::Numeric(from_bytes(
                    tok.value,
                )))))
            }
            TK_INTEGER => {
                let tok = eat_assert!(self, TK_INTEGER);
                Ok(Box::new(Expr::Literal(Literal::Numeric(from_bytes(
                    tok.value,
                )))))
            }
            TK_VARIABLE => {
                let tok = eat_assert!(self, TK_VARIABLE);
                Ok(Box::new(Expr::Variable(from_bytes(tok.value))))
            }
            TK_CAST => {
                eat_assert!(self, TK_CAST);
                eat_expect!(self, TK_LP);
                let expr = self.parse_expr(0)?;
                eat_expect!(self, TK_AS);
                let typ = self.parse_type()?;
                eat_expect!(self, TK_RP);
                Ok(Box::new(Expr::Cast {
                    expr,
                    type_name: typ,
                }))
            }
            TK_CTIME_KW => {
                let tok = eat_assert!(self, TK_CTIME_KW);
                match_ignore_ascii_case!(match tok.value {
                    b"CURRENT_DATE" => Ok(Box::new(Expr::Literal(Literal::CurrentDate))),
                    b"CURRENT_TIME" => Ok(Box::new(Expr::Literal(Literal::CurrentTime))),
                    b"CURRENT_TIMESTAMP" => Ok(Box::new(Expr::Literal(Literal::CurrentTimestamp))),
                    _ => unreachable!(),
                })
            }
            TK_NOT => {
                eat_assert!(self, TK_NOT);
                let expr = self.parse_expr(2)?; // NOT precedence is 2
                Ok(Box::new(Expr::Unary(UnaryOperator::Not, expr)))
            }
            TK_BITNOT => {
                eat_assert!(self, TK_BITNOT);
                let expr = self.parse_expr(11)?; // BITNOT precedence is 11
                Ok(Box::new(Expr::Unary(UnaryOperator::BitwiseNot, expr)))
            }
            TK_PLUS => {
                eat_assert!(self, TK_PLUS);
                let expr = self.parse_expr(11)?; // PLUS precedence is 11
                Ok(Box::new(Expr::Unary(UnaryOperator::Positive, expr)))
            }
            TK_MINUS => {
                eat_assert!(self, TK_MINUS);
                let expr = self.parse_expr(11)?; // MINUS precedence is 11
                Ok(Box::new(Expr::Unary(UnaryOperator::Negative, expr)))
            }
            TK_EXISTS => {
                eat_assert!(self, TK_EXISTS);
                eat_expect!(self, TK_LP);
                let select = self.parse_select()?;
                eat_expect!(self, TK_RP);
                Ok(Box::new(Expr::Exists(select)))
            }
            TK_CASE => {
                eat_assert!(self, TK_CASE);
                let base = if self.peek_no_eof()?.token_type.unwrap() != TK_WHEN {
                    Some(self.parse_expr(0)?)
                } else {
                    None
                };

                eat_expect!(self, TK_WHEN);
                let first_when = self.parse_expr(0)?;
                eat_expect!(self, TK_THEN);
                let mut when_then_pairs = vec![(first_when, self.parse_expr(0)?)];

                while let Some(tok) = self.peek()? {
                    if tok.token_type.unwrap() != TK_WHEN {
                        break;
                    }

                    eat_assert!(self, TK_WHEN);
                    let when = self.parse_expr(0)?;
                    eat_expect!(self, TK_THEN);
                    let then = self.parse_expr(0)?;
                    when_then_pairs.push((when, then));
                }

                let else_expr = if let Some(ok) = self.peek()? {
                    if ok.token_type == Some(TK_ELSE) {
                        eat_assert!(self, TK_ELSE);
                        Some(self.parse_expr(0)?)
                    } else {
                        None
                    }
                } else {
                    None
                };

                eat_expect!(self, TK_END);
                Ok(Box::new(Expr::Case {
                    base,
                    when_then_pairs,
                    else_expr,
                }))
            }
            TK_RAISE => {
                eat_assert!(self, TK_RAISE);
                eat_expect!(self, TK_LP);

                let resolve = match self.peek_no_eof()?.token_type.unwrap() {
                    TK_IGNORE => {
                        eat_assert!(self, TK_IGNORE);
                        ResolveType::Ignore
                    }
                    _ => self.parse_raise_type()?,
                };

                let expr = if resolve != ResolveType::Ignore {
                    eat_expect!(self, TK_COMMA);
                    Some(self.parse_expr(0)?)
                } else {
                    None
                };

                eat_expect!(self, TK_RP);
                Ok(Box::new(Expr::Raise(resolve, expr)))
            }
            _ => {
                let can_be_lit_str = tok.token_type == Some(TK_STRING);
                let name = self.parse_nm()?;

                let second_name = if let Some(tok) = self.peek()? {
                    if tok.token_type == Some(TK_DOT) {
                        eat_assert!(self, TK_DOT);
                        Some(self.parse_nm()?)
                    } else if tok.token_type == Some(TK_LP) {
                        if can_be_lit_str {
                            return Err(Error::ParseUnexpectedToken {
                                parsed_offset: (self.offset(), 1).into(),
                                got: TK_STRING,
                                expected: &[TK_ID, TK_INDEXED, TK_JOIN_KW],
                            });
                        } // can not be literal string in function name

                        eat_assert!(self, TK_LP);
                        let tok = self.peek_no_eof()?;
                        match tok.token_type.unwrap() {
                            TK_STAR => {
                                eat_assert!(self, TK_STAR);
                                eat_expect!(self, TK_RP);
                                return Ok(Box::new(Expr::FunctionCallStar {
                                    name,
                                    filter_over: self.parse_filter_over()?,
                                }));
                            }
                            _ => {
                                let distinct = self.parse_distinct()?;
                                let exprs = self.parse_expr_list()?;
                                let order_by = self.parse_order_by()?;
                                eat_expect!(self, TK_RP);
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
                    if tok.token_type == Some(TK_DOT) {
                        debug_assert!(second_name.is_some());
                        eat_assert!(self, TK_DOT);
                        Some(self.parse_nm()?)
                    } else {
                        None
                    }
                } else {
                    None
                };

                if let Some(second_name) = second_name {
                    if let Some(third_name) = third_name {
                        Ok(Box::new(Expr::DoublyQualified(
                            name,
                            second_name,
                            third_name,
                        )))
                    } else {
                        Ok(Box::new(Expr::Qualified(name, second_name)))
                    }
                } else if can_be_lit_str {
                    Ok(Box::new(Expr::Literal(match name {
                        Name::Quoted(s) => Literal::String(s),
                        Name::Ident(s) => Literal::String(s),
                    })))
                } else {
                    match name {
                        Name::Ident(s) => {
                            let s_bytes = s.as_bytes();
                            match_ignore_ascii_case!(match s_bytes {
                                b"true" => {
                                    Ok(Box::new(Expr::Literal(Literal::Numeric("1".into()))))
                                }
                                b"false" => {
                                    Ok(Box::new(Expr::Literal(Literal::Numeric("0".into()))))
                                }
                                _ => return Ok(Box::new(Expr::Id(Name::Ident(s)))),
                            })
                        }
                        _ => Ok(Box::new(Expr::Id(name))),
                    }
                }
            }
        }
    }

    #[allow(clippy::vec_box)]
    fn parse_expr_list(&mut self) -> Result<Vec<Box<Expr>>> {
        let mut exprs = vec![];
        while let Some(tok) = self.peek()? {
            match tok.token_type.unwrap().fallback_id_if_ok() {
                TK_LP | TK_CAST | TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW | TK_NULL
                | TK_BLOB | TK_FLOAT | TK_INTEGER | TK_VARIABLE | TK_CTIME_KW | TK_NOT
                | TK_BITNOT | TK_PLUS | TK_MINUS | TK_EXISTS | TK_CASE => {}
                _ => break,
            }

            exprs.push(self.parse_expr(0)?);
            match self.peek_no_eof()?.token_type.unwrap() {
                TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                }
                _ => break,
            }
        }

        Ok(exprs)
    }

    fn parse_expr(&mut self, precedence: u8) -> Result<Box<Expr>> {
        let mut result = self.parse_expr_operand()?;

        loop {
            let pre = match self.current_token_precedence()? {
                Some(pre) if pre < precedence => break,
                None => break, // no more ops
                Some(pre) => pre,
            };

            let mut tok = self.peek_no_eof()?;
            let mut not = false;
            if tok.token_type.unwrap() == TK_NOT {
                eat_assert!(self, TK_NOT);
                tok = peek_expect!(
                    self, TK_BETWEEN, TK_IN, TK_MATCH, TK_LIKE_KW, TK_NULL, // `NOT NULL`
                );
                not = true;
            }

            result = match tok.token_type.unwrap() {
                TK_NULL => {
                    // special case `NOT NULL`
                    debug_assert!(not); // FIXME: not always true because of current_token_precedence
                    eat_assert!(self, TK_NULL);
                    Box::new(Expr::NotNull(result))
                }
                TK_OR => {
                    eat_assert!(self, TK_OR);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Or,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_AND => {
                    eat_assert!(self, TK_AND);
                    Box::new(Expr::Binary(
                        result,
                        Operator::And,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_EQ => {
                    eat_assert!(self, TK_EQ);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Equals,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_NE => {
                    eat_assert!(self, TK_NE);
                    Box::new(Expr::Binary(
                        result,
                        Operator::NotEquals,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_IS => {
                    eat_assert!(self, TK_IS);

                    let not = match self.peek_no_eof()?.token_type.unwrap() {
                        TK_NOT => {
                            eat_assert!(self, TK_NOT);
                            true
                        }
                        _ => false,
                    };

                    let op = match self.peek_no_eof()?.token_type.unwrap() {
                        TK_DISTINCT => {
                            eat_assert!(self, TK_DISTINCT);
                            eat_expect!(self, TK_FROM);
                            if not {
                                Operator::Is
                            } else {
                                Operator::IsNot
                            }
                        }
                        _ => {
                            if not {
                                Operator::IsNot
                            } else {
                                Operator::Is
                            }
                        }
                    };

                    Box::new(Expr::Binary(result, op, self.parse_expr(pre + 1)?))
                }
                TK_BETWEEN => {
                    eat_assert!(self, TK_BETWEEN);
                    let start = self.parse_expr(pre)?;
                    eat_expect!(self, TK_AND);
                    let end = self.parse_expr(pre)?;
                    Box::new(Expr::Between {
                        lhs: result,
                        not,
                        start,
                        end,
                    })
                }
                TK_IN => {
                    eat_assert!(self, TK_IN);
                    let tok = self.peek_no_eof()?;
                    match tok.token_type.unwrap() {
                        TK_LP => {
                            eat_assert!(self, TK_LP);
                            let tok = self.peek_no_eof()?;
                            match tok.token_type.unwrap() {
                                TK_SELECT | TK_WITH | TK_VALUES => {
                                    let select = self.parse_select()?;
                                    eat_expect!(self, TK_RP);
                                    Box::new(Expr::InSelect {
                                        lhs: result,
                                        not,
                                        rhs: select,
                                    })
                                }
                                _ => {
                                    let exprs = self.parse_expr_list()?;
                                    eat_expect!(self, TK_RP);
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
                                if tok.token_type == Some(TK_LP) {
                                    eat_assert!(self, TK_LP);
                                    exprs = self.parse_expr_list()?;
                                    eat_expect!(self, TK_RP);
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
                TK_MATCH | TK_LIKE_KW => {
                    let tok = eat_assert!(self, TK_MATCH, TK_LIKE_KW);
                    let op = match tok.token_type.unwrap() {
                        TK_MATCH => LikeOperator::Match,
                        TK_LIKE_KW => match_ignore_ascii_case!(match tok.value {
                            b"LIKE" => LikeOperator::Like,
                            b"GLOB" => LikeOperator::Glob,
                            b"REGEXP" => LikeOperator::Regexp,
                            _ => unreachable!(),
                        }),
                        _ => unreachable!(),
                    };

                    let expr = self.parse_expr(pre)?;
                    let escape = if let Some(tok) = self.peek()? {
                        if tok.token_type == Some(TK_ESCAPE) {
                            eat_assert!(self, TK_ESCAPE);
                            Some(self.parse_expr(pre)?)
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
                TK_ISNULL => {
                    eat_assert!(self, TK_ISNULL);
                    Box::new(Expr::IsNull(result))
                }
                TK_NOTNULL => {
                    eat_assert!(self, TK_NOTNULL);
                    Box::new(Expr::NotNull(result))
                }
                TK_LT => {
                    eat_assert!(self, TK_LT);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Less,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_GT => {
                    eat_assert!(self, TK_GT);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Greater,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_LE => {
                    eat_assert!(self, TK_LE);
                    Box::new(Expr::Binary(
                        result,
                        Operator::LessEquals,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_GE => {
                    eat_assert!(self, TK_GE);
                    Box::new(Expr::Binary(
                        result,
                        Operator::GreaterEquals,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_ESCAPE => unreachable!(),
                TK_BITAND => {
                    eat_assert!(self, TK_BITAND);
                    Box::new(Expr::Binary(
                        result,
                        Operator::BitwiseAnd,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_BITOR => {
                    eat_assert!(self, TK_BITOR);
                    Box::new(Expr::Binary(
                        result,
                        Operator::BitwiseOr,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_LSHIFT => {
                    eat_assert!(self, TK_LSHIFT);
                    Box::new(Expr::Binary(
                        result,
                        Operator::LeftShift,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_RSHIFT => {
                    eat_assert!(self, TK_RSHIFT);
                    Box::new(Expr::Binary(
                        result,
                        Operator::RightShift,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_PLUS => {
                    eat_assert!(self, TK_PLUS);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Add,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_MINUS => {
                    eat_assert!(self, TK_MINUS);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Subtract,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_STAR => {
                    eat_assert!(self, TK_STAR);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Multiply,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_SLASH => {
                    eat_assert!(self, TK_SLASH);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Divide,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_REM => {
                    eat_assert!(self, TK_REM);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Modulus,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_CONCAT => {
                    eat_assert!(self, TK_CONCAT);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Concat,
                        self.parse_expr(pre + 1)?,
                    ))
                }
                TK_PTR => {
                    let tok = eat_assert!(self, TK_PTR);
                    let op = if tok.value.len() == 2 {
                        Operator::ArrowRight
                    } else {
                        Operator::ArrowRightShift
                    };

                    Box::new(Expr::Binary(result, op, self.parse_expr(pre + 1)?))
                }
                TK_COLLATE => Box::new(Expr::Collate(result, self.parse_collate()?.unwrap())),
                _ => unreachable!(),
            }
        }

        Ok(result)
    }

    fn parse_collate(&mut self) -> Result<Option<Name>> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TK_COLLATE) {
                eat_assert!(self, TK_COLLATE);
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        let tok = eat_expect!(self, TK_ID, TK_STRING);
        let first_char = tok.value[0]; // no need to check empty
        match first_char {
            b'[' | b'\'' | b'`' | b'"' => Ok(Some(Name::Quoted(from_bytes(tok.value)))),
            _ => Ok(Some(Name::Ident(from_bytes(tok.value)))),
        }
    }

    fn parse_sort_order(&mut self) -> Result<Option<SortOrder>> {
        match self.peek()? {
            Some(tok) if tok.token_type == Some(TK_ASC) => {
                eat_assert!(self, TK_ASC);
                Ok(Some(SortOrder::Asc))
            }
            Some(tok) if tok.token_type == Some(TK_DESC) => {
                eat_assert!(self, TK_DESC);
                Ok(Some(SortOrder::Desc))
            }
            _ => Ok(None),
        }
    }

    fn parse_eid(&mut self) -> Result<IndexedColumn> {
        let nm = self.parse_nm()?;
        let collate = self.parse_collate()?;
        let sort_order = self.parse_sort_order()?;
        Ok(IndexedColumn {
            col_name: nm,
            collation_name: collate,
            order: sort_order,
        })
    }

    fn parse_eid_list(&mut self, check_dup: bool) -> Result<Vec<IndexedColumn>> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TK_LP) {
                eat_assert!(self, TK_LP);
            } else {
                return Ok(vec![]);
            }
        } else {
            return Ok(vec![]);
        }

        let mut columns = vec![self.parse_eid()?];
        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == Some(TK_COMMA) => {
                    eat_assert!(self, TK_COMMA);
                    columns.push(self.parse_eid()?);

                    // check dup
                    if check_dup {
                        let las_col = columns.last().unwrap();
                        for col in &columns[..columns.len() - 1] {
                            if col.col_name == las_col.col_name {
                                return Err(Error::Custom(format!(
                                    "duplicate column name: {}",
                                    col.col_name
                                )));
                            }
                        }
                    }
                }
                _ => break,
            }
        }
        eat_expect!(self, TK_RP);

        Ok(columns)
    }

    fn parse_common_table_expr(&mut self) -> Result<CommonTableExpr> {
        let nm = self.parse_nm()?;
        let eid_list = self.parse_eid_list(false)?;
        eat_expect!(self, TK_AS);
        let wqas = match self.peek_no_eof()?.token_type.unwrap() {
            TK_MATERIALIZED => {
                eat_assert!(self, TK_MATERIALIZED);
                Materialized::Yes
            }
            TK_NOT => {
                eat_assert!(self, TK_NOT);
                eat_expect!(self, TK_MATERIALIZED);
                Materialized::No
            }
            _ => Materialized::Any,
        };
        eat_expect!(self, TK_LP);
        let select = self.parse_select()?;
        eat_expect!(self, TK_RP);
        Ok(CommonTableExpr {
            tbl_name: nm,
            columns: eid_list,
            materialized: wqas,
            select,
        })
    }

    fn parse_with(&mut self) -> Result<Option<With>> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TK_WITH) {
                eat_assert!(self, TK_WITH);
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        let recursive = if self.peek_no_eof()?.token_type == Some(TK_RECURSIVE) {
            eat_assert!(self, TK_RECURSIVE);
            true
        } else {
            false
        };

        let mut ctes = vec![self.parse_common_table_expr()?];

        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == Some(TK_COMMA) => {
                    eat_assert!(self, TK_COMMA);
                    ctes.push(self.parse_common_table_expr()?);
                }
                _ => break,
            }
        }

        Ok(Some(With { recursive, ctes }))
    }

    fn parse_as(&mut self) -> Result<Option<As>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.unwrap().fallback_id_if_ok() {
                TK_AS => {
                    eat_assert!(self, TK_AS);
                    Ok(Some(As::As(self.parse_nm()?)))
                }
                TK_STRING | TK_ID => Ok(Some(As::Elided(self.parse_nm()?))),
                _ => Ok(None),
            },
        }
    }

    fn parse_window_defn(&mut self) -> Result<WindowDef> {
        let name = self.parse_nm()?;
        eat_expect!(self, TK_AS);
        eat_expect!(self, TK_LP);
        let window = self.parse_window()?;
        eat_expect!(self, TK_RP);
        Ok(WindowDef { name, window })
    }

    fn parse_window_clause(&mut self) -> Result<Vec<WindowDef>> {
        match self.peek()? {
            None => return Ok(vec![]),
            Some(tok) => match tok.token_type.unwrap() {
                TK_WINDOW => {
                    eat_assert!(self, TK_WINDOW);
                }
                _ => return Ok(vec![]),
            },
        }

        let mut result = vec![self.parse_window_defn()?];
        while let Some(tok) = self.peek()? {
            match tok.token_type.unwrap() {
                TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                    result.push(self.parse_window_defn()?);
                }
                _ => break,
            }
        }

        Ok(result)
    }

    fn parse_group_by(&mut self) -> Result<Option<GroupBy>> {
        match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TK_GROUP => {
                    eat_assert!(self, TK_GROUP);
                    eat_expect!(self, TK_BY);
                }
                _ => return Ok(None),
            },
        }

        let exprs = self.parse_nexpr_list()?;
        let having = match self.peek()? {
            Some(tok) if tok.token_type == Some(TK_HAVING) => {
                eat_assert!(self, TK_HAVING);
                Some(self.parse_expr(0)?)
            }
            _ => None,
        };

        Ok(Some(GroupBy { exprs, having }))
    }

    fn parse_where(&mut self) -> Result<Option<Box<Expr>>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TK_WHERE => {
                    eat_assert!(self, TK_WHERE);
                    let expr = self.parse_expr(0)?;
                    Ok(Some(expr))
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_indexed(&mut self) -> Result<Option<Indexed>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TK_INDEXED => {
                    eat_assert!(self, TK_INDEXED);
                    eat_expect!(self, TK_BY);
                    Ok(Some(Indexed::IndexedBy(self.parse_nm()?)))
                }
                TK_NOT => {
                    eat_assert!(self, TK_NOT);
                    eat_expect!(self, TK_INDEXED);
                    Ok(Some(Indexed::NotIndexed))
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_nm_list(&mut self) -> Result<Vec<Name>> {
        let mut names = vec![self.parse_nm()?];

        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == Some(TK_COMMA) => {
                    eat_assert!(self, TK_COMMA);
                    names.push(self.parse_nm()?);
                }
                _ => break,
            }
        }

        Ok(names)
    }

    fn parse_nm_list_opt(&mut self) -> Result<Vec<Name>> {
        match self.peek()? {
            Some(tok) if tok.token_type == Some(TK_LP) => {
                eat_assert!(self, TK_LP);
            }
            _ => return Ok(vec![]),
        }

        let result = self.parse_nm_list()?;
        eat_expect!(self, TK_RP);
        Ok(result)
    }

    fn parse_on_using(&mut self) -> Result<Option<JoinConstraint>> {
        match self.peek()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TK_ON => {
                    eat_assert!(self, TK_ON);
                    let expr = self.parse_expr(0)?;
                    Ok(Some(JoinConstraint::On(expr)))
                }
                TK_USING => {
                    eat_assert!(self, TK_USING);
                    eat_expect!(self, TK_LP);
                    let names = self.parse_nm_list()?;
                    eat_expect!(self, TK_RP);
                    Ok(Some(JoinConstraint::Using(names)))
                }
                _ => Ok(None),
            },
        }
    }

    fn parse_joined_tables(&mut self) -> Result<Vec<JoinedSelectTable>> {
        let mut result = vec![];
        while let Some(tok) = self.peek()? {
            let op = match tok.token_type.unwrap() {
                TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                    JoinOperator::Comma
                }
                TK_JOIN => {
                    eat_assert!(self, TK_JOIN);
                    JoinOperator::TypedJoin(None)
                }
                TK_JOIN_KW => {
                    let jkw = eat_assert!(self, TK_JOIN_KW);
                    let tok = eat_expect!(self, TK_JOIN, TK_ID, TK_STRING, TK_INDEXED, TK_JOIN_KW);

                    match tok.token_type.unwrap() {
                        TK_JOIN => {
                            JoinOperator::TypedJoin(Some(new_join_type(jkw.value, None, None)?))
                        }
                        _ => {
                            let name_1 = tok.value;
                            let tok = eat_expect!(
                                self, TK_JOIN, TK_ID, TK_STRING, TK_INDEXED, TK_JOIN_KW,
                            );

                            match tok.token_type.unwrap() {
                                TK_JOIN => JoinOperator::TypedJoin(Some(new_join_type(
                                    jkw.value,
                                    Some(name_1),
                                    None,
                                )?)),
                                _ => {
                                    let name_2 = tok.value;
                                    eat_expect!(self, TK_JOIN);
                                    JoinOperator::TypedJoin(Some(new_join_type(
                                        jkw.value,
                                        Some(name_1),
                                        Some(name_2),
                                    )?))
                                }
                            }
                        }
                    }
                }
                _ => break,
            };

            let tok = peek_expect!(self, TK_ID, TK_STRING, TK_INDEXED, TK_JOIN_KW, TK_LP);

            match tok.token_type.unwrap().fallback_id_if_ok() {
                TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW => {
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
                            TK_LP => {
                                eat_assert!(self, TK_LP);
                                let exprs = self.parse_expr_list()?;
                                eat_expect!(self, TK_RP);
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
                TK_LP => {
                    eat_assert!(self, TK_LP);
                    match self.peek_no_eof()?.token_type.unwrap() {
                        TK_SELECT | TK_WITH | TK_VALUES => {
                            let select = self.parse_select()?;
                            eat_expect!(self, TK_RP);
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
                            eat_expect!(self, TK_RP);
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

    fn parse_from_clause(&mut self) -> Result<FromClause> {
        let tok = peek_expect!(self, TK_ID, TK_STRING, TK_INDEXED, TK_JOIN_KW, TK_LP);

        match tok.token_type.unwrap().fallback_id_if_ok() {
            TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW => {
                let name = self.parse_fullname(false)?;
                match self.peek()? {
                    None => Ok(FromClause {
                        select: Box::new(SelectTable::Table(name, None, None)),
                        joins: vec![],
                    }),
                    Some(tok) => match tok.token_type.unwrap() {
                        TK_LP => {
                            eat_assert!(self, TK_LP);
                            let exprs = self.parse_expr_list()?;
                            eat_expect!(self, TK_RP);
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
            TK_LP => {
                eat_assert!(self, TK_LP);
                match self.peek_no_eof()?.token_type.unwrap() {
                    TK_SELECT | TK_WITH | TK_VALUES => {
                        let select = self.parse_select()?;
                        eat_expect!(self, TK_RP);
                        let alias = self.parse_as()?;
                        Ok(FromClause {
                            select: Box::new(SelectTable::Select(select, alias)),
                            joins: self.parse_joined_tables()?,
                        })
                    }
                    _ => {
                        let fr = self.parse_from_clause()?;
                        eat_expect!(self, TK_RP);
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

    fn parse_from_clause_opt(&mut self) -> Result<Option<FromClause>> {
        match self.peek()? {
            None => return Ok(None),
            Some(tok) if tok.token_type == Some(TK_FROM) => {
                eat_assert!(self, TK_FROM);
            }
            _ => return Ok(None),
        }

        Ok(Some(self.parse_from_clause()?))
    }

    fn parse_select_column(&mut self) -> Result<ResultColumn> {
        match self.peek_no_eof()?.token_type.unwrap().fallback_id_if_ok() {
            TK_STAR => {
                eat_assert!(self, TK_STAR);
                Ok(ResultColumn::Star)
            }
            tt => {
                // dot STAR case
                if tt == TK_ID || tt == TK_STRING || tt == TK_INDEXED || tt == TK_JOIN_KW {
                    if let Ok(res) = self.mark(|p| -> Result<ResultColumn> {
                        let name = p.parse_nm()?;
                        eat_expect!(p, TK_DOT);
                        eat_expect!(p, TK_STAR);
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

    fn parse_select_columns(&mut self) -> Result<Vec<ResultColumn>> {
        let mut result = vec![self.parse_select_column()?];

        while let Some(tok) = self.peek()? {
            if tok.token_type == Some(TK_COMMA) {
                eat_assert!(self, TK_COMMA);
            } else {
                break;
            }

            result.push(self.parse_select_column()?);
        }

        Ok(result)
    }

    #[allow(clippy::vec_box)]
    fn parse_nexpr_list(&mut self) -> Result<Vec<Box<Expr>>> {
        let mut result = vec![self.parse_expr(0)?];
        while let Some(tok) = self.peek()? {
            if tok.token_type == Some(TK_COMMA) {
                eat_assert!(self, TK_COMMA);
            } else {
                break;
            }

            result.push(self.parse_expr(0)?);
        }

        Ok(result)
    }

    fn parse_one_select(&mut self) -> Result<OneSelect> {
        let tok = eat_expect!(self, TK_SELECT, TK_VALUES);
        match tok.token_type.unwrap() {
            TK_SELECT => {
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
            TK_VALUES => {
                eat_expect!(self, TK_LP);
                let mut values = vec![self.parse_nexpr_list()?];
                eat_expect!(self, TK_RP);

                while let Some(tok) = self.peek()? {
                    if tok.token_type == Some(TK_COMMA) {
                        eat_assert!(self, TK_COMMA);
                    } else {
                        break;
                    }

                    eat_expect!(self, TK_LP);
                    values.push(self.parse_nexpr_list()?);
                    if values.last().unwrap().len() != values[0].len() {
                        return Err(Error::Custom(
                            "all VALUES must have the same number of terms".to_owned(),
                        ));
                    }

                    eat_expect!(self, TK_RP);
                }

                Ok(OneSelect::Values(values))
            }
            _ => unreachable!(),
        }
    }

    fn parse_select_body(&mut self) -> Result<SelectBody> {
        let select = self.parse_one_select()?;
        let mut compounds = vec![];
        while let Some(tok) = self.peek()? {
            let op = match tok.token_type.unwrap() {
                TK_UNION => {
                    eat_assert!(self, TK_UNION);
                    if self.peek_no_eof()?.token_type == Some(TK_ALL) {
                        eat_assert!(self, TK_ALL);
                        CompoundOperator::UnionAll
                    } else {
                        CompoundOperator::Union
                    }
                }
                TK_EXCEPT => {
                    eat_assert!(self, TK_EXCEPT);
                    CompoundOperator::Except
                }
                TK_INTERSECT => {
                    eat_assert!(self, TK_INTERSECT);
                    CompoundOperator::Intersect
                }
                _ => break,
            };

            compounds.push(CompoundSelect {
                operator: op,
                select: self.parse_one_select()?,
            });
        }

        Ok(SelectBody { select, compounds })
    }

    fn parse_sorted_column(&mut self) -> Result<SortedColumn> {
        let expr = self.parse_expr(0)?;
        let sort_order = self.parse_sort_order()?;

        let nulls = match self.peek()? {
            Some(tok) if tok.token_type == Some(TK_NULLS) => {
                eat_assert!(self, TK_NULLS);
                let tok = eat_expect!(self, TK_FIRST, TK_LAST);
                match tok.token_type.unwrap() {
                    TK_FIRST => Some(NullsOrder::First),
                    TK_LAST => Some(NullsOrder::Last),
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

    fn parse_sort_list(&mut self) -> Result<Vec<SortedColumn>> {
        let mut columns = vec![self.parse_sorted_column()?];
        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == Some(TK_COMMA) => {
                    eat_assert!(self, TK_COMMA);
                    columns.push(self.parse_sorted_column()?);
                }
                _ => break,
            }
        }

        Ok(columns)
    }

    fn parse_order_by(&mut self) -> Result<Vec<SortedColumn>> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TK_ORDER) {
                eat_assert!(self, TK_ORDER);
            } else {
                return Ok(vec![]);
            }
        } else {
            return Ok(vec![]);
        }

        eat_expect!(self, TK_BY);
        self.parse_sort_list()
    }

    fn parse_limit(&mut self) -> Result<Option<Limit>> {
        if let Some(tok) = self.peek()? {
            if tok.token_type == Some(TK_LIMIT) {
                eat_assert!(self, TK_LIMIT);
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        let limit = self.parse_expr(0)?;
        let offset = match self.peek()? {
            Some(tok) => match tok.token_type.unwrap() {
                TK_OFFSET | TK_COMMA => {
                    eat_assert!(self, TK_OFFSET, TK_COMMA);
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

    fn parse_select_without_cte(&mut self, with: Option<With>) -> Result<Select> {
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

    fn parse_select(&mut self) -> Result<Select> {
        let with = self.parse_with()?;
        self.parse_select_without_cte(with)
    }

    fn parse_primary_table_constraint(&mut self) -> Result<TableConstraint> {
        eat_assert!(self, TK_PRIMARY);
        eat_expect!(self, TK_KEY);
        eat_expect!(self, TK_LP);
        let columns = self.parse_sort_list()?;
        let auto_increment = self.parse_auto_increment()?;
        eat_expect!(self, TK_RP);
        let conflict_clause = self.parse_on_conflict()?;
        Ok(TableConstraint::PrimaryKey {
            columns,
            auto_increment,
            conflict_clause,
        })
    }

    fn parse_unique_table_constraint(&mut self) -> Result<TableConstraint> {
        eat_assert!(self, TK_UNIQUE);
        eat_expect!(self, TK_LP);
        let columns = self.parse_sort_list()?;
        eat_expect!(self, TK_RP);
        let conflict_clause = self.parse_on_conflict()?;
        Ok(TableConstraint::Unique {
            columns,
            conflict_clause,
        })
    }

    fn parse_check_table_constraint(&mut self) -> Result<TableConstraint> {
        eat_assert!(self, TK_CHECK);
        eat_expect!(self, TK_LP);
        let expr = self.parse_expr(0)?;
        eat_expect!(self, TK_RP);
        Ok(TableConstraint::Check(expr))
    }

    fn parse_foreign_key_table_constraint(&mut self) -> Result<TableConstraint> {
        eat_assert!(self, TK_FOREIGN);
        eat_expect!(self, TK_KEY);
        peek_expect!(self, TK_LP); // make sure we have columns
        let columns = self.parse_eid_list(false)?;
        peek_expect!(self, TK_REFERENCES);
        let clause = self.parse_foreign_key_clause()?;
        let deref_clause = self.parse_defer_subclause()?;
        Ok(TableConstraint::ForeignKey {
            columns,
            clause,
            deref_clause,
        })
    }

    fn parse_named_table_constraints(&mut self) -> Result<Vec<NamedTableConstraint>> {
        let mut result = vec![];

        while let Some(tok) = self.peek()? {
            match tok.token_type.unwrap() {
                TK_COMMA => {
                    eat_assert!(self, TK_COMMA);
                }
                TK_CONSTRAINT | TK_PRIMARY | TK_UNIQUE | TK_CHECK | TK_FOREIGN => {}
                _ => break,
            }

            let name = match self.peek_no_eof()?.token_type.unwrap() {
                TK_CONSTRAINT => {
                    eat_assert!(self, TK_CONSTRAINT);
                    Some(self.parse_nm()?)
                }
                _ => None,
            };

            let tok = peek_expect!(self, TK_PRIMARY, TK_UNIQUE, TK_CHECK, TK_FOREIGN);

            match tok.token_type.unwrap() {
                TK_PRIMARY => {
                    result.push(NamedTableConstraint {
                        name,
                        constraint: self.parse_primary_table_constraint()?,
                    });
                }
                TK_UNIQUE => {
                    result.push(NamedTableConstraint {
                        name,
                        constraint: self.parse_unique_table_constraint()?,
                    });
                }
                TK_CHECK => {
                    result.push(NamedTableConstraint {
                        name,
                        constraint: self.parse_check_table_constraint()?,
                    });
                }
                TK_FOREIGN => {
                    result.push(NamedTableConstraint {
                        name,
                        constraint: self.parse_foreign_key_table_constraint()?,
                    });
                }
                _ => unreachable!(),
            }
        }

        Ok(result)
    }

    fn parse_table_option(&mut self) -> Result<TableOptions> {
        match self.peek()? {
            Some(tok) => match tok.token_type.unwrap().fallback_id_if_ok() {
                TK_WITHOUT => {
                    eat_assert!(self, TK_WITHOUT);
                    let tok = eat_expect!(self, TK_ID);
                    match_ignore_ascii_case!(match tok.value {
                        b"ROWID" => Ok(TableOptions::WITHOUT_ROWID),
                        _ => Err(Error::Custom(format!(
                            "unknown table option: {}",
                            from_bytes(tok.value)
                        ))),
                    })
                }
                TK_ID => {
                    let tok = eat_assert!(self, TK_ID);
                    match_ignore_ascii_case!(match tok.value {
                        b"STRICT" => Ok(TableOptions::STRICT),
                        _ => Err(Error::Custom(format!(
                            "unknown table option: {}",
                            from_bytes(tok.value)
                        ))),
                    })
                }
                _ => Ok(TableOptions::NONE),
            },
            _ => Ok(TableOptions::NONE),
        }
    }

    fn parse_table_options(&mut self) -> Result<TableOptions> {
        let mut result = self.parse_table_option()?;
        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == Some(TK_COMMA) => {
                    eat_assert!(self, TK_COMMA);
                    result |= self.parse_table_option()?;
                }
                _ => break,
            }
        }

        Ok(result)
    }

    fn parse_create_table_args(&mut self, tbl_name: &QualifiedName) -> Result<CreateTableBody> {
        let tok = eat_expect!(self, TK_LP, TK_AS);

        match tok.token_type.unwrap() {
            TK_AS => Ok(CreateTableBody::AsSelect(self.parse_select()?)),
            TK_LP => {
                let mut columns = vec![self.parse_column_definition(false)?];
                let mut generated_count = 0;
                let mut has_primary_key = false;
                for cs in &columns[0].constraints {
                    match cs.constraint {
                        ColumnConstraint::Generated { .. } => generated_count += 1,
                        ColumnConstraint::PrimaryKey { .. } => has_primary_key = true,
                        _ => {}
                    }
                }

                let mut constraints = vec![];
                loop {
                    match self.peek()? {
                        Some(tok) if tok.token_type == Some(TK_COMMA) => {
                            eat_assert!(self, TK_COMMA);
                            match self.peek_no_eof()?.token_type.unwrap() {
                                TK_CONSTRAINT | TK_PRIMARY | TK_UNIQUE | TK_CHECK | TK_FOREIGN => {
                                    constraints = self.parse_named_table_constraints()?;
                                    for cons in &constraints {
                                        if let TableConstraint::PrimaryKey { .. } = cons.constraint
                                        {
                                            has_primary_key = true;
                                            break;
                                        }
                                    }

                                    break;
                                }
                                _ => {
                                    columns.push(self.parse_column_definition(false)?);
                                    for cs in &columns.last().unwrap().constraints {
                                        match cs.constraint {
                                            ColumnConstraint::Generated { .. } => {
                                                generated_count += 1
                                            }
                                            ColumnConstraint::PrimaryKey { .. } => {
                                                has_primary_key = true
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }
                        _ => break,
                    }
                }

                // larger than or equal columns len means we have only generated columns
                if generated_count >= columns.len() {
                    return Err(Error::Custom(
                        "must have at least one non-generated column".to_owned(),
                    ));
                }

                eat_expect!(self, TK_RP);
                let options = self.parse_table_options()?;

                // strict check
                if options.contains(TableOptions::STRICT) {
                    for c in &columns {
                        match &c.col_type {
                            Some(Type { name, .. }) => {
                                // The datatype must be one of following: INT INTEGER REAL TEXT BLOB ANY
                                let bytes_name = name.as_bytes();
                                match_ignore_ascii_case!(match bytes_name {
                                    b"INT" | b"INTEGER" | b"REAL" | b"TEXT" | b"BLOB" | b"ANY" => {}
                                    _ => {
                                        return Err(Error::Custom(format!(
                                            "unknown datatype for {}.{}: \"{}\"",
                                            tbl_name, c.col_name, name
                                        )));
                                    }
                                })
                            }
                            _ => {
                                // Every column definition must specify a datatype for that column. The freedom to specify a column without a datatype is removed.
                                return Err(Error::Custom(format!(
                                    "missing datatype for {}.{}",
                                    tbl_name, c.col_name
                                )));
                            }
                        }
                    }
                }

                // primary key check
                if options.contains(TableOptions::WITHOUT_ROWID) && !has_primary_key {
                    return Err(Error::Custom(format!(
                        "PRIMARY KEY missing on table {tbl_name}"
                    )));
                }

                Ok(CreateTableBody::ColumnsAndConstraints {
                    columns,
                    constraints,
                    options,
                })
            }
            _ => unreachable!(),
        }
    }

    fn parse_create_table(&mut self, temporary: bool) -> Result<Stmt> {
        eat_assert!(self, TK_TABLE);
        let if_not_exists = self.parse_if_not_exists()?;
        let tbl_name = self.parse_fullname(false)?;
        if temporary {
            if let Some(ref db_name) = tbl_name.db_name {
                if !db_name.as_str().eq_ignore_ascii_case("TEMP") {
                    return Err(Error::Custom(
                        "temporary table name must be unqualified".to_owned(),
                    ));
                }
            }
        }

        let body = self.parse_create_table_args(&tbl_name)?;
        Ok(Stmt::CreateTable {
            temporary,
            if_not_exists,
            tbl_name,
            body,
        })
    }

    fn parse_analyze(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_ANALYZE);
        let name = match self.peek()? {
            Some(tok) => match tok.token_type.unwrap().fallback_id_if_ok() {
                TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW => Some(self.parse_fullname(false)?),
                _ => None,
            },
            _ => None,
        };

        Ok(Stmt::Analyze { name })
    }

    fn parse_attach(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_ATTACH);
        if self.peek_no_eof()?.token_type == Some(TK_DATABASE) {
            eat_assert!(self, TK_DATABASE);
        }

        let expr = self.parse_expr(0)?;
        eat_expect!(self, TK_AS);
        let db_name = self.parse_expr(0)?;
        let key = match self.peek()? {
            Some(tok) => match tok.token_type.unwrap() {
                TK_KEY => {
                    eat_assert!(self, TK_KEY);
                    Some(self.parse_expr(0)?)
                }
                _ => None,
            },
            _ => None,
        };

        Ok(Stmt::Attach { expr, db_name, key })
    }

    fn parse_detach(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_DETACH);
        if self.peek_no_eof()?.token_type == Some(TK_DATABASE) {
            eat_assert!(self, TK_DATABASE);
        }

        Ok(Stmt::Detach {
            name: self.parse_expr(0)?,
        })
    }

    fn parse_pragma_value(&mut self) -> Result<PragmaValue> {
        match self.peek_no_eof()?.token_type.unwrap().fallback_id_if_ok() {
            TK_ON | TK_DELETE | TK_DEFAULT => {
                let tok = eat_assert!(self, TK_ON, TK_DELETE, TK_DEFAULT);
                Ok(Box::new(Expr::Literal(Literal::Keyword(from_bytes(
                    tok.value,
                )))))
            }
            TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW => {
                Ok(Box::new(Expr::Name(self.parse_nm()?)))
            }
            _ => self.parse_signed(),
        }
    }

    fn parse_pragma(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_PRAGMA);
        let name = self.parse_fullname(false)?;
        match self.peek()? {
            Some(tok) => match tok.token_type.unwrap() {
                TK_EQ => {
                    eat_assert!(self, TK_EQ);
                    Ok(Stmt::Pragma {
                        name,
                        body: Some(PragmaBody::Equals(self.parse_pragma_value()?)),
                    })
                }
                TK_LP => {
                    eat_assert!(self, TK_LP);
                    let value = self.parse_pragma_value()?;
                    eat_expect!(self, TK_RP);
                    Ok(Stmt::Pragma {
                        name,
                        body: Some(PragmaBody::Call(value)),
                    })
                }
                _ => Ok(Stmt::Pragma { name, body: None }),
            },
            _ => Ok(Stmt::Pragma { name, body: None }),
        }
    }

    fn parse_vacuum(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_VACUUM);

        let name = match self.peek()? {
            Some(tok) => match tok.token_type.unwrap().fallback_id_if_ok() {
                TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW => Some(self.parse_nm()?),
                _ => None,
            },
            _ => None,
        };

        let into = match self.peek()? {
            Some(tok) if tok.token_type == Some(TK_INTO) => {
                eat_assert!(self, TK_INTO);
                Some(self.parse_expr(0)?)
            }
            _ => None,
        };

        Ok(Stmt::Vacuum { name, into })
    }

    fn parse_term(&mut self) -> Result<Box<Expr>> {
        peek_expect!(
            self,
            TK_NULL,
            TK_BLOB,
            TK_STRING,
            TK_FLOAT,
            TK_INTEGER,
            TK_CTIME_KW,
        );

        self.parse_expr_operand()
    }

    fn parse_default_column_constraint(&mut self) -> Result<ColumnConstraint> {
        eat_assert!(self, TK_DEFAULT);
        let tok = peek_expect!(
            self,
            TK_NULL,
            TK_BLOB,
            TK_STRING,
            TK_FLOAT,
            TK_INTEGER,
            TK_CTIME_KW,
            TK_LP,
            TK_PLUS,
            TK_MINUS,
            TK_ID,
            TK_INDEXED,
        );

        match tok.token_type.unwrap() {
            TK_LP => {
                eat_assert!(self, TK_LP);
                let expr = self.parse_expr(0)?;
                eat_expect!(self, TK_RP);
                Ok(ColumnConstraint::Default(Box::new(Expr::Parenthesized(
                    vec![expr],
                ))))
            }
            TK_PLUS => {
                eat_assert!(self, TK_PLUS);
                Ok(ColumnConstraint::Default(Box::new(Expr::Unary(
                    UnaryOperator::Positive,
                    self.parse_term()?,
                ))))
            }
            TK_MINUS => {
                eat_assert!(self, TK_MINUS);
                Ok(ColumnConstraint::Default(Box::new(Expr::Unary(
                    UnaryOperator::Negative,
                    self.parse_term()?,
                ))))
            }
            TK_NULL | TK_BLOB | TK_STRING | TK_FLOAT | TK_INTEGER | TK_CTIME_KW => {
                Ok(ColumnConstraint::Default(self.parse_term()?))
            }
            _ => Ok(ColumnConstraint::Default(Box::new(Expr::Id(
                self.parse_nm()?,
            )))),
        }
    }

    fn parse_resolve_type(&mut self) -> Result<ResolveType> {
        match self.peek_no_eof()?.token_type.unwrap() {
            TK_IGNORE => {
                eat_assert!(self, TK_IGNORE);
                Ok(ResolveType::Ignore)
            }
            TK_REPLACE => {
                eat_assert!(self, TK_REPLACE);
                Ok(ResolveType::Replace)
            }
            _ => Ok(self.parse_raise_type()?),
        }
    }

    fn parse_on_conflict(&mut self) -> Result<Option<ResolveType>> {
        match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TK_ON => {
                    eat_assert!(self, TK_ON);
                    eat_expect!(self, TK_CONFLICT);
                }
                _ => return Ok(None),
            },
        }

        Ok(Some(self.parse_resolve_type()?))
    }

    fn parse_or_conflict(&mut self) -> Result<Option<ResolveType>> {
        match self.peek()? {
            None => return Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TK_OR => {
                    eat_assert!(self, TK_OR);
                }
                _ => return Ok(None),
            },
        }

        Ok(Some(self.parse_resolve_type()?))
    }

    fn parse_auto_increment(&mut self) -> Result<bool> {
        match self.peek()? {
            None => Ok(false),
            Some(tok) => match tok.token_type.unwrap() {
                TK_AUTOINCR => {
                    eat_assert!(self, TK_AUTOINCR);
                    Ok(true)
                }
                _ => Ok(false),
            },
        }
    }

    fn parse_not_null_column_constraint(&mut self) -> Result<ColumnConstraint> {
        let has_not = match self.peek_no_eof()?.token_type.unwrap() {
            TK_NOT => {
                eat_assert!(self, TK_NOT);
                true
            }
            _ => false,
        };

        eat_expect!(self, TK_NULL);
        Ok(ColumnConstraint::NotNull {
            nullable: !has_not,
            conflict_clause: self.parse_on_conflict()?,
        })
    }

    fn parse_primary_column_constraint(&mut self) -> Result<ColumnConstraint> {
        eat_assert!(self, TK_PRIMARY);
        eat_expect!(self, TK_KEY);
        let sort_order = self.parse_sort_order()?;
        let conflict_clause = self.parse_on_conflict()?;
        let autoincr = self.parse_auto_increment()?;

        Ok(ColumnConstraint::PrimaryKey {
            order: sort_order,
            conflict_clause,
            auto_increment: autoincr,
        })
    }

    fn parse_unique_column_constraint(&mut self) -> Result<ColumnConstraint> {
        eat_assert!(self, TK_UNIQUE);
        Ok(ColumnConstraint::Unique(self.parse_on_conflict()?))
    }

    fn parse_check_column_constraint(&mut self) -> Result<ColumnConstraint> {
        eat_assert!(self, TK_CHECK);
        eat_expect!(self, TK_LP);
        let expr = self.parse_expr(0)?;
        eat_expect!(self, TK_RP);
        Ok(ColumnConstraint::Check(expr))
    }

    fn parse_ref_act(&mut self) -> Result<RefAct> {
        let tok = eat_expect!(self, TK_SET, TK_CASCADE, TK_RESTRICT, TK_NO);

        match tok.token_type.unwrap() {
            TK_SET => {
                let tok = eat_expect!(self, TK_NULL, TK_DEFAULT);
                match tok.token_type.unwrap() {
                    TK_NULL => Ok(RefAct::SetNull),
                    TK_DEFAULT => Ok(RefAct::SetDefault),
                    _ => unreachable!(),
                }
            }
            TK_CASCADE => Ok(RefAct::Cascade),
            TK_RESTRICT => Ok(RefAct::Restrict),
            TK_NO => {
                eat_expect!(self, TK_ACTION);
                Ok(RefAct::NoAction)
            }
            _ => unreachable!(),
        }
    }

    fn parse_ref_args(&mut self) -> Result<Vec<RefArg>> {
        let mut result = vec![];

        while let Some(tok) = self.peek()? {
            match tok.token_type.unwrap() {
                TK_MATCH => {
                    eat_assert!(self, TK_MATCH);
                    result.push(RefArg::Match(self.parse_nm()?));
                }
                TK_ON => {
                    eat_assert!(self, TK_ON);
                    let tok = eat_expect!(self, TK_INSERT, TK_DELETE, TK_UPDATE);
                    match tok.token_type.unwrap() {
                        TK_INSERT => result.push(RefArg::OnInsert(self.parse_ref_act()?)),
                        TK_DELETE => result.push(RefArg::OnDelete(self.parse_ref_act()?)),
                        TK_UPDATE => result.push(RefArg::OnUpdate(self.parse_ref_act()?)),
                        _ => unreachable!(),
                    }
                }
                _ => break,
            }
        }

        Ok(result)
    }

    fn parse_foreign_key_clause(&mut self) -> Result<ForeignKeyClause> {
        eat_assert!(self, TK_REFERENCES);
        let name = self.parse_nm()?;
        let eid_list = self.parse_eid_list(false)?;
        let ref_args = self.parse_ref_args()?;
        Ok(ForeignKeyClause {
            tbl_name: name,
            columns: eid_list,
            args: ref_args,
        })
    }

    fn parse_defer_subclause(&mut self) -> Result<Option<DeferSubclause>> {
        let has_not = match self.peek()? {
            Some(tok) => match tok.token_type.unwrap() {
                TK_DEFERRABLE => false,
                TK_NOT => {
                    eat_assert!(self, TK_NOT);
                    true
                }
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };

        eat_expect!(self, TK_DEFERRABLE);

        let init = match self.peek()? {
            Some(tok) => match tok.token_type.unwrap() {
                TK_INITIALLY => {
                    eat_assert!(self, TK_INITIALLY);
                    let tok = eat_expect!(self, TK_DEFERRED, TK_IMMEDIATE);
                    match tok.token_type.unwrap() {
                        TK_DEFERRED => Some(InitDeferredPred::InitiallyDeferred),
                        TK_IMMEDIATE => Some(InitDeferredPred::InitiallyImmediate),
                        _ => unreachable!(),
                    }
                }
                _ => None,
            },
            _ => None,
        };

        Ok(Some(DeferSubclause {
            deferrable: !has_not,
            init_deferred: init,
        }))
    }

    fn parse_reference_column_constraint(&mut self) -> Result<ColumnConstraint> {
        let clause = self.parse_foreign_key_clause()?;
        let deref_clause = self.parse_defer_subclause()?;
        Ok(ColumnConstraint::ForeignKey {
            clause,
            deref_clause,
        })
    }

    fn parse_generated_column_constraint(&mut self) -> Result<ColumnConstraint> {
        let tok = eat_assert!(self, TK_GENERATED, TK_AS);
        match tok.token_type.unwrap() {
            TK_GENERATED => {
                eat_expect!(self, TK_ALWAYS);
                eat_expect!(self, TK_AS);
            }
            TK_AS => {}
            _ => unreachable!(),
        }

        eat_expect!(self, TK_LP);
        let expr = self.parse_expr(0)?;
        eat_expect!(self, TK_RP);

        let typ = match self.peek()? {
            Some(tok) => match tok.token_type.unwrap().fallback_id_if_ok() {
                TK_ID => {
                    let tok = eat_assert!(self, TK_ID);
                    Some(Name::Ident(from_bytes(tok.value)))
                }
                _ => None,
            },
            _ => None,
        };

        Ok(ColumnConstraint::Generated { expr, typ })
    }

    fn parse_named_column_constraints(
        &mut self,
        in_alter: bool,
    ) -> Result<Vec<NamedColumnConstraint>> {
        let mut result = vec![];

        loop {
            let name = match self.peek()? {
                Some(tok) => match tok.token_type.unwrap() {
                    TK_CONSTRAINT => {
                        eat_assert!(self, TK_CONSTRAINT);
                        Some(self.parse_nm()?)
                    }
                    _ => None,
                },
                _ => None,
            };

            if name.is_some() {
                peek_expect!(
                    self,
                    TK_DEFAULT,
                    TK_NOT,
                    TK_NULL,
                    TK_PRIMARY,
                    TK_UNIQUE,
                    TK_CHECK,
                    TK_REFERENCES,
                    TK_COLLATE,
                    TK_GENERATED,
                    TK_AS,
                );
            }

            match self.peek()? {
                Some(tok) => match tok.token_type.unwrap() {
                    TK_DEFAULT => {
                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_default_column_constraint()?,
                        });
                    }
                    TK_NOT | TK_NULL => {
                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_not_null_column_constraint()?,
                        });
                    }
                    TK_PRIMARY => {
                        if in_alter {
                            return Err(Error::Custom(
                                "Cannot add a PRIMARY KEY column in ALTER TABLE statement"
                                    .to_owned(),
                            ));
                        }

                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_primary_column_constraint()?,
                        });
                    }
                    TK_UNIQUE => {
                        if in_alter {
                            return Err(Error::Custom(
                                "Cannot add a UNIQUE column in ALTER TABLE statement".to_owned(),
                            ));
                        }

                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_unique_column_constraint()?,
                        });
                    }
                    TK_CHECK => {
                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_check_column_constraint()?,
                        });
                    }
                    TK_REFERENCES => {
                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_reference_column_constraint()?,
                        });
                    }
                    TK_COLLATE => {
                        result.push(NamedColumnConstraint {
                            name,
                            constraint: ColumnConstraint::Collate {
                                collation_name: self.parse_collate()?.unwrap(),
                            },
                        });
                    }
                    TK_GENERATED | TK_AS => {
                        result.push(NamedColumnConstraint {
                            name,
                            constraint: self.parse_generated_column_constraint()?,
                        });
                    }
                    _ => break,
                },
                _ => break,
            }
        }

        Ok(result)
    }

    pub fn parse_column_definition(&mut self, in_alter: bool) -> Result<ColumnDefinition> {
        let col_name = self.parse_nm()?;
        if !in_alter && col_name.as_str().eq_ignore_ascii_case("rowid") {
            return Err(Error::Custom("cannot use reserved word: ROWID".to_owned()));
        }

        let col_type = self.parse_type()?;
        let constraints = self.parse_named_column_constraints(in_alter)?;
        Ok(ColumnDefinition {
            col_name,
            col_type,
            constraints,
        })
    }

    fn parse_alter(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_ALTER);
        eat_expect!(self, TK_TABLE);
        let tbl_name = self.parse_fullname(false)?;
        let tok = eat_expect!(self, TK_ADD, TK_DROP, TK_RENAME, TK_ALTER);

        match tok.token_type.unwrap() {
            TK_ADD => {
                if self.peek_no_eof()?.token_type == Some(TK_COLUMNKW) {
                    eat_assert!(self, TK_COLUMNKW);
                }

                Ok(Stmt::AlterTable(AlterTable {
                    name: tbl_name,
                    body: AlterTableBody::AddColumn(self.parse_column_definition(true)?),
                }))
            }
            TK_DROP => {
                if self.peek_no_eof()?.token_type == Some(TK_COLUMNKW) {
                    eat_assert!(self, TK_COLUMNKW);
                }

                Ok(Stmt::AlterTable(AlterTable {
                    name: tbl_name,
                    body: AlterTableBody::DropColumn(self.parse_nm()?),
                }))
            }
            TK_RENAME => {
                let col_name = match self.peek_no_eof()?.token_type.unwrap().fallback_id_if_ok() {
                    TK_COLUMNKW => {
                        eat_assert!(self, TK_COLUMNKW);
                        Some(self.parse_nm()?)
                    }
                    TK_ID | TK_STRING | TK_INDEXED | TK_JOIN_KW => Some(self.parse_nm()?),
                    _ => None,
                };

                eat_expect!(self, TK_TO);
                let to_name = self.parse_nm()?;

                if let Some(col_name) = col_name {
                    Ok(Stmt::AlterTable(AlterTable {
                        name: tbl_name,
                        body: AlterTableBody::RenameColumn {
                            old: col_name,
                            new: to_name,
                        },
                    }))
                } else {
                    Ok(Stmt::AlterTable(AlterTable {
                        name: tbl_name,
                        body: AlterTableBody::RenameTo(to_name),
                    }))
                }
            }
            TK_ALTER => {
                eat_expect!(self, TK_COLUMNKW);
                let col_name = self.parse_nm()?;

                eat_expect!(self, TK_TO);

                let new = self.parse_column_definition(true)?;

                Ok(Stmt::AlterTable(AlterTable {
                    name: tbl_name,
                    body: AlterTableBody::AlterColumn { old: col_name, new },
                }))
            }
            _ => unreachable!(),
        }
    }

    fn parse_create_index(&mut self) -> Result<Stmt> {
        let tok = eat_assert!(self, TK_INDEX, TK_UNIQUE);
        let has_unique = tok.token_type == Some(TK_UNIQUE);
        if has_unique {
            eat_expect!(self, TK_INDEX);
        }

        let if_not_exists = self.parse_if_not_exists()?;
        let idx_name = self.parse_fullname(false)?;
        eat_expect!(self, TK_ON);
        let tbl_name = self.parse_nm()?;
        eat_expect!(self, TK_LP);
        let columns = self.parse_sort_list()?;
        eat_expect!(self, TK_RP);
        let where_clause = self.parse_where()?;

        Ok(Stmt::CreateIndex {
            if_not_exists,
            idx_name,
            tbl_name,
            columns,
            where_clause,
            unique: has_unique,
        })
    }

    fn parse_set(&mut self) -> Result<Set> {
        let tok = peek_expect!(self, TK_LP, TK_ID, TK_STRING, TK_JOIN_KW, TK_INDEXED);

        match tok.token_type.unwrap() {
            TK_LP => {
                eat_assert!(self, TK_LP);
                let names = self.parse_nm_list()?;
                eat_expect!(self, TK_RP);
                eat_expect!(self, TK_EQ);
                Ok(Set {
                    col_names: names,
                    expr: self.parse_expr(0)?,
                })
            }
            _ => {
                let name = self.parse_nm()?;
                eat_expect!(self, TK_EQ);
                Ok(Set {
                    col_names: vec![name],
                    expr: self.parse_expr(0)?,
                })
            }
        }
    }

    fn parse_set_list(&mut self) -> Result<Vec<Set>> {
        let mut results = vec![self.parse_set()?];
        loop {
            match self.peek()? {
                Some(tok) if tok.token_type == Some(TK_COMMA) => {
                    eat_assert!(self, TK_COMMA);
                    results.push(self.parse_set()?);
                }
                _ => break,
            }
        }

        Ok(results)
    }

    fn parse_returning(&mut self) -> Result<Vec<ResultColumn>> {
        match self.peek()? {
            Some(tok) if tok.token_type == Some(TK_RETURNING) => {
                eat_assert!(self, TK_RETURNING);
            }
            _ => return Ok(vec![]),
        }

        self.parse_select_columns()
    }

    fn parse_upsert(&mut self) -> Result<(Option<Box<Upsert>>, Vec<ResultColumn>)> {
        match self.peek()? {
            Some(tok) => match tok.token_type.unwrap() {
                TK_ON => {
                    eat_assert!(self, TK_ON);
                }
                TK_RETURNING => {
                    return Ok((None, self.parse_returning()?));
                }
                _ => return Ok((None, vec![])),
            },
            _ => return Ok((None, vec![])),
        }

        eat_expect!(self, TK_CONFLICT);
        let targets = match self.peek_no_eof()?.token_type.unwrap() {
            TK_LP => {
                eat_assert!(self, TK_LP);
                let result = self.parse_sort_list()?;
                eat_expect!(self, TK_RP);
                result
            }
            _ => vec![],
        };

        let where_clause = if !targets.is_empty() {
            self.parse_where()?
        } else {
            None
        };

        eat_expect!(self, TK_DO);

        let tok = eat_expect!(self, TK_NOTHING, TK_UPDATE);
        let do_clause = match tok.token_type.unwrap() {
            TK_NOTHING => UpsertDo::Nothing,
            TK_UPDATE => {
                eat_expect!(self, TK_SET);
                let set_list = self.parse_set_list()?;
                let update_where_clause = self.parse_where()?;
                UpsertDo::Set {
                    sets: set_list,
                    where_clause: update_where_clause,
                }
            }
            _ => unreachable!(),
        };

        if !targets.is_empty() {
            let (next, returning) = self.parse_upsert()?;
            Ok((
                Some(Box::new(Upsert {
                    index: Some(UpsertIndex {
                        targets,
                        where_clause,
                    }),
                    do_clause,
                    next,
                })),
                returning,
            ))
        } else {
            Ok((
                Some(Box::new(Upsert {
                    index: None,
                    do_clause,
                    next: None,
                })),
                self.parse_returning()?,
            ))
        }
    }

    fn parse_trigger_insert_cmd(&mut self) -> Result<TriggerCmd> {
        let tok = eat_assert!(self, TK_INSERT, TK_REPLACE);
        let resolve_type = match tok.token_type.unwrap() {
            TK_INSERT => self.parse_or_conflict()?,
            TK_REPLACE => Some(ResolveType::Replace),
            _ => unreachable!(),
        };

        eat_expect!(self, TK_INTO);
        let tbl_name = self.parse_nm()?;
        let col_names = self.parse_nm_list_opt()?;
        let select = self.parse_select()?;
        let (upsert, returning) = self.parse_upsert()?;
        Ok(TriggerCmd::Insert {
            or_conflict: resolve_type,
            tbl_name,
            col_names,
            select,
            upsert,
            returning,
        })
    }

    fn parse_trigger_update_cmd(&mut self) -> Result<TriggerCmd> {
        eat_assert!(self, TK_UPDATE);
        let or_conflict = self.parse_or_conflict()?;
        let tbl_name = self.parse_nm()?;
        eat_expect!(self, TK_SET);
        let sets = self.parse_set_list()?;
        let from = self.parse_from_clause_opt()?;
        let where_clause = self.parse_where()?;
        Ok(TriggerCmd::Update {
            or_conflict,
            tbl_name,
            sets,
            from,
            where_clause,
        })
    }

    fn parse_trigger_delete_cmd(&mut self) -> Result<TriggerCmd> {
        eat_assert!(self, TK_DELETE);
        eat_expect!(self, TK_FROM);
        let tbl_name = self.parse_nm()?;
        let where_clause = self.parse_where()?;
        Ok(TriggerCmd::Delete {
            tbl_name,
            where_clause,
        })
    }

    fn parse_trigger_cmd(&mut self) -> Result<TriggerCmd> {
        let tok = peek_expect!(
            self, TK_INSERT, TK_REPLACE, TK_UPDATE, TK_DELETE, TK_WITH, TK_SELECT, TK_VALUES,
        );

        let result = match tok.token_type.unwrap() {
            TK_WITH | TK_SELECT | TK_VALUES => TriggerCmd::Select(self.parse_select()?),
            TK_INSERT | TK_REPLACE => self.parse_trigger_insert_cmd()?,
            TK_UPDATE => self.parse_trigger_update_cmd()?,
            TK_DELETE => self.parse_trigger_delete_cmd()?,
            _ => unreachable!(),
        };

        eat_expect!(self, TK_SEMI);
        Ok(result)
    }

    fn parse_create_trigger(&mut self, temporary: bool) -> Result<Stmt> {
        eat_assert!(self, TK_TRIGGER);

        let if_not_exists = self.parse_if_not_exists()?;
        let trigger_name = self.parse_fullname(false)?;

        let trigger_time = match self.peek_no_eof()?.token_type.unwrap() {
            TK_BEFORE => {
                eat_assert!(self, TK_BEFORE);
                Some(TriggerTime::Before)
            }
            TK_AFTER => {
                eat_assert!(self, TK_AFTER);
                Some(TriggerTime::After)
            }
            TK_INSTEAD => {
                eat_assert!(self, TK_INSTEAD);
                eat_expect!(self, TK_OF);
                Some(TriggerTime::InsteadOf)
            }
            _ => None,
        };

        let tok = eat_expect!(self, TK_INSERT, TK_UPDATE, TK_DELETE);
        let trigger_event = match tok.token_type.unwrap() {
            TK_INSERT => TriggerEvent::Insert,
            TK_DELETE => TriggerEvent::Delete,
            TK_UPDATE => match self.peek_no_eof()?.token_type.unwrap() {
                TK_OF => {
                    eat_assert!(self, TK_OF);
                    TriggerEvent::UpdateOf(self.parse_nm_list()?)
                }
                _ => TriggerEvent::Update,
            },
            _ => unreachable!(),
        };

        eat_expect!(self, TK_ON);
        let tbl_name = self.parse_fullname(false)?;

        let foreach_clause = match self.peek_no_eof()?.token_type.unwrap() {
            TK_FOR => {
                eat_assert!(self, TK_FOR);
                eat_expect!(self, TK_EACH);
                eat_expect!(self, TK_ROW);
                true
            }
            _ => false,
        };

        let when_clause = match self.peek()? {
            Some(tok) if tok.token_type == Some(TK_WHEN) => {
                eat_assert!(self, TK_WHEN);
                Some(self.parse_expr(0)?)
            }
            _ => None,
        };

        eat_expect!(self, TK_BEGIN);

        let mut cmds = vec![self.parse_trigger_cmd()?];
        while let TK_UPDATE | TK_REPLACE | TK_INSERT | TK_DELETE | TK_WITH | TK_SELECT | TK_VALUES =
            self.peek_no_eof()?.token_type.unwrap()
        {
            cmds.push(self.parse_trigger_cmd()?);
        }

        eat_expect!(self, TK_END);

        Ok(Stmt::CreateTrigger {
            temporary,
            if_not_exists,
            trigger_name,
            time: trigger_time,
            event: trigger_event,
            tbl_name,
            for_each_row: foreach_clause,
            when_clause,
            commands: cmds,
        })
    }

    fn parse_delete_without_cte(&mut self, with: Option<With>) -> Result<Stmt> {
        eat_assert!(self, TK_DELETE);
        eat_expect!(self, TK_FROM);
        let tbl_name = self.parse_fullname(true)?;
        let indexed = self.parse_indexed()?;
        let where_clause = self.parse_where()?;
        let returning = self.parse_returning()?;
        let order_by = self.parse_order_by()?;
        let limit = self.parse_limit()?;
        if !order_by.is_empty() && limit.is_none() {
            return Err(Error::Custom("ORDER BY without LIMIT on DELETE".to_owned()));
        }
        Ok(Stmt::Delete {
            with,
            tbl_name,
            indexed,
            where_clause,
            returning,
            order_by,
            limit,
        })
    }

    fn parse_delete(&mut self) -> Result<Stmt> {
        let with = self.parse_with()?;
        self.parse_delete_without_cte(with)
    }

    fn parse_if_exists(&mut self) -> Result<bool> {
        match self.peek()? {
            Some(tok) if tok.token_type == Some(TK_IF) => {
                eat_assert!(self, TK_IF);
                eat_expect!(self, TK_EXISTS);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn parse_drop_stmt(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_DROP);
        let tok = peek_expect!(self, TK_TABLE, TK_INDEX, TK_TRIGGER, TK_VIEW);

        match tok.token_type.unwrap() {
            TK_TABLE => {
                eat_assert!(self, TK_TABLE);
                let if_exists = self.parse_if_exists()?;
                let tbl_name = self.parse_fullname(false)?;
                Ok(Stmt::DropTable {
                    if_exists,
                    tbl_name,
                })
            }
            TK_INDEX => {
                eat_assert!(self, TK_INDEX);
                let if_exists = self.parse_if_exists()?;
                let idx_name = self.parse_fullname(false)?;
                Ok(Stmt::DropIndex {
                    if_exists,
                    idx_name,
                })
            }
            TK_TRIGGER => {
                eat_assert!(self, TK_TRIGGER);
                let if_exists = self.parse_if_exists()?;
                let trigger_name = self.parse_fullname(false)?;
                Ok(Stmt::DropTrigger {
                    if_exists,
                    trigger_name,
                })
            }
            TK_VIEW => {
                eat_assert!(self, TK_VIEW);
                let if_exists = self.parse_if_exists()?;
                let view_name = self.parse_fullname(false)?;
                Ok(Stmt::DropView {
                    if_exists,
                    view_name,
                })
            }
            _ => unreachable!(),
        }
    }

    fn parse_insert_without_cte(&mut self, with: Option<With>) -> Result<Stmt> {
        let tok = eat_assert!(self, TK_INSERT, TK_REPLACE);
        let resolve_type = match tok.token_type.unwrap() {
            TK_INSERT => self.parse_or_conflict()?,
            TK_REPLACE => Some(ResolveType::Replace),
            _ => unreachable!(),
        };

        eat_expect!(self, TK_INTO);
        let tbl_name = self.parse_fullname(true)?;
        let columns = self.parse_nm_list_opt()?;
        let (body, returning) = match self.peek_no_eof()?.token_type.unwrap() {
            TK_DEFAULT => {
                if !columns.is_empty() {
                    return Err(Error::Custom(format!(
                        "0 values for {} columns",
                        columns.len()
                    )));
                }

                eat_assert!(self, TK_DEFAULT);
                eat_expect!(self, TK_VALUES);
                (InsertBody::DefaultValues, self.parse_returning()?)
            }
            _ => {
                let select = self.parse_select()?;
                if !columns.is_empty() {
                    if let ColumnCount::Fixed(n) = select.body.select.column_count() {
                        if n != columns.len() {
                            return Err(Error::Custom(format!(
                                "{} values for {} columns",
                                n,
                                columns.len()
                            )));
                        }
                    }
                }

                let (upsert, returning) = self.parse_upsert()?;
                (InsertBody::Select(select, upsert), returning)
            }
        };

        Ok(Stmt::Insert {
            with,
            or_conflict: resolve_type,
            tbl_name,
            columns,
            body,
            returning,
        })
    }

    fn parse_insert(&mut self) -> Result<Stmt> {
        let with = self.parse_with()?;
        self.parse_insert_without_cte(with)
    }

    fn parse_update_without_cte(&mut self, with: Option<With>) -> Result<Stmt> {
        eat_assert!(self, TK_UPDATE);
        let resolve_type = self.parse_or_conflict()?;
        let tbl_name = self.parse_fullname(true)?;
        let indexed = self.parse_indexed()?;
        eat_expect!(self, TK_SET);
        let sets = self.parse_set_list()?;
        let from = self.parse_from_clause_opt()?;
        let where_clause = self.parse_where()?;
        let returning = self.parse_returning()?;
        let order_by = self.parse_order_by()?;
        let limit = self.parse_limit()?;
        if !order_by.is_empty() && limit.is_none() {
            return Err(Error::Custom("ORDER BY without LIMIT on UPDATE".to_owned()));
        }
        Ok(Stmt::Update(Update {
            with,
            or_conflict: resolve_type,
            tbl_name,
            indexed,
            sets,
            from,
            where_clause,
            returning,
            order_by,
            limit,
        }))
    }

    fn parse_update(&mut self) -> Result<Stmt> {
        let with = self.parse_with()?;
        self.parse_update_without_cte(with)
    }

    fn parse_reindex(&mut self) -> Result<Stmt> {
        eat_assert!(self, TK_REINDEX);
        match self.peek()? {
            Some(tok) => match tok.token_type.unwrap().fallback_id_if_ok() {
                TK_ID | TK_STRING | TK_JOIN_KW | TK_INDEXED => Ok(Stmt::Reindex {
                    name: Some(self.parse_fullname(false)?),
                }),
                _ => Ok(Stmt::Reindex { name: None }),
            },
            _ => Ok(Stmt::Reindex { name: None }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset() {
        let s = "SELECT 1; SELECT 1";
        let mut p = Parser::new(s.as_bytes());
        p.next_cmd().unwrap();
        assert_eq!(&s[..p.offset()], "SELECT 1; ");
    }

    #[test]
    fn test_expect_fail() {
        let testcases = vec![
            "ALTER TABLE my_table ADD COLUMN my_column PRIMARY KEY",
            "ALTER TABLE my_table ADD COLUMN my_column UNIQUE",
            "CREATE TEMP TABLE baz.foo(bar)",
            "CREATE TABLE foo(rowid)",
            "CREATE TABLE foo(d INT AS (a*abs(b)))",
            "CREATE TABLE foo(d INT AS (a*abs(b)))",
            "CREATE TABLE foo(bar UNKNOWN_INT) STRICT",
            "CREATE TABLE foo(bar) STRICT",
            "CREATE TABLE foo(bar) WITHOUT ROWID",
            "CREATE VIEW foo(bar, bar) AS SELECT 1, 1",
            "CREATE VIEW foo(bar) AS SELECT 1, 1",
            "DELETE FROM my_table ORDER BY col1",
            "INSERT INTO my_table(bar) DEFAULT VALUES",
            "INSERT INTO my_table(bar, baz, barr) VALUES (1, 1)",
            "UPDATE foo SET bar = 1 ORDER BY bar",
        ];

        for tc in testcases {
            let mut p = Parser::new(tc.as_bytes());
            let result = p.next_cmd();
            assert!(result.is_err(), "Expected error for: {tc}");
        }
    }

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
                b"BEGIN CONCURRENT TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Concurrent),
                    name: None,
                })],
            ),
            (
                b"BEGIN CONCURRENT TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Concurrent),
                    name: Some(Name::Ident("my_transaction".to_string())),
                })],
            ),
            (
                b"BEGIN CONCURRENT TRANSACTION 'my_transaction'".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Concurrent),
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
            // test expr operand
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
            (
                b"SELECT (1)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Parenthesized(vec![Box::new(Expr::Literal(
                                    Literal::Numeric("1".to_owned()),
                                ))])),
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
            (
                b"SELECT NULL".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Null)),
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
            (
                b"SELECT X'ab'".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Blob("ab".to_owned()))),
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
            (
                b"SELECT 3.333".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("3.333".to_owned()))),
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
            (
                b"SELECT ?".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Variable("".to_owned())),
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
            (
                b"SELECT ?1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Variable("1".to_owned())),
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
            (
                b"SELECT CAST(1 AS INTEGER)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Cast {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    type_name: Some(Type {
                                        name: "INTEGER".to_owned(),
                                        size: None,
                                    }),
                                }),
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
            (
                b"SELECT CAST(1 AS VARCHAR(255))".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Cast {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    type_name: Some(Type {
                                        name: "VARCHAR".to_owned(),
                                        size: Some(TypeSize::MaxSize(Box::new(Expr::Literal(
                                            Literal::Numeric("255".to_owned()),
                                        )))),
                                    }),
                                }),
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
            (
                b"SELECT CAST(1 AS DECIMAL(10, 5))".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Cast {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    type_name: Some(Type {
                                        name: "DECIMAL".to_owned(),
                                        size: Some(TypeSize::TypeSize(
                                            Box::new(Expr::Literal(Literal::Numeric(
                                                "10".to_owned(),
                                            ))),
                                            Box::new(Expr::Literal(Literal::Numeric(
                                                "5".to_owned(),
                                            ))),
                                        )),
                                    }),
                                }),
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
            (
                b"SELECT CURRENT_DATE".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::CurrentDate)),
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
            (
                b"SELECT CURRENT_TIME".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::CurrentTime)),
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
            (
                b"SELECT CURRENT_TIMESTAMP".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::CurrentTimestamp)),
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
            (
                b"SELECT NOT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Unary(
                                    UnaryOperator::Not,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT NOT 1 + 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Unary(
                                    UnaryOperator::Not,
                                    Box::new(Expr::Binary(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Add,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                )),
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
            (
                b"SELECT ~1 + 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary(
                                    Box::new(Expr::Unary(
                                        UnaryOperator::BitwiseNot,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                    Operator::Add,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT +1 + 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary(
                                    Box::new(Expr::Unary(
                                        UnaryOperator::Positive,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                    Operator::Add,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT -1 + 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary(
                                    Box::new(Expr::Unary(
                                        UnaryOperator::Negative,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                    Operator::Add,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT EXISTS (SELECT 1)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Exists(Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric(
                                                    "1".to_owned(),
                                                ))),
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
                                })),
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
            (
                b"SELECT CASE WHEN 1 THEN 2 ELSE 3 END".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Case {
                                    base: None,
                                    when_then_pairs: vec![(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )],
                                    else_expr: Some(Box::new(Expr::Literal(Literal::Numeric(
                                        "3".to_owned(),
                                    )))),
                                }),
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
            (
                b"SELECT CASE 4 WHEN 1 THEN 2 ELSE 3 END".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Case {
                                    base: Some(Box::new(Expr::Literal(Literal::Numeric(
                                        "4".to_owned(),
                                    )))),
                                    when_then_pairs: vec![(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )],
                                    else_expr: Some(Box::new(Expr::Literal(Literal::Numeric(
                                        "3".to_owned(),
                                    )))),
                                }),
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
            (
                b"SELECT CASE 4 WHEN 1 THEN 2 END".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Case {
                                    base: Some(Box::new(Expr::Literal(Literal::Numeric(
                                        "4".to_owned(),
                                    )))),
                                    when_then_pairs: vec![(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )],
                                    else_expr: None,
                                }),
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
            (
                b"SELECT (SELECT 1)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Subquery(Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric(
                                                    "1".to_owned(),
                                                ))),
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
                                })),
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
            (
                b"SELECT RAISE (Ignore)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Raise(ResolveType::Ignore, None)),
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
            (
                b"SELECT RAISE (FAIL, 'error')".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Raise(
                                    ResolveType::Fail,
                                    Some(Box::new(Expr::Literal(Literal::String(
                                        "'error'".to_owned(),
                                    )))),
                                )),
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
            (
                b"SELECT RAISE (ROLLBACK, 'error')".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Raise(
                                    ResolveType::Rollback,
                                    Some(Box::new(Expr::Literal(Literal::String(
                                        "'error'".to_owned(),
                                    )))),
                                )),
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
            (
                b"SELECT RAISE (ABORT, 'error')".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Raise(
                                    ResolveType::Abort,
                                    Some(Box::new(Expr::Literal(Literal::String(
                                        "'error'".to_owned(),
                                    )))),
                                )),
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
            (
                b"SELECT RAISE (ABORT, 'error')".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Raise(
                                    ResolveType::Abort,
                                    Some(Box::new(Expr::Literal(Literal::String(
                                        "'error'".to_owned(),
                                    )))),
                                )),
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
            (
                b"SELECT col_1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Id(Name::Ident("col_1".to_owned()))),
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
            (
                b"SELECT 'col_1'".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::String("'col_1'".to_owned()))),
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
            (
                b"SELECT tbl_name.col_1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Qualified(
                                    Name::Ident("tbl_name".to_owned()),
                                    Name::Ident("col_1".to_owned()),
                                )),
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
            (
                b"SELECT schema_name.tbl_name.col_1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::DoublyQualified(
                                    Name::Ident("schema_name".to_owned()),
                                    Name::Ident("tbl_name".to_owned()),
                                    Name::Ident("col_1".to_owned()),
                                )),
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
            (
                b"SELECT func_name()".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: None,
                                    args: vec![],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: None,
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) FILTER (WHERE x) OVER window_name".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: Some(Box::new(Expr::Id(Name::Ident(
                                            "x".to_owned(),
                                        )))),
                                        over_clause: Some(Over::Name(Name::Ident(
                                            "window_name".to_owned(),
                                        ))),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (PARTITION BY product)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: None,
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: None,
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: None,
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product ORDER BY test ASC NULLS LAST)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![
                                                SortedColumn {
                                                    expr: Box::new(Expr::Id(Name::Ident("test".to_owned()))),
                                                    order: Some(SortOrder::Asc),
                                                    nulls: Some(NullsOrder::Last),
                                                }
                                            ],
                                            frame_clause: None,
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Rows,
                                                start: FrameBound::Preceding(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                                                end: Some(FrameBound::CurrentRow),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Range,
                                                start: FrameBound::Preceding(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                                                end: Some(FrameBound::CurrentRow),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN 2 PRECEDING AND CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::Preceding(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                                                end: Some(FrameBound::CurrentRow),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN 2 FOLLOWING AND CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::Following(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                                                end: Some(FrameBound::CurrentRow),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::UnboundedPreceding,
                                                end: Some(FrameBound::CurrentRow),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN CURRENT ROW AND CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: Some(FrameBound::CurrentRow),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: Some(FrameBound::UnboundedFollowing),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN CURRENT ROW AND 1 PRECEDING)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: Some(FrameBound::Preceding(
                                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                                                )),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: Some(FrameBound::Following(
                                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                                                )),
                                                exclude: None
                                            }),
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS CURRENT ROW EXCLUDE NO OTHERS)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: None,
                                                exclude: Some(FrameExclude::NoOthers)
                                            }),
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS CURRENT ROW EXCLUDE CURRENT ROW)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: None,
                                                exclude: Some(FrameExclude::CurrentRow)
                                            }),
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS CURRENT ROW EXCLUDE GROUP)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: None,
                                                exclude: Some(FrameExclude::Group)
                                            }),
                                        })),
                                    },
                                }),
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
            (
                b"SELECT func_name(DISTINCT 1, 2) OVER (test PARTITION BY product GROUPS CURRENT ROW EXCLUDE TIES)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::FunctionCall {
                                    name: Name::Ident("func_name".to_owned()),
                                    distinctness: Some(Distinctness::Distinct),
                                    args: vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    order_by: vec![],
                                    filter_over: FunctionTail {
                                        filter_clause: None,
                                        over_clause: Some(Over::Window(Window {
                                            base: Some(Name::Ident("test".to_owned())),
                                            partition_by: vec![Box::new(Expr::Id(Name::Ident(
                                                "product".to_owned(),
                                            )))],
                                            order_by: vec![],
                                            frame_clause: Some(FrameClause{
                                                mode: FrameMode::Groups,
                                                start: FrameBound::CurrentRow,
                                                end: None,
                                                exclude: Some(FrameExclude::Ties)
                                            }),
                                        })),
                                    },
                                }),
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
            // parse expr
            (
                b"SELECT 1 NOT NULL AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::NotNull(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 IS 1 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::Is,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                            )),
                            Operator::And,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        )),
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
            (
                b"SELECT 1 IS NOT 1 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::IsNot,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                            )),
                            Operator::And,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        )),
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
            (
                b"SELECT 1 IS NOT DISTINCT FROM 1 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::Is,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                            )),
                            Operator::And,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        )),
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
            (
                b"SELECT 1 IS DISTINCT FROM 1 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::IsNot,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                            )),
                            Operator::And,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        )),
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
            (
                b"SELECT 1 + 2 * 3".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            Operator::Add,
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                Operator::Multiply,
                                Box::new(Expr::Literal(Literal::Numeric("3".to_owned())))
                            ))
                        )),
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
            (
                b"SELECT 1 AND 2 OR 3".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::And,
                                Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))
                            )),
                            Operator::Or,
                            Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                        )),
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
            (
                b"SELECT 1 = 0 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::Equals,
                                Box::new(Expr::Literal(Literal::Numeric("0".to_owned())))
                            )),
                            Operator::And,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        )),
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
            (
                b"SELECT 1 != 0 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                select: OneSelect::Select {
                    distinctness: None,
                    columns: vec![ResultColumn::Expr(
                        Box::new(Expr::Binary (
                            Box::new(Expr::Binary (
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::NotEquals,
                                Box::new(Expr::Literal(Literal::Numeric("0".to_owned())))
                            )),
                            Operator::And,
                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        )),
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
            (
                b"SELECT 1 BETWEEN 2 AND 3 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Between {
                                        lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        not: false,
                                        start: Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        end: Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 NOT BETWEEN 2 AND 3 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Between {
                                        lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        not: true,
                                        start: Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        end: Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 IN (SELECT 1) AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::InSelect {
                                        lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        not: false,
                                        rhs: Select {
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
                                            limit: None
                                        },
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 NOT IN (SELECT 1) AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::InSelect {
                                        lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        not: true,
                                        rhs: Select {
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
                                            limit: None
                                        },
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 IN (1, 2, 3) AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::InList {
                                        lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        not: false,
                                        rhs: vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                        ],
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 IN test(1, 2, 3) AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::InTable {
                                        lhs: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        not: false,
                                        rhs: QualifiedName {
                                            db_name: None,
                                            name: Name::Ident("test".to_owned()),
                                            alias: None,
                                        },
                                        args: vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                        ],
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 'test' MATCH 'foo' AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Like {
                                        lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                        not: false,
                                        op: LikeOperator::Match,
                                        rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        escape: None,
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 'test' NOT MATCH 'foo' AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Like {
                                        lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                        not: true,
                                        op: LikeOperator::Match,
                                        rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        escape: None,
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 'test' NOT MATCH 'foo' ESCAPE 'bar' AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Like {
                                        lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                        not: true,
                                        op: LikeOperator::Match,
                                        rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        escape: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 'test' NOT LIKE 'foo' ESCAPE 'bar' AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Like {
                                        lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                        not: true,
                                        op: LikeOperator::Like,
                                        rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        escape: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 'test' NOT GLOB 'foo' ESCAPE 'bar' AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Like {
                                        lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                        not: true,
                                        op: LikeOperator::Glob,
                                        rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        escape: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 'test' NOT REGEXP 'foo' ESCAPE 'bar' AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Like {
                                        lhs: Box::new(Expr::Literal(Literal::String("'test'".to_owned()))),
                                        not: true,
                                        op: LikeOperator::Regexp,
                                        rhs: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        escape: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                                    }),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 ISNULL AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::IsNull (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 NOTNULL AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::NotNull(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 < 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Less,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 > 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Greater,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 <= 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::LessEquals,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 >= 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::GreaterEquals,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 & 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::BitwiseAnd,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 | 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::BitwiseOr,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 << 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::LeftShift,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 >> 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::RightShift,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 / 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Divide,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 % 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Modulus,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 || 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Concat,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 -> 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::ArrowRight,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 1 ->> 2 AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Binary (
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::ArrowRightShift,
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            (
                b"SELECT 'foo' COLLATE bar AND 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Binary (
                                    Box::new(Expr::Collate (
                                        Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                                        Name::Ident("bar".to_owned()),
                                    )),
                                    Operator::And,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                )),
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
            // test select
            (
                b"VALUES (1, 2), (3, 4), (5, 6)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Values(vec![
                            vec![
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                            ],
                            vec![
                                Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                Box::new(Expr::Literal(Literal::Numeric("4".to_owned()))),
                            ],
                            vec![
                                Box::new(Expr::Literal(Literal::Numeric("5".to_owned()))),
                                Box::new(Expr::Literal(Literal::Numeric("6".to_owned()))),
                            ],
                        ]),
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT *".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Star],
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
            (
                b"SELECT tbl_name.*".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::TableStar(
                                Name::Ident("tbl_name".to_owned()),
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
            (
                b"SELECT col_1 OVER".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Id(Name::Ident("col_1".to_owned()))),
                                Some(As::Elided(Name::Ident("OVER".to_owned()))),
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
            (
                b"SELECT col_1 AS OVER".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Id(Name::Ident("col_1".to_owned()))),
                                Some(As::As(Name::Ident("OVER".to_owned()))),
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
            (
                b"WITH test AS (SELECT 1) SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::Ident("test".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
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
                                }
                            },
                        ]
                    }),
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
            (
                b"WITH test(col_1) AS MATERIALIZED (SELECT 1 AS col_1) SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::Ident("test".to_owned()),
                                columns: vec![
                                    IndexedColumn {
                                        col_name: Name::Ident("col_1".to_owned()),
                                        collation_name: None,
                                        order: None,
                                    },
                                ],
                                materialized: Materialized::Yes,
                                select: Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                Some(As::As(Name::Ident("col_1".to_owned()))),
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
                                }
                            },
                        ]
                    }),
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
            (
                b"WITH test(col_1) AS NOT MATERIALIZED (SELECT 1 AS col_1) SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::Ident("test".to_owned()),
                                columns: vec![
                                    IndexedColumn {
                                        col_name: Name::Ident("col_1".to_owned()),
                                        collation_name: None,
                                        order: None,
                                    },
                                ],
                                materialized: Materialized::No,
                                select: Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                Some(As::As(Name::Ident("col_1".to_owned()))),
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
                                }
                            },
                        ]
                    }),
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
            (
                b"WITH test AS (SELECT 1), test_2 AS (SELECT 1) SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::Ident("test".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                None
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
                                }
                            },
                            CommonTableExpr {
                                tbl_name: Name::Ident("test_2".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
                                    with: None,
                                    body: SelectBody {
                                        select: OneSelect::Select {
                                            distinctness: None,
                                            columns: vec![ResultColumn::Expr(
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                None
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
                                }
                            },
                        ]
                    }),
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
            (
                b"SELECT 1 ORDER BY 1".as_slice(),
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
                    order_by: vec![
                        SortedColumn {
                            expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            order: None,
                            nulls: None,
                        },
                    ],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 ORDER BY 1 DESC NULLS FIRST".as_slice(),
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
                    order_by: vec![
                        SortedColumn {
                            expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            order: Some(SortOrder::Desc),
                            nulls: Some(NullsOrder::First),
                        },
                    ],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 ORDER BY 1 ASC NULLS LAST".as_slice(),
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
                    order_by: vec![
                        SortedColumn {
                            expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            order: Some(SortOrder::Asc),
                            nulls: Some(NullsOrder::Last),
                        },
                    ],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 LIMIT 1".as_slice(),
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
                    limit: Some(Limit {
                        expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        offset: None,
                    }),
                }))],
            ),
            (
                b"SELECT 1 LIMIT 1,2".as_slice(),
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
                    limit: Some(Limit {
                        expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        offset: Some(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                    }),
                }))],
            ),
            (
                b"SELECT 1 LIMIT 1 OFFSET 2".as_slice(),
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
                    limit: Some(Limit {
                        expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        offset: Some(Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))),
                    }),
                }))],
            ),
            (
                b"SELECT 1 UNION SELECT 2".as_slice(),
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
                        compounds: vec![
                            CompoundSelect {
                                operator: CompoundOperator::Union,
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                }
                            }
                        ],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 UNION ALL SELECT 2".as_slice(),
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
                        compounds: vec![
                            CompoundSelect {
                                operator: CompoundOperator::UnionAll,
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                }
                            }
                        ],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 EXCEPT SELECT 2".as_slice(),
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
                        compounds: vec![
                            CompoundSelect {
                                operator: CompoundOperator::Except,
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                }
                            }
                        ],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 INTERSECT SELECT 2".as_slice(),
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
                        compounds: vec![
                            CompoundSelect {
                                operator: CompoundOperator::Intersect,
                                select: OneSelect::Select {
                                    distinctness: None,
                                    columns: vec![ResultColumn::Expr(
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        None,
                                    )],
                                    from: None,
                                    where_clause: None,
                                    group_by: None,
                                    window_clause: vec![],
                                }
                            }
                        ],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
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
            (
                b"SELECT 1 FROM foo(1, 2)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::TableCall(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                    ],
                                    None,
                                )),
                                joins: vec![]
                            }),
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
            (
                b"SELECT 1 FROM (SELECT 1)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Select(
                                    Select {
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
                                    },
                                    None,
                                )),
                                joins: vec![]
                            }),
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
            (
                b"SELECT 1 FROM (tbl_name)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Sub(
                                    FromClause {
                                        select: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("tbl_name".to_owned()), alias: None },
                                            None,
                                            None
                                        )),
                                        joins: vec![]
                                    },
                                    None,
                                )),
                                joins: vec![]
                            }),
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
            (
                b"SELECT 1 FROM foo INDEXED BY bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    Some(Indexed::IndexedBy(Name::Ident("bar".to_owned()))),
                                )),
                                joins: vec![]
                            }),
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
            (
                b"SELECT 1 FROM foo NOT INDEXED".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    Some(Indexed::NotIndexed),
                                )),
                                joins: vec![]
                            }),
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
            (
                b"SELECT 1 FROM foo, bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::Comma,
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo, bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::Comma,
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo NATURAL JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::NATURAL)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo CROSS JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::INNER|JoinType::CROSS)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo LEFT JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::LEFT|JoinType::OUTER)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo RIGHT JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::RIGHT|JoinType::OUTER)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo FULL JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::LEFT | JoinType::RIGHT | JoinType::OUTER)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo INNER JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::INNER)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo NATURAL INNER JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::NATURAL | JoinType::INNER)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo NATURAL LEFT OUTER JOIN bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(Some(JoinType::NATURAL | JoinType::LEFT | JoinType::OUTER)),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo JOIN bar ON 1 = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: Some(JoinConstraint::On(Box::new(Expr::Binary(
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Operator::Equals,
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        )))),
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo JOIN bar USING (col_1)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: Some(JoinConstraint::Using(vec![
                                            Name::Ident("col_1".to_owned()),
                                        ])),
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo JOIN bar bar_alias USING (col_1)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            Some(As::Elided(Name::Ident("bar_alias".to_owned()))),
                                            None,
                                        )),
                                        constraint: Some(JoinConstraint::Using(vec![
                                            Name::Ident("col_1".to_owned()),
                                        ])),
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo JOIN bar(1, 2)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::TableCall(
                                            QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                            vec![
                                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                            ],
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo JOIN (VALUES (1,2), (3, 4))".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Select(
                                            Select {
                                                with: None,
                                                body: SelectBody {
                                                    select: OneSelect::Values(vec![
                                                    vec![
                                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                                        Box::new(Expr::Literal(Literal::Numeric("2".to_owned())))
                                                    ],
                                                    vec![
                                                        Box::new(Expr::Literal(Literal::Numeric("3".to_owned()))),
                                                        Box::new(Expr::Literal(Literal::Numeric("4".to_owned())))
                                                    ],
                                                    ]),
                                                    compounds: vec![],
                                                },
                                                order_by: vec![],
                                                limit: None,
                                            },
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo JOIN (bar)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Sub(
                                            FromClause {
                                                select: Box::new(SelectTable::Table(
                                                    QualifiedName { db_name: None, name: Name::Ident("bar".to_owned()), alias: None },
                                                    None,
                                                    None,
                                                )),
                                                joins: vec![]
                                            },
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT 1 FROM foo WHERE 1 = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: Some(Box::new(Expr::Binary(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                Operator::Equals,
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                            ))),
                            group_by: None,
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo GROUP BY 1 = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: Some(GroupBy {
                                exprs: vec![
                                    Box::new(Expr::Binary(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Equals,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                ],
                                having: None,
                            }),
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo GROUP BY 1 = 1 HAVING 1 = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: Some(GroupBy {
                                exprs: vec![
                                    Box::new(Expr::Binary(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Equals,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                ],
                                having: Some(Box::new(Expr::Binary(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::Equals,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                ))),
                            }),
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT 1 FROM foo GROUP BY 1 = 1 HAVING 1 = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Expr(
                                Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                None,
                            )],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: Some(GroupBy {
                                exprs: vec![
                                    Box::new(Expr::Binary(
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        Operator::Equals,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    )),
                                ],
                                having: Some(Box::new(Expr::Binary(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Operator::Equals,
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                ))),
                            }),
                            window_clause: vec![],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT * FROM t0 WINDOW JOIN t0;".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Star],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("t0".to_owned()), alias: None },
                                    Some(As::Elided(Name::Ident("WINDOW".to_owned()))),
                                    None,
                                )),
                                joins: vec![
                                    JoinedSelectTable {
                                        operator: JoinOperator::TypedJoin(None),
                                        table: Box::new(SelectTable::Table(
                                            QualifiedName { db_name: None, name: Name::Ident("t0".to_owned()), alias: None },
                                            None,
                                            None,
                                        )),
                                        constraint: None,
                                    }
                                ]
                            }),
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
            (
                b"SELECT * FROM t0 WINDOW window_1 AS (PARTITION BY product)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Star],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("t0".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![
                                WindowDef {
                                    name: Name::Ident("window_1".to_owned()),
                                    window: Window {
                                        base: None,
                                        partition_by: vec![
                                            Box::new(Expr::Id(Name::Ident("product".to_owned()))),
                                        ],
                                        order_by: vec![],
                                        frame_clause: None,
                                    },
                                }
                            ],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"SELECT * FROM t0 WINDOW window_1 AS (PARTITION BY product), window_2 AS (PARTITION BY product_2)".as_slice(),
                vec![Cmd::Stmt(Stmt::Select(Select {
                    with: None,
                    body: SelectBody {
                        select: OneSelect::Select {
                            distinctness: None,
                            columns: vec![ResultColumn::Star],
                            from: Some(FromClause {
                                select: Box::new(SelectTable::Table(
                                    QualifiedName { db_name: None, name: Name::Ident("t0".to_owned()), alias: None },
                                    None,
                                    None,
                                )),
                                joins: vec![]
                            }),
                            where_clause: None,
                            group_by: None,
                            window_clause: vec![
                                WindowDef {
                                    name: Name::Ident("window_1".to_owned()),
                                    window: Window {
                                        base: None,
                                        partition_by: vec![
                                            Box::new(Expr::Id(Name::Ident("product".to_owned()))),
                                        ],
                                        order_by: vec![],
                                        frame_clause: None,
                                    },
                                },
                                WindowDef {
                                    name: Name::Ident("window_2".to_owned()),
                                    window: Window {
                                        base: None,
                                        partition_by: vec![
                                            Box::new(Expr::Id(Name::Ident("product_2".to_owned()))),
                                        ],
                                        order_by: vec![],
                                        frame_clause: None,
                                    },
                                }
                            ],
                        },
                        compounds: vec![],
                    },
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            // parse Analyze
            (
                b"ANALYZE".as_slice(),
                vec![Cmd::Stmt(Stmt::Analyze {
                    name: None,
                })],
            ),
            (
                b"ANALYZE foo".as_slice(),
                vec![Cmd::Stmt(Stmt::Analyze {
                    name: Some(QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None }),
                })],
            ),
            // parse attach
            (
                b"ATTACH DATABASE 'foo' AS bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Attach {
                    expr: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                    db_name: Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                    key: None,
                })],
            ),
            (
                b"ATTACH 'foo' AS bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Attach {
                    expr: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                    db_name: Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                    key: None,
                })],
            ),
            (
                b"ATTACH 'foo' AS bar key baz".as_slice(),
                vec![Cmd::Stmt(Stmt::Attach {
                    expr: Box::new(Expr::Literal(Literal::String("'foo'".to_owned()))),
                    db_name: Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                    key: Some(Box::new(Expr::Id(Name::Ident("baz".to_owned())))),
                })],
            ),
            // parse detach
            (
                b"DETACH DATABASE bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Detach {
                    name: Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                })],
            ),
            (
                b"DETACH bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Detach {
                    name: Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                })],
            ),
            // parse pragma
            (
                b"PRAGMA foreign_keys = ON".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::Ident("foreign_keys".to_owned()),  alias: None },
                    body: Some(PragmaBody::Equals(Box::new(Expr::Literal(Literal::Keyword("ON".to_owned()))))),
                })],
            ),
            (
                b"PRAGMA foreign_keys = DELETE".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::Ident("foreign_keys".to_owned()),  alias: None },
                    body: Some(PragmaBody::Equals(Box::new(Expr::Literal(Literal::Keyword("DELETE".to_owned()))))),
                })],
            ),
            (
                b"PRAGMA foreign_keys = DEFAULT".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::Ident("foreign_keys".to_owned()),  alias: None },
                    body: Some(PragmaBody::Equals(Box::new(Expr::Literal(Literal::Keyword("DEFAULT".to_owned()))))),
                })],
            ),
            (
                b"PRAGMA foreign_keys".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::Ident("foreign_keys".to_owned()),  alias: None },
                    body: None,
                })],
            ),
            (
                b"PRAGMA foreign_keys = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::Ident("foreign_keys".to_owned()),  alias: None },
                    body: Some(PragmaBody::Equals(Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))))),
                })],
            ),
            (
                b"PRAGMA foreign_keys = test".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::Ident("foreign_keys".to_owned()),  alias: None },
                    body: Some(PragmaBody::Equals(Box::new(Expr::Name(Name::Ident("test".to_owned()))))),
                })],
            ),
            (
                b"PRAGMA foreign_keys".as_slice(),
                vec![Cmd::Stmt(Stmt::Pragma {
                    name: QualifiedName { db_name: None, name: Name::Ident("foreign_keys".to_owned()),  alias: None },
                    body: None,
                })],
            ),
            // parse vacuum
            (
                b"VACUUM".as_slice(),
                vec![Cmd::Stmt(Stmt::Vacuum {
                    name: None,
                    into: None,
                })],
            ),
            (
                b"VACUUM INTO 'foo'".as_slice(),
                vec![Cmd::Stmt(Stmt::Vacuum {
                    name: None,
                    into: Some(Box::new(Expr::Literal(Literal::String("'foo'".to_owned())))),
                })],
            ),
            (
                b"VACUUM INTO foo".as_slice(),
                vec![Cmd::Stmt(Stmt::Vacuum {
                    name: None,
                    into: Some(Box::new(Expr::Id(Name::Ident("foo".to_owned())))),
                })],
            ),
            (
                b"VACUUM foo".as_slice(),
                vec![Cmd::Stmt(Stmt::Vacuum {
                    name: Some(Name::Ident("foo".to_owned())),
                    into: None,
                })],
            ),
            (
                b"VACUUM foo INTO 'bar'".as_slice(),
                vec![Cmd::Stmt(Stmt::Vacuum {
                    name: Some(Name::Ident("foo".to_owned())),
                    into: Some(Box::new(Expr::Literal(Literal::String("'bar'".to_owned())))),
                })],
            ),
            // parse alter
            (
                b"ALTER TABLE foo RENAME TO bar".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::RenameTo(Name::Ident("bar".to_owned())),
                }))],
            ),
            (
                b"ALTER TABLE foo RENAME baz TO bar".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::RenameColumn {
                        old: Name::Ident("baz".to_owned()),
                        new: Name::Ident("bar".to_owned())
                    },
                }))],
            ),
            (
                b"ALTER TABLE foo RENAME COLUMN baz TO bar".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::RenameColumn {
                        old: Name::Ident("baz".to_owned()),
                        new: Name::Ident("bar".to_owned())
                    },
                }))],
            ),
            (
                b"ALTER TABLE foo DROP baz".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::DropColumn(Name::Ident("baz".to_owned())),
                }))],
            ),
            (
                b"ALTER TABLE foo DROP COLUMN baz".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::DropColumn(Name::Ident("baz".to_owned())),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD baz".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: None,
                        constraints: vec![],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Default(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))
                                ),
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT (1)".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Default(
                                    Box::new(Expr::Parenthesized(vec![
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    ]))
                                ),
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT +1".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Default(
                                    Box::new(Expr::Unary(
                                        UnaryOperator::Positive,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    ))
                                ),
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT -1".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Default(
                                    Box::new(Expr::Unary(
                                        UnaryOperator::Negative,
                                        Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    ))
                                ),
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER DEFAULT hello".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Default(
                                    Box::new(Expr::Id(Name::Ident("hello".to_owned())))
                                ),
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER NULL".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::NotNull {
                                    nullable: true,
                                    conflict_clause: None,
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER NOT NULL ON CONFLICT IGNORE".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::NotNull {
                                    nullable: false,
                                    conflict_clause: Some(ResolveType::Ignore),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER NOT NULL ON CONFLICT REPLACE".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::NotNull {
                                    nullable: false,
                                    conflict_clause: Some(ResolveType::Replace),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER NOT NULL ON CONFLICT ROLLBACK".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::NotNull {
                                    nullable: false,
                                    conflict_clause: Some(ResolveType::Rollback),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER NOT NULL ON CONFLICT ROLLBACK".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::NotNull {
                                    nullable: false,
                                    conflict_clause: Some(ResolveType::Rollback),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER CHECK (1)".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable(AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Check(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                ),
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER CHECK (1)".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Check(
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                ),
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::Ident("bar".to_owned()),
                                        columns: vec![],
                                        args: vec![]
                                    },
                                    deref_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON INSERT SET NULL".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::Ident("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::Ident("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::Ident("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::Ident("test_3".to_owned())),
                                            RefArg::OnInsert(RefAct::SetNull),
                                        ]
                                    },
                                    deref_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON UPDATE SET NULL".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::Ident("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::Ident("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::Ident("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::Ident("test_3".to_owned())),
                                            RefArg::OnUpdate(RefAct::SetNull),
                                        ]
                                    },
                                    deref_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE SET NULL".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::Ident("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::Ident("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::Ident("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::Ident("test_3".to_owned())),
                                            RefArg::OnDelete(RefAct::SetNull),
                                        ]
                                    },
                                    deref_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE SET DEFAULT".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::Ident("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::Ident("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::Ident("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::Ident("test_3".to_owned())),
                                            RefArg::OnDelete(RefAct::SetDefault),
                                        ]
                                    },
                                    deref_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE CASCADE".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::Ident("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::Ident("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::Ident("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::Ident("test_3".to_owned())),
                                            RefArg::OnDelete(RefAct::Cascade),
                                        ]
                                    },
                                    deref_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE RESTRICT".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::Ident("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::Ident("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::Ident("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::Ident("test_3".to_owned())),
                                            RefArg::OnDelete(RefAct::Restrict),
                                        ]
                                    },
                                    deref_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar(test, test_2) MATCH test_3 ON DELETE NO ACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::Ident("bar".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::Ident("test".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                            IndexedColumn {
                                                col_name: Name::Ident("test_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![
                                            RefArg::Match(Name::Ident("test_3".to_owned())),
                                            RefArg::OnDelete(RefAct::NoAction),
                                        ]
                                    },
                                    deref_clause: None
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar DEFERRABLE".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::Ident("bar".to_owned()),
                                        columns: vec![],
                                        args: vec![]
                                    },
                                    deref_clause: Some(DeferSubclause {
                                        deferrable: true,
                                        init_deferred: None,
                                    })
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar NOT DEFERRABLE INITIALLY IMMEDIATE".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::Ident("bar".to_owned()),
                                        columns: vec![],
                                        args: vec![]
                                    },
                                    deref_clause: Some(DeferSubclause {
                                        deferrable: false,
                                        init_deferred: Some(InitDeferredPred::InitiallyImmediate),
                                    })
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar NOT DEFERRABLE INITIALLY DEFERRED".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::Ident("bar".to_owned()),
                                        columns: vec![],
                                        args: vec![]
                                    },
                                    deref_clause: Some(DeferSubclause {
                                        deferrable: false,
                                        init_deferred: Some(InitDeferredPred::InitiallyDeferred),
                                    })
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER REFERENCES bar NOT DEFERRABLE INITIALLY DEFERRED".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::ForeignKey {
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::Ident("bar".to_owned()),
                                        columns: vec![],
                                        args: vec![]
                                    },
                                    deref_clause: Some(DeferSubclause {
                                        deferrable: false,
                                        init_deferred: Some(InitDeferredPred::InitiallyDeferred),
                                    })
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER COLLATE bar".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Collate {
                                    collation_name: Name::Ident("bar".to_owned()),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER GENERATED ALWAYS AS (1)".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Generated {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    typ: None,
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER AS (1)".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Generated {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    typ: None,
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ADD COLUMN baz INTEGER AS (1) STORED".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AddColumn(ColumnDefinition {
                        col_name: Name::Ident("baz".to_owned()),
                        col_type: Some(Type {
                            name: "INTEGER".to_owned(),
                            size: None,
                        }),
                        constraints: vec![
                            NamedColumnConstraint {
                                name: None,
                                constraint: ColumnConstraint::Generated {
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    typ: Some(Name::Ident("STORED".to_owned())),
                                },
                            },
                        ],
                    }),
                }))],
            ),
            (
                b"ALTER TABLE foo ALTER COLUMN bar TO baz INTEGER".as_slice(),
                vec![Cmd::Stmt(Stmt::AlterTable (AlterTable {
                    name: QualifiedName { db_name: None, name: Name::Ident("foo".to_owned()), alias: None },
                    body: AlterTableBody::AlterColumn {
                        old: Name::Ident("bar".to_owned()),
                        new: ColumnDefinition {
                            col_name: Name::Ident("baz".to_owned()),
                            col_type: Some(Type {
                                name: "INTEGER".to_owned(),
                                size: None,
                            }),
                            constraints: vec![],
                        },

                    },
                }))],
            ),
            // parse create index
            (
                b"CREATE INDEX idx_foo ON foo (bar)".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateIndex {
                    unique: false,
                    if_not_exists: false,
                    idx_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("idx_foo".to_owned()),
                        alias: None,
                    },
                    tbl_name: Name::Ident("foo".to_owned()),
                    columns: vec![SortedColumn {
                        expr: Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                        order: None,
                        nulls: None,
                    }],
                    where_clause: None,
                })],
            ),
            (
                b"CREATE UNIQUE INDEX IF NOT EXISTS idx_foo ON foo (bar) WHERE 1".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateIndex {
                    unique: true,
                    if_not_exists: true,
                    idx_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("idx_foo".to_owned()),
                        alias: None,
                    },
                    tbl_name: Name::Ident("foo".to_owned()),
                    columns: vec![SortedColumn {
                        expr: Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                        order: None,
                        nulls: None,
                    }],
                    where_clause: Some(Box::new(
                        Expr::Literal(Literal::Numeric("1".to_owned()))
                      )),
                })],
            ),
            // parse create table
            (
                b"CREATE TABLE foo (column)".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: false,
                    if_not_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::ColumnsAndConstraints {
                        columns: vec![
                            ColumnDefinition {
                                col_name: Name::Ident("column".to_owned()),
                                col_type: None,
                                constraints: vec![],
                            },
                        ],
                        constraints: vec![],
                        options: TableOptions::NONE,
                    },
                })],
            ),
            (
                b"CREATE TABLE foo AS SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: false,
                    if_not_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::AsSelect(Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Select {
                                distinctness: None,
                                columns: vec![
                                    ResultColumn::Expr(Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))), None)
                                ],
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: vec![],
                            },
                            compounds: vec![]
                        },
                        order_by: vec![],
                        limit: None,
                    }),
                })],
            ),
            (
                b"CREATE TEMP TABLE IF NOT EXISTS foo (baz INTEGER, CONSTRAINT tbl_cons PRIMARY KEY (bar AUTOINCREMENT) ON CONFLICT ROLLBACK) STRICT".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: true,
                    if_not_exists: true,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::ColumnsAndConstraints {
                        columns: vec![
                            ColumnDefinition {
                                col_name: Name::Ident("baz".to_owned()),
                                col_type: Some(Type {
                                    name: "INTEGER".to_owned(),
                                    size: None,
                                }),
                                constraints: vec![],
                            },
                        ],
                        constraints: vec![
                            NamedTableConstraint {
                                name: Some(Name::Ident("tbl_cons".to_owned())),
                                constraint: TableConstraint::PrimaryKey {
                                    columns: vec![
                                        SortedColumn {
                                            expr: Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                                            order: None,
                                            nulls: None,
                                        },
                                    ],
                                    auto_increment: true,
                                    conflict_clause:  Some(ResolveType::Rollback)
                                }
                            },
                        ],
                        options: TableOptions::STRICT,
                    },
                })],
            ),
            (
                b"CREATE TEMP TABLE IF NOT EXISTS foo (bar INTEGER PRIMARY KEY, baz INTEGER, UNIQUE (bar) ON CONFLICT ROLLBACK) WITHOUT ROWID".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: true,
                    if_not_exists: true,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::ColumnsAndConstraints {
                        columns: vec![
                            ColumnDefinition {
                                col_name: Name::Ident("bar".to_owned()),
                                col_type: Some(Type {
                                    name: "INTEGER".to_owned(),
                                    size: None,
                                }),
                                constraints: vec![
                                    NamedColumnConstraint {
                                        name: None,
                                        constraint: ColumnConstraint::PrimaryKey {
                                            order: None,
                                            conflict_clause: None,
                                            auto_increment: false,
                                        }
                                    }
                                ],
                            },
                            ColumnDefinition {
                                col_name: Name::Ident("baz".to_owned()),
                                col_type: Some(Type {
                                    name: "INTEGER".to_owned(),
                                    size: None,
                                }),
                                constraints: vec![],
                            },
                        ],
                        constraints: vec![
                            NamedTableConstraint {
                                name: None,
                                constraint: TableConstraint::Unique {
                                    columns: vec![
                                        SortedColumn {
                                            expr: Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                                            order: None,
                                            nulls: None,
                                        },
                                    ],
                                    conflict_clause:  Some(ResolveType::Rollback)
                                }
                            },
                        ],
                        options: TableOptions::WITHOUT_ROWID,
                    },
                })],
            ),
            (
                b"CREATE TEMP TABLE IF NOT EXISTS foo (bar, baz INTEGER, CHECK (1))".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: true,
                    if_not_exists: true,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::ColumnsAndConstraints {
                        columns: vec![
                            ColumnDefinition {
                                col_name: Name::Ident("bar".to_owned()),
                                col_type: None,
                                constraints: vec![],
                            },
                            ColumnDefinition {
                                col_name: Name::Ident("baz".to_owned()),
                                col_type: Some(Type {
                                    name: "INTEGER".to_owned(),
                                    size: None,
                                }),
                                constraints: vec![],
                            },
                        ],
                        constraints: vec![
                            NamedTableConstraint {
                                name: None,
                                constraint: TableConstraint::Check(Box::new(
                                    Expr::Literal(Literal::Numeric("1".to_owned()))
                                )),
                            },
                        ],
                        options: TableOptions::NONE,
                    },
                })],
            ),
            (
                b"CREATE TEMP TABLE IF NOT EXISTS foo (bar, baz INTEGER, FOREIGN KEY (bar) REFERENCES foo_2(bar_2), CHECK (1))".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: true,
                    if_not_exists: true,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::ColumnsAndConstraints {
                        columns: vec![
                            ColumnDefinition {
                                col_name: Name::Ident("bar".to_owned()),
                                col_type: None,
                                constraints: vec![],
                            },
                            ColumnDefinition {
                                col_name: Name::Ident("baz".to_owned()),
                                col_type: Some(Type {
                                    name: "INTEGER".to_owned(),
                                    size: None,
                                }),
                                constraints: vec![],
                            },
                        ],
                        constraints: vec![
                            NamedTableConstraint {
                                name: None,
                                constraint: TableConstraint::ForeignKey {
                                    columns: vec![
                                        IndexedColumn {
                                            col_name: Name::Ident("bar".to_owned()),
                                            collation_name: None,
                                            order: None,
                                        },
                                    ],
                                    clause: ForeignKeyClause {
                                        tbl_name: Name::Ident("foo_2".to_owned()),
                                        columns: vec![
                                            IndexedColumn {
                                                col_name: Name::Ident("bar_2".to_owned()),
                                                collation_name: None,
                                                order: None,
                                            },
                                        ],
                                        args: vec![],
                                    },
                                    deref_clause: None,
                                },
                            },
                            NamedTableConstraint {
                                name: None,
                                constraint: TableConstraint::Check(Box::new(
                                    Expr::Literal(Literal::Numeric("1".to_owned()))
                                )),
                            },
                        ],
                        options: TableOptions::NONE,
                    },
                })],
            ),
            // parse create trigger
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN SELECT 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Select(Select {
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
                        })
                    ],
                })],
            ),
            (
                b"CREATE TEMP TRIGGER IF NOT EXISTS foo AFTER UPDATE ON bar FOR EACH ROW WHEN 1 BEGIN SELECT 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: true,
                    if_not_exists: true,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: Some(TriggerTime::After),
                    event: TriggerEvent::Update,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: true,
                    when_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                    commands: vec![
                        TriggerCmd::Select(Select {
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
                        })
                    ],
                })],
            ),
            (
                b"CREATE TEMP TRIGGER IF NOT EXISTS foo BEFORE DELETE ON bar FOR EACH ROW WHEN 1 BEGIN SELECT 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: true,
                    if_not_exists: true,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: Some(TriggerTime::Before),
                    event: TriggerEvent::Delete,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: true,
                    when_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                    commands: vec![
                        TriggerCmd::Select(Select {
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
                        })
                    ],
                })],
            ),
            (
                b"CREATE TEMP TRIGGER IF NOT EXISTS foo INSTEAD OF UPDATE OF baz, bar ON bar FOR EACH ROW WHEN 1 BEGIN SELECT 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: true,
                    if_not_exists: true,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: Some(TriggerTime::InsteadOf),
                    event: TriggerEvent::UpdateOf(vec![
                        Name::Ident("baz".to_owned()),
                        Name::Ident("bar".to_owned()),
                    ]),
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: true,
                    when_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                    commands: vec![
                        TriggerCmd::Select(Select {
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
                        })
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2); END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Insert {
                            or_conflict: None,
                            tbl_name: Name::Ident("foo".to_owned()),
                            col_names: vec![],
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Values(vec![
                                        vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        ],
                                    ]),
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            },
                            upsert: None,
                            returning: vec![],
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN INSERT OR ROLLBACK INTO foo(bar, baz) VALUES (1, 2); END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Insert {
                            or_conflict: Some(ResolveType::Rollback),
                            tbl_name: Name::Ident("foo".to_owned()),
                            col_names: vec![
                                Name::Ident("bar".to_owned()),
                                Name::Ident("baz".to_owned()),
                            ],
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Values(vec![
                                        vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        ],
                                    ]),
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            },
                            upsert: None,
                            returning: vec![],
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2) RETURNING bar, baz; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Insert {
                            or_conflict: None,
                            tbl_name: Name::Ident("foo".to_owned()),
                            col_names: vec![],
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Values(vec![
                                        vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        ],
                                    ]),
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            },
                            upsert: None,
                            returning: vec![
                                ResultColumn::Expr(
                                    Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                                    None,
                                ),
                                ResultColumn::Expr(
                                    Box::new(Expr::Id(Name::Ident("baz".to_owned()))),
                                    None,
                                ),
                            ],
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2) ON CONFLICT (bar, baz) WHERE 1 DO NOTHING RETURNING bar, baz; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Insert {
                            or_conflict: None,
                            tbl_name: Name::Ident("foo".to_owned()),
                            col_names: vec![],
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Values(vec![
                                        vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        ],
                                    ]),
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            },
                            upsert: Some(Box::new(Upsert {
                                index: Some(UpsertIndex {
                                    targets: vec![
                                        SortedColumn {
                                            expr: Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                                            order: None,
                                            nulls: None
                                        },
                                        SortedColumn {
                                            expr: Box::new(Expr::Id(Name::Ident("baz".to_owned()))),
                                            order: None,
                                            nulls: None
                                        },
                                    ],
                                    where_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                                }),
                                do_clause: UpsertDo::Nothing,
                                next: None
                            })),
                            returning: vec![
                                ResultColumn::Expr(
                                    Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                                    None,
                                ),
                                ResultColumn::Expr(
                                    Box::new(Expr::Id(Name::Ident("baz".to_owned()))),
                                    None,
                                ),
                            ],
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2) ON CONFLICT DO UPDATE SET (bar, baz) = 1 WHERE 1 RETURNING bar, baz; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Insert {
                            or_conflict: None,
                            tbl_name: Name::Ident("foo".to_owned()),
                            col_names: vec![],
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Values(vec![
                                        vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        ],
                                    ]),
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            },
                            upsert: Some(Box::new(Upsert {
                                index: None,
                                do_clause: UpsertDo::Set {
                                    sets: vec![
                                        Set {
                                            col_names: vec![
                                                Name::Ident("bar".to_owned()),
                                                Name::Ident("baz".to_owned()),
                                            ],
                                            expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                        }
                                    ],
                                    where_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                                },
                                next: None
                            })),
                            returning: vec![
                                ResultColumn::Expr(
                                    Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                                    None,
                                ),
                                ResultColumn::Expr(
                                    Box::new(Expr::Id(Name::Ident("baz".to_owned()))),
                                    None,
                                ),
                            ],
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN INSERT INTO foo VALUES (1, 2) ON CONFLICT (bar, baz) DO NOTHING ON CONFLICT DO NOTHING; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Insert {
                            or_conflict: None,
                            tbl_name: Name::Ident("foo".to_owned()),
                            col_names: vec![],
                            select: Select {
                                with: None,
                                body: SelectBody {
                                    select: OneSelect::Values(vec![
                                        vec![
                                            Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                            Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                        ],
                                    ]),
                                    compounds: vec![],
                                },
                                order_by: vec![],
                                limit: None,
                            },
                            upsert: Some(Box::new(Upsert {
                                index: Some(UpsertIndex {
                                    targets: vec![
                                        SortedColumn {
                                            expr: Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                                            order: None,
                                            nulls: None
                                        },
                                        SortedColumn {
                                            expr: Box::new(Expr::Id(Name::Ident("baz".to_owned()))),
                                            order: None,
                                            nulls: None
                                        },
                                    ],
                                    where_clause: None,
                                }),
                                do_clause: UpsertDo::Nothing,
                                next: Some(Box::new(Upsert {
                                    index: None,
                                    do_clause: UpsertDo::Nothing,
                                    next: None,
                                })),
                            })),
                            returning: vec![],
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN UPDATE foo SET bar = 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Update {
                            or_conflict: None,
                            tbl_name: Name::Ident("foo".to_owned()),
                            sets: vec![
                                Set {
                                    col_names: vec![Name::Ident("bar".to_owned())],
                                    expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                },
                            ],
                            from: None,
                            where_clause: None,
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN DELETE FROM foo; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Delete {
                            tbl_name: Name::Ident("foo".to_owned()),
                            where_clause: None,
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN DELETE FROM foo WHERE 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Delete {
                            tbl_name: Name::Ident("foo".to_owned()),
                            where_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                        },
                    ],
                })],
            ),
            (
                b"CREATE TRIGGER foo INSERT ON bar BEGIN SELECT 1; SELECT 1; END".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTrigger {
                    temporary: false,
                    if_not_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    time: None,
                    event: TriggerEvent::Insert,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("bar".to_owned()),
                        alias: None,
                    },
                    for_each_row: false,
                    when_clause: None,
                    commands: vec![
                        TriggerCmd::Select(Select {
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
                        }),
                        TriggerCmd::Select(Select {
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
                        })
                    ],
                })],
            ),
            // parse create view
            (
                b"CREATE VIEW foo(bar) AS SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateView {
                    temporary: false,
                    if_not_exists: false,
                    view_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    columns: vec![
                        IndexedColumn {
                            col_name: Name::Ident("bar".to_owned()),
                            collation_name: None,
                            order: None,
                        }
                    ],
                    select: Select {
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
                    },
                })],
            ),
            (
                b"CREATE TEMP VIEW IF NOT EXISTS foo(bar) AS SELECT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateView {
                    temporary: true,
                    if_not_exists: true,
                    view_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    columns: vec![
                        IndexedColumn {
                            col_name: Name::Ident("bar".to_owned()),
                            collation_name: None,
                            order: None,
                        }
                    ],
                    select: Select {
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
                    },
                })],
            ),
            // parse CREATE VIRTUAL TABLE
            (
                b"CREATE VIRTUAL TABLE foo USING bar".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateVirtualTable(CreateVirtualTable {
                    if_not_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    module_name: Name::Ident("bar".to_owned()),
                    args: vec![],
                }))],
            ),
            (
                b"CREATE VIRTUAL TABLE foo USING bar()".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateVirtualTable(CreateVirtualTable{
                    if_not_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    module_name: Name::Ident("bar".to_owned()),
                    args: vec![],
                }))],
            ),
            (
                b"CREATE VIRTUAL TABLE IF NOT EXISTS foo USING bar(1, 2, ('hello', (3.333), 'world', (1, 2)))".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateVirtualTable(CreateVirtualTable{
                    if_not_exists: true,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    module_name: Name::Ident("bar".to_owned()),
                    args: vec![
                        "1".to_owned(),
                        "2".to_owned(),
                        "('hello', (3.333), 'world', (1, 2))".to_owned(),
                    ],
                }))],
            ),
            (
                b"CREATE VIRTUAL TABLE ft USING fts5(x, tokenize = '''porter'' ''ascii''')".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateVirtualTable(CreateVirtualTable {
                    if_not_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("ft".to_owned()),
                        alias: None,
                    },
                    module_name: Name::Ident("fts5".to_owned()),
                    args: vec![
                        "x".to_owned(),
                        "tokenize = '''porter'' ''ascii'''".to_owned(),
                    ],
                }))],
            ),
            // parse delete
            (
                b"DELETE FROM foo".as_slice(),
                vec![Cmd::Stmt(Stmt::Delete {
                    with: None,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None
                    },
                    indexed: None,
                    where_clause: None,
                    returning: vec![],
                    order_by: vec![],
                    limit: None,
                })],
            ),
            (
                b"WITH test AS (SELECT 1) DELETE FROM foo NOT INDEXED WHERE 1 RETURNING bar ORDER BY bar LIMIT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Delete {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::Ident("test".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
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
                                },
                            }
                        ],
                    }),
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None
                    },
                    indexed: Some(Indexed::NotIndexed),
                    where_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                    returning: vec![
                        ResultColumn::Expr(
                            Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                            None,
                        ),
                    ],
                    order_by: vec![
                        SortedColumn {
                            expr: Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                            order: None,
                            nulls: None,
                        }
                    ],
                    limit: Some(Limit {
                        expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        offset: None,
                    }),
                })],
            ),
            // parse drop index
            (
                b"DROP INDEX foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropIndex {
                    if_exists: false,
                    idx_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            (
                b"DROP INDEX IF EXISTS foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropIndex {
                    if_exists: true,
                    idx_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            // parse drop table
            (
                b"DROP TABLE foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropTable {
                    if_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            (
                b"DROP TABLE IF EXISTS foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropTable {
                    if_exists: true,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            // parse drop trigger
            (
                b"DROP TRIGGER foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropTrigger {
                    if_exists: false,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            (
                b"DROP TRIGGER IF EXISTS foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropTrigger {
                    if_exists: true,
                    trigger_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            // parse drop view
            (
                b"DROP VIEW foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropView {
                    if_exists: false,
                    view_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            (
                b"DROP VIEW IF EXISTS foo".as_slice(),
                vec![Cmd::Stmt(Stmt::DropView {
                    if_exists: true,
                    view_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                })],
            ),
            // parse insert
            (
                b"INSERT INTO foo VALUES (1, 2)".as_slice(),
                vec![Cmd::Stmt(Stmt::Insert {
                    with: None,
                    or_conflict: None,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    columns: vec![],
                    body: InsertBody::Select(Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Values(vec![
                                vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                            ]),
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    }, None),
                    returning: vec![],
                })],
            ),
            (
                b"REPLACE INTO foo VALUES (1, 2)".as_slice(),
                vec![Cmd::Stmt(Stmt::Insert {
                    with: None,
                    or_conflict: Some(ResolveType::Replace),
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    columns: vec![],
                    body: InsertBody::Select(Select {
                        with: None,
                        body: SelectBody {
                            select: OneSelect::Values(vec![
                                vec![
                                    Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                                    Box::new(Expr::Literal(Literal::Numeric("2".to_owned()))),
                                ],
                            ]),
                            compounds: vec![],
                        },
                        order_by: vec![],
                        limit: None,
                    }, None),
                    returning: vec![],
                })],
            ),
            (
                b"WITH test AS (SELECT 1) INSERT INTO foo DEFAULT VALUES RETURNING bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Insert {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::Ident("test".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
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
                                },
                            }
                        ],
                    }),
                    or_conflict: None,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    columns: vec![],
                    body: InsertBody::DefaultValues,
                    returning: vec![
                        ResultColumn::Expr(
                            Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                            None,
                        ),
                    ],
                })],
            ),
            (
                b"WITH test AS (SELECT 1) REPLACE INTO foo DEFAULT VALUES RETURNING bar".as_slice(),
                vec![Cmd::Stmt(Stmt::Insert {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::Ident("test".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
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
                                },
                            }
                        ],
                    }),
                    or_conflict: Some(ResolveType::Replace),
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    columns: vec![],
                    body: InsertBody::DefaultValues,
                    returning: vec![
                        ResultColumn::Expr(
                            Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                            None,
                        ),
                    ],
                })],
            ),
            // parse update
            (
                b"UPDATE foo SET bar = 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Update(Update {
                    with: None,
                    or_conflict: None,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    indexed: None,
                    sets: vec![
                        Set {
                            col_names: vec![
                                Name::Ident("bar".to_owned()),
                            ],
                            expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        }
                    ],
                    from: None,
                    where_clause: None,
                    returning: vec![],
                    order_by: vec![],
                    limit: None,
                }))],
            ),
            (
                b"WITH test AS (SELECT 1) UPDATE OR REPLACE foo NOT INDEXED SET bar = 1 FROM foo_2 WHERE 1 RETURNING bar ORDER By bar LIMIT 1".as_slice(),
                vec![Cmd::Stmt(Stmt::Update(Update {
                    with: Some(With {
                        recursive: false,
                        ctes: vec![
                            CommonTableExpr {
                                tbl_name: Name::Ident("test".to_owned()),
                                columns: vec![],
                                materialized: Materialized::Any,
                                select: Select {
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
                                },
                            }
                        ],
                    }),
                    or_conflict: Some(ResolveType::Replace),
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None,
                    },
                    indexed: Some(Indexed::NotIndexed),
                    sets: vec![
                        Set {
                            col_names: vec![
                                Name::Ident("bar".to_owned()),
                            ],
                            expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        }
                    ],
                    from: Some(FromClause {
                        select: Box::new(SelectTable::Table(
                            QualifiedName {
                                db_name: None,
                                name: Name::Ident("foo_2".to_owned()),
                                alias: None,
                            },
                            None,
                            None,
                        )),
                        joins: vec![]
                    }),
                    where_clause: Some(Box::new(Expr::Literal(Literal::Numeric("1".to_owned())))),
                    returning: vec![
                        ResultColumn::Expr(
                            Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                            None,
                        ),
                    ],
                    order_by: vec![
                        SortedColumn {
                            expr: Box::new(Expr::Id(Name::Ident("bar".to_owned()))),
                            order: None,
                            nulls: None,
                        }
                    ],
                    limit: Some(Limit {
                        expr: Box::new(Expr::Literal(Literal::Numeric("1".to_owned()))),
                        offset: None,
                    }),
                }))],
            ),
            // parse reindex
            (
                b"REINDEX".as_slice(),
                vec![Cmd::Stmt(Stmt::Reindex {
                    name: None,
                })],
            ),
            (
                b"REINDEX foo".as_slice(),
                vec![Cmd::Stmt(Stmt::Reindex {
                    name: Some(QualifiedName {
                        db_name: None,
                        name: Name::Ident("foo".to_owned()),
                        alias: None
                    }),
                })],
            ),
            // issue 2875
            (
                b"CREATE TABLE \"settings\" (\"enabled\" INTEGER DEFAULT CURRENT_TIMESTAMP NOT NULL)".as_slice(),
                vec![Cmd::Stmt(Stmt::CreateTable {
                    temporary: false,
                    if_not_exists: false,
                    tbl_name: QualifiedName {
                        db_name: None,
                        name: Name::Quoted("\"settings\"".to_owned()),
                        alias: None,
                    },
                    body: CreateTableBody::ColumnsAndConstraints{
                        columns: vec![
                            ColumnDefinition {
                                col_name: Name::Quoted("\"enabled\"".to_owned()),
                                col_type: Some(Type {
                                    name: "INTEGER".to_owned(),
                                    size: None,
                                }),
                                constraints: vec![
                                    NamedColumnConstraint {
                                        name: None,
                                        constraint: ColumnConstraint::Default(Box::new(Expr::Literal(Literal::CurrentTimestamp))),
                                    },
                                    NamedColumnConstraint {
                                        name: None,
                                        constraint: ColumnConstraint::NotNull { nullable: false, conflict_clause: None }
                                    },
                                ],
                            }
                        ],
                        constraints: vec![],
                        options: TableOptions::NONE,
                    },
                })],
            )
        ];

        for (input, expected) in test_cases {
            let input_str = from_bytes(input);
            let parser = Parser::new(input);
            let mut results = Vec::new();
            for cmd in parser {
                results.push(cmd.unwrap());
            }

            assert_eq!(results, expected, "Input: {input_str:?}");

            // to_string tests
            for (i, r) in results.iter().enumerate() {
                let rstring = r.to_string();
                // put new string into parser again
                let result = Parser::new(rstring.as_bytes()).next().unwrap().unwrap();
                let expected = &expected[i];
                assert_eq!(result, expected.clone(), "Input: {rstring:?}");
            }
        }
    }
}
