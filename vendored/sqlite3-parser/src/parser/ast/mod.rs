//! Abstract Syntax Tree

pub mod check;
pub mod fmt;

use std::num::ParseIntError;
use std::ops::Deref;
use std::str::{self, Bytes, FromStr};

use strum_macros::{EnumIter, EnumString};

use fmt::{ToTokens, TokenStream};
use indexmap::{IndexMap, IndexSet};

use crate::custom_err;
use crate::dialect::TokenType::{self, *};
use crate::dialect::{from_token, is_identifier, Token};
use crate::parser::{parse::YYCODETYPE, ParserError};

/// `?` or `$` Prepared statement arg placeholder(s)
#[derive(Default)]
pub struct ParameterInfo {
    /// Number of SQL parameters in a prepared statement, like `sqlite3_bind_parameter_count`
    pub count: u32,
    /// Parameter name(s) if any
    pub names: IndexSet<String>,
}

// https://sqlite.org/lang_expr.html#parameters
impl TokenStream for ParameterInfo {
    type Error = ParseIntError;

    fn append(&mut self, ty: TokenType, value: Option<&str>) -> Result<(), Self::Error> {
        if ty == TK_VARIABLE {
            if let Some(variable) = value {
                if variable == "?" {
                    self.count = self.count.saturating_add(1);
                } else if variable.as_bytes()[0] == b'?' {
                    let n = u32::from_str(&variable[1..])?;
                    if n > self.count {
                        self.count = n;
                    }
                } else if self.names.insert(variable.to_owned()) {
                    self.count = self.count.saturating_add(1);
                }
            }
        }
        Ok(())
    }
}

/// Statement or Explain statement
// https://sqlite.org/syntax/sql-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cmd {
    /// `EXPLAIN` statement
    Explain(Stmt),
    /// `EXPLAIN QUERY PLAN` statement
    ExplainQueryPlan(Stmt),
    /// statement
    Stmt(Stmt),
}

pub(crate) enum ExplainKind {
    Explain,
    QueryPlan,
}

/// SQL statement
// https://sqlite.org/syntax/sql-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Stmt {
    /// `ALTER TABLE`: table name, body
    AlterTable(Box<(QualifiedName, AlterTableBody)>),
    /// `ANALYSE`: object name
    Analyze(Option<QualifiedName>),
    /// `ATTACH DATABASE`
    Attach {
        /// filename
        // TODO distinction between ATTACH and ATTACH DATABASE
        expr: Box<Expr>,
        /// schema name
        db_name: Box<Expr>,
        /// password
        key: Option<Box<Expr>>,
    },
    /// `BEGIN`: tx type, tx name
    Begin(Option<TransactionType>, Option<Name>),
    /// `COMMIT`/`END`: tx name
    Commit(Option<Name>), // TODO distinction between COMMIT and END
    /// `CREATE INDEX`
    CreateIndex {
        /// `UNIQUE`
        unique: bool,
        /// `IF NOT EXISTS`
        if_not_exists: bool,
        /// index name
        idx_name: Box<QualifiedName>,
        /// table name
        tbl_name: Name,
        /// indexed columns or expressions
        columns: Vec<SortedColumn>,
        /// partial index
        where_clause: Option<Box<Expr>>,
    },
    /// `CREATE TABLE`
    CreateTable {
        /// `TEMPORARY`
        temporary: bool, // TODO distinction between TEMP and TEMPORARY
        /// `IF NOT EXISTS`
        if_not_exists: bool,
        /// table name
        tbl_name: QualifiedName,
        /// table body
        body: Box<CreateTableBody>,
    },
    /// `CREATE TRIGGER`
    CreateTrigger(Box<CreateTrigger>),
    /// `CREATE VIEW`
    CreateView {
        /// `TEMPORARY`
        temporary: bool,
        /// `IF NOT EXISTS`
        if_not_exists: bool,
        /// view name
        view_name: QualifiedName,
        /// columns
        columns: Option<Vec<IndexedColumn>>,
        /// query
        select: Box<Select>,
    },
    /// `CREATE VIRTUAL TABLE`
    CreateVirtualTable(Box<CreateVirtualTable>),
    /// `DELETE`
    Delete(Box<Delete>),
    /// `DETACH DATABASE`: db name
    Detach(Box<Expr>), // TODO distinction between DETACH and DETACH DATABASE
    /// `DROP INDEX`
    DropIndex {
        /// `IF EXISTS`
        if_exists: bool,
        /// index name
        idx_name: QualifiedName,
    },
    /// `DROP TABLE`
    DropTable {
        /// `IF EXISTS`
        if_exists: bool,
        /// table name
        tbl_name: QualifiedName,
    },
    /// `DROP TRIGGER`
    DropTrigger {
        /// `IF EXISTS`
        if_exists: bool,
        /// trigger name
        trigger_name: QualifiedName,
    },
    /// `DROP VIEW`
    DropView {
        /// `IF EXISTS`
        if_exists: bool,
        /// view name
        view_name: QualifiedName,
    },
    /// `INSERT`
    Insert(Box<Insert>),
    /// `PRAGMA`: pragma name, body
    Pragma(Box<QualifiedName>, Option<Box<PragmaBody>>),
    /// `REINDEX`
    Reindex {
        /// collation or index or table name
        obj_name: Option<QualifiedName>,
    },
    /// `RELEASE`: savepoint name
    Release(Name), // TODO distinction between RELEASE and RELEASE SAVEPOINT
    /// `ROLLBACK`
    Rollback {
        /// transaction name
        tx_name: Option<Name>,
        /// savepoint name
        savepoint_name: Option<Name>, // TODO distinction between TO and TO SAVEPOINT
    },
    /// `SAVEPOINT`: savepoint name
    Savepoint(Name),
    /// `SELECT`
    Select(Box<Select>),
    /// `UPDATE`
    Update(Box<Update>),
    /// `VACUUM`: database name, into expr
    Vacuum(Option<Name>, Option<Box<Expr>>),
}

/// `CREATE VIRTUAL TABLE`
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CreateVirtualTable {
    /// `IF NOT EXISTS`
    pub if_not_exists: bool,
    /// table name
    pub tbl_name: QualifiedName,
    /// module name
    pub module_name: Name,
    /// args
    pub args: Option<Vec<String>>, // TODO smol str
}

/// `CREATE TRIGGER
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CreateTrigger {
    /// `TEMPORARY`
    pub temporary: bool,
    /// `IF NOT EXISTS`
    pub if_not_exists: bool,
    /// trigger name
    pub trigger_name: QualifiedName,
    /// `BEFORE`/`AFTER`/`INSTEAD OF`
    pub time: Option<TriggerTime>,
    /// `DELETE`/`INSERT`/`UPDATE`
    pub event: TriggerEvent,
    /// table name
    pub tbl_name: QualifiedName,
    /// `FOR EACH ROW`
    pub for_each_row: bool,
    /// `WHEN`
    pub when_clause: Option<Expr>,
    /// statements
    pub commands: Vec<TriggerCmd>,
}

/// `INSERT`
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Insert {
    /// CTE
    pub with: Option<With>,
    /// `OR`
    pub or_conflict: Option<ResolveType>, // TODO distinction between REPLACE and INSERT OR REPLACE
    /// table name
    pub tbl_name: QualifiedName,
    /// `COLUMNS`
    pub columns: Option<DistinctNames>,
    /// `VALUES` or `SELECT`
    pub body: InsertBody,
    /// `RETURNING`
    pub returning: Option<Vec<ResultColumn>>,
}

/// `UPDATE` clause
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Update {
    /// CTE
    pub with: Option<With>,
    /// `OR`
    pub or_conflict: Option<ResolveType>,
    /// table name
    pub tbl_name: QualifiedName,
    /// `INDEXED`
    pub indexed: Option<Indexed>,
    /// `SET` assignments
    pub sets: Vec<Set>,
    /// `FROM`
    pub from: Option<FromClause>,
    /// `WHERE` clause
    pub where_clause: Option<Box<Expr>>,
    /// `RETURNING`
    pub returning: Option<Vec<ResultColumn>>,
    /// `ORDER BY`
    pub order_by: Option<Vec<SortedColumn>>,
    /// `LIMIT`
    pub limit: Option<Box<Limit>>,
}

/// `DELETE`
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Delete {
    /// CTE
    pub with: Option<With>,
    /// `FROM` table name
    pub tbl_name: QualifiedName,
    /// `INDEXED`
    pub indexed: Option<Indexed>,
    /// `WHERE` clause
    pub where_clause: Option<Box<Expr>>,
    /// `RETURNING`
    pub returning: Option<Vec<ResultColumn>>,
    /// `ORDER BY`
    pub order_by: Option<Vec<SortedColumn>>,
    /// `LIMIT`
    pub limit: Option<Box<Limit>>,
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
/// Internal ID of a table reference.
///
/// Used by [Expr::Column] and [Expr::RowId] to refer to a table.
/// E.g. in 'SELECT * FROM t UNION ALL SELECT * FROM t', there are two table references,
/// so there are two TableInternalIds.
///
/// FIXME: rename this to TableReferenceId.
pub struct TableInternalId(usize);

impl Default for TableInternalId {
    fn default() -> Self {
        Self(1)
    }
}

impl From<usize> for TableInternalId {
    fn from(value: usize) -> Self {
        Self(value)
    }
}

impl std::ops::AddAssign<usize> for TableInternalId {
    fn add_assign(&mut self, rhs: usize) {
        self.0 += rhs;
    }
}

impl From<TableInternalId> for usize {
    fn from(value: TableInternalId) -> Self {
        value.0
    }
}

impl std::fmt::Display for TableInternalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "t{}", self.0)
    }
}

/// SQL expression
// https://sqlite.org/syntax/expr.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Expr {
    /// `BETWEEN`
    Between {
        /// expression
        lhs: Box<Expr>,
        /// `NOT`
        not: bool,
        /// start
        start: Box<Expr>,
        /// end
        end: Box<Expr>,
    },
    /// binary expression
    Binary(Box<Expr>, Operator, Box<Expr>),
    /// `CASE` expression
    Case {
        /// operand
        base: Option<Box<Expr>>,
        /// `WHEN` condition `THEN` result
        when_then_pairs: Vec<(Expr, Expr)>,
        /// `ELSE` result
        else_expr: Option<Box<Expr>>,
    },
    /// CAST expression
    Cast {
        /// expression
        expr: Box<Expr>,
        /// `AS` type name
        type_name: Option<Type>,
    },
    /// `COLLATE`: expression
    Collate(Box<Expr>, String),
    /// schema-name.table-name.column-name
    DoublyQualified(Name, Name, Name),
    /// `EXISTS` subquery
    Exists(Box<Select>),
    /// call to a built-in function
    FunctionCall {
        /// function name
        name: Id,
        /// `DISTINCT`
        distinctness: Option<Distinctness>,
        /// arguments
        args: Option<Vec<Expr>>,
        /// `ORDER BY`
        order_by: Option<Vec<SortedColumn>>,
        /// `FILTER`
        filter_over: Option<FunctionTail>,
    },
    /// Function call expression with '*' as arg
    FunctionCallStar {
        /// function name
        name: Id,
        /// `FILTER`
        filter_over: Option<FunctionTail>,
    },
    /// Identifier
    Id(Id),
    /// Column
    Column {
        /// the x in `x.y.z`. index of the db in catalog.
        database: Option<usize>,
        /// the y in `x.y.z`. index of the table in catalog.
        table: TableInternalId,
        /// the z in `x.y.z`. index of the column in the table.
        column: usize,
        /// is the column a rowid alias
        is_rowid_alias: bool,
    },
    /// `ROWID`
    RowId {
        /// the x in `x.y.z`. index of the db in catalog.
        database: Option<usize>,
        /// the y in `x.y.z`. index of the table in catalog.
        table: TableInternalId,
    },
    /// `IN`
    InList {
        /// expression
        lhs: Box<Expr>,
        /// `NOT`
        not: bool,
        /// values
        rhs: Option<Vec<Expr>>,
    },
    /// `IN` subselect
    InSelect {
        /// expression
        lhs: Box<Expr>,
        /// `NOT`
        not: bool,
        /// subquery
        rhs: Box<Select>,
    },
    /// `IN` table name / function
    InTable {
        /// expression
        lhs: Box<Expr>,
        /// `NOT`
        not: bool,
        /// table name
        rhs: QualifiedName,
        /// table function arguments
        args: Option<Vec<Expr>>,
    },
    /// `IS NULL`
    IsNull(Box<Expr>),
    /// `LIKE`
    Like {
        /// expression
        lhs: Box<Expr>,
        /// `NOT`
        not: bool,
        /// operator
        op: LikeOperator,
        /// pattern
        rhs: Box<Expr>,
        /// `ESCAPE` char
        escape: Option<Box<Expr>>,
    },
    /// Literal expression
    Literal(Literal),
    /// Name
    Name(Name),
    /// `NOT NULL` or `NOTNULL`
    NotNull(Box<Expr>),
    /// Parenthesized subexpression
    Parenthesized(Vec<Expr>),
    /// Qualified name
    Qualified(Name, Name),
    /// `RAISE` function call
    Raise(ResolveType, Option<Box<Expr>>),
    /// Subquery expression
    Subquery(Box<Select>),
    /// Unary expression
    Unary(UnaryOperator, Box<Expr>),
    /// Parameters
    Variable(String),
}

impl Expr {
    /// Constructor
    pub fn parenthesized(x: Self) -> Self {
        Self::Parenthesized(vec![x])
    }
    /// Constructor
    pub fn id(xt: YYCODETYPE, x: Token) -> Self {
        Self::Id(Id::from_token(xt, x))
    }
    /// Constructor
    pub fn collate(x: Self, ct: YYCODETYPE, c: Token) -> Self {
        Self::Collate(Box::new(x), from_token(ct, c))
    }
    /// Constructor
    pub fn cast(x: Self, type_name: Option<Type>) -> Self {
        Self::Cast {
            expr: Box::new(x),
            type_name,
        }
    }
    /// Constructor
    pub fn binary(left: Self, op: YYCODETYPE, right: Self) -> Self {
        Self::Binary(Box::new(left), Operator::from(op), Box::new(right))
    }
    /// Constructor
    pub fn ptr(left: Self, op: Token, right: Self) -> Self {
        let mut ptr = Operator::ArrowRight;
        if op.1 == b"->>" {
            ptr = Operator::ArrowRightShift;
        }
        Self::Binary(Box::new(left), ptr, Box::new(right))
    }
    /// Constructor
    pub fn like(lhs: Self, not: bool, op: LikeOperator, rhs: Self, escape: Option<Self>) -> Self {
        Self::Like {
            lhs: Box::new(lhs),
            not,
            op,
            rhs: Box::new(rhs),
            escape: escape.map(Box::new),
        }
    }
    /// Constructor
    pub fn not_null(x: Self, op: YYCODETYPE) -> Self {
        if op == TK_ISNULL as YYCODETYPE {
            Self::IsNull(Box::new(x))
        } else if op == TK_NOTNULL as YYCODETYPE {
            Self::NotNull(Box::new(x))
        } else {
            unreachable!()
        }
    }
    /// Constructor
    pub fn unary(op: UnaryOperator, x: Self) -> Self {
        Self::Unary(op, Box::new(x))
    }
    /// Constructor
    pub fn between(lhs: Self, not: bool, start: Self, end: Self) -> Self {
        Self::Between {
            lhs: Box::new(lhs),
            not,
            start: Box::new(start),
            end: Box::new(end),
        }
    }
    /// Constructor
    pub fn in_list(lhs: Self, not: bool, rhs: Option<Vec<Self>>) -> Self {
        Self::InList {
            lhs: Box::new(lhs),
            not,
            rhs,
        }
    }
    /// Constructor
    pub fn in_select(lhs: Self, not: bool, rhs: Select) -> Self {
        Self::InSelect {
            lhs: Box::new(lhs),
            not,
            rhs: Box::new(rhs),
        }
    }
    /// Constructor
    pub fn in_table(lhs: Self, not: bool, rhs: QualifiedName, args: Option<Vec<Self>>) -> Self {
        Self::InTable {
            lhs: Box::new(lhs),
            not,
            rhs,
            args,
        }
    }
    /// Constructor
    pub fn sub_query(query: Select) -> Self {
        Self::Subquery(Box::new(query))
    }
}

/// SQL literal
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Literal {
    /// Number
    Numeric(String),
    /// String
    // TODO Check that string is already quoted and correctly escaped
    String(String),
    /// BLOB
    // TODO Check that string is valid (only hexa)
    Blob(String),
    /// Keyword
    Keyword(String),
    /// `NULL`
    Null,
    /// `CURRENT_DATE`
    CurrentDate,
    /// `CURRENT_TIME`
    CurrentTime,
    /// `CURRENT_TIMESTAMP`
    CurrentTimestamp,
}

impl Literal {
    /// Constructor
    pub fn from_ctime_kw(token: Token) -> Self {
        if b"CURRENT_DATE".eq_ignore_ascii_case(token.1) {
            Self::CurrentDate
        } else if b"CURRENT_TIME".eq_ignore_ascii_case(token.1) {
            Self::CurrentTime
        } else if b"CURRENT_TIMESTAMP".eq_ignore_ascii_case(token.1) {
            Self::CurrentTimestamp
        } else {
            unreachable!()
        }
    }
}

/// Textual comparison operator in an expression
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum LikeOperator {
    /// `GLOB`
    Glob,
    /// `LIKE`
    Like,
    /// `MATCH`
    Match,
    /// `REGEXP`
    Regexp,
}

impl LikeOperator {
    /// Constructor
    pub fn from_token(token_type: YYCODETYPE, token: Token) -> Self {
        if token_type == TK_MATCH as YYCODETYPE {
            return Self::Match;
        } else if token_type == TK_LIKE_KW as YYCODETYPE {
            let token = token.1;
            if b"LIKE".eq_ignore_ascii_case(token) {
                return Self::Like;
            } else if b"GLOB".eq_ignore_ascii_case(token) {
                return Self::Glob;
            } else if b"REGEXP".eq_ignore_ascii_case(token) {
                return Self::Regexp;
            }
        }
        unreachable!()
    }
}

/// SQL operators
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Operator {
    /// `+`
    Add,
    /// `AND`
    And,
    /// `->`
    ArrowRight,
    /// `->>`
    ArrowRightShift,
    /// `&`
    BitwiseAnd,
    /// `|`
    BitwiseOr,
    /// `~`
    BitwiseNot,
    /// String concatenation (`||`)
    Concat,
    /// `=` or `==`
    Equals,
    /// `/`
    Divide,
    /// `>`
    Greater,
    /// `>=`
    GreaterEquals,
    /// `IS`
    Is,
    /// `IS NOT`
    IsNot,
    /// `<<`
    LeftShift,
    /// `<`
    Less,
    /// `<=`
    LessEquals,
    /// `%`
    Modulus,
    /// `*`
    Multiply,
    /// `!=` or `<>`
    NotEquals,
    /// `OR`
    Or,
    /// `>>`
    RightShift,
    /// `-`
    Subtract,
}

impl From<YYCODETYPE> for Operator {
    fn from(token_type: YYCODETYPE) -> Self {
        match token_type {
            x if x == TK_AND as YYCODETYPE => Self::And,
            x if x == TK_OR as YYCODETYPE => Self::Or,
            x if x == TK_LT as YYCODETYPE => Self::Less,
            x if x == TK_GT as YYCODETYPE => Self::Greater,
            x if x == TK_GE as YYCODETYPE => Self::GreaterEquals,
            x if x == TK_LE as YYCODETYPE => Self::LessEquals,
            x if x == TK_EQ as YYCODETYPE => Self::Equals,
            x if x == TK_NE as YYCODETYPE => Self::NotEquals,
            x if x == TK_BITAND as YYCODETYPE => Self::BitwiseAnd,
            x if x == TK_BITOR as YYCODETYPE => Self::BitwiseOr,
            x if x == TK_BITNOT as YYCODETYPE => Self::BitwiseNot,
            x if x == TK_LSHIFT as YYCODETYPE => Self::LeftShift,
            x if x == TK_RSHIFT as YYCODETYPE => Self::RightShift,
            x if x == TK_PLUS as YYCODETYPE => Self::Add,
            x if x == TK_MINUS as YYCODETYPE => Self::Subtract,
            x if x == TK_STAR as YYCODETYPE => Self::Multiply,
            x if x == TK_SLASH as YYCODETYPE => Self::Divide,
            x if x == TK_REM as YYCODETYPE => Self::Modulus,
            x if x == TK_CONCAT as YYCODETYPE => Self::Concat,
            x if x == TK_IS as YYCODETYPE => Self::Is,
            x if x == TK_NOT as YYCODETYPE => Self::IsNot,
            _ => unreachable!(),
        }
    }
}

impl Operator {
    /// returns whether order of operations can be ignored
    pub fn is_commutative(&self) -> bool {
        matches!(
            self,
            Operator::Add
                | Operator::Multiply
                | Operator::BitwiseAnd
                | Operator::BitwiseOr
                | Operator::Equals
                | Operator::NotEquals
        )
    }

    /// Returns true if this operator is a comparison operator that may need affinity conversion
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            Self::Equals
                | Self::NotEquals
                | Self::Less
                | Self::LessEquals
                | Self::Greater
                | Self::GreaterEquals
                | Self::Is
                | Self::IsNot
        )
    }
}

/// Unary operators
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UnaryOperator {
    /// bitwise negation (`~`)
    BitwiseNot,
    /// negative-sign
    Negative,
    /// `NOT`
    Not,
    /// positive-sign
    Positive,
}

impl From<YYCODETYPE> for UnaryOperator {
    fn from(token_type: YYCODETYPE) -> Self {
        match token_type {
            x if x == TK_BITNOT as YYCODETYPE => Self::BitwiseNot,
            x if x == TK_MINUS as YYCODETYPE => Self::Negative,
            x if x == TK_NOT as YYCODETYPE => Self::Not,
            x if x == TK_PLUS as YYCODETYPE => Self::Positive,
            _ => unreachable!(),
        }
    }
}

/// `SELECT` statement
// https://sqlite.org/lang_select.html
// https://sqlite.org/syntax/factored-select-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Select {
    /// CTE
    pub with: Option<With>,
    /// body
    pub body: SelectBody,
    /// `ORDER BY`
    pub order_by: Option<Vec<SortedColumn>>, // ORDER BY term does not match any column in the result set
    /// `LIMIT`
    pub limit: Option<Box<Limit>>,
}

/// `SELECT` body
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SelectBody {
    /// first select
    pub select: Box<OneSelect>,
    /// compounds
    pub compounds: Option<Vec<CompoundSelect>>,
}

impl SelectBody {
    pub(crate) fn push(&mut self, cs: CompoundSelect) -> Result<(), ParserError> {
        use crate::ast::check::ColumnCount;
        if let ColumnCount::Fixed(n) = self.select.column_count() {
            if let ColumnCount::Fixed(m) = cs.select.column_count() {
                if n != m {
                    return Err(custom_err!(
                        "SELECTs to the left and right of {} do not have the same number of result columns",
                        cs.operator
                    ));
                }
            }
        }
        if let Some(ref mut v) = self.compounds {
            v.push(cs);
        } else {
            self.compounds = Some(vec![cs]);
        }
        Ok(())
    }
}

/// Compound select
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CompoundSelect {
    /// operator
    pub operator: CompoundOperator,
    /// select
    pub select: Box<OneSelect>,
}

/// Compound operators
// https://sqlite.org/syntax/compound-operator.html
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CompoundOperator {
    /// `UNION`
    Union,
    /// `UNION ALL`
    UnionAll,
    /// `EXCEPT`
    Except,
    /// `INTERSECT`
    Intersect,
}

/// `SELECT` core
// https://sqlite.org/syntax/select-core.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum OneSelect {
    /// `SELECT`
    Select(Box<SelectInner>),
    /// `VALUES`
    Values(Vec<Vec<Expr>>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
/// `SELECT` core
pub struct SelectInner {
    /// `DISTINCT`
    pub distinctness: Option<Distinctness>,
    /// columns
    pub columns: Vec<ResultColumn>,
    /// `FROM` clause
    pub from: Option<FromClause>,
    /// `WHERE` clause
    pub where_clause: Option<Expr>,
    /// `GROUP BY`
    pub group_by: Option<GroupBy>,
    /// `WINDOW` definition
    pub window_clause: Option<Vec<WindowDef>>,
}

/// `SELECT` ... `FROM` clause
// https://sqlite.org/syntax/join-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FromClause {
    /// table
    pub select: Option<Box<SelectTable>>, // FIXME mandatory
    /// `JOIN`ed tabled
    pub joins: Option<Vec<JoinedSelectTable>>,
    op: Option<JoinOperator>, // FIXME transient
}
impl FromClause {
    pub(crate) fn empty() -> Self {
        Self {
            select: None,
            joins: None,
            op: None,
        }
    }

    pub(crate) fn push(
        &mut self,
        table: SelectTable,
        jc: Option<JoinConstraint>,
    ) -> Result<(), ParserError> {
        let op = self.op.take();
        if let Some(op) = op {
            let jst = JoinedSelectTable {
                operator: op,
                table,
                constraint: jc,
            };
            if jst.operator.is_natural() && jst.constraint.is_some() {
                return Err(custom_err!(
                    "a NATURAL join may not have an ON or USING clause"
                ));
            }
            if let Some(ref mut joins) = self.joins {
                joins.push(jst);
            } else {
                self.joins = Some(vec![jst]);
            }
        } else {
            if jc.is_some() {
                return Err(custom_err!("a JOIN clause is required before ON"));
            }
            debug_assert!(self.select.is_none());
            debug_assert!(self.joins.is_none());
            self.select = Some(Box::new(table));
        }
        Ok(())
    }

    pub(crate) fn push_op(&mut self, op: JoinOperator) {
        self.op = Some(op);
    }
}

/// `SELECT` distinctness
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Distinctness {
    /// `DISTINCT`
    Distinct,
    /// `ALL`
    All,
}

/// `SELECT` or `RETURNING` result column
// https://sqlite.org/syntax/result-column.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ResultColumn {
    /// expression
    Expr(Expr, Option<As>),
    /// `*`
    Star,
    /// table name.`*`
    TableStar(Name),
}

/// Alias
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum As {
    /// `AS`
    As(Name),
    /// no `AS`
    Elided(Name), // FIXME Ids
}

/// `JOIN` clause
// https://sqlite.org/syntax/join-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JoinedSelectTable {
    /// operator
    pub operator: JoinOperator,
    /// table
    pub table: SelectTable,
    /// constraint
    pub constraint: Option<JoinConstraint>,
}

/// Table or subquery
// https://sqlite.org/syntax/table-or-subquery.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SelectTable {
    /// table
    Table(QualifiedName, Option<As>, Option<Indexed>),
    /// table function call
    TableCall(QualifiedName, Option<Vec<Expr>>, Option<As>),
    /// `SELECT` subquery
    Select(Box<Select>, Option<As>),
    /// subquery
    Sub(FromClause, Option<As>),
}

/// Join operators
// https://sqlite.org/syntax/join-operator.html
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum JoinOperator {
    /// `,`
    Comma,
    /// `JOIN`
    TypedJoin(Option<JoinType>),
}

impl JoinOperator {
    pub(crate) fn from(
        token: Token,
        n1: Option<Name>,
        n2: Option<Name>,
    ) -> Result<Self, ParserError> {
        Ok({
            let mut jt = JoinType::try_from(token.1)?;
            for n in [&n1, &n2].into_iter().flatten() {
                jt |= JoinType::try_from(n.0.as_ref())?;
            }
            if (jt & (JoinType::INNER | JoinType::OUTER)) == (JoinType::INNER | JoinType::OUTER)
                || (jt & (JoinType::OUTER | JoinType::LEFT | JoinType::RIGHT)) == JoinType::OUTER
            {
                return Err(custom_err!(
                    "unsupported JOIN type: {:?} {:?} {:?}",
                    str::from_utf8(token.1),
                    n1,
                    n2
                ));
            }
            Self::TypedJoin(Some(jt))
        })
    }
    fn is_natural(&self) -> bool {
        match self {
            Self::TypedJoin(Some(jt)) => jt.contains(JoinType::NATURAL),
            _ => false,
        }
    }
}

// https://github.com/sqlite/sqlite/blob/80511f32f7e71062026edd471913ef0455563964/src/select.c#L197-L257
bitflags::bitflags! {
    /// `JOIN` types
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    pub struct JoinType: u8 {
        /// `INNER`
        const INNER   = 0x01;
        /// `CROSS` => INNER|CROSS
        const CROSS   = 0x02;
        /// `NATURAL`
        const NATURAL = 0x04;
        /// `LEFT` => LEFT|OUTER
        const LEFT    = 0x08;
        /// `RIGHT` => RIGHT|OUTER
        const RIGHT   = 0x10;
        /// `OUTER`
        const OUTER   = 0x20;
    }
}

impl TryFrom<&[u8]> for JoinType {
    type Error = ParserError;
    fn try_from(s: &[u8]) -> Result<Self, ParserError> {
        if b"CROSS".eq_ignore_ascii_case(s) {
            Ok(Self::INNER | Self::CROSS)
        } else if b"FULL".eq_ignore_ascii_case(s) {
            Ok(Self::LEFT | Self::RIGHT | Self::OUTER)
        } else if b"INNER".eq_ignore_ascii_case(s) {
            Ok(Self::INNER)
        } else if b"LEFT".eq_ignore_ascii_case(s) {
            Ok(Self::LEFT | Self::OUTER)
        } else if b"NATURAL".eq_ignore_ascii_case(s) {
            Ok(Self::NATURAL)
        } else if b"RIGHT".eq_ignore_ascii_case(s) {
            Ok(Self::RIGHT | Self::OUTER)
        } else if b"OUTER".eq_ignore_ascii_case(s) {
            Ok(Self::OUTER)
        } else {
            Err(custom_err!(
                "unsupported JOIN type: {:?}",
                str::from_utf8(s)
            ))
        }
    }
}

/// `JOIN` constraint
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum JoinConstraint {
    /// `ON`
    On(Expr),
    /// `USING`: col names
    Using(DistinctNames),
}

/// `GROUP BY`
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct GroupBy {
    /// expressions
    pub exprs: Vec<Expr>,
    /// `HAVING`
    pub having: Option<Box<Expr>>, // HAVING clause on a non-aggregate query
}

/// identifier or one of several keywords or `INDEXED`
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Id(pub String);

impl Id {
    /// Constructor
    pub fn from_token(ty: YYCODETYPE, token: Token) -> Self {
        Self(from_token(ty, token))
    }
}

// TODO ids (identifier or string)

/// identifier or string or `CROSS` or `FULL` or `INNER` or `LEFT` or `NATURAL` or `OUTER` or `RIGHT`.
#[derive(Clone, Debug, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Name(pub String); // TODO distinction between Name and "Name"/[Name]/`Name`

impl Name {
    /// Constructor
    pub fn from_token(ty: YYCODETYPE, token: Token) -> Self {
        Self(from_token(ty, token))
    }

    fn as_bytes(&self) -> QuotedIterator<'_> {
        if self.0.is_empty() {
            return QuotedIterator(self.0.bytes(), 0);
        }
        let bytes = self.0.as_bytes();
        let mut quote = bytes[0];
        if quote != b'"' && quote != b'`' && quote != b'\'' && quote != b'[' {
            return QuotedIterator(self.0.bytes(), 0);
        } else if quote == b'[' {
            quote = b']';
        }
        debug_assert!(bytes.len() > 1);
        debug_assert_eq!(quote, bytes[bytes.len() - 1]);
        let sub = &self.0.as_str()[1..bytes.len() - 1];
        if quote == b']' {
            return QuotedIterator(sub.bytes(), 0); // no escape
        }
        QuotedIterator(sub.bytes(), quote)
    }
}

struct QuotedIterator<'s>(Bytes<'s>, u8);
impl Iterator for QuotedIterator<'_> {
    type Item = u8;

    fn next(&mut self) -> Option<u8> {
        match self.0.next() {
            x @ Some(b) => {
                if b == self.1 && self.0.next() != Some(self.1) {
                    panic!("Malformed string literal: {:?}", self.0);
                }
                x
            }
            x => x,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.1 == 0 {
            return self.0.size_hint();
        }
        (0, None)
    }
}

fn eq_ignore_case_and_quote(mut it: QuotedIterator<'_>, mut other: QuotedIterator<'_>) -> bool {
    loop {
        match (it.next(), other.next()) {
            (Some(b1), Some(b2)) => {
                if !b1.eq_ignore_ascii_case(&b2) {
                    return false;
                }
            }
            (None, None) => break,
            _ => return false,
        }
    }
    true
}

/// Ignore case and quote
impl std::hash::Hash for Name {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        self.as_bytes()
            .for_each(|b| hasher.write_u8(b.to_ascii_lowercase()));
    }
}
/// Ignore case and quote
impl PartialEq for Name {
    fn eq(&self, other: &Self) -> bool {
        eq_ignore_case_and_quote(self.as_bytes(), other.as_bytes())
    }
}
/// Ignore case and quote
impl PartialEq<str> for Name {
    fn eq(&self, other: &str) -> bool {
        eq_ignore_case_and_quote(self.as_bytes(), QuotedIterator(other.bytes(), 0u8))
    }
}
/// Ignore case and quote
impl PartialEq<&str> for Name {
    fn eq(&self, other: &&str) -> bool {
        eq_ignore_case_and_quote(self.as_bytes(), QuotedIterator(other.bytes(), 0u8))
    }
}

/// Qualified name
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct QualifiedName {
    /// schema
    pub db_name: Option<Name>,
    /// object name
    pub name: Name,
    /// alias
    pub alias: Option<Name>, // FIXME restrict alias usage (fullname vs xfullname)
}

impl QualifiedName {
    /// Constructor
    pub fn single(name: Name) -> Self {
        Self {
            db_name: None,
            name,
            alias: None,
        }
    }
    /// Constructor
    pub fn fullname(db_name: Name, name: Name) -> Self {
        Self {
            db_name: Some(db_name),
            name,
            alias: None,
        }
    }
    /// Constructor
    pub fn xfullname(db_name: Name, name: Name, alias: Name) -> Self {
        Self {
            db_name: Some(db_name),
            name,
            alias: Some(alias),
        }
    }
    /// Constructor
    pub fn alias(name: Name, alias: Name) -> Self {
        Self {
            db_name: None,
            name,
            alias: Some(alias),
        }
    }
}

/// Ordered set of distinct column names
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DistinctNames(IndexSet<Name>);

impl DistinctNames {
    /// Initialize
    pub fn new(name: Name) -> Self {
        let mut dn = Self(IndexSet::new());
        dn.0.insert(name);
        dn
    }
    /// Single column name
    pub fn single(name: Name) -> Self {
        let mut dn = Self(IndexSet::with_capacity(1));
        dn.0.insert(name);
        dn
    }
    /// Push a distinct name or fail
    pub fn insert(&mut self, name: Name) -> Result<(), ParserError> {
        if self.0.contains(&name) {
            return Err(custom_err!("column \"{}\" specified more than once", name));
        }
        self.0.insert(name);
        Ok(())
    }
}
impl Deref for DistinctNames {
    type Target = IndexSet<Name>;

    fn deref(&self) -> &IndexSet<Name> {
        &self.0
    }
}

/// `ALTER TABLE` body
// https://sqlite.org/lang_altertable.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AlterTableBody {
    /// `RENAME TO`: new table name
    RenameTo(Name),
    /// `ADD COLUMN`
    AddColumn(ColumnDefinition), // TODO distinction between ADD and ADD COLUMN
    /// `RENAME COLUMN`
    RenameColumn {
        /// old name
        old: Name,
        /// new name
        new: Name,
    },
    /// `DROP COLUMN`
    DropColumn(Name), // TODO distinction between DROP and DROP COLUMN
}

/// `CREATE TABLE` body
// https://sqlite.org/lang_createtable.html
// https://sqlite.org/syntax/create-table-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CreateTableBody {
    /// columns and constraints
    ColumnsAndConstraints {
        /// table column definitions
        columns: IndexMap<Name, ColumnDefinition>,
        /// table constraints
        constraints: Option<Vec<NamedTableConstraint>>,
        /// table options
        options: TableOptions,
    },
    /// `AS` select
    AsSelect(Box<Select>),
}

impl CreateTableBody {
    /// Constructor
    pub fn columns_and_constraints(
        columns: IndexMap<Name, ColumnDefinition>,
        constraints: Option<Vec<NamedTableConstraint>>,
        options: TableOptions,
    ) -> Result<Self, ParserError> {
        Ok(Self::ColumnsAndConstraints {
            columns,
            constraints,
            options,
        })
    }

    /// Constructor from Vec of column definition
    pub fn columns_and_constraints_from_definition(
        columns_vec: Vec<ColumnDefinition>,
        constraints: Option<Vec<NamedTableConstraint>>,
        options: TableOptions,
    ) -> Result<Self, ParserError> {
        let mut columns = IndexMap::new();
        for def in columns_vec {
            columns.insert(def.col_name.clone(), def);
        }
        Ok(Self::ColumnsAndConstraints {
            columns,
            constraints,
            options,
        })
    }
}

/// Table column definition
// https://sqlite.org/syntax/column-def.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ColumnDefinition {
    /// column name
    pub col_name: Name,
    /// column type
    pub col_type: Option<Type>,
    /// column constraints
    pub constraints: Vec<NamedColumnConstraint>,
}

impl ColumnDefinition {
    /// Constructor
    pub fn add_column(columns: &mut IndexMap<Name, Self>, mut cd: Self) -> Result<(), ParserError> {
        let col_name = &cd.col_name;
        if columns.contains_key(col_name) {
            // TODO unquote
            return Err(custom_err!("duplicate column name: {}", col_name));
        }
        // https://github.com/sqlite/sqlite/blob/e452bf40a14aca57fd9047b330dff282f3e4bbcc/src/build.c#L1511-L1514
        if let Some(ref mut col_type) = cd.col_type {
            let mut split = col_type.name.split_ascii_whitespace();
            let truncate = if split
                .next_back()
                .is_some_and(|s| s.eq_ignore_ascii_case("ALWAYS"))
                && split
                    .next_back()
                    .is_some_and(|s| s.eq_ignore_ascii_case("GENERATED"))
            {
                let mut generated = false;
                for constraint in &cd.constraints {
                    if let ColumnConstraint::Generated { .. } = constraint.constraint {
                        generated = true;
                        break;
                    }
                }
                generated
            } else {
                false
            };
            if truncate {
                // str_split_whitespace_remainder
                let new_type: Vec<&str> = split.collect();
                col_type.name = new_type.join(" ");
            }
        }
        for constraint in &cd.constraints {
            if let ColumnConstraint::ForeignKey {
                clause:
                    ForeignKeyClause {
                        tbl_name, columns, ..
                    },
                ..
            } = &constraint.constraint
            {
                // The child table may reference the primary key of the parent without specifying the primary key column
                if columns.as_ref().map_or(0, Vec::len) > 1 {
                    return Err(custom_err!(
                        "foreign key on {} should reference only one column of table {}",
                        col_name,
                        tbl_name
                    ));
                }
            }
        }
        columns.insert(col_name.clone(), cd);
        Ok(())
    }
}

/// Named column constraint
// https://sqlite.org/syntax/column-constraint.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NamedColumnConstraint {
    /// constraint name
    pub name: Option<Name>,
    /// constraint
    pub constraint: ColumnConstraint,
}

/// Column constraint
// https://sqlite.org/syntax/column-constraint.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ColumnConstraint {
    /// `PRIMARY KEY`
    PrimaryKey {
        /// `ASC` / `DESC`
        order: Option<SortOrder>,
        /// `ON CONFLICT` clause
        conflict_clause: Option<ResolveType>,
        /// `AUTOINCREMENT`
        auto_increment: bool,
    },
    /// `NULL`
    NotNull {
        /// `NOT`
        nullable: bool,
        /// `ON CONFLICT` clause
        conflict_clause: Option<ResolveType>,
    },
    /// `UNIQUE`
    Unique(Option<ResolveType>),
    /// `CHECK`
    Check(Expr),
    /// `DEFAULT`
    Default(Expr),
    /// `DEFERRABLE`
    Defer(DeferSubclause), // FIXME
    /// `COLLATE`
    Collate {
        /// collation name
        collation_name: Name, // FIXME Ids
    },
    /// `REFERENCES` foreign-key clause
    ForeignKey {
        /// clause
        clause: ForeignKeyClause,
        /// `DEFERRABLE`
        deref_clause: Option<DeferSubclause>,
    },
    /// `GENERATED`
    Generated {
        /// expression
        expr: Expr,
        /// `STORED` / `VIRTUAL`
        typ: Option<Id>,
    },
}

/// Named table constraint
// https://sqlite.org/syntax/table-constraint.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NamedTableConstraint {
    /// constraint name
    pub name: Option<Name>,
    /// constraint
    pub constraint: TableConstraint,
}

/// Table constraint
// https://sqlite.org/syntax/table-constraint.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TableConstraint {
    /// `PRIMARY KEY`
    PrimaryKey {
        /// columns
        columns: Vec<SortedColumn>,
        /// `AUTOINCREMENT`
        auto_increment: bool,
        /// `ON CONFLICT` clause
        conflict_clause: Option<ResolveType>,
    },
    /// `UNIQUE`
    Unique {
        /// columns
        columns: Vec<SortedColumn>,
        /// `ON CONFLICT` clause
        conflict_clause: Option<ResolveType>,
    },
    /// `CHECK`
    Check(Expr),
    /// `FOREIGN KEY`
    ForeignKey {
        /// columns
        columns: Vec<IndexedColumn>,
        /// `REFERENCES`
        clause: ForeignKeyClause,
        /// `DEFERRABLE`
        deref_clause: Option<DeferSubclause>,
    },
}

bitflags::bitflags! {
    /// `CREATE TABLE` options
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    pub struct TableOptions: u8 {
        /// None
        const NONE = 0;
        /// `WITHOUT ROWID`
        const WITHOUT_ROWID = 1;
        /// `STRICT`
        const STRICT = 2;
    }
}

/// Sort orders
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SortOrder {
    /// `ASC`
    Asc,
    /// `DESC`
    Desc,
}

/// `NULLS FIRST` or `NULLS LAST`
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum NullsOrder {
    /// `NULLS FIRST`
    First,
    /// `NULLS LAST`
    Last,
}

/// `REFERENCES` clause
// https://sqlite.org/syntax/foreign-key-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ForeignKeyClause {
    /// foreign table name
    pub tbl_name: Name,
    /// foreign table columns
    pub columns: Option<Vec<IndexedColumn>>,
    /// referential action(s) / deferrable option(s)
    pub args: Vec<RefArg>,
}

/// foreign-key reference args
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RefArg {
    /// `ON DELETE`
    OnDelete(RefAct),
    /// `ON INSERT`
    OnInsert(RefAct),
    /// `ON UPDATE`
    OnUpdate(RefAct),
    /// `MATCH`
    Match(Name),
}

/// foreign-key reference actions
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RefAct {
    /// `SET NULL`
    SetNull,
    /// `SET DEFAULT`
    SetDefault,
    /// `CASCADE`
    Cascade,
    /// `RESTRICT`
    Restrict,
    /// `NO ACTION`
    NoAction,
}

/// foreign-key defer clause
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeferSubclause {
    /// `DEFERRABLE`
    pub deferrable: bool,
    /// `INITIALLY` `DEFERRED` / `IMMEDIATE`
    pub init_deferred: Option<InitDeferredPred>,
}

/// `INITIALLY` `DEFERRED` / `IMMEDIATE`
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum InitDeferredPred {
    /// `INITIALLY DEFERRED`
    InitiallyDeferred,
    /// `INITIALLY IMMEDIATE`
    InitiallyImmediate, // default
}

/// Indexed column
// https://sqlite.org/syntax/indexed-column.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IndexedColumn {
    /// column name
    pub col_name: Name,
    /// `COLLATE`
    pub collation_name: Option<Name>, // FIXME Ids
    /// `ORDER BY`
    pub order: Option<SortOrder>,
}

/// `INDEXED BY` / `NOT INDEXED`
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Indexed {
    /// `INDEXED BY`: idx name
    IndexedBy(Name),
    /// `NOT INDEXED`
    NotIndexed,
}

/// Sorted column
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SortedColumn {
    /// expression
    pub expr: Expr,
    /// `ASC` / `DESC`
    pub order: Option<SortOrder>,
    /// `NULLS FIRST` / `NULLS LAST`
    pub nulls: Option<NullsOrder>,
}

/// `LIMIT`
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Limit {
    /// count
    pub expr: Expr,
    /// `OFFSET`
    pub offset: Option<Expr>, // TODO distinction between LIMIT offset, count and LIMIT count OFFSET offset
}

/// `INSERT` body
// https://sqlite.org/lang_insert.html
// https://sqlite.org/syntax/insert-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum InsertBody {
    /// `SELECT` or `VALUES`
    Select(Box<Select>, Option<Upsert>),
    /// `DEFAULT VALUES`
    DefaultValues,
}

/// `UPDATE ... SET`
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Set {
    /// column name(s)
    pub col_names: DistinctNames,
    /// expression
    pub expr: Expr,
}

/// `PRAGMA` body
// https://sqlite.org/syntax/pragma-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum PragmaBody {
    /// `=`
    Equals(PragmaValue),
    /// function call
    Call(PragmaValue),
}
/// `PRAGMA` value
// https://sqlite.org/syntax/pragma-value.html
pub type PragmaValue = Expr; // TODO

/// `PRAGMA` value
// https://sqlite.org/pragma.html
#[derive(Clone, Debug, PartialEq, Eq, EnumIter, EnumString, strum::Display)]
#[strum(serialize_all = "snake_case")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum PragmaName {
    /// set the autovacuum mode
    AutoVacuum,
    /// `cache_size` pragma
    CacheSize,
    /// Run integrity check on the database file
    IntegrityCheck,
    /// `journal_mode` pragma
    JournalMode,
    /// Noop as per SQLite docs
    LegacyFileFormat,
    /// Return the total number of pages in the database file.
    PageCount,
    /// Return the page size of the database in bytes.
    PageSize,
    /// Returns schema version of the database file.
    SchemaVersion,
    /// returns information about the columns of a table
    TableInfo,
    /// enable capture-changes logic for the connection
    UnstableCaptureDataChangesConn,
    /// Returns the user version of the database file.
    UserVersion,
    /// trigger a checkpoint to run on database(s) if WAL is enabled
    WalCheckpoint,
    /// Enables or disables the pager cache spilling in the middle of a transaction.
    CacheSpill,
}

/// `CREATE TRIGGER` time
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TriggerTime {
    /// `BEFORE`
    Before, // default
    /// `AFTER`
    After,
    /// `INSTEAD OF`
    InsteadOf,
}

/// `CREATE TRIGGER` event
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TriggerEvent {
    /// `DELETE`
    Delete,
    /// `INSERT`
    Insert,
    /// `UPDATE`
    Update,
    /// `UPDATE OF`: col names
    UpdateOf(DistinctNames),
}

/// `CREATE TRIGGER` command
// https://sqlite.org/lang_createtrigger.html
// https://sqlite.org/syntax/create-trigger-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TriggerCmd {
    /// `UPDATE`
    Update(Box<TriggerCmdUpdate>),
    /// `INSERT`
    Insert(Box<TriggerCmdInsert>),
    /// `DELETE`
    Delete(Box<TriggerCmdDelete>),
    /// `SELECT`
    Select(Box<Select>),
}

/// `UPDATE` trigger command
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TriggerCmdUpdate {
    /// `OR`
    pub or_conflict: Option<ResolveType>,
    /// table name
    pub tbl_name: Name,
    /// `SET` assignments
    pub sets: Vec<Set>,
    /// `FROM`
    pub from: Option<FromClause>,
    /// `WHERE` clause
    pub where_clause: Option<Expr>,
}

/// `INSERT` trigger command
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TriggerCmdInsert {
    /// `OR`
    pub or_conflict: Option<ResolveType>,
    /// table name
    pub tbl_name: Name,
    /// `COLUMNS`
    pub col_names: Option<DistinctNames>,
    /// `SELECT` or `VALUES`
    pub select: Box<Select>,
    /// `ON CONFLICT` clause
    pub upsert: Option<Upsert>,
    /// `RETURNING`
    pub returning: Option<Vec<ResultColumn>>,
}

/// `DELETE` trigger command
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TriggerCmdDelete {
    /// table name
    pub tbl_name: Name,
    /// `WHERE` clause
    pub where_clause: Option<Expr>,
}

/// Conflict resolution types
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ResolveType {
    /// `ROLLBACK`
    Rollback,
    /// `ABORT`
    Abort, // default
    /// `FAIL`
    Fail,
    /// `IGNORE`
    Ignore,
    /// `REPLACE`
    Replace,
}
impl ResolveType {
    /// Get the OE_XXX bit value
    pub fn bit_value(&self) -> usize {
        match self {
            ResolveType::Rollback => 1,
            ResolveType::Abort => 2,
            ResolveType::Fail => 3,
            ResolveType::Ignore => 4,
            ResolveType::Replace => 5,
        }
    }
}

/// `WITH` clause
// https://sqlite.org/lang_with.html
// https://sqlite.org/syntax/with-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct With {
    /// `RECURSIVE`
    pub recursive: bool,
    /// CTEs
    pub ctes: Vec<CommonTableExpr>,
}

/// CTE materialization
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Materialized {
    /// No hint
    Any,
    /// `MATERIALIZED`
    Yes,
    /// `NOT MATERIALIZED`
    No,
}

/// CTE
// https://sqlite.org/syntax/common-table-expression.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommonTableExpr {
    /// table name
    pub tbl_name: Name,
    /// table columns
    pub columns: Option<Vec<IndexedColumn>>, // check no duplicate
    /// `MATERIALIZED`
    pub materialized: Materialized,
    /// query
    pub select: Box<Select>,
}

impl CommonTableExpr {
    /// Constructor
    pub fn add_cte(ctes: &mut Vec<Self>, cte: Self) -> Result<(), ParserError> {
        if ctes.iter().any(|c| c.tbl_name == cte.tbl_name) {
            return Err(custom_err!("duplicate WITH table name: {}", cte.tbl_name));
        }
        ctes.push(cte);
        Ok(())
    }
}

/// Column type
// https://sqlite.org/syntax/type-name.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Type {
    /// type name
    pub name: String, // TODO Validate: Ids+
    /// type size
    pub size: Option<TypeSize>,
}

/// Column type size limit(s)
// https://sqlite.org/syntax/type-name.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TypeSize {
    /// maximum size
    MaxSize(Box<Expr>),
    /// precision
    TypeSize(Box<Expr>, Box<Expr>),
}

/// Transaction types
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TransactionType {
    /// `DEFERRED`
    Deferred, // default
    /// `IMMEDIATE`
    Immediate,
    /// `EXCLUSIVE`
    Exclusive,
}

/// Upsert clause
// https://sqlite.org/lang_upsert.html
// https://sqlite.org/syntax/upsert-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Upsert {
    /// conflict targets
    pub index: Option<Box<UpsertIndex>>,
    /// `DO` clause
    pub do_clause: Box<UpsertDo>,
    /// next upsert
    pub next: Option<Box<Upsert>>,
}

/// Upsert conflict targets
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UpsertIndex {
    /// columns
    pub targets: Vec<SortedColumn>,
    /// `WHERE` clause
    pub where_clause: Option<Expr>,
}

/// Upsert `DO` action
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UpsertDo {
    /// `SET`
    Set {
        /// assignments
        sets: Vec<Set>,
        /// `WHERE` clause
        where_clause: Option<Expr>,
    },
    /// `NOTHING`
    Nothing,
}

/// Function call tail
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FunctionTail {
    /// `FILTER` clause
    pub filter_clause: Option<Box<Expr>>,
    /// `OVER` clause
    pub over_clause: Option<Box<Over>>,
}

/// Function call `OVER` clause
// https://sqlite.org/syntax/over-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Over {
    /// Window definition
    Window(Window),
    /// Window name
    Name(Name),
}

/// `OVER` window definition
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct WindowDef {
    /// window name
    pub name: Name,
    /// window definition
    pub window: Window,
}

/// Window definition
// https://sqlite.org/syntax/window-defn.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Window {
    /// base window name
    pub base: Option<Name>,
    /// `PARTITION BY`
    pub partition_by: Option<Vec<Expr>>,
    /// `ORDER BY`
    pub order_by: Option<Vec<SortedColumn>>,
    /// frame spec
    pub frame_clause: Option<FrameClause>,
}

/// Frame specification
// https://sqlite.org/syntax/frame-spec.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FrameClause {
    /// unit
    pub mode: FrameMode,
    /// start bound
    pub start: FrameBound,
    /// end bound
    pub end: Option<FrameBound>,
    /// `EXCLUDE`
    pub exclude: Option<FrameExclude>,
}

/// Frame modes
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum FrameMode {
    /// `GROUPS`
    Groups,
    /// `RANGE`
    Range,
    /// `ROWS`
    Rows,
}

/// Frame bounds
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum FrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `FOLLOWING`
    Following(Box<Expr>),
    /// `PRECEDING`
    Preceding(Box<Expr>),
    /// `UNBOUNDED FOLLOWING`
    UnboundedFollowing,
    /// `UNBOUNDED PRECEDING`
    UnboundedPreceding,
}

/// Frame exclusions
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum FrameExclude {
    /// `NO OTHERS`
    NoOthers,
    /// `CURRENT ROW`
    CurrentRow,
    /// `GROUP`
    Group,
    /// `TIES`
    Ties,
}

#[cfg(test)]
mod test {
    use super::{Name, PragmaName};
    use strum::IntoEnumIterator;

    #[test]
    fn test_dequote() {
        assert_eq!(name("x"), "x");
        assert_eq!(name("`x`"), "x");
        assert_eq!(name("`x``y`"), "x`y");
        assert_eq!(name(r#""x""#), "x");
        assert_eq!(name(r#""x""y""#), "x\"y");
        assert_eq!(name("[x]"), "x");
    }

    #[test]
    // pragma pragma_list expects this to be sorted. We can avoid allocations there if we keep
    // the list sorted.
    fn pragma_list_sorted() {
        let pragma_strings: Vec<String> = PragmaName::iter().map(|x| x.to_string()).collect();
        let mut pragma_strings_sorted = pragma_strings.clone();
        pragma_strings_sorted.sort();
        assert_eq!(pragma_strings, pragma_strings_sorted);
    }

    fn name(s: &'static str) -> Name {
        Name(s.to_owned())
    }
}
