//! Generated SQL AST types with recording constructors.
//!
//! The AST types use constructors that automatically record to the context,
//! making tracing invisible to the generator code.

use crate::context::Context;
use crate::schema::DataType;
use std::fmt;

// =============================================================================
// Statements
// =============================================================================

/// A SQL statement.
#[derive(Debug, Clone, strum::EnumDiscriminants)]
#[strum_discriminants(name(StmtKind))]
#[strum_discriminants(derive(Hash, strum::EnumIter, strum::Display))]
pub enum Stmt {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    AlterTable(AlterTableStmt),
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
    CreateTrigger(CreateTriggerStmt),
    DropTrigger(DropTriggerStmt),
    Begin,
    Commit,
    Rollback,
}

impl fmt::Display for Stmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Stmt::Select(s) => write!(f, "{s}"),
            Stmt::Insert(s) => write!(f, "{s}"),
            Stmt::Update(s) => write!(f, "{s}"),
            Stmt::Delete(s) => write!(f, "{s}"),
            Stmt::CreateTable(s) => write!(f, "{s}"),
            Stmt::DropTable(s) => write!(f, "{s}"),
            Stmt::AlterTable(s) => write!(f, "{s}"),
            Stmt::CreateIndex(s) => write!(f, "{s}"),
            Stmt::DropIndex(s) => write!(f, "{s}"),
            Stmt::CreateTrigger(s) => write!(f, "{s}"),
            Stmt::DropTrigger(s) => write!(f, "{s}"),
            Stmt::Begin => write!(f, "BEGIN"),
            Stmt::Commit => write!(f, "COMMIT"),
            Stmt::Rollback => write!(f, "ROLLBACK"),
        }
    }
}

impl Stmt {
    /// Returns true if this is a SELECT with LIMIT but no ORDER BY.
    pub fn has_unordered_limit(&self) -> bool {
        match self {
            Stmt::Select(s) => s.limit.is_some() && s.order_by.is_empty(),
            _ => false,
        }
    }
}

impl StmtKind {
    /// Returns true if this is a DDL statement (modifies schema).
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            StmtKind::CreateTable
                | StmtKind::DropTable
                | StmtKind::AlterTable
                | StmtKind::CreateIndex
                | StmtKind::DropIndex
                | StmtKind::CreateTrigger
                | StmtKind::DropTrigger
        )
    }
}

/// A SELECT statement.
#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub distinct: bool,
    pub columns: Vec<SelectColumn>,
    /// The FROM table. None for table-less SELECTs (e.g. `SELECT 1+2`).
    pub from: Option<String>,
    pub from_alias: Option<String>,
    pub where_clause: Option<Expr>,
    pub group_by: Option<GroupByClause>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// A GROUP BY clause with an optional HAVING condition.
#[derive(Debug, Clone)]
pub struct GroupByClause {
    pub exprs: Vec<Expr>,
    pub having: Option<Expr>,
}

impl fmt::Display for SelectStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT ")?;
        if self.distinct {
            write!(f, "DISTINCT ")?;
        }

        if self.columns.is_empty() {
            write!(f, "*")?;
        } else {
            for (i, col) in self.columns.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{col}")?;
            }
        }

        if let Some(from) = &self.from {
            write!(f, " FROM {from}")?;
            if let Some(alias) = &self.from_alias {
                write!(f, " AS {alias}")?;
            }
        }

        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }

        if let Some(group_by) = &self.group_by {
            write!(f, " GROUP BY ")?;
            for (i, expr) in group_by.exprs.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{expr}")?;
            }
            if let Some(having) = &group_by.having {
                write!(f, " HAVING {having}")?;
            }
        }

        if !self.order_by.is_empty() {
            write!(f, " ORDER BY ")?;
            for (i, item) in self.order_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{item}")?;
            }
        }

        if let Some(limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }

        if let Some(offset) = self.offset {
            write!(f, " OFFSET {offset}")?;
        }

        Ok(())
    }
}

/// A column in a SELECT statement.
#[derive(Debug, Clone)]
pub struct SelectColumn {
    pub expr: Expr,
    pub alias: Option<String>,
}

impl fmt::Display for SelectColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if let Some(alias) = &self.alias {
            write!(f, " AS {alias}")?;
        }
        Ok(())
    }
}

/// An ORDER BY item.
#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub expr: Expr,
    pub direction: OrderDirection,
    pub nulls: Option<NullsOrder>,
}

impl fmt::Display for OrderByItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.expr, self.direction)?;
        if let Some(nulls) = &self.nulls {
            write!(f, " {nulls}")?;
        }
        Ok(())
    }
}

/// Order direction.
#[derive(Debug, Clone, Copy)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl fmt::Display for OrderDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderDirection::Asc => write!(f, "ASC"),
            OrderDirection::Desc => write!(f, "DESC"),
        }
    }
}

/// NULLS ordering.
#[derive(Debug, Clone, Copy)]
pub enum NullsOrder {
    First,
    Last,
}

impl fmt::Display for NullsOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NullsOrder::First => write!(f, "NULLS FIRST"),
            NullsOrder::Last => write!(f, "NULLS LAST"),
        }
    }
}

/// An INSERT statement.
#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
}

impl fmt::Display for InsertStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "INSERT INTO {}", self.table)?;

        if !self.columns.is_empty() {
            write!(f, " (")?;
            for (i, col) in self.columns.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{col}")?;
            }
            write!(f, ")")?;
        }

        write!(f, " VALUES ")?;
        for (i, row) in self.values.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "(")?;
            for (j, val) in row.iter().enumerate() {
                if j > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{val}")?;
            }
            write!(f, ")")?;
        }

        Ok(())
    }
}

/// An UPDATE statement.
#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub table: String,
    pub sets: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

impl fmt::Display for UpdateStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UPDATE {} SET ", self.table)?;

        for (i, (col, val)) in self.sets.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{col} = {val}")?;
        }

        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }

        Ok(())
    }
}

/// A DELETE statement.
#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<Expr>,
}

impl fmt::Display for DeleteStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DELETE FROM {}", self.table)?;

        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {where_clause}")?;
        }

        Ok(())
    }
}

/// A CREATE TABLE statement.
#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub table: String,
    pub columns: Vec<ColumnDefStmt>,
    pub if_not_exists: bool,
}

impl fmt::Display for CreateTableStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE TABLE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} (", self.table)?;

        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{col}")?;
        }

        write!(f, ")")
    }
}

/// A column definition in CREATE TABLE.
#[derive(Debug, Clone)]
pub struct ColumnDefStmt {
    pub name: String,
    pub data_type: DataType,
    pub primary_key: bool,
    pub not_null: bool,
    pub unique: bool,
    pub default: Option<Expr>,
}

impl fmt::Display for ColumnDefStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;

        if self.primary_key {
            write!(f, " PRIMARY KEY")?;
        }

        if self.not_null && !self.primary_key {
            write!(f, " NOT NULL")?;
        }

        if self.unique && !self.primary_key {
            write!(f, " UNIQUE")?;
        }

        if let Some(default) = &self.default {
            write!(f, " DEFAULT ({default})")?;
        }

        Ok(())
    }
}

/// A DROP TABLE statement.
#[derive(Debug, Clone)]
pub struct DropTableStmt {
    pub table: String,
    pub if_exists: bool,
}

impl fmt::Display for DropTableStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP TABLE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.table)
    }
}

/// An ALTER TABLE statement.
#[derive(Debug, Clone)]
pub struct AlterTableStmt {
    pub table: String,
    pub action: AlterTableAction,
}

impl fmt::Display for AlterTableStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER TABLE {} {}", self.table, self.action)
    }
}

/// An action in an ALTER TABLE statement.
#[derive(Debug, Clone, strum::EnumDiscriminants)]
#[strum_discriminants(name(AlterTableActionKind))]
pub enum AlterTableAction {
    /// RENAME TO new_name
    RenameTo(String),
    /// ADD COLUMN column_def
    AddColumn(ColumnDefStmt),
    /// DROP COLUMN column_name
    DropColumn(String),
    /// RENAME COLUMN old_name TO new_name
    RenameColumn { old_name: String, new_name: String },
    // TODO: ADD alter column later
}

impl fmt::Display for AlterTableAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterTableAction::RenameTo(new_name) => write!(f, "RENAME TO {new_name}"),
            AlterTableAction::AddColumn(col) => write!(f, "ADD COLUMN {col}"),
            AlterTableAction::DropColumn(col_name) => write!(f, "DROP COLUMN {col_name}"),
            AlterTableAction::RenameColumn { old_name, new_name } => {
                write!(f, "RENAME COLUMN {old_name} TO {new_name}")
            }
        }
    }
}

/// A CREATE INDEX statement.
#[derive(Debug, Clone)]
pub struct CreateIndexStmt {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub if_not_exists: bool,
}

impl fmt::Display for CreateIndexStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE ")?;
        if self.unique {
            write!(f, "UNIQUE ")?;
        }
        write!(f, "INDEX ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} ON {} (", self.name, self.table)?;

        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{col}")?;
        }

        write!(f, ")")
    }
}

/// A DROP INDEX statement.
#[derive(Debug, Clone)]
pub struct DropIndexStmt {
    pub name: String,
    pub if_exists: bool,
}

impl fmt::Display for DropIndexStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP INDEX ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)
    }
}

/// A CREATE TRIGGER statement.
#[derive(Debug, Clone)]
pub struct CreateTriggerStmt {
    pub name: String,
    pub table: String,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    pub for_each_row: bool,
    pub when_clause: Option<Expr>,
    pub body: Vec<TriggerStmt>,
    pub if_not_exists: bool,
}

impl fmt::Display for CreateTriggerStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE TRIGGER ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(
            f,
            "{} {} {} ON {}",
            self.name, self.timing, self.event, self.table
        )?;

        if self.for_each_row {
            write!(f, " FOR EACH ROW")?;
        }

        if let Some(when) = &self.when_clause {
            write!(f, " WHEN {when}")?;
        }

        write!(f, " BEGIN ")?;
        for stmt in &self.body {
            write!(f, "{stmt}; ")?;
        }
        write!(f, "END")
    }
}

/// Trigger timing (BEFORE, AFTER, INSTEAD OF).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

impl fmt::Display for TriggerTiming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TriggerTiming::Before => write!(f, "BEFORE"),
            TriggerTiming::After => write!(f, "AFTER"),
            TriggerTiming::InsteadOf => write!(f, "INSTEAD OF"),
        }
    }
}

/// Trigger event (INSERT, UPDATE, DELETE).
#[derive(Debug, Clone, PartialEq, Eq, strum::EnumDiscriminants)]
#[strum_discriminants(name(TriggerEventKind))]
pub enum TriggerEvent {
    Insert,
    Update(Vec<String>), // Optional column list for UPDATE OF
    Delete,
}

impl fmt::Display for TriggerEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TriggerEvent::Insert => write!(f, "INSERT"),
            TriggerEvent::Update(cols) => {
                write!(f, "UPDATE")?;
                if !cols.is_empty() {
                    write!(f, " OF ")?;
                    for (i, col) in cols.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{col}")?;
                    }
                }
                Ok(())
            }
            TriggerEvent::Delete => write!(f, "DELETE"),
        }
    }
}

/// A statement that can appear in a trigger body.
#[derive(Debug, Clone, strum::EnumDiscriminants)]
#[strum_discriminants(name(TriggerBodyStmtKind))]
pub enum TriggerStmt {
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    Select(SelectStmt),
}

impl fmt::Display for TriggerStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TriggerStmt::Insert(s) => write!(f, "{s}"),
            TriggerStmt::Update(s) => write!(f, "{s}"),
            TriggerStmt::Delete(s) => write!(f, "{s}"),
            TriggerStmt::Select(s) => write!(f, "{s}"),
        }
    }
}

/// A DROP TRIGGER statement.
#[derive(Debug, Clone)]
pub struct DropTriggerStmt {
    pub name: String,
    pub if_exists: bool,
}

impl fmt::Display for DropTriggerStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP TRIGGER ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)
    }
}

// =============================================================================
// Expressions
// =============================================================================

/// A SQL expression.
#[derive(Debug, Clone, strum::EnumDiscriminants)]
#[strum_discriminants(name(ExprKind))]
#[strum_discriminants(derive(Hash, strum::EnumIter, strum::Display))]
pub enum Expr {
    ColumnRef(ColumnRef),
    Literal(Literal),
    BinaryOp(Box<BinaryOpExpr>),
    UnaryOp(Box<UnaryOpExpr>),
    FunctionCall(FunctionCallExpr),
    Subquery(Box<SelectStmt>),
    Case(Box<CaseExpr>),
    Cast(Box<CastExpr>),
    Between(Box<BetweenExpr>),
    InList(Box<InListExpr>),
    InSubquery(Box<InSubqueryExpr>),
    IsNull(Box<IsNullExpr>),
    Exists(Box<ExistsExpr>),
    Parenthesized(Box<Expr>),
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::ColumnRef(c) => write!(f, "{c}"),
            Expr::Literal(l) => write!(f, "{l}"),
            Expr::BinaryOp(b) => write!(f, "{b}"),
            Expr::UnaryOp(u) => write!(f, "{u}"),
            Expr::FunctionCall(fc) => write!(f, "{fc}"),
            Expr::Subquery(s) => write!(f, "({s})"),
            Expr::Case(c) => write!(f, "{c}"),
            Expr::Cast(c) => write!(f, "{c}"),
            Expr::Between(b) => write!(f, "{b}"),
            Expr::InList(i) => write!(f, "{i}"),
            Expr::InSubquery(i) => write!(f, "{i}"),
            Expr::IsNull(i) => write!(f, "{i}"),
            Expr::Exists(e) => write!(f, "{e}"),
            Expr::Parenthesized(e) => write!(f, "({e})"),
        }
    }
}

// Recording constructors for Expr
impl Expr {
    /// Create a column reference (records to context).
    pub fn column_ref(ctx: &mut Context, table: Option<String>, column: String) -> Self {
        ctx.record(ExprKind::ColumnRef);
        Expr::ColumnRef(ColumnRef { table, column })
    }

    /// Create a literal (records to context).
    pub fn literal(ctx: &mut Context, lit: Literal) -> Self {
        ctx.record(ExprKind::Literal);
        Expr::Literal(lit)
    }

    /// Create a binary operation (records to context).
    pub fn binary_op(ctx: &mut Context, left: Expr, op: BinOp, right: Expr) -> Self {
        ctx.record(ExprKind::BinaryOp);
        Expr::BinaryOp(Box::new(BinaryOpExpr { left, op, right }))
    }

    /// Create a unary operation (records to context).
    pub fn unary_op(ctx: &mut Context, op: UnaryOp, operand: Expr) -> Self {
        ctx.record(ExprKind::UnaryOp);
        Expr::UnaryOp(Box::new(UnaryOpExpr { op, operand }))
    }

    /// Create a function call (records to context).
    pub fn function_call(ctx: &mut Context, name: String, args: Vec<Expr>) -> Self {
        ctx.record(ExprKind::FunctionCall);
        Expr::FunctionCall(FunctionCallExpr { name, args })
    }

    /// Create a subquery (records to context).
    pub fn subquery(ctx: &mut Context, select: SelectStmt) -> Self {
        ctx.record(ExprKind::Subquery);
        Expr::Subquery(Box::new(select))
    }

    /// Create a CASE expression (records to context).
    pub fn case_expr(
        ctx: &mut Context,
        operand: Option<Expr>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Expr>,
    ) -> Self {
        ctx.record(ExprKind::Case);
        Expr::Case(Box::new(CaseExpr {
            operand: operand.map(Box::new),
            when_clauses,
            else_clause: else_clause.map(Box::new),
        }))
    }

    /// Create a CAST expression (records to context).
    pub fn cast(ctx: &mut Context, expr: Expr, target_type: DataType) -> Self {
        ctx.record(ExprKind::Cast);
        Expr::Cast(Box::new(CastExpr { expr, target_type }))
    }

    /// Create a BETWEEN expression (records to context).
    pub fn between(ctx: &mut Context, expr: Expr, low: Expr, high: Expr, negated: bool) -> Self {
        ctx.record(ExprKind::Between);
        Expr::Between(Box::new(BetweenExpr {
            expr,
            low,
            high,
            negated,
        }))
    }

    /// Create an IN list expression (records to context).
    pub fn in_list(ctx: &mut Context, expr: Expr, list: Vec<Expr>, negated: bool) -> Self {
        ctx.record(ExprKind::InList);
        Expr::InList(Box::new(InListExpr {
            expr,
            list,
            negated,
        }))
    }

    /// Create an IS NULL expression (records to context).
    pub fn is_null(ctx: &mut Context, expr: Expr, negated: bool) -> Self {
        ctx.record(ExprKind::IsNull);
        Expr::IsNull(Box::new(IsNullExpr { expr, negated }))
    }

    /// Create an EXISTS expression (records to context).
    pub fn exists(ctx: &mut Context, subquery: SelectStmt, negated: bool) -> Self {
        ctx.record(ExprKind::Exists);
        Expr::Exists(Box::new(ExistsExpr { subquery, negated }))
    }

    /// Create an IN subquery expression (records to context).
    pub fn in_subquery(ctx: &mut Context, expr: Expr, subquery: SelectStmt, negated: bool) -> Self {
        ctx.record(ExprKind::InSubquery);
        Expr::InSubquery(Box::new(InSubqueryExpr {
            expr,
            subquery,
            negated,
        }))
    }

    /// Create a parenthesized expression (does not record - it's just grouping).
    pub fn parenthesized(expr: Expr) -> Self {
        Expr::Parenthesized(Box::new(expr))
    }
}

/// A column reference.
#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(table) = &self.table {
            write!(f, "{table}.")?;
        }
        write!(f, "{}", self.column)
    }
}

/// A literal value.
#[derive(Debug, Clone)]
pub enum Literal {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Integer(i) => write!(f, "{i}"),
            Literal::Real(r) => {
                if r.is_infinite() || r.is_nan() {
                    write!(f, "NULL")
                } else {
                    write!(f, "{r}")
                }
            }
            Literal::Text(s) => {
                // Escape single quotes
                let escaped = s.replace('\'', "''");
                write!(f, "'{escaped}'")
            }
            Literal::Blob(b) => {
                write!(f, "X'")?;
                for byte in b {
                    write!(f, "{byte:02X}")?;
                }
                write!(f, "'")
            }
        }
    }
}

/// A binary operation.
#[derive(Debug, Clone)]
pub struct BinaryOpExpr {
    pub left: Expr,
    pub op: BinOp,
    pub right: Expr,
}

impl fmt::Display for BinaryOpExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    // Logical
    And,
    Or,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // String
    Concat,
    Like,
    Glob,
}

impl fmt::Display for BinOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinOp::Eq => write!(f, "="),
            BinOp::Ne => write!(f, "!="),
            BinOp::Lt => write!(f, "<"),
            BinOp::Le => write!(f, "<="),
            BinOp::Gt => write!(f, ">"),
            BinOp::Ge => write!(f, ">="),
            BinOp::And => write!(f, "AND"),
            BinOp::Or => write!(f, "OR"),
            BinOp::Add => write!(f, "+"),
            BinOp::Sub => write!(f, "-"),
            BinOp::Mul => write!(f, "*"),
            BinOp::Div => write!(f, "/"),
            BinOp::Mod => write!(f, "%"),
            BinOp::Concat => write!(f, "||"),
            BinOp::Like => write!(f, "LIKE"),
            BinOp::Glob => write!(f, "GLOB"),
        }
    }
}

impl BinOp {
    /// Returns comparison operators.
    pub fn comparison() -> &'static [BinOp] {
        &[
            BinOp::Eq,
            BinOp::Ne,
            BinOp::Lt,
            BinOp::Le,
            BinOp::Gt,
            BinOp::Ge,
        ]
    }

    /// Returns logical operators.
    pub fn logical() -> &'static [BinOp] {
        &[BinOp::And, BinOp::Or]
    }

    /// Returns arithmetic operators.
    pub fn arithmetic() -> &'static [BinOp] {
        &[BinOp::Add, BinOp::Sub, BinOp::Mul, BinOp::Div, BinOp::Mod]
    }

    /// Returns string operators.
    pub fn string() -> &'static [BinOp] {
        &[BinOp::Concat, BinOp::Like, BinOp::Glob]
    }

    /// Returns true if this is a comparison operator.
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge
        )
    }

    /// Returns true if this is a logical operator.
    pub fn is_logical(&self) -> bool {
        matches!(self, BinOp::And | BinOp::Or)
    }
}

/// A unary operation.
#[derive(Debug, Clone)]
pub struct UnaryOpExpr {
    pub op: UnaryOp,
    pub operand: Expr,
}

impl fmt::Display for UnaryOpExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.op {
            UnaryOp::Neg => write!(f, "-{}", self.operand),
            UnaryOp::Not => write!(f, "NOT {}", self.operand),
            UnaryOp::BitNot => write!(f, "~{}", self.operand),
        }
    }
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
    BitNot,
}

/// A function call.
#[derive(Debug, Clone)]
pub struct FunctionCallExpr {
    pub name: String,
    pub args: Vec<Expr>,
}

impl fmt::Display for FunctionCallExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, arg) in self.args.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{arg}")?;
        }
        write!(f, ")")
    }
}

/// A CASE expression.
#[derive(Debug, Clone)]
pub struct CaseExpr {
    pub operand: Option<Box<Expr>>,
    pub when_clauses: Vec<(Expr, Expr)>,
    pub else_clause: Option<Box<Expr>>,
}

impl fmt::Display for CaseExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CASE")?;
        if let Some(op) = &self.operand {
            write!(f, " {op}")?;
        }
        for (when_expr, then_expr) in &self.when_clauses {
            write!(f, " WHEN {when_expr} THEN {then_expr}")?;
        }
        if let Some(else_expr) = &self.else_clause {
            write!(f, " ELSE {else_expr}")?;
        }
        write!(f, " END")
    }
}

/// A CAST expression.
#[derive(Debug, Clone)]
pub struct CastExpr {
    pub expr: Expr,
    pub target_type: DataType,
}

impl fmt::Display for CastExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST({} AS {})", self.expr, self.target_type)
    }
}

/// A BETWEEN expression.
#[derive(Debug, Clone)]
pub struct BetweenExpr {
    pub expr: Expr,
    pub low: Expr,
    pub high: Expr,
    pub negated: bool,
}

impl fmt::Display for BetweenExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if self.negated {
            write!(f, " NOT")?;
        }
        write!(f, " BETWEEN {} AND {}", self.low, self.high)
    }
}

/// An IN list expression.
#[derive(Debug, Clone)]
pub struct InListExpr {
    pub expr: Expr,
    pub list: Vec<Expr>,
    pub negated: bool,
}

impl fmt::Display for InListExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if self.negated {
            write!(f, " NOT")?;
        }
        write!(f, " IN (")?;
        for (i, item) in self.list.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{item}")?;
        }
        write!(f, ")")
    }
}

/// An IS NULL expression.
#[derive(Debug, Clone)]
pub struct IsNullExpr {
    pub expr: Expr,
    pub negated: bool,
}

impl fmt::Display for IsNullExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if self.negated {
            write!(f, " IS NOT NULL")
        } else {
            write!(f, " IS NULL")
        }
    }
}

/// An EXISTS expression.
#[derive(Debug, Clone)]
pub struct ExistsExpr {
    pub subquery: SelectStmt,
    pub negated: bool,
}

impl fmt::Display for ExistsExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.negated {
            write!(f, "NOT ")?;
        }
        write!(f, "EXISTS ({})", self.subquery)
    }
}

/// An IN subquery expression.
#[derive(Debug, Clone)]
pub struct InSubqueryExpr {
    pub expr: Expr,
    pub subquery: SelectStmt,
    pub negated: bool,
}

impl fmt::Display for InSubqueryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if self.negated {
            write!(f, " NOT")?;
        }
        write!(f, " IN ({})", self.subquery)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_display() {
        assert_eq!(Literal::Null.to_string(), "NULL");
        assert_eq!(Literal::Integer(42).to_string(), "42");
        assert_eq!(Literal::Real(3.5).to_string(), "3.5");
        assert_eq!(Literal::Text("hello".to_string()).to_string(), "'hello'");
        assert_eq!(Literal::Text("it's".to_string()).to_string(), "'it''s'");
        assert_eq!(Literal::Blob(vec![0xDE, 0xAD]).to_string(), "X'DEAD'");
    }

    #[test]
    fn test_select_display() {
        let select = SelectStmt {
            distinct: false,
            columns: vec![SelectColumn {
                expr: Expr::ColumnRef(ColumnRef {
                    table: None,
                    column: "name".to_string(),
                }),
                alias: None,
            }],
            from: Some("users".to_string()),
            from_alias: None,
            where_clause: None,
            group_by: None,
            order_by: vec![],
            limit: Some(10),
            offset: None,
        };

        assert_eq!(select.to_string(), "SELECT name FROM users LIMIT 10");
    }

    #[test]
    fn test_select_distinct_display() {
        let select = SelectStmt {
            distinct: true,
            columns: vec![SelectColumn {
                expr: Expr::ColumnRef(ColumnRef {
                    table: None,
                    column: "name".to_string(),
                }),
                alias: None,
            }],
            from: Some("users".to_string()),
            from_alias: None,
            where_clause: None,
            group_by: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        assert_eq!(select.to_string(), "SELECT DISTINCT name FROM users");
    }

    #[test]
    fn test_insert_display() {
        let mut ctx = Context::new_with_seed(42);
        let insert = InsertStmt {
            table: "users".to_string(),
            columns: vec!["name".to_string(), "age".to_string()],
            values: vec![vec![
                Expr::literal(&mut ctx, Literal::Text("Alice".to_string())),
                Expr::literal(&mut ctx, Literal::Integer(30)),
            ]],
        };

        assert_eq!(
            insert.to_string(),
            "INSERT INTO users (name, age) VALUES ('Alice', 30)"
        );
    }

    #[test]
    fn test_binary_op_display() {
        let mut ctx = Context::new_with_seed(42);
        let left = Expr::literal(&mut ctx, Literal::Integer(1));
        let right = Expr::literal(&mut ctx, Literal::Integer(2));
        let expr = Expr::binary_op(&mut ctx, left, BinOp::Add, right);

        assert_eq!(expr.to_string(), "1 + 2");
    }

    #[test]
    fn test_case_display() {
        let mut ctx = Context::new_with_seed(42);
        let when_expr = Expr::literal(&mut ctx, Literal::Integer(1));
        let then_expr = Expr::literal(&mut ctx, Literal::Text("one".to_string()));
        let else_expr = Expr::literal(&mut ctx, Literal::Text("other".to_string()));
        let case = Expr::case_expr(
            &mut ctx,
            None,
            vec![(when_expr, then_expr)],
            Some(else_expr),
        );

        assert_eq!(case.to_string(), "CASE WHEN 1 THEN 'one' ELSE 'other' END");
    }
}
