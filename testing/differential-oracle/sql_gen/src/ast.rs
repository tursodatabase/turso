//! Generated SQL AST types with recording constructors.
//!
//! The AST types use constructors that automatically record to the context,
//! making tracing invisible to the generator code.

use crate::context::Context;
use crate::functions::AGGREGATE_FUNCTIONS;
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
    // Stubs: not yet generated (weight 0), shown in coverage report
    CreateView,
    DropView,
    Vacuum,
    Reindex,
    Analyze,
    Savepoint(String),
    Release(String),
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
            // Stubs
            Stmt::CreateView => todo!("CREATE VIEW generation"),
            Stmt::DropView => todo!("DROP VIEW generation"),
            Stmt::Vacuum => todo!("VACUUM generation"),
            Stmt::Reindex => todo!("REINDEX generation"),
            Stmt::Analyze => todo!("ANALYZE generation"),
            Stmt::Savepoint(_) => todo!("SAVEPOINT generation"),
            Stmt::Release(_) => todo!("RELEASE generation"),
        }
    }
}

impl Stmt {
    /// Returns true if this statement contains any SELECT with LIMIT but no ORDER BY,
    /// including in subqueries within expressions.
    pub fn has_unordered_limit(&self) -> bool {
        self.unordered_limit_reason().is_some()
    }

    /// Returns a reason code when this statement contains a potentially
    /// non-deterministic LIMIT query.
    pub fn unordered_limit_reason(&self) -> Option<&'static str> {
        match self {
            Stmt::Select(s) => s.unordered_limit_reason(),
            Stmt::Insert(_)
            | Stmt::Update(_)
            | Stmt::Delete(_)
            | Stmt::CreateTable(_)
            | Stmt::DropTable(_)
            | Stmt::AlterTable(_)
            | Stmt::CreateIndex(_)
            | Stmt::DropIndex(_)
            | Stmt::CreateTrigger(_)
            | Stmt::DropTrigger(_)
            | Stmt::Begin
            | Stmt::Commit
            | Stmt::Rollback
            | Stmt::CreateView
            | Stmt::DropView
            | Stmt::Vacuum
            | Stmt::Reindex
            | Stmt::Analyze
            | Stmt::Savepoint(_)
            | Stmt::Release(_) => None,
        }
    }
}

impl SelectStmt {
    /// Returns true if this SELECT or any nested subquery has a potentially
    /// non-deterministic LIMIT result set.
    ///
    /// This includes:
    /// - `LIMIT` without `ORDER BY`
    /// - `LIMIT` with `ORDER BY` terms that are all constant expressions
    ///   (for example `ORDER BY ZEROBLOB(10)`), which leaves row ordering undefined
    ///   among ties.
    pub fn has_unordered_limit(&self) -> bool {
        self.unordered_limit_reason().is_some()
    }

    /// Returns a reason code when this SELECT contains a potentially
    /// non-deterministic LIMIT query.
    pub fn unordered_limit_reason(&self) -> Option<&'static str> {
        // Check CTE inner queries
        if let Some(with) = &self.with_clause {
            for cte in &with.ctes {
                if let Some(reason) = cte.query.unordered_limit_reason() {
                    return Some(reason);
                }
            }
        }
        if self.limit.is_some() {
            if self.order_by.is_empty() {
                return Some("limit_without_order_by");
            }
            if self
                .order_by
                .iter()
                .any(|item| item.expr.contains_scalar_subquery())
            {
                return Some("limit_order_by_scalar_subquery");
            }
            if self
                .order_by
                .iter()
                .all(|item| !item.expr.contains_column_ref())
            {
                return Some("limit_constant_order_by");
            }
        }
        // Check subqueries in SELECT columns
        for col in &self.columns {
            if let Some(reason) = col.expr.unordered_limit_reason() {
                return Some(reason);
            }
        }
        // Check JOIN ON conditions
        for join in &self.joins {
            if let Some(JoinConstraint::On(expr)) = &join.constraint {
                if let Some(reason) = expr.unordered_limit_reason() {
                    return Some(reason);
                }
            }
        }
        // Check WHERE clause
        if let Some(w) = &self.where_clause {
            if let Some(reason) = w.unordered_limit_reason() {
                return Some(reason);
            }
        }
        // Check GROUP BY / HAVING
        if let Some(gb) = &self.group_by {
            for expr in &gb.exprs {
                if let Some(reason) = expr.unordered_limit_reason() {
                    return Some(reason);
                }
            }
            if let Some(h) = &gb.having {
                if let Some(reason) = h.unordered_limit_reason() {
                    return Some(reason);
                }
            }
        }
        // Check ORDER BY expressions
        for item in &self.order_by {
            if let Some(reason) = item.expr.unordered_limit_reason() {
                return Some(reason);
            }
        }
        None
    }
}

impl Expr {
    /// Returns true if this expression contains any subquery with LIMIT but no ORDER BY.
    pub fn has_unordered_limit(&self) -> bool {
        self.unordered_limit_reason().is_some()
    }

    /// Returns a reason code when this expression contains a potentially
    /// non-deterministic LIMIT query in a nested subquery.
    pub fn unordered_limit_reason(&self) -> Option<&'static str> {
        match self {
            Expr::Subquery(s) => s.unordered_limit_reason(),
            Expr::InSubquery(i) => i
                .expr
                .unordered_limit_reason()
                .or_else(|| i.subquery.unordered_limit_reason()),
            Expr::Exists(e) => e.subquery.unordered_limit_reason(),
            Expr::BinaryOp(b) => b
                .left
                .unordered_limit_reason()
                .or_else(|| b.right.unordered_limit_reason()),
            Expr::UnaryOp(u) => u.operand.unordered_limit_reason(),
            Expr::FunctionCall(fc) => fc.args.iter().find_map(|a| a.unordered_limit_reason()),
            Expr::Case(c) => c
                .operand
                .as_ref()
                .and_then(|o| o.unordered_limit_reason())
                .or_else(|| {
                    c.when_clauses.iter().find_map(|(w, t)| {
                        w.unordered_limit_reason()
                            .or_else(|| t.unordered_limit_reason())
                    })
                })
                .or_else(|| {
                    c.else_clause
                        .as_ref()
                        .and_then(|e| e.unordered_limit_reason())
                }),
            Expr::Cast(c) => c.expr.unordered_limit_reason(),
            Expr::Between(b) => b
                .expr
                .unordered_limit_reason()
                .or_else(|| b.low.unordered_limit_reason())
                .or_else(|| b.high.unordered_limit_reason()),
            Expr::InList(i) => i
                .expr
                .unordered_limit_reason()
                .or_else(|| i.list.iter().find_map(|e| e.unordered_limit_reason())),
            Expr::IsNull(i) => i.expr.unordered_limit_reason(),
            Expr::Parenthesized(e) => e.unordered_limit_reason(),
            Expr::ColumnRef(_) | Expr::Literal(_) => None,
            // Stubs: never instantiated
            Expr::WindowFunction(_) | Expr::Collate(_) | Expr::Raise(_) => None,
        }
    }
}

impl Expr {
    /// Returns true if this expression contains an aggregate function call
    /// (COUNT, SUM, AVG, TOTAL, GROUP_CONCAT, MIN, MAX) at any nesting level,
    /// excluding independent subqueries (which have their own scope).
    pub fn contains_aggregate(&self) -> bool {
        match self {
            Expr::FunctionCall(fc) => {
                let upper = fc.name.to_uppercase();
                if AGGREGATE_FUNCTIONS
                    .iter()
                    .any(|func| func.name == upper.as_str())
                {
                    return true;
                }
                fc.args.iter().any(|a| a.contains_aggregate())
            }
            Expr::BinaryOp(b) => b.left.contains_aggregate() || b.right.contains_aggregate(),
            Expr::UnaryOp(u) => u.operand.contains_aggregate(),
            Expr::Cast(c) => c.expr.contains_aggregate(),
            Expr::Between(b) => {
                b.expr.contains_aggregate()
                    || b.low.contains_aggregate()
                    || b.high.contains_aggregate()
            }
            Expr::InList(i) => {
                i.expr.contains_aggregate() || i.list.iter().any(|e| e.contains_aggregate())
            }
            Expr::IsNull(i) => i.expr.contains_aggregate(),
            Expr::Parenthesized(e) => e.contains_aggregate(),
            Expr::Case(c) => {
                c.operand.as_ref().is_some_and(|o| o.contains_aggregate())
                    || c.when_clauses
                        .iter()
                        .any(|(w, t)| w.contains_aggregate() || t.contains_aggregate())
                    || c.else_clause
                        .as_ref()
                        .is_some_and(|e| e.contains_aggregate())
            }
            // Subqueries have their own scope — aggregates inside them don't
            // affect the outer query's aggregate/non-aggregate classification.
            Expr::Subquery(_) | Expr::InSubquery(_) | Expr::Exists(_) => false,
            Expr::ColumnRef(_) | Expr::Literal(_) => false,
            // Stubs: never instantiated
            Expr::WindowFunction(_) | Expr::Collate(_) | Expr::Raise(_) => false,
        }
    }

    /// Returns true if this expression references a column (at any depth,
    /// excluding independent subqueries).
    pub fn contains_column_ref(&self) -> bool {
        match self {
            Expr::ColumnRef(_) => true,
            Expr::FunctionCall(fc) => fc.args.iter().any(|a| a.contains_column_ref()),
            Expr::BinaryOp(b) => b.left.contains_column_ref() || b.right.contains_column_ref(),
            Expr::UnaryOp(u) => u.operand.contains_column_ref(),
            Expr::Cast(c) => c.expr.contains_column_ref(),
            Expr::Between(b) => {
                b.expr.contains_column_ref()
                    || b.low.contains_column_ref()
                    || b.high.contains_column_ref()
            }
            Expr::InList(i) => {
                i.expr.contains_column_ref() || i.list.iter().any(|e| e.contains_column_ref())
            }
            Expr::IsNull(i) => i.expr.contains_column_ref(),
            Expr::Parenthesized(e) => e.contains_column_ref(),
            Expr::Case(c) => {
                c.operand.as_ref().is_some_and(|o| o.contains_column_ref())
                    || c.when_clauses
                        .iter()
                        .any(|(w, t)| w.contains_column_ref() || t.contains_column_ref())
                    || c.else_clause
                        .as_ref()
                        .is_some_and(|e| e.contains_column_ref())
            }
            Expr::Subquery(_) | Expr::InSubquery(_) | Expr::Exists(_) => false,
            Expr::Literal(_) => false,
            // Stubs: never instantiated
            Expr::WindowFunction(_) | Expr::Collate(_) | Expr::Raise(_) => false,
        }
    }

    /// Returns true if this expression tree contains a scalar subquery node.
    ///
    /// Note that this intentionally excludes EXISTS/IN-subquery forms.
    pub fn contains_scalar_subquery(&self) -> bool {
        match self {
            Expr::Subquery(_) => true,
            Expr::FunctionCall(fc) => fc.args.iter().any(|a| a.contains_scalar_subquery()),
            Expr::BinaryOp(b) => {
                b.left.contains_scalar_subquery() || b.right.contains_scalar_subquery()
            }
            Expr::UnaryOp(u) => u.operand.contains_scalar_subquery(),
            Expr::Cast(c) => c.expr.contains_scalar_subquery(),
            Expr::Between(b) => {
                b.expr.contains_scalar_subquery()
                    || b.low.contains_scalar_subquery()
                    || b.high.contains_scalar_subquery()
            }
            Expr::InList(i) => {
                i.expr.contains_scalar_subquery()
                    || i.list.iter().any(|e| e.contains_scalar_subquery())
            }
            Expr::IsNull(i) => i.expr.contains_scalar_subquery(),
            Expr::Parenthesized(e) => e.contains_scalar_subquery(),
            Expr::Case(c) => {
                c.operand
                    .as_ref()
                    .is_some_and(|o| o.contains_scalar_subquery())
                    || c.when_clauses
                        .iter()
                        .any(|(w, t)| w.contains_scalar_subquery() || t.contains_scalar_subquery())
                    || c.else_clause
                        .as_ref()
                        .is_some_and(|e| e.contains_scalar_subquery())
            }
            Expr::ColumnRef(_) | Expr::Literal(_) | Expr::InSubquery(_) | Expr::Exists(_) => false,
            // Stubs: never instantiated
            Expr::WindowFunction(_) | Expr::Collate(_) | Expr::Raise(_) => false,
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
                | StmtKind::CreateView
                | StmtKind::DropView
        )
    }
}

// =============================================================================
// CTE (Common Table Expression) types
// =============================================================================

/// Materialization hint for a CTE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CteMaterialization {
    /// No hint (default behavior).
    Default,
    /// Force materialization.
    Materialized,
    /// Suggest against materialization.
    NotMaterialized,
}

impl fmt::Display for CteMaterialization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CteMaterialization::Default => Ok(()),
            CteMaterialization::Materialized => write!(f, "MATERIALIZED "),
            CteMaterialization::NotMaterialized => write!(f, "NOT MATERIALIZED "),
        }
    }
}

/// A single CTE definition: `name[(col1, col2)] AS [MATERIALIZED ](SELECT ...)`.
#[derive(Debug, Clone)]
pub struct CteDefinition {
    pub name: String,
    pub column_aliases: Vec<String>,
    pub materialization: CteMaterialization,
    pub query: SelectStmt,
}

impl fmt::Display for CteDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if !self.column_aliases.is_empty() {
            write!(f, "(")?;
            for (i, alias) in self.column_aliases.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{alias}")?;
            }
            write!(f, ")")?;
        }
        write!(f, " AS {}({})", self.materialization, self.query)
    }
}

/// A WITH clause containing one or more CTEs.
#[derive(Debug, Clone)]
pub struct WithClause {
    pub ctes: Vec<CteDefinition>,
}

impl fmt::Display for WithClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WITH ")?;
        for (i, cte) in self.ctes.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{cte}")?;
        }
        Ok(())
    }
}

/// The FROM clause of a SELECT statement.
#[derive(Debug, Clone)]
pub struct FromClause {
    pub table: String,
    pub alias: Option<String>,
}

/// A JOIN clause.
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub alias: Option<String>,
    pub constraint: Option<JoinConstraint>,
}

/// The type of JOIN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Cross,
    Natural,
}

/// A JOIN constraint (ON condition).
#[derive(Debug, Clone)]
pub enum JoinConstraint {
    On(Expr),
}

/// A SELECT statement.
#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub with_clause: Option<WithClause>,
    pub distinct: bool,
    pub columns: Vec<SelectColumn>,
    /// The FROM clause. None for table-less SELECTs (e.g. `SELECT 1+2`).
    pub from: Option<FromClause>,
    pub joins: Vec<JoinClause>,
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
        if let Some(with) = &self.with_clause {
            write!(f, "{with} ")?;
        }
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
            write!(f, " FROM {}", from.table)?;
            if let Some(alias) = &from.alias {
                write!(f, " AS {alias}")?;
            }
        }

        for join in &self.joins {
            match join.join_type {
                JoinType::Inner => write!(f, " JOIN {}", join.table)?,
                JoinType::Left => write!(f, " LEFT JOIN {}", join.table)?,
                JoinType::Cross => write!(f, " CROSS JOIN {}", join.table)?,
                JoinType::Natural => write!(f, " NATURAL JOIN {}", join.table)?,
            }
            if let Some(alias) = &join.alias {
                write!(f, " AS {alias}")?;
            }
            if let Some(JoinConstraint::On(expr)) = &join.constraint {
                write!(f, " ON {expr}")?;
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

/// Conflict resolution clause for INSERT/UPDATE (OR ABORT/FAIL/IGNORE/REPLACE/ROLLBACK).
#[derive(Debug, Clone, Copy)]
pub enum ConflictClause {
    Abort,
    Fail,
    Ignore,
    Replace,
    Rollback,
}

impl fmt::Display for ConflictClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConflictClause::Abort => write!(f, " OR ABORT"),
            ConflictClause::Fail => write!(f, " OR FAIL"),
            ConflictClause::Ignore => write!(f, " OR IGNORE"),
            ConflictClause::Replace => write!(f, " OR REPLACE"),
            ConflictClause::Rollback => write!(f, " OR ROLLBACK"),
        }
    }
}

/// An INSERT statement.
#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub with_clause: Option<WithClause>,
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
    pub conflict: Option<ConflictClause>,
}

impl fmt::Display for InsertStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(with) = &self.with_clause {
            write!(f, "{with} ")?;
        }
        write!(f, "INSERT")?;
        if let Some(conflict) = &self.conflict {
            write!(f, "{conflict}")?;
        }
        write!(f, " INTO {}", self.table)?;

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
    pub with_clause: Option<WithClause>,
    pub table: String,
    pub sets: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
    pub conflict: Option<ConflictClause>,
}

impl fmt::Display for UpdateStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(with) = &self.with_clause {
            write!(f, "{with} ")?;
        }
        write!(f, "UPDATE")?;
        if let Some(conflict) = &self.conflict {
            write!(f, "{conflict}")?;
        }
        write!(f, " {} SET ", self.table)?;

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
    pub with_clause: Option<WithClause>,
    pub table: String,
    pub where_clause: Option<Expr>,
}

impl fmt::Display for DeleteStmt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(with) = &self.with_clause {
            write!(f, "{with} ")?;
        }
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
    pub strict: bool,
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

        write!(f, ")")?;

        if self.strict {
            write!(f, " STRICT")?;
        }

        Ok(())
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
    pub check: Option<Expr>,
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

        if let Some(check) = &self.check {
            write!(f, " CHECK ({check})")?;
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
    // Stubs: not yet generated (weight 0), shown in coverage report
    /// expr OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)
    WindowFunction(Box<WindowFunctionExpr>),
    /// expr COLLATE collation_name
    Collate(Box<CollateExpr>),
    /// RAISE(ABORT|FAIL|IGNORE|ROLLBACK, error_message) — triggers only
    Raise(Box<RaiseExpr>),
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
            // Stubs
            Expr::WindowFunction(_) => todo!("window function generation"),
            Expr::Collate(_) => todo!("COLLATE expression generation"),
            Expr::Raise(_) => todo!("RAISE expression generation"),
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

    /// Create a window function expression (stub — records to context).
    pub fn window_function(ctx: &mut Context) -> Self {
        ctx.record(ExprKind::WindowFunction);
        todo!("window function generation")
    }

    /// Create a COLLATE expression (stub — records to context).
    pub fn collate(ctx: &mut Context) -> Self {
        ctx.record(ExprKind::Collate);
        todo!("COLLATE expression generation")
    }

    /// Create a RAISE expression (stub — records to context).
    pub fn raise(ctx: &mut Context) -> Self {
        ctx.record(ExprKind::Raise);
        todo!("RAISE expression generation")
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
    // Bitwise (stubs — weight 0)
    BitAnd,
    BitOr,
    LeftShift,
    RightShift,
    // Null-safe comparison (stubs — weight 0)
    Is,
    IsNot,
    // Pattern matching (stub — weight 0)
    Regexp,
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
            // Stubs
            BinOp::BitAnd => write!(f, "&"),
            BinOp::BitOr => write!(f, "|"),
            BinOp::LeftShift => write!(f, "<<"),
            BinOp::RightShift => write!(f, ">>"),
            BinOp::Is => write!(f, "IS"),
            BinOp::IsNot => write!(f, "IS NOT"),
            BinOp::Regexp => write!(f, "REGEXP"),
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

    /// Returns bitwise operators.
    pub fn bitwise() -> &'static [BinOp] {
        &[
            BinOp::BitAnd,
            BinOp::BitOr,
            BinOp::LeftShift,
            BinOp::RightShift,
        ]
    }

    /// Returns true if this is a comparison operator.
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            BinOp::Eq
                | BinOp::Ne
                | BinOp::Lt
                | BinOp::Le
                | BinOp::Gt
                | BinOp::Ge
                | BinOp::Is
                | BinOp::IsNot
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

// =============================================================================
// Stub expression types (not yet generated)
// =============================================================================

/// A window function expression (stub).
#[derive(Debug, Clone)]
pub struct WindowFunctionExpr;

/// A COLLATE expression (stub).
#[derive(Debug, Clone)]
pub struct CollateExpr;

/// A RAISE expression for triggers (stub).
#[derive(Debug, Clone)]
pub struct RaiseExpr;

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
            with_clause: None,
            distinct: false,
            columns: vec![SelectColumn {
                expr: Expr::ColumnRef(ColumnRef {
                    table: None,
                    column: "name".to_string(),
                }),
                alias: None,
            }],
            from: Some(FromClause {
                table: "users".to_string(),
                alias: None,
            }),
            joins: vec![],
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
            with_clause: None,
            distinct: true,
            columns: vec![SelectColumn {
                expr: Expr::ColumnRef(ColumnRef {
                    table: None,
                    column: "name".to_string(),
                }),
                alias: None,
            }],
            from: Some(FromClause {
                table: "users".to_string(),
                alias: None,
            }),
            joins: vec![],
            where_clause: None,
            group_by: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        assert_eq!(select.to_string(), "SELECT DISTINCT name FROM users");
    }

    #[test]
    fn test_select_with_join_display() {
        let select = SelectStmt {
            with_clause: None,
            distinct: false,
            columns: vec![],
            from: Some(FromClause {
                table: "users".to_string(),
                alias: Some("u".to_string()),
            }),
            joins: vec![JoinClause {
                join_type: JoinType::Inner,
                table: "orders".to_string(),
                alias: Some("o".to_string()),
                constraint: Some(JoinConstraint::On(Expr::ColumnRef(ColumnRef {
                    table: Some("u".to_string()),
                    column: "id".to_string(),
                }))),
            }],
            where_clause: None,
            group_by: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        assert_eq!(
            select.to_string(),
            "SELECT * FROM users AS u JOIN orders AS o ON u.id"
        );
    }

    #[test]
    fn test_insert_display() {
        let mut ctx = Context::new_with_seed(42);
        let insert = InsertStmt {
            with_clause: None,
            table: "users".to_string(),
            columns: vec!["name".to_string(), "age".to_string()],
            values: vec![vec![
                Expr::literal(&mut ctx, Literal::Text("Alice".to_string())),
                Expr::literal(&mut ctx, Literal::Integer(30)),
            ]],
            conflict: None,
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

    #[test]
    fn test_has_unordered_limit_for_constant_order_by() {
        let select = SelectStmt {
            with_clause: None,
            distinct: false,
            columns: vec![SelectColumn {
                expr: Expr::Literal(Literal::Integer(1)),
                alias: None,
            }],
            from: Some(FromClause {
                table: "t".to_string(),
                alias: None,
            }),
            joins: vec![],
            where_clause: None,
            group_by: None,
            order_by: vec![OrderByItem {
                expr: Expr::Literal(Literal::Integer(1)),
                direction: OrderDirection::Asc,
                nulls: None,
            }],
            limit: Some(1),
            offset: None,
        };

        assert!(select.has_unordered_limit());
    }

    #[test]
    fn test_has_unordered_limit_false_for_column_order_by() {
        let select = SelectStmt {
            with_clause: None,
            distinct: false,
            columns: vec![SelectColumn {
                expr: Expr::ColumnRef(ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                alias: None,
            }],
            from: Some(FromClause {
                table: "t".to_string(),
                alias: None,
            }),
            joins: vec![],
            where_clause: None,
            group_by: None,
            order_by: vec![OrderByItem {
                expr: Expr::ColumnRef(ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                direction: OrderDirection::Asc,
                nulls: None,
            }],
            limit: Some(1),
            offset: None,
        };

        assert!(!select.has_unordered_limit());
    }

    #[test]
    fn test_unordered_limit_reason_for_scalar_subquery_order_by() {
        let select = SelectStmt {
            with_clause: None,
            distinct: false,
            columns: vec![SelectColumn {
                expr: Expr::ColumnRef(ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                alias: None,
            }],
            from: Some(FromClause {
                table: "t".to_string(),
                alias: None,
            }),
            joins: vec![],
            where_clause: None,
            group_by: None,
            order_by: vec![OrderByItem {
                expr: Expr::BinaryOp(Box::new(BinaryOpExpr {
                    left: Expr::ColumnRef(ColumnRef {
                        table: None,
                        column: "id".to_string(),
                    }),
                    op: BinOp::Or,
                    right: Expr::Subquery(Box::new(SelectStmt {
                        with_clause: None,
                        distinct: false,
                        columns: vec![SelectColumn {
                            expr: Expr::Literal(Literal::Integer(1)),
                            alias: None,
                        }],
                        from: None,
                        joins: vec![],
                        where_clause: None,
                        group_by: None,
                        order_by: vec![],
                        limit: Some(1),
                        offset: None,
                    })),
                })),
                direction: OrderDirection::Desc,
                nulls: None,
            }],
            limit: Some(1),
            offset: None,
        };

        assert!(select.has_unordered_limit());
        assert_eq!(
            select.unordered_limit_reason(),
            Some("limit_order_by_scalar_subquery")
        );
    }

    #[test]
    fn test_unordered_limit_reason_for_constant_order_by() {
        let select = SelectStmt {
            with_clause: None,
            distinct: false,
            columns: vec![SelectColumn {
                expr: Expr::ColumnRef(ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                alias: None,
            }],
            from: Some(FromClause {
                table: "t".to_string(),
                alias: None,
            }),
            joins: vec![],
            where_clause: None,
            group_by: None,
            order_by: vec![OrderByItem {
                expr: Expr::Literal(Literal::Integer(1)),
                direction: OrderDirection::Asc,
                nulls: None,
            }],
            limit: Some(1),
            offset: None,
        };

        assert_eq!(
            select.unordered_limit_reason(),
            Some("limit_constant_order_by")
        );
    }

    #[test]
    fn test_cte_display() {
        let cte = CteDefinition {
            name: "cte_0".to_string(),
            column_aliases: vec![],
            materialization: CteMaterialization::Default,
            query: SelectStmt {
                with_clause: None,
                distinct: false,
                columns: vec![SelectColumn {
                    expr: Expr::Literal(Literal::Integer(1)),
                    alias: None,
                }],
                from: None,
                joins: vec![],
                where_clause: None,
                group_by: None,
                order_by: vec![],
                limit: None,
                offset: None,
            },
        };

        assert_eq!(cte.to_string(), "cte_0 AS (SELECT 1)");
    }

    #[test]
    fn test_with_clause_display() {
        let wc = WithClause {
            ctes: vec![
                CteDefinition {
                    name: "cte_0".to_string(),
                    column_aliases: vec![],
                    materialization: CteMaterialization::Default,
                    query: SelectStmt {
                        with_clause: None,
                        distinct: false,
                        columns: vec![SelectColumn {
                            expr: Expr::Literal(Literal::Integer(1)),
                            alias: Some("x".to_string()),
                        }],
                        from: None,
                        joins: vec![],
                        where_clause: None,
                        group_by: None,
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    },
                },
                CteDefinition {
                    name: "cte_1".to_string(),
                    column_aliases: vec!["a".to_string(), "b".to_string()],
                    materialization: CteMaterialization::Default,
                    query: SelectStmt {
                        with_clause: None,
                        distinct: false,
                        columns: vec![SelectColumn {
                            expr: Expr::Literal(Literal::Integer(2)),
                            alias: None,
                        }],
                        from: None,
                        joins: vec![],
                        where_clause: None,
                        group_by: None,
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    },
                },
            ],
        };

        assert_eq!(
            wc.to_string(),
            "WITH cte_0 AS (SELECT 1 AS x), cte_1(a, b) AS (SELECT 2)"
        );
    }

    #[test]
    fn test_select_with_cte_display() {
        let select = SelectStmt {
            with_clause: Some(WithClause {
                ctes: vec![CteDefinition {
                    name: "cte_0".to_string(),
                    column_aliases: vec![],
                    materialization: CteMaterialization::Default,
                    query: SelectStmt {
                        with_clause: None,
                        distinct: false,
                        columns: vec![SelectColumn {
                            expr: Expr::Literal(Literal::Integer(1)),
                            alias: Some("x".to_string()),
                        }],
                        from: None,
                        joins: vec![],
                        where_clause: None,
                        group_by: None,
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    },
                }],
            }),
            distinct: false,
            columns: vec![],
            from: Some(FromClause {
                table: "cte_0".to_string(),
                alias: None,
            }),
            joins: vec![],
            where_clause: None,
            group_by: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        assert_eq!(
            select.to_string(),
            "WITH cte_0 AS (SELECT 1 AS x) SELECT * FROM cte_0"
        );
    }

    #[test]
    fn test_insert_with_cte_display() {
        let mut ctx = Context::new_with_seed(42);
        let insert = InsertStmt {
            with_clause: Some(WithClause {
                ctes: vec![CteDefinition {
                    name: "cte_0".to_string(),
                    column_aliases: vec![],
                    materialization: CteMaterialization::Default,
                    query: SelectStmt {
                        with_clause: None,
                        distinct: false,
                        columns: vec![SelectColumn {
                            expr: Expr::Literal(Literal::Integer(1)),
                            alias: None,
                        }],
                        from: None,
                        joins: vec![],
                        where_clause: None,
                        group_by: None,
                        order_by: vec![],
                        limit: None,
                        offset: None,
                    },
                }],
            }),
            table: "users".to_string(),
            columns: vec!["id".to_string()],
            values: vec![vec![Expr::literal(&mut ctx, Literal::Integer(1))]],
            conflict: None,
        };

        let sql = insert.to_string();
        assert!(sql.starts_with("WITH cte_0 AS (SELECT 1) INSERT INTO users"));
    }

    #[test]
    fn test_materialization_display() {
        assert_eq!(CteMaterialization::Default.to_string(), "");
        assert_eq!(
            CteMaterialization::Materialized.to_string(),
            "MATERIALIZED "
        );
        assert_eq!(
            CteMaterialization::NotMaterialized.to_string(),
            "NOT MATERIALIZED "
        );
    }

    #[test]
    fn test_unordered_limit_in_cte_detected() {
        let select = SelectStmt {
            with_clause: Some(WithClause {
                ctes: vec![CteDefinition {
                    name: "cte_0".to_string(),
                    column_aliases: vec![],
                    materialization: CteMaterialization::Default,
                    query: SelectStmt {
                        with_clause: None,
                        distinct: false,
                        columns: vec![SelectColumn {
                            expr: Expr::Literal(Literal::Integer(1)),
                            alias: None,
                        }],
                        from: Some(FromClause {
                            table: "t".to_string(),
                            alias: None,
                        }),
                        joins: vec![],
                        where_clause: None,
                        group_by: None,
                        order_by: vec![],
                        limit: Some(5),
                        offset: None,
                    },
                }],
            }),
            distinct: false,
            columns: vec![],
            from: Some(FromClause {
                table: "cte_0".to_string(),
                alias: None,
            }),
            joins: vec![],
            where_clause: None,
            group_by: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };

        assert!(select.has_unordered_limit());
        assert_eq!(
            select.unordered_limit_reason(),
            Some("limit_without_order_by")
        );
    }
}
