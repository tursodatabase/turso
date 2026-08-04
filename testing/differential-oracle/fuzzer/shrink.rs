//! Statement minimization for oracle failures.
//!
//! A failing statement from the generator is often thousands of bytes of
//! nested expressions, most of which have nothing to do with the divergence.
//! This module rebuilds both engines from the failure-state dump, confirms the
//! divergence still reproduces there, and then repeatedly simplifies the
//! statement, keeping each edit only if the same kind of divergence still
//! occurs. The result is written next to the other run artifacts as
//! `minimized.sql`.
//!
//! Candidates come from the SQL parser, not from text manipulation: the
//! statement is parsed, one AST node is simplified — a clause dropped, an
//! expression replaced by `1` or `''` or one of its own children — and the
//! tree is printed back to SQL. Every candidate is therefore syntactically
//! valid, and edits cannot be confused by quotes or keywords inside literals.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use turso_core::{Database, SqliteDialect};
use turso_parser::ast::{
    Cmd, Expr, InsertBody, Literal, OneSelect, ResultColumn, Select, SelectTable, Stmt,
};
use turso_parser::parser::Parser;

use crate::memory::MemorySimIO;
use crate::oracle::{DifferentialOracle, QueryResult};

/// Upper bound on candidate executions per shrink, so a pathological
/// statement cannot stall a fuzzing loop.
const MAX_CANDIDATES: usize = 800;

/// How the two engines diverged on a statement. Shrinking only accepts an
/// edit if the candidate reproduces the same class (and, for errors, the same
/// error prefix), so the reduction cannot drift onto a different bug.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Divergence {
    /// Turso returned an error, SQLite did not. Holds an error prefix.
    TursoErr(String),
    /// SQLite returned an error, Turso did not. Holds an error prefix.
    SqliteErr(String),
    /// Both succeeded but returned different rows, or left the databases in
    /// different states.
    ResultMismatch,
}

/// Take a stable prefix of an error message for matching candidates against
/// the original failure. Long enough to pin the error kind ("integer
/// overflow", "UNIQUE constraint failed: ..."), short enough to tolerate
/// differing identifiers further into the message.
fn error_prefix(msg: &str) -> String {
    msg.chars().take(40).collect()
}

/// A fresh pair of engines with the failure state loaded.
pub struct EnginePair {
    turso: Arc<turso_core::Connection>,
    sqlite: rusqlite::Connection,
    /// Keeps the database (and its in-memory IO) alive for `turso`.
    _turso_db: Arc<Database>,
}

impl EnginePair {
    /// Build both engines and replay the state script into each. Individual
    /// statement errors during replay are ignored: the script was produced
    /// from a live database, so failures here would only come from replay
    /// artifacts, and the baseline check below decides whether the replayed
    /// state is good enough to shrink against.
    pub fn build(state_sql: &str) -> Result<Self> {
        let io = Arc::new(MemorySimIO::new(0));
        let opts = turso_core::DatabaseOpts::new().with_attach(true);
        let turso_db = Database::open_file_with_flags(
            io,
            "shrink.db",
            turso_core::OpenFlags::default(),
            opts,
            None,
            Arc::new(SqliteDialect),
        )?;
        let turso = turso_db.connect()?;
        let sqlite = rusqlite::Connection::open_in_memory()?;
        for stmt in state_sql.lines() {
            let stmt = stmt.trim();
            if stmt.is_empty() || stmt.starts_with("--") {
                continue;
            }
            let _ = DifferentialOracle::execute_turso(&turso, stmt);
            let _ = DifferentialOracle::execute_sqlite(&sqlite, stmt);
        }
        Ok(Self {
            turso,
            sqlite,
            _turso_db: turso_db,
        })
    }

    /// Run `sql` on both engines and return both raw results (turso, sqlite).
    pub fn run_both(&self, sql: &str) -> (QueryResult, QueryResult) {
        (
            DifferentialOracle::execute_turso(&self.turso, sql),
            DifferentialOracle::execute_sqlite(&self.sqlite, sql),
        )
    }

    /// Run `sql` on both engines and classify the outcome. `None` means the
    /// engines agreed (no divergence).
    pub fn classify(&self, sql: &str) -> Option<Divergence> {
        let t = DifferentialOracle::execute_turso(&self.turso, sql);
        let s = DifferentialOracle::execute_sqlite(&self.sqlite, sql);
        match (&t, &s) {
            // Both rejected the candidate; not a divergence.
            (QueryResult::Error(_), QueryResult::Error(_)) => None,
            (QueryResult::Error(te), _) => Some(Divergence::TursoErr(error_prefix(te))),
            (_, QueryResult::Error(se)) => Some(Divergence::SqliteErr(error_prefix(se))),
            _ => {
                if query_results_differ(&t, &s) || self.states_differ() {
                    Some(Divergence::ResultMismatch)
                } else {
                    None
                }
            }
        }
    }

    /// Compare the full contents of every table on both engines.
    pub fn states_differ(&self) -> bool {
        for db in ["main", "temp", "aux"] {
            let master = if db == "main" {
                "sqlite_master".to_string()
            } else {
                format!("{db}.sqlite_master")
            };
            let names_sql = format!("SELECT name FROM {master} WHERE type='table' ORDER BY name");
            let turso_names = DifferentialOracle::execute_turso(&self.turso, &names_sql);
            let sqlite_names = DifferentialOracle::execute_sqlite(&self.sqlite, &names_sql);
            if query_results_differ(&turso_names, &sqlite_names) {
                return true;
            }
            let QueryResult::Rows(names) = sqlite_names else {
                continue;
            };
            for row in &names {
                let Some(sql_gen_prop::SqlValue::Text(name)) = row.0.first() else {
                    continue;
                };
                let table_sql = format!("SELECT * FROM {db}.{name} ORDER BY rowid");
                let t = DifferentialOracle::execute_turso(&self.turso, &table_sql);
                let s = DifferentialOracle::execute_sqlite(&self.sqlite, &table_sql);
                if query_results_differ(&t, &s) {
                    return true;
                }
            }
        }
        false
    }
}

pub fn query_results_differ(a: &QueryResult, b: &QueryResult) -> bool {
    match (a, b) {
        (QueryResult::Rows(ra), QueryResult::Rows(rb)) => {
            !sql_gen_prop::result::diff_results(ra, rb).is_empty()
        }
        (QueryResult::Ok, QueryResult::Ok) => false,
        (QueryResult::Error(_), QueryResult::Error(_)) => false,
        // Ok vs empty Rows means the same thing here: no differing rows.
        (QueryResult::Ok, QueryResult::Rows(r)) | (QueryResult::Rows(r), QueryResult::Ok) => {
            !r.is_empty()
        }
        _ => true,
    }
}

/// `true` if `candidate` reproduces the same kind of divergence as `original`.
fn matches_divergence(original: &Divergence, candidate: Option<Divergence>) -> bool {
    match (original, candidate) {
        (Divergence::TursoErr(p), Some(Divergence::TursoErr(q))) => p == &q,
        (Divergence::SqliteErr(p), Some(Divergence::SqliteErr(q))) => p == &q,
        (Divergence::ResultMismatch, Some(Divergence::ResultMismatch)) => true,
        _ => false,
    }
}

// --- candidate generation ------------------------------------------------

fn parse_one(sql: &str) -> Option<Stmt> {
    let mut parser = Parser::new(sql.as_bytes());
    match parser.next()?.ok()? {
        Cmd::Stmt(stmt) => Some(stmt),
        _ => None,
    }
}

/// Ways to simplify one expression node.
#[derive(Clone, Copy)]
enum ExprAction {
    /// Replace the node with the literal 1.
    One,
    /// Replace the node with the literal ''.
    EmptyText,
    /// Replace the node with its n-th expression child (`x AND y` -> `x`,
    /// `RTRIM(a)` -> `a`).
    Child(usize),
    /// Remove the n-th element of the node's own list (a function argument or
    /// an IN-list value), keeping the node itself.
    DropListItem(usize),
}

/// Direct expression children that can stand in for the whole node.
fn expr_children(e: &mut Expr) -> Vec<&mut Expr> {
    match e {
        Expr::Between {
            lhs, start, end, ..
        } => vec![lhs, start, end],
        Expr::Binary(l, _, r) => vec![l, r],
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            let mut v: Vec<&mut Expr> = Vec::new();
            if let Some(b) = base {
                v.push(b);
            }
            for (w, t) in when_then_pairs {
                v.push(w);
                v.push(t);
            }
            if let Some(x) = else_expr {
                v.push(x);
            }
            v
        }
        Expr::Cast { expr, .. }
        | Expr::Collate(expr, _)
        | Expr::IsNull(expr)
        | Expr::NotNull(expr)
        | Expr::Unary(_, expr)
        | Expr::FieldAccess { base: expr, .. } => vec![expr],
        Expr::FunctionCall { args, .. } => args.iter_mut().map(|a| &mut **a).collect(),
        Expr::InList { lhs, rhs, .. } => {
            let mut v: Vec<&mut Expr> = vec![lhs];
            v.extend(rhs.iter_mut().map(|a| &mut **a));
            v
        }
        Expr::InSelect { lhs, .. } => vec![lhs],
        Expr::InTable { lhs, args, .. } => {
            let mut v: Vec<&mut Expr> = vec![lhs];
            v.extend(args.iter_mut().map(|a| &mut **a));
            v
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            let mut v: Vec<&mut Expr> = vec![lhs, rhs];
            if let Some(esc) = escape {
                v.push(esc);
            }
            v
        }
        Expr::Parenthesized(exprs) => exprs.iter_mut().map(|a| &mut **a).collect(),
        Expr::Raise(_, Some(expr)) => vec![expr],
        Expr::Subscript { base, index } => vec![base, index],
        Expr::Array { elements } => elements.iter_mut().map(|a| &mut **a).collect(),
        _ => vec![],
    }
}

/// Actions worth trying on this node, cheapest-to-verify structural wins
/// first. Skips no-ops like replacing `1` with `1`.
fn expr_actions(e: &Expr) -> Vec<ExprAction> {
    let mut actions = Vec::new();
    let child_count = match e {
        Expr::Between { .. } => 3,
        Expr::Binary(..) | Expr::Subscript { .. } => 2,
        Expr::Case { .. } => 0, // covered by One; branches vary in count
        Expr::Cast { .. }
        | Expr::Collate(..)
        | Expr::IsNull(..)
        | Expr::NotNull(..)
        | Expr::Unary(..)
        | Expr::FieldAccess { .. } => 1,
        Expr::FunctionCall { args, .. } => args.len().min(1),
        Expr::InList { .. } | Expr::InSelect { .. } | Expr::InTable { .. } => 1,
        Expr::Like { .. } => 2,
        Expr::Parenthesized(exprs) => exprs.len().min(1),
        _ => 0,
    };
    for i in 0..child_count {
        actions.push(ExprAction::Child(i));
    }
    match e {
        Expr::FunctionCall { args, .. } if args.len() > 1 => {
            for i in 0..args.len() {
                actions.push(ExprAction::DropListItem(i));
            }
        }
        Expr::InList { rhs, .. } if rhs.len() > 1 => {
            for i in 0..rhs.len() {
                actions.push(ExprAction::DropListItem(i));
            }
        }
        _ => {}
    }
    if !matches!(e, Expr::Literal(Literal::Numeric(n)) if n == "1") {
        actions.push(ExprAction::One);
    }
    if !matches!(e, Expr::Literal(Literal::String(s)) if s == "''") {
        actions.push(ExprAction::EmptyText);
    }
    actions
}

fn apply_expr_action(e: &mut Expr, action: ExprAction) {
    match action {
        ExprAction::One => *e = Expr::Literal(Literal::Numeric("1".to_string())),
        ExprAction::EmptyText => *e = Expr::Literal(Literal::String("''".to_string())),
        ExprAction::Child(i) => {
            let mut children = expr_children(e);
            if i < children.len() {
                let child = std::mem::take(children[i]);
                *e = child;
            }
        }
        ExprAction::DropListItem(i) => match e {
            Expr::FunctionCall { args, .. } if i < args.len() && args.len() > 1 => {
                args.remove(i);
            }
            Expr::InList { rhs, .. } if i < rhs.len() && rhs.len() > 1 => {
                rhs.remove(i);
            }
            _ => {}
        },
    }
}

/// Clauses that can be dropped from one SELECT/UPDATE/DELETE site.
#[derive(Clone, Copy)]
enum ClauseDrop {
    Where,
    GroupBy,
    Having,
    Distinct,
    SelectColumn(usize),
    OrderByItem(usize),
    Limit,
    Compound(usize),
    Cte(usize),
    Join(usize),
    SetItem(usize),
    ReturningItem(usize),
}

/// A place where clause-level edits apply.
enum Site<'a> {
    Core(&'a mut OneSelect),
    Outer(&'a mut Select),
    Update(&'a mut turso_parser::ast::Update),
    Delete {
        where_clause: &'a mut Option<Box<Expr>>,
        returning: &'a mut Vec<ResultColumn>,
    },
    InsertReturning(&'a mut Vec<ResultColumn>),
}

fn site_actions(site: &Site<'_>) -> Vec<ClauseDrop> {
    let mut actions = Vec::new();
    match site {
        Site::Core(OneSelect::Select {
            distinctness,
            columns,
            where_clause,
            group_by,
            ..
        }) => {
            if where_clause.is_some() {
                actions.push(ClauseDrop::Where);
            }
            if let Some(gb) = group_by {
                actions.push(ClauseDrop::GroupBy);
                if gb.having.is_some() {
                    actions.push(ClauseDrop::Having);
                }
            }
            if distinctness.is_some() {
                actions.push(ClauseDrop::Distinct);
            }
            if columns.len() > 1 {
                for i in 0..columns.len() {
                    actions.push(ClauseDrop::SelectColumn(i));
                }
            }
        }
        Site::Core(OneSelect::Values(..)) => {}
        Site::Outer(select) => {
            for i in 0..select.order_by.len() {
                actions.push(ClauseDrop::OrderByItem(i));
            }
            if select.limit.is_some() {
                actions.push(ClauseDrop::Limit);
            }
            for i in 0..select.body.compounds.len() {
                actions.push(ClauseDrop::Compound(i));
            }
            if let Some(with) = &select.with {
                for i in 0..with.ctes.len() {
                    actions.push(ClauseDrop::Cte(i));
                }
            }
            if let OneSelect::Select {
                from: Some(from), ..
            } = &select.body.select
            {
                for i in 0..from.joins.len() {
                    actions.push(ClauseDrop::Join(i));
                }
            }
        }
        Site::Update(update) => {
            if update.where_clause.is_some() {
                actions.push(ClauseDrop::Where);
            }
            if update.sets.len() > 1 {
                for i in 0..update.sets.len() {
                    actions.push(ClauseDrop::SetItem(i));
                }
            }
            for i in 0..update.returning.len() {
                actions.push(ClauseDrop::ReturningItem(i));
            }
        }
        Site::Delete {
            where_clause,
            returning,
        } => {
            if where_clause.is_some() {
                actions.push(ClauseDrop::Where);
            }
            for i in 0..returning.len() {
                actions.push(ClauseDrop::ReturningItem(i));
            }
        }
        Site::InsertReturning(returning) => {
            for i in 0..returning.len() {
                actions.push(ClauseDrop::ReturningItem(i));
            }
        }
    }
    actions
}

fn apply_clause_action(site: &mut Site<'_>, action: ClauseDrop) {
    match (site, action) {
        (Site::Core(OneSelect::Select { where_clause, .. }), ClauseDrop::Where) => {
            *where_clause = None
        }
        (Site::Core(OneSelect::Select { group_by, .. }), ClauseDrop::GroupBy) => *group_by = None,
        (
            Site::Core(OneSelect::Select {
                group_by: Some(gb), ..
            }),
            ClauseDrop::Having,
        ) => gb.having = None,
        (Site::Core(OneSelect::Select { distinctness, .. }), ClauseDrop::Distinct) => {
            *distinctness = None
        }
        (Site::Core(OneSelect::Select { columns, .. }), ClauseDrop::SelectColumn(i)) => {
            if i < columns.len() && columns.len() > 1 {
                columns.remove(i);
            }
        }
        (Site::Outer(select), ClauseDrop::OrderByItem(i)) => {
            if i < select.order_by.len() {
                select.order_by.remove(i);
            }
        }
        (Site::Outer(select), ClauseDrop::Limit) => select.limit = None,
        (Site::Outer(select), ClauseDrop::Compound(i)) => {
            if i < select.body.compounds.len() {
                select.body.compounds.remove(i);
            }
        }
        (Site::Outer(select), ClauseDrop::Cte(i)) => {
            if let Some(with) = &mut select.with {
                if i < with.ctes.len() {
                    with.ctes.remove(i);
                    if with.ctes.is_empty() {
                        select.with = None;
                    }
                }
            }
        }
        (Site::Outer(select), ClauseDrop::Join(i)) => {
            if let OneSelect::Select {
                from: Some(from), ..
            } = &mut select.body.select
            {
                if i < from.joins.len() {
                    from.joins.remove(i);
                }
            }
        }
        (Site::Update(update), ClauseDrop::Where) => update.where_clause = None,
        (Site::Update(update), ClauseDrop::SetItem(i)) => {
            if i < update.sets.len() && update.sets.len() > 1 {
                update.sets.remove(i);
            }
        }
        (Site::Update(update), ClauseDrop::ReturningItem(i)) => {
            if i < update.returning.len() {
                update.returning.remove(i);
            }
        }
        (Site::Delete { where_clause, .. }, ClauseDrop::Where) => **where_clause = None,
        (Site::Delete { returning, .. }, ClauseDrop::ReturningItem(i))
        | (Site::InsertReturning(returning), ClauseDrop::ReturningItem(i)) => {
            if i < returning.len() {
                returning.remove(i);
            }
        }
        _ => {}
    }
}

/// Pre-order walk over every expression in the statement, including inside
/// subqueries and CTE bodies. `f` returning `true` stops the walk (used to
/// apply an edit at one position).
fn walk_exprs(stmt: &mut Stmt, f: &mut dyn FnMut(&mut Expr) -> bool) -> bool {
    match stmt {
        Stmt::Select(select) => walk_select(select, f),
        Stmt::Insert {
            body, returning, ..
        } => {
            if let InsertBody::Select(select, _) = body {
                if walk_select(select, f) {
                    return true;
                }
            }
            walk_result_columns(returning, f)
        }
        Stmt::Update(update) => {
            if let Some(with) = &mut update.with {
                for cte in &mut with.ctes {
                    if walk_select(&mut cte.select, f) {
                        return true;
                    }
                }
            }
            for set in &mut update.sets {
                if walk_expr(&mut set.expr, f) {
                    return true;
                }
            }
            if let Some(from) = &mut update.from {
                if walk_from(from, f) {
                    return true;
                }
            }
            if let Some(w) = &mut update.where_clause {
                if walk_expr(w, f) {
                    return true;
                }
            }
            if walk_result_columns(&mut update.returning, f) {
                return true;
            }
            for sc in &mut update.order_by {
                if walk_expr(&mut sc.expr, f) {
                    return true;
                }
            }
            walk_limit(&mut update.limit, f)
        }
        Stmt::Delete {
            with,
            where_clause,
            returning,
            order_by,
            limit,
            ..
        } => {
            if let Some(with) = with {
                for cte in &mut with.ctes {
                    if walk_select(&mut cte.select, f) {
                        return true;
                    }
                }
            }
            if let Some(w) = where_clause {
                if walk_expr(w, f) {
                    return true;
                }
            }
            if walk_result_columns(returning, f) {
                return true;
            }
            for sc in order_by {
                if walk_expr(&mut sc.expr, f) {
                    return true;
                }
            }
            walk_limit(limit, f)
        }
        _ => false,
    }
}

fn walk_limit(
    limit: &mut Option<turso_parser::ast::Limit>,
    f: &mut dyn FnMut(&mut Expr) -> bool,
) -> bool {
    if let Some(limit) = limit {
        if walk_expr(&mut limit.expr, f) {
            return true;
        }
        if let Some(offset) = &mut limit.offset {
            if walk_expr(offset, f) {
                return true;
            }
        }
    }
    false
}

fn walk_result_columns(columns: &mut [ResultColumn], f: &mut dyn FnMut(&mut Expr) -> bool) -> bool {
    for rc in columns {
        if let ResultColumn::Expr(expr, _) = rc {
            if walk_expr(expr, f) {
                return true;
            }
        }
    }
    false
}

fn walk_select(select: &mut Select, f: &mut dyn FnMut(&mut Expr) -> bool) -> bool {
    if let Some(with) = &mut select.with {
        for cte in &mut with.ctes {
            if walk_select(&mut cte.select, f) {
                return true;
            }
        }
    }
    if walk_one_select(&mut select.body.select, f) {
        return true;
    }
    for compound in &mut select.body.compounds {
        if walk_one_select(&mut compound.select, f) {
            return true;
        }
    }
    for sc in &mut select.order_by {
        if walk_expr(&mut sc.expr, f) {
            return true;
        }
    }
    walk_limit(&mut select.limit, f)
}

fn walk_one_select(one: &mut OneSelect, f: &mut dyn FnMut(&mut Expr) -> bool) -> bool {
    match one {
        OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            ..
        } => {
            if walk_result_columns(columns, f) {
                return true;
            }
            if let Some(from) = from {
                if walk_from(from, f) {
                    return true;
                }
            }
            if let Some(w) = where_clause {
                if walk_expr(w, f) {
                    return true;
                }
            }
            if let Some(gb) = group_by {
                for e in &mut gb.exprs {
                    if walk_expr(e, f) {
                        return true;
                    }
                }
                if let Some(h) = &mut gb.having {
                    if walk_expr(h, f) {
                        return true;
                    }
                }
            }
            false
        }
        OneSelect::Values(rows) => {
            for row in rows {
                for e in row {
                    if walk_expr(e, f) {
                        return true;
                    }
                }
            }
            false
        }
    }
}

fn walk_from(
    from: &mut turso_parser::ast::FromClause,
    f: &mut dyn FnMut(&mut Expr) -> bool,
) -> bool {
    if walk_select_table(&mut from.select, f) {
        return true;
    }
    for join in &mut from.joins {
        if walk_select_table(&mut join.table, f) {
            return true;
        }
        if let Some(turso_parser::ast::JoinConstraint::On(e)) = &mut join.constraint {
            if walk_expr(e, f) {
                return true;
            }
        }
    }
    false
}

fn walk_select_table(table: &mut SelectTable, f: &mut dyn FnMut(&mut Expr) -> bool) -> bool {
    match table {
        SelectTable::Table(..) => false,
        SelectTable::TableCall(_, args, _) => {
            for a in args {
                if walk_expr(a, f) {
                    return true;
                }
            }
            false
        }
        SelectTable::Select(select, _) => walk_select(select, f),
        SelectTable::Sub(from, _) => walk_from(from, f),
    }
}

fn walk_expr(e: &mut Expr, f: &mut dyn FnMut(&mut Expr) -> bool) -> bool {
    if f(e) {
        return true;
    }
    // Subquery-carrying variants recurse through the select walker; everything
    // else recurses through its expression children.
    match e {
        Expr::Exists(select) | Expr::Subquery(select) => return walk_select(select, f),
        Expr::InSelect { lhs, rhs, .. } => {
            if walk_expr(lhs, f) {
                return true;
            }
            return walk_select(rhs, f);
        }
        _ => {}
    }
    for child in expr_children(e) {
        if walk_expr(child, f) {
            return true;
        }
    }
    false
}

/// Walk every clause site. `f` returning `true` stops the walk.
fn walk_sites(stmt: &mut Stmt, f: &mut dyn FnMut(&mut Site<'_>) -> bool) -> bool {
    match stmt {
        Stmt::Select(select) => walk_select_sites(select, f),
        Stmt::Insert {
            body, returning, ..
        } => {
            if let InsertBody::Select(select, _) = body {
                if walk_select_sites(select, f) {
                    return true;
                }
            }
            f(&mut Site::InsertReturning(returning))
        }
        Stmt::Update(update) => {
            if let Some(with) = &mut update.with {
                for cte in &mut with.ctes {
                    if walk_select_sites(&mut cte.select, f) {
                        return true;
                    }
                }
            }
            if let Some(from) = &mut update.from {
                if walk_from_sites(from, f) {
                    return true;
                }
            }
            f(&mut Site::Update(update))
        }
        Stmt::Delete {
            with,
            where_clause,
            returning,
            ..
        } => {
            if let Some(with) = with {
                for cte in &mut with.ctes {
                    if walk_select_sites(&mut cte.select, f) {
                        return true;
                    }
                }
            }
            f(&mut Site::Delete {
                where_clause,
                returning,
            })
        }
        _ => false,
    }
}

fn walk_select_sites(select: &mut Select, f: &mut dyn FnMut(&mut Site<'_>) -> bool) -> bool {
    if let Some(with) = &mut select.with {
        for cte in &mut with.ctes {
            if walk_select_sites(&mut cte.select, f) {
                return true;
            }
        }
    }
    if f(&mut Site::Outer(select)) {
        return true;
    }
    if f(&mut Site::Core(&mut select.body.select)) {
        return true;
    }
    for compound in &mut select.body.compounds {
        if f(&mut Site::Core(&mut compound.select)) {
            return true;
        }
    }
    // Subquery sites inside FROM.
    if let OneSelect::Select {
        from: Some(from), ..
    } = &mut select.body.select
    {
        if walk_from_sites(from, f) {
            return true;
        }
    }
    false
}

fn walk_from_sites(
    from: &mut turso_parser::ast::FromClause,
    f: &mut dyn FnMut(&mut Site<'_>) -> bool,
) -> bool {
    if let SelectTable::Select(select, _) = &mut *from.select {
        if walk_select_sites(select, f) {
            return true;
        }
    }
    for join in &mut from.joins {
        if let SelectTable::Select(select, _) = &mut *join.table {
            if walk_select_sites(select, f) {
                return true;
            }
        }
    }
    false
}

/// All simplified renderings of `sql`, structural (clause) edits first, then
/// expression edits in pre-order so outer nodes are tried before inner ones.
fn candidates(sql: &str) -> Vec<String> {
    let Some(stmt) = parse_one(sql) else {
        return vec![];
    };
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    // Clause edits.
    let mut site_action_counts = Vec::new();
    {
        let mut probe = stmt.clone();
        walk_sites(&mut probe, &mut |site| {
            site_action_counts.push(site_actions(site).len());
            false
        });
    }
    for (site_idx, &count) in site_action_counts.iter().enumerate() {
        for action_idx in 0..count {
            let mut cand = stmt.clone();
            let mut pos = 0usize;
            walk_sites(&mut cand, &mut |site| {
                if pos == site_idx {
                    let actions = site_actions(site);
                    if let Some(&action) = actions.get(action_idx) {
                        apply_clause_action(site, action);
                    }
                    return true;
                }
                pos += 1;
                false
            });
            push_candidate(&cand, sql, &mut seen, &mut out);
        }
    }

    // Expression edits.
    let mut expr_action_counts = Vec::new();
    {
        let mut probe = stmt.clone();
        walk_exprs(&mut probe, &mut |e| {
            expr_action_counts.push(expr_actions(e).len());
            false
        });
    }
    for (expr_idx, &count) in expr_action_counts.iter().enumerate() {
        for action_idx in 0..count {
            let mut cand = stmt.clone();
            let mut pos = 0usize;
            walk_exprs(&mut cand, &mut |e| {
                if pos == expr_idx {
                    let actions = expr_actions(e);
                    if let Some(&action) = actions.get(action_idx) {
                        apply_expr_action(e, action);
                    }
                    return true;
                }
                pos += 1;
                false
            });
            push_candidate(&cand, sql, &mut seen, &mut out);
        }
    }
    out
}

/// Render a candidate and keep it when it is new and strictly shorter, which
/// also guarantees the shrink loop terminates.
fn push_candidate(cand: &Stmt, original: &str, seen: &mut HashSet<String>, out: &mut Vec<String>) {
    let rendered = cand.to_string();
    if rendered.len() < original.len() && seen.insert(rendered.clone()) {
        out.push(rendered);
    }
}

// --- driver -------------------------------------------------------------

/// Repeatedly apply the first candidate edit that `judge` accepts, until no
/// edit is accepted or the attempt budget runs out.
fn shrink_with(initial: &str, mut judge: impl FnMut(&str) -> Result<bool>) -> Result<String> {
    let mut current = initial.to_string();
    let mut attempts = 0usize;
    let mut progress = true;
    while progress && attempts < MAX_CANDIDATES {
        progress = false;
        for candidate in candidates(&current) {
            attempts += 1;
            if attempts >= MAX_CANDIDATES {
                break;
            }
            if judge(&candidate)? {
                current = candidate;
                progress = true;
                break;
            }
        }
    }
    tracing::info!(
        "Shrink finished: {} -> {} bytes in {attempts} attempts",
        initial.len(),
        current.len()
    );
    Ok(current)
}

/// Classic ddmin-style list reduction: repeatedly try dropping chunks of
/// lines from the state script, keeping a deletion when `judge` still accepts
/// the remaining script. Halves the chunk size down to single lines.
fn reduce_state_lines(
    state_sql: &str,
    mut judge: impl FnMut(&str) -> Result<bool>,
) -> Result<String> {
    let mut lines: Vec<&str> = state_sql
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.trim_start().starts_with("--"))
        .collect();
    let mut chunk = lines.len().div_ceil(2).max(1);
    let mut attempts = 0usize;
    loop {
        let mut i = 0;
        let mut deleted_any = false;
        while i < lines.len() && attempts < MAX_CANDIDATES {
            let end = (i + chunk).min(lines.len());
            let mut candidate: Vec<&str> = Vec::with_capacity(lines.len());
            candidate.extend_from_slice(&lines[..i]);
            candidate.extend_from_slice(&lines[end..]);
            attempts += 1;
            if judge(&candidate.join("\n"))? {
                lines = candidate;
                deleted_any = true;
                // Do not advance: the next chunk slid into position i.
            } else {
                i = end;
            }
        }
        if attempts >= MAX_CANDIDATES || (chunk == 1 && !deleted_any) {
            break;
        }
        chunk = (chunk / 2).max(1);
    }
    Ok(lines.join("\n"))
}

/// The minimized reproduction for an oracle failure.
pub struct Minimized {
    /// The reduced state script the divergence needs.
    pub state_sql: String,
    /// The reduced failing statement.
    pub statement: String,
}

/// Minimize `failing_sql` — and then the state script it needs — against the
/// state in `state_sql`, falling back to `history_sql` (the run's executed
/// statements) when the divergence needs the statement history rather than
/// just the final data. Returns None when neither replay reproduces it.
///
/// The fallback matters for history-dependent bugs: a wrong trigger firing
/// order, for example, leaves a table whose *contents* replay cleanly from a
/// dump, so the dump-based baseline shows no divergence — but replaying the
/// statements that built the table hits the divergence again. The ddmin pass
/// then deletes every history line the divergence does not need.
pub fn shrink_statement(
    state_sql: &str,
    history_sql: &str,
    failing_sql: &str,
) -> Result<Option<Minimized>> {
    let mut state_sql = state_sql;
    let mut baseline = {
        let pair = EnginePair::build(state_sql)?;
        pair.classify(failing_sql)
    };
    if baseline.is_none() {
        let pair = EnginePair::build(history_sql)?;
        baseline = pair.classify(failing_sql);
        if baseline.is_some() {
            tracing::info!(
                "Divergence needs the statement history; shrinking against it instead of the state dump"
            );
            state_sql = history_sql;
        }
    }
    let Some(original) = baseline else {
        tracing::info!(
            "Shrink skipped: divergence reproduces on neither the rebuilt state nor the statement history"
        );
        return Ok(None);
    };
    tracing::info!(
        "Shrinking {} byte statement ({original:?})",
        failing_sql.len()
    );

    let statement = shrink_with(failing_sql, |candidate| {
        // Fresh engines per attempt: a DML candidate that ran on both engines
        // would otherwise contaminate the next attempt's state.
        let pair = EnginePair::build(state_sql)?;
        Ok(matches_divergence(&original, pair.classify(candidate)))
    })?;

    // With the statement fixed, drop every state line it does not need.
    let state_sql = reduce_state_lines(state_sql, |candidate_state| {
        let pair = EnginePair::build(candidate_state)?;
        Ok(matches_divergence(&original, pair.classify(&statement)))
    })?;
    tracing::info!(
        "State script reduced to {} lines",
        state_sql.lines().count()
    );
    Ok(Some(Minimized {
        state_sql,
        statement,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Normalize SQL through the parser so tests compare renderings, not
    /// hand-written spacing.
    fn norm(sql: &str) -> String {
        parse_one(sql).expect("test SQL must parse").to_string()
    }

    #[test]
    fn function_calls_shrink_to_one_and_their_argument() {
        let results = candidates("SELECT ABS(x + 1) FROM t");
        assert!(results.contains(&norm("SELECT 1 FROM t")), "{results:?}");
        assert!(
            results.contains(&norm("SELECT x + 1 FROM t")),
            "{results:?}"
        );
    }

    #[test]
    fn nested_case_shrinks_as_a_unit() {
        let sql = "SELECT CASE WHEN CASE WHEN a THEN b END THEN c END FROM t";
        let results = candidates(sql);
        assert!(results.contains(&norm("SELECT 1 FROM t")), "{results:?}");
        assert!(
            results.contains(&norm("SELECT CASE WHEN 1 THEN c END FROM t")),
            "{results:?}"
        );
    }

    #[test]
    fn literals_inside_strings_do_not_confuse_edits() {
        // The string contains parens and would break a text-based scanner.
        let results = candidates("SELECT f('((', x) FROM t");
        assert!(results.contains(&norm("SELECT 1 FROM t")), "{results:?}");
        assert!(
            results.contains(&norm("SELECT f('', x) FROM t")),
            "{results:?}"
        );
    }

    #[test]
    fn and_operands_can_replace_the_conjunction() {
        let results = candidates("SELECT a FROM t WHERE x AND f(y) ORDER BY z");
        assert!(
            results.contains(&norm("SELECT a FROM t WHERE x ORDER BY z")),
            "{results:?}"
        );
        let results = candidates("SELECT a FROM t WHERE x OR y OR z");
        assert!(
            results.contains(&norm("SELECT a FROM t WHERE x OR z")),
            "{results:?}"
        );
    }

    #[test]
    fn clauses_and_list_items_can_be_dropped() {
        let results = candidates("SELECT a FROM t WHERE x ORDER BY a DESC, b ASC LIMIT 3");
        assert!(
            results.contains(&norm("SELECT a FROM t WHERE x ORDER BY a DESC LIMIT 3")),
            "{results:?}"
        );
        assert!(
            results.contains(&norm("SELECT a FROM t ORDER BY a DESC, b ASC LIMIT 3")),
            "{results:?}"
        );
        assert!(
            results.contains(&norm("SELECT a FROM t WHERE x ORDER BY a DESC, b ASC")),
            "{results:?}"
        );
        let results = candidates("SELECT MAX(a, b) FROM t");
        assert!(
            results.contains(&norm("SELECT MAX(a) FROM t")),
            "{results:?}"
        );
    }

    #[test]
    fn function_calls_also_shrink_to_empty_string() {
        let results = candidates("SELECT c <= RTRIM('ab') FROM t");
        assert!(
            results.contains(&norm("SELECT c <= '' FROM t")),
            "{results:?}"
        );
        assert!(
            results.contains(&norm("SELECT c <= 'ab' FROM t")),
            "{results:?}"
        );
    }

    #[test]
    fn shrink_loop_keeps_only_the_load_bearing_kernel() {
        // Synthetic judge: the "divergence" needs ABS( to survive. Everything
        // else — the CASE block, the other function call, the literals — must
        // be simplified away.
        let sql = "SELECT ABS(CASE WHEN LENGTH('hello world') THEN 123456 \
                   ELSE UPPER('junk') END), COALESCE(999999, X'DEADBEEF') FROM t";
        let out = shrink_with(sql, |cand| Ok(cand.contains("ABS"))).unwrap();
        assert!(out.contains("ABS"), "{out}");
        assert!(!out.contains("CASE"), "{out}");
        assert!(!out.contains("hello world"), "{out}");
        assert!(!out.contains("123456"), "{out}");
        assert!(!out.contains("999999"), "{out}");
    }

    #[test]
    fn shrink_loop_terminates_when_everything_is_accepted() {
        let sql = "SELECT MAX(1, MIN(2, 3)), 'literal', X'AB' FROM t";
        let out = shrink_with(sql, |_| Ok(true)).unwrap();
        assert!(out.len() < sql.len(), "{out}");
    }

    #[test]
    fn shrink_statement_returns_none_without_divergence() {
        // Same statement behaves identically on both engines, so there is
        // nothing to shrink against.
        let state = "CREATE TABLE t(x);\nINSERT INTO t VALUES (1);\n";
        let out = shrink_statement(state, state, "SELECT x + 1 FROM t").unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn state_reduction_keeps_only_needed_lines() {
        // Synthetic judge: the reproduction needs the CREATE and one INSERT;
        // every other line must be deleted.
        let state = "CREATE TABLE t(x);\nINSERT INTO t VALUES (1);\n\
                     CREATE TABLE junk(y);\nINSERT INTO junk VALUES (2);\n\
                     CREATE INDEX i ON junk(y);";
        let out = reduce_state_lines(state, |cand| {
            Ok(cand.contains("CREATE TABLE t(x);") && cand.contains("INSERT INTO t VALUES (1);"))
        })
        .unwrap();
        assert_eq!(
            out, "CREATE TABLE t(x);\nINSERT INTO t VALUES (1);",
            "{out}"
        );
    }
}
