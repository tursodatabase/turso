//! Trace tree and coverage tracking.
//!
//! The trace system automatically records what SQL constructs are generated
//! and tracks hierarchical coverage by origin (WHERE clause, SELECT list, etc.).

use std::collections::HashMap;
use std::fmt;

/// A trace of generated SQL, organized as a tree.
#[derive(Clone, Debug, Default)]
pub struct Trace {
    pub root: TraceNode,
}

impl Trace {
    pub fn new() -> Self {
        Self {
            root: TraceNode::new(NodeKind::Scope(Origin::Root)),
        }
    }
}

/// A node in the trace tree.
#[derive(Clone, Debug)]
pub struct TraceNode {
    pub kind: NodeKind,
    pub children: Vec<TraceNode>,
}

impl TraceNode {
    pub fn new(kind: NodeKind) -> Self {
        Self {
            kind,
            children: Vec::new(),
        }
    }
}

impl Default for TraceNode {
    fn default() -> Self {
        Self::new(NodeKind::Scope(Origin::Root))
    }
}

/// Kind of trace node.
#[derive(Clone, Debug)]
pub enum NodeKind {
    Scope(Origin),
    Expr(ExprKind),
    Stmt(StmtKind),
}

/// Origin of a generated construct (where in the SQL it appears).
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub enum Origin {
    Root,
    Select,
    From,
    Where,
    GroupBy,
    Having,
    OrderBy,
    Limit,
    Insert,
    InsertValues,
    Update,
    UpdateSet,
    Delete,
    Subquery,
    CaseWhen,
    CaseThen,
    CaseElse,
    FunctionArg,
    BinaryOpLeft,
    BinaryOpRight,
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Origin::Root => write!(f, "Root"),
            Origin::Select => write!(f, "Select"),
            Origin::From => write!(f, "From"),
            Origin::Where => write!(f, "Where"),
            Origin::GroupBy => write!(f, "GroupBy"),
            Origin::Having => write!(f, "Having"),
            Origin::OrderBy => write!(f, "OrderBy"),
            Origin::Limit => write!(f, "Limit"),
            Origin::Insert => write!(f, "Insert"),
            Origin::InsertValues => write!(f, "InsertValues"),
            Origin::Update => write!(f, "Update"),
            Origin::UpdateSet => write!(f, "UpdateSet"),
            Origin::Delete => write!(f, "Delete"),
            Origin::Subquery => write!(f, "Subquery"),
            Origin::CaseWhen => write!(f, "CaseWhen"),
            Origin::CaseThen => write!(f, "CaseThen"),
            Origin::CaseElse => write!(f, "CaseElse"),
            Origin::FunctionArg => write!(f, "FunctionArg"),
            Origin::BinaryOpLeft => write!(f, "BinaryOpLeft"),
            Origin::BinaryOpRight => write!(f, "BinaryOpRight"),
        }
    }
}

/// Kind of expression.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub enum ExprKind {
    ColumnRef,
    Literal,
    BinaryOp,
    UnaryOp,
    FunctionCall,
    Subquery,
    CaseExpr,
    Cast,
    Between,
    InList,
    IsNull,
    IsNotNull,
}

impl fmt::Display for ExprKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExprKind::ColumnRef => write!(f, "ColumnRef"),
            ExprKind::Literal => write!(f, "Literal"),
            ExprKind::BinaryOp => write!(f, "BinaryOp"),
            ExprKind::UnaryOp => write!(f, "UnaryOp"),
            ExprKind::FunctionCall => write!(f, "FunctionCall"),
            ExprKind::Subquery => write!(f, "Subquery"),
            ExprKind::CaseExpr => write!(f, "CaseExpr"),
            ExprKind::Cast => write!(f, "Cast"),
            ExprKind::Between => write!(f, "Between"),
            ExprKind::InList => write!(f, "InList"),
            ExprKind::IsNull => write!(f, "IsNull"),
            ExprKind::IsNotNull => write!(f, "IsNotNull"),
        }
    }
}

/// Kind of statement.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub enum StmtKind {
    // DML
    Select,
    Insert,
    Update,
    Delete,
    // DDL
    CreateTable,
    DropTable,
    CreateIndex,
    DropIndex,
    // Transactions
    Begin,
    Commit,
    Rollback,
}

impl fmt::Display for StmtKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StmtKind::Select => write!(f, "SELECT"),
            StmtKind::Insert => write!(f, "INSERT"),
            StmtKind::Update => write!(f, "UPDATE"),
            StmtKind::Delete => write!(f, "DELETE"),
            StmtKind::CreateTable => write!(f, "CREATE TABLE"),
            StmtKind::DropTable => write!(f, "DROP TABLE"),
            StmtKind::CreateIndex => write!(f, "CREATE INDEX"),
            StmtKind::DropIndex => write!(f, "DROP INDEX"),
            StmtKind::Begin => write!(f, "BEGIN"),
            StmtKind::Commit => write!(f, "COMMIT"),
            StmtKind::Rollback => write!(f, "ROLLBACK"),
        }
    }
}

/// A path of origins representing nesting.
#[derive(Clone, Debug, Hash, Eq, PartialEq, Default)]
pub struct OriginPath(pub Vec<Origin>);

impl OriginPath {
    pub fn new() -> Self {
        Self(vec![Origin::Root])
    }

    pub fn push(&mut self, origin: Origin) {
        self.0.push(origin);
    }

    pub fn pop(&mut self) -> Option<Origin> {
        if self.0.len() > 1 { self.0.pop() } else { None }
    }

    pub fn display(&self) -> String {
        self.0
            .iter()
            .map(|o| format!("{o}"))
            .collect::<Vec<_>>()
            .join(" > ")
    }

    pub fn current(&self) -> Origin {
        *self.0.last().unwrap_or(&Origin::Root)
    }
}

impl fmt::Display for OriginPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display())
    }
}

/// Coverage statistics for generated SQL.
#[derive(Clone, Debug, Default)]
pub struct Coverage {
    /// Total counts by expression kind.
    pub expr_counts: HashMap<ExprKind, usize>,

    /// Total counts by statement kind.
    pub stmt_counts: HashMap<StmtKind, usize>,

    /// Hierarchical: origin path -> expr kind -> count.
    pub by_origin: HashMap<OriginPath, HashMap<ExprKind, usize>>,
}

impl Coverage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an expression at the given origin path.
    pub fn record_expr(&mut self, path: &OriginPath, kind: ExprKind) {
        *self.expr_counts.entry(kind).or_default() += 1;
        *self
            .by_origin
            .entry(path.clone())
            .or_default()
            .entry(kind)
            .or_default() += 1;
    }

    /// Record a statement.
    pub fn record_stmt(&mut self, kind: StmtKind) {
        *self.stmt_counts.entry(kind).or_default() += 1;
    }

    /// Get total expression count.
    pub fn total_exprs(&self) -> usize {
        self.expr_counts.values().sum()
    }

    /// Get total statement count.
    pub fn total_stmts(&self) -> usize {
        self.stmt_counts.values().sum()
    }

    /// Generate a coverage report.
    pub fn report(&self) -> CoverageReport {
        let total_exprs = self.total_exprs();
        let total_stmts = self.total_stmts();

        let mut expr_distribution: Vec<_> =
            self.expr_counts.iter().map(|(k, v)| (*k, *v)).collect();
        expr_distribution.sort_by(|a, b| b.1.cmp(&a.1));

        let mut stmt_distribution: Vec<_> =
            self.stmt_counts.iter().map(|(k, v)| (*k, *v)).collect();
        stmt_distribution.sort_by(|a, b| b.1.cmp(&a.1));

        let mut by_origin: Vec<_> = self
            .by_origin
            .iter()
            .map(|(path, counts)| (path.display(), counts.clone()))
            .collect();
        by_origin.sort_by(|a, b| a.0.cmp(&b.0));

        CoverageReport {
            total_exprs,
            total_stmts,
            expr_distribution,
            stmt_distribution,
            by_origin,
        }
    }

    /// Merge another coverage into this one.
    pub fn merge(&mut self, other: &Coverage) {
        for (kind, count) in &other.expr_counts {
            *self.expr_counts.entry(*kind).or_default() += count;
        }
        for (kind, count) in &other.stmt_counts {
            *self.stmt_counts.entry(*kind).or_default() += count;
        }
        for (path, counts) in &other.by_origin {
            let entry = self.by_origin.entry(path.clone()).or_default();
            for (kind, count) in counts {
                *entry.entry(*kind).or_default() += count;
            }
        }
    }
}

/// A coverage report with statistics.
#[derive(Debug)]
pub struct CoverageReport {
    pub total_exprs: usize,
    pub total_stmts: usize,
    pub expr_distribution: Vec<(ExprKind, usize)>,
    pub stmt_distribution: Vec<(StmtKind, usize)>,
    pub by_origin: Vec<(String, HashMap<ExprKind, usize>)>,
}

impl fmt::Display for CoverageReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=== Coverage Report ===")?;
        writeln!(f, "Total statements: {}", self.total_stmts)?;
        writeln!(f, "Total expressions: {}", self.total_exprs)?;

        if !self.stmt_distribution.is_empty() {
            writeln!(f, "\nStatement distribution:")?;
            for (kind, count) in &self.stmt_distribution {
                let pct = if self.total_stmts > 0 {
                    (*count as f64 / self.total_stmts as f64) * 100.0
                } else {
                    0.0
                };
                writeln!(f, "  {kind}: {count} ({pct:.1}%)")?;
            }
        }

        if !self.expr_distribution.is_empty() {
            writeln!(f, "\nExpression distribution:")?;
            for (kind, count) in &self.expr_distribution {
                let pct = if self.total_exprs > 0 {
                    (*count as f64 / self.total_exprs as f64) * 100.0
                } else {
                    0.0
                };
                writeln!(f, "  {kind}: {count} ({pct:.1}%)")?;
            }
        }

        if !self.by_origin.is_empty() {
            writeln!(f, "\nBy origin:")?;
            for (path, counts) in &self.by_origin {
                writeln!(f, "  {path}:")?;
                let mut sorted: Vec<_> = counts.iter().collect();
                sorted.sort_by(|a, b| b.1.cmp(a.1));
                for (kind, count) in sorted {
                    writeln!(f, "    {kind}: {count}")?;
                }
            }
        }

        Ok(())
    }
}

/// Builder for constructing trace trees.
#[derive(Debug, Default)]
pub struct TraceBuilder {
    stack: Vec<TraceNode>,
}

impl TraceBuilder {
    pub fn new() -> Self {
        Self {
            stack: vec![TraceNode::new(NodeKind::Scope(Origin::Root))],
        }
    }

    pub fn push_scope(&mut self, origin: Origin) {
        self.stack.push(TraceNode::new(NodeKind::Scope(origin)));
    }

    pub fn pop_scope(&mut self) {
        if self.stack.len() > 1 {
            if let Some(node) = self.stack.pop() {
                if let Some(parent) = self.stack.last_mut() {
                    parent.children.push(node);
                }
            }
        }
    }

    pub fn add_expr(&mut self, kind: ExprKind) {
        if let Some(current) = self.stack.last_mut() {
            current.children.push(TraceNode::new(NodeKind::Expr(kind)));
        }
    }

    pub fn add_stmt(&mut self, kind: StmtKind) {
        if let Some(current) = self.stack.last_mut() {
            current.children.push(TraceNode::new(NodeKind::Stmt(kind)));
        }
    }

    pub fn build(mut self) -> Trace {
        // Close any remaining scopes
        while self.stack.len() > 1 {
            self.pop_scope();
        }

        Trace {
            root: self.stack.pop().unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_origin_path() {
        let mut path = OriginPath::new();
        assert_eq!(path.current(), Origin::Root);

        path.push(Origin::Select);
        assert_eq!(path.current(), Origin::Select);
        assert_eq!(path.display(), "Root > Select");

        path.push(Origin::Where);
        assert_eq!(path.display(), "Root > Select > Where");

        path.pop();
        assert_eq!(path.current(), Origin::Select);
    }

    #[test]
    fn test_coverage_recording() {
        let mut coverage = Coverage::new();
        let path = OriginPath::new();

        coverage.record_expr(&path, ExprKind::ColumnRef);
        coverage.record_expr(&path, ExprKind::ColumnRef);
        coverage.record_expr(&path, ExprKind::Literal);
        coverage.record_stmt(StmtKind::Select);

        assert_eq!(coverage.total_exprs(), 3);
        assert_eq!(coverage.total_stmts(), 1);
        assert_eq!(coverage.expr_counts.get(&ExprKind::ColumnRef), Some(&2));
    }

    #[test]
    fn test_coverage_report() {
        let mut coverage = Coverage::new();
        let path = OriginPath::new();

        coverage.record_expr(&path, ExprKind::ColumnRef);
        coverage.record_expr(&path, ExprKind::Literal);
        coverage.record_stmt(StmtKind::Select);

        let report = coverage.report();
        assert_eq!(report.total_exprs, 2);
        assert_eq!(report.total_stmts, 1);
    }

    #[test]
    fn test_trace_builder() {
        let mut builder = TraceBuilder::new();

        builder.push_scope(Origin::Select);
        builder.add_expr(ExprKind::ColumnRef);
        builder.push_scope(Origin::Where);
        builder.add_expr(ExprKind::BinaryOp);
        builder.pop_scope();
        builder.pop_scope();

        let trace = builder.build();
        assert!(!trace.root.children.is_empty());
    }

    #[test]
    fn test_coverage_merge() {
        let mut coverage1 = Coverage::new();
        let mut coverage2 = Coverage::new();
        let path = OriginPath::new();

        coverage1.record_expr(&path, ExprKind::ColumnRef);
        coverage2.record_expr(&path, ExprKind::ColumnRef);
        coverage2.record_expr(&path, ExprKind::Literal);

        coverage1.merge(&coverage2);

        assert_eq!(coverage1.total_exprs(), 3);
        assert_eq!(coverage1.expr_counts.get(&ExprKind::ColumnRef), Some(&2));
    }
}
