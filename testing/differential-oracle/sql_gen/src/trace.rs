//! Trace tree and coverage tracking.
//!
//! The trace system automatically records what SQL constructs are generated
//! and tracks hierarchical coverage by origin (WHERE clause, SELECT list, etc.).

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use strum::IntoEnumIterator;

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
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, strum::EnumIter)]
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
    AlterTable,
    AlterTableAction,
    Subquery,
    CaseWhen,
    CaseThen,
    CaseElse,
    FunctionArg,
    BinaryOpLeft,
    BinaryOpRight,
    CreateTrigger,
    TriggerWhen,
    TriggerBody,
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
            Origin::AlterTable => write!(f, "AlterTable"),
            Origin::AlterTableAction => write!(f, "AlterTableAction"),
            Origin::Subquery => write!(f, "Subquery"),
            Origin::CaseWhen => write!(f, "CaseWhen"),
            Origin::CaseThen => write!(f, "CaseThen"),
            Origin::CaseElse => write!(f, "CaseElse"),
            Origin::FunctionArg => write!(f, "FunctionArg"),
            Origin::BinaryOpLeft => write!(f, "BinaryOpLeft"),
            Origin::BinaryOpRight => write!(f, "BinaryOpRight"),
            Origin::CreateTrigger => write!(f, "CreateTrigger"),
            Origin::TriggerWhen => write!(f, "TriggerWhen"),
            Origin::TriggerBody => write!(f, "TriggerBody"),
        }
    }
}

// ExprKind and StmtKind are derived from Expr and Stmt in ast.rs via EnumDiscriminants
pub use crate::ast::{ExprKind, StmtKind};

/// Controls how the generation tree section of the coverage report is rendered.
#[derive(Clone, Copy, Debug, Default)]
pub enum TreeMode {
    /// Full hierarchical tree showing nesting structure.
    Full,
    /// Flat view: for each Origin variant, show aggregated expression counts
    /// regardless of nesting depth.
    #[default]
    Simplified,
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
    /// Paths are inserted (with empty maps) on scope entry, so even scopes
    /// that generate no expressions are tracked.
    pub by_origin: HashMap<OriginPath, HashMap<ExprKind, usize>>,
}

impl Coverage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a scope was entered at the given origin path.
    /// Ensures the path exists in `by_origin` even if no expressions are generated.
    pub fn record_scope(&mut self, path: &OriginPath) {
        self.by_origin.entry(path.clone()).or_default();
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
    ///
    /// Uses `EnumIter` to enumerate all possible `ExprKind`, `StmtKind`, and
    /// `Origin` variants, so the report includes zero-count (missing) variants.
    pub fn report(&self) -> CoverageReport {
        self.report_with_mode(TreeMode::default())
    }

    /// Generate a coverage report with a specific tree rendering mode.
    pub fn report_with_mode(&self, tree_mode: TreeMode) -> CoverageReport {
        let total_exprs = self.total_exprs();
        let total_stmts = self.total_stmts();

        // All variants, including zero-count ones
        let mut expr_distribution: Vec<_> = ExprKind::iter()
            .map(|k| (k, *self.expr_counts.get(&k).unwrap_or(&0)))
            .collect();
        expr_distribution.sort_by(|a, b| b.1.cmp(&a.1));

        let mut stmt_distribution: Vec<_> = StmtKind::iter()
            .map(|k| (k, *self.stmt_counts.get(&k).unwrap_or(&0)))
            .collect();
        stmt_distribution.sort_by(|a, b| b.1.cmp(&a.1));

        let expr_variants_hit = ExprKind::iter()
            .filter(|k| self.expr_counts.get(k).is_some_and(|c| *c > 0))
            .count();
        let stmt_variants_hit = StmtKind::iter()
            .filter(|k| self.stmt_counts.get(k).is_some_and(|c| *c > 0))
            .count();

        // Build the origin tree from by_origin paths
        let tree = CoverageTree::from_by_origin(&self.by_origin);

        // Count how many unique origins were entered (appear in any path).
        let origins_hit: std::collections::HashSet<Origin> = self
            .by_origin
            .keys()
            .flat_map(|path| path.0.iter().copied())
            .collect();
        let origin_variants_hit = origins_hit.len();

        CoverageReport {
            total_exprs,
            total_stmts,
            expr_variants_total: ExprKind::iter().count(),
            expr_variants_hit,
            stmt_variants_total: StmtKind::iter().count(),
            stmt_variants_hit,
            origin_variants_total: Origin::iter().count(),
            origin_variants_hit,
            expr_distribution,
            stmt_distribution,
            tree,
            tree_mode,
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

/// A node in the coverage tree.
#[derive(Debug, Default)]
pub struct CoverageTreeNode {
    /// Expression counts at this origin level (includes rolled-up deeper counts).
    pub expr_counts: HashMap<ExprKind, usize>,
    /// Children keyed by origin, sorted for deterministic output.
    pub children: BTreeMap<String, CoverageTreeNode>,
}

impl CoverageTreeNode {
    pub fn total_exprs(&self) -> usize {
        self.expr_counts.values().sum()
    }

    /// Recursively total all expressions in this subtree.
    pub fn subtree_exprs(&self) -> usize {
        let mut total = self.total_exprs();
        for child in self.children.values() {
            total += child.subtree_exprs();
        }
        total
    }

    fn fmt_expr_line(&self) -> String {
        let mut sorted: Vec<_> = self
            .expr_counts
            .iter()
            .filter(|(_, c)| **c > 0)
            .map(|(k, c)| (*k, *c))
            .collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));
        sorted
            .iter()
            .map(|(k, c)| format!("{k}: {c}"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn fmt_tree(
        &self,
        f: &mut fmt::Formatter<'_>,
        prefix: &str,
        is_last: bool,
        name: &str,
    ) -> fmt::Result {
        let connector = if is_last { "└── " } else { "├── " };
        let subtotal = self.subtree_exprs();
        let local = self.total_exprs();

        if subtotal == 0 && local == 0 {
            writeln!(f, "{prefix}{connector}{name}  (not hit)")?;
            return Ok(());
        }

        writeln!(f, "{prefix}{connector}{name}  ({subtotal} exprs)")?;

        let child_prefix = format!("{prefix}{}", if is_last { "    " } else { "│   " });

        if local > 0 {
            let line = self.fmt_expr_line();
            if !line.is_empty() {
                writeln!(f, "{child_prefix}  {line}")?;
            }
        }

        let children: Vec<_> = self.children.iter().collect();
        for (i, (child_name, child)) in children.iter().enumerate() {
            let child_is_last = i == children.len() - 1;
            child.fmt_tree(f, &child_prefix, child_is_last, child_name)?;
        }

        Ok(())
    }
}

/// A tree representation of expression generation coverage, built from origin paths.
#[derive(Debug, Default)]
pub struct CoverageTree {
    pub root: CoverageTreeNode,
}

impl CoverageTree {
    /// Build a coverage tree from the `by_origin` flat map of paths.
    pub fn from_by_origin(by_origin: &HashMap<OriginPath, HashMap<ExprKind, usize>>) -> Self {
        let mut root = CoverageTreeNode::default();

        for (path, expr_counts) in by_origin {
            // Skip Root (implicit)
            let segments: Vec<String> = path.0.iter().skip(1).map(|o| o.to_string()).collect();
            let mut node = &mut root;

            if segments.is_empty() {
                for (kind, count) in expr_counts {
                    *node.expr_counts.entry(*kind).or_default() += count;
                }
            } else {
                for (i, seg) in segments.iter().enumerate() {
                    node = node.children.entry(seg.clone()).or_default();
                    if i == segments.len() - 1 {
                        for (kind, count) in expr_counts {
                            *node.expr_counts.entry(*kind).or_default() += count;
                        }
                    }
                }
            }
        }

        CoverageTree { root }
    }
}

impl fmt::Display for CoverageTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total = self.root.subtree_exprs();
        writeln!(f, "Root  ({total} exprs)")?;
        let local = self.root.total_exprs();
        if local > 0 {
            let line = self.root.fmt_expr_line();
            if !line.is_empty() {
                writeln!(f, "    {line}")?;
            }
        }
        let children: Vec<_> = self.root.children.iter().collect();
        for (i, (name, child)) in children.iter().enumerate() {
            let is_last = i == children.len() - 1;
            child.fmt_tree(f, "", is_last, name)?;
        }
        Ok(())
    }
}

/// A coverage report with statistics.
#[derive(Debug)]
pub struct CoverageReport {
    pub total_exprs: usize,
    pub total_stmts: usize,
    pub expr_variants_total: usize,
    pub expr_variants_hit: usize,
    pub stmt_variants_total: usize,
    pub stmt_variants_hit: usize,
    pub origin_variants_total: usize,
    pub origin_variants_hit: usize,
    pub expr_distribution: Vec<(ExprKind, usize)>,
    pub stmt_distribution: Vec<(StmtKind, usize)>,
    pub tree: CoverageTree,
    pub tree_mode: TreeMode,
}

impl CoverageReport {
    pub fn expr_coverage_pct(&self) -> f64 {
        if self.expr_variants_total == 0 {
            return 0.0;
        }
        (self.expr_variants_hit as f64 / self.expr_variants_total as f64) * 100.0
    }

    pub fn stmt_coverage_pct(&self) -> f64 {
        if self.stmt_variants_total == 0 {
            return 0.0;
        }
        (self.stmt_variants_hit as f64 / self.stmt_variants_total as f64) * 100.0
    }

    pub fn origin_coverage_pct(&self) -> f64 {
        if self.origin_variants_total == 0 {
            return 0.0;
        }
        (self.origin_variants_hit as f64 / self.origin_variants_total as f64) * 100.0
    }

    pub fn missing_expr_kinds(&self) -> Vec<ExprKind> {
        self.expr_distribution
            .iter()
            .filter(|(_, count)| *count == 0)
            .map(|(kind, _)| *kind)
            .collect()
    }

    pub fn missing_stmt_kinds(&self) -> Vec<StmtKind> {
        self.stmt_distribution
            .iter()
            .filter(|(_, count)| *count == 0)
            .map(|(kind, _)| *kind)
            .collect()
    }
}

impl fmt::Display for CoverageReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=== Coverage Report ===")?;
        writeln!(f)?;
        writeln!(
            f,
            "Statements:  {} generated, {}/{} variants hit ({:.1}%)",
            self.total_stmts,
            self.stmt_variants_hit,
            self.stmt_variants_total,
            self.stmt_coverage_pct(),
        )?;
        writeln!(
            f,
            "Expressions: {} generated, {}/{} variants hit ({:.1}%)",
            self.total_exprs,
            self.expr_variants_hit,
            self.expr_variants_total,
            self.expr_coverage_pct(),
        )?;
        writeln!(
            f,
            "Origins:     {}/{} variants hit ({:.1}%)",
            self.origin_variants_hit,
            self.origin_variants_total,
            self.origin_coverage_pct(),
        )?;

        writeln!(f, "\n--- Statement Distribution ---")?;
        for (kind, count) in &self.stmt_distribution {
            let pct = if self.total_stmts > 0 {
                (*count as f64 / self.total_stmts as f64) * 100.0
            } else {
                0.0
            };
            if *count > 0 {
                writeln!(f, "  {kind:<20} {count:>6}  ({pct:.1}%)")?;
            } else {
                writeln!(f, "  {kind:<20}      -  (not generated)")?;
            }
        }

        writeln!(f, "\n--- Expression Distribution ---")?;
        for (kind, count) in &self.expr_distribution {
            let pct = if self.total_exprs > 0 {
                (*count as f64 / self.total_exprs as f64) * 100.0
            } else {
                0.0
            };
            if *count > 0 {
                writeln!(f, "  {kind:<20} {count:>6}  ({pct:.1}%)")?;
            } else {
                writeln!(f, "  {kind:<20}      -  (not generated)")?;
            }
        }

        match self.tree_mode {
            TreeMode::Full => {
                writeln!(f, "\n--- Generation Tree ---")?;
                write!(f, "{}", self.tree)?;
            }
            TreeMode::Simplified => {
                writeln!(f, "\n--- Origin Distribution (simplified) ---")?;
                // Compute flattened origin -> subtree expr counts from the tree.
                // Each origin gets the total of all expressions in its subtree,
                // so container origins like Insert/Update/Delete include their
                // children's counts.
                let mut origin_exprs: HashMap<Origin, HashMap<ExprKind, usize>> =
                    HashMap::new();
                fn collect_subtree(
                    node: &CoverageTreeNode,
                    acc: &mut HashMap<ExprKind, usize>,
                ) {
                    for (kind, count) in &node.expr_counts {
                        *acc.entry(*kind).or_default() += count;
                    }
                    for child in node.children.values() {
                        collect_subtree(child, acc);
                    }
                }
                fn collect_origins(
                    name: Option<Origin>,
                    node: &CoverageTreeNode,
                    acc: &mut HashMap<Origin, HashMap<ExprKind, usize>>,
                ) {
                    if let Some(origin) = name {
                        let mut subtree_counts = HashMap::new();
                        collect_subtree(node, &mut subtree_counts);
                        let entry = acc.entry(origin).or_default();
                        for (kind, count) in subtree_counts {
                            *entry.entry(kind).or_default() += count;
                        }
                    }
                    for (child_name, child) in &node.children {
                        let child_origin = Origin::iter()
                            .find(|o| o.to_string() == *child_name);
                        collect_origins(child_origin, child, acc);
                    }
                }
                collect_origins(Some(Origin::Root), &self.tree.root, &mut origin_exprs);

                // Collect which origins exist as tree nodes (were entered).
                let mut entered: std::collections::HashSet<Origin> = std::collections::HashSet::new();
                fn collect_entered(
                    name: Option<Origin>,
                    node: &CoverageTreeNode,
                    entered: &mut std::collections::HashSet<Origin>,
                ) {
                    if let Some(origin) = name {
                        entered.insert(origin);
                    }
                    for (child_name, child) in &node.children {
                        let child_origin = Origin::iter()
                            .find(|o| o.to_string() == *child_name);
                        collect_entered(child_origin, child, entered);
                    }
                }
                collect_entered(Some(Origin::Root), &self.tree.root, &mut entered);

                let mut origins: Vec<_> = Origin::iter()
                    .map(|o| {
                        let counts = origin_exprs.remove(&o).unwrap_or_default();
                        let total: usize = counts.values().sum();
                        let was_entered = entered.contains(&o);
                        (o, total, counts, was_entered)
                    })
                    .collect();
                origins.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.3.cmp(&b.3).reverse()));
                for (origin, total, counts, was_entered) in &origins {
                    if !was_entered {
                        writeln!(f, "  {origin}  (not hit)")?;
                    } else if *total == 0 {
                        writeln!(f, "  {origin}  (no expressions)")?;
                    } else {
                        writeln!(f, "  {origin}  ({total} exprs)")?;
                        let mut sorted: Vec<_> = counts
                            .iter()
                            .filter(|(_, c)| **c > 0)
                            .map(|(k, c)| (*k, *c))
                            .collect();
                        sorted.sort_by(|a, b| b.1.cmp(&a.1));
                        let line: Vec<String> =
                            sorted.iter().map(|(k, c)| format!("{k}: {c}")).collect();
                        writeln!(f, "    {}", line.join(", "))?;
                    }
                }
            }
        }

        let missing_stmts = self.missing_stmt_kinds();
        let missing_exprs = self.missing_expr_kinds();
        if !missing_stmts.is_empty() || !missing_exprs.is_empty() {
            writeln!(f, "\n--- Not Covered ---")?;
            if !missing_stmts.is_empty() {
                let names: Vec<_> = missing_stmts.iter().map(|k| format!("{k}")).collect();
                writeln!(f, "  Statements:  {}", names.join(", "))?;
            }
            if !missing_exprs.is_empty() {
                let names: Vec<_> = missing_exprs.iter().map(|k| format!("{k}")).collect();
                writeln!(f, "  Expressions: {}", names.join(", "))?;
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
        assert_eq!(report.expr_variants_hit, 2);
        assert_eq!(report.stmt_variants_hit, 1);
        // All variants are listed (including zero-count ones)
        assert_eq!(report.expr_distribution.len(), ExprKind::iter().count());
        assert_eq!(report.stmt_distribution.len(), StmtKind::iter().count());
        // Missing variants don't include ColumnRef/Literal (they were generated)
        assert!(!report.missing_expr_kinds().contains(&ExprKind::ColumnRef));
        assert!(!report.missing_expr_kinds().contains(&ExprKind::Literal));
        assert!(report.missing_expr_kinds().contains(&ExprKind::BinaryOp));
        // Tree: Root was hit (path defaults to Root)
        assert_eq!(report.origin_variants_hit, 1); // only Root
        assert_eq!(report.tree.root.total_exprs(), 2);
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
