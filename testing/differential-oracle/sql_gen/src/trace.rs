//! Coverage tracking for SQL generation.
//!
//! Tracks what SQL constructs are generated and provides hierarchical coverage
//! by origin (WHERE clause, SELECT list, etc.).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use strum::IntoEnumIterator;

/// Origin of a generated construct (where in the SQL it appears).
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, strum::EnumIter, strum::Display)]
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

    pub fn current(&self) -> Origin {
        *self.0.last().unwrap_or(&Origin::Root)
    }
}

impl fmt::Display for OriginPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for o in &self.0 {
            if !first {
                write!(f, " > ")?;
            }
            write!(f, "{o}")?;
            first = false;
        }
        Ok(())
    }
}

/// Coverage statistics for generated SQL.
///
/// Expression counts are derived from `by_origin` at query time rather than
/// stored redundantly. Only statement counts are stored separately since
/// statements are not tracked by origin.
#[derive(Clone, Debug, Default)]
pub struct Coverage {
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
    pub fn record_scope(&mut self, path: &OriginPath) {
        self.by_origin.entry(path.clone()).or_default();
    }

    /// Record an expression at the given origin path.
    pub fn record_expr(&mut self, path: &OriginPath, kind: ExprKind) {
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

    /// Compute expression counts aggregated across all origin paths.
    pub fn expr_counts(&self) -> HashMap<ExprKind, usize> {
        let mut counts = HashMap::new();
        for path_counts in self.by_origin.values() {
            for (kind, count) in path_counts {
                *counts.entry(*kind).or_default() += count;
            }
        }
        counts
    }

    /// Get total expression count.
    pub fn total_exprs(&self) -> usize {
        self.by_origin.values().flat_map(|m| m.values()).sum()
    }

    /// Get total statement count.
    pub fn total_stmts(&self) -> usize {
        self.stmt_counts.values().sum()
    }

    /// Generate a coverage report.
    pub fn report(&self) -> CoverageReport {
        self.report_with_mode(TreeMode::default())
    }

    /// Generate a coverage report with a specific tree rendering mode.
    pub fn report_with_mode(&self, tree_mode: TreeMode) -> CoverageReport {
        CoverageReport {
            coverage: self.clone(),
            tree_mode,
        }
    }

    /// Merge another coverage into this one.
    pub fn merge(&mut self, other: &Coverage) {
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

/// A node in the coverage tree (used internally for report rendering).
#[derive(Debug, Default)]
struct CoverageTreeNode {
    expr_counts: HashMap<ExprKind, usize>,
    children: BTreeMap<String, CoverageTreeNode>,
}

impl CoverageTreeNode {
    fn total_exprs(&self) -> usize {
        self.expr_counts.values().sum()
    }

    fn subtree_exprs(&self) -> usize {
        let mut total = self.total_exprs();
        for child in self.children.values() {
            total += child.subtree_exprs();
        }
        total
    }

    fn collect_all_expr_counts(&self, acc: &mut HashMap<ExprKind, usize>) {
        for (kind, count) in &self.expr_counts {
            *acc.entry(*kind).or_default() += count;
        }
        for child in self.children.values() {
            child.collect_all_expr_counts(acc);
        }
    }

    fn collect_entered_origins(&self, name: Option<Origin>, entered: &mut HashSet<Origin>) {
        if let Some(origin) = name {
            entered.insert(origin);
        }
        for (child_name, child) in &self.children {
            let child_origin = Origin::iter().find(|o| o.to_string() == *child_name);
            child.collect_entered_origins(child_origin, entered);
        }
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

/// Build a coverage tree from the `by_origin` flat map of paths.
fn build_coverage_tree(
    by_origin: &HashMap<OriginPath, HashMap<ExprKind, usize>>,
) -> CoverageTreeNode {
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

    root
}

fn fmt_coverage_tree(root: &CoverageTreeNode, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let total = root.subtree_exprs();
    writeln!(f, "Root  ({total} exprs)")?;
    let local = root.total_exprs();
    if local > 0 {
        let line = root.fmt_expr_line();
        if !line.is_empty() {
            writeln!(f, "    {line}")?;
        }
    }
    let children: Vec<_> = root.children.iter().collect();
    for (i, (name, child)) in children.iter().enumerate() {
        let is_last = i == children.len() - 1;
        child.fmt_tree(f, "", is_last, name)?;
    }
    Ok(())
}

/// A coverage report. All statistics are computed on-the-fly from the
/// underlying `Coverage` data rather than stored.
#[derive(Debug)]
pub struct CoverageReport {
    coverage: Coverage,
    tree_mode: TreeMode,
}

impl CoverageReport {
    pub fn total_exprs(&self) -> usize {
        self.coverage.total_exprs()
    }

    pub fn total_stmts(&self) -> usize {
        self.coverage.total_stmts()
    }

    pub fn expr_variants_hit(&self) -> usize {
        let counts = self.coverage.expr_counts();
        ExprKind::iter()
            .filter(|k| counts.get(k).is_some_and(|c| *c > 0))
            .count()
    }

    pub fn stmt_variants_hit(&self) -> usize {
        StmtKind::iter()
            .filter(|k| self.coverage.stmt_counts.get(k).is_some_and(|c| *c > 0))
            .count()
    }

    pub fn origin_variants_hit(&self) -> usize {
        self.coverage
            .by_origin
            .keys()
            .flat_map(|path| path.0.iter().copied())
            .collect::<HashSet<Origin>>()
            .len()
    }

    pub fn expr_coverage_pct(&self) -> f64 {
        let total = ExprKind::iter().count();
        if total == 0 {
            return 0.0;
        }
        (self.expr_variants_hit() as f64 / total as f64) * 100.0
    }

    pub fn stmt_coverage_pct(&self) -> f64 {
        let total = StmtKind::iter().count();
        if total == 0 {
            return 0.0;
        }
        (self.stmt_variants_hit() as f64 / total as f64) * 100.0
    }

    pub fn origin_coverage_pct(&self) -> f64 {
        let total = Origin::iter().count();
        if total == 0 {
            return 0.0;
        }
        (self.origin_variants_hit() as f64 / total as f64) * 100.0
    }

    pub fn missing_expr_kinds(&self) -> Vec<ExprKind> {
        let counts = self.coverage.expr_counts();
        ExprKind::iter()
            .filter(|k| !counts.get(k).is_some_and(|c| *c > 0))
            .collect()
    }

    pub fn missing_stmt_kinds(&self) -> Vec<StmtKind> {
        StmtKind::iter()
            .filter(|k| !self.coverage.stmt_counts.get(k).is_some_and(|c| *c > 0))
            .collect()
    }
}

impl fmt::Display for CoverageReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total_exprs = self.total_exprs();
        let total_stmts = self.total_stmts();
        let expr_variants_total = ExprKind::iter().count();
        let stmt_variants_total = StmtKind::iter().count();
        let origin_variants_total = Origin::iter().count();

        writeln!(f, "=== Coverage Report ===")?;
        writeln!(f)?;
        writeln!(
            f,
            "Statements:  {} generated, {}/{} variants hit ({:.1}%)",
            total_stmts,
            self.stmt_variants_hit(),
            stmt_variants_total,
            self.stmt_coverage_pct(),
        )?;
        writeln!(
            f,
            "Expressions: {} generated, {}/{} variants hit ({:.1}%)",
            total_exprs,
            self.expr_variants_hit(),
            expr_variants_total,
            self.expr_coverage_pct(),
        )?;
        writeln!(
            f,
            "Origins:     {}/{} variants hit ({:.1}%)",
            self.origin_variants_hit(),
            origin_variants_total,
            self.origin_coverage_pct(),
        )?;

        // Statement distribution
        let mut stmt_distribution: Vec<_> = StmtKind::iter()
            .map(|k| (k, *self.coverage.stmt_counts.get(&k).unwrap_or(&0)))
            .collect();
        stmt_distribution.sort_by(|a, b| b.1.cmp(&a.1));

        writeln!(f, "\n--- Statement Distribution ---")?;
        for (kind, count) in &stmt_distribution {
            let pct = if total_stmts > 0 {
                (*count as f64 / total_stmts as f64) * 100.0
            } else {
                0.0
            };
            if *count > 0 {
                writeln!(f, "  {kind:<20} {count:>6}  ({pct:.1}%)")?;
            } else {
                writeln!(f, "  {kind:<20}      -  (not generated)")?;
            }
        }

        // Expression distribution
        let expr_counts = self.coverage.expr_counts();
        let mut expr_distribution: Vec<_> = ExprKind::iter()
            .map(|k| (k, *expr_counts.get(&k).unwrap_or(&0)))
            .collect();
        expr_distribution.sort_by(|a, b| b.1.cmp(&a.1));

        writeln!(f, "\n--- Expression Distribution ---")?;
        for (kind, count) in &expr_distribution {
            let pct = if total_exprs > 0 {
                (*count as f64 / total_exprs as f64) * 100.0
            } else {
                0.0
            };
            if *count > 0 {
                writeln!(f, "  {kind:<20} {count:>6}  ({pct:.1}%)")?;
            } else {
                writeln!(f, "  {kind:<20}      -  (not generated)")?;
            }
        }

        // Tree section
        let tree_root = build_coverage_tree(&self.coverage.by_origin);

        match self.tree_mode {
            TreeMode::Full => {
                writeln!(f, "\n--- Generation Tree ---")?;
                fmt_coverage_tree(&tree_root, f)?;
            }
            TreeMode::Simplified => {
                writeln!(f, "\n--- Origin Distribution (simplified) ---")?;
                // Compute flattened origin -> subtree expr counts from the tree.
                let mut origin_exprs: HashMap<Origin, HashMap<ExprKind, usize>> = HashMap::new();
                fn collect_origins(
                    name: Option<Origin>,
                    node: &CoverageTreeNode,
                    acc: &mut HashMap<Origin, HashMap<ExprKind, usize>>,
                ) {
                    if let Some(origin) = name {
                        let mut subtree_counts = HashMap::new();
                        node.collect_all_expr_counts(&mut subtree_counts);
                        let entry = acc.entry(origin).or_default();
                        for (kind, count) in subtree_counts {
                            *entry.entry(kind).or_default() += count;
                        }
                    }
                    for (child_name, child) in &node.children {
                        let child_origin =
                            Origin::iter().find(|o| o.to_string() == *child_name);
                        collect_origins(child_origin, child, acc);
                    }
                }
                collect_origins(Some(Origin::Root), &tree_root, &mut origin_exprs);

                let mut entered = HashSet::new();
                tree_root.collect_entered_origins(Some(Origin::Root), &mut entered);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_origin_path() {
        let mut path = OriginPath::new();
        assert_eq!(path.current(), Origin::Root);

        path.push(Origin::Select);
        assert_eq!(path.current(), Origin::Select);
        assert_eq!(path.to_string(), "Root > Select");

        path.push(Origin::Where);
        assert_eq!(path.to_string(), "Root > Select > Where");

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
        assert_eq!(coverage.expr_counts().get(&ExprKind::ColumnRef), Some(&2));
    }

    #[test]
    fn test_coverage_report() {
        let mut coverage = Coverage::new();
        let path = OriginPath::new();

        coverage.record_expr(&path, ExprKind::ColumnRef);
        coverage.record_expr(&path, ExprKind::Literal);
        coverage.record_stmt(StmtKind::Select);

        let report = coverage.report();
        assert_eq!(report.total_exprs(), 2);
        assert_eq!(report.total_stmts(), 1);
        assert_eq!(report.expr_variants_hit(), 2);
        assert_eq!(report.stmt_variants_hit(), 1);
        // Missing variants don't include ColumnRef/Literal (they were generated)
        assert!(!report.missing_expr_kinds().contains(&ExprKind::ColumnRef));
        assert!(!report.missing_expr_kinds().contains(&ExprKind::Literal));
        assert!(report.missing_expr_kinds().contains(&ExprKind::BinaryOp));
        // Root was hit (path defaults to Root)
        assert_eq!(report.origin_variants_hit(), 1);
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
        assert_eq!(
            coverage1.expr_counts().get(&ExprKind::ColumnRef),
            Some(&2)
        );
    }
}
