use super::{cost::RowCountEstimate, cost_params::CostModelParams, AvailableIndexes};
use crate::alloc::TursoIteratorExt;
use crate::translate::expr::comparison_affinity;
use crate::{
    schema::{Column, Index, Schema},
    translate::{
        collate::{get_collseq_from_expr, CollationSeq},
        expr::{
            as_binary_components, get_expr_affinity, truth_test_rhs, unwrap_parens, walk_expr,
            walk_expr_mut, WalkControl,
        },
        expression_index::normalize_expr_for_index_matching,
        plan::{
            is_non_null_literal, HirPlannedSource, HirWhereTerm, JoinOrderMember, JoinedTable,
            NonFromClauseSubquery, Plan, PredicateExpr, SubqueryState, TableReferences, WhereTerm,
        },
        planner::{
            rewrite_between_exprs, table_mask_from_expr, table_mask_from_hir_scope, HirFromScope,
            TableMask, ROWID_STRS,
        },
        semantic::hir,
        Resolver,
    },
    util::exprs_are_equivalent,
    vdbe::affinity::Affinity,
    Result,
};
use crate::{turso_assert, turso_debug_assert};
use smallvec::SmallVec;
use std::{collections::VecDeque, sync::Arc};
use turso_ext::{ConstraintInfo, ConstraintOp};
use turso_parser::ast::{self, SortOrder, TableInternalId};

/// Represents a single condition derived from a `WHERE` clause term
/// that constrains a specific column of a table.
///
/// Constraints are precomputed for each table involved in a query. They are used
/// during query optimization to estimate the cost of different access paths (e.g., using an index)
/// and to determine the optimal join order. A constraint can only be applied if all tables
/// referenced in its expression (other than the constrained table itself) are already
/// available in the current join context, i.e. on the left side in the join order
/// relative to the table. Expression indexes are represented by leaving `table_col_pos` empty
/// and storing the indexed expression in `expr`.
#[derive(Debug, Clone)]
pub struct Constraint<E = ast::Expr> {
    /// The position of the original `WHERE` clause term this constraint derives from,
    /// and which side of the [ast::Expr::Binary] comparison contains the expression
    /// that constrains the column.
    /// E.g. in SELECT * FROM t WHERE t.x = 10, the constraint is (0, BinaryExprSide::Rhs)
    /// because the RHS '10' is the constraining expression.
    ///
    /// This is tracked so we can:
    ///
    /// 1. Extract the constraining expression for use in an index seek key, and
    /// 2. Remove the relevant binary expression from the WHERE clause, if used as an index seek key.
    pub where_clause_pos: (usize, BinaryExprSide),
    /// The comparison operator (e.g., `=`, `>`, `<`) used in the constraint.
    pub operator: ConstraintOperator,
    /// The zero-based index of the constrained column within the table's schema.
    /// None for expression-index constraints.
    pub table_col_pos: Option<usize>,
    /// The expression constrained by this constraint, if it is not a simple column reference.
    pub expr: Option<E>,
    /// For multi-index scan branches: the constraining expression and its affinity.
    /// When set, `get_constraining_expr` uses this instead of looking up in where_clause.
    /// This is needed because multi-index branches come from sub-expressions of an OR/AND,
    /// not directly from a top-level WHERE term.
    pub constraining_expr: Option<(ast::Operator, E, Affinity)>,
    /// A bitmask representing the set of tables that appear on the *constraining* side
    /// of the comparison expression. For example, in SELECT * FROM t1,t2,t3 WHERE t1.x = t2.x + t3.x,
    /// the lhs_mask contains t2 and t3. Thus, this constraint can only be used if t2 and t3
    /// have already been joined (i.e. are on the left side of the join order relative to t1).
    pub lhs_mask: TableMask,
    /// An estimated selectivity factor (0.0 to 1.0) indicating the fraction of rows
    /// expected to satisfy this constraint. Used for cost and cardinality estimation.
    pub selectivity: f64,
    /// Whether the constraint can participate in range-seek index matching
    /// (the eq/lower_bound/upper_bound model in RangeConstraintRef).
    /// False for IN constraints (which use a separate multi-value seek path)
    /// and for collation mismatches.
    pub usable: bool,
    /// Whether this constraint references the implicit rowid (tables without an INTEGER PRIMARY KEY alias).
    /// When true and `table_col_pos` is None, this constraint targets the rowid pseudo-column.
    pub is_rowid: bool,
    /// The constraint's resolved comparison affinity, as defined by SQLite's
    /// `comparisonAffinity` in `expr.c`. Cached at construction time so the
    /// `sqlite3IndexAffinityOk` check can run without re-resolving the
    /// WhereTerm at every index-selection callsite.
    ///
    /// `None` for forms whose comparison affinity has no single resolved value:
    /// FTS MATCH and virtual-table push-downs (operators outside SQLite's
    /// comparison set), and row-value IN (`(a,b) IN (...)`, which SQLite
    /// handles per-LHS-column via `sqlite3VectorFieldSubexpr` — Turso does
    /// not yet plumb per-column affinity into the index-selection path, so
    /// such constraints fall through to scans).
    pub comparison_affinity: Option<Affinity>,
    /// Comparison collation already chosen by semantic analysis.
    ///
    /// HIR constraints carry this directly. Legacy AST constraints leave it
    /// empty and keep their current expression lookup until that path is
    /// removed.
    pub comparison_collation: Option<CollationSeq>,
    /// Whether this constraint's seek key can be NULL and still match rows.
    /// True only for `IS` whose constraining value is not known to be
    /// non-NULL. `a IS 5` gets false: a literal 5 is never NULL, so the
    /// constraint filters exactly like `a = 5`. `a IS NULL`, `a IS ?` and
    /// `a IS other.col` get true — an index (even a UNIQUE one) can store
    /// many NULL keys, so such a constraint can match many rows and its cost
    /// and row estimates must not be taken from equality statistics.
    pub null_matching: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConstraintOperator {
    AstNativeOperator(ast::Operator),
    Like { not: bool },
    In { not: bool, estimated_values: f64 },
}

impl ConstraintOperator {
    pub fn as_ast_operator(&self) -> Option<ast::Operator> {
        let ConstraintOperator::AstNativeOperator(op) = self else {
            return None;
        };
        Some(*op)
    }
}

impl From<ast::Operator> for ConstraintOperator {
    fn from(op: ast::Operator) -> Self {
        ConstraintOperator::AstNativeOperator(op)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryExprSide {
    Lhs,
    Rhs,
}

impl Constraint<ast::Expr> {
    /// Get the constraining expression and operator, e.g. ('>=', '2+3') from 't.x >= 2+3'
    pub fn get_constraining_expr(
        &self,
        where_clause: &[WhereTerm],
        referenced_tables: Option<&TableReferences>,
        resolver: Option<&Resolver>,
    ) -> (ast::Operator, ast::Expr, Affinity) {
        // For multi-index branches, use the pre-computed constraining expression
        if let Some(constraining) = &self.constraining_expr {
            return constraining.clone();
        }

        let (idx, side) = self.where_clause_pos;
        let where_term = &where_clause[idx];
        let Ok(Some((lhs, op, rhs))) = as_binary_components(&where_term.expr) else {
            panic!("Expected a valid binary expression");
        };
        let mut affinity = Affinity::Blob;
        if op.as_ast_operator().is_some_and(|op| op.is_comparison()) {
            // The resolver matters here: a scalar subquery's affinity is only
            // known through it. Without it, `text_col < (SELECT int_col ...)`
            // would take the TEXT side's affinity and the seek would compare
            // the integer as text.
            affinity = comparison_affinity(lhs, rhs, referenced_tables, resolver);
        }

        if side == BinaryExprSide::Lhs {
            if affinity.expr_needs_no_affinity_change(lhs) {
                affinity = Affinity::Blob;
            }
            (
                self.operator
                    .as_ast_operator()
                    .expect("expected an ast operator because as_binary_components returned Some"),
                lhs.clone(),
                affinity,
            )
        } else {
            if affinity.expr_needs_no_affinity_change(rhs) {
                affinity = Affinity::Blob;
            }
            (
                self.operator
                    .as_ast_operator()
                    .expect("expected an ast operator because as_binary_components returned Some"),
                rhs.clone(),
                affinity,
            )
        }
    }

    pub fn get_constraining_expr_ref<'a>(&self, where_clause: &'a [WhereTerm]) -> &'a ast::Expr {
        let (idx, side) = self.where_clause_pos;
        let where_term = &where_clause[idx];
        let Ok(Some((lhs, _, rhs))) = as_binary_components(&where_term.expr) else {
            panic!("Expected a valid binary expression");
        };
        if side == BinaryExprSide::Lhs {
            lhs
        } else {
            rhs
        }
    }
}

impl<E> Constraint<E> {
    /// Returns true when an index column with affinity `idx_aff` can satisfy
    /// this constraint per SQLite's `sqlite3IndexAffinityOk`. Constraints
    /// whose form has no SQLite-defined comparison affinity (FTS MATCH,
    /// virtual-table push-downs, row-value IN) carry `None` and bypass the
    /// check — those paths handle types themselves.
    pub fn satisfies_index_affinity(&self, idx_aff: Affinity) -> bool {
        match self.comparison_affinity {
            Some(comparison_aff) => idx_aff.index_affinity_ok(comparison_aff),
            None => true,
        }
    }

    /// Whether this constraint can drive an index seek on its target column.
    /// Composes the `usable`/`table_col_pos` gates with the affinity check
    /// against the column at `table_col_pos` in `columns` (set `is_strict`
    /// only for STRICT tables; subqueries pass `false`).
    pub fn can_drive_index_seek(&self, columns: &[Column], is_strict: bool) -> bool {
        if !self.usable {
            return false;
        }
        let Some(pos) = self.table_col_pos else {
            return false;
        };
        let col = columns.get(pos).unwrap_or_else(|| {
            unreachable!("constraint table_col_pos {pos} out of bounds for {columns:?}")
        });
        self.satisfies_index_affinity(col.affinity_with_strict(is_strict))
    }
}

#[derive(Debug, Clone)]
/// A reference to a [Constraint] in a [TableConstraints].
///
/// This is used to track which constraints may be used as an index seek key.
pub struct ConstraintRef {
    /// The position of the constraint in the [TableConstraints::constraints] vector.
    pub constraint_vec_pos: usize,
    /// The position of the constrained column in the index. Always 0 for rowid indices.
    pub index_col_pos: usize,
    /// The sort order of the constrained column in the index. Always ascending for rowid indices.
    pub sort_order: SortOrder,
    /// Where the constrained index column stores NULLs in its forward layout.
    pub nulls_order: ast::NullsOrder,
}

/// A collection of [ConstraintRef]s for a given index, or if index is None, for the table's rowid index.
/// For example, given a table `T (x,y,z)` with an index `T_I (y desc,z)`, take the following query:
/// ```sql
/// SELECT * FROM T WHERE y = 10 AND z = 20;
/// ```
///
/// This will produce the following [ConstraintUseCandidate]:
///
/// ConstraintUseCandidate {
///     index: Some(T_I)
///     refs: [
///         ConstraintRef {
///             constraint_vec_pos: 0, // y = 10
///             index_col_pos: 0, // y
///             sort_order: SortOrder::Desc,
///         },
///         ConstraintRef {
///             constraint_vec_pos: 1, // z = 20
///             index_col_pos: 1, // z
///             sort_order: SortOrder::Asc,
///         },
///     ],
/// }
///
#[derive(Debug)]
pub struct PartialIndexCandidate {
    /// Fraction of table rows physically stored in the index.
    pub selectivity: f64,
    /// Query predicate terms that prove the partial-index predicate.
    pub predicate_terms: SmallVec<[usize; 4]>,
}

#[derive(Debug)]
pub struct ConstraintUseCandidate {
    /// The index that may be used to satisfy the constraints. If none, the table's rowid index is used.
    pub index: Option<Arc<Index>>,
    /// References to the constraints that may be used as an access path for the index.
    /// Refs are sorted by [ConstraintRef::index_col_pos]
    pub refs: Vec<ConstraintRef>,
    /// Facts established while accepting a partial-index candidate.
    pub partial_index: Option<PartialIndexCandidate>,
}

#[derive(Debug)]
/// A collection of [Constraint]s and their potential [ConstraintUseCandidate]s for a given table.
pub struct TableConstraints<E = ast::Expr, S = TableInternalId> {
    /// Identity of the source these constraints apply to.
    pub table_id: S,
    /// The constraints for the table, i.e. any [WhereTerm]s that reference columns from this table.
    pub constraints: Vec<Constraint<E>>,
    /// Candidates for indexes that may use the constraints to perform a lookup.
    pub candidates: Vec<ConstraintUseCandidate>,
    /// Conditions that a temporary index may use for a lookup.
    pub temporary_index_terms: SmallVec<[ConstraintRef; 4]>,
}

pub(crate) type HirConstraint = Constraint<hir::Expr>;
pub(crate) type HirTableConstraints = TableConstraints<hir::Expr, hir::SourceId>;

/// Resolved source facts needed while collecting HIR constraints.
///
/// Physical tables can use catalog statistics and produce index candidates.
/// Derived queries only produce filter selectivity for their scan.
#[derive(Clone, Copy, Debug)]
pub(crate) enum HirConstraintSource<'a> {
    BTree(&'a HirPlannedSource),
    Derived {
        source: hir::SourceId,
        row_count: RowCountEstimate,
    },
    Cte {
        source: hir::SourceId,
        row_count: RowCountEstimate,
    },
}

impl<'a> HirConstraintSource<'a> {
    fn source(self) -> hir::SourceId {
        match self {
            Self::BTree(source) => source.internal_id,
            Self::Derived { source, .. } | Self::Cte { source, .. } => source,
        }
    }

    fn btree(self) -> Option<&'a HirPlannedSource> {
        match self {
            Self::BTree(source) => Some(source),
            Self::Derived { .. } | Self::Cte { .. } => None,
        }
    }

    fn row_count(self, schema: &Schema, params: &CostModelParams) -> f64 {
        match self {
            Self::BTree(source) => schema
                .analyze_stats
                .table_stats(source.table.get_name())
                .and_then(|stats| stats.row_count)
                .unwrap_or(params.rows_per_table_fallback as u64)
                as f64,
            Self::Derived { row_count, .. } | Self::Cte { row_count, .. } => *row_count,
        }
    }

    fn rowid_alias_column(self) -> Option<usize> {
        self.btree().and_then(|source| {
            source
                .table
                .columns()
                .iter()
                .position(Column::is_rowid_alias)
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HirConstraintTarget {
    Column(hir::ColumnRef),
    RowId(hir::SourceId),
    Expression(hir::SourceId),
}

impl HirConstraintTarget {
    fn source(self) -> hir::SourceId {
        match self {
            Self::Column(column) => column.source,
            Self::RowId(source) | Self::Expression(source) => source,
        }
    }
}

#[derive(Debug)]
pub(crate) struct HirBinaryConstraintPart<'a> {
    pub(crate) target: HirConstraintTarget,
    pub(crate) operator: ConstraintOperator,
    pub(crate) side: BinaryExprSide,
    pub(crate) constrained_expr: &'a hir::Expr,
    pub(crate) constraining_expr: &'a hir::Expr,
    pub(crate) comparison_affinity: Option<Affinity>,
    pub(crate) comparison_collation: Option<CollationSeq>,
}

/// Read every resolved comparison target for one HIR source.
///
/// The side points at the constraining expression. For example,
/// `1 > items.value` returns `Lhs` and normalizes the operator to `<`. Both
/// sides are returned when both directly reference the source.
pub(crate) fn hir_binary_constraint_parts<'a>(
    expr: &'a hir::Expr,
    source: hir::SourceId,
) -> SmallVec<[HirBinaryConstraintPart<'a>; 2]> {
    let mut parts = SmallVec::new();
    let hir::Expr::Binary {
        lhs,
        operator,
        rhs,
        comparison: Some(comparison),
        ..
    } = expr
    else {
        return parts;
    };

    let target = |expr: &hir::Expr| match expr {
        hir::Expr::Column(column) if column.source == source => {
            Some(HirConstraintTarget::Column(*column))
        }
        hir::Expr::RowId(rowid_source) if *rowid_source == source => {
            Some(HirConstraintTarget::RowId(source))
        }
        hir::Expr::Literal(_)
        | hir::Expr::Parameter(_)
        | hir::Expr::Column(_)
        | hir::Expr::RowId(_)
        | hir::Expr::Output(_)
        | hir::Expr::Subquery(_)
        | hir::Expr::Raise { .. } => None,
        _ => Some(HirConstraintTarget::Expression(source)),
    };

    let component = comparison.components.first();
    let affinity = component.map(|component| component.affinity);
    let collation = component
        .and_then(|component| component.collation.as_ref())
        .map(|collation| *collation.value());
    let operator = ConstraintOperator::from(*operator);

    if let Some(target) = target(lhs) {
        parts.push(HirBinaryConstraintPart {
            target,
            operator,
            side: BinaryExprSide::Rhs,
            constrained_expr: lhs,
            constraining_expr: rhs,
            comparison_affinity: affinity,
            comparison_collation: collation,
        });
    }
    if let Some(target) = target(rhs) {
        parts.push(HirBinaryConstraintPart {
            target,
            operator: opposite_cmp_op(operator),
            side: BinaryExprSide::Lhs,
            constrained_expr: rhs,
            constraining_expr: lhs,
            comparison_affinity: affinity,
            comparison_collation: collation,
        });
    }
    parts
}

fn hir_truth_test_rhs(expr: &hir::Expr) -> Option<bool> {
    match expr {
        hir::Expr::Literal(ast::Literal::True) => Some(true),
        hir::Expr::Literal(ast::Literal::False) => Some(false),
        hir::Expr::Collate { expr, .. } => hir_truth_test_rhs(expr),
        _ => None,
    }
}

fn hir_is_non_null_literal(expr: &hir::Expr) -> bool {
    matches!(
        expr,
        hir::Expr::Literal(
            ast::Literal::Numeric(_)
                | ast::Literal::String(_)
                | ast::Literal::Blob(_)
                | ast::Literal::True
                | ast::Literal::False
        )
    )
}

#[allow(clippy::too_many_arguments)]
fn hir_binary_constraints_for_term(
    document: &hir::HirDocument,
    from: &impl HirFromScope,
    term_position: usize,
    term: &HirWhereTerm,
    source_definition: &hir::Source,
    constraint_source: HirConstraintSource<'_>,
    schema: &Schema,
    params: &CostModelParams,
) -> Result<SmallVec<[HirConstraint; 2]>> {
    let source = source_definition.id;
    let mut constraints = SmallVec::new();
    let hir::Expr::Binary { operator, rhs, .. } = &term.expr else {
        return Ok(constraints);
    };
    if *operator == ast::Operator::Is && hir_truth_test_rhs(rhs).is_some() {
        return Ok(constraints);
    }
    if term
        .from_outer_join
        .is_some_and(|outer_source| outer_source != source)
    {
        return Ok(constraints);
    }

    for part in hir_binary_constraint_parts(&term.expr, source) {
        if matches!(part.target, HirConstraintTarget::Expression(_)) {
            let target_mask = table_mask_from_hir_scope(document, from, part.constrained_expr)?;
            let target_position = from.source_position(part.target.source()).ok_or_else(|| {
                crate::LimboError::InternalError(format!(
                    "HIR constraint source {} is absent from FROM",
                    part.target.source()
                ))
            })?;
            if !target_mask.get(target_position) || target_mask.count() != 1 {
                continue;
            }
        }
        let is_op = part.operator.as_ast_operator() == Some(ast::Operator::Is);
        let null_matching = is_op && !hir_is_non_null_literal(part.constraining_expr);
        let usable = term.from_outer_join == Some(source)
            || if is_op {
                !from.outer_join_may_null_extend(source)
            } else {
                !from.full_join_may_null_extend(source)
            };

        let (table_col_pos, is_rowid) = match part.target {
            HirConstraintTarget::Column(column) => (Some(column.column), false),
            HirConstraintTarget::RowId(_) => (None, true),
            HirConstraintTarget::Expression(_) => (None, false),
        };
        let selectivity = match constraint_source {
            HirConstraintSource::BTree(table) => {
                let column = table_col_pos.map(|position| &table.table.columns()[position]);
                let index = table_col_pos.and_then(|position| {
                    hir_selectivity_index_for_column(schema, table, source_definition, position)
                });
                hir_estimate_constraint_selectivity(
                    schema,
                    table,
                    column,
                    part.operator,
                    null_matching,
                    index,
                    params,
                    is_rowid,
                )
            }
            HirConstraintSource::Derived { .. } | HirConstraintSource::Cte { .. } => {
                hir_estimate_scan_constraint_selectivity(part.operator, null_matching, params)
            }
        };

        constraints.push(HirConstraint {
            where_clause_pos: (term_position, part.side),
            operator: part.operator,
            table_col_pos,
            expr: None,
            constraining_expr: None,
            lhs_mask: table_mask_from_hir_scope(document, from, part.constraining_expr)?,
            selectivity,
            usable,
            is_rowid,
            comparison_affinity: part.comparison_affinity,
            comparison_collation: part.comparison_collation,
            null_matching,
        });
    }
    Ok(constraints)
}

fn hir_estimate_constraint_selectivity(
    schema: &Schema,
    table: &HirPlannedSource,
    column: Option<&Column>,
    operator: ConstraintOperator,
    null_matching: bool,
    index: Option<&Index>,
    params: &CostModelParams,
    is_rowid: bool,
) -> f64 {
    estimate_constraint_selectivity_for_table(
        schema,
        table.table.get_name(),
        column,
        operator,
        null_matching,
        index,
        params,
        is_rowid,
    )
}

fn hir_estimate_scan_constraint_selectivity(
    operator: ConstraintOperator,
    null_matching: bool,
    params: &CostModelParams,
) -> f64 {
    let operator = if operator.as_ast_operator() == Some(ast::Operator::Is) && !null_matching {
        ConstraintOperator::from(ast::Operator::Equals)
    } else {
        operator
    };
    match operator {
        ConstraintOperator::AstNativeOperator(ast::Operator::Equals) => params.sel_eq_unindexed,
        ConstraintOperator::AstNativeOperator(
            ast::Operator::Greater
            | ast::Operator::GreaterEquals
            | ast::Operator::Less
            | ast::Operator::LessEquals,
        ) => params.sel_range,
        ConstraintOperator::AstNativeOperator(ast::Operator::Is) => params.sel_is_null,
        ConstraintOperator::AstNativeOperator(ast::Operator::IsNot) => params.sel_is_not_null,
        ConstraintOperator::Like { not: false } => params.sel_like,
        ConstraintOperator::Like { not: true } => params.sel_not_like,
        ConstraintOperator::In {
            not,
            estimated_values,
        } => estimate_in_selectivity(estimated_values, params.rows_per_table_fallback, not),
        _ => params.sel_other,
    }
}

#[allow(clippy::too_many_arguments)]
fn estimate_constraint_selectivity_for_table(
    schema: &Schema,
    table_name: &str,
    column: Option<&Column>,
    operator: ConstraintOperator,
    null_matching: bool,
    index: Option<&Index>,
    params: &CostModelParams,
    is_rowid: bool,
) -> f64 {
    // `a IS 5` filters exactly like `a = 5` because the key is never NULL.
    // Only NULL-matching `IS` keeps its less-selective estimate.
    let operator = if operator.as_ast_operator() == Some(ast::Operator::Is) && !null_matching {
        ConstraintOperator::from(ast::Operator::Equals)
    } else {
        operator
    };
    estimate_selectivity(
        schema, table_name, column, index, operator, params, is_rowid,
    )
}

fn hir_selectivity_index_for_column<'a>(
    schema: &Schema,
    table: &HirPlannedSource,
    source: &'a hir::Source,
    column_pos: usize,
) -> Option<&'a Index> {
    let table_stats = schema.analyze_stats.table_stats(table.table.get_name());
    source
        .index_expressions
        .iter()
        .map(|metadata| metadata.index.value())
        .filter(|index| {
            index.index_method.is_none()
                && index.column_table_pos_to_index_pos(column_pos) == Some(0)
        })
        .find(|index| {
            if index.unique && index.columns.len() == 1 {
                return true;
            }
            let Some(table_stats) = table_stats else {
                return true;
            };
            table_stats
                .index_stats
                .get(&index.name)
                .is_some_and(|stats| {
                    matches!(
                        (stats.total_rows, stats.avg_rows_per_distinct_prefix.first()),
                        (Some(total), Some(&average)) if total > 0 && average > 0
                    )
                })
        })
}

fn hir_partial_index_selectivity(
    predicate: &hir::Expr,
    table: &HirPlannedSource,
    source: &hir::Source,
    schema: &Schema,
    params: &CostModelParams,
) -> f64 {
    let resolve_side = |expression: &hir::Expr| {
        let (column, is_rowid) = match expression {
            hir::Expr::Column(column) if column.source == source.id => (Some(column.column), false),
            hir::Expr::RowId(row_source) if *row_source == source.id => (None, true),
            _ => return None,
        };
        let physical_column = column.map(|position| &table.table.columns()[position]);
        Some((physical_column, column, is_rowid))
    };
    let leaf = |lhs: &hir::Expr, rhs: &hir::Expr, operator: ConstraintOperator| {
        let (column, column_position, is_rowid) = resolve_side(lhs)
            .or_else(|| resolve_side(rhs))
            .unwrap_or((None, None, false));
        let index = column_position
            .and_then(|position| hir_selectivity_index_for_column(schema, table, source, position));
        let null_matching = operator.as_ast_operator() == Some(ast::Operator::Is)
            && !(hir_is_non_null_literal(lhs) || hir_is_non_null_literal(rhs));
        hir_estimate_constraint_selectivity(
            schema,
            table,
            column,
            operator,
            null_matching,
            index,
            params,
            is_rowid,
        )
    };

    predicate.fold(&mut |expression, children: &[f64]| match expression {
        hir::Expr::Binary {
            operator: ast::Operator::And,
            ..
        } => {
            let [left, right] = children else {
                unreachable!("binary AND has two children")
            };
            left * right
        }
        hir::Expr::Binary {
            operator: ast::Operator::Or,
            ..
        } => {
            let [left, right] = children else {
                unreachable!("binary OR has two children")
            };
            (left + right - left * right).min(1.0)
        }
        hir::Expr::Binary {
            operator: ast::Operator::Is,
            rhs,
            ..
        } if matches!(rhs.as_ref(), hir::Expr::Literal(ast::Literal::Null)) => params.sel_is_null,
        hir::Expr::Binary {
            operator: ast::Operator::IsNot,
            rhs,
            ..
        } if matches!(rhs.as_ref(), hir::Expr::Literal(ast::Literal::Null)) => {
            params.sel_is_not_null
        }
        hir::Expr::Binary {
            lhs, operator, rhs, ..
        } => leaf(lhs, rhs, (*operator).into()),
        hir::Expr::IsNull(_) => params.sel_is_null,
        hir::Expr::NotNull(_) => params.sel_is_not_null,
        hir::Expr::Between { negated, .. } => {
            if *negated {
                1.0 - params.sel_range
            } else {
                params.sel_range
            }
        }
        hir::Expr::InList {
            lhs,
            negated,
            values,
            ..
        } => leaf(
            lhs,
            lhs,
            ConstraintOperator::In {
                not: *negated,
                estimated_values: values.len() as f64,
            },
        ),
        hir::Expr::Like { negated, .. } => {
            if *negated {
                params.sel_not_like
            } else {
                params.sel_like
            }
        }
        hir::Expr::Unary {
            operator: ast::UnaryOperator::Not,
            ..
        } => {
            let [value] = children else {
                unreachable!("unary NOT has one child")
            };
            1.0 - value
        }
        _ => params.sel_other,
    })
}

fn hir_constrained_expr<'a>(
    constraint: &HirConstraint,
    where_clause: &'a [HirWhereTerm],
) -> Result<&'a hir::Expr> {
    let (term_position, constraining_side) = constraint.where_clause_pos;
    let term = where_clause.get(term_position).ok_or_else(|| {
        crate::LimboError::InternalError(format!(
            "HIR constraint WHERE position {term_position} is out of bounds"
        ))
    })?;
    let hir::Expr::Binary { lhs, rhs, .. } = &term.expr else {
        return Err(crate::LimboError::InternalError(format!(
            "HIR constraint WHERE position {term_position} is not binary"
        )));
    };
    Ok(match constraining_side {
        BinaryExprSide::Lhs => rhs,
        BinaryExprSide::Rhs => lhs,
    })
}

fn hir_index_expressions<'a>(
    source: &'a hir::Source,
    index: &Index,
) -> Result<Option<&'a hir::IndexExpressions>> {
    let expressions = source
        .index_expressions
        .iter()
        .find(|expressions| std::ptr::eq(expressions.index.value(), index));
    if expressions.is_none() && matches!(source.index_coverage, hir::IndexCoverage::Complete { .. })
    {
        return Err(crate::LimboError::InternalError(format!(
            "source {} is missing complete metadata for index {}",
            source.id, index.name
        )));
    }
    Ok(expressions)
}

fn hir_partial_index_predicate_terms(
    from: &impl HirFromScope,
    source: hir::SourceId,
    predicate: &hir::Expr,
    query_where_clause: &[HirWhereTerm],
) -> Option<SmallVec<[usize; 4]>> {
    let full_join = from.full_join_may_null_extend(source);
    let outer_join = from.outer_join_may_null_extend(source);
    let can_use_query_term =
        |term: &HirWhereTerm| !full_join && (!outer_join || term.from_outer_join == Some(source));
    let mut matched_terms = SmallVec::new();
    for index_conjunct in predicate.conjuncts() {
        let (term_position, _) = query_where_clause.iter().enumerate().find(|(_, term)| {
            can_use_query_term(term) && index_conjunct.equivalent_for_index(&term.expr)
        })?;
        if !matched_terms.contains(&term_position) {
            matched_terms.push(term_position);
        }
    }
    Some(matched_terms)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn hir_binary_constraints_for_source(
    document: &hir::HirDocument,
    from: &impl HirFromScope,
    where_clause: &[HirWhereTerm],
    constraint_source: HirConstraintSource<'_>,
    schema: &Schema,
    params: &CostModelParams,
) -> Result<Vec<HirConstraint>> {
    let source_id = constraint_source.source();
    let source = document.source(source_id).ok_or_else(|| {
        crate::LimboError::InternalError(format!(
            "missing HIR source {} for constraint planning",
            source_id
        ))
    })?;
    let mut constraints = Vec::new();
    for (term_position, term) in where_clause.iter().enumerate() {
        constraints.extend(hir_binary_constraints_for_term(
            document,
            from,
            term_position,
            term,
            source,
            constraint_source,
            schema,
            params,
        )?);
    }
    constraints.sort_by_key(|constraint| !is_equality_operator(constraint.operator));
    Ok(constraints)
}

fn hir_scalar_comparison(
    comparison: &hir::ComparisonSemantics,
) -> Option<(Affinity, Option<CollationSeq>)> {
    let [component] = comparison.components.as_slice() else {
        return None;
    };
    if component.array {
        return None;
    }
    Some((
        component.affinity,
        component
            .collation
            .as_ref()
            .map(|collation| *collation.value()),
    ))
}

fn hir_in_list_comparison(
    values: &[hir::Expr],
    comparisons: &[hir::ComparisonSemantics],
) -> Result<Option<(Affinity, Option<CollationSeq>)>> {
    if comparisons.len() != values.len() {
        return Err(crate::LimboError::InternalError(format!(
            "HIR IN list has {} values but {} comparisons",
            values.len(),
            comparisons.len()
        )));
    }
    let Some(first) = comparisons.first().and_then(hir_scalar_comparison) else {
        return Ok(None);
    };
    Ok(comparisons
        .iter()
        .all(|comparison| hir_scalar_comparison(comparison) == Some(first))
        .then_some(first))
}

#[allow(clippy::too_many_arguments)]
fn hir_in_list_constraint_for_term(
    document: &hir::HirDocument,
    from: &impl HirFromScope,
    term_position: usize,
    term: &HirWhereTerm,
    constraint_source: HirConstraintSource<'_>,
    schema: &Schema,
    params: &CostModelParams,
) -> Result<Option<HirConstraint>> {
    let source = constraint_source.source();
    if term
        .from_outer_join
        .is_some_and(|outer_source| outer_source != source)
    {
        return Ok(None);
    }
    let hir::Expr::InList {
        lhs,
        negated,
        values,
        comparisons,
    } = &term.expr
    else {
        return Ok(None);
    };
    let Some((comparison_affinity, comparison_collation)) =
        hir_in_list_comparison(values, comparisons)?
    else {
        return Ok(None);
    };

    let rowid_alias_column = constraint_source.rowid_alias_column();
    let (table_col_pos, is_rowid) = match lhs.as_ref() {
        hir::Expr::Column(column) if column.source == source => (
            Some(column.column),
            rowid_alias_column == Some(column.column),
        ),
        hir::Expr::RowId(rowid_source) if *rowid_source == source => (rowid_alias_column, true),
        _ => return Ok(None),
    };

    let mut rhs_mask = TableMask::default();
    for value in values {
        rhs_mask.union_with(&table_mask_from_hir_scope(document, from, value)?)?;
    }
    let estimated_values = values.len() as f64;
    let row_count = constraint_source.row_count(schema, params);

    Ok(Some(HirConstraint {
        where_clause_pos: (term_position, BinaryExprSide::Rhs),
        operator: ConstraintOperator::In {
            not: *negated,
            estimated_values,
        },
        table_col_pos,
        expr: None,
        constraining_expr: None,
        lhs_mask: rhs_mask,
        selectivity: estimate_in_selectivity(estimated_values, row_count, *negated),
        usable: false,
        is_rowid,
        comparison_affinity: Some(comparison_affinity),
        comparison_collation,
        null_matching: false,
    }))
}

#[allow(clippy::too_many_arguments)]
fn hir_in_query_constraint_for_term(
    document: &hir::HirDocument,
    term_position: usize,
    term: &HirWhereTerm,
    constraint_source: HirConstraintSource<'_>,
    schema: &Schema,
    params: &CostModelParams,
    query_output_rows: &dyn Fn(hir::QueryId) -> Option<f64>,
) -> Result<Option<HirConstraint>> {
    let source = constraint_source.source();
    if term
        .from_outer_join
        .is_some_and(|outer_source| outer_source != source)
    {
        return Ok(None);
    }
    let hir::Expr::Subquery(hir::SubqueryExpr::In {
        lhs,
        query,
        negated,
        comparison,
    }) = &term.expr
    else {
        return Ok(None);
    };
    let query_definition = document.query(*query).ok_or_else(|| {
        crate::LimboError::InternalError(format!(
            "HIR IN constraint references missing query {query}"
        ))
    })?;
    if !query_definition.captures.is_empty() {
        return Ok(None);
    }
    let Some((comparison_affinity, comparison_collation)) = hir_scalar_comparison(comparison)
    else {
        return Ok(None);
    };

    let rowid_alias_column = constraint_source.rowid_alias_column();
    let (table_col_pos, is_rowid) = match lhs.as_ref() {
        hir::Expr::Column(column) if column.source == source => (
            Some(column.column),
            rowid_alias_column == Some(column.column),
        ),
        hir::Expr::RowId(rowid_source) if *rowid_source == source => (rowid_alias_column, true),
        _ => return Ok(None),
    };

    let row_count = constraint_source.row_count(schema, params);
    // The HIR owns query meaning, while its row estimate remains planner state.
    // Keep the existing cap and fallback when that plan is not available yet.
    let estimated_values = query_output_rows(*query)
        .map(|rows| rows.clamp(0.0, row_count.sqrt().max(1.0)))
        .unwrap_or_else(|| params.in_subquery_rows.min(row_count));

    Ok(Some(HirConstraint {
        where_clause_pos: (term_position, BinaryExprSide::Rhs),
        operator: ConstraintOperator::In {
            not: *negated,
            estimated_values,
        },
        table_col_pos,
        expr: None,
        constraining_expr: None,
        lhs_mask: TableMask::default(),
        selectivity: estimate_in_selectivity(estimated_values, row_count, *negated),
        usable: false,
        is_rowid,
        comparison_affinity: Some(comparison_affinity),
        comparison_collation,
        null_matching: false,
    }))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn hir_constraints_for_source(
    document: &hir::HirDocument,
    from: &impl HirFromScope,
    where_clause: &[HirWhereTerm],
    constraint_source: HirConstraintSource<'_>,
    schema: &Schema,
    params: &CostModelParams,
    query_output_rows: &dyn Fn(hir::QueryId) -> Option<f64>,
) -> Result<HirTableConstraints> {
    let source = constraint_source.source();
    let source_definition = document.source(source).ok_or_else(|| {
        crate::LimboError::InternalError(format!(
            "missing HIR source {source} for constraint planning"
        ))
    })?;
    let mut constraints = hir_binary_constraints_for_source(
        document,
        from,
        where_clause,
        constraint_source,
        schema,
        params,
    )?;
    for (term_position, term) in where_clause.iter().enumerate() {
        if let Some(constraint) = hir_in_list_constraint_for_term(
            document,
            from,
            term_position,
            term,
            constraint_source,
            schema,
            params,
        )? {
            constraints.push(constraint);
        }
        if let Some(constraint) = hir_in_query_constraint_for_term(
            document,
            term_position,
            term,
            constraint_source,
            schema,
            params,
            query_output_rows,
        )? {
            constraints.push(constraint);
        }
    }
    let Some(table) = constraint_source.btree() else {
        return Ok(HirTableConstraints {
            table_id: source,
            constraints,
            candidates: Vec::new(),
            temporary_index_terms: SmallVec::new(),
        });
    };
    let mut candidates = Vec::new();
    for expressions in &source_definition.index_expressions {
        let index = expressions.index.value();
        if index.index_method.is_some() {
            continue;
        }
        let partial_index = if index.where_clause.is_some() {
            let predicate = expressions.predicate.as_ref().ok_or_else(|| {
                crate::LimboError::InternalError(format!(
                    "HIR metadata for partial index {} has no predicate",
                    index.name
                ))
            })?;
            let Some(predicate_terms) =
                hir_partial_index_predicate_terms(from, source, predicate, where_clause)
            else {
                continue;
            };
            Some(PartialIndexCandidate {
                selectivity: hir_partial_index_selectivity(
                    predicate,
                    table,
                    source_definition,
                    schema,
                    params,
                ),
                predicate_terms,
            })
        } else {
            None
        };
        candidates.push(ConstraintUseCandidate {
            index: Some(expressions.index.handle()),
            refs: Vec::new(),
            partial_index,
        });
    }
    let mut table_constraints = HirTableConstraints {
        table_id: source,
        constraints,
        candidates,
        temporary_index_terms: SmallVec::new(),
    };
    table_constraints.candidates.push(ConstraintUseCandidate {
        index: None,
        refs: Vec::new(),
        partial_index: None,
    });

    let rowid_alias_column = table
        .table
        .columns()
        .iter()
        .position(Column::is_rowid_alias);
    for (constraint_position, constraint) in table_constraints.constraints.iter_mut().enumerate() {
        if !constraint.usable {
            continue;
        }
        let constrained_column = constraint
            .table_col_pos
            .and_then(|position| table.table.columns().get(position));
        if matches!(
            (constraint.comparison_collation, constrained_column),
            (Some(comparison), Some(column)) if comparison != column.collation()
        ) {
            constraint.usable = false;
            continue;
        }

        if constraint.is_rowid
            || rowid_alias_column.is_some_and(|position| constraint.table_col_pos == Some(position))
        {
            table_constraints
                .candidates
                .iter_mut()
                .find(|candidate| candidate.index.is_none())
                .expect("HIR table constraints contain a rowid candidate")
                .refs
                .push(ConstraintRef {
                    constraint_vec_pos: constraint_position,
                    index_col_pos: 0,
                    sort_order: SortOrder::Asc,
                    nulls_order: ast::NullsOrder::First,
                });
        }

        for candidate in table_constraints
            .candidates
            .iter_mut()
            .filter(|candidate| candidate.index.is_some())
        {
            let index = candidate.index.as_ref().expect("candidate has an index");
            let index_column_position = match constraint.table_col_pos {
                Some(table_column_position) => {
                    index.column_table_pos_to_index_pos(table_column_position)
                }
                None if !constraint.is_rowid => {
                    let source_definition = document.source(source).ok_or_else(|| {
                        crate::LimboError::InternalError(format!(
                            "missing HIR source {source} for expression-index matching"
                        ))
                    })?;
                    let Some(expressions) =
                        hir_index_expressions(source_definition, index.as_ref())?
                    else {
                        continue;
                    };
                    let constrained_expr = hir_constrained_expr(constraint, where_clause)?;
                    expressions.columns.iter().position(|indexed_expr| {
                        indexed_expr.as_ref().is_some_and(|indexed_expr| {
                            constrained_expr.equivalent_for_index(indexed_expr)
                        })
                    })
                }
                None => None,
            };
            let Some(index_column_position) = index_column_position else {
                continue;
            };
            let index_column = &index.columns[index_column_position];
            if let Some(table_column_position) = constraint.table_col_pos {
                let constrained_column = &table.table.columns()[table_column_position];
                if constrained_column.collation() != index_column.collation.unwrap_or_default() {
                    continue;
                }
                if schema
                    .get_type_def(&constrained_column.ty_str, table.table.is_strict())
                    .is_some()
                    && constraint.operator != ast::Operator::Equals.into()
                {
                    continue;
                }
                if !constraint.satisfies_index_affinity(
                    constrained_column.affinity_with_strict(table.table.is_strict()),
                ) {
                    continue;
                }
            } else if constraint.comparison_collation.unwrap_or_default()
                != index_column.collation.unwrap_or_default()
            {
                continue;
            }
            candidate.refs.push(ConstraintRef {
                constraint_vec_pos: constraint_position,
                index_col_pos: index_column_position,
                sort_order: index_column.order,
                nulls_order: index_column.effective_nulls_order(),
            });
        }
    }

    for candidate in &mut table_constraints.candidates {
        candidate
            .refs
            .sort_by_key(|reference| reference.index_col_pos);
    }
    table_constraints.temporary_index_terms =
        automatic_index_terms(&table.table, &table_constraints)
            .into_iter()
            .filter(|reference| {
                !matches!(
                    table_constraints.constraints[reference.constraint_vec_pos]
                        .comparison_collation,
                    Some(CollationSeq::Custom(_))
                )
            })
            .collect();
    Ok(table_constraints)
}

/// Build the search terms for an automatic index.
///
/// Terms for the same table column use the same index column.
pub(super) fn automatic_index_terms<E, S>(
    table: &crate::schema::Table,
    constraints: &TableConstraints<E, S>,
) -> SmallVec<[ConstraintRef; 4]> {
    let columns = table.columns();
    let is_strict = table.is_strict();
    let usable_constraints: SmallVec<[&Constraint<E>; 4]> = constraints
        .constraints
        .iter()
        .filter(|term| term.can_drive_index_seek(columns, is_strict))
        .collect();
    let index_columns = ordered_ephemeral_key_columns(&usable_constraints);

    let mut terms: SmallVec<[ConstraintRef; 4]> = constraints
        .constraints
        .iter()
        .enumerate()
        .filter(|(_, term)| term.can_drive_index_seek(columns, is_strict))
        .filter_map(|(term_index, term)| {
            let table_col_pos = term.table_col_pos?;
            Some(ConstraintRef {
                constraint_vec_pos: term_index,
                index_col_pos: index_columns
                    .iter()
                    .position(|column| *column == table_col_pos)?,
                sort_order: SortOrder::Asc,
                nulls_order: ast::NullsOrder::First,
            })
        })
        .collect();
    terms.sort_by_key(|term| term.index_col_pos);
    terms
}

/// Return true when an expression names a custom text order.
pub(super) fn expr_uses_custom_collation(expr: &ast::Expr) -> bool {
    let mut uses_custom = false;
    walk_expr(expr, &mut |expr| -> Result<WalkControl> {
        if let ast::Expr::Collate(_, collation_name) = expr {
            uses_custom = CollationSeq::known_custom(collation_name.as_str()).is_some();
            if uses_custom {
                return Ok(WalkControl::SkipChildren);
            }
        }
        Ok(WalkControl::Continue)
    })
    .expect("reading a constraint cannot fail");
    uses_custom
}

/// Estimate selectivity for IN expressions given the number of values and table row count.
fn estimate_in_selectivity(in_list_len: f64, row_count: f64, not: bool) -> f64 {
    if not {
        // NOT IN: each value in the list excludes roughly 1/ndv of the rows.
        // Without ANALYZE stats we don't know ndv, so we use the equality
        // selectivity heuristic (sel_eq_unindexed = 0.1) per excluded value.
        // This gives NOT IN (v1,v2,v3) ≈ (1 - 0.1)^3 ≈ 0.729, which is a
        // reasonable estimate that the filter does meaningful work.
        let per_value_sel = 0.1_f64; // matches sel_eq_unindexed default
        (1.0 - per_value_sel).powf(in_list_len).max(0.01)
    } else {
        (in_list_len / row_count).min(1.0)
    }
}

/// Estimate the selectivity of a constraint based on the operator, column type, and ANALYZE stats.
///
/// When ANALYZE stats are available, we use:
/// - For unique/PK columns: 1 / row_count (one row expected per lookup)
/// - For non-unique indexed columns: uses index stats to find avg rows per distinct value
///
/// The sqlite_stat1 format stores: total_rows, avg_rows_per_key_col1, avg_rows_per_key_col1_col2, ...
/// So selectivity = avg_rows_per_key / total_rows
///
/// Falls back to hardcoded estimates when stats are unavailable.
#[allow(clippy::too_many_arguments)]
fn estimate_selectivity(
    schema: &Schema,
    table_name: &str,
    column: Option<&Column>,
    index: Option<&Index>,
    op: ConstraintOperator,
    params: &CostModelParams,
    is_rowid: bool,
) -> f64 {
    // Get ANALYZE stats for this table if available
    let table_stats = schema.analyze_stats.table_stats(table_name);
    let row_count = table_stats.and_then(|s| s.row_count).unwrap_or(0);

    match op {
        ConstraintOperator::AstNativeOperator(ast::Operator::Equals) => {
            let is_pk_or_rowid_alias =
                is_rowid || column.is_some_and(|c| c.is_rowid_alias() || c.primary_key());

            let selectivity_when_unique = if row_count > 0 {
                1.0 / row_count as f64
            } else {
                // Fallback: use hardcoded estimate based on expected table size
                1.0 / params.rows_per_table_fallback
            };

            if is_pk_or_rowid_alias {
                selectivity_when_unique
            } else if let Some(index) = index {
                // Only use unique selectivity for single-column unique indexes.
                // For composite unique indexes like tpc-h (l_orderkey, l_linenumber),
                // the first column alone is NOT unique.
                if index.unique && index.columns.len() == 1 {
                    return selectivity_when_unique;
                }
                if let Some(stats) = table_stats {
                    if let Some(idx_stat) = stats.index_stats.get(&index.name) {
                        if let (Some(total), Some(&avg_rows)) = (
                            idx_stat.total_rows,
                            idx_stat.avg_rows_per_distinct_prefix.first(),
                        ) {
                            if total > 0 && avg_rows > 0 {
                                // selectivity = avg_rows_per_key / total_rows
                                return avg_rows as f64 / total as f64;
                            }
                        }
                    }
                } else {
                    return params.sel_eq_indexed;
                }
                // Fallback: use hardcoded selectivity for non-indexed columns
                // Don't scale by row_count - keep it distinct from PK selectivity
                params.sel_eq_unindexed
            } else {
                params.sel_eq_unindexed
            }
        }
        ConstraintOperator::AstNativeOperator(ast::Operator::Greater)
        | ConstraintOperator::AstNativeOperator(ast::Operator::GreaterEquals)
        | ConstraintOperator::AstNativeOperator(ast::Operator::Less)
        | ConstraintOperator::AstNativeOperator(ast::Operator::LessEquals) => params.sel_range,
        ConstraintOperator::AstNativeOperator(ast::Operator::Is) => params.sel_is_null,
        ConstraintOperator::AstNativeOperator(ast::Operator::IsNot) => params.sel_is_not_null,
        ConstraintOperator::Like { not: false } => params.sel_like,
        ConstraintOperator::Like { not: true } => params.sel_not_like,
        ConstraintOperator::In {
            not,
            estimated_values,
        } => estimate_in_selectivity(estimated_values, row_count as f64, not),
        _ => params.sel_other,
    }
}

#[allow(clippy::too_many_arguments)]
/// Estimate selectivity for a single WHERE/ON constraint applied to `table_reference`.
fn estimate_constraint_selectivity(
    schema: &Schema,
    table_reference: &JoinedTable,
    column: Option<&Column>,
    operator: ConstraintOperator,
    null_matching: bool,
    index: Option<&Index>,
    params: &CostModelParams,
    is_rowid: bool,
) -> f64 {
    estimate_constraint_selectivity_for_table(
        schema,
        table_reference.table.get_name(),
        column,
        operator,
        null_matching,
        index,
        params,
        is_rowid,
    )
}

fn selectivity_index_for_column<'a>(
    schema: &Schema,
    table_reference: &JoinedTable,
    available_indexes: &'a AvailableIndexes,
    column_pos: usize,
) -> Option<&'a Index> {
    let table_stats = schema
        .analyze_stats
        .table_stats(table_reference.table.get_name());
    available_indexes
        .btree_indexes_for_column(table_reference.internal_id, column_pos)
        .find(|index| {
            if index.unique && index.columns.len() == 1 {
                return true;
            }
            let Some(table_stats) = table_stats else {
                return true;
            };
            table_stats
                .index_stats
                .get(&index.name)
                .is_some_and(|idx_stat| {
                    matches!(
                        (
                            idx_stat.total_rows,
                            idx_stat.avg_rows_per_distinct_prefix.first()
                        ),
                        (Some(total), Some(&avg_rows)) if total > 0 && avg_rows > 0
                    )
                })
        })
}

fn expression_matches_table(
    expr: &ast::Expr,
    table_reference: &JoinedTable,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
) -> bool {
    match table_mask_from_expr(expr, table_references, subqueries) {
        Ok(mask) => table_references
            .joined_tables()
            .iter()
            .position(|t| t.internal_id == table_reference.internal_id)
            .is_some_and(|idx| mask.get(idx) && mask.count() == 1),
        Err(_) => false,
    }
}

/// Precompute all potentially usable [Constraints] from a WHERE clause.
/// The resulting list of [TableConstraints] is then used to evaluate the best access methods for various join orders.
///
/// This method do not perform much filtering of constraints and delegate this tasks to the consumers of the method
/// Consumers must inspect [TableConstraints] and its candidates and pick best constraints for optimized access
pub fn constraints_from_where_clause(
    where_clause: &[WhereTerm],
    table_references: &TableReferences,
    available_indexes: &AvailableIndexes,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    params: &CostModelParams,
) -> Result<Vec<TableConstraints>> {
    let mut constraints = Vec::new();

    // For each table, collect all the Constraints and all potential index candidates that may use them.
    for table_reference in table_references.joined_tables() {
        let rowid_alias_column = table_reference
            .columns()
            .iter()
            .position(|c| c.is_rowid_alias());

        let mut cs = TableConstraints {
            table_id: table_reference.internal_id,
            constraints: Vec::new(),
            temporary_index_terms: SmallVec::new(),
            candidates: available_indexes
                .indexes_for_table(table_reference.internal_id)
                .map_or(Vec::new(), |indexes| {
                    indexes
                        .iter()
                        // Skip IndexMethod-based indexes (FTS, vector, etc.) - they use
                        // pattern matching rather than btree index scans
                        .filter_map(|index| {
                            if index.index_method.is_some() {
                                return None;
                            }
                            let partial_index = match index.where_clause.as_deref() {
                                Some(predicate) => Some(PartialIndexCandidate {
                                    selectivity: estimate_partial_index_where_selectivity(
                                        predicate,
                                        table_reference,
                                        schema,
                                        available_indexes,
                                        params,
                                    ),
                                    predicate_terms: partial_index_predicate_terms(
                                        index,
                                        table_reference,
                                        where_clause,
                                    )?,
                                }),
                                None => None,
                            };
                            Some(ConstraintUseCandidate {
                                index: Some(index.clone()),
                                refs: Vec::new(),
                                partial_index,
                            })
                        })
                        .collect()
                }),
        };
        // Add a candidate for the rowid index, which is always available when the table has a rowid alias.
        cs.candidates.push(ConstraintUseCandidate {
            index: None,
            refs: Vec::new(),
            partial_index: None,
        });

        let index_for_column = |column_pos| {
            selectivity_index_for_column(schema, table_reference, available_indexes, column_pos)
        };

        for (i, term) in where_clause.iter().enumerate() {
            // Constraints originating from a LEFT JOIN must always be evaluated in that join's RHS table's loop,
            // regardless of which tables the constraint references.
            if let Some(outer_join_tbl) = term.from_outer_join {
                if outer_join_tbl != table_reference.internal_id {
                    continue;
                }
            }

            // Try to extract as binary expression first
            if let Some((lhs, operator, rhs)) = as_binary_components(&term.expr)? {
                // `x IS TRUE` checks whether x is true; it does not compare x
                // with 1. For example, `2 IS TRUE` is true, so an index lookup
                // for 1 would miss that row. The same rule applies to FALSE,
                // and it holds through parentheses and COLLATE: `x IS (TRUE)`
                // is still a truth test (see [truth_test_rhs]).
                if matches!(operator.as_ast_operator(), Some(ast::Operator::Is))
                    && truth_test_rhs(rhs).is_some()
                {
                    continue;
                }
                // Resolve the comparison affinity once per term per SQLite's
                // `comparisonAffinity` (see `Constraint::comparison_affinity`)
                // and propagate it to every constraint derived from this term.
                let cmp_aff = operator
                    .as_ast_operator()
                    .filter(|op| op.is_comparison())
                    .map(|_| comparison_affinity(lhs, rhs, Some(table_references), None));
                // A WHERE term must not constrain the loop of a table that an
                // outer join can null-extend, with two exceptions below.
                // Consuming the term into the access path filters that table's
                // rows, which changes which rows of the other side count as
                // unmatched — and the join then emits null-extended rows the
                // consumed term is never checked against.
                //
                // Exception 1: terms from that join's own ON clause define what
                // counts as a match, so they are always fine.
                //
                // Exception 2: on the right side of a plain LEFT JOIN, the
                // engine re-checks consumed terms when it emits the
                // null-extended row, so any operator except `IS` stays usable
                // there: such terms are never TRUE on a null-extended row, so
                // the re-check removes the bogus rows. `IS` (e.g. `e.id IS
                // NULL`) *is* TRUE on the null-extended row, so no re-check can
                // repair it — it is unusable for every null-extendable table.
                // A FULL JOIN synthesizes its extra rows by jumping past the
                // scan with no re-check, so nothing is usable for any table a
                // FULL JOIN can null-extend.
                let is_op = matches!(operator.as_ast_operator(), Some(ast::Operator::Is));
                let usable = term.from_outer_join == Some(table_reference.internal_id)
                    || if is_op {
                        !table_references.outer_join_may_null_extend(table_reference.internal_id)
                    } else {
                        !table_references.full_join_may_null_extend(table_reference.internal_id)
                    };
                // See [Constraint::null_matching]. The constraining value sits
                // on the opposite side of the constrained column.
                let null_matching = |constraining_expr: &ast::Expr| {
                    is_op && !is_non_null_literal(constraining_expr)
                };
                // If either the LHS or RHS of the constraint is a column from the table, add the constraint.
                match lhs {
                    ast::Expr::Column { table, column, .. } => {
                        if *table == table_reference.internal_id {
                            let table_column = &table_reference.table.columns()[*column];
                            cs.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Rhs),
                                operator,
                                table_col_pos: Some(*column),
                                expr: None,
                                constraining_expr: None,
                                lhs_mask: table_mask_from_expr(rhs, table_references, subqueries)?,
                                selectivity: estimate_constraint_selectivity(
                                    schema,
                                    table_reference,
                                    Some(table_column),
                                    operator,
                                    null_matching(rhs),
                                    index_for_column(*column),
                                    params,
                                    false,
                                ),
                                usable,
                                is_rowid: false,
                                comparison_affinity: cmp_aff,
                                comparison_collation: None,
                                null_matching: null_matching(rhs),
                            });
                        }
                    }
                    ast::Expr::RowId { table, .. } => {
                        if *table == table_reference.internal_id {
                            let (col, col_pos) = if let Some(alias) = rowid_alias_column {
                                (Some(&table_reference.table.columns()[alias]), Some(alias))
                            } else {
                                (None, None)
                            };
                            cs.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Rhs),
                                operator,
                                table_col_pos: col_pos,
                                expr: None,
                                constraining_expr: None,
                                lhs_mask: table_mask_from_expr(rhs, table_references, subqueries)?,
                                selectivity: estimate_constraint_selectivity(
                                    schema,
                                    table_reference,
                                    col,
                                    operator,
                                    null_matching(rhs),
                                    None,
                                    params,
                                    true,
                                ),
                                usable,
                                is_rowid: true,
                                comparison_affinity: cmp_aff,
                                comparison_collation: None,
                                null_matching: null_matching(rhs),
                            });
                        }
                    }
                    _ if expression_matches_table(
                        lhs,
                        table_reference,
                        table_references,
                        subqueries,
                    ) =>
                    {
                        let selectivity = estimate_constraint_selectivity(
                            schema,
                            table_reference,
                            None,
                            operator,
                            null_matching(rhs),
                            None,
                            params,
                            false,
                        );
                        tracing::debug!(
                            table = table_reference.table.get_name(),
                            where_clause_pos = i,
                            operator = ?operator,
                            lhs_mask = ?table_mask_from_expr(rhs, table_references, subqueries)?,
                            selectivity,
                            "expr constraint (lhs matches table)"
                        );
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Rhs),
                            operator,
                            table_col_pos: None,
                            expr: Some(lhs.clone()),
                            constraining_expr: None,
                            lhs_mask: table_mask_from_expr(rhs, table_references, subqueries)?,
                            selectivity,
                            usable,
                            is_rowid: false,
                            comparison_affinity: cmp_aff,
                            comparison_collation: None,
                            null_matching: null_matching(rhs),
                        });
                    }
                    _ => {}
                };
                match rhs {
                    ast::Expr::Column { table, column, .. } => {
                        if *table == table_reference.internal_id {
                            let table_column = &table_reference.table.columns()[*column];
                            cs.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Lhs),
                                operator: opposite_cmp_op(operator),
                                table_col_pos: Some(*column),
                                expr: None,
                                constraining_expr: None,
                                lhs_mask: table_mask_from_expr(lhs, table_references, subqueries)?,
                                selectivity: estimate_constraint_selectivity(
                                    schema,
                                    table_reference,
                                    Some(table_column),
                                    operator,
                                    null_matching(lhs),
                                    index_for_column(*column),
                                    params,
                                    false,
                                ),
                                usable,
                                is_rowid: false,
                                comparison_affinity: cmp_aff,
                                comparison_collation: None,
                                null_matching: null_matching(lhs),
                            });
                        }
                    }
                    ast::Expr::RowId { table, .. } => {
                        if *table == table_reference.internal_id {
                            let (col, col_pos) = if let Some(alias) = rowid_alias_column {
                                (Some(&table_reference.table.columns()[alias]), Some(alias))
                            } else {
                                (None, None)
                            };
                            cs.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Lhs),
                                operator: opposite_cmp_op(operator),
                                table_col_pos: col_pos,
                                expr: None,
                                constraining_expr: None,
                                lhs_mask: table_mask_from_expr(lhs, table_references, subqueries)?,
                                selectivity: estimate_constraint_selectivity(
                                    schema,
                                    table_reference,
                                    col,
                                    operator,
                                    null_matching(lhs),
                                    None,
                                    params,
                                    true,
                                ),
                                usable,
                                is_rowid: true,
                                comparison_affinity: cmp_aff,
                                comparison_collation: None,
                                null_matching: null_matching(lhs),
                            });
                        }
                    }
                    _ if expression_matches_table(
                        rhs,
                        table_reference,
                        table_references,
                        subqueries,
                    ) =>
                    {
                        let selectivity = estimate_constraint_selectivity(
                            schema,
                            table_reference,
                            None,
                            operator,
                            null_matching(lhs),
                            None,
                            params,
                            false,
                        );
                        tracing::debug!(
                            table = table_reference.table.get_name(),
                            where_clause_pos = i,
                            operator = ?operator,
                            lhs_mask = ?table_mask_from_expr(lhs, table_references, subqueries)?,
                            selectivity,
                            "expr constraint (rhs matches table)"
                        );
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Lhs),
                            operator: opposite_cmp_op(operator),
                            table_col_pos: None,
                            expr: Some(rhs.clone()),
                            constraining_expr: None,
                            lhs_mask: table_mask_from_expr(lhs, table_references, subqueries)?,
                            selectivity,
                            usable,
                            is_rowid: false,
                            comparison_affinity: cmp_aff,
                            comparison_collation: None,
                            null_matching: null_matching(lhs),
                        });
                    }
                    _ => {}
                };
            }

            // IN expressions are handled separately from binary expressions above because:
            // - as_binary_components returns (&Expr, ConstraintOperator, &Expr) - a single RHS
            // - InList has Vec<Expr> as RHS, SubqueryResult has a different structure entirely
            // - They don't fit the binary expression abstraction without a more complex return type

            // Handle IN list: col IN (val1, val2, ...)
            if let ast::Expr::InList { lhs, not, rhs } = &term.expr {
                let estimated_values = rhs.len() as f64;
                let mut rhs_mask = TableMask::default();
                for rhs_expr in rhs.iter() {
                    rhs_mask.union_with(&table_mask_from_expr(
                        rhs_expr,
                        table_references,
                        subqueries,
                    )?)?;
                }
                let table_stats = schema
                    .analyze_stats
                    .table_stats(table_reference.table.get_name());
                let row_count = table_stats
                    .and_then(|s| s.row_count)
                    .unwrap_or(params.rows_per_table_fallback as u64)
                    as f64;
                let selectivity = estimate_in_selectivity(estimated_values, row_count, *not);
                // SQLite's `comparisonAffinity` for IN-list (`x IN (lit, ...)`)
                // is the LHS column's affinity; the RHS literals are not folded.
                let cmp_aff = Some(get_expr_affinity(lhs, Some(table_references), None));

                match lhs.as_ref() {
                    ast::Expr::Column { table, column, .. }
                        if *table == table_reference.internal_id =>
                    {
                        let is_rowid = rowid_alias_column == Some(*column);
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Rhs),
                            operator: ConstraintOperator::In {
                                not: *not,
                                estimated_values,
                            },
                            table_col_pos: Some(*column),
                            expr: None,
                            constraining_expr: None,
                            lhs_mask: rhs_mask,
                            selectivity,
                            usable: false, // IN uses a separate seek path, not the range-seek model
                            is_rowid,
                            comparison_affinity: cmp_aff,
                            comparison_collation: None,
                            null_matching: false,
                        });
                    }
                    ast::Expr::RowId { table, .. } if *table == table_reference.internal_id => {
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Rhs),
                            operator: ConstraintOperator::In {
                                not: *not,
                                estimated_values,
                            },
                            table_col_pos: rowid_alias_column,
                            expr: None,
                            constraining_expr: None,
                            lhs_mask: rhs_mask,
                            selectivity,
                            usable: false,
                            is_rowid: true,
                            comparison_affinity: cmp_aff,
                            comparison_collation: None,
                            null_matching: false,
                        });
                    }
                    _ => {}
                }
            }

            // Handle IN subquery: col IN (SELECT ...)
            if let ast::Expr::SubqueryResult {
                subquery_id,
                lhs: Some(lhs_expr),
                not_in,
                query_type: ast::SubqueryType::In { affinity_str, .. },
            } = &term.expr
            {
                // Find the subquery to check if it's correlated
                let subquery = subqueries
                    .iter()
                    .find(|s| s.internal_id == *subquery_id)
                    .expect("subquery not found");
                // Only use as constraint if NOT correlated
                if !subquery.correlated {
                    let table_stats = schema
                        .analyze_stats
                        .table_stats(table_reference.table.get_name());
                    let row_count = table_stats
                        .and_then(|s| s.row_count)
                        .unwrap_or(params.rows_per_table_fallback as u64)
                        as f64;
                    // Use the inner plan's row count instead of always using 25.
                    // FIXME: The plan does not estimate distinct result values.
                    // Until it does, cap this estimate at the square root of the
                    // table row count.
                    let planned_rows = match &subquery.state {
                        SubqueryState::Unevaluated {
                            plan: Some(inner_plan),
                        } => match inner_plan.as_ref() {
                            Plan::Select(plan) => plan.estimated_output_rows,
                            _ => None,
                        },
                        _ => None,
                    };
                    let estimated_values = planned_rows
                        .map(|rows| rows.clamp(0.0, row_count.sqrt().max(1.0)))
                        .unwrap_or_else(|| params.in_subquery_rows.min(row_count));
                    let selectivity = estimate_in_selectivity(estimated_values, row_count, *not_in);
                    // SQLite's `comparisonAffinity` for IN-subquery combines the
                    // LHS column affinity with each result column via
                    // `sqlite3CompareAffinity` — that result is already cached on
                    // `SubqueryType::In::affinity_str`. For a single-LHS-column
                    // IN it is the first character; row-value IN has no single
                    // resolved affinity and is left as `None`.
                    let is_row_value = matches!(
                        unwrap_parens(lhs_expr.as_ref()).ok(),
                        Some(ast::Expr::Parenthesized(exprs)) if exprs.len() != 1
                    );
                    let cmp_aff = (!is_row_value)
                        .then(|| affinity_str.chars().next().map(Affinity::from_char))
                        .flatten();

                    match lhs_expr.as_ref() {
                        ast::Expr::Column { table, column, .. }
                            if *table == table_reference.internal_id =>
                        {
                            let is_rowid = rowid_alias_column == Some(*column);
                            cs.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Rhs),
                                operator: ConstraintOperator::In {
                                    not: *not_in,
                                    estimated_values,
                                },
                                table_col_pos: Some(*column),
                                expr: None,
                                constraining_expr: None,
                                lhs_mask: TableMask::default(), // non-correlated = no dependencies
                                selectivity,
                                usable: false, // IN uses a separate seek path (consider_in_list_seek)
                                is_rowid,
                                comparison_affinity: cmp_aff,
                                comparison_collation: None,
                                null_matching: false,
                            });
                        }
                        ast::Expr::RowId { table, .. } if *table == table_reference.internal_id => {
                            cs.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Rhs),
                                operator: ConstraintOperator::In {
                                    not: *not_in,
                                    estimated_values,
                                },
                                table_col_pos: rowid_alias_column,
                                expr: None,
                                constraining_expr: None,
                                lhs_mask: TableMask::default(),
                                selectivity,
                                usable: false,
                                is_rowid: true,
                                comparison_affinity: cmp_aff,
                                comparison_collation: None,
                                null_matching: false,
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
        // sort equalities first so that index keys will be properly constructed.
        // see e.g.: https://www.solarwinds.com/blog/the-left-prefix-index-rule
        // A stable partition, not a comparison: comparing two equalities as
        // "less" in both directions is not a valid ordering, and now that `IS`
        // counts as an equality there are more pairs that would hit it.
        cs.constraints
            .sort_by_key(|c| !is_equality_operator(c.operator));

        // For each constraint we found, add a reference to it for each index that may be able to use it.
        for (i, constraint) in cs.constraints.iter_mut().enumerate() {
            // Skip constraints that don't participate in range-seek matching (IN, collation mismatches)
            if !constraint.usable {
                continue;
            }

            let constrained_column = constraint
                .table_col_pos
                .and_then(|pos| table_reference.table.columns().get(pos));
            let column_collation = constrained_column.map(|c| c.collation());
            let constraining_expr = constraint.get_constraining_expr_ref(where_clause);
            // Index seek keys must use the same collation as the constrained column.
            match (
                get_collseq_from_expr(constraining_expr, table_references)?,
                column_collation,
            ) {
                (Some(collation), Some(column_collation)) if collation != column_collation => {
                    constraint.usable = false;
                    continue;
                }
                _ => {}
            }

            if constraint.is_rowid
                || rowid_alias_column.is_some_and(|p| constraint.table_col_pos == Some(p))
            {
                let rowid_candidate = cs
                    .candidates
                    .iter_mut()
                    .find_map(|candidate| {
                        if candidate.index.is_none() {
                            Some(candidate)
                        } else {
                            None
                        }
                    })
                    .unwrap();
                rowid_candidate.refs.push(ConstraintRef {
                    constraint_vec_pos: i,
                    index_col_pos: 0,
                    sort_order: SortOrder::Asc,
                    nulls_order: ast::NullsOrder::First,
                });
            }
            for index in available_indexes
                .indexes_for_table(table_reference.internal_id)
                .into_iter()
                .flat_map(|indexes| indexes.iter())
                .filter(|idx| idx.index_method.is_none())
            {
                if let Some(position_in_index) = match constraint.table_col_pos {
                    Some(pos) => index.column_table_pos_to_index_pos(pos),
                    None => constraint.expr.as_ref().and_then(|e| {
                        let normalized =
                            normalize_expr_for_index_matching(e, table_reference, table_references);
                        index.expression_to_index_pos(&normalized)
                    }),
                } {
                    turso_assert!(
                        constraint.usable,
                        "constraint collation must match table column collation"
                    );
                    if let Some(table_col_pos) = constraint.table_col_pos {
                        let constrained_column = &table_reference.table.columns()[table_col_pos];
                        let table_collation = constrained_column.collation();
                        let index_collation = index.columns[position_in_index]
                            .collation
                            .unwrap_or_default();
                        if table_collation != index_collation {
                            continue;
                        }
                        // Custom type columns encode values as blobs. Blob ordering (memcmp)
                        // doesn't necessarily match the custom type's semantic ordering, so
                        // range constraints (>, <, >=, <=) can't use the index. Equality (=)
                        // still works because encoded(A) == encoded(B) iff A == B.
                        if schema
                            .get_type_def(
                                &constrained_column.ty_str,
                                table_reference.table.is_strict(),
                            )
                            .is_some()
                            && constraint.operator != ast::Operator::Equals.into()
                        {
                            continue;
                        }
                        let idx_col_aff = constrained_column
                            .affinity_with_strict(table_reference.table.is_strict());
                        if !constraint.satisfies_index_affinity(idx_col_aff) {
                            continue;
                        }
                    }
                    if let Some(index_candidate) = cs.candidates.iter_mut().find_map(|candidate| {
                        if candidate
                            .index
                            .as_ref()
                            .is_some_and(|i| Arc::ptr_eq(index, i))
                        {
                            Some(candidate)
                        } else {
                            None
                        }
                    }) {
                        index_candidate.refs.push(ConstraintRef {
                            constraint_vec_pos: i,
                            index_col_pos: position_in_index,
                            sort_order: index.columns[position_in_index].order,
                            nulls_order: index.columns[position_in_index].effective_nulls_order(),
                        });
                    }
                }
            }
        }

        for candidate in cs.candidates.iter_mut() {
            // Sort by index_col_pos, ascending -- index columns must be consumed in contiguous order.
            candidate.refs.sort_by_key(|cref| cref.index_col_pos);
        }
        cs.temporary_index_terms = automatic_index_terms(&table_reference.table, &cs)
            .into_iter()
            .filter(|term| {
                let constraint = &cs.constraints[term.constraint_vec_pos];
                let where_term = &where_clause[constraint.where_clause_pos.0];
                !expr_uses_custom_collation(&where_term.expr)
            })
            .collect();
        constraints.push(cs);
    }

    Ok(constraints)
}

/// A reference to a [Constraint]s in a [TableConstraints] for single column.
///
/// This is specialized version of [ConstraintRef] which specifically holds range-like constraints:
/// - x = 10 (eq is set)
/// - x >= 10, x > 10 (lower_bound is set)
/// - x <= 10, x < 10 (upper_bound is set)
/// - x > 10 AND x < 20 (both lower_bound and upper_bound are set)
///
/// eq, lower_bound and upper_bound holds None or position of the constraint in the [Constraint] array

#[derive(Debug, Clone)]
pub struct EqConstraintRef {
    /// Position of the constraint in the [Constraint] array.
    pub constraint_pos: usize,
    /// Whether this equality constrains the column to a single value for the
    /// entire query (true for `col = 5`, false for `t2.x = t1.b` where the
    /// value changes per outer row in a nested-loop join).
    pub is_const: bool,
    /// Whether this equality comes from `IS` instead of `=`. An `IS` equality
    /// matches NULL keys, and an index (even a UNIQUE one) can store many NULL
    /// keys, so an `IS` equality does not pin the column to at most one row.
    pub null_matching: bool,
}

#[derive(Debug, Clone)]
pub struct RangeConstraintRef {
    /// position of the column in the table definition
    pub table_col_pos: Option<usize>,
    /// position of the column in the index definition
    pub index_col_pos: usize,
    /// sort order for the column in the index definition
    pub sort_order: SortOrder,
    /// where the index column stores NULLs in its forward layout
    pub nulls_order: ast::NullsOrder,
    /// equality constraint
    pub eq: Option<EqConstraintRef>,
    /// lower bound constraint (either > or >=)
    pub lower_bound: Option<usize>,
    /// upper bound constraint (either < or <=)
    pub upper_bound: Option<usize>,
}

#[derive(Debug, Clone)]
/// Represent seek range which can be used in query planning to emit range scan over table or index
pub struct SeekRangeConstraint<E = ast::Expr> {
    pub sort_order: SortOrder,
    pub nulls_order: ast::NullsOrder,
    pub eq: Option<(ast::Operator, E, Affinity)>,
    pub lower_bound: Option<(ast::Operator, E, Affinity)>,
    pub upper_bound: Option<(ast::Operator, E, Affinity)>,
}

impl<E> SeekRangeConstraint<E> {
    pub fn new_eq(
        sort_order: SortOrder,
        nulls_order: ast::NullsOrder,
        eq: (ast::Operator, E, Affinity),
    ) -> Self {
        Self {
            sort_order,
            nulls_order,
            eq: Some(eq),
            lower_bound: None,
            upper_bound: None,
        }
    }
    pub fn new_range(
        sort_order: SortOrder,
        nulls_order: ast::NullsOrder,
        lower_bound: Option<(ast::Operator, E, Affinity)>,
        upper_bound: Option<(ast::Operator, E, Affinity)>,
    ) -> Self {
        turso_assert!(lower_bound.is_some() || upper_bound.is_some());
        Self {
            sort_order,
            nulls_order,
            eq: None,
            lower_bound,
            upper_bound,
        }
    }
}

impl RangeConstraintRef {
    fn map_seek_range<E>(
        &self,
        mut get: impl FnMut(usize) -> (ast::Operator, E, Affinity),
    ) -> SeekRangeConstraint<E> {
        if let Some(ref eq) = self.eq {
            return SeekRangeConstraint::new_eq(
                self.sort_order,
                self.nulls_order,
                get(eq.constraint_pos),
            );
        }
        SeekRangeConstraint::new_range(
            self.sort_order,
            self.nulls_order,
            self.lower_bound.map(&mut get),
            self.upper_bound.map(get),
        )
    }

    /// Convert the [RangeConstraintRef] to a [SeekRangeConstraint] usable in a [crate::translate::plan::SeekDef::key].
    pub fn as_seek_range_constraint(
        &self,
        constraints: &[Constraint],
        where_clause: &[WhereTerm],
        referenced_tables: Option<&TableReferences>,
        resolver: Option<&Resolver>,
    ) -> SeekRangeConstraint {
        self.map_seek_range(|position| {
            constraints[position].get_constraining_expr(where_clause, referenced_tables, resolver)
        })
    }

    pub(crate) fn as_hir_seek_range_constraint(
        &self,
        constraints: &[HirConstraint],
        where_clause: &[HirWhereTerm],
    ) -> SeekRangeConstraint<hir::Expr> {
        self.map_seek_range(|position| {
            constraints[position].get_hir_constraining_expr(where_clause)
        })
    }
}

impl HirConstraint {
    pub(super) fn get_hir_constraining_expr(
        &self,
        where_clause: &[HirWhereTerm],
    ) -> (ast::Operator, hir::Expr, Affinity) {
        if let Some(constraining) = &self.constraining_expr {
            return constraining.clone();
        }

        let (term_position, side) = self.where_clause_pos;
        let hir::Expr::Binary { lhs, rhs, .. } = &where_clause[term_position].expr else {
            panic!("expected HIR seek constraint to reference a binary expression");
        };
        let expression = match side {
            BinaryExprSide::Lhs => lhs.as_ref(),
            BinaryExprSide::Rhs => rhs.as_ref(),
        };
        let mut affinity = self.comparison_affinity.unwrap_or(Affinity::Blob);
        if hir_expr_needs_no_affinity_change(affinity, expression) {
            affinity = Affinity::Blob;
        }
        (
            self.operator
                .as_ast_operator()
                .expect("HIR seek constraints use comparison operators"),
            expression.clone(),
            affinity,
        )
    }
}

fn hir_expr_needs_no_affinity_change(affinity: Affinity, expression: &hir::Expr) -> bool {
    if matches!(affinity, Affinity::Blob | Affinity::None) {
        return true;
    }
    match expression {
        hir::Expr::Literal(literal) => match literal {
            ast::Literal::Numeric(_) => affinity.is_numeric(),
            ast::Literal::String(_) => affinity == Affinity::Text,
            ast::Literal::Blob(_) => true,
            _ => false,
        },
        hir::Expr::RowId(_) => affinity.is_numeric(),
        _ => false,
    }
}

/// Find which [Constraint]s are usable for a given join order.
/// Returns a slice of the references to the constraints that are usable.
/// A constraint is considered usable for a given table if all of the other tables referenced by the constraint
/// are on the left side in the join order relative to the table.
///
/// This enforces the normal B-tree prefix rules:
/// - usable index columns must form a contiguous prefix starting at column 0
/// - once a prefix column has no usable constraint, later columns cannot be used
/// - once a prefix column uses a range constraint, later columns cannot be used
///
/// Multiple constraints on the same index column are merged into a single
/// [RangeConstraintRef]. Equality wins over range constraints; otherwise we keep
/// at most one lower bound and one upper bound for that column.
pub fn usable_constraints_for_lhs_mask<E>(
    constraints: &[Constraint<E>],
    refs: &[ConstraintRef],
    lhs_mask: &TableMask,
    table_idx: usize,
) -> SmallVec<[RangeConstraintRef; 2]> {
    turso_debug_assert!(refs.is_sorted_by_key(|x| x.index_col_pos));

    let mut usable: SmallVec<[RangeConstraintRef; 2]> = SmallVec::new();
    let mut current_required_column_pos = 0;
    for cref in refs.iter() {
        let constraint = &constraints[cref.constraint_vec_pos];
        let other_side_refers_to_self = constraint.lhs_mask.get(table_idx);
        if other_side_refers_to_self {
            // Self-referential constraints cannot seed a lookup, but if they are
            // on a later index column they also terminate the usable prefix.
            if cref.index_col_pos != current_required_column_pos {
                break;
            }
            continue;
        }
        if !lhs_mask.contains_all_set_bits_of(&constraint.lhs_mask) {
            // Join-dependent constraints are only usable when every referenced
            // outer table is already on the left side of the join order. As
            // above, a missing earlier prefix column terminates the prefix.
            if cref.index_col_pos != current_required_column_pos {
                break;
            }
            continue;
        }
        if Some(cref.index_col_pos) == usable.last().map(|x| x.index_col_pos) {
            // Merge multiple usable constraints for the same index column into a
            // single equality-or-range group.
            assert_eq!(cref.sort_order, usable.last().unwrap().sort_order);
            assert_eq!(cref.index_col_pos, usable.last().unwrap().index_col_pos);
            assert_eq!(
                constraints[cref.constraint_vec_pos].table_col_pos,
                usable.last().unwrap().table_col_pos
            );
            if usable.last().unwrap().eq.is_some() {
                // An equality already fixes this column exactly, so extra
                // constraints on the same column do not change the seek shape.
                continue;
            }
            match constraints[cref.constraint_vec_pos]
                .operator
                .as_ast_operator()
            {
                Some(ast::Operator::Greater) | Some(ast::Operator::GreaterEquals) => {
                    usable.last_mut().unwrap().lower_bound = Some(cref.constraint_vec_pos);
                }
                Some(ast::Operator::Less) | Some(ast::Operator::LessEquals) => {
                    usable.last_mut().unwrap().upper_bound = Some(cref.constraint_vec_pos);
                }
                _ => {}
            }
            continue;
        }
        if cref.index_col_pos != current_required_column_pos {
            // We found a gap in the usable prefix, so later index columns are
            // not usable for the lookup.
            break;
        }
        if usable.last().is_some_and(|x| x.eq.is_none()) {
            // The previous prefix column is already a range, so no later column
            // can participate in the seek key.
            break;
        }
        let operator = constraints[cref.constraint_vec_pos].operator;
        let table_col_pos = constraints[cref.constraint_vec_pos].table_col_pos;
        if is_equality_operator(operator)
            && usable
                .last()
                .is_some_and(|x| x.table_col_pos == table_col_pos)
        {
            // Duplicate equalities on the same column do not expand the usable
            // prefix or change the seek shape.
            continue;
        }
        let constraint_group = match operator.as_ast_operator() {
            Some(ast::Operator::Equals | ast::Operator::Is) => RangeConstraintRef {
                table_col_pos,
                index_col_pos: cref.index_col_pos,
                sort_order: cref.sort_order,
                nulls_order: cref.nulls_order,
                eq: Some(EqConstraintRef {
                    constraint_pos: cref.constraint_vec_pos,
                    is_const: constraints[cref.constraint_vec_pos].lhs_mask.is_empty(),
                    null_matching: constraints[cref.constraint_vec_pos].null_matching,
                }),
                lower_bound: None,
                upper_bound: None,
            },
            Some(ast::Operator::Greater) | Some(ast::Operator::GreaterEquals) => {
                RangeConstraintRef {
                    table_col_pos,
                    index_col_pos: cref.index_col_pos,
                    sort_order: cref.sort_order,
                    nulls_order: cref.nulls_order,
                    eq: None,
                    lower_bound: Some(cref.constraint_vec_pos),
                    upper_bound: None,
                }
            }
            Some(ast::Operator::Less) | Some(ast::Operator::LessEquals) => RangeConstraintRef {
                table_col_pos,
                index_col_pos: cref.index_col_pos,
                sort_order: cref.sort_order,
                nulls_order: cref.nulls_order,
                eq: None,
                lower_bound: None,
                upper_bound: Some(cref.constraint_vec_pos),
            },
            _ => continue,
        };
        usable.push(constraint_group);
        current_required_column_pos += 1;
    }
    usable
}

pub fn usable_constraints_for_join_order<E>(
    constraints: &[Constraint<E>],
    refs: &[ConstraintRef],
    join_order: &[JoinOrderMember],
) -> Result<Vec<RangeConstraintRef>> {
    turso_debug_assert!(refs.is_sorted_by_key(|x| x.index_col_pos));

    let table_idx = join_order.last().unwrap().original_idx;
    let lhs_mask = join_order
        .iter()
        .take(join_order.len() - 1)
        .map(|j| j.original_idx)
        .try_collect()?;
    Ok(usable_constraints_for_lhs_mask(constraints, refs, &lhs_mask, table_idx).into_vec())
}

/// Order key columns for a temporary index.
///
/// Equalities come first because an index cannot use a column after a range.
/// A column with both an equality and a range stays in the equality part.
pub fn ordered_ephemeral_key_columns<E>(constraints: &[&Constraint<E>]) -> SmallVec<[usize; 4]> {
    let mut equality_cols = SmallVec::<[usize; 4]>::new();
    let mut range_only_cols = SmallVec::<[usize; 4]>::new();

    for constraint in constraints {
        let Some(col_pos) = constraint.table_col_pos else {
            continue;
        };
        match constraint.operator.as_ast_operator() {
            Some(ast::Operator::Equals | ast::Operator::Is) => equality_cols.push(col_pos),
            Some(
                ast::Operator::Greater
                | ast::Operator::GreaterEquals
                | ast::Operator::Less
                | ast::Operator::LessEquals,
            ) => range_only_cols.push(col_pos),
            _ => {}
        }
    }

    equality_cols.sort_unstable();
    equality_cols.dedup();
    range_only_cols.sort_unstable();
    range_only_cols.dedup();
    range_only_cols.retain(|col_pos| !equality_cols.contains(col_pos));

    let mut ordered = equality_cols;
    ordered.extend(range_only_cols);
    ordered
}

/// This returns `None` if any of the index's WHERE clause conjunct terms don't match
/// with at least one term from the query's WHERE clause.
///
/// Otherwise, it returns the indexes into `query_where_clause` of the matching terms.
/// For example, using this partial index:
///
/// CREATE INDEX idx on t(a) WHERE length(a) < 5 AND substr(a, 1, 1) == 'B';
///
/// this will return 1 and 2:
///
/// SELECT a FROM t WHERE substr(a, 2, 2) == 'C' AND length(a) < 5 AND substr(a, 1, 1) == 'B';
///
/// And this will return `None`:
///
/// CREATE INDEX idx on t(a) WHERE length(a) < 1234 AND substr(a, 1, 1) == 'B';
pub(super) fn partial_index_predicate_terms(
    index: &Index,
    table_reference: &JoinedTable,
    query_where_clause: &[WhereTerm],
) -> Option<SmallVec<[usize; 4]>> {
    let index_where = index
        .where_clause
        .as_ref()
        .expect("partial_index_predicate_terms requires a partial index");
    let can_use_query_term = |term: &WhereTerm| -> bool {
        let Some(join_info) = &table_reference.join_info else {
            return true;
        };
        if join_info.is_full_outer() {
            return false;
        }
        if join_info.is_outer() {
            return term.from_outer_join == Some(table_reference.internal_id);
        }
        true
    };
    // Bind the index WHERE expression's column references to this query's
    // table reference so it can be compared symmetrically against bound query
    // WHERE terms. Each conjunct of the index WHERE must match some query
    // WHERE term for the partial index to be safe to use.
    let mut bound = (**index_where).clone();
    bind_partial_index_columns(&mut bound, table_reference);
    rewrite_between_exprs(&mut bound).ok()?;
    let mut index_conjuncts: Vec<ast::Expr> = Vec::new();
    bound.append_conjuncts(&mut index_conjuncts);
    let mut matched_terms = SmallVec::<[usize; 4]>::new();
    for index_conjunct in index_conjuncts.iter() {
        let (term_idx, _) = query_where_clause.iter().enumerate().find(|(_, term)| {
            can_use_query_term(term) && exprs_are_equivalent(index_conjunct, &term.expr)
        })?;
        if !matched_terms.contains(&term_idx) {
            matched_terms.push(term_idx);
        }
    }
    Some(matched_terms)
    // TODO: recognize implication beyond syntactic equivalence (e.g. `x = 5` implies
    // `x IS NOT NULL`, `x > 10` implies `x > 5`).
}

pub(super) fn can_use_partial_index(
    index: &Index,
    table_reference: &JoinedTable,
    query_where_clause: &[WhereTerm],
) -> bool {
    assert!(
        index.where_clause.is_some(),
        "can_use_partial_index requires a partial index"
    );
    partial_index_predicate_terms(index, table_reference, query_where_clause).is_some()
}

/// Rewrite identifier nodes in a partial-index WHERE expression to bound
/// `Expr::Column` / `Expr::RowId` against `table_reference`. Partial-index
/// WHERE clauses are validated to only reference columns of the indexed
/// table (`Index::validate_where_expr`), so this minimal binder is enough
/// to make the expression directly comparable to a bound query expression
/// via `exprs_are_equivalent` — without threading a full `Resolver`.
fn bind_partial_index_columns(expr: &mut ast::Expr, table_reference: &JoinedTable) {
    let column_pos = |name: &str| -> Option<usize> {
        table_reference.columns().iter().position(|c| {
            c.name
                .as_ref()
                .is_some_and(|cn| cn.eq_ignore_ascii_case(name))
        })
    };
    let is_rowid_keyword = |name: &str| ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(name));
    let qualifier_matches = |ns: &str| -> bool {
        ns.eq_ignore_ascii_case(&table_reference.identifier)
            || ns.eq_ignore_ascii_case(table_reference.table.get_name())
    };
    let make_column = |col_idx: usize| -> ast::Expr {
        let col = &table_reference.columns()[col_idx];
        ast::Expr::Column {
            database: None,
            table: table_reference.internal_id,
            column: col_idx,
            is_rowid_alias: col.is_rowid_alias(),
        }
    };
    let make_rowid = || -> ast::Expr {
        ast::Expr::RowId {
            database: None,
            table: table_reference.internal_id,
        }
    };

    let _ = walk_expr_mut(expr, &mut |e: &mut ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Id(name) => {
                if let Some(idx) = column_pos(name.as_str()) {
                    *e = make_column(idx);
                } else if is_rowid_keyword(name.as_str()) {
                    *e = make_rowid();
                }
            }
            ast::Expr::Qualified(ns, col) | ast::Expr::DoublyQualified(_, ns, col) => {
                if qualifier_matches(ns.as_str()) {
                    if let Some(idx) = column_pos(col.as_str()) {
                        *e = make_column(idx);
                    } else if is_rowid_keyword(col.as_str()) {
                        *e = make_rowid();
                    }
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
}

/// Estimate the selectivity of a partial index's WHERE clause, i.e. what
/// fraction of the table's rows pass the predicate (and therefore appear in
/// the partial index). The expression is bound to the table's column space
/// first so that leaf comparisons can dispatch through the standard
/// `estimate_selectivity` path — picking up ANALYZE stats when available.
pub fn estimate_partial_index_where_selectivity(
    where_expr: &ast::Expr,
    table_reference: &JoinedTable,
    schema: &Schema,
    available_indexes: &AvailableIndexes,
    params: &CostModelParams,
) -> f64 {
    let mut bound = where_expr.clone();
    bind_partial_index_columns(&mut bound, table_reference);
    estimate_bound_expr_selectivity(&bound, table_reference, schema, available_indexes, params)
}

fn estimate_bound_expr_selectivity(
    expr: &ast::Expr,
    table_reference: &JoinedTable,
    schema: &Schema,
    available_indexes: &AvailableIndexes,
    params: &CostModelParams,
) -> f64 {
    use ast::Expr;
    let expr = crate::translate::expr::unwrap_parens(expr).unwrap_or(expr);

    // Try to resolve the constrained column from a binary leaf so we can use
    // ANALYZE-aware selectivity. Returns (Column, column_pos, is_rowid).
    let resolve_side = |side: &ast::Expr| -> Option<(Option<&Column>, Option<usize>, bool)> {
        match side {
            Expr::Column { table, column, .. } if *table == table_reference.internal_id => Some((
                Some(&table_reference.table.columns()[*column]),
                Some(*column),
                false,
            )),
            Expr::RowId { table, .. } if *table == table_reference.internal_id => {
                let rowid_alias = table_reference
                    .columns()
                    .iter()
                    .position(|c| c.is_rowid_alias());
                Some((
                    rowid_alias.map(|p| &table_reference.table.columns()[p]),
                    rowid_alias,
                    true,
                ))
            }
            _ => None,
        }
    };
    let leaf_selectivity = |lhs: &ast::Expr, rhs: &ast::Expr, op: ConstraintOperator| -> f64 {
        let resolved = resolve_side(lhs).or_else(|| resolve_side(rhs));
        let (col, col_pos, is_rowid) = resolved.unwrap_or((None, None, false));
        let index = col_pos.and_then(|pos| {
            selectivity_index_for_column(schema, table_reference, available_indexes, pos)
        });
        // For `IS`, the key can be NULL unless one side is a non-NULL literal
        // (the constrained column is on the other side).
        let null_matching = op.as_ast_operator() == Some(ast::Operator::Is)
            && !(is_non_null_literal(lhs) || is_non_null_literal(rhs));
        estimate_constraint_selectivity(
            schema,
            table_reference,
            col,
            op,
            null_matching,
            index,
            params,
            is_rowid,
        )
    };

    match expr {
        Expr::Binary(lhs, ast::Operator::And, rhs) => {
            let l = estimate_bound_expr_selectivity(
                lhs,
                table_reference,
                schema,
                available_indexes,
                params,
            );
            let r = estimate_bound_expr_selectivity(
                rhs,
                table_reference,
                schema,
                available_indexes,
                params,
            );
            l * r
        }
        Expr::Binary(lhs, ast::Operator::Or, rhs) => {
            let l = estimate_bound_expr_selectivity(
                lhs,
                table_reference,
                schema,
                available_indexes,
                params,
            );
            let r = estimate_bound_expr_selectivity(
                rhs,
                table_reference,
                schema,
                available_indexes,
                params,
            );
            (l + r - l * r).min(1.0)
        }
        Expr::Binary(_, ast::Operator::Is, rhs)
            if matches!(rhs.as_ref(), Expr::Literal(ast::Literal::Null)) =>
        {
            params.sel_is_null
        }
        Expr::Binary(_, ast::Operator::IsNot, rhs)
            if matches!(rhs.as_ref(), Expr::Literal(ast::Literal::Null)) =>
        {
            params.sel_is_not_null
        }
        Expr::Binary(lhs, op, rhs) => leaf_selectivity(lhs, rhs, (*op).into()),
        Expr::IsNull(_) => params.sel_is_null,
        Expr::NotNull(_) => params.sel_is_not_null,
        Expr::Between { not, .. } => {
            if *not {
                1.0 - params.sel_range
            } else {
                params.sel_range
            }
        }
        Expr::InList { lhs, not, rhs } => {
            let resolved = resolve_side(lhs);
            let (col, col_pos, is_rowid) = resolved.unwrap_or((None, None, false));
            let index = col_pos.and_then(|pos| {
                selectivity_index_for_column(schema, table_reference, available_indexes, pos)
            });
            estimate_constraint_selectivity(
                schema,
                table_reference,
                col,
                ConstraintOperator::In {
                    not: *not,
                    estimated_values: rhs.len() as f64,
                },
                false,
                index,
                params,
                is_rowid,
            )
        }
        Expr::Like { not, .. } => {
            if *not {
                params.sel_not_like
            } else {
                params.sel_like
            }
        }
        Expr::Unary(ast::UnaryOperator::Not, inner) => {
            1.0 - estimate_bound_expr_selectivity(
                inner,
                table_reference,
                schema,
                available_indexes,
                params,
            )
        }
        _ => params.sel_other,
    }
}

pub fn convert_to_vtab_constraint<E>(
    constraints: &[Constraint<E>],
    join_order: &[JoinOrderMember],
) -> Result<Vec<ConstraintInfo>> {
    let table_idx = join_order.last().unwrap().original_idx;
    let lhs_mask: TableMask = join_order
        .iter()
        .take(join_order.len() - 1)
        .map(|j| j.original_idx)
        .try_collect()?;
    Ok(convert_to_vtab_constraint_for_prefix(
        constraints,
        table_idx,
        &lhs_mask,
    ))
}

pub fn convert_to_vtab_constraint_for_prefix<E>(
    constraints: &[Constraint<E>],
    table_idx: usize,
    lhs_mask: &TableMask,
) -> Vec<ConstraintInfo> {
    constraints
        .iter()
        .enumerate()
        .filter_map(|(i, constraint)| {
            let table_col_pos = constraint.table_col_pos?;
            let other_side_refers_to_self = constraint.lhs_mask.get(table_idx);
            if other_side_refers_to_self {
                return None;
            }
            let all_required_tables_are_on_left_side =
                lhs_mask.contains_all_set_bits_of(&constraint.lhs_mask);
            to_ext_constraint_op(&constraint.operator).map(|op| ConstraintInfo {
                column_index: table_col_pos as u32,
                op,
                usable: all_required_tables_are_on_left_side,
                index: i,
            })
        })
        .collect()
}

/// Whether `op` constrains an index column to a single value, making it usable
/// as an index seek key.
///
/// `IS` belongs here next to `=`: SQLite treats it as an index-usable equality
/// that additionally matches NULL (`x IS NULL` seeks the index's NULL entries,
/// and `x IS ?` with a NULL bind finds the rows whose key component is NULL).
/// The only difference is in codegen: an `=` seek can stop early when its key is
/// NULL because `NULL = NULL` is not true, while an `IS` seek must seek with the
/// NULL key. See [`SeekDef::is_null_matching_key_component`].
pub fn is_equality_operator(op: ConstraintOperator) -> bool {
    matches!(
        op.as_ast_operator(),
        Some(ast::Operator::Equals | ast::Operator::Is)
    )
}

fn to_ext_constraint_op(op: &ConstraintOperator) -> Option<ConstraintOp> {
    let ConstraintOperator::AstNativeOperator(op) = op else {
        return None;
    };
    match op {
        ast::Operator::Equals => Some(ConstraintOp::Eq),
        ast::Operator::Less => Some(ConstraintOp::Lt),
        ast::Operator::LessEquals => Some(ConstraintOp::Le),
        ast::Operator::Greater => Some(ConstraintOp::Gt),
        ast::Operator::GreaterEquals => Some(ConstraintOp::Ge),
        ast::Operator::NotEquals => Some(ConstraintOp::Ne),
        _ => None,
    }
}

fn opposite_cmp_op(op: ConstraintOperator) -> ConstraintOperator {
    let ConstraintOperator::AstNativeOperator(op_inner) = &op else {
        return op;
    };
    match op_inner {
        ast::Operator::Equals => ast::Operator::Equals,
        ast::Operator::Greater => ast::Operator::Less,
        ast::Operator::GreaterEquals => ast::Operator::LessEquals,
        ast::Operator::Less => ast::Operator::Greater,
        ast::Operator::LessEquals => ast::Operator::GreaterEquals,
        ast::Operator::NotEquals => ast::Operator::NotEquals,
        ast::Operator::Is => ast::Operator::Is,
        ast::Operator::IsNot => ast::Operator::IsNot,
        _ => panic!("unexpected operator: {op:?}"),
    }
    .into()
}

/// Result of analyzing a single term for multi-index scan potential.
/// This is a shared intermediate structure used by both OR and AND analysis.
#[derive(Debug)]
pub struct AnalyzedTerm {
    /// The constraint derived from this term.
    pub constraint: Constraint,
    /// The best index for this term, if any.
    pub best_index: Option<Arc<Index>>,
    /// Constraint references for this term.
    pub constraint_refs: Vec<RangeConstraintRef>,
}

/// Lightweight prepass result for a binary term that can seed a multi-index branch.
#[derive(Debug, Clone)]
pub struct IndexableTermSummary {
    /// The constrained table column, if any.
    pub table_col_pos: Option<usize>,
    /// The other tables referenced by the constraining expression.
    pub lhs_mask: TableMask,
    /// The chosen index for this term, or `None` for rowid access.
    pub best_index: Option<Arc<Index>>,
}

struct BinaryTermIndexInfo<'a> {
    lhs: &'a ast::Expr,
    rhs: &'a ast::Expr,
    operator: ConstraintOperator,
    table_col_pos: Option<usize>,
    constraining_expr: &'a ast::Expr,
    side: BinaryExprSide,
    is_rowid: bool,
}

fn analyze_binary_term_index_info<'a>(
    expr: &'a ast::Expr,
    table_id: TableInternalId,
    rowid_alias_column: Option<usize>,
) -> Option<BinaryTermIndexInfo<'a>> {
    let (lhs, operator, rhs) = as_binary_components(expr).ok().flatten()?;

    // Check if the operator is usable for index seeks
    let is_usable_op = matches!(
        operator.as_ast_operator(),
        Some(
            ast::Operator::Equals
                | ast::Operator::Greater
                | ast::Operator::GreaterEquals
                | ast::Operator::Less
                | ast::Operator::LessEquals
        )
    );

    if !is_usable_op {
        return None;
    }

    // Check if this is an indexable constraint on our table
    let (table_col_pos, constraining_expr, side, is_rowid) = match lhs {
        ast::Expr::Column { table, column, .. } if *table == table_id => {
            (Some(*column), rhs, BinaryExprSide::Rhs, false)
        }
        ast::Expr::RowId { table, .. } if *table == table_id => {
            (rowid_alias_column, rhs, BinaryExprSide::Rhs, true)
        }
        _ => match rhs {
            ast::Expr::Column { table, column, .. } if *table == table_id => {
                (Some(*column), lhs, BinaryExprSide::Lhs, false)
            }
            ast::Expr::RowId { table, .. } if *table == table_id => {
                (rowid_alias_column, lhs, BinaryExprSide::Lhs, true)
            }
            _ => return None, // Doesn't reference our table
        },
    };

    // Normalize operator direction so it matches the constrained table column.
    // Example: `1 > t.b` constrains `t.b < 1`.
    let operator = if side == BinaryExprSide::Lhs {
        opposite_cmp_op(operator)
    } else {
        operator
    };

    Some(BinaryTermIndexInfo {
        lhs,
        rhs,
        operator,
        table_col_pos,
        constraining_expr,
        side,
        is_rowid,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn summarize_binary_term_for_index(
    expr: &ast::Expr,
    table_id: TableInternalId,
    table_reference: &JoinedTable,
    query_where_clause: &[WhereTerm],
    indexes: Option<&VecDeque<Arc<Index>>>,
    rowid_alias_column: Option<usize>,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
) -> Option<IndexableTermSummary> {
    let BinaryTermIndexInfo {
        operator,
        table_col_pos,
        constraining_expr,
        is_rowid,
        ..
    } = analyze_binary_term_index_info(expr, table_id, rowid_alias_column)?;

    let (best_index, constraint_refs) = find_best_index_for_constraint(
        table_col_pos,
        operator,
        indexes,
        rowid_alias_column,
        is_rowid,
        table_reference,
        query_where_clause,
    );
    if constraint_refs.is_empty() {
        return None;
    }

    let lhs_mask = table_mask_from_expr(constraining_expr, table_references, subqueries)
        .unwrap_or_else(|_| TableMask::default());

    let table_pos = table_references
        .joined_tables()
        .iter()
        .position(|t| t.internal_id == table_id)
        .expect("target table must exist in table_references");
    if lhs_mask.get(table_pos) {
        return None;
    }

    Some(IndexableTermSummary {
        table_col_pos,
        lhs_mask,
        best_index,
    })
}

/// Analyzes a single binary expression to determine if it can use an index.
///
/// This is a shared helper for both OR and AND multi-index analysis.
/// Returns `Some(AnalyzedTerm)` if the expression is a usable indexed constraint,
/// `None` otherwise.
#[allow(clippy::too_many_arguments)]
pub(crate) fn analyze_binary_term_for_index(
    expr: &ast::Expr,
    where_term_idx: usize,
    table_id: TableInternalId,
    table_reference: &JoinedTable,
    query_where_clause: &[WhereTerm],
    indexes: Option<&VecDeque<Arc<Index>>>,
    rowid_alias_column: Option<usize>,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    params: &CostModelParams,
) -> Option<AnalyzedTerm> {
    let BinaryTermIndexInfo {
        lhs,
        rhs,
        operator,
        table_col_pos,
        constraining_expr,
        side,
        is_rowid,
    } = analyze_binary_term_index_info(expr, table_id, rowid_alias_column)?;

    // Find the best index for this constraint
    let (best_index, constraint_refs) = find_best_index_for_constraint(
        table_col_pos,
        operator,
        indexes,
        rowid_alias_column,
        is_rowid,
        table_reference,
        query_where_clause,
    );

    // If no index can be used, this term is not indexable
    if constraint_refs.is_empty() {
        return None;
    }

    let table_column = table_col_pos.and_then(|pos| table_reference.table.columns().get(pos));
    // See [Constraint::null_matching].
    let null_matching = operator.as_ast_operator() == Some(ast::Operator::Is)
        && !is_non_null_literal(constraining_expr);
    let selectivity = estimate_constraint_selectivity(
        schema,
        table_reference,
        table_column,
        operator,
        null_matching,
        best_index.as_deref(),
        params,
        is_rowid,
    );

    let lhs_mask = table_mask_from_expr(constraining_expr, table_references, subqueries)
        .unwrap_or_else(|_| TableMask::default());

    // Cannot use index seek if the constraining expression references the same table
    // being scanned, since the expression value varies per row and cannot be evaluated
    // before the scan (e.g. TYPEOF(b) NOT BETWEEN a AND a where both columns are from
    // the same table).
    if let Some(table_pos) = table_references
        .joined_tables()
        .iter()
        .position(|t| t.internal_id == table_id)
    {
        if lhs_mask.get(table_pos) {
            return None;
        }
    }

    // Compute the affinity for the constraining expression
    let affinity = if let Some(ast_op) = operator.as_ast_operator() {
        if ast_op.is_comparison() && table_col_pos.is_some() {
            comparison_affinity(lhs, rhs, Some(table_references), None)
        } else {
            Affinity::Blob
        }
    } else {
        Affinity::Blob
    };

    // Store the pre-computed constraining expression for multi-index branches
    let stored_constraining_expr = operator
        .as_ast_operator()
        .map(|ast_op| (ast_op, constraining_expr.clone(), affinity));

    let constraint = Constraint {
        where_clause_pos: (where_term_idx, side),
        operator,
        table_col_pos,
        expr: None,
        constraining_expr: stored_constraining_expr,
        lhs_mask,
        selectivity,
        usable: true,
        is_rowid,
        comparison_affinity: Some(affinity),
        comparison_collation: None,
        null_matching,
    };

    Some(AnalyzedTerm {
        constraint,
        best_index,
        constraint_refs,
    })
}

/// Find the best index for a single constraint.
fn find_best_index_for_constraint(
    table_col_pos: Option<usize>,
    operator: ConstraintOperator,
    indexes: Option<&VecDeque<Arc<Index>>>,
    rowid_alias_column: Option<usize>,
    is_rowid: bool,
    table_reference: &JoinedTable,
    query_where_clause: &[WhereTerm],
) -> (Option<Arc<Index>>, Vec<RangeConstraintRef>) {
    // Handle implicit rowid (no alias column, table_col_pos is None)
    if is_rowid && table_col_pos.is_none() {
        let constraint_ref = RangeConstraintRef {
            table_col_pos: None,
            index_col_pos: 0,
            sort_order: SortOrder::Asc,
            nulls_order: ast::NullsOrder::First,
            eq: if operator.as_ast_operator() == Some(ast::Operator::Equals) {
                Some(EqConstraintRef {
                    constraint_pos: 0,
                    is_const: false,
                    null_matching: false,
                })
            } else {
                None
            },
            lower_bound: match operator.as_ast_operator() {
                Some(ast::Operator::Greater | ast::Operator::GreaterEquals) => Some(0),
                _ => None,
            },
            upper_bound: match operator.as_ast_operator() {
                Some(ast::Operator::Less | ast::Operator::LessEquals) => Some(0),
                _ => None,
            },
        };
        return (None, vec![constraint_ref]);
    }

    let Some(col_pos) = table_col_pos else {
        return (None, vec![]);
    };

    // Check rowid index first if this is a rowid constraint
    if rowid_alias_column == Some(col_pos) {
        let constraint_ref = RangeConstraintRef {
            table_col_pos: Some(col_pos),
            index_col_pos: 0,
            sort_order: SortOrder::Asc,
            nulls_order: ast::NullsOrder::First,
            eq: if operator.as_ast_operator() == Some(ast::Operator::Equals) {
                Some(EqConstraintRef {
                    constraint_pos: 0,
                    is_const: false,
                    null_matching: false,
                })
            } else {
                None
            },
            lower_bound: match operator.as_ast_operator() {
                Some(ast::Operator::Greater | ast::Operator::GreaterEquals) => Some(0),
                _ => None,
            },
            upper_bound: match operator.as_ast_operator() {
                Some(ast::Operator::Less | ast::Operator::LessEquals) => Some(0),
                _ => None,
            },
        };
        return (None, vec![constraint_ref]);
    }

    // Find the best index that has this column as its first column
    if let Some(indexes) = indexes {
        for index in indexes.iter().filter(|idx| idx.index_method.is_none()) {
            if index.where_clause.is_some()
                && !can_use_partial_index(index.as_ref(), table_reference, query_where_clause)
            {
                continue;
            }
            if let Some(idx_col_pos) = index.column_table_pos_to_index_pos(col_pos) {
                // For multi-index OR, we prefer indexes where the constraint column
                // is the first column (leftmost prefix)
                if idx_col_pos == 0 {
                    let constraint_ref = RangeConstraintRef {
                        table_col_pos: Some(col_pos),
                        index_col_pos: 0,
                        sort_order: index.columns[0].order,
                        nulls_order: index.columns[0].effective_nulls_order(),
                        eq: if operator.as_ast_operator() == Some(ast::Operator::Equals) {
                            Some(EqConstraintRef {
                                constraint_pos: 0,
                                is_const: false,
                                null_matching: false,
                            })
                        } else {
                            None
                        },
                        lower_bound: match operator.as_ast_operator() {
                            Some(ast::Operator::Greater | ast::Operator::GreaterEquals) => Some(0),
                            _ => None,
                        },
                        upper_bound: match operator.as_ast_operator() {
                            Some(ast::Operator::Less | ast::Operator::LessEquals) => Some(0),
                            _ => None,
                        },
                    };
                    return (Some(index.clone()), vec![constraint_ref]);
                }
            }
        }
    }

    (None, vec![])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_hir_document(source: hir::SourceId) -> hir::HirDocument {
        let sources = (0..=source.index())
            .map(|index| hir::Source {
                id: hir::SourceId::new(index),
                owner: hir::SourceOwner::Root,
                database: None,
                name: format!("source_{index}"),
                alias: None,
                kind: hir::SourceKind::SchemaExpression,
                columns: Vec::new(),
                generated_expressions: Vec::new(),
                default_expressions: Vec::new(),
                column_type_programs: Vec::new(),
                check_constraints: None,
                rowid_available: true,
                index_hint: hir::IndexHint::None,
                index_expressions: Vec::new(),
                index_coverage: hir::IndexCoverage::Selective,
                index_method_patterns: Vec::new(),
            })
            .collect();
        hir::HirDocument {
            snapshot: hir::CatalogSnapshot::from_id(0),
            databases: Vec::new(),
            root: hir::HirRoot::SchemaExpressions(hir::SchemaExpressionRoot {
                source,
                expressions: Vec::new(),
            }),
            queries: Vec::new(),
            sources,
            ctes: Vec::new(),
            schema_programs: Vec::new(),
            cdc: None,
        }
    }

    fn hir_document_with_query(
        source: hir::SourceId,
        query: hir::QueryId,
        captures: Vec<hir::SourceId>,
    ) -> hir::HirDocument {
        let mut document = empty_hir_document(source);
        document.queries.push(hir::Query {
            id: query,
            parent: None,
            captures,
            reachable_ctes: Vec::new(),
            blocks: Vec::new(),
            first: hir::QueryBlockId::new(query, 0),
            compounds: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            output: Vec::new(),
        });
        document
    }

    fn hir_document_with_index(
        source: hir::SourceId,
        table: &JoinedTable,
        index: Arc<Index>,
        columns: Vec<Option<hir::Expr>>,
        predicate: Option<hir::Expr>,
    ) -> hir::HirDocument {
        let index_id = hir::CatalogObjectId::new(2);
        let resolved_index = hir::CatalogObject::new(
            index_id,
            hir::CatalogSnapshot::from_id(0),
            Some(hir::DatabaseId::new(crate::MAIN_DB_ID)),
            index,
        );
        let resolved_table = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(0),
            Some(hir::DatabaseId::new(crate::MAIN_DB_ID)),
            Arc::new(table.table.clone()),
        );
        let width = table.columns().len();
        let mut document = empty_hir_document(source);
        *document
            .sources
            .get_mut(source.index())
            .expect("test source slot exists") = hir::Source {
            id: source,
            owner: hir::SourceOwner::Root,
            database: Some(hir::DatabaseId::new(crate::MAIN_DB_ID)),
            name: "items".into(),
            alias: None,
            kind: hir::SourceKind::Table(resolved_table),
            columns: table
                .columns()
                .iter()
                .map(|column| hir::SourceColumn {
                    name: column.name.clone().expect("test column has a name"),
                    type_fact: hir::TypeFact::known(crate::schema::Type::Integer),
                    affinity: Affinity::Integer,
                    has_affinity: true,
                    collation: None,
                    hidden: false,
                    rowid_alias: column.is_rowid_alias(),
                })
                .collect(),
            generated_expressions: vec![hir::ColumnReadExpression::Absent; width],
            default_expressions: vec![hir::ColumnReadExpression::Absent; width],
            column_type_programs: vec![None; width],
            check_constraints: None,
            rowid_available: true,
            index_hint: hir::IndexHint::None,
            index_expressions: vec![hir::IndexExpressions {
                index: resolved_index,
                columns,
                predicate,
            }],
            index_coverage: hir::IndexCoverage::Complete {
                indexes: vec![index_id],
            },
            index_method_patterns: Vec::new(),
        };
        document
    }

    fn table_with_columns(columns: Vec<Column>) -> JoinedTable {
        let table = crate::schema::Table::BTree(Arc::new(crate::schema::BTreeTable::new(
            1,
            "items".into(),
            Vec::new(),
            columns,
            crate::schema::BTreeCharacteristics::HAS_ROWID,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        )));
        JoinedTable {
            op: crate::translate::plan::Operation::default_scan_for(&table),
            table,
            identifier: "items".into(),
            internal_id: TableInternalId::default(),
            join_info: None,
            col_used_mask: crate::translate::plan::ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: crate::MAIN_DB_ID,
            indexed: None,
        }
    }

    fn rowid_table() -> JoinedTable {
        table_with_columns(Vec::new())
    }

    fn hir_planned_source(source: hir::SourceId, table: &JoinedTable) -> HirPlannedSource {
        HirPlannedSource {
            op: (),
            table: table.table.clone(),
            identifier: (),
            internal_id: source,
            join_info: None,
            col_used_mask: table.col_used_mask.clone(),
            column_use_counts: table.column_use_counts.clone(),
            expression_index_usages: Vec::new(),
            database_id: table.database_id,
            indexed: hir::IndexHint::None,
        }
    }

    fn hir_comparison(
        lhs: hir::Expr,
        operator: ast::Operator,
        rhs: hir::Expr,
        affinity: Affinity,
    ) -> hir::Expr {
        hir_comparison_with_collation(lhs, operator, rhs, affinity, None)
    }

    fn hir_binary(lhs: hir::Expr, operator: ast::Operator, rhs: hir::Expr) -> hir::Expr {
        hir::Expr::Binary {
            lhs: Box::new(lhs),
            operator,
            rhs: Box::new(rhs),
            array_concat: false,
            custom: None,
            comparison: None,
        }
    }

    fn hir_comparison_with_collation(
        lhs: hir::Expr,
        operator: ast::Operator,
        rhs: hir::Expr,
        affinity: Affinity,
        collation: Option<hir::ResolvedCollation>,
    ) -> hir::Expr {
        hir::Expr::Binary {
            lhs: Box::new(lhs),
            operator,
            rhs: Box::new(rhs),
            array_concat: false,
            custom: None,
            comparison: Some(hir::ComparisonSemantics {
                components: vec![hir::ComparisonComponent {
                    affinity,
                    collation,
                    array: false,
                }],
            }),
        }
    }

    fn resolved_collation(value: CollationSeq) -> hir::ResolvedCollation {
        hir::ResolvedCollation::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(0),
            None,
            Arc::new(value),
        )
    }

    fn hir_in_list(
        lhs: hir::Expr,
        negated: bool,
        values: Vec<hir::Expr>,
        affinity: Affinity,
        collation: Option<CollationSeq>,
    ) -> hir::Expr {
        let comparisons = values
            .iter()
            .map(|_| hir::ComparisonSemantics {
                components: vec![hir::ComparisonComponent {
                    affinity,
                    collation: collation.map(resolved_collation),
                    array: false,
                }],
            })
            .collect();
        hir::Expr::InList {
            lhs: Box::new(lhs),
            negated,
            values,
            comparisons,
        }
    }

    #[test]
    fn hir_constraints_use_shared_index_rules() {
        let constraint = HirConstraint {
            where_clause_pos: (0, BinaryExprSide::Rhs),
            operator: ast::Operator::Equals.into(),
            table_col_pos: Some(2),
            expr: Some(hir::Expr::Literal(ast::Literal::Numeric("1".into()))),
            constraining_expr: None,
            lhs_mask: TableMask::default(),
            selectivity: 0.1,
            usable: true,
            is_rowid: false,
            comparison_affinity: Some(Affinity::Integer),
            comparison_collation: None,
            null_matching: false,
        };
        let table_constraints = HirTableConstraints {
            table_id: hir::SourceId::new(7),
            constraints: vec![constraint],
            candidates: Vec::new(),
            temporary_index_terms: SmallVec::new(),
        };

        assert!(table_constraints.constraints[0].satisfies_index_affinity(Affinity::Integer));
        assert_eq!(table_constraints.table_id, hir::SourceId::new(7));
        let key_columns = ordered_ephemeral_key_columns(&[&table_constraints.constraints[0]]);
        assert_eq!(key_columns.as_slice(), [2]);
    }

    #[test]
    fn shared_btree_selector_chooses_hir_in_seek() -> Result<()> {
        let source = hir::SourceId::new(7);
        let constraints = HirTableConstraints {
            table_id: source,
            constraints: vec![HirConstraint {
                where_clause_pos: (3, BinaryExprSide::Rhs),
                operator: ConstraintOperator::In {
                    not: false,
                    estimated_values: 2.0,
                },
                table_col_pos: None,
                expr: None,
                constraining_expr: None,
                lhs_mask: TableMask::default(),
                selectivity: 0.1,
                usable: false,
                is_rowid: true,
                comparison_affinity: Some(Affinity::Integer),
                comparison_collation: None,
                null_matching: false,
            }],
            candidates: vec![ConstraintUseCandidate {
                index: None,
                refs: Vec::new(),
                partial_index: None,
            }],
            temporary_index_terms: SmallVec::new(),
        };
        let params = CostModelParams::default();
        let table = rowid_table();
        let planned = hir_planned_source(source, &table);
        let document = empty_hir_document(source);
        let access_source = crate::translate::optimizer::access_method::HirAccessSource::new(
            &planned,
            document
                .source(source)
                .expect("test document contains HIR source"),
        );

        let access_method =
            crate::translate::optimizer::access_method::choose_single_btree_access_method(
                &access_source,
                &constraints,
                &TableMask::default(),
                source.index(),
                None,
                &[],
                &Schema::new(),
                &crate::stats::AnalyzeStats::default(),
                1.0,
                crate::translate::optimizer::cost::RowCountEstimate::hardcoded_fallback(&params),
                &params,
            )?;

        assert_eq!(
            (&access_method.consumed_where_terms)
                .into_iter()
                .collect::<Vec<_>>(),
            [3]
        );
        assert!(matches!(
            access_method.params,
            crate::translate::optimizer::access_method::AccessMethodParams::InSeek {
                index: None,
                affinity: Affinity::Integer,
                where_term_idx: 3,
            }
        ));
        Ok(())
    }

    #[test]
    fn shared_btree_selector_builds_hir_automatic_index() -> Result<()> {
        let source = hir::SourceId::new(7);
        let mut prerequisite = TableMask::default();
        prerequisite.set(0)?;
        let constraints = HirTableConstraints {
            table_id: source,
            constraints: vec![HirConstraint {
                where_clause_pos: (4, BinaryExprSide::Rhs),
                operator: ast::Operator::Equals.into(),
                table_col_pos: Some(0),
                expr: None,
                constraining_expr: None,
                lhs_mask: prerequisite.clone(),
                selectivity: CostModelParams::default().sel_eq_unindexed,
                usable: true,
                is_rowid: false,
                comparison_affinity: Some(Affinity::Integer),
                comparison_collation: None,
                null_matching: false,
            }],
            candidates: vec![ConstraintUseCandidate {
                index: None,
                refs: Vec::new(),
                partial_index: None,
            }],
            temporary_index_terms: smallvec::smallvec![ConstraintRef {
                constraint_vec_pos: 0,
                index_col_pos: 0,
                sort_order: SortOrder::Asc,
                nulls_order: ast::NullsOrder::First,
            }],
        };
        let table = table_with_columns(vec![Column::new_default_integer(
            Some("a".into()),
            "INTEGER".into(),
            None,
        )]);
        let mut planned = hir_planned_source(source, &table);
        planned.col_used_mask.set(0)?;
        let document = empty_hir_document(source);
        let access_source = crate::translate::optimizer::access_method::HirAccessSource::new(
            &planned,
            document
                .source(source)
                .expect("test document contains HIR source"),
        );
        let params = CostModelParams::default();

        let access_method =
            crate::translate::optimizer::access_method::choose_single_btree_access_method(
                &access_source,
                &constraints,
                &prerequisite,
                1,
                None,
                &[],
                &Schema::new(),
                &crate::stats::AnalyzeStats::default(),
                100.0,
                crate::translate::optimizer::cost::RowCountEstimate::AnalyzeStats(10_000.0),
                &params,
            )?;

        assert_eq!(
            (&access_method.consumed_where_terms)
                .into_iter()
                .collect::<Vec<_>>(),
            [4]
        );
        assert!(matches!(
            access_method.params,
            crate::translate::optimizer::access_method::AccessMethodParams::BTreeTable {
                index: None,
                build_index: true,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn hir_binary_constraint_parts_follow_the_constrained_source() {
        let source = hir::SourceId::new(7);
        let left = hir_comparison(
            hir::Expr::column(source, 2),
            ast::Operator::GreaterEquals,
            hir::Expr::Literal(ast::Literal::Numeric("10".into())),
            Affinity::Integer,
        );
        let left_parts = hir_binary_constraint_parts(&left, source);
        let [left_part] = left_parts.as_slice() else {
            panic!("left column produces one constraint part");
        };
        assert_eq!(
            left_part.target,
            HirConstraintTarget::Column(hir::ColumnRef { source, column: 2 })
        );
        assert_eq!(
            left_part.operator,
            ConstraintOperator::AstNativeOperator(ast::Operator::GreaterEquals)
        );
        assert_eq!(left_part.side, BinaryExprSide::Rhs);
        assert!(matches!(
            left_part.constraining_expr,
            hir::Expr::Literal(ast::Literal::Numeric(value)) if value == "10"
        ));
        assert_eq!(left_part.comparison_affinity, Some(Affinity::Integer));

        let right = hir_comparison(
            hir::Expr::Literal(ast::Literal::Numeric("1".into())),
            ast::Operator::Greater,
            hir::Expr::rowid(source),
            Affinity::Integer,
        );
        let right_parts = hir_binary_constraint_parts(&right, source);
        let [right_part] = right_parts.as_slice() else {
            panic!("right rowid produces one constraint part");
        };
        assert_eq!(right_part.target, HirConstraintTarget::RowId(source));
        assert_eq!(
            right_part.operator,
            ConstraintOperator::AstNativeOperator(ast::Operator::Less)
        );
        assert_eq!(right_part.side, BinaryExprSide::Lhs);
        assert!(matches!(
            right_part.constraining_expr,
            hir::Expr::Literal(ast::Literal::Numeric(value)) if value == "1"
        ));
        assert_eq!(right_part.comparison_affinity, Some(Affinity::Integer));
    }

    #[test]
    fn hir_binary_constraint_uses_resolved_masks_and_outer_join_rules() -> Result<()> {
        let left_source = hir::SourceId::new(8);
        let source = hir::SourceId::new(7);
        let from = hir::From {
            first: left_source,
            joins: vec![hir::Join {
                right: source,
                kind: hir::JoinKind::Left,
                constraint: hir::JoinConstraint::None,
            }],
        };
        let term = HirWhereTerm {
            expr: hir_comparison_with_collation(
                hir::Expr::rowid(source),
                ast::Operator::Is,
                hir::Expr::column(left_source, 0),
                Affinity::Integer,
                Some(resolved_collation(CollationSeq::NoCase)),
            ),
            from_outer_join: None,
            consumed: false,
        };
        let table = rowid_table();
        let table = hir_planned_source(source, &table);
        let constraints = hir_binary_constraints_for_source(
            &empty_hir_document(source),
            &from,
            &[term],
            HirConstraintSource::BTree(&table),
            &Schema::new(),
            &CostModelParams::default(),
        )?;
        let [constraint] = constraints.as_slice() else {
            panic!("rowid comparison becomes one constraint");
        };

        assert_eq!(constraint.where_clause_pos, (0, BinaryExprSide::Rhs));
        assert_eq!(constraint.table_col_pos, None);
        assert!(constraint.is_rowid);
        assert!(constraint.null_matching);
        assert!(!constraint.usable);
        assert!(constraint.lhs_mask.get(0));
        assert!(!constraint.lhs_mask.get(1));
        assert_eq!(constraint.comparison_affinity, Some(Affinity::Integer));
        assert_eq!(constraint.comparison_collation, Some(CollationSeq::NoCase));
        Ok(())
    }

    #[test]
    fn hir_binary_constraint_collection_keeps_both_sides_and_sorts_equalities() -> Result<()> {
        let source = hir::SourceId::new(7);
        let from = hir::From {
            first: source,
            joins: Vec::new(),
        };
        let range = HirWhereTerm {
            expr: hir_comparison(
                hir::Expr::column(source, 0),
                ast::Operator::Greater,
                hir::Expr::Literal(ast::Literal::Numeric("1".into())),
                Affinity::Integer,
            ),
            from_outer_join: None,
            consumed: false,
        };
        let equality = HirWhereTerm {
            expr: hir_comparison(
                hir::Expr::column(source, 0),
                ast::Operator::Equals,
                hir::Expr::column(source, 1),
                Affinity::Integer,
            ),
            from_outer_join: None,
            consumed: false,
        };
        let table = table_with_columns(vec![
            Column::new_default_integer(Some("a".into()), "INTEGER".into(), None),
            Column::new_default_integer(Some("b".into()), "INTEGER".into(), None),
        ]);
        let table = hir_planned_source(source, &table);

        let constraints = hir_binary_constraints_for_source(
            &empty_hir_document(source),
            &from,
            &[range, equality],
            HirConstraintSource::BTree(&table),
            &Schema::new(),
            &CostModelParams::default(),
        )?;

        assert_eq!(constraints.len(), 3);
        assert_eq!(constraints[0].operator, ast::Operator::Equals.into());
        assert_eq!(constraints[0].table_col_pos, Some(0));
        assert_eq!(constraints[1].operator, ast::Operator::Equals.into());
        assert_eq!(constraints[1].table_col_pos, Some(1));
        assert_eq!(constraints[2].operator, ast::Operator::Greater.into());
        assert_eq!(constraints[2].table_col_pos, Some(0));
        Ok(())
    }

    #[test]
    fn hir_derived_scan_constraints_reduce_rows_without_access_candidates() -> Result<()> {
        let source = hir::SourceId::new(7);
        let from = hir::From {
            first: source,
            joins: Vec::new(),
        };
        let equality = HirWhereTerm {
            expr: hir_comparison(
                hir::Expr::column(source, 0),
                ast::Operator::Equals,
                hir::Expr::Literal(ast::Literal::Numeric("1".into())),
                Affinity::Integer,
            ),
            from_outer_join: None,
            consumed: false,
        };
        let in_list = HirWhereTerm {
            expr: hir_in_list(
                hir::Expr::column(source, 0),
                false,
                vec![
                    hir::Expr::Literal(ast::Literal::Numeric("2".into())),
                    hir::Expr::Literal(ast::Literal::Numeric("3".into())),
                ],
                Affinity::Integer,
                None,
            ),
            from_outer_join: None,
            consumed: false,
        };
        let params = CostModelParams::default();
        let constraints = hir_constraints_for_source(
            &empty_hir_document(source),
            &from,
            &[equality, in_list],
            HirConstraintSource::Derived {
                source,
                row_count: RowCountEstimate::AnalyzeStats(40.0),
            },
            &Schema::new(),
            &params,
            &|query| panic!("derived scan constraint references unexpected query {query}"),
        )?;

        assert_eq!(constraints.table_id, source);
        assert!(constraints.candidates.is_empty());
        assert!(constraints.temporary_index_terms.is_empty());
        assert_eq!(constraints.constraints.len(), 2);
        assert_eq!(
            constraints.constraints[0].selectivity,
            params.sel_eq_unindexed
        );
        assert_eq!(constraints.constraints[1].selectivity, 2.0 / 40.0);
        Ok(())
    }

    #[test]
    fn hir_in_list_constraint_uses_frozen_comparison_and_rhs_mask() -> Result<()> {
        let source = hir::SourceId::new(7);
        let rhs_source = hir::SourceId::new(8);
        let from = hir::From {
            first: source,
            joins: vec![hir::Join {
                right: rhs_source,
                kind: hir::JoinKind::Inner,
                constraint: hir::JoinConstraint::None,
            }],
        };
        let term = HirWhereTerm {
            expr: hir_in_list(
                hir::Expr::column(source, 0),
                false,
                vec![
                    hir::Expr::Literal(ast::Literal::Numeric("1".into())),
                    hir::Expr::column(rhs_source, 0),
                ],
                Affinity::Integer,
                Some(CollationSeq::NoCase),
            ),
            from_outer_join: None,
            consumed: false,
        };
        let table = table_with_columns(vec![Column::new_default_integer(
            Some("a".into()),
            "INTEGER".into(),
            None,
        )]);
        let table = hir_planned_source(source, &table);

        let constraints = hir_constraints_for_source(
            &empty_hir_document(source),
            &from,
            &[term],
            HirConstraintSource::BTree(&table),
            &Schema::new(),
            &CostModelParams::default(),
            &|_| None,
        )?;
        let [constraint] = constraints.constraints.as_slice() else {
            panic!("scalar IN list becomes one constraint");
        };

        assert_eq!(constraint.where_clause_pos, (0, BinaryExprSide::Rhs));
        assert_eq!(constraint.table_col_pos, Some(0));
        assert!(!constraint.is_rowid);
        assert!(!constraint.usable);
        assert_eq!(
            constraint.operator,
            ConstraintOperator::In {
                not: false,
                estimated_values: 2.0,
            }
        );
        assert!(!constraint.lhs_mask.get(0));
        assert!(constraint.lhs_mask.get(1));
        assert_eq!(constraint.comparison_affinity, Some(Affinity::Integer));
        assert_eq!(constraint.comparison_collation, Some(CollationSeq::NoCase));
        Ok(())
    }

    #[test]
    fn hir_in_list_constraint_keeps_rowid_alias_and_rejects_mixed_comparisons() -> Result<()> {
        let source = hir::SourceId::new(7);
        let from = hir::From {
            first: source,
            joins: Vec::new(),
        };
        let mut expression = hir_in_list(
            hir::Expr::column(source, 0),
            true,
            vec![
                hir::Expr::Literal(ast::Literal::Numeric("1".into())),
                hir::Expr::Literal(ast::Literal::Numeric("2".into())),
            ],
            Affinity::Integer,
            None,
        );
        let mut rowid_alias =
            Column::new_default_integer(Some("id".into()), "INTEGER".into(), None);
        rowid_alias.set_rowid_alias(true);
        let table = table_with_columns(vec![rowid_alias]);
        let table = hir_planned_source(source, &table);
        let term = |expr| HirWhereTerm {
            expr,
            from_outer_join: None,
            consumed: false,
        };

        let constraint = hir_in_list_constraint_for_term(
            &empty_hir_document(source),
            &from,
            0,
            &term(expression.clone()),
            HirConstraintSource::BTree(&table),
            &Schema::new(),
            &CostModelParams::default(),
        )?
        .expect("rowid-alias IN list becomes a constraint");
        assert_eq!(constraint.table_col_pos, Some(0));
        assert!(constraint.is_rowid);
        assert!(matches!(
            constraint.operator,
            ConstraintOperator::In { not: true, .. }
        ));

        let hir::Expr::InList { comparisons, .. } = &mut expression else {
            unreachable!();
        };
        comparisons[1].components[0].collation = Some(resolved_collation(CollationSeq::NoCase));
        assert!(hir_in_list_constraint_for_term(
            &empty_hir_document(source),
            &from,
            0,
            &term(expression),
            HirConstraintSource::BTree(&table),
            &Schema::new(),
            &CostModelParams::default(),
        )?
        .is_none());
        Ok(())
    }

    #[test]
    fn hir_in_query_constraint_uses_captures_comparison_and_planned_rows() -> Result<()> {
        let source = hir::SourceId::new(7);
        let captured = hir::SourceId::new(8);
        let query = hir::QueryId::new(0);
        let from = hir::From {
            first: source,
            joins: Vec::new(),
        };
        let expression = hir::Expr::Subquery(hir::SubqueryExpr::In {
            lhs: Box::new(hir::Expr::column(source, 0)),
            query,
            negated: false,
            comparison: hir::ComparisonSemantics {
                components: vec![hir::ComparisonComponent {
                    affinity: Affinity::Text,
                    collation: Some(resolved_collation(CollationSeq::NoCase)),
                    array: false,
                }],
            },
        });
        let term = HirWhereTerm {
            expr: expression.clone(),
            from_outer_join: None,
            consumed: false,
        };
        let table = table_with_columns(vec![Column::new_default_text(
            Some("a".into()),
            "TEXT".into(),
            None,
        )]);
        let table = hir_planned_source(source, &table);
        let params = CostModelParams::default();
        let row_count = params.rows_per_table_fallback;
        let document = hir_document_with_query(source, query, Vec::new());

        let constraints = hir_constraints_for_source(
            &document,
            &from,
            std::slice::from_ref(&term),
            HirConstraintSource::BTree(&table),
            &Schema::new(),
            &params,
            &|planned_query| {
                assert_eq!(planned_query, query);
                Some(row_count * 2.0)
            },
        )?;
        let [constraint] = constraints.constraints.as_slice() else {
            panic!("uncorrelated scalar IN query becomes one constraint");
        };
        assert_eq!(
            constraint.operator,
            ConstraintOperator::In {
                not: false,
                estimated_values: row_count.sqrt().max(1.0),
            }
        );
        assert_eq!(constraint.comparison_affinity, Some(Affinity::Text));
        assert_eq!(constraint.comparison_collation, Some(CollationSeq::NoCase));
        assert_eq!(constraint.lhs_mask.count(), 0);

        let fallback = hir_in_query_constraint_for_term(
            &document,
            0,
            &term,
            HirConstraintSource::BTree(&table),
            &Schema::new(),
            &params,
            &|_| None,
        )?
        .expect("missing plan estimate uses existing fallback");
        assert_eq!(
            fallback.operator,
            ConstraintOperator::In {
                not: false,
                estimated_values: params.in_subquery_rows.min(row_count),
            }
        );

        let correlated = hir_document_with_query(source, query, vec![captured]);
        assert!(hir_in_query_constraint_for_term(
            &correlated,
            0,
            &HirWhereTerm {
                expr: expression,
                from_outer_join: None,
                consumed: false,
            },
            HirConstraintSource::BTree(&table),
            &Schema::new(),
            &params,
            &|_| panic!("correlated query must not request a row estimate"),
        )?
        .is_none());
        Ok(())
    }

    #[test]
    fn hir_table_constraints_attach_compatible_index_candidates() -> Result<()> {
        let source = hir::SourceId::new(7);
        let from = hir::From {
            first: source,
            joins: Vec::new(),
        };
        let compatible = HirWhereTerm {
            expr: hir_comparison(
                hir::Expr::column(source, 0),
                ast::Operator::Equals,
                hir::Expr::Literal(ast::Literal::Numeric("1".into())),
                Affinity::Integer,
            ),
            from_outer_join: None,
            consumed: false,
        };
        let incompatible = HirWhereTerm {
            expr: hir_comparison_with_collation(
                hir::Expr::column(source, 0),
                ast::Operator::Equals,
                hir::Expr::Literal(ast::Literal::Numeric("2".into())),
                Affinity::Integer,
                Some(resolved_collation(CollationSeq::NoCase)),
            ),
            from_outer_join: None,
            consumed: false,
        };
        let table = table_with_columns(vec![Column::new_default_integer(
            Some("a".into()),
            "INTEGER".into(),
            None,
        )]);
        let index = Arc::new(Index {
            name: "items_a".into(),
            table_name: "items".into(),
            root_page: 2,
            columns: vec![crate::schema::IndexColumn::new("a", 0)],
            unique: false,
            ephemeral: false,
            has_rowid: true,
            where_clause: None,
            index_method: None,
            on_conflict: None,
        });
        let document = hir_document_with_index(source, &table, index.clone(), vec![None], None);
        let table = hir_planned_source(source, &table);

        let constraints = hir_constraints_for_source(
            &document,
            &from,
            &[compatible, incompatible],
            HirConstraintSource::BTree(&table),
            &Schema::new(),
            &CostModelParams::default(),
            &|_| None,
        )?;

        assert_eq!(constraints.table_id, source);
        assert!(constraints.constraints[0].usable);
        assert!(!constraints.constraints[1].usable);
        let index_candidate = constraints
            .candidates
            .iter()
            .find(|candidate| {
                candidate
                    .index
                    .as_ref()
                    .is_some_and(|value| Arc::ptr_eq(value, &index))
            })
            .expect("ordinary index remains a candidate");
        assert_eq!(index_candidate.refs.len(), 1);
        assert_eq!(index_candidate.refs[0].constraint_vec_pos, 0);
        assert_eq!(index_candidate.refs[0].index_col_pos, 0);
        assert_eq!(constraints.temporary_index_terms.len(), 1);
        assert_eq!(constraints.temporary_index_terms[0].constraint_vec_pos, 0);
        Ok(())
    }

    #[test]
    fn hir_table_constraints_match_expression_index_candidates() -> Result<()> {
        let source = hir::SourceId::new(0);
        let from = hir::From {
            first: source,
            joins: Vec::new(),
        };
        let term = HirWhereTerm {
            expr: hir_comparison(
                hir_binary(
                    hir::Expr::column(source, 0),
                    ast::Operator::Add,
                    hir::Expr::column(source, 1),
                ),
                ast::Operator::Equals,
                hir::Expr::Literal(ast::Literal::Numeric("10".into())),
                Affinity::Integer,
            ),
            from_outer_join: None,
            consumed: false,
        };
        let table = table_with_columns(vec![
            Column::new_default_integer(Some("a".into()), "INTEGER".into(), None),
            Column::new_default_integer(Some("b".into()), "INTEGER".into(), None),
        ]);
        let index = Arc::new(Index {
            name: "items_sum".into(),
            table_name: "items".into(),
            root_page: 2,
            columns: vec![crate::schema::IndexColumn {
                name: "a + b".into(),
                order: SortOrder::Asc,
                nulls_order: None,
                pos_in_table: crate::schema::EXPR_INDEX_SENTINEL,
                collation: None,
                default: None,
                expr: Some(Box::new(ast::Expr::Literal(ast::Literal::Null))),
            }],
            unique: false,
            ephemeral: false,
            has_rowid: true,
            where_clause: None,
            index_method: None,
            on_conflict: None,
        });
        let document = hir_document_with_index(
            source,
            &table,
            index.clone(),
            vec![Some(hir_binary(
                hir::Expr::column(source, 1),
                ast::Operator::Add,
                hir::Expr::column(source, 0),
            ))],
            None,
        );
        let table = hir_planned_source(source, &table);

        let params = CostModelParams::default();
        let constraints = hir_constraints_for_source(
            &document,
            &from,
            &[term],
            HirConstraintSource::BTree(&table),
            &Schema::new(),
            &params,
            &|_| None,
        )?;

        let candidate = constraints
            .candidates
            .iter()
            .find(|candidate| {
                candidate
                    .index
                    .as_ref()
                    .is_some_and(|value| Arc::ptr_eq(value, &index))
            })
            .expect("expression index remains a candidate");
        assert_eq!(candidate.refs.len(), 1);
        assert_eq!(candidate.refs[0].constraint_vec_pos, 0);
        assert_eq!(candidate.refs[0].index_col_pos, 0);
        assert!(constraints.constraints[0].expr.is_none());

        let scan_constraints = hir_constraints_for_source(
            &document,
            &from,
            &[],
            HirConstraintSource::BTree(&table),
            &Schema::new(),
            &params,
            &|_| None,
        )?;
        let target_expression = hir_binary(
            hir::Expr::column(source, 0),
            ast::Operator::Add,
            hir::Expr::column(source, 1),
        );
        let order_target: crate::translate::optimizer::order::HirOrderTarget<'_> =
            crate::translate::optimizer::order::OrderTarget {
                columns: vec![crate::translate::optimizer::order::ColumnOrder {
                    source,
                    target: crate::translate::optimizer::order::ColumnTarget::Expr(
                        &target_expression,
                    ),
                    order: SortOrder::Desc,
                    collation: CollationSeq::Binary,
                    nulls_order: None,
                }],
                purpose: crate::translate::optimizer::order::OrderTargetPurpose::EliminatesSort(
                    crate::translate::optimizer::order::EliminatesSortBy::Order,
                ),
            };
        let access_source = crate::translate::optimizer::access_method::HirAccessSource::new(
            &table,
            document
                .source(source)
                .expect("test document contains HIR source"),
        );
        let chosen = crate::translate::optimizer::access_method::choose_best_btree_candidate(
            &access_source,
            &scan_constraints,
            &TableMask::default(),
            source.index(),
            Some(&order_target),
            &Schema::new(),
            &crate::stats::AnalyzeStats::default(),
            1.0,
            crate::translate::optimizer::cost::RowCountEstimate::AnalyzeStats(10_000.0),
            &params,
        )?
        .expect("HIR constraints produce a B-tree candidate");
        assert!(chosen
            .index
            .as_ref()
            .is_some_and(|value| Arc::ptr_eq(value, &index)));
        assert_eq!(
            chosen.iter_dir,
            crate::translate::plan::IterationDirection::Backwards
        );
        Ok(())
    }

    #[test]
    fn hir_partial_index_requires_every_hir_predicate_conjunct() -> Result<()> {
        let source = hir::SourceId::new(0);
        let from = hir::From {
            first: source,
            joins: Vec::new(),
        };
        let seek = HirWhereTerm {
            expr: hir_comparison(
                hir::Expr::column(source, 0),
                ast::Operator::Equals,
                hir::Expr::Literal(ast::Literal::Numeric("10".into())),
                Affinity::Integer,
            ),
            from_outer_join: None,
            consumed: false,
        };
        let lower = HirWhereTerm {
            expr: hir_comparison(
                hir::Expr::column(source, 1),
                ast::Operator::Greater,
                hir::Expr::Literal(ast::Literal::Numeric("0".into())),
                Affinity::Integer,
            ),
            from_outer_join: None,
            consumed: false,
        };
        let upper = HirWhereTerm {
            expr: hir_comparison(
                hir::Expr::column(source, 1),
                ast::Operator::Less,
                hir::Expr::Literal(ast::Literal::Numeric("100".into())),
                Affinity::Integer,
            ),
            from_outer_join: None,
            consumed: false,
        };
        let predicate = hir_binary(lower.expr.clone(), ast::Operator::And, upper.expr.clone());
        let table = table_with_columns(vec![
            Column::new_default_integer(Some("a".into()), "INTEGER".into(), None),
            Column::new_default_integer(Some("b".into()), "INTEGER".into(), None),
        ]);
        let index = Arc::new(Index {
            name: "items_a_partial".into(),
            table_name: "items".into(),
            root_page: 2,
            columns: vec![crate::schema::IndexColumn::new("a", 0)],
            unique: false,
            ephemeral: false,
            has_rowid: true,
            where_clause: Some(Box::new(ast::Expr::Literal(ast::Literal::True))),
            index_method: None,
            on_conflict: None,
        });
        let document =
            hir_document_with_index(source, &table, index.clone(), vec![None], Some(predicate));
        let table = hir_planned_source(source, &table);
        let params = CostModelParams::default();

        let accepted = hir_constraints_for_source(
            &document,
            &from,
            &[seek.clone(), lower.clone(), upper],
            HirConstraintSource::BTree(&table),
            &Schema::new(),
            &params,
            &|_| None,
        )?;
        let accepted_partial = accepted
            .candidates
            .iter()
            .find(|candidate| {
                candidate
                    .index
                    .as_ref()
                    .is_some_and(|value| Arc::ptr_eq(value, &index))
            })
            .expect("implied partial index remains a candidate");
        assert_eq!(
            accepted_partial
                .partial_index
                .as_ref()
                .map(|partial_index| partial_index.selectivity),
            Some(params.sel_range * params.sel_range)
        );
        assert_eq!(
            accepted_partial
                .partial_index
                .as_ref()
                .expect("partial index carries facts")
                .predicate_terms
                .as_slice(),
            [1, 2]
        );

        let access_source = crate::translate::optimizer::access_method::HirAccessSource::new(
            &table,
            document
                .source(source)
                .expect("test document contains HIR source"),
        );
        let base_row_count =
            crate::translate::optimizer::cost::RowCountEstimate::AnalyzeStats(10_000.0);
        let chosen = crate::translate::optimizer::access_method::choose_best_btree_candidate(
            &access_source,
            &accepted,
            &TableMask::default(),
            source.index(),
            None,
            &Schema::new(),
            &crate::stats::AnalyzeStats::default(),
            1.0,
            base_row_count,
            &params,
        )?
        .expect("HIR constraints produce a B-tree candidate");
        assert!(chosen
            .index
            .as_ref()
            .is_some_and(|value| Arc::ptr_eq(value, &index)));
        assert_eq!(chosen.partial_index_predicate_terms.as_slice(), [1, 2]);
        assert_eq!(
            chosen.base_row_count,
            crate::translate::optimizer::cost::RowCountEstimate::AnalyzeStats(
                10_000.0
                    * accepted_partial
                        .partial_index
                        .as_ref()
                        .expect("partial index carries facts")
                        .selectivity
            )
        );
        let access_method =
            crate::translate::optimizer::access_method::choose_single_btree_access_method(
                &access_source,
                &accepted,
                &TableMask::default(),
                source.index(),
                None,
                &[],
                &Schema::new(),
                &crate::stats::AnalyzeStats::default(),
                1.0,
                base_row_count,
                &params,
            )?;
        assert_eq!(
            (&access_method.consumed_where_terms)
                .into_iter()
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert!(matches!(
            &access_method.params,
            crate::translate::optimizer::access_method::AccessMethodParams::BTreeTable {
                index: Some(chosen_index),
                build_index: false,
                constraint_refs,
                ..
            } if Arc::ptr_eq(chosen_index, &index) && !constraint_refs.is_empty()
        ));

        let rejected = hir_constraints_for_source(
            &document,
            &from,
            &[seek, lower],
            HirConstraintSource::BTree(&table),
            &Schema::new(),
            &params,
            &|_| None,
        )?;
        assert!(!rejected.candidates.iter().any(|candidate| {
            candidate
                .index
                .as_ref()
                .is_some_and(|value| Arc::ptr_eq(value, &index))
        }));
        Ok(())
    }

    #[test]
    fn hir_partial_index_respects_outer_join_predicate_origin() {
        let left = hir::SourceId::new(0);
        let source = hir::SourceId::new(1);
        let predicate = hir_comparison(
            hir::Expr::column(source, 0),
            ast::Operator::Greater,
            hir::Expr::Literal(ast::Literal::Numeric("0".into())),
            Affinity::Integer,
        );
        let mut term = HirWhereTerm {
            expr: predicate.clone(),
            from_outer_join: None,
            consumed: false,
        };
        let mut from = hir::From {
            first: left,
            joins: vec![hir::Join {
                right: source,
                kind: hir::JoinKind::Left,
                constraint: hir::JoinConstraint::None,
            }],
        };

        assert!(hir_partial_index_predicate_terms(
            &from,
            source,
            &predicate,
            std::slice::from_ref(&term),
        )
        .is_none());
        term.from_outer_join = Some(source);
        let matched = hir_partial_index_predicate_terms(
            &from,
            source,
            &predicate,
            std::slice::from_ref(&term),
        )
        .expect("matching LEFT JOIN predicate proves partial index");
        assert_eq!(matched.as_slice(), [0]);

        from.joins[0].kind = hir::JoinKind::Full;
        assert!(hir_partial_index_predicate_terms(
            &from,
            source,
            &predicate,
            std::slice::from_ref(&term),
        )
        .is_none());
    }
}
