use crate::alloc::TursoIteratorExt;
use crate::{
    schema::{Column, Index, Schema},
    translate::{
        plan::{
            JoinOrderMember, JoinedTable, NonFromClauseSubquery, PlanIndex, TableReferences,
            WhereTerm,
        },
        plan_expr::{
            plan_expr_affinity, plan_expr_collation, plan_expr_type_fact, plan_expr_vector_size,
            plan_exprs_are_equivalent, resolve_plan_comparison_affinity, PlanExpr,
            PlanExprAffinity, PlanExprFactSource, PlanSourceId, PlanSubqueryExpr, PlanSubqueryId,
        },
        planner::TableMask,
    },
    vdbe::affinity::Affinity,
    Result,
};
use crate::{turso_assert, turso_debug_assert};
use smallvec::SmallVec;
use std::{cmp::Ordering, collections::VecDeque};
use turso_ext::{ConstraintInfo, ConstraintOp};
use turso_parser::ast::{self, SortOrder};

use super::{cost_params::CostModelParams, AvailableIndexes};

struct ConstraintFacts<'a> {
    subqueries: &'a [NonFromClauseSubquery],
}

impl PlanExprFactSource for ConstraintFacts<'_> {
    fn subquery_output_type_fact(
        &self,
        query: PlanSubqueryId,
        output: usize,
    ) -> Option<crate::translate::semantic::hir::TypeFact> {
        self.subqueries
            .iter()
            .find(|subquery| subquery.internal_id == query)?
            .output_facts
            .get(output)
            .map(|fact| fact.type_fact.clone())
    }

    fn subquery_width(&self, query: PlanSubqueryId) -> Option<usize> {
        self.subqueries
            .iter()
            .find(|subquery| subquery.internal_id == query)
            .map(|subquery| subquery.output_facts.len())
    }

    fn subquery_output_affinity(
        &self,
        query: PlanSubqueryId,
        output: usize,
    ) -> Option<PlanExprAffinity> {
        self.subqueries
            .iter()
            .find(|subquery| subquery.internal_id == query)?
            .output_facts
            .get(output)
            .map(|fact| fact.affinity)
    }

    fn subquery_output_collation(
        &self,
        query: PlanSubqueryId,
        output: usize,
    ) -> Option<crate::translate::collate::CollationSeq> {
        self.subqueries
            .iter()
            .find(|subquery| subquery.internal_id == query)?
            .output_facts
            .get(output)
            .and_then(|fact| fact.collation.as_ref().map(|collation| *collation.value()))
    }
}

fn binary_components_unchecked(
    expr: &PlanExpr,
) -> Option<(&PlanExpr, ConstraintOperator, &PlanExpr)> {
    match expr {
        PlanExpr::Binary {
            lhs, operator, rhs, ..
        } if matches!(
            operator,
            ast::Operator::Equals
                | ast::Operator::NotEquals
                | ast::Operator::Greater
                | ast::Operator::Less
                | ast::Operator::GreaterEquals
                | ast::Operator::LessEquals
                | ast::Operator::Is
                | ast::Operator::IsNot
        ) =>
        {
            Some((lhs, (*operator).into(), rhs))
        }
        PlanExpr::Like {
            lhs, negated, rhs, ..
        } => Some((lhs, ConstraintOperator::Like { not: *negated }, rhs)),
        _ => None,
    }
}

fn binary_components<'a>(
    expr: &'a PlanExpr,
    facts: &impl PlanExprFactSource,
) -> Result<Option<(&'a PlanExpr, ConstraintOperator, &'a PlanExpr)>> {
    let Some((lhs, operator, rhs)) = binary_components_unchecked(expr) else {
        return Ok(None);
    };
    if matches!(expr, PlanExpr::Binary { .. })
        && (plan_expr_vector_size(lhs, facts)? > 1 || plan_expr_vector_size(rhs, facts)? > 1)
    {
        return Ok(None);
    }
    Ok(Some((lhs, operator, rhs)))
}

pub(crate) fn table_mask_from_plan_expr(
    expr: &PlanExpr,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
) -> Result<TableMask> {
    crate::translate::planner::table_mask_from_expr(expr, table_references, subqueries)
}

pub(crate) fn plan_expr_needs_no_affinity_change(affinity: Affinity, expr: &PlanExpr) -> bool {
    if !affinity.has_affinity() {
        return true;
    }
    match expr {
        PlanExpr::Literal(literal) => match literal {
            ast::Literal::Numeric(_) => affinity.is_numeric(),
            ast::Literal::String(_) => matches!(affinity, Affinity::Text),
            ast::Literal::Blob(_) => true,
            _ => false,
        },
        PlanExpr::Column(column) if column.rowid_alias => affinity.is_numeric(),
        PlanExpr::RowId(_) => affinity.is_numeric(),
        _ => false,
    }
}

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
pub struct Constraint {
    /// The position of the original `WHERE` clause term this constraint derives from,
    /// and which side of the [`PlanExpr::Binary`] comparison contains the expression
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
    pub expr: Option<PlanExpr>,
    /// For multi-index scan branches: the constraining expression and its affinity.
    /// When set, `get_constraining_expr` uses this instead of looking up in where_clause.
    /// This is needed because multi-index branches come from sub-expressions of an OR/AND,
    /// not directly from a top-level WHERE term.
    pub constraining_expr: Option<(ast::Operator, PlanExpr, Affinity)>,
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

impl Constraint {
    /// Get the constraining expression and operator, e.g. ('>=', '2+3') from 't.x >= 2+3'
    pub fn get_constraining_expr(
        &self,
        where_clause: &[WhereTerm],
    ) -> (ast::Operator, PlanExpr, Affinity) {
        // For multi-index branches, use the pre-computed constraining expression
        if let Some(constraining) = &self.constraining_expr {
            return constraining.clone();
        }

        let (idx, side) = self.where_clause_pos;
        let where_term = &where_clause[idx];
        let Some((lhs, _, rhs)) = binary_components_unchecked(&where_term.expr) else {
            panic!("Expected a valid binary expression");
        };
        let mut affinity = self.comparison_affinity.unwrap_or(Affinity::Blob);

        if side == BinaryExprSide::Lhs {
            if plan_expr_needs_no_affinity_change(affinity, lhs) {
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
            if plan_expr_needs_no_affinity_change(affinity, rhs) {
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

    pub fn get_constraining_expr_ref<'a>(&self, where_clause: &'a [WhereTerm]) -> &'a PlanExpr {
        let (idx, side) = self.where_clause_pos;
        let where_term = &where_clause[idx];
        let Some((lhs, _, rhs)) = binary_components_unchecked(&where_term.expr) else {
            panic!("Expected a valid binary expression");
        };
        if side == BinaryExprSide::Lhs {
            lhs
        } else {
            rhs
        }
    }

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
pub struct ConstraintUseCandidate {
    /// The index that may be used to satisfy the constraints. If none, the table's rowid index is used.
    pub index: Option<PlanIndex>,
    /// References to the constraints that may be used as an access path for the index.
    /// Refs are sorted by [ConstraintRef::index_col_pos]
    pub refs: Vec<ConstraintRef>,
}

#[derive(Debug)]
/// A collection of [Constraint]s and their potential [ConstraintUseCandidate]s for a given table.
pub struct TableConstraints {
    /// The internal ID of the [TableReference] that these constraints are for.
    pub table_id: PlanSourceId,
    /// The constraints for the table, i.e. any [WhereTerm]s that reference columns from this table.
    pub constraints: Vec<Constraint>,
    /// Candidates for indexes that may use the constraints to perform a lookup.
    pub candidates: Vec<ConstraintUseCandidate>,
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
    index: Option<&Index>,
    params: &CostModelParams,
    is_rowid: bool,
) -> f64 {
    estimate_selectivity(
        schema,
        table_reference.table.get_name(),
        column,
        index,
        operator,
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
    expr: &PlanExpr,
    table_reference: &JoinedTable,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
) -> Result<bool> {
    let mask = table_mask_from_plan_expr(expr, table_references, subqueries)?;
    Ok(table_references
        .joined_tables()
        .iter()
        .position(|t| t.internal_id == table_reference.internal_id)
        .is_some_and(|idx| mask.get(idx) && mask.count() == 1))
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
    let facts = ConstraintFacts { subqueries };

    // For each table, collect all the Constraints and all potential index candidates that may use them.
    for table_reference in table_references.joined_tables() {
        let rowid_alias_column = table_reference
            .columns()
            .iter()
            .position(|c| c.is_rowid_alias());

        let mut cs = TableConstraints {
            table_id: table_reference.internal_id,
            constraints: Vec::new(),
            candidates: available_indexes
                .indexes_for_table(table_reference.internal_id)
                .map_or(Vec::new(), |indexes| {
                    indexes
                        .iter()
                        // Skip IndexMethod-based indexes (FTS, vector, etc.) - they use
                        // pattern matching rather than btree index scans
                        .filter(|index| index.index_method.is_none())
                        .map(|index| ConstraintUseCandidate {
                            index: Some(index.clone()),
                            refs: Vec::new(),
                        })
                        .collect()
                }),
        };
        // Add a candidate for the rowid index, which is always available when the table has a rowid alias.
        cs.candidates.push(ConstraintUseCandidate {
            index: None,
            refs: Vec::new(),
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
            if let Some((lhs, operator, rhs)) = binary_components(&term.expr, &facts)? {
                // Resolve the comparison affinity once per term per SQLite's
                // `comparisonAffinity` (see `Constraint::comparison_affinity`)
                // and propagate it to every constraint derived from this term.
                let cmp_aff = operator
                    .as_ast_operator()
                    .filter(|op| op.is_comparison())
                    .map(|_| resolve_plan_comparison_affinity(lhs, rhs, &facts));
                // If either the LHS or RHS of the constraint is a column from the table, add the constraint.
                match lhs {
                    PlanExpr::Column(column) => {
                        if column.source == table_reference.internal_id {
                            let table_column = &table_reference.table.columns()[column.column];
                            cs.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Rhs),
                                operator,
                                table_col_pos: Some(column.column),
                                expr: None,
                                constraining_expr: None,
                                lhs_mask: table_mask_from_plan_expr(
                                    rhs,
                                    table_references,
                                    subqueries,
                                )?,
                                selectivity: estimate_constraint_selectivity(
                                    schema,
                                    table_reference,
                                    Some(table_column),
                                    operator,
                                    index_for_column(column.column),
                                    params,
                                    false,
                                ),
                                usable: true,
                                is_rowid: false,
                                comparison_affinity: cmp_aff,
                            });
                        }
                    }
                    PlanExpr::RowId(source) => {
                        if *source == table_reference.internal_id {
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
                                lhs_mask: table_mask_from_plan_expr(
                                    rhs,
                                    table_references,
                                    subqueries,
                                )?,
                                selectivity: estimate_constraint_selectivity(
                                    schema,
                                    table_reference,
                                    col,
                                    operator,
                                    None,
                                    params,
                                    true,
                                ),
                                usable: true,
                                is_rowid: true,
                                comparison_affinity: cmp_aff,
                            });
                        }
                    }
                    _ if expression_matches_table(
                        lhs,
                        table_reference,
                        table_references,
                        subqueries,
                    )? =>
                    {
                        let selectivity = estimate_constraint_selectivity(
                            schema,
                            table_reference,
                            None,
                            operator,
                            None,
                            params,
                            false,
                        );
                        tracing::debug!(
                            table = table_reference.table.get_name(),
                            where_clause_pos = i,
                            operator = ?operator,
                            lhs_mask = ?table_mask_from_plan_expr(rhs, table_references, subqueries)?,
                            selectivity,
                            "expr constraint (lhs matches table)"
                        );
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Rhs),
                            operator,
                            table_col_pos: None,
                            expr: Some(lhs.clone()),
                            constraining_expr: None,
                            lhs_mask: table_mask_from_plan_expr(rhs, table_references, subqueries)?,
                            selectivity,
                            usable: true,
                            is_rowid: false,
                            comparison_affinity: cmp_aff,
                        });
                    }
                    _ => {}
                };
                match rhs {
                    PlanExpr::Column(column) => {
                        if column.source == table_reference.internal_id {
                            let table_column = &table_reference.table.columns()[column.column];
                            cs.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Lhs),
                                operator: opposite_cmp_op(operator),
                                table_col_pos: Some(column.column),
                                expr: None,
                                constraining_expr: None,
                                lhs_mask: table_mask_from_plan_expr(
                                    lhs,
                                    table_references,
                                    subqueries,
                                )?,
                                selectivity: estimate_constraint_selectivity(
                                    schema,
                                    table_reference,
                                    Some(table_column),
                                    operator,
                                    index_for_column(column.column),
                                    params,
                                    false,
                                ),
                                usable: true,
                                is_rowid: false,
                                comparison_affinity: cmp_aff,
                            });
                        }
                    }
                    PlanExpr::RowId(source) => {
                        if *source == table_reference.internal_id {
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
                                lhs_mask: table_mask_from_plan_expr(
                                    lhs,
                                    table_references,
                                    subqueries,
                                )?,
                                selectivity: estimate_constraint_selectivity(
                                    schema,
                                    table_reference,
                                    col,
                                    operator,
                                    None,
                                    params,
                                    true,
                                ),
                                usable: true,
                                is_rowid: true,
                                comparison_affinity: cmp_aff,
                            });
                        }
                    }
                    _ if expression_matches_table(
                        rhs,
                        table_reference,
                        table_references,
                        subqueries,
                    )? =>
                    {
                        let selectivity = estimate_constraint_selectivity(
                            schema,
                            table_reference,
                            None,
                            operator,
                            None,
                            params,
                            false,
                        );
                        tracing::debug!(
                            table = table_reference.table.get_name(),
                            where_clause_pos = i,
                            operator = ?operator,
                            lhs_mask = ?table_mask_from_plan_expr(lhs, table_references, subqueries)?,
                            selectivity,
                            "expr constraint (rhs matches table)"
                        );
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Lhs),
                            operator: opposite_cmp_op(operator),
                            table_col_pos: None,
                            expr: Some(rhs.clone()),
                            constraining_expr: None,
                            lhs_mask: table_mask_from_plan_expr(lhs, table_references, subqueries)?,
                            selectivity,
                            usable: true,
                            is_rowid: false,
                            comparison_affinity: cmp_aff,
                        });
                    }
                    _ => {}
                };
            }

            // IN expressions are handled separately from binary expressions above because:
            // - as_binary_components returns (&Expr, ConstraintOperator, &Expr) - a single RHS
            // - InList has Vec<Expr> as RHS, while a subquery has a different structure
            // - They don't fit the binary expression abstraction without a more complex return type

            // Handle IN list: col IN (val1, val2, ...)
            if let PlanExpr::InList {
                lhs,
                negated,
                values,
            } = &term.expr
            {
                let estimated_values = values.len() as f64;
                let mut rhs_mask = TableMask::default();
                for rhs_expr in values {
                    rhs_mask.union_with(&table_mask_from_plan_expr(
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
                let selectivity = estimate_in_selectivity(estimated_values, row_count, *negated);
                // SQLite's `comparisonAffinity` for IN-list (`x IN (lit, ...)`)
                // is the LHS column's affinity; the RHS literals are not folded.
                let cmp_aff = Some(plan_expr_affinity(lhs, &facts).affinity);

                match lhs.as_ref() {
                    PlanExpr::Column(column) if column.source == table_reference.internal_id => {
                        let is_rowid = rowid_alias_column == Some(column.column);
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Rhs),
                            operator: ConstraintOperator::In {
                                not: *negated,
                                estimated_values,
                            },
                            table_col_pos: Some(column.column),
                            expr: None,
                            constraining_expr: None,
                            lhs_mask: rhs_mask,
                            selectivity,
                            usable: false, // IN uses a separate seek path, not the range-seek model
                            is_rowid,
                            comparison_affinity: cmp_aff,
                        });
                    }
                    PlanExpr::RowId(source) if *source == table_reference.internal_id => {
                        cs.constraints.push(Constraint {
                            where_clause_pos: (i, BinaryExprSide::Rhs),
                            operator: ConstraintOperator::In {
                                not: *negated,
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
                        });
                    }
                    _ => {}
                }
            }

            // Handle IN subquery: col IN (SELECT ...)
            if let PlanExpr::Subquery(PlanSubqueryExpr::In {
                lhs: lhs_expr,
                query,
                negated,
            }) = &term.expr
            {
                // Find the subquery to check if it's correlated
                let subquery = subqueries
                    .iter()
                    .find(|s| s.internal_id == *query)
                    .expect("subquery not found");
                // Only use as constraint if NOT correlated
                if !subquery.correlated {
                    let estimated_values = params.in_subquery_rows;
                    let table_stats = schema
                        .analyze_stats
                        .table_stats(table_reference.table.get_name());
                    let row_count = table_stats
                        .and_then(|s| s.row_count)
                        .unwrap_or(params.rows_per_table_fallback as u64)
                        as f64;
                    let selectivity =
                        estimate_in_selectivity(estimated_values, row_count, *negated);
                    // SQLite's `comparisonAffinity` for IN-subquery combines the
                    // LHS column affinity with each result column via
                    // `sqlite3CompareAffinity` — that result is already cached on
                    // the planned IN-subquery metadata. For a single-LHS-column
                    // IN it is the first character; row-value IN has no single
                    // resolved affinity and is left as `None`.
                    let is_row_value =
                        matches!(lhs_expr.as_ref(), PlanExpr::Row(exprs) if exprs.len() != 1);
                    let affinity_str = match &subquery.query_type {
                        crate::translate::plan::PlanSubqueryType::In { affinity_str, .. } => {
                            Some(affinity_str)
                        }
                        _ => None,
                    };
                    let cmp_aff = (!is_row_value)
                        .then(|| affinity_str?.chars().next().map(Affinity::from_char))
                        .flatten();

                    match lhs_expr.as_ref() {
                        PlanExpr::Column(column)
                            if column.source == table_reference.internal_id =>
                        {
                            let is_rowid = rowid_alias_column == Some(column.column);
                            cs.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Rhs),
                                operator: ConstraintOperator::In {
                                    not: *negated,
                                    estimated_values,
                                },
                                table_col_pos: Some(column.column),
                                expr: None,
                                constraining_expr: None,
                                lhs_mask: TableMask::default(), // non-correlated = no dependencies
                                selectivity,
                                usable: false, // IN uses a separate seek path (consider_in_list_seek)
                                is_rowid,
                                comparison_affinity: cmp_aff,
                            });
                        }
                        PlanExpr::RowId(source) if *source == table_reference.internal_id => {
                            cs.constraints.push(Constraint {
                                where_clause_pos: (i, BinaryExprSide::Rhs),
                                operator: ConstraintOperator::In {
                                    not: *negated,
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
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
        // sort equalities first so that index keys will be properly constructed.
        // see e.g.: https://www.solarwinds.com/blog/the-left-prefix-index-rule
        cs.constraints.sort_by(|a, b| {
            if a.operator == ast::Operator::Equals.into() {
                Ordering::Less
            } else if b.operator == ast::Operator::Equals.into() {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        });

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
                plan_expr_collation(constraining_expr, &facts)?,
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
                    None => constraint
                        .expr
                        .as_ref()
                        .and_then(|expr| table_reference.expression_index_pos(index, expr)),
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
                        let (lhs, _, rhs) = binary_components(
                            &where_clause[constraint.where_clause_pos.0].expr,
                            &facts,
                        )?
                        .expect("indexable constraint must retain its comparison expression");
                        let constrained_expr = match constraint.where_clause_pos.1 {
                            BinaryExprSide::Lhs => rhs,
                            BinaryExprSide::Rhs => lhs,
                        };
                        let has_custom_type = plan_expr_type_fact(constrained_expr, &facts)
                            .declared
                            .as_ref()
                            .is_some_and(|declared| declared.custom().is_some());
                        if has_custom_type && constraint.operator != ast::Operator::Equals.into() {
                            continue;
                        }
                        let idx_col_aff = constrained_column
                            .affinity_with_strict(table_reference.table.is_strict());
                        if !constraint.satisfies_index_affinity(idx_col_aff) {
                            continue;
                        }
                    }
                    if let Some(index_candidate) = cs.candidates.iter_mut().find_map(|candidate| {
                        if candidate.index.as_ref().is_some_and(|i| {
                            std::ptr::eq(index.value(), i.value())
                                && (index.where_clause.is_none()
                                    || can_use_partial_index(index, table_reference, where_clause))
                        }) {
                            Some(candidate)
                        } else {
                            None
                        }
                    }) {
                        index_candidate.refs.push(ConstraintRef {
                            constraint_vec_pos: i,
                            index_col_pos: position_in_index,
                            sort_order: index.columns[position_in_index].order,
                        });
                    }
                }
            }
        }

        for candidate in cs.candidates.iter_mut() {
            // Sort by index_col_pos, ascending -- index columns must be consumed in contiguous order.
            candidate.refs.sort_by_key(|cref| cref.index_col_pos);
        }
        cs.candidates.retain(|c| {
            if let Some(idx) = &c.index {
                if idx.where_clause.is_some()
                    && c.refs.is_empty()
                    && !can_use_partial_index(idx, table_reference, where_clause)
                {
                    // A partial index with no column constraints can still drive a
                    // scan, but only if every conjunct of its WHERE clause is implied
                    // by the query's WHERE. Otherwise it would skip rows the query
                    // needs.
                    return false;
                }
            }
            true
        });
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
}

#[derive(Debug, Clone)]
pub struct RangeConstraintRef {
    /// position of the column in the table definition
    pub table_col_pos: Option<usize>,
    /// position of the column in the index definition
    pub index_col_pos: usize,
    /// sort order for the column in the index definition
    pub sort_order: SortOrder,
    /// equality constraint
    pub eq: Option<EqConstraintRef>,
    /// lower bound constraint (either > or >=)
    pub lower_bound: Option<usize>,
    /// upper bound constraint (either < or <=)
    pub upper_bound: Option<usize>,
}

#[derive(Debug, Clone)]
/// Represent seek range which can be used in query planning to emit range scan over table or index
pub struct SeekRangeConstraint {
    pub sort_order: SortOrder,
    pub eq: Option<(ast::Operator, PlanExpr, Affinity)>,
    pub lower_bound: Option<(ast::Operator, PlanExpr, Affinity)>,
    pub upper_bound: Option<(ast::Operator, PlanExpr, Affinity)>,
}

impl SeekRangeConstraint {
    pub fn new_eq(sort_order: SortOrder, eq: (ast::Operator, PlanExpr, Affinity)) -> Self {
        Self {
            sort_order,
            eq: Some(eq),
            lower_bound: None,
            upper_bound: None,
        }
    }
    pub fn new_range(
        sort_order: SortOrder,
        lower_bound: Option<(ast::Operator, PlanExpr, Affinity)>,
        upper_bound: Option<(ast::Operator, PlanExpr, Affinity)>,
    ) -> Self {
        turso_assert!(lower_bound.is_some() || upper_bound.is_some());
        Self {
            sort_order,
            eq: None,
            lower_bound,
            upper_bound,
        }
    }
}

impl RangeConstraintRef {
    /// Convert the [RangeConstraintRef] to a [SeekRangeConstraint] usable in a [crate::translate::plan::SeekDef::key].
    pub fn as_seek_range_constraint(
        &self,
        constraints: &[Constraint],
        where_clause: &[WhereTerm],
    ) -> SeekRangeConstraint {
        if let Some(ref eq) = self.eq {
            return SeekRangeConstraint::new_eq(
                self.sort_order,
                constraints[eq.constraint_pos].get_constraining_expr(where_clause),
            );
        }
        SeekRangeConstraint::new_range(
            self.sort_order,
            self.lower_bound
                .map(|x| constraints[x].get_constraining_expr(where_clause)),
            self.upper_bound
                .map(|x| constraints[x].get_constraining_expr(where_clause)),
        )
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
pub fn usable_constraints_for_lhs_mask(
    constraints: &[Constraint],
    refs: &[ConstraintRef],
    lhs_mask: &TableMask,
    table_idx: usize,
) -> Vec<RangeConstraintRef> {
    turso_debug_assert!(refs.is_sorted_by_key(|x| x.index_col_pos));

    let mut usable: Vec<RangeConstraintRef> = Vec::new();
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
        if operator == ast::Operator::Equals.into()
            && usable
                .last()
                .is_some_and(|x| x.table_col_pos == table_col_pos)
        {
            // Duplicate equalities on the same column do not expand the usable
            // prefix or change the seek shape.
            continue;
        }
        let constraint_group = match operator.as_ast_operator() {
            Some(ast::Operator::Equals) => RangeConstraintRef {
                table_col_pos,
                index_col_pos: cref.index_col_pos,
                sort_order: cref.sort_order,
                eq: Some(EqConstraintRef {
                    constraint_pos: cref.constraint_vec_pos,
                    is_const: constraints[cref.constraint_vec_pos].lhs_mask.is_empty(),
                }),
                lower_bound: None,
                upper_bound: None,
            },
            Some(ast::Operator::Greater) | Some(ast::Operator::GreaterEquals) => {
                RangeConstraintRef {
                    table_col_pos,
                    index_col_pos: cref.index_col_pos,
                    sort_order: cref.sort_order,
                    eq: None,
                    lower_bound: Some(cref.constraint_vec_pos),
                    upper_bound: None,
                }
            }
            Some(ast::Operator::Less) | Some(ast::Operator::LessEquals) => RangeConstraintRef {
                table_col_pos,
                index_col_pos: cref.index_col_pos,
                sort_order: cref.sort_order,
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

pub fn usable_constraints_for_join_order<'a>(
    constraints: &'a [Constraint],
    refs: &'a [ConstraintRef],
    join_order: &[JoinOrderMember],
) -> Result<Vec<RangeConstraintRef>> {
    turso_debug_assert!(refs.is_sorted_by_key(|x| x.index_col_pos));

    let table_idx = join_order.last().unwrap().original_idx;
    let lhs_mask = join_order
        .iter()
        .take(join_order.len() - 1)
        .map(|j| j.original_idx)
        .try_collect()?;
    Ok(usable_constraints_for_lhs_mask(
        constraints,
        refs,
        &lhs_mask,
        table_idx,
    ))
}

/// Order synthetic key columns for a materialized subquery seek index.
///
/// Unlike ordinary index analysis, the ephemeral index does not have a fixed
/// on-disk column order, so we can choose one that matches the intended probe
/// shape. Equalities come first, followed by columns that are constrained only
/// by ranges. Columns that have both equality and range predicates stay in the
/// equality prefix; the range side is redundant for key ordering.
pub fn ordered_materialized_key_columns(constraints: &[&Constraint]) -> Vec<usize> {
    let mut equality_cols = Vec::new();
    let mut range_only_cols = Vec::new();

    for constraint in constraints {
        let Some(col_pos) = constraint.table_col_pos else {
            continue;
        };
        match constraint.operator.as_ast_operator() {
            Some(ast::Operator::Equals) => equality_cols.push(col_pos),
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

fn break_plan_predicate_at_and_boundaries(expr: &PlanExpr, out: &mut Vec<PlanExpr>) {
    match expr {
        PlanExpr::Binary {
            lhs,
            operator: ast::Operator::And,
            rhs,
            ..
        } => {
            break_plan_predicate_at_and_boundaries(lhs, out);
            break_plan_predicate_at_and_boundaries(rhs, out);
        }
        _ => out.push(expr.clone()),
    }
}

pub(super) fn partial_index_predicate_terms(
    index: &Index,
    table_reference: &JoinedTable,
    query_where_clause: &[WhereTerm],
) -> Option<SmallVec<[usize; 4]>> {
    assert!(
        index.where_clause.is_some(),
        "partial_index_predicate_terms requires a partial index"
    );
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
    // Semantic analysis and planning instantiated the schema predicate for this
    // exact source. Each conjunct must match some query WHERE term for the
    // partial index to be safe.
    let predicate = table_reference.partial_index_predicate(index)?;
    let mut index_conjuncts: Vec<PlanExpr> = Vec::new();
    break_plan_predicate_at_and_boundaries(predicate, &mut index_conjuncts);
    let mut matched_terms = SmallVec::<[usize; 4]>::new();
    for index_conjunct in index_conjuncts.iter() {
        let (term_idx, _) = query_where_clause.iter().enumerate().find(|(_, term)| {
            can_use_query_term(term) && plan_exprs_are_equivalent(index_conjunct, &term.expr)
        })?;
        if !matched_terms.contains(&term_idx) {
            matched_terms.push(term_idx);
        }
    }
    Some(matched_terms)
    // TODO: recognize implication beyond syntactic equivalence (e.g. `x = 5` implies
    // `x IS NOT NULL`, `x > 10` implies `x > 5`).
}

pub(super) fn partial_index(index: Option<&PlanIndex>) -> Option<&Index> {
    let index = index?;
    index.where_clause.as_ref()?;
    Some(index.value())
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

/// Estimate the selectivity of a partial index's WHERE clause, i.e. what
/// fraction of the table's rows pass the predicate (and therefore appear in
/// the partial index). Semantic planning already instantiated the expression
/// for this source, so leaf comparisons can use the standard ANALYZE-aware path.
pub fn estimate_partial_index_where_selectivity(
    index: &Index,
    table_reference: &JoinedTable,
    schema: &Schema,
    available_indexes: &AvailableIndexes,
    params: &CostModelParams,
) -> f64 {
    let predicate = table_reference
        .partial_index_predicate(index)
        .expect("partial index predicate must be planned before optimization");
    estimate_plan_expr_selectivity(
        predicate,
        table_reference,
        schema,
        available_indexes,
        params,
    )
}

fn estimate_plan_expr_selectivity(
    expr: &PlanExpr,
    table_reference: &JoinedTable,
    schema: &Schema,
    available_indexes: &AvailableIndexes,
    params: &CostModelParams,
) -> f64 {
    // Try to resolve the constrained column from a binary leaf so we can use
    // ANALYZE-aware selectivity. Returns (Column, column_pos, is_rowid).
    let resolve_side = |side: &PlanExpr| -> Option<(Option<&Column>, Option<usize>, bool)> {
        match side {
            PlanExpr::Column(column) if column.source == table_reference.internal_id => Some((
                Some(&table_reference.table.columns()[column.column]),
                Some(column.column),
                false,
            )),
            PlanExpr::RowId(source) if *source == table_reference.internal_id => {
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
    let leaf_selectivity = |lhs: &PlanExpr, rhs: &PlanExpr, op: ConstraintOperator| -> f64 {
        let resolved = resolve_side(lhs).or_else(|| resolve_side(rhs));
        let (col, col_pos, is_rowid) = resolved.unwrap_or((None, None, false));
        let index = col_pos.and_then(|pos| {
            selectivity_index_for_column(schema, table_reference, available_indexes, pos)
        });
        estimate_constraint_selectivity(schema, table_reference, col, op, index, params, is_rowid)
    };

    match expr {
        PlanExpr::Binary {
            lhs,
            operator: ast::Operator::And,
            rhs,
            ..
        } => {
            let l = estimate_plan_expr_selectivity(
                lhs,
                table_reference,
                schema,
                available_indexes,
                params,
            );
            let r = estimate_plan_expr_selectivity(
                rhs,
                table_reference,
                schema,
                available_indexes,
                params,
            );
            l * r
        }
        PlanExpr::Binary {
            lhs,
            operator: ast::Operator::Or,
            rhs,
            ..
        } => {
            let l = estimate_plan_expr_selectivity(
                lhs,
                table_reference,
                schema,
                available_indexes,
                params,
            );
            let r = estimate_plan_expr_selectivity(
                rhs,
                table_reference,
                schema,
                available_indexes,
                params,
            );
            (l + r - l * r).min(1.0)
        }
        PlanExpr::Binary {
            operator: ast::Operator::Is,
            rhs,
            ..
        } if matches!(rhs.as_ref(), PlanExpr::Literal(ast::Literal::Null)) => params.sel_is_null,
        PlanExpr::Binary {
            operator: ast::Operator::IsNot,
            rhs,
            ..
        } if matches!(rhs.as_ref(), PlanExpr::Literal(ast::Literal::Null)) => {
            params.sel_is_not_null
        }
        PlanExpr::Binary {
            lhs, operator, rhs, ..
        } => leaf_selectivity(lhs, rhs, (*operator).into()),
        PlanExpr::IsNull(_) => params.sel_is_null,
        PlanExpr::NotNull(_) => params.sel_is_not_null,
        PlanExpr::Between { negated, .. } => {
            if *negated {
                1.0 - params.sel_range
            } else {
                params.sel_range
            }
        }
        PlanExpr::InList {
            lhs,
            negated,
            values,
        } => {
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
                    not: *negated,
                    estimated_values: values.len() as f64,
                },
                index,
                params,
                is_rowid,
            )
        }
        PlanExpr::Like { negated, .. } => {
            if *negated {
                params.sel_not_like
            } else {
                params.sel_like
            }
        }
        PlanExpr::Unary {
            operator: ast::UnaryOperator::Not,
            expr: inner,
        } => {
            1.0 - estimate_plan_expr_selectivity(
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

pub fn convert_to_vtab_constraint(
    constraints: &[Constraint],
    join_order: &[JoinOrderMember],
) -> Result<Vec<ConstraintInfo>> {
    let table_idx = join_order.last().unwrap().original_idx;
    let lhs_mask: TableMask = join_order
        .iter()
        .take(join_order.len() - 1)
        .map(|j| j.original_idx)
        .try_collect()?;
    let constraints = constraints
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
        .collect();
    Ok(constraints)
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
    pub best_index: Option<PlanIndex>,
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
    pub best_index: Option<PlanIndex>,
}

struct BinaryTermIndexInfo<'a> {
    lhs: &'a PlanExpr,
    rhs: &'a PlanExpr,
    operator: ConstraintOperator,
    table_col_pos: Option<usize>,
    constraining_expr: &'a PlanExpr,
    side: BinaryExprSide,
    is_rowid: bool,
}

fn analyze_binary_term_index_info<'a>(
    expr: &'a PlanExpr,
    table_id: PlanSourceId,
    rowid_alias_column: Option<usize>,
    facts: &impl PlanExprFactSource,
) -> Result<Option<BinaryTermIndexInfo<'a>>> {
    let Some((lhs, operator, rhs)) = binary_components(expr, facts)? else {
        return Ok(None);
    };

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
        return Ok(None);
    }

    // Check if this is an indexable constraint on our table
    let (table_col_pos, constraining_expr, side, is_rowid) = match lhs {
        PlanExpr::Column(column) if column.source == table_id => {
            (Some(column.column), rhs, BinaryExprSide::Rhs, false)
        }
        PlanExpr::RowId(source) if *source == table_id => {
            (rowid_alias_column, rhs, BinaryExprSide::Rhs, true)
        }
        _ => match rhs {
            PlanExpr::Column(column) if column.source == table_id => {
                (Some(column.column), lhs, BinaryExprSide::Lhs, false)
            }
            PlanExpr::RowId(source) if *source == table_id => {
                (rowid_alias_column, lhs, BinaryExprSide::Lhs, true)
            }
            _ => return Ok(None), // Doesn't reference our table
        },
    };

    // Normalize operator direction so it matches the constrained table column.
    // Example: `1 > t.b` constrains `t.b < 1`.
    let operator = if side == BinaryExprSide::Lhs {
        opposite_cmp_op(operator)
    } else {
        operator
    };

    Ok(Some(BinaryTermIndexInfo {
        lhs,
        rhs,
        operator,
        table_col_pos,
        constraining_expr,
        side,
        is_rowid,
    }))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn summarize_binary_term_for_index(
    expr: &PlanExpr,
    table_id: PlanSourceId,
    table_reference: &JoinedTable,
    query_where_clause: &[WhereTerm],
    indexes: Option<&VecDeque<PlanIndex>>,
    rowid_alias_column: Option<usize>,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
) -> Result<Option<IndexableTermSummary>> {
    let BinaryTermIndexInfo {
        operator,
        table_col_pos,
        constraining_expr,
        is_rowid,
        ..
    } = match analyze_binary_term_index_info(
        expr,
        table_id,
        rowid_alias_column,
        &ConstraintFacts { subqueries },
    )? {
        Some(info) => info,
        None => return Ok(None),
    };

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
        return Ok(None);
    }

    let lhs_mask = table_mask_from_plan_expr(constraining_expr, table_references, subqueries)?;

    let table_pos = table_references
        .joined_tables()
        .iter()
        .position(|t| t.internal_id == table_id)
        .expect("target table must exist in table_references");
    if lhs_mask.get(table_pos) {
        return Ok(None);
    }

    Ok(Some(IndexableTermSummary {
        table_col_pos,
        lhs_mask,
        best_index,
    }))
}

/// Analyzes a single binary expression to determine if it can use an index.
///
/// This is a shared helper for both OR and AND multi-index analysis.
/// Returns `Some(AnalyzedTerm)` if the expression is a usable indexed constraint,
/// `None` otherwise.
#[allow(clippy::too_many_arguments)]
pub(crate) fn analyze_binary_term_for_index(
    expr: &PlanExpr,
    where_term_idx: usize,
    table_id: PlanSourceId,
    table_reference: &JoinedTable,
    query_where_clause: &[WhereTerm],
    indexes: Option<&VecDeque<PlanIndex>>,
    rowid_alias_column: Option<usize>,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    params: &CostModelParams,
) -> Result<Option<AnalyzedTerm>> {
    let BinaryTermIndexInfo {
        lhs,
        rhs,
        operator,
        table_col_pos,
        constraining_expr,
        side,
        is_rowid,
    } = match analyze_binary_term_index_info(
        expr,
        table_id,
        rowid_alias_column,
        &ConstraintFacts { subqueries },
    )? {
        Some(info) => info,
        None => return Ok(None),
    };

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
        return Ok(None);
    }

    let table_column = table_col_pos.and_then(|pos| table_reference.table.columns().get(pos));
    let selectivity = estimate_constraint_selectivity(
        schema,
        table_reference,
        table_column,
        operator,
        best_index.as_deref(),
        params,
        is_rowid,
    );

    let lhs_mask = table_mask_from_plan_expr(constraining_expr, table_references, subqueries)?;

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
            return Ok(None);
        }
    }

    // Compute the affinity for the constraining expression
    let affinity = if let Some(ast_op) = operator.as_ast_operator() {
        if ast_op.is_comparison() && table_col_pos.is_some() {
            resolve_plan_comparison_affinity(lhs, rhs, &ConstraintFacts { subqueries })
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
    };

    Ok(Some(AnalyzedTerm {
        constraint,
        best_index,
        constraint_refs,
    }))
}

/// Find the best index for a single constraint.
fn find_best_index_for_constraint(
    table_col_pos: Option<usize>,
    operator: ConstraintOperator,
    indexes: Option<&VecDeque<PlanIndex>>,
    rowid_alias_column: Option<usize>,
    is_rowid: bool,
    table_reference: &JoinedTable,
    query_where_clause: &[WhereTerm],
) -> (Option<PlanIndex>, Vec<RangeConstraintRef>) {
    // Handle implicit rowid (no alias column, table_col_pos is None)
    if is_rowid && table_col_pos.is_none() {
        let constraint_ref = RangeConstraintRef {
            table_col_pos: None,
            index_col_pos: 0,
            sort_order: SortOrder::Asc,
            eq: if operator.as_ast_operator() == Some(ast::Operator::Equals) {
                Some(EqConstraintRef {
                    constraint_pos: 0,
                    is_const: false,
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
            eq: if operator.as_ast_operator() == Some(ast::Operator::Equals) {
                Some(EqConstraintRef {
                    constraint_pos: 0,
                    is_const: false,
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
                        eq: if operator.as_ast_operator() == Some(ast::Operator::Equals) {
                            Some(EqConstraintRef {
                                constraint_pos: 0,
                                is_const: false,
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
