//! Multi-index-specific planning for OR-by-union and AND-by-intersection.
//!
//! This module owns the parts of planning that are unique to combining several
//! index probes for the same table. It reuses the generic btree candidate
//! chooser from `access_method.rs` for each individual branch, then layers the
//! union/intersection-specific decomposition, costing, and residual handling on
//! top.

use crate::alloc::{TryClone, TursoIteratorExt};
use crate::schema::{Index, Schema};
use crate::stats::AnalyzeStats;
use crate::translate::expr::expr_references_any_subquery;
use crate::translate::optimizer::access_method::{
    choose_best_btree_candidate, choose_best_in_seek_candidate, AccessMethod, AccessMethodParams,
    BranchReadMode, ChosenInSeekCandidate,
};
use crate::translate::optimizer::constraints::{
    analyze_binary_term_for_index, can_use_partial_index, constraints_for_table,
    partial_index_predicate_terms, summarize_binary_term_for_index, Constraint, RangeConstraintRef,
    TableConstraints,
};
use crate::translate::optimizer::cost::{
    estimate_cost_for_scan_or_seek, estimate_rows_per_seek, rows_per_leaf_page_for_index,
    where_expr_steps, AnalyzeCtx, Cost, IndexInfo, RowCountEstimate,
};
use crate::translate::optimizer::cost_params::CostModelParams;
use crate::translate::optimizer::AvailableIndexes;
use crate::translate::plan::{
    BitSet, InSeekSource, JoinedTable, NonFromClauseSubquery, SetOperation, TableReferences,
    UnionBranchPrePostFilters, WhereTerm,
};
use crate::translate::planner::{table_mask_from_expr, TableMask};
use crate::Result;
use smallvec::SmallVec;
use std::cell::OnceCell;
use std::sync::Arc;
use turso_macros::turso_assert_eq;
use turso_parser::ast::{self, TableInternalId};

#[derive(Debug, Clone)]
/// Parameters for a single branch of a multi-index scan.
pub struct MultiIndexBranchParams {
    /// The index to use for this branch, or None for rowid access.
    pub index: Option<Arc<Index>>,
    /// How this branch probes the table/index.
    pub access: MultiIndexBranchAccessParams,
    /// Estimated number of rows from this branch.
    pub estimated_rows: f64,
    /// Residual filters for union (OR) branches. `None` for intersection branches.
    pub residuals: Option<UnionBranchPrePostFilters>,
}

#[derive(Debug, Clone)]
pub enum MultiIndexBranchAccessParams {
    Seek {
        constraints: Vec<Constraint>,
        constraint_refs: Vec<RangeConstraintRef>,
    },
    InSeek {
        source: InSeekSource,
    },
}

/// Internal decomposition of an AND clause into intersection branches.
#[derive(Debug)]
struct AndClauseDecomposition {
    term_indices: Vec<usize>,
    branches: Vec<AndBranch>,
}

/// One decomposition slot per joined table, filled on first use.
///
/// [`analyze_and_terms_for_multi_index`] answers a single question: which of
/// the query's top-level `AND` terms could each drive their own index lookup on
/// one table. That answer depends on the table, the `WHERE` clause, the
/// available indexes and the schema — and on nothing that changes while the
/// join order search runs. The search, however, asks it again for every (join
/// order prefix, table) pair it tries, so a join-heavy query rebuilt the same
/// answer thousands of times, and the wasted work grew with the number of join
/// orders explored. Ask once per table instead.
///
/// The memo is only sound while the `WHERE` clause it was built against holds
/// still, so it lives in the join planner's context and dies with the search
/// that created it. The search borrows the clause as `&[WhereTerm]`, which is
/// what keeps that true.
#[derive(Debug)]
pub(crate) struct MultiIndexAndTermsMemo {
    per_table: Vec<OnceCell<Option<AndClauseDecomposition>>>,
}

impl MultiIndexAndTermsMemo {
    /// One slot per entry of [`TableReferences::joined_tables`].
    pub(crate) fn new(joined_table_count: usize) -> Self {
        Self {
            per_table: (0..joined_table_count).map(|_| OnceCell::new()).collect(),
        }
    }

    /// The decomposition for the table at `table_idx`, computing it if this is
    /// the first time it has been asked for.
    fn get_or_analyze(
        &self,
        table_idx: usize,
        analyze: impl FnOnce() -> Option<AndClauseDecomposition>,
    ) -> Option<&AndClauseDecomposition> {
        self.per_table[table_idx].get_or_init(analyze).as_ref()
    }
}

/// One slot per joined table, filled on first use.
///
/// Before the planner can cost an OR-by-union path it has to know what each
/// disjunct of an `OR` term constrains on the table being planned: build
/// branch-local `WhereTerm`s out of the disjunct's conjuncts and run the normal
/// constraint analysis over them. That answer depends on the table, the `WHERE`
/// clause, the available indexes and the schema — and on nothing that changes
/// while the join order search runs. The search asked it again for every (join
/// order prefix, table) pair it tried, so a join-heavy query rebuilt the same
/// answer thousands of times. Ask once per (table, term) instead.
///
/// The prepass also settles, once and for all, which `OR` terms are even
/// shaped like something that could drive a union scan of the table, so the
/// rest are never looked at again. In a join-heavy query the answer for nearly
/// every (table, term) pair is "this term constrains nothing here": an `OR`
/// term is about one table, and the rest of the query's tables were paying the
/// full analysis only to find they had nothing to seek by.
///
/// Both halves stay as lazy as the search was: the term list is built the first
/// time the table is planned, and a term's disjuncts are analyzed the first
/// time the search actually reaches that term. A term the search always decides
/// before is never analyzed at all.
///
/// Like [`MultiIndexAndTermsMemo`], the memo is only sound while the `WHERE`
/// clause it was built against holds still, so it lives in the join planner's
/// context and dies with the search that created it.
#[derive(Debug)]
pub(crate) struct MultiIndexOrTermsMemo {
    per_table: Vec<OnceCell<Vec<OrTermSlot>>>,
}

impl MultiIndexOrTermsMemo {
    /// One slot per entry of [`TableReferences::joined_tables`].
    pub(crate) fn new(joined_table_count: usize) -> Self {
        Self {
            per_table: (0..joined_table_count).map(|_| OnceCell::new()).collect(),
        }
    }

    /// The `OR` terms worth trying on the table at `table_idx`, finding them if
    /// this is the first time they have been asked for.
    fn get_or_find_terms(
        &self,
        table_idx: usize,
        find_terms: impl FnOnce() -> Vec<OrTermSlot>,
    ) -> &[OrTermSlot] {
        self.per_table[table_idx].get_or_init(find_terms)
    }
}

/// One `OR` term worth trying on one table, with room for the analysis of its
/// disjuncts.
#[derive(Debug)]
struct OrTermSlot {
    /// Where the term sits in the `WHERE` clause.
    where_term_idx: usize,
    /// The join whose `ON` clause the term came from, if any.
    from_outer_join: Option<TableInternalId>,
    /// One entry per disjunct once analyzed, or `None` once the disjuncts have
    /// been found unusable. Empty until the search first reaches this term.
    disjuncts: OnceCell<Option<Vec<BranchConstraints>>>,
}

/// One disjunct's conjuncts as planner terms, plus what they constrain on the
/// table being planned.
#[derive(Debug)]
struct BranchConstraints {
    terms: Vec<WhereTerm>,
    constraints: TableConstraints,
}

/// One term that can participate in an AND-by-intersection plan.
#[derive(Debug)]
struct AndBranch {
    where_term_idx: usize,
    constraint: Constraint,
    index: Option<Arc<Index>>,
    constraint_refs: Vec<RangeConstraintRef>,
}

struct AndBranchSummary {
    where_term_idx: usize,
    table_col_pos: Option<usize>,
    index: Option<Arc<Index>>,
}

/// Internal branch representation while evaluating a candidate multi-index plan.
struct MultiIdxBranch {
    index: Option<Arc<Index>>,
    access: MultiIdxBranchAccess,
    cost: Cost,
    estimated_rows: f64,
    union_prepost_filters: Option<UnionBranchPrePostFilters>,
}

enum MultiIdxBranchAccess {
    Seek {
        constraints: Vec<Constraint>,
        constraint_refs: Vec<RangeConstraintRef>,
    },
    InSeek {
        source: InSeekSource,
        constraint_idx: usize,
    },
}

/// Flattens nested OR expressions into a list of disjuncts.
///
/// For example, `(a OR b) OR c` becomes `[a, b, c]`.
fn flatten_or_expr(expr: &ast::Expr) -> Vec<&ast::Expr> {
    match expr {
        ast::Expr::Binary(lhs, ast::Operator::Or, rhs) => {
            let mut result = flatten_or_expr(lhs);
            result.extend(flatten_or_expr(rhs));
            result
        }
        _ => vec![expr],
    }
}

/// Flattens nested AND expressions into a list of conjuncts.
///
/// For example, `(a AND b) AND c` becomes `[a, b, c]`.
fn flatten_and_expr(expr: &ast::Expr) -> Vec<&ast::Expr> {
    match expr {
        ast::Expr::Binary(lhs, ast::Operator::And, rhs) => {
            let mut result = flatten_and_expr(lhs);
            result.extend(flatten_and_expr(rhs));
            result
        }
        _ => vec![expr],
    }
}

/// Build temporary `WhereTerm`s from branch-local expressions and extract the
/// constraints for exactly one target table.
///
/// This is narrower than `constraints_for_table()`:
/// - `exprs` are synthetic planner inputs, not the query's real top-level
///   `WHERE` terms.
/// - The returned `WhereTerm`s are only suitable for branch-local planning
///   and constraint bookkeeping for `table_reference`; they must not be reused
///   for global predicate consumption or join rewrites.
///
/// FIXME: stop synthesizing `WhereTerm`s here just to reuse
/// `constraints_for_table()`. Branch-local planning should have a direct
/// constraint-extraction path that does not fabricate top-level planner terms.
#[expect(clippy::too_many_arguments)]
fn get_table_local_constraints_for_branch(
    exprs: &[ast::Expr],
    from_outer_join: Option<TableInternalId>,
    table_reference: &JoinedTable,
    table_references: &TableReferences,
    available_indexes: &AvailableIndexes,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    params: &CostModelParams,
) -> crate::Result<(Vec<WhereTerm>, TableConstraints)> {
    let synthetic_where_terms = exprs
        .iter()
        .cloned()
        .map(|expr| WhereTerm {
            expr,
            from_outer_join,
            consumed: false,
        })
        .collect::<Vec<_>>();
    let mut table_constraints = constraints_for_table(
        table_reference,
        &synthetic_where_terms,
        table_references,
        available_indexes,
        subqueries,
        schema,
        params,
    )?;
    // Branch-local constraints originate from synthetic `WhereTerm`s, so copy
    // out their constraining expressions while those temporary terms still
    // exist.
    for constraint in table_constraints.constraints.iter_mut() {
        if constraint.constraining_expr.is_some() || constraint.operator.as_ast_operator().is_none()
        {
            continue;
        }
        constraint.constraining_expr = Some(constraint.get_constraining_expr(
            &synthetic_where_terms,
            Some(table_references),
            None,
        ));
    }
    Ok((synthetic_where_terms, table_constraints))
}

/// Estimate the cost of a multi-index union scan (OR-by-union optimization).
///
/// The cost model accounts for:
/// 1. Cost of each branch scan
/// 2. RowSet insert/test work needed to deduplicate rowids
/// 3. Table fetches after deduplication
/// 4. Overlap between branches, approximated from independent selectivities
fn estimate_multi_index_scan_cost(
    branch_costs: &[Cost],
    branch_rows: &[f64],
    base_row_count: RowCountEstimate,
    input_cardinality: f64,
    params: &CostModelParams,
) -> (Cost, f64) {
    let base_row_count = *base_row_count;
    // Total cost of all branch scans.
    let branch_scan_cost: f64 = branch_costs.iter().map(|c| c.0).sum();
    // Sum of branch row counts before RowSet deduplication.
    let total_rows_before_dedup: f64 = branch_rows.iter().sum();

    // Estimate overlap between branches. For independent predicates:
    //   P(A OR B) = 1 - (1 - P(A)) * (1 - P(B))
    let mut unique_row_ratio = 1.0f64;
    for rows in branch_rows.iter() {
        let branch_selectivity = (*rows / base_row_count).min(1.0);
        unique_row_ratio *= 1.0 - branch_selectivity;
    }
    let estimated_unique_rows = base_row_count * (1.0 - unique_row_ratio);

    // RowSet operations do an insert and membership test per candidate rowid.
    let rowset_ops_cost = total_rows_before_dedup * params.cpu_cost_per_row * 2.0;

    // Table fetch cost mirrors single-index lookup costing, assuming some
    // locality benefit from rowid-ordered access after RowSet deduplication.
    let table_pages = (base_row_count / params.rows_per_table_page).max(1.0);
    let selectivity = estimated_unique_rows / base_row_count.max(1.0);
    let table_fetch_cost = selectivity * table_pages;
    let total_cost = (branch_scan_cost + rowset_ops_cost + table_fetch_cost) * input_cardinality;

    (Cost(total_cost), estimated_unique_rows)
}

/// Estimate the cost of a multi-index intersection (AND-by-intersection).
///
/// The cost model accounts for:
/// 1. Cost of each branch scan
/// 2. RowSet test work while intersecting rowids
/// 3. Table fetches for the surviving rowids
/// 4. Final result size as the product of branch selectivities
fn estimate_multi_index_intersection_cost(
    branch_costs: &[Cost],
    branch_rows: &[f64],
    base_row_count: RowCountEstimate,
    input_cardinality: f64,
    params: &CostModelParams,
) -> (Cost, f64) {
    let base_row_count = *base_row_count;
    // Total cost of all branch scans.
    let branch_scan_cost: f64 = branch_costs.iter().map(|c| c.0).sum();

    // Estimate intersection result as the product of selectivities:
    //   P(A AND B) = P(A) * P(B)
    let mut intersection_selectivity = 1.0f64;
    for rows in branch_rows.iter() {
        let branch_selectivity = (*rows / base_row_count).min(1.0);
        intersection_selectivity *= branch_selectivity;
    }
    let estimated_intersection_rows = (base_row_count * intersection_selectivity).max(1.0);

    // First branch inserts rowids; later branches test against the RowSet.
    let first_branch_rows = branch_rows.first().copied().unwrap_or(0.0);
    let subsequent_branch_rows: f64 = branch_rows.iter().skip(1).sum();
    let rowset_ops_cost =
        (first_branch_rows + subsequent_branch_rows) * params.cpu_cost_per_row * 1.5;

    // Table fetch cost mirrors single-index lookup costing, assuming some
    // locality benefit from rowid-ordered access after intersection.
    let table_pages = (base_row_count / params.rows_per_table_page).max(1.0);
    let selectivity = estimated_intersection_rows / base_row_count.max(1.0);
    let table_fetch_cost = selectivity * table_pages;
    let total_cost = (branch_scan_cost + rowset_ops_cost + table_fetch_cost) * input_cardinality;

    (Cost(total_cost), estimated_intersection_rows)
}

/// Compute [`IndexInfo`] for a multi-index branch.
///
/// RowSet-building branches only need rowids from the scan, so an index can be
/// treated as covering even if it does not contain all later table columns.
fn index_info_for_branch(
    index: Option<&Index>,
    rhs_table: &JoinedTable,
    read_mode: BranchReadMode,
    rows_per_table_page: f64,
) -> Option<IndexInfo> {
    let rowid_only = matches!(read_mode, BranchReadMode::RowIdOnly);
    match index {
        Some(index) => Some(IndexInfo {
            unique: index.unique,
            covering: rowid_only || rhs_table.index_is_covering(index),
            column_count: index.columns.len(),
            rows_per_leaf_page: rows_per_leaf_page_for_index(
                index.columns.len(),
                rhs_table,
                rows_per_table_page,
            ),
        }),
        None => Some(IndexInfo {
            unique: true,
            covering: true,
            column_count: 1,
            rows_per_leaf_page: rows_per_table_page,
        }),
    }
}

fn in_seek_source_from_expr(
    expr: &ast::Expr,
    chosen: &ChosenInSeekCandidate,
) -> Option<InSeekSource> {
    match expr {
        ast::Expr::InList { rhs, .. } => Some(InSeekSource::LiteralList {
            values: rhs.iter().map(|e| *e.clone()).collect(),
            affinity: chosen.affinity,
        }),
        ast::Expr::SubqueryResult {
            query_type: ast::SubqueryType::In { cursor_id, .. },
            ..
        } => Some(InSeekSource::Subquery {
            cursor_id: *cursor_id,
        }),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn choose_multi_index_branch_access(
    rhs_table: &JoinedTable,
    table_constraints: &TableConstraints,
    branch_terms: &[WhereTerm],
    lhs_mask: &TableMask,
    rhs_idx: usize,
    schema: &Schema,
    available_indexes: &AvailableIndexes,
    base_row_count: RowCountEstimate,
    analyze_stats: &AnalyzeStats,
    params: &CostModelParams,
) -> Result<Option<MultiIdxBranch>> {
    let chosen_seek = choose_best_btree_candidate(
        rhs_table,
        table_constraints,
        lhs_mask,
        rhs_idx,
        None,
        schema,
        available_indexes,
        analyze_stats,
        1.0,
        base_row_count,
        params,
    )?;

    let mut best_branch = chosen_seek
        .as_ref()
        .filter(|chosen| !chosen.constraint_refs.is_empty())
        .map(|chosen| {
            let index_info = index_info_for_branch(
                chosen.index.as_deref(),
                rhs_table,
                BranchReadMode::RowIdOnly,
                params.rows_per_table_page,
            )
            .expect("multi-index branches always have costable access");
            let analyze_ctx = AnalyzeCtx {
                rhs_table,
                index: chosen.index.as_ref(),
                stats: analyze_stats,
            };
            let branch_cost = estimate_cost_for_scan_or_seek(
                Some(index_info),
                &table_constraints.constraints,
                &chosen.constraint_refs,
                1.0,
                base_row_count,
                false,
                params,
                Some(&analyze_ctx),
            );
            MultiIdxBranch {
                index: chosen.index.clone(),
                access: MultiIdxBranchAccess::Seek {
                    constraints: table_constraints.constraints.clone(),
                    constraint_refs: chosen.constraint_refs.to_vec(),
                },
                cost: branch_cost,
                estimated_rows: estimate_rows_per_seek(
                    index_info,
                    &table_constraints.constraints,
                    &chosen.constraint_refs,
                    base_row_count,
                    Some(&analyze_ctx),
                ),
                union_prepost_filters: None,
            }
        });

    let in_seek_threshold = best_branch
        .as_ref()
        .map(|branch| branch.cost)
        .unwrap_or(Cost(f64::INFINITY));
    if let Some(chosen_in_seek) = choose_best_in_seek_candidate(
        rhs_table,
        table_constraints,
        lhs_mask,
        1.0,
        base_row_count,
        params,
        in_seek_threshold,
        BranchReadMode::RowIdOnly,
    )? {
        let Some(source) = in_seek_source_from_expr(
            &branch_terms[chosen_in_seek.constraint_idx].expr,
            &chosen_in_seek,
        ) else {
            return Ok(None);
        };
        best_branch = Some(MultiIdxBranch {
            index: chosen_in_seek.index,
            access: MultiIdxBranchAccess::InSeek {
                source,
                constraint_idx: chosen_in_seek.constraint_idx,
            },
            cost: chosen_in_seek.cost,
            estimated_rows: chosen_in_seek.estimated_rows_per_outer_row,
            union_prepost_filters: None,
        });
    }

    Ok(best_branch)
}

/// Residual output from [`partition_residual_multi_or_exprs`].
struct MultiOrResidualPrePostFilters {
    pre_filter_exprs: Vec<ast::Expr>,
    post_filter_exprs: Vec<ast::Expr>,
    /// Combined table mask for `post_filter_exprs`.
    post_mask: TableMask,
}

/// Classify unconsumed branch conjuncts into pre-filters (outer-table-only,
/// evaluated before the index seek) and post-filters (evaluated after the seek).
///
/// Returns `None` if any residual contains a subquery or has an unresolvable
/// table mask—matching the old `residual_tables_mask` rejection.
fn partition_residual_multi_or_exprs(
    branch_terms: &[WhereTerm],
    access: &MultiIdxBranchAccess,
    index: Option<&Index>,
    rhs_table: &JoinedTable,
    lhs_mask: &TableMask,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
) -> Result<Option<MultiOrResidualPrePostFilters>> {
    let mut consumed = vec![false; branch_terms.len()];
    match access {
        MultiIdxBranchAccess::Seek {
            constraints,
            constraint_refs,
        } => {
            for cref in constraint_refs.iter() {
                for idx in [
                    cref.eq.as_ref().map(|e| e.constraint_pos),
                    cref.lower_bound,
                    cref.upper_bound,
                ]
                .into_iter()
                .flatten()
                {
                    consumed[constraints[idx].where_clause_pos.0] = true;
                }
            }
        }
        MultiIdxBranchAccess::InSeek { constraint_idx, .. } => consumed[*constraint_idx] = true,
    }
    if let Some(index) = index {
        if index.where_clause.is_some() {
            let Some(predicate_terms) =
                partial_index_predicate_terms(index, rhs_table, branch_terms)
            else {
                return Ok(None);
            };
            for idx in predicate_terms {
                consumed[idx] = true;
            }
        }
    }

    let mut pre_filter_exprs = Vec::new();
    let mut post_filter_exprs = Vec::new();
    let mut post_mask = TableMask::default();

    for (idx, term) in branch_terms.iter().enumerate() {
        if consumed[idx] {
            continue;
        }
        let expr = &term.expr;
        if expr_references_any_subquery(expr) {
            return Ok(None);
        }
        let mask = table_mask_from_expr(expr, table_references, subqueries)?;
        if lhs_mask.contains_all_set_bits_of(&mask) {
            pre_filter_exprs.push(expr.clone());
        } else {
            post_mask.union_with(&mask)?;
            post_filter_exprs.push(expr.clone());
        }
    }

    Ok(Some(MultiOrResidualPrePostFilters {
        pre_filter_exprs,
        post_filter_exprs,
        post_mask,
    }))
}

/// Estimate selectivity for a residual predicate that remains after a branch
/// seek is chosen.
///
/// We keep this intentionally heuristic: recurse through boolean structure and,
/// for leaf predicates, reuse normal constraint selectivity analysis when the
/// expression can be recognized as a single-table constraint.
#[allow(clippy::too_many_arguments)]
fn estimate_residual_expr_selectivity(
    expr: &ast::Expr,
    rhs_table: &JoinedTable,
    table_references: &TableReferences,
    available_indexes: &AvailableIndexes,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    params: &CostModelParams,
) -> f64 {
    let Ok(expr) = crate::translate::expr::unwrap_parens(expr) else {
        return params.sel_other;
    };

    match expr {
        ast::Expr::Binary(lhs, ast::Operator::And, rhs) => {
            estimate_residual_expr_selectivity(
                lhs,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            ) * estimate_residual_expr_selectivity(
                rhs,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            )
        }
        ast::Expr::Binary(lhs, ast::Operator::Or, rhs) => {
            let lhs_selectivity = estimate_residual_expr_selectivity(
                lhs,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            );
            let rhs_selectivity = estimate_residual_expr_selectivity(
                rhs,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            );
            1.0 - (1.0 - lhs_selectivity) * (1.0 - rhs_selectivity)
        }
        ast::Expr::Unary(ast::UnaryOperator::Not, inner) => {
            1.0 - estimate_residual_expr_selectivity(
                inner,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            )
        }
        _ => {
            let Ok((_, table_constraints)) = get_table_local_constraints_for_branch(
                &[expr.clone()],
                None,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            ) else {
                return params.sel_other;
            };

            table_constraints
                .constraints
                .iter()
                .filter(|constraint| constraint.where_clause_pos.0 == 0)
                .map(|constraint| constraint.selectivity)
                // A single residual expression can sometimes yield multiple
                // derived constraints (for example, self-comparisons). Use the
                // strongest single estimate instead of multiplying duplicates.
                .reduce(f64::min)
                .unwrap_or(params.sel_other)
        }
    }
    .clamp(0.0, 1.0)
}

#[allow(clippy::too_many_arguments)]
fn estimate_multi_or_residual_selectivity(
    residual_exprs: &[ast::Expr],
    rhs_table: &JoinedTable,
    table_references: &TableReferences,
    available_indexes: &AvailableIndexes,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    params: &CostModelParams,
) -> f64 {
    residual_exprs
        .iter()
        .map(|expr| {
            estimate_residual_expr_selectivity(
                expr,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            )
        })
        .product::<f64>()
        .clamp(0.0, 1.0)
}

#[allow(clippy::too_many_arguments)]
/// Evaluate a fully decomposed multi-index plan and return it if it beats the
/// current best non-multi-index access cost.
fn evaluate_multi_index_branches(
    branches: Vec<MultiIdxBranch>,
    set_op: SetOperation,
    where_term_idx: usize,
    rhs_table: &JoinedTable,
    table_references: &TableReferences,
    available_indexes: &AvailableIndexes,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    base_row_count: RowCountEstimate,
    input_cardinality: f64,
    params: &CostModelParams,
    best_cost: Cost,
) -> Option<AccessMethod> {
    let mut branch_costs = Vec::with_capacity(branches.len());
    let mut branch_rows = Vec::with_capacity(branches.len());
    let mut branch_params = Vec::with_capacity(branches.len());

    for branch in branches {
        let where_cost = branch
            .union_prepost_filters
            .as_ref()
            .map(|filters| {
                let pre_steps: usize = filters.pre_filter_exprs.iter().map(where_expr_steps).sum();
                let post_steps: usize =
                    filters.post_filter_exprs.iter().map(where_expr_steps).sum();
                Cost(
                    (pre_steps as f64 + branch.estimated_rows * post_steps as f64)
                        * params.cpu_cost_per_where_step,
                )
            })
            .unwrap_or(Cost(0.0));
        let post_filter_exprs = branch
            .union_prepost_filters
            .as_ref()
            .map(|r| &r.post_filter_exprs);
        let selectivity = if let Some(post_filter_exprs) = post_filter_exprs {
            estimate_multi_or_residual_selectivity(
                post_filter_exprs,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            )
        } else {
            1.0
        };
        let estimated_rows = branch.estimated_rows * selectivity;

        let params_for_branch = MultiIndexBranchParams {
            index: branch.index.clone(),
            access: match branch.access {
                MultiIdxBranchAccess::Seek {
                    constraints,
                    constraint_refs,
                } => MultiIndexBranchAccessParams::Seek {
                    constraints,
                    constraint_refs,
                },
                MultiIdxBranchAccess::InSeek { source, .. } => {
                    MultiIndexBranchAccessParams::InSeek { source }
                }
            },
            estimated_rows,
            residuals: branch.union_prepost_filters,
        };

        branch_costs.push(branch.cost + where_cost);
        branch_rows.push(params_for_branch.estimated_rows);
        branch_params.push(params_for_branch);
    }

    let (multi_index_cost, estimated_rows) = match &set_op {
        SetOperation::Union => estimate_multi_index_scan_cost(
            &branch_costs,
            &branch_rows,
            base_row_count,
            input_cardinality,
            params,
        ),
        SetOperation::Intersection { .. } => estimate_multi_index_intersection_cost(
            &branch_costs,
            &branch_rows,
            base_row_count,
            input_cardinality,
            params,
        ),
    };

    if multi_index_cost < best_cost {
        let mut consumed_where_terms = SmallVec::<[usize; 4]>::new();
        consumed_where_terms.push(where_term_idx);
        if let SetOperation::Intersection {
            additional_consumed_terms,
        } = &set_op
        {
            for term_idx in additional_consumed_terms.iter() {
                if !consumed_where_terms.contains(&term_idx) {
                    consumed_where_terms.push(term_idx);
                }
            }
        }
        for branch in &branch_params {
            if let MultiIndexBranchAccessParams::Seek { constraints, .. } = &branch.access {
                for constraint in constraints {
                    let where_term_idx = constraint.where_clause_pos.0;
                    if !consumed_where_terms.contains(&where_term_idx) {
                        consumed_where_terms.push(where_term_idx);
                    }
                }
            }
        }
        Some(AccessMethod {
            cost: multi_index_cost,
            estimated_rows_per_outer_row: estimated_rows,
            consumed_where_terms,
            params: AccessMethodParams::MultiIndexScan {
                branches: branch_params,
                where_term_idx,
                set_op,
            },
        })
    } else {
        None
    }
}

/// Whether a multi-index scan on `table` may be driven by `term`.
///
/// The scan *is* the term's evaluation: rows failing it are never visited, and
/// the term is marked consumed so nothing checks it again. For a table that an
/// outer join can null-extend (the right-hand table of a LEFT/FULL JOIN, or
/// any table on the left side of a FULL JOIN) that only holds for the join's
/// own ON clause, which defines what counts as a match. Any other term must
/// also reject the null-extended row the join emits when nothing matched, and
/// that row is produced by jumping straight past the scan — so consuming such
/// a term silently drops it.
fn multi_index_can_consume_term(
    table: &JoinedTable,
    term: &WhereTerm,
    table_references: &TableReferences,
) -> bool {
    !table_references.outer_join_may_null_extend(table.internal_id)
        || term.from_outer_join == Some(table.internal_id)
}

#[allow(clippy::too_many_arguments)]
/// Analyze top-level AND terms to determine whether they can be executed as an
/// AND-by-intersection plan.
///
/// Returns `Some(...)` only when:
/// 1. Multiple terms constrain the same table
/// 2. Each term is individually indexable
/// 3. No single composite index already covers multiple terms more directly
/// 4. At least two distinct indexes participate in the final branch set
fn analyze_and_terms_for_multi_index(
    table_reference: &JoinedTable,
    where_clause: &[WhereTerm],
    available_indexes: &AvailableIndexes,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    params: &CostModelParams,
) -> Option<AndClauseDecomposition> {
    let table_id = table_reference.internal_id;
    let indexes = available_indexes.indexes_for_table(table_reference.internal_id);
    let rowid_alias_column = table_reference
        .columns()
        .iter()
        .position(|c| c.is_rowid_alias());

    // Collect AND terms that:
    // 1. Reference this table
    // 2. Are simple binary comparisons
    // 3. Can use an index
    // 4. Are not already consumed
    // 5. Are local constraints rather than cross-table join conditions
    let mut candidate_branches: Vec<AndBranchSummary> = Vec::new();

    for (where_term_idx, term) in where_clause.iter().enumerate() {
        if term.consumed || matches!(&term.expr, ast::Expr::Binary(_, ast::Operator::Or, _)) {
            continue;
        }
        if !multi_index_can_consume_term(table_reference, term, table_references) {
            continue;
        }

        let Some(summary) = summarize_binary_term_for_index(
            &term.expr,
            table_id,
            table_reference,
            where_clause,
            indexes,
            rowid_alias_column,
            table_references,
            subqueries,
        ) else {
            continue;
        };

        if !summary.lhs_mask.is_empty() {
            continue;
        }

        candidate_branches.push(AndBranchSummary {
            where_term_idx,
            table_col_pos: summary.table_col_pos,
            index: summary.best_index,
        });
    }

    if candidate_branches.len() < 2 {
        return None;
    }

    // If a composite index already covers multiple constrained columns, prefer
    // that single lookup path over intersection.
    if let Some(indexes) = indexes {
        for index in indexes.iter().filter(|idx| idx.index_method.is_none()) {
            // An unproven partial index cannot be the single-index alternative
            // that suppresses intersection planning.
            if index.where_clause.is_some()
                && !can_use_partial_index(index, table_reference, where_clause)
            {
                continue;
            }
            let mut columns_covered = 0;
            for (i, branch) in candidate_branches.iter().enumerate() {
                let col_pos = branch.table_col_pos;
                if let Some(col_pos) = col_pos {
                    if let Some(idx_pos) = index.column_table_pos_to_index_pos(col_pos) {
                        if idx_pos < index.columns.len() {
                            let earlier_covered = candidate_branches[..i]
                                .iter()
                                .filter_map(|candidate| candidate.table_col_pos)
                                .any(|c| {
                                    index
                                        .column_table_pos_to_index_pos(c)
                                        .is_some_and(|p| p < idx_pos)
                                });
                            if idx_pos == 0 || earlier_covered {
                                columns_covered += 1;
                            }
                        }
                    }
                }
            }
            if columns_covered >= 2 {
                return None;
            }
        }
    }

    // Keep only branches that use distinct named indexes. Rowid (`None`) may
    // still appear more than once because it is not tied to a named index.
    let mut selected_branches: Vec<AndBranchSummary> = Vec::new();
    let mut seen_indexes: Vec<*const Index> = Vec::new();
    for branch in candidate_branches {
        if let Some(index) = branch.index.as_ref() {
            let index_ptr = Arc::as_ptr(index);
            if seen_indexes.contains(&index_ptr) {
                continue;
            }
            seen_indexes.push(index_ptr);
        }
        selected_branches.push(branch);
    }

    if selected_branches.len() < 2 {
        return None;
    }

    let unique_branches = selected_branches
        .into_iter()
        .map(|branch| {
            let analyzed = analyze_binary_term_for_index(
                &where_clause[branch.where_term_idx].expr,
                branch.where_term_idx,
                table_id,
                table_reference,
                where_clause,
                indexes,
                rowid_alias_column,
                table_references,
                subqueries,
                schema,
                params,
            )
            .expect("multi-index prepass accepted a term that full analysis rejected");

            turso_assert_eq!(analyzed.constraint.table_col_pos, branch.table_col_pos);

            AndBranch {
                where_term_idx: branch.where_term_idx,
                constraint: analyzed.constraint,
                index: analyzed.best_index,
                constraint_refs: analyzed.constraint_refs,
            }
        })
        .collect::<Vec<_>>();

    Some(AndClauseDecomposition {
        term_indices: unique_branches.iter().map(|b| b.where_term_idx).collect(),
        branches: unique_branches,
    })
}

/// The `OR` terms of the `WHERE` clause that a union scan of `table_reference`
/// could be built from, by shape alone.
///
/// These are the cheap checks. What each disjunct actually constrains on the
/// table is worked out later, per term, by
/// [`branch_constraints_for_or_term`].
fn or_terms_worth_trying(
    table_reference: &JoinedTable,
    where_clause: &[WhereTerm],
    table_references: &TableReferences,
) -> Vec<OrTermSlot> {
    where_clause
        .iter()
        .enumerate()
        .filter(|(_, term)| {
            !term.consumed
                && multi_index_can_consume_term(table_reference, term, table_references)
                && matches!(&term.expr, ast::Expr::Binary(_, ast::Operator::Or, _))
                && flatten_or_expr(&term.expr).len() >= 2
        })
        .map(|(where_term_idx, term)| OrTermSlot {
            where_term_idx,
            from_outer_join: term.from_outer_join,
            disjuncts: OnceCell::new(),
        })
        .collect()
}

/// What each disjunct of an `OR` term constrains on `table_reference`, or
/// `None` when no join order can turn the term into a union scan of it.
#[expect(clippy::too_many_arguments)]
fn branch_constraints_for_or_term(
    term_expr: &ast::Expr,
    from_outer_join: Option<TableInternalId>,
    table_reference: &JoinedTable,
    table_references: &TableReferences,
    available_indexes: &AvailableIndexes,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    params: &CostModelParams,
) -> Option<Vec<BranchConstraints>> {
    // Every disjunct has to become a branch, so one unusable disjunct rules out
    // the whole term.
    flatten_or_expr(term_expr)
        .into_iter()
        .map(|disjunct_expr| {
            branch_constraints_for_disjunct(
                disjunct_expr,
                from_outer_join,
                table_reference,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            )
        })
        .collect()
}

/// What one disjunct constrains on `table_reference`, or `None` when no join
/// order can turn it into a union branch.
#[expect(clippy::too_many_arguments)]
fn branch_constraints_for_disjunct(
    disjunct_expr: &ast::Expr,
    from_outer_join: Option<TableInternalId>,
    table_reference: &JoinedTable,
    table_references: &TableReferences,
    available_indexes: &AvailableIndexes,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    params: &CostModelParams,
) -> Option<BranchConstraints> {
    let Ok(disjunct_expr) = crate::translate::expr::unwrap_parens(disjunct_expr) else {
        return None;
    };
    // Each disjunct is replanned with branch-local `TableConstraints`, so
    // compound conjuncts can reuse the same compound-seek analysis as ordinary
    // btree access.
    let conjuncts = flatten_and_expr(disjunct_expr)
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let (terms, constraints) = get_table_local_constraints_for_branch(
        &conjuncts,
        from_outer_join,
        table_reference,
        table_references,
        available_indexes,
        subqueries,
        schema,
        params,
    )
    .ok()?;

    // A branch is a seek on this table, so a disjunct that constrains nothing
    // on it has nothing to seek by. Both branch access paths agree on that:
    // `choose_best_btree_candidate` can only pick constraint refs drawn from
    // these constraints, and `choose_best_in_seek_candidate` scans them for an
    // `IN`. A join order prefix only ever narrows which of them may be used, so
    // no prefix can conjure a branch out of none.
    if constraints.constraints.is_empty() {
        return None;
    }

    Some(BranchConstraints { terms, constraints })
}

#[allow(clippy::too_many_arguments)]
/// Analyze OR clauses for OR-by-union optimization.
///
/// Returns a `MultiIndexScan` access method when every disjunct can be planned
/// as an individual lookup branch and the combined cost beats the current best
/// non-multi-index alternative.
pub fn consider_multi_index_union(
    rhs_table: &JoinedTable,
    rhs_table_idx: usize,
    or_terms_memo: &MultiIndexOrTermsMemo,
    where_clause: &[WhereTerm],
    available_indexes: &AvailableIndexes,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
    best_cost: Cost,
    lhs_mask: &TableMask,
    analyze_stats: &AnalyzeStats,
) -> Result<Option<AccessMethod>> {
    let or_terms = or_terms_memo.get_or_find_terms(rhs_table_idx, || {
        or_terms_worth_trying(rhs_table, where_clause, table_references)
    });
    if or_terms.is_empty() {
        return Ok(None);
    }

    let mut allowed_mask = lhs_mask.try_clone()?;
    allowed_mask.set(rhs_table_idx)?;

    for or_term in or_terms {
        let Some(disjuncts) = or_term.disjuncts.get_or_init(|| {
            branch_constraints_for_or_term(
                &where_clause[or_term.where_term_idx].expr,
                or_term.from_outer_join,
                rhs_table,
                table_references,
                available_indexes,
                subqueries,
                schema,
                params,
            )
        }) else {
            continue;
        };

        let branches = disjuncts
            .iter()
            .map(|disjunct| {
                let Some(mut chosen) = choose_multi_index_branch_access(
                    rhs_table,
                    &disjunct.constraints,
                    &disjunct.terms,
                    lhs_mask,
                    rhs_table_idx,
                    schema,
                    available_indexes,
                    base_row_count,
                    analyze_stats,
                    params,
                )?
                else {
                    return Ok(None);
                };
                // Partition residuals in a single pass: pre-filters reference
                // only outer (lhs) tables and can short-circuit the branch
                // before the index seek; post-filters reference the target
                // table and are evaluated after the seek.
                let Some(partitioned_pre_post) = partition_residual_multi_or_exprs(
                    &disjunct.terms,
                    &chosen.access,
                    chosen.index.as_deref(),
                    rhs_table,
                    lhs_mask,
                    table_references,
                    subqueries,
                )?
                else {
                    return Ok(None);
                };
                if !allowed_mask.contains_all_set_bits_of(&partitioned_pre_post.post_mask) {
                    return Ok(None);
                }
                chosen.union_prepost_filters = Some(UnionBranchPrePostFilters {
                    requires_table_cursor: partitioned_pre_post.post_mask.get(rhs_table_idx),
                    pre_filter_exprs: partitioned_pre_post.pre_filter_exprs,
                    post_filter_exprs: partitioned_pre_post.post_filter_exprs,
                });
                Ok(Some(chosen))
            })
            .collect::<Result<_>>()?;

        let Some(branches) = branches else {
            continue;
        };

        if let Some(access_method) = evaluate_multi_index_branches(
            branches,
            SetOperation::Union,
            or_term.where_term_idx,
            rhs_table,
            table_references,
            available_indexes,
            subqueries,
            schema,
            base_row_count,
            input_cardinality,
            params,
            best_cost,
        ) {
            return Ok(Some(access_method));
        }
    }

    Ok(None)
}

/// Analyze top-level AND terms for AND-by-intersection optimization.
///
/// This is more restrictive than OR-by-union because every branch must be a
/// local term on the current table, and the final plan only survives if it
/// beats the best ordinary access path.
#[expect(clippy::too_many_arguments)]
pub fn consider_multi_index_intersection(
    rhs_table: &JoinedTable,
    rhs_table_idx: usize,
    and_terms_memo: &MultiIndexAndTermsMemo,
    where_clause: &[WhereTerm],
    available_indexes: &AvailableIndexes,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
    best_cost: Cost,
    lhs_mask: &TableMask,
    analyze_stats: &AnalyzeStats,
) -> Result<Option<AccessMethod>> {
    let Some(decomposition) = and_terms_memo.get_or_analyze(rhs_table_idx, || {
        analyze_and_terms_for_multi_index(
            rhs_table,
            where_clause,
            available_indexes,
            table_references,
            subqueries,
            schema,
            params,
        )
    }) else {
        return Ok(None);
    };

    if decomposition.branches.len() < 2 {
        return Ok(None);
    }

    let all_usable = decomposition
        .branches
        .iter()
        .all(|b| lhs_mask.contains_all_set_bits_of(&b.constraint.lhs_mask));
    if !all_usable {
        return Ok(None);
    }

    let branches: Vec<_> = decomposition
        .branches
        .iter()
        .map(|b| {
            let constraints = vec![b.constraint.clone()];
            let index_info = index_info_for_branch(
                b.index.as_deref(),
                rhs_table,
                BranchReadMode::RowIdOnly,
                params.rows_per_table_page,
            )
            .expect("intersection branches always have costable access");
            let analyze_ctx = AnalyzeCtx {
                rhs_table,
                index: b.index.as_ref(),
                stats: analyze_stats,
            };
            MultiIdxBranch {
                index: b.index.clone(),
                access: MultiIdxBranchAccess::Seek {
                    constraints: constraints.clone(),
                    constraint_refs: b.constraint_refs.clone(),
                },
                cost: estimate_cost_for_scan_or_seek(
                    Some(index_info),
                    &constraints,
                    &b.constraint_refs,
                    1.0,
                    base_row_count,
                    false,
                    params,
                    Some(&analyze_ctx),
                ),
                estimated_rows: estimate_rows_per_seek(
                    index_info,
                    &constraints,
                    &b.constraint_refs,
                    base_row_count,
                    Some(&analyze_ctx),
                ),
                union_prepost_filters: None,
            }
        })
        .collect();

    let where_term_idx = decomposition.term_indices[0];
    let additional_consumed_terms: BitSet = decomposition
        .term_indices
        .iter()
        .skip(1)
        .copied()
        .try_collect()?;

    Ok(evaluate_multi_index_branches(
        branches,
        SetOperation::Intersection {
            additional_consumed_terms,
        },
        where_term_idx,
        rhs_table,
        table_references,
        available_indexes,
        subqueries,
        schema,
        base_row_count,
        input_cardinality,
        params,
        best_cost,
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        consider_multi_index_intersection, consider_multi_index_union, AnalyzeStats,
        AndClauseDecomposition, MultiIndexAndTermsMemo, MultiIndexBranchParams,
        MultiIndexOrTermsMemo, OrTermSlot,
    };
    use crate::alloc::TursoIteratorExt;
    use crate::alloc::TursoSliceExt;
    use crate::{
        schema::{
            BTreeCharacteristics, BTreeTable, ColDef, Column, Index, IndexColumn, Schema, Table,
            Type,
        },
        translate::{
            optimizer::{
                access_method::AccessMethodParams,
                cost::{Cost, RowCountEstimate},
                cost_params::DEFAULT_PARAMS,
                AvailableIndexes,
            },
            plan::{
                ColumnUsedMask, JoinInfo, JoinType, JoinedTable, Operation, TableReferences,
                WhereTerm,
            },
            planner::TableMask,
        },
        vdbe::builder::TableRefIdCounter,
        MAIN_DB_ID,
    };
    use std::cell::{Cell, OnceCell};
    use std::{collections::VecDeque, sync::Arc};
    use turso_parser::ast::{self, Expr, Operator, TableInternalId};

    struct TestColumn {
        name: String,
        ty: Type,
        is_rowid_alias: bool,
    }

    fn empty_schema() -> Schema {
        Schema::default()
    }

    fn create_column(c: &TestColumn) -> Column {
        Column::new(
            Some(c.name.clone()),
            c.ty.to_string(),
            None,
            None,
            c.ty,
            None,
            ColDef {
                primary_key: false,
                rowid_alias: c.is_rowid_alias,
                ..Default::default()
            },
        )
    }

    fn create_column_of_type(name: &str, ty: Type) -> Column {
        create_column(&TestColumn {
            name: name.to_string(),
            ty,
            is_rowid_alias: false,
        })
    }

    fn create_btree_table(name: &str, columns: Vec<Column>) -> Arc<BTreeTable> {
        Arc::new(BTreeTable::new(
            1,
            name.to_string(),
            crate::alloc::vec![],
            columns.try_to_vec().expect(crate::alloc::ALLOC_ERR_MSG),
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        ))
    }

    fn create_table_reference(
        table: Arc<BTreeTable>,
        join_info: Option<JoinInfo>,
        internal_id: TableInternalId,
    ) -> JoinedTable {
        let name = table.name.clone();
        let table = Table::BTree(table);
        JoinedTable {
            op: Operation::default_scan_for(&table),
            table,
            identifier: name,
            internal_id,
            join_info,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: MAIN_DB_ID,
            indexed: None,
        }
    }

    fn create_column_expr(table: TableInternalId, column: usize, is_rowid_alias: bool) -> Expr {
        Expr::Column {
            database: None,
            table,
            column,
            is_rowid_alias,
        }
    }

    fn create_numeric_literal(value: &str) -> Expr {
        Expr::Literal(ast::Literal::Numeric(value.to_string()))
    }

    fn create_string_literal(value: &str) -> Expr {
        Expr::Literal(ast::Literal::String(value.to_string()))
    }

    fn assert_is_multi_index(
        access_method: &crate::translate::optimizer::access_method::AccessMethod,
    ) -> &Vec<MultiIndexBranchParams> {
        let AccessMethodParams::MultiIndexScan { branches, .. } = &access_method.params else {
            panic!("expected multi-index scan access method");
        };
        branches
    }

    #[test]
    fn test_multi_index_union_rejects_residuals_on_future_tables() {
        let link = create_btree_table(
            "link",
            vec![
                create_column_of_type("src", Type::Integer),
                create_column_of_type("dst", Type::Integer),
            ],
        );
        let item = create_btree_table(
            "item",
            vec![
                create_column_of_type("id", Type::Integer),
                create_column_of_type("kind", Type::Text),
            ],
        );
        let meta = create_btree_table(
            "meta",
            vec![
                create_column_of_type("id", Type::Integer),
                create_column_of_type("kind", Type::Text),
            ],
        );

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            create_table_reference(link, None, table_id_counter.next()),
            create_table_reference(
                item,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
            create_table_reference(
                meta,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
        ];

        const LINK: usize = 0;
        const ITEM: usize = 1;
        const META: usize = 2;

        let mut available_indexes = AvailableIndexes::default();
        available_indexes.insert_for_table_name(
            &joined_tables,
            "item",
            VecDeque::from([Arc::new(Index {
                name: "idx_item_id".to_string(),
                table_name: "item".to_string(),
                where_clause: None,
                columns: IndexColumn::new_many(vec!["id"]),
                unique: false,
                ephemeral: false,
                root_page: 2,
                has_rowid: true,
                index_method: None,
                on_conflict: None,
            })]),
        );

        let lhs_link_src = Expr::Binary(
            Box::new(create_column_expr(
                joined_tables[LINK].internal_id,
                0,
                false,
            )),
            Operator::Equals,
            Box::new(create_numeric_literal("1")),
        );
        let lhs_link_dst_item_id = Expr::Binary(
            Box::new(create_column_expr(
                joined_tables[LINK].internal_id,
                1,
                false,
            )),
            Operator::Equals,
            Box::new(create_column_expr(
                joined_tables[ITEM].internal_id,
                0,
                false,
            )),
        );
        let rhs_link_dst = Expr::Binary(
            Box::new(create_column_expr(
                joined_tables[LINK].internal_id,
                1,
                false,
            )),
            Operator::Equals,
            Box::new(create_numeric_literal("1")),
        );
        let rhs_link_src_item_id = Expr::Binary(
            Box::new(create_column_expr(
                joined_tables[LINK].internal_id,
                0,
                false,
            )),
            Operator::Equals,
            Box::new(create_column_expr(
                joined_tables[ITEM].internal_id,
                0,
                false,
            )),
        );
        let future_meta_kind = Expr::Binary(
            Box::new(create_column_expr(
                joined_tables[META].internal_id,
                1,
                false,
            )),
            Operator::Equals,
            Box::new(create_string_literal("entity")),
        );

        let left_disjunct = Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(lhs_link_src),
                Operator::And,
                Box::new(lhs_link_dst_item_id),
            )),
            Operator::And,
            Box::new(future_meta_kind.clone()),
        );
        let right_disjunct = Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(rhs_link_dst),
                Operator::And,
                Box::new(rhs_link_src_item_id),
            )),
            Operator::And,
            Box::new(future_meta_kind),
        );
        let where_clause = vec![WhereTerm {
            expr: Expr::Binary(
                Box::new(left_disjunct),
                Operator::Or,
                Box::new(right_disjunct),
            ),
            from_outer_join: None,
            consumed: false,
        }];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let base_row_count = RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS);
        let lhs_mask: TableMask = [LINK].into_iter().try_collect().unwrap();

        let access_method = consider_multi_index_union(
            &table_references.joined_tables()[ITEM],
            ITEM,
            &MultiIndexOrTermsMemo::new(table_references.joined_tables().len()),
            &where_clause,
            &available_indexes,
            &table_references,
            &[],
            &empty_schema(),
            1.0,
            base_row_count,
            &DEFAULT_PARAMS,
            Cost(f64::INFINITY),
            &lhs_mask,
            &AnalyzeStats::default(),
        )
        .unwrap();

        assert!(
            access_method.is_none(),
            "future-table residuals must not produce a multi-index OR access method"
        );
    }

    #[test]
    fn test_multi_index_intersection_supports_rowid_and_secondary_index_branches() {
        let item = create_btree_table(
            "item",
            vec![
                create_column(&TestColumn {
                    name: "id".to_string(),
                    ty: Type::Integer,
                    is_rowid_alias: true,
                }),
                create_column_of_type("a", Type::Integer),
            ],
        );

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![create_table_reference(item, None, table_id_counter.next())];
        let item_id = joined_tables[0].internal_id;

        let mut available_indexes = AvailableIndexes::default();
        available_indexes.insert_for_table_name(
            &joined_tables,
            "item",
            VecDeque::from([Arc::new(Index {
                name: "idx_item_a".to_string(),
                table_name: "item".to_string(),
                where_clause: None,
                columns: crate::alloc::vec![IndexColumn::new("a", 1)],
                unique: false,
                ephemeral: false,
                root_page: 2,
                has_rowid: true,
                index_method: None,
                on_conflict: None,
            })]),
        );

        let where_clause = vec![
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(create_column_expr(item_id, 0, true)),
                    Operator::Greater,
                    Box::new(create_numeric_literal("10")),
                ),
                from_outer_join: None,
                consumed: false,
            },
            WhereTerm {
                expr: Expr::Binary(
                    Box::new(create_column_expr(item_id, 1, false)),
                    Operator::Equals,
                    Box::new(create_numeric_literal("7")),
                ),
                from_outer_join: None,
                consumed: false,
            },
        ];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let base_row_count = RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS);

        let and_terms_memo = MultiIndexAndTermsMemo::new(table_references.joined_tables().len());
        let access_method = consider_multi_index_intersection(
            &table_references.joined_tables()[0],
            0,
            &and_terms_memo,
            &where_clause,
            &available_indexes,
            &table_references,
            &[],
            &empty_schema(),
            1.0,
            base_row_count,
            &DEFAULT_PARAMS,
            Cost(f64::INFINITY),
            &TableMask::default(),
            &AnalyzeStats::default(),
        )
        .unwrap()
        .expect("rowid and secondary-index terms should be eligible for intersection");

        let branches = assert_is_multi_index(&access_method);
        assert_eq!(branches.len(), 2);
        assert!(
            branches.iter().any(|branch| branch.index.is_none()),
            "expected one rowid branch"
        );
        assert!(
            branches
                .iter()
                .any(|branch| branch.index.as_ref().map(|idx| idx.name.as_str())
                    == Some("idx_item_a")),
            "expected one secondary-index branch"
        );

        // The memo holds the decomposition, not the plan built from it: asking
        // again with a cost ceiling the intersection cannot beat must still
        // reject it. A memo that froze the whole answer would hand back the
        // plan above a second time.
        let too_expensive = consider_multi_index_intersection(
            &table_references.joined_tables()[0],
            0,
            &and_terms_memo,
            &where_clause,
            &available_indexes,
            &table_references,
            &[],
            &empty_schema(),
            1.0,
            base_row_count,
            &DEFAULT_PARAMS,
            Cost(0.0),
            &TableMask::default(),
            &AnalyzeStats::default(),
        )
        .unwrap();
        assert!(
            too_expensive.is_none(),
            "a zero cost ceiling must reject the intersection plan"
        );
    }

    #[test]
    fn and_terms_memo_analyzes_each_table_once() {
        let memo = MultiIndexAndTermsMemo::new(2);
        let analyses = Cell::new(0);
        // Both closures only capture `&analyses`, so they are `Copy` and can be
        // handed to the memo more than once.
        let analyze_table_0 = || {
            analyses.set(analyses.get() + 1);
            Some(AndClauseDecomposition {
                term_indices: vec![7],
                branches: vec![],
            })
        };
        let analyze_table_1 = || {
            analyses.set(analyses.get() + 1);
            None
        };

        assert_eq!(
            memo.get_or_analyze(0, analyze_table_0)
                .map(|d| d.term_indices.as_slice()),
            Some([7].as_slice())
        );
        assert_eq!(analyses.get(), 1);

        // Asking about the same table again answers from the memo. This is the
        // whole point: the join order search asks once per join order it tries.
        assert_eq!(
            memo.get_or_analyze(0, analyze_table_0)
                .map(|d| d.term_indices.as_slice()),
            Some([7].as_slice())
        );
        assert_eq!(analyses.get(), 1);

        // Another table is a separate question, so it gets analyzed.
        assert!(memo.get_or_analyze(1, analyze_table_1).is_none());
        assert_eq!(analyses.get(), 2);

        // "No intersection possible" is an answer worth remembering too - it is
        // the answer for most tables in a join-heavy query.
        assert!(memo.get_or_analyze(1, analyze_table_1).is_none());
        assert_eq!(analyses.get(), 2);
    }

    #[test]
    fn or_terms_memo_finds_each_table_s_terms_once() {
        let memo = MultiIndexOrTermsMemo::new(2);
        let searches = Cell::new(0);
        // Both closures only capture `&searches`, so they are `Copy` and can be
        // handed to the memo more than once.
        let find_for_table_0 = || {
            searches.set(searches.get() + 1);
            vec![OrTermSlot {
                where_term_idx: 7,
                from_outer_join: None,
                disjuncts: OnceCell::new(),
            }]
        };
        let find_for_table_1 = || {
            searches.set(searches.get() + 1);
            vec![]
        };

        let term_indices =
            |slots: &[OrTermSlot]| slots.iter().map(|s| s.where_term_idx).collect::<Vec<_>>();

        assert_eq!(
            term_indices(memo.get_or_find_terms(0, find_for_table_0)),
            [7]
        );
        assert_eq!(searches.get(), 1);

        // Asking about the same table again answers from the memo. This is the
        // whole point: the join order search asks once per join order it tries.
        assert_eq!(
            term_indices(memo.get_or_find_terms(0, find_for_table_0)),
            [7]
        );
        assert_eq!(searches.get(), 1);

        // Another table is a separate question, so it gets its own search.
        assert!(memo.get_or_find_terms(1, find_for_table_1).is_empty());
        assert_eq!(searches.get(), 1 + 1);

        // "No OR term is worth trying on this table" is an answer worth
        // remembering too - it is the answer for most tables in a join-heavy
        // query.
        assert!(memo.get_or_find_terms(1, find_for_table_1).is_empty());
        assert_eq!(searches.get(), 2);
    }

    /// An `OR` term only offers union branches to the table its disjuncts
    /// constrain. Every other table in the query has nothing to seek by, so the
    /// term is rejected once instead of being re-analyzed for every join order
    /// the search tries.
    #[test]
    fn or_term_is_usable_only_by_the_table_its_disjuncts_constrain() {
        let link = create_btree_table(
            "link",
            vec![
                create_column_of_type("src", Type::Integer),
                create_column_of_type("dst", Type::Integer),
            ],
        );
        let item = create_btree_table(
            "item",
            vec![
                create_column_of_type("id", Type::Integer),
                create_column_of_type("kind", Type::Integer),
            ],
        );

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            create_table_reference(link, None, table_id_counter.next()),
            create_table_reference(
                item,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
        ];

        const LINK: usize = 0;
        const ITEM: usize = 1;
        let item_id = joined_tables[ITEM].internal_id;

        // `item.id = 1 OR item.id = 2` - about `item`, and about nothing else.
        let where_clause = vec![WhereTerm {
            expr: Expr::Binary(
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(item_id, 0, false)),
                    Operator::Equals,
                    Box::new(create_numeric_literal("1")),
                )),
                Operator::Or,
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(item_id, 0, false)),
                    Operator::Equals,
                    Box::new(create_numeric_literal("2")),
                )),
            ),
            from_outer_join: None,
            consumed: false,
        }];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let available_indexes = AvailableIndexes::default();

        // By shape alone the term is worth trying on either table.
        for table_idx in [LINK, ITEM] {
            let slots = super::or_terms_worth_trying(
                &table_references.joined_tables()[table_idx],
                &where_clause,
                &table_references,
            );
            assert_eq!(slots.len(), 1);
            assert_eq!(slots[0].where_term_idx, 0);
        }

        let branches = |table_idx: usize| {
            super::branch_constraints_for_or_term(
                &where_clause[0].expr,
                None,
                &table_references.joined_tables()[table_idx],
                &table_references,
                &available_indexes,
                &[],
                &empty_schema(),
                &DEFAULT_PARAMS,
            )
        };

        assert_eq!(
            branches(ITEM).map(|b| b.len()),
            Some(2),
            "both disjuncts constrain `item`, so both become branches"
        );
        assert!(
            branches(LINK).is_none(),
            "an OR term that constrains nothing on `link` cannot drive a union scan of it"
        );
    }

    #[test]
    fn test_multi_index_union_branch_reuses_compound_seek_analysis() {
        let link = create_btree_table(
            "link",
            vec![
                create_column_of_type("src", Type::Integer),
                create_column_of_type("dst", Type::Integer),
            ],
        );
        let item = create_btree_table(
            "item",
            vec![
                create_column_of_type("id", Type::Integer),
                create_column_of_type("kind", Type::Integer),
            ],
        );

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            create_table_reference(link, None, table_id_counter.next()),
            create_table_reference(
                item,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
        ];

        const LINK: usize = 0;
        const ITEM: usize = 1;

        let mut available_indexes = AvailableIndexes::default();
        available_indexes.insert_for_table_name(
            &joined_tables,
            "item",
            VecDeque::from([Arc::new(Index {
                name: "idx_item_id_kind".to_string(),
                table_name: "item".to_string(),
                where_clause: None,
                columns: IndexColumn::new_many(vec!["id", "kind"]),
                unique: false,
                ephemeral: false,
                root_page: 2,
                has_rowid: true,
                index_method: None,
                on_conflict: None,
            })]),
        );

        let left_disjunct = Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(
                        joined_tables[LINK].internal_id,
                        0,
                        false,
                    )),
                    Operator::Equals,
                    Box::new(create_numeric_literal("1")),
                )),
                Operator::And,
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(
                        joined_tables[ITEM].internal_id,
                        0,
                        false,
                    )),
                    Operator::Equals,
                    Box::new(create_column_expr(
                        joined_tables[LINK].internal_id,
                        1,
                        false,
                    )),
                )),
            )),
            Operator::And,
            Box::new(Expr::Binary(
                Box::new(create_column_expr(
                    joined_tables[ITEM].internal_id,
                    1,
                    false,
                )),
                Operator::Equals,
                Box::new(create_numeric_literal("7")),
            )),
        );
        let right_disjunct = Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(
                        joined_tables[LINK].internal_id,
                        1,
                        false,
                    )),
                    Operator::Equals,
                    Box::new(create_numeric_literal("1")),
                )),
                Operator::And,
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(
                        joined_tables[ITEM].internal_id,
                        0,
                        false,
                    )),
                    Operator::Equals,
                    Box::new(create_column_expr(
                        joined_tables[LINK].internal_id,
                        0,
                        false,
                    )),
                )),
            )),
            Operator::And,
            Box::new(Expr::Binary(
                Box::new(create_column_expr(
                    joined_tables[ITEM].internal_id,
                    1,
                    false,
                )),
                Operator::Equals,
                Box::new(create_numeric_literal("7")),
            )),
        );

        let where_clause = vec![WhereTerm {
            expr: Expr::Binary(
                Box::new(left_disjunct),
                Operator::Or,
                Box::new(right_disjunct),
            ),
            from_outer_join: None,
            consumed: false,
        }];

        let table_references = TableReferences::new(joined_tables, vec![]);
        let lhs_mask = [LINK].into_iter().try_collect().unwrap();
        let base_row_count = RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS);

        let access_method = consider_multi_index_union(
            &table_references.joined_tables()[ITEM],
            ITEM,
            &MultiIndexOrTermsMemo::new(table_references.joined_tables().len()),
            &where_clause,
            &available_indexes,
            &table_references,
            &[],
            &empty_schema(),
            1.0,
            base_row_count,
            &DEFAULT_PARAMS,
            Cost(f64::INFINITY),
            &lhs_mask,
            &AnalyzeStats::default(),
        )
        .unwrap()
        .expect("compound OR branches should produce a multi-index union");

        let branches = assert_is_multi_index(&access_method);
        assert_eq!(branches.len(), 2);
        for branch in branches {
            assert_eq!(
                branch.index.as_ref().map(|idx| idx.name.as_str()),
                Some("idx_item_id_kind")
            );
            let super::MultiIndexBranchAccessParams::Seek {
                constraint_refs, ..
            } = &branch.access
            else {
                panic!("compound OR test should choose ordinary seek branches");
            };
            assert_eq!(
                constraint_refs.len(),
                2,
                "branch should use both id and kind in the compound seek"
            );
        }
    }

    #[test]
    fn test_multi_index_union_residual_selectivity_reduces_row_estimate() {
        let link = create_btree_table(
            "link",
            vec![
                create_column_of_type("src", Type::Integer),
                create_column_of_type("dst", Type::Integer),
            ],
        );
        let item = create_btree_table(
            "item",
            vec![
                create_column_of_type("id", Type::Integer),
                create_column_of_type("kind", Type::Integer),
            ],
        );

        let mut table_id_counter = TableRefIdCounter::new();
        let joined_tables = vec![
            create_table_reference(link, None, table_id_counter.next()),
            create_table_reference(
                item,
                Some(JoinInfo {
                    join_type: JoinType::Inner,
                    using: vec![],
                    no_reorder: false,
                }),
                table_id_counter.next(),
            ),
        ];

        const LINK: usize = 0;
        const ITEM: usize = 1;
        let link_id = joined_tables[LINK].internal_id;
        let item_id = joined_tables[ITEM].internal_id;

        let mut available_indexes = AvailableIndexes::default();
        available_indexes.insert_for_table_name(
            &joined_tables,
            "item",
            VecDeque::from([Arc::new(Index {
                name: "idx_item_id".to_string(),
                table_name: "item".to_string(),
                where_clause: None,
                columns: IndexColumn::new_many(vec!["id"]),
                unique: false,
                ephemeral: false,
                root_page: 2,
                has_rowid: true,
                index_method: None,
                on_conflict: None,
            })]),
        );

        let make_branch = |literal_col, join_col, item_kind: Option<&str>| {
            let branch = Expr::Binary(
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(link_id, literal_col, false)),
                    Operator::Equals,
                    Box::new(create_numeric_literal("1")),
                )),
                Operator::And,
                Box::new(Expr::Binary(
                    Box::new(create_column_expr(item_id, 0, false)),
                    Operator::Equals,
                    Box::new(create_column_expr(link_id, join_col, false)),
                )),
            );

            if let Some(kind) = item_kind {
                Expr::Binary(
                    Box::new(branch),
                    Operator::And,
                    Box::new(Expr::Binary(
                        Box::new(create_column_expr(item_id, 1, false)),
                        Operator::Equals,
                        Box::new(create_numeric_literal(kind)),
                    )),
                )
            } else {
                branch
            }
        };
        let make_join_expr = |item_kind: Option<&str>| {
            vec![WhereTerm {
                expr: Expr::Binary(
                    Box::new(make_branch(0, 1, item_kind)),
                    Operator::Or,
                    Box::new(make_branch(1, 0, item_kind)),
                ),
                from_outer_join: None,
                consumed: false,
            }]
        };

        let table_references = TableReferences::new(joined_tables, vec![]);
        let lhs_mask = [LINK].into_iter().try_collect().unwrap();
        let base_row_count = RowCountEstimate::hardcoded_fallback(&DEFAULT_PARAMS);

        // Each call plans a different `WHERE` clause, so each gets its own
        // memo: a memo only answers for the clause it was built against.
        let without_residual = consider_multi_index_union(
            &table_references.joined_tables()[ITEM],
            ITEM,
            &MultiIndexOrTermsMemo::new(table_references.joined_tables().len()),
            &make_join_expr(None),
            &available_indexes,
            &table_references,
            &[],
            &empty_schema(),
            1.0,
            base_row_count,
            &DEFAULT_PARAMS,
            Cost(f64::INFINITY),
            &lhs_mask,
            &AnalyzeStats::default(),
        )
        .unwrap()
        .expect("plain OR branches should produce a multi-index union");

        let with_residual = consider_multi_index_union(
            &table_references.joined_tables()[ITEM],
            ITEM,
            &MultiIndexOrTermsMemo::new(table_references.joined_tables().len()),
            &make_join_expr(Some("7")),
            &available_indexes,
            &table_references,
            &[],
            &empty_schema(),
            1.0,
            base_row_count,
            &DEFAULT_PARAMS,
            Cost(f64::INFINITY),
            &lhs_mask,
            &AnalyzeStats::default(),
        )
        .unwrap()
        .expect("residual-filtered OR branches should still produce a multi-index union");

        assert!(
            with_residual.estimated_rows_per_outer_row
                < without_residual.estimated_rows_per_outer_row,
            "branch-local residual filters must reduce the multi-index row estimate"
        );
    }
}
