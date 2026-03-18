use crate::schema::Index;
use crate::stats::{self, AnalyzeStats};
use crate::sync::Arc;
use crate::translate::optimizer::constraints::RangeConstraintRef;
use crate::translate::plan::{JoinedTable, WhereTerm};
use crate::Value;

use smallvec::SmallVec;

use super::constraints::Constraint;
use super::cost_params::CostModelParams;
use super::selectivity::{closed_range_selectivity, dampened_product};

/// A simple newtype wrapper over a f64 that represents the cost of an operation.
///
/// This is used to estimate the cost of scans, seeks, and joins.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Cost(pub f64);

impl std::ops::Add for Cost {
    type Output = Cost;

    fn add(self, other: Cost) -> Cost {
        Cost(self.0 + other.0)
    }
}

impl std::ops::Deref for Cost {
    type Target = f64;

    fn deref(&self) -> &f64 {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IndexInfo {
    pub unique: bool,
    pub column_count: usize,
    /// Whether the index satisfies the query without table lookups.
    /// True for genuinely covering indexes and for multi-index branches
    /// that only harvest rowids into a RowSet.
    pub covering: bool,
    /// Estimated rows per index leaf page, derived from the column count
    /// ratio between the index and its parent table.
    pub rows_per_leaf_page: f64,
}

/// Estimate rows per index leaf page based on the index/table width ratio.
/// Narrower indexes have smaller entries, fitting more rows per page.
pub fn index_leaf_rows_per_page(
    index_column_count: usize,
    table_column_count: usize,
    has_rowid_alias: bool,
    rows_per_table_page: f64,
) -> f64 {
    // Table width: all columns + implicit rowid (unless one column IS the rowid alias).
    let table_width = table_column_count as f64 + if has_rowid_alias { 0.0 } else { 1.0 };
    // Index width: indexed columns + rowid suffix (always present).
    let index_width = index_column_count as f64 + 1.0;
    (rows_per_table_page * table_width / index_width).max(1.0)
}

/// Compute `rows_per_leaf_page` for an index on the given table.
pub fn rows_per_leaf_page_for_index(
    index_column_count: usize,
    rhs_table: &JoinedTable,
    rows_per_table_page: f64,
) -> f64 {
    let table_column_count = rhs_table.columns().len();
    let has_rowid_alias = rhs_table
        .btree()
        .is_some_and(|bt| bt.get_rowid_alias_column().is_some());
    index_leaf_rows_per_page(
        index_column_count,
        table_column_count,
        has_rowid_alias,
        rows_per_table_page,
    )
}

/// IO cost for `num_scans` passes over `pages`, where the first scan is cold
/// and subsequent scans benefit from the buffer pool.
fn cached_io_cost(pages: f64, num_scans: f64, cache_reuse_factor: f64) -> f64 {
    if num_scans <= 1.0 {
        pages
    } else {
        pages + (num_scans - 1.0) * pages * cache_reuse_factor
    }
}

/// Estimate IO and CPU cost for a full table scan.
fn estimate_scan_cost(base_row_count: f64, num_scans: f64, params: &CostModelParams) -> Cost {
    let table_pages = (base_row_count / params.rows_per_table_page).max(1.0);
    let io_cost = cached_io_cost(table_pages, num_scans, params.cache_reuse_factor);
    let cpu_cost = num_scans * base_row_count * params.cpu_cost_per_row;
    Cost(io_cost + cpu_cost)
}

/// Estimate IO and CPU cost for index-based access.
///
/// This properly separates the number of B-tree seeks from the number of rows
/// returned per seek. A range scan does ONE seek followed by sequential leaf
/// page reads, not one seek per row.
pub fn estimate_index_cost(
    base_row_count: f64,
    tree_depth: f64,
    index_info: IndexInfo,
    input_cardinality: f64,
    rows_per_seek: f64,
    params: &CostModelParams,
) -> Cost {
    // Detect full index scan: when rows_per_seek equals base_row_count, we're scanning
    // the entire index, not seeking to specific positions.
    let is_full_scan = (rows_per_seek - base_row_count).abs() < 1.0;

    // Cost of B-tree traversals: each seek traverses tree_depth pages.
    let seek_cost = if is_full_scan {
        // Full scan: one seek to start, then sequential reads.
        // When re-scanned (nested loop inner), first scan is cold, rest are cached.
        cached_io_cost(tree_depth, input_cardinality, params.cache_reuse_factor)
    } else if input_cardinality <= 1.0 {
        input_cardinality * tree_depth
    } else {
        // Repeated seeks on the same B-tree: the root page (and often upper
        // internal pages) stay in the buffer pool after the first traversal.
        // Subsequent seeks only need to read from the first uncached level,
        // giving an effective depth of (tree_depth - 1), minimum 1 page.
        let subsequent_seek_depth = (tree_depth - 1.0).max(params.cache_reuse_factor);
        tree_depth + (input_cardinality - 1.0) * subsequent_seek_depth
    };

    let index_leaf_pages_count = (rows_per_seek / index_info.rows_per_leaf_page).max(1.0);
    let leaf_scan_cost = if !is_full_scan && rows_per_seek <= 1.0 {
        // Point lookup: the leaf page is the last page of the B-tree traversal,
        // already counted in seek_cost.
        0.0
    } else {
        // Full scan or range scan: first iteration cold, subsequent cached.
        cached_io_cost(
            index_leaf_pages_count,
            input_cardinality,
            params.cache_reuse_factor,
        )
    };

    // For non-covering indexes, we need to fetch from the table for each row.
    let table_lookup_cost = if index_info.covering {
        0.0
    } else {
        let table_pages_count = (base_row_count / params.rows_per_table_page).max(1.0);
        let selectivity = rows_per_seek / base_row_count.max(1.0);
        input_cardinality * selectivity * table_pages_count
    };

    let io_cost = seek_cost + leaf_scan_cost + table_lookup_cost;

    // CPU cost: key comparisons during seeks + row processing
    let total_rows = input_cardinality * rows_per_seek;
    let cpu_cost =
        input_cardinality * params.cpu_cost_per_seek + total_rows * params.cpu_cost_per_row;

    Cost((io_cost + cpu_cost - params.index_bonus).max(0.001))
}

pub(crate) fn is_unique_point_lookup(
    index_info: IndexInfo,
    usable_constraint_refs: &[RangeConstraintRef],
) -> bool {
    let eq_count = usable_constraint_refs
        .iter()
        .take_while(|cref| cref.eq.is_some())
        .count();
    index_info.unique && eq_count >= index_info.column_count
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RowCountEstimate {
    HardcodedFallback(f64),
    AnalyzeStats(f64),
}

impl RowCountEstimate {
    /// Create a hardcoded fallback using the given params.
    pub fn hardcoded_fallback(params: &CostModelParams) -> Self {
        RowCountEstimate::HardcodedFallback(params.rows_per_table_fallback)
    }
}

impl std::ops::Deref for RowCountEstimate {
    type Target = f64;

    fn deref(&self) -> &f64 {
        match self {
            RowCountEstimate::HardcodedFallback(val) => val,
            RowCountEstimate::AnalyzeStats(val) => val,
        }
    }
}

/// ANALYZE-based context for cost estimation.
/// Uses sqlite_stat1/stat4 histogram data for row estimates when available,
/// otherwise falls through to heuristic selectivity multipliers.
pub struct AnalyzeCtx<'a> {
    pub rhs_table: &'a JoinedTable,
    pub index: Option<&'a Arc<Index>>,
    pub stats: &'a AnalyzeStats,
    /// The constraint array for this table (needed to extract constant values for stat4).
    pub constraints: Option<&'a [Constraint]>,
    /// The WHERE clause terms (needed to extract constant values for stat4).
    pub where_clause: Option<&'a [WhereTerm]>,
}

/// Result from ANALYZE-based row estimation, carrying the eq prefix length
/// and resolved idx_stats so callers avoid recomputing them.
struct AnalyzeRowEstimate<'a> {
    rows: f64,
    eq_prefix_len: usize,
    idx_stats: &'a stats::IndexStat,
}

pub(crate) fn estimate_rows_per_seek(
    index_info: IndexInfo,
    constraints: &[Constraint],
    usable_constraint_refs: &[RangeConstraintRef],
    base_row_count: RowCountEstimate,
    analyze_ctx: Option<&AnalyzeCtx>,
    params: &CostModelParams,
) -> f64 {
    if is_unique_point_lookup(index_info, usable_constraint_refs) {
        return 1.0;
    }

    if let Some(ctx) = analyze_ctx {
        if !usable_constraint_refs.is_empty() {
            if let Some(est) = estimate_rows_from_analyze_stats(ctx, usable_constraint_refs) {
                // Apply range selectivity for any trailing non-equality constraints
                // beyond the equality prefix.
                let mut range_sels: SmallVec<[f64; 4]> =
                    usable_constraint_refs[est.eq_prefix_len..]
                        .iter()
                        .filter_map(|cref| {
                            range_selectivity_for_cref(
                                cref,
                                constraints,
                                Some(est.idx_stats),
                                ctx.index.map(|arc| arc.as_ref()),
                                Some(ctx),
                                *base_row_count,
                                params,
                            )
                        })
                        .collect();
                let range_selectivity = dampened_product(&mut range_sels);
                return (est.rows * range_selectivity).max(1.0);
            }
        }
    }

    let mut sels: SmallVec<[f64; 4]> = usable_constraint_refs
        .iter()
        .map(|cref| {
            if let Some(ref eq) = cref.eq {
                return constraints[eq.constraint_pos].selectivity;
            }
            range_selectivity_for_cref(
                cref,
                constraints,
                None,
                None,
                None,
                *base_row_count,
                params,
            )
            .unwrap_or(1.0)
        })
        .collect();
    let selectivity_multiplier = dampened_product(&mut sels);
    (selectivity_multiplier * *base_row_count).max(1.0)
}

/// Compute range selectivity for a single constraint ref, trying stat4 first,
/// then stat4-informed CDF combination, then heuristic closed-range selectivity.
fn range_selectivity_for_cref(
    cref: &RangeConstraintRef,
    constraints: &[Constraint],
    idx_stats: Option<&stats::IndexStat>,
    index: Option<&Index>,
    ctx: Option<&AnalyzeCtx>,
    base_row_count: f64,
    params: &CostModelParams,
) -> Option<f64> {
    if let (Some(idx_stats), Some(index), Some(ctx)) = (idx_stats, index, ctx) {
        if let Some(sel) =
            try_stat4_range_for_constraint_ref(idx_stats, index, ctx, cref, constraints)
        {
            return Some(sel);
        }
    }
    let lower = cref.lower_bound.map(|i| constraints[i].selectivity);
    let upper = cref.upper_bound.map(|i| constraints[i].selectivity);
    if let (Some(l), Some(u)) = (lower, upper) {
        let l_idx = cref.lower_bound.expect("must be Some");
        let u_idx = cref.upper_bound.expect("must be Some");
        if constraints[l_idx].stat4_informed && constraints[u_idx].stat4_informed {
            // Additive CDF formula for stat4-informed bounds
            return Some((l + u - 1.0).max(1.0 / base_row_count));
        }
    }
    closed_range_selectivity(lower, upper, params.closed_range_selectivity_factor)
}

/// Estimate rows per seek using ANALYZE stats (sqlite_stat4 then stat1).
/// Returns `None` when no actual stats exist for this index, signaling the
/// caller to fall back to selectivity-based estimation.
fn estimate_rows_from_analyze_stats<'a>(
    ctx: &AnalyzeCtx<'a>,
    constraint_refs: &[RangeConstraintRef],
) -> Option<AnalyzeRowEstimate<'a>> {
    let index = ctx.index?;
    let table_name = ctx.rhs_table.table.get_name();
    let table_stats = ctx.stats.table_stats(table_name)?;
    let idx_stats = table_stats.index_stats.get(&index.name)?;

    let eq_prefix_len = constraint_refs
        .iter()
        .take_while(|cref| cref.eq.is_some())
        .count();

    if eq_prefix_len == 0 {
        // Pure range scan — try stat4 direct range estimation on the first index column.
        if let Some(first_cref) = constraint_refs.first() {
            let constraints = ctx.constraints?;
            if let Some(sel) =
                try_stat4_range_for_constraint_ref(idx_stats, index, ctx, first_cref, constraints)
            {
                if let Some(total) = table_stats.row_count {
                    if total > 0 {
                        return Some(AnalyzeRowEstimate {
                            rows: (sel * total as f64).max(1.0),
                            eq_prefix_len: 0,
                            idx_stats,
                        });
                    }
                }
            }
        }
        return None;
    }

    // Try stat4 for constant equality lookups on ALL leading equality columns.
    // Extract constant values for as many leading equality columns as possible,
    // then do a multi-column binary search over the stat4 samples.
    if !idx_stats.samples.is_empty() && eq_prefix_len >= 1 {
        let mut probe_values: Vec<Value> = Vec::with_capacity(eq_prefix_len);
        let mut collations: Vec<crate::translate::collate::CollationSeq> =
            Vec::with_capacity(eq_prefix_len);

        for (i, cref) in constraint_refs[..eq_prefix_len].iter().enumerate() {
            let eq_ref = cref
                .eq
                .as_ref()
                .expect("must be Some due to eq_prefix range");
            if !eq_ref.is_const {
                break;
            }
            let Some(val) = extract_const_value_from_constraint(ctx, eq_ref.constraint_pos) else {
                break;
            };
            let collation = index
                .columns
                .get(i)
                .and_then(|c| c.collation)
                .unwrap_or_default();
            probe_values.push(val);
            collations.push(collation);
        }

        if !probe_values.is_empty() {
            if let Some(est) =
                stats::stat4_equality_estimate(idx_stats, &probe_values, &collations)
            {
                return Some(AnalyzeRowEstimate {
                    rows: est,
                    eq_prefix_len,
                    idx_stats,
                });
            }
        }
    }

    // Fallback to stat1 average.
    if eq_prefix_len <= idx_stats.avg_rows_per_distinct_prefix.len() {
        let rows = idx_stats.avg_rows_per_distinct_prefix[eq_prefix_len - 1] as f64;
        Some(AnalyzeRowEstimate {
            rows,
            eq_prefix_len,
            idx_stats,
        })
    } else {
        // Stats exist for this index but don't cover the equality prefix length.
        // Return None to fall through to selectivity-based estimation.
        None
    }
}

/// Try stat4 direct range estimation for a single RangeConstraintRef.
/// Extracts constant bound values from constraints and calls stat4_range_selectivity.
fn try_stat4_range_for_constraint_ref(
    idx_stats: &stats::IndexStat,
    index: &Index,
    ctx: &AnalyzeCtx,
    cref: &RangeConstraintRef,
    constraints: &[Constraint],
) -> Option<f64> {
    if idx_stats.samples.is_empty() {
        return None;
    }

    // Only handle range on the first index column for now
    if cref.index_col_pos != 0 {
        return None;
    }

    let collation = index
        .columns
        .first()
        .and_then(|c| c.collation)
        .unwrap_or_default();

    let lower = if let Some(i) = cref.lower_bound {
        let val = extract_const_value_from_constraint(ctx, i)?;
        let is_strict = matches!(
            constraints[i].operator.as_ast_operator(),
            Some(turso_parser::ast::Operator::Greater)
        );
        Some((val, is_strict))
    } else {
        None
    };

    let upper = if let Some(i) = cref.upper_bound {
        let val = extract_const_value_from_constraint(ctx, i)?;
        let is_strict = matches!(
            constraints[i].operator.as_ast_operator(),
            Some(turso_parser::ast::Operator::Less)
        );
        Some((val, is_strict))
    } else {
        None
    };

    if lower.is_none() && upper.is_none() {
        return None;
    }

    stats::stat4_range_selectivity(
        idx_stats,
        lower.as_ref().map(|(v, s)| (v, *s)),
        upper.as_ref().map(|(v, s)| (v, *s)),
        collation,
    )
}

/// Extract a constant literal Value from a constraint's constraining expression.
/// Handles literals, negative numbers, booleans, and parenthesized constant expressions
/// via the shared `eval_constant_default_value` infrastructure.
fn extract_const_value_from_constraint(ctx: &AnalyzeCtx, constraint_pos: usize) -> Option<Value> {
    let constraints = ctx.constraints?;
    let where_clause = ctx.where_clause?;
    let constraint = &constraints[constraint_pos];
    let expr = constraint.get_constraining_expr_ref(where_clause);
    // eval_constant_default_value handles Expr::Id as a bare string (ALTER DEFAULT
    // context), which is not what we want here — column references are not constants.
    if matches!(expr, turso_parser::ast::Expr::Id(_)) {
        return None;
    }
    crate::translate::alter::eval_constant_default_value(expr).ok()
}

/// Estimate the cost of a scan or seek operation.
#[expect(clippy::too_many_arguments)]
pub fn estimate_cost_for_scan_or_seek(
    index_info: Option<IndexInfo>,
    constraints: &[Constraint],
    usable_constraint_refs: &[RangeConstraintRef],
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    is_index_ordered: bool,
    params: &CostModelParams,
    analyze_ctx: Option<&AnalyzeCtx>,
) -> Cost {
    let base_row_count = *base_row_count;

    let tree_depth = if base_row_count <= 1.0 {
        1.0
    } else {
        (base_row_count.ln() / params.rows_per_table_page.ln())
            .ceil()
            .max(1.0)
    };

    let Some(index_info) = index_info else {
        // Full table scan (no index)
        return estimate_scan_cost(base_row_count, input_cardinality, params);
    };

    if is_unique_point_lookup(index_info, usable_constraint_refs) {
        // Unique point lookup: 1 seek per input row, 1 row returned per seek
        return estimate_index_cost(
            base_row_count,
            tree_depth,
            index_info,
            input_cardinality, // num_seeks = outer cardinality
            1.0,               // rows_per_seek = 1 for unique point lookup
            params,
        );
    }

    let rows_per_seek = estimate_rows_per_seek(
        index_info,
        constraints,
        usable_constraint_refs,
        RowCountEstimate::AnalyzeStats(base_row_count),
        analyze_ctx,
        params,
    );

    let base_cost = estimate_index_cost(
        base_row_count,
        tree_depth,
        index_info,
        input_cardinality, // num_seeks = outer cardinality
        rows_per_seek,
        params,
    );

    let is_full_scan = usable_constraint_refs.is_empty();
    // Penalize non-covering indexes doing full scans when not ordered by the index.
    // Without ordering benefit, a full scan on a non-covering index requires random
    // table lookups for each row, which is expensive.
    if !index_info.covering && is_full_scan && !is_index_ordered {
        // Full index scan without ordering benefit - prefer table scan instead
        Cost(base_cost.0 * 2.0)
    } else {
        base_cost
    }
}
