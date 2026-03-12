use crate::schema::Index;
use crate::stats::AnalyzeStats;
use crate::sync::Arc;
use crate::translate::optimizer::constraints::RangeConstraintRef;
use crate::translate::plan::JoinedTable;

use super::constraints::Constraint;
use super::cost_params::CostModelParams;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexInfo {
    pub unique: bool,
    pub column_count: usize,
    pub covering: bool,
}

/// Estimate IO and CPU cost for a full table scan.
///
/// # Arguments
/// * `base_row_count` - Total rows in the table
/// * `num_scans` - Number of times we scan the table (e.g., from outer loop in nested loop join)
/// * `params` - Cost model parameters
fn estimate_scan_cost(base_row_count: f64, num_scans: f64, params: &CostModelParams) -> Cost {
    let table_pages = (base_row_count / params.rows_per_page).max(1.0);

    // First scan reads all pages; subsequent scans benefit from caching
    let io_cost = if num_scans <= 1.0 {
        table_pages
    } else {
        // First scan + discounted cost for subsequent scans
        table_pages + (num_scans - 1.0) * table_pages * params.cache_reuse_factor
    };

    // CPU cost for processing all rows on each scan
    let cpu_cost = num_scans * base_row_count * params.cpu_cost_per_row;

    Cost(io_cost + cpu_cost)
}

/// Estimate IO and CPU cost for index-based access.
///
/// This properly separates the number of B-tree seeks from the number of rows
/// returned per seek. A range scan does ONE seek followed by sequential leaf
/// page reads, not one seek per row.
///
/// # Arguments
/// * `base_row_count` - Total rows in the table (for estimating tree depth and page counts)
/// * `tree_depth` - B-tree depth (number of pages to traverse per seek)
/// * `index_info` - Index properties (covering, unique, etc.)
/// * `num_seeks` - Number of B-tree traversals (typically = outer cardinality for joins)
/// * `rows_per_seek` - Expected rows returned per seek (1 for point lookup, more for range)
/// * `params` - Cost model parameters
pub fn estimate_index_cost(
    base_row_count: f64,
    tree_depth: f64,
    index_info: IndexInfo,
    num_seeks: f64,
    rows_per_seek: f64,
    params: &CostModelParams,
) -> Cost {
    // Detect full index scan: when rows_per_seek equals base_row_count, we're scanning
    // the entire index, not seeking to specific positions.
    let is_full_scan = (rows_per_seek - base_row_count).abs() < 1.0;

    // Cost of B-tree traversals: each seek traverses tree_depth pages.
    // For a full scan, we only do one initial seek to the start of the index.
    let seek_cost = if is_full_scan {
        tree_depth // Single seek to start
    } else {
        num_seeks * tree_depth
    };

    // Cost of reading leaf pages after seeking.
    // For covering indexes, entries are smaller (only indexed columns), so more rows fit per page.
    let rows_per_page = if index_info.covering {
        params.rows_per_page * params.covering_index_density
    } else {
        params.rows_per_page
    };
    let leaf_pages = (rows_per_seek / rows_per_page).max(1.0);
    let leaf_scan_cost = if is_full_scan {
        leaf_pages // Sequential scan of all leaf pages
    } else {
        num_seeks * leaf_pages
    };

    // For non-covering indexes, we need to fetch from the table for each row.
    let table_lookup_cost = if index_info.covering {
        0.0
    } else {
        let table_pages = (base_row_count / params.rows_per_page).max(1.0);
        let selectivity = rows_per_seek / base_row_count.max(1.0);
        num_seeks * selectivity * table_pages
    };

    let io_cost = seek_cost + leaf_scan_cost + table_lookup_cost;

    // CPU cost: key comparisons during seeks + row processing
    let total_rows = num_seeks * rows_per_seek;
    let cpu_cost = num_seeks * params.cpu_cost_per_seek + total_rows * params.cpu_cost_per_row;

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

/// Optional ANALYZE-based context for cost estimation.
/// When available, uses sqlite_stat1 histogram data for row estimates
/// instead of heuristic selectivity multipliers.
pub struct AnalyzeCtx<'a> {
    pub rhs_table: &'a JoinedTable,
    pub index: Option<&'a Arc<Index>>,
    pub stats: &'a AnalyzeStats,
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

    // When ANALYZE stats are available and we have constraints, use histogram-based estimation.
    if let Some(ctx) = analyze_ctx {
        if !usable_constraint_refs.is_empty() {
            return estimate_rows_from_analyze_stats(ctx, usable_constraint_refs, params);
        }
    }

    let selectivity_multiplier: f64 = usable_constraint_refs
        .iter()
        .map(|cref| {
            if let Some(ref eq) = cref.eq {
                return constraints[eq.constraint_pos].selectivity;
            }
            let mut selectivity = 1.0;
            if let Some(lower_bound) = cref.lower_bound {
                selectivity *= constraints[lower_bound].selectivity;
            }
            if let Some(upper_bound) = cref.upper_bound {
                selectivity *= constraints[upper_bound].selectivity;
            }
            selectivity
        })
        .product();

    (selectivity_multiplier * *base_row_count).max(1.0)
}

/// Estimate rows per seek using ANALYZE stats (sqlite_stat1 histogram data).
fn estimate_rows_from_analyze_stats(
    ctx: &AnalyzeCtx,
    constraint_refs: &[RangeConstraintRef],
    params: &CostModelParams,
) -> f64 {
    let Some(index) = ctx.index else {
        return params.fanout_index_seek_unique;
    };

    let matched_cols = constraint_refs.len();
    let total_cols = index.columns.len();
    let unmatched = total_cols.saturating_sub(matched_cols);
    let table_name = ctx.rhs_table.table.get_name();

    if let Some(fanout) = ctx
        .stats
        .table_stats(table_name)
        .and_then(|ts| ts.index_stats.get(&index.name))
        .and_then(|stats| {
            if matched_cols > 0 && matched_cols <= stats.avg_rows_per_distinct_prefix.len() {
                Some(stats.avg_rows_per_distinct_prefix[matched_cols - 1] as f64)
            } else {
                None
            }
        })
    {
        return fanout;
    }

    if matched_cols >= total_cols {
        if index.unique {
            params.fanout_index_seek_unique
        } else {
            params.fanout_index_seek_non_unique
        }
    } else {
        params
            .fanout_index_seek_per_unmatched_column
            .powi(unmatched as i32)
    }
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
        (base_row_count.ln() / params.rows_per_page.ln())
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
