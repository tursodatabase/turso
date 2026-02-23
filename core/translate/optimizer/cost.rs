use crate::translate::optimizer::constraints::RangeConstraintRef;

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
fn estimate_index_cost(
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

/// Estimate the cost of a scan or seek operation.
pub fn estimate_cost_for_scan_or_seek(
    index_info: Option<IndexInfo>,
    constraints: &[Constraint],
    usable_constraint_refs: &[RangeConstraintRef],
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    is_index_ordered: bool,
    params: &CostModelParams,
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

    // Check if this is a unique index with full equality constraints on all columns.
    // If so, it's guaranteed to return at most 1 row per lookup.
    let is_unique_point_lookup = index_info.unique && {
        // Count how many leading index columns have equality constraints
        let eq_count = usable_constraint_refs
            .iter()
            .take_while(|cref| cref.eq.is_some())
            .count();
        // A unique point lookup requires equality on all index columns
        eq_count >= index_info.column_count
    };

    if is_unique_point_lookup {
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

    // Calculate selectivity to estimate rows returned per seek
    let selectivity_multiplier: f64 = usable_constraint_refs
        .iter()
        .map(|cref| {
            // for equality constraints, use the selectivity of the equality constraint directly
            if let Some(eq) = cref.eq {
                let constraint = &constraints[eq];
                return constraint.selectivity;
            }
            // otherwise, multiply the selectivities of the range constraints
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

    // rows_per_seek: how many rows we expect to get from each index seek
    let rows_per_seek = (selectivity_multiplier * base_row_count).max(1.0);

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

/// Estimate the cost of a multi-index scan (OR-by-union optimization).
///
/// The cost model accounts for:
/// 1. Cost of each index branch scan
/// 2. Deduplication overhead using RowSet
/// 3. Table row fetches after deduplication
/// 4. Potential overlap between branches (rows appearing in multiple disjuncts)
///
/// # Arguments
/// * `branch_costs` - Cost of scanning each individual index branch
/// * `branch_rows` - Estimated rows from each branch before deduplication
/// * `base_row_count` - Total rows in the table
/// * `input_cardinality` - Number of times the multi-index scan is executed (from outer join)
/// * `params` - Cost model parameters
pub fn estimate_multi_index_scan_cost(
    branch_costs: &[Cost],
    branch_rows: &[f64],
    base_row_count: RowCountEstimate,
    input_cardinality: f64,
    params: &CostModelParams,
) -> (Cost, f64) {
    let base_row_count = *base_row_count;

    // Total cost of all branch scans
    let branch_scan_cost: f64 = branch_costs.iter().map(|c| c.0).sum();

    // Estimate total rows before deduplication (sum of all branch estimates)
    let total_rows_before_dedup: f64 = branch_rows.iter().sum();

    // Estimate overlap between branches (rough approximation)
    // For independent branches, P(A OR B) = P(A) + P(B) - P(A AND B)
    // We approximate P(A AND B) = P(A) * P(B) for independence
    let mut unique_row_ratio = 1.0f64;
    for rows in branch_rows.iter() {
        let branch_selectivity = (*rows / base_row_count).min(1.0);
        unique_row_ratio *= 1.0 - branch_selectivity;
    }
    let estimated_unique_rows = base_row_count * (1.0 - unique_row_ratio);

    // Deduplication cost: adding and checking rowids in the RowSet
    // RowSet operations are O(log n) for balanced tree implementation
    let rowset_ops_cost = total_rows_before_dedup * params.cpu_cost_per_row * 2.0; // add + test

    // Table fetch cost: use same formula as single-index table lookup for consistency
    // This models the expected number of pages to fetch, assuming some locality benefit
    // from sorted rowid access after RowSet deduplication
    let table_pages = (base_row_count / params.rows_per_page).max(1.0);
    let selectivity = estimated_unique_rows / base_row_count.max(1.0);
    let table_fetch_cost = selectivity * table_pages;

    // Total cost
    let total_cost = (branch_scan_cost + rowset_ops_cost + table_fetch_cost) * input_cardinality;

    (Cost(total_cost), estimated_unique_rows)
}

/// Estimate the cost of a multi-index intersection (AND-by-intersection optimization).
///
/// The cost model accounts for:
/// 1. Cost of each index branch scan
/// 2. Intersection overhead using RowSet test operations
/// 3. Table row fetches for intersection result
/// 4. Final row estimate is the product of selectivities (assuming independence)
///
/// Intersection works differently from union:
/// - First branch populates the RowSet
/// - Subsequent branches test against and reduce the set
/// - Final result is only rowids that appear in ALL branches
///
/// # Arguments
/// * `branch_costs` - Cost of scanning each individual index branch
/// * `branch_rows` - Estimated rows from each branch
/// * `base_row_count` - Total rows in the table
/// * `input_cardinality` - Number of times the multi-index scan is executed (from outer join)
/// * `params` - Cost model parameters
///
/// # Returns
/// A tuple of (Cost, estimated_result_rows)
pub fn estimate_multi_index_intersection_cost(
    branch_costs: &[Cost],
    branch_rows: &[f64],
    base_row_count: RowCountEstimate,
    input_cardinality: f64,
    params: &CostModelParams,
) -> (Cost, f64) {
    let base_row_count = *base_row_count;

    // Total cost of all branch scans
    let branch_scan_cost: f64 = branch_costs.iter().map(|c| c.0).sum();

    // Estimate intersection result (product of selectivities, assuming independence)
    // P(A AND B) = P(A) * P(B) for independent predicates
    let mut intersection_selectivity = 1.0f64;
    for rows in branch_rows.iter() {
        let branch_selectivity = (*rows / base_row_count).min(1.0);
        intersection_selectivity *= branch_selectivity;
    }
    let estimated_intersection_rows = (base_row_count * intersection_selectivity).max(1.0);

    // Intersection cost: first branch adds, subsequent branches test
    // First branch: rows[0] inserts
    // Subsequent branches: rows[i] tests
    let first_branch_rows = branch_rows.first().copied().unwrap_or(0.0);
    let subsequent_branch_rows: f64 = branch_rows.iter().skip(1).sum();
    let rowset_ops_cost =
        (first_branch_rows + subsequent_branch_rows) * params.cpu_cost_per_row * 1.5; // add is cheaper than test+add

    // Table fetch cost: use same formula as single-index table lookup for consistency
    // This models the expected number of pages to fetch, assuming some locality benefit
    // from sorted rowid access after RowSet intersection
    let table_pages = (base_row_count / params.rows_per_page).max(1.0);
    let selectivity = estimated_intersection_rows / base_row_count.max(1.0);
    let table_fetch_cost = selectivity * table_pages;

    // Total cost
    let total_cost = (branch_scan_cost + rowset_ops_cost + table_fetch_cost) * input_cardinality;

    (Cost(total_cost), estimated_intersection_rows)
}
