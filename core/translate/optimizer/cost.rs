use crate::translate::optimizer::constraints::RangeConstraintRef;

use super::constraints::Constraint;

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

pub const ESTIMATED_HARDCODED_ROWS_PER_TABLE: usize = 1000000;
pub const ESTIMATED_HARDCODED_ROWS_PER_PAGE: usize = 50; // roughly 80 bytes per 4096 byte page

/// Estimate IO and CPU cost for a full table scan.
///
/// # Arguments
/// * `base_row_count` - Total rows in the table
/// * `num_scans` - Number of times we scan the table (e.g., from outer loop in nested loop join)
fn estimate_scan_cost(base_row_count: f64, num_scans: f64) -> Cost {
    let table_pages = (base_row_count / ESTIMATED_HARDCODED_ROWS_PER_PAGE as f64).max(1.0);

    // First scan reads all pages; subsequent scans benefit from caching
    let cache_reuse_factor = 0.2;
    let io_cost = if num_scans <= 1.0 {
        table_pages
    } else {
        // First scan + discounted cost for subsequent scans
        table_pages + (num_scans - 1.0) * table_pages * cache_reuse_factor
    };

    // CPU cost for processing all rows on each scan
    let cpu_cost = num_scans * base_row_count * 0.001;

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
fn estimate_index_cost(
    base_row_count: f64,
    tree_depth: f64,
    index_info: IndexInfo,
    num_seeks: f64,
    rows_per_seek: f64,
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
    // Use a 2x density factor for covering indexes.
    let rows_per_page = if index_info.covering {
        ESTIMATED_HARDCODED_ROWS_PER_PAGE as f64 * 2.0
    } else {
        ESTIMATED_HARDCODED_ROWS_PER_PAGE as f64
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
        let table_pages = (base_row_count / ESTIMATED_HARDCODED_ROWS_PER_PAGE as f64).max(1.0);
        let selectivity = rows_per_seek / base_row_count.max(1.0);
        num_seeks * selectivity * table_pages
    };

    let io_cost = seek_cost + leaf_scan_cost + table_lookup_cost;

    // CPU cost: key comparisons during seeks + row processing
    let total_rows = num_seeks * rows_per_seek;
    let cpu_cost = num_seeks * 0.01 + total_rows * 0.001;

    // Small bonus for using an existing index
    let index_bonus = 0.5;

    Cost((io_cost + cpu_cost - index_bonus).max(0.001))
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RowCountEstimate {
    HardcodedFallback(f64),
    AnalyzeStats(f64),
}

impl Default for RowCountEstimate {
    fn default() -> Self {
        RowCountEstimate::HardcodedFallback(ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64)
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
) -> Cost {
    let base_row_count = *base_row_count;

    let tree_depth = if base_row_count <= 1.0 {
        1.0
    } else {
        (base_row_count.ln() / (ESTIMATED_HARDCODED_ROWS_PER_PAGE as f64).ln())
            .ceil()
            .max(1.0)
    };

    let Some(index_info) = index_info else {
        // Full table scan (no index)
        return estimate_scan_cost(base_row_count, input_cardinality);
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
