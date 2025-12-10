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

pub fn estimate_page_io_cost(rowcount: f64) -> Cost {
    Cost((rowcount / ESTIMATED_HARDCODED_ROWS_PER_PAGE as f64).ceil())
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RowCountEstimate {
    HardcodedFallback(f64),
    AnalyzeStats(f64),
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
) -> Cost {
    let has_real_stats = matches!(base_row_count, RowCountEstimate::AnalyzeStats(_));
    let base_row_count = *base_row_count;

    let Some(index_info) = index_info else {
        // Full table scan
        if has_real_stats {
            // With real stats, account for caching on small tables
            let table_pages = (base_row_count / ESTIMATED_HARDCODED_ROWS_PER_PAGE as f64).max(1.0);

            // Without caching: input_cardinality * base_row_count rows = many page reads
            let uncached_io = estimate_page_io_cost(input_cardinality * base_row_count);

            // With caching: pages get cached after first scan
            let cached_io = Cost(table_pages * 2.0);

            // CPU cost for processing rows: key differentiator for scans
            let cpu_cost = input_cardinality * base_row_count * 0.001;
            let io_cost = uncached_io.0.min(cached_io.0);
            return Cost(io_cost + cpu_cost);
        } else {
            // Without real stats, use simple IO-based model
            return estimate_page_io_cost(input_cardinality * base_row_count);
        }
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
        // Unique point lookup: at most 1 row returned per input row
        // Tree depth is log(rows) / log(fanout)
        let tree_depth = if base_row_count <= 1.0 {
            1.0
        } else {
            (base_row_count.ln() / (ESTIMATED_HARDCODED_ROWS_PER_PAGE as f64).ln())
                .ceil()
                .max(1.0)
        };

        // Cost per lookup: tree traversal + optional table lookup for non-covering
        let pages_per_lookup = tree_depth + if index_info.covering { 0.0 } else { 1.0 };

        if has_real_stats {
            // With real stats, account for page caching on small tables
            let table_pages = (base_row_count / ESTIMATED_HARDCODED_ROWS_PER_PAGE as f64).max(1.0);

            // For small tables that fit in cache, the total IO is bounded by:
            // - All unique pages touched (at most table_pages for covering, 2*table_pages otherwise)
            // - Plus a small per-lookup CPU cost
            //
            // With caching, repeated lookups into the same small table
            // are very cheap after the first few reads. So the cost is dominated by
            // the number of distinct pages, not the number of lookups.
            let distinct_pages = table_pages * (if index_info.covering { 1.0 } else { 2.0 });

            // Still add a small per-lookup cost to differentiate from scan due to overhead for key comparison
            let lookup_overhead = input_cardinality * 0.001;
            let io_cost = distinct_pages + lookup_overhead;
            let cpu_cost = input_cardinality * 0.001;

            // Give a bonus for using direct lookup vs. scan.
            // This accounts for the fact that scans in join contexts often trigger
            // auto-creation of ephemeral indexes, which is more expensive than
            // using an existing index directly.
            let direct_lookup_bonus = 0.5;

            return Cost((io_cost + cpu_cost - direct_lookup_bonus).max(0.001));
        } else {
            // Without real stats, use simple per-lookup cost
            return Cost(input_cardinality * pages_per_lookup);
        }
    }

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

    // little cheeky bonus for covering indexes
    let covering_multiplier = if index_info.covering { 0.9 } else { 1.0 };
    estimate_page_io_cost(
        selectivity_multiplier * base_row_count * input_cardinality * covering_multiplier,
    )
}
