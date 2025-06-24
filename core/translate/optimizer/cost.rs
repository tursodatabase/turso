use super::constraints::{Constraint, ConstraintRef};

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

/// Estimate the cost of a scan or seek operation.
///
/// This is a very simple model that estimates the number of pages read
/// based on the number of rows read, ignoring any CPU costs.
pub fn estimate_cost_for_scan_or_seek(
    index_info: Option<IndexInfo>,
    constraints: &[Constraint],
    usable_constraint_refs: &[ConstraintRef],
    input_cardinality: f64,
) -> Cost {
    let Some(index_info) = index_info else {
        return estimate_page_io_cost(
            input_cardinality * ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64,
        );
    };

    let selectivity_multiplier: f64 = usable_constraint_refs
        .iter()
        .map(|cref| {
            let constraint = &constraints[cref.constraint_vec_pos];
            constraint.selectivity
        })
        .product();

    // little cheeky bonus for covering indexes
    let covering_multiplier = if index_info.covering { 0.9 } else { 1.0 };

    estimate_page_io_cost(
        selectivity_multiplier
            * ESTIMATED_HARDCODED_ROWS_PER_TABLE as f64
            * input_cardinality
            * covering_multiplier,
    )
}
