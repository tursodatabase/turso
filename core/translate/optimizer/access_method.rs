use std::sync::Arc;

use turso_sqlite3_parser::ast::SortOrder;

use crate::{
    schema::Index,
    translate::plan::{IterationDirection, JoinOrderMember, JoinedTable},
    Result,
};

use super::{
    constraints::{usable_constraints_for_join_order, ConstraintRef, TableConstraints},
    cost::{estimate_cost_for_scan_or_seek, Cost, IndexInfo},
    order::OrderTarget,
};

#[derive(Debug, Clone)]
/// Represents a way to access a table.
pub struct AccessMethod<'a> {
    /// The estimated number of page fetches.
    /// We are ignoring CPU cost for now.
    pub cost: Cost,
    /// The direction of iteration for the access method.
    /// Typically this is backwards only if it helps satisfy an [OrderTarget].
    pub iter_dir: IterationDirection,
    /// The index that is being used, if any. For rowid based searches (and full table scans), this is None.
    pub index: Option<Arc<Index>>,
    /// The constraint references that are being used, if any.
    /// An empty list of constraint refs means a scan (full table or index);
    /// a non-empty list means a search.
    pub constraint_refs: &'a [ConstraintRef],
}

impl AccessMethod<'_> {
    pub fn is_scan(&self) -> bool {
        self.constraint_refs.is_empty()
    }

    pub fn new_table_scan(input_cardinality: f64, iter_dir: IterationDirection) -> Self {
        Self {
            cost: estimate_cost_for_scan_or_seek(None, &[], &[], input_cardinality),
            iter_dir,
            index: None,
            constraint_refs: &[],
        }
    }
}

/// Return the best [AccessMethod] for a given join order.
pub fn find_best_access_method_for_join_order<'a>(
    rhs_table: &JoinedTable,
    rhs_constraints: &'a TableConstraints,
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
    input_cardinality: f64,
) -> Result<AccessMethod<'a>> {
    let table_no = join_order.last().unwrap().table_id;
    let mut best_access_method =
        AccessMethod::new_table_scan(input_cardinality, IterationDirection::Forwards);
    let rowid_column_idx = rhs_table.columns().iter().position(|c| c.is_rowid_alias);

    // Estimate cost for each candidate index (including the rowid index) and replace best_access_method if the cost is lower.
    for candidate in rhs_constraints.candidates.iter() {
        let index_info = match candidate.index.as_ref() {
            Some(index) => IndexInfo {
                unique: index.unique,
                covering: rhs_table.index_is_covering(index),
                column_count: index.columns.len(),
            },
            None => IndexInfo {
                unique: true, // rowids are always unique
                covering: false,
                column_count: 1,
            },
        };
        let usable_constraint_refs = usable_constraints_for_join_order(
            &rhs_constraints.constraints,
            &candidate.refs,
            join_order,
        );
        let cost = estimate_cost_for_scan_or_seek(
            Some(index_info),
            &rhs_constraints.constraints,
            usable_constraint_refs,
            input_cardinality,
        );

        // All other things being equal, prefer an access method that satisfies the order target.
        let (iter_dir, order_satisfiability_bonus) = if let Some(order_target) = maybe_order_target
        {
            // If the index delivers rows in the same direction (or the exact reverse direction) as the order target, then it
            // satisfies the order target.
            let mut all_same_direction = true;
            let mut all_opposite_direction = true;
            for i in 0..order_target.0.len().min(index_info.column_count) {
                let correct_table = order_target.0[i].table_id == table_no;
                let correct_column = {
                    match &candidate.index {
                        Some(index) => index.columns[i].pos_in_table == order_target.0[i].column_no,
                        None => {
                            rowid_column_idx.is_some_and(|idx| idx == order_target.0[i].column_no)
                        }
                    }
                };
                if !correct_table || !correct_column {
                    all_same_direction = false;
                    all_opposite_direction = false;
                    break;
                }
                let correct_order = {
                    match &candidate.index {
                        Some(index) => order_target.0[i].order == index.columns[i].order,
                        None => order_target.0[i].order == SortOrder::Asc,
                    }
                };
                if correct_order {
                    all_opposite_direction = false;
                } else {
                    all_same_direction = false;
                }
            }
            if all_same_direction || all_opposite_direction {
                (
                    if all_same_direction {
                        IterationDirection::Forwards
                    } else {
                        IterationDirection::Backwards
                    },
                    Cost(1.0),
                )
            } else {
                (IterationDirection::Forwards, Cost(0.0))
            }
        } else {
            (IterationDirection::Forwards, Cost(0.0))
        };
        if cost < best_access_method.cost + order_satisfiability_bonus {
            best_access_method = AccessMethod {
                cost,
                index: candidate.index.clone(),
                iter_dir,
                constraint_refs: usable_constraint_refs,
            };
        }
    }

    Ok(best_access_method)
}
