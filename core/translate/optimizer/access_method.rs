use std::sync::Arc;

use turso_ext::{ConstraintInfo, ConstraintUsage, ResultCode};
use turso_parser::ast::SortOrder;

use crate::translate::optimizer::constraints::{
    convert_to_vtab_constraint, Constraint, RangeConstraintRef,
};
use crate::{
    schema::{Index, Table},
    translate::plan::{IterationDirection, JoinOrderMember, JoinedTable},
    vtab::VirtualTable,
    LimboError, Result,
};

use super::{
    constraints::{usable_constraints_for_join_order, TableConstraints},
    cost::{estimate_cost_for_scan_or_seek, Cost, IndexInfo},
    order::OrderTarget,
};

#[derive(Debug, Clone)]
/// Represents a way to access a table.
pub struct AccessMethod {
    /// The estimated number of page fetches.
    /// We are ignoring CPU cost for now.
    pub cost: Cost,
    /// Table-type specific access method details.
    pub params: AccessMethodParams,
}

/// Table‑specific details of how an [`AccessMethod`] operates.
#[derive(Debug, Clone)]
pub enum AccessMethodParams {
    BTreeTable {
        /// The direction of iteration for the access method.
        /// Typically this is backwards only if it helps satisfy an [OrderTarget].
        iter_dir: IterationDirection,
        /// The index that is being used, if any. For rowid based searches (and full table scans), this is None.
        index: Option<Arc<Index>>,
        /// The constraint references that are being used, if any.
        /// An empty list of constraint refs means a scan (full table or index);
        /// a non-empty list means a search.
        constraint_refs: Vec<RangeConstraintRef>,
    },
    VirtualTable {
        /// Index identifier returned by the table's `best_index` method.
        idx_num: i32,
        /// Optional index string returned by the table's `best_index` method.
        idx_str: Option<String>,
        /// Constraint descriptors passed to the virtual table’s `filter` method.
        /// Each corresponds to a column/operator pair from the WHERE clause.
        constraints: Vec<ConstraintInfo>,
        /// Information returned by the virtual table's `best_index` method
        /// describing how each constraint will be used.
        constraint_usages: Vec<ConstraintUsage>,
    },
    Subquery,
}

/// Return the best [AccessMethod] for a given join order.
pub fn find_best_access_method_for_join_order(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
    input_cardinality: f64,
) -> Result<Option<AccessMethod>> {
    match &rhs_table.table {
        Table::BTree(_) => find_best_access_method_for_btree(
            rhs_table,
            rhs_constraints,
            join_order,
            maybe_order_target,
            input_cardinality,
        ),
        Table::Virtual(vtab) => find_best_access_method_for_vtab(
            vtab,
            &rhs_constraints.constraints,
            join_order,
            input_cardinality,
        ),
        Table::FromClauseSubquery(_) => Ok(Some(AccessMethod {
            cost: estimate_cost_for_scan_or_seek(None, &[], &[], input_cardinality),
            params: AccessMethodParams::Subquery,
        })),
    }
}

fn find_best_access_method_for_btree(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
    input_cardinality: f64,
) -> Result<Option<AccessMethod>> {
    let table_no = join_order.last().unwrap().table_id;
    let mut best_cost = estimate_cost_for_scan_or_seek(None, &[], &[], input_cardinality);
    let mut best_params = AccessMethodParams::BTreeTable {
        iter_dir: IterationDirection::Forwards,
        index: None,
        constraint_refs: vec![],
    };
    let rowid_column_idx = rhs_table.columns().iter().position(|c| c.is_rowid_alias());

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
            &usable_constraint_refs,
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
        if cost < best_cost + order_satisfiability_bonus {
            best_cost = cost;
            best_params = AccessMethodParams::BTreeTable {
                iter_dir,
                index: candidate.index.clone(),
                constraint_refs: usable_constraint_refs,
            };
        }
    }

    Ok(Some(AccessMethod {
        cost: best_cost,
        params: best_params,
    }))
}

fn find_best_access_method_for_vtab(
    vtab: &VirtualTable,
    constraints: &[Constraint],
    join_order: &[JoinOrderMember],
    input_cardinality: f64,
) -> Result<Option<AccessMethod>> {
    let vtab_constraints = convert_to_vtab_constraint(constraints, join_order);

    // TODO: get proper order_by information to pass to the vtab.
    // maybe encode more info on t_ctx? we need: [col_idx , is_descending]
    let best_index_result = vtab.best_index(&vtab_constraints, &[]);

    match best_index_result {
        Ok(index_info) => {
            Ok(Some(AccessMethod {
                // TODO: Base cost on `IndexInfo::estimated_cost` and output cardinality on `IndexInfo::estimated_rows`
                cost: estimate_cost_for_scan_or_seek(None, &[], &[], input_cardinality),
                params: AccessMethodParams::VirtualTable {
                    idx_num: index_info.idx_num,
                    idx_str: index_info.idx_str,
                    constraints: vtab_constraints,
                    constraint_usages: index_info.constraint_usages,
                },
            }))
        }
        Err(ResultCode::ConstraintViolation) => Ok(None),
        Err(e) => Err(LimboError::from(e)),
    }
}
