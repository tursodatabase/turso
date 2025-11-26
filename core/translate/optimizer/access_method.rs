use std::sync::Arc;

use turso_ext::{ConstraintInfo, ConstraintUsage, ResultCode};
use turso_parser::ast::{self, SortOrder, TableInternalId};

use crate::translate::expr::as_binary_components;
use crate::translate::optimizer::constraints::{
    convert_to_vtab_constraint, Constraint, RangeConstraintRef,
};
use crate::translate::plan::{NonFromClauseSubquery, SubqueryState, WhereTerm};
use crate::util::exprs_are_equivalent;
use crate::vdbe::hash_table::DEFAULT_MEM_BUDGET;
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
use crate::translate::optimizer::order::ColumnTarget;

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
    HashJoin {
        /// The table to build the hash table from.
        build_table_idx: usize,
        /// The table to probe the hash table with.
        probe_table_idx: usize,
        /// Column indices for the join keys (from both tables).
        /// These are the columns that will be used to build and probe the hash table.
        join_key_columns: Vec<(usize, usize)>, // (build_col, probe_col)
        /// Memory budget for the hash table in bytes.
        mem_budget: usize,
        /// WHERE clause indices that should be marked as consumed if this hash join is selected.
        where_clause_indices: Vec<usize>,
    },
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
                let target = &order_target.0[i];
                let correct_table = target.table_id == table_no;
                let correct_column = match (&target.target, &candidate.index) {
                    // Regular column target on normal index, simply check column number
                    // against index column's pos_in_table
                    (ColumnTarget::Column(col_no), Some(index)) => {
                        index.columns[i].expr.is_none() && index.columns[i].pos_in_table == *col_no
                    }
                    // Expression column target on expression index, check expr equivalence
                    (ColumnTarget::Expr(expr), Some(index)) => index.columns[i]
                        .expr
                        .as_ref()
                        .is_some_and(|e| exprs_are_equivalent(e, unsafe { &**expr })),
                    // Normal column target on rowid index, check if column is rowid
                    (ColumnTarget::Column(col_no), None) => {
                        rowid_column_idx.is_some_and(|idx| idx == *col_no)
                    }
                    _ => false,
                };
                if !correct_table || !correct_column {
                    all_same_direction = false;
                    all_opposite_direction = false;
                    break;
                }
                let correct_order = {
                    match &candidate.index {
                        Some(index) => target.order == index.columns[i].order,
                        None => target.order == SortOrder::Asc,
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

/// Detect equi-join conditions between two tables that could be used for a hash join.
/// Returns a list of (build_col, probe_col, where_idx) tuples if equi-join conditions are found.
///
/// This function analyzes the WHERE clause constraints to find equality conditions
/// that join columns from the build table with columns from the probe table.
/// The caller is responsible for marking WHERE terms as consumed if hash join is selected.
pub fn find_equijoin_conditions(
    build_table_id: TableInternalId,
    probe_table_id: TableInternalId,
    constraints: &[Constraint],
    where_clause: &[WhereTerm],
) -> Vec<(usize, usize, usize)> {
    let mut equijoin_cols = Vec::new();

    for constraint in constraints {
        // Check if this is an equality constraint
        if !matches!(constraint.operator, ast::Operator::Equals) {
            continue;
        }

        // Get the original WHERE term
        let (where_idx, _side) = constraint.where_clause_pos;
        let where_term = &where_clause[where_idx];

        // Skip already consumed terms
        if where_term.consumed {
            continue;
        }
        let Ok(Some((lhs, ast::Operator::Equals, rhs))) = as_binary_components(&where_term.expr)
        else {
            continue;
        };

        // Extract column information from both sides
        let lhs_col = match lhs {
            ast::Expr::Column { table, column, .. } => Some((*table, *column)),
            ast::Expr::RowId { table, .. } => Some((*table, 0)),
            _ => None,
        };

        let rhs_col = match rhs {
            ast::Expr::Column { table, column, .. } => Some((*table, *column)),
            _ => None,
        };

        // Check if we have a column from each table
        if let (Some((lhs_table, lhs_column)), Some((rhs_table, rhs_column))) = (lhs_col, rhs_col) {
            // Check if one column is from build table and one from probe table
            if lhs_table == build_table_id && rhs_table == probe_table_id {
                equijoin_cols.push((lhs_column, rhs_column, where_idx));
            } else if lhs_table == probe_table_id && rhs_table == build_table_id {
                equijoin_cols.push((rhs_column, lhs_column, where_idx));
            }
        }
    }

    equijoin_cols
}

/// Estimate the cost of a hash join between two tables.
/// TODO(preston): improve cost model when we have more metadata/factors to consider, like table
/// size and spilling implemented for hash tables.
pub fn estimate_hash_join_cost(
    build_cardinality: f64,
    probe_cardinality: f64,
    mem_budget: usize,
) -> Cost {
    const CPU_HASH_COST: f64 = 0.001;
    const CPU_INSERT_COST: f64 = 0.002;
    const CPU_LOOKUP_COST: f64 = 0.003;
    const IO_COST: f64 = 1.0;
    const BYTES_PER_ROW_ESTIMATE: usize = 100;

    // Estimate if the hash table will fit in memory
    let estimated_hash_table_size = (build_cardinality as usize) * BYTES_PER_ROW_ESTIMATE;
    let will_spill = estimated_hash_table_size > mem_budget;

    // hash and insert all rows from build table
    let build_cost = build_cardinality * (CPU_HASH_COST + CPU_INSERT_COST);
    let probe_cost = probe_cardinality * (CPU_HASH_COST + CPU_LOOKUP_COST);
    let spill_cost = if will_spill {
        (build_cardinality + probe_cardinality) * IO_COST * 2.0
    } else {
        0.0
    };
    Cost(build_cost + probe_cost + spill_cost)
}

/// Try to create a hash join access method for joining two tables.
/// Returns Some(AccessMethod) if hash join is viable, None otherwise.
#[allow(clippy::too_many_arguments)]
pub fn try_hash_join_access_method(
    build_table: &JoinedTable,
    probe_table: &JoinedTable,
    build_table_idx: usize,
    probe_table_idx: usize,
    probe_constraints: &TableConstraints,
    where_clause: &mut [WhereTerm],
    build_cardinality: f64,
    probe_cardinality: f64,
    subqueries: &[NonFromClauseSubquery],
) -> Option<AccessMethod> {
    // Only works for B-tree tables
    if !matches!(build_table.table, Table::BTree(_))
        || !matches!(probe_table.table, Table::BTree(_))
    {
        return None;
    }

    // Avoid hash join on self-joins over the same underlying table. The current
    // implementation assumes distinct build/probe sources; sharing storage can
    // lead to incorrect matches.
    if build_table
        .table
        .btree()
        .and_then(|b| {
            probe_table
                .table
                .btree()
                .map(|p| b.root_page == p.root_page)
        })
        .unwrap_or(false)
    {
        return None;
    }

    // Hash joins only support INNER JOIN semantics.
    // Don't use hash joins for any form of OUTER JOINs
    if build_table.join_info.as_ref().is_some_and(|ji| ji.outer)
        || probe_table.join_info.as_ref().is_some_and(|ji| ji.outer)
    {
        return None;
    }

    // USING/NATURAL joins carry implicit equality constraints that aren't represented
    // the same way as WHERE-derived predicates; skip hash join for now to avoid
    // hashing on the wrong keys.
    if build_table
        .join_info
        .as_ref()
        .is_some_and(|ji| !ji.using.is_empty())
        || probe_table
            .join_info
            .as_ref()
            .is_some_and(|ji| !ji.using.is_empty())
    {
        return None;
    }

    // Avoid hash joins when there are correlated subqueries that reference the joined tables.
    // Executing Gosub during the probe phase while reading from the build table cursor
    // can trigger hash table operations in invalid states.
    for subquery in subqueries {
        if !subquery.correlated {
            continue;
        }
        // Check if the subquery references the build or probe table
        if let SubqueryState::Unevaluated { plan } = &subquery.state {
            if let Some(plan) = plan.as_ref() {
                let outer_refs = plan.table_references.outer_query_refs();
                for outer_ref in outer_refs {
                    if outer_ref.internal_id == build_table.internal_id
                        || outer_ref.internal_id == probe_table.internal_id
                    {
                        return None;
                    }
                }
            }
        }
    }

    // Find equi-join conditions
    let equijoin_cols_with_indices = find_equijoin_conditions(
        build_table.internal_id,
        probe_table.internal_id,
        &probe_constraints.constraints,
        where_clause,
    );

    // Need at least one equi-join condition
    if equijoin_cols_with_indices.is_empty() {
        return None;
    }

    // Extract join key columns and WHERE clause indices
    let mut join_key_columns = Vec::new();
    let mut where_clause_indices = Vec::new();
    for (build_col, probe_col, where_idx) in equijoin_cols_with_indices {
        join_key_columns.push((build_col, probe_col));
        where_clause_indices.push(where_idx);
    }

    let cost = estimate_hash_join_cost(build_cardinality, probe_cardinality, DEFAULT_MEM_BUDGET);
    Some(AccessMethod {
        cost,
        params: AccessMethodParams::HashJoin {
            build_table_idx,
            probe_table_idx,
            join_key_columns,
            mem_budget: DEFAULT_MEM_BUDGET,
            where_clause_indices,
        },
    })
}
