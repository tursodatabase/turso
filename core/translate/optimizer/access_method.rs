use std::sync::Arc;

use turso_ext::{ConstraintInfo, ConstraintUsage, ResultCode};
use turso_parser::ast::{self, SortOrder, TableInternalId};

use crate::translate::expr::{as_binary_components, walk_expr, WalkControl};
use crate::translate::optimizer::constraints::{
    convert_to_vtab_constraint, BinaryExprSide, Constraint, RangeConstraintRef,
};
use crate::translate::optimizer::cost::{RowCountEstimate, ESTIMATED_HARDCODED_ROWS_PER_PAGE};
use crate::translate::plan::{HashJoinKey, NonFromClauseSubquery, SubqueryState, WhereTerm};
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
        /// Join key references - each entry contains the where_clause index and which side
        /// of the equality belongs to the build table. Supports expression-based join keys.
        join_keys: Vec<HashJoinKey>,
        /// Memory budget for the hash table in bytes.
        mem_budget: usize,
    },
}

/// Return the best [AccessMethod] for a given join order.
pub fn find_best_access_method_for_join_order(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
) -> Result<Option<AccessMethod>> {
    match &rhs_table.table {
        Table::BTree(_) => find_best_access_method_for_btree(
            rhs_table,
            rhs_constraints,
            join_order,
            maybe_order_target,
            input_cardinality,
            base_row_count,
        ),
        Table::Virtual(vtab) => find_best_access_method_for_vtab(
            vtab,
            &rhs_constraints.constraints,
            join_order,
            input_cardinality,
            base_row_count,
        ),
        Table::FromClauseSubquery(_) => Ok(Some(AccessMethod {
            cost: estimate_cost_for_scan_or_seek(None, &[], &[], input_cardinality, base_row_count),
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
    base_row_count: RowCountEstimate,
) -> Result<Option<AccessMethod>> {
    let table_no = join_order.last().unwrap().table_id;
    let mut best_cost =
        estimate_cost_for_scan_or_seek(None, &[], &[], input_cardinality, base_row_count);
    let mut best_params = AccessMethodParams::BTreeTable {
        iter_dir: IterationDirection::Forwards,
        index: None,
        constraint_refs: vec![],
    };
    let rowid_column_idx = rhs_table.columns().iter().position(|c| c.is_rowid_alias());

    // Estimate cost for each candidate index (including the rowid index) and replace best_access_method if the cost is lower.
    for candidate in rhs_constraints.candidates.iter() {
        let usable_constraint_refs = usable_constraints_for_join_order(
            &rhs_constraints.constraints,
            &candidate.refs,
            join_order,
        );

        let index_info = match candidate.index.as_ref() {
            Some(index) => IndexInfo {
                unique: index.unique,
                covering: rhs_table.index_is_covering(index),
                column_count: index.columns.len(),
            },
            None => IndexInfo {
                unique: true, // rowids are always unique
                // Only treat rowid as covering when there are usable constraints/rowid seek
                covering: !usable_constraint_refs.is_empty(),
                column_count: 1,
            },
        };
        let cost = estimate_cost_for_scan_or_seek(
            Some(index_info),
            &rhs_constraints.constraints,
            &usable_constraint_refs,
            input_cardinality,
            base_row_count,
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
    base_row_count: RowCountEstimate,
) -> Result<Option<AccessMethod>> {
    let vtab_constraints = convert_to_vtab_constraint(constraints, join_order);

    // TODO: get proper order_by information to pass to the vtab.
    // maybe encode more info on t_ctx? we need: [col_idx , is_descending]
    let best_index_result = vtab.best_index(&vtab_constraints, &[]);

    match best_index_result {
        Ok(index_info) => {
            Ok(Some(AccessMethod {
                // TODO: Base cost on `IndexInfo::estimated_cost` and output cardinality on `IndexInfo::estimated_rows`
                cost: estimate_cost_for_scan_or_seek(
                    None,
                    &[],
                    &[],
                    input_cardinality,
                    base_row_count,
                ),
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

/// Collect all table IDs referenced in an expression.
fn collect_table_refs(expr: &ast::Expr) -> Option<Vec<TableInternalId>> {
    let mut tables = Vec::new();
    let result = walk_expr(expr, &mut |e| {
        match e {
            ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } => {
                if !tables.contains(table) {
                    tables.push(*table);
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
    result.ok().map(|_| tables)
}

/// Detect equi-join conditions between two tables that could be used for a hash join.
/// Returns a list of `HashJoinKey` entries pointing to equality conditions in the WHERE clause.
///
/// This function analyzes the WHERE clause constraints to find equality conditions
/// where one side references only the build table and the other side references only the probe table.
/// This supports expression-based join keys like `lower(t.a) = substr(t2.b, 1, 3)`.
/// The caller is responsible for marking WHERE terms as consumed if hash join is selected.
pub fn find_equijoin_conditions(
    build_table_id: TableInternalId,
    probe_table_id: TableInternalId,
    constraints: &[Constraint],
    where_clause: &[WhereTerm],
) -> Vec<HashJoinKey> {
    let mut join_keys = Vec::new();
    // Track which WHERE clause indices we've already processed to avoid duplicates
    let mut seen_where_indices = Vec::new();

    for constraint in constraints {
        // Check if this is an equality constraint
        if !matches!(constraint.operator, ast::Operator::Equals) {
            continue;
        }
        // Get the original WHERE term
        let (where_idx, _side) = constraint.where_clause_pos;
        // Skip already processed indices (constraints can share the same WHERE term)
        if seen_where_indices.contains(&where_idx) {
            continue;
        }

        let where_term = &where_clause[where_idx];
        // Skip already consumed terms
        if where_term.consumed {
            continue;
        }

        let Ok(Some((lhs, ast::Operator::Equals, rhs))) = as_binary_components(&where_term.expr)
        else {
            continue;
        };

        // Collect table references from each side of the equality
        let Some(lhs_tables) = collect_table_refs(lhs) else {
            continue;
        };
        let Some(rhs_tables) = collect_table_refs(rhs) else {
            continue;
        };

        // Each side must reference exactly one table for a valid equi-join key
        if lhs_tables.len() != 1 || rhs_tables.len() != 1 {
            continue;
        }

        let lhs_table = lhs_tables[0];
        let rhs_table = rhs_tables[0];
        // Check if one side references build table and the other references probe table
        let build_side = if lhs_table == build_table_id && rhs_table == probe_table_id {
            Some(BinaryExprSide::Lhs)
        } else if lhs_table == probe_table_id && rhs_table == build_table_id {
            Some(BinaryExprSide::Rhs)
        } else {
            None
        };

        if let Some(build_side) = build_side {
            seen_where_indices.push(where_idx);
            join_keys.push(HashJoinKey {
                where_clause_idx: where_idx,
                build_side,
            });
        }
    }

    join_keys
}

/// Estimate the cost of a hash join between two tables.
///
/// The cost model accounts for:
/// - Build phase: Creating the hash table from the build side (one-time cost)
/// - Probe phase: Looking up each probe row in the hash table (one scan of probe table)
/// - Memory pressure: Additional IO cost if the hash table spills to disk
pub fn estimate_hash_join_cost(
    build_cardinality: f64,
    probe_cardinality: f64,
    mem_budget: usize,
) -> Cost {
    const CPU_HASH_COST: f64 = 0.001;
    const CPU_INSERT_COST: f64 = 0.002;
    const CPU_LOOKUP_COST: f64 = 0.003;
    const BYTES_PER_ROW_ESTIMATE: usize = 100;

    // Estimate if the hash table will fit in memory based on actual row counts
    let estimated_hash_table_size =
        (build_cardinality as usize).saturating_mul(BYTES_PER_ROW_ESTIMATE);
    let will_spill = estimated_hash_table_size > mem_budget;

    // Build phase: hash and insert all rows from build table (one-time cost)
    // With real ANALYZE stats, this accurately reflects the actual build table size
    let build_cost = build_cardinality * (CPU_HASH_COST + CPU_INSERT_COST);

    // Probe phase: scan probe table once, hash each row and lookup in hash table.
    // Unlike nested loop join, the probe phase is NOT multiplied by input_cardinality
    // because a hash join executes as a single unit - the probe table is scanned
    // exactly once regardless of how many rows came from prior tables.
    let probe_cost = probe_cardinality * (CPU_HASH_COST + CPU_LOOKUP_COST);

    // Spill cost: if hash table exceeds memory budget, we need to write/read partitions to disk.
    // Grace hash join writes partitions and reads them back, so it's 2x the page IO.
    // Use page-based IO cost (rows / rows_per_page) rather than per-row IO.
    let spill_cost = if will_spill {
        let build_pages = (build_cardinality / ESTIMATED_HARDCODED_ROWS_PER_PAGE as f64).ceil();
        let probe_pages = (probe_cardinality / ESTIMATED_HARDCODED_ROWS_PER_PAGE as f64).ceil();
        // Write both sides to partitions, then read back: 2 * (build_pages + probe_pages)
        (build_pages + probe_pages) * 2.0
    } else {
        0.0
    };

    Cost(build_cost + probe_cost + spill_cost)
}

/// Try to create a hash join access method for joining two tables.
/// NOTE: we are intentionally strict in choosing hash joins for the MVP, can be expanded on later on.
#[allow(clippy::too_many_arguments)]
pub fn try_hash_join_access_method(
    build_table: &JoinedTable,
    probe_table: &JoinedTable,
    build_table_idx: usize,
    probe_table_idx: usize,
    build_constraints: &TableConstraints,
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
    let probe_root_page = probe_table.table.btree().expect("table is BTree").root_page;
    let build_root_page = build_table.table.btree().expect("table is BTree").root_page;
    if build_root_page == probe_root_page {
        return None;
    }
    // Hash joins only support INNER JOIN semantics.
    // Don't use hash joins for any form of OUTER JOINs
    if build_table.join_info.as_ref().is_some_and(|ji| ji.outer)
        || probe_table.join_info.as_ref().is_some_and(|ji| ji.outer)
    {
        return None;
    }

    //skip hash join for now on USING/NATURAL joins
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

    let join_keys = find_equijoin_conditions(
        build_table.internal_id,
        probe_table.internal_id,
        &probe_constraints.constraints,
        where_clause,
    );

    // Need at least one equi-join condition
    if join_keys.is_empty() {
        return None;
    }

    // Check if either table has an index (or rowid) on any of the join columns.
    // If so, prefer nested-loop with index lookup over hash join,
    // this avoids building a hash table when we can use an existing index.
    // Check both tables because we could potentially use a different
    // join order where the indexed table becomes the probe/inner table.
    for join_key in &join_keys {
        // Check probe table constraints for index on join column
        if let Some(constraint) = probe_constraints
            .constraints
            .iter()
            .find(|c| c.where_clause_pos.0 == join_key.where_clause_idx)
        {
            if let Some(col_pos) = constraint.table_col_pos {
                // Check if the join column is a rowid alias directly from the table schema
                if let Some(column) = probe_table.columns().get(col_pos) {
                    if column.is_rowid_alias() {
                        return None;
                    }
                }
                // Also check regular indexes
                for candidate in &probe_constraints.candidates {
                    if let Some(index) = &candidate.index {
                        if index.column_table_pos_to_index_pos(col_pos).is_some() {
                            return None;
                        }
                    }
                }
            }
        }

        // Check build table constraints for index on join column
        if let Some(constraint) = build_constraints
            .constraints
            .iter()
            .find(|c| c.where_clause_pos.0 == join_key.where_clause_idx)
        {
            if let Some(col_pos) = constraint.table_col_pos {
                // Check if the join column is a rowid alias directly from the table schema
                if let Some(column) = build_table.columns().get(col_pos) {
                    if column.is_rowid_alias() {
                        return None;
                    }
                }
                // Also check regular indexes
                for candidate in &build_constraints.candidates {
                    if let Some(index) = &candidate.index {
                        if index.column_table_pos_to_index_pos(col_pos).is_some() {
                            return None;
                        }
                    }
                }
            }
        }
    }

    let cost = estimate_hash_join_cost(build_cardinality, probe_cardinality, DEFAULT_MEM_BUDGET);
    Some(AccessMethod {
        cost,
        params: AccessMethodParams::HashJoin {
            build_table_idx,
            probe_table_idx,
            join_keys,
            mem_budget: DEFAULT_MEM_BUDGET,
        },
    })
}
