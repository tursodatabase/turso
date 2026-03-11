use crate::sync::Arc;
use rustc_hash::FxHashMap as HashMap;
use std::collections::VecDeque;

use turso_ext::{ConstraintInfo, ConstraintUsage, ResultCode};
use turso_parser::ast::{self, SortOrder, TableInternalId};

use crate::schema::Schema;
use crate::stats::AnalyzeStats;
use crate::translate::expr::{as_binary_components, walk_expr, WalkControl};
use crate::translate::optimizer::constraints::{
    convert_to_vtab_constraint, BinaryExprSide, Constraint, ConstraintOperator, RangeConstraintRef,
};
use crate::translate::optimizer::cost::RowCountEstimate;
use crate::translate::optimizer::cost_params::CostModelParams;
use crate::translate::plan::{
    plan_is_correlated, HashJoinKey, HashJoinType, NonFromClauseSubquery, SetOperation,
    SubqueryState, TableReferences, WhereTerm,
};
use crate::util::exprs_are_equivalent;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::hash_table::DEFAULT_MEM_BUDGET;
use crate::{
    schema::{FromClauseSubquery, Index, IndexColumn, Table},
    translate::plan::{IndexMethodQuery, IterationDirection, JoinOrderMember, JoinedTable},
    turso_assert,
    vtab::VirtualTable,
    LimboError, Result,
};

use super::{
    constraints::{
        usable_constraints_for_join_order, usable_constraints_for_lhs_mask, TableConstraints,
    },
    cost::{estimate_cost_for_scan_or_seek, estimate_index_cost, Cost, IndexInfo},
    multi_index::{
        consider_multi_index_intersection, consider_multi_index_union, MultiIndexBranchParams,
    },
    order::OrderTarget,
};
use crate::translate::optimizer::order::ColumnTarget;
use crate::translate::planner::TableMask;

#[derive(Debug, Clone)]
/// Represents a way to access a table.
pub struct AccessMethod {
    /// The estimated number of page fetches.
    /// CPU costs are folded into the same scalar cost model.
    pub cost: Cost,
    /// Estimated rows produced per outer row before applying remaining filters.
    pub estimated_rows_per_outer_row: f64,
    /// Which remaining filter multiplier join cardinality estimation should apply.
    pub post_access_filter: PostAccessFilter,
    /// Table-type specific access method details.
    pub params: AccessMethodParams,
}

/// Describes which remaining WHERE-term selectivity join planning must still
/// apply after choosing an access path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostAccessFilter {
    /// Full scans and virtual-table scans still need join planning to apply all
    /// relevant WHERE-term selectivity, because the access path itself did not
    /// consume any specific seek predicates.
    AllConstraints,
    /// Rowid seeks, ordinary index seeks, and `MultiIndexScan` branches already
    /// account for the lookup predicates that drive the access path, so join
    /// planning should only apply remaining local/self residual filters.
    LocalOnly,
    /// Index-method access paths such as FTS return their own output estimate, so
    /// join planning should not apply any extra filter multiplier here.
    None,
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
    /// Coroutine-based subquery (re-executed for each outer row)
    Subquery,
    /// Materialized subquery with an ephemeral index for seeking.
    /// The subquery results are materialized once into an ephemeral index,
    /// which can then be seeked using join conditions.
    MaterializedSubquery {
        /// The ephemeral index to build and seek into.
        index: Arc<Index>,
        /// The constraint references used for seeking.
        constraint_refs: Vec<RangeConstraintRef>,
    },
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
        /// Whether the build input should be materialized as a rowid list before hash build.
        materialize_build_input: bool,
        /// Whether to use a bloom filter on the probe side.
        use_bloom_filter: bool,
        /// Join semantics: Inner, LeftOuter, or FullOuter.
        join_type: HashJoinType,
    },
    /// Custom index method access (e.g., FTS).
    /// This variant is used when the optimizer determines that a custom index method
    /// should be used for table access in a join query.
    IndexMethod {
        /// The fully constructed IndexMethodQuery operation to apply to this table.
        query: IndexMethodQuery,
        /// Index in WHERE clause that was covered by this index method (if any).
        where_covered: Option<usize>,
    },
    /// Multi-index scan for OR-by-union or AND-by-intersection optimization.
    /// Used when a WHERE clause has OR/AND terms that can each use a different index.
    /// Example: WHERE a = 1 AND|OR b = 2 with separate indexes on a and b.
    MultiIndexScan {
        /// Each branch represents one term with its own index access.
        branches: Vec<MultiIndexBranchParams>,
        /// Index of the primary WHERE term.
        where_term_idx: usize,
        /// The set operation (Union for OR, Intersection for AND).
        set_op: SetOperation,
        /// For Intersection: additional WHERE term indices consumed.
        additional_consumed_terms: Vec<usize>,
    },
    /// IN-list driven index seek.
    InSeek {
        index: Option<Arc<Index>>,
        affinity: Affinity,
        where_term_idx: usize,
    },
}

/// Result of generic btree candidate selection before it is wrapped into a full
/// [`AccessMethod`].
pub(super) struct ChosenBtreeCandidate {
    pub(super) iter_dir: IterationDirection,
    pub(super) index: Option<Arc<Index>>,
    pub(super) constraint_refs: Vec<RangeConstraintRef>,
    pub(super) cost: Cost,
}

#[allow(clippy::too_many_arguments)]
/// Choose the best ordinary btree lookup candidate for one table under the
/// current join-order prefix.
pub(super) fn choose_best_btree_candidate(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    lhs_mask: &TableMask,
    rhs_table_idx: usize,
    maybe_order_target: Option<&OrderTarget>,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Option<ChosenBtreeCandidate> {
    let mut best_cost = estimate_cost_for_scan_or_seek(
        None,
        &[],
        &[],
        input_cardinality,
        base_row_count,
        false,
        params,
    );
    let mut best_choice = ChosenBtreeCandidate {
        iter_dir: IterationDirection::Forwards,
        index: None,
        constraint_refs: vec![],
        cost: best_cost,
    };
    let mut best_prereq_count = usize::MAX;
    let table_no = rhs_table.internal_id;
    let rowid_column_idx = rhs_table.columns().iter().position(|c| c.is_rowid_alias());

    // Estimate cost for each candidate index (including the rowid index) and
    // keep the best candidate.
    for candidate in rhs_constraints.candidates.iter() {
        let usable_constraint_refs = usable_constraints_for_lhs_mask(
            &rhs_constraints.constraints,
            &candidate.refs,
            lhs_mask,
            rhs_table_idx,
        );

        let index_info = match candidate.index.as_ref() {
            Some(index) => IndexInfo {
                unique: index.unique,
                covering: rhs_table.index_is_covering(index),
                column_count: index.columns.len(),
            },
            None => IndexInfo {
                unique: true,
                covering: !usable_constraint_refs.is_empty(),
                column_count: 1,
            },
        };

        let (iter_dir, is_index_ordered, order_satisfiability_bonus) = if let Some(order_target) =
            maybe_order_target
        {
            // If the index delivers rows in the same direction, or the exact
            // reverse direction, as the order target, then it satisfies the
            // ORDER BY and we can credit the access path with the saved sort.
            let mut all_same_direction = true;
            let mut all_opposite_direction = true;
            for i in 0..order_target.0.len().min(index_info.column_count) {
                let target = &order_target.0[i];
                let correct_table = target.table_id == table_no;
                let correct_column = match (&target.target, &candidate.index) {
                    // Regular column target on a normal index: compare the table
                    // column position against the indexed column.
                    (ColumnTarget::Column(col_no), Some(index)) => {
                        index.columns[i].expr.is_none() && index.columns[i].pos_in_table == *col_no
                    }
                    // Expression target on an expression index: compare the
                    // normalized expressions for equivalence.
                    (ColumnTarget::Expr(expr), Some(index)) => index.columns[i]
                        .expr
                        .as_ref()
                        .is_some_and(|e| exprs_are_equivalent(e, unsafe { &**expr })),
                    // Normal column target on the rowid access path.
                    (ColumnTarget::Column(col_no), None) => {
                        rowid_column_idx.is_some_and(|idx| idx == *col_no)
                    }
                    // Explicit rowid target on the rowid access path.
                    (ColumnTarget::RowId, None) => true,
                    _ => false,
                };
                if !correct_table || !correct_column {
                    all_same_direction = false;
                    all_opposite_direction = false;
                    break;
                }
                let correct_order = match &candidate.index {
                    Some(index) => target.order == index.columns[i].order,
                    None => target.order == SortOrder::Asc,
                };
                if correct_order {
                    all_opposite_direction = false;
                } else {
                    all_same_direction = false;
                }
            }

            let satisfies_order =
                (all_same_direction || all_opposite_direction) && !order_target.0.is_empty();
            if satisfies_order {
                // Bonus = estimated sort cost saved. Sorting is O(n log n).
                let n = *base_row_count;
                let sort_cost_saved = Cost(n * (n.max(1.0).log2()) * params.sort_cpu_per_row);
                (
                    if all_same_direction {
                        IterationDirection::Forwards
                    } else {
                        IterationDirection::Backwards
                    },
                    true,
                    sort_cost_saved,
                )
            } else {
                (IterationDirection::Forwards, false, Cost(0.0))
            }
        } else {
            (IterationDirection::Forwards, false, Cost(0.0))
        };

        let cost = estimate_cost_for_scan_or_seek(
            Some(index_info),
            &rhs_constraints.constraints,
            &usable_constraint_refs,
            input_cardinality,
            base_row_count,
            is_index_ordered,
            params,
        );
        // Prerequisite tiebreaker (mirrors SQLite's whereLoopFindLesser).
        // When costs are equal, prefer fewer outer-table prerequisites: a
        // constant-bound seek touches the same key range each iteration, while a
        // join-dependent seek may bounce around the index.
        let prereq_count: usize = usable_constraint_refs
            .iter()
            .flat_map(|ucref| {
                [
                    ucref.eq.as_ref().map(|e| e.constraint_pos),
                    ucref.lower_bound,
                    ucref.upper_bound,
                ]
                .into_iter()
            })
            .flatten()
            .map(|idx| rhs_constraints.constraints[idx].lhs_mask.table_count())
            .sum();
        let adjusted_best = best_cost + order_satisfiability_bonus;
        let costs_equal = (cost.0 - adjusted_best.0).abs() < 1e-9;
        if cost < adjusted_best || (costs_equal && prereq_count < best_prereq_count) {
            best_cost = cost;
            best_prereq_count = prereq_count;
            best_choice = ChosenBtreeCandidate {
                iter_dir,
                index: candidate.index.clone(),
                constraint_refs: usable_constraint_refs.clone(),
                cost,
            };
        }
    }

    Some(best_choice)
}

#[allow(clippy::too_many_arguments)]
fn consider_in_seek_access_method(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    lhs_mask: &TableMask,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
    best_cost: Cost,
) -> Result<Option<AccessMethod>> {
    let Table::BTree(btree) = &rhs_table.table else {
        return Err(LimboError::InternalError(
            "consider_in_seek_access_method called on non-BTree table".into(),
        ));
    };

    let base = *base_row_count;
    let tree_depth = if base <= 1.0 {
        1.0
    } else {
        (base.ln() / params.rows_per_page.ln()).ceil().max(1.0)
    };
    let mut best_in_seek = None;
    let mut best_in_seek_cost = best_cost;

    for candidate in rhs_constraints.candidates.iter() {
        let first_col_pos = candidate
            .index
            .as_ref()
            .and_then(|idx| idx.columns.first().map(|c| c.pos_in_table));

        let index_info = match candidate.index.as_ref() {
            Some(index) => IndexInfo {
                unique: index.unique,
                covering: rhs_table.index_is_covering(index),
                column_count: index.columns.len(),
            },
            None => IndexInfo {
                unique: true,
                covering: false,
                column_count: 1,
            },
        };

        for constraint in &rhs_constraints.constraints {
            let ConstraintOperator::In {
                not,
                estimated_values,
            } = constraint.operator
            else {
                continue;
            };
            if not || !lhs_mask.contains_all(&constraint.lhs_mask) {
                continue;
            }

            let matches = if candidate.index.is_none() {
                constraint.is_rowid
            } else {
                !constraint.is_rowid
                    && constraint.table_col_pos.is_some()
                    && constraint.table_col_pos == first_col_pos
            };
            if !matches {
                continue;
            }

            if let (Some(index), Some(col_pos)) = (&candidate.index, constraint.table_col_pos) {
                let constrained_column = &rhs_table.table.columns()[col_pos];
                let table_collation = constrained_column.collation();
                let index_collation = index.columns[0].collation.unwrap_or_default();
                if table_collation != index_collation {
                    continue;
                }
            }

            let rows_per_seek = if (index_info.unique && index_info.column_count == 1)
                || candidate.index.is_none()
            {
                1.0
            } else {
                (base * params.sel_eq_indexed).sqrt().max(1.0)
            };
            let in_cost = estimate_index_cost(
                base,
                tree_depth,
                index_info,
                estimated_values * input_cardinality,
                rows_per_seek,
                params,
            );
            if in_cost >= best_in_seek_cost {
                continue;
            }

            let affinity = if let Some(col_pos) = constraint.table_col_pos {
                btree
                    .columns
                    .get(col_pos)
                    .map(|col| col.affinity())
                    .unwrap_or(Affinity::Blob)
            } else {
                Affinity::Integer
            };
            best_in_seek_cost = in_cost;
            best_in_seek = Some(AccessMethod {
                cost: in_cost,
                estimated_rows_per_outer_row: (constraint.selectivity * base).max(1.0),
                post_access_filter: PostAccessFilter::LocalOnly,
                params: AccessMethodParams::InSeek {
                    index: candidate.index.clone(),
                    affinity,
                    where_term_idx: constraint.where_clause_pos.0,
                },
            });
        }
    }

    Ok(best_in_seek)
}

/// Estimate rows produced per outer row for an ordinary btree access path using
/// the join-order heuristic that the planner expects for btree lookups.
fn estimate_btree_rows_per_outer_row(
    rhs_table: &JoinedTable,
    index: Option<&Arc<Index>>,
    constraint_refs: &[RangeConstraintRef],
    analyze_stats: &AnalyzeStats,
    params: &CostModelParams,
) -> f64 {
    turso_assert!(
        !constraint_refs.is_empty(),
        "btree row-estimate helper expects a lookup, not a full scan"
    );

    if index.is_none() {
        return params.fanout_index_seek_unique;
    }

    let index = index.expect("checked above");
    let matched_cols = constraint_refs.len();
    let total_cols = index.columns.len();
    let unmatched = total_cols.saturating_sub(matched_cols);
    let table_name = rhs_table.table.get_name();
    if let Some(fanout) = analyze_stats
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

/// Return the best [AccessMethod] for a given join order.
#[allow(clippy::too_many_arguments)]
pub fn find_best_access_method_for_join_order(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
    where_clause: &[WhereTerm],
    available_indexes: &HashMap<String, VecDeque<Arc<Index>>>,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    analyze_stats: &AnalyzeStats,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<Option<AccessMethod>> {
    match &rhs_table.table {
        Table::BTree(_) => find_best_access_method_for_btree(
            rhs_table,
            rhs_constraints,
            join_order,
            maybe_order_target,
            where_clause,
            available_indexes,
            table_references,
            subqueries,
            schema,
            analyze_stats,
            input_cardinality,
            base_row_count,
            params,
        ),
        Table::Virtual(vtab) => find_best_access_method_for_vtab(
            vtab,
            &rhs_constraints.constraints,
            join_order,
            input_cardinality,
            base_row_count,
            params,
        ),
        Table::FromClauseSubquery(subquery) => find_best_access_method_for_subquery(
            rhs_table,
            subquery,
            rhs_constraints,
            join_order,
            input_cardinality,
            base_row_count,
            params,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn find_best_access_method_for_btree(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
    where_clause: &[WhereTerm],
    available_indexes: &HashMap<String, VecDeque<Arc<Index>>>,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    analyze_stats: &AnalyzeStats,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<Option<AccessMethod>> {
    let rhs_table_idx = join_order.last().unwrap().original_idx;
    let lhs_mask = TableMask::from_table_number_iter(
        join_order
            .iter()
            .take(join_order.len() - 1)
            .map(|member| member.original_idx),
    );
    let best = choose_best_btree_candidate(
        rhs_table,
        rhs_constraints,
        &lhs_mask,
        rhs_table_idx,
        maybe_order_target,
        input_cardinality,
        base_row_count,
        params,
    )
    .expect("btree candidate selection must always consider the rowid candidate");

    let mut best_access_method = AccessMethod {
        cost: best.cost,
        estimated_rows_per_outer_row: if best.constraint_refs.is_empty() {
            *base_row_count
        } else {
            estimate_btree_rows_per_outer_row(
                rhs_table,
                best.index.as_ref(),
                &best.constraint_refs,
                analyze_stats,
                params,
            )
        },
        post_access_filter: if best.constraint_refs.is_empty() {
            PostAccessFilter::AllConstraints
        } else {
            PostAccessFilter::LocalOnly
        },
        params: AccessMethodParams::BTreeTable {
            iter_dir: best.iter_dir,
            index: best.index,
            constraint_refs: best.constraint_refs,
        },
    };

    if rhs_table.btree().is_some_and(|b| b.has_rowid) {
        if let Some(in_seek_method) = consider_in_seek_access_method(
            rhs_table,
            rhs_constraints,
            &lhs_mask,
            input_cardinality,
            base_row_count,
            params,
            best_access_method.cost,
        )? {
            best_access_method = in_seek_method;
        }

        if let Some(multi_idx_method) = consider_multi_index_union(
            rhs_table,
            where_clause,
            available_indexes,
            table_references,
            subqueries,
            schema,
            input_cardinality,
            base_row_count,
            params,
            best_access_method.cost,
            &lhs_mask,
        ) {
            best_access_method = multi_idx_method;
        }

        if let Some(multi_idx_and_method) = consider_multi_index_intersection(
            rhs_table,
            where_clause,
            available_indexes,
            table_references,
            subqueries,
            schema,
            input_cardinality,
            base_row_count,
            params,
            best_access_method.cost,
            &lhs_mask,
        ) {
            best_access_method = multi_idx_and_method;
        }
    }

    Ok(Some(best_access_method))
}

fn find_best_access_method_for_vtab(
    vtab: &VirtualTable,
    constraints: &[Constraint],
    join_order: &[JoinOrderMember],
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
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
                    false,
                    params,
                ),
                estimated_rows_per_outer_row: *base_row_count,
                post_access_filter: PostAccessFilter::AllConstraints,
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

/// Detect equi-join conditions between exactly two tables for hash join.
///
/// Returns `HashJoinKey` entries pointing at `WHERE` terms of the form:
///   <build-only expr> = <probe-only expr>
/// or
///   <probe-only expr> = <build-only expr>
///
/// Both sides may be arbitrary expressions (e.g. `lower(t1.a) = substr(t2.b,1,3)`),
/// but each side must reference columns from exactly one table:
/// - the build side must reference only `build_table_id`
/// - the probe side must reference only `probe_table_id`
///
/// This function does *not* mark any terms as consumed; the caller is responsible
/// for doing so if a hash join is selected.
pub fn find_equijoin_conditions(
    build_table_id: TableInternalId,
    probe_table_id: TableInternalId,
    where_clause: &[WhereTerm],
) -> Vec<HashJoinKey> {
    let mut join_keys = Vec::new();

    for (where_idx, where_term) in where_clause.iter().enumerate() {
        if where_term.consumed {
            continue;
        }

        let Ok(Some((lhs, op, rhs))) = as_binary_components(&where_term.expr) else {
            continue;
        };
        if !matches!(op.as_ast_operator(), Some(ast::Operator::Equals)) {
            continue;
        }

        let Some(lhs_tables) = collect_table_refs(lhs) else {
            continue;
        };
        let Some(rhs_tables) = collect_table_refs(rhs) else {
            continue;
        };

        // Require each side to reference exactly one table. This prevents
        // constants or multi-table expressions from being considered join keys.
        if lhs_tables.len() != 1 || rhs_tables.len() != 1 {
            continue;
        }

        let lhs_tid = lhs_tables[0];
        let rhs_tid = rhs_tables[0];

        // Accept either orientation: build=probe or probe=build.
        let build_side = if lhs_tid == build_table_id && rhs_tid == probe_table_id {
            Some(BinaryExprSide::Lhs)
        } else if rhs_tid == build_table_id && lhs_tid == probe_table_id {
            Some(BinaryExprSide::Rhs)
        } else {
            None
        };

        if let Some(build_side) = build_side {
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
    probe_multiplier: f64,
    params: &CostModelParams,
) -> Cost {
    // Estimate if the hash table will fit in memory based on actual row counts
    let estimated_hash_table_size =
        (build_cardinality as usize).saturating_mul(params.hash_bytes_per_row as usize);
    let will_spill = estimated_hash_table_size > mem_budget;

    // Build phase: hash and insert all rows from build table (one-time cost)
    // With real ANALYZE stats, this accurately reflects the actual build table size
    let build_cost = build_cardinality * (params.hash_cpu_cost + params.hash_insert_cost);

    // Probe phase: scan probe table, hash each row and lookup in hash table.
    // If the hash-join probe loop is nested under prior tables, the probe
    // scan repeats per outer row, so scale by probe_multiplier.
    let probe_cost =
        probe_cardinality * (params.hash_cpu_cost + params.hash_lookup_cost) * probe_multiplier;

    // Spill cost: if hash table exceeds memory budget, we need to write/read partitions to disk.
    // Grace hash join writes partitions and reads them back, so it's 2x the page IO.
    // Use page-based IO cost (rows / rows_per_page) rather than per-row IO.
    let spill_cost = if will_spill {
        let build_pages = (build_cardinality / params.rows_per_page).ceil();
        let probe_pages = (probe_cardinality / params.rows_per_page).ceil();
        // Write both sides to partitions, then read back: 2 * (build_pages + probe_pages)
        (build_pages + probe_pages) * 2.0 * probe_multiplier
    } else {
        0.0
    };

    Cost(build_cost + probe_cost + spill_cost)
}

/// Try to create a hash join access method for joining two tables.
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
    probe_multiplier: f64,
    subqueries: &[NonFromClauseSubquery],
    params: &CostModelParams,
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
    // No hash join for semi/anti-joins (nested loop with index seek is preferred).
    if probe_table
        .join_info
        .as_ref()
        .is_some_and(|ji| ji.is_semi_or_anti())
        || build_table
            .join_info
            .as_ref()
            .is_some_and(|ji| ji.is_semi_or_anti())
    {
        return None;
    }
    // Determine join type from the probe table's join_info.
    let hash_join_type = if probe_table
        .join_info
        .as_ref()
        .is_some_and(|ji| ji.is_full_outer())
    {
        HashJoinType::FullOuter
    } else if probe_table
        .join_info
        .as_ref()
        .is_some_and(|ji| ji.is_outer())
    {
        HashJoinType::LeftOuter
    } else {
        HashJoinType::Inner
    };

    // Can't build from a NullRow'd table — the hash table would hold real data
    // even when the cursor is in NullRow mode.
    if build_table
        .join_info
        .as_ref()
        .is_some_and(|ji| ji.is_outer())
    {
        return None;
    }

    // Skip hash join on USING/NATURAL joins.
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
        where_clause,
    )
    .into_iter()
    .filter(|join_key| {
        let probe_expr = join_key.get_probe_expr(where_clause);
        let Some(probe_tables) = collect_table_refs(probe_expr) else {
            return false;
        };
        probe_tables.len() == 1 && probe_tables[0] == probe_table.internal_id
    })
    .collect::<Vec<_>>();
    tracing::debug!(
        build_table = build_table.table.get_name(),
        probe_table = probe_table.table.get_name(),
        join_key_count = join_keys.len(),
        "hash-join equi-join keys"
    );

    // Need at least one equi-join condition
    if join_keys.is_empty() {
        return None;
    }

    // Prefer nested-loop with index lookup when an index exists on join columns.
    // FULL OUTER must use hash join (needed for the unmatched-build scan).
    // Check both tables because we could potentially use a different
    // join order where the indexed table becomes the probe/inner table.
    if hash_join_type != HashJoinType::FullOuter {
        for join_key in &join_keys {
            let probe_expr = join_key.get_probe_expr(where_clause);
            let probe_tables = collect_table_refs(probe_expr).unwrap_or_default();
            let probe_is_single_table =
                probe_tables.len() == 1 && probe_tables[0] == probe_table.internal_id;
            let probe_is_simple_column =
                expr_is_simple_column_from_table(probe_expr, probe_table.internal_id);
            let build_expr = join_key.get_build_expr(where_clause);
            let build_is_simple_column =
                expr_is_simple_column_from_table(build_expr, build_table.internal_id);
            // Check probe table constraints for index on join column, only when the probe side
            // references the probe table alone and is a simple column/rowid reference.
            if probe_is_single_table && probe_is_simple_column {
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
            }

            // Check build table constraints for index on join column, only when the build side
            // is a simple column/rowid reference.
            if build_is_simple_column {
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
        }
    }

    let cost = estimate_hash_join_cost(
        build_cardinality,
        probe_cardinality,
        DEFAULT_MEM_BUDGET,
        probe_multiplier,
        params,
    );
    Some(AccessMethod {
        cost,
        estimated_rows_per_outer_row: probe_cardinality,
        post_access_filter: PostAccessFilter::AllConstraints,
        params: AccessMethodParams::HashJoin {
            build_table_idx,
            probe_table_idx,
            join_keys,
            mem_budget: DEFAULT_MEM_BUDGET,
            materialize_build_input: false,
            use_bloom_filter: false,
            join_type: hash_join_type,
        },
    })
}

/// Returns true when the expression is a simple column/rowid reference to the table.
/// Used to decide if an index seek could replace a hash join.
fn expr_is_simple_column_from_table(expr: &ast::Expr, table_id: TableInternalId) -> bool {
    matches!(
        expr,
        ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } if *table == table_id
    )
}

/// Find the best access method for a FROM clause subquery.
///
/// For subqueries not in the first join position (which would be scanned multiple times),
/// we try to create an ephemeral index on columns used in equality join conditions.
/// This allows seeking into the materialized subquery results instead of scanning.
fn find_best_access_method_for_subquery(
    rhs_table: &JoinedTable,
    subquery: &FromClauseSubquery,
    rhs_constraints: &TableConstraints,
    join_order: &[JoinOrderMember],
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<Option<AccessMethod>> {
    use super::constraints::ConstraintRef;

    // Correlated subqueries (referencing outer tables) cannot be materialized once -
    // they must re-execute for each outer row. Use coroutine for these.
    // This check must come first because correlated CTEs should NOT share materialized data.
    if plan_is_correlated(&subquery.plan) {
        return Ok(Some(AccessMethod {
            cost: estimate_cost_for_scan_or_seek(
                None,
                &[],
                &[],
                input_cardinality,
                base_row_count,
                false,
                params,
            ),
            estimated_rows_per_outer_row: *base_row_count,
            post_access_filter: PostAccessFilter::AllConstraints,
            params: AccessMethodParams::Subquery,
        }));
    }

    // If this is the first/only table in the join and materialization is not requested, use coroutine.
    // Note: materialize_hint is set for explicit WITH MATERIALIZED hints.
    // Multi-reference CTE materialization decisions are handled at emission time via reference counting.
    let is_leftmost = join_order.len() <= 1;
    if is_leftmost && !subquery.materialize_hint {
        return Ok(Some(AccessMethod {
            cost: estimate_cost_for_scan_or_seek(
                None,
                &[],
                &[],
                input_cardinality,
                base_row_count,
                false,
                params,
            ),
            estimated_rows_per_outer_row: *base_row_count,
            post_access_filter: PostAccessFilter::AllConstraints,
            params: AccessMethodParams::Subquery,
        }));
    }

    // Find usable equality constraints on subquery columns.
    // We need to build constraint refs from the raw constraints, similar to how
    // ephemeral index building works for regular tables.
    // Filter to usable constraints that target a table column and are equality ops.
    let usable: Vec<(usize, &Constraint)> = rhs_constraints
        .constraints
        .iter()
        .enumerate()
        .filter(|(_, c)| {
            c.usable
                && c.table_col_pos.is_some()
                && c.operator.as_ast_operator() == Some(ast::Operator::Equals)
        })
        .collect();

    if usable.is_empty() {
        // No usable equality constraints - fall back to coroutine
        return Ok(Some(AccessMethod {
            cost: estimate_cost_for_scan_or_seek(
                None,
                &[],
                &[],
                input_cardinality,
                base_row_count,
                false,
                params,
            ),
            estimated_rows_per_outer_row: *base_row_count,
            post_access_filter: PostAccessFilter::AllConstraints,
            params: AccessMethodParams::Subquery,
        }));
    }

    // Build a mapping from table_col_pos to index_col_pos.
    // Multiple constraints on the same column should share the same index_col_pos.
    let mut unique_col_positions: Vec<usize> = usable
        .iter()
        .map(|(_, c)| c.table_col_pos.expect("table_col_pos was Some above"))
        .collect();
    unique_col_positions.sort_unstable();
    unique_col_positions.dedup();

    // Map each usable constraint to a ConstraintRef
    let mut temp_constraint_refs: Vec<ConstraintRef> = usable
        .iter()
        .map(|(orig_idx, c)| {
            let table_col_pos = c.table_col_pos.expect("table_col_pos was Some above");
            let index_col_pos = unique_col_positions
                .binary_search(&table_col_pos)
                .expect("table_col_pos must exist in unique_col_positions");
            ConstraintRef {
                constraint_vec_pos: *orig_idx,
                index_col_pos,
                sort_order: SortOrder::Asc,
            }
        })
        .collect();

    temp_constraint_refs.sort_by_key(|x| x.index_col_pos);

    // Filter to only constraints that can be used given the current join order
    let usable_constraint_refs = usable_constraints_for_join_order(
        &rhs_constraints.constraints,
        &temp_constraint_refs,
        join_order,
    );

    if usable_constraint_refs.is_empty() {
        // No usable constraints after join order filtering - fall back to coroutine
        return Ok(Some(AccessMethod {
            cost: estimate_cost_for_scan_or_seek(
                None,
                &[],
                &[],
                input_cardinality,
                base_row_count,
                false,
                params,
            ),
            estimated_rows_per_outer_row: *base_row_count,
            post_access_filter: PostAccessFilter::AllConstraints,
            params: AccessMethodParams::Subquery,
        }));
    }

    // Build index columns: key columns first (from constraints), then all remaining columns.
    // Key columns are used for seeking, remaining columns are needed to read full result data.
    let mut index_columns: Vec<IndexColumn> = Vec::new();
    let mut seen_col_positions = std::collections::HashSet::new();

    // First, add key columns from constraints (for seeking)
    for cref in usable_constraint_refs.iter() {
        let col_pos = cref.table_col_pos.unwrap();
        if seen_col_positions.contains(&col_pos) {
            continue;
        }
        seen_col_positions.insert(col_pos);

        if let Some(column) = subquery.columns.get(col_pos) {
            index_columns.push(IndexColumn {
                name: column.name.clone().unwrap_or_default(),
                order: SortOrder::Asc,
                pos_in_table: col_pos,
                collation: column.collation_opt(),
                default: column.default.clone(),
                expr: None,
            });
        }
    }

    if index_columns.is_empty() {
        // Couldn't build index columns - fall back to coroutine
        return Ok(Some(AccessMethod {
            cost: estimate_cost_for_scan_or_seek(
                None,
                &[],
                &[],
                input_cardinality,
                base_row_count,
                false,
                params,
            ),
            estimated_rows_per_outer_row: *base_row_count,
            post_access_filter: PostAccessFilter::AllConstraints,
            params: AccessMethodParams::Subquery,
        }));
    }

    // Then add all remaining columns (needed for reading full result data)
    for (col_pos, column) in subquery.columns.iter().enumerate() {
        if seen_col_positions.contains(&col_pos) {
            continue;
        }
        index_columns.push(IndexColumn {
            name: column.name.clone().unwrap_or_default(),
            order: SortOrder::Asc,
            pos_in_table: col_pos,
            collation: column.collation_opt(),
            default: column.default.clone(),
            expr: None,
        });
    }

    // Create the ephemeral index definition
    // has_rowid: true ensures each row gets a unique rowid appended to the key,
    // allowing duplicate column values (e.g., two rows with identical a,b,c,d values
    // will have different keys due to different rowids).
    let ephemeral_index = Arc::new(Index {
        name: format!("ephemeral_subquery_{}", rhs_table.internal_id),
        columns: index_columns,
        unique: false,
        ephemeral: true,
        table_name: subquery.name.clone(),
        root_page: 0,
        where_clause: None,
        has_rowid: true,
        index_method: None,
    });

    // Estimate cost: materialization (one scan of subquery) + index seeks
    // This is much cheaper than nested loop (input_cardinality * subquery_size)
    let materialization_cost = *base_row_count; // RowCountEstimate implements Deref to f64
    let seek_cost = input_cardinality * params.cpu_cost_per_seek;
    let total_cost = Cost(materialization_cost + seek_cost);

    Ok(Some(AccessMethod {
        cost: total_cost,
        estimated_rows_per_outer_row: *base_row_count,
        post_access_filter: PostAccessFilter::AllConstraints,
        params: AccessMethodParams::MaterializedSubquery {
            index: ephemeral_index,
            constraint_refs: usable_constraint_refs,
        },
    }))
}
