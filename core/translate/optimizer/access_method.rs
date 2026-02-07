use crate::sync::Arc;
use rustc_hash::FxHashMap as HashMap;
use std::collections::VecDeque;

use turso_ext::{ConstraintInfo, ConstraintUsage, ResultCode};
use turso_parser::ast::{self, SortOrder, TableInternalId};

use crate::schema::Schema;
use crate::translate::expr::{as_binary_components, walk_expr, WalkControl};
use crate::translate::optimizer::constraints::{
    convert_to_vtab_constraint, BinaryExprSide, Constraint, RangeConstraintRef,
};
use crate::translate::optimizer::cost::RowCountEstimate;
use crate::translate::optimizer::cost_params::CostModelParams;
use crate::translate::plan::{
    plan_is_correlated, HashJoinKey, HashJoinType, NonFromClauseSubquery, SetOperation,
    SubqueryState, TableReferences, WhereTerm,
};
use crate::util::exprs_are_equivalent;
use crate::vdbe::hash_table::DEFAULT_MEM_BUDGET;
use crate::{
    schema::{FromClauseSubquery, Index, IndexColumn, Table},
    translate::plan::{IndexMethodQuery, IterationDirection, JoinOrderMember, JoinedTable},
    vtab::VirtualTable,
    LimboError, Result,
};

use super::{
    constraints::{
        analyze_and_terms_for_multi_index, analyze_or_term_for_multi_index,
        usable_constraints_for_join_order, TableConstraints,
    },
    cost::{
        estimate_cost_for_scan_or_seek, estimate_multi_index_intersection_cost,
        estimate_multi_index_scan_cost, Cost, IndexInfo,
    },
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
        /// The type of join semantics (Inner, LeftOuter, FullOuter).
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
}

/// Parameters for a single branch of a multi-index scan.
#[derive(Debug, Clone)]
pub struct MultiIndexBranchParams {
    /// The index to use for this branch, or None for rowid access.
    pub index: Option<Arc<Index>>,
    /// The constraint for this branch (needed for building seek_def).
    pub constraint: Constraint,
    /// The constraint references used for this branch.
    pub constraint_refs: Vec<RangeConstraintRef>,
    /// Estimated number of rows from this branch.
    pub estimated_rows: f64,
}

type MultiIdxBranch = (Option<Arc<Index>>, Constraint, Vec<RangeConstraintRef>, f64);

/// Computes IndexInfo for a multi-index branch given an optional index and table reference.
///
/// When `rowid_only` is true (for intersection branches), the index is treated as covering
/// because we only need to extract rowids during the branch scan, not fetch table data.
fn index_info_for_branch(
    index: Option<&Index>,
    rhs_table: &JoinedTable,
    rowid_only: bool,
) -> Option<IndexInfo> {
    match index {
        Some(index) => Some(IndexInfo {
            unique: index.unique,
            // For intersection branches, we only collect rowids, so treat as covering
            covering: rowid_only || rhs_table.index_is_covering(index),
            column_count: index.columns.len(),
        }),
        None => Some(IndexInfo {
            unique: true,
            covering: true,
            column_count: 1,
        }),
    }
}

/// Computes cost and constructs MultiIndexBranchParams for a single branch.
///
/// When `rowid_only` is true (for intersection), the branch only scans the index to collect
/// rowids into a RowSet, so we treat the index as covering (no table lookup during branch scan).
/// For union branches, we eventually fetch all rows, so covering status depends on the query.
#[allow(clippy::too_many_arguments)]
fn compute_branch_cost_and_params(
    index: Option<&Arc<Index>>,
    constraint: &Constraint,
    constraint_refs: &[RangeConstraintRef],
    estimated_rows: f64,
    rhs_table: &JoinedTable,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
    rowid_only: bool,
) -> (Cost, MultiIndexBranchParams) {
    let index_info = index_info_for_branch(index.map(|i| i.as_ref()), rhs_table, rowid_only);

    let cost = estimate_cost_for_scan_or_seek(
        index_info,
        &[constraint.clone()],
        constraint_refs,
        1.0, // Single pass for each branch
        base_row_count,
        false,
        params,
    );

    let branch_params = MultiIndexBranchParams {
        index: index.cloned(),
        constraint: constraint.clone(),
        constraint_refs: constraint_refs.to_vec(),
        estimated_rows,
    };

    (cost, branch_params)
}

/// Return the best [AccessMethod] for a given join order.
pub fn find_best_access_method_for_join_order(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
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

fn find_best_access_method_for_btree(
    rhs_table: &JoinedTable,
    rhs_constraints: &TableConstraints,
    join_order: &[JoinOrderMember],
    maybe_order_target: Option<&OrderTarget>,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
) -> Result<Option<AccessMethod>> {
    let table_no = join_order.last().unwrap().table_id;
    let mut best_cost = estimate_cost_for_scan_or_seek(
        None,
        &[],
        &[],
        input_cardinality,
        base_row_count,
        false,
        params,
    );
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

        let (iter_dir, is_index_ordered, order_satisfiability_bonus) = if let Some(order_target) =
            maybe_order_target
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
                    // Rowid target on rowid index
                    (ColumnTarget::RowId, None) => true,
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

/// Evaluates multi-index branches and returns AccessMethod if cost-effective.
#[allow(clippy::too_many_arguments)]
fn evaluate_multi_index_branches(
    branches: &[MultiIdxBranch],
    set_op: SetOperation,
    where_term_idx: usize,
    additional_consumed_terms: Vec<usize>,
    rhs_table: &JoinedTable,
    base_row_count: RowCountEstimate,
    input_cardinality: f64,
    params: &CostModelParams,
    best_cost: Cost,
) -> Option<AccessMethod> {
    let mut branch_costs = Vec::with_capacity(branches.len());
    let mut branch_rows = Vec::with_capacity(branches.len());
    let mut branch_params = Vec::with_capacity(branches.len());

    for (index, constraint, constraint_refs, estimated_rows) in branches {
        if constraint_refs.is_empty() {
            return None; // Cannot use multi-index if any branch has no index
        }

        let (cost, params_for_branch) = compute_branch_cost_and_params(
            index.as_ref(),
            constraint,
            constraint_refs,
            *estimated_rows,
            rhs_table,
            base_row_count,
            params,
            true, // rowid_only: branches only collect rowids
        );

        branch_costs.push(cost);
        branch_rows.push(*estimated_rows);
        branch_params.push(params_for_branch);
    }

    let multi_index_cost = match set_op {
        SetOperation::Union => estimate_multi_index_scan_cost(
            &branch_costs,
            &branch_rows,
            base_row_count,
            input_cardinality,
            params,
        ),
        SetOperation::Intersection => {
            let (cost, _) = estimate_multi_index_intersection_cost(
                &branch_costs,
                &branch_rows,
                base_row_count,
                input_cardinality,
                params,
            );
            cost
        }
    };

    if multi_index_cost < best_cost {
        Some(AccessMethod {
            cost: multi_index_cost,
            params: AccessMethodParams::MultiIndexScan {
                branches: branch_params,
                where_term_idx,
                set_op,
                additional_consumed_terms,
            },
        })
    } else {
        None
    }
}

/// Analyze OR clauses for multi-index union optimization.
///
/// Returns an `AccessMethod` using `MultiIndexScan` with `SetOperation::Union` if:
/// 1. An OR term has all disjuncts indexable on different indexes
/// 2. The estimated cost is lower than the provided best_cost
#[allow(clippy::too_many_arguments)]
pub fn consider_multi_index_union(
    rhs_table: &JoinedTable,
    where_clause: &[WhereTerm],
    available_indexes: &HashMap<String, VecDeque<Arc<Index>>>,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
    best_cost: Cost,
) -> Option<AccessMethod> {
    for (where_term_idx, term) in where_clause.iter().enumerate() {
        if term.consumed {
            continue;
        }

        let ast::Expr::Binary(_, ast::Operator::Or, _) = &term.expr else {
            continue;
        };

        let Some(decomposition) = analyze_or_term_for_multi_index(
            where_term_idx,
            &term.expr,
            rhs_table,
            available_indexes,
            table_references,
            subqueries,
            schema,
            params,
        ) else {
            continue;
        };

        if !decomposition.all_indexable {
            continue;
        }

        // Extract branch info from disjuncts
        let branches: Option<Vec<_>> = decomposition
            .disjuncts
            .iter()
            .map(|d| {
                d.constraint.as_ref().map(|c| {
                    (
                        d.best_index.clone(),
                        c.clone(),
                        d.constraint_refs.clone(),
                        d.estimated_rows,
                    )
                })
            })
            .collect();

        let Some(branches) = branches else {
            continue;
        };

        if branches.len() != decomposition.disjuncts.len() {
            continue;
        }

        if let Some(access_method) = evaluate_multi_index_branches(
            &branches,
            SetOperation::Union,
            where_term_idx,
            vec![],
            rhs_table,
            base_row_count,
            input_cardinality,
            params,
            best_cost,
        ) {
            return Some(access_method);
        }
    }

    None
}

/// Analyze AND terms for multi-index intersection optimization.
///
/// Returns an `AccessMethod` using `MultiIndexScan` with `SetOperation::Intersection` if:
/// 1. Multiple AND terms reference different indexed columns
/// 2. The estimated cost is lower than the provided best_cost
#[allow(clippy::too_many_arguments)]
pub fn consider_multi_index_intersection(
    rhs_table: &JoinedTable,
    where_clause: &[WhereTerm],
    available_indexes: &HashMap<String, VecDeque<Arc<Index>>>,
    table_references: &TableReferences,
    subqueries: &[NonFromClauseSubquery],
    schema: &Schema,
    input_cardinality: f64,
    base_row_count: RowCountEstimate,
    params: &CostModelParams,
    best_cost: Cost,
) -> Option<AccessMethod> {
    let decomposition = analyze_and_terms_for_multi_index(
        rhs_table,
        where_clause,
        available_indexes,
        table_references,
        subqueries,
        schema,
        params,
    )?;

    if decomposition.branches.len() < 2 {
        return None;
    }

    // Extract branch info
    let branches: Vec<_> = decomposition
        .branches
        .iter()
        .map(|b| {
            (
                b.index.clone(),
                b.constraint.clone(),
                b.constraint_refs.clone(),
                b.estimated_rows,
            )
        })
        .collect::<Vec<MultiIdxBranch>>();

    let where_term_idx = decomposition.term_indices[0];
    let additional_consumed_terms: Vec<usize> =
        decomposition.term_indices.iter().skip(1).copied().collect();

    evaluate_multi_index_branches(
        &branches,
        SetOperation::Intersection,
        where_term_idx,
        additional_consumed_terms,
        rhs_table,
        base_row_count,
        input_cardinality,
        params,
        best_cost,
    )
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
    // Determine hash join type based on outer join info.
    // The caller passes build=LHS, probe=RHS (the optimizer's convention).
    // For LEFT JOIN, the RHS (probe_table) has join_info.outer = true.
    // For FULL OUTER, the RHS has join_info.full_outer = true.
    // The code gen handles role reversal: for outer joins, the hash table is
    // built from the RHS (the table that can produce NULLs), and the LHS scans.
    let hash_join_type = if probe_table
        .join_info
        .as_ref()
        .is_some_and(|ji| ji.full_outer)
    {
        HashJoinType::FullOuter
    } else if probe_table.join_info.as_ref().is_some_and(|ji| ji.outer) {
        // LEFT OUTER hash join is not yet supported — the unmatched build scan
        // does not re-open inner loops for tables nested inside the probe loop,
        // which causes incorrect results for multi-table LEFT JOINs.
        return None;
    } else {
        HashJoinType::Inner
    };

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

    // Check if either table has an index (or rowid) on any of the join columns.
    // If so, prefer nested-loop with index lookup over hash join,
    // this avoids building a hash table when we can use an existing index.
    // Skip this check for FULL OUTER JOIN — hash join is required for unmatched build scanning.
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
        params: AccessMethodParams::MaterializedSubquery {
            index: ephemeral_index,
            constraint_refs: usable_constraint_refs,
        },
    }))
}
