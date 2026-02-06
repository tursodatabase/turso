use crate::{
    function::Deterministic,
    index_method::IndexMethodCostEstimate,
    numeric::Numeric,
    schema::{BTreeTable, Index, IndexColumn, Schema, Table, ROWID_SENTINEL},
    translate::{
        insert::ROWID_COLUMN,
        optimizer::{
            access_method::AccessMethodParams,
            constraints::{RangeConstraintRef, SeekRangeConstraint, TableConstraints},
            cost::RowCountEstimate,
            order::{ColumnTarget, OrderTarget},
        },
        plan::{
            ColumnUsedMask, EphemeralRowidMode, HashJoinOp, IndexMethodQuery,
            NonFromClauseSubquery, OuterQueryReference, QueryDestination, ResultSetColumn, Scan,
            SeekKeyComponent,
        },
        trigger_exec::has_relevant_triggers_type_only,
    },
    types::SeekOp,
    util::{
        count_fts_column_args, exprs_are_equivalent, simple_bind_expr, try_capture_parameters,
        try_capture_parameters_column_agnostic, try_substitute_parameters,
    },
    vdbe::{
        affinity::Affinity,
        builder::{CursorKey, CursorType, ProgramBuilder},
    },
    LimboError, Result,
};
use constraints::{
    constraints_from_where_clause, usable_constraints_for_join_order, Constraint, ConstraintRef,
};
use cost::Cost;
use join::{compute_best_join_order, BestJoinOrderResult};
use lift_common_subexpressions::lift_common_subexpressions_from_binary_or_terms;
use order::{compute_order_target, plan_satisfies_order_target, EliminatesSortBy};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::{cmp::Ordering, collections::VecDeque, sync::Arc};
use turso_ext::{ConstraintInfo, ConstraintUsage};
use turso_parser::ast::{self, Expr, SortOrder, TriggerEvent};

use super::{
    emitter::Resolver,
    plan::{
        DeletePlan, GroupBy, IterationDirection, JoinOrderMember, JoinedTable, Operation, Plan,
        Search, SeekDef, SeekKey, SelectPlan, TableReferences, UpdatePlan, WhereTerm,
    },
    planner::TableMask,
};

pub(crate) mod access_method;
pub(crate) mod constraints;
pub(crate) mod cost;
mod cost_params;
pub(crate) mod join;
pub(crate) mod lift_common_subexpressions;
pub(crate) mod order;

/// A candidate index method that could be used for table access in a join query.
/// This struct captures all information needed to construct an IndexMethodQuery
/// operation, allowing the DP join ordering algorithm to consider custom index
/// methods alongside BTree indexes.
#[derive(Debug, Clone)]
pub struct IndexMethodCandidate {
    /// Index of the table in the joined_tables list
    pub table_idx: usize,
    /// The index that defines this index method
    pub index: Arc<Index>,
    /// Pattern index from the index method definition that matched
    pub pattern_idx: usize,
    /// Arguments captured from pattern matching
    pub arguments: Vec<ast::Expr>,
    /// Mapping from synthetic column IDs to pattern column IDs for covered columns
    pub covered_columns: HashMap<usize, usize>,
    /// Index in WHERE clause that was covered by this pattern (if any)
    pub where_covered: Option<usize>,
    /// Cost estimate from the index method
    pub cost_estimate: Option<IndexMethodCostEstimate>,
}

impl IndexMethodCandidate {
    /// Build the IndexMethodQuery operation from this candidate
    pub fn to_query(&self) -> IndexMethodQuery {
        IndexMethodQuery {
            index: self.index.clone(),
            pattern_idx: self.pattern_idx,
            arguments: self.arguments.clone(),
            covered_columns: self.covered_columns.clone(),
        }
    }
}

/// Result of successfully matching an index method pattern against a query.
/// This intermediate struct allows both `collect_index_method_candidates` and
/// `optimize_table_access_with_custom_modules` to share pattern matching logic.
#[derive(Debug, Clone)]
struct IndexMethodPatternMatch {
    /// Pattern index from the index method definition that matched
    pattern_idx: usize,
    /// Parameters captured from pattern matching (positional placeholders)
    parameters: HashMap<i32, ast::Expr>,
    /// Index in WHERE clause that was covered by this pattern (if any)
    where_covered: Option<usize>,
    /// Whether the pattern explicitly handles ORDER BY
    pattern_has_order_by: bool,
    /// Whether the pattern explicitly handles LIMIT
    pattern_has_limit: bool,
    /// Pattern result columns (needed for covered columns calculation)
    pattern_columns: Vec<ast::ResultColumn>,
}

/// Try to match an index method pattern against a query's clauses.
#[allow(clippy::too_many_arguments)]
fn try_match_index_method_pattern(
    pattern: &ast::Select,
    table: &JoinedTable,
    query_where_terms: &[WhereTerm],
    order_by: &[(Box<ast::Expr>, SortOrder)],
    limit: &Option<Box<Expr>>,
    offset: &Option<Box<Expr>>,
    pattern_idx: usize,
    soft_bind_errors: bool,
) -> Option<IndexMethodPatternMatch> {
    let mut pattern = pattern.clone();
    if pattern.with.is_some() || !pattern.body.compounds.is_empty() {
        return None;
    }

    let ast::OneSelect::Select {
        columns,
        from: Some(ast::FromClause { select, joins }),
        distinctness: None,
        where_clause: ref mut pattern_where_clause,
        group_by: None,
        window_clause,
    } = &mut pattern.body.select
    else {
        if soft_bind_errors {
            return None;
        }
        panic!("unexpected select pattern body");
    };

    if !window_clause.is_empty() || !joins.is_empty() {
        return None;
    }

    let ast::SelectTable::Table(name, _, _) = select.as_ref() else {
        if soft_bind_errors {
            return None;
        }
        panic!("unexpected from clause");
    };

    // Bind expressions to this table
    for column in columns.iter_mut() {
        if let ast::ResultColumn::Expr(e, _) = column {
            if soft_bind_errors {
                if simple_bind_expr(table, &[], e).is_err() {
                    return None;
                }
            } else {
                simple_bind_expr(table, &[], e).ok()?;
            }
        }
    }
    for column in pattern.order_by.iter_mut() {
        if soft_bind_errors {
            if simple_bind_expr(table, columns, &mut column.expr).is_err() {
                return None;
            }
        } else {
            simple_bind_expr(table, columns, &mut column.expr).ok()?;
        }
    }
    if let Some(pattern_where) = pattern_where_clause {
        if soft_bind_errors {
            if simple_bind_expr(table, columns, pattern_where).is_err() {
                return None;
            }
        } else {
            simple_bind_expr(table, columns, pattern_where).ok()?;
        }
    }

    if name.name.as_str() != table.table.get_name() {
        return None;
    }

    let pattern_has_order_by = !pattern.order_by.is_empty();
    let pattern_has_limit = pattern.limit.is_some();

    // If pattern has ORDER BY, it must match exactly
    if pattern_has_order_by && order_by.len() != pattern.order_by.len() {
        return None;
    }

    let mut where_query_covered: Option<usize> = None;
    let mut parameters = HashMap::default();

    // Match ORDER BY if pattern has it
    if pattern_has_order_by {
        for (pattern_column, (query_column, query_order)) in
            pattern.order_by.iter().zip(order_by.iter())
        {
            if *query_order != pattern_column.order.unwrap_or(SortOrder::Asc) {
                return None;
            }
            let num_col_args = count_fts_column_args(&pattern_column.expr);
            let captured = if num_col_args > 0 {
                try_capture_parameters_column_agnostic(
                    &pattern_column.expr,
                    query_column,
                    num_col_args,
                )
            } else {
                try_capture_parameters(&pattern_column.expr, query_column)
            };
            parameters.extend(captured?);
        }
    }

    // Match LIMIT if pattern has it
    match (pattern.limit.as_ref().map(|x| &x.expr), limit) {
        (Some(_), None) => return None,
        (Some(pattern_limit), Some(query_limit)) => {
            let captured = try_capture_parameters(pattern_limit, query_limit)?;
            parameters.extend(captured);
        }
        (None, Some(_)) | (None, None) => {}
    }

    // Match OFFSET if pattern has it
    match (
        pattern.limit.as_ref().and_then(|x| x.offset.as_ref()),
        offset,
    ) {
        (Some(_), None) => return None,
        (Some(pattern_off), Some(query_off)) => {
            let captured = try_capture_parameters(pattern_off, query_off)?;
            parameters.extend(captured);
        }
        (None, Some(_)) | (None, None) => {}
    }

    // Match WHERE clause
    if let Some(pattern_where) = pattern_where_clause {
        for (i, query_where) in query_where_terms.iter().enumerate() {
            let num_col_args = count_fts_column_args(pattern_where);
            let captured = if num_col_args > 0 {
                try_capture_parameters_column_agnostic(
                    pattern_where,
                    &query_where.expr,
                    num_col_args,
                )
            } else {
                try_capture_parameters(pattern_where, &query_where.expr)
            };
            let Some(captured) = captured else {
                continue;
            };
            parameters.extend(captured);
            where_query_covered = Some(i);
            break;
        }
    }

    // Pattern requires WHERE but we didn't match any
    if pattern_where_clause.is_some() && where_query_covered.is_none() {
        return None;
    }

    let where_covered_completely = query_where_terms.is_empty()
        || (where_query_covered.is_some() && query_where_terms.len() == 1);

    // When WHERE is not completely covered, skip patterns with ORDER BY/LIMIT
    // because post-filtering would disrupt the order or apply limits incorrectly
    if !where_covered_completely && (pattern_has_order_by || pattern_has_limit) {
        return None;
    }

    Some(IndexMethodPatternMatch {
        pattern_idx,
        parameters,
        where_covered: where_query_covered,
        pattern_has_order_by,
        pattern_has_limit,
        pattern_columns: columns.clone(),
    })
}

/// Build covered columns mapping from pattern columns.
/// Returns a HashMap mapping synthetic column IDs to pattern column IDs.
fn build_covered_columns_mapping(
    pattern_columns: &[ast::ResultColumn],
    parameters: &HashMap<i32, ast::Expr>,
) -> HashMap<usize, usize> {
    let mut covered_column_id = 1_000_000;
    let mut covered_columns = HashMap::default();
    for (pattern_column_id, pattern_column) in pattern_columns.iter().enumerate() {
        let ast::ResultColumn::Expr(pattern_expr, _) = pattern_column else {
            continue;
        };
        let Some(_substituted) = try_substitute_parameters(pattern_expr, parameters) else {
            continue;
        };
        covered_columns.insert(covered_column_id, pattern_column_id);
        covered_column_id += 1;
    }
    covered_columns
}

/// Sort parameters by key and extract just the expressions as a Vec.
fn sorted_arguments_from_parameters(parameters: &HashMap<i32, ast::Expr>) -> Vec<ast::Expr> {
    let mut arguments: Vec<_> = parameters.iter().collect();
    arguments.sort_by_key(|(&i, _)| i);
    arguments.iter().map(|(_, e)| (*e).clone()).collect()
}

/// Collect index method candidates for all tables that have custom index methods.
/// This function performs pattern matching but does NOT apply the operations,
/// allowing the DP join ordering algorithm to consider index methods as candidates.
#[allow(clippy::too_many_arguments)]
fn collect_index_method_candidates(
    table_references: &TableReferences,
    available_indexes: &HashMap<String, VecDeque<Arc<Index>>>,
    where_clause: &[WhereTerm],
    order_by: &[(Box<ast::Expr>, SortOrder)],
    group_by: &Option<GroupBy>,
    limit: &Option<Box<Expr>>,
    offset: &Option<Box<Expr>>,
    base_table_rows: &[RowCountEstimate],
    params: &cost_params::CostModelParams,
) -> Result<Vec<IndexMethodCandidate>> {
    let mut candidates = Vec::new();

    // Group by is not supported for index methods
    if group_by.is_some() {
        return Ok(candidates);
    }

    let tables = table_references.joined_tables();
    for (table_idx, table) in tables.iter().enumerate() {
        let Some(indexes) = available_indexes.get(table.table.get_name()) else {
            continue;
        };

        for index in indexes {
            let Some(module) = &index.index_method else {
                continue;
            };
            if index.is_backing_btree_index() {
                continue;
            }

            let definition = module.definition();
            for (pattern_idx, pattern) in definition.patterns.iter().enumerate() {
                // Use shared helper for pattern matching
                let Some(pattern_match) = try_match_index_method_pattern(
                    pattern,
                    table,
                    where_clause,
                    order_by,
                    limit,
                    offset,
                    pattern_idx,
                    true, // continue on binding failures
                ) else {
                    continue;
                };

                // Build covered columns mapping from pattern match
                let covered_columns = build_covered_columns_mapping(
                    &pattern_match.pattern_columns,
                    &pattern_match.parameters,
                );

                // Get cost estimate from the index method
                let cost_estimate = module.init().ok().and_then(|cursor| {
                    let base_rows = base_table_rows
                        .get(table_idx)
                        .map(|r| **r)
                        .unwrap_or(params.rows_per_table_fallback);
                    cursor.estimate_cost(pattern_match.pattern_idx, base_rows)
                });

                // Sort and collect arguments
                let arguments = sorted_arguments_from_parameters(&pattern_match.parameters);

                candidates.push(IndexMethodCandidate {
                    table_idx,
                    index: index.clone(),
                    pattern_idx: pattern_match.pattern_idx,
                    arguments,
                    covered_columns,
                    where_covered: pattern_match.where_covered,
                    cost_estimate,
                });

                // Found a match for this table+index, try next index
                break;
            }
        }
    }

    Ok(candidates)
}

#[tracing::instrument(skip_all, level = tracing::Level::DEBUG)]
pub fn optimize_plan(program: &mut ProgramBuilder, plan: &mut Plan, schema: &Schema) -> Result<()> {
    match plan {
        Plan::Select(plan) => optimize_select_plan(plan, schema)?,
        Plan::Delete(plan) => optimize_delete_plan(plan, schema)?,
        Plan::Update(plan) => optimize_update_plan(program, plan, schema)?,
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            optimize_select_plan(right_most, schema)?;
            for (plan, _) in left {
                optimize_select_plan(plan, schema)?;
            }
        }
    }
    // When debug tracing is enabled, print the optimized plan as a SQL string for debugging
    tracing::debug!(plan_sql = plan.to_string());
    Ok(())
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
/// Transform MATCH expressions to fts_match() function calls.
fn transform_match_to_fts_match(where_clause: &mut [WhereTerm]) {
    use super::ast::{FunctionTail, LikeOperator, Name};
    use super::expr::{walk_expr_mut, WalkControl};

    for term in where_clause.iter_mut() {
        let _ = walk_expr_mut(&mut term.expr, &mut |e: &mut Expr| -> Result<WalkControl> {
            match e {
                Expr::Like {
                    lhs,
                    not,
                    op: LikeOperator::Match,
                    rhs,
                    escape: _,
                } => {
                    // Transform MATCH to fts_match():
                    // - `col MATCH 'query'` -> `fts_match(col, 'query')`
                    // - `(col1, col2) MATCH 'query'` -> `fts_match(col1, col2, 'query')`
                    let mut args: Vec<Box<Expr>> = match lhs.as_ref() {
                        Expr::Parenthesized(cols) => cols.clone(),
                        _ => vec![lhs.clone()],
                    };
                    args.push(rhs.clone());

                    let func_call = Expr::FunctionCall {
                        name: Name::exact("fts_match".to_string()),
                        distinctness: None,
                        args,
                        order_by: vec![],
                        filter_over: FunctionTail {
                            filter_clause: None,
                            over_clause: None,
                        },
                    };
                    if *not {
                        // For NOT MATCH, just wrap the whole thing in a unary NOT
                        *e = Expr::Unary(ast::UnaryOperator::Not, Box::new(func_call));
                    } else {
                        *e = func_call;
                    }
                    Ok(WalkControl::Continue)
                }
                _ => Ok(WalkControl::Continue),
            }
        });
    }
}

/**
 * Make a few passes over the plan to optimize it.
 * TODO: these could probably be done in less passes,
 * but having them separate makes them easier to understand
 */
pub fn optimize_select_plan(plan: &mut SelectPlan, schema: &Schema) -> Result<()> {
    // Transform MATCH expressions to fts_match() for FTS optimizer recognition
    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    transform_match_to_fts_match(&mut plan.where_clause);

    optimize_subqueries(plan, schema)?;
    lift_common_subexpressions_from_binary_or_terms(&mut plan.where_clause)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    let best_join_order = optimize_table_access(
        schema,
        &mut plan.result_columns,
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
        &mut plan.order_by,
        &mut plan.group_by,
        &plan.non_from_clause_subqueries,
        &mut plan.limit,
        &mut plan.offset,
    )?;

    if let Some(best_join_order) = best_join_order {
        plan.join_order = best_join_order;
    }

    Ok(())
}

fn optimize_delete_plan(plan: &mut DeletePlan, schema: &Schema) -> Result<()> {
    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    transform_match_to_fts_match(&mut plan.where_clause);

    lift_common_subexpressions_from_binary_or_terms(&mut plan.where_clause)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    if let Some(rowset_plan) = plan.rowset_plan.as_mut() {
        optimize_select_plan(rowset_plan, schema)?;
    }

    let _ = optimize_table_access(
        schema,
        &mut plan.result_columns,
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
        &mut plan.order_by,
        &mut None,
        &plan.non_from_clause_subqueries,
        &mut plan.limit,
        &mut plan.offset,
    )?;

    Ok(())
}

fn optimize_update_plan(
    program: &mut ProgramBuilder,
    plan: &mut UpdatePlan,
    schema: &Schema,
) -> Result<()> {
    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    transform_match_to_fts_match(&mut plan.where_clause);
    lift_common_subexpressions_from_binary_or_terms(&mut plan.where_clause)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }
    let _ = optimize_table_access(
        schema,
        &mut [],
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
        &mut plan.order_by,
        &mut None,
        &plan.non_from_clause_subqueries,
        &mut plan.limit,
        &mut plan.offset,
    )?;

    let table_ref = &mut plan.table_references.joined_tables_mut()[0];

    // An ephemeral table is required if:
    // 1. The UPDATE modifies any column that is present in the key of the btree used to iterate over the table.
    //    For regular table scans or seeks, this is just the rowid or the rowid alias column (INTEGER PRIMARY KEY)
    //    For index scans and seeks, this is any column in the index used.
    // 2. There are UPDATE triggers on the table. This is done in SQLite for all UPDATE triggers on
    //    the affected table even if the trigger would not have an impact on the target table --
    //    presumably due to lack of static analysis capabilities to determine whether it's safe
    //    to skip the rowset materialization.
    let requires_ephemeral_table = 'requires: {
        let Some(btree_table_arc) = table_ref.table.btree() else {
            break 'requires false;
        };
        let btree_table = btree_table_arc.as_ref();

        // Check if there are UPDATE triggers
        let updated_cols: HashSet<usize> = plan.set_clauses.iter().map(|(i, _)| *i).collect();
        if has_relevant_triggers_type_only(
            schema,
            TriggerEvent::Update,
            Some(&updated_cols),
            btree_table,
        ) {
            break 'requires true;
        }

        // REPLACE mode requires ephemeral table because REPLACE deletes conflicting rows,
        // which can corrupt the iteration order when iterating via an index.
        if matches!(
            plan.or_conflict,
            Some(turso_parser::ast::ResolveType::Replace)
        ) {
            break 'requires true;
        }

        let Some(index) = table_ref.op.index() else {
            let rowid_alias_used = plan.set_clauses.iter().fold(false, |accum, (idx, _)| {
                accum || (*idx != ROWID_SENTINEL && btree_table.columns[*idx].is_rowid_alias())
            });
            if rowid_alias_used {
                break 'requires true;
            }
            let direct_rowid_update = plan
                .set_clauses
                .iter()
                .any(|(idx, _)| *idx == ROWID_SENTINEL);
            if direct_rowid_update {
                break 'requires true;
            }
            break 'requires false;
        };

        plan.set_clauses
            .iter()
            .any(|(idx, _)| index.columns.iter().any(|c| c.pos_in_table == *idx))
    };

    if !requires_ephemeral_table {
        return Ok(());
    }

    add_ephemeral_table_to_update_plan(program, plan)
}

/// An ephemeral table is required if:
/// 1. The UPDATE modifies any column that is present in the key of the btree used to iterate over the table.
///    For regular table scans or seeks, the key is the rowid or the rowid alias column (INTEGER PRIMARY KEY).
///    For index scans and seeks, the key is any column in the index used.
/// 2. There are UPDATE triggers on the table (SQLite always uses ephemeral tables when triggers exist).
///
/// The ephemeral table will accumulate all the rowids of the rows that are affected by the UPDATE,
/// and then the temp table will be iterated over and the actual row updates performed.
///
/// This is necessary because an UPDATE is implemented as a DELETE-then-INSERT operation, which could
/// mess up the iteration order of the rows by changing the keys in the table/index that the iteration
/// is performed over. The ephemeral table ensures stable iteration because it is not modified during
/// the UPDATE loop.
fn add_ephemeral_table_to_update_plan(
    program: &mut ProgramBuilder,
    plan: &mut UpdatePlan,
) -> Result<()> {
    let internal_id = program.table_reference_counter.next();
    let ephemeral_table = Arc::new(BTreeTable {
        root_page: 0, // Not relevant for ephemeral table definition
        name: "ephemeral_scratch".to_string(),
        has_rowid: true,
        has_autoincrement: false,
        primary_key_columns: vec![],
        columns: vec![ROWID_COLUMN],
        is_strict: false,
        unique_sets: vec![],
        foreign_keys: vec![],
    });

    let temp_cursor_id = program.alloc_cursor_id_keyed(
        CursorKey::table(internal_id),
        CursorType::BTreeTable(ephemeral_table.clone()),
    );

    // The actual update loop will use the ephemeral table as the single [JoinedTable] which it then loops over.
    let table_references_update = TableReferences::new(
        vec![JoinedTable {
            table: Table::BTree(ephemeral_table.clone()),
            identifier: "ephemeral_scratch".to_string(),
            internal_id,
            op: Operation::Scan(Scan::BTreeTable {
                iter_dir: IterationDirection::Forwards,
                index: None,
            }),
            join_info: None,
            col_used_mask: ColumnUsedMask::default(),
            column_use_counts: Vec::new(),
            expression_index_usages: Vec::new(),
            database_id: 0,
        }],
        vec![],
    );

    // Building the ephemeral table will use the TableReferences from the original plan -- i.e. if we chose an index scan originally,
    // we will build the ephemeral table by using the same index scan and using the same WHERE filters.
    let table_references_ephemeral_select =
        std::mem::replace(&mut plan.table_references, table_references_update);

    for table in table_references_ephemeral_select.joined_tables() {
        // The update loop needs to reference columns from the original source table, so we add it as an outer query reference.
        plan.table_references
            .add_outer_query_reference(OuterQueryReference {
                identifier: table.identifier.clone(),
                internal_id: table.internal_id,
                table: table.table.clone(),
                col_used_mask: table.col_used_mask.clone(),
            });
    }

    let join_order = table_references_ephemeral_select
        .joined_tables()
        .iter()
        .enumerate()
        .map(|(i, t)| JoinOrderMember {
            table_id: t.internal_id,
            original_idx: i,
            is_outer: t
                .join_info
                .as_ref()
                .is_some_and(|join_info| join_info.outer),
        })
        .collect();
    let rowid_internal_id = table_references_ephemeral_select
        .joined_tables()
        .first()
        .unwrap()
        .internal_id;

    let ephemeral_plan = SelectPlan {
        table_references: table_references_ephemeral_select,
        result_columns: vec![ResultSetColumn {
            expr: Expr::RowId {
                database: None,
                table: rowid_internal_id,
            },
            alias: None,
            contains_aggregates: false,
        }],
        where_clause: plan.where_clause.drain(..).collect(),
        group_by: None,     // N/A
        order_by: vec![],   // N/A
        aggregates: vec![], // N/A
        limit: None,        // N/A
        query_destination: QueryDestination::EphemeralTable {
            cursor_id: temp_cursor_id,
            table: ephemeral_table,
            rowid_mode: EphemeralRowidMode::FromResultColumns,
        },
        join_order,
        offset: None,
        contains_constant_false_condition: false,
        distinctness: super::plan::Distinctness::NonDistinct,
        values: vec![],
        window: None,
        // Move subqueries from the main plan to the ephemeral plan since the WHERE clause was moved
        non_from_clause_subqueries: plan.non_from_clause_subqueries.drain(..).collect(),
    };

    plan.ephemeral_plan = Some(ephemeral_plan);

    Ok(())
}

fn optimize_subqueries(plan: &mut SelectPlan, schema: &Schema) -> Result<()> {
    for table in plan.table_references.joined_tables_mut() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table.table {
            // Use match to handle both SelectPlan and CompoundSelect variants
            match from_clause_subquery.plan.as_mut() {
                Plan::Select(select_plan) => optimize_select_plan(select_plan, schema)?,
                Plan::CompoundSelect {
                    left, right_most, ..
                } => {
                    optimize_select_plan(right_most, schema)?;
                    for (select_plan, _) in left {
                        optimize_select_plan(select_plan, schema)?;
                    }
                }
                Plan::Delete(_) | Plan::Update(_) => {
                    return Err(LimboError::InternalError(
                        "DELETE/UPDATE plans should not appear in FROM clause subqueries"
                            .to_string(),
                    ));
                }
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn optimize_table_access_with_custom_modules(
    result_columns: &mut [ResultSetColumn],
    table_references: &mut TableReferences,
    available_indexes: &HashMap<String, VecDeque<Arc<Index>>>,
    where_query: &mut [WhereTerm],
    order_by: &mut Vec<(Box<ast::Expr>, SortOrder)>,
    group_by: &mut Option<GroupBy>,
    limit: &mut Option<Box<Expr>>,
    offset: &mut Option<Box<Expr>>,
) -> Result<bool> {
    let tables = table_references.joined_tables_mut();
    if tables.is_empty() {
        return Ok(false);
    }

    // group by is not supported for now
    if group_by.is_some() {
        return Ok(false);
    }

    // Only optimize the first table with custom index methods.
    // This allows FTS to be used as the driving table in joins.
    let table = &mut tables[0];
    let Some(indexes) = available_indexes.get(table.table.get_name()) else {
        return Ok(false);
    };
    for index in indexes {
        let Some(module) = &index.index_method else {
            continue;
        };
        if index.is_backing_btree_index() {
            continue;
        }
        let definition = module.definition();
        for (pattern_idx, pattern) in definition.patterns.iter().enumerate() {
            let Some(pattern_match) = try_match_index_method_pattern(
                pattern,
                table,
                where_query,
                order_by,
                limit,
                offset,
                pattern_idx,
                false, // panic on binding failures
            ) else {
                continue;
            };

            // Mark WHERE clause as consumed
            if let Some(where_covered) = pattern_match.where_covered {
                where_query[where_covered].consumed = true;
            }

            // Build covered columns mapping and update result_columns.
            // This differs from collect_index_method_candidates: we modify result_columns
            // and increment covered_column_id per matching query column, not per pattern column.
            let mut covered_column_id = 1_000_000;
            let mut covered_columns = HashMap::default();
            for (pattern_column_id, pattern_column) in
                pattern_match.pattern_columns.iter().enumerate()
            {
                let ast::ResultColumn::Expr(pattern_expr, _) = pattern_column else {
                    continue;
                };
                let Some(substituted) =
                    try_substitute_parameters(pattern_expr, &pattern_match.parameters)
                else {
                    continue;
                };
                for query_column in result_columns.iter_mut() {
                    if !exprs_are_equivalent(&query_column.expr, &substituted) {
                        continue;
                    }
                    query_column.expr = ast::Expr::Column {
                        database: None,
                        table: table.internal_id,
                        column: covered_column_id,
                        is_rowid_alias: false,
                    };
                    covered_columns.insert(covered_column_id, pattern_column_id);
                    covered_column_id += 1;
                }
            }

            // Calculate whether WHERE is completely covered for ORDER BY/LIMIT clearing
            let where_covered_completely = where_query.is_empty()
                || (pattern_match.where_covered.is_some() && where_query.len() == 1);

            // Only clear ORDER BY/LIMIT/OFFSET if:
            // 1. The pattern explicitly handles them (has ORDER BY/LIMIT), AND
            // 2. WHERE is completely covered (no post-filtering needed)
            // Otherwise, keep them so they're applied after post-filtering
            if pattern_match.pattern_has_order_by && where_covered_completely {
                let _ = order_by.drain(..);
            }
            if pattern_match.pattern_has_limit && where_covered_completely {
                let _ = limit.take();
                let _ = offset.take();
            }

            // Sort and collect arguments
            let arguments = sorted_arguments_from_parameters(&pattern_match.parameters);

            table.op = Operation::IndexMethodQuery(IndexMethodQuery {
                index: index.clone(),
                pattern_idx: pattern_match.pattern_idx,
                covered_columns,
                arguments,
            });
            return Ok(true);
        }
    }
    Ok(false)
}

/// We do a single pass over projected, grouping, and ordering expressions to
/// capture every expression that could be served directly from an expression index.
/// Example:
///   CREATE INDEX idx ON t(lower(a));
///   SELECT lower(a) FROM t ORDER BY lower(a);
/// Both the SELECT list and ORDER BY can be covered by idx, avoiding a
/// table cursor entirely. Recording them upfront lets both the cost model
/// and covering checks reuse the same facts.
fn register_expression_index_usages_for_plan(
    table_references: &mut TableReferences,
    result_columns: &[ResultSetColumn],
    order_by: &[(Box<ast::Expr>, SortOrder)],
    group_by: Option<&GroupBy>,
) {
    table_references.reset_expression_index_usages();
    for rc in result_columns {
        table_references.register_expression_index_usage(&rc.expr);
    }
    for (expr, _) in order_by {
        table_references.register_expression_index_usage(expr);
    }
    if let Some(group_by) = group_by {
        for expr in &group_by.exprs {
            table_references.register_expression_index_usage(expr);
        }
        if let Some(having) = &group_by.having {
            for expr in having {
                table_references.register_expression_index_usage(expr);
            }
        }
    }
}

/// Derive a base row-count estimate for a table, preferring ANALYZE stats.
fn base_row_estimate(
    schema: &Schema,
    table: &JoinedTable,
    params: &cost_params::CostModelParams,
) -> RowCountEstimate {
    match &table.table {
        Table::BTree(btree) => {
            if let Some(stats) = schema.analyze_stats.table_stats(&btree.name) {
                if let Some(rows) = stats.row_count.or_else(|| {
                    stats
                        .index_stats
                        .values()
                        .find_map(|idx_stat| idx_stat.total_rows)
                }) {
                    return RowCountEstimate::AnalyzeStats(rows as f64);
                }
            }
            RowCountEstimate::hardcoded_fallback(params)
        }
        _ => RowCountEstimate::hardcoded_fallback(params),
    }
}

/// Optimize the join order and index selection for a query.
///
/// This function does the following:
/// - Computes a set of [Constraint]s for each table.
/// - Using those constraints, computes the best join order for the list of [TableReference]s
///   and selects the best [crate::translate::optimizer::access_method::AccessMethod] for each table in the join order.
/// - Mutates the [Operation]s in `joined_tables` to use the selected access methods.
/// - Removes predicates from the `where_clause` that are now redundant due to the selected access methods.
/// - Removes sorting operations if the selected join order and access methods satisfy the [crate::translate::optimizer::order::OrderTarget].
///
/// Returns the join order if it was optimized, or None if the default join order was considered best.
#[allow(clippy::too_many_arguments)]
fn optimize_table_access(
    schema: &Schema,
    result_columns: &mut [ResultSetColumn],
    table_references: &mut TableReferences,
    available_indexes: &HashMap<String, VecDeque<Arc<Index>>>,
    where_clause: &mut [WhereTerm],
    order_by: &mut Vec<(Box<ast::Expr>, SortOrder)>,
    group_by: &mut Option<GroupBy>,
    subqueries: &[NonFromClauseSubquery],
    limit: &mut Option<Box<Expr>>,
    offset: &mut Option<Box<Expr>>,
) -> Result<Option<Vec<JoinOrderMember>>> {
    // When optimizer_params feature is enabled, use lazily-loaded params (cached process-wide).
    // Otherwise, use the compile-time static for zero overhead.
    #[cfg(feature = "optimizer_params")]
    let params: &cost_params::CostModelParams = &cost_params::LOADED_PARAMS;
    #[cfg(not(feature = "optimizer_params"))]
    let params: &cost_params::CostModelParams = &cost_params::DEFAULT_PARAMS;

    if table_references.joined_tables().is_empty() {
        return Ok(None);
    }
    if table_references.joined_tables().len() > TableReferences::MAX_JOINED_TABLES {
        crate::bail_parse_error!(
            "Only up to {} tables can be joined",
            TableReferences::MAX_JOINED_TABLES
        );
    }

    let has_expression_index = table_references.joined_tables().iter().any(|t| {
        matches!(&t.table, Table::BTree(btree) if available_indexes
            .get(&btree.name)
            .is_some_and(|indexes| indexes.iter().any(|index| index.is_expression_index())))
    });

    if has_expression_index {
        register_expression_index_usages_for_plan(
            table_references,
            result_columns,
            order_by.as_slice(),
            group_by.as_ref(),
        );
    }

    // For single-table queries, try to optimize with custom index methods directly.
    // This is the fast path that preserves the original behavior.
    let is_single_table = table_references.joined_tables().len() == 1;
    if is_single_table {
        let optimized = optimize_table_access_with_custom_modules(
            result_columns,
            table_references,
            available_indexes,
            where_clause,
            order_by,
            group_by,
            limit,
            offset,
        )?;
        if optimized {
            return Ok(None);
        }
    }

    let mut access_methods_arena = Vec::new();

    // For multi-table queries, collect index method candidates to pass to the DP algorithm.
    // This allows the optimizer to consider index methods at any position in the join order.
    let base_table_rows_for_candidates = table_references
        .joined_tables()
        .iter()
        .map(|t| base_row_estimate(schema, t, params))
        .collect::<Vec<_>>();

    let index_method_candidates = if !is_single_table {
        collect_index_method_candidates(
            table_references,
            available_indexes,
            where_clause,
            order_by,
            group_by,
            limit,
            offset,
            &base_table_rows_for_candidates,
            params,
        )?
    } else {
        Vec::new()
    };
    let maybe_order_target = compute_order_target(order_by, group_by.as_mut(), table_references);
    let mut constraints_per_table = constraints_from_where_clause(
        where_clause,
        table_references,
        available_indexes,
        subqueries,
        schema,
        params,
    )?;

    let base_table_rows = table_references
        .joined_tables()
        .iter()
        .map(|t| base_row_estimate(schema, t, params))
        .collect::<Vec<_>>();

    // Currently the expressions we evaluate as constraints are binary comparisons that (except for IS/IS NOT)
    // will never be true for a NULL operand.
    // If there are any constraints on the right hand side table of an outer join that are not part of the outer join condition,
    // the outer join can be converted into an inner join.
    // for example:
    // - SELECT * FROM t1 LEFT JOIN t2 ON false WHERE t2.id = 5
    // there can never be a situation where null columns are emitted for t2 because t2.id = 5 will never be true in that case.
    // hence: we can convert the outer join into an inner join.
    //
    // Converting a LEFT JOIN into an INNER JOIN is an optimization opportunity:
    // it can enable join reordering and let more predicates participate in key selection.
    // -> recompute constraints if we rewrote a LEFT JOIN into an INNER JOIN.
    loop {
        let mut outer_join_rewritten = false;
        for (i, t) in table_references
            .joined_tables_mut()
            .iter_mut()
            .enumerate()
            .filter(|(_, t)| {
                t.join_info
                    .as_ref()
                    .is_some_and(|join_info| join_info.outer)
            })
        {
            // Check if there's a constraint that would filter out NULL rows,
            // allowing us to convert the LEFT JOIN into an INNER JOIN for join reordering purposes.
            // Most binary ops like x = foo filter out NULL rows, but
            // IS NULL constraints do NOT - they specifically KEEP them.
            // So we should not convert LEFT JOIN to INNER JOIN based on IS NULL constraints.
            if constraints_per_table[i].constraints.iter().any(|c| {
                let is_from_where = where_clause[c.where_clause_pos.0].from_outer_join.is_none();
                let is_is_null = c.operator == ast::Operator::Is.into();
                is_from_where && !is_is_null
            }) {
                t.join_info.as_mut().unwrap().outer = false;
                for term in where_clause.iter_mut() {
                    if let Some(from_outer_join) = term.from_outer_join {
                        if from_outer_join == t.internal_id {
                            term.from_outer_join = None;
                        }
                    }
                }
                outer_join_rewritten = true;
            }
        }
        if !outer_join_rewritten {
            break;
        }
        constraints_per_table = constraints_from_where_clause(
            where_clause,
            table_references,
            available_indexes,
            subqueries,
            schema,
            params,
        )?;
    }

    let Some(best_join_order_result) = compute_best_join_order(
        table_references.joined_tables_mut(),
        maybe_order_target.as_ref(),
        &constraints_per_table,
        &base_table_rows,
        &mut access_methods_arena,
        where_clause,
        subqueries,
        &index_method_candidates,
        params,
        &schema.analyze_stats,
    )?
    else {
        return Ok(None);
    };

    let BestJoinOrderResult {
        best_plan,
        best_ordered_plan,
    } = best_join_order_result;

    // See if best_ordered_plan is better than the overall best_plan if we add a sorting penalty
    // to the unordered plan's cost.
    let best_plan = if let Some(best_ordered_plan) = best_ordered_plan {
        let best_unordered_plan_cost = best_plan.cost;
        let best_ordered_plan_cost = best_ordered_plan.cost;
        const SORT_COST_PER_ROW_MULTIPLIER: f64 = 0.001;
        let sorting_penalty =
            Cost(best_plan.output_cardinality as f64 * SORT_COST_PER_ROW_MULTIPLIER);
        if best_unordered_plan_cost + sorting_penalty > best_ordered_plan_cost {
            best_ordered_plan
        } else {
            best_plan
        }
    } else {
        best_plan
    };

    let mut sort_eliminated = false;

    // Eliminate sorting if possible.
    if let Some(order_target) = maybe_order_target.as_ref() {
        let satisfies_order_target = plan_satisfies_order_target(
            &best_plan,
            &access_methods_arena,
            table_references.joined_tables_mut(),
            order_target,
        );
        if satisfies_order_target {
            match order_target.1 {
                EliminatesSortBy::Group => {
                    let _ = group_by.as_mut().and_then(|g| g.sort_order.take());
                }
                EliminatesSortBy::Order => {
                    order_by.clear();
                }
                EliminatesSortBy::GroupByAndOrder => {
                    let _ = group_by.as_mut().and_then(|g| g.sort_order.take());
                    order_by.clear();
                }
            }
        }
        sort_eliminated = satisfies_order_target;
    }

    let (best_access_methods, best_table_numbers) = (
        best_plan.best_access_methods().collect::<Vec<_>>(),
        best_plan.table_numbers().collect::<Vec<_>>(),
    );

    // Collect hash join build/probe table indices. Build tables are excluded from the main
    // join order because they are consumed during hash build. A table may appear as both
    // probe and build (probe->build chaining) only when the build input is materialized.
    let (hash_join_build_tables, hash_join_probe_tables): (Vec<usize>, Vec<usize>) =
        best_access_methods
            .iter()
            .filter_map(|&am_idx| {
                let arena = &access_methods_arena;
                arena.get(am_idx).and_then(|am| {
                    if let AccessMethodParams::HashJoin {
                        build_table_idx,
                        probe_table_idx,
                        ..
                    } = &am.params
                    {
                        Some((*build_table_idx, *probe_table_idx))
                    } else {
                        None
                    }
                })
            })
            .unzip();
    #[cfg(debug_assertions)]
    {
        let mut probe_tables: HashSet<usize> = HashSet::default();
        let mut build_tables: HashMap<usize, bool> = HashMap::default();
        let mut pos_by_table: Vec<Option<usize>> =
            vec![None; table_references.joined_tables().len()];
        for (pos, table_idx) in best_table_numbers.iter().enumerate() {
            pos_by_table[*table_idx] = Some(pos);
        }

        for &am_idx in best_access_methods.iter() {
            let arena = &access_methods_arena;
            let Some(am) = arena.get(am_idx) else {
                continue;
            };
            if let AccessMethodParams::HashJoin {
                build_table_idx,
                probe_table_idx,
                materialize_build_input,
                ..
            } = &am.params
            {
                if let (Some(build_pos), Some(probe_pos)) = (
                    pos_by_table[*build_table_idx],
                    pos_by_table[*probe_table_idx],
                ) {
                    crate::turso_assert!(
                        probe_pos == build_pos + 1,
                        "hash join build/probe tables are not adjacent in join order"
                    );
                }
                probe_tables.insert(*probe_table_idx);
                build_tables.insert(*build_table_idx, *materialize_build_input);
            }
        }

        for (build_table_idx, materialize_build_input) in build_tables {
            if probe_tables.contains(&build_table_idx) {
                crate::turso_assert!(
                    materialize_build_input,
                    "probe->build chaining requires materialized build input"
                );
            }
        }
    }
    let hash_join_build_only_tables: HashSet<usize> = hash_join_build_tables
        .iter()
        .copied()
        .filter(|table_idx| !hash_join_probe_tables.contains(table_idx))
        .collect();

    let best_join_order: Vec<JoinOrderMember> = best_table_numbers
        .iter()
        .filter(|table_number| {
            !hash_join_build_tables.contains(table_number)
                || hash_join_probe_tables.contains(table_number)
        })
        .map(|&table_number| JoinOrderMember {
            table_id: table_references.joined_tables_mut()[table_number].internal_id,
            original_idx: table_number,
            is_outer: table_references.joined_tables_mut()[table_number]
                .join_info
                .as_ref()
                .is_some_and(|join_info| join_info.outer),
        })
        .collect();

    // Mutate the Operations in `joined_tables` to use the selected access methods.
    // We iterate over ALL tables (including hash join build tables) to set their operations,
    // even though build tables are not in best_join_order.
    for (i, &table_idx) in best_table_numbers.iter().enumerate() {
        // Skip tables that already have an IndexMethodQuery operation set.
        // This happens when the first table was optimized with a custom index (e.g., FTS)
        // and we're continuing to optimize remaining tables in a multi-table query.
        if matches!(
            table_references.joined_tables()[table_idx].op,
            Operation::IndexMethodQuery(_)
        ) {
            continue;
        }
        let access_method = &mut access_methods_arena[best_access_methods[i]];
        match &mut access_method.params {
            AccessMethodParams::BTreeTable {
                iter_dir,
                index,
                constraint_refs,
            } => {
                maybe_remove_index_candidate(
                    index,
                    &table_references.joined_tables()[table_idx],
                    maybe_order_target.as_ref(),
                    sort_eliminated,
                );
                if constraint_refs.is_empty() {
                    let is_leftmost_table = i == 0;
                    let uses_index = index.is_some();
                    let try_to_build_ephemeral_index = !is_leftmost_table && !uses_index;

                    if !try_to_build_ephemeral_index {
                        table_references.joined_tables_mut()[table_idx].op =
                            Operation::Scan(Scan::BTreeTable {
                                iter_dir: *iter_dir,
                                index: index.clone(),
                            });
                        continue;
                    }
                    // This branch means we have a full table scan for a non-outermost table.
                    // Try to construct an ephemeral index since it's going to be better than a scan.
                    let table_id = table_references.joined_tables()[table_idx].internal_id;
                    let table_constraints = constraints_per_table
                        .iter()
                        .find(|c| c.table_id == table_id);
                    let Some(table_constraints) = table_constraints else {
                        table_references.joined_tables_mut()[table_idx].op =
                            Operation::Scan(Scan::BTreeTable {
                                iter_dir: *iter_dir,
                                index: index.clone(),
                            });
                        continue;
                    };
                    // Ephemeral indexes mirror rowid/column lookups. If the constraint targets an
                    // expression (table_col_pos == None) we cannot derive a seek key that matches
                    // the row layout, so fall back to a scan in that situation.
                    let usable: Vec<(usize, &Constraint)> = table_constraints
                        .constraints
                        .iter()
                        .enumerate()
                        .filter(|(_, c)| c.usable && c.table_col_pos.is_some())
                        .collect();
                    // Find this table's position in best_join_order (which excludes build tables)
                    let join_order_pos = best_join_order
                        .iter()
                        .position(|m| m.original_idx == table_idx)
                        .unwrap_or_else(|| best_join_order.len().saturating_sub(1));

                    // Build a mapping from table_col_pos to index_col_pos.
                    // Multiple constraints on the same column should share the same index_col_pos.
                    //
                    // This is important when a column appears in multiple constraints.
                    // For example, in:
                    //   SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a AND t1.c = t2.c WHERE t2.a = 17
                    //
                    // The constraints on t2 are:
                    //   t2.a = t1.a (from ON clause)
                    //   t2.c = t1.c (from ON clause)
                    //   t2.a = 17   (from WHERE clause)
                    //
                    // Both t2.a constraints must map to index_col_pos=0. If we incorrectly
                    // assigned sequential index positions (0, 1, 2), the seek key would include
                    // 3 components but the ephemeral index only has 2 key columns (t2.a, t2.c),
                    // causing the seek to compare against the wrong columns and return no results.
                    let mut unique_col_positions: Vec<usize> = usable
                        .iter()
                        .map(|(_, c)| c.table_col_pos.expect("table_col_pos was Some above"))
                        .collect();
                    unique_col_positions.sort_unstable();
                    unique_col_positions.dedup();
                    // Map each usable constraint to a ConstraintRef.
                    // Multiple constraints with the same table_col_pos share the same index_col_pos.
                    let mut temp_constraint_refs: Vec<ConstraintRef> = usable
                        .iter()
                        .map(|(orig_idx, c)| {
                            let table_col_pos =
                                c.table_col_pos.expect("table_col_pos was Some above");
                            let index_col_pos = unique_col_positions
                                .binary_search(&table_col_pos)
                                .expect("table_col_pos must exist in unique_col_positions");
                            ConstraintRef {
                                constraint_vec_pos: *orig_idx, // index in the original constraints vec
                                index_col_pos,
                                sort_order: SortOrder::Asc,
                            }
                        })
                        .collect();

                    temp_constraint_refs.sort_by_key(|x| x.index_col_pos);
                    let usable_constraint_refs = usable_constraints_for_join_order(
                        &table_constraints.constraints,
                        &temp_constraint_refs,
                        &best_join_order[..=join_order_pos],
                    );

                    if usable_constraint_refs.is_empty() {
                        table_references.joined_tables_mut()[table_idx].op =
                            Operation::Scan(Scan::BTreeTable {
                                iter_dir: *iter_dir,
                                index: index.clone(),
                            });
                        continue;
                    }
                    let ephemeral_index = ephemeral_index_build(
                        &table_references.joined_tables_mut()[table_idx],
                        &usable_constraint_refs,
                    );

                    let ephemeral_index = Arc::new(ephemeral_index);
                    table_references.joined_tables_mut()[table_idx].op =
                        Operation::Search(Search::Seek {
                            index: Some(ephemeral_index),
                            seek_def: build_seek_def_from_constraints(
                                &table_constraints.constraints,
                                &usable_constraint_refs,
                                *iter_dir,
                                where_clause,
                                Some(table_references),
                            )?,
                        });
                } else {
                    let is_outer_join = table_references.joined_tables_mut()[table_idx]
                        .join_info
                        .as_ref()
                        .is_some_and(|join_info| join_info.outer);
                    // Build-only hash-join tables do not have a main-loop cursor,
                    // so leave cross-table constraints for probe-side evaluation.
                    let defer_cross_table_constraints =
                        hash_join_build_only_tables.contains(&table_idx);
                    for cref in constraint_refs.iter() {
                        for constraint_vec_pos in &[cref.eq, cref.lower_bound, cref.upper_bound] {
                            let Some(constraint_vec_pos) = constraint_vec_pos else {
                                continue;
                            };
                            let constraint =
                                &constraints_per_table[table_idx].constraints[*constraint_vec_pos];
                            let where_term = &mut where_clause[constraint.where_clause_pos.0];
                            assert!(
                                !where_term.consumed,
                                "trying to consume a where clause term twice: {where_term:?}",
                            );
                            if is_outer_join && where_term.from_outer_join.is_none() {
                                // Don't consume WHERE terms from outer joins if the where term is not part of the outer join condition. Consider:
                                // - SELECT * FROM t1 LEFT JOIN t2 ON false WHERE t2.id = 5
                                // - there is no row in t2 where t2.id = 5
                                // This should never produce any rows with null columns for t2 (because NULL != 5), but if we consume 't2.id = 5' to use it as a seek key,
                                // this will cause a null row to be emitted for EVERY row of t1.
                                // Note: in most cases like this, the LEFT JOIN could just be converted into an INNER JOIN (because e.g. t2.id=5 statically excludes any null rows),
                                // but that optimization should not be done here - it should be done before the join order optimization happens.
                                continue;
                            }
                            if defer_cross_table_constraints && !constraint.lhs_mask.is_empty() {
                                continue;
                            }
                            where_term.consumed = true;
                        }
                    }
                    if let Some(index) = &index {
                        table_references.joined_tables_mut()[table_idx].op =
                            Operation::Search(Search::Seek {
                                index: Some(index.clone()),
                                seek_def: build_seek_def_from_constraints(
                                    &constraints_per_table[table_idx].constraints,
                                    constraint_refs,
                                    *iter_dir,
                                    where_clause,
                                    Some(table_references),
                                )?,
                            });
                        continue;
                    }
                    assert!(
                        constraint_refs.len() == 1,
                        "expected exactly one constraint for rowid seek, got {constraint_refs:?}"
                    );
                    table_references.joined_tables_mut()[table_idx].op =
                        if let Some(eq) = constraint_refs[0].eq {
                            Operation::Search(Search::RowidEq {
                                cmp_expr: constraints_per_table[table_idx].constraints[eq]
                                    .get_constraining_expr(where_clause, Some(table_references))
                                    .1,
                            })
                        } else {
                            Operation::Search(Search::Seek {
                                index: None,
                                seek_def: build_seek_def_from_constraints(
                                    &constraints_per_table[table_idx].constraints,
                                    constraint_refs,
                                    *iter_dir,
                                    where_clause,
                                    Some(table_references),
                                )?,
                            })
                        };
                }
            }
            AccessMethodParams::VirtualTable {
                idx_num,
                idx_str,
                constraints,
                constraint_usages,
            } => {
                table_references.joined_tables_mut()[table_idx].op = build_vtab_scan_op(
                    where_clause,
                    &constraints_per_table[table_idx],
                    idx_num,
                    idx_str,
                    constraints,
                    constraint_usages,
                    Some(table_references),
                )?;
            }
            AccessMethodParams::Subquery => {
                table_references.joined_tables_mut()[table_idx].op =
                    Operation::Scan(Scan::Subquery);
            }
            AccessMethodParams::HashJoin {
                build_table_idx,
                probe_table_idx,
                join_keys,
                mem_budget,
                materialize_build_input,
                use_bloom_filter,
            } => {
                // Mark WHERE clause terms as consumed since we're using hash join
                for join_key in join_keys.iter() {
                    where_clause[join_key.where_clause_idx].consumed = true;
                }
                // Set up hash join operation on the probe table
                table_references.joined_tables_mut()[table_idx].op =
                    Operation::HashJoin(HashJoinOp {
                        build_table_idx: *build_table_idx,
                        probe_table_idx: *probe_table_idx,
                        join_keys: join_keys.clone(),
                        mem_budget: *mem_budget,
                        materialize_build_input: *materialize_build_input,
                        use_bloom_filter: *use_bloom_filter,
                    });
            }
            AccessMethodParams::IndexMethod {
                query,
                where_covered,
            } => {
                // Mark WHERE clause term as consumed if the index method covered it
                if let Some(idx) = where_covered {
                    where_clause[*idx].consumed = true;
                }
                // Set up the index method query operation
                table_references.joined_tables_mut()[table_idx].op =
                    Operation::IndexMethodQuery(query.clone());
            }
        }
    }

    let mut probe_pos_by_table: Vec<Option<usize>> =
        vec![None; table_references.joined_tables().len()];
    for (pos, member) in best_join_order.iter().enumerate() {
        let table = &table_references.joined_tables()[member.original_idx];
        if matches!(table.op, Operation::HashJoin(_)) {
            probe_pos_by_table[member.original_idx] = Some(pos);
        }
    }

    // If hash-join build constraints are still evaluated later (not consumed),
    // avoid materializing the build input to reduce redundant scans.
    for table in table_references.joined_tables_mut().iter_mut() {
        let Operation::HashJoin(hash_join_op) = &mut table.op else {
            continue;
        };
        if !hash_join_op.materialize_build_input {
            continue;
        }
        let Some(probe_pos) = best_join_order
            .iter()
            .position(|member| member.original_idx == hash_join_op.probe_table_idx)
        else {
            continue;
        };
        let build_table_was_prior_probe = probe_pos_by_table
            .get(hash_join_op.build_table_idx)
            .copied()
            .flatten()
            .is_some_and(|pos| pos < probe_pos);
        if build_table_was_prior_probe {
            continue;
        }
        let prior_mask = TableMask::from_table_number_iter(
            best_join_order[..probe_pos]
                .iter()
                .map(|member| member.original_idx),
        );
        let join_key_indices: HashSet<usize> = hash_join_op
            .join_keys
            .iter()
            .map(|key| key.where_clause_idx)
            .collect();
        let build_constraints = &constraints_per_table[hash_join_op.build_table_idx];
        let mut has_prior_constraints = false;
        for constraint in build_constraints.constraints.iter() {
            if !constraint.lhs_mask.intersects(&prior_mask) {
                continue;
            }
            if join_key_indices.contains(&constraint.where_clause_pos.0) {
                continue;
            }
            has_prior_constraints = true;
            break;
        }
        if !has_prior_constraints {
            hash_join_op.materialize_build_input = false;
        }
    }

    Ok(Some(best_join_order))
}

fn build_vtab_scan_op(
    where_clause: &mut [WhereTerm],
    table_constraints: &TableConstraints,
    idx_num: &i32,
    idx_str: &Option<String>,
    vtab_constraints: &[ConstraintInfo],
    constraint_usages: &[ConstraintUsage],
    referenced_tables: Option<&TableReferences>,
) -> Result<Operation> {
    if constraint_usages.len() != vtab_constraints.len() {
        return Err(LimboError::ExtensionError(format!(
            "Constraint usage count mismatch (expected {}, got {})",
            vtab_constraints.len(),
            constraint_usages.len()
        )));
    }

    let mut constraints = vec![None; constraint_usages.len()];
    let mut arg_count = 0;

    for (i, vtab_constraint) in vtab_constraints.iter().enumerate() {
        let usage = constraint_usages[i];
        let argv_index = match usage.argv_index {
            Some(idx) if idx >= 1 && (idx as usize) <= constraint_usages.len() => idx,
            Some(idx) => {
                return Err(LimboError::ExtensionError(format!(
                    "argv_index {} is out of valid range [1..{}]",
                    idx,
                    constraint_usages.len()
                )));
            }
            None => continue,
        };

        let zero_based_argv_index = (argv_index - 1) as usize;
        if constraints[zero_based_argv_index].is_some() {
            return Err(LimboError::ExtensionError(format!(
                "duplicate argv_index {argv_index}"
            )));
        }

        let constraint = &table_constraints.constraints[vtab_constraint.index];
        if usage.omit {
            where_clause[constraint.where_clause_pos.0].consumed = true;
        }
        let (_, expr, _) = constraint.get_constraining_expr(where_clause, referenced_tables);
        constraints[zero_based_argv_index] = Some(expr);
        arg_count += 1;
    }

    // Verify that used indices form a contiguous sequence starting from 1
    let constraints = constraints
        .into_iter()
        .take(arg_count)
        .enumerate()
        .map(|(i, c)| {
            c.ok_or_else(|| {
                LimboError::ExtensionError(format!(
                    "argv_index values must form contiguous sequence starting from 1, missing index {}",
                    i + 1
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Operation::Scan(Scan::VirtualTable {
        idx_num: *idx_num,
        idx_str: idx_str.clone(),
        constraints,
    }))
}

#[derive(Debug, PartialEq, Clone)]
enum ConstantConditionEliminationResult {
    Continue,
    ImpossibleCondition,
}

/// Removes predicates that are always true.
/// Returns a ConstantEliminationResult indicating whether any predicates are always false.
/// This is used to determine whether the query can be aborted early.
fn eliminate_constant_conditions(
    where_clause: &mut [WhereTerm],
) -> Result<ConstantConditionEliminationResult> {
    let mut i = 0;
    while i < where_clause.len() {
        let predicate = &where_clause[i];
        if predicate.expr.is_always_true()? {
            // true predicates can be removed since they don't affect the result
            where_clause[i].consumed = true;
            i += 1;
        } else if predicate.expr.is_always_false()? {
            // any false predicate in a list of conjuncts (AND-ed predicates) will make the whole list false,
            // except an outer join condition, because that just results in NULLs, not skipping the whole loop
            if predicate.from_outer_join.is_some() {
                i += 1;
                continue;
            }
            where_clause
                .iter_mut()
                .for_each(|term| term.consumed = true);
            return Ok(ConstantConditionEliminationResult::ImpossibleCondition);
        } else {
            i += 1;
        }
    }

    Ok(ConstantConditionEliminationResult::Continue)
}

/// Check if the order by collation matches the index columns collations.
/// Only remove the index if sort was eliminated.
fn maybe_remove_index_candidate(
    index: &mut Option<Arc<Index>>,
    table_reference: &JoinedTable,
    order_target: Option<&OrderTarget>,
    sort_eliminated: bool,
) {
    if !sort_eliminated {
        return;
    }
    if let Some((idx, order_target)) = index.as_mut().zip(order_target) {
        for col_order in &order_target.0 {
            // Only check columns from this table
            if col_order.table_id != table_reference.internal_id {
                continue;
            }

            // Find matching index column
            let matching_idx_col = match &col_order.target {
                ColumnTarget::Column(col_no) => {
                    idx.columns.iter().find(|ic| ic.pos_in_table == *col_no)
                }
                ColumnTarget::RowId => {
                    continue;
                }
                ColumnTarget::Expr(_expr) => {
                    continue;
                }
            };

            if let Some(idx_col) = matching_idx_col {
                let idx_collation = idx_col.collation;

                // If ORDER BY collation doesn't match index collation, this index can't satisfy the ordering
                if idx_collation.is_some_and(|idx_collation| col_order.collation != idx_collation)
                    && sort_eliminated
                {
                    *index = None;
                    return;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlwaysTrueOrFalse {
    AlwaysTrue,
    AlwaysFalse,
}

/**
  Helper trait for expressions that can be optimized
  Implemented for ast::Expr
*/
pub trait Optimizable {
    // if the expression is a constant expression that, when evaluated as a condition, is always true or false
    // return a [ConstantPredicate].
    fn check_always_true_or_false(&self) -> Result<Option<AlwaysTrueOrFalse>>;
    fn is_always_true(&self) -> Result<bool> {
        Ok(self.check_always_true_or_false()? == Some(AlwaysTrueOrFalse::AlwaysTrue))
    }
    fn is_always_false(&self) -> Result<bool> {
        Ok(self.check_always_true_or_false()? == Some(AlwaysTrueOrFalse::AlwaysFalse))
    }
    fn is_constant(&self, resolver: &Resolver<'_>) -> bool;
    fn is_nonnull(&self, tables: &TableReferences) -> bool;
}

impl Optimizable for ast::Expr {
    /// Returns true if the expressions is (verifiably) non-NULL.
    /// It might still be non-NULL even if we return false; we just
    /// weren't able to prove it.
    /// This function is currently very conservative, and will return false
    /// for any expression where we aren't sure and didn't bother to find out
    /// by writing more complex code.
    fn is_nonnull(&self, tables: &TableReferences) -> bool {
        match self {
            Expr::SubqueryResult { .. } => false,
            Expr::Between {
                lhs, start, end, ..
            } => lhs.is_nonnull(tables) && start.is_nonnull(tables) && end.is_nonnull(tables),
            Expr::Binary(_, ast::Operator::Modulus | ast::Operator::Divide, _) => false, // 1 % 0, 1 / 0
            Expr::Binary(expr, _, expr1) => expr.is_nonnull(tables) && expr1.is_nonnull(tables),
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
                ..
            } => {
                base.as_ref().is_none_or(|base| base.is_nonnull(tables))
                    && when_then_pairs
                        .iter()
                        .all(|(_, then)| then.is_nonnull(tables))
                    && else_expr
                        .as_ref()
                        .is_none_or(|else_expr| else_expr.is_nonnull(tables))
            }
            Expr::Cast { expr, .. } => expr.is_nonnull(tables),
            Expr::Collate(expr, _) => expr.is_nonnull(tables),
            Expr::DoublyQualified(..) => {
                panic!("Do not call is_nonnull before DoublyQualified has been rewritten as Column")
            }
            Expr::Exists(..) => false,
            Expr::FunctionCall { .. } => false,
            Expr::FunctionCallStar { .. } => false,
            Expr::Id(..) => panic!("Do not call is_nonnull before Id has been rewritten as Column"),
            Expr::Column {
                table,
                column,
                is_rowid_alias,
                ..
            } => {
                if *is_rowid_alias {
                    return true;
                }

                let (_, table_ref) = tables
                    .find_table_by_internal_id(*table)
                    .expect("table not found");
                let columns = table_ref.columns();
                let column = &columns[*column];
                // Only INTEGER PRIMARY KEY (rowid alias) is implicitly NOT NULL.
                // Other PRIMARY KEY types (e.g., TEXT PRIMARY KEY) can contain NULL.
                column.is_rowid_alias() || column.notnull()
            }
            Expr::RowId { .. } => true,
            Expr::InList { lhs, rhs, .. } => {
                lhs.is_nonnull(tables) && rhs.is_empty() || rhs.iter().all(|v| v.is_nonnull(tables))
            }
            Expr::InSelect { .. } => false,
            Expr::InTable { .. } => false,
            Expr::IsNull(..) => true,
            Expr::Like { lhs, rhs, .. } => lhs.is_nonnull(tables) && rhs.is_nonnull(tables),
            Expr::Literal(literal) => match literal {
                ast::Literal::Numeric(_) => true,
                ast::Literal::String(_) => true,
                ast::Literal::Blob(_) => true,
                ast::Literal::Keyword(_) => true,
                ast::Literal::Null => false,
                ast::Literal::True => true,
                ast::Literal::False => true,
                ast::Literal::CurrentDate => true,
                ast::Literal::CurrentTime => true,
                ast::Literal::CurrentTimestamp => true,
            },
            Expr::Name(..) => false,
            Expr::NotNull(..) => true,
            Expr::Parenthesized(exprs) => exprs.iter().all(|expr| expr.is_nonnull(tables)),
            Expr::Qualified(..) => {
                panic!("Do not call is_nonnull before Qualified has been rewritten as Column")
            }
            Expr::Raise(..) => false,
            Expr::Subquery(..) => false,
            Expr::Unary(_, expr) => expr.is_nonnull(tables),
            Expr::Variable(..) => false,
            Expr::Register(..) => false, // Register values can be null
        }
    }
    /// Returns true if the expression is a constant i.e. does not depend on columns and can be evaluated only once during the execution
    fn is_constant(&self, resolver: &Resolver<'_>) -> bool {
        match self {
            Expr::SubqueryResult { .. } => false,
            Expr::Between {
                lhs, start, end, ..
            } => {
                lhs.is_constant(resolver)
                    && start.is_constant(resolver)
                    && end.is_constant(resolver)
            }
            Expr::Binary(expr, _, expr1) => {
                expr.is_constant(resolver) && expr1.is_constant(resolver)
            }
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                base.as_ref().is_none_or(|base| base.is_constant(resolver))
                    && when_then_pairs.iter().all(|(when, then)| {
                        when.is_constant(resolver) && then.is_constant(resolver)
                    })
                    && else_expr
                        .as_ref()
                        .is_none_or(|else_expr| else_expr.is_constant(resolver))
            }
            Expr::Cast { expr, .. } => expr.is_constant(resolver),
            Expr::Collate(expr, _) => expr.is_constant(resolver),
            Expr::DoublyQualified(_, _, _) => {
                panic!("DoublyQualified should have been rewritten as Column")
            }
            Expr::Exists(_) => false,
            Expr::FunctionCall { args, name, .. } => {
                let Some(func) = resolver.resolve_function(name.as_str(), args.len()) else {
                    return false;
                };
                func.is_deterministic() && args.iter().all(|arg| arg.is_constant(resolver))
            }
            Expr::FunctionCallStar { .. } => false,
            Expr::Id(_) => true,
            Expr::Column { .. } => false,
            Expr::RowId { .. } => false,
            Expr::InList { lhs, rhs, .. } => {
                lhs.is_constant(resolver) && rhs.is_empty()
                    || rhs.iter().all(|v| v.is_constant(resolver))
            }
            Expr::InSelect { .. } => {
                false // might be constant, too annoying to check subqueries etc. implement later
            }
            Expr::InTable { .. } => false,
            Expr::IsNull(expr) => expr.is_constant(resolver),
            Expr::Like {
                lhs, rhs, escape, ..
            } => {
                lhs.is_constant(resolver)
                    && rhs.is_constant(resolver)
                    && escape
                        .as_ref()
                        .is_none_or(|escape| escape.is_constant(resolver))
            }
            Expr::Literal(_) => true,
            Expr::Name(_) => false,
            Expr::NotNull(expr) => expr.is_constant(resolver),
            Expr::Parenthesized(exprs) => exprs.iter().all(|expr| expr.is_constant(resolver)),
            Expr::Qualified(_, _) => {
                panic!("Qualified should have been rewritten as Column")
            }
            Expr::Raise(_, expr) => expr.as_ref().is_none_or(|expr| expr.is_constant(resolver)),
            Expr::Subquery(_) => false,
            Expr::Unary(_, expr) => expr.is_constant(resolver),
            Expr::Variable(_) => true,
            Expr::Register(_) => false,
        }
    }
    /// Returns true if the expression is a constant expression that, when evaluated as a condition, is always true or false
    fn check_always_true_or_false(&self) -> Result<Option<AlwaysTrueOrFalse>> {
        match self {
            Self::Literal(lit) => match lit {
                ast::Literal::Numeric(b) => {
                    if let Ok(int_value) = b.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }
                    if let Ok(float_value) = b.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }

                    Ok(None)
                }
                ast::Literal::String(s) => {
                    // Use Numeric::from to match SQLite's string-to-numeric conversion,
                    // which extracts leading numeric prefixes (e.g., '9S' -> 9, 'abc' -> 0)
                    let without_quotes = s.trim_matches('\'');
                    let numeric = Numeric::from(without_quotes);
                    match numeric.try_into_bool() {
                        Some(true) => Ok(Some(AlwaysTrueOrFalse::AlwaysTrue)),
                        Some(false) => Ok(Some(AlwaysTrueOrFalse::AlwaysFalse)),
                        None => Ok(None),
                    }
                }
                _ => Ok(None),
            },
            Self::Unary(op, expr) => {
                if *op == ast::UnaryOperator::Not {
                    let trivial = expr.check_always_true_or_false()?;
                    return Ok(trivial.map(|t| match t {
                        AlwaysTrueOrFalse::AlwaysTrue => AlwaysTrueOrFalse::AlwaysFalse,
                        AlwaysTrueOrFalse::AlwaysFalse => AlwaysTrueOrFalse::AlwaysTrue,
                    }));
                }

                if *op == ast::UnaryOperator::Negative {
                    let trivial = expr.check_always_true_or_false()?;
                    return Ok(trivial);
                }

                Ok(None)
            }
            Self::InList { lhs: _, not, rhs } => {
                if rhs.is_empty() {
                    return Ok(Some(if *not {
                        AlwaysTrueOrFalse::AlwaysTrue
                    } else {
                        AlwaysTrueOrFalse::AlwaysFalse
                    }));
                }

                Ok(None)
            }
            Self::Binary(lhs, op, rhs) => {
                let lhs_trivial = lhs.check_always_true_or_false()?;
                let rhs_trivial = rhs.check_always_true_or_false()?;
                match op {
                    ast::Operator::And => {
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                            || rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysFalse));
                        }
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                            && rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysTrue));
                        }

                        Ok(None)
                    }
                    ast::Operator::Or => {
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                            || rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysTrue)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysTrue));
                        }
                        if lhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                            && rhs_trivial == Some(AlwaysTrueOrFalse::AlwaysFalse)
                        {
                            return Ok(Some(AlwaysTrueOrFalse::AlwaysFalse));
                        }

                        Ok(None)
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

fn ephemeral_index_build(
    table_reference: &JoinedTable,
    constraint_refs: &[RangeConstraintRef],
) -> Index {
    let mut ephemeral_columns: Vec<IndexColumn> = table_reference
        .columns()
        .iter()
        .enumerate()
        .map(|(i, c)| IndexColumn {
            name: c.name.clone().unwrap(),
            order: SortOrder::Asc,
            pos_in_table: i,
            collation: c.collation_opt(),
            default: c.default.clone(),
            expr: None,
        })
        // only include columns that are used in the query
        .filter(|c| table_reference.column_is_used(c.pos_in_table))
        .collect();
    // sort so that constraints first, then rest in whatever order they were in in the table
    ephemeral_columns.sort_by(|a, b| {
        let a_constraint = constraint_refs
            .iter()
            .enumerate()
            .find(|(_, c)| c.table_col_pos == Some(a.pos_in_table));
        let b_constraint = constraint_refs
            .iter()
            .enumerate()
            .find(|(_, c)| c.table_col_pos == Some(b.pos_in_table));
        match (a_constraint, b_constraint) {
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some((a_idx, _)), Some((b_idx, _))) => a_idx.cmp(&b_idx),
            (None, None) => Ordering::Equal,
        }
    });
    let ephemeral_index = Index {
        name: format!(
            "ephemeral_{}_{}",
            table_reference.table.get_name(),
            table_reference.internal_id
        ),
        columns: ephemeral_columns,
        unique: false,
        ephemeral: true,
        table_name: table_reference.table.get_name().to_string(),
        root_page: 0,
        where_clause: None,
        has_rowid: table_reference
            .table
            .btree()
            .is_some_and(|btree| btree.has_rowid),
        index_method: None,
    };

    ephemeral_index
}

/// Build a [SeekDef] for a given list of [Constraint]s
pub fn build_seek_def_from_constraints(
    constraints: &[Constraint],
    constraint_refs: &[RangeConstraintRef],
    iter_dir: IterationDirection,
    where_clause: &[WhereTerm],
    referenced_tables: Option<&TableReferences>,
) -> Result<SeekDef> {
    assert!(
        !constraint_refs.is_empty(),
        "cannot build seek def from empty list of constraint refs"
    );
    // Extract the key values and operators
    let key = constraint_refs
        .iter()
        .map(|cref| cref.as_seek_range_constraint(constraints, where_clause, referenced_tables))
        .collect();

    let seek_def = build_seek_def(iter_dir, key)?;
    Ok(seek_def)
}

/// Build a [SeekDef] for a given [SeekRangeConstraint] and [IterationDirection].
/// To be usable as a seek key, all but potentially the last term must be equalities.
/// The last term can be a nonequality (range with potentially one unbounded range).
///
/// There are two parts to the seek definition:
/// 1. start [SeekKey], which specifies the key that we will use to seek to the first row that matches the index key.
/// 2. end [SeekKey], which specifies the key that we will use to terminate the index scan that follows the seek.
///
/// There are some nuances to how, and which parts of, the index key can be used in the start and end [SeekKey]s,
/// depending on the operator and iteration order. This function explains those nuances inline when dealing with
/// each case.
///
/// But to illustrate the general idea, consider the following examples:
///
/// 1. For example, having two conditions like (x>10 AND y>20) cannot be used as a valid [SeekKey] GT(x:10, y:20)
///    because the first row greater than (x:10, y:20) might be (x:11, y:19), which does not satisfy the where clause.
///    In this case, only GT(x:10) must be used as the [SeekKey], and rows with y <= 20 must be filtered as a regular condition expression for each value of x.
///
/// 2. In contrast, having (x=10 AND y>20) forms a valid index key GT(x:10, y:20) because after the seek, we can simply terminate as soon as x > 10,
///    i.e. use GT(x:10, y:20) as the start [SeekKey] and GT(x:10) as the end.
///
/// The preceding examples are for an ascending index. The logic is similar for descending indexes, but an important distinction is that
/// since a descending index is laid out in reverse order, the comparison operators are reversed, e.g. LT becomes GT, LE becomes GE, etc.
/// So when you see e.g. a SeekOp::GT below for a descending index, it actually means that we are seeking the first row where the index key is LESS than the seek key.
///
fn build_seek_def(
    iter_dir: IterationDirection,
    mut key: Vec<SeekRangeConstraint>,
) -> Result<SeekDef> {
    assert!(!key.is_empty());
    let last = key.last().unwrap();

    // if we searching for exact key - emit definition immediately with prefix as a full key
    if last.eq.is_some() {
        let (start_op, end_op) = match iter_dir {
            IterationDirection::Forwards => (SeekOp::GE { eq_only: true }, SeekOp::GT),
            IterationDirection::Backwards => (SeekOp::LE { eq_only: true }, SeekOp::LT),
        };
        return Ok(SeekDef {
            prefix: key,
            iter_dir,
            start: SeekKey {
                last_component: SeekKeyComponent::None,
                op: start_op,
                affinity: Affinity::Blob,
            },
            end: SeekKey {
                last_component: SeekKeyComponent::None,
                op: end_op,
                affinity: Affinity::Blob,
            },
        });
    }
    assert!(last.lower_bound.is_some() || last.upper_bound.is_some());

    // pop last key as we will do some form of range search
    let last = key.pop().unwrap();

    // after that all key components must be equality constraints
    debug_assert!(key.iter().all(|k| k.eq.is_some()));

    // For the commented examples below, keep in mind that since a descending index is laid out in reverse order, the comparison operators are reversed, e.g. LT becomes GT, LE becomes GE, etc.
    // Also keep in mind that index keys are compared based on the number of columns given, so for example:
    // - if key is GT(x:10), then (x=10, y=usize::MAX) is not GT because only X is compared. (x=11, y=<any>) is GT.
    // - if key is GT(x:10, y:20), then (x=10, y=21) is GT because both X and Y are compared.
    // - if key is GT(x:10, y:NULL), then (x=10, y=0) is GT because NULL is always LT in index key comparisons.
    Ok(match iter_dir {
        IterationDirection::Forwards => {
            let (start, end) = match last.sort_order {
                SortOrder::Asc => {
                    let start = match last.lower_bound {
                        // Forwards, Asc, GT: (x=10 AND y>20)
                        // Start key: start from the first GT(x:10, y:20)
                        Some((ast::Operator::Greater, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                            affinity,
                        },
                        // Forwards, Asc, GE: (x=10 AND y>=20)
                        // Start key: start from the first GE(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                            affinity,
                        },
                        // Forwards, Asc, None, (x=10 AND y<30)
                        // Start key: start from the first GE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GE { eq_only: false },
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    let end = match last.upper_bound {
                        // Forwards, Asc, LT, (x=10 AND y<30)
                        // End key: end at first GE(x:10, y:30)
                        Some((ast::Operator::Less, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                            affinity,
                        },
                        // Forwards, Asc, LE, (x=10 AND y<=30)
                        // End key: end at first GT(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                            affinity,
                        },
                        // Forwards, Asc, None, (x=10 AND y>20)
                        // End key: end at first GT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GT,
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
                SortOrder::Desc => {
                    let start = match last.upper_bound {
                        // Forwards, Desc, LT: (x=10 AND y<30)
                        // Start key: start from the first GT(x:10, y:30)
                        Some((ast::Operator::Less, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                            affinity,
                        },
                        // Forwards, Desc, LE: (x=10 AND y<=30)
                        // Start key: start from the first GE(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                            affinity,
                        },
                        // Forwards, Desc, None: (x=10 AND y>20)
                        // Start key: start from the first GE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GE { eq_only: false },
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    let end = match last.lower_bound {
                        // Forwards, Asc, GT, (x=10 AND y>20)
                        // End key: end at first GE(x:10, y:20)
                        Some((ast::Operator::Greater, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                            affinity,
                        },
                        // Forwards, Asc, GE, (x=10 AND y>=20)
                        // End key: end at first GT(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                            affinity,
                        },
                        // Forwards, Asc, None, (x=10 AND y<30)
                        // End key: end at first GT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GT,
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
            };
            SeekDef {
                prefix: key,
                iter_dir,
                start,
                end,
            }
        }
        IterationDirection::Backwards => {
            let (start, end) = match last.sort_order {
                SortOrder::Asc => {
                    let start = match last.upper_bound {
                        // Backwards, Asc, LT: (x=10 AND y<30)
                        // Start key: start from the first LT(x:10, y:30)
                        Some((ast::Operator::Less, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                            affinity,
                        },
                        // Backwards, Asc, LT: (x=10 AND y<=30)
                        // Start key: start from the first LE(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                            affinity,
                        },
                        // Backwards, Asc, None: (x=10 AND y>20)
                        // Start key: start from the first LE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LE { eq_only: false },
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op)
                        }
                    };
                    let end = match last.lower_bound {
                        // Backwards, Asc, GT, (x=10 AND y>20)
                        // End key: end at first LE(x:10, y:20)
                        Some((ast::Operator::Greater, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                            affinity,
                        },
                        // Backwards, Asc, GT, (x=10 AND y>=20)
                        // End key: end at first LT(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                            affinity,
                        },
                        // Backwards, Asc, None, (x=10 AND y<30)
                        // End key: end at first LT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LT,
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
                SortOrder::Desc => {
                    let start = match last.lower_bound {
                        // Backwards, Desc, LT: (x=10 AND y>20)
                        // Start key: start from the first LT(x:10, y:20)
                        Some((ast::Operator::Greater, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                            affinity,
                        },
                        // Backwards, Desc, LE: (x=10 AND y>=20)
                        // Start key: start from the first LE(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                            affinity,
                        },
                        // Backwards, Desc, LE: (x=10 AND y<30)
                        // Start key: start from the first LE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LE { eq_only: false },
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    let end = match last.upper_bound {
                        // Backwards, Desc, LT, (x=10 AND y<30)
                        // End key: end at first LE(x:10, y:30)
                        Some((ast::Operator::Less, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                            affinity,
                        },
                        // Backwards, Desc, LT, (x=10 AND y<=30)
                        // End key: end at first LT(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound, affinity)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                            affinity,
                        },
                        // Backwards, Desc, LT, (x=10 AND y>20)
                        // End key: end at first LT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LT,
                            affinity: Affinity::Blob,
                        },
                        Some((op, _, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
            };
            SeekDef {
                prefix: key,
                iter_dir,
                start,
                end,
            }
        }
    })
}

pub trait TakeOwnership {
    fn take_ownership(&mut self) -> Self;
}

impl TakeOwnership for ast::Expr {
    fn take_ownership(&mut self) -> Self {
        std::mem::replace(self, ast::Expr::Literal(ast::Literal::Null))
    }
}
