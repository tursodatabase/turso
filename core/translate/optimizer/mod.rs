use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use constraints::{
    constraints_from_where_clause, usable_constraints_for_join_order, Constraint, ConstraintRef,
};
use cost::Cost;
use join::{compute_best_join_order, BestJoinOrderResult};
use lift_common_subexpressions::lift_common_subexpressions_from_binary_or_terms;
use order::{compute_order_target, plan_satisfies_order_target, EliminatesSortBy};
use turso_ext::{ConstraintInfo, ConstraintUsage};
use turso_parser::ast::{self, Expr, SortOrder};

use crate::{
    schema::{BTreeTable, Index, IndexColumn, Schema, Table, ROWID_SENTINEL},
    translate::{
        insert::ROWID_COLUMN,
        optimizer::{
            access_method::AccessMethodParams,
            constraints::{RangeConstraintRef, SeekRangeConstraint, TableConstraints},
        },
        plan::{
            ColumnUsedMask, IndexMethodQuery, NonFromClauseSubquery, OuterQueryReference,
            QueryDestination, ResultSetColumn, Scan, SeekKeyComponent,
        },
    },
    types::SeekOp,
    util::{
        exprs_are_equivalent, simple_bind_expr, try_capture_parameters, try_substitute_parameters,
    },
    vdbe::builder::{CursorKey, CursorType, ProgramBuilder},
    LimboError, Result,
};

use super::{
    emitter::Resolver,
    plan::{
        DeletePlan, GroupBy, IterationDirection, JoinOrderMember, JoinedTable, Operation, Plan,
        Search, SeekDef, SeekKey, SelectPlan, TableReferences, UpdatePlan, WhereTerm,
    },
};

pub(crate) mod access_method;
pub(crate) mod constraints;
pub(crate) mod cost;
pub(crate) mod join;
pub(crate) mod lift_common_subexpressions;
pub(crate) mod order;

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

/**
 * Make a few passes over the plan to optimize it.
 * TODO: these could probably be done in less passes,
 * but having them separate makes them easier to understand
 */
pub fn optimize_select_plan(plan: &mut SelectPlan, schema: &Schema) -> Result<()> {
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
    lift_common_subexpressions_from_binary_or_terms(&mut plan.where_clause)?;
    if let ConstantConditionEliminationResult::ImpossibleCondition =
        eliminate_constant_conditions(&mut plan.where_clause)?
    {
        plan.contains_constant_false_condition = true;
        return Ok(());
    }

    let _ = optimize_table_access(
        schema,
        &mut plan.result_columns,
        &mut plan.table_references,
        &schema.indexes,
        &mut plan.where_clause,
        &mut plan.order_by,
        &mut None,
        &[],
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
        &[],
        &mut plan.limit,
        &mut plan.offset,
    )?;

    let table_ref = &mut plan.table_references.joined_tables_mut()[0];

    // An ephemeral table is required if the UPDATE modifies any column that is present in the key of the
    // btree used to iterate over the table.
    // For regular table scans or seeks, this is just the rowid or the rowid alias column (INTEGER PRIMARY KEY)
    // For index scans and seeks, this is any column in the index used.
    let requires_ephemeral_table = 'requires: {
        let Some(btree_table) = table_ref.table.btree() else {
            break 'requires false;
        };
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

/// An ephemeral table is required if the UPDATE modifies any column that is present in the key of the
/// btree used to iterate over the table.
/// For regular table scans or seeks, the key is the rowid or the rowid alias column (INTEGER PRIMARY KEY).
/// For index scans and seeks, the key is any column in the index used.
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
        },
        join_order,
        offset: None,
        contains_constant_false_condition: false,
        distinctness: super::plan::Distinctness::NonDistinct,
        values: vec![],
        window: None,
        non_from_clause_subqueries: vec![],
    };

    plan.ephemeral_plan = Some(ephemeral_plan);

    Ok(())
}

fn optimize_subqueries(plan: &mut SelectPlan, schema: &Schema) -> Result<()> {
    for table in plan.table_references.joined_tables_mut() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table.table {
            optimize_select_plan(&mut from_clause_subquery.plan, schema)?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn optimize_table_access_with_custom_modules(
    schema: &Schema,
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
    assert_eq!(tables.len(), 1);

    // group by is not supported for now
    if group_by.is_some() {
        return Ok(false);
    }

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
        'pattern: for (pattern_idx, pattern) in definition.patterns.iter().enumerate() {
            let mut pattern = pattern.clone();
            assert!(pattern.with.is_none());
            assert!(pattern.body.compounds.is_empty());
            let ast::OneSelect::Select {
                columns,
                from: Some(ast::FromClause { select, joins }),
                distinctness: None,
                ref mut where_clause,
                group_by: None,
                window_clause,
            } = &mut pattern.body.select
            else {
                panic!("unexpected select pattern body");
            };
            assert!(window_clause.is_empty());
            assert!(joins.is_empty());
            let ast::SelectTable::Table(name, _, _) = select.as_ref() else {
                panic!("unexpected from clause");
            };

            for column in columns.iter_mut() {
                if let ast::ResultColumn::Expr(e, _) = column {
                    simple_bind_expr(schema, table, &[], e)?;
                }
            }
            for column in pattern.order_by.iter_mut() {
                simple_bind_expr(schema, table, columns, &mut column.expr)?;
            }
            if let Some(pattern_where) = where_clause {
                simple_bind_expr(schema, table, columns, pattern_where)?;
            }

            if name.name.as_str() != table.table.get_name() {
                continue;
            }
            if order_by.len() != pattern.order_by.len() {
                continue;
            }

            let mut where_query_covered = None;
            let mut parameters = HashMap::new();

            for (pattern_column, (query_column, query_order)) in
                pattern.order_by.iter().zip(order_by.iter())
            {
                if *query_order != pattern_column.order.unwrap_or(SortOrder::Asc) {
                    continue 'pattern;
                }
                let Some(captured) = try_capture_parameters(&pattern_column.expr, query_column)
                else {
                    continue 'pattern;
                };
                parameters.extend(captured);
            }
            match (pattern.limit.as_ref().map(|x| &x.expr), &limit) {
                (None, Some(_)) | (Some(_), None) => continue,
                (Some(pattern_limit), Some(query_limit)) => {
                    let Some(captured) = try_capture_parameters(pattern_limit, query_limit) else {
                        continue 'pattern;
                    };
                    parameters.extend(captured);
                }
                (None, None) => {}
            }
            match (
                pattern.limit.as_ref().and_then(|x| x.offset.as_ref()),
                &offset,
            ) {
                (None, Some(_)) | (Some(_), None) => continue,
                (Some(pattern_off), Some(query_off)) => {
                    let Some(captured) = try_capture_parameters(pattern_off, query_off) else {
                        continue 'pattern;
                    };
                    parameters.extend(captured);
                }
                (None, None) => {}
            }
            if let Some(pattern_where) = where_clause {
                for (i, query_where) in where_query.iter().enumerate() {
                    let captured = try_capture_parameters(pattern_where, &query_where.expr);
                    let Some(captured) = captured else {
                        continue;
                    };
                    parameters.extend(captured);
                    where_query_covered = Some(i);
                    break;
                }
            }

            if where_clause.is_some() && where_query_covered.is_none() {
                continue;
            }

            let where_covered_completely =
                where_query.is_empty() || where_query_covered.is_some() && where_query.len() == 1;

            if !where_covered_completely
                && (!order_by.is_empty() || limit.is_some() || offset.is_some())
            {
                continue;
            }

            if let Some(where_covered) = where_query_covered {
                where_query[where_covered].consumed = true;
            }

            // todo: fix this
            let mut covered_column_id = 1_000_000;
            let mut covered_columns = HashMap::new();
            for (patter_column_id, pattern_column) in columns.iter().enumerate() {
                let ast::ResultColumn::Expr(pattern, _) = pattern_column else {
                    continue;
                };
                let Some(pattern) = try_substitute_parameters(pattern, &parameters) else {
                    continue;
                };
                for query_column in result_columns.iter_mut() {
                    if !exprs_are_equivalent(&query_column.expr, &pattern) {
                        continue;
                    }
                    query_column.expr = ast::Expr::Column {
                        database: None,
                        table: table.internal_id,
                        column: covered_column_id,
                        is_rowid_alias: false,
                    };
                    covered_columns.insert(covered_column_id, patter_column_id);
                    covered_column_id += 1;
                }
            }
            let _ = order_by.drain(..);
            let _ = limit.take();
            let _ = offset.take();
            let mut arguments = parameters.iter().collect::<Vec<_>>();
            arguments.sort_by_key(|(&i, _)| i);

            table.op = Operation::IndexMethodQuery(IndexMethodQuery {
                index: index.clone(),
                pattern_idx,
                covered_columns,
                arguments: arguments.iter().map(|(_, e)| (*e).clone()).collect(),
            });
            return Ok(true);
        }
    }
    Ok(false)
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
    if table_references.joined_tables().len() > TableReferences::MAX_JOINED_TABLES {
        crate::bail_parse_error!(
            "Only up to {} tables can be joined",
            TableReferences::MAX_JOINED_TABLES
        );
    }

    if table_references.joined_tables().len() == 1 {
        let optimized = optimize_table_access_with_custom_modules(
            schema,
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

    let access_methods_arena = RefCell::new(Vec::new());
    let maybe_order_target = compute_order_target(order_by, group_by.as_mut());
    let constraints_per_table = constraints_from_where_clause(
        where_clause,
        table_references,
        available_indexes,
        subqueries,
    )?;

    // Currently the expressions we evaluate as constraints are binary expressions that will never be true for a NULL operand.
    // If there are any constraints on the right hand side table of an outer join that are not part of the outer join condition,
    // the outer join can be converted into an inner join.
    // for example:
    // - SELECT * FROM t1 LEFT JOIN t2 ON false WHERE t2.id = 5
    // there can never be a situation where null columns are emitted for t2 because t2.id = 5 will never be true in that case.
    // hence: we can convert the outer join into an inner join.
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
        if constraints_per_table[i]
            .constraints
            .iter()
            .any(|c| where_clause[c.where_clause_pos.0].from_outer_join.is_none())
        {
            t.join_info.as_mut().unwrap().outer = false;
            for term in where_clause.iter_mut() {
                if let Some(from_outer_join) = term.from_outer_join {
                    if from_outer_join == t.internal_id {
                        term.from_outer_join = None;
                    }
                }
            }
            continue;
        }
    }

    let Some(best_join_order_result) = compute_best_join_order(
        table_references.joined_tables_mut(),
        maybe_order_target.as_ref(),
        &constraints_per_table,
        &access_methods_arena,
    )?
    else {
        return Ok(None);
    };

    let BestJoinOrderResult {
        best_plan,
        best_ordered_plan,
    } = best_join_order_result;

    let joined_tables = table_references.joined_tables_mut();

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

    // Eliminate sorting if possible.
    if let Some(order_target) = maybe_order_target {
        let satisfies_order_target = plan_satisfies_order_target(
            &best_plan,
            &access_methods_arena,
            joined_tables,
            &order_target,
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
    }

    let (best_access_methods, best_table_numbers) = (
        best_plan.best_access_methods().collect::<Vec<_>>(),
        best_plan.table_numbers().collect::<Vec<_>>(),
    );

    let best_join_order: Vec<JoinOrderMember> = best_table_numbers
        .into_iter()
        .map(|table_number| JoinOrderMember {
            table_id: joined_tables[table_number].internal_id,
            original_idx: table_number,
            is_outer: joined_tables[table_number]
                .join_info
                .as_ref()
                .is_some_and(|join_info| join_info.outer),
        })
        .collect();

    // Mutate the Operations in `joined_tables` to use the selected access methods.
    for (i, join_order_member) in best_join_order.iter().enumerate() {
        let table_idx = join_order_member.original_idx;
        let access_method = &access_methods_arena.borrow()[best_access_methods[i]];

        match &access_method.params {
            AccessMethodParams::BTreeTable {
                iter_dir,
                index,
                constraint_refs,
            } => {
                if constraint_refs.is_empty() {
                    let try_to_build_ephemeral_index = if schema.indexes_enabled() {
                        let is_leftmost_table = i == 0;
                        let uses_index = index.is_some();
                        !is_leftmost_table && !uses_index
                    } else {
                        false
                    };

                    if !try_to_build_ephemeral_index {
                        joined_tables[table_idx].op = Operation::Scan(Scan::BTreeTable {
                            iter_dir: *iter_dir,
                            index: index.clone(),
                        });
                        continue;
                    }
                    // This branch means we have a full table scan for a non-outermost table.
                    // Try to construct an ephemeral index since it's going to be better than a scan.
                    let table_constraints = constraints_per_table
                        .iter()
                        .find(|c| c.table_id == join_order_member.table_id);
                    let Some(table_constraints) = table_constraints else {
                        joined_tables[table_idx].op = Operation::Scan(Scan::BTreeTable {
                            iter_dir: *iter_dir,
                            index: index.clone(),
                        });
                        continue;
                    };
                    let usable_constraints = table_constraints
                        .constraints
                        .iter()
                        .filter(|c| c.usable)
                        .cloned()
                        .collect::<Vec<_>>();
                    let mut temp_constraint_refs = (0..usable_constraints.len())
                        .map(|i| ConstraintRef {
                            constraint_vec_pos: i,
                            index_col_pos: i,
                            sort_order: SortOrder::Asc,
                        })
                        .collect::<Vec<_>>();
                    temp_constraint_refs.sort_by_key(|x| x.index_col_pos);

                    let usable_constraint_refs = usable_constraints_for_join_order(
                        &usable_constraints,
                        &temp_constraint_refs,
                        &best_join_order[..=i],
                    );
                    if usable_constraint_refs.is_empty() {
                        joined_tables[table_idx].op = Operation::Scan(Scan::BTreeTable {
                            iter_dir: *iter_dir,
                            index: index.clone(),
                        });
                        continue;
                    }
                    let ephemeral_index =
                        ephemeral_index_build(&joined_tables[table_idx], &usable_constraint_refs);
                    let ephemeral_index = Arc::new(ephemeral_index);
                    joined_tables[table_idx].op = Operation::Search(Search::Seek {
                        index: Some(ephemeral_index),
                        seek_def: build_seek_def_from_constraints(
                            &table_constraints.constraints,
                            &usable_constraint_refs,
                            *iter_dir,
                            where_clause,
                        )?,
                    });
                } else {
                    let is_outer_join = joined_tables[table_idx]
                        .join_info
                        .as_ref()
                        .is_some_and(|join_info| join_info.outer);
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
                            where_term.consumed = true;
                        }
                    }
                    if let Some(index) = &index {
                        joined_tables[table_idx].op = Operation::Search(Search::Seek {
                            index: Some(index.clone()),
                            seek_def: build_seek_def_from_constraints(
                                &constraints_per_table[table_idx].constraints,
                                constraint_refs,
                                *iter_dir,
                                where_clause,
                            )?,
                        });
                        continue;
                    }
                    assert!(
                        constraint_refs.len() == 1,
                        "expected exactly one constraint for rowid seek, got {constraint_refs:?}"
                    );
                    joined_tables[table_idx].op = if let Some(eq) = constraint_refs[0].eq {
                        Operation::Search(Search::RowidEq {
                            cmp_expr: constraints_per_table[table_idx].constraints[eq]
                                .get_constraining_expr(where_clause)
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
                joined_tables[table_idx].op = build_vtab_scan_op(
                    where_clause,
                    &constraints_per_table[table_idx],
                    idx_num,
                    idx_str,
                    constraints,
                    constraint_usages,
                )?;
            }
            AccessMethodParams::Subquery => {
                joined_tables[table_idx].op = Operation::Scan(Scan::Subquery);
            }
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
        let (_, expr) = constraint.get_constraining_expr(where_clause);
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
                column.primary_key() || column.notnull()
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
    /// Returns true if the expression is a constant i.e. does not depend on variables or columns etc.
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
            Expr::Variable(_) => false,
            Expr::Register(_) => false, // Register values are not constants
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
                    let without_quotes = s.trim_matches('\'');
                    if let Ok(int_value) = without_quotes.parse::<i64>() {
                        return Ok(Some(if int_value == 0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }

                    if let Ok(float_value) = without_quotes.parse::<f64>() {
                        return Ok(Some(if float_value == 0.0 {
                            AlwaysTrueOrFalse::AlwaysFalse
                        } else {
                            AlwaysTrueOrFalse::AlwaysTrue
                        }));
                    }

                    Ok(Some(AlwaysTrueOrFalse::AlwaysFalse))
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
        })
        // only include columns that are used in the query
        .filter(|c| table_reference.column_is_used(c.pos_in_table))
        .collect();
    // sort so that constraints first, then rest in whatever order they were in in the table
    ephemeral_columns.sort_by(|a, b| {
        let a_constraint = constraint_refs
            .iter()
            .enumerate()
            .find(|(_, c)| c.table_col_pos == a.pos_in_table);
        let b_constraint = constraint_refs
            .iter()
            .enumerate()
            .find(|(_, c)| c.table_col_pos == b.pos_in_table);
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
) -> Result<SeekDef> {
    assert!(
        !constraint_refs.is_empty(),
        "cannot build seek def from empty list of constraint refs"
    );
    // Extract the key values and operators
    let key = constraint_refs
        .iter()
        .map(|cref| cref.as_seek_range_constraint(constraints, where_clause))
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
            },
            end: SeekKey {
                last_component: SeekKeyComponent::None,
                op: end_op,
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
                        Some((ast::Operator::Greater, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                        },
                        // Forwards, Asc, GE: (x=10 AND y>=20)
                        // Start key: start from the first GE(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                        },
                        // Forwards, Asc, None, (x=10 AND y<30)
                        // Start key: start from the first GE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GE { eq_only: false },
                        },
                        Some((op, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    let end = match last.upper_bound {
                        // Forwards, Asc, LT, (x=10 AND y<30)
                        // End key: end at first GE(x:10, y:30)
                        Some((ast::Operator::Less, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                        },
                        // Forwards, Asc, LE, (x=10 AND y<=30)
                        // End key: end at first GT(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                        },
                        // Forwards, Asc, None, (x=10 AND y>20)
                        // End key: end at first GT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GT,
                        },
                        Some((op, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
                SortOrder::Desc => {
                    let start = match last.upper_bound {
                        // Forwards, Desc, LT: (x=10 AND y<30)
                        // Start key: start from the first GT(x:10, y:30)
                        Some((ast::Operator::Less, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                        },
                        // Forwards, Desc, LE: (x=10 AND y<=30)
                        // Start key: start from the first GE(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                        },
                        // Forwards, Desc, None: (x=10 AND y>20)
                        // Start key: start from the first GE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GE { eq_only: false },
                        },
                        Some((op, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    let end = match last.lower_bound {
                        // Forwards, Asc, GT, (x=10 AND y>20)
                        // End key: end at first GE(x:10, y:20)
                        Some((ast::Operator::Greater, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GE { eq_only: false },
                        },
                        // Forwards, Asc, GE, (x=10 AND y>=20)
                        // End key: end at first GT(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::GT,
                        },
                        // Forwards, Asc, None, (x=10 AND y<30)
                        // End key: end at first GT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::GT,
                        },
                        Some((op, _)) => {
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
                        Some((ast::Operator::Less, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                        },
                        // Backwards, Asc, LT: (x=10 AND y<=30)
                        // Start key: start from the first LE(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                        },
                        // Backwards, Asc, None: (x=10 AND y>20)
                        // Start key: start from the first LE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LE { eq_only: false },
                        },
                        Some((op, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op)
                        }
                    };
                    let end = match last.lower_bound {
                        // Backwards, Asc, GT, (x=10 AND y>20)
                        // End key: end at first LE(x:10, y:20)
                        Some((ast::Operator::Greater, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                        },
                        // Backwards, Asc, GT, (x=10 AND y>=20)
                        // End key: end at first LT(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                        },
                        // Backwards, Asc, None, (x=10 AND y<30)
                        // End key: end at first LT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LT,
                        },
                        Some((op, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    (start, end)
                }
                SortOrder::Desc => {
                    let start = match last.lower_bound {
                        // Backwards, Desc, LT: (x=10 AND y>20)
                        // Start key: start from the first LT(x:10, y:20)
                        Some((ast::Operator::Greater, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                        },
                        // Backwards, Desc, LE: (x=10 AND y>=20)
                        // Start key: start from the first LE(x:10, y:20)
                        Some((ast::Operator::GreaterEquals, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                        },
                        // Backwards, Desc, LE: (x=10 AND y<30)
                        // Start key: start from the first LE(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LE { eq_only: false },
                        },
                        Some((op, _)) => {
                            crate::bail_parse_error!("build_seek_def: invalid operator: {:?}", op,)
                        }
                    };
                    let end = match last.upper_bound {
                        // Backwards, Desc, LT, (x=10 AND y<30)
                        // End key: end at first LE(x:10, y:30)
                        Some((ast::Operator::Less, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LE { eq_only: false },
                        },
                        // Backwards, Desc, LT, (x=10 AND y<=30)
                        // End key: end at first LT(x:10, y:30)
                        Some((ast::Operator::LessEquals, bound)) => SeekKey {
                            last_component: SeekKeyComponent::Expr(bound),
                            op: SeekOp::LT,
                        },
                        // Backwards, Desc, LT, (x=10 AND y>20)
                        // End key: end at first LT(x:10)
                        None => SeekKey {
                            last_component: SeekKeyComponent::None,
                            op: SeekOp::LT,
                        },
                        Some((op, _)) => {
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
