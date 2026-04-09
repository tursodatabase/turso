use super::emitter::{emit_program, TranslateCtx};
use super::plan::{
    select_star, Distinctness, InSeekSource, JoinOrderMember, Operation, OuterQueryReference,
    QueryDestination, Search, TableReferences, WhereTerm, Window,
};
use crate::schema::Table;
use crate::sync::Arc;
use crate::translate::emitter::{OperationMode, Resolver};
use crate::translate::expr::{
    bind_and_rewrite_expr, expr_vector_size, walk_expr, BindingBehavior, WalkControl,
};
use crate::translate::group_by::compute_group_by_sort_order;
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{GroupBy, Plan, ResultSetColumn, SelectPlan, SubqueryState};
use crate::translate::planner::{
    break_predicate_at_and_boundaries, parse_from, parse_limit, parse_where,
    plan_ctes_as_outer_refs, resolve_window_and_aggregate_functions,
};
use crate::translate::result_row::emit_select_result;
use crate::translate::subquery::{plan_subqueries_from_select_plan, plan_subqueries_from_values};
use crate::translate::window::plan_windows;
use crate::util::{exprs_are_equivalent, normalize_ident};
use crate::vdbe::builder::ProgramBuilderOpts;
use crate::vdbe::insn::Insn;
use crate::{vdbe::builder::ProgramBuilder, Result};
use std::borrow::Cow;
use turso_parser::ast::ResultColumn;
use turso_parser::ast::{self, CompoundSelect, Expr};

/// Maximum number of columns in a result set.
/// SQLite's default SQLITE_MAX_COLUMN is 2000, with a hard upper limit of 32767.
const SQLITE_MAX_COLUMN: usize = 2000;

pub fn translate_select(
    select: ast::Select,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    query_destination: QueryDestination,
    connection: &Arc<crate::Connection>,
) -> Result<usize> {
    let mut select_plan = prepare_select_plan(
        select,
        resolver,
        program,
        &[],
        query_destination,
        connection,
    )?;
    if program.trigger.is_some() {
        if let Some(virtual_table) = plan_first_virtual_table_name(&select_plan) {
            crate::bail_parse_error!("unsafe use of virtual table \"{}\"", virtual_table);
        }
    }
    optimize_plan(program, &mut select_plan, resolver)?;
    let num_result_cols;
    let opts = match &select_plan {
        Plan::Select(select) => {
            num_result_cols = select.result_columns.len();
            ProgramBuilderOpts {
                num_cursors: count_required_cursors_for_simple_select(select),
                approx_num_insns: estimate_num_instructions_for_simple_select(select),
                approx_num_labels: estimate_num_labels_for_simple_select(select),
            }
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            // Compound Selects must return the same number of columns
            num_result_cols = right_most.result_columns.len();

            ProgramBuilderOpts {
                num_cursors: count_required_cursors_for_simple_select(right_most)
                    + left
                        .iter()
                        .map(|(plan, _)| count_required_cursors_for_simple_select(plan))
                        .sum::<usize>(),
                approx_num_insns: estimate_num_instructions_for_simple_select(right_most)
                    + left
                        .iter()
                        .map(|(plan, _)| estimate_num_instructions_for_simple_select(plan))
                        .sum::<usize>(),
                approx_num_labels: estimate_num_labels_for_simple_select(right_most)
                    + left
                        .iter()
                        .map(|(plan, _)| estimate_num_labels_for_simple_select(plan))
                        .sum::<usize>(),
            }
        }
        other => panic!("plan is not a SelectPlan: {other:?}"),
    };

    program.extend(&opts);
    emit_program(connection, resolver, program, select_plan, |_| {})?;
    Ok(num_result_cols)
}

fn plan_first_virtual_table_name(plan: &Plan) -> Option<String> {
    match plan {
        Plan::Select(select_plan) => select_plan_first_virtual_table_name(select_plan),
        Plan::CompoundSelect {
            left, right_most, ..
        } => select_plan_first_virtual_table_name(right_most).or_else(|| {
            left.iter()
                .find_map(|(plan, _)| select_plan_first_virtual_table_name(plan))
        }),
        Plan::Delete(_) | Plan::Update(_) => None,
    }
}

fn select_plan_first_virtual_table_name(select_plan: &SelectPlan) -> Option<String> {
    for joined_table in select_plan.joined_tables() {
        match &joined_table.table {
            Table::Virtual(virtual_table) if !virtual_table.innocuous => {
                return Some(virtual_table.name.clone())
            }
            Table::FromClauseSubquery(from_clause_subquery) => {
                if let Some(name) = plan_first_virtual_table_name(&from_clause_subquery.plan) {
                    return Some(name);
                }
            }
            _ => {}
        }
    }
    for subquery in &select_plan.non_from_clause_subqueries {
        if let SubqueryState::Unevaluated { plan: Some(plan) } = &subquery.state {
            if let Plan::Select(plan) = plan.as_ref() {
                if let Some(name) = select_plan_first_virtual_table_name(plan) {
                    return Some(name);
                }
            }
        }
    }
    None
}

pub fn prepare_select_plan(
    select: ast::Select,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    outer_query_refs: &[OuterQueryReference],
    query_destination: QueryDestination,
    connection: &Arc<crate::Connection>,
) -> Result<Plan> {
    let compounds = select.body.compounds;
    match compounds.is_empty() {
        true => Ok(Plan::Select(prepare_one_select_plan(
            select.body.select,
            resolver,
            program,
            select.limit,
            select.order_by,
            select.with,
            outer_query_refs,
            query_destination,
            connection,
        )?)),
        false => {
            // For compound SELECTs, the WITH clause applies to all parts.
            // We clone the WITH clause for each SELECT in the compound so that
            // each one can resolve CTE references independently.
            let with = select.with;

            let mut last = prepare_one_select_plan(
                select.body.select,
                resolver,
                program,
                None,
                vec![],
                with.clone(),
                outer_query_refs,
                query_destination.clone(),
                connection,
            )?;

            let mut left = Vec::with_capacity(compounds.len());
            for CompoundSelect {
                select: compound_select,
                operator,
            } in compounds
            {
                left.push((last, operator));
                last = prepare_one_select_plan(
                    compound_select,
                    resolver,
                    program,
                    None,
                    vec![],
                    with.clone(),
                    outer_query_refs,
                    query_destination.clone(),
                    connection,
                )?;
            }

            // Ensure all subplans have the same number of result columns
            let right_most_num_result_columns = last.result_columns.len();
            for (plan, operator) in left.iter() {
                if plan.result_columns.len() != right_most_num_result_columns {
                    crate::bail_parse_error!(
                        "SELECTs to the left and right of {} do not have the same number of result columns",
                        operator
                    );
                }
            }
            let (limit, offset) = select
                .limit
                .map_or(Ok((None, None)), |l| parse_limit(l, resolver))?;

            // Parse ORDER BY for compound selects.
            // ORDER BY can reference columns by number (1-based) or by name/alias
            // from any constituent SELECT's result columns.
            let all_plans: Vec<&SelectPlan> = left
                .iter()
                .map(|(plan, _)| plan)
                .chain(std::iter::once(&last))
                .collect();
            let order_by = if select.order_by.is_empty() {
                None
            } else {
                let mut key = Vec::with_capacity(select.order_by.len());
                for (i, o) in select.order_by.iter().enumerate() {
                    let col_idx = resolve_compound_order_by_expr(&o.expr, &all_plans, i + 1)?;
                    key.push((col_idx, o.order.unwrap_or(ast::SortOrder::Asc), o.nulls));
                }
                Some(key)
            };

            Ok(Plan::CompoundSelect {
                left,
                right_most: last,
                limit,
                offset,
                order_by,
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_one_select_plan(
    select: ast::OneSelect,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    limit: Option<ast::Limit>,
    order_by: Vec<ast::SortedColumn>,
    with: Option<ast::With>,
    outer_query_refs: &[OuterQueryReference],
    query_destination: QueryDestination,
    connection: &Arc<crate::Connection>,
) -> Result<SelectPlan> {
    match select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            distinctness,
            window_clause,
        } => {
            let col_count = columns.len();
            if col_count == 0 {
                crate::bail_parse_error!("SELECT without columns is not allowed");
            }

            let mut where_predicates = vec![];
            let mut vtab_predicates = vec![];

            let mut table_references = TableReferences::new(vec![], outer_query_refs.to_vec());

            if from.is_none() {
                for column in &columns {
                    if matches!(column, ResultColumn::Star) {
                        crate::bail_parse_error!("no tables specified");
                    }
                }
            }

            // Parse the FROM clause into a vec of TableReferences. Fold all the join conditions expressions into the WHERE clause.
            let preplan_ctes_for_non_from_subqueries = with.is_some()
                && select_has_non_from_subqueries(
                    &columns,
                    where_clause.as_deref(),
                    group_by.as_ref(),
                    &window_clause,
                    &order_by,
                    limit.as_ref(),
                );
            parse_from(
                from,
                resolver,
                program,
                with,
                preplan_ctes_for_non_from_subqueries,
                &mut where_predicates,
                &mut vtab_predicates,
                &mut table_references,
                connection,
            )?;

            // Preallocate space for the result columns
            let result_columns = Vec::with_capacity(
                columns
                    .iter()
                    .map(|c| match c {
                        // Allocate space for all columns in all tables
                        ResultColumn::Star => table_references
                            .joined_tables()
                            .iter()
                            .map(|t| t.columns().iter().filter(|col| !col.hidden()).count())
                            .sum(),
                        // Guess 5 columns if we can't find the table using the identifier (maybe it's in [brackets] or `tick_quotes`, or miXeDcAse)
                        ResultColumn::TableStar(n) => table_references
                            .joined_tables()
                            .iter()
                            .find(|t| t.identifier == n.as_str())
                            .map(|t| t.columns().iter().filter(|col| !col.hidden()).count())
                            .unwrap_or(5),
                        // Otherwise allocate space for 1 column
                        ResultColumn::Expr(_, _) => 1,
                    })
                    .sum(),
            );
            let mut plan = SelectPlan {
                join_order: table_references
                    .joined_tables()
                    .iter()
                    .enumerate()
                    .map(|(i, t)| JoinOrderMember {
                        table_id: t.internal_id,
                        original_idx: i,
                        is_outer: t.join_info.as_ref().is_some_and(|j| j.is_outer()),
                    })
                    .collect(),
                table_references,
                result_columns,
                where_clause: where_predicates,
                group_by: None,
                order_by: vec![],
                aggregates: vec![],
                limit: None,
                offset: None,
                contains_constant_false_condition: false,
                query_destination,
                distinctness: Distinctness::from_ast(distinctness.as_ref()),
                values: vec![],
                window: None,
                non_from_clause_subqueries: vec![],
                input_cardinality_hint: None,
                estimated_output_rows: None,
                simple_aggregate: None,
            };

            let mut windows = Vec::with_capacity(window_clause.len());
            for window_def in window_clause.iter() {
                let name = normalize_ident(window_def.name.as_str());
                let mut window = Window::new(Some(name), &window_def.window)?;

                for expr in window.partition_by.iter_mut() {
                    bind_and_rewrite_expr(
                        expr,
                        Some(&mut plan.table_references),
                        None,
                        resolver,
                        BindingBehavior::ResultColumnsNotAllowed,
                    )?;
                }
                for (expr, _, _) in window.order_by.iter_mut() {
                    bind_and_rewrite_expr(
                        expr,
                        Some(&mut plan.table_references),
                        None,
                        resolver,
                        BindingBehavior::ResultColumnsNotAllowed,
                    )?;
                }

                windows.push(window);
            }

            let long_names =
                connection.get_full_column_names() && !connection.get_short_column_names();
            let mut aggregate_expressions = Vec::new();
            for column in columns.into_iter() {
                match column {
                    ResultColumn::Star => {
                        select_star(
                            plan.table_references.joined_tables(),
                            &mut plan.result_columns,
                            plan.table_references.right_join_swapped(),
                            long_names,
                        )?;
                        for table in plan.table_references.joined_tables_mut() {
                            for idx in 0..table.columns().len() {
                                let column = &table.columns()[idx];
                                if column.hidden() {
                                    continue;
                                }
                                table.mark_column_used(idx);
                            }
                        }
                    }
                    ResultColumn::TableStar(name) => {
                        let name_normalized = normalize_ident(name.as_str());
                        // If this table identifier appears more than once in the FROM
                        // clause, `A.*` is ambiguous (matches SQLite behavior).
                        let dup_count = plan
                            .table_references
                            .joined_tables()
                            .iter()
                            .filter(|t| t.identifier == name_normalized)
                            .count();
                        if dup_count > 1 {
                            let first_tbl = plan
                                .table_references
                                .joined_tables()
                                .iter()
                                .find(|t| t.identifier == name_normalized)
                                .unwrap(); // safe: dup_count > 1 guarantees a match
                            let col_name = first_tbl
                                .columns()
                                .iter()
                                .find(|c| !c.hidden())
                                .and_then(|c| c.name.as_ref())
                                .map(|n| n.as_str())
                                .unwrap_or("?");
                            crate::bail_parse_error!(
                                "ambiguous column name: {}.{}",
                                name.as_str(),
                                col_name
                            );
                        }
                        let referenced_table = plan
                            .table_references
                            .joined_tables_mut()
                            .iter_mut()
                            .find(|t| t.identifier == name_normalized);

                        if referenced_table.is_none() {
                            crate::bail_parse_error!("no such table: {}", name.as_str());
                        }
                        let table = referenced_table.unwrap();
                        let num_columns = table.columns().len();
                        for idx in 0..num_columns {
                            let column = &table.columns()[idx];
                            if column.hidden() {
                                continue;
                            }
                            let alias = column.name.as_ref().map(|col_name| {
                                if long_names {
                                    format!("{}.{}", table.identifier, col_name)
                                } else {
                                    col_name.clone()
                                }
                            });
                            plan.result_columns.push(ResultSetColumn {
                                expr: ast::Expr::Column {
                                    database: None, // TODO: support different databases
                                    table: table.internal_id,
                                    column: idx,
                                    is_rowid_alias: column.is_rowid_alias(),
                                },
                                alias,
                                implicit_column_name: None,
                                contains_aggregates: false,
                            });
                            table.mark_column_used(idx);
                        }
                    }
                    ResultColumn::Expr(mut expr, maybe_alias) => {
                        bind_and_rewrite_expr(
                            &mut expr,
                            Some(&mut plan.table_references),
                            None,
                            resolver,
                            BindingBehavior::ResultColumnsNotAllowed,
                        )?;
                        let contains_aggregates = resolve_window_and_aggregate_functions(
                            &expr,
                            resolver,
                            &mut aggregate_expressions,
                            Some(&mut windows),
                        )?;
                        let (alias, implicit_column_name) = match &maybe_alias {
                            Some(ast::As::As(name)) | Some(ast::As::Elided(name)) => {
                                (Some(name.as_str().to_string()), None)
                            }
                            Some(ast::As::ImplicitColumnName(name)) => {
                                (None, Some(name.as_str().to_string()))
                            }
                            None => (None, None),
                        };
                        plan.result_columns.push(ResultSetColumn {
                            alias,
                            implicit_column_name,
                            expr: *expr,
                            contains_aggregates,
                        });
                    }
                }
            }

            if plan.result_columns.len() > SQLITE_MAX_COLUMN {
                crate::bail_parse_error!("too many columns in result set");
            }

            // This step can only be performed at this point, because all table references are now available.
            // Virtual table predicates may depend on column bindings from tables to the right in the join order,
            // so we must wait until the full set of references has been collected.
            add_vtab_predicates_to_where_clause(&mut vtab_predicates, &mut plan, resolver)?;

            // Parse the actual WHERE clause and add its conditions to the plan WHERE clause that already contains the join conditions.
            parse_where(
                where_clause.as_deref(),
                &mut plan.table_references,
                Some(&plan.result_columns),
                &mut plan.where_clause,
                resolver,
            )?;

            if let Some(mut group_by) = group_by {
                // Process HAVING clause if present
                let having_predicates = if let Some(having) = group_by.having {
                    Some(process_having_clause(
                        having,
                        &mut plan.table_references,
                        &plan.result_columns,
                        resolver,
                        &mut aggregate_expressions,
                    )?)
                } else {
                    None
                };

                if !group_by.exprs.is_empty() {
                    // Normal GROUP BY with expressions
                    for expr in group_by.exprs.iter_mut() {
                        replace_column_number_with_copy_of_column_expr(
                            expr,
                            &plan.result_columns,
                            "GROUP BY",
                        )?;
                        bind_and_rewrite_expr(
                            expr,
                            Some(&mut plan.table_references),
                            Some(&plan.result_columns),
                            resolver,
                            BindingBehavior::TryCanonicalColumnsFirst,
                        )?;
                    }

                    plan.group_by = Some(GroupBy {
                        sort_order: Vec::new(),
                        nulls_order: Vec::new(),
                        sort_elided: false,
                        exprs: group_by.exprs.iter().map(|expr| *expr.clone()).collect(),
                        having: having_predicates,
                    });
                } else {
                    // HAVING without GROUP BY: treat as ungrouped aggregation with filter
                    plan.group_by = Some(GroupBy {
                        sort_order: Vec::new(),
                        nulls_order: Vec::new(),
                        sort_elided: false,
                        exprs: vec![],
                        having: having_predicates,
                    });
                }
            }

            plan.aggregates = aggregate_expressions;

            // HAVING without GROUP BY requires aggregates in the SELECT
            if let Some(ref group_by) = plan.group_by {
                if group_by.exprs.is_empty()
                    && group_by.having.is_some()
                    && plan.aggregates.is_empty()
                {
                    crate::bail_parse_error!("HAVING clause on a non-aggregate query");
                }
            }

            // Parse the ORDER BY clause
            let mut key = Vec::new();
            let agg_count_before_order_by = plan.aggregates.len();
            let has_group_by = plan
                .group_by
                .as_ref()
                .is_some_and(|gb| !gb.exprs.is_empty());

            for mut o in order_by {
                replace_column_number_with_copy_of_column_expr(
                    &mut o.expr,
                    &plan.result_columns,
                    "ORDER BY",
                )?;

                bind_and_rewrite_expr(
                    &mut o.expr,
                    Some(&mut plan.table_references),
                    Some(&plan.result_columns),
                    resolver,
                    BindingBehavior::TryResultColumnsFirst,
                )?;
                let had_agg = resolve_window_and_aggregate_functions(
                    &o.expr,
                    resolver,
                    &mut plan.aggregates,
                    Some(&mut windows),
                )?;

                // SQLite rejects aggregate functions in ORDER BY when the query
                // has a FROM clause and is not already an aggregate query (no
                // GROUP BY and no aggregates in SELECT/HAVING).
                // e.g. SELECT f1 FROM t ORDER BY min(f1);
                // But SELECT 1 ORDER BY sum(1) is allowed (no FROM clause).
                let has_from = !plan.table_references.joined_tables().is_empty();
                if had_agg && has_from && !has_group_by && agg_count_before_order_by == 0 {
                    let agg = &plan.aggregates[agg_count_before_order_by];
                    crate::bail_parse_error!("misuse of aggregate: {}()", agg.func);
                }

                key.push((o.expr, o.order.unwrap_or(ast::SortOrder::Asc), o.nulls));
            }
            // Remove duplicate ORDER BY expressions, keeping the first occurrence.
            // Duplicates are semantically redundant.
            let mut i = 0;
            while i < key.len() {
                if key[..i]
                    .iter()
                    .any(|(prev, _, _)| exprs_are_equivalent(prev, &key[i].0))
                {
                    key.remove(i);
                } else {
                    i += 1;
                }
            }
            plan.order_by = key;

            // Single-row aggregate queries (aggregates without GROUP BY and without window functions)
            // produce exactly one row, so ORDER BY is meaningless. Clearing it here also avoids
            // eagerly validating subqueries in ORDER BY that SQLite would skip due to optimization.
            // Note: HAVING without GROUP BY sets group_by to Some with empty exprs, still single-row.
            let is_single_row_aggregate = !plan.aggregates.is_empty()
                && plan.group_by.as_ref().is_none_or(|gb| gb.exprs.is_empty())
                && windows.is_empty();
            if is_single_row_aggregate {
                plan.order_by.clear();
            }

            // SQLite optimizes away ORDER BY clauses after a rowid/INTEGER PRIMARY KEY column
            // when it's FIRST in the ORDER BY, since the table is stored in rowid order.
            // This means we truncate the ORDER BY to just the rowid column.
            // We do this for SQLite compatibility - SQLite truncates before validating, so
            // even invalid constructions like ORDER BY rowid, a IN (SELECT a, b FROM t) pass.
            if plan.order_by.len() > 1 && plan.table_references.joined_tables().len() == 1 {
                let joined = &plan.table_references.joined_tables()[0];
                let table_id = joined.internal_id;
                let rowid_alias_col = joined
                    .btree()
                    .and_then(|t| t.get_rowid_alias_column().map(|(idx, _)| idx));

                let first_is_rowid = match plan.order_by[0].0.as_ref() {
                    ast::Expr::Column { table, column, .. } => {
                        *table == table_id && rowid_alias_col == Some(*column)
                    }
                    ast::Expr::RowId { table, .. } => *table == table_id,
                    _ => false,
                };
                if first_is_rowid {
                    plan.order_by.truncate(1);
                }
            }

            if let Some(group_by) = &mut plan.group_by {
                // now that we have resolved the ORDER BY expressions and aggregates, we can
                // compute the necessary sort order for the GROUP BY clause
                (group_by.sort_order, group_by.nulls_order) = compute_group_by_sort_order(
                    &group_by.exprs,
                    &plan.order_by,
                    &plan.aggregates,
                    resolver,
                );
                debug_assert_eq!(
                    group_by.exprs.len(),
                    group_by.sort_order.len(),
                    "GROUP BY exprs and sort_order must have the same length"
                );
            }

            // Parse the LIMIT/OFFSET clause
            (plan.limit, plan.offset) =
                limit.map_or(Ok((None, None)), |l| parse_limit(l, resolver))?;

            if !windows.is_empty() {
                plan_windows(program, &mut plan, resolver, connection, &mut windows)?;
            }

            plan_subqueries_from_select_plan(program, &mut plan, resolver, connection)?;

            validate_group_by_outer_scope_refs(&plan)?;

            validate_expr_correct_column_counts(&plan)?;

            // Return the unoptimized query plan
            Ok(plan)
        }
        ast::OneSelect::Values(mut values) => {
            if !order_by.is_empty() {
                crate::bail_parse_error!("ORDER BY clause is not allowed with VALUES clause");
            }
            if limit.is_some() {
                crate::bail_parse_error!("LIMIT clause is not allowed with VALUES clause");
            }
            let len = values[0].len();
            if len > SQLITE_MAX_COLUMN {
                crate::bail_parse_error!("too many columns in result set");
            }
            let mut result_columns = Vec::with_capacity(len);
            for i in 0..len {
                result_columns.push(ResultSetColumn {
                    // these result_columns work as placeholders for the values, so the expr doesn't matter
                    expr: ast::Expr::Literal(ast::Literal::Numeric(i.to_string())),
                    alias: Some(format!("column{}", i + 1)),
                    implicit_column_name: None,
                    contains_aggregates: false,
                });
            }

            let mut table_references = TableReferences::new(vec![], outer_query_refs.to_vec());

            // Plan CTEs from WITH clause so they're available for subqueries in VALUES
            plan_ctes_as_outer_refs(with, resolver, program, &mut table_references, connection)?;

            for value_row in values.iter_mut() {
                for value in value_row.iter_mut() {
                    // Before binding, we check for unquoted literals. Sqlite throws an error in this case
                    bind_and_rewrite_expr(
                        value,
                        Some(&mut table_references),
                        None,
                        resolver,
                        // Allow sqlite quirk of inserting "double-quoted" literals (which our AST maps as identifiers)
                        BindingBehavior::TryResultColumnsFirst,
                    )?;
                }
            }

            // Plan subqueries in VALUES expressions
            let mut non_from_clause_subqueries = vec![];
            plan_subqueries_from_values(
                program,
                &mut non_from_clause_subqueries,
                &mut table_references,
                &mut values,
                resolver,
                connection,
            )?;

            let plan = SelectPlan {
                join_order: vec![],
                table_references,
                result_columns,
                where_clause: vec![],
                group_by: None,
                order_by: vec![],
                aggregates: vec![],
                limit: None,
                offset: None,
                contains_constant_false_condition: false,
                query_destination,
                distinctness: Distinctness::NonDistinct,
                values: values
                    .iter()
                    .map(|values| values.iter().map(|value| *value.clone()).collect())
                    .collect(),
                window: None,
                non_from_clause_subqueries,
                input_cardinality_hint: None,
                estimated_output_rows: None,
                simple_aggregate: None,
            };

            validate_expr_correct_column_counts(&plan)?;

            Ok(plan)
        }
    }
}

/// Validate that all expressions in the plan return the correct number of values;
/// generally this only applies to parenthesized lists and subqueries.
fn validate_expr_correct_column_counts(plan: &SelectPlan) -> Result<()> {
    for result_column in plan.result_columns.iter() {
        let vec_size = expr_vector_size(&result_column.expr)?;
        if vec_size != 1 {
            crate::bail_parse_error!("result column must return 1 value, got {}", vec_size);
        }
    }
    for (expr, _, _) in plan.order_by.iter() {
        let vec_size = expr_vector_size(expr)?;
        if vec_size != 1 {
            crate::bail_parse_error!("order by expression must return 1 value, got {}", vec_size);
        }
    }
    if let Some(group_by) = &plan.group_by {
        for expr in group_by.exprs.iter() {
            let vec_size = expr_vector_size(expr)?;
            if vec_size != 1 {
                crate::bail_parse_error!(
                    "group by expression must return 1 value, got {}",
                    vec_size
                );
            }
        }
        if let Some(having) = &group_by.having {
            for expr in having.iter() {
                let vec_size = expr_vector_size(expr)?;
                if vec_size != 1 {
                    crate::bail_parse_error!(
                        "having expression must return 1 value, got {}",
                        vec_size
                    );
                }
            }
        }
    }
    for aggregate in plan.aggregates.iter() {
        for arg in aggregate.args.iter() {
            let vec_size = expr_vector_size(arg)?;
            if vec_size != 1 {
                crate::bail_parse_error!(
                    "aggregate argument must return 1 value, got {}",
                    vec_size
                );
            }
        }
    }
    for term in plan.where_clause.iter() {
        let vec_size = expr_vector_size(&term.expr)?;
        if vec_size != 1 {
            crate::bail_parse_error!(
                "where clause expression must return 1 value, got {}",
                vec_size
            );
        }
    }
    for expr in plan.values.iter() {
        for value in expr.iter() {
            let vec_size = expr_vector_size(value)?;
            if vec_size != 1 {
                crate::bail_parse_error!("value must return 1 value, got {}", vec_size);
            }
        }
    }
    if let Some(limit) = &plan.limit {
        let vec_size = expr_vector_size(limit)?;
        if vec_size != 1 {
            crate::bail_parse_error!("limit expression must return 1 value, got {}", vec_size);
        }
    }
    if let Some(offset) = &plan.offset {
        let vec_size = expr_vector_size(offset)?;
        if vec_size != 1 {
            crate::bail_parse_error!("offset expression must return 1 value, got {}", vec_size);
        }
    }
    Ok(())
}

/// SQLite compatibility: GROUP BY expressions in a correlated subquery cannot
/// reference columns from the outer query scope.
fn validate_group_by_outer_scope_refs(plan: &SelectPlan) -> Result<()> {
    if plan.table_references.outer_query_refs().is_empty() {
        return Ok(());
    }
    let Some(group_by) = &plan.group_by else {
        return Ok(());
    };
    for expr in &group_by.exprs {
        reject_outer_query_refs_in_group_by_expr(
            expr,
            &plan.table_references,
            &plan.non_from_clause_subqueries,
        )?;
    }
    Ok(())
}

fn reject_outer_query_refs_in_group_by_expr(
    expr: &Expr,
    table_references: &TableReferences,
    subqueries: &[super::plan::NonFromClauseSubquery],
) -> Result<()> {
    walk_expr(expr, &mut |node: &Expr| -> Result<WalkControl> {
        match node {
            Expr::Column { table, column, .. } => {
                if let Some(outer_ref) =
                    table_references.find_outer_query_ref_by_internal_id(*table)
                {
                    let column_name = outer_ref
                        .columns()
                        .get(*column)
                        .and_then(|col| col.name.as_deref())
                        .expect(
                            "bound outer-scope Expr::Column must point to a named column in schema",
                        );
                    crate::bail_parse_error!(
                        "no such column: {}.{}",
                        outer_ref.identifier,
                        column_name
                    );
                }
            }
            Expr::RowId { table, .. } => {
                if let Some(outer_ref) =
                    table_references.find_outer_query_ref_by_internal_id(*table)
                {
                    crate::bail_parse_error!("no such column: {}.rowid", outer_ref.identifier);
                }
            }
            Expr::SubqueryResult { subquery_id, .. } => {
                let subquery = subqueries
                    .iter()
                    .find(|subquery| subquery.internal_id == *subquery_id)
                    .expect("GROUP BY SubqueryResult must reference a planned subquery");
                let super::plan::SubqueryState::Unevaluated {
                    plan: Some(subquery_plan),
                } = &subquery.state
                else {
                    unreachable!("GROUP BY subquery must be in unevaluated state during planning");
                };
                reject_outer_scope_refs_inside_plan_tree(subquery_plan, table_references)?;
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

fn reject_outer_scope_refs_inside_plan_tree(
    plan: &Plan,
    current_scope_table_refs: &TableReferences,
) -> Result<()> {
    match plan {
        Plan::Select(select_plan) => {
            reject_outer_scope_refs_inside_select_plan(select_plan, current_scope_table_refs)
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            for (sub_plan, _) in left {
                reject_outer_scope_refs_inside_select_plan(sub_plan, current_scope_table_refs)?;
            }
            reject_outer_scope_refs_inside_select_plan(right_most, current_scope_table_refs)
        }
        Plan::Delete(_) | Plan::Update(_) => Ok(()),
    }
}

fn reject_outer_scope_refs_inside_select_plan(
    plan: &SelectPlan,
    current_scope_table_refs: &TableReferences,
) -> Result<()> {
    for outer_ref in plan
        .table_references
        .outer_query_refs()
        .iter()
        .filter(|outer_ref| outer_ref.is_used())
    {
        if current_scope_table_refs
            .find_outer_query_ref_by_internal_id(outer_ref.internal_id)
            .is_none()
        {
            continue;
        }
        if let Some(col_idx) = outer_ref.col_used_mask.iter().next() {
            let column_name = outer_ref
                .columns()
                .get(col_idx)
                .and_then(|col| col.name.as_deref())
                .expect("bound outer-scope Expr::Column must point to a named column in schema");
            crate::bail_parse_error!("no such column: {}.{}", outer_ref.identifier, column_name);
        }
        if outer_ref.rowid_referenced {
            crate::bail_parse_error!("no such column: {}.rowid", outer_ref.identifier);
        }
        unreachable!("used outer query reference must reference at least one column or rowid");
    }

    for subquery in &plan.non_from_clause_subqueries {
        let super::plan::SubqueryState::Unevaluated {
            plan: Some(subquery_plan),
        } = &subquery.state
        else {
            continue;
        };
        reject_outer_scope_refs_inside_plan_tree(subquery_plan, current_scope_table_refs)?;
    }

    for joined_table in plan.table_references.joined_tables().iter() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &joined_table.table {
            reject_outer_scope_refs_inside_plan_tree(
                from_clause_subquery.plan.as_ref(),
                current_scope_table_refs,
            )?;
        }
    }

    Ok(())
}

fn add_vtab_predicates_to_where_clause(
    vtab_predicates: &mut Vec<Expr>,
    plan: &mut SelectPlan,
    resolver: &Resolver,
) -> Result<()> {
    for expr in vtab_predicates.iter_mut() {
        bind_and_rewrite_expr(
            expr,
            Some(&mut plan.table_references),
            Some(&plan.result_columns),
            resolver,
            BindingBehavior::TryCanonicalColumnsFirst,
        )?;
    }
    for expr in vtab_predicates.drain(..) {
        // Virtual table argument predicates (e.g. the 't2' in pragma_table_info('t2'))
        // must be associated with the virtual table's outer join context if the table is
        // the RHS of a LEFT JOIN. Otherwise the optimizer may incorrectly simplify the
        // LEFT JOIN into an INNER JOIN, breaking NULL row emission for unmatched rows.
        let from_outer_join = vtab_predicate_table_id(&expr).and_then(|table_id| {
            plan.table_references
                .find_joined_table_by_internal_id(table_id)
                .and_then(|t| {
                    t.join_info
                        .as_ref()
                        .and_then(|ji| ji.is_outer().then_some(table_id))
                })
        });
        plan.where_clause.push(WhereTerm {
            expr,
            from_outer_join,
            consumed: false,
        });
    }
    Ok(())
}

/// Extract the table internal_id from a virtual table argument predicate.
/// These are always of the form `Column { table, .. } = literal` or `IsNull(Column { table, .. })`.
fn vtab_predicate_table_id(expr: &Expr) -> Option<ast::TableInternalId> {
    match expr {
        Expr::Binary(lhs, _, _) | Expr::IsNull(lhs) => match lhs.as_ref() {
            Expr::Column { table, .. } => Some(*table),
            _ => None,
        },
        _ => None,
    }
}

/// Replaces a column number in an ORDER BY or GROUP BY expression with a copy of the column expression.
/// For example, in SELECT u.first_name, count(1) FROM users u GROUP BY 1 ORDER BY 2,
/// the column number 1 is replaced with u.first_name and the column number 2 is replaced with count(1).
///
/// Per SQLite documentation, only constant integers are treated as column references.
/// Non-integer numeric literals (floats) are treated as constant expressions.
fn replace_column_number_with_copy_of_column_expr(
    order_by_or_group_by_expr: &mut ast::Expr,
    columns: &[ResultSetColumn],
    clause_name: &str,
) -> Result<()> {
    // Extract the numeric literal string, handling both bare integers (e.g. `2`)
    // and unary-plus integers (e.g. `+2`). In SQLite, `ORDER BY +2` strips the
    // unary plus and still resolves `2` as a column index reference.
    let num_str = match order_by_or_group_by_expr {
        ast::Expr::Literal(ast::Literal::Numeric(num)) => Some(num.clone()),
        ast::Expr::Unary(ast::UnaryOperator::Positive, inner) => {
            if let ast::Expr::Literal(ast::Literal::Numeric(num)) = inner.as_ref() {
                Some(num.clone())
            } else {
                None
            }
        }
        ast::Expr::Unary(ast::UnaryOperator::Negative, inner) => {
            if let ast::Expr::Literal(ast::Literal::Numeric(num)) = inner.as_ref() {
                if num.parse::<usize>().is_ok() {
                    crate::bail_parse_error!(
                        "1st {} term out of range - should be between 1 and {}",
                        clause_name,
                        columns.len()
                    );
                }
            }
            None
        }
        _ => None,
    };
    if let Some(num) = num_str {
        // Only treat as column reference if it parses as a positive integer.
        // Float literals like "0.5" or "1.0" are valid constant expressions, not column references.
        if let Ok(column_number) = num.parse::<usize>() {
            if column_number == 0 || column_number > columns.len() {
                crate::bail_parse_error!(
                    "1st {} term out of range - should be between 1 and {}",
                    clause_name,
                    columns.len()
                );
            }
            let ResultSetColumn { expr, .. } = &columns[column_number - 1];
            *order_by_or_group_by_expr = expr.clone();
        }
        // Otherwise, leave the expression as-is (constant expression, case 3 per SQLite docs)
    }
    Ok(())
}

/// Resolves a compound SELECT ORDER BY expression to a 0-based column index.
/// ORDER BY in compound selects can reference columns by:
/// 1. Numeric position (1-based): ORDER BY 1
/// 2. Column name or alias from any constituent SELECT: ORDER BY name
fn resolve_compound_order_by_expr(
    expr: &ast::Expr,
    all_plans: &[&SelectPlan],
    term_number: usize,
) -> Result<usize> {
    let num_result_columns = all_plans[0].result_columns.len();
    match expr {
        // Case 1: Numeric column reference (e.g., ORDER BY 1)
        ast::Expr::Literal(ast::Literal::Numeric(num)) => {
            if let Ok(column_number) = num.parse::<usize>() {
                if column_number == 0 || column_number > num_result_columns {
                    crate::bail_parse_error!(
                        "{} ORDER BY term out of range - should be between 1 and {}",
                        column_number,
                        num_result_columns
                    );
                }
                Ok(column_number - 1)
            } else {
                crate::bail_parse_error!(
                    "{} ORDER BY term does not match any column in the result set",
                    ordinal(term_number)
                );
            }
        }
        // Case 2: Name reference (e.g., ORDER BY name or ORDER BY alias)
        ast::Expr::Id(name) => {
            let name_normalized = normalize_ident(name.as_str());
            // Check aliases and column names across all constituent SELECTs
            for plan in all_plans {
                let result_columns = &plan.result_columns;
                let table_references = &plan.table_references;
                // Try matching against aliases
                for (i, rc) in result_columns.iter().enumerate() {
                    if let Some(alias) = &rc.alias {
                        if normalize_ident(alias) == name_normalized {
                            return Ok(i);
                        }
                    }
                }
                // Try matching against column names from the table references
                for (i, rc) in result_columns.iter().enumerate() {
                    if let Some(col_name) = rc.name(table_references) {
                        if normalize_ident(col_name) == name_normalized {
                            return Ok(i);
                        }
                    }
                }
            }
            crate::bail_parse_error!(
                "{} ORDER BY term does not match any column in the result set",
                ordinal(term_number)
            );
        }
        _ => {
            crate::bail_parse_error!(
                "{} ORDER BY term does not match any column in the result set",
                ordinal(term_number)
            );
        }
    }
}

fn ordinal(n: usize) -> String {
    let suffix = match (n % 10, n % 100) {
        (1, 11) | (2, 12) | (3, 13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    };
    format!("{n}{suffix}")
}

/// Count required cursors for a Plan (either Select or CompoundSelect)
fn count_required_cursors_for_simple_or_compound_select(plan: &Plan) -> usize {
    match plan {
        Plan::Select(select_plan) => count_required_cursors_for_simple_select(select_plan),
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            count_required_cursors_for_simple_select(right_most)
                + left
                    .iter()
                    .map(|(p, _)| count_required_cursors_for_simple_select(p))
                    .sum::<usize>()
        }
        Plan::Delete(_) | Plan::Update(_) => 0,
    }
}

fn count_required_cursors_for_simple_select(plan: &SelectPlan) -> usize {
    let num_table_cursors: usize = plan
        .joined_tables()
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 1,
            Operation::Search(search) => match search {
                Search::RowidEq { .. } => 1,
                Search::Seek { index, .. } => 1 + index.is_some() as usize,
                Search::InSeek { index, source } => match source {
                    // table cursor + new ephemeral cursor + optional index cursor
                    InSeekSource::LiteralList { .. } => 2 + index.is_some() as usize,
                    // table cursor + optional index cursor (ephemeral already counted)
                    InSeekSource::Subquery { .. } => 1 + index.is_some() as usize,
                },
            }
            Operation::IndexMethodQuery(_) => 1,
            Operation::HashJoin(_) => 2,
            // One table cursor + one cursor per index branch
            Operation::MultiIndexScan(multi_idx) => 1 + multi_idx.branches.len(),
        } + if let Table::FromClauseSubquery(from_clause_subquery) = &t.table {
            count_required_cursors_for_simple_or_compound_select(&from_clause_subquery.plan)
        } else {
            0
        })
        .sum();
    let has_group_by_with_exprs = plan
        .group_by
        .as_ref()
        .is_some_and(|gb| !gb.exprs.is_empty());
    let num_sorter_cursors = has_group_by_with_exprs as usize + !plan.order_by.is_empty() as usize;
    let num_pseudo_cursors = has_group_by_with_exprs as usize + !plan.order_by.is_empty() as usize;

    num_table_cursors + num_sorter_cursors + num_pseudo_cursors
}

/// Estimate number of instructions for a Plan (either Select or CompoundSelect)
fn estimate_num_instructions_for_simple_or_compound_select(plan: &Plan) -> usize {
    match plan {
        Plan::Select(select_plan) => estimate_num_instructions_for_simple_select(select_plan),
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            estimate_num_instructions_for_simple_select(right_most)
                + left
                    .iter()
                    .map(|(p, _)| estimate_num_instructions_for_simple_select(p))
                    .sum::<usize>()
                + 20 // overhead for compound select operations
        }
        Plan::Delete(_) | Plan::Update(_) => 0,
    }
}

fn estimate_num_instructions_for_simple_select(select: &SelectPlan) -> usize {
    let table_instructions: usize = select
        .joined_tables()
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 10,
            Operation::Search(_) => 15,
            Operation::IndexMethodQuery(_) => 15,
            Operation::HashJoin(_) => 20,
            // Multi-index scan: scan overhead per branch + deduplication + final rowid fetch
            Operation::MultiIndexScan(multi_idx) => 15 * multi_idx.branches.len() + 10,
        } + if let Table::FromClauseSubquery(from_clause_subquery) = &t.table {
            10 + estimate_num_instructions_for_simple_or_compound_select(&from_clause_subquery.plan)
        } else {
            0
        })
        .sum();

    let group_by_instructions = select.group_by.is_some() as usize * 10;
    let order_by_instructions = !select.order_by.is_empty() as usize * 10;
    let condition_instructions = select.where_clause.len() * 3;

    20 + table_instructions + group_by_instructions + order_by_instructions + condition_instructions
}

fn push_function_tail_exprs<'a>(stack: &mut Vec<&'a Expr>, tail: &'a ast::FunctionTail) {
    if let Some(filter_expr) = tail.filter_clause.as_deref() {
        stack.push(filter_expr);
    }

    let Some(ast::Over::Window(window)) = tail.over_clause.as_ref() else {
        return;
    };

    if let Some(frame_clause) = window.frame_clause.as_ref() {
        if let ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) =
            &frame_clause.start
        {
            stack.push(expr.as_ref());
        }
        if let Some(ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr)) =
            frame_clause.end.as_ref()
        {
            stack.push(expr.as_ref());
        }
    }

    for sorted in window.order_by.iter().rev() {
        stack.push(sorted.expr.as_ref());
    }
    for part_expr in window.partition_by.iter().rev() {
        stack.push(part_expr.as_ref());
    }
}

fn expr_contains_subquery(expr: &Expr) -> bool {
    // Iterative traversal avoids stack overflows on deeply nested expression trees
    // such as very large left-associative AND chains.
    let mut stack = vec![expr];
    while let Some(node) = stack.pop() {
        match node {
            Expr::Subquery(_) | Expr::InSelect { .. } | Expr::Exists(_) => return true,
            Expr::Between {
                lhs, start, end, ..
            } => {
                stack.push(lhs.as_ref());
                stack.push(start.as_ref());
                stack.push(end.as_ref());
            }
            Expr::Binary(lhs, _, rhs) => {
                stack.push(rhs.as_ref());
                stack.push(lhs.as_ref());
            }
            Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                if let Some(expr) = else_expr.as_deref() {
                    stack.push(expr);
                }
                for (when_expr, then_expr) in when_then_pairs.iter().rev() {
                    stack.push(then_expr.as_ref());
                    stack.push(when_expr.as_ref());
                }
                if let Some(base_expr) = base.as_deref() {
                    stack.push(base_expr);
                }
            }
            Expr::Cast { expr, .. }
            | Expr::Collate(expr, _)
            | Expr::IsNull(expr)
            | Expr::NotNull(expr)
            | Expr::Unary(_, expr) => {
                stack.push(expr.as_ref());
            }
            Expr::FunctionCall {
                args,
                order_by,
                filter_over,
                ..
            } => {
                push_function_tail_exprs(&mut stack, filter_over);
                for sorted in order_by.iter().rev() {
                    stack.push(sorted.expr.as_ref());
                }
                for arg in args.iter().rev() {
                    stack.push(arg.as_ref());
                }
            }
            Expr::FunctionCallStar { filter_over, .. } => {
                push_function_tail_exprs(&mut stack, filter_over);
            }
            Expr::InList { lhs, rhs, .. } => {
                for item in rhs.iter().rev() {
                    stack.push(item.as_ref());
                }
                stack.push(lhs.as_ref());
            }
            Expr::InTable { lhs, args, .. } => {
                for arg in args.iter().rev() {
                    stack.push(arg.as_ref());
                }
                stack.push(lhs.as_ref());
            }
            Expr::Like {
                lhs, rhs, escape, ..
            } => {
                if let Some(escape_expr) = escape.as_deref() {
                    stack.push(escape_expr);
                }
                stack.push(rhs.as_ref());
                stack.push(lhs.as_ref());
            }
            Expr::Parenthesized(exprs) => {
                for expr in exprs.iter().rev() {
                    stack.push(expr.as_ref());
                }
            }
            Expr::Raise(_, raise_expr) => {
                if let Some(expr) = raise_expr.as_deref() {
                    stack.push(expr);
                }
            }
            Expr::SubqueryResult { lhs, .. } => {
                if let Some(expr) = lhs.as_deref() {
                    stack.push(expr);
                }
            }
            Expr::Array { .. } | Expr::Subscript { .. } => {
                unreachable!("Array and Subscript are desugared into function calls by the parser")
            }
            Expr::Column { .. }
            | Expr::DoublyQualified(_, _, _)
            | Expr::Id(_)
            | Expr::Literal(_)
            | Expr::Name(_)
            | Expr::Qualified(_, _)
            | Expr::Register(_)
            | Expr::RowId { .. }
            | Expr::Variable(_)
            | Expr::Default => {}
        }
    }
    false
}

fn select_has_non_from_subqueries(
    columns: &[ResultColumn],
    where_clause: Option<&Expr>,
    group_by: Option<&ast::GroupBy>,
    window_clause: &[ast::WindowDef],
    order_by: &[ast::SortedColumn],
    limit: Option<&ast::Limit>,
) -> bool {
    if columns.iter().any(|column| match column {
        ResultColumn::Expr(expr, _) => expr_contains_subquery(expr),
        ResultColumn::Star | ResultColumn::TableStar(_) => false,
    }) {
        return true;
    }

    if where_clause.is_some_and(expr_contains_subquery) {
        return true;
    }

    if let Some(group_by) = group_by {
        if group_by.exprs.iter().any(|e| expr_contains_subquery(e))
            || group_by
                .having
                .as_deref()
                .is_some_and(expr_contains_subquery)
        {
            return true;
        }
    }

    if window_clause.iter().any(|w| {
        w.window
            .partition_by
            .iter()
            .any(|e| expr_contains_subquery(e))
            || w.window
                .order_by
                .iter()
                .any(|s| expr_contains_subquery(&s.expr))
    }) {
        return true;
    }

    if order_by.iter().any(|s| expr_contains_subquery(&s.expr)) {
        return true;
    }

    if let Some(limit) = limit {
        if expr_contains_subquery(&limit.expr)
            || limit.offset.as_deref().is_some_and(expr_contains_subquery)
        {
            return true;
        }
    }

    false
}

/// Estimate number of labels for a Plan (either Select or CompoundSelect)
fn estimate_num_labels_for_simple_or_compound_select(plan: &Plan) -> usize {
    match plan {
        Plan::Select(select_plan) => estimate_num_labels_for_simple_select(select_plan),
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            estimate_num_labels_for_simple_select(right_most)
                + left
                    .iter()
                    .map(|(p, _)| estimate_num_labels_for_simple_select(p))
                    .sum::<usize>()
                + 10 // overhead for compound select operations
        }
        Plan::Delete(_) | Plan::Update(_) => 0,
    }
}

fn estimate_num_labels_for_simple_select(select: &SelectPlan) -> usize {
    let init_halt_labels = 2;
    // 3 loop labels for each table in main loop + 1 to signify end of main loop
    let table_labels = select
        .joined_tables()
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 3,
            Operation::Search(_) => 3,
            Operation::IndexMethodQuery(_) => 3,
            Operation::HashJoin(_) => 3,
            // Multi-index scan needs extra labels for each branch + rowset loop
            Operation::MultiIndexScan(multi_idx) => 3 + multi_idx.branches.len() * 2,
        } + if let Table::FromClauseSubquery(from_clause_subquery) = &t.table {
            3 + estimate_num_labels_for_simple_or_compound_select(&from_clause_subquery.plan)
        } else {
            0
        })
        .sum::<usize>()
        + 1;

    let group_by_labels = select.group_by.is_some() as usize * 10;
    let order_by_labels = !select.order_by.is_empty() as usize * 10;
    let condition_labels = select.where_clause.len() * 2;

    init_halt_labels + table_labels + group_by_labels + order_by_labels + condition_labels
}

pub fn emit_simple_count(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<bool> {
    let cursors = plan
        .joined_tables()
        .first()
        .unwrap()
        .resolve_cursors(program, OperationMode::SELECT)?;

    let cursor_id = {
        match cursors {
            (_, Some(cursor_id)) | (Some(cursor_id), None) => cursor_id,
            _ => return Ok(false),
        }
    };

    // Count opcode only works on BTree cursors. Materialized view trigger
    // queries may have pseudo cursors — fall back to normal aggregation.
    if !program.cursor_is_btree(cursor_id) {
        return Ok(false);
    }

    let target_reg = program.alloc_register();

    program.emit_insn(Insn::Count {
        cursor_id,
        target_reg,
        exact: true,
    });

    program.emit_insn(Insn::Close { cursor_id });

    let agg = plan
        .aggregates
        .first()
        .expect("simple count requires exactly one aggregate");
    t_ctx.resolver.cache_expr_reg(
        Cow::Owned(agg.original_expr.clone()),
        target_reg,
        false,
        None,
    );
    t_ctx.resolver.enable_expr_to_reg_cache();

    emit_select_result(
        program,
        &t_ctx.resolver,
        plan,
        None,
        None,
        None,
        None,
        t_ctx.reg_result_cols_start.unwrap(),
        t_ctx.limit_ctx,
    )?;
    Ok(true)
}

fn process_having_clause(
    having: Box<ast::Expr>,
    table_references: &mut TableReferences,
    result_columns: &[ResultSetColumn],
    resolver: &Resolver,
    aggregate_expressions: &mut Vec<super::plan::Aggregate>,
) -> Result<Vec<ast::Expr>> {
    let mut predicates = vec![];
    break_predicate_at_and_boundaries(&having, &mut predicates);

    // Before alias resolution replaces identifiers with their underlying expressions,
    // check for aliased aggregate misuse. SQLite does this during name resolution by
    // checking the NC_AllowAgg flag on the NameContext (see resolve.c). When an identifier
    // inside an aggregate function's arguments resolves to an alias whose original expression
    // has EP_Agg, SQLite reports "misuse of aliased aggregate X".
    for expr in predicates.iter() {
        check_aliased_aggregate_misuse(expr, result_columns)?;
    }

    for expr in predicates.iter_mut() {
        bind_and_rewrite_expr(
            expr,
            Some(table_references),
            Some(result_columns),
            resolver,
            BindingBehavior::TryResultColumnsFirst,
        )?;
        resolve_window_and_aggregate_functions(expr, resolver, aggregate_expressions, None)?;
    }

    Ok(predicates)
}

/// Walk a HAVING expression looking for aggregate function calls whose arguments
/// reference aliases of aggregate result columns (SQLite ticket #2526).
fn check_aliased_aggregate_misuse(
    expr: &ast::Expr,
    result_columns: &[ResultSetColumn],
) -> Result<()> {
    use crate::translate::expr::{walk_expr, WalkControl};

    walk_expr(expr, &mut |e| {
        match e {
            Expr::FunctionCall { name, args, .. } => {
                let is_agg = matches!(
                    crate::function::Func::resolve_function(name.as_str(), args.len()),
                    Ok(Some(crate::function::Func::Agg(_)))
                );
                if is_agg {
                    for arg in args.iter() {
                        find_aliased_aggregate_ref(arg, result_columns)?;
                    }
                    return Ok(WalkControl::SkipChildren);
                }
            }
            Expr::FunctionCallStar { name, .. } => {
                if matches!(
                    crate::function::Func::resolve_function(name.as_str(), 0),
                    Ok(Some(crate::function::Func::Agg(_)))
                ) {
                    return Ok(WalkControl::SkipChildren);
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// Check if an expression (inside an aggregate's arguments) contains an identifier
/// that matches an alias of an aggregate result column.
fn find_aliased_aggregate_ref(expr: &ast::Expr, result_columns: &[ResultSetColumn]) -> Result<()> {
    use crate::translate::expr::{walk_expr, WalkControl};

    walk_expr(expr, &mut |e| {
        if let Expr::Id(id) = e {
            let normalized = normalize_ident(id.as_str());
            for rc in result_columns.iter() {
                if let Some(alias) = &rc.alias {
                    if alias.eq_ignore_ascii_case(&normalized) && rc.contains_aggregates {
                        crate::bail_parse_error!("misuse of aliased aggregate {}", normalized);
                    }
                }
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}
