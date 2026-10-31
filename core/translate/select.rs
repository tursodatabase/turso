use super::emitter::{emit_program, TranslateCtx};
use super::expr_context::ExprContext;
use super::plan::{
    expand_star, expand_table_star, Distinctness, InSeekSource, JoinOrderMember, NamedWindowBound,
    NamedWindowDef, Operation, OuterQueryReference, QueryDestination, Search, TableReferences,
    Window,
};
use crate::schema::Table;
use crate::stack::trace_stack;
use crate::sync::Arc;
use crate::translate::collate::CollationSeq;
use crate::translate::emitter::{OperationMode, Resolver};
use crate::translate::expr::{
    bind_and_rewrite_expr, expr_vector_size, walk_expr, walk_expr_mut, BindingBehavior, WalkControl,
};
use crate::translate::group_by::compute_group_by_sort_order;
use crate::translate::optimizer::{optimize_plan, Optimizable};
use crate::translate::plan::{
    GroupBy, Plan, ResultSetColumn, SelectPlan, SubqueryState, WhereTerm,
};
use crate::translate::planner::{
    append_vtab_predicates_to_where_clause, break_predicate_at_and_boundaries,
    collect_subquery_table_refs_in_expr, collect_subquery_table_refs_in_window_clause, parse_from,
    parse_limit, parse_where, plan_ctes_as_outer_refs, resolve_window_and_aggregate_functions,
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
use turso_parser::ast::SortOrder;
use turso_parser::ast::{self, CompoundSelect, Expr};

#[turso_macros::trace_stack]
pub fn translate_select(
    select: ast::Select,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    query_destination: QueryDestination,
    connection: &Arc<crate::Connection>,
) -> Result<usize> {
    let plan = prepare_select_plan(
        select,
        resolver,
        program,
        &[],
        query_destination,
        connection,
    )?;
    if program.trigger.is_some() {
        if let Some(virtual_table) = plan_first_virtual_table_name(&plan) {
            crate::bail_parse_error!("unsafe use of virtual table \"{}\"", virtual_table);
        }
    }
    emit_select_plan(plan, resolver, program, connection)
}

/// Optimize and emit bytecode for an already-prepared select plan.
#[turso_macros::trace_stack]
pub fn emit_select_plan(
    mut plan: Plan,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<usize> {
    optimize_plan(program, &mut plan, resolver)?;
    let num_result_cols;
    let opts = match &plan {
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
        Plan::RecursiveCte(recursive_cte) => {
            num_result_cols = recursive_cte.initial_query.select_result_columns().len();
            ProgramBuilderOpts {
                num_cursors: count_required_cursors_for_plan(&plan),
                approx_num_insns: estimate_num_instructions_for_plan(&plan),
                approx_num_labels: estimate_num_labels_for_plan(&plan),
            }
        }
        _ => crate::bail_parse_error!("emit_select_plan called with non-SELECT plan"),
    };

    program.extend(&opts);
    emit_program(connection, resolver, program, plan, |_| {})?;
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
        Plan::RecursiveCte(recursive_cte) => {
            plan_first_virtual_table_name(&recursive_cte.initial_query)
                .or_else(|| plan_first_virtual_table_name(&recursive_cte.recursive_query))
        }
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
    prepare_select_plan_from_arms(
        select.body.select,
        select.body.compounds,
        select.with,
        select.order_by,
        select.limit,
        resolver,
        program,
        outer_query_refs,
        query_destination,
        connection,
    )
}

/// Plans a first SELECT arm followed by zero or more compound arms.
///
/// Accepting the arms directly lets recursive CTE planning divide a stored
/// SELECT without allocating temporary compound-arm vectors.
#[allow(clippy::too_many_arguments)]
#[turso_macros::trace_stack]
pub(crate) fn prepare_select_plan_from_arms(
    first_arm: ast::OneSelect,
    compound_arms: impl IntoIterator<Item = CompoundSelect>,
    with: Option<ast::With>,
    order_by: Vec<ast::SortedColumn>,
    limit: Option<ast::Limit>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    outer_query_refs: &[OuterQueryReference],
    query_destination: QueryDestination,
    connection: &Arc<crate::Connection>,
) -> Result<Plan> {
    let mut compound_arms = compound_arms.into_iter().peekable();
    if compound_arms.peek().is_none() {
        return Ok(Plan::Select(Box::new(prepare_one_select_plan(
            first_arm,
            resolver,
            program,
            limit,
            order_by,
            with,
            outer_query_refs,
            query_destination,
            connection,
        )?)));
    }

    // The WITH clause applies to every arm, so each arm needs its own copy
    // while names are resolved.
    let mut last = prepare_one_select_plan(
        first_arm,
        resolver,
        program,
        None,
        vec![],
        with.clone(),
        outer_query_refs,
        query_destination.clone(),
        connection,
    )?;

    let mut left = Vec::with_capacity(compound_arms.size_hint().0);
    for CompoundSelect {
        select: compound_select,
        operator,
    } in compound_arms
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

    let right_most_num_result_columns = last.result_columns.len();
    for (plan, operator) in &left {
        if plan.result_columns.len() != right_most_num_result_columns {
            crate::bail_parse_error!(
                "SELECTs to the left and right of {} do not have the same number of result columns",
                operator
            );
        }
    }
    let (limit, offset) = limit.map_or(Ok((None, None)), |limit| parse_limit(limit, resolver))?;

    // ORDER BY names can come from any arm of a compound SELECT.
    let all_plans: Vec<&SelectPlan> = left
        .iter()
        .map(|(plan, _)| plan)
        .chain(std::iter::once(&last))
        .collect();
    let order_by = resolve_compound_order_by(&order_by, &all_plans, resolver)?;

    Ok(Plan::CompoundSelect {
        left,
        right_most: Box::new(last),
        limit,
        offset,
        order_by,
    })
}

#[allow(clippy::too_many_arguments)]
#[turso_macros::trace_stack]
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
            let table_refs_outside_from = if with.is_some() {
                collect_table_refs_outside_from(
                    &columns,
                    where_clause.as_deref(),
                    group_by.as_ref(),
                    &window_clause,
                    &order_by,
                    limit.as_ref(),
                )
            } else {
                Vec::new()
            };
            {
                parse_from(
                    from,
                    resolver,
                    program,
                    with,
                    &table_refs_outside_from,
                    &mut where_predicates,
                    &mut vtab_predicates,
                    &mut table_references,
                    connection,
                )?;
            }

            // Preallocate space for the result columns
            let result_columns = Vec::with_capacity(
                columns
                    .iter()
                    .map(|c| match c {
                        // Allocate space for all columns in all tables
                        ResultColumn::Star => table_references
                            .joined_tables()
                            .iter()
                            .map(|table| table.columns_for_star().count())
                            .sum(),
                        // Guess 5 columns if we can't find the table using the identifier (maybe it's in [brackets] or `tick_quotes`, or miXeDcAse)
                        ResultColumn::TableStar(n) => table_references
                            .joined_tables()
                            .iter()
                            .find(|t| t.identifier == n.as_str())
                            .map(|table| table.columns_for_star().count())
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
                estimated_cost: None,
                simple_aggregate: None,
            };

            let mut windows: Vec<Window> = Vec::new();
            let mut named_windows: Vec<NamedWindowDef> = Vec::with_capacity(window_clause.len());
            {
                trace_stack!("bind_windows");
                for window_def in window_clause.iter() {
                    let name = normalize_ident(window_def.name.as_str());
                    let mut partition_by: Vec<_> = window_def
                        .window
                        .partition_by
                        .iter()
                        .map(|arg| *arg.clone())
                        .collect();
                    let mut order_by: Vec<_> = window_def
                        .window
                        .order_by
                        .iter()
                        .map(|col| {
                            (
                                *col.expr.clone(),
                                col.order.unwrap_or(SortOrder::Asc),
                                col.nulls,
                            )
                        })
                        .collect();
                    for expr in partition_by.iter_mut() {
                        bind_and_rewrite_expr(
                            expr,
                            Some(&mut plan.table_references),
                            None,
                            resolver,
                            BindingBehavior::ResultColumnsNotAllowed,
                        )?;
                    }
                    for (expr, _, _) in order_by.iter_mut() {
                        bind_and_rewrite_expr(
                            expr,
                            Some(&mut plan.table_references),
                            None,
                            resolver,
                            BindingBehavior::ResultColumnsNotAllowed,
                        )?;
                    }
                    // Chain a `w2 AS (w1 ORDER BY ...)` definition to its base
                    // (sqlite3WindowChain, window.c:1276). SQLite's grammar
                    // only chains within the WINDOW clause, so:
                    // - only EARLIER definitions are candidates, and
                    // - the first definition is never chained — its base
                    //   name (or a forward reference) is silently ignored,
                    //   not an error.
                    if let Some(base) = &window_def.window.base {
                        if !named_windows.is_empty() {
                            let base_name = normalize_ident(base.as_str());
                            let Some(base_def) =
                                named_windows.iter().rfind(|d| d.name == base_name)
                            else {
                                crate::bail_parse_error!("no such window: {}", base_name);
                            };
                            if !window_def.window.partition_by.is_empty() {
                                crate::bail_parse_error!(
                                    "cannot override PARTITION clause of window: {}",
                                    base_name
                                );
                            }
                            let base_bound = base_def
                                .bound
                                .as_ref()
                                .expect("named defs retain bound until function resolution");
                            if !base_bound.order_by.is_empty() && !order_by.is_empty() {
                                crate::bail_parse_error!(
                                    "cannot override ORDER BY clause of window: {}",
                                    base_name
                                );
                            }
                            if base_def.user_frame_clause.is_some() {
                                crate::bail_parse_error!(
                                    "cannot override frame specification of window: {}",
                                    base_name
                                );
                            }
                            partition_by.clone_from(&base_bound.partition_by);
                            if order_by.is_empty() {
                                order_by.clone_from(&base_bound.order_by);
                            }
                        }
                    }
                    named_windows.push(NamedWindowDef {
                        name,
                        user_frame_clause: window_def.window.frame_clause.clone(),
                        bound: Some(NamedWindowBound {
                            partition_by,
                            order_by,
                        }),
                    });
                }
            }

            let long_names =
                connection.get_full_column_names() && !connection.get_short_column_names();
            let mut aggregate_expressions = Vec::new();
            {
                trace_stack!("bind_result_columns");
                for column in columns.into_iter() {
                    match column {
                        ResultColumn::Star => {
                            expand_star(
                                &mut plan.table_references,
                                &mut plan.result_columns,
                                long_names,
                                resolver,
                            )?;
                        }
                        ResultColumn::TableStar(name) => {
                            expand_table_star(
                                &mut plan.table_references,
                                &mut plan.result_columns,
                                name.as_str(),
                                long_names,
                            )?;
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
                                &mut named_windows,
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
            }

            if plan.result_columns.len() > crate::types::MAX_COLUMN {
                crate::bail_parse_error!("too many columns in result set");
            }

            // Parse the actual WHERE clause and add its conditions to the plan WHERE clause that already contains the join conditions.
            {
                parse_where(
                    where_clause.as_deref(),
                    &mut plan.table_references,
                    Some(&plan.result_columns),
                    &mut plan.where_clause,
                    resolver,
                )?;
            }

            // This step can only be performed at this point, because all table references are now available.
            // Virtual table predicates may depend on column bindings from tables to the right in the join order,
            // so we must wait until the full set of references has been collected.
            {
                // SQLite adds table-function arguments after ON and WHERE terms.
                // Some virtual tables select the last constraint for a hidden column.
                add_vtab_predicates_to_where_clause(&mut vtab_predicates, &mut plan, resolver)?;
            }

            {
                trace_stack!("process_group_by");
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
                                resolver,
                            )?;
                            bind_and_rewrite_expr(
                                expr,
                                Some(&mut plan.table_references),
                                Some(&plan.result_columns),
                                resolver,
                                BindingBehavior::TryCanonicalColumnsFirst,
                            )?;
                        }

                        let exprs: Vec<ast::Expr> =
                            group_by.exprs.into_iter().map(|expr| *expr).collect();
                        let having = match having_predicates {
                            Some(having) => Some(move_group_by_only_having_terms_to_where(
                                having,
                                &exprs,
                                &aggregate_expressions,
                                &plan.table_references,
                                resolver,
                                &mut plan.where_clause,
                            )?)
                            .filter(|having| !having.is_empty()),
                            None => None,
                        };
                        plan.group_by = Some(GroupBy {
                            sort_order: Vec::new(),
                            nulls_order: Vec::new(),
                            exprs,
                            sort_elided: false,
                            having,
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
            }

            plan.aggregates = aggregate_expressions;

            // An aggregate whose argument columns belong to the enclosing query
            // is computed by that query, not by this subquery (SQLite binds an
            // aggregate to the nearest query that supplies a referenced column).
            // Move those up so this subquery never computes them; the enclosing
            // query collects them once its subqueries are planned. When there is
            // no enclosing query the move fails and the aggregate stays here.
            let tables = &plan.table_references;
            plan.aggregates.retain(|agg| {
                !(aggregate_belongs_to_enclosing_query(agg, tables)
                    && resolver.move_aggregate_to_enclosing_query(agg.clone()))
            });

            if !plan.aggregates.is_empty()
                && plan
                    .group_by
                    .as_ref()
                    .is_none_or(|group_by| group_by.exprs.is_empty())
                && !resolver
                    .catalog
                    .dialect
                    .allows_bare_columns_in_aggregate_queries()
            {
                for result_column in plan
                    .result_columns
                    .iter()
                    .filter(|rc| !rc.contains_aggregates)
                {
                    if let Some(column) =
                        first_column_reference(&result_column.expr, &plan.table_references)?
                    {
                        crate::bail_parse_error!(
                            "column \"{column}\" must appear in the GROUP BY clause or be used in an aggregate function"
                        );
                    }
                }
            }

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
                    resolver,
                )?;

                bind_and_rewrite_expr(
                    &mut o.expr,
                    Some(&mut plan.table_references),
                    Some(&plan.result_columns),
                    resolver,
                    BindingBehavior::TryResultColumnsFirst,
                )?;
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

            // SQLite optimizes away ORDER BY clauses after a rowid/INTEGER PRIMARY KEY column
            // when it's FIRST in the ORDER BY, since the table is stored in rowid order.
            // This means we truncate the ORDER BY to just the rowid column.
            // We do this for SQLite compatibility - SQLite truncates before validating, so
            // even invalid constructions like ORDER BY rowid, a IN (SELECT a, b FROM t) pass.
            let mut terms_cut_after_rowid = Vec::new();
            if key.len() > 1 && plan.table_references.joined_tables().len() == 1 {
                let joined = &plan.table_references.joined_tables()[0];
                let table_id = joined.internal_id;
                let rowid_alias_col = joined
                    .btree()
                    .and_then(|t| t.get_rowid_alias_column().map(|(idx, _)| idx));

                let first_is_rowid = match key[0].0.as_ref() {
                    ast::Expr::Column { table, column, .. } => {
                        *table == table_id && rowid_alias_col == Some(*column)
                    }
                    ast::Expr::RowId { table, .. } => *table == table_id,
                    _ => false,
                };
                if first_is_rowid {
                    terms_cut_after_rowid = key.split_off(1);
                }
            }

            {
                trace_stack!("process_order_by");
                for (expr, _, _) in key.iter() {
                    let had_agg = resolve_window_and_aggregate_functions(
                        expr,
                        resolver,
                        &mut plan.aggregates,
                        Some(&mut windows),
                        &mut named_windows,
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
                }
                let mut cut_aggregates = plan.aggregates.clone();
                let mut cut_windows = windows.clone();
                let mut cut_named_windows = named_windows.clone();
                for (expr, _, _) in terms_cut_after_rowid.iter() {
                    resolve_window_and_aggregate_functions(
                        expr,
                        resolver,
                        &mut cut_aggregates,
                        Some(&mut cut_windows),
                        &mut cut_named_windows,
                    )?;
                }
                let is_aggregate_query = has_group_by || agg_count_before_order_by > 0;
                if !cut_windows.is_empty() && !is_aggregate_query {
                    for (expr, _, _) in terms_cut_after_rowid.iter() {
                        if let Some(name) = first_aggregate_without_over_clause(expr, resolver)? {
                            crate::bail_parse_error!("misuse of aggregate: {}()", name);
                        }
                    }
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
            {
                (plan.limit, plan.offset) =
                    limit.map_or(Ok((None, None)), |l| parse_limit(l, resolver))?;
            }

            if !windows.is_empty() {
                plan_windows(program, &mut plan, resolver, connection, &mut windows)?;
            }

            // Collect aggregates that this query's subqueries find to belong to
            // this query. Collection starts before the subqueries are planned
            // and ends after; the collected aggregates become this query's,
            // making it an aggregate query.
            resolver.begin_collecting_aggregates_from_subqueries();
            plan_subqueries_from_select_plan(program, &mut plan, resolver, connection)?;
            let aggregates_from_subqueries = resolver.take_aggregates_from_subqueries();
            plan.aggregates.extend(aggregates_from_subqueries);

            {
                trace_stack!("validate_plan");
                validate_group_by_outer_scope_refs(&plan)?;

                validate_expr_correct_column_counts(&plan)?;
            }

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
            if len > crate::types::MAX_COLUMN {
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
            let mut subquery_table_refs = Vec::new();
            for expr in values.iter().flatten() {
                collect_subquery_table_refs_in_expr(expr, &mut subquery_table_refs);
            }
            plan_ctes_as_outer_refs(
                with,
                &subquery_table_refs,
                resolver,
                program,
                &mut table_references,
                connection,
            )?;

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
                    .into_iter()
                    .map(|values| values.into_iter().map(|value| *value).collect())
                    .collect(),
                window: None,
                non_from_clause_subqueries,
                input_cardinality_hint: None,
                estimated_output_rows: None,
                estimated_cost: None,
                simple_aggregate: None,
            };

            validate_expr_correct_column_counts(&plan)?;

            Ok(plan)
        }
    }
}

/// The first column that `expr` references, as "table.column", for the
/// bare-column error of dialects that reject bare columns in aggregate
/// queries.
fn first_column_reference(expr: &ast::Expr, tables: &TableReferences) -> Result<Option<String>> {
    let mut found = None;
    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        if found.is_none() {
            if let ast::Expr::Column { table, column, .. } = e {
                if let Some(joined) = tables
                    .joined_tables()
                    .iter()
                    .find(|t| t.internal_id == *table)
                {
                    if let Some(name) = joined
                        .table
                        .get_column_at(*column)
                        .and_then(|c| c.name.as_deref())
                    {
                        found = Some(format!("{}.{}", joined.identifier, name));
                    }
                }
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(found)
}

fn first_aggregate_without_over_clause<'a>(
    expr: &'a ast::Expr,
    resolver: &Resolver,
) -> Result<Option<&'a str>> {
    let mut found = None;
    walk_expr(expr, &mut |e: &'a ast::Expr| -> Result<WalkControl> {
        let (name, args_count, filter_over) = match e {
            ast::Expr::FunctionCall {
                name,
                args,
                filter_over,
                ..
            } => (name, args.len(), filter_over),
            ast::Expr::FunctionCallStar { name, filter_over } => (name, 0, filter_over),
            _ => return Ok(WalkControl::Continue),
        };
        if found.is_some() || filter_over.over_clause.is_some() {
            return Ok(WalkControl::Continue);
        }
        let is_aggregate = match crate::function::Func::resolve_function(name.as_str(), args_count)
        {
            Ok(Some(crate::function::Func::Agg(_))) => true,
            _ => resolver
                .catalog
                .symbol_table
                .resolve_function(name.as_str(), args_count)
                .is_some_and(|f| f.func.is_aggregate()),
        };
        if is_aggregate {
            found = Some(name.as_str());
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(found)
}

/// Validate that all expressions in the plan return the correct number of values;
/// generally this only applies to parenthesized lists and subqueries.
fn validate_expr_correct_column_counts(plan: &SelectPlan) -> Result<()> {
    for result_column in plan.result_columns.iter() {
        let vec_size = expr_vector_size(&result_column.expr)?;
        if vec_size != 1 {
            crate::bail_parse_error!("row value misused");
        }
    }
    for (expr, _, _) in plan.order_by.iter() {
        let vec_size = expr_vector_size(expr)?;
        if vec_size != 1 {
            crate::bail_parse_error!("row value misused");
        }
    }
    if let Some(group_by) = &plan.group_by {
        for expr in group_by.exprs.iter() {
            let vec_size = expr_vector_size(expr)?;
            if vec_size != 1 {
                crate::bail_parse_error!("row value misused");
            }
        }
        if let Some(having) = &group_by.having {
            for expr in having.iter() {
                let vec_size = expr_vector_size(expr)?;
                if vec_size != 1 {
                    crate::bail_parse_error!("row value misused");
                }
            }
        }
    }
    for aggregate in plan.aggregates.iter() {
        for arg in aggregate.input_exprs() {
            let vec_size = expr_vector_size(arg)?;
            if vec_size != 1 {
                crate::bail_parse_error!("row value misused");
            }
        }
    }
    for term in plan.where_clause.iter() {
        let vec_size = expr_vector_size(&term.expr)?;
        if vec_size != 1 {
            crate::bail_parse_error!("row value misused");
        }
    }
    for expr in plan.values.iter() {
        for value in expr.iter() {
            let vec_size = expr_vector_size(value)?;
            if vec_size != 1 {
                crate::bail_parse_error!("row value misused");
            }
        }
    }
    if let Some(limit) = &plan.limit {
        let vec_size = expr_vector_size(limit)?;
        if vec_size != 1 {
            crate::bail_parse_error!("row value misused");
        }
    }
    if let Some(offset) = &plan.offset {
        let vec_size = expr_vector_size(offset)?;
        if vec_size != 1 {
            crate::bail_parse_error!("row value misused");
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
                        normalize_ident(column_name)
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
        Plan::RecursiveCte(recursive_cte) => {
            reject_outer_scope_refs_inside_plan_tree(
                &recursive_cte.initial_query,
                current_scope_table_refs,
            )?;
            reject_outer_scope_refs_inside_plan_tree(
                &recursive_cte.recursive_query,
                current_scope_table_refs,
            )
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
            crate::bail_parse_error!(
                "no such column: {}.{}",
                outer_ref.identifier,
                normalize_ident(column_name)
            );
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

#[turso_macros::trace_stack]
fn add_vtab_predicates_to_where_clause(
    vtab_predicates: &mut Vec<Expr>,
    plan: &mut SelectPlan,
    resolver: &Resolver,
) -> Result<()> {
    append_vtab_predicates_to_where_clause(
        vtab_predicates,
        &mut plan.table_references,
        &plan.result_columns,
        &mut plan.where_clause,
        resolver,
    )
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
    resolver: &Resolver,
) -> Result<()> {
    match order_by_or_group_by_expr {
        ast::Expr::Collate(inner, _) => {
            return replace_column_number_with_copy_of_column_expr(
                inner,
                columns,
                clause_name,
                resolver,
            );
        }
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            return replace_column_number_with_copy_of_column_expr(
                exprs[0].as_mut(),
                columns,
                clause_name,
                resolver,
            );
        }
        _ => {}
    }

    if !resolver.catalog.dialect.allows_non_integer_sort_constants() {
        reject_non_integer_sort_constant(order_by_or_group_by_expr, clause_name)?;
    }

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
                if num.parse::<i32>().is_ok() {
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
        // Only treat as column reference if it parses as an integer that fits
        // a 32-bit int, mirroring SQLite's sqlite3ExprIsInteger. Float
        // literals like "0.5" and integers past the 32-bit range are valid
        // constant expressions, not column references.
        if let Ok(column_number) = num.parse::<i32>() {
            if column_number <= 0 || column_number as usize > columns.len() {
                crate::bail_parse_error!(
                    "1st {} term out of range - should be between 1 and {}",
                    clause_name,
                    columns.len()
                );
            }
            let ResultSetColumn { expr, .. } = &columns[column_number as usize - 1];
            *order_by_or_group_by_expr = expr.clone();
        }
        // Otherwise, leave the expression as-is (constant expression, case 3 per SQLite docs)
    }
    Ok(())
}

/// PostgreSQL reads an integer constant in ORDER BY or GROUP BY as a
/// select-list position and rejects any other constant as a syntax error.
/// The translator writes TRUE and FALSE as casts of 1 and 0 to boolean, so
/// those count as constants here too.
fn reject_non_integer_sort_constant(expr: &ast::Expr, clause_name: &str) -> Result<()> {
    let non_integer = match expr {
        ast::Expr::Literal(ast::Literal::String(_)) => true,
        ast::Expr::Literal(ast::Literal::Numeric(num)) => num.parse::<i64>().is_err(),
        ast::Expr::Cast {
            expr: inner,
            type_name: Some(ty),
        } => {
            (ty.name.eq_ignore_ascii_case("boolean") || ty.name.eq_ignore_ascii_case("bool"))
                && matches!(inner.as_ref(), ast::Expr::Literal(ast::Literal::Numeric(_)))
        }
        _ => false,
    };
    if non_integer {
        crate::bail_parse_error!("non-integer constant in {}", clause_name);
    }
    Ok(())
}

/// Resolves a compound SELECT ORDER BY term to a 0-based result column index.
fn resolve_compound_order_by_expr(
    expr: &ast::Expr,
    all_plans: &[&SelectPlan],
    resolver: &Resolver,
    term_number: usize,
) -> Result<(usize, Option<CollationSeq>)> {
    let num_result_columns = all_plans[0].result_columns.len();
    match expr {
        // An explicit COLLATE wraps the column reference. Resolve the inner
        // reference and carry the collation so it overrides the referenced
        // column's own collation when the compound result is sorted.
        ast::Expr::Collate(inner, collation_name) => {
            let (col_idx, _) =
                resolve_compound_order_by_expr(inner, all_plans, resolver, term_number)?;
            return Ok((col_idx, Some(CollationSeq::new(collation_name.as_str())?)));
        }
        // Case 1: Numeric column reference (e.g., ORDER BY 1). As in SQLite's
        // sqlite3ExprIsInteger, only literals that fit a 32-bit int count as
        // column positions.
        ast::Expr::Literal(ast::Literal::Numeric(num)) => {
            if let Ok(column_number) = num.parse::<i32>() {
                if column_number <= 0 || column_number as usize > num_result_columns {
                    crate::bail_parse_error!(
                        "{} ORDER BY term out of range - should be between 1 and {}",
                        column_number,
                        num_result_columns
                    );
                }
                return Ok((column_number as usize - 1, None));
            }
        }
        _ => {}
    }

    for plan in all_plans {
        if let ast::Expr::Id(name) = expr {
            let normalized_name = normalize_ident(name.as_str());
            for (index, result_column) in plan.result_columns.iter().enumerate() {
                if let Some(alias) = &result_column.alias {
                    if normalize_ident(alias) == normalized_name {
                        return Ok((index, None));
                    }
                }
            }
            for (index, result_column) in plan.result_columns.iter().enumerate() {
                if let Some(column_name) = result_column.name(&plan.table_references) {
                    if normalize_ident(column_name) == normalized_name {
                        return Ok((index, None));
                    }
                }
            }
        }

        let mut bound_expr = expr.clone();
        let mut table_references = plan.table_references.clone();
        if bind_and_rewrite_expr(
            &mut bound_expr,
            Some(&mut table_references),
            None,
            resolver,
            BindingBehavior::ResultColumnsNotAllowed,
        )
        .is_err()
        {
            continue;
        }
        if let Some(index) = plan
            .result_columns
            .iter()
            .position(|result_column| exprs_are_equivalent(&bound_expr, &result_column.expr))
        {
            return Ok((index, None));
        }
    }

    crate::bail_parse_error!(
        "{} ORDER BY term does not match any column in the result set",
        ordinal(term_number)
    );
}

fn resolve_compound_order_by(
    order_by: &[ast::SortedColumn],
    plans: &[&SelectPlan],
    resolver: &Resolver,
) -> Result<Option<Vec<super::plan::CompoundOrderByKey>>> {
    if order_by.is_empty() {
        return Ok(None);
    }
    order_by
        .iter()
        .enumerate()
        .map(|(index, term)| {
            let (column, collation) =
                resolve_compound_order_by_expr(&term.expr, plans, resolver, index + 1)?;
            Ok((
                column,
                term.order.unwrap_or(ast::SortOrder::Asc),
                term.nulls,
                collation,
            ))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

pub(crate) fn resolve_recursive_cte_queue_order(
    order_by: &[ast::SortedColumn],
    initial_query: &Plan,
    recursive_query: &Plan,
    resolver: &Resolver,
) -> Result<Option<Vec<super::plan::CompoundOrderByKey>>> {
    fn collect_selects<'a>(plan: &'a Plan, out: &mut Vec<&'a SelectPlan>) {
        match plan {
            Plan::Select(plan) => out.push(plan),
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                out.extend(left.iter().map(|(plan, _)| plan));
                out.push(right_most);
            }
            Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => {
                unreachable!("recursive CTE queries must be SELECT plans")
            }
        }
    }

    let mut plans = Vec::new();
    collect_selects(initial_query, &mut plans);
    collect_selects(recursive_query, &mut plans);
    resolve_compound_order_by(order_by, &plans, resolver)
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

/// Counts cursors needed to emit a query plan.
fn count_required_cursors_for_plan(plan: &Plan) -> usize {
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
        Plan::RecursiveCte(recursive_cte) => {
            count_required_cursors_for_plan(&recursive_cte.initial_query)
                + count_required_cursors_for_plan(&recursive_cte.recursive_query)
                + 2
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
            count_required_cursors_for_plan(&from_clause_subquery.plan)
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

/// Estimates bytecode instructions needed to emit a query plan.
fn estimate_num_instructions_for_plan(plan: &Plan) -> usize {
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
        Plan::RecursiveCte(recursive_cte) => {
            estimate_num_instructions_for_plan(&recursive_cte.initial_query)
                + estimate_num_instructions_for_plan(&recursive_cte.recursive_query)
                + 32
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
            10 + estimate_num_instructions_for_plan(&from_clause_subquery.plan)
        } else {
            0
        })
        .sum();

    let group_by_instructions = select.group_by.is_some() as usize * 10;
    let order_by_instructions = !select.order_by.is_empty() as usize * 10;
    let condition_instructions = select.where_clause.len() * 3;

    20 + table_instructions + group_by_instructions + order_by_instructions + condition_instructions
}

/// Collects the table names read by subqueries outside the FROM clause.
fn collect_table_refs_outside_from(
    columns: &[ResultColumn],
    where_clause: Option<&Expr>,
    group_by: Option<&ast::GroupBy>,
    window_clause: &[ast::WindowDef],
    order_by: &[ast::SortedColumn],
    limit: Option<&ast::Limit>,
) -> Vec<String> {
    let mut table_refs = Vec::new();
    for column in columns {
        if let ResultColumn::Expr(expr, _) = column {
            collect_subquery_table_refs_in_expr(expr, &mut table_refs);
        }
    }
    if let Some(where_clause) = where_clause {
        collect_subquery_table_refs_in_expr(where_clause, &mut table_refs);
    }
    if let Some(group_by) = group_by {
        for expr in &group_by.exprs {
            collect_subquery_table_refs_in_expr(expr, &mut table_refs);
        }
        if let Some(having) = &group_by.having {
            collect_subquery_table_refs_in_expr(having, &mut table_refs);
        }
    }
    collect_subquery_table_refs_in_window_clause(window_clause, &mut table_refs);
    for sorted in order_by {
        collect_subquery_table_refs_in_expr(&sorted.expr, &mut table_refs);
    }
    if let Some(limit) = limit {
        collect_subquery_table_refs_in_expr(&limit.expr, &mut table_refs);
        if let Some(offset) = &limit.offset {
            collect_subquery_table_refs_in_expr(offset, &mut table_refs);
        }
    }
    table_refs
}

/// Estimates jump labels needed to emit a query plan.
fn estimate_num_labels_for_plan(plan: &Plan) -> usize {
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
        Plan::RecursiveCte(recursive_cte) => {
            estimate_num_labels_for_plan(&recursive_cte.initial_query)
                + estimate_num_labels_for_plan(&recursive_cte.recursive_query)
                + 4
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
            3 + estimate_num_labels_for_plan(&from_clause_subquery.plan)
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
    t_ctx
        .resolver
        .cache_expr_reg(Cow::Owned(agg.original_expr.clone()), target_reg, false);
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

/// True when this aggregate belongs to the immediate enclosing query rather
/// than the subquery it is written in: every argument column is a reference to
/// the enclosing query (scope depth 0), and none is this query's own column.
/// SQLite binds an aggregate to the nearest query that supplies a referenced
/// column; an argument column referencing a further ancestor (scope depth > 0)
/// is not handled here, so the aggregate stays with this query.
fn aggregate_belongs_to_enclosing_query(
    agg: &super::plan::Aggregate,
    tables: &TableReferences,
) -> bool {
    let mut belongs = false;
    let mut disqualified = false;
    for arg in agg.input_exprs() {
        let _ = walk_expr(arg, &mut |e: &ast::Expr| -> Result<WalkControl> {
            if let ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } = e {
                if tables
                    .joined_tables()
                    .iter()
                    .any(|t| t.internal_id == *table)
                {
                    disqualified = true;
                } else if let Some(outer) = tables.find_outer_query_ref_by_internal_id(*table) {
                    if outer.scope_depth == 0 {
                        belongs = true;
                    } else {
                        disqualified = true;
                    }
                }
            }
            Ok(WalkControl::Continue)
        });
    }
    belongs && !disqualified
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
        resolve_window_and_aggregate_functions(
            expr,
            resolver,
            aggregate_expressions,
            None,
            &mut [],
        )?;
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

/// Moves each HAVING term that reads only constants and GROUP BY terms into WHERE,
/// like SQLite's havingToWhere(). Such a term has the same value for every row of a
/// group, so filtering the rows before grouping removes the same groups, and the
/// aggregates of those groups are never computed. Like SQLite, a literal false term
/// stays in HAVING.
fn move_group_by_only_having_terms_to_where(
    having: Vec<ast::Expr>,
    group_by_exprs: &[ast::Expr],
    aggregates: &[super::plan::Aggregate],
    table_references: &TableReferences,
    resolver: &Resolver,
    where_clause: &mut Vec<WhereTerm>,
) -> Result<Vec<ast::Expr>> {
    let mut remaining = Vec::new();
    for term in having {
        if !is_literal_false(&term)
            && is_constant_or_group_by(
                &term,
                group_by_exprs,
                aggregates,
                table_references,
                resolver,
            )?
        {
            where_clause.push(term.into());
        } else {
            remaining.push(term);
        }
    }
    Ok(remaining)
}

/// Matches the terms SQLite keeps in HAVING as always false (`EP_IsFalse`): integer
/// literal zero (decimal or hex, without `_` separators), `FALSE` and `x IN ()`.
/// `Expr::is_always_false()` is not used because it also matches other constant false
/// expressions such as `0.0`, `'0'` and `NOT 1`, which SQLite moves into WHERE.
/// SQLite's parser also folds `X AND 0` into `0` when X has no function call, so it
/// keeps `HAVING g = 2 AND 0` in HAVING, while here `g = 2` is moved.
fn is_literal_false(expr: &ast::Expr) -> bool {
    match expr {
        Expr::Literal(ast::Literal::Numeric(n)) => {
            match n.strip_prefix("0x").or_else(|| n.strip_prefix("0X")) {
                Some(hex_digits) => u64::from_str_radix(hex_digits, 16) == Ok(0),
                None => n.parse::<i64>() == Ok(0),
            }
        }
        Expr::Literal(ast::Literal::False) => true,
        Expr::InList {
            rhs, not: false, ..
        } => rhs.is_empty(),
        _ => false,
    }
}

/// Checks that `term` has no aggregates and is constant once every GROUP BY term in
/// it is replaced by NULL. Like SQLite's sqlite3ExprIsConstantOrGroupBy(), a GROUP BY
/// term is only replaced when it groups with BINARY collation, because rows that
/// another collation puts in one group can differ under BINARY.
fn is_constant_or_group_by(
    term: &ast::Expr,
    group_by_exprs: &[ast::Expr],
    aggregates: &[super::plan::Aggregate],
    table_references: &TableReferences,
    resolver: &Resolver,
) -> Result<bool> {
    let mut term = term.clone();
    let mut has_aggregate = false;
    walk_expr_mut(&mut term, &mut |e: &mut ast::Expr| -> Result<WalkControl> {
        if aggregates
            .iter()
            .any(|agg| exprs_are_equivalent(&agg.original_expr, e))
        {
            has_aggregate = true;
            return Ok(WalkControl::SkipChildren);
        }
        for group_by_expr in group_by_exprs {
            if exprs_are_equivalent(e, group_by_expr)
                && ExprContext::new(Some(table_references), resolver)
                    .expr_collation(group_by_expr)?
                    == CollationSeq::BINARY
            {
                *e = Expr::Literal(ast::Literal::Null);
                return Ok(WalkControl::SkipChildren);
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(!has_aggregate && term.is_constant(resolver))
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
