use super::emitter::{emit_program, TranslateCtx};
use super::plan::{
    select_star, Distinctness, JoinOrderMember, Operation, OuterQueryReference, QueryDestination,
    Search, TableReferences, WhereTerm, Window,
};
use crate::schema::Table;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{bind_and_rewrite_expr, BindingBehavior, ParamState};
use crate::translate::group_by::compute_group_by_sort_order;
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{GroupBy, Plan, ResultSetColumn, SelectPlan};
use crate::translate::planner::{
    break_predicate_at_and_boundaries, parse_from, parse_limit, parse_where,
    resolve_window_and_aggregate_functions,
};
use crate::translate::window::plan_windows;
use crate::util::normalize_ident;
use crate::vdbe::builder::ProgramBuilderOpts;
use crate::vdbe::insn::Insn;
use crate::Connection;
use crate::{vdbe::builder::ProgramBuilder, Result};
use std::sync::Arc;
use turso_parser::ast::ResultColumn;
use turso_parser::ast::{self, CompoundSelect, Expr};

pub struct TranslateSelectResult {
    pub program: ProgramBuilder,
    pub num_result_cols: usize,
}

pub fn translate_select(
    select: ast::Select,
    resolver: &Resolver,
    mut program: ProgramBuilder,
    query_destination: QueryDestination,
    connection: &Arc<crate::Connection>,
) -> Result<TranslateSelectResult> {
    let mut select_plan = prepare_select_plan(
        select,
        resolver,
        &mut program,
        &[],
        query_destination,
        connection,
    )?;
    optimize_plan(&mut select_plan, resolver.schema)?;
    let num_result_cols;
    let opts = match &select_plan {
        Plan::Select(select) => {
            num_result_cols = select.result_columns.len();
            ProgramBuilderOpts {
                num_cursors: count_plan_required_cursors(select),
                approx_num_insns: estimate_num_instructions(select),
                approx_num_labels: estimate_num_labels(select),
            }
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            // Compound Selects must return the same number of columns
            num_result_cols = right_most.result_columns.len();

            ProgramBuilderOpts {
                num_cursors: count_plan_required_cursors(right_most)
                    + left
                        .iter()
                        .map(|(plan, _)| count_plan_required_cursors(plan))
                        .sum::<usize>(),
                approx_num_insns: estimate_num_instructions(right_most)
                    + left
                        .iter()
                        .map(|(plan, _)| estimate_num_instructions(plan))
                        .sum::<usize>(),
                approx_num_labels: estimate_num_labels(right_most)
                    + left
                        .iter()
                        .map(|(plan, _)| estimate_num_labels(plan))
                        .sum::<usize>(),
            }
        }
        other => panic!("plan is not a SelectPlan: {other:?}"),
    };

    program.extend(&opts);
    emit_program(connection, resolver, &mut program, select_plan, |_| {})?;
    Ok(TranslateSelectResult {
        program,
        num_result_cols,
    })
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
            let mut last = prepare_one_select_plan(
                select.body.select,
                resolver,
                program,
                None,
                vec![],
                None,
                outer_query_refs,
                query_destination.clone(),
                connection,
            )?;

            let mut left = Vec::with_capacity(compounds.len());
            for CompoundSelect { select, operator } in compounds {
                left.push((last, operator));
                last = prepare_one_select_plan(
                    select,
                    resolver,
                    program,
                    None,
                    vec![],
                    None,
                    outer_query_refs,
                    query_destination.clone(),
                    connection,
                )?;
            }

            // Ensure all subplans have the same number of result columns
            let right_most_num_result_columns = last.result_columns.len();
            for (plan, operator) in left.iter() {
                if plan.result_columns.len() != right_most_num_result_columns {
                    crate::bail_parse_error!("SELECTs to the left and right of {} do not have the same number of result columns", operator);
                }
            }
            let (limit, offset) = select.limit.map_or(Ok((None, None)), |mut l| {
                parse_limit(&mut l, connection, &mut program.param_ctx)
            })?;

            // FIXME: handle ORDER BY for compound selects
            if !select.order_by.is_empty() {
                crate::bail_parse_error!("ORDER BY is not supported for compound SELECTs yet");
            }
            // FIXME: handle WITH for compound selects
            if select.with.is_some() {
                crate::bail_parse_error!("WITH is not supported for compound SELECTs yet");
            }
            Ok(Plan::CompoundSelect {
                left,
                right_most: last,
                limit,
                offset,
                order_by: None,
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
            mut columns,
            from,
            where_clause,
            group_by,
            distinctness,
            window_clause,
            ..
        } => {
            if !resolver.schema.indexes_enabled() && distinctness.is_some() {
                crate::bail_parse_error!(
                    "SELECT with DISTINCT is not allowed without indexes enabled"
                );
            }

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
            parse_from(
                from,
                resolver,
                program,
                with,
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
                            .map(|t| t.columns().iter().filter(|col| !col.hidden).count())
                            .sum(),
                        // Guess 5 columns if we can't find the table using the identifier (maybe it's in [brackets] or `tick_quotes`, or miXeDcAse)
                        ResultColumn::TableStar(n) => table_references
                            .joined_tables()
                            .iter()
                            .find(|t| t.identifier == n.as_str())
                            .map(|t| t.columns().iter().filter(|col| !col.hidden).count())
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
                        is_outer: t.join_info.as_ref().is_some_and(|j| j.outer),
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
                        connection,
                        &mut program.param_ctx,
                        BindingBehavior::ResultColumnsNotAllowed,
                    )?;
                }
                for (expr, _) in window.order_by.iter_mut() {
                    bind_and_rewrite_expr(
                        expr,
                        Some(&mut plan.table_references),
                        None,
                        connection,
                        &mut program.param_ctx,
                        BindingBehavior::ResultColumnsNotAllowed,
                    )?;
                }

                windows.push(window);
            }

            let mut aggregate_expressions = Vec::new();
            for column in columns.iter_mut() {
                match column {
                    ResultColumn::Star => {
                        select_star(
                            plan.table_references.joined_tables(),
                            &mut plan.result_columns,
                        );
                        for table in plan.table_references.joined_tables_mut() {
                            for idx in 0..table.columns().len() {
                                let column = &table.columns()[idx];
                                if column.hidden {
                                    continue;
                                }
                                table.mark_column_used(idx);
                            }
                        }
                    }
                    ResultColumn::TableStar(name) => {
                        let name_normalized = normalize_ident(name.as_str());
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
                            if column.hidden {
                                continue;
                            }
                            plan.result_columns.push(ResultSetColumn {
                                expr: ast::Expr::Column {
                                    database: None, // TODO: support different databases
                                    table: table.internal_id,
                                    column: idx,
                                    is_rowid_alias: column.is_rowid_alias,
                                },
                                alias: None,
                                contains_aggregates: false,
                            });
                            table.mark_column_used(idx);
                        }
                    }
                    ResultColumn::Expr(ref mut expr, maybe_alias) => {
                        bind_and_rewrite_expr(
                            expr,
                            Some(&mut plan.table_references),
                            None,
                            connection,
                            &mut program.param_ctx,
                            BindingBehavior::ResultColumnsNotAllowed,
                        )?;
                        let contains_aggregates = resolve_window_and_aggregate_functions(
                            expr,
                            resolver,
                            &mut aggregate_expressions,
                            Some(&mut windows),
                        )?;
                        plan.result_columns.push(ResultSetColumn {
                            alias: maybe_alias.as_ref().map(|alias| match alias {
                                ast::As::Elided(alias) => alias.as_str().to_string(),
                                ast::As::As(alias) => alias.as_str().to_string(),
                            }),
                            expr: expr.as_ref().clone(),
                            contains_aggregates,
                        });
                    }
                }
            }

            // This step can only be performed at this point, because all table references are now available.
            // Virtual table predicates may depend on column bindings from tables to the right in the join order,
            // so we must wait until the full set of references has been collected.
            add_vtab_predicates_to_where_clause(
                &mut vtab_predicates,
                &mut plan,
                connection,
                &mut program.param_ctx,
            )?;

            // Parse the actual WHERE clause and add its conditions to the plan WHERE clause that already contains the join conditions.
            parse_where(
                where_clause.as_deref(),
                &mut plan.table_references,
                Some(&plan.result_columns),
                &mut plan.where_clause,
                connection,
                &mut program.param_ctx,
            )?;

            if let Some(mut group_by) = group_by {
                for expr in group_by.exprs.iter_mut() {
                    replace_column_number_with_copy_of_column_expr(expr, &plan.result_columns)?;
                    bind_and_rewrite_expr(
                        expr,
                        Some(&mut plan.table_references),
                        Some(&plan.result_columns),
                        connection,
                        &mut program.param_ctx,
                        BindingBehavior::TryResultColumnsFirst,
                    )?;
                }

                plan.group_by = Some(GroupBy {
                    sort_order: None,
                    exprs: group_by.exprs.iter().map(|expr| *expr.clone()).collect(),
                    having: if let Some(having) = group_by.having {
                        let mut predicates = vec![];
                        break_predicate_at_and_boundaries(&having, &mut predicates);
                        for expr in predicates.iter_mut() {
                            bind_and_rewrite_expr(
                                expr,
                                Some(&mut plan.table_references),
                                Some(&plan.result_columns),
                                connection,
                                &mut program.param_ctx,
                                BindingBehavior::TryResultColumnsFirst,
                            )?;
                            let contains_aggregates = resolve_window_and_aggregate_functions(
                                expr,
                                resolver,
                                &mut aggregate_expressions,
                                None,
                            )?;
                            if !contains_aggregates {
                                // TODO: sqlite allows HAVING clauses with non aggregate expressions like
                                // HAVING id = 5. We should support this too eventually (I guess).
                                // sqlite3-parser does not support HAVING without group by though, so we'll
                                // need to either make a PR or add it to our vendored version.
                                crate::bail_parse_error!(
                                    "HAVING clause must contain an aggregate function"
                                );
                            }
                        }
                        Some(predicates)
                    } else {
                        None
                    },
                });
            }

            plan.aggregates = aggregate_expressions;

            // Parse the ORDER BY clause
            let mut key = Vec::new();

            for mut o in order_by {
                replace_column_number_with_copy_of_column_expr(&mut o.expr, &plan.result_columns)?;

                bind_and_rewrite_expr(
                    &mut o.expr,
                    Some(&mut plan.table_references),
                    Some(&plan.result_columns),
                    connection,
                    &mut program.param_ctx,
                    BindingBehavior::TryResultColumnsFirst,
                )?;
                resolve_window_and_aggregate_functions(
                    &o.expr,
                    resolver,
                    &mut plan.aggregates,
                    Some(&mut windows),
                )?;

                key.push((o.expr, o.order.unwrap_or(ast::SortOrder::Asc)));
            }
            plan.order_by = key;
            if let Some(group_by) = &mut plan.group_by {
                // now that we have resolved the ORDER BY expressions and aggregates, we can
                // compute the necessary sort order for the GROUP BY clause
                group_by.sort_order = Some(compute_group_by_sort_order(
                    &group_by.exprs,
                    &plan.order_by,
                    &plan.aggregates,
                    resolver,
                ));
            }

            // Parse the LIMIT/OFFSET clause
            (plan.limit, plan.offset) = limit.map_or(Ok((None, None)), |mut l| {
                parse_limit(&mut l, connection, &mut program.param_ctx)
            })?;

            if !windows.is_empty() {
                plan_windows(
                    &mut plan,
                    resolver,
                    &mut program.table_reference_counter,
                    &mut windows,
                )?;
            }

            // Return the unoptimized query plan
            Ok(plan)
        }
        ast::OneSelect::Values(values) => {
            if !order_by.is_empty() {
                crate::bail_parse_error!("ORDER BY clause is not allowed with VALUES clause");
            }
            if limit.is_some() {
                crate::bail_parse_error!("LIMIT clause is not allowed with VALUES clause");
            }
            let len = values[0].len();
            let mut result_columns = Vec::with_capacity(len);
            for i in 0..len {
                result_columns.push(ResultSetColumn {
                    // these result_columns work as placeholders for the values, so the expr doesn't matter
                    expr: ast::Expr::Literal(ast::Literal::Numeric(i.to_string())),
                    alias: None,
                    contains_aggregates: false,
                });
            }
            let plan = SelectPlan {
                join_order: vec![],
                table_references: TableReferences::new(vec![], vec![]),
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
            };

            Ok(plan)
        }
    }
}

fn add_vtab_predicates_to_where_clause(
    vtab_predicates: &mut Vec<Expr>,
    plan: &mut SelectPlan,
    connection: &Arc<Connection>,
    param_ctx: &mut ParamState,
) -> Result<()> {
    for expr in vtab_predicates.iter_mut() {
        bind_and_rewrite_expr(
            expr,
            Some(&mut plan.table_references),
            Some(&plan.result_columns),
            connection,
            param_ctx,
            BindingBehavior::TryCanonicalColumnsFirst,
        )?;
    }
    for expr in vtab_predicates.drain(..) {
        plan.where_clause.push(WhereTerm {
            expr,
            from_outer_join: None,
            consumed: false,
        });
    }
    Ok(())
}

/// Replaces a column number in an ORDER BY or GROUP BY expression with a copy of the column expression.
/// For example, in SELECT u.first_name, count(1) FROM users u GROUP BY 1 ORDER BY 2,
/// the column number 1 is replaced with u.first_name and the column number 2 is replaced with count(1).
fn replace_column_number_with_copy_of_column_expr(
    order_by_or_group_by_expr: &mut ast::Expr,
    columns: &[ResultSetColumn],
) -> Result<()> {
    if let ast::Expr::Literal(ast::Literal::Numeric(num)) = order_by_or_group_by_expr {
        let column_number = num.parse::<usize>()?;
        if column_number == 0 {
            crate::bail_parse_error!("invalid column index: {}", column_number);
        }
        let maybe_result_column = columns.get(column_number - 1);
        match maybe_result_column {
            Some(ResultSetColumn { expr, .. }) => {
                *order_by_or_group_by_expr = expr.clone();
            }
            None => {
                crate::bail_parse_error!("invalid column index: {}", column_number)
            }
        };
    }
    Ok(())
}

fn count_plan_required_cursors(plan: &SelectPlan) -> usize {
    let num_table_cursors: usize = plan
        .joined_tables()
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 1,
            Operation::Search(search) => match search {
                Search::RowidEq { .. } => 1,
                Search::Seek { index, .. } => 1 + index.is_some() as usize,
            }
        } + if let Table::FromClauseSubquery(from_clause_subquery) = &t.table {
            count_plan_required_cursors(&from_clause_subquery.plan)
        } else {
            0
        })
        .sum();
    let num_sorter_cursors = plan.group_by.is_some() as usize + !plan.order_by.is_empty() as usize;
    let num_pseudo_cursors = plan.group_by.is_some() as usize + !plan.order_by.is_empty() as usize;

    num_table_cursors + num_sorter_cursors + num_pseudo_cursors
}

fn estimate_num_instructions(select: &SelectPlan) -> usize {
    let table_instructions: usize = select
        .joined_tables()
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 10,
            Operation::Search(_) => 15,
        } + if let Table::FromClauseSubquery(from_clause_subquery) = &t.table {
            10 + estimate_num_instructions(&from_clause_subquery.plan)
        } else {
            0
        })
        .sum();

    let group_by_instructions = select.group_by.is_some() as usize * 10;
    let order_by_instructions = !select.order_by.is_empty() as usize * 10;
    let condition_instructions = select.where_clause.len() * 3;

    20 + table_instructions + group_by_instructions + order_by_instructions + condition_instructions
}

fn estimate_num_labels(select: &SelectPlan) -> usize {
    let init_halt_labels = 2;
    // 3 loop labels for each table in main loop + 1 to signify end of main loop
    let table_labels = select
        .joined_tables()
        .iter()
        .map(|t| match &t.op {
            Operation::Scan { .. } => 3,
            Operation::Search(_) => 3,
        } + if let Table::FromClauseSubquery(from_clause_subquery) = &t.table {
            3 + estimate_num_labels(&from_clause_subquery.plan)
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
    _t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let cursors = plan
        .joined_tables()
        .first()
        .unwrap()
        .resolve_cursors(program)?;

    let cursor_id = {
        match cursors {
            (_, Some(cursor_id)) | (Some(cursor_id), None) => cursor_id,
            _ => panic!("cursor for table should have been opened"),
        }
    };

    // TODO: I think this allocation can be avoided if we are smart with the `TranslateCtx`
    let target_reg = program.alloc_register();

    program.emit_insn(Insn::Count {
        cursor_id,
        target_reg,
        exact: true,
    });

    program.emit_insn(Insn::Close { cursor_id });
    let output_reg = program.alloc_register();
    program.emit_insn(Insn::Copy {
        src_reg: target_reg,
        dst_reg: output_reg,
        extra_amount: 0,
    });
    program.emit_result_row(output_reg, 1);
    Ok(())
}
