use super::emitter::{emit_program, TranslateCtx};
use super::plan::{
    select_star, Distinctness, JoinOrderMember, Operation, OuterQueryReference, QueryDestination,
    Search, TableReferences,
};
use crate::function::{AggFunc, ExtFunc, Func};
use crate::schema::Table;
use crate::translate::optimizer::optimize_plan;
use crate::translate::plan::{Aggregate, GroupBy, Plan, ResultSetColumn, SelectPlan};
use crate::translate::planner::{
    bind_column_references, break_predicate_at_and_boundaries, parse_from, parse_limit,
    parse_where, resolve_aggregates,
};
use crate::util::normalize_ident;
use crate::vdbe::builder::{ProgramBuilderOpts, TableRefIdCounter};
use crate::vdbe::insn::Insn;
use crate::SymbolTable;
use crate::{schema::Schema, vdbe::builder::ProgramBuilder, Result};
use turso_sqlite3_parser::ast::{self, CompoundSelect, SortOrder};
use turso_sqlite3_parser::ast::{ResultColumn, SelectInner};

pub struct TranslateSelectResult {
    pub program: ProgramBuilder,
    pub num_result_cols: usize,
}

pub fn translate_select(
    schema: &Schema,
    select: ast::Select,
    syms: &SymbolTable,
    mut program: ProgramBuilder,
    query_destination: QueryDestination,
) -> Result<TranslateSelectResult> {
    let mut select_plan = prepare_select_plan(
        schema,
        select,
        syms,
        &[],
        &mut program.table_reference_counter,
        query_destination,
    )?;
    optimize_plan(&mut select_plan, schema)?;
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
    emit_program(&mut program, select_plan, schema, syms, |_| {})?;
    Ok(TranslateSelectResult {
        program,
        num_result_cols,
    })
}

pub fn prepare_select_plan(
    schema: &Schema,
    mut select: ast::Select,
    syms: &SymbolTable,
    outer_query_refs: &[OuterQueryReference],
    table_ref_counter: &mut TableRefIdCounter,
    query_destination: QueryDestination,
) -> Result<Plan> {
    let compounds = select.body.compounds.take();
    match compounds {
        None => {
            let limit = select.limit.take();
            Ok(Plan::Select(prepare_one_select_plan(
                schema,
                *select.body.select,
                limit.as_deref(),
                select.order_by.take(),
                select.with.take(),
                syms,
                outer_query_refs,
                table_ref_counter,
                query_destination,
            )?))
        }
        Some(compounds) => {
            let mut last = prepare_one_select_plan(
                schema,
                *select.body.select,
                None,
                None,
                None,
                syms,
                outer_query_refs,
                table_ref_counter,
                query_destination.clone(),
            )?;

            let mut left = Vec::with_capacity(compounds.len());
            for CompoundSelect { select, operator } in compounds {
                left.push((last, operator));
                last = prepare_one_select_plan(
                    schema,
                    *select,
                    None,
                    None,
                    None,
                    syms,
                    outer_query_refs,
                    table_ref_counter,
                    query_destination.clone(),
                )?;
            }

            // Ensure all subplans have the same number of result columns
            let right_most_num_result_columns = last.result_columns.len();
            for (plan, operator) in left.iter() {
                if plan.result_columns.len() != right_most_num_result_columns {
                    crate::bail_parse_error!("SELECTs to the left and right of {} do not have the same number of result columns", operator);
                }
            }
            let (limit, offset) = select.limit.map_or(Ok((None, None)), |l| parse_limit(&l))?;

            // FIXME: handle OFFSET for compound selects
            if offset.is_some_and(|o| o > 0) {
                crate::bail_parse_error!("OFFSET is not supported for compound SELECTs yet");
            }
            // FIXME: handle ORDER BY for compound selects
            if select.order_by.is_some() {
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
    schema: &Schema,
    select: ast::OneSelect,
    limit: Option<&ast::Limit>,
    order_by: Option<Vec<ast::SortedColumn>>,
    with: Option<ast::With>,
    syms: &SymbolTable,
    outer_query_refs: &[OuterQueryReference],
    table_ref_counter: &mut TableRefIdCounter,
    query_destination: QueryDestination,
) -> Result<SelectPlan> {
    match select {
        ast::OneSelect::Select(select_inner) => {
            let SelectInner {
                mut columns,
                from,
                where_clause,
                group_by,
                distinctness,
                ..
            } = *select_inner;
            if !schema.indexes_enabled() && distinctness.is_some() {
                crate::bail_parse_error!(
                    "SELECT with DISTINCT is not allowed without indexes enabled"
                );
            }
            let col_count = columns.len();
            if col_count == 0 {
                crate::bail_parse_error!("SELECT without columns is not allowed");
            }

            let mut where_predicates = vec![];

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
                schema,
                from,
                syms,
                with,
                &mut where_predicates,
                &mut table_references,
                table_ref_counter,
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
                            .map(|t| t.columns().len())
                            .sum(),
                        // Guess 5 columns if we can't find the table using the identifier (maybe it's in [brackets] or `tick_quotes`, or miXeDcAse)
                        ResultColumn::TableStar(n) => table_references
                            .joined_tables()
                            .iter()
                            .find(|t| t.identifier == n.0)
                            .map(|t| t.columns().len())
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
                order_by: None,
                aggregates: vec![],
                limit: None,
                offset: None,
                contains_constant_false_condition: false,
                query_destination,
                distinctness: Distinctness::from_ast(distinctness.as_ref()),
                values: vec![],
            };

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
                                table.mark_column_used(idx);
                            }
                        }
                    }
                    ResultColumn::TableStar(name) => {
                        let name_normalized = normalize_ident(name.0.as_str());
                        let referenced_table = plan
                            .table_references
                            .joined_tables_mut()
                            .iter_mut()
                            .find(|t| t.identifier == name_normalized);

                        if referenced_table.is_none() {
                            crate::bail_parse_error!("no such table: {}", name.0);
                        }
                        let table = referenced_table.unwrap();
                        let num_columns = table.columns().len();
                        for idx in 0..num_columns {
                            let is_rowid_alias = {
                                let columns = table.columns();
                                columns[idx].is_rowid_alias
                            };
                            plan.result_columns.push(ResultSetColumn {
                                expr: ast::Expr::Column {
                                    database: None, // TODO: support different databases
                                    table: table.internal_id,
                                    column: idx,
                                    is_rowid_alias,
                                },
                                alias: None,
                                contains_aggregates: false,
                            });
                            table.mark_column_used(idx);
                        }
                    }
                    ResultColumn::Expr(ref mut expr, maybe_alias) => {
                        bind_column_references(
                            expr,
                            &mut plan.table_references,
                            Some(&plan.result_columns),
                        )?;
                        match expr {
                            ast::Expr::FunctionCall {
                                name,
                                distinctness,
                                args,
                                filter_over: _,
                                order_by: _,
                            } => {
                                let args_count = if let Some(args) = &args {
                                    args.len()
                                } else {
                                    0
                                };
                                let distinctness = Distinctness::from_ast(distinctness.as_ref());

                                if !schema.indexes_enabled() && distinctness.is_distinct() {
                                    crate::bail_parse_error!(
                                       "SELECT with DISTINCT is not allowed without indexes enabled"
                                   );
                                }
                                if distinctness.is_distinct() && args_count != 1 {
                                    crate::bail_parse_error!("DISTINCT aggregate functions must have exactly one argument");
                                }
                                match Func::resolve_function(&name.0, args_count) {
                                    Ok(Func::Agg(f)) => {
                                        let agg_args = match (args, &f) {
                                            (None, crate::function::AggFunc::Count0) => {
                                                // COUNT() case
                                                vec![ast::Expr::Literal(ast::Literal::Numeric(
                                                    "1".to_string(),
                                                ))]
                                            }
                                            (None, _) => crate::bail_parse_error!(
                                                "Aggregate function {} requires arguments",
                                                name.0
                                            ),
                                            (Some(args), _) => args.clone(),
                                        };

                                        let agg = Aggregate {
                                            func: f,
                                            args: agg_args.clone(),
                                            original_expr: expr.clone(),
                                            distinctness,
                                        };
                                        aggregate_expressions.push(agg.clone());
                                        plan.result_columns.push(ResultSetColumn {
                                            alias: maybe_alias.as_ref().map(|alias| match alias {
                                                ast::As::Elided(alias) => alias.0.clone(),
                                                ast::As::As(alias) => alias.0.clone(),
                                            }),
                                            expr: expr.clone(),
                                            contains_aggregates: true,
                                        });
                                    }
                                    Ok(_) => {
                                        let contains_aggregates = resolve_aggregates(
                                            schema,
                                            expr,
                                            &mut aggregate_expressions,
                                        )?;
                                        plan.result_columns.push(ResultSetColumn {
                                            alias: maybe_alias.as_ref().map(|alias| match alias {
                                                ast::As::Elided(alias) => alias.0.clone(),
                                                ast::As::As(alias) => alias.0.clone(),
                                            }),
                                            expr: expr.clone(),
                                            contains_aggregates,
                                        });
                                    }
                                    Err(e) => {
                                        if let Some(f) = syms.resolve_function(&name.0, args_count)
                                        {
                                            if let ExtFunc::Scalar(_) = f.as_ref().func {
                                                let contains_aggregates = resolve_aggregates(
                                                    schema,
                                                    expr,
                                                    &mut aggregate_expressions,
                                                )?;
                                                plan.result_columns.push(ResultSetColumn {
                                                    alias: maybe_alias.as_ref().map(|alias| {
                                                        match alias {
                                                            ast::As::Elided(alias) => {
                                                                alias.0.clone()
                                                            }
                                                            ast::As::As(alias) => alias.0.clone(),
                                                        }
                                                    }),
                                                    expr: expr.clone(),
                                                    contains_aggregates,
                                                });
                                            } else {
                                                let agg = Aggregate {
                                                    func: AggFunc::External(f.func.clone().into()),
                                                    args: args.as_ref().unwrap().clone(),
                                                    original_expr: expr.clone(),
                                                    distinctness,
                                                };
                                                aggregate_expressions.push(agg.clone());
                                                plan.result_columns.push(ResultSetColumn {
                                                    alias: maybe_alias.as_ref().map(|alias| {
                                                        match alias {
                                                            ast::As::Elided(alias) => {
                                                                alias.0.clone()
                                                            }
                                                            ast::As::As(alias) => alias.0.clone(),
                                                        }
                                                    }),
                                                    expr: expr.clone(),
                                                    contains_aggregates: true,
                                                });
                                            }
                                            continue; // Continue with the normal flow instead of returning
                                        } else {
                                            return Err(e);
                                        }
                                    }
                                }
                            }
                            ast::Expr::FunctionCallStar {
                                name,
                                filter_over: _,
                            } => match Func::resolve_function(&name.0, 0) {
                                Ok(Func::Agg(f)) => {
                                    let agg = Aggregate {
                                        func: f,
                                        args: vec![ast::Expr::Literal(ast::Literal::Numeric(
                                            "1".to_string(),
                                        ))],
                                        original_expr: expr.clone(),
                                        distinctness: Distinctness::NonDistinct,
                                    };
                                    aggregate_expressions.push(agg.clone());
                                    plan.result_columns.push(ResultSetColumn {
                                        alias: maybe_alias.as_ref().map(|alias| match alias {
                                            ast::As::Elided(alias) => alias.0.clone(),
                                            ast::As::As(alias) => alias.0.clone(),
                                        }),
                                        expr: expr.clone(),
                                        contains_aggregates: true,
                                    });
                                }
                                Ok(_) => {
                                    crate::bail_parse_error!(
                                        "Invalid aggregate function: {}",
                                        name.0
                                    );
                                }
                                Err(e) => match e {
                                    crate::LimboError::ParseError(e) => {
                                        crate::bail_parse_error!("{}", e);
                                    }
                                    _ => {
                                        crate::bail_parse_error!(
                                            "Invalid aggregate function: {}",
                                            name.0
                                        );
                                    }
                                },
                            },
                            expr => {
                                let contains_aggregates =
                                    resolve_aggregates(schema, expr, &mut aggregate_expressions)?;
                                plan.result_columns.push(ResultSetColumn {
                                    alias: maybe_alias.as_ref().map(|alias| match alias {
                                        ast::As::Elided(alias) => alias.0.clone(),
                                        ast::As::As(alias) => alias.0.clone(),
                                    }),
                                    expr: expr.clone(),
                                    contains_aggregates,
                                });
                            }
                        }
                    }
                }
            }

            // Parse the actual WHERE clause and add its conditions to the plan WHERE clause that already contains the join conditions.
            parse_where(
                where_clause,
                &mut plan.table_references,
                Some(&plan.result_columns),
                &mut plan.where_clause,
            )?;

            if let Some(mut group_by) = group_by {
                for expr in group_by.exprs.iter_mut() {
                    replace_column_number_with_copy_of_column_expr(expr, &plan.result_columns)?;
                    bind_column_references(
                        expr,
                        &mut plan.table_references,
                        Some(&plan.result_columns),
                    )?;
                }

                plan.group_by = Some(GroupBy {
                    sort_order: Some((0..group_by.exprs.len()).map(|_| SortOrder::Asc).collect()),
                    exprs: group_by.exprs,
                    having: if let Some(having) = group_by.having {
                        let mut predicates = vec![];
                        break_predicate_at_and_boundaries(*having, &mut predicates);
                        for expr in predicates.iter_mut() {
                            bind_column_references(
                                expr,
                                &mut plan.table_references,
                                Some(&plan.result_columns),
                            )?;
                            let contains_aggregates =
                                resolve_aggregates(schema, expr, &mut aggregate_expressions)?;
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
            if let Some(order_by) = order_by {
                let mut key = Vec::new();

                for mut o in order_by {
                    replace_column_number_with_copy_of_column_expr(
                        &mut o.expr,
                        &plan.result_columns,
                    )?;

                    bind_column_references(
                        &mut o.expr,
                        &mut plan.table_references,
                        Some(&plan.result_columns),
                    )?;
                    resolve_aggregates(schema, &o.expr, &mut plan.aggregates)?;

                    key.push((o.expr, o.order.unwrap_or(ast::SortOrder::Asc)));
                }
                plan.order_by = Some(key);
            }

            // Parse the LIMIT/OFFSET clause
            (plan.limit, plan.offset) = limit.map_or(Ok((None, None)), parse_limit)?;

            // Return the unoptimized query plan
            Ok(plan)
        }
        ast::OneSelect::Values(values) => {
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
                order_by: None,
                aggregates: vec![],
                limit: None,
                offset: None,
                contains_constant_false_condition: false,
                query_destination,
                distinctness: Distinctness::NonDistinct,
                values,
            };

            Ok(plan)
        }
    }
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
    let num_sorter_cursors = plan.group_by.is_some() as usize + plan.order_by.is_some() as usize;
    let num_pseudo_cursors = plan.group_by.is_some() as usize + plan.order_by.is_some() as usize;

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
    let order_by_instructions = select.order_by.is_some() as usize * 10;
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
    let order_by_labels = select.order_by.is_some() as usize * 10;
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
        amount: 0,
    });
    program.emit_result_row(output_reg, 1);
    Ok(())
}
