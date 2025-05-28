use std::{cell::RefCell, sync::Arc};

use crate::{
    schema::{Index, IndexColumn, Table},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::Insn,
    },
    Result,
};

use limbo_sqlite3_parser::ast;

use super::{
    emitter::{emit_query, Resolver, TranslateCtx},
    expr::{unwrap_parens, walk_expr},
    main_loop::LoopLabels,
    optimizer::optimize_select_plan,
    plan::{
        ColumnUsedMask, EvalAt, JoinOrderMember, OuterQueryReference, Plan, QueryDestination,
        SelectPlan, TableReferences, WhereClauseSubqueryPlan, WhereClauseSubqueryType, WhereTerm,
    },
    select::prepare_select_plan,
};

/// Emit the subqueries contained in the FROM clause.
/// This is done first so the results can be read in the main query loop.
pub fn emit_from_clause_subqueries(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &mut TableReferences,
) -> Result<()> {
    for table_reference in tables.joined_tables_mut() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table_reference.table {
            // Emit the subquery and get the start register of the result columns.
            let result_columns_start =
                emit_from_clause_subquery(program, &mut from_clause_subquery.plan, t_ctx)?;
            // Set the start register of the subquery's result columns.
            // This is done so that translate_expr() can read the result columns of the subquery,
            // as if it were reading from a regular table.
            from_clause_subquery.result_columns_start_reg = Some(result_columns_start);
        }
    }
    Ok(())
}

/// Emit a FROM clausesubquery and return the start register of the result columns.
/// This is done by emitting a coroutine that stores the result columns in sequential registers.
/// Each subquery in a FROM clause has its own separate SelectPlan which is wrapped in a coroutine.
///
/// The resulting bytecode from a subquery is mostly exactly the same as a regular query, except:
/// - it ends in an EndCoroutine instead of a Halt.
/// - instead of emitting ResultRows, the coroutine yields to the main query loop.
/// - the first register of the result columns is returned to the parent query,
///   so that translate_expr() can read the result columns of the subquery,
///   as if it were reading from a regular table.
///
/// Since a subquery has its own SelectPlan, it can contain nested subqueries,
/// which can contain even more nested subqueries, etc.
pub fn emit_from_clause_subquery<'a>(
    program: &mut ProgramBuilder,
    plan: &mut SelectPlan,
    t_ctx: &mut TranslateCtx<'a>,
) -> Result<usize> {
    let yield_reg = program.alloc_register();
    let coroutine_implementation_start_offset = program.allocate_label();
    match &mut plan.query_destination {
        QueryDestination::CoroutineYield {
            yield_reg: y,
            coroutine_implementation_start,
        } => {
            // The parent query will use this register to jump to/from the subquery.
            *y = yield_reg;
            // The parent query will use this register to reinitialize the coroutine when it needs to run multiple times.
            *coroutine_implementation_start = coroutine_implementation_start_offset;
        }
        _ => unreachable!("emit_subquery called on non-subquery"),
    }
    let end_coroutine_label = program.allocate_label();
    let mut metadata = TranslateCtx {
        labels_main_loop: (0..plan.joined_tables().len())
            .map(|_| LoopLabels::new(program))
            .collect(),
        label_main_loop_end: None,
        meta_group_by: None,
        meta_left_joins: (0..plan.joined_tables().len()).map(|_| None).collect(),
        meta_sort: None,
        reg_agg_start: None,
        reg_nonagg_emit_once_flag: None,
        reg_result_cols_start: None,
        result_column_indexes_in_orderby_sorter: (0..plan.result_columns.len()).collect(),
        result_columns_to_skip_in_orderby_sorter: None,
        limit_ctx: None,
        reg_offset: None,
        reg_limit_offset_sum: None,
        resolver: Resolver::new(t_ctx.resolver.schema, t_ctx.resolver.symbol_table),
    };
    let subquery_body_end_label = program.allocate_label();
    program.emit_insn(Insn::InitCoroutine {
        yield_reg,
        jump_on_definition: subquery_body_end_label,
        start_offset: coroutine_implementation_start_offset,
    });
    program.preassign_label_to_next_insn(coroutine_implementation_start_offset);
    let result_column_start_reg = emit_query(program, plan, &mut metadata)?;
    program.resolve_label(end_coroutine_label, program.offset());
    program.emit_insn(Insn::EndCoroutine { yield_reg });
    program.preassign_label_to_next_insn(subquery_body_end_label);
    Ok(result_column_start_reg)
}

/// Compute a [crate::translate::plan::WhereClauseSubqueryPlan] for each
/// subquery appearing in the WHERE clause.
///
/// Ideally all of these subqueries would be unnested in the optimizer so that
/// they could be written as regular joins, but right now we translate them in
/// a naive manner.
///
/// This is not a huge problem for uncorrelated subqueries (subqueries that do not
/// reference any columns from the outer query), but it is a problem for correlated
/// subqueries, because they need to be evaluated for each row of the outer query,
/// which has at least quadratic complexity.
pub fn compute_plans_for_where_clause_subqueries<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    referenced_tables: &TableReferences,
    where_terms: &'a [WhereTerm],
    join_order: &[JoinOrderMember],
) -> Result<()> {
    let get_outer_query_refs = |referenced_tables: &TableReferences| {
        referenced_tables
            .joined_tables()
            .iter()
            .map(|t| OuterQueryReference {
                table: t.table.clone(),
                identifier: t.identifier.clone(),
                internal_id: t.internal_id,
                col_used_mask: ColumnUsedMask::new(),
            })
            .chain(
                referenced_tables
                    .outer_query_refs()
                    .iter()
                    .map(|t| OuterQueryReference {
                        table: t.table.clone(),
                        identifier: t.identifier.clone(),
                        internal_id: t.internal_id,
                        col_used_mask: ColumnUsedMask::new(),
                    }),
            )
            .collect::<Vec<_>>()
    };

    let compute_eval_at = |plan: &SelectPlan| -> Result<EvalAt> {
        let mut max_eval_at = EvalAt::BeforeLoop;
        for outer_query_ref in plan
            .table_references
            .outer_query_refs()
            .iter()
            .filter(|t| t.is_used())
        {
            let Some(join_idx) = join_order
                .iter()
                .position(|j| j.table_id == outer_query_ref.internal_id)
            else {
                crate::bail_parse_error!("subquery references outer table {}, but it's not found in the outer query's join order", &outer_query_ref.identifier);
            };
            max_eval_at = max_eval_at.max(EvalAt::Loop(join_idx));
        }
        Ok(max_eval_at)
    };

    for (where_term_idx, where_term) in where_terms.iter().enumerate() {
        walk_expr(&where_term.expr, &mut |expr: &'a ast::Expr| -> Result<()> {
            match expr {
                ast::Expr::Exists(subselect) => {
                    let outer_query_refs = get_outer_query_refs(referenced_tables);

                    let plan = prepare_select_plan(
                        t_ctx.resolver.schema,
                        subselect.as_ref().clone(),
                        t_ctx.resolver.symbol_table,
                        &outer_query_refs,
                        &mut program.table_reference_counter,
                        QueryDestination::Unset, // The destination is set at translation time.
                    )?;
                    let Plan::Select(mut plan) = plan else {
                        crate::bail_parse_error!(
                            "compound SELECT queries not supported yet in WHERE clause subqueries"
                        );
                    };
                    optimize_select_plan(&mut plan, t_ctx.resolver.schema)?;
                    where_term
                        .eval_at_override
                        .set(Some(compute_eval_at(&plan)?));

                    t_ctx.resolver.where_clause_subquery_plans.push((
                        expr,
                        RefCell::new(Some(WhereClauseSubqueryPlan {
                            subquery_type: WhereClauseSubqueryType::Exists,
                            plan,
                        })),
                    ));
                }
                ast::Expr::Subquery(subselect) => {
                    let outer_query_refs = get_outer_query_refs(referenced_tables);
                    let plan = prepare_select_plan(
                        t_ctx.resolver.schema,
                        subselect.as_ref().clone(),
                        t_ctx.resolver.symbol_table,
                        &outer_query_refs,
                        &mut program.table_reference_counter,
                        QueryDestination::Unset, // The destination is set at translation time.
                    )?;
                    let Plan::Select(mut plan) = plan else {
                        crate::bail_parse_error!(
                            "compound SELECT queries not supported yet in WHERE clause subqueries"
                        );
                    };
                    if plan.result_columns.len() != 1 {
                        crate::bail_parse_error!(
                            "scalar subqueries must return exactly one column"
                        );
                    }
                    plan.limit = Some(1);
                    optimize_select_plan(&mut plan, t_ctx.resolver.schema)?;
                    where_term
                        .eval_at_override
                        .set(Some(compute_eval_at(&plan)?));

                    t_ctx.resolver.where_clause_subquery_plans.push((
                        expr,
                        RefCell::new(Some(WhereClauseSubqueryPlan {
                            subquery_type: WhereClauseSubqueryType::Scalar,
                            plan,
                        })),
                    ));
                }
                ast::Expr::InSelect { lhs, not, rhs } => {
                    let outer_query_refs = get_outer_query_refs(referenced_tables);
                    let plan = prepare_select_plan(
                        t_ctx.resolver.schema,
                        rhs.as_ref().clone(),
                        t_ctx.resolver.symbol_table,
                        &outer_query_refs,
                        &mut program.table_reference_counter,
                        QueryDestination::Unset, // The destination is set at translation time.
                    )?;
                    let Plan::Select(mut plan) = plan else {
                        crate::bail_parse_error!(
                            "compound SELECT queries not supported yet in WHERE clause subqueries"
                        );
                    };

                    // e.g. (x,y) IN (SELECT ...)
                    // or x IN (SELECT ...)
                    let lhs_column_count = match unwrap_parens(lhs.as_ref())? {
                        ast::Expr::Parenthesized(exprs) => exprs.len(),
                        _ => 1,
                    };
                    if lhs_column_count != plan.result_columns.len() {
                        crate::bail_parse_error!(
                            "lhs of IN subquery must have the same number of columns as the subquery"
                        );
                    }
                    optimize_select_plan(&mut plan, t_ctx.resolver.schema)?;

                    let ephemeral_index = Arc::new(Index {
                        columns: plan
                            .result_columns
                            .iter()
                            .enumerate()
                            .map(|(i, c)| IndexColumn {
                                name: c.name(&plan.table_references).unwrap_or("").to_string(),
                                order: ast::SortOrder::Asc,
                                pos_in_table: i,
                                collation: None, // TODO: this should be inferred
                            })
                            .collect(),
                        name: format!("ephemeral_index_where_sub_{}", where_term_idx),
                        table_name: String::new(),
                        ephemeral: true,
                        has_rowid: false,
                        root_page: 0,
                        unique: false,
                    });

                    let cursor_id =
                        program.alloc_cursor_id(CursorType::BTreeIndex(ephemeral_index.clone()));

                    t_ctx.resolver.where_clause_subquery_plans.push((
                        expr,
                        RefCell::new(Some(WhereClauseSubqueryPlan {
                            subquery_type: WhereClauseSubqueryType::In {
                                not: *not,
                                lhs: lhs.clone(),
                                ephemeral_index: (cursor_id, ephemeral_index),
                            },
                            plan,
                        })),
                    ));
                }
                _ => {}
            }
            Ok(())
        })?;
    }
    Ok(())
}
