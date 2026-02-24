use std::sync::Arc;

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use turso_parser::ast::{self, SortOrder, SubqueryType};

use crate::{
    emit_explain,
    schema::{BTreeTable, Index, IndexColumn, Table},
    translate::{
        collate::get_collseq_from_expr,
        compound_select::emit_program_for_compound_select,
        emitter::emit_program_for_select,
        expr::{compare_affinity, get_expr_affinity, unwrap_parens, walk_expr_mut, WalkControl},
        optimizer::optimize_select_plan,
        plan::{
            ColumnUsedMask, JoinOrderMember, NonFromClauseSubquery, OuterQueryReference, Plan,
            SetOperation, SubqueryPosition, SubqueryState, TableReferences, WhereTerm,
        },
        select::prepare_select_plan,
    },
    vdbe::{
        affinity,
        builder::{CursorType, MaterializedCteInfo, ProgramBuilder},
        insn::Insn,
        CursorID,
    },
    Connection, QueryMode, Result,
};

use super::{
    emitter::{emit_query, Resolver, TranslateCtx},
    main_loop::LoopLabels,
    plan::{Aggregate, Operation, QueryDestination, Scan, Search, SelectPlan},
    planner::resolve_window_and_aggregate_functions,
};
use crate::vdbe::builder::CursorKey;
use turso_parser::ast::TableInternalId;

// Compute query plans for subqueries occurring in any position other than the FROM clause.
// This includes the WHERE clause, HAVING clause, GROUP BY clause, ORDER BY clause, LIMIT clause, and OFFSET clause.
/// The AST expression containing the subquery ([ast::Expr::Exists], [ast::Expr::Subquery], [ast::Expr::InSelect]) is replaced with a [ast::Expr::SubqueryResult] expression.
/// The [ast::Expr::SubqueryResult] expression contains the subquery ID, the left-hand side expression (only applicable to IN subqueries), the NOT IN flag (only applicable to IN subqueries), and the subquery type.
/// The computed plans are stored in the [NonFromClauseSubquery] structs on the [SelectPlan], and evaluated at the appropriate time during the translation of the main query.
/// The appropriate time is determined by whether the subquery is correlated or uncorrelated;
/// if it is uncorrelated, it can be evaluated as early as possible, but if it is correlated, it must be evaluated after all of its dependencies from the
/// outer query are 'in scope', i.e. their cursors are open and rewound.
pub fn plan_subqueries_from_select_plan(
    program: &mut ProgramBuilder,
    plan: &mut SelectPlan,
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<()> {
    // WHERE
    plan_subqueries_with_outer_query_access(
        program,
        &mut plan.non_from_clause_subqueries,
        &mut plan.table_references,
        resolver,
        plan.where_clause.iter_mut().map(|t| &mut t.expr),
        connection,
        SubqueryPosition::Where,
    )?;

    // GROUP BY
    if let Some(group_by) = &mut plan.group_by {
        plan_subqueries_with_outer_query_access(
            program,
            &mut plan.non_from_clause_subqueries,
            &mut plan.table_references,
            resolver,
            group_by.exprs.iter_mut(),
            connection,
            SubqueryPosition::GroupBy,
        )?;
        if let Some(having) = group_by.having.as_mut() {
            plan_subqueries_with_outer_query_access(
                program,
                &mut plan.non_from_clause_subqueries,
                &mut plan.table_references,
                resolver,
                having.iter_mut(),
                connection,
                SubqueryPosition::Having,
            )?;
        }
    }

    // Result columns
    plan_subqueries_with_outer_query_access(
        program,
        &mut plan.non_from_clause_subqueries,
        &mut plan.table_references,
        resolver,
        plan.result_columns.iter_mut().map(|c| &mut c.expr),
        connection,
        SubqueryPosition::ResultColumn,
    )?;

    // ORDER BY
    plan_subqueries_with_outer_query_access(
        program,
        &mut plan.non_from_clause_subqueries,
        &mut plan.table_references,
        resolver,
        plan.order_by.iter_mut().map(|(expr, _, _)| &mut **expr),
        connection,
        SubqueryPosition::OrderBy,
    )?;

    // LIMIT and OFFSET cannot reference columns from the outer query
    let get_outer_query_refs = |_: &TableReferences| vec![];
    {
        let mut subquery_parser = get_subquery_parser(
            program,
            &mut plan.non_from_clause_subqueries,
            &mut plan.table_references,
            resolver,
            connection,
            get_outer_query_refs,
            SubqueryPosition::LimitOffset,
        );
        // Limit
        if let Some(limit) = &mut plan.limit {
            walk_expr_mut(limit, &mut subquery_parser)?;
        }
        // Offset
        if let Some(offset) = &mut plan.offset {
            walk_expr_mut(offset, &mut subquery_parser)?;
        }
    }

    // Recollect aggregates after all subquery planning.
    // This is necessary because:
    // 1. Aggregates are collected with cloned expressions before subquery planning modifies them
    //    (e.g., EXISTS -> SubqueryResult), causing stale args in aggregates.
    // 2. ORDER BY may be cleared for single-row aggregates AFTER aggregates were collected from it,
    //    leaving orphaned aggregates with unprocessed subqueries in their args.
    // Recollecting from the current state of result_columns, HAVING, and ORDER BY ensures
    // aggregates have updated expressions and excludes aggregates from cleared ORDER BY.
    if !plan.aggregates.is_empty() {
        recollect_aggregates(plan, resolver)?;
    }

    update_column_used_masks(
        &mut plan.table_references,
        &mut plan.non_from_clause_subqueries,
    );
    Ok(())
}

/// Compute query plans for subqueries in a DML statement's WHERE clause.
/// This is used by DELETE and UPDATE statements which only have subqueries in the WHERE clause.
/// Similar to [plan_subqueries_from_select_plan] but only handles the WHERE clause
/// since these statements don't have GROUP BY, ORDER BY, or result column subqueries.
pub fn plan_subqueries_from_where_clause(
    program: &mut ProgramBuilder,
    non_from_clause_subqueries: &mut Vec<NonFromClauseSubquery>,
    table_references: &mut TableReferences,
    where_clause: &mut [WhereTerm],
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<()> {
    plan_subqueries_with_outer_query_access(
        program,
        non_from_clause_subqueries,
        table_references,
        resolver,
        where_clause.iter_mut().map(|t| &mut t.expr),
        connection,
        SubqueryPosition::Where,
    )?;

    update_column_used_masks(table_references, non_from_clause_subqueries);
    Ok(())
}

/// Compute query plans for subqueries in VALUES expressions.
/// This is used by INSERT statements with VALUES clauses and SELECT with VALUES.
/// The VALUES expressions may contain scalar subqueries that need to be planned.
#[allow(clippy::vec_box)]
pub fn plan_subqueries_from_values(
    program: &mut ProgramBuilder,
    non_from_clause_subqueries: &mut Vec<NonFromClauseSubquery>,
    table_references: &mut TableReferences,
    values: &mut [Vec<Box<ast::Expr>>],
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<()> {
    plan_subqueries_with_outer_query_access(
        program,
        non_from_clause_subqueries,
        table_references,
        resolver,
        values.iter_mut().flatten().map(|e| e.as_mut()),
        connection,
        SubqueryPosition::ResultColumn, // VALUES are similar to result columns in terms of subquery handling
    )?;

    update_column_used_masks(table_references, non_from_clause_subqueries);
    Ok(())
}

/// Compute query plans for subqueries in UPDATE SET clause expressions.
/// This is used by UPDATE statements where SET clause values contain scalar subqueries.
/// e.g. `UPDATE t SET col = (SELECT max(id) FROM t2)`
pub fn plan_subqueries_from_set_clauses(
    program: &mut ProgramBuilder,
    non_from_clause_subqueries: &mut Vec<NonFromClauseSubquery>,
    table_references: &mut TableReferences,
    set_clauses: &mut [(usize, Box<ast::Expr>)],
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<()> {
    plan_subqueries_with_outer_query_access(
        program,
        non_from_clause_subqueries,
        table_references,
        resolver,
        set_clauses.iter_mut().map(|(_, expr)| expr.as_mut()),
        connection,
        SubqueryPosition::ResultColumn, // SET clause subqueries are similar to result columns
    )?;

    update_column_used_masks(table_references, non_from_clause_subqueries);
    Ok(())
}

/// Compute query plans for subqueries in RETURNING expressions.
/// This is used by INSERT, UPDATE, and DELETE statements with RETURNING clauses.
/// RETURNING expressions may contain scalar subqueries that need to be planned.
pub fn plan_subqueries_from_returning(
    program: &mut ProgramBuilder,
    non_from_clause_subqueries: &mut Vec<NonFromClauseSubquery>,
    table_references: &mut TableReferences,
    returning: &mut [ast::ResultColumn],
    resolver: &Resolver,
    connection: &Arc<Connection>,
) -> Result<()> {
    // Extract mutable references to expressions from ResultColumn::Expr variants
    let exprs = returning.iter_mut().filter_map(|rc| match rc {
        ast::ResultColumn::Expr(expr, _) => Some(expr.as_mut()),
        ast::ResultColumn::Star | ast::ResultColumn::TableStar(_) => None,
    });

    plan_subqueries_with_outer_query_access(
        program,
        non_from_clause_subqueries,
        table_references,
        resolver,
        exprs,
        connection,
        SubqueryPosition::ResultColumn,
    )?;

    update_column_used_masks(table_references, non_from_clause_subqueries);
    Ok(())
}

/// Compute query plans for subqueries in the WHERE clause and HAVING clause (both of which have access to the outer query scope)
fn plan_subqueries_with_outer_query_access<'a>(
    program: &mut ProgramBuilder,
    out_subqueries: &mut Vec<NonFromClauseSubquery>,
    referenced_tables: &mut TableReferences,
    resolver: &Resolver,
    exprs: impl Iterator<Item = &'a mut ast::Expr>,
    connection: &Arc<Connection>,
    position: SubqueryPosition,
) -> Result<()> {
    // Most subqueries can reference columns from the outer query,
    // including nested cases where a subquery inside a subquery references columns from its parent's parent
    // and so on.
    let get_outer_query_refs = |referenced_tables: &TableReferences| {
        referenced_tables
            .joined_tables()
            .iter()
            .map(|t| {
                // Extract cte_id from FromClauseSubquery if this is a CTE reference
                let cte_id = match &t.table {
                    Table::FromClauseSubquery(subq) => subq.cte_id,
                    _ => None,
                };
                OuterQueryReference {
                    table: t.table.clone(),
                    identifier: t.identifier.clone(),
                    internal_id: t.internal_id,
                    col_used_mask: ColumnUsedMask::default(),
                    cte_select: None,
                    cte_explicit_columns: vec![],
                    cte_id,
                    cte_definition_only: false,
                    rowid_referenced: false,
                }
            })
            .chain(
                referenced_tables
                    .outer_query_refs()
                    .iter()
                    .map(|t| OuterQueryReference {
                        table: t.table.clone(),
                        identifier: t.identifier.clone(),
                        internal_id: t.internal_id,
                        col_used_mask: ColumnUsedMask::default(),
                        cte_select: t.cte_select.clone(),
                        cte_explicit_columns: t.cte_explicit_columns.clone(),
                        cte_id: t.cte_id, // Preserve CTE ID from outer query refs
                        cte_definition_only: t.cte_definition_only,
                        rowid_referenced: false,
                    }),
            )
            .collect::<Vec<_>>()
    };

    let mut subquery_parser = get_subquery_parser(
        program,
        out_subqueries,
        referenced_tables,
        resolver,
        connection,
        get_outer_query_refs,
        position,
    );
    for expr in exprs {
        walk_expr_mut(expr, &mut subquery_parser)?;
    }

    Ok(())
}

/// Create a closure that will walk the AST and replace subqueries with [ast::Expr::SubqueryResult] expressions.
fn get_subquery_parser<'a>(
    program: &'a mut ProgramBuilder,
    out_subqueries: &'a mut Vec<NonFromClauseSubquery>,
    referenced_tables: &'a mut TableReferences,
    resolver: &'a Resolver,
    connection: &'a Arc<Connection>,
    get_outer_query_refs: fn(&TableReferences) -> Vec<OuterQueryReference>,
    position: SubqueryPosition,
) -> impl FnMut(&mut ast::Expr) -> Result<WalkControl> + 'a {
    fn handle_unsupported_correlation(correlated: bool, position: SubqueryPosition) -> Result<()> {
        if correlated && !position.allow_correlated() {
            crate::bail_parse_error!(
                "correlated subqueries in {} clause are not supported yet",
                position.name()
            );
        }
        Ok(())
    }

    move |expr: &mut ast::Expr| -> Result<WalkControl> {
        match expr {
            ast::Expr::Exists(_) => {
                let subquery_id = program.table_reference_counter.next();
                let outer_query_refs = get_outer_query_refs(referenced_tables);

                let result_reg = program.alloc_register();
                let subquery_type = SubqueryType::Exists { result_reg };
                let result_expr = ast::Expr::SubqueryResult {
                    subquery_id,
                    lhs: None,
                    not_in: false,
                    query_type: subquery_type.clone(),
                };
                let ast::Expr::Exists(subselect) = std::mem::replace(expr, result_expr) else {
                    unreachable!();
                };

                let plan = prepare_select_plan(
                    subselect,
                    resolver,
                    program,
                    &outer_query_refs,
                    QueryDestination::ExistsSubqueryResult { result_reg },
                    connection,
                )?;
                let Plan::Select(mut plan) = plan else {
                    crate::bail_parse_error!(
                        "compound SELECT queries not supported yet in WHERE clause subqueries"
                    );
                };
                optimize_select_plan(&mut plan, resolver.schema())?;
                // EXISTS subqueries are satisfied after at most 1 row has been returned.
                plan.limit = Some(Box::new(ast::Expr::Literal(ast::Literal::Numeric(
                    "1".to_string(),
                ))));
                let correlated = plan.is_correlated();
                handle_unsupported_correlation(correlated, position)?;
                out_subqueries.push(NonFromClauseSubquery {
                    internal_id: subquery_id,
                    query_type: subquery_type,
                    state: SubqueryState::Unevaluated {
                        plan: Some(Box::new(plan)),
                    },
                    correlated,
                });
                Ok(WalkControl::Continue)
            }
            ast::Expr::Subquery(_) => {
                let subquery_id = program.table_reference_counter.next();
                let outer_query_refs = get_outer_query_refs(referenced_tables);

                let result_expr = ast::Expr::SubqueryResult {
                    subquery_id,
                    lhs: None,
                    not_in: false,
                    // Placeholder values because the number of columns returned is not known until the plan is prepared.
                    // These are replaced below after planning.
                    query_type: SubqueryType::RowValue {
                        result_reg_start: 0,
                        num_regs: 0,
                    },
                };
                let ast::Expr::Subquery(subselect) = std::mem::replace(expr, result_expr) else {
                    unreachable!();
                };
                let plan = prepare_select_plan(
                    subselect,
                    resolver,
                    program,
                    &outer_query_refs,
                    QueryDestination::Unset,
                    connection,
                )?;
                let Plan::Select(mut plan) = plan else {
                    crate::bail_parse_error!(
                        "compound SELECT queries not supported yet in WHERE clause subqueries"
                    );
                };
                optimize_select_plan(&mut plan, resolver.schema())?;
                let reg_count = plan.result_columns.len();
                let reg_start = program.alloc_registers(reg_count);

                plan.query_destination = QueryDestination::RowValueSubqueryResult {
                    result_reg_start: reg_start,
                    num_regs: reg_count,
                };
                // RowValue subqueries are satisfied after at most 1 row has been returned,
                // as they are used in comparisons with a scalar or a tuple of scalars like (x,y) = (SELECT ...) or x = (SELECT ...).
                plan.limit = Some(Box::new(ast::Expr::Literal(ast::Literal::Numeric(
                    "1".to_string(),
                ))));

                let ast::Expr::SubqueryResult {
                    subquery_id,
                    lhs: None,
                    not_in: false,
                    query_type:
                        SubqueryType::RowValue {
                            result_reg_start,
                            num_regs,
                        },
                } = &mut *expr
                else {
                    unreachable!();
                };
                *result_reg_start = reg_start;
                *num_regs = reg_count;

                let correlated = plan.is_correlated();
                handle_unsupported_correlation(correlated, position)?;

                out_subqueries.push(NonFromClauseSubquery {
                    internal_id: *subquery_id,
                    query_type: SubqueryType::RowValue {
                        result_reg_start: reg_start,
                        num_regs: reg_count,
                    },
                    state: SubqueryState::Unevaluated {
                        plan: Some(Box::new(plan)),
                    },
                    correlated,
                });
                Ok(WalkControl::Continue)
            }
            ast::Expr::InSelect { .. } => {
                let subquery_id = program.table_reference_counter.next();
                let outer_query_refs = get_outer_query_refs(referenced_tables);

                let ast::Expr::InSelect { lhs, not, rhs } =
                    std::mem::replace(expr, ast::Expr::Literal(ast::Literal::Null))
                else {
                    unreachable!();
                };
                let plan = prepare_select_plan(
                    rhs,
                    resolver,
                    program,
                    &outer_query_refs,
                    QueryDestination::Unset,
                    connection,
                )?;
                let Plan::Select(mut plan) = plan else {
                    crate::bail_parse_error!(
                        "compound SELECT queries not supported yet in WHERE clause subqueries"
                    );
                };
                optimize_select_plan(&mut plan, resolver.schema())?;
                // e.g. (x,y) IN (SELECT ...)
                // or x IN (SELECT ...)
                let lhs_columns = match unwrap_parens(lhs.as_ref())? {
                    ast::Expr::Parenthesized(exprs) => {
                        either::Left(exprs.iter().map(|e| e.as_ref()))
                    }
                    expr => either::Right(core::iter::once(expr)),
                };
                let lhs_column_count = lhs_columns.len();
                if lhs_column_count != plan.result_columns.len() {
                    crate::bail_parse_error!(
                        "sub-select returns {} columns - expected {lhs_column_count}",
                        plan.result_columns.len()
                    );
                }
                let in_affinity_str: Arc<String> = Arc::new(
                    lhs_columns
                        .enumerate()
                        .map(|(i, lhs_expr)| {
                            let lhs_affinity =
                                get_expr_affinity(lhs_expr, Some(referenced_tables), None);
                            compare_affinity(
                                &plan.result_columns[i].expr,
                                lhs_affinity,
                                Some(&plan.table_references),
                                None,
                            )
                            .aff_mask()
                        })
                        .collect(),
                );

                let mut columns = plan
                    .result_columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| IndexColumn {
                        name: c.name(&plan.table_references).unwrap_or("").to_string(),
                        order: SortOrder::Asc,
                        pos_in_table: i,
                        collation: None,
                        default: None,
                        expr: None,
                    })
                    .collect::<Vec<_>>();

                for (i, column) in columns.iter_mut().enumerate() {
                    column.collation = get_collseq_from_expr(
                        &plan.result_columns[i].expr,
                        &plan.table_references,
                    )?;
                }

                let ephemeral_index = Arc::new(Index {
                    columns,
                    name: format!("ephemeral_index_where_sub_{subquery_id}"),
                    table_name: String::new(),
                    ephemeral: true,
                    has_rowid: false,
                    root_page: 0,
                    unique: false,
                    where_clause: None,
                    index_method: None,
                });

                let cursor_id =
                    program.alloc_cursor_id(CursorType::BTreeIndex(ephemeral_index.clone()));

                plan.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id,
                    index: ephemeral_index,
                    affinity_str: Some(in_affinity_str.clone()),
                    is_delete: false,
                };

                *expr = ast::Expr::SubqueryResult {
                    subquery_id,
                    lhs: Some(lhs),
                    not_in: not,
                    query_type: SubqueryType::In {
                        cursor_id,
                        affinity_str: in_affinity_str.clone(),
                    },
                };

                let correlated = plan.is_correlated();
                handle_unsupported_correlation(correlated, position)?;

                out_subqueries.push(NonFromClauseSubquery {
                    internal_id: subquery_id,
                    query_type: SubqueryType::In {
                        cursor_id,
                        affinity_str: in_affinity_str,
                    },
                    state: SubqueryState::Unevaluated {
                        plan: Some(Box::new(plan)),
                    },
                    correlated,
                });
                Ok(WalkControl::Continue)
            }
            _ => Ok(WalkControl::Continue),
        }
    }
}

/// Recollect all aggregates after subquery planning.
///
/// Aggregates are collected during parsing with cloned expressions. When subquery planning
/// modifies expressions in place (e.g. replacing EXISTS with SubqueryResult), the aggregate's
/// cloned original_expr and args become stale. This causes cache misses during translation.
///
/// Instead of trying to sync stale clones, this function recollects all aggregates fresh
/// from the updated expressions in result_columns, HAVING, and ORDER BY.
fn recollect_aggregates(plan: &mut SelectPlan, resolver: &Resolver) -> Result<()> {
    let mut new_aggregates: Vec<Aggregate> = Vec::new();

    // Collect from result columns (same order as original collection)
    for rc in &plan.result_columns {
        resolve_window_and_aggregate_functions(&rc.expr, resolver, &mut new_aggregates, None)?;
    }

    // Collect from HAVING
    if let Some(group_by) = &plan.group_by {
        if let Some(having) = &group_by.having {
            for expr in having {
                resolve_window_and_aggregate_functions(expr, resolver, &mut new_aggregates, None)?;
            }
        }
    }

    // Collect from ORDER BY
    for (expr, _, _) in &plan.order_by {
        resolve_window_and_aggregate_functions(expr, resolver, &mut new_aggregates, None)?;
    }

    plan.aggregates = new_aggregates;
    Ok(())
}

/// We make decisions about when to evaluate expressions or whether to use covering indexes based on
/// which columns of a table have been referenced.
/// Since subquery nesting is arbitrarily deep, a reference to a column must propagate recursively
/// up to the parent. Example:
///
/// SELECT * FROM t WHERE EXISTS (SELECT * FROM u WHERE EXISTS (SELECT * FROM v WHERE v.foo = t.foo))
///
/// In this case, t.foo is referenced in the innermost subquery, so the top level query must be notified
/// that t.foo has been used.
fn update_column_used_masks(
    table_refs: &mut TableReferences,
    subqueries: &mut [NonFromClauseSubquery],
) {
    fn propagate_outer_refs_from_select_plan(table_refs: &mut TableReferences, plan: &SelectPlan) {
        for child_outer_query_ref in plan
            .table_references
            .outer_query_refs()
            .iter()
            .filter(|t| t.is_used())
        {
            if let Some(joined_table) =
                table_refs.find_joined_table_by_internal_id_mut(child_outer_query_ref.internal_id)
            {
                joined_table.col_used_mask |= &child_outer_query_ref.col_used_mask;
            }
            if let Some(outer_query_ref) = table_refs
                .find_outer_query_ref_by_internal_id_mut(child_outer_query_ref.internal_id)
            {
                outer_query_ref.col_used_mask |= &child_outer_query_ref.col_used_mask;
            }
        }

        for joined_table in plan.table_references.joined_tables().iter() {
            if let Table::FromClauseSubquery(from_clause_subquery) = &joined_table.table {
                propagate_outer_refs_from_plan(table_refs, from_clause_subquery.plan.as_ref());
            }
        }
    }

    fn propagate_outer_refs_from_plan(table_refs: &mut TableReferences, plan: &Plan) {
        match plan {
            Plan::Select(select_plan) => {
                propagate_outer_refs_from_select_plan(table_refs, select_plan);
            }
            Plan::CompoundSelect {
                left, right_most, ..
            } => {
                for (select_plan, _) in left.iter() {
                    propagate_outer_refs_from_select_plan(table_refs, select_plan);
                }
                propagate_outer_refs_from_select_plan(table_refs, right_most);
            }
            Plan::Delete(_) | Plan::Update(_) => {}
        }
    }

    for subquery in subqueries.iter_mut() {
        let SubqueryState::Unevaluated { plan } = &mut subquery.state else {
            panic!("subquery has already been evaluated");
        };
        let Some(child_plan) = plan.as_mut() else {
            panic!("subquery has no plan");
        };

        propagate_outer_refs_from_select_plan(table_refs, child_plan);
    }

    // Collect raw plan pointers to avoid cloning while sidestepping borrow rules.
    let from_clause_plans = table_refs
        .joined_tables()
        .iter()
        .filter_map(|t| match &t.table {
            Table::FromClauseSubquery(from_clause_subquery) => {
                Some(from_clause_subquery.plan.as_ref() as *const Plan)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    for plan in from_clause_plans {
        // SAFETY: plans live within table_refs for the duration of this function.
        let plan = unsafe { &*plan };
        propagate_outer_refs_from_plan(table_refs, plan);
    }
}

/// Recursively pre-materialize all multi-ref CTEs in a plan tree.
/// This must be called BEFORE emitting any coroutines to ensure CTEs referenced
/// inside coroutines have their cursors opened at the top level.
fn pre_materialize_multi_ref_ctes(
    program: &mut ProgramBuilder,
    plan: &mut Plan,
    t_ctx: &mut TranslateCtx,
) -> Result<()> {
    match plan {
        Plan::Select(select_plan) => {
            pre_materialize_multi_ref_ctes_in_tables(
                program,
                &mut select_plan.table_references,
                t_ctx,
            )?;
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            for (select_plan, _) in left.iter_mut() {
                pre_materialize_multi_ref_ctes_in_tables(
                    program,
                    &mut select_plan.table_references,
                    t_ctx,
                )?;
            }
            pre_materialize_multi_ref_ctes_in_tables(
                program,
                &mut right_most.table_references,
                t_ctx,
            )?;
        }
        Plan::Delete(_) | Plan::Update(_) => {}
    }
    Ok(())
}

fn pre_materialize_multi_ref_ctes_in_tables(
    program: &mut ProgramBuilder,
    tables: &mut TableReferences,
    t_ctx: &mut TranslateCtx,
) -> Result<()> {
    for table_reference in tables.joined_tables_mut().iter_mut() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table_reference.table {
            let from_clause_subquery = Arc::make_mut(from_clause_subquery);
            // First, recursively process nested plans
            pre_materialize_multi_ref_ctes(program, from_clause_subquery.plan.as_mut(), t_ctx)?;

            // Then check if THIS CTE should be materialized
            if let Some(cte_id) = from_clause_subquery.cte_id {
                if program.get_materialized_cte(cte_id).is_some() {
                    continue;
                }
                let is_multi_ref = program.get_cte_reference_count(cte_id) > 1;
                if from_clause_subquery.materialize_hint || is_multi_ref {
                    let (result_columns_start, cte_cursor_id, cte_table) = emit_materialized_cte(
                        program,
                        from_clause_subquery.plan.as_mut(),
                        t_ctx,
                        from_clause_subquery.columns.len(),
                    )?;
                    program.register_materialized_cte(
                        cte_id,
                        MaterializedCteInfo {
                            cursor_id: cte_cursor_id,
                            table: cte_table,
                            result_columns_start_reg: result_columns_start,
                            num_columns: from_clause_subquery.columns.len(),
                        },
                    );
                    from_clause_subquery.result_columns_start_reg = Some(result_columns_start);
                    program
                        .set_subquery_result_reg(table_reference.internal_id, result_columns_start);
                }
            }
        }
    }
    Ok(())
}

/// Emit the subqueries contained in the FROM clause.
/// This is done first so the results can be read in the main query loop.
pub fn emit_from_clause_subqueries(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &mut TableReferences,
    join_order: &[JoinOrderMember],
) -> Result<()> {
    if tables.joined_tables().is_empty() {
        emit_explain!(program, false, "SCAN CONSTANT ROW".to_owned());
    }

    // FIRST PASS: Pre-materialize all multi-ref CTEs recursively.
    // This ensures materialized CTEs are available BEFORE any coroutines that might
    // reference them internally. Without this, a coroutine might try to materialize
    // a CTE inside its bytecode, but the cursor wouldn't be opened until the coroutine
    // runs, causing OpenDup failures in the main code path.
    for table_reference in tables.joined_tables_mut().iter_mut() {
        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table_reference.table {
            let from_clause_subquery = Arc::make_mut(from_clause_subquery);
            // Recursively pre-materialize CTEs in this subquery's plan FIRST
            pre_materialize_multi_ref_ctes(program, from_clause_subquery.plan.as_mut(), t_ctx)?;

            // Then check if THIS subquery's CTE should be materialized
            if let Some(cte_id) = from_clause_subquery.cte_id {
                if program.get_materialized_cte(cte_id).is_some() {
                    continue;
                }
                let is_multi_ref = program.get_cte_reference_count(cte_id) > 1;
                if from_clause_subquery.materialize_hint || is_multi_ref {
                    let (result_columns_start, cte_cursor_id, cte_table) = emit_materialized_cte(
                        program,
                        from_clause_subquery.plan.as_mut(),
                        t_ctx,
                        from_clause_subquery.columns.len(),
                    )?;
                    program.register_materialized_cte(
                        cte_id,
                        MaterializedCteInfo {
                            cursor_id: cte_cursor_id,
                            table: cte_table,
                            result_columns_start_reg: result_columns_start,
                            num_columns: from_clause_subquery.columns.len(),
                        },
                    );
                    from_clause_subquery.result_columns_start_reg = Some(result_columns_start);
                    program
                        .set_subquery_result_reg(table_reference.internal_id, result_columns_start);
                }
            }
        }
    }

    // Build the iteration order: join_order first (execution order), then any
    // hash-join build tables that aren't already in the join order.
    let mut visit_order: Vec<usize> = join_order
        .iter()
        .map(|member| member.original_idx)
        .collect();
    let visit_set: HashSet<usize> = visit_order.iter().copied().collect();
    for table in tables.joined_tables().iter() {
        if let Operation::HashJoin(hash_join_op) = &table.op {
            let build_idx = hash_join_op.build_table_idx;
            if !visit_set.contains(&build_idx) {
                visit_order.push(build_idx);
            }
        }
    }

    for table_index in visit_order {
        let table_reference = &mut tables.joined_tables_mut()[table_index];
        emit_explain!(
            program,
            true,
            match &table_reference.op {
                Operation::Scan(scan) => {
                    let table_name =
                        if table_reference.table.get_name() == table_reference.identifier {
                            table_reference.identifier.clone()
                        } else {
                            format!(
                                "{} AS {}",
                                table_reference.table.get_name(),
                                table_reference.identifier
                            )
                        };

                    match scan {
                        Scan::BTreeTable { index, .. } => {
                            if let Some(index) = index {
                                if table_reference.utilizes_covering_index() {
                                    format!("SCAN {table_name} USING COVERING INDEX {}", index.name)
                                } else {
                                    format!("SCAN {table_name} USING INDEX {}", index.name)
                                }
                            } else {
                                format!("SCAN {table_name}")
                            }
                        }
                        Scan::VirtualTable { .. } | Scan::Subquery => {
                            format!("SCAN {table_name}")
                        }
                    }
                }
                Operation::Search(search) => match search {
                    Search::RowidEq { .. } | Search::Seek { index: None, .. } => {
                        format!(
                            "SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)",
                            table_reference.identifier
                        )
                    }
                    Search::Seek {
                        index: Some(index), ..
                    } => {
                        format!(
                            "SEARCH {} USING INDEX {}",
                            table_reference.identifier, index.name
                        )
                    }
                },
                Operation::IndexMethodQuery(query) => {
                    let index_method = query.index.index_method.as_ref().unwrap();
                    format!(
                        "QUERY INDEX METHOD {}",
                        index_method.definition().method_name
                    )
                }
                Operation::HashJoin(_) => {
                    let table_name =
                        if table_reference.table.get_name() == table_reference.identifier {
                            table_reference.identifier.clone()
                        } else {
                            format!(
                                "{} AS {}",
                                table_reference.table.get_name(),
                                table_reference.identifier
                            )
                        };
                    format!("HASH JOIN {table_name}")
                }
                Operation::MultiIndexScan(multi_idx) => {
                    let index_names: Vec<&str> = multi_idx
                        .branches
                        .iter()
                        .map(|b| {
                            b.index
                                .as_ref()
                                .map(|i| i.name.as_str())
                                .unwrap_or("PRIMARY KEY")
                        })
                        .collect();
                    format!(
                        "MULTI-INDEX {} {} ({})",
                        match multi_idx.set_op {
                            SetOperation::Union => "OR",
                            SetOperation::Intersection => "AND",
                        },
                        table_reference.identifier,
                        index_names.join(", ")
                    )
                }
            }
        );

        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table_reference.table {
            let from_clause_subquery = Arc::make_mut(from_clause_subquery);
            // Check if this is a CTE that's already materialized
            if let Some(cte_id) = from_clause_subquery.cte_id {
                if let Some(cte_info) = program.get_materialized_cte(cte_id).cloned() {
                    // === SUBSEQUENT CTE REFERENCE: Use OpenDup ===
                    // Create a dup cursor pointing to the same ephemeral table
                    let dup_cursor_id =
                        program.alloc_cursor_id(CursorType::BTreeTable(cte_info.table.clone()));
                    program.emit_insn(Insn::OpenDup {
                        new_cursor_id: dup_cursor_id,
                        original_cursor_id: cte_info.cursor_id,
                    });

                    // If this reference needs a seek index, build it from the dup cursor
                    if let Operation::Search(Search::Seek {
                        index: Some(index),
                        seek_def,
                        ..
                    }) = &table_reference.op
                    {
                        if !index.ephemeral {
                            panic!("subquery has non-ephemeral index: {}", index.name);
                        }
                        let dup_affinity_str = {
                            let num_key_cols = seek_def.size(&seek_def.start);
                            let total_cols =
                                index.columns.len() + if index.has_rowid { 1 } else { 0 };
                            let mut aff: String = seek_def
                                .iter_affinity(&seek_def.start)
                                .map(|a| a.aff_mask())
                                .collect();
                            for _ in num_key_cols..total_cols {
                                aff.push(affinity::SQLITE_AFF_NONE);
                            }
                            if aff.chars().any(|c| c != affinity::SQLITE_AFF_NONE) {
                                Some(Arc::new(aff))
                            } else {
                                None
                            }
                        };
                        emit_seek_index_from_cursor(
                            program,
                            dup_cursor_id,
                            index,
                            table_reference.internal_id,
                            cte_info.num_columns,
                            dup_affinity_str.as_ref(),
                        )?;
                    }

                    // Update the plan's query destination to EphemeralTable so that
                    // main_loop knows to use Rewind/Next instead of coroutine Yield
                    if let Some(dest) = from_clause_subquery.plan.select_query_destination_mut() {
                        *dest = QueryDestination::EphemeralTable {
                            cursor_id: dup_cursor_id,
                            table: cte_info.table.clone(),
                            rowid_mode: super::plan::EphemeralRowidMode::Auto,
                        };
                    }

                    // Each CTE reference needs its OWN registers to read column values into.
                    // We cannot share the original's result_columns_start_reg because multiple
                    // iterators of the same CTE (e.g., outer query and subquery) would
                    // overwrite each other's values when reading columns from their cursors.
                    let result_columns_start = program.alloc_registers(cte_info.num_columns);
                    from_clause_subquery.result_columns_start_reg = Some(result_columns_start);
                    program
                        .set_subquery_result_reg(table_reference.internal_id, result_columns_start);
                    program.pop_current_parent_explain();
                    continue; // Skip normal emission
                }
            }

            let (seek_index, seek_affinity_str) = match &table_reference.op {
                Operation::Search(Search::Seek {
                    index: Some(idx),
                    seek_def,
                    ..
                }) if idx.ephemeral => {
                    // Build affinity string for MakeRecord when inserting into the
                    // ephemeral index. Key columns get the comparison affinity so that
                    // cross-type seeks work (e.g. integer probe on text CTE values).
                    // Non-key columns and rowid get NONE (no coercion).
                    let num_key_cols = seek_def.size(&seek_def.start);
                    let total_cols = idx.columns.len() + if idx.has_rowid { 1 } else { 0 };
                    let mut aff: String = seek_def
                        .iter_affinity(&seek_def.start)
                        .map(|a| a.aff_mask())
                        .collect();
                    for _ in num_key_cols..total_cols {
                        aff.push(affinity::SQLITE_AFF_NONE);
                    }
                    let has_non_none = aff.chars().any(|c| c != affinity::SQLITE_AFF_NONE);
                    (
                        Some(idx.clone()),
                        if has_non_none {
                            Some(Arc::new(aff))
                        } else {
                            None
                        },
                    )
                }
                _ => (None, None),
            };

            // Determine if this CTE/subquery should be materialized:
            // - materialize_hint=true: from WITH ... AS MATERIALIZED hint
            // - Multi-reference CTE: materialize once, share via OpenDup
            // - Single-reference CTE: use coroutine (more efficient)
            let is_multi_ref_cte = from_clause_subquery
                .cte_id
                .is_some_and(|id| program.get_cte_reference_count(id) > 1);
            let is_cte_and_should_materialize =
                from_clause_subquery.materialize_hint || is_multi_ref_cte;

            if is_cte_and_should_materialize {
                // CTE sharing path: EphemeralTable + optional seek index
                // Must use EphemeralTable to enable sharing via OpenDup
                let (result_columns_start, cte_cursor_id, cte_table) = emit_materialized_cte(
                    program,
                    from_clause_subquery.plan.as_mut(),
                    t_ctx,
                    from_clause_subquery.columns.len(),
                )?;

                // Build seek index if needed
                if let Some(index) = &seek_index {
                    emit_seek_index_from_cursor(
                        program,
                        cte_cursor_id,
                        index,
                        table_reference.internal_id,
                        from_clause_subquery.columns.len(),
                        seek_affinity_str.as_ref(),
                    )?;
                }

                // Register CTE for future references via OpenDup
                if let Some(cte_id) = from_clause_subquery.cte_id {
                    program.register_materialized_cte(
                        cte_id,
                        MaterializedCteInfo {
                            cursor_id: cte_cursor_id,
                            table: cte_table,
                            result_columns_start_reg: result_columns_start,
                            num_columns: from_clause_subquery.columns.len(),
                        },
                    );
                }

                from_clause_subquery.result_columns_start_reg = Some(result_columns_start);
                program.set_subquery_result_reg(table_reference.internal_id, result_columns_start);
            } else if let Some(index) = seek_index {
                // Seek-only path: materialize directly into EphemeralIndex.
                // This works for both Select and CompoundSelect plans.
                let result_columns_start = emit_indexed_materialized_subquery(
                    program,
                    from_clause_subquery.plan.as_mut(),
                    t_ctx,
                    &index,
                    table_reference.internal_id,
                    from_clause_subquery.columns.len(),
                    seek_affinity_str,
                )?;
                from_clause_subquery.result_columns_start_reg = Some(result_columns_start);
                program.set_subquery_result_reg(table_reference.internal_id, result_columns_start);
            } else {
                // Emit the subquery as a coroutine and get the start register of the result columns.
                let result_columns_start =
                    emit_from_clause_subquery(program, from_clause_subquery.plan.as_mut(), t_ctx)?;
                // Set the start register of the subquery's result columns.
                // This is done so that translate_expr() can read the result columns of the subquery,
                // as if it were reading from a regular table.
                from_clause_subquery.result_columns_start_reg = Some(result_columns_start);
                // Also store in program builder so nested subqueries can look it up by internal_id.
                program.set_subquery_result_reg(table_reference.internal_id, result_columns_start);
            }
        }

        program.pop_current_parent_explain();
    }
    Ok(())
}

/// Emit a FROM clause subquery and return the start register of the result columns.
/// This is done by emitting a coroutine that stores the result columns in sequential registers.
/// Each FROM clause subquery has its own Plan (either SelectPlan or CompoundSelect) which is wrapped in a coroutine.
///
/// The resulting bytecode from a subquery is mostly exactly the same as a regular query, except:
/// - it ends in an EndCoroutine instead of a Halt.
/// - instead of emitting ResultRows, the coroutine yields to the main query loop.
/// - the first register of the result columns is returned to the parent query,
///   so that translate_expr() can read the result columns of the subquery,
///   as if it were reading from a regular table.
///
/// Since a subquery has its own Plan, it can contain nested subqueries,
/// which can contain even more nested subqueries, etc.
pub fn emit_from_clause_subquery(
    program: &mut ProgramBuilder,
    plan: &mut Plan,
    t_ctx: &mut TranslateCtx,
) -> Result<usize> {
    let yield_reg = program.alloc_register();
    let coroutine_implementation_start_offset = program.allocate_label();

    // Set up the coroutine yield destination for the plan
    match plan.select_query_destination_mut() {
        Some(QueryDestination::CoroutineYield {
            yield_reg: y,
            coroutine_implementation_start,
        }) => {
            // The parent query will use this register to jump to/from the subquery.
            *y = yield_reg;
            // The parent query will use this register to reinitialize the coroutine when it needs to run multiple times.
            *coroutine_implementation_start = coroutine_implementation_start_offset;
        }
        _ => unreachable!("emit_from_clause_subquery called on non-subquery"),
    }

    let subquery_body_end_label = program.allocate_label();

    program.emit_insn(Insn::InitCoroutine {
        yield_reg,
        jump_on_definition: subquery_body_end_label,
        start_offset: coroutine_implementation_start_offset,
    });
    program.preassign_label_to_next_insn(coroutine_implementation_start_offset);

    let result_column_start_reg = match plan {
        Plan::Select(select_plan) => {
            let mut metadata = TranslateCtx {
                labels_main_loop: (0..select_plan.joined_tables().len())
                    .map(|_| LoopLabels::new(program))
                    .collect(),
                label_main_loop_end: None,
                meta_group_by: None,
                meta_left_joins: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                meta_sort: None,
                reg_agg_start: None,
                reg_nonagg_emit_once_flag: None,
                reg_result_cols_start: None,
                limit_ctx: None,
                reg_offset: None,
                reg_limit_offset_sum: None,
                resolver: t_ctx.resolver.fork(),
                non_aggregate_expressions: Vec::new(),
                cdc_cursor_id: None,
                meta_window: None,
                materialized_build_inputs: HashMap::default(),
                hash_table_contexts: HashMap::default(),
            };
            emit_query(program, select_plan, &mut metadata)?
        }
        Plan::CompoundSelect { .. } => {
            // Clone the plan to pass to emit_program_for_compound_select (it takes ownership)
            let plan_clone = plan.clone();
            let resolver = t_ctx.resolver.fork();
            // emit_program_for_compound_select returns the result column start register
            // for coroutine mode, which is needed by the outer query.
            emit_program_for_compound_select(program, &resolver, plan_clone)?
                .expect("compound CTE in coroutine mode must have result register")
        }
        Plan::Delete(_) | Plan::Update(_) => {
            unreachable!("DELETE/UPDATE plans cannot be FROM clause subqueries")
        }
    };

    program.emit_insn(Insn::EndCoroutine { yield_reg });
    program.preassign_label_to_next_insn(subquery_body_end_label);
    Ok(result_column_start_reg)
}
/// Materialize a CTE/subquery into an ephemeral table (not index).
/// This is used for CTEs to enable sharing via OpenDup - the ephemeral table holds
/// the CTE data, and subsequent references can alias it with OpenDup.
///
/// Returns (result_columns_start_reg, cursor_id, table) where table is the ephemeral
/// table definition needed for allocating dup cursors.
fn emit_materialized_cte(
    program: &mut ProgramBuilder,
    plan: &mut Plan,
    t_ctx: &mut TranslateCtx,
    num_columns: usize,
) -> Result<(usize, CursorID, Arc<BTreeTable>)> {
    use super::plan::EphemeralRowidMode;

    // Create a dummy BTreeTable for the ephemeral table's cursor type.
    // EphemeralTable (not EphemeralIndex) is required because it preserves
    // insertion order, which SQL semantics require for UNION ALL.
    let ephemeral_table = Arc::new(BTreeTable {
        root_page: 0,
        name: String::new(),
        columns: vec![],
        primary_key_columns: vec![],
        has_rowid: true,
        is_strict: false,
        has_autoincrement: false,
        unique_sets: vec![],
        foreign_keys: vec![],
        check_constraints: vec![],
    });

    // Allocate cursor for the ephemeral table
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(ephemeral_table.clone()));

    // Allocate registers for reading result columns
    let result_columns_start_reg = program.alloc_registers(num_columns);

    // Open the ephemeral table
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: true,
    });

    // Set the query destination to write to the ephemeral table
    if let Some(dest) = plan.select_query_destination_mut() {
        *dest = QueryDestination::EphemeralTable {
            cursor_id,
            table: ephemeral_table.clone(),
            rowid_mode: EphemeralRowidMode::Auto,
        };
    }

    // Emit the subquery - it will insert rows into the ephemeral table
    match plan {
        Plan::Select(select_plan) => {
            let mut metadata = TranslateCtx {
                labels_main_loop: (0..select_plan.joined_tables().len())
                    .map(|_| LoopLabels::new(program))
                    .collect(),
                label_main_loop_end: None,
                meta_group_by: None,
                meta_left_joins: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                meta_sort: None,
                reg_agg_start: None,
                reg_nonagg_emit_once_flag: None,
                reg_result_cols_start: None,
                limit_ctx: None,
                reg_offset: None,
                reg_limit_offset_sum: None,
                resolver: t_ctx.resolver.fork(),
                non_aggregate_expressions: Vec::new(),
                cdc_cursor_id: None,
                meta_window: None,
                materialized_build_inputs: HashMap::default(),
                hash_table_contexts: HashMap::default(),
            };
            emit_query(program, select_plan, &mut metadata)?;
        }
        Plan::CompoundSelect { .. } => {
            // Clone the plan to pass to emit_program_for_compound_select (it takes ownership)
            let plan_clone = plan.clone();
            let resolver = t_ctx.resolver.fork();
            emit_program_for_compound_select(program, &resolver, plan_clone)?;
        }
        Plan::Delete(_) | Plan::Update(_) => {
            unreachable!("DELETE/UPDATE plans cannot be FROM clause subqueries")
        }
    }

    Ok((result_columns_start_reg, cursor_id, ephemeral_table))
}

/// Materialize a simple SELECT subquery directly into an ephemeral index.
/// This avoids the intermediate ephemeral table, halving materialization cost.
///
/// # Preconditions
/// - Must be a simple SELECT plan, not CompoundSelect (UNION/INTERSECT/EXCEPT).
///   Compound selects need separate deduplication indexes that conflict with seek indexes.
/// - The index must be ephemeral and configured for seeking (has_rowid: true).
///
/// # Returns
/// The `result_columns_start_reg` - the main loop will read columns into these registers
/// when iterating the index via `Insn::Column`.
fn emit_indexed_materialized_subquery(
    program: &mut ProgramBuilder,
    plan: &mut Plan,
    t_ctx: &mut TranslateCtx,
    index: &Arc<Index>,
    table_internal_id: TableInternalId,
    num_columns: usize,
    affinity_str: Option<Arc<String>>,
) -> Result<usize> {
    // Allocate index cursor using CursorKey::index so main_loop can find it
    let index_cursor_id = program.alloc_cursor_id_keyed(
        CursorKey::index(table_internal_id, index.clone()),
        CursorType::BTreeIndex(index.clone()),
    );

    // Set QueryDestination::EphemeralIndex on the plan
    if let Some(dest) = plan.select_query_destination_mut() {
        *dest = QueryDestination::EphemeralIndex {
            cursor_id: index_cursor_id,
            index: index.clone(),
            affinity_str,
            is_delete: false,
        };
    }

    // Allocate registers for reading result columns
    let result_columns_start_reg = program.alloc_registers(num_columns);

    // Open the ephemeral index
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: index_cursor_id,
        is_table: false, // false = index
    });

    // Emit the plan - it will insert rows directly into the index
    match plan {
        Plan::Select(select_plan) => {
            let mut metadata = TranslateCtx {
                labels_main_loop: (0..select_plan.joined_tables().len())
                    .map(|_| LoopLabels::new(program))
                    .collect(),
                label_main_loop_end: None,
                meta_group_by: None,
                meta_left_joins: (0..select_plan.joined_tables().len())
                    .map(|_| None)
                    .collect(),
                meta_sort: None,
                reg_agg_start: None,
                reg_nonagg_emit_once_flag: None,
                reg_result_cols_start: None,
                limit_ctx: None,
                reg_offset: None,
                reg_limit_offset_sum: None,
                resolver: t_ctx.resolver.fork(),
                non_aggregate_expressions: Vec::new(),
                cdc_cursor_id: None,
                meta_window: None,
                materialized_build_inputs: HashMap::default(),
                hash_table_contexts: HashMap::default(),
            };
            emit_query(program, select_plan, &mut metadata)?;
        }
        Plan::CompoundSelect { .. } => {
            let plan_clone = plan.clone();
            let resolver = t_ctx.resolver.fork();
            emit_program_for_compound_select(program, &resolver, plan_clone)?;
        }
        Plan::Delete(_) | Plan::Update(_) => {
            unreachable!("DELETE/UPDATE plans cannot be FROM clause subqueries")
        }
    }

    Ok(result_columns_start_reg)
}

/// Build a seek index from an existing ephemeral table cursor.
/// This is used when there is a materialized CTE that is used
/// in multiple places (e.g. one place scans it and other wants to seek it),
/// so it's first built into an ephmeral table elsewhere and then a separate
/// index is constructed from that here.
fn emit_seek_index_from_cursor(
    program: &mut ProgramBuilder,
    source_cursor_id: CursorID,
    index: &Arc<Index>,
    table_internal_id: TableInternalId,
    num_columns: usize,
    affinity_str: Option<&Arc<String>>,
) -> Result<()> {
    // Allocate cursor for the seek index
    let index_cursor_id = program.alloc_cursor_id_keyed(
        CursorKey::index(table_internal_id, index.clone()),
        CursorType::BTreeIndex(index.clone()),
    );

    // Open the ephemeral index
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: index_cursor_id,
        is_table: false,
    });

    // Rewind the source cursor to iterate all rows
    let loop_start = program.allocate_label();
    let loop_end = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: source_cursor_id,
        pc_if_empty: loop_end,
    });

    program.preassign_label_to_next_insn(loop_start);

    // Read columns from source cursor and build index record
    let record_reg = program.alloc_register();
    let column_regs_start = program.alloc_registers(num_columns);

    // Read all columns from source cursor
    for i in 0..num_columns {
        program.emit_insn(Insn::Column {
            cursor_id: source_cursor_id,
            column: i,
            dest: column_regs_start + i,
            default: None,
        });
    }

    // Build the index key from the index columns
    let num_key_cols = index.columns.len() + 1; // +1 for rowid
    let key_regs_start = program.alloc_registers(num_key_cols);
    for (i, col) in index.columns.iter().enumerate() {
        program.emit_insn(Insn::Copy {
            src_reg: column_regs_start + col.pos_in_table,
            dst_reg: key_regs_start + i,
            extra_amount: 0,
        });
    }

    // Add rowid as last column of index key
    program.emit_insn(Insn::RowId {
        cursor_id: source_cursor_id,
        dest: key_regs_start + index.columns.len(),
    });

    // Make record and insert into index.
    // affinity_str coerces key columns so cross-type seeks work (e.g. integer
    // probe on text CTE values).
    program.emit_insn(Insn::MakeRecord {
        start_reg: key_regs_start as u16,
        count: num_key_cols as u16,
        dest_reg: record_reg as u16,
        index_name: Some(index.name.clone()),
        affinity_str: affinity_str.map(|s| (**s).clone()),
    });

    program.emit_insn(Insn::IdxInsert {
        cursor_id: index_cursor_id,
        record_reg,
        unpacked_start: Some(key_regs_start),
        unpacked_count: Some(num_key_cols as u16),
        flags: crate::vdbe::insn::IdxInsertFlags::default(),
    });

    // Next row
    program.emit_insn(Insn::Next {
        cursor_id: source_cursor_id,
        pc_if_next: loop_start,
    });

    program.preassign_label_to_next_insn(loop_end);

    Ok(())
}

/// Translate a subquery that is not part of the FROM clause.
/// If a subquery is uncorrelated (i.e. does not reference columns from the outer query),
/// it will be executed only once.
///
/// If it is correlated (i.e. references columns from the outer query),
/// it will be executed for each row of the outer query.
///
/// The result of the subquery is stored in:
///
/// - a single register for EXISTS subqueries,
/// - a range of registers for RowValue subqueries,
/// - an ephemeral index for IN subqueries.
pub fn emit_non_from_clause_subquery(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: SelectPlan,
    query_type: &SubqueryType,
    is_correlated: bool,
) -> Result<()> {
    program.nested(|program| {
        let subquery_id = program.next_subquery_eqp_id();
        let correlated_prefix = if is_correlated { "CORRELATED " } else { "" };
        match query_type {
            SubqueryType::Exists { .. } => {
                // EXISTS subqueries don't get a separate EQP annotation in SQLite;
                // instead the SEARCH/SCAN line gets an "EXISTS" suffix handled elsewhere.
            }
            SubqueryType::In { .. } => {
                emit_explain!(
                    program,
                    true,
                    format!("{correlated_prefix}LIST SUBQUERY {subquery_id}")
                );
            }
            SubqueryType::RowValue { .. } => {
                emit_explain!(
                    program,
                    true,
                    format!("{correlated_prefix}SCALAR SUBQUERY {subquery_id}")
                );
            }
        }

        let label_skip_after_first_run = if !is_correlated {
            let label = program.allocate_label();
            program.emit_insn(Insn::Once {
                target_pc_when_reentered: label,
            });
            Some(label)
        } else {
            None
        };

        match query_type {
            SubqueryType::Exists { result_reg, .. } => {
                let subroutine_reg = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: subroutine_reg,
                    dest_end: None,
                });
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: *result_reg,
                });
                emit_program_for_select(program, resolver, plan)?;
                program.emit_insn(Insn::Return {
                    return_reg: subroutine_reg,
                    can_fallthrough: true,
                });
            }
            SubqueryType::In { cursor_id, .. } => {
                program.emit_insn(Insn::OpenEphemeral {
                    cursor_id: *cursor_id,
                    is_table: false,
                });
                emit_program_for_select(program, resolver, plan)?;
            }
            SubqueryType::RowValue {
                result_reg_start,
                num_regs,
            } => {
                let subroutine_reg = program.alloc_register();
                program.emit_insn(Insn::BeginSubrtn {
                    dest: subroutine_reg,
                    dest_end: None,
                });
                for result_reg in *result_reg_start..*result_reg_start + *num_regs {
                    program.emit_insn(Insn::Null {
                        dest: result_reg,
                        dest_end: None,
                    });
                }
                emit_program_for_select(program, resolver, plan)?;
                program.emit_insn(Insn::Return {
                    return_reg: subroutine_reg,
                    can_fallthrough: true,
                });
            }
        }
        // Pop the parent explain for LIST/SCALAR SUBQUERY annotations.
        if !matches!(query_type, SubqueryType::Exists { .. }) {
            program.pop_current_parent_explain();
        }
        if let Some(label) = label_skip_after_first_run {
            program.preassign_label_to_next_insn(label);
        }
        Ok(())
    })
}
