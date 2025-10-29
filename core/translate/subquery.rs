use std::sync::Arc;

use turso_parser::ast::{self, SortOrder, SubqueryType};

use crate::{
    emit_explain,
    schema::{Index, IndexColumn, Table},
    translate::{
        collate::get_collseq_from_expr,
        emitter::emit_program_for_select,
        expr::{unwrap_parens, walk_expr_mut, WalkControl},
        optimizer::optimize_select_plan,
        plan::{
            ColumnUsedMask, NonFromClauseSubquery, OuterQueryReference, Plan, SubqueryPosition,
            SubqueryState,
        },
        select::prepare_select_plan,
    },
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::Insn,
    },
    Connection, QueryMode, Result,
};

use super::{
    emitter::{emit_query, Resolver, TranslateCtx},
    main_loop::LoopLabels,
    plan::{Operation, QueryDestination, Search, SelectPlan, TableReferences},
};

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
        plan.order_by.iter_mut().map(|(expr, _)| &mut **expr),
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

    update_column_used_masks(
        &mut plan.table_references,
        &mut plan.non_from_clause_subqueries,
    );
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
            .map(|t| OuterQueryReference {
                table: t.table.clone(),
                identifier: t.identifier.clone(),
                internal_id: t.internal_id,
                col_used_mask: ColumnUsedMask::default(),
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
                optimize_select_plan(&mut plan, resolver.schema)?;
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
                optimize_select_plan(&mut plan, resolver.schema)?;
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
                optimize_select_plan(&mut plan, resolver.schema)?;
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
                    index: ephemeral_index.clone(),
                    is_delete: false,
                };

                *expr = ast::Expr::SubqueryResult {
                    subquery_id,
                    lhs: Some(lhs),
                    not_in: not,
                    query_type: SubqueryType::In { cursor_id },
                };

                let correlated = plan.is_correlated();
                handle_unsupported_correlation(correlated, position)?;

                out_subqueries.push(NonFromClauseSubquery {
                    internal_id: subquery_id,
                    query_type: SubqueryType::In { cursor_id },
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
    for subquery in subqueries.iter_mut() {
        let SubqueryState::Unevaluated { plan } = &mut subquery.state else {
            panic!("subquery has already been evaluated");
        };
        let Some(child_plan) = plan.as_mut() else {
            panic!("subquery has no plan");
        };

        for child_outer_query_ref in child_plan
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
    }
}

/// Emit the subqueries contained in the FROM clause.
/// This is done first so the results can be read in the main query loop.
pub fn emit_from_clause_subqueries(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &mut TableReferences,
) -> Result<()> {
    if tables.joined_tables().is_empty() {
        emit_explain!(program, false, "SCAN CONSTANT ROW".to_owned());
    }

    for table_reference in tables.joined_tables_mut() {
        emit_explain!(
            program,
            true,
            match &table_reference.op {
                Operation::Scan { .. } => {
                    if table_reference.table.get_name() == table_reference.identifier {
                        format!("SCAN {}", table_reference.identifier)
                    } else {
                        format!(
                            "SCAN {} AS {}",
                            table_reference.table.get_name(),
                            table_reference.identifier
                        )
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
            }
        );

        if let Table::FromClauseSubquery(from_clause_subquery) = &mut table_reference.table {
            // Emit the subquery and get the start register of the result columns.
            let result_columns_start =
                emit_from_clause_subquery(program, &mut from_clause_subquery.plan, t_ctx)?;
            // Set the start register of the subquery's result columns.
            // This is done so that translate_expr() can read the result columns of the subquery,
            // as if it were reading from a regular table.
            from_clause_subquery.result_columns_start_reg = Some(result_columns_start);
        }

        program.pop_current_parent_explain();
    }
    Ok(())
}

/// Emit a FROM clause subquery and return the start register of the result columns.
/// This is done by emitting a coroutine that stores the result columns in sequential registers.
/// Each FROM clause subquery has its own separate SelectPlan which is wrapped in a coroutine.
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
pub fn emit_from_clause_subquery(
    program: &mut ProgramBuilder,
    plan: &mut SelectPlan,
    t_ctx: &mut TranslateCtx,
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
        _ => unreachable!("emit_from_clause_subquery called on non-subquery"),
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
        limit_ctx: None,
        reg_offset: None,
        reg_limit_offset_sum: None,
        resolver: Resolver::new(t_ctx.resolver.schema, t_ctx.resolver.symbol_table),
        non_aggregate_expressions: Vec::new(),
        cdc_cursor_id: None,
        meta_window: None,
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
    t_ctx: &mut TranslateCtx,
    plan: SelectPlan,
    query_type: &SubqueryType,
    is_correlated: bool,
) -> Result<()> {
    program.incr_nesting();

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
            emit_program_for_select(program, &t_ctx.resolver, plan)?;
            program.emit_insn(Insn::Return {
                return_reg: subroutine_reg,
                can_fallthrough: true,
            });
        }
        SubqueryType::In { cursor_id } => {
            program.emit_insn(Insn::OpenEphemeral {
                cursor_id: *cursor_id,
                is_table: false,
            });
            emit_program_for_select(program, &t_ctx.resolver, plan)?;
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
            emit_program_for_select(program, &t_ctx.resolver, plan)?;
            program.emit_insn(Insn::Return {
                return_reg: subroutine_reg,
                can_fallthrough: true,
            });
        }
    }
    if let Some(label) = label_skip_after_first_run {
        program.preassign_label_to_next_insn(label);
    }

    program.decr_nesting();
    Ok(())
}
