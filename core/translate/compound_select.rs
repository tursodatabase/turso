use crate::schema::{Index, IndexColumn, PseudoCursorType};
use crate::sync::Arc;
use crate::translate::collate::get_collseq_from_expr;
use crate::translate::emitter::{select::emit_query, LimitCtx, Resolver, TranslateCtx};
use crate::translate::expr::translate_expr;
use crate::translate::order_by::{custom_type_comparator, sorter_insert};
use crate::translate::plan::{Plan, QueryDestination, SelectPlan};
use crate::translate::result_row::emit_columns_to_destination;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::Insn;
use crate::{emit_explain, LimboError};
use tracing::instrument;
use turso_parser::ast::{CompoundOperator, Expr, Literal, SortOrder};

use tracing::Level;

/// Emits bytecode for a compound SELECT statement (UNION, INTERSECT, EXCEPT, UNION ALL).
/// Returns the result column start register when in coroutine mode (for CTE subqueries),
/// or None for top-level queries.
#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_program_for_compound_select(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: Plan,
) -> crate::Result<Option<usize>> {
    // Extract fields we need before plan is consumed by emit_compound_select.
    let (has_order_by, order_by_owned, limit_owned, offset_owned, right_plan) = {
        let Plan::CompoundSelect {
            left: _,
            right_most,
            limit,
            offset,
            order_by,
        } = &plan
        else {
            crate::bail_parse_error!("expected compound select plan");
        };
        (
            order_by.is_some(),
            order_by.clone(),
            limit.clone(),
            offset.clone(),
            right_most.clone(),
        )
    };
    let Plan::CompoundSelect { ref left, .. } = plan else {
        unreachable!()
    };
    let right_most_ctx = Box::new(TranslateCtx::new(
        program,
        resolver.fork(),
        right_plan.table_references.joined_tables().len(),
        false,
    ));

    // Each subselect shares the same limit_ctx and offset, because the LIMIT, OFFSET applies to
    // the entire compound select, not just a single subselect.
    // When ORDER BY is present, LIMIT/OFFSET apply to the final sorted output, not intermediate results.
    let limit_ctx = if has_order_by {
        None
    } else {
        limit_owned
            .as_ref()
            .map(|limit| {
                let reg = program.alloc_register();
                match limit.as_ref() {
                    Expr::Literal(Literal::Numeric(n)) => {
                        if let Ok(value) = n.parse::<i64>() {
                            program.add_comment(program.offset(), "LIMIT counter");
                            program.emit_insn(Insn::Integer { value, dest: reg });
                        } else {
                            let value = n
                                .parse::<f64>()
                                .map_err(|_| LimboError::ParseError("invalid limit".to_string()))?;
                            program.emit_insn(Insn::Real { value, dest: reg });
                            program.add_comment(program.offset(), "LIMIT counter");
                            program.emit_insn(Insn::MustBeInt { reg });
                        }
                    }
                    _ => {
                        _ = translate_expr(program, None, limit, reg, &right_most_ctx.resolver);
                        program.add_comment(program.offset(), "LIMIT counter");
                        program.emit_insn(Insn::MustBeInt { reg });
                    }
                }
                Ok::<_, LimboError>(LimitCtx::new_shared(reg))
            })
            .transpose()?
    };
    let offset_reg = if has_order_by {
        None
    } else {
        offset_owned
            .as_ref()
            .map(|offset_expr| {
                let reg = program.alloc_register();
                match offset_expr.as_ref() {
                    Expr::Literal(Literal::Numeric(n)) => {
                        // Compile-time constant offset
                        if let Ok(value) = n.parse::<i64>() {
                            program.emit_insn(Insn::Integer { value, dest: reg });
                        } else {
                            let value = n.parse::<f64>().map_err(|_| {
                                LimboError::ParseError("invalid offset".to_string())
                            })?;
                            program.emit_insn(Insn::Real { value, dest: reg });
                        }
                    }
                    _ => {
                        _ = translate_expr(
                            program,
                            None,
                            offset_expr,
                            reg,
                            &right_most_ctx.resolver,
                        );
                    }
                }
                program.add_comment(program.offset(), "OFFSET counter");
                program.emit_insn(Insn::MustBeInt { reg });
                let combined_reg = program.alloc_register();
                program.add_comment(program.offset(), "OFFSET + LIMIT");
                program.emit_insn(Insn::OffsetLimit {
                    offset_reg: reg,
                    combined_reg,
                    limit_reg: limit_ctx.as_ref().unwrap().reg_limit,
                });

                Ok::<_, LimboError>(reg)
            })
            .transpose()?
    };

    let real_query_destination = right_plan.query_destination.clone();
    let num_result_cols = right_plan.result_columns.len();

    // When ORDER BY is present, redirect compound output to a collection index,
    // then sort and emit to the real destination afterwards.
    let (query_destination, collection_cursor, collection_index) = if has_order_by {
        let (cursor_id, index) = create_collection_index(program, &left[0].0, &right_plan)?;
        let dest = QueryDestination::EphemeralIndex {
            cursor_id,
            index: index.clone(),
            affinity_str: None,
            is_delete: false,
        };
        (dest, Some(cursor_id), Some(index))
    } else {
        (real_query_destination.clone(), None, None)
    };

    // Allocate registers for result columns when we need to hold values before emitting.
    let reg_result_cols_start = match &query_destination {
        QueryDestination::CoroutineYield { .. }
        | QueryDestination::EphemeralTable { .. }
        | QueryDestination::EphemeralIndex { .. } => Some(program.alloc_registers(num_result_cols)),
        QueryDestination::ResultRows => None,
        other => {
            return Err(LimboError::InternalError(format!(
                "Unexpected query destination: {other:?} for compound select"
            )));
        }
    };

    emit_explain!(program, true, "COMPOUND QUERY".to_owned());

    // This is inefficient, but emit_compound_select() takes ownership of 'plan' and we
    // must set the result columns to the leftmost subselect's result columns to be compatible
    // with SQLite.
    program.result_columns.clone_from(&left[0].0.result_columns);

    // These must also be set because we make the decision to start a transaction based on whether
    // any tables are actually touched by the query. Previously this only used the rightmost subselect's
    // table references, but that breaks down with e.g. "SELECT * FROM t UNION VALUES(1)" where VALUES(1)
    // does not have any table references and we would erroneously not start a transaction.
    for (plan, _) in left {
        program
            .table_references
            .extend(plan.table_references.clone());
    }
    program.table_references.extend(right_plan.table_references);

    // When ORDER BY is present, update all subselect destinations in the Plan
    // to write to the collection index instead of ResultRows.
    let mut plan = plan;
    if has_order_by {
        set_compound_plan_destinations(&mut plan, &query_destination);
    }

    program.with_scoped_result_cols_start(|program| {
        emit_compound_select(
            program,
            plan,
            &right_most_ctx.resolver,
            limit_ctx,
            offset_reg,
            reg_result_cols_start,
            &query_destination,
        )
    })?;
    program.pop_current_parent_explain();

    // When ORDER BY is present, sort the collected results and emit to the real destination.
    if let (Some(order_by), Some(collection_cursor_id), Some(collection_idx)) =
        (order_by_owned, collection_cursor, collection_index)
    {
        let result_reg = emit_compound_order_by(
            program,
            &order_by,
            collection_cursor_id,
            &collection_idx,
            num_result_cols,
            limit_owned.as_deref(),
            offset_owned.as_deref(),
            &real_query_destination,
            &right_most_ctx,
        )?;
        program.reg_result_cols_start = result_reg;
    } else {
        program.reg_result_cols_start = reg_result_cols_start;
    }

    Ok(program.reg_result_cols_start)
}

// Emits bytecode for a compound SELECT statement. This function processes the rightmost part of
// the compound SELECT and handles the left parts recursively based on the compound operator type.
#[allow(clippy::too_many_arguments)]
fn emit_compound_select(
    program: &mut ProgramBuilder,
    plan: Plan,
    resolver: &Resolver,
    limit_ctx: Option<LimitCtx>,
    offset_reg: Option<usize>,
    reg_result_cols_start: Option<usize>,
    query_destination: &QueryDestination,
) -> crate::Result<()> {
    let Plan::CompoundSelect {
        mut left,
        mut right_most,
        limit,
        offset,
        order_by,
    } = plan
    else {
        unreachable!()
    };

    let compound_select_end = program.allocate_label();
    if let Some(limit_ctx) = &limit_ctx {
        program.emit_insn(Insn::IfNot {
            reg: limit_ctx.reg_limit,
            target_pc: compound_select_end,
            jump_if_null: false,
        });
    }
    let mut right_most_ctx = Box::new(TranslateCtx::new(
        program,
        resolver.fork(),
        right_most.table_references.joined_tables().len(),
        false,
    ));
    right_most_ctx.reg_result_cols_start = reg_result_cols_start;
    match left.pop() {
        Some((mut plan, operator)) => match operator {
            CompoundOperator::UnionAll => {
                if matches!(
                    right_most.query_destination,
                    QueryDestination::EphemeralIndex { .. }
                        | QueryDestination::CoroutineYield { .. }
                        | QueryDestination::EphemeralTable { .. }
                ) {
                    plan.query_destination = right_most.query_destination.clone();
                }
                let compound_select = Plan::CompoundSelect {
                    left,
                    right_most: Box::new(plan),
                    limit: limit.clone(),
                    offset: offset.clone(),
                    order_by,
                };
                emit_compound_select(
                    program,
                    compound_select,
                    resolver,
                    limit_ctx,
                    offset_reg,
                    reg_result_cols_start,
                    query_destination,
                )?;

                let label_next_select = program.allocate_label();
                if let Some(limit_ctx) = limit_ctx {
                    program.emit_insn(Insn::IfNot {
                        reg: limit_ctx.reg_limit,
                        target_pc: label_next_select,
                        jump_if_null: true,
                    });
                    right_most.limit = limit;
                    right_most_ctx.limit_ctx = Some(limit_ctx);
                }
                if offset_reg.is_some() {
                    right_most.offset = offset;
                    right_most_ctx.reg_offset = offset_reg;
                }

                emit_explain!(program, true, "UNION ALL".to_owned());
                emit_query(program, &mut right_most, &mut right_most_ctx)?;
                program.pop_current_parent_explain();
                program.preassign_label_to_next_insn(label_next_select);
            }
            CompoundOperator::Union => {
                let mut new_dedupe_index = false;
                let dedupe_index = match &right_most.query_destination {
                    QueryDestination::EphemeralIndex {
                        cursor_id, index, ..
                    } if !index.has_rowid => (*cursor_id, index.clone()),
                    _ => {
                        new_dedupe_index = true;
                        create_dedupe_index(program, &plan, &right_most)?
                    }
                };
                plan.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: dedupe_index.0,
                    index: dedupe_index.1.clone(),
                    affinity_str: None,
                    is_delete: false,
                };
                let compound_select = Plan::CompoundSelect {
                    left,
                    right_most: Box::new(plan),
                    limit,
                    offset,
                    order_by,
                };
                emit_compound_select(
                    program,
                    compound_select,
                    resolver,
                    None,
                    None,
                    reg_result_cols_start,
                    query_destination,
                )?;

                right_most.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: dedupe_index.0,
                    index: dedupe_index.1.clone(),
                    affinity_str: None,
                    is_delete: false,
                };

                emit_explain!(program, true, "UNION USING TEMP B-TREE".to_owned());
                emit_query(program, &mut right_most, &mut right_most_ctx)?;
                program.pop_current_parent_explain();

                if new_dedupe_index {
                    read_deduplicated_union_or_except_rows(
                        program,
                        dedupe_index.0,
                        dedupe_index.1.as_ref(),
                        limit_ctx,
                        offset_reg,
                        reg_result_cols_start,
                        query_destination,
                    )?;
                }
            }
            CompoundOperator::Intersect => {
                // For nested compound selects (e.g., A INTERSECT B UNION C), the outer UNION
                // sets right_most.query_destination to its dedupe_index. We need to capture
                // this BEFORE we overwrite it with our own indexes for the intersection.
                let intersect_destination = right_most.query_destination.clone();

                let (left_cursor_id, left_index) =
                    create_dedupe_index(program, &plan, &right_most)?;
                plan.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: left_cursor_id,
                    index: left_index.clone(),
                    affinity_str: None,
                    is_delete: false,
                };

                let (right_cursor_id, right_index) =
                    create_dedupe_index(program, &plan, &right_most)?;
                right_most.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: right_cursor_id,
                    index: right_index,
                    affinity_str: None,
                    is_delete: false,
                };
                let compound_select = Plan::CompoundSelect {
                    left,
                    right_most: Box::new(plan),
                    limit,
                    offset,
                    order_by,
                };
                emit_compound_select(
                    program,
                    compound_select,
                    resolver,
                    None,
                    None,
                    reg_result_cols_start,
                    query_destination,
                )?;

                emit_explain!(program, true, "INTERSECT USING TEMP B-TREE".to_owned());
                emit_query(program, &mut right_most, &mut right_most_ctx)?;
                program.pop_current_parent_explain();
                read_intersect_rows(
                    program,
                    left_cursor_id,
                    &left_index,
                    right_cursor_id,
                    limit_ctx,
                    offset_reg,
                    reg_result_cols_start,
                    &intersect_destination,
                )?;
            }
            CompoundOperator::Except => {
                let mut new_index = false;
                let (cursor_id, index) = match &right_most.query_destination {
                    QueryDestination::EphemeralIndex {
                        cursor_id, index, ..
                    } if !index.has_rowid => (*cursor_id, index.clone()),
                    _ => {
                        new_index = true;
                        create_dedupe_index(program, &plan, &right_most)?
                    }
                };
                plan.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id,
                    index: index.clone(),
                    affinity_str: None,
                    is_delete: false,
                };
                let compound_select = Plan::CompoundSelect {
                    left,
                    right_most: Box::new(plan),
                    limit,
                    offset,
                    order_by,
                };
                emit_compound_select(
                    program,
                    compound_select,
                    resolver,
                    None,
                    None,
                    reg_result_cols_start,
                    query_destination,
                )?;
                right_most.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id,
                    index: index.clone(),
                    affinity_str: None,
                    is_delete: true,
                };
                emit_explain!(program, true, "EXCEPT USING TEMP B-TREE".to_owned());
                emit_query(program, &mut right_most, &mut right_most_ctx)?;
                program.pop_current_parent_explain();
                if new_index {
                    read_deduplicated_union_or_except_rows(
                        program,
                        cursor_id,
                        &index,
                        limit_ctx,
                        offset_reg,
                        reg_result_cols_start,
                        query_destination,
                    )?;
                }
            }
        },
        None => {
            if let Some(limit_ctx) = limit_ctx {
                right_most_ctx.limit_ctx = Some(limit_ctx);
                right_most.limit = limit;
            }
            if offset_reg.is_some() {
                right_most.offset = offset;
                right_most_ctx.reg_offset = offset_reg;
            }
            emit_explain!(program, true, "LEFT-MOST SUBQUERY".to_owned());
            emit_query(program, &mut right_most, &mut right_most_ctx)?;
            program.pop_current_parent_explain();
        }
    }

    program.preassign_label_to_next_insn(compound_select_end);

    Ok(())
}

// Creates an ephemeral index that will be used to deduplicate the results of any sub-selects
fn create_dedupe_index(
    program: &mut ProgramBuilder,
    left_select: &SelectPlan,
    right_select: &SelectPlan,
) -> crate::Result<(usize, Arc<Index>)> {
    let mut dedupe_columns = right_select
        .result_columns
        .iter()
        .enumerate()
        .map(|(i, c)| IndexColumn {
            name: c
                .name(&right_select.table_references)
                .map(|n| n.to_string())
                .unwrap_or_default(),
            order: SortOrder::Asc,
            pos_in_table: i,
            default: None,
            collation: None,
            expr: None,
        })
        .collect::<Vec<_>>();
    for (i, column) in dedupe_columns.iter_mut().enumerate() {
        let left_collation = get_collseq_from_expr(
            &left_select.result_columns[i].expr,
            &left_select.table_references,
        )?;
        let right_collation = get_collseq_from_expr(
            &right_select.result_columns[i].expr,
            &right_select.table_references,
        )?;
        // Left precedence
        let collation = match (left_collation, right_collation) {
            (None, None) => None,
            (Some(coll), None) | (None, Some(coll)) => Some(coll),
            (Some(coll), Some(_)) => Some(coll),
        };
        column.collation = collation;
    }

    let dedupe_index = Arc::new(Index {
        columns: dedupe_columns,
        name: "compound_dedupe".to_string(),
        root_page: 0,
        ephemeral: true,
        table_name: String::new(),
        unique: false,
        has_rowid: false,
        where_clause: None,
        index_method: None,
        on_conflict: None,
    });
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(dedupe_index.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: false,
    });
    Ok((cursor_id, dedupe_index))
}

/// Emits the bytecode for reading deduplicated rows from the ephemeral index created for
/// UNION or EXCEPT operators.
#[allow(clippy::too_many_arguments)]
fn read_deduplicated_union_or_except_rows(
    program: &mut ProgramBuilder,
    dedupe_cursor_id: usize,
    dedupe_index: &Index,
    limit_ctx: Option<LimitCtx>,
    offset_reg: Option<usize>,
    reg_result_cols_start: Option<usize>,
    query_destination: &QueryDestination,
) -> crate::Result<()> {
    let label_close = program.allocate_label();
    let label_dedupe_next = program.allocate_label();
    let label_dedupe_loop_start = program.allocate_label();
    // When in coroutine mode or emitting to index/table, use the pre-allocated result column registers.
    // Otherwise, allocate new registers for reading from the dedupe index.
    let dedupe_cols_start_reg = reg_result_cols_start
        .unwrap_or_else(|| program.alloc_registers(dedupe_index.columns.len()));
    program.emit_insn(Insn::Rewind {
        cursor_id: dedupe_cursor_id,
        pc_if_empty: label_dedupe_next,
    });
    program.preassign_label_to_next_insn(label_dedupe_loop_start);
    if let Some(reg) = offset_reg {
        program.emit_insn(Insn::IfPos {
            reg,
            target_pc: label_dedupe_next,
            decrement_by: 1,
        });
    }
    for col_idx in 0..dedupe_index.columns.len() {
        program.emit_insn(Insn::Column {
            cursor_id: dedupe_cursor_id,
            column: col_idx,
            dest: dedupe_cols_start_reg + col_idx,
            default: None,
        });
    }
    emit_columns_to_destination(
        program,
        query_destination,
        dedupe_cols_start_reg,
        dedupe_index.columns.len(),
    )?;

    if let Some(limit_ctx) = limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: label_close,
        })
    }
    program.preassign_label_to_next_insn(label_dedupe_next);
    program.emit_insn(Insn::Next {
        cursor_id: dedupe_cursor_id,
        pc_if_next: label_dedupe_loop_start,
    });
    program.preassign_label_to_next_insn(label_close);
    program.emit_insn(Insn::Close {
        cursor_id: dedupe_cursor_id,
    });
    Ok(())
}

/// Emits the bytecode for reading rows from the intersection of two cursors.
#[allow(clippy::too_many_arguments)]
fn read_intersect_rows(
    program: &mut ProgramBuilder,
    left_cursor_id: usize,
    index: &Index,
    right_cursor_id: usize,
    limit_ctx: Option<LimitCtx>,
    offset_reg: Option<usize>,
    reg_result_cols_start: Option<usize>,
    query_destination: &QueryDestination,
) -> crate::Result<()> {
    let label_close = program.allocate_label();
    let label_loop_start = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: left_cursor_id,
        pc_if_empty: label_close,
    });

    program.preassign_label_to_next_insn(label_loop_start);
    let row_content_reg = program.alloc_register();
    program.emit_insn(Insn::RowData {
        cursor_id: left_cursor_id,
        dest: row_content_reg,
    });
    let label_next = program.allocate_label();
    program.emit_insn(Insn::NotFound {
        cursor_id: right_cursor_id,
        target_pc: label_next,
        record_reg: row_content_reg,
        num_regs: 0,
    });
    if let Some(reg) = offset_reg {
        program.emit_insn(Insn::IfPos {
            reg,
            target_pc: label_next,
            decrement_by: 1,
        });
    }
    let column_count = index.columns.len();
    // When in coroutine mode, use the pre-allocated result column registers.
    // Otherwise, allocate new registers for reading from the index.
    let cols_start_reg =
        reg_result_cols_start.unwrap_or_else(|| program.alloc_registers(column_count));
    for i in 0..column_count {
        program.emit_insn(Insn::Column {
            cursor_id: left_cursor_id,
            column: i,
            dest: cols_start_reg + i,
            default: None,
        });
    }

    emit_columns_to_destination(program, query_destination, cols_start_reg, column_count)?;

    if let Some(limit_ctx) = limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: label_close,
        });
    }
    program.preassign_label_to_next_insn(label_next);
    program.emit_insn(Insn::Next {
        cursor_id: left_cursor_id,
        pc_if_next: label_loop_start,
    });

    program.preassign_label_to_next_insn(label_close);
    program.emit_insn(Insn::Close {
        cursor_id: right_cursor_id,
    });
    program.emit_insn(Insn::Close {
        cursor_id: left_cursor_id,
    });
    Ok(())
}

/// Recursively sets the query_destination of all SelectPlans within a CompoundSelect.
/// This ensures UNION ALL subselects write to the collection index instead of ResultRows.
fn set_compound_plan_destinations(plan: &mut Plan, dest: &QueryDestination) {
    match plan {
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            for (subplan, _) in left.iter_mut() {
                subplan.query_destination = dest.clone();
            }
            right_most.query_destination = dest.clone();
        }
        Plan::Select(select_plan) => {
            select_plan.query_destination = dest.clone();
        }
        _ => {}
    }
}

/// Creates an ephemeral index for collecting all compound select results.
/// Uses `has_rowid=true` to allow duplicate entries (needed for UNION ALL).
fn create_collection_index(
    program: &mut ProgramBuilder,
    left_select: &SelectPlan,
    right_select: &SelectPlan,
) -> crate::Result<(usize, Arc<Index>)> {
    let mut columns = right_select
        .result_columns
        .iter()
        .enumerate()
        .map(|(i, c)| IndexColumn {
            name: c
                .name(&right_select.table_references)
                .map(|n| n.to_string())
                .unwrap_or_default(),
            order: SortOrder::Asc,
            pos_in_table: i,
            default: None,
            collation: None,
            expr: None,
        })
        .collect::<Vec<_>>();
    for (i, column) in columns.iter_mut().enumerate() {
        let left_collation = get_collseq_from_expr(
            &left_select.result_columns[i].expr,
            &left_select.table_references,
        )?;
        let right_collation = get_collseq_from_expr(
            &right_select.result_columns[i].expr,
            &right_select.table_references,
        )?;
        let collation = match (left_collation, right_collation) {
            (None, None) => None,
            (Some(coll), None) | (None, Some(coll)) => Some(coll),
            (Some(coll), Some(_)) => Some(coll),
        };
        column.collation = collation;
    }

    let index = Arc::new(Index {
        columns,
        name: "compound_collection".to_string(),
        root_page: 0,
        ephemeral: true,
        table_name: String::new(),
        unique: false,
        has_rowid: true, // Allow duplicates for UNION ALL
        where_clause: None,
        index_method: None,
        on_conflict: None,
    });
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: false,
    });
    Ok((cursor_id, index))
}

/// Emits bytecode for sorting compound select results and outputting them.
/// Reads from the collection index, inserts into a Sorter with ORDER BY keys,
/// then reads sorted rows and emits to the real destination with LIMIT/OFFSET.
#[allow(clippy::too_many_arguments)]
fn emit_compound_order_by(
    program: &mut ProgramBuilder,
    order_by: &[(usize, SortOrder, Option<turso_parser::ast::NullsOrder>)],
    collection_cursor_id: usize,
    collection_index: &Index,
    num_result_cols: usize,
    limit: Option<&Expr>,
    offset: Option<&Expr>,
    real_destination: &QueryDestination,
    right_most_ctx: &TranslateCtx,
) -> crate::Result<Option<usize>> {
    // Open a Sorter with ORDER BY specifications.
    // Sorter layout: [sort_key_0, sort_key_1, ..., result_col_0, result_col_1, ...]
    // Sort keys that match result columns are deduplicated.

    // Build sort key collations and orders.
    // We append a sequence number as an extra sort key after the ORDER BY columns
    // to break ties by insertion order, matching SQLite's merge-based compound SELECT
    // which naturally outputs left-arm rows before right-arm rows for equal keys.
    let mut order_collations_nulls: Vec<(
        SortOrder,
        Option<crate::translate::collate::CollationSeq>,
        Option<turso_parser::ast::NullsOrder>,
    )> = order_by
        .iter()
        .map(|(col_idx, order, nulls)| {
            let collation = collection_index
                .columns
                .get(*col_idx)
                .and_then(|c| c.collation);
            (*order, collation, *nulls)
        })
        .collect();
    // Sequence tie-breaker: preserves insertion order for rows with equal ORDER BY keys
    order_collations_nulls.push((SortOrder::Asc, None, None));

    // Compute deduplication remappings: which result columns share a sort key slot.
    // The sorter layout is: [order_by_keys..., sequence, non-dedup data cols...]
    let seq_slot = order_by.len();
    let data_start = order_by.len() + 1; // data columns start after ORDER BY keys + sequence
    let mut remappings: Vec<(usize, bool)> = Vec::with_capacity(num_result_cols);
    let mut non_dedup_count = 0;
    for col_idx in 0..num_result_cols {
        if let Some((sort_key_idx, _)) = order_by
            .iter()
            .enumerate()
            .find(|(_, (ob_col, _, _))| *ob_col == col_idx)
        {
            // This result column is also a sort key - deduplicate
            remappings.push((sort_key_idx, true));
        } else {
            remappings.push((data_start + non_dedup_count, false));
            non_dedup_count += 1;
        }
    }
    let sorter_column_count = order_by.len() + 1 + non_dedup_count;

    let sort_cursor = program.alloc_cursor_id(CursorType::Sorter);
    // Resolve custom type comparators for ORDER BY columns (e.g. numeric(10,2) needs
    // NumericLt to sort correctly instead of default blob/text comparison).
    let mut comparators: Vec<Option<crate::vdbe::insn::SortComparatorType>> = order_by
        .iter()
        .map(|(col_idx, _, _)| {
            program.result_columns.get(*col_idx).and_then(|rc| {
                custom_type_comparator(
                    &rc.expr,
                    &program.table_references,
                    right_most_ctx.resolver.schema(),
                )
            })
        })
        .collect();
    // No comparator needed for the sequence tie-breaker column
    comparators.push(None);
    program.emit_insn(Insn::SorterOpen {
        cursor_id: sort_cursor,
        columns: order_collations_nulls.len(),
        order_collations_nulls,
        comparators,
    });

    // Read from collection index and insert into Sorter
    let label_sorter_done = program.allocate_label();
    let label_sorter_loop = program.allocate_label();
    // Allocate registers for result columns + the sequence column from the collection
    let read_regs = program.alloc_registers(num_result_cols + 1);
    let seq_reg = read_regs + num_result_cols;

    program.emit_insn(Insn::Rewind {
        cursor_id: collection_cursor_id,
        pc_if_empty: label_sorter_done,
    });
    program.preassign_label_to_next_insn(label_sorter_loop);

    // Read all result columns from collection index
    for col_idx in 0..num_result_cols {
        program.emit_insn(Insn::Column {
            cursor_id: collection_cursor_id,
            column: col_idx,
            dest: read_regs + col_idx,
            default: None,
        });
    }
    // Read the sequence column (appended after result columns in the collection index)
    program.emit_insn(Insn::Column {
        cursor_id: collection_cursor_id,
        column: num_result_cols,
        dest: seq_reg,
        default: None,
    });

    // Build sorter record: [sort_keys..., sequence, non-dedup result cols...]
    let sorter_regs = program.alloc_registers(sorter_column_count);
    // First emit sort keys
    for (sort_key_idx, (col_idx, _, _)) in order_by.iter().enumerate() {
        program.emit_insn(Insn::Copy {
            src_reg: read_regs + col_idx,
            dst_reg: sorter_regs + sort_key_idx,
            extra_amount: 0,
        });
    }
    // Then emit sequence number for tie-breaking
    program.emit_insn(Insn::Copy {
        src_reg: seq_reg,
        dst_reg: sorter_regs + seq_slot,
        extra_amount: 0,
    });
    // Then emit non-deduplicated result columns
    let mut sorter_data_idx = data_start;
    for (col_idx, &(_sorter_idx, deduplicated)) in
        remappings.iter().enumerate().take(num_result_cols)
    {
        if !deduplicated {
            program.emit_insn(Insn::Copy {
                src_reg: read_regs + col_idx,
                dst_reg: sorter_regs + sorter_data_idx,
                extra_amount: 0,
            });
            sorter_data_idx += 1;
        }
    }

    let reg_sorter_data = program.alloc_register();
    sorter_insert(
        program,
        sorter_regs,
        sorter_column_count,
        sort_cursor,
        reg_sorter_data,
    );

    program.emit_insn(Insn::Next {
        cursor_id: collection_cursor_id,
        pc_if_next: label_sorter_loop,
    });
    program.preassign_label_to_next_insn(label_sorter_done);
    program.emit_insn(Insn::Close {
        cursor_id: collection_cursor_id,
    });

    // Now emit LIMIT/OFFSET for the sorted output
    let limit_ctx = limit
        .map(|limit_expr| {
            let reg = program.alloc_register();
            match limit_expr {
                Expr::Literal(Literal::Numeric(n)) => {
                    if let Ok(value) = n.parse::<i64>() {
                        program.add_comment(program.offset(), "LIMIT counter");
                        program.emit_insn(Insn::Integer { value, dest: reg });
                    } else {
                        let value = n
                            .parse::<f64>()
                            .map_err(|_| LimboError::ParseError("invalid limit".to_string()))?;
                        program.emit_insn(Insn::Real { value, dest: reg });
                        program.add_comment(program.offset(), "LIMIT counter");
                        program.emit_insn(Insn::MustBeInt { reg });
                    }
                }
                _ => {
                    _ = translate_expr(program, None, limit_expr, reg, &right_most_ctx.resolver);
                    program.add_comment(program.offset(), "LIMIT counter");
                    program.emit_insn(Insn::MustBeInt { reg });
                }
            }
            Ok::<_, LimboError>(reg)
        })
        .transpose()?;

    let offset_reg = offset
        .map(|offset_expr| {
            let reg = program.alloc_register();
            match offset_expr {
                Expr::Literal(Literal::Numeric(n)) => {
                    if let Ok(value) = n.parse::<i64>() {
                        program.emit_insn(Insn::Integer { value, dest: reg });
                    } else {
                        let value = n
                            .parse::<f64>()
                            .map_err(|_| LimboError::ParseError("invalid offset".to_string()))?;
                        program.emit_insn(Insn::Real { value, dest: reg });
                    }
                }
                _ => {
                    _ = translate_expr(program, None, offset_expr, reg, &right_most_ctx.resolver);
                }
            }
            program.add_comment(program.offset(), "OFFSET counter");
            program.emit_insn(Insn::MustBeInt { reg });
            if let Some(limit_reg) = limit_ctx {
                let combined_reg = program.alloc_register();
                program.add_comment(program.offset(), "OFFSET + LIMIT");
                program.emit_insn(Insn::OffsetLimit {
                    offset_reg: reg,
                    combined_reg,
                    limit_reg,
                });
            }
            Ok::<_, LimboError>(reg)
        })
        .transpose()?;

    // Sort and emit results
    emit_explain!(program, false, "USE SORTER FOR ORDER BY".to_owned());

    let pseudo_cursor = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: sorter_column_count,
    }));
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo_cursor,
        content_reg: reg_sorter_data,
        num_fields: sorter_column_count,
    });

    let sort_loop_start = program.allocate_label();
    let sort_loop_next = program.allocate_label();
    let sort_loop_end = program.allocate_label();

    // Skip output entirely if LIMIT is 0
    if let Some(limit_reg) = limit_ctx {
        program.emit_insn(Insn::IfNot {
            reg: limit_reg,
            target_pc: sort_loop_end,
            jump_if_null: false,
        });
    }

    program.emit_insn(Insn::SorterSort {
        cursor_id: sort_cursor,
        pc_if_empty: sort_loop_end,
    });

    program.preassign_label_to_next_insn(sort_loop_start);

    // Apply OFFSET
    if let Some(offset_r) = offset_reg {
        program.emit_insn(Insn::IfPos {
            reg: offset_r,
            target_pc: sort_loop_next,
            decrement_by: 1,
        });
    }

    program.emit_insn(Insn::SorterData {
        cursor_id: sort_cursor,
        dest_reg: reg_sorter_data,
        pseudo_cursor,
    });

    // Read result columns from the pseudo cursor, remapping from sorter order to SELECT order
    let result_start_reg = program.alloc_registers(num_result_cols);
    for (col_idx, &(sorter_idx, _deduplicated)) in
        remappings.iter().enumerate().take(num_result_cols)
    {
        program.emit_column_or_rowid(pseudo_cursor, sorter_idx, result_start_reg + col_idx);
    }

    // Emit to real destination
    emit_columns_to_destination(program, real_destination, result_start_reg, num_result_cols)?;

    // Apply LIMIT
    if let Some(limit_reg) = limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_reg,
            target_pc: sort_loop_end,
        });
    }

    program.preassign_label_to_next_insn(sort_loop_next);
    program.emit_insn(Insn::SorterNext {
        cursor_id: sort_cursor,
        pc_if_next: sort_loop_start,
    });
    program.preassign_label_to_next_insn(sort_loop_end);

    Ok(Some(result_start_reg))
}
