use crate::schema::{Index, IndexColumn, Schema};
use crate::translate::emitter::{emit_query, LimitCtx, Resolver, TranslateCtx};
use crate::translate::expr::translate_expr;
use crate::translate::plan::{Plan, QueryDestination, SelectPlan};
use crate::translate::result_row::try_fold_expr_to_i64;
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::Insn;
use crate::vdbe::BranchOffset;
use crate::{emit_explain, QueryMode, SymbolTable};
use std::sync::Arc;
use tracing::instrument;
use turso_parser::ast::{CompoundOperator, SortOrder};

use tracing::Level;

#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_program_for_compound_select(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: Plan,
) -> crate::Result<()> {
    let Plan::CompoundSelect {
        left: _left,
        right_most,
        limit,
        offset,
        ..
    } = &plan
    else {
        crate::bail_parse_error!("expected compound select plan");
    };

    let right_plan = right_most.clone();
    let right_most_ctx = TranslateCtx::new(
        program,
        resolver.schema,
        resolver.symbol_table,
        right_most.table_references.joined_tables().len(),
    );

    // Each subselect shares the same limit_ctx and offset, because the LIMIT, OFFSET applies to
    // the entire compound select, not just a single subselect.
    let limit_ctx = limit.as_ref().map(|limit| {
        let reg = program.alloc_register();
        if let Some(val) = try_fold_expr_to_i64(limit) {
            program.emit_insn(Insn::Integer {
                value: val,
                dest: reg,
            });
        } else {
            program.add_comment(program.offset(), "OFFSET expr");
            _ = translate_expr(program, None, limit, reg, &right_most_ctx.resolver);
            program.emit_insn(Insn::MustBeInt { reg });
        }
        LimitCtx::new_shared(reg)
    });
    let offset_reg = offset.as_ref().map(|offset_expr| {
        let reg = program.alloc_register();

        if let Some(val) = try_fold_expr_to_i64(offset_expr) {
            // Compile-time constant offset
            program.emit_insn(Insn::Integer {
                value: val,
                dest: reg,
            });
        } else {
            program.add_comment(program.offset(), "OFFSET expr");
            _ = translate_expr(program, None, offset_expr, reg, &right_most_ctx.resolver);
            program.emit_insn(Insn::MustBeInt { reg });
        }

        let combined_reg = program.alloc_register();
        program.emit_insn(Insn::OffsetLimit {
            offset_reg: reg,
            combined_reg,
            limit_reg: limit_ctx.as_ref().unwrap().reg_limit,
        });

        reg
    });

    // When a compound SELECT is part of a query that yields results to a coroutine (e.g. within an INSERT clause),
    // we must allocate registers for the result columns to be yielded. Each subselect will then yield to
    // the coroutine using the same set of registers.
    let (yield_reg, reg_result_cols_start) = match right_most.query_destination {
        QueryDestination::CoroutineYield { yield_reg, .. } => {
            let start_reg = program.alloc_registers(right_most.result_columns.len());
            (Some(yield_reg), Some(start_reg))
        }
        _ => (None, None),
    };

    emit_explain!(program, true, "COMPOUND QUERY".to_owned());
    emit_compound_select(
        program,
        plan,
        right_most_ctx.resolver.schema,
        right_most_ctx.resolver.symbol_table,
        limit_ctx,
        offset_reg,
        yield_reg,
        reg_result_cols_start,
    )?;
    program.pop_current_parent_explain();

    program.result_columns = right_plan.result_columns;
    program.table_references.extend(right_plan.table_references);

    Ok(())
}

// Emits bytecode for a compound SELECT statement. This function processes the rightmost part of
// the compound SELECT and handles the left parts recursively based on the compound operator type.
#[allow(clippy::too_many_arguments)]
fn emit_compound_select(
    program: &mut ProgramBuilder,
    plan: Plan,
    schema: &Schema,
    syms: &SymbolTable,
    limit_ctx: Option<LimitCtx>,
    offset_reg: Option<usize>,
    yield_reg: Option<usize>,
    reg_result_cols_start: Option<usize>,
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
    let mut right_most_ctx = TranslateCtx::new(
        program,
        schema,
        syms,
        right_most.table_references.joined_tables().len(),
    );
    right_most_ctx.reg_result_cols_start = reg_result_cols_start;
    match left.pop() {
        Some((mut plan, operator)) => match operator {
            CompoundOperator::UnionAll => {
                if matches!(
                    right_most.query_destination,
                    QueryDestination::EphemeralIndex { .. }
                ) {
                    plan.query_destination = right_most.query_destination.clone();
                }
                let compound_select = Plan::CompoundSelect {
                    left,
                    right_most: plan,
                    limit: limit.clone(),
                    offset: offset.clone(),
                    order_by,
                };
                emit_compound_select(
                    program,
                    compound_select,
                    schema,
                    syms,
                    limit_ctx,
                    offset_reg,
                    yield_reg,
                    reg_result_cols_start,
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
                let dedupe_index = match right_most.query_destination {
                    QueryDestination::EphemeralIndex {
                        cursor_id, index, ..
                    } => (cursor_id, index.clone()),
                    _ => {
                        new_dedupe_index = true;
                        create_dedupe_index(program, &right_most, schema)?
                    }
                };
                plan.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: dedupe_index.0,
                    index: dedupe_index.1.clone(),
                    is_delete: false,
                };
                let compound_select = Plan::CompoundSelect {
                    left,
                    right_most: plan,
                    limit,
                    offset,
                    order_by,
                };
                emit_compound_select(
                    program,
                    compound_select,
                    schema,
                    syms,
                    None,
                    None,
                    yield_reg,
                    reg_result_cols_start,
                )?;

                right_most.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: dedupe_index.0,
                    index: dedupe_index.1.clone(),
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
                        yield_reg,
                    );
                }
            }
            CompoundOperator::Intersect => {
                let mut target_cursor_id = None;
                if let QueryDestination::EphemeralIndex { cursor_id, .. } =
                    right_most.query_destination
                {
                    target_cursor_id = Some(cursor_id);
                }

                let (left_cursor_id, left_index) =
                    create_dedupe_index(program, &right_most, schema)?;
                plan.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: left_cursor_id,
                    index: left_index.clone(),
                    is_delete: false,
                };
                let compound_select = Plan::CompoundSelect {
                    left,
                    right_most: plan,
                    limit,
                    offset,
                    order_by,
                };
                emit_compound_select(
                    program,
                    compound_select,
                    schema,
                    syms,
                    None,
                    None,
                    yield_reg,
                    reg_result_cols_start,
                )?;

                let (right_cursor_id, right_index) =
                    create_dedupe_index(program, &right_most, schema)?;
                right_most.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: right_cursor_id,
                    index: right_index,
                    is_delete: false,
                };
                emit_explain!(program, true, "INTERSECT USING TEMP B-TREE".to_owned());
                emit_query(program, &mut right_most, &mut right_most_ctx)?;
                program.pop_current_parent_explain();
                read_intersect_rows(
                    program,
                    left_cursor_id,
                    &left_index,
                    right_cursor_id,
                    target_cursor_id,
                    limit_ctx,
                    offset_reg,
                    yield_reg,
                );
            }
            CompoundOperator::Except => {
                let mut new_index = false;
                let (cursor_id, index) = match right_most.query_destination {
                    QueryDestination::EphemeralIndex {
                        cursor_id, index, ..
                    } => (cursor_id, index),
                    _ => {
                        new_index = true;
                        create_dedupe_index(program, &right_most, schema)?
                    }
                };
                plan.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id,
                    index: index.clone(),
                    is_delete: false,
                };
                let compound_select = Plan::CompoundSelect {
                    left,
                    right_most: plan,
                    limit,
                    offset,
                    order_by,
                };
                emit_compound_select(
                    program,
                    compound_select,
                    schema,
                    syms,
                    None,
                    None,
                    yield_reg,
                    reg_result_cols_start,
                )?;
                right_most.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id,
                    index: index.clone(),
                    is_delete: true,
                };
                emit_explain!(program, true, "EXCEPT USING TEMP B-TREE".to_owned());
                emit_query(program, &mut right_most, &mut right_most_ctx)?;
                program.pop_current_parent_explain();
                if new_index {
                    read_deduplicated_union_or_except_rows(
                        program, cursor_id, &index, limit_ctx, offset_reg, yield_reg,
                    );
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
    select: &SelectPlan,
    schema: &Schema,
) -> crate::Result<(usize, Arc<Index>)> {
    if !schema.indexes_enabled {
        crate::bail_parse_error!("UNION OR INTERSECT or EXCEPT is not supported without indexes");
    }

    let dedupe_index = Arc::new(Index {
        columns: select
            .result_columns
            .iter()
            .map(|c| IndexColumn {
                name: c
                    .name(&select.table_references)
                    .map(|n| n.to_string())
                    .unwrap_or_default(),
                order: SortOrder::Asc,
                pos_in_table: 0,
                default: None,
                collation: None, // FIXME: this should be inferred
            })
            .collect(),
        name: "compound_dedupe".to_string(),
        root_page: 0,
        ephemeral: true,
        table_name: String::new(),
        unique: false,
        has_rowid: false,
        where_clause: None,
    });
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(dedupe_index.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: false,
    });
    Ok((cursor_id, dedupe_index.clone()))
}

/// Emits the bytecode for reading deduplicated rows from the ephemeral index created for
/// UNION or EXCEPT operators.
fn read_deduplicated_union_or_except_rows(
    program: &mut ProgramBuilder,
    dedupe_cursor_id: usize,
    dedupe_index: &Index,
    limit_ctx: Option<LimitCtx>,
    offset_reg: Option<usize>,
    yield_reg: Option<usize>,
) {
    let label_close = program.allocate_label();
    let label_dedupe_next = program.allocate_label();
    let label_dedupe_loop_start = program.allocate_label();
    let dedupe_cols_start_reg = program.alloc_registers(dedupe_index.columns.len());
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
        let start_reg = if let Some(yield_reg) = yield_reg {
            // Need to reuse the yield_reg for the column being emitted
            yield_reg + 1
        } else {
            dedupe_cols_start_reg
        };
        program.emit_insn(Insn::Column {
            cursor_id: dedupe_cursor_id,
            column: col_idx,
            dest: start_reg + col_idx,
            default: None,
        });
    }
    if let Some(yield_reg) = yield_reg {
        program.emit_insn(Insn::Yield {
            yield_reg,
            end_offset: BranchOffset::Offset(0),
        });
    } else {
        program.emit_insn(Insn::ResultRow {
            start_reg: dedupe_cols_start_reg,
            count: dedupe_index.columns.len(),
        });
    }

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
}

// Emits the bytecode for Reading rows from the intersection of two cursors.
#[allow(clippy::too_many_arguments)]
fn read_intersect_rows(
    program: &mut ProgramBuilder,
    left_cursor_id: usize,
    index: &Index,
    right_cursor_id: usize,
    target_cursor: Option<usize>,
    limit_ctx: Option<LimitCtx>,
    offset_reg: Option<usize>,
    yield_reg: Option<usize>,
) {
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
    let cols_start_reg = if let Some(yield_reg) = yield_reg {
        yield_reg + 1
    } else {
        program.alloc_registers(column_count)
    };
    for i in 0..column_count {
        program.emit_insn(Insn::Column {
            cursor_id: left_cursor_id,
            column: i,
            dest: cols_start_reg + i,
            default: None,
        });
    }
    if let Some(target_cursor_id) = target_cursor {
        program.emit_insn(Insn::MakeRecord {
            start_reg: cols_start_reg,
            count: column_count,
            dest_reg: row_content_reg,
            index_name: None,
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: target_cursor_id,
            record_reg: row_content_reg,
            unpacked_start: Some(cols_start_reg),
            unpacked_count: Some(column_count as u16),
            flags: Default::default(),
        });
    } else if let Some(yield_reg) = yield_reg {
        program.emit_insn(Insn::Yield {
            yield_reg,
            end_offset: BranchOffset::Offset(0),
        })
    } else {
        program.emit_insn(Insn::ResultRow {
            start_reg: cols_start_reg,
            count: column_count,
        });
    }
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
}
