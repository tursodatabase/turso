use crate::schema::{Index, IndexColumn, Schema};
use crate::translate::emitter::{emit_query, LimitCtx, TransactionMode, TranslateCtx};
use crate::translate::plan::{Plan, QueryDestination, SelectPlan};
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::Insn;
use crate::vdbe::BranchOffset;
use crate::SymbolTable;
use limbo_sqlite3_parser::ast::{CompoundOperator, SortOrder};
use std::sync::Arc;
use tracing::instrument;

use tracing::Level;

#[instrument(skip_all, level = Level::TRACE)]
pub fn emit_program_for_compound_select(
    program: &mut ProgramBuilder,
    plan: Plan,
    schema: &Schema,
    syms: &SymbolTable,
) -> crate::Result<()> {
    let Plan::CompoundSelect {
        left: _left,
        right_most,
        limit,
        ..
    } = &plan
    else {
        crate::bail_parse_error!("expected compound select plan");
    };

    let right_plan = right_most.clone();
    // Trivial exit on LIMIT 0
    if let Some(limit) = limit {
        if *limit == 0 {
            program.epilogue(TransactionMode::Read);
            program.result_columns = right_plan.result_columns;
            program.table_references.extend(right_plan.table_references);
            return Ok(());
        }
    }

    // Each subselect shares the same limit_ctx, because the LIMIT applies to the entire compound select,
    // not just a single subselect.
    let limit_ctx = limit.map(|limit| {
        let reg = program.alloc_register();
        program.emit_insn(Insn::Integer {
            value: limit as i64,
            dest: reg,
        });
        LimitCtx::new_shared(reg)
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

    emit_compound_select(
        program,
        plan,
        schema,
        syms,
        limit_ctx,
        yield_reg,
        reg_result_cols_start,
    )?;

    program.epilogue(TransactionMode::Read);
    program.result_columns = right_plan.result_columns;
    program.table_references.extend(right_plan.table_references);

    Ok(())
}

// Emits bytecode for a compound SELECT statement. This function processes the rightmost part of
// the compound SELECT and handles the left parts recursively based on the compound operator type.
fn emit_compound_select(
    program: &mut ProgramBuilder,
    plan: Plan,
    schema: &Schema,
    syms: &SymbolTable,
    limit_ctx: Option<LimitCtx>,
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

    let mut right_most_ctx = TranslateCtx::new(
        program,
        schema,
        syms,
        right_most.table_references.joined_tables().len(),
        right_most.result_columns.len(),
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
                    limit,
                    offset,
                    order_by,
                };
                emit_compound_select(
                    program,
                    compound_select,
                    schema,
                    syms,
                    limit_ctx,
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
                emit_query(program, &mut right_most, &mut right_most_ctx)?;
                program.preassign_label_to_next_insn(label_next_select);
            }
            CompoundOperator::Union => {
                let mut new_dedupe_index = false;
                let dedupe_index = match right_most.query_destination {
                    QueryDestination::EphemeralIndex { cursor_id, index } => {
                        (cursor_id, index.clone())
                    }
                    _ => {
                        new_dedupe_index = true;
                        create_union_dedupe_index(program, &right_most)
                    }
                };
                plan.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: dedupe_index.0,
                    index: dedupe_index.1.clone(),
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
                    yield_reg,
                    reg_result_cols_start,
                )?;

                right_most.query_destination = QueryDestination::EphemeralIndex {
                    cursor_id: dedupe_index.0,
                    index: dedupe_index.1.clone(),
                };
                emit_query(program, &mut right_most, &mut right_most_ctx)?;

                if new_dedupe_index {
                    let label_jump_over_dedupe = program.allocate_label();
                    read_deduplicated_union_rows(
                        program,
                        dedupe_index.0,
                        dedupe_index.1.as_ref(),
                        limit_ctx,
                        label_jump_over_dedupe,
                        yield_reg,
                    );
                    program.preassign_label_to_next_insn(label_jump_over_dedupe);
                }
            }
            _ => {
                crate::bail_parse_error!("unimplemented compound select operator: {:?}", operator);
            }
        },
        None => {
            if let Some(limit_ctx) = limit_ctx {
                right_most_ctx.limit_ctx = Some(limit_ctx);
                right_most.limit = limit;
            }
            emit_query(program, &mut right_most, &mut right_most_ctx)?;
        }
    }

    Ok(())
}

/// Creates an ephemeral index that will be used to deduplicate the results of any sub-selects
/// that appear before the last UNION operator.
fn create_union_dedupe_index(
    program: &mut ProgramBuilder,
    first_select_in_compound: &SelectPlan,
) -> (usize, Arc<Index>) {
    let dedupe_index = Arc::new(Index {
        columns: first_select_in_compound
            .result_columns
            .iter()
            .map(|c| IndexColumn {
                name: c
                    .name(&first_select_in_compound.table_references)
                    .map(|n| n.to_string())
                    .unwrap_or_default(),
                order: SortOrder::Asc,
                pos_in_table: 0,
                default: None,
                collation: None, // FIXME: this should be inferred
            })
            .collect(),
        name: "union_dedupe".to_string(),
        root_page: 0,
        ephemeral: true,
        table_name: String::new(),
        unique: true,
        has_rowid: false,
    });
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(dedupe_index.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id,
        is_table: false,
    });
    (cursor_id, dedupe_index.clone())
}

/// Emits the bytecode for reading deduplicated rows from the ephemeral index created for UNION operators.
fn read_deduplicated_union_rows(
    program: &mut ProgramBuilder,
    dedupe_cursor_id: usize,
    dedupe_index: &Index,
    limit_ctx: Option<LimitCtx>,
    label_limit_reached: BranchOffset,
    yield_reg: Option<usize>,
) {
    let label_dedupe_next = program.allocate_label();
    let label_dedupe_loop_start = program.allocate_label();
    let dedupe_cols_start_reg = program.alloc_registers(dedupe_index.columns.len());
    program.emit_insn(Insn::Rewind {
        cursor_id: dedupe_cursor_id,
        pc_if_empty: label_dedupe_next,
    });
    program.preassign_label_to_next_insn(label_dedupe_loop_start);
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
            target_pc: label_limit_reached,
        })
    }
    program.preassign_label_to_next_insn(label_dedupe_next);
    program.emit_insn(Insn::Next {
        cursor_id: dedupe_cursor_id,
        pc_if_next: label_dedupe_loop_start,
    });
    program.emit_insn(Insn::Close {
        cursor_id: dedupe_cursor_id,
    });
}
