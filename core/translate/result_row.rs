use crate::{
    vdbe::{
        builder::ProgramBuilder,
        insn::{to_u16, IdxInsertFlags, InsertFlags, Insn},
        BranchOffset,
    },
    Result,
};

use super::{
    emitter::{LimitCtx, Resolver},
    expr::{translate_expr, translate_expr_no_constant_opt, NoConstantOptReason},
    plan::{Distinctness, QueryDestination, SelectPlan},
};

/// Emits the bytecode for:
/// - all result columns
/// - result row (or if a subquery, yields to the parent query)
/// - limit
#[allow(clippy::too_many_arguments)]
pub fn emit_select_result(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    plan: &SelectPlan,
    label_on_limit_reached: Option<BranchOffset>,
    offset_jump_to: Option<BranchOffset>,
    reg_nonagg_emit_once_flag: Option<usize>,
    reg_offset: Option<usize>,
    reg_result_cols_start: usize,
    limit_ctx: Option<LimitCtx>,
) -> Result<()> {
    let has_distinct = matches!(plan.distinctness, Distinctness::Distinct { .. });
    if !has_distinct {
        if let (Some(jump_to), Some(_)) = (offset_jump_to, label_on_limit_reached) {
            emit_offset(program, jump_to, reg_offset);
        }
    }

    let start_reg = reg_result_cols_start;

    // For EXISTS subqueries, we usually only need to determine whether any row exists, not
    // the row's column values. The result is simply writing `1` to the result register.
    //
    // Important: SELECT DISTINCT deduplication reads the result registers as its key. So for
    // EXISTS(SELECT DISTINCT ...), we must still evaluate the result expressions; otherwise the
    // dedup key is uninitialized and EXISTS can incorrectly evaluate to false.
    let skip_column_eval = matches!(
        plan.query_destination,
        QueryDestination::ExistsSubqueryResult { .. }
    ) && !matches!(plan.distinctness, Distinctness::Distinct { .. });

    // For compound selects (UNION, UNION ALL, etc.), multiple subselects may share the same
    // result column registers. If constants are moved to the init section, they can be
    // overwritten by subsequent subselects before being used.
    //
    // We conservatively disable constant optimization for EphemeralIndex, CoroutineYield,
    // and EphemeralTable destinations because these are used in compound select contexts
    // and CTE materialization. This is slightly over-broad (e.g., simple INSERT INTO ...
    // SELECT with no UNION doesn't need this), but we lack context here to distinguish
    // compound vs non-compound cases.
    let disable_constant_opt = matches!(
        plan.query_destination,
        QueryDestination::EphemeralIndex { .. }
            | QueryDestination::CoroutineYield { .. }
            | QueryDestination::EphemeralTable { .. }
    );

    if !skip_column_eval {
        for (i, rc) in plan.result_columns.iter().enumerate().filter(|(_, rc)| {
            // For aggregate queries, we handle columns differently; example: select id, first_name, sum(age) from users limit 1;
            // 1. Columns with aggregates (e.g., sum(age)) are computed in each iteration of aggregation
            // 2. Non-aggregate columns (e.g., id, first_name) are only computed once in the first iteration
            // This filter ensures we only emit expressions for non aggregate columns once,
            // preserving previously calculated values while updating aggregate results
            // For all other queries where reg_nonagg_emit_once_flag is none we do nothing.
            reg_nonagg_emit_once_flag.is_some() && rc.contains_aggregates
                || reg_nonagg_emit_once_flag.is_none()
        }) {
            let reg = start_reg + i;
            if disable_constant_opt {
                translate_expr_no_constant_opt(
                    program,
                    Some(&plan.table_references),
                    &rc.expr,
                    reg,
                    resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
            } else {
                translate_expr(
                    program,
                    Some(&plan.table_references),
                    &rc.expr,
                    reg,
                    resolver,
                )?;
            }
        }
    }

    // Handle SELECT DISTINCT deduplication
    if let Distinctness::Distinct { ctx } = &plan.distinctness {
        let distinct_ctx = ctx.as_ref().expect("distinct context must exist");
        let num_regs = plan.result_columns.len();
        distinct_ctx.emit_deduplication_insns(program, num_regs, start_reg);
    }

    if has_distinct {
        if let (Some(jump_to), Some(_)) = (offset_jump_to, label_on_limit_reached) {
            emit_offset(program, jump_to, reg_offset);
        }
    }

    emit_result_row_and_limit(program, plan, start_reg, limit_ctx, label_on_limit_reached)?;
    Ok(())
}

/// Emits bytecode to send column values to a destination.
/// This is the core "emit to destination" logic shared by both regular SELECT emission
/// and compound SELECT (UNION/INTERSECT/EXCEPT) final output.
///
/// Parameters:
/// - `start_reg`: First register containing column values
/// - `num_columns`: Number of columns to emit
/// - `destination`: Where to send the columns (ResultRow, EphemeralIndex, etc.)
pub fn emit_columns_to_destination(
    program: &mut ProgramBuilder,
    destination: &QueryDestination,
    start_reg: usize,
    num_columns: usize,
) -> Result<()> {
    match destination {
        QueryDestination::ResultRows => {
            program.emit_insn(Insn::ResultRow {
                start_reg,
                count: num_columns,
            });
        }
        QueryDestination::EphemeralIndex {
            cursor_id: index_cursor_id,
            index: dedupe_index,
            is_delete,
        } => {
            if *is_delete {
                program.emit_insn(Insn::IdxDelete {
                    start_reg,
                    num_regs: num_columns,
                    cursor_id: *index_cursor_id,
                    raise_error_if_no_matching_entry: false,
                });
            } else {
                let record_reg = program.alloc_register();

                // For ephemeral indexes, we may need to:
                // 1. Reorder columns if index key order differs from result order (seek indexes)
                // 2. Append a unique rowid to allow duplicate key values (has_rowid indexes)
                let (record_start, record_count) =
                    if dedupe_index.ephemeral && dedupe_index.columns.len() == num_columns {
                        // Check if reordering is needed (any pos_in_table != idx_pos)
                        let needs_reorder = dedupe_index
                            .columns
                            .iter()
                            .enumerate()
                            .any(|(idx_pos, col)| col.pos_in_table != idx_pos);

                        if needs_reorder {
                            // Reorder columns to match index key order
                            let extra_for_rowid = if dedupe_index.has_rowid { 1 } else { 0 };
                            let reordered_start =
                                program.alloc_registers(num_columns + extra_for_rowid);

                            for (idx_pos, idx_col) in dedupe_index.columns.iter().enumerate() {
                                program.emit_insn(Insn::Copy {
                                    src_reg: start_reg + idx_col.pos_in_table,
                                    dst_reg: reordered_start + idx_pos,
                                    extra_amount: 0,
                                });
                            }

                            if dedupe_index.has_rowid {
                                program.emit_insn(Insn::Sequence {
                                    cursor_id: *index_cursor_id,
                                    target_reg: reordered_start + num_columns,
                                });
                            }

                            (reordered_start, num_columns + extra_for_rowid)
                        } else if dedupe_index.has_rowid {
                            // No reordering needed, but need to append rowid
                            let new_start = program.alloc_registers(num_columns + 1);
                            program.emit_insn(Insn::Copy {
                                src_reg: start_reg,
                                dst_reg: new_start,
                                extra_amount: num_columns - 1,
                            });
                            program.emit_insn(Insn::Sequence {
                                cursor_id: *index_cursor_id,
                                target_reg: new_start + num_columns,
                            });
                            (new_start, num_columns + 1)
                        } else {
                            // No reordering or rowid needed - use registers directly
                            (start_reg, num_columns)
                        }
                    } else {
                        (start_reg, num_columns)
                    };

                program.emit_insn(Insn::MakeRecord {
                    start_reg: to_u16(record_start),
                    count: to_u16(record_count),
                    dest_reg: to_u16(record_reg),
                    index_name: Some(dedupe_index.name.clone()),
                    affinity_str: None,
                });
                program.emit_insn(Insn::IdxInsert {
                    cursor_id: *index_cursor_id,
                    record_reg,
                    unpacked_start: None,
                    unpacked_count: None,
                    flags: IdxInsertFlags::new().no_op_duplicate(),
                });
            }
        }
        QueryDestination::EphemeralTable {
            cursor_id: table_cursor_id,
            table,
            rowid_mode,
        } => {
            // Prevent constant hoisting so that each row's constants are evaluated inline.
            // This is critical for UNION ALL where each arm should insert its own values.
            program.constant_span_end_all();
            let record_reg = program.alloc_register();
            match rowid_mode {
                super::plan::EphemeralRowidMode::FromResultColumns => {
                    // For single-column case (RowidOnly materialization), we still need to
                    // create a record containing the rowid so it can be read back later.
                    if num_columns == 1 {
                        program.emit_insn(Insn::MakeRecord {
                            start_reg: to_u16(start_reg),
                            count: to_u16(1),
                            dest_reg: to_u16(record_reg),
                            index_name: Some(table.name.clone()),
                            affinity_str: None,
                        });
                    } else if num_columns > 1 {
                        program.emit_insn(Insn::MakeRecord {
                            start_reg: to_u16(start_reg),
                            count: to_u16(num_columns - 1),
                            dest_reg: to_u16(record_reg),
                            index_name: Some(table.name.clone()),
                            affinity_str: None,
                        });
                    }
                    program.emit_insn(Insn::Insert {
                        cursor: *table_cursor_id,
                        key_reg: start_reg + (num_columns - 1),
                        record_reg,
                        flag: InsertFlags::new()
                            .require_seek()
                            .is_ephemeral_table_insert(),
                        table_name: table.name.clone(),
                    });
                }
                super::plan::EphemeralRowidMode::Auto => {
                    if num_columns > 0 {
                        program.emit_insn(Insn::MakeRecord {
                            start_reg: to_u16(start_reg),
                            count: to_u16(num_columns),
                            dest_reg: to_u16(record_reg),
                            index_name: Some(table.name.clone()),
                            affinity_str: None,
                        });
                    }
                    let rowid_reg = program.alloc_register();
                    program.emit_insn(Insn::NewRowid {
                        cursor: *table_cursor_id,
                        rowid_reg,
                        prev_largest_reg: 0,
                    });
                    program.emit_insn(Insn::Insert {
                        cursor: *table_cursor_id,
                        key_reg: rowid_reg,
                        record_reg,
                        flag: InsertFlags::new().is_ephemeral_table_insert(),
                        table_name: table.name.clone(),
                    });
                }
            }
        }
        QueryDestination::CoroutineYield { yield_reg, .. } => {
            program.emit_insn(Insn::Yield {
                yield_reg: *yield_reg,
                end_offset: BranchOffset::Offset(0),
            });
        }
        QueryDestination::ExistsSubqueryResult { result_reg } => {
            program.emit_insn(Insn::Integer {
                value: 1,
                dest: *result_reg,
            });
        }
        QueryDestination::RowValueSubqueryResult {
            result_reg_start,
            num_regs,
        } => {
            assert!(
                num_columns == *num_regs,
                "Row value subqueries should have the same number of result columns as the number of registers"
            );
            program.emit_insn(Insn::Copy {
                src_reg: start_reg,
                dst_reg: *result_reg_start,
                extra_amount: num_regs - 1,
            });
        }
        QueryDestination::RowSet { rowset_reg } => {
            assert_eq!(
                num_columns, 1,
                "RowSet should only have one result column (rowid)"
            );
            program.emit_insn(Insn::RowSetAdd {
                rowset_reg: *rowset_reg,
                value_reg: start_reg,
            });
        }
        QueryDestination::Unset => unreachable!("Unset query destination should not be reached"),
    }
    Ok(())
}

/// Emits the bytecode for:
/// - result row (or if a subquery, yields to the parent query)
/// - limit
pub fn emit_result_row_and_limit(
    program: &mut ProgramBuilder,
    plan: &SelectPlan,
    result_columns_start_reg: usize,
    limit_ctx: Option<LimitCtx>,
    label_on_limit_reached: Option<BranchOffset>,
) -> Result<()> {
    emit_columns_to_destination(
        program,
        &plan.query_destination,
        result_columns_start_reg,
        plan.result_columns.len(),
    )?;

    if plan.limit.is_some() {
        if label_on_limit_reached.is_none() {
            return Ok(());
        }
        let limit_ctx = limit_ctx.expect("limit_ctx must be Some if plan.limit is Some");

        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: label_on_limit_reached.unwrap(),
        });
    }
    Ok(())
}

pub fn emit_offset(program: &mut ProgramBuilder, jump_to: BranchOffset, reg_offset: Option<usize>) {
    let Some(reg_offset) = &reg_offset else {
        return;
    };
    program.emit_insn(Insn::IfPos {
        reg: *reg_offset,
        target_pc: jump_to,
        decrement_by: 1,
    });
}
