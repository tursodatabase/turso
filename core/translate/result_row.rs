use crate::turso_assert_eq;
use crate::{
    vdbe::{
        builder::ProgramBuilder,
        insn::{to_u32, IdxInsertFlags, InsertFlags, Insn},
        BranchOffset,
    },
    Result,
};

use turso_parser::ast;

use super::plan::QueryDestination;

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
            affinity_str,
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
                    start_reg: to_u32(record_start),
                    count: to_u32(record_count),
                    dest_reg: to_u32(record_reg),
                    index_name: Some(dedupe_index.name.clone()),
                    affinity_str: affinity_str.as_ref().map(|s| (**s).clone()),
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
                            start_reg: to_u32(start_reg),
                            count: to_u32(1),
                            dest_reg: to_u32(record_reg),
                            index_name: Some(table.name.clone()),
                            affinity_str: None,
                        });
                    } else if num_columns > 1 {
                        program.emit_insn(Insn::MakeRecord {
                            start_reg: to_u32(start_reg),
                            count: to_u32(num_columns - 1),
                            dest_reg: to_u32(record_reg),
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
                            start_reg: to_u32(start_reg),
                            count: to_u32(num_columns),
                            dest_reg: to_u32(record_reg),
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
        QueryDestination::RecursiveCteQueue {
            cursor_id,
            index,
            sort_keys,
            seen_rows,
        } => {
            let skip_seen_row = seen_rows.as_ref().map(|(cursor_id, index)| {
                let skip_seen_row = program.allocate_label();
                // Build the probe record once and reuse it for the insert.
                // The failed Found leaves the cursor at the insertion point
                // (both run the same eq-only GE seek), so the insert can skip
                // its own descent via USE_SEEK, as SQLite does.
                let record_reg = program.alloc_register();
                program.emit_insn(Insn::MakeRecord {
                    start_reg: to_u32(start_reg),
                    count: to_u32(num_columns),
                    dest_reg: to_u32(record_reg),
                    index_name: Some(index.name.clone()),
                    affinity_str: None,
                });
                program.emit_insn(Insn::Found {
                    cursor_id: *cursor_id,
                    target_pc: skip_seen_row,
                    record_reg,
                    num_regs: 0,
                });
                program.emit_insn(Insn::IdxInsert {
                    cursor_id: *cursor_id,
                    record_reg,
                    unpacked_start: Some(start_reg),
                    unpacked_count: Some(to_u32(num_columns)),
                    flags: IdxInsertFlags::new().no_op_duplicate().use_seek(true),
                });
                skip_seen_row
            });

            let sort_key_column_count = sort_keys
                .iter()
                .map(|key| 1 + usize::from(key.nulls_override.is_some()))
                .sum::<usize>();
            let queue_row_start_reg =
                program.alloc_registers(sort_key_column_count + 1 + num_columns);
            let mut queue_key_reg = queue_row_start_reg;
            for key in sort_keys {
                if let Some(nulls) = key.nulls_override {
                    let nulls_last = matches!(nulls, ast::NullsOrder::Last);
                    let null_rank_ready = program.allocate_label();
                    program.emit_insn(Insn::Integer {
                        value: i64::from(nulls_last),
                        dest: queue_key_reg,
                    });
                    program.emit_insn(Insn::IsNull {
                        reg: start_reg + key.result_column_index,
                        target_pc: null_rank_ready,
                    });
                    program.emit_insn(Insn::Integer {
                        value: i64::from(!nulls_last),
                        dest: queue_key_reg,
                    });
                    program.preassign_label_to_next_insn(null_rank_ready);
                    queue_key_reg += 1;
                }
                program.emit_insn(Insn::Copy {
                    src_reg: start_reg + key.result_column_index,
                    dst_reg: queue_key_reg,
                    extra_amount: 0,
                });
                queue_key_reg += 1;
            }
            program.emit_insn(Insn::Sequence {
                cursor_id: *cursor_id,
                target_reg: queue_row_start_reg + sort_key_column_count,
            });
            program.emit_insn(Insn::Copy {
                src_reg: start_reg,
                dst_reg: queue_row_start_reg + sort_key_column_count + 1,
                extra_amount: num_columns - 1,
            });
            let record_reg = program.alloc_register();
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(queue_row_start_reg),
                count: to_u32(sort_key_column_count + 1 + num_columns),
                dest_reg: to_u32(record_reg),
                index_name: Some(index.name.clone()),
                affinity_str: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: *cursor_id,
                record_reg,
                unpacked_start: None,
                unpacked_count: None,
                flags: IdxInsertFlags::new().no_op_duplicate(),
            });

            if let Some(skip_seen_row) = skip_seen_row {
                program.preassign_label_to_next_insn(skip_seen_row);
            }
        }
        QueryDestination::CoroutineYield { yield_reg, .. } => {
            program.emit_insn(Insn::Yield {
                yield_reg: *yield_reg,
                end_offset: BranchOffset::Offset(0),
                subtype_clear_start_reg: start_reg,
                subtype_clear_count: num_columns,
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
            turso_assert_eq!(
                num_columns,
                *num_regs,
                "Row value subqueries should have the same number of result columns as the number of registers"
            );
            program.emit_insn(Insn::Copy {
                src_reg: start_reg,
                dst_reg: *result_reg_start,
                extra_amount: num_regs - 1,
            });
        }
        QueryDestination::RowSet { rowset_reg } => {
            turso_assert_eq!(
                num_columns,
                1,
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
