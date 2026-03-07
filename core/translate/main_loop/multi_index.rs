use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn emit_multi_index_scan_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table: &JoinedTable,
    table_references: &TableReferences,
    multi_idx_op: &MultiIndexScanOp,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
    _next: BranchOffset,
) -> Result<()> {
    let table_cursor_id = program.resolve_cursor_id(&CursorKey::table(table.internal_id));
    let rowid_reg = program.alloc_register();
    let is_intersection = multi_idx_op.set_op == SetOperation::Intersection;
    let rowset1_reg = program.alloc_register();
    let rowset2_reg = if is_intersection {
        program.alloc_register()
    } else {
        rowset1_reg
    };

    program.emit_insn(Insn::Null {
        dest: rowset1_reg,
        dest_end: None,
    });
    if is_intersection {
        program.emit_insn(Insn::Null {
            dest: rowset2_reg,
            dest_end: None,
        });
    }

    let mut current_read_rowset = rowset1_reg;
    let mut current_write_rowset = if is_intersection {
        rowset2_reg
    } else {
        rowset1_reg
    };

    for (branch_idx, branch) in multi_idx_op.branches.iter().enumerate() {
        let branch_loop_start = program.allocate_label();
        let branch_loop_end = program.allocate_label();
        let branch_next = program.allocate_label();
        let found_in_prev_label = if is_intersection && branch_idx > 0 {
            Some(program.allocate_label())
        } else {
            None
        };

        let seek_def = &branch.seek_def;
        let is_index = branch.index.is_some();
        let branch_cursor_id = if let Some(index) = &branch.index {
            program.resolve_cursor_id(&CursorKey::index(table.internal_id, index.clone()))
        } else {
            table_cursor_id
        };

        let max_key_regs = seek_def
            .size(&seek_def.start)
            .max(seek_def.size(&seek_def.end))
            .max(1);
        let key_start_reg = program.alloc_registers(max_key_regs);
        emit_seek(
            program,
            table_references,
            seek_def,
            t_ctx,
            branch_cursor_id,
            key_start_reg,
            branch_loop_end,
            branch.index.as_ref(),
            false,
        )?;
        emit_seek_termination(
            program,
            table_references,
            seek_def,
            t_ctx,
            branch_cursor_id,
            key_start_reg,
            branch_loop_start,
            branch_loop_end,
            branch.index.as_ref(),
        )?;

        if is_index {
            program.emit_insn(Insn::IdxRowId {
                cursor_id: branch_cursor_id,
                dest: rowid_reg,
            });
        } else {
            program.emit_insn(Insn::RowId {
                cursor_id: branch_cursor_id,
                dest: rowid_reg,
            });
        }

        if is_intersection {
            if branch_idx == 0 {
                program.emit_insn(Insn::RowSetAdd {
                    rowset_reg: rowset1_reg,
                    value_reg: rowid_reg,
                });
            } else {
                program.emit_insn(Insn::RowSetTest {
                    rowset_reg: current_read_rowset,
                    pc_if_found: found_in_prev_label.unwrap(),
                    value_reg: rowid_reg,
                    batch: -1,
                });
                program.emit_insn(Insn::Goto {
                    target_pc: branch_next,
                });
                program.preassign_label_to_next_insn(found_in_prev_label.unwrap());
                program.emit_insn(Insn::RowSetAdd {
                    rowset_reg: current_write_rowset,
                    value_reg: rowid_reg,
                });
            }
        } else {
            program.emit_insn(Insn::RowSetAdd {
                rowset_reg: rowset1_reg,
                value_reg: rowid_reg,
            });
        }

        program.preassign_label_to_next_insn(branch_next);
        match seek_def.iter_dir {
            IterationDirection::Forwards => program.emit_insn(Insn::Next {
                cursor_id: branch_cursor_id,
                pc_if_next: branch_loop_start,
            }),
            IterationDirection::Backwards => program.emit_insn(Insn::Prev {
                cursor_id: branch_cursor_id,
                pc_if_prev: branch_loop_start,
            }),
        }
        program.preassign_label_to_next_insn(branch_loop_end);

        if is_intersection && branch_idx > 0 && branch_idx < multi_idx_op.branches.len() - 1 {
            std::mem::swap(&mut current_read_rowset, &mut current_write_rowset);
            program.emit_insn(Insn::Null {
                dest: current_write_rowset,
                dest_end: None,
            });
        }
    }

    let final_rowset = if is_intersection && multi_idx_op.branches.len() > 1 {
        let num_swaps = multi_idx_op.branches.len().saturating_sub(2);
        if num_swaps % 2 == 0 {
            rowset2_reg
        } else {
            rowset1_reg
        }
    } else {
        rowset1_reg
    };

    program.preassign_label_to_next_insn(loop_start);
    program.emit_insn(Insn::RowSetRead {
        rowset_reg: final_rowset,
        pc_if_empty: loop_end,
        dest_reg: rowid_reg,
    });

    let skip_label = program.allocate_label();
    program.emit_insn(Insn::SeekRowid {
        cursor_id: table_cursor_id,
        src_reg: rowid_reg,
        target_pc: skip_label,
    });

    let rowid_expr = Expr::RowId {
        database: None,
        table: table.internal_id,
    };
    t_ctx
        .resolver
        .expr_to_reg_cache
        .push((Cow::Owned(rowid_expr), rowid_reg, false));

    program.preassign_label_to_next_insn(skip_label);
    Ok(())
}
