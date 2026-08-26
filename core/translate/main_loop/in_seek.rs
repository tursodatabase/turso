use super::*;

/// Labels and cursor for the outer loop over one IN source.
#[derive(Debug)]
pub(crate) struct InSeekLoop {
    pub(crate) source_cursor: CursorID,
    pub(crate) outer_loop_start: BranchOffset,
    pub(crate) next_value: BranchOffset,
}

/// Materialize IN values into the unique ephemeral index used by an IN seek.
pub(crate) fn open_in_seek_values_cursor<E>(
    program: &mut ProgramBuilder,
    index: Option<&Arc<Index>>,
    values: &[E],
    affinity: Affinity,
    mut emit_value: impl FnMut(&mut ProgramBuilder, &E, usize) -> Result<()>,
) -> Result<CursorID> {
    let once_done = program.allocate_label();
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: once_done,
    });
    let collation = index
        .as_ref()
        .and_then(|index| index.columns.first())
        .and_then(|column| column.collation);
    let ephemeral_index = Arc::new(Index {
        name: String::new(),
        table_name: String::new(),
        root_page: 0,
        columns: crate::alloc::try_vec![IndexColumn {
            name: String::new(),
            order: SortOrder::Asc,
            nulls_order: None,
            pos_in_table: 0,
            collation,
            default: None,
            expr: None,
        }]?,
        unique: true,
        ephemeral: true,
        has_rowid: false,
        where_clause: None,
        index_method: None,
        on_conflict: None,
    });
    let source_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(ephemeral_index));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: source_cursor,
        is_table: false,
    });
    let value_register = program.alloc_register();
    let record_register = program.alloc_register();
    let affinity = affinity.aff_mask().to_string();
    for value in values {
        emit_value(program, value, value_register)?;
        program.emit_insn(Insn::MakeRecord {
            start_reg: to_u32(value_register),
            count: 1,
            dest_reg: to_u32(record_register),
            index_name: None,
            affinity_str: Some(affinity.clone()),
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: source_cursor,
            record_reg: record_register,
            unpacked_start: Some(value_register),
            unpacked_count: Some(1),
            flags: IdxInsertFlags::new().no_op_duplicate(),
        });
    }
    program.preassign_label_to_next_insn(once_done);
    Ok(source_cursor)
}

/// Open or reuse the ephemeral cursor that supplies RHS values for an IN-seek.
///
/// Literal lists are materialized once into a unique ephemeral index so both
/// ordinary `Search::InSeek` and multi-index OR branches can drive repeated
/// equality seeks from the same bytecode pattern. IN-subqueries already have an
/// ephemeral cursor from subquery translation, so they are reused directly.
pub(super) fn open_in_seek_source_cursor(
    program: &mut ProgramBuilder,
    table_references: &TableReferences,
    resolver: &Resolver<'_>,
    index: Option<&Arc<Index>>,
    source: &InSeekSource,
) -> Result<CursorID> {
    match source {
        InSeekSource::LiteralList { values, affinity } => open_in_seek_values_cursor(
            program,
            index,
            values,
            *affinity,
            |program, value, target| {
                translate_expr_no_constant_opt(
                    program,
                    Some(table_references),
                    value,
                    target,
                    resolver,
                    NoConstantOptReason::InListEphemeral,
                )?;
                Ok(())
            },
        ),
        InSeekSource::Subquery { cursor_id } => Ok(*cursor_id),
    }
}

/// Emit the outer IN-value loop and position the table or index cursor for its
/// first matching row.
pub(crate) fn emit_in_seek_start(
    program: &mut ProgramBuilder,
    source_cursor: CursorID,
    scan_cursor: CursorID,
    table_cursor: Option<CursorID>,
    index_backed: bool,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
) -> InSeekLoop {
    program.emit_insn(Insn::NullRow {
        cursor_id: source_cursor,
    });
    program.emit_insn(Insn::Rewind {
        cursor_id: source_cursor,
        pc_if_empty: loop_end,
    });

    let outer_loop_start = program.allocate_label();
    program.preassign_label_to_next_insn(outer_loop_start);
    let seek_register = program.alloc_register();
    program.emit_insn(Insn::Column {
        cursor_id: source_cursor,
        column: 0,
        dest: seek_register,
        default: None,
    });

    let next_value = program.allocate_label();
    program.emit_insn(Insn::IsNull {
        reg: seek_register,
        target_pc: next_value,
    });

    if index_backed {
        program.emit_insn(Insn::SeekGE {
            cursor_id: scan_cursor,
            start_reg: seek_register,
            num_regs: 1,
            target_pc: next_value,
            is_index: true,
            eq_only: false,
            null_matching_mask: Default::default(),
        });
        program.preassign_label_to_next_insn(loop_start);
        program.emit_insn(Insn::IdxGT {
            cursor_id: scan_cursor,
            start_reg: seek_register,
            num_regs: 1,
            target_pc: next_value,
        });
        if let Some(table_cursor) = table_cursor {
            program.emit_insn(Insn::DeferredSeek {
                index_cursor_id: scan_cursor,
                table_cursor_id: table_cursor,
            });
        }
    } else {
        assert!(
            table_cursor.is_none(),
            "rowid IN seek cannot have a second table cursor"
        );
        program.emit_insn(Insn::SeekRowid {
            cursor_id: scan_cursor,
            src_reg: seek_register,
            target_pc: next_value,
        });
    }

    InSeekLoop {
        source_cursor,
        outer_loop_start,
        next_value,
    }
}

/// Finish one IN value, scan duplicate index keys when needed, then advance to
/// the next value.
pub(crate) fn emit_in_seek_advance(
    program: &mut ProgramBuilder,
    state: &InSeekLoop,
    scan_cursor: CursorID,
    index_backed: bool,
    loop_start: BranchOffset,
) {
    if index_backed {
        program.emit_insn(Insn::Next {
            cursor_id: scan_cursor,
            pc_if_next: loop_start,
            fullscan: false,
        });
    }
    program.preassign_label_to_next_insn(state.next_value);
    program.emit_insn(Insn::Next {
        cursor_id: state.source_cursor,
        pc_if_next: state.outer_loop_start,
        fullscan: false,
    });
}
