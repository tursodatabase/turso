use super::*;

/// Read a single column from a BTreeTable cursor, transparently computing
/// virtual generated columns inline instead of hitting `emit_column`.
/// All bulk column-reading call sites should use this instead of
/// `emit_column_or_rowid` directly.
#[allow(clippy::too_many_arguments)]
pub fn emit_table_column(
    program: &mut ProgramBuilder,
    cursor_id: CursorID,
    table_ref_id: TableInternalId,
    referenced_tables: &TableReferences,
    column: &Column,
    column_index: usize,
    target_register: usize,
    resolver: &Resolver,
) -> Result<()> {
    do_emit_table_column(
        program,
        cursor_id,
        &SelfTableContext::ForSelect {
            table_ref_id,
            referenced_tables: referenced_tables.clone(),
        },
        Some(referenced_tables),
        column,
        column_index,
        target_register,
        resolver,
        None,
    )
}

/// Equivalent of [emit_table_column] for when registers are laid out for DML.
#[allow(clippy::too_many_arguments)]
pub fn emit_table_column_for_dml(
    program: &mut ProgramBuilder,
    cursor_id: CursorID,
    dml_column_context: DmlColumnContext,
    column: &Column,
    column_index: usize,
    target_register: usize,
    resolver: &Resolver,
    table: &Arc<BTreeTable>,
) -> Result<()> {
    do_emit_table_column(
        program,
        cursor_id,
        &SelfTableContext::ForDML {
            dml_ctx: dml_column_context,
            table: Arc::clone(table),
        },
        None,
        column,
        column_index,
        target_register,
        resolver,
        None,
    )
}

#[inline(always)]
#[allow(clippy::too_many_arguments)]
pub(super) fn do_emit_table_column(
    program: &mut ProgramBuilder,
    cursor_id: CursorID,
    self_table_context: &SelfTableContext,
    referenced_tables: Option<&TableReferences>,
    column: &Column,
    column_index: usize,
    target_register: usize,
    resolver: &Resolver,
    null_row_cursor: Option<CursorID>,
) -> Result<()> {
    match column.generated_type() {
        GeneratedType::Virtual { expr, .. } => {
            let end_column_emission = null_row_cursor.map(|null_row_cursor| {
                let end = program.allocate_label();
                program.emit_insn(Insn::IfNullRow {
                    cursor_id: null_row_cursor,
                    target_pc: end,
                    dest: target_register,
                });
                end
            });
            resolver.with_self_table_context(program, Some(self_table_context), |program, _| {
                if expr.is_constant(resolver) {
                    let constant_register = program.alloc_register();
                    translate_expr(
                        program,
                        referenced_tables,
                        expr,
                        constant_register,
                        resolver,
                    )?;
                    program.emit_insn(Insn::Copy {
                        src_reg: constant_register,
                        dst_reg: target_register,
                        extra_amount: 0,
                    });
                } else {
                    translate_expr(program, referenced_tables, expr, target_register, resolver)?;
                }
                Ok(())
            })?;
            program.emit_column_affinity(target_register, column.affinity());
            if let Some(end) = end_column_emission {
                program.preassign_label_to_next_insn(end);
            }
        }
        _ => {
            program.emit_column_or_rowid(cursor_id, column_index, target_register);
        }
    }
    Ok(())
}
