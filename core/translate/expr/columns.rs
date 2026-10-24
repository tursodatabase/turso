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
            uses_custom_type_operators: false,
        },
        None,
        column,
        column_index,
        target_register,
        resolver,
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
) -> Result<()> {
    match column.generated_type() {
        GeneratedType::Virtual { expr, .. } => {
            resolver.with_self_table_context(program, Some(self_table_context), |program, _| {
                translate_expr(program, referenced_tables, expr, target_register, resolver)?;
                Ok(())
            })?;
            program.emit_column_affinity(target_register, column.affinity());
        }
        _ => {
            emit_stored_column(program, cursor_id, column_index, target_register, resolver)?;
            maybe_apply_affinity(column.ty(), target_register, program);
        }
    }
    Ok(())
}

pub fn emit_stored_column(
    program: &mut ProgramBuilder,
    cursor_id: CursorID,
    column_index: usize,
    target_register: usize,
    resolver: &Resolver,
) -> Result<()> {
    let Some(table) = program.btree_table_from_cursor(cursor_id).map(Arc::clone) else {
        program.emit_column_or_rowid(cursor_id, column_index, target_register);
        return Ok(());
    };
    emit_stored_column_from_table(
        program,
        cursor_id,
        &table,
        column_index,
        target_register,
        resolver,
    )
}

pub fn emit_stored_column_from_table(
    program: &mut ProgramBuilder,
    cursor_id: CursorID,
    table: &BTreeTable,
    column_index: usize,
    target_register: usize,
    resolver: &Resolver,
) -> Result<()> {
    let column = &table.columns()[column_index];
    let Some(default_expr) = column.default.as_ref() else {
        program.emit_column_or_rowid(cursor_id, column_index, target_register);
        return Ok(());
    };
    let has_encode = resolver
        .schema()
        .resolve_type(&column.ty_str, table.is_strict)
        .ok()
        .flatten()
        .is_some_and(|resolved| resolved.chain.iter().any(|td| td.encode().is_some()));
    if !has_encode {
        program.emit_column_or_rowid(cursor_id, column_index, target_register);
        return Ok(());
    }
    program.flags.set_suppress_column_default(true);
    program.emit_column_or_rowid(cursor_id, column_index, target_register);
    let has_field_label = program.allocate_label();
    program.emit_column_has_field(cursor_id, column_index, has_field_label);
    translate_expr_no_constant_opt(
        program,
        None,
        default_expr,
        target_register,
        resolver,
        NoConstantOptReason::RegisterReuse,
    )?;
    emit_custom_type_encode_columns(
        program,
        resolver,
        std::slice::from_ref(column),
        target_register,
        None,
        &table.name,
        &ColumnLayout::Identity { column_count: 1 },
    )?;
    program.preassign_label_to_next_insn(has_field_label);
    Ok(())
}
