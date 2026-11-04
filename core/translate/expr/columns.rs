use super::*;
use crate::translate::emitter::gencol::emit_virtual_column_store;

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
            let end_column_emission = match self_table_context {
                SelfTableContext::ForSelect { .. } => {
                    let end = program.allocate_label();
                    program.emit_insn(Insn::IfNullRow {
                        cursor_id,
                        target_pc: end,
                        dest: target_register,
                    });
                    Some(end)
                }
                SelfTableContext::ForDML { .. } => None,
            };
            resolver.with_self_table_context(program, Some(self_table_context), |program, _| {
                if end_column_emission.is_some() {
                    translate_expr_no_constant_opt(
                        program,
                        referenced_tables,
                        expr,
                        target_register,
                        resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                } else {
                    translate_expr(program, referenced_tables, expr, target_register, resolver)?;
                }
                Ok(())
            })?;
            let table = match self_table_context {
                SelfTableContext::ForSelect {
                    table_ref_id,
                    referenced_tables,
                } => referenced_tables
                    .find_table_by_internal_id(*table_ref_id)
                    .and_then(|(_, table)| table.btree())
                    .expect("a virtual column read in a query belongs to a btree table in it"),
                SelfTableContext::ForDML { table, .. } => Arc::clone(table),
            };
            emit_virtual_column_store(program, resolver, column, target_register, &table)?;
            program.emit_column_affinity(target_register, column.affinity());
            if let Some(end) = end_column_emission {
                program.preassign_label_to_next_insn(end);
            }
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
    let table = program.btree_table_from_cursor(cursor_id).map(Arc::clone);
    // A row written before ADD COLUMN reads the column's default, which must be
    // turned into what the column stores: an array is stored as a record blob,
    // and a custom type stores what its ENCODE and COERCE return.
    let mut encoded_default = None;
    if let Some(table) = table.as_ref() {
        let column = &table.columns()[column_index];
        if let Some(default_expr) = column.default.as_ref() {
            let stores_another_form = column.is_array()
                || match resolver
                    .schema()
                    .resolve_type(&column.ty_str, table.is_strict)?
                {
                    Some(resolved) => resolved.chain.iter().any(|td| td.has_encode_or_coerce()),
                    None => false,
                };
            if stores_another_form {
                encoded_default = Some((table, column, default_expr));
            }
        }
    }
    let Some((table, column, default_expr)) = encoded_default else {
        program.emit_column_or_rowid(cursor_id, column_index, target_register);
        return Ok(());
    };
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
