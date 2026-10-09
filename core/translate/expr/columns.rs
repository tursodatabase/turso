use super::*;
use crate::schema::bind_schema_expr;

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
    match column.generated_expr() {
        Some(expr) => {
            let expr = bind_schema_expr(expr, table_ref_id);
            translate_expr(
                program,
                Some(referenced_tables),
                &expr,
                target_register,
                resolver,
            )?;
            program.emit_column_affinity(target_register, column.affinity());
        }
        None => program.emit_column_or_rowid(cursor_id, column_index, target_register),
    }
    Ok(())
}

/// Equivalent of [emit_table_column] for when the row of the table reference
/// is held in registers: a virtual generated column is computed from them.
#[allow(clippy::too_many_arguments)]
pub fn emit_table_column_for_dml(
    program: &mut ProgramBuilder,
    cursor_id: CursorID,
    registers: &DmlColumnContext,
    table_ref_id: TableInternalId,
    referenced_tables: &TableReferences,
    column: &Column,
    column_index: usize,
    target_register: usize,
    resolver: &Resolver,
) -> Result<()> {
    resolver.with_row_image(program, table_ref_id, Some(registers), |program| {
        emit_table_column(
            program,
            cursor_id,
            table_ref_id,
            referenced_tables,
            column,
            column_index,
            target_register,
            resolver,
        )
    })
}
