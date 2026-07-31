use super::*;
use crate::translate::plan_expr::PlanSourceId;

/// Read a single column from a BTreeTable cursor, transparently computing
/// virtual generated columns inline instead of hitting `emit_column`.
/// All bulk column-reading call sites should use this instead of
/// `emit_column_or_rowid` directly.
#[allow(clippy::too_many_arguments)]
pub fn emit_table_column(
    program: &mut ProgramBuilder,
    cursor_id: CursorID,
    table_ref_id: PlanSourceId,
    referenced_tables: &TableReferences,
    column: &Column,
    column_index: usize,
    target_register: usize,
    resolver: &Resolver,
) -> Result<()> {
    if !column.is_virtual_generated() {
        program.emit_column_or_rowid(cursor_id, column_index, target_register);
        return Ok(());
    }

    let generated = referenced_tables
        .find_source_read_programs_by_internal_id(table_ref_id)
        .and_then(|programs| programs.generated_expressions.get(column_index))
        .and_then(Option::as_ref)
        .ok_or_else(|| {
            LimboError::InternalError(format!(
                "virtual generated column {table_ref_id}.{column_index} has no planned expression"
            ))
        })?;
    translate_plan_expr(
        program,
        Some(referenced_tables),
        generated,
        target_register,
        resolver,
    )?;
    program.emit_column_affinity(target_register, column.affinity());
    Ok(())
}

/// Copy a column from a DML row image whose generated columns have already
/// been computed by `emit_columns_and_dependencies`.
pub fn emit_table_column_for_dml(
    program: &mut ProgramBuilder,
    dml_column_context: &DmlColumnContext,
    column_index: usize,
    target_register: usize,
) {
    program.emit_insn(Insn::Copy {
        src_reg: dml_column_context.to_column_reg(column_index),
        dst_reg: target_register,
        extra_amount: 0,
    });
}
