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
            let null_row_expr = match self_table_context {
                SelfTableContext::ForSelect {
                    table_ref_id,
                    referenced_tables,
                } => {
                    let tables = referenced_tables.joined_tables();
                    // Outer-scope references have no join metadata here. A later FULL
                    // JOIN can also null out this table, even if its own join is inner.
                    let can_be_null_row = tables
                        .iter()
                        .position(|t| t.internal_id == *table_ref_id)
                        .is_none_or(|i| {
                            tables[i].join_info.as_ref().is_some_and(|j| j.is_outer())
                                || tables[i + 1..].iter().any(|t| {
                                    t.join_info.as_ref().is_some_and(|j| j.is_full_outer())
                                })
                        });
                    can_be_null_row.then(|| ast::Expr::IfNullRow {
                        table: *table_ref_id,
                        expr: expr.clone(),
                    })
                }
                SelfTableContext::ForDML { .. } => None,
            };
            resolver.with_self_table_context(program, Some(self_table_context), |program, _| {
                translate_expr(
                    program,
                    referenced_tables,
                    null_row_expr.as_ref().unwrap_or(expr),
                    target_register,
                    resolver,
                )?;
                Ok(())
            })?;
            program.emit_column_affinity(target_register, column.affinity());
        }
        _ => {
            program.emit_column_or_rowid(cursor_id, column_index, target_register);
        }
    }
    Ok(())
}
