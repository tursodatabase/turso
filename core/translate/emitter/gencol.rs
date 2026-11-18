use crate::schema::{BTreeTable, Column, ColumnLayout, ColumnsTopologicalSort, GeneratedType};
use crate::translate::expr::{emit_custom_type_encode_columns, translate_expr};
use crate::vdbe::builder::{DmlColumnContext, DmlRowForm, SelfTableContext};
use crate::{Arc, Result};

use super::{ProgramBuilder, Resolver};

/// Emit bytecode to compute the generated columns of a row being written, in dependency order.
#[turso_macros::trace_stack]
pub fn compute_generated_columns(
    program: &mut ProgramBuilder,
    columns: &ColumnsTopologicalSort<'_>,
    dml_ctx: &DmlColumnContext,
    resolver: &Resolver,
    table: &Arc<BTreeTable>,
) -> Result<()> {
    emit_generated_columns(
        program,
        columns,
        dml_ctx,
        resolver,
        table,
        GeneratedColumns::All,
    )
}

/// Emit bytecode to compute virtual generated columns for a row.
#[turso_macros::trace_stack]
pub fn compute_virtual_columns(
    program: &mut ProgramBuilder,
    columns: &ColumnsTopologicalSort<'_>,
    dml_ctx: &DmlColumnContext,
    resolver: &Resolver,
    table: &Arc<BTreeTable>,
) -> Result<()> {
    emit_generated_columns(
        program,
        columns,
        dml_ctx,
        resolver,
        table,
        GeneratedColumns::VirtualOnly,
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GeneratedColumns {
    All,
    VirtualOnly,
}

fn emit_generated_columns(
    program: &mut ProgramBuilder,
    columns: &ColumnsTopologicalSort<'_>,
    dml_ctx: &DmlColumnContext,
    resolver: &Resolver,
    table: &Arc<BTreeTable>,
    which: GeneratedColumns,
) -> Result<()> {
    let ctx = SelfTableContext::ForDML {
        dml_ctx: dml_ctx.clone(),
        table: Arc::clone(table),
    };
    resolver.with_self_table_context(program, Some(&ctx), |program, _| {
        for (idx, column) in columns.iter() {
            let expr = match column.generated_type() {
                GeneratedType::Virtual { expr, .. } => expr,
                GeneratedType::Stored { expr, .. } if which == GeneratedColumns::All => expr,
                GeneratedType::Stored { .. } | GeneratedType::NotGenerated => continue,
            };
            let target_reg = dml_ctx.to_column_reg(idx);
            translate_expr(program, None, expr, target_reg, resolver)?;
            if column.is_virtual_generated() && dml_ctx.holds_stored_value(idx) {
                emit_virtual_column_store(program, resolver, column, target_reg, table)?;
            }
            program.emit_column_affinity(target_reg, column.affinity());
        }
        Ok(())
    })
}

/// Emit bytecode to compute the stored value of a single virtual generated
/// column from a row whose registers hold stored values.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_gencol_expr_from_registers(
    program: &mut ProgramBuilder,
    column: &Column,
    target_reg: usize,
    registers_start: usize,
    columns: &[Column],
    resolver: &Resolver,
    rowid_reg: usize,
    layout: &ColumnLayout,
    table: &Arc<BTreeTable>,
) -> Result<()> {
    let GeneratedType::Virtual { expr, .. } = column.generated_type() else {
        unreachable!("emit_gencol_expr_from_registers expects a virtual generated column");
    };
    let ctx = SelfTableContext::ForDML {
        dml_ctx: DmlColumnContext::layout(
            columns,
            registers_start,
            rowid_reg,
            layout.clone(),
            DmlRowForm::Stored,
        ),
        table: Arc::clone(table),
    };
    resolver.with_self_table_context(program, Some(&ctx), |program, _| {
        translate_expr(program, None, expr, target_reg, resolver)?;
        Ok(())
    })?;
    emit_virtual_column_store(program, resolver, column, target_reg, table)?;

    Ok(())
}

/// Turns the value of a virtual generated column's expression in `reg` into
/// the value storing it in the column would store, by running the column
/// type's ENCODE and COERCE. Every other column is read in that form and
/// DECODE turns it into the value a query sees, so a virtual column is read
/// the same way: `y cents AS (w)` with w = 300 reads 300. Only a STRICT
/// table has custom types; elsewhere a type name such as NUMERIC is just an
/// affinity.
pub(crate) fn emit_virtual_column_store(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    column: &Column,
    reg: usize,
    table: &BTreeTable,
) -> Result<()> {
    if !table.is_strict {
        return Ok(());
    }
    emit_custom_type_encode_columns(
        program,
        resolver,
        std::slice::from_ref(column),
        reg,
        None,
        &table.name,
        &ColumnLayout::Identity { column_count: 1 },
    )
}
