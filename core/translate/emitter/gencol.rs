use crate::schema::ColumnsTopologicalSort;
use crate::translate::expr::translate_expr;
use crate::translate::plan::TableReferences;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::DmlColumnContext;
use crate::Result;
use turso_parser::ast::{self, TableInternalId};

use super::{ProgramBuilder, Resolver};

/// Compute the virtual generated columns of the row of table reference
/// `table_id`, which is held in `registers`.
#[turso_macros::trace_stack]
pub fn compute_virtual_columns(
    program: &mut ProgramBuilder,
    columns: &ColumnsTopologicalSort<'_>,
    registers: &DmlColumnContext,
    resolver: &Resolver,
    table_references: &TableReferences,
    table_id: TableInternalId,
) -> Result<()> {
    resolver.with_row_image(program, table_id, Some(registers), |program| {
        for (idx, column) in columns.iter() {
            if !column.is_virtual_generated() {
                continue;
            }
            let expr = table_references.virtual_column_expr(table_id, idx);
            let target_reg = registers.to_column_reg(idx);
            translate_expr(program, Some(table_references), &expr, target_reg, resolver)?;
            if column.affinity() != Affinity::Blob {
                program.emit_column_affinity(target_reg, column.affinity());
            }
        }
        Ok(())
    })
}

/// Compute one virtual generated column of the row of table reference
/// `table_id`, which is held in `registers`. `expr` is the expression of the
/// column, bound to that reference.
pub(crate) fn emit_gencol_expr_from_registers(
    program: &mut ProgramBuilder,
    expr: &ast::Expr,
    target_reg: usize,
    registers: &DmlColumnContext,
    resolver: &Resolver,
    table_references: &TableReferences,
    table_id: TableInternalId,
) -> Result<()> {
    resolver.with_row_image(program, table_id, Some(registers), |program| {
        translate_expr(program, Some(table_references), expr, target_reg, resolver)?;
        Ok(())
    })
}
