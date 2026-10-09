use crate::schema::{bind_schema_expr, ColumnsTopologicalSort};
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
            let Some(expr) = column.generated_expr() else {
                continue;
            };
            let target_reg = registers.to_column_reg(idx);
            let expr = bind_schema_expr(expr, table_id);
            translate_expr(program, Some(table_references), &expr, target_reg, resolver)?;
            if column.affinity() != Affinity::Blob {
                program.emit_column_affinity(target_reg, column.affinity());
            }
        }
        Ok(())
    })
}

/// Compute one virtual generated column expression of the row of table
/// reference `table_id`, which is held in `registers`.
pub(crate) fn emit_gencol_expr_from_registers(
    program: &mut ProgramBuilder,
    expr: &ast::Expr,
    target_reg: usize,
    registers: &DmlColumnContext,
    resolver: &Resolver,
    table_references: &TableReferences,
    table_id: TableInternalId,
) -> Result<()> {
    let expr = bind_schema_expr(expr, table_id);
    resolver.with_row_image(program, table_id, Some(registers), |program| {
        translate_expr(program, Some(table_references), &expr, target_reg, resolver)?;
        Ok(())
    })
}
