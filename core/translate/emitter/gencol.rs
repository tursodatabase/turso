use crate::schema::{ColumnLayout, GeneratedType};
use crate::translate::expr::translate_expr;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::{DmlColumnContext, SelfTableContext};
use crate::Result;
use turso_parser::ast;

use super::{ProgramBuilder, Resolver};

/// Emit bytecode to compute all virtual generated columns for a row.
pub fn compute_virtual_columns(
    program: &mut ProgramBuilder,
    columns: &[crate::schema::Column],
    dml_ctx: &DmlColumnContext,
    resolver: &Resolver,
) -> Result<()> {
    let ctx = SelfTableContext::ForDML(dml_ctx.clone());
    for (idx, column) in columns.iter().enumerate() {
        let GeneratedType::Virtual { resolved: expr, .. } = column.generated_type() else {
            continue;
        };
        let target_reg = dml_ctx.to_column_reg(idx);
        program.with_self_table_context(Some(&ctx), |program, _| {
            translate_expr(program, None, expr, target_reg, resolver)
        })?;
        if column.affinity() != Affinity::Blob {
            program.emit_column_affinity(target_reg, column.affinity());
        }
    }
    Ok(())
}

/// Emit bytecode to compute a single virtual generated column expression.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_gencol_expr_from_registers(
    program: &mut ProgramBuilder,
    expr: &ast::Expr,
    target_reg: usize,
    registers_start: usize,
    columns: &[crate::schema::Column],
    resolver: &Resolver,
    rowid_reg: usize,
    layout: &ColumnLayout,
) -> Result<()> {
    let ctx = SelfTableContext::ForDML(DmlColumnContext::layout(
        columns,
        registers_start,
        rowid_reg,
        layout.clone(),
    ));
    program.with_self_table_context(Some(&ctx), |program, _| {
        translate_expr(program, None, expr, target_reg, resolver)?;
        Ok(())
    })?;

    Ok(())
}
