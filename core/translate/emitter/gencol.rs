use crate::schema::{BTreeTable, ColumnLayout, ColumnsTopologicalSort, GeneratedType};
use crate::translate::expr::translate_expr;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::{DmlColumnContext, SelfTableContext};
use crate::{Arc, Result};
use turso_parser::ast;

use super::{ProgramBuilder, Resolver};

/// Emit bytecode to compute virtual generated columns for a row.
pub fn compute_virtual_columns(
    program: &mut ProgramBuilder,
    columns: &ColumnsTopologicalSort<'_>,
    dml_ctx: &DmlColumnContext,
    resolver: &Resolver,
    table: &Arc<BTreeTable>,
) -> Result<()> {
    let ctx = SelfTableContext::ForDML {
        dml_ctx: dml_ctx.clone(),
        table: Arc::clone(table),
    };
    for (idx, column) in columns.iter() {
        let GeneratedType::Virtual { expr, .. } = column.generated_type() else {
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
    table: &Arc<BTreeTable>,
) -> Result<()> {
    let ctx = SelfTableContext::ForDML {
        dml_ctx: DmlColumnContext::layout(columns, registers_start, rowid_reg, layout.clone()),
        table: Arc::clone(table),
    };
    program.with_self_table_context(Some(&ctx), |program, _| {
        translate_expr(program, None, expr, target_reg, resolver)?;
        Ok(())
    })?;

    Ok(())
}
