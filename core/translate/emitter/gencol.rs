use crate::schema::{ColumnsTopologicalSort, GeneratedType};
use crate::translate::expr::translate_expr;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::{DmlColumnContext, SelfTableContext};
use crate::Result;

use super::{ProgramBuilder, Resolver};

/// Emit bytecode to compute virtual generated columns for a row.
pub fn compute_virtual_columns(
    program: &mut ProgramBuilder,
    columns: &ColumnsTopologicalSort<'_>,
    dml_ctx: &DmlColumnContext,
    resolver: &Resolver,
) -> Result<()> {
    let ctx = SelfTableContext::ForDML(dml_ctx.clone());
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
