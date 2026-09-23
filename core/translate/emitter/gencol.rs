use crate::schema::{BTreeTable, ColumnsTopologicalSort, GeneratedType};
use crate::translate::expr::translate_expr;
use crate::vdbe::builder::{DmlColumnContext, SelfTableContext};
use crate::{Arc, Result};

use super::{ProgramBuilder, Resolver};

/// Emit bytecode to compute virtual generated columns for a row.
#[turso_macros::trace_stack]
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
    resolver.with_self_table_context(program, Some(&ctx), |program, _| {
        for (idx, column) in columns.iter() {
            let GeneratedType::Virtual { expr, .. } = column.generated_type() else {
                continue;
            };
            let target_reg = dml_ctx.to_column_reg(idx);
            translate_expr(program, None, expr, target_reg, resolver)?;
            program.emit_column_affinity(target_reg, column.affinity());
        }
        Ok(())
    })
}
