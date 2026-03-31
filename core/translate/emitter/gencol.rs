use rustc_hash::FxHashSet as HashSet;

use crate::schema::{ColumnLayout, GeneratedType};
use crate::translate::expr::translate_expr;
use crate::vdbe::affinity::Affinity;
use crate::vdbe::builder::{DmlColumnContext, SelfTableContext};
use crate::Result;
use turso_parser::ast;

use super::{ProgramBuilder, Resolver};

/// Emit bytecode to compute virtual generated columns for a row.
///
/// When `affected_columns` is `Some`, only virtual columns whose schema index
/// is in the set are recomputed (UPDATE/UPSERT optimization — unchanged
/// columns already hold the correct value from the cursor read).
/// When `None`, all virtual columns are recomputed (INSERT path, trigger
/// contexts, current-row snapshots).
pub fn compute_virtual_columns(
    program: &mut ProgramBuilder,
    columns: &[crate::schema::Column],
    dml_ctx: &DmlColumnContext,
    resolver: &Resolver,
    affected_columns: Option<&HashSet<usize>>,
) -> Result<()> {
    let ctx = SelfTableContext::ForDML(dml_ctx.clone());
    for (idx, column) in columns.iter().enumerate() {
        let GeneratedType::Virtual { resolved: expr, .. } = column.generated_type() else {
            continue;
        };
        if let Some(affected) = affected_columns {
            if !affected.contains(&idx) {
                continue;
            }
        }
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
