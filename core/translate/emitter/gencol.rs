use crate::function::PureContext;
use crate::schema::{BTreeTable, Column, ColumnLayout, ColumnsTopologicalSort, GeneratedType};
use crate::translate::expr::{
    emit_custom_type_encode_columns, translate_expr, translate_expr_no_constant_opt,
    NoConstantOptReason,
};
use crate::translate::plan::TableReferences;
use crate::vdbe::builder::{DmlColumnContext, DmlRowForm, SelfTableContext};
use crate::{Arc, Result};
use turso_parser::ast;

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
            translate_generated_column_expr(program, None, expr, target_reg, resolver, None)?;
            if dml_ctx.holds_stored_value(idx) {
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
        translate_generated_column_expr(program, None, expr, target_reg, resolver, None)?;
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

/// Translates the expression of a virtual generated column into `target_reg`.
/// Every read or write of a generated column's value goes through here, so the
/// date and time functions in the expression are checked the way SQLite checks
/// them in a generated column.
pub(crate) fn translate_generated_column_expr(
    program: &mut ProgramBuilder,
    referenced_tables: Option<&TableReferences>,
    expr: &ast::Expr,
    target_reg: usize,
    resolver: &Resolver,
    no_constant_opt: Option<NoConstantOptReason>,
) -> Result<()> {
    program.with_pure_context(PureContext::GeneratedColumn, |program| {
        match no_constant_opt {
            Some(reason) => translate_expr_no_constant_opt(
                program,
                referenced_tables,
                expr,
                target_reg,
                resolver,
                reason,
            )?,
            None => translate_expr(program, referenced_tables, expr, target_reg, resolver)?,
        };
        Ok(())
    })
}
