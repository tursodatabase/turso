use crate::schema::{BTreeTable, Column, ColumnLayout, ColumnsTopologicalSort, GeneratedType};
use crate::translate::expr::{
    emit_custom_type_encode_columns, emit_stored_column, maybe_apply_affinity, translate_expr,
    translate_expr_no_constant_opt, NoConstantOptReason,
};
use crate::translate::plan::ColumnMask;
use crate::vdbe::builder::{DmlColumnContext, DmlRowForm, SelfTableContext};
use crate::{Arc, Result};

use super::{ProgramBuilder, Resolver};

/// Read the row at `cursor_id` into `column_regs`: each stored column once, then each
/// virtual column, computed from those registers in dependency order.
pub(crate) fn emit_row_from_cursor(
    program: &mut ProgramBuilder,
    table: &Arc<BTreeTable>,
    cursor_id: usize,
    column_regs: &[usize],
    resolver: &Resolver,
) -> Result<()> {
    let columns = table.columns();
    for (idx, column) in columns.iter().enumerate() {
        if !column.is_virtual_generated() {
            emit_stored_column(program, cursor_id, idx, column_regs[idx], resolver)?;
            maybe_apply_affinity(column.ty(), column_regs[idx], program);
        }
    }
    if !table.has_virtual_columns() {
        return Ok(());
    }
    let dml_ctx = DmlColumnContext::from_column_reg_mapping(
        columns.iter().zip(column_regs.iter().copied()),
        DmlRowForm::Stored,
    );
    compute_virtual_columns(
        program,
        &table.columns_topo_sort()?,
        &dml_ctx,
        resolver,
        table,
    )
}

/// Emit bytecode to compute the generated columns of a row being written, in dependency order.
///
/// INSERT, UPDATE and UPSERT use it to compute every generated column of the new row, also the
/// virtual columns that nothing reads:
///
/// - Possible optimization: skip a virtual column that no index, CHECK, NOT NULL, foreign key,
///   trigger or RETURNING reads.
/// - Why we do not skip it: SQLite computes every generated column of the new row before the
///   BEFORE triggers fire, and an error in one of them stops the statement. With
///   `d AS (abs(a))`, `INSERT INTO t(a) VALUES (-9223372036854775808)` fails with "integer
///   overflow", also when nothing reads `d`. The `gencol_error_order_*` tests check this.
/// - How to skip safely: skip such a column only if its expression cannot fail, for example
///   column references, literals, arithmetic, comparisons, CASE, COALESCE and CAST. JSON
///   functions can fail, so a column that extracts a field from a JSON document is still
///   computed.
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
            let stores_value = dml_ctx.holds_stored_value(idx);
            if stores_value
                && resolver
                    .schema()
                    .column_has_custom_type(column, table.is_strict)
            {
                // ENCODE changes the register in place below, so the expression must
                // write the register again for each row.
                translate_expr_no_constant_opt(
                    program,
                    None,
                    expr,
                    target_reg,
                    resolver,
                    NoConstantOptReason::CustomTypeEncode,
                )?;
            } else {
                translate_expr(program, None, expr, target_reg, resolver)?;
            }
            if stores_value {
                emit_virtual_column_store(program, resolver, column, target_reg, table)?;
            }
            program.emit_column_affinity(target_reg, column.affinity());
        }
        Ok(())
    })
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

/// Whether the statement sets a column that has a custom type or is an array. Its ENCODE,
/// COERCE, and DECODE can make a read of the written row give another value than the SET
/// value: COERCE changes 2.5 into 2.50 in a numeric(10,2) column. UPDATE and UPSERT compute
/// the generated columns from the SET values before ENCODE, so these can then differ from
/// the values that a read of the row gives.
pub(crate) fn sets_custom_type_column(
    table: &BTreeTable,
    set_columns: &ColumnMask,
    resolver: &Resolver,
) -> bool {
    table.columns().iter().enumerate().any(|(idx, column)| {
        set_columns.get(idx)
            && resolver
                .schema()
                .column_has_custom_type(column, table.is_strict)
    })
}
