use crate::alloc::TursoIteratorExt;
use crate::schema::{
    columns_referenced_by_expr, BTreeTable, Column, ColumnsTopologicalSort, GeneratedType, Index,
    EXPR_INDEX_SENTINEL,
};
use crate::translate::expr::translate_expr;
use crate::translate::plan::ColumnMask;
use crate::vdbe::builder::{DmlColumnContext, SelfTableContext};
use crate::{Arc, Result};

use super::{ProgramBuilder, Resolver};

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
            program.emit_column_or_rowid(cursor_id, idx, column_regs[idx]);
        }
    }
    if !table.has_virtual_columns {
        return Ok(());
    }
    let dml_ctx =
        DmlColumnContext::from_column_reg_mapping(columns.iter().zip(column_regs.iter().copied()))
            .with_encoded_columns((0..columns.len()).try_collect()?);
    compute_virtual_columns(
        program,
        &table.columns_topo_sort()?,
        &dml_ctx,
        resolver,
        table,
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

pub(crate) fn columns_read_by_index(index: &Index, columns: &[Column]) -> Result<ColumnMask> {
    let mut read = ColumnMask::default();
    for index_column in &index.columns {
        if index_column.pos_in_table == EXPR_INDEX_SENTINEL {
            let expr = index_column
                .expr
                .as_ref()
                .expect("expression index column has an expression");
            read.union_with(&columns_referenced_by_expr(expr, columns)?)?;
        } else {
            read.set(index_column.pos_in_table)?;
        }
    }
    if let Some(where_clause) = &index.where_clause {
        read.union_with(&columns_referenced_by_expr(where_clause, columns)?)?;
    }
    Ok(read)
}

pub(crate) fn foreign_key_columns(
    table: &BTreeTable,
    resolver: &Resolver,
    database_id: usize,
) -> Result<ColumnMask> {
    let mut columns = ColumnMask::default();
    for fk in resolver.with_schema(database_id, |s| s.resolved_fks_for_child(&table.name))? {
        for &pos in fk.child_pos.iter() {
            columns.set(pos)?;
        }
    }
    for fk in resolver.with_schema(database_id, |s| s.resolved_fks_referencing(&table.name))? {
        for &pos in fk.parent_pos.iter() {
            columns.set(pos)?;
        }
    }
    Ok(columns)
}
