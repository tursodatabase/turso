use crate::translate::expr::{walk_expr, WalkControl};
use crate::translate::plan::{ColumnUsedMask, JoinedTable, TableReferences};
use crate::{schema::Index, sync::Arc, Result};
use turso_parser::ast;
use turso_parser::ast::TableInternalId;

/// Find the selected index key that stores this complete expression.
///
/// GROUP BY and expression translation must use the same match rules.
pub fn selected_expression_index<'a>(
    expr: &ast::Expr,
    table_references: &'a TableReferences,
) -> Option<(&'a JoinedTable, &'a Arc<Index>, usize)> {
    let (table_id, _) = single_table_column_usage(expr)?;
    let table = table_references.find_joined_table_by_internal_id(table_id)?;
    let index = table.op.index()?;
    let expression_position = table.expression_index_position(index, expr)?;
    Some((table, index, expression_position))
}

/// Determine whether an expression references columns from exactly one table
/// and, if so, which specific columns are used.
///
/// The optimizer only treats an expression index as covering if every column
/// required to compute that expression is satisfied by the index key itself.
/// This helper tells us:
///
/// - `a + b` on table `t` -> returns table `t` plus a mask for `a` and `b`.
/// - `t.a + u.b` -> returns `None` so we do not mis-apply a single-table expression index.
pub fn single_table_column_usage(expr: &ast::Expr) -> Option<(TableInternalId, ColumnUsedMask)> {
    let mut table_id: Option<TableInternalId> = None;
    let mut columns = ColumnUsedMask::default();
    let mut ok = true;
    let _ = walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        if let ast::Expr::Column { table, column, .. } = e {
            if let Some(existing) = table_id {
                if existing != *table {
                    ok = false;
                    return Ok(WalkControl::SkipChildren);
                }
            } else {
                table_id = Some(*table);
            }
            columns.set(*column)?;
        }
        Ok(WalkControl::Continue)
    });

    if ok {
        table_id.map(|id| (id, columns))
    } else {
        None
    }
}

/// The set of table columns that a stored index key expression or
/// partial-index WHERE clause reads.
pub fn expression_index_column_usage(expr: &ast::Expr) -> ColumnUsedMask {
    single_table_column_usage(expr)
        .map(|(_, columns_mask)| columns_mask)
        .unwrap_or_default()
}
