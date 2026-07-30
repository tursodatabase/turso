use crate::translate::expr::{walk_expr, WalkControl};
use crate::translate::plan::ColumnUsedMask;
use crate::Result;
use turso_parser::ast;
use turso_parser::ast::TableInternalId;

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

/// Return the set of table columns an expression-index key expression reads.
///
/// Index expressions are stored with their column references pre-resolved to
/// `SELF_TABLE` positional form at schema load, so this is a plain walk — no
/// name resolution.
pub fn expression_index_column_usage(expr: &ast::Expr) -> Result<ColumnUsedMask> {
    let mut mask = ColumnUsedMask::default();
    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        if let ast::Expr::Column { table, column, .. } = e {
            if table.is_self_table() {
                mask.set(*column)?;
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(mask)
}
