use crate::translate::expr::{walk_expr, walk_expr_mut, WalkControl};
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
    let normalized = normalize_expr_for_index_matching(expr, table);
    let expression_position = index.expression_to_index_pos(&normalized)?;
    Some((table, index, expression_position))
}

/// Normalize a query expression so it can be compared with an
/// expression stored on an index definition.
///
/// Index definitions store their expressions with the column references of
/// the indexed table resolved to `Expr::Column { table: SELF_TABLE }`. The
/// query expression is bound to the table reference of this statement, so
/// its references to that table are rewritten to the same stored form:
///
/// - `CREATE INDEX idx ON t(a + b)` stores `Column(SELF_TABLE, 0) + Column(SELF_TABLE, 1)`
/// - `SELECT * FROM t WHERE a + b = 10` binds `Column(t, 0) + Column(t, 1)`
///
/// After normalization, both sides are equal, allowing an equality check to
/// spot the match.
pub fn normalize_expr_for_index_matching(
    expr: &ast::Expr,
    table_reference: &JoinedTable,
) -> ast::Expr {
    let mut expr = expr.clone();
    let mut normalize = |e: &mut ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Column {
                database, table, ..
            } if *table == table_reference.internal_id => {
                *database = None;
                *table = TableInternalId::SELF_TABLE;
            }
            ast::Expr::RowId { database, table } if *table == table_reference.internal_id => {
                *database = None;
                *table = TableInternalId::SELF_TABLE;
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    };
    let _ = walk_expr_mut(&mut expr, &mut normalize);
    expr
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
