use crate::translate::expr::{walk_expr, walk_expr_mut, WalkControl};
use crate::translate::plan::{ColumnUsedMask, JoinedTable, TableReferences};
use crate::Result;
use turso_parser::ast;
use turso_parser::ast::TableInternalId;

/// Normalize a query expression so it can be compared with an
/// expression stored on an index definition.
///
/// Index expressions are stored with their column references pre-resolved to
/// `SELF_TABLE` positional form at schema load. A bound query expression uses
/// the query's table ids, so rewriting `table_reference`'s id back to
/// `SELF_TABLE` makes both sides directly comparable:
///
/// - `CREATE INDEX idx ON t(a + b);` stores `Column(SELF, 0) + Column(SELF, 1)`
/// - `SELECT * FROM t WHERE a + b = 10;` binds to `Column(t, 0) + Column(t, 1)`
///
/// After normalization, both sides look like `Column(SELF, 0) + Column(SELF, 1)`.
/// Columns of other tables keep their real ids and can never match.
pub fn normalize_expr_for_index_matching(
    expr: &ast::Expr,
    table_reference: &JoinedTable,
    _table_references: &TableReferences,
) -> ast::Expr {
    let mut expr = expr.clone();
    let mut normalize = |e: &mut ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. }
                if *table == table_reference.internal_id =>
            {
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
