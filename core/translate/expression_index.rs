use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    bind_and_rewrite_expr, walk_expr, walk_expr_mut, BindingBehavior, WalkControl,
};
use crate::translate::plan::{ColumnUsedMask, JoinedTable, TableReferences};
use crate::translate::planner::ROWID_STRS;
use crate::Result;
use turso_parser::ast;
use turso_parser::ast::TableInternalId;

/// Normalize a query expression so it can be compared with an
/// expression stored on an index definition.
///
/// We need to remove the bindings and turn them back into identifiers so we can say:
///
/// - `CREATE INDEX idx ON t(Expr::Id(a) + Expr::Id(b));`
/// - `SELECT * FROM t WHERE Expr::Column(name: 'a') + Expr::Column(name: 'b') = 10;`
///
/// After normalization, both sides look like `Expr::Id('a') + Expr::Id('b')`, allowing an
/// equality check to spot the match.
pub fn normalize_expr_for_index_matching(
    expr: &ast::Expr,
    table_reference: &JoinedTable,
    table_references: &TableReferences,
) -> ast::Expr {
    let mut expr = expr.clone();
    let _table_idx = table_references
        .joined_tables()
        .iter()
        .position(|t| t.internal_id == table_reference.internal_id)
        .expect("table must exist in table_references");
    let columns = table_reference.table.columns();
    let mut normalize = |e: &mut ast::Expr| -> Result<WalkControl> {
        match e {
            ast::Expr::Column { column, .. } => {
                if let Some(name) = columns.get(*column).and_then(|c| c.name.as_ref()) {
                    *e = ast::Expr::Id(ast::Name::exact(name.clone()));
                }
            }
            ast::Expr::RowId { .. } => {
                *e = ast::Expr::Id(ast::Name::exact(ROWID_STRS[0].to_string()));
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
            columns.set(*column);
        }
        Ok(WalkControl::Continue)
    });

    if ok {
        table_id.map(|id| (id, columns))
    } else {
        None
    }
}

/// Bind an expression index key expression against the target table and return
/// the set of referenced columns.
///
/// Expression index SQL is stored in schema form and may use the base table
/// name even when the query uses an alias. We bind using the base table name
/// to keep dependency analysis stable across aliases.
pub fn expression_index_column_usage(
    expr: &ast::Expr,
    table_reference: &JoinedTable,
    resolver: &Resolver<'_>,
) -> Result<ColumnUsedMask> {
    let mut bound_expr = expr.clone();
    let mut binding_table = table_reference.clone();
    if let Some(btree_table) = binding_table.table.btree() {
        binding_table.identifier.clone_from(&btree_table.name);
    }
    let mut binding_tables = TableReferences::new(vec![binding_table], vec![]);
    bind_and_rewrite_expr(
        &mut bound_expr,
        Some(&mut binding_tables),
        None,
        resolver,
        BindingBehavior::ResultColumnsNotAllowed,
    )?;

    Ok(single_table_column_usage(&bound_expr)
        .map(|(_, columns_mask)| columns_mask)
        .unwrap_or_default())
}
