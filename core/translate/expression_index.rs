use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    bind_and_rewrite_expr, walk_expr, walk_expr_mut, BindingBehavior, WalkControl,
};
use crate::translate::plan::{ColumnUsedMask, JoinedTable, TableReferences};
use crate::translate::planner::ROWID_STRS;
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
    let normalized = normalize_expr_for_index_matching(expr, table, table_references);
    let expression_position = index.expression_to_index_pos(&normalized)?;
    Some((table, index, expression_position))
}

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

pub fn for_each_part_that_can_use_index_value<F>(
    expr: &ast::Expr,
    resolver: &Resolver,
    visit: &mut F,
) -> Result<()>
where
    F: FnMut(&ast::Expr),
{
    visit_part_that_can_use_index_value(expr, false, resolver, visit)
}

fn visit_part_that_can_use_index_value<F>(
    expr: &ast::Expr,
    is_subtype_argument: bool,
    resolver: &Resolver,
    visit: &mut F,
) -> Result<()>
where
    F: FnMut(&ast::Expr),
{
    if !index_value_would_lose_subtype(expr, is_subtype_argument, resolver)? {
        visit(expr);
    }
    let subtype_arguments = arguments_that_keep_subtypes(expr, is_subtype_argument, resolver)?;
    walk_expr(expr, &mut |part| {
        if std::ptr::eq(part, expr) {
            return Ok(WalkControl::Continue);
        }
        let is_subtype_argument = subtype_arguments
            .iter()
            .any(|argument| std::ptr::eq(*argument, part));
        visit_part_that_can_use_index_value(part, is_subtype_argument, resolver, visit)?;
        Ok(WalkControl::SkipChildren)
    })?;
    Ok(())
}

pub fn index_value_would_lose_subtype(
    expr: &ast::Expr,
    is_subtype_argument: bool,
    resolver: &Resolver,
) -> Result<bool> {
    Ok(is_subtype_argument && can_return_subtype(expr, resolver)?)
}

pub fn arguments_that_keep_subtypes<'a>(
    expr: &'a ast::Expr,
    is_subtype_argument: bool,
    resolver: &Resolver,
) -> Result<Vec<&'a ast::Expr>> {
    let arguments: Vec<&ast::Expr> = match expr {
        ast::Expr::FunctionCall { name, args, .. } => {
            let reads_subtypes = is_subtype_argument
                || resolver
                    .resolve_function(name.as_str(), args.len())?
                    .is_some_and(|func| func.reads_argument_subtypes());
            if !reads_subtypes {
                return Ok(Vec::new());
            }
            args.iter().map(|arg| arg.as_ref()).collect()
        }
        _ if !is_subtype_argument => return Ok(Vec::new()),
        ast::Expr::Binary(lhs, ast::Operator::ArrowRight | ast::Operator::ArrowRightShift, rhs) => {
            vec![lhs.as_ref(), rhs.as_ref()]
        }
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => vec![exprs[0].as_ref()],
        _ => Vec::new(),
    };
    Ok(arguments)
}

fn can_return_subtype(expr: &ast::Expr, resolver: &Resolver) -> Result<bool> {
    match expr {
        ast::Expr::FunctionCall { name, args, .. } => {
            let func = resolver.resolve_function(name.as_str(), args.len())?;
            if func.is_none_or(|func| func.can_return_subtype()) {
                return Ok(true);
            }
            for arg in args {
                if can_return_subtype(arg, resolver)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        ast::Expr::FunctionCallStar { name, .. } => Ok(resolver
            .resolve_function(name.as_str(), 0)?
            .is_none_or(|func| func.can_return_subtype())),
        ast::Expr::Binary(_, ast::Operator::ArrowRight, _) => Ok(true),
        ast::Expr::Binary(lhs, ast::Operator::ArrowRightShift, rhs) => {
            Ok(can_return_subtype(lhs, resolver)? || can_return_subtype(rhs, resolver)?)
        }
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            can_return_subtype(&exprs[0], resolver)
        }
        _ => Ok(false),
    }
}
