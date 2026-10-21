use turso_core::{walk_expr_mut, Result, WalkControl};
use turso_parser::ast;

pub(crate) fn walk_stmt_exprs_mut<F>(stmt: &mut ast::Stmt, f: &mut F) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<WalkControl>,
{
    match stmt {
        ast::Stmt::Select(select) => walk_select_exprs_mut(select, f),
        ast::Stmt::Insert {
            with,
            body,
            returning,
            ..
        } => {
            walk_with_exprs_mut(with.as_mut(), f)?;
            if let ast::InsertBody::Select(select, upsert) = body {
                walk_select_exprs_mut(select, f)?;
                let mut upsert = upsert.as_deref_mut();
                while let Some(clause) = upsert {
                    if let Some(index) = &mut clause.index {
                        for target in &mut index.targets {
                            walk_tree_mut(&mut target.expr, f)?;
                        }
                        if let Some(where_clause) = &mut index.where_clause {
                            walk_tree_mut(where_clause, f)?;
                        }
                    }
                    if let ast::UpsertDo::Set { sets, where_clause } = &mut clause.do_clause {
                        for set in sets {
                            walk_tree_mut(&mut set.expr, f)?;
                        }
                        if let Some(where_clause) = where_clause {
                            walk_tree_mut(where_clause, f)?;
                        }
                    }
                    upsert = clause.next.as_deref_mut();
                }
            }
            walk_result_columns_mut(returning, f)
        }
        ast::Stmt::Update(update) => {
            walk_with_exprs_mut(update.with.as_mut(), f)?;
            for set in &mut update.sets {
                walk_tree_mut(&mut set.expr, f)?;
            }
            if let Some(from) = &mut update.from {
                walk_from_exprs_mut(from, f)?;
            }
            if let Some(where_clause) = &mut update.where_clause {
                walk_tree_mut(where_clause, f)?;
            }
            walk_result_columns_mut(&mut update.returning, f)
        }
        ast::Stmt::Delete {
            with,
            where_clause,
            returning,
            ..
        } => {
            walk_with_exprs_mut(with.as_mut(), f)?;
            if let Some(where_clause) = where_clause {
                walk_tree_mut(where_clause, f)?;
            }
            walk_result_columns_mut(returning, f)
        }
        _ => Ok(()),
    }
}

pub(crate) fn walk_tree_mut<F>(expr: &mut ast::Expr, f: &mut F) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<WalkControl>,
{
    walk_expr_mut(expr, &mut |e| {
        let control = f(e)?;
        if matches!(control, WalkControl::Continue) {
            match e {
                ast::Expr::Subquery(select) | ast::Expr::Exists(select) => {
                    walk_select_exprs_mut(select, f)?
                }
                ast::Expr::InSelect { rhs, .. } => walk_select_exprs_mut(rhs, f)?,
                _ => {}
            }
        }
        Ok(control)
    })?;
    Ok(())
}

fn walk_select_exprs_mut<F>(select: &mut ast::Select, f: &mut F) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<WalkControl>,
{
    walk_with_exprs_mut(select.with.as_mut(), f)?;
    walk_one_select_exprs_mut(&mut select.body.select, f)?;
    for compound in &mut select.body.compounds {
        walk_one_select_exprs_mut(&mut compound.select, f)?;
    }
    for sorted in &mut select.order_by {
        walk_tree_mut(&mut sorted.expr, f)?;
    }
    if let Some(limit) = &mut select.limit {
        walk_tree_mut(&mut limit.expr, f)?;
        if let Some(offset) = &mut limit.offset {
            walk_tree_mut(offset, f)?;
        }
    }
    Ok(())
}

fn walk_one_select_exprs_mut<F>(select: &mut ast::OneSelect, f: &mut F) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<WalkControl>,
{
    match select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            ..
        } => {
            walk_result_columns_mut(columns, f)?;
            if let Some(from) = from {
                walk_from_exprs_mut(from, f)?;
            }
            if let Some(where_clause) = where_clause {
                walk_tree_mut(where_clause, f)?;
            }
            if let Some(group_by) = group_by {
                for expr in &mut group_by.exprs {
                    walk_tree_mut(expr, f)?;
                }
                if let Some(having) = &mut group_by.having {
                    walk_tree_mut(having, f)?;
                }
            }
            Ok(())
        }
        ast::OneSelect::Values(rows) => {
            for expr in rows.iter_mut().flatten() {
                walk_tree_mut(expr, f)?;
            }
            Ok(())
        }
    }
}

fn walk_from_exprs_mut<F>(from: &mut ast::FromClause, f: &mut F) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<WalkControl>,
{
    walk_select_table_exprs_mut(&mut from.select, f)?;
    for join in &mut from.joins {
        walk_select_table_exprs_mut(&mut join.table, f)?;
        if let Some(ast::JoinConstraint::On(expr)) = &mut join.constraint {
            walk_tree_mut(expr, f)?;
        }
    }
    Ok(())
}

fn walk_select_table_exprs_mut<F>(table: &mut ast::SelectTable, f: &mut F) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<WalkControl>,
{
    match table {
        ast::SelectTable::Table(..) => Ok(()),
        ast::SelectTable::TableCall(_, args, _) => {
            for arg in args {
                walk_tree_mut(arg, f)?;
            }
            Ok(())
        }
        ast::SelectTable::Select { select, .. } => walk_select_exprs_mut(select, f),
        ast::SelectTable::Sub(from, _) => walk_from_exprs_mut(from, f),
    }
}

fn walk_with_exprs_mut<F>(with: Option<&mut ast::With>, f: &mut F) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<WalkControl>,
{
    for cte in with.into_iter().flat_map(|with| with.ctes.iter_mut()) {
        walk_select_exprs_mut(&mut cte.select, f)?;
    }
    Ok(())
}

fn walk_result_columns_mut<F>(columns: &mut [ast::ResultColumn], f: &mut F) -> Result<()>
where
    F: FnMut(&mut ast::Expr) -> Result<WalkControl>,
{
    for column in columns {
        if let ast::ResultColumn::Expr(expr, _) = column {
            walk_tree_mut(expr, f)?;
        }
    }
    Ok(())
}
