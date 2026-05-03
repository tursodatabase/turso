use super::*;

pub enum WalkControl {
    Continue,     // Visit children
    SkipChildren, // Skip children but continue walking siblings
}

/// Recursively walks an immutable expression, applying a function to each sub-expression.
pub fn walk_expr<'a, F>(expr: &'a ast::Expr, func: &mut F) -> Result<WalkControl>
where
    F: FnMut(&'a ast::Expr) -> Result<WalkControl>,
{
    match func(expr)? {
        WalkControl::Continue => {
            match expr {
                ast::Expr::SubqueryResult { lhs, .. } => {
                    if let Some(lhs) = lhs {
                        walk_expr(lhs, func)?;
                    }
                }
                ast::Expr::Between {
                    lhs, start, end, ..
                } => {
                    walk_expr(lhs, func)?;
                    walk_expr(start, func)?;
                    walk_expr(end, func)?;
                }
                ast::Expr::Binary(lhs, _, rhs) => {
                    walk_expr(lhs, func)?;
                    walk_expr(rhs, func)?;
                }
                ast::Expr::Case {
                    base,
                    when_then_pairs,
                    else_expr,
                } => {
                    if let Some(base_expr) = base {
                        walk_expr(base_expr, func)?;
                    }
                    for (when_expr, then_expr) in when_then_pairs {
                        walk_expr(when_expr, func)?;
                        walk_expr(then_expr, func)?;
                    }
                    if let Some(else_expr) = else_expr {
                        walk_expr(else_expr, func)?;
                    }
                }
                ast::Expr::Cast { expr, .. } => {
                    walk_expr(expr, func)?;
                }
                ast::Expr::Collate(expr, _) => {
                    walk_expr(expr, func)?;
                }
                ast::Expr::Exists(_select) | ast::Expr::Subquery(_select) => {
                    // TODO: Walk through select statements if needed
                }
                ast::Expr::FunctionCall {
                    args,
                    order_by,
                    filter_over,
                    ..
                } => {
                    for arg in args {
                        walk_expr(arg, func)?;
                    }
                    for sort_col in order_by {
                        walk_expr(&sort_col.expr, func)?;
                    }
                    if let Some(filter_clause) = &filter_over.filter_clause {
                        walk_expr(filter_clause, func)?;
                    }
                    if let Some(over_clause) = &filter_over.over_clause {
                        match over_clause {
                            ast::Over::Window(window) => {
                                for part_expr in &window.partition_by {
                                    walk_expr(part_expr, func)?;
                                }
                                for sort_col in &window.order_by {
                                    walk_expr(&sort_col.expr, func)?;
                                }
                                if let Some(frame_clause) = &window.frame_clause {
                                    walk_expr_frame_bound(&frame_clause.start, func)?;
                                    if let Some(end_bound) = &frame_clause.end {
                                        walk_expr_frame_bound(end_bound, func)?;
                                    }
                                }
                            }
                            ast::Over::Name(_) => {}
                        }
                    }
                }
                ast::Expr::FunctionCallStar { filter_over, .. } => {
                    if let Some(filter_clause) = &filter_over.filter_clause {
                        walk_expr(filter_clause, func)?;
                    }
                    if let Some(over_clause) = &filter_over.over_clause {
                        match over_clause {
                            ast::Over::Window(window) => {
                                for part_expr in &window.partition_by {
                                    walk_expr(part_expr, func)?;
                                }
                                for sort_col in &window.order_by {
                                    walk_expr(&sort_col.expr, func)?;
                                }
                                if let Some(frame_clause) = &window.frame_clause {
                                    walk_expr_frame_bound(&frame_clause.start, func)?;
                                    if let Some(end_bound) = &frame_clause.end {
                                        walk_expr_frame_bound(end_bound, func)?;
                                    }
                                }
                            }
                            ast::Over::Name(_) => {}
                        }
                    }
                }
                ast::Expr::InList { lhs, rhs, .. } => {
                    walk_expr(lhs, func)?;
                    for expr in rhs {
                        walk_expr(expr, func)?;
                    }
                }
                ast::Expr::InSelect { lhs, rhs: _, .. } => {
                    walk_expr(lhs, func)?;
                    // TODO: Walk through select statements if needed
                }
                ast::Expr::InTable { lhs, args, .. } => {
                    walk_expr(lhs, func)?;
                    for expr in args {
                        walk_expr(expr, func)?;
                    }
                }
                ast::Expr::IsNull(expr) | ast::Expr::NotNull(expr) => {
                    walk_expr(expr, func)?;
                }
                ast::Expr::Like {
                    lhs, rhs, escape, ..
                } => {
                    walk_expr(lhs, func)?;
                    walk_expr(rhs, func)?;
                    if let Some(esc_expr) = escape {
                        walk_expr(esc_expr, func)?;
                    }
                }
                ast::Expr::Parenthesized(exprs) => {
                    for expr in exprs {
                        walk_expr(expr, func)?;
                    }
                }
                ast::Expr::Raise(_, expr) => {
                    if let Some(raise_expr) = expr {
                        walk_expr(raise_expr, func)?;
                    }
                }
                ast::Expr::Unary(_, expr) => {
                    walk_expr(expr, func)?;
                }
                ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
                    unreachable!(
                        "Array and Subscript are desugared into function calls by the parser"
                    )
                }
                ast::Expr::Id(_)
                | ast::Expr::Column { .. }
                | ast::Expr::RowId { .. }
                | ast::Expr::Literal(_)
                | ast::Expr::DoublyQualified(..)
                | ast::Expr::Name(_)
                | ast::Expr::Qualified(..)
                | ast::Expr::Variable(_)
                | ast::Expr::Register(_)
                | ast::Expr::Default => {
                    // No nested expressions
                }
                ast::Expr::FieldAccess { base, .. } => {
                    walk_expr(base, func)?;
                }
            }
        }
        WalkControl::SkipChildren => return Ok(WalkControl::Continue),
    };
    Ok(WalkControl::Continue)
}

pub fn expr_references_subquery_id(expr: &ast::Expr, subquery_id: TableInternalId) -> bool {
    let mut found = false;
    let _ = walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        if let ast::Expr::SubqueryResult {
            subquery_id: sid, ..
        } = e
        {
            if *sid == subquery_id {
                found = true;
                return Ok(WalkControl::SkipChildren);
            }
        }
        Ok(WalkControl::Continue)
    });
    found
}

pub fn expr_references_any_subquery(expr: &ast::Expr) -> bool {
    let mut found = false;
    let _ = walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        if matches!(e, ast::Expr::SubqueryResult { .. }) {
            found = true;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    });
    found
}

pub(super) fn walk_expr_frame_bound<'a, F>(
    bound: &'a ast::FrameBound,
    func: &mut F,
) -> Result<WalkControl>
where
    F: FnMut(&'a ast::Expr) -> Result<WalkControl>,
{
    match bound {
        ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) => {
            walk_expr(expr, func)?;
        }
        ast::FrameBound::CurrentRow
        | ast::FrameBound::UnboundedFollowing
        | ast::FrameBound::UnboundedPreceding => {}
    }

    Ok(WalkControl::Continue)
}

/// Recursively walks a mutable expression, applying a function to each sub-expression.
pub fn walk_expr_mut<F>(expr: &mut ast::Expr, func: &mut F) -> Result<WalkControl>
where
    F: FnMut(&mut ast::Expr) -> Result<WalkControl>,
{
    match func(expr)? {
        WalkControl::Continue => {
            match expr {
                ast::Expr::SubqueryResult { lhs, .. } => {
                    if let Some(lhs) = lhs {
                        walk_expr_mut(lhs, func)?;
                    }
                }
                ast::Expr::Between {
                    lhs, start, end, ..
                } => {
                    walk_expr_mut(lhs, func)?;
                    walk_expr_mut(start, func)?;
                    walk_expr_mut(end, func)?;
                }
                ast::Expr::Binary(lhs, _, rhs) => {
                    walk_expr_mut(lhs, func)?;
                    walk_expr_mut(rhs, func)?;
                }
                ast::Expr::Case {
                    base,
                    when_then_pairs,
                    else_expr,
                } => {
                    if let Some(base_expr) = base {
                        walk_expr_mut(base_expr, func)?;
                    }
                    for (when_expr, then_expr) in when_then_pairs {
                        walk_expr_mut(when_expr, func)?;
                        walk_expr_mut(then_expr, func)?;
                    }
                    if let Some(else_expr) = else_expr {
                        walk_expr_mut(else_expr, func)?;
                    }
                }
                ast::Expr::Cast { expr, .. } => {
                    walk_expr_mut(expr, func)?;
                }
                ast::Expr::Collate(expr, _) => {
                    walk_expr_mut(expr, func)?;
                }
                ast::Expr::Exists(_) | ast::Expr::Subquery(_) => {
                    // TODO: Walk through select statements if needed
                }
                ast::Expr::FunctionCall {
                    args,
                    order_by,
                    filter_over,
                    ..
                } => {
                    for arg in args {
                        walk_expr_mut(arg, func)?;
                    }
                    for sort_col in order_by {
                        walk_expr_mut(&mut sort_col.expr, func)?;
                    }
                    if let Some(filter_clause) = &mut filter_over.filter_clause {
                        walk_expr_mut(filter_clause, func)?;
                    }
                    if let Some(over_clause) = &mut filter_over.over_clause {
                        match over_clause {
                            ast::Over::Window(window) => {
                                for part_expr in &mut window.partition_by {
                                    walk_expr_mut(part_expr, func)?;
                                }
                                for sort_col in &mut window.order_by {
                                    walk_expr_mut(&mut sort_col.expr, func)?;
                                }
                                if let Some(frame_clause) = &mut window.frame_clause {
                                    walk_expr_mut_frame_bound(&mut frame_clause.start, func)?;
                                    if let Some(end_bound) = &mut frame_clause.end {
                                        walk_expr_mut_frame_bound(end_bound, func)?;
                                    }
                                }
                            }
                            ast::Over::Name(_) => {}
                        }
                    }
                }
                ast::Expr::FunctionCallStar { filter_over, .. } => {
                    if let Some(ref mut filter_clause) = filter_over.filter_clause {
                        walk_expr_mut(filter_clause, func)?;
                    }
                    if let Some(ref mut over_clause) = filter_over.over_clause {
                        match over_clause {
                            ast::Over::Window(window) => {
                                for part_expr in &mut window.partition_by {
                                    walk_expr_mut(part_expr, func)?;
                                }
                                for sort_col in &mut window.order_by {
                                    walk_expr_mut(&mut sort_col.expr, func)?;
                                }
                                if let Some(frame_clause) = &mut window.frame_clause {
                                    walk_expr_mut_frame_bound(&mut frame_clause.start, func)?;
                                    if let Some(end_bound) = &mut frame_clause.end {
                                        walk_expr_mut_frame_bound(end_bound, func)?;
                                    }
                                }
                            }
                            ast::Over::Name(_) => {}
                        }
                    }
                }
                ast::Expr::InList { lhs, rhs, .. } => {
                    walk_expr_mut(lhs, func)?;
                    for expr in rhs {
                        walk_expr_mut(expr, func)?;
                    }
                }
                ast::Expr::InSelect { lhs, rhs: _, .. } => {
                    walk_expr_mut(lhs, func)?;
                    // TODO: Walk through select statements if needed
                }
                ast::Expr::InTable { lhs, args, .. } => {
                    walk_expr_mut(lhs, func)?;
                    for expr in args {
                        walk_expr_mut(expr, func)?;
                    }
                }
                ast::Expr::IsNull(expr) | ast::Expr::NotNull(expr) => {
                    walk_expr_mut(expr, func)?;
                }
                ast::Expr::Like {
                    lhs, rhs, escape, ..
                } => {
                    walk_expr_mut(lhs, func)?;
                    walk_expr_mut(rhs, func)?;
                    if let Some(esc_expr) = escape {
                        walk_expr_mut(esc_expr, func)?;
                    }
                }
                ast::Expr::Parenthesized(exprs) => {
                    for expr in exprs {
                        walk_expr_mut(expr, func)?;
                    }
                }
                ast::Expr::Raise(_, expr) => {
                    if let Some(raise_expr) = expr {
                        walk_expr_mut(raise_expr, func)?;
                    }
                }
                ast::Expr::Unary(_, expr) => {
                    walk_expr_mut(expr, func)?;
                }
                ast::Expr::Array { .. } | ast::Expr::Subscript { .. } => {
                    unreachable!(
                        "Array and Subscript are desugared into function calls by the parser"
                    )
                }
                ast::Expr::Id(_)
                | ast::Expr::Column { .. }
                | ast::Expr::RowId { .. }
                | ast::Expr::Literal(_)
                | ast::Expr::DoublyQualified(..)
                | ast::Expr::Name(_)
                | ast::Expr::Qualified(..)
                | ast::Expr::Variable(_)
                | ast::Expr::Register(_)
                | ast::Expr::Default => {
                    // No nested expressions
                }
                ast::Expr::FieldAccess { base, .. } => {
                    walk_expr_mut(base, func)?;
                }
            }
        }
        WalkControl::SkipChildren => return Ok(WalkControl::Continue),
    };
    Ok(WalkControl::Continue)
}

pub(super) fn walk_expr_mut_frame_bound<F>(
    bound: &mut ast::FrameBound,
    func: &mut F,
) -> Result<WalkControl>
where
    F: FnMut(&mut ast::Expr) -> Result<WalkControl>,
{
    match bound {
        ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) => {
            walk_expr_mut(expr, func)?;
        }
        ast::FrameBound::CurrentRow
        | ast::FrameBound::UnboundedFollowing
        | ast::FrameBound::UnboundedPreceding => {}
    }

    Ok(WalkControl::Continue)
}
