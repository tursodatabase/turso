use super::*;
use crate::function::{Deterministic, ExtFunc};

pub enum WalkControl {
    Continue,     // Visit children
    SkipChildren, // Skip children but continue walking siblings
}

/// Recursively walks an immutable expression, applying a function to each sub-expression.
pub fn walk_expr<'a, F>(expr: &'a ast::Expr, func: &mut F) -> Result<WalkControl>
where
    F: FnMut(&'a ast::Expr) -> Result<WalkControl>,
{
    enum WalkItem<'a> {
        Expr(&'a ast::Expr),
        FrameBound(&'a ast::FrameBound),
    }

    type Stack<'b> = smallvec::SmallVec<[WalkItem<'b>; 4]>;

    let mut stack: Stack<'a> = smallvec::smallvec![WalkItem::Expr(expr)];
    let push_over_clause_walk_items = |stack: &mut Stack<'a>, over_clause: &'a ast::Over| {
        if let ast::Over::Window(window) = over_clause {
            if let Some(frame_clause) = &window.frame_clause {
                if let Some(end_bound) = &frame_clause.end {
                    stack.push(WalkItem::FrameBound(end_bound));
                }
                stack.push(WalkItem::FrameBound(&frame_clause.start));
            }
            for sort_col in window.order_by.iter().rev() {
                stack.push(WalkItem::Expr(&sort_col.expr));
            }
            for part_expr in window.partition_by.iter().rev() {
                stack.push(WalkItem::Expr(part_expr));
            }
        }
    };
    while let Some(item) = stack.pop() {
        match item {
            WalkItem::Expr(expr) => {
                if matches!(func(expr)?, WalkControl::SkipChildren) {
                    continue;
                }
                match expr {
                    ast::Expr::SubqueryResult { lhs, .. } => {
                        if let Some(lhs) = lhs {
                            stack.push(WalkItem::Expr(lhs));
                        }
                    }
                    ast::Expr::Between {
                        lhs, start, end, ..
                    } => {
                        stack.push(WalkItem::Expr(end));
                        stack.push(WalkItem::Expr(start));
                        stack.push(WalkItem::Expr(lhs));
                    }
                    ast::Expr::Binary(lhs, _, rhs) => {
                        stack.push(WalkItem::Expr(rhs));
                        stack.push(WalkItem::Expr(lhs));
                    }
                    ast::Expr::Case {
                        base,
                        when_then_pairs,
                        else_expr,
                    } => {
                        if let Some(else_expr) = else_expr {
                            stack.push(WalkItem::Expr(else_expr));
                        }
                        for (when_expr, then_expr) in when_then_pairs.iter().rev() {
                            stack.push(WalkItem::Expr(then_expr));
                            stack.push(WalkItem::Expr(when_expr));
                        }
                        if let Some(base_expr) = base {
                            stack.push(WalkItem::Expr(base_expr));
                        }
                    }
                    ast::Expr::Cast { expr, .. } | ast::Expr::Collate(expr, _) => {
                        stack.push(WalkItem::Expr(expr));
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
                        if let Some(over_clause) = &filter_over.over_clause {
                            push_over_clause_walk_items(&mut stack, over_clause);
                        }
                        if let Some(filter_clause) = &filter_over.filter_clause {
                            stack.push(WalkItem::Expr(filter_clause));
                        }
                        for sort_col in order_by.iter().rev() {
                            stack.push(WalkItem::Expr(&sort_col.expr));
                        }
                        for arg in args.iter().rev() {
                            stack.push(WalkItem::Expr(arg));
                        }
                    }
                    ast::Expr::FunctionCallStar { filter_over, .. } => {
                        if let Some(over_clause) = &filter_over.over_clause {
                            push_over_clause_walk_items(&mut stack, over_clause);
                        }
                        if let Some(filter_clause) = &filter_over.filter_clause {
                            stack.push(WalkItem::Expr(filter_clause));
                        }
                    }
                    ast::Expr::InList { lhs, rhs, .. } => {
                        for expr in rhs.iter().rev() {
                            stack.push(WalkItem::Expr(expr));
                        }
                        stack.push(WalkItem::Expr(lhs));
                    }
                    ast::Expr::InSelect { lhs, rhs: _, .. } => {
                        stack.push(WalkItem::Expr(lhs));
                        // TODO: Walk through select statements if needed
                    }
                    ast::Expr::InTable { lhs, args, .. } => {
                        for expr in args.iter().rev() {
                            stack.push(WalkItem::Expr(expr));
                        }
                        stack.push(WalkItem::Expr(lhs));
                    }
                    ast::Expr::IsNull(expr) | ast::Expr::NotNull(expr) => {
                        stack.push(WalkItem::Expr(expr));
                    }
                    ast::Expr::Like {
                        lhs, rhs, escape, ..
                    } => {
                        if let Some(esc_expr) = escape {
                            stack.push(WalkItem::Expr(esc_expr));
                        }
                        stack.push(WalkItem::Expr(rhs));
                        stack.push(WalkItem::Expr(lhs));
                    }
                    ast::Expr::Parenthesized(exprs) => {
                        for expr in exprs.iter().rev() {
                            stack.push(WalkItem::Expr(expr));
                        }
                    }
                    ast::Expr::Raise(_, expr) => {
                        if let Some(raise_expr) = expr {
                            stack.push(WalkItem::Expr(raise_expr));
                        }
                    }
                    ast::Expr::Unary(_, expr) => {
                        stack.push(WalkItem::Expr(expr));
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
                    | ast::Expr::Default => {}
                    ast::Expr::FieldAccess { base, .. } => {
                        stack.push(WalkItem::Expr(base));
                    }
                }
            }
            WalkItem::FrameBound(bound) => match bound {
                ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) => {
                    stack.push(WalkItem::Expr(expr));
                }
                ast::FrameBound::CurrentRow
                | ast::FrameBound::UnboundedFollowing
                | ast::FrameBound::UnboundedPreceding => {}
            },
        }
    }
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

/// Returns true if this expression calls a scalar function whose result can
/// change between calls.
///
/// This is used when deciding whether two repeated window expressions can use
/// one `WindowFunction` entry. For example, two copies of `sum(x) OVER w` can
/// use one entry. Two copies of
/// `sum(x) FILTER (WHERE random() % 2 = 0) OVER w` cannot share the filter
/// value, because SQLite runs `random()` separately at each place it appears.
///
/// FIXME: `walk_expr` does not descend into `Expr::Subquery`, `Expr::Exists`,
/// or the `Select` side of `Expr::InSelect`. A nondeterministic call buried
/// inside such a subquery in a FILTER predicate will not be detected, and the
/// two FILTERs would be incorrectly deduped. Lift the walker's subquery TODO
/// before relying on this for correctness in those cases.
pub fn expr_contains_nondeterministic_scalar_function(
    expr: &ast::Expr,
    resolver: &Resolver<'_>,
) -> Result<bool> {
    fn is_nondeterministic_scalar_like_function(func: &Func) -> bool {
        match func {
            // Aggregate and window function calls themselves are not flagged
            // at any nesting depth — their outputs are deterministic given the
            // same input rows. The walker still descends into their args,
            // FILTER, and OVER subexprs, so nondet scalars buried inside
            // (e.g. `sum(random())`) are still caught. Note also that
            // `AggFunc::is_deterministic` returns `false`, so the catch-all
            // below would otherwise flag every aggregate call.
            Func::Agg(_) | Func::Window(_) => false,

            // User-defined scalar functions can do anything, so treat them
            // like `random()`. User-defined aggregates are treated like
            // built-in aggregates: two copies of `myagg(x) OVER w` should
            // share one window entry when `x` and the FILTER/OVER clauses are
            // stable.
            Func::External(external) if matches!(external.func, ExtFunc::Aggregate { .. }) => false,

            _ => !func.is_deterministic(),
        }
    }

    let mut found = false;
    walk_expr(expr, &mut |e: &ast::Expr| -> Result<WalkControl> {
        if found {
            return Ok(WalkControl::SkipChildren);
        }

        let func = match e {
            ast::Expr::FunctionCall { name, args, .. } => {
                resolver.resolve_function(name.as_str(), args.len())?
            }
            ast::Expr::FunctionCallStar { name, .. } => {
                resolver.resolve_function(name.as_str(), 0)?
            }
            _ => None,
        };

        // If the name is unknown here, leave the error to the normal resolver.
        // This helper only answers "may repeated copies share work?"
        if func
            .as_ref()
            .is_some_and(is_nondeterministic_scalar_like_function)
        {
            found = true;
            return Ok(WalkControl::SkipChildren);
        }

        Ok(WalkControl::Continue)
    })?;

    Ok(found)
}

/// Walks a mutable expression, applying a function to each sub-expression.
pub fn walk_expr_mut<'a, F>(expr: &'a mut ast::Expr, func: &mut F) -> Result<WalkControl>
where
    F: FnMut(&mut ast::Expr) -> Result<WalkControl>,
{
    enum WalkItem<'a> {
        Expr(&'a mut ast::Expr),
        FrameBound(&'a mut ast::FrameBound),
    }

    type Stack<'a> = smallvec::SmallVec<[WalkItem<'a>; 4]>;

    fn push_over_clause_walk_items<'a>(stack: &mut Stack<'a>, over_clause: &'a mut ast::Over) {
        if let ast::Over::Window(window) = over_clause {
            if let Some(frame_clause) = &mut window.frame_clause {
                if let Some(end_bound) = &mut frame_clause.end {
                    stack.push(WalkItem::FrameBound(end_bound));
                }
                stack.push(WalkItem::FrameBound(&mut frame_clause.start));
            }
            for sort_col in window.order_by.iter_mut().rev() {
                stack.push(WalkItem::Expr(&mut sort_col.expr));
            }
            for part_expr in window.partition_by.iter_mut().rev() {
                stack.push(WalkItem::Expr(part_expr));
            }
        }
    }

    let mut stack: Stack<'a> = smallvec::smallvec![WalkItem::Expr(expr)];
    while let Some(item) = stack.pop() {
        match item {
            WalkItem::Expr(expr) => {
                if matches!(func(expr)?, WalkControl::SkipChildren) {
                    continue;
                }
                match expr {
                    ast::Expr::SubqueryResult { lhs, .. } => {
                        if let Some(lhs) = lhs {
                            stack.push(WalkItem::Expr(lhs));
                        }
                    }
                    ast::Expr::Between {
                        lhs, start, end, ..
                    } => {
                        stack.push(WalkItem::Expr(end));
                        stack.push(WalkItem::Expr(start));
                        stack.push(WalkItem::Expr(lhs));
                    }
                    ast::Expr::Binary(lhs, _, rhs) => {
                        stack.push(WalkItem::Expr(rhs));
                        stack.push(WalkItem::Expr(lhs));
                    }
                    ast::Expr::Case {
                        base,
                        when_then_pairs,
                        else_expr,
                    } => {
                        if let Some(else_expr) = else_expr {
                            stack.push(WalkItem::Expr(else_expr));
                        }
                        for (when_expr, then_expr) in when_then_pairs.iter_mut().rev() {
                            stack.push(WalkItem::Expr(then_expr));
                            stack.push(WalkItem::Expr(when_expr));
                        }
                        if let Some(base_expr) = base {
                            stack.push(WalkItem::Expr(base_expr));
                        }
                    }
                    ast::Expr::Cast { expr, .. } | ast::Expr::Collate(expr, _) => {
                        stack.push(WalkItem::Expr(expr));
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
                        if let Some(over_clause) = &mut filter_over.over_clause {
                            push_over_clause_walk_items(&mut stack, over_clause);
                        }
                        if let Some(filter_clause) = &mut filter_over.filter_clause {
                            stack.push(WalkItem::Expr(filter_clause));
                        }
                        for sort_col in order_by.iter_mut().rev() {
                            stack.push(WalkItem::Expr(&mut sort_col.expr));
                        }
                        for arg in args.iter_mut().rev() {
                            stack.push(WalkItem::Expr(arg));
                        }
                    }
                    ast::Expr::FunctionCallStar { filter_over, .. } => {
                        if let Some(over_clause) = &mut filter_over.over_clause {
                            push_over_clause_walk_items(&mut stack, over_clause);
                        }
                        if let Some(filter_clause) = &mut filter_over.filter_clause {
                            stack.push(WalkItem::Expr(filter_clause));
                        }
                    }
                    ast::Expr::InList { lhs, rhs, .. } => {
                        for expr in rhs.iter_mut().rev() {
                            stack.push(WalkItem::Expr(expr));
                        }
                        stack.push(WalkItem::Expr(lhs));
                    }
                    ast::Expr::InSelect { lhs, rhs: _, .. } => {
                        stack.push(WalkItem::Expr(lhs));
                        // TODO: Walk through select statements if needed
                    }
                    ast::Expr::InTable { lhs, args, .. } => {
                        for expr in args.iter_mut().rev() {
                            stack.push(WalkItem::Expr(expr));
                        }
                        stack.push(WalkItem::Expr(lhs));
                    }
                    ast::Expr::IsNull(expr) | ast::Expr::NotNull(expr) => {
                        stack.push(WalkItem::Expr(expr));
                    }
                    ast::Expr::Like {
                        lhs, rhs, escape, ..
                    } => {
                        if let Some(esc_expr) = escape {
                            stack.push(WalkItem::Expr(esc_expr));
                        }
                        stack.push(WalkItem::Expr(rhs));
                        stack.push(WalkItem::Expr(lhs));
                    }
                    ast::Expr::Parenthesized(exprs) => {
                        for expr in exprs.iter_mut().rev() {
                            stack.push(WalkItem::Expr(expr));
                        }
                    }
                    ast::Expr::Raise(_, expr) => {
                        if let Some(raise_expr) = expr {
                            stack.push(WalkItem::Expr(raise_expr));
                        }
                    }
                    ast::Expr::Unary(_, expr) => {
                        stack.push(WalkItem::Expr(expr));
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
                    | ast::Expr::Default => {}
                    ast::Expr::FieldAccess { base, .. } => {
                        stack.push(WalkItem::Expr(base));
                    }
                }
            }
            WalkItem::FrameBound(bound) => match bound {
                ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) => {
                    stack.push(WalkItem::Expr(expr));
                }
                ast::FrameBound::CurrentRow
                | ast::FrameBound::UnboundedFollowing
                | ast::FrameBound::UnboundedPreceding => {}
            },
        }
    }
    Ok(WalkControl::Continue)
}
