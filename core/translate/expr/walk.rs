use super::*;

const MAX_EXPR_DEPTH: usize = 1000;

pub enum WalkControl {
    Continue,     // Visit children
    SkipChildren, // Skip children but continue walking siblings
}

pub(crate) fn validate_expr_depth(expr: &ast::Expr) -> Result<()> {
    let mut stack = vec![(expr, 1usize)];

    while let Some((expr, depth)) = stack.pop() {
        if depth > MAX_EXPR_DEPTH {
            crate::bail_parse_error!(
                "Expression tree is too large (maximum depth {MAX_EXPR_DEPTH})"
            );
        }

        let child_depth = depth + 1;
        match expr {
            ast::Expr::SubqueryResult { lhs, .. } => {
                if let Some(lhs) = lhs.as_deref() {
                    stack.push((lhs, child_depth));
                }
            }
            ast::Expr::Between {
                lhs, start, end, ..
            } => {
                stack.push((end, child_depth));
                stack.push((start, child_depth));
                stack.push((lhs, child_depth));
            }
            ast::Expr::Binary(lhs, _, rhs) => {
                stack.push((rhs, child_depth));
                stack.push((lhs, child_depth));
            }
            ast::Expr::Case {
                base,
                when_then_pairs,
                else_expr,
            } => {
                if let Some(else_expr) = else_expr.as_deref() {
                    stack.push((else_expr, child_depth));
                }
                for (when_expr, then_expr) in when_then_pairs.iter().rev() {
                    stack.push((then_expr, child_depth));
                    stack.push((when_expr, child_depth));
                }
                if let Some(base) = base.as_deref() {
                    stack.push((base, child_depth));
                }
            }
            ast::Expr::Cast { expr, .. }
            | ast::Expr::Collate(expr, _)
            | ast::Expr::IsNull(expr)
            | ast::Expr::NotNull(expr)
            | ast::Expr::Unary(_, expr) => {
                stack.push((expr, child_depth));
            }
            ast::Expr::Exists(select) | ast::Expr::Subquery(select) => {
                validate_select_expr_depth(select)?;
            }
            ast::Expr::FunctionCall {
                args,
                order_by,
                filter_over,
                ..
            } => {
                push_function_tail_exprs_with_depth(&mut stack, filter_over, child_depth);
                for sort_col in order_by.iter().rev() {
                    stack.push((&sort_col.expr, child_depth));
                }
                for arg in args.iter().rev() {
                    stack.push((arg, child_depth));
                }
            }
            ast::Expr::FunctionCallStar { filter_over, .. } => {
                push_function_tail_exprs_with_depth(&mut stack, filter_over, child_depth);
            }
            ast::Expr::InList { lhs, rhs, .. } => {
                for expr in rhs.iter().rev() {
                    stack.push((expr, child_depth));
                }
                stack.push((lhs, child_depth));
            }
            ast::Expr::InSelect { lhs, rhs, .. } => {
                validate_select_expr_depth(rhs)?;
                stack.push((lhs, child_depth));
            }
            ast::Expr::InTable { lhs, args, .. } => {
                for expr in args.iter().rev() {
                    stack.push((expr, child_depth));
                }
                stack.push((lhs, child_depth));
            }
            ast::Expr::Like {
                lhs, rhs, escape, ..
            } => {
                if let Some(escape) = escape.as_deref() {
                    stack.push((escape, child_depth));
                }
                stack.push((rhs, child_depth));
                stack.push((lhs, child_depth));
            }
            ast::Expr::Parenthesized(exprs) => {
                for expr in exprs.iter().rev() {
                    stack.push((expr, child_depth));
                }
            }
            ast::Expr::Raise(_, expr) => {
                if let Some(expr) = expr.as_deref() {
                    stack.push((expr, child_depth));
                }
            }
            ast::Expr::Array { elements } => {
                for expr in elements.iter().rev() {
                    stack.push((expr, child_depth));
                }
            }
            ast::Expr::Subscript { base, index } => {
                stack.push((index, child_depth));
                stack.push((base, child_depth));
            }
            ast::Expr::FieldAccess { base, .. } => {
                stack.push((base, child_depth));
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
        }
    }

    Ok(())
}

pub(crate) fn validate_select_expr_depth(select: &ast::Select) -> Result<()> {
    let mut selects = vec![select];

    while let Some(select) = selects.pop() {
        if let Some(with) = &select.with {
            for cte in with.ctes.iter().rev() {
                selects.push(&cte.select);
            }
        }

        validate_one_select_expr_depth(&select.body.select, &mut selects)?;
        for compound in &select.body.compounds {
            validate_one_select_expr_depth(&compound.select, &mut selects)?;
        }
        validate_sorted_columns_expr_depth(&select.order_by)?;
        if let Some(limit) = &select.limit {
            validate_limit_expr_depth(limit)?;
        }
    }

    Ok(())
}

fn validate_one_select_expr_depth<'a>(
    select: &'a ast::OneSelect,
    selects: &mut Vec<&'a ast::Select>,
) -> Result<()> {
    match select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            window_clause,
            ..
        } => {
            validate_result_columns_expr_depth(columns)?;
            if let Some(from) = from {
                validate_from_clause_expr_depth(from, selects)?;
            }
            if let Some(where_clause) = where_clause {
                validate_expr_depth(where_clause)?;
            }
            if let Some(group_by) = group_by {
                for expr in &group_by.exprs {
                    validate_expr_depth(expr)?;
                }
                if let Some(having) = &group_by.having {
                    validate_expr_depth(having)?;
                }
            }
            for window in window_clause {
                for expr in &window.window.partition_by {
                    validate_expr_depth(expr)?;
                }
                validate_sorted_columns_expr_depth(&window.window.order_by)?;
                if let Some(frame_clause) = &window.window.frame_clause {
                    validate_frame_bound_expr_depth(&frame_clause.start)?;
                    if let Some(end) = &frame_clause.end {
                        validate_frame_bound_expr_depth(end)?;
                    }
                }
            }
        }
        ast::OneSelect::Values(rows) => {
            for row in rows {
                for expr in row {
                    validate_expr_depth(expr)?;
                }
            }
        }
    }

    Ok(())
}

pub(crate) fn validate_from_clause_expr_depth<'a>(
    from: &'a ast::FromClause,
    selects: &mut Vec<&'a ast::Select>,
) -> Result<()> {
    validate_select_table_expr_depth(&from.select, selects)?;
    for join in &from.joins {
        validate_select_table_expr_depth(&join.table, selects)?;
        if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
            validate_expr_depth(expr)?;
        }
    }

    Ok(())
}

fn validate_select_table_expr_depth<'a>(
    table: &'a ast::SelectTable,
    selects: &mut Vec<&'a ast::Select>,
) -> Result<()> {
    match table {
        ast::SelectTable::Table(_, _, _) => {}
        ast::SelectTable::TableCall(_, args, _) => {
            for arg in args {
                validate_expr_depth(arg)?;
            }
        }
        ast::SelectTable::Select(select, _) => selects.push(select),
        ast::SelectTable::Sub(from, _) => validate_from_clause_expr_depth(from, selects)?,
    }

    Ok(())
}

pub(crate) fn validate_result_columns_expr_depth(columns: &[ast::ResultColumn]) -> Result<()> {
    for column in columns {
        if let ast::ResultColumn::Expr(expr, _) = column {
            validate_expr_depth(expr)?;
        }
    }

    Ok(())
}

pub(crate) fn validate_sorted_columns_expr_depth(columns: &[ast::SortedColumn]) -> Result<()> {
    for column in columns {
        validate_expr_depth(&column.expr)?;
    }

    Ok(())
}

pub(crate) fn validate_limit_expr_depth(limit: &ast::Limit) -> Result<()> {
    validate_expr_depth(&limit.expr)?;
    if let Some(offset) = &limit.offset {
        validate_expr_depth(offset)?;
    }

    Ok(())
}

fn validate_frame_bound_expr_depth(bound: &ast::FrameBound) -> Result<()> {
    match bound {
        ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) => {
            validate_expr_depth(expr)?;
        }
        ast::FrameBound::CurrentRow
        | ast::FrameBound::UnboundedFollowing
        | ast::FrameBound::UnboundedPreceding => {}
    }

    Ok(())
}

fn push_function_tail_exprs_with_depth<'a>(
    stack: &mut Vec<(&'a ast::Expr, usize)>,
    tail: &'a ast::FunctionTail,
    depth: usize,
) {
    if let Some(filter_clause) = tail.filter_clause.as_deref() {
        stack.push((filter_clause, depth));
    }
    if let Some(ast::Over::Window(window)) = &tail.over_clause {
        if let Some(frame_clause) = &window.frame_clause {
            push_frame_bound_expr_with_depth(stack, &frame_clause.start, depth);
            if let Some(end_bound) = &frame_clause.end {
                push_frame_bound_expr_with_depth(stack, end_bound, depth);
            }
        }
        for sort_col in window.order_by.iter().rev() {
            stack.push((&sort_col.expr, depth));
        }
        for part_expr in window.partition_by.iter().rev() {
            stack.push((part_expr, depth));
        }
    }
}

fn push_frame_bound_expr_with_depth<'a>(
    stack: &mut Vec<(&'a ast::Expr, usize)>,
    bound: &'a ast::FrameBound,
    depth: usize,
) {
    match bound {
        ast::FrameBound::Following(expr) | ast::FrameBound::Preceding(expr) => {
            stack.push((expr, depth));
        }
        ast::FrameBound::CurrentRow
        | ast::FrameBound::UnboundedFollowing
        | ast::FrameBound::UnboundedPreceding => {}
    }
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
