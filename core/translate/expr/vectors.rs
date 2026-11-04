use super::*;

/// Fails when a comparison, BETWEEN or IN list of this expression puts row
/// values of different sizes side by side, as SQLite's resolver does before
/// anything is folded: `1 OR (1, 2) = (1, 2, 3)` is "row value misused".
/// A row value in any other place, like `(1, 2) + 1`, is only reported when
/// it is compiled, so it is left alone here.
pub fn reject_row_value_misuse(expr: &Expr) -> Result<()> {
    walk_expr(expr, &mut |e| {
        match e {
            Expr::Binary(lhs, op, rhs) if is_comparison(op) => {
                if let (Some(left), Some(right)) = (row_value_width(lhs), row_value_width(rhs)) {
                    if left != right {
                        crate::bail_parse_error!("row value misused");
                    }
                }
            }
            Expr::Between {
                lhs, start, end, ..
            } => {
                if let (Some(left), Some(start), Some(end)) = (
                    row_value_width(lhs),
                    row_value_width(start),
                    row_value_width(end),
                ) {
                    if left != start || left != end {
                        crate::bail_parse_error!("row value misused");
                    }
                }
            }
            Expr::InList { lhs, rhs, .. } => {
                let Some(left) = row_value_width(lhs) else {
                    return Ok(WalkControl::Continue);
                };
                for element in rhs.iter() {
                    let Some(width) = row_value_width(element) else {
                        continue;
                    };
                    if width == left {
                        continue;
                    }
                    if left == 1 {
                        crate::bail_parse_error!("row value misused");
                    }
                    crate::bail_parse_error!(
                        "IN(...) element has {width} term{} - expected {left}",
                        if width == 1 { "" } else { "s" }
                    );
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

fn is_comparison(op: &ast::Operator) -> bool {
    use ast::Operator::*;
    matches!(
        *op,
        Equals | NotEquals | Less | LessEquals | Greater | GreaterEquals | Is | IsNot
    )
}

/// How many values this operand stands for, when that is known before it is
/// bound: a parenthesized list, a subquery with named result columns, or one
/// value. None for a subquery with a `*` to expand.
fn row_value_width(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Parenthesized(exprs) => Some(exprs.len()),
        Expr::Subquery(select) => select_column_count(select),
        Expr::SubqueryResult {
            query_type: SubqueryType::RowValue { num_regs, .. },
            ..
        } => Some(*num_regs),
        _ => Some(1),
    }
}

/// The number of result columns of a select, or None when a `*` has to be
/// expanded to know it.
pub(crate) fn select_column_count(select: &ast::Select) -> Option<usize> {
    match &select.body.select {
        ast::OneSelect::Select { columns, .. } => columns
            .iter()
            .all(|column| matches!(column, ast::ResultColumn::Expr(..)))
            .then_some(columns.len()),
        ast::OneSelect::Values(rows) => rows.first().map(|row| row.len()),
    }
}

/// Get the number of values returned by an expression
pub fn expr_vector_size(expr: &Expr) -> Result<usize> {
    Ok(match unwrap_parens(expr)? {
        Expr::Between {
            lhs, start, end, ..
        } => {
            let evs_left = expr_vector_size(lhs)?;
            let evs_start = expr_vector_size(start)?;
            let evs_end = expr_vector_size(end)?;
            if evs_left != evs_start || evs_left != evs_end {
                crate::bail_parse_error!("row value misused");
            }
            1
        }
        Expr::Binary(expr, operator, expr1) => {
            let evs_left = expr_vector_size(expr)?;
            let evs_right = expr_vector_size(expr1)?;
            if evs_left != evs_right {
                crate::bail_parse_error!("row value misused");
            }
            if evs_left > 1 && !supports_row_value_binary_comparison(operator) {
                crate::bail_parse_error!("row value misused");
            }
            1
        }
        Expr::Register { .. } => 1,
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                let evs_base = expr_vector_size(base)?;
                if evs_base != 1 {
                    crate::bail_parse_error!("row value misused");
                }
            }
            for (when, then) in when_then_pairs {
                let evs_when = expr_vector_size(when)?;
                if evs_when != 1 {
                    crate::bail_parse_error!("row value misused");
                }
                let evs_then = expr_vector_size(then)?;
                if evs_then != 1 {
                    crate::bail_parse_error!("row value misused");
                }
            }
            if let Some(else_expr) = else_expr {
                let evs_else_expr = expr_vector_size(else_expr)?;
                if evs_else_expr != 1 {
                    crate::bail_parse_error!("row value misused");
                }
            }
            1
        }
        Expr::Cast { expr, .. } => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!("row value misused");
            }
            1
        }
        Expr::Collate(expr, _) => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!("row value misused");
            }
            1
        }
        Expr::DoublyQualified(..) => 1,
        Expr::Exists(_) => 1, // EXISTS returns a single boolean value (0 or 1)
        Expr::FunctionCall { args, .. } => {
            for arg in args.iter() {
                let evs_arg = expr_vector_size(arg)?;
                if evs_arg != 1 {
                    crate::bail_parse_error!("row value misused");
                }
            }
            1
        }
        Expr::MergedColumn(columns) => {
            // A merged USING column holds plain table columns, so it is never a row value.
            for column in columns {
                turso_assert!(
                    expr_vector_size(column)? == 1,
                    "a merged column must hold single values"
                );
            }
            1
        }
        Expr::FunctionCallStar { .. } => 1,
        Expr::Id(_) => 1,
        Expr::Column { .. } => 1,
        Expr::RowId { .. } => 1,
        Expr::InList { lhs, rhs, .. } => {
            let evs_lhs = expr_vector_size(lhs)?;
            for rhs in rhs.iter() {
                let evs_rhs = expr_vector_size(rhs)?;
                if evs_lhs != evs_rhs {
                    // SQLite reports "row value misused" for a scalar LHS, but a
                    // dedicated arity error when the LHS is a row value.
                    if evs_lhs == 1 {
                        crate::bail_parse_error!("row value misused");
                    }
                    crate::bail_parse_error!(
                        "IN(...) element has {evs_rhs} term{} - expected {evs_lhs}",
                        if evs_rhs == 1 { "" } else { "s" }
                    );
                }
            }
            1
        }
        Expr::InSelect { .. } => {
            crate::bail_parse_error!("InSelect is not supported in this position")
        }
        Expr::InTable { .. } => {
            crate::bail_parse_error!("InTable is not supported in this position")
        }
        Expr::IsNull(expr) => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!("row value misused");
            }
            1
        }
        Expr::Like { lhs, rhs, op, .. } => {
            let evs_lhs = expr_vector_size(lhs)?;
            // MATCH allows multi-column LHS: (col1, col2) MATCH 'query'
            if evs_lhs != 1 && *op != ast::LikeOperator::Match {
                crate::bail_parse_error!("row value misused");
            }
            let evs_rhs = expr_vector_size(rhs)?;
            if evs_rhs != 1 {
                crate::bail_parse_error!("row value misused");
            }
            1
        }
        Expr::Literal(_) => 1,
        Expr::Name(_) => 1,
        Expr::NotNull(expr) => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!("row value misused");
            }
            1
        }
        Expr::Parenthesized(exprs) => exprs.len(),
        Expr::Qualified(..) => 1,
        Expr::FieldAccess { .. } => 1,
        Expr::Raise(..) => 1,
        Expr::Subquery(_) => {
            crate::bail_parse_error!("Scalar subquery is not supported in this context")
        }
        Expr::Unary(_, expr) => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!("row value misused");
            }
            1
        }
        Expr::Variable(_) => 1,
        Expr::SubqueryResult { query_type, .. } => match query_type {
            SubqueryType::Exists { .. } => 1,
            SubqueryType::In { .. } => 1,
            SubqueryType::RowValue { num_regs, .. } => *num_regs,
        },
        Expr::Default => 1,
        Expr::Array { .. } | Expr::Subscript { .. } => {
            unreachable!("Array and Subscript are desugared into function calls by the parser")
        }
    })
}
