use super::*;

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
                crate::bail_parse_error!(
                    "all arguments to BETWEEN must return the same number of values. Got: ({evs_left}) BETWEEN ({evs_start}) AND ({evs_end})"
                );
            }
            1
        }
        Expr::Binary(expr, operator, expr1) => {
            let evs_left = expr_vector_size(expr)?;
            let evs_right = expr_vector_size(expr1)?;
            if evs_left != evs_right {
                crate::bail_parse_error!(
                    "all arguments to binary operator {operator} must return the same number of values. Got: ({evs_left}) {operator} ({evs_right})"
                );
            }
            if evs_left > 1 && !supports_row_value_binary_comparison(operator) {
                crate::bail_parse_error!("row value misused");
            }
            1
        }
        Expr::Register(_) => 1,
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                let evs_base = expr_vector_size(base)?;
                if evs_base != 1 {
                    crate::bail_parse_error!(
                        "base expression in CASE must return 1 value. Got: ({evs_base})"
                    );
                }
            }
            for (when, then) in when_then_pairs {
                let evs_when = expr_vector_size(when)?;
                if evs_when != 1 {
                    crate::bail_parse_error!(
                        "when expression in CASE must return 1 value. Got: ({evs_when})"
                    );
                }
                let evs_then = expr_vector_size(then)?;
                if evs_then != 1 {
                    crate::bail_parse_error!(
                        "then expression in CASE must return 1 value. Got: ({evs_then})"
                    );
                }
            }
            if let Some(else_expr) = else_expr {
                let evs_else_expr = expr_vector_size(else_expr)?;
                if evs_else_expr != 1 {
                    crate::bail_parse_error!(
                        "else expression in CASE must return 1 value. Got: ({evs_else_expr})"
                    );
                }
            }
            1
        }
        Expr::Cast { expr, .. } => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!("argument to CAST must return 1 value. Got: ({evs_expr})");
            }
            1
        }
        Expr::Collate(expr, _) => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!(
                    "argument to COLLATE must return 1 value. Got: ({evs_expr})"
                );
            }
            1
        }
        Expr::DoublyQualified(..) => 1,
        Expr::Exists(_) => 1, // EXISTS returns a single boolean value (0 or 1)
        Expr::FunctionCall { name, args, .. } => {
            for (pos, arg) in args.iter().enumerate() {
                let evs_arg = expr_vector_size(arg)?;
                if evs_arg != 1 {
                    crate::bail_parse_error!(
                        "argument {} to function call {name} must return 1 value. Got: ({evs_arg})",
                        pos + 1
                    );
                }
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
                    crate::bail_parse_error!(
                        "all arguments to IN list must return the same number of values, got: ({evs_lhs}) IN ({evs_rhs})"
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
                crate::bail_parse_error!(
                    "argument to IS NULL must return 1 value. Got: ({evs_expr})"
                );
            }
            1
        }
        Expr::Like { lhs, rhs, op, .. } => {
            let evs_lhs = expr_vector_size(lhs)?;
            // MATCH allows multi-column LHS: (col1, col2) MATCH 'query'
            if evs_lhs != 1 && *op != ast::LikeOperator::Match {
                crate::bail_parse_error!(
                    "left operand of LIKE must return 1 value. Got: ({evs_lhs})"
                );
            }
            let evs_rhs = expr_vector_size(rhs)?;
            if evs_rhs != 1 {
                crate::bail_parse_error!(
                    "right operand of LIKE must return 1 value. Got: ({evs_rhs})"
                );
            }
            1
        }
        Expr::Literal(_) => 1,
        Expr::Name(_) => 1,
        Expr::NotNull(expr) => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!(
                    "argument to NOT NULL must return 1 value. Got: ({evs_expr})"
                );
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
        Expr::Unary(unary_operator, expr) => {
            let evs_expr = expr_vector_size(expr)?;
            if evs_expr != 1 {
                crate::bail_parse_error!(
                    "argument to unary operator {unary_operator} must return 1 value. Got: ({evs_expr})"
                );
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
