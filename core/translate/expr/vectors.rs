use super::*;
use std::collections::HashMap;

/// Get the number of values returned by an expression
pub fn expr_vector_size(expr: &Expr) -> Result<usize> {
    enum Frame<'a> {
        Visit(&'a Expr),
        Eval(&'a Expr),
    }

    fn unwrap_single_parens(mut expr: &Expr) -> &Expr {
        while let Expr::Parenthesized(exprs) = expr {
            if exprs.len() != 1 {
                break;
            }
            expr = exprs[0].as_ref();
        }
        expr
    }

    fn key(expr: &Expr) -> *const Expr {
        unwrap_single_parens(expr) as *const Expr
    }

    fn size_of(sizes: &HashMap<*const Expr, usize>, expr: &Expr) -> usize {
        sizes[&key(expr)]
    }

    let root = unwrap_single_parens(expr);
    let mut stack = vec![Frame::Visit(root)];
    let mut sizes = HashMap::new();

    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Visit(expr) => {
                let expr = unwrap_single_parens(expr);
                if sizes.contains_key(&(expr as *const Expr)) {
                    continue;
                }

                stack.push(Frame::Eval(expr));
                match expr {
                    Expr::Between {
                        lhs, start, end, ..
                    } => {
                        stack.push(Frame::Visit(end));
                        stack.push(Frame::Visit(start));
                        stack.push(Frame::Visit(lhs));
                    }
                    Expr::Binary(lhs, _, rhs) => {
                        stack.push(Frame::Visit(rhs));
                        stack.push(Frame::Visit(lhs));
                    }
                    Expr::Case {
                        base,
                        when_then_pairs,
                        else_expr,
                    } => {
                        if let Some(else_expr) = else_expr {
                            stack.push(Frame::Visit(else_expr));
                        }
                        for (when, then) in when_then_pairs.iter().rev() {
                            stack.push(Frame::Visit(then));
                            stack.push(Frame::Visit(when));
                        }
                        if let Some(base) = base {
                            stack.push(Frame::Visit(base));
                        }
                    }
                    Expr::Cast { expr, .. }
                    | Expr::Collate(expr, _)
                    | Expr::IsNull(expr)
                    | Expr::NotNull(expr)
                    | Expr::Unary(_, expr) => {
                        stack.push(Frame::Visit(expr));
                    }
                    Expr::FunctionCall { args, .. } => {
                        for arg in args.iter().rev() {
                            stack.push(Frame::Visit(arg));
                        }
                    }
                    Expr::InList { lhs, rhs, .. } => {
                        for rhs in rhs.iter().rev() {
                            stack.push(Frame::Visit(rhs));
                        }
                        stack.push(Frame::Visit(lhs));
                    }
                    Expr::Like { lhs, rhs, .. } => {
                        stack.push(Frame::Visit(rhs));
                        stack.push(Frame::Visit(lhs));
                    }
                    Expr::Register(_)
                    | Expr::DoublyQualified(..)
                    | Expr::Exists(_)
                    | Expr::FunctionCallStar { .. }
                    | Expr::Id(_)
                    | Expr::Column { .. }
                    | Expr::RowId { .. }
                    | Expr::InSelect { .. }
                    | Expr::InTable { .. }
                    | Expr::Literal(_)
                    | Expr::Name(_)
                    | Expr::Parenthesized(_)
                    | Expr::Qualified(..)
                    | Expr::FieldAccess { .. }
                    | Expr::Raise(..)
                    | Expr::Subquery(_)
                    | Expr::Variable(_)
                    | Expr::SubqueryResult { .. }
                    | Expr::Default
                    | Expr::Array { .. }
                    | Expr::Subscript { .. } => {}
                }
            }
            Frame::Eval(expr) => {
                let size = match expr {
                    Expr::Between {
                        lhs, start, end, ..
                    } => {
                        let evs_left = size_of(&sizes, lhs);
                        let evs_start = size_of(&sizes, start);
                        let evs_end = size_of(&sizes, end);
                        if evs_left != evs_start || evs_left != evs_end {
                            crate::bail_parse_error!(
                                "all arguments to BETWEEN must return the same number of values. Got: ({evs_left}) BETWEEN ({evs_start}) AND ({evs_end})"
                            );
                        }
                        1
                    }
                    Expr::Binary(lhs, operator, rhs) => {
                        let evs_left = size_of(&sizes, lhs);
                        let evs_right = size_of(&sizes, rhs);
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
                            let evs_base = size_of(&sizes, base);
                            if evs_base != 1 {
                                crate::bail_parse_error!(
                                    "base expression in CASE must return 1 value. Got: ({evs_base})"
                                );
                            }
                        }
                        for (when, then) in when_then_pairs {
                            let evs_when = size_of(&sizes, when);
                            if evs_when != 1 {
                                crate::bail_parse_error!(
                                    "when expression in CASE must return 1 value. Got: ({evs_when})"
                                );
                            }
                            let evs_then = size_of(&sizes, then);
                            if evs_then != 1 {
                                crate::bail_parse_error!(
                                    "then expression in CASE must return 1 value. Got: ({evs_then})"
                                );
                            }
                        }
                        if let Some(else_expr) = else_expr {
                            let evs_else_expr = size_of(&sizes, else_expr);
                            if evs_else_expr != 1 {
                                crate::bail_parse_error!(
                                    "else expression in CASE must return 1 value. Got: ({evs_else_expr})"
                                );
                            }
                        }
                        1
                    }
                    Expr::Cast { expr, .. } => {
                        let evs_expr = size_of(&sizes, expr);
                        if evs_expr != 1 {
                            crate::bail_parse_error!(
                                "argument to CAST must return 1 value. Got: ({evs_expr})"
                            );
                        }
                        1
                    }
                    Expr::Collate(expr, _) => {
                        let evs_expr = size_of(&sizes, expr);
                        if evs_expr != 1 {
                            crate::bail_parse_error!(
                                "argument to COLLATE must return 1 value. Got: ({evs_expr})"
                            );
                        }
                        1
                    }
                    Expr::DoublyQualified(..) => 1,
                    Expr::Exists(_) => 1,
                    Expr::FunctionCall { name, args, .. } => {
                        for (pos, arg) in args.iter().enumerate() {
                            let evs_arg = size_of(&sizes, arg);
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
                        let evs_lhs = size_of(&sizes, lhs);
                        for rhs in rhs.iter() {
                            let evs_rhs = size_of(&sizes, rhs);
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
                        let evs_expr = size_of(&sizes, expr);
                        if evs_expr != 1 {
                            crate::bail_parse_error!(
                                "argument to IS NULL must return 1 value. Got: ({evs_expr})"
                            );
                        }
                        1
                    }
                    Expr::Like { lhs, rhs, op, .. } => {
                        let evs_lhs = size_of(&sizes, lhs);
                        if evs_lhs != 1 && *op != ast::LikeOperator::Match {
                            crate::bail_parse_error!(
                                "left operand of LIKE must return 1 value. Got: ({evs_lhs})"
                            );
                        }
                        let evs_rhs = size_of(&sizes, rhs);
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
                        let evs_expr = size_of(&sizes, expr);
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
                        let evs_expr = size_of(&sizes, expr);
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
                        unreachable!(
                            "Array and Subscript are desugared into function calls by the parser"
                        )
                    }
                };
                sizes.insert(expr as *const Expr, size);
            }
        }
    }

    Ok(sizes[&(root as *const Expr)])
}
