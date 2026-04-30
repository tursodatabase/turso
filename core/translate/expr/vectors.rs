use super::*;

/// Get the number of values returned by an expression
pub fn expr_vector_size(expr: &Expr) -> Result<usize> {
    enum Work<'a> {
        Visit(&'a Expr),
        Reduce(&'a Expr, usize),
    }

    let mut work = vec![Work::Visit(expr)];
    let mut values = Vec::new();

    while let Some(item) = work.pop() {
        match item {
            Work::Visit(expr) => {
                let expr = unwrap_parens(expr)?;
                match expr {
                    Expr::Between {
                        lhs, start, end, ..
                    } => {
                        work.push(Work::Reduce(expr, 3));
                        work.push(Work::Visit(end));
                        work.push(Work::Visit(start));
                        work.push(Work::Visit(lhs));
                    }
                    Expr::Binary(lhs, _, rhs) => {
                        work.push(Work::Reduce(expr, 2));
                        work.push(Work::Visit(rhs));
                        work.push(Work::Visit(lhs));
                    }
                    Expr::Case {
                        base,
                        when_then_pairs,
                        else_expr,
                    } => {
                        let child_count = base.iter().count()
                            + when_then_pairs.len() * 2
                            + else_expr.iter().count();
                        work.push(Work::Reduce(expr, child_count));
                        if let Some(else_expr) = else_expr {
                            work.push(Work::Visit(else_expr));
                        }
                        for (when, then) in when_then_pairs.iter().rev() {
                            work.push(Work::Visit(then));
                            work.push(Work::Visit(when));
                        }
                        if let Some(base) = base {
                            work.push(Work::Visit(base));
                        }
                    }
                    Expr::Cast { expr: inner, .. }
                    | Expr::Collate(inner, _)
                    | Expr::IsNull(inner)
                    | Expr::NotNull(inner)
                    | Expr::Unary(_, inner) => {
                        work.push(Work::Reduce(expr, 1));
                        work.push(Work::Visit(inner));
                    }
                    Expr::FunctionCall { args, .. } => {
                        work.push(Work::Reduce(expr, args.len()));
                        for arg in args.iter().rev() {
                            work.push(Work::Visit(arg));
                        }
                    }
                    Expr::InList { lhs, rhs, .. } => {
                        work.push(Work::Reduce(expr, rhs.len() + 1));
                        for rhs in rhs.iter().rev() {
                            work.push(Work::Visit(rhs));
                        }
                        work.push(Work::Visit(lhs));
                    }
                    Expr::Like { lhs, rhs, .. } => {
                        work.push(Work::Reduce(expr, 2));
                        work.push(Work::Visit(rhs));
                        work.push(Work::Visit(lhs));
                    }
                    Expr::Parenthesized(exprs) => values.push(exprs.len()),
                    Expr::InSelect { .. } => {
                        crate::bail_parse_error!("InSelect is not supported in this position")
                    }
                    Expr::InTable { .. } => {
                        crate::bail_parse_error!("InTable is not supported in this position")
                    }
                    Expr::Subquery(_) => {
                        crate::bail_parse_error!("Scalar subquery is not supported in this context")
                    }
                    Expr::SubqueryResult { query_type, .. } => values.push(match query_type {
                        SubqueryType::Exists { .. } | SubqueryType::In { .. } => 1,
                        SubqueryType::RowValue { num_regs, .. } => *num_regs,
                    }),
                    Expr::Array { .. } | Expr::Subscript { .. } => {
                        unreachable!(
                            "Array and Subscript are desugared into function calls by the parser"
                        )
                    }
                    Expr::Register(_)
                    | Expr::DoublyQualified(..)
                    | Expr::Exists(_)
                    | Expr::FunctionCallStar { .. }
                    | Expr::Id(_)
                    | Expr::Column { .. }
                    | Expr::RowId { .. }
                    | Expr::Literal(_)
                    | Expr::Name(_)
                    | Expr::Qualified(..)
                    | Expr::FieldAccess { .. }
                    | Expr::Raise(..)
                    | Expr::Variable(_)
                    | Expr::Default => values.push(1),
                }
            }
            Work::Reduce(expr, child_count) => {
                let start = values.len() - child_count;
                let child_values: Vec<usize> = values.drain(start..).collect();
                let result = match expr {
                    Expr::Between { .. } => {
                        let [evs_left, evs_start, evs_end] = child_values[..] else {
                            unreachable!("BETWEEN vector-size reducer expects 3 values");
                        };
                        if evs_left != evs_start || evs_left != evs_end {
                            crate::bail_parse_error!(
                                "all arguments to BETWEEN must return the same number of values. Got: ({evs_left}) BETWEEN ({evs_start}) AND ({evs_end})"
                            );
                        }
                        1
                    }
                    Expr::Binary(_, operator, _) => {
                        let [evs_left, evs_right] = child_values[..] else {
                            unreachable!("binary vector-size reducer expects 2 values");
                        };
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
                    Expr::Case {
                        base,
                        when_then_pairs,
                        else_expr,
                    } => {
                        let mut idx = 0;
                        if base.is_some() {
                            let evs_base = child_values[idx];
                            idx += 1;
                            if evs_base != 1 {
                                crate::bail_parse_error!(
                                    "base expression in CASE must return 1 value. Got: ({evs_base})"
                                );
                            }
                        }
                        for _ in when_then_pairs {
                            let evs_when = child_values[idx];
                            let evs_then = child_values[idx + 1];
                            idx += 2;
                            if evs_when != 1 {
                                crate::bail_parse_error!(
                                    "when expression in CASE must return 1 value. Got: ({evs_when})"
                                );
                            }
                            if evs_then != 1 {
                                crate::bail_parse_error!(
                                    "then expression in CASE must return 1 value. Got: ({evs_then})"
                                );
                            }
                        }
                        if else_expr.is_some() {
                            let evs_else_expr = child_values[idx];
                            if evs_else_expr != 1 {
                                crate::bail_parse_error!(
                                    "else expression in CASE must return 1 value. Got: ({evs_else_expr})"
                                );
                            }
                        }
                        1
                    }
                    Expr::Cast { .. } => {
                        let evs_expr = child_values[0];
                        if evs_expr != 1 {
                            crate::bail_parse_error!(
                                "argument to CAST must return 1 value. Got: ({evs_expr})"
                            );
                        }
                        1
                    }
                    Expr::Collate(..) => {
                        let evs_expr = child_values[0];
                        if evs_expr != 1 {
                            crate::bail_parse_error!(
                                "argument to COLLATE must return 1 value. Got: ({evs_expr})"
                            );
                        }
                        1
                    }
                    Expr::FunctionCall { name, .. } => {
                        for (pos, evs_arg) in child_values.iter().copied().enumerate() {
                            if evs_arg != 1 {
                                crate::bail_parse_error!(
                                    "argument {} to function call {name} must return 1 value. Got: ({evs_arg})",
                                    pos + 1
                                );
                            }
                        }
                        1
                    }
                    Expr::InList { .. } => {
                        let evs_lhs = child_values[0];
                        for evs_rhs in child_values.iter().copied().skip(1) {
                            if evs_lhs != evs_rhs {
                                crate::bail_parse_error!(
                                    "all arguments to IN list must return the same number of values, got: ({evs_lhs}) IN ({evs_rhs})"
                                );
                            }
                        }
                        1
                    }
                    Expr::IsNull(_) => {
                        let evs_expr = child_values[0];
                        if evs_expr != 1 {
                            crate::bail_parse_error!(
                                "argument to IS NULL must return 1 value. Got: ({evs_expr})"
                            );
                        }
                        1
                    }
                    Expr::Like { op, .. } => {
                        let evs_lhs = child_values[0];
                        // MATCH allows multi-column LHS: (col1, col2) MATCH 'query'
                        if evs_lhs != 1 && *op != ast::LikeOperator::Match {
                            crate::bail_parse_error!(
                                "left operand of LIKE must return 1 value. Got: ({evs_lhs})"
                            );
                        }
                        let evs_rhs = child_values[1];
                        if evs_rhs != 1 {
                            crate::bail_parse_error!(
                                "right operand of LIKE must return 1 value. Got: ({evs_rhs})"
                            );
                        }
                        1
                    }
                    Expr::NotNull(_) => {
                        let evs_expr = child_values[0];
                        if evs_expr != 1 {
                            crate::bail_parse_error!(
                                "argument to NOT NULL must return 1 value. Got: ({evs_expr})"
                            );
                        }
                        1
                    }
                    Expr::Unary(unary_operator, _) => {
                        let evs_expr = child_values[0];
                        if evs_expr != 1 {
                            crate::bail_parse_error!(
                                "argument to unary operator {unary_operator} must return 1 value. Got: ({evs_expr})"
                            );
                        }
                        1
                    }
                    _ => unreachable!("non-composite expression scheduled for vector-size reduce"),
                };
                values.push(result);
            }
        }
    }

    let [result] = values[..] else {
        unreachable!("expr_vector_size must leave exactly one value on the stack");
    };
    Ok(result)
}
