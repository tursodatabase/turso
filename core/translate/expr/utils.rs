use super::*;

pub fn maybe_apply_affinity(col_type: Type, target_register: usize, program: &mut ProgramBuilder) {
    if col_type == Type::Real {
        program.emit_insn(Insn::RealAffinity {
            register: target_register,
        })
    }
}

/// Sanitizes a string literal by removing single quote at front and back
/// and escaping double single quotes
pub fn sanitize_string(input: &str) -> String {
    let inner = &input[1..input.len() - 1];

    // Fast path, avoid replacing.
    if !inner.contains("''") {
        return inner.to_string();
    }

    inner.replace("''", "'")
}

/// Returns the components of a binary expression
/// e.g. t.x = 5 -> Some((t.x, =, 5))
pub fn as_binary_components(
    expr: &ast::Expr,
) -> Result<Option<(&ast::Expr, ConstraintOperator, &ast::Expr)>> {
    match unwrap_parens(expr)? {
        ast::Expr::Binary(lhs, operator, rhs)
            if matches!(
                operator,
                ast::Operator::Equals
                    | ast::Operator::NotEquals
                    | ast::Operator::Greater
                    | ast::Operator::Less
                    | ast::Operator::GreaterEquals
                    | ast::Operator::LessEquals
                    | ast::Operator::Is
                    | ast::Operator::IsNot
            ) =>
        {
            // Row-valued binary comparisons are translated directly in expression codegen.
            // They are not safe to expose as scalar binary constraints in optimizer paths.
            if expr_vector_size(lhs)? > 1 || expr_vector_size(rhs)? > 1 {
                return Ok(None);
            }
            Ok(Some((lhs.as_ref(), (*operator).into(), rhs.as_ref())))
        }
        ast::Expr::Like { lhs, not, rhs, .. } => Ok(Some((
            lhs.as_ref(),
            ConstraintOperator::Like { not: *not },
            rhs.as_ref(),
        ))),
        _ => Ok(None),
    }
}

/// Recursively unwrap parentheses from an expression
/// e.g. (((t.x > 5))) -> t.x > 5
pub fn unwrap_parens(expr: &ast::Expr) -> Result<&ast::Expr> {
    match expr {
        ast::Expr::Column { .. } => Ok(expr),
        ast::Expr::Parenthesized(exprs) => match exprs.len() {
            1 => unwrap_parens(exprs.first().unwrap()),
            _ => Ok(expr), // If the expression is e.g. (x, y), as used in e.g. (x, y) IN (SELECT ...), return as is.
        },
        _ => Ok(expr),
    }
}

/// If the right-hand side of `IS` / `IS NOT` is the TRUE or FALSE keyword —
/// possibly inside parentheses or under COLLATE — return its boolean value.
/// `x IS TRUE` and friends then test the truth of x instead of comparing
/// values. SQLite behaves the same way: it resolves the keyword after
/// skipping COLLATE, and its grammar collapses parentheses (resolve.c,
/// `sqlite3ExprSkipCollateAndLikely`). A unary `+` blocks the keyword there
/// too, so `x IS +TRUE` stays an ordinary comparison with 1.
///
/// Every place that decides "is this a truth test?" — the expression
/// evaluator, the condition emitter, and the index-seek guard in the
/// optimizer — must call this one function, so they cannot drift apart.
pub fn truth_test_rhs(expr: &ast::Expr) -> Option<bool> {
    match expr {
        ast::Expr::Literal(ast::Literal::True) => Some(true),
        ast::Expr::Literal(ast::Literal::False) => Some(false),
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => truth_test_rhs(&exprs[0]),
        ast::Expr::Collate(inner, _) => truth_test_rhs(inner),
        _ => None,
    }
}

/// The truth value of TRUE, FALSE or an integer literal that fits in 32 bits,
/// through parentheses: the operands SQLite folds `x AND 0` and `x OR 1` on
/// (sqlite3ExprAlwaysFalse, sqlite3ExprAlwaysTrue). A float, a string, NULL,
/// a COLLATE or a unary sign in front make it None.
pub fn literal_truth_value(expr: &ast::Expr) -> Option<bool> {
    match expr {
        ast::Expr::Literal(ast::Literal::True) => Some(true),
        ast::Expr::Literal(ast::Literal::False) => Some(false),
        ast::Expr::Parenthesized(items) if items.len() == 1 => literal_truth_value(&items[0]),
        _ => integer_literal_truth_value(expr),
    }
}

/// [literal_truth_value] for an integer literal only. SQLite's parser folds
/// `x AND 0` as soon as it reads it, before TRUE and FALSE are resolved, so a
/// place that follows the parser must not accept the keywords.
pub fn integer_literal_truth_value(expr: &ast::Expr) -> Option<bool> {
    match expr {
        ast::Expr::Literal(ast::Literal::Numeric(text)) => {
            match crate::util::parse_numeric_literal(text) {
                Ok(crate::Value::Numeric(crate::Numeric::Integer(value))) => {
                    i32::try_from(value).ok().map(|value| value != 0)
                }
                _ => None,
            }
        }
        ast::Expr::Parenthesized(items) if items.len() == 1 => {
            integer_literal_truth_value(&items[0])
        }
        _ => None,
    }
}

/// Recursively unwrap parentheses from an owned Expr.
/// Returns how many pairs of parentheses were removed.
pub fn unwrap_parens_owned(expr: ast::Expr) -> Result<(ast::Expr, usize)> {
    let mut paren_count = 0;
    match expr {
        ast::Expr::Parenthesized(mut exprs) => match exprs.len() {
            1 => {
                paren_count += 1;
                let (expr, count) = unwrap_parens_owned(*exprs.pop().unwrap())?;
                paren_count += count;
                Ok((expr, paren_count))
            }
            _ => crate::bail_parse_error!("expected single expression in parentheses"),
        },
        _ => Ok((expr, paren_count)),
    }
}
