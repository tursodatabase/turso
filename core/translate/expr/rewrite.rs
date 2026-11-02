//! Rewrites decided by the shape of an expression alone, before anything is
//! planned or emitted: no table data, no registers. SQLite applies the same
//! kind of rule in its parser and resolver (sqlite3ExprAnd, whereExprAnalyze).
//!
//! A rule is a [StaticRewrite]. [apply_static_rewrites] walks one expression
//! and gives every node to each rule in order. [BINDING_REWRITES] runs on
//! every expression at the end of [bind_and_rewrite_expr], so a rule placed
//! there reaches result columns, CASE, function arguments, WHERE, HAVING,
//! ORDER BY and DML values alike. A rule that has to run somewhere else, like
//! [BetweenToComparisons], which WHERE applies before it splits the predicate
//! into AND terms, is passed to [apply_static_rewrites] by that caller.

use super::*;

pub(crate) trait StaticRewrite {
    /// Rewrites `expr` in place when its shape matches. Returns
    /// `WalkControl::SkipChildren` when the children of the new expression
    /// must not be visited, because they were dropped or are already in their
    /// final form, and `WalkControl::Continue` otherwise.
    fn rewrite(&self, expr: &mut ast::Expr, resolver: Option<&Resolver<'_>>)
        -> Result<WalkControl>;
}

/// The rules every expression gets at the end of [bind_and_rewrite_expr].
pub(crate) const BINDING_REWRITES: &[&dyn StaticRewrite] = &[&AndOrConstantOperand];

pub(crate) fn apply_static_rewrites(
    expr: &mut ast::Expr,
    rules: &[&dyn StaticRewrite],
    resolver: Option<&Resolver<'_>>,
) -> Result<()> {
    walk_expr_mut(expr, &mut |expr: &mut ast::Expr| -> Result<WalkControl> {
        for rule in rules {
            if matches!(rule.rewrite(expr, resolver)?, WalkControl::SkipChildren) {
                return Ok(WalkControl::SkipChildren);
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

/// SQLite replaces `x AND 0` with 0 and `x OR 1` with 1 when one operand is
/// an integer literal or TRUE/FALSE, so the other operand never runs:
/// `SELECT 1 OR json_extract('bare', '$.a')` is 1 and
/// `CASE WHEN (SELECT abs(-9223372036854775807 - 1)) AND 0 ...` does not run
/// the subquery. In [BINDING_REWRITES] it runs after binding, so names in the
/// dropped operand are still checked, and before subquery planning, so the
/// subquery is never planned or emitted.
pub(crate) struct AndOrConstantOperand;

impl StaticRewrite for AndOrConstantOperand {
    fn rewrite(
        &self,
        expr: &mut ast::Expr,
        _resolver: Option<&Resolver<'_>>,
    ) -> Result<WalkControl> {
        let Expr::Binary(lhs, op @ (ast::Operator::And | ast::Operator::Or), rhs) = expr else {
            return Ok(WalkControl::Continue);
        };
        let deciding_value = *op == ast::Operator::Or;
        if literal_truth_value(lhs) == Some(deciding_value)
            || literal_truth_value(rhs) == Some(deciding_value)
        {
            let folded = if deciding_value { "1" } else { "0" };
            *expr = Expr::Literal(ast::Literal::Numeric(folded.to_string()));
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    }
}

/// `x BETWEEN a AND b` becomes `x >= a AND x <= b` (and `NOT BETWEEN`
/// becomes `x < a OR x > b`), so a WHERE clause can consider each side on its
/// own for constraint extraction and range planning. When both bounds are
/// literals the rewrite is skipped for a left side with a subquery or a
/// nondeterministic function, which must not run twice.
pub(crate) struct BetweenToComparisons;

impl StaticRewrite for BetweenToComparisons {
    fn rewrite(
        &self,
        expr: &mut ast::Expr,
        resolver: Option<&Resolver<'_>>,
    ) -> Result<WalkControl> {
        let Expr::Between {
            lhs,
            not,
            start,
            end,
        } = expr
        else {
            return Ok(WalkControl::Continue);
        };
        if is_literal_between_bound(start) && is_literal_between_bound(end) {
            let mut lhs_has_subquery = false;
            walk_expr(lhs, &mut |expr| {
                if matches!(
                    expr,
                    Expr::Subquery(_)
                        | Expr::Exists(_)
                        | Expr::InSelect { .. }
                        | Expr::SubqueryResult { .. }
                ) {
                    lhs_has_subquery = true;
                    return Ok(WalkControl::SkipChildren);
                }
                Ok(WalkControl::Continue)
            })?;
            let lhs_is_nondeterministic = match resolver {
                Some(resolver) => expr_contains_nondeterministic_scalar_function(lhs, resolver)?,
                None => false,
            };
            if lhs_has_subquery || lhs_is_nondeterministic {
                return Ok(WalkControl::Continue);
            }
        }

        let lhs_expr = std::mem::take(lhs.as_mut());
        let start_expr = std::mem::take(start.as_mut());
        let end_expr = std::mem::take(end.as_mut());

        let (lower, upper, combine_op) = if *not {
            (
                Expr::Binary(
                    Box::new(lhs_expr.clone()),
                    ast::Operator::Less,
                    Box::new(start_expr),
                ),
                Expr::Binary(
                    Box::new(lhs_expr),
                    ast::Operator::Greater,
                    Box::new(end_expr),
                ),
                ast::Operator::Or,
            )
        } else {
            (
                Expr::Binary(
                    Box::new(lhs_expr.clone()),
                    ast::Operator::GreaterEquals,
                    Box::new(start_expr),
                ),
                Expr::Binary(
                    Box::new(lhs_expr),
                    ast::Operator::LessEquals,
                    Box::new(end_expr),
                ),
                ast::Operator::And,
            )
        };
        *expr = Expr::Binary(Box::new(lower), combine_op, Box::new(upper));
        Ok(WalkControl::Continue)
    }
}

fn is_literal_between_bound(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) => true,
        Expr::Unary(_, inner) => is_literal_between_bound(inner),
        Expr::Parenthesized(exprs) if exprs.len() == 1 => is_literal_between_bound(&exprs[0]),
        _ => false,
    }
}
