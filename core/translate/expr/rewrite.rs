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
use crate::function::AggFunc;

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

/// SQLite's parser replaces `x AND 0` by 0 as it reads the statement
/// (sqlite3ExprAnd): the 0 is a plain integer literal, parentheses do not
/// matter, and neither operand calls a function. That happens before any
/// name is resolved, so `0 AND nosuchcol` is 0, before a WHERE, HAVING or
/// ON clause is split into terms, and before a HAVING term can move to
/// WHERE. So bind_expr applies this rule before it resolves names, and
/// break_predicate_at_and_boundaries applies it to the whole predicate
/// before splitting it: `TRUE AND (a AND 0)` becomes 0, which stays in
/// HAVING, while `g = 2 AND 0 AND abs(1)` becomes `0 AND abs(1)`, whose
/// `abs(1)` term may move. The folds SQLite's code generator does later,
/// on TRUE, FALSE and `x OR 1`, are [AndOrConstantOperand].
pub(crate) struct AndWithIntegerZero;

impl StaticRewrite for AndWithIntegerZero {
    fn rewrite(
        &self,
        expr: &mut ast::Expr,
        _resolver: Option<&Resolver<'_>>,
    ) -> Result<WalkControl> {
        let is_and = matches!(
            unwrap_parens(expr).unwrap_or(expr),
            Expr::Binary(_, ast::Operator::And, _)
        );
        if is_and && and_chain_folds_to_zero(expr) {
            *expr = Expr::Literal(ast::Literal::Numeric("0".to_string()));
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    }
}

/// True when SQLite's parser replaces this AND chain by 0 (sqlite3ExprAnd):
/// the integer literal 0 is one of its terms, as in `a AND 0 AND b`, and no
/// term calls a function. A function call keeps the chain as it is there, so
/// `f(x) AND 0` still runs `f(x)` in WHERE, and `abs(g) = 2 AND 0` still lets
/// `abs(g) = 2` move from HAVING to WHERE.
fn and_chain_folds_to_zero(expr: &Expr) -> bool {
    let expr = unwrap_parens(expr).unwrap_or(expr);
    match expr {
        Expr::Binary(left, ast::Operator::And, right) => {
            (and_chain_folds_to_zero(left) || and_chain_folds_to_zero(right))
                && !expr_calls_function(left)
                && !expr_calls_function(right)
        }
        _ => integer_literal_truth_value(expr) == Some(false),
    }
}

/// True if the expression calls a function outside of its subqueries. LIKE,
/// GLOB, REGEXP and MATCH, the JSON `->` and `->>` operators and
/// CURRENT_DATE, CURRENT_TIME and CURRENT_TIMESTAMP are function calls to
/// SQLite's parser too.
fn expr_calls_function(expr: &Expr) -> bool {
    let mut found = false;
    walk_expr(expr, &mut |e| {
        if matches!(
            e,
            Expr::FunctionCall { .. }
                | Expr::FunctionCallStar { .. }
                | Expr::Like { .. }
                | Expr::Binary(
                    _,
                    ast::Operator::ArrowRight | ast::Operator::ArrowRightShift,
                    _
                )
                | Expr::Literal(
                    ast::Literal::CurrentDate
                        | ast::Literal::CurrentTime
                        | ast::Literal::CurrentTimestamp
                )
        ) {
            found = true;
            return Ok(WalkControl::SkipChildren);
        }
        Ok(WalkControl::Continue)
    })
    .expect("walking an expression with an infallible visitor cannot fail");
    found
}

/// SQLite's code generator replaces `x AND 0` with 0 and `x OR 1` with 1
/// when one operand is an integer literal or TRUE/FALSE
/// (sqlite3ExprSimplifiedAndOr), so the other operand never runs:
/// `SELECT 1 OR json_extract('bare', '$.a')` is 1 and
/// `(SELECT abs(-9223372036854775807 - 1)) OR 1` does not run the subquery.
/// In [BINDING_REWRITES] it runs after binding, so names in the dropped
/// operand are still checked. An operand with a subquery waits for the
/// subquery planner, which resolves the names inside the subquery first and
/// then drops the operand, so the subquery is never emitted (see
/// subquery::get_subquery_parser). A HAVING clause applies it after its
/// terms have moved to WHERE or not, because SQLite decides that before
/// this fold. An expression that is translated without binding, like a
/// column DEFAULT, gets the same fold when its bytecode is emitted (see
/// [and_or_truth_value]).
pub(crate) struct AndOrConstantOperand;

impl StaticRewrite for AndOrConstantOperand {
    fn rewrite(
        &self,
        expr: &mut ast::Expr,
        resolver: Option<&Resolver<'_>>,
    ) -> Result<WalkControl> {
        // SQLite looks the collation of a compared operand up on the operand
        // as written, through NOT, arithmetic and function arguments, so a
        // collation name in the part of `x OR 1` the fold drops is still
        // checked there: `(1 OR a COLLATE nosuch) = 1` and
        // `abs(1 OR a COLLATE nosuch) = 1` are errors, while
        // `SELECT 1 OR a COLLATE nosuch` is 1.
        if let Some(resolver) = resolver {
            for operand in compared_operands(expr, resolver)? {
                reject_unknown_collation(operand, resolver)?;
            }
        }
        // The operand that replaces the node can be an AND or OR of the same
        // shape: `(x AND 0) AND y` becomes `x AND 0`, which becomes `0`.
        while replace_and_or_with_deciding_operand(expr, resolver, &mut |dropped| {
            Ok(!expr_contains_subquery(dropped))
        })? {}
        Ok(WalkControl::Continue)
    }
}

/// The operands of this node whose collating sequence SQLite looks up: the
/// sides of a comparison, BETWEEN or IN, the base of a CASE, the arguments
/// of min, max and nullif, and the PARTITION BY and ORDER BY keys of a
/// window. (The keys of ORDER BY, GROUP BY and DISTINCT are whole
/// expressions, which [bind_and_rewrite_sort_key] checks.)
fn compared_operands<'e>(expr: &'e Expr, resolver: &Resolver<'_>) -> Result<Vec<&'e Expr>> {
    use ast::Operator::*;
    Ok(match expr {
        Expr::Binary(
            lhs,
            Equals | NotEquals | Less | LessEquals | Greater | GreaterEquals | Is | IsNot,
            rhs,
        ) => vec![lhs.as_ref(), rhs.as_ref()],
        Expr::Between {
            lhs, start, end, ..
        } => vec![lhs.as_ref(), start.as_ref(), end.as_ref()],
        Expr::InList { lhs, .. } | Expr::InSelect { lhs, .. } => vec![lhs.as_ref()],
        Expr::Case {
            base: Some(base), ..
        } => vec![base.as_ref()],
        Expr::FunctionCall {
            name,
            args,
            filter_over,
            ..
        } => {
            let compares = match resolver.resolve_function(name.as_str(), args.len())? {
                Some(Func::Agg(AggFunc::Max | AggFunc::Min)) => true,
                Some(func) => func.needs_collation(),
                None => false,
            };
            let mut operands: Vec<&Expr> = if compares {
                args.iter().map(|arg| arg.as_ref()).collect()
            } else {
                vec![]
            };
            if let Some(ast::Over::Window(window)) = filter_over.over_clause.as_deref() {
                operands.extend(window.partition_by.iter().map(|key| key.as_ref()));
                operands.extend(window.order_by.iter().map(|key| key.expr.as_ref()));
            }
            operands
        }
        _ => vec![],
    })
}

/// Keeps the operand that decides the value of an AND or OR and drops the
/// other, as sqlite3ExprSimplifiedAndOr does. The deciding operand is a
/// literal, which becomes the 0 or 1 the AND or OR would have given (`0 OR 5`
/// is 1, not 5), or an AND or OR whose own operands decide it, which is kept
/// as it is, so `5 AND 0 AND x` keeps `5 AND 0`. `may_drop` sees the operand
/// about to go and says whether it goes now. Returns true when `expr` was
/// replaced.
pub(crate) fn replace_and_or_with_deciding_operand(
    expr: &mut ast::Expr,
    resolver: Option<&Resolver<'_>>,
    may_drop: &mut dyn FnMut(&ast::Expr) -> Result<bool>,
) -> Result<bool> {
    let Expr::Binary(lhs, op @ (ast::Operator::And | ast::Operator::Or), rhs) = expr else {
        return Ok(false);
    };
    let deciding_value = *op == ast::Operator::Or;
    let (kept, dropped) = if and_or_truth_value(lhs) == Some(deciding_value) {
        (lhs, rhs)
    } else if and_or_truth_value(rhs) == Some(deciding_value) {
        (rhs, lhs)
    } else {
        return Ok(false);
    };
    // SQLite resolves every name and checks every row value before this
    // fold, so the dropped operand still fails for a function that does not
    // exist and for `(1, 2) = (1, 2, 3)`.
    reject_row_value_misuse(dropped)?;
    // An aggregate in the dropped operand still makes the query an aggregate
    // query: `SELECT count(*) OR 1 FROM t` gives one row, not one per row.
    let dropped_has_aggregate = match resolver {
        Some(resolver) => {
            check_function_calls(dropped, resolver)?;
            expr_contains_aggregate_or_window_function(dropped, resolver)?
        }
        None => true,
    };
    if dropped_has_aggregate {
        return Ok(false);
    }
    // A name that binding left for later, as a trigger WHEN clause or an
    // UPSERT DO UPDATE does, is still checked when it is resolved, so the
    // operand that holds it stays until then.
    if expr_contains_unresolved_column_name(dropped) {
        return Ok(false);
    }
    if !may_drop(dropped)? {
        return Ok(false);
    }
    let kept_is_and_or = matches!(
        unwrap_parens(kept).unwrap_or(kept),
        Expr::Binary(_, ast::Operator::And | ast::Operator::Or, _)
    );
    *expr = if kept_is_and_or {
        std::mem::take(kept.as_mut())
    } else {
        let folded = if deciding_value { "1" } else { "0" };
        Expr::Literal(ast::Literal::Numeric(folded.to_string()))
    };
    Ok(true)
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
            let lhs_is_nondeterministic = match resolver {
                Some(resolver) => expr_contains_nondeterministic_scalar_function(lhs, resolver)?,
                None => false,
            };
            if expr_contains_subquery(lhs) || lhs_is_nondeterministic {
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
