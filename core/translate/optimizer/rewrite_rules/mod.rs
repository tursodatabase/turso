//! WHERE-clause rewrite rules that run before constraint extraction.
//!
//! These rewrites produce logically-redundant but optimizer-useful terms.
//! - `lift_common`: lifts shared AND conjuncts out of OR branches
//! - `or_decomposition`: decomposes cross-table ORs into per-table IN lists

mod lift_common;
mod or_decomposition;

pub(crate) use lift_common::lift_common_subexpressions_from_binary_or_terms;
pub(crate) use or_decomposition::decompose_cross_table_ors;

use crate::Result;
use turso_parser::ast::{Expr, Operator};

use crate::turso_assert;

// ── Shared helpers for OR/AND flattening ──────────────────────────────────

/// Flatten `a OR b OR c` into `[a, b, c]` by reference.
pub(super) fn flatten_or_refs(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Binary(lhs, Operator::Or, rhs) => {
            let mut result = flatten_or_refs(lhs);
            result.extend(flatten_or_refs(rhs));
            result
        }
        Expr::Parenthesized(exprs) if exprs.len() == 1 => flatten_or_refs(&exprs[0]),
        _ => vec![expr],
    }
}

/// Flatten `a AND b AND c` into `[a, b, c]` by reference.
pub(super) fn flatten_and_refs(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Binary(lhs, Operator::And, rhs) => {
            let mut result = flatten_and_refs(lhs);
            result.extend(flatten_and_refs(rhs));
            result
        }
        Expr::Parenthesized(exprs) if exprs.len() == 1 => flatten_and_refs(&exprs[0]),
        _ => vec![expr],
    }
}

/// Flatten an owned `a OR b OR c` into `[a, b, c]`.
pub(super) fn flatten_or_expr_owned(expr: Expr) -> Result<Vec<Expr>> {
    let Expr::Binary(lhs, Operator::Or, rhs) = expr else {
        return Ok(vec![expr]);
    };
    let mut flattened = flatten_or_expr_owned(*lhs)?;
    flattened.extend(flatten_or_expr_owned(*rhs)?);
    Ok(flattened)
}

/// Flatten an owned `a AND b AND c` into `[a, b, c]`.
pub(super) fn flatten_and_expr_owned(expr: Expr) -> Result<Vec<Expr>> {
    let Expr::Binary(lhs, Operator::And, rhs) = expr else {
        return Ok(vec![expr]);
    };
    let mut flattened = flatten_and_expr_owned(*lhs)?;
    flattened.extend(flatten_and_expr_owned(*rhs)?);
    Ok(flattened)
}

/// Rebuild `a AND b AND c` from a list of conjuncts.
pub(super) fn rebuild_and_expr_from_list(mut conjuncts: Vec<Expr>) -> Expr {
    turso_assert!(!conjuncts.is_empty());

    if conjuncts.len() == 1 {
        return conjuncts.pop().unwrap();
    }

    let mut current_expr = conjuncts.remove(0);
    for next_expr in conjuncts {
        current_expr = Expr::Binary(Box::new(current_expr), Operator::And, Box::new(next_expr));
    }
    current_expr
}

/// Rebuild `a OR b OR c` from a list of operands.
pub(super) fn rebuild_or_expr_from_list(mut operands: Vec<Expr>) -> Expr {
    turso_assert!(!operands.is_empty());

    if operands.len() == 1 {
        return operands.pop().unwrap();
    }

    let mut current_expr = operands.remove(0);
    for next_expr in operands {
        current_expr = Expr::Binary(Box::new(current_expr), Operator::Or, Box::new(next_expr));
    }
    current_expr
}
