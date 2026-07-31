use crate::{turso_assert, turso_assert_greater_than};
use turso_parser::ast::Operator;

use crate::{
    translate::{
        plan::WhereTerm,
        plan_expr::{plan_exprs_are_equivalent, PlanExpr as Expr},
    },
    Result,
};
/// Lifts shared conjuncts (ANDs) from sibling OR terms.
/// For example, given:
/// (a AND b AND c AND d)
///     OR
/// (a AND b AND e AND f)
/// Notice that both OR terms contain the same conjuncts (a AND b).
///
/// This function will lift the common conjuncts (a AND b) to the top level,
/// resulting in a Vec of three [WhereTerm]s like:
/// 1. (c AND d) OR (e AND f)
/// 2. a,
/// 3. b,
///
/// where `a` and `b` become separate WhereTerms, and the original WhereTerm
/// is updated to `(c AND d) OR (e AND f)`.
///
/// This optimization is important because we rely on individual [WhereTerm]s
/// for index selection. Imagine an index on (a,b) -- with our current optimizer
/// we wouldn't be able to use the index based on the original [WhereTerm]s, but
/// if we can lift [a,b] to the top level, we can use the index.
///
/// This function is horribly inefficient atm, but it at least makes certain
/// less trivial queries (e.g. perf/tpc-h/queries/19.sql) finish reasonably fast.
pub(crate) fn lift_common_subexpressions_from_binary_or_terms(
    where_clause: &mut Vec<WhereTerm>,
) -> Result<()> {
    let mut i = 0;
    while i < where_clause.len() {
        if !matches!(
            where_clause[i].expr,
            Expr::Binary {
                operator: Operator::Or,
                ..
            }
        ) {
            // Not an OR term, skip.
            i += 1;
            continue;
        }
        let term_expr_owned = where_clause[i].expr.clone(); // Own the expression for flattening
        let term_from_outer_join = where_clause[i].from_outer_join; // This needs to be remembered for the new WhereTerms

        // e.g. a OR b OR c becomes effectively OR [a,b,c].
        let or_operands = flatten_or_expr_owned(term_expr_owned)?;

        turso_assert!(or_operands.len() > 1);

        // Each OR operand is potentially an AND chain, e.g.
        // (a AND b) OR (c AND d).
        // Flatten them.
        // Parentheses do not survive semantic lowering, so each operand can be
        // flattened directly.
        let all_or_operands_conjunct_lists: Vec<Vec<Expr>> = or_operands
            .into_iter()
            .map(flatten_and_expr_owned)
            .collect::<Result<Vec<_>>>()?;

        // Find common conjuncts across all OR branches.
        // Initialize with conjuncts from the first OR branch.
        // We clone because `common_conjuncts_accumulator` will be modified.
        let mut common_conjuncts_accumulator = all_or_operands_conjunct_lists[0].clone();

        for other_conjunct_list in all_or_operands_conjunct_lists.iter().skip(1) {
            // Retain only those expressions in `common_conjuncts_accumulator`
            // that are also present in `other_conjunct_list`.
            common_conjuncts_accumulator.retain(|common_expr| {
                other_conjunct_list
                    .iter()
                    .any(|expr| plan_exprs_are_equivalent(common_expr, expr))
            });
        }

        // If no common conjuncts were found, move to the next WhereTerm.
        if common_conjuncts_accumulator.is_empty() {
            i += 1;
            continue;
        }

        // We found common conjuncts. Let's remove the common ones and rebuild the OR branches.
        // E.g. (a AND b) OR (a AND c) -> (b OR c) AND a.
        let mut new_or_operands_for_original_term = Vec::new();
        let mut found_non_empty_or_branches = false;
        for mut conjunct_list_for_or_branch in all_or_operands_conjunct_lists {
            // Remove the common conjuncts from this specific OR branch's list of conjuncts.
            conjunct_list_for_or_branch.retain(|expr_in_list| {
                !common_conjuncts_accumulator
                    .iter()
                    .any(|common| plan_exprs_are_equivalent(common, expr_in_list))
            });

            if conjunct_list_for_or_branch.is_empty() {
                // If any of the OR branches are empty, we can remove the entire OR term.
                // E.g. (a AND b) OR (a) OR (a AND c) just becomes a.
                found_non_empty_or_branches = true;
                break;
            }

            // Rebuild this OR branch from its remaining (non-common) conjuncts.
            new_or_operands_for_original_term
                .push(rebuild_and_expr_from_list(conjunct_list_for_or_branch));
        }

        if found_non_empty_or_branches {
            // If we found an empty OR branch, we can remove the entire OR term.
            // E.g. (a AND b) OR (a) OR (a AND c) just becomes a.
            where_clause[i].consumed = true;
        } else {
            turso_assert_greater_than!(new_or_operands_for_original_term.len(), 1);
            // Update the original WhereTerm's expression with the new OR structure (without common parts).
            where_clause[i].expr = rebuild_or_expr_from_list(new_or_operands_for_original_term);
        }

        // Add the lifted common conjuncts as new, separate WhereTerms.
        for common_expr_to_add in common_conjuncts_accumulator {
            where_clause.push(WhereTerm {
                expr: common_expr_to_add,
                from_outer_join: term_from_outer_join,
                consumed: false,
            });
        }

        // Simply incrementing i is correct because we added new WhereTerms at the end.
        i += 1;
    }
    Ok(())
}

/// Flatten a binary OR into a list of disjuncts.
fn flatten_or_expr_owned(expr: Expr) -> Result<Vec<Expr>> {
    let Expr::Binary {
        lhs,
        operator: Operator::Or,
        rhs,
        ..
    } = expr
    else {
        return Ok(vec![expr]);
    };
    let mut flattened = flatten_or_expr_owned(*lhs)?;
    flattened.extend(flatten_or_expr_owned(*rhs)?);
    Ok(flattened)
}

/// Flatten a binary AND into a list of conjuncts.
fn flatten_and_expr_owned(expr: Expr) -> Result<Vec<Expr>> {
    let Expr::Binary {
        lhs,
        operator: Operator::And,
        rhs,
        ..
    } = expr
    else {
        return Ok(vec![expr]);
    };
    let mut flattened = flatten_and_expr_owned(*lhs)?;
    flattened.extend(flatten_and_expr_owned(*rhs)?);
    Ok(flattened)
}

/// Rebuild a binary AND for a list of conjuncts.
fn rebuild_and_expr_from_list(mut conjuncts: Vec<Expr>) -> Expr {
    turso_assert!(!conjuncts.is_empty());

    if conjuncts.len() == 1 {
        return conjuncts.pop().unwrap();
    }

    let mut current_expr = conjuncts.remove(0);
    for next_expr in conjuncts {
        current_expr = Expr::binary(current_expr, Operator::And, next_expr);
    }
    current_expr
}

/// Rebuild a binary OR for a list of operands.
fn rebuild_or_expr_from_list(mut operands: Vec<Expr>) -> Expr {
    turso_assert!(!operands.is_empty());

    if operands.len() == 1 {
        return operands.pop().unwrap();
    }

    let mut current_expr = operands.remove(0);
    for next_expr in operands {
        current_expr = Expr::binary(current_expr, Operator::Or, next_expr);
    }
    current_expr
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::{
        plan::WhereTerm,
        plan_expr::{PlanExpr, PlanSourceId},
    };
    use turso_parser::ast::{Literal, Operator};

    fn equals(column: usize) -> PlanExpr {
        PlanExpr::binary(
            PlanExpr::column(PlanSourceId::new(0), column),
            Operator::Equals,
            PlanExpr::literal(Literal::Numeric("1".to_string())),
        )
    }

    fn and(expressions: Vec<PlanExpr>) -> PlanExpr {
        rebuild_and_expr_from_list(expressions)
    }

    fn or(expressions: Vec<PlanExpr>) -> PlanExpr {
        rebuild_or_expr_from_list(expressions)
    }

    fn assert_expr_eq(actual: &PlanExpr, expected: &PlanExpr) {
        assert!(plan_exprs_are_equivalent(actual, expected));
    }

    #[test]
    fn lifts_two_common_conjuncts() -> Result<()> {
        let a = equals(0);
        let b = equals(1);
        let x = equals(2);
        let y = equals(3);
        let mut where_clause = vec![WhereTerm {
            expr: or(vec![
                and(vec![a.clone(), x.clone(), b.clone()]),
                and(vec![a.clone(), y.clone(), b.clone()]),
            ]),
            from_outer_join: None,
            consumed: false,
        }];

        lift_common_subexpressions_from_binary_or_terms(&mut where_clause)?;

        let terms = where_clause
            .iter()
            .filter(|term| !term.consumed)
            .collect::<Vec<_>>();
        assert_eq!(terms.len(), 3);
        assert_expr_eq(&terms[0].expr, &or(vec![x, y]));
        assert_expr_eq(&terms[1].expr, &a);
        assert_expr_eq(&terms[2].expr, &b);
        Ok(())
    }

    #[test]
    fn lifts_common_conjunct_across_three_branches() -> Result<()> {
        let a = equals(0);
        let x = equals(1);
        let y = equals(2);
        let z = equals(3);
        let mut where_clause = vec![WhereTerm {
            expr: or(vec![
                and(vec![a.clone(), x.clone()]),
                and(vec![a.clone(), y.clone()]),
                and(vec![a.clone(), z.clone()]),
            ]),
            from_outer_join: None,
            consumed: false,
        }];

        lift_common_subexpressions_from_binary_or_terms(&mut where_clause)?;

        let terms = where_clause
            .iter()
            .filter(|term| !term.consumed)
            .collect::<Vec<_>>();
        assert_eq!(terms.len(), 2);
        assert_expr_eq(&terms[0].expr, &or(vec![x, y, z]));
        assert_expr_eq(&terms[1].expr, &a);
        Ok(())
    }

    #[test]
    fn leaves_disjoint_branches_unchanged() -> Result<()> {
        let original = or(vec![equals(0), equals(1)]);
        let mut where_clause = vec![WhereTerm {
            expr: original.clone(),
            from_outer_join: None,
            consumed: false,
        }];

        lift_common_subexpressions_from_binary_or_terms(&mut where_clause)?;

        assert_eq!(where_clause.len(), 1);
        assert!(!where_clause[0].consumed);
        assert_expr_eq(&where_clause[0].expr, &original);
        Ok(())
    }

    #[test]
    fn preserves_outer_join_origin_on_lifted_terms() -> Result<()> {
        let source = PlanSourceId::new(9);
        let a = equals(0);
        let x = equals(1);
        let y = equals(2);
        let mut where_clause = vec![WhereTerm {
            expr: or(vec![
                and(vec![a.clone(), x.clone()]),
                and(vec![a.clone(), y.clone()]),
            ]),
            from_outer_join: Some(source),
            consumed: false,
        }];

        lift_common_subexpressions_from_binary_or_terms(&mut where_clause)?;

        let terms = where_clause
            .iter()
            .filter(|term| !term.consumed)
            .collect::<Vec<_>>();
        assert_eq!(terms.len(), 2);
        assert!(terms
            .iter()
            .all(|term| term.from_outer_join == Some(source)));
        assert_expr_eq(&terms[0].expr, &or(vec![x, y]));
        assert_expr_eq(&terms[1].expr, &a);
        Ok(())
    }

    #[test]
    fn leaves_non_or_term_unchanged() -> Result<()> {
        let expression = equals(0);
        let mut where_clause = vec![WhereTerm {
            expr: expression.clone(),
            from_outer_join: None,
            consumed: false,
        }];

        lift_common_subexpressions_from_binary_or_terms(&mut where_clause)?;

        assert_eq!(where_clause.len(), 1);
        assert!(!where_clause[0].consumed);
        assert_expr_eq(&where_clause[0].expr, &expression);
        Ok(())
    }

    #[test]
    fn removes_or_when_one_branch_is_only_the_common_term() -> Result<()> {
        let a = equals(0);
        let b = equals(1);
        let mut where_clause = vec![WhereTerm {
            expr: or(vec![and(vec![a.clone(), b]), a.clone()]),
            from_outer_join: None,
            consumed: false,
        }];

        lift_common_subexpressions_from_binary_or_terms(&mut where_clause)?;

        let terms = where_clause
            .iter()
            .filter(|term| !term.consumed)
            .collect::<Vec<_>>();
        assert_eq!(terms.len(), 1);
        assert_expr_eq(&terms[0].expr, &a);
        Ok(())
    }
}
