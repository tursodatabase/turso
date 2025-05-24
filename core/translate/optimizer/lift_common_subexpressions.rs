use limbo_sqlite3_parser::ast::{Expr, Operator};

use crate::{
    translate::{expr::unwrap_parens_owned, plan::WhereTerm},
    util::exprs_are_equivalent,
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
        if !matches!(where_clause[i].expr, Expr::Binary(_, Operator::Or, _)) {
            // Not an OR term, skip.
            i += 1;
            continue;
        }
        let term_expr_owned = where_clause[i].expr.clone(); // Own the expression for flattening
        let term_from_outer_join = where_clause[i].from_outer_join; // This needs to be remembered for the new WhereTerms

        // e.g. a OR b OR c becomes effectively OR [a,b,c].
        let or_operands = flatten_or_expr_owned(term_expr_owned)?;

        assert!(or_operands.len() > 1);

        // Each OR operand is potentially an AND chain, e.g.
        // (a AND b) OR (c AND d).
        // Flatten them.
        // It's safe to remove parentheses with `unwrap_parens_owned` because
        // we will add them back once we reconstruct the OR term's child expressions.
        // e.g. (a AND b) OR (c AND d) becomes effectively AND [[a,b], [c,d]].
        let all_or_operands_conjunct_lists: Vec<(Vec<Expr>, usize)> = or_operands
            .into_iter()
            .map(|expr| {
                let (expr, paren_count) = unwrap_parens_owned(expr)?;
                Ok((flatten_and_expr_owned(expr)?, paren_count))
            })
            .collect::<Result<Vec<_>>>()?;

        // Find common conjuncts across all OR branches.
        // Initialize with conjuncts from the first OR branch.
        // We clone because `common_conjuncts_accumulator` will be modified.
        let mut common_conjuncts_accumulator = all_or_operands_conjunct_lists[0].0.clone();

        for (other_conjunct_list, _) in all_or_operands_conjunct_lists.iter().skip(1) {
            // Retain only those expressions in `common_conjuncts_accumulator`
            // that are also present in `other_conjunct_list`.
            common_conjuncts_accumulator.retain(|common_expr| {
                other_conjunct_list
                    .iter()
                    .any(|expr| exprs_are_equivalent(common_expr, expr))
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
        for (mut conjunct_list_for_or_branch, mut num_unwrapped_parens) in
            all_or_operands_conjunct_lists.into_iter()
        {
            // Remove the common conjuncts from this specific OR branch's list of conjuncts.
            conjunct_list_for_or_branch
                .retain(|expr_in_list| !common_conjuncts_accumulator.contains(expr_in_list));

            if conjunct_list_for_or_branch.is_empty() {
                // If any of the OR branches are empty, we can remove the entire OR term.
                // E.g. (a AND b) OR (a) OR (a AND c) just becomes a.
                found_non_empty_or_branches = true;
                break;
            }

            // Rebuild this OR branch from its remaining (non-common) conjuncts.
            // If we unwrapped parentheses before, let's add them back.
            let mut top_level_expr = rebuild_and_expr_from_list(conjunct_list_for_or_branch);
            while num_unwrapped_parens > 0 {
                top_level_expr = Expr::Parenthesized(vec![top_level_expr]);
                num_unwrapped_parens -= 1;
            }
            new_or_operands_for_original_term.push(top_level_expr);
        }

        if found_non_empty_or_branches {
            // If we found an empty OR branch, we can remove the entire OR term.
            // E.g. (a AND b) OR (a) OR (a AND c) just becomes a.
            where_clause[i].consumed = true;
        } else {
            assert!(new_or_operands_for_original_term.len() > 1);
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

/// Flatten an ast::Expr::Binary(lhs, OR, rhs) into a list of disjuncts.
fn flatten_or_expr_owned(expr: Expr) -> Result<Vec<Expr>> {
    let Expr::Binary(lhs, Operator::Or, rhs) = expr else {
        return Ok(vec![expr]);
    };
    let mut flattened = flatten_or_expr_owned(*lhs)?;
    flattened.extend(flatten_or_expr_owned(*rhs)?);
    Ok(flattened)
}

/// Flatten an ast::Expr::Binary(lhs, AND, rhs) into a list of conjuncts.
fn flatten_and_expr_owned(expr: Expr) -> Result<Vec<Expr>> {
    let Expr::Binary(lhs, Operator::And, rhs) = expr else {
        return Ok(vec![expr]);
    };
    let mut flattened = flatten_and_expr_owned(*lhs)?;
    flattened.extend(flatten_and_expr_owned(*rhs)?);
    Ok(flattened)
}

/// Rebuild an ast::Expr::Binary(lhs, AND, rhs) for a list of conjuncts.
fn rebuild_and_expr_from_list(mut conjuncts: Vec<Expr>) -> Expr {
    assert!(!conjuncts.is_empty());

    if conjuncts.len() == 1 {
        return conjuncts.pop().unwrap();
    }

    let mut current_expr = conjuncts.remove(0);
    for next_expr in conjuncts {
        current_expr = Expr::Binary(Box::new(current_expr), Operator::And, Box::new(next_expr));
    }
    current_expr
}

/// Rebuild an ast::Expr::Binary(lhs, OR, rhs) for a list of operands.
fn rebuild_or_expr_from_list(mut operands: Vec<Expr>) -> Expr {
    assert!(!operands.is_empty());

    if operands.len() == 1 {
        return operands.pop().unwrap();
    }

    let mut current_expr = operands.remove(0);
    for next_expr in operands {
        current_expr = Expr::Binary(Box::new(current_expr), Operator::Or, Box::new(next_expr));
    }
    current_expr
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::plan::WhereTerm;
    use limbo_sqlite3_parser::ast::{self, Expr, Literal, Operator};

    #[test]
    fn test_lift_common_subexpressions() -> Result<()> {
        // SELECT * FROM t WHERE (a = 1 and x = 1 and b = 1) OR (a = 1 and y = 1 and b = 1)
        // should be rewritten to:
        // SELECT * FROM t WHERE (x = 1 OR y = 1) and a = 1 and b = 1

        // assume the table has 4 columns: a, b, x, y
        let a_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 0,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let b_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 1,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let x_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 2,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let y_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 3,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        // Create (a = 1 AND x = 1 AND b = 1) OR (a = 1 AND y = 1 AND b = 1)
        let or_expr = Expr::Binary(
            Box::new(ast::Expr::Parenthesized(vec![rebuild_and_expr_from_list(
                vec![a_expr.clone(), x_expr.clone(), b_expr.clone()],
            )])),
            Operator::Or,
            Box::new(ast::Expr::Parenthesized(vec![rebuild_and_expr_from_list(
                vec![a_expr.clone(), y_expr.clone(), b_expr.clone()],
            )])),
        );

        let mut where_clause = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: None,
            consumed: false,
        }];

        lift_common_subexpressions_from_binary_or_terms(&mut where_clause)?;

        // Should now have 3 terms:
        // 1. (x = 1) OR (y = 1)
        // 2. a = 1
        // 3. b = 1
        let nonconsumed_terms = where_clause
            .iter()
            .filter(|term| !term.consumed)
            .collect::<Vec<_>>();
        assert_eq!(nonconsumed_terms.len(), 3);
        assert_eq!(
            nonconsumed_terms[0].expr,
            Expr::Binary(
                Box::new(ast::Expr::Parenthesized(vec![x_expr.clone()])),
                Operator::Or,
                Box::new(ast::Expr::Parenthesized(vec![y_expr.clone()]))
            )
        );
        assert_eq!(nonconsumed_terms[1].expr, a_expr);
        assert_eq!(nonconsumed_terms[2].expr, b_expr);

        Ok(())
    }

    #[test]
    fn test_lift_common_subexpressions_three_branches() -> Result<()> {
        // Test case with three OR branches and one common term:
        // (a = 1 AND x = 1) OR (a = 1 AND y = 1) OR (a = 1 AND z = 1)
        // Should become:
        // (x = 1 OR y = 1 OR z = 1) AND a = 1

        let a_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 0,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let x_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 1,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let y_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 2,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let z_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 3,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        // Create (a = 1 AND x = 1) OR (a = 1 AND y = 1) OR (a = 1 AND z = 1)
        let or_expr = Expr::Binary(
            Box::new(Expr::Binary(
                Box::new(ast::Expr::Parenthesized(vec![rebuild_and_expr_from_list(
                    vec![a_expr.clone(), x_expr.clone()],
                )])),
                Operator::Or,
                Box::new(ast::Expr::Parenthesized(vec![rebuild_and_expr_from_list(
                    vec![a_expr.clone(), y_expr.clone()],
                )])),
            )),
            Operator::Or,
            Box::new(ast::Expr::Parenthesized(vec![rebuild_and_expr_from_list(
                vec![a_expr.clone(), z_expr.clone()],
            )])),
        );

        let mut where_clause = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: None,
            consumed: false,
        }];

        lift_common_subexpressions_from_binary_or_terms(&mut where_clause)?;

        // Should now have 2 terms:
        // 1. (x = 1) OR (y = 1) OR (z = 1)
        // 2. a = 1
        let nonconsumed_terms = where_clause
            .iter()
            .filter(|term| !term.consumed)
            .collect::<Vec<_>>();
        assert_eq!(nonconsumed_terms.len(), 2);
        assert_eq!(
            nonconsumed_terms[0].expr,
            Expr::Binary(
                Box::new(Expr::Binary(
                    Box::new(ast::Expr::Parenthesized(vec![x_expr])),
                    Operator::Or,
                    Box::new(ast::Expr::Parenthesized(vec![y_expr])),
                )),
                Operator::Or,
                Box::new(ast::Expr::Parenthesized(vec![z_expr])),
            )
        );
        assert_eq!(nonconsumed_terms[1].expr, a_expr);

        Ok(())
    }

    #[test]
    fn test_lift_common_subexpressions_no_common_terms() -> Result<()> {
        // Test case where there are no common terms between OR branches:
        // SELECT * FROM t WHERE (x = 1) OR (y = 1)
        // should remain unchanged.

        let x_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 0,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let y_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 1,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let or_expr = Expr::Binary(
            Box::new(ast::Expr::Parenthesized(vec![x_expr.clone()])),
            Operator::Or,
            Box::new(ast::Expr::Parenthesized(vec![y_expr.clone()])),
        );

        let mut where_clause = vec![WhereTerm {
            expr: or_expr.clone(),
            from_outer_join: None,
            consumed: false,
        }];

        lift_common_subexpressions_from_binary_or_terms(&mut where_clause)?;

        // Should remain unchanged since no common terms
        let nonconsumed_terms = where_clause
            .iter()
            .filter(|term| !term.consumed)
            .collect::<Vec<_>>();
        assert_eq!(nonconsumed_terms.len(), 1);
        assert_eq!(nonconsumed_terms[0].expr, or_expr);

        Ok(())
    }

    #[test]
    fn test_lift_common_subexpressions_from_outer_join() -> Result<()> {
        // Test case with from_outer_join flag set;
        // it should be retained in the new WhereTerms, for outer join correctness.

        let a_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 0,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let x_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 1,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let y_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 2,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let or_expr = Expr::Binary(
            Box::new(ast::Expr::Parenthesized(vec![rebuild_and_expr_from_list(
                vec![a_expr.clone(), x_expr.clone()],
            )])),
            Operator::Or,
            Box::new(ast::Expr::Parenthesized(vec![rebuild_and_expr_from_list(
                vec![a_expr.clone(), y_expr.clone()],
            )])),
        );

        let mut where_clause = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: Some(0), // Set from_outer_join
            consumed: false,
        }];

        lift_common_subexpressions_from_binary_or_terms(&mut where_clause)?;

        // Should have 2 terms, both with from_outer_join set
        let nonconsumed_terms = where_clause
            .iter()
            .filter(|term| !term.consumed)
            .collect::<Vec<_>>();
        assert_eq!(nonconsumed_terms.len(), 2);
        assert_eq!(
            nonconsumed_terms[0].expr,
            Expr::Binary(
                Box::new(ast::Expr::Parenthesized(vec![x_expr])),
                Operator::Or,
                Box::new(ast::Expr::Parenthesized(vec![y_expr]))
            )
        );
        assert_eq!(nonconsumed_terms[0].from_outer_join, Some(0));
        assert_eq!(nonconsumed_terms[1].expr, a_expr);
        assert_eq!(nonconsumed_terms[1].from_outer_join, Some(0));

        Ok(())
    }

    #[test]
    fn test_lift_common_subexpressions_single_term() -> Result<()> {
        // Test case with a single non-OR term:
        // SELECT * FROM t WHERE a = 1
        // should remain unchanged.

        let single_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: 0,
                column: 0,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let mut where_clause = vec![WhereTerm {
            expr: single_expr.clone(),
            from_outer_join: None,
            consumed: false,
        }];

        lift_common_subexpressions_from_binary_or_terms(&mut where_clause)?;

        // Should remain unchanged
        let nonconsumed_terms = where_clause
            .iter()
            .filter(|term| !term.consumed)
            .collect::<Vec<_>>();
        assert_eq!(nonconsumed_terms.len(), 1);
        assert_eq!(nonconsumed_terms[0].expr, single_expr);

        Ok(())
    }

    #[test]
    fn test_lift_common_subexpressions_empty_or_branch() -> Result<()> {
        // Test case where OR becomes redundant:
        // (a = 1 AND b = 1) OR (a = 1) becomes -> a = 1.
        let exprs = (0..=1)
            .map(|i| {
                Expr::Binary(
                    Box::new(Expr::Column {
                        database: None,
                        table: 0,
                        column: i,
                        is_rowid_alias: false,
                    }),
                    Operator::Equals,
                    Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
                )
            })
            .collect::<Vec<_>>();

        let a_expr = exprs[0].clone();
        let b_expr = exprs[1].clone();

        let a_and_b_expr = Expr::Binary(
            Box::new(a_expr.clone()),
            Operator::And,
            Box::new(b_expr.clone()),
        );

        let or_expr = Expr::Binary(
            Box::new(a_and_b_expr),
            Operator::Or,
            Box::new(a_expr.clone()),
        );

        let mut where_clause = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: None,
            consumed: false,
        }];

        lift_common_subexpressions_from_binary_or_terms(&mut where_clause)?;

        let nonconsumed_terms = where_clause
            .iter()
            .filter(|term| !term.consumed)
            .collect::<Vec<_>>();
        assert_eq!(nonconsumed_terms.len(), 1);
        assert_eq!(nonconsumed_terms[0].expr, a_expr);

        Ok(())
    }
}
