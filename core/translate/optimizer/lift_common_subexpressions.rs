use crate::{turso_assert, turso_assert_greater_than};
use turso_parser::ast::{Expr, Operator};

use crate::{
    translate::{expr::unwrap_parens_owned, plan::WhereTerm},
    util::exprs_are_equivalent,
    Result,
};
/// Adds useful top-level filters from OR terms.
///
/// `(a AND b) OR (a AND c)` becomes `a AND (b OR c)`.
/// `(a = 1 AND b = 2) OR (a = 2 AND b = 1)` also adds
/// `a IN (1, 2)` and `b IN (2, 1)`. The exact OR term stays in place.
pub(crate) fn simplify_binary_or_terms(where_clause: &mut Vec<WhereTerm>) -> Result<()> {
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

        turso_assert!(or_operands.len() > 1);

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

        if term_from_outer_join.is_none() {
            for expr in implied_value_filters(&all_or_operands_conjunct_lists) {
                if !where_clause
                    .iter()
                    .any(|term| exprs_are_equivalent(&term.expr, &expr))
                {
                    where_clause.push(WhereTerm {
                        expr,
                        from_outer_join: None,
                        consumed: false,
                    });
                }
            }
        }

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
                top_level_expr = Expr::Parenthesized(vec![top_level_expr.into()]);
                num_unwrapped_parens -= 1;
            }
            new_or_operands_for_original_term.push(top_level_expr);
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

fn implied_value_filters(or_branches: &[(Vec<Expr>, usize)]) -> Vec<Expr> {
    let mut columns = Vec::new();
    for conjunct in &or_branches[0].0 {
        let Some((column, _)) = column_literal_equality(conjunct) else {
            continue;
        };
        if !columns
            .iter()
            .any(|other| exprs_are_equivalent(other, column))
        {
            columns.push(column.clone());
        }
    }

    columns
        .into_iter()
        .filter_map(|column| {
            let mut values = Vec::new();
            for (branch, _) in or_branches {
                let branch_values = branch.iter().filter_map(|conjunct| {
                    let (other_column, value) = column_literal_equality(conjunct)?;
                    exprs_are_equivalent(&column, other_column).then_some(value)
                });

                let mut found_value = false;
                for value in branch_values {
                    found_value = true;
                    if !values
                        .iter()
                        .any(|other| exprs_are_equivalent(other, value))
                    {
                        values.push(value.clone());
                    }
                }
                if !found_value {
                    return None;
                }
            }

            (values.len() > 1).then(|| Expr::InList {
                lhs: Box::new(column),
                not: false,
                rhs: values.into_iter().map(Box::new).collect(),
            })
        })
        .collect()
}

fn column_literal_equality(expr: &Expr) -> Option<(&Expr, &Expr)> {
    let Expr::Binary(lhs, Operator::Equals, rhs) = expr else {
        return None;
    };

    match (lhs.as_ref(), rhs.as_ref()) {
        (column @ Expr::Column { .. }, value @ Expr::Literal(literal))
        | (value @ Expr::Literal(literal), column @ Expr::Column { .. })
            if safe_literal(literal) =>
        {
            Some((column, value))
        }
        _ => None,
    }
}

fn safe_literal(literal: &turso_parser::ast::Literal) -> bool {
    use turso_parser::ast::Literal;

    matches!(
        literal,
        Literal::Numeric(_)
            | Literal::String(_)
            | Literal::Blob(_)
            | Literal::True
            | Literal::False
    )
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

/// Rebuild an ast::Expr::Binary(lhs, OR, rhs) for a list of operands.
fn rebuild_or_expr_from_list(mut operands: Vec<Expr>) -> Expr {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::plan::WhereTerm;
    use turso_parser::ast::{self, Expr, Literal, Operator, TableInternalId};

    #[test]
    fn test_lift_common_subexpressions() -> Result<()> {
        // SELECT * FROM t WHERE (a = 1 and x = 1 and b = 1) OR (a = 1 and y = 1 and b = 1)
        // should be rewritten to:
        // SELECT * FROM t WHERE (x = 1 OR y = 1) and a = 1 and b = 1

        // assume the table has 4 columns: a, b, x, y
        let a_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::default(),
                column: 0,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let b_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::default(),
                column: 1,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let x_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::default(),
                column: 2,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let y_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::default(),
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
            )
            .into()])),
            Operator::Or,
            Box::new(ast::Expr::Parenthesized(vec![rebuild_and_expr_from_list(
                vec![a_expr.clone(), y_expr.clone(), b_expr.clone()],
            )
            .into()])),
        );

        let mut where_clause = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: None,
            consumed: false,
        }];

        simplify_binary_or_terms(&mut where_clause)?;

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
                Box::new(ast::Expr::Parenthesized(vec![x_expr.into()])),
                Operator::Or,
                Box::new(ast::Expr::Parenthesized(vec![y_expr.into()]))
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
                table: TableInternalId::default(),
                column: 0,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let x_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::default(),
                column: 1,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let y_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::default(),
                column: 2,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let z_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::default(),
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
                )
                .into()])),
                Operator::Or,
                Box::new(ast::Expr::Parenthesized(vec![rebuild_and_expr_from_list(
                    vec![a_expr.clone(), y_expr.clone()],
                )
                .into()])),
            )),
            Operator::Or,
            Box::new(ast::Expr::Parenthesized(vec![rebuild_and_expr_from_list(
                vec![a_expr.clone(), z_expr.clone()],
            )
            .into()])),
        );

        let mut where_clause = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: None,
            consumed: false,
        }];

        simplify_binary_or_terms(&mut where_clause)?;

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
                    Box::new(ast::Expr::Parenthesized(vec![x_expr.into()])),
                    Operator::Or,
                    Box::new(ast::Expr::Parenthesized(vec![y_expr.into()])),
                )),
                Operator::Or,
                Box::new(ast::Expr::Parenthesized(vec![z_expr.into()])),
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
                table: TableInternalId::default(),
                column: 0,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let y_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::default(),
                column: 1,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let or_expr = Expr::Binary(
            Box::new(ast::Expr::Parenthesized(vec![x_expr.into()])),
            Operator::Or,
            Box::new(ast::Expr::Parenthesized(vec![y_expr.into()])),
        );

        let mut where_clause = vec![WhereTerm {
            expr: or_expr.clone(),
            from_outer_join: None,
            consumed: false,
        }];

        simplify_binary_or_terms(&mut where_clause)?;

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
                table: TableInternalId::default(),
                column: 0,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let x_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::default(),
                column: 1,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let y_expr = Expr::Binary(
            Box::new(Expr::Column {
                database: None,
                table: TableInternalId::default(),
                column: 2,
                is_rowid_alias: false,
            }),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric("1".to_string()))),
        );

        let or_expr = Expr::Binary(
            Box::new(ast::Expr::Parenthesized(vec![rebuild_and_expr_from_list(
                vec![a_expr.clone(), x_expr.clone()],
            )
            .into()])),
            Operator::Or,
            Box::new(ast::Expr::Parenthesized(vec![rebuild_and_expr_from_list(
                vec![a_expr.clone(), y_expr.clone()],
            )
            .into()])),
        );

        let mut where_clause = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: Some(TableInternalId::default()), // Set from_outer_join
            consumed: false,
        }];

        simplify_binary_or_terms(&mut where_clause)?;

        // Should have 2 terms, both with from_outer_join set
        let nonconsumed_terms = where_clause
            .iter()
            .filter(|term| !term.consumed)
            .collect::<Vec<_>>();
        assert_eq!(nonconsumed_terms.len(), 2);
        assert_eq!(
            nonconsumed_terms[0].expr,
            Expr::Binary(
                Box::new(ast::Expr::Parenthesized(vec![x_expr.into()])),
                Operator::Or,
                Box::new(ast::Expr::Parenthesized(vec![y_expr.into()]))
            )
        );
        assert_eq!(
            nonconsumed_terms[0].from_outer_join,
            Some(TableInternalId::default())
        );
        assert_eq!(nonconsumed_terms[1].expr, a_expr);
        assert_eq!(
            nonconsumed_terms[1].from_outer_join,
            Some(TableInternalId::default())
        );

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
                table: TableInternalId::default(),
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

        simplify_binary_or_terms(&mut where_clause)?;

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
                        table: TableInternalId::default(),
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

        let a_and_b_expr = Expr::Binary(Box::new(a_expr.clone()), Operator::And, Box::new(b_expr));

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

        simplify_binary_or_terms(&mut where_clause)?;

        let nonconsumed_terms = where_clause
            .iter()
            .filter(|term| !term.consumed)
            .collect::<Vec<_>>();
        assert_eq!(nonconsumed_terms.len(), 1);
        assert_eq!(nonconsumed_terms[0].expr, a_expr);

        Ok(())
    }

    #[test]
    fn crossed_equalities_add_allowed_value_filters() -> Result<()> {
        let a_one = column_equals_literal(0, "1");
        let a_two = column_equals_literal(0, "2");
        let b_one = column_equals_literal(1, "1");
        let b_two = column_equals_literal(1, "2");
        let or_expr = Expr::Binary(
            Box::new(rebuild_and_expr_from_list(vec![a_one, b_two])),
            Operator::Or,
            Box::new(rebuild_and_expr_from_list(vec![a_two, b_one])),
        );
        let mut where_clause = vec![WhereTerm {
            expr: or_expr.clone(),
            from_outer_join: None,
            consumed: false,
        }];

        simplify_binary_or_terms(&mut where_clause)?;

        assert_eq!(where_clause.len(), 3);
        assert_eq!(where_clause[0].expr, or_expr);
        assert!(where_clause
            .iter()
            .any(|term| { exprs_are_equivalent(&term.expr, &column_in_literals(0, &["1", "2"])) }));
        assert!(where_clause
            .iter()
            .any(|term| { exprs_are_equivalent(&term.expr, &column_in_literals(1, &["2", "1"])) }));
        Ok(())
    }

    #[test]
    fn a_missing_branch_does_not_add_an_allowed_value_filter() -> Result<()> {
        let or_expr = Expr::Binary(
            Box::new(rebuild_and_expr_from_list(vec![
                column_equals_literal(0, "1"),
                column_equals_literal(1, "2"),
            ])),
            Operator::Or,
            Box::new(column_equals_literal(1, "1")),
        );
        let mut where_clause = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: None,
            consumed: false,
        }];

        simplify_binary_or_terms(&mut where_clause)?;

        assert_eq!(where_clause.len(), 2);
        assert!(!where_clause
            .iter()
            .any(|term| matches!(&term.expr, Expr::InList { lhs, .. } if matches!(lhs.as_ref(), Expr::Column { column: 0, .. }))));
        assert!(where_clause
            .iter()
            .any(|term| { exprs_are_equivalent(&term.expr, &column_in_literals(1, &["2", "1"])) }));
        Ok(())
    }

    #[test]
    fn outer_join_terms_do_not_add_allowed_value_filters() -> Result<()> {
        let or_expr = Expr::Binary(
            Box::new(column_equals_literal(0, "1")),
            Operator::Or,
            Box::new(column_equals_literal(0, "2")),
        );
        let mut where_clause = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: Some(TableInternalId::default()),
            consumed: false,
        }];

        simplify_binary_or_terms(&mut where_clause)?;

        assert_eq!(where_clause.len(), 1);
        Ok(())
    }

    #[test]
    fn a_column_comparison_does_not_add_an_allowed_value_filter() -> Result<()> {
        let other_column = Expr::Column {
            database: None,
            table: TableInternalId::default(),
            column: 1,
            is_rowid_alias: false,
        };
        let or_expr = Expr::Binary(
            Box::new(column_equals_literal(0, "1")),
            Operator::Or,
            Box::new(Expr::Binary(
                Box::new(column(0)),
                Operator::Equals,
                Box::new(other_column),
            )),
        );
        let mut where_clause = vec![WhereTerm {
            expr: or_expr,
            from_outer_join: None,
            consumed: false,
        }];

        simplify_binary_or_terms(&mut where_clause)?;

        assert_eq!(where_clause.len(), 1);
        Ok(())
    }

    fn column_equals_literal(column: usize, value: &str) -> Expr {
        Expr::Binary(
            Box::new(self::column(column)),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric(value.to_owned()))),
        )
    }

    fn column_in_literals(column: usize, values: &[&str]) -> Expr {
        Expr::InList {
            lhs: Box::new(self::column(column)),
            not: false,
            rhs: values
                .iter()
                .map(|value| Box::new(Expr::Literal(Literal::Numeric((*value).to_owned()))))
                .collect(),
        }
    }

    fn column(column: usize) -> Expr {
        Expr::Column {
            database: None,
            table: TableInternalId::default(),
            column,
            is_rowid_alias: false,
        }
    }
}
