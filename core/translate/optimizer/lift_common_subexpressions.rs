use crate::{turso_assert, turso_assert_greater_than};
use turso_parser::ast::{Expr, Operator, TableInternalId, UnaryOperator};

use crate::{
    translate::{
        expr::{unwrap_parens, unwrap_parens_owned},
        plan::WhereTerm,
    },
    util::exprs_are_equivalent,
    Result,
};

use super::AvailableIndexes;

/// Adds implied `IN` filters and moves shared `AND` terms out of `OR` expressions.
///
/// For `(a.x = 1 AND b.y = 2) OR (a.x = 2 AND b.y = 1)`, the planner
/// can use `a.x IN (1, 2)` and `b.y IN (1, 2)` to search each table.
/// The original `OR` remains because the new filters also accept `(1, 1)`.
pub(crate) fn rewrite_or_terms(
    where_clause: &mut Vec<WhereTerm>,
    available_indexes: &AvailableIndexes,
) -> Result<()> {
    let mut term_index = 0;
    while term_index < where_clause.len() {
        if !matches!(
            where_clause[term_index].expr,
            Expr::Binary(_, Operator::Or, _)
        ) {
            term_index += 1;
            continue;
        }
        let or_expr = where_clause[term_index].expr.clone();
        let term_origin = where_clause[term_index].origin;

        let or_branches = flatten_or_expr_owned(or_expr)?;

        turso_assert!(or_branches.len() > 1);

        let mut branch_terms = Vec::with_capacity(or_branches.len());
        let mut branch_parentheses = Vec::with_capacity(or_branches.len());
        for expr in or_branches {
            let (expr, parenthesis_count) = unwrap_parens_owned(expr)?;
            branch_terms.push(flatten_and_expr_owned(expr)?);
            branch_parentheses.push(parenthesis_count);
        }

        for expr in in_filters_implied_by_or_branches(&branch_terms, available_indexes) {
            if !where_clause
                .iter()
                .any(|term| exprs_are_equivalent(&term.expr, &expr))
            {
                where_clause.push(WhereTerm {
                    expr,
                    // Keep this filter in the ON clause so it cannot remove
                    // unmatched rows after the join.
                    origin: term_origin,
                    consumed: false,
                });
            }
        }

        let mut common_terms = branch_terms[0].clone();

        for branch in branch_terms.iter().skip(1) {
            common_terms.retain(|common_expr| {
                branch
                    .iter()
                    .any(|expr| exprs_are_equivalent(common_expr, expr))
            });
        }

        if common_terms.is_empty() {
            term_index += 1;
            continue;
        }

        let mut remaining_branches = Vec::new();
        let mut one_branch_only_has_common_terms = false;
        for (mut branch, parenthesis_count) in branch_terms.into_iter().zip(branch_parentheses) {
            branch.retain(|expr| !common_terms.contains(expr));

            if branch.is_empty() {
                // `(a AND b) OR a OR (a AND c)` is equal to `a`.
                one_branch_only_has_common_terms = true;
                break;
            }

            let remaining_expr = rebuild_and_expr_from_list(branch);
            remaining_branches.push(restore_parentheses(remaining_expr, parenthesis_count));
        }

        if one_branch_only_has_common_terms {
            where_clause[term_index].consumed = true;
        } else {
            turso_assert_greater_than!(remaining_branches.len(), 1);
            where_clause[term_index].expr = rebuild_or_expr_from_list(remaining_branches);
        }

        for common_term in common_terms {
            where_clause.push(WhereTerm {
                expr: common_term,
                origin: term_origin,
                consumed: false,
            });
        }

        term_index += 1;
    }
    Ok(())
}

/// Returns `IN` filters that hold for every `OR` branch.
///
/// If several columns belong to one table, it returns one filter only when one column starts an index.
/// If a compound index starts with two candidate columns, it returns no filter for that table.
fn in_filters_implied_by_or_branches(
    or_branches: &[Vec<Expr>],
    available_indexes: &AvailableIndexes,
) -> Vec<Expr> {
    // A filter that is true for every branch must use a column from the first branch.
    let mut candidate_columns = Vec::new();
    for term in &or_branches[0] {
        let Some((column, _)) = column_literal_equality(term) else {
            continue;
        };
        if !candidate_columns
            .iter()
            .any(|other| exprs_are_equivalent(other, column))
        {
            candidate_columns.push(column.clone());
        }
    }

    let filters = candidate_columns
        .into_iter()
        .filter_map(|column| {
            let Expr::Column {
                table,
                column: column_pos,
                ..
            } = column
            else {
                unreachable!("implied IN filters require a column")
            };
            let mut values = Vec::new();
            for branch in or_branches {
                let branch_values = branch.iter().filter_map(|term| {
                    let (other_column, value) = column_literal_equality(term)?;
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
                    // This branch does not restrict the column. A new filter
                    // on that column can reject rows from this branch.
                    return None;
                }
            }

            // The common-term rewrite handles a column that has one value.
            (values.len() > 1).then(|| {
                (
                    table,
                    column_pos,
                    Expr::InList {
                        lhs: Box::new(column),
                        not: false,
                        rhs: values.into_iter().map(Box::new).collect(),
                    },
                )
            })
        })
        .collect::<Vec<_>>();

    filters
        .iter()
        .filter_map(|(table, column_pos, filter)| {
            let same_table_columns = filters
                .iter()
                .filter_map(|(other_table, other_column, _)| {
                    (other_table == table).then_some(*other_column)
                })
                .collect::<Vec<_>>();
            if same_table_columns.len() == 1
                || sole_indexed_column(*table, &same_table_columns, available_indexes)
                    == Some(*column_pos)
            {
                Some(filter.clone())
            } else {
                None
            }
        })
        .collect()
}

fn sole_indexed_column(
    table: TableInternalId,
    columns: &[usize],
    available_indexes: &AvailableIndexes,
) -> Option<usize> {
    let indexes = available_indexes.indexes_for_table(table)?;
    let indexes = indexes.iter().filter(|index| index.index_method.is_none());

    if indexes.clone().any(|index| {
        index.columns.len() > 1
            && index.columns[0].expr.is_none()
            && index.columns[1].expr.is_none()
            && columns.contains(&index.columns[0].pos_in_table)
            && columns.contains(&index.columns[1].pos_in_table)
    }) {
        return None;
    }

    let mut indexed = columns.iter().copied().filter(|column| {
        indexes.clone().any(|index| {
            index.where_clause.is_none() && index.column_table_pos_to_index_pos(*column) == Some(0)
        })
    });
    let column = indexed.next()?;
    indexed.next().is_none().then_some(column)
}

/// Returns the column and value from a simple equality.
///
/// The column can be on either side of `=`.
/// The value must be a numeric, text, BLOB, or Boolean literal.
fn column_literal_equality(expr: &Expr) -> Option<(&Expr, &Expr)> {
    let Expr::Binary(lhs, Operator::Equals, rhs) = unwrap_parens(expr).ok()? else {
        return None;
    };

    let lhs_expr = unwrap_parens(lhs).ok()?;
    let rhs_expr = unwrap_parens(rhs).ok()?;
    match (lhs_expr, rhs_expr) {
        (column @ Expr::Column { .. }, value) if literal_expr_can_be_copied(value) => {
            Some((column, rhs))
        }
        (value, column @ Expr::Column { .. }) if literal_expr_can_be_copied(value) => {
            Some((column, lhs))
        }
        _ => None,
    }
}

fn literal_expr_can_be_copied(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(literal) => literal_can_be_copied(literal),
        Expr::Unary(UnaryOperator::Negative | UnaryOperator::Positive, value) => {
            matches!(
                unwrap_parens(value),
                Ok(Expr::Literal(turso_parser::ast::Literal::Numeric(_)))
            )
        }
        _ => false,
    }
}

/// Selects the literal types that the rewrite copies into a new `IN` filter.
///
/// `NULL` cannot satisfy `=`. The rewrite leaves all other literal types in the original expression.
fn literal_can_be_copied(literal: &turso_parser::ast::Literal) -> bool {
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
    let (expr, parenthesis_count) = unwrap_parens_owned(expr)?;
    let Expr::Binary(lhs, Operator::Or, rhs) = expr else {
        return Ok(vec![restore_parentheses(expr, parenthesis_count)]);
    };
    let mut flattened = flatten_or_expr_owned(*lhs)?;
    flattened.extend(flatten_or_expr_owned(*rhs)?);
    Ok(flattened)
}

/// Flatten an ast::Expr::Binary(lhs, AND, rhs) into a list of conjuncts.
fn flatten_and_expr_owned(expr: Expr) -> Result<Vec<Expr>> {
    let (expr, parenthesis_count) = unwrap_parens_owned(expr)?;
    let Expr::Binary(lhs, Operator::And, rhs) = expr else {
        return Ok(vec![restore_parentheses(expr, parenthesis_count)]);
    };
    let mut flattened = flatten_and_expr_owned(*lhs)?;
    flattened.extend(flatten_and_expr_owned(*rhs)?);
    Ok(flattened)
}

fn restore_parentheses(mut expr: Expr, mut count: usize) -> Expr {
    while count > 0 {
        expr = Expr::Parenthesized(vec![expr.into()]);
        count -= 1;
    }
    expr
}

/// Rebuild an ast::Expr::Binary(lhs, AND, rhs) for a list of conjuncts.
fn rebuild_and_expr_from_list(mut terms: Vec<Expr>) -> Expr {
    turso_assert!(!terms.is_empty());

    if terms.len() == 1 {
        return terms.pop().unwrap();
    }

    let mut current_expr = terms.remove(0);
    for next_expr in terms {
        current_expr = Expr::Binary(Box::new(current_expr), Operator::And, Box::new(next_expr));
    }
    current_expr
}

/// Rebuild an ast::Expr::Binary(lhs, OR, rhs) for a list of operands.
fn rebuild_or_expr_from_list(mut branches: Vec<Expr>) -> Expr {
    turso_assert!(!branches.is_empty());

    if branches.len() == 1 {
        return branches.pop().unwrap();
    }

    let mut current_expr = branches.remove(0);
    for next_expr in branches {
        current_expr = Expr::Binary(Box::new(current_expr), Operator::Or, Box::new(next_expr));
    }
    current_expr
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::translate::plan::{JoinOrigin, WhereTerm, WhereTermOrigin};
    use turso_parser::ast::{self, Expr, Literal, Operator, TableInternalId};

    #[test]
    fn common_and_terms_move_out_of_two_or_branches() -> Result<()> {
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
            origin: WhereTermOrigin::Where,
            consumed: false,
        }];

        rewrite_or_terms(&mut where_clause, &AvailableIndexes::default())?;

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
    fn common_and_terms_move_out_of_three_or_branches() -> Result<()> {
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
            origin: WhereTermOrigin::Where,
            consumed: false,
        }];

        rewrite_or_terms(&mut where_clause, &AvailableIndexes::default())?;

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
    fn or_without_common_terms_stays_unchanged() -> Result<()> {
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
            origin: WhereTermOrigin::Where,
            consumed: false,
        }];

        rewrite_or_terms(&mut where_clause, &AvailableIndexes::default())?;

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
    fn moved_outer_join_terms_keep_their_source_join() -> Result<()> {
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
            origin: WhereTermOrigin::Join(JoinOrigin::Outer(TableInternalId::default())),
            consumed: false,
        }];

        rewrite_or_terms(&mut where_clause, &AvailableIndexes::default())?;

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
            nonconsumed_terms[0].origin.join_origin(),
            Some(JoinOrigin::Outer(TableInternalId::default()))
        );
        assert_eq!(nonconsumed_terms[1].expr, a_expr);
        assert_eq!(
            nonconsumed_terms[1].origin.join_origin(),
            Some(JoinOrigin::Outer(TableInternalId::default()))
        );

        Ok(())
    }

    #[test]
    fn non_or_term_stays_unchanged() -> Result<()> {
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
            origin: WhereTermOrigin::Where,
            consumed: false,
        }];

        rewrite_or_terms(&mut where_clause, &AvailableIndexes::default())?;

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
    fn or_is_removed_when_one_branch_only_has_common_terms() -> Result<()> {
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
            origin: WhereTermOrigin::Where,
            consumed: false,
        }];

        rewrite_or_terms(&mut where_clause, &AvailableIndexes::default())?;

        let nonconsumed_terms = where_clause
            .iter()
            .filter(|term| !term.consumed)
            .collect::<Vec<_>>();
        assert_eq!(nonconsumed_terms.len(), 1);
        assert_eq!(nonconsumed_terms[0].expr, a_expr);

        Ok(())
    }

    #[test]
    fn two_columns_from_one_table_do_not_add_independent_in_filters() -> Result<()> {
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
            origin: WhereTermOrigin::Where,
            consumed: false,
        }];

        rewrite_or_terms(&mut where_clause, &AvailableIndexes::default())?;

        assert_eq!(where_clause.len(), 1);
        assert_eq!(where_clause[0].expr, or_expr);
        Ok(())
    }

    #[test]
    fn one_column_from_each_table_adds_both_in_filters() -> Result<()> {
        let first_table = TableInternalId::from(1);
        let second_table = TableInternalId::from(2);
        let a_one = table_column_equals_literal(first_table, 0, "1");
        let a_two = table_column_equals_literal(first_table, 0, "2");
        let b_one = table_column_equals_literal(second_table, 0, "1");
        let b_two = table_column_equals_literal(second_table, 0, "2");
        let or_expr = Expr::Binary(
            Box::new(rebuild_and_expr_from_list(vec![a_one, b_two])),
            Operator::Or,
            Box::new(rebuild_and_expr_from_list(vec![a_two, b_one])),
        );
        let mut where_clause = vec![WhereTerm {
            expr: or_expr.clone(),
            origin: WhereTermOrigin::Where,
            consumed: false,
        }];

        rewrite_or_terms(&mut where_clause, &AvailableIndexes::default())?;

        assert_eq!(where_clause.len(), 3);
        assert_eq!(where_clause[0].expr, or_expr);
        assert!(where_clause.iter().any(|term| {
            exprs_are_equivalent(
                &term.expr,
                &table_column_in_literals(first_table, 0, &["1", "2"]),
            )
        }));
        assert!(where_clause.iter().any(|term| {
            exprs_are_equivalent(
                &term.expr,
                &table_column_in_literals(second_table, 0, &["2", "1"]),
            )
        }));
        Ok(())
    }

    #[test]
    fn column_missing_from_one_branch_does_not_add_in_filter() -> Result<()> {
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
            origin: WhereTermOrigin::Where,
            consumed: false,
        }];

        rewrite_or_terms(&mut where_clause, &AvailableIndexes::default())?;

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
    fn outer_join_in_filter_keeps_join_origin() -> Result<()> {
        let or_expr = Expr::Binary(
            Box::new(column_equals_literal(0, "1")),
            Operator::Or,
            Box::new(column_equals_literal(0, "2")),
        );
        let mut where_clause = vec![WhereTerm {
            expr: or_expr,
            origin: WhereTermOrigin::Join(JoinOrigin::Outer(TableInternalId::default())),
            consumed: false,
        }];

        rewrite_or_terms(&mut where_clause, &AvailableIndexes::default())?;

        assert_eq!(where_clause.len(), 2);
        assert_eq!(where_clause[1].origin, where_clause[0].origin);
        Ok(())
    }

    #[test]
    fn inner_join_in_filter_keeps_join_origin() -> Result<()> {
        let or_expr = Expr::Binary(
            Box::new(column_equals_literal(0, "1")),
            Operator::Or,
            Box::new(column_equals_literal(0, "2")),
        );
        let mut where_clause = vec![WhereTerm {
            expr: or_expr,
            origin: WhereTermOrigin::Join(JoinOrigin::Inner(TableInternalId::default())),
            consumed: false,
        }];

        rewrite_or_terms(&mut where_clause, &AvailableIndexes::default())?;

        assert_eq!(where_clause.len(), 2);
        assert_eq!(where_clause[1].origin, where_clause[0].origin);
        Ok(())
    }

    #[test]
    fn column_to_column_equality_does_not_add_in_filter() -> Result<()> {
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
            origin: WhereTermOrigin::Where,
            consumed: false,
        }];

        rewrite_or_terms(&mut where_clause, &AvailableIndexes::default())?;

        assert_eq!(where_clause.len(), 1);
        Ok(())
    }

    fn column_equals_literal(column: usize, value: &str) -> Expr {
        table_column_equals_literal(TableInternalId::default(), column, value)
    }

    fn table_column_equals_literal(table: TableInternalId, column: usize, value: &str) -> Expr {
        Expr::Binary(
            Box::new(table_column(table, column)),
            Operator::Equals,
            Box::new(Expr::Literal(Literal::Numeric(value.to_owned()))),
        )
    }

    fn column_in_literals(column: usize, values: &[&str]) -> Expr {
        table_column_in_literals(TableInternalId::default(), column, values)
    }

    fn table_column_in_literals(table: TableInternalId, column: usize, values: &[&str]) -> Expr {
        Expr::InList {
            lhs: Box::new(table_column(table, column)),
            not: false,
            rhs: values
                .iter()
                .map(|value| Box::new(Expr::Literal(Literal::Numeric((*value).to_owned()))))
                .collect(),
        }
    }

    fn column(column: usize) -> Expr {
        table_column(TableInternalId::default(), column)
    }

    fn table_column(table: TableInternalId, column: usize) -> Expr {
        Expr::Column {
            database: None,
            table,
            column,
            is_rowid_alias: false,
        }
    }
}
