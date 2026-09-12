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

    lift_common_subexpressions_from_binary_or_terms(&mut where_clause)?;

    let nonconsumed_terms = where_clause
        .iter()
        .filter(|term| !term.consumed)
        .collect::<Vec<_>>();
    assert_eq!(nonconsumed_terms.len(), 1);
    assert_eq!(nonconsumed_terms[0].expr, a_expr);

    Ok(())
}
