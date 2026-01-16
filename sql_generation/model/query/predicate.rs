use std::fmt::Display;

use serde::{Deserialize, Serialize};
use turso_parser::ast::{
    self,
    fmt::{BlankContext, ToTokens},
};

use crate::model::table::{SimValue, Table, TableContext};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Predicate(pub ast::Expr);

impl Predicate {
    pub fn true_() -> Self {
        Self(ast::Expr::Literal(ast::Literal::Keyword(
            "TRUE".to_string(),
        )))
    }

    pub fn false_() -> Self {
        Self(ast::Expr::Literal(ast::Literal::Keyword(
            "FALSE".to_string(),
        )))
    }
    pub fn null() -> Self {
        Self(ast::Expr::Literal(ast::Literal::Null))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(predicate: Predicate) -> Self {
        let expr = ast::Expr::Unary(ast::UnaryOperator::Not, Box::new(predicate.0));
        Self(expr).parens()
    }

    pub fn and(predicates: Vec<Predicate>) -> Self {
        if predicates.is_empty() {
            Self::true_()
        } else if predicates.len() == 1 {
            predicates.into_iter().next().unwrap().parens()
        } else {
            let expr = ast::Expr::Binary(
                Box::new(predicates[0].0.clone()),
                ast::Operator::And,
                Box::new(Self::and(predicates[1..].to_vec()).0),
            );
            Self(expr).parens()
        }
    }

    pub fn or(predicates: Vec<Predicate>) -> Self {
        if predicates.is_empty() {
            Self::false_()
        } else if predicates.len() == 1 {
            predicates.into_iter().next().unwrap().parens()
        } else {
            let expr = ast::Expr::Binary(
                Box::new(predicates[0].0.clone()),
                ast::Operator::Or,
                Box::new(Self::or(predicates[1..].to_vec()).0),
            );
            Self(expr).parens()
        }
    }

    pub fn eq(lhs: Predicate, rhs: Predicate) -> Self {
        let expr = ast::Expr::Binary(Box::new(lhs.0), ast::Operator::Equals, Box::new(rhs.0));
        Self(expr).parens()
    }

    pub fn is(lhs: Predicate, rhs: Predicate) -> Self {
        let expr = ast::Expr::Binary(Box::new(lhs.0), ast::Operator::Is, Box::new(rhs.0));
        Self(expr).parens()
    }

    pub fn parens(self) -> Self {
        let expr = ast::Expr::Parenthesized(vec![Box::new(self.0)]);
        Self(expr)
    }

    pub fn eval(&self, row: &[SimValue], table: &Table) -> Option<SimValue> {
        expr_to_value(&self.0, row, table)
    }

    pub fn test<T: TableContext>(&self, row: &[SimValue], table: &T) -> bool {
        let value = expr_to_value(&self.0, row, table);
        value.is_some_and(|value| value.as_bool())
    }
}

// TODO: In the future pass a Vec<Table> to support resolving a value from another table
// This function attempts to convert an simpler easily computable expression into values
// TODO: In the future, we can try to expand this computation if we want to support harder properties that require us
// to already know more values before hand
pub fn expr_to_value<T: TableContext>(
    expr: &ast::Expr,
    row: &[SimValue],
    table: &T,
) -> Option<SimValue> {
    match expr {
        ast::Expr::DoublyQualified(_, _, col_name)
        | ast::Expr::Qualified(_, col_name)
        | ast::Expr::Id(col_name) => {
            let columns = table.columns().collect::<Vec<_>>();
            let col_name = col_name.as_str();
            assert_eq!(row.len(), columns.len());
            columns
                .iter()
                .zip(row.iter())
                .find(|(column, _)| column.column.name == col_name)
                .map(|(_, value)| value)
                .cloned()
        }
        ast::Expr::Literal(literal) => Some(literal.into()),
        ast::Expr::Binary(lhs, op, rhs) => {
            let lhs = expr_to_value(lhs, row, table)?;
            let rhs = expr_to_value(rhs, row, table)?;
            Some(lhs.binary_compare(&rhs, *op))
        }
        ast::Expr::Like {
            lhs,
            not,
            op,
            rhs,
            escape: _, // TODO: support escape
        } => {
            let lhs = expr_to_value(lhs, row, table)?;
            let rhs = expr_to_value(rhs, row, table)?;
            let res = lhs.like_compare(&rhs, *op).ok()?;
            let value: SimValue = if *not { !res } else { res }.into();
            Some(value)
        }
        ast::Expr::Unary(op, expr) => {
            let value = expr_to_value(expr, row, table)?;
            Some(value.unary_exec(*op))
        }
        ast::Expr::Parenthesized(exprs) => {
            assert_eq!(exprs.len(), 1);
            expr_to_value(&exprs[0], row, table)
        }
        _ => unreachable!("{:?}", expr),
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.displayer(&BlankContext).fmt(f)
    }
}
