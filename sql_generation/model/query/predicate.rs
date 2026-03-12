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
        let mut predicates = predicates.into_iter();
        let Some(first) = predicates.next() else {
            return Self::true_();
        };

        predicates.fold(first.parens(), |lhs, rhs| {
            let expr = ast::Expr::Binary(Box::new(lhs.0), ast::Operator::And, Box::new(rhs.0));
            Self(expr).parens()
        })
    }

    pub fn or(predicates: Vec<Predicate>) -> Self {
        let mut predicates = predicates.into_iter();
        let Some(first) = predicates.next() else {
            return Self::false_();
        };

        predicates.fold(first.parens(), |lhs, rhs| {
            let expr = ast::Expr::Binary(Box::new(lhs.0), ast::Operator::Or, Box::new(rhs.0));
            Self(expr).parens()
        })
    }

    pub fn eq(lhs: Predicate, rhs: Predicate) -> Self {
        let expr = ast::Expr::Binary(Box::new(lhs.0), ast::Operator::Equals, Box::new(rhs.0));
        Self(expr).parens()
    }

    pub fn is(lhs: Predicate, rhs: Predicate) -> Self {
        let expr = ast::Expr::Binary(Box::new(lhs.0), ast::Operator::Is, Box::new(rhs.0));
        Self(expr).parens()
    }

    /// Create a predicate from a column reference
    pub fn column(name: String) -> Self {
        Self(ast::Expr::Id(ast::Name::exact(name)))
    }

    /// Create a predicate from a SimValue literal
    pub fn value(val: SimValue) -> Self {
        Self(ast::Expr::Literal(val.into()))
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
            let col_name = col_name.as_str();
            assert_eq!(row.len(), table.columns().count());
            table
                .columns()
                .position(|c| c.column.name == col_name)
                .map(|idx| row[idx].clone())
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
        ast::Expr::FunctionCall {
            name,
            distinctness: _,
            args,
            order_by: _,
            filter_over: _,
        } => {
            let name = name.as_str();
            if name.eq_ignore_ascii_case("abs") && args.len() == 1 {
                let val = expr_to_value(args[0].as_ref(), row, table)?;
                val.0.exec_abs().ok().map(SimValue)
            } else if (name.eq_ignore_ascii_case("substr")
                || name.eq_ignore_ascii_case("substring"))
                && (args.len() == 2 || args.len() == 3)
            {
                let value = expr_to_value(args[0].as_ref(), row, table)?;
                let start = expr_to_value(args[1].as_ref(), row, table)?;
                let len = args
                    .get(2)
                    .and_then(|a| expr_to_value(a.as_ref(), row, table));
                let out = turso_core::Value::exec_substring(
                    &value.0,
                    &start.0,
                    len.as_ref().map(|v| &v.0),
                );
                Some(SimValue(out))
            } else {
                None
            }
        }
        _ => unreachable!("{:?}", expr),
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.displayer(&BlankContext).fmt(f)
    }
}
