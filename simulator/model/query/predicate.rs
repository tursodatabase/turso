use std::fmt::Display;

use limbo_sqlite3_parser::{ast, to_sql_string::ToSqlString};
use serde::{Deserialize, Serialize};

use crate::model::{
    query::EmptyContext,
    table::{Table, Value},
};

macro_rules! assert_implemented_predicate_expr {
    ($val:expr) => {
        assert!(matches!(
            $val,
            ast::Expr::DoublyQualified(..) | ast::Expr::Qualified(..) | ast::Expr::Literal(..)
        ))
    };
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Predicate(ast::Expr);

impl Predicate {
    pub(crate) fn true_() -> Self {
        Self(ast::Expr::Literal(ast::Literal::Numeric("1".to_string())))
    }

    pub(crate) fn false_() -> Self {
        Self(ast::Expr::Literal(ast::Literal::Numeric("0".to_string())))
    }

    pub(crate) fn test(&self, row: &[Value], table: &Table) -> bool {
        match &self.0 {
            ast::Expr::Binary(lhs, operator, rhs) => {
                let lhs = expr_to_value(lhs, row, table);
                let rhs = expr_to_value(rhs, row, table);
                match (lhs, rhs) {
                    (Some(lhs), Some(rhs)) => lhs.binary_compare(&rhs, *operator),
                    _ => false,
                }
            }
            ast::Expr::Like {
                lhs,
                not,
                op,
                rhs,
                escape: _, // TODO: support escape
            } => {
                let lhs = expr_to_value(lhs, row, table);
                let rhs = expr_to_value(rhs, row, table);
                let res = match (lhs, rhs) {
                    (Some(lhs), Some(rhs)) => lhs.like_compare(&rhs, *op),
                    _ => false,
                };
                if *not {
                    !res
                } else {
                    res
                }
            }
            ast::Expr::Literal(literal) => Value::from(literal).into_bool(),
            ast::Expr::Unary(unary_operator, expr) => todo!(),
            expr => unimplemented!("{:?}", expr),
        }
    }
}

// TODO: In the future pass a Vec<Table> to support resolving a value from another table
// This function attempts to convert an simpler easily computable expression into values
// TODO: In the future, we can try to expand this computation if we want to support harder properties that require us
// to already know more values before hand
fn expr_to_value(expr: &ast::Expr, row: &[Value], table: &Table) -> Option<Value> {
    assert_implemented_predicate_expr!(expr);
    match expr {
        ast::Expr::DoublyQualified(_, _, col_name) | ast::Expr::Qualified(_, col_name) => table
            .columns
            .iter()
            .zip(row.iter())
            .find(|(column, _)| column.name == col_name.0)
            .map(|(_, value)| value)
            .cloned(),
        ast::Expr::Literal(literal) => Some(literal.into()),
        // TODO: add binary and unary
        _ => unreachable!("{:?}", expr),
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.to_sql_string(&EmptyContext))
    }
}
