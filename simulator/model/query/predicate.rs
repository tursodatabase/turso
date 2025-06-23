use std::fmt::Display;

use serde::{Deserialize, Serialize};
use turso_sqlite3_parser::{ast, to_sql_string::ToSqlString};

use crate::model::{
    query::EmptyContext,
    table::{SimValue, Table},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Predicate(pub ast::Expr);

impl Predicate {
    pub(crate) fn true_() -> Self {
        Self(ast::Expr::Literal(ast::Literal::Numeric("1".to_string())))
    }

    pub(crate) fn false_() -> Self {
        Self(ast::Expr::Literal(ast::Literal::Numeric("0".to_string())))
    }

    pub(crate) fn test(&self, row: &[SimValue], table: &Table) -> bool {
        let value = expr_to_value(&self.0, row, table);
        value.is_some_and(|value| value.as_bool())
    }
}

// TODO: In the future pass a Vec<Table> to support resolving a value from another table
// This function attempts to convert an simpler easily computable expression into values
// TODO: In the future, we can try to expand this computation if we want to support harder properties that require us
// to already know more values before hand
pub fn expr_to_value(expr: &ast::Expr, row: &[SimValue], table: &Table) -> Option<SimValue> {
    match expr {
        ast::Expr::DoublyQualified(_, _, ast::Name(col_name))
        | ast::Expr::Qualified(_, ast::Name(col_name))
        | ast::Expr::Id(ast::Id(col_name)) => {
            assert_eq!(row.len(), table.columns.len());
            table
                .columns
                .iter()
                .zip(row.iter())
                .find(|(column, _)| column.name == *col_name)
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
            let res = lhs.like_compare(&rhs, *op);
            let value: SimValue = if *not { !res } else { res }.into();
            Some(value)
        }
        ast::Expr::Unary(op, expr) => {
            let value = expr_to_value(expr, row, table)?;
            Some(value.unary_exec(*op))
        }
        _ => unreachable!("{:?}", expr),
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.to_sql_string(&EmptyContext))
    }
}
