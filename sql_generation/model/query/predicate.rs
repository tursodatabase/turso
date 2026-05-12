use std::fmt::Display;

use serde::{Deserialize, Serialize};
use turso_parser::ast::{
    self,
    fmt::{BlankContext, ToTokens},
};

use crate::model::table::{ContextColumn, SimValue, Table, TableContext};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
        ast::Expr::DoublyQualified(db_name, table_name, col_name) => {
            let columns = table.columns().collect::<Vec<_>>();
            assert_eq!(row.len(), columns.len());
            find_column_value(
                row,
                &columns,
                ColumnQualifier::DbTable {
                    db_name: db_name.as_str(),
                    table_name: table_name.as_str(),
                },
                col_name.as_str(),
            )
        }
        ast::Expr::Qualified(table_name, col_name) => {
            let columns = table.columns().collect::<Vec<_>>();
            assert_eq!(row.len(), columns.len());
            find_column_value(
                row,
                &columns,
                ColumnQualifier::Table(table_name.as_str()),
                col_name.as_str(),
            )
        }
        ast::Expr::Id(col_name) => {
            let columns = table.columns().collect::<Vec<_>>();
            assert_eq!(row.len(), columns.len());
            find_column_value(row, &columns, ColumnQualifier::Any, col_name.as_str())
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
        ast::Expr::InList { lhs, not, rhs } => {
            let lhs = expr_to_value(lhs, row, table)?;
            let mut found = false;
            for expr in rhs {
                let candidate = expr_to_value(expr, row, table)?;
                if lhs == candidate {
                    found = true;
                    break;
                }
            }
            Some(if *not { !found } else { found }.into())
        }
        _ => unreachable!("{:?}", expr),
    }
}

#[derive(Clone, Copy)]
enum ColumnQualifier<'a> {
    Any,
    Table(&'a str),
    DbTable {
        db_name: &'a str,
        table_name: &'a str,
    },
}

fn find_column_value(
    row: &[SimValue],
    columns: &[ContextColumn<'_>],
    qualifier: ColumnQualifier<'_>,
    col_name: &str,
) -> Option<SimValue> {
    columns
        .iter()
        .zip(row.iter())
        .find(|(column, _)| {
            column.column.name == col_name && column_matches_qualifier(column.table_name, qualifier)
        })
        .map(|(_, value)| value)
        .cloned()
}

fn column_matches_qualifier(column_table_name: &str, qualifier: ColumnQualifier<'_>) -> bool {
    match qualifier {
        ColumnQualifier::Any => true,
        ColumnQualifier::Table(table_name) => column_table_name == table_name,
        ColumnQualifier::DbTable {
            db_name,
            table_name,
        } => column_table_name
            .split_once('.')
            .is_some_and(|(column_db, column_table)| {
                column_db == db_name && column_table == table_name
            }),
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.displayer(&BlankContext).fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use turso_core::types;
    use turso_parser::ast;

    use crate::model::table::{Column, ColumnType, JoinTable};

    use super::*;

    fn int_col(name: &str) -> Column {
        Column {
            name: name.to_string(),
            column_type: ColumnType::Integer,
            constraints: Vec::new(),
        }
    }

    #[test]
    fn qualified_columns_resolve_to_the_matching_table() {
        let src = Table {
            name: "src".to_string(),
            columns: vec![int_col("x")],
            rows: Vec::new(),
            indexes: Vec::new(),
        };
        let rhs = Table {
            name: "rhs".to_string(),
            columns: vec![int_col("x")],
            rows: Vec::new(),
            indexes: Vec::new(),
        };
        let join_table = JoinTable {
            tables: vec![src, rhs],
            rows: vec![vec![
                SimValue(types::Value::from_i64(10)),
                SimValue(types::Value::from_i64(20)),
            ]],
        };
        let row = &join_table.rows[0];

        let src_x =
            ast::Expr::Qualified(ast::Name::from_string("src"), ast::Name::from_string("x"));
        let rhs_x =
            ast::Expr::Qualified(ast::Name::from_string("rhs"), ast::Name::from_string("x"));
        let bare_x = ast::Expr::Id(ast::Name::from_string("x"));

        assert_eq!(
            expr_to_value(&src_x, row, &join_table),
            Some(SimValue(types::Value::from_i64(10)))
        );
        assert_eq!(
            expr_to_value(&rhs_x, row, &join_table),
            Some(SimValue(types::Value::from_i64(20)))
        );
        assert_eq!(
            expr_to_value(&bare_x, row, &join_table),
            Some(SimValue(types::Value::from_i64(10)))
        );
    }
}
