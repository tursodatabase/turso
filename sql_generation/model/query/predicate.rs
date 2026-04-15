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
        | ast::Expr::Id(col_name) => lookup_column_value(row, table, col_name.as_str()),
        ast::Expr::Literal(literal) => Some(literal.into()),
        ast::Expr::Binary(lhs, op, rhs) => {
            let lhs = expr_to_value(lhs, row, table)?;
            let rhs = expr_to_value(rhs, row, table)?;
            Some(lhs.binary_compare(&rhs, *op))
        }
        ast::Expr::Exists(select) => eval_exists_subquery(select, row, table),
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

fn lookup_column_value<T: TableContext>(
    row: &[SimValue],
    table: &T,
    column_name: &str,
) -> Option<SimValue> {
    let columns = table.columns().collect::<Vec<_>>();
    assert_eq!(row.len(), columns.len());
    columns
        .iter()
        .zip(row.iter())
        .find(|(column, _)| column.column.name == column_name)
        .map(|(_, value)| value)
        .cloned()
}

fn eval_exists_subquery<T: TableContext>(
    select: &ast::Select,
    outer_row: &[SimValue],
    table: &T,
) -> Option<SimValue> {
    let ast::Select {
        with: None,
        body:
            ast::SelectBody {
                select:
                    ast::OneSelect::Select {
                        from: Some(from),
                        where_clause,
                        ..
                    },
                compounds,
            },
        order_by,
        limit,
    } = select
    else {
        return None;
    };

    if !compounds.is_empty() || !order_by.is_empty() || limit.is_some() || !from.joins.is_empty() {
        return None;
    }

    let ast::SelectTable::Table(tbl_name, alias, _) = from.select.as_ref() else {
        return None;
    };

    let inner_table_name = alias
        .as_ref()
        .map(|a| a.name().as_str().to_string())
        .unwrap_or_else(|| tbl_name.name.as_str().to_string());
    let outer_table_name = table.columns().next().map(|c| c.table_name.to_string())?;

    let matches = table.rows().iter().any(|inner_row| {
        where_clause.as_ref().is_none_or(|where_expr| {
            eval_correlated_exists_expr(
                where_expr,
                outer_row,
                inner_row,
                table,
                &outer_table_name,
                &inner_table_name,
            )
            .is_some_and(|v| v.as_bool())
        })
    });

    Some(if matches {
        SimValue::TRUE
    } else {
        SimValue::FALSE
    })
}

fn eval_correlated_exists_expr<T: TableContext>(
    expr: &ast::Expr,
    outer_row: &[SimValue],
    inner_row: &[SimValue],
    table: &T,
    outer_table_name: &str,
    inner_table_name: &str,
) -> Option<SimValue> {
    match expr {
        ast::Expr::Literal(literal) => Some(literal.into()),
        ast::Expr::Unary(op, expr) => {
            let value = eval_correlated_exists_expr(
                expr,
                outer_row,
                inner_row,
                table,
                outer_table_name,
                inner_table_name,
            )?;
            Some(value.unary_exec(*op))
        }
        ast::Expr::Parenthesized(exprs) => {
            assert_eq!(exprs.len(), 1);
            eval_correlated_exists_expr(
                &exprs[0],
                outer_row,
                inner_row,
                table,
                outer_table_name,
                inner_table_name,
            )
        }
        ast::Expr::Binary(lhs, op, rhs) => {
            let lhs = eval_correlated_exists_expr(
                lhs,
                outer_row,
                inner_row,
                table,
                outer_table_name,
                inner_table_name,
            )?;
            let rhs = eval_correlated_exists_expr(
                rhs,
                outer_row,
                inner_row,
                table,
                outer_table_name,
                inner_table_name,
            )?;
            Some(lhs.binary_compare(&rhs, *op))
        }
        ast::Expr::Qualified(table_name, col_name)
        | ast::Expr::DoublyQualified(_, table_name, col_name) => {
            let table_name = table_name.as_str();
            if table_name == inner_table_name {
                lookup_column_value(inner_row, table, col_name.as_str())
            } else if table_name == outer_table_name {
                lookup_column_value(outer_row, table, col_name.as_str())
            } else {
                None
            }
        }
        ast::Expr::Id(col_name) => lookup_column_value(inner_row, table, col_name.as_str())
            .or_else(|| lookup_column_value(outer_row, table, col_name.as_str())),
        _ => None,
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.displayer(&BlankContext).fmt(f)
    }
}
