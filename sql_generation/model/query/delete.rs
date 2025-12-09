use std::fmt::Display;

use serde::{Deserialize, Serialize};
use turso_parser::ast::{Expr, Indexed, Limit, QualifiedName, ResultColumn, SortedColumn, With};

use super::predicate::Predicate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Delete {
    pub table: String,
    pub predicate: Predicate,
}

impl
    From<(
        Option<With>,
        QualifiedName,
        Option<Indexed>,
        Option<Box<Expr>>,
        Vec<ResultColumn>,
        Vec<SortedColumn>,
        Option<Limit>,
    )> for Delete
{
    fn from(
        (with, tbl_name, indexed, where_clause, returning, order_by, limit): (
            Option<With>,
            QualifiedName,
            Option<Indexed>,
            Option<Box<Expr>>,
            Vec<ResultColumn>,
            Vec<SortedColumn>,
            Option<Limit>,
        ),
    ) -> Self {
        if with.is_some() {
            panic!("WITH clause in DELETE not supported");
        }

        if indexed.is_some() {
            panic!("INDEXED BY clause in DELETE not supported");
        }

        if !returning.is_empty() {
            panic!("RETURNING clause in DELETE not supported");
        }

        if order_by.len() > 0 {
            panic!("ORDER BY clause in DELETE not supported");
        }

        if limit.is_some() {
            panic!("LIMIT clause in DELETE not supported");
        }

        Delete {
            table: tbl_name.to_string(),
            predicate: where_clause.map_or(Predicate::true_(), |wc| Predicate(*wc)),
        }
    }
}

impl Display for Delete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DELETE FROM {} WHERE {}", self.table, self.predicate)
    }
}
