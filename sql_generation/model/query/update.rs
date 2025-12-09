use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::model::table::SimValue;

use super::predicate::Predicate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Update {
    pub table: String,
    pub set_values: Vec<(String, SimValue)>, // Pair of value for set expressions => SET name=value
    pub predicate: Predicate,
}

impl Update {
    pub fn table(&self) -> &str {
        &self.table
    }
}

impl From<turso_parser::ast::Update> for Update {
    fn from(update: turso_parser::ast::Update) -> Self {
        if update.with.is_some() {
            todo!("WITH clause in UPDATE not supported");
        }
        if update.or_conflict.is_some() {
            todo!("OR CONFLICT clause in UPDATE not supported");
        }
        if update.indexed.is_some() {
            todo!("INDEXED BY clause in UPDATE not supported");
        }
        if !update.returning.is_empty() {
            todo!("RETURNING clause in UPDATE not supported");
        }
        if !update.order_by.is_empty() {
            todo!("ORDER BY clause in UPDATE not supported");
        }
        if update.limit.is_some() {
            todo!("LIMIT clause in UPDATE not supported");
        }

        Update {
            table: update.tbl_name.to_string(),
            set_values: update
                .sets
                .into_iter()
                .map(|set_clause| {
                    let value = SimValue::from(*set_clause.expr);
                    let col_name = set_clause.col_names[0].to_string();
                    if set_clause.col_names.len() > 1 {
                        todo!(
                            "Only single column names are supported in UPDATE SET clauses, got {}",
                            set_clause.col_names.len()
                        );
                    }
                    (col_name, value)
                })
                .collect(),
            predicate: update
                .where_clause
                .map_or(Predicate::true_(), |wc| Predicate(*wc)),
        }
    }
}

impl Display for Update {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UPDATE {} SET ", self.table)?;
        for (i, (name, value)) in self.set_values.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{name} = {value}")?;
        }
        write!(f, " WHERE {}", self.predicate)?;
        Ok(())
    }
}
