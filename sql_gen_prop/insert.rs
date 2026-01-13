//! INSERT statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::schema::Table;
use crate::value::{SqlValue, value_for_type};

/// An INSERT statement.
#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<SqlValue>,
}

impl fmt::Display for InsertStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "INSERT INTO \"{}\"", self.table)?;

        if !self.columns.is_empty() {
            let cols: Vec<String> = self.columns.iter().map(|c| format!("\"{c}\"")).collect();
            write!(f, " ({})", cols.join(", "))?;
        }

        write!(f, " VALUES (")?;
        let vals: Vec<String> = self.values.iter().map(|v| v.to_string()).collect();
        write!(f, "{})", vals.join(", "))
    }
}

/// Generate an INSERT statement for a table.
pub fn insert_for_table(table: &Table) -> BoxedStrategy<InsertStatement> {
    let table_name = table.name.clone();
    let columns = table.columns.clone();

    let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

    let value_strategies: Vec<BoxedStrategy<SqlValue>> = columns
        .iter()
        .map(|c| value_for_type(&c.data_type, c.nullable))
        .collect();

    value_strategies
        .into_iter()
        .collect::<Vec<_>>()
        .prop_map(move |values| InsertStatement {
            table: table_name.clone(),
            columns: col_names.clone(),
            values,
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_display() {
        let stmt = InsertStatement {
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            values: vec![SqlValue::Integer(1), SqlValue::Text("Alice".to_string())],
        };

        let sql = stmt.to_string();
        assert_eq!(
            sql,
            "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'Alice')"
        );
    }
}
