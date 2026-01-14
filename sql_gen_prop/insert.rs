//! INSERT statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::expression::{Expression, ExpressionContext};
use crate::function::builtin_functions;
use crate::schema::TableRef;

/// An INSERT statement.
#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    /// The values to insert. These can be literals, function calls, or other expressions.
    pub values: Vec<Expression>,
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

/// Generate an INSERT statement for a table with expression support.
///
/// This generates function calls and other expressions in the VALUES clause.
pub fn insert_for_table(table: &TableRef) -> BoxedStrategy<InsertStatement> {
    let table_name = table.name.clone();
    let columns = table.columns.clone();
    let functions = builtin_functions();

    let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

    // Build expression context (no column refs for INSERT values, no aggregates)
    let ctx = ExpressionContext::new(functions)
        .with_max_depth(2)
        .with_aggregates(false);

    let value_strategies: Vec<BoxedStrategy<Expression>> = columns
        .iter()
        .map(|c| crate::expression::expression_for_type(Some(&c.data_type), &ctx))
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
    use crate::schema::{ColumnDef, DataType, Table};
    use crate::value::SqlValue;

    #[test]
    fn test_insert_display() {
        let stmt = InsertStatement {
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            values: vec![
                Expression::Value(SqlValue::Integer(1)),
                Expression::Value(SqlValue::Text("Alice".to_string())),
            ],
        };

        let sql = stmt.to_string();
        assert_eq!(
            sql,
            "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'Alice')"
        );
    }

    #[test]
    fn test_insert_with_function() {
        let stmt = InsertStatement {
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            values: vec![
                Expression::Value(SqlValue::Integer(1)),
                Expression::function_call(
                    "UPPER",
                    vec![Expression::Value(SqlValue::Text("alice".to_string()))],
                ),
            ],
        };

        let sql = stmt.to_string();
        assert_eq!(
            sql,
            "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, UPPER('alice'))"
        );
    }

    proptest::proptest! {
        #[test]
        fn generated_insert_is_valid(
            stmt in {
                let table = Table::new(
                    "test",
                    vec![
                        ColumnDef::new("id", DataType::Integer).primary_key(),
                        ColumnDef::new("name", DataType::Text),
                    ],
                ).into();
                insert_for_table(&table)
            }
        ) {
            let sql = stmt.to_string();
            proptest::prop_assert!(sql.starts_with("INSERT INTO \"test\""));
            proptest::prop_assert!(sql.contains("VALUES"));
        }
    }
}
