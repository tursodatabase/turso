//! SQL generation library using proptest.
//!
//! This crate provides composable strategies for generating valid SQL statements
//! given a schema definition. It supports SELECT, INSERT, UPDATE, and DELETE operations.

pub mod condition;
pub mod delete;
pub mod insert;
pub mod schema;
pub mod select;
pub mod statement;
pub mod update;
pub mod value;

// Re-export main types for convenience
pub use condition::{ComparisonOp, Condition, LogicalOp, OrderByItem, OrderDirection};
pub use delete::DeleteStatement;
pub use insert::InsertStatement;
pub use schema::{Column, DataType, Schema, Table};
pub use select::SelectStatement;
pub use statement::SqlStatement;
pub use update::UpdateStatement;
pub use value::SqlValue;

/// Strategies for generating SQL values and statements.
pub mod strategies {
    pub use crate::condition::{
        comparison_op, condition_for_table, logical_op, optional_where_clause, order_by_for_table,
        order_direction, simple_condition,
    };
    pub use crate::delete::delete_for_table;
    pub use crate::insert::insert_for_table;
    pub use crate::select::select_for_table;
    pub use crate::statement::{statement_for_schema, statement_for_table, statement_sequence};
    pub use crate::update::update_for_table;
    pub use crate::value::{
        blob_value, integer_value, null_value, real_value, text_value, value_for_type,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn test_schema() -> Schema {
        Schema::new()
            .add_table(Table::new(
                "users",
                vec![
                    Column::new("id", DataType::Integer).primary_key(),
                    Column::new("name", DataType::Text).not_null(),
                    Column::new("email", DataType::Text),
                    Column::new("age", DataType::Integer),
                    Column::new("balance", DataType::Real),
                ],
            ))
            .add_table(Table::new(
                "posts",
                vec![
                    Column::new("id", DataType::Integer).primary_key(),
                    Column::new("user_id", DataType::Integer).not_null(),
                    Column::new("title", DataType::Text).not_null(),
                    Column::new("content", DataType::Text),
                    Column::new("data", DataType::Blob),
                ],
            ))
    }

    proptest! {
        #[test]
        fn generated_select_is_valid_sql(stmt in strategies::select_for_table(&test_schema().tables[0])) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("SELECT"));
            prop_assert!(sql.contains("FROM \"users\""));
        }

        #[test]
        fn generated_insert_is_valid_sql(stmt in strategies::insert_for_table(&test_schema().tables[0])) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("INSERT INTO \"users\""));
            prop_assert!(sql.contains("VALUES"));
        }

        #[test]
        fn generated_update_is_valid_sql(stmt in strategies::update_for_table(&test_schema().tables[0])) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("UPDATE \"users\""));
        }

        #[test]
        fn generated_delete_is_valid_sql(stmt in strategies::delete_for_table(&test_schema().tables[0])) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("DELETE FROM \"users\""));
        }

        #[test]
        fn generated_statement_for_schema(stmt in strategies::statement_for_schema(&test_schema())) {
            let sql = stmt.to_string();
            prop_assert!(sql.contains("\"users\"") || sql.contains("\"posts\""));
        }

        #[test]
        fn generated_sequence_has_correct_length(stmts in strategies::statement_sequence(&test_schema(), 5..10)) {
            prop_assert!(stmts.len() >= 5 && stmts.len() < 10);
        }
    }
}
