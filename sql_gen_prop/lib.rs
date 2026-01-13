//! SQL generation library using proptest.
//!
//! This crate provides composable strategies for generating valid SQL statements
//! given a schema definition. It supports SELECT, INSERT, UPDATE, DELETE,
//! CREATE TABLE, CREATE INDEX, DROP TABLE, and DROP INDEX operations.
//!
//! All DDL generators are schema-aware to avoid naming conflicts.

pub mod condition;
pub mod create_index;
pub mod create_table;
pub mod delete;
pub mod drop_index;
pub mod drop_table;
pub mod insert;
pub mod schema;
pub mod select;
pub mod statement;
pub mod update;
pub mod value;

// Re-export main types for convenience
pub use condition::{ComparisonOp, Condition, LogicalOp, OrderByItem, OrderDirection};
pub use create_index::{CreateIndexStatement, IndexColumn};
pub use create_table::{ColumnDef, CreateTableStatement};
pub use delete::DeleteStatement;
pub use drop_index::DropIndexStatement;
pub use drop_table::DropTableStatement;
pub use insert::InsertStatement;
pub use schema::{Column, DataType, Index, Schema, Table};
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
    pub use crate::create_index::{create_index, create_index_for_table, index_column};
    pub use crate::create_table::{
        column_def, create_table, data_type, identifier, identifier_excluding,
        primary_key_column_def,
    };
    pub use crate::delete::delete_for_table;
    pub use crate::drop_index::{drop_index, drop_index_named};
    pub use crate::drop_table::{drop_table, drop_table_for_schema, drop_table_for_table};
    pub use crate::insert::insert_for_table;
    pub use crate::select::select_for_table;
    pub use crate::statement::{
        dml_for_schema, dml_for_table, dml_sequence, statement_for_schema, statement_for_table,
        statement_sequence,
    };
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
            .add_index(Index::new(
                "idx_users_email",
                "users",
                vec!["email".to_string()],
            ))
            .add_index(Index::new("idx_posts_user", "posts", vec!["user_id".to_string()]).unique())
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
        fn generated_create_table_avoids_conflicts(stmt in strategies::create_table(&test_schema())) {
            // Should not generate a table named "users" or "posts"
            prop_assert!(stmt.table_name != "users");
            prop_assert!(stmt.table_name != "posts");
            prop_assert!(stmt.to_string().starts_with("CREATE TABLE"));
        }

        #[test]
        fn generated_create_index_is_valid_sql(stmt in strategies::create_index_for_table(&test_schema().tables[0], &test_schema())) {
            let sql = stmt.to_string();
            prop_assert!(sql.contains("INDEX"));
            prop_assert!(sql.contains("ON \"users\""));
        }

        #[test]
        fn generated_create_index_for_schema_is_valid(stmt in strategies::create_index(&test_schema())) {
            let sql = stmt.to_string();
            prop_assert!(sql.contains("INDEX"));
            // Should be on one of the existing tables
            prop_assert!(sql.contains("ON \"users\"") || sql.contains("ON \"posts\""));
        }

        #[test]
        fn generated_drop_table_is_valid_sql(stmt in strategies::drop_table_for_table(&test_schema().tables[0])) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("DROP TABLE"));
            prop_assert!(sql.contains("\"users\""));
        }

        #[test]
        fn generated_drop_index_is_valid_sql(stmt in strategies::drop_index()) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("DROP INDEX"));
        }

        #[test]
        fn generated_statement_for_schema(stmt in strategies::statement_for_schema(&test_schema())) {
            let sql = stmt.to_string();
            // Should be a valid SQL statement
            prop_assert!(!sql.is_empty());
        }

        #[test]
        fn generated_sequence_has_correct_length(stmts in strategies::statement_sequence(&test_schema(), 5..10)) {
            prop_assert!(stmts.len() >= 5 && stmts.len() < 10);
        }
    }

    #[test]
    fn test_schema_with_indexes() {
        let schema = test_schema();
        assert_eq!(schema.tables.len(), 2);
        assert_eq!(schema.indexes.len(), 2);
        assert_eq!(schema.table_names().len(), 2);
        assert_eq!(schema.index_names().len(), 2);
        assert!(schema.table_names().contains("users"));
        assert!(schema.index_names().contains("idx_users_email"));
        assert_eq!(schema.indexes_for_table("users").len(), 1);
        assert_eq!(schema.indexes_for_table("posts").len(), 1);
    }
}
