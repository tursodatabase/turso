//! SQL generation library using proptest.
//!
//! This crate provides composable strategies for generating valid SQL statements
//! given a schema definition. It supports:
//!
//! - **DML**: SELECT, INSERT, UPDATE, DELETE
//! - **DDL**: CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX, ALTER TABLE,
//!   CREATE VIEW, DROP VIEW, CREATE TRIGGER, DROP TRIGGER
//! - **Transaction control**: BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE
//! - **Utility**: VACUUM, ANALYZE, REINDEX
//! - **Expressions**: Function calls, column references, literals, and nested expressions
//!
//! All DDL generators are schema-aware to avoid naming conflicts.

pub mod alter_table;
pub mod condition;
pub mod create_index;
pub mod create_table;
pub mod create_trigger;
pub mod delete;
pub mod drop_index;
pub mod drop_table;
pub mod drop_trigger;
pub mod expression;
pub mod function;
pub mod generator;
pub mod insert;
pub mod profile;
pub mod result;
pub mod schema;
pub mod select;
pub mod statement;
pub mod transaction;
pub mod update;
pub mod utility;
pub mod value;
pub mod view;

// Re-export main types for convenience
pub use alter_table::{
    AlterTableContext, AlterTableOp, AlterTableOpKind, AlterTableOpWeights, AlterTableStatement,
};
pub use condition::{
    ComparisonOp, Condition, ConditionProfile, LogicalOp, OrderByItem, OrderDirection,
    SubqueryProfile, SubqueryQuantifier,
};
pub use create_index::{CreateIndexStatement, IndexColumn};
pub use create_table::{ColumnProfile, CreateTableStatement, DataTypeWeights, PrimaryKeyProfile};
pub use create_trigger::{
    CreateTriggerContext, CreateTriggerKind, CreateTriggerOpWeights, CreateTriggerStatement,
    TriggerEvent, TriggerTiming,
};
pub use delete::DeleteStatement;
pub use drop_index::DropIndexStatement;
pub use drop_table::DropTableStatement;
pub use drop_trigger::DropTriggerStatement;
pub use expression::{
    BinaryOperator, Expression, ExpressionContext, ExpressionKind, ExpressionProfile, UnaryOperator,
};
pub use function::{
    Arity, FunctionCategory, FunctionContext, FunctionDef, FunctionProfile, FunctionRegistry,
};
pub use generator::{SqlGeneratorKind, WeightedKindIteratorExt};
pub use insert::InsertStatement;
pub use profile::{
    CreateIndexProfile, CreateTableProfile, CreateTriggerProfile, DeleteProfile,
    ExtendedExpressionProfile, ExtendedFunctionProfile, GenerationProfile, InsertProfile,
    SelectProfile, StatementProfile, UpdateProfile, ValueProfile, WeightedProfile,
};
pub use schema::{ColumnDef, DataType, Index, Schema, SchemaBuilder, Table, Trigger, View};
pub use select::SelectStatement;
pub use statement::{SqlStatement, StatementContext, StatementKind};
pub use transaction::{
    BeginStatement, CommitStatement, ReleaseStatement, RollbackStatement, SavepointStatement,
    TransactionType,
};
pub use update::UpdateStatement;
pub use utility::{AnalyzeStatement, ReindexStatement, VacuumStatement};
pub use value::SqlValue;
pub use view::{CreateViewStatement, DropViewStatement};

/// Strategies for generating SQL values and statements.
pub mod strategies {
    // ALTER TABLE
    pub use crate::alter_table::{
        alter_table_add_column, alter_table_drop_column, alter_table_for_schema,
        alter_table_rename_column, alter_table_rename_to,
    };
    // Conditions
    pub use crate::condition::{
        comparison_op,
        // Subquery strategies
        comparison_subquery_condition,
        condition_for_table,
        exists_condition,
        expression_condition,
        in_subquery_condition,
        logical_op,
        optional_where_clause,
        order_by_for_table,
        order_direction,
        simple_condition,
        subquery_select_for_table,
    };
    // CREATE INDEX
    pub use crate::create_index::{create_index, create_index_for_table, index_column};
    // CREATE TABLE
    pub use crate::create_table::{
        column_def, create_table, data_type, identifier, identifier_excluding,
        primary_key_column_def,
    };
    // DELETE
    pub use crate::delete::delete_for_table;
    // DROP INDEX
    pub use crate::drop_index::{drop_index, drop_index_named};
    // DROP TABLE
    pub use crate::drop_table::{drop_table, drop_table_for_schema, drop_table_for_table};
    // INSERT
    pub use crate::insert::insert_for_table;
    // SELECT
    pub use crate::select::select_for_table;
    // Statement union
    pub use crate::statement::{
        dml_for_schema, dml_for_table, dml_sequence, statement_for_schema, statement_for_table,
        statement_sequence,
    };
    // Transaction control
    pub use crate::transaction::{
        begin, commit, release, rollback, rollback_to_savepoint, savepoint, transaction_type,
    };
    // UPDATE
    pub use crate::update::update_for_table;
    // Utility statements
    pub use crate::utility::{analyze, analyze_for_schema, reindex, reindex_for_schema, vacuum};
    // Values
    pub use crate::value::{
        blob_value, integer_value, null_value, real_value, text_value, value_for_type,
    };
    // Views
    pub use crate::view::{create_view, drop_view, drop_view_for_schema};
    // Triggers
    pub use crate::create_trigger::{
        create_trigger_for_schema, create_trigger_for_table, create_trigger_with_timing_event,
    };
    pub use crate::drop_trigger::{drop_trigger, drop_trigger_for_schema};
    // Expressions
    pub use crate::expression::{expression, expression_for_type};
    // Functions
    pub use crate::function::{
        aggregate_function, aggregate_functions, blob_functions, builtin_functions,
        control_flow_functions, datetime_functions, function_from_registry, functions_in_category,
        math_functions, null_handling_functions, scalar_function, string_functions, type_functions,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn test_schema() -> Schema {
        SchemaBuilder::new()
            .add_table(Table::new(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("name", DataType::Text).not_null(),
                    ColumnDef::new("email", DataType::Text),
                    ColumnDef::new("age", DataType::Integer),
                    ColumnDef::new("balance", DataType::Real),
                ],
            ))
            .add_table(Table::new(
                "posts",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("user_id", DataType::Integer).not_null(),
                    ColumnDef::new("title", DataType::Text).not_null(),
                    ColumnDef::new("content", DataType::Text),
                    ColumnDef::new("data", DataType::Blob),
                ],
            ))
            .add_index(Index::new(
                "idx_users_email",
                "users",
                vec!["email".to_string()],
            ))
            .add_index(Index::new("idx_posts_user", "posts", vec!["user_id".to_string()]).unique())
            .build()
    }

    proptest! {
        #[test]
        fn generated_select_is_valid_sql(stmt in {
            let schema = test_schema();
            strategies::select_for_table(&schema.tables[0], &schema, &SelectProfile::default())
        }) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("SELECT"));
            prop_assert!(sql.contains("FROM \"users\""));
        }

        #[test]
        fn generated_insert_is_valid_sql(stmt in strategies::insert_for_table(&test_schema().tables[0], &InsertProfile::default())) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("INSERT INTO \"users\""));
            prop_assert!(sql.contains("VALUES"));
        }

        #[test]
        fn generated_update_is_valid_sql(stmt in {
            let schema = test_schema();
            strategies::update_for_table(&schema.tables[0], &schema, &UpdateProfile::default())
        }) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("UPDATE \"users\""));
        }

        #[test]
        fn generated_delete_is_valid_sql(stmt in {
            let schema = test_schema();
            strategies::delete_for_table(&schema.tables[0], &schema, &DeleteProfile::default())
        }) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("DELETE FROM \"users\""));
        }

        #[test]
        fn generated_create_table_avoids_conflicts(stmt in strategies::create_table(&test_schema(), &CreateTableProfile::default())) {
            // Should not generate a table named "users" or "posts"
            prop_assert!(stmt.table_name != "users");
            prop_assert!(stmt.table_name != "posts");
            prop_assert!(stmt.to_string().starts_with("CREATE TABLE"));
        }

        #[test]
        fn generated_create_index_is_valid_sql(stmt in strategies::create_index_for_table(&test_schema().tables[0], &test_schema(), &CreateIndexProfile::default())) {
            let sql = stmt.to_string();
            prop_assert!(sql.contains("INDEX"));
            prop_assert!(sql.contains("ON \"users\""));
        }

        #[test]
        fn generated_create_index_for_schema_is_valid(stmt in strategies::create_index(&test_schema(), &CreateIndexProfile::default())) {
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
        fn generated_statement_for_schema(stmt in strategies::statement_for_schema(&test_schema(), &profile::StatementProfile::default())) {
            let sql = stmt.to_string();
            // Should be a valid SQL statement
            prop_assert!(!sql.is_empty());
        }

        #[test]
        fn generated_sequence_has_correct_length(stmts in strategies::statement_sequence(&test_schema(), &profile::StatementProfile::default(), 5..10)) {
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
