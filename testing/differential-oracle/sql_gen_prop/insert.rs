//! INSERT statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::expression::{Expression, ExpressionContext, ExpressionProfile};
use crate::function::builtin_functions;
use crate::profile::StatementProfile;
use crate::schema::{Schema, TableRef};

// =============================================================================
// INSERT STATEMENT PROFILE
// =============================================================================

/// Profile for controlling INSERT statement generation.
#[derive(Debug, Clone)]
pub struct InsertProfile {
    /// Maximum depth for expressions in VALUES.
    pub expression_max_depth: u32,
    /// Whether to allow aggregate functions (usually false for INSERT).
    pub allow_aggregates: bool,
    /// Expression profile for value expressions.
    pub expression_profile: ExpressionProfile,
}

impl Default for InsertProfile {
    fn default() -> Self {
        Self {
            expression_max_depth: 2,
            allow_aggregates: false,
            expression_profile: ExpressionProfile::default(),
        }
    }
}

impl InsertProfile {
    /// Builder method to set expression max depth.
    pub fn with_expression_max_depth(mut self, depth: u32) -> Self {
        self.expression_max_depth = depth;
        self
    }

    /// Builder method to set whether aggregates are allowed.
    pub fn with_aggregates(mut self, allow: bool) -> Self {
        self.allow_aggregates = allow;
        self
    }

    /// Builder method to set expression profile.
    pub fn with_expression_profile(mut self, profile: ExpressionProfile) -> Self {
        self.expression_profile = profile;
        self
    }
}

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
        write!(f, "INSERT INTO {}", self.table)?;

        if !self.columns.is_empty() {
            let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
            write!(f, " ({})", cols.join(", "))?;
        }

        write!(f, " VALUES (")?;
        let vals: Vec<String> = self.values.iter().map(|v| v.to_string()).collect();
        write!(f, "{})", vals.join(", "))
    }
}

/// Generate an INSERT statement for a table with profile.
pub fn insert_for_table(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<InsertStatement> {
    let table_name = table.qualified_name();
    let columns = table.columns.clone();
    let functions = builtin_functions();

    // Extract profile values from the InsertProfile
    let insert_profile = profile.insert_profile();
    let expression_max_depth = insert_profile.expression_max_depth;
    let allow_aggregates = insert_profile.allow_aggregates;

    let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

    // Build expression context (no column refs or subqueries for INSERT values)
    let expr_profile = ExpressionProfile::default().with_subqueries_disabled();
    let ctx = ExpressionContext::new(functions, schema.clone())
        .with_max_depth(expression_max_depth)
        .with_aggregates(allow_aggregates)
        .with_profile(expr_profile);

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
    use crate::profile::StatementProfile;
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
        assert_eq!(sql, "INSERT INTO users (id, name) VALUES (1, 'Alice')");
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
            "INSERT INTO users (id, name) VALUES (1, UPPER('alice'))"
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
                );
                // Use empty schema - INSERT values don't need to reference other tables
                let schema = Schema::default();
                let table_ref: TableRef = table.into();
                insert_for_table(&table_ref, &schema, &StatementProfile::default())
            }
        ) {
            let sql = stmt.to_string();
            proptest::prop_assert!(sql.starts_with("INSERT INTO test"));
            proptest::prop_assert!(sql.contains("VALUES"));
        }
    }
}
