//! UPDATE statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::expression::{Expression, ExpressionContext, ExpressionProfile};
use crate::function::builtin_functions;
use crate::profile::StatementProfile;
use crate::schema::{ColumnDef, Schema, TableRef};
use crate::select::optional_where_clause;

// =============================================================================
// UPDATE STATEMENT PROFILE
// =============================================================================

/// Profile for controlling UPDATE statement generation.
///
/// UPDATE statements use the condition settings from the global
/// `StatementProfile.generation.expression.base` for WHERE clause generation.
#[derive(Debug, Clone)]
pub struct UpdateProfile {
    /// Maximum depth for expressions in SET values.
    pub expression_max_depth: u32,
    /// Whether to allow aggregate functions (usually false for UPDATE).
    pub allow_aggregates: bool,
    /// Expression profile for SET expressions.
    pub expression_profile: ExpressionProfile,
    /// Probability (0-100) of including generated columns in UPDATE SET clause.
    /// This tests that both DBs reject the invalid UPDATE consistently.
    /// Default is 1 (1%).
    pub include_generated_probability: u8,
}

impl Default for UpdateProfile {
    fn default() -> Self {
        Self {
            expression_max_depth: 2,
            allow_aggregates: false,
            expression_profile: ExpressionProfile::default(),
            include_generated_probability: 1,
        }
    }
}

impl UpdateProfile {
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

    /// Builder method to set probability of including generated columns.
    pub fn with_include_generated_probability(mut self, probability: u8) -> Self {
        self.include_generated_probability = probability.min(100);
        self
    }
}

/// An UPDATE statement.
#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table: String,
    /// Column assignments as (column_name, expression) pairs.
    pub assignments: Vec<(String, Expression)>,
    pub where_clause: Option<Expression>,
}

impl fmt::Display for UpdateStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UPDATE {} SET ", self.table)?;

        let sets: Vec<String> = self
            .assignments
            .iter()
            .map(|(col, val)| format!("{col} = {val}"))
            .collect();
        write!(f, "{}", sets.join(", "))?;

        if let Some(cond) = &self.where_clause {
            write!(f, " WHERE {cond}")?;
        }

        Ok(())
    }
}

/// Generate an UPDATE statement for a table with profile.
///
/// By default, generated columns are excluded from the SET clause since
/// SQLite/Turso will reject attempts to set them. With `include_generated_probability`,
/// we occasionally include them to test that both databases reject consistently.
pub fn update_for_table(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<UpdateStatement> {
    let table_name = table.name.clone();
    let functions = builtin_functions();

    // Extract profile values from the UpdateProfile
    let update_profile = profile.update_profile();
    let expression_max_depth = update_profile.expression_max_depth;
    let allow_aggregates = update_profile.allow_aggregates;
    let include_generated_prob = update_profile.include_generated_probability;

    let table_clone = table.clone();
    let schema_clone = schema.clone();
    let profile_clone = profile.clone();

    // Decide whether to include generated columns (for error testing)
    (0u8..100)
        .prop_flat_map(move |roll| {
            let include_generated = roll < include_generated_prob;

            // Select columns based on whether we're including generated columns
            let updatable: Vec<ColumnDef> = if include_generated {
                // Include all non-PK columns (including generated - will cause error)
                table_clone
                    .columns
                    .iter()
                    .filter(|c| !c.primary_key)
                    .cloned()
                    .collect()
            } else {
                // Normal case: exclude both PK and generated columns
                table_clone.updatable_columns().cloned().collect()
            };

            if updatable.is_empty() {
                let table_name = table_name.clone();
                return optional_where_clause(&table_clone, &schema_clone, &profile_clone)
                    .prop_map(move |where_clause| UpdateStatement {
                        table: table_name.clone(),
                        assignments: vec![],
                        where_clause,
                    })
                    .boxed();
            }

            // Build expression context with columns (allows `SET x = x + 1` style expressions)
            // Disable subqueries to avoid infinite recursion
            let expr_profile = profile_clone
                .generation
                .expression
                .base
                .clone()
                .with_subqueries_disabled();
            let ctx = ExpressionContext::new(functions.clone(), schema_clone.clone())
                .with_columns(table_clone.columns.clone())
                .with_max_depth(expression_max_depth)
                .with_aggregates(allow_aggregates)
                .with_profile(expr_profile);

            let col_indices: Vec<usize> = (0..updatable.len()).collect();
            let updatable_clone = updatable.clone();
            let table_name = table_name.clone();
            let table_clone = table_clone.clone();
            let schema_clone = schema_clone.clone();
            let profile_clone = profile_clone.clone();

            (
                proptest::sample::subsequence(col_indices, 1..=updatable.len()),
                optional_where_clause(&table_clone, &schema_clone, &profile_clone),
            )
                .prop_flat_map(move |(indices, where_clause)| {
                    let selected_cols: Vec<&ColumnDef> =
                        indices.iter().map(|&i| &updatable_clone[i]).collect();

                    let assignment_strategies: Vec<BoxedStrategy<(String, Expression)>> =
                        selected_cols
                            .iter()
                            .map(|c| {
                                let name = c.name.clone();
                                crate::expression::expression_for_type(Some(&c.data_type), &ctx)
                                    .prop_map(move |expr| (name.clone(), expr))
                                    .boxed()
                            })
                            .collect();

                    let table_name = table_name.clone();
                    assignment_strategies
                        .into_iter()
                        .collect::<Vec<_>>()
                        .prop_map(move |assignments| UpdateStatement {
                            table: table_name.clone(),
                            assignments,
                            where_clause: where_clause.clone(),
                        })
                })
                .boxed()
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Table;
    use crate::expression::BinaryOperator;
    use crate::profile::StatementProfile;
    use crate::schema::DataType;
    use crate::value::SqlValue;

    #[test]
    fn test_update_display() {
        let stmt = UpdateStatement {
            table: "users".to_string(),
            assignments: vec![
                (
                    "name".to_string(),
                    Expression::Value(SqlValue::Text("Bob".to_string())),
                ),
                ("age".to_string(), Expression::Value(SqlValue::Integer(30))),
            ],
            where_clause: Some(Expression::binary(
                Expression::Column("id".to_string()),
                BinaryOperator::Eq,
                Expression::Value(SqlValue::Integer(1)),
            )),
        };

        let sql = stmt.to_string();
        assert_eq!(sql, "UPDATE users SET name = 'Bob', age = 30 WHERE id = 1");
    }

    #[test]
    fn test_update_with_expression() {
        let stmt = UpdateStatement {
            table: "users".to_string(),
            assignments: vec![(
                "name".to_string(),
                Expression::function_call("UPPER", vec![Expression::Column("name".to_string())]),
            )],
            where_clause: None,
        };

        let sql = stmt.to_string();
        assert_eq!(sql, "UPDATE users SET name = UPPER(name)");
    }

    proptest::proptest! {
        #[test]
        fn generated_update_is_valid(
            stmt in {
                let table = Table::new(
                    "test",
                    vec![
                        ColumnDef::new("id", DataType::Integer).primary_key(),
                        ColumnDef::new("name", DataType::Text),
                        ColumnDef::new("age", DataType::Integer),
                    ],
                );
                let schema = crate::schema::SchemaBuilder::new().add_table(table.clone()).build();
                let table_ref: crate::schema::TableRef = table.into();
                update_for_table(&table_ref, &schema, &StatementProfile::default())
            }
        ) {
            let sql = stmt.to_string();
            proptest::prop_assert!(sql.starts_with("UPDATE test SET"));
        }
    }
}
