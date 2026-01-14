//! UPDATE statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::condition::{ConditionProfile, optional_where_clause};
use crate::expression::{Expression, ExpressionContext, ExpressionProfile};
use crate::function::builtin_functions;
use crate::schema::{ColumnDef, Schema, TableRef};

// =============================================================================
// UPDATE STATEMENT PROFILE
// =============================================================================

/// Profile for controlling UPDATE statement generation.
#[derive(Debug, Clone)]
pub struct UpdateProfile {
    /// Maximum depth for expressions in SET values.
    pub expression_max_depth: u32,
    /// Whether to allow aggregate functions (usually false for UPDATE).
    pub allow_aggregates: bool,
    /// Expression profile for SET expressions.
    pub expression_profile: ExpressionProfile,
    /// Condition profile for WHERE clause.
    pub condition_profile: ConditionProfile,
}

impl Default for UpdateProfile {
    fn default() -> Self {
        Self {
            expression_max_depth: 2,
            allow_aggregates: false,
            expression_profile: ExpressionProfile::default(),
            condition_profile: ConditionProfile::default(),
        }
    }
}

impl UpdateProfile {
    /// Create a profile for simple UPDATE (values only, no functions).
    pub fn simple() -> Self {
        Self {
            expression_max_depth: 0,
            allow_aggregates: false,
            expression_profile: ExpressionProfile::simple(),
            condition_profile: ConditionProfile::simple(),
        }
    }

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

    /// Builder method to set condition profile.
    pub fn with_condition_profile(mut self, profile: ConditionProfile) -> Self {
        self.condition_profile = profile;
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
        write!(f, "UPDATE \"{}\" SET ", self.table)?;

        let sets: Vec<String> = self
            .assignments
            .iter()
            .map(|(col, val)| format!("\"{col}\" = {val}"))
            .collect();
        write!(f, "{}", sets.join(", "))?;

        if let Some(cond) = &self.where_clause {
            write!(f, " WHERE {cond}")?;
        }

        Ok(())
    }
}

/// Generate an UPDATE statement for a table with profile.
pub fn update_for_table(
    table: &TableRef,
    schema: &Schema,
    profile: &UpdateProfile,
) -> BoxedStrategy<UpdateStatement> {
    let table_name = table.name.clone();
    let updatable: Vec<ColumnDef> = table.updatable_columns().cloned().collect();
    let functions = builtin_functions();

    // Extract profile values
    let expression_max_depth = profile.expression_max_depth;
    let allow_aggregates = profile.allow_aggregates;
    let condition_profile = profile.condition_profile.clone();

    let table_clone = table.clone();
    let schema_clone = schema.clone();
    if updatable.is_empty() {
        return optional_where_clause(&table_clone, &schema_clone, &condition_profile)
            .prop_map(move |where_clause| UpdateStatement {
                table: table_name.clone(),
                assignments: vec![],
                where_clause,
            })
            .boxed();
    }

    // Build expression context with columns (allows `SET x = x + 1` style expressions)
    let ctx = ExpressionContext::new(functions)
        .with_columns(table.columns.clone())
        .with_max_depth(expression_max_depth)
        .with_aggregates(allow_aggregates);

    let col_indices: Vec<usize> = (0..updatable.len()).collect();
    let updatable_clone = updatable.clone();

    (
        proptest::sample::subsequence(col_indices, 1..=updatable.len()),
        optional_where_clause(&table_clone, &schema_clone, &condition_profile),
    )
        .prop_flat_map(move |(indices, where_clause)| {
            let selected_cols: Vec<&ColumnDef> =
                indices.iter().map(|&i| &updatable_clone[i]).collect();

            let assignment_strategies: Vec<BoxedStrategy<(String, Expression)>> = selected_cols
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Table;
    use crate::expression::BinaryOperator;
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
        assert_eq!(
            sql,
            "UPDATE \"users\" SET \"name\" = 'Bob', \"age\" = 30 WHERE \"id\" = 1"
        );
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
        assert_eq!(sql, "UPDATE \"users\" SET \"name\" = UPPER(\"name\")");
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
                update_for_table(&table_ref, &schema, &UpdateProfile::default())
            }
        ) {
            let sql = stmt.to_string();
            proptest::prop_assert!(sql.starts_with("UPDATE \"test\" SET"));
        }
    }
}
