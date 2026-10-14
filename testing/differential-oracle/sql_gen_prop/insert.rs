//! INSERT statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::expression::{Expression, ExpressionContext, ExpressionProfile};
use crate::function::builtin_functions;
use crate::profile::StatementProfile;
use crate::schema::{Schema, TableRef};
use crate::spelling::table_name_spelling;

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

/// What an INSERT does when the new row conflicts with an existing one.
#[derive(Debug, Clone)]
pub enum OnConflict {
    /// Fail with a constraint error.
    Abort,
    /// `INSERT OR REPLACE`: delete the existing row, then insert the new one.
    Replace,
    /// `ON CONFLICT(<key>) DO UPDATE SET c = excluded.c` for each column in `set`.
    Update { key: Vec<String>, set: Vec<String> },
}

/// An INSERT statement.
#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    /// The values to insert. These can be literals, function calls, or other expressions.
    pub values: Vec<Expression>,
    pub on_conflict: OnConflict,
}

impl fmt::Display for InsertStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.on_conflict {
            OnConflict::Replace => write!(f, "INSERT OR REPLACE INTO {}", self.table)?,
            OnConflict::Abort | OnConflict::Update { .. } => {
                write!(f, "INSERT INTO {}", self.table)?
            }
        }

        if !self.columns.is_empty() {
            let cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
            write!(f, " ({})", cols.join(", "))?;
        }

        write!(f, " VALUES (")?;
        let vals: Vec<String> = self.values.iter().map(|v| v.to_string()).collect();
        write!(f, "{})", vals.join(", "))?;

        if let OnConflict::Update { key, set } = &self.on_conflict {
            let assignments: Vec<String> =
                set.iter().map(|c| format!("{c} = excluded.{c}")).collect();
            write!(
                f,
                " ON CONFLICT({}) DO UPDATE SET {}",
                key.join(", "),
                assignments.join(", ")
            )?;
        }
        Ok(())
    }
}

/// Generate an INSERT statement for a table with profile.
pub fn insert_for_table(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<InsertStatement> {
    insert_with_conflict(table, schema, profile, OnConflict::Abort)
}

/// Generate an `INSERT OR REPLACE` for a table with a primary key.
pub fn insert_or_replace_for_table(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<InsertStatement> {
    assert!(
        table.columns.iter().any(|c| c.primary_key),
        "INSERT OR REPLACE needs a table with a primary key: {}",
        table.name
    );
    insert_with_conflict(table, schema, profile, OnConflict::Replace)
}

/// Generate an upsert that updates every non-key column of a table with a
/// primary key and at least one other column.
pub fn upsert_for_table(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
) -> BoxedStrategy<InsertStatement> {
    let (key, set): (Vec<_>, Vec<_>) = table.columns.iter().partition(|c| c.primary_key);
    assert!(
        !key.is_empty() && !set.is_empty(),
        "an upsert needs a primary key and another column: {}",
        table.name
    );
    let on_conflict = OnConflict::Update {
        key: key.iter().map(|c| c.name.clone()).collect(),
        set: set.iter().map(|c| c.name.clone()).collect(),
    };
    insert_with_conflict(table, schema, profile, on_conflict)
}

fn insert_with_conflict(
    table: &TableRef,
    schema: &Schema,
    profile: &StatementProfile,
    on_conflict: OnConflict,
) -> BoxedStrategy<InsertStatement> {
    let table_name = table_name_spelling(table, &profile.generation.table_spelling);
    let columns = table.columns.clone();
    let is_strict = table.strict;
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
        .with_profile(expr_profile)
        .with_values(profile.generation.value.clone());

    let profile_clone = profile.clone();
    let value_strategies: Vec<BoxedStrategy<Expression>> = columns
        .iter()
        .map(|c| {
            if is_strict {
                // For STRICT tables, use only literal values. expression_for_type
                // targets a type via SQL affinity rules, but STRICT tables enforce
                // runtime type checking that rejects values whose storage class doesn't
                // match (e.g., `1 + 0.5` yields REAL for an INTEGER column, or
                // `CAST('abc' AS INTEGER)` yields 0). Literal values guarantee the
                // storage class matches the column type.
                crate::value::value_for_type(&c.data_type, c.nullable, &profile_clone)
                    .prop_map(Expression::Value)
                    .boxed()
            } else {
                crate::expression::expression_for_type(Some(&c.data_type), &ctx)
            }
        })
        .collect();

    (value_strategies.into_iter().collect::<Vec<_>>(), table_name)
        .prop_map(move |(values, table)| InsertStatement {
            table,
            columns: col_names.clone(),
            values,
            on_conflict: on_conflict.clone(),
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
            on_conflict: OnConflict::Abort,
        };

        let sql = stmt.to_string();
        assert_eq!(sql, "INSERT INTO users (id, name) VALUES (1, 'Alice')");
    }

    #[test]
    fn insert_or_replace_and_upsert_display() {
        let stmt = |on_conflict| InsertStatement {
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "team".to_string()],
            values: vec![
                Expression::Value(SqlValue::Integer(1)),
                Expression::Value(SqlValue::Text("Alice".to_string())),
                Expression::Value(SqlValue::Null),
            ],
            on_conflict,
        };
        assert_eq!(
            stmt(OnConflict::Replace).to_string(),
            "INSERT OR REPLACE INTO users (id, name, team) VALUES (1, 'Alice', NULL)"
        );
        assert_eq!(
            stmt(OnConflict::Update {
                key: vec!["id".to_string()],
                set: vec!["name".to_string(), "team".to_string()],
            })
            .to_string(),
            "INSERT INTO users (id, name, team) VALUES (1, 'Alice', NULL) \
             ON CONFLICT(id) DO UPDATE SET name = excluded.name, team = excluded.team"
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
            on_conflict: OnConflict::Abort,
        };

        let sql = stmt.to_string();
        assert_eq!(
            sql,
            "INSERT INTO users (id, name) VALUES (1, UPPER('alice'))"
        );
    }

    #[test]
    fn narrow_values_reach_the_expressions_of_a_non_strict_table() {
        let table: TableRef = Table::new("t", vec![ColumnDef::new("name", DataType::Text)]).into();
        let mut profile = StatementProfile::default();
        profile.generation.value = profile.generation.value.narrow();
        let strategy = insert_for_table(&table, &Schema::default(), &profile);
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let with_narrow_text = (0..200)
            .filter(|_| {
                let sql = strategy
                    .new_tree(&mut runner)
                    .unwrap()
                    .current()
                    .to_string();
                sql.split('\'').skip(1).step_by(2).any(|text| {
                    (1..=2).contains(&text.len()) && text.chars().all(|c| ('a'..='c').contains(&c))
                })
            })
            .count();
        assert!(with_narrow_text > 40, "{with_narrow_text} of 200");
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

        #[test]
        fn generated_upsert_updates_every_non_key_column(
            stmt in {
                let table = Table::new(
                    "test",
                    vec![
                        ColumnDef::new("id", DataType::Integer).primary_key(),
                        ColumnDef::new("name", DataType::Text),
                        ColumnDef::new("qty", DataType::Integer),
                    ],
                );
                let table_ref: TableRef = table.into();
                upsert_for_table(&table_ref, &Schema::default(), &StatementProfile::default())
            }
        ) {
            proptest::prop_assert!(stmt.to_string().ends_with(
                " ON CONFLICT(id) DO UPDATE SET name = excluded.name, qty = excluded.qty"
            ));
        }
    }
}
