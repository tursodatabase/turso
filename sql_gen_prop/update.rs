//! UPDATE statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::condition::{Condition, optional_where_clause};
use crate::expression::{Expression, ExpressionContext};
use crate::function::builtin_functions;
use crate::schema::{ColumnDef, Table};

/// An UPDATE statement.
#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table: String,
    /// Column assignments as (column_name, expression) pairs.
    pub assignments: Vec<(String, Expression)>,
    pub where_clause: Option<Condition>,
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

/// Generate an UPDATE statement for a table with expression support.
///
/// This generates function calls and other expressions in SET values.
pub fn update_for_table(table: &Table) -> BoxedStrategy<UpdateStatement> {
    let table_name = table.name.clone();
    let updatable: Vec<ColumnDef> = table.updatable_columns().cloned().collect();
    let functions = builtin_functions();

    if updatable.is_empty() {
        return optional_where_clause(table)
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
        .with_max_depth(2)
        .with_aggregates(false);

    let col_indices: Vec<usize> = (0..updatable.len()).collect();
    let updatable_clone = updatable.clone();

    (
        proptest::sample::subsequence(col_indices, 1..=updatable.len()),
        optional_where_clause(table),
    )
        .prop_flat_map(move |(indices, where_clause)| {
            let selected_cols: Vec<&ColumnDef> =
                indices.iter().map(|&i| &updatable_clone[i]).collect();

            let assignment_strategies: Vec<BoxedStrategy<(String, Expression)>> = selected_cols
                .iter()
                .map(|c| {
                    let name = c.name.clone();
                    crate::expression::expression_for_type(Some(&c.data_type), &ctx, 2)
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
    use crate::condition::ComparisonOp;
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
            where_clause: Some(Condition::SimpleComparison {
                column: "id".to_string(),
                op: ComparisonOp::Eq,
                value: SqlValue::Integer(1),
            }),
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
                update_for_table(&table)
            }
        ) {
            let sql = stmt.to_string();
            proptest::prop_assert!(sql.starts_with("UPDATE \"test\" SET"));
        }
    }
}
