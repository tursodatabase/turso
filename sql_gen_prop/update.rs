//! UPDATE statement type and generation strategy.

use proptest::prelude::*;
use std::fmt;

use crate::condition::{Condition, optional_where_clause};
use crate::schema::{Column, Table};
use crate::value::{SqlValue, value_for_type};

/// An UPDATE statement.
#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<(String, SqlValue)>,
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

/// Generate an UPDATE statement for a table.
pub fn update_for_table(table: &Table) -> BoxedStrategy<UpdateStatement> {
    let table_name = table.name.clone();
    let updatable: Vec<Column> = table.updatable_columns().into_iter().cloned().collect();

    if updatable.is_empty() {
        // No updatable columns: update nothing (unusual but valid)
        let table_for_where = table.clone();
        return optional_where_clause(&table_for_where)
            .prop_map(move |where_clause| UpdateStatement {
                table: table_name.clone(),
                assignments: vec![],
                where_clause,
            })
            .boxed();
    }

    let col_indices: Vec<usize> = (0..updatable.len()).collect();
    let updatable_clone = updatable.clone();

    let table_for_where = table.clone();

    (
        proptest::sample::subsequence(col_indices, 1..=updatable.len()),
        optional_where_clause(&table_for_where),
    )
        .prop_flat_map(move |(indices, where_clause)| {
            let selected_cols: Vec<&Column> =
                indices.iter().map(|&i| &updatable_clone[i]).collect();

            let assignment_strategies: Vec<BoxedStrategy<(String, SqlValue)>> = selected_cols
                .iter()
                .map(|c| {
                    let name = c.name.clone();
                    value_for_type(&c.data_type, c.nullable)
                        .prop_map(move |v| (name.clone(), v))
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

    #[test]
    fn test_update_display() {
        let stmt = UpdateStatement {
            table: "users".to_string(),
            assignments: vec![
                ("name".to_string(), SqlValue::Text("Bob".to_string())),
                ("age".to_string(), SqlValue::Integer(30)),
            ],
            where_clause: Some(Condition::Comparison {
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
}
