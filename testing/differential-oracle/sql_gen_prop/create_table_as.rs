//! CREATE TABLE AS SELECT statement generation.

use std::collections::HashSet;
use std::fmt;

use proptest::prelude::*;

use crate::create_table::identifier_excluding;
use crate::profile::StatementProfile;
use crate::schema::Schema;
use crate::select::select_for_table;

/// CREATE TABLE AS SELECT statement.
#[derive(Debug, Clone)]
pub struct CreateTableAsStatement {
    /// Whether to use IF NOT EXISTS clause.
    pub if_not_exists: bool,
    /// Table name.
    pub table_name: String,
    /// SELECT statement as string.
    pub select_sql: String,
}

impl fmt::Display for CreateTableAsStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE TABLE")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {} AS {}", self.table_name, self.select_sql)
    }
}

/// Generate a CREATE TABLE AS SELECT statement for a schema.
///
/// Generates varied SELECT statements including column subsets, expressions,
/// WHERE clauses, ORDER BY/LIMIT, and cross-joins that produce duplicate column names.
pub fn create_table_as(schema: &Schema) -> BoxedStrategy<CreateTableAsStatement> {
    assert!(
        !schema.tables.is_empty(),
        "Schema must have at least one table to create a table AS SELECT"
    );

    let tables = schema.tables.clone();
    let existing_names: HashSet<String> = schema
        .table_names()
        .into_iter()
        .chain(schema.view_names())
        .collect();

    let schema_clone = schema.clone();
    let profile = StatementProfile::default();

    (
        any::<bool>(),
        identifier_excluding(existing_names),
        proptest::sample::select((*tables).clone()),
    )
        .prop_flat_map(move |(if_not_exists, table_name, table)| {
            let schema = schema_clone.clone();
            let profile = profile.clone();

            // Use the full select generator for richer coverage
            let rich_select = select_for_table(&table, &schema, &profile)
                .prop_map(|stmt| stmt.to_string())
                .boxed();

            // Join variant: cross-join two tables to exercise duplicate column names
            let join_select = if schema.tables.len() >= 2 {
                let tables_vec: Vec<_> = schema.tables.iter().cloned().collect();
                proptest::sample::subsequence(tables_vec, 2..=2)
                    .prop_map(|pair| {
                        let t1 = &pair[0];
                        let t2 = &pair[1];
                        format!(
                            "SELECT {t1}.*, {t2}.* FROM {t1}, {t2}",
                            t1 = t1.qualified_name(),
                            t2 = t2.qualified_name(),
                        )
                    })
                    .boxed()
            } else {
                // Fallback: self-join with aliases
                let tname = table.qualified_name();
                Just(format!("SELECT a.*, b.* FROM {tname} AS a, {tname} AS b")).boxed()
            };

            // SELECT * (simple, original behavior)
            let star_select = {
                let tname = table.qualified_name();
                Just(format!("SELECT * FROM {tname}")).boxed()
            };

            // Weight: rich selects most common, joins for dup coverage, star for simplicity
            let select_strategy = proptest::strategy::Union::new_weighted(vec![
                (5, rich_select),
                (3, join_select),
                (2, star_select),
            ]);

            select_strategy.prop_map(move |select_sql| CreateTableAsStatement {
                if_not_exists,
                table_name: table_name.clone(),
                select_sql,
            })
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, DataType, SchemaBuilder, Table};

    fn test_schema() -> Schema {
        SchemaBuilder::new()
            .add_table(Table::new(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("name", DataType::Text),
                ],
            ))
            .add_table(Table::new(
                "orders",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("user_id", DataType::Integer),
                    ColumnDef::new("amount", DataType::Real),
                ],
            ))
            .build()
    }

    proptest! {
        #[test]
        fn create_table_as_generates_valid_sql(stmt in create_table_as(&test_schema())) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("CREATE TABLE"));
            // The SELECT part may start with WITH (CTE) or SELECT
            prop_assert!(sql.contains(" AS "));
            prop_assert!(stmt.select_sql.contains("SELECT"));
        }

        #[test]
        fn create_table_as_join_variant_generates_cross_joins(
            stmt in create_table_as(&test_schema())
        ) {
            // Just ensure generation doesn't panic and produces valid SQL
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("CREATE TABLE"));
        }
    }

    #[test]
    fn single_table_schema_still_works() {
        let schema = SchemaBuilder::new()
            .add_table(Table::new(
                "t1",
                vec![
                    ColumnDef::new("a", DataType::Integer),
                    ColumnDef::new("b", DataType::Text),
                ],
            ))
            .build();

        proptest!(|(stmt in create_table_as(&schema))| {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("CREATE TABLE"));
            prop_assert!(stmt.select_sql.contains("SELECT"));
        });
    }
}
