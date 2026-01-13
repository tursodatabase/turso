//! View statements.
//!
//! Includes CREATE VIEW and DROP VIEW.

use std::collections::HashSet;
use std::fmt;

use proptest::prelude::*;

use crate::create_table::identifier_excluding;
use crate::schema::Schema;

/// CREATE VIEW statement.
#[derive(Debug, Clone)]
pub struct CreateViewStatement {
    /// Whether to use IF NOT EXISTS clause.
    pub if_not_exists: bool,
    /// View name.
    pub view_name: String,
    /// Column names (optional).
    pub columns: Vec<String>,
    /// SELECT statement as string.
    pub select_sql: String,
}

impl fmt::Display for CreateViewStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE VIEW")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " \"{}\"", self.view_name)?;
        if !self.columns.is_empty() {
            let cols: Vec<String> = self.columns.iter().map(|c| format!("\"{c}\"")).collect();
            write!(f, " ({})", cols.join(", "))?;
        }
        write!(f, " AS {}", self.select_sql)
    }
}

/// DROP VIEW statement.
#[derive(Debug, Clone)]
pub struct DropViewStatement {
    /// Whether to use IF EXISTS clause.
    pub if_exists: bool,
    /// View name.
    pub view_name: String,
}

impl fmt::Display for DropViewStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP VIEW")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " \"{}\"", self.view_name)
    }
}

/// Generate a CREATE VIEW statement for a schema.
///
/// Creates simple views that select from existing tables.
pub fn create_view(schema: &Schema) -> BoxedStrategy<CreateViewStatement> {
    assert!(
        !schema.tables.is_empty(),
        "Schema must have at least one table to create a view"
    );

    let tables = schema.tables.clone();
    let existing_names: HashSet<String> = schema
        .table_names()
        .into_iter()
        .chain(schema.view_names())
        .collect();

    (
        any::<bool>(),
        identifier_excluding(existing_names),
        proptest::sample::select((*tables).clone()),
    )
        .prop_map(|(if_not_exists, view_name, table)| {
            // Generate a simple SELECT * FROM table
            let select_sql = format!("SELECT * FROM \"{}\"", table.name);
            CreateViewStatement {
                if_not_exists,
                view_name,
                columns: vec![],
                select_sql,
            }
        })
        .boxed()
}

/// Generate a DROP VIEW statement for a schema.
pub fn drop_view_for_schema(schema: &Schema) -> BoxedStrategy<DropViewStatement> {
    if schema.views.is_empty() {
        // No views to drop - generate with random name and IF EXISTS
        crate::create_table::identifier()
            .prop_map(|view_name| DropViewStatement {
                if_exists: true, // Always use IF EXISTS when no views exist
                view_name,
            })
            .boxed()
    } else {
        let view_names: Vec<String> = schema.views.iter().map(|v| v.name.clone()).collect();
        (any::<bool>(), proptest::sample::select(view_names))
            .prop_map(|(if_exists, view_name)| DropViewStatement {
                if_exists,
                view_name,
            })
            .boxed()
    }
}

/// Generate a DROP VIEW statement for a specific view name.
pub fn drop_view(view_name: String) -> impl Strategy<Value = DropViewStatement> {
    any::<bool>().prop_map(move |if_exists| DropViewStatement {
        if_exists,
        view_name: view_name.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, DataType, SchemaBuilder, Table};

    fn test_schema() -> Schema {
        SchemaBuilder::new()
            .add_table(Table::new(
                "users",
                vec![
                    Column::new("id", DataType::Integer).primary_key(),
                    Column::new("name", DataType::Text),
                ],
            ))
            .build()
    }

    proptest! {
        #[test]
        fn create_view_generates_valid_sql(stmt in create_view(&test_schema())) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("CREATE VIEW"));
            prop_assert!(sql.contains(" AS SELECT"));
        }

        #[test]
        fn drop_view_generates_valid_sql(stmt in drop_view("my_view".to_string())) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("DROP VIEW"));
            prop_assert!(sql.contains("\"my_view\""));
        }
    }
}
