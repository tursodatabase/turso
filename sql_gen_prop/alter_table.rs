//! ALTER TABLE statement generation.
//!
//! SQLite supports:
//! - ALTER TABLE ... RENAME TO ...
//! - ALTER TABLE ... RENAME COLUMN ... TO ...
//! - ALTER TABLE ... ADD COLUMN ...
//! - ALTER TABLE ... DROP COLUMN ...

use std::collections::HashSet;
use std::fmt;

use proptest::prelude::*;

use crate::create_table::{column_def, identifier_excluding};
use crate::schema::ColumnDef;
use crate::schema::{Schema, Table};

/// Types of ALTER TABLE operations.
#[derive(Debug, Clone)]
pub enum AlterTableOp {
    /// RENAME TO new_name
    RenameTo(String),
    /// RENAME COLUMN old_name TO new_name
    RenameColumn { old_name: String, new_name: String },
    /// ADD COLUMN column_def
    AddColumn(ColumnDef),
    /// DROP COLUMN column_name
    DropColumn(String),
}

impl fmt::Display for AlterTableOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterTableOp::RenameTo(new_name) => write!(f, "RENAME TO \"{new_name}\""),
            AlterTableOp::RenameColumn { old_name, new_name } => {
                write!(f, "RENAME COLUMN \"{old_name}\" TO \"{new_name}\"")
            }
            AlterTableOp::AddColumn(col_def) => write!(f, "ADD COLUMN {col_def}"),
            AlterTableOp::DropColumn(col_name) => write!(f, "DROP COLUMN \"{col_name}\""),
        }
    }
}

/// ALTER TABLE statement.
#[derive(Debug, Clone)]
pub struct AlterTableStatement {
    /// Table name to alter.
    pub table_name: String,
    /// The alteration operation.
    pub operation: AlterTableOp,
}

impl fmt::Display for AlterTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ALTER TABLE \"{}\" {}", self.table_name, self.operation)
    }
}

/// Generate an ALTER TABLE RENAME TO statement.
pub fn alter_table_rename_to(table: &Table, schema: &Schema) -> BoxedStrategy<AlterTableStatement> {
    let table_name = table.name.clone();
    let existing_names: HashSet<String> = schema.table_names();
    identifier_excluding(existing_names)
        .prop_map(move |new_name| AlterTableStatement {
            table_name: table_name.clone(),
            operation: AlterTableOp::RenameTo(new_name),
        })
        .boxed()
}

/// Generate an ALTER TABLE RENAME COLUMN statement.
pub fn alter_table_rename_column(table: &Table) -> BoxedStrategy<AlterTableStatement> {
    if table.columns.is_empty() {
        // No columns to rename - return empty strategy
        proptest::strategy::Just(AlterTableStatement {
            table_name: table.name.clone(),
            operation: AlterTableOp::RenameColumn {
                old_name: String::new(),
                new_name: String::new(),
            },
        })
        .prop_filter("table has no columns", |_| false)
        .boxed()
    } else {
        let table_name = table.name.clone();
        let col_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
        let existing_cols: HashSet<String> = col_names.iter().cloned().collect();
        (
            proptest::sample::select(col_names),
            identifier_excluding(existing_cols),
        )
            .prop_map(move |(old_name, new_name)| AlterTableStatement {
                table_name: table_name.clone(),
                operation: AlterTableOp::RenameColumn { old_name, new_name },
            })
            .boxed()
    }
}

/// Generate an ALTER TABLE ADD COLUMN statement.
pub fn alter_table_add_column(table: &Table) -> BoxedStrategy<AlterTableStatement> {
    let table_name = table.name.clone();
    let existing_cols: HashSet<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    (identifier_excluding(existing_cols), column_def())
        .prop_map(move |(col_name, mut col_def)| {
            col_def.name = col_name;
            // New columns added via ALTER TABLE cannot be PRIMARY KEY
            col_def.primary_key = false;
            AlterTableStatement {
                table_name: table_name.clone(),
                operation: AlterTableOp::AddColumn(col_def),
            }
        })
        .boxed()
}

/// Generate an ALTER TABLE DROP COLUMN statement.
pub fn alter_table_drop_column(table: &Table) -> BoxedStrategy<AlterTableStatement> {
    // Can only drop non-primary-key columns, and table must have more than one column
    let droppable_cols: Vec<String> = table
        .columns
        .iter()
        .filter(|c| !c.primary_key)
        .map(|c| c.name.clone())
        .collect();

    if droppable_cols.is_empty() || table.columns.len() <= 1 {
        // No columns can be dropped - return empty strategy
        proptest::strategy::Just(AlterTableStatement {
            table_name: table.name.clone(),
            operation: AlterTableOp::DropColumn(String::new()),
        })
        .prop_filter("no droppable columns", |_| false)
        .boxed()
    } else {
        let table_name = table.name.clone();
        proptest::sample::select(droppable_cols)
            .prop_map(move |col_name| AlterTableStatement {
                table_name: table_name.clone(),
                operation: AlterTableOp::DropColumn(col_name),
            })
            .boxed()
    }
}

/// Generate any ALTER TABLE statement for a table.
pub fn alter_table_for_table(table: &Table, schema: &Schema) -> BoxedStrategy<AlterTableStatement> {
    let has_droppable_cols =
        table.columns.iter().any(|c| !c.primary_key) && table.columns.len() > 1;
    let has_columns = !table.columns.is_empty();

    let mut strategies: Vec<BoxedStrategy<AlterTableStatement>> = vec![
        alter_table_rename_to(table, schema),
        alter_table_add_column(table),
    ];

    if has_columns {
        strategies.push(alter_table_rename_column(table));
    }

    if has_droppable_cols {
        strategies.push(alter_table_drop_column(table));
    }

    proptest::strategy::Union::new(strategies).boxed()
}

/// Generate any ALTER TABLE statement for a schema.
pub fn alter_table_for_schema(schema: &Schema) -> BoxedStrategy<AlterTableStatement> {
    assert!(
        !schema.tables.is_empty(),
        "Schema must have at least one table"
    );

    let tables = schema.tables.clone();
    let schema_clone = schema.clone();
    proptest::sample::select((*tables).clone())
        .prop_flat_map(move |table| alter_table_for_table(&table, &schema_clone))
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, DataType, SchemaBuilder};

    fn test_table() -> Table {
        Table::new(
            "users",
            vec![
                ColumnDef::new("id", DataType::Integer).primary_key(),
                ColumnDef::new("name", DataType::Text).not_null(),
                ColumnDef::new("email", DataType::Text),
            ],
        )
    }

    fn test_schema() -> Schema {
        SchemaBuilder::new().add_table(test_table()).build()
    }

    proptest! {
        #[test]
        fn alter_table_rename_generates_valid_sql(stmt in alter_table_rename_to(&test_table(), &test_schema())) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("ALTER TABLE \"users\" RENAME TO"));
        }

        #[test]
        fn alter_table_add_column_generates_valid_sql(stmt in alter_table_add_column(&test_table())) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("ALTER TABLE \"users\" ADD COLUMN"));
        }
    }
}
