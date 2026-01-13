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
use strum::IntoEnumIterator;

use crate::create_table::{column_def, identifier_excluding};
use crate::generator::SqlGeneratorKind;
use crate::schema::{ColumnDef, Schema, Table};

/// Context needed for ALTER TABLE operation generation.
#[derive(Debug, Clone)]
pub struct AlterTableContext<'a> {
    /// The table being altered.
    pub table: &'a Table,
    /// The schema containing the table.
    pub schema: &'a Schema,
}

/// Enum representing the kinds of ALTER TABLE operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum AlterTableOpKind {
    /// RENAME TO new_name
    RenameTo,
    /// RENAME COLUMN old_name TO new_name
    RenameColumn,
    /// ADD COLUMN column_def
    AddColumn,
    /// DROP COLUMN column_name
    DropColumn,
}

impl SqlGeneratorKind for AlterTableOpKind {
    type Context<'a> = AlterTableContext<'a>;
    type Output = AlterTableStatement;
    type Profile = (); // Weights are handled externally via AlterTableOpWeights

    /// Returns true if this operation kind can be generated for the given table.
    fn available(&self, ctx: &Self::Context<'_>) -> bool {
        let table = ctx.table;
        match self {
            // RENAME TO is always available
            AlterTableOpKind::RenameTo => true,
            // ADD COLUMN is always available
            AlterTableOpKind::AddColumn => true,
            // RENAME COLUMN requires at least one column
            AlterTableOpKind::RenameColumn => !table.columns.is_empty(),
            // DROP COLUMN requires droppable (non-PK) columns and more than one column
            AlterTableOpKind::DropColumn => {
                table.columns.len() > 1 && table.columns.iter().any(|c| !c.primary_key)
            }
        }
    }

    /// Returns true if this operation kind is currently supported for generation.
    fn supported(&self) -> bool {
        match self {
            AlterTableOpKind::RenameTo => true,
            AlterTableOpKind::RenameColumn => true,
            AlterTableOpKind::AddColumn => false,
            AlterTableOpKind::DropColumn => true,
        }
    }

    /// Builds a strategy for generating this operation kind.
    ///
    /// Caller must ensure `available(table)` returns true before calling this.
    fn strategy<'a>(
        &self,
        ctx: &Self::Context<'_>,
        _profile: Option<&Self::Profile>,
    ) -> BoxedStrategy<Self::Output> {
        match self {
            AlterTableOpKind::RenameTo => alter_table_rename_to(ctx.table, ctx.schema),
            AlterTableOpKind::RenameColumn => alter_table_rename_column(ctx.table),
            AlterTableOpKind::AddColumn => alter_table_add_column(ctx.table),
            AlterTableOpKind::DropColumn => alter_table_drop_column(ctx.table),
        }
    }
}

/// Weights for ALTER TABLE operation types.
///
/// Each weight determines the relative probability of generating that
/// operation type. A weight of 0 disables that operation type entirely.
#[derive(Debug, Clone)]
pub struct AlterTableOpWeights {
    /// Weight for RENAME TO operations.
    pub rename_to: u32,
    /// Weight for RENAME COLUMN operations.
    pub rename_column: u32,
    /// Weight for ADD COLUMN operations.
    pub add_column: u32,
    /// Weight for DROP COLUMN operations.
    pub drop_column: u32,
}

impl Default for AlterTableOpWeights {
    fn default() -> Self {
        Self {
            rename_to: 10,
            rename_column: 20,
            add_column: 40,
            drop_column: 30,
        }
    }
}

impl AlterTableOpWeights {
    /// Create weights with all values set to zero.
    pub fn none() -> Self {
        Self {
            rename_to: 0,
            rename_column: 0,
            add_column: 0,
            drop_column: 0,
        }
    }

    /// Builder method to set RENAME TO weight.
    pub fn with_rename_to(mut self, weight: u32) -> Self {
        self.rename_to = weight;
        self
    }

    /// Builder method to set RENAME COLUMN weight.
    pub fn with_rename_column(mut self, weight: u32) -> Self {
        self.rename_column = weight;
        self
    }

    /// Builder method to set ADD COLUMN weight.
    pub fn with_add_column(mut self, weight: u32) -> Self {
        self.add_column = weight;
        self
    }

    /// Builder method to set DROP COLUMN weight.
    pub fn with_drop_column(mut self, weight: u32) -> Self {
        self.drop_column = weight;
        self
    }

    /// Returns the total weight (sum of all weights).
    pub fn total_weight(&self) -> u32 {
        self.rename_to + self.rename_column + self.add_column + self.drop_column
    }

    /// Returns true if at least one operation type is enabled.
    pub fn has_enabled_operations(&self) -> bool {
        self.total_weight() > 0
    }

    /// Returns the weight for a given operation kind.
    pub fn weight_for(&self, kind: AlterTableOpKind) -> u32 {
        match kind {
            AlterTableOpKind::RenameTo => self.rename_to,
            AlterTableOpKind::RenameColumn => self.rename_column,
            AlterTableOpKind::AddColumn => self.add_column,
            AlterTableOpKind::DropColumn => self.drop_column,
        }
    }

    /// Returns an iterator over all operation kinds with weight > 0.
    pub fn enabled_operations(&self) -> impl Iterator<Item = (AlterTableOpKind, u32)> + '_ {
        AlterTableOpKind::iter()
            .map(|kind| (kind, self.weight_for(kind)))
            .filter(|(_, w)| *w > 0)
    }
}

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
    let existing_names = schema.table_names();
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

/// Generate any ALTER TABLE statement for a schema with optional operation weights.
///
/// When `op_weights` is `None`, uses default weights for all applicable operation types.
/// When `op_weights` is `Some`, uses the specified weights to control operation distribution.
///
/// Operations are filtered based on table state via `AlterTableOpKind::available()`.
pub fn alter_table_for_schema(
    schema: &Schema,
    op_weights: Option<&AlterTableOpWeights>,
) -> BoxedStrategy<AlterTableStatement> {
    assert!(
        !schema.tables.is_empty(),
        "Schema must have at least one table"
    );

    let tables = schema.tables.clone();
    let schema_clone = schema.clone();
    let op_weights_clone = op_weights.cloned();
    proptest::sample::select((*tables).clone())
        .prop_flat_map(move |table| {
            let w = op_weights_clone.clone().unwrap_or_default();
            let ctx = AlterTableContext {
                table: &table,
                schema: &schema_clone,
            };

            let strategies: Vec<(u32, BoxedStrategy<AlterTableStatement>)> = w
                .enabled_operations()
                .filter(|(kind, _)| kind.supported() && kind.available(&ctx))
                .map(|(kind, weight)| (weight, kind.strategy(&ctx, None)))
                .collect();

            assert!(
                !strategies.is_empty(),
                "No valid ALTER TABLE operations can be generated for the given table and profile"
            );

            proptest::strategy::Union::new_weighted(strategies)
        })
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

        #[test]
        fn alter_table_for_schema_with_default_profile(stmt in alter_table_for_schema(&test_schema(), None)) {
            let sql = stmt.to_string();
            prop_assert!(sql.starts_with("ALTER TABLE \"users\""));
        }

        #[test]
        fn alter_table_for_schema_rename_only(stmt in alter_table_for_schema(
            &test_schema(),
            Some(&AlterTableOpWeights::none().with_rename_to(100))
        )) {
            let sql = stmt.to_string();
            prop_assert!(sql.contains("RENAME TO"));
        }
    }

    #[test]
    fn test_alter_table_op_weights_default() {
        let weights = AlterTableOpWeights::default();
        assert_eq!(weights.rename_to, 10);
        assert_eq!(weights.rename_column, 20);
        assert_eq!(weights.add_column, 40);
        assert_eq!(weights.drop_column, 30);
        assert!(weights.has_enabled_operations());
    }

    #[test]
    fn test_alter_table_op_weights_builder() {
        let weights = AlterTableOpWeights::none()
            .with_rename_to(25)
            .with_add_column(75);
        assert_eq!(weights.rename_to, 25);
        assert_eq!(weights.rename_column, 0);
        assert_eq!(weights.add_column, 75);
        assert_eq!(weights.drop_column, 0);
        assert_eq!(weights.total_weight(), 100);
    }
}
