//! Schema types for defining database structure.

use std::collections::HashSet;
use std::fmt;

/// SQL data types supported by the generator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub enum DataType {
    Integer,
    Real,
    Text,
    Blob,
    Null,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Real => write!(f, "REAL"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Blob => write!(f, "BLOB"),
            DataType::Null => write!(f, "NULL"),
        }
    }
}

/// A column definition for table schemas.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<String>,
}

impl ColumnDef {
    /// Create a new column definition with default settings (nullable, not primary key).
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            primary_key: false,
            unique: false,
            default: None,
        }
    }

    /// Mark the column as NOT NULL.
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Mark the column as PRIMARY KEY (implies NOT NULL).
    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self
    }

    /// Mark the column as UNIQUE.
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Set a default value for the column.
    pub fn default_value(mut self, value: impl Into<String>) -> Self {
        self.default = Some(value.into());
        self
    }
}

impl fmt::Display for ColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;

        if self.primary_key {
            write!(f, " PRIMARY KEY")?;
        }

        if !self.nullable && !self.primary_key {
            write!(f, " NOT NULL")?;
        }

        if self.unique && !self.primary_key {
            write!(f, " UNIQUE")?;
        }

        if let Some(default) = &self.default {
            write!(f, " DEFAULT {default}")?;
        }

        Ok(())
    }
}

/// A table schema definition.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Table {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

impl Table {
    pub fn new(name: impl Into<String>, columns: Vec<ColumnDef>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }

    /// Returns columns that can be used in WHERE clauses (non-blob types).
    pub fn filterable_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns
            .iter()
            .filter(|c| c.data_type != DataType::Blob)
    }

    /// Returns columns with a specific data type.
    pub fn columns_of_type(&self, data_type: DataType) -> impl Iterator<Item = &ColumnDef> {
        self.columns
            .iter()
            .filter(move |c| c.data_type == data_type)
    }
}

/// An index definition in a schema.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Index {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

impl Index {
    pub fn new(
        name: impl Into<String>,
        table_name: impl Into<String>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            name: name.into(),
            table_name: table_name.into(),
            columns,
            unique: false,
        }
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

/// A trigger definition in a schema.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Trigger {
    pub name: String,
    pub table_name: String,
}

impl Trigger {
    pub fn new(name: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table_name: table_name.into(),
        }
    }
}

/// Builder for constructing a schema.
#[derive(Debug, Default)]
pub struct SchemaBuilder {
    tables: Vec<Table>,
    indexes: Vec<Index>,
    triggers: Vec<Trigger>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn table(mut self, table: Table) -> Self {
        self.tables.push(table);
        self
    }

    pub fn index(mut self, index: Index) -> Self {
        self.indexes.push(index);
        self
    }

    pub fn trigger(mut self, trigger: Trigger) -> Self {
        self.triggers.push(trigger);
        self
    }

    pub fn build(self) -> Schema {
        Schema {
            tables: self.tables,
            indexes: self.indexes,
            triggers: self.triggers,
        }
    }
}

/// A schema containing tables, indexes, and triggers.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Schema {
    pub tables: Vec<Table>,
    pub indexes: Vec<Index>,
    pub triggers: Vec<Trigger>,
}

impl Schema {
    /// Returns all table names in the schema.
    pub fn table_names(&self) -> HashSet<String> {
        self.tables.iter().map(|t| t.name.clone()).collect()
    }

    /// Returns all index names in the schema.
    pub fn index_names(&self) -> HashSet<String> {
        self.indexes.iter().map(|i| i.name.clone()).collect()
    }

    /// Returns all trigger names in the schema.
    pub fn trigger_names(&self) -> HashSet<String> {
        self.triggers.iter().map(|t| t.name.clone()).collect()
    }

    /// Returns a table by name.
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// Returns indexes for a specific table.
    pub fn indexes_for_table(&self, table_name: &str) -> Vec<&Index> {
        self.indexes
            .iter()
            .filter(|i| i.table_name == table_name)
            .collect()
    }

    /// Returns true if the schema has at least one table.
    pub fn has_tables(&self) -> bool {
        !self.tables.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_def_builder() {
        let col = ColumnDef::new("id", DataType::Integer)
            .primary_key()
            .not_null();

        assert_eq!(col.name, "id");
        assert_eq!(col.data_type, DataType::Integer);
        assert!(col.primary_key);
        assert!(!col.nullable);
    }

    #[test]
    fn test_table_filterable_columns() {
        let table = Table::new(
            "test",
            vec![
                ColumnDef::new("id", DataType::Integer),
                ColumnDef::new("data", DataType::Blob),
                ColumnDef::new("name", DataType::Text),
            ],
        );

        let filterable: Vec<_> = table.filterable_columns().collect();
        assert_eq!(filterable.len(), 2);
        assert!(filterable.iter().all(|c| c.data_type != DataType::Blob));
    }

    #[test]
    fn test_schema_builder() {
        let schema = SchemaBuilder::new()
            .table(Table::new(
                "users",
                vec![ColumnDef::new("id", DataType::Integer)],
            ))
            .table(Table::new(
                "posts",
                vec![ColumnDef::new("id", DataType::Integer)],
            ))
            .index(Index::new("idx_users_id", "users", vec!["id".to_string()]))
            .build();

        assert_eq!(schema.tables.len(), 2);
        assert_eq!(schema.indexes.len(), 1);
        assert!(schema.table_names().contains("users"));
        assert!(schema.index_names().contains("idx_users_id"));
    }
}
