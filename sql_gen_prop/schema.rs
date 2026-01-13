//! Schema types for defining database structure.

use std::fmt;

/// SQL data types supported by the generator.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// A column definition in a table schema.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            primary_key: false,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self
    }
}

/// A table schema definition.
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }

    /// Returns columns that can be used in WHERE clauses (non-blob types).
    pub fn filterable_columns(&self) -> Vec<&Column> {
        self.columns
            .iter()
            .filter(|c| c.data_type != DataType::Blob)
            .collect()
    }

    /// Returns columns that can be updated (non-primary key).
    pub fn updatable_columns(&self) -> Vec<&Column> {
        self.columns.iter().filter(|c| !c.primary_key).collect()
    }
}

/// A schema containing multiple tables.
#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub tables: Vec<Table>,
}

impl Schema {
    pub fn new() -> Self {
        Self { tables: Vec::new() }
    }

    pub fn add_table(mut self, table: Table) -> Self {
        self.tables.push(table);
        self
    }
}
