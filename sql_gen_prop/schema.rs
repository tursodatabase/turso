//! Schema types for defining database structure.

use std::collections::HashSet;
use std::fmt;
use std::rc::Rc;

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

/// An index definition in a schema.
#[derive(Debug, Clone)]
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

/// Builder for constructing a schema.
#[derive(Debug, Default)]
pub struct SchemaBuilder {
    tables: Vec<Table>,
    indexes: Vec<Index>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(mut self, table: Table) -> Self {
        self.tables.push(table);
        self
    }

    pub fn add_index(mut self, index: Index) -> Self {
        self.indexes.push(index);
        self
    }

    pub fn build(self) -> Schema {
        Schema {
            tables: Rc::new(self.tables),
            indexes: Rc::new(self.indexes),
        }
    }
}

/// A schema containing tables and indexes.
///
/// Uses `Rc` internally to allow cheap cloning for strategy composition.
/// Use `SchemaBuilder` to construct a schema.
#[derive(Debug, Clone)]
pub struct Schema {
    pub tables: Rc<Vec<Table>>,
    pub indexes: Rc<Vec<Index>>,
}

impl Default for Schema {
    fn default() -> Self {
        SchemaBuilder::new().build()
    }
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
}
