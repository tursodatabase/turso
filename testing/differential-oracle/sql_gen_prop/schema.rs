//! Schema types for defining database structure.

use std::collections::HashSet;
use std::fmt;
use std::rc::Rc;

use serde::Serialize;

pub type TableRef = Rc<Table>;

// =============================================================================
// GENERATED COLUMN TYPES
// =============================================================================

/// Storage mode for generated columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum GeneratedStorage {
    /// Computed on read, not stored on disk.
    Virtual,
    /// Computed on write and persisted to disk.
    Stored,
}

impl fmt::Display for GeneratedStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedStorage::Virtual => write!(f, "VIRTUAL"),
            GeneratedStorage::Stored => write!(f, "STORED"),
        }
    }
}

/// Collation for text columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Collation {
    /// Binary comparison (default).
    Binary,
    /// Case-insensitive comparison.
    NoCase,
    /// Trailing spaces are ignored.
    RTrim,
}

impl fmt::Display for Collation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Collation::Binary => write!(f, "BINARY"),
            Collation::NoCase => write!(f, "NOCASE"),
            Collation::RTrim => write!(f, "RTRIM"),
        }
    }
}

/// A generated (computed) column definition.
#[derive(Debug, Clone, Serialize)]
pub struct GeneratedColumn {
    /// The SQL expression that computes this column's value.
    pub expr: String,
    /// Whether the column is VIRTUAL (computed on read) or STORED (persisted).
    pub storage: GeneratedStorage,
    /// Optional collation for text columns.
    pub collation: Option<Collation>,
}
pub type IndexRef = Rc<Index>;
pub type ViewRef = Rc<View>;
pub type TriggerRef = Rc<Trigger>;

/// SQL data types supported by the generator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
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

/// A column definition for table schemas and CREATE TABLE statements.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<String>,
    /// If set, this is a generated (computed) column.
    pub generated: Option<GeneratedColumn>,
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
            generated: None,
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

    /// Make this a generated (computed) column.
    pub fn generated(mut self, expr: impl Into<String>, storage: GeneratedStorage) -> Self {
        self.generated = Some(GeneratedColumn {
            expr: expr.into(),
            storage,
            collation: None,
        });
        // Generated columns cannot have DEFAULT
        self.default = None;
        self
    }

    /// Make this a generated column with collation.
    pub fn generated_with_collation(
        mut self,
        expr: impl Into<String>,
        storage: GeneratedStorage,
        collation: Collation,
    ) -> Self {
        self.generated = Some(GeneratedColumn {
            expr: expr.into(),
            storage,
            collation: Some(collation),
        });
        // Generated columns cannot have DEFAULT
        self.default = None;
        self
    }

    /// Returns true if this is a generated column.
    pub fn is_generated(&self) -> bool {
        self.generated.is_some()
    }

    /// Returns true if this is a STORED generated column.
    pub fn is_stored_generated(&self) -> bool {
        matches!(
            &self.generated,
            Some(GeneratedColumn {
                storage: GeneratedStorage::Stored,
                ..
            })
        )
    }

    /// Returns true if this is a VIRTUAL generated column.
    pub fn is_virtual_generated(&self) -> bool {
        matches!(
            &self.generated,
            Some(GeneratedColumn {
                storage: GeneratedStorage::Virtual,
                ..
            })
        )
    }
}

impl fmt::Display for ColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;

        // Generated column syntax: AS (expr) STORED|VIRTUAL
        if let Some(generated) = &self.generated {
            write!(f, " AS ({})", generated.expr)?;
            // Note: VIRTUAL is the default in SQLite, but we always write it explicitly
            // for clarity and to ensure both DBs interpret it the same way
            write!(f, " {}", generated.storage)?;

            if let Some(collation) = &generated.collation {
                write!(f, " COLLATE {collation}")?;
            }
        }

        if self.primary_key {
            write!(f, " PRIMARY KEY")?;
        }

        if !self.nullable && !self.primary_key {
            write!(f, " NOT NULL")?;
        }

        if self.unique && !self.primary_key {
            write!(f, " UNIQUE")?;
        }

        // DEFAULT is mutually exclusive with GENERATED, so only show if not generated
        if let Some(default) = &self.default {
            if self.generated.is_none() {
                write!(f, " DEFAULT {default}")?;
            }
        }

        Ok(())
    }
}

/// A table schema definition.
#[derive(Debug, Clone, Serialize)]
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

    /// Returns columns that can be updated (non-primary key, non-generated).
    pub fn updatable_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns
            .iter()
            .filter(|c| !c.primary_key && !c.is_generated())
    }

    /// Returns columns that can be inserted (non-generated).
    /// Generated columns cannot be included in INSERT column lists.
    pub fn insertable_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns.iter().filter(|c| !c.is_generated())
    }

    /// Returns only generated columns.
    pub fn generated_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns.iter().filter(|c| c.is_generated())
    }

    /// Returns non-generated columns.
    pub fn non_generated_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns.iter().filter(|c| !c.is_generated())
    }
}

/// An index definition in a schema.
#[derive(Debug, Clone, Serialize)]
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

/// A view definition in a schema.
#[derive(Debug, Clone, Serialize)]
pub struct View {
    pub name: String,
    /// The SELECT statement that defines the view.
    pub select_sql: String,
}

impl View {
    pub fn new(name: impl Into<String>, select_sql: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            select_sql: select_sql.into(),
        }
    }
}

/// A trigger definition in a schema.
#[derive(Debug, Clone, Serialize)]
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
    tables: Vec<TableRef>,
    indexes: Vec<IndexRef>,
    views: Vec<ViewRef>,
    triggers: Vec<TriggerRef>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(mut self, table: Table) -> Self {
        self.tables.push(Rc::new(table));
        self
    }

    pub fn add_index(mut self, index: Index) -> Self {
        self.indexes.push(Rc::new(index));
        self
    }

    pub fn add_view(mut self, view: View) -> Self {
        self.views.push(Rc::new(view));
        self
    }

    pub fn add_trigger(mut self, trigger: Trigger) -> Self {
        self.triggers.push(Rc::new(trigger));
        self
    }

    pub fn build(self) -> Schema {
        Schema {
            tables: Rc::new(self.tables),
            indexes: Rc::new(self.indexes),
            views: Rc::new(self.views),
            triggers: Rc::new(self.triggers),
        }
    }
}

/// A schema containing tables, indexes, views, and triggers.
///
/// Uses `Rc` internally to allow cheap cloning for strategy composition.
/// Use `SchemaBuilder` to construct a schema.
#[derive(Debug, Clone, Serialize)]
pub struct Schema {
    pub tables: Rc<Vec<TableRef>>,
    pub indexes: Rc<Vec<IndexRef>>,
    pub views: Rc<Vec<ViewRef>>,
    pub triggers: Rc<Vec<TriggerRef>>,
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

    /// Returns all view names in the schema.
    pub fn view_names(&self) -> HashSet<String> {
        self.views.iter().map(|v| v.name.clone()).collect()
    }

    /// Returns a table by name.
    pub fn get_table(&self, name: &str) -> Option<&TableRef> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// Returns a view by name.
    pub fn get_view(&self, name: &str) -> Option<&ViewRef> {
        self.views.iter().find(|v| v.name == name)
    }

    /// Returns indexes for a specific table.
    pub fn indexes_for_table(&self, table_name: &str) -> Vec<&IndexRef> {
        self.indexes
            .iter()
            .filter(|i| i.table_name == table_name)
            .collect()
    }

    /// Returns all trigger names in the schema.
    pub fn trigger_names(&self) -> HashSet<String> {
        self.triggers.iter().map(|t| t.name.clone()).collect()
    }

    /// Returns triggers for a specific table.
    pub fn triggers_for_table(&self, table_name: &str) -> Vec<&TriggerRef> {
        self.triggers
            .iter()
            .filter(|t| t.table_name == table_name)
            .collect()
    }
}

// GENERATION
