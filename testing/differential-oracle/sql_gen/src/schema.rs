//! Schema types for defining database structure.

use crate::ast::{self, AlterTableAction, CreateIndexKind, Stmt};
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
    IntegerArray,
    RealArray,
    TextArray,
}

impl DataType {
    /// Returns true if this is an array type.
    pub fn is_array(&self) -> bool {
        matches!(
            self,
            DataType::IntegerArray | DataType::RealArray | DataType::TextArray
        )
    }

    /// Returns the element type for array types, or None for scalars.
    pub fn array_element_type(&self) -> Option<DataType> {
        match self {
            DataType::IntegerArray => Some(DataType::Integer),
            DataType::RealArray => Some(DataType::Real),
            DataType::TextArray => Some(DataType::Text),
            _ => None,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Real => write!(f, "REAL"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Blob => write!(f, "BLOB"),
            DataType::Null => write!(f, "NULL"),
            DataType::IntegerArray => write!(f, "INTEGER[]"),
            DataType::RealArray => write!(f, "REAL[]"),
            DataType::TextArray => write!(f, "TEXT[]"),
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
    /// The database this table belongs to (e.g. "temp" or "aux").
    /// `None` means the main database.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub database: Option<String>,
    pub strict: bool,
}

impl Table {
    pub fn new(name: impl Into<String>, columns: Vec<ColumnDef>) -> Self {
        Self {
            name: name.into(),
            columns,
            database: None,
            strict: false,
        }
    }

    pub fn new_strict(name: impl Into<String>, columns: Vec<ColumnDef>) -> Self {
        Self {
            name: name.into(),
            columns,
            database: None,
            strict: true,
        }
    }

    /// Returns the qualified table name (e.g. "aux.t1" or just "t1").
    pub fn qualified_name(&self) -> String {
        match &self.database {
            Some(db) => format!("{}.{}", db, self.name),
            None => self.name.clone(),
        }
    }

    /// Returns the unqualified table name.
    pub fn unqualified_name(&self) -> &str {
        &self.name
    }

    /// Set the database this table belongs to.
    pub fn in_database(mut self, db: impl Into<String>) -> Self {
        self.database = Some(db.into());
        self
    }

    /// Returns columns that can be used in WHERE clauses (non-blob types).
    pub fn filterable_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns
            .iter()
            .filter(|c| c.data_type != DataType::Blob && !c.data_type.is_array())
    }

    /// Returns only array-typed columns.
    pub fn array_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns.iter().filter(|c| c.data_type.is_array())
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
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Option::is_none"))]
    pub database: Option<String>,
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
            database: None,
        }
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn in_database(mut self, db: impl Into<String>) -> Self {
        self.database = Some(db.into());
        self
    }

    pub fn qualified_name(&self) -> String {
        match &self.database {
            Some(db) => format!("{}.{}", db, self.name),
            None => self.name.clone(),
        }
    }
}

/// Split `schema.name` into `(schema, name)`, leaving unqualified names in
/// the main schema.
pub fn split_qualified_name(name: &str) -> (Option<&str>, &str) {
    if let Some((database, name)) = name.split_once('.') {
        (Some(database), name)
    } else {
        (None, name)
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
    attached_databases: Vec<String>,
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

    pub fn database(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        if !self.attached_databases.contains(&name) {
            self.attached_databases.push(name);
        }
        self
    }

    pub fn build(self) -> Schema {
        Schema {
            tables: self.tables,
            indexes: self.indexes,
            triggers: self.triggers,
            attached_databases: self.attached_databases,
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
    /// Names of attached databases (e.g. ["aux"]).
    /// Empty means only the main database is available.
    #[cfg_attr(feature = "serde", serde(skip_serializing_if = "Vec::is_empty"))]
    pub attached_databases: Vec<String>,
}

impl Schema {
    /// Returns all table names in the schema.
    pub fn table_names(&self) -> HashSet<String> {
        self.tables.iter().map(|t| t.name.clone()).collect()
    }

    /// Returns all table names in a specific database scope.
    pub fn table_names_in_database(&self, database: Option<&str>) -> HashSet<String> {
        self.tables
            .iter()
            .filter(|t| t.database.as_deref() == database)
            .map(|t| t.name.clone())
            .collect()
    }

    /// Returns all index names in the schema.
    pub fn index_names(&self) -> HashSet<String> {
        self.indexes.iter().map(|i| i.name.clone()).collect()
    }

    /// Returns all index names in a specific database scope.
    pub fn index_names_in_database(&self, database: Option<&str>) -> HashSet<String> {
        self.indexes
            .iter()
            .filter(|i| i.database.as_deref() == database)
            .map(|i| i.name.clone())
            .collect()
    }

    /// Returns all trigger names in the schema.
    pub fn trigger_names(&self) -> HashSet<String> {
        self.triggers.iter().map(|t| t.name.clone()).collect()
    }

    /// Returns a table by name.
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// Returns a table by a possibly schema-qualified name.
    pub fn get_table_by_qualified_name(&self, name: &str) -> Option<&Table> {
        let (database, table_name) = split_qualified_name(name);
        self.table_in_database(table_name, database)
    }

    /// Returns indexes for a specific table.
    pub fn indexes_for_table(&self, table_name: &str) -> Vec<&Index> {
        self.indexes
            .iter()
            .filter(|i| i.table_name == table_name)
            .collect()
    }

    /// Returns indexes for a specific table in a specific database.
    pub fn indexes_for_table_in_database(
        &self,
        table_name: &str,
        database: Option<&str>,
    ) -> Vec<&Index> {
        self.indexes
            .iter()
            .filter(|i| i.table_name == table_name && i.database.as_deref() == database)
            .collect()
    }

    /// Returns true if the schema has at least one table.
    pub fn has_tables(&self) -> bool {
        !self.tables.is_empty()
    }

    /// Apply a statement that completed successfully to the schema model.
    ///
    /// Non-schema statements are intentionally no-ops. Schema statements that
    /// are represented in the AST must be modeled here; if a new schema-changing
    /// statement becomes executable, leaving it unmodeled would let generator
    /// state drift from the database.
    pub fn apply_successful_statement(&mut self, stmt: &Stmt) {
        match stmt {
            Stmt::CreateTable(create_table) => self.apply_create_table(create_table),
            Stmt::DropTable(drop_table) => self.apply_drop_table(drop_table),
            Stmt::AlterTable(alter_table) => self.apply_alter_table(alter_table),
            Stmt::CreateIndex(create_index) => self.apply_create_index(create_index),
            Stmt::DropIndex(drop_index) => self.apply_drop_index(drop_index),
            Stmt::CreateTrigger(create_trigger) => self.apply_create_trigger(create_trigger),
            Stmt::DropTrigger(drop_trigger) => self.apply_drop_trigger(drop_trigger),
            Stmt::Select(_)
            | Stmt::Insert(_)
            | Stmt::Update(_)
            | Stmt::Delete(_)
            | Stmt::PragmaForeignKeyList(_)
            | Stmt::Begin
            | Stmt::Commit
            | Stmt::Rollback
            | Stmt::Vacuum
            | Stmt::Reindex
            | Stmt::Analyze => {}
            Stmt::CreateView | Stmt::DropView | Stmt::Savepoint(_) | Stmt::Release(_) => {
                panic!("successful schema statement is not modeled: {stmt:?}")
            }
        }
    }

    fn apply_create_table(&mut self, create_table: &ast::CreateTableStmt) {
        let (database_from_name, table_name) = split_qualified_name(&create_table.table);
        let database = if create_table.temporary.is_some() {
            assert!(
                database_from_name.is_none() || database_from_name == Some("temp"),
                "successful CREATE TEMP TABLE used non-temp schema qualifier"
            );
            Some(String::from("temp"))
        } else {
            database_from_name.map(str::to_string)
        };

        if self
            .tables
            .iter()
            .any(|table| table.name == table_name && table.database == database)
        {
            assert!(
                create_table.if_not_exists,
                "successful CREATE TABLE duplicated an existing table without IF NOT EXISTS"
            );
            return;
        }

        let mut table = Table::new(
            table_name,
            create_table
                .columns
                .iter()
                .map(column_from_create)
                .collect(),
        );
        table.database = database;
        table.strict = create_table.strict;
        self.tables.push(table);
    }

    fn apply_drop_table(&mut self, drop_table: &ast::DropTableStmt) {
        let (database, table_name) = split_qualified_name(&drop_table.table);
        let Some(table_idx) = self.table_index_in_database(table_name, database) else {
            assert!(
                drop_table.if_exists,
                "successful DROP TABLE referenced an untracked table"
            );
            return;
        };

        let table = self.tables.remove(table_idx);
        self.indexes
            .retain(|index| !index_targets_table(index, &table.name, table.database.as_deref()));
        self.triggers
            .retain(|trigger| trigger.table_name != table.name);
    }

    fn apply_create_index(&mut self, create_index: &ast::CreateIndexStmt) {
        let (index_database_from_name, index_name) = split_qualified_name(&create_index.name);

        if self.indexes.iter().any(|index| {
            index.name == index_name && index.database.as_deref() == index_database_from_name
        }) {
            assert!(
                create_index.if_not_exists,
                "successful CREATE INDEX duplicated an existing index without IF NOT EXISTS"
            );
            return;
        }

        let (table_database_from_name, table_name) = split_qualified_name(&create_index.table);
        let table_database = table_database_from_name.or(index_database_from_name);
        let Some(table) = self.table_in_database(table_name, table_database) else {
            panic!("successful CREATE INDEX referenced an untracked table");
        };
        assert!(
            index_database_from_name.is_none()
                || index_database_from_name == table.database.as_deref(),
            "successful CREATE INDEX used a different schema than its table"
        );

        let mut index = Index::new(index_name, table.name.clone(), create_index.columns.clone());
        if let Some(database) = index_database_from_name.or(table.database.as_deref()) {
            index = index.in_database(database);
        }
        match &create_index.kind {
            CreateIndexKind::BTree { unique } => {
                if *unique {
                    index = index.unique();
                }
            }
        }
        self.indexes.push(index);
    }

    fn apply_drop_index(&mut self, drop_index: &ast::DropIndexStmt) {
        let (database, index_name) = split_qualified_name(&drop_index.name);
        let Some(index_idx) = self
            .indexes
            .iter()
            .position(|index| index.name == index_name && index.database.as_deref() == database)
        else {
            assert!(
                drop_index.if_exists,
                "successful DROP INDEX referenced an untracked index"
            );
            return;
        };
        self.indexes.remove(index_idx);
    }

    fn apply_alter_table(&mut self, alter_table: &ast::AlterTableStmt) {
        let (database, table_name) = split_qualified_name(&alter_table.table);
        let Some(table_idx) = self.table_index_in_database(table_name, database) else {
            panic!("successful ALTER TABLE referenced an untracked table");
        };

        match &alter_table.action {
            AlterTableAction::RenameTo(new_name) => {
                let table_database = self.tables[table_idx].database.clone();
                let old_name = self.tables[table_idx].name.clone();
                let (new_database, new_table_name) = split_qualified_name(new_name);
                assert!(
                    new_database.is_none() || new_database == table_database.as_deref(),
                    "successful ALTER TABLE RENAME crossed schema boundaries"
                );

                for index in self.indexes.iter_mut().filter(|index| {
                    index_targets_table(index, &old_name, table_database.as_deref())
                }) {
                    index.table_name = new_table_name.to_string();
                }
                self.tables[table_idx].name = new_table_name.to_string();
            }
            AlterTableAction::AddColumn(column) => {
                let table = &mut self.tables[table_idx];
                assert!(
                    !table
                        .columns
                        .iter()
                        .any(|existing| existing.name == column.name),
                    "successful ALTER TABLE ADD COLUMN duplicated an existing column"
                );
                table.columns.push(column_from_create(column));
            }
            AlterTableAction::DropColumn(column_name) => {
                let table = &mut self.tables[table_idx];
                let original_len = table.columns.len();
                table.columns.retain(|column| column.name != *column_name);
                assert!(
                    table.columns.len() < original_len,
                    "successful ALTER TABLE DROP COLUMN referenced an untracked column"
                );

                for index in self
                    .indexes
                    .iter_mut()
                    .filter(|index| index_targets_table(index, table_name, database))
                {
                    index.columns.retain(|column| column != column_name);
                }
            }
            AlterTableAction::RenameColumn { old_name, new_name } => {
                let table = &mut self.tables[table_idx];
                let Some(column) = table
                    .columns
                    .iter_mut()
                    .find(|column| column.name == *old_name)
                else {
                    panic!("successful ALTER TABLE RENAME COLUMN referenced an untracked column");
                };
                column.name.clone_from(new_name);

                for index in self
                    .indexes
                    .iter_mut()
                    .filter(|index| index_targets_table(index, table_name, database))
                {
                    for column in &mut index.columns {
                        if column == old_name {
                            column.clone_from(new_name);
                        }
                    }
                }
            }
        }
    }

    fn apply_create_trigger(&mut self, create_trigger: &ast::CreateTriggerStmt) {
        let (_, trigger_name) = split_qualified_name(&create_trigger.name);
        if self
            .triggers
            .iter()
            .any(|trigger| trigger.name == trigger_name)
        {
            assert!(
                create_trigger.if_not_exists,
                "successful CREATE TRIGGER duplicated an existing trigger without IF NOT EXISTS"
            );
            return;
        }
        self.triggers
            .push(Trigger::new(trigger_name, create_trigger.table.clone()));
    }

    fn apply_drop_trigger(&mut self, drop_trigger: &ast::DropTriggerStmt) {
        let (_, trigger_name) = split_qualified_name(&drop_trigger.name);
        let Some(trigger_idx) = self
            .triggers
            .iter()
            .position(|trigger| trigger.name == trigger_name)
        else {
            assert!(
                drop_trigger.if_exists,
                "successful DROP TRIGGER referenced an untracked trigger"
            );
            return;
        };
        self.triggers.remove(trigger_idx);
    }

    fn table_in_database(&self, table_name: &str, database: Option<&str>) -> Option<&Table> {
        self.tables
            .iter()
            .find(|table| table.name == table_name && table.database.as_deref() == database)
    }

    fn table_index_in_database(&self, table_name: &str, database: Option<&str>) -> Option<usize> {
        self.tables
            .iter()
            .position(|table| table.name == table_name && table.database.as_deref() == database)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SchemaTransactionState {
    begin_snapshot: Option<Schema>,
}

impl SchemaTransactionState {
    pub fn in_transaction(&self) -> bool {
        self.begin_snapshot.is_some()
    }

    pub fn can_run_external_snapshot_oracles(&self) -> bool {
        self.begin_snapshot.is_none()
    }

    pub fn apply_successful_statement(&mut self, schema: &mut Schema, stmt: &Stmt) {
        match stmt {
            Stmt::Begin => {
                assert!(
                    self.begin_snapshot.is_none(),
                    "successful BEGIN entered an already tracked transaction"
                );
                self.begin_snapshot = Some(schema.clone());
            }
            Stmt::Commit => {
                assert!(
                    self.begin_snapshot.is_some(),
                    "successful COMMIT left no tracked transaction to close"
                );
                self.begin_snapshot = None;
            }
            Stmt::Rollback => {
                let Some(snapshot) = self.begin_snapshot.take() else {
                    panic!("successful ROLLBACK left no tracked transaction to restore");
                };
                *schema = snapshot;
            }
            Stmt::Savepoint(_) | Stmt::Release(_) => {
                panic!("successful savepoint statement is not modeled: {stmt:?}")
            }
            _ => schema.apply_successful_statement(stmt),
        }
    }
}

fn column_from_create(column: &ast::ColumnDefStmt) -> ColumnDef {
    let mut col = ColumnDef::new(column.name.clone(), column.data_type);
    if column.primary_key {
        col = col.primary_key();
    } else if column.not_null {
        col = col.not_null();
    }
    if column.unique {
        col = col.unique();
    }
    col
}

fn index_targets_table(index: &Index, table_name: &str, database: Option<&str>) -> bool {
    let (index_table_database, index_table_name) = split_qualified_name(&index.table_name);
    index_table_name == table_name && index_table_database.or(index.database.as_deref()) == database
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{
        AlterTableAction, AlterTableStmt, ColumnDefStmt, CreateIndexKind, CreateIndexStmt,
        CreateTableStmt, CreateTriggerStmt, DropTableStmt, Stmt, TriggerEvent, TriggerTiming,
    };

    fn create_table_stmt(table: &str) -> Stmt {
        Stmt::CreateTable(CreateTableStmt {
            table: table.to_string(),
            columns: vec![
                ColumnDefStmt {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    primary_key: true,
                    not_null: true,
                    unique: false,
                    default: None,
                    check: None,
                },
                ColumnDefStmt {
                    name: "body".to_string(),
                    data_type: DataType::Text,
                    primary_key: false,
                    not_null: false,
                    unique: false,
                    default: None,
                    check: None,
                },
            ],
            if_not_exists: false,
            strict: false,
            temporary: None,
        })
    }

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

    #[test]
    fn test_scoped_names_keep_main_and_temp_separate() {
        let schema = SchemaBuilder::new()
            .database("temp")
            .table(Table::new(
                "shadowed",
                vec![ColumnDef::new("id", DataType::Integer)],
            ))
            .table(
                Table::new("shadowed", vec![ColumnDef::new("id", DataType::Integer)])
                    .in_database("temp"),
            )
            .index(Index::new(
                "shadowed_idx",
                "shadowed",
                vec!["id".to_string()],
            ))
            .index(
                Index::new("shadowed_idx", "shadowed", vec!["id".to_string()]).in_database("temp"),
            )
            .build();

        assert_eq!(
            schema.table_names_in_database(None),
            HashSet::from([String::from("shadowed")])
        );
        assert_eq!(
            schema.table_names_in_database(Some("temp")),
            HashSet::from([String::from("shadowed")])
        );
        assert_eq!(
            schema.index_names_in_database(None),
            HashSet::from([String::from("shadowed_idx")])
        );
        assert_eq!(
            schema.index_names_in_database(Some("temp")),
            HashSet::from([String::from("shadowed_idx")])
        );
    }

    #[test]
    fn successful_schema_statements_update_tables_and_indexes() {
        let mut schema = Schema::default();
        schema.apply_successful_statement(&create_table_stmt("docs"));
        schema.apply_successful_statement(&Stmt::CreateIndex(CreateIndexStmt {
            name: "idx_docs_body".to_string(),
            table: "docs".to_string(),
            columns: vec!["body".to_string()],
            kind: CreateIndexKind::BTree { unique: false },
            if_not_exists: false,
        }));

        schema.apply_successful_statement(&Stmt::AlterTable(AlterTableStmt {
            table: "docs".to_string(),
            action: AlterTableAction::RenameColumn {
                old_name: "body".to_string(),
                new_name: "title".to_string(),
            },
        }));
        assert_eq!(schema.indexes[0].columns, vec!["title"]);

        schema.apply_successful_statement(&Stmt::AlterTable(AlterTableStmt {
            table: "docs".to_string(),
            action: AlterTableAction::RenameTo("articles".to_string()),
        }));

        assert!(schema.get_table_by_qualified_name("articles").is_some());
        assert_eq!(schema.indexes[0].table_name, "articles");
    }

    #[test]
    fn schema_transaction_rollback_restores_schema_snapshot() {
        let mut schema = Schema::default();
        let mut tx_state = SchemaTransactionState::default();
        schema.apply_successful_statement(&create_table_stmt("docs"));

        tx_state.apply_successful_statement(&mut schema, &Stmt::Begin);
        tx_state.apply_successful_statement(&mut schema, &create_table_stmt("scratch"));
        assert!(schema.get_table_by_qualified_name("scratch").is_some());

        tx_state.apply_successful_statement(&mut schema, &Stmt::Rollback);

        assert!(!tx_state.in_transaction());
        assert!(schema.get_table_by_qualified_name("docs").is_some());
        assert!(schema.get_table_by_qualified_name("scratch").is_none());
    }

    #[test]
    fn drop_table_removes_dependent_indexes_and_triggers() {
        let mut schema = Schema::default();
        schema.apply_successful_statement(&create_table_stmt("docs"));
        schema.apply_successful_statement(&Stmt::CreateIndex(CreateIndexStmt {
            name: "idx_docs_body".to_string(),
            table: "docs".to_string(),
            columns: vec!["body".to_string()],
            kind: CreateIndexKind::BTree { unique: false },
            if_not_exists: false,
        }));
        schema.apply_successful_statement(&Stmt::CreateTrigger(CreateTriggerStmt {
            name: "trg_docs".to_string(),
            table: "docs".to_string(),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            for_each_row: true,
            when_clause: None,
            body: Vec::new(),
            if_not_exists: false,
            temporary: false,
        }));

        schema.apply_successful_statement(&Stmt::DropTable(DropTableStmt {
            table: "docs".to_string(),
            if_exists: false,
        }));

        assert!(schema.tables.is_empty());
        assert!(schema.indexes.is_empty());
        assert!(schema.triggers.is_empty());
    }
}
