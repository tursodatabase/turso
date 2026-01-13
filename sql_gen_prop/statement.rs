//! Union type for all SQL statements and schema-level generation strategies.

use proptest::prelude::*;
use std::fmt;

use crate::create_index::{CreateIndexStatement, create_index, create_index_for_table};
use crate::create_table::{CreateTableStatement, create_table};
use crate::delete::{DeleteStatement, delete_for_table};
use crate::drop_index::DropIndexStatement;
use crate::drop_table::{DropTableStatement, drop_table_for_schema, drop_table_for_table};
use crate::insert::{InsertStatement, insert_for_table};
use crate::schema::{Schema, Table};
use crate::select::{SelectStatement, select_for_table};
use crate::update::{UpdateStatement, update_for_table};

/// Union of all supported SQL statements.
#[derive(Debug, Clone)]
pub enum SqlStatement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    DropTable(DropTableStatement),
    DropIndex(DropIndexStatement),
}

impl fmt::Display for SqlStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlStatement::Select(s) => write!(f, "{s}"),
            SqlStatement::Insert(s) => write!(f, "{s}"),
            SqlStatement::Update(s) => write!(f, "{s}"),
            SqlStatement::Delete(s) => write!(f, "{s}"),
            SqlStatement::CreateTable(s) => write!(f, "{s}"),
            SqlStatement::CreateIndex(s) => write!(f, "{s}"),
            SqlStatement::DropTable(s) => write!(f, "{s}"),
            SqlStatement::DropIndex(s) => write!(f, "{s}"),
        }
    }
}

/// Generate a DML (Data Manipulation Language) statement for a table.
/// Includes SELECT, INSERT, UPDATE, DELETE.
pub fn dml_for_table(table: &Table) -> BoxedStrategy<SqlStatement> {
    prop_oneof![
        select_for_table(table).prop_map(SqlStatement::Select),
        insert_for_table(table).prop_map(SqlStatement::Insert),
        update_for_table(table).prop_map(SqlStatement::Update),
        delete_for_table(table).prop_map(SqlStatement::Delete),
    ]
    .boxed()
}

/// Generate any SQL statement for a table, using schema context for safe DDL generation.
pub fn statement_for_table(table: &Table, schema: &Schema) -> BoxedStrategy<SqlStatement> {
    prop_oneof![
        select_for_table(table).prop_map(SqlStatement::Select),
        insert_for_table(table).prop_map(SqlStatement::Insert),
        update_for_table(table).prop_map(SqlStatement::Update),
        delete_for_table(table).prop_map(SqlStatement::Delete),
        create_index_for_table(table, schema).prop_map(SqlStatement::CreateIndex),
        drop_table_for_table(table).prop_map(SqlStatement::DropTable),
    ]
    .boxed()
}

/// Generate a DML statement for any table in a schema.
pub fn dml_for_schema(schema: &Schema) -> BoxedStrategy<SqlStatement> {
    assert!(
        !schema.tables.is_empty(),
        "Schema must have at least one table"
    );

    let table_strategies: Vec<BoxedStrategy<SqlStatement>> =
        schema.tables.iter().map(dml_for_table).collect();

    proptest::strategy::Union::new(table_strategies).boxed()
}

/// Generate any SQL statement for a schema.
/// - CREATE TABLE avoids existing table names
/// - CREATE INDEX avoids existing index names (only if tables exist)
/// - DROP TABLE targets existing tables (only if tables exist)
/// - DROP INDEX targets existing indexes (only if indexes exist)
/// - DML statements only generated if tables exist
pub fn statement_for_schema(schema: &Schema) -> BoxedStrategy<SqlStatement> {
    let mut all_strategies: Vec<BoxedStrategy<SqlStatement>> = Vec::new();

    // DML statements only if there are tables
    if !schema.tables.is_empty() {
        let dml_strategies: Vec<BoxedStrategy<SqlStatement>> =
            schema.tables.iter().map(dml_for_table).collect();
        all_strategies.extend(dml_strategies);

        // CREATE INDEX only if there are tables
        all_strategies.push(
            create_index(schema)
                .prop_map(SqlStatement::CreateIndex)
                .boxed(),
        );

        // DROP TABLE only if there are tables
        all_strategies.push(
            drop_table_for_schema(schema)
                .prop_map(SqlStatement::DropTable)
                .boxed(),
        );
    }

    // CREATE TABLE is always available
    all_strategies.push(
        create_table(schema)
            .prop_map(SqlStatement::CreateTable)
            .boxed(),
    );

    // DROP INDEX only if there are indexes
    if !schema.indexes.is_empty() {
        let index_names: Vec<String> = schema.indexes.iter().map(|i| i.name.clone()).collect();
        all_strategies.push(
            (proptest::sample::select(index_names), any::<bool>())
                .prop_map(|(index_name, if_exists)| {
                    SqlStatement::DropIndex(DropIndexStatement {
                        index_name,
                        if_exists,
                    })
                })
                .boxed(),
        );
    }

    proptest::strategy::Union::new(all_strategies).boxed()
}

/// Generate a sequence of SQL statements for a schema.
pub fn statement_sequence(
    schema: &Schema,
    count: impl Into<proptest::collection::SizeRange>,
) -> BoxedStrategy<Vec<SqlStatement>> {
    proptest::collection::vec(statement_for_schema(schema), count).boxed()
}

/// Generate a sequence of DML statements for a schema.
pub fn dml_sequence(
    schema: &Schema,
    count: impl Into<proptest::collection::SizeRange>,
) -> BoxedStrategy<Vec<SqlStatement>> {
    proptest::collection::vec(dml_for_schema(schema), count).boxed()
}
