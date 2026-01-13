//! Union type for all SQL statements and schema-level generation strategies.

use proptest::prelude::*;
use std::fmt;

use crate::create_index::{CreateIndexStatement, create_index, create_index_for_table};
use crate::create_table::{CreateTableStatement, create_table};
use crate::delete::{DeleteStatement, delete_for_table};
use crate::drop_index::DropIndexStatement;
use crate::drop_table::{DropTableStatement, drop_table_for_schema, drop_table_for_table};
use crate::insert::{InsertStatement, insert_for_table};
use crate::profile::StatementProfile;
use crate::schema::{Schema, Table};
use crate::select::{SelectStatement, select_for_table};
use crate::update::{UpdateStatement, update_for_table};

/// Union of all supported SQL statements.
#[derive(Debug, Clone, strum::EnumDiscriminants)]
#[strum_discriminants(name(StatementKind), vis(pub))]
#[strum_discriminants(derive(strum::EnumIter))]
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

impl StatementKind {
    /// Returns true if this statement kind can be generated for the given schema.
    pub fn is_available(&self, schema: &Schema) -> bool {
        match self {
            // DML and most DDL require tables
            StatementKind::Select
            | StatementKind::Insert
            | StatementKind::Update
            | StatementKind::Delete
            | StatementKind::CreateIndex
            | StatementKind::DropTable => !schema.tables.is_empty(),
            // DROP INDEX requires indexes
            StatementKind::DropIndex => !schema.indexes.is_empty(),
            // CREATE TABLE is always available
            StatementKind::CreateTable => true,
        }
    }

    /// Builds a strategy for generating this statement kind.
    ///
    /// Caller must ensure `is_available(schema)` returns true before calling this.
    pub fn strategy(&self, schema: &Schema) -> BoxedStrategy<SqlStatement> {
        let tables = schema.tables.clone();
        match self {
            StatementKind::Select => table_dml(tables, |t| {
                select_for_table(t).prop_map(SqlStatement::Select).boxed()
            }),
            StatementKind::Insert => table_dml(tables, |t| {
                insert_for_table(t).prop_map(SqlStatement::Insert).boxed()
            }),
            StatementKind::Update => table_dml(tables, |t| {
                update_for_table(t).prop_map(SqlStatement::Update).boxed()
            }),
            StatementKind::Delete => table_dml(tables, |t| {
                delete_for_table(t).prop_map(SqlStatement::Delete).boxed()
            }),
            StatementKind::CreateIndex => create_index(schema)
                .prop_map(SqlStatement::CreateIndex)
                .boxed(),
            StatementKind::DropTable => drop_table_for_schema(schema)
                .prop_map(SqlStatement::DropTable)
                .boxed(),
            StatementKind::CreateTable => create_table(schema)
                .prop_map(SqlStatement::CreateTable)
                .boxed(),
            StatementKind::DropIndex => {
                let index_names: Vec<String> =
                    schema.indexes.iter().map(|i| i.name.clone()).collect();
                (proptest::sample::select(index_names), any::<bool>())
                    .prop_map(|(name, if_exists)| {
                        SqlStatement::DropIndex(DropIndexStatement {
                            index_name: name,
                            if_exists,
                        })
                    })
                    .boxed()
            }
        }
    }
}

/// Helper to create a table-based DML strategy.
fn table_dml<F>(tables: Vec<Table>, f: F) -> BoxedStrategy<SqlStatement>
where
    F: Fn(&Table) -> BoxedStrategy<SqlStatement> + 'static,
{
    proptest::sample::select(tables)
        .prop_flat_map(move |t| f(&t))
        .boxed()
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

/// Generate any SQL statement for a schema with an optional profile.
///
/// When `profile` is `None`, uses default weights for all applicable statement types.
/// When `profile` is `Some`, uses the specified weights to control statement distribution.
///
/// Schema constraints are enforced via `StatementKind::is_available`.
pub fn statement_for_schema(
    schema: &Schema,
    profile: Option<&StatementProfile>,
) -> BoxedStrategy<SqlStatement> {
    let p = profile.cloned().unwrap_or_default();

    let strategies: Vec<(u32, BoxedStrategy<SqlStatement>)> = p
        .enabled_statements()
        .filter(|(kind, _)| kind.is_available(schema))
        .map(|(kind, weight)| (weight, kind.strategy(schema)))
        .collect();

    assert!(
        !strategies.is_empty(),
        "No valid statements can be generated for the given schema and profile"
    );

    proptest::strategy::Union::new_weighted(strategies).boxed()
}

/// Generate a sequence of SQL statements for a schema with an optional profile.
pub fn statement_sequence(
    schema: &Schema,
    profile: Option<&StatementProfile>,
    count: impl Into<proptest::collection::SizeRange>,
) -> BoxedStrategy<Vec<SqlStatement>> {
    let profile = profile.cloned().unwrap_or_default();
    let schema = schema.clone();
    proptest::collection::vec(
        proptest::strategy::LazyJust::new(move || statement_for_schema(&schema, Some(&profile)))
            .prop_flat_map(|s| s),
        count,
    )
    .boxed()
}

/// Generate a sequence of DML statements for a schema.
pub fn dml_sequence(
    schema: &Schema,
    count: impl Into<proptest::collection::SizeRange>,
) -> BoxedStrategy<Vec<SqlStatement>> {
    proptest::collection::vec(dml_for_schema(schema), count).boxed()
}
