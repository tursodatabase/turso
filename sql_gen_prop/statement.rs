//! Union type for all SQL statements and schema-level generation strategies.

use proptest::prelude::*;
use std::fmt;
use std::rc::Rc;

use crate::alter_table::{AlterTableStatement, alter_table_for_schema};
use crate::create_index::{CreateIndexStatement, create_index, create_index_for_table};
use crate::create_table::{CreateTableStatement, create_table};
use crate::create_trigger::{CreateTriggerStatement, create_trigger_for_schema};
use crate::delete::{DeleteStatement, delete_for_table};
use crate::drop_index::DropIndexStatement;
use crate::drop_table::{DropTableStatement, drop_table_for_schema, drop_table_for_table};
use crate::drop_trigger::{DropTriggerStatement, drop_trigger_for_schema};
use crate::generator::SqlGeneratorKind;
use crate::insert::{InsertStatement, insert_for_table};
use crate::profile::StatementProfile;
use crate::schema::{Schema, TableRef};
use crate::select::{SelectStatement, select_for_table};
use crate::transaction::{
    BeginStatement, CommitStatement, ReleaseStatement, RollbackStatement, SavepointStatement,
    begin, commit, rollback,
};
use crate::update::{UpdateStatement, update_for_table};
use crate::utility::{
    AnalyzeStatement, ReindexStatement, VacuumStatement, analyze_for_schema, reindex_for_schema,
    vacuum,
};
use crate::view::{CreateViewStatement, DropViewStatement, create_view, drop_view_for_schema};

/// Context needed for statement generation.
#[derive(Debug, Clone)]
pub struct StatementContext<'a> {
    /// The schema to generate statements for.
    pub schema: &'a Schema,
    /// Optional profile for controlling generation weights.
    pub profile: Option<&'a StatementProfile>,
}

/// Union of all supported SQL statements.
#[derive(Debug, Clone, strum::EnumDiscriminants)]
#[strum_discriminants(name(StatementKind), vis(pub))]
#[strum_discriminants(derive(strum::EnumIter))]
pub enum SqlStatement {
    // DML
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),

    // DDL - Tables
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    AlterTable(AlterTableStatement),

    // DDL - Indexes
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),

    // DDL - Views
    CreateView(CreateViewStatement),
    DropView(DropViewStatement),

    // DDL - Triggers
    CreateTrigger(CreateTriggerStatement),
    DropTrigger(DropTriggerStatement),

    // Transaction control
    Begin(BeginStatement),
    Commit(CommitStatement),
    Rollback(RollbackStatement),
    Savepoint(SavepointStatement),
    Release(ReleaseStatement),

    // Utility
    Vacuum(VacuumStatement),
    Analyze(AnalyzeStatement),
    Reindex(ReindexStatement),
}

impl fmt::Display for SqlStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlStatement::Select(s) => write!(f, "{s}"),
            SqlStatement::Insert(s) => write!(f, "{s}"),
            SqlStatement::Update(s) => write!(f, "{s}"),
            SqlStatement::Delete(s) => write!(f, "{s}"),
            SqlStatement::CreateTable(s) => write!(f, "{s}"),
            SqlStatement::DropTable(s) => write!(f, "{s}"),
            SqlStatement::AlterTable(s) => write!(f, "{s}"),
            SqlStatement::CreateIndex(s) => write!(f, "{s}"),
            SqlStatement::DropIndex(s) => write!(f, "{s}"),
            SqlStatement::CreateView(s) => write!(f, "{s}"),
            SqlStatement::DropView(s) => write!(f, "{s}"),
            SqlStatement::CreateTrigger(s) => write!(f, "{s}"),
            SqlStatement::DropTrigger(s) => write!(f, "{s}"),
            SqlStatement::Begin(s) => write!(f, "{s}"),
            SqlStatement::Commit(s) => write!(f, "{s}"),
            SqlStatement::Rollback(s) => write!(f, "{s}"),
            SqlStatement::Savepoint(s) => write!(f, "{s}"),
            SqlStatement::Release(s) => write!(f, "{s}"),
            SqlStatement::Vacuum(s) => write!(f, "{s}"),
            SqlStatement::Analyze(s) => write!(f, "{s}"),
            SqlStatement::Reindex(s) => write!(f, "{s}"),
        }
    }
}

impl SqlStatement {
    /// Returns true if this is a SELECT statement with LIMIT but no ORDER BY.
    ///
    /// Such queries may return different rows between database implementations
    /// since the order is undefined.
    pub fn has_unordered_limit(&self) -> bool {
        match self {
            SqlStatement::Select(s) => s.has_unordered_limit(),
            _ => false,
        }
    }
}

impl StatementKind {
    /// Returns true if this statement kind is DDL (modifies schema).
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            StatementKind::CreateTable
                | StatementKind::DropTable
                | StatementKind::AlterTable
                | StatementKind::CreateIndex
                | StatementKind::DropIndex
                | StatementKind::CreateView
                | StatementKind::DropView
                | StatementKind::CreateTrigger
                | StatementKind::DropTrigger
        )
    }

    /// Returns true if this statement kind is DML (data manipulation).
    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            StatementKind::Select
                | StatementKind::Insert
                | StatementKind::Update
                | StatementKind::Delete
        )
    }

    /// Returns true if this statement kind is transaction control.
    pub fn is_transaction(&self) -> bool {
        matches!(
            self,
            StatementKind::Begin
                | StatementKind::Commit
                | StatementKind::Rollback
                | StatementKind::Savepoint
                | StatementKind::Release
        )
    }
}

impl SqlGeneratorKind for StatementKind {
    type Context<'a> = Schema;
    type Output = SqlStatement;
    type Profile = StatementProfile;

    /// Returns true if this statement kind can be generated for the given schema.
    fn available(&self, schema: &Self::Context<'_>) -> bool {
        match self {
            // DML requires tables
            StatementKind::Select
            | StatementKind::Insert
            | StatementKind::Update
            | StatementKind::Delete => !schema.tables.is_empty(),

            // DDL - Table operations
            StatementKind::CreateTable => true,
            StatementKind::DropTable | StatementKind::AlterTable => !schema.tables.is_empty(),

            // DDL - Index operations
            StatementKind::CreateIndex => !schema.tables.is_empty(),
            StatementKind::DropIndex => !schema.indexes.is_empty(),

            // DDL - View operations
            StatementKind::CreateView => !schema.tables.is_empty(),
            StatementKind::DropView => true, // Can always generate DROP VIEW IF EXISTS

            // DDL - Trigger operations
            StatementKind::CreateTrigger => !schema.tables.is_empty(),
            StatementKind::DropTrigger => true, // Can always generate DROP TRIGGER IF EXISTS

            // Transaction control - always available
            StatementKind::Begin
            | StatementKind::Commit
            | StatementKind::Rollback
            | StatementKind::Savepoint
            | StatementKind::Release => true,

            // Utility - always available
            StatementKind::Vacuum | StatementKind::Analyze | StatementKind::Reindex => true,
        }
    }

    fn supported(&self) -> bool {
        match self {
            // DML requires tables
            StatementKind::Select
            | StatementKind::Insert
            | StatementKind::Update
            | StatementKind::Delete => true,

            // DDL - Table operations
            StatementKind::CreateTable => true,
            StatementKind::DropTable | StatementKind::AlterTable => true,

            // DDL - Index operations
            StatementKind::CreateIndex => true,
            StatementKind::DropIndex => true,

            // DDL - View operations
            StatementKind::CreateView => false,
            StatementKind::DropView => false,

            // DDL - Trigger operations
            StatementKind::CreateTrigger => false,
            StatementKind::DropTrigger => false,

            // Transaction control
            StatementKind::Begin
            | StatementKind::Commit
            | StatementKind::Rollback
            | StatementKind::Savepoint
            | StatementKind::Release => false,

            // Utility - always available
            StatementKind::Vacuum | StatementKind::Analyze | StatementKind::Reindex => false,
        }
    }

    /// Builds a strategy for generating this statement kind.
    ///
    /// Caller must ensure `available(schema)` returns true before calling this.
    /// The optional `profile` is used to pass sub-profiles (e.g., `alter_table.extra`)
    /// to statement generators that support fine-grained control.
    fn strategy<'a>(
        &self,
        schema: &Self::Context<'a>,
        profile: Option<&Self::Profile>,
    ) -> BoxedStrategy<Self::Output> {
        let tables = schema.tables.clone();
        match self {
            // DML - all use expression generation
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

            // DDL - Tables
            StatementKind::CreateTable => create_table(schema)
                .prop_map(SqlStatement::CreateTable)
                .boxed(),
            StatementKind::DropTable => drop_table_for_schema(schema)
                .prop_map(SqlStatement::DropTable)
                .boxed(),
            StatementKind::AlterTable => {
                let op_weights = profile.and_then(|p| p.alter_table.extra.as_ref());
                alter_table_for_schema(schema, op_weights)
                    .prop_map(SqlStatement::AlterTable)
                    .boxed()
            }

            // DDL - Indexes
            StatementKind::CreateIndex => create_index(schema)
                .prop_map(SqlStatement::CreateIndex)
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

            // DDL - Views
            StatementKind::CreateView => create_view(schema)
                .prop_map(SqlStatement::CreateView)
                .boxed(),
            StatementKind::DropView => drop_view_for_schema(schema)
                .prop_map(SqlStatement::DropView)
                .boxed(),

            // DDL - Triggers
            StatementKind::CreateTrigger => {
                let op_weights = profile.and_then(|p| p.create_trigger.extra.as_ref());
                create_trigger_for_schema(schema, op_weights)
                    .prop_map(SqlStatement::CreateTrigger)
                    .boxed()
            }
            StatementKind::DropTrigger => drop_trigger_for_schema(schema)
                .prop_map(SqlStatement::DropTrigger)
                .boxed(),

            // Transaction control
            StatementKind::Begin => begin().prop_map(SqlStatement::Begin).boxed(),
            StatementKind::Commit => commit().prop_map(SqlStatement::Commit).boxed(),
            StatementKind::Rollback => rollback().prop_map(SqlStatement::Rollback).boxed(),
            StatementKind::Savepoint => crate::transaction::savepoint()
                .prop_map(SqlStatement::Savepoint)
                .boxed(),
            StatementKind::Release => {
                // Generate a release with a random savepoint name
                crate::create_table::identifier()
                    .prop_map(|name| SqlStatement::Release(ReleaseStatement { name }))
                    .boxed()
            }

            // Utility
            StatementKind::Vacuum => vacuum().prop_map(SqlStatement::Vacuum).boxed(),
            StatementKind::Analyze => analyze_for_schema(schema)
                .prop_map(SqlStatement::Analyze)
                .boxed(),
            StatementKind::Reindex => reindex_for_schema(schema)
                .prop_map(SqlStatement::Reindex)
                .boxed(),
        }
    }
}

/// Helper to create a table-based DML strategy.
fn table_dml<F>(tables: Rc<Vec<TableRef>>, f: F) -> BoxedStrategy<SqlStatement>
where
    F: Fn(&TableRef) -> BoxedStrategy<SqlStatement> + 'static,
{
    proptest::sample::select((*tables).clone())
        .prop_flat_map(move |t| f(&t))
        .boxed()
}

/// Generate a DML (Data Manipulation Language) statement for a table.
/// Includes SELECT, INSERT, UPDATE, DELETE with expression support.
pub fn dml_for_table(table: &TableRef) -> BoxedStrategy<SqlStatement> {
    prop_oneof![
        select_for_table(table).prop_map(SqlStatement::Select),
        insert_for_table(table).prop_map(SqlStatement::Insert),
        update_for_table(table).prop_map(SqlStatement::Update),
        delete_for_table(table).prop_map(SqlStatement::Delete),
    ]
    .boxed()
}

/// Generate any SQL statement for a table, using schema context for safe DDL generation.
pub fn statement_for_table(table: &TableRef, schema: &Schema) -> BoxedStrategy<SqlStatement> {
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
        .filter(|(kind, _)| kind.supported() && kind.available(schema))
        .map(|(kind, weight)| (weight, kind.strategy(schema, Some(&p))))
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
