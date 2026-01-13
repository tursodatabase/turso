//! Union type for all SQL statements and schema-level generation strategies.

use proptest::prelude::*;
use std::fmt;

use crate::delete::{DeleteStatement, delete_for_table};
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
}

impl fmt::Display for SqlStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlStatement::Select(s) => write!(f, "{s}"),
            SqlStatement::Insert(s) => write!(f, "{s}"),
            SqlStatement::Update(s) => write!(f, "{s}"),
            SqlStatement::Delete(s) => write!(f, "{s}"),
        }
    }
}

/// Generate any SQL statement for a table.
pub fn statement_for_table(table: &Table) -> BoxedStrategy<SqlStatement> {
    prop_oneof![
        select_for_table(table).prop_map(SqlStatement::Select),
        insert_for_table(table).prop_map(SqlStatement::Insert),
        update_for_table(table).prop_map(SqlStatement::Update),
        delete_for_table(table).prop_map(SqlStatement::Delete),
    ]
    .boxed()
}

/// Generate any SQL statement for any table in a schema.
pub fn statement_for_schema(schema: &Schema) -> BoxedStrategy<SqlStatement> {
    assert!(
        !schema.tables.is_empty(),
        "Schema must have at least one table"
    );

    let table_strategies: Vec<BoxedStrategy<SqlStatement>> =
        schema.tables.iter().map(statement_for_table).collect();

    proptest::strategy::Union::new(table_strategies).boxed()
}

/// Generate a sequence of SQL statements for a schema.
pub fn statement_sequence(
    schema: &Schema,
    count: impl Into<proptest::collection::SizeRange>,
) -> BoxedStrategy<Vec<SqlStatement>> {
    proptest::collection::vec(statement_for_schema(schema), count).boxed()
}
