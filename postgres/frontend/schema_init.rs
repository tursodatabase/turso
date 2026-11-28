//! The tables that the frontend creates in a database when it opens it.
//!
//! Each table's module lists the statements that create its own table.
//! [`crate::open`] runs them in one transaction.

use turso_core::namespace::NamespaceId;
use turso_core::schema::Schema;
use turso_core::Result;

use crate::{auth_members, catalog_table, classes, roles};

/// The statements that create `table`. They run when the database has no
/// such table.
pub(crate) struct TableInit {
    pub(crate) table: &'static str,
    pub(crate) statements: &'static [&'static str],
}

/// Every table of the frontend, in the order in which they are created.
/// The stored tables of the catalog tables come after them.
static TABLE_INITS: [TableInit; 3] = [
    classes::TABLE_INIT,
    roles::TABLE_INIT,
    auth_members::TABLE_INIT,
];

/// The statements that create the frontend's tables that `schema` lacks.
/// A read-only database gets none.
pub(crate) fn initialize_statements(
    schema: &Schema,
    read_only: bool,
) -> Result<std::vec::IntoIter<String>> {
    classes::refuse_relations_without_class_table(schema)?;
    let is_missing = |table| schema.get_btree_table(NamespaceId::ROOT, table).is_none();
    if read_only {
        return Ok(Vec::new().into_iter());
    }
    let statements: Vec<String> = TABLE_INITS
        .iter()
        .filter(|init| is_missing(init.table))
        .inspect(|init| {
            assert!(
                !catalog_table::is_catalog_table(init.table),
                "{} is a catalog table, which create_missing_catalog_tables creates",
                init.table
            )
        })
        .flat_map(|init| init.statements.iter().copied())
        .chain(catalog_table::create_missing_catalog_tables(schema))
        .map(str::to_owned)
        .collect();
    Ok(statements.into_iter())
}

/// Whether `name` in the root namespace is the stored table of a catalog
/// table of [`catalog_table`]. Such a table has no class row: every role
/// reads it, through its overlay, and no user statement writes it.
pub(crate) fn is_stored_catalog_table(namespace: NamespaceId, name: &str) -> bool {
    namespace.is_root() && catalog_table::is_catalog_table(name)
}
