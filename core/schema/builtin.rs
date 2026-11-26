//! The catalog tables and types that the schema of every database has,
//! whatever its dialect.

use super::{SchemaBuilder, Table};
use crate::pragma::PragmaVirtualTable;
use crate::sync::Arc;
use crate::vtab::{VirtualTable, VirtualTableType};
use turso_ext::VTabKind;

/// Starts the schema of a database: the built-in catalog tables and, if
/// `enable_custom_types`, the built-in types. A dialect adds its own tables
/// and types to the builder.
pub fn schema_builder(enable_custom_types: bool) -> crate::Result<SchemaBuilder> {
    let mut builder = SchemaBuilder::new()?.custom_types(enable_custom_types);
    if enable_custom_types {
        for ty in crate::types::builtin_types() {
            builder = builder.builtin_type(ty.as_ref())?;
        }
    }
    builtin_catalog(builder, enable_custom_types)
}

/// Adds the standard SQLite-style catalog tables.
///
/// `pragma_*` virtual tables use the dedicated `VirtualTableType::Pragma`
/// variant (they aren't `InternalVirtualTable`), so they are added as
/// tables.
fn builtin_catalog(
    mut builder: SchemaBuilder,
    enable_custom_types: bool,
) -> crate::Result<SchemaBuilder> {
    for vtab in pragma_vtabs()? {
        builder = builder.table(
            vtab.name.to_owned(),
            Arc::new(Table::Virtual(Arc::new((*vtab).clone()))),
        );
    }

    #[cfg(feature = "json")]
    {
        builder = builder
            .internal_vtab(crate::json::vtab::JsonVirtualTable::json_each())?
            .internal_vtab(crate::json::vtab::JsonVirtualTable::json_tree())?;
    }
    #[cfg(feature = "cli_only")]
    {
        builder = builder
            .internal_vtab(crate::dbpage::DbPageTable::new())?
            .internal_vtab(crate::btree_dump::BtreeDumpTable::new())?;
    }
    if enable_custom_types {
        builder = builder.internal_vtab(crate::turso_types_vtab::TursoTypesTable::new())?;
    }
    Ok(builder)
}

/// Build a `VirtualTable` for each PRAGMA table-valued function.
fn pragma_vtabs() -> crate::Result<Vec<Arc<VirtualTable>>> {
    PragmaVirtualTable::functions()
        .into_iter()
        .map(|(tab, columns)| {
            Ok(Arc::new(VirtualTable {
                name: format!("pragma_{}", tab.pragma_name),
                columns: VirtualTable::columns_from_definitions(&columns)?,
                kind: VTabKind::TableValuedFunction,
                vtab_type: VirtualTableType::Pragma(tab),
                vtab_id: 0,
                is_droppable: false,
                innocuous: true,
                columns_with_declared_type: Vec::new(),
            }))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::dialect::cannot_parse::DialectThatCannotParse;
    use crate::security::roles::SessionRole;
    use crate::sync::Arc;
    use crate::{ConnectOpts, Database, DatabaseOpts, MemoryIO, OpenOptions};

    #[test]
    fn opening_a_new_database_parses_no_sql() {
        let db = Database::open_file(
            Arc::new(MemoryIO::new()),
            ":memory:",
            Arc::new(DialectThatCannotParse),
        )
        .unwrap();
        db.connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
    }

    #[test]
    fn opening_a_new_mvcc_database_parses_no_sql() {
        let db = Database::open(
            Arc::new(MemoryIO::new()),
            ":memory:",
            OpenOptions::new(Arc::new(DialectThatCannotParse))
                .db_opts(DatabaseOpts::default().with_mvcc(true)),
        )
        .unwrap();
        assert!(db.get_mv_store().is_some());
        db.connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
    }
}
