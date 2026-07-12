//! Built-in SQLite-style catalog tables.
//!
//! `pragma_*` table-valued functions, `json_each`/`json_tree`,
//! `sqlite_dbpage`, `btree_dump`, and `sqlite_turso_types`. Registered into
//! every fresh schema by [`crate::schema::Schema::with_options`] through
//! [`register_builtin_catalog`].

use crate::pragma::PragmaVirtualTable;
use crate::schema::{Schema, Table};
use crate::sync::Arc;
use crate::vtab::{VirtualTable, VirtualTableType};
use turso_ext::VTabKind;

/// Insert the standard SQLite-style catalog tables into `schema`.
///
/// `pragma_*` virtual tables use the dedicated [`VirtualTableType::Pragma`]
/// variant (they aren't `InternalVirtualTable`), so they are inserted
/// directly. The rest go through [`Schema::register_internal_vtab`] — the
/// same path external callers use via [`crate::Database::register_internal_vtab`].
pub fn register_builtin_catalog(
    schema: &mut Schema,
    enable_custom_types: bool,
) -> crate::Result<()> {
    for vtab in pragma_vtabs() {
        schema.tables.insert(
            vtab.name.to_owned(),
            Arc::new(Table::Virtual(Arc::new((*vtab).clone()))),
        );
    }

    #[cfg(feature = "json")]
    {
        schema.register_internal_vtab(crate::json::vtab::JsonVirtualTable::json_each())?;
        schema.register_internal_vtab(crate::json::vtab::JsonVirtualTable::json_tree())?;
    }
    #[cfg(feature = "cli_only")]
    {
        schema.register_internal_vtab(crate::dbpage::DbPageTable::new())?;
        schema.register_internal_vtab(crate::btree_dump::BtreeDumpTable::new())?;
    }
    if enable_custom_types {
        schema.register_internal_vtab(crate::turso_types_vtab::TursoTypesTable::new())?;
    }
    Ok(())
}

/// Build a `VirtualTable` for each PRAGMA table-valued function.
fn pragma_vtabs() -> Vec<Arc<VirtualTable>> {
    PragmaVirtualTable::functions()
        .into_iter()
        .map(|(tab, schema_sql)| {
            Arc::new(VirtualTable {
                name: format!("pragma_{}", tab.pragma_name),
                columns: VirtualTable::resolve_columns(schema_sql)
                    .expect("pragma table-valued function schema resolution should not fail"),
                kind: VTabKind::TableValuedFunction,
                vtab_type: VirtualTableType::Pragma(tab),
                vtab_id: 0,
                innocuous: true,
            })
        })
        .collect()
}
