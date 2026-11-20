//! The catalog tables and types that the schema of every database has,
//! whatever its dialect.

use super::{RangeOffsets, SchemaBuilder, Table, TypeDef, TypeOrigin};
use crate::pragma::PragmaVirtualTable;
use crate::sync::Arc;
use crate::vtab::{VirtualTable, VirtualTableType};
use turso_ext::VTabKind;

/// Starts the schema of a database: the built-in catalog tables and, if
/// `enable_custom_types`, the built-in types. A dialect adds its own tables
/// and types to the builder.
pub fn schema_builder(
    dialect: &(impl crate::dialect::Dialect + ?Sized),
    enable_custom_types: bool,
) -> crate::Result<SchemaBuilder> {
    let mut builder = SchemaBuilder::new()?.custom_types(enable_custom_types);
    if enable_custom_types {
        builder = builtin_types(dialect, builder)?;
    }
    builtin_catalog(builder, enable_custom_types)
}

fn builtin_types(
    dialect: &(impl crate::dialect::Dialect + ?Sized),
    mut builder: SchemaBuilder,
) -> crate::Result<SchemaBuilder> {
    use turso_parser::ast::{Cmd, Stmt};

    // Whether values of the type take a COLLATE. In PostgreSQL only text
    // types do, so of the types below only varchar does.
    const COLLATE: bool = true;
    const NO_COLLATE: bool = false;
    // Whether the stored form sorts in the same order as the values.
    const SORTS: bool = true;
    const NO_SORT: bool = false;
    let types: &[(bool, RangeOffsets, bool, &str)] = &[
        #[cfg(feature = "uuid")]
        (NO_COLLATE, RangeOffsets::None, NO_SORT, "CREATE TYPE uuid(value any) BASE blob ENCODE CASE WHEN value IS NULL THEN NULL WHEN typeof(value) = 'blob' AND length(value) = 16 THEN value ELSE coalesce(uuid_blob(value), RAISE(ABORT, 'invalid UUID value')) END DECODE uuid_str(value) OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::None, NO_SORT, "CREATE TYPE boolean(value any) BASE integer ENCODE boolean_to_int(value) DECODE CASE WHEN value THEN 1 ELSE 0 END OPERATOR '<'"),
        #[cfg(feature = "json")]
        (NO_COLLATE, RangeOffsets::None, NO_SORT, "CREATE TYPE json(value text) BASE text ENCODE json(value) DECODE value"),
        #[cfg(feature = "json")]
        (NO_COLLATE, RangeOffsets::None, NO_SORT, "CREATE TYPE jsonb(value text) BASE blob ENCODE jsonb(value) DECODE json(value)"),
        (COLLATE, RangeOffsets::None, NO_SORT, "CREATE TYPE varchar(value text, maxlen integer) BASE text COERCE CASE WHEN length(value) <= maxlen THEN value ELSE RAISE(ABORT, 'value too long for varchar') END DECODE value OPERATOR '<'"),
        // The ENCODEs read PostgreSQL's input forms. A date or time is stored
        // as one text form with a 4-digit year, which sorts in time order; a
        // timestamp or timestamptz as microseconds since 2000-01-01, as
        // PostgreSQL stores it (see functions/datetime_input.rs).
        (NO_COLLATE, RangeOffsets::None, SORTS, "CREATE TYPE date(value text) BASE text ENCODE temporal_encode(value, 'date') DECODE value OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::None, SORTS, "CREATE TYPE time(value text) BASE text ENCODE temporal_encode(value, 'time') DECODE value OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::None, SORTS, "CREATE TYPE timestamp(value any) BASE integer ENCODE timestamp_encode(value) DECODE timestamp_decode(value) OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::None, SORTS, "CREATE TYPE timestamptz(value any) BASE integer ENCODE timestamptz_encode(value) DECODE timestamptz_decode(value) OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::Integer, NO_SORT, "CREATE TYPE smallint(value integer) BASE integer COERCE CASE WHEN value BETWEEN -32768 AND 32767 THEN value ELSE RAISE(ABORT, 'integer out of range for smallint') END OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::Integer, NO_SORT, "CREATE TYPE bigint(value integer) BASE integer"),
        (NO_COLLATE, RangeOffsets::None, NO_SORT, "CREATE TYPE inet(value text) BASE text ENCODE validate_ipaddr(value) DECODE value"),
        (NO_COLLATE, RangeOffsets::None, NO_SORT, "CREATE TYPE cidr(value text) BASE text ENCODE value DECODE value"),
        (NO_COLLATE, RangeOffsets::None, NO_SORT, "CREATE TYPE macaddr(value text) BASE text ENCODE value DECODE value"),
        (NO_COLLATE, RangeOffsets::None, NO_SORT, "CREATE TYPE macaddr8(value text) BASE text ENCODE value DECODE value"),
        (NO_COLLATE, RangeOffsets::None, NO_SORT, "CREATE TYPE bytea(value blob) BASE blob OPERATOR '<'"),
        (NO_COLLATE, RangeOffsets::Numeric, NO_SORT, "CREATE TYPE numeric(value any, precision integer, scale integer) BASE blob ENCODE numeric_encode(value) COERCE numeric_fit(value, precision, scale) DECODE numeric_decode(value)"),
    ];

    let aliases: &[(&str, &str)] = &[
        ("bool", "boolean"),
        ("int2", "smallint"),
        ("int8", "bigint"),
    ];
    for &(accepts_collate, range_offsets, stored_form_sorts_in_value_order, sql) in types {
        let Ok(Some(Cmd::Stmt(Stmt::CreateType {
            type_name, body, ..
        }))) = crate::dialect::parse_statement(dialect, sql)
        else {
            return Err(crate::LimboError::InternalError(format!(
                "failed to parse built-in type SQL: {sql}"
            )));
        };

        let type_def = TypeDef::from_create_type(
            &type_name,
            &body,
            TypeOrigin::Builtin {
                accepts_collate,
                range_offsets,
                stored_form_sorts_in_value_order,
            },
            sql.to_string(),
        )?;
        let type_def = Arc::new(type_def);
        for (alias, target) in aliases {
            if type_name.eq_ignore_ascii_case(target) {
                builder = builder.type_def(alias, Arc::clone(&type_def));
            }
        }
        builder = builder.type_def(&type_name, type_def);
    }
    Ok(builder)
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
