//! The types and catalog tables that a SQLite database starts with.

use turso_core::schema::{RangeOffsets, SchemaBuilder};
use turso_core::{Dialect, Result, VirtualTable};

/// Starts the schema of a database: the built-in catalog tables and, if
/// `enable_custom_types`, the built-in types. A dialect built on SQLite adds
/// its own tables and types to the builder.
pub fn schema_builder(
    dialect: &(impl Dialect + ?Sized),
    enable_custom_types: bool,
) -> Result<SchemaBuilder> {
    let mut builder = SchemaBuilder::new()?.custom_types(enable_custom_types);
    if enable_custom_types {
        builder = builtin_types(dialect, builder)?;
    }
    builtin_catalog(dialect, builder, enable_custom_types)
}

fn builtin_types(
    dialect: &(impl Dialect + ?Sized),
    mut builder: SchemaBuilder,
) -> Result<SchemaBuilder> {
    // Whether values of the type take a COLLATE. In PostgreSQL only text
    // types do, so of the types below only varchar does.
    const COLLATE: bool = true;
    const NO_COLLATE: bool = false;
    // Whether the stored form sorts in the same order as the values.
    const SORTS: bool = true;
    const NO_SORT: bool = false;
    // Whether core has the functions that the type's SQL calls.
    const ALWAYS: bool = true;
    // Whether the type is available, whether it takes a COLLATE, its RANGE
    // offsets, whether its stored form sorts, its aliases and its SQL.
    type BuiltinType = (
        bool,
        bool,
        RangeOffsets,
        bool,
        &'static [&'static str],
        &'static str,
    );
    let types: &[BuiltinType] = &[
        (turso_core::HAS_UUID, NO_COLLATE, RangeOffsets::None, NO_SORT, &[], "CREATE TYPE uuid(value any) BASE blob ENCODE CASE WHEN value IS NULL THEN NULL WHEN typeof(value) = 'blob' AND length(value) = 16 THEN value ELSE coalesce(uuid_blob(value), RAISE(ABORT, 'invalid UUID value')) END DECODE uuid_str(value) OPERATOR '<'"),
        (ALWAYS, NO_COLLATE, RangeOffsets::None, NO_SORT, &["bool"], "CREATE TYPE boolean(value any) BASE integer ENCODE boolean_to_int(value) DECODE CASE WHEN value THEN 1 ELSE 0 END OPERATOR '<'"),
        (turso_core::HAS_JSON, NO_COLLATE, RangeOffsets::None, NO_SORT, &[], "CREATE TYPE json(value text) BASE text ENCODE json(value) DECODE value"),
        (turso_core::HAS_JSON, NO_COLLATE, RangeOffsets::None, NO_SORT, &[], "CREATE TYPE jsonb(value text) BASE blob ENCODE jsonb(value) DECODE json(value)"),
        (ALWAYS, COLLATE, RangeOffsets::None, NO_SORT, &[], "CREATE TYPE varchar(value text, maxlen integer) BASE text COERCE CASE WHEN length(value) <= maxlen THEN value ELSE RAISE(ABORT, 'value too long for varchar') END DECODE value OPERATOR '<'"),
        // The ENCODEs read PostgreSQL's input forms and store one text form
        // with a 4-digit year, which sorts in time order (see
        // functions/datetime_input.rs).
        (ALWAYS, NO_COLLATE, RangeOffsets::None, SORTS, &[], "CREATE TYPE date(value text) BASE text ENCODE temporal_encode(value, 'date') DECODE value OPERATOR '<'"),
        (ALWAYS, NO_COLLATE, RangeOffsets::None, SORTS, &[], "CREATE TYPE time(value text) BASE text ENCODE temporal_encode(value, 'time') DECODE value OPERATOR '<'"),
        (ALWAYS, NO_COLLATE, RangeOffsets::None, SORTS, &[], "CREATE TYPE timestamp(value text) BASE text ENCODE temporal_encode(value, 'timestamp') DECODE value OPERATOR '<'"),
        (ALWAYS, NO_COLLATE, RangeOffsets::None, SORTS, &[], "CREATE TYPE timestamptz(value text) BASE text ENCODE temporal_encode(value, 'timestamptz') DECODE value OPERATOR '<'"),
        (ALWAYS, NO_COLLATE, RangeOffsets::Integer, NO_SORT, &["int2"], "CREATE TYPE smallint(value integer) BASE integer COERCE CASE WHEN value BETWEEN -32768 AND 32767 THEN value ELSE RAISE(ABORT, 'integer out of range for smallint') END OPERATOR '<'"),
        (ALWAYS, NO_COLLATE, RangeOffsets::Integer, NO_SORT, &["int8"], "CREATE TYPE bigint(value integer) BASE integer"),
        (ALWAYS, NO_COLLATE, RangeOffsets::None, NO_SORT, &[], "CREATE TYPE inet(value text) BASE text ENCODE validate_ipaddr(value) DECODE value"),
        (ALWAYS, NO_COLLATE, RangeOffsets::None, NO_SORT, &[], "CREATE TYPE cidr(value text) BASE text ENCODE value DECODE value"),
        (ALWAYS, NO_COLLATE, RangeOffsets::None, NO_SORT, &[], "CREATE TYPE macaddr(value text) BASE text ENCODE value DECODE value"),
        (ALWAYS, NO_COLLATE, RangeOffsets::None, NO_SORT, &[], "CREATE TYPE macaddr8(value text) BASE text ENCODE value DECODE value"),
        (ALWAYS, NO_COLLATE, RangeOffsets::None, NO_SORT, &[], "CREATE TYPE bytea(value blob) BASE blob OPERATOR '<'"),
        (ALWAYS, NO_COLLATE, RangeOffsets::Numeric, NO_SORT, &[], "CREATE TYPE numeric(value any, precision integer, scale integer) BASE blob ENCODE numeric_encode(value) COERCE numeric_fit(value, precision, scale) DECODE numeric_decode(value)"),
    ];

    for &(
        available,
        accepts_collate,
        range_offsets,
        stored_form_sorts_in_value_order,
        aliases,
        sql,
    ) in types
    {
        if available {
            builder = builder.builtin_type(
                dialect,
                sql,
                aliases,
                accepts_collate,
                range_offsets,
                stored_form_sorts_in_value_order,
            )?;
        }
    }
    Ok(builder)
}

/// Adds the standard SQLite-style catalog tables.
fn builtin_catalog(
    dialect: &(impl Dialect + ?Sized),
    mut builder: SchemaBuilder,
    enable_custom_types: bool,
) -> Result<SchemaBuilder> {
    for table in VirtualTable::pragma_functions(dialect)? {
        builder = builder.virtual_table(table);
    }
    let tables_of_core_features = [
        VirtualTable::json_each(dialect)?,
        VirtualTable::json_tree(dialect)?,
        VirtualTable::sqlite_dbpage(dialect)?,
        VirtualTable::btree_dump(dialect)?,
    ];
    for table in tables_of_core_features.into_iter().flatten() {
        builder = builder.virtual_table(table);
    }
    if enable_custom_types {
        builder = builder.virtual_table(VirtualTable::sqlite_turso_types(dialect)?);
    }
    Ok(builder)
}
