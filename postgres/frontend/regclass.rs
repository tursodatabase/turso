//! The `regclass` and `regnamespace` input functions, which turn the name
//! of a relation or a schema into its oid, as `'pg_class'::regclass` does.
//! Like every oid in tursopg, the result is an integer.

use turso_core::{Connection, LimboError, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_INVALID_NAME, SQLSTATE_INVALID_SCHEMA_NAME, SQLSTATE_UNDEFINED_TABLE,
};

use crate::catalog::{builtin_schema_oid, namespace_oid, stored_relation_oid};
use crate::error::sqlstate_error;

/// The system catalog tables of PostgreSQL 18 and their fixed oids, which
/// clients such as pg_dump look up by name.
const SYSTEM_CATALOG_OIDS: [(&str, i64); 64] = [
    ("pg_aggregate", 2600),
    ("pg_am", 2601),
    ("pg_amop", 2602),
    ("pg_amproc", 2603),
    ("pg_attrdef", 2604),
    ("pg_attribute", 1249),
    ("pg_auth_members", 1261),
    ("pg_authid", 1260),
    ("pg_cast", 2605),
    ("pg_class", 1259),
    ("pg_collation", 3456),
    ("pg_constraint", 2606),
    ("pg_conversion", 2607),
    ("pg_database", 1262),
    ("pg_db_role_setting", 2964),
    ("pg_default_acl", 826),
    ("pg_depend", 2608),
    ("pg_description", 2609),
    ("pg_enum", 3501),
    ("pg_event_trigger", 3466),
    ("pg_extension", 3079),
    ("pg_foreign_data_wrapper", 2328),
    ("pg_foreign_server", 1417),
    ("pg_foreign_table", 3118),
    ("pg_index", 2610),
    ("pg_inherits", 2611),
    ("pg_init_privs", 3394),
    ("pg_language", 2612),
    ("pg_largeobject", 2613),
    ("pg_largeobject_metadata", 2995),
    ("pg_namespace", 2615),
    ("pg_opclass", 2616),
    ("pg_operator", 2617),
    ("pg_opfamily", 2753),
    ("pg_parameter_acl", 6243),
    ("pg_partitioned_table", 3350),
    ("pg_policy", 3256),
    ("pg_proc", 1255),
    ("pg_publication", 6104),
    ("pg_publication_namespace", 6237),
    ("pg_publication_rel", 6106),
    ("pg_range", 3541),
    ("pg_replication_origin", 6000),
    ("pg_rewrite", 2618),
    ("pg_seclabel", 3596),
    ("pg_sequence", 2224),
    ("pg_shdepend", 1214),
    ("pg_shdescription", 2396),
    ("pg_shseclabel", 3592),
    ("pg_statistic", 2619),
    ("pg_statistic_ext", 3381),
    ("pg_statistic_ext_data", 3429),
    ("pg_subscription", 6100),
    ("pg_subscription_rel", 6102),
    ("pg_tablespace", 1213),
    ("pg_transform", 3576),
    ("pg_trigger", 2620),
    ("pg_ts_config", 3602),
    ("pg_ts_config_map", 3603),
    ("pg_ts_dict", 3600),
    ("pg_ts_parser", 3601),
    ("pg_ts_template", 3764),
    ("pg_type", 1247),
    ("pg_user_mapping", 1418),
];

/// The oid of the relation that `value` names, as `value::regclass` gives
/// it: an oid is kept, and a name, optionally qualified with its schema and
/// double-quoted, is looked up in `pg_catalog` and then in the search path.
pub(crate) fn regclass_oid(conn: &Connection, value: &Value) -> Result<Option<i64>> {
    let text = match value {
        Value::Null => return Ok(None),
        Value::Numeric(_) => return Ok(value.as_int()),
        Value::Text(text) => text.as_str(),
        _ => return Err(invalid_name(&value.to_string())),
    };
    if let Ok(oid) = text.parse::<i64>() {
        return Ok(Some(oid));
    }
    let (schema_name, name) =
        turso_core::split_qualified_name(text).ok_or_else(|| invalid_name(text))?;
    let name = name.to_ascii_lowercase();
    let catalog_oid = || {
        SYSTEM_CATALOG_OIDS
            .iter()
            .find(|(catalog, _)| *catalog == name)
            .map(|(_, oid)| *oid)
    };
    let schema = conn.current_schema();
    let oid = match schema_name.map(|schema| schema.to_ascii_lowercase()) {
        Some(schema_name) if schema_name == "pg_catalog" => catalog_oid(),
        Some(schema_name) => conn
            .qualifier(&schema_name)
            .and_then(|qualifier| stored_relation_oid(&schema, qualifier.target.id, &name)),
        // As in PostgreSQL, pg_catalog comes first when the search path does
        // not name it.
        None => catalog_oid().or_else(|| {
            conn.search_order()
                .into_iter()
                .find_map(|qualifier| stored_relation_oid(&schema, qualifier.target.id, &name))
        }),
    };
    oid.map(Some).ok_or_else(|| {
        sqlstate_error(
            SQLSTATE_UNDEFINED_TABLE,
            format!("relation \"{text}\" does not exist"),
        )
    })
}

/// The oid of the schema that `value` names, as `value::regnamespace` gives
/// it.
pub(crate) fn regnamespace_oid(conn: &Connection, value: &Value) -> Result<Option<i64>> {
    let text = match value {
        Value::Null => return Ok(None),
        Value::Numeric(_) => return Ok(value.as_int()),
        Value::Text(text) => text.as_str(),
        _ => return Err(invalid_name(&value.to_string())),
    };
    if let Ok(oid) = text.parse::<i64>() {
        return Ok(Some(oid));
    }
    let (None, name) = turso_core::split_qualified_name(text).ok_or_else(|| invalid_name(text))?
    else {
        return Err(invalid_name(text));
    };
    let name = name.to_ascii_lowercase();
    if let Some(oid) = builtin_schema_oid(&name) {
        return Ok(Some(oid));
    }
    let schema = conn.current_schema();
    conn.qualifier(&name)
        .filter(|qualifier| !qualifier.is_database())
        .map(|qualifier| Some(namespace_oid(&schema, qualifier.target.id)))
        .ok_or_else(|| {
            sqlstate_error(
                SQLSTATE_INVALID_SCHEMA_NAME,
                format!("schema \"{text}\" does not exist"),
            )
        })
}

fn invalid_name(text: &str) -> LimboError {
    sqlstate_error(
        SQLSTATE_INVALID_NAME,
        format!("invalid name syntax: {text}"),
    )
}
