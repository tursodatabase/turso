use crate::catalog::{db_name_from_path, namespace_oid_by_name, relation_oid_by_name};
use crate::error::{parse_error_to_core, sqlstate_error};
use crate::session::current_schemas;
use turso_core::{Connection, Result};
use turso_pg_parser::coerce::parse_oid;
use turso_pg_parser::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_NAME, SQLSTATE_INVALID_SCHEMA_NAME,
    SQLSTATE_SYNTAX_ERROR, SQLSTATE_UNDEFINED_TABLE,
};

const MAX_NAME_BYTES: usize = 63;

const SYSTEM_CATALOG_OIDS: &[(&str, i64)] = &[
    ("pg_default_acl", 826),
    ("pg_tablespace", 1213),
    ("pg_shdepend", 1214),
    ("pg_type", 1247),
    ("pg_attribute", 1249),
    ("pg_proc", 1255),
    ("pg_class", 1259),
    ("pg_authid", 1260),
    ("pg_auth_members", 1261),
    ("pg_database", 1262),
    ("pg_foreign_server", 1417),
    ("pg_user_mapping", 1418),
    ("pg_sequence", 2224),
    ("pg_foreign_data_wrapper", 2328),
    ("pg_shdescription", 2396),
    ("pg_aggregate", 2600),
    ("pg_am", 2601),
    ("pg_amop", 2602),
    ("pg_amproc", 2603),
    ("pg_attrdef", 2604),
    ("pg_cast", 2605),
    ("pg_constraint", 2606),
    ("pg_conversion", 2607),
    ("pg_depend", 2608),
    ("pg_description", 2609),
    ("pg_index", 2610),
    ("pg_inherits", 2611),
    ("pg_language", 2612),
    ("pg_largeobject", 2613),
    ("pg_namespace", 2615),
    ("pg_opclass", 2616),
    ("pg_operator", 2617),
    ("pg_rewrite", 2618),
    ("pg_statistic", 2619),
    ("pg_trigger", 2620),
    ("pg_opfamily", 2753),
    ("pg_db_role_setting", 2964),
    ("pg_largeobject_metadata", 2995),
    ("pg_extension", 3079),
    ("pg_foreign_table", 3118),
    ("pg_policy", 3256),
    ("pg_partitioned_table", 3350),
    ("pg_statistic_ext", 3381),
    ("pg_init_privs", 3394),
    ("pg_statistic_ext_data", 3429),
    ("pg_collation", 3456),
    ("pg_event_trigger", 3466),
    ("pg_enum", 3501),
    ("pg_range", 3541),
    ("pg_transform", 3576),
    ("pg_shseclabel", 3592),
    ("pg_seclabel", 3596),
    ("pg_ts_dict", 3600),
    ("pg_ts_parser", 3601),
    ("pg_ts_config", 3602),
    ("pg_ts_config_map", 3603),
    ("pg_ts_template", 3764),
    ("pg_replication_origin", 6000),
    ("pg_subscription", 6100),
    ("pg_subscription_rel", 6102),
    ("pg_publication", 6104),
    ("pg_publication_rel", 6106),
    ("pg_publication_namespace", 6237),
    ("pg_parameter_acl", 6243),
];

/// The oid of the relation that `input` names, with PostgreSQL's rules for a value of type regclass read from text: a number or `-` is an oid.
pub(crate) fn regclass_of_input(conn: &Connection, input: &str) -> Result<i64> {
    match oid_of_dash_or_digits(input) {
        Some(oid) => oid,
        None => regclass_of_name(conn, input),
    }
}

/// The oid of the relation that `name` names, which is always a relation name, even when it is all digits.
pub(crate) fn regclass_of_name(conn: &Connection, name: &str) -> Result<i64> {
    let parts = split_name(name)?;
    let (schema, relation) = match parts.as_slice() {
        [relation] => (None, relation),
        [schema, relation] => (Some(schema.as_str()), relation),
        [database, schema, relation] => {
            let current = db_name_from_path(conn.db_file_path());
            if *database != current {
                return Err(sqlstate_error(
                    SQLSTATE_FEATURE_NOT_SUPPORTED,
                    format!(
                        "cross-database references are not implemented: {}",
                        parts.join(".")
                    ),
                ));
            }
            (Some(schema.as_str()), relation)
        }
        _ => {
            return Err(sqlstate_error(
                SQLSTATE_SYNTAX_ERROR,
                format!(
                    "improper relation name (too many dotted names): {}",
                    parts.join(".")
                ),
            ))
        }
    };
    relation_oid(conn, schema, relation).ok_or_else(|| {
        let shown = match schema {
            Some(schema) => format!("{schema}.{relation}"),
            None => relation.to_string(),
        };
        sqlstate_error(
            SQLSTATE_UNDEFINED_TABLE,
            format!("relation \"{shown}\" does not exist"),
        )
    })
}

/// The oid of the schema that `input` names, with PostgreSQL's rules for a value of type regnamespace read from text: a number or `-` is an oid.
pub(crate) fn regnamespace_of_input(conn: &Connection, input: &str) -> Result<i64> {
    if let Some(oid) = oid_of_dash_or_digits(input) {
        return oid;
    }
    let parts = split_name(input)?;
    let [schema] = parts.as_slice() else {
        return Err(sqlstate_error(SQLSTATE_INVALID_NAME, "invalid name syntax"));
    };
    namespace_oid_by_name(conn, schema).ok_or_else(|| {
        sqlstate_error(
            SQLSTATE_INVALID_SCHEMA_NAME,
            format!("schema \"{schema}\" does not exist"),
        )
    })
}

fn relation_oid(conn: &Connection, schema: Option<&str>, relation: &str) -> Option<i64> {
    let searched = match schema {
        Some(schema) => vec![schema.to_string()],
        None => current_schemas(conn, true),
    };
    let current = conn.current_schema();
    searched.iter().find_map(|searched| {
        if searched == "pg_catalog" {
            return SYSTEM_CATALOG_OIDS
                .iter()
                .find(|(name, _)| *name == relation)
                .map(|(_, oid)| *oid);
        }
        let namespace = current.namespace_named(searched)?;
        relation_oid_by_name(&current, namespace, relation)
    })
}

fn oid_of_dash_or_digits(input: &str) -> Option<Result<i64>> {
    if input == "-" {
        return Some(Ok(0));
    }
    let is_digits = input.starts_with(|c: char| c.is_ascii_digit())
        && input.bytes().all(|b| b.is_ascii_digit());
    is_digits.then(|| parse_oid(input).map_err(parse_error_to_core))
}

fn split_name(name: &str) -> Result<Vec<String>> {
    let invalid = || sqlstate_error(SQLSTATE_INVALID_NAME, "invalid name syntax");
    let is_space = |c: char| matches!(c, ' ' | '\t' | '\n' | '\x0b' | '\x0c' | '\r');
    let mut rest = name.trim_start_matches(is_space);
    if rest.is_empty() {
        return Err(invalid());
    }
    let mut parts = Vec::new();
    loop {
        let (part, after) = if let Some(quoted) = rest.strip_prefix('"') {
            read_quoted_part(quoted).ok_or_else(invalid)?
        } else {
            let end = rest
                .find(|c: char| c == '.' || is_space(c))
                .unwrap_or(rest.len());
            (rest[..end].to_ascii_lowercase(), &rest[end..])
        };
        if part.is_empty() {
            return Err(invalid());
        }
        parts.push(truncate_name(part));
        rest = after.trim_start_matches(is_space);
        match rest.strip_prefix('.') {
            Some(next) => rest = next.trim_start_matches(is_space),
            None if rest.is_empty() => return Ok(parts),
            None => return Err(invalid()),
        }
    }
}

fn read_quoted_part(after_quote: &str) -> Option<(String, &str)> {
    let mut part = String::new();
    let mut chars = after_quote.char_indices();
    while let Some((index, c)) = chars.next() {
        if c != '"' {
            part.push(c);
            continue;
        }
        let rest = &after_quote[index + 1..];
        match rest.strip_prefix('"') {
            Some(_) => {
                part.push('"');
                chars.next();
            }
            None => return Some((part, rest)),
        }
    }
    None
}

fn truncate_name(mut name: String) -> String {
    if name.len() > MAX_NAME_BYTES {
        let mut end = MAX_NAME_BYTES;
        while !name.is_char_boundary(end) {
            end -= 1;
        }
        name.truncate(end);
    }
    name
}
