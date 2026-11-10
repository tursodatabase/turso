//! What the translator knows about PostgreSQL's built-in functions before
//! the engine has resolved anything: whether a function always returns the
//! same value for the same arguments, whether it returns a set of rows, and
//! whether it is an aggregate.

use phf::phf_map;

/// How the immutability of a function depends on its arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Immutability {
    /// Always returns the same value for the same arguments.
    Always,
    /// Never returns the same value for the same arguments (STABLE or VOLATILE).
    Never,
    /// Immutable when the argument count equals `n`.
    ArgCount(usize),
    /// Immutable when the argument at `position` is not timezone-dependent.
    NonTzArg(usize),
    /// Immutable when the argument count equals `count` OR the argument at
    /// `position` is not timezone-dependent.
    ArgCountOrNonTz { count: usize, position: usize },
}

impl Immutability {
    /// Whether this function call is immutable given its `arg_count` and a
    /// predicate `is_tz_arg(position)` that returns true when the argument at
    /// that position is timezone-dependent. The predicate is only called when
    /// the answer depends on it, and its error is passed on.
    pub fn check<E>(
        self,
        arg_count: usize,
        is_tz_arg: impl Fn(usize) -> Result<bool, E>,
    ) -> Result<bool, E> {
        Ok(match self {
            Self::Always => true,
            Self::Never => false,
            Self::ArgCount(n) => arg_count == n,
            Self::NonTzArg(pos) => !is_tz_arg(pos)?,
            Self::ArgCountOrNonTz { count, position } => {
                arg_count == count || !is_tz_arg(position)?
            }
        })
    }
}

/// A PostgreSQL built-in function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgFunc {
    /// How the immutability of this function depends on its arguments.
    pub immutability: Immutability,
    /// Whether the function returns a set of rows.
    pub set_returning: bool,
    /// Whether the function is an aggregate.
    pub aggregate: bool,
}

/// Whether `name` is a PostgreSQL system column.
pub fn is_system_column(name: &str) -> bool {
    matches!(
        name,
        "tableoid" | "ctid" | "xmin" | "xmax" | "cmin" | "cmax"
    )
}

/// The built-in function called `name`, compared without regard to case.
/// A name not in the registry is an immutable, non-set-returning function.
pub fn pg_func(name: &str) -> Option<&'static PgFunc> {
    PG_FUNCS.get(name.to_ascii_lowercase().as_str())
}

#[rustfmt::skip]
static PG_FUNCS: phf::Map<&'static str, PgFunc> = phf_map! {
    "abs" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "age" => PgFunc { immutability: Immutability::ArgCount(2), set_returning: false, aggregate: false },
    "array_append" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "array_cat" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "array_length" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "array_lower" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "array_position" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "array_prepend" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "array_remove" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "array_to_string" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "array_upper" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "ascii" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "bit_length" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "btrim" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "cbrt" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "ceil" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "ceiling" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "char_length" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "character_length" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "chr" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "clock_timestamp" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "coalesce" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "concat" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "concat_ws" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "current_catalog" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "current_database" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "current_date" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "current_query" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "current_role" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "current_schema" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "current_schemas" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "current_setting" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "current_time" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "current_timestamp" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "current_user" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "currval" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "date_part" => PgFunc { immutability: Immutability::NonTzArg(1), set_returning: false, aggregate: false },
    "date_trunc" => PgFunc { immutability: Immutability::ArgCountOrNonTz { count: 3, position: 1 }, set_returning: false, aggregate: false },
    "decode" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "degrees" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "div" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "encode" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "exp" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "extract" => PgFunc { immutability: Immutability::NonTzArg(1), set_returning: false, aggregate: false },
    "floor" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "format" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "gen_random_uuid" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "generate_series" => PgFunc { immutability: Immutability::ArgCountOrNonTz { count: 4, position: 0 }, set_returning: true, aggregate: false },
    "generate_subscripts" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "greatest" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "has_column_privilege" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "has_database_privilege" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "has_function_privilege" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "has_schema_privilege" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "has_sequence_privilege" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "has_table_privilege" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "inet_client_addr" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "inet_client_port" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "inet_server_addr" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "inet_server_port" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "initcap" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "json_array_elements" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "json_array_elements_text" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "json_array_length" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "json_build_array" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "json_build_object" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "json_each" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "json_each_text" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "json_extract_path" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "json_extract_path_text" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "json_object_keys" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "json_populate_recordset" => PgFunc { immutability: Immutability::Never, set_returning: true, aggregate: false },
    "json_to_recordset" => PgFunc { immutability: Immutability::Never, set_returning: true, aggregate: false },
    "json_typeof" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "jsonb_array_elements" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "jsonb_array_elements_text" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "jsonb_array_length" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "jsonb_build_array" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "jsonb_build_object" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "jsonb_each" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "jsonb_each_text" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "jsonb_extract_path" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "jsonb_extract_path_text" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "jsonb_object_keys" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "jsonb_path_query" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "jsonb_populate_recordset" => PgFunc { immutability: Immutability::Never, set_returning: true, aggregate: false },
    "jsonb_set" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "jsonb_strip_nulls" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "jsonb_to_recordset" => PgFunc { immutability: Immutability::Never, set_returning: true, aggregate: false },
    "jsonb_typeof" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "lastval" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "least" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "left" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "length" => PgFunc { immutability: Immutability::ArgCount(1), set_returning: false, aggregate: false },
    "ln" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "localtime" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "localtimestamp" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "log" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "lower" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "lpad" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "ltrim" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "md5" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "mod" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "nextval" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "now" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "nullif" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "octet_length" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "overlay" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "pg_advisory_lock" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_advisory_unlock" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_advisory_unlock_all" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_advisory_xact_lock" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_backend_pid" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_cancel_backend" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_conf_load_time" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_current_wal_lsn" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_database_size" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_get_constraintdef" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_get_expr" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_get_indexdef" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_get_userbyid" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_get_viewdef" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_has_role" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_is_in_recovery" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_listening_channels" => PgFunc { immutability: Immutability::Never, set_returning: true, aggregate: false },
    "pg_notify" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_postmaster_start_time" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_relation_size" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_sleep" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_sleep_for" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_sleep_until" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_table_is_visible" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_table_size" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_terminate_backend" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_total_relation_size" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_try_advisory_lock" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pg_typeof" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "pi" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "position" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "power" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "quote_ident" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "quote_literal" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "quote_nullable" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "radians" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "random" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "random_normal" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "regexp_matches" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "regexp_replace" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "regexp_split_to_array" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "regexp_split_to_table" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "repeat" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "replace" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "reverse" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "right" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "round" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "rpad" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "rtrim" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "session_user" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "set_config" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "setseed" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "setval" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "sign" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "split_part" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "sqrt" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "statement_timestamp" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "string_to_array" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "string_to_table" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "strpos" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "substr" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "substring" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "timeofday" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "to_char" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "to_date" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "to_hex" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "to_number" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "to_timestamp" => PgFunc { immutability: Immutability::ArgCount(1), set_returning: false, aggregate: false },
    "transaction_timestamp" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "translate" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "trim" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "trunc" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "txid_current" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "unnest" => PgFunc { immutability: Immutability::Always, set_returning: true, aggregate: false },
    "upper" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "user" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "uuid_generate_v1" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "uuid_generate_v4" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "uuidv4" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "uuidv7" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "version" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: false },
    "width_bucket" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: false },
    "array_agg" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "avg" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "bit_and" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "bit_or" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "bit_xor" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "bool_and" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "bool_or" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "count" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "every" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "json_agg" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: true },
    "json_object_agg" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: true },
    "jsonb_agg" => PgFunc { immutability: Immutability::Never, set_returning: false, aggregate: true },
    "jsonb_object_agg" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "max" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "min" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "stddev" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "stddev_pop" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "stddev_samp" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "string_agg" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "sum" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "var_pop" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "var_samp" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "variance" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
    "xmlagg" => PgFunc { immutability: Immutability::Always, set_returning: false, aggregate: true },
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_registered_name_is_lowercase() {
        for name in PG_FUNCS.keys() {
            assert_eq!(*name, name.to_ascii_lowercase());
        }
    }

    #[test]
    fn lookup_ignores_case() {
        let func = pg_func("NOW").unwrap();
        assert_eq!(func.immutability, Immutability::Never);
        assert!(!func.set_returning);
        assert_eq!(pg_func("Generate_Series"), pg_func("generate_series"));
    }

    #[test]
    fn volatile_functions_are_not_immutable() {
        for name in [
            "now",
            "random",
            "currval",
            "nextval",
            "clock_timestamp",
            "timeofday",
            "statement_timestamp",
            "transaction_timestamp",
            "pg_sleep",
            "inet_client_addr",
            "inet_server_addr",
            "pg_postmaster_start_time",
            "uuid_generate_v4",
            "uuidv4",
            "uuidv7",
        ] {
            let func = pg_func(name).unwrap_or_else(|| panic!("{name} is not registered"));
            assert_eq!(
                func.immutability,
                Immutability::Never,
                "{name} must not be immutable"
            );
            assert!(!func.set_returning, "{name} must not return a set");
        }
    }

    #[test]
    fn set_returning_functions_return_sets() {
        for name in [
            "generate_series",
            "generate_subscripts",
            "unnest",
            "json_array_elements",
            "json_array_elements_text",
            "json_each",
            "json_each_text",
            "json_object_keys",
            "jsonb_array_elements",
            "jsonb_array_elements_text",
            "jsonb_each",
            "jsonb_each_text",
            "jsonb_object_keys",
            "regexp_matches",
            "regexp_split_to_table",
        ] {
            let func = pg_func(name).unwrap_or_else(|| panic!("{name} is not registered"));
            assert!(func.set_returning, "{name} must return a set");
        }
    }

    #[test]
    fn unknown_function_is_not_registered() {
        assert_eq!(pg_func("my_function"), None);
        assert_eq!(pg_func(""), None);
    }

    #[test]
    fn stable_functions_are_not_immutable() {
        for name in [
            "json_agg",
            "jsonb_agg",
            "json_object_agg",
            "json_populate_recordset",
            "jsonb_populate_recordset",
            "json_to_recordset",
            "jsonb_to_recordset",
            "pg_listening_channels",
        ] {
            let func = pg_func(name).unwrap_or_else(|| panic!("{name} is not registered"));
            assert_eq!(
                func.immutability,
                Immutability::Never,
                "{name} must not be immutable"
            );
        }
    }

    #[test]
    fn immutability_that_depends_on_the_call() {
        let never_tz = |_: usize| Ok::<_, ()>(false);
        let tz = |_: usize| Ok::<_, ()>(true);
        let unknown = |_: usize| Err(());
        let length = pg_func("length").unwrap().immutability;
        assert_eq!(length.check(1, never_tz), Ok(true));
        assert_eq!(length.check(2, never_tz), Ok(false));
        let series = pg_func("generate_series").unwrap().immutability;
        assert_eq!(series.check(3, never_tz), Ok(true));
        assert_eq!(series.check(3, tz), Ok(false));
        assert_eq!(series.check(4, tz), Ok(true));
        assert_eq!(series.check(4, unknown), Ok(true));
        assert_eq!(series.check(3, unknown), Err(()));
    }

    #[test]
    fn system_columns_match_lowercase_only() {
        assert!(is_system_column("ctid"));
        assert!(is_system_column("xmin"));
        assert!(is_system_column("xmax"));
        assert!(is_system_column("cmin"));
        assert!(is_system_column("cmax"));
        assert!(is_system_column("tableoid"));
        assert!(!is_system_column("CTID"));
        assert!(!is_system_column("Ctid"));
    }
}
