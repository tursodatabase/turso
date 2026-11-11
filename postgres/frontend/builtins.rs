//! SQLite built-ins are not available in tursopg. Core has the built-in
//! functions of SQLite and Turso, such as `typeof()` and `iif()`, and the
//! functions of its built-in extensions, such as `uuid4()`. tursopg has only
//! those that PostgreSQL also has. A call to another one fails, as a call to
//! an unknown function fails in PostgreSQL.
//!
//! The frontend's own functions, such as `to_char()`, are not core built-ins,
//! so this module does not name them.

use turso_pg_parser::translator::FUNCTIONS_ONLY_THE_TRANSLATOR_CALLS;

/// The core built-in functions that PostgreSQL also has.
const BUILTINS_THAT_POSTGRES_HAS: &[&str] = &[
    "abs",
    "acos",
    "acosh",
    "array_agg",
    "array_append",
    "array_cat",
    "array_length",
    "array_position",
    "array_prepend",
    "array_remove",
    "array_to_string",
    "array_upper",
    "asin",
    "asinh",
    "atan",
    "atan2",
    "atanh",
    "avg",
    "btrim",
    "ceil",
    "ceiling",
    "char",
    "char_length",
    "character_length",
    "chr",
    "concat",
    "concat_ws",
    "cos",
    "cosh",
    "count",
    "cume_dist",
    "currval",
    "date",
    "degrees",
    "dense_rank",
    "exp",
    "first_value",
    "floor",
    "format",
    "gcd",
    "gen_random_uuid",
    "get_byte",
    "json_array_length",
    "json_object",
    "jsonb_insert",
    "jsonb_object",
    "jsonb_set",
    "lag",
    "last_value",
    "lcm",
    "lead",
    "length",
    "like",
    "ln",
    "log",
    "log10",
    "lower",
    "lpad",
    "ltrim",
    "make_date",
    "make_timestamp",
    "max",
    "min",
    "mod",
    "nextval",
    "nth_value",
    "ntile",
    "numeric_add",
    "numeric_div",
    "numeric_mod",
    "numeric_mul",
    "numeric_sub",
    "octet_length",
    "percent_rank",
    "percentile_cont",
    "percentile_disc",
    "pi",
    "pow",
    "power",
    "radians",
    "rank",
    "repeat",
    "replace",
    "reverse",
    "round",
    "row_number",
    "rpad",
    "rtrim",
    "set_byte",
    "setval",
    "sign",
    "sin",
    "sinh",
    "sqrt",
    "stddev",
    "string_agg",
    "string_to_array",
    "strpos",
    "substr",
    "substring",
    "sum",
    "tan",
    "tanh",
    "time",
    "to_timestamp",
    "trunc",
    "unistr",
    "upper",
];

/// Whether tursopg has the core built-in function `name`. tursopg also has
/// the built-ins that the translator calls in the SQL it writes, such as
/// `array_element()` for a subscript. The translator does not let user SQL
/// call those.
pub(crate) fn has_builtin_function(name: &str) -> bool {
    BUILTINS_THAT_POSTGRES_HAS
        .iter()
        .chain(FUNCTIONS_ONLY_THE_TRANSLATOR_CALLS)
        .any(|builtin| builtin.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn builtins_that_postgres_has_are_sorted_without_duplicates() {
        assert!(BUILTINS_THAT_POSTGRES_HAS
            .windows(2)
            .all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn builtins_that_postgres_has_are_core_functions() {
        let db = crate::session::open_database_with_io(
            Arc::new(turso_core::MemoryIO::new()),
            "builtins.db",
            crate::OpenFlags::default(),
            crate::DatabaseOpts::new(),
        )
        .unwrap();
        let conn = db.connect(turso_core::ConnectOpts::new()).unwrap();
        let extension_functions: Vec<String> = conn
            .get_syms_functions()
            .into_iter()
            .map(|(name, ..)| name)
            .collect();
        for name in BUILTINS_THAT_POSTGRES_HAS {
            let is_builtin = (0..=3).any(|arg_count| {
                matches!(crate::Func::resolve_function(name, arg_count), Ok(Some(_)))
            });
            assert!(
                is_builtin || extension_functions.iter().any(|function| function == name),
                "{name} is not a core function"
            );
        }
    }
}
