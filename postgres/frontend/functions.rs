use std::sync::Arc;
use turso_core::schema::{Schema, Table};
use turso_core::{Connection, LimboError, Result, Value};
use turso_parser::ast::RefAct;

const USER_TABLE_OID_START: i64 = 16384;

/// Resolve a PostgreSQL scalar function by name and argument count. Entry
/// point for [`crate::catalog::PostgresDialect::resolve_function`].
pub(crate) fn resolve_scalar(name: &str, arg_count: usize) -> bool {
    let arities: &[i64] = match name {
        "pg_get_userbyid"
        | "pg_table_is_visible"
        | "pg_function_is_visible"
        | "pg_type_is_visible"
        | "pg_encoding_to_char"
        | "pg_get_function_result"
        | "pg_get_function_arguments"
        | "pg_get_statisticsobjdef_columns"
        | "pg_relation_is_publishable"
        | "quote_ident"
        | "quote_literal" => &[1],
        "md5" | "initcap" | "pg_typeof" => &[1],
        "left" | "right" | "encode" | "decode" => &[2],
        "split_part" => &[3],
        "regexp_replace" => &[3, 4],
        "format_type" | "pg_get_constraintdef" | "pg_get_indexdef" | "obj_description" => &[1, 2],
        "pg_get_expr" => &[2, 3],
        "to_char" | "pg_input_is_valid" | "booleq" | "boolne" | "col_description" => &[2],
        // Overrides the engine's SQLite-style random(), which returns a
        // 64-bit integer; PostgreSQL returns a double in [0, 1).
        "random" => &[0],
        "setseed" => &[1],
        "version" | "current_database" | "current_schema" | "pg_backend_pid" => &[0],
        _ => return false,
    };
    arities.contains(&(arg_count as i64))
}

/// Execute a PostgreSQL scalar function by name. Entry point for
/// [`crate::catalog::PostgresDialect::scalar_function`].
pub(crate) fn exec_scalar(conn: &Connection, name: &str, args: &[Value]) -> Result<Value> {
    let int_arg = |i: usize, default: i64| args.get(i).and_then(|v| v.as_int()).unwrap_or(default);
    let text_arg = |i: usize| match args.get(i) {
        Some(Value::Text(t)) => t.as_str().to_string(),
        _ => String::new(),
    };
    match name {
        "pg_get_userbyid" => Ok(exec_pg_get_user_by_id(int_arg(0, 0))),
        "pg_table_is_visible" | "pg_function_is_visible" | "pg_type_is_visible" => {
            Ok(exec_pg_is_visible(int_arg(0, 0)))
        }
        "pg_get_constraintdef" => Ok(exec_pg_get_constraintdef(conn, int_arg(0, 0))),
        "pg_get_indexdef" => Ok(exec_pg_get_indexdef(conn, int_arg(0, 0))),
        "pg_encoding_to_char" => Ok(exec_pg_encoding_to_char(int_arg(0, 0))),
        "format_type" => Ok(exec_pg_format_type(int_arg(0, 0), int_arg(1, -1))),
        "to_char" => Ok(exec_to_char(
            args.first().unwrap_or(&Value::Null),
            &text_arg(1),
        )),
        "pg_input_is_valid" => Ok(exec_pg_input_is_valid(
            args.first().unwrap_or(&Value::Null),
            &text_arg(1),
        )),
        "booleq" => Ok(Value::from_i64((args.first() == args.get(1)) as i64)),
        "boolne" => Ok(Value::from_i64((args.first() != args.get(1)) as i64)),
        "version" => Ok(exec_version()),
        "current_database" => Ok(Value::build_text(crate::catalog::db_name_from_path(
            conn.db_file_path(),
        ))),
        // pg_catalog presents every user object under the hardcoded "public"
        // namespace, so that is always the current schema.
        "current_schema" => Ok(Value::build_text("public")),
        "pg_backend_pid" => Ok(Value::from_i64(std::process::id() as i64)),
        "quote_ident" => match args.first() {
            Some(Value::Null) | None => Ok(Value::Null),
            _ => Ok(Value::build_text(turso_pg_parser::quote_identifier(
                &text_arg(0),
            ))),
        },
        "quote_literal" => Ok(exec_quote_literal(args.first().unwrap_or(&Value::Null))),
        "md5" => Ok(exec_md5(args.first().unwrap_or(&Value::Null))),
        "initcap" => Ok(match args.first() {
            Some(Value::Null) | None => Value::Null,
            _ => Value::build_text(exec_initcap(&text_arg(0))),
        }),
        "left" | "right" => Ok(match (args.first(), args.get(1)) {
            (Some(Value::Null), _) | (_, Some(Value::Null)) | (None, _) | (_, None) => Value::Null,
            _ => Value::build_text(exec_left_right(&text_arg(0), int_arg(1, 0), name == "left")),
        }),
        "split_part" => match (args.first(), args.get(1), args.get(2)) {
            (Some(Value::Null), _, _) | (_, Some(Value::Null), _) | (_, _, Some(Value::Null)) => {
                Ok(Value::Null)
            }
            _ => exec_split_part(&text_arg(0), &text_arg(1), int_arg(2, 0)),
        },
        "regexp_replace" => match (args.first(), args.get(1), args.get(2)) {
            (Some(Value::Null), _, _) | (_, Some(Value::Null), _) | (_, _, Some(Value::Null)) => {
                Ok(Value::Null)
            }
            _ => exec_regexp_replace(&text_arg(0), &text_arg(1), &text_arg(2), &text_arg(3)),
        },
        "pg_typeof" => Ok(exec_pg_typeof(args.first().unwrap_or(&Value::Null))),
        "random" => Ok(exec_random()),
        "setseed" => exec_setseed(args.first().unwrap_or(&Value::Null)),
        "encode" => match (args.first(), args.get(1)) {
            (Some(Value::Null), _) | (_, Some(Value::Null)) | (None, _) | (_, None) => {
                Ok(Value::Null)
            }
            (Some(data), _) => exec_encode(data, &text_arg(1)),
        },
        "decode" => match (args.first(), args.get(1)) {
            (Some(Value::Null), _) | (_, Some(Value::Null)) | (None, _) | (_, None) => {
                Ok(Value::Null)
            }
            _ => exec_decode(&text_arg(0), &text_arg(1)),
        },
        // Catalog introspection stubs: accepted for compatibility, no output.
        // obj_description/col_description are NULL because COMMENT ON is not persisted.
        "pg_get_expr"
        | "pg_get_statisticsobjdef_columns"
        | "pg_relation_is_publishable"
        | "pg_get_function_result"
        | "pg_get_function_arguments"
        | "obj_description"
        | "col_description" => Ok(Value::Null),
        _ => Err(LimboError::ParseError(format!("no such function: {name}"))),
    }
}

fn exec_pg_get_user_by_id(_oid: i64) -> Value {
    Value::build_text("turso")
}

fn exec_pg_is_visible(_oid: i64) -> Value {
    Value::from_i64(1)
}

/// PostgreSQL version advertised by version(). The major version matches the
/// server_version startup parameter pgwire's DefaultServerParameterProvider
/// sends, so the two claims agree.
const SERVER_VERSION: &str = "16.6";

/// Clients sometimes gate connection setup on this string: knex and TypeORM regex
/// parse it as `^PostgreSQL ([\d.]+)`, so it must lead with a numeric version.
fn exec_version() -> Value {
    Value::build_text(format!(
        "PostgreSQL {SERVER_VERSION} (Turso v{})",
        env!("CARGO_PKG_VERSION")
    ))
}

/// PostgreSQL's quote_literal(): wrap a value in single quotes for inclusion
/// in SQL, doubling embedded quotes. Backslashes force the E'' escape-string
/// form so the result reads back identically regardless of
/// standard_conforming_strings.
fn exec_quote_literal(value: &Value) -> Value {
    if matches!(value, Value::Null) {
        return Value::Null;
    }
    let text = match value {
        Value::Text(t) => t.as_str().to_string(),
        other => other.to_string(),
    };
    let quoted = if text.contains('\\') {
        format!("E'{}'", text.replace('\\', "\\\\").replace('\'', "''"))
    } else {
        format!("'{}'", text.replace('\'', "''"))
    };
    Value::build_text(quoted)
}

/// md5() accepts text or bytea and returns the lowercase hex digest.
fn exec_md5(value: &Value) -> Value {
    let bytes: &[u8] = match value {
        Value::Text(t) => t.as_str().as_bytes(),
        Value::Blob(b) => b.as_ref(),
        _ => return Value::Null,
    };
    Value::build_text(format!("{:x}", md5::compute(bytes)))
}

/// A word starts after any non-alphanumeric character, matching PostgreSQL's
/// definition rather than whitespace-only splitting.
fn exec_initcap(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_is_word = false;
    for c in s.chars() {
        if prev_is_word {
            out.extend(c.to_lowercase());
        } else {
            out.extend(c.to_uppercase());
        }
        prev_is_word = c.is_alphanumeric();
    }
    out
}

/// left()/right() count characters, not bytes; a negative count drops that
/// many characters from the other end instead.
fn exec_left_right(s: &str, n: i64, from_left: bool) -> String {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len();
    let keep = if n >= 0 {
        (n as usize).min(len)
    } else {
        len.saturating_sub(n.unsigned_abs() as usize)
    };
    if from_left {
        chars[..keep].iter().collect()
    } else {
        chars[len - keep..].iter().collect()
    }
}

fn exec_split_part(s: &str, delimiter: &str, n: i64) -> Result<Value> {
    if n == 0 {
        return Err(LimboError::ParseError(
            "field position must not be zero".to_string(),
        ));
    }
    // An empty delimiter means the whole string is one field.
    let fields: Vec<&str> = if delimiter.is_empty() {
        vec![s]
    } else {
        s.split(delimiter).collect()
    };
    // A negative position counts fields from the end.
    let idx = if n > 0 {
        (n - 1) as usize
    } else {
        match fields.len().checked_sub(n.unsigned_abs() as usize) {
            Some(i) => i,
            None => return Ok(Value::build_text("")),
        }
    };
    Ok(Value::build_text(
        fields.get(idx).copied().unwrap_or("").to_string(),
    ))
}

fn exec_regexp_replace(
    source: &str,
    pattern: &str,
    replacement: &str,
    flags: &str,
) -> Result<Value> {
    let mut global = false;
    let mut case_insensitive = false;
    for c in flags.chars() {
        match c {
            'g' => global = true,
            'i' => case_insensitive = true,
            'c' => case_insensitive = false,
            _ => {
                return Err(LimboError::ParseError(format!(
                    "invalid regular expression option: \"{c}\""
                )))
            }
        }
    }
    let re = regex::RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .build()
        .map_err(|e| LimboError::ParseError(format!("invalid regular expression: {e}")))?;
    // PostgreSQL replacement text uses \1..\9 and \& for backreferences; the
    // regex crate uses ${n}, treats $ as special, and backslash as literal.
    let mut converted = String::with_capacity(replacement.len());
    let mut chars = replacement.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '$' => converted.push_str("$$"),
            '\\' => match chars.next() {
                Some(d @ '1'..='9') => converted.push_str(&format!("${{{d}}}")),
                Some('&') => converted.push_str("${0}"),
                Some(other) => converted.push(other),
                None => {}
            },
            other => converted.push(other),
        }
    }
    let result = if global {
        re.replace_all(source, converted.as_str())
    } else {
        re.replace(source, converted.as_str())
    };
    Ok(Value::build_text(result.into_owned()))
}

/// The engine only knows the runtime storage class, so this reports the
/// closest PostgreSQL type name rather than the statically inferred type.
fn exec_pg_typeof(value: &Value) -> Value {
    use turso_core::Numeric;
    Value::build_text(match value {
        Value::Null => "unknown",
        Value::Numeric(Numeric::Integer(_)) => "integer",
        Value::Numeric(_) => "double precision",
        Value::Text(_) => "text",
        Value::Blob(_) => "bytea",
    })
}

/// State for random()/setseed(). Process-wide rather than per-session:
/// PostgreSQL scopes the seed to a backend, but sessions here share the
/// process, and the difference is only observable when two sessions
/// interleave draws after seeding — an accepted deviation.
static RANDOM_STATE: std::sync::Mutex<u64> = std::sync::Mutex::new(0);

fn next_random_u64() -> u64 {
    let mut state = RANDOM_STATE.lock().unwrap();
    if *state == 0 {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64 ^ d.as_secs())
            .unwrap_or(0x9e3779b97f4a7c15);
        *state = nanos ^ ((std::process::id() as u64) << 32) | 1;
    }
    // xorshift64*: fast, and quality is fine for SQL random().
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545F4914F6CDD1D)
}

/// PostgreSQL's random(): uniform double in [0, 1).
fn exec_random() -> Value {
    Value::from_f64((next_random_u64() >> 11) as f64 / (1u64 << 53) as f64)
}

/// PostgreSQL's setseed(): reseed the generator from a value in [-1, 1].
fn exec_setseed(value: &Value) -> Result<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let seed = value.to_float_or_zero();
    if !(-1.0..=1.0).contains(&seed) || seed.is_nan() {
        return Err(LimboError::ParseError(format!(
            "setseed parameter {seed} is out of allowed range [-1,1]"
        )));
    }
    // Map [-1, 1] onto the full state space; never leave the state zero.
    *RANDOM_STATE.lock().unwrap() = (seed.to_bits() ^ 0x9e3779b97f4a7c15) | 1;
    Ok(Value::Null)
}

fn exec_encode(data: &Value, format: &str) -> Result<Value> {
    let bytes: &[u8] = match data {
        Value::Text(t) => t.as_str().as_bytes(),
        Value::Blob(b) => b.as_ref(),
        _ => return Ok(Value::Null),
    };
    let out = match format {
        "hex" => data_encoding::HEXLOWER.encode(bytes),
        "base64" => {
            // PostgreSQL wraps base64 output at 76 characters per RFC 2045.
            let raw = data_encoding::BASE64.encode(bytes);
            let mut wrapped = String::with_capacity(raw.len() + raw.len() / 76 + 1);
            for (i, c) in raw.chars().enumerate() {
                if i > 0 && i % 76 == 0 {
                    wrapped.push('\n');
                }
                wrapped.push(c);
            }
            wrapped
        }
        "escape" => {
            let mut out = String::with_capacity(bytes.len());
            for &b in bytes {
                match b {
                    b'\\' => out.push_str("\\\\"),
                    0x20..=0x7e => out.push(b as char),
                    _ => out.push_str(&format!("\\{b:03o}")),
                }
            }
            out
        }
        other => {
            return Err(LimboError::ParseError(format!(
                "unrecognized encoding: \"{other}\""
            )))
        }
    };
    Ok(Value::build_text(out))
}

fn exec_decode(text: &str, format: &str) -> Result<Value> {
    let bytes = match format {
        "hex" => {
            let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
            data_encoding::HEXLOWER_PERMISSIVE
                .decode(compact.as_bytes())
                .map_err(|_| LimboError::ParseError("invalid hexadecimal data".to_string()))?
        }
        "base64" => {
            let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
            data_encoding::BASE64
                .decode(compact.as_bytes())
                .map_err(|_| LimboError::ParseError("invalid base64 data".to_string()))?
        }
        "escape" => {
            let mut out = Vec::with_capacity(text.len());
            let mut input = text.bytes().peekable();
            while let Some(b) = input.next() {
                if b != b'\\' {
                    out.push(b);
                    continue;
                }
                match input.next() {
                    Some(b'\\') => out.push(b'\\'),
                    Some(d1 @ b'0'..=b'3') => {
                        let d2 = input.next();
                        let d3 = input.next();
                        match (d2, d3) {
                            (Some(d2 @ b'0'..=b'7'), Some(d3 @ b'0'..=b'7')) => {
                                out.push((d1 - b'0') * 64 + (d2 - b'0') * 8 + (d3 - b'0'));
                            }
                            _ => {
                                return Err(LimboError::ParseError(
                                    "invalid input syntax for type bytea".to_string(),
                                ))
                            }
                        }
                    }
                    _ => {
                        return Err(LimboError::ParseError(
                            "invalid input syntax for type bytea".to_string(),
                        ))
                    }
                }
            }
            out
        }
        other => {
            return Err(LimboError::ParseError(format!(
                "unrecognized encoding: \"{other}\""
            )))
        }
    };
    Ok(Value::from_blob(bytes))
}

fn exec_pg_encoding_to_char(encoding: i64) -> Value {
    let name = match encoding {
        6 => "UTF8",
        0 => "SQL_ASCII",
        _ => "UTF8",
    };
    Value::build_text(name)
}

fn exec_pg_get_constraintdef(conn: &Connection, oid: i64) -> Value {
    match pg_get_constraintdef(conn, oid) {
        Some(s) => Value::build_text(s),
        None => Value::Null,
    }
}

fn exec_pg_get_indexdef(conn: &Connection, oid: i64) -> Value {
    match pg_get_indexdef(conn, oid) {
        Some(s) => Value::build_text(s),
        None => Value::Null,
    }
}

fn exec_pg_format_type(type_oid: i64, typemod: i64) -> Value {
    let type_name = match type_oid {
        16 => "boolean".to_string(),
        17 => "bytea".to_string(),
        18 => "\"char\"".to_string(),
        19 => "name".to_string(),
        20 => "bigint".to_string(),
        21 => "smallint".to_string(),
        23 => "integer".to_string(),
        25 => "text".to_string(),
        26 => "oid".to_string(),
        114 => "json".to_string(),
        700 => "real".to_string(),
        701 => "double precision".to_string(),
        1000 => "boolean[]".to_string(),
        1007 => "integer[]".to_string(),
        1009 => "text[]".to_string(),
        1022 => "double precision[]".to_string(),
        1042 => {
            if typemod > 4 {
                format!("character({})", typemod - 4)
            } else {
                "character".to_string()
            }
        }
        1043 => {
            if typemod > 4 {
                format!("character varying({})", typemod - 4)
            } else {
                "character varying".to_string()
            }
        }
        1082 => "date".to_string(),
        1083 => "time without time zone".to_string(),
        1114 => "timestamp without time zone".to_string(),
        1184 => "timestamp with time zone".to_string(),
        1186 => "interval".to_string(),
        1700 => {
            if typemod > 4 {
                let precision = ((typemod - 4) >> 16) & 0xffff;
                let scale = (typemod - 4) & 0xffff;
                format!("numeric({precision},{scale})")
            } else {
                "numeric".to_string()
            }
        }
        2205 => "regclass".to_string(),
        2206 => "regtype".to_string(),
        2278 => "void".to_string(),
        2950 => "uuid".to_string(),
        3802 => "jsonb".to_string(),
        _ => "unknown".to_string(),
    };
    Value::build_text(type_name)
}

/// Simplified to_char: formats a number with the given format pattern.
/// Supports basic PG numeric format patterns (9, 0, S, MI, FM, D, G, PR, TH, L).
fn exec_to_char(value: &Value, format: &str) -> Value {
    let num = match value {
        Value::Null => return Value::Null,
        Value::Numeric(_) => value.as_float(),
        Value::Text(t) => match t.as_str().parse::<f64>() {
            Ok(f) => f,
            Err(_) => return Value::Null,
        },
        _ => return Value::Null,
    };

    let result = pg_to_char_numeric(num, format);
    Value::build_text(result)
}

/// pg_input_is_valid(text, type) → boolean
/// Returns true if the text is valid input for the given type.
fn exec_pg_input_is_valid(input: &Value, type_name: &str) -> Value {
    let s = match input {
        Value::Text(t) => t.as_str().to_string(),
        Value::Null => return Value::Null,
        v => v.to_string(),
    };
    let valid = validate_pg_input(&s, type_name).is_none();
    Value::from_i64(if valid { 1 } else { 0 })
}

fn user_tables_sorted(schema: &Schema) -> Vec<(&String, &Arc<Table>)> {
    let mut tables: Vec<_> = schema
        .tables
        .iter()
        .filter(|(name, table)| {
            if name.starts_with("sqlite_")
                || name.starts_with("pg_")
                || name.starts_with("pragma_")
                || name.starts_with("json_")
            {
                return false;
            }
            matches!(table.as_ref(), Table::BTree(_))
        })
        .collect();
    tables.sort_by_key(|(name, _)| *name);
    tables
}

fn ref_act_to_char(act: &RefAct) -> &'static str {
    match act {
        RefAct::NoAction => "a",
        RefAct::Restrict => "r",
        RefAct::Cascade => "c",
        RefAct::SetNull => "n",
        RefAct::SetDefault => "d",
    }
}

fn ref_act_to_sql(code: &str) -> &'static str {
    match code {
        "r" => "RESTRICT",
        "c" => "CASCADE",
        "n" => "SET NULL",
        "d" => "SET DEFAULT",
        _ => "NO ACTION",
    }
}

fn pg_get_constraintdef(conn: &Connection, target_oid: i64) -> Option<String> {
    let schema = conn.current_schema();
    let tables = user_tables_sorted(&schema);
    let num_tables = tables.len() as i64;

    let mut next_index_oid = USER_TABLE_OID_START + num_tables;
    for (table_name, _) in &tables {
        for idx in schema.get_indices(table_name) {
            if !idx.ephemeral {
                next_index_oid += 1;
            }
        }
    }

    let mut constraint_oid = next_index_oid;

    for (_, table) in &tables {
        let btree = match table.as_ref() {
            Table::BTree(bt) => bt,
            _ => continue,
        };

        let has_pk_in_unique_sets = btree.unique_sets.iter().any(|us| us.is_primary_key);
        if !has_pk_in_unique_sets && !btree.primary_key_columns.is_empty() {
            if constraint_oid == target_oid {
                let cols: Vec<String> = btree
                    .primary_key_columns
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect();
                return Some(format!("PRIMARY KEY ({})", cols.join(", ")));
            }
            constraint_oid += 1;
        }

        for us in &btree.unique_sets {
            if constraint_oid == target_oid {
                let col_names: Vec<&str> = us.columns.iter().map(|c| c.name.as_str()).collect();
                let kw = if us.is_primary_key {
                    "PRIMARY KEY"
                } else {
                    "UNIQUE"
                };
                return Some(format!("{kw} ({})", col_names.join(", ")));
            }
            constraint_oid += 1;
        }

        for fk in &btree.foreign_keys {
            if constraint_oid == target_oid {
                let child_cols = fk.child_columns.join(", ");
                let parent_cols = fk.parent_columns.join(", ");
                let mut def = format!(
                    "FOREIGN KEY ({child_cols}) REFERENCES {}({parent_cols})",
                    fk.parent_table
                );
                let on_update = ref_act_to_char(&fk.on_update);
                let on_delete = ref_act_to_char(&fk.on_delete);
                if on_update != "a" {
                    def.push_str(&format!(" ON UPDATE {}", ref_act_to_sql(on_update)));
                }
                if on_delete != "a" {
                    def.push_str(&format!(" ON DELETE {}", ref_act_to_sql(on_delete)));
                }
                return Some(def);
            }
            constraint_oid += 1;
        }

        for chk in &btree.check_constraints {
            if constraint_oid == target_oid {
                return Some(format!("CHECK ({})", chk.expr));
            }
            constraint_oid += 1;
        }
    }

    None
}

fn pg_get_indexdef(conn: &Connection, target_oid: i64) -> Option<String> {
    let schema = conn.current_schema();
    let tables = user_tables_sorted(&schema);
    let num_tables = tables.len() as i64;

    let mut index_oid = USER_TABLE_OID_START + num_tables;
    for (table_name, _) in &tables {
        for idx in schema.get_indices(table_name) {
            if idx.ephemeral {
                continue;
            }
            if index_oid == target_oid {
                let unique = if idx.unique { "UNIQUE " } else { "" };
                let cols: Vec<String> = idx
                    .columns
                    .iter()
                    .map(|col| {
                        if let Some(expr) = &col.expr {
                            expr.to_string()
                        } else {
                            col.name.clone()
                        }
                    })
                    .collect();
                let mut def = format!(
                    "CREATE {unique}INDEX {} ON {table_name} USING btree ({})",
                    idx.name,
                    cols.join(", ")
                );
                if let Some(where_clause) = &idx.where_clause {
                    def.push_str(&format!(" WHERE {where_clause}"));
                }
                return Some(def);
            }
            index_oid += 1;
        }
    }

    None
}

/// Validate input for a PostgreSQL type, returning error info if invalid.
///
/// Returns `None` for valid input, or `Some((message, sql_error_code))` for invalid input.
/// Used by both `pg_input_error_info` (table-valued) and `pg_input_is_valid` (scalar).
pub(crate) fn validate_pg_input(input: &str, type_name: &str) -> Option<(String, String)> {
    let trimmed = input.trim();

    // Extract base type and optional length modifier, e.g. "varchar(4)" → ("varchar", Some(4))
    let (base_type, type_mod) = match type_name.find('(') {
        Some(pos) => {
            let base = type_name[..pos].trim();
            let mod_str = type_name[pos + 1..].trim_end_matches(')').trim();
            let modifier = mod_str.parse::<usize>().ok();
            (base.to_lowercase(), modifier)
        }
        None => (type_name.to_lowercase(), None),
    };

    match base_type.as_str() {
        "bool" | "boolean" => {
            let lower = trimmed.to_lowercase();
            let valid = matches!(
                lower.as_str(),
                "t" | "true" | "y" | "yes" | "on" | "1" | "f" | "false" | "n" | "no" | "off" | "0"
            );
            if valid {
                None
            } else {
                Some((
                    format!("invalid input syntax for type boolean: \"{input}\""),
                    "22P02".to_string(),
                ))
            }
        }
        "int2" | "smallint" => match trimmed.parse::<i64>() {
            Ok(v) if v < i16::MIN as i64 || v > i16::MAX as i64 => Some((
                format!("value \"{input}\" is out of range for type smallint"),
                "22003".to_string(),
            )),
            Ok(_) => None,
            Err(_) => Some((
                format!("invalid input syntax for type smallint: \"{input}\""),
                "22P02".to_string(),
            )),
        },
        "int4" | "integer" | "int" => match trimmed.parse::<i64>() {
            Ok(v) if v < i32::MIN as i64 || v > i32::MAX as i64 => Some((
                format!("value \"{input}\" is out of range for type integer"),
                "22003".to_string(),
            )),
            Ok(_) => None,
            Err(_) => Some((
                format!("invalid input syntax for type integer: \"{input}\""),
                "22P02".to_string(),
            )),
        },
        "int8" | "bigint" => match trimmed.parse::<i64>() {
            Ok(_) => None,
            Err(_) => {
                if trimmed.parse::<i128>().is_ok() {
                    Some((
                        format!("value \"{input}\" is out of range for type bigint"),
                        "22003".to_string(),
                    ))
                } else {
                    Some((
                        format!("invalid input syntax for type bigint: \"{input}\""),
                        "22P02".to_string(),
                    ))
                }
            }
        },
        "float4" | "real" => match trimmed.parse::<f32>() {
            Ok(v) if v.is_infinite() => Some((
                format!("value \"{input}\" is out of range for type real"),
                "22003".to_string(),
            )),
            Ok(_) => None,
            Err(_) => Some((
                format!("invalid input syntax for type real: \"{input}\""),
                "22P02".to_string(),
            )),
        },
        "float8" | "double precision" => match trimmed.parse::<f64>() {
            Ok(v) if v.is_infinite() => Some((
                format!("value \"{input}\" is out of range for type double precision"),
                "22003".to_string(),
            )),
            Ok(_) => None,
            Err(_) => Some((
                format!("invalid input syntax for type double precision: \"{input}\""),
                "22P02".to_string(),
            )),
        },
        "numeric" | "decimal" => match trimmed.parse::<f64>() {
            Ok(v) if v.is_nan() || v.is_infinite() => Some((
                format!("invalid input syntax for type numeric: \"{input}\""),
                "22P02".to_string(),
            )),
            Ok(_) => None,
            Err(_) => Some((
                format!("invalid input syntax for type numeric: \"{input}\""),
                "22P02".to_string(),
            )),
        },
        "text" => None,
        "varchar" | "character varying" => {
            if let Some(max_len) = type_mod {
                if trimmed.chars().count() > max_len {
                    return Some((
                        format!("value too long for type character varying({max_len})"),
                        "22001".to_string(),
                    ));
                }
            }
            None
        }
        "char" | "character" => {
            if let Some(max_len) = type_mod {
                if trimmed.chars().count() > max_len {
                    return Some((
                        format!("value too long for type character({max_len})"),
                        "22001".to_string(),
                    ));
                }
            }
            None
        }
        "uuid" => {
            let hex: String = trimmed.chars().filter(|c| *c != '-').collect();
            if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                Some((
                    format!("invalid input syntax for type uuid: \"{input}\""),
                    "22P02".to_string(),
                ))
            } else {
                None
            }
        }
        "date" => {
            // Accept YYYY-MM-DD
            let parts: Vec<&str> = trimmed.split('-').collect();
            if parts.len() == 3
                && parts[0].parse::<i32>().is_ok()
                && parts[1].parse::<u32>().is_ok_and(|m| (1..=12).contains(&m))
                && parts[2].parse::<u32>().is_ok_and(|d| (1..=31).contains(&d))
            {
                None
            } else {
                Some((
                    format!("invalid input syntax for type date: \"{input}\""),
                    "22007".to_string(),
                ))
            }
        }
        "timestamp" | "timestamp without time zone" => {
            // Accept YYYY-MM-DD HH:MM:SS[.fff]
            if parse_timestamp_prefix(trimmed).is_some() {
                None
            } else {
                Some((
                    format!("invalid input syntax for type timestamp: \"{input}\""),
                    "22007".to_string(),
                ))
            }
        }
        "timestamptz" | "timestamp with time zone" => {
            // Accept YYYY-MM-DD HH:MM:SS[.fff][+/-HH[:MM]]
            let (base, _tz) = match trimmed.rfind('+') {
                Some(pos) if pos > 10 => (&trimmed[..pos], Some(&trimmed[pos..])),
                _ => match trimmed.rfind('-') {
                    Some(pos) if pos > 10 => (&trimmed[..pos], Some(&trimmed[pos..])),
                    _ => (trimmed, None),
                },
            };
            if parse_timestamp_prefix(base).is_some() {
                None
            } else {
                Some((
                    format!("invalid input syntax for type timestamp with time zone: \"{input}\""),
                    "22007".to_string(),
                ))
            }
        }
        "time" | "time without time zone" => {
            // Accept HH:MM:SS[.fff]
            let time_part = trimmed.split('.').next().unwrap_or(trimmed);
            let parts: Vec<&str> = time_part.split(':').collect();
            if parts.len() >= 2
                && parts.len() <= 3
                && parts[0].parse::<u32>().is_ok_and(|h| (0..=23).contains(&h))
                && parts[1].parse::<u32>().is_ok_and(|m| (0..=59).contains(&m))
                && (parts.len() < 3 || parts[2].parse::<u32>().is_ok_and(|s| (0..=59).contains(&s)))
            {
                None
            } else {
                Some((
                    format!("invalid input syntax for type time: \"{input}\""),
                    "22007".to_string(),
                ))
            }
        }
        "json" | "jsonb" => {
            if is_valid_json(trimmed) {
                None
            } else {
                let type_label = if base_type == "jsonb" {
                    "jsonb"
                } else {
                    "json"
                };
                Some((
                    format!("invalid input syntax for type {type_label}: \"{input}\""),
                    "22P02".to_string(),
                ))
            }
        }
        "bytea" => {
            // Accept \x hex format
            if let Some(hex) = trimmed.strip_prefix("\\x") {
                if hex.len() % 2 == 0 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
                    None
                } else {
                    Some((
                        format!("invalid input syntax for type bytea: \"{input}\""),
                        "22P02".to_string(),
                    ))
                }
            } else {
                // Plain text is valid bytea input (escape format)
                None
            }
        }
        "inet" | "cidr" => {
            // Accept IP address with optional /prefix
            let addr_part = trimmed.split('/').next().unwrap_or(trimmed);
            if addr_part.parse::<std::net::IpAddr>().is_ok() {
                // If there's a prefix, validate it
                if let Some(prefix_str) = trimmed.split('/').nth(1) {
                    if prefix_str.parse::<u8>().is_err() {
                        return Some((
                            format!("invalid input syntax for type {base_type}: \"{input}\""),
                            "22P02".to_string(),
                        ));
                    }
                }
                None
            } else {
                Some((
                    format!("invalid input syntax for type {base_type}: \"{input}\""),
                    "22P02".to_string(),
                ))
            }
        }
        "macaddr" => {
            let parts: Vec<&str> = trimmed.split(':').collect();
            if parts.len() == 6
                && parts
                    .iter()
                    .all(|p| p.len() == 2 && p.chars().all(|c| c.is_ascii_hexdigit()))
            {
                None
            } else {
                Some((
                    format!("invalid input syntax for type macaddr: \"{input}\""),
                    "22P02".to_string(),
                ))
            }
        }
        "oid" => {
            if trimmed.parse::<u32>().is_ok() {
                None
            } else {
                Some((
                    format!("invalid input syntax for type oid: \"{input}\""),
                    "22P02".to_string(),
                ))
            }
        }
        _ => Some((
            format!("type \"{type_name}\" does not exist"),
            "42704".to_string(),
        )),
    }
}

/// Parse a YYYY-MM-DD HH:MM:SS[.fff] prefix, returning Some(()) if valid.
fn parse_timestamp_prefix(s: &str) -> Option<()> {
    let parts: Vec<&str> = s.splitn(2, [' ', 'T']).collect();
    if parts.len() != 2 {
        return None;
    }
    // Validate date part
    let date_parts: Vec<&str> = parts[0].split('-').collect();
    if date_parts.len() != 3
        || date_parts[0].parse::<i32>().is_err()
        || !date_parts[1]
            .parse::<u32>()
            .is_ok_and(|m| (1..=12).contains(&m))
        || !date_parts[2]
            .parse::<u32>()
            .is_ok_and(|d| (1..=31).contains(&d))
    {
        return None;
    }
    // Validate time part (strip fractional seconds)
    let time_str = parts[1].split('.').next().unwrap_or(parts[1]);
    let time_parts: Vec<&str> = time_str.split(':').collect();
    if time_parts.len() < 2
        || time_parts.len() > 3
        || !time_parts[0]
            .parse::<u32>()
            .is_ok_and(|h| (0..=23).contains(&h))
        || !time_parts[1]
            .parse::<u32>()
            .is_ok_and(|m| (0..=59).contains(&m))
    {
        return None;
    }
    if time_parts.len() == 3
        && !time_parts[2]
            .parse::<u32>()
            .is_ok_and(|s| (0..=59).contains(&s))
    {
        return None;
    }
    Some(())
}

/// Minimal JSON validation without requiring serde_json.
fn is_valid_json(s: &str) -> bool {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return false;
    }
    // Quick structural check: must start with {, [, ", digit, true, false, or null
    let first = trimmed.as_bytes()[0];
    match first {
        b'{' => trimmed.ends_with('}') && validate_json_braces(trimmed),
        b'[' => trimmed.ends_with(']') && validate_json_braces(trimmed),
        b'"' => trimmed.len() >= 2 && trimmed.ends_with('"'),
        b't' => trimmed == "true",
        b'f' => trimmed == "false",
        b'n' => trimmed == "null",
        b'0'..=b'9' | b'-' => trimmed.parse::<f64>().is_ok(),
        _ => false,
    }
}

/// Check that braces/brackets are balanced in a JSON string.
fn validate_json_braces(s: &str) -> bool {
    let mut stack = Vec::new();
    let mut in_string = false;
    let mut escape = false;

    for ch in s.chars() {
        if escape {
            escape = false;
            continue;
        }
        if ch == '\\' && in_string {
            escape = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        match ch {
            '{' => stack.push('}'),
            '[' => stack.push(']'),
            '}' | ']' => {
                if stack.pop() != Some(ch) {
                    return false;
                }
            }
            _ => {}
        }
    }
    stack.is_empty() && !in_string
}

/// Format a number using PG's to_char numeric format patterns.
fn pg_to_char_numeric(num: f64, format: &str) -> String {
    let is_negative = num < 0.0;
    let abs_num = num.abs();

    // Parse format string for flags
    let upper_fmt = format.to_uppercase();
    let fm = upper_fmt.contains("FM"); // fill mode (suppress padding)
    let has_pr = upper_fmt.contains("PR"); // angle brackets for negative
    let has_s = upper_fmt.contains('S'); // sign
    let has_mi = upper_fmt.starts_with("MI") || upper_fmt.ends_with("MI");

    // Count digit positions
    let mut integer_digits = 0;
    let mut decimal_digits = 0;
    let mut leading_zeros = 0;
    let mut seen_dot = false;

    for ch in upper_fmt.chars() {
        match ch {
            '9' => {
                if seen_dot {
                    decimal_digits += 1;
                } else {
                    integer_digits += 1;
                }
            }
            '0' => {
                if seen_dot {
                    decimal_digits += 1;
                } else {
                    integer_digits += 1;
                    leading_zeros += 1;
                }
            }
            'D' | '.' => seen_dot = true,
            _ => {}
        }
    }

    if integer_digits == 0 && decimal_digits == 0 {
        return format!("{num}");
    }

    // Format the number
    let formatted = if decimal_digits > 0 {
        let prec = decimal_digits;
        format!("{abs_num:.prec$}")
    } else {
        let int_val = abs_num as i64;
        format!("{int_val}")
    };

    // Split into integer and decimal parts
    let parts: Vec<&str> = formatted.split('.').collect();
    let int_part = parts[0];
    let dec_part = if parts.len() > 1 { parts[1] } else { "" };

    // Pad integer part
    let padded_int = if !fm {
        let width = integer_digits.max(int_part.len());
        if leading_zeros > 0 {
            format!("{int_part:0>width$}")
        } else {
            format!("{int_part:>width$}")
        }
    } else {
        int_part.to_string()
    };

    // Build result
    let mut result = if decimal_digits > 0 {
        format!("{padded_int}.{dec_part}")
    } else {
        padded_int
    };

    // Add sign
    if has_pr {
        result = if is_negative {
            format!("<{result}>")
        } else {
            format!(" {result} ")
        };
    } else if has_s {
        let sign_pos = upper_fmt.find('S').unwrap_or(0);
        let sign = if is_negative { "-" } else { "+" };
        if sign_pos == 0 {
            result = format!("{sign}{result}");
        } else {
            result = format!("{result}{sign}");
        }
    } else if has_mi {
        if is_negative {
            result = format!("{result}-");
        } else {
            result = format!("{result} ");
        }
    } else if is_negative {
        result = format!("-{result}");
    } else {
        result = format!(" {result}");
    }

    result
}
