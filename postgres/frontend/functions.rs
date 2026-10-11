use chrono::Utc;
use std::sync::Arc;
use turso_core::native_ext::{FunctionArity, ScalarCall, ScalarFunction};
use turso_core::types::IOResultOr;
use turso_core::{
    Connection, ExternalFunc, IOResult, LimboError, OpenOptions, Register, Result, Value,
};

pub(crate) fn register_functions(mut options: OpenOptions) -> OpenOptions {
    for function in SCALAR_FUNCTIONS {
        options = options.extension_function(
            ExternalFunc::new_native_scalar(
                function.name().to_string(),
                function.arity(),
                function.is_deterministic(),
                *function,
            )
            .expect("PostgreSQL function declarations have valid argument counts"),
        );
    }
    options
}

macro_rules! scalar_functions {
    ($($($variant:ident)|+($arity:expr, $deterministic:expr)),* $(,)?) => {
        #[derive(Debug, Clone, Copy, strum::AsRefStr)]
        #[strum(serialize_all = "snake_case")]
        enum PgScalarFunction {
            $($($variant,)+)*
        }

        const SCALAR_FUNCTIONS: &[PgScalarFunction] = &[$($(PgScalarFunction::$variant,)+)*];

        impl PgScalarFunction {
            fn name(&self) -> &str {
                self.as_ref()
            }

            fn arity(&self) -> FunctionArity {
                match self {
                    $($(Self::$variant)|+ => $arity,)*
                }
            }

            fn is_deterministic(&self) -> bool {
                match self {
                    $($(Self::$variant)|+ => $deterministic,)*
                }
            }
        }
    };
}

scalar_functions! {
    PgGetUserbyid(FunctionArity::Exact(1), true),
    PgTableIsVisible | PgFunctionIsVisible | PgTypeIsVisible(FunctionArity::Exact(1), true),
    PgEncodingToChar(FunctionArity::Exact(1), true),
    PgGetFunctionResult | PgGetFunctionArguments | PgGetStatisticsobjdefColumns | PgRelationIsPublishable(
        FunctionArity::Exact(1), true
    ),
    QuoteIdent(FunctionArity::Exact(1), true),
    QuoteLiteral(FunctionArity::Exact(1), true),
    FormatType(FunctionArity::OneOf(&[1, 2]), true),
    PgGetConstraintdef(FunctionArity::OneOf(&[1, 2]), false),
    PgGetIndexdef(FunctionArity::OneOf(&[1, 2]), false),
    ObjDescription(FunctionArity::OneOf(&[1, 2]), true),
    PgGetExpr(FunctionArity::OneOf(&[2, 3]), true),
    ToChar(FunctionArity::Exact(2), true),
    PgInputIsValid(FunctionArity::Exact(2), true),
    Booleq(FunctionArity::Exact(2), true),
    Boolne(FunctionArity::Exact(2), true),
    ColDescription(FunctionArity::Exact(2), true),
    Version(FunctionArity::Exact(0), true),
    CurrentDatabase(FunctionArity::Exact(0), false),
    CurrentSchema(FunctionArity::Exact(0), true),
    PgBackendPid(FunctionArity::Exact(0), true),
    Now | ClockTimestamp | TransactionTimestamp | StatementTimestamp(FunctionArity::Variadic, false),
}

impl ScalarFunction for PgScalarFunction {
    type Call = Self;

    fn create_call(&self) -> Result<Self::Call> {
        Ok(*self)
    }
}

impl ScalarCall for PgScalarFunction {
    fn step(&mut self, connection: &Arc<Connection>, args: &[Register]) -> IOResultOr<Value> {
        let value = match self {
            Self::PgGetUserbyid => exec_pg_get_user_by_id(int_arg(args, 0, 0)),
            Self::PgTableIsVisible | Self::PgFunctionIsVisible | Self::PgTypeIsVisible => {
                exec_pg_is_visible(int_arg(args, 0, 0))
            }
            Self::PgEncodingToChar => exec_pg_encoding_to_char(int_arg(args, 0, 0)),
            Self::PgGetFunctionResult
            | Self::PgGetFunctionArguments
            | Self::PgGetStatisticsobjdefColumns
            | Self::PgRelationIsPublishable
            | Self::ObjDescription
            | Self::ColDescription => Value::Null,
            Self::QuoteIdent => match args[0].get_value() {
                Value::Null => Value::Null,
                _ => Value::build_text(turso_pg_parser::quote_identifier(&text_arg(args, 0))),
            },
            Self::QuoteLiteral => exec_quote_literal(args[0].get_value()),
            Self::FormatType => exec_pg_format_type(int_arg(args, 0, 0), int_arg(args, 1, -1)),
            Self::PgGetConstraintdef => exec_pg_get_constraintdef(connection, int_arg(args, 0, 0)),
            Self::PgGetIndexdef => exec_pg_get_indexdef(connection, int_arg(args, 0, 0)),
            Self::PgGetExpr => exec_pg_get_expr(args)?,
            Self::ToChar => exec_to_char(args[0].get_value(), &text_arg(args, 1)),
            Self::PgInputIsValid => exec_pg_input_is_valid(args[0].get_value(), &text_arg(args, 1)),
            Self::Booleq => Value::from_i64((args[0].get_value() == args[1].get_value()) as i64),
            Self::Boolne => Value::from_i64((args[0].get_value() != args[1].get_value()) as i64),
            Self::Version => exec_version(),
            Self::CurrentDatabase => {
                Value::build_text(crate::catalog::db_name_from_path(connection.db_file_path()))
            }
            Self::CurrentSchema => Value::build_text("public"),
            Self::PgBackendPid => Value::from_i64(std::process::id() as i64),
            Self::Now
            | Self::ClockTimestamp
            | Self::TransactionTimestamp
            | Self::StatementTimestamp => {
                Value::build_text(Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string())
            }
        };
        Ok(IOResult::Done(value))
    }
}

fn int_arg(args: &[Register], i: usize, default: i64) -> i64 {
    args.get(i)
        .and_then(|arg| arg.get_value().as_int())
        .unwrap_or(default)
}

fn text_arg(args: &[Register], i: usize) -> String {
    match args.get(i).map(Register::get_value) {
        Some(Value::Text(t)) => t.as_str().to_string(),
        _ => String::new(),
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

fn exec_pg_encoding_to_char(encoding: i64) -> Value {
    let name = match encoding {
        6 => "UTF8",
        0 => "SQL_ASCII",
        _ => "UTF8",
    };
    Value::build_text(name)
}

fn exec_pg_get_constraintdef(conn: &Connection, oid: i64) -> Value {
    match crate::catalog::pg_get_constraintdef(conn, oid) {
        Some(s) => Value::build_text(s),
        None => Value::Null,
    }
}

fn exec_pg_get_indexdef(conn: &Connection, oid: i64) -> Value {
    match crate::catalog::pg_get_indexdef(conn, oid) {
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

fn exec_pg_get_expr(args: &[Register]) -> Result<Value> {
    match args.first().map(Register::get_value) {
        Some(Value::Text(expression)) => {
            if args[1..]
                .iter()
                .any(|arg| matches!(arg.get_value(), Value::Null))
            {
                return Ok(Value::Null);
            }

            // TursoPG stores adbin and conbin as rendered SQL, so the relation OID and pretty flag do not change the output.
            Ok(Value::Text(expression.clone()))
        }
        Some(Value::Null) => Ok(Value::Null),
        Some(_) | None => Err(LimboError::ConversionError(
            "Expected text value".to_string(),
        )),
    }
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
