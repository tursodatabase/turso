use crate::catalog::PG_DATABASE_OWNER_OID;
use crate::error::sqlstate_error;
use crate::settings::{find_parameter, Parameter, SettingScope};
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Timelike, Utc};
use std::{fmt, sync::Arc};
use turso_core::native_ext::{FunctionArity, ReturnType, ScalarCall, ScalarFunction, ScalarReturn};
use turso_core::types::IOResultOr;
use turso_core::{
    Connection, ExternalFunc, IOResult, LimboError, Numeric, OpenOptions, Register, Result, Value,
};
use turso_pg_parser::sqlstate::{
    SQLSTATE_DIVISION_BY_ZERO, SQLSTATE_INTERNAL_ERROR, SQLSTATE_INVALID_ARGUMENT_FOR_LOG,
    SQLSTATE_INVALID_ARGUMENT_FOR_POWER_FUNCTION, SQLSTATE_INVALID_ESCAPE_SEQUENCE,
    SQLSTATE_INVALID_PARAMETER_VALUE, SQLSTATE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE,
    SQLSTATE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE, SQLSTATE_INVALID_TEXT_REPRESENTATION,
    SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE, SQLSTATE_PROGRAM_LIMIT_EXCEEDED,
    SQLSTATE_UNDEFINED_FUNCTION, SQLSTATE_UNDEFINED_OBJECT,
};

/// Registers one native scalar function per name. Each body is a Rust
/// expression that can use `?`. The planner reads the SQL return type from the
/// Rust type of the body (see `ScalarReturn`), so it cannot drift from what
/// the body returns.
macro_rules! scalar_functions {
    ($options:expr, |$connection:ident, $args:ident| {
        $($($name:literal)|+ ($arity:expr, $deterministic:expr) => $body:expr,)*
    }) => {{
        let mut options = $options;
        $($(
            #[allow(unused_variables)]
            let step = |$connection: &Arc<Connection>, $args: &[Register]| -> Result<_> {
                Ok($body)
            };
            options = options.extension_function(pg_scalar_function(
                $name,
                $arity,
                $deterministic,
                step,
            ));
        )+)*
        options
    }};
}

pub(crate) fn register_functions(options: OpenOptions) -> OpenOptions {
    scalar_functions!(options, |connection, args| {
        "acldefault"(FunctionArity::Exact(2), true) => exec_acl_default(args)?,
        "pg_get_userbyid"(FunctionArity::Exact(1), true) => {
            oid_arg(args, 0)?.map(exec_pg_get_user_by_id)
        },
        "pg_table_is_visible" | "pg_function_is_visible" | "pg_type_is_visible"(
            FunctionArity::Exact(1), true
        ) => oid_arg(args, 0)?.map(exec_pg_is_visible),
        "pg_encoding_to_char"(FunctionArity::Exact(1), true) => {
            exec_pg_encoding_to_char(int_arg(args, 0, 0))
        },
        "pg_get_function_result"
        | "pg_get_function_arguments"
        | "pg_get_statisticsobjdef_columns"(FunctionArity::Exact(1), true) => None::<String>,
        "pg_relation_is_publishable"(FunctionArity::Exact(1), true) => None::<Boolean>,
        "quote_ident"(FunctionArity::Exact(1), true) => match args[0].get_value() {
            Value::Null => None,
            _ => Some(turso_pg_parser::quote_identifier(&text_arg(args, 0))),
        },
        "quote_literal"(FunctionArity::Exact(1), true) => {
            exec_quote_literal(args[0].get_value())
        },
        "format_type"(FunctionArity::OneOf(&[1, 2]), true) => oid_arg(args, 0)?
            .map(|type_oid| exec_pg_format_type(type_oid, int_arg(args, 1, -1))),
        "pg_get_constraintdef"(FunctionArity::OneOf(&[1, 2]), false) => oid_arg(args, 0)?
            .and_then(|oid| crate::catalog::pg_get_constraintdef(connection, oid)),
        "pg_get_indexdef"(FunctionArity::OneOf(&[1, 2]), false) => oid_arg(args, 0)?
            .and_then(|oid| crate::catalog::pg_get_indexdef(connection, oid)),
        "pg_get_triggerdef"(FunctionArity::OneOf(&[1, 2]), false) => None::<String>,
        "obj_description"(FunctionArity::OneOf(&[1, 2]), true) => None::<String>,
        "pg_get_expr"(FunctionArity::OneOf(&[2, 3]), true) => exec_pg_get_expr(args)?,
        "to_char"(FunctionArity::Exact(2), true) => {
            exec_to_char(args[0].get_value(), &text_arg(args, 1))
        },
        "age"(FunctionArity::OneOf(&[1, 2]), false) => exec_age(args)?,
        "pg_input_is_valid"(FunctionArity::Exact(2), true) => {
            exec_pg_input_is_valid(args[0].get_value(), &text_arg(args, 1))
        },
        "pg_cast_input"(FunctionArity::Exact(2), true) => {
            exec_pg_cast_input(args[0].get_value(), &text_arg(args, 1))?
        },
        "ascii"(FunctionArity::Exact(1), true) => match args[0].get_value() {
            Value::Null => None::<i64>,
            _ => Some(
                text_arg(args, 0)
                    .chars()
                    .next()
                    .map_or(0, |c| i64::from(u32::from(c))),
            ),
        },
        "textlike"(FunctionArity::Exact(2), true) => exec_textlike(args)?,
        "like_escape"(FunctionArity::Exact(2), true) => exec_like_escape(args)?,
        "similar_to_escape"(FunctionArity::OneOf(&[1, 2]), true) => exec_similar_to_escape(args)?,
        "booleq"(FunctionArity::Exact(2), true) => Boolean(args[0].get_value() == args[1].get_value()),
        "boolne"(FunctionArity::Exact(2), true) => Boolean(args[0].get_value() != args[1].get_value()),
        "col_description"(FunctionArity::Exact(2), true) => None::<String>,
        "set_config"(FunctionArity::Exact(3), false) => exec_set_config(connection, args)?,
        "current_setting"(FunctionArity::OneOf(&[1, 2]), false) => {
            exec_current_setting(connection, args)?
        },
        "version"(FunctionArity::Exact(0), true) => exec_version(),
        "current_database"(FunctionArity::Exact(0), false) => {
            crate::catalog::db_name_from_path(connection.db_file_path())
        },
        "current_schema"(FunctionArity::Exact(0), true) => {
            crate::session::current_schema(connection)
        },
        "current_schemas"(FunctionArity::Exact(1), false) => {
            exec_current_schemas(connection, args[0].get_value())?
        },
        "pg_backend_pid"(FunctionArity::Exact(0), true) => std::process::id() as i64,
        // Temporary pg_dump compatibility: "t" means true, not actual recovery state.
        "pg_is_in_recovery"(FunctionArity::Exact(0), false) => "t",
        "pg_lock_access_share"(FunctionArity::Exact(0), false) => {
            if connection.get_auto_commit() {
                return Err(LimboError::InvalidArgument(
                    "LOCK TABLE can only be used in transaction blocks".to_string(),
                ));
            }
            0i64
        },
        "pg_limit_value"(FunctionArity::Exact(2), true) => exec_pg_limit_value(args)?,
        // A double precision number in [0, 1), unlike SQLite's random integer.
        "random"(FunctionArity::Exact(0), false) => {
            exec_random(connection.get_pager().io.generate_random_number())
        },
        "now" | "clock_timestamp" | "transaction_timestamp" | "statement_timestamp"(
            FunctionArity::Variadic, false
        ) => Timestamptz(Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()),
    })
}

pub(crate) fn pg_scalar_function<O: ScalarReturn + 'static>(
    name: &'static str,
    arity: FunctionArity,
    deterministic: bool,
    step: fn(&Arc<Connection>, &[Register]) -> Result<O>,
) -> ExternalFunc {
    ExternalFunc::new_native_scalar(
        name.to_string(),
        arity,
        deterministic,
        PgScalarFunction { name, step },
    )
    .expect("PostgreSQL function declarations have valid argument counts")
}

/// A PostgreSQL function that computes its result without I/O.
struct PgScalarFunction<O> {
    name: &'static str,
    step: fn(&Arc<Connection>, &[Register]) -> Result<O>,
}

impl<O> fmt::Debug for PgScalarFunction<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PgScalarFunction").field(&self.name).finish()
    }
}

impl<O: ScalarReturn + 'static> ScalarFunction for PgScalarFunction<O> {
    type Call = Self;

    fn create_call(&self) -> Result<Self::Call> {
        Ok(Self {
            name: self.name,
            step: self.step,
        })
    }
}

impl<O: ScalarReturn + 'static> ScalarCall for PgScalarFunction<O> {
    type Output = O;

    fn step(&mut self, connection: &Arc<Connection>, args: &[Register]) -> IOResultOr<O> {
        Ok(IOResult::Done((self.step)(connection, args)?))
    }
}

/// A `timestamptz` value in its text form, such as `2000-01-01 00:00:00.5`.
/// Returning one gives the function the type `timestamptz`, so `now() + 1` is
/// an error as in PostgreSQL instead of adding 1 to text.
struct Timestamptz(String);

impl ScalarReturn for Timestamptz {
    const TYPE: Option<ReturnType> = Some(ReturnType::Custom("timestamptz"));

    fn into_value(self) -> Value {
        Value::build_text(self.0)
    }
}

/// A `boolean` value. Its stored form is the integer 0 or 1, which is also the
/// form the type's DECODE gives, so a function can return it directly. Returning
/// one gives the function the type `boolean`, as in PostgreSQL, instead of
/// integer.
pub(crate) struct Boolean(pub(crate) bool);

impl ScalarReturn for Boolean {
    const TYPE: Option<ReturnType> = Some(ReturnType::Custom("boolean"));

    fn into_value(self) -> Value {
        Value::from_i64(self.0 as i64)
    }
}

/// A `uuid` value in its text form, such as
/// `a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11`.
pub(crate) struct Uuid(pub(crate) String);

impl ScalarReturn for Uuid {
    const TYPE: Option<ReturnType> = Some(ReturnType::Custom("uuid"));

    fn into_value(self) -> Value {
        Value::build_text(self.0)
    }
}

fn exec_current_schemas(conn: &Connection, argument: &Value) -> Result<Option<String>> {
    let include_implicit = match argument {
        Value::Null => return Ok(None),
        Value::Text(text) => parse_pg_bool(text.as_str()).ok_or_else(|| {
            sqlstate_error(
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                format!(
                    "invalid input syntax for type boolean: \"{}\"",
                    text.as_str()
                ),
            )
        })?,
        value => match value.as_int() {
            Some(0) => false,
            Some(1) => true,
            _ => {
                return Err(undefined_function(
                    "current_schemas",
                    &[pg_type_name(value)],
                ))
            }
        },
    };
    let schemas = crate::session::current_schemas(conn, include_implicit);
    let mut result = String::from("{");
    for (i, schema) in schemas.iter().enumerate() {
        if i > 0 {
            result.push(',');
        }
        let quoted = schema.is_empty()
            || schema.eq_ignore_ascii_case("null")
            || schema
                .chars()
                .any(|c| c.is_ascii_whitespace() || matches!(c, ',' | '{' | '}' | '"' | '\\'));
        if quoted {
            result.push('"');
        }
        for c in schema.chars() {
            if matches!(c, '"' | '\\') {
                result.push('\\');
            }
            result.push(c);
        }
        if quoted {
            result.push('"');
        }
    }
    result.push('}');
    Ok(Some(result))
}

fn exec_acl_default(args: &[Register]) -> Result<Option<String>> {
    if matches!(args[0].get_value(), Value::Null) || matches!(args[1].get_value(), Value::Null) {
        return Ok(None);
    }

    let kind_code = match args[0].get_value() {
        Value::Text(value) => value.as_str(),
        _ => "",
    };
    let owner = args[1].get_value().as_int().ok_or_else(|| {
        LimboError::InvalidArgument("acldefault owner must be an OID".to_string())
    })?;
    if !(0..=u32::MAX as i64).contains(&owner) {
        return Err(LimboError::InvalidArgument(
            "acldefault owner must be an OID".to_string(),
        ));
    }
    let kind = kind_code
        .get(..1)
        .unwrap_or("")
        .parse::<AclObjectKind>()
        // PostgreSQL raises this error with elog(), which gives SQLSTATE XX000.
        .map_err(|_| {
            sqlstate_error(
                SQLSTATE_INTERNAL_ERROR,
                format!("unrecognized object type abbreviation: {kind_code}"),
            )
        })?;
    let (public_privileges, owner_privileges) = kind.default_privileges();

    // TODO: This hardcoded role mapping is temporary pg_dump compatibility.
    // Replace it with a real role OID-to-name lookup later. Keep the numeric fallback for unknown OIDs.
    let role = if owner == 10 {
        "turso".to_string()
    } else if owner == PG_DATABASE_OWNER_OID {
        "pg_database_owner".to_string()
    } else {
        owner.to_string()
    };
    let mut entries = Vec::with_capacity(2);
    if !public_privileges.is_empty() {
        entries.push(format!("={public_privileges}/{role}"));
    }
    if !owner_privileges.is_empty() {
        let grantee = if owner == 0 { "" } else { &role };
        entries.push(format!("{grantee}={owner_privileges}/{role}"));
    }
    Ok(Some(format!("{{{}}}", entries.join(","))))
}

#[derive(strum::EnumString)]
enum AclObjectKind {
    #[strum(serialize = "c")]
    Column,
    #[strum(serialize = "r")]
    Table,
    #[strum(serialize = "s")]
    Sequence,
    #[strum(serialize = "d")]
    Database,
    #[strum(serialize = "f")]
    Function,
    #[strum(serialize = "l")]
    Language,
    #[strum(serialize = "L")]
    LargeObject,
    #[strum(serialize = "n")]
    Schema,
    #[strum(serialize = "p")]
    Parameter,
    #[strum(serialize = "t")]
    Tablespace,
    #[strum(serialize = "F")]
    ForeignDataWrapper,
    #[strum(serialize = "S")]
    ForeignServer,
    #[strum(serialize = "T")]
    Type,
}

impl AclObjectKind {
    fn default_privileges(&self) -> (AclPrivileges, AclPrivileges) {
        let no_privileges = AclPrivileges::empty();
        match self {
            Self::Column => (no_privileges, no_privileges),
            Self::Table => (
                no_privileges,
                AclPrivileges::INSERT
                    | AclPrivileges::SELECT
                    | AclPrivileges::UPDATE
                    | AclPrivileges::DELETE
                    | AclPrivileges::TRUNCATE
                    | AclPrivileges::REFERENCES
                    | AclPrivileges::TRIGGER,
            ),
            Self::Sequence => (
                no_privileges,
                AclPrivileges::SELECT | AclPrivileges::UPDATE | AclPrivileges::USAGE,
            ),
            Self::Database => (
                AclPrivileges::TEMPORARY | AclPrivileges::CONNECT,
                AclPrivileges::CREATE | AclPrivileges::TEMPORARY | AclPrivileges::CONNECT,
            ),
            Self::Function => (AclPrivileges::EXECUTE, AclPrivileges::EXECUTE),
            Self::Language => (AclPrivileges::USAGE, AclPrivileges::USAGE),
            Self::LargeObject => (no_privileges, AclPrivileges::SELECT | AclPrivileges::UPDATE),
            Self::Schema => (no_privileges, AclPrivileges::USAGE | AclPrivileges::CREATE),
            Self::Parameter => (
                no_privileges,
                AclPrivileges::SET | AclPrivileges::ALTER_SYSTEM,
            ),
            Self::Tablespace => (no_privileges, AclPrivileges::CREATE),
            Self::ForeignDataWrapper | Self::ForeignServer => (no_privileges, AclPrivileges::USAGE),
            Self::Type => (AclPrivileges::USAGE, AclPrivileges::USAGE),
        }
    }
}

bitflags::bitflags! {
    #[derive(Clone, Copy)]
    struct AclPrivileges: u16 {
        const INSERT = 1 << 0;
        const SELECT = 1 << 1;
        const UPDATE = 1 << 2;
        const DELETE = 1 << 3;
        const TRUNCATE = 1 << 4;
        const REFERENCES = 1 << 5;
        const TRIGGER = 1 << 6;
        const EXECUTE = 1 << 7;
        const USAGE = 1 << 8;
        const CREATE = 1 << 9;
        const TEMPORARY = 1 << 10;
        const CONNECT = 1 << 11;
        const SET = 1 << 12;
        const ALTER_SYSTEM = 1 << 13;
    }
}

impl fmt::Display for AclPrivileges {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (privilege, letter) in [
            (Self::INSERT, "a"),
            (Self::SELECT, "r"),
            (Self::UPDATE, "w"),
            (Self::DELETE, "d"),
            (Self::TRUNCATE, "D"),
            (Self::REFERENCES, "x"),
            (Self::TRIGGER, "t"),
            (Self::EXECUTE, "X"),
            (Self::USAGE, "U"),
            (Self::CREATE, "C"),
            (Self::TEMPORARY, "T"),
            (Self::CONNECT, "c"),
            (Self::SET, "s"),
            (Self::ALTER_SYSTEM, "A"),
        ] {
            if self.contains(privilege) {
                f.write_str(letter)?;
            }
        }
        Ok(())
    }
}

fn int_arg(args: &[Register], i: usize, default: i64) -> i64 {
    args.get(i)
        .and_then(|arg| arg.get_value().as_int())
        .unwrap_or(default)
}

fn oid_arg(args: &[Register], i: usize) -> Result<Option<i64>> {
    let value = args[i].get_value();
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => parse_oid(text.as_str()).map(Some),
        value => value.as_int().map(Some).ok_or_else(|| {
            LimboError::ConversionError(format!("invalid input syntax for type oid: \"{value}\""))
        }),
    }
}

fn parse_oid(text: &str) -> Result<i64> {
    let invalid =
        || LimboError::ConversionError(format!("invalid input syntax for type oid: \"{text}\""));
    let out_of_range =
        || LimboError::ConversionError(format!("value \"{text}\" is out of range for type oid"));
    let is_space = |c: char| matches!(c, ' ' | '\t' | '\n' | '\x0b' | '\x0c' | '\r');
    let number = text.trim_matches(is_space);
    let (negative, unsigned) = match number.as_bytes().first() {
        Some(b'-') => (true, &number[1..]),
        Some(b'+') => (false, &number[1..]),
        _ => (false, number),
    };
    let (radix, digits) = if let Some(hex) = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
        .filter(|hex| hex.starts_with(|c: char| c.is_ascii_hexdigit()))
    {
        (16, hex)
    } else if unsigned.len() > 1 && unsigned.starts_with('0') {
        (8, &unsigned[1..])
    } else {
        (10, unsigned)
    };
    if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
        return Err(invalid());
    }
    let magnitude = u64::from_str_radix(digits, radix).map_err(|_| out_of_range())?;
    let value = if negative {
        magnitude.wrapping_neg()
    } else {
        magnitude
    };
    let oid = value as u32;
    if value != u64::from(oid) && value != oid as i32 as i64 as u64 {
        return Err(out_of_range());
    }
    Ok(i64::from(oid))
}

fn text_arg(args: &[Register], i: usize) -> String {
    match args.get(i).map(Register::get_value) {
        Some(Value::Text(t)) => t.as_str().to_string(),
        _ => String::new(),
    }
}

fn exec_set_config(conn: &Connection, args: &[Register]) -> Result<String> {
    let name = match args[0].get_value() {
        Value::Text(name) => name.as_str(),
        Value::Null => {
            return Err(LimboError::InvalidArgument(
                "SET requires parameter name".to_string(),
            ));
        }
        _ => {
            return Err(LimboError::InvalidArgument(
                "set_config requires a text parameter name".to_string(),
            ));
        }
    };
    let parameter = find_parameter(name).ok_or_else(|| unrecognized_parameter(name))?;
    let value = match args[1].get_value() {
        Value::Text(value) => Some(value.as_str()),
        Value::Null => None,
        _ => {
            return Err(LimboError::InvalidArgument(
                "set_config requires a text value".to_string(),
            ));
        }
    };
    let scope = match args[2].get_value() {
        Value::Null => SettingScope::Session,
        value => match value.as_int() {
            Some(0) => SettingScope::Session,
            Some(1) => SettingScope::Transaction,
            _ => {
                return Err(LimboError::InvalidArgument(
                    "set_config requires a boolean".to_string(),
                ));
            }
        },
    };
    match parameter {
        Parameter::BuiltIn(parameter) => parameter.set(conn, value, scope),
        Parameter::Custom(name) => crate::session::set_custom_parameter(conn, name, value, scope),
    }
}

fn exec_current_setting(conn: &Connection, args: &[Register]) -> Result<Option<String>> {
    if args
        .iter()
        .any(|arg| matches!(arg.get_value(), Value::Null))
    {
        return Ok(None);
    }
    let Value::Text(name) = args[0].get_value() else {
        let mut arg_types = vec![pg_type_name(args[0].get_value())];
        arg_types.extend(args[1..].iter().map(|_| "boolean"));
        return Err(undefined_function("current_setting", &arg_types));
    };
    let name = name.as_str();
    let missing_ok = match args.get(1).map(Register::get_value) {
        None => false,
        Some(Value::Text(text)) => parse_pg_bool(text.as_str()).ok_or_else(|| {
            sqlstate_error(
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                format!(
                    "invalid input syntax for type boolean: \"{}\"",
                    text.as_str()
                ),
            )
        })?,
        Some(value) => match value.as_int() {
            Some(0) => false,
            Some(1) => true,
            _ => {
                return Err(LimboError::InvalidArgument(
                    "current_setting requires a boolean".to_string(),
                ));
            }
        },
    };
    let setting = match find_parameter(name) {
        Some(Parameter::BuiltIn(parameter)) => Some(parameter.get(conn)),
        Some(Parameter::Custom(name)) => crate::session::custom_parameter(conn, name),
        None => None,
    };
    match setting {
        Some(setting) => Ok(Some(setting)),
        None if missing_ok => Ok(None),
        None => Err(unrecognized_parameter(name)),
    }
}

fn unrecognized_parameter(name: &str) -> LimboError {
    sqlstate_error(
        SQLSTATE_UNDEFINED_OBJECT,
        format!("unrecognized configuration parameter \"{name}\""),
    )
}

/// Turns 64 random bits into a number in [0, 1). A double holds 53 bits of
/// precision, so only the top 53 bits are used.
fn exec_random(bits: i64) -> f64 {
    let mantissa = (bits as u64) >> 11;
    mantissa as f64 / (1_u64 << 53) as f64
}

/// Converts a LIMIT or OFFSET value to bigint the way PostgreSQL does, rounding
/// fractions half away from zero, and rejects negative values.
fn exec_pg_limit_value(args: &[Register]) -> Result<Option<i64>> {
    let value = match args[0].get_value() {
        Value::Null => return Ok(None),
        Value::Numeric(Numeric::Integer(value)) => *value,
        Value::Numeric(Numeric::Float(value)) => {
            let rounded = f64::from(*value).round();
            if !(i64::MIN as f64..i64::MAX as f64).contains(&rounded) {
                return Err(sqlstate_error(
                    SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
                    "bigint out of range",
                ));
            }
            rounded as i64
        }
        Value::Text(text) => text.as_str().trim().parse().map_err(|_| {
            sqlstate_error(
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                format!(
                    "invalid input syntax for type bigint: \"{}\"",
                    text.as_str()
                ),
            )
        })?,
        Value::Blob(_) => return Err(LimboError::InvalidArgument("datatype mismatch".to_string())),
    };
    if value < 0 {
        let clause = text_arg(args, 1);
        let sqlstate = if clause == "OFFSET" {
            SQLSTATE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE
        } else {
            SQLSTATE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE
        };
        return Err(sqlstate_error(
            sqlstate,
            format!("{clause} must not be negative"),
        ));
    }
    Ok(Some(value))
}

fn exec_pg_get_user_by_id(oid: i64) -> &'static str {
    if oid == PG_DATABASE_OWNER_OID {
        "pg_database_owner"
    } else {
        "turso"
    }
}

fn exec_pg_is_visible(_oid: i64) -> Boolean {
    Boolean(true)
}

/// PostgreSQL version advertised by version(). The major version matches the
/// server_version startup parameter pgwire's DefaultServerParameterProvider
/// sends, so the two claims agree.
const SERVER_VERSION: &str = "16.6";

/// Clients sometimes gate connection setup on this string: knex and TypeORM regex
/// parse it as `^PostgreSQL ([\d.]+)`, so it must lead with a numeric version.
fn exec_version() -> String {
    format!(
        "PostgreSQL {SERVER_VERSION} (Turso v{})",
        env!("CARGO_PKG_VERSION")
    )
}

/// PostgreSQL's quote_literal(): wrap a value in single quotes for inclusion
/// in SQL, doubling embedded quotes. Backslashes force the E'' escape-string
/// form so the result reads back identically regardless of
/// standard_conforming_strings.
fn exec_quote_literal(value: &Value) -> Option<String> {
    if matches!(value, Value::Null) {
        return None;
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
    Some(quoted)
}

fn exec_textlike(args: &[Register]) -> Result<Option<Boolean>> {
    if args
        .iter()
        .any(|arg| matches!(arg.get_value(), Value::Null))
    {
        return Ok(None);
    }
    let text: Vec<char> = args[0].get_value().to_string().chars().collect();
    let pattern: Vec<char> = args[1].get_value().to_string().chars().collect();
    Ok(Some(Boolean(like_matches(&text, &pattern)?)))
}

/// PostgreSQL's LIKE: case-sensitive, `%` and `_` wildcards, and `\` makes the
/// next pattern character literal. On a mismatch, the last `%` absorbs one more
/// text character and matching resumes after it.
fn like_matches(text: &[char], pattern: &[char]) -> Result<bool> {
    let (mut p, mut t) = (0, 0);
    let mut backtrack: Option<(usize, usize)> = None;
    while t < text.len() {
        let step = match pattern.get(p) {
            Some('%') => {
                backtrack = Some((p, t));
                p += 1;
                continue;
            }
            Some('_') => Some(1),
            Some('\\') => match pattern.get(p + 1) {
                Some(&c) => (c == text[t]).then_some(2),
                None => {
                    return Err(sqlstate_error(
                        SQLSTATE_INVALID_ESCAPE_SEQUENCE,
                        "LIKE pattern must not end with escape character",
                    ));
                }
            },
            Some(&c) => (c == text[t]).then_some(1),
            None => None,
        };
        match (step, backtrack) {
            (Some(step), _) => {
                p += step;
                t += 1;
            }
            (None, Some((percent_p, percent_t))) => {
                p = percent_p + 1;
                t = percent_t + 1;
                backtrack = Some((percent_p, t));
            }
            (None, None) => return Ok(false),
        }
    }
    Ok(pattern[p..].iter().all(|&c| c == '%'))
}

/// Rewrites a `LIKE ... ESCAPE` pattern to use `\` as the escape character,
/// as PostgreSQL's like_escape() does.
fn exec_like_escape(args: &[Register]) -> Result<Option<String>> {
    if args
        .iter()
        .any(|arg| matches!(arg.get_value(), Value::Null))
    {
        return Ok(None);
    }
    let pattern = args[0].get_value().to_string();
    let escape = args[1].get_value().to_string();
    let mut escape_chars = escape.chars();
    let result = match (escape_chars.next(), escape_chars.next()) {
        (None, _) => pattern.replace('\\', "\\\\"),
        (Some('\\'), None) => pattern,
        (Some(escape), None) => {
            let mut result = String::with_capacity(pattern.len());
            let mut after_escape = false;
            for c in pattern.chars() {
                if c == escape && !after_escape {
                    result.push('\\');
                    after_escape = true;
                    continue;
                }
                if c == '\\' && !after_escape {
                    result.push('\\');
                }
                result.push(c);
                after_escape = false;
            }
            result
        }
        _ => {
            return Err(sqlstate_error(
                SQLSTATE_INVALID_ESCAPE_SEQUENCE,
                "invalid escape string: Escape string must be empty or one character.",
            ));
        }
    };
    Ok(Some(result))
}

/// Converts a SIMILAR TO pattern to a regular expression, as PostgreSQL's
/// similar_to_escape() does. Without an ESCAPE argument, `\` is the escape.
fn exec_similar_to_escape(args: &[Register]) -> Result<Option<String>> {
    if args
        .iter()
        .any(|arg| matches!(arg.get_value(), Value::Null))
    {
        return Ok(None);
    }
    let pattern = args[0].get_value().to_string();
    let escape = args
        .get(1)
        .map_or_else(|| "\\".to_string(), |arg| arg.get_value().to_string());
    match turso_pg_parser::translator::similar_to_regex(&pattern, &escape) {
        Ok(regex) => Ok(Some(regex)),
        Err(turso_pg_parser::ParseError::Sqlstate { sqlstate, message }) => {
            Err(sqlstate_error(sqlstate, message))
        }
        Err(error) => Err(LimboError::ParseError(error.to_string())),
    }
}

fn exec_pg_encoding_to_char(encoding: i64) -> &'static str {
    match encoding {
        6 => "UTF8",
        0 => "SQL_ASCII",
        _ => "UTF8",
    }
}

fn exec_pg_format_type(type_oid: i64, typemod: i64) -> String {
    match type_oid {
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
    }
}

/// Simplified to_char: formats a number with the given format pattern.
/// Supports basic PG numeric format patterns (9, 0, S, MI, FM, D, G, PR, TH, L).
fn exec_to_char(value: &Value, format: &str) -> Option<String> {
    let num = match value {
        Value::Null => return None,
        Value::Numeric(_) => value.as_float(),
        Value::Text(t) => t.as_str().parse::<f64>().ok()?,
        _ => return None,
    };
    Some(pg_to_char_numeric(num, format))
}

/// age(end, start) the way PostgreSQL's timestamp_age computes it: subtract the
/// timestamps field by field and borrow from the next larger field, where a
/// borrowed month is as long as the earlier timestamp's month. age(start)
/// measures from midnight of the current date.
fn exec_age(args: &[Register]) -> Result<Value> {
    if args
        .iter()
        .any(|arg| matches!(arg.get_value(), Value::Numeric(_) | Value::Blob(_)))
    {
        let argument_types = args
            .iter()
            .map(|arg| match arg.get_value() {
                Value::Null => "unknown",
                Value::Numeric(Numeric::Integer(_)) => "integer",
                Value::Numeric(Numeric::Float(_)) => "numeric",
                Value::Text(_) => "timestamp",
                Value::Blob(_) => "bytea",
            })
            .collect::<Vec<_>>()
            .join(", ");
        return Err(LimboError::InvalidArgument(format!(
            "function age({argument_types}) does not exist"
        )));
    }
    let mut timestamps = Vec::with_capacity(args.len());
    for arg in args {
        match arg.get_value() {
            Value::Text(text) => timestamps.push(parse_timestamp(text.as_str())?),
            _ => return Ok(Value::Null),
        }
    }
    let (end, start) = match timestamps[..] {
        [start] => (Utc::now().date_naive().and_time(NaiveTime::MIN), start),
        [end, start] => (end, start),
        _ => unreachable!("age() is declared with one or two arguments"),
    };
    let (later, earlier, sign) = if end < start {
        (start, end, -1)
    } else {
        (end, start, 1)
    };

    let micros_of_day = |t: NaiveDateTime| {
        i64::from(t.num_seconds_from_midnight()) * 1_000_000 + i64::from(t.nanosecond() / 1000)
    };
    let mut micros = micros_of_day(later) - micros_of_day(earlier);
    let mut days = i64::from(later.day()) - i64::from(earlier.day());
    let mut months = i64::from(later.year() - earlier.year()) * 12 + i64::from(later.month())
        - i64::from(earlier.month());
    if micros < 0 {
        micros += 86_400_000_000;
        days -= 1;
    }
    if days < 0 {
        days += i64::from(earlier.num_days_in_month());
        months -= 1;
    }
    Ok(Value::build_text(format_interval(
        sign * months,
        sign * days,
        sign * micros,
    )))
}

/// Reads a timestamp in the form the timestamp type stores, `YYYY-MM-DD
/// HH:MM:SS[.f]`, or a date in the form the date type stores, `YYYY-MM-DD`,
/// as midnight of that day, since PostgreSQL casts a date to a timestamp.
/// The timestamp type keeps an hour of 24 as written, so `24:00:00` is
/// midnight at the end of the day.
fn parse_timestamp(text: &str) -> Result<NaiveDateTime> {
    let (text_without_hour_24, extra_days) = match text.split_once(" 24:") {
        Some((date, time)) => (format!("{date} 00:{time}"), 1),
        None => (text.to_string(), 0),
    };
    NaiveDateTime::parse_from_str(&text_without_hour_24, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| {
            NaiveDate::parse_from_str(&text_without_hour_24, "%Y-%m-%d")
                .map(|date| date.and_time(NaiveTime::MIN))
        })
        .ok()
        .and_then(|timestamp| timestamp.checked_add_signed(TimeDelta::days(extra_days)))
        .ok_or_else(|| {
            LimboError::InvalidArgument(format!(
                "invalid input syntax for type timestamp: \"{text}\""
            ))
        })
}

/// Formats an interval like PostgreSQL's interval_out with IntervalStyle
/// postgres, e.g. `-1 years -9 mons -7 days -03:04:05.5`. All parts must have
/// the same sign.
fn format_interval(months: i64, days: i64, micros: i64) -> String {
    let mut parts = Vec::new();
    for (value, unit) in [(months / 12, "year"), (months % 12, "mon"), (days, "day")] {
        if value != 0 {
            let plural = if value == 1 { "" } else { "s" };
            parts.push(format!("{value} {unit}{plural}"));
        }
    }
    if parts.is_empty() || micros != 0 {
        let sign = if micros < 0 { "-" } else { "" };
        let seconds = micros.abs() / 1_000_000;
        let mut time = format!(
            "{sign}{:02}:{:02}:{:02}",
            seconds / 3600,
            seconds / 60 % 60,
            seconds % 60
        );
        let fraction = micros.abs() % 1_000_000;
        if fraction != 0 {
            time.push_str(format!(".{fraction:06}").trim_end_matches('0'));
        }
        parts.push(time);
    }
    parts.join(" ")
}

/// PostgreSQL's float math raises errors where SQLite returns NULL or lets
/// the float saturate: logarithms and square roots of invalid inputs, and
/// exp and power results that overflow or underflow float8. The dialect
/// resolves these functions here instead of to the engine builtins.
pub(crate) fn exec_math_with_range_errors(name: &str, args: &[Value]) -> Result<Value> {
    if name == "chr" {
        return exec_pg_chr(&args[0]);
    }
    let Some(x) = float_argument(&args[0])? else {
        return Ok(Value::Null);
    };
    let result = match name {
        "exp" => {
            let result = x.exp();
            if x.is_finite() {
                if result.is_infinite() {
                    return Err(float_out_of_range("overflow"));
                }
                if result == 0.0 {
                    return Err(float_out_of_range("underflow"));
                }
            }
            result
        }
        "sqrt" => {
            if x < 0.0 {
                return Err(sqlstate_error(
                    SQLSTATE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
                    "cannot take square root of a negative number",
                ));
            }
            x.sqrt()
        }
        "ln" => checked_ln(x)?,
        "log" if args.len() == 1 => {
            checked_ln(x)?;
            x.log10()
        }
        "log" => {
            let Some(y) = float_argument(&args[1])? else {
                return Ok(Value::Null);
            };
            let ln_base = checked_ln(x)?;
            let ln_value = checked_ln(y)?;
            if ln_base == 0.0 {
                return Err(sqlstate_error(
                    SQLSTATE_DIVISION_BY_ZERO,
                    "division by zero",
                ));
            }
            ln_value / ln_base
        }
        "power" | "pow" => {
            let Some(y) = float_argument(&args[1])? else {
                return Ok(Value::Null);
            };
            if x == 0.0 && y < 0.0 {
                return Err(sqlstate_error(
                    SQLSTATE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
                    "zero raised to a negative power is undefined",
                ));
            }
            if x < 0.0 && y.is_finite() && y.fract() != 0.0 {
                return Err(sqlstate_error(
                    SQLSTATE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
                    "a negative number raised to a non-integer power yields a complex result",
                ));
            }
            let result = x.powf(y);
            if x.is_finite() && y.is_finite() {
                if result.is_infinite() {
                    return Err(float_out_of_range("overflow"));
                }
                if result == 0.0 && x != 0.0 {
                    return Err(float_out_of_range("underflow"));
                }
            }
            result
        }
        _ => {
            return Err(sqlstate_error(
                SQLSTATE_UNDEFINED_FUNCTION,
                format!("no such function: {name}"),
            ))
        }
    };
    Ok(Value::from_f64(result))
}

fn checked_ln(x: f64) -> Result<f64> {
    if x == 0.0 {
        return Err(sqlstate_error(
            SQLSTATE_INVALID_ARGUMENT_FOR_LOG,
            "cannot take logarithm of zero",
        ));
    }
    if x < 0.0 {
        return Err(sqlstate_error(
            SQLSTATE_INVALID_ARGUMENT_FOR_LOG,
            "cannot take logarithm of a negative number",
        ));
    }
    Ok(x.ln())
}

/// PostgreSQL's chr is strict and rejects code points its text cannot
/// hold; the engine's own chr turns NULL into an empty string.
fn exec_pg_chr(value: &Value) -> Result<Value> {
    let code = match value {
        Value::Null => return Ok(Value::Null),
        Value::Numeric(Numeric::Integer(i)) => *i,
        other => match strict_numeric_argument(other) {
            Some(Numeric::Integer(i)) => i,
            _ => {
                return Err(sqlstate_error(
                    SQLSTATE_INVALID_TEXT_REPRESENTATION,
                    format!("invalid input syntax for type integer: \"{other}\""),
                ))
            }
        },
    };
    if code < 0 {
        return Err(sqlstate_error(
            SQLSTATE_INVALID_PARAMETER_VALUE,
            "character number must be positive",
        ));
    }
    if code == 0 {
        return Err(sqlstate_error(
            SQLSTATE_PROGRAM_LIMIT_EXCEEDED,
            "null character not permitted",
        ));
    }
    let character = u32::try_from(code).ok().and_then(char::from_u32);
    let Some(character) = character else {
        return Err(sqlstate_error(
            SQLSTATE_PROGRAM_LIMIT_EXCEEDED,
            format!("requested character too large for encoding: {code}"),
        ));
    };
    Ok(Value::build_text(character.to_string()))
}

/// The float value of a math function argument, coerced the way
/// PostgreSQL coerces an unknown literal to double precision: NULL stays
/// NULL, and text must be a number or an Infinity or NaN word.
fn float_argument(value: &Value) -> Result<Option<f64>> {
    if let Value::Text(t) = value {
        let trimmed = t.as_str().trim();
        if let Some(f) = float_word(trimmed) {
            return Ok(Some(f));
        }
        return match strict_numeric_argument(value) {
            Some(n) => Ok(Some(n.to_f64())),
            None => Err(sqlstate_error(
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                format!(
                    "invalid input syntax for type double precision: \"{}\"",
                    t.as_str()
                ),
            )),
        };
    }
    Ok(Option::<Numeric>::from(value).map(|n| n.to_f64()))
}

/// The numeric value of an argument PostgreSQL would coerce strictly:
/// non-decimal integer text is accepted, other text must parse fully.
fn strict_numeric_argument(value: &Value) -> Option<Numeric> {
    if let Value::Text(t) = value {
        let trimmed = t.as_str().trim();
        if has_non_decimal_prefix(trimmed) {
            return match parse_integer_input(trimmed) {
                IntegerInput::Value(v) => Some(Numeric::Integer(v)),
                IntegerInput::OutOfRange | IntegerInput::Invalid => None,
            };
        }
    }
    Numeric::from_value_strict(value)
}

/// The special float words PostgreSQL accepts, which the engine's numeric
/// type cannot hold: NaN, and Infinity in each spelling and sign.
fn float_word(trimmed: &str) -> Option<f64> {
    let negative = trimmed.starts_with('-');
    let word = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
    match word.to_ascii_lowercase().as_str() {
        "inf" | "infinity" if negative => Some(f64::NEG_INFINITY),
        "inf" | "infinity" => Some(f64::INFINITY),
        "nan" => Some(f64::NAN),
        _ => None,
    }
}

fn float_out_of_range(direction: &str) -> LimboError {
    sqlstate_error(
        SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
        format!("value out of range: {direction}"),
    )
}

/// pg_cast_input(value, type) validates and converts a cast's input with
/// PostgreSQL's input rules, where SQLite's CAST would accept anything. The
/// translator wraps casts to the numeric, json, and jsonb types with it; the
/// SQLite CAST that follows sees only values it keeps unchanged.
fn exec_pg_cast_input(value: &Value, cast_type: &str) -> Result<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    match cast_type {
        "smallint" => integer_cast_input(value, cast_type, i16::MIN as i64, i16::MAX as i64),
        "integer" => integer_cast_input(value, cast_type, i32::MIN as i64, i32::MAX as i64),
        "bigint" => integer_cast_input(value, cast_type, i64::MIN, i64::MAX),
        "real" | "double precision" | "numeric" => float_cast_input(value, cast_type),
        "json" | "jsonb" => json_cast_input(value),
        _ => Err(LimboError::InternalError(format!(
            "pg_cast_input does not handle type {cast_type}"
        ))),
    }
}

fn integer_cast_input(value: &Value, cast_type: &str, min: i64, max: i64) -> Result<Value> {
    match value {
        Value::Numeric(Numeric::Integer(i)) => {
            if *i < min || *i > max {
                return Err(cast_out_of_range(cast_type));
            }
            Ok(value.clone())
        }
        Value::Numeric(Numeric::Float(f)) => {
            let rounded = f64::from(*f).round();
            // max + 1 is -min, a power of two that f64 holds exactly; max itself
            // rounds up to 2^63 for bigint.
            if !rounded.is_finite() || rounded < min as f64 || rounded >= -(min as f64) {
                return Err(cast_out_of_range(cast_type));
            }
            Ok(Value::from_i64(rounded as i64))
        }
        Value::Text(t) => match parse_integer_input(t.as_str().trim()) {
            IntegerInput::Value(v) if v >= min && v <= max => Ok(Value::from_i64(v)),
            IntegerInput::Value(_) | IntegerInput::OutOfRange => {
                Err(text_out_of_range(cast_type, t.as_str()))
            }
            IntegerInput::Invalid => Err(invalid_cast_input(cast_type, t.as_str())),
        },
        _ => Ok(value.clone()),
    }
}

enum IntegerInput {
    Value(i64),
    OutOfRange,
    Invalid,
}

/// Parses integer text the way PostgreSQL's integer input does: an optional
/// sign, then decimal digits or a 0x/0o/0b prefix with digits of that base,
/// with single underscores allowed between digits.
fn parse_integer_input(text: &str) -> IntegerInput {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    let lower = unsigned.to_ascii_lowercase();
    let (radix, digits) = match lower.strip_prefix("0x") {
        Some(digits) => (16, digits),
        None => match lower.strip_prefix("0o") {
            Some(digits) => (8, digits),
            None => match lower.strip_prefix("0b") {
                Some(digits) => (2, digits),
                None => (10, lower.as_str()),
            },
        },
    };
    if digits.is_empty() || digits.starts_with('_') || digits.ends_with('_') {
        return IntegerInput::Invalid;
    }
    let mut magnitude: u64 = 0;
    let mut out_of_range = false;
    let mut previous_was_underscore = false;
    for c in digits.chars() {
        if c == '_' {
            if previous_was_underscore {
                return IntegerInput::Invalid;
            }
            previous_was_underscore = true;
            continue;
        }
        previous_was_underscore = false;
        let Some(digit) = c.to_digit(radix) else {
            return IntegerInput::Invalid;
        };
        match magnitude
            .checked_mul(radix as u64)
            .and_then(|m| m.checked_add(digit as u64))
        {
            Some(m) => magnitude = m,
            None => out_of_range = true,
        }
    }
    if out_of_range {
        return IntegerInput::OutOfRange;
    }
    let limit = if negative {
        i64::MIN.unsigned_abs()
    } else {
        i64::MAX as u64
    };
    if magnitude > limit {
        return IntegerInput::OutOfRange;
    }
    IntegerInput::Value(if negative {
        (magnitude as i128).wrapping_neg() as i64
    } else {
        magnitude as i64
    })
}

fn float_cast_input(value: &Value, cast_type: &str) -> Result<Value> {
    let overflows_real = |f: f64| cast_type == "real" && f.is_finite() && (f as f32).is_infinite();
    let underflows_real =
        |f: f64| cast_type == "real" && f != 0.0 && f.is_finite() && (f as f32) == 0.0;
    match value {
        Value::Numeric(Numeric::Integer(_)) => Ok(value.clone()),
        Value::Numeric(Numeric::Float(f)) => {
            let f = f64::from(*f);
            if overflows_real(f) || underflows_real(f) {
                return Err(cast_out_of_range(cast_type));
            }
            Ok(value.clone())
        }
        Value::Text(t) => {
            let mut trimmed = t.as_str().trim();
            // Every numeric input of PostgreSQL takes the non-decimal
            // integer forms: numeric by rule, the float types through
            // strtod. Underscores between digits are numeric-only.
            if has_non_decimal_prefix(trimmed) {
                return match parse_integer_input(trimmed) {
                    IntegerInput::Value(v) => Ok(Value::from_i64(v)),
                    IntegerInput::OutOfRange => Err(text_out_of_range(cast_type, t.as_str())),
                    IntegerInput::Invalid => Err(invalid_cast_input(cast_type, t.as_str())),
                };
            }
            let without_underscores;
            if cast_type == "numeric" && trimmed.contains('_') {
                match strip_digit_group_underscores(trimmed) {
                    Some(stripped) => {
                        without_underscores = stripped;
                        trimmed = &without_underscores;
                    }
                    None => return Err(invalid_cast_input(cast_type, t.as_str())),
                }
            }
            let spells_infinity = trimmed.to_ascii_lowercase().contains("inf");
            // PostgreSQL's numeric has a far larger range than float8, so a
            // value like '1e400' is valid numeric input; this engine stores
            // it as the float it parsed to. The float types report values
            // beyond their range in either direction.
            let out_of_range = |f: f64| {
                (cast_type != "numeric" && f.is_infinite() && !spells_infinity)
                    || (cast_type != "numeric" && f == 0.0 && has_nonzero_mantissa(trimmed))
                    || overflows_real(f)
                    || underflows_real(f)
            };
            match trimmed.parse::<f64>() {
                Ok(f) if out_of_range(f) => Err(text_out_of_range(cast_type, trimmed)),
                Ok(f) => Ok(Value::from_f64(f)),
                // PostgreSQL's numeric rejects trailing junk as invalid
                // syntax; only the float types parse like strtod and report
                // the overflow of a prefix like '1e999' first.
                Err(_) if cast_type == "numeric" => Err(invalid_cast_input(cast_type, t.as_str())),
                Err(_) => match overflowing_float_prefix(trimmed) {
                    Some(prefix) => Err(text_out_of_range(cast_type, prefix)),
                    None => Err(invalid_cast_input(cast_type, t.as_str())),
                },
            }
        }
        _ => Ok(value.clone()),
    }
}

/// Removes PostgreSQL's digit-group underscores: each must sit between
/// two digits. Returns None when one does not.
fn strip_digit_group_underscores(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'_'
            && !(i > 0
                && bytes[i - 1].is_ascii_digit()
                && bytes.get(i + 1).is_some_and(u8::is_ascii_digit))
        {
            return None;
        }
    }
    Some(text.replace('_', ""))
}

/// Whether the digits before the exponent are not all zero, so a parse
/// that came back exactly zero lost the value to underflow.
fn has_nonzero_mantissa(text: &str) -> bool {
    text.split(['e', 'E'])
        .next()
        .is_some_and(|mantissa| mantissa.bytes().any(|b| b.is_ascii_digit() && b != b'0'))
}

fn has_non_decimal_prefix(text: &str) -> bool {
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    let mut prefix = unsigned.chars().take(2).map(|c| c.to_ascii_lowercase());
    prefix.next() == Some('0') && matches!(prefix.next(), Some('x' | 'o' | 'b'))
}

/// PostgreSQL parses float text like strtod, so '1e999x' reports the
/// overflow of '1e999' rather than the trailing junk.
fn overflowing_float_prefix(text: &str) -> Option<&str> {
    let prefix = &text[..float_syntax_prefix_len(text)];
    match prefix.parse::<f64>() {
        Ok(f) if f.is_infinite() => Some(prefix),
        _ => None,
    }
}

/// The length of the longest leading substring with number syntax:
/// an optional sign, digits with an optional fraction, and an optional
/// exponent. Infinity and NaN words never overflow, so they are not part
/// of this scan.
fn float_syntax_prefix_len(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut i = 0;
    if matches!(bytes.first(), Some(b'+' | b'-')) {
        i += 1;
    }
    let integer_start = i;
    while bytes.get(i).is_some_and(u8::is_ascii_digit) {
        i += 1;
    }
    let mut has_digits = i > integer_start;
    if bytes.get(i) == Some(&b'.') {
        let fraction_start = i + 1;
        let mut j = fraction_start;
        while bytes.get(j).is_some_and(u8::is_ascii_digit) {
            j += 1;
        }
        if has_digits || j > fraction_start {
            i = j;
            has_digits = true;
        }
    }
    if !has_digits {
        return 0;
    }
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        let mut j = i + 1;
        if matches!(bytes.get(j), Some(b'+' | b'-')) {
            j += 1;
        }
        let exponent_start = j;
        while bytes.get(j).is_some_and(u8::is_ascii_digit) {
            j += 1;
        }
        if j > exponent_start {
            i = j;
        }
    }
    i
}

fn json_cast_input(value: &Value) -> Result<Value> {
    if let Value::Text(t) = value {
        // RawValue checks the syntax without building a value, so numbers
        // beyond f64's range stay valid like they are in PostgreSQL.
        if serde_json::from_str::<&serde_json::value::RawValue>(t.as_str()).is_err() {
            return Err(sqlstate_error(
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                "invalid input syntax for type json",
            ));
        }
    }
    Ok(value.clone())
}

fn invalid_cast_input(cast_type: &str, input: &str) -> LimboError {
    sqlstate_error(
        SQLSTATE_INVALID_TEXT_REPRESENTATION,
        format!("invalid input syntax for type {cast_type}: \"{input}\""),
    )
}

fn text_out_of_range(cast_type: &str, input: &str) -> LimboError {
    sqlstate_error(
        SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
        format!("value \"{input}\" is out of range for type {cast_type}"),
    )
}

fn cast_out_of_range(cast_type: &str) -> LimboError {
    sqlstate_error(
        SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
        format!("{cast_type} out of range"),
    )
}

/// pg_input_is_valid(text, type) → boolean
/// Returns true if the text is valid input for the given type.
fn exec_pg_input_is_valid(input: &Value, type_name: &str) -> Option<Boolean> {
    let s = match input {
        Value::Text(t) => t.as_str().to_string(),
        Value::Null => return None,
        v => v.to_string(),
    };
    Some(Boolean(validate_pg_input(&s, type_name).is_none()))
}

fn exec_pg_get_expr(args: &[Register]) -> Result<Option<String>> {
    match args.first().map(Register::get_value) {
        Some(Value::Text(expression)) => {
            if args[1..]
                .iter()
                .any(|arg| matches!(arg.get_value(), Value::Null))
            {
                return Ok(None);
            }

            // TursoPG stores adbin and conbin as rendered SQL, so the relation OID and pretty flag do not change the output.
            Ok(Some(expression.as_str().to_string()))
        }
        Some(Value::Null) => Ok(None),
        Some(expression) => {
            let arg_types = [pg_type_name(expression), "oid", "boolean"];
            Err(undefined_function("pg_get_expr", &arg_types[..args.len()]))
        }
        None => unreachable!("pg_get_expr takes 2 or 3 arguments"),
    }
}

/// The error of PostgreSQL for a call with arguments of types that no
/// function of this name accepts.
fn undefined_function(name: &str, arg_types: &[&str]) -> LimboError {
    sqlstate_error(
        SQLSTATE_UNDEFINED_FUNCTION,
        format!("function {name}({}) does not exist", arg_types.join(", ")),
    )
}

/// The PostgreSQL type that a value of the engine has, for error messages.
fn pg_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "unknown",
        Value::Numeric(Numeric::Integer(_)) => "integer",
        Value::Numeric(Numeric::Float(_)) => "numeric",
        Value::Text(_) => "text",
        Value::Blob(_) => "bytea",
    }
}

/// The message and SQLSTATE of a failed cast-input check, in the shape
/// validate_pg_input reports.
fn cast_input_error(result: Result<Value>) -> Option<(String, String)> {
    match result {
        Ok(_) => None,
        Err(LimboError::Dialect(error)) => Some((error.message, error.code.to_string())),
        Err(other) => Some((other.to_string(), "22P02".to_string())),
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
            if parse_pg_bool(trimmed).is_some() {
                None
            } else {
                Some((
                    format!("invalid input syntax for type boolean: \"{input}\""),
                    "22P02".to_string(),
                ))
            }
        }
        // The numeric types validate with the same parsers as casts and
        // column assignments, so pg_input_is_valid cannot disagree with
        // what a cast accepts.
        "int2" | "smallint" => cast_input_error(exec_pg_cast_input(
            &Value::build_text(input.to_string()),
            "smallint",
        )),
        "int4" | "integer" | "int" => cast_input_error(exec_pg_cast_input(
            &Value::build_text(input.to_string()),
            "integer",
        )),
        "int8" | "bigint" => cast_input_error(exec_pg_cast_input(
            &Value::build_text(input.to_string()),
            "bigint",
        )),
        "float4" | "real" => cast_input_error(exec_pg_cast_input(
            &Value::build_text(input.to_string()),
            "real",
        )),
        "float8" | "double precision" => cast_input_error(exec_pg_cast_input(
            &Value::build_text(input.to_string()),
            "double precision",
        )),
        "numeric" | "decimal" => cast_input_error(exec_pg_cast_input(
            &Value::build_text(input.to_string()),
            "numeric",
        )),
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

/// The value of a PostgreSQL boolean literal such as 'yes', 'off' or 'tr',
/// with the rules of PostgreSQL's boolean input: white space around the text
/// and the case of the letters do not matter, and the start of true, false,
/// yes or no is enough. For on and off, the first two letters are necessary.
/// None when the text is not a boolean literal.
pub(crate) fn parse_pg_bool(text: &str) -> Option<bool> {
    let word = text
        .trim_matches(|c: char| c.is_ascii_whitespace() || c == '\u{b}')
        .to_ascii_lowercase();
    let starts = |full: &str, min_len: usize| word.len() >= min_len && full.starts_with(&word);
    if starts("true", 1) || starts("yes", 1) || starts("on", 2) || word == "1" {
        Some(true)
    } else if starts("false", 1) || starts("no", 1) || starts("off", 2) || word == "0" {
        Some(false)
    } else {
        None
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
