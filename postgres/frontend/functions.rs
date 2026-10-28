use crate::catalog::PG_DATABASE_OWNER_OID;
use crate::error::sqlstate_error;
use crate::settings::SettingScope;
use chrono::Utc;
use std::{fmt, sync::Arc};
use turso_core::native_ext::{FunctionArity, ReturnType, ScalarCall, ScalarFunction};
use turso_core::types::IOResultOr;
use turso_core::{
    Connection, ExternalFunc, IOResult, LimboError, Numeric, OpenOptions, Register, Result, Value,
};
use turso_pg_parser::sqlstate::{
    SQLSTATE_INTERNAL_ERROR, SQLSTATE_INVALID_ESCAPE_SEQUENCE,
    SQLSTATE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE, SQLSTATE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE,
    SQLSTATE_INVALID_TEXT_REPRESENTATION, SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
    SQLSTATE_UNDEFINED_FUNCTION, SQLSTATE_UNDEFINED_OBJECT,
};

pub(crate) fn register_functions(mut options: OpenOptions) -> OpenOptions {
    for function in SCALAR_FUNCTIONS {
        options = options.extension_function(
            ExternalFunc::new_native_scalar(
                function.name().to_string(),
                function.arity(),
                function.is_deterministic(),
                Some(function.return_type()),
                *function,
            )
            .expect("PostgreSQL function declarations have valid argument counts"),
        );
    }
    options
}

macro_rules! scalar_functions {
    ($($($variant:ident)|+($arity:expr, $deterministic:expr, $return_type:ident)),* $(,)?) => {
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

            fn return_type(&self) -> ReturnType {
                match self {
                    $($(Self::$variant)|+ => ReturnType::$return_type,)*
                }
            }
        }
    };
}

scalar_functions! {
    Acldefault(FunctionArity::Exact(2), true, Text),
    PgGetUserbyid(FunctionArity::Exact(1), true, Text),
    PgTableIsVisible | PgFunctionIsVisible | PgTypeIsVisible(FunctionArity::Exact(1), true, Integer),
    PgEncodingToChar(FunctionArity::Exact(1), true, Text),
    PgGetFunctionResult | PgGetFunctionArguments | PgGetStatisticsobjdefColumns(
        FunctionArity::Exact(1), true, Text
    ),
    PgRelationIsPublishable(FunctionArity::Exact(1), true, Integer),
    QuoteIdent(FunctionArity::Exact(1), true, Text),
    QuoteLiteral(FunctionArity::Exact(1), true, Text),
    FormatType(FunctionArity::OneOf(&[1, 2]), true, Text),
    PgGetConstraintdef(FunctionArity::OneOf(&[1, 2]), false, Text),
    PgGetIndexdef(FunctionArity::OneOf(&[1, 2]), false, Text),
    PgGetTriggerdef(FunctionArity::OneOf(&[1, 2]), false, Text),
    ObjDescription(FunctionArity::OneOf(&[1, 2]), true, Text),
    PgGetExpr(FunctionArity::OneOf(&[2, 3]), true, Text),
    ToChar(FunctionArity::Exact(2), true, Text),
    PgInputIsValid(FunctionArity::Exact(2), true, Integer),
    Textlike(FunctionArity::Exact(2), true, Integer),
    LikeEscape(FunctionArity::Exact(2), true, Text),
    Booleq(FunctionArity::Exact(2), true, Integer),
    Boolne(FunctionArity::Exact(2), true, Integer),
    ColDescription(FunctionArity::Exact(2), true, Text),
    SetConfig(FunctionArity::Exact(3), false, Text),
    CurrentSetting(FunctionArity::OneOf(&[1, 2]), false, Text),
    Version(FunctionArity::Exact(0), true, Text),
    CurrentDatabase(FunctionArity::Exact(0), false, Text),
    CurrentSchema(FunctionArity::Exact(0), true, Text),
    CurrentSchemas(FunctionArity::Exact(1), false, Text),
    PgBackendPid(FunctionArity::Exact(0), true, Integer),
    PgIsInRecovery(FunctionArity::Exact(0), false, Text),
    PgLockAccessShare(FunctionArity::Exact(0), false, Integer),
    PgLimitValue(FunctionArity::Exact(2), true, Integer),
    Now | ClockTimestamp | TransactionTimestamp | StatementTimestamp(FunctionArity::Variadic, false, Text),
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
            Self::Acldefault => exec_acl_default(args)?,
            Self::PgGetUserbyid => oid_arg(args, 0)?.map_or(Value::Null, exec_pg_get_user_by_id),
            Self::PgTableIsVisible | Self::PgFunctionIsVisible | Self::PgTypeIsVisible => {
                oid_arg(args, 0)?.map_or(Value::Null, exec_pg_is_visible)
            }
            Self::PgEncodingToChar => exec_pg_encoding_to_char(int_arg(args, 0, 0)),
            Self::PgGetFunctionResult
            | Self::PgGetFunctionArguments
            | Self::PgGetStatisticsobjdefColumns
            | Self::PgRelationIsPublishable
            | Self::PgGetTriggerdef
            | Self::ObjDescription
            | Self::ColDescription => Value::Null,
            Self::QuoteIdent => match args[0].get_value() {
                Value::Null => Value::Null,
                _ => Value::build_text(turso_pg_parser::quote_identifier(&text_arg(args, 0))),
            },
            Self::QuoteLiteral => exec_quote_literal(args[0].get_value()),
            Self::FormatType => oid_arg(args, 0)?.map_or(Value::Null, |type_oid| {
                exec_pg_format_type(type_oid, int_arg(args, 1, -1))
            }),
            Self::PgGetConstraintdef => oid_arg(args, 0)?.map_or(Value::Null, |oid| {
                exec_pg_get_constraintdef(connection, oid)
            }),
            Self::PgGetIndexdef => {
                oid_arg(args, 0)?.map_or(Value::Null, |oid| exec_pg_get_indexdef(connection, oid))
            }
            Self::PgGetExpr => exec_pg_get_expr(args)?,
            Self::ToChar => exec_to_char(args[0].get_value(), &text_arg(args, 1)),
            Self::PgInputIsValid => exec_pg_input_is_valid(args[0].get_value(), &text_arg(args, 1)),
            Self::Textlike => exec_textlike(args)?,
            Self::LikeEscape => exec_like_escape(args)?,
            Self::Booleq => Value::from_i64((args[0].get_value() == args[1].get_value()) as i64),
            Self::Boolne => Value::from_i64((args[0].get_value() != args[1].get_value()) as i64),
            Self::SetConfig => exec_set_config(connection, args)?,
            Self::CurrentSetting => exec_current_setting(connection, args)?,
            Self::Version => exec_version(),
            Self::CurrentDatabase => {
                Value::build_text(crate::catalog::db_name_from_path(connection.db_file_path()))
            }
            Self::CurrentSchema => match crate::session::current_schema(connection) {
                Some(schema) => Value::build_text(schema),
                None => Value::Null,
            },
            Self::CurrentSchemas => exec_current_schemas(connection, args[0].get_value())?,
            Self::PgBackendPid => Value::from_i64(std::process::id() as i64),
            Self::PgIsInRecovery => {
                // Temporary pg_dump compatibility: "t" means true, not actual recovery state.
                Value::build_text("t")
            }
            Self::PgLockAccessShare => {
                if connection.get_auto_commit() {
                    return Err(LimboError::InvalidArgument(
                        "LOCK TABLE can only be used in transaction blocks".to_string(),
                    )
                    .into());
                }
                Value::from_i64(0)
            }
            Self::PgLimitValue => exec_pg_limit_value(args)?,
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

fn exec_current_schemas(conn: &Connection, argument: &Value) -> Result<Value> {
    let include_implicit = match argument {
        Value::Null => return Ok(Value::Null),
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
    Ok(Value::build_text(result))
}

fn exec_acl_default(args: &[Register]) -> Result<Value> {
    if matches!(args[0].get_value(), Value::Null) || matches!(args[1].get_value(), Value::Null) {
        return Ok(Value::Null);
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
    Ok(Value::build_text(format!("{{{}}}", entries.join(","))))
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

#[derive(strum::EnumString)]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub(crate) enum ConfigParameter {
    SearchPath,
}

pub(crate) enum Parameter<'a> {
    BuiltIn(ConfigParameter),
    Custom(&'a str),
}

fn exec_set_config(conn: &Connection, args: &[Register]) -> Result<Value> {
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
        Parameter::BuiltIn(ConfigParameter::SearchPath) => {
            crate::session::set_search_path(conn, value, scope)
        }
        Parameter::Custom(name) => crate::session::set_custom_parameter(conn, name, value, scope),
    }
}

fn exec_current_setting(conn: &Connection, args: &[Register]) -> Result<Value> {
    if args
        .iter()
        .any(|arg| matches!(arg.get_value(), Value::Null))
    {
        return Ok(Value::Null);
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
        Some(Parameter::BuiltIn(ConfigParameter::SearchPath)) => Some(
            crate::session::search_path_setting(conn)
                .unwrap_or_else(|| crate::session::DEFAULT_SEARCH_PATH.to_owned()),
        ),
        Some(Parameter::Custom(name)) => crate::session::custom_parameter(conn, name),
        None => None,
    };
    match setting {
        Some(setting) => Ok(Value::build_text(setting)),
        None if missing_ok => Ok(Value::Null),
        None => Err(unrecognized_parameter(name)),
    }
}

fn unrecognized_parameter(name: &str) -> LimboError {
    sqlstate_error(
        SQLSTATE_UNDEFINED_OBJECT,
        format!("unrecognized configuration parameter \"{name}\""),
    )
}

pub(crate) fn find_parameter(name: &str) -> Option<Parameter<'_>> {
    match name.parse::<ConfigParameter>() {
        Ok(parameter) => Some(Parameter::BuiltIn(parameter)),
        Err(_) if name.contains('.') => Some(Parameter::Custom(name)),
        Err(_) => None,
    }
}

/// Converts a LIMIT or OFFSET value to bigint the way PostgreSQL does, rounding
/// fractions half away from zero, and rejects negative values.
fn exec_pg_limit_value(args: &[Register]) -> Result<Value> {
    let value = match args[0].get_value() {
        Value::Null => return Ok(Value::Null),
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
    Ok(Value::from_i64(value))
}

fn exec_pg_get_user_by_id(oid: i64) -> Value {
    Value::build_text(if oid == PG_DATABASE_OWNER_OID {
        "pg_database_owner"
    } else {
        "turso"
    })
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

fn exec_textlike(args: &[Register]) -> Result<Value> {
    if args
        .iter()
        .any(|arg| matches!(arg.get_value(), Value::Null))
    {
        return Ok(Value::Null);
    }
    let text: Vec<char> = args[0].get_value().to_string().chars().collect();
    let pattern: Vec<char> = args[1].get_value().to_string().chars().collect();
    Ok(Value::from_i64(like_matches(&text, &pattern)? as i64))
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
fn exec_like_escape(args: &[Register]) -> Result<Value> {
    if args
        .iter()
        .any(|arg| matches!(arg.get_value(), Value::Null))
    {
        return Ok(Value::Null);
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
    Ok(Value::build_text(result))
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

/// The value of a PostgreSQL boolean literal such as 'yes', 'off' or 'tr',
/// with the rules of PostgreSQL's boolean input: white space around the text
/// and the case of the letters do not matter, and the start of true, false,
/// yes or no is enough. For on and off, the first two letters are necessary.
/// None when the text is not a boolean literal.
fn parse_pg_bool(text: &str) -> Option<bool> {
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

#[cfg(test)]
mod tests {
    use super::ConfigParameter;

    #[test]
    fn config_parameter_parses_sql_names() {
        for name in ["search_path", "SEARCH_PATH", "SeArCh_PaTh"] {
            assert!(
                matches!(
                    name.parse::<ConfigParameter>(),
                    Ok(ConfigParameter::SearchPath)
                ),
                "{name}"
            );
        }
        for name in ["", "statement_timeout", "SearchPath", "search_path "] {
            assert!(name.parse::<ConfigParameter>().is_err(), "{name}");
        }
    }
}
