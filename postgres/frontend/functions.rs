use crate::acl::{acl_role_name, Privileges as AclPrivileges};
use crate::catalog::role_name_by_oid;
use crate::error::{sqlstate_error, sqlstate_error_with_hint};
use crate::pg_type::{built_in_type_named, lookup_type_name, type_input, NamedTypeKind};
use crate::settings::{find_parameter, Parameter, SettingScope};
use chrono::{DateTime, NaiveDateTime};
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use turso_core::native_ext::{FunctionArity, ReturnType, ScalarCall, ScalarFunction, ScalarReturn};
use turso_core::security::roles::RoleId;

use crate::roles::{connection_roles, RoleCatalog};
use turso_core::dialect::ScalarFuncContext;
use turso_core::types::IOResultOr;
use turso_core::{
    exec_temporal_encode, Connection, Decimal, ExternalFunc, IOResult, LimboError, Numeric,
    OpenOptions, Register, Result, Value, WallClockInstant,
};
use turso_pg_parser::datetime_unit::DatetimeUnit;
use turso_pg_parser::sqlstate::{
    SQLSTATE_DATETIME_FIELD_OVERFLOW, SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INTERNAL_ERROR,
    SQLSTATE_INVALID_DATETIME_FORMAT, SQLSTATE_INVALID_ESCAPE_SEQUENCE,
    SQLSTATE_INVALID_PARAMETER_VALUE, SQLSTATE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE,
    SQLSTATE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE, SQLSTATE_INVALID_TEXT_REPRESENTATION,
    SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE, SQLSTATE_STATEMENT_TOO_COMPLEX,
    SQLSTATE_UNDEFINED_FUNCTION, SQLSTATE_UNDEFINED_OBJECT, SQLSTATE_UNTRANSLATABLE_CHARACTER,
};

/// Registers one native scalar function per name. Each body is a Rust
/// expression that can use `?`. The planner reads the SQL return type from the
/// Rust type of the body (see `ScalarReturn`), so it cannot drift from what
/// the body returns.
macro_rules! scalar_functions {
    ($options:expr, |$context:ident, $connection:ident, $args:ident| {
        $($($name:literal)|+ ($arity:expr, $deterministic:expr) => $body:expr,)*
    }) => {{
        let mut options = $options;
        $($(
            #[allow(unused_variables)]
            let step = |$context: &ScalarFuncContext<'_>, $args: &[Register]| -> Result<_> {
                let $connection = $context.connection;
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
    let options = register_scalar_functions(options);
    let options = crate::pg_type::register_integer_operator_functions(options);
    options
        .extension_function(
            pg_scalar_function(
                "array_to_json",
                FunctionArity::OneOf(&[1, 2]),
                true,
                |_, args| exec_array_to_json(args),
            )
            .receiving_argument_types(),
        )
        .extension_function(
            pg_scalar_function(
                "date_trunc",
                FunctionArity::OneOf(&[2, 3]),
                true,
                |_, args| exec_date_trunc(args),
            )
            .receiving_argument_types()
            .checking_argument_types(check_date_trunc_types)
            .returning_argument_type(1),
        )
        // FIXME(pg-interval): PostgreSQL has no function of this name. It exists
        // because tursopg has no interval type: the translator rewrites
        // `timestamp ± interval literal` into a call of it, and users can also
        // call it by name. An interval type with the pg_operator rows
        // timestamp_pl_interval and timestamptz_pl_interval replaces it.
        .extension_function(
            pg_scalar_function(
                turso_pg_parser::TIMESTAMP_PLUS_INTERVAL_FUNCTION,
                FunctionArity::Exact(4),
                true,
                |_, args| exec_timestamp_plus_interval(args),
            )
            .receiving_argument_types()
            .checking_argument_types(check_timestamp_plus_interval_types)
            .returning_argument_type(0),
        )
}

/// Refuses the left side of `+ interval` when it is not timestamp or
/// timestamptz. `types` are the argument type names, with a domain replaced by
/// its base type; the first is the left side. Every other type gets 0A000, including the ones that have no operator
/// in PostgreSQL (which gives 42883), because the type name does not say
/// whether PostgreSQL has one. The translator turns `-` into `+` of the
/// negated interval, so the message always says `+`.
// FIXME(pg-catalog): tursopg refuses every other left type with 0A000 because
// it cannot tell which have a PostgreSQL operator. Some do, such as date, and
// an untyped literal or NULL is resolved by PostgreSQL to a type that does. A
// pg_operator lookup replaces this.
fn check_timestamp_plus_interval_types(types: &[String]) -> Result<()> {
    let time_type = types[0].as_str();
    match time_type {
        "timestamp" | "timestamptz" => Ok(()),
        "date" => Err(sqlstate_error_with_hint(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!(
                "adding an interval is supported only for timestamp and timestamptz, not {time_type}"
            ),
            "Cast the date to timestamp first: d::timestamp + interval.",
        )),
        _ => Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!(
                "adding an interval is supported only for timestamp and timestamptz, not {time_type}"
            ),
            None,
        )),
    }
}

/// Refuses date_trunc when its second argument is not timestamp or
/// timestamptz. `types` are the argument type names, with a domain replaced by
/// its base type, as PostgreSQL resolves it.
// FIXME(pg-catalog): HACK. This is a hand-written overload rule: the second
// argument of date_trunc is timestamp or timestamptz, and the result has that
// type (see `returning_argument_type(1)` where date_trunc is registered). It
// stands in for the pg_proc rows date_trunc(text, timestamp) returns timestamp
// and date_trunc(text, timestamptz) returns timestamptz. It refuses with 0A000
// arguments that PostgreSQL accepts, such as a date, which PostgreSQL casts to
// timestamptz. A pg_proc lookup replaces it.
fn check_date_trunc_types(types: &[String]) -> Result<()> {
    let time_type = types[1].as_str();
    match time_type {
        "timestamp" | "timestamptz" => Ok(()),
        "date" => Err(sqlstate_error_with_hint(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!("date_trunc is supported only for timestamp and timestamptz, not {time_type}"),
            "Cast the value to timestamptz: date_trunc(unit, d::timestamptz).",
        )),
        _ => Err(sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!("date_trunc is supported only for timestamp and timestamptz, not {time_type}"),
            None,
        )),
    }
}

/// `timestamp + interval` and `timestamptz + interval` for an interval
/// literal, which the translator passes as its months, days and microseconds:
/// `(time, months, days, microseconds)`. The result has the type of `time`.
/// The parts are added as PostgreSQL's timestamp_pl_interval adds them: the
/// months first, moving a day past the end of the month back to its last day,
/// then the days, then the microseconds. Only UTC is supported, so timestamptz
/// is computed like timestamp. The type of `time` is checked when the
/// statement is compiled.
fn exec_timestamp_plus_interval(args: &[Register]) -> Result<Value> {
    use chrono::Datelike;
    let (args, types) = args.split_at(args.len() / 2);
    let time_type = types[0].get_value().to_string();
    // The function can be called by name with any arguments, and a value
    // typed timestamp can reach it in a form the ENCODE of timestamp would
    // not write, so unexpected arguments are errors, not assertions.
    let not_supported = |what: String| sqlstate_error(SQLSTATE_FEATURE_NOT_SUPPORTED, what, None);
    let text = match args[0].get_value() {
        Value::Null => return Ok(Value::Null),
        Value::Text(text) => text.as_str(),
        other => {
            return Err(not_supported(format!(
                "adding an interval to the {time_type} value {other} is not supported"
            )))
        }
    };
    let integer = |register: &Register| match register.get_value() {
        Value::Numeric(Numeric::Integer(value)) => Ok(*value),
        other => Err(not_supported(format!(
            "an interval part that is not an integer, {other}, is not supported"
        ))),
    };
    let (months, days, micros) = (integer(&args[1])?, integer(&args[2])?, integer(&args[3])?);
    let unreadable = || {
        not_supported(format!(
            "adding an interval to the {time_type} value \"{text}\" is not supported"
        ))
    };
    let time =
        NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").map_err(|_| unreadable())?;
    let sum = add_interval(time, months, days, micros)
        .filter(|sum| (1..=9999).contains(&sum.year()))
        .ok_or_else(|| {
            sqlstate_error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                "a result outside the years 1 to 9999 is not supported",
                None,
            )
        })?;
    Ok(Value::build_text(timestamp_text(sum)))
}

/// `time` plus `months`, then `days`, then `micros`, as PostgreSQL's
/// timestamp_pl_interval adds them, or None if a step leaves chrono's range.
/// Adding months clamps the day to the last day of the target month, as
/// PostgreSQL does.
fn add_interval(time: NaiveDateTime, months: i64, days: i64, micros: i64) -> Option<NaiveDateTime> {
    use chrono::{Duration, Months};
    let month_count = Months::new(u32::try_from(months.unsigned_abs()).ok()?);
    let time = if months >= 0 {
        time.checked_add_months(month_count)?
    } else {
        time.checked_sub_months(month_count)?
    };
    time.checked_add_signed(Duration::try_days(days)?)?
        .checked_add_signed(Duration::microseconds(micros))
}

/// date_trunc(field, timestamp) and date_trunc(field, timestamptz). The result
/// has the type of the second argument, which `check_date_trunc_types` limits
/// to timestamp and timestamptz when the statement is compiled. Only UTC is
/// supported, so timestamptz is truncated like timestamp.
fn exec_date_trunc(args: &[Register]) -> Result<Value> {
    // Registered with arity 2 or 3; each argument is followed by its type.
    if args.len() == 6 {
        let message = "date_trunc with a time zone argument is not supported".to_string();
        let zone_is_utc = matches!(
            args[2].get_value(),
            Value::Text(zone) if zone.as_str().eq_ignore_ascii_case("utc")
        );
        return Err(if zone_is_utc {
            sqlstate_error_with_hint(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                message,
                "Leave out the time zone argument; the session time zone is UTC.",
            )
        } else {
            sqlstate_error(SQLSTATE_FEATURE_NOT_SUPPORTED, message, None)
        });
    }
    let (args, types) = args.split_at(args.len() / 2);
    let argument_type = types[1].get_value().to_string();
    // The name PostgreSQL uses for the type in its messages.
    let type_name = match argument_type.as_str() {
        "timestamp" => "timestamp without time zone",
        "timestamptz" => "timestamp with time zone",
        other => unreachable!("check_date_trunc_types refuses date_trunc of {other}"),
    };
    let not_supported = |what: String| sqlstate_error(SQLSTATE_FEATURE_NOT_SUPPORTED, what, None);
    let field = match args[0].get_value() {
        Value::Text(field) => field,
        // A SQL NULL unit gives NULL. A typed NULL such as NULL::int is
        // indistinguishable from it here, because the argument types are
        // not available for a NULL value.
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(sqlstate_error(
                SQLSTATE_UNDEFINED_FUNCTION,
                format!(
                    "function date_trunc({}, {type_name}) does not exist",
                    types[0].get_value()
                ),
                Some(format!("Got a unit argument that is not text: {other}")),
            ))
        }
    };
    let Value::Text(value) = args[1].get_value() else {
        return Ok(Value::Null);
    };
    let time =
        NaiveDateTime::parse_from_str(value.as_str(), "%Y-%m-%d %H:%M:%S%.f").map_err(|_| {
            not_supported(format!(
                "date_trunc is not supported for the {argument_type} value \"{}\"",
                value.as_str()
            ))
        })?;
    let lowered_field = field.as_str().to_ascii_lowercase();
    let parsed = DatetimeUnit::parse(field.as_str());
    if parsed == Some(DatetimeUnit::TimeZone) {
        return Err(not_supported(format!(
            "unit \"{lowered_field}\" not supported for type {type_name}"
        )));
    }
    let Some(unit) = parsed else {
        return Err(sqlstate_error(
            SQLSTATE_INVALID_PARAMETER_VALUE,
            format!("unit \"{lowered_field}\" not recognized for type {type_name}"),
            None,
        ));
    };
    let truncated = truncate_timestamp(time, unit).ok_or_else(|| {
        not_supported(format!(
            "date_trunc is not supported for the unit \"{}\" with the value \"{}\"",
            field.as_str(),
            value.as_str()
        ))
    })?;
    Ok(Value::build_text(timestamp_text(truncated)))
}

/// `time` truncated to `unit`, as PostgreSQL's date_trunc does it, or None
/// when the result is before year 1: PostgreSQL prints it as BC, which the
/// timestamptz encoding cannot store. Weeks start on Monday; century and
/// millennium start at years ending in 01, as in PostgreSQL.
fn truncate_timestamp(time: NaiveDateTime, unit: DatetimeUnit) -> Option<NaiveDateTime> {
    use chrono::{Datelike, Duration, NaiveDate, NaiveTime, Timelike};
    let date = time.date();
    let first_day_of_year = |year: i32| NaiveDate::from_ymd_opt(year, 1, 1);
    let day = match unit {
        DatetimeUnit::Microsecond => return Some(time),
        DatetimeUnit::Millisecond => {
            let nanos = time.nanosecond() / 1_000_000 * 1_000_000;
            return time.with_nanosecond(nanos);
        }
        DatetimeUnit::Second => return time.with_nanosecond(0),
        DatetimeUnit::Minute => return time.with_nanosecond(0)?.with_second(0),
        DatetimeUnit::Hour => return time.with_nanosecond(0)?.with_second(0)?.with_minute(0),
        DatetimeUnit::Day => date,
        DatetimeUnit::Week => {
            date - Duration::days(i64::from(date.weekday().num_days_from_monday()))
        }
        DatetimeUnit::Month => date.with_day(1)?,
        DatetimeUnit::Quarter => {
            NaiveDate::from_ymd_opt(date.year(), (date.month() - 1) / 3 * 3 + 1, 1)?
        }
        DatetimeUnit::Year => first_day_of_year(date.year())?,
        DatetimeUnit::Decade => first_day_of_year(date.year().div_euclid(10) * 10)?,
        DatetimeUnit::Century => first_day_of_year((date.year() - 1).div_euclid(100) * 100 + 1)?,
        DatetimeUnit::Millennium => {
            first_day_of_year((date.year() - 1).div_euclid(1000) * 1000 + 1)?
        }
        DatetimeUnit::TimeZone => {
            unreachable!("date_trunc refuses the time zone units before truncating")
        }
    };
    // A year before 1 is BC in PostgreSQL. Only the decade of years 1 to 9
    // gets here, because the other units stay in the year of `time` or start
    // at year 1.
    assert!(day.year() >= 0, "date_trunc result before year 0");
    if day.year() < 1 {
        return None;
    }
    Some(day.and_time(NaiveTime::MIN))
}

fn register_scalar_functions(options: OpenOptions) -> OpenOptions {
    scalar_functions!(options, |context, connection, args| {
        "acldefault"(FunctionArity::Exact(2), true) => exec_acl_default(connection, args)?,
        "pg_get_userbyid"(FunctionArity::Exact(1), false) => {
            oid_arg(args, 0)?.map(|oid| exec_pg_get_user_by_id(connection, oid))
        },
        "pg_table_is_visible"(FunctionArity::Exact(1), false) => {
            oid_arg(args, 0)?.and_then(|oid| exec_pg_table_is_visible(connection, oid))
        },
        "pg_function_is_visible" | "pg_type_is_visible"(
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
        "json_strip_nulls"(FunctionArity::Exact(1), true) => {
            exec_json_strip_nulls(args[0].get_value())?
        },
        "jsonb_field"(FunctionArity::Exact(2), true) => {
            exec_jsonb_field(args[0].get_value(), args[1].get_value())?
        },
        "jsonb_field_text"(FunctionArity::Exact(2), true) => {
            exec_jsonb_field_text(args[0].get_value(), args[1].get_value())?
        },
        "jsonb_contains"(FunctionArity::Exact(2), true) => {
            exec_jsonb_contains(args[0].get_value(), args[1].get_value())?
        },
        "jsonb_extract_path"(FunctionArity::Variadic, true) => {
            exec_jsonb_extract_path(args)?
        },
        "jsonb_extract_path_text"(FunctionArity::Variadic, true) => {
            exec_jsonb_extract_path_text(args)?
        },
        "jsonb_extract_path_op"(FunctionArity::Exact(2), true) => {
            exec_jsonb_extract_path_op(args[0].get_value(), args[1].get_value())?
        },
        "jsonb_extract_path_text_op"(FunctionArity::Exact(2), true) => {
            exec_jsonb_extract_path_text_op(args[0].get_value(), args[1].get_value())?
        },
        "jsonb_exists"(FunctionArity::Exact(2), true) => {
            exec_jsonb_exists(args[0].get_value(), args[1].get_value())?
        },
        "jsonb_exists_any"(FunctionArity::Exact(2), true) => {
            exec_jsonb_exists_keys(args[0].get_value(), args[1].get_value(), false)?
        },
        "jsonb_exists_all"(FunctionArity::Exact(2), true) => {
            exec_jsonb_exists_keys(args[0].get_value(), args[1].get_value(), true)?
        },
        "format_type"(FunctionArity::OneOf(&[1, 2]), true) => oid_arg(args, 0)?
            .map(|type_oid| format_type(type_oid, int_arg(args, 1, -1))),
        "pg_get_constraintdef"(FunctionArity::OneOf(&[1, 2]), false) => oid_arg(args, 0)?
            .and_then(|oid| crate::catalog::pg_get_constraintdef(connection, oid)),
        "pg_get_indexdef"(FunctionArity::OneOf(&[1, 2]), false) => oid_arg(args, 0)?
            .and_then(|oid| crate::catalog::pg_get_indexdef(connection, oid)),
        "pg_get_triggerdef"(FunctionArity::OneOf(&[1, 2]), false) => None::<String>,
        "pg_get_expr"(FunctionArity::OneOf(&[2, 3]), true) => exec_pg_get_expr(args)?,
        "to_char"(FunctionArity::Exact(2), true) => {
            // to_char is strict: a NULL argument gives NULL for every argument type.
            if args.iter().any(|arg| matches!(arg.get_value(), Value::Null)) {
                None
            } else {
                exec_to_char(args[0].get_value(), &text_arg(args, 1))?
            }
        },
        // Registered so that the core time extension's to_timestamp, which
        // returns a blob, never answers a PostgreSQL query.
        "to_timestamp"(FunctionArity::OneOf(&[1, 2]), true) => Err::<Timestamptz, _>(
            sqlstate_error(
                SQLSTATE_FEATURE_NOT_SUPPORTED,
                "to_timestamp is not supported", None,
            )
        )?,
        "pg_input_is_valid"(FunctionArity::Exact(2), true) => {
            exec_pg_input_is_valid(connection, args[0].get_value(), args[1].get_value())?
        },
        "int8_out_of_range"(FunctionArity::Exact(0), false) => Err::<i64, _>(sqlstate_error(
            SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
            "bigint out of range", None,
        ))?,
        "pg_cast"(FunctionArity::Exact(3), true) => crate::cast_planning::exec_pg_cast(connection, args)?,
        "textlike"(FunctionArity::Exact(2), true) => exec_textlike(args)?,
        "like_escape"(FunctionArity::Exact(2), true) => exec_like_escape(args)?,
        "similar_to_escape"(FunctionArity::OneOf(&[1, 2]), true) => exec_similar_to_escape(args)?,
        "booleq"(FunctionArity::Exact(2), true) => Boolean(args[0].get_value() == args[1].get_value()),
        "boolne"(FunctionArity::Exact(2), true) => Boolean(args[0].get_value() != args[1].get_value()),
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
        "current_user" | "current_role" | "user"(FunctionArity::Exact(0), false) => {
            role_name(connection, connection.current_role())?
        },
        "session_user"(FunctionArity::Exact(0), false) => {
            role_name(connection, connection.session_role())?
        },
        "pg_backend_pid"(FunctionArity::Exact(0), true) => std::process::id() as i64,
        // Temporary pg_dump compatibility: true, not the actual recovery state.
        "pg_is_in_recovery"(FunctionArity::Exact(0), false) => Boolean(true),
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
        "gen_random_uuid"(FunctionArity::Exact(0), false) => {
            let io = &connection.get_pager().io;
            Uuid(crate::pg_type::random_uuid_text(
                io.generate_random_number(),
                io.generate_random_number(),
            ))
        },
        // The time the transaction started, or the statement outside a
        // transaction.
        "now" | "transaction_timestamp"(FunctionArity::Exact(0), false) => {
            timestamptz_at(context.statement_times().transaction)
        },
        "statement_timestamp"(FunctionArity::Exact(0), false) => {
            timestamptz_at(context.statement_times().statement)
        },
        // The clock at each call.
        "clock_timestamp"(FunctionArity::Exact(0), false) => {
            timestamptz_at(context.current_time())
        },
    })
}

/// The timestamptz of `instant`, a reading of the system clock, with
/// microseconds as PostgreSQL keeps them.
fn timestamptz_at(instant: WallClockInstant) -> Timestamptz {
    let time: NaiveDateTime = DateTime::from_timestamp(instant.secs, instant.micros * 1_000)
        .expect("a system clock reading is a valid date and time")
        .naive_utc();
    Timestamptz(timestamp_text(time))
}

/// The text of `time` with microseconds, without trailing zeros of the
/// fraction and without a dot that has no digits after it, as PostgreSQL
/// prints it.
fn timestamp_text(time: NaiveDateTime) -> String {
    let text = time.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

pub(crate) fn pg_scalar_function<O: ScalarReturn + 'static>(
    name: &'static str,
    arity: FunctionArity,
    deterministic: bool,
    step: fn(&ScalarFuncContext<'_>, &[Register]) -> Result<O>,
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
    step: fn(&ScalarFuncContext<'_>, &[Register]) -> Result<O>,
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

    fn step(&mut self, context: &ScalarFuncContext<'_>, args: &[Register]) -> IOResultOr<O> {
        Ok(IOResult::Done((self.step)(context, args)?))
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

/// A `json` value in its text form. Returning one gives the function the type
/// `json`, as in PostgreSQL, instead of text.
struct Json(String);

impl ScalarReturn for Json {
    const TYPE: Option<ReturnType> = Some(ReturnType::Custom("json"));

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
                None,
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

fn exec_acl_default(conn: &Connection, args: &[Register]) -> Result<Option<String>> {
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
                None,
            )
        })?;
    let (public_privileges, owner_privileges) = kind.default_privileges();

    let role = acl_role_name(
        &role_name_by_oid(&connection_roles(conn), owner).unwrap_or_else(|| owner.to_string()),
    );
    let mut entries = Vec::with_capacity(2);
    if !public_privileges.is_empty() {
        entries.push(format!("={public_privileges}/{role}"));
    }
    if !owner_privileges.is_empty() {
        let grantee = if owner == 0 { "" } else { &role };
        entries.push(format!("{grantee}={owner_privileges}/{role}"));
    }
    Ok(Some(turso_core::text_array_literal(
        entries.iter().map(String::as_str),
    )))
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
            Self::Table => (no_privileges, AclPrivileges::RELATION),
            Self::Sequence => (no_privileges, AclPrivileges::SEQUENCE),
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

fn int_arg(args: &[Register], i: usize, default: i64) -> i64 {
    args.get(i)
        .and_then(|arg| arg.get_value().as_int())
        .unwrap_or(default)
}

fn oid_arg(args: &[Register], i: usize) -> Result<Option<i64>> {
    let value = args[i].get_value();
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(crate::pg_type::oid_input(text.as_str())?.as_int()),
        value => value.as_int().map(Some).ok_or_else(|| {
            LimboError::ConversionError(format!("invalid input syntax for type oid: \"{value}\""))
        }),
    }
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
                None,
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
        None,
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
                    None,
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
                None,
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
            None,
        ));
    }
    Ok(Some(value))
}

fn role_name(conn: &Connection, role: RoleId) -> Result<String> {
    role_name_in(&connection_roles(conn), role)
}

/// The role of a connection is normally in the catalog, but it may have been
/// dropped by another connection since the connection was opened.
pub(crate) fn role_name_in(roles: &RoleCatalog, role: RoleId) -> Result<String> {
    match crate::catalog::postgres_roles(roles).find(|found| found.id == role) {
        Some(role) => Ok(role.name),
        None => Err(sqlstate_error(
            SQLSTATE_UNDEFINED_OBJECT,
            format!("invalid role OID: {}", crate::catalog::role_oid(role)),
            None,
        )),
    }
}

fn exec_pg_get_user_by_id(conn: &Connection, oid: i64) -> String {
    role_name_by_oid(&connection_roles(conn), oid).unwrap_or_else(|| format!("unknown (OID={oid})"))
}

fn exec_pg_table_is_visible(conn: &Connection, oid: i64) -> Option<Boolean> {
    let schema = conn.current_schema();
    let catalog = crate::classes::class_catalog(&schema)?;
    let relation = catalog.get_by_oid(oid)?;
    for name in crate::session::current_schemas(conn, true) {
        let namespace = if name == "pg_catalog" {
            turso_core::namespace::NamespaceId::ROOT
        } else {
            let Some(qualifier) = conn.qualifier(&name) else {
                continue;
            };
            qualifier.target.id
        };
        if namespace == relation.relnamespace {
            return Some(Boolean(true));
        }
        // Any earlier relation with the same SQL name hides this one, even
        // when the earlier relation has a different kind.
        if catalog.name_is_taken(namespace, &relation.relname) {
            return Some(Boolean(false));
        }
    }
    Some(Boolean(false))
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
    match value {
        Value::Null => None,
        Value::Text(t) => Some(quote_literal(t.as_str())),
        other => Some(quote_literal(&other.to_string())),
    }
}

/// `text` as PostgreSQL's quote_literal() writes it: in single quotes, with
/// quotes doubled, and as an escape string when it has a backslash.
pub(crate) fn quote_literal(text: &str) -> String {
    if text.contains('\\') {
        format!("E'{}'", text.replace('\\', "\\\\").replace('\'', "''"))
    } else {
        format!("'{}'", text.replace('\'', "''"))
    }
}

/// PostgreSQL's json_strip_nulls(json): drops object fields whose value is null,
/// keeping number text and duplicate keys as the json type does.
fn exec_json_strip_nulls(value: &Value) -> Result<Option<String>> {
    let json = match value {
        Value::Null => return Ok(None),
        Value::Text(text) => text.as_str(),
        other => {
            return Err(undefined_function(
                "json_strip_nulls",
                &[pg_type_name(other)],
            ))
        }
    };
    // Validate first, so the walk below can assume well-formed input.
    serde_json::from_str::<serde::de::IgnoredAny>(json).map_err(|_| invalid_json())?;
    let mut stripped = String::with_capacity(json.len());
    copy_without_null_fields(json, &mut 0, 0, &mut stripped)?;
    Ok(Some(stripped))
}

/// Appends the rows of PostgreSQL's json_each(json): the key of each member
/// unescaped and the text of its value as the input writes it, in the order of
/// the input and with repeated keys kept. The engine's json_each() is SQLite's,
/// which turns a number member into a SQL value and writes it again.
pub(crate) fn exec_json_each(value: &Value, rows: &mut Vec<[Value; 2]>) -> Result<()> {
    let json = match value {
        Value::Null => return Ok(()),
        Value::Text(text) => text.as_str(),
        other => return Err(undefined_function("json_each", &[pg_type_name(other)])),
    };
    // Validate first, so the walk below can assume well-formed input.
    serde_json::from_str::<serde::de::IgnoredAny>(json).map_err(|_| invalid_json())?;
    let bytes = json.as_bytes();
    let mut pos = 0;
    skip_json_whitespace(bytes, &mut pos);
    match bytes[pos] {
        b'{' => {}
        b'[' => {
            return Err(sqlstate_error(
                SQLSTATE_INVALID_PARAMETER_VALUE,
                "cannot deconstruct an array as an object",
                None,
            ))
        }
        _ => {
            return Err(sqlstate_error(
                SQLSTATE_INVALID_PARAMETER_VALUE,
                "cannot deconstruct a scalar",
                None,
            ))
        }
    }
    walk_json_members(json, |key, value| {
        let key = key.expect("every member of an object has a key");
        rows.push([Value::build_text(key), Value::build_text(value.to_string())]);
    })
}

/// PostgreSQL's `json -> key` and `jsonb -> key`: the JSON text of the field
/// `key` of an object, or of the element at index `key` of an array. The
/// translator turns the operator into this function.
fn exec_jsonb_field(json: &Value, key: &Value) -> Result<Option<String>> {
    find_json_field(json, key, "->")
}

/// PostgreSQL's `json ->> key` and `jsonb ->> key`: the value that `->` finds,
/// as text. A string has no quotes and no escapes, a number is written as
/// jsonb writes it, and a JSON null is NULL.
fn exec_jsonb_field_text(json: &Value, key: &Value) -> Result<Option<String>> {
    match find_json_field(json, key, "->>")? {
        Some(field) => json_value_text(&field),
        None => Ok(None),
    }
}

/// PostgreSQL's functions json_extract_path() and jsonb_extract_path(): the
/// JSON text of the value at a path in a JSON document. The arguments after
/// the document are the elements of the path, as PostgreSQL's VARIADIC
/// parameter gives them.
fn exec_jsonb_extract_path(args: &[Register]) -> Result<Option<String>> {
    Ok(find_json_path("jsonb_extract_path", "#>", args)?.map(str::to_string))
}

/// PostgreSQL's functions json_extract_path_text() and
/// jsonb_extract_path_text(): the value that jsonb_extract_path() finds, as
/// text, as `->>` gives it.
fn exec_jsonb_extract_path_text(args: &[Register]) -> Result<Option<String>> {
    match find_json_path("jsonb_extract_path_text", "#>>", args)? {
        Some(value) => json_value_text(value),
        None => Ok(None),
    }
}

/// PostgreSQL's `json #> path` and `jsonb #> path`: the value that
/// jsonb_extract_path() finds at the text array `path` in the JSON document
/// `json`. The translator turns the operator into a call of this function.
fn exec_jsonb_extract_path_op(json: &Value, path: &Value) -> Result<Option<String>> {
    Ok(find_json_path_of_array("jsonb_extract_path", "#>", json, path)?.map(str::to_string))
}

/// PostgreSQL's `json #>> path` and `jsonb #>> path`: the value that `#>`
/// finds, as text, as `->>` gives it.
fn exec_jsonb_extract_path_text_op(json: &Value, path: &Value) -> Result<Option<String>> {
    match find_json_path_of_array("jsonb_extract_path_text", "#>>", json, path)? {
        Some(value) => json_value_text(value),
        None => Ok(None),
    }
}

/// PostgreSQL's `jsonb ? key`, which is also its function jsonb_exists():
/// whether the text `key` is the name of a field of the object `json`, or a
/// string element of the array `json`. Only the top level counts. jsonb
/// stores a scalar document as an array with one element, so `'"a"' ? 'a'`
/// is true.
fn exec_jsonb_exists(json: &Value, key: &Value) -> Result<Option<Boolean>> {
    let Some(document) = json_document(json, "?", "text")? else {
        return Ok(None);
    };
    let key = match key {
        Value::Null => return Ok(None),
        Value::Text(key) => key.as_str(),
        other => {
            return Err(undefined_function(
                "jsonb_exists",
                &["jsonb", pg_type_name(other)],
            ))
        }
    };
    let keys = json_top_level_keys(document)?;
    Ok(Some(Boolean(keys.contains(key))))
}

/// PostgreSQL's `jsonb ?| keys` and `jsonb ?& keys`, which are also its
/// functions jsonb_exists_any() and jsonb_exists_all(): whether any element,
/// or every element, of the text array `keys` is a key of `json` as `?` finds
/// it: every element when `every_key` is true. A NULL element does not count,
/// so `?|` is false and `?&` is true when `keys` has no other element.
fn exec_jsonb_exists_keys(json: &Value, keys: &Value, every_key: bool) -> Result<Option<Boolean>> {
    let (func, op) = if every_key {
        ("jsonb_exists_all", "?&")
    } else {
        ("jsonb_exists_any", "?|")
    };
    let Some(document) = json_document(json, op, "text[]")? else {
        return Ok(None);
    };
    let Some(wanted) = text_array_elements(func, keys)? else {
        return Ok(None);
    };
    let keys = json_top_level_keys(document)?;
    let mut wanted = wanted.iter().flatten();
    Ok(Some(Boolean(if every_key {
        wanted.all(|key| keys.contains(key.as_str()))
    } else {
        wanted.any(|key| keys.contains(key.as_str()))
    })))
}

/// The text that `->>` gives for the JSON value `value`, which must be valid
/// JSON: a string without quotes and escapes, a number as jsonb writes it, and
/// NULL for a JSON null. A boolean, an object and an array are their JSON
/// text.
fn json_value_text(value: &str) -> Result<Option<String>> {
    if value.starts_with('"') {
        return decode_json_string(value, &mut 0).map(Some);
    }
    if value.starts_with(|c: char| c == '-' || c.is_ascii_digit()) {
        // A valid JSON number fails only when it has more digits than numeric
        // allows. PostgreSQL then fails when it makes the jsonb value.
        return turso_core::format_numeric_text(value)
            .map(Some)
            .map_err(|_| {
                sqlstate_error(
                    SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
                    "value overflows numeric format",
                    None,
                )
            });
    }
    Ok((value != "null").then(|| value.to_string()))
}

/// The JSON text of the value at a path in a JSON document, for
/// jsonb_extract_path() and jsonb_extract_path_text(). `args` are the document
/// and one text argument for each element of the path. `func` and `op` name
/// the function and its operator, for errors.
fn find_json_path<'a>(func: &str, op: &str, args: &'a [Register]) -> Result<Option<&'a str>> {
    let Some((json, path_args)) = args.split_first().filter(|(_, path)| !path.is_empty()) else {
        let arg_types = vec!["jsonb"; args.len()];
        return Err(undefined_function(func, &arg_types));
    };
    let Some(document) = json_document(json.get_value(), op, "text[]")? else {
        return Ok(None);
    };
    let path = path_args
        .iter()
        .map(|arg| match arg.get_value() {
            Value::Null => Ok(None),
            Value::Text(element) => Ok(Some(element.as_str().to_string())),
            // PostgreSQL reads a text array as the path only after the
            // keyword VARIADIC, which the translator rejects.
            other => Err(undefined_function(func, &["jsonb", pg_type_name(other)])),
        })
        .collect::<Result<_>>()?;
    json_path_value(document, path)
}

/// The JSON text of the value at the text array `path` in the JSON document
/// `json`, for `#>` and `#>>`. `func` and `op` name the function and the
/// operator, for errors.
fn find_json_path_of_array<'a>(
    func: &str,
    op: &str,
    json: &'a Value,
    path: &Value,
) -> Result<Option<&'a str>> {
    let Some(document) = json_document(json, op, "text[]")? else {
        return Ok(None);
    };
    match text_array_elements(func, path)? {
        Some(path) => json_path_value(document, path),
        None => Ok(None),
    }
}

/// The JSON text of the value at `path` in `document`, which must be valid
/// JSON. Each element of the path is the name of a field of an object, or the
/// index of an element of an array (see `json_path_index`). None when the
/// document has no value at the path, or when an element is NULL. A scalar
/// has no value at a path that is not empty, and the empty path gives the
/// whole document.
fn json_path_value(document: &str, path: Vec<Option<String>>) -> Result<Option<&str>> {
    // A NULL element gives NULL, as a NULL key of `->` does.
    let Some(path) = path.into_iter().collect::<Option<Vec<_>>>() else {
        return Ok(None);
    };
    let mut value = document.trim_matches(|c| matches!(c, ' ' | '\t' | '\n' | '\r'));
    if path.is_empty() {
        // The walk reads the whole document, so that a bad escape or deep
        // nesting anywhere in it errors as it does for a path that is not
        // empty.
        walk_json_members(value, |_, _| {})?;
    }
    for element in &path {
        let key = match value.as_bytes()[0] {
            b'{' => JsonKey::Field(element),
            b'[' => match json_path_index(element) {
                Some(index) => JsonKey::Index(index),
                None => return Ok(None),
            },
            _ => return Ok(None),
        };
        match json_field_span(value, &key)? {
            Some(found) => value = found,
            None => return Ok(None),
        }
    }
    Ok(Some(value))
}

/// The index of an array element that the path element `text` of `#>` gives,
/// as PostgreSQL reads it with strtoint(): a decimal integer that fits in 32
/// bits, after white space and an optional sign. None when `text` is not one,
/// and then the path leads to no value.
fn json_path_index(text: &str) -> Option<i64> {
    let number = text.trim_start_matches([' ', '\t', '\n', '\u{b}', '\u{c}', '\r']);
    let digits = number.strip_prefix(['+', '-']).unwrap_or(number);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    number.parse::<i32>().ok().map(i64::from)
}

/// The names of the fields of the object `json`, or the string elements of
/// the array `json`: the keys that `?` finds. A scalar string is an array
/// with one element in jsonb, and another scalar has no keys. `json` must be
/// valid JSON.
fn json_top_level_keys(json: &str) -> Result<HashSet<String>> {
    let bytes = json.as_bytes();
    let mut pos = 0;
    skip_json_whitespace(bytes, &mut pos);
    if bytes[pos] == b'"' {
        return Ok(HashSet::from([decode_json_string(json, &mut pos)?]));
    }
    let mut keys = HashSet::new();
    let mut strings = Vec::new();
    walk_json_members(json, |name, value| match name {
        Some(name) => {
            keys.insert(name);
        }
        None if value.starts_with('"') => strings.push(value),
        None => {}
    })?;
    for string in strings {
        keys.insert(decode_json_string(string, &mut 0)?);
    }
    Ok(keys)
}

/// The elements of the text array `value`, an argument of `func`, with None
/// for a NULL element, or None when `value` is NULL. A text value, such as a
/// string literal on the right of `?|`, `?&`, `#>` and `#>>`, is read as an
/// array literal, such as `{a,b}`, as PostgreSQL reads a string literal for a
/// text array.
fn text_array_elements(func: &str, value: &Value) -> Result<Option<Vec<Option<String>>>> {
    let elements = match value {
        Value::Null => return Ok(None),
        // FIXME(pg-catalog): This reads each text value as a text array literal, because a value does not carry its PostgreSQL type. PostgreSQL reads only a string literal or a parameter without a type so: pg_operator has `?|`, `?&`, `#>` and `#>>` only with a text[] right operand, so `j ?| '{a}'::text` fails with 42883. An expression tree that carries PostgreSQL types replaces this.
        Value::Text(text) => {
            turso_core::parse_text_array_of_texts(text.as_str()).ok_or_else(|| {
                sqlstate_error(
                    SQLSTATE_INVALID_TEXT_REPRESENTATION,
                    format!("malformed array literal: \"{}\"", text.as_str()),
                    None,
                )
            })?
        }
        Value::Blob(_) => turso_core::array_values_from_any(value)
            .ok_or_else(|| undefined_function(func, &["jsonb", "bytea"]))?,
        Value::Numeric(_) => return Err(undefined_function(func, &["jsonb", pg_type_name(value)])),
    };
    elements
        .into_iter()
        .map(|element| match element {
            Value::Null => Ok(None),
            Value::Text(text) => Ok(Some(text.as_str().to_string())),
            // An array of numbers or of arrays: PostgreSQL has these
            // functions only for text arrays.
            other => Err(undefined_function(
                func,
                &["jsonb", &format!("{}[]", pg_type_name(&other))],
            )),
        })
        .collect::<Result<_>>()
        .map(Some)
}

/// The field or array element that `->`, `->>`, `#>` and `#>>` look up.
enum JsonKey<'a> {
    /// The field with this name of an object.
    Field(&'a str),
    /// The element at this index of an array. A negative index counts from
    /// the end.
    Index(i64),
}

/// The JSON text of the value at `key` in the JSON document `json`, or None
/// when the document has no such value. `op` is the operator, for errors.
fn find_json_field(json: &Value, key: &Value, op: &str) -> Result<Option<String>> {
    let Some(json) = json_document(json, op, pg_type_name(key))? else {
        return Ok(None);
    };
    let key = match key {
        Value::Null => return Ok(None),
        Value::Text(name) => JsonKey::Field(name.as_str()),
        Value::Numeric(Numeric::Integer(index)) => JsonKey::Index(*index),
        other => return Err(undefined_operator("jsonb", op, pg_type_name(other))),
    };
    Ok(json_field_span(json, &key)?.map(str::to_string))
}

/// The JSON document that `value`, the left operand of the JSON operator
/// `op`, holds, or None when `value` is NULL. The document is checked, so
/// that the code that reads it can assume valid JSON. A json or jsonb value
/// reads as text. A number, or a blob, which is a bytea, has no JSON
/// operators in PostgreSQL. `right_type` is the type of the right operand,
/// for that error.
fn json_document<'a>(value: &'a Value, op: &str, right_type: &str) -> Result<Option<&'a str>> {
    let json = match value {
        Value::Null => return Ok(None),
        Value::Text(text) => text.as_str(),
        Value::Numeric(_) | Value::Blob(_) => {
            return Err(undefined_operator(pg_type_name(value), op, right_type))
        }
    };
    serde_json::from_str::<serde::de::IgnoredAny>(json).map_err(|_| invalid_json())?;
    Ok(Some(json))
}

/// The JSON text of the value at `key` in `json`, which must be valid JSON.
/// When an object has two fields with the name, the last one counts, as in
/// PostgreSQL.
fn json_field_span<'a>(json: &'a str, key: &JsonKey) -> Result<Option<&'a str>> {
    // The walk covers the whole document, also when the key cannot match, so
    // that a bad escape or deep nesting anywhere in it errors as in PostgreSQL.
    let mut found = None;
    let mut elements = Vec::new();
    walk_json_members(json, |member_key, value| match (member_key, key) {
        (Some(name), JsonKey::Field(field)) if name == *field => found = Some(value),
        (None, JsonKey::Index(_)) => elements.push(value),
        // A name on an array or an index on an object.
        _ => {}
    })?;
    let JsonKey::Index(index) = key else {
        return Ok(found);
    };
    let index = if *index < 0 {
        *index + elements.len() as i64
    } else {
        *index
    };
    Ok(usize::try_from(index)
        .ok()
        .and_then(|index| elements.get(index).copied()))
}

/// Calls `visit` with the key and the text of the value of each member of the
/// JSON object `json`, or with None and the text of each element of the JSON
/// array `json`. A scalar has no members. `json` must be valid JSON.
fn walk_json_members<'a>(
    json: &'a str,
    mut visit: impl FnMut(Option<String>, &'a str),
) -> Result<()> {
    let bytes = json.as_bytes();
    let mut pos = 0;
    skip_json_whitespace(bytes, &mut pos);
    // copy_without_null_fields decodes every string of a value and counts its
    // nesting, so a \u0000, a lone surrogate or nesting deeper than
    // MAX_JSON_DEPTH errors as in PostgreSQL. Its copy of the value is not used.
    let mut walked_value = String::new();
    let is_object = match bytes[pos] {
        b'{' => true,
        b'[' => false,
        _ => return copy_without_null_fields(json, &mut pos, 0, &mut walked_value),
    };
    pos += 1;
    loop {
        skip_json_whitespace(bytes, &mut pos);
        match bytes[pos] {
            b'}' | b']' => break,
            b',' => {
                pos += 1;
                continue;
            }
            _ => {}
        }
        let key = if is_object {
            let key = decode_json_string(json, &mut pos)?;
            skip_json_whitespace(bytes, &mut pos);
            pos += 1; // ':'
            skip_json_whitespace(bytes, &mut pos);
            Some(key)
        } else {
            None
        };
        let start = pos;
        walked_value.clear();
        copy_without_null_fields(json, &mut pos, 1, &mut walked_value)?;
        visit(key, &json[start..pos]);
    }
    Ok(())
}

/// PostgreSQL's `jsonb @> jsonb`, which is also its function jsonb_contains():
/// whether `json` contains `template`. Core runs this function for `a @> b`
/// when neither operand has an array type, and the translator turns `a <@ b`
/// into `b @> a`.
///
/// Core does not know the type of each array: a column of a VALUES list has
/// no array type, and CREATE TABLE AS stores an array as text. Thus two
/// arrays that are not JSON documents are compared with the array function of
/// core, as `@>` compared them before it compared jsonb values.
///
/// FIXME(pg-catalog): This chooses between arraycontains and jsonb_contains from the values, because the type of a VALUES column or of a column that CREATE TABLE AS made does not say that it is an array. PostgreSQL chooses from the types, so it fails with 22P02 for `'{}'::jsonb @> '{a}'`, where this gives false. An expression tree that carries PostgreSQL types replaces this.
fn exec_jsonb_contains(json: &Value, template: &Value) -> Result<Option<bool>> {
    match (json, template) {
        (Value::Null, _) | (_, Value::Null) => Ok(None),
        // A json or jsonb column reads as text.
        (Value::Text(json_text), Value::Text(template_text)) => {
            let documents = read_json_document(json_text.as_str())
                .and_then(|json| Ok((json, read_json_document(template_text.as_str())?)));
            match documents {
                Ok((json, template)) => Ok(Some(json_document_contains(&json, &template))),
                // `{a}` is an array, not JSON. A JSON object such as `{"a":1}`
                // is not an array, so a jsonb value keeps the JSON error.
                Err(error) => turso_core::array_contains_all(json, template)
                    .map(Some)
                    .ok_or(error),
            }
        }
        _ if holds_array(json) || holds_array(template) => {
            Ok(turso_core::array_contains_all(json, template))
        }
        // A number or a bytea has no @> for jsonb in PostgreSQL.
        _ => Err(undefined_operator(
            pg_type_name(json),
            "@>",
            pg_type_name(template),
        )),
    }
}

/// Whether `value` is a blob that holds an array. An array is stored as a
/// blob, and a bytea is a blob too.
fn holds_array(value: &Value) -> bool {
    matches!(value, Value::Blob(_)) && turso_core::array_values_from_any(value).is_some()
}

/// A JSON value as jsonb compares it. A number is its exact decimal value, so
/// `1.5` and `1.50` are equal. An object has one field for each name, sorted
/// by name.
enum JsonNode {
    Null,
    Bool(bool),
    Number(Decimal),
    String(String),
    Array(Vec<JsonNode>),
    Object(Vec<(String, JsonNode)>),
}

/// Reads the JSON document `json` into a tree, with the errors of PostgreSQL's
/// jsonb input.
///
/// A loop with its own list of open objects and arrays reads the document,
/// not a function that calls itself for each level: the server runs
/// statements on threads with small stacks, and 1000 levels of such calls
/// do not fit in them.
fn read_json_document(json: &str) -> Result<JsonNode> {
    // Validate first, so the reading below can assume well-formed input.
    serde_json::from_str::<serde::de::IgnoredAny>(json).map_err(|_| invalid_json())?;
    let bytes = json.as_bytes();
    let mut pos = 0;
    // The objects and arrays that are open, innermost last, each with the
    // name of the field that it is the value of, if it is in an object.
    let mut open: Vec<(Option<String>, OpenJsonContainer)> = Vec::new();
    // The name of the field whose value comes next.
    let mut name: Option<String> = None;
    loop {
        skip_json_whitespace(bytes, &mut pos);
        let value = match bytes[pos] {
            b',' => {
                pos += 1;
                continue;
            }
            b'{' | b'[' => {
                if open.len() == MAX_JSON_DEPTH {
                    return Err(sqlstate_error(
                        SQLSTATE_STATEMENT_TOO_COMPLEX,
                        "stack depth limit exceeded",
                        None,
                    ));
                }
                let container = if bytes[pos] == b'{' {
                    OpenJsonContainer::Object(Vec::new())
                } else {
                    OpenJsonContainer::Array(Vec::new())
                };
                pos += 1;
                open.push((name.take(), container));
                continue;
            }
            b'}' | b']' => {
                pos += 1;
                let (field_name, container) =
                    open.pop().expect("a closing bracket has its opening one");
                name = field_name;
                container.into_node()
            }
            b'"' if name.is_none()
                && matches!(open.last(), Some((_, OpenJsonContainer::Object(_)))) =>
            {
                name = Some(decode_json_string(json, &mut pos)?);
                skip_json_whitespace(bytes, &mut pos);
                pos += 1; // ':'
                continue;
            }
            b'"' => JsonNode::String(decode_json_string(json, &mut pos)?),
            b't' => {
                pos += 4;
                JsonNode::Bool(true)
            }
            b'f' => {
                pos += 5;
                JsonNode::Bool(false)
            }
            b'n' => {
                pos += 4;
                JsonNode::Null
            }
            // The arms above match every other value, so this is a number.
            _ => {
                let start = pos;
                while matches!(
                    bytes.get(pos),
                    Some(b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
                ) {
                    pos += 1;
                }
                // A JSON number is also valid numeric text, so the only error
                // is a number with more digits than numeric allows. jsonb
                // stores its numbers as numeric, so PostgreSQL fails then too.
                let number = json[start..pos].parse::<Decimal>().map_err(|_| {
                    sqlstate_error(
                        SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
                        "value overflows numeric format",
                        None,
                    )
                })?;
                JsonNode::Number(number)
            }
        };
        match open.last_mut() {
            None => return Ok(value),
            Some((_, OpenJsonContainer::Array(elements))) => elements.push(value),
            Some((_, OpenJsonContainer::Object(fields))) => {
                fields.push((name.take().expect("a field value has a name"), value))
            }
        }
    }
}

/// An object or array that read_json_document has not read to its end yet.
enum OpenJsonContainer {
    Array(Vec<JsonNode>),
    Object(Vec<(String, JsonNode)>),
}

impl OpenJsonContainer {
    fn into_node(self) -> JsonNode {
        match self {
            Self::Array(elements) => JsonNode::Array(elements),
            Self::Object(mut fields) => {
                // Of two fields with the same name, the last one counts, as in
                // jsonb. The sort is stable, so such fields keep their order.
                fields.sort_by(|a, b| a.0.cmp(&b.0));
                let mut unique: Vec<(String, JsonNode)> = Vec::with_capacity(fields.len());
                for field in fields {
                    match unique.last_mut() {
                        Some(last) if last.0 == field.0 => *last = field,
                        _ => unique.push(field),
                    }
                }
                JsonNode::Object(unique)
            }
        }
    }
}

/// Whether the document `json` contains the document `template`. Only at this
/// level does an array also contain a scalar that is one of its elements:
/// jsonb stores a scalar document as an array with one element.
fn json_document_contains(json: &JsonNode, template: &JsonNode) -> bool {
    match (json, template) {
        (
            JsonNode::Array(elements),
            JsonNode::Null | JsonNode::Bool(_) | JsonNode::Number(_) | JsonNode::String(_),
        ) => elements
            .iter()
            .any(|element| json_contains(element, template)),
        _ => json_contains(json, template),
    }
}

/// Whether `json` contains `template`: each field of an object in `template`
/// is in `json` with a value that contains it, and each element of an array
/// in `template` is contained by an element of the array in `json`, in any
/// order. Scalars must be equal.
fn json_contains(json: &JsonNode, template: &JsonNode) -> bool {
    match (json, template) {
        (JsonNode::Object(fields), JsonNode::Object(wanted)) => {
            wanted.iter().all(|(name, wanted_value)| {
                fields
                    .binary_search_by(|(field_name, _)| field_name.cmp(name))
                    .is_ok_and(|index| json_contains(&fields[index].1, wanted_value))
            })
        }
        (JsonNode::Array(elements), JsonNode::Array(wanted)) => {
            wanted.iter().all(|wanted_element| {
                elements
                    .iter()
                    .any(|element| json_contains(element, wanted_element))
            })
        }
        (JsonNode::Null, JsonNode::Null) => true,
        (JsonNode::Bool(a), JsonNode::Bool(b)) => a == b,
        (JsonNode::Number(a), JsonNode::Number(b)) => a == b,
        (JsonNode::String(a), JsonNode::String(b)) => a == b,
        _ => false,
    }
}

/// `array_to_json(array [, pretty])`: the array as a JSON array, with line
/// feeds between its elements when `pretty` is true. After the arguments come
/// their type names, such as `boolean[]`, because an element's stored value
/// doesn't tell its type: a boolean is stored as 1, and a bytea like a nested
/// array, as a blob.
fn exec_array_to_json(args: &[Register]) -> Result<Option<Json>> {
    let (args, types) = args.split_at(args.len() / 2);
    let array = args[0].get_value();
    let pretty = match args.get(1).map(Register::get_value) {
        None => false,
        Some(Value::Null) => return Ok(None),
        Some(Value::Text(text)) => parse_pg_bool(text.as_str()).ok_or_else(|| {
            sqlstate_error(
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                format!(
                    "invalid input syntax for type boolean: \"{}\"",
                    text.as_str()
                ),
                None,
            )
        })?,
        Some(pretty) => !matches!(pretty, Value::Numeric(Numeric::Integer(0))),
    };
    if matches!(array, Value::Null) {
        return Ok(None);
    }
    let array_type = types[0].get_value().to_string();
    let element_type = array_type.trim_end_matches("[]");
    let dimensions = (array_type.len() - element_type.len()) / 2;
    let element = JsonElement::of_type(element_type);
    let mut json = String::new();
    write_json_array(
        array,
        &JsonArrayLayout {
            element,
            dimensions,
            separator: if pretty { ",\n " } else { "," },
        },
        &mut json,
    )?;
    Ok(Some(Json(json)))
}

struct JsonArrayLayout {
    element: JsonElement,
    /// The number of array levels, or 0 when the argument's type doesn't say.
    dimensions: usize,
    separator: &'static str,
}

/// How an array element is written as JSON, which depends on its type.
#[derive(Clone, Copy)]
enum JsonElement {
    /// `true` or `false`.
    Boolean,
    /// A JSON number, or a JSON string for Infinity and -Infinity.
    Number,
    /// The value as it is, since it is already JSON.
    Json,
    /// `"\\x0102"`: the bytes in hex, as a string.
    Bytea,
    /// A JSON string with a `T` between the date and the time.
    Timestamp,
    /// Like Timestamp, followed by the UTC offset `+00:00`.
    Timestamptz,
    /// The value's text, as a JSON string.
    Text,
    /// The argument's type doesn't say: numbers are written as numbers, text as
    /// strings and a blob as a nested array.
    Unknown,
}

impl JsonElement {
    // FIXME(pg-catalog): PostgreSQL also picks how to_json writes a value by type oid (json_categorize_type), so the named arms stay. The last arm stands in for the cast to json in pg_cast of a type that has one, and for the typoutput function in pg_type of the others. A pg_cast and pg_type lookup replaces that arm.
    fn of_type(name: &str) -> Self {
        match name {
            "boolean" => Self::Boolean,
            "integer" | "smallint" | "bigint" | "real" | "numeric" => Self::Number,
            "json" | "jsonb" => Self::Json,
            // A cast to bytea is a cast to blob, so a blob is a bytea.
            "bytea" | "blob" => Self::Bytea,
            "timestamp" => Self::Timestamp,
            "timestamptz" => Self::Timestamptz,
            "anyarray" | "any" | "unknown" | "null" => Self::Unknown,
            _ => Self::Text,
        }
    }
}

fn write_json_array(array: &Value, layout: &JsonArrayLayout, json: &mut String) -> Result<()> {
    write_json_array_level(array, layout, layout.separator, 1, json)
}

fn write_json_array_level(
    array: &Value,
    layout: &JsonArrayLayout,
    separator: &str,
    level: usize,
    json: &mut String,
) -> Result<()> {
    let elements = turso_core::array_values_from_any(array)
        .ok_or_else(|| undefined_function("array_to_json", &[pg_type_name(array)]))?;
    json.push('[');
    for (i, element) in elements.iter().enumerate() {
        if i > 0 {
            json.push_str(separator);
        }
        let inner_level = level < layout.dimensions
            || (matches!(element, Value::Blob(_)) && !matches!(layout.element, JsonElement::Bytea));
        match element {
            Value::Null => json.push_str("null"),
            // Only the outermost level gets line feeds, as in PostgreSQL.
            _ if inner_level => write_json_array_level(element, layout, ",", level + 1, json)?,
            _ => write_json_element(element, layout.element, json),
        }
    }
    json.push(']');
    Ok(())
}

fn write_json_element(element: &Value, kind: JsonElement, json: &mut String) {
    match (kind, element) {
        (JsonElement::Boolean, Value::Numeric(Numeric::Integer(0))) => json.push_str("false"),
        (JsonElement::Boolean, Value::Numeric(_)) => json.push_str("true"),
        (JsonElement::Json, Value::Text(text)) => json.push_str(text.as_str()),
        (JsonElement::Timestamp, Value::Text(text)) => {
            write_json_string(&text.as_str().replacen(' ', "T", 1), json)
        }
        (JsonElement::Timestamptz, Value::Text(text)) => write_json_string(
            &format!("{}+00:00", text.as_str().replacen(' ', "T", 1)),
            json,
        ),
        (JsonElement::Text, Value::Numeric(number)) => {
            write_json_string(&Value::Numeric(*number).to_string(), json)
        }
        (_, Value::Numeric(Numeric::Integer(number))) => json.push_str(&number.to_string()),
        (_, Value::Numeric(Numeric::Float(number))) => {
            let text = float8_text(f64::from(*number));
            if number.is_finite() {
                json.push_str(&text);
            } else {
                // JSON has no infinity, so PostgreSQL writes it as a string.
                write_json_string(&text, json);
            }
        }
        (_, Value::Text(text)) => write_json_string(text.as_str(), json),
        (_, Value::Blob(bytes)) => {
            write_json_string(&format!("\\x{}", hex::encode(bytes.as_slice())), json)
        }
        (_, Value::Null) => json.push_str("null"),
    }
}

fn write_json_string(text: &str, json: &mut String) {
    json.push_str(&serde_json::to_string(text).expect("a string serializes"));
}

/// A float8 in PostgreSQL's text form: the shortest digits that read back as
/// the same number, in scientific notation when the exponent is below -4 or
/// 15 or more, as in `1e+20` and `1e-05`.
pub(crate) fn float8_text(value: f64) -> String {
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("scientific notation has an exponent");
    let exponent: i32 = exponent.parse().expect("the exponent is a number");
    if (-4..15).contains(&exponent) {
        format!("{value}")
    } else {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{mantissa}e{sign}{:02}", exponent.abs())
    }
}

/// The nesting limit of the walk below, the same as core's jsonb parser. Past
/// it PostgreSQL also errors, at a depth that depends on max_stack_depth.
const MAX_JSON_DEPTH: usize = 1000;

pub(crate) fn invalid_json() -> LimboError {
    sqlstate_error(
        SQLSTATE_INVALID_TEXT_REPRESENTATION,
        "invalid input syntax for type json",
        None,
    )
}

pub(crate) fn unsupported_unicode_escape() -> LimboError {
    sqlstate_error(
        SQLSTATE_UNTRANSLATABLE_CHARACTER,
        "unsupported Unicode escape sequence",
        None,
    )
}

/// Copies the JSON value at `pos` into `out`, leaving out object fields whose
/// value is null. `depth` is the number of objects and arrays around it.
fn copy_without_null_fields(
    json: &str,
    pos: &mut usize,
    depth: usize,
    out: &mut String,
) -> Result<()> {
    let bytes = json.as_bytes();
    skip_json_whitespace(bytes, pos);
    if matches!(bytes[*pos], b'{' | b'[') && depth == MAX_JSON_DEPTH {
        return Err(sqlstate_error(
            SQLSTATE_STATEMENT_TOO_COMPLEX,
            "stack depth limit exceeded",
            None,
        ));
    }
    match bytes[*pos] {
        b'{' => {
            *pos += 1;
            out.push('{');
            let mut first = true;
            loop {
                skip_json_whitespace(bytes, pos);
                match bytes[*pos] {
                    b'}' => break,
                    b',' => {
                        *pos += 1;
                        continue;
                    }
                    _ => {}
                }
                // Decoded even when the field is dropped, so a bad escape in
                // its key errors as in PostgreSQL.
                let key = decode_json_string(json, pos)?;
                skip_json_whitespace(bytes, pos);
                *pos += 1; // ':'
                skip_json_whitespace(bytes, pos);
                if bytes[*pos..].starts_with(b"null") {
                    *pos += 4;
                    continue;
                }
                if !first {
                    out.push(',');
                }
                first = false;
                push_json_string(&key, out);
                out.push(':');
                copy_without_null_fields(json, pos, depth + 1, out)?;
            }
            *pos += 1;
            out.push('}');
        }
        b'[' => {
            *pos += 1;
            out.push('[');
            let mut first = true;
            loop {
                skip_json_whitespace(bytes, pos);
                match bytes[*pos] {
                    b']' => break,
                    b',' => {
                        *pos += 1;
                        continue;
                    }
                    _ => {}
                }
                if !first {
                    out.push(',');
                }
                first = false;
                copy_without_null_fields(json, pos, depth + 1, out)?;
            }
            *pos += 1;
            out.push(']');
        }
        b'"' => {
            let string = decode_json_string(json, pos)?;
            push_json_string(&string, out);
        }
        // A number, true, false or null: copied as written.
        _ => {
            let start = *pos;
            while !matches!(
                bytes.get(*pos),
                None | Some(b',' | b']' | b'}' | b' ' | b'\t' | b'\n' | b'\r')
            ) {
                *pos += 1;
            }
            out.push_str(&json[start..*pos]);
        }
    }
    Ok(())
}

fn skip_json_whitespace(bytes: &[u8], pos: &mut usize) {
    while matches!(bytes.get(*pos), Some(b' ' | b'\t' | b'\n' | b'\r')) {
        *pos += 1;
    }
}

/// Decodes the string literal at `pos` and moves `pos` past its closing quote.
fn decode_json_string(json: &str, pos: &mut usize) -> Result<String> {
    let bytes = json.as_bytes();
    let start = *pos;
    *pos += 1;
    loop {
        match bytes[*pos] {
            b'\\' => *pos += 2,
            b'"' => break,
            _ => *pos += 1,
        }
    }
    *pos += 1;
    let decoded: String = serde_json::from_str(&json[start..*pos]).map_err(|_| invalid_json())?;
    if decoded.contains('\0') {
        return Err(unsupported_unicode_escape());
    }
    Ok(decoded)
}

/// Writes a string the way PostgreSQL's escape_json() does: only quotes,
/// backslashes and control characters are escaped.
fn push_json_string(string: &str, out: &mut String) {
    out.push('"');
    for c in string.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

fn exec_textlike(args: &[Register]) -> Result<Option<Boolean>> {
    let (Some(text), Some(pattern)) = (
        args[0].get_value().cast_text_ref(),
        args[1].get_value().cast_text_ref(),
    ) else {
        return Ok(None);
    };
    Ok(Some(Boolean(like_matches(&text, &pattern)?)))
}

/// PostgreSQL's LIKE (MatchText in like_match.c): case-sensitive, `%` and `_`
/// wildcards, and `\` makes the next pattern character literal. Literals are
/// compared byte by byte, and `_` steps over one UTF-8 character. A `%` with
/// the wildcards after it is skipped at once; on a later mismatch, matching
/// resumes after it at the next text position that starts with the literal
/// following it. Only the last `%` needs this: once the pattern after it
/// reaches another `%`, no other position for the earlier one can do better.
fn like_matches(text: &str, pattern: &str) -> Result<bool> {
    let next_char = |t: usize| t + text[t..].chars().next().map_or(1, char::len_utf8);
    let trailing_escape = || {
        sqlstate_error(
            SQLSTATE_INVALID_ESCAPE_SEQUENCE,
            "LIKE pattern must not end with escape character",
            None,
        )
    };
    let (text_bytes, pattern_bytes) = (text.as_bytes(), pattern.as_bytes());
    let (mut p, mut t) = (0, 0);
    // The pattern position after the last `%` and the text position from
    // which to look for its next match.
    let mut resume: Option<(usize, usize)> = None;
    loop {
        if t == text.len() {
            return Ok(pattern_bytes[p..].iter().all(|&b| b == b'%'));
        }
        let matched = match pattern_bytes.get(p) {
            Some(b'%') => {
                p += 1;
                while let Some(&wildcard @ (b'%' | b'_')) = pattern_bytes.get(p) {
                    if wildcard == b'_' {
                        if t == text.len() {
                            return Ok(false);
                        }
                        t = next_char(t);
                    }
                    p += 1;
                }
                match &pattern_bytes[p..] {
                    [] => return Ok(true),
                    [b'\\'] => return Err(trailing_escape()),
                    _ => {}
                }
                resume = Some((p, t));
                false
            }
            Some(b'_') => {
                p += 1;
                t = next_char(t);
                true
            }
            Some(b'\\') => {
                let Some(&literal) = pattern_bytes.get(p + 1) else {
                    return Err(trailing_escape());
                };
                p += 2;
                t += 1;
                literal == text_bytes[t - 1]
            }
            Some(&literal) => {
                p += 1;
                t += 1;
                literal == text_bytes[t - 1]
            }
            None => false,
        };
        if matched {
            continue;
        }
        let Some((resume_p, search_from)) = resume else {
            return Ok(false);
        };
        let first = match pattern_bytes[resume_p] {
            b'\\' => pattern_bytes[resume_p + 1],
            literal => literal,
        };
        // `first` starts a character, so a text byte equal to it does too.
        let Some(offset) = text_bytes[search_from..].iter().position(|&b| b == first) else {
            return Ok(false);
        };
        p = resume_p;
        t = search_from + offset;
        resume = Some((resume_p, t + 1));
    }
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
                None,
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
    turso_pg_parser::translator::similar_to_regex(&pattern, &escape)
        .map(Some)
        .map_err(crate::error::sql_parse_error)
}

fn exec_pg_encoding_to_char(encoding: i64) -> &'static str {
    match encoding {
        6 => "UTF8",
        0 => "SQL_ASCII",
        _ => "UTF8",
    }
}

/// FIXME(pg-catalog): This table of oids and names was copied by hand from pg_type. It lists the types that `format_type` prints in another form than their `typname`, such as integer for int4. The others print their `typname` from the pg_type table, which is not what PostgreSQL does for every one: it prints `time with time zone` for timetz. It prints character for bpchar with typemod -1, where PostgreSQL prints bpchar. A rule in pg_type for how `format_type` prints each type replaces this.
/// The name of a type as `format_type` prints it.
pub(crate) fn format_type(type_oid: i64, typemod: i64) -> String {
    match type_oid {
        16 => "boolean".to_string(),
        18 => "\"char\"".to_string(),
        20 => "bigint".to_string(),
        21 => "smallint".to_string(),
        23 => "integer".to_string(),
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
        1083 => "time without time zone".to_string(),
        1114 => "timestamp without time zone".to_string(),
        1184 => "timestamp with time zone".to_string(),
        1700 => {
            if typemod > 4 {
                let precision = ((typemod - 4) >> 16) & 0xffff;
                let scale = (typemod - 4) & 0xffff;
                format!("numeric({precision},{scale})")
            } else {
                "numeric".to_string()
            }
        }
        // These have no row in the pg_type table yet.
        19 => "name".to_string(),
        1186 => "interval".to_string(),
        2206 => "regtype".to_string(),
        2278 => "void".to_string(),
        _ => crate::catalog::pg_type_name(type_oid)
            .unwrap_or("unknown")
            .to_string(),
    }
}

/// Simplified to_char: formats a number with the given format pattern.
/// Supports basic PG numeric format patterns (9, 0, S, MI, FM, D, G, PR, TH, L).
/// Any other argument, such as a timestamp, fails with 0A000 instead of
/// returning NULL.
fn exec_to_char(value: &Value, format: &str) -> Result<Option<String>> {
    let num = match value {
        Value::Null => return Ok(None),
        Value::Numeric(_) => value.as_float(),
        Value::Text(t) => match t.as_str().parse::<f64>() {
            Ok(num) => num,
            Err(_) => return Err(to_char_not_supported()),
        },
        _ => return Err(to_char_not_supported()),
    };
    Ok(Some(pg_to_char_numeric(num, format)))
}

fn to_char_not_supported() -> LimboError {
    sqlstate_error(
        SQLSTATE_FEATURE_NOT_SUPPORTED,
        "to_char is supported only for numbers",
        None,
    )
}

/// pg_input_is_valid(text, type) → boolean
/// Returns true if the text is valid input for the given type.
fn exec_pg_input_is_valid(
    conn: &Arc<Connection>,
    input: &Value,
    type_name: &Value,
) -> Result<Option<Boolean>> {
    let Value::Text(type_name) = type_name else {
        return Ok(None);
    };
    let input = match input {
        Value::Text(text) => text.as_str().to_string(),
        Value::Null => return Ok(None),
        other => other.to_string(),
    };
    // FIXME(pg-class): Catalog row types are missing from type lookup. PostgreSQL
    // gives pg_class a composite row type whose INPUT is record_in, so
    // pg_input_is_valid('1', 'pg_class') returns false for a malformed record.
    // We instead raise 42704 before checking the input. The pg_class work must
    // register relation row types in pg_type, expose their field types, and
    // connect them to record_in. Then flip pg-input-is-valid-catalog-row-type
    // in postgres/conformance/pg-sqltests/functions.sqltest: keep PostgreSQL's
    // expected result and remove @compat-gap and the expect @tursopg block.
    let error = validate_pg_input(conn, &input, type_name.as_str())?;
    Ok(Some(Boolean(error.is_none())))
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
        None,
    )
}

fn undefined_operator(left_type: &str, op: &str, right_type: &str) -> LimboError {
    undefined_operator_of(op, &[left_type, right_type])
}

/// The 42883 error for an operator with one operand (a prefix operator) or two.
pub(crate) fn undefined_operator_of(op: &str, operand_types: &[&str]) -> LimboError {
    let signature = match operand_types {
        [operand] => format!("{op} {operand}"),
        [left, right] => format!("{left} {op} {right}"),
        _ => unreachable!("an operator has one or two operands"),
    };
    sqlstate_error(
        SQLSTATE_UNDEFINED_FUNCTION,
        format!("operator does not exist: {signature}"),
        None,
    )
}

/// FIXME(pg-catalog): This guesses the PostgreSQL type of an argument from the storage class of its value, because the arguments have no types. The typed expression tree replaces it.
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

/// What is wrong with text as input of a type: PostgreSQL's message and
/// SQLSTATE.
pub(crate) type InputError = (String, String);

/// What is wrong with `input` as text of the type that `type_name` names, or
/// None when the type reads it. `type_name` is resolved as a statement
/// resolves a type name (see `lookup_type_name`), so a name of no type fails
/// with 42704. `pg_input_is_valid` and `pg_input_error_info` both read this.
pub(crate) fn validate_pg_input(
    conn: &Arc<Connection>,
    input: &str,
    type_name: &str,
) -> Result<Option<InputError>> {
    let ty = lookup_type_name(conn, type_name)?;
    // FIXME(pg-catalog): The text of an array, of an enum or a domain, and of a row type is not checked, because none of them has an INPUT. An INPUT for each type made by CREATE TYPE and for arrays replaces this refusal (see `Type::input`).
    let element_name = match ty.kind {
        NamedTypeKind::BuiltIn { typname, .. } => typname,
        NamedTypeKind::Custom | NamedTypeKind::RowOfRelation => ty.written_name.as_str(),
    };
    let not_supported = || {
        sqlstate_error(
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            format!(
                "checking the input of type {element_name}{} is not supported",
                "[]".repeat(ty.array_dimensions as usize)
            ),
            None,
        )
    };
    let NamedTypeKind::BuiltIn { oid, typname } = ty.kind else {
        return Err(not_supported());
    };
    if ty.array_dimensions > 0 {
        return Err(not_supported());
    }
    if let Some(read) = type_input(oid) {
        return Ok(input_error_of(read(input)));
    }
    let modifier = ty
        .modifiers
        .first()
        .and_then(|modifier| usize::try_from(*modifier).ok());
    hand_written_input_error(typname, modifier, input).ok_or_else(not_supported)
}

/// What is wrong with `text` as input of the built-in type whose pg_type row
/// is named `typname`, for a caller that already holds the type.
pub(crate) fn built_in_input_error(typname: &str, text: &str) -> Option<InputError> {
    let read = built_in_type_named(typname)
        .filter(|(_, _, dimensions_of_name)| *dimensions_of_name == 0)
        .and_then(|(oid, _, _)| type_input(oid));
    match read {
        Some(read) => input_error_of(read(text)),
        None => hand_written_input_error(typname, None, text).flatten(),
    }
}

fn input_error_of(read: Result<Value>) -> Option<InputError> {
    match read {
        Ok(_) => None,
        Err(LimboError::Dialect(error)) => Some((error.message, error.code.to_string())),
        Err(other) => Some((
            other.to_string(),
            SQLSTATE_INVALID_TEXT_REPRESENTATION.to_string(),
        )),
    }
}

/// FIXME(pg-catalog): This hand-codes the input function (typinput) of each built-in type that has no INPUT yet, by the name of its pg_type row. An arm goes away when its type gets an INPUT (see `type_input`).
/// What is wrong with `input` as text of the built-in type `typname`, with
/// the length `type_mod` when the type name gave one. The outer None is a
/// type this function does not read.
fn hand_written_input_error(
    typname: &str,
    type_mod: Option<usize>,
    input: &str,
) -> Option<Option<InputError>> {
    let trimmed = input.trim();
    Some(match typname {
        "float4" => float_input_error(input, "real", true),
        "float8" => float_input_error(input, "double precision", false),
        "numeric" => match trimmed.parse::<f64>() {
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
        "varchar" => {
            if let Some(max_len) = type_mod {
                if trimmed.chars().count() > max_len {
                    return Some(Some((
                        format!("value too long for type character varying({max_len})"),
                        "22001".to_string(),
                    )));
                }
            }
            None
        }
        "bpchar" => {
            if let Some(max_len) = type_mod {
                if trimmed.chars().count() > max_len {
                    return Some(Some((
                        format!("value too long for type character({max_len})"),
                        "22001".to_string(),
                    )));
                }
            }
            None
        }
        "time" => check_temporal_input(input, "time"),
        "json" | "jsonb" => {
            if is_valid_json(trimmed) {
                None
            } else {
                let type_label = if typname == "jsonb" { "jsonb" } else { "json" };
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
                        return Some(Some((
                            format!("invalid input syntax for type {typname}: \"{input}\""),
                            "22P02".to_string(),
                        )));
                    }
                }
                None
            } else {
                Some((
                    format!("invalid input syntax for type {typname}: \"{input}\""),
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
        _ => return None,
    })
}

/// The error of text that is not a float8 or float4, or is too large for it.
/// `inf` and `infinity` are valid, with or without a sign, in any case.
fn float_input_error(input: &str, type_name: &str, single: bool) -> Option<(String, String)> {
    let trimmed = input.trim();
    let is_infinity_word = matches!(
        trimmed
            .trim_start_matches(['+', '-'])
            .to_ascii_lowercase()
            .as_str(),
        "inf" | "infinity"
    );
    match trimmed.parse::<f64>() {
        Ok(_) if is_infinity_word => None,
        Ok(v) if v.is_infinite() || (single && (v as f32).is_infinite()) => Some((
            format!("value \"{input}\" is out of range for type {type_name}"),
            "22003".to_string(),
        )),
        Ok(_) => None,
        Err(_) => Some((
            format!("invalid input syntax for type {type_name}: \"{input}\""),
            "22P02".to_string(),
        )),
    }
}

/// The value of a PostgreSQL boolean literal such as 'yes', 'off' or 'tr',
/// with the rules of PostgreSQL's boolean input: white space around the text
/// and the case of the letters do not matter, and the start of true, false,
/// yes or no is enough. For on and off, the first two letters are necessary.
/// None when the text is not a boolean literal.
pub fn parse_pg_bool(text: &str) -> Option<bool> {
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

/// Reads `input` as the date or time type `type_name` does when it stores a
/// value, so `pg_input_is_valid` and the type agree on what is valid.
fn check_temporal_input(input: &str, type_name: &'static str) -> Option<(String, String)> {
    match exec_temporal_encode(
        &Value::build_text(input.to_string()),
        &Value::build_text(type_name),
    ) {
        Ok(_) => None,
        Err(LimboError::InvalidDate(message)) => {
            Some((message, SQLSTATE_INVALID_DATETIME_FORMAT.to_string()))
        }
        Err(LimboError::DatetimeFieldOverflow(message)) => {
            Some((message, SQLSTATE_DATETIME_FIELD_OVERFLOW.to_string()))
        }
        Err(other) => unreachable!("temporal_encode of text failed with {other}"),
    }
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
    use super::*;

    #[test]
    fn role_name_of_a_role_that_is_not_in_the_catalog_is_an_error() {
        let with_alice = RoleCatalog::new(
            &[vec![
                Value::from_i64(1),
                Value::build_text("alice"),
                Value::from_i64(0),
                Value::from_i64(1),
                Value::from_i64(0),
            ]],
            &[],
        )
        .unwrap();
        let without_alice = RoleCatalog::new(&[], &[]).unwrap();
        let alice = with_alice.get_by_name("alice").unwrap().id;
        assert_eq!(role_name_in(&with_alice, alice).unwrap(), "alice");

        let error = role_name_in(&without_alice, alice).unwrap_err();
        assert_eq!(error.to_string(), "invalid role OID: 16385");
        assert_eq!(
            role_name_in(&without_alice, RoleId::SUPERUSER).unwrap(),
            "postgres"
        );
    }

    #[test]
    fn array_to_json_writes_infinite_floats_as_strings() {
        let record = turso_core::types::ImmutableRecord::from_values(
            &[
                Value::from_f64(f64::INFINITY),
                Value::from_f64(f64::NEG_INFINITY),
                Value::from_f64(1.5),
            ],
            3,
        )
        .unwrap();
        let layout = JsonArrayLayout {
            element: JsonElement::Number,
            dimensions: 1,
            separator: ",",
        };
        let mut json = String::new();
        write_json_array(&Value::Blob(record.as_blob().clone()), &layout, &mut json).unwrap();
        assert_eq!(json, r#"["Infinity","-Infinity",1.5]"#);
    }

    #[test]
    fn float8_text_is_postgres_float8_output() {
        for (value, text) in [
            (1.0, "1"),
            (-0.0, "-0"),
            (0.1 + 0.2, "0.30000000000000004"),
            (1e-4, "0.0001"),
            (1.5e-5, "1.5e-05"),
            (123456789012345.0, "123456789012345"),
            (1e15, "1e+15"),
            (1e20, "1e+20"),
            (-1.25e300, "-1.25e+300"),
            (f64::MAX, "1.7976931348623157e+308"),
        ] {
            assert_eq!(float8_text(value), text, "{value:e}");
        }
    }
}
