//! PostgreSQL's `extract` and `date_trunc` on `date`, `timestamp` and
//! `timestamptz`, read and written with `turso_core::Temporal`. The session
//! time zone is UTC.

use crate::error::sqlstate_error;
use crate::functions::pg_scalar_function;
use chrono::{Datelike, Days, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Timelike};
use turso_core::dialect::ScalarFuncContext;
use turso_core::native_ext::{ArgumentType, FunctionArity, ReturnType};
use turso_core::{LimboError, OpenOptions, Register, Result, Temporal, TemporalType, Value};
use turso_pg_parser::sqlstate::SQLSTATE_FEATURE_NOT_SUPPORTED;

pub(crate) fn register_datetime_functions(options: OpenOptions) -> OpenOptions {
    options
        .extension_function(
            pg_scalar_function("extract", FunctionArity::Exact(2), true, exec_extract)
                .receiving_argument_types()
                .choosing_result_type(extract_result_type),
        )
        .extension_function(
            pg_scalar_function("date_trunc", FunctionArity::Exact(2), true, exec_date_trunc)
                .receiving_argument_types()
                .choosing_result_type(date_trunc_result_type),
        )
}

fn extract_result_type(args: &[ArgumentType]) -> Result<ReturnType> {
    source_type("extract", args)?;
    Ok(ReturnType::Custom("numeric"))
}

fn date_trunc_result_type(args: &[ArgumentType]) -> Result<ReturnType> {
    let (result_type, _) = truncated_type(source_type("date_trunc", args)?);
    Ok(ReturnType::Custom(result_type.type_name()))
}

/// The type `date_trunc` returns for a source of type `source`, and its values.
/// PostgreSQL truncates a `date` as the `timestamptz` it casts it to.
fn truncated_type(source: TemporalType) -> (TemporalType, fn(NaiveDateTime) -> Temporal) {
    match source {
        TemporalType::Timestamp => (TemporalType::Timestamp, Temporal::Timestamp),
        TemporalType::Date | TemporalType::Timestamptz => {
            (TemporalType::Timestamptz, Temporal::Timestamptz)
        }
        TemporalType::Time => unreachable!("source_type refuses a time"),
    }
}

/// The type of `source` in `function(unit, source)`. The unit must be a string
/// constant and the source a date, timestamp or timestamptz.
fn source_type(function: &str, args: &[ArgumentType]) -> Result<TemporalType> {
    let [unit, source] = args else {
        unreachable!("{function} is declared with two arguments");
    };
    let source = match source {
        ArgumentType::Typed(name) => TemporalType::from_name(name),
        ArgumentType::StringLiteral | ArgumentType::Unknown => None,
    };
    let Some(source @ (TemporalType::Date | TemporalType::Timestamp | TemporalType::Timestamptz)) =
        source
    else {
        return Err(not_supported(format!(
            "{function} takes only a value of type date, timestamp or timestamptz, such as date '2024-01-05'"
        )));
    };
    match unit {
        ArgumentType::StringLiteral => Ok(source),
        ArgumentType::Typed(_) | ArgumentType::Unknown => Err(not_supported(format!(
            "{function} takes only a string constant as its unit, such as 'day'"
        ))),
    }
}

/// `extract(field FROM source)` as a numeric. Seconds, and the epoch of a
/// timestamp, have six decimal places, as in PostgreSQL.
fn exec_extract(_: &ScalarFuncContext<'_>, args: &[Register]) -> Result<Value> {
    let (args, types) = args.split_at(args.len() / 2);
    let ty = type_of_source(&types[1]);
    let Some(source) = read_source(&args[1], ty)? else {
        return Ok(Value::Null);
    };
    let (date, time) = match source {
        Temporal::Date(date) => (date, None),
        Temporal::Timestamp(at) | Temporal::Timestamptz(at) => (at.date(), Some(at)),
        Temporal::Time(_) => unreachable!("source_type refuses a time"),
    };
    let text = match (unit(&args[0]).as_str(), time) {
        ("year", _) => date.year().to_string(),
        ("quarter", _) => (date.month0() / 3 + 1).to_string(),
        ("month", _) => date.month().to_string(),
        ("week", _) => date.iso_week().week().to_string(),
        ("day", _) => date.day().to_string(),
        ("dow", _) => date.weekday().num_days_from_sunday().to_string(),
        ("doy", _) => date.ordinal().to_string(),
        ("epoch", None) => midnight(date).and_utc().timestamp().to_string(),
        ("epoch", Some(at)) => seconds_text(at.and_utc().timestamp_micros()),
        ("hour", Some(at)) => at.hour().to_string(),
        ("minute", Some(at)) => at.minute().to_string(),
        ("second", Some(at)) => {
            seconds_text(i64::from(at.second()) * 1_000_000 + i64::from(at.nanosecond() / 1_000))
        }
        (field, _) => {
            let ty = ty.type_name();
            return Err(not_supported(format!(
                "extract({field} FROM {ty}) is not supported"
            )));
        }
    };
    Ok(Value::build_text(text))
}

/// `date_trunc(unit, source)`: the source with every field smaller than the
/// unit set to its lowest value. Weeks start on Monday.
fn exec_date_trunc(_: &ScalarFuncContext<'_>, args: &[Register]) -> Result<Value> {
    let (args, types) = args.split_at(args.len() / 2);
    let ty = type_of_source(&types[1]);
    let at = match read_source(&args[1], ty)? {
        None => return Ok(Value::Null),
        Some(Temporal::Date(date)) => midnight(date),
        Some(Temporal::Timestamp(at) | Temporal::Timestamptz(at)) => at,
        Some(Temporal::Time(_)) => unreachable!("source_type refuses a time"),
    };
    let date = at.date();
    let first_day = |month: u32| {
        let Some(day) = NaiveDate::from_ymd_opt(date.year(), month, 1) else {
            unreachable!("the first of month {month} is a date");
        };
        midnight(day)
    };
    let truncated = match unit(&args[0]).as_str() {
        "year" => first_day(1),
        "quarter" => first_day(date.month0() / 3 * 3 + 1),
        "month" => first_day(date.month()),
        "week" => midnight(date - Days::new(date.weekday().num_days_from_monday().into())),
        "day" => midnight(date),
        "hour" => midnight(date) + TimeDelta::hours(at.hour().into()),
        "minute" => midnight(date) + TimeDelta::minutes((at.hour() * 60 + at.minute()).into()),
        "second" => midnight(date) + TimeDelta::seconds(at.num_seconds_from_midnight().into()),
        unit => {
            return Err(not_supported(format!(
                "date_trunc('{unit}', ...) is not supported"
            )))
        }
    };
    let (_, value_of_result_type) = truncated_type(ty);
    Ok(Value::build_text(value_of_result_type(truncated).to_text()))
}

/// The type of a call's source, from the type name that follows the arguments.
fn type_of_source(type_name: &Register) -> TemporalType {
    let Some(ty) = type_name
        .get_value()
        .to_text()
        .and_then(TemporalType::from_name)
    else {
        unreachable!("source_type accepts only a date or timestamp type");
    };
    ty
}

/// The source value, or `None` for NULL.
fn read_source(value: &Register, ty: TemporalType) -> Result<Option<Temporal>> {
    match value.get_value() {
        Value::Null => Ok(None),
        Value::Text(text) => Temporal::read_stored(text.as_str(), ty).map(Some),
        other => Err(LimboError::InternalError(format!(
            "a {} value is stored as text, not as {other:?}",
            ty.type_name()
        ))),
    }
}

/// The field or unit, lowercased as PostgreSQL reads it.
fn unit(arg: &Register) -> String {
    let Value::Text(unit) = arg.get_value() else {
        unreachable!("source_type accepts only a string constant as the unit");
    };
    unit.as_str().to_ascii_lowercase()
}

fn midnight(date: NaiveDate) -> NaiveDateTime {
    date.and_time(NaiveTime::MIN)
}

/// `micros` as seconds with six decimal places, as PostgreSQL prints them:
/// `-0.500000`.
fn seconds_text(micros: i64) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let micros = micros.unsigned_abs();
    format!("{sign}{}.{:06}", micros / 1_000_000, micros % 1_000_000)
}

fn not_supported(message: impl Into<String>) -> LimboError {
    sqlstate_error(SQLSTATE_FEATURE_NOT_SUPPORTED, message)
}
