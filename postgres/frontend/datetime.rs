use chrono::{Datelike, NaiveDate, Utc};
use turso_core::{LimboError, Result, Value};

const USECS_PER_SEC: i64 = 1_000_000;
const USECS_PER_MINUTE: i64 = 60 * USECS_PER_SEC;
const USECS_PER_HOUR: i64 = 60 * USECS_PER_MINUTE;
const USECS_PER_DAY: i64 = 24 * USECS_PER_HOUR;
const DAYS_PER_MONTH: i32 = 30;
const MAX_TZDISP_HOUR: i32 = 15;
const UNIX_EPOCH_DAYS_FROM_CE: i64 = 719_163;
const JULIAN_DAYS_FROM_CE: i64 = 1_721_425;
const MIN_YEAR: i64 = 1;
const MAX_YEAR: i64 = 9999;
/// 4714-11-24 BC, the earliest timestamp PostgreSQL can represent. Stored
/// values stay within years 1 to 9999, but interval arithmetic may pass
/// through earlier dates, as it does in PostgreSQL.
const PG_MIN_MICROS: i64 = -2_440_588 * USECS_PER_DAY;

const TIMESTAMPTZ: &str = "timestamp with time zone";
const TIMESTAMP: &str = "timestamp without time zone";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Timestamp {
    NegInfinity,
    Finite(i64),
    Infinity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Interval {
    NegInfinity,
    Finite { months: i32, days: i32, micros: i64 },
    Infinity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DatetimeKind {
    Date,
    Timestamp,
    TimestampTz,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DtErr {
    BadFormat,
    FieldOverflow,
    TzOverflow,
    OutOfRange,
    BadTimezone(String),
    IntervalOutOfRange,
}

fn input_error(err: DtErr, type_name: &str, input: &str) -> LimboError {
    LimboError::InvalidDate(match err {
        DtErr::BadFormat => format!("invalid input syntax for type {type_name}: \"{input}\""),
        DtErr::FieldOverflow if type_name == "interval" => {
            format!("interval field value out of range: \"{input}\"")
        }
        DtErr::FieldOverflow => format!("date/time field value out of range: \"{input}\""),
        DtErr::TzOverflow => format!("time zone displacement out of range: \"{input}\""),
        DtErr::OutOfRange if type_name == "date" => format!("date out of range: \"{input}\""),
        DtErr::OutOfRange => format!("timestamp out of range: \"{input}\""),
        DtErr::BadTimezone(name) => format!("time zone \"{name}\" not recognized"),
        DtErr::IntervalOutOfRange => "interval out of range".to_string(),
    })
}

fn timestamp_out_of_range() -> LimboError {
    LimboError::InvalidDate("timestamp out of range".to_string())
}

fn interval_out_of_range() -> LimboError {
    LimboError::InvalidDate("interval out of range".to_string())
}

/// Converts a value to the stored form of `timestamptz`: UTC text without an
/// offset, or `infinity` / `-infinity`. This is the form tursopg stored before
/// it had its own timestamptz type, so older rows compare equal to new ones.
pub(crate) fn exec_timestamptz_in(value: &Value) -> Result<Value> {
    let Some(input) = input_text(value) else {
        return Ok(Value::Null);
    };
    let ts = parse_timestamp(&input, now_micros(), true)
        .map_err(|err| input_error(err, TIMESTAMPTZ, &input))?;
    Ok(Value::build_text(format_timestamp(ts, false)))
}

/// Converts a stored `timestamptz` to the text PostgreSQL shows: the UTC time
/// with a `+00` offset.
pub(crate) fn exec_timestamptz_out(value: &Value) -> Value {
    match value {
        Value::Null => Value::Null,
        Value::Text(text) if matches!(text.as_str(), "infinity" | "-infinity") => value.clone(),
        other => Value::build_text(format!("{other}+00")),
    }
}

/// Converts a value to a `timestamptz` the way a cast does.
pub(crate) fn timestamptz_value(value: &Value) -> Result<Value> {
    Ok(exec_timestamptz_out(&exec_timestamptz_in(value)?))
}

/// Converts a value to the stored form of `timestamp`. A time zone in the
/// input is ignored, as in PostgreSQL.
pub(crate) fn exec_timestamp_in(value: &Value) -> Result<Value> {
    let Some(input) = input_text(value) else {
        return Ok(Value::Null);
    };
    let ts = parse_timestamp(&input, now_micros(), false)
        .map_err(|err| input_error(err, "timestamp", &input))?;
    Ok(Value::build_text(format_timestamp(ts, false)))
}

/// Converts a value to the stored form of `date`: `YYYY-MM-DD`, `infinity` or
/// `-infinity`. A time and time zone in the input are ignored.
pub(crate) fn exec_date_in(value: &Value) -> Result<Value> {
    let Some(input) = input_text(value) else {
        return Ok(Value::Null);
    };
    let date = parse_date(&input, now_micros()).map_err(|err| input_error(err, "date", &input))?;
    Ok(Value::build_text(format_date(date)))
}

/// Converts a value to the stored form of `time`: `HH:MM:SS` with the
/// fraction's trailing zeros removed. A time zone in the input is ignored.
pub(crate) fn exec_time_in(value: &Value) -> Result<Value> {
    let Some(input) = input_text(value) else {
        return Ok(Value::Null);
    };
    let micros =
        parse_time(&input, now_micros()).map_err(|err| input_error(err, "time", &input))?;
    Ok(Value::build_text(format_time(micros)))
}

/// The text a date/time input function reads, or None for NULL.
fn input_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Text(text) => Some(text.as_str().to_string()),
        other => Some(other.to_string()),
    }
}

pub(crate) fn error_message(error: &LimboError) -> String {
    match error {
        LimboError::InvalidDate(message) => message.clone(),
        other => other.to_string(),
    }
}

pub(crate) fn error_sqlstate(message: &str) -> &'static str {
    if message.starts_with("interval field value out of range") {
        "22015"
    } else if message.starts_with("date/time field value out of range")
        || message.starts_with("timestamp out of range")
        || message.starts_with("date out of range")
        || message == "cannot subtract infinite dates"
        || message == "cannot add infinite interval to time"
        || message == "cannot subtract infinite interval from time"
        || message.starts_with("interval out of range")
    {
        "22008"
    } else if message.starts_with("operator does not exist") {
        "42883"
    } else if message.starts_with("time zone displacement out of range") {
        "22009"
    } else if message.starts_with("time zone \"") && message.ends_with("not recognized") {
        "22023"
    } else {
        "22007"
    }
}

pub(crate) fn now_text() -> String {
    timestamptz_text(now_micros())
}

pub(crate) fn timestamptz_text(micros: i64) -> String {
    format_timestamp(Timestamp::Finite(micros), true)
}

/// `+` written in SQL. `lhs_type` and `rhs_type` are the operand types the
/// query names (`date`, `time`, `timestamp`, `timestamptz`, `interval`),
/// `column` for a column, whose value is a time when it has the stored form of
/// a time, or "" when only the stored value can tell.
pub(crate) fn exec_typed_datetime_pl(
    lhs: &Value,
    rhs: &Value,
    lhs_type: &str,
    rhs_type: &str,
) -> Result<Value> {
    if matches!(lhs, Value::Null) || matches!(rhs, Value::Null) {
        return Ok(Value::Null);
    }
    let has_date = lhs_type == "date" || rhs_type == "date";
    if let Some(result) = date_pl_integer(lhs, rhs, has_date) {
        return result;
    }
    datetime_pl(
        classify_declared(lhs, lhs_type)?,
        classify_declared(rhs, rhs_type)?,
    )
}

/// `+` for a `time` column. See [exec_typed_datetime_pl].
pub(crate) fn exec_time_pl(lhs: &Value, rhs: &Value) -> Result<Value> {
    exec_typed_datetime_pl(lhs, rhs, COLUMN, COLUMN)
}

/// `-` for a `time` column. See [exec_typed_datetime_pl].
pub(crate) fn exec_time_mi(lhs: &Value, rhs: &Value) -> Result<Value> {
    exec_typed_datetime_mi(lhs, rhs, COLUMN, COLUMN)
}

const COLUMN: &str = "column";

/// `-` written in SQL. See [exec_typed_datetime_pl].
pub(crate) fn exec_typed_datetime_mi(
    lhs: &Value,
    rhs: &Value,
    lhs_type: &str,
    rhs_type: &str,
) -> Result<Value> {
    if lhs_type == "date" && rhs_type == "date" {
        return exec_date_mi(lhs, rhs);
    }
    if matches!(lhs, Value::Null) || matches!(rhs, Value::Null) {
        return Ok(Value::Null);
    }
    let has_date = lhs_type == "date" || rhs_type == "date";
    if let Some(result) = date_mi_integer(lhs, rhs, has_date) {
        return result;
    }
    datetime_mi(
        classify_declared(lhs, lhs_type)?,
        classify_declared(rhs, rhs_type)?,
    )
}

/// `*` written in SQL with an operand that is an interval or a datetime: an
/// interval times a number, in either order. See [exec_typed_datetime_pl] for
/// the type arguments.
pub(crate) fn exec_typed_interval_mul(
    lhs: &Value,
    rhs: &Value,
    lhs_type: &str,
    rhs_type: &str,
) -> Result<Value> {
    if matches!(lhs, Value::Null) || matches!(rhs, Value::Null) {
        return Ok(Value::Null);
    }
    let result = match (
        classify_factor(lhs, lhs_type)?,
        classify_factor(rhs, rhs_type)?,
    ) {
        (Factor::Interval(span), Factor::Number(factor, _))
        | (Factor::Number(factor, _), Factor::Interval(span)) => interval_mul(span, factor)?,
        (a, b) => return Err(factor_operator_does_not_exist(&a, "*", &b)),
    };
    Ok(Value::build_text(format_interval(result)))
}

/// `/` written in SQL with an operand that is an interval or a datetime: an
/// interval divided by a number.
pub(crate) fn exec_typed_interval_div(
    lhs: &Value,
    rhs: &Value,
    lhs_type: &str,
    rhs_type: &str,
) -> Result<Value> {
    if matches!(lhs, Value::Null) || matches!(rhs, Value::Null) {
        return Ok(Value::Null);
    }
    let result = match (
        classify_factor(lhs, lhs_type)?,
        classify_factor(rhs, rhs_type)?,
    ) {
        (Factor::Interval(span), Factor::Number(factor, _)) => interval_div(span, factor)?,
        (a, b) => return Err(factor_operator_does_not_exist(&a, "/", &b)),
    };
    Ok(Value::build_text(format_interval(result)))
}

/// An operand of `*` or `/`. A quoted string without a type, such as the
/// `'1.5'` in `interval '1 day' * '1.5'`, is a number when it reads as one,
/// as PostgreSQL resolves it to `double precision`.
enum Factor {
    Interval(Interval),
    Number(f64, &'static str),
    Other(Operand),
}

fn classify_factor(value: &Value, declared: &str) -> Result<Factor> {
    match value {
        Value::Numeric(turso_core::Numeric::Integer(n)) => {
            return Ok(Factor::Number(*n as f64, "integer"))
        }
        Value::Numeric(_) => {
            let number = value.as_float();
            return Ok(Factor::Number(number, "double precision"));
        }
        Value::Text(text) if declared.is_empty() => {
            if let Ok(number) = text.as_str().trim().parse::<f64>() {
                return Ok(Factor::Number(number, "double precision"));
            }
        }
        _ => {}
    }
    Ok(match classify_declared(value, declared)? {
        Operand::Interval(span) => Factor::Interval(span),
        Operand::Infinite(positive) if declared.is_empty() => {
            Factor::Interval(infinite_interval(positive))
        }
        other => Factor::Other(other),
    })
}

fn factor_operator_does_not_exist(lhs: &Factor, op: &str, rhs: &Factor) -> LimboError {
    let type_name = |factor: &Factor| match factor {
        Factor::Interval(_) => "interval",
        Factor::Number(_, name) => name,
        Factor::Other(operand) => operand_type_name(operand),
    };
    LimboError::InvalidDate(format!(
        "operator does not exist: {} {op} {}",
        type_name(lhs),
        type_name(rhs)
    ))
}

/// `+` for timestamps, dates and intervals stored as text.
pub(crate) fn exec_datetime_pl(lhs: &Value, rhs: &Value) -> Result<Value> {
    exec_typed_datetime_pl(lhs, rhs, "", "")
}

/// `-` for timestamps, dates and intervals stored as text.
pub(crate) fn exec_datetime_mi(lhs: &Value, rhs: &Value) -> Result<Value> {
    exec_typed_datetime_mi(lhs, rhs, "", "")
}

fn datetime_pl(a: Operand, b: Operand) -> Result<Value> {
    let result = match (a, b) {
        (Operand::Datetime(ts, kind), Operand::Interval(iv))
        | (Operand::Interval(iv), Operand::Datetime(ts, kind)) => {
            datetime_text(timestamp_pl_interval(ts, iv)?, kind)
        }
        (Operand::Datetime(ts, kind), Operand::Infinite(inf))
        | (Operand::Infinite(inf), Operand::Datetime(ts, kind)) => {
            datetime_text(timestamp_pl_interval(ts, infinite_interval(inf))?, kind)
        }
        (Operand::Infinite(inf), Operand::Interval(iv))
        | (Operand::Interval(iv), Operand::Infinite(inf)) => datetime_text(
            timestamp_pl_interval(infinite_timestamp(inf), iv)?,
            DatetimeKind::TimestampTz,
        ),
        (Operand::Infinite(a), Operand::Infinite(b)) => datetime_text(
            timestamp_pl_interval(infinite_timestamp(a), infinite_interval(b))?,
            DatetimeKind::TimestampTz,
        ),
        (Operand::Interval(a), Operand::Interval(b)) => format_interval(interval_pl(a, b)?),
        (Operand::Time(time), Operand::Interval(iv))
        | (Operand::Interval(iv), Operand::Time(time)) => format_time(time_pl_interval(
            time,
            iv,
            1,
            "cannot add infinite interval to time",
        )?),
        (Operand::Time(time), Operand::Datetime(date, DatetimeKind::Date))
        | (Operand::Datetime(date, DatetimeKind::Date), Operand::Time(time)) => datetime_text(
            timestamp_pl_interval(date, time_interval(time))?,
            DatetimeKind::Timestamp,
        ),
        (a, b) => return Err(operator_does_not_exist(&a, "+", &b)),
    };
    Ok(Value::build_text(result))
}

fn datetime_mi(a: Operand, b: Operand) -> Result<Value> {
    let result = match (a, b) {
        (
            Operand::Datetime(Timestamp::Finite(a), DatetimeKind::Date),
            Operand::Datetime(Timestamp::Finite(b), DatetimeKind::Date),
        ) => return Ok(Value::from_i64((a - b) / USECS_PER_DAY)),
        (Operand::Datetime(a, _), Operand::Datetime(b, _)) => format_interval(timestamp_mi(a, b)?),
        (Operand::Datetime(a, _), Operand::Infinite(inf)) => {
            format_interval(timestamp_mi(a, infinite_timestamp(inf))?)
        }
        (Operand::Infinite(inf), Operand::Datetime(b, _)) => {
            format_interval(timestamp_mi(infinite_timestamp(inf), b)?)
        }
        (Operand::Infinite(a), Operand::Infinite(b)) => {
            format_interval(timestamp_mi(infinite_timestamp(a), infinite_timestamp(b))?)
        }
        (Operand::Datetime(ts, kind), Operand::Interval(iv)) => {
            datetime_text(timestamp_pl_interval(ts, interval_um(iv)?)?, kind)
        }
        (Operand::Infinite(inf), Operand::Interval(iv)) => datetime_text(
            timestamp_pl_interval(infinite_timestamp(inf), interval_um(iv)?)?,
            DatetimeKind::TimestampTz,
        ),
        (Operand::Interval(a), Operand::Interval(b)) => format_interval(interval_mi(a, b)?),
        (Operand::Interval(a), Operand::Infinite(inf)) => {
            format_interval(interval_mi(a, infinite_interval(inf))?)
        }
        (Operand::Time(a), Operand::Time(b)) => format_interval(time_interval(a - b)),
        (Operand::Time(time), Operand::Interval(iv)) => format_time(time_pl_interval(
            time,
            iv,
            -1,
            "cannot subtract infinite interval from time",
        )?),
        (Operand::Datetime(date, DatetimeKind::Date), Operand::Time(time)) => datetime_text(
            timestamp_pl_interval(date, time_interval(-time))?,
            DatetimeKind::Timestamp,
        ),
        (a, b) => return Err(operator_does_not_exist(&a, "-", &b)),
    };
    Ok(Value::build_text(result))
}

/// `time` plus `sign` times the time part of `span`, wrapped to one day.
/// The months and days of `span` are ignored, as in PostgreSQL.
fn time_pl_interval(time: i64, span: Interval, sign: i64, infinite_error: &str) -> Result<i64> {
    let Interval::Finite { micros, .. } = span else {
        return Err(LimboError::InvalidDate(infinite_error.to_string()));
    };
    Ok((time + sign * micros.rem_euclid(USECS_PER_DAY)).rem_euclid(USECS_PER_DAY))
}

fn time_interval(micros: i64) -> Interval {
    Interval::Finite {
        months: 0,
        days: 0,
        micros,
    }
}

/// `+` where at least one operand is known to be a `date`, so `infinity` and
/// `-infinity` operands are infinite dates.
pub(crate) fn exec_date_pl(lhs: &Value, rhs: &Value) -> Result<Value> {
    if matches!(lhs, Value::Null) || matches!(rhs, Value::Null) {
        return Ok(Value::Null);
    }
    if let Some(result) = date_pl_integer(lhs, rhs, true) {
        return result;
    }
    exec_datetime_pl(lhs, rhs)
}

/// `-` where at least one operand is known to be a `date`, so `infinity` and
/// `-infinity` operands are infinite dates.
pub(crate) fn exec_date_mi(lhs: &Value, rhs: &Value) -> Result<Value> {
    if matches!(lhs, Value::Null) || matches!(rhs, Value::Null) {
        return Ok(Value::Null);
    }
    if let Some(result) = date_mi_integer(lhs, rhs, true) {
        return result;
    }
    if let (Some(a), Some(b)) = (stored_date(lhs, true), stored_date(rhs, true)) {
        let (Timestamp::Finite(a), Timestamp::Finite(b)) = (a, b) else {
            return Err(LimboError::InvalidDate(
                "cannot subtract infinite dates".to_string(),
            ));
        };
        return Ok(Value::from_i64((a - b) / USECS_PER_DAY));
    }
    exec_datetime_mi(lhs, rhs)
}

/// `date + integer` and `integer + date`. Returns `None` when the operands
/// are not a date and an integer.
fn date_pl_integer(lhs: &Value, rhs: &Value, infinity_is_date: bool) -> Option<Result<Value>> {
    let (date, days, operator) = match (lhs, rhs) {
        (_, Value::Numeric(turso_core::Numeric::Integer(days))) => (lhs, *days, "date + bigint"),
        (Value::Numeric(turso_core::Numeric::Integer(days)), _) => (rhs, *days, "bigint + date"),
        _ => return None,
    };
    let date = stored_date(date, infinity_is_date)?;
    Some(integer_days(days, operator).and_then(|days| date_pl_days(date, i64::from(days))))
}

/// `date - integer`. Returns `None` when the operands are not a date and an
/// integer.
fn date_mi_integer(lhs: &Value, rhs: &Value, infinity_is_date: bool) -> Option<Result<Value>> {
    let Value::Numeric(turso_core::Numeric::Integer(days)) = rhs else {
        return None;
    };
    let date = stored_date(lhs, infinity_is_date)?;
    Some(integer_days(*days, "date - bigint").and_then(|days| date_pl_days(date, -i64::from(days))))
}

fn stored_date(value: &Value, infinity_is_date: bool) -> Option<Timestamp> {
    let Value::Text(text) = value else {
        return None;
    };
    match infinity_sign(text.as_str()) {
        Some(positive) => infinity_is_date.then(|| infinite_timestamp(positive)),
        None => match decode_stored_datetime(text.as_str())? {
            (date, DatetimeKind::Date) => Some(date),
            _ => None,
        },
    }
}

/// PostgreSQL types an integer outside the `integer` range as `bigint`, which
/// has no arithmetic operators with `date`.
fn integer_days(days: i64, operator: &str) -> Result<i32> {
    i32::try_from(days)
        .map_err(|_| LimboError::InvalidDate(format!("operator does not exist: {operator}")))
}

/// Port of PostgreSQL's `date_pli`: infinite dates are returned unchanged.
fn date_pl_days(date: Timestamp, days: i64) -> Result<Value> {
    let Timestamp::Finite(micros) = date else {
        return Ok(Value::build_text(format_date(date)));
    };
    match (micros.div_euclid(USECS_PER_DAY) + days).checked_mul(USECS_PER_DAY) {
        Some(result) if micros_in_range(result) => {
            Ok(Value::build_text(format_date(Timestamp::Finite(result))))
        }
        _ => Err(LimboError::InvalidDate("date out of range".to_string())),
    }
}

/// Formats a date or timestamp with a `to_char` pattern. Returns `None` when
/// the value is not a date or timestamp.
pub(crate) fn to_char_datetime(value: &Value, format: &str) -> Option<Value> {
    let Value::Text(text) = value else {
        return None;
    };
    let text = text.as_str();
    if infinity_sign(text).is_some() {
        return Some(Value::Null);
    }
    match decode_stored_datetime(text) {
        Some((Timestamp::Finite(micros), kind)) => Some(Value::build_text(format_to_char(
            micros,
            kind != DatetimeKind::Timestamp,
            format,
        ))),
        _ => None,
    }
}

/// Converts a text-format parameter sent with a date, time, timestamp or
/// timestamptz type OID to a value of that type, as PostgreSQL runs the type's
/// input function when it binds the parameter. Returns `None` for other OIDs.
pub fn datetime_param_in(text: &str, oid: u32) -> Option<Result<Value>> {
    let value = Value::build_text(text.to_string());
    match oid {
        DATE_OID => Some(exec_date_in(&value)),
        TIME_OID => Some(exec_time_in(&value)),
        TIMESTAMP_OID => Some(exec_timestamp_in(&value)),
        TIMESTAMPTZ_OID => Some(timestamptz_value(&value)),
        _ => None,
    }
}

/// Adds a cast to every `$N` parameter the client declared as date, time,
/// timestamp or timestamptz (`param_oids[N - 1]`), so `$1 + 1` adds a day to
/// a date parameter instead of adding numbers.
pub fn cast_datetime_parameters(sql: &str, param_oids: &[Option<u32>]) -> Result<String> {
    let type_of = |param_number: usize| match param_oids.get(param_number.checked_sub(1)?)? {
        Some(DATE_OID) => Some("date"),
        Some(TIME_OID) => Some("time"),
        Some(TIMESTAMP_OID) => Some("timestamp"),
        Some(TIMESTAMPTZ_OID) => Some("timestamptz"),
        _ => None,
    };
    if !(1..=param_oids.len()).any(|n| type_of(n).is_some()) {
        return Ok(sql.to_string());
    }
    turso_pg_parser::cast_parameters(sql, type_of)
        .map_err(|err| LimboError::ParseError(err.to_string()))
}

/// Converts a stored date, time, timestamp or timestamptz text to the value
/// PostgreSQL sends in binary format: days since 2000-01-01 for `date`,
/// microseconds since 2000-01-01 for timestamps and microseconds since
/// midnight for `time`. Infinite values map to the integer limits. Returns
/// `None` when the text is not a stored value of that type.
pub fn datetime_wire_binary(text: &str, oid: u32) -> Option<i64> {
    match oid {
        DATE_OID => match infinity_sign(text) {
            Some(true) => Some(i64::from(i32::MAX)),
            Some(false) => Some(i64::from(i32::MIN)),
            None => match decode_stored_datetime(text)? {
                (Timestamp::Finite(micros), DatetimeKind::Date) => {
                    Some(micros.div_euclid(USECS_PER_DAY) - POSTGRES_EPOCH_DAYS)
                }
                _ => None,
            },
        },
        TIME_OID => stored_time_micros(text),
        TIMESTAMP_OID | TIMESTAMPTZ_OID => match infinity_sign(text) {
            Some(true) => Some(i64::MAX),
            Some(false) => Some(i64::MIN),
            None => match decode_stored_datetime(text)? {
                (Timestamp::Finite(micros), _) => {
                    Some(micros - POSTGRES_EPOCH_DAYS * USECS_PER_DAY)
                }
                _ => None,
            },
        },
        _ => None,
    }
}

const DATE_OID: u32 = 1082;
const TIME_OID: u32 = 1083;
const TIMESTAMP_OID: u32 = 1114;
const TIMESTAMPTZ_OID: u32 = 1184;
const POSTGRES_EPOCH_DAYS: i64 = 10_957;

/// Parses `HH:MM:SS[.f]` with up to six fraction digits, the stored form of
/// `time`. Hour 24 is allowed only as exactly `24:00:00`.
fn stored_time_micros(text: &str) -> Option<i64> {
    let (clock, fraction) = match text.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (text, None),
    };
    let mut parts = clock.split(':');
    let mut two_digits = |max: i64| -> Option<i64> {
        let part = parts.next()?;
        if part.len() != 2 || !part.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let value: i64 = part.parse().ok()?;
        (value <= max).then_some(value)
    };
    let (hour, minute, second) = (two_digits(24)?, two_digits(59)?, two_digits(59)?);
    if parts.next().is_some() {
        return None;
    }
    let fraction_micros = match fraction {
        None => 0,
        Some(digits) => {
            if digits.is_empty() || digits.len() > 6 || !digits.bytes().all(|c| c.is_ascii_digit())
            {
                return None;
            }
            format!("{digits:0<6}").parse::<i64>().ok()?
        }
    };
    let micros = hour * USECS_PER_HOUR
        + minute * USECS_PER_MINUTE
        + second * USECS_PER_SEC
        + fraction_micros;
    (micros <= USECS_PER_DAY).then_some(micros)
}

pub(crate) fn now_micros() -> i64 {
    Utc::now().timestamp_micros()
}

enum Operand {
    Datetime(Timestamp, DatetimeKind),
    Time(i64),
    Interval(Interval),
    Infinite(bool),
    Number(&'static str),
}

fn classify(value: &Value) -> Result<Operand> {
    let text = match value {
        Value::Text(text) => text.as_str(),
        Value::Numeric(turso_core::Numeric::Integer(_)) => return Ok(Operand::Number("integer")),
        Value::Numeric(_) => return Ok(Operand::Number("double precision")),
        _ => return Ok(Operand::Number("bytea")),
    };
    if let Some(positive) = infinity_sign(text) {
        return Ok(Operand::Infinite(positive));
    }
    if let Some((ts, kind)) = decode_stored_datetime(text) {
        return Ok(Operand::Datetime(ts, kind));
    }
    parse_interval(text)
        .map(Operand::Interval)
        .map_err(|err| input_error(err, "interval", text))
}

/// Like [classify], but an operand whose type the query names is read as that
/// type, so `interval 'infinity'` is an interval and `'infinity'::date` a date.
fn classify_declared(value: &Value, declared: &str) -> Result<Operand> {
    let Value::Text(text) = value else {
        return classify(value);
    };
    let text = text.as_str();
    let kind = match declared {
        "interval" => {
            return parse_interval(text)
                .map(Operand::Interval)
                .map_err(|err| input_error(err, "interval", text))
        }
        "time" => {
            return stored_time_micros(text)
                .map(Operand::Time)
                .ok_or_else(|| input_error(DtErr::BadFormat, "time", text))
        }
        COLUMN => {
            return match stored_time_micros(text) {
                Some(time) => Ok(Operand::Time(time)),
                None => classify(value),
            }
        }
        "date" => DatetimeKind::Date,
        "timestamp" => DatetimeKind::Timestamp,
        "timestamptz" => DatetimeKind::TimestampTz,
        _ => return classify(value),
    };
    match infinity_sign(text) {
        Some(positive) => Ok(Operand::Datetime(infinite_timestamp(positive), kind)),
        None => classify(value),
    }
}

fn infinity_sign(text: &str) -> Option<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "infinity" | "+infinity" => Some(true),
        "-infinity" => Some(false),
        _ => None,
    }
}

fn operand_type_name(operand: &Operand) -> &'static str {
    match operand {
        Operand::Datetime(_, DatetimeKind::Date) => "date",
        Operand::Datetime(_, DatetimeKind::Timestamp) => TIMESTAMP,
        Operand::Datetime(..) | Operand::Infinite(_) => TIMESTAMPTZ,
        Operand::Time(_) => "time without time zone",
        Operand::Interval(_) => "interval",
        Operand::Number(name) => name,
    }
}

fn operator_does_not_exist(lhs: &Operand, op: &str, rhs: &Operand) -> LimboError {
    LimboError::InvalidDate(format!(
        "operator does not exist: {} {op} {}",
        operand_type_name(lhs),
        operand_type_name(rhs)
    ))
}

fn infinite_timestamp(positive: bool) -> Timestamp {
    if positive {
        Timestamp::Infinity
    } else {
        Timestamp::NegInfinity
    }
}

fn infinite_interval(positive: bool) -> Interval {
    if positive {
        Interval::Infinity
    } else {
        Interval::NegInfinity
    }
}

fn datetime_text(ts: Timestamp, kind: DatetimeKind) -> String {
    format_timestamp(ts, kind == DatetimeKind::TimestampTz)
}

fn parse_timestamp(
    input: &str,
    now: i64,
    with_zone: bool,
) -> std::result::Result<Timestamp, DtErr> {
    let decoded = decode_datetime_text(input, now)?;
    decoded_timestamp(&decoded, with_zone)
}

/// Recognizes a stored date, timestamp or timestamptz operand. Only text that
/// starts with a `YYYY-` date is considered, so interval text such as
/// `1 day` or `now` is never mistaken for a timestamp.
fn decode_stored_datetime(text: &str) -> Option<(Timestamp, DatetimeKind)> {
    let trimmed = text.trim_start_matches(|c: char| c.is_ascii() && is_c_space(c as u8));
    let digits = trimmed.bytes().take_while(u8::is_ascii_digit).count();
    if digits < 4 || trimmed.as_bytes().get(digits) != Some(&b'-') {
        return None;
    }
    let decoded = decode_datetime_text(text, now_micros()).ok()?;
    let kind = if decoded.fmask & dtk_m(TZ) != 0 {
        DatetimeKind::TimestampTz
    } else if decoded.fmask & DTK_TIME_M != 0 {
        DatetimeKind::Timestamp
    } else {
        DatetimeKind::Date
    };
    let ts = decoded_timestamp(&decoded, true).ok()?;
    Some((ts, kind))
}

fn decode_datetime_text(input: &str, now: i64) -> std::result::Result<DecodedDatetime, DtErr> {
    let mut fields = parse_date_time(until_nul(input.as_bytes()), TIMESTAMP_BUFFER_LEN)?;
    decode_date_time(&mut fields, now)
}

fn decoded_timestamp(
    decoded: &DecodedDatetime,
    with_zone: bool,
) -> std::result::Result<Timestamp, DtErr> {
    match decoded.dtype {
        DateType::Epoch => Ok(Timestamp::Finite(0)),
        DateType::Late => Ok(Timestamp::Infinity),
        DateType::Early => Ok(Timestamp::NegInfinity),
        DateType::Date => {
            let tm = &decoded.tm;
            if !is_valid_julian(tm.year, tm.mon) {
                return Err(DtErr::OutOfRange);
            }
            let days = i64::from(date2j(tm.year, tm.mon, tm.mday) - UNIX_EPOCH_JDATE);
            let time = time2t(tm.hour, tm.min, tm.sec, decoded.fsec);
            let mut micros = days
                .checked_mul(USECS_PER_DAY)
                .and_then(|micros| micros.checked_add(time))
                .ok_or(DtErr::OutOfRange)?;
            if with_zone {
                micros = micros
                    .checked_add(i64::from(decoded.tz) * USECS_PER_SEC)
                    .ok_or(DtErr::OutOfRange)?;
            }
            if !micros_in_range(micros) {
                return Err(DtErr::OutOfRange);
            }
            Ok(Timestamp::Finite(micros))
        }
    }
}

fn parse_date(input: &str, now: i64) -> std::result::Result<Timestamp, DtErr> {
    let mut fields = parse_date_time(until_nul(input.as_bytes()), DATE_BUFFER_LEN)?;
    let decoded = decode_date_time(&mut fields, now)?;
    let tm = match decoded.dtype {
        DateType::Epoch => return Ok(Timestamp::Finite(0)),
        DateType::Late => return Ok(Timestamp::Infinity),
        DateType::Early => return Ok(Timestamp::NegInfinity),
        DateType::Date => decoded.tm,
    };
    if !(MIN_YEAR..=MAX_YEAR).contains(&i64::from(tm.year)) {
        return Err(DtErr::OutOfRange);
    }
    let days = i64::from(date2j(tm.year, tm.mon, tm.mday) - UNIX_EPOCH_JDATE);
    Ok(Timestamp::Finite(days * USECS_PER_DAY))
}

fn parse_time(input: &str, now: i64) -> std::result::Result<i64, DtErr> {
    let mut fields = parse_date_time(until_nul(input.as_bytes()), DATE_BUFFER_LEN)?;
    let (tm, fsec) = decode_time_only(&mut fields, now)?;
    Ok(time2t(tm.hour, tm.min, tm.sec, fsec))
}

fn until_nul(bytes: &[u8]) -> &[u8] {
    match bytes.iter().position(|&c| c == 0) {
        Some(end) => &bytes[..end],
        None => bytes,
    }
}

/// Reads byte `i`, returning 0 past the end, like reading a C string.
fn at(text: &[u8], i: usize) -> u8 {
    text.get(i).copied().unwrap_or(0)
}

fn is_c_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

const MAXDATEFIELDS: usize = 25;
const TIMESTAMP_BUFFER_LEN: usize = 128 + MAXDATEFIELDS;
const DATE_BUFFER_LEN: usize = 128 + 1;
const INTERVAL_BUFFER_LEN: usize = 256;
const TOKMAXLEN: usize = 10;
const MONTHS_PER_YEAR: i32 = 12;
const UNIX_EPOCH_JDATE: i32 = 2_440_588;

const RESERV: i32 = 0;
const MONTH: i32 = 1;
const YEAR: i32 = 2;
const DAY: i32 = 3;
const TZ: i32 = 5;
const DTZ: i32 = 6;
const IGNORE_DTF: i32 = 8;
const AMPM: i32 = 9;
const HOUR: i32 = 10;
const MINUTE: i32 = 11;
const SECOND: i32 = 12;
const MILLISECOND: i32 = 13;
const MICROSECOND: i32 = 14;
const DOY: i32 = 15;
const DOW: i32 = 16;
const UNITS: i32 = 17;
const ADBC: i32 = 18;
const AGO: i32 = 19;
const ISOTIME: i32 = 23;
const WEEK: i32 = 24;
const DECADE: i32 = 25;
const CENTURY: i32 = 26;
const MILLENNIUM: i32 = 27;
const DTZMOD: i32 = 28;
const UNKNOWN_FIELD: i32 = 31;

const DTK_TIME: i32 = 3;
const DTK_EARLY: i32 = 9;
const DTK_LATE: i32 = 10;
const DTK_EPOCH: i32 = 11;
const DTK_NOW: i32 = 12;
const DTK_YESTERDAY: i32 = 13;
const DTK_TODAY: i32 = 14;
const DTK_TOMORROW: i32 = 15;
const DTK_ZULU: i32 = 16;
const DTK_SECOND: i32 = 18;
const DTK_MINUTE: i32 = 19;
const DTK_HOUR: i32 = 20;
const DTK_DAY: i32 = 21;
const DTK_WEEK: i32 = 22;
const DTK_MONTH: i32 = 23;
const DTK_QUARTER: i32 = 24;
const DTK_YEAR: i32 = 25;
const DTK_DECADE: i32 = 26;
const DTK_CENTURY: i32 = 27;
const DTK_MILLENNIUM: i32 = 28;
const DTK_MILLISEC: i32 = 29;
const DTK_MICROSEC: i32 = 30;
const DTK_JULIAN: i32 = 31;
const DTK_DOW: i32 = 32;
const DTK_DOY: i32 = 33;
const DTK_TZ: i32 = 34;
const DTK_TZ_HOUR: i32 = 35;
const DTK_TZ_MINUTE: i32 = 36;
const DTK_ISOYEAR: i32 = 37;
const DTK_ISODOW: i32 = 38;

const AM: i32 = 0;
const PM: i32 = 1;
const HR24: i32 = 2;
const AD: i32 = 0;
const BC: i32 = 1;

const fn dtk_m(t: i32) -> u32 {
    1 << t
}

const DTK_ALL_SECS_M: u32 = dtk_m(SECOND) | dtk_m(MILLISECOND) | dtk_m(MICROSECOND);
const DTK_DATE_M: u32 = dtk_m(YEAR) | dtk_m(MONTH) | dtk_m(DAY);
const DTK_TIME_M: u32 = dtk_m(HOUR) | dtk_m(MINUTE) | DTK_ALL_SECS_M;

/// Keywords for timestamp input, as in PostgreSQL's `datetktbl`.
const DATE_TOKENS: &[(&str, i32, i32)] = &[
    ("+infinity", RESERV, DTK_LATE),
    ("-infinity", RESERV, DTK_EARLY),
    ("ad", ADBC, AD),
    ("allballs", RESERV, DTK_ZULU),
    ("am", AMPM, AM),
    ("apr", MONTH, 4),
    ("april", MONTH, 4),
    ("at", IGNORE_DTF, 0),
    ("aug", MONTH, 8),
    ("august", MONTH, 8),
    ("bc", ADBC, BC),
    ("d", UNITS, DTK_DAY),
    ("dec", MONTH, 12),
    ("december", MONTH, 12),
    ("dow", UNITS, DTK_DOW),
    ("doy", UNITS, DTK_DOY),
    ("dst", DTZMOD, 3600),
    ("epoch", RESERV, DTK_EPOCH),
    ("feb", MONTH, 2),
    ("february", MONTH, 2),
    ("fri", DOW, 5),
    ("friday", DOW, 5),
    ("h", UNITS, DTK_HOUR),
    ("infinity", RESERV, DTK_LATE),
    ("isodow", UNITS, DTK_ISODOW),
    ("isoyear", UNITS, DTK_ISOYEAR),
    ("j", UNITS, DTK_JULIAN),
    ("jan", MONTH, 1),
    ("january", MONTH, 1),
    ("jd", UNITS, DTK_JULIAN),
    ("jul", MONTH, 7),
    ("julian", UNITS, DTK_JULIAN),
    ("july", MONTH, 7),
    ("jun", MONTH, 6),
    ("june", MONTH, 6),
    ("m", UNITS, DTK_MONTH),
    ("mar", MONTH, 3),
    ("march", MONTH, 3),
    ("may", MONTH, 5),
    ("mm", UNITS, DTK_MINUTE),
    ("mon", DOW, 1),
    ("monday", DOW, 1),
    ("nov", MONTH, 11),
    ("november", MONTH, 11),
    ("now", RESERV, DTK_NOW),
    ("oct", MONTH, 10),
    ("october", MONTH, 10),
    ("on", IGNORE_DTF, 0),
    ("pm", AMPM, PM),
    ("s", UNITS, DTK_SECOND),
    ("sat", DOW, 6),
    ("saturday", DOW, 6),
    ("sep", MONTH, 9),
    ("sept", MONTH, 9),
    ("september", MONTH, 9),
    ("sun", DOW, 0),
    ("sunday", DOW, 0),
    ("t", ISOTIME, DTK_TIME),
    ("thu", DOW, 4),
    ("thur", DOW, 4),
    ("thurs", DOW, 4),
    ("thursday", DOW, 4),
    ("today", RESERV, DTK_TODAY),
    ("tomorrow", RESERV, DTK_TOMORROW),
    ("tue", DOW, 2),
    ("tues", DOW, 2),
    ("tuesday", DOW, 2),
    ("wed", DOW, 3),
    ("wednesday", DOW, 3),
    ("weds", DOW, 3),
    ("y", UNITS, DTK_YEAR),
    ("yesterday", RESERV, DTK_YESTERDAY),
];

/// Keywords for interval input, as in PostgreSQL's `deltatktbl`.
const INTERVAL_TOKENS: &[(&str, i32, i32)] = &[
    ("@", IGNORE_DTF, 0),
    ("ago", AGO, 0),
    ("c", UNITS, DTK_CENTURY),
    ("cent", UNITS, DTK_CENTURY),
    ("centuries", UNITS, DTK_CENTURY),
    ("century", UNITS, DTK_CENTURY),
    ("d", UNITS, DTK_DAY),
    ("day", UNITS, DTK_DAY),
    ("days", UNITS, DTK_DAY),
    ("dec", UNITS, DTK_DECADE),
    ("decade", UNITS, DTK_DECADE),
    ("decades", UNITS, DTK_DECADE),
    ("decs", UNITS, DTK_DECADE),
    ("h", UNITS, DTK_HOUR),
    ("hour", UNITS, DTK_HOUR),
    ("hours", UNITS, DTK_HOUR),
    ("hr", UNITS, DTK_HOUR),
    ("hrs", UNITS, DTK_HOUR),
    ("m", UNITS, DTK_MINUTE),
    ("microsecon", UNITS, DTK_MICROSEC),
    ("mil", UNITS, DTK_MILLENNIUM),
    ("millennia", UNITS, DTK_MILLENNIUM),
    ("millennium", UNITS, DTK_MILLENNIUM),
    ("millisecon", UNITS, DTK_MILLISEC),
    ("mils", UNITS, DTK_MILLENNIUM),
    ("min", UNITS, DTK_MINUTE),
    ("mins", UNITS, DTK_MINUTE),
    ("minute", UNITS, DTK_MINUTE),
    ("minutes", UNITS, DTK_MINUTE),
    ("mon", UNITS, DTK_MONTH),
    ("mons", UNITS, DTK_MONTH),
    ("month", UNITS, DTK_MONTH),
    ("months", UNITS, DTK_MONTH),
    ("ms", UNITS, DTK_MILLISEC),
    ("msec", UNITS, DTK_MILLISEC),
    ("msecond", UNITS, DTK_MILLISEC),
    ("mseconds", UNITS, DTK_MILLISEC),
    ("msecs", UNITS, DTK_MILLISEC),
    ("qtr", UNITS, DTK_QUARTER),
    ("quarter", UNITS, DTK_QUARTER),
    ("s", UNITS, DTK_SECOND),
    ("sec", UNITS, DTK_SECOND),
    ("second", UNITS, DTK_SECOND),
    ("seconds", UNITS, DTK_SECOND),
    ("secs", UNITS, DTK_SECOND),
    ("timezone", UNITS, DTK_TZ),
    ("timezone_h", UNITS, DTK_TZ_HOUR),
    ("timezone_m", UNITS, DTK_TZ_MINUTE),
    ("us", UNITS, DTK_MICROSEC),
    ("usec", UNITS, DTK_MICROSEC),
    ("usecond", UNITS, DTK_MICROSEC),
    ("useconds", UNITS, DTK_MICROSEC),
    ("usecs", UNITS, DTK_MICROSEC),
    ("w", UNITS, DTK_WEEK),
    ("week", UNITS, DTK_WEEK),
    ("weeks", UNITS, DTK_WEEK),
    ("y", UNITS, DTK_YEAR),
    ("year", UNITS, DTK_YEAR),
    ("years", UNITS, DTK_YEAR),
    ("yr", UNITS, DTK_YEAR),
    ("yrs", UNITS, DTK_YEAR),
];

/// The time zone abbreviations from PostgreSQL's default set that mean UTC.
/// Abbreviations with other offsets are not supported.
const UTC_ABBREVIATIONS: &[(&str, i32, i32)] = &[
    ("azost", DTZ, 0),
    ("egst", DTZ, 0),
    ("gmt", TZ, 0),
    ("uct", TZ, 0),
    ("ut", TZ, 0),
    ("utc", TZ, 0),
    ("wet", TZ, 0),
    ("z", TZ, 0),
    ("zulu", TZ, 0),
];

/// Full time zone names that mean UTC. Other zone names are not supported.
const UTC_ZONE_NAMES: &[&str] = &[
    "etc/gmt",
    "etc/gmt+0",
    "etc/gmt-0",
    "etc/gmt0",
    "etc/greenwich",
    "etc/uct",
    "etc/universal",
    "etc/utc",
    "etc/zulu",
    "gmt+0",
    "gmt-0",
    "gmt0",
];

/// Finds a keyword. Like PostgreSQL, only the first `TOKMAXLEN` characters
/// are compared, so `microseconds` matches `microsecon`.
fn lookup_token(table: &[(&str, i32, i32)], key: &[u8]) -> Option<(i32, i32)> {
    let key = &key[..key.len().min(TOKMAXLEN)];
    table.iter().find_map(|&(token, ty, value)| {
        let token = token.as_bytes();
        (&token[..token.len().min(TOKMAXLEN)] == key).then_some((ty, value))
    })
}

fn decode_special(key: &[u8]) -> (i32, i32) {
    lookup_token(DATE_TOKENS, key).unwrap_or((UNKNOWN_FIELD, 0))
}

fn decode_units(key: &[u8]) -> (i32, i32) {
    lookup_token(INTERVAL_TOKENS, key).unwrap_or((UNKNOWN_FIELD, 0))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldKind {
    Number,
    String,
    Date,
    Time,
    Tz,
    Special,
}

#[derive(Debug)]
struct Field {
    text: Vec<u8>,
    kind: FieldKind,
}

struct FieldWriter {
    text: Vec<u8>,
    used: usize,
    buffer_len: usize,
}

impl FieldWriter {
    /// PostgreSQL copies fields into a fixed-size buffer and rejects input
    /// that does not fit, counting one terminator byte per field.
    fn push(&mut self, c: u8) -> std::result::Result<(), DtErr> {
        if self.used + 1 >= self.buffer_len {
            return Err(DtErr::BadFormat);
        }
        self.used += 1;
        self.text.push(c);
        Ok(())
    }

    fn finish(&mut self, kind: FieldKind) -> Field {
        self.used += 1;
        Field {
            text: std::mem::take(&mut self.text),
            kind,
        }
    }
}

/// Port of PostgreSQL's `ParseDateTime`: splits input into fields and
/// lowercases text.
fn parse_date_time(input: &[u8], buffer_len: usize) -> std::result::Result<Vec<Field>, DtErr> {
    let mut fields = Vec::new();
    let mut w = FieldWriter {
        text: Vec::new(),
        used: 0,
        buffer_len,
    };
    let mut cp = 0;
    while at(input, cp) != 0 {
        let c = at(input, cp);
        if is_c_space(c) {
            cp += 1;
            continue;
        }
        if fields.len() >= MAXDATEFIELDS {
            return Err(DtErr::BadFormat);
        }
        let kind;
        if c.is_ascii_digit() {
            w.push(c)?;
            cp += 1;
            while at(input, cp).is_ascii_digit() {
                w.push(at(input, cp))?;
                cp += 1;
            }
            let next = at(input, cp);
            if next == b':' {
                kind = FieldKind::Time;
                w.push(next)?;
                cp += 1;
                while matches!(at(input, cp), b'0'..=b'9' | b':' | b'.') {
                    w.push(at(input, cp))?;
                    cp += 1;
                }
            } else if matches!(next, b'-' | b'/' | b'.') {
                let delim = next;
                w.push(delim)?;
                cp += 1;
                if at(input, cp).is_ascii_digit() {
                    let mut field_kind = if delim == b'.' {
                        FieldKind::Number
                    } else {
                        FieldKind::Date
                    };
                    while at(input, cp).is_ascii_digit() {
                        w.push(at(input, cp))?;
                        cp += 1;
                    }
                    if at(input, cp) == delim {
                        field_kind = FieldKind::Date;
                        w.push(delim)?;
                        cp += 1;
                        while at(input, cp).is_ascii_digit() || at(input, cp) == delim {
                            w.push(at(input, cp))?;
                            cp += 1;
                        }
                    }
                    kind = field_kind;
                } else {
                    kind = FieldKind::Date;
                    while at(input, cp).is_ascii_alphanumeric() || at(input, cp) == delim {
                        w.push(at(input, cp).to_ascii_lowercase())?;
                        cp += 1;
                    }
                }
            } else {
                kind = FieldKind::Number;
            }
        } else if c == b'.' {
            w.push(c)?;
            cp += 1;
            while at(input, cp).is_ascii_digit() {
                w.push(at(input, cp))?;
                cp += 1;
            }
            kind = FieldKind::Number;
        } else if c.is_ascii_alphabetic() {
            w.push(c.to_ascii_lowercase())?;
            cp += 1;
            while at(input, cp).is_ascii_alphabetic() {
                w.push(at(input, cp).to_ascii_lowercase())?;
                cp += 1;
            }
            let next = at(input, cp);
            let is_date = if matches!(next, b'-' | b'/' | b'.') {
                true
            } else if next == b'+' || next.is_ascii_digit() {
                lookup_token(DATE_TOKENS, &w.text).is_none()
            } else {
                false
            };
            if is_date {
                loop {
                    w.push(at(input, cp).to_ascii_lowercase())?;
                    cp += 1;
                    let next = at(input, cp);
                    if !(matches!(next, b'+' | b'-' | b'/' | b'_' | b'.' | b':')
                        || next.is_ascii_alphanumeric())
                    {
                        break;
                    }
                }
                kind = FieldKind::Date;
            } else {
                kind = FieldKind::String;
            }
        } else if c == b'+' || c == b'-' {
            w.push(c)?;
            cp += 1;
            while is_c_space(at(input, cp)) {
                cp += 1;
            }
            let next = at(input, cp);
            if next.is_ascii_digit() {
                kind = FieldKind::Tz;
                w.push(next)?;
                cp += 1;
                while matches!(at(input, cp), b'0'..=b'9' | b':' | b'.' | b'-') {
                    w.push(at(input, cp))?;
                    cp += 1;
                }
            } else if next.is_ascii_alphabetic() {
                kind = FieldKind::Special;
                w.push(next.to_ascii_lowercase())?;
                cp += 1;
                while at(input, cp).is_ascii_alphabetic() {
                    w.push(at(input, cp).to_ascii_lowercase())?;
                    cp += 1;
                }
            } else {
                return Err(DtErr::BadFormat);
            }
        } else if c.is_ascii_punctuation() {
            cp += 1;
            continue;
        } else {
            return Err(DtErr::BadFormat);
        }
        fields.push(w.finish(kind));
    }
    Ok(fields)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DateType {
    Date,
    Epoch,
    Late,
    Early,
}

#[derive(Debug, Clone, Copy, Default)]
struct Tm {
    year: i32,
    mon: i32,
    mday: i32,
    hour: i32,
    min: i32,
    sec: i32,
    yday: i32,
}

struct DecodedDatetime {
    dtype: DateType,
    tm: Tm,
    fsec: i32,
    /// Seconds west of UTC.
    tz: i32,
    fmask: u32,
}

/// Port of PostgreSQL's `DecodeDateTime` with DateStyle `ISO, MDY` and
/// TimeZone `UTC`.
fn decode_date_time(fields: &mut [Field], now: i64) -> std::result::Result<DecodedDatetime, DtErr> {
    let mut fmask: u32 = 0;
    let mut ptype: i32 = 0;
    let mut mer = HR24;
    let mut have_text_month = false;
    let mut is_julian = false;
    let mut is2digits = false;
    let mut bc = false;
    let mut have_named_zone = false;
    let mut dtype = DateType::Date;
    let mut tm = Tm::default();
    let mut fsec: i32 = 0;
    let mut tz: i32 = 0;

    for field in fields.iter_mut() {
        let tmask: u32;
        match field.kind {
            FieldKind::Date => {
                let text = &mut field.text;
                if ptype == DTK_JULIAN {
                    let (jday, end, erange) = c_strtoint(text, 0);
                    if erange || jday < 0 {
                        return Err(DtErr::FieldOverflow);
                    }
                    (tm.year, tm.mon, tm.mday) = j2date(jday);
                    is_julian = true;
                    tz = decode_timezone(&text[end..])?;
                    tmask = DTK_DATE_M | DTK_TIME_M | dtk_m(TZ);
                    ptype = 0;
                } else if ptype != 0
                    || fmask & (dtk_m(MONTH) | dtk_m(DAY)) == dtk_m(MONTH) | dtk_m(DAY)
                {
                    if at(text, 0).is_ascii_digit() || ptype != 0 {
                        if ptype != 0 {
                            if ptype != DTK_TIME {
                                return Err(DtErr::BadFormat);
                            }
                            ptype = 0;
                        }
                        if fmask & DTK_TIME_M == DTK_TIME_M {
                            return Err(DtErr::BadFormat);
                        }
                        let Some(dash) = text.iter().position(|&c| c == b'-') else {
                            return Err(DtErr::BadFormat);
                        };
                        tz = decode_timezone(&text[dash..])?;
                        text.truncate(dash);
                        tmask =
                            decode_number_field(text, fmask, &mut tm, &mut fsec, &mut is2digits)?
                                | dtk_m(TZ);
                    } else {
                        if !UTC_ZONE_NAMES.contains(&String::from_utf8_lossy(text).as_ref()) {
                            return Err(DtErr::BadTimezone(
                                String::from_utf8_lossy(text).into_owned(),
                            ));
                        }
                        have_named_zone = true;
                        tmask = dtk_m(TZ);
                    }
                } else {
                    tmask = decode_date(text, fmask, &mut is2digits, &mut tm)?;
                }
            }
            FieldKind::Time => {
                if ptype != 0 {
                    if ptype != DTK_TIME {
                        return Err(DtErr::BadFormat);
                    }
                    ptype = 0;
                }
                tmask = decode_time(&field.text, &mut tm, &mut fsec)?;
                if time_overflows(tm.hour, tm.min, tm.sec, fsec) {
                    return Err(DtErr::FieldOverflow);
                }
            }
            FieldKind::Tz => {
                tz = decode_timezone(&field.text)?;
                tmask = dtk_m(TZ);
            }
            FieldKind::Number => {
                let text = &field.text;
                if ptype != 0 {
                    let (value, end, erange) = c_strtoint(text, 0);
                    if erange {
                        return Err(DtErr::FieldOverflow);
                    }
                    if at(text, end) != b'.' && at(text, end) != 0 {
                        return Err(DtErr::BadFormat);
                    }
                    match ptype {
                        DTK_JULIAN => {
                            if value < 0 {
                                return Err(DtErr::FieldOverflow);
                            }
                            let mut mask = DTK_DATE_M;
                            (tm.year, tm.mon, tm.mday) = j2date(value);
                            is_julian = true;
                            if at(text, end) == b'.' {
                                let time = parse_fraction(&text[end..])? * USECS_PER_DAY as f64;
                                dt2time(time as i64, &mut tm, &mut fsec);
                                mask |= DTK_TIME_M;
                            }
                            tmask = mask;
                        }
                        DTK_TIME => {
                            tmask = decode_number_field(
                                text,
                                fmask | DTK_DATE_M,
                                &mut tm,
                                &mut fsec,
                                &mut is2digits,
                            )?;
                            if tmask != DTK_TIME_M {
                                return Err(DtErr::BadFormat);
                            }
                        }
                        _ => return Err(DtErr::BadFormat),
                    }
                    ptype = 0;
                    dtype = DateType::Date;
                } else {
                    let flen = text.len();
                    let dot = text.iter().position(|&c| c == b'.');
                    tmask = match dot {
                        Some(_) if fmask & DTK_DATE_M == 0 => {
                            decode_date(text, fmask, &mut is2digits, &mut tm)?
                        }
                        Some(dot) if dot > 2 => {
                            decode_number_field(text, fmask, &mut tm, &mut fsec, &mut is2digits)?
                        }
                        _ if flen >= 6 && (fmask & DTK_DATE_M == 0 || fmask & DTK_TIME_M == 0) => {
                            decode_number_field(text, fmask, &mut tm, &mut fsec, &mut is2digits)?
                        }
                        _ => decode_number(
                            text,
                            have_text_month,
                            fmask,
                            &mut tm,
                            &mut fsec,
                            &mut is2digits,
                        )?,
                    };
                }
            }
            FieldKind::String | FieldKind::Special => {
                let text = &field.text;
                let (ty, val) =
                    lookup_token(UTC_ABBREVIATIONS, text).unwrap_or_else(|| decode_special(text));
                if ty == IGNORE_DTF {
                    continue;
                }
                let mut mask = dtk_m(ty);
                match ty {
                    RESERV => match val {
                        DTK_NOW => {
                            mask = DTK_DATE_M | DTK_TIME_M | dtk_m(TZ);
                            dtype = DateType::Date;
                            (tm, fsec) = now_tm(now);
                            tz = 0;
                        }
                        DTK_YESTERDAY | DTK_TODAY | DTK_TOMORROW => {
                            mask = DTK_DATE_M;
                            dtype = DateType::Date;
                            let today = now_tm(now).0;
                            let offset = match val {
                                DTK_YESTERDAY => -1,
                                DTK_TODAY => 0,
                                _ => 1,
                            };
                            (tm.year, tm.mon, tm.mday) =
                                j2date(date2j(today.year, today.mon, today.mday) + offset);
                        }
                        DTK_ZULU => {
                            mask = DTK_TIME_M | dtk_m(TZ);
                            dtype = DateType::Date;
                            tm.hour = 0;
                            tm.min = 0;
                            tm.sec = 0;
                            tz = 0;
                        }
                        DTK_EPOCH | DTK_LATE | DTK_EARLY => {
                            mask = DTK_DATE_M | DTK_TIME_M | dtk_m(TZ);
                            dtype = match val {
                                DTK_EPOCH => DateType::Epoch,
                                DTK_LATE => DateType::Late,
                                _ => DateType::Early,
                            };
                        }
                        _ => unreachable!("unknown reserved datetime token {val}"),
                    },
                    MONTH => {
                        if fmask & dtk_m(MONTH) != 0
                            && !have_text_month
                            && fmask & dtk_m(DAY) == 0
                            && (1..=31).contains(&tm.mon)
                        {
                            tm.mday = tm.mon;
                            mask = dtk_m(DAY);
                        }
                        have_text_month = true;
                        tm.mon = val;
                    }
                    DTZMOD => {
                        mask |= dtk_m(DTZ);
                        tz -= val;
                    }
                    DTZ => {
                        mask |= dtk_m(TZ);
                        tz = -val;
                    }
                    TZ => tz = -val,
                    AMPM => mer = val,
                    ADBC => bc = val == BC,
                    DOW => {}
                    UNITS => {
                        mask = 0;
                        if ptype != 0 {
                            return Err(DtErr::BadFormat);
                        }
                        ptype = val;
                    }
                    ISOTIME => {
                        mask = 0;
                        if fmask & DTK_DATE_M != DTK_DATE_M {
                            return Err(DtErr::BadFormat);
                        }
                        if ptype != 0 {
                            return Err(DtErr::BadFormat);
                        }
                        ptype = val;
                    }
                    // PostgreSQL would try the word as a time zone name here.
                    // The all-letter names it knows are not supported.
                    _ => return Err(DtErr::BadFormat),
                }
                tmask = mask;
            }
        }
        if tmask & fmask != 0 {
            return Err(DtErr::BadFormat);
        }
        fmask |= tmask;
    }

    if ptype != 0 {
        return Err(DtErr::BadFormat);
    }
    if dtype == DateType::Date {
        validate_date(fmask, is_julian, is2digits, bc, &mut tm)?;
        if mer != HR24 && tm.hour > 12 {
            return Err(DtErr::FieldOverflow);
        }
        if mer == AM && tm.hour == 12 {
            tm.hour = 0;
        } else if mer == PM && tm.hour != 12 {
            tm.hour += 12;
        }
        // PostgreSQL also rejects time-only input here.
        if fmask & DTK_DATE_M != DTK_DATE_M {
            return Err(DtErr::BadFormat);
        }
        if have_named_zone {
            if fmask & dtk_m(DTZMOD) != 0 {
                return Err(DtErr::BadFormat);
            }
            tz = 0;
        }
        if fmask & dtk_m(TZ) == 0 {
            if fmask & dtk_m(DTZMOD) != 0 {
                return Err(DtErr::BadFormat);
            }
            tz = 0;
        }
    }
    Ok(DecodedDatetime {
        dtype,
        tm,
        fsec,
        tz,
        fmask,
    })
}

/// Port of PostgreSQL's `DecodeTimeOnly` with TimeZone `UTC`. The time zone
/// is checked but not returned, because `time` ignores it.
fn decode_time_only(fields: &mut [Field], now: i64) -> std::result::Result<(Tm, i32), DtErr> {
    let mut fmask: u32 = 0;
    let mut ptype: i32 = 0;
    let mut mer = HR24;
    let mut is_julian = false;
    let mut is2digits = false;
    let mut bc = false;
    let mut have_named_zone = false;
    let mut tm = Tm::default();
    let mut fsec: i32 = 0;

    let field_count = fields.len();
    let ends_with_date = field_count >= 2 && fields[field_count - 1].kind == FieldKind::Date;
    let date_comes_first =
        ends_with_date || (field_count >= 2 && fields[1].kind == FieldKind::Time);

    for (i, field) in fields.iter_mut().enumerate() {
        let tmask: u32;
        match field.kind {
            FieldKind::Date => {
                let text = &mut field.text;
                if i == 0 && date_comes_first {
                    tmask = decode_date(text, fmask, &mut is2digits, &mut tm)?;
                } else if at(text, 0).is_ascii_digit() {
                    if fmask & DTK_TIME_M == DTK_TIME_M {
                        return Err(DtErr::BadFormat);
                    }
                    let Some(dash) = text.iter().position(|&c| c == b'-') else {
                        return Err(DtErr::BadFormat);
                    };
                    decode_timezone(&text[dash..])?;
                    text.truncate(dash);
                    tmask = decode_number_field(
                        text,
                        fmask | DTK_DATE_M,
                        &mut tm,
                        &mut fsec,
                        &mut is2digits,
                    )? | dtk_m(TZ);
                } else {
                    if !UTC_ZONE_NAMES.contains(&String::from_utf8_lossy(text).as_ref()) {
                        return Err(DtErr::BadTimezone(
                            String::from_utf8_lossy(text).into_owned(),
                        ));
                    }
                    have_named_zone = true;
                    tmask = dtk_m(TZ);
                }
            }
            FieldKind::Time => {
                if ptype != 0 {
                    if ptype != DTK_TIME {
                        return Err(DtErr::BadFormat);
                    }
                    ptype = 0;
                }
                tmask = decode_time(&field.text, &mut tm, &mut fsec)?;
            }
            FieldKind::Tz => {
                decode_timezone(&field.text)?;
                tmask = dtk_m(TZ);
            }
            FieldKind::Number => {
                let text = &field.text;
                if ptype != 0 {
                    let (value, end, erange) = c_strtoint(text, 0);
                    if erange {
                        return Err(DtErr::FieldOverflow);
                    }
                    if at(text, end) != b'.' && at(text, end) != 0 {
                        return Err(DtErr::BadFormat);
                    }
                    match ptype {
                        DTK_JULIAN => {
                            if value < 0 {
                                return Err(DtErr::FieldOverflow);
                            }
                            let mut mask = DTK_DATE_M;
                            (tm.year, tm.mon, tm.mday) = j2date(value);
                            is_julian = true;
                            if at(text, end) == b'.' {
                                let time = parse_fraction(&text[end..])? * USECS_PER_DAY as f64;
                                dt2time(time as i64, &mut tm, &mut fsec);
                                mask |= DTK_TIME_M;
                            }
                            tmask = mask;
                        }
                        DTK_TIME => {
                            tmask = decode_number_field(
                                text,
                                fmask | DTK_DATE_M,
                                &mut tm,
                                &mut fsec,
                                &mut is2digits,
                            )?;
                            if tmask != DTK_TIME_M {
                                return Err(DtErr::BadFormat);
                            }
                        }
                        _ => return Err(DtErr::BadFormat),
                    }
                    ptype = 0;
                } else {
                    tmask = match text.iter().position(|&c| c == b'.') {
                        Some(_) if i == 0 && ends_with_date => {
                            decode_date(text, fmask, &mut is2digits, &mut tm)?
                        }
                        Some(dot) if dot > 2 => decode_number_field(
                            text,
                            fmask | DTK_DATE_M,
                            &mut tm,
                            &mut fsec,
                            &mut is2digits,
                        )?,
                        Some(_) => return Err(DtErr::BadFormat),
                        None if text.len() > 4 => decode_number_field(
                            text,
                            fmask | DTK_DATE_M,
                            &mut tm,
                            &mut fsec,
                            &mut is2digits,
                        )?,
                        None => decode_number(
                            text,
                            false,
                            fmask | DTK_DATE_M,
                            &mut tm,
                            &mut fsec,
                            &mut is2digits,
                        )?,
                    };
                }
            }
            FieldKind::String | FieldKind::Special => {
                let text = &field.text;
                let (ty, val) =
                    lookup_token(UTC_ABBREVIATIONS, text).unwrap_or_else(|| decode_special(text));
                if ty == IGNORE_DTF {
                    continue;
                }
                let mut mask = dtk_m(ty);
                match ty {
                    RESERV => match val {
                        DTK_NOW => {
                            mask = DTK_TIME_M;
                            let (now, now_fsec) = now_tm(now);
                            tm.hour = now.hour;
                            tm.min = now.min;
                            tm.sec = now.sec;
                            fsec = now_fsec;
                        }
                        DTK_ZULU => {
                            mask = DTK_TIME_M | dtk_m(TZ);
                            tm.hour = 0;
                            tm.min = 0;
                            tm.sec = 0;
                        }
                        _ => return Err(DtErr::BadFormat),
                    },
                    DTZMOD => mask |= dtk_m(DTZ),
                    DTZ => mask |= dtk_m(TZ),
                    TZ => {}
                    AMPM => mer = val,
                    ADBC => bc = val == BC,
                    UNITS | ISOTIME => {
                        mask = 0;
                        if ptype != 0 {
                            return Err(DtErr::BadFormat);
                        }
                        ptype = val;
                    }
                    // PostgreSQL would try an unknown word as a time zone
                    // name here. The all-letter names it knows are not
                    // supported.
                    _ => return Err(DtErr::BadFormat),
                }
                tmask = mask;
            }
        }
        if tmask & fmask != 0 {
            return Err(DtErr::BadFormat);
        }
        fmask |= tmask;
    }

    if ptype != 0 {
        return Err(DtErr::BadFormat);
    }
    validate_date(fmask, is_julian, is2digits, bc, &mut tm)?;
    if mer != HR24 && tm.hour > 12 {
        return Err(DtErr::FieldOverflow);
    }
    if mer == AM && tm.hour == 12 {
        tm.hour = 0;
    } else if mer == PM && tm.hour != 12 {
        tm.hour += 12;
    }
    if time_overflows(tm.hour, tm.min, tm.sec, fsec) {
        return Err(DtErr::FieldOverflow);
    }
    if fmask & DTK_TIME_M != DTK_TIME_M {
        return Err(DtErr::BadFormat);
    }
    if have_named_zone && fmask & dtk_m(DTZMOD) != 0 {
        return Err(DtErr::BadFormat);
    }
    if fmask & dtk_m(TZ) == 0 {
        let date_mask = fmask & DTK_DATE_M;
        if fmask & dtk_m(DTZMOD) != 0 || (date_mask != 0 && date_mask != DTK_DATE_M) {
            return Err(DtErr::BadFormat);
        }
    }
    Ok((tm, fsec))
}

fn now_tm(now: i64) -> (Tm, i32) {
    let days = now.div_euclid(USECS_PER_DAY);
    let mut tm = Tm::default();
    (tm.year, tm.mon, tm.mday) = j2date((days + i64::from(UNIX_EPOCH_JDATE)) as i32);
    let mut fsec = 0;
    dt2time(now.rem_euclid(USECS_PER_DAY), &mut tm, &mut fsec);
    (tm, fsec)
}

/// Port of PostgreSQL's `DecodeDate`: a date with delimiters, possibly with a
/// month name.
fn decode_date(
    text: &[u8],
    mut fmask: u32,
    is2digits: &mut bool,
    tm: &mut Tm,
) -> std::result::Result<u32, DtErr> {
    let mut tmask = 0;
    let mut parts: Vec<Option<&[u8]>> = Vec::new();
    let mut pos = 0;
    while at(text, pos) != 0 && parts.len() < MAXDATEFIELDS {
        while at(text, pos) != 0 && !at(text, pos).is_ascii_alphanumeric() {
            pos += 1;
        }
        if at(text, pos) == 0 {
            return Err(DtErr::BadFormat);
        }
        let start = pos;
        if at(text, pos).is_ascii_digit() {
            while at(text, pos).is_ascii_digit() {
                pos += 1;
            }
        } else {
            while at(text, pos).is_ascii_alphabetic() {
                pos += 1;
            }
        }
        parts.push(Some(&text[start..pos]));
        // PostgreSQL overwrites the character after each part with a
        // terminator, so it is skipped even when it is a letter or digit.
        if at(text, pos) != 0 {
            pos += 1;
        }
    }

    let mut have_text_month = false;
    for part in parts.iter_mut() {
        let Some(text) = *part else { continue };
        if !text[0].is_ascii_alphabetic() {
            continue;
        }
        let (ty, val) = decode_special(text);
        if ty == IGNORE_DTF {
            continue;
        }
        let dmask = dtk_m(ty);
        if ty != MONTH {
            return Err(DtErr::BadFormat);
        }
        tm.mon = val;
        have_text_month = true;
        if fmask & dmask != 0 {
            return Err(DtErr::BadFormat);
        }
        fmask |= dmask;
        tmask |= dmask;
        *part = None;
    }

    for part in parts.into_iter().flatten() {
        if part.is_empty() {
            return Err(DtErr::BadFormat);
        }
        let mut fsec = 0;
        let dmask = decode_number(part, have_text_month, fmask, tm, &mut fsec, is2digits)?;
        if fmask & dmask != 0 {
            return Err(DtErr::BadFormat);
        }
        fmask |= dmask;
        tmask |= dmask;
    }

    if fmask & !(dtk_m(DOY) | dtk_m(TZ)) != DTK_DATE_M {
        return Err(DtErr::BadFormat);
    }
    Ok(tmask)
}

/// Port of PostgreSQL's `ValidateDate`: applies BC and two-digit years,
/// resolves day of year and checks month and day ranges.
fn validate_date(
    fmask: u32,
    is_julian: bool,
    is2digits: bool,
    bc: bool,
    tm: &mut Tm,
) -> std::result::Result<(), DtErr> {
    if fmask & dtk_m(YEAR) != 0 {
        if is_julian {
        } else if bc {
            if tm.year <= 0 {
                return Err(DtErr::FieldOverflow);
            }
            tm.year = -(tm.year - 1);
        } else if is2digits {
            if tm.year < 0 {
                return Err(DtErr::FieldOverflow);
            }
            if tm.year < 70 {
                tm.year += 2000;
            } else if tm.year < 100 {
                tm.year += 1900;
            }
        } else if tm.year <= 0 {
            return Err(DtErr::FieldOverflow);
        }
    }
    if fmask & dtk_m(DOY) != 0 {
        (tm.year, tm.mon, tm.mday) =
            j2date(date2j(tm.year, 1, 1).wrapping_add(tm.yday).wrapping_sub(1));
    }
    if fmask & dtk_m(MONTH) != 0 && !(1..=MONTHS_PER_YEAR).contains(&tm.mon) {
        return Err(DtErr::FieldOverflow);
    }
    if fmask & dtk_m(DAY) != 0 && !(1..=31).contains(&tm.mday) {
        return Err(DtErr::FieldOverflow);
    }
    if fmask & DTK_DATE_M == DTK_DATE_M && tm.mday > days_in_month_of(tm.year, tm.mon) {
        return Err(DtErr::FieldOverflow);
    }
    Ok(())
}

fn days_in_month_of(year: i32, month: i32) -> i32 {
    const DAYS: [i32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    if month == 2 && leap {
        29
    } else {
        DAYS[(month - 1) as usize]
    }
}

/// Port of PostgreSQL's `DecodeNumber`: a plain number whose meaning depends
/// on the date fields seen so far.
fn decode_number(
    text: &[u8],
    have_text_month: bool,
    fmask: u32,
    tm: &mut Tm,
    fsec: &mut i32,
    is2digits: &mut bool,
) -> std::result::Result<u32, DtErr> {
    let flen = text.len();
    let (val, end, erange) = c_strtoint(text, 0);
    if erange {
        return Err(DtErr::FieldOverflow);
    }
    if end == 0 {
        return Err(DtErr::BadFormat);
    }
    if at(text, end) == b'.' {
        if end > 2 {
            return decode_number_field(text, fmask | DTK_DATE_M, tm, fsec, is2digits);
        }
        *fsec = parse_fractional_second(&text[end..])?;
    } else if at(text, end) != 0 {
        return Err(DtErr::BadFormat);
    }

    if flen == 3 && fmask & DTK_DATE_M == dtk_m(YEAR) && (1..=366).contains(&val) {
        tm.yday = val;
        return Ok(dtk_m(DOY) | dtk_m(MONTH) | dtk_m(DAY));
    }

    const YEAR_M: u32 = dtk_m(YEAR);
    const MONTH_M: u32 = dtk_m(MONTH);
    const DAY_M: u32 = dtk_m(DAY);
    let tmask = match fmask & DTK_DATE_M {
        0 => {
            if flen >= 3 {
                tm.year = val;
                YEAR_M
            } else {
                tm.mon = val;
                MONTH_M
            }
        }
        YEAR_M => {
            tm.mon = val;
            MONTH_M
        }
        MONTH_M => {
            if have_text_month && flen >= 3 {
                tm.year = val;
                YEAR_M
            } else {
                tm.mday = val;
                DAY_M
            }
        }
        m if m == YEAR_M | MONTH_M => {
            if have_text_month && flen >= 3 && *is2digits {
                tm.mday = tm.year;
                tm.year = val;
                *is2digits = false;
            } else {
                tm.mday = val;
            }
            DAY_M
        }
        DAY_M => {
            tm.mon = val;
            MONTH_M
        }
        m if m == MONTH_M | DAY_M => {
            tm.year = val;
            YEAR_M
        }
        DTK_DATE_M => return decode_number_field(text, fmask, tm, fsec, is2digits),
        _ => return Err(DtErr::BadFormat),
    };
    if tmask == YEAR_M {
        *is2digits = flen <= 2;
    }
    Ok(tmask)
}

/// Port of PostgreSQL's `DecodeNumberField`: run-together digits such as
/// `20011225` or `040506`, possibly with a fraction.
fn decode_number_field(
    text: &[u8],
    fmask: u32,
    tm: &mut Tm,
    fsec: &mut i32,
    is2digits: &mut bool,
) -> std::result::Result<u32, DtErr> {
    if !text.iter().all(|&c| c.is_ascii_digit() || c == b'.') {
        return Err(DtErr::BadFormat);
    }
    let mut len = text.len();
    if let Some(dot) = text.iter().position(|&c| c == b'.') {
        *fsec = parse_fractional_second(&text[dot..])?;
        len = dot;
    } else if fmask & DTK_DATE_M != DTK_DATE_M && len >= 6 {
        tm.mday = atoi(&text[len - 2..len]);
        tm.mon = atoi(&text[len - 4..len - 2]);
        tm.year = atoi(&text[..len - 4]);
        if len - 4 == 2 {
            *is2digits = true;
        }
        return Ok(DTK_DATE_M);
    }
    let text = &text[..len];
    if fmask & DTK_TIME_M != DTK_TIME_M {
        if len == 6 {
            tm.sec = atoi(&text[4..6]);
            tm.min = atoi(&text[2..4]);
            tm.hour = atoi(&text[..2]);
            return Ok(DTK_TIME_M);
        }
        if len == 4 {
            tm.sec = 0;
            tm.min = atoi(&text[2..4]);
            tm.hour = atoi(&text[..2]);
            return Ok(DTK_TIME_M);
        }
    }
    Err(DtErr::BadFormat)
}

/// Port of PostgreSQL's `DecodeTimezone`: a numeric offset such as `+02`,
/// `-0530` or `+05:30:15`. Returns seconds west of UTC.
fn decode_timezone(text: &[u8]) -> std::result::Result<i32, DtErr> {
    if !matches!(at(text, 0), b'+' | b'-') {
        return Err(DtErr::BadFormat);
    }
    let (mut hr, mut cp, erange) = c_strtoint(text, 1);
    if erange {
        return Err(DtErr::TzOverflow);
    }
    let mut min = 0;
    let mut sec = 0;
    if at(text, cp) == b':' {
        let (value, end, erange) = c_strtoint(text, cp + 1);
        if erange {
            return Err(DtErr::TzOverflow);
        }
        min = value;
        cp = end;
        if at(text, cp) == b':' {
            let (value, end, erange) = c_strtoint(text, cp + 1);
            if erange {
                return Err(DtErr::TzOverflow);
            }
            sec = value;
            cp = end;
        }
    } else if at(text, cp) == 0 && text.len() > 3 {
        min = hr % 100;
        hr /= 100;
    }
    if !(0..=MAX_TZDISP_HOUR).contains(&hr) || !(0..60).contains(&min) || !(0..60).contains(&sec) {
        return Err(DtErr::TzOverflow);
    }
    let mut tz = (hr * 60 + min) * 60 + sec;
    if text[0] == b'-' {
        tz = -tz;
    }
    // PostgreSQL range-checks the offset before rejecting trailing text.
    if at(text, cp) != 0 {
        return Err(DtErr::BadFormat);
    }
    Ok(-tz)
}

struct TimeFields {
    hour: i64,
    min: i32,
    sec: i32,
    usec: i32,
}

fn decode_time(text: &[u8], tm: &mut Tm, fsec: &mut i32) -> std::result::Result<u32, DtErr> {
    let time = decode_time_common(text)?;
    let hour = i32::try_from(time.hour).map_err(|_| DtErr::FieldOverflow)?;
    tm.hour = hour;
    tm.min = time.min;
    tm.sec = time.sec;
    *fsec = time.usec;
    Ok(DTK_TIME_M)
}

/// Port of PostgreSQL's `DecodeTimeCommon`: `hh:mm`, `hh:mm:ss[.fff]` or
/// `mm:ss.fff`.
fn decode_time_common(text: &[u8]) -> std::result::Result<TimeFields, DtErr> {
    let (mut hour, cp, erange) = c_strtol(text, 0);
    if erange {
        return Err(DtErr::FieldOverflow);
    }
    if at(text, cp) != b':' {
        return Err(DtErr::BadFormat);
    }
    let (mut min, mut cp, erange) = c_strtoint(text, cp + 1);
    if erange {
        return Err(DtErr::FieldOverflow);
    }
    let mut sec = 0;
    let mut fsec = 0;
    match at(text, cp) {
        0 => {}
        b'.' => {
            fsec = parse_fractional_second(&text[cp..])?;
            let minutes = i32::try_from(hour).map_err(|_| DtErr::FieldOverflow)?;
            sec = min;
            min = minutes;
            hour = 0;
        }
        b':' => {
            let (value, end, erange) = c_strtoint(text, cp + 1);
            if erange {
                return Err(DtErr::FieldOverflow);
            }
            sec = value;
            cp = end;
            if at(text, cp) == b'.' {
                fsec = parse_fractional_second(&text[cp..])?;
            } else if at(text, cp) != 0 {
                return Err(DtErr::BadFormat);
            }
        }
        _ => return Err(DtErr::BadFormat),
    }
    if hour < 0
        || !(0..60).contains(&min)
        || !(0..=60).contains(&sec)
        || !(0..=USECS_PER_SEC as i32).contains(&fsec)
    {
        return Err(DtErr::FieldOverflow);
    }
    Ok(TimeFields {
        hour,
        min,
        sec,
        usec: fsec,
    })
}

fn time_overflows(hour: i32, min: i32, sec: i32, fsec: i32) -> bool {
    if !(0..=24).contains(&hour)
        || !(0..60).contains(&min)
        || !(0..=60).contains(&sec)
        || !(0..=USECS_PER_SEC as i32).contains(&fsec)
    {
        return true;
    }
    i64::from((hour * 60 + min) * 60 + sec) * USECS_PER_SEC + i64::from(fsec) > USECS_PER_DAY
}

fn dt2time(time: i64, tm: &mut Tm, fsec: &mut i32) {
    tm.hour = (time / USECS_PER_HOUR) as i32;
    let time = time - i64::from(tm.hour) * USECS_PER_HOUR;
    tm.min = (time / USECS_PER_MINUTE) as i32;
    let time = time - i64::from(tm.min) * USECS_PER_MINUTE;
    tm.sec = (time / USECS_PER_SEC) as i32;
    *fsec = (time - i64::from(tm.sec) * USECS_PER_SEC) as i32;
}

fn time2t(hour: i32, min: i32, sec: i32, fsec: i32) -> i64 {
    let seconds = hour
        .wrapping_mul(60)
        .wrapping_add(min)
        .wrapping_mul(60)
        .wrapping_add(sec);
    i64::from(seconds) * USECS_PER_SEC + i64::from(fsec)
}

fn is_valid_julian(year: i32, month: i32) -> bool {
    (year > -4713 || (year == -4713 && month >= 11))
        && (year < 5_874_898 || (year == 5_874_898 && month < 6))
}

/// Julian day number of a date. Uses wrapping arithmetic like PostgreSQL,
/// which is compiled with `-fwrapv`.
fn date2j(year: i32, month: i32, day: i32) -> i32 {
    let (year, month) = if month > 2 {
        (year.wrapping_add(4800), month.wrapping_add(1))
    } else {
        (year.wrapping_add(4799), month.wrapping_add(13))
    };
    let century = year / 100;
    year.wrapping_mul(365)
        .wrapping_sub(32167)
        .wrapping_add(year / 4 - century + century / 4)
        .wrapping_add(7834i32.wrapping_mul(month) / 256)
        .wrapping_add(day)
}

/// Date of a Julian day number, using unsigned arithmetic like PostgreSQL.
fn j2date(jd: i32) -> (i32, i32, i32) {
    let mut julian = (jd as u32).wrapping_add(32044);
    let mut quad = julian / 146_097;
    let extra = julian
        .wrapping_sub(quad.wrapping_mul(146_097))
        .wrapping_mul(4)
        .wrapping_add(3);
    julian = julian.wrapping_add(60 + quad.wrapping_mul(3) + extra / 146_097);
    quad = julian / 1461;
    julian = julian.wrapping_sub(quad.wrapping_mul(1461));
    let mut y = julian.wrapping_mul(4) / 1461;
    julian = if y != 0 {
        (julian + 305) % 365
    } else {
        (julian + 306) % 366
    } + 123;
    y = y.wrapping_add(quad.wrapping_mul(4));
    let year = (y as i32).wrapping_sub(4800);
    let quad = julian * 2141 / 65536;
    let day = julian - 7834 * quad / 256;
    let month = (quad + 10) % 12 + 1;
    (year, month as i32, day as i32)
}

/// C `strtol` with base 10: skips leading white space, accepts a sign, and
/// saturates on overflow. Returns the value, the end position and whether it
/// overflowed. With no digits the end position is `start`.
fn c_strtol(text: &[u8], start: usize) -> (i64, usize, bool) {
    let mut i = start;
    while is_c_space(at(text, i)) {
        i += 1;
    }
    let negative = match at(text, i) {
        b'-' => {
            i += 1;
            true
        }
        b'+' => {
            i += 1;
            false
        }
        _ => false,
    };
    if !at(text, i).is_ascii_digit() {
        return (0, start, false);
    }
    let mut value: i64 = 0;
    let mut overflow = false;
    while at(text, i).is_ascii_digit() {
        let digit = i64::from(at(text, i) - b'0');
        if !overflow {
            let next = value.checked_mul(10).and_then(|v| {
                if negative {
                    v.checked_sub(digit)
                } else {
                    v.checked_add(digit)
                }
            });
            match next {
                Some(next) => value = next,
                None => {
                    overflow = true;
                    value = if negative { i64::MIN } else { i64::MAX };
                }
            }
        }
        i += 1;
    }
    (value, i, overflow)
}

/// PostgreSQL's `strtoint`: `strtol` that also reports values outside `i32`
/// as overflow.
fn c_strtoint(text: &[u8], start: usize) -> (i32, usize, bool) {
    let (value, end, overflow) = c_strtol(text, start);
    (
        value as i32,
        end,
        overflow || i64::from(value as i32) != value,
    )
}

fn atoi(text: &[u8]) -> i32 {
    c_strtol(text, 0).0 as i32
}

/// Port of PostgreSQL's `ParseFraction`: `text` starts at the decimal point
/// and must contain only digits after it.
fn parse_fraction(text: &[u8]) -> std::result::Result<f64, DtErr> {
    if text.len() == 1 {
        return Ok(0.0);
    }
    if !text[1..].iter().all(u8::is_ascii_digit) {
        return Err(DtErr::BadFormat);
    }
    let digits = std::str::from_utf8(&text[1..]).map_err(|_| DtErr::BadFormat)?;
    format!("0.{digits}").parse().map_err(|_| DtErr::BadFormat)
}

fn parse_fractional_second(text: &[u8]) -> std::result::Result<i32, DtErr> {
    Ok((parse_fraction(text)? * USECS_PER_SEC as f64).round_ties_even() as i32)
}

fn parse_interval(input: &str) -> std::result::Result<Interval, DtErr> {
    let bytes = until_nul(input.as_bytes());
    let decoded = match parse_date_time(bytes, INTERVAL_BUFFER_LEN)
        .and_then(|fields| decode_interval(&fields))
    {
        Err(DtErr::BadFormat) => decode_iso8601_interval(bytes).map(DecodedInterval::Finite),
        other => other,
    }?;
    match decoded {
        DecodedInterval::Infinity => Ok(Interval::Infinity),
        DecodedInterval::NegInfinity => Ok(Interval::NegInfinity),
        DecodedInterval::Finite(itm) => itmin2interval(itm),
    }
}

/// Interval fields while decoding, as in PostgreSQL's `pg_itm_in`.
#[derive(Debug, Default, Clone, Copy)]
struct IntervalFields {
    usec: i64,
    mday: i32,
    mon: i32,
    year: i32,
}

enum DecodedInterval {
    Finite(IntervalFields),
    Infinity,
    NegInfinity,
}

/// PostgreSQL stores infinite intervals as all-maximum or all-minimum fields,
/// so input that reaches those values reads back as infinity.
fn itmin2interval(itm: IntervalFields) -> std::result::Result<Interval, DtErr> {
    let months = i64::from(itm.year) * i64::from(MONTHS_PER_YEAR) + i64::from(itm.mon);
    let months = i32::try_from(months).map_err(|_| DtErr::IntervalOutOfRange)?;
    Ok(match (months, itm.mday, itm.usec) {
        (i32::MAX, i32::MAX, i64::MAX) => Interval::Infinity,
        (i32::MIN, i32::MIN, i64::MIN) => Interval::NegInfinity,
        (months, days, micros) => Interval::Finite {
            months,
            days,
            micros,
        },
    })
}

/// Port of PostgreSQL's `DecodeInterval` with IntervalStyle `postgres` and
/// no typmod. Fields are read from right to left so units come before their
/// values.
fn decode_interval(fields: &[Field]) -> std::result::Result<DecodedInterval, DtErr> {
    let mut is_before = false;
    let mut parsing_unit_val = false;
    let mut fmask: u32 = 0;
    let mut ty = IGNORE_DTF;
    let mut infinite = None;
    let mut itm = IntervalFields::default();
    let nf = fields.len();

    for i in (0..nf).rev() {
        let text = &fields[i].text;
        let tmask: u32;
        match fields[i].kind {
            FieldKind::Time => {
                tmask = decode_time_for_interval(text, &mut itm)?;
                ty = DTK_DAY;
                parsing_unit_val = false;
            }
            FieldKind::Tz
                if text[1..].contains(&b':')
                    && decode_time_for_interval(&text[1..], &mut itm).is_ok() =>
            {
                if text[0] == b'-' {
                    if itm.usec == i64::MIN {
                        return Err(DtErr::FieldOverflow);
                    }
                    itm.usec = -itm.usec;
                }
                tmask = DTK_TIME_M;
                ty = DTK_DAY;
                parsing_unit_val = false;
            }
            FieldKind::Tz | FieldKind::Date | FieldKind::Number => {
                if ty == IGNORE_DTF {
                    ty = DTK_SECOND;
                }
                let (mut val, cp, erange) = c_strtol(text, 0);
                if erange {
                    return Err(DtErr::FieldOverflow);
                }
                let fval = match at(text, cp) {
                    b'-' => {
                        let (mut val2, end, erange) = c_strtoint(text, cp + 1);
                        if erange || !(0..MONTHS_PER_YEAR).contains(&val2) {
                            return Err(DtErr::FieldOverflow);
                        }
                        if at(text, end) != 0 {
                            return Err(DtErr::BadFormat);
                        }
                        ty = DTK_MONTH;
                        if text[0] == b'-' {
                            val2 = -val2;
                        }
                        val = val
                            .checked_mul(i64::from(MONTHS_PER_YEAR))
                            .and_then(|v| v.checked_add(i64::from(val2)))
                            .ok_or(DtErr::FieldOverflow)?;
                        0.0
                    }
                    b'.' => {
                        let fval = parse_fraction(&text[cp..])?;
                        if text[0] == b'-' {
                            -fval
                        } else {
                            fval
                        }
                    }
                    0 => 0.0,
                    _ => return Err(DtErr::BadFormat),
                };
                let ok;
                (ok, tmask) = match ty {
                    DTK_MICROSEC => (itm.adjust_microseconds(val, fval, 1), dtk_m(MICROSECOND)),
                    DTK_MILLISEC => (itm.adjust_microseconds(val, fval, 1000), dtk_m(MILLISECOND)),
                    DTK_SECOND => (
                        itm.adjust_microseconds(val, fval, USECS_PER_SEC),
                        if fval == 0.0 {
                            dtk_m(SECOND)
                        } else {
                            DTK_ALL_SECS_M
                        },
                    ),
                    DTK_MINUTE => (
                        itm.adjust_microseconds(val, fval, USECS_PER_MINUTE),
                        dtk_m(MINUTE),
                    ),
                    DTK_HOUR => {
                        ty = DTK_DAY;
                        (
                            itm.adjust_microseconds(val, fval, USECS_PER_HOUR),
                            dtk_m(HOUR),
                        )
                    }
                    DTK_DAY => (
                        itm.adjust_days(val, 1)
                            && itm.adjust_fract_microseconds(fval, USECS_PER_DAY),
                        dtk_m(DAY),
                    ),
                    DTK_WEEK => (
                        itm.adjust_days(val, 7) && itm.adjust_fract_days(fval, 7),
                        dtk_m(WEEK),
                    ),
                    DTK_MONTH => (
                        itm.adjust_months(val) && itm.adjust_fract_days(fval, DAYS_PER_MONTH),
                        dtk_m(MONTH),
                    ),
                    DTK_YEAR => (
                        itm.adjust_years(val, 1) && itm.adjust_fract_years(fval, 1),
                        dtk_m(YEAR),
                    ),
                    DTK_DECADE => (
                        itm.adjust_years(val, 10) && itm.adjust_fract_years(fval, 10),
                        dtk_m(DECADE),
                    ),
                    DTK_CENTURY => (
                        itm.adjust_years(val, 100) && itm.adjust_fract_years(fval, 100),
                        dtk_m(CENTURY),
                    ),
                    DTK_MILLENNIUM => (
                        itm.adjust_years(val, 1000) && itm.adjust_fract_years(fval, 1000),
                        dtk_m(MILLENNIUM),
                    ),
                    _ => return Err(DtErr::BadFormat),
                };
                if !ok {
                    return Err(DtErr::FieldOverflow);
                }
                parsing_unit_val = false;
            }
            FieldKind::String | FieldKind::Special => {
                if parsing_unit_val {
                    return Err(DtErr::BadFormat);
                }
                let (word_type, uval) = match decode_units(text) {
                    (UNKNOWN_FIELD, _) => decode_special(text),
                    found => found,
                };
                // The pending unit is replaced even by an ignored word, so in
                // `5 at 10:00` the 5 is read as seconds.
                ty = word_type;
                if ty == IGNORE_DTF {
                    continue;
                }
                match ty {
                    UNITS => {
                        ty = uval;
                        parsing_unit_val = true;
                        tmask = 0;
                    }
                    AGO => {
                        if i != nf - 1 {
                            return Err(DtErr::BadFormat);
                        }
                        is_before = true;
                        ty = uval;
                        tmask = 0;
                    }
                    RESERV => {
                        if uval != DTK_LATE && uval != DTK_EARLY {
                            return Err(DtErr::BadFormat);
                        }
                        if i != nf - 1 {
                            return Err(DtErr::BadFormat);
                        }
                        infinite = Some(uval);
                        tmask = DTK_DATE_M | DTK_TIME_M;
                    }
                    _ => return Err(DtErr::BadFormat),
                }
            }
        }
        if tmask & fmask != 0 {
            return Err(DtErr::BadFormat);
        }
        fmask |= tmask;
    }

    if fmask == 0 || parsing_unit_val {
        return Err(DtErr::BadFormat);
    }
    if is_before {
        if itm.usec == i64::MIN
            || itm.mday == i32::MIN
            || itm.mon == i32::MIN
            || itm.year == i32::MIN
        {
            return Err(DtErr::FieldOverflow);
        }
        itm.usec = -itm.usec;
        itm.mday = -itm.mday;
        itm.mon = -itm.mon;
        itm.year = -itm.year;
    }
    Ok(match infinite {
        Some(DTK_LATE) => DecodedInterval::Infinity,
        Some(_) => DecodedInterval::NegInfinity,
        None => DecodedInterval::Finite(itm),
    })
}

/// Port of PostgreSQL's `DecodeTimeForInterval`. Like PostgreSQL, this
/// replaces any microseconds collected so far.
fn decode_time_for_interval(
    text: &[u8],
    itm: &mut IntervalFields,
) -> std::result::Result<u32, DtErr> {
    let time = decode_time_common(text)?;
    itm.usec = i64::from(time.usec);
    if !itm.multiply_add(time.hour, USECS_PER_HOUR)
        || !itm.multiply_add(i64::from(time.min), USECS_PER_MINUTE)
        || !itm.multiply_add(i64::from(time.sec), USECS_PER_SEC)
    {
        return Err(DtErr::FieldOverflow);
    }
    Ok(DTK_TIME_M)
}

/// The `Adjust*` helpers of PostgreSQL's interval input. Each returns false
/// on overflow.
impl IntervalFields {
    fn multiply_add(&mut self, val: i64, multiplier: i64) -> bool {
        let Some(product) = val.checked_mul(multiplier) else {
            return false;
        };
        let (sum, overflow) = self.usec.overflowing_add(product);
        self.usec = sum;
        !overflow
    }

    fn adjust_fract_microseconds(&mut self, frac: f64, scale: i64) -> bool {
        if frac == 0.0 {
            return true;
        }
        let frac = frac * scale as f64;
        let mut usec = frac as i64;
        let rest = frac - usec as f64;
        if rest > 0.5 {
            usec += 1;
        } else if rest < -0.5 {
            usec -= 1;
        }
        let (sum, overflow) = self.usec.overflowing_add(usec);
        self.usec = sum;
        !overflow
    }

    fn adjust_fract_days(&mut self, frac: f64, scale: i32) -> bool {
        if frac == 0.0 {
            return true;
        }
        let frac = frac * f64::from(scale);
        let extra_days = frac as i32;
        let Some(mday) = self.mday.checked_add(extra_days) else {
            return false;
        };
        self.mday = mday;
        self.adjust_fract_microseconds(frac - f64::from(extra_days), USECS_PER_DAY)
    }

    fn adjust_fract_years(&mut self, frac: f64, scale: i32) -> bool {
        let extra_months =
            (frac * f64::from(scale) * f64::from(MONTHS_PER_YEAR)).round_ties_even() as i32;
        match self.mon.checked_add(extra_months) {
            Some(mon) => {
                self.mon = mon;
                true
            }
            None => false,
        }
    }

    fn adjust_microseconds(&mut self, val: i64, fval: f64, scale: i64) -> bool {
        self.multiply_add(val, scale) && self.adjust_fract_microseconds(fval, scale)
    }

    fn adjust_days(&mut self, val: i64, scale: i32) -> bool {
        let Ok(val) = i32::try_from(val) else {
            return false;
        };
        match val
            .checked_mul(scale)
            .and_then(|days| self.mday.checked_add(days))
        {
            Some(mday) => {
                self.mday = mday;
                true
            }
            None => false,
        }
    }

    fn adjust_months(&mut self, val: i64) -> bool {
        let Ok(val) = i32::try_from(val) else {
            return false;
        };
        match self.mon.checked_add(val) {
            Some(mon) => {
                self.mon = mon;
                true
            }
            None => false,
        }
    }

    fn adjust_years(&mut self, val: i64, scale: i32) -> bool {
        let Ok(val) = i32::try_from(val) else {
            return false;
        };
        match val
            .checked_mul(scale)
            .and_then(|years| self.year.checked_add(years))
        {
            Some(year) => {
                self.year = year;
                true
            }
            None => false,
        }
    }
}

/// Port of PostgreSQL's `DecodeISO8601Interval`: `P1Y2M3DT4H5M6S` or the
/// alternative format `P0001-02-03T04:05:06`. Works on the raw input, which
/// is case sensitive.
fn decode_iso8601_interval(text: &[u8]) -> std::result::Result<IntervalFields, DtErr> {
    let mut itm = IntervalFields::default();
    if text.len() < 2 || text[0] != b'P' {
        return Err(DtErr::BadFormat);
    }
    let overflow = |ok: bool| {
        if ok {
            Ok(())
        } else {
            Err(DtErr::FieldOverflow)
        }
    };
    let mut pos = 1;
    let mut datepart = true;
    let mut havefield = false;
    while at(text, pos) != 0 {
        if at(text, pos) == b'T' {
            datepart = false;
            havefield = false;
            pos += 1;
            continue;
        }
        let fieldstart = pos;
        let (val, fval, end) = parse_iso8601_number(text, pos)?;
        pos = end;
        let unit = at(text, pos);
        pos += 1;
        if datepart {
            match unit {
                b'Y' => overflow(itm.adjust_years(val, 1) && itm.adjust_fract_years(fval, 1))?,
                b'M' => {
                    overflow(itm.adjust_months(val) && itm.adjust_fract_days(fval, DAYS_PER_MONTH))?
                }
                b'W' => overflow(itm.adjust_days(val, 7) && itm.adjust_fract_days(fval, 7))?,
                b'D' => overflow(
                    itm.adjust_days(val, 1) && itm.adjust_fract_microseconds(fval, USECS_PER_DAY),
                )?,
                b'T' | 0 | b'-' => {
                    if unit != b'-' && iso8601_integer_width(text, fieldstart) == 8 && !havefield {
                        overflow(
                            itm.adjust_years(val / 10000, 1)
                                && itm.adjust_months((val / 100) % 100)
                                && itm.adjust_days(val % 100, 1)
                                && itm.adjust_fract_microseconds(fval, USECS_PER_DAY),
                        )?;
                        if unit == 0 {
                            return Ok(itm);
                        }
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    if havefield {
                        return Err(DtErr::BadFormat);
                    }
                    overflow(itm.adjust_years(val, 1) && itm.adjust_fract_years(fval, 1))?;
                    if unit == 0 {
                        return Ok(itm);
                    }
                    if unit == b'T' {
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    let (val, fval, end) = parse_iso8601_number(text, pos)?;
                    pos = end;
                    overflow(
                        itm.adjust_months(val) && itm.adjust_fract_days(fval, DAYS_PER_MONTH),
                    )?;
                    match at(text, pos) {
                        0 => return Ok(itm),
                        b'T' => {
                            datepart = false;
                            havefield = false;
                            continue;
                        }
                        b'-' => pos += 1,
                        _ => return Err(DtErr::BadFormat),
                    }
                    let (val, fval, end) = parse_iso8601_number(text, pos)?;
                    pos = end;
                    overflow(
                        itm.adjust_days(val, 1)
                            && itm.adjust_fract_microseconds(fval, USECS_PER_DAY),
                    )?;
                    match at(text, pos) {
                        0 => return Ok(itm),
                        b'T' => {
                            datepart = false;
                            havefield = false;
                            continue;
                        }
                        _ => return Err(DtErr::BadFormat),
                    }
                }
                _ => return Err(DtErr::BadFormat),
            }
        } else {
            match unit {
                b'H' => overflow(itm.adjust_microseconds(val, fval, USECS_PER_HOUR))?,
                b'M' => overflow(itm.adjust_microseconds(val, fval, USECS_PER_MINUTE))?,
                b'S' => overflow(itm.adjust_microseconds(val, fval, USECS_PER_SEC))?,
                0 | b':' => {
                    if unit == 0 && iso8601_integer_width(text, fieldstart) == 6 && !havefield {
                        overflow(
                            itm.adjust_microseconds(val / 10000, 0.0, USECS_PER_HOUR)
                                && itm.adjust_microseconds(
                                    (val / 100) % 100,
                                    0.0,
                                    USECS_PER_MINUTE,
                                )
                                && itm.adjust_microseconds(val % 100, 0.0, USECS_PER_SEC)
                                && itm.adjust_fract_microseconds(fval, 1),
                        )?;
                        return Ok(itm);
                    }
                    if havefield {
                        return Err(DtErr::BadFormat);
                    }
                    overflow(itm.adjust_microseconds(val, fval, USECS_PER_HOUR))?;
                    if unit == 0 {
                        return Ok(itm);
                    }
                    let (val, fval, end) = parse_iso8601_number(text, pos)?;
                    pos = end;
                    overflow(itm.adjust_microseconds(val, fval, USECS_PER_MINUTE))?;
                    match at(text, pos) {
                        0 => return Ok(itm),
                        b':' => pos += 1,
                        _ => return Err(DtErr::BadFormat),
                    }
                    let (val, fval, end) = parse_iso8601_number(text, pos)?;
                    pos = end;
                    overflow(itm.adjust_microseconds(val, fval, USECS_PER_SEC))?;
                    if at(text, pos) == 0 {
                        return Ok(itm);
                    }
                    return Err(DtErr::BadFormat);
                }
                _ => return Err(DtErr::BadFormat),
            }
        }
        havefield = true;
    }
    Ok(itm)
}

/// Port of PostgreSQL's `ParseISO8601Number`: reads a number the way C
/// `strtod` does and splits it into integer and fraction parts.
fn parse_iso8601_number(text: &[u8], pos: usize) -> std::result::Result<(i64, f64, usize), DtErr> {
    let c = at(text, pos);
    if !(c.is_ascii_digit() || c == b'-' || c == b'.') {
        return Err(DtErr::BadFormat);
    }
    let (val, end, erange) = c_strtod(text, pos);
    if end == pos || erange {
        return Err(DtErr::BadFormat);
    }
    if val.is_nan() || !(-1.0e15..=1.0e15).contains(&val) {
        return Err(DtErr::FieldOverflow);
    }
    let ipart = if val >= 0.0 {
        val.floor() as i64
    } else {
        -((-val).floor() as i64)
    };
    Ok((ipart, val - ipart as f64, end))
}

fn iso8601_integer_width(text: &[u8], mut pos: usize) -> usize {
    if at(text, pos) == b'-' {
        pos += 1;
    }
    text[pos.min(text.len())..]
        .iter()
        .take_while(|c| c.is_ascii_digit())
        .count()
}

/// C `strtod`: decimal and hexadecimal numbers, exponents, `inf`, `infinity`
/// and `nan`. Returns the value, the end position (`start` when nothing was
/// read) and whether the result overflowed or underflowed.
fn c_strtod(text: &[u8], start: usize) -> (f64, usize, bool) {
    let mut i = start;
    while is_c_space(at(text, i)) {
        i += 1;
    }
    let negative = match at(text, i) {
        b'-' => {
            i += 1;
            true
        }
        b'+' => {
            i += 1;
            false
        }
        _ => false,
    };
    let sign = if negative { -1.0 } else { 1.0 };
    let starts_with = |pos: usize, word: &[u8]| {
        word.iter()
            .enumerate()
            .all(|(k, &c)| at(text, pos + k).to_ascii_lowercase() == c)
    };
    if starts_with(i, b"inf") {
        i += 3;
        if starts_with(i, b"inity") {
            i += 5;
        }
        return (sign * f64::INFINITY, i, false);
    }
    if starts_with(i, b"nan") {
        i += 3;
        if at(text, i) == b'(' {
            let mut j = i + 1;
            while at(text, j).is_ascii_alphanumeric() || at(text, j) == b'_' {
                j += 1;
            }
            if at(text, j) == b')' {
                i = j + 1;
            }
        }
        return (f64::NAN, i, false);
    }
    if at(text, i) == b'0'
        && at(text, i + 1).eq_ignore_ascii_case(&b'x')
        && (at(text, i + 2).is_ascii_hexdigit()
            || (at(text, i + 2) == b'.' && at(text, i + 3).is_ascii_hexdigit()))
    {
        let (value, end, range_error) = c_strtod_hex(text, i + 2);
        return (sign * value, end, range_error);
    }

    let int_start = i;
    while at(text, i).is_ascii_digit() {
        i += 1;
    }
    let int_digits = &text[int_start..i];
    let mut frac_digits: &[u8] = &[];
    if at(text, i) == b'.' {
        let frac_start = i + 1;
        let mut j = frac_start;
        while at(text, j).is_ascii_digit() {
            j += 1;
        }
        frac_digits = &text[frac_start..j];
        if int_digits.is_empty() && frac_digits.is_empty() {
            return (0.0, start, false);
        }
        i = j;
    } else if int_digits.is_empty() {
        return (0.0, start, false);
    }
    let (exponent, end) = c_strtod_exponent(text, i, b'e');
    i = end;
    let digits_or_zero = |digits: &[u8]| {
        if digits.is_empty() {
            "0".to_string()
        } else {
            String::from_utf8_lossy(digits).into_owned()
        }
    };
    let value: f64 = format!(
        "{}.{}e{exponent}",
        digits_or_zero(int_digits),
        digits_or_zero(frac_digits)
    )
    .parse()
    .expect("decimal digits with an exponent always parse as f64");
    let nonzero = int_digits.iter().chain(frac_digits).any(|&c| c != b'0');
    let range_error = value.is_infinite() || (nonzero && (value == 0.0 || value.is_subnormal()));
    (sign * value, i, range_error)
}

fn c_strtod_hex(text: &[u8], start: usize) -> (f64, usize, bool) {
    let mut i = start;
    let mut mantissa = 0.0f64;
    let mut scale = 0i64;
    let mut nonzero = false;
    while at(text, i).is_ascii_hexdigit() {
        let digit = (at(text, i) as char).to_digit(16).unwrap_or(0);
        nonzero |= digit != 0;
        mantissa = mantissa * 16.0 + f64::from(digit);
        i += 1;
    }
    if at(text, i) == b'.' {
        i += 1;
        while at(text, i).is_ascii_hexdigit() {
            let digit = (at(text, i) as char).to_digit(16).unwrap_or(0);
            nonzero |= digit != 0;
            mantissa = mantissa * 16.0 + f64::from(digit);
            scale -= 4;
            i += 1;
        }
    }
    let (exponent, end) = c_strtod_exponent(text, i, b'p');
    let exponent = (exponent + scale).clamp(-100_000, 100_000) as i32;
    let value = mantissa * 2f64.powi(exponent);
    let range_error = value.is_infinite() || (nonzero && (value == 0.0 || value.is_subnormal()));
    (value, end, range_error)
}

/// Reads an optional exponent such as `e-5`. Without digits after the
/// marker, nothing is read.
fn c_strtod_exponent(text: &[u8], start: usize, marker: u8) -> (i64, usize) {
    if at(text, start).to_ascii_lowercase() != marker {
        return (0, start);
    }
    let mut j = start + 1;
    let negative = match at(text, j) {
        b'-' => {
            j += 1;
            true
        }
        b'+' => {
            j += 1;
            false
        }
        _ => false,
    };
    if !at(text, j).is_ascii_digit() {
        return (0, start);
    }
    let mut exponent: i64 = 0;
    while at(text, j).is_ascii_digit() {
        exponent = (exponent * 10 + i64::from(at(text, j) - b'0')).min(1_000_000);
        j += 1;
    }
    (if negative { -exponent } else { exponent }, j)
}

fn micros_in_range(micros: i64) -> bool {
    let days = micros.div_euclid(USECS_PER_DAY) + UNIX_EPOCH_DAYS_FROM_CE;
    let Ok(days) = i32::try_from(days) else {
        return false;
    };
    NaiveDate::from_num_days_from_ce_opt(days)
        .is_some_and(|date| (MIN_YEAR..=MAX_YEAR).contains(&i64::from(date.year())))
}

struct Fields {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    fsec: i64,
    date: NaiveDate,
}

fn fields(micros: i64) -> Fields {
    let days = micros.div_euclid(USECS_PER_DAY);
    let time = micros.rem_euclid(USECS_PER_DAY);
    let date = NaiveDate::from_num_days_from_ce_opt((days + UNIX_EPOCH_DAYS_FROM_CE) as i32)
        .unwrap_or_default();
    Fields {
        year: i64::from(date.year()),
        month: i64::from(date.month()),
        day: i64::from(date.day()),
        hour: time / USECS_PER_HOUR,
        minute: time % USECS_PER_HOUR / USECS_PER_MINUTE,
        second: time % USECS_PER_MINUTE / USECS_PER_SEC,
        fsec: time % USECS_PER_SEC,
        date,
    }
}

fn format_timestamp(ts: Timestamp, with_zone: bool) -> String {
    let micros = match ts {
        Timestamp::Infinity => return "infinity".to_string(),
        Timestamp::NegInfinity => return "-infinity".to_string(),
        Timestamp::Finite(micros) => micros,
    };
    let f = fields(micros);
    let mut out = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        f.year, f.month, f.day, f.hour, f.minute, f.second
    );
    append_fraction(&mut out, f.fsec);
    if with_zone {
        out.push_str("+00");
    }
    out
}

fn format_date(date: Timestamp) -> String {
    match date {
        Timestamp::Infinity => "infinity".to_string(),
        Timestamp::NegInfinity => "-infinity".to_string(),
        Timestamp::Finite(micros) => {
            let f = fields(micros);
            format!("{:04}-{:02}-{:02}", f.year, f.month, f.day)
        }
    }
}

fn format_time(micros: i64) -> String {
    let mut out = format!(
        "{:02}:{:02}:{:02}",
        micros / USECS_PER_HOUR,
        micros % USECS_PER_HOUR / USECS_PER_MINUTE,
        micros % USECS_PER_MINUTE / USECS_PER_SEC
    );
    append_fraction(&mut out, micros % USECS_PER_SEC);
    out
}

fn append_fraction(out: &mut String, fsec: i64) {
    if fsec != 0 {
        let digits = format!("{:06}", fsec.abs());
        out.push('.');
        out.push_str(digits.trim_end_matches('0'));
    }
}

fn timestamp_pl_interval(ts: Timestamp, iv: Interval) -> Result<Timestamp> {
    match (ts, iv) {
        (Timestamp::Infinity, Interval::NegInfinity)
        | (Timestamp::NegInfinity, Interval::Infinity) => Err(timestamp_out_of_range()),
        (Timestamp::Finite(_), Interval::Infinity) => Ok(Timestamp::Infinity),
        (Timestamp::Finite(_), Interval::NegInfinity) => Ok(Timestamp::NegInfinity),
        (Timestamp::Infinity | Timestamp::NegInfinity, _) => Ok(ts),
        (
            Timestamp::Finite(mut micros),
            Interval::Finite {
                months,
                days,
                micros: delta,
            },
        ) => {
            if months != 0 {
                let f = fields(micros);
                let total = f.year * 12 + f.month - 1 + i64::from(months);
                let year = total.div_euclid(12);
                let month = total.rem_euclid(12) + 1;
                let day = f.day.min(days_in_month(year, month));
                let date = i32::try_from(year)
                    .ok()
                    .and_then(|year| NaiveDate::from_ymd_opt(year, month as u32, day as u32))
                    .ok_or_else(timestamp_out_of_range)?;
                let time = micros.rem_euclid(USECS_PER_DAY);
                micros = (i64::from(date.num_days_from_ce()) - UNIX_EPOCH_DAYS_FROM_CE)
                    * USECS_PER_DAY
                    + time;
                if micros < PG_MIN_MICROS {
                    return Err(timestamp_out_of_range());
                }
            }
            micros = i64::from(days)
                .checked_mul(USECS_PER_DAY)
                .and_then(|d| micros.checked_add(d))
                .filter(|m| *m >= PG_MIN_MICROS)
                .and_then(|m| m.checked_add(delta))
                .ok_or_else(timestamp_out_of_range)?;
            if !micros_in_range(micros) {
                return Err(timestamp_out_of_range());
            }
            Ok(Timestamp::Finite(micros))
        }
    }
}

fn days_in_month(year: i64, month: i64) -> i64 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first = NaiveDate::from_ymd_opt(year as i32, month as u32, 1);
    let next = NaiveDate::from_ymd_opt(next_year as i32, next_month as u32, 1);
    match (first, next) {
        (Some(first), Some(next)) => (next - first).num_days(),
        _ => 31,
    }
}

fn timestamp_mi(a: Timestamp, b: Timestamp) -> Result<Interval> {
    match (a, b) {
        (Timestamp::NegInfinity, Timestamp::NegInfinity)
        | (Timestamp::Infinity, Timestamp::Infinity) => Err(interval_out_of_range()),
        (Timestamp::NegInfinity, _) | (_, Timestamp::Infinity) => Ok(Interval::NegInfinity),
        (Timestamp::Infinity, _) | (_, Timestamp::NegInfinity) => Ok(Interval::Infinity),
        (Timestamp::Finite(a), Timestamp::Finite(b)) => {
            let mut micros = a - b;
            let mut days = micros / USECS_PER_DAY;
            micros -= days * USECS_PER_DAY;
            if days > 0 && micros < 0 {
                micros += USECS_PER_DAY;
                days -= 1;
            } else if days < 0 && micros > 0 {
                micros -= USECS_PER_DAY;
                days += 1;
            }
            Ok(Interval::Finite {
                months: 0,
                days: days as i32,
                micros,
            })
        }
    }
}

fn interval_pl(a: Interval, b: Interval) -> Result<Interval> {
    match (a, b) {
        (Interval::NegInfinity, Interval::Infinity)
        | (Interval::Infinity, Interval::NegInfinity) => Err(interval_out_of_range()),
        (Interval::NegInfinity, _) | (_, Interval::NegInfinity) => Ok(Interval::NegInfinity),
        (Interval::Infinity, _) | (_, Interval::Infinity) => Ok(Interval::Infinity),
        (
            Interval::Finite {
                months: m1,
                days: d1,
                micros: u1,
            },
            Interval::Finite {
                months: m2,
                days: d2,
                micros: u2,
            },
        ) => finite_interval(m1.checked_add(m2), d1.checked_add(d2), u1.checked_add(u2)),
    }
}

fn interval_mi(a: Interval, b: Interval) -> Result<Interval> {
    match (a, b) {
        (Interval::NegInfinity, Interval::NegInfinity)
        | (Interval::Infinity, Interval::Infinity) => Err(interval_out_of_range()),
        (Interval::NegInfinity, _) | (_, Interval::Infinity) => Ok(Interval::NegInfinity),
        (Interval::Infinity, _) | (_, Interval::NegInfinity) => Ok(Interval::Infinity),
        (
            Interval::Finite {
                months: m1,
                days: d1,
                micros: u1,
            },
            Interval::Finite {
                months: m2,
                days: d2,
                micros: u2,
            },
        ) => finite_interval(m1.checked_sub(m2), d1.checked_sub(d2), u1.checked_sub(u2)),
    }
}

fn interval_um(iv: Interval) -> Result<Interval> {
    match iv {
        Interval::Infinity => Ok(Interval::NegInfinity),
        Interval::NegInfinity => Ok(Interval::Infinity),
        Interval::Finite {
            months,
            days,
            micros,
        } => finite_interval(
            months.checked_neg(),
            days.checked_neg(),
            micros.checked_neg(),
        ),
    }
}

/// PostgreSQL's `interval_mul`. The whole part of each field is multiplied;
/// a fraction of a month moves to days (30 per month) and a fraction of a
/// day moves to the time part. Fractions never move up to larger units.
fn interval_mul(span: Interval, factor: f64) -> Result<Interval> {
    if factor.is_nan() {
        return Err(interval_out_of_range());
    }
    let Interval::Finite {
        months,
        days,
        micros,
    } = span
    else {
        if factor == 0.0 {
            return Err(interval_out_of_range());
        }
        return if factor < 0.0 {
            interval_um(span)
        } else {
            Ok(span)
        };
    };
    if factor.is_infinite() {
        let sign = finite_interval_sign(months, days, micros);
        if sign == 0 {
            return Err(interval_out_of_range());
        }
        return Ok(infinite_interval(factor * sign as f64 > 0.0));
    }
    scale_interval(months, days, micros, |field| field * factor)
}

/// PostgreSQL's `interval_div`, with the same moving of fractions as
/// [interval_mul].
fn interval_div(span: Interval, factor: f64) -> Result<Interval> {
    if factor.is_nan() {
        return Err(interval_out_of_range());
    }
    if factor == 0.0 {
        return Err(LimboError::DivisionByZero);
    }
    let Interval::Finite {
        months,
        days,
        micros,
    } = span
    else {
        if factor.is_infinite() {
            return Err(interval_out_of_range());
        }
        return if factor < 0.0 {
            interval_um(span)
        } else {
            Ok(span)
        };
    };
    if factor.is_infinite() {
        return Ok(Interval::Finite {
            months: 0,
            days: 0,
            micros: 0,
        });
    }
    scale_interval(months, days, micros, |field| field / factor)
}

fn scale_interval(
    months: i32,
    days: i32,
    micros: i64,
    scale: impl Fn(f64) -> f64,
) -> Result<Interval> {
    const SECS_PER_DAY: f64 = 86_400.0;
    let ts_round = |value: f64| (value * 1_000_000.0).round_ties_even() / 1_000_000.0;
    let fits_i32 = |value: f64| value >= i32::MIN as f64 && value < -(i32::MIN as f64);
    let scaled_months = scale(months as f64);
    let scaled_days = scale(days as f64);
    if !fits_i32(scaled_months) || !fits_i32(scaled_days) {
        return Err(interval_out_of_range());
    }
    let result_months = scaled_months as i32;
    let mut result_days = scaled_days as i32;
    let month_remainder_days =
        ts_round((scaled_months - result_months as f64) * DAYS_PER_MONTH as f64);
    let mut sec_remainder = ts_round(
        (scaled_days - result_days as f64 + month_remainder_days
            - (month_remainder_days as i32) as f64)
            * SECS_PER_DAY,
    );
    if sec_remainder.abs() >= SECS_PER_DAY {
        let whole_days = (sec_remainder / SECS_PER_DAY) as i32;
        result_days = result_days
            .checked_add(whole_days)
            .ok_or_else(interval_out_of_range)?;
        sec_remainder -= whole_days as f64 * SECS_PER_DAY;
    }
    result_days = result_days
        .checked_add(month_remainder_days as i32)
        .ok_or_else(interval_out_of_range)?;
    let result_micros =
        (scale(micros as f64) + sec_remainder * USECS_PER_SEC as f64).round_ties_even();
    if !(result_micros >= i64::MIN as f64 && result_micros < -(i64::MIN as f64)) {
        return Err(interval_out_of_range());
    }
    finite_interval(
        Some(result_months),
        Some(result_days),
        Some(result_micros as i64),
    )
}

fn finite_interval_sign(months: i32, days: i32, micros: i64) -> i32 {
    let span = micros as i128
        + days as i128 * USECS_PER_DAY as i128
        + months as i128 * DAYS_PER_MONTH as i128 * USECS_PER_DAY as i128;
    span.signum() as i32
}

fn finite_interval(
    months: Option<i32>,
    days: Option<i32>,
    micros: Option<i64>,
) -> Result<Interval> {
    match (months, days, micros) {
        (Some(months), Some(days), Some(micros))
            if (months, days, micros) != (i32::MAX, i32::MAX, i64::MAX)
                && (months, days, micros) != (i32::MIN, i32::MIN, i64::MIN) =>
        {
            Ok(Interval::Finite {
                months,
                days,
                micros,
            })
        }
        _ => Err(interval_out_of_range()),
    }
}

fn format_interval(iv: Interval) -> String {
    let (months, days, micros) = match iv {
        Interval::Infinity => return "infinity".to_string(),
        Interval::NegInfinity => return "-infinity".to_string(),
        Interval::Finite {
            months,
            days,
            micros,
        } => (i64::from(months), i64::from(days), micros),
    };
    let mut out = String::new();
    let mut is_zero = true;
    let mut is_before = false;
    for (value, unit) in [(months / 12, "year"), (months % 12, "mon"), (days, "day")] {
        if value == 0 {
            continue;
        }
        if !is_zero {
            out.push(' ');
        }
        if is_before && value > 0 {
            out.push('+');
        }
        out.push_str(&format!(
            "{value} {unit}{}",
            if value != 1 { "s" } else { "" }
        ));
        is_before = value < 0;
        is_zero = false;
    }
    if is_zero || micros != 0 {
        let abs = micros.unsigned_abs();
        let hours = abs / USECS_PER_HOUR as u64;
        let minutes = abs % USECS_PER_HOUR as u64 / USECS_PER_MINUTE as u64;
        let seconds = abs % USECS_PER_MINUTE as u64 / USECS_PER_SEC as u64;
        let fsec = abs % USECS_PER_SEC as u64;
        if !is_zero {
            out.push(' ');
        }
        if micros < 0 {
            out.push('-');
        } else if is_before {
            out.push('+');
        }
        out.push_str(&format!("{hours:02}:{minutes:02}:{seconds:02}"));
        append_fraction(&mut out, fsec as i64);
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Keyword {
    AmDots,
    AmDotsLower,
    Am,
    AmLower,
    AdDots,
    AdDotsLower,
    Ad,
    AdLower,
    Cc,
    DayUpper,
    DayCap,
    DayLower,
    Ddd,
    Dd,
    DyUpper,
    DyCap,
    DyLower,
    D,
    Ff(u32),
    Fx,
    Hh24,
    Hh12,
    Iddd,
    Id,
    Iw,
    Iyyy,
    Iyy,
    Iy,
    I,
    J,
    Mi,
    Mm,
    MonthUpper,
    MonthCap,
    MonthLower,
    MonUpper,
    MonCap,
    MonLower,
    Ms,
    Of,
    Q,
    RmUpper,
    RmLower,
    Ssss,
    Ss,
    Tzh,
    Tzm,
    TzUpper,
    TzLower,
    Us,
    Ww,
    W,
    YCommaYyy,
    Yyyy,
    Yyy,
    Yy,
    Y,
}

const KEYWORDS: &[(&str, Keyword)] = &[
    ("A.D.", Keyword::AdDots),
    ("A.M.", Keyword::AmDots),
    ("AD", Keyword::Ad),
    ("AM", Keyword::Am),
    ("B.C.", Keyword::AdDots),
    ("BC", Keyword::Ad),
    ("CC", Keyword::Cc),
    ("DAY", Keyword::DayUpper),
    ("DDD", Keyword::Ddd),
    ("DD", Keyword::Dd),
    ("DY", Keyword::DyUpper),
    ("Day", Keyword::DayCap),
    ("Dy", Keyword::DyCap),
    ("D", Keyword::D),
    ("FF1", Keyword::Ff(1)),
    ("FF2", Keyword::Ff(2)),
    ("FF3", Keyword::Ff(3)),
    ("FF4", Keyword::Ff(4)),
    ("FF5", Keyword::Ff(5)),
    ("FF6", Keyword::Ff(6)),
    ("FX", Keyword::Fx),
    ("HH24", Keyword::Hh24),
    ("HH12", Keyword::Hh12),
    ("HH", Keyword::Hh12),
    ("IDDD", Keyword::Iddd),
    ("ID", Keyword::Id),
    ("IW", Keyword::Iw),
    ("IYYY", Keyword::Iyyy),
    ("IYY", Keyword::Iyy),
    ("IY", Keyword::Iy),
    ("I", Keyword::I),
    ("J", Keyword::J),
    ("MI", Keyword::Mi),
    ("MM", Keyword::Mm),
    ("MONTH", Keyword::MonthUpper),
    ("MON", Keyword::MonUpper),
    ("MS", Keyword::Ms),
    ("Month", Keyword::MonthCap),
    ("Mon", Keyword::MonCap),
    ("OF", Keyword::Of),
    ("P.M.", Keyword::AmDots),
    ("PM", Keyword::Am),
    ("Q", Keyword::Q),
    ("RM", Keyword::RmUpper),
    ("SSSSS", Keyword::Ssss),
    ("SSSS", Keyword::Ssss),
    ("SS", Keyword::Ss),
    ("TZH", Keyword::Tzh),
    ("TZM", Keyword::Tzm),
    ("TZ", Keyword::TzUpper),
    ("US", Keyword::Us),
    ("WW", Keyword::Ww),
    ("W", Keyword::W),
    ("Y,YYY", Keyword::YCommaYyy),
    ("YYYY", Keyword::Yyyy),
    ("YYY", Keyword::Yyy),
    ("YY", Keyword::Yy),
    ("Y", Keyword::Y),
    ("a.d.", Keyword::AdDotsLower),
    ("a.m.", Keyword::AmDotsLower),
    ("ad", Keyword::AdLower),
    ("am", Keyword::AmLower),
    ("b.c.", Keyword::AdDotsLower),
    ("bc", Keyword::AdLower),
    ("cc", Keyword::Cc),
    ("day", Keyword::DayLower),
    ("ddd", Keyword::Ddd),
    ("dd", Keyword::Dd),
    ("dy", Keyword::DyLower),
    ("d", Keyword::D),
    ("ff1", Keyword::Ff(1)),
    ("ff2", Keyword::Ff(2)),
    ("ff3", Keyword::Ff(3)),
    ("ff4", Keyword::Ff(4)),
    ("ff5", Keyword::Ff(5)),
    ("ff6", Keyword::Ff(6)),
    ("fx", Keyword::Fx),
    ("hh24", Keyword::Hh24),
    ("hh12", Keyword::Hh12),
    ("hh", Keyword::Hh12),
    ("iddd", Keyword::Iddd),
    ("id", Keyword::Id),
    ("iw", Keyword::Iw),
    ("iyyy", Keyword::Iyyy),
    ("iyy", Keyword::Iyy),
    ("iy", Keyword::Iy),
    ("i", Keyword::I),
    ("j", Keyword::J),
    ("mi", Keyword::Mi),
    ("mm", Keyword::Mm),
    ("month", Keyword::MonthLower),
    ("mon", Keyword::MonLower),
    ("ms", Keyword::Ms),
    ("of", Keyword::Of),
    ("p.m.", Keyword::AmDotsLower),
    ("pm", Keyword::AmLower),
    ("q", Keyword::Q),
    ("rm", Keyword::RmLower),
    ("sssss", Keyword::Ssss),
    ("ssss", Keyword::Ssss),
    ("ss", Keyword::Ss),
    ("tzh", Keyword::Tzh),
    ("tzm", Keyword::Tzm),
    ("tz", Keyword::TzLower),
    ("us", Keyword::Us),
    ("ww", Keyword::Ww),
    ("w", Keyword::W),
    ("y,yyy", Keyword::YCommaYyy),
    ("yyyy", Keyword::Yyyy),
    ("yyy", Keyword::Yyy),
    ("yy", Keyword::Yy),
    ("y", Keyword::Y),
];

const MONTH_NAMES: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

const DAY_NAMES: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

const ROMAN_MONTHS: [&str; 12] = [
    "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII",
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Ordinal {
    None,
    Upper,
    Lower,
}

/// Renders a timestamp with PostgreSQL's `to_char` template patterns, in UTC.
fn format_to_char(micros: i64, with_zone: bool, format: &str) -> String {
    let f = fields(micros);
    let mut out = String::new();
    let mut rest = format;
    while !rest.is_empty() {
        let mut fill_mode = false;
        let mut translation_mode = false;
        if let Some(after) = rest.strip_prefix("FM").or_else(|| rest.strip_prefix("fm")) {
            fill_mode = true;
            rest = after;
        } else if let Some(after) = rest.strip_prefix("TM").or_else(|| rest.strip_prefix("tm")) {
            translation_mode = true;
            rest = after;
        }
        if rest.is_empty() {
            break;
        }
        if let Some(&(name, keyword)) = KEYWORDS.iter().find(|(name, _)| rest.starts_with(name)) {
            rest = &rest[name.len()..];
            let mut ordinal = Ordinal::None;
            if let Some(after) = rest.strip_prefix("TH") {
                ordinal = Ordinal::Upper;
                rest = after;
            } else if let Some(after) = rest.strip_prefix("th") {
                ordinal = Ordinal::Lower;
                rest = after;
            } else if let Some(after) = rest.strip_prefix("SP") {
                rest = after;
            }
            let pad_names = !fill_mode && !translation_mode;
            render_keyword(
                &mut out, keyword, &f, with_zone, fill_mode, pad_names, ordinal,
            );
            continue;
        }
        let mut chars = rest.chars();
        let c = chars.next().unwrap_or_default();
        if c == '"' {
            let mut literal = chars.as_str();
            loop {
                let mut inner = literal.chars();
                match inner.next() {
                    None => break,
                    Some('"') => {
                        literal = inner.as_str();
                        break;
                    }
                    Some('\\') if !inner.as_str().is_empty() => {
                        let escaped = inner.next().unwrap_or_default();
                        out.push(escaped);
                    }
                    Some(other) => out.push(other),
                }
                literal = inner.as_str();
            }
            rest = literal;
            continue;
        }
        if c == '\\' && chars.as_str().starts_with('"') {
            out.push('"');
            rest = &chars.as_str()[1..];
            continue;
        }
        out.push(c);
        rest = chars.as_str();
    }
    out
}

fn render_keyword(
    out: &mut String,
    keyword: Keyword,
    f: &Fields,
    with_zone: bool,
    fill_mode: bool,
    pad_names: bool,
    ordinal: Ordinal,
) {
    let width = |w: usize| if fill_mode { 0 } else { w };
    let iso = f.date.iso_week();
    let yday = i64::from(f.date.ordinal());
    let wday = i64::from(f.date.weekday().num_days_from_sunday());
    let pm = f.hour >= 12;
    let month_name = MONTH_NAMES[(f.month - 1) as usize];
    let day_name = DAY_NAMES[wday as usize];
    let name = |text: String| {
        if pad_names {
            format!("{text:<9}")
        } else {
            text
        }
    };
    let number = match keyword {
        Keyword::Am => Some(if pm { "PM" } else { "AM" }.to_string()),
        Keyword::AmLower => Some(if pm { "pm" } else { "am" }.to_string()),
        Keyword::AmDots => Some(if pm { "P.M." } else { "A.M." }.to_string()),
        Keyword::AmDotsLower => Some(if pm { "p.m." } else { "a.m." }.to_string()),
        Keyword::Ad => Some("AD".to_string()),
        Keyword::AdLower => Some("ad".to_string()),
        Keyword::AdDots => Some("A.D.".to_string()),
        Keyword::AdDotsLower => Some("a.d.".to_string()),
        Keyword::MonthUpper => Some(name(month_name.to_ascii_uppercase())),
        Keyword::MonthCap => Some(name(month_name.to_string())),
        Keyword::MonthLower => Some(name(month_name.to_ascii_lowercase())),
        Keyword::MonUpper => Some(month_name[..3].to_ascii_uppercase()),
        Keyword::MonCap => Some(month_name[..3].to_string()),
        Keyword::MonLower => Some(month_name[..3].to_ascii_lowercase()),
        Keyword::DayUpper => Some(name(day_name.to_ascii_uppercase())),
        Keyword::DayCap => Some(name(day_name.to_string())),
        Keyword::DayLower => Some(name(day_name.to_ascii_lowercase())),
        Keyword::DyUpper => Some(day_name[..3].to_ascii_uppercase()),
        Keyword::DyCap => Some(day_name[..3].to_string()),
        Keyword::DyLower => Some(day_name[..3].to_ascii_lowercase()),
        Keyword::RmUpper | Keyword::RmLower => {
            let roman = ROMAN_MONTHS[(f.month - 1) as usize];
            let roman = if keyword == Keyword::RmLower {
                roman.to_ascii_lowercase()
            } else {
                roman.to_string()
            };
            Some(if fill_mode {
                roman
            } else {
                format!("{roman:<4}")
            })
        }
        Keyword::TzUpper => Some(if with_zone { "UTC" } else { "" }.to_string()),
        Keyword::TzLower => Some(if with_zone { "utc" } else { "" }.to_string()),
        Keyword::Tzh => Some("+00".to_string()),
        Keyword::Tzm => Some("00".to_string()),
        Keyword::Of => Some(format!("+{:0w$}", 0, w = width(2))),
        Keyword::Fx => Some(String::new()),
        _ => None,
    };
    if let Some(text) = number {
        out.push_str(&text);
        return;
    }
    let digits = match keyword {
        Keyword::Hh24 => format!("{:0w$}", f.hour, w = width(2)),
        Keyword::Hh12 => {
            let hour = if f.hour % 12 == 0 { 12 } else { f.hour % 12 };
            format!("{hour:0w$}", w = width(2))
        }
        Keyword::Mi => format!("{:0w$}", f.minute, w = width(2)),
        Keyword::Ss => format!("{:0w$}", f.second, w = width(2)),
        Keyword::Ff(n) => {
            let value = f.fsec / 10_i64.pow(6 - n);
            format!("{value:0w$}", w = n as usize)
        }
        Keyword::Ms => format!("{:03}", f.fsec / 1000),
        Keyword::Us => format!("{:06}", f.fsec),
        Keyword::Ssss => (f.hour * 3600 + f.minute * 60 + f.second).to_string(),
        Keyword::Mm => format!("{:0w$}", f.month, w = width(2)),
        Keyword::Ddd => format!("{yday:0w$}", w = width(3)),
        Keyword::Iddd => {
            let iso_yday =
                i64::from(iso.week() - 1) * 7 + i64::from(f.date.weekday().number_from_monday());
            format!("{iso_yday:0w$}", w = width(3))
        }
        Keyword::Dd => format!("{:0w$}", f.day, w = width(2)),
        Keyword::D => (wday + 1).to_string(),
        Keyword::Id => f.date.weekday().number_from_monday().to_string(),
        Keyword::Ww => format!("{:0w$}", (yday - 1) / 7 + 1, w = width(2)),
        Keyword::Iw => format!("{:0w$}", iso.week(), w = width(2)),
        Keyword::Q => ((f.month - 1) / 3 + 1).to_string(),
        Keyword::Cc => format!("{:0w$}", (f.year - 1) / 100 + 1, w = width(2)),
        Keyword::YCommaYyy => format!("{},{:03}", f.year / 1000, f.year % 1000),
        Keyword::Yyyy => format!("{:0w$}", f.year, w = width(4)),
        Keyword::Yyy => format!("{:0w$}", f.year % 1000, w = width(3)),
        Keyword::Yy => format!("{:0w$}", f.year % 100, w = width(2)),
        Keyword::Y => (f.year % 10).to_string(),
        Keyword::Iyyy => format!("{:0w$}", iso.year(), w = width(4)),
        Keyword::Iyy => format!("{:0w$}", iso.year() % 1000, w = width(3)),
        Keyword::Iy => format!("{:0w$}", iso.year() % 100, w = width(2)),
        Keyword::I => (iso.year() % 10).to_string(),
        Keyword::W => ((f.day - 1) / 7 + 1).to_string(),
        Keyword::J => (i64::from(f.date.num_days_from_ce()) + JULIAN_DAYS_FROM_CE).to_string(),
        _ => String::new(),
    };
    out.push_str(&digits);
    if ordinal != Ordinal::None {
        out.push_str(ordinal_suffix(&digits, ordinal == Ordinal::Upper));
    }
}

fn ordinal_suffix(digits: &str, upper: bool) -> &'static str {
    let bytes = digits.as_bytes();
    let last = bytes.last().copied().unwrap_or(b'0');
    let teen = bytes.len() > 1 && bytes[bytes.len() - 2] == b'1';
    let suffix = match (teen, last) {
        (false, b'1') => ["ST", "st"],
        (false, b'2') => ["ND", "nd"],
        (false, b'3') => ["RD", "rd"],
        _ => ["TH", "th"],
    };
    suffix[if upper { 0 } else { 1 }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prop_assert;
    use proptest::prop_assert_eq;

    fn tstz(input: &str) -> std::result::Result<String, DtErr> {
        parse_timestamp(input, 0, true).map(|ts| format_timestamp(ts, true))
    }

    fn interval(input: &str) -> String {
        format_interval(parse_interval(input).unwrap())
    }

    #[test]
    fn input_matches_postgres() {
        let mut mismatches = Vec::new();
        for (cases, convert) in [
            (
                TIMESTAMPTZ_CASES,
                timestamptz_value as fn(&Value) -> Result<Value>,
            ),
            (TIMESTAMP_CASES, exec_timestamp_in),
            (INTERVAL_CASES, exec_interval_in),
            (DATE_CASES, exec_date_in),
            (TIME_CASES, exec_time_in),
        ] {
            for (input, expected) in cases {
                let actual = match convert(&text(input)) {
                    Ok(value) => format!("ok {value}"),
                    Err(err) => {
                        let message = error_message(&err);
                        format!("{} {message}", error_sqlstate(&message))
                    }
                };
                if actual != *expected {
                    mismatches.push(format!("{input:?}: expected {expected:?}, got {actual:?}"));
                }
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    fn exec_interval_in(value: &Value) -> Result<Value> {
        let input = value.to_string();
        parse_interval(&input)
            .map(|iv| Value::build_text(format_interval(iv)))
            .map_err(|err| input_error(err, "interval", &input))
    }

    const TIMESTAMPTZ_CASES: &[(&str, &str)] = &[
        ("2026-10-08 14:00:00+02", "ok 2026-10-08 12:00:00+00"),
        ("2026-10-08T12:00:00Z", "ok 2026-10-08 12:00:00+00"),
        ("2026-10-08\u{b}12:00", "ok 2026-10-08 12:00:00+00"),
        ("2026-10-08\u{c}12:00", "ok 2026-10-08 12:00:00+00"),
        ("2026-10-08,12:00", "ok 2026-10-08 12:00:00+00"),
        ("2026-10-08 @12:00", "ok 2026-10-08 12:00:00+00"),
        ("2026-10-08 \u{e9}", "22007 invalid input syntax for type timestamp with time zone: \"2026-10-08 \u{e9}\""),
        ("2026.10.08", "ok 2026-10-08 00:00:00+00"),
        ("2026/10/08", "ok 2026-10-08 00:00:00+00"),
        ("10/08/2026", "ok 2026-10-08 00:00:00+00"),
        ("8/oct/2026", "ok 2026-10-08 00:00:00+00"),
        ("8-oct-2026", "ok 2026-10-08 00:00:00+00"),
        ("oct-08-2026", "ok 2026-10-08 00:00:00+00"),
        ("oct/08/2026", "ok 2026-10-08 00:00:00+00"),
        ("oct.08.2026", "ok 2026-10-08 00:00:00+00"),
        ("jan1 2026", "ok 2026-01-01 00:00:00+00"),
        ("2026-01-01 pm3", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 pm3\""),
        ("2026-01-01 12:00 + 02", "ok 2026-01-01 10:00:00+00"),
        ("2026-01-01 12:00 +", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:00 +\""),
        ("2026-01-01 12:00 - infinity", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:00 - infinity\""),
        ("+ infinity", "ok infinity"),
        ("- infinity", "ok -infinity"),
        ("2026-1-1-1", "22007 invalid input syntax for type timestamp with time zone: \"2026-1-1-1\""),
        ("2026--01-01", "ok 2026-01-01 00:00:00+00"),
        ("2026-01-01-", "ok 2026-01-01 00:00:00+00"),
        ("1.2.3", "ok 2003-01-02 00:00:00+00"),
        ("12.5 2026-01-01", "22007 invalid input syntax for type timestamp with time zone: \"12.5 2026-01-01\""),
        ("jan 1 2026 at at at at at at at at at at at at at at at at at at at at at at", "ok 2026-01-01 00:00:00+00"),
        ("jan 1 2026 at at at at at at at at at at at at at at at at at at at at at at at", "22007 invalid input syntax for type timestamp with time zone: \"jan 1 2026 at at at at at at at at at at at at at at at at at at at at at at at\""),
        ("J2461322", "ok 2026-10-08 00:00:00+00"),
        ("j2461322 12:00", "ok 2026-10-08 12:00:00+00"),
        ("J2461322.25", "ok 2026-10-08 06:00:00+00"),
        ("julian 2461322", "ok 2026-10-08 00:00:00+00"),
        ("jd 2461322", "ok 2026-10-08 00:00:00+00"),
        ("j2461322-05", "ok 2026-10-08 05:00:00+00"),
        ("j2461322-99", "22009 time zone displacement out of range: \"j2461322-99\""),
        ("j2461322/05", "22007 invalid input syntax for type timestamp with time zone: \"j2461322/05\""),
        ("j 99999999999", "22008 date/time field value out of range: \"j 99999999999\""),
        ("j2461322 j2461322", "22007 invalid input syntax for type timestamp with time zone: \"j2461322 j2461322\""),
        ("2026-01-01 t123000-05", "ok 2026-01-01 17:30:00+00"),
        ("2026-01-01 t1230-05", "ok 2026-01-01 17:30:00+00"),
        ("2026-01-01 t12-05", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 t12-05\""),
        ("2026-01-01 12:00 t1230-05", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:00 t1230-05\""),
        ("2026-01-01 t1230/05", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 t1230/05\""),
        ("jan 1 2026 1230-05", "ok 2026-01-01 17:30:00+00"),
        ("jan 1 2026 12-05", "22007 invalid input syntax for type timestamp with time zone: \"jan 1 2026 12-05\""),
        ("jan 1 123000-05", "22007 invalid input syntax for type timestamp with time zone: \"jan 1 123000-05\""),
        ("2026-01-01 12:00 etc/utc", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 Etc/GMT+0", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 foo/bar", "22023 time zone \"foo/bar\" not recognized"),
        ("2026-01-01 12:00 utc dst", "ok 2026-01-01 11:00:00+00"),
        ("2026-01-01 12:00 etc/utc dst", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:00 etc/utc dst\""),
        ("2026-01-01 12:00 universal", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:00 universal\""),
        ("2026-01-01 12:00 etc/greenwich", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 Etc/Zulu", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 gmt0", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 GMT+0", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 Etc/GMT-0", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 etc/greenwich dst", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:00 etc/greenwich dst\""),
        ("2026-01-01 12:00 dst", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:00 dst\""),
        ("2026-01-01 12:00 dst dst", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:00 dst dst\""),
        ("j 12:00", "22007 invalid input syntax for type timestamp with time zone: \"j 12:00\""),
        ("2026-01-01 y 12:00", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 y 12:00\""),
        ("2026-01-01 t 12:00", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 24:00:00.000001", "22008 date/time field value out of range: \"2026-01-01 24:00:00.000001\""),
        ("2026-01-01 24:00:01", "22008 date/time field value out of range: \"2026-01-01 24:00:01\""),
        ("2026-01-01 24:00", "ok 2026-01-02 00:00:00+00"),
        ("2026-01-01 12:30", "ok 2026-01-01 12:30:00+00"),
        ("2026-01-01 12:30:45.123", "ok 2026-01-01 12:30:45.123+00"),
        ("2026-01-01 12:30.5", "ok 2026-01-01 00:12:30.5+00"),
        ("2026-01-01 99999999999:00", "22008 date/time field value out of range: \"2026-01-01 99999999999:00\""),
        ("2026-01-01 2147483648:00", "22008 date/time field value out of range: \"2026-01-01 2147483648:00\""),
        ("2026-01-01 25:00", "22008 date/time field value out of range: \"2026-01-01 25:00\""),
        ("2026-01-01 12:60", "22008 date/time field value out of range: \"2026-01-01 12:60\""),
        ("2026-01-01 12:30:61", "22008 date/time field value out of range: \"2026-01-01 12:30:61\""),
        ("2026-01-01 12:30:60", "ok 2026-01-01 12:31:00+00"),
        ("2026-01-01 12:30:45:1", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:30:45:1\""),
        ("2026-01-01 12:", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:30:", "ok 2026-01-01 12:30:00+00"),
        ("2026-01-01 12:30:45.", "ok 2026-01-01 12:30:45+00"),
        ("2026-01-01 12:30:45.1.2", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:30:45.1.2\""),
        ("2026-01-01 12:30:45.1234565", "ok 2026-01-01 12:30:45.123456+00"),
        ("2026-01-01 12:30:45.1234575", "ok 2026-01-01 12:30:45.123458+00"),
        ("2026-01-01 23:59:59.9999995", "ok 2026-01-02 00:00:00+00"),
        ("2026-01-01 1:2:3", "ok 2026-01-01 01:02:03+00"),
        ("2026-01-01 12:00 12:00", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:00 12:00\""),
        ("2026-01-01 t123000", "ok 2026-01-01 12:30:00+00"),
        ("2026-01-01 t1230", "ok 2026-01-01 12:30:00+00"),
        ("2026-01-01 t12", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 t12\""),
        ("2026-01-01 t123000.5", "ok 2026-01-01 12:30:00.5+00"),
        ("2026-01-01 y 5", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 y 5\""),
        ("2026-01-01 h 5", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 h 5\""),
        ("j y 2026", "22007 invalid input syntax for type timestamp with time zone: \"j y 2026\""),
        ("2026-01-01 t t1200", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 t t1200\""),
        ("t12:00", "22007 invalid input syntax for type timestamp with time zone: \"t12:00\""),
        ("jan 1 2026 t 12:00", "ok 2026-01-01 12:00:00+00"),
        ("20260101.5", "22007 invalid input syntax for type timestamp with time zone: \"20260101.5\""),
        ("2026-01-01 123000.5", "ok 2026-01-01 12:30:00.5+00"),
        ("2026-01-01 1230.5", "ok 2026-01-01 12:30:00.5+00"),
        ("20260101 123000", "ok 2026-01-01 12:30:00+00"),
        ("20260101123000", "22008 date/time field value out of range: \"20260101123000\""),
        ("260101", "ok 2026-01-01 00:00:00+00"),
        ("260101 1230", "ok 2026-01-01 12:30:00+00"),
        ("0101", "22007 invalid input syntax for type timestamp with time zone: \"0101\""),
        ("20260101 1230", "ok 2026-01-01 12:30:00+00"),
        ("20260101 12", "22007 invalid input syntax for type timestamp with time zone: \"20260101 12\""),
        ("2026 1 2", "ok 2026-01-02 00:00:00+00"),
        ("1 2 2026", "ok 2026-01-02 00:00:00+00"),
        ("1 2 26", "ok 2026-01-02 00:00:00+00"),
        ("1 2 69", "ok 2069-01-02 00:00:00+00"),
        ("1 2 70", "ok 1970-01-02 00:00:00+00"),
        ("1 2 99", "ok 1999-01-02 00:00:00+00"),
        ("1 2 100", "ok 0100-01-02 00:00:00+00"),
        ("1 2 0", "ok 2000-01-02 00:00:00+00"),
        ("1 2 00", "ok 2000-01-02 00:00:00+00"),
        ("2026 100", "ok 2026-04-10 00:00:00+00"),
        ("2026.100", "ok 2026-04-10 00:00:00+00"),
        ("2026 366", "ok 2027-01-01 00:00:00+00"),
        ("2024 366", "ok 2024-12-31 00:00:00+00"),
        ("2026 367", "22008 date/time field value out of range: \"2026 367\""),
        ("2026 001", "ok 2026-01-01 00:00:00+00"),
        ("1 2 2026 3", "22007 invalid input syntax for type timestamp with time zone: \"1 2 2026 3\""),
        ("jan 2 26", "ok 2026-01-02 00:00:00+00"),
        ("jan 2 2026", "ok 2026-01-02 00:00:00+00"),
        ("2 jan 2026", "ok 2026-01-02 00:00:00+00"),
        ("jan 2026 05", "ok 2026-01-05 00:00:00+00"),
        ("26 jan 2026", "ok 2026-01-26 00:00:00+00"),
        ("1 jan 2026", "ok 2026-01-01 00:00:00+00"),
        ("31 dec 2026", "ok 2026-12-31 00:00:00+00"),
        ("32 dec 2026", "22007 invalid input syntax for type timestamp with time zone: \"32 dec 2026\""),
        ("jan jan 2026", "22007 invalid input syntax for type timestamp with time zone: \"jan jan 2026\""),
        ("2026 jan 2", "ok 2026-01-02 00:00:00+00"),
        ("2026-jan-02", "ok 2026-01-02 00:00:00+00"),
        ("02-jan-2026", "ok 2026-01-02 00:00:00+00"),
        ("02-jan-26", "ok 2026-01-02 00:00:00+00"),
        ("jan-02-26", "ok 2026-01-02 00:00:00+00"),
        ("2026-02-jan", "22007 invalid input syntax for type timestamp with time zone: \"2026-02-jan\""),
        ("1-2-3", "ok 2003-01-02 00:00:00+00"),
        ("2026-01-01 99999999999", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 99999999999\""),
        ("1/2/2026 12.5", "22007 invalid input syntax for type timestamp with time zone: \"1/2/2026 12.5\""),
        ("2026-01-01 12.5", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12.5\""),
        ("2026.01.01 12:00", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00+1", "ok 2026-01-01 11:00:00+00"),
        ("2026-01-01 12:00+12", "ok 2026-01-01 00:00:00+00"),
        ("2026-01-01 12:00+123", "ok 2026-01-01 10:37:00+00"),
        ("2026-01-01 12:00+1234", "ok 2025-12-31 23:26:00+00"),
        ("2026-01-01 12:00+123456", "22009 time zone displacement out of range: \"2026-01-01 12:00+123456\""),
        ("2026-01-01 12:00+12:30", "ok 2025-12-31 23:30:00+00"),
        ("2026-01-01 12:00+12:30:45", "ok 2025-12-31 23:29:15+00"),
        ("2026-01-01 12:00-15:59:59", "ok 2026-01-02 03:59:59+00"),
        ("2026-01-01 12:00+16", "22009 time zone displacement out of range: \"2026-01-01 12:00+16\""),
        ("2026-01-01 12:00-16:00", "22009 time zone displacement out of range: \"2026-01-01 12:00-16:00\""),
        ("2026-01-01 12:00+12:60", "22009 time zone displacement out of range: \"2026-01-01 12:00+12:60\""),
        ("2026-01-01 12:00+12:30:60", "22009 time zone displacement out of range: \"2026-01-01 12:00+12:30:60\""),
        ("2026-01-01 12:00+1:2:3", "ok 2026-01-01 10:57:57+00"),
        ("2026-01-01 12:00+01:30.5", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:00+01:30.5\""),
        ("2026-01-01 12:00+99.5", "22009 time zone displacement out of range: \"2026-01-01 12:00+99.5\""),
        ("2026-01-01 12:00+0130-", "22009 time zone displacement out of range: \"2026-01-01 12:00+0130-\""),
        ("2026-01-01 12:00+99999999999", "22009 time zone displacement out of range: \"2026-01-01 12:00+99999999999\""),
        ("2026-01-01 12:00+00:99999999999", "22009 time zone displacement out of range: \"2026-01-01 12:00+00:99999999999\""),
        ("2026-01-01 12:00 +02 +03", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:00 +02 +03\""),
        ("2026-01-01 12:00 z", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 zulu", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 gmt", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 ut", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 UTC", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 uct", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 wet", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 egst", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 azost", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 12:00 utc +02", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:00 utc +02\""),
        ("2026-01-01 allballs", "ok 2026-01-01 00:00:00+00"),
        ("2026-01-01 12:00 allballs", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 12:00 allballs\""),
        ("epoch", "ok 1970-01-01 00:00:00+00"),
        ("infinity", "ok infinity"),
        ("+infinity", "ok infinity"),
        ("-infinity", "ok -infinity"),
        ("Infinity", "ok infinity"),
        ("infinity 2026", "22007 invalid input syntax for type timestamp with time zone: \"infinity 2026\""),
        ("2026-01-01 epoch", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 epoch\""),
        ("2026-01-01 12:00 am", "ok 2026-01-01 00:00:00+00"),
        ("2026-01-01 12:00 pm", "ok 2026-01-01 12:00:00+00"),
        ("2026-01-01 1:00 pm", "ok 2026-01-01 13:00:00+00"),
        ("2026-01-01 1:00 am", "ok 2026-01-01 01:00:00+00"),
        ("2026-01-01 13:00 pm", "22008 date/time field value out of range: \"2026-01-01 13:00 pm\""),
        ("2026-01-01 0:00 am", "ok 2026-01-01 00:00:00+00"),
        ("2026-01-01 am", "ok 2026-01-01 00:00:00+00"),
        ("2026-01-01 am pm", "22007 invalid input syntax for type timestamp with time zone: \"2026-01-01 am pm\""),
        ("0000-01-01 BC", "22008 date/time field value out of range: \"0000-01-01 BC\""),
        ("0000-01-01", "22008 date/time field value out of range: \"0000-01-01\""),
        ("2026-01-01 ad", "ok 2026-01-01 00:00:00+00"),
        ("thursday 2026-01-01", "22007 invalid input syntax for type timestamp with time zone: \"thursday 2026-01-01\""),
        ("thurs 2026-01-01", "22007 invalid input syntax for type timestamp with time zone: \"thurs 2026-01-01\""),
        ("weds jan 1 2026", "ok 2026-01-01 00:00:00+00"),
        ("tues 2026-01-01", "22007 invalid input syntax for type timestamp with time zone: \"tues 2026-01-01\""),
        ("2026-01-01 at 12:00", "ok 2026-01-01 12:00:00+00"),
        ("on 2026-01-01", "ok 2026-01-01 00:00:00+00"),
        ("foo", "22007 invalid input syntax for type timestamp with time zone: \"foo\""),
        ("nope", "22007 invalid input syntax for type timestamp with time zone: \"nope\""),
        ("12:00", "22007 invalid input syntax for type timestamp with time zone: \"12:00\""),
        ("jan 1", "22007 invalid input syntax for type timestamp with time zone: \"jan 1\""),
        ("2026", "22007 invalid input syntax for type timestamp with time zone: \"2026\""),
        ("", "22007 invalid input syntax for type timestamp with time zone: \"\""),
        ("   ", "22007 invalid input syntax for type timestamp with time zone: \"   \""),
        ("septemberxyz 1 2026", "22007 invalid input syntax for type timestamp with time zone: \"septemberxyz 1 2026\""),
        ("september 1 2026", "ok 2026-09-01 00:00:00+00"),
        ("sept 1 2026", "ok 2026-09-01 00:00:00+00"),
        ("2026-01-00", "22008 date/time field value out of range: \"2026-01-00\""),
        ("2026-02-29", "22008 date/time field value out of range: \"2026-02-29\""),
        ("2024-02-29", "ok 2024-02-29 00:00:00+00"),
        ("2000-02-29", "ok 2000-02-29 00:00:00+00"),
        ("1900-02-29", "22008 date/time field value out of range: \"1900-02-29\""),
        ("2026-13-01", "22008 date/time field value out of range: \"2026-13-01\""),
        ("2026-00-10", "22008 date/time field value out of range: \"2026-00-10\""),
        ("2026-04-31", "22008 date/time field value out of range: \"2026-04-31\""),
        ("2026-01-32", "22008 date/time field value out of range: \"2026-01-32\""),
        ("13/01/2026", "22008 date/time field value out of range: \"13/01/2026\""),
        ("02026-10-08", "ok 2026-10-08 00:00:00+00"),
        ("0001-01-01", "ok 0001-01-01 00:00:00+00"),
        ("9999-12-31 23:59:59.999999", "ok 9999-12-31 23:59:59.999999+00"),
        ("now 1:00", "22007 invalid input syntax for type timestamp with time zone: \"now 1:00\""),
        ("today now", "22007 invalid input syntax for type timestamp with time zone: \"today now\""),
        ("allballs", "22007 invalid input syntax for type timestamp with time zone: \"allballs\""),
        ("today today", "22007 invalid input syntax for type timestamp with time zone: \"today today\""),
        ("now z", "22007 invalid input syntax for type timestamp with time zone: \"now z\""),
    ];

    const TIMESTAMP_CASES: &[(&str, &str)] = &[
        ("2026-10-08 14:00:00+02", "ok 2026-10-08 14:00:00"),
        (
            "2026-10-08 12:00 foo/bar",
            "22023 time zone \"foo/bar\" not recognized",
        ),
        (
            "2026-10-08 12:00 dst",
            "22007 invalid input syntax for type timestamp: \"2026-10-08 12:00 dst\"",
        ),
        (
            "2026-10-08 12:00+16",
            "22009 time zone displacement out of range: \"2026-10-08 12:00+16\"",
        ),
        (
            "9999-12-31 23:59:59.999999-01",
            "ok 9999-12-31 23:59:59.999999",
        ),
        ("0001-01-01 00:00:00+01", "ok 0001-01-01 00:00:00"),
        ("epoch", "ok 1970-01-01 00:00:00"),
        ("-infinity", "ok -infinity"),
        ("2026-10-08 12:00 utc", "ok 2026-10-08 12:00:00"),
        ("j2461322-05", "ok 2026-10-08 00:00:00"),
        ("2026-01-01 t123000-05", "ok 2026-01-01 12:30:00"),
    ];

    const INTERVAL_CASES: &[(&str, &str)] = &[
        ("1 day 1 day", "22007 invalid input syntax for type interval: \"1 day 1 day\""),
        ("1 hour 1 hour", "22007 invalid input syntax for type interval: \"1 hour 1 hour\""),
        ("1:00 1:00", "22007 invalid input syntax for type interval: \"1:00 1:00\""),
        ("1 year 1 year", "22007 invalid input syntax for type interval: \"1 year 1 year\""),
        ("1 month 1 mon", "22007 invalid input syntax for type interval: \"1 month 1 mon\""),
        ("1 week 1 week", "22007 invalid input syntax for type interval: \"1 week 1 week\""),
        ("1 sec 1.5 sec", "22007 invalid input syntax for type interval: \"1 sec 1.5 sec\""),
        ("1.5 sec 1 ms", "22007 invalid input syntax for type interval: \"1.5 sec 1 ms\""),
        ("1 ms 1 sec", "ok 00:00:01.001"),
        ("1 ms 1 ms", "22007 invalid input syntax for type interval: \"1 ms 1 ms\""),
        ("1 us 1 us", "22007 invalid input syntax for type interval: \"1 us 1 us\""),
        ("10 1 sec", "22007 invalid input syntax for type interval: \"10 1 sec\""),
        ("1 day 01:00 2:00", "22007 invalid input syntax for type interval: \"1 day 01:00 2:00\""),
        ("1 decade 1 decade", "22007 invalid input syntax for type interval: \"1 decade 1 decade\""),
        ("1 century 1 century", "22007 invalid input syntax for type interval: \"1 century 1 century\""),
        ("1 millennium 1 millennium", "22007 invalid input syntax for type interval: \"1 millennium 1 millennium\""),
        ("1 minute 1 min", "22007 invalid input syntax for type interval: \"1 minute 1 min\""),
        ("1 day 1 week", "ok 8 days"),
        ("1:00:00 2 hours", "22007 invalid input syntax for type interval: \"1:00:00 2 hours\""),
        ("2 hours 1:00:00", "22007 invalid input syntax for type interval: \"2 hours 1:00:00\""),
        ("1 year 2 decades", "ok 21 years"),
        ("1 day ago", "ok -1 days"),
        ("1 day ago 1 hour", "22007 invalid input syntax for type interval: \"1 day ago 1 hour\""),
        ("ago", "22007 invalid input syntax for type interval: \"ago\""),
        ("1 day 2 hours ago", "ok -1 days -02:00:00"),
        ("@ 1 day", "ok 1 day"),
        ("@", "22007 invalid input syntax for type interval: \"@\""),
        ("@ 1 day ago", "ok -1 days"),
        ("day", "22007 invalid input syntax for type interval: \"day\""),
        ("day day", "22007 invalid input syntax for type interval: \"day day\""),
        ("1 day hour", "22007 invalid input syntax for type interval: \"1 day hour\""),
        ("1 day day", "22007 invalid input syntax for type interval: \"1 day day\""),
        ("5 at 10:00", "22007 invalid input syntax for type interval: \"5 at 10:00\""),
        ("1 quarter", "22007 invalid input syntax for type interval: \"1 quarter\""),
        ("1 timezone", "22007 invalid input syntax for type interval: \"1 timezone\""),
        ("1 microsecond", "ok 00:00:00.000001"),
        ("1 microseconds", "ok 00:00:00.000001"),
        ("1 millisecondsxyz", "ok 00:00:00.001"),
        ("1 microsecondxyz", "ok 00:00:00.000001"),
        ("1 foo", "22007 invalid input syntax for type interval: \"1 foo\""),
        ("1 days 2", "ok 1 day 00:00:02"),
        ("1 Day", "ok 1 day"),
        ("1 DAYS", "ok 1 day"),
        ("1-2", "ok 1 year 2 mons"),
        ("-1-2", "ok -1 years -2 mons"),
        ("1-12", "22015 interval field value out of range: \"1-12\""),
        ("1-11", "ok 1 year 11 mons"),
        ("1-x", "22007 invalid input syntax for type interval: \"1-x\""),
        ("1-2 3", "ok 1 year 2 mons 00:00:03"),
        ("1-2 3 days", "ok 1 year 2 mons 3 days"),
        ("2147483647-11", "22015 interval field value out of range: \"2147483647-11\""),
        ("178956970-7", "ok 178956970 years 7 mons"),
        ("1-2 1-2", "22007 invalid input syntax for type interval: \"1-2 1-2\""),
        ("1-2 1 mon", "22007 invalid input syntax for type interval: \"1-2 1 mon\""),
        ("+1-2", "ok 1 year 2 mons"),
        ("1-2-3", "22007 invalid input syntax for type interval: \"1-2-3\""),
        ("1/2", "22007 invalid input syntax for type interval: \"1/2\""),
        ("1.5 months", "ok 1 mon 15 days"),
        ("0.5 years", "ok 6 mons"),
        ("1.5 weeks", "ok 10 days 12:00:00"),
        ("1.5 days", "ok 1 day 12:00:00"),
        ("1.25 hours", "ok 01:15:00"),
        ("1.5 min", "ok 00:01:30"),
        ("0.5 ms", "ok 00:00:00.0005"),
        ("1.5 us", "ok 00:00:00.000001"),
        ("1.5 decades", "ok 15 years"),
        ("1.5 century", "ok 150 years"),
        ("0.001 millennium", "ok 1 year"),
        ("-1.5 days", "ok -1 days -12:00:00"),
        ("1.5", "ok 00:00:01.5"),
        (".5 day", "ok 12:00:00"),
        ("1.01 months", "ok 1 mon 07:12:00"),
        ("-1.01 months", "ok -1 mons -07:12:00"),
        ("0.1 years", "ok 1 mon"),
        ("0.04 years", "ok 00:00:00"),
        ("-0.04 years", "ok 00:00:00"),
        ("1.5 weeks 1 day", "ok 11 days 12:00:00"),
        ("0.0000001 days", "ok 00:00:00.00864"),
        ("0.0000005 sec", "ok 00:00:00"),
        ("0.0000015 sec", "ok 00:00:00.000001"),
        ("-0.0000015 sec", "ok -00:00:00.000001"),
        ("1.. day", "22007 invalid input syntax for type interval: \"1.. day\""),
        ("1.2.3", "22007 invalid input syntax for type interval: \"1.2.3\""),
        ("1.x day", "22007 invalid input syntax for type interval: \"1.x day\""),
        ("1 day 5", "ok 1 day 00:00:05"),
        ("5", "ok 00:00:05"),
        ("-5", "ok -00:00:05"),
        ("+5", "ok 00:00:05"),
        ("1 day 1:00:00", "ok 1 day 01:00:00"),
        ("-1:00", "ok -01:00:00"),
        ("+1:00:00", "ok 01:00:00"),
        ("1:2", "ok 01:02:00"),
        ("1:2:3.5", "ok 01:02:03.5"),
        ("1:2.5", "ok 00:01:02.5"),
        ("-1:2.5", "ok -00:01:02.5"),
        ("1 2:03:04", "ok 1 day 02:03:04"),
        ("-1 -2:03:04", "ok -1 days -02:03:04"),
        ("-1:99", "22007 invalid input syntax for type interval: \"-1:99\""),
        ("+1:2:3.5", "ok 01:02:03.5"),
        ("1 day -1:99", "22007 invalid input syntax for type interval: \"1 day -1:99\""),
        ("-1:2:3.5", "ok -01:02:03.5"),
        ("1 day -1:00", "ok 1 day -01:00:00"),
        ("100000000:00", "ok 100000000:00:00"),
        ("2562047788:00:54.775807", "ok 2562047788:00:54.775807"),
        ("2562047789:00", "22015 interval field value out of range: \"2562047789:00\""),
        ("-2562047788:00:54.775808", "22007 invalid input syntax for type interval: \"-2562047788:00:54.775808\""),
        ("1 day 25:00", "ok 1 day 25:00:00"),
        ("1:60", "22015 interval field value out of range: \"1:60\""),
        ("1:30:61", "22015 interval field value out of range: \"1:30:61\""),
        ("1:2:3:4", "22007 invalid input syntax for type interval: \"1:2:3:4\""),
        ("-5 days", "ok -5 days"),
        ("-5 days +3 hours", "ok -5 days +03:00:00"),
        ("infinity", "ok infinity"),
        ("-infinity", "ok -infinity"),
        ("+infinity", "ok infinity"),
        ("infinity 1 day", "22007 invalid input syntax for type interval: \"infinity 1 day\""),
        ("1 day infinity", "22007 invalid input syntax for type interval: \"1 day infinity\""),
        ("now", "22007 invalid input syntax for type interval: \"now\""),
        ("epoch", "22007 invalid input syntax for type interval: \"epoch\""),
        ("infinity ago", "22007 invalid input syntax for type interval: \"infinity ago\""),
        ("2147483648 days", "22015 interval field value out of range: \"2147483648 days\""),
        ("2147483647 days 1 day", "22015 interval field value out of range: \"2147483647 days 1 day\""),
        ("2147483647 days", "ok 2147483647 days"),
        ("-2147483648 days", "ok -2147483648 days"),
        ("-2147483648 days ago", "22015 interval field value out of range: \"-2147483648 days ago\""),
        ("9223372036854775807 us", "ok 2562047788:00:54.775807"),
        ("9223372036854775808 us", "22015 interval field value out of range: \"9223372036854775808 us\""),
        ("-9223372036854775808 us", "ok -2562047788:00:54.775808"),
        ("-9223372036854775808 us ago", "22015 interval field value out of range: \"-9223372036854775808 us ago\""),
        ("1000000 years", "ok 1000000 years"),
        ("178956971 years", "22008 interval out of range"),
        ("2147483647 years", "22008 interval out of range"),
        ("2147483648 years", "22015 interval field value out of range: \"2147483648 years\""),
        ("-178956971 years", "22008 interval out of range"),
        ("178956970 years 7 months 2147483647 days 2562047788:00:54.775807", "ok infinity"),
        ("-178956970 years -8 months -2147483648 days -2562047788:00:54.775808", "22007 invalid input syntax for type interval: \"-178956970 years -8 months -2147483648 days -2562047788:00:54.775808\""),
        ("2147483647 months 1 year", "22008 interval out of range"),
        ("3000000000 hours", "22015 interval field value out of range: \"3000000000 hours\""),
        ("153722867280912931 min", "22015 interval field value out of range: \"153722867280912931 min\""),
        ("1e3 days", "22007 invalid input syntax for type interval: \"1e3 days\""),
        ("0.5 years 2147483647 mons", "22015 interval field value out of range: \"0.5 years 2147483647 mons\""),
        ("1 week 2147483647 days", "22015 interval field value out of range: \"1 week 2147483647 days\""),
        ("306783379 weeks", "22015 interval field value out of range: \"306783379 weeks\""),
        ("306783378 weeks", "ok 2147483646 days"),
        ("1.5 2147483647 days", "22015 interval field value out of range: \"1.5 2147483647 days\""),
        ("P1Y2M3DT4H5M6S", "ok 1 year 2 mons 3 days 04:05:06"),
        ("P0001-02-03T04:05:06", "ok 1 year 2 mons 3 days 04:05:06"),
        ("P0001-02-03", "ok 1 year 2 mons 3 days"),
        ("P0001-02", "ok 1 year 2 mons"),
        ("P0001", "ok 1 year"),
        ("P1Y2M", "ok 1 year 2 mons"),
        ("PT1H", "ok 01:00:00"),
        ("PT1.5H", "ok 01:30:00"),
        ("P1.5Y", "ok 1 year 6 mons"),
        ("P1.5M", "ok 1 mon 15 days"),
        ("P1.5W", "ok 10 days 12:00:00"),
        ("P1.5D", "ok 1 day 12:00:00"),
        ("P-1Y-2M", "ok -1 years -2 mons"),
        ("PT-1H-2M-3.5S", "ok -01:02:03.5"),
        ("P20260102", "ok 2026 years 1 mon 2 days"),
        ("P20260102T040506", "ok 2026 years 1 mon 2 days 04:05:06"),
        ("PT040506", "ok 04:05:06"),
        ("PT040506.5", "ok 04:05:06"),
        ("PT04:05:06", "ok 04:05:06"),
        ("PT04:05", "ok 04:05:00"),
        ("PT04", "ok 04:00:00"),
        ("P1Y-2", "22007 invalid input syntax for type interval: \"P1Y-2\""),
        ("p1y", "22007 invalid input syntax for type interval: \"p1y\""),
        ("P", "22007 invalid input syntax for type interval: \"P\""),
        ("PT", "ok 00:00:00"),
        ("P1", "ok 1 year"),
        ("P1X", "22007 invalid input syntax for type interval: \"P1X\""),
        ("P1e2Y", "ok 100 years"),
        ("P0x10Y", "ok 16 years"),
        ("Pinf", "22007 invalid input syntax for type interval: \"Pinf\""),
        ("P1e400Y", "22007 invalid input syntax for type interval: \"P1e400Y\""),
        ("P1e16Y", "22015 interval field value out of range: \"P1e16Y\""),
        ("P.5Y", "ok 6 mons"),
        ("P-.5Y", "ok -6 mons"),
        ("P1Y1Y", "ok 2 years"),
        ("PT1H1H", "ok 02:00:00"),
        ("P1DT", "ok 1 day"),
        ("P1-", "22007 invalid input syntax for type interval: \"P1-\""),
        ("P0001-02-03T", "ok 1 year 2 mons 3 days"),
        ("P2147483648D", "22015 interval field value out of range: \"P2147483648D\""),
        ("P1Y T1H", "22007 invalid input syntax for type interval: \"P1Y T1H\""),
        ("P1W2D", "ok 9 days"),
        ("PT1S2", "22007 invalid input syntax for type interval: \"PT1S2\""),
        ("P1Y2", "22007 invalid input syntax for type interval: \"P1Y2\""),
        ("P0001-02-03T04:05:06.5", "ok 1 year 2 mons 3 days 04:05:06.5"),
        ("P0001-02T04", "ok 1 year 2 mons 04:00:00"),
        ("P0001T04", "ok 1 year 04:00:00"),
        ("PT04:05:06:07", "22007 invalid input syntax for type interval: \"PT04:05:06:07\""),
        ("PT04:05x", "22007 invalid input syntax for type interval: \"PT04:05x\""),
        ("P0001-02-03x", "22007 invalid input syntax for type interval: \"P0001-02-03x\""),
        ("P1.5-2", "ok 1 year 8 mons"),
        ("P1M-2", "22007 invalid input syntax for type interval: \"P1M-2\""),
        ("PT1.5:30", "ok 02:00:00"),
        ("P00010203", "ok 1 year 2 mons 3 days"),
        ("P0001020304", "ok 1020304 years"),
        ("P0001-02-03T040506", "ok 1 year 2 mons 3 days 04:05:06"),
        ("P-0001-02-03", "ok -10 mons +3 days"),
        ("P1nan(abc)Y", "22007 invalid input syntax for type interval: \"P1nan(abc)Y\""),
        ("P0x1p4Y", "ok 16 years"),
        ("P1e-400Y", "22007 invalid input syntax for type interval: \"P1e-400Y\""),
        ("P 1Y", "22007 invalid input syntax for type interval: \"P 1Y\""),
        ("PT1M30.5S", "ok 00:01:30.5"),
        ("P1Y2M3DT4H5M6S ago", "22007 invalid input syntax for type interval: \"P1Y2M3DT4H5M6S ago\""),
        ("P999999999999999Y", "22015 interval field value out of range: \"P999999999999999Y\""),
        ("P1000000000000001D", "22015 interval field value out of range: \"P1000000000000001D\""),
    ];

    const DATE_CASES: &[(&str, &str)] = &[
        ("2026-10-08", "ok 2026-10-08"),
        ("2026-10-08 23:30-02", "ok 2026-10-08"),
        ("2026-10-08 23:30:00.5+14", "ok 2026-10-08"),
        ("20261008", "ok 2026-10-08"),
        ("10/08/2026", "ok 2026-10-08"),
        ("Oct 8 2026", "ok 2026-10-08"),
        ("8 Oct 2026", "ok 2026-10-08"),
        ("0001-01-01", "ok 0001-01-01"),
        ("9999-12-31", "ok 9999-12-31"),
        ("2026-02-29", "22008 date/time field value out of range: \"2026-02-29\""),
        ("2024-02-29", "ok 2024-02-29"),
        ("epoch", "ok 1970-01-01"),
        ("infinity", "ok infinity"),
        ("-infinity", "ok -infinity"),
        ("+infinity", "ok infinity"),
        ("garbage", "22007 invalid input syntax for type date: \"garbage\""),
        ("", "22007 invalid input syntax for type date: \"\""),
        ("   ", "22007 invalid input syntax for type date: \"   \""),
        ("J2461322", "ok 2026-10-08"),
        ("26-10-08", "22008 date/time field value out of range: \"26-10-08\""),
        ("2026-10-08T12:00:00Z", "ok 2026-10-08"),
        ("2026-10-08 25:00", "22008 date/time field value out of range: \"2026-10-08 25:00\""),
        ("allballs", "22007 invalid input syntax for type date: \"allballs\""),
        ("12:00", "22007 invalid input syntax for type date: \"12:00\""),
        ("2026-10-08 foo/bar", "22023 time zone \"foo/bar\" not recognized"),
        ("2026-10-08 etc/utc", "ok 2026-10-08"),
        ("2026-10-08 utc", "ok 2026-10-08"),
        ("2026-10-08 +16", "22009 time zone displacement out of range: \"2026-10-08 +16\""),
        ("2026-13-01", "22008 date/time field value out of range: \"2026-13-01\""),
        ("2026-10-08 12:00 pm", "ok 2026-10-08"),
        ("2026-10-08 13:00 pm", "22008 date/time field value out of range: \"2026-10-08 13:00 pm\""),
        ("2026-10-08 12:00:00.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", "22007 invalid input syntax for type date: \"2026-10-08 12:00:00.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\""),
        ("  2026-10-08  ", "ok 2026-10-08"),
        ("2026-10-08 monday", "ok 2026-10-08"),
        ("2026-281", "ok 2026-10-08"),
        ("2026.281", "ok 2026-10-08"),
    ];

    const TIME_CASES: &[(&str, &str)] = &[
        ("12:00", "ok 12:00:00"),
        ("12:00:00", "ok 12:00:00"),
        ("12:00:00.123456", "ok 12:00:00.123456"),
        ("12:00:00.1234567", "ok 12:00:00.123457"),
        ("12:00:00.9999995", "ok 12:00:01"),
        ("24:00:00", "ok 24:00:00"),
        ("24:00:00.000001", "22008 date/time field value out of range: \"24:00:00.000001\""),
        ("23:59:60", "ok 24:00:00"),
        ("23:59:59.9999995", "ok 24:00:00"),
        ("25:00", "22008 date/time field value out of range: \"25:00\""),
        ("12:60", "22008 date/time field value out of range: \"12:60\""),
        ("12:00:61", "22008 date/time field value out of range: \"12:00:61\""),
        ("-1:00", "22007 invalid input syntax for type time: \"-1:00\""),
        ("0:0:0", "ok 00:00:00"),
        ("2026-10-08 12:30", "ok 12:30:00"),
        ("2026-10-08 12:30:00.500+00", "ok 12:30:00.5"),
        ("2026-10-08T12:30:00Z", "22007 invalid input syntax for type time: \"2026-10-08T12:30:00Z\""),
        ("12:30 2026-10-08", "22007 invalid input syntax for type time: \"12:30 2026-10-08\""),
        ("12:30 2026-10", "22007 invalid input syntax for type time: \"12:30 2026-10\""),
        ("12:30-05", "ok 12:30:00"),
        ("040506-08", "ok 04:05:06"),
        ("0405-08", "ok 04:05:00"),
        ("04-08", "22007 invalid input syntax for type time: \"04-08\""),
        ("12:00 utc", "ok 12:00:00"),
        ("12:00 Etc/UTC", "ok 12:00:00"),
        ("12:00 etc/gmt+0", "ok 12:00:00"),
        ("12:00 foo/bar", "22023 time zone \"foo/bar\" not recognized"),
        ("12:00+02", "ok 12:00:00"),
        ("12:00 +02:30", "ok 12:00:00"),
        ("12:00 +16", "22009 time zone displacement out of range: \"12:00 +16\""),
        ("12:00+02+03", "22007 invalid input syntax for type time: \"12:00+02+03\""),
        ("T040506", "ok 04:05:06"),
        ("T0405", "ok 04:05:00"),
        ("T04", "22007 invalid input syntax for type time: \"T04\""),
        ("T040506.5", "ok 04:05:06.5"),
        ("T12:00", "ok 12:00:00"),
        ("J2451545", "22007 invalid input syntax for type time: \"J2451545\""),
        ("J2451545.5", "ok 12:00:00"),
        ("J2451545 12:00", "ok 12:00:00"),
        ("J2451545.5 12:00", "22007 invalid input syntax for type time: \"J2451545.5 12:00\""),
        ("J", "22007 invalid input syntax for type time: \"J\""),
        ("h 12", "22007 invalid input syntax for type time: \"h 12\""),
        ("123456", "ok 12:34:56"),
        ("1234", "ok 12:34:00"),
        ("12.5", "22007 invalid input syntax for type time: \"12.5\""),
        ("123.5", "22007 invalid input syntax for type time: \"123.5\""),
        ("040506.789", "ok 04:05:06.789"),
        ("2026.10.08 12:00", "ok 12:00:00"),
        ("12.5 2026-10-08", "22007 invalid input syntax for type time: \"12.5 2026-10-08\""),
        ("20261008 12:00", "22007 invalid input syntax for type time: \"20261008 12:00\""),
        ("20261008", "22007 invalid input syntax for type time: \"20261008\""),
        ("allballs", "ok 00:00:00"),
        ("12:00 pm", "ok 12:00:00"),
        ("12:00 am", "ok 00:00:00"),
        ("13:00 pm", "22008 date/time field value out of range: \"13:00 pm\""),
        ("0:30 am", "ok 00:30:00"),
        ("12:00 bc", "ok 12:00:00"),
        ("2026-10-08 bc 12:00", "22007 invalid input syntax for type time: \"2026-10-08 bc 12:00\""),
        ("today", "22007 invalid input syntax for type time: \"today\""),
        ("tomorrow", "22007 invalid input syntax for type time: \"tomorrow\""),
        ("12:00 z", "ok 12:00:00"),
        ("12:00 zulu", "ok 12:00:00"),
        ("12:00 gmt", "ok 12:00:00"),
        ("epoch", "22007 invalid input syntax for type time: \"epoch\""),
        ("infinity", "22007 invalid input syntax for type time: \"infinity\""),
        ("-infinity", "22007 invalid input syntax for type time: \"-infinity\""),
        ("12:00 monday", "22007 invalid input syntax for type time: \"12:00 monday\""),
        ("12:00 jan", "22007 invalid input syntax for type time: \"12:00 jan\""),
        ("12:00 foo", "22007 invalid input syntax for type time: \"12:00 foo\""),
        ("12:00 at", "ok 12:00:00"),
        ("12:00 t", "22007 invalid input syntax for type time: \"12:00 t\""),
        ("t 12:00", "ok 12:00:00"),
        ("12:00 dst", "22007 invalid input syntax for type time: \"12:00 dst\""),
        ("12:00 utc dst", "ok 12:00:00"),
        ("10/08 12:00", "22007 invalid input syntax for type time: \"10/08 12:00\""),
        ("10/08 12:00+02", "22007 invalid input syntax for type time: \"10/08 12:00+02\""),
        ("2026-02-30 12:00", "22008 date/time field value out of range: \"2026-02-30 12:00\""),
        ("0000-01-01 12:00", "22008 date/time field value out of range: \"0000-01-01 12:00\""),
        ("99-01-01 12:00", "22008 date/time field value out of range: \"99-01-01 12:00\""),
        ("", "22007 invalid input syntax for type time: \"\""),
        ("   ", "22007 invalid input syntax for type time: \"   \""),
        ("12:00:00 12:00", "22007 invalid input syntax for type time: \"12:00:00 12:00\""),
        ("garbage", "22007 invalid input syntax for type time: \"garbage\""),
        ("12:00 + 02", "ok 12:00:00"),
        ("12:00 - 02", "ok 12:00:00"),
        ("12:00 +", "22007 invalid input syntax for type time: \"12:00 +\""),
        ("040506 12:00", "22007 invalid input syntax for type time: \"040506 12:00\""),
        ("12:00 040506", "22007 invalid input syntax for type time: \"12:00 040506\""),
        ("4:05:06 PM", "ok 16:05:06"),
        ("04:05 PM", "ok 16:05:00"),
        ("12:00:00.5 pm", "ok 12:00:00.5"),
        ("12 pm", "22007 invalid input syntax for type time: \"12 pm\""),
        ("2026 12:00", "22007 invalid input syntax for type time: \"2026 12:00\""),
        ("12:00 2026", "22007 invalid input syntax for type time: \"12:00 2026\""),
        ("oct 8 12:00", "22007 invalid input syntax for type time: \"oct 8 12:00\""),
        ("12:00 +02", "ok 12:00:00"),
        ("12:00:00.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", "ok 12:00:00"),
        ("12:00:00.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", "ok 12:00:00"),
        ("12:00 ad", "ok 12:00:00"),
        ("12:00 mon", "22007 invalid input syntax for type time: \"12:00 mon\""),
    ];

    #[test]
    fn date_input_outside_supported_years_is_out_of_range() {
        for input in ["10000-01-01", "0001-01-01 BC", "2026-10-08 BC"] {
            let err = exec_date_in(&text(input)).unwrap_err();
            let message = error_message(&err);
            assert_eq!(message, format!("date out of range: \"{input}\""));
            assert_eq!(error_sqlstate(&message), "22008");
        }
    }

    #[test]
    fn time_now_is_current_time_of_day() {
        let before = now_micros().rem_euclid(USECS_PER_DAY);
        let Value::Text(now) = exec_time_in(&text("now")).unwrap() else {
            panic!("time input should return text");
        };
        let now = stored_time_micros(now.as_str()).unwrap();
        let after = now_micros().rem_euclid(USECS_PER_DAY);
        assert!(before <= now && now <= after || after < before);
    }

    #[test]
    fn date_and_time_input_reads_its_own_output() {
        for (cases, convert) in [
            (DATE_CASES, exec_date_in as fn(&Value) -> Result<Value>),
            (TIME_CASES, exec_time_in),
        ] {
            for (_, expected) in cases {
                let Some(stored) = expected.strip_prefix("ok ") else {
                    continue;
                };
                assert_eq!(convert(&text(stored)).unwrap().to_string(), stored);
            }
        }
    }

    #[test]
    fn date_arithmetic_matches_postgres() {
        let int = Value::from_i64;
        let date = || text("2026-10-08");
        let ok = |result: Result<Value>| result.unwrap().to_string();
        let err = |result: Result<Value>| error_message(&result.unwrap_err());

        assert_eq!(ok(exec_date_pl(&date(), &int(1))), "2026-10-09");
        assert_eq!(ok(exec_date_pl(&int(1), &date())), "2026-10-09");
        assert_eq!(ok(exec_date_mi(&date(), &int(1))), "2026-10-07");
        assert_eq!(ok(exec_datetime_pl(&date(), &int(1))), "2026-10-09");
        assert_eq!(ok(exec_datetime_mi(&date(), &int(1))), "2026-10-07");
        assert_eq!(ok(exec_date_mi(&date(), &text("2026-01-01"))), "280");
        assert_eq!(ok(exec_date_pl(&text("infinity"), &int(1))), "infinity");
        assert_eq!(ok(exec_date_mi(&text("-infinity"), &int(1))), "-infinity");
        for (a, b) in [
            ("infinity", "2026-01-01"),
            ("2026-01-01", "-infinity"),
            ("infinity", "infinity"),
        ] {
            assert_eq!(
                err(exec_date_mi(&text(a), &text(b))),
                "cannot subtract infinite dates"
            );
        }
        assert_eq!(
            ok(exec_date_pl(&date(), &text("1 day"))),
            "2026-10-09 00:00:00"
        );
        assert_eq!(
            ok(exec_date_pl(&text("1 day"), &date())),
            "2026-10-09 00:00:00"
        );
        assert_eq!(
            ok(exec_date_mi(&date(), &text("1 hour"))),
            "2026-10-07 23:00:00"
        );
        assert_eq!(
            ok(exec_date_pl(&date(), &text("12:30"))),
            "2026-10-08 12:30:00"
        );
        assert_eq!(
            ok(exec_date_pl(&text("infinity"), &text("1 day"))),
            "infinity"
        );
        assert_eq!(
            ok(exec_date_mi(&text("infinity"), &text("1 day"))),
            "infinity"
        );
        assert_eq!(
            ok(exec_datetime_mi(
                &text("2026-10-08 12:00:00+00"),
                &text("2026-10-01")
            )),
            "7 days 12:00:00"
        );
        assert_eq!(
            ok(exec_datetime_mi(
                &text("2026-10-01"),
                &text("2026-10-08 12:00:00+00")
            )),
            "-7 days -12:00:00"
        );
        assert_eq!(
            err(exec_date_pl(&date(), &int(2_147_483_647))),
            "date out of range"
        );
        assert_eq!(
            err(exec_date_mi(&date(), &int(2_147_483_647))),
            "date out of range"
        );
        assert_eq!(
            err(exec_date_pl(&text("9999-12-31"), &int(1))),
            "date out of range"
        );
        assert_eq!(
            err(exec_date_mi(&date(), &int(2_147_483_648))),
            "operator does not exist: date - bigint"
        );
        assert_eq!(
            error_sqlstate("operator does not exist: date - bigint"),
            "42883"
        );
        assert_eq!(exec_date_pl(&Value::Null, &int(1)).unwrap(), Value::Null);
        assert_eq!(exec_date_mi(&date(), &Value::Null).unwrap(), Value::Null);
    }

    #[test]
    fn wire_binary_matches_postgres_send_functions() {
        let cases: &[(&str, u32, Option<i64>)] = &[
            ("2026-10-08 12:30:00.5+00", 1184, Some(844_777_800_500_000)),
            ("2026-10-08 12:30:00.5", 1184, Some(844_777_800_500_000)),
            ("2026-10-08 12:30:00.500", 1184, Some(844_777_800_500_000)),
            ("infinity", 1184, Some(i64::MAX)),
            ("-infinity", 1114, Some(i64::MIN)),
            ("0001-01-01 00:00:00", 1114, Some(-63_082_281_600_000_000)),
            ("2026-10-08", 1082, Some(9777)),
            ("1999-12-31", 1082, Some(-1)),
            ("infinity", 1082, Some(i64::from(i32::MAX))),
            ("-infinity", 1082, Some(i64::from(i32::MIN))),
            ("24:00:00", 1083, Some(86_400_000_000)),
            ("23:59:59.999999", 1083, Some(86_399_999_999)),
            ("12:30:00.500", 1083, Some(45_000_500_000)),
            ("00:00:00", 1083, Some(0)),
            ("garbage", 1184, None),
            ("now", 1184, None),
            ("1 day", 1114, None),
            ("2026-10-08 12:00:00", 1082, None),
            ("24:00:00.000001", 1083, None),
            ("12:60:00", 1083, None),
            ("12:00", 1083, None),
            ("12:00:00.1234567", 1083, None),
            ("infinity", 1083, None),
            ("2026-10-08", 25, None),
        ];
        for &(text, oid, expected) in cases {
            assert_eq!(datetime_wire_binary(text, oid), expected, "{text} as {oid}");
        }
    }

    #[test]
    fn relative_words_match_postgres() {
        let since_today = |input: &str| {
            let value = exec_timestamptz_in(&text(input)).unwrap();
            let today = exec_timestamptz_in(&text("today")).unwrap();
            exec_datetime_mi(&value, &today).unwrap().to_string()
        };
        assert_eq!(since_today("tomorrow"), "1 day");
        assert_eq!(since_today("yesterday"), "-1 days");
        assert_eq!(since_today("tomorrow 12:00"), "1 day 12:00:00");
        assert_eq!(since_today("today 1:00 +01"), "00:00:00");
        assert_eq!(since_today("today allballs"), "00:00:00");
        assert_eq!(since_today("yesterday 12:00 z"), "-12:00:00");
        let today = timestamptz_value(&text("today")).unwrap().to_string();
        assert_eq!(today, format!("{} 00:00:00+00", &now_text()[..10]));
        let now = exec_timestamptz_in(&text("now")).unwrap();
        let elapsed = exec_datetime_mi(&text(&now_text()), &now)
            .unwrap()
            .to_string();
        assert!(elapsed.starts_with("00:00:00"), "{elapsed}");
    }

    #[test]
    fn timestamptz_input_matches_postgres() {
        for (input, expected) in [
            ("2026-10-08 14:00:00+02", "2026-10-08 12:00:00+00"),
            ("2026-10-08T12:00:00Z", "2026-10-08 12:00:00+00"),
            (
                "2026-10-08T12:00:00.123456+05:30",
                "2026-10-08 06:30:00.123456+00",
            ),
            ("2026-10-08", "2026-10-08 00:00:00+00"),
            ("2026-10-08 +02", "2026-10-07 22:00:00+00"),
            ("20261008", "2026-10-08 00:00:00+00"),
            ("2026-10-08 12:00:00+0230", "2026-10-08 09:30:00+00"),
            ("2026-10-08 12:00:00 +2", "2026-10-08 10:00:00+00"),
            ("2026-10-08 12:00:60", "2026-10-08 12:01:00+00"),
            ("2026-10-08 24:00:00", "2026-10-09 00:00:00+00"),
            (
                "2026-10-08 12:00:00.1234565",
                "2026-10-08 12:00:00.123456+00",
            ),
            (
                "2026-10-08 12:00:00.1234575",
                "2026-10-08 12:00:00.123458+00",
            ),
            ("2026-10-08 12:00:00.9999995", "2026-10-08 12:00:01+00"),
            ("2026-10-08 12:00:00.", "2026-10-08 12:00:00+00"),
            ("2026-10-08 12:00:00 utc", "2026-10-08 12:00:00+00"),
            ("2026-1-8 1:2:3", "2026-01-08 01:02:03+00"),
            ("02026-10-08", "2026-10-08 00:00:00+00"),
            ("2026-10-08 12:00:00-15:59:59", "2026-10-09 03:59:59+00"),
            (" epoch ", "1970-01-01 00:00:00+00"),
            ("Infinity", "infinity"),
            ("-infinity", "-infinity"),
        ] {
            assert_eq!(tstz(input).as_deref(), Ok(expected), "{input}");
        }
    }

    #[test]
    fn timestamptz_input_errors_match_postgres() {
        for (input, expected) in [
            ("nope", DtErr::BadFormat),
            ("2026-10-08T12", DtErr::BadFormat),
            ("2026-02-30", DtErr::FieldOverflow),
            ("2026-10-08 25:00:00", DtErr::FieldOverflow),
            ("2026-10-08 12:60:00", DtErr::FieldOverflow),
            ("2026-10-08 12:00:61", DtErr::FieldOverflow),
            ("2026-10-08 24:00:00.1", DtErr::FieldOverflow),
            ("0000-01-01", DtErr::FieldOverflow),
            ("2026-10-08 12:00:00+16", DtErr::TzOverflow),
            ("2026-10-08 12:00:00+02:60", DtErr::TzOverflow),
            ("2026-10-08 12:00:00+023015", DtErr::TzOverflow),
            ("2026-10-08 12:00:00+00:00:60", DtErr::TzOverflow),
            ("10000-01-01", DtErr::OutOfRange),
            ("9999-12-31 23:59:59.999999-01", DtErr::OutOfRange),
            ("0001-01-01 00:00:00+01", DtErr::OutOfRange),
        ] {
            assert_eq!(tstz(input), Err(expected), "{input}");
        }
    }

    #[test]
    fn interval_input_and_output_match_postgres() {
        for (input, expected) in [
            ("1.5 years", "1 year 6 mons"),
            ("1.01 months", "1 mon 07:12:00"),
            ("-1 day +2 hours", "-1 days +02:00:00"),
            ("1 day ago", "-1 days"),
            ("@ 1 hour", "01:00:00"),
            ("10", "00:00:10"),
            ("1:2", "01:02:00"),
            ("1 2:03:04", "1 day 02:03:04"),
            ("P1Y2M3DT4H5M6.5S", "1 year 2 mons 3 days 04:05:06.5"),
            ("P1W", "7 days"),
            ("1-2", "1 year 2 mons"),
            ("25 hours", "25:00:00"),
            ("1 week 2 days", "9 days"),
            ("-1:30", "-01:30:00"),
            ("1.5 weeks", "10 days 12:00:00"),
            ("1 day -1 hour", "1 day -01:00:00"),
            ("infinity", "infinity"),
        ] {
            assert_eq!(interval(input), expected, "{input}");
        }
    }

    #[test]
    fn timestamp_arithmetic_matches_postgres() {
        let pl = |a: &'static str, b: &'static str| {
            exec_datetime_pl(&Value::build_text(a), &Value::build_text(b))
                .map(|v| v.to_string())
                .map_err(|e| e.to_string())
        };
        let mi = |a: &'static str, b: &'static str| {
            exec_datetime_mi(&Value::build_text(a), &Value::build_text(b))
                .map(|v| v.to_string())
                .map_err(|e| e.to_string())
        };
        assert_eq!(
            pl("2026-10-08 12:00:00+00", "1 day 1 hour").unwrap(),
            "2026-10-09 13:00:00+00"
        );
        assert_eq!(
            pl("1 month", "2026-01-31 00:00:00+00").unwrap(),
            "2026-02-28 00:00:00+00"
        );
        assert_eq!(
            pl("2026-10-08", "1 day 1 hour").unwrap(),
            "2026-10-09 01:00:00"
        );
        assert_eq!(pl("1 day", "2 hours").unwrap(), "1 day 02:00:00");
        assert_eq!(pl("infinity", "1 day").unwrap(), "infinity");
        assert_eq!(
            mi("2026-10-08 12:00:00+00", "2026-10-01 00:00:00+00").unwrap(),
            "7 days 12:00:00"
        );
        assert_eq!(
            mi("2026-10-01 00:00:00+00", "2026-10-08 12:00:00+00").unwrap(),
            "-7 days -12:00:00"
        );
        assert_eq!(
            mi("2026-03-31 00:00:00+00", "1 month").unwrap(),
            "2026-02-28 00:00:00+00"
        );
        assert_eq!(mi("1 day", "2 hours").unwrap(), "1 day -02:00:00");
        assert_eq!(
            mi("infinity", "2026-10-08 00:00:00+00").unwrap(),
            "infinity"
        );
        assert!(mi("infinity", "infinity").is_err());
        assert!(pl("9999-12-31 23:00:00+00", "2 hours").is_err());
        assert!(pl("2026-10-08 12:00:00+00", "2026-10-08 12:00:00+00").is_err());
    }

    #[test]
    fn to_char_matches_postgres() {
        let micros = parse_timestamp("2026-10-08 14:05:09.123456+00", 0, true).unwrap();
        let Timestamp::Finite(micros) = micros else {
            unreachable!()
        };
        for (format, expected) in [
            ("YYYY-MM-DD HH24:MI", "2026-10-08 14:05"),
            ("Day Dy Month Mon", "Thursday  Thu October   Oct"),
            ("FMDay FMMonth DDth", "Thursday October 08th"),
            ("HH12:MI:SS AM TZ OF", "02:05:09 PM UTC +00"),
            ("US MS FF2 J Q WW IW", "123456 123 12 2461322 4 41 41"),
            ("\"Year\" YYYY \\\"x\\\"", "Year 2026 \"x\""),
        ] {
            assert_eq!(format_to_char(micros, true, format), expected, "{format}");
        }
    }

    fn micros_range() -> std::ops::RangeInclusive<i64> {
        let first = NaiveDate::from_ymd_opt(1, 1, 1).unwrap().num_days_from_ce();
        let last = NaiveDate::from_ymd_opt(9999, 12, 31)
            .unwrap()
            .num_days_from_ce();
        let to_micros = |days: i32| (i64::from(days) - UNIX_EPOCH_DAYS_FROM_CE) * USECS_PER_DAY;
        to_micros(first)..=to_micros(last + 1) - 1
    }

    fn text(value: &str) -> Value {
        Value::build_text(value.to_string())
    }

    proptest::proptest! {
        #[test]
        fn arbitrary_input_never_panics(input in "\\PC{0,40}", format in "\\PC{0,20}") {
            let _ = exec_timestamptz_in(&text(&input));
            let _ = exec_timestamp_in(&text(&input));
            let _ = parse_interval(&input);
            let _ = exec_datetime_pl(&text(&input), &text("1 day"));
            let _ = exec_datetime_mi(&text("2026-10-08 12:00:00+00"), &text(&input));
            let _ = to_char_datetime(&text("2026-10-08 12:00:00.5+00"), &format);
        }

        #[test]
        fn datetime_like_input_never_panics(
            input in "[0-9]{1,8}(-[0-9]{1,3}(-[0-9]{1,3})?)?([ Tt][0-9]{1,3}(:[0-9]{1,3}(:[0-9]{1,3}(\\.[0-9]{0,9})?)?)?)?( ?([+-][0-9:]{0,9}|z|utc|bc|ad))?"
        ) {
            let _ = exec_timestamptz_in(&text(&input));
            let _ = exec_timestamp_in(&text(&input));
        }

        #[test]
        fn interval_like_input_never_panics(
            input in "(@ )?([+-]?[0-9]{0,12}(\\.[0-9]{0,8})?(:[0-9]{0,3}){0,2} ?(years?|mons?|days?|hours?|min|s|weeks?|ms|us|centuries|-[0-9]{0,3})? ?){1,4}(ago)?"
        ) {
            if let Ok(iv) = parse_interval(&input) {
                prop_assert_eq!(parse_interval(&format_interval(iv)), Ok(iv));
            }
        }

        #[test]
        fn timestamptz_output_parses_back_to_itself(micros in micros_range()) {
            let formatted = format_timestamp(Timestamp::Finite(micros), true);
            let stored = timestamptz_value(&text(&formatted)).unwrap();
            prop_assert_eq!(stored.to_string(), formatted.as_str());
            prop_assert_eq!(
                decode_stored_datetime(&formatted),
                Some((Timestamp::Finite(micros), DatetimeKind::TimestampTz))
            );
        }

        #[test]
        fn text_order_matches_instant_order(a in micros_range(), b in micros_range()) {
            let (text_a, text_b) = (
                format_timestamp(Timestamp::Finite(a), true),
                format_timestamp(Timestamp::Finite(b), true),
            );
            prop_assert_eq!(text_a.cmp(&text_b), a.cmp(&b));
            prop_assert!(text_a.as_str() < "infinity" && text_a.as_str() > "-infinity");
        }

        #[test]
        fn adding_a_difference_returns_the_other_timestamp(a in micros_range(), b in micros_range()) {
            let (text_a, text_b) = (
                format_timestamp(Timestamp::Finite(a), true),
                format_timestamp(Timestamp::Finite(b), true),
            );
            let difference = exec_datetime_mi(&text(&text_b), &text(&text_a)).unwrap();
            let sum = exec_datetime_pl(&text(&text_a), &difference).unwrap();
            prop_assert_eq!(sum.to_string(), text_b);
        }
    }
}
