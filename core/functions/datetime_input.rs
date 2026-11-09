//! Reads date and time input as PostgreSQL's `date_in`, `time_in`,
//! `timestamp_in` and `timestamptz_in` do, for the ENCODE of the built-in
//! types of the same names. Each value is written in one fixed text form, with
//! a 4-digit year, so the stored texts sort in time order:
//! `2024-01-05`, `10:00:00.5`, `2024-01-05 10:00:00.5`. A timestamptz is
//! written as the same form in UTC, without an offset.
//!
//! The input is split into fields as PostgreSQL's `ParseDateTime` does and the
//! fields are read as its `DecodeDateTime` does, for a subset of its forms.
//! Values whose text would not sort in time order are rejected: BC, years past
//! 9999, years not written with 4 digits and `infinity`.

use crate::types::Value;
use crate::{LimboError, Result};
use chrono::{Datelike, Duration, Month, NaiveDate, NaiveDateTime, Timelike};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemporalType {
    Date,
    Time,
    Timestamp,
    Timestamptz,
}

impl TemporalType {
    /// The type's name in PostgreSQL's error messages.
    fn name(self) -> &'static str {
        match self {
            Self::Date => "date",
            Self::Time => "time",
            Self::Timestamp => "timestamp",
            Self::Timestamptz => "timestamp with time zone",
        }
    }
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// `temporal_encode(value, type)`, the ENCODE of the built-in type `type`:
/// NULL stays NULL, text is read as `type`, anything else is an error, as
/// PostgreSQL has no conversion from numbers or bytes to dates.
pub fn exec_temporal_encode(value: &Value, type_name: &Value) -> Result<Value> {
    let ty = match type_name.to_text() {
        Some("date") => TemporalType::Date,
        Some("time") => TemporalType::Time,
        Some("timestamp") => TemporalType::Timestamp,
        Some("timestamptz") => TemporalType::Timestamptz,
        _ => {
            return Err(LimboError::InvalidArgument(format!(
                "temporal_encode: unknown type {type_name}"
            )))
        }
    };
    let input = match value {
        Value::Null => return Ok(Value::Null),
        Value::Text(text) => text.as_str(),
        Value::Numeric(_) | Value::Blob(_) => {
            return Err(error(format!(
                "only text can be converted to {}",
                ty.name()
            )))
        }
    };
    let fields = Fields::read(input, ty)?;
    Ok(Value::build_text(fields.format(input, ty)?))
}

/// PostgreSQL's frontend reports these as `invalid_datetime_format`. Values
/// out of range are `DatetimeFieldOverflow` instead.
fn error(message: String) -> LimboError {
    LimboError::InvalidDate(message)
}

fn syntax_error(input: &str, ty: TemporalType) -> LimboError {
    error(format!(
        "invalid input syntax for type {}: \"{input}\"",
        ty.name()
    ))
}

fn range_error(input: &str) -> LimboError {
    LimboError::DatetimeFieldOverflow(format!("date/time field value out of range: \"{input}\""))
}

/// A piece of the input, split as PostgreSQL's `ParseDateTime` splits it.
/// Spaces and commas separate fields.
enum Field<'a> {
    /// `2024-01-05` or `2024/1/5`, with its separator.
    Date(&'a str, char),
    /// `10:00`, `9:30:00.5`.
    Time(&'a str),
    /// Digits only, such as `20240105` or the `5` of `Jan 5 2024`.
    Number(&'a str),
    /// Letters only, such as `Jan`, `PM` or the `T` of `2024-01-05T10:00`.
    Word(&'a str),
    /// A UTC offset with its sign: `+02`, `-05:30`, `+0530`.
    Offset(&'a str),
}

fn split_fields(input: &str) -> Option<Vec<Field<'_>>> {
    let bytes = input.as_bytes();
    let skip = |mut i: usize, keep: &dyn Fn(u8) -> bool| {
        while i < bytes.len() && keep(bytes[i]) {
            i += 1;
        }
        i
    };
    let mut fields = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        let c = bytes[i];
        if c.is_ascii_whitespace() || c == b',' {
            i += 1;
        } else if c.is_ascii_digit() {
            i = skip(i, &|c| c.is_ascii_digit());
            match bytes.get(i) {
                Some(b':') => {
                    i = skip(i, &|c| c.is_ascii_digit() || c == b':' || c == b'.');
                    fields.push(Field::Time(&input[start..i]));
                }
                Some(&sep @ (b'-' | b'/')) => {
                    i = skip(i, &|c| c.is_ascii_digit() || c == sep);
                    fields.push(Field::Date(&input[start..i], char::from(sep)));
                }
                _ => fields.push(Field::Number(&input[start..i])),
            }
        } else if c.is_ascii_alphabetic() {
            i = skip(i, &|c| c.is_ascii_alphabetic());
            fields.push(Field::Word(&input[start..i]));
        } else if c == b'+' || c == b'-' {
            i = skip(i + 1, &|c| c.is_ascii_digit() || c == b':');
            fields.push(Field::Offset(&input[start..i]));
        } else {
            return None;
        }
    }
    Some(fields)
}

/// What the input says, before it is checked and put together.
#[derive(Default)]
struct Fields {
    /// Year, month and day.
    date: Option<(i32, u32, u32)>,
    /// Microseconds since midnight. Up to `24:00:00` and second 60, which
    /// PostgreSQL also takes; they carry into the next minute or day.
    time: Option<i64>,
    /// Seconds east of UTC.
    offset: Option<i64>,
}

impl Fields {
    fn read(input: &str, ty: TemporalType) -> Result<Self> {
        match input.trim().to_ascii_lowercase().as_str() {
            "allballs" if ty == TemporalType::Time => {
                return Ok(Self {
                    time: Some(0),
                    ..Self::default()
                })
            }
            word @ ("infinity" | "-infinity" | "epoch" | "now" | "today" | "tomorrow"
            | "yesterday" | "allballs") => {
                return Err(error(format!(
                    "the {} value \"{word}\" is not supported",
                    ty.name()
                )))
            }
            _ => {}
        }
        let fields = split_fields(input).ok_or_else(|| syntax_error(input, ty))?;
        let mut out = Self::default();
        // The month, day and year of a date written with a month name, such
        // as `Jan 5 2024` or `5 January 2024`.
        let mut month_name = None;
        let mut numbers = Vec::new();
        let mut pm = None;
        let mut time = None;
        for field in fields {
            match field {
                Field::Date(text, sep) => {
                    let parts: Vec<&str> = text.split(sep).collect();
                    // A first number of 1 or 2 digits is a month or a day, in
                    // an order that depends on PostgreSQL's DateStyle, which
                    // is not supported.
                    let [year_text, month, day] = parts[..] else {
                        return Err(syntax_error(input, ty));
                    };
                    if year_text.len() <= 2 {
                        return Err(syntax_error(input, ty));
                    }
                    let date = (
                        year(year_text, input, ty)?,
                        two_digits(month, input, ty)?,
                        two_digits(day, input, ty)?,
                    );
                    set_once(&mut out.date, date, input, ty)?;
                }
                Field::Number(digits) if digits.len() == 8 => {
                    let date = (
                        year(&digits[..4], input, ty)?,
                        two_digits(&digits[4..6], input, ty)?,
                        two_digits(&digits[6..], input, ty)?,
                    );
                    set_once(&mut out.date, date, input, ty)?;
                }
                Field::Number(digits) => numbers.push(digits),
                Field::Time(text) => set_once(&mut time, read_time(text, input, ty)?, input, ty)?,
                Field::Word(word) => match word.to_ascii_lowercase().as_str() {
                    "am" => set_once(&mut pm, false, input, ty)?,
                    "pm" => set_once(&mut pm, true, input, ty)?,
                    // ISO 8601's separator of the date and the time.
                    "t" => {}
                    "z" => set_once(&mut out.offset, 0, input, ty)?,
                    "bc" => {
                        return Err(error(format!(
                            "BC dates are not supported for type {}: \"{input}\"",
                            ty.name()
                        )))
                    }
                    other => {
                        let month = other
                            .parse::<Month>()
                            .map_err(|_| syntax_error(input, ty))?;
                        set_once(&mut month_name, month.number_from_month(), input, ty)?;
                    }
                },
                Field::Offset(text) => {
                    set_once(&mut out.offset, read_offset(text, input, ty)?, input, ty)?
                }
            }
        }
        if let Some(month) = month_name {
            // One number is the day and one, with 4 digits, the year.
            let [first, second] = numbers[..] else {
                return Err(syntax_error(input, ty));
            };
            let (day, year_text) = if first.len() <= 2 {
                (first, second)
            } else {
                (second, first)
            };
            let date = (
                year(year_text, input, ty)?,
                month,
                two_digits(day, input, ty)?,
            );
            set_once(&mut out.date, date, input, ty)?;
        } else if !numbers.is_empty() {
            return Err(syntax_error(input, ty));
        }
        if let Some((mut hour, minute, second, micros)) = time {
            match pm {
                Some(_) if hour > 12 => return Err(range_error(input)),
                Some(true) if hour < 12 => hour += 12,
                Some(false) if hour == 12 => hour = 0,
                Some(_) | None => {}
            }
            if hour > 24
                || minute > 59
                || second > 60
                || (hour == 24 && (minute, second, micros) != (0, 0, 0))
            {
                return Err(range_error(input));
            }
            out.time = Some(((hour * 60 + minute) * 60 + second) * 1_000_000 + micros);
        } else if pm.is_some() {
            return Err(syntax_error(input, ty));
        }
        Ok(out)
    }

    /// The value in `ty`'s fixed text form.
    fn format(self, input: &str, ty: TemporalType) -> Result<String> {
        let date = match self.date {
            Some((year, month, day)) => {
                Some(NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| range_error(input))?)
            }
            None => None,
        };
        // As in PostgreSQL, a date without a time is at midnight, and a
        // timestamptz without an offset is in the session's time zone, UTC.
        let time = match (self.time, ty) {
            (Some(time), _) => time,
            (None, TemporalType::Time) => return Err(syntax_error(input, ty)),
            (None, _) => 0,
        };
        let (date, ty) = match (date, ty) {
            (_, TemporalType::Time) => {
                if time > MICROS_PER_DAY {
                    return Err(range_error(input));
                }
                let mut out = String::new();
                push_time(&mut out, time);
                return Ok(out);
            }
            (None, _) => return Err(syntax_error(input, ty)),
            (Some(date), TemporalType::Date) => return Ok(date.format("%Y-%m-%d").to_string()),
            (Some(date), ty) => (date, ty),
        };
        let mut timestamp = NaiveDateTime::from(date) + Duration::microseconds(time);
        if let (TemporalType::Timestamptz, Some(offset)) = (ty, self.offset) {
            timestamp -= Duration::seconds(offset);
        }
        check_year(timestamp.year(), input, ty)?;
        let time = i64::from(timestamp.num_seconds_from_midnight()) * 1_000_000
            + i64::from(timestamp.nanosecond() / 1000);
        let mut out = timestamp.format("%Y-%m-%d ").to_string();
        push_time(&mut out, time);
        Ok(out)
    }
}

/// `HH:MM:SS`, then the fraction of a second with its trailing zeros dropped.
fn push_time(out: &mut String, micros: i64) {
    let seconds = micros / 1_000_000;
    out.push_str(&format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    ));
    let fraction = micros % 1_000_000;
    if fraction != 0 {
        out.push_str(format!(".{fraction:06}").trim_end_matches('0'));
    }
}

fn set_once<T>(slot: &mut Option<T>, value: T, input: &str, ty: TemporalType) -> Result<()> {
    if slot.replace(value).is_some() {
        return Err(syntax_error(input, ty));
    }
    Ok(())
}

fn is_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|c| c.is_ascii_digit())
}

/// A field of 1 or 2 digits, such as a month, a day or an hour.
fn two_digits<T: std::str::FromStr>(text: &str, input: &str, ty: TemporalType) -> Result<T> {
    if !is_digits(text) || text.len() > 2 {
        return Err(syntax_error(input, ty));
    }
    text.parse().map_err(|_| syntax_error(input, ty))
}

/// A year must have 4 digits, so that its text sorts in time order.
fn year(text: &str, input: &str, ty: TemporalType) -> Result<i32> {
    if !is_digits(text) {
        return Err(syntax_error(input, ty));
    }
    if text.len() != 4 {
        return Err(error(format!(
            "years must be written with 4 digits, from 0001 to 9999: \"{input}\""
        )));
    }
    let year = text.parse().map_err(|_| syntax_error(input, ty))?;
    check_year(year, input, ty)?;
    Ok(year)
}

fn check_year(year: i32, input: &str, ty: TemporalType) -> Result<()> {
    match year {
        1..=9999 => Ok(()),
        _ => Err(LimboError::DatetimeFieldOverflow(format!(
            "{} values must be between years 0001 and 9999: \"{input}\"",
            ty.name()
        ))),
    }
}

/// `H:M[:S[.fraction]]` as hour, minute, second and microseconds. The fraction
/// is rounded as PostgreSQL rounds it: `rint(strtod(fraction) * 1e6)`.
fn read_time(text: &str, input: &str, ty: TemporalType) -> Result<(i64, i64, i64, i64)> {
    let (clock, fraction) = match text.split_once('.') {
        Some((clock, fraction)) if is_digits(fraction) => (clock, fraction),
        Some(_) => return Err(syntax_error(input, ty)),
        None => (text, ""),
    };
    let parts: Vec<&str> = clock.split(':').collect();
    let (hour, minute, second) = match parts[..] {
        [hour, minute] if fraction.is_empty() => (hour, minute, "0"),
        [hour, minute, second] => (hour, minute, second),
        _ => return Err(syntax_error(input, ty)),
    };
    let micros = if fraction.is_empty() {
        0
    } else {
        let fraction: f64 = format!("0.{fraction}")
            .parse()
            .map_err(|_| syntax_error(input, ty))?;
        (fraction * 1e6).round_ties_even() as i64
    };
    Ok((
        two_digits(hour, input, ty)?,
        two_digits(minute, input, ty)?,
        two_digits(second, input, ty)?,
        micros,
    ))
}

/// `+HH`, `+HH:MM` or `+HHMM` as seconds east of UTC. As in PostgreSQL, an
/// offset is at most 15:59.
fn read_offset(text: &str, input: &str, ty: TemporalType) -> Result<i64> {
    let (sign, digits) = text.split_at(1);
    let (hours, minutes) = match digits.split_once(':') {
        Some((hours, minutes)) => (hours, minutes),
        None if digits.len() == 4 => digits.split_at(2),
        None => (digits, "0"),
    };
    let hours: i64 = two_digits(hours, input, ty)?;
    let minutes: i64 = two_digits(minutes, input, ty)?;
    if hours > 15 || minutes > 59 {
        return Err(LimboError::DatetimeFieldOverflow(format!(
            "time zone displacement out of range: \"{input}\""
        )));
    }
    let offset = (hours * 60 + minutes) * 60;
    Ok(if sign == "-" { -offset } else { offset })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(input: &str, ty: &str) -> std::result::Result<String, String> {
        let value = Value::build_text(input.to_string());
        match exec_temporal_encode(&value, &Value::build_text(ty.to_string())) {
            Ok(Value::Text(text)) => Ok(text.as_str().to_string()),
            Ok(other) => Err(format!("not text: {other:?}")),
            Err(err) => Err(err.to_string()),
        }
    }

    /// Forms the .sqltest files do not cover.
    #[test]
    fn reads_postgresql_input_forms() {
        let cases = [
            (
                "2024-01-05 10:00:00.1234565",
                "timestamp",
                "2024-01-05 10:00:00.123456",
            ),
            ("  2024-01-05  ", "date", "2024-01-05"),
            ("2024-01-05 10:00:00", "date", "2024-01-05"),
            ("2024-01-05 10:00", "time", "10:00:00"),
            ("9:5", "time", "09:05:00"),
            ("12:30 PM", "time", "12:30:00"),
            ("2024-01-05 10:00", "timestamptz", "2024-01-05 10:00:00"),
        ];
        for (input, ty, expected) in cases {
            assert_eq!(
                encode(input, ty).as_deref(),
                Ok(expected),
                "{input} as {ty}"
            );
        }
    }

    /// Input the .sqltest files do not cover.
    #[test]
    fn rejects_input_postgresql_rejects_or_that_would_not_sort() {
        let cases = [
            ("0000-01-01", "date"),
            ("-infinity", "timestamptz"),
            ("now", "timestamp"),
            ("allballs", "timestamp"),
            ("", "timestamp"),
            ("2024-01-05", "time"),
            ("10:00", "date"),
            ("2024-01-05 10", "timestamp"),
            ("24:00:00.1", "time"),
            ("23:59:60.5", "time"),
            ("13:00 PM", "time"),
            ("2024-01-05 10:00:00+16", "timestamptz"),
            ("01/05/2024", "date"),
            ("05-Jan-2024", "date"),
            ("2024-01-05 10:00 UTC", "timestamptz"),
            ("0001-01-01 00:00:00+01", "timestamptz"),
            ("2024-01-05 2024-01-06", "date"),
            ("2024-01-05", "interval"),
        ];
        for (input, ty) in cases {
            assert!(
                encode(input, ty).is_err(),
                "{input} as {ty}: {:?}",
                encode(input, ty)
            );
        }
    }
}
