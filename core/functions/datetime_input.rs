//! Reads date and time input as PostgreSQL's `date_in`, `time_in`,
//! `timestamp_in` and `timestamptz_in` do, for the ENCODE of the built-in
//! types of the same names. A date or time is written in one fixed text form,
//! with a 4-digit year, so the stored texts sort in time order: `2024-01-05`,
//! `10:00:00.5`. A timestamp and a timestamptz are stored as PostgreSQL stores
//! them: the microseconds since 2000-01-01 00:00:00 as an integer, in UTC for a
//! timestamptz. PostgreSQL keeps the largest and smallest integer for
//! `infinity` and `-infinity`.
//!
//! The input is split into fields as PostgreSQL's `ParseDateTime` does and the
//! fields are read as its `DecodeDateTime` does, for a subset of its forms.
//! BC, years past 9999, years not written with 4 digits and `infinity` are
//! rejected: the text form of a date would not sort in time order, and the
//! timestamp types read the same input as date.
//!
//! Input that PostgreSQL may read but this parser does not, such as those
//! years, `infinity` or a time zone name, is an error with SQLSTATE 0A000.
//! Only input PostgreSQL rejects too gets PostgreSQL's 22007 or 22008.

use crate::numeric::Numeric;
use crate::types::Value;
use crate::{LimboError, Result};
use chrono::{DateTime, Datelike, Duration, Month, NaiveDate, NaiveDateTime, Timelike};

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

/// Microseconds from 1970-01-01, where Unix time starts, to 2000-01-01, where
/// PostgreSQL's time starts.
pub const POSTGRES_EPOCH_IN_UNIX_MICROS: i64 = 946_684_800_000_000;

/// `temporal_encode(value, type)`, the ENCODE of the built-in type `type`,
/// date or time.
pub fn exec_temporal_encode(value: &Value, type_name: &Value) -> Result<Value> {
    let ty = match type_name.to_text() {
        Some("date") => TemporalType::Date,
        Some("time") => TemporalType::Time,
        _ => {
            return Err(LimboError::InvalidArgument(format!(
                "temporal_encode: unknown type {type_name}"
            )))
        }
    };
    encode(value, ty)
}

/// `timestamp_encode(value)`, the ENCODE of timestamp. As in PostgreSQL, a
/// UTC offset in the input is ignored.
pub fn exec_timestamp_encode(value: &Value) -> Result<Value> {
    encode(value, TemporalType::Timestamp)
}

/// `timestamptz_encode(value)`, the ENCODE of timestamptz.
pub fn exec_timestamptz_encode(value: &Value) -> Result<Value> {
    encode(value, TemporalType::Timestamptz)
}

/// NULL stays NULL, text is read as `ty`, anything else is an error, as
/// PostgreSQL has no conversion from numbers or bytes to dates.
fn encode(value: &Value, ty: TemporalType) -> Result<Value> {
    let input = match value {
        Value::Null => return Ok(Value::Null),
        Value::Text(text) => text.as_str(),
        // PostgreSQL reads only text as a date or time, so `20240105` is not
        // 2024-01-05.
        Value::Numeric(_) => {
            return Err(not_supported(format!(
                "converting a number to {} is not supported",
                ty.name()
            )))
        }
        Value::Blob(_) => {
            return Err(not_supported(format!(
                "converting a blob to {} is not supported",
                ty.name()
            )))
        }
    };
    Fields::read(input, ty)?.encode(input, ty)
}

/// The stored microseconds of timestamp or timestamptz text, read as the ENCODE
/// reads it. The ordering of the two types uses it for the decoded text of a
/// value, so the ordering has no text parser of its own.
pub fn read_timestamp_text(input: &str) -> Result<i64> {
    let ty = TemporalType::Timestamptz;
    match Fields::read(input, ty)?.encode(input, ty)? {
        Value::Numeric(Numeric::Integer(micros)) => Ok(micros),
        other => unreachable!("the encode of a timestamp is an integer, got {other:?}"),
    }
}

fn not_supported(message: String) -> LimboError {
    LimboError::DatetimeNotSupported(message)
}

/// `timestamp_decode(value)`, the DECODE of timestamp: the stored
/// microseconds as PostgreSQL prints a timestamp, such as
/// `2024-01-05 10:00:00.5`.
pub fn exec_timestamp_decode(value: &Value) -> Result<Value> {
    Ok(match decode(value, TemporalType::Timestamp)? {
        Some(timestamp) => Value::build_text(format_timestamp(timestamp)),
        None => Value::Null,
    })
}

/// `timestamptz_decode(value)`, the DECODE of timestamptz: the stored
/// microseconds as PostgreSQL prints a timestamptz in the time zone UTC, such
/// as `2024-01-05 10:00:00.5+00`.
pub fn exec_timestamptz_decode(value: &Value) -> Result<Value> {
    Ok(match decode(value, TemporalType::Timestamptz)? {
        Some(timestamp) => Value::build_text(format_timestamptz(timestamp)),
        None => Value::Null,
    })
}

/// The timestamp of stored microseconds, or `None` for NULL. A value the
/// ENCODE does not write, such as `infinity`, is an error.
fn decode(value: &Value, ty: TemporalType) -> Result<Option<NaiveDateTime>> {
    let micros = match value {
        Value::Null => return Ok(None),
        Value::Numeric(Numeric::Integer(micros)) => *micros,
        Value::Numeric(Numeric::Float(_)) | Value::Text(_) | Value::Blob(_) => {
            return Err(LimboError::Corrupt(format!(
                "a {} must be stored as an integer, got {}",
                ty.name(),
                value.value_type()
            )))
        }
    };
    timestamp_of_micros(micros)
        .map(Some)
        .ok_or_else(|| LimboError::DatetimeFieldOverflow("timestamp out of range".to_string()))
}

/// The timestamp `micros` microseconds after 2000-01-01 00:00:00, the form
/// PostgreSQL stores, or None outside the years 0001 to 9999, which Turso
/// neither reads nor prints.
pub fn timestamp_of_micros(micros: i64) -> Option<NaiveDateTime> {
    micros
        .checked_add(POSTGRES_EPOCH_IN_UNIX_MICROS)
        .and_then(DateTime::from_timestamp_micros)
        .map(|timestamp| timestamp.naive_utc())
        .filter(|timestamp| (1..=9999).contains(&timestamp.year()))
}

/// How PostgreSQL prints a timestamptz in the time zone UTC, such as
/// `2024-01-05 10:00:00.5+00`. Digits below a microsecond are dropped.
pub fn format_timestamptz(timestamp: NaiveDateTime) -> String {
    format!("{}+00", format_timestamp(timestamp))
}

/// Input that PostgreSQL rejects too. PostgreSQL's frontend reports it as
/// `invalid_datetime_format`. Values out of range are `DatetimeFieldOverflow`
/// instead.
fn syntax_error(input: &str, ty: TemporalType) -> LimboError {
    LimboError::InvalidDate(format!(
        "invalid input syntax for type {}: \"{input}\"",
        ty.name()
    ))
}

/// Input that PostgreSQL may read but this parser does not, such as a time
/// zone name or a date in the order day, month, year. It is an error with
/// SQLSTATE 0A000, not 22007, because PostgreSQL may return a value for it.
fn unsupported_form(input: &str, ty: TemporalType) -> LimboError {
    not_supported(format!(
        "invalid or unsupported input for type {}: \"{input}\"",
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
            let word = &input[start..i];
            // As in PostgreSQL, letters followed by `-`, `/` or `.` are a
            // date with a month name, and letters followed by a digit or `+`
            // are one too unless they are a keyword, such as `Jan5` or
            // `T10:00`. Those dates are not supported.
            let glued_to_date = match bytes.get(i) {
                Some(b'-' | b'/' | b'.') => true,
                Some(c) if c.is_ascii_digit() || *c == b'+' => !is_keyword(word),
                Some(_) | None => false,
            };
            if glued_to_date {
                return None;
            }
            fields.push(Field::Word(word));
        } else if c == b'+' || c == b'-' {
            i = skip(i + 1, &|c| c.is_ascii_digit() || c == b':');
            fields.push(Field::Offset(&input[start..i]));
        } else {
            return None;
        }
    }
    Some(fields)
}

/// The words of PostgreSQL's keyword table that this parser reads when a
/// digit or `+` follows them. `z` is not one: PostgreSQL reads `z10` as a
/// POSIX time zone 10 hours behind UTC, not as UTC followed by `10`.
fn is_keyword(word: &str) -> bool {
    let word = word.to_ascii_lowercase();
    matches!(word.as_str(), "am" | "pm" | "t" | "bc") || word.parse::<Month>().is_ok()
}

/// What the input says, before it is checked and put together.
#[derive(Default)]
struct Fields {
    /// Year, month and day.
    date: Option<(i32, u32, u32)>,
    /// Microseconds since midnight, up to `24:00:00`. Second 60 carries into
    /// the next minute.
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
                return Err(not_supported(format!(
                    "the {} value \"{word}\" is not supported",
                    ty.name()
                )))
            }
            _ => {}
        }
        let fields = split_fields(input).ok_or_else(|| unsupported_form(input, ty))?;
        let mut reader = Reader {
            input,
            ty,
            seen: 0,
            iso_time_pending: false,
            have_text_month: false,
            year: None,
            month: None,
            day: None,
            time: None,
            pm: None,
            offset: None,
        };
        for (i, field) in fields.iter().enumerate() {
            // `time` takes a date only as the first field, right before the time.
            let date_may_come = ty != TemporalType::Time
                || (i == 0 && matches!(fields.get(1), Some(Field::Time(_))));
            reader.read_field(field, date_may_come)?;
        }
        reader.finish()
    }

    /// The value in `ty`'s stored form.
    fn encode(self, input: &str, ty: TemporalType) -> Result<Value> {
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
        let date = match (date, ty) {
            (_, TemporalType::Time) => {
                let mut out = String::new();
                push_time(&mut out, time);
                return Ok(Value::build_text(out));
            }
            (None, _) => return Err(syntax_error(input, ty)),
            (Some(date), TemporalType::Date) => {
                return Ok(Value::build_text(date.format("%Y-%m-%d").to_string()))
            }
            (Some(date), TemporalType::Timestamp | TemporalType::Timestamptz) => date,
        };
        let mut timestamp = NaiveDateTime::from(date) + Duration::microseconds(time);
        if let (TemporalType::Timestamptz, Some(offset)) = (ty, self.offset) {
            timestamp -= Duration::seconds(offset);
        }
        check_year(timestamp.year(), input, ty)?;
        Ok(Value::from_i64(
            timestamp.and_utc().timestamp_micros() - POSTGRES_EPOCH_IN_UNIX_MICROS,
        ))
    }
}

/// How PostgreSQL prints a timestamp, such as `2024-01-05 10:00:00.5`.
/// Digits below a microsecond are dropped.
pub fn format_timestamp(timestamp: NaiveDateTime) -> String {
    let time = i64::from(timestamp.num_seconds_from_midnight()) * 1_000_000
        + i64::from(timestamp.nanosecond() / 1000);
    let mut out = timestamp.format("%Y-%m-%d ").to_string();
    push_time(&mut out, time);
    out
}

/// What a field of the input says about the date and time. A field can be
/// read once, as PostgreSQL's `fmask` enforces.
const YEAR: u8 = 1;
const MONTH: u8 = 2;
const DAY: u8 = 4;
const TIME: u8 = 8;
const TIME_ZONE: u8 = 16;
const AM_PM: u8 = 32;
const DATE: u8 = YEAR | MONTH | DAY;

/// Reads the fields one at a time as PostgreSQL's `DecodeDateTime` does
/// (`DecodeTimeOnly` for `time`). What a field means depends on the fields
/// before it: in `Jan 5 2024` the 5 is the day and 2024 the year, in
/// `5 2024 Jan` PostgreSQL rejects the same numbers.
struct Reader<'a> {
    input: &'a str,
    ty: TemporalType,
    /// The kinds of field read so far.
    seen: u8,
    /// A `T` was read and the time it announces has not come yet.
    iso_time_pending: bool,
    have_text_month: bool,
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
    time: Option<(i64, i64, i64, i64)>,
    pm: Option<bool>,
    offset: Option<i64>,
}

impl Reader<'_> {
    fn read_field(&mut self, field: &Field<'_>, date_may_come: bool) -> Result<()> {
        let (input, ty) = (self.input, self.ty);
        let mask = match field {
            Field::Date(text, sep) => self.read_date(text, *sep, date_may_come)?,
            Field::Number(digits) => self.read_number(digits)?,
            Field::Time(text) => {
                self.iso_time_pending = false;
                self.time = Some(read_time(text, input, ty)?);
                TIME
            }
            Field::Word(word) => self.read_word(word)?,
            Field::Offset(text) => {
                self.offset = Some(read_offset(text, input, ty)?);
                TIME_ZONE
            }
        };
        if mask & self.seen != 0 {
            return Err(syntax_error(input, ty));
        }
        self.seen |= mask;
        Ok(())
    }

    /// `2024-01-05`
    fn read_date(&mut self, text: &str, sep: char, date_may_come: bool) -> Result<u8> {
        let (input, ty) = (self.input, self.ty);
        // As in PostgreSQL, a date is not read after anything but a time zone.
        if !date_may_come || self.seen & !TIME_ZONE != 0 {
            return Err(syntax_error(input, ty));
        }
        let parts: Vec<&str> = text.split(sep).collect();
        // A first number of 1 or 2 digits is a month or a day, in an order
        // that depends on PostgreSQL's DateStyle, which is not supported.
        let [year_text, month, day] = parts[..] else {
            return Err(unsupported_form(input, ty));
        };
        if year_text.len() <= 2 {
            return Err(unsupported_form(input, ty));
        }
        self.year = Some(year(year_text, input, ty)?);
        self.month = Some(two_digits(month, input, ty)?);
        self.day = Some(two_digits(day, input, ty)?);
        Ok(DATE)
    }

    /// Digits only: `20240105`, or the `5` and `2024` of `Jan 5 2024`.
    fn read_number(&mut self, digits: &str) -> Result<u8> {
        let (input, ty) = (self.input, self.ty);
        // `time` takes no number. After a `T`, PostgreSQL reads a number as a
        // time written as `hhmm`, which is not supported.
        if ty == TemporalType::Time || self.iso_time_pending {
            return Err(unsupported_form(input, ty));
        }
        // 6 digits or more, with no date yet, are a date: `20240105`.
        if digits.len() >= 6 && self.seen & DATE == 0 {
            if digits.len() != 8 {
                return Err(unsupported_form(input, ty));
            }
            self.year = Some(year(&digits[..4], input, ty)?);
            self.month = Some(two_digits(&digits[4..6], input, ty)?);
            self.day = Some(two_digits(&digits[6..], input, ty)?);
            return Ok(DATE);
        }
        // The order of the date fields is PostgreSQL's DateStyle `MDY`, with a
        // number of 3 digits or more as the year.
        let is_long = digits.len() >= 3;
        let has = |bit: u8| self.seen & bit != 0;
        match (has(YEAR), has(MONTH), has(DAY)) {
            (false, false, false) if is_long => {
                self.year = Some(year(digits, input, ty)?);
                Ok(YEAR)
            }
            (false, false, false) => {
                self.month = Some(two_digits(digits, input, ty)?);
                Ok(MONTH)
            }
            // PostgreSQL reads 3 digits after the year as a day of the year.
            (true, false, false) if is_long => Err(unsupported_form(input, ty)),
            (true, false, false) => {
                self.month = Some(two_digits(digits, input, ty)?);
                Ok(MONTH)
            }
            // After a month name, a long number is the year: `Jan 2024 5`.
            (false, true, false) if self.have_text_month && is_long => {
                self.year = Some(year(digits, input, ty)?);
                Ok(YEAR)
            }
            (false, true, false) | (true, true, false) => {
                self.day = Some(two_digits(digits, input, ty)?);
                Ok(DAY)
            }
            (false, true, true) => {
                self.year = Some(year(digits, input, ty)?);
                Ok(YEAR)
            }
            // A day first needs DateStyle `DMY`. A number after a whole date
            // would be a time written as `hhmm`, which is not supported.
            (false, false, true) | (true, false, true) | (true, true, true) => {
                Err(unsupported_form(input, ty))
            }
        }
    }

    /// Letters only: `AM`, `PM`, `T`, `Z`, a month name.
    fn read_word(&mut self, word: &str) -> Result<u8> {
        let (input, ty) = (self.input, self.ty);
        match word.to_ascii_lowercase().as_str() {
            "am" => {
                self.pm = Some(false);
                Ok(AM_PM)
            }
            "pm" => {
                self.pm = Some(true);
                Ok(AM_PM)
            }
            // ISO 8601's separator of the date and the time: the next field
            // must be the time. PostgreSQL wants a whole date before it, but
            // not for `time`.
            "t" => {
                let date_missing = ty != TemporalType::Time && self.seen & DATE != DATE;
                if date_missing || self.iso_time_pending {
                    return Err(syntax_error(input, ty));
                }
                self.iso_time_pending = true;
                Ok(0)
            }
            "z" => {
                self.offset = Some(0);
                Ok(TIME_ZONE)
            }
            "bc" => Err(not_supported(format!(
                "BC dates are not supported for type {}: \"{input}\"",
                ty.name()
            ))),
            // Any other word may be a time zone name or abbreviation, a day
            // name or another word PostgreSQL reads, such as `EST`, `Fri` or
            // `ago`. Telling those from a word PostgreSQL rejects, such as
            // `hello`, needs PostgreSQL's tables of time zone names.
            _ if ty == TemporalType::Time => Err(unsupported_form(input, ty)),
            other => {
                let month = other
                    .parse::<Month>()
                    .map_err(|_| unsupported_form(input, ty))?
                    .number_from_month();
                let mut mask = MONTH;
                // A number before the name is the day, not the month:
                // `5 Jan 2024`.
                let number_was_month = self.seen & MONTH != 0
                    && !self.have_text_month
                    && self.seen & DAY == 0
                    && matches!(self.month, Some(1..=31));
                if number_was_month {
                    self.day = self.month;
                    mask = DAY;
                }
                self.have_text_month = true;
                self.month = Some(month);
                Ok(mask)
            }
        }
    }

    /// Puts the fields together and checks what is left over.
    fn finish(self) -> Result<Fields> {
        let (input, ty) = (self.input, self.ty);
        if self.iso_time_pending {
            return Err(syntax_error(input, ty));
        }
        let date = match (self.year, self.month, self.day) {
            (Some(year), Some(month), Some(day)) => Some((year, month, day)),
            (None, None, None) => None,
            (_, _, _) => return Err(syntax_error(input, ty)),
        };
        let time = match self.time {
            Some(time) => Some(time),
            // As in PostgreSQL, AM or PM with no time is hour 0:
            // `2024-01-05 PM` is noon.
            None if self.pm.is_some() && ty != TemporalType::Time => Some((0, 0, 0, 0)),
            None => None,
        };
        let mut out = Fields {
            date,
            time: None,
            offset: self.offset,
        };
        if let Some((mut hour, minute, second, micros)) = time {
            match self.pm {
                Some(_) if hour > 12 => return Err(range_error(input)),
                Some(true) if hour < 12 => hour += 12,
                Some(false) if hour == 12 => hour = 0,
                Some(_) | None => {}
            }
            let micros_of_day = ((hour * 60 + minute) * 60 + second) * 1_000_000 + micros;
            if time_overflows(hour, minute, second, micros_of_day) {
                return Err(range_error(input));
            }
            out.time = Some(micros_of_day);
        }
        Ok(out)
    }
}

/// PostgreSQL's `time_overflows`: each field is in range and the whole time,
/// second 60 and fraction included, is at most `24:00:00`.
fn time_overflows(hour: i64, minute: i64, second: i64, micros_of_day: i64) -> bool {
    hour > 24 || minute > 59 || second > 60 || micros_of_day > MICROS_PER_DAY
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

fn is_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|c| c.is_ascii_digit())
}

/// A field of 1 or 2 digits, such as a month, a day or an hour. PostgreSQL
/// reads more digits, such as the `005` of `2024-01-005`.
fn two_digits<T: std::str::FromStr>(text: &str, input: &str, ty: TemporalType) -> Result<T> {
    if !is_digits(text) {
        return Err(syntax_error(input, ty));
    }
    if text.len() > 2 {
        return Err(unsupported_form(input, ty));
    }
    text.parse().map_err(|_| syntax_error(input, ty))
}

/// A year must have 4 digits, so that its text sorts in time order.
/// PostgreSQL reads other years, such as `24` or `10000`, and rejects year 0.
fn year(text: &str, input: &str, ty: TemporalType) -> Result<i32> {
    if !is_digits(text) {
        return Err(syntax_error(input, ty));
    }
    if text.len() != 4 {
        return Err(not_supported(format!(
            "years must be written with 4 digits, from 0001 to 9999: \"{input}\""
        )));
    }
    let year = text.parse().map_err(|_| syntax_error(input, ty))?;
    if year == 0 {
        return Err(range_error(input));
    }
    Ok(year)
}

/// PostgreSQL reads years before 0001, as BC, and after 9999.
fn check_year(year: i32, input: &str, ty: TemporalType) -> Result<()> {
    match year {
        1..=9999 => Ok(()),
        _ => Err(not_supported(format!(
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
        Some(_) => return Err(unsupported_form(input, ty)),
        None => (text, ""),
    };
    let parts: Vec<&str> = clock.split(':').collect();
    let (hour, minute, second) = match parts[..] {
        [hour, minute] if fraction.is_empty() => (hour, minute, "0"),
        // PostgreSQL reads `10:00.5` as minutes and seconds.
        [_, _] => return Err(unsupported_form(input, ty)),
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

    /// The value as it is stored, or as the DECODE prints it for a timestamp
    /// or a timestamptz.
    fn encode(input: &str, ty: &str) -> std::result::Result<String, String> {
        let value = Value::build_text(input.to_string());
        let encoded = match ty {
            "timestamp" => {
                exec_timestamp_encode(&value).and_then(|stored| exec_timestamp_decode(&stored))
            }
            "timestamptz" => {
                exec_timestamptz_encode(&value).and_then(|stored| exec_timestamptz_decode(&stored))
            }
            _ => exec_temporal_encode(&value, &Value::build_text(ty.to_string())),
        };
        match encoded {
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
            ("2024-01-05 10:00", "timestamptz", "2024-01-05 10:00:00+00"),
            ("+02 2024-01-05 10:00", "timestamp", "2024-01-05 10:00:00"),
            ("Z 2024-01-05", "timestamp", "2024-01-05 00:00:00"),
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
            ("2024-01-05 Z10:00", "timestamp"),
            ("2024-01-05 Jan-05", "timestamp"),
            ("+02 PM 2024-01-05", "timestamp"),
        ];
        for (input, ty) in cases {
            assert!(
                encode(input, ty).is_err(),
                "{input} as {ty}: {:?}",
                encode(input, ty)
            );
        }
    }

    /// The stored values are the ones PostgreSQL 18's `timestamp_send` and
    /// `timestamptz_send` write for the same timestamps.
    #[test]
    fn timestamps_are_stored_as_postgresql_stores_them() {
        let cases = [
            ("2000-01-01 00:00:00", 0),
            ("0001-01-01 00:00:00", 0xff1f_e2ff_c59c_6000_u64 as i64),
            ("9999-12-31 23:59:59.999999", 0x0380_e70b_913b_7fff),
        ];
        for (text, stored) in cases {
            let timestamp = Value::build_text(text.to_string());
            let timestamptz = Value::build_text(format!("{text}+00"));
            let stored = Value::from_i64(stored);
            assert_eq!(exec_timestamp_encode(&timestamp).unwrap(), stored);
            assert_eq!(exec_timestamp_decode(&stored).unwrap(), timestamp);
            assert_eq!(exec_timestamptz_encode(&timestamptz).unwrap(), stored);
            assert_eq!(exec_timestamptz_decode(&stored).unwrap(), timestamptz);
        }
        for stored in [i64::MIN, i64::MAX, 0x0380_e70b_913b_8000] {
            let stored = Value::from_i64(stored);
            assert!(exec_timestamp_decode(&stored).is_err());
            assert!(exec_timestamptz_decode(&stored).is_err());
        }
    }

    /// An index orders the stored integers, while an expression compares the
    /// DECODE's text, so both must give the same order for every stored value.
    ///
    /// A stored value is a day, a second of that day and a microsecond of that
    /// second. The DECODE's text is the day in 10 characters, a space, the
    /// second in 8 characters, then a suffix that depends only on the
    /// microsecond. Because the day and the second have a fixed width, the
    /// text compares the day first, then the second, then the suffix, as the
    /// integer does. So the text sorts as the integer if each part sorts as
    /// its number. The test checks every day from 0001-01-01 to 9999-12-31,
    /// every second of a day and every microsecond of a second.
    #[test]
    fn decoded_text_sorts_as_the_stored_integer() {
        const SECOND: i64 = 1_000_000;
        const DAY: i64 = 86_400 * SECOND;
        let first_day = 0xff1f_e2ff_c59c_6000_u64 as i64; // 0001-01-01 00:00:00
        let last_day = 0x0380_e70b_913b_7fff - (DAY - 1); // 9999-12-31 00:00:00
        for (decode, offset) in [
            (exec_timestamp_decode as fn(&Value) -> Result<Value>, ""),
            (exec_timestamptz_decode, "+00"),
        ] {
            let text = |stored: i64| {
                decode(&Value::from_i64(stored))
                    .unwrap()
                    .to_text()
                    .unwrap()
                    .to_string()
            };
            assert!(decode(&Value::from_i64(first_day - 1)).is_err());
            assert!(decode(&Value::from_i64(last_day + DAY)).is_err());

            let mut previous = String::new();
            for stored in (first_day..=last_day).step_by(DAY as usize) {
                let text = text(stored);
                assert_eq!(&text[10..], format!(" 00:00:00{offset}"), "{text}");
                assert!(previous.as_str() < &text[..10], "{previous} then {text}");
                previous = text[..10].to_string();
            }

            let mut previous = String::new();
            for stored in (0..DAY).step_by(SECOND as usize) {
                let text = text(stored);
                assert_eq!(&text[..11], "2000-01-01 ", "{text}");
                assert_eq!(&text[19..], offset, "{text}");
                assert!(previous.as_str() < &text[11..19], "{previous} then {text}");
                previous = text[11..19].to_string();
            }

            let mut previous = String::new();
            for stored in 0..SECOND {
                let text = text(stored);
                assert_eq!(&text[..19], "2000-01-01 00:00:00", "{text}");
                assert!(
                    stored == 0 || previous.as_str() < &text[19..],
                    "{previous} then {text}"
                );
                previous = text[19..].to_string();
            }
        }
    }
}
