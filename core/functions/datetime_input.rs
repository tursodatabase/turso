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

use crate::numeric::Numeric;
use crate::types::Value;
use crate::{LimboError, Result};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Timelike};

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
        Value::Numeric(_) | Value::Blob(_) => {
            return Err(error(format!(
                "only text can be converted to {}",
                ty.name()
            )))
        }
    };
    Fields::read(input, ty)?.encode(input, ty)
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

/// Input that PostgreSQL reads and this parser does not. PostgreSQL's
/// frontend reports these as `feature_not_supported`, not as invalid input.
fn not_supported(message: String) -> LimboError {
    LimboError::DatetimeNotSupported(message)
}

fn form_not_supported(input: &str, ty: TemporalType) -> LimboError {
    not_supported(format!(
        "this form of {} input is not supported: \"{input}\"",
        ty.name()
    ))
}

fn time_zone_not_supported(input: &str, ty: TemporalType) -> LimboError {
    not_supported(format!(
        "time zone names are not supported for type {}: \"{input}\"",
        ty.name()
    ))
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
    /// A field that starts with letters and goes on past them, as
    /// `Jan-05-2024` or `Europe/Helsinki`.
    GluedWord(&'a str),
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
            // date with a month name or a time zone, and letters followed by
            // a digit or `+` are one too unless they are a keyword, such as
            // `Jan5` or `T10:00`.
            let glued = match bytes.get(i) {
                Some(b'-' | b'/' | b'.') => true,
                Some(c) if c.is_ascii_digit() || *c == b'+' => keyword(word).is_none(),
                Some(_) | None => false,
            };
            if glued {
                // The field ends where PostgreSQL's `ParseDateTime` ends it.
                i = skip(i, &|c| c.is_ascii_alphanumeric() || b"+-/_.:".contains(&c));
                fields.push(Field::GluedWord(&input[start..i]));
                continue;
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

/// A word of PostgreSQL's keyword table, `datetktbl`.
enum Keyword {
    Am,
    Pm,
    /// `T`, ISO 8601's separator of the date and the time.
    IsoTime,
    Ad,
    Bc,
    DayOfWeek,
    /// `at` and `on`, which PostgreSQL skips.
    Skipped,
    Month(u32),
    /// The keywords this parser does not read: special values such as
    /// `today`, units such as `y` and `j`, and `dst`.
    NotRead,
}

/// `z` is not a keyword: PostgreSQL reads `z10` as a POSIX time zone 10
/// hours behind UTC, not as UTC followed by `10`.
fn keyword(word: &str) -> Option<Keyword> {
    let keyword = match word.to_ascii_lowercase().as_str() {
        "am" => Keyword::Am,
        "pm" => Keyword::Pm,
        "t" => Keyword::IsoTime,
        "ad" => Keyword::Ad,
        "bc" => Keyword::Bc,
        "sun" | "sunday" | "mon" | "monday" | "tue" | "tues" | "tuesday" | "wed" | "weds"
        | "wednesday" | "thu" | "thur" | "thurs" | "thursday" | "fri" | "friday" | "sat"
        | "saturday" => Keyword::DayOfWeek,
        "at" | "on" => Keyword::Skipped,
        "jan" | "january" => Keyword::Month(1),
        "feb" | "february" => Keyword::Month(2),
        "mar" | "march" => Keyword::Month(3),
        "apr" | "april" => Keyword::Month(4),
        "may" => Keyword::Month(5),
        "jun" | "june" => Keyword::Month(6),
        "jul" | "july" => Keyword::Month(7),
        "aug" | "august" => Keyword::Month(8),
        "sep" | "sept" | "september" => Keyword::Month(9),
        "oct" | "october" => Keyword::Month(10),
        "nov" | "november" => Keyword::Month(11),
        "dec" | "december" => Keyword::Month(12),
        "allballs" | "epoch" | "infinity" | "now" | "today" | "tomorrow" | "yesterday" | "d"
        | "h" | "j" | "jd" | "julian" | "m" | "mm" | "s" | "y" | "dow" | "doy" | "isodow"
        | "isoyear" | "dst" => Keyword::NotRead,
        _ => return None,
    };
    Some(keyword)
}

/// What the input says, checked as PostgreSQL checks it.
#[derive(Default)]
struct Fields {
    date: Option<NaiveDate>,
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
            "allballs" => return Err(syntax_error(input, ty)),
            word @ ("infinity" | "-infinity" | "epoch" | "now" | "today" | "tomorrow"
            | "yesterday") => {
                return Err(not_supported(format!(
                    "the {} value \"{word}\" is not supported",
                    ty.name()
                )))
            }
            _ => {}
        }
        let fields = split_fields(input).ok_or_else(|| syntax_error(input, ty))?;
        let mut reader = Reader {
            input,
            ty,
            seen: 0,
            iso_time_pending: false,
            have_text_month: false,
            bc: false,
            two_digit_year: false,
            year: None,
            month: None,
            day: None,
            time: None,
            pm: None,
            offset: None,
            not_supported: None,
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
        // As in PostgreSQL, a date without a time is at midnight, and a
        // timestamptz without an offset is in the session's time zone, UTC.
        let time = match (self.time, ty) {
            (Some(time), _) => time,
            (None, TemporalType::Time) => unreachable!("Reader::finish wants a time for `time`"),
            (None, _) => 0,
        };
        let date = match (self.date, ty) {
            (_, TemporalType::Time) => {
                let mut out = String::new();
                push_time(&mut out, time);
                return Ok(Value::build_text(out));
            }
            (None, _) => unreachable!("Reader::finish wants a date for {}", ty.name()),
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
const AD_BC: u8 = 64;
const DAY_OF_WEEK: u8 = 128;
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
    bc: bool,
    /// The year was written with 1 or 2 digits.
    two_digit_year: bool,
    /// The year as written, before `BC` or 2 digits change it.
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
    time: Option<(i64, i64, i64, i64)>,
    pm: Option<bool>,
    offset: Option<i64>,
    /// The error for the first field PostgreSQL reads and this parser does
    /// not.
    not_supported: Option<LimboError>,
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
            // As in PostgreSQL, it is a date until the month and the day are
            // known, and a time zone after: `2024 Feb-05` is February 5, and
            // `Jan 5 Feb-05 2024` is January 5 in the time zone `Feb-05`.
            Field::GluedWord(text) if date_may_come && self.seen & (MONTH | DAY) != MONTH | DAY => {
                self.read_glued_date(text)?
            }
            Field::GluedWord(_) => self.read_time_zone_name()?,
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
        let [first, second, third] = parts[..] else {
            return Err(syntax_error(input, ty));
        };
        // A first number of 1 or 2 digits is a month or a day, in an order
        // that depends on PostgreSQL's DateStyle, which is not supported.
        // The date is read in PostgreSQL's default order, month, day, year,
        // as in `01/05/2024`, so that a date out of range gets its range error.
        if first.len() <= 2 {
            self.unsupported_field(form_not_supported(input, ty), DATE)?;
            self.month = Some(two_digits(first, input, ty)?);
            self.day = Some(two_digits(second, input, ty)?);
            self.year = Some(self.read_year(third)?);
            return Ok(DATE);
        }
        self.year = Some(self.read_year(first)?);
        self.month = Some(two_digits(second, input, ty)?);
        self.day = Some(two_digits(third, input, ty)?);
        Ok(DATE)
    }

    /// Digits only: `20240105`, or the `5` and `2024` of `Jan 5 2024`.
    fn read_number(&mut self, digits: &str) -> Result<u8> {
        let (input, ty) = (self.input, self.ty);
        // `time` takes no number.
        if ty == TemporalType::Time {
            return Err(syntax_error(input, ty));
        }
        // A number that is a whole time, after a `T` or a whole date.
        // PostgreSQL reads 4 or 6 digits as `hhmm` or `hhmmss`, which is not
        // supported, and rejects other lengths.
        let is_whole_time = self.iso_time_pending || (self.seen & DATE == DATE);
        if is_whole_time {
            if self.seen & TIME != 0 || !matches!(digits.len(), 4 | 6) {
                return Err(syntax_error(input, ty));
            }
            self.iso_time_pending = false;
            return self.unsupported_field(form_not_supported(input, ty), TIME);
        }
        // 6 digits or more, with no date yet, are a date: `20240105`.
        // PostgreSQL also reads other lengths, such as `240105`.
        if digits.len() >= 6 && self.seen & DATE == 0 {
            if digits.len() != 8 {
                return self.unsupported_field(form_not_supported(input, ty), DATE);
            }
            self.year = Some(self.read_year(&digits[..4])?);
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
                self.year = Some(self.read_year(digits)?);
                Ok(YEAR)
            }
            (false, false, false) => {
                self.month = Some(two_digits(digits, input, ty)?);
                Ok(MONTH)
            }
            // PostgreSQL reads 3 digits after the year as a day of the year.
            (true, false, false) if digits.len() == 3 => {
                self.unsupported_field(form_not_supported(input, ty), MONTH | DAY)
            }
            (true, false, false) if is_long => Err(syntax_error(input, ty)),
            (true, false, false) => {
                self.month = Some(two_digits(digits, input, ty)?);
                Ok(MONTH)
            }
            // After a month name, a long number is the year: `Jan 2024 5`.
            (false, true, false) if self.have_text_month && is_long => {
                self.year = Some(self.read_year(digits)?);
                Ok(YEAR)
            }
            (false, true, false) | (true, true, false) => {
                self.day = Some(two_digits(digits, input, ty)?);
                Ok(DAY)
            }
            (false, true, true) => {
                self.year = Some(self.read_year(digits)?);
                Ok(YEAR)
            }
            // A day first needs DateStyle `DMY`.
            (false, false, true) | (true, false, true) => Err(syntax_error(input, ty)),
            (true, true, true) => unreachable!("a number after a whole date is read as a time"),
        }
    }

    /// Letters only: `AM`, `PM`, `T`, `Z`, a month name.
    fn read_word(&mut self, word: &str) -> Result<u8> {
        let (input, ty) = (self.input, self.ty);
        let Some(keyword) = keyword(word) else {
            return match word {
                "z" | "Z" => {
                    self.offset = Some(0);
                    Ok(TIME_ZONE)
                }
                // `time` does not take a time zone name.
                _ if ty == TemporalType::Time => Err(syntax_error(input, ty)),
                _ => self.read_time_zone_name(),
            };
        };
        match keyword {
            Keyword::Am => {
                self.pm = Some(false);
                Ok(AM_PM)
            }
            Keyword::Pm => {
                self.pm = Some(true);
                Ok(AM_PM)
            }
            // The next field must be the time. PostgreSQL wants a whole date
            // before it, but not for `time`.
            Keyword::IsoTime => {
                let date_missing = ty != TemporalType::Time && self.seen & DATE != DATE;
                if date_missing || self.iso_time_pending {
                    return Err(syntax_error(input, ty));
                }
                self.iso_time_pending = true;
                Ok(0)
            }
            Keyword::Bc => {
                self.bc = true;
                self.unsupported_field(
                    not_supported(format!(
                        "BC dates are not supported for type {}: \"{input}\"",
                        ty.name()
                    )),
                    AD_BC,
                )
            }
            Keyword::Ad => self.unsupported_field(form_not_supported(input, ty), AD_BC),
            Keyword::DayOfWeek => {
                self.unsupported_field(form_not_supported(input, ty), DAY_OF_WEEK)
            }
            Keyword::Skipped => self.unsupported_field(form_not_supported(input, ty), 0),
            // What these do to the fields around them is not known here, so
            // the input is refused at once.
            Keyword::NotRead => Err(form_not_supported(input, ty)),
            // `time` does not take a month name.
            Keyword::Month(_) if ty == TemporalType::Time => Err(syntax_error(input, ty)),
            Keyword::Month(month) => {
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

    /// A date that starts with a month name, as `Feb-05-2024`, or `Feb-05`
    /// after the year. It is not supported. Its parts are checked as
    /// PostgreSQL's `DecodeDate` checks them, so that input PostgreSQL
    /// rejects gets PostgreSQL's error.
    fn read_glued_date(&mut self, text: &str) -> Result<u8> {
        let (input, ty) = (self.input, self.ty);
        let parts = date_parts(text).ok_or_else(|| syntax_error(input, ty))?;
        let is_month = |part: &str| matches!(keyword(part), Some(Keyword::Month(_)));
        let mut seen = self.seen;
        // The words first, as in PostgreSQL. Only a month name is read.
        for part in parts.iter().filter(|part| !is_digits(part)) {
            match keyword(part) {
                Some(Keyword::Month(_)) if seen & MONTH == 0 => seen |= MONTH,
                // PostgreSQL skips these here and rejects them below, where
                // it reads them as numbers.
                Some(Keyword::Skipped) => {}
                Some(_) | None => return Err(syntax_error(input, ty)),
            }
        }
        // Then the numbers, in PostgreSQL's `DecodeNumber` order for a date
        // with a month name.
        for part in parts.into_iter().filter(|part| !is_month(part)) {
            if !is_digits(part) {
                return Err(syntax_error(input, ty));
            }
            if part.parse::<i32>().is_err() {
                return Err(range_error(input));
            }
            let has = |bit: u8| seen & bit != 0;
            let mask = match (has(YEAR), has(MONTH), has(DAY)) {
                (_, false, _) => unreachable!("a field that starts with other letters is refused"),
                (false, true, false) if part.len() >= 3 => YEAR,
                (false, true, false) | (true, true, false) => DAY,
                (false, true, true) => YEAR,
                // After a whole date PostgreSQL reads a number as a time,
                // and this field must be only a date.
                (true, true, true) => return Err(syntax_error(input, ty)),
            };
            seen |= mask;
        }
        // PostgreSQL wants a whole date, with nothing before it but parts of
        // the date and time zones.
        if seen & !TIME_ZONE != DATE {
            return Err(syntax_error(input, ty));
        }
        self.unsupported_field(form_not_supported(input, ty), DATE & !self.seen)
    }

    /// PostgreSQL reads a word that is not a keyword or a month name as a
    /// time zone, such as `EST` or `Europe/Helsinki`. Time zone names are
    /// not supported. Before any date or time, the word is read as invalid
    /// input, so that `'hello'` gives PostgreSQL's error.
    fn read_time_zone_name(&mut self) -> Result<u8> {
        if self.seen & (DATE | TIME) == 0 {
            return Err(syntax_error(self.input, self.ty));
        }
        self.unsupported_field(time_zone_not_supported(self.input, self.ty), TIME_ZONE)
    }

    /// A year as written. Only years of 4 digits are supported. PostgreSQL
    /// also reads other lengths: `24` as 2024, `999`, `10000`.
    fn read_year(&mut self, text: &str) -> Result<i32> {
        let (input, ty) = (self.input, self.ty);
        if !is_digits(text) {
            return Err(syntax_error(input, ty));
        }
        // Only a number too large for an `i32` fails to parse.
        let year: i32 = text.parse().map_err(|_| range_error(input))?;
        if text.len() != 4 {
            self.unsupported_field(
                not_supported(format!(
                    "years must be written with 4 digits, from 0001 to 9999: \"{input}\""
                )),
                0,
            )?;
        }
        self.two_digit_year = text.len() <= 2;
        Ok(year)
    }

    /// A field that PostgreSQL reads as `mask` and this parser does not.
    /// The fields after it are still read, so that input PostgreSQL rejects
    /// for another reason, such as `2024-01-05 1000 10:00` with two times,
    /// gets PostgreSQL's error.
    fn unsupported_field(&mut self, error: LimboError, mask: u8) -> Result<u8> {
        self.not_supported.get_or_insert(error);
        Ok(mask)
    }

    /// Puts the fields together and checks them in PostgreSQL's order, so
    /// that input PostgreSQL rejects gets PostgreSQL's error before a field
    /// this parser does not read is reported.
    fn finish(self) -> Result<Fields> {
        let (input, ty) = (self.input, self.ty);
        if self.iso_time_pending {
            return Err(syntax_error(input, ty));
        }
        let year = self.validate_date()?;
        let time = match self.time {
            Some(time) => Some(time),
            // As in PostgreSQL, AM or PM with no time is hour 0:
            // `2024-01-05 PM` is noon.
            None if self.pm.is_some() && ty != TemporalType::Time => Some((0, 0, 0, 0)),
            None => None,
        };
        let mut micros_of_day = None;
        if let Some((mut hour, minute, second, micros)) = time {
            match self.pm {
                Some(_) if hour > 12 => return Err(range_error(input)),
                Some(true) if hour < 12 => hour += 12,
                Some(false) if hour == 12 => hour = 0,
                Some(_) | None => {}
            }
            let micros = ((hour * 60 + minute) * 60 + second) * 1_000_000 + micros;
            if time_overflows(hour, minute, second, micros) {
                return Err(range_error(input));
            }
            micros_of_day = Some(micros);
        }
        // As in PostgreSQL, `time` needs a time and the other types a whole
        // date, which a field this parser does not read may have given.
        let needed = match ty {
            TemporalType::Time => TIME,
            TemporalType::Date | TemporalType::Timestamp | TemporalType::Timestamptz => DATE,
        };
        if self.seen & needed != needed {
            return Err(syntax_error(input, ty));
        }
        if let Some(error) = self.not_supported {
            return Err(error);
        }
        let date = match (year, self.month, self.day) {
            (Some(year), Some(month), Some(day)) => {
                let Some(date) = NaiveDate::from_ymd_opt(year, month, day) else {
                    unreachable!("validate_date checked {year}-{month}-{day}");
                };
                Some(date)
            }
            (None, None, None) => None,
            _ => unreachable!("a field that gives part of the date without its value is refused"),
        };
        Ok(Fields {
            date,
            time: micros_of_day,
            offset: self.offset,
        })
    }

    /// PostgreSQL's `ValidateDate`, on the parts of the date that were read.
    /// Returns the year as PostgreSQL counts it, with 1 BC as year 0.
    fn validate_date(&self) -> Result<Option<i32>> {
        let input = self.input;
        let year = match self.year {
            // There is no year 0: 1 BC comes before 1 AD.
            Some(year) if self.bc && year <= 0 => return Err(range_error(input)),
            Some(year) if self.bc => Some(1 - year),
            // A year of 1 or 2 digits is between 1970 and 2069.
            Some(year) if self.two_digit_year && year < 70 => Some(year + 2000),
            Some(year) if self.two_digit_year => Some(year + 1900),
            Some(year) if year <= 0 => return Err(range_error(input)),
            year => year,
        };
        let month_out_of_range = self.month.is_some_and(|month| !(1..=12).contains(&month));
        let day_out_of_range = self.day.is_some_and(|day| !(1..=31).contains(&day));
        if month_out_of_range || day_out_of_range {
            return Err(range_error(input));
        }
        if let (Some(year), Some(month), Some(day)) = (year, self.month, self.day) {
            if day > days_in_month(year, month) {
                return Err(range_error(input));
            }
        }
        Ok(year)
    }
}

/// The runs of letters and of digits in a date such as `Feb-05-2024`, split
/// as PostgreSQL's `DecodeDate` splits them. PostgreSQL drops the character
/// after each run, even a letter or a digit: `Feb-5a2024` is February 5, 2024.
/// Returns `None` when separators end the text after that dropped character,
/// which PostgreSQL rejects.
fn date_parts(text: &str) -> Option<Vec<&str>> {
    let bytes = text.as_bytes();
    let mut parts = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && !bytes[i].is_ascii_alphanumeric() {
            i += 1;
        }
        if i == bytes.len() {
            return None;
        }
        let start = i;
        let is_digit = bytes[i].is_ascii_digit();
        while i < bytes.len()
            && bytes[i].is_ascii_alphanumeric()
            && bytes[i].is_ascii_digit() == is_digit
        {
            i += 1;
        }
        parts.push(&text[start..i]);
        i += 1;
    }
    Some(parts)
}

/// PostgreSQL's `day_tab`, with its leap year rule for every year, year 0
/// and years BC included.
fn days_in_month(year: i32, month: u32) -> u32 {
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
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

/// A field of 1 or 2 digits, such as a month, a day or an hour.
fn two_digits<T: std::str::FromStr>(text: &str, input: &str, ty: TemporalType) -> Result<T> {
    if !is_digits(text) || text.len() > 2 {
        return Err(syntax_error(input, ty));
    }
    text.parse().map_err(|_| syntax_error(input, ty))
}

/// PostgreSQL also reads BC dates and years past 9999, as in
/// `0001-01-01 00:00+01` or `9999-12-31 23:00-02`.
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

    /// The value as it is stored, or as the DECODE prints it for a timestamp
    /// or a timestamptz.
    fn encode(input: &str, ty: &str) -> std::result::Result<String, String> {
        let decoded = store(input, ty).and_then(|stored| match ty {
            "timestamp" => exec_timestamp_decode(&stored),
            "timestamptz" => exec_timestamptz_decode(&stored),
            _ => Ok(stored),
        });
        match decoded {
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
            ("Sept 5 2024", "date", "2024-09-05"),
            ("sept5 2024", "timestamp", "2024-09-05 00:00:00"),
        ];
        for (input, ty, expected) in cases {
            assert_eq!(
                encode(input, ty).as_deref(),
                Ok(expected),
                "{input} as {ty}"
            );
        }
    }

    /// Input PostgreSQL 18 rejects, which is invalid input here too.
    #[test]
    fn rejects_input_postgresql_rejects() {
        let cases = [
            ("0000-01-01", "date"),
            ("allballs", "timestamp"),
            ("", "timestamp"),
            ("hello", "timestamptz"),
            ("2024-01-05", "time"),
            ("10:00", "date"),
            ("2024-01-05 10", "timestamp"),
            ("2024-01-05T10", "timestamp"),
            ("2024-01-05T10300", "timestamp"),
            ("2024-01-05 10:00 1000", "timestamp"),
            ("2024-01-05 1000 10:00", "timestamp"),
            ("24:00:00.1", "time"),
            ("23:59:60.5", "time"),
            ("13:00 PM", "time"),
            ("2024-01-05 10:00:00+16", "timestamptz"),
            ("2024-01-05 2024-01-06", "date"),
            ("+02 PM 2024-01-05", "timestamp"),
            ("15/01/2024", "date"),
            ("01/32/2024", "date"),
            ("13-05-2024 10:00", "timestamp"),
            ("01/05/", "date"),
            ("02/30/2024", "date"),
            ("02/29/23", "date"),
            ("000-01-05", "date"),
            ("99999999999-01-05", "date"),
            // Input with a field this parser does not read, which PostgreSQL
            // rejects for another reason.
            ("2024-13-05 10:00 EST", "timestamptz"),
            ("2024-01-05 25:00 EST", "timestamptz"),
            ("2024-02-30 10:00 EST", "timestamptz"),
            ("2024-01-05T EST", "timestamptz"),
            ("2024-01-05 Europe/Helsinki 10:00 11:00", "timestamptz"),
            ("10:00 EST", "timestamptz"),
            ("2024-13-05 BC", "date"),
            ("0000-01-05 BC", "date"),
            ("BC 2024-01-05", "date"),
            ("2024-01-05 10:00 ad bc", "timestamp"),
            ("10000-13-01", "date"),
            ("Jan-05-2024 10:00 11:00", "timestamp"),
            ("Fri 2024-01-05", "timestamp"),
            ("2024-01-05 10:00 Fri Sat", "timestamp"),
            ("10:00 Fri", "timestamp"),
            ("2024 on", "timestamp"),
            ("on", "date"),
            ("Jan 35 2024 EST", "timestamptz"),
            ("Jan 5 00 BC", "timestamp"),
            ("01/05/00 BC", "date"),
            ("2023-02-29 BC", "timestamp"),
            ("Feb 29 4 BC", "timestamp"),
            ("0001-02-29 BC 10:00 11:00", "timestamp"),
            ("10:00 Feb-05-2024", "timestamp"),
            ("Fri Feb-05-2024", "timestamp"),
            ("PM Feb-05-2024", "timestamp"),
            ("BC Feb-05-2024", "timestamp"),
            ("Jan 2024 Feb-05", "timestamp"),
            ("Feb-05 2024", "timestamp"),
            ("Feb-05 on 2024", "timestamp"),
            ("Feb-05-", "timestamp"),
            ("Feb--", "timestamp"),
            ("Feb-05-2024-1000", "timestamp"),
            ("Jan-Feb-05", "timestamp"),
            ("Feb-05bc-2024", "timestamp"),
            ("2024 Feb-05-06", "timestamp"),
            ("2024 on-01-05", "timestamp"),
            ("on-01-05-2024", "timestamp"),
            ("at-jan-05-2024", "timestamp"),
            ("Feb-99999999999-2024", "timestamp"),
            ("2024 Europe/Helsinki", "timestamptz"),
            ("Europe/Helsinki 2024-01-05", "timestamptz"),
            ("2024-01-05 sat10:00 11:00", "timestamp"),
        ];
        for (input, ty) in cases {
            let error = encode_error(input, ty);
            assert!(
                matches!(
                    error,
                    LimboError::InvalidDate(_) | LimboError::DatetimeFieldOverflow(_)
                ),
                "{input} as {ty}: {error:?}"
            );
        }
        assert!(encode("2024-01-05", "interval").is_err());
        // PostgreSQL reads this as January 5, and this parser reports it as
        // invalid input.
        assert!(encode("05-Jan-2024", "date").is_err());
    }

    /// Input PostgreSQL 18 reads and this parser does not.
    #[test]
    fn refuses_input_postgresql_reads_and_this_parser_does_not() {
        let cases = [
            ("-infinity", "timestamptz"),
            ("now", "timestamp"),
            ("0044-03-15 BC", "date"),
            ("10000-01-01", "date"),
            ("0001-01-01 00:00:00+01", "timestamptz"),
            ("01/05/2024", "date"),
            ("01/05/24", "timestamp"),
            ("240105", "timestamp"),
            ("2024 100", "timestamp"),
            ("2024-01-05 1000", "timestamp"),
            ("2024-01-05T103000", "timestamp"),
            ("2024-01-05 Jan-05", "timestamp"),
            ("2024-01-05 10:00 EST", "timestamptz"),
            ("2024-01-05 10:00 UTC", "timestamptz"),
            ("2024-01-05 10:00 Europe/Helsinki", "timestamptz"),
            ("2024-01-05 Z10:00", "timestamp"),
            ("2024-01-05 BC", "date"),
            ("2024-01-05 ad", "date"),
            ("999-01-05", "date"),
            ("02/29/24", "date"),
            ("Jan 5 24", "date"),
            ("Jan-05-2024 10:00", "timestamp"),
            ("Fri Jan 05 10:00:00 2024", "timestamp"),
            ("2024-01-05 10:00 Fri", "timestamp"),
            ("on 2024-01-05", "timestamp"),
            ("2024-01-05 at 10:00", "timestamp"),
            ("0001-02-29 BC", "timestamp"),
            ("02/29/01 BC", "date"),
            ("270000-01-05", "timestamp"),
            ("300000-01-05", "date"),
            ("2024 Feb-05", "timestamptz"),
            ("Jan 5 Feb-05 2024", "timestamp"),
            ("Feb-05-2024-", "timestamp"),
            ("Feb-5a2024", "timestamp"),
            ("Feb-2024-05", "timestamp"),
            ("2024-01-05 fri10:00", "timestamp"),
            ("Fri05 Jan 2024", "timestamp"),
            ("today 10:00", "timestamp"),
            ("J2451187", "timestamp"),
        ];
        for (input, ty) in cases {
            let error = encode_error(input, ty);
            assert!(
                matches!(error, LimboError::DatetimeNotSupported(_)),
                "{input} as {ty}: {error:?}"
            );
        }
    }

    fn encode_error(input: &str, ty: &str) -> LimboError {
        store(input, ty).expect_err(&format!("{input} as {ty} must fail"))
    }

    /// The value as it is stored.
    fn store(input: &str, ty: &str) -> Result<Value> {
        let value = Value::build_text(input.to_string());
        match ty {
            "timestamp" => exec_timestamp_encode(&value),
            "timestamptz" => exec_timestamptz_encode(&value),
            _ => exec_temporal_encode(&value, &Value::build_text(ty.to_string())),
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
    /// DECODE's text, so both must give the same order.
    #[test]
    fn decoded_text_sorts_as_the_stored_integer() {
        let inputs = [
            "0001-01-01 00:00:00",
            "0999-12-31 23:59:59.999999",
            "1999-12-31 23:59:59.9",
            "2000-01-01 00:00:00",
            "2000-01-01 00:00:00.000001",
            "2000-01-01 00:00:00.05",
            "2000-01-01 00:00:00.5",
            "2000-01-01 00:00:00.55",
            "2000-01-01 00:00:01",
            "2000-01-01 10:00:00",
            "2024-02-29 12:00:00",
            "9999-12-31 23:59:59.999999",
        ];
        for (encode, decode) in [
            (
                exec_timestamp_encode as fn(&Value) -> Result<Value>,
                exec_timestamp_decode as fn(&Value) -> Result<Value>,
            ),
            (exec_timestamptz_encode, exec_timestamptz_decode),
        ] {
            let mut values: Vec<(i64, String)> = inputs
                .iter()
                .map(|input| {
                    let stored = encode(&Value::build_text(input.to_string())).unwrap();
                    let text = decode(&stored).unwrap().to_text().unwrap().to_string();
                    let Value::Numeric(Numeric::Integer(stored)) = stored else {
                        panic!("{input} is stored as {stored:?}");
                    };
                    (stored, text)
                })
                .collect();
            values.sort_by_key(|(stored, _)| *stored);
            assert!(
                values.windows(2).all(|pair| pair[0].1 < pair[1].1),
                "{values:?}"
            );
        }
    }
}
