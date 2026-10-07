use crate::numeric::decimal::{parse_decimal_text, value_to_bigdecimal};
use crate::types::Value;
use crate::{turso_assert_eq, LimboError, Numeric, Result};
use bigdecimal::BigDecimal;
use num_bigint::{BigInt, Sign};
use num_traits::{ToPrimitive, Zero};

const MICROSECONDS_PER_SECOND: i64 = 1_000_000;
const MICROSECONDS_PER_MINUTE: i64 = 60 * MICROSECONDS_PER_SECOND;
const MICROSECONDS_PER_HOUR: i64 = 60 * MICROSECONDS_PER_MINUTE;
const MICROSECONDS_PER_DAY: i64 = 24 * MICROSECONDS_PER_HOUR;
const UNIX_DAYS_AT_2000_01_01: i64 = 10_957;
const FIRST_YEAR: i64 = 1;
const LAST_YEAR: i64 = 9999;
const FIRST_DAY: i64 = days_since_2000(FIRST_YEAR, 1, 1);
const LAST_DAY: i64 = days_since_2000(LAST_YEAR, 12, 31);
const FIRST_TIMESTAMP: i64 = FIRST_DAY * MICROSECONDS_PER_DAY;
const LAST_TIMESTAMP: i64 = (LAST_DAY + 1) * MICROSECONDS_PER_DAY - 1;
const MAX_OFFSET_HOURS: i64 = 15;
pub(crate) const MAX_NUMERIC_PRECISION: i64 = 18;
pub(crate) const NUMERIC_STORED_LIMIT: i128 = 10i128.pow(MAX_NUMERIC_PRECISION as u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PgTemporal {
    Timestamp,
    Timestamptz,
    Date,
    Time,
}

impl PgTemporal {
    fn type_name(self) -> &'static str {
        match self {
            Self::Timestamp => "timestamp",
            Self::Timestamptz => "timestamp with time zone",
            Self::Date => "date",
            Self::Time => "time",
        }
    }

    pub(crate) fn stored_range(self) -> (i64, i64) {
        match self {
            Self::Timestamp | Self::Timestamptz => (FIRST_TIMESTAMP, LAST_TIMESTAMP),
            Self::Date => (FIRST_DAY, LAST_DAY),
            Self::Time => (0, MICROSECONDS_PER_DAY),
        }
    }
}

pub(crate) fn exec_pg_temporal_encode(kind: PgTemporal, value: &Value) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Text(text) => parse_temporal(kind, text.as_str()).map(Value::from_i64),
        other => Err(LimboError::Constraint(format!(
            "invalid input for type {}: {} is not text",
            kind.type_name(),
            describe_value(other)
        ))),
    }
}

pub(crate) fn exec_pg_temporal_decode(kind: PgTemporal, value: &Value) -> Result<Value> {
    let stored = match value {
        Value::Null => return Ok(Value::Null),
        Value::Numeric(Numeric::Integer(stored)) => *stored,
        other => {
            return Err(LimboError::Corrupt(format!(
                "stored {} value is not an integer: {}",
                kind.type_name(),
                describe_value(other)
            )))
        }
    };
    let text = format_temporal(kind, stored).ok_or_else(|| {
        LimboError::Corrupt(format!(
            "stored {} value {stored} is out of range",
            kind.type_name()
        ))
    })?;
    Ok(Value::build_text(text.as_str().to_string()))
}

pub(crate) fn exec_pg_temporal_cast(kind: PgTemporal, value: &Value) -> Result<Value> {
    let stored = exec_pg_temporal_encode(kind, value)?;
    exec_pg_temporal_decode(kind, &stored)
}

pub(crate) fn exec_pg_numeric_encode(
    value: &Value,
    precision: &Value,
    scale: &Value,
) -> Result<Value> {
    let precision = numeric_type_parameter("precision", precision)?;
    let scale = numeric_type_parameter("scale", scale)?;
    check_numeric_type_parameters(precision, scale)?;
    let decimal = match value {
        Value::Null => return Ok(Value::Null),
        Value::Text(text) => parse_decimal_text(text.as_str()).ok_or_else(|| {
            LimboError::Constraint(format!(
                "invalid input syntax for type numeric: \"{}\"",
                text.as_str()
            ))
        })?,
        Value::Numeric(_) => value_to_bigdecimal(value)?,
        Value::Blob(_) => {
            return Err(LimboError::Constraint(
                "invalid input for type numeric: blob is not a number".to_string(),
            ))
        }
    };
    let overflow = || {
        LimboError::Constraint(format!(
            "numeric field overflow: a field with precision {precision}, scale {scale} must round to an absolute value less than 10^{}",
            precision - scale
        ))
    };
    let rounded = match scale_decimal(&decimal, scale) {
        ScaledDecimal::InRange {
            truncated,
            remainder,
        } => truncated + i128::from(remainder.rounds_away_from_zero()) * truncated_sign(&decimal),
        ScaledDecimal::Above | ScaledDecimal::Below => return Err(overflow()),
    };
    if rounded.abs() >= 10i128.pow(precision as u32) {
        return Err(overflow());
    }
    Ok(Value::from_i64(
        i64::try_from(rounded).expect("a value below 10^18 fits an i64"),
    ))
}

pub(crate) fn exec_pg_numeric_decode(value: &Value, scale: &Value) -> Result<Value> {
    let scale = numeric_type_parameter("scale", scale)?;
    check_numeric_scale(scale)?;
    match value {
        Value::Null => Ok(Value::Null),
        Value::Numeric(Numeric::Integer(stored)) => {
            Ok(Value::build_text(format_scaled_integer(*stored, scale)))
        }
        other => Err(LimboError::Corrupt(format!(
            "stored numeric value is not an integer: {}",
            describe_value(other)
        ))),
    }
}

pub(crate) fn numeric_type_parameter(name: &str, value: &Value) -> Result<i64> {
    match value {
        Value::Numeric(Numeric::Integer(value)) => Ok(*value),
        other => Err(LimboError::Constraint(format!(
            "pg_numeric: {name} must be an integer, got {}",
            describe_value(other)
        ))),
    }
}

pub(crate) fn check_numeric_scale(scale: i64) -> Result<()> {
    if !(0..=MAX_NUMERIC_PRECISION).contains(&scale) {
        return Err(LimboError::Constraint(format!(
            "pg_numeric needs a scale from 0 to {MAX_NUMERIC_PRECISION}, got {scale}"
        )));
    }
    Ok(())
}

fn check_numeric_type_parameters(precision: i64, scale: i64) -> Result<()> {
    if !(1..=MAX_NUMERIC_PRECISION).contains(&precision) || !(0..=precision).contains(&scale) {
        return Err(LimboError::Constraint(format!(
            "pg_numeric needs a precision from 1 to {MAX_NUMERIC_PRECISION} and a scale from 0 to the precision, got ({precision}, {scale})"
        )));
    }
    Ok(())
}

fn truncated_sign(decimal: &BigDecimal) -> i128 {
    match decimal.sign() {
        Sign::Minus => -1,
        Sign::NoSign | Sign::Plus => 1,
    }
}

fn describe_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Numeric(Numeric::Integer(i)) => format!("integer {i}"),
        Value::Numeric(Numeric::Float(f)) => format!("real {f}"),
        Value::Text(text) => format!("text \"{}\"", text.as_str()),
        Value::Blob(_) => "a blob".to_string(),
    }
}

pub(crate) fn format_scaled_integer(stored: i64, scale: i64) -> String {
    let sign = if stored < 0 { "-" } else { "" };
    let magnitude = stored.unsigned_abs();
    if scale == 0 {
        return format!("{sign}{magnitude}");
    }
    let divisor = 10u64.pow(scale as u32);
    format!(
        "{sign}{}.{:0width$}",
        magnitude / divisor,
        magnitude % divisor,
        width = scale as usize
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Remainder {
    None,
    BelowHalf,
    Half,
    AboveHalf,
}

impl Remainder {
    fn rounds_away_from_zero(self) -> bool {
        matches!(self, Self::Half | Self::AboveHalf)
    }
}

/// `decimal * 10^scale`, truncated toward zero, when its magnitude is below
/// 10^19.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScaledDecimal {
    InRange {
        truncated: i128,
        remainder: Remainder,
    },
    Above,
    Below,
}

pub(crate) fn scale_decimal(decimal: &BigDecimal, scale: i64) -> ScaledDecimal {
    let (digits, exponent) = decimal.as_bigint_and_exponent();
    if digits.is_zero() {
        return ScaledDecimal::InRange {
            truncated: 0,
            remainder: Remainder::None,
        };
    }
    let out_of_range = if digits.sign() == Sign::Minus {
        ScaledDecimal::Below
    } else {
        ScaledDecimal::Above
    };
    let digit_count = decimal_digit_count(digits.magnitude());
    let shift = scale.saturating_sub(exponent);
    if digit_count.saturating_add(shift) > 19 {
        return out_of_range;
    }
    if shift >= 0 {
        let truncated =
            digits.to_i128().expect("fewer than 20 digits fit an i128") * 10i128.pow(shift as u32);
        return ScaledDecimal::InRange {
            truncated,
            remainder: Remainder::None,
        };
    }
    let divisor_digits = shift.unsigned_abs();
    if divisor_digits > digit_count as u64 {
        return ScaledDecimal::InRange {
            truncated: 0,
            remainder: Remainder::BelowHalf,
        };
    }
    let divisor = BigInt::from(10).pow(divisor_digits as u32);
    let truncated = &digits / &divisor;
    let remainder = (&digits % &divisor).magnitude() * 2u32;
    let remainder = match remainder.cmp(divisor.magnitude()) {
        _ if remainder.is_zero() => Remainder::None,
        std::cmp::Ordering::Less => Remainder::BelowHalf,
        std::cmp::Ordering::Equal => Remainder::Half,
        std::cmp::Ordering::Greater => Remainder::AboveHalf,
    };
    ScaledDecimal::InRange {
        truncated: truncated
            .to_i128()
            .expect("a quotient below 10^19 fits an i128"),
        remainder,
    }
}

fn decimal_digit_count(magnitude: &num_bigint::BigUint) -> i64 {
    match magnitude.to_u128() {
        Some(small) => i64::from(small.ilog10()) + 1,
        None => magnitude.to_string().len() as i64,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemporalError {
    Syntax,
    FieldOutOfRange,
    OutOfRange,
    Infinity,
}

fn parse_temporal(kind: PgTemporal, text: &str) -> Result<i64> {
    let trimmed = text.trim_matches(|c: char| c.is_ascii_whitespace());
    let stored = match special_word(kind, trimmed) {
        Some(stored) => stored,
        None => TemporalFields::parse(trimmed).and_then(|fields| fields.stored(kind)),
    };
    stored.map_err(|error| {
        let type_name = kind.type_name();
        LimboError::Constraint(match error {
            TemporalError::Syntax => format!("invalid input syntax for type {type_name}: \"{text}\""),
            TemporalError::FieldOutOfRange => {
                format!("date/time field value out of range: \"{text}\"")
            }
            TemporalError::OutOfRange => format!(
                "{type_name} out of range: \"{text}\": the years {FIRST_YEAR} to {LAST_YEAR} are supported"
            ),
            TemporalError::Infinity => {
                format!("{type_name} out of range: \"{text}\": infinity is not supported")
            }
        })
    })
}

pub(crate) fn parse_canonical_temporal(kind: PgTemporal, text: &str) -> Option<i64> {
    let stored = TemporalFields::parse(text)
        .and_then(|fields| fields.stored(kind))
        .ok()?;
    let canonical = format_temporal(kind, stored)?;
    (canonical.as_str() == text).then_some(stored)
}

fn special_word(kind: PgTemporal, text: &str) -> Option<std::result::Result<i64, TemporalError>> {
    if !text
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'+' || b == b'-')
    {
        return None;
    }
    let word = text.to_ascii_lowercase();
    let day_offset = match word.as_str() {
        "infinity" | "+infinity" | "-infinity" => return Some(Err(TemporalError::Infinity)),
        "now" => {
            let now = current_microseconds_since_2000();
            return Some(Ok(match kind {
                PgTemporal::Timestamp | PgTemporal::Timestamptz => now,
                PgTemporal::Date => now.div_euclid(MICROSECONDS_PER_DAY),
                PgTemporal::Time => now.rem_euclid(MICROSECONDS_PER_DAY),
            }));
        }
        "allballs" => {
            return Some(match kind {
                PgTemporal::Time => Ok(0),
                _ => Err(TemporalError::Syntax),
            })
        }
        "epoch" => {
            return Some(Ok(match kind {
                PgTemporal::Timestamp | PgTemporal::Timestamptz => {
                    days_since_2000(1970, 1, 1) * MICROSECONDS_PER_DAY
                }
                PgTemporal::Date => days_since_2000(1970, 1, 1),
                PgTemporal::Time => return Some(Err(TemporalError::Syntax)),
            }))
        }
        "today" => 0,
        "tomorrow" => 1,
        "yesterday" => -1,
        _ => return None,
    };
    let today = current_microseconds_since_2000().div_euclid(MICROSECONDS_PER_DAY);
    let day = today + day_offset;
    Some(match kind {
        PgTemporal::Timestamp | PgTemporal::Timestamptz => Ok(day * MICROSECONDS_PER_DAY),
        PgTemporal::Date => Ok(day),
        PgTemporal::Time => Err(TemporalError::Syntax),
    })
}

pub(crate) const CLOCK_WORDS: [&str; 4] = ["now", "today", "tomorrow", "yesterday"];

pub(crate) fn is_clock_word(text: &str) -> bool {
    CLOCK_WORDS
        .iter()
        .any(|word| word.eq_ignore_ascii_case(text))
}

fn current_microseconds_since_2000() -> i64 {
    let since_unix_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the system clock is after 1970");
    since_unix_epoch.as_micros() as i64 - UNIX_DAYS_AT_2000_01_01 * MICROSECONDS_PER_DAY
}

#[derive(Debug, Clone, Copy)]
struct CivilDate {
    year: i64,
    month: i64,
    day: i64,
}

#[derive(Debug, Default)]
struct TemporalFields {
    date: Option<CivilDate>,
    time_of_day: Option<i64>,
    offset_seconds: Option<i64>,
}

impl TemporalFields {
    fn parse(text: &str) -> std::result::Result<Self, TemporalError> {
        let mut cursor = Cursor::new(text);
        let mut fields = Self::default();
        if cursor.leading_digits_end_with(b'-') {
            fields.date = Some(cursor.date()?);
            if cursor.is_done() {
                return Ok(fields);
            }
            if !(cursor.eat(b'T') || cursor.eat(b't') || cursor.skip_spaces()) {
                return Err(TemporalError::Syntax);
            }
        }
        fields.time_of_day = Some(cursor.time_of_day()?);
        let has_space = cursor.skip_spaces();
        if !cursor.is_done() {
            fields.offset_seconds = Some(cursor.offset_seconds()?);
        } else if has_space {
            return Err(TemporalError::Syntax);
        }
        if !cursor.is_done() {
            return Err(TemporalError::Syntax);
        }
        Ok(fields)
    }

    fn stored(&self, kind: PgTemporal) -> std::result::Result<i64, TemporalError> {
        let days = self.date.map(checked_days).transpose()?;
        match kind {
            PgTemporal::Timestamp | PgTemporal::Timestamptz => {
                let days = days.ok_or(TemporalError::Syntax)?;
                if !(FIRST_DAY - 1..=LAST_DAY + 1).contains(&days) {
                    return Err(TemporalError::OutOfRange);
                }
                let mut stored = days * MICROSECONDS_PER_DAY + self.time_of_day.unwrap_or(0);
                if kind == PgTemporal::Timestamptz {
                    stored -= self.offset_seconds.unwrap_or(0) * MICROSECONDS_PER_SECOND;
                }
                if !(FIRST_TIMESTAMP..=LAST_TIMESTAMP).contains(&stored) {
                    return Err(TemporalError::OutOfRange);
                }
                Ok(stored)
            }
            PgTemporal::Date => {
                let days = days.ok_or(TemporalError::Syntax)?;
                if !(FIRST_DAY..=LAST_DAY).contains(&days) {
                    return Err(TemporalError::OutOfRange);
                }
                Ok(days)
            }
            PgTemporal::Time => self.time_of_day.ok_or(TemporalError::Syntax),
        }
    }
}

fn checked_days(date: CivilDate) -> std::result::Result<i64, TemporalError> {
    if !(1..=12).contains(&date.month) || date.day < 1 || date.year < 1 {
        return Err(TemporalError::FieldOutOfRange);
    }
    if date.year > LAST_YEAR + 1 {
        return Err(TemporalError::OutOfRange);
    }
    if date.day > days_in_month(date.year, date.month) {
        return Err(TemporalError::FieldOutOfRange);
    }
    Ok(days_since_2000(date.year, date.month, date.day))
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            bytes: text.as_bytes(),
            position: 0,
        }
    }

    fn is_done(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.position).copied()
    }

    fn eat(&mut self, byte: u8) -> bool {
        if self.peek() == Some(byte) {
            self.position += 1;
            return true;
        }
        false
    }

    fn eat_word(&mut self, word: &str) -> bool {
        let end = self.position + word.len();
        if self
            .bytes
            .get(self.position..end)
            .is_some_and(|bytes| bytes.eq_ignore_ascii_case(word.as_bytes()))
        {
            self.position = end;
            return true;
        }
        false
    }

    fn skip_spaces(&mut self) -> bool {
        let start = self.position;
        while self.peek().is_some_and(|b| b.is_ascii_whitespace()) {
            self.position += 1;
        }
        self.position > start
    }

    fn leading_digits_end_with(&self, byte: u8) -> bool {
        let digits = self.bytes[self.position..]
            .iter()
            .take_while(|b| b.is_ascii_digit())
            .count();
        digits > 0 && self.bytes.get(self.position + digits) == Some(&byte)
    }

    fn number(
        &mut self,
        min_digits: usize,
        max_digits: usize,
    ) -> std::result::Result<i64, TemporalError> {
        let digits = self.bytes[self.position..]
            .iter()
            .take(max_digits)
            .take_while(|b| b.is_ascii_digit())
            .count();
        if digits < min_digits {
            return Err(TemporalError::Syntax);
        }
        let value = self.bytes[self.position..self.position + digits]
            .iter()
            .fold(0i64, |value, digit| value * 10 + i64::from(digit - b'0'));
        self.position += digits;
        Ok(value)
    }

    fn expect(&mut self, byte: u8) -> std::result::Result<(), TemporalError> {
        if self.eat(byte) {
            Ok(())
        } else {
            Err(TemporalError::Syntax)
        }
    }

    fn date(&mut self) -> std::result::Result<CivilDate, TemporalError> {
        let year = self.number(3, 9)?;
        self.expect(b'-')?;
        let month = self.number(1, 2)?;
        self.expect(b'-')?;
        let day = self.number(1, 2)?;
        Ok(CivilDate { year, month, day })
    }

    fn time_of_day(&mut self) -> std::result::Result<i64, TemporalError> {
        let hour = self.number(1, 2)?;
        self.expect(b':')?;
        let minute = self.number(2, 2)?;
        let mut second = 0;
        let mut microsecond = 0;
        if self.eat(b':') {
            second = self.number(2, 2)?;
            if self.eat(b'.') {
                microsecond = self.rounded_microseconds()?;
            }
        }
        if hour > 24 || minute > 59 || second > 59 {
            return Err(TemporalError::FieldOutOfRange);
        }
        let time_of_day = hour * MICROSECONDS_PER_HOUR
            + minute * MICROSECONDS_PER_MINUTE
            + second * MICROSECONDS_PER_SECOND
            + microsecond;
        if time_of_day > MICROSECONDS_PER_DAY {
            return Err(TemporalError::FieldOutOfRange);
        }
        Ok(time_of_day)
    }

    fn rounded_microseconds(&mut self) -> std::result::Result<i64, TemporalError> {
        let digits = self.bytes[self.position..]
            .iter()
            .take_while(|b| b.is_ascii_digit())
            .count();
        if digits == 0 {
            return Err(TemporalError::Syntax);
        }
        let point = self.position - 1;
        turso_assert_eq!(self.bytes[point], b'.');
        self.position += digits;
        let fraction: f64 = std::str::from_utf8(&self.bytes[point..self.position])
            .expect("a point and ASCII digits")
            .parse()
            .expect("a point and ASCII digits parse as a number");
        Ok((fraction * MICROSECONDS_PER_SECOND as f64).round_ties_even() as i64)
    }

    fn offset_seconds(&mut self) -> std::result::Result<i64, TemporalError> {
        if self.eat(b'Z') || self.eat(b'z') || self.eat_word("utc") || self.eat_word("gmt") {
            return Ok(0);
        }
        let sign = if self.eat(b'+') {
            1
        } else if self.eat(b'-') {
            -1
        } else {
            return Err(TemporalError::Syntax);
        };
        let hours = self.number(1, 2)?;
        let mut minutes = 0;
        let mut seconds = 0;
        let has_colon = self.eat(b':');
        if has_colon || self.peek().is_some_and(|b| b.is_ascii_digit()) {
            minutes = self.number(2, 2)?;
            if (has_colon && self.eat(b':'))
                || (!has_colon && self.peek().is_some_and(|b| b.is_ascii_digit()))
            {
                seconds = self.number(2, 2)?;
            }
        }
        if hours > MAX_OFFSET_HOURS || minutes > 59 || seconds > 59 {
            return Err(TemporalError::FieldOutOfRange);
        }
        Ok(sign * (hours * 3600 + minutes * 60 + seconds))
    }
}

pub(crate) struct TemporalText {
    bytes: [u8; 26],
    len: usize,
}

impl TemporalText {
    fn new() -> Self {
        Self {
            bytes: [0; 26],
            len: 0,
        }
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }

    pub(crate) fn as_str(&self) -> &str {
        std::str::from_utf8(self.as_bytes()).expect("temporal text is ASCII")
    }

    fn push(&mut self, byte: u8) {
        self.bytes[self.len] = byte;
        self.len += 1;
    }

    fn push_number(&mut self, mut value: i64, width: usize) {
        for i in (0..width).rev() {
            self.bytes[self.len + i] = b'0' + (value % 10) as u8;
            value /= 10;
        }
        self.len += width;
    }
}

pub(crate) fn format_temporal(kind: PgTemporal, stored: i64) -> Option<TemporalText> {
    let (first, last) = kind.stored_range();
    if !(first..=last).contains(&stored) {
        return None;
    }
    let mut text = TemporalText::new();
    match kind {
        PgTemporal::Timestamp | PgTemporal::Timestamptz => {
            push_date(&mut text, stored.div_euclid(MICROSECONDS_PER_DAY));
            text.push(b' ');
            push_time_of_day(&mut text, stored.rem_euclid(MICROSECONDS_PER_DAY));
        }
        PgTemporal::Date => push_date(&mut text, stored),
        PgTemporal::Time => push_time_of_day(&mut text, stored),
    }
    Some(text)
}

fn push_date(text: &mut TemporalText, days_since_2000: i64) {
    let (year, month, day) = civil_from_unix_days(days_since_2000 + UNIX_DAYS_AT_2000_01_01);
    text.push_number(year, 4);
    text.push(b'-');
    text.push_number(month, 2);
    text.push(b'-');
    text.push_number(day, 2);
}

fn push_time_of_day(text: &mut TemporalText, microseconds: i64) {
    text.push_number(microseconds / MICROSECONDS_PER_HOUR, 2);
    text.push(b':');
    text.push_number(
        microseconds % MICROSECONDS_PER_HOUR / MICROSECONDS_PER_MINUTE,
        2,
    );
    text.push(b':');
    text.push_number(
        microseconds % MICROSECONDS_PER_MINUTE / MICROSECONDS_PER_SECOND,
        2,
    );
    let mut fraction = microseconds % MICROSECONDS_PER_SECOND;
    if fraction == 0 {
        return;
    }
    let mut width = 6;
    while fraction % 10 == 0 {
        fraction /= 10;
        width -= 1;
    }
    text.push(b'.');
    text.push_number(fraction, width);
}

const fn days_since_2000(year: i64, month: i64, day: i64) -> i64 {
    unix_days_from_civil(year, month, day) - UNIX_DAYS_AT_2000_01_01
}

const fn unix_days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_from_march = (month + 9) % 12;
    let day_of_year = (153 * month_from_march + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_unix_days(unix_days: i64) -> (i64, i64, i64) {
    let days = unix_days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_from_march = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_from_march + 2) / 5 + 1;
    let month = if month_from_march < 10 {
        month_from_march + 3
    } else {
        month_from_march - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    fn encode(kind: PgTemporal, text: &str) -> Result<i64> {
        match exec_pg_temporal_encode(kind, &Value::build_text(text.to_string()))? {
            Value::Numeric(Numeric::Integer(stored)) => Ok(stored),
            other => panic!("unexpected stored value {other:?}"),
        }
    }

    fn decode(kind: PgTemporal, stored: i64) -> String {
        format_temporal(kind, stored).unwrap().as_str().to_string()
    }

    fn round_trip(kind: PgTemporal, text: &str) -> String {
        decode(kind, encode(kind, text).unwrap())
    }

    fn error(kind: PgTemporal, text: &str) -> String {
        encode(kind, text).unwrap_err().to_string()
    }

    #[test]
    fn stored_values_count_from_2000() {
        assert_eq!(encode(PgTemporal::Date, "2000-01-01").unwrap(), 0);
        assert_eq!(encode(PgTemporal::Date, "2000-01-02").unwrap(), 1);
        assert_eq!(encode(PgTemporal::Date, "1999-12-31").unwrap(), -1);
        assert_eq!(
            encode(PgTemporal::Timestamp, "2000-01-01 00:00:01").unwrap(),
            1_000_000
        );
        assert_eq!(encode(PgTemporal::Time, "00:00:00.000001").unwrap(), 1);
        assert_eq!(
            encode(PgTemporal::Time, "24:00:00").unwrap(),
            MICROSECONDS_PER_DAY
        );
        assert_eq!(
            encode(PgTemporal::Timestamp, "1970-01-01").unwrap(),
            -946_684_800 * MICROSECONDS_PER_SECOND
        );
    }

    #[test]
    fn decode_gives_canonical_text() {
        for (kind, input, expected) in [
            (PgTemporal::Timestamp, "2024-01-01", "2024-01-01 00:00:00"),
            (
                PgTemporal::Timestamp,
                "2024-1-5 7:08",
                "2024-01-05 07:08:00",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01T10:00:00Z",
                "2024-01-01 10:00:00",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01 10:00:00+02",
                "2024-01-01 10:00:00",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01 10:00:00.120",
                "2024-01-01 10:00:00.12",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01 10:00:00.1234567",
                "2024-01-01 10:00:00.123457",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01 23:59:59.9999995",
                "2024-01-02 00:00:00",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01 24:00:00",
                "2024-01-02 00:00:00",
            ),
            (
                PgTemporal::Timestamp,
                " 2024-02-29 10:00 ",
                "2024-02-29 10:00:00",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01 10:00:00 UTC",
                "2024-01-01 10:00:00",
            ),
            (PgTemporal::Timestamp, "0001-01-01", "0001-01-01 00:00:00"),
            (
                PgTemporal::Timestamp,
                "9999-12-31 23:59:59.999999",
                "9999-12-31 23:59:59.999999",
            ),
            (
                PgTemporal::Timestamptz,
                "2024-01-01 10:00:00+02",
                "2024-01-01 08:00:00",
            ),
            (
                PgTemporal::Timestamptz,
                "2024-01-01 10:00:00-0530",
                "2024-01-01 15:30:00",
            ),
            (
                PgTemporal::Timestamptz,
                "2024-01-01 01:00:00+02:00",
                "2023-12-31 23:00:00",
            ),
            (
                PgTemporal::Timestamptz,
                "2024-01-01T10:00:00z",
                "2024-01-01 10:00:00",
            ),
            (PgTemporal::Date, "2024-01-01 23:00:00+05", "2024-01-01"),
            (PgTemporal::Date, "2024-3-7", "2024-03-07"),
            (PgTemporal::Time, "10:00", "10:00:00"),
            (PgTemporal::Time, "2024-01-01 10:00:00.5", "10:00:00.5"),
            (PgTemporal::Time, "10:00:00+02", "10:00:00"),
            (PgTemporal::Time, "24:00:00", "24:00:00"),
            (PgTemporal::Time, "23:59:59.9999999", "24:00:00"),
            (PgTemporal::Time, "allballs", "00:00:00"),
            (PgTemporal::Date, "epoch", "1970-01-01"),
            (PgTemporal::Timestamp, "EPOCH", "1970-01-01 00:00:00"),
        ] {
            assert_eq!(round_trip(kind, input), expected, "{kind:?} {input}");
        }
    }

    #[test]
    fn years_of_three_or_more_digits_are_accepted() {
        assert_eq!(round_trip(PgTemporal::Date, "124-01-01"), "0124-01-01");
        assert_eq!(round_trip(PgTemporal::Date, "0001-01-01"), "0001-01-01");
        assert_eq!(
            round_trip(PgTemporal::Timestamp, "999-12-31 10:00"),
            "0999-12-31 10:00:00"
        );
    }

    #[test]
    fn fraction_at_half_rounds_as_postgresql() {
        for (input, expected) in [
            ("10:00:00.0000005", "10:00:00"),
            ("10:00:00.0000015", "10:00:00.000002"),
            ("10:00:00.0000025", "10:00:00.000002"),
            ("10:00:00.1234565", "10:00:00.123456"),
            ("10:00:00.5000005", "10:00:00.5"),
            ("10:00:00.12345651", "10:00:00.123457"),
            ("23:59:59.9999995", "24:00:00"),
            ("10:59:59.99999999999999999999", "11:00:00"),
        ] {
            assert_eq!(round_trip(PgTemporal::Time, input), expected, "{input}");
        }
    }

    #[test]
    fn clock_words_are_the_words_that_read_the_clock() {
        for word in CLOCK_WORDS {
            assert!(special_word(PgTemporal::Date, word).is_some(), "{word}");
        }
    }

    #[test]
    fn words_that_depend_on_the_clock_are_accepted() {
        let today = encode(PgTemporal::Date, "today").unwrap();
        assert!(today > encode(PgTemporal::Date, "2026-01-01").unwrap());
        assert_eq!(encode(PgTemporal::Date, "tomorrow").unwrap(), today + 1);
        assert_eq!(encode(PgTemporal::Date, "yesterday").unwrap(), today - 1);
        let now = encode(PgTemporal::Timestamp, "now").unwrap();
        assert_eq!(now.div_euclid(MICROSECONDS_PER_DAY), today);
        assert!(encode(PgTemporal::Time, "NOW").unwrap() < MICROSECONDS_PER_DAY);
    }

    #[test]
    fn invalid_input_is_refused() {
        for (kind, input, expected) in [
            (
                PgTemporal::Timestamp,
                "not a timestamp",
                "invalid input syntax for type timestamp",
            ),
            (
                PgTemporal::Timestamp,
                "10:00:00",
                "invalid input syntax for type timestamp",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01 10",
                "invalid input syntax for type timestamp",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01 10:00:00.",
                "invalid input syntax",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01 10:00:00 junk",
                "invalid input syntax",
            ),
            (PgTemporal::Timestamp, "2024/01/01", "invalid input syntax"),
            (PgTemporal::Timestamp, "20240101", "invalid input syntax"),
            (PgTemporal::Timestamp, "1-2-3", "invalid input syntax"),
            (PgTemporal::Timestamptz, "12-01-01", "invalid input syntax"),
            (PgTemporal::Date, "1-01-01", "invalid input syntax"),
            (PgTemporal::Date, "24-01-15", "invalid input syntax"),
            (PgTemporal::Time, "99-12-31 10:00", "invalid input syntax"),
            (
                PgTemporal::Timestamp,
                "2024-02-30",
                "date/time field value out of range",
            ),
            (
                PgTemporal::Timestamp,
                "2023-02-29",
                "date/time field value out of range",
            ),
            (
                PgTemporal::Timestamp,
                "2024-13-01",
                "date/time field value out of range",
            ),
            (
                PgTemporal::Timestamp,
                "0000-01-01",
                "date/time field value out of range",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01 25:00",
                "date/time field value out of range",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01 24:00:01",
                "date/time field value out of range",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01 10:60",
                "date/time field value out of range",
            ),
            (
                PgTemporal::Timestamp,
                "2024-01-01 10:00+16",
                "date/time field value out of range",
            ),
            (
                PgTemporal::Timestamp,
                "10000-01-01",
                "timestamp out of range",
            ),
            (
                PgTemporal::Timestamp,
                "9999-12-31 24:00:00",
                "timestamp out of range",
            ),
            (
                PgTemporal::Timestamptz,
                "0001-01-01 00:00:00+01",
                "out of range",
            ),
            (
                PgTemporal::Timestamp,
                "infinity",
                "infinity is not supported",
            ),
            (PgTemporal::Date, "-infinity", "infinity is not supported"),
            (
                PgTemporal::Date,
                "10:00",
                "invalid input syntax for type date",
            ),
            (
                PgTemporal::Time,
                "2024-01-01",
                "invalid input syntax for type time",
            ),
            (
                PgTemporal::Time,
                "today",
                "invalid input syntax for type time",
            ),
            (
                PgTemporal::Time,
                "24:00:00.5",
                "date/time field value out of range",
            ),
            (
                PgTemporal::Date,
                "allballs",
                "invalid input syntax for type date",
            ),
        ] {
            let message = error(kind, input);
            assert!(message.contains(expected), "{kind:?} {input}: {message}");
        }
        for value in [
            Value::from_i64(20240101),
            Value::from_f64(1.5),
            Value::from_slice(b"x").unwrap(),
        ] {
            let message = exec_pg_temporal_encode(PgTemporal::Date, &value)
                .unwrap_err()
                .to_string();
            assert!(message.contains("is not text"), "{message}");
        }
    }

    #[test]
    fn decode_refuses_values_outside_the_stored_range() {
        for (kind, stored) in [
            (PgTemporal::Timestamp, FIRST_TIMESTAMP - 1),
            (PgTemporal::Timestamp, LAST_TIMESTAMP + 1),
            (PgTemporal::Date, LAST_DAY + 1),
            (PgTemporal::Time, -1),
            (PgTemporal::Time, MICROSECONDS_PER_DAY + 1),
        ] {
            assert!(exec_pg_temporal_decode(kind, &Value::from_i64(stored)).is_err());
        }
        assert!(exec_pg_temporal_decode(PgTemporal::Date, &Value::build_text("x")).is_err());
    }

    #[test]
    fn text_order_of_decoded_values_is_the_stored_order() {
        let mut rng = ChaCha8Rng::seed_from_u64(11);
        for kind in [PgTemporal::Timestamp, PgTemporal::Date, PgTemporal::Time] {
            let (first, last) = kind.stored_range();
            let mut stored: Vec<i64> = (0..5_000).map(|_| rng.random_range(first..=last)).collect();
            stored.extend([first, last, first + 1, last - 1, 0]);
            stored.sort_unstable();
            for pair in stored.windows(2) {
                let (low, high) = (decode(kind, pair[0]), decode(kind, pair[1]));
                assert_eq!(low.cmp(&high), pair[0].cmp(&pair[1]), "{low} {high}");
            }
            for value in stored {
                let text = decode(kind, value);
                assert_eq!(encode(kind, &text).unwrap(), value, "{text}");
                assert_eq!(parse_canonical_temporal(kind, &text), Some(value), "{text}");
            }
        }
    }

    #[test]
    fn civil_conversion_matches_for_every_day_in_range() {
        for days in FIRST_DAY..=LAST_DAY {
            let (year, month, day) = civil_from_unix_days(days + UNIX_DAYS_AT_2000_01_01);
            assert_eq!(days_since_2000(year, month, day), days);
            assert!(day <= days_in_month(year, month));
        }
    }

    fn numeric(value: Value, precision: i64, scale: i64) -> Result<i64> {
        match exec_pg_numeric_encode(&value, &Value::from_i64(precision), &Value::from_i64(scale))?
        {
            Value::Numeric(Numeric::Integer(stored)) => Ok(stored),
            other => panic!("unexpected stored value {other:?}"),
        }
    }

    fn numeric_text(text: &str, precision: i64, scale: i64) -> Result<i64> {
        numeric(Value::build_text(text.to_string()), precision, scale)
    }

    #[test]
    fn numeric_rounds_half_away_from_zero() {
        for (input, expected) in [
            ("1.005", 101),
            ("1.004", 100),
            ("-1.005", -101),
            ("2.999", 300),
            ("-0.004", 0),
            ("12.5", 1250),
            (" 7 ", 700),
            ("1e2", 10000),
            ("0.000000000000000000001", 0),
        ] {
            assert_eq!(numeric_text(input, 10, 2).unwrap(), expected, "{input}");
        }
        assert_eq!(numeric(Value::from_i64(5), 10, 2).unwrap(), 500);
        assert_eq!(numeric(Value::from_f64(0.1), 10, 2).unwrap(), 10);
        assert_eq!(numeric(Value::from_f64(1.005), 10, 2).unwrap(), 101);
        assert_eq!(
            numeric_text("999999999999999999", 18, 0).unwrap(),
            999_999_999_999_999_999
        );
        assert_eq!(
            numeric_text("-0.999999999999999999", 18, 18).unwrap(),
            -999_999_999_999_999_999
        );
    }

    #[test]
    fn numeric_refuses_overflow_and_invalid_input() {
        for (input, precision, scale) in [
            ("100000000", 10, 2),
            ("99999999.995", 10, 2),
            ("1e30", 18, 0),
            ("1000000000000000000", 18, 0),
            ("1e1000000000", 18, 2),
        ] {
            let message = numeric_text(input, precision, scale)
                .unwrap_err()
                .to_string();
            assert!(
                message.contains("numeric field overflow"),
                "{input}: {message}"
            );
        }
        for input in ["abc", "1__000", "1000_", "_1000", "1_.5", "1._5"] {
            assert!(
                numeric_text(input, 10, 2)
                    .unwrap_err()
                    .to_string()
                    .contains("invalid input syntax for type numeric"),
                "{input}"
            );
        }
        assert_eq!(numeric_text(" 7 ", 10, 2).unwrap(), 700);
        assert_eq!(numeric_text("1_000.5_0", 10, 2).unwrap(), 100_050);
        assert!(numeric(Value::from_slice(b"x").unwrap(), 10, 2).is_err());
        assert!(numeric_text("1", 19, 2).is_err());
        assert!(numeric_text("1", 10, 11).is_err());
        assert_eq!(
            exec_pg_numeric_encode(&Value::Null, &Value::from_i64(10), &Value::from_i64(2))
                .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn numeric_decode_refuses_a_scale_out_of_range() {
        for scale in [-1, 19, 25] {
            let message = exec_pg_numeric_decode(&Value::from_i64(5), &Value::from_i64(scale))
                .unwrap_err()
                .to_string();
            assert!(
                message.contains(&format!(
                    "pg_numeric needs a scale from 0 to 18, got {scale}"
                )),
                "{message}"
            );
        }
        assert!(exec_pg_numeric_decode(&Value::from_i64(5), &Value::from_f64(2.5)).is_err());
        assert!(matches!(
            exec_pg_numeric_decode(&Value::build_text("5"), &Value::from_i64(2)),
            Err(LimboError::Corrupt(_))
        ));
    }

    #[test]
    fn numeric_decode_keeps_the_scale() {
        for (stored, scale, expected) in [
            (1250, 2, "12.50"),
            (-5, 2, "-0.05"),
            (0, 2, "0.00"),
            (7, 0, "7"),
            (-999_999_999_999_999_999, 18, "-0.999999999999999999"),
        ] {
            assert_eq!(format_scaled_integer(stored, scale), expected);
        }
    }

    #[test]
    fn numeric_text_round_trips() {
        let mut rng = ChaCha8Rng::seed_from_u64(3);
        for _ in 0..10_000 {
            let scale = rng.random_range(0..=18);
            let stored = rng.random_range(-999_999_999_999_999_999i64..=999_999_999_999_999_999);
            let text = format_scaled_integer(stored, scale);
            assert_eq!(numeric_text(&text, 18, scale).unwrap(), stored, "{text}");
        }
    }
}
