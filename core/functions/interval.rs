//! PostgreSQL's `interval` type: a number of months, days and microseconds.
//! The three are kept apart because a month has no fixed number of days:
//! `'1 mon'` and `'30 days'` are different values that compare equal.
//!
//! An interval is stored as 16 bytes in the layout of PostgreSQL's
//! `interval_send`, read from text as `interval_in` reads two of its forms,
//! and printed as `interval_out` prints it with `IntervalStyle` set to
//! `postgres`.

use std::fmt;

use crate::types::Value;
use crate::{LimboError, Result};

const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;
/// The length of a month when intervals are compared.
const DAYS_PER_MONTH: i32 = 30;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Interval {
    months: i32,
    days: i32,
    micros: i64,
}

/// `interval_encode(value)`, the ENCODE of the built-in type `interval`: text
/// is read as an interval and stored as 16 bytes. Other values are an error,
/// as PostgreSQL has no conversion from numbers or bytes to intervals.
pub fn exec_interval_encode(value: &Value) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Text(text) => Ok(Value::from_blob(
            Interval::parse(text.as_str())?.to_blob().to_vec(),
        )),
        Value::Numeric(_) | Value::Blob(_) => Err(LimboError::InvalidDate(
            "only text can be converted to interval".to_string(),
        )),
    }
}

/// `interval_decode(value)`, the DECODE of `interval`: the stored 16 bytes
/// printed as PostgreSQL prints the interval.
pub fn exec_interval_decode(value: &Value) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Blob(blob) => match Interval::from_blob(blob) {
            Some(interval) => {
                let text = interval.to_string();
                // Comparisons read a register's interval back from this text.
                debug_assert!(
                    matches!(Interval::parse(&text), Ok(read) if read == interval),
                    "{text:?} does not read back as {interval:?}"
                );
                Ok(Value::build_text(text))
            }
            None => Err(not_a_stored_interval()),
        },
        Value::Numeric(_) | Value::Text(_) => Err(not_a_stored_interval()),
    }
}

impl Interval {
    fn new(months: i32, days: i32, micros: i64) -> Result<Self> {
        if !Self::is_in_range(months, days, micros) {
            return Err(LimboError::NotSupported(format!(
                "the interval of {months} months, {days} days and {micros} microseconds"
            )));
        }
        Ok(Self {
            months,
            days,
            micros,
        })
    }

    /// `interval_send`'s layout: microseconds, days, months, big-endian.
    pub fn to_blob(self) -> [u8; 16] {
        let mut blob = [0; 16];
        blob[..8].copy_from_slice(&self.micros.to_be_bytes());
        blob[8..12].copy_from_slice(&self.days.to_be_bytes());
        blob[12..].copy_from_slice(&self.months.to_be_bytes());
        blob
    }

    /// The interval stored in `blob`, or None if `blob` is not a stored
    /// interval: not 16 bytes, or a value [Interval::new] refuses.
    pub fn from_blob(blob: &[u8]) -> Option<Self> {
        let blob: &[u8; 16] = match blob.try_into() {
            Ok(blob) => blob,
            Err(_) => return None,
        };
        let (micros, rest) = blob.split_at(8);
        let (days, months) = rest.split_at(4);
        let micros = i64::from_be_bytes(micros.try_into().expect("8 bytes"));
        let days = i32::from_be_bytes(days.try_into().expect("4 bytes"));
        let months = i32::from_be_bytes(months.try_into().expect("4 bytes"));
        Self::is_in_range(months, days, micros).then_some(Self {
            months,
            days,
            micros,
        })
    }

    /// False for two values PostgreSQL 17 reads and Turso does not: the
    /// value PostgreSQL stores for `infinity` (every field at its largest),
    /// as Turso has no infinite intervals, and a time of i64::MIN
    /// microseconds, which `-infinity` also has. PostgreSQL prints that time
    /// in a form it cannot read back, and a register holds an interval's
    /// printed form, which comparisons read back.
    fn is_in_range(months: i32, days: i32, micros: i64) -> bool {
        let is_infinity = months == i32::MAX && days == i32::MAX && micros == i64::MAX;
        !is_infinity && micros != i64::MIN
    }

    /// PostgreSQL's `interval_cmp_value`: the length in microseconds, with a
    /// month counted as 30 days and a day as 24 hours. Intervals compare and
    /// hash by it, so `'1 day'` equals `'24 hours'`.
    pub fn cmp_key(self) -> i128 {
        let days = i128::from(self.months) * i128::from(DAYS_PER_MONTH) + i128::from(self.days);
        days * i128::from(MICROS_PER_DAY) + i128::from(self.micros)
    }
}

fn out_of_range() -> LimboError {
    LimboError::DatetimeFieldOverflow("interval out of range".to_string())
}

fn not_a_stored_interval() -> LimboError {
    LimboError::InternalError("interval_decode expects a stored interval".to_string())
}

/// PostgreSQL's `EncodeInterval` for `IntervalStyle` `postgres`: each nonzero
/// part of `N years N mons N days`, then the time if it is nonzero or nothing
/// else was printed. A part after a negative one gets an explicit `+`, so
/// `-1 days +02:00:00` reads back as the same interval.
impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut wrote_any = false;
        let mut after_negative = false;
        for (value, unit) in [
            (self.months / 12, "year"),
            (self.months % 12, "mon"),
            (self.days, "day"),
        ] {
            if value == 0 {
                continue;
            }
            let separator = if wrote_any { " " } else { "" };
            let sign = if after_negative && value > 0 { "+" } else { "" };
            let plural = if value == 1 { "" } else { "s" };
            write!(f, "{separator}{sign}{value} {unit}{plural}")?;
            wrote_any = true;
            after_negative = value < 0;
        }
        if wrote_any && self.micros == 0 {
            return Ok(());
        }
        let separator = if wrote_any { " " } else { "" };
        let sign = if self.micros < 0 {
            "-"
        } else if after_negative {
            "+"
        } else {
            ""
        };
        let micros = self.micros.unsigned_abs();
        let (micros_per_hour, micros_per_minute, micros_per_second) = (
            MICROS_PER_HOUR.unsigned_abs(),
            MICROS_PER_MINUTE.unsigned_abs(),
            MICROS_PER_SECOND.unsigned_abs(),
        );
        write!(
            f,
            "{separator}{sign}{:02}:{:02}:{:02}",
            micros / micros_per_hour,
            micros % micros_per_hour / micros_per_minute,
            micros % micros_per_minute / micros_per_second,
        )?;
        let fraction = micros % micros_per_second;
        if fraction != 0 {
            write!(f, "{}", format!(".{fraction:06}").trim_end_matches('0'))?;
        }
        Ok(())
    }
}

/// The parts of an interval that a unit of the input fills, as the bits of
/// PostgreSQL's `fmask`. Each part may be filled once, so `'1 day 1 day'` is
/// refused rather than read as 2 days. A time `h:mm:ss` fills every part
/// below a day.
const YEAR: u16 = 1 << 0;
const MONTH: u16 = 1 << 1;
const WEEK: u16 = 1 << 2;
const DAY: u16 = 1 << 3;
const HOUR: u16 = 1 << 4;
const MINUTE: u16 = 1 << 5;
const SECOND: u16 = 1 << 6;
const MILLISECOND: u16 = 1 << 7;
const MICROSECOND: u16 = 1 << 8;
const TIME: u16 = HOUR | MINUTE | SECOND | MILLISECOND | MICROSECOND;

/// The field a unit of the input adds to. Years are kept apart from months
/// until the end, as PostgreSQL keeps them, because the two overflows are
/// different errors: in `'2147483648 months'` one number is past an i32
/// (22015); in `'178956971 years'` the number fits but the months do not
/// (22008).
#[derive(Clone, Copy)]
enum Field {
    Years,
    Months,
    Days,
    Micros,
}

impl Interval {
    /// Reads `input` as PostgreSQL's `interval_in` does, for two of its forms,
    /// in any letter case:
    /// - the form `interval_out` prints, such as
    ///   `1 year 2 mons -3 days +04:05:06.789`;
    /// - `[+-]N unit` with the full name of a unit, such as `2 weeks 3 hours`.
    ///
    /// A number with no unit, as the last word, is seconds. Any other input is
    /// refused as not supported, also input that PostgreSQL rejects.
    pub fn parse(input: &str) -> Result<Self> {
        let mut parser = Parser {
            input,
            years: 0,
            months: 0,
            days: 0,
            micros: 0,
            filled: 0,
        };
        let mut words = input.split_ascii_whitespace();
        while let Some(word) = words.next() {
            if word.contains(':') {
                parser.add_time(word)?;
                continue;
            }
            let (negative, digits) = split_sign(word);
            if !is_digits(digits) {
                return Err(parser.not_supported());
            }
            let count = read_digits(digits).ok_or_else(|| parser.field_overflow())?;
            let count = if negative { -count } else { count };
            let (part, field, per_unit) = match words.next() {
                Some(unit) => read_unit(unit).ok_or_else(|| parser.not_supported())?,
                None => (SECOND, Field::Micros, MICROS_PER_SECOND),
            };
            parser.fill(part)?;
            parser.add(field, count, per_unit)?;
        }
        if parser.filled == 0 {
            return Err(parser.not_supported());
        }
        parser.finish()
    }
}

/// The part a unit fills, the field it adds to, and how many of that field's
/// units one of it is.
fn read_unit(word: &str) -> Option<(u16, Field, i64)> {
    Some(match word.to_ascii_lowercase().as_str() {
        "year" | "years" => (YEAR, Field::Years, 1),
        "mon" | "mons" | "month" | "months" => (MONTH, Field::Months, 1),
        "week" | "weeks" => (WEEK, Field::Days, 7),
        "day" | "days" => (DAY, Field::Days, 1),
        "hour" | "hours" => (HOUR, Field::Micros, MICROS_PER_HOUR),
        "minute" | "minutes" => (MINUTE, Field::Micros, MICROS_PER_MINUTE),
        "second" | "seconds" => (SECOND, Field::Micros, MICROS_PER_SECOND),
        "millisecond" | "milliseconds" => (MILLISECOND, Field::Micros, 1000),
        "microsecond" | "microseconds" => (MICROSECOND, Field::Micros, 1),
        _ => return None,
    })
}

/// The interval read so far.
struct Parser<'a> {
    input: &'a str,
    years: i32,
    months: i32,
    days: i32,
    micros: i64,
    filled: u16,
}

impl Parser<'_> {
    fn not_supported(&self) -> LimboError {
        LimboError::NotSupported(format!("interval input form \"{}\"", self.input))
    }

    fn field_overflow(&self) -> LimboError {
        LimboError::IntervalFieldOverflow(self.input.to_string())
    }

    fn fill(&mut self, parts: u16) -> Result<()> {
        if self.filled & parts != 0 {
            return Err(self.not_supported());
        }
        self.filled |= parts;
        Ok(())
    }

    fn add(&mut self, field: Field, count: i128, per_unit: i64) -> Result<()> {
        match field {
            Field::Years => self.years = self.sum(self.years, count, per_unit)?,
            Field::Months => self.months = self.sum(self.months, count, per_unit)?,
            Field::Days => self.days = self.sum(self.days, count, per_unit)?,
            Field::Micros => self.micros = self.sum(self.micros, count, per_unit)?,
        }
        Ok(())
    }

    /// `field + count * per_unit`, or an error if it does not fit the field.
    fn sum<T>(&self, field: T, count: i128, per_unit: i64) -> Result<T>
    where
        T: Into<i128> + TryFrom<i128>,
    {
        let total = count
            .checked_mul(per_unit.into())
            .and_then(|value| value.checked_add(field.into()));
        match total.map(T::try_from) {
            Some(Ok(total)) => Ok(total),
            Some(Err(_)) | None => Err(self.field_overflow()),
        }
    }

    /// `[+-]h:mm:ss[.ffffff]`, as `interval_out` prints a time: hours of any
    /// size, minutes and seconds of two digits and below 60, and up to 6
    /// digits after the point.
    fn add_time(&mut self, word: &str) -> Result<()> {
        self.fill(TIME)?;
        let (negative, clock) = split_sign(word);
        let (clock, fraction) = match clock.split_once('.') {
            Some((clock, fraction)) => (clock, fraction),
            None => (clock, "0"),
        };
        let parts: Vec<&str> = clock.split(':').collect();
        let [hours, minutes, seconds] = parts[..] else {
            return Err(self.not_supported());
        };
        let is_time = is_digits(hours)
            && minutes.len() == 2
            && is_digits(minutes)
            && seconds.len() == 2
            && is_digits(seconds)
            && fraction.len() <= 6
            && is_digits(fraction);
        if !is_time {
            return Err(self.not_supported());
        }
        let small = |digits: &str| read_digits(digits).expect("at most 6 digits fit an i128");
        let (minutes, seconds) = (small(minutes), small(seconds));
        if minutes > 59 || seconds > 59 {
            return Err(self.not_supported());
        }
        let below_an_hour = minutes * i128::from(MICROS_PER_MINUTE)
            + seconds * i128::from(MICROS_PER_SECOND)
            + small(&format!("{fraction:0<6}"));
        let magnitude = read_digits(hours).and_then(|hours| {
            hours
                .checked_mul(i128::from(MICROS_PER_HOUR))?
                .checked_add(below_an_hour)
                .filter(|micros| *micros <= i128::from(i64::MAX))
        });
        let Some(magnitude) = magnitude else {
            // A negative time too long for an i64, such as `'-2562047789:00:00'`,
            // is a bad-format error in PostgreSQL, not an overflow.
            return Err(if negative {
                self.not_supported()
            } else {
                self.field_overflow()
            });
        };
        self.add(
            Field::Micros,
            if negative { -magnitude } else { magnitude },
            1,
        )
    }

    fn finish(self) -> Result<Interval> {
        let months = self
            .years
            .checked_mul(12)
            .and_then(|year_months| year_months.checked_add(self.months))
            .ok_or_else(out_of_range)?;
        Interval::new(months, self.days, self.micros)
    }
}

fn split_sign(word: &str) -> (bool, &str) {
    match word.as_bytes().first() {
        Some(b'-') => (true, &word[1..]),
        Some(b'+') => (false, &word[1..]),
        Some(_) | None => (false, word),
    }
}

fn is_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

/// ASCII digits as a number, or None when it does not fit an i128.
fn read_digits(digits: &str) -> Option<i128> {
    digits.bytes().try_fold(0i128, |value, digit| {
        value.checked_mul(10)?.checked_add(i128::from(digit - b'0'))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(input: &str) -> Interval {
        Interval::parse(input).unwrap_or_else(|error| panic!("{input:?}: {error}"))
    }

    fn parse_error(input: &str) -> &'static str {
        match Interval::parse(input) {
            Ok(interval) => panic!("{input:?} read as {interval}"),
            Err(LimboError::DatetimeFieldOverflow(_)) => "22008",
            Err(LimboError::IntervalFieldOverflow(_)) => "22015",
            Err(LimboError::NotSupported(_)) => "0A000",
            Err(other) => panic!("{input:?}: unexpected error: {other}"),
        }
    }

    /// PostgreSQL 17 prints each input as its second column.
    #[test]
    fn reads_and_prints_as_postgres() {
        for (input, printed) in [
            ("1 day", "1 day"),
            ("1 DAYS", "1 day"),
            ("90 minutes", "01:30:00"),
            ("2 weeks 3 hours", "14 days 03:00:00"),
            ("1 Week 1 day", "8 days"),
            ("1 year 2 months", "1 year 2 mons"),
            ("1 month 1 minute", "1 mon 00:01:00"),
            ("1 years 1 hours", "1 year 01:00:00"),
            (
                "1 year 2 mons 3 days 04:05:06.789",
                "1 year 2 mons 3 days 04:05:06.789",
            ),
            ("1 microseconds 1 milliseconds", "00:00:00.001001"),
            ("0", "00:00:00"),
            ("10", "00:00:10"),
            ("-0", "00:00:00"),
            ("+0", "00:00:00"),
            ("1 day  2 hours", "1 day 02:00:00"),
            ("\t1 day", "1 day"),
            ("1 days 1", "1 day 00:00:01"),
            ("-1 days -1", "-1 days -00:00:01"),
            ("1 hour -30 minutes", "00:30:00"),
            ("+01:30:00.5", "01:30:00.5"),
            ("00:00:00.5", "00:00:00.5"),
            ("-00:00:01", "-00:00:01"),
            ("00:00:00.000000", "00:00:00"),
            ("36:00:00", "36:00:00"),
            ("1 second 1 millisecond", "00:00:01.001"),
            ("1 millisecond 1", "00:00:01.001"),
            ("300000000 weeks", "2100000000 days"),
            ("153722867280 minutes", "2562047788:00:00"),
            ("2562047788 hours", "2562047788:00:00"),
            ("9223372036854 seconds", "2562047788:00:54"),
            ("-2147483648 months", "-178956970 years -8 mons"),
            ("1 year -1 mon", "11 mons"),
            ("-1 years +1 mons", "-11 mons"),
            ("1 day -1 microsecond", "1 day -00:00:00.000001"),
            ("-1 mons -1 days 1 hour", "-1 mons -1 days +01:00:00"),
            ("-1 days +02:00:00", "-1 days +02:00:00"),
        ] {
            assert_eq!(read(input).to_string(), printed, "{input:?}");
            assert_eq!(read(printed), read(input), "{printed:?} reads back");
        }
        // PostgreSQL reads the smallest time but cannot read its printed form
        // `-2562047788:00:54.775808` back, so Turso refuses both. PostgreSQL
        // reads the largest value of every field as `infinity`.
        assert_eq!(parse_error("-9223372036854775808 microseconds"), "0A000");
        assert_eq!(parse_error("-2562047788:00:54.775808"), "0A000");
        for infinity in [
            "2147483647 months 2147483647 days 9223372036854775807 microseconds",
            "178956970 years 7 mons 2147483647 days 2562047788:00:54.775807",
        ] {
            assert_eq!(parse_error(infinity), "0A000", "{infinity:?}");
        }
        let next_smallest = read("-9223372036854775807 microseconds");
        assert_eq!(next_smallest.to_string(), "-2562047788:00:54.775807");
    }

    /// PostgreSQL 17 gives the same SQLSTATE for each of these.
    #[test]
    fn refuses_values_out_of_range_as_postgres() {
        for (input, code) in [
            ("2147483648 years", "22015"),
            ("2147483648 months", "22015"),
            ("9223372036854775808 microseconds", "22015"),
            ("3000000000 hours", "22015"),
            ("2147483647 days 1 week", "22015"),
            ("153722867281 minutes", "22015"),
            ("2562047789 hours", "22015"),
            ("9223372036855 seconds", "22015"),
            ("-2147483649 days", "22015"),
            ("2562047788:00:54.775808", "22015"),
            ("9999999999999:00:00", "22015"),
            ("178956971 years", "22008"),
        ] {
            assert_eq!(parse_error(input), code, "{input:?}");
        }
    }

    /// PostgreSQL reads some of these and rejects the others.
    #[test]
    fn refuses_other_input_as_not_supported() {
        for input in [
            // PostgreSQL rejects these.
            "",
            " ",
            "abc",
            "1e2",
            "1 day 1 day",
            "1 2",
            "10 1 day",
            "00:00:01 1 millisecond",
            "1 microsecond 01:00:00",
            "01:30:00:00",
            "01:30:05.5.5",
            "01:60:00",
            "01:59:61",
            "-9999999999999:00:00",
            "3 fortnights",
            // PostgreSQL reads these.
            "1.5 days",
            "1.5 seconds",
            "1d",
            "1day",
            "1 day ago",
            "1 hr",
            "2 mins",
            "1 decade",
            "1 century",
            "1 millennium",
            "1 microsecondsxyz",
            "1 day, 2 hours",
            "1h30m",
            "P1DT2H",
            "@ 1 day",
            "1-2",
            "1 2:03:04",
            "infinity",
            "1:30",
            "01:30",
            "1:3:00",
            "01:02:03.",
            "00:00:00.1234567",
            "01:59:60",
            "- 1 day",
        ] {
            assert_eq!(parse_error(input), "0A000", "{input:?}");
        }
    }

    #[test]
    fn stores_postgres_binary_layout() {
        let interval = read("1 year 2 mons 3 days 04:05:06.000007");
        let blob = interval.to_blob();
        assert_eq!(
            blob,
            [
                0, 0, 0, 3, 0x6c, 0x8b, 0xc0, 0x87, // 14706000007 microseconds
                0, 0, 0, 3, // 3 days
                0, 0, 0, 14, // 14 months
            ]
        );
        assert_eq!(Interval::from_blob(&blob), Some(interval));
        assert_eq!(Interval::from_blob(&blob[..15]), None);
        // `infinity` and `-infinity` as PostgreSQL 17 stores them.
        let mut infinity = [0; 16];
        infinity[..8].copy_from_slice(&i64::MAX.to_be_bytes());
        infinity[8..12].copy_from_slice(&i32::MAX.to_be_bytes());
        infinity[12..].copy_from_slice(&i32::MAX.to_be_bytes());
        assert_eq!(Interval::from_blob(&infinity), None);
        let mut minus_infinity = [0; 16];
        minus_infinity[..8].copy_from_slice(&i64::MIN.to_be_bytes());
        minus_infinity[8..12].copy_from_slice(&i32::MIN.to_be_bytes());
        minus_infinity[12..].copy_from_slice(&i32::MIN.to_be_bytes());
        assert_eq!(Interval::from_blob(&minus_infinity), None);
        let mut smallest_time = [0; 16];
        smallest_time[..8].copy_from_slice(&i64::MIN.to_be_bytes());
        assert_eq!(Interval::from_blob(&smallest_time), None);
    }

    /// Comparisons read a register's interval back from its printed form, so
    /// every value must print as text that reads back to the same value.
    #[test]
    fn every_value_reads_back_from_its_printed_form() {
        use rand::{Rng, SeedableRng};
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(11314);
        let months = [i32::MIN, i32::MIN + 1, -13, -12, -1, 0, 1, 11, 12, i32::MAX];
        let days = [i32::MIN, -1, 0, 1, i32::MAX];
        let micros = [
            i64::MIN,
            i64::MIN + 1,
            -86_400_000_001,
            -1,
            0,
            1,
            999_999,
            i64::MAX,
        ];
        let edges = months
            .iter()
            .flat_map(|&m| days.iter().map(move |&d| (m, d)))
            .flat_map(|(m, d)| micros.iter().map(move |&us| (m, d, us)));
        let random = (0..20_000).map(|_| (rng.random(), rng.random(), rng.random()));
        for (months, days, micros) in edges.chain(random) {
            if !Interval::is_in_range(months, days, micros) {
                continue;
            }
            let interval = Interval {
                months,
                days,
                micros,
            };
            let text = interval.to_string();
            assert_eq!(read(&text), interval, "{text:?}");
        }
    }

    #[test]
    fn compares_by_length_with_30_day_months() {
        assert_eq!(read("1 day").cmp_key(), read("24 hours").cmp_key());
        assert_eq!(read("1 mon").cmp_key(), read("30 days").cmp_key());
        assert!(read("1 mon").cmp_key() < read("31 days").cmp_key());
        assert!(read("-1 day").cmp_key() < read("1 microsecond").cmp_key());
        assert_ne!(read("1 day"), read("24 hours"));
    }
}
