use crate::types::AsValueRef;
use crate::types::Value;
use crate::LimboError::InvalidModifier;
use crate::{Result, ValueRef};
use chrono::{
    DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, TimeZone, Timelike, Utc,
};

/// Execution of date/time/datetime functions
#[inline(always)]
pub fn exec_date<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime(values, DateTimeOutput::Date)
}

#[inline(always)]
pub fn exec_time<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime(values, DateTimeOutput::Time)
}

#[inline(always)]
pub fn exec_datetime_full<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime(values, DateTimeOutput::DateTime)
}

#[inline(always)]
pub fn exec_strftime<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut values = values.into_iter();
    if values.len() == 0 {
        return Value::Null;
    }

    let value = values.next().unwrap();
    let value = value.as_value_ref();
    let format_str = if matches!(
        value,
        ValueRef::Text(_) | ValueRef::Integer(_) | ValueRef::Float(_)
    ) {
        format!("{value}")
    } else {
        return Value::Null;
    };

    exec_datetime(values, DateTimeOutput::StrfTime(format_str))
}

pub fn exec_julianday<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime(values, DateTimeOutput::JulianDay)
}

pub fn exec_unixepoch<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    exec_datetime(values, DateTimeOutput::UnixEpoch)
}

enum DateTimeOutput {
    Date,
    Time,
    DateTime,
    // Holds the format string
    StrfTime(String),
    JulianDay,
    UnixEpoch,
}

#[derive(Debug, Clone, Copy)]
struct ParsedDateTime {
    val: NaiveDateTime,
    is_utc: bool,
    overflow: i64,
}

impl ParsedDateTime {
    fn new(val: NaiveDateTime, is_utc: bool) -> Self {
        Self {
            val,
            is_utc,
            overflow: 0,
        }
    }
}

fn exec_datetime<I, E, V>(values: I, output_type: DateTimeOutput) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let values = values.into_iter();
    if values.len() == 0 {
        let now = chrono::Local::now().to_utc().naive_utc();
        return format_dt(now, output_type, false);
    }

    let mut values = values.peekable();
    let first = values.peek().unwrap();
    let initial_numeric = match first.as_value_ref() {
        ValueRef::Integer(i) => Some(i as f64),
        ValueRef::Float(f) => Some(f),
        ValueRef::Text(s) => s.as_str().parse::<f64>().ok(),
        _ => None,
    };

    if let Some(parsed) = parse_naive_date_time(first) {
        let mut dt = parsed.val;
        return modify_dt(
            &mut dt,
            values.skip(1),
            output_type,
            initial_numeric,
            true,
            parsed.is_utc,
            parsed.overflow,
        );
    }

    let first_arg_is_modifier = if let ValueRef::Text(s) = first.as_value_ref() {
        let s = s.as_str();
        s.eq_ignore_ascii_case("subsec") || s.eq_ignore_ascii_case("subsecond")
    } else {
        false
    };

    if first_arg_is_modifier {
        let mut dt = chrono::Local::now().to_utc().naive_utc();
        return modify_dt(&mut dt, values, output_type, None, true, true, 0);
    }

    let mut dt = DateTime::from_timestamp(0, 0).unwrap().naive_utc();
    let is_valid = false;
    modify_dt(
        &mut dt,
        values.skip(1),
        output_type,
        initial_numeric,
        is_valid,
        true,
        0,
    )
}

fn modify_dt<I, E, V>(
    dt: &mut NaiveDateTime,
    mods: I,
    output_type: DateTimeOutput,
    initial_numeric: Option<f64>,
    mut is_valid: bool,
    mut is_utc: bool,
    initial_overflow: i64,
) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mods = mods.into_iter();
    let mut n_floor: i64 = initial_overflow;
    let mut subsec_requested = false;
    let mut is_first_modifier = true;

    for modifier in mods {
        if let ValueRef::Text(ref text_rc) = modifier.as_value_ref() {
            if is_first_modifier && initial_numeric.is_some() {
                let m = parse_modifier(text_rc.as_str());
                if matches!(m, Ok(Modifier::UnixEpoch)) {
                    is_valid = true;
                } else if matches!(m, Ok(Modifier::Auto)) {
                    if let Some(val) = initial_numeric {
                        if !is_julian_day_value(val) {
                            is_valid = true;
                        }
                    }
                }
            }

            let parsed = parse_modifier(text_rc.as_str());
            if !matches!(parsed, Ok(Modifier::Floor) | Ok(Modifier::Ceiling)) {
                n_floor = 0;
            }

            match apply_modifier(
                dt,
                text_rc.as_str(),
                &mut n_floor,
                initial_numeric,
                is_first_modifier,
                &mut is_utc,
            ) {
                Ok(true) => subsec_requested = true,
                Ok(false) => {}
                Err(_) => return Value::Null,
            }

            if matches!(parsed, Ok(Modifier::Floor) | Ok(Modifier::Ceiling)) {
                n_floor = 0;
            }
            is_first_modifier = false;
        } else {
            return Value::Null;
        }
    }

    if !is_valid {
        return Value::Null;
    }

    if is_leap_second(dt) || dt.year() >= 10000 {
        return Value::Null;
    }
    let final_jd = to_julian_day_exact(dt);
    if !is_julian_day_value(final_jd) {
        return Value::Null;
    }
    format_dt(*dt, output_type, subsec_requested)
}

fn format_dt(dt: NaiveDateTime, output_type: DateTimeOutput, subsec: bool) -> Value {
    match output_type {
        DateTimeOutput::Date => Value::from_text(dt.format("%Y-%m-%d").to_string()),
        DateTimeOutput::Time => {
            let t = if subsec {
                dt.format("%H:%M:%S%.3f").to_string()
            } else {
                dt.format("%H:%M:%S").to_string()
            };
            Value::from_text(t)
        }
        DateTimeOutput::DateTime => {
            let t = if subsec {
                dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
            } else {
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            };
            Value::from_text(t)
        }
        DateTimeOutput::StrfTime(format_str) => match strftime_format(&dt, &format_str) {
            Some(s) => Value::from_text(s),
            None => Value::Null,
        },
        DateTimeOutput::JulianDay => Value::Float(to_julian_day_exact(&dt)),
        DateTimeOutput::UnixEpoch => {
            if subsec {
                let seconds = dt.and_utc().timestamp() as f64;
                let nanos = dt.nanosecond() as f64;
                let val = seconds + (nanos / 1_000_000_000.0);
                Value::Float(val)
            } else {
                let seconds = dt.and_utc().timestamp();
                Value::Integer(seconds)
            }
        }
    }
}

// Not as fast as if the formatting was native to chrono, but a good enough
// for now, just to have the feature implemented
fn strftime_format(dt: &NaiveDateTime, format_str: &str) -> Option<String> {
    use crate::functions::strftime::CustomStrftimeItems;
    use std::fmt::Write;
    // Necessary to remove %f and %J that are exclusive formatters to sqlite
    // Chrono does not support them, so it is necessary to replace the modifiers manually

    // Sqlite uses 9 decimal places for julianday in strftime
    let julian = format!("{:.9}", to_julian_day_exact(dt));
    let julian_trimmed = julian
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string();
    let copy_format = format_str.replace("%J", &julian_trimmed);
    let items = CustomStrftimeItems::new(&copy_format);

    // The write! macro is used here as chrono's format can panic if the formatting string contains
    // unknown specifiers. By using a writer, we can catch the panic and handle the error
    let mut formatted = String::new();
    match write!(formatted, "{}", dt.format_with_items(items)) {
        Ok(_) => Some(formatted),
        Err(_) => None,
    }
}

fn apply_modifier(
    dt: &mut NaiveDateTime,
    modifier: &str,
    n_floor: &mut i64,
    initial_numeric: Option<f64>,
    is_first_modifier: bool,
    is_utc: &mut bool,
) -> Result<bool> {
    let parsed_modifier = parse_modifier(modifier)?;

    match parsed_modifier {
        Modifier::Days(days) => {
            // Convert fractional days to milliseconds for precision
            let ms = (days * 86_400_000.0).round() as i64;
            *dt += TimeDelta::try_milliseconds(ms).unwrap_or(TimeDelta::zero());
        }
        Modifier::Hours(hours) => {
            let ms = (hours * 3_600_000.0).round() as i64;
            *dt += TimeDelta::try_milliseconds(ms).unwrap_or(TimeDelta::zero());
        }
        Modifier::Minutes(minutes) => {
            let ms = (minutes * 60_000.0).round() as i64;
            *dt += TimeDelta::try_milliseconds(ms).unwrap_or(TimeDelta::zero());
        }
        Modifier::Seconds(seconds) => {
            let ms = (seconds * 1_000.0).round() as i64;
            *dt += TimeDelta::try_milliseconds(ms).unwrap_or(TimeDelta::zero());
        }
        Modifier::Months(m) => {
            // SQLite splits float into integer months and fractional seconds
            // 1 Month = 30.0 days = 2592000.0 seconds
            let m_int = m as i32;
            let years = m_int / 12;
            let leftover = m_int % 12;
            add_years_and_months(dt, years, leftover, n_floor)?;

            let frac = m - m_int as f64;
            if frac.abs() > f64::EPSILON {
                let extra_ms = (frac * 2592000.0 * 1000.0).round() as i64;
                *dt += TimeDelta::milliseconds(extra_ms);
            }
        }
        Modifier::Years(y) => {
            // 1 Year = 365.0 days = 31536000.0 seconds
            let y_int = y as i32;
            add_years_and_months(dt, y_int, 0, n_floor)?;

            let frac = y - y_int as f64;
            if frac.abs() > f64::EPSILON {
                let extra_ms = (frac * 31536000.0 * 1000.0).round() as i64;
                *dt += TimeDelta::milliseconds(extra_ms);
            }
        }
        Modifier::DateTimeOffset {
            years,
            months,
            days,
            seconds,
        } => {
            add_years_and_months(dt, years, months, n_floor)?;
            *dt += TimeDelta::days(days as i64);
            let ms = (seconds * 1000.0).round() as i64;
            *dt += TimeDelta::milliseconds(ms);
        }
        Modifier::TimeOffset(offset) => *dt += offset,
        Modifier::DateOffset {
            years,
            months,
            days,
        } => {
            add_years_and_months(dt, years, months, n_floor)?;
            *dt += TimeDelta::days(days as i64);
        }
        Modifier::Floor => {
            if *n_floor > 0 {
                *dt -= TimeDelta::days(*n_floor);
            }
        }
        Modifier::Ceiling => {
            *n_floor = 0;
        }
        Modifier::StartOfMonth => {
            *dt = NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
        }
        Modifier::StartOfYear => {
            *dt = NaiveDate::from_ymd_opt(dt.year(), 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
        }
        Modifier::StartOfDay => {
            *dt = dt.date().and_hms_opt(0, 0, 0).unwrap();
        }
        Modifier::Weekday(day) => {
            let current_day = dt.weekday().num_days_from_sunday();
            let days_to_add = (day + 7 - current_day) % 7;
            *dt += TimeDelta::days(days_to_add as i64);
        }
        Modifier::UnixEpoch => {
            if !is_first_modifier {
                return Err(InvalidModifier(
                    "unixepoch must be the first modifier".into(),
                ));
            }
            *is_utc = true;
            let val = initial_numeric
                .ok_or_else(|| InvalidModifier("unixepoch requires numeric input".into()))?;
            let total_ms = (val * 1000.0).round() as i64;
            let base = DateTime::from_timestamp(0, 0)
                .ok_or_else(|| InvalidModifier("Internal error".into()))?;

            *dt = base
                .checked_add_signed(TimeDelta::milliseconds(total_ms))
                .ok_or_else(|| InvalidModifier("Date overflow".into()))?
                .naive_utc();
        }
        Modifier::JulianDay => {
            if !is_first_modifier {
                return Err(InvalidModifier("julianday must be first".into()));
            }
            *is_utc = true;
            if let Some(val) = initial_numeric {
                if let Ok(new_dt) = julian_day_to_datetime(val) {
                    *dt = new_dt;
                }
            } else {
                return Err(InvalidModifier("julianday requires numeric".into()));
            }
        }
        Modifier::Auto => {
            if is_first_modifier {
                *is_utc = true;
                if let Some(val) = initial_numeric {
                    if is_julian_day_value(val) {
                        *dt = julian_day_to_datetime(val)?;
                    } else {
                        let total_ms = (val * 1000.0).round() as i64;
                        let base = DateTime::from_timestamp(0, 0)
                            .ok_or_else(|| InvalidModifier("Internal error".into()))?;
                        *dt = base
                            .checked_add_signed(TimeDelta::milliseconds(total_ms))
                            .ok_or_else(|| InvalidModifier("Date overflow".into()))?
                            .naive_utc();
                    }
                }
            }
        }
        Modifier::Localtime => {
            let utc_dt = DateTime::<Utc>::from_naive_utc_and_offset(*dt, Utc);
            *dt = utc_dt.with_timezone(&chrono::Local).naive_local();
            *is_utc = false;
        }
        Modifier::Utc => {
            // "utc" modifier converts Local -> UTC.
            // If already UTC, it is a no-op.
            if !*is_utc {
                match chrono::Local.from_local_datetime(dt) {
                    chrono::LocalResult::Single(local_dt) => {
                        *dt = local_dt.with_timezone(&Utc).naive_utc();
                    }
                    chrono::LocalResult::Ambiguous(earliest, _latest) => {
                        *dt = earliest.with_timezone(&Utc).naive_utc();
                    }
                    chrono::LocalResult::None => {
                        return Err(InvalidModifier(
                            "Time does not exist in local timezone".into(),
                        ));
                    }
                }
                *is_utc = true;
            }
        }
        Modifier::Subsec => {
            return Ok(true);
        }
    }
    Ok(false)
}

fn is_julian_day_value(value: f64) -> bool {
    (0.0..5373484.5).contains(&value)
}

fn add_years_and_months(
    dt: &mut NaiveDateTime,
    years: i32,
    months: i32,
    n_floor: &mut i64,
) -> Result<()> {
    // 1. Preserve the original time and day
    let (original_y, original_m, original_d) = (dt.year(), dt.month(), dt.day());
    let (hh, mm, ss) = (dt.hour(), dt.minute(), dt.second());
    let nanos = dt.nanosecond();

    // 2. Calculate total months to add (years * 12 + months)
    // We convert everything to a 0-indexed month count from year 0 to do the math cleanly
    let total_months_current = (original_y as i64 * 12) + (original_m as i64 - 1);
    let months_to_add = (years as i64 * 12) + months as i64;
    let target_total_months = total_months_current + months_to_add;

    // 3. Convert back to Year and Month
    // div_euclid and rem_euclid handle negative numbers correctly (e.g. subtracting months across year boundary)
    let target_year = target_total_months.div_euclid(12) as i32;
    let target_month = (target_total_months.rem_euclid(12) + 1) as u32;

    // 4. Determine the last day of the target month
    let max_days_in_target = last_day_in_month(target_year, target_month);

    // 5. Calculate the new day and overflow (n_floor)
    // SQLite "Ceiling" behavior (default): If original day is 31, and target month has 28 days,
    // we go to day 28 + (31-28) = March 3rd.
    let (final_day, overflow_days) = if original_d > max_days_in_target {
        (max_days_in_target, (original_d - max_days_in_target) as i64)
    } else {
        (original_d, 0)
    };

    // 6. Construct the base date at the valid end of the target month
    let base_date = NaiveDate::from_ymd_opt(target_year, target_month, final_day)
        .ok_or_else(|| crate::LimboError::InternalError("Invalid calculated date".to_string()))?
        .and_hms_nano_opt(hh, mm, ss, nanos)
        .ok_or_else(|| crate::LimboError::InternalError("Invalid calculated time".to_string()))?;

    // 7. Apply the overflow
    // If we had overflow (e.g. Feb 31st -> Feb 29 + 2 days), we add it here.
    *dt = base_date + TimeDelta::days(overflow_days);

    // 8. Store overflow for potential "Floor" modifier usage later
    *n_floor += overflow_days;

    Ok(())
}

#[inline(always)]
fn last_day_in_month(year: i32, month: u32) -> u32 {
    for day in (28..=31).rev() {
        if NaiveDate::from_ymd_opt(year, month, day).is_some() {
            return day;
        }
    }
    28
}

fn get_date_time_from_time_value_string(value: &str) -> Option<ParsedDateTime> {
    let value = value.trim();

    if value.eq_ignore_ascii_case("now") {
        return Some(ParsedDateTime::new(
            chrono::Local::now().to_utc().naive_utc(),
            true,
        ));
    }

    if let Ok(julian_day) = value.parse::<f64>() {
        return get_date_time_from_time_value_float(julian_day);
    }

    if value.starts_with('+') {
        return None;
    }
    if value.contains(":60") {
        return None;
    }

    let datetime_formats: [&str; 9] = [
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%H:%M",
        "%H:%M:%S",
        "%H:%M:%S%.f",
    ];

    for format in &datetime_formats {
        let result = if format.starts_with("%H") {
            parse_datetime_with_optional_tz(
                &format!("2000-01-01 {value}"),
                &format!("%Y-%m-%d {format}"),
            )
        } else {
            parse_datetime_with_optional_tz(value, format)
        };

        if let Some(mut parsed) = result {
            let dt = parsed.val;
            let nanos = dt.nanosecond();
            let ms = (nanos + 500_000) / 1_000_000;
            let rounded_dt = if ms >= 1000 {
                dt.with_nanosecond(999_000_000).unwrap()
            } else {
                dt.with_nanosecond(ms * 1_000_000).unwrap()
            };
            parsed.val = rounded_dt;
            return Some(parsed);
        }
    }

    if let Some(parsed) = parse_permissive_datetime(value) {
        return Some(parsed);
    }

    None
}

fn parse_datetime_with_optional_tz(value: &str, format: &str) -> Option<ParsedDateTime> {
    let is_invalid_suffix = |s: &str| -> bool {
        let b = s.as_bytes();
        let len = b.len();

        // Reject +HHMM or -HHMM
        if len >= 5 {
            let c = b[len - 5];
            if (c == b'+' || c == b'-') && b[len - 4..].iter().all(|x| x.is_ascii_digit()) {
                return true;
            }
        }
        false
    };

    if is_invalid_suffix(value) {
        return None;
    }

    // Case A: Format includes timezone specifier (e.g. %:z) -> Return UTC
    let with_tz_format = format.to_owned() + "%:z";
    if let Ok(dt) = DateTime::parse_from_str(value, &with_tz_format) {
        return Some(ParsedDateTime::new(
            dt.with_timezone(&Utc).naive_utc(),
            true,
        ));
    }

    // Case B: String manually ends in 'Z' (UTC) -> Return UTC
    if value.ends_with('Z') {
        let value_without_tz = &value[0..value.len() - 1];
        if let Ok(dt) = NaiveDateTime::parse_from_str(value_without_tz, format) {
            return Some(ParsedDateTime::new(dt, true));
        }
    }

    // Case C: No timezone info -> Assumed Local (Timezone Abstract, is_utc=false)
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, format) {
        return Some(ParsedDateTime::new(dt, false));
    }
    None
}

fn get_date_time_from_time_value_integer(value: i64) -> Option<ParsedDateTime> {
    i32::try_from(value).map_or_else(
        |_| None,
        |value| {
            if value.is_negative() || !is_julian_day_value(value as f64) {
                return None;
            }
            get_date_time_from_time_value_float(value as f64)
        },
    )
}

fn get_date_time_from_time_value_float(value: f64) -> Option<ParsedDateTime> {
    if value.is_infinite() || value.is_nan() || !is_julian_day_value(value) {
        return None;
    }
    julian_day_to_datetime(value)
        .ok()
        .map(|dt| ParsedDateTime::new(dt, true))
}

fn parse_naive_date_time(time_value: impl AsValueRef) -> Option<ParsedDateTime> {
    let time_value = time_value.as_value_ref();
    match time_value {
        ValueRef::Text(s) => get_date_time_from_time_value_string(s.as_str()),
        ValueRef::Integer(i) => get_date_time_from_time_value_integer(i),
        ValueRef::Float(f) => get_date_time_from_time_value_float(f),
        _ => None,
    }
}

fn strip_timezone_suffix(s: &str) -> &str {
    if s.is_empty() {
        return s;
    }

    // 1. Check for 'Z' (UTC)
    if s.ends_with('Z') || s.ends_with('z') {
        return &s[..s.len() - 1];
    }

    let is_valid_tz = |h_str: &str, m_str: &str| -> bool {
        if let (Ok(h), Ok(m)) = (h_str.parse::<u32>(), m_str.parse::<u32>()) {
            h <= 14 && m < 60
        } else {
            false
        }
    };

    // 2. Check for numeric offsets

    // Check +HH:MM or -HH:MM (length >= 6)
    if s.len() >= 6 {
        let idx = s.len() - 6;
        let c = s.chars().nth(idx).unwrap();
        if (c == '+' || c == '-') && s[idx + 3..].starts_with(':') {
            let h_part = &s[idx + 1..idx + 3];
            let m_part = &s[idx + 4..];

            // Check digits AND value range
            if h_part.chars().all(|x| x.is_ascii_digit())
                && m_part.chars().all(|x| x.is_ascii_digit())
                && is_valid_tz(h_part, m_part)
            {
                return &s[..idx];
            }
        }
    }

    // The parser will subsequently fail on this string, returning NULL.
    s
}

fn to_julian_day_exact(dt: &NaiveDateTime) -> f64 {
    let s = dt.second() as f64 + (dt.nanosecond() as f64 / 1_000_000_000.0);
    compute_julian_day(
        dt.year(),
        dt.month() as i32,
        dt.day() as i32,
        dt.hour() as i32,
        dt.minute() as i32,
        s,
    )
}
// Helper to compute JD from raw components (allows invalid dates like 2023-02-31)
// 's' is a double that includes subseconds (e.g., 12.345)
fn compute_julian_day(y: i32, mut m: i32, d: i32, h: i32, min: i32, s: f64) -> f64 {
    let mut y = y;
    if m <= 2 {
        y -= 1;
        m += 12;
    }

    let a = (y + 4800) / 100;
    let b = 38 - a + (a / 4);
    let x1 = 36525 * (y + 4716) / 100;
    let x2 = 306001 * (m + 1) / 10000;

    let jd_days = (x1 + x2 + d + b) as f64 - 1524.5;
    let mut i_jd = (jd_days * 86400000.0) as i64;

    let h_ms = h as i64 * 3600000;
    let m_ms = min as i64 * 60000;

    // SQLite Algorithm: (s * 1000 + 0.5) cast to int
    let s_ms = (s * 1000.0 + 0.5) as i64;

    i_jd += h_ms + m_ms + s_ms;

    i_jd as f64 / 86400000.0
}

/// SQLite allows invalid dates like '2023-02-31' (overflows), but STRICTLY enforces
/// format lengths (YYYY-MM-DD). Malformed lengths (e.g., 200-1-1) must return NULL.
/// It consumes any combination of 'T' or whitespace between Date and Time.
/// Whitespace preceding the timezone suffix is strictly trimmed.
fn parse_permissive_datetime(value: &str) -> Option<ParsedDateTime> {
    let value = value.trim();

    // 1. Parse Year
    let (rest_after_y, is_neg) = if let Some(stripped) = value.strip_prefix('-') {
        (stripped, true)
    } else {
        (value, false)
    };

    let y_sep_idx = rest_after_y.find('-')?;
    if y_sep_idx != 4 {
        return None;
    }

    let y_str = &rest_after_y[..y_sep_idx];
    if !y_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    let mut y = y_str.parse::<i32>().ok()?;
    if is_neg {
        y = -y;
    }

    let rest_after_y = &rest_after_y[y_sep_idx + 1..];

    // 2. Parse Month
    let m_sep_idx = rest_after_y.find('-')?;
    if m_sep_idx != 2 {
        return None;
    }

    let m_str = &rest_after_y[..m_sep_idx];
    if !m_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let m = m_str.parse::<i32>().ok()?;

    let rest_after_m = &rest_after_y[m_sep_idx + 1..];

    // 3. Parse Day
    // Day string ends at 'T', ' ', or end of string.
    let d_end_idx = rest_after_m.find(['T', ' ']).unwrap_or(rest_after_m.len());

    if d_end_idx != 2 {
        return None;
    }

    let d_str = &rest_after_m[..d_end_idx];
    if !d_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let d = d_str.parse::<i32>().ok()?;

    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }

    let max_days = last_day_in_month(y, m as u32) as i32;
    let overflow = if d > max_days {
        (d - max_days) as i64
    } else {
        0
    };

    // 4. Parse Time (Optional)
    let rest_time = &rest_after_m[d_end_idx..];

    // SQLite skips ANY amount of whitespace or 'T' characters here.
    let t_start = rest_time.trim_start_matches(|c: char| c == 'T' || c.is_whitespace());

    let (h, min, s) = if !t_start.is_empty() {
        // Strip timezone info (Z, +00:00, etc) before splitting.
        // NOTE: This might leave trailing spaces if the input was "12:00 Z" -> "12:00 "
        let t_clean = strip_timezone_suffix(t_start);
        let parts: Vec<&str> = t_clean.split(':').collect();

        if parts.len() < 2 {
            return None;
        }

        // Trim each part to remove any spaces left over from stripping the timezone
        let h_str = parts[0].trim();
        if h_str.len() != 2 || !h_str.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let h = h_str.parse::<i32>().ok()?;

        let min_str = parts[1].trim();
        if min_str.len() != 2 || !min_str.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let min = min_str.parse::<i32>().ok()?;

        let s = if parts.len() > 2 {
            let s_str = parts[2].trim();
            let dot_idx = s_str.find('.').unwrap_or(s_str.len());
            // Strict check: Seconds integer part must be 2 digits
            if dot_idx != 2 {
                return None;
            }
            // Strict check: Trailing dot not allowed (e.g. "00.")
            if s_str.ends_with('.') {
                return None;
            }

            s_str.parse::<f64>().ok()?
        } else {
            0.0
        };

        if !(0..=24).contains(&h) || !(0..=59).contains(&min) || !(0.0..60.0).contains(&s) {
            return None;
        }
        (h, min, s)
    } else {
        (0, 0, 0.0)
    };

    let jd = compute_julian_day(y, m, d, h, min, s);
    julian_day_to_datetime(jd).ok().map(|dt| ParsedDateTime {
        val: dt,
        is_utc: false,
        overflow,
    })
}

/// Convert a Julian Day number (as f64) to a NaiveDateTime
/// This uses SQLite's algorithm which converts to integer milliseconds first
/// to preserve precision, then converts back to date/time components.
fn julian_day_to_datetime(jd: f64) -> Result<NaiveDateTime> {
    // SQLite approach: Convert JD to integer milliseconds
    // iJD = (sqlite3_int64)(jd * 86400000.0 + 0.5)
    let i_jd = (jd * 86400000.0 + 0.5) as i64;

    // Compute the date (Year, Month, Day) from iJD
    // Z = (int)((iJD + 43200000)/86400000)
    let z = ((i_jd + 43200000) / 86400000) as i32;

    // SQLite's algorithm from computeYMD
    let alpha = ((z as f64 + 32044.75) / 36524.25) as i32 - 52;
    let a = z + 1 + alpha - ((alpha + 100) / 4) + 25;
    let b = a + 1524;
    let c = ((b as f64 - 122.1) / 365.25) as i32;
    let d = (36525 * (c & 32767)) / 100;
    let e = ((b - d) as f64 / 30.6001) as i32;
    let x1 = (30.6001 * e as f64) as i32;

    let day = (b - d - x1) as u32;
    let month = if e < 14 { e - 1 } else { e - 13 } as u32;
    let year = if month > 2 { c - 4716 } else { c - 4715 };

    // Compute the time (Hour, Minute, Second) from iJD
    // day_ms = (int)((iJD + 43200000) % 86400000)
    let day_ms = (i_jd + 43200000).rem_euclid(86400000) as i32;

    // s = (day_ms % 60000) / 1000.0
    let s_millis = day_ms % 60000;
    let seconds = (s_millis / 1000) as u32;
    let millis = (s_millis % 1000) as u32;

    // day_min = day_ms / 60000
    let day_min = day_ms / 60000;
    let minutes = (day_min % 60) as u32;
    let hours = (day_min / 60) as u32;

    // Create the date
    let date = NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| crate::LimboError::InternalError("Invalid date".to_string()))?;

    // Create time with millisecond precision converted to nanoseconds
    let nanos = millis * 1_000_000;
    let time = NaiveTime::from_hms_nano_opt(hours, minutes, seconds, nanos)
        .ok_or_else(|| crate::LimboError::InternalError("Invalid time".to_string()))?;

    Ok(NaiveDateTime::new(date, time))
}

fn is_leap_second(dt: &NaiveDateTime) -> bool {
    // The range from 1,000,000,000 to 1,999,999,999 represents the leap second.
    dt.second() == 59 && dt.nanosecond() > 999_999_999
}

/// Modifier doc https://www.sqlite.org/lang_datefunc.html#modifiers
#[allow(dead_code)]
#[derive(Debug, PartialEq)]
enum Modifier {
    Days(f64),
    Hours(f64),
    Minutes(f64),
    Seconds(f64),
    Months(f64),
    Years(f64),
    TimeOffset(TimeDelta),
    DateOffset {
        years: i32,
        months: i32,
        days: i32,
    },
    DateTimeOffset {
        years: i32,
        months: i32,
        days: i32,
        seconds: f64,
    },
    Ceiling,
    Floor,
    StartOfMonth,
    StartOfYear,
    StartOfDay,
    Weekday(u32),
    UnixEpoch,
    JulianDay,
    Auto,
    Localtime,
    Utc,
    Subsec,
}

fn parse_modifier_number(s: &str) -> Result<f64> {
    s.trim()
        .parse::<f64>()
        .map_err(|_| InvalidModifier(format!("Invalid number: {s}")))
}

/// supports following formats for time shift modifiers
/// - HH:MM
/// - HH:MM:SS
/// - HH:MM:SS.SSS
fn parse_modifier_time(s: &str) -> Result<NaiveTime> {
    match s.len() {
        5 => NaiveTime::parse_from_str(s, "%H:%M"),
        8 => NaiveTime::parse_from_str(s, "%H:%M:%S"),
        12 => NaiveTime::parse_from_str(s, "%H:%M:%S.%3f"),
        _ => return Err(InvalidModifier(format!("Invalid time format: {s}"))),
    }
    .map_err(|_| InvalidModifier(format!("Invalid time format: {s}")))
}

fn parse_modifier(modifier: &str) -> Result<Modifier> {
    // Small helpers to check string suffix/prefix in a case-insensitive way with no allocation
    fn ends_with_ignore_ascii_case(s: &str, suffix: &str) -> bool {
        // suffix is always ASCII, so suffix.len() is the char count
        // But s might have multi-byte UTF-8 chars, so we need to check char boundary
        let start = s.len().saturating_sub(suffix.len());
        s.is_char_boundary(start) && s[start..].eq_ignore_ascii_case(suffix)
    }
    fn starts_with_ignore_ascii_case(s: &str, prefix: &str) -> bool {
        // prefix is always ASCII, so prefix.len() is the char count
        s.is_char_boundary(prefix.len())
            && s.len() >= prefix.len()
            && s[..prefix.len()].eq_ignore_ascii_case(prefix)
    }

    let modifier = modifier.trim();

    // We intentionally avoid usage of match_ignore_ascii_case! macro here because it lead to enormous amount of LLVM code which signifnicantly increase compilation time (see https://github.com/tursodatabase/turso/pull/3929)
    // Fast path for exact matches
    if modifier.eq_ignore_ascii_case("ceiling") {
        return Ok(Modifier::Ceiling);
    } else if modifier.eq_ignore_ascii_case("floor") {
        return Ok(Modifier::Floor);
    } else if modifier.eq_ignore_ascii_case("start of month") {
        return Ok(Modifier::StartOfMonth);
    } else if modifier.eq_ignore_ascii_case("start of year") {
        return Ok(Modifier::StartOfYear);
    } else if modifier.eq_ignore_ascii_case("start of day") {
        return Ok(Modifier::StartOfDay);
    } else if modifier.eq_ignore_ascii_case("unixepoch") {
        return Ok(Modifier::UnixEpoch);
    } else if modifier.eq_ignore_ascii_case("julianday") {
        return Ok(Modifier::JulianDay);
    } else if modifier.eq_ignore_ascii_case("auto") {
        return Ok(Modifier::Auto);
    } else if modifier.eq_ignore_ascii_case("localtime") {
        return Ok(Modifier::Localtime);
    } else if modifier.eq_ignore_ascii_case("utc") {
        return Ok(Modifier::Utc);
    } else if modifier.eq_ignore_ascii_case("subsec") || modifier.eq_ignore_ascii_case("subsecond")
    {
        return Ok(Modifier::Subsec);
    }

    // Patterns
    if starts_with_ignore_ascii_case(modifier, "weekday ") {
        let s = &modifier[8..];
        let day = parse_modifier_number(s)?;
        let day_int = day as i64;
        if day != day_int as f64 || !(0..=6).contains(&day_int) {
            Err(InvalidModifier(
                "Weekday must be between 0 and 6".to_string(),
            ))
        } else {
            Ok(Modifier::Weekday(day_int as u32))
        }
    } else if ends_with_ignore_ascii_case(modifier, " day") {
        Ok(Modifier::Days(parse_modifier_number(
            &modifier[..modifier.len() - 4],
        )?))
    } else if ends_with_ignore_ascii_case(modifier, " days") {
        Ok(Modifier::Days(parse_modifier_number(
            &modifier[..modifier.len() - 5],
        )?))
    } else if ends_with_ignore_ascii_case(modifier, " hour") {
        Ok(Modifier::Hours(parse_modifier_number(
            &modifier[..modifier.len() - 5],
        )?))
    } else if ends_with_ignore_ascii_case(modifier, " hours") {
        Ok(Modifier::Hours(parse_modifier_number(
            &modifier[..modifier.len() - 6],
        )?))
    } else if ends_with_ignore_ascii_case(modifier, " minute") {
        Ok(Modifier::Minutes(parse_modifier_number(
            &modifier[..modifier.len() - 7],
        )?))
    } else if ends_with_ignore_ascii_case(modifier, " minutes") {
        Ok(Modifier::Minutes(parse_modifier_number(
            &modifier[..modifier.len() - 8],
        )?))
    } else if ends_with_ignore_ascii_case(modifier, " second") {
        Ok(Modifier::Seconds(parse_modifier_number(
            &modifier[..modifier.len() - 7],
        )?))
    } else if ends_with_ignore_ascii_case(modifier, " seconds") {
        Ok(Modifier::Seconds(parse_modifier_number(
            &modifier[..modifier.len() - 8],
        )?))
    } else if ends_with_ignore_ascii_case(modifier, " month") {
        Ok(Modifier::Months(parse_modifier_number(
            &modifier[..modifier.len() - 6],
        )?))
    } else if ends_with_ignore_ascii_case(modifier, " months") {
        Ok(Modifier::Months(parse_modifier_number(
            &modifier[..modifier.len() - 7],
        )?))
    } else if ends_with_ignore_ascii_case(modifier, " year") {
        Ok(Modifier::Years(parse_modifier_number(
            &modifier[..modifier.len() - 5],
        )?))
    } else if ends_with_ignore_ascii_case(modifier, " years") {
        Ok(Modifier::Years(parse_modifier_number(
            &modifier[..modifier.len() - 6],
        )?))
    } else if starts_with_ignore_ascii_case(modifier, "+")
        || starts_with_ignore_ascii_case(modifier, "-")
    {
        let sign = if starts_with_ignore_ascii_case(modifier, "-") {
            -1
        } else {
            1
        };
        let rest = &modifier[1..];
        let mut parts = rest.split(' ');
        let part1 = parts.next();
        let part2 = parts.next();
        let part3 = parts.next();
        let digits_in_date = 10;
        match (part1, part2, part3) {
            (Some(s), None, None) => {
                if s.len() == digits_in_date {
                    let (y, mo, d) = parse_offset_ymd(s)?;
                    Ok(Modifier::DateOffset {
                        years: sign * y,
                        months: sign * mo,
                        days: sign * d,
                    })
                } else {
                    let time = parse_modifier_time(s)?;
                    let time_delta = sign * (time.num_seconds_from_midnight() as i32);
                    Ok(Modifier::TimeOffset(TimeDelta::seconds(time_delta.into())))
                }
            }
            (Some(s1), Some(s2), None) => {
                let (y, mo, d) = parse_offset_ymd(s1)?;
                let time = parse_modifier_time(s2)?;

                let total_seconds = time.num_seconds_from_midnight() as f64
                    + (time.nanosecond() as f64 / 1_000_000_000.0);

                Ok(Modifier::DateTimeOffset {
                    years: sign * y,
                    months: sign * mo,
                    days: sign * d,
                    seconds: (sign as f64) * total_seconds,
                })
            }
            _ => Err(InvalidModifier(
                "Invalid date/time offset format".to_string(),
            )),
        }
    } else if modifier.chars().next().is_some_and(|c| c.is_ascii_digit()) && modifier.contains(':')
    {
        let time = parse_modifier_time(modifier)?;
        let ms =
            time.num_seconds_from_midnight() as i64 * 1000 + time.nanosecond() as i64 / 1_000_000;
        Ok(Modifier::TimeOffset(TimeDelta::milliseconds(ms)))
    } else {
        Err(InvalidModifier("Invalid modifier".to_string()))
    }
}

// Custom parser for YYYY-MM-DD that ignores calendar validity (allows 0000-00-00)
fn parse_offset_ymd(s: &str) -> Result<(i32, i32, i32)> {
    let mut parts = s.split('-');

    // Grab exactly 3 parts, then check if a 4th exists
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(y_str), Some(m_str), Some(d_str), None) => {
            let y = y_str
                .parse()
                .map_err(|_| InvalidModifier("Invalid year".into()))?;
            let m = m_str
                .parse()
                .map_err(|_| InvalidModifier("Invalid month".into()))?;
            let d = d_str
                .parse()
                .map_err(|_| InvalidModifier("Invalid day".into()))?;
            Ok((y, m, d))
        }
        _ => Err(InvalidModifier("Invalid offset date format".into())),
    }
}

pub fn exec_timediff<I, E, V>(values: I) -> Value
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut values = values.into_iter();
    if values.len() < 2 {
        return Value::Null;
    }

    let start = parse_naive_date_time(values.next().unwrap());
    let end = parse_naive_date_time(values.next().unwrap());

    match (start, end) {
        (Some(p1), Some(p2)) => compute_timediff(p1.val, p2.val),
        _ => Value::Null,
    }
}

fn compute_timediff(d1: NaiveDateTime, d2: NaiveDateTime) -> Value {
    let (start, end, sign) = if d1 < d2 {
        (d1, d2, "-")
    } else {
        (d2, d1, "+")
    };

    let mut y = end.year() - start.year();
    let mut m = end.month() as i32 - start.month() as i32;
    let mut d = end.day() as i32 - start.day() as i32;

    // Use Nanoseconds for precision
    let start_ns =
        start.num_seconds_from_midnight() as i64 * 1_000_000_000 + start.nanosecond() as i64;
    let end_ns = end.num_seconds_from_midnight() as i64 * 1_000_000_000 + end.nanosecond() as i64;

    let mut ns_diff = end_ns - start_ns;

    if ns_diff < 0 {
        d -= 1;
        ns_diff += 86_400_000_000_000; // 24 * 60 * 60 * 1e9
    }

    if d < 0 {
        m -= 1;
        let mut prev_month = end.month() as i32 - 1;
        let mut prev_year = end.year();
        if prev_month < 1 {
            prev_month = 12;
            prev_year -= 1;
        }
        d += last_day_in_month(prev_year, prev_month as u32) as i32;
    }

    if m < 0 {
        y -= 1;
        m += 12;
    }

    let total_sec = ns_diff / 1_000_000_000;
    let nanos = ns_diff % 1_000_000_000;
    let millis = nanos / 1_000_000;

    let hh = total_sec / 3600;
    let mm = (total_sec % 3600) / 60;
    let ss = total_sec % 60;

    Value::build_text(format!(
        "{sign}{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{millis:03}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_get_date_from_time_value() {
        let now = chrono::Local::now().to_utc().format("%Y-%m-%d").to_string();

        let prev_date_str = "2024-07-20";
        let test_date_str = "2024-07-21";
        let next_date_str = "2024-07-22";

        let test_cases: Vec<(Value, &str)> = vec![
            // Format 1: YYYY-MM-DD (no timezone applicable)
            (Value::build_text("2024-07-21"), test_date_str),
            // Format 2: YYYY-MM-DD HH:MM
            (Value::build_text("2024-07-21 22:30"), test_date_str),
            (Value::build_text("2024-07-21 22:30+02:00"), test_date_str),
            (Value::build_text("2024-07-21 22:30-05:00"), next_date_str),
            (Value::build_text("2024-07-21 01:30+05:00"), prev_date_str),
            (Value::build_text("2024-07-21 22:30Z"), test_date_str),
            // Format 3: YYYY-MM-DD HH:MM:SS
            (Value::build_text("2024-07-21 22:30:45"), test_date_str),
            (
                Value::build_text("2024-07-21 22:30:45+02:00"),
                test_date_str,
            ),
            (
                Value::build_text("2024-07-21 22:30:45-05:00"),
                next_date_str,
            ),
            (
                Value::build_text("2024-07-21 01:30:45+05:00"),
                prev_date_str,
            ),
            (Value::build_text("2024-07-21 22:30:45Z"), test_date_str),
            // Format 4: YYYY-MM-DD HH:MM:SS.SSS
            (Value::build_text("2024-07-21 22:30:45.123"), test_date_str),
            (
                Value::build_text("2024-07-21 22:30:45.123+02:00"),
                test_date_str,
            ),
            (
                Value::build_text("2024-07-21 22:30:45.123-05:00"),
                next_date_str,
            ),
            (
                Value::build_text("2024-07-21 01:30:45.123+05:00"),
                prev_date_str,
            ),
            (Value::build_text("2024-07-21 22:30:45.123Z"), test_date_str),
            // Format 5: YYYY-MM-DDTHH:MM
            (Value::build_text("2024-07-21T22:30"), test_date_str),
            (Value::build_text("2024-07-21T22:30+02:00"), test_date_str),
            (Value::build_text("2024-07-21T22:30-05:00"), next_date_str),
            (Value::build_text("2024-07-21T01:30+05:00"), prev_date_str),
            (Value::build_text("2024-07-21T22:30Z"), test_date_str),
            // Format 6: YYYY-MM-DDTHH:MM:SS
            (Value::build_text("2024-07-21T22:30:45"), test_date_str),
            (
                Value::build_text("2024-07-21T22:30:45+02:00"),
                test_date_str,
            ),
            (
                Value::build_text("2024-07-21T22:30:45-05:00"),
                next_date_str,
            ),
            (
                Value::build_text("2024-07-21T01:30:45+05:00"),
                prev_date_str,
            ),
            (Value::build_text("2024-07-21T22:30:45Z"), test_date_str),
            // Format 7: YYYY-MM-DDTHH:MM:SS.SSS
            (Value::build_text("2024-07-21T22:30:45.123"), test_date_str),
            (
                Value::build_text("2024-07-21T22:30:45.123+02:00"),
                test_date_str,
            ),
            (
                Value::build_text("2024-07-21T22:30:45.123-05:00"),
                next_date_str,
            ),
            (
                Value::build_text("2024-07-21T01:30:45.123+05:00"),
                prev_date_str,
            ),
            (Value::build_text("2024-07-21T22:30:45.123Z"), test_date_str),
            // Format 8: HH:MM
            (Value::build_text("22:30"), "2000-01-01"),
            (Value::build_text("22:30+02:00"), "2000-01-01"),
            (Value::build_text("22:30-05:00"), "2000-01-02"),
            (Value::build_text("01:30+05:00"), "1999-12-31"),
            (Value::build_text("22:30Z"), "2000-01-01"),
            // Format 9: HH:MM:SS
            (Value::build_text("22:30:45"), "2000-01-01"),
            (Value::build_text("22:30:45+02:00"), "2000-01-01"),
            (Value::build_text("22:30:45-05:00"), "2000-01-02"),
            (Value::build_text("01:30:45+05:00"), "1999-12-31"),
            (Value::build_text("22:30:45Z"), "2000-01-01"),
            // Format 10: HH:MM:SS.SSS
            (Value::build_text("22:30:45.123"), "2000-01-01"),
            (Value::build_text("22:30:45.123+02:00"), "2000-01-01"),
            (Value::build_text("22:30:45.123-05:00"), "2000-01-02"),
            (Value::build_text("01:30:45.123+05:00"), "1999-12-31"),
            (Value::build_text("22:30:45.123Z"), "2000-01-01"),
            // Test Format 11: 'now'
            (Value::build_text("now"), &now),
            // Format 12: DDDDDDDDDD (Julian date as float or integer)
            (Value::Float(2460512.5), test_date_str),
            (Value::Integer(2460513), test_date_str),
        ];

        for (input, expected) in test_cases {
            let result = exec_date(&[input.clone()]);
            assert_eq!(
                result,
                Value::build_text(expected.to_string()),
                "Failed for input: {input:?}"
            );
        }
    }

    #[test]
    fn test_invalid_get_date_from_time_value() {
        let invalid_cases = vec![
            Value::build_text("2024-07-21 25:00"),    // Invalid hour
            Value::build_text("2024-07-21 25:00:00"), // Invalid hour
            Value::build_text("2024-07-21 23:60:00"), // Invalid minute
            Value::build_text("2024-07-21 22:58:60"), // Invalid second
            Value::build_text("2024-07-32"),          // Invalid day
            Value::build_text("2024-13-01"),          // Invalid month
            Value::build_text("invalid_date"),        // Completely invalid string
            Value::build_text(""),                    // Empty string
            Value::Integer(i64::MAX),                 // Large Julian day
            Value::Integer(-1),                       // Negative Julian day
            Value::Float(f64::MAX),                   // Large float
            Value::Float(-1.0),                       // Negative Julian day as float
            Value::Float(f64::NAN),                   // NaN
            Value::Float(f64::INFINITY),              // Infinity
            Value::Null,                              // Null value
            Value::Blob(vec![1, 2, 3]),               // Blob (unsupported type)
            // Invalid timezone tests
            Value::build_text("2024-07-21T12:00:00+24:00"), // Invalid timezone offset (too large)
            Value::build_text("2024-07-21T12:00:00-24:00"), // Invalid timezone offset (too small)
            Value::build_text("2024-07-21T12:00:00+00:60"), // Invalid timezone minutes
            Value::build_text("2024-07-21T12:00:00+00:00:00"), // Invalid timezone format (extra seconds)
            Value::build_text("2024-07-21T12:00:00+"),         // Incomplete timezone
            Value::build_text("2024-07-21T12:00:00+Z"),        // Invalid timezone format
            Value::build_text("2024-07-21T12:00:00+00:00Z"),   // Mixing offset and Z
            Value::build_text("2024-07-21T12:00:00UTC"),       // Named timezone (not supported)
        ];

        for case in invalid_cases.iter() {
            let result = exec_date([case]);
            assert_eq!(result, Value::Null);
        }
    }

    #[test]
    fn test_valid_get_time_from_datetime_value() {
        let test_time_str = "22:30:45";
        let prev_time_str = "20:30:45";
        let next_time_str = "03:30:45";

        let test_cases = vec![
            // Format 1: YYYY-MM-DD (no timezone applicable)
            (Value::build_text("2024-07-21"), "00:00:00"),
            // Format 2: YYYY-MM-DD HH:MM
            (Value::build_text("2024-07-21 22:30"), "22:30:00"),
            (Value::build_text("2024-07-21 22:30+02:00"), "20:30:00"),
            (Value::build_text("2024-07-21 22:30-05:00"), "03:30:00"),
            (Value::build_text("2024-07-21 22:30Z"), "22:30:00"),
            // Format 3: YYYY-MM-DD HH:MM:SS
            (Value::build_text("2024-07-21 22:30:45"), test_time_str),
            (
                Value::build_text("2024-07-21 22:30:45+02:00"),
                prev_time_str,
            ),
            (
                Value::build_text("2024-07-21 22:30:45-05:00"),
                next_time_str,
            ),
            (Value::build_text("2024-07-21 22:30:45Z"), test_time_str),
            // Format 4: YYYY-MM-DD HH:MM:SS.SSS
            (Value::build_text("2024-07-21 22:30:45.123"), test_time_str),
            (
                Value::build_text("2024-07-21 22:30:45.123+02:00"),
                prev_time_str,
            ),
            (
                Value::build_text("2024-07-21 22:30:45.123-05:00"),
                next_time_str,
            ),
            (Value::build_text("2024-07-21 22:30:45.123Z"), test_time_str),
            // Format 5: YYYY-MM-DDTHH:MM
            (Value::build_text("2024-07-21T22:30"), "22:30:00"),
            (Value::build_text("2024-07-21T22:30+02:00"), "20:30:00"),
            (Value::build_text("2024-07-21T22:30-05:00"), "03:30:00"),
            (Value::build_text("2024-07-21T22:30Z"), "22:30:00"),
            // Format 6: YYYY-MM-DDTHH:MM:SS
            (Value::build_text("2024-07-21T22:30:45"), test_time_str),
            (
                Value::build_text("2024-07-21T22:30:45+02:00"),
                prev_time_str,
            ),
            (
                Value::build_text("2024-07-21T22:30:45-05:00"),
                next_time_str,
            ),
            (Value::build_text("2024-07-21T22:30:45Z"), test_time_str),
            // Format 7: YYYY-MM-DDTHH:MM:SS.SSS
            (Value::build_text("2024-07-21T22:30:45.123"), test_time_str),
            (
                Value::build_text("2024-07-21T22:30:45.123+02:00"),
                prev_time_str,
            ),
            (
                Value::build_text("2024-07-21T22:30:45.123-05:00"),
                next_time_str,
            ),
            (Value::build_text("2024-07-21T22:30:45.123Z"), test_time_str),
            // Format 8: HH:MM
            (Value::build_text("22:30"), "22:30:00"),
            (Value::build_text("22:30+02:00"), "20:30:00"),
            (Value::build_text("22:30-05:00"), "03:30:00"),
            (Value::build_text("22:30Z"), "22:30:00"),
            // Format 9: HH:MM:SS
            (Value::build_text("22:30:45"), test_time_str),
            (Value::build_text("22:30:45+02:00"), prev_time_str),
            (Value::build_text("22:30:45-05:00"), next_time_str),
            (Value::build_text("22:30:45Z"), test_time_str),
            // Format 10: HH:MM:SS.SSS
            (Value::build_text("22:30:45.123"), test_time_str),
            (Value::build_text("22:30:45.123+02:00"), prev_time_str),
            (Value::build_text("22:30:45.123-05:00"), next_time_str),
            (Value::build_text("22:30:45.123Z"), test_time_str),
            // Format 12: DDDDDDDDDD (Julian date as float or integer)
            (Value::Float(2460082.1), "14:24:00"),
            (Value::Integer(2460082), "12:00:00"),
        ];

        for (input, expected) in test_cases {
            let result = exec_time(&[input]);
            if let Value::Text(result_str) = result {
                assert_eq!(result_str.as_str(), expected);
            } else {
                panic!("Expected Value::Text, but got: {result:?}");
            }
        }
    }

    #[test]
    fn test_invalid_get_time_from_datetime_value() {
        let invalid_cases = vec![
            Value::build_text("2024-07-21 25:00"),    // Invalid hour
            Value::build_text("2024-07-21 25:00:00"), // Invalid hour
            Value::build_text("2024-07-21 23:60:00"), // Invalid minute
            Value::build_text("2024-07-21 22:58:60"), // Invalid second
            Value::build_text("2024-07-32"),          // Invalid day
            Value::build_text("2024-13-01"),          // Invalid month
            Value::build_text("invalid_date"),        // Completely invalid string
            Value::build_text(""),                    // Empty string
            Value::Integer(i64::MAX),                 // Large Julian day
            Value::Integer(-1),                       // Negative Julian day
            Value::Float(f64::MAX),                   // Large float
            Value::Float(-1.0),                       // Negative Julian day as float
            Value::Float(f64::NAN),                   // NaN
            Value::Float(f64::INFINITY),              // Infinity
            Value::Null,                              // Null value
            Value::Blob(vec![1, 2, 3]),               // Blob (unsupported type)
            // Invalid timezone tests
            Value::build_text("2024-07-21T12:00:00+24:00"), // Invalid timezone offset (too large)
            Value::build_text("2024-07-21T12:00:00-24:00"), // Invalid timezone offset (too small)
            Value::build_text("2024-07-21T12:00:00+00:60"), // Invalid timezone minutes
            Value::build_text("2024-07-21T12:00:00+00:00:00"), // Invalid timezone format (extra seconds)
            Value::build_text("2024-07-21T12:00:00+"),         // Incomplete timezone
            Value::build_text("2024-07-21T12:00:00+Z"),        // Invalid timezone format
            Value::build_text("2024-07-21T12:00:00+00:00Z"),   // Mixing offset and Z
            Value::build_text("2024-07-21T12:00:00UTC"),       // Named timezone (not supported)
        ];

        for case in invalid_cases {
            let result = exec_time(&[case.clone()]);
            assert_eq!(result, Value::Null);
        }
    }

    #[test]
    fn test_parse_days() {
        assert_eq!(parse_modifier("5 days").unwrap(), Modifier::Days(5.0));
        assert_eq!(parse_modifier("-3 days").unwrap(), Modifier::Days(-3.0));
        assert_eq!(parse_modifier("+2 days").unwrap(), Modifier::Days(2.0));
        assert_eq!(parse_modifier("4  days").unwrap(), Modifier::Days(4.0));
        assert_eq!(parse_modifier("6   DAYS").unwrap(), Modifier::Days(6.0));
        assert_eq!(parse_modifier("+5  DAYS").unwrap(), Modifier::Days(5.0));
        // Fractional days
        assert_eq!(parse_modifier("1.5 days").unwrap(), Modifier::Days(1.5));
        assert_eq!(parse_modifier("-0.25 days").unwrap(), Modifier::Days(-0.25));
    }

    #[test]
    fn test_parse_hours() {
        assert_eq!(parse_modifier("12 hours").unwrap(), Modifier::Hours(12.0));
        assert_eq!(parse_modifier("-2 hours").unwrap(), Modifier::Hours(-2.0));
        assert_eq!(parse_modifier("+3  HOURS").unwrap(), Modifier::Hours(3.0));
        // Fractional hours
        assert_eq!(parse_modifier("0.5 hours").unwrap(), Modifier::Hours(0.5));
    }

    #[test]
    fn test_parse_minutes() {
        assert_eq!(
            parse_modifier("30 minutes").unwrap(),
            Modifier::Minutes(30.0)
        );
        assert_eq!(
            parse_modifier("-15 minutes").unwrap(),
            Modifier::Minutes(-15.0)
        );
        assert_eq!(
            parse_modifier("+45  MINUTES").unwrap(),
            Modifier::Minutes(45.0)
        );
    }

    #[test]
    fn test_parse_seconds() {
        assert_eq!(
            parse_modifier("45 seconds").unwrap(),
            Modifier::Seconds(45.0)
        );
        assert_eq!(
            parse_modifier("-10 seconds").unwrap(),
            Modifier::Seconds(-10.0)
        );
        assert_eq!(
            parse_modifier("+20  SECONDS").unwrap(),
            Modifier::Seconds(20.0)
        );
    }

    #[test]
    fn test_parse_months() {
        assert_eq!(parse_modifier("3 months").unwrap(), Modifier::Months(3.0));
        assert_eq!(parse_modifier("-1 months").unwrap(), Modifier::Months(-1.0));
        assert_eq!(parse_modifier("+6  MONTHS").unwrap(), Modifier::Months(6.0));
    }

    #[test]
    fn test_parse_years() {
        assert_eq!(parse_modifier("2 years").unwrap(), Modifier::Years(2.0));
        assert_eq!(parse_modifier("-1 years").unwrap(), Modifier::Years(-1.0));
        assert_eq!(parse_modifier("+10  YEARS").unwrap(), Modifier::Years(10.0));
    }

    #[test]
    fn test_parse_time_offset() {
        assert_eq!(
            parse_modifier("+01:30").unwrap(),
            Modifier::TimeOffset(TimeDelta::hours(1) + TimeDelta::minutes(30))
        );
        assert_eq!(
            parse_modifier("-00:45").unwrap(),
            Modifier::TimeOffset(TimeDelta::minutes(-45))
        );
        assert_eq!(
            parse_modifier("+02:15:30").unwrap(),
            Modifier::TimeOffset(
                TimeDelta::hours(2) + TimeDelta::minutes(15) + TimeDelta::seconds(30)
            )
        );
        assert_eq!(
            parse_modifier("+02:15:30.250").unwrap(),
            Modifier::TimeOffset(
                TimeDelta::hours(2) + TimeDelta::minutes(15) + TimeDelta::seconds(30)
            )
        );
    }

    #[test]
    fn test_parse_date_offset() {
        assert_eq!(
            parse_modifier("+2023-05-15").unwrap(),
            Modifier::DateOffset {
                years: 2023,
                months: 5,
                days: 15,
            }
        );
        assert_eq!(
            parse_modifier("-2023-05-15").unwrap(),
            Modifier::DateOffset {
                years: -2023,
                months: -5,
                days: -15,
            }
        );
    }

    #[test]
    fn test_parse_date_time_offset() {
        assert_eq!(
            parse_modifier("+2023-05-15 14:30").unwrap(),
            Modifier::DateTimeOffset {
                years: 2023,
                months: 5,
                days: 15,
                seconds: ((14 * 60 + 30) * 60) as f64,
            }
        );
        assert_eq!(
            parse_modifier("-0001-05-15 14:30").unwrap(),
            Modifier::DateTimeOffset {
                years: -1,
                months: -5,
                days: -15,
                seconds: -((14 * 60 + 30) * 60) as f64,
            }
        );
    }

    #[test]
    fn test_parse_start_of() {
        assert_eq!(
            parse_modifier("start of month").unwrap(),
            Modifier::StartOfMonth
        );
        assert_eq!(
            parse_modifier("START OF MONTH").unwrap(),
            Modifier::StartOfMonth
        );
        assert_eq!(
            parse_modifier("start of year").unwrap(),
            Modifier::StartOfYear
        );
        assert_eq!(
            parse_modifier("START OF YEAR").unwrap(),
            Modifier::StartOfYear
        );
        assert_eq!(
            parse_modifier("start of day").unwrap(),
            Modifier::StartOfDay
        );
        assert_eq!(
            parse_modifier("START OF DAY").unwrap(),
            Modifier::StartOfDay
        );
    }

    #[test]
    fn test_parse_weekday() {
        assert_eq!(parse_modifier("weekday 0").unwrap(), Modifier::Weekday(0));
        assert_eq!(parse_modifier("WEEKDAY 6").unwrap(), Modifier::Weekday(6));
    }

    #[test]
    fn test_parse_ceiling_modifier() {
        assert_eq!(parse_modifier("ceiling").unwrap(), Modifier::Ceiling);
        assert_eq!(parse_modifier("CEILING").unwrap(), Modifier::Ceiling);
    }

    #[test]
    fn test_parse_other_modifiers() {
        assert_eq!(parse_modifier("unixepoch").unwrap(), Modifier::UnixEpoch);
        assert_eq!(parse_modifier("UNIXEPOCH").unwrap(), Modifier::UnixEpoch);
        assert_eq!(parse_modifier("julianday").unwrap(), Modifier::JulianDay);
        assert_eq!(parse_modifier("JULIANDAY").unwrap(), Modifier::JulianDay);
        assert_eq!(parse_modifier("auto").unwrap(), Modifier::Auto);
        assert_eq!(parse_modifier("AUTO").unwrap(), Modifier::Auto);
        assert_eq!(parse_modifier("localtime").unwrap(), Modifier::Localtime);
        assert_eq!(parse_modifier("LOCALTIME").unwrap(), Modifier::Localtime);
        assert_eq!(parse_modifier("utc").unwrap(), Modifier::Utc);
        assert_eq!(parse_modifier("UTC").unwrap(), Modifier::Utc);
        assert_eq!(parse_modifier("subsec").unwrap(), Modifier::Subsec);
        assert_eq!(parse_modifier("SUBSEC").unwrap(), Modifier::Subsec);
        assert_eq!(parse_modifier("subsecond").unwrap(), Modifier::Subsec);
        assert_eq!(parse_modifier("SUBSECOND").unwrap(), Modifier::Subsec);
    }

    #[test]
    fn test_parse_invalid_modifier() {
        assert!(parse_modifier("invalid modifier").is_err());
        assert!(parse_modifier("5").is_err());
        assert!(parse_modifier("days").is_err());
        assert!(parse_modifier("++5 days").is_err());
        assert!(parse_modifier("weekday 7").is_err());
    }

    fn create_datetime(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
    ) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(hour, min, sec)
            .unwrap()
    }

    fn setup_datetime() -> NaiveDateTime {
        create_datetime(2023, 6, 15, 12, 30, 45)
    }

    #[test]
    fn test_apply_modifier_days() {
        let mut dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "5 days", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 20, 12, 30, 45));

        dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "-3 days", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 12, 12, 30, 45));
    }

    #[test]
    fn test_apply_modifier_hours() {
        let mut dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "6 hours", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 18, 30, 45));

        dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "-2 hours", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 10, 30, 45));
    }

    #[test]
    fn test_apply_modifier_minutes() {
        let mut dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "45 minutes",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 13, 15, 45));

        dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "-15 minutes",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 12, 15, 45));
    }

    #[test]
    fn test_apply_modifier_seconds() {
        let mut dt = setup_datetime();

        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "30 seconds",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 12, 31, 15));

        dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "-20 seconds",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 12, 30, 25));
    }

    #[test]
    fn test_apply_modifier_time_offset() {
        let mut dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "+01:30", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 14, 0, 45));

        dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "-00:45", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 11, 45, 45));
    }

    #[test]
    fn test_apply_modifier_date_time_offset() {
        let mut dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "+0001-01-01 01:01",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2024, 7, 16, 13, 31, 45));

        dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "-0001-01-01 01:01",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2022, 5, 14, 11, 29, 45));

        // Test with larger offsets
        dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "+0002-03-04 05:06",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2025, 9, 19, 17, 36, 45));

        dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "-0002-03-04 05:06",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2021, 3, 11, 7, 24, 45));
    }

    #[test]
    fn test_apply_modifier_start_of_year() {
        let mut dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "start of year",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 1, 0, 0, 0));
    }

    #[test]
    fn test_apply_modifier_start_of_day() {
        let mut dt = setup_datetime();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "start of day",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 0, 0, 0));
    }

    fn text(value: &str) -> Value {
        Value::build_text(value.to_string())
    }

    fn format(dt: NaiveDateTime) -> String {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    }
    fn weekday_sunday_based(dt: &NaiveDateTime) -> u32 {
        dt.weekday().num_days_from_sunday()
    }

    #[test]
    fn test_single_modifier() {
        let time = setup_datetime();
        let expected = format(time - TimeDelta::days(1));
        let result = exec_datetime(
            &[text("2023-06-15 12:30:45"), text("-1 day")],
            DateTimeOutput::DateTime,
        );
        assert_eq!(result, text(&expected));
    }

    #[test]
    fn test_multiple_modifiers() {
        let time = setup_datetime();
        let expected = format(time - TimeDelta::days(1) + TimeDelta::hours(3));
        let result = exec_datetime(
            &[
                text("2023-06-15 12:30:45"),
                text("-1 day"),
                text("+3 hours"),
            ],
            DateTimeOutput::DateTime,
        );
        assert_eq!(result, text(&expected));
    }

    #[test]
    fn test_subsec_modifier() {
        let time = setup_datetime();
        let result = exec_datetime(
            &[text("2023-06-15 12:30:45"), text("subsec")],
            DateTimeOutput::Time,
        );
        let result = NaiveTime::parse_from_str(&result.to_string(), "%H:%M:%S%.3f").unwrap();
        assert_eq!(time.time(), result);
    }

    #[test]
    fn test_start_of_day_modifier() {
        let time = setup_datetime();
        let start_of_day = time.date().and_hms_opt(0, 0, 0).unwrap();
        let expected = format(start_of_day - TimeDelta::days(1));
        let result = exec_datetime(
            &[
                text("2023-06-15 12:30:45"),
                text("start of day"),
                text("-1 day"),
            ],
            DateTimeOutput::DateTime,
        );
        assert_eq!(result, text(&expected));
    }

    #[test]
    fn test_start_of_month_modifier() {
        let time = setup_datetime();
        let start_of_month = NaiveDate::from_ymd_opt(time.year(), time.month(), 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let expected = format(start_of_month + TimeDelta::days(1));
        let result = exec_datetime(
            &[
                text("2023-06-15 12:30:45"),
                text("start of month"),
                text("+1 day"),
            ],
            DateTimeOutput::DateTime,
        );
        assert_eq!(result, text(&expected));
    }

    #[test]
    fn test_start_of_year_modifier() {
        let time = setup_datetime();
        let start_of_year = NaiveDate::from_ymd_opt(time.year(), 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let expected = format(start_of_year + TimeDelta::days(30) + TimeDelta::hours(5));
        let result = exec_datetime(
            &[
                text("2023-06-15 12:30:45"),
                text("start of year"),
                text("+30 days"),
                text("+5 hours"),
            ],
            DateTimeOutput::DateTime,
        );
        assert_eq!(result, text(&expected));
    }

    #[test]
    fn test_timezone_modifiers() {
        let dt = setup_datetime();
        let result_local = exec_datetime(
            &[text("2023-06-15 12:30:45"), text("localtime")],
            DateTimeOutput::DateTime,
        );
        assert_eq!(
            result_local,
            text(
                &dt.and_utc()
                    .with_timezone(&chrono::Local)
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string()
            )
        );
        // TODO: utc modifier assumes time given is not already utc
        // add test when fixed in the future
    }

    #[test]
    fn test_combined_modifiers() {
        let time = create_datetime(2000, 1, 1, 0, 0, 0);
        let expected = time - TimeDelta::days(1)
            + TimeDelta::hours(5)
            + TimeDelta::minutes(30)
            + TimeDelta::seconds(15);
        let result = exec_datetime(
            &[
                text("2000-01-01 00:00:00"),
                text("-1 day"),
                text("+5 hours"),
                text("+30 minutes"),
                text("+15 seconds"),
                text("subsec"),
            ],
            DateTimeOutput::DateTime,
        );
        let result =
            NaiveDateTime::parse_from_str(&result.to_string(), "%Y-%m-%d %H:%M:%S%.3f").unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_max_datetime_limit() {
        // max datetime limit
        let max = NaiveDate::from_ymd_opt(9999, 12, 31)
            .unwrap()
            .and_hms_opt(23, 59, 59)
            .unwrap();
        let expected = format(max);
        let result = exec_datetime(&[text("9999-12-31 23:59:59")], DateTimeOutput::DateTime);
        assert_eq!(result, text(&expected));
    }

    // leap second
    #[test]
    fn test_leap_second_ignored() {
        // SQLite returns NULL for invalid times (like second=60 which parsing fails for ex. SELECT typeof(datetime('2023-05-18 15:30:45+25:00')); )
        let result = exec_datetime(&[text("2024-06-30 23:59:60")], DateTimeOutput::DateTime);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_already_on_weekday_no_change() {
        // 2023-01-01 is a Sunday => weekday 0
        let mut dt = create_datetime(2023, 1, 1, 12, 0, 0);
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "weekday 0", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 1, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 0);
    }

    #[test]
    fn test_move_forward_if_different() {
        // 2023-01-01 is a Sunday => weekday 0
        // "weekday 1" => next Monday => 2023-01-02
        let mut dt = create_datetime(2023, 1, 1, 12, 0, 0);
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "weekday 1", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 2, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 1);

        // 2023-01-03 is a Tuesday => weekday 2
        // "weekday 5" => next Friday => 2023-01-06
        let mut dt = create_datetime(2023, 1, 3, 12, 0, 0);
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "weekday 5", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 6, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 5);
    }

    #[test]
    fn test_wrap_around_weekend() {
        // 2023-01-06 is a Friday => weekday 5
        // "weekday 0" => next Sunday => 2023-01-08
        let mut dt = create_datetime(2023, 1, 6, 12, 0, 0);
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "weekday 0", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 8, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 0);

        // Now confirm that being on Sunday (weekday 0) and asking for "weekday 0" stays put
        let mut is_utc = false;
        apply_modifier(&mut dt, "weekday 0", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 8, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 0);
    }

    #[test]
    fn test_same_day_stays_put() {
        // 2023-01-05 is a Thursday => weekday 4
        // Asking for weekday 4 => no change
        let mut dt = create_datetime(2023, 1, 5, 12, 0, 0);
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "weekday 4", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 5, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 4);
    }

    #[test]
    fn test_already_on_friday_no_change() {
        // 2023-01-06 is a Friday => weekday 5
        // Asking for weekday 5 => no change if already on Friday
        let mut dt = create_datetime(2023, 1, 6, 12, 0, 0);
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "weekday 5", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 1, 6, 12, 0, 0));
        assert_eq!(weekday_sunday_based(&dt), 5);
    }

    #[test]
    fn test_apply_modifier_julianday() {
        let dt = create_datetime(2000, 1, 1, 12, 0, 0);

        // Convert datetime to julian day using our implementation
        let julian_day_value = to_julian_day_exact(&dt);

        // Convert back
        let dt_result = julian_day_to_datetime(julian_day_value).unwrap();
        assert_eq!(dt_result, dt);
    }

    #[test]
    fn test_apply_modifier_start_of_month() {
        let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "start of month",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 1, 0, 0, 0));
    }

    #[test]
    fn test_apply_modifier_subsec() {
        let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
        let dt_with_nanos = dt.with_nanosecond(123_456_789).unwrap();
        dt = dt_with_nanos;
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "subsec", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, dt_with_nanos);
    }

    #[test]
    fn test_apply_modifier_floor_modifier_n_floor_gt_0() {
        let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
        let mut n_floor = 3;

        let mut is_utc = false;
        apply_modifier(&mut dt, "floor", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 12, 12, 30, 45));
    }

    #[test]
    fn test_apply_modifier_floor_modifier_n_floor_le_0() {
        let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
        let mut n_floor = 0;

        let mut is_utc = false;
        apply_modifier(&mut dt, "floor", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 15, 12, 30, 45));

        n_floor = 2;
        let mut is_utc = false;
        apply_modifier(&mut dt, "floor", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 13, 12, 30, 45));
    }

    #[test]
    fn test_apply_modifier_ceiling_modifier_sets_n_floor_to_zero() {
        let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
        let mut n_floor = 5;

        let mut is_utc = false;
        apply_modifier(&mut dt, "ceiling", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(n_floor, 0);
    }

    #[test]
    fn test_apply_modifier_start_of_month_basic() {
        // Basic check: from mid-month to the 1st at 00:00:00.
        let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "start of month",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 1, 0, 0, 0));
    }

    #[test]
    fn test_apply_modifier_start_of_month_already_at_first() {
        // If we're already at the start of the month, no change.
        let mut dt = create_datetime(2023, 6, 1, 0, 0, 0);
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "start of month",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2023, 6, 1, 0, 0, 0));
    }

    #[test]
    fn test_apply_modifier_start_of_month_edge_case() {
        // edge case: month boundary. 2023-07-31 -> start of July.
        let mut dt = create_datetime(2023, 7, 31, 23, 59, 59);
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(
            &mut dt,
            "start of month",
            &mut n_floor,
            None,
            false,
            &mut is_utc,
        )
        .unwrap();
        assert_eq!(dt, create_datetime(2023, 7, 1, 0, 0, 0));
    }

    #[test]
    fn test_apply_modifier_subsec_no_change() {
        let mut dt = create_datetime(2023, 6, 15, 12, 30, 45);
        let dt_with_nanos = dt.with_nanosecond(123_456_789).unwrap();
        dt = dt_with_nanos;
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "subsec", &mut n_floor, None, false, &mut is_utc).unwrap();
        assert_eq!(dt, dt_with_nanos);
    }

    #[test]
    fn test_apply_modifier_subsec_preserves_fractional_seconds() {
        let mut dt = create_datetime(2025, 1, 2, 4, 12, 21)
            .with_nanosecond(891_000_000) // 891 milliseconds
            .unwrap();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "subsec", &mut n_floor, None, false, &mut is_utc).unwrap();

        let formatted = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        assert_eq!(formatted, "2025-01-02 04:12:21.891");
    }

    #[test]
    fn test_apply_modifier_subsec_no_fractional_seconds() {
        let mut dt = create_datetime(2025, 1, 2, 4, 12, 21);
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "subsec", &mut n_floor, None, false, &mut is_utc).unwrap();

        let formatted = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        assert_eq!(formatted, "2025-01-02 04:12:21.000");
    }

    #[test]
    fn test_apply_modifier_subsec_truncate_to_milliseconds() {
        let mut dt = create_datetime(2025, 1, 2, 4, 12, 21)
            .with_nanosecond(891_123_456)
            .unwrap();
        let mut n_floor = 0;
        let mut is_utc = false;
        apply_modifier(&mut dt, "subsec", &mut n_floor, None, false, &mut is_utc).unwrap();

        let formatted = dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        assert_eq!(formatted, "2025-01-02 04:12:21.891");
    }

    #[test]
    fn test_is_leap_second() {
        let dt = DateTime::from_timestamp(1483228799, 999_999_999)
            .unwrap()
            .naive_utc();
        assert!(!is_leap_second(&dt));

        let dt = DateTime::from_timestamp(1483228799, 1_500_000_000)
            .unwrap()
            .naive_utc();
        assert!(is_leap_second(&dt));
    }

    #[test]
    fn test_strftime() {}

    #[test]
    fn test_exec_timediff() {
        let start = Value::build_text("12:00:00");
        let end = Value::build_text("14:30:45");
        let expected = Value::build_text("-0000-00-00 02:30:45.000");
        assert_eq!(exec_timediff(&[start, end]), expected);

        let start = Value::build_text("14:30:45");
        let end = Value::build_text("12:00:00");
        let expected = Value::build_text("+0000-00-00 02:30:45.000");
        assert_eq!(exec_timediff(&[start, end]), expected);

        let start = Value::build_text("12:00:01.300");
        let end = Value::build_text("12:00:00.500");
        let expected = Value::build_text("+0000-00-00 00:00:00.800");
        assert_eq!(exec_timediff(&[start, end]), expected);

        let start = Value::build_text("13:30:00");
        let end = Value::build_text("16:45:30");
        let expected = Value::build_text("-0000-00-00 03:15:30.000");
        assert_eq!(exec_timediff(&[start, end]), expected);

        let start = Value::build_text("2023-05-10 23:30:00");
        let end = Value::build_text("2023-05-11 01:15:00");
        let expected = Value::build_text("-0000-00-00 01:45:00.000");
        assert_eq!(exec_timediff(&[start, end]), expected);

        let start = Value::Null;
        let end = Value::build_text("12:00:00");
        let expected = Value::Null;
        assert_eq!(exec_timediff(&[start, end]), expected);

        let start = Value::build_text("not a time");
        let end = Value::build_text("12:00:00");
        let expected = Value::Null;
        assert_eq!(exec_timediff(&[start, end.clone()]), expected);

        // Test identical times - should return zero duration, not Null
        let start = Value::build_text("12:00:00");
        let end = Value::build_text("12:00:00");
        let expected = Value::build_text("+0000-00-00 00:00:00.000");
        assert_eq!(exec_timediff(&[start, end]), expected);
    }

    #[test]
    fn test_subsec_fixed_time_expansion() {
        let result = exec_datetime(
            &[text("2024-01-01 12:00:00"), text("subsec")],
            DateTimeOutput::DateTime,
        );
        assert_eq!(
            result,
            text("2024-01-01 12:00:00.000"),
            "Failed to expand zero-nanosecond time with subsec"
        );
    }

    #[test]
    fn test_subsec_date_only_expansion() {
        let result = exec_datetime(
            &[text("2024-01-01"), text("subsec")],
            DateTimeOutput::DateTime,
        );
        assert_eq!(
            result,
            text("2024-01-01 00:00:00.000"),
            "Failed to expand date-only input to midnight.000"
        );
    }

    #[test]
    fn test_subsec_iso_separator() {
        let result = exec_datetime(
            &[text("2024-01-01T15:30:00"), text("subsec")],
            DateTimeOutput::DateTime,
        );
        assert_eq!(
            result,
            text("2024-01-01 15:30:00.000"),
            "Failed to normalize ISO T separator with subsec"
        );
    }

    #[test]
    fn test_subsec_chaining_before_math() {
        let result = exec_datetime(
            &[text("2024-01-01 12:00:00"), text("subsec"), text("+1 hour")],
            DateTimeOutput::DateTime,
        );
        assert_eq!(
            result,
            text("2024-01-01 13:00:00.000"),
            "Subsec flag failed to persist through subsequent arithmetic"
        );
    }

    #[test]
    fn test_subsec_chaining_after_math() {
        let result = exec_datetime(
            &[text("2024-01-01 12:00:00"), text("+1 hour"), text("subsec")],
            DateTimeOutput::DateTime,
        );
        assert_eq!(
            result,
            text("2024-01-01 13:00:00.000"),
            "Standard chaining failed"
        );
    }

    #[test]
    fn test_subsec_rollover_math() {
        let result = exec_datetime(
            &[
                text("2024-01-01 12:00:00.999"),
                text("+1 second"),
                text("subsec"),
            ],
            DateTimeOutput::DateTime,
        );
        assert_eq!(
            result,
            text("2024-01-01 12:00:01.999"),
            "Rollover math with milliseconds failed"
        );
    }

    #[test]
    fn test_subsec_case_insensitivity() {
        let result = exec_datetime(
            &[text("2024-01-01 12:00:00"), text("SuBsEc")],
            DateTimeOutput::DateTime,
        );
        assert_eq!(
            result,
            text("2024-01-01 12:00:00.000"),
            "Case insensitivity check failed"
        );
    }

    #[test]
    fn test_parse_modifier_unicode_no_panic() {
        // Regression test: parse_modifier should not panic on multi-byte UTF-8 strings
        // that are shorter than expected modifier suffixes when measured in bytes
        let unicode_inputs = [
            "!*\u{ea37}", // <-- this produced a crash in SQLancer :]
            "\u{1F600}",  // Emoji (4 bytes)
            "日本語",     // Japanese text
            "中",         // Single Chinese character
            "\u{0080}",   // 2-byte UTF-8
            "",           // Empty string
        ];

        for input in unicode_inputs {
            // Should not panic - just return an error for invalid modifiers
            let result = parse_modifier(input);
            assert!(
                result.is_err(),
                "Expected error for invalid modifier: {input}"
            );
        }
    }

    #[test]
    fn test_unixepoch_basic_usage() {
        let result = exec_unixepoch(&[text("1970-01-01 00:00:00")]);
        assert_eq!(result, Value::Integer(0));

        let result = exec_unixepoch(&[text("2023-01-01 00:00:00")]);
        assert_eq!(result, Value::Integer(1672531200));

        let result = exec_unixepoch(&[text("1969-12-31 23:59:59")]);
        assert_eq!(result, Value::Integer(-1));

        let result = exec_unixepoch(&[Value::Float(2440587.5)]);
        assert_eq!(result, Value::Integer(0));
    }

    #[test]
    fn test_unixepoch_numeric_modifiers_unixepoch() {
        let result = exec_unixepoch(&[Value::Integer(1672531200), text("unixepoch")]);
        assert_eq!(result, Value::Integer(1672531200));

        let result = exec_unixepoch(&[Value::Integer(0), text("unixepoch")]);
        assert_eq!(result, Value::Integer(0));

        let result = exec_unixepoch(&[
            Value::Integer(1672531200),
            text("unixepoch"),
            text("start of year"),
        ]);
        assert_eq!(result, Value::Integer(1672531200));
    }

    #[test]
    fn test_unixepoch_numeric_modifiers_julianday() {
        let result = exec_unixepoch(&[Value::Float(2440587.5), text("julianday")]);
        assert_eq!(result, Value::Integer(0));

        let result = exec_unixepoch(&[Value::Float(2460311.5), text("julianday")]);
        assert_eq!(result, Value::Integer(1704153600));

        let result = exec_unixepoch(&[Value::Float(0.0), text("julianday")]);
        match result {
            Value::Integer(i) => assert_eq!(i, -210866760000),
            _ => panic!("Expected Integer result for JD 0"),
        }
    }

    #[test]
    fn test_unixepoch_numeric_modifiers_auto() {
        let result = exec_unixepoch(&[Value::Float(2440587.5), text("auto")]);
        assert_eq!(result, Value::Integer(0));

        let result = exec_unixepoch(&[Value::Integer(1672531200), text("auto")]);
        assert_eq!(result, Value::Integer(1672531200));

        let result = exec_unixepoch(&[Value::Float(0.0), text("auto")]);
        match result {
            Value::Integer(i) => assert!(i < 0, "Expected JD interpretation (negative), got {i}"),
            _ => panic!("Expected Integer result"),
        }
    }

    #[test]
    fn test_unixepoch_invalid_usage() {
        let result = exec_unixepoch(&[Value::Integer(0), text("start of year"), text("unixepoch")]);
        assert_eq!(result, Value::Null);

        let result = exec_unixepoch(&[text("2023-01-01"), text("unixepoch")]);
        assert_eq!(result, Value::Null);

        let result = exec_unixepoch(&[Value::Integer(0), text("unixepoch"), text("julianday")]);
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_unixepoch_complex_calculations() {
        let result = exec_unixepoch(&[Value::Float(2440587.5), text("julianday"), text("+1 day")]);
        assert_eq!(result, Value::Integer(86400));

        let result = exec_unixepoch(&[
            Value::Float(2460311.5),
            text("auto"),
            text("start of month"),
            text("+1 month"),
        ]);
        assert_eq!(result, Value::Integer(1706745600));
    }

    #[test]
    fn test_unixepoch_subsecond_precision() {
        let result = exec_unixepoch(&[text("1970-01-01 00:00:00.0006"), text("subsec")]);
        match result {
            Value::Float(f) => assert!((f - 0.001).abs() < f64::EPSILON),
            _ => panic!("Expected Float result"),
        }
        let result = exec_unixepoch(&[text("1970-01-01 00:00:00.9996"), text("subsec")]);
        match result {
            Value::Float(f) => assert!((f - 0.999).abs() < f64::EPSILON),
            _ => panic!("Expected Float result"),
        }
    }
}
