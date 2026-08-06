// Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.

//! The `DateStyle` configuration parameter: how dates and timestamps print.
//!
//! A DateStyle value is two independent settings spelled as one
//! comma-separated string: an output format (`ISO`, `SQL`, `Postgres`,
//! `German`) and a field order (`MDY`, `DMY`, `YMD`). Either half may appear
//! alone, in which case the other keeps its current value, and `SHOW` always
//! renders both. Only `DMY` changes the output — the other two orders print
//! the month before the day.
//!
//! The engine always hands the wire layer ISO text, so the rewriting here is
//! purely presentational and mirrors PostgreSQL's `EncodeDateOnly` and
//! `EncodeDateTime`. Anything that is not recognizable ISO text is passed
//! through untouched: printing a value we do not understand unchanged is
//! always better than mangling it.

/// The output-format half of DateStyle.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DateFormat {
    #[default]
    Iso,
    Sql,
    Postgres,
    German,
}

/// The field-order half of DateStyle. It decides how ambiguous input is read
/// and, for the non-ISO formats, whether the day or the month prints first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DateOrder {
    #[default]
    Mdy,
    Dmy,
    Ymd,
}

/// A parsed DateStyle setting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DateStyle {
    pub format: DateFormat,
    pub order: DateOrder,
}

const DAY_NAMES: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTH_NAMES: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

impl DateStyle {
    /// Parses a DateStyle value on top of the values already in effect, the
    /// way PostgreSQL's `check_datestyle` does: keywords in any order, at
    /// most one format and one order, plus the historic aliases (`US` and
    /// `NonEuropean` for MDY, `European` for DMY). `German` implies DMY
    /// unless an order is given explicitly. Returns None for an unknown
    /// keyword or two conflicting formats, which the caller reports as
    /// PostgreSQL's "invalid value for parameter" error.
    pub fn parse(current: Self, value: &str) -> Option<Self> {
        let mut style = current;
        let mut have_format = false;
        let mut have_order = false;
        for token in value.split(',') {
            let token = token.trim().to_lowercase();
            let (format, order) = match token.as_str() {
                "iso" => (Some(DateFormat::Iso), None),
                "sql" => (Some(DateFormat::Sql), None),
                // PostgreSQL matches this one by prefix, so "postgresql"
                // and "postgres_verbose" are accepted spellings too.
                t if t.starts_with("postgres") => (Some(DateFormat::Postgres), None),
                "german" => (Some(DateFormat::German), None),
                "ymd" => (None, Some(DateOrder::Ymd)),
                "dmy" => (None, Some(DateOrder::Dmy)),
                t if t.starts_with("euro") => (None, Some(DateOrder::Dmy)),
                "mdy" | "us" => (None, Some(DateOrder::Mdy)),
                t if t.starts_with("noneuro") => (None, Some(DateOrder::Mdy)),
                // DEFAULT resets both halves to the session defaults.
                "default" => (Some(DateFormat::Iso), Some(DateOrder::Mdy)),
                _ => return None,
            };
            if let Some(format) = format {
                if have_format && style.format != format {
                    return None;
                }
                style.format = format;
                have_format = true;
                // German is a German-language format, so it brings the
                // German field order with it unless one was asked for.
                if format == DateFormat::German && !have_order {
                    style.order = DateOrder::Dmy;
                }
            }
            if let Some(order) = order {
                style.order = order;
                have_order = true;
            }
        }
        Some(style)
    }

    /// The value `SHOW DateStyle` displays: both halves, always.
    pub fn canonical(&self) -> String {
        let format = match self.format {
            DateFormat::Iso => "ISO",
            DateFormat::Sql => "SQL",
            DateFormat::Postgres => "Postgres",
            DateFormat::German => "German",
        };
        let order = match self.order {
            DateOrder::Mdy => "MDY",
            DateOrder::Dmy => "DMY",
            DateOrder::Ymd => "YMD",
        };
        format!("{format}, {order}")
    }

    /// Rewrites a `date` value the engine rendered as `YYYY-MM-DD`, or None
    /// when it already prints correctly (ISO) or is not that form.
    pub fn reformat_date(&self, text: &str) -> Option<String> {
        if self.format == DateFormat::Iso {
            return None;
        }
        let parts = IsoParts::parse(text)?;
        // A date carrying a time of day is not something PostgreSQL's
        // date_out can produce, so leave it alone rather than drop the time.
        if parts.time.is_some() {
            return None;
        }
        Some(self.render_date(&parts))
    }

    /// Rewrites a `timestamp` or `timestamptz` value the engine rendered as
    /// `YYYY-MM-DD HH:MM:SS[.fff][zone]`, or None when it already prints
    /// correctly (ISO) or is not that form.
    pub fn reformat_timestamp(&self, text: &str) -> Option<String> {
        if self.format == DateFormat::Iso {
            return None;
        }
        let parts = IsoParts::parse(text)?;
        let time = parts.time?;
        let mut out = match self.format {
            // Postgres format leads with the weekday and trails with the
            // year: "Mon Feb 10 17:32:01 1997".
            DateFormat::Postgres => {
                let month = MONTH_NAMES[parts.month as usize - 1];
                let day_of_week = DAY_NAMES[weekday_index(parts.year, parts.month, parts.day)?];
                let day_and_month = if self.order == DateOrder::Dmy {
                    format!("{:02} {month}", parts.day)
                } else {
                    format!("{month} {:02}", parts.day)
                };
                format!(
                    "{day_of_week} {day_and_month} {time} {:04}",
                    parts.year.abs()
                )
            }
            _ => format!("{} {time}", self.render_date(&parts)),
        };
        if let Some(zone) = parts.zone {
            out.push_str(&render_zone(zone));
        }
        Some(out)
    }

    /// The date half of a value, in this style's format. ISO never reaches
    /// here: its callers return early so unchanged values cost no allocation.
    fn render_date(&self, parts: &IsoParts) -> String {
        let year = parts.year.abs();
        let (month, day) = (parts.month, parts.day);
        match self.format {
            // German is always day.month.year.
            DateFormat::German => format!("{day:02}.{month:02}.{year:04}"),
            // SQL and Postgres differ only in the separator, and put the day
            // first for DMY. YMD prints like MDY, as PostgreSQL does.
            DateFormat::Sql | DateFormat::Postgres => {
                let separator = if self.format == DateFormat::Sql {
                    '/'
                } else {
                    '-'
                };
                let (first, second) = if self.order == DateOrder::Dmy {
                    (day, month)
                } else {
                    (month, day)
                };
                format!("{first:02}{separator}{second:02}{separator}{year:04}")
            }
            DateFormat::Iso => format!("{year:04}-{month:02}-{day:02}"),
        }
    }
}

/// A time zone the way the non-ISO formats print it: PostgreSQL prints the
/// zone's abbreviation when it has one, and only the sessions we serve are
/// UTC. Any other offset keeps its ISO spelling, which is what PostgreSQL
/// falls back to for a zone with no abbreviation.
fn render_zone(zone: &str) -> String {
    match zone {
        "Z" | "+00" | "+0000" | "+00:00" | "-00" | "-00:00" => " UTC".to_string(),
        other => other.to_string(),
    }
}

/// Day of week as an index into [`DAY_NAMES`], or None for a date that does
/// not exist.
fn weekday_index(year: i32, month: u32, day: u32) -> Option<usize> {
    let date = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
    Some(chrono::Datelike::weekday(&date).num_days_from_sunday() as usize)
}

/// The pieces of the engine's ISO rendering of a date or timestamp.
struct IsoParts<'a> {
    year: i32,
    month: u32,
    day: u32,
    /// `HH:MM:SS` with any fractional seconds, kept verbatim: every format
    /// prints the time of day the same way.
    time: Option<&'a str>,
    zone: Option<&'a str>,
}

impl<'a> IsoParts<'a> {
    fn parse(text: &'a str) -> Option<Self> {
        let (date, rest) = text.split_at_checked(10)?;
        let mut fields = date.split('-');
        let year: i32 = parse_fixed_digits(fields.next()?, 4)?;
        let month: u32 = parse_fixed_digits(fields.next()?, 2)?;
        let day: u32 = parse_fixed_digits(fields.next()?, 2)?;
        if fields.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            return None;
        }
        if rest.is_empty() {
            return Some(Self {
                year,
                month,
                day,
                time: None,
                zone: None,
            });
        }
        // A timestamp separates the time of day with a space or a `T`.
        let time = rest.strip_prefix(' ').or_else(|| rest.strip_prefix('T'))?;
        let (time, zone) = split_zone(time);
        Some(Self {
            year,
            month,
            day,
            time: Some(time),
            zone,
        })
    }
}

/// Splits a time of day from its trailing time zone, if it has one.
fn split_zone(time: &str) -> (&str, Option<&str>) {
    if let Some(head) = time.strip_suffix('Z') {
        return (head, Some("Z"));
    }
    match time.rfind(['+', '-']) {
        Some(at) => (&time[..at], Some(&time[at..])),
        None => (time, None),
    }
}

/// Parses exactly `width` decimal digits, so partially numeric text (a
/// `text` column that happens to start with digits) is not mistaken for a
/// date.
fn parse_fixed_digits<T: std::str::FromStr>(field: &str, width: usize) -> Option<T> {
    if field.len() != width || !field.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    field.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ISO_MDY: DateStyle = DateStyle {
        format: DateFormat::Iso,
        order: DateOrder::Mdy,
    };

    fn parse(value: &str) -> DateStyle {
        DateStyle::parse(ISO_MDY, value).expect("value is valid")
    }

    #[test]
    fn both_halves_always_display() {
        assert_eq!(parse("SQL").canonical(), "SQL, MDY");
        assert_eq!(parse("DMY").canonical(), "ISO, DMY");
        assert_eq!(parse("Postgres, DMY").canonical(), "Postgres, DMY");
    }

    #[test]
    fn german_brings_the_german_field_order() {
        assert_eq!(parse("German").canonical(), "German, DMY");
        assert_eq!(parse("German, MDY").canonical(), "German, MDY");
        assert_eq!(parse("MDY, German").canonical(), "German, MDY");
    }

    #[test]
    fn historic_aliases_are_accepted() {
        assert_eq!(parse("US, Postgres").canonical(), "Postgres, MDY");
        assert_eq!(parse("European, ISO").canonical(), "ISO, DMY");
        assert_eq!(parse("NonEuropean").canonical(), "ISO, MDY");
    }

    #[test]
    fn a_lone_order_keeps_the_current_format() {
        let german = parse("German");
        assert_eq!(
            DateStyle::parse(german, "MDY").unwrap().canonical(),
            "German, MDY"
        );
    }

    #[test]
    fn unknown_and_conflicting_values_are_rejected() {
        assert_eq!(DateStyle::parse(ISO_MDY, "garbage"), None);
        assert_eq!(DateStyle::parse(ISO_MDY, "ISO, SQL"), None);
        assert_eq!(DateStyle::parse(ISO_MDY, "ISO, ISO"), Some(ISO_MDY));
    }

    #[test]
    fn dates_render_per_format() {
        assert_eq!(parse("ISO").reformat_date("1957-04-09"), None);
        for (value, expected) in [
            ("Postgres, MDY", "04-09-1957"),
            ("Postgres, DMY", "09-04-1957"),
            ("SQL, MDY", "04/09/1957"),
            ("SQL, DMY", "09/04/1957"),
            ("SQL, YMD", "04/09/1957"),
            ("German", "09.04.1957"),
        ] {
            assert_eq!(
                parse(value).reformat_date("1957-04-09").as_deref(),
                Some(expected),
                "DateStyle {value}"
            );
        }
    }

    #[test]
    fn timestamps_render_per_format() {
        for (value, expected) in [
            ("Postgres, MDY", "Mon Feb 10 17:32:01 1997"),
            ("Postgres, DMY", "Mon 10 Feb 17:32:01 1997"),
            ("SQL, MDY", "02/10/1997 17:32:01"),
            ("SQL, DMY", "10/02/1997 17:32:01"),
            ("German", "10.02.1997 17:32:01"),
        ] {
            assert_eq!(
                parse(value)
                    .reformat_timestamp("1997-02-10 17:32:01")
                    .as_deref(),
                Some(expected),
                "DateStyle {value}"
            );
        }
    }

    #[test]
    fn fractional_seconds_and_zones_survive() {
        assert_eq!(
            parse("Postgres")
                .reformat_timestamp("1997-02-10 17:32:01.4")
                .as_deref(),
            Some("Mon Feb 10 17:32:01.4 1997")
        );
        assert_eq!(
            parse("German")
                .reformat_timestamp("2001-12-27 04:05:06.789+00")
                .as_deref(),
            Some("27.12.2001 04:05:06.789 UTC")
        );
        assert_eq!(
            parse("SQL")
                .reformat_timestamp("2001-12-27 04:05:06+05:30")
                .as_deref(),
            Some("12/27/2001 04:05:06+05:30")
        );
    }

    #[test]
    fn text_that_is_not_an_iso_timestamp_is_left_alone() {
        let style = parse("Postgres");
        assert_eq!(style.reformat_date("not a date"), None);
        assert_eq!(style.reformat_date("197-04-09"), None);
        assert_eq!(style.reformat_date("1957-13-09"), None);
        assert_eq!(style.reformat_date("1957-04-09 10:00:00"), None);
        assert_eq!(style.reformat_timestamp("1957-04-09"), None);
        assert_eq!(style.reformat_timestamp("infinity"), None);
    }
}
