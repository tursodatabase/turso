use std::num::{IntErrorKind, ParseIntError};

use turso_core::LimboError;
use turso_pg_parser::sqlstate::{
    SQLSTATE_INVALID_TEXT_REPRESENTATION, SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
};

use crate::error::sqlstate_error;

const VERTICAL_TAB: char = '\u{b}';

#[derive(Debug, Copy, Clone)]
pub enum IntegerWidth {
    Int16,
    Int32,
    Int64,
}

pub fn str_to_integer(input: impl AsRef<str>, width: IntegerWidth) -> Result<i64, LimboError> {
    match str_to_integer_checked(input, width) {
        Ok(val) => Ok(val),
        Err(err) => Err(map_parse_int_err_kind(err, width)),
    }
}

pub fn str_to_integer_checked(
    input: impl AsRef<str>,
    width: IntegerWidth,
) -> Result<i64, ParseIntError> {
    let mut input = input
        .as_ref()
        .trim_matches(|ch: char| ch.is_ascii_whitespace() || ch == VERTICAL_TAB);

    let sign = match input.chars().next() {
        Some(s @ ('+' | '-')) => {
            input = &input[1..];
            Some(s)
        }
        _ => None,
    };

    let (radix, mut clean) = match input.get(..2) {
        Some("0x" | "0X") => (16, &input[2..]),
        Some("0o" | "0O") => (8, &input[2..]),
        Some("0b" | "0B") => (2, &input[2..]),
        _ => (10, input),
    };

    if radix != 10 && clean.starts_with('_') {
        clean = &clean[1..];
    }

    let invalid_underscore = clean.char_indices().any(|(i, ch)| {
        if ch != '_' {
            return false;
        }

        let before = clean[..i].chars().next_back();
        let after = clean[i + 1..].chars().next();

        !matches!(before, Some(c) if c.is_digit(radix))
            || !matches!(after, Some(c) if c.is_digit(radix))
    });

    if invalid_underscore {
        return Err(i64::from_str_radix("_", radix).unwrap_err());
    }

    let clean = clean.replace('_', "");
    let native_str = match sign {
        Some(s) => format!("{s}{clean}"),
        None => clean,
    };

    match width {
        IntegerWidth::Int16 => i16::from_str_radix(&native_str, radix).map(i64::from),
        IntegerWidth::Int32 => i32::from_str_radix(&native_str, radix).map(i64::from),
        IntegerWidth::Int64 => i64::from_str_radix(&native_str, radix),
    }
}

fn map_parse_int_err_kind(err: ParseIntError, width: IntegerWidth) -> LimboError {
    match err.kind() {
        IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => match width {
            IntegerWidth::Int16 => {
                sqlstate_error(SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE, "smallint out of range")
            }
            IntegerWidth::Int32 => {
                sqlstate_error(SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE, "integer out of range")
            }
            IntegerWidth::Int64 => {
                sqlstate_error(SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE, "bigint out of range")
            }
        },
        IntErrorKind::InvalidDigit | IntErrorKind::Empty => {
            let type_name = match width {
                IntegerWidth::Int16 => "smallint",
                IntegerWidth::Int32 => "integer",
                IntegerWidth::Int64 => "bigint",
            };

            sqlstate_error(
                SQLSTATE_INVALID_TEXT_REPRESENTATION,
                &format!("invalid input syntax for type {type_name}"),
            )
        }
        _ => sqlstate_error(
            SQLSTATE_INVALID_TEXT_REPRESENTATION,
            "invalid input syntax for integer",
        ),
    }
}

pub fn str_to_i64_checked(input: impl AsRef<str>) -> Result<i64, ParseIntError> {
    let mut input = input
        .as_ref()
        .trim_matches(|ch: char| ch.is_ascii_whitespace() || ch == VERTICAL_TAB);

    let sign = match input.chars().next() {
        Some(s @ ('+' | '-')) => {
            input = &input[1..];
            Some(s)
        }
        _ => None,
    };

    let (radix, clean) = match input.get(..2) {
        Some("0x" | "0X") => (16, &input[2..]),
        Some("0o" | "0O") => (8, &input[2..]),
        Some("0b" | "0B") => (2, &input[2..]),
        _ => (10, input),
    };

    let invalid_underscore = clean.char_indices().any(|(i, ch)| {
        if ch != '_' {
            return false;
        }

        let before = clean[..i].chars().next_back();
        let after = clean[i + 1..].chars().next();

        !matches!(before, Some(c) if c.is_digit(radix))
            || !matches!(after, Some(c) if c.is_digit(radix))
    });

    if invalid_underscore {
        return Err(i64::from_str_radix("_", radix).unwrap_err());
    }

    let clean = clean.replace('_', "");

    match sign {
        Some(s) => {
            let signed = format!("{s}{clean}");
            i64::from_str_radix(&signed, radix)
        }
        None => i64::from_str_radix(&clean, radix),
    }
}
