use std::num::{IntErrorKind, ParseIntError};

use turso_core::{LimboError, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_INVALID_TEXT_REPRESENTATION, SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
};

use crate::error::sqlstate_error;

const VERTICAL_TAB: char = '\u{b}';

#[derive(Debug, Copy, Clone)]
#[repr(i64)]
pub enum IntegerWidth {
    Int16 = 16,
    Int32 = 32,
    Int64 = 64,
}

impl TryFrom<i64> for IntegerWidth {
    type Error = &'static str;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            16 => Ok(IntegerWidth::Int16),
            32 => Ok(IntegerWidth::Int32),
            64 => Ok(IntegerWidth::Int64),
            _ => Err("Invalid integer width"),
        }
    }
}

pub fn cast_value_to_integer(value: &Value, width: IntegerWidth) -> Result<Value> {
    match value {
        Value::Text(raw) => Ok(Value::from_i64(str_to_integer(raw.as_str(), width)?)),

        Value::Numeric(_) => {
            if let Some(integer) = value.as_int() {
                Ok(Value::from_i64(check_integer_bounds(integer, width)?))
            } else {
                let float = value.as_float();
                let rounded = float.round();

                if !rounded.is_finite()
                    || rounded < i64::MIN as f64
                    || rounded >= 9223372036854775808.0
                {
                    return Err(integer_out_of_range_error(width));
                }

                Ok(Value::from_i64(check_integer_bounds(
                    rounded as i64,
                    width,
                )?))
            }
        }

        Value::Null => Ok(Value::Null),

        Value::Blob(_) => {
            let result = value.exec_cast("INTEGER")?;
            let integer = result
                .as_int()
                .ok_or_else(|| integer_out_of_range_error(width))?;
            Ok(Value::from_i64(check_integer_bounds(integer, width)?))
        }
    }
}

fn check_integer_bounds(value: i64, width: IntegerWidth) -> Result<i64> {
    let in_range = match width {
        IntegerWidth::Int16 => (i16::MIN as i64..=i16::MAX as i64).contains(&value),
        IntegerWidth::Int32 => (i32::MIN as i64..=i32::MAX as i64).contains(&value),
        IntegerWidth::Int64 => true,
    };

    if in_range {
        Ok(value)
    } else {
        Err(integer_out_of_range_error(width))
    }
}

fn integer_out_of_range_error(width: IntegerWidth) -> LimboError {
    match width {
        IntegerWidth::Int16 => {
            sqlstate_error(SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE, "smallint out of range")
        }
        IntegerWidth::Int32 => {
            sqlstate_error(SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE, "integer out of range")
        }
        IntegerWidth::Int64 => {
            sqlstate_error(SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE, "bigint out of range")
        }
    }
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

    if radix != 10 {
        if clean.starts_with(['+', '-']) {
            return Err(i64::from_str_radix("_", radix).unwrap_err());
        }

        if clean.starts_with('_') {
            clean = &clean[1..];
        }
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
