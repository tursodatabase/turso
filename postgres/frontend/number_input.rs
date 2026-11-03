use crate::catalog::PostgresDialect;
use crate::error::sqlstate_error;
use turso_core::{CastTypeTag, Dialect, LimboError, Result as CoreResult, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_FEATURE_NOT_SUPPORTED, SQLSTATE_INVALID_TEXT_REPRESENTATION,
    SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
};

const MAX_EXACT_INTEGER: u128 = 1 << 53;
const MAX_EXACT_DIGITS: usize = 15;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PgNumberType {
    Int2,
    Int4,
    Int8,
    Real,
    Double,
    Numeric,
}

/// A number that PostgreSQL accepts as input for a numeric type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum PgNumber {
    Integer(i64),
    Float(f64),
    Unsupported(Unsupported),
}

/// Valid PostgreSQL input that the engine cannot hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unsupported {
    NaN,
    NotExactInFloat,
    HexFloat,
}

pub(crate) type InputError = (String, &'static str);

impl PgNumberType {
    pub(crate) fn from_cast_tag(tag: CastTypeTag) -> Option<Self> {
        match tag {
            CastTypeTag::Int2 => Some(Self::Int2),
            CastTypeTag::Int4 => Some(Self::Int4),
            CastTypeTag::Int8 => Some(Self::Int8),
            CastTypeTag::Real => Some(Self::Real),
            CastTypeTag::Double => Some(Self::Double),
            CastTypeTag::Numeric => Some(Self::Numeric),
            CastTypeTag::Other => None,
        }
    }

    pub(crate) fn from_type_name(name: &str) -> Option<Self> {
        Self::from_cast_tag(PostgresDialect.cast_type_tag(name))
    }

    fn name(self) -> &'static str {
        match self {
            Self::Int2 => "smallint",
            Self::Int4 => "integer",
            Self::Int8 => "bigint",
            Self::Real => "real",
            Self::Double => "double precision",
            Self::Numeric => "numeric",
        }
    }

    fn integer_range(self) -> Option<(i64, i64)> {
        match self {
            Self::Int2 => Some((i16::MIN.into(), i16::MAX.into())),
            Self::Int4 => Some((i32::MIN.into(), i32::MAX.into())),
            Self::Int8 => Some((i64::MIN, i64::MAX)),
            Self::Real | Self::Double | Self::Numeric => None,
        }
    }
}

impl Unsupported {
    fn into_error(self, input: &str, ty: PgNumberType) -> LimboError {
        let message = match self {
            Self::NaN => format!("NaN is not supported for type {}", ty.name()),
            Self::NotExactInFloat => format!(
                "value \"{input}\" is not supported for type {}: it needs more precision or range than a 64-bit float",
                ty.name()
            ),
            Self::HexFloat => format!(
                "hexadecimal floating point input \"{input}\" is not supported for type {}",
                ty.name()
            ),
        };
        sqlstate_error(SQLSTATE_FEATURE_NOT_SUPPORTED, message)
    }
}

pub(crate) fn cast_text_to_number(input: &str, ty: PgNumberType) -> CoreResult<Value> {
    match parse_pg_number(input, ty) {
        Ok(PgNumber::Integer(value)) => Ok(Value::from_i64(value)),
        Ok(PgNumber::Float(value)) => Ok(Value::from_f64(value)),
        Ok(PgNumber::Unsupported(reason)) => Err(reason.into_error(input, ty)),
        Err((message, code)) => Err(sqlstate_error(code, message)),
    }
}

pub(crate) fn cast_integer_to_number(value: i64, ty: PgNumberType) -> CoreResult<Value> {
    if let Some((min, max)) = ty.integer_range() {
        if !(min..=max).contains(&value) {
            return Err(out_of_range_error(ty));
        }
        return Ok(Value::from_i64(value));
    }
    if ty == PgNumberType::Numeric && u128::from(value.unsigned_abs()) > MAX_EXACT_INTEGER {
        return Err(Unsupported::NotExactInFloat.into_error(&value.to_string(), ty));
    }
    Ok(Value::from_f64(value as f64))
}

pub(crate) fn cast_float_to_number(value: f64, ty: PgNumberType) -> CoreResult<Value> {
    if let Some((min, max)) = ty.integer_range() {
        let rounded = value.round();
        if !(rounded >= min as f64 && rounded < max as f64 + 1.0) {
            return Err(out_of_range_error(ty));
        }
        return Ok(Value::from_i64(rounded as i64));
    }
    if ty == PgNumberType::Real && value.is_finite() && (value as f32).is_infinite() {
        return Err(sqlstate_error(
            SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
            "value out of range: overflow",
        ));
    }
    Ok(Value::from_f64(value))
}

fn out_of_range_error(ty: PgNumberType) -> LimboError {
    sqlstate_error(
        SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
        format!("{} out of range", ty.name()),
    )
}

/// Parses the text of a PostgreSQL numeric input, giving the value that a
/// cast stores or the SQLSTATE and message of the error.
pub(crate) fn parse_pg_number(input: &str, ty: PgNumberType) -> Result<PgNumber, InputError> {
    let text = input.trim_matches(is_pg_space);
    if ty.integer_range().is_some() {
        parse_integer(input, text, ty)
    } else {
        parse_fractional(input, text, ty)
    }
}

fn parse_integer(input: &str, text: &str, ty: PgNumberType) -> Result<PgNumber, InputError> {
    let (negative, unsigned) = split_sign(text);
    let (radix, digits) = scan_integer(unsigned).ok_or_else(|| invalid_input(input, ty))?;
    let magnitude = u128::from_str_radix(&digits, radix)
        .ok()
        .and_then(|magnitude| i128::try_from(magnitude).ok());
    let (min, max) = ty.integer_range().expect("integer types have a range");
    let value = magnitude.map(|magnitude| if negative { -magnitude } else { magnitude });
    match value {
        Some(value) if (i128::from(min)..=i128::from(max)).contains(&value) => {
            Ok(PgNumber::Integer(value as i64))
        }
        _ => Err((
            format!("value \"{input}\" is out of range for type {}", ty.name()),
            SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
        )),
    }
}

fn parse_fractional(input: &str, text: &str, ty: PgNumberType) -> Result<PgNumber, InputError> {
    let is_numeric = ty == PgNumberType::Numeric;
    let (negative, unsigned) = split_sign(text);
    let signed = unsigned.len() != text.len();
    let lowercase = unsigned.to_ascii_lowercase();
    let sign = if negative { -1.0 } else { 1.0 };

    if lowercase == "nan" {
        if is_numeric && signed {
            return Err(invalid_input(input, ty));
        }
        return Ok(PgNumber::Unsupported(Unsupported::NaN));
    }
    if lowercase == "inf" || lowercase == "infinity" {
        return Ok(PgNumber::Float(sign * f64::INFINITY));
    }

    if let Some((radix, digits)) = scan_prefixed_integer(unsigned) {
        if !is_numeric && (radix != 16 || unsigned.contains('_')) {
            return Err(invalid_input(input, ty));
        }
        return match u128::from_str_radix(&digits, radix) {
            Ok(magnitude) if magnitude <= MAX_EXACT_INTEGER || !is_numeric => {
                Ok(PgNumber::Float(sign * magnitude as f64))
            }
            _ => Ok(PgNumber::Unsupported(Unsupported::NotExactInFloat)),
        };
    }
    if lowercase.starts_with("0x") && !is_numeric {
        return if lowercase.contains(['.', 'p']) {
            Ok(PgNumber::Unsupported(Unsupported::HexFloat))
        } else {
            Err(invalid_input(input, ty))
        };
    }

    let decimal = scan_decimal(&lowercase, is_numeric).ok_or_else(|| invalid_input(input, ty))?;
    let value: f64 = decimal
        .canonical
        .parse()
        .map_err(|_| invalid_input(input, ty))?;
    let lost_to_zero = value == 0.0 && decimal.significant_digits > 0;
    match ty {
        PgNumberType::Numeric => {
            if !value.is_finite() || lost_to_zero || decimal.significant_digits > MAX_EXACT_DIGITS {
                return Ok(PgNumber::Unsupported(Unsupported::NotExactInFloat));
            }
        }
        PgNumberType::Real => {
            let single: f32 = decimal
                .canonical
                .parse()
                .map_err(|_| invalid_input(input, ty))?;
            if single.is_infinite() || (single == 0.0 && decimal.significant_digits > 0) {
                return Err(float_range_error(input, ty));
            }
        }
        _ => {
            if !value.is_finite() || lost_to_zero {
                return Err(float_range_error(input, ty));
            }
        }
    }
    Ok(PgNumber::Float(sign * value))
}

fn invalid_input(input: &str, ty: PgNumberType) -> InputError {
    (
        format!("invalid input syntax for type {}: \"{input}\"", ty.name()),
        SQLSTATE_INVALID_TEXT_REPRESENTATION,
    )
}

fn float_range_error(input: &str, ty: PgNumberType) -> InputError {
    (
        format!("\"{input}\" is out of range for type {}", ty.name()),
        SQLSTATE_NUMERIC_VALUE_OUT_OF_RANGE,
    )
}

fn is_pg_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\u{b}' | '\u{c}' | '\r')
}

fn split_sign(text: &str) -> (bool, &str) {
    match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    }
}

/// The radix and digits of an integer such as 12, 1_000, 0x1F, 0o17 or 0b101.
fn scan_integer(unsigned: &str) -> Option<(u32, String)> {
    match scan_prefixed_integer(unsigned) {
        Some(found) => Some(found),
        None if has_radix_prefix(unsigned) => None,
        None => strip_underscores(unsigned, 10, false).map(|digits| (10, digits)),
    }
}

fn scan_prefixed_integer(unsigned: &str) -> Option<(u32, String)> {
    let radix = match unsigned.as_bytes() {
        [b'0', b'x' | b'X', ..] => 16,
        [b'0', b'o' | b'O', ..] => 8,
        [b'0', b'b' | b'B', ..] => 2,
        _ => return None,
    };
    strip_underscores(&unsigned[2..], radix, true).map(|digits| (radix, digits))
}

fn has_radix_prefix(unsigned: &str) -> bool {
    matches!(
        unsigned.as_bytes(),
        [b'0', b'x' | b'X' | b'o' | b'O' | b'b' | b'B', ..]
    )
}

/// The digits of `text` without the single underscores between digits.
fn strip_underscores(text: &str, radix: u32, allow_leading: bool) -> Option<String> {
    let mut digits = String::new();
    let mut previous_underscore = false;
    for (index, c) in text.chars().enumerate() {
        if c == '_' {
            if previous_underscore || (index == 0 && !allow_leading) {
                return None;
            }
            previous_underscore = true;
        } else if c.is_digit(radix) {
            digits.push(c);
            previous_underscore = false;
        } else {
            return None;
        }
    }
    (!previous_underscore && !digits.is_empty()).then_some(digits)
}

struct DecimalText {
    canonical: String,
    significant_digits: usize,
}

/// Reads decimal text such as 1.5, .5e3 or (for numeric) 1_000.5, without
/// a sign.
fn scan_decimal(text: &str, allow_underscores: bool) -> Option<DecimalText> {
    let (mantissa, exponent) = match text.split_once('e') {
        Some((mantissa, exponent)) => (mantissa, Some(exponent)),
        None => (text, None),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    let digit_group = |group: &str| {
        if group.is_empty() {
            Some(String::new())
        } else if group.contains('_') && !allow_underscores {
            None
        } else {
            strip_underscores(group, 10, false)
        }
    };
    let whole = digit_group(whole)?;
    let fraction = digit_group(fraction)?;
    let exponent = match exponent {
        None => "0".to_string(),
        Some(exponent) => {
            let (sign, digits) = match exponent.as_bytes().first() {
                Some(b'-') => ("-", &exponent[1..]),
                Some(b'+') => ("", &exponent[1..]),
                _ => ("", exponent),
            };
            if digits.contains('_') && !allow_underscores {
                return None;
            }
            format!("{sign}{}", strip_underscores(digits, 10, false)?)
        }
    };
    let all_digits = format!("{whole}{fraction}");
    let significant_digits = all_digits
        .trim_start_matches('0')
        .trim_end_matches('0')
        .len();
    Some(DecimalText {
        canonical: format!(
            "{}.{}e{exponent}",
            if whole.is_empty() { "0" } else { &whole },
            if fraction.is_empty() { "0" } else { &fraction },
        ),
        significant_digits,
    })
}
