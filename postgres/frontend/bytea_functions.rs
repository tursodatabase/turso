//! The functions of bytea, as PostgreSQL's `bytea.c` defines them.

use crate::error::sqlstate_error;
use turso_core::{Register, Result, Value};
use turso_pg_parser::sqlstate::{
    SQLSTATE_INTERNAL_ERROR, SQLSTATE_INVALID_TEXT_REPRESENTATION, SQLSTATE_SUBSTRING_ERROR,
};

/// The bytes of argument `index`, or `None` for NULL. A string literal that the
/// resolver read as bytea arrives as text and is read as bytea input.
pub(crate) fn bytea_arg(args: &[Register], index: usize) -> Result<Option<Vec<u8>>> {
    match args[index].get_value() {
        Value::Null => Ok(None),
        Value::Blob(bytes) => Ok(Some(bytes.to_vec())),
        Value::Text(text) => read_bytea_input(text.as_str()).map(Some),
        other => Err(sqlstate_error(
            SQLSTATE_INTERNAL_ERROR,
            format!("a bytea function got {other}, which is not bytea"),
            None,
        )),
    }
}

pub(crate) fn integer_arg(args: &[Register], index: usize) -> Result<Option<i64>> {
    match args[index].get_value() {
        Value::Null => Ok(None),
        value => value.as_int().map(Some).ok_or_else(|| {
            sqlstate_error(
                SQLSTATE_INTERNAL_ERROR,
                format!("a bytea function got {value}, which is not an integer"),
                None,
            )
        }),
    }
}

pub(crate) fn bytea_octet_length(args: &[Register]) -> Result<Option<i64>> {
    Ok(bytea_arg(args, 0)?.map(|bytes| bytes.len() as i64))
}

pub(crate) fn bytea_reverse(args: &[Register]) -> Result<Option<Value>> {
    Ok(bytea_arg(args, 0)?.map(|mut bytes| {
        bytes.reverse();
        Value::from_blob(bytes)
    }))
}

pub(crate) fn bytea_trim(args: &[Register], start: bool, end: bool) -> Result<Option<Value>> {
    let (Some(bytes), Some(set)) = (bytea_arg(args, 0)?, bytea_arg(args, 1)?) else {
        return Ok(None);
    };
    let mut from = 0;
    let mut to = bytes.len();
    if start {
        while from < to && set.contains(&bytes[from]) {
            from += 1;
        }
    }
    if end {
        while to > from && set.contains(&bytes[to - 1]) {
            to -= 1;
        }
    }
    Ok(Some(Value::from_blob(bytes[from..to].to_vec())))
}

pub(crate) fn bytea_substring(args: &[Register], with_length: bool) -> Result<Option<Value>> {
    let Some(bytes) = bytea_arg(args, 0)? else {
        return Ok(None);
    };
    let Some(start) = integer_arg(args, 1)? else {
        return Ok(None);
    };
    let length = if with_length {
        match integer_arg(args, 2)? {
            Some(length) => Some(length),
            None => return Ok(None),
        }
    } else {
        None
    };
    let Some((from, to)) = substring_range(bytes.len(), start, length)? else {
        return Ok(Some(Value::from_blob(Vec::new())));
    };
    Ok(Some(Value::from_blob(bytes[from..to].to_vec())))
}

/// The zero-based range `from..to` that `substring(value, start, length)` keeps
/// out of `size` items, or `None` for the empty result.
pub(crate) fn substring_range(
    size: usize,
    start: i64,
    length: Option<i64>,
) -> Result<Option<(usize, usize)>> {
    let size = size as i64;
    let end = match length {
        Some(length) if length < 0 => {
            return Err(sqlstate_error(
                SQLSTATE_SUBSTRING_ERROR,
                "negative substring length not allowed",
                None,
            ))
        }
        Some(length) => start.saturating_add(length),
        None => i64::MAX,
    };
    let from = start.max(1);
    let to = end.min(size.saturating_add(1));
    if from > size || to <= from {
        return Ok(None);
    }
    Ok(Some((from as usize - 1, to as usize - 1)))
}

/// Reads the text form of a bytea: `\x` followed by hex digits, or the escape
/// form with `\\` and `\` followed by three octal digits.
fn read_bytea_input(text: &str) -> Result<Vec<u8>> {
    let invalid =
        |detail: String| sqlstate_error(SQLSTATE_INVALID_TEXT_REPRESENTATION, detail, None);
    if let Some(hex) = text.strip_prefix("\\x") {
        let digits: Vec<u8> = hex
            .bytes()
            .filter(|byte| !byte.is_ascii_whitespace())
            .collect();
        let mut bytes = Vec::with_capacity(digits.len() / 2);
        let digit_value = |digit: u8| {
            (digit as char)
                .to_digit(16)
                .ok_or_else(|| invalid(format!("invalid hexadecimal digit: \"{}\"", digit as char)))
        };
        for pair in digits.chunks(2) {
            let [high, low] = pair else {
                return Err(invalid(
                    "invalid hexadecimal data: odd number of digits".to_string(),
                ));
            };
            bytes.push((digit_value(*high)? * 16 + digit_value(*low)?) as u8);
        }
        return Ok(bytes);
    }
    let input = text.as_bytes();
    let mut bytes = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        if input[index] != b'\\' {
            bytes.push(input[index]);
            index += 1;
        } else if input.get(index + 1) == Some(&b'\\') {
            bytes.push(b'\\');
            index += 2;
        } else if let Some(octal) = input.get(index + 1..index + 4).filter(|octal| {
            octal[0] <= b'3' && octal.iter().all(|digit| (b'0'..=b'7').contains(digit))
        }) {
            bytes.push(
                octal
                    .iter()
                    .fold(0, |value, digit| value * 8 + (digit - b'0')),
            );
            index += 4;
        } else {
            return Err(invalid("invalid input syntax for type bytea".to_string()));
        }
    }
    Ok(bytes)
}
