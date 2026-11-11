//! PostgreSQL `bytea` text input.

use std::fmt;

/// Why a text is not valid `bytea` input. The messages are PostgreSQL's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ByteaInputError {
    /// A character of the hex format that is not a hex digit.
    InvalidHexDigit(char),
    /// The hex format ends in the middle of a byte.
    OddNumberOfHexDigits,
    /// A backslash of the escape format that starts neither `\\` nor an
    /// octal byte `\nnn`.
    InvalidEscape,
}

impl fmt::Display for ByteaInputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidHexDigit(c) => write!(f, "invalid hexadecimal digit: \"{c}\""),
            Self::OddNumberOfHexDigits => {
                write!(f, "invalid hexadecimal data: odd number of digits")
            }
            Self::InvalidEscape => write!(f, "invalid input syntax for type bytea"),
        }
    }
}

/// Reads `text` as PostgreSQL reads `bytea` input. Text that starts with `\x`
/// is the hex format: pairs of hex digits, with whitespace allowed between
/// pairs. Other text is the escape format: each character is its bytes,
/// except that `\\` is a backslash and `\nnn` is the byte with octal value
/// nnn.
pub fn bytea_from_text(text: &str) -> Result<Vec<u8>, ByteaInputError> {
    match text.strip_prefix("\\x") {
        Some(hex) => bytea_from_hex(hex),
        None => bytea_from_escape(text),
    }
}

fn bytea_from_hex(hex: &str) -> Result<Vec<u8>, ByteaInputError> {
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let mut chars = hex.chars();
    while let Some(c) = chars.next() {
        if matches!(c, ' ' | '\n' | '\t' | '\r') {
            continue;
        }
        let high = hex_digit(c)?;
        let low = hex_digit(chars.next().ok_or(ByteaInputError::OddNumberOfHexDigits)?)?;
        bytes.push(high << 4 | low);
    }
    Ok(bytes)
}

fn hex_digit(c: char) -> Result<u8, ByteaInputError> {
    c.to_digit(16)
        .map(|d| d as u8)
        .ok_or(ByteaInputError::InvalidHexDigit(c))
}

fn bytea_from_escape(text: &str) -> Result<Vec<u8>, ByteaInputError> {
    let input = text.as_bytes();
    let mut bytes = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        if input[i] != b'\\' {
            bytes.push(input[i]);
            i += 1;
        } else if let [b'0'..=b'3', b'0'..=b'7', b'0'..=b'7'] =
            input.get(i + 1..i + 4).unwrap_or_default()
        {
            // The first digit is at most 3, so the value fits in a byte.
            let octal = &input[i + 1..i + 4];
            bytes.push((octal[0] - b'0') << 6 | (octal[1] - b'0') << 3 | (octal[2] - b'0'));
            i += 4;
        } else if input.get(i + 1) == Some(&b'\\') {
            bytes.push(b'\\');
            i += 2;
        } else {
            return Err(ByteaInputError::InvalidEscape);
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_format() {
        assert_eq!(bytea_from_text("\\x0aff"), Ok(vec![0x0a, 0xff]));
        assert_eq!(
            bytea_from_text("\\xDE ad\n\tBE\ref"),
            Ok(vec![0xde, 0xad, 0xbe, 0xef])
        );
        assert_eq!(bytea_from_text("\\x"), Ok(vec![]));
        assert_eq!(
            bytea_from_text("\\x0a f"),
            Err(ByteaInputError::OddNumberOfHexDigits)
        );
        // Whitespace is allowed between pairs, not inside one.
        assert_eq!(
            bytea_from_text("\\x0 a"),
            Err(ByteaInputError::InvalidHexDigit(' '))
        );
        assert_eq!(
            bytea_from_text("\\x0g"),
            Err(ByteaInputError::InvalidHexDigit('g'))
        );
        assert_eq!(
            bytea_from_text("\\xé0"),
            Err(ByteaInputError::InvalidHexDigit('é'))
        );
    }

    #[test]
    fn escape_format() {
        assert_eq!(bytea_from_text("abc"), Ok(b"abc".to_vec()));
        assert_eq!(bytea_from_text(""), Ok(vec![]));
        assert_eq!(bytea_from_text("é"), Ok("é".as_bytes().to_vec()));
        assert_eq!(bytea_from_text("a\\\\b"), Ok(b"a\\b".to_vec()));
        assert_eq!(bytea_from_text("\\000\\377\\101"), Ok(vec![0, 0xff, b'A']));
        // `\X` is not the hex format, and `\400` is not an octal byte.
        assert_eq!(
            bytea_from_text("\\X0a"),
            Err(ByteaInputError::InvalidEscape)
        );
        assert_eq!(
            bytea_from_text("\\400"),
            Err(ByteaInputError::InvalidEscape)
        );
        assert_eq!(bytea_from_text("\\12"), Err(ByteaInputError::InvalidEscape));
        assert_eq!(bytea_from_text("a\\"), Err(ByteaInputError::InvalidEscape));
    }
}
