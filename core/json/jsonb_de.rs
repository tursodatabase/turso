use crate::{error, LimboError};
use thiserror::Error;

static MAX_JSONB_DEPTH: u16 = 2000;

/// All possible JSONB types
enum JsonbType {
    Null,
    True,
    False,
    /// JSON integer value in the canonical RFC 8259 format, without extensions
    Int,
    /// JSON integer value that is not in the canonical format
    Int5,
    /// JSON floating-point value in the canonical RFC 8259 format, without extensions
    Float,
    /// JSON floating-point value that is not in the canonical format
    Float5,
    /// JSON string value that does not contain any escapes nor any characters that need
    /// to be escaped for either SQL or JSON
    Text,
    /// JSON string value that contains RFC 8259 character escapes (such as "\n" or "\u0020")
    TextJ,
    /// JSON string value that contains character escapes, including some character escapes
    /// that part of JSON5 and which are not found in the canonical RFC 8259 spec
    Text5,
    /// JSON string value that contains UTF8 characters that need to be escaped if
    /// this string is rendered into standard JSON text.
    /// The payload does not include string delimiters.
    TextRaw,
    /// JSON Array
    Array,
    /// JSON Object
    Object,
    /// Reserved for future use
    Reserved1,
    /// Reserved for future use
    Reserved2,
    /// Reserved for future use
    Reserved3,
}

impl Into<JsonbType> for u8 {
    fn into(self) -> JsonbType {
        match (self & 0x0f) {
            0 => JsonbType::Null,
            1 => JsonbType::True,
            2 => JsonbType::False,
            3 => JsonbType::Int,
            4 => JsonbType::Int5,
            5 => JsonbType::Float,
            6 => JsonbType::Float5,
            7 => JsonbType::Text,
            8 => JsonbType::TextJ,
            9 => JsonbType::Text5,
            10 => JsonbType::TextRaw,
            11 => JsonbType::Array,
            12 => JsonbType::Object,
            13 => JsonbType::Reserved1,
            14 => JsonbType::Reserved2,
            15 => JsonbType::Reserved3,
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Error, miette::Diagnostic)]
pub enum JsonbError {
    #[error("JSONB Parse error: {0}")]
    ParseError(String),
    #[error("Corrupted JSONB header: {0}")]
    CorruptedHeader(u8),
    #[error("Maximum JSONB depth exceeded: {0}", MAX_JSONB_DEPTH)]
    TooDeep,
}

pub type Result<T, E = JsonbError> = std::result::Result<T, E>;

impl From<JsonbError> for LimboError {
    fn from(value: JsonbError) -> Self {
        LimboError::ParseError(value.to_string())
    }
}

pub fn jsonb_to_string(arr: &[u8]) -> Result<String> {
    jsonb_to_string_internal(arr, 0)
}

fn jsonb_to_string_internal(arr: &[u8], depth: u16) -> Result<String> {
    if depth > MAX_JSONB_DEPTH {
        return Err(JsonbError::TooDeep);
    }

    if arr.is_empty() {
        return Ok("".to_string());
    }

    let current_element = 0;
    let jsonb_type: JsonbType = arr[current_element].into();

    match jsonb_type {
        JsonbType::Null => Ok("".to_string()),
        JsonbType::True => Ok("true".to_string()),
        JsonbType::False => Ok("false".to_string()),
        JsonbType::Int => {
            let value_slice = value_slice_from_header(arr, current_element)?;

            from_ascii_digits(value_slice)
        }
        JsonbType::Text | JsonbType::TextJ | JsonbType::Text5 | JsonbType::TextRaw => {
            // TODO: Implement differences between those text types
            let value_slice = value_slice_from_header(arr, current_element)?;

            Ok(format!(
                "\"{}\"",
                String::from_utf8(value_slice.to_vec()).unwrap()
            ))
        }
        _ => unimplemented!(),
    }
}

/// Extracts the value slice from the header
/// Assumes that `current_element` points to the first header byte in `arr`
///
/// The size of the header is dependent on the 4 most significant bits of the first byte
/// The size of the value is encoded in the bytes of the header, starting from the 2nd byte:
/// FIRST_BYTE | VALUE_SIZE | VALUE
///
/// Example:
/// value_slice_from_header([0x13, b'1'], 0) -> [b'1']
/// value_slice_from_header([0xc3, 0x01, b'1'], 0) -> [b'1']
/// value_slice_from_header([0xd3, 0x00, 0x01, b'1'], 0) -> [b'1']
fn value_slice_from_header(arr: &[u8], current_element: usize) -> Result<&[u8]> {
    let header_mask = (arr[current_element] >> 4) & 0x0f;

    let header_size = match header_mask {
        0..12 => 1,
        12 => 2,
        13 => 3,
        14 => 5,
        15 => 9,
        _ => return Err(JsonbError::CorruptedHeader(header_mask)),
    };

    let value_size: usize = if header_size == 1 {
        1
    } else {
        let mut size = 0;
        for i in 1..header_size {
            size |= (arr[current_element + i] as usize) << (8 * (header_size - i - 1));
        }
        size
    };

    // TODO: Bound checks
    Ok(&arr[current_element + header_size..current_element + header_size + value_size])
}

fn from_ascii_digits(arr: &[u8]) -> Result<String> {
    let mut result = String::new();

    for &char in arr {
        if char.is_ascii_digit() {
            result.push(char as char)
        } else {
            return Err(JsonbError::ParseError("Expected ASCII digit".to_string()));
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_arr() {
        assert_eq!(jsonb_to_string(&[]).unwrap(), "".to_string());
    }

    #[test]
    fn test_null() {
        assert_eq!(jsonb_to_string(&[0x10]).unwrap(), "".to_string());
    }

    #[test]
    fn test_booleans() {
        assert_eq!(jsonb_to_string(&[0x11]).unwrap(), "true".to_string());
        assert_eq!(jsonb_to_string(&[0x12]).unwrap(), "false".to_string());
    }

    #[test]
    fn test_numbers() {
        assert_eq!(jsonb_to_string(&[0x13, b'0']).unwrap(), "0".to_string());
        assert_eq!(jsonb_to_string(&[0x13, b'1']).unwrap(), "1".to_string());
        assert_eq!(jsonb_to_string(&[0x13, b'2']).unwrap(), "2".to_string());
        assert_eq!(jsonb_to_string(&[0x13, b'9']).unwrap(), "9".to_string());
        assert_eq!(
            jsonb_to_string(&[0xc3, 0x01, b'1']).unwrap(),
            "1".to_string()
        );
        assert_eq!(
            jsonb_to_string(&[0xd3, 0x00, 0x01, b'1']).unwrap(),
            "1".to_string()
        );
        assert_eq!(
            jsonb_to_string(&[0xe3, 0x00, 0x00, 0x00, 0x01, b'1']).unwrap(),
            "1".to_string()
        );
        assert_eq!(
            jsonb_to_string(&[0xf3, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, b'1']).unwrap(),
            "1".to_string()
        );
        assert_eq!(
            jsonb_to_string(&[0xd3, 0x00, 0x02, b'1', b'2']).unwrap(),
            "12".to_string()
        );
        assert_eq!(
            jsonb_to_string(&[0xc3, 0x03, b'1', b'2', b'3']).unwrap(),
            "123".to_string()
        );
    }

    #[test]
    fn test_numbers_invalid() {
        assert!(jsonb_to_string(&[0x13, b'a']).is_err());
        assert!(jsonb_to_string(&[0x13, b'X']).is_err());
        assert!(jsonb_to_string(&[0x13, 0]).is_err());
        assert!(jsonb_to_string(&[0x13, 255]).is_err());
    }

    #[test]
    fn test_text() {
        assert_eq!(
            jsonb_to_string(&[0xc7, 0x03, b'f', b'o', b'o']).unwrap(),
            "\"foo\"".to_string()
        );
    }
}
