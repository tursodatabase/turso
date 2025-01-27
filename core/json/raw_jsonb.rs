use crate::json::error::Error;
use crate::json::Val;
use crate::{json, LimboError};
use std::fmt::Formatter;
use thiserror::Error;

/// Maximum allowable depth of a sane JSON, after which we will return an error
static MAX_JSONB_DEPTH: u16 = 2000;

/// Represents a raw Jsonb value that does not own its data.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RawJsonb<'a> {
    /// The underlying byte slice representing the JSONB data.
    pub(crate) data: &'a [u8],
}

/// Converts a borrowed byte slice into a RawJsonb.
/// This provides a convenient way to create a RawJsonb from existing data without copying.
impl<'a> From<&'a [u8]> for RawJsonb<'a> {
    fn from(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl TryFrom<RawJsonb<'_>> for Val {
    type Error = json::JsonError;

    fn try_from(raw: RawJsonb) -> Result<Val, Self::Error> {
        match raw.to_string() {
            // TODO: implement more efficient way to convert to Val without
            //   converting to string first
            Ok(s) => crate::json::from_str::<Val>(&s),
            Err(e) => Err(e.into()),
        }
    }
}

/// Allows accessing the underlying byte slice as a reference.
/// This enables easy integration with functions that expect a &[u8].
impl AsRef<[u8]> for RawJsonb<'_> {
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}

/// All possible JSONB types
#[derive(Debug)]
enum JsonbType {
    /// JSON null value
    Null,
    /// JSON true value
    True,
    /// JSON false value
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

impl std::fmt::Display for JsonbType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Into<JsonbType> for u8 {
    fn into(self) -> JsonbType {
        match self & 0x0f {
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
            _ => unreachable!("0x0f mask will not allow for a bigger number"),
        }
    }
}

// TODO: add position to the error - should be easy
#[derive(Debug, Error, miette::Diagnostic)]
pub enum JsonbError {
    #[error("JSONB Parse error: {0}")]
    ParseError(String),
    #[error("Corrupted JSONB header: {0}")]
    CorruptedHeader(u8),
    #[error("Maximum JSONB depth exceeded: {0}", MAX_JSONB_DEPTH)]
    TooDeep,
    #[error("Expected JSONB value to have {0} bytes, but got {1}")]
    OutOfBounds(usize, usize),
    #[error("Expected JSONB key to be a string, got: {0}")]
    KeyNotAString(JsonbType),
}

pub type Result<T, E = JsonbError> = std::result::Result<T, E>;

impl From<JsonbError> for LimboError {
    fn from(value: JsonbError) -> Self {
        // TODO: should we expose better error messages?
        LimboError::ParseError("malformed JSON".to_string())
    }
}

impl From<JsonbError> for Error {
    fn from(value: JsonbError) -> Self {
        // TODO: should we expose better error messages?
        Error::Message {
            msg: "malformed JSON".to_string(),
            location: None,
        }
    }
}

impl<'a> RawJsonb<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    /// Converts the JSONB value into a string.
    pub fn to_string(self) -> Result<String> {
        if self.data.is_empty() {
            return Ok("".to_string());
        }

        let (_, value_size) = header_and_value_size(self.data, 0)?;
        // TODO: can we come up with a better initial capacity?
        let mut result = String::with_capacity(value_size);
        jsonb_to_string_internal(self.data, 0, &mut result)?;

        Ok(result)
    }
}

/// Internal function that converts a JSONB value into a string.
/// This function is recursive in case the value is an array or an object.
/// Returns the amount of bytes consumed from `arr`
fn jsonb_to_string_internal(arr: &[u8], depth: u16, result: &mut String) -> Result<usize> {
    if depth > MAX_JSONB_DEPTH {
        return Err(JsonbError::TooDeep);
    }

    if arr.is_empty() {
        return Ok(0);
    }

    let current_element = 0;
    let jsonb_type: JsonbType = arr[current_element].into();

    match jsonb_type {
        JsonbType::Null => {
            result.push_str("null");
            Ok(1)
        }
        JsonbType::True => {
            result.push_str("true");
            Ok(1)
        }
        JsonbType::False => {
            result.push_str("false");
            Ok(1)
        }
        JsonbType::Int => {
            let (value_slice, header_size, value_size) =
                value_slice_from_header(arr, current_element)?;

            from_ascii_digits(value_slice, result)?;

            Ok(header_size + value_size)
        }
        JsonbType::Text | JsonbType::TextJ | JsonbType::Text5 | JsonbType::TextRaw => {
            // TODO: Implement differences between those text types
            let (value_slice, header_size, value_size) =
                value_slice_from_header(arr, current_element)?;

            result.push('"');
            // TODO: we should probably be more strict and not allow non-UTF8 characters
            result.push_str(&String::from_utf8_lossy(value_slice));
            result.push('"');

            Ok(header_size + value_size)
        }
        JsonbType::Array => {
            let (value_slice, header_size, value_size) =
                value_slice_from_header(arr, current_element)?;
            let mut arr_idx: usize = 0;

            result.push('[');

            while arr_idx < value_size {
                let consumed =
                    jsonb_to_string_internal(&value_slice[arr_idx..], depth + 1, result)?;
                arr_idx += consumed;

                if arr_idx < value_size {
                    result.push(',');
                }
            }

            result.push(']');

            Ok(header_size + value_size)
        }
        JsonbType::Object => {
            let (value_slice, header_size, object_size) =
                value_slice_from_header(arr, current_element)?;
            let mut obj_idx: usize = 0;

            result.push('{');

            while obj_idx < object_size {
                let key_type: JsonbType = value_slice[obj_idx].into();

                match key_type {
                    JsonbType::Text | JsonbType::Text5 | JsonbType::TextJ | JsonbType::TextRaw => {}
                    _ => return Err(JsonbError::KeyNotAString(key_type)),
                };

                let key_size =
                    jsonb_to_string_internal(&value_slice[obj_idx..], depth + 1, result)?;
                obj_idx += key_size;

                result.push(':');

                let value_size =
                    jsonb_to_string_internal(&value_slice[obj_idx..], depth + 1, result)?;
                obj_idx += value_size;

                if obj_idx < object_size {
                    result.push(',');
                }
            }

            result.push('}');

            Ok(header_size + object_size)
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
fn value_slice_from_header(arr: &[u8], current_element: usize) -> Result<(&[u8], usize, usize)> {
    let (header_size, value_size) = header_and_value_size(arr, current_element)?;

    let start = current_element + header_size;
    let end = start + value_size;

    if end > arr.len() {
        return Err(JsonbError::OutOfBounds(value_size, arr[start..].len()));
    }

    Ok((&arr[start..end], header_size, value_size))
}

fn header_and_value_size(arr: &[u8], current_element: usize) -> Result<(usize, usize)> {
    let upper_four_bits = arr[current_element] >> 4;
    let header_mask = upper_four_bits & 0x0f;

    let bytes_to_read = match header_mask {
        0..12 => 0,
        12 => 2,
        13 => 3,
        14 => 5,
        15 => 9,
        _ => return Err(JsonbError::CorruptedHeader(header_mask)),
    };

    if bytes_to_read == 0 {
        Ok((1, usize::from(upper_four_bits)))
    } else {
        let mut size = 0;
        for i in 1..bytes_to_read {
            size |= (arr[current_element + i] as usize) << (8 * (bytes_to_read - i - 1));
        }
        Ok((bytes_to_read, size))
    }
}

fn from_ascii_digits(arr: &[u8], result: &mut String) -> Result<()> {
    for &char in arr {
        if char.is_ascii_digit() {
            result.push(char as char)
        } else {
            return Err(JsonbError::ParseError("Expected ASCII digit".to_string()));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_arr() {
        assert_eq!(RawJsonb::new(&[]).to_string().unwrap(), "".to_string());
    }

    #[test]
    fn test_null() {
        assert_eq!(
            RawJsonb::new(&[0x10]).to_string().unwrap(),
            "null".to_string()
        );
    }

    #[test]
    fn test_booleans() {
        assert_eq!(
            RawJsonb::new(&[0x11]).to_string().unwrap(),
            "true".to_string()
        );
        assert_eq!(
            RawJsonb::new(&[0x12]).to_string().unwrap(),
            "false".to_string()
        );
    }

    #[test]
    fn test_numbers() {
        assert_eq!(
            RawJsonb::new(&[0x13, b'0']).to_string().unwrap(),
            "0".to_string()
        );
        assert_eq!(
            RawJsonb::new(&[0x13, b'1']).to_string().unwrap(),
            "1".to_string()
        );
        assert_eq!(
            RawJsonb::new(&[0x13, b'2']).to_string().unwrap(),
            "2".to_string()
        );
        assert_eq!(
            RawJsonb::new(&[0x13, b'9']).to_string().unwrap(),
            "9".to_string()
        );
        assert_eq!(
            RawJsonb::new(&[0xc3, 0x01, b'1']).to_string().unwrap(),
            "1".to_string()
        );
        assert_eq!(
            RawJsonb::new(&[0xd3, 0x00, 0x01, b'1'])
                .to_string()
                .unwrap(),
            "1".to_string()
        );
        assert_eq!(
            RawJsonb::new(&[0xe3, 0x00, 0x00, 0x00, 0x01, b'1'])
                .to_string()
                .unwrap(),
            "1".to_string()
        );
        assert_eq!(
            RawJsonb::new(&[0xf3, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, b'1'])
                .to_string()
                .unwrap(),
            "1".to_string()
        );
        assert_eq!(
            RawJsonb::new(&[0xd3, 0x00, 0x02, b'1', b'2'])
                .to_string()
                .unwrap(),
            "12".to_string()
        );
        assert_eq!(
            RawJsonb::new(&[0xc3, 0x03, b'1', b'2', b'3'])
                .to_string()
                .unwrap(),
            "123".to_string()
        );
    }

    #[test]
    fn test_numbers_invalid() {
        assert!(RawJsonb::new(&[0x13, b'a']).to_string().is_err());
        assert!(RawJsonb::new(&[0x13, b'X']).to_string().is_err());
        assert!(RawJsonb::new(&[0x13, 0]).to_string().is_err());
        assert!(RawJsonb::new(&[0x13, 255]).to_string().is_err());
    }

    #[test]
    fn test_text() {
        assert_eq!(
            RawJsonb::new(&[0xc7, 0x03, b'f', b'o', b'o'])
                .to_string()
                .unwrap(),
            "\"foo\"".to_string()
        );
    }

    #[test]
    fn test_text_oob() {
        match RawJsonb::new(&[0xc7, 0x03, b'f', b'o']).to_string() {
            Err(JsonbError::OutOfBounds(expected, got)) => {
                assert_eq!(expected, 3);
                assert_eq!(got, 2);
            }
            _ => panic!("Expected OutOfBounds error"),
        }
    }

    #[test]
    fn test_array() {
        assert_eq!(
            RawJsonb::new(&[0x0b]).to_string().unwrap(),
            "[]".to_string()
        );

        assert_eq!(
            RawJsonb::new(&[0xcb, 0x04, 0x13, b'1', 0x13, b'2'])
                .to_string()
                .unwrap(),
            "[1,2]".to_string()
        );

        assert_eq!(
            RawJsonb::new(&[0xcb, 0x03, 0x10, 0x11, 0x12])
                .to_string()
                .unwrap(),
            "[null,true,false]".to_string()
        );

        assert_eq!(
            RawJsonb::new(&[0xcb, 0x09, 0x13, b'1', 0x13, b'2', 0xc7, 0x03, b'f', b'o', b'o'])
                .to_string()
                .unwrap(),
            "[1,2,\"foo\"]".to_string()
        );

        assert_eq!(
            RawJsonb::new(&[0xcb, 0x06, 0x0b, 0xcb, 0x03, 0xc7, 0x01, b'1'])
                .to_string()
                .unwrap(),
            "[[],[\"1\"]]".to_string()
        );
    }

    #[test]
    fn test_object() {
        assert_eq!(
            RawJsonb::new(&[0x0c]).to_string().unwrap(),
            "{}".to_string()
        );

        assert_eq!(
            RawJsonb::new(&[0x9c, 0x17, b'a', 0x10, 0x17, b'b', 0x11, 0x17, b'c', 0x12])
                .to_string()
                .unwrap(),
            "{\"a\":null,\"b\":true,\"c\":false}".to_string()
        );
    }

    #[test]
    fn test_object_invalid() {
        match RawJsonb::new(&[0x9c, 0x17, b'a', 0x10, 0x17, b'b', 0x11, 0x17, b'c']).to_string() {
            Err(JsonbError::OutOfBounds(expected, got)) => {
                assert_eq!(expected, 9);
                assert_eq!(got, 8);
            }
            _ => panic!("Expected OutOfBounds error"),
        }

        match RawJsonb::new(&[0x8c, 0x13, b'a', 0x10, 0x17, b'b', 0x11, 0x17, b'c']).to_string() {
            Err(JsonbError::KeyNotAString(JsonbType::Int)) => {}
            _ => panic!("Expected KeyNotAString error"),
        }
    }
}
