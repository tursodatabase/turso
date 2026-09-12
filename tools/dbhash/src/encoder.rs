//! Value encoding for dbhash.
//!
//! Each value is encoded with a type prefix followed by normalized data:
//! - '0' = NULL (no data)
//! - '1' + 8 bytes big-endian = INTEGER
//! - '2' + 8 bytes big-endian IEEE 754 bits = FLOAT
//! - '3' + raw UTF-8 bytes = TEXT
//! - '4' + raw bytes = BLOB

use turso_core::{Numeric, Value};

/// Encode a value for hashing with type prefix.
///
/// The encoding matches SQLite's dbhash tool:
/// - Type prefix distinguishes NULL/int/float/text/blob
/// - Big-endian normalization ensures platform independence
pub fn encode_value(value: &Value, output: &mut Vec<u8>) {
    match value {
        Value::Null => {
            output.push(b'0');
        }
        Value::Numeric(Numeric::Integer(v)) => {
            output.push(b'1');
            output.extend_from_slice(&v.to_be_bytes());
        }
        Value::Numeric(Numeric::Float(v)) => {
            output.push(b'2');
            output.extend_from_slice(&f64::from(*v).to_bits().to_be_bytes());
        }
        Value::Text(text) => {
            output.push(b'3');
            output.extend_from_slice(text.as_str().as_bytes());
        }
        Value::Blob(b) => {
            output.push(b'4');
            output.extend_from_slice(b);
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/encoder/tests.rs"]
mod tests;
