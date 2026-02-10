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
        Value::Numeric(Numeric::Null) => {
            output.push(b'0');
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
mod tests {
    use super::*;
    use turso_core::types::Text;

    #[test]
    fn test_null_encoding() {
        let mut buf = Vec::new();
        encode_value(&Value::Null, &mut buf);
        assert_eq!(buf, vec![b'0']);
    }

    #[test]
    fn test_integer_encoding() {
        let mut buf = Vec::new();
        encode_value(&Value::from_i64(0x0102030405060708), &mut buf);
        assert_eq!(
            buf,
            vec![b'1', 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
    }

    #[test]
    fn test_negative_integer() {
        let mut buf = Vec::new();
        encode_value(&Value::from_i64(-1), &mut buf);
        // -1 as i64 = 0xFFFFFFFFFFFFFFFF in two's complement
        assert_eq!(
            buf,
            vec![b'1', 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        );
    }

    #[test]
    fn test_zero_integer() {
        let mut buf = Vec::new();
        encode_value(&Value::from_i64(0), &mut buf);
        assert_eq!(buf, vec![b'1', 0, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_float_encoding() {
        let mut buf = Vec::new();
        encode_value(&Value::from_f64(1.0), &mut buf);
        // 1.0 as IEEE 754 = 0x3FF0000000000000
        assert_eq!(buf[0], b'2');
        assert_eq!(buf[1..], 1.0_f64.to_bits().to_be_bytes());
    }

    #[test]
    fn test_float_zero() {
        let mut buf = Vec::new();
        encode_value(&Value::from_f64(0.0), &mut buf);
        assert_eq!(buf[0], b'2');
        assert_eq!(buf[1..], 0.0_f64.to_bits().to_be_bytes());
    }

    #[test]
    fn test_text_encoding() {
        let mut buf = Vec::new();
        encode_value(&Value::Text(Text::new("hello")), &mut buf);
        assert_eq!(buf, vec![b'3', b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_empty_text() {
        let mut buf = Vec::new();
        encode_value(&Value::Text(Text::new("")), &mut buf);
        assert_eq!(buf, vec![b'3']);
    }

    #[test]
    fn test_blob_encoding() {
        let mut buf = Vec::new();
        encode_value(&Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]), &mut buf);
        assert_eq!(buf, vec![b'4', 0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_empty_blob() {
        let mut buf = Vec::new();
        encode_value(&Value::Blob(vec![]), &mut buf);
        assert_eq!(buf, vec![b'4']);
    }

    #[test]
    fn test_type_prefixes_differ() {
        // Ensure different types produce different prefixes
        let values = [
            Value::Null,
            Value::from_i64(0),
            Value::from_f64(0.0),
            Value::Text(Text::new("0")),
            Value::Blob(vec![0]),
        ];

        let mut encodings: Vec<Vec<u8>> = Vec::new();
        for value in &values {
            let mut buf = Vec::new();
            encode_value(value, &mut buf);
            // just check the prefix differs
            encodings.push(buf);
        }

        // All prefixes should be unique
        let prefixes: Vec<u8> = encodings.iter().map(|e| e[0]).collect();
        assert_eq!(prefixes, vec![b'0', b'1', b'2', b'3', b'4']);
    }

    #[test]
    fn test_integer_max() {
        let mut buf = Vec::new();
        encode_value(&Value::from_i64(i64::MAX), &mut buf);
        assert_eq!(buf[0], b'1');
        // i64::MAX = 0x7FFFFFFFFFFFFFFF
        assert_eq!(buf[1..], [0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_integer_min() {
        let mut buf = Vec::new();
        encode_value(&Value::from_i64(i64::MIN), &mut buf);
        assert_eq!(buf[0], b'1');
        // i64::MIN = 0x8000000000000000 (two's complement)
        assert_eq!(buf[1..], [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_integer_minus_two() {
        let mut buf = Vec::new();
        encode_value(&Value::from_i64(-2), &mut buf);
        assert_eq!(buf[0], b'1');
        // -2 as i64 = 0xFFFFFFFFFFFFFFFE
        assert_eq!(buf[1..], [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE]);
    }

    #[test]
    fn test_float_negative_zero() {
        let mut buf = Vec::new();
        encode_value(&Value::from_f64(-0.0), &mut buf);
        assert_eq!(buf[0], b'2');
        // -0.0 has a different bit pattern than 0.0
        // -0.0 = 0x8000000000000000
        assert_eq!(buf[1..], (-0.0_f64).to_bits().to_be_bytes());
        // Verify it's different from positive zero
        let mut buf_pos = Vec::new();
        encode_value(&Value::from_f64(0.0), &mut buf_pos);
        // Note: 0.0 and -0.0 have different bit patterns but compare equal
        // SQLite's dbhash uses the bit pattern, so they hash differently
        assert_ne!(buf[1..], buf_pos[1..]);
    }

    #[test]
    fn test_float_infinity() {
        let mut buf = Vec::new();
        encode_value(&Value::from_f64(f64::INFINITY), &mut buf);
        assert_eq!(buf[0], b'2');
        assert_eq!(buf[1..], f64::INFINITY.to_bits().to_be_bytes());
    }

    #[test]
    fn test_float_neg_infinity() {
        let mut buf = Vec::new();
        encode_value(&Value::from_f64(f64::NEG_INFINITY), &mut buf);
        assert_eq!(buf[0], b'2');
        assert_eq!(buf[1..], f64::NEG_INFINITY.to_bits().to_be_bytes());
    }

    #[test]
    fn test_float_nan() {
        let mut buf = Vec::new();
        // NaN is not representable in NonNan, so from_f64(NaN) returns Null
        encode_value(&Value::from_f64(f64::NAN), &mut buf);
        assert_eq!(buf, vec![b'0']);
    }

    #[test]
    fn test_float_very_small() {
        let mut buf = Vec::new();
        encode_value(&Value::from_f64(f64::MIN_POSITIVE), &mut buf);
        assert_eq!(buf[0], b'2');
        assert_eq!(buf[1..], f64::MIN_POSITIVE.to_bits().to_be_bytes());
    }

    #[test]
    fn test_float_very_large() {
        let mut buf = Vec::new();
        encode_value(&Value::from_f64(f64::MAX), &mut buf);
        assert_eq!(buf[0], b'2');
        assert_eq!(buf[1..], f64::MAX.to_bits().to_be_bytes());
    }

    #[test]
    fn test_float_pi() {
        let mut buf = Vec::new();
        encode_value(&Value::from_f64(std::f64::consts::PI), &mut buf);
        assert_eq!(buf[0], b'2');
        assert_eq!(buf[1..], std::f64::consts::PI.to_bits().to_be_bytes());
    }

    #[test]
    fn test_text_unicode() {
        let mut buf = Vec::new();
        encode_value(&Value::Text(Text::new("Hello, 世界! 🌍")), &mut buf);
        assert_eq!(buf[0], b'3');
        assert_eq!(&buf[1..], "Hello, 世界! 🌍".as_bytes());
    }

    #[test]
    fn test_text_with_null_byte() {
        let mut buf = Vec::new();
        // Text with embedded null byte (valid in SQLite TEXT)
        encode_value(&Value::Text(Text::new("a\0b")), &mut buf);
        assert_eq!(buf[0], b'3');
        assert_eq!(&buf[1..], b"a\0b");
    }

    #[test]
    fn test_text_newlines_and_tabs() {
        let mut buf = Vec::new();
        encode_value(&Value::Text(Text::new("line1\nline2\ttab")), &mut buf);
        assert_eq!(buf[0], b'3');
        assert_eq!(&buf[1..], b"line1\nline2\ttab");
    }

    #[test]
    fn test_blob_with_all_byte_values() {
        let mut buf = Vec::new();
        // Blob containing all possible byte values
        let all_bytes: Vec<u8> = (0u8..=255).collect();
        encode_value(&Value::Blob(all_bytes.clone()), &mut buf);
        assert_eq!(buf[0], b'4');
        assert_eq!(&buf[1..], all_bytes.as_slice());
    }

    #[test]
    fn test_blob_single_null_byte() {
        let mut buf = Vec::new();
        encode_value(&Value::Blob(vec![0x00]), &mut buf);
        assert_eq!(buf, vec![b'4', 0x00]);
    }
}
