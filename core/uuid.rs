use crate::ext::register_scalar_function;
use turso_ext::{scalar, ExtensionApi, ResultCode, Value, ValueType};

pub fn register_extension(ext_api: &mut ExtensionApi) {
    // FIXME: Add macro magic to register functions automatically.
    unsafe {
        register_scalar_function(ext_api.ctx, c"uuid4_str".as_ptr(), uuid4_str);
        register_scalar_function(ext_api.ctx, c"gen_random_uuid".as_ptr(), uuid4_str);
        register_scalar_function(ext_api.ctx, c"uuid4".as_ptr(), uuid4_blob);
        register_scalar_function(ext_api.ctx, c"uuid7_str".as_ptr(), uuid7_str);
        register_scalar_function(ext_api.ctx, c"uuid7".as_ptr(), uuid7);
        register_scalar_function(ext_api.ctx, c"uuid7_timestamp_ms".as_ptr(), uuid7_ts);
        register_scalar_function(ext_api.ctx, c"uuid_str".as_ptr(), uuid_str);
        register_scalar_function(ext_api.ctx, c"uuid_blob".as_ptr(), uuid_blob);
    }
}

#[scalar(name = "uuid4_str", alias = "gen_random_uuid")]
fn uuid4_str(_args: &[Value]) -> Value {
    let uuid = uuid::Uuid::new_v4().to_string();
    Value::from_text(uuid)
}

#[scalar(name = "uuid4")]
fn uuid4_blob(_args: &[Value]) -> Value {
    let uuid = uuid::Uuid::new_v4();
    let bytes = uuid.as_bytes();
    Value::from_blob(bytes.to_vec())
}

/// Largest Unix-seconds argument a v7 UUID can hold: its timestamp is
/// milliseconds in a 48-bit field, and beyond this it stops round-tripping.
const MAX_UUID7_UNIX_SECONDS: i64 = ((1i64 << 48) - 1) / 1000;

/// Build a v7 timestamp from a Unix-seconds argument, or `None` when the value
/// is out of range.
///
/// `Timestamp::from_unix` takes `u64` and multiplies by 1000 unchecked, so a
/// negative `i64` cast straight through overflows inside the uuid crate.
fn uuid7_timestamp_from_unix_seconds(unix: i64) -> Option<uuid::Timestamp> {
    if unix <= 0 || unix > MAX_UUID7_UNIX_SECONDS {
        return None;
    }
    Some(uuid::Timestamp::from_unix(
        uuid::ContextV7::new(),
        unix as u64,
        0,
    ))
}

#[scalar(name = "uuid7_str")]
fn uuid7_str(args: &[Value]) -> Value {
    let timestamp = if args.is_empty() {
        let ctx = uuid::ContextV7::new();
        uuid::Timestamp::now(ctx)
    } else {
        let unix = match args[0].value_type() {
            ValueType::Integer => {
                let Some(int) = args[0].to_integer() else {
                    return Value::error(ResultCode::InvalidArgs);
                };
                int
            }
            ValueType::Text => {
                let Some(text) = args[0].to_text() else {
                    return Value::error(ResultCode::InvalidArgs);
                };
                match text.parse::<i64>() {
                    Ok(unix) => unix,
                    Err(_) => return Value::error(ResultCode::InvalidArgs),
                }
            }
            _ => return Value::error(ResultCode::InvalidArgs),
        };
        let Some(timestamp) = uuid7_timestamp_from_unix_seconds(unix) else {
            return Value::error_with_message("Invalid timestamp".to_string());
        };
        timestamp
    };
    let uuid = uuid::Uuid::new_v7(timestamp);
    Value::from_text(uuid.to_string())
}

#[scalar(name = "uuid7")]
fn uuid7(&self, args: &[Value]) -> Value {
    let timestamp = if args.is_empty() {
        let ctx = uuid::ContextV7::new();
        uuid::Timestamp::now(ctx)
    } else {
        match args[0].value_type() {
            ValueType::Integer => {
                let Some(int) = args[0].to_integer() else {
                    return Value::null();
                };
                // Out-of-range timestamps are NULL here rather than an error,
                // matching how this function reports every other bad argument.
                let Some(timestamp) = uuid7_timestamp_from_unix_seconds(int) else {
                    return Value::null();
                };
                timestamp
            }
            _ => return Value::null(),
        }
    };
    let uuid = uuid::Uuid::new_v7(timestamp);
    let bytes = uuid.as_bytes();
    Value::from_blob(bytes.to_vec())
}

#[scalar(name = "uuid7_timestamp_ms")]
fn uuid7_ts(args: &[Value]) -> Value {
    match args.first().map(|a| a.value_type()) {
        Some(ValueType::Blob) => {
            let Some(blob) = &args[0].to_blob() else {
                return Value::null();
            };
            let Ok(uuid) = uuid::Uuid::from_slice(blob.as_slice()) else {
                return Value::null();
            };
            let unix = uuid_to_unix(uuid.as_bytes());
            Value::from_integer(unix as i64)
        }
        Some(ValueType::Text) => {
            let Some(text) = args[0].to_text() else {
                return Value::null();
            };
            let Ok(uuid) = uuid::Uuid::parse_str(text) else {
                return Value::null();
            };
            let unix = uuid_to_unix(uuid.as_bytes());
            Value::from_integer(unix as i64)
        }
        None => Value::error_with_message(
            "wrong number of arguments to function uuid7_timestamp_ms()".into(),
        ),
        _ => Value::null(),
    }
}

#[scalar(name = "uuid_str")]
fn uuid_str(args: &[Value]) -> Value {
    match args.first() {
        Some(arg) => match arg.to_blob() {
            Some(blob) => {
                let parsed = uuid::Uuid::from_slice(blob.as_slice())
                    .ok()
                    .map(|u| u.to_string());
                match parsed {
                    Some(s) => Value::from_text(s),
                    None => Value::null(),
                }
            }
            None => {
                // just return Null here if there is an arg but it's not convertable to a blob,
                // if 'select uuid_str(c) from t' is called on null `c` we don't want to throw err
                return Value::null();
            }
        },
        None => {
            return Value::error_with_message(
                "wrong number of arguments to function uuid_str()".into(),
            );
        }
    }
}

#[scalar(name = "uuid_blob")]
fn uuid_blob(&self, args: &[Value]) -> Value {
    match args.first() {
        Some(arg) => match arg.to_text() {
            Some(text) => {
                if let Some(bytes) = parse_uuid_bytes(text) {
                    Value::from_blob(bytes.to_vec())
                } else if let Ok(uuid) = uuid::Uuid::parse_str(text) {
                    Value::from_blob(uuid.as_bytes().to_vec())
                } else {
                    Value::null()
                }
            }
            None => return Value::null(),
        },
        None => {
            return Value::error_with_message(
                "wrong number of arguments to function uuid_blob()".into(),
            );
        }
    }
}

/// Parse a UUID from a string adhering to PostgreSQL's `uuid_in` rules:
/// - 32 hexadecimal digits (16 bytes)
/// - Optional surrounding braces `{...}`
/// - Optional hyphen `-` after any group of 4 hex digits (every 2 bytes)
/// - Optional leading and trailing whitespace
pub fn parse_uuid_bytes(s: &str) -> Option<[u8; 16]> {
    let s = s.trim();
    let (s, has_braces) = if let Some(stripped) = s.strip_prefix('{') {
        (stripped, true)
    } else {
        (s, false)
    };

    let bytes = s.as_bytes();
    let mut pos = 0;
    let mut result = [0u8; 16];

    for (i, byte) in result.iter_mut().enumerate() {
        if i > 0 && (i % 2) == 0 && pos < bytes.len() && bytes[pos] == b'-' {
            pos += 1;
        }

        if pos + 2 > bytes.len() {
            return None;
        }

        let hi = hex_digit(bytes[pos])?;
        let lo = hex_digit(bytes[pos + 1])?;
        pos += 2;

        *byte = (hi << 4) | lo;
    }

    if has_braces {
        if pos >= bytes.len() || bytes[pos] != b'}' {
            return None;
        }
        pos += 1;
    }

    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }

    if pos != bytes.len() {
        return None;
    }

    Some(result)
}

#[inline(always)]
fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[inline(always)]
fn uuid_to_unix(uuid: &[u8; 16]) -> u64 {
    ((uuid[0] as u64) << 40)
        | ((uuid[1] as u64) << 32)
        | ((uuid[2] as u64) << 24)
        | ((uuid[3] as u64) << 16)
        | ((uuid[4] as u64) << 8)
        | (uuid[5] as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_uuid_bytes() {
        let expected = [
            0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38,
            0x0a, 0x11,
        ];

        // Standard canonical format (8-4-4-4-12)
        assert_eq!(
            parse_uuid_bytes("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
            Some(expected)
        );

        // Hyphen after every 4 digits (PostgreSQL uuid_in format)
        assert_eq!(
            parse_uuid_bytes("a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11"),
            Some(expected)
        );

        // Bare 32 hex digits (no hyphens)
        assert_eq!(
            parse_uuid_bytes("a0eebc999c0b4ef8bb6d6bb9bd380a11"),
            Some(expected)
        );

        // Braced formats
        assert_eq!(
            parse_uuid_bytes("{a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11}"),
            Some(expected)
        );
        assert_eq!(
            parse_uuid_bytes("{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"),
            Some(expected)
        );
        assert_eq!(
            parse_uuid_bytes("{a0eebc999c0b4ef8bb6d6bb9bd380a11}"),
            Some(expected)
        );

        // Surrounding whitespace
        assert_eq!(
            parse_uuid_bytes("  a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11  \t\n"),
            Some(expected)
        );

        // Uppercase / mixed case
        assert_eq!(
            parse_uuid_bytes("A0EE-BC99-9C0B-4EF8-BB6D-6BB9-BD38-0A11"),
            Some(expected)
        );

        // Invalid cases
        assert_eq!(parse_uuid_bytes(""), None);
        assert_eq!(parse_uuid_bytes("a"), None);
        assert_eq!(
            parse_uuid_bytes("11111111-1111-1111-1111-111111111111F"),
            None
        );
        assert_eq!(
            parse_uuid_bytes("{11111111-1111-1111-1111-11111111111}"),
            None
        );
        assert_eq!(
            parse_uuid_bytes("111-11111-1111-1111-1111-111111111111"),
            None
        );
        assert_eq!(
            parse_uuid_bytes("{22222222-2222-2222-2222-222222222222 }"),
            None
        );
        assert_eq!(
            parse_uuid_bytes("11111111-1111-1111-G111-111111111111"),
            None
        );
        assert_eq!(
            parse_uuid_bytes("11+11111-1111-1111-1111-111111111111"),
            None
        );
    }
}
