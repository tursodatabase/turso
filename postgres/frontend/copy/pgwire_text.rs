// Vendored from pgwire 0.36.3:
// src/types/to_sql_text.rs (bool, i64, &[u8])
// src/types/format/float_digits.rs (ExtraFloatDigits::format_f64)
// Copyright (c) 2018 Ning Sun. See licenses/pgwire-mit-license.md.
// Adaptations: Vec<u8> instead of BytesMut; only the default hex bytea and
// shortest float formats; an array flag instead of postgres_types::Type;
// infallible functions instead of the ToSqlText trait and IsNull results.

pub(super) fn encode_bool(value: bool, out: &mut Vec<u8>) {
    if value {
        out.extend_from_slice(b"t");
    } else {
        out.extend_from_slice(b"f");
    }
}

pub(super) fn encode_integer(value: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(value.to_string().as_bytes());
}

pub(super) fn format_f64(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_positive() {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }

    if value == 0.0 {
        return "0".to_string();
    }

    let mut buffer = ryu::Buffer::new();
    buffer.format(value).to_string()
}

pub(super) fn encode_bytea(value: &[u8], array: bool, out: &mut Vec<u8>) {
    // for a bytea_array, the bytes are enclosed in quotes,
    // and \x is escaped
    if array {
        out.extend_from_slice(b"\"\\");
    }

    out.extend_from_slice(b"\\x");
    out.extend_from_slice(hex::encode(value).as_bytes());

    if array {
        out.extend_from_slice(b"\"");
    }
}
