use super::*;

fn parse_has_json5(input: &str) -> bool {
    let (_, info) = Jsonb::from_str_tracking(input).unwrap();
    info.has_json5
}

#[test]
fn plain_json_does_not_set_the_json5_flag() {
    for input in [
        "{\"a\":1}",
        "[1,2]",
        "\"aA\\n\\u0041\\\"b\"",
        " \t\r\n 1 ",
        "-1.5e+2",
        "null",
        "true",
    ] {
        assert!(!parse_has_json5(input), "{input:?}");
    }
}

#[test]
fn lenient_validation_skips_numeric_payload_bytes_strict_checks_them() {
    // FLOAT element with payload "1.e+": structurally fine, so the
    // lenient pass used by is_valid accepts it, but the payload is
    // not a well-formed number, so the strict check reports the
    // offset of the bad byte. sqlite3 agrees:
    // json_error_position(x'45312e652b') = 3.
    let blob = &[0x45, b'1', b'.', b'e', b'+'];
    assert!(validate_jsonb(blob));
    assert_eq!(jsonb_error_position(blob), 3);
}

#[test]
fn jsonb_error_position_is_zero_for_valid_blobs() {
    // null, the integer 1, {"a":3} and an empty array. All checked
    // against sqlite3's json_error_position.
    for blob in [
        &[0x00_u8] as &[u8],
        &[0x13, b'1'],
        &[0x4C, 0x17, b'a', 0x13, b'3'],
        &[0x0B],
    ] {
        assert_eq!(jsonb_error_position(blob), 0, "{blob:x?}");
    }
}

#[test]
fn jsonb_error_position_reports_first_malformed_byte() {
    // Each expectation matches sqlite3's json_error_position for
    // the same blob.
    // A header announcing a 1-byte size that is not there.
    assert_eq!(jsonb_error_position(&[0xC3]), 1);
    // INT payload "A" is not a digit.
    assert_eq!(jsonb_error_position(&[0x13, b'A']), 2);
    // TEXTJ payload "\q" is not an RFC 8259 escape.
    assert_eq!(jsonb_error_position(&[0x28, b'\\', b'q']), 2);
    // An object holding only a key reports the end of its payload.
    assert_eq!(jsonb_error_position(&[0x2C, 0x17, b'a']), 4);
}

#[test]
fn jsonb_error_position_bounds_escape_lookahead_to_the_payload() {
    // SQLite's jsonb('["\0",123]'): the TEXT5 payload ends in \0
    // and the next element's header byte is the digit '3'. The \0
    // digit lookahead must stop at the payload end, so the blob is
    // valid. sqlite3 agrees: json_error_position(x'7B295C3033313233')
    // is 0.
    let blob = b"\x7B\x29\\0\x33123";
    assert_eq!(jsonb_error_position(blob), 0);
}

#[test]
fn jsonb_error_position_reports_bad_escape_at_the_line_continuation() {
    // A bad escape reached through a line continuation reports the
    // first backslash of the run, because SQLite decodes the whole
    // run as one escape. Each expectation matches sqlite3's
    // json_error_position for the same blob.
    // "\<LF>\" inside an array: the dangling backslash reports the
    // continuation's backslash at offset 3.
    assert_eq!(jsonb_error_position(&[0x4B, 0x39, b'\\', b'\n', b'\\']), 3);
    // "\<LF>\q": the bad escape after the continuation likewise.
    assert_eq!(
        jsonb_error_position(&[0xC9, 4, b'\\', b'\n', b'\\', b'q']),
        3
    );
    // "\<LF>\<LF>\a": a chain of continuations still reports the
    // first backslash.
    assert_eq!(
        jsonb_error_position(&[0xC9, 6, b'\\', b'\n', b'\\', b'\n', b'\\', b'a']),
        3
    );
    // A continuation that ends the payload is complete and valid.
    assert_eq!(jsonb_error_position(&[0xC9, 2, b'\\', b'\n']), 0);
}

#[test]
fn jsonb_error_position_skips_hex_checks_for_u_after_continuation() {
    // \uZZZZ alone is a bad escape, but reached through a line
    // continuation SQLite converts the hex digits blindly and
    // accepts it. sqlite3: json_error_position(x'C9065C755A5A5A5A')
    // is 3 and json_error_position(x'C9085C0A5C755A5A5A5A') is 0.
    assert_eq!(jsonb_error_position(b"\xC9\x06\\uZZZZ"), 3);
    assert_eq!(jsonb_error_position(b"\xC9\x08\\\n\\uZZZZ"), 0);
}

#[test]
fn jsonb_error_position_rejects_extended_headers_on_primitives() {
    // SQLite accepts NULL, TRUE and FALSE only as their bare
    // one-byte headers (jsonbValidityCheck requires n+sz==1), so
    // an extended-size encoding of the empty payload is malformed
    // at the element start. The lenient document check keeps
    // accepting it. sqlite3: json_error_position(x'C000') = 1,
    // likewise for x'C100' and x'C200'.
    for blob in [&[0xC0_u8, 0x00] as &[u8], &[0xC1, 0x00], &[0xC2, 0x00]] {
        assert_eq!(jsonb_error_position(blob), 1, "{blob:x?}");
        assert!(validate_jsonb(blob), "{blob:x?}");
    }
}

#[test]
fn jsonb_error_position_accepts_backslash_nul_like_sqlite() {
    // sqlite3: json_error_position(x'C8025C00') = 0 and
    // json_error_position(x'C9025C00') = 0 -- strchr's terminator
    // match lets a backslash-NUL pass as a standard escape in
    // TEXTJ and TEXT5. It stays malformed in TEXT (no escapes at
    // all, x'C7025C00' errors at 3) and when reached through a
    // line continuation (jsonUnescapeOneChar has no NUL case, so
    // x'C9045C0A5C00' errors at the first backslash).
    assert_eq!(jsonb_error_position(&[0xC8, 2, b'\\', 0]), 0);
    assert_eq!(jsonb_error_position(&[0xC9, 2, b'\\', 0]), 0);
    assert_eq!(jsonb_error_position(&[0xC7, 2, b'\\', 0]), 3);
    assert_eq!(jsonb_error_position(&[0xC9, 4, b'\\', b'\n', b'\\', 0]), 3);
}

#[test]
fn strict_check_accepts_non_utf8_text_payload_bytes() {
    // SQLite's strict check works on raw bytes: a raw 0xFF passes
    // in every text type (sqlite3: json_valid(x'17FF',8) through
    // json_valid(x'1AFF',8) are all 1). The lenient document
    // check keeps requiring UTF-8, because document readers
    // decode text payloads as &str.
    for blob in [
        &[0x17_u8, 0xFF] as &[u8],
        &[0x18, 0xFF],
        &[0x19, 0xFF],
        &[0x1A, 0xFF],
    ] {
        assert_eq!(jsonb_error_position(blob), 0, "{blob:x?}");
        assert!(!validate_jsonb(blob), "{blob:x?}");
    }
}

#[test]
fn strict_check_consumes_malformed_utf8_after_continuations_like_sqlite() {
    // After a line continuation SQLite steps over the following
    // character with sqlite3Utf8ReadLimited, which consumes a lead
    // byte plus only actual continuation bytes. A truncated lead
    // like 0xC3 before a backslash consumes one byte, so the
    // backslash is still scanned. Each expectation matches
    // sqlite3's json_error_position for the same blob.
    // "\<LF>" 0xC3 "\q": the bad escape is found at 6.
    assert_eq!(
        jsonb_error_position(&[0xC9, 5, b'\\', b'\n', 0xC3, b'\\', b'q']),
        6
    );
    // "\<LF>" 0xC3 "\": the dangling backslash is found at 6.
    assert_eq!(
        jsonb_error_position(&[0xC9, 4, b'\\', b'\n', 0xC3, b'\\']),
        6
    );
    // A complete two-byte character, then a dangling backslash.
    assert_eq!(
        jsonb_error_position(&[0xC9, 5, b'\\', b'\n', 0xC3, 0xA8, b'\\']),
        7
    );
}

#[test]
fn jsonb_error_position_requires_zero_high_bytes_in_9_byte_headers() {
    // Header nibble 15 declares an 8-byte payload size. SQLite
    // reads it with a 32-bit size, so the header is valid exactly
    // when the first four size bytes are zero, at any nesting
    // level. sqlite3: json_error_position(x'F3000000000000000133')
    // and json_error_position(x'9BFB0000000000000000') are 0,
    // json_error_position(x'FB0000000100000000') is 1.
    let int = &[0xF3, 0, 0, 0, 0, 0, 0, 0, 1, b'3'];
    assert_eq!(jsonb_error_position(int), 0);
    let empty_array = &[0xFB, 0, 0, 0, 0, 0, 0, 0, 0];
    assert_eq!(jsonb_error_position(empty_array), 0);
    let nested = &[0x9B, 0xFB, 0, 0, 0, 0, 0, 0, 0, 0];
    assert_eq!(jsonb_error_position(nested), 0);
    let high_bytes_set = &[0xFB, 0, 0, 0, 1, 0, 0, 0, 0];
    assert_eq!(jsonb_error_position(high_bytes_set), 1);
}

#[test]
fn jsonb_error_position_starts_depth_at_one_like_sqlite() {
    // Exactly MAX_JSON_DEPTH nested arrays pass; one more fails at
    // the innermost element, whose offset is the last byte.
    fn nested_arrays(n: usize) -> Vec<u8> {
        let mut data = vec![0x0B];
        for _ in 0..n {
            let len = data.len();
            let mut wrapped = match len {
                0..=11 => vec![(len as u8) << 4 | 0x0B],
                12..=255 => vec![0xCB, len as u8],
                _ => vec![0xDB, (len >> 8) as u8, len as u8],
            };
            wrapped.extend_from_slice(&data);
            data = wrapped;
        }
        data
    }
    let ok = nested_arrays(MAX_JSON_DEPTH - 1);
    assert_eq!(jsonb_error_position(&ok), 0);
    let too_deep = nested_arrays(MAX_JSON_DEPTH);
    assert_eq!(jsonb_error_position(&too_deep), too_deep.len());
}

#[test]
fn each_json5_construct_sets_the_json5_flag() {
    for input in [
        "{a:1}",        // unquoted object key
        "'x'",          // single-quoted string
        "[1,]",         // trailing comma in array
        "{\"a\":1,}",   // trailing comma in object
        "/*c*/1",       // block comment
        "//c\n1",       // line comment
        "0x10",         // hex number
        "+1",           // leading plus sign
        ".5",           // leading decimal point
        "4.",           // trailing decimal point
        "Infinity",     // JSON5 infinity literal
        "NaN",          // JSON5 not-a-number literal
        "\"\\x41\"",    // JSON5 \x escape
        "\"\\v\"",      // JSON5 \v escape
        "\"a\u{1}b\"",  // raw control byte in a string
        "\"\\x41\\n\"", // JSON5 escape followed by a standard escape
    ] {
        assert!(parse_has_json5(input), "{input:?}");
    }
}

#[test]
fn scalar_string_value_rejects_overflowing_payload_length() {
    // TEXT5 header in the 8-byte size format declaring a payload
    // length near usize::MAX: the payload end computation must not
    // overflow (SQL paths validate blobs first, but this decoder is
    // public API).
    let blob = [0xF9, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xF7];
    let jsonb = Jsonb::from_raw_data(&blob).unwrap();
    assert!(jsonb.scalar_string_value().is_err());
}

#[test]
fn test_null_serialization() {
    // Create JSONB with null value
    let mut jsonb = Jsonb::new(10).unwrap();
    jsonb.data.push(ElementType::NULL as u8);

    // Test serialization
    let json_str = jsonb.to_string().unwrap();
    assert_eq!(json_str, "null");

    // Test round-trip
    let reparsed = Jsonb::from_str("null").unwrap();
    assert_eq!(reparsed.data[0], ElementType::NULL as u8);
}

#[test]
fn test_boolean_serialization() {
    // True
    let mut jsonb_true = Jsonb::new(10).unwrap();
    jsonb_true.data.push(ElementType::TRUE as u8);
    assert_eq!(jsonb_true.to_string().unwrap(), "true");

    // False
    let mut jsonb_false = Jsonb::new(10).unwrap();
    jsonb_false.data.push(ElementType::FALSE as u8);
    assert_eq!(jsonb_false.to_string().unwrap(), "false");

    // Round-trip
    let true_parsed = Jsonb::from_str("true").unwrap();
    assert_eq!(true_parsed.data[0], ElementType::TRUE as u8);

    let false_parsed = Jsonb::from_str("false").unwrap();
    assert_eq!(false_parsed.data[0], ElementType::FALSE as u8);
}

#[test]
fn test_integer_serialization() {
    // Standard integer
    let parsed = Jsonb::from_str("42").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "42");

    // Negative integer
    let parsed = Jsonb::from_str("-123").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "-123");

    // Zero
    let parsed = Jsonb::from_str("0").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "0");

    // Verify correct type
    let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
    assert!(matches!(header.0, ElementType::INT));
}

#[test]
fn test_json5_integer_serialization() {
    // Hexadecimal notation
    let parsed = Jsonb::from_str("0x1A").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "26"); // Should convert to decimal

    // Positive sign (JSON5)
    let parsed = Jsonb::from_str("+42").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "42");

    // Negative hexadecimal
    let parsed = Jsonb::from_str("-0xFF").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "-255");

    // Verify correct type
    let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
    assert!(matches!(header.0, ElementType::INT5));
}

#[test]
fn test_int5_with_multibyte_utf8_does_not_panic() {
    // Regression test for fuzzer crash: serialize_int5 would panic on string
    // slicing when the payload contained multi-byte UTF-8 characters.
    // The fix uses byte-level checks for hex prefix detection.

    // Construct malformed JSONB: INT5 header followed by UTF-8 with multi-byte chars
    // Header: (6 << 4) | 4 = 0x64 (INT5 element, 6 bytes payload)
    // Payload: "T" + U+F5D3 (3-byte UTF-8) + "?]"
    let data: Vec<u8> = vec![
        0x64, // INT5 header, 6 bytes payload
        84,   // 'T'
        239, 151, 147, // U+F5D3 (3-byte UTF-8 char)
        63,  // '?'
        93,  // ']'
    ];

    let jsonb = Jsonb::from_raw_data(&data).expect(crate::alloc::ALLOC_ERR_MSG);
    // This should not panic - the fix uses byte-level checks that handle
    // non-ASCII safely. The malformed data is rejected as invalid INT5,
    // matching SQLite's "malformed JSON" behavior.
    let err = jsonb.to_string().unwrap_err();
    assert!(err.to_string().contains("malformed JSON"));
}

#[test]
fn test_to_string_propagates_errors() {
    // Valid JSONB should succeed
    let valid = Jsonb::from_str(r#"{"key": "value"}"#).unwrap();
    assert!(valid.to_string().is_ok());

    // Malformed JSONB with invalid element type should error
    let malformed = Jsonb::from_raw_data(&[0xFF]).expect(crate::alloc::ALLOC_ERR_MSG);
    let err = malformed.to_string().unwrap_err();
    assert!(err.to_string().contains("Invalid element type"));

    // Malformed INT5 with non-numeric content should error
    let bad_int5 = Jsonb::from_raw_data(&[
        0x34, // INT5 header, 3 bytes payload
        b'a', b'b', b'c', // not a valid number
    ])
    .expect(crate::alloc::ALLOC_ERR_MSG);
    let err = bad_int5.to_string().unwrap_err();
    assert!(err.to_string().contains("malformed JSON"));

    // Empty INT5 payload should error
    let empty_int5 = Jsonb::from_raw_data(&[
        0x04, // INT5 header, 0 bytes payload
    ])
    .expect(crate::alloc::ALLOC_ERR_MSG);
    let err = empty_int5.to_string().unwrap_err();
    assert!(err.to_string().contains("malformed JSON"));

    // Sign-only INT5 payload should error
    let sign_only_int5 = Jsonb::from_raw_data(&[
        0x14, // INT5 header, 1 byte payload
        b'+',
    ])
    .expect(crate::alloc::ALLOC_ERR_MSG);
    let err = sign_only_int5.to_string().unwrap_err();
    assert!(err.to_string().contains("malformed JSON"));

    let minus_only_int5 = Jsonb::from_raw_data(&[
        0x14, // INT5 header, 1 byte payload
        b'-',
    ])
    .expect(crate::alloc::ALLOC_ERR_MSG);
    let err = minus_only_int5.to_string().unwrap_err();
    assert!(err.to_string().contains("malformed JSON"));
}

#[test]
fn test_reserved_element_types_rejected() {
    for value in [13_u8, 14, 15] {
        let err = ElementType::try_from(value).unwrap_err();
        assert!(err.to_string().contains("Invalid element type"));
    }
}

#[test]
fn test_float_serialization() {
    // Standard float
    let parsed = Jsonb::from_str("3.14159").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "3.14159");

    // Negative float
    let parsed = Jsonb::from_str("-2.718").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "-2.718");

    // Scientific notation
    let parsed = Jsonb::from_str("6.022e23").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "6.022e23");

    // Verify correct type
    let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
    assert!(matches!(header.0, ElementType::FLOAT));
}

#[test]
fn test_json5_float_serialization() {
    // Leading decimal point
    let parsed = Jsonb::from_str(".123").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "0.123");

    // Trailing decimal point
    let parsed = Jsonb::from_str("42.").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "42.0");

    // Plus sign in exponent
    let parsed = Jsonb::from_str("1.5e+10").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "1.5e+10");

    // Infinity renders as SQLite's short form
    let parsed = Jsonb::from_str("Infinity").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "9e999");

    // Negative Infinity
    let parsed = Jsonb::from_str("-Infinity").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "-9e999");

    // Verify correct type
    let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
    assert!(matches!(header.0, ElementType::FLOAT5));
}

#[test]
fn test_string_serialization() {
    // Simple string
    let parsed = Jsonb::from_str(r#""hello world""#).unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#""hello world""#);

    // String with escaped characters
    let parsed = Jsonb::from_str(r#""hello\nworld""#).unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#""hello\nworld""#);

    // Unicode escape
    let parsed = Jsonb::from_str(r#""hello\u0020world""#).unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#""hello\u0020world""#);

    // Verify correct type
    let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
    assert!(matches!(header.0, ElementType::TEXTJ));
}

#[test]
fn test_json5_string_serialization() {
    // Single quotes
    let parsed = Jsonb::from_str("'hello world'").unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#""hello world""#);

    // Hex escape
    let parsed = Jsonb::from_str(r#"'\x41\x42\x43'"#).unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#""\u0041\u0042\u0043""#);

    // Multiline string with line continuation
    let parsed = Jsonb::from_str(
        r#""hello \
world""#,
    )
    .unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#""hello world""#);

    // Escaped single quote
    let parsed = Jsonb::from_str(r#"'Don\'t worry'"#).unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#""Don't worry""#);

    // Verify correct type
    let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
    assert!(matches!(header.0, ElementType::TEXT5));

    // Multi-byte UTF-8 alongside a raw control byte (regression for #7786:
    // TEXT5 serialization was casting each raw byte to `char`, corrupting
    // multi-byte UTF-8 sequences).
    let json_input = "\"ä\nworld\"";
    let parsed = Jsonb::from_str(json_input).unwrap();
    let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
    assert!(matches!(header.0, ElementType::TEXT5));
    assert_eq!(parsed.to_string().unwrap(), "\"ä\\nworld\"");
}

#[test]
fn test_array_serialization() {
    // Empty array
    let parsed = Jsonb::from_str("[]").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "[]");

    // Simple array
    let parsed = Jsonb::from_str("[1,2,3]").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");

    // Nested array
    let parsed = Jsonb::from_str("[[1,2],[3,4]]").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "[[1,2],[3,4]]");

    // Mixed types array
    let parsed = Jsonb::from_str(r#"[1,"text",true,null,{"key":"value"}]"#).unwrap();
    assert_eq!(
        parsed.to_string().unwrap(),
        r#"[1,"text",true,null,{"key":"value"}]"#
    );

    // Verify correct type
    let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
    assert!(matches!(header.0, ElementType::ARRAY));
}

#[test]
fn test_json5_array_serialization() {
    // Trailing comma
    let parsed = Jsonb::from_str("[1,2,3,]").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");

    // Comments in array
    let parsed = Jsonb::from_str("[1,/* comment */2,3]").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");

    // Line comment in array
    let parsed = Jsonb::from_str("[1,// line comment\n2,3]").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");
}

#[test]
fn test_object_serialization() {
    // Empty object
    let parsed = Jsonb::from_str("{}").unwrap();
    assert_eq!(parsed.to_string().unwrap(), "{}");

    // Simple object
    let parsed = Jsonb::from_str(r#"{"key":"value"}"#).unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#"{"key":"value"}"#);

    // Multiple properties
    let parsed = Jsonb::from_str(r#"{"a":1,"b":2,"c":3}"#).unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#"{"a":1,"b":2,"c":3}"#);

    // Nested object
    let parsed = Jsonb::from_str(r#"{"outer":{"inner":"value"}}"#).unwrap();
    assert_eq!(
        parsed.to_string().unwrap(),
        r#"{"outer":{"inner":"value"}}"#
    );

    // Mixed values
    let parsed =
        Jsonb::from_str(r#"{"str":"text","num":42,"bool":true,"null":null,"arr":[1,2]}"#).unwrap();
    assert_eq!(
        parsed.to_string().unwrap(),
        r#"{"str":"text","num":42,"bool":true,"null":null,"arr":[1,2]}"#
    );

    // Verify correct type
    let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
    assert!(matches!(header.0, ElementType::OBJECT));
}

#[test]
fn test_json5_object_serialization() {
    // Unquoted keys
    let parsed = Jsonb::from_str("{key:\"value\"}").unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#"{"key":"value"}"#);

    // Trailing comma
    let parsed = Jsonb::from_str(r#"{"a":1,"b":2,}"#).unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#"{"a":1,"b":2}"#);

    // Comments in object
    let parsed = Jsonb::from_str(r#"{"a":1,/*comment*/"b":2}"#).unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#"{"a":1,"b":2}"#);

    // Single quotes for keys and values
    let parsed = Jsonb::from_str("{'a':'value'}").unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#"{"a":"value"}"#);
}

#[test]
fn test_complex_json() {
    let complex_json = r#"{
            "string": "Hello, world!",
            "number": 42,
            "float": 3.14159,
            "boolean": true,
            "null": null,
            "array": [1, 2, 3, "text", {"nested": "object"}],
            "object": {
                "key1": "value1",
                "key2": [4, 5, 6],
                "key3": {
                    "nested": true
                }
            }
        }"#;

    let parsed = Jsonb::from_str(complex_json).unwrap();
    // Round-trip test
    let reparsed = Jsonb::from_str(&parsed.to_string().unwrap()).unwrap();
    assert_eq!(parsed.to_string().unwrap(), reparsed.to_string().unwrap());
}

#[test]
fn test_error_handling() {
    // Invalid JSON syntax
    assert!(Jsonb::from_str("{").is_err());
    assert!(Jsonb::from_str("[").is_err());
    assert!(Jsonb::from_str("}").is_err());
    assert!(Jsonb::from_str("]").is_err());

    assert!(Jsonb::from_str(r#"{"a":"55,"b":72}"#).is_err());

    assert!(Jsonb::from_str(r#"{"a":"55",,"b":72}"#).is_err());

    // Unclosed string
    assert!(Jsonb::from_str(r#"{"key":"value"#).is_err());

    // Invalid number format
    assert!(Jsonb::from_str("01234").is_err()); // Leading zero not allowed in JSON

    // Invalid escape sequence
    assert!(Jsonb::from_str(r#""\z""#).is_err());

    // Missing colon in object
    assert!(Jsonb::from_str(r#"{"key" "value"}"#).is_err());

    // Trailing characters
    assert!(Jsonb::from_str(r#"{"key":"value"} extra"#).is_err());
}

#[test]
fn test_depth_limit() {
    // Create a JSON string that exceeds MAX_JSON_DEPTH
    let mut deep_json = String::from("[");
    for _ in 0..MAX_JSON_DEPTH + 1 {
        deep_json.push('[');
    }
    for _ in 0..MAX_JSON_DEPTH + 1 {
        deep_json.push(']');
    }
    deep_json.push(']');

    // Should fail due to exceeding depth limit
    assert!(Jsonb::from_str(&deep_json).is_err());
}

#[test]
fn test_header_encoding() {
    // Small payload (fits in 4 bits)
    let header = JsonbHeader::new(ElementType::TEXT, 5);
    let bytes = header.into_bytes().as_bytes().to_vec();
    assert_eq!(bytes[0], (5 << 4) | (ElementType::TEXT as u8));

    // Medium payload (8-bit)
    let header = JsonbHeader::new(ElementType::TEXT, 200);
    let bytes = header.into_bytes().as_bytes().to_vec();
    assert_eq!(
        bytes[0],
        (SIZE_MARKER_8BIT << 4) | (ElementType::TEXT as u8)
    );
    assert_eq!(bytes[1], 200);

    // Large payload (16-bit)
    let header = JsonbHeader::new(ElementType::TEXT, 40000);
    let bytes = header.into_bytes().as_bytes().to_vec();
    assert_eq!(
        bytes[0],
        (SIZE_MARKER_16BIT << 4) | (ElementType::TEXT as u8)
    );
    assert_eq!(bytes[1], (40000 >> 8) as u8);
    assert_eq!(bytes[2], (40000 & 0xFF) as u8);

    // Extra large payload (32-bit)
    let header = JsonbHeader::new(ElementType::TEXT, 70000);
    let bytes = header.into_bytes().as_bytes().to_vec();
    assert_eq!(
        bytes[0],
        (SIZE_MARKER_32BIT << 4) | (ElementType::TEXT as u8)
    );
    assert_eq!(bytes[1], (70000 >> 24) as u8);
    assert_eq!(bytes[2], ((70000 >> 16) & 0xFF) as u8);
    assert_eq!(bytes[3], ((70000 >> 8) & 0xFF) as u8);
    assert_eq!(bytes[4], (70000 & 0xFF) as u8);
}

#[test]
fn test_header_decoding() {
    // Create sample data with various headers
    let data = vec![
        (5 << 4) | (ElementType::TEXT as u8),
        (SIZE_MARKER_8BIT << 4) | (ElementType::ARRAY as u8),
        150,
        (SIZE_MARKER_16BIT << 4) | (ElementType::OBJECT as u8),
        0x98,
        0x68,
    ];

    // Parse and verify each header
    let (header1, offset1) = JsonbHeader::from_slice(0, &data).unwrap();
    assert_eq!(offset1, 1);
    assert_eq!(header1.0, ElementType::TEXT);
    assert_eq!(header1.1, 5);

    let (header2, offset2) = JsonbHeader::from_slice(1, &data).unwrap();
    assert_eq!(offset2, 2);
    assert_eq!(header2.0, ElementType::ARRAY);
    assert_eq!(header2.1, 150);

    let (header3, offset3) = JsonbHeader::from_slice(3, &data).unwrap();
    assert_eq!(offset3, 3);
    assert_eq!(header3.0, ElementType::OBJECT);
    assert_eq!(header3.1, 0x9868); // 39000
}

#[test]
fn test_unicode_escapes() {
    // Basic unicode escape
    let parsed = Jsonb::from_str(r#""\u00A9""#).unwrap(); // Copyright symbol
    assert_eq!(parsed.to_string().unwrap(), r#""\u00A9""#);

    // Non-BMP character (surrogate pair)
    let parsed = Jsonb::from_str(r#""\uD83D\uDE00""#).unwrap(); // Smiley emoji
    assert_eq!(parsed.to_string().unwrap(), r#""\uD83D\uDE00""#);
}

#[test]
fn test_json5_comments() {
    // Line comments
    let parsed = Jsonb::from_str(
        r#"{
            // This is a line comment
            "key": "value"
        }"#,
    )
    .unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#"{"key":"value"}"#);

    // Block comments
    let parsed = Jsonb::from_str(
        r#"{
            /* This is a
               block comment */
            "key": "value"
        }"#,
    )
    .unwrap();
    assert_eq!(parsed.to_string().unwrap(), r#"{"key":"value"}"#);

    // Comments inside array
    let parsed = Jsonb::from_str(
        r#"[1, // Comment
                                       2, /* Another comment */ 3]"#,
    )
    .unwrap();
    assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");
}

#[test]
fn test_whitespace_handling() {
    // Various whitespace patterns
    let json_with_whitespace = r#"
        {
            "key1"    :    "value1"   ,
             "key2": [   1,    2,    3   ]  ,
            "key3":   {
                "nested"   :   true
            }
        }
        "#;

    let parsed = Jsonb::from_str(json_with_whitespace).unwrap();
    assert_eq!(
        parsed.to_string().unwrap(),
        r#"{"key1":"value1","key2":[1,2,3],"key3":{"nested":true}}"#
    );
}

#[test]
fn test_binary_roundtrip() {
    // Test that binary data can be round-tripped through the JSONB format
    let original = r#"{"test":"value","array":[1,2,3]}"#;
    let parsed = Jsonb::from_str(original).unwrap();
    let binary_data = parsed.data;

    // Create a new Jsonb from the binary data
    let from_binary = Jsonb::from_raw_data(&binary_data).expect(crate::alloc::ALLOC_ERR_MSG);
    assert_eq!(from_binary.to_string().unwrap(), original);
}

#[test]
fn test_large_json() {
    // Generate a large JSON with many elements
    let mut large_array = String::from("[");
    for i in 0..1000 {
        large_array.push_str(&format!("{i}"));
        if i < 999 {
            large_array.push(',');
        }
    }
    large_array.push(']');

    let parsed = Jsonb::from_str(&large_array).unwrap();
    assert!(parsed.to_string().unwrap().starts_with("[0,1,2,"));
    assert!(parsed.to_string().unwrap().ends_with("998,999]"));
}

#[test]
fn test_jsonb_is_valid() {
    // Valid JSONB
    let jsonb = Jsonb::from_str(r#"{"test":"value"}"#).unwrap();
    assert!(jsonb.element_type().is_ok());

    // Invalid JSONB (manually corrupted)
    let mut invalid = jsonb.data;
    if !invalid.is_empty() {
        invalid[0] = 0xFF; // Invalid element type
        let jsonb = Jsonb::from_raw_data(&invalid).expect(crate::alloc::ALLOC_ERR_MSG);
        assert!(jsonb.element_type().is_err());
    }
}

#[test]
fn test_special_characters_in_strings() {
    // Test handling of various special characters
    let json = r#"{
            "escaped_quotes": "He said \"Hello\"",
            "backslashes": "C:\\Windows\\System32",
            "control_chars": "\b\f\n\r\t",
            "unicode": "\u00A9 2023"
        }"#;

    let parsed = Jsonb::from_str(json).unwrap();
    let result = parsed.to_string().unwrap();

    assert!(result.contains(r#""escaped_quotes":"He said \"Hello\"""#));
    assert!(result.contains(r#""backslashes":"C:\\Windows\\System32""#));
    assert!(result.contains(r#""control_chars":"\b\f\n\r\t""#));
    assert!(result.contains(r#""unicode":"\u00A9 2023""#));
}

#[test]
fn test_malformed_jsonb_payload_size_overflow() {
    // Test that malformed JSONB data with extremely large payload sizes
    // does not cause a panic due to integer overflow.
    // This creates JSONB data with a header indicating a payload size
    // that would overflow when added to the cursor position.
    //
    // Header format: lower 4 bits = element type, upper 4 bits = size marker
    // When upper 4 bits = 0xF (15), the payload size follows as 8 bytes

    let expected_error = "Invalid JSONB: payload size overflow";

    // Test TEXT type (0x7) with overflow payload size
    // Header: 0xF7 = TEXT type (0x7) with 8-byte size marker (0xF)
    let malformed_text: Vec<u8> = vec![
        0xF7, // TEXT with 8-byte size (header_size=15)
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // u64::MAX as payload size
        b'a', b'b', b'c', // some actual data (doesn't matter, cursor+len will overflow)
    ];
    let jsonb = Jsonb::from_raw_data(&malformed_text).expect(crate::alloc::ALLOC_ERR_MSG);
    let result = jsonb.to_string();
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains(expected_error),
        "TEXT overflow should report payload size overflow"
    );

    // Test ARRAY type (0xB = 11) with overflow payload size
    // Header: 0xFB = ARRAY type (0xB) with 8-byte size marker (0xF)
    let malformed_array: Vec<u8> = vec![
        0xFB, // ARRAY with 8-byte size (header_size=15)
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // u64::MAX as payload size
        0x01, // NULL element inside (doesn't matter, cursor+len will overflow)
    ];
    let jsonb = Jsonb::from_raw_data(&malformed_array).expect(crate::alloc::ALLOC_ERR_MSG);
    let result = jsonb.to_string();
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains(expected_error),
        "ARRAY overflow should report payload size overflow"
    );

    // Test OBJECT type (0xC = 12) with overflow payload size
    // Header: 0xFC = OBJECT type (0xC) with 8-byte size marker (0xF)
    let malformed_object: Vec<u8> = vec![
        0xFC, // OBJECT with 8-byte size (header_size=15)
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // u64::MAX as payload size
        0x17, b'k', // TEXT key "k" (doesn't matter, cursor+len will overflow)
    ];
    let jsonb = Jsonb::from_raw_data(&malformed_object).expect(crate::alloc::ALLOC_ERR_MSG);
    let result = jsonb.to_string();
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains(expected_error),
        "OBJECT overflow should report payload size overflow"
    );
}
