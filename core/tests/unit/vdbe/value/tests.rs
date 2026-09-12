use crate::numeric::Numeric;
use crate::types::Value;
use crate::vdbe::Register;

use rand::{Rng, RngCore};

fn blob(bytes: &[u8]) -> Value {
    Value::from_slice(bytes).expect(crate::alloc::ALLOC_ERR_MSG)
}

fn allocated(result: std::result::Result<Value, crate::alloc::TryReserveError>) -> Value {
    result.expect(crate::alloc::ALLOC_ERR_MSG)
}

#[test]
fn exec_concat_builds_blob_fallibly() {
    let lhs = blob(&[1, 2]);
    let rhs = blob(&[3, 4]);

    assert_eq!(lhs.exec_concat(&rhs).unwrap(), blob(&[1, 2, 3, 4]));
}

#[test]
fn test_exec_add() {
    let inputs = vec![
        (Value::from_i64(3), Value::from_i64(1)),
        (Value::from_f64(3.0), Value::from_f64(1.0)),
        (Value::from_f64(3.0), Value::from_i64(1)),
        (Value::from_i64(3), Value::from_f64(1.0)),
        (Value::Null, Value::Null),
        (Value::Null, Value::from_i64(1)),
        (Value::Null, Value::from_f64(1.0)),
        (Value::Null, Value::Text("2".into())),
        (Value::from_i64(1), Value::Null),
        (Value::from_f64(1.0), Value::Null),
        (Value::Text("1".into()), Value::Null),
        (Value::Text("1".into()), Value::Text("3".into())),
        (Value::Text("1.0".into()), Value::Text("3.0".into())),
        (Value::Text("1.0".into()), Value::from_f64(3.0)),
        (Value::Text("1.0".into()), Value::from_i64(3)),
        (Value::from_f64(1.0), Value::Text("3.0".into())),
        (Value::from_i64(1), Value::Text("3".into())),
    ];

    let outputs = [
        Value::from_i64(4),
        Value::from_f64(4.0),
        Value::from_f64(4.0),
        Value::from_f64(4.0),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::from_i64(4),
        Value::from_f64(4.0),
        Value::from_f64(4.0),
        Value::from_f64(4.0),
        Value::from_f64(4.0),
        Value::from_f64(4.0),
    ];

    assert_eq!(
        inputs.len(),
        outputs.len(),
        "Inputs and Outputs should have same size"
    );
    for (i, (lhs, rhs)) in inputs.iter().enumerate() {
        assert_eq!(
            lhs.exec_add(rhs),
            outputs[i],
            "Wrong ADD for lhs: {lhs}, rhs: {rhs}"
        );
    }
}

#[test]
fn test_exec_subtract() {
    let inputs = vec![
        (Value::from_i64(3), Value::from_i64(1)),
        (Value::from_f64(3.0), Value::from_f64(1.0)),
        (Value::from_f64(3.0), Value::from_i64(1)),
        (Value::from_i64(3), Value::from_f64(1.0)),
        (Value::Null, Value::Null),
        (Value::Null, Value::from_i64(1)),
        (Value::Null, Value::from_f64(1.0)),
        (Value::Null, Value::Text("1".into())),
        (Value::from_i64(1), Value::Null),
        (Value::from_f64(1.0), Value::Null),
        (Value::Text("4".into()), Value::Null),
        (Value::Text("1".into()), Value::Text("3".into())),
        (Value::Text("1.0".into()), Value::Text("3.0".into())),
        (Value::Text("1.0".into()), Value::from_f64(3.0)),
        (Value::Text("1.0".into()), Value::from_i64(3)),
        (Value::from_f64(1.0), Value::Text("3.0".into())),
        (Value::from_i64(1), Value::Text("3".into())),
    ];

    let outputs = [
        Value::from_i64(2),
        Value::from_f64(2.0),
        Value::from_f64(2.0),
        Value::from_f64(2.0),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::from_i64(-2),
        Value::from_f64(-2.0),
        Value::from_f64(-2.0),
        Value::from_f64(-2.0),
        Value::from_f64(-2.0),
        Value::from_f64(-2.0),
    ];

    assert_eq!(
        inputs.len(),
        outputs.len(),
        "Inputs and Outputs should have same size"
    );
    for (i, (lhs, rhs)) in inputs.iter().enumerate() {
        assert_eq!(
            lhs.exec_subtract(rhs),
            outputs[i],
            "Wrong subtract for lhs: {lhs}, rhs: {rhs}"
        );
    }
}

#[test]
fn test_exec_get_byte() {
    // PostgreSQL: get_byte('\x1234567890'::bytea, 4) = 144.
    let input = blob(&[0x12, 0x34, 0x56, 0x78, 0x90]);
    assert_eq!(
        input.exec_get_byte(&Value::from_i64(4)).unwrap(),
        Value::from_i64(144)
    );
    assert_eq!(
        input.exec_get_byte(&Value::from_i64(0)).unwrap(),
        Value::from_i64(0x12)
    );
    // Text is read as its UTF-8 bytes ('A' == 65).
    assert_eq!(
        Value::build_text("ABC")
            .exec_get_byte(&Value::from_i64(0))
            .unwrap(),
        Value::from_i64(65)
    );
    // A text offset that casts to an integer is accepted.
    assert_eq!(
        input.exec_get_byte(&Value::build_text("4")).unwrap(),
        Value::from_i64(144)
    );
    // NULL input or offset yields NULL.
    assert_eq!(
        Value::Null.exec_get_byte(&Value::from_i64(0)).unwrap(),
        Value::Null
    );
    assert_eq!(input.exec_get_byte(&Value::Null).unwrap(), Value::Null);
    // Out-of-range and negative offsets raise an error, as does an empty blob.
    assert!(input.exec_get_byte(&Value::from_i64(5)).is_err());
    assert!(input.exec_get_byte(&Value::from_i64(-1)).is_err());
    assert!(blob(&[]).exec_get_byte(&Value::from_i64(0)).is_err());
}

#[test]
fn test_exec_set_byte() {
    let input = blob(&[0x12, 0x34, 0x56, 0x78, 0x90]);
    // PostgreSQL: set_byte('\x1234567890'::bytea, 4, 64) = '\x1234567840'.
    assert_eq!(
        input
            .exec_set_byte(&Value::from_i64(4), &Value::from_i64(64))
            .unwrap(),
        blob(&[0x12, 0x34, 0x56, 0x78, 0x40])
    );
    // Values wrap to their low 8 bits: 6555 & 0xff == 0x9b.
    assert_eq!(
        input
            .exec_set_byte(&Value::from_i64(4), &Value::from_i64(6555))
            .unwrap(),
        blob(&[0x12, 0x34, 0x56, 0x78, 0x9b])
    );
    // Negative values wrap too: -1 -> 0xff.
    assert_eq!(
        input
            .exec_set_byte(&Value::from_i64(4), &Value::from_i64(-1))
            .unwrap(),
        blob(&[0x12, 0x34, 0x56, 0x78, 0xff])
    );
    // NULL in any argument yields NULL.
    assert_eq!(
        Value::Null
            .exec_set_byte(&Value::from_i64(0), &Value::from_i64(1))
            .unwrap(),
        Value::Null
    );
    assert_eq!(
        input
            .exec_set_byte(&Value::Null, &Value::from_i64(1))
            .unwrap(),
        Value::Null
    );
    assert_eq!(
        input
            .exec_set_byte(&Value::from_i64(0), &Value::Null)
            .unwrap(),
        Value::Null
    );
    // Out-of-range and negative offsets raise an error.
    assert!(input
        .exec_set_byte(&Value::from_i64(5), &Value::from_i64(0))
        .is_err());
    assert!(input
        .exec_set_byte(&Value::from_i64(-1), &Value::from_i64(0))
        .is_err());
}

#[test]
fn test_exec_multiply() {
    let inputs = vec![
        (Value::from_i64(3), Value::from_i64(2)),
        (Value::from_f64(3.0), Value::from_f64(2.0)),
        (Value::from_f64(3.0), Value::from_i64(2)),
        (Value::from_i64(3), Value::from_f64(2.0)),
        (Value::Null, Value::Null),
        (Value::Null, Value::from_i64(1)),
        (Value::Null, Value::from_f64(1.0)),
        (Value::Null, Value::Text("1".into())),
        (Value::from_i64(1), Value::Null),
        (Value::from_f64(1.0), Value::Null),
        (Value::Text("4".into()), Value::Null),
        (Value::Text("2".into()), Value::Text("3".into())),
        (Value::Text("2.0".into()), Value::Text("3.0".into())),
        (Value::Text("2.0".into()), Value::from_f64(3.0)),
        (Value::Text("2.0".into()), Value::from_i64(3)),
        (Value::from_f64(2.0), Value::Text("3.0".into())),
        (Value::from_i64(2), Value::Text("3.0".into())),
    ];

    let outputs = [
        Value::from_i64(6),
        Value::from_f64(6.0),
        Value::from_f64(6.0),
        Value::from_f64(6.0),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::from_i64(6),
        Value::from_f64(6.0),
        Value::from_f64(6.0),
        Value::from_f64(6.0),
        Value::from_f64(6.0),
        Value::from_f64(6.0),
    ];

    assert_eq!(
        inputs.len(),
        outputs.len(),
        "Inputs and Outputs should have same size"
    );
    for (i, (lhs, rhs)) in inputs.iter().enumerate() {
        assert_eq!(
            lhs.exec_multiply(rhs),
            outputs[i],
            "Wrong multiply for lhs: {lhs}, rhs: {rhs}"
        );
    }
}

#[test]
fn test_exec_divide() {
    let inputs = vec![
        (Value::from_i64(1), Value::from_i64(0)),
        (Value::from_f64(1.0), Value::from_f64(0.0)),
        (Value::from_i64(i64::MIN), Value::from_i64(-1)),
        (Value::from_f64(6.0), Value::from_f64(2.0)),
        (Value::from_f64(6.0), Value::from_i64(2)),
        (Value::from_i64(6), Value::from_i64(2)),
        (Value::Null, Value::from_i64(2)),
        (Value::from_i64(2), Value::Null),
        (Value::Null, Value::Null),
        (Value::Text("6".into()), Value::Text("2".into())),
        (Value::Text("6".into()), Value::from_i64(2)),
    ];

    let outputs = [
        Value::Null,
        Value::Null,
        Value::from_f64(9.223372036854776e18),
        Value::from_f64(3.0),
        Value::from_f64(3.0),
        Value::from_f64(3.0),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::from_f64(3.0),
        Value::from_f64(3.0),
    ];

    assert_eq!(
        inputs.len(),
        outputs.len(),
        "Inputs and Outputs should have same size"
    );
    for (i, (lhs, rhs)) in inputs.iter().enumerate() {
        assert_eq!(
            lhs.exec_divide(rhs),
            outputs[i],
            "Wrong divide for lhs: {lhs}, rhs: {rhs}"
        );
    }
}

#[test]
fn test_exec_remainder() {
    let inputs = vec![
        (Value::Null, Value::Null),
        (Value::Null, Value::from_f64(1.0)),
        (Value::Null, Value::from_i64(1)),
        (Value::Null, Value::Text("1".into())),
        (Value::from_f64(1.0), Value::Null),
        (Value::from_i64(1), Value::Null),
        (Value::from_i64(12), Value::from_i64(0)),
        (Value::from_f64(12.0), Value::from_f64(0.0)),
        (Value::from_f64(12.0), Value::from_i64(0)),
        (Value::from_i64(12), Value::from_f64(0.0)),
        (Value::from_i64(i64::MIN), Value::from_i64(-1)),
        (Value::from_i64(12), Value::from_i64(3)),
        (Value::from_f64(12.0), Value::from_f64(3.0)),
        (Value::from_f64(12.0), Value::from_i64(3)),
        (Value::from_i64(12), Value::from_f64(3.0)),
        (Value::from_i64(12), Value::from_i64(-3)),
        (Value::from_f64(12.0), Value::from_f64(-3.0)),
        (Value::from_f64(12.0), Value::from_i64(-3)),
        (Value::from_i64(12), Value::from_f64(-3.0)),
        (Value::Text("12.0".into()), Value::Text("3.0".into())),
        (Value::Text("12.0".into()), Value::from_f64(3.0)),
        (Value::from_f64(12.0), Value::Text("3.0".into())),
    ];
    let outputs = vec![
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::from_f64(0.0),
        Value::from_i64(0),
        Value::from_f64(0.0),
        Value::from_f64(0.0),
        Value::from_f64(0.0),
        Value::from_i64(0),
        Value::from_f64(0.0),
        Value::from_f64(0.0),
        Value::from_f64(0.0),
        Value::from_f64(0.0),
        Value::from_f64(0.0),
        Value::from_f64(0.0),
    ];

    assert_eq!(
        inputs.len(),
        outputs.len(),
        "Inputs and Outputs should have same size"
    );

    for (i, (lhs, rhs)) in inputs.iter().enumerate() {
        assert_eq!(
            lhs.exec_remainder(rhs),
            outputs[i],
            "Wrong remainder for lhs: {lhs}, rhs: {rhs}"
        );
    }
}

#[test]
fn test_exec_and() {
    let inputs = vec![
        (Value::from_i64(0), Value::Null),
        (Value::Null, Value::from_i64(1)),
        (Value::Null, Value::Null),
        (Value::from_f64(0.0), Value::Null),
        (Value::from_i64(1), Value::from_f64(2.2)),
        (Value::from_i64(0), Value::Text("string".into())),
        (Value::from_i64(0), Value::Text("1".into())),
        (Value::from_i64(1), Value::Text("1".into())),
    ];
    let outputs = [
        Value::from_i64(0),
        Value::Null,
        Value::Null,
        Value::from_i64(0),
        Value::from_i64(1),
        Value::from_i64(0),
        Value::from_i64(0),
        Value::from_i64(1),
    ];

    assert_eq!(
        inputs.len(),
        outputs.len(),
        "Inputs and Outputs should have same size"
    );
    for (i, (lhs, rhs)) in inputs.iter().enumerate() {
        assert_eq!(
            lhs.exec_and(rhs),
            outputs[i],
            "Wrong AND for lhs: {lhs}, rhs: {rhs}"
        );
    }
}

#[test]
fn test_exec_or() {
    let inputs = vec![
        (Value::from_i64(0), Value::Null),
        (Value::Null, Value::from_i64(1)),
        (Value::Null, Value::Null),
        (Value::from_f64(0.0), Value::Null),
        (Value::from_i64(1), Value::from_f64(2.2)),
        (Value::from_f64(0.0), Value::from_i64(0)),
        (Value::from_i64(0), Value::Text("string".into())),
        (Value::from_i64(0), Value::Text("1".into())),
        (Value::from_i64(0), Value::Text("".into())),
    ];
    let outputs = [
        Value::Null,
        Value::from_i64(1),
        Value::Null,
        Value::Null,
        Value::from_i64(1),
        Value::from_i64(0),
        Value::from_i64(0),
        Value::from_i64(1),
        Value::from_i64(0),
    ];

    assert_eq!(
        inputs.len(),
        outputs.len(),
        "Inputs and Outputs should have same size"
    );
    for (i, (lhs, rhs)) in inputs.iter().enumerate() {
        assert_eq!(
            lhs.exec_or(rhs),
            outputs[i],
            "Wrong OR for lhs: {lhs}, rhs: {rhs}"
        );
    }
}

#[test]
fn test_length() {
    let input_str = Value::build_text("bob");
    let expected_len = Value::from_i64(3);
    assert_eq!(input_str.exec_length(), expected_len);

    let input_integer = Value::from_i64(123);
    let expected_len = Value::from_i64(3);
    assert_eq!(input_integer.exec_length(), expected_len);

    let input_float = Value::from_f64(123.456);
    let expected_len = Value::from_i64(7);
    assert_eq!(input_float.exec_length(), expected_len);

    let expected_blob = blob(b"example");
    let expected_len = Value::from_i64(7);
    assert_eq!(expected_blob.exec_length(), expected_len);
}

#[test]
fn test_quote() {
    let input = Value::build_text("abc\0edf");
    let expected = Value::build_text("'abc'");
    assert_eq!(input.exec_quote(), expected);

    let input = Value::from_i64(123);
    let expected = Value::build_text("123");
    assert_eq!(input.exec_quote(), expected);

    let input = Value::from_f64(12.34);
    let expected = Value::build_text("12.34");
    assert_eq!(input.exec_quote(), expected);

    let input = Value::build_text("hello''world");
    let expected = Value::build_text("'hello''''world'");
    assert_eq!(input.exec_quote(), expected);

    let input = Value::from_f64(
        crate::numeric::str_to_f64("2.042747795102219097e+05")
            .map(f64::from)
            .unwrap(),
    );
    let expected = Value::build_text("2.042747795102219097e+05");
    assert_eq!(input.exec_quote(), expected);
}

#[test]
fn test_typeof() {
    let input = Value::Null;
    let expected: Value = Value::build_text("null");
    assert_eq!(input.exec_typeof(), expected);

    let input = Value::from_i64(123);
    let expected: Value = Value::build_text("integer");
    assert_eq!(input.exec_typeof(), expected);

    let input = Value::from_f64(123.456);
    let expected: Value = Value::build_text("real");
    assert_eq!(input.exec_typeof(), expected);

    let input = Value::build_text("hello");
    let expected: Value = Value::build_text("text");
    assert_eq!(input.exec_typeof(), expected);

    let input = blob(b"limbo");
    let expected: Value = Value::build_text("blob");
    assert_eq!(input.exec_typeof(), expected);
}

#[test]
fn test_unicode() {
    assert_eq!(Value::build_text("a").exec_unicode(), Value::from_i64(97));
    assert_eq!(
        Value::build_text("😊").exec_unicode(),
        Value::from_i64(128522)
    );
    assert_eq!(Value::build_text("").exec_unicode(), Value::Null);
    assert_eq!(Value::build_text("\0").exec_unicode(), Value::Null);
    assert_eq!(Value::from_i64(23).exec_unicode(), Value::from_i64(50));
    assert_eq!(Value::from_i64(0).exec_unicode(), Value::from_i64(48));
    assert_eq!(Value::from_f64(0.0).exec_unicode(), Value::from_i64(48));
    assert_eq!(Value::from_f64(23.45).exec_unicode(), Value::from_i64(50));
    assert_eq!(Value::Null.exec_unicode(), Value::Null);
    assert_eq!(blob(b"example").exec_unicode(), Value::from_i64(101));
}

#[test]
fn test_unistr() {
    // Each escape form individually
    assert_eq!(
        Value::build_text(r"\u0041").exec_unistr().unwrap(),
        Value::build_text("A")
    );
    assert_eq!(
        Value::build_text(r"\0041").exec_unistr().unwrap(),
        Value::build_text("A")
    );
    assert_eq!(
        Value::build_text(r"\+01F600").exec_unistr().unwrap(),
        Value::build_text("😀")
    );
    assert_eq!(
        Value::build_text(r"\U0001F600").exec_unistr().unwrap(),
        Value::build_text("😀")
    );
    // Escaped backslash
    assert_eq!(
        Value::build_text(r"a\\b").exec_unistr().unwrap(),
        Value::build_text(r"a\b")
    );
    // Hex is case-insensitive
    assert_eq!(
        Value::build_text(r"\u00E4").exec_unistr().unwrap(),
        Value::build_text("ä")
    );
    assert_eq!(
        Value::build_text(r"\u00e4").exec_unistr().unwrap(),
        Value::build_text("ä")
    );
    // Multiple escapes in one string
    assert_eq!(
        Value::build_text(r"\u0048\u0065\u006C\u006C\u006F")
            .exec_unistr()
            .unwrap(),
        Value::build_text("Hello")
    );
    // Mixed literal and escape forms
    assert_eq!(
        Value::build_text(r"hi \u0041 \U0001F600")
            .exec_unistr()
            .unwrap(),
        Value::build_text("hi A 😀")
    );
    // No escapes
    assert_eq!(
        Value::build_text("hello").exec_unistr().unwrap(),
        Value::build_text("hello")
    );
    // Empty string
    assert_eq!(
        Value::build_text("").exec_unistr().unwrap(),
        Value::build_text("")
    );
    // NULL input
    assert_eq!(Value::Null.exec_unistr().unwrap(), Value::Null);
    // NUL codepoint accepted (matches SQLite, which carries NUL via explicit length)
    assert_eq!(
        Value::build_text(r"\u0000").exec_unistr().unwrap(),
        Value::build_text("\0")
    );
    // Surrogate rejected (Value::Text requires valid UTF-8)
    assert!(Value::build_text(r"\uD83D").exec_unistr().is_err());
    // Above U+10FFFF rejected
    assert!(Value::build_text(r"\U00110000").exec_unistr().is_err());
    // Malformed escapes
    assert!(Value::build_text(r"\q").exec_unistr().is_err());
    assert!(Value::build_text(r"\u00").exec_unistr().is_err());
    assert!(Value::build_text("abc\\").exec_unistr().is_err());
    // Non-hex in fixed-width span
    assert!(Value::build_text(r"\u00GG").exec_unistr().is_err());
    assert!(Value::build_text(r"\+01FG00").exec_unistr().is_err());
    assert!(Value::build_text(r"\U0001F6GG").exec_unistr().is_err());
}

#[test]
fn test_unistr_quote() {
    assert_eq!(Value::Null.exec_unistr_quote(), Value::build_text("NULL"));
    assert_eq!(
        Value::from_i64(42).exec_unistr_quote(),
        Value::build_text("42")
    );
    assert_eq!(
        Value::from_f64(1.5).exec_unistr_quote(),
        Value::build_text("1.5")
    );
    assert_eq!(
        blob(&[0xDE, 0xAD]).exec_unistr_quote(),
        Value::build_text("X'DEAD'")
    );
    assert_eq!(
        Value::build_text("hello").exec_unistr_quote(),
        Value::build_text("'hello'")
    );
    // Backslash is NOT doubled when no control chars are present
    assert_eq!(
        Value::build_text("a\\b").exec_unistr_quote(),
        Value::build_text("'a\\b'")
    );
    assert_eq!(
        Value::build_text("it's").exec_unistr_quote(),
        Value::build_text("'it''s'")
    );
    assert_eq!(
        Value::build_text("a\tb").exec_unistr_quote(),
        Value::build_text("unistr('a\\u0009b')")
    );
    assert_eq!(
        Value::build_text("a\t\\b").exec_unistr_quote(),
        Value::build_text("unistr('a\\u0009\\\\b')")
    );
    assert_eq!(
        Value::build_text("a\tb'c").exec_unistr_quote(),
        Value::build_text("unistr('a\\u0009b''c')")
    );
    assert_eq!(
        Value::build_text("\x01abc'\\\t\n\r\x1fXYZ\0\x01tail").exec_unistr_quote(),
        Value::build_text(r"unistr('\u0001abc''\\\u0009\u000a\u000d\u001fXYZ')")
    );
    assert_eq!(
        Value::build_text("a\x01b\0c").exec_unistr_quote(),
        Value::build_text("unistr('a\\u0001b')")
    );
    assert_eq!(
        Value::build_text("\x01").exec_unistr_quote(),
        Value::build_text("unistr('\\u0001')")
    );
    assert_eq!(
        Value::build_text("\x01\x1f").exec_unistr_quote(),
        Value::build_text("unistr('\\u0001\\u001f')")
    );
    assert_eq!(
        Value::build_text("\x10").exec_unistr_quote(),
        Value::build_text("unistr('\\u0010')")
    );
    assert_eq!(
        Value::build_text("\x1f").exec_unistr_quote(),
        Value::build_text("unistr('\\u001f')")
    );
    // 0x20 is the first char outside the control range
    assert_eq!(
        Value::build_text(" ").exec_unistr_quote(),
        Value::build_text("' '")
    );
    assert_eq!(
        Value::build_text("\0abc").exec_unistr_quote(),
        Value::build_text("''")
    );
    assert_eq!(
        Value::build_text("").exec_unistr_quote(),
        Value::build_text("''")
    );
    assert_eq!(
        Value::build_text("a\nb").exec_unistr_quote(),
        Value::build_text("unistr('a\\u000ab')")
    );
    assert_eq!(
        Value::build_text("a\rb").exec_unistr_quote(),
        Value::build_text("unistr('a\\u000db')")
    );
    assert_eq!(
        Value::build_text("a\0\t").exec_unistr_quote(),
        Value::build_text("'a'")
    );
}

#[test]
fn test_min_max() {
    let input_int_vec = [
        Register::Value(Value::from_i64(-1)),
        Register::Value(Value::from_i64(10)),
    ];
    assert_eq!(
        Value::exec_min(input_int_vec.iter().map(|v| v.get_value())),
        Value::from_i64(-1)
    );
    assert_eq!(
        Value::exec_max(input_int_vec.iter().map(|v| v.get_value())),
        Value::from_i64(10)
    );

    let str1 = Register::Value(Value::build_text("A"));
    let str2 = Register::Value(Value::build_text("z"));
    let input_str_vec = [str2, str1.clone()];
    assert_eq!(
        Value::exec_min(input_str_vec.iter().map(|v| v.get_value())),
        Value::build_text("A")
    );
    assert_eq!(
        Value::exec_max(input_str_vec.iter().map(|v| v.get_value())),
        Value::build_text("z")
    );

    let input_null_vec = [Register::Value(Value::Null), Register::Value(Value::Null)];
    assert_eq!(
        Value::exec_min(input_null_vec.iter().map(|v| v.get_value())),
        Value::Null
    );
    assert_eq!(
        Value::exec_max(input_null_vec.iter().map(|v| v.get_value())),
        Value::Null
    );

    let input_mixed_vec = [Register::Value(Value::from_i64(10)), str1];
    assert_eq!(
        Value::exec_min(input_mixed_vec.iter().map(|v| v.get_value())),
        Value::from_i64(10)
    );
    assert_eq!(
        Value::exec_max(input_mixed_vec.iter().map(|v| v.get_value())),
        Value::build_text("A")
    );

    // SQLite: multi-arg min/max returns NULL if ANY argument is NULL
    let input_with_null = [
        Register::Value(Value::from_i64(1)),
        Register::Value(Value::Null),
    ];
    assert_eq!(
        Value::exec_min(input_with_null.iter().map(|v| v.get_value())),
        Value::Null
    );
    assert_eq!(
        Value::exec_max(input_with_null.iter().map(|v| v.get_value())),
        Value::Null
    );
}

#[test]
fn test_trim() {
    let input_str = Value::build_text("     Bob and Alice     ");
    let expected_str = Value::build_text("Bob and Alice");
    assert_eq!(input_str.exec_trim(None), expected_str);

    let input_str = Value::build_text("     Bob and Alice     ");
    let pattern_str = Value::build_text("Bob and");
    let expected_str = Value::build_text("Alice");
    assert_eq!(input_str.exec_trim(Some(&pattern_str)), expected_str);

    let input_str = Value::build_text("\ta");
    let expected_str = Value::build_text("\ta");
    assert_eq!(input_str.exec_trim(None), expected_str);

    let input_str = Value::build_text("\na");
    let expected_str = Value::build_text("\na");
    assert_eq!(input_str.exec_trim(None), expected_str);

    // TRIM on Integer should return TEXT (SQLite compatibility)
    let input_int = Value::from_i64(12345);
    let expected_text = Value::build_text("12345");
    assert_eq!(input_int.exec_trim(None), expected_text);

    // TRIM on Float should return TEXT (SQLite compatibility)
    let input_float = Value::from_f64(123.5);
    let expected_text = Value::build_text("123.5");
    assert_eq!(input_float.exec_trim(None), expected_text);
}

#[test]
fn test_ltrim() {
    let input_str = Value::build_text("     Bob and Alice     ");
    let expected_str = Value::build_text("Bob and Alice     ");
    assert_eq!(input_str.exec_ltrim(None), expected_str);

    let input_str = Value::build_text("     Bob and Alice     ");
    let pattern_str = Value::build_text("Bob and");
    let expected_str = Value::build_text("Alice     ");
    assert_eq!(input_str.exec_ltrim(Some(&pattern_str)), expected_str);
}

#[test]
fn test_rtrim() {
    let input_str = Value::build_text("     Bob and Alice     ");
    let expected_str = Value::build_text("     Bob and Alice");
    assert_eq!(input_str.exec_rtrim(None), expected_str);

    let input_str = Value::build_text("     Bob and Alice     ");
    let pattern_str = Value::build_text("Bob and");
    let expected_str = Value::build_text("     Bob and Alice");
    assert_eq!(input_str.exec_rtrim(Some(&pattern_str)), expected_str);

    let input_str = Value::build_text("     Bob and Alice     ");
    let pattern_str = Value::build_text("and Alice");
    let expected_str = Value::build_text("     Bob");
    assert_eq!(input_str.exec_rtrim(Some(&pattern_str)), expected_str);
}

#[test]
fn test_soundex() {
    let input_str = Value::build_text("Pfister");
    let expected_str = Value::build_text("P236");
    assert_eq!(input_str.exec_soundex(), expected_str);

    let input_str = Value::build_text("husobee");
    let expected_str = Value::build_text("H210");
    assert_eq!(input_str.exec_soundex(), expected_str);

    let input_str = Value::build_text("Tymczak");
    let expected_str = Value::build_text("T522");
    assert_eq!(input_str.exec_soundex(), expected_str);

    let input_str = Value::build_text("Ashcraft");
    let expected_str = Value::build_text("A261");
    assert_eq!(input_str.exec_soundex(), expected_str);

    let input_str = Value::build_text("Robert");
    let expected_str = Value::build_text("R163");
    assert_eq!(input_str.exec_soundex(), expected_str);

    let input_str = Value::build_text("Rupert");
    let expected_str = Value::build_text("R163");
    assert_eq!(input_str.exec_soundex(), expected_str);

    let input_str = Value::build_text("Rubin");
    let expected_str = Value::build_text("R150");
    assert_eq!(input_str.exec_soundex(), expected_str);

    let input_str = Value::build_text("Kant");
    let expected_str = Value::build_text("K530");
    assert_eq!(input_str.exec_soundex(), expected_str);

    let input_str = Value::build_text("Knuth");
    let expected_str = Value::build_text("K530");
    assert_eq!(input_str.exec_soundex(), expected_str);

    let input_str = Value::build_text("x");
    let expected_str = Value::build_text("X000");
    assert_eq!(input_str.exec_soundex(), expected_str);

    let input_str = Value::build_text("闪电五连鞭");
    let expected_str = Value::build_text("?000");
    assert_eq!(input_str.exec_soundex(), expected_str);
}

#[test]
fn test_upper_case() {
    let input_str = Value::build_text("Limbo");
    let expected_str = Value::build_text("LIMBO");
    assert_eq!(input_str.exec_upper().unwrap(), expected_str);

    let input_int = Value::from_i64(10);
    assert_eq!(input_int.exec_upper().unwrap(), Value::build_text("10"));
    assert_eq!(Value::Null.exec_upper(), None)
}

#[test]
fn test_lower_case() {
    let input_str = Value::build_text("Limbo");
    let expected_str = Value::build_text("limbo");
    assert_eq!(input_str.exec_lower().unwrap(), expected_str);

    let input_int = Value::from_i64(10);
    assert_eq!(input_int.exec_lower().unwrap(), Value::build_text("10"));
    assert_eq!(Value::Null.exec_lower(), None)
}

#[test]
fn test_hex() {
    let input_str = Value::build_text("limbo");
    let expected_val = Value::build_text("6C696D626F");
    assert_eq!(input_str.exec_hex(), expected_val);

    let input_int = Value::from_i64(100);
    let expected_val = Value::build_text("313030");
    assert_eq!(input_int.exec_hex(), expected_val);

    let input_float = Value::from_f64(12.34);
    let expected_val = Value::build_text("31322E3334");
    assert_eq!(input_float.exec_hex(), expected_val);

    let input_blob = blob(&[0xff]);
    let expected_val = Value::build_text("FF");
    assert_eq!(input_blob.exec_hex(), expected_val);
}

#[test]
fn test_cast_blob_preserves_blob_bytes() {
    let input_blob = blob(&[0xd2, 0x64, 0xc0, 0x07, 0xf6, 0x44, 0xe4, 0x59]);
    let expected = input_blob.clone();

    assert_eq!(allocated(input_blob.exec_cast("BLOB")), expected);
}

#[test]
fn test_unhex() {
    let input = Value::build_text("6f");
    let expected = blob(&[0x6f]);
    assert_eq!(input.exec_unhex(None), expected);

    let input = Value::build_text("6f");
    let expected = blob(&[0x6f]);
    assert_eq!(input.exec_unhex(None), expected);

    let input = Value::build_text("611");
    let expected = Value::Null;
    assert_eq!(input.exec_unhex(None), expected);

    let input = Value::build_text("");
    let expected = blob(&[]);
    assert_eq!(input.exec_unhex(None), expected);

    let input = Value::build_text("61x");
    let expected = Value::Null;
    assert_eq!(input.exec_unhex(None), expected);

    let input = Value::Null;
    let expected = Value::Null;
    assert_eq!(input.exec_unhex(None), expected);

    let input = Value::build_text("aa-bb");
    let expected = blob(&[0xaa, 0xbb]);
    assert_eq!(input.exec_unhex(Some(&Value::build_text("-"))), expected);

    let input = Value::build_text("aa--bb");
    let expected = blob(&[0xaa, 0xbb]);
    assert_eq!(input.exec_unhex(Some(&Value::build_text("-"))), expected);

    let input = Value::build_text("aa-bb-cc");
    let expected = blob(&[0xaa, 0xbb, 0xcc]);
    assert_eq!(input.exec_unhex(Some(&Value::build_text("-"))), expected);

    let input = Value::build_text("aa bb");
    let expected = blob(&[0xaa, 0xbb]);
    assert_eq!(input.exec_unhex(Some(&Value::build_text(" "))), expected);

    let input = Value::build_text("A BCD");
    let expected = Value::Null;
    assert_eq!(input.exec_unhex(Some(&Value::build_text(" "))), expected);

    let input = Value::build_text("yx2xEzyx");
    let expected = Value::Null;
    assert_eq!(input.exec_unhex(Some(&Value::build_text("xyz"))), expected);

    let input = Value::build_text("aa?bb");
    let expected = Value::Null;
    assert_eq!(input.exec_unhex(Some(&Value::build_text("-"))), expected);

    let input = Value::build_text("aabb");
    let expected = Value::Null;
    assert_eq!(input.exec_unhex(Some(&Value::Null)), expected);
}

#[test]
fn test_abs() {
    let int_positive_reg = Value::from_i64(10);
    let int_negative_reg = Value::from_i64(-10);
    assert_eq!(int_positive_reg.exec_abs().unwrap(), int_positive_reg);
    assert_eq!(int_negative_reg.exec_abs().unwrap(), int_positive_reg);

    let float_positive_reg = Value::from_i64(10);
    let float_negative_reg = Value::from_i64(-10);
    assert_eq!(float_positive_reg.exec_abs().unwrap(), float_positive_reg);
    assert_eq!(float_negative_reg.exec_abs().unwrap(), float_positive_reg);

    assert_eq!(
        Value::build_text("a").exec_abs().unwrap(),
        Value::from_f64(0.0)
    );
    assert_eq!(Value::Null.exec_abs().unwrap(), Value::Null);

    // ABS(i64::MIN) should return RuntimeError
    assert!(Value::from_i64(i64::MIN).exec_abs().is_err());
}

#[test]
fn test_char() {
    assert_eq!(
        Value::exec_char(
            [
                Register::Value(Value::from_i64(108)),
                Register::Value(Value::from_i64(105))
            ]
            .iter()
            .map(|reg| reg.get_value())
        ),
        Value::build_text("li")
    );
    assert_eq!(Value::exec_char(std::iter::empty()), Value::build_text(""));
    assert_eq!(
        Value::exec_char(
            [Register::Value(Value::Null)]
                .iter()
                .map(|reg| reg.get_value())
        ),
        Value::build_text("\0")
    );
    // Non-numeric text coerces to integer 0, so char('a') is a NUL byte,
    // the same as SQLite (it feeds every argument through integer coercion).
    assert_eq!(
        Value::exec_char(
            [Register::Value(Value::build_text("a"))]
                .iter()
                .map(|reg| reg.get_value())
        ),
        Value::build_text("\0")
    );
}

#[test]
fn test_like_with_escape_or_regexmeta_chars() {
    assert!(Value::exec_like(r#"\%A"#, r#"\A"#, None).unwrap());
    assert!(Value::exec_like("%a%a", "aaaa", None).unwrap());
}

#[test]
fn like_ascii_agrees_with_pattern_compare() {
    fn words(alphabet: &[u8], max_len: usize) -> Vec<Vec<u8>> {
        let mut all = vec![Vec::new()];
        let mut last = vec![Vec::new()];
        for _ in 0..max_len {
            let mut next = Vec::new();
            for word in &last {
                for &c in alphabet {
                    let mut longer = word.clone();
                    longer.push(c);
                    next.push(longer);
                }
            }
            all.extend(next.iter().cloned());
            last = next;
        }
        all
    }
    for pattern in words(b"ab%_", 4) {
        for text in words(b"abA", 4) {
            let pattern_str = std::str::from_utf8(&pattern).unwrap();
            let text_str = std::str::from_utf8(&text).unwrap();
            let expected = super::pattern_compare(pattern_str, text_str, &super::LIKE_INFO, None)
                == super::CompareResult::Match;
            assert_eq!(
                super::like_ascii(&pattern, &text),
                expected,
                "pattern {pattern_str:?}, text {text_str:?}"
            );
            assert_eq!(
                Value::exec_like(pattern_str, text_str, None).unwrap(),
                expected,
                "exec_like: pattern {pattern_str:?}, text {text_str:?}"
            );
        }
    }
}

#[test]
fn test_like_without_escape() {
    assert!(Value::exec_like("a%", "aaaa", None).unwrap());
    assert!(Value::exec_like("%a%a", "aaaa", None).unwrap());
    assert!(!Value::exec_like("%a.a", "aaaa", None).unwrap());
    assert!(!Value::exec_like("a.a%", "aaaa", None).unwrap());
    assert!(!Value::exec_like("%a.ab", "aaaa", None).unwrap());
}

#[test]
fn test_exec_like_with_escape() {
    assert!(Value::exec_like("abcX%", "abc%", Some('X')).unwrap());
    assert!(!Value::exec_like("abcX%", "abc5", Some('X')).unwrap());
    assert!(!Value::exec_like("abcX%", "abc", Some('X')).unwrap());
    assert!(!Value::exec_like("abcX%", "abcX%", Some('X')).unwrap());
    assert!(!Value::exec_like("abcX%", "abc%%", Some('X')).unwrap());

    assert!(Value::exec_like("abcX_", "abc_", Some('X')).unwrap());
    assert!(!Value::exec_like("abcX_", "abc5", Some('X')).unwrap());
    assert!(!Value::exec_like("abcX_", "abc", Some('X')).unwrap());
    assert!(!Value::exec_like("abcX_", "abcX_", Some('X')).unwrap());
    assert!(!Value::exec_like("abcX_", "abc__", Some('X')).unwrap());

    assert!(Value::exec_like("abcXX", "abcX", Some('X')).unwrap());
    assert!(!Value::exec_like("abcXX", "abc5", Some('X')).unwrap());
    assert!(!Value::exec_like("abcXX", "abc", Some('X')).unwrap());
    assert!(!Value::exec_like("abcXX", "abcXX", Some('X')).unwrap());
}

#[test]
fn test_glob() {
    assert!(Value::exec_glob(r#"?*/abc/?*"#, r#"x//a/ab/abc/y"#).unwrap());
    assert!(Value::exec_glob(r#"a[1^]"#, r#"a1"#).unwrap());
    assert!(Value::exec_glob(r#"a[1^]*"#, r#"a^"#).unwrap());
    assert!(!Value::exec_glob(r#"a[a*"#, r#"a["#).unwrap());
    assert!(!Value::exec_glob(r#"a[a"#, r#"a[a"#).unwrap());
    assert!(Value::exec_glob(r#"a[[]"#, r#"a["#).unwrap());
    assert!(Value::exec_glob(r#"abc[^][*?]efg"#, r#"abcdefg"#).unwrap());
    assert!(!Value::exec_glob(r#"abc[^][*?]efg"#, r#"abc]efg"#).unwrap());
}

#[test]
fn test_random() {
    match Value::exec_random(|| rand::rng().random()) {
        Value::Numeric(Numeric::Integer(value)) => {
            // Check that the value is within the range of i64
            assert!(
                (i64::MIN..=i64::MAX).contains(&value),
                "Random number out of range"
            );
        }
        _ => panic!("exec_random did not return an Integer variant"),
    }
}

#[test]
fn test_exec_randomblob() {
    struct TestCase {
        input: Value,
        expected_len: usize,
    }

    let test_cases = vec![
        TestCase {
            input: Value::from_i64(5),
            expected_len: 5,
        },
        TestCase {
            input: Value::from_i64(0),
            expected_len: 1,
        },
        TestCase {
            input: Value::from_i64(-1),
            expected_len: 1,
        },
        TestCase {
            input: Value::build_text(""),
            expected_len: 1,
        },
        TestCase {
            input: Value::build_text("5"),
            expected_len: 5,
        },
        TestCase {
            input: Value::build_text("0"),
            expected_len: 1,
        },
        TestCase {
            input: Value::build_text("-1"),
            expected_len: 1,
        },
        TestCase {
            input: Value::from_f64(2.9),
            expected_len: 2,
        },
        TestCase {
            input: Value::from_f64(-3.15),
            expected_len: 1,
        },
        TestCase {
            input: Value::Null,
            expected_len: 1,
        },
    ];

    for test_case in &test_cases {
        let result = test_case
            .input
            .exec_randomblob(|dest| {
                rand::rng().fill_bytes(dest);
            })
            .unwrap();
        match result {
            Value::Blob(blob) => {
                assert_eq!(blob.len(), test_case.expected_len);
            }
            _ => panic!("exec_randomblob did not return a Blob variant"),
        }
    }

    // Test TooBig error
    let input = Value::from_i64(Value::MAX_BLOB_LENGTH + 1);
    assert!(input.exec_randomblob(|_| {}).is_err());
}

#[test]
fn test_exec_round() {
    let input_val = Value::from_f64(123.456);
    let expected_val = Value::from_f64(123.0);
    assert_eq!(input_val.exec_round(None), expected_val);

    let input_val = Value::from_f64(123.456);
    let precision_val = Value::from_i64(2);
    let expected_val = Value::from_f64(123.46);
    assert_eq!(input_val.exec_round(Some(&precision_val)), expected_val);

    let input_val = Value::from_f64(123.456);
    let precision_val = Value::build_text("1");
    let expected_val = Value::from_f64(123.5);
    assert_eq!(input_val.exec_round(Some(&precision_val)), expected_val);

    let input_val = Value::build_text("123.456");
    let precision_val = Value::from_i64(2);
    let expected_val = Value::from_f64(123.46);
    assert_eq!(input_val.exec_round(Some(&precision_val)), expected_val);

    let input_val = Value::from_i64(123);
    let precision_val = Value::from_i64(1);
    let expected_val = Value::from_f64(123.0);
    assert_eq!(input_val.exec_round(Some(&precision_val)), expected_val);

    let input_val = Value::from_f64(100.123);
    let expected_val = Value::from_f64(100.0);
    assert_eq!(input_val.exec_round(None), expected_val);

    let input_val = Value::from_f64(100.123);
    let expected_val = Value::Null;
    assert_eq!(input_val.exec_round(Some(&Value::Null)), expected_val);
}

#[test]
fn test_exec_if() {
    let reg = Value::from_i64(0);
    assert!(!reg.exec_if(false, false));
    assert!(reg.exec_if(false, true));

    let reg = Value::from_i64(1);
    assert!(reg.exec_if(false, false));
    assert!(!reg.exec_if(false, true));

    let reg = Value::Null;
    assert!(!reg.exec_if(false, false));
    assert!(!reg.exec_if(false, true));

    let reg = Value::Null;
    assert!(reg.exec_if(true, false));
    assert!(reg.exec_if(true, true));

    let reg = Value::Null;
    assert!(!reg.exec_if(false, false));
    assert!(!reg.exec_if(false, true));
}

#[test]
fn test_nullif() {
    assert_eq!(
        Value::from_i64(1).exec_nullif(&Value::from_i64(1)),
        Value::Null
    );
    assert_eq!(
        Value::from_f64(1.1).exec_nullif(&Value::from_f64(1.1)),
        Value::Null
    );
    assert_eq!(
        Value::build_text("limbo").exec_nullif(&Value::build_text("limbo")),
        Value::Null
    );

    assert_eq!(
        Value::from_i64(1).exec_nullif(&Value::from_i64(2)),
        Value::from_i64(1)
    );
    assert_eq!(
        Value::from_f64(1.1).exec_nullif(&Value::from_f64(1.2)),
        Value::from_f64(1.1)
    );
    assert_eq!(
        Value::build_text("limbo").exec_nullif(&Value::build_text("limb")),
        Value::build_text("limbo")
    );
}

#[test]
fn test_substring() {
    let str_value = Value::build_text("limbo");
    let start_value = Value::from_i64(1);
    let length_value = Value::from_i64(3);
    let expected_val = Value::build_text("lim");
    assert_eq!(
        allocated(Value::exec_substring(
            &str_value,
            &start_value,
            Some(&length_value),
        )),
        expected_val
    );

    let str_value = Value::build_text("limbo");
    let start_value = Value::from_i64(1);
    let length_value = Value::from_i64(10);
    let expected_val = Value::build_text("limbo");
    assert_eq!(
        allocated(Value::exec_substring(
            &str_value,
            &start_value,
            Some(&length_value),
        )),
        expected_val
    );

    let str_value = Value::build_text("limbo");
    let start_value = Value::from_i64(10);
    let length_value = Value::from_i64(3);
    let expected_val = Value::build_text("");
    assert_eq!(
        allocated(Value::exec_substring(
            &str_value,
            &start_value,
            Some(&length_value),
        )),
        expected_val
    );

    let str_value = Value::build_text("limbo");
    let start_value = Value::from_i64(3);
    let length_value = Value::Null;
    let expected_val = Value::Null;
    assert_eq!(
        allocated(Value::exec_substring(
            &str_value,
            &start_value,
            Some(&length_value),
        )),
        expected_val
    );

    let str_value = Value::build_text("limbo");
    let start_value = Value::from_i64(10);
    let length_value = Value::Null;
    let expected_val = Value::Null;
    assert_eq!(
        allocated(Value::exec_substring(
            &str_value,
            &start_value,
            Some(&length_value),
        )),
        expected_val
    );

    let str_value = Value::build_text("limbo");
    let start_value = Value::from_i64(-7_096_519_388_852_014_892);
    let length_value = Value::from_i64(-4_829_175_794_346_763_833);
    let expected_val = Value::build_text("");
    assert_eq!(
        allocated(Value::exec_substring(
            &str_value,
            &start_value,
            Some(&length_value),
        )),
        expected_val
    );
}

#[test]
fn test_exec_instr() {
    let input = Value::build_text("limbo");
    let pattern = Value::build_text("im");
    let expected = Value::from_i64(2);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::build_text("limbo");
    let pattern = Value::build_text("limbo");
    let expected = Value::from_i64(1);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::build_text("limbo");
    let pattern = Value::build_text("o");
    let expected = Value::from_i64(5);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::build_text("liiiiimbo");
    let pattern = Value::build_text("ii");
    let expected = Value::from_i64(2);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::build_text("limbo");
    let pattern = Value::build_text("limboX");
    let expected = Value::from_i64(0);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::build_text("limbo");
    let pattern = Value::build_text("");
    let expected = Value::from_i64(1);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::build_text("");
    let pattern = Value::build_text("limbo");
    let expected = Value::from_i64(0);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::build_text("");
    let pattern = Value::build_text("");
    let expected = Value::from_i64(1);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::Null;
    let pattern = Value::Null;
    let expected = Value::Null;
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::build_text("limbo");
    let pattern = Value::Null;
    let expected = Value::Null;
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::Null;
    let pattern = Value::build_text("limbo");
    let expected = Value::Null;
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::from_i64(123);
    let pattern = Value::from_i64(2);
    let expected = Value::from_i64(2);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::from_i64(123);
    let pattern = Value::from_i64(5);
    let expected = Value::from_i64(0);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::from_f64(12.34);
    let pattern = Value::from_f64(2.3);
    let expected = Value::from_i64(2);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::from_f64(12.34);
    let pattern = Value::from_f64(5.6);
    let expected = Value::from_i64(0);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::from_f64(12.34);
    let pattern = Value::build_text(".");
    let expected = Value::from_i64(3);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = blob(&[1, 2, 3, 4, 5]);
    let pattern = blob(&[3, 4]);
    let expected = Value::from_i64(3);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = blob(&[1, 2, 3, 4, 5]);
    let pattern = blob(&[3, 2]);
    let expected = Value::from_i64(0);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = blob(&[0x61, 0x62, 0x63, 0x64, 0x65]);
    let pattern = Value::build_text("cd");
    let expected = Value::from_i64(3);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::build_text("abcde");
    let pattern = blob(&[0x63, 0x64]);
    let expected = Value::from_i64(3);
    assert_eq!(input.exec_instr(&pattern), expected);

    let input = Value::build_text("abcde");
    let pattern = Value::build_text("");
    let expected = Value::from_i64(1);
    assert_eq!(input.exec_instr(&pattern), expected);
}

#[test]
fn test_exec_sign() {
    let input = Value::from_i64(42);
    let expected = Some(Value::from_i64(1));
    assert_eq!(input.exec_sign(), expected);

    let input = Value::from_i64(-42);
    let expected = Some(Value::from_i64(-1));
    assert_eq!(input.exec_sign(), expected);

    let input = Value::from_i64(0);
    let expected = Some(Value::from_i64(0));
    assert_eq!(input.exec_sign(), expected);

    let input = Value::from_f64(0.0);
    let expected = Some(Value::from_i64(0));
    assert_eq!(input.exec_sign(), expected);

    let input = Value::from_f64(0.1);
    let expected = Some(Value::from_i64(1));
    assert_eq!(input.exec_sign(), expected);

    let input = Value::from_f64(42.0);
    let expected = Some(Value::from_i64(1));
    assert_eq!(input.exec_sign(), expected);

    let input = Value::from_f64(-42.0);
    let expected = Some(Value::from_i64(-1));
    assert_eq!(input.exec_sign(), expected);

    let input = Value::build_text("abc");
    let expected = None;
    assert_eq!(input.exec_sign(), expected);

    let input = Value::build_text("42");
    let expected = Some(Value::from_i64(1));
    assert_eq!(input.exec_sign(), expected);

    let input = Value::build_text("-42");
    let expected = Some(Value::from_i64(-1));
    assert_eq!(input.exec_sign(), expected);

    let input = Value::build_text("0");
    let expected = Some(Value::from_i64(0));
    assert_eq!(input.exec_sign(), expected);

    let input = blob(b"abc");
    let expected = None;
    assert_eq!(input.exec_sign(), expected);

    let input = blob(b"42");
    let expected = None;
    assert_eq!(input.exec_sign(), expected);

    let input = blob(b"-42");
    let expected = None;
    assert_eq!(input.exec_sign(), expected);

    let input = blob(b"0");
    let expected = None;
    assert_eq!(input.exec_sign(), expected);

    let input = Value::Null;
    let expected = None;
    assert_eq!(input.exec_sign(), expected);
}

#[test]
fn test_exec_zeroblob() {
    let input = Value::from_i64(0);
    let expected = blob(&[]);
    assert_eq!(input.exec_zeroblob().unwrap(), expected);

    let input = Value::Null;
    let expected = blob(&[]);
    assert_eq!(input.exec_zeroblob().unwrap(), expected);

    let input = Value::from_i64(4);
    let expected = Value::Blob(crate::alloc::vec![0; 4]);
    assert_eq!(input.exec_zeroblob().unwrap(), expected);

    let input = Value::from_i64(-1);
    let expected = blob(&[]);
    assert_eq!(input.exec_zeroblob().unwrap(), expected);

    let input = Value::build_text("5");
    let expected = Value::Blob(crate::alloc::vec![0; 5]);
    assert_eq!(input.exec_zeroblob().unwrap(), expected);

    let input = Value::build_text("-5");
    let expected = blob(&[]);
    assert_eq!(input.exec_zeroblob().unwrap(), expected);

    let input = Value::build_text("text");
    let expected = blob(&[]);
    assert_eq!(input.exec_zeroblob().unwrap(), expected);

    let input = Value::from_f64(2.6);
    let expected = Value::Blob(crate::alloc::vec![0; 2]);
    assert_eq!(input.exec_zeroblob().unwrap(), expected);

    let input = blob(&[1]);
    let expected = blob(&[]);
    assert_eq!(input.exec_zeroblob().unwrap(), expected);

    // Test TooBig error
    let input = Value::from_i64(Value::MAX_BLOB_LENGTH + 1);
    assert!(input.exec_zeroblob().is_err());
}

#[test]
fn test_replace() {
    let input_str = Value::build_text("bob");
    let pattern_str = Value::build_text("b");
    let replace_str = Value::build_text("a");
    let expected_str = Value::build_text("aoa");
    assert_eq!(
        allocated(Value::exec_replace(&input_str, &pattern_str, &replace_str,)),
        expected_str
    );

    let input_str = Value::build_text("bob");
    let pattern_str = Value::build_text("b");
    let replace_str = Value::build_text("");
    let expected_str = Value::build_text("o");
    assert_eq!(
        allocated(Value::exec_replace(&input_str, &pattern_str, &replace_str,)),
        expected_str
    );

    let input_str = Value::build_text("bob");
    let pattern_str = Value::build_text("b");
    let replace_str = Value::build_text("abc");
    let expected_str = Value::build_text("abcoabc");
    assert_eq!(
        allocated(Value::exec_replace(&input_str, &pattern_str, &replace_str,)),
        expected_str
    );

    let input_str = Value::build_text("bob");
    let pattern_str = Value::build_text("a");
    let replace_str = Value::build_text("b");
    let expected_str = Value::build_text("bob");
    assert_eq!(
        allocated(Value::exec_replace(&input_str, &pattern_str, &replace_str,)),
        expected_str
    );

    let input_str = Value::build_text("bob");
    let pattern_str = Value::build_text("");
    let replace_str = Value::build_text("a");
    let expected_str = Value::build_text("bob");
    assert_eq!(
        allocated(Value::exec_replace(&input_str, &pattern_str, &replace_str,)),
        expected_str
    );

    let input_str = Value::build_text("bob");
    let pattern_str = Value::Null;
    let replace_str = Value::build_text("a");
    let expected_str = Value::Null;
    assert_eq!(
        allocated(Value::exec_replace(&input_str, &pattern_str, &replace_str,)),
        expected_str
    );

    let input_str = Value::build_text("bo5");
    let pattern_str = Value::from_i64(5);
    let replace_str = Value::build_text("a");
    let expected_str = Value::build_text("boa");
    assert_eq!(
        allocated(Value::exec_replace(&input_str, &pattern_str, &replace_str,)),
        expected_str
    );

    let input_str = Value::build_text("bo5.0");
    let pattern_str = Value::from_f64(5.0);
    let replace_str = Value::build_text("a");
    let expected_str = Value::build_text("boa");
    assert_eq!(
        allocated(Value::exec_replace(&input_str, &pattern_str, &replace_str,)),
        expected_str
    );

    let input_str = Value::build_text("bo5");
    let pattern_str = Value::from_f64(5.0);
    let replace_str = Value::build_text("a");
    let expected_str = Value::build_text("bo5");
    assert_eq!(
        allocated(Value::exec_replace(&input_str, &pattern_str, &replace_str,)),
        expected_str
    );

    let input_str = Value::build_text("bo5.0");
    let pattern_str = Value::from_f64(5.0);
    let replace_str = Value::from_f64(6.0);
    let expected_str = Value::build_text("bo6.0");
    assert_eq!(
        allocated(Value::exec_replace(&input_str, &pattern_str, &replace_str,)),
        expected_str
    );

    // todo: change this test to use (0.1 + 0.2) instead of 0.3 when decimals are implemented.
    let input_str = Value::build_text("tes3");
    let pattern_str = Value::from_i64(3);
    let replace_str = Value::from_f64(0.3);
    let expected_str = Value::build_text("tes0.3");
    assert_eq!(
        allocated(Value::exec_replace(&input_str, &pattern_str, &replace_str,)),
        expected_str
    );
}
