use super::*;

fn text(value: &str) -> Register {
    Register::Value(Value::build_text(value.to_string()))
}

fn integer(value: i64) -> Register {
    Register::Value(Value::from_i64(value))
}

fn float(value: f64) -> Register {
    Register::Value(Value::from_f64(value))
}

#[test]
fn test_printf_no_args() {
    assert_eq!(exec_printf(&[]).unwrap(), Value::Null);
}

#[test]
fn test_printf_basic_string() {
    assert_eq!(
        exec_printf(&[text("Hello World")]).unwrap(),
        *text("Hello World").get_value()
    );
}

#[test]
fn test_printf_string_formatting() {
    let test_cases = vec![
        (
            vec![text("Hello, %s!"), text("World")],
            text("Hello, World!"),
        ),
        (
            vec![text("%s %s!"), text("Hello"), text("World")],
            text("Hello World!"),
        ),
        (
            vec![text("Hello, %s!"), Register::Value(Value::Null)],
            text("Hello, !"),
        ),
        (vec![text("Value: %s"), integer(42)], text("Value: 42")),
        (vec![text("100%% complete")], text("100% complete")),
    ];
    for (input, output) in test_cases {
        assert_eq!(exec_printf(&input).unwrap(), *output.get_value());
    }
}

#[test]
fn test_printf_integer_formatting() {
    let test_cases = vec![
        (vec![text("Number: %d"), integer(42)], text("Number: 42")),
        (vec![text("Number: %d"), integer(-42)], text("Number: -42")),
        (
            vec![text("%d + %d = %d"), integer(2), integer(3), integer(5)],
            text("2 + 3 = 5"),
        ),
        (
            vec![text("Number: %d"), text("not a number")],
            text("Number: 0"),
        ),
        (
            vec![text("Truncated float: %d"), float(3.9)],
            text("Truncated float: 3"),
        ),
        (vec![text("Number: %i"), integer(42)], text("Number: 42")),
    ];
    for (input, output) in test_cases {
        assert_eq!(exec_printf(&input).unwrap(), *output.get_value());
    }
}

#[test]
fn test_printf_unsigned_integer_formatting() {
    let test_cases = vec![
        (vec![text("Number: %u"), integer(42)], text("Number: 42")),
        (
            vec![text("Negative: %u"), integer(-1)],
            text("Negative: 18446744073709551615"),
        ),
        (vec![text("NaN: %u"), text("not a number")], text("NaN: 0")),
    ];
    for (input, output) in test_cases {
        assert_eq!(exec_printf(&input).unwrap(), *output.get_value());
    }
}

#[test]
fn test_printf_float_formatting() {
    let test_cases = vec![
        (
            vec![text("Number: %f"), float(42.5)],
            text("Number: 42.500000"),
        ),
        (
            vec![text("Number: %f"), float(-42.5)],
            text("Number: -42.500000"),
        ),
        (
            vec![text("Number: %f"), integer(42)],
            text("Number: 42.000000"),
        ),
        (
            vec![text("Number: %f"), text("not a number")],
            text("Number: 0.000000"),
        ),
    ];

    // Huge finite float must not overflow rounding to produce "inf"
    let huge = exec_printf(&[text("%f"), float(1e308)]).unwrap();
    let huge_str = match &huge {
        Value::Text(t) => t.as_str().to_string(),
        _ => panic!("expected text"),
    };
    assert!(huge_str.starts_with("9999999999999999"));
    assert!(huge_str.ends_with(".000000"));
    assert!(!huge_str.contains("inf"));
    for (input, expected) in test_cases {
        assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
    }
}

#[test]
fn test_printf_width_precision() {
    let test_cases = vec![
        (vec![text("%.2f"), float(4.002)], text("4.00")),
        (vec![text("%05d"), integer(42)], text("00042")),
        (vec![text("%.5d"), integer(42)], text("00042")),
        (vec![text("%+d"), integer(42)], text("+42")),
        (vec![text("%.3s"), text("hello")], text("hel")),
        (vec![text("%08x"), integer(255)], text("000000ff")),
        (vec![text("%#x"), integer(255)], text("0xff")),
    ];
    for (input, expected) in test_cases {
        assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
    }
}

#[test]
fn test_printf_dynamic_width() {
    assert_eq!(
        exec_printf(&[text("%.*f"), integer(2), float(3.14258)]).unwrap(),
        *text("3.14").get_value()
    );
}

#[test]
fn test_printf_character_formatting() {
    let test_cases = vec![
        (vec![text("character: %c"), text("a")], text("character: a")),
        (
            vec![text("character: %c"), text("this is a test")],
            text("character: t"),
        ),
        (
            vec![text("character: %c"), integer(123)],
            text("character: 1"),
        ),
        (
            vec![text("character: %c"), float(42.5)],
            text("character: 4"),
        ),
        // Empty string → NUL char → no output (matches SQLite)
        (vec![text("character: %c"), text("")], text("character: ")),
        // NULL → coerces to empty string → NUL → no output
        (
            vec![text("character: %c"), Register::Value(Value::Null)],
            text("character: "),
        ),
    ];
    for (input, expected) in test_cases {
        assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
    }
}

#[test]
fn test_printf_exponential_formatting() {
    let test_cases = vec![
        (
            vec![text("Exp: %e"), float(23000000.0)],
            text("Exp: 2.300000e+07"),
        ),
        (
            vec![text("Exp: %e"), float(-23000000.0)],
            text("Exp: -2.300000e+07"),
        ),
        (vec![text("Exp: %e"), float(0.0)], text("Exp: 0.000000e+00")),
    ];
    for (input, expected) in test_cases {
        assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
    }
}

#[test]
fn test_printf_general_formatting() {
    let test_cases = vec![
        (vec![text("%g"), float(100.0)], text("100")),
        (vec![text("%g"), float(0.00123)], text("0.00123")),
        (vec![text("%g"), float(1.0)], text("1")),
        (vec![text("%g"), float(1.5)], text("1.5")),
        (vec![text("%g"), float(0.0)], text("0")),
        (vec![text("%g"), integer(42)], text("42")),
        // Comma separator applies to %G decimal notation
        (vec![text("%,G"), integer(1000)], text("1,000")),
        (
            vec![text("%,.20G"), float(1234567.89)],
            text("1,234,567.89"),
        ),
    ];
    for (input, expected) in test_cases {
        assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
    }
}

#[test]
fn test_printf_sql_quoting() {
    assert_eq!(
        exec_printf(&[text("%q"), text("it's")]).unwrap(),
        *text("it''s").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%Q"), text("it's")]).unwrap(),
        *text("'it''s'").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%Q"), Register::Value(Value::Null)]).unwrap(),
        *text("NULL").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%q"), Register::Value(Value::Null)]).unwrap(),
        *text("(NULL)").get_value()
    );
}

#[test]
fn test_printf_comma_separator() {
    assert_eq!(
        exec_printf(&[text("%,d"), integer(1234567)]).unwrap(),
        *text("1,234,567").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%,d"), integer(-1234567)]).unwrap(),
        *text("-1,234,567").get_value()
    );
}

#[test]
fn test_printf_edge_cases() {
    let test_cases = vec![
        (vec![text("%%%%")], text("%%")),
        (vec![text("No substitutions")], text("No substitutions")),
        (
            vec![text("%d%d%d"), integer(1), integer(2), integer(3)],
            text("123"),
        ),
        // Trailing % is preserved
        (vec![text("test%")], text("test%")),
        // Unknown specifier: NULL if nothing processed before, else accumulated text
        (vec![text("%d%j"), integer(42)], text("42")),
        (vec![text("hello%j")], text("hello")),
        // Negative zero should not show minus sign
        (vec![text("%f"), float(-0.0)], text("0.000000")),
    ];
    for (input, expected) in test_cases {
        assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
    }
    assert_eq!(exec_printf(&[text("")]).unwrap(), Value::Null);
    assert_eq!(
        exec_printf(&[text("%s"), text("")]).unwrap(),
        *text("").get_value()
    );
}

#[test]
fn test_printf_hexadecimal_formatting() {
    let test_cases = vec![
        (vec![text("hex: %x"), integer(4)], text("hex: 4")),
        (
            vec![text("hex: %X"), integer(15565303546)],
            text("hex: 39FC3AEFA"),
        ),
        (
            vec![text("hex: %x"), integer(-15565303546)],
            text("hex: fffffffc603c5106"),
        ),
        (vec![text("hex: %x"), float(42.5)], text("hex: 2a")),
        (vec![text("hex: %x"), text("42")], text("hex: 2a")),
        (vec![text("hex: %x"), text("")], text("hex: 0")),
    ];
    for (input, expected) in test_cases {
        assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
    }
}

#[test]
fn test_printf_octal_formatting() {
    let test_cases = vec![
        (vec![text("octal: %o"), integer(4)], text("octal: 4")),
        (vec![text("octal: %o"), float(42.5)], text("octal: 52")),
        (vec![text("octal: %o"), text("42")], text("octal: 52")),
        // # flag always adds "0" prefix when value is non-zero
        (vec![text("%#o"), integer(8)], text("010")),
        (vec![text("%#o"), integer(0)], text("0")),
        // # flag with precision: "0" prefix added even if precision pads with zeros
        (vec![text("%#.5o"), integer(8)], text("000010")),
        (
            vec![text("%#.20o"), integer(1000)],
            text("000000000000000001750"),
        ),
    ];
    for (input, expected) in test_cases {
        assert_eq!(exec_printf(&input).unwrap(), *expected.get_value());
    }
}

// ── Bug fix regression tests ────────────────────────────────────

#[test]
fn test_rounding_half_away_from_zero() {
    // Bug 1: SQLite uses half-away-from-zero, not half-to-even
    assert_eq!(
        exec_printf(&[text("%.0f"), float(0.5)]).unwrap(),
        *text("1").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%.0f"), float(2.5)]).unwrap(),
        *text("3").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%.0f"), float(-0.5)]).unwrap(),
        *text("-1").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%.0e"), float(2.5)]).unwrap(),
        *text("3e+00").get_value()
    );
}

#[test]
fn test_alt_hex_zero_pad_width() {
    // Bug 2: # flag with 0 flag - prefix not counted in width
    assert_eq!(
        exec_printf(&[text("%#08x"), integer(255)]).unwrap(),
        *text("0x000000ff").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%#04x"), integer(255)]).unwrap(),
        *text("0x00ff").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%#08o"), integer(8)]).unwrap(),
        *text("000000010").get_value()
    );
}

#[test]
fn test_alt_flag_forces_decimal_point() {
    // Bug 3: # flag forces decimal point on %e and %g
    assert_eq!(
        exec_printf(&[text("%#.0e"), float(1.0)]).unwrap(),
        *text("1.e+00").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%#.0g"), float(1.0)]).unwrap(),
        *text("1.").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%#g"), float(100000.0)]).unwrap(),
        *text("100000.").get_value()
    );
}

#[test]
fn test_g_threshold_rounding() {
    // Bug 4: %g pre-rounding changes the exponent threshold
    assert_eq!(
        exec_printf(&[text("%g"), float(999999.5)]).unwrap(),
        *text("1e+06").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%.1g"), float(9.5)]).unwrap(),
        *text("1e+01").get_value()
    );
}

#[test]
fn test_zero_pad_ignored_for_strings() {
    // Bug 5: 0 flag should be ignored for %s and %c
    assert_eq!(
        exec_printf(&[text("%05s"), text("hi")]).unwrap(),
        *text("   hi").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%05c"), text("A")]).unwrap(),
        *text("    A").get_value()
    );
}

#[test]
fn test_q_width_precision() {
    // Bug 6: %q/%Q/%w should respect width and precision
    assert_eq!(
        exec_printf(&[text("%.2q"), text("hello")]).unwrap(),
        *text("he").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%10q"), text("hi")]).unwrap(),
        *text("        hi").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%10Q"), text("hi")]).unwrap(),
        *text("      'hi'").get_value()
    );
}

#[test]
fn test_infinity_handling() {
    // SQLite source (sqlite3.c:32502): infinity + flag_zeropad → 9-fill
    // infinity without flag_zeropad → "Inf"
    let inf_f = exec_printf(&[text("%020f"), float(f64::INFINITY)]).unwrap();
    let inf_str = match &inf_f {
        Value::Text(t) => t.as_str().to_string(),
        _ => panic!("expected text"),
    };
    assert!(inf_str.starts_with("9000"));
    assert_eq!(inf_str.len(), 1007); // 9 + 999 zeros + ".000000"

    assert_eq!(
        exec_printf(&[text("%020e"), float(f64::INFINITY)]).unwrap(),
        *text("00000009.000000e+999").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%020g"), float(f64::INFINITY)]).unwrap(),
        *text("000000000000009e+999").get_value()
    );
    // Without zero-pad → "Inf" (not 9-fill)
    assert_eq!(
        exec_printf(&[text("%e"), float(f64::INFINITY)]).unwrap(),
        *text("Inf").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%G"), float(f64::INFINITY)]).unwrap(),
        *text("Inf").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%f"), float(f64::INFINITY)]).unwrap(),
        *text("Inf").get_value()
    );
    // With zero-pad but no width still triggers 9-fill
    assert_eq!(
        exec_printf(&[text("%0G"), float(f64::INFINITY)]).unwrap(),
        *text("9E+999").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%0,G"), float(f64::INFINITY)]).unwrap(),
        *text("9E+999").get_value()
    );
    // Negative infinity
    assert_eq!(
        exec_printf(&[text("%e"), float(f64::NEG_INFINITY)]).unwrap(),
        *text("-Inf").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%020e"), float(f64::NEG_INFINITY)]).unwrap(),
        *text("-0000009.000000e+999").get_value()
    );
    // # flag with %g infinity: RTZ disabled, so trailing zeros remain
    assert_eq!(
        exec_printf(&[text("%#0g"), float(f64::INFINITY)]).unwrap(),
        *text("9.00000e+999").get_value()
    );
    // ! flag with %e infinity: RTZ enabled, strips to .0
    assert_eq!(
        exec_printf(&[text("%!0e"), float(f64::INFINITY)]).unwrap(),
        *text("9.0e+999").get_value()
    );
    // ! flag with %f infinity: strips trailing fractional zeros
    let inf_bang_f = exec_printf(&[text("%!0f"), float(f64::INFINITY)]).unwrap();
    let inf_bang_str = match &inf_bang_f {
        Value::Text(t) => t.as_str().to_string(),
        _ => panic!("expected text"),
    };
    assert!(
        inf_bang_str.ends_with(".0"),
        "Infinity with %!0f should end with .0, got: ...{}",
        &inf_bang_str[inf_bang_str.len().saturating_sub(10)..]
    );
}

#[test]
fn test_significant_digits_limiting() {
    // Default: 16 significant digits (hide IEEE noise)
    assert_eq!(
        exec_printf(&[text("%.20f"), float(1.0 / 3.0)]).unwrap(),
        *text("0.33333333333333330000").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%.20e"), float(1.0 / 3.0)]).unwrap(),
        *text("3.33333333333333300000e-01").get_value()
    );
    // ! flag: 26 significant digits max, trailing zeros stripped (sqlite3.c:32496).
    // fp_decode extracts 19 digits from the u64; the ! flag's RTZ then strips
    // the trailing '0', yielding 19 fractional characters.
    assert_eq!(
        exec_printf(&[text("%!.20f"), float(1.0 / 3.0)]).unwrap(),
        *text("0.3333333333333333148").get_value()
    );
}

#[test]
fn test_nan_handling() {
    // Value::from_f64(NaN) returns Value::Null (NonNan rejects NaN),
    // so NaN is treated as NULL which coerces to 0.0 for float formats.
    // The NaN-specific formatting code (NaN/null output) is defense-in-depth
    // that can't be triggered through the Value system.
    assert_eq!(
        exec_printf(&[text("%f"), float(f64::NAN)]).unwrap(),
        *text("0.000000").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%e"), float(f64::NAN)]).unwrap(),
        *text("0.000000e+00").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%g"), float(f64::NAN)]).unwrap(),
        *text("0").get_value()
    );
}

#[test]
fn test_blob_nul_truncation() {
    // Bug 9: %s on blobs truncates at first NUL byte
    let blob_val =
        Register::Value(Value::from_slice(&[0x48, 0x00, 0x4C]).expect(crate::alloc::ALLOC_ERR_MSG)); // H\0L
    let result = exec_printf(&[text("%s"), blob_val]).unwrap();
    assert_eq!(result, *text("H").get_value());

    let blob_hello =
        Register::Value(Value::from_slice(b"Hello").expect(crate::alloc::ALLOC_ERR_MSG));
    assert_eq!(
        exec_printf(&[text("%s"), blob_hello]).unwrap(),
        *text("Hello").get_value()
    );
}

#[test]
fn test_limit_significant_digits_rounding() {
    // Verify the rounding behavior of limit_significant_digits
    assert_eq!(limit_significant_digits("123456789", 5), "123460000");
    assert_eq!(limit_significant_digits("1.23456789", 5), "1.23460000");
    assert_eq!(limit_significant_digits("0.001234", 3), "0.001230");
    assert_eq!(limit_significant_digits("9.9999", 3), "10.0000");
    assert_eq!(limit_significant_digits("0.099999", 4), "0.100000");
}

#[test]
fn test_i32_star_precision_wrapping() {
    // i32::MIN as * precision wraps back to itself after negation → treated as 0
    assert_eq!(
        exec_printf(&[text("%.*d"), integer(-2147483648), integer(42)]).unwrap(),
        *text("42").get_value()
    );
    // 4294967295 as i64 → -1 as i32 → wrapping_neg → 1
    assert_eq!(
        exec_printf(&[text("%.*d"), integer(4294967295), integer(42)]).unwrap(),
        *text("42").get_value()
    );
}

#[test]
fn test_comma_zero_pad_interaction() {
    // When comma + zero_pad: zero-pad digits to width, then insert commas
    // Width 15 = 15 digit positions, commas added on top
    assert_eq!(
        exec_printf(&[text("%0,15d"), integer(42)]).unwrap(),
        *text("000,000,000,000,042").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%0,15u"), integer(42)]).unwrap(),
        *text("000,000,000,000,042").get_value()
    );
    // Left-justify is ignored when comma + zero-pad are both set
    assert_eq!(
        exec_printf(&[text("%-0,15d"), integer(42)]).unwrap(),
        *text("000,000,000,000,042").get_value()
    );
}

#[test]
fn test_ordinal_format() {
    assert_eq!(
        exec_printf(&[text("%r"), integer(1)]).unwrap(),
        *text("1st").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%r"), integer(2)]).unwrap(),
        *text("2nd").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%r"), integer(3)]).unwrap(),
        *text("3rd").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%r"), integer(11)]).unwrap(),
        *text("11th").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%r"), integer(112)]).unwrap(),
        *text("112th").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%.5r"), integer(-39)]).unwrap(),
        *text("-039th").get_value()
    );
    assert_eq!(
        exec_printf(&[text("% r"), integer(42)]).unwrap(),
        *text(" 42nd").get_value()
    );
    // Zero-pad pads the digits before the suffix
    assert_eq!(
        exec_printf(&[text("%010r"), integer(0)]).unwrap(),
        *text("00000000th").get_value()
    );
}

#[test]
fn test_q_null_precision_truncation() {
    // Precision truncates the NULL literal representation
    assert_eq!(
        exec_printf(&[text("%.0q"), Register::Value(Value::Null)]).unwrap(),
        *text("").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%.3q"), Register::Value(Value::Null)]).unwrap(),
        *text("(NU").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%.0Q"), Register::Value(Value::Null)]).unwrap(),
        *text("").get_value()
    );
}

#[test]
fn test_q_null_width_padding() {
    // Width applies to the NULL representation
    assert_eq!(
        exec_printf(&[text("%-10q"), Register::Value(Value::Null)]).unwrap(),
        *text("(NULL)    ").get_value()
    );
}

#[test]
fn test_unknown_specifier_returns_early() {
    // Unknown specifier as first thing → NULL (SQLite's StrAccum never allocated)
    assert_eq!(
        exec_printf(&[text("%b"), integer(42)]).unwrap(),
        Value::Null,
    );
    // Unknown specifier after literal text → accumulated text
    assert_eq!(
        exec_printf(&[text("hello%b"), integer(42)]).unwrap(),
        *text("hello").get_value()
    );
    // Unknown specifier after %n → "" (StrAccum was allocated by %n processing)
    assert_eq!(
        exec_printf(&[text("%n%b"), integer(42)]).unwrap(),
        *text("").get_value(),
    );
}

#[test]
fn test_control_char_escaping_with_hash_q() {
    // %#q escapes control characters as \uXXXX and doubles backslashes
    assert_eq!(
        exec_printf(&[text("%#q"), text("a\nb")]).unwrap(),
        *text("a\\u000ab").get_value()
    );
    assert_eq!(
        exec_printf(&[text("%#q"), text("a\tb")]).unwrap(),
        *text("a\\u0009b").get_value()
    );
    // Backslash is doubled in escape mode
    assert_eq!(
        exec_printf(&[text("%#q"), text("a\\b")]).unwrap(),
        *text("a\\\\b").get_value()
    );
}

#[test]
fn test_hash_q_upper_unistr_wrapping() {
    // %#Q wraps with unistr('...') when control chars are present
    assert_eq!(
        exec_printf(&[text("%#Q"), text("a\nb")]).unwrap(),
        *text("unistr('a\\u000ab')").get_value()
    );
    // %#Q without control chars — no unistr wrapping
    assert_eq!(
        exec_printf(&[text("%#Q"), text("hello")]).unwrap(),
        *text("'hello'").get_value()
    );
    // %Q without # — no unistr wrapping even with control chars
    assert_eq!(
        exec_printf(&[text("%Q"), text("a\nb")]).unwrap(),
        *text("'a\nb'").get_value()
    );
}

#[test]
fn test_very_small_float_no_nan() {
    // 1e-300 with %G should not produce NaN — round_half_away_e must handle
    // subnormal scale values from 10^(-309+) without dividing by ~0.
    let result = exec_printf(&[text("%.*G"), integer(10), float(1e-300)]).unwrap();
    assert_eq!(result, *text("1E-300").get_value());

    // Also test with %e
    let result = exec_printf(&[text("%.10e"), float(1e-300)]).unwrap();
    assert!(
        !result.to_string().contains("NaN"),
        "1e-300 with %e should not produce NaN"
    );

    // And %g
    let result = exec_printf(&[text("%.10g"), float(1e-300)]).unwrap();
    assert!(
        !result.to_string().contains("NaN"),
        "1e-300 with %g should not produce NaN"
    );
}

#[test]
fn test_large_float_f_format() {
    // 1e308 with %f must produce leading digits "9999..." (matching SQLite's
    // sqlite3FpDecode), NOT "1000..." (which Rust's format! produces).
    let result = exec_printf(&[text("%.0f"), float(1e308)]).unwrap();
    let s = result.to_string();
    assert!(
        s.starts_with("99999999999999990"),
        "1e308 with %.0f should start with 9999..., got: {}",
        &s[..s.len().min(40)]
    );

    // With commas too
    let result = exec_printf(&[text("%,f"), float(1e308)]).unwrap();
    let s = result.to_string();
    assert!(
        s.starts_with("99,999,999,999,999,990"),
        "1e308 with %,f should start with 99,999..., got: {}",
        &s[..s.len().min(40)]
    );
}

#[test]
fn test_negative_zero_suppression() {
    // SQLite 3.51+ (sqlite3.c:32520-32532): With # flag (no + or space),
    // %f suppresses minus sign when displayed value rounds to zero.

    // -0.0000001 with %#f displays as 0.000000 — suppress minus
    let result = exec_printf(&[text("%#f"), float(-0.0000001)]).unwrap();
    assert_eq!(result.to_string(), "0.000000");

    // Same without # flag — keep minus
    let result = exec_printf(&[text("%f"), float(-0.0000001)]).unwrap();
    assert_eq!(result.to_string(), "-0.000000");

    // With + flag, # doesn't suppress (flag_prefix is set)
    let result = exec_printf(&[text("%#+f"), float(-0.0000001)]).unwrap();
    assert_eq!(result.to_string(), "-0.000000");

    // -0.5 rounds to -1, not zero — keep minus
    let result = exec_printf(&[text("%#.0f"), float(-0.5)]).unwrap();
    assert_eq!(result.to_string(), "-1.");

    // -0.4 rounds to 0 — suppress
    let result = exec_printf(&[text("%#.0f"), float(-0.4)]).unwrap();
    assert_eq!(result.to_string(), "0.");

    // -0.0000001 with comma separator
    let result = exec_printf(&[text("%#,f"), float(-0.0000001)]).unwrap();
    assert_eq!(result.to_string(), "0.000000");
}
