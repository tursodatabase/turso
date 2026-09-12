use super::*;

#[test]
fn test_apply_numeric_affinity_partial_numbers() {
    let val = Value::Text("123abc".into());
    let res = apply_numeric_affinity(val.as_value_ref(), false);
    assert!(res.is_none());

    let val = Value::Text("-53093015420544-15062897".into());
    let res = apply_numeric_affinity(val.as_value_ref(), false);
    assert!(res.is_none());

    let val = Value::Text("123.45xyz".into());
    let res = apply_numeric_affinity(val.as_value_ref(), false);
    assert!(res.is_none());
}

#[test]
fn test_apply_numeric_affinity_complete_numbers() {
    let val = Value::Text("123".into());
    let res = apply_numeric_affinity(val.as_value_ref(), false);
    assert_eq!(res, Some(ValueRef::Numeric(Numeric::Integer(123))));

    let val = Value::Text("123.45".into());
    let res = apply_numeric_affinity(val.as_value_ref(), false);
    assert_eq!(res, Some(ValueRef::from_f64(123.45)));

    let val = Value::Text("  -456  ".into());
    let res = apply_numeric_affinity(val.as_value_ref(), false);
    assert_eq!(res, Some(ValueRef::Numeric(Numeric::Integer(-456))));

    let val = Value::Text("0".into());
    let res = apply_numeric_affinity(val.as_value_ref(), false);
    assert_eq!(res, Some(ValueRef::Numeric(Numeric::Integer(0))));
}

#[test]
fn test_apply_numeric_affinity_vertical_tab() {
    // vertical tab (0x0B) is whitespace to SQLite, unlike Rust's
    // is_ascii_whitespace(). https://github.com/tursodatabase/turso/issues/8454
    let val = Value::Text("\x0b12".into());
    let res = apply_numeric_affinity(val.as_value_ref(), false);
    assert_eq!(res, Some(ValueRef::Numeric(Numeric::Integer(12))));

    let val = Value::Text("12\x0b".into());
    let res = apply_numeric_affinity(val.as_value_ref(), false);
    assert_eq!(res, Some(ValueRef::Numeric(Numeric::Integer(12))));
}

#[test]
fn test_apply_numeric_affinity_extreme_exponent_gives_infinity() {
    let val = Value::Text("3139353734372E383932303939343135".into());
    let res = apply_numeric_affinity(val.as_value_ref(), false);
    assert!(res.is_some());
    match res.unwrap() {
        ValueRef::Numeric(Numeric::Float(f)) => assert!(f64::from(f).is_infinite()),
        other => panic!("expected Float, got {other:?}"),
    }
}

#[test]
fn test_try_for_float_precision() {
    // This test verifies that try_for_float uses high-precision arithmetic
    // to avoid rounding errors when computing significand * 10^exponent.
    // Naive f64 multiplication accumulates errors; Dekker double-double fixes this.
    let (_, parsed) = try_for_float(b"12345678901234567e-5");
    let expected: f64 = "12345678901234567e-5".parse().unwrap();
    assert_eq!(
        parsed.as_float().unwrap().to_bits(),
        expected.to_bits(),
        "try_for_float precision mismatch: got {}, expected {expected}",
        parsed.as_float().unwrap(),
    );
}

#[test]
fn test_try_for_float_i64_min_is_integer() {
    // |i64::MIN| is one past i64::MAX, so the integer range check has to be
    // sign-dependent the way sqlite3Atoi64 is.
    let (res, parsed) = try_for_float(b"-9223372036854775808");
    assert_eq!(res, NumericParseResult::PureInteger);
    assert_eq!(parsed.as_integer(), Some(i64::MIN));

    let (res, parsed) = try_for_float(b"  -9223372036854775808  ");
    assert_eq!(res, NumericParseResult::PureInteger);
    assert_eq!(parsed.as_integer(), Some(i64::MIN));

    let (res, parsed) = try_for_float(b"-009223372036854775808");
    assert_eq!(res, NumericParseResult::PureInteger);
    assert_eq!(parsed.as_integer(), Some(i64::MIN));

    // One past the negative limit, and the positive twin, stay floats.
    let (_, parsed) = try_for_float(b"-9223372036854775809");
    assert_eq!(parsed.as_integer(), None);
    let (_, parsed) = try_for_float(b"9223372036854775808");
    assert_eq!(parsed.as_integer(), None);

    // The limits that already worked must keep working.
    let (_, parsed) = try_for_float(b"-9223372036854775807");
    assert_eq!(parsed.as_integer(), Some(-i64::MAX));
    let (_, parsed) = try_for_float(b"9223372036854775807");
    assert_eq!(parsed.as_integer(), Some(i64::MAX));

    // The magnitude is only integral when it is a bare integer.
    let (_, parsed) = try_for_float(b"-9223372036854775808.0");
    assert_eq!(parsed.as_integer(), None);
    let (_, parsed) = try_for_float(b"-9.223372036854775808e18");
    assert_eq!(parsed.as_integer(), None);
}

#[test]
fn affinity_repr_round_trips() {
    for affinity in [
        Affinity::Blob,
        Affinity::Text,
        Affinity::Numeric,
        Affinity::Integer,
        Affinity::Real,
        Affinity::None,
    ] {
        assert_eq!(
            Affinity::from_repr(affinity as u32),
            Some(affinity),
            "{affinity:?} did not round trip",
        );
    }

    assert_eq!(Affinity::from_repr(0), None);
}
