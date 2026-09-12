use super::*;
use bigdecimal::Zero;
use std::str::FromStr;

#[test]
fn test_roundtrip_positive() {
    let val = BigDecimal::from_str("123.45").unwrap();
    let blob = bigdecimal_to_blob(&val);
    let decoded = blob_to_bigdecimal(&blob).unwrap();
    assert_eq!(val, decoded);
}

#[test]
fn test_roundtrip_negative() {
    let val = BigDecimal::from_str("-987.654").unwrap();
    let blob = bigdecimal_to_blob(&val);
    let decoded = blob_to_bigdecimal(&blob).unwrap();
    assert_eq!(val, decoded);
}

#[test]
fn test_roundtrip_zero() {
    let val = BigDecimal::from_str("0").unwrap();
    let blob = bigdecimal_to_blob(&val);
    let decoded = blob_to_bigdecimal(&blob).unwrap();
    assert!(decoded.is_zero());
}

#[test]
fn test_roundtrip_large() {
    // 100+ digit number
    let s = "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890.99";
    let val = BigDecimal::from_str(s).unwrap();
    let blob = bigdecimal_to_blob(&val);
    let decoded = blob_to_bigdecimal(&blob).unwrap();
    assert_eq!(val, decoded);
}

#[test]
fn test_validate_precision_scale_ok() {
    let val = BigDecimal::from_str("123.45").unwrap();
    let result = validate_precision_scale(&val, 5, 2);
    assert!(result.is_ok());
}

#[test]
fn test_validate_precision_scale_overflow() {
    let val = BigDecimal::from_str("12345.67").unwrap();
    let result = validate_precision_scale(&val, 5, 2);
    assert!(result.is_err());
}

// ================================================================
// Property-based fuzz tests (quickcheck)
// ================================================================

use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;

/// Newtype for generating arbitrary BigDecimal values via quickcheck.
#[derive(Debug, Clone)]
struct ArbDecimal(BigDecimal);

impl Arbitrary for ArbDecimal {
    fn arbitrary(g: &mut Gen) -> Self {
        let mantissa = i64::arbitrary(g);
        // Scale in [-100, 100] — covers negative scale (large integers),
        // zero scale (plain integers), and positive scale (decimals).
        let scale = (i64::arbitrary(g) % 101).abs();
        let sign_flip = bool::arbitrary(g);
        let s = if sign_flip { -scale } else { scale };
        ArbDecimal(BigDecimal::new(BigInt::from(mantissa), s))
    }
}

/// Newtype for generating valid (precision, scale) pairs.
#[derive(Debug, Clone)]
struct ArbPrecisionScale {
    precision: i64,
    scale: i64,
}

impl Arbitrary for ArbPrecisionScale {
    fn arbitrary(g: &mut Gen) -> Self {
        let precision = (u8::arbitrary(g) % 38) as i64 + 1; // 1..=38
        let scale = (u8::arbitrary(g) as i64) % (precision + 1); // 0..=precision
        ArbPrecisionScale { precision, scale }
    }
}

// P1: Roundtrip encode/decode — blob_to_bigdecimal(bigdecimal_to_blob(x)) == x
#[quickcheck]
fn prop_blob_roundtrip(arb: ArbDecimal) -> bool {
    let blob = bigdecimal_to_blob(&arb.0);
    let decoded = blob_to_bigdecimal(&blob).unwrap();
    decoded == arb.0
}

// P2: format_numeric parse roundtrip — for non-negative scale,
// parsing the formatted string should produce a numerically equal value.
#[quickcheck]
fn prop_format_parse_roundtrip(arb: ArbDecimal) -> bool {
    let (_, scale) = arb.0.as_bigint_and_exponent();
    if scale < 0 {
        return true; // skip negative scale — format_numeric expands it
    }
    let formatted = format_numeric(&arb.0);
    let parsed = BigDecimal::from_str(&formatted).unwrap();
    // Numerically equal (may differ in internal scale representation)
    parsed == arb.0
}

// P3: validate_precision_scale idempotence — applying it twice gives same result.
#[quickcheck]
fn prop_validate_idempotent(arb: ArbDecimal, ps: ArbPrecisionScale) -> bool {
    let first = validate_precision_scale(&arb.0, ps.precision, ps.scale);
    match first {
        Ok(y) => {
            let second = validate_precision_scale(&y, ps.precision, ps.scale).unwrap();
            second == y
        }
        Err(_) => true, // rejection is fine
    }
}

// P4: validate_precision_scale bounds — result fits within declared precision/scale.
#[quickcheck]
fn prop_validate_respects_bounds(arb: ArbDecimal, ps: ArbPrecisionScale) -> bool {
    match validate_precision_scale(&arb.0, ps.precision, ps.scale) {
        Ok(y) => {
            let (bigint, result_scale) = y.as_bigint_and_exponent();
            // Scale must match requested scale
            if result_scale != ps.scale {
                return false;
            }
            // Digit count must be <= precision
            let digits = bigint.magnitude().to_string();
            let digit_count = if digits == "0" {
                0
            } else {
                digits.len() as i64
            };
            digit_count <= ps.precision
        }
        Err(_) => true,
    }
}

// P5: blob_to_bigdecimal never panics on arbitrary bytes.
#[quickcheck]
fn prop_blob_decode_no_panic(data: Vec<u8>) -> bool {
    let _ = blob_to_bigdecimal(&data);
    true // just checking it doesn't panic
}

// P6: format_numeric never panics.
#[quickcheck]
fn prop_format_no_panic(arb: ArbDecimal) -> bool {
    let _ = format_numeric(&arb.0);
    true
}

// P7: Ordering is preserved through encode/decode roundtrip.
#[quickcheck]
fn prop_ordering_preserved(a: ArbDecimal, b: ArbDecimal) -> bool {
    let blob_a = bigdecimal_to_blob(&a.0);
    let blob_b = bigdecimal_to_blob(&b.0);
    let decoded_a = blob_to_bigdecimal(&blob_a).unwrap();
    let decoded_b = blob_to_bigdecimal(&blob_b).unwrap();
    // The ordering of decoded values must match the ordering of originals
    a.0.cmp(&b.0) == decoded_a.cmp(&decoded_b)
}
