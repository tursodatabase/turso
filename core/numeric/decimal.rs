use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use bigdecimal::{BigDecimal, Zero};
use num_bigint::{BigInt, Sign};

use crate::alloc::TursoVecExt;
use crate::{LimboError, Numeric, Value, ValueBlob, ValueRef};

const NUMERIC_BLOB_VERSION: u8 = 0x01;
const FLAG_NEGATIVE: u8 = 0x01;
const MAX_SCALE_MAGNITUDE: i64 = 1_000_000;

/// PostgreSQL's limits on a numeric value: the most digits it may have before
/// the decimal point, and after it.
const MAX_INTEGER_DIGITS: i64 = 131_072;
pub(crate) const MAX_FRACTION_DIGITS: i64 = 16_383;

/// The numeric value of `val`: an integer, a real, text such as `1.5` or `1e5`,
/// or a stored numeric blob. Every numeric function reads its arguments with
/// this, so they all accept the same values within the same limits.
pub fn numeric_from_value(val: &Value) -> crate::Result<BigDecimal> {
    let value = match val {
        Value::Numeric(Numeric::Integer(i)) => return Ok(BigDecimal::from(*i)),
        Value::Numeric(Numeric::Float(f)) => parse_numeric(&f.to_string())?,
        Value::Text(t) => parse_numeric(t.as_str())?,
        Value::Blob(b) => blob_to_bigdecimal(b)?,
        _ => {
            return Err(LimboError::Constraint(format!(
                "invalid input syntax for type numeric: \"{val}\""
            )))
        }
    };
    check_numeric_limits(&value)?;
    // `0e5` parses as zero with scale -5. Zero has no digits, so its exponent
    // is dropped, as PostgreSQL does.
    if value.is_zero() && value.fractional_digit_count() < 0 {
        return Ok(value.with_scale(0));
    }
    Ok(value)
}

fn parse_numeric(text: &str) -> crate::Result<BigDecimal> {
    use std::str::FromStr;
    // Parsing takes time quadratic in the number of digits, so text longer
    // than any value within the limits is refused before it is parsed. The
    // allowance covers a sign, a decimal point and an exponent.
    const MAX_TEXT_LEN: usize = (MAX_INTEGER_DIGITS + MAX_FRACTION_DIGITS) as usize + 32;
    // PostgreSQL skips the whitespace C's isspace() matches around a number.
    let number = text.trim_matches([' ', '\t', '\n', '\r', '\x0b', '\x0c']);
    // Leading zeros are not digits of the value, so they do not count.
    let (sign, unsigned) = match number.strip_prefix(['-', '+']) {
        Some(unsigned) => (&number[..1], unsigned),
        None => ("", number),
    };
    let significant = unsigned.trim_start_matches('0');
    if significant.len() > MAX_TEXT_LEN {
        return Err(numeric_overflow());
    }
    // Keep one zero when the text had any, so `0`, `-0.5` and `00e3` stay
    // valid and `-` and `e3` stay invalid.
    let had_zeros = significant.len() < unsigned.len();
    let number = if had_zeros && !significant.starts_with(|c: char| c.is_ascii_digit()) {
        format!("{sign}0{significant}")
    } else {
        format!("{sign}{significant}")
    };
    BigDecimal::from_str(&number).map_err(|_| {
        LimboError::Constraint(format!("invalid input syntax for type numeric: \"{text}\""))
    })
}

/// An error when `value` has more digits than PostgreSQL allows. Text such as
/// `1e100000000` parses into a small value, but rounding it or adding to it
/// writes out all hundred million digits.
pub fn check_numeric_limits(value: &BigDecimal) -> crate::Result<()> {
    // A scale can be any i64, so the digit counts are computed in i128.
    let fraction_digits = i128::from(value.fractional_digit_count());
    let integer_digits = if value.is_zero() {
        0
    } else {
        i128::from(value.digits()) - fraction_digits
    };
    if integer_digits > i128::from(MAX_INTEGER_DIGITS)
        || fraction_digits > i128::from(MAX_FRACTION_DIGITS)
    {
        return Err(numeric_overflow());
    }
    Ok(())
}

fn numeric_overflow() -> LimboError {
    LimboError::Constraint("value overflows numeric format".to_string())
}

/// The numeric type's order. Indexes and sorts use it, so unlike the numeric
/// functions it never fails: every value has a place. NULL comes first, then
/// numbers by value (integers, reals, numeric text and stored numeric blobs,
/// with -infinity and +infinity at the ends), then any other text by bytes,
/// then any other blob by bytes. Numbers that differ only in trailing zeros,
/// such as 1.5 and 1.50, are equal.
pub fn numeric_compare(left: ValueRef, right: ValueRef) -> Ordering {
    match (NumericSortKey::of(left), NumericSortKey::of(right)) {
        (NumericSortKey::Number(l), NumericSortKey::Number(r)) => compare_numbers(&l, &r),
        (NumericSortKey::Text(l), NumericSortKey::Text(r)) => l.cmp(r),
        (NumericSortKey::Blob(l), NumericSortKey::Blob(r)) => l.cmp(r),
        (l, r) => l.rank().cmp(&r.rank()),
    }
}

/// Hashes `value` so that values `numeric_compare` finds equal hash alike.
pub fn numeric_hash<H: Hasher>(value: ValueRef, state: &mut H) {
    let key = NumericSortKey::of(value);
    key.rank().hash(state);
    match key {
        NumericSortKey::Number(number) => {
            let (digits, scale) = number.normalized().into_bigint_and_exponent();
            digits.hash(state);
            scale.hash(state);
        }
        NumericSortKey::Text(text) => text.hash(state),
        NumericSortKey::Blob(blob) => blob.hash(state),
        NumericSortKey::Null | NumericSortKey::NegativeInfinity | NumericSortKey::Infinity => {}
    }
}

/// Where a value falls in the numeric type's order.
enum NumericSortKey<'a> {
    Null,
    NegativeInfinity,
    Number(BigDecimal),
    Infinity,
    Text(&'a str),
    Blob(&'a [u8]),
}

impl<'a> NumericSortKey<'a> {
    fn of(value: ValueRef<'a>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Numeric(Numeric::Integer(i)) => Self::Number(BigDecimal::from(i)),
            ValueRef::Numeric(Numeric::Float(f)) => {
                let f = f64::from(f);
                if f == f64::INFINITY {
                    Self::Infinity
                } else if f == f64::NEG_INFINITY {
                    Self::NegativeInfinity
                } else {
                    // Every finite real is within the limits once written out.
                    Self::Number(
                        parse_numeric(&f.to_string()).expect("a finite real is a valid numeric"),
                    )
                }
            }
            ValueRef::Text(text) => {
                let text = text.as_str();
                match parse_numeric(text) {
                    Ok(number) if check_numeric_limits(&number).is_ok() => Self::Number(number),
                    _ => Self::Text(text),
                }
            }
            ValueRef::Blob(blob) => match blob_to_bigdecimal(blob) {
                Ok(number) => Self::Number(number),
                Err(_) => Self::Blob(blob),
            },
        }
    }

    fn rank(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::NegativeInfinity => 1,
            Self::Number(_) => 2,
            Self::Infinity => 3,
            Self::Text(_) => 4,
            Self::Blob(_) => 5,
        }
    }
}

/// Compares two numbers without lining up their scales unless they have the
/// same sign and the same number of digits before the decimal point, so
/// 1e131071 against 1e-16383 does not write out 147,000 digits.
fn compare_numbers(left: &BigDecimal, right: &BigDecimal) -> Ordering {
    let by_sign = left.sign().cmp(&right.sign());
    if by_sign != Ordering::Equal || left.is_zero() {
        return by_sign;
    }
    let by_magnitude = leading_digit_position(left).cmp(&leading_digit_position(right));
    let by_magnitude = if left.sign() == Sign::Minus {
        by_magnitude.reverse()
    } else {
        by_magnitude
    };
    by_magnitude.then_with(|| left.cmp(right))
}

/// The power of ten just above the first digit: 3 for 123.4, -1 for 0.012.
/// Trailing zeros do not change it, so 1.5 and 1.50 have the same position.
fn leading_digit_position(number: &BigDecimal) -> i128 {
    i128::from(number.digits()) - i128::from(number.fractional_digit_count())
}

/// Serialize a BigDecimal to our portable blob format.
///
/// Format (all multi-byte values little-endian):
/// ```text
/// [version: 1 byte (0x01)]
/// [flags:   1 byte (bit 0 = sign, 0=positive/zero, 1=negative)]
/// [scale:   8 bytes, i64 LE]
/// [num_limbs: 4 bytes, u32 LE]
/// [limb0: 4 bytes u32 LE] [limb1: 4 bytes u32 LE] ... [limbN: 4 bytes u32 LE]
/// ```
pub fn bigdecimal_to_blob(val: &BigDecimal) -> ValueBlob {
    let (bigint, scale) = val.as_bigint_and_exponent();
    let (sign, magnitude) = bigint.into_parts();

    // magnitude.to_u32_digits() returns limbs in little-endian order
    let limbs = magnitude.to_u32_digits();
    debug_assert!(
        limbs.len() <= u32::MAX as usize,
        "limb count {} exceeds u32::MAX",
        limbs.len()
    );
    let num_limbs = limbs.len() as u32;

    let mut buf = <ValueBlob as TursoVecExt<u8>>::with_capacity(14 + limbs.len() * 4);

    // version
    buf.push(NUMERIC_BLOB_VERSION);

    // flags
    let flags = if sign == Sign::Minus {
        FLAG_NEGATIVE
    } else {
        0
    };
    buf.push(flags);

    // scale (i64 LE)
    buf.extend_from_slice(&scale.to_le_bytes());

    // num_limbs (u32 LE)
    buf.extend_from_slice(&num_limbs.to_le_bytes());

    // limbs (u32 LE each)
    for limb in &limbs {
        buf.extend_from_slice(&limb.to_le_bytes());
    }

    buf
}

/// Deserialize a BigDecimal from our portable blob format.
pub fn blob_to_bigdecimal(blob: &[u8]) -> crate::Result<BigDecimal> {
    // Minimum size: version(1) + flags(1) + scale(8) + num_limbs(4) = 14
    if blob.len() < 14 {
        return Err(LimboError::Constraint(
            "invalid numeric blob: too short".to_string(),
        ));
    }

    let version = blob[0];
    if version != NUMERIC_BLOB_VERSION {
        return Err(LimboError::Constraint(format!(
            "unsupported numeric blob version: {version}"
        )));
    }

    let flags = blob[1];
    let sign = if flags & FLAG_NEGATIVE != 0 {
        Sign::Minus
    } else {
        Sign::Plus
    };

    // SAFETY: blob[2..10] is exactly 8 bytes (checked by min-length guard above)
    let scale = i64::from_le_bytes(blob[2..10].try_into().unwrap());

    // Reject absurd scales from corrupted blobs.
    if !(-MAX_SCALE_MAGNITUDE..=MAX_SCALE_MAGNITUDE).contains(&scale) {
        return Err(LimboError::Constraint(format!(
            "invalid numeric blob: scale {scale} out of range"
        )));
    }

    // SAFETY: blob[10..14] is exactly 4 bytes (checked by min-length guard above)
    let num_limbs = u32::from_le_bytes(blob[10..14].try_into().unwrap()) as usize;

    // Cap limb count to prevent OOM from crafted blobs. 65536 limbs = 256KB,
    // which supports numbers with ~600,000 decimal digits — far beyond any
    // reasonable precision.
    const MAX_LIMBS: usize = 65536;
    if num_limbs > MAX_LIMBS {
        return Err(LimboError::Constraint(format!(
            "invalid numeric blob: limb count {num_limbs} exceeds maximum {MAX_LIMBS}"
        )));
    }

    let expected_len = num_limbs
        .checked_mul(4)
        .and_then(|n| n.checked_add(14))
        .ok_or_else(|| {
            LimboError::Constraint("invalid numeric blob: limb count overflow".to_string())
        })?;
    if blob.len() != expected_len {
        return Err(LimboError::Constraint(format!(
            "invalid numeric blob: expected {expected_len} bytes, got {}",
            blob.len()
        )));
    }

    let mut limbs = Vec::with_capacity(num_limbs);
    for i in 0..num_limbs {
        let offset = 14 + i * 4;
        let limb = u32::from_le_bytes(blob[offset..offset + 4].try_into().unwrap());
        limbs.push(limb);
    }

    // Handle zero case: BigInt::new with empty limbs and Plus sign = 0
    let sign = if limbs.is_empty() { Sign::NoSign } else { sign };
    let bigint = BigInt::new(sign, limbs);
    Ok(BigDecimal::new(bigint, scale))
}

/// Format a BigDecimal as a string, preserving trailing zeros for the scale.
/// BigDecimal::to_string() may drop trailing zeros, but we want "1.10" to
/// remain "1.10" and "0.00" to remain "0.00" for a value stored with scale=2.
pub fn format_numeric(bd: &BigDecimal) -> String {
    let (bigint, scale) = bd.as_bigint_and_exponent();
    if scale <= 0 {
        // No decimal places: just print the integer (possibly scaled up)
        if scale == 0 {
            return bigint.to_string();
        }
        // Negative scale means multiply by 10^(-scale).
        // Cap to MAX_SCALE_MAGNITUDE (validated on read) to avoid runaway allocation.
        let neg_scale = scale.unsigned_abs().min(MAX_SCALE_MAGNITUDE as u64) as u32;
        let factor = num_bigint::BigInt::from(10).pow(neg_scale);
        return (bigint * factor).to_string();
    }
    let scale = scale as usize;
    let is_negative = bigint.sign() == Sign::Minus;
    let digits = bigint.magnitude().to_string();
    let digits_len = digits.len();

    let (integer_part, frac_part) = if digits_len > scale {
        let split = digits_len - scale;
        (&digits[..split], digits[split..].to_string())
    } else {
        let zeros = "0".repeat(scale - digits_len);
        ("0", format!("{zeros}{digits}"))
    };

    if is_negative {
        format!("-{integer_part}.{frac_part}")
    } else {
        format!("{integer_part}.{frac_part}")
    }
}

/// Validate that a BigDecimal fits within the given precision and scale.
/// Precision = total number of significant digits.
/// Scale = number of digits after the decimal point.
pub fn validate_precision_scale(
    val: &BigDecimal,
    precision: i64,
    scale: i64,
) -> crate::Result<BigDecimal> {
    use bigdecimal::Zero;

    if precision <= 0 {
        return Err(LimboError::Constraint(format!(
            "numeric precision must be positive, got {precision}"
        )));
    }
    if scale < 0 {
        return Err(LimboError::Constraint(format!(
            "numeric scale must be non-negative, got {scale}"
        )));
    }
    if scale > precision {
        return Err(LimboError::Constraint(format!(
            "numeric scale ({scale}) must not exceed precision ({precision})"
        )));
    }

    if val.is_zero() {
        return Ok(BigDecimal::new(BigInt::from(0), scale));
    }

    // Round to the requested scale
    let rounded = val.with_scale(scale);

    // Count total significant digits (excluding leading zeros)
    let (bigint, _) = rounded.as_bigint_and_exponent();
    let abs_str = bigint.magnitude().to_string();
    let total_digits = abs_str.len() as i64;

    if total_digits > precision {
        return Err(LimboError::Constraint(format!(
            "numeric value out of range: precision {precision}, scale {scale}"
        )));
    }

    Ok(rounded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AsValueRef;
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

    fn sort_key_values() -> Vec<Value> {
        // In the numeric type's order; values on one line are equal.
        vec![
            Value::Null,
            Value::from_f64(f64::NEG_INFINITY),
            Value::from_text("-1e131071"),
            Value::Numeric(Numeric::Integer(-2)),
            Value::from_text("-1.5"),
            Value::from_text("-1e-16383"),
            Value::Numeric(Numeric::Integer(0)),
            Value::from_text("1e-16383"),
            Value::from_text("1.5"),
            Value::from_f64(2.5),
            Value::from_text("10"),
            Value::from_text("1e131071"),
            Value::from_f64(f64::INFINITY),
            Value::from_text("abc"),
            Value::from_text("abd"),
            Value::from_blob(crate::alloc::vec![0x00]),
            Value::from_blob(crate::alloc::vec![0x01]),
        ]
    }

    #[test]
    fn numeric_compare_orders_every_kind_of_value() {
        let values = sort_key_values();
        for (i, l) in values.iter().enumerate() {
            for (j, r) in values.iter().enumerate() {
                assert_eq!(
                    numeric_compare(l.as_value_ref(), r.as_value_ref()),
                    i.cmp(&j),
                    "{l:?} vs {r:?}"
                );
            }
        }
    }

    #[test]
    fn numeric_equal_values_compare_and_hash_alike() {
        let blob = Value::Blob(bigdecimal_to_blob(&BigDecimal::from_str("1.500").unwrap()));
        let equal = [
            Value::from_text("1.5"),
            Value::from_text("1.50"),
            Value::from_text(" 15e-1\n"),
            Value::from_f64(1.5),
            blob,
        ];
        for l in &equal {
            for r in &equal {
                assert_eq!(
                    numeric_compare(l.as_value_ref(), r.as_value_ref()),
                    Ordering::Equal,
                    "{l:?} vs {r:?}"
                );
                assert_eq!(hash_of(l), hash_of(r), "{l:?} vs {r:?}");
            }
        }
        let zeros = [
            Value::Numeric(Numeric::Integer(0)),
            Value::from_f64(-0.0),
            Value::from_text("0.000"),
        ];
        for z in &zeros {
            assert_eq!(
                numeric_compare(z.as_value_ref(), zeros[0].as_value_ref()),
                Ordering::Equal
            );
            assert_eq!(hash_of(z), hash_of(&zeros[0]));
        }
    }

    fn hash_of(value: &Value) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        numeric_hash(value.as_value_ref(), &mut hasher);
        hasher.finish()
    }
}
