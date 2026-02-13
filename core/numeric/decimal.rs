use bigdecimal::BigDecimal;
use num_bigint::{BigInt, Sign};

use crate::LimboError;

const NUMERIC_BLOB_VERSION: u8 = 0x01;
const FLAG_NEGATIVE: u8 = 0x01;
const MAX_SCALE_MAGNITUDE: i64 = 1_000_000;

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
pub fn bigdecimal_to_blob(val: &BigDecimal) -> Vec<u8> {
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

    let mut buf = Vec::with_capacity(14 + limbs.len() * 4);

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
}
