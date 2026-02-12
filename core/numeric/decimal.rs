use bigdecimal::BigDecimal;
use num_bigint::{BigInt, Sign};

use crate::LimboError;

const NUMERIC_BLOB_VERSION: u8 = 0x01;
const FLAG_NEGATIVE: u8 = 0x01;

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

    let scale = i64::from_le_bytes(blob[2..10].try_into().unwrap());

    let num_limbs = u32::from_le_bytes(blob[10..14].try_into().unwrap()) as usize;

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
        // Negative scale means multiply by 10^(-scale)
        let factor = num_bigint::BigInt::from(10).pow((-scale) as u32);
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
}
