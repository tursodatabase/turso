//! Common SQL math functions not in SQLite's built-in set.
//!
//! `gcd(a, b)` and `lcm(a, b)` are present in PostgreSQL, MySQL 8 and Oracle.
//! Both operate on signed 64-bit integers and propagate `LimboError::IntegerOverflow`
//! on values that can't be represented — matching PG's `ERROR: bigint out of range`
//! contract rather than silently wrapping.

use crate::types::Value;
use crate::{LimboError, Result};

/// Internal Euclidean GCD. Always returns a non-negative value.
fn gcd_inner(mut a: i64, mut b: i64) -> Option<i64> {
    // GCD is defined as positive; `wrapping_abs(i64::MIN)` is still `i64::MIN`,
    // so flag that case explicitly rather than returning a negative result.
    if a == i64::MIN || b == i64::MIN {
        // Special case: gcd(i64::MIN, 0) = |i64::MIN|, which doesn't fit in i64.
        // gcd(i64::MIN, i64::MIN) = |i64::MIN|, same problem.
        if a == 0 || b == 0 || a == b {
            return None;
        }
        // Recover: reduce the MIN-valued operand once via the modulo loop body.
        // a = a % b is safe because b != 0 and b != i64::MIN here.
        if a == i64::MIN {
            a %= b;
        } else {
            b %= a;
        }
    }
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    Some(a.abs())
}

/// `gcd(a, b)` — greatest common divisor of two integers. Returns NULL when
/// either argument is NULL. Errors with [`LimboError::IntegerOverflow`] when
/// the result would not fit in `i64` (only happens with `i64::MIN`).
pub fn exec_gcd(a: &Value, b: &Value) -> Result<Value> {
    let (Some(a), Some(b)) = (value_as_i64(a), value_as_i64(b)) else {
        return Ok(Value::Null);
    };
    match gcd_inner(a, b) {
        Some(g) => Ok(Value::from_i64(g)),
        None => Err(LimboError::IntegerOverflow),
    }
}

/// `lcm(a, b)` — least common multiple of two integers. Returns 0 when either
/// argument is 0 (matches PG), NULL when either is NULL. Errors with
/// [`LimboError::IntegerOverflow`] when the result doesn't fit in `i64`.
pub fn exec_lcm(a: &Value, b: &Value) -> Result<Value> {
    let (Some(a), Some(b)) = (value_as_i64(a), value_as_i64(b)) else {
        return Ok(Value::Null);
    };
    if a == 0 || b == 0 {
        return Ok(Value::from_i64(0));
    }
    let g = gcd_inner(a, b).ok_or(LimboError::IntegerOverflow)?;
    // (a / g) * |b| — checked to surface overflow. PG returns a non-negative
    // LCM; we mirror that with `abs` after the multiplication so the sign of
    // the inputs doesn't leak into the result.
    let lcm = (a / g)
        .checked_mul(b.checked_abs().ok_or(LimboError::IntegerOverflow)?)
        .and_then(i64::checked_abs)
        .ok_or(LimboError::IntegerOverflow)?;
    Ok(Value::from_i64(lcm))
}

/// Coerce a Value to i64 for the math-function entry points. Text inputs are
/// parsed in the same way SQLite's arithmetic operators would. NULL passes
/// through as `None`; anything that can't be parsed also returns `None` (the
/// caller surfaces that as SQL NULL).
fn value_as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Null => None,
        Value::Numeric(crate::Numeric::Integer(i)) => Some(*i),
        Value::Numeric(crate::Numeric::Float(f)) => {
            let f: f64 = (*f).into();
            if f.is_finite() {
                Some(f as i64)
            } else {
                None
            }
        }
        Value::Text(t) => t.as_str().parse::<i64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    fn v(i: i64) -> Value {
        Value::from_i64(i)
    }

    #[test]
    fn gcd_basic() {
        assert_eq!(exec_gcd(&v(12), &v(8)).unwrap(), v(4));
        assert_eq!(exec_gcd(&v(0), &v(7)).unwrap(), v(7));
        assert_eq!(exec_gcd(&v(7), &v(0)).unwrap(), v(7));
        assert_eq!(exec_gcd(&v(0), &v(0)).unwrap(), v(0));
        // GCD is always non-negative regardless of operand signs.
        assert_eq!(exec_gcd(&v(-12), &v(8)).unwrap(), v(4));
        assert_eq!(exec_gcd(&v(-12), &v(-8)).unwrap(), v(4));
    }

    #[test]
    fn gcd_null_propagates() {
        assert!(matches!(
            exec_gcd(&Value::Null, &v(7)).unwrap(),
            Value::Null
        ));
        assert!(matches!(
            exec_gcd(&v(7), &Value::Null).unwrap(),
            Value::Null
        ));
    }

    #[test]
    fn gcd_overflow() {
        // i64::MIN doesn't have a representable absolute value in i64.
        assert!(matches!(
            exec_gcd(&v(i64::MIN), &v(0)),
            Err(LimboError::IntegerOverflow)
        ));
        assert!(matches!(
            exec_gcd(&v(i64::MIN), &v(i64::MIN)),
            Err(LimboError::IntegerOverflow)
        ));
        // gcd(i64::MIN, x) for x != 0, i64::MIN works because we reduce first.
        assert_eq!(exec_gcd(&v(i64::MIN), &v(2)).unwrap(), v(2));
    }

    #[test]
    fn lcm_basic() {
        assert_eq!(exec_lcm(&v(4), &v(6)).unwrap(), v(12));
        assert_eq!(exec_lcm(&v(0), &v(5)).unwrap(), v(0));
        assert_eq!(exec_lcm(&v(5), &v(0)).unwrap(), v(0));
        // PG returns the non-negative LCM regardless of operand signs.
        assert_eq!(exec_lcm(&v(-4), &v(6)).unwrap(), v(12));
        assert_eq!(exec_lcm(&v(-4), &v(-6)).unwrap(), v(12));
    }

    #[test]
    fn lcm_null_propagates() {
        assert!(matches!(
            exec_lcm(&Value::Null, &v(7)).unwrap(),
            Value::Null
        ));
        assert!(matches!(
            exec_lcm(&v(7), &Value::Null).unwrap(),
            Value::Null
        ));
    }

    #[test]
    fn lcm_overflow() {
        // Two large coprime values can't multiply within i64 range.
        assert!(matches!(
            exec_lcm(&v(i64::MAX), &v(3)),
            Err(LimboError::IntegerOverflow)
        ));
    }
}
