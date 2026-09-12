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
        // Must precede the reduction: `i64::MIN % -1` overflows, and rustc emits
        // that check even in release. -1 divides everything, so the GCD is 1.
        if a == -1 || b == -1 {
            return Some(1);
        }
        // Reduce the MIN operand once. Safe now: the divisor is neither 0 nor
        // -1, and the result has |a| < i64::MAX so the loop cannot see MIN again.
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
#[path = "../tests/unit/functions/math/tests.rs"]
mod tests;
