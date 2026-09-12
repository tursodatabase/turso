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
fn gcd_min_by_minus_one() {
    // Without the -1 guard these overflow in `i64::MIN % -1`.
    assert_eq!(exec_gcd(&v(i64::MIN), &v(-1)).unwrap(), v(1));
    assert_eq!(exec_gcd(&v(-1), &v(i64::MIN)).unwrap(), v(1));
    // +1 takes the reduction path instead and must agree.
    assert_eq!(exec_gcd(&v(i64::MIN), &v(1)).unwrap(), v(1));
    assert_eq!(exec_gcd(&v(1), &v(i64::MIN)).unwrap(), v(1));
}

#[test]
fn lcm_min_by_minus_one() {
    // lcm is 2^63, not representable in i64.
    assert!(matches!(
        exec_lcm(&v(i64::MIN), &v(-1)),
        Err(LimboError::IntegerOverflow)
    ));
    assert!(matches!(
        exec_lcm(&v(-1), &v(i64::MIN)),
        Err(LimboError::IntegerOverflow)
    ));
    assert!(matches!(
        exec_lcm(&v(i64::MIN), &v(1)),
        Err(LimboError::IntegerOverflow)
    ));
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
