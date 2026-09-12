use super::*;

#[test]
fn integer_conversions_are_checked() {
    assert_eq!(i32::from_value(Value::Integer(-5)).unwrap(), -5);
    i32::from_value(Value::Integer(i64::from(i32::MAX) + 1)).unwrap_err();
    i32::from_value(Value::Integer(i64::from(i32::MIN) - 1)).unwrap_err();

    assert_eq!(u32::from_value(Value::Integer(7)).unwrap(), 7);
    u32::from_value(Value::Integer(-1)).unwrap_err();
    u32::from_value(Value::Integer(i64::from(u32::MAX) + 1)).unwrap_err();

    assert_eq!(
        u64::from_value(Value::Integer(i64::MAX)).unwrap(),
        i64::MAX as u64
    );
    u64::from_value(Value::Integer(-1)).unwrap_err();
}

#[test]
fn f64_accepts_integers() {
    assert_eq!(f64::from_value(Value::Integer(3)).unwrap(), 3.0);
    assert_eq!(f64::from_value(Value::Real(1.5)).unwrap(), 1.5);
    f64::from_value(Value::Text("x".to_string())).unwrap_err();
}

#[test]
fn bool_accepts_only_zero_and_one() {
    assert!(!bool::from_value(Value::Integer(0)).unwrap());
    assert!(bool::from_value(Value::Integer(1)).unwrap());
    bool::from_value(Value::Integer(2)).unwrap_err();
    bool::from_value(Value::Integer(-1)).unwrap_err();
    bool::from_value(Value::Text("true".to_string())).unwrap_err();
}

#[test]
fn fixed_size_byte_arrays_require_exact_length() {
    assert_eq!(
        <[u8; 3]>::from_value(Value::Blob(vec![1, 2, 3])).unwrap(),
        [1, 2, 3]
    );
    <[u8; 3]>::from_value(Value::Blob(vec![1, 2])).unwrap_err();
    <[u8; 3]>::from_value(Value::Integer(1)).unwrap_err();
}

#[test]
fn option_maps_null() {
    assert_eq!(Option::<i64>::from_value(Value::Null).unwrap(), None);
    assert_eq!(
        Option::<i64>::from_value(Value::Integer(4)).unwrap(),
        Some(4)
    );
}
