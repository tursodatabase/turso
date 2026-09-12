use super::*;
use crate::Value;

#[test]
fn tuple_positional_params() {
    let params = (1, "hello", 3.5).into_params().unwrap();
    match params {
        Params::Positional(values) => {
            assert_eq!(values[0], Value::Integer(1));
            assert_eq!(values[1], Value::Text("hello".to_string()));
            assert_eq!(values[2], Value::Real(3.5));
        }
        other => panic!("expected positional params, got {other:?}"),
    }
}

#[test]
fn named_tuple_params() {
    let params = ((":a", 1), (":b", "x")).into_params().unwrap();
    match params {
        Params::Named(values) => {
            assert_eq!(values[0].0, ":a");
            assert_eq!(values[0].1, Value::Integer(1));
            assert_eq!(values[1].0, ":b");
            assert_eq!(values[1].1, Value::Text("x".to_string()));
        }
        other => panic!("expected named params, got {other:?}"),
    }
}

#[test]
fn params_macro() {
    let params = params![1, "hello"].into_params().unwrap();
    assert!(matches!(params, Params::Positional(v) if v.len() == 2));
    let params = params![].into_params().unwrap();
    assert!(matches!(params, Params::None));
}

#[test]
fn named_params_macro() {
    let params = named_params![":a": 1, ":b": "x"].into_params().unwrap();
    assert!(matches!(params, Params::Named(v) if v.len() == 2));
}

#[test]
fn params_from_iter_positional() {
    let params = params_from_iter(vec![1, 2, 3]).into_params().unwrap();
    assert!(matches!(params, Params::Positional(v) if v.len() == 3));
}
