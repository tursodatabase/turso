use crate::Value;

#[test]
fn test_serialize_array() {
    assert_eq!(
        params!([0; 16])[0].as_ref().unwrap(),
        &Value::Blob(vec![0; 16])
    );
}
