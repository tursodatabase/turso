use crate::types::Text;

use super::*;

fn create_text(s: &str) -> Value {
    Value::Text(s.into())
}

fn create_json(s: &str) -> Value {
    Value::Text(Text::json(s.to_string()))
}

#[test]
fn test_basic_text_replacement() {
    let target = create_text(r#"{"name":"John","age":"30"}"#);
    let patch = create_text(r#"{"age":"31"}"#);
    let cache = JsonCacheCell::new();

    let result = json_patch(&target, &patch, &cache).unwrap();
    assert_eq!(result, create_json(r#"{"name":"John","age":"31"}"#));
}

#[test]
fn test_null_field_removal() {
    let target = create_text(r#"{"name":"John","email":"john@example.com"}"#);
    let patch = create_text(r#"{"email":null}"#);
    let cache = JsonCacheCell::new();

    let result = json_patch(&target, &patch, &cache).unwrap();
    assert_eq!(result, create_json(r#"{"name":"John"}"#));
}

#[test]
fn test_nested_object_merge() {
    let target = create_text(r#"{"user":{"name":"John","details":{"age":"30","score":"95.5"}}}"#);

    let patch = create_text(r#"{"user":{"details":{"score":"97.5"}}}"#);
    let cache = JsonCacheCell::new();

    let result = json_patch(&target, &patch, &cache).unwrap();
    assert_eq!(
        result,
        create_json(r#"{"user":{"name":"John","details":{"age":"30","score":"97.5"}}}"#)
    );
}

#[test]
fn test_blob_target_parses_as_document() {
    // x'313233' is the text "123": a blob that is not JSONB is read
    // as JSON text, like SQLite does.
    let target = Value::from_slice(b"123").expect(crate::alloc::ALLOC_ERR_MSG);
    let patch = create_text("{}");
    let cache = JsonCacheCell::new();

    let result = json_patch(&target, &patch, &cache).unwrap();
    assert_eq!(result, create_json("{}"));
}

#[test]
fn test_deep_null_replacement() {
    let target = create_text(r#"{"level1":{"level2":{"keep":"value","remove":"value"}}}"#);

    let patch = create_text(r#"{"level1":{"level2":{"remove":null}}}"#);
    let cache = JsonCacheCell::new();

    let result = json_patch(&target, &patch, &cache).unwrap();
    assert_eq!(
        result,
        create_json(r#"{"level1":{"level2":{"keep":"value"}}}"#)
    );
}

#[test]
fn test_empty_patch() {
    let target = create_json(r#"{"name":"John","age":"30"}"#);
    let patch = create_text("{}");
    let cache = JsonCacheCell::new();

    let result = json_patch(&target, &patch, &cache).unwrap();
    assert_eq!(result, target);
}

#[test]
fn test_add_new_field() {
    let target = create_text(r#"{"existing":"value"}"#);
    let patch = create_text(r#"{"new":"field"}"#);
    let cache = JsonCacheCell::new();

    let result = json_patch(&target, &patch, &cache).unwrap();
    assert_eq!(result, create_json(r#"{"existing":"value","new":"field"}"#));
}

#[test]
fn test_complete_object_replacement() {
    let target = create_text(r#"{"old":{"nested":"value"}}"#);
    let patch = create_text(r#"{"old":"new_value"}"#);
    let cache = JsonCacheCell::new();

    let result = json_patch(&target, &patch, &cache).unwrap();
    assert_eq!(result, create_json(r#"{"old":"new_value"}"#));
}

#[test]
fn test_json_remove_empty_args() {
    let args: [Value; 0] = [];
    let json_cache = JsonCacheCell::new();
    assert_eq!(json_remove(&args, &json_cache).unwrap(), Value::Null);
}

#[test]
fn test_json_remove_array_element() {
    let args = [create_json(r#"[1,2,3,4,5]"#), create_text("$[2]")];

    let json_cache = JsonCacheCell::new();
    let result = json_remove(&args, &json_cache).unwrap();
    match result {
        Value::Text(t) => assert_eq!(t.as_str(), "[1,2,4,5]"),
        _ => panic!("Expected Text value"),
    }
}

#[test]
fn test_json_remove_multiple_paths() {
    let args = [
        create_json(r#"{"a": 1, "b": 2, "c": 3}"#),
        create_text("$.a"),
        create_text("$.c"),
    ];

    let json_cache = JsonCacheCell::new();
    let result = json_remove(&args, &json_cache).unwrap();
    match result {
        Value::Text(t) => assert_eq!(t.as_str(), r#"{"b":2}"#),
        _ => panic!("Expected Text value"),
    }
}

#[test]
fn test_json_remove_nested_paths() {
    let args = [
        create_json(r#"{"a": {"b": {"c": 1, "d": 2}}}"#),
        create_text("$.a.b.c"),
    ];

    let json_cache = JsonCacheCell::new();
    let result = json_remove(&args, &json_cache).unwrap();
    match result {
        Value::Text(t) => assert_eq!(t.as_str(), r#"{"a":{"b":{"d":2}}}"#),
        _ => panic!("Expected Text value"),
    }
}

#[test]
fn test_json_remove_duplicate_keys() {
    let args = [
        create_json(r#"{"a": 1, "a": 2, "a": 3}"#),
        create_text("$.a"),
    ];

    let json_cache = JsonCacheCell::new();
    let result = json_remove(&args, &json_cache).unwrap();
    match result {
        Value::Text(t) => assert_eq!(t.as_str(), r#"{"a":2,"a":3}"#),
        _ => panic!("Expected Text value"),
    }
}

#[test]
fn test_json_remove_invalid_path() {
    let args = [
        create_json(r#"{"a": 1}"#),
        Value::from_i64(42), // Invalid path type
    ];

    let json_cache = JsonCacheCell::new();
    assert!(json_remove(&args, &json_cache).is_err());
}

#[test]
fn test_json_remove_complex_case() {
    let args = [
        create_json(r#"{"a":[1,2,3],"b":{"x":1,"x":2},"c":[{"y":1},{"y":2}]}"#),
        create_text("$.a[1]"),
        create_text("$.b.x"),
        create_text("$.c[0].y"),
    ];

    let json_cache = JsonCacheCell::new();
    let result = json_remove(&args, &json_cache).unwrap();
    match result {
        Value::Text(t) => {
            let value = t.as_str();
            assert!(value.contains(r#"[1,3]"#));
            assert!(value.contains(r#"{"x":2}"#));
        }
        _ => panic!("Expected Text value"),
    }
}
