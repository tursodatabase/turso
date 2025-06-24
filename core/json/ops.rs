use crate::{types::Value, vdbe::Register};

use super::{
    convert_dbtype_to_jsonb, curry_convert_dbtype_to_jsonb, json_path_from_owned_value,
    json_string_to_db_type,
    jsonb::{DeleteOperation, InsertOperation, ReplaceOperation},
    Conv, JsonCacheCell, OutputVariant,
};

/// The function follows RFC 7386 JSON Merge Patch semantics:
/// * If the patch is null, the target is replaced with null
/// * If the patch contains a scalar value, the target is replaced with that value
/// * If both target and patch are objects, the patch is recursively applied
/// * null values in the patch result in property removal from the target
pub fn json_patch(target: &Value, patch: &Value, cache: &JsonCacheCell) -> crate::Result<Value> {
    match (target, patch) {
        (Value::Blob(_), _) | (_, Value::Blob(_)) => {
            crate::bail_constraint_error!("blob is not supported!");
        }
        _ => (),
    }
    let make_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut target = cache.get_or_insert_with(target, &make_jsonb)?;
    let patch = cache.get_or_insert_with(patch, &make_jsonb)?;

    target.patch(&patch)?;

    let element_type = target.is_valid()?;

    json_string_to_db_type(target, element_type, OutputVariant::ElementType)
}

pub fn jsonb_patch(target: &Value, patch: &Value, cache: &JsonCacheCell) -> crate::Result<Value> {
    match (target, patch) {
        (Value::Blob(_), _) | (_, Value::Blob(_)) => {
            crate::bail_constraint_error!("blob is not supported!");
        }
        _ => (),
    }
    let make_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut target = cache.get_or_insert_with(target, &make_jsonb)?;
    let patch = cache.get_or_insert_with(patch, &make_jsonb)?;

    target.patch(&patch)?;

    let element_type = target.is_valid()?;

    json_string_to_db_type(target, element_type, OutputVariant::Binary)
}

pub fn json_remove(args: &[Register], json_cache: &JsonCacheCell) -> crate::Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(args[0].get_owned_value(), make_jsonb_fn)?;
    for arg in &args[1..] {
        if let Some(path) = json_path_from_owned_value(arg.get_owned_value(), true)? {
            let mut op = DeleteOperation::new();
            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.is_valid()?;

    json_string_to_db_type(json, el_type, OutputVariant::String)
}

pub fn jsonb_remove(args: &[Register], json_cache: &JsonCacheCell) -> crate::Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(args[0].get_owned_value(), make_jsonb_fn)?;
    for arg in &args[1..] {
        if let Some(path) = json_path_from_owned_value(arg.get_owned_value(), true)? {
            let mut op = DeleteOperation::new();
            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    Ok(Value::Blob(json.data()))
}

pub fn json_replace(args: &[Register], json_cache: &JsonCacheCell) -> crate::Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(args[0].get_owned_value(), make_jsonb_fn)?;
    let other = args[1..].chunks_exact(2);
    for chunk in other {
        let path = json_path_from_owned_value(chunk[0].get_owned_value(), true)?;

        let value = convert_dbtype_to_jsonb(chunk[1].get_owned_value(), Conv::NotStrict)?;
        if let Some(path) = path {
            let mut op = ReplaceOperation::new(value);

            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.is_valid()?;

    json_string_to_db_type(json, el_type, super::OutputVariant::String)
}

pub fn jsonb_replace(args: &[Register], json_cache: &JsonCacheCell) -> crate::Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(args[0].get_owned_value(), make_jsonb_fn)?;
    let other = args[1..].chunks_exact(2);
    for chunk in other {
        let path = json_path_from_owned_value(chunk[0].get_owned_value(), true)?;
        let value = convert_dbtype_to_jsonb(chunk[1].get_owned_value(), Conv::NotStrict)?;
        if let Some(path) = path {
            let mut op = ReplaceOperation::new(value);

            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.is_valid()?;

    json_string_to_db_type(json, el_type, OutputVariant::Binary)
}

pub fn json_insert(args: &[Register], json_cache: &JsonCacheCell) -> crate::Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(args[0].get_owned_value(), make_jsonb_fn)?;
    let other = args[1..].chunks_exact(2);
    for chunk in other {
        let path = json_path_from_owned_value(chunk[0].get_owned_value(), true)?;
        let value = convert_dbtype_to_jsonb(chunk[1].get_owned_value(), Conv::NotStrict)?;
        if let Some(path) = path {
            let mut op = InsertOperation::new(value);

            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.is_valid()?;

    json_string_to_db_type(json, el_type, OutputVariant::String)
}

pub fn jsonb_insert(args: &[Register], json_cache: &JsonCacheCell) -> crate::Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(args[0].get_owned_value(), make_jsonb_fn)?;
    let other = args[1..].chunks_exact(2);
    for chunk in other {
        let path = json_path_from_owned_value(chunk[0].get_owned_value(), true)?;
        let value = convert_dbtype_to_jsonb(chunk[1].get_owned_value(), Conv::NotStrict)?;
        if let Some(path) = path {
            let mut op = InsertOperation::new(value);

            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.is_valid()?;

    json_string_to_db_type(json, el_type, OutputVariant::Binary)
}

#[cfg(test)]
mod tests {
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
        let target =
            create_text(r#"{"user":{"name":"John","details":{"age":"30","score":"95.5"}}}"#);

        let patch = create_text(r#"{"user":{"details":{"score":"97.5"}}}"#);
        let cache = JsonCacheCell::new();

        let result = json_patch(&target, &patch, &cache).unwrap();
        assert_eq!(
            result,
            create_json(r#"{"user":{"name":"John","details":{"age":"30","score":"97.5"}}}"#)
        );
    }

    #[test]
    #[should_panic(expected = "blob is not supported!")]
    fn test_blob_not_supported() {
        let target = Value::Blob(vec![1, 2, 3]);
        let patch = create_text("{}");
        let cache = JsonCacheCell::new();

        json_patch(&target, &patch, &cache).unwrap();
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
        let args = vec![];
        let json_cache = JsonCacheCell::new();
        assert_eq!(json_remove(&args, &json_cache).unwrap(), Value::Null);
    }

    #[test]
    fn test_json_remove_array_element() {
        let args = vec![
            Register::Value(create_json(r#"[1,2,3,4,5]"#)),
            Register::Value(create_text("$[2]")),
        ];

        let json_cache = JsonCacheCell::new();
        let result = json_remove(&args, &json_cache).unwrap();
        match result {
            Value::Text(t) => assert_eq!(t.as_str(), "[1,2,4,5]"),
            _ => panic!("Expected Text value"),
        }
    }

    #[test]
    fn test_json_remove_multiple_paths() {
        let args = vec![
            Register::Value(create_json(r#"{"a": 1, "b": 2, "c": 3}"#)),
            Register::Value(create_text("$.a")),
            Register::Value(create_text("$.c")),
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
        let args = vec![
            Register::Value(create_json(r#"{"a": {"b": {"c": 1, "d": 2}}}"#)),
            Register::Value(create_text("$.a.b.c")),
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
        let args = vec![
            Register::Value(create_json(r#"{"a": 1, "a": 2, "a": 3}"#)),
            Register::Value(create_text("$.a")),
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
        let args = vec![
            Register::Value(create_json(r#"{"a": 1}"#)),
            Register::Value(Value::Integer(42)), // Invalid path type
        ];

        let json_cache = JsonCacheCell::new();
        assert!(json_remove(&args, &json_cache).is_err());
    }

    #[test]
    fn test_json_remove_complex_case() {
        let args = vec![
            Register::Value(create_json(
                r#"{"a":[1,2,3],"b":{"x":1,"x":2},"c":[{"y":1},{"y":2}]}"#,
            )),
            Register::Value(create_text("$.a[1]")),
            Register::Value(create_text("$.b.x")),
            Register::Value(create_text("$.c[0].y")),
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
}
