use super::{
    convert_dbtype_to_jsonb, curry_convert_dbtype_to_jsonb, json_document_to_db_type,
    json_edit_path, json_path_from_db_value,
    jsonb::{DeleteOperation, InsertOperation, ReplaceOperation},
    Conv, JsonCacheCell, OutputVariant,
};
use crate::{
    types::{AsValueRef, Value},
    ValueRef,
};

/// The function follows RFC 7386 JSON Merge Patch semantics:
/// * If the patch is null, the target is replaced with null
/// * If the patch contains a scalar value, the target is replaced with that value
/// * If both target and patch are objects, the patch is recursively applied
/// * null values in the patch result in property removal from the target
pub fn json_patch(
    target: impl AsValueRef,
    patch: impl AsValueRef,
    cache: &JsonCacheCell,
) -> crate::Result<Value> {
    let (target, patch) = (target.as_value_ref(), patch.as_value_ref());
    if matches!(target, ValueRef::Null) || matches!(patch, ValueRef::Null) {
        return Ok(Value::Null);
    }
    let make_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut target = cache.get_or_insert_with(target, make_jsonb)?;
    let make_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let patch = cache.get_or_insert_with(patch, make_jsonb)?;

    if patch.element_type()? != super::jsonb::ElementType::OBJECT {
        target = patch;
    } else {
        if target.element_type()? != super::jsonb::ElementType::OBJECT {
            target = super::jsonb::Jsonb::make_empty_obj(0)?;
        }
        target.patch(&patch)?;
    }

    let element_type = target.element_type()?;

    json_document_to_db_type(target, element_type, OutputVariant::String)
}

pub fn jsonb_patch(
    target: impl AsValueRef,
    patch: impl AsValueRef,
    cache: &JsonCacheCell,
) -> crate::Result<Value> {
    let (target, patch) = (target.as_value_ref(), patch.as_value_ref());
    if matches!(target, ValueRef::Null) || matches!(patch, ValueRef::Null) {
        return Ok(Value::Null);
    }
    let make_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut target = cache.get_or_insert_with(target, make_jsonb)?;
    let make_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let patch = cache.get_or_insert_with(patch, make_jsonb)?;

    if patch.element_type()? != super::jsonb::ElementType::OBJECT {
        target = patch;
    } else {
        if target.element_type()? != super::jsonb::ElementType::OBJECT {
            target = super::jsonb::Jsonb::make_empty_obj(0)?;
        }
        target.patch(&patch)?;
    }

    let element_type = target.element_type()?;

    json_document_to_db_type(target, element_type, OutputVariant::Binary)
}

pub fn json_remove<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    // A SQL NULL document is SQL NULL, whatever the paths are.
    if matches!(first_arg.as_value_ref(), ValueRef::Null) {
        return Ok(Value::Null);
    }
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;
    for arg in args {
        if let ValueRef::Text(s) = arg.as_value_ref() {
            if s.as_str() == "$" {
                return Ok(Value::Null);
            }
        }
        // Unlike json_set() and friends, which drop a pair with a SQL NULL
        // path, json_remove() makes the whole result SQL NULL.
        let Some(path) = json_path_from_db_value(&arg, true)? else {
            return Ok(Value::Null);
        };
        let mut op = DeleteOperation::new();
        let _ = json.operate_on_path(&path, &mut op);
    }

    let el_type = json.element_type()?;

    json_document_to_db_type(json, el_type, OutputVariant::String)
}

pub fn jsonb_remove<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    // A SQL NULL document is SQL NULL, whatever the paths are.
    if matches!(first_arg.as_value_ref(), ValueRef::Null) {
        return Ok(Value::Null);
    }
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;
    for arg in args {
        if let ValueRef::Text(s) = arg.as_value_ref() {
            if s.as_str() == "$" {
                return Ok(Value::Null);
            }
        }
        // Unlike json_set() and friends, which drop a pair with a SQL NULL
        // path, json_remove() makes the whole result SQL NULL.
        let Some(path) = json_path_from_db_value(&arg, true)? else {
            return Ok(Value::Null);
        };
        let mut op = DeleteOperation::new();
        let _ = json.operate_on_path(&path, &mut op);
    }

    Ok(Value::Blob(json.data()))
}

pub fn json_replace<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    // A SQL NULL document is SQL NULL, whatever the paths are.
    if matches!(first_arg.as_value_ref(), ValueRef::Null) {
        return Ok(Value::Null);
    }
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;
    // TODO: when `array_chunks` is stabilized we can chunk by 2 here
    while args.len() > 1 {
        let first = args.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "args should have at least 2 elements in loop".to_string(),
            )
        })?;
        let second = args.next().ok_or_else(|| {
            crate::LimboError::InternalError("args should have second element in loop".to_string())
        })?;
        if let Some(path) = json_edit_path(&first, &second)? {
            let value = convert_dbtype_to_jsonb(&second, Conv::SetValue)?;
            let mut op = ReplaceOperation::new(value);

            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.element_type()?;

    json_document_to_db_type(json, el_type, OutputVariant::String)
}

pub fn jsonb_replace<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    // A SQL NULL document is SQL NULL, whatever the paths are.
    if matches!(first_arg.as_value_ref(), ValueRef::Null) {
        return Ok(Value::Null);
    }
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;
    // TODO: when `array_chunks` is stabilized we can chunk by 2 here
    while args.len() > 1 {
        let first = args.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "args should have at least 2 elements in loop".to_string(),
            )
        })?;
        let second = args.next().ok_or_else(|| {
            crate::LimboError::InternalError("args should have second element in loop".to_string())
        })?;
        if let Some(path) = json_edit_path(&first, &second)? {
            let value = convert_dbtype_to_jsonb(&second, Conv::SetValue)?;
            let mut op = ReplaceOperation::new(value);

            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.element_type()?;

    json_document_to_db_type(json, el_type, OutputVariant::Binary)
}

pub fn json_insert<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    // A SQL NULL document is SQL NULL, whatever the paths are.
    if matches!(first_arg.as_value_ref(), ValueRef::Null) {
        return Ok(Value::Null);
    }
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;

    // TODO: when `array_chunks` is stabilized we can chunk by 2 here
    while args.len() > 1 {
        let first = args.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "args should have at least 2 elements in loop".to_string(),
            )
        })?;
        let second = args.next().ok_or_else(|| {
            crate::LimboError::InternalError("args should have second element in loop".to_string())
        })?;
        if let Some(path) = json_edit_path(&first, &second)? {
            let value = convert_dbtype_to_jsonb(&second, Conv::SetValue)?;
            let mut op = InsertOperation::new(value);

            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.element_type()?;

    json_document_to_db_type(json, el_type, OutputVariant::String)
}

pub fn jsonb_insert<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    // A SQL NULL document is SQL NULL, whatever the paths are.
    if matches!(first_arg.as_value_ref(), ValueRef::Null) {
        return Ok(Value::Null);
    }
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;

    // TODO: when `array_chunks` is stabilized we can chunk by 2 here
    while args.len() > 1 {
        let first = args.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "args should have at least 2 elements in loop".to_string(),
            )
        })?;
        let second = args.next().ok_or_else(|| {
            crate::LimboError::InternalError("args should have second element in loop".to_string())
        })?;
        if let Some(path) = json_edit_path(&first, &second)? {
            let value = convert_dbtype_to_jsonb(&second, Conv::SetValue)?;
            let mut op = InsertOperation::new(value);

            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.element_type()?;

    json_document_to_db_type(json, el_type, OutputVariant::Binary)
}

#[cfg(test)]
mod tests {
    use crate::json::{json_set, jsonb_set};
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

    #[test]
    fn test_null_document_decides_result_before_arguments() {
        // SQLite returns SQL NULL as soon as it sees a SQL NULL document, so
        // the invalid path and the value that is not JSON below are never read.
        let args = [Value::Null, create_text("bad path"), create_text("nope")];
        let cache = JsonCacheCell::new();
        assert_eq!(json_set(&args, &cache).unwrap(), Value::Null);
        let cache = JsonCacheCell::new();
        assert_eq!(jsonb_set(&args, &cache).unwrap(), Value::Null);
        let cache = JsonCacheCell::new();
        assert_eq!(json_insert(&args, &cache).unwrap(), Value::Null);
        let cache = JsonCacheCell::new();
        assert_eq!(jsonb_insert(&args, &cache).unwrap(), Value::Null);
        let cache = JsonCacheCell::new();
        assert_eq!(json_replace(&args, &cache).unwrap(), Value::Null);
        let cache = JsonCacheCell::new();
        assert_eq!(jsonb_replace(&args, &cache).unwrap(), Value::Null);

        let paths = [Value::Null, create_text("bad path")];
        let cache = JsonCacheCell::new();
        assert_eq!(json_remove(&paths, &cache).unwrap(), Value::Null);
        let cache = JsonCacheCell::new();
        assert_eq!(jsonb_remove(&paths, &cache).unwrap(), Value::Null);
    }

    #[test]
    fn test_edited_null_document_is_still_a_document() {
        // A document that is JSON `null` was edited, so SQLite returns the text
        // `null` rather than SQL NULL. json_extract() has to map `null` to SQL
        // NULL, and the mutators used to share that conversion.
        for (name, result) in [
            (
                "json_set",
                json_set(
                    &[create_text("null"), create_text("$.a"), Value::from_i64(1)],
                    &JsonCacheCell::new(),
                )
                .unwrap(),
            ),
            (
                "json_insert",
                json_insert(
                    &[create_text("null"), create_text("$.a"), Value::from_i64(1)],
                    &JsonCacheCell::new(),
                )
                .unwrap(),
            ),
            (
                "json_replace",
                json_replace(
                    &[create_text("null"), create_text("$.a"), Value::from_i64(1)],
                    &JsonCacheCell::new(),
                )
                .unwrap(),
            ),
            (
                "json_remove",
                json_remove(&[create_text("null")], &JsonCacheCell::new()).unwrap(),
            ),
            (
                "json_set_root",
                json_set(
                    &[create_json("[1]"), create_text("$"), Value::Null],
                    &JsonCacheCell::new(),
                )
                .unwrap(),
            ),
        ] {
            assert_eq!(result, create_json("null"), "{name}");
        }

        // The `jsonb_` variants return the JSONB encoding of `null` instead.
        for (name, result) in [
            (
                "jsonb_set",
                jsonb_set(
                    &[create_text("null"), create_text("$.a"), Value::from_i64(1)],
                    &JsonCacheCell::new(),
                )
                .unwrap(),
            ),
            (
                "jsonb_insert",
                jsonb_insert(
                    &[create_text("null"), create_text("$.a"), Value::from_i64(1)],
                    &JsonCacheCell::new(),
                )
                .unwrap(),
            ),
            (
                "jsonb_replace",
                jsonb_replace(
                    &[create_text("null"), create_text("$.a"), Value::from_i64(1)],
                    &JsonCacheCell::new(),
                )
                .unwrap(),
            ),
            (
                "jsonb_remove",
                jsonb_remove(&[create_text("null")], &JsonCacheCell::new()).unwrap(),
            ),
            (
                "jsonb_set_root",
                jsonb_set(
                    &[create_json("[1]"), create_text("$"), Value::Null],
                    &JsonCacheCell::new(),
                )
                .unwrap(),
            ),
        ] {
            assert_eq!(
                result,
                Value::Blob(
                    crate::types::value_blob_from_slice(&[0x00]).expect("test blob allocation")
                ),
                "{name}"
            );
        }

        // Removing the whole document is the one case where SQLite does give
        // SQL NULL back, even though the edited document is a document.
        let cache = JsonCacheCell::new();
        assert_eq!(
            json_remove(&[create_json("[1]"), create_text("$")], &cache).unwrap(),
            Value::Null
        );
        let cache = JsonCacheCell::new();
        assert_eq!(
            jsonb_remove(&[create_json("[1]"), create_text("$")], &cache).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_null_path_drops_pair_without_reading_value() {
        // A PATH/VALUE pair whose path is SQL NULL is skipped, so the value is
        // never converted and a BLOB in it is not an error.
        for (name, result) in [
            (
                "json_set",
                json_set(
                    &[
                        create_json("[1]"),
                        Value::Null,
                        Value::Blob(
                            crate::types::value_blob_from_slice(&[0xff])
                                .expect("test blob allocation"),
                        ),
                    ],
                    &JsonCacheCell::new(),
                )
                .unwrap(),
            ),
            (
                "json_insert",
                json_insert(
                    &[
                        create_json("[1]"),
                        Value::Null,
                        Value::Blob(
                            crate::types::value_blob_from_slice(&[0xff])
                                .expect("test blob allocation"),
                        ),
                    ],
                    &JsonCacheCell::new(),
                )
                .unwrap(),
            ),
            (
                "json_replace",
                json_replace(
                    &[
                        create_json("[1]"),
                        Value::Null,
                        Value::Blob(
                            crate::types::value_blob_from_slice(&[0xff])
                                .expect("test blob allocation"),
                        ),
                    ],
                    &JsonCacheCell::new(),
                )
                .unwrap(),
            ),
        ] {
            assert_eq!(result, create_json("[1]"), "{name}");
        }

        // Only the pair with the NULL path is dropped, whichever side of it the
        // usable pair is on.
        let cache = JsonCacheCell::new();
        assert_eq!(
            json_set(
                &[
                    create_json("[1]"),
                    Value::Null,
                    Value::from_i64(2),
                    create_text("$[0]"),
                    Value::from_i64(3)
                ],
                &cache
            )
            .unwrap(),
            create_json("[3]")
        );

        // A value that is SQL NULL at a real path is still stored, so this is
        // JSON `null` in the document and not a dropped pair.
        let cache = JsonCacheCell::new();
        assert_eq!(
            json_set(
                &[create_json(r#"{"a":1}"#), create_text("$.b"), Value::Null],
                &cache
            )
            .unwrap(),
            create_json(r#"{"a":1,"b":null}"#)
        );
    }

    #[test]
    fn test_null_path_makes_json_remove_null() {
        // json_remove() treats a SQL NULL path the other way round: the whole
        // result becomes SQL NULL, whatever the other paths are.
        for args in [
            vec![create_json("[1]"), Value::Null],
            vec![create_json("[1]"), create_text("$"), Value::Null],
            vec![create_json("[1]"), Value::Null, create_text("$")],
            vec![create_json("[1]"), create_text("$[0]"), Value::Null],
        ] {
            let cache = JsonCacheCell::new();
            assert_eq!(json_remove(&args, &cache).unwrap(), Value::Null);
            let cache = JsonCacheCell::new();
            assert_eq!(jsonb_remove(&args, &cache).unwrap(), Value::Null);
        }
    }
}
