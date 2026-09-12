use super::*;

#[test]
fn test_parse_text_array_multibyte_utf8() {
    let input = r#"{"café","naïve","über"}"#;
    let result = parse_text_array(input).unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], Value::build_text("café"));
    assert_eq!(result[1], Value::build_text("naïve"));
    assert_eq!(result[2], Value::build_text("über"));
}

#[test]
fn test_parse_text_array_emoji() {
    let input = r#"{"hello 🌍","test 🚀"}"#;
    let result = parse_text_array(input).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], Value::build_text("hello 🌍"));
    assert_eq!(result[1], Value::build_text("test 🚀"));
}

#[test]
fn test_parse_text_array_cjk() {
    let input = r#"{"你好","世界"}"#;
    let result = parse_text_array(input).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], Value::build_text("你好"));
    assert_eq!(result[1], Value::build_text("世界"));
}

#[test]
fn test_compute_array_length_null_returns_none() {
    assert_eq!(compute_array_length(&Value::Null), None);
}

#[test]
fn test_compute_array_length_valid_array() {
    let blob = values_to_record_blob(&[Value::from_i64(1), Value::from_i64(2)]).unwrap();
    assert_eq!(compute_array_length(&blob), Some(2));
}

#[test]
fn test_compute_array_length_non_blob_returns_none() {
    assert_eq!(compute_array_length(&Value::from_i64(42)), None,);
}

#[test]
fn test_array_remove_all_occurrences() {
    let arr = values_to_record_blob(&[
        Value::from_i64(1),
        Value::from_i64(2),
        Value::from_i64(3),
        Value::from_i64(2),
        Value::from_i64(1),
    ])
    .unwrap();
    let result = exec_array_remove(&arr, &Value::from_i64(2)).unwrap();
    let Value::Blob(blob) = &result else {
        panic!("Expected Blob");
    };
    let elements = array_values_from_blob(blob).unwrap();
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[0], Value::from_i64(1));
    assert_eq!(elements[1], Value::from_i64(3));
    assert_eq!(elements[2], Value::from_i64(1));
}

#[test]
fn test_array_contains_null_array_returns_null() {
    assert_eq!(
        exec_array_contains(&Value::Null, &Value::from_i64(1)),
        Value::Null,
    );
}

#[test]
fn test_array_position_null_array_returns_null() {
    assert_eq!(
        exec_array_position(&Value::Null, &Value::from_i64(1)),
        Value::Null,
    );
}

#[test]
fn test_compute_array_length_invalid_blob_returns_none() {
    // A random blob that is not a valid record should return None
    let invalid = Value::from_slice(&[0xFF, 0xFE, 0xFD]).expect(crate::alloc::ALLOC_ERR_MSG);
    assert_eq!(compute_array_length(&invalid), None);
}

#[test]
fn test_parse_text_array_rejects_json_format() {
    // JSON [1,2,3] format is no longer accepted — only PG {1,2,3}
    assert!(parse_text_array("[1,2,3]").is_none());
    assert!(parse_text_array(r#"["hello"]"#).is_none());
}

#[test]
fn test_parse_text_array_rejects_trailing_comma() {
    assert!(parse_text_array("{1,2,}").is_none());
    assert!(parse_text_array("{1, 2, }").is_none());
}

#[test]
fn test_parse_text_array_rejects_infinity() {
    assert!(parse_text_array("{1e309}").is_none());
    assert!(parse_text_array("{-1e309}").is_none());
}

#[test]
fn test_string_to_array_null_delimiter_splits_chars() {
    let result = exec_string_to_array(&Value::build_text("hello"), &Value::Null, None).unwrap();
    let Value::Blob(blob) = &result else {
        panic!("Expected Blob, got {result:?}");
    };
    let elements = array_values_from_blob(blob).unwrap();
    assert_eq!(elements.len(), 5);
    assert_eq!(elements[0], Value::build_text("h"));
    assert_eq!(elements[1], Value::build_text("e"));
    assert_eq!(elements[4], Value::build_text("o"));
}

#[test]
fn test_exec_array_contains_streaming() {
    let arr = values_to_record_blob(&[
        Value::from_i64(10),
        Value::from_i64(20),
        Value::from_i64(30),
    ])
    .unwrap();
    assert_eq!(
        exec_array_contains(&arr, &Value::from_i64(20)),
        Value::from_i64(1)
    );
    assert_eq!(
        exec_array_contains(&arr, &Value::from_i64(99)),
        Value::from_i64(0)
    );
}

#[test]
fn test_exec_array_position_streaming() {
    let arr = values_to_record_blob(&[
        Value::from_i64(10),
        Value::from_i64(20),
        Value::from_i64(30),
    ])
    .unwrap();
    // 1-based: element 20 is at position 2
    assert_eq!(
        exec_array_position(&arr, &Value::from_i64(20)),
        Value::from_i64(2)
    );
    assert_eq!(exec_array_position(&arr, &Value::from_i64(99)), Value::Null);
}

#[test]
fn test_dc1_negative_index_preserves_array() {
    let arr = values_to_record_blob(&[
        Value::from_i64(10),
        Value::from_i64(20),
        Value::from_i64(30),
    ])
    .unwrap();
    // array_find_streaming with impossible predicate should return None
    let Value::Blob(blob) = &arr else {
        panic!("Expected Blob");
    };
    assert!(array_find_streaming(blob, |_| false).is_none());
}

#[test]
fn test_dc4_array_remove_null_returns_null() {
    assert_eq!(
        exec_array_remove(&Value::Null, &Value::from_i64(1)).unwrap(),
        Value::Null
    );
}

#[test]
fn test_dc4_array_slice_null_returns_null() {
    assert_eq!(
        exec_array_slice(&Value::Null, &Value::from_i64(0), &Value::from_i64(2)).unwrap(),
        Value::Null,
    );
}

#[test]
fn test_dc4_array_cat_null_returns_null() {
    assert_eq!(
        exec_array_cat(&Value::Null, &Value::Null).unwrap(),
        Value::Null
    );
    assert_eq!(
        exec_array_cat(&Value::Null, &Value::from_i64(1)).unwrap(),
        Value::Null,
    );
}

#[test]
fn test_serialize_array_from_blob() {
    let arr = values_to_record_blob(&[Value::from_i64(1), Value::build_text("hello"), Value::Null])
        .unwrap();
    let Value::Blob(blob) = &arr else {
        panic!("Expected Blob");
    };
    let text = serialize_array_from_blob(blob).unwrap();
    assert_eq!(text, "{1,hello,NULL}");
}

#[test]
fn test_make_array_from_registers() {
    use super::super::Register;
    let registers = vec![
        Register::Value(Value::from_i64(1)),
        Register::Value(Value::build_text("two")),
        Register::Value(Value::from_i64(3)),
    ];
    let result = make_array_from_registers(&registers, 0, 3).unwrap();
    let Value::Blob(blob) = &result else {
        panic!("Expected Blob");
    };
    let elements = array_values_from_blob(blob).unwrap();
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[0], Value::from_i64(1));
    assert_eq!(elements[1], Value::build_text("two"));
    assert_eq!(elements[2], Value::from_i64(3));
}
