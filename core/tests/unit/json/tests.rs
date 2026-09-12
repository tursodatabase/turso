use super::*;
use crate::numeric::Numeric;
use crate::types::Value;

#[test]
fn json_valid_bad_flags_are_a_plain_sql_error_not_a_constraint() {
    // SQLite raises the FLAGS error through sqlite3_result_error,
    // which is error class SQLITE_ERROR. The C bindings map
    // LimboError::Constraint to SQLITE_CONSTRAINT, so the variant
    // matters to C API users, not just the message.
    for flags in [Value::from_i64(0), Value::from_i64(16), Value::Null] {
        let err = is_json_valid(Value::build_text("{}"), &flags).unwrap_err();
        assert!(matches!(err, LimboError::SqlError(_)), "{err:?}");
    }
}

#[test]
fn test_jsonb_preserves_malformed_json_error_and_cache_reusability() {
    let cache = JsonCacheCell::new();
    let invalid = Value::build_text("{");

    assert!(matches!(
        jsonb(&invalid, &cache),
        Err(LimboError::ParseError(message)) if message == "malformed JSON"
    ));

    let valid = Value::build_text(r#"{"key":"value"}"#);
    assert!(jsonb(&valid, &cache).is_ok());
}

#[test]
fn test_get_json_valid_json5() {
    let input = Value::build_text("{ key: 'value' }");
    let result = get_json(&input, None).unwrap();
    if let Value::Text(result_str) = result {
        assert!(result_str.as_str().contains("\"key\":\"value\""));
        assert_eq!(result_str.subtype, TextSubtype::Json);
    } else {
        panic!("Expected Value::Text");
    }
}

#[test]
fn test_get_json_valid_json5_infinity() {
    let input = Value::build_text("{ \"key\": Infinity }");
    let result = get_json(&input, None).unwrap();
    if let Value::Text(result_str) = result {
        assert!(result_str.as_str().contains("{\"key\":9e999}"));
        assert_eq!(result_str.subtype, TextSubtype::Json);
    } else {
        panic!("Expected Value::Text");
    }
}

#[test]
fn test_get_json_valid_json5_negative_infinity() {
    let input = Value::build_text("{ \"key\": -Infinity }");
    let result = get_json(&input, None).unwrap();
    if let Value::Text(result_str) = result {
        assert!(result_str.as_str().contains("{\"key\":-9e999}"));
        assert_eq!(result_str.subtype, TextSubtype::Json);
    } else {
        panic!("Expected Value::Text");
    }
}

#[test]
fn test_get_json_valid_json5_nan() {
    let input = Value::build_text("{ \"key\": NaN }");
    let result = get_json(&input, None).unwrap();
    if let Value::Text(result_str) = result {
        assert!(result_str.as_str().contains("{\"key\":null}"));
        assert_eq!(result_str.subtype, TextSubtype::Json);
    } else {
        panic!("Expected Value::Text");
    }
}

#[test]
fn test_get_json_invalid_json5() {
    let input = Value::build_text("{ key: value }");
    let result = get_json(&input, None);
    match result {
        Ok(_) => panic!("Expected error for malformed JSON"),
        Err(e) => assert!(e.to_string().contains("malformed JSON")),
    }
}

#[test]
fn test_get_json_valid_jsonb() {
    let input = Value::build_text("{\"key\":\"value\"}");
    let result = get_json(&input, None).unwrap();
    if let Value::Text(result_str) = result {
        assert!(result_str.as_str().contains("\"key\":\"value\""));
        assert_eq!(result_str.subtype, TextSubtype::Json);
    } else {
        panic!("Expected Value::Text");
    }
}

#[test]
fn test_get_json_invalid_jsonb() {
    let input = Value::build_text("{key:\"value\"");
    let result = get_json(&input, None);
    match result {
        Ok(_) => panic!("Expected error for malformed JSON"),
        Err(e) => assert!(e.to_string().contains("malformed JSON")),
    }
}

#[test]
fn test_get_json_blob_valid_jsonb() {
    let binary_json = crate::alloc::vec![124, 55, 104, 101, 121, 39, 121, 111];
    let input = Value::Blob(binary_json);
    let result = get_json(&input, None).unwrap();
    if let Value::Text(result_str) = result {
        assert!(result_str.as_str().contains(r#"{"hey":"yo"}"#));
        assert_eq!(result_str.subtype, TextSubtype::Json);
    } else {
        panic!("Expected Value::Text");
    }
}

#[test]
fn test_get_json_blob_invalid_jsonb() {
    let binary_json: crate::ValueBlob = crate::alloc::vec![0xA2, 0x62, 0x6B, 0x31, 0x62, 0x76]; // Incomplete binary JSON
    let input = Value::Blob(binary_json);
    let result = get_json(&input, None);
    println!("{result:?}");
    match result {
        Ok(_) => panic!("Expected error for malformed JSON"),
        Err(e) => assert!(e.to_string().contains("malformed JSON")),
    }
}

#[test]
fn test_get_json_non_text() {
    let input = Value::Null;
    let result = get_json(&input, None).unwrap();
    if let Value::Null = result {
        // Test passed
    } else {
        panic!("Expected Value::Null");
    }
}

#[test]
fn test_json_array_simple() {
    let text = Value::build_text("value1");
    let json = Value::Text(Text::json("\"value2\"".to_string()));
    let input = [text, json, Value::from_i64(1), Value::from_f64(1.1)];

    let result = json_array(&input).unwrap();
    if let Value::Text(res) = result {
        assert_eq!(res.as_str(), "[\"value1\",\"value2\",1,1.1]");
        assert_eq!(res.subtype, TextSubtype::Json);
    } else {
        panic!("Expected Value::Text");
    }
}

#[test]
fn test_json_array_with_infinity() {
    let infinity = Value::from_f64(f64::INFINITY);
    let neg_infinity = Value::from_f64(f64::NEG_INFINITY);
    let input = [Value::from_i64(1), infinity, neg_infinity];

    let result = json_array(&input).unwrap();
    if let Value::Text(res) = result {
        assert_eq!(res.as_str(), "[1,9.0e+999,-9.0e+999]");
        assert_eq!(res.subtype, TextSubtype::Json);
    } else {
        panic!("Expected Value::Text");
    }
}

#[test]
fn test_json_object_with_infinity() {
    let infinity = Value::from_f64(f64::INFINITY);
    let key = Value::build_text("k");
    let input = [key, infinity];

    let result = json_object(&input).unwrap();
    if let Value::Text(res) = result {
        assert_eq!(res.as_str(), r#"{"k":9.0e+999}"#);
        assert_eq!(res.subtype, TextSubtype::Json);
    } else {
        panic!("Expected Value::Text");
    }
}

#[test]
fn test_json_object_with_negative_infinity() {
    let neg_infinity = Value::from_f64(f64::NEG_INFINITY);
    let key = Value::build_text("k");
    let input = [key, neg_infinity];

    let result = json_object(&input).unwrap();
    if let Value::Text(res) = result {
        assert_eq!(res.as_str(), r#"{"k":-9.0e+999}"#);
        assert_eq!(res.subtype, TextSubtype::Json);
    } else {
        panic!("Expected Value::Text");
    }
}

#[test]
fn test_json_with_infinity() {
    let infinity = Value::from_f64(f64::INFINITY);
    let result = get_json(&infinity, None).unwrap();
    if let Value::Text(res) = result {
        assert_eq!(res.as_str(), "9e999");
        assert_eq!(res.subtype, TextSubtype::Json);
    } else {
        panic!("Expected Value::Text, got {result:?}");
    }
}

#[test]
fn test_json_with_negative_infinity() {
    let neg_infinity = Value::from_f64(f64::NEG_INFINITY);
    let result = get_json(&neg_infinity, None).unwrap();
    if let Value::Text(res) = result {
        assert_eq!(res.as_str(), "-9e999");
        assert_eq!(res.subtype, TextSubtype::Json);
    } else {
        panic!("Expected Value::Text, got {result:?}");
    }
}

#[test]
fn test_json_array_empty() {
    let input: [Value; 0] = [];

    let result = json_array(input).unwrap();
    if let Value::Text(res) = result {
        assert_eq!(res.as_str(), "[]");
        assert_eq!(res.subtype, TextSubtype::Json);
    } else {
        panic!("Expected Value::Text");
    }
}

#[test]
fn test_json_array_blob_invalid() {
    let blob = Value::from_slice(b"1").expect(crate::alloc::ALLOC_ERR_MSG);

    let input = [blob];

    let result = json_array(&input);

    match result {
        Ok(_) => panic!("Expected error for blob input"),
        Err(e) => assert!(e.to_string().contains("JSON cannot hold BLOB values")),
    }
}

#[test]
fn test_json_array_length() {
    let input = Value::build_text("[1,2,3,4]");
    let json_cache = JsonCacheCell::new();
    let result = json_array_length(&input, None, &json_cache).unwrap();
    if let Value::Numeric(Numeric::Integer(res)) = result {
        assert_eq!(res, 4);
    } else {
        panic!("Expected Value::Numeric(Numeric::Integer)");
    }
}

#[test]
fn test_json_array_length_null() {
    let input = Value::Null;
    let json_cache = JsonCacheCell::new();
    let result = json_array_length(&input, None, &json_cache).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn test_json_array_length_empty() {
    let input = Value::build_text("[]");
    let json_cache = JsonCacheCell::new();
    let result = json_array_length(&input, None, &json_cache).unwrap();
    if let Value::Numeric(Numeric::Integer(res)) = result {
        assert_eq!(res, 0);
    } else {
        panic!("Expected Value::Numeric(Numeric::Integer)");
    }
}

#[test]
fn test_json_array_length_root() {
    let input = Value::build_text("[1,2,3,4]");
    let json_cache = JsonCacheCell::new();
    let result = json_array_length(&input, Some(&Value::build_text("$")), &json_cache).unwrap();
    if let Value::Numeric(Numeric::Integer(res)) = result {
        assert_eq!(res, 4);
    } else {
        panic!("Expected Value::Numeric(Numeric::Integer)");
    }
}

#[test]
fn test_json_array_length_not_array() {
    let input = Value::build_text("{one: [1,2,3,4]}");
    let json_cache = JsonCacheCell::new();
    let result = json_array_length(&input, None, &json_cache).unwrap();
    if let Value::Numeric(Numeric::Integer(res)) = result {
        assert_eq!(res, 0);
    } else {
        panic!("Expected Value::Numeric(Numeric::Integer)");
    }
}

#[test]
fn test_json_array_length_via_prop() {
    let input = Value::build_text("{one: [1,2,3,4]}");
    let json_cache = JsonCacheCell::new();
    let result = json_array_length(&input, Some(&Value::build_text("$.one")), &json_cache).unwrap();
    if let Value::Numeric(Numeric::Integer(res)) = result {
        assert_eq!(res, 4);
    } else {
        panic!("Expected Value::Numeric(Numeric::Integer)");
    }
}

#[test]
fn test_json_array_length_via_index() {
    let input = Value::build_text("[[1,2,3,4]]");
    let json_cache = JsonCacheCell::new();
    let result = json_array_length(&input, Some(&Value::build_text("$[0]")), &json_cache).unwrap();
    if let Value::Numeric(Numeric::Integer(res)) = result {
        assert_eq!(res, 4);
    } else {
        panic!("Expected Value::Numeric(Numeric::Integer)");
    }
}

#[test]
fn test_json_array_length_via_index_not_array() {
    let input = Value::build_text("[1,2,3,4]");
    let json_cache = JsonCacheCell::new();
    let result = json_array_length(&input, Some(&Value::build_text("$[2]")), &json_cache).unwrap();
    if let Value::Numeric(Numeric::Integer(res)) = result {
        assert_eq!(res, 0);
    } else {
        panic!("Expected Value::Numeric(Numeric::Integer)");
    }
}

#[test]
fn test_json_array_length_via_index_bad_prop() {
    let input = Value::build_text("{one: [1,2,3,4]}");
    let json_cache = JsonCacheCell::new();
    let result = json_array_length(&input, Some(&Value::build_text("$.two")), &json_cache).unwrap();
    assert_eq!(Value::Null, result);
}

#[test]
fn test_json_array_length_simple_json_subtype() {
    let input = Value::build_text("[1,2,3]");
    let json_cache = JsonCacheCell::new();
    let wrapped = get_json(&input, None).unwrap();
    let result = json_array_length(&wrapped, None, &json_cache).unwrap();

    if let Value::Numeric(Numeric::Integer(res)) = result {
        assert_eq!(res, 3);
    } else {
        panic!("Expected Value::Numeric(Numeric::Integer)");
    }
}

#[test]
fn test_json_extract_missing_path() {
    let json_cache = JsonCacheCell::new();
    let result = json_extract(
        Value::build_text("{\"a\":2}"),
        &[Value::build_text("$.x")],
        &json_cache,
    );

    match result {
        Ok(Value::Null) => (),
        _ => panic!("Expected null result, got: {result:?}"),
    }
}
#[test]
fn test_json_extract_null_path() {
    let json_cache = JsonCacheCell::new();
    let result = json_extract(Value::build_text("{\"a\":2}"), &[Value::Null], &json_cache);

    match result {
        Ok(Value::Null) => (),
        _ => panic!("Expected null result, got: {result:?}"),
    }
}

#[test]
fn test_json_path_invalid() {
    let json_cache = JsonCacheCell::new();
    let result = json_extract(
        Value::build_text("{\"a\":2}"),
        &[Value::from_f64(1.1)],
        &json_cache,
    );

    match result {
        Ok(_) => panic!("expected error"),
        Err(e) => assert!(e.to_string().contains("JSON path error")),
    }
}

#[test]
fn test_json_error_position_no_error() {
    let input = Value::build_text("[1,2,3]");
    let result = json_error_position(&input).unwrap();
    assert_eq!(result, Value::from_i64(0));
}

#[test]
fn test_json_error_position_no_error_more() {
    let input = Value::build_text(r#"{"a":55,"b":72 , }"#);
    let result = json_error_position(&input).unwrap();
    assert_eq!(result, Value::from_i64(0));
}

#[test]
fn test_json_error_position_object() {
    let input = Value::build_text(r#"{"a":55,"b":72,,}"#);
    let result = json_error_position(&input).unwrap();
    assert_eq!(result, Value::from_i64(16));
}

#[test]
fn test_json_error_position_array() {
    let input = Value::build_text(r#"["a",55,"b",72,,]"#);
    let result = json_error_position(&input).unwrap();
    assert_eq!(result, Value::from_i64(16));
}

#[test]
fn test_json_error_position_null() {
    let input = Value::Null;
    let result = json_error_position(&input).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn test_json_error_position_integer() {
    let input = Value::from_i64(5);
    let result = json_error_position(&input).unwrap();
    assert_eq!(result, Value::from_i64(0));
}

#[test]
fn test_json_error_position_float() {
    let input = Value::from_f64(-5.5);
    let result = json_error_position(&input).unwrap();
    assert_eq!(result, Value::from_i64(0));
}

#[test]
fn test_json_object_simple() {
    let key = Value::build_text("key");
    let value = Value::build_text("value");
    let input = [key, value];

    let result = json_object(&input).unwrap();
    let Value::Text(json_text) = result else {
        panic!("Expected Value::Text");
    };
    assert_eq!(json_text.as_str(), r#"{"key":"value"}"#);
}

#[test]
fn test_json_object_multiple_values() {
    let text_key = Value::build_text("text_key");
    let text_value = Value::build_text("text_value");
    let json_key = Value::build_text("json_key");
    let json_value = Value::Text(Text::json(r#"{"json":"value","number":1}"#.to_string()));
    let integer_key = Value::build_text("integer_key");
    let integer_value = Value::from_i64(1);
    let float_key = Value::build_text("float_key");
    let float_value = Value::from_f64(1.1);
    let null_key = Value::build_text("null_key");
    let null_value = Value::Null;

    let input = [
        text_key,
        text_value,
        json_key,
        json_value,
        integer_key,
        integer_value,
        float_key,
        float_value,
        null_key,
        null_value,
    ];

    let result = json_object(&input).unwrap();
    let Value::Text(json_text) = result else {
        panic!("Expected Value::Text");
    };
    assert_eq!(
        json_text.as_str(),
        r#"{"text_key":"text_value","json_key":{"json":"value","number":1},"integer_key":1,"float_key":1.1,"null_key":null}"#
    );
}

#[test]
fn test_json_object_json_value_is_rendered_as_json() {
    let key = Value::build_text("key");
    let value = Value::Text(Text::json(r#"{"json":"value"}"#.to_string()));
    let input = [key, value];

    let result = json_object(&input).unwrap();
    let Value::Text(json_text) = result else {
        panic!("Expected Value::Text");
    };
    assert_eq!(json_text.as_str(), r#"{"key":{"json":"value"}}"#);
}

#[test]
fn test_json_object_json_text_value_is_rendered_as_regular_text() {
    let key = Value::build_text("key");
    let value = Value::Text(Text::new(r#"{"json":"value"}"#));
    let input = [key, value];

    let result = json_object(&input).unwrap();
    let Value::Text(json_text) = result else {
        panic!("Expected Value::Text");
    };
    assert_eq!(json_text.as_str(), r#"{"key":"{\"json\":\"value\"}"}"#);
}

#[test]
fn test_json_object_nested() {
    let key = Value::build_text("key");
    let value = Value::build_text("value");
    let input = [key, value];

    let parent_key = Value::build_text("parent_key");
    let parent_value = json_object(&input).unwrap();
    let parent_input = [parent_key, parent_value];

    let result = json_object(&parent_input).unwrap();

    let Value::Text(json_text) = result else {
        panic!("Expected Value::Text");
    };
    assert_eq!(json_text.as_str(), r#"{"parent_key":{"key":"value"}}"#);
}

#[test]
fn test_json_object_duplicated_keys() {
    let key = Value::build_text("key");
    let value = Value::build_text("value");
    let input = [key.clone(), value.clone(), key, value];

    let result = json_object(&input).unwrap();
    let Value::Text(json_text) = result else {
        panic!("Expected Value::Text");
    };
    assert_eq!(json_text.as_str(), r#"{"key":"value","key":"value"}"#);
}

#[test]
fn test_json_object_empty() {
    let input: [Value; 0] = [];

    let result = json_object(&input).unwrap();
    let Value::Text(json_text) = result else {
        panic!("Expected Value::Text");
    };
    assert_eq!(json_text.as_str(), r#"{}"#);
}

#[test]
fn test_json_object_non_text_key() {
    let key = Value::from_i64(1);
    let value = Value::build_text("value");
    let input = [key, value];

    match json_object(&input) {
        Ok(_) => panic!("Expected error for non-TEXT key"),
        Err(e) => assert!(e.to_string().contains("labels must be TEXT")),
    }
}

#[test]
fn test_json_odd_number_of_values() {
    let key = Value::build_text("key");
    let value = Value::build_text("value");
    let input = [key.clone(), value, key];

    assert!(json_object(&input).is_err());
}

#[test]
fn test_json_object_escapes_special_characters() {
    let cases = [
        // (key, value, expected_json)
        ("key", r"Hello\World", r#"{"key":"Hello\\World"}"#),
        (
            r"key\with\backslash",
            "value",
            r#"{"key\\with\\backslash":"value"}"#,
        ),
        ("key", "Hello\nWorld", r#"{"key":"Hello\nWorld"}"#),
        ("key", "Hello\tWorld", r#"{"key":"Hello\tWorld"}"#),
        ("key", "Hello\rWorld", r#"{"key":"Hello\rWorld"}"#),
        ("key", "Hello\x01World", r#"{"key":"Hello\u0001World"}"#),
        ("key", "Hello\x08\x0cWorld", r#"{"key":"Hello\b\fWorld"}"#),
        ("key", "ä\n", "{\"key\":\"ä\\n\"}"),
        ("key", "日本語\t", "{\"key\":\"日本語\\t\"}"),
    ];

    for (key, value, expected) in cases {
        let input = [Value::build_text(key), Value::build_text(value)];
        let result = json_object(&input).unwrap();
        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(
            json_text.as_str(),
            expected,
            "Failed for key={key:?}, value={value:?}"
        );
    }
}

#[test]
fn test_json_path_from_db_value_root_strict() {
    let path = Value::Text(Text::new("$"));

    let result = json_path_from_db_value(&path, true);
    assert!(result.is_ok());

    let result = result.unwrap();
    assert!(result.is_some());

    let result = result.unwrap();
    match result.elements[..] {
        [PathElement::Root()] => {}
        _ => panic!("Expected root"),
    }
}

#[test]
fn test_json_path_from_db_value_root_non_strict() {
    let path = Value::Text(Text::new("$"));

    let result = json_path_from_db_value(&path, false);
    assert!(result.is_ok());

    let result = result.unwrap();
    assert!(result.is_some());

    let result = result.unwrap();
    match result.elements[..] {
        [PathElement::Root()] => {}
        _ => panic!("Expected root"),
    }
}

#[test]
fn test_json_path_from_db_value_named_strict() {
    let path = Value::Text(Text::new("field"));

    assert!(json_path_from_db_value(&path, true).is_err());
}

#[test]
fn test_json_path_from_db_value_named_non_strict() {
    let path = Value::Text(Text::new("field"));

    let result = json_path_from_db_value(&path, false);
    assert!(result.is_ok());

    let result = result.unwrap();
    assert!(result.is_some());

    let result = result.unwrap();
    match &result.elements[..] {
        [PathElement::Root(), PathElement::Key(field, true)] if *field == "field" => {}
        _ => panic!("Expected root and field"),
    }
}

#[test]
fn test_json_path_from_db_value_integer_strict() {
    let path = Value::from_i64(3);
    assert!(json_path_from_db_value(&path, true).is_err());
}

#[test]
fn test_json_path_from_db_value_integer_non_strict() {
    let path = Value::from_i64(3);

    let result = json_path_from_db_value(&path, false);
    assert!(result.is_ok());

    let result = result.unwrap();
    assert!(result.is_some());

    let result = result.unwrap();
    match &result.elements[..] {
        [PathElement::Root(), PathElement::ArrayLocator(index)] if *index == Some(3) => {}
        _ => panic!("Expected root and array locator"),
    }
}

#[test]
fn test_json_path_from_db_value_null_strict() {
    let path = Value::Null;

    let result = json_path_from_db_value(&path, true);
    assert!(result.is_ok());

    let result = result.unwrap();
    assert!(result.is_none());
}

#[test]
fn test_json_path_from_db_value_null_non_strict() {
    let path = Value::Null;

    let result = json_path_from_db_value(&path, false);
    assert!(result.is_ok());

    let result = result.unwrap();
    assert!(result.is_none());
}

#[test]
fn test_json_path_from_db_value_float_strict() {
    let path = Value::from_f64(1.23);

    assert!(json_path_from_db_value(&path, true).is_err());
}

#[test]
fn test_json_path_from_db_value_float_non_strict() {
    let path = Value::from_f64(1.23);

    let result = json_path_from_db_value(&path, false);
    assert!(result.is_ok());

    let result = result.unwrap();
    assert!(result.is_some());

    let result = result.unwrap();
    match &result.elements[..] {
        [PathElement::Root(), PathElement::Key(field, false)] if *field == "1.23" => {}
        _ => panic!("Expected root and field"),
    }
}

#[test]
fn test_json_set_field_empty_object() {
    let json_cache = JsonCacheCell::new();
    let result = json_set(
        &[
            Value::build_text("{}"),
            Value::build_text("$.field"),
            Value::build_text("value"),
        ],
        &json_cache,
    );

    assert!(result.is_ok());

    assert_eq!(result.unwrap().to_text().unwrap(), r#"{"field":"value"}"#);
}

#[test]
fn test_json_set_replace_field() {
    let json_cache = JsonCacheCell::new();
    let result = json_set(
        &[
            Value::build_text(r#"{"field":"old_value"}"#),
            Value::build_text("$.field"),
            Value::build_text("new_value"),
        ],
        &json_cache,
    );

    assert!(result.is_ok());

    assert_eq!(
        result.unwrap().to_text().unwrap(),
        r#"{"field":"new_value"}"#
    );
}

#[test]
fn test_json_set_set_deeply_nested_key() {
    let json_cache = JsonCacheCell::new();
    let result = json_set(
        &[
            Value::build_text("{}"),
            Value::build_text("$.object.doesnt.exist"),
            Value::build_text("value"),
        ],
        &json_cache,
    );

    assert!(result.is_ok());

    assert_eq!(
        result.unwrap().to_text().unwrap(),
        r#"{"object":{"doesnt":{"exist":"value"}}}"#
    );
}

#[test]
fn test_json_set_add_value_to_empty_array() {
    let json_cache = JsonCacheCell::new();
    let result = json_set(
        &[
            Value::build_text("[]"),
            Value::build_text("$[0]"),
            Value::build_text("value"),
        ],
        &json_cache,
    );

    assert!(result.is_ok());

    assert_eq!(result.unwrap().to_text().unwrap(), r#"["value"]"#);
}

#[test]
fn test_json_set_add_value_to_nonexistent_array() {
    let json_cache = JsonCacheCell::new();
    let result = json_set(
        &[
            Value::build_text("{}"),
            Value::build_text("$.some_array[0]"),
            Value::from_i64(123),
        ],
        &json_cache,
    );

    assert!(result.is_ok());

    assert_eq!(
        result.unwrap().to_text().unwrap(),
        r#"{"some_array":[123]}"#
    );
}

#[test]
fn test_json_set_add_value_to_array() {
    let json_cache = JsonCacheCell::new();
    let result = json_set(
        &[
            Value::build_text("[123]"),
            Value::build_text("$[1]"),
            Value::from_i64(456),
        ],
        &json_cache,
    );

    assert!(result.is_ok());

    assert_eq!(result.unwrap().to_text().unwrap(), "[123,456]");
}

#[test]
fn test_json_set_add_value_to_array_out_of_bounds() {
    let json_cache = JsonCacheCell::new();
    let result = json_set(
        &[
            Value::build_text("[123]"),
            Value::build_text("$[200]"),
            Value::from_i64(456),
        ],
        &json_cache,
    );

    assert!(result.is_ok());

    assert_eq!(result.unwrap().to_text().unwrap(), "[123]");
}

#[test]
fn test_json_set_replace_value_in_array() {
    let json_cache = JsonCacheCell::new();
    let result = json_set(
        &[
            Value::build_text("[123]"),
            Value::build_text("$[0]"),
            Value::from_i64(456),
        ],
        &json_cache,
    );

    assert!(result.is_ok());

    assert_eq!(result.unwrap().to_text().unwrap(), "[456]");
}

#[test]
fn test_json_set_null_path() {
    let json_cache = JsonCacheCell::new();
    let result = json_set(
        &[Value::build_text("{}"), Value::Null, Value::from_i64(456)],
        &json_cache,
    );

    assert!(result.is_ok());

    assert_eq!(result.unwrap().to_text().unwrap(), "{}");
}

#[test]
fn test_json_set_multiple_keys() {
    let json_cache = JsonCacheCell::new();
    let result = json_set(
        &[
            Value::build_text("[123]"),
            Value::build_text("$[0]"),
            Value::from_i64(456),
            Value::build_text("$[1]"),
            Value::from_i64(789),
        ],
        &json_cache,
    );

    assert!(result.is_ok());

    assert_eq!(result.unwrap().to_text().unwrap(), "[456,789]");
}

#[test]
fn test_json_set_add_array_in_nested_object() {
    let json_cache = JsonCacheCell::new();
    let result = json_set(
        &[
            Value::build_text("{}"),
            Value::build_text("$.object[0].field"),
            Value::from_i64(123),
        ],
        &json_cache,
    );

    assert!(result.is_ok());

    assert_eq!(
        result.unwrap().to_text().unwrap(),
        r#"{"object":[{"field":123}]}"#
    );
}

#[test]
fn test_json_set_add_array_in_array_in_nested_object() {
    let json_cache = JsonCacheCell::new();
    let result = json_set(
        &[
            Value::build_text("{}"),
            Value::build_text("$.object[0][0]"),
            Value::from_i64(123),
        ],
        &json_cache,
    );

    assert!(result.is_ok());

    assert_eq!(result.unwrap().to_text().unwrap(), r#"{"object":[[123]]}"#);
}

#[test]
fn test_json_set_add_array_in_array_in_nested_object_out_of_bounds() {
    let json_cache = JsonCacheCell::new();
    let result = json_set(
        &[
            Value::build_text("{}"),
            Value::build_text("$.object[123].another"),
            Value::build_text("value"),
            Value::build_text("$.field"),
            Value::build_text("value"),
        ],
        &json_cache,
    );

    assert!(result.is_ok());

    assert_eq!(result.unwrap().to_text().unwrap(), r#"{"field":"value"}"#,);
}

#[test]
fn test_is_jsonb_blob_rejects_scalar_like_overlap_header() {
    // `|` is 0x7C: OBJECT with a 7-byte inline payload, so this is exactly at
    // the length where a scalar blob and a JSONB object are indistinguishable
    // by header alone.
    let overlapping_scalar = b"|1234567";
    assert_eq!(overlapping_scalar.len(), 8);
    assert!(!is_jsonb_blob(overlapping_scalar));
}

/// Object with a payload larger than a scalar blob, so header inspection
/// alone accepts it, but the TEXT5 key holds bytes that are not UTF-8.
/// Reading such a key used to reach `str::from_utf8_unchecked`.
#[test]
fn test_is_jsonb_blob_rejects_invalid_utf8_key() {
    let invalid_utf8_key = b"\x9C\x79aaaaaa\xF0\x00";
    assert!(!is_jsonb_blob(invalid_utf8_key));
}
