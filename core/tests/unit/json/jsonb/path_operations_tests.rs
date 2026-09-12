use super::*;
use crate::json::path::{JsonPath, PathElement};
use std::borrow::Cow;

// Helper function to create a simple JsonPath
fn create_path(elements: Vec<PathElement>) -> JsonPath {
    JsonPath { elements }
}

#[test]
fn test_navigate_root_path() {
    let json_str = r#"{"name": "John", "age": 30}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    // Create a path to the root
    let path = create_path(vec![PathElement::Root()]);

    // Navigate to the path
    let result = jsonb.navigate_path(&path, PathOperationMode::ReplaceExisting);

    // Verify navigation succeeds
    assert!(result.is_ok());
    let stack = result.unwrap();
    assert_eq!(stack.len(), 1);
    assert_eq!(stack[0].field_value_index, 0);
    assert_eq!(stack[0].field_key_index, JsonLocationKind::DocumentRoot);
}

#[test]
fn test_navigate_object_property() {
    let json_str = r#"{"name": "John", "age": 30}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    // Create a path to the "name" property
    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("name"), false),
    ]);

    // Navigate to the path
    let result = jsonb.navigate_path(&path, PathOperationMode::ReplaceExisting);

    // Verify navigation succeeds and points to the correct value
    assert!(result.is_ok());
    let stack = result.unwrap();
    assert_eq!(stack.len(), 2);

    // Verify we can get the value at this position
    let name_index = stack[1].field_value_index;
    let (header, header_size) = jsonb.read_header(name_index).unwrap();
    assert_eq!(header.0, ElementType::TEXT);

    // Extract the actual string value to verify
    let text_bytes = &jsonb.data[name_index + header_size..name_index + header_size + header.1];
    let text = std::str::from_utf8(text_bytes).unwrap();
    assert_eq!(text, "John");
}

#[test]
fn test_navigate_nested_object_property() {
    let json_str = r#"{"person": {"name": "John", "age": 30}}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    // Create a path to the nested "name" property
    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("person"), false),
        PathElement::Key(Cow::Borrowed("name"), false),
    ]);

    // Navigate to the path
    let result = jsonb.navigate_path(&path, PathOperationMode::ReplaceExisting);

    // Verify navigation succeeds
    assert!(result.is_ok());
    let stack = result.unwrap();
    assert_eq!(stack.len(), 3);
}

#[test]
fn test_navigate_array_element() {
    let json_str = r#"{"items": [10, 20, 30]}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    // Create a path to the second array element (index 1)
    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("items"), false),
        PathElement::ArrayLocator(Some(1)),
    ]);

    // Navigate to the path
    let result = jsonb.navigate_path(&path, PathOperationMode::ReplaceExisting);

    // Verify navigation succeeds
    assert!(result.is_ok());
    let stack = result.unwrap();
    assert_eq!(stack.len(), 2);

    // Verify we can get the value at the array position
    assert!(stack[1].has_specific_index());
    let array_element_index = stack[1].get_array_index().unwrap();
    let (header, header_size) = jsonb.read_header(array_element_index).unwrap();
    assert_eq!(header.0, ElementType::INT);

    // Extract the actual integer value to verify
    let int_bytes = &jsonb.data
        [array_element_index + header_size..array_element_index + header_size + header.1];
    let int_str = std::str::from_utf8(int_bytes).unwrap();
    assert_eq!(int_str, "20");
}

#[test]
fn test_navigate_negative_array_index() {
    let json_str = r#"{"items": [10, 20, 30]}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    // Create a path to the last array element (index -1)
    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("items"), false),
        PathElement::ArrayLocator(Some(-1)),
    ]);

    // Navigate to the path
    let result = jsonb.navigate_path(&path, PathOperationMode::ReplaceExisting);

    // Verify navigation succeeds
    assert!(result.is_ok());
    let stack = result.unwrap();
    assert_eq!(stack.len(), 2);

    // Verify we can get the value at the array position
    assert!(stack[1].has_specific_index());
}

#[test]
fn test_set_operation() {
    let json_str = r#"{"name": "John", "age": 30}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    // Create a new value to set
    let new_value = Jsonb::from_str("\"Jane\"").unwrap();
    let mut operation = SetOperation::new(new_value);

    // Create a path to the "name" property
    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("name"), false),
    ]);

    // Execute the operation
    let result = jsonb.operate_on_path(&path, &mut operation);
    assert!(result.is_ok());

    // Verify the value was updated
    let updated_json = jsonb.to_string().unwrap();
    assert_eq!(updated_json, r#"{"name":"Jane","age":30}"#);
}

#[test]
fn test_insert_operation() {
    let json_str = r#"{"name": "John"}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    // Create a new value to insert
    let new_value = Jsonb::from_str("30").unwrap();
    let mut operation = InsertOperation::new(new_value);

    // Create a path to a new "age" property
    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("age"), false),
    ]);

    // Execute the operation
    let result = jsonb.operate_on_path(&path, &mut operation);
    assert!(result.is_ok());

    // Verify the value was inserted
    let updated_json = jsonb.to_string().unwrap();
    assert_eq!(updated_json, r#"{"name":"John","age":30}"#);
}

#[test]
fn test_delete_operation() {
    let json_str = r#"{"name": "John", "age": 30}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    // Create a delete operation
    let mut operation = DeleteOperation::new();

    // Create a path to the "age" property
    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("age"), false),
    ]);

    // Execute the operation
    let result = jsonb.operate_on_path(&path, &mut operation);
    assert!(result.is_ok());

    // Verify the property was deleted
    let updated_json = jsonb.to_string().unwrap();
    assert_eq!(updated_json, r#"{"name":"John"}"#);
}

#[test]
fn test_replace_operation() {
    let json_str = r#"{"items": [10, 20, 30]}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    // Create a new value to replace with
    let new_value = Jsonb::from_str("50").unwrap();
    let mut operation = ReplaceOperation::new(new_value);

    // Create a path to the second array element (index 1)
    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("items"), false),
        PathElement::ArrayLocator(Some(1)),
    ]);

    // Execute the operation
    let result = jsonb.operate_on_path(&path, &mut operation);
    assert!(result.is_ok());

    // Verify the value was replaced
    let updated_json = jsonb.to_string().unwrap();
    assert_eq!(updated_json, r#"{"items":[10,50,30]}"#);
}

#[test]
fn test_search_operation() {
    let json_str = r#"{"person": {"name": "John", "age": 30}}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    // Create a search operation
    let mut operation = SearchOperation::new(100).unwrap();

    // Create a path to the "person" property
    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("person"), false),
    ]);

    // Execute the operation
    let result = jsonb.operate_on_path(&path, &mut operation);
    assert!(result.is_ok());

    // Get the search result
    let search_result = operation.result();
    let result_str = search_result.to_string().unwrap();

    // Verify the search found the correct value
    assert_eq!(result_str, r#"{"name":"John","age":30}"#);
}

#[test]
fn test_error_for_nonexistent_path() {
    let json_str = r#"{"name": "John", "age": 30}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    // Create a new value to set
    let new_value = Jsonb::from_str("\"Doe\"").unwrap();
    let mut operation = ReplaceOperation::new(new_value);

    // Create a path to a non-existent property with ReplaceExisting mode
    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("surname"), false),
    ]);

    // Execute the operation - should fail because path doesn't exist
    let result = jsonb.operate_on_path(&path, &mut operation);
    assert!(result.is_err());
}

#[test]
fn test_deep_nested_path() {
    let json_str = r#"{"level1": {"level2": {"level3": {"value": 42}}}}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    // Create a new value to set
    let new_value = Jsonb::from_str("100").unwrap();
    let mut operation = SetOperation::new(new_value);

    // Create a deeply nested path
    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("level1"), false),
        PathElement::Key(Cow::Borrowed("level2"), false),
        PathElement::Key(Cow::Borrowed("level3"), false),
        PathElement::Key(Cow::Borrowed("value"), false),
    ]);

    // Execute the operation
    let result = jsonb.operate_on_path(&path, &mut operation);
    assert!(result.is_ok());

    // Verify the deep value was updated
    let updated_json = jsonb.to_string().unwrap();
    assert_eq!(
        updated_json,
        r#"{"level1":{"level2":{"level3":{"value":100}}}}"#
    );
}

#[test]
fn test_path_modes() {
    // Test the different path operation modes

    // 1. ReplaceExisting mode - should fail when path doesn't exist
    let json_str = r#"{"name": "John"}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    let mut operation = SetOperation::new(Jsonb::from_str("30").unwrap());
    operation.mode = PathOperationMode::ReplaceExisting;

    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("age"), false),
    ]);

    let result = jsonb.operate_on_path(&path, &mut operation);
    assert!(result.is_err());

    // 2. InsertNew mode - should succeed for new paths
    let json_str = r#"{"name": "John"}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    let mut operation = InsertOperation::new(Jsonb::from_str("30").unwrap());
    operation.mode = PathOperationMode::InsertNew;

    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("age"), false),
    ]);

    let result = jsonb.operate_on_path(&path, &mut operation);
    assert!(result.is_ok());

    let updated_json = jsonb.to_string().unwrap();
    assert_eq!(updated_json, r#"{"name":"John","age":30}"#);

    // 3. InsertNew mode - should fail when path already exists
    let mut operation = InsertOperation::new(Jsonb::from_str("31").unwrap());
    operation.mode = PathOperationMode::InsertNew;

    let result = jsonb.operate_on_path(&path, &mut operation);
    assert!(result.is_err());

    // 4. Upsert mode - should work for both existing and new paths
    let json_str = r#"{"name": "John", "age": 30}"#;
    let mut jsonb = Jsonb::from_str(json_str).unwrap();

    // Update existing value with Upsert
    let mut operation = SetOperation::new(Jsonb::from_str("31").unwrap());
    operation.mode = PathOperationMode::Upsert;

    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("age"), false),
    ]);

    let result = jsonb.operate_on_path(&path, &mut operation);
    assert!(result.is_ok());

    // Insert new value with Upsert
    let mut operation = SetOperation::new(Jsonb::from_str("\"Doe\"").unwrap());
    operation.mode = PathOperationMode::Upsert;

    let path = create_path(vec![
        PathElement::Root(),
        PathElement::Key(Cow::Borrowed("surname"), false),
    ]);

    let result = jsonb.operate_on_path(&path, &mut operation);
    assert!(result.is_ok());

    let updated_json = jsonb.to_string().unwrap();
    assert_eq!(updated_json, r#"{"name":"John","age":31,"surname":"Doe"}"#);
}

#[test]
fn test_array_len_malformed_overflow() {
    // Test that malformed JSONB with huge payload size doesn't panic.
    // This blob has an 8-byte payload size header (header_size = 15) with
    // a value that would cause overflow when added to the position.
    // Header byte: 0xFB = element type ARRAY (11) + size marker 15 (8-byte size)
    // Followed by 8 bytes of near-max u64 value.
    let malformed: ValueBlob = crate::alloc::vec![
        0xFB, // ARRAY type (11) with 8-byte payload size marker (15 << 4)
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F, // huge payload size
    ];
    let jsonb = Jsonb { data: malformed };

    // Should return an error instead of panicking with overflow
    let result = jsonb.array_len();
    assert!(result.is_err());
}

/// A child element whose declared size runs past the buffer must not reach
/// the slice in `SearchOperation` or the `drain` in `DeleteOperation`.
///
/// Blob-sourced documents are validated on the way in, so no SQL input
/// reaches these ranges today. The checks exist because the sizes are
/// caller-controlled and a range derived from them must not depend on a
/// validation pass having run somewhere else: that coupling is what made
/// the sibling `from_utf8_unchecked` defect undefined behaviour rather than
/// a clean error.
#[test]
fn malformed_element_size_is_rejected_before_slicing() {
    // ARRAY (type 11, inline payload size 8) whose single child is a TEXT5
    // with a 1-byte size marker declaring 200 bytes, in a 9-byte buffer.
    let bytes = [0x8Bu8, 0xC7, 0xC8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    let path = create_path(vec![
        PathElement::Root(),
        PathElement::ArrayLocator(Some(0)),
    ]);

    let mut jsonb = Jsonb {
        data: crate::alloc::vec![0x8B, 0xC7, 0xC8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
    };
    let mut search = SearchOperation::new(bytes.len()).unwrap();
    assert!(
        jsonb.operate_on_path(&path, &mut search).is_err(),
        "oversized child size must not be sliced out of bounds"
    );

    let mut jsonb = Jsonb {
        data: crate::alloc::vec![0x8B, 0xC7, 0xC8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
    };
    let mut delete = DeleteOperation::new();
    assert!(
        jsonb.operate_on_path(&path, &mut delete).is_err(),
        "oversized child size must not be drained out of bounds"
    );
}

#[test]
fn element_end_rejects_overflow_and_out_of_range() {
    let jsonb = Jsonb {
        data: crate::alloc::vec![0x0C, 0x00],
    };

    // Sum overflows usize (the 8-byte size marker can declare this).
    assert!(jsonb.element_end(1, &[usize::MAX, 2]).is_err());
    // Sum is representable but past the 2-byte buffer.
    assert!(jsonb.element_end(0, &[3]).is_err());
    // Exactly the end of the buffer is in range.
    assert!(jsonb.element_end(0, &[2]).is_ok());
}
