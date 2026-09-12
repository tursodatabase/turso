use super::*;
use std::str::FromStr;

// Helper function to create test Value and Jsonb from JSON string
fn create_test_pair(json_str: &str) -> (Value, Jsonb) {
    // Create Value as text representation of JSON
    let key = Value::build_text(json_str.to_string());

    // Create Jsonb from the same JSON string
    let value = Jsonb::from_str(json_str).unwrap();

    (key, value)
}

#[test]
fn test_json_cache_new() {
    let cache = JsonCache::new();
    assert_eq!(cache.used, 0);
    assert_eq!(cache.counter, 0);
    assert_eq!(cache.age, [0, 0, 0, 0]);
    assert!(cache.entries.iter().all(|entry| entry.is_none()));
}

#[test]
fn test_json_cache_insert_and_lookup() {
    let mut cache = JsonCache::new();
    let json_str = "{\"test\": \"value\"}";
    let (key, value) = create_test_pair(json_str);

    // Insert a value
    cache
        .insert(&key, &value)
        .expect(crate::alloc::ALLOC_ERR_MSG);

    // Verify it was inserted
    assert_eq!(cache.used, 1);
    assert_eq!(cache.counter, 1);

    // Look it up
    let result = cache.lookup(&key).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), value);

    // Counter should be incremented after lookup
    assert_eq!(cache.counter, 2);
}

#[test]
fn test_json_cache_lookup_nonexistent() {
    let mut cache = JsonCache::new();
    let (key, _) = create_test_pair("{\"id\": 123}");

    // Look up a non-existent key
    let result = cache.lookup(&key).unwrap();
    assert!(result.is_none());

    // Counter should remain unchanged
    assert_eq!(cache.counter, 0);
}

#[test]
fn test_json_cache_multiple_entries() {
    let mut cache = JsonCache::new();

    // Insert multiple entries
    let (key1, value1) = create_test_pair("{\"id\": 1}");
    let (key2, value2) = create_test_pair("{\"id\": 2}");
    let (key3, value3) = create_test_pair("{\"id\": 3}");

    cache
        .insert(&key1, &value1)
        .expect(crate::alloc::ALLOC_ERR_MSG);
    cache
        .insert(&key2, &value2)
        .expect(crate::alloc::ALLOC_ERR_MSG);
    cache
        .insert(&key3, &value3)
        .expect(crate::alloc::ALLOC_ERR_MSG);

    // Verify they were all inserted
    assert_eq!(cache.used, 3);
    assert_eq!(cache.counter, 3);

    // Look them up in reverse order
    let result3 = cache.lookup(&key3).unwrap();
    let result2 = cache.lookup(&key2).unwrap();
    let result1 = cache.lookup(&key1).unwrap();

    assert_eq!(result3.unwrap(), value3);
    assert_eq!(result2.unwrap(), value2);
    assert_eq!(result1.unwrap(), value1);

    // Counter should be incremented for each lookup
    assert_eq!(cache.counter, 6);
}

#[test]
fn test_json_cache_eviction() {
    let mut cache = JsonCache::new();

    // Insert more than JSON_CACHE_SIZE entries
    let (key1, value1) = create_test_pair("{\"id\": 1}");
    let (key2, value2) = create_test_pair("{\"id\": 2}");
    let (key3, value3) = create_test_pair("{\"id\": 3}");
    let (key4, value4) = create_test_pair("{\"id\": 4}");
    let (key5, value5) = create_test_pair("{\"id\": 5}");

    cache
        .insert(&key1, &value1)
        .expect(crate::alloc::ALLOC_ERR_MSG);
    cache
        .insert(&key2, &value2)
        .expect(crate::alloc::ALLOC_ERR_MSG);
    cache
        .insert(&key3, &value3)
        .expect(crate::alloc::ALLOC_ERR_MSG);
    cache
        .insert(&key4, &value4)
        .expect(crate::alloc::ALLOC_ERR_MSG);

    // Cache is now full
    assert_eq!(cache.used, 4);

    // Look up key1 to make it the most recently used
    let _ = cache.lookup(&key1).unwrap();

    // Insert one more entry - should evict the oldest (key2)
    cache
        .insert(&key5, &value5)
        .expect(crate::alloc::ALLOC_ERR_MSG);

    // Cache size should still be JSON_CACHE_SIZE
    assert_eq!(cache.used, 4);

    // key2 should have been evicted
    let result2 = cache.lookup(&key2).unwrap();
    assert!(result2.is_none());

    // Other entries should still be present
    assert!(cache.lookup(&key1).unwrap().is_some());
    assert!(cache.lookup(&key3).unwrap().is_some());
    assert!(cache.lookup(&key4).unwrap().is_some());
    assert!(cache.lookup(&key5).unwrap().is_some());
}

#[test]
fn test_json_cache_find_oldest_entry() {
    let mut cache = JsonCache::new();

    // Insert entries
    let (key1, value1) = create_test_pair("{\"id\": 1}");
    let (key2, value2) = create_test_pair("{\"id\": 2}");
    let (key3, value3) = create_test_pair("{\"id\": 3}");

    cache
        .insert(&key1, &value1)
        .expect(crate::alloc::ALLOC_ERR_MSG);
    cache
        .insert(&key2, &value2)
        .expect(crate::alloc::ALLOC_ERR_MSG);
    cache
        .insert(&key3, &value3)
        .expect(crate::alloc::ALLOC_ERR_MSG);

    // key1 should be the oldest
    assert_eq!(cache.find_oldest_entry(), 0);

    // Access key1 to make it the newest
    let _ = cache.lookup(&key1).unwrap();

    // Now key2 should be the oldest
    assert_eq!(cache.find_oldest_entry(), 1);
}

// Tests for JsonCacheCell

#[test]
fn test_json_cache_cell_new() {
    let cache_cell = JsonCacheCell::new();

    // Access flag should be false initially
    assert!(!cache_cell.accessed.get());

    // Inner cache should be None initially
    unsafe {
        let inner = &*cache_cell.inner.get();
        assert!(inner.is_none());
    }
}

#[test]
fn test_json_cache_cell_lookup() {
    let cache_cell = JsonCacheCell::new();
    let (key, value) = create_test_pair("{\"test\": \"value\"}");

    // First lookup should return None since cache is empty
    let result = cache_cell.lookup(&key);
    assert!(result.is_none());

    // Cache should be initialized after first lookup
    unsafe {
        let inner = &*cache_cell.inner.get();
        assert!(inner.is_some());
    }

    // Access flag should be reset to false
    assert!(!cache_cell.accessed.get());

    // Insert the value using get_or_insert_with
    let insert_result = cache_cell.get_or_insert_with(&key, |k| {
        // Verify that k is the same as our key
        assert_eq!(k, key);
        Ok(value.clone())
    });

    assert!(insert_result.is_ok());
    assert_eq!(insert_result.unwrap(), value);

    // Access flag should be reset to false
    assert!(!cache_cell.accessed.get());

    // Lookup should now return the value
    let lookup_result = cache_cell.lookup(&key);
    assert!(lookup_result.is_some());
    assert_eq!(lookup_result.unwrap(), value);
}

#[test]
fn test_json_cache_cell_get_or_insert_with_existing() {
    let cache_cell = JsonCacheCell::new();
    let (key, value) = create_test_pair("{\"test\": \"value\"}");

    // Insert a value
    let _ = cache_cell.get_or_insert_with(&key, |_| Ok(value.clone()));

    // Counter indicating if the closure was called
    let closure_called = Cell::new(false);

    // Try to insert again with the same key
    let result = cache_cell.get_or_insert_with(&key, |_| {
        closure_called.set(true);
        Ok(Jsonb::from_str("{\"test\": \"value\"}").unwrap())
    });

    // The closure should not have been called
    assert!(!closure_called.get());

    // Should return the original value
    assert_eq!(result.unwrap(), value);
}

#[test]
#[should_panic]
fn test_json_cache_cell_double_access() {
    let cache_cell = JsonCacheCell::new();
    let (key, _) = create_test_pair("{\"test\": \"value\"}");

    // Access the cache

    // Set accessed flag to true manually
    cache_cell.accessed.set(true);

    // This should panic due to double access

    let _ = cache_cell.lookup(&key);
}

#[test]
fn test_json_cache_cell_get_or_insert_error_handling() {
    let cache_cell = JsonCacheCell::new();
    let (key, _) = create_test_pair("{\"test\": \"value\"}");

    // Test error handling
    let error_result = cache_cell.get_or_insert_with(&key, |_| {
        // Return an error
        Err(crate::LimboError::Constraint("Test error".to_string()))
    });

    // Should propagate the error
    assert!(error_result.is_err());

    // Access flag should be reset to false
    assert!(!cache_cell.accessed.get());

    // The entry should not be cached
    let lookup_result = cache_cell.lookup(&key);
    assert!(lookup_result.is_none());
}
