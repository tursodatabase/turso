use std::cell::{Cell, UnsafeCell};

use crate::Value;

use super::jsonb::Jsonb;

const JSON_CACHE_SIZE: usize = 4;

#[derive(Debug)]
pub struct JsonCache {
    entries: [Option<(Value, Jsonb)>; JSON_CACHE_SIZE],
    age: [usize; JSON_CACHE_SIZE],
    used: usize,
    counter: usize,
}

impl JsonCache {
    pub fn new() -> Self {
        Self {
            entries: [None, None, None, None],
            age: [0, 0, 0, 0],
            used: 0,
            counter: 0,
        }
    }

    fn find_oldest_entry(&self) -> usize {
        let mut oldest_idx = 0;
        let mut oldest_age = self.age[0];

        for i in 1..self.used {
            if self.age[i] < oldest_age {
                oldest_idx = i;
                oldest_age = self.age[i];
            }
        }

        oldest_idx
    }

    pub fn insert(&mut self, key: &Value, value: &Jsonb) {
        if self.used < JSON_CACHE_SIZE {
            self.entries[self.used] = Some((key.clone(), value.clone()));
            self.age[self.used] = self.counter;
            self.counter += 1;
            self.used += 1
        } else {
            let id = self.find_oldest_entry();

            self.entries[id] = Some((key.clone(), value.clone()));
            self.age[id] = self.counter;
            self.counter += 1;
        }
    }

    pub fn lookup(&mut self, key: &Value) -> Option<Jsonb> {
        for i in (0..self.used).rev() {
            if let Some((stored_key, value)) = &self.entries[i] {
                if key == stored_key {
                    self.age[i] = self.counter;
                    self.counter += 1;
                    let json = value.clone();

                    return Some(json);
                }
            }
        }
        None
    }

    pub fn clear(&mut self) {
        self.counter = 0;
        self.used = 0;
    }
}

#[derive(Debug)]
pub struct JsonCacheCell {
    inner: UnsafeCell<Option<JsonCache>>,
    accessed: Cell<bool>,
}

impl JsonCacheCell {
    pub fn new() -> Self {
        Self {
            inner: UnsafeCell::new(None),
            accessed: Cell::new(false),
        }
    }

    #[cfg(test)]
    pub fn lookup(&self, key: &Value) -> Option<Jsonb> {
        assert!(!self.accessed.get());

        self.accessed.set(true);

        let result = unsafe {
            let cache_ptr = self.inner.get();
            if (*cache_ptr).is_none() {
                *cache_ptr = Some(JsonCache::new());
            }

            if let Some(cache) = &mut (*cache_ptr) {
                cache.lookup(key)
            } else {
                None
            }
        };

        self.accessed.set(false);
        result
    }

    pub fn get_or_insert_with(
        &self,
        key: &Value,
        value: impl Fn(&Value) -> crate::Result<Jsonb>,
    ) -> crate::Result<Jsonb> {
        assert!(!self.accessed.get());

        self.accessed.set(true);
        let result = unsafe {
            let cache_ptr = self.inner.get();
            if (*cache_ptr).is_none() {
                *cache_ptr = Some(JsonCache::new());
            }

            if let Some(cache) = &mut (*cache_ptr) {
                if let Some(jsonb) = cache.lookup(key) {
                    Ok(jsonb)
                } else {
                    let result = value(key);
                    match result {
                        Ok(json) => {
                            cache.insert(key, &json);
                            Ok(json)
                        }
                        Err(e) => Err(e),
                    }
                }
            } else {
                value(key)
            }
        };
        self.accessed.set(false);

        result
    }

    pub fn clear(&mut self) {
        assert!(!self.accessed.get());
        self.accessed.set(true);
        unsafe {
            let cache_ptr = self.inner.get();
            if (*cache_ptr).is_none() {
                self.accessed.set(false);
                return;
            }

            if let Some(cache) = &mut (*cache_ptr) {
                cache.clear()
            }
        }
        self.accessed.set(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // Helper function to create test Value and Jsonb from JSON string
    fn create_test_pair(json_str: &str) -> (Value, Jsonb) {
        // Create Value as text representation of JSON
        let key = Value::build_text(json_str);

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
        cache.insert(&key, &value);

        // Verify it was inserted
        assert_eq!(cache.used, 1);
        assert_eq!(cache.counter, 1);

        // Look it up
        let result = cache.lookup(&key);
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
        let result = cache.lookup(&key);
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

        cache.insert(&key1, &value1);
        cache.insert(&key2, &value2);
        cache.insert(&key3, &value3);

        // Verify they were all inserted
        assert_eq!(cache.used, 3);
        assert_eq!(cache.counter, 3);

        // Look them up in reverse order
        let result3 = cache.lookup(&key3);
        let result2 = cache.lookup(&key2);
        let result1 = cache.lookup(&key1);

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

        cache.insert(&key1, &value1);
        cache.insert(&key2, &value2);
        cache.insert(&key3, &value3);
        cache.insert(&key4, &value4);

        // Cache is now full
        assert_eq!(cache.used, 4);

        // Look up key1 to make it the most recently used
        let _ = cache.lookup(&key1);

        // Insert one more entry - should evict the oldest (key2)
        cache.insert(&key5, &value5);

        // Cache size should still be JSON_CACHE_SIZE
        assert_eq!(cache.used, 4);

        // key2 should have been evicted
        let result2 = cache.lookup(&key2);
        assert!(result2.is_none());

        // Other entries should still be present
        assert!(cache.lookup(&key1).is_some());
        assert!(cache.lookup(&key3).is_some());
        assert!(cache.lookup(&key4).is_some());
        assert!(cache.lookup(&key5).is_some());
    }

    #[test]
    fn test_json_cache_find_oldest_entry() {
        let mut cache = JsonCache::new();

        // Insert entries
        let (key1, value1) = create_test_pair("{\"id\": 1}");
        let (key2, value2) = create_test_pair("{\"id\": 2}");
        let (key3, value3) = create_test_pair("{\"id\": 3}");

        cache.insert(&key1, &value1);
        cache.insert(&key2, &value2);
        cache.insert(&key3, &value3);

        // key1 should be the oldest
        assert_eq!(cache.find_oldest_entry(), 0);

        // Access key1 to make it the newest
        let _ = cache.lookup(&key1);

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
            assert_eq!(k, &key);
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
}
