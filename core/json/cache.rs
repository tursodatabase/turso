use std::borrow::Cow;
use std::cell::{Cell, RefCell, UnsafeCell};

use crate::alloc::TryClone;
use crate::types::{AsValueRef, Text};
use crate::{Value, ValueRef};

use super::json_path_from_db_value;
use super::jsonb::Jsonb;
use super::path::JsonPath;

const JSON_CACHE_SIZE: usize = 4;
const PATH_CACHE_SIZE: usize = 4;

#[derive(Debug)]
pub struct JsonCache {
    entries: [Option<(Value, Jsonb)>; JSON_CACHE_SIZE],
    key_lens: [usize; JSON_CACHE_SIZE],
    age: [usize; JSON_CACHE_SIZE],
    used: usize,
    counter: usize,
}

impl JsonCache {
    pub fn new() -> Self {
        Self {
            entries: [None, None, None, None],
            key_lens: [0; JSON_CACHE_SIZE],
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

    #[cfg(test)]
    pub fn insert(&mut self, key: impl AsValueRef, value: Jsonb) -> crate::Result<usize> {
        self.insert_parsed(key.as_value_ref(), |_, json| {
            *json = value;
            Ok(())
        })
    }

    fn insert_parsed(
        &mut self,
        key: ValueRef,
        parse: impl FnOnce(ValueRef, &mut Jsonb) -> crate::Result<()>,
    ) -> crate::Result<usize> {
        let slot = if self.used < JSON_CACHE_SIZE {
            self.used
        } else {
            self.find_oldest_entry()
        };
        match &mut self.entries[slot] {
            Some((stored_key, json)) => {
                let stored = parse(key, json).and_then(|()| copy_key_into(stored_key, key));
                if let Err(error) = stored {
                    self.entries[slot] = None;
                    return Err(error);
                }
            }
            None => {
                let mut json = Jsonb::empty();
                parse(key, &mut json)?;
                self.entries[slot] = Some((key.to_owned()?, json));
            }
        }
        self.key_lens[slot] = key_byte_len(key);
        if slot == self.used {
            self.used += 1;
        }
        self.age[slot] = self.counter;
        self.counter += 1;
        Ok(slot)
    }

    #[cfg(test)]
    pub fn lookup(&mut self, key: impl AsValueRef) -> crate::Result<Option<Jsonb>> {
        match self.find(key) {
            Some(slot) => Ok(Some(self.entry(slot).try_clone()?)),
            None => Ok(None),
        }
    }

    fn find(&mut self, key: impl AsValueRef) -> Option<usize> {
        let key = key.as_value_ref();
        let key_len = key_byte_len(key);
        for i in (0..self.used).rev() {
            if self.key_lens[i] != key_len {
                continue;
            }
            if let Some((stored_key, _)) = &self.entries[i] {
                if key == *stored_key {
                    self.age[i] = self.counter;
                    self.counter += 1;
                    return Some(i);
                }
            }
        }
        None
    }

    fn entry(&self, slot: usize) -> &Jsonb {
        let (_, json) = self.entries[slot]
            .as_ref()
            .expect("a slot returned by find or insert holds an entry");
        json
    }

    pub fn clear(&mut self) {
        self.counter = 0;
        self.used = 0;
    }
}

fn copy_key_into(stored_key: &mut Value, key: ValueRef) -> crate::Result<()> {
    match (stored_key, key) {
        (
            Value::Text(Text {
                value: Cow::Owned(text),
                subtype,
            }),
            ValueRef::Text(new_text),
        ) => {
            text.clear();
            text.try_reserve(new_text.value.len())?;
            text.push_str(new_text.value);
            *subtype = new_text.subtype;
        }
        (Value::Blob(blob), ValueRef::Blob(new_blob)) => {
            blob.clear();
            blob.try_reserve(new_blob.len())?;
            blob.extend_from_slice(new_blob);
        }
        (stored_key, key) => *stored_key = key.to_owned()?,
    }
    Ok(())
}

fn key_byte_len(key: ValueRef) -> usize {
    match key {
        ValueRef::Text(text) => text.value.len(),
        ValueRef::Blob(blob) => blob.len(),
        ValueRef::Null | ValueRef::Numeric(_) => 0,
    }
}

#[derive(Debug)]
pub struct JsonCacheCell {
    inner: UnsafeCell<Option<JsonCache>>,
    accessed: Cell<bool>,
    paths: RefCell<ParsedPathCache>,
}

#[derive(Debug, Default)]
struct ParsedPathCache {
    entries: Vec<ParsedPath>,
    next_slot: usize,
}

#[derive(Debug)]
struct ParsedPath {
    text: String,
    strict: bool,
    path: JsonPath<'static>,
}

impl ParsedPathCache {
    fn find(&self, text: &str, strict: bool) -> Option<usize> {
        self.entries
            .iter()
            .position(|entry| entry.strict == strict && entry.text == text)
    }

    fn insert(&mut self, entry: ParsedPath) -> usize {
        if self.entries.len() < PATH_CACHE_SIZE {
            self.entries.push(entry);
            return self.entries.len() - 1;
        }
        let slot = self.next_slot;
        self.next_slot = (slot + 1) % PATH_CACHE_SIZE;
        self.entries[slot] = entry;
        slot
    }
}

struct JsonCacheAccessGuard<'a> {
    accessed: &'a Cell<bool>,
}

impl Drop for JsonCacheAccessGuard<'_> {
    fn drop(&mut self) {
        self.accessed.set(false);
    }
}

#[expect(
    clippy::new_without_default,
    reason = "callers should construct the cache explicitly"
)]
impl JsonCacheCell {
    pub fn new() -> Self {
        Self {
            inner: UnsafeCell::new(None),
            accessed: Cell::new(false),
            paths: RefCell::new(ParsedPathCache::default()),
        }
    }

    fn access_guard(&self) -> JsonCacheAccessGuard<'_> {
        assert!(!self.accessed.replace(true));
        JsonCacheAccessGuard {
            accessed: &self.accessed,
        }
    }

    #[cfg(test)]
    pub fn lookup(&self, key: impl AsValueRef) -> Option<Jsonb> {
        let _guard = self.access_guard();

        unsafe {
            let cache_ptr = self.inner.get();
            if (*cache_ptr).is_none() {
                *cache_ptr = Some(JsonCache::new());
            }

            if let Some(cache) = &mut (*cache_ptr) {
                cache.lookup(key).expect(crate::alloc::ALLOC_ERR_MSG)
            } else {
                None
            }
        }
    }

    pub fn get_or_insert_with(
        &self,
        key: impl AsValueRef,
        value: impl FnOnce(ValueRef) -> crate::Result<Jsonb>,
    ) -> crate::Result<Jsonb> {
        self.with_jsonb(key, value, |json| Ok(json.try_clone()?))
    }

    pub fn with_path<R>(
        &self,
        path: ValueRef<'_>,
        strict: bool,
        read: impl FnOnce(Option<&JsonPath<'_>>) -> crate::Result<R>,
    ) -> crate::Result<R> {
        let ValueRef::Text(text) = path else {
            let parsed = json_path_from_db_value(&path, strict)?;
            return read(parsed.as_ref());
        };
        let mut paths = self.paths.borrow_mut();
        let slot = match paths.find(text.as_str(), strict) {
            Some(slot) => slot,
            None => {
                let Some(parsed) = json_path_from_db_value(&path, strict)? else {
                    return read(None);
                };
                paths.insert(ParsedPath {
                    text: text.as_str().to_string(),
                    strict,
                    path: parsed.into_owned(),
                })
            }
        };
        read(Some(&paths.entries[slot].path))
    }

    pub fn with_jsonb<R>(
        &self,
        key: impl AsValueRef,
        value: impl FnOnce(ValueRef) -> crate::Result<Jsonb>,
        read: impl FnOnce(&Jsonb) -> crate::Result<R>,
    ) -> crate::Result<R> {
        self.with_parsed_jsonb(
            key,
            |key, json| {
                *json = value(key)?;
                Ok(())
            },
            read,
        )
    }

    pub fn with_parsed_jsonb<R>(
        &self,
        key: impl AsValueRef,
        parse: impl FnOnce(ValueRef, &mut Jsonb) -> crate::Result<()>,
        read: impl FnOnce(&Jsonb) -> crate::Result<R>,
    ) -> crate::Result<R> {
        let key = key.as_value_ref();
        let _guard = self.access_guard();
        // SAFETY: the access guard asserts that no other borrow of the
        // cache is alive, and neither closure can reach this cell.
        let cache = unsafe { (*self.inner.get()).get_or_insert_with(JsonCache::new) };
        let slot = match cache.find(key) {
            Some(slot) => slot,
            None => cache.insert_parsed(key, parse)?,
        };
        read(cache.entry(slot))
    }

    pub fn is_empty(&self) -> bool {
        let _guard = self.access_guard();
        // SAFETY: the access guard asserts that no other borrow of the
        // cache is alive.
        unsafe { (*self.inner.get()).as_ref() }.is_none_or(|cache| cache.used == 0)
    }

    pub fn clear(&mut self) {
        let _guard = self.access_guard();
        unsafe {
            let cache_ptr = self.inner.get();
            if (*cache_ptr).is_none() {
                return;
            }

            if let Some(cache) = &mut (*cache_ptr) {
                cache.clear()
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
    fn parsed_path_cache_returns_the_same_parse_as_the_parser() {
        let cache_cell = JsonCacheCell::new();
        let texts = ["$.a", "$.b[1]", "a", "$", "$.c.d", "$.a", "a", "$[#-1]"];
        for _ in 0..3 {
            for text in texts {
                let value = Value::build_text(text);
                for strict in [true, false] {
                    let expected = json_path_from_db_value(&value, strict)
                        .map(|path| path.map(|path| format!("{:?}", path.elements)))
                        .map_err(|err| err.to_string());
                    let cached = cache_cell
                        .with_path(value.as_value_ref(), strict, |path| {
                            Ok(path.map(|path| format!("{:?}", path.elements)))
                        })
                        .map_err(|err| err.to_string());
                    assert_eq!(cached, expected, "{text} strict={strict}");
                }
            }
        }
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
            .insert(&key, value.try_clone().unwrap())
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
            .insert(&key1, value1.try_clone().unwrap())
            .expect(crate::alloc::ALLOC_ERR_MSG);
        cache
            .insert(&key2, value2.try_clone().unwrap())
            .expect(crate::alloc::ALLOC_ERR_MSG);
        cache
            .insert(&key3, value3.try_clone().unwrap())
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
            .insert(&key1, value1.try_clone().unwrap())
            .expect(crate::alloc::ALLOC_ERR_MSG);
        cache
            .insert(&key2, value2.try_clone().unwrap())
            .expect(crate::alloc::ALLOC_ERR_MSG);
        cache
            .insert(&key3, value3.try_clone().unwrap())
            .expect(crate::alloc::ALLOC_ERR_MSG);
        cache
            .insert(&key4, value4.try_clone().unwrap())
            .expect(crate::alloc::ALLOC_ERR_MSG);

        // Cache is now full
        assert_eq!(cache.used, 4);

        // Look up key1 to make it the most recently used
        let _ = cache.lookup(&key1).unwrap();

        // Insert one more entry - should evict the oldest (key2)
        cache
            .insert(&key5, value5.try_clone().unwrap())
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
            .insert(&key1, value1.try_clone().unwrap())
            .expect(crate::alloc::ALLOC_ERR_MSG);
        cache
            .insert(&key2, value2.try_clone().unwrap())
            .expect(crate::alloc::ALLOC_ERR_MSG);
        cache
            .insert(&key3, value3.try_clone().unwrap())
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

    #[test]
    fn insert_after_eviction_parses_into_the_evicted_buffer() {
        let mut cache = JsonCache::new();
        let keys: Vec<Value> = (0..5)
            .map(|i| Value::build_text(format!("{{\"id\": {i}, \"pad\": \"{}\"}}", "x".repeat(40))))
            .collect();
        for key in &keys[..4] {
            cache
                .insert_parsed(key.as_value_ref(), |key, json| {
                    let ValueRef::Text(text) = key else {
                        unreachable!("the keys are text")
                    };
                    json.replace_with_parsed_text(text.as_str())
                        .map_err(|_| crate::LimboError::ParseError("malformed JSON".to_string()))
                })
                .unwrap();
        }

        let mut reused_capacity = 0;
        cache
            .insert_parsed(keys[4].as_value_ref(), |key, json| {
                reused_capacity = json.data_capacity();
                let ValueRef::Text(text) = key else {
                    unreachable!("the keys are text")
                };
                json.replace_with_parsed_text(text.as_str())
                    .map_err(|_| crate::LimboError::ParseError("malformed JSON".to_string()))
            })
            .unwrap();

        assert!(reused_capacity > 0);
        assert!(cache.lookup(&keys[0]).unwrap().is_none());
        for key in &keys[1..] {
            let expected = Jsonb::from_str(key.to_text().unwrap()).unwrap();
            assert_eq!(cache.lookup(key).unwrap(), Some(expected));
        }
    }

    #[test]
    fn lookup_does_not_match_a_key_of_the_same_length_with_other_bytes() {
        let mut cache = JsonCache::new();
        let (key, value) = create_test_pair("[1]");
        cache.insert(&key, value.try_clone().unwrap()).unwrap();

        assert!(cache.lookup(Value::build_text("[2]")).unwrap().is_none());
        assert!(cache
            .lookup(Value::from_blob(
                crate::types::value_blob_from_slice(b"[1]").unwrap()
            ))
            .unwrap()
            .is_none());
        assert_eq!(cache.lookup(&key).unwrap(), Some(value));
    }

    #[test]
    fn insert_after_eviction_replaces_a_text_key_with_a_blob_key() {
        let mut cache = JsonCache::new();
        let pairs: Vec<(Value, Jsonb)> = (0..4)
            .map(|i| create_test_pair(&format!("[{i}]")))
            .collect();
        for (key, value) in &pairs {
            cache.insert(key, value.try_clone().unwrap()).unwrap();
        }

        let blob_key = Value::from_blob(crate::types::value_blob_from_slice(b"[0]").unwrap());
        let blob_value = Jsonb::from_str("[0]").unwrap();
        cache
            .insert(&blob_key, blob_value.try_clone().unwrap())
            .unwrap();

        assert!(cache.lookup(&pairs[0].0).unwrap().is_none());
        assert_eq!(cache.lookup(&blob_key).unwrap(), Some(blob_value));
        for (key, value) in &pairs[1..] {
            assert_eq!(cache.lookup(key).unwrap().as_ref(), Some(value));
        }
    }

    #[test]
    fn failed_parse_after_eviction_leaves_the_cache_usable() {
        let mut cache = JsonCache::new();
        let pairs: Vec<(Value, Jsonb)> = (0..4)
            .map(|i| create_test_pair(&format!("{{\"id\": {i}}}")))
            .collect();
        for (key, value) in &pairs {
            cache.insert(key, value.try_clone().unwrap()).unwrap();
        }

        let (failing_key, _) = create_test_pair("{\"id\": 4}");
        let result = cache.insert_parsed(failing_key.as_value_ref(), |_, _| {
            Err(crate::LimboError::ParseError("malformed JSON".to_string()))
        });
        assert!(result.is_err());
        assert_eq!(cache.used, JSON_CACHE_SIZE);
        assert!(cache.lookup(&failing_key).unwrap().is_none());
        assert!(cache.lookup(&pairs[0].0).unwrap().is_none());
        for (key, value) in &pairs[1..] {
            assert_eq!(cache.lookup(key).unwrap().as_ref(), Some(value));
        }

        let (next_key, next_value) = create_test_pair("{\"id\": 5}");
        cache
            .insert(&next_key, next_value.try_clone().unwrap())
            .unwrap();
        assert_eq!(cache.lookup(&next_key).unwrap(), Some(next_value));
    }
}
