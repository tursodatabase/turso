use std::borrow::Cow;
use std::cell::{Cell, RefCell, UnsafeCell};

use crate::alloc::{TryClone, TryReserveError};
use crate::sync::Arc;
use crate::types::{AsValueRef, Text};
use crate::{Value, ValueRef};

use super::json_path_from_text;
use super::jsonb::Jsonb;
use super::path::JsonPath;

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

    pub fn insert(
        &mut self,
        key: impl AsValueRef,
        value: &Jsonb,
    ) -> std::result::Result<(), TryReserveError> {
        let key = key.as_value_ref();
        let id = self.new_entry_slot();
        let entry = &mut self.entries[id];
        match entry {
            // The entry that the new entry evicts, or an entry from before a
            // clear. Its buffers usually have the capacity for the new entry,
            // so a cache miss does not allocate.
            Some((stored_key, stored_value)) => {
                if let Err(err) = stored_value
                    .copy_from(value)
                    .and_then(|()| copy_key(stored_key, key))
                {
                    // Half of a new entry must not match a lookup.
                    *entry = None;
                    return Err(err);
                }
            }
            None => *entry = Some((key.to_owned()?, value.try_clone()?)),
        }
        self.add_entry(id);
        Ok(())
    }

    /// Like [Self::insert], but the cache takes `value`, and the document of
    /// the evicted entry is dropped. Returns the slot of the new entry.
    fn insert_owned(
        &mut self,
        key: ValueRef,
        value: Jsonb,
    ) -> std::result::Result<usize, TryReserveError> {
        let id = self.new_entry_slot();
        let entry = &mut self.entries[id];
        match entry {
            Some((stored_key, stored_value)) => {
                *stored_value = value;
                if let Err(err) = copy_key(stored_key, key) {
                    // Half of a new entry must not match a lookup.
                    *entry = None;
                    return Err(err);
                }
            }
            None => *entry = Some((key.to_owned()?, value)),
        }
        self.add_entry(id);
        Ok(id)
    }

    /// The slot for a new entry: the next unused slot, or the slot of the
    /// oldest entry when the cache is full.
    fn new_entry_slot(&self) -> usize {
        if self.used < JSON_CACHE_SIZE {
            self.used
        } else {
            self.find_oldest_entry()
        }
    }

    /// Makes the entry in `id`, a slot from [Self::new_entry_slot], the newest
    /// entry.
    fn add_entry(&mut self, id: usize) {
        if self.used < JSON_CACHE_SIZE {
            self.used += 1;
        }
        self.age[id] = self.counter;
        self.counter += 1;
    }

    pub fn lookup(
        &mut self,
        key: impl AsValueRef,
    ) -> std::result::Result<Option<Jsonb>, TryReserveError> {
        match self.find(key.as_value_ref()) {
            Some(id) => Ok(Some(self.entry(id).try_clone()?)),
            None => Ok(None),
        }
    }

    /// Returns the slot of the entry for `key` and makes it the newest entry,
    /// or returns None when the cache has no entry for `key`.
    fn find(&mut self, key: ValueRef) -> Option<usize> {
        for i in (0..self.used).rev() {
            if let Some((stored_key, _)) = &self.entries[i] {
                if key_matches(key, stored_key) {
                    self.age[i] = self.counter;
                    self.counter += 1;
                    return Some(i);
                }
            }
        }
        None
    }

    /// The document of the entry in `id`, a slot that [Self::find] or
    /// [Self::insert_owned] returned.
    fn entry(&self, id: usize) -> &Jsonb {
        let (_, json) = self.entries[id]
            .as_ref()
            .expect("a slot from find or insert_owned holds an entry");
        json
    }

    pub fn clear(&mut self) {
        self.counter = 0;
        self.used = 0;
    }
}

/// Whether `key` is `stored`: the same type and the same value. A text or a
/// blob, the usual keys, is compared as bytes directly.
#[inline]
fn key_matches(key: ValueRef, stored: &Value) -> bool {
    match (key, stored) {
        (ValueRef::Text(key), Value::Text(stored)) => {
            key.value.as_bytes() == stored.value.as_bytes()
        }
        (ValueRef::Blob(key), Value::Blob(stored)) => key == stored.as_slice(),
        _ => key.value_type() == stored.value_type() && key == *stored,
    }
}

/// Makes `stored` a copy of `key`. A text or blob key is copied into the
/// buffer of `stored` when `stored` has the same type.
fn copy_key(stored: &mut Value, key: ValueRef) -> std::result::Result<(), TryReserveError> {
    match (&mut *stored, key) {
        (
            Value::Text(Text {
                value: Cow::Owned(stored_text),
                subtype,
            }),
            ValueRef::Text(text),
        ) => {
            stored_text.try_reserve(text.value.len().saturating_sub(stored_text.len()))?;
            stored_text.clear();
            stored_text.push_str(text.value);
            *subtype = text.subtype;
        }
        (Value::Blob(stored_blob), ValueRef::Blob(blob)) => {
            stored_blob.try_reserve(blob.len().saturating_sub(stored_blob.len()))?;
            stored_blob.clear();
            stored_blob.extend_from_slice(blob);
        }
        _ => *stored = key.to_owned()?,
    }
    Ok(())
}

#[derive(Debug)]
pub struct JsonCacheCell {
    inner: UnsafeCell<Option<JsonCache>>,
    accessed: Cell<bool>,
    paths: RefCell<PathCache>,
}

/// Parsed JSON paths, by their text and mode. A statement usually gives the
/// same paths for each row, and then each path is parsed once.
///
/// A path enters the cache when it misses the cache a second time within
/// the last [JSON_CACHE_SIZE] misses. Thus a statement that gives a new
/// path for each row does not pay for cache entries that it never uses:
/// its misses only copy the text into the buffers of `misses`.
#[derive(Debug, Default)]
struct PathCache {
    entries: [Option<CachedPath>; JSON_CACHE_SIZE],
    /// The entry that the next new path replaces.
    next_entry: usize,
    /// The texts and modes of the last paths that missed the cache.
    misses: [(String, bool); JSON_CACHE_SIZE],
    /// The miss that the next miss replaces.
    next_miss: usize,
}

#[derive(Debug)]
struct CachedPath {
    text: String,
    strict: bool,
    path: Arc<JsonPath<'static>>,
}

/// A parsed path: shared from the path cache, or made for one call.
pub enum CachedJsonPath<'a> {
    Cached(Arc<JsonPath<'static>>),
    Parsed(JsonPath<'a>),
}

impl<'a> std::ops::Deref for CachedJsonPath<'a> {
    type Target = JsonPath<'a>;

    fn deref(&self) -> &JsonPath<'a> {
        match self {
            Self::Cached(path) => path,
            Self::Parsed(path) => path,
        }
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
            paths: RefCell::new(PathCache::default()),
        }
    }

    /// Returns the path that [json_path_from_text] parses from `text` in the
    /// mode `strict`. A path in the cache is not parsed again.
    pub fn path<'t>(&self, text: &'t str, strict: bool) -> crate::Result<CachedJsonPath<'t>> {
        let mut paths = self.paths.borrow_mut();
        if let Some(cached) = paths
            .entries
            .iter()
            .flatten()
            .find(|cached| cached.strict == strict && cached.text == text)
        {
            return Ok(CachedJsonPath::Cached(cached.path.clone()));
        }
        let path = json_path_from_text(text, strict)?;
        let missed_before = paths
            .misses
            .iter()
            .any(|(missed, missed_strict)| *missed_strict == strict && missed == text);
        if !missed_before {
            let next_miss = paths.next_miss;
            let (missed, missed_strict) = &mut paths.misses[next_miss];
            missed.clear();
            missed.push_str(text);
            *missed_strict = strict;
            paths.next_miss = (next_miss + 1) % JSON_CACHE_SIZE;
            return Ok(CachedJsonPath::Parsed(path));
        }
        let path = Arc::new(path.into_owned());
        let next_entry = paths.next_entry;
        paths.entries[next_entry] = Some(CachedPath {
            text: text.to_owned(),
            strict,
            path: path.clone(),
        });
        paths.next_entry = (next_entry + 1) % JSON_CACHE_SIZE;
        Ok(CachedJsonPath::Cached(path))
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

    /// Calls `read` with the document for `key`: the document in the cache, or
    /// the document that `value` makes for `key`, which then enters the cache.
    /// The document is not copied, so this is for a caller that only reads it.
    #[inline]
    pub fn with_jsonb<R>(
        &self,
        key: impl AsValueRef,
        value: impl FnOnce(ValueRef) -> crate::Result<Jsonb>,
        read: impl FnOnce(&Jsonb) -> crate::Result<R>,
    ) -> crate::Result<R> {
        let key = key.as_value_ref();
        let _guard = self.access_guard();
        // SAFETY: the access guard makes a second access to the cache panic
        // while this reference lives, also from `value` or `read`.
        let cache = unsafe { (*self.inner.get()).get_or_insert_with(JsonCache::new) };
        let id = match cache.find(key) {
            Some(id) => id,
            None => cache.insert_owned(key, value(key)?)?,
        };
        read(cache.entry(id))
    }

    pub fn get_or_insert_with(
        &self,
        key: impl AsValueRef,
        value: impl FnOnce(ValueRef) -> crate::Result<Jsonb>,
    ) -> crate::Result<Jsonb> {
        let key = key.as_value_ref();
        let _guard = self.access_guard();
        unsafe {
            let cache_ptr = self.inner.get();
            if (*cache_ptr).is_none() {
                *cache_ptr = Some(JsonCache::new());
            }

            if let Some(cache) = &mut (*cache_ptr) {
                if let Some(jsonb) = cache.lookup(key)? {
                    Ok(jsonb)
                } else {
                    let result = value(key);
                    match result {
                        Ok(json) => {
                            cache.insert(key, &json)?;
                            Ok(json)
                        }
                        Err(e) => Err(e),
                    }
                }
            } else {
                value(key)
            }
        }
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

    #[test]
    fn test_json_cache_reuses_evicted_entries() {
        // Each insert evicts a key of another type, or a shorter or longer
        // key of the same type, so the copies into the evicted buffers
        // shrink and grow them.
        let keys = [
            Value::build_text("[1]"),
            Value::from_slice(&[0x0b]).unwrap(),
            Value::build_text("[\"a document that is longer than the others\"]"),
            Value::from_i64(7),
            Value::build_text("[2]"),
            Value::from_slice(&[0x0c, 0x0d]).unwrap(),
            Value::build_text("{}"),
            Value::from_i64(8),
            Value::build_text("[3]"),
        ];
        let values: Vec<Jsonb> = (0..keys.len())
            .map(|i| Jsonb::from_str(&format!("[{i}, \"{}\"]", "x".repeat(i * 7))).unwrap())
            .collect();

        let mut cache = JsonCache::new();
        for inserted in 0..keys.len() {
            cache
                .insert(&keys[inserted], &values[inserted])
                .expect(crate::alloc::ALLOC_ERR_MSG);
            // The lookups go from the oldest key to the newest, so the
            // entries keep their order of age.
            for (i, (key, value)) in keys.iter().zip(&values).enumerate().take(inserted + 1) {
                let expected = (i + JSON_CACHE_SIZE > inserted).then_some(value);
                assert_eq!(cache.lookup(key).unwrap().as_ref(), expected, "key {i}");
            }
        }

        // After a clear, the inserts reuse the entries from before it.
        cache.clear();
        cache
            .insert(&keys[0], &values[0])
            .expect(crate::alloc::ALLOC_ERR_MSG);
        cache
            .insert(&keys[2], &values[2])
            .expect(crate::alloc::ALLOC_ERR_MSG);
        for (i, (key, value)) in keys.iter().zip(&values).enumerate() {
            let expected = (i == 0 || i == 2).then_some(value);
            assert_eq!(cache.lookup(key).unwrap().as_ref(), expected, "key {i}");
        }
    }

    #[test]
    fn test_json_cache_cell_path_enters_on_the_second_miss() {
        use crate::json::path::json_path;
        let cell = JsonCacheCell::new();
        let parsed = |text| cell.path(text, true).unwrap();
        let expected = json_path("$.a[1].b").unwrap().elements;

        // The first miss parses the path, and the second also caches it.
        assert!(matches!(parsed("$.a[1].b"), CachedJsonPath::Parsed(_)));
        let second = parsed("$.a[1].b");
        assert!(matches!(second, CachedJsonPath::Cached(_)));
        assert_eq!(second.elements, expected);
        let third = parsed("$.a[1].b");
        assert!(matches!(third, CachedJsonPath::Cached(_)));
        assert_eq!(third.elements, expected);

        // The mode is part of the key: in non-strict mode, `a` is the label
        // of a member, and in strict mode, it is not a path.
        let label = vec![
            crate::json::path::PathElement::Root(),
            crate::json::path::PathElement::Key(Cow::Borrowed("a"), true),
        ];
        for _ in 0..3 {
            assert_eq!(cell.path("a", false).unwrap().elements, label);
        }
        assert!(matches!(
            cell.path("a", false).unwrap(),
            CachedJsonPath::Cached(_)
        ));
        assert!(cell.path("a", true).is_err());
        assert_eq!(parsed("$.a[1].b").elements, expected);
    }

    #[test]
    fn test_json_cache_cell_path_does_not_cache_new_paths_or_errors() {
        use crate::json::path::json_path;
        let cell = JsonCacheCell::new();
        // A new path for each call is parsed each time.
        for i in 0..20 {
            let text = format!("$.k{i}");
            let path = cell.path(&text, true).unwrap();
            assert!(matches!(path, CachedJsonPath::Parsed(_)));
            assert_eq!(path.elements, json_path(&text).unwrap().elements);
        }
        // Paths that come back within the last misses enter the cache.
        for _ in 0..2 {
            for text in ["$.x", "$.y", "$.z"] {
                cell.path(text, true).unwrap();
            }
        }
        for text in ["$.x", "$.y", "$.z"] {
            assert!(matches!(
                cell.path(text, true).unwrap(),
                CachedJsonPath::Cached(_)
            ));
        }
        // An error is returned each time, and is not cached.
        for _ in 0..3 {
            assert!(cell.path("$[", true).is_err());
        }
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
    fn with_jsonb_reads_the_cached_document() {
        let cache_cell = JsonCacheCell::new();
        let (key, value) = create_test_pair("{\"test\": \"value\"}");
        let text = cache_cell
            .with_jsonb(&key, |_| Ok(value.clone()), |json| json.to_string())
            .unwrap();
        assert_eq!(text, "{\"test\":\"value\"}");

        // The second call reads the cached document, without a new conversion.
        let converted = Cell::new(false);
        let text = cache_cell
            .with_jsonb(
                &key,
                |_| {
                    converted.set(true);
                    Ok(Jsonb::from_str("1").unwrap())
                },
                |json| json.to_string(),
            )
            .unwrap();
        assert!(!converted.get());
        assert_eq!(text, "{\"test\":\"value\"}");
        assert!(!cache_cell.accessed.get());
    }

    #[test]
    fn with_jsonb_does_not_cache_a_failed_conversion() {
        let cache_cell = JsonCacheCell::new();
        let (key, _) = create_test_pair("{\"test\": \"value\"}");
        let result = cache_cell.with_jsonb(
            &key,
            |_| Err(crate::LimboError::ParseError("malformed JSON".to_string())),
            |_| Ok(()),
        );
        assert!(result.is_err());
        assert!(!cache_cell.accessed.get());
        assert!(cache_cell.lookup(&key).is_none());
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
