use std::cell::{Cell, UnsafeCell};

use crate::alloc::{TryClone, TryReserveError};
use crate::types::AsValueRef;
use crate::{Value, ValueRef};

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

    pub fn insert(
        &mut self,
        key: impl AsValueRef,
        value: &Jsonb,
    ) -> std::result::Result<(), TryReserveError> {
        let key = key.as_value_ref();
        let entry = (key.to_owned()?, value.try_clone()?);
        if self.used < JSON_CACHE_SIZE {
            self.entries[self.used] = Some(entry);
            self.age[self.used] = self.counter;
            self.counter += 1;
            self.used += 1
        } else {
            let id = self.find_oldest_entry();

            self.entries[id] = Some(entry);
            self.age[id] = self.counter;
            self.counter += 1;
        }
        Ok(())
    }

    pub fn lookup(
        &mut self,
        key: impl AsValueRef,
    ) -> std::result::Result<Option<Jsonb>, TryReserveError> {
        let key = key.as_value_ref();
        for i in (0..self.used).rev() {
            if let Some((stored_key, value)) = &self.entries[i] {
                if key == *stored_key {
                    let json = value.try_clone()?;
                    self.age[i] = self.counter;
                    self.counter += 1;

                    return Ok(Some(json));
                }
            }
        }
        Ok(None)
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
#[path = "../tests/unit/json/cache/tests.rs"]
mod tests;
