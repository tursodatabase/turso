use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, Ordering};

use super::pager::PageRef;
use crate::turso_assert;
use std::sync::Arc;
use tracing::{debug, trace};

/// FIXME: https://github.com/tursodatabase/turso/issues/1661
const DEFAULT_PAGE_CACHE_SIZE_IN_PAGES_MAKE_ME_SMALLER_ONCE_WAL_SPILL_IS_IMPLEMENTED: usize =
    100000;

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct PageCacheKey {
    pgno: usize,
}

struct PageCacheEntry {
    key: PageCacheKey,
    page: PageRef,
    reference_bit: AtomicBool,
}

pub struct PageCache {
    capacity: usize,
    map: RefCell<PageHashMap>,
    // Current position of the clock hand
    clock_hand: Cell<usize>,
    // Fixed-size array for clock algorithm
    entries: RefCell<Vec<Option<Arc<PageCacheEntry>>>>,
}

unsafe impl Send for PageCache {}
unsafe impl Sync for PageCache {}

struct PageHashMap {
    buckets: Vec<Vec<HashMapNode>>,
    capacity: usize,
    size: usize,
}

#[derive(Clone)]
struct HashMapNode {
    key: PageCacheKey,
    value: Arc<PageCacheEntry>,
    // Index in the entries array
    slot_index: usize,
}

#[derive(Debug, PartialEq)]
pub enum CacheError {
    InternalError(String),
    Locked { pgno: usize },
    Dirty { pgno: usize },
    Pinned { pgno: usize },
    ActiveRefs,
    Full,
    KeyExists,
}

#[derive(Debug, PartialEq)]
pub enum CacheResizeResult {
    Done,
    PendingEvictions,
}

impl PageCacheKey {
    pub fn new(pgno: usize) -> Self {
        Self { pgno }
    }
}

impl PageCacheEntry {
    #[allow(dead_code)]
    pub fn new(key: PageCacheKey, page: PageRef) -> Self {
        Self {
            key,
            page,
            // set the reference bit on creation
            reference_bit: AtomicBool::new(true),
        }
    }
    #[inline]
    pub fn touch(&self) {
        self.reference_bit.store(true, Ordering::Relaxed);
    }
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity of cache should be at least 1");
        Self {
            capacity,
            map: RefCell::new(PageHashMap::new(capacity)),
            clock_hand: Cell::new(0),
            entries: RefCell::new(vec![None; capacity]),
        }
    }

    pub fn contains_key(&self, key: &PageCacheKey) -> bool {
        self.map.borrow().contains_key(key)
    }

    pub fn insert(&mut self, key: PageCacheKey, value: PageRef) -> Result<(), CacheError> {
        self._insert(key, value, false)
    }

    pub fn insert_ignore_existing(
        &mut self,
        key: PageCacheKey,
        value: PageRef,
    ) -> Result<(), CacheError> {
        self._insert(key, value, true)
    }

    pub fn _insert(
        &mut self,
        key: PageCacheKey,
        value: PageRef,
        ignore_exists: bool,
    ) -> Result<(), CacheError> {
        trace!("insert(key={:?})", key);

        // Check first if page already exists in cache
        if !ignore_exists {
            if let Some(existing_page_ref) = self.get(&key) {
                assert!(
                    Arc::ptr_eq(&value, &existing_page_ref),
                    "Attempted to insert different page with same key: {key:?}"
                );
                return Err(CacheError::KeyExists);
            }
        }

        self.make_room_for(1)?;
        let slot_index = self.find_free_slot()?;

        let entry = Arc::new(PageCacheEntry::new(key.clone(), value));
        self.entries.borrow_mut()[slot_index] = Some(entry.clone());
        self.map.borrow_mut().insert(key, entry, slot_index);

        Ok(())
    }

    /// Attempts to find a free slot in the entries array by scanning
    /// from the current clock hand position.
    fn find_free_slot(&self) -> Result<usize, CacheError> {
        let entries = self.entries.borrow();
        let mut clock_hand = self.clock_hand.get();
        let start_hand = clock_hand;

        loop {
            if entries[clock_hand].is_none() {
                self.clock_hand.set((clock_hand + 1) % self.capacity);
                return Ok(clock_hand);
            }
            clock_hand = (clock_hand + 1) % self.capacity;
            if clock_hand == start_hand {
                // We've made a full rotation without finding a free slot
                return Err(CacheError::Full);
            }
        }
    }

    #[inline]
    fn cycle_clock_hand(&self) {
        self.clock_hand
            .set((self.clock_hand.get() + 1) % self.capacity);
    }

    #[inline]
    pub fn delete(&mut self, key: PageCacheKey) -> Result<(), CacheError> {
        trace!("cache_delete(key={:?})", key);
        self._delete(key, true)
    }

    pub fn _delete(&mut self, key: PageCacheKey, clean_page: bool) -> Result<(), CacheError> {
        if !self.contains_key(&key) {
            return Ok(());
        }

        let (entry, slot_index) = {
            let map = self.map.borrow();
            let node = map.get_with_slot(&key).ok_or_else(|| {
                CacheError::InternalError("Key exists but not found in map".to_string())
            })?;
            (node.0.clone(), node.1)
        };

        // Check if page can be deleted
        if entry.page.is_locked() {
            return Err(CacheError::Locked {
                pgno: entry.page.get().id,
            });
        }
        if entry.page.is_dirty() {
            return Err(CacheError::Dirty {
                pgno: entry.page.get().id,
            });
        }
        if entry.page.is_pinned() {
            return Err(CacheError::Pinned {
                pgno: entry.page.get().id,
            });
        }

        if clean_page {
            entry.page.clear_loaded();
            debug!("clean(page={})", entry.page.get().id);
            let _ = entry.page.get().contents.take();
        }

        self.map.borrow_mut().remove(&key);
        self.entries.borrow_mut()[slot_index] = None;

        Ok(())
    }

    #[inline]
    pub fn get(&self, key: &PageCacheKey) -> Option<PageRef> {
        self.peek(key, true)
    }

    /// Get page without promoting entry
    #[inline]
    pub fn peek(&self, key: &PageCacheKey, touch: bool) -> Option<PageRef> {
        trace!("cache_get(key={:?})", key);

        let map = self.map.borrow();
        let entry = map.get(key)?;
        let page = entry.page.clone();
        if touch {
            entry.touch();
        }
        Some(page)
    }

    pub fn resize(&mut self, capacity: usize) -> CacheResizeResult {
        if capacity == self.capacity {
            return CacheResizeResult::Done;
        }

        if capacity < self.capacity {
            let to_drop: Vec<PageCacheKey> = self.entries.borrow()[capacity..]
                .iter()
                .filter_map(|e| e.as_ref().map(|en| en.key.clone()))
                .collect();
            for k in to_drop {
                let clean_page = true;
                let _ = self._delete(k, clean_page);
            }
        }

        self.entries.borrow_mut().resize(capacity, None);
        self.capacity = capacity;

        // Rebuild the hashmap (all remaining slot_index are < capacity now)
        let new_map = self.map.borrow().rehash(capacity);
        self.map.replace(new_map);
        if self.clock_hand.get() >= capacity {
            self.clock_hand.set(0);
        }
        match self.make_room_for(0) {
            Ok(_) => CacheResizeResult::Done,
            Err(_) => CacheResizeResult::PendingEvictions,
        }
    }

    pub fn make_room_for(&mut self, n: usize) -> Result<(), CacheError> {
        if n > self.capacity {
            return Err(CacheError::Full);
        }

        let len = self.len();
        let available = self.capacity.saturating_sub(len);
        if n <= available && len <= self.capacity {
            return Ok(());
        }

        // Calculate how many entries we need to evict
        let available = self.capacity.saturating_sub(len);
        let x = n.saturating_sub(available);
        let mut need_to_evict = x.saturating_add(len.saturating_sub(self.capacity));

        // Clock algorithm: sweep through entries looking for victims
        let start = self.clock_hand.get();
        let mut clock_hand = start;
        let mut made_progress = false;
        while need_to_evict > 0 {
            let entry_opt = { self.entries.borrow()[clock_hand].as_ref().cloned() };
            if let Some(entry) = entry_opt {
                if entry.reference_bit.swap(false, Ordering::Relaxed) {
                    // second chance granted this turn
                } else if self._delete(entry.key.clone(), true).is_ok() {
                    made_progress = true;
                    need_to_evict -= 1;
                }
            }
            self.cycle_clock_hand();
            clock_hand = self.clock_hand.get();
            // if we wrapped and made no progress: give up
            if clock_hand == start && !made_progress {
                return Err(CacheError::Full);
            }
            // if we wrapped, reset the progress flag for the next lap
            if clock_hand == start {
                made_progress = false;
            }
        }
        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), CacheError> {
        let entries = self.entries.borrow().clone();

        for entry in entries.iter().flatten() {
            if entry.page.is_dirty() {
                return Err(CacheError::Dirty {
                    pgno: entry.page.get().id,
                });
            }
            if entry.page.is_locked() {
                return Err(CacheError::Locked {
                    pgno: entry.page.get().id,
                });
            }
            if entry.page.is_pinned() {
                return Err(CacheError::Pinned {
                    pgno: entry.page.get().id,
                });
            }

            entry.page.clear_loaded();
            let _ = entry.page.get().contents.take();
        }

        self.entries.borrow_mut().fill(None);
        self.map.borrow_mut().clear();
        self.clock_hand.set(0);

        Ok(())
    }

    /// Removes all pages from the cache with pgno greater than len
    pub fn truncate(&mut self, len: usize) -> Result<(), CacheError> {
        let entries = self.entries.borrow().clone();

        for entry in entries.iter().flatten() {
            if entry.key.pgno > len {
                turso_assert!(!entry.page.is_dirty(), "page must be clean");
                turso_assert!(!entry.page.is_locked(), "page must be unlocked");
                turso_assert!(!entry.page.is_pinned(), "page must be unpinned");

                self.delete(entry.key.clone())?;
            }
        }

        Ok(())
    }

    pub fn print(&self) {
        tracing::debug!("page_cache_len={}", self.map.borrow().len());
        let entries = self.entries.borrow();

        for (i, entry_opt) in entries.iter().enumerate() {
            if let Some(entry) = entry_opt {
                tracing::debug!(
                    "slot={}, page={:?}, flags={}, pin_count={}, ref_bit={}",
                    i,
                    entry.key,
                    entry.page.get().flags.load(Ordering::SeqCst),
                    entry.page.get().pin_count.load(Ordering::SeqCst),
                    entry.reference_bit.load(Ordering::Relaxed),
                );
            }
        }
    }

    #[cfg(test)]
    pub fn keys(&mut self) -> Vec<PageCacheKey> {
        let mut keys = Vec::new();
        let entries = self.entries.borrow();

        for entry in entries.iter().flatten() {
            keys.push(entry.key.clone());
        }

        keys
    }

    pub fn len(&self) -> usize {
        self.map.borrow().len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn unset_dirty_all_pages(&mut self) {
        let entries = self.entries.borrow();
        for entry in entries.iter().flatten() {
            entry.page.clear_dirty();
        }
    }

    #[cfg(test)]
    fn verify_cache_integrity(&self) {
        let map_len = self.map.borrow().len();
        let entries = self.entries.borrow();

        // Count non-None entries
        let entry_count = entries.iter().filter(|e| e.is_some()).count();

        assert_eq!(
            map_len, entry_count,
            "Map size {map_len} doesn't match entry count {entry_count}",
        );

        // Verify all map entries point to valid slots
        for node in self.map.borrow().iter() {
            assert!(
                node.slot_index < self.capacity,
                "Invalid slot index {} for key {:?}",
                node.slot_index,
                node.key
            );

            let slot_entry = entries[node.slot_index].as_ref();
            assert!(
                slot_entry.is_some(),
                "Slot {} should not be None for key {:?}",
                node.slot_index,
                node.key
            );

            assert_eq!(
                slot_entry.unwrap().key,
                node.key,
                "Key mismatch at slot {}",
                node.slot_index
            );
        }
    }
}

impl Default for PageCache {
    fn default() -> Self {
        PageCache::new(
            DEFAULT_PAGE_CACHE_SIZE_IN_PAGES_MAKE_ME_SMALLER_ONCE_WAL_SPILL_IS_IMPLEMENTED,
        )
    }
}

impl PageHashMap {
    pub fn new(capacity: usize) -> PageHashMap {
        PageHashMap {
            buckets: vec![vec![]; capacity],
            capacity,
            size: 0,
        }
    }

    pub fn insert(
        &mut self,
        key: PageCacheKey,
        value: Arc<PageCacheEntry>,
        slot_index: usize,
    ) -> Option<Arc<PageCacheEntry>> {
        let bucket = self.hash(&key);
        let bucket = &mut self.buckets[bucket];
        let mut idx = 0;
        while let Some(node) = bucket.get_mut(idx) {
            if node.key == key {
                let prev = node.value.clone();
                node.value = value;
                node.slot_index = slot_index;
                return Some(prev);
            }
            idx += 1;
        }
        bucket.push(HashMapNode {
            key,
            value,
            slot_index,
        });
        self.size += 1;
        None
    }

    pub fn contains_key(&self, key: &PageCacheKey) -> bool {
        let bucket = self.hash(key);
        self.buckets[bucket].iter().any(|node| node.key == *key)
    }

    pub fn get(&self, key: &PageCacheKey) -> Option<&Arc<PageCacheEntry>> {
        let bucket = self.hash(key);
        let bucket = &self.buckets[bucket];
        for node in bucket {
            if node.key == *key {
                return Some(&node.value);
            }
        }
        None
    }

    pub fn get_with_slot(&self, key: &PageCacheKey) -> Option<(&Arc<PageCacheEntry>, usize)> {
        let bucket = self.hash(key);
        let bucket = &self.buckets[bucket];
        for node in bucket {
            if node.key == *key {
                return Some((&node.value, node.slot_index));
            }
        }
        None
    }

    pub fn remove(&mut self, key: &PageCacheKey) -> Option<Arc<PageCacheEntry>> {
        let bucket = self.hash(key);
        let bucket = &mut self.buckets[bucket];
        let mut idx = 0;
        while let Some(node) = bucket.get(idx) {
            if node.key == *key {
                break;
            }
            idx += 1;
        }
        if idx == bucket.len() {
            None
        } else {
            let v = bucket.remove(idx);
            self.size -= 1;
            Some(v.value)
        }
    }

    pub fn clear(&mut self) {
        for bucket in &mut self.buckets {
            bucket.clear();
        }
        self.size = 0;
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn iter(&self) -> impl Iterator<Item = &HashMapNode> {
        self.buckets.iter().flat_map(|bucket| bucket.iter())
    }

    fn hash(&self, key: &PageCacheKey) -> usize {
        if self.capacity.is_power_of_two() {
            key.pgno & (self.capacity - 1)
        } else {
            key.pgno % self.capacity
        }
    }

    pub fn rehash(&self, new_capacity: usize) -> PageHashMap {
        let mut new_hash_map = PageHashMap::new(new_capacity);
        for node in self.iter() {
            new_hash_map.insert(node.key.clone(), node.value.clone(), node.slot_index);
        }
        new_hash_map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page_cache::CacheError;
    use crate::storage::pager::{Page, PageRef};
    use crate::storage::sqlite3_ondisk::PageContent;
    use crate::{BufferPool, IO};
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };
    use std::sync::Arc;
    use std::sync::OnceLock;

    fn create_key(id: usize) -> PageCacheKey {
        PageCacheKey::new(id)
    }

    static TEST_BUFFER_POOL: OnceLock<Arc<BufferPool>> = OnceLock::new();

    #[allow(clippy::arc_with_non_send_sync)]
    pub fn page_with_content(page_id: usize) -> PageRef {
        let page = Arc::new(Page::new(page_id));
        {
            let mock_io = Arc::new(crate::PlatformIO::new().unwrap()) as Arc<dyn IO>;
            let pool = TEST_BUFFER_POOL
                .get_or_init(|| BufferPool::begin_init(&mock_io, BufferPool::TEST_ARENA_SIZE));
            let buffer = pool.allocate(4096);
            let page_content = PageContent {
                offset: 0,
                buffer: Arc::new(buffer),
                overflow_cells: Vec::new(),
            };
            page.get().contents = Some(page_content);
            page.set_loaded();
        }
        page
    }

    fn insert_page(cache: &mut PageCache, id: usize) -> PageCacheKey {
        let key = create_key(id);
        let page = page_with_content(id);
        assert!(cache.insert(key.clone(), page).is_ok());
        key
    }

    fn page_has_content(page: &PageRef) -> bool {
        page.is_loaded() && page.get().contents.is_some()
    }

    #[test]
    fn test_delete_only_element() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        cache.verify_cache_integrity();
        assert_eq!(cache.len(), 1);

        assert!(cache.delete(key1.clone()).is_ok());

        assert_eq!(
            cache.len(),
            0,
            "Length should be 0 after deleting only element"
        );
        assert!(
            !cache.contains_key(&key1),
            "Cache should not contain key after delete"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_multiple_elements() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);
        cache.verify_cache_integrity();
        assert_eq!(cache.len(), 3);

        // Delete middle element
        assert!(cache.delete(key2.clone()).is_ok());
        assert_eq!(cache.len(), 2, "Length should be 2 after deleting one");
        assert!(!cache.contains_key(&key2), "Should not contain deleted key");
        cache.verify_cache_integrity();

        // Delete another
        assert!(cache.delete(key1.clone()).is_ok());
        assert_eq!(cache.len(), 1, "Length should be 1 after deleting two");
        assert!(!cache.contains_key(&key1), "Should not contain deleted key");
        cache.verify_cache_integrity();

        // Delete last
        assert!(cache.delete(key3.clone()).is_ok());
        assert_eq!(cache.len(), 0, "Length should be 0 after deleting all");
        cache.verify_cache_integrity();
    }

    #[test]
    #[ignore = "for now let's not track active refs"]
    fn test_delete_with_active_ref() {
        let mut cache = PageCache::default();
        let key1 = create_key(1);
        let page1 = page_with_content(1);
        assert!(cache.insert(key1.clone(), page1.clone()).is_ok());
        assert!(page_has_content(&page1));
        cache.verify_cache_integrity();

        let result = cache.delete(key1.clone());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), CacheError::ActiveRefs);
        assert_eq!(cache.len(), 1);

        drop(page1);

        assert!(cache.delete(key1).is_ok());
        assert_eq!(cache.len(), 0);
        cache.verify_cache_integrity();
    }

    #[test]
    #[should_panic(expected = "Attempted to insert different page with same key")]
    fn test_insert_existing_key_fail() {
        let mut cache = PageCache::default();
        let key1 = create_key(1);
        let page1_v1 = page_with_content(1);
        let page1_v2 = page_with_content(1);
        assert!(cache.insert(key1.clone(), page1_v1.clone()).is_ok());
        assert_eq!(cache.len(), 1);
        cache.verify_cache_integrity();
        let _ = cache.insert(key1.clone(), page1_v2.clone()); // Panic
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let mut cache = PageCache::default();
        let key_nonexist = create_key(99);

        assert!(cache.delete(key_nonexist.clone()).is_ok()); // no-op
    }

    #[test]
    fn test_page_cache_evict() {
        let mut cache = PageCache::new(1);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.get(&key2).unwrap().get().id, 2);
        assert!(cache.get(&key1).is_none());
    }

    #[test]
    fn test_clock_algorithm_second_chance() {
        // This test verifies the clock algorithm gives pages with reference bit a second chance
        let mut cache = PageCache::new(3);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        assert_eq!(cache.len(), 3);

        // Access key2 to set its reference bit
        assert!(cache.get(&key2).is_some());
        // Insert key4, this will trigger eviction
        // The clock will sweep and should skip key2 due to its reference bit
        let key4 = insert_page(&mut cache, 4);
        // key2 should definitely be in cache due to recent access
        assert!(
            cache.get(&key2).is_some(),
            "key2 should still be in cache due to reference bit"
        );
        assert!(cache.get(&key4).is_some(), "key4 should be in cache");
        // One of key1 or key3 should be evicted
        let key1_present = cache.get(&key1).is_some();
        let key3_present = cache.get(&key3).is_some();
        assert!(
            key1_present != key3_present,
            "Exactly one of key1 or key3 should be evicted, but not both"
        );
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_delete_locked_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).unwrap();
        page.set_locked();

        assert_eq!(cache.delete(key), Err(CacheError::Locked { pgno: 1 }));
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_dirty_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).expect("Page should exist");
        page.set_dirty();

        assert_eq!(cache.delete(key), Err(CacheError::Dirty { pgno: 1 }));
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_pinned_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).expect("Page should exist");
        page.pin();

        assert_eq!(cache.delete(key), Err(CacheError::Pinned { pgno: 1 }));
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_make_room_for_with_dirty_pages() {
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Make both pages dirty
        cache.get(&key1).unwrap().set_dirty();
        cache.get(&key2).unwrap().set_dirty();

        // Try to insert a third page, should fail because can't evict dirty pages
        let key3 = create_key(3);
        let page3 = page_with_content(3);
        let result = cache.insert(key3, page3);

        assert_eq!(result, Err(CacheError::Full));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_page_cache_insert_and_get() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.get(&key1).unwrap().get().id, 1);
        assert_eq!(cache.get(&key2).unwrap().get().id, 2);
    }

    #[test]
    fn test_page_cache_over_capacity() {
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // With clock algorithm, key1 might still be in cache if it was accessed
        // Let's check that we have exactly 2 pages and key3 is definitely there
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(&key3).unwrap().get().id, 3);

        // One of key1 or key2 should be evicted
        let key1_present = cache.get(&key1).is_some();
        let key2_present = cache.get(&key2).is_some();
        assert!(
            key1_present ^ key2_present,
            "Exactly one of key1 or key2 should be evicted"
        );
    }

    #[test]
    fn test_page_cache_delete() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        assert!(cache.delete(key1.clone()).is_ok());
        assert!(cache.get(&key1).is_none());
    }

    #[test]
    fn test_page_cache_clear() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert!(cache.clear().is_ok());
        assert!(cache.get(&key1).is_none());
        assert!(cache.get(&key2).is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_page_cache_insert_sequential() {
        let mut cache = PageCache::default();
        for i in 0..10000 {
            let key = insert_page(&mut cache, i);
            assert_eq!(cache.peek(&key, false).unwrap().get().id, i);
        }
    }

    #[test]
    fn test_resize_smaller_success() {
        let mut cache = PageCache::default();
        for i in 1..=5 {
            let _ = insert_page(&mut cache, i);
        }
        assert_eq!(cache.len(), 5);
        let result = cache.resize(3);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.capacity(), 3);
        assert!(cache.insert(create_key(6), page_with_content(6)).is_ok());
    }

    #[test]
    fn test_resize_larger() {
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.len(), 2);

        let result = cache.resize(5);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.capacity(), 5);

        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key2).is_some());

        // Now we should be able to add 3 more
        for i in 3..=5 {
            let _ = insert_page(&mut cache, i);
        }
        assert_eq!(cache.len(), 5);
        cache.verify_cache_integrity();
    }

    #[test]
    #[ignore = "for now let's not track active refs"]
    fn test_resize_with_active_references() {
        let mut cache = PageCache::default();
        let page1 = page_with_content(1);
        let page2 = page_with_content(2);
        let page3 = page_with_content(3);
        assert!(cache.insert(create_key(1), page1.clone()).is_ok());
        assert!(cache.insert(create_key(2), page2.clone()).is_ok());
        assert!(cache.insert(create_key(3), page3.clone()).is_ok());
        assert_eq!(cache.len(), 3);
        cache.verify_cache_integrity();

        assert_eq!(cache.resize(2), CacheResizeResult::PendingEvictions);
        assert_eq!(cache.capacity(), 2);
        assert_eq!(cache.len(), 3);

        drop(page2);
        drop(page3);
        assert_eq!(cache.resize(1), CacheResizeResult::Done);
        assert_eq!(cache.len(), 1);

        assert!(cache.insert(create_key(4), page_with_content(4)).is_err());
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_resize_same_capacity() {
        let mut cache = PageCache::new(3);
        for i in 1..=3 {
            let _ = insert_page(&mut cache, i);
        }
        let result = cache.resize(3);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.capacity(), 3);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_truncate_page_cache() {
        let mut cache = PageCache::new(10);
        let _ = insert_page(&mut cache, 1);
        let _ = insert_page(&mut cache, 4);
        let _ = insert_page(&mut cache, 8);
        let _ = insert_page(&mut cache, 10);

        cache.truncate(4).unwrap();

        assert!(cache.contains_key(&PageCacheKey { pgno: 1 }));
        assert!(cache.contains_key(&PageCacheKey { pgno: 4 }));
        assert!(!cache.contains_key(&PageCacheKey { pgno: 8 }));
        assert!(!cache.contains_key(&PageCacheKey { pgno: 10 }));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.capacity(), 10);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_truncate_page_cache_remove_all() {
        let mut cache = PageCache::new(10);
        let _ = insert_page(&mut cache, 8);
        let _ = insert_page(&mut cache, 10);

        cache.truncate(4).unwrap();

        assert!(!cache.contains_key(&PageCacheKey { pgno: 8 }));
        assert!(!cache.contains_key(&PageCacheKey { pgno: 10 }));
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.capacity(), 10);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_fuzz() {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        tracing::info!("super seed: {}", seed);
        let max_pages = 10;
        let mut cache = PageCache::new(10);
        let mut reference_map = std::collections::HashMap::new();

        for _ in 0..10000 {
            cache.print();

            match rng.next_u64() % 2 {
                0 => {
                    // add
                    let id_page = rng.next_u64() % max_pages;
                    let key = PageCacheKey::new(id_page as usize);
                    #[allow(clippy::arc_with_non_send_sync)]
                    let page = Arc::new(Page::new(id_page as usize));

                    if cache.peek(&key, false).is_some() {
                        continue; // skip duplicate page ids
                    }

                    tracing::debug!("inserting page {:?}", key);
                    match cache.insert(key.clone(), page.clone()) {
                        Err(CacheError::Full | CacheError::ActiveRefs) => {} // Ignore
                        Err(err) => {
                            panic!("Cache insertion failed: {err:?}");
                        }
                        Ok(_) => {
                            reference_map.insert(key, page);
                            // Clean up reference_map if cache evicted something
                            if cache.len() < reference_map.len() {
                                reference_map.retain(|k, _| cache.contains_key(k));
                            }
                        }
                    }
                    assert!(cache.len() <= 10);
                }
                1 => {
                    // remove
                    let random = rng.next_u64() % 2 == 0;
                    let key = if random || reference_map.is_empty() {
                        let id_page: u64 = rng.next_u64() % max_pages;
                        PageCacheKey::new(id_page as usize)
                    } else {
                        let i = rng.next_u64() as usize % reference_map.len();
                        reference_map.keys().nth(i).unwrap().clone()
                    };

                    tracing::debug!("removing page {:?}", key);
                    reference_map.remove(&key);
                    assert!(cache.delete(key).is_ok());
                }
                _ => unreachable!(),
            }

            cache.verify_cache_integrity();

            // Verify all pages in reference_map are in cache
            for (key, page) in &reference_map {
                let cached_page = cache.peek(key, false).expect("Page should be in cache");
                assert_eq!(cached_page.get().id, key.pgno);
                assert_eq!(page.get().id, key.pgno);
            }
        }
    }

    #[test]
    fn test_peek_without_touch() {
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Peek without touching - shouldn't set reference bit
        assert!(cache.peek(&key1, false).is_some());

        // Insert key3 - key1 might be evicted since we didn't touch it
        let key3 = insert_page(&mut cache, 3);
        assert!(cache.get(&key3).is_some());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_unset_dirty_all_pages() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        cache.get(&key1).unwrap().set_dirty();
        cache.get(&key2).unwrap().set_dirty();

        cache.unset_dirty_all_pages();

        assert!(!cache.get(&key1).unwrap().is_dirty());
        assert!(!cache.get(&key2).unwrap().is_dirty());
    }

    #[test]
    #[ignore = "long running test, remove to verify"]
    fn test_clear_memory_stability() {
        let initial_memory = memory_stats::memory_stats().unwrap().physical_mem;

        for _ in 0..100000 {
            let mut cache = PageCache::new(1000);

            for i in 0..1000 {
                let key = create_key(i);
                let page = page_with_content(i);
                cache.insert(key, page).unwrap();
            }

            cache.clear().unwrap();
            drop(cache);
        }

        let final_memory = memory_stats::memory_stats().unwrap().physical_mem;

        let growth = final_memory.saturating_sub(initial_memory);
        println!("Growth: {growth}");
        assert!(
            growth < 10_000_000,
            "Memory grew by {growth} bytes over 10 cycles"
        );
    }
}
