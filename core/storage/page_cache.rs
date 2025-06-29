use std::{cell::RefCell, ptr::NonNull};

use std::sync::Arc;
use tracing::trace;

use super::pager::PageRef;

const DEFAULT_PAGE_CACHE_SIZE_IN_PAGES: usize = 2000;

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct PageCacheKey {
    pgno: usize,
}

#[allow(dead_code)]
struct PageCacheEntry {
    key: PageCacheKey,
    page: PageRef,
    use_bit: std::cell::Cell<bool>,
}

#[derive(Debug)]
pub struct DumbLruPageCache {
    capacity: usize,
    map: RefCell<PageHashMap>,
    clock_hand: RefCell<usize>,
    pages: RefCell<Vec<Option<NonNull<PageCacheEntry>>>>,
    free_list: RefCell<Vec<usize>>,
}
unsafe impl Send for DumbLruPageCache {}
unsafe impl Sync for DumbLruPageCache {}

#[derive(Debug)]
struct PageHashMap {
    // FIXME: do we prefer array buckets or list? Deletes will be slower here which I guess happens often. I will do this for now to test how well it does.
    buckets: Vec<Vec<HashMapNode>>,
    capacity: usize,
    size: usize,
}

#[derive(Clone, Debug)]
struct HashMapNode {
    key: PageCacheKey,
    value: NonNull<PageCacheEntry>,
    idx: usize,
}

#[derive(Debug, PartialEq)]
pub enum CacheError {
    InternalError(String),
    Locked,
    Dirty { pgno: usize },
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
impl DumbLruPageCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity of cache should be at least 1");
        assert!(capacity < usize::MAX / 2, "capacity of cache is too big");
        Self {
            capacity,
            map: RefCell::new(PageHashMap::new(capacity)),
            clock_hand: RefCell::new(0),
            pages: RefCell::new(Vec::with_capacity(capacity)),
            free_list: RefCell::new(Vec::new()),
        }
    }

    pub fn contains_key(&mut self, key: &PageCacheKey) -> bool {
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

        if !ignore_exists {
            if let Some(existing_page_ref) = self.get(&key) {
                assert!(
                    Arc::ptr_eq(&value, &existing_page_ref),
                    "Attempted to insert different page with same key: {:?}",
                    key
                );
                return Err(CacheError::KeyExists);
            }
        }
        self.make_room_for(1)?;
        let entry = Box::new(PageCacheEntry {
            key: key.clone(),
            page: value,
            use_bit: std::cell::Cell::new(true),
        });
        let ptr_raw = Box::into_raw(entry);
        let ptr = unsafe { NonNull::new_unchecked(ptr_raw) };

        let index_to_insert = if let Some(free_index) = self.free_list.borrow_mut().pop() {
            self.pages.borrow_mut()[free_index] = Some(ptr);
            free_index
        } else {
            let pages_len = self.pages.borrow().len();
            self.pages.borrow_mut().push(Some(ptr));
            pages_len
        };

        self.map.borrow_mut().insert(key, ptr, index_to_insert);
        Ok(())
    }

    pub fn delete(&mut self, key: PageCacheKey) -> Result<(), CacheError> {
        trace!("cache_delete(key={:?})", key);
        self._delete(key, true)
    }

    // Returns Ok if key is not found
    pub fn _delete(&mut self, key: PageCacheKey, clean_page: bool) -> Result<(), CacheError> {
        let Some((mut ptr, index_to_free)) = self.map.borrow_mut().remove_with_index(&key) else {
            return Ok(());
        };
        let entry_mut = unsafe { ptr.as_mut() };
        if entry_mut.page.is_locked() {
            self.map.borrow_mut().insert(key, ptr, index_to_free);
            return Err(CacheError::Locked);
        }
        if entry_mut.page.is_dirty() {
            self.map.borrow_mut().insert(key, ptr, index_to_free);
            return Err(CacheError::Dirty {
                pgno: entry_mut.page.get().id,
            });
        }

        if clean_page {
            entry_mut.page.clear_loaded();
            let _ = entry_mut.page.get().contents.take();
        }

        self.pages.borrow_mut()[index_to_free] = None;

        self.free_list.borrow_mut().push(index_to_free);

        unsafe {
            let _ = Box::from_raw(ptr.as_ptr());
        };
        Ok(())
    }

    fn get_ptr(&mut self, key: &PageCacheKey) -> Option<NonNull<PageCacheEntry>> {
        let m = self.map.borrow_mut();
        let ptr = m.get(key);
        ptr.copied()
    }

    pub fn get(&mut self, key: &PageCacheKey) -> Option<PageRef> {
        self.peek(key)
    }

    /// Get page without promoting entry
    pub fn peek(&mut self, key: &PageCacheKey) -> Option<PageRef> {
        trace!("cache_get(key={:?})", key);
        let mut ptr = self.get_ptr(key)?;
        let page = unsafe {
            let entry = ptr.as_mut();
            entry.use_bit.set(true);
            entry.page.clone()
        };
        Some(page)
    }

    pub fn resize(&mut self, capacity: usize) -> CacheResizeResult {
        let new_map = self.map.borrow().rehash(capacity);
        self.map.replace(new_map);
        self.capacity = capacity;
        match self.make_room_for(0) {
            Ok(_) => CacheResizeResult::Done,
            Err(_) => CacheResizeResult::PendingEvictions,
        }
    }

    pub fn make_room_for(&mut self, n: usize) -> Result<(), CacheError> {
        let mut need_to_evict = (self.len() + n).saturating_sub(self.capacity);

        while need_to_evict > 0 {
            let mut key_to_delete: Option<PageCacheKey> = None;
            let mut evict_failed = false;

            {
                let mut clock_hand = self.clock_hand.borrow_mut();
                let pages = self.pages.borrow();
                let len = pages.len();

                if len == 0 {
                    return Err(CacheError::InternalError(
                        "Cache empty but needs eviction".to_string(),
                    ));
                }

                for _ in 0..(len * 2) {
                    *clock_hand %= len;
                    let Some(pointer) = &pages[*clock_hand] else {
                        *clock_hand += 1;
                        continue;
                    };

                    let entry = unsafe { pointer.as_ref() };

                    if entry.use_bit.get() {
                        entry.use_bit.set(false);
                        *clock_hand += 1;
                    } else {
                        key_to_delete = Some(entry.key.clone());
                        break;
                    }
                }
            }

            if let Some(key) = key_to_delete {
                let delete_res = self._delete(key, true);
                if delete_res.is_ok() {
                    need_to_evict -= 1;
                } else {
                    let mut clock_hand = self.clock_hand.borrow_mut();
                    *clock_hand += 1;
                    evict_failed = true;
                }
            } else {
                return Err(CacheError::Full);
            }

            if evict_failed {
                continue;
            }
        }

        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), CacheError> {
        self.map.borrow_mut().clear();
        for ptr in self.pages.borrow_mut().drain(..) {
            let Some(ptr) = ptr else { continue };
            let entry = unsafe { ptr.as_ref() };

            if entry.page.is_locked() {
                return Err(CacheError::Locked);
            }
            if entry.page.is_dirty() {
                return Err(CacheError::Dirty {
                    pgno: entry.page.get().id,
                });
            }

            unsafe {
                let _ = Box::from_raw(ptr.as_ptr());
            }
        }

        *self.clock_hand.borrow_mut() = 0;

        assert!(self.pages.borrow().is_empty());
        assert!(self.map.borrow().is_empty());

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.map.borrow().len()
    }

    pub fn unset_dirty_all_pages(&mut self) {
        for node in self.map.borrow_mut().iter_mut() {
            unsafe {
                let entry = node.value.as_mut();
                entry.page.clear_dirty()
            };
        }
    }
}

impl Default for DumbLruPageCache {
    fn default() -> Self {
        DumbLruPageCache::new(DEFAULT_PAGE_CACHE_SIZE_IN_PAGES)
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

    pub fn clear(&mut self) {
        for bucket in self.buckets.iter_mut() {
            bucket.clear();
        }
        self.size = 0;
    }

    /// Insert page into hashmap. If a key was already in the hashmap, then update it and return the previous value.
    pub fn insert(
        &mut self,
        key: PageCacheKey,
        value: NonNull<PageCacheEntry>,
        clock_idx: usize,
    ) -> Option<NonNull<PageCacheEntry>> {
        let bucket = self.hash(&key);
        let bucket = &mut self.buckets[bucket];
        let mut idx = 0;
        while let Some(node) = bucket.get_mut(idx) {
            if node.key == key {
                let prev = node.value;
                node.value = value;
                return Some(prev);
            }
            idx += 1;
        }
        bucket.push(HashMapNode {
            key,
            value,
            idx: clock_idx,
        });
        self.size += 1;
        None
    }

    pub fn remove_with_index(
        &mut self,
        key: &PageCacheKey,
    ) -> Option<(NonNull<PageCacheEntry>, usize)> {
        let bucket = self.hash(key);
        let bucket = &mut self.buckets[bucket];
        for i in 0..bucket.len() {
            if bucket[i].key == *key {
                let removed_node = bucket.remove(i);
                self.size -= 1;
                return Some((removed_node.value, removed_node.idx));
            }
        }
        None
    }

    pub fn contains_key(&self, key: &PageCacheKey) -> bool {
        let bucket = self.hash(key);
        self.buckets[bucket].iter().any(|node| node.key == *key)
    }

    pub fn get(&self, key: &PageCacheKey) -> Option<&NonNull<PageCacheEntry>> {
        let bucket = self.hash(key);
        let bucket = &self.buckets[bucket];
        let mut idx = 0;
        while let Some(node) = bucket.get(idx) {
            if node.key == *key {
                return Some(&node.value);
            }
            idx += 1;
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn iter(&self) -> impl Iterator<Item = &HashMapNode> {
        self.buckets.iter().flat_map(|bucket| bucket.iter())
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut HashMapNode> {
        self.buckets.iter_mut().flat_map(|bucket| bucket.iter_mut())
    }

    fn hash(&self, key: &PageCacheKey) -> usize {
        key.pgno % self.capacity
    }

    pub fn rehash(&self, new_capacity: usize) -> PageHashMap {
        let mut new_hash_map = PageHashMap::new(new_capacity);
        for node in self.iter() {
            new_hash_map.insert(node.key.clone(), node.value, node.idx);
        }
        new_hash_map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{Buffer, BufferData};
    use crate::storage::pager::{Page, PageRef};
    use crate::storage::sqlite3_ondisk::PageContent;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };
    use std::{cell::RefCell, pin::Pin, rc::Rc, sync::Arc};

    // --- Test Helpers ---

    fn create_key(id: usize) -> PageCacheKey {
        PageCacheKey::new(id)
    }

    #[allow(clippy::arc_with_non_send_sync)]
    pub fn page_with_content(page_id: usize) -> PageRef {
        let page = Arc::new(Page::new(page_id));
        {
            let buffer_drop_fn = Rc::new(|_data: BufferData| {});
            let buffer = Buffer::new(Pin::new(vec![0; 4096]), buffer_drop_fn);
            let page_content = PageContent {
                offset: 0,
                buffer: Arc::new(RefCell::new(buffer)),
                overflow_cells: Vec::new(),
            };
            page.get().contents = Some(page_content);
            page.set_loaded();
        }
        page
    }

    fn insert_page(cache: &mut DumbLruPageCache, id: usize) -> PageCacheKey {
        let key = create_key(id);
        let page = page_with_content(id);
        assert!(cache.insert(key.clone(), page).is_ok());
        key
    }

    fn page_has_content(page: &PageRef) -> bool {
        page.is_loaded() && page.get().contents.is_some()
    }

    // --- Basic Tests ---

    #[test]
    fn test_page_cache_insert_and_get() {
        let mut cache = DumbLruPageCache::new(10);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.get(&key1).unwrap().get().id, 1);
        assert_eq!(cache.get(&key2).unwrap().get().id, 2);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let mut cache = DumbLruPageCache::new(10);
        insert_page(&mut cache, 1);
        let key_nonexist = create_key(99);
        assert!(cache.delete(key_nonexist).is_ok());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    #[should_panic(expected = "Attempted to insert different page with same key")]
    fn test_insert_existing_key_fail() {
        let mut cache = DumbLruPageCache::new(10);
        let key1 = create_key(1);
        let page1_v1 = page_with_content(1);
        let page1_v2 = page_with_content(1);
        assert!(cache.insert(key1.clone(), page1_v1.clone()).is_ok());
        assert_eq!(cache.len(), 1);
        let _ = cache.insert(key1.clone(), page1_v2.clone()); // Should panic
    }

    // --- Deletion and Eviction Tests ---

    #[test]
    fn test_page_cache_delete() {
        let mut cache = DumbLruPageCache::new(10);
        let key1 = insert_page(&mut cache, 1);
        assert!(cache.get(&key1).is_some());
        assert!(cache.delete(key1.clone()).is_ok());
        assert!(cache.get(&key1).is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_delete_cleans_page_content() {
        let mut cache = DumbLruPageCache::new(10);
        let key = insert_page(&mut cache, 1);
        let page = cache.peek(&key).unwrap();

        // Hold a reference to the page to check its state after deletion from cache
        let page_clone = page.clone();
        assert!(page_has_content(&page_clone));

        // Delete from cache with cleaning enabled
        assert!(cache.delete(key.clone()).is_ok());
        assert_eq!(cache.len(), 0);

        // The page content should have been cleared
        assert!(!page_has_content(&page_clone));
        assert!(!page_clone.is_loaded());
    }

    #[test]
    fn test_page_cache_clear() {
        let mut cache = DumbLruPageCache::new(10);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert!(cache.clear().is_ok());
        assert!(cache.get(&key1).is_none());
        assert!(cache.get(&key2).is_none());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_page_cache_over_capacity_eviction() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // At this point, keys 1 and 2 are in the cache, and their use_bit is true.
        // Let's get key1 to keep its use_bit as true.
        let _ = cache.get(&key1);

        // Now, inserting key3 should trigger an eviction.
        // The clock hand starts at 0.
        // 1. Looks at page 1. use_bit is true. Sets to false. Advances hand.
        // 2. Looks at page 2. use_bit is true. Sets to false. Advances hand.
        // 3. Looks at page 1 again. use_bit is now false. Evicts page 1.
        let key3 = insert_page(&mut cache, 3);

        assert_eq!(cache.len(), 2);
        assert!(cache.get(&key1).is_none(), "Key 1 should have been evicted");
        assert!(cache.get(&key2).is_some(), "Key 2 should still be in cache");
        assert!(cache.get(&key3).is_some(), "Key 3 should be in cache");
    }

    #[test]
    fn test_delete_locked_page() {
        let mut cache = DumbLruPageCache::new(10);
        let key = insert_page(&mut cache, 1);
        let page = cache.peek(&key).unwrap();
        page.set_locked();

        assert_eq!(cache.delete(key.clone()), Err(CacheError::Locked));
        assert_eq!(cache.len(), 1, "Cache should still contain the page");
        assert!(cache.get(&key).is_some(), "Page should still be accessible");
    }

    #[test]
    fn test_delete_dirty_page() {
        let mut cache = DumbLruPageCache::new(10);
        let key = insert_page(&mut cache, 1);
        let page = cache.peek(&key).unwrap();
        page.set_dirty();

        assert_eq!(
            cache.delete(key.clone()),
            Err(CacheError::Dirty { pgno: 1 })
        );
        assert_eq!(cache.len(), 1, "Cache should still contain the page");
    }

    #[test]
    #[ignore = "for now let's not track active refs"]
    fn test_delete_with_active_references() {
        let mut cache = DumbLruPageCache::new(10);
        let key = create_key(1);
        let page = page_with_content(1);
        cache.insert(key.clone(), page.clone()).unwrap();

        // page is still held outside, but our cache doesn't track this.
        // This test is here to match the LRU suite, but may need
        // a different design to be meaningful.
        let result = cache.delete(key.clone());
        assert!(result.is_ok());
    }

    // --- Resize Tests ---

    #[test]
    fn test_resize_smaller_success() {
        let mut cache = DumbLruPageCache::new(5);
        for i in 1..=5 {
            let _ = insert_page(&mut cache, i);
        }
        assert_eq!(cache.len(), 5);

        let result = cache.resize(3);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.capacity, 3);

        // Check that the most recently inserted pages remain
        assert!(cache.get(&create_key(1)).is_none());
        assert!(cache.get(&create_key(2)).is_none());
        assert!(cache.get(&create_key(3)).is_some());
        assert!(cache.get(&create_key(4)).is_some());
        assert!(cache.get(&create_key(5)).is_some());
    }

    #[test]
    fn test_resize_larger() {
        let mut cache = DumbLruPageCache::new(2);
        let _ = insert_page(&mut cache, 1);
        let _ = insert_page(&mut cache, 2);
        assert_eq!(cache.len(), 2);

        let result = cache.resize(5);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.capacity, 5);

        assert!(cache.get(&create_key(1)).is_some());
        assert!(cache.get(&create_key(2)).is_some());

        for i in 3..=5 {
            let _ = insert_page(&mut cache, i);
        }
        assert_eq!(cache.len(), 5);
    }

    #[test]
    fn test_resize_same_capacity() {
        let mut cache = DumbLruPageCache::new(3);
        for i in 1..=3 {
            let _ = insert_page(&mut cache, i);
        }

        let result = cache.resize(3);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.capacity, 3);

        let _ = insert_page(&mut cache, 4);
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_page_cache_insert_sequential() {
        let mut cache = DumbLruPageCache::new(100);
        for i in 0..1000 {
            let key = insert_page(&mut cache, i);
            assert_eq!(cache.peek(&key).unwrap().get().id, i);
            assert!(cache.len() <= 100);
        }
    }

    #[test]
    fn test_page_cache_fuzz() {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        println!("Fuzz test seed: {}", seed);

        let capacity = 50;
        let mut cache = DumbLruPageCache::new(capacity);
        let max_page_id = 100;

        for _ in 0..10_000 {
            let op = rng.next_u64() % 3;

            match op {
                0 => {
                    let id = (rng.next_u64() % max_page_id) as usize;
                    let key = create_key(id);
                    if !cache.contains_key(&key) {
                        let _ = cache.insert(key, page_with_content(id));
                    }
                }
                1 => {
                    if cache.len() == 0 {
                        continue;
                    }
                    let id = (rng.next_u64() % max_page_id) as usize;
                    let key = create_key(id);
                    let _ = cache.get(&key);
                }
                2 => {
                    if cache.len() == 0 {
                        continue;
                    }
                    let id = (rng.next_u64() % max_page_id) as usize;
                    let key = create_key(id);
                    let _ = cache.delete(key);
                }
                _ => unreachable!(),
            }

            assert!(
                cache.len() <= cache.capacity,
                "Cache length {} exceeded capacity {}",
                cache.len(),
                cache.capacity
            );
        }
    }

    #[test]
    fn test_hot_and_cold_eviction() {
        let mut cache = DumbLruPageCache::new(10);

        for i in 1..=10 {
            insert_page(&mut cache, i);
        }

        assert!(cache.get(&create_key(5)).is_some());
        assert!(cache.get(&create_key(6)).is_some());

        insert_page(&mut cache, 11);
        insert_page(&mut cache, 12);
        insert_page(&mut cache, 13);

        assert_eq!(cache.len(), 10, "Cache should remain at full capacity");

        assert!(
            cache.get(&create_key(5)).is_some(),
            "Hot page 5 should still be in cache"
        );
        assert!(
            cache.get(&create_key(6)).is_some(),
            "Hot page 6 should still be in cache"
        );

        assert!(
            cache.get(&create_key(1)).is_none()
                || cache.get(&create_key(2)).is_none()
                || cache.get(&create_key(3)).is_none(),
            "At least one of the cold pages should have been evicted"
        );
    }

    #[test]
    fn test_pinned_pages_are_skipped_during_eviction() {
        let mut cache = DumbLruPageCache::new(5);

        for i in 1..=5 {
            insert_page(&mut cache, i);
        }

        cache.peek(&create_key(1)).unwrap().set_dirty();
        cache.peek(&create_key(2)).unwrap().set_dirty();
        cache.peek(&create_key(3)).unwrap().set_dirty();

        insert_page(&mut cache, 6);
        insert_page(&mut cache, 7);

        assert_eq!(cache.len(), 5, "Cache should be at full capacity");

        assert!(
            cache.get(&create_key(1)).is_some(),
            "Pinned page 1 should not be evicted"
        );
        assert!(
            cache.get(&create_key(2)).is_some(),
            "Pinned page 2 should not be evicted"
        );
        assert!(
            cache.get(&create_key(3)).is_some(),
            "Pinned page 3 should not be evicted"
        );

        assert!(
            cache.get(&create_key(4)).is_none(),
            "Page 4 should have been evicted"
        );
        assert!(
            cache.get(&create_key(5)).is_none(),
            "Page 5 should have been evicted"
        );

        assert!(
            cache.get(&create_key(6)).is_some(),
            "New page 6 should be in cache"
        );
        assert!(
            cache.get(&create_key(7)).is_some(),
            "New page 7 should be in cache"
        );
    }

    #[test]
    fn test_delete_and_reinsert_reuses_slots() {
        let mut cache = DumbLruPageCache::new(5);

        for i in 1..=5 {
            insert_page(&mut cache, i);
        }
        assert_eq!(cache.len(), 5);
        assert!(
            cache.free_list.borrow().is_empty(),
            "Free list should be empty initially"
        );

        assert!(cache.delete(create_key(2)).is_ok());
        assert!(cache.delete(create_key(3)).is_ok());
        assert!(cache.delete(create_key(4)).is_ok());

        assert_eq!(cache.len(), 2, "Cache length should be 2 after deletions");
        assert_eq!(
            cache.free_list.borrow().len(),
            3,
            "Free list should contain 3 reusable slots"
        );

        insert_page(&mut cache, 6);
        insert_page(&mut cache, 7);
        insert_page(&mut cache, 8);

        assert_eq!(cache.len(), 5, "Cache should be full again");
        assert!(
            cache.free_list.borrow().is_empty(),
            "Free list should be empty after reuse"
        );

        assert!(
            cache.get(&create_key(1)).is_some(),
            "Original page 1 should still exist"
        );
        assert!(
            cache.get(&create_key(5)).is_some(),
            "Original page 5 should still exist"
        );

        assert!(
            cache.get(&create_key(6)).is_some(),
            "New page 6 should have been added"
        );
        assert!(
            cache.get(&create_key(7)).is_some(),
            "New page 7 should have been added"
        );
        assert!(
            cache.get(&create_key(8)).is_some(),
            "New page 8 should have been added"
        );
    }
}
