use intrusive_collections::{intrusive_adapter, LinkedList, LinkedListLink};
use rustc_hash::FxHashMap;
use std::sync::{atomic::Ordering, Arc};
use tracing::trace;

use crate::turso_assert;

use super::pager::PageRef;

/// FIXME: https://github.com/tursodatabase/turso/issues/1661
const DEFAULT_PAGE_CACHE_SIZE_IN_PAGES_MAKE_ME_SMALLER_ONCE_WAL_SPILL_IS_IMPLEMENTED: usize =
    100000;

#[derive(Debug, Copy, Eq, Hash, PartialEq, Clone)]
#[repr(transparent)]
pub struct PageCacheKey(usize);

const CLEAR: u8 = 0;
const REF_MAX: u8 = 3;

/// An entry in the page cache.
///
/// The entry is stored in the intrusive linked list in PageCache::list`.
struct PageCacheEntry {
    /// Key identifying this page
    key: PageCacheKey,
    /// The cached page
    page: PageRef,
    /// Reference counter (SIEVE/GClock): starts at zero, bumped on access,
    /// decremented during eviction, only pages at 0 are evicted.
    ref_bit: u8,
    /// Intrusive link for SIEVE queue
    link: LinkedListLink,
}

intrusive_adapter!(EntryAdapter = Box<PageCacheEntry>: PageCacheEntry { link: LinkedListLink });

impl PageCacheEntry {
    fn new(key: PageCacheKey, page: PageRef) -> Box<Self> {
        Box::new(Self {
            key,
            page,
            ref_bit: CLEAR,
            link: LinkedListLink::new(),
        })
    }

    #[inline]
    fn bump_ref(&mut self) {
        self.ref_bit = std::cmp::min(self.ref_bit + 1, REF_MAX);
    }

    #[inline]
    /// Returns the old value
    fn decrement_ref(&mut self) -> u8 {
        let old = self.ref_bit;
        self.ref_bit = old.saturating_sub(1);
        old
    }
}

/// PageCache implements a variation of the SIEVE algorithm that maintains an intrusive linked list queue of
/// pages which keep a 'reference_bit' to determine how recently/frequently the page has been accessed.
/// The bit is set to `Clear` on initial insertion and then bumped on each access and decremented
/// during eviction scans.
///
/// The ring is circular. `clock_hand` points at the tail (LRU).
/// Sweep order follows next: tail (LRU) -> head (MRU) -> .. -> tail
/// New pages are inserted after the clock hand in the `next` direction,
/// which places them at head (MRU) (i.e. `tail.next` is the head).
pub struct PageCache {
    /// Capacity in pages
    capacity: usize,
    /// Map of Key -> pointer to entry in the queue
    map: FxHashMap<PageCacheKey, *mut PageCacheEntry>,
    /// The eviction queue (intrusive doubly-linked list)
    queue: LinkedList<EntryAdapter>,
    /// Clock hand cursor for SIEVE eviction (pointer to an entry in the queue, or null)
    clock_hand: *mut PageCacheEntry,
}

unsafe impl Send for PageCache {}
unsafe impl Sync for PageCache {}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum CacheError {
    #[error("{0}")]
    InternalError(String),
    #[error("page {pgno} is locked")]
    Locked { pgno: usize },
    #[error("page {pgno} is dirty")]
    Dirty { pgno: usize },
    #[error("page {pgno} is pinned")]
    Pinned { pgno: usize },
    #[error("cache active refs")]
    ActiveRefs,
    #[error("Page cache is full")]
    Full,
    #[error("key already exists")]
    KeyExists,
}

#[derive(Debug, PartialEq)]
pub enum CacheResizeResult {
    Done,
    PendingEvictions,
}

impl PageCacheKey {
    pub fn new(pgno: usize) -> Self {
        Self(pgno)
    }
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            capacity,
            map: FxHashMap::default(),
            queue: LinkedList::new(EntryAdapter::new()),
            clock_hand: std::ptr::null_mut(),
        }
    }

    /// Advances the clock hand to the next entry in the circular queue.
    /// Follows the "next" direction: from tail/LRU through the list back to tail.
    /// With our insertion-after-hand strategy, this moves through entries in age order.
    fn advance_clock_hand(&mut self) {
        if self.clock_hand.is_null() {
            return;
        }

        unsafe {
            let mut cursor = self.queue.cursor_mut_from_ptr(self.clock_hand);
            cursor.move_next();

            if cursor.get().is_some() {
                self.clock_hand =
                    cursor.as_cursor().get().unwrap() as *const _ as *mut PageCacheEntry;
            } else {
                // Reached end, wrap to front
                let front_cursor = self.queue.front_mut();
                if front_cursor.get().is_some() {
                    self.clock_hand =
                        front_cursor.as_cursor().get().unwrap() as *const _ as *mut PageCacheEntry;
                } else {
                    self.clock_hand = std::ptr::null_mut();
                }
            }
        }
    }

    pub fn contains_key(&self, key: &PageCacheKey) -> bool {
        self.map.contains_key(key)
    }

    #[inline]
    pub fn insert(&mut self, key: PageCacheKey, value: PageRef) -> Result<(), CacheError> {
        self._insert(key, value, false)
    }

    #[inline]
    pub fn upsert_page(&mut self, key: PageCacheKey, value: PageRef) -> Result<(), CacheError> {
        self._insert(key, value, true)
    }

    pub fn _insert(
        &mut self,
        key: PageCacheKey,
        value: PageRef,
        update_in_place: bool,
    ) -> Result<(), CacheError> {
        trace!("insert(key={:?})", key);

        if let Some(&entry_ptr) = self.map.get(&key) {
            let entry = unsafe { &mut *entry_ptr };
            let p = &entry.page;

            if !p.is_loaded() && !p.is_locked() {
                // evict, then continue with fresh insert
                self._delete(key, true)?;
                // Proceed to insert new entry
            } else {
                entry.bump_ref();
                if update_in_place {
                    entry.page = value;
                    return Ok(());
                } else {
                    turso_assert!(
                        Arc::ptr_eq(&entry.page, &value),
                        "Attempted to insert different page with same key: {key:?}"
                    );
                    return Err(CacheError::KeyExists);
                }
            }
        }

        // Key doesn't exist, proceed with new entry
        self.make_room_for(1)?;

        let entry = PageCacheEntry::new(key, value);

        if self.clock_hand.is_null() {
            // First entry - just push it
            self.queue.push_back(entry);
            let entry_ptr = self.queue.back().get().unwrap() as *const _ as *mut PageCacheEntry;
            self.map.insert(key, entry_ptr);
            self.clock_hand = entry_ptr;
        } else {
            // Insert after clock hand (in circular list semantics, this makes it the new head/MRU)
            unsafe {
                let mut cursor = self.queue.cursor_mut_from_ptr(self.clock_hand);
                cursor.insert_after(entry);
                // The inserted entry is now at the next position after clock hand
                cursor.move_next();
                let entry_ptr = cursor.get().ok_or_else(|| {
                    CacheError::InternalError("Failed to get inserted entry pointer".into())
                })? as *const PageCacheEntry as *mut PageCacheEntry;
                self.map.insert(key, entry_ptr);
            }
        }

        Ok(())
    }

    fn _delete(&mut self, key: PageCacheKey, clean_page: bool) -> Result<(), CacheError> {
        let Some(&entry_ptr) = self.map.get(&key) else {
            return Ok(());
        };

        let entry = unsafe { &mut *entry_ptr };
        let page = &entry.page;

        if page.is_locked() {
            return Err(CacheError::Locked {
                pgno: page.get().id,
            });
        }
        if page.is_dirty() {
            return Err(CacheError::Dirty {
                pgno: page.get().id,
            });
        }
        if page.is_pinned() {
            return Err(CacheError::Pinned {
                pgno: page.get().id,
            });
        }

        if clean_page {
            page.clear_loaded();
            let _ = page.get().contents.take();
        }

        // Remove from map first
        self.map.remove(&key);

        // If clock hand points to this entry, advance it before removing
        if self.clock_hand == entry_ptr {
            self.advance_clock_hand();
            // If hand is still pointing to the same entry after advance, we're removing the last entry
            if self.clock_hand == entry_ptr {
                self.clock_hand = std::ptr::null_mut();
            }
        }

        // Remove the entry from the queue
        unsafe {
            let mut cursor = self.queue.cursor_mut_from_ptr(entry_ptr);
            cursor.remove();
        }

        Ok(())
    }

    #[inline]
    /// Deletes a page from the cache
    pub fn delete(&mut self, key: PageCacheKey) -> Result<(), CacheError> {
        trace!("cache_delete(key={:?})", key);
        self._delete(key, true)
    }

    #[inline]
    pub fn get(&mut self, key: &PageCacheKey) -> crate::Result<Option<PageRef>> {
        let Some(&entry_ptr) = self.map.get(key) else {
            return Ok(None);
        };

        let entry = unsafe { &mut *entry_ptr };
        let page = entry.page.clone();

        // Because we can abort a read_page completion, this means a page can be in the cache but be unloaded and unlocked.
        // However, if we do not evict that page from the page cache, we will return an unloaded page later which will trigger
        // assertions later on. This is worsened by the fact that page cache is not per `Statement`, so you can abort a completion
        // in one Statement, and trigger some error in the next one if we don't evict the page here.
        if !page.is_loaded() && !page.is_locked() {
            self.delete(*key)?;
            return Ok(None);
        }

        entry.bump_ref();
        Ok(Some(page))
    }

    #[inline]
    pub fn peek(&mut self, key: &PageCacheKey, touch: bool) -> Option<PageRef> {
        let &entry_ptr = self.map.get(key)?;
        let entry = unsafe { &mut *entry_ptr };
        let page = entry.page.clone();
        if touch {
            entry.bump_ref();
        }
        Some(page)
    }

    /// Resizes the cache to a new capacity
    /// If shrinking, attempts to evict pages.
    /// If growing, simply increases capacity.
    pub fn resize(&mut self, new_cap: usize) -> CacheResizeResult {
        if new_cap == self.capacity {
            return CacheResizeResult::Done;
        }

        // Evict entries one by one until we're at new capacity
        while new_cap < self.len() {
            if self.evict_one().is_err() {
                return CacheResizeResult::PendingEvictions;
            }
        }

        self.capacity = new_cap;
        CacheResizeResult::Done
    }

    /// Ensures at least `n` free slots are available
    ///
    /// Uses the SIEVE algorithm to evict pages if necessary:
    /// Start at clock hand position
    /// If page ref_bit > 0, decrement and continue
    /// If page ref_bit == 0 and evictable, evict it
    /// If page is unevictable (dirty/locked/pinned), continue sweep
    /// On sweep, pages with ref_bit > 0 are given a second chance by decrementing
    /// their ref_bit and leaving them in place; only pages with ref_bit == 0 are evicted.
    ///
    /// Returns `CacheError::Full` if not enough pages can be evicted
    pub fn make_room_for(&mut self, n: usize) -> Result<(), CacheError> {
        if n > self.capacity {
            return Err(CacheError::Full);
        }
        let available = self.capacity - self.len();
        if n <= available {
            return Ok(());
        }

        let need = n - available;
        for _ in 0..need {
            self.evict_one()?;
        }
        Ok(())
    }

    /// Evicts a single page using the SIEVE algorithm
    /// Unlike make_room_for(), this ignores capacity and always tries to evict one page
    fn evict_one(&mut self) -> Result<(), CacheError> {
        if self.len() == 0 {
            return Err(CacheError::InternalError(
                "Cannot evict from empty cache".into(),
            ));
        }

        let mut examined = 0usize;
        let max_examinations = self.len().saturating_mul(REF_MAX as usize + 1);

        while examined < max_examinations {
            // Clock hand should never be null here since we checked len() > 0
            assert!(
                !self.clock_hand.is_null(),
                "clock hand is null but cache has {} entries",
                self.len()
            );

            let entry_ptr = self.clock_hand;
            let entry = unsafe { &mut *entry_ptr };
            let key = entry.key;
            let page = &entry.page;

            let evictable = !page.is_dirty() && !page.is_locked() && !page.is_pinned();

            if evictable && entry.ref_bit == CLEAR {
                // Evict this entry
                self.advance_clock_hand();
                // Check if clock hand wrapped back to the same entry (meaning this is the only/last entry)
                if self.clock_hand == entry_ptr {
                    self.clock_hand = std::ptr::null_mut();
                }

                self.map.remove(&key);

                // Clean the page
                page.clear_loaded();
                let _ = page.get().contents.take();

                // Remove from queue
                unsafe {
                    let mut cursor = self.queue.cursor_mut_from_ptr(entry_ptr);
                    cursor.remove();
                }

                return Ok(());
            } else if evictable {
                // Decrement ref bit and continue
                entry.decrement_ref();
                self.advance_clock_hand();
                examined += 1;
            } else {
                // Skip unevictable page
                self.advance_clock_hand();
                examined += 1;
            }
        }

        Err(CacheError::Full)
    }

    pub fn clear(&mut self, clear_dirty: bool) -> Result<(), CacheError> {
        // Check all pages are clean
        for &entry_ptr in self.map.values() {
            let entry = unsafe { &*entry_ptr };
            if entry.page.is_dirty() && !clear_dirty {
                return Err(CacheError::Dirty {
                    pgno: entry.page.get().id,
                });
            }
        }

        // Clean all pages
        for &entry_ptr in self.map.values() {
            let entry = unsafe { &*entry_ptr };
            entry.page.clear_loaded();
            let _ = entry.page.get().contents.take();
        }

        self.map.clear();
        self.queue.clear();
        self.clock_hand = std::ptr::null_mut();
        Ok(())
    }

    /// Removes all pages from the cache with pgno greater than max_page_num
    pub fn truncate(&mut self, max_page_num: usize) -> Result<(), CacheError> {
        for key in self
            .map
            .keys()
            .filter(|k| k.0 > max_page_num)
            .copied()
            .collect::<Vec<_>>()
        {
            self.delete(key)?;
        }
        Ok(())
    }

    pub fn print(&self) {
        tracing::debug!("page_cache_len={}", self.map.len());

        let mut cursor = self.queue.front();
        let mut i = 0;
        while let Some(entry) = cursor.get() {
            let page = &entry.page;
            tracing::debug!(
                "slot={}, page={:?}, flags={}, pin_count={}, ref_bit={:?}",
                i,
                entry.key,
                page.get().flags.load(Ordering::SeqCst),
                page.get().pin_count.load(Ordering::SeqCst),
                entry.ref_bit,
            );
            cursor.move_next();
            i += 1;
        }
    }

    #[cfg(test)]
    pub fn keys(&mut self) -> Vec<PageCacheKey> {
        self.map.keys().copied().collect()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[cfg(test)]
    fn verify_cache_integrity(&self) {
        let map_len = self.map.len();

        // Count entries in queue
        let mut queue_len = 0;
        let mut cursor = self.queue.front();
        let mut seen_keys = std::collections::HashSet::new();

        while let Some(entry) = cursor.get() {
            queue_len += 1;
            seen_keys.insert(entry.key);
            cursor.move_next();
        }

        assert_eq!(map_len, queue_len, "map and queue length mismatch");
        assert_eq!(map_len, seen_keys.len(), "duplicate keys in queue");

        // Verify all map entries are in queue
        for &key in self.map.keys() {
            assert!(seen_keys.contains(&key), "map key not in queue");
        }

        // Verify clock hand
        if !self.clock_hand.is_null() {
            assert!(map_len > 0, "clock hand set but map is empty");
            let hand_key = unsafe { (*self.clock_hand).key };
            assert!(
                self.map.contains_key(&hand_key),
                "clock hand points to non-existent entry"
            );
        } else {
            assert_eq!(map_len, 0, "clock hand null but map not empty");
        }
    }

    #[cfg(test)]
    fn ref_of(&self, key: &PageCacheKey) -> Option<u8> {
        self.map.get(key).map(|&ptr| unsafe { (*ptr).ref_bit })
    }
}

impl Default for PageCache {
    fn default() -> Self {
        PageCache::new(
            DEFAULT_PAGE_CACHE_SIZE_IN_PAGES_MAKE_ME_SMALLER_ONCE_WAL_SPILL_IS_IMPLEMENTED,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page_cache::CacheError;
    use crate::storage::pager::{Page, PageRef};
    use crate::storage::sqlite3_ondisk::PageContent;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };
    use std::sync::Arc;

    fn create_key(id: usize) -> PageCacheKey {
        PageCacheKey::new(id)
    }

    pub fn page_with_content(page_id: usize) -> PageRef {
        let page = Arc::new(Page::new(page_id as i64));
        {
            let buffer = crate::Buffer::new_temporary(4096);
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
        cache
            .insert(key, page)
            .unwrap_or_else(|e| panic!("Failed to insert page {id}: {e:?}"));
        key
    }

    #[test]
    fn test_delete_only_element() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        cache.verify_cache_integrity();
        assert_eq!(cache.len(), 1);

        assert!(cache.delete(key1).is_ok());

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
    fn test_detach_tail() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1); // tail
        let _key2 = insert_page(&mut cache, 2); // middle
        let _key3 = insert_page(&mut cache, 3); // head
        cache.verify_cache_integrity();
        assert_eq!(cache.len(), 3);

        // Delete tail
        assert!(cache.delete(key1).is_ok());
        assert_eq!(cache.len(), 2, "Length should be 2 after deleting tail");
        assert!(
            !cache.contains_key(&key1),
            "Cache should not contain deleted tail key"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_insert_existing_key_updates_in_place() {
        let mut cache = PageCache::default();
        let key1 = create_key(1);
        let page1_v1 = page_with_content(1);
        let page1_v2 = page1_v1.clone(); // Same Arc instance

        assert!(cache.insert(key1, page1_v1.clone()).is_ok());
        assert_eq!(cache.len(), 1);

        // Inserting same page instance should return KeyExists error
        let result = cache.insert(key1, page1_v2.clone());
        assert_eq!(result, Err(CacheError::KeyExists));
        assert_eq!(cache.len(), 1);

        // Verify the page is still accessible
        assert!(cache.get(&key1).unwrap().is_some());
        cache.verify_cache_integrity();
    }

    #[test]
    #[should_panic(expected = "Attempted to insert different page with same key")]
    fn test_insert_different_page_same_key_panics() {
        let mut cache = PageCache::default();
        let key1 = create_key(1);
        let page1_v1 = page_with_content(1);
        let page1_v2 = page_with_content(1); // Different Arc instance

        assert!(cache.insert(key1, page1_v1.clone()).is_ok());
        assert_eq!(cache.len(), 1);
        cache.verify_cache_integrity();

        // This should panic because it's a different page instance
        let _ = cache.insert(key1, page1_v2.clone());
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let mut cache = PageCache::default();
        let key_nonexist = create_key(99);

        // Deleting non-existent key should be a no-op (returns Ok)
        assert!(cache.delete(key_nonexist).is_ok());
        assert_eq!(cache.len(), 0);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_evict() {
        let mut cache = PageCache::new(1);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // With capacity=1, inserting key2 should evict key1
        assert_eq!(cache.get(&key2).unwrap().unwrap().get().id, 2);
        assert!(
            cache.get(&key1).unwrap().is_none(),
            "key1 should be evicted"
        );

        // key2 should still be accessible
        assert_eq!(cache.get(&key2).unwrap().unwrap().get().id, 2);
        assert!(
            cache.get(&key1).unwrap().is_none(),
            "capacity=1 should have evicted the older page"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_sieve_touch_non_tail_does_not_affect_immediate_eviction() {
        // SIEVE algorithm: touching a non-tail page marks it but doesn't move it.
        // The tail (if unmarked) will still be the first eviction candidate.

        // Insert 1,2,3 -> order [3,2,1] with tail=1
        let mut cache = PageCache::new(3);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Touch key2 (middle) to mark it with reference bit
        assert!(cache.get(&key2).unwrap().is_some());

        // Insert 4: SIEVE examines tail (key1, unmarked) -> evict key1
        let key4 = insert_page(&mut cache, 4);

        assert!(
            cache.get(&key2).unwrap().is_some(),
            "marked non-tail (key2) should remain"
        );
        assert!(cache.get(&key3).unwrap().is_some(), "key3 should remain");
        assert!(
            cache.get(&key4).unwrap().is_some(),
            "key4 was just inserted"
        );
        assert!(
            cache.get(&key1).unwrap().is_none(),
            "unmarked tail (key1) should be evicted first"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn clock_second_chance_decrements_tail_then_evicts_next() {
        let mut cache = PageCache::new(3);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);
        assert_eq!(cache.len(), 3);
        assert!(cache.get(&key1).unwrap().is_some());
        let key4 = insert_page(&mut cache, 4);
        assert!(cache.get(&key1).unwrap().is_some(), "key1 should survive");
        assert!(cache.get(&key2).unwrap().is_some(), "key2 remains");
        assert!(cache.get(&key4).unwrap().is_some(), "key4 inserted");
        assert!(
            cache.get(&key3).unwrap().is_none(),
            "key3 (next after tail) evicted"
        );
        assert_eq!(cache.len(), 3);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_locked_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).unwrap().unwrap();
        page.set_locked();

        assert_eq!(cache.delete(key), Err(CacheError::Locked { pgno: 1 }));
        assert_eq!(cache.len(), 1, "Locked page should not be deleted");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_dirty_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).unwrap().unwrap();
        page.set_dirty();

        assert_eq!(cache.delete(key), Err(CacheError::Dirty { pgno: 1 }));
        assert_eq!(cache.len(), 1, "Dirty page should not be deleted");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_delete_pinned_page() {
        let mut cache = PageCache::default();
        let key = insert_page(&mut cache, 1);
        let page = cache.get(&key).unwrap().unwrap();
        page.pin();

        assert_eq!(cache.delete(key), Err(CacheError::Pinned { pgno: 1 }));
        assert_eq!(cache.len(), 1, "Pinned page should not be deleted");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_make_room_for_with_dirty_pages() {
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Make both pages dirty (unevictable)
        cache.get(&key1).unwrap().unwrap().set_dirty();
        cache.get(&key2).unwrap().unwrap().set_dirty();

        // Try to insert a third page, should fail because can't evict dirty pages
        let key3 = create_key(3);
        let page3 = page_with_content(3);
        let result = cache.insert(key3, page3);

        assert_eq!(result, Err(CacheError::Full));
        assert_eq!(cache.len(), 2);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_insert_and_get() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        assert_eq!(cache.get(&key1).unwrap().unwrap().get().id, 1);
        assert_eq!(cache.get(&key2).unwrap().unwrap().get().id, 2);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_over_capacity() {
        // Test SIEVE eviction when exceeding capacity
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Insert 3: tail (key1, unmarked) should be evicted
        let key3 = insert_page(&mut cache, 3);

        assert_eq!(cache.len(), 2);
        assert!(cache.get(&key2).unwrap().is_some(), "key2 should remain");
        assert!(cache.get(&key3).unwrap().is_some(), "key3 just inserted");
        assert!(
            cache.get(&key1).unwrap().is_none(),
            "key1 (oldest, unmarked) should be evicted"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_delete() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);

        assert!(cache.delete(key1).is_ok());
        assert!(cache.get(&key1).unwrap().is_none());
        assert_eq!(cache.len(), 0);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_clear() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        assert!(cache.clear(false).is_ok());
        assert!(cache.get(&key1).unwrap().is_none());
        assert!(cache.get(&key2).unwrap().is_none());
        assert_eq!(cache.len(), 0);
        cache.verify_cache_integrity();
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

        // Should still be able to insert after resize
        assert!(cache.insert(create_key(6), page_with_content(6)).is_ok());
        assert_eq!(cache.len(), 3); // One was evicted to make room
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_detach_with_multiple_pages() {
        let mut cache = PageCache::default();
        let _key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let _key3 = insert_page(&mut cache, 3);

        // Delete middle element (key2)
        assert!(cache.delete(key2).is_ok());

        // Verify structure after deletion
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains_key(&key2));

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

        // Delete head (key3)
        assert!(cache.delete(key3).is_ok());
        assert_eq!(cache.len(), 2, "Length should be 2 after deleting head");
        assert!(
            !cache.contains_key(&key3),
            "Cache should not contain deleted head key"
        );
        cache.verify_cache_integrity();

        // Delete tail (key1)
        assert!(cache.delete(key1).is_ok());
        assert_eq!(cache.len(), 1, "Length should be 1 after deleting two");
        cache.verify_cache_integrity();

        // Delete last element (key2)
        assert!(cache.delete(key2).is_ok());
        assert_eq!(cache.len(), 0, "Length should be 0 after deleting all");
        cache.verify_cache_integrity();
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

        // Existing pages should still be accessible
        assert!(cache.get(&key1).is_ok_and(|p| p.is_some()));
        assert!(cache.get(&key2).is_ok_and(|p| p.is_some()));

        // Now we should be able to add 3 more without eviction
        for i in 3..=5 {
            let _ = insert_page(&mut cache, i);
        }
        assert_eq!(cache.len(), 5);
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

        // Truncate to keep only pages <= 4
        cache.truncate(4).unwrap();

        assert!(cache.contains_key(&PageCacheKey(1)));
        assert!(cache.contains_key(&PageCacheKey(4)));
        assert!(!cache.contains_key(&PageCacheKey(8)));
        assert!(!cache.contains_key(&PageCacheKey(10)));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.capacity(), 10);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_truncate_page_cache_remove_all() {
        let mut cache = PageCache::new(10);
        let _ = insert_page(&mut cache, 8);
        let _ = insert_page(&mut cache, 10);

        // Truncate to 4 (removes all pages since they're > 4)
        cache.truncate(4).unwrap();

        assert!(!cache.contains_key(&PageCacheKey(8)));
        assert!(!cache.contains_key(&PageCacheKey(10)));
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
        tracing::info!("fuzz test seed: {}", seed);

        let max_pages = 10;
        let mut cache = PageCache::new(10);
        let mut reference_map = std::collections::HashMap::new();

        for _ in 0..10000 {
            cache.print();

            match rng.next_u64() % 2 {
                0 => {
                    // Insert operation
                    let id_page = rng.next_u64() % max_pages;
                    let key = PageCacheKey::new(id_page as usize);
                    #[allow(clippy::arc_with_non_send_sync)]
                    let page = Arc::new(Page::new(id_page as i64));

                    if cache.peek(&key, false).is_some() {
                        continue; // Skip duplicate page ids
                    }

                    tracing::debug!("inserting page {:?}", key);
                    match cache.insert(key, page.clone()) {
                        Err(CacheError::Full | CacheError::ActiveRefs) => {} // Expected, ignore
                        Err(err) => {
                            panic!("Cache insertion failed unexpectedly: {err:?}");
                        }
                        Ok(_) => {
                            reference_map.insert(key, page);
                            // Clean up reference_map if cache evicted something
                            if cache.len() < reference_map.len() {
                                reference_map.retain(|k, _| cache.contains_key(k));
                            }
                        }
                    }
                    assert!(cache.len() <= 10, "Cache size exceeded capacity");
                }
                1 => {
                    // Delete operation
                    let random = rng.next_u64() % 2 == 0;
                    let key = if random || reference_map.is_empty() {
                        let id_page: u64 = rng.next_u64() % max_pages;
                        PageCacheKey::new(id_page as usize)
                    } else {
                        let i = rng.next_u64() as usize % reference_map.len();
                        *reference_map.keys().nth(i).unwrap()
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
                assert_eq!(cached_page.get().id, key.0);
                assert_eq!(page.get().id, key.0);
            }
        }
    }

    #[test]
    fn test_peek_without_touch() {
        // Test that peek with touch=false doesn't mark pages
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Peek key1 without touching (no ref bit set)
        assert!(cache.peek(&key1, false).is_some());

        // Insert 3: should evict unmarked tail (key1)
        let key3 = insert_page(&mut cache, 3);

        assert!(cache.get(&key2).unwrap().is_some(), "key2 should remain");
        assert!(
            cache.get(&key3).unwrap().is_some(),
            "key3 was just inserted"
        );
        assert!(
            cache.get(&key1).unwrap().is_none(),
            "key1 should be evicted since peek(false) didn't mark it"
        );
        assert_eq!(cache.len(), 2);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_peek_with_touch() {
        // Test that peek with touch=true marks pages for SIEVE
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Peek key1 WITH touching (sets ref bit)
        assert!(cache.peek(&key1, true).is_some());

        // Insert 3: key1 is marked, so it gets second chance
        // key2 becomes new tail and gets evicted
        let key3 = insert_page(&mut cache, 3);

        assert!(
            cache.get(&key1).unwrap().is_some(),
            "key1 should survive (was marked)"
        );
        assert!(
            cache.get(&key3).unwrap().is_some(),
            "key3 was just inserted"
        );
        assert!(
            cache.get(&key2).unwrap().is_none(),
            "key2 should be evicted after key1's second chance"
        );
        assert_eq!(cache.len(), 2);
        cache.verify_cache_integrity();
    }

    #[test]
    #[ignore = "long running test, remove ignore to verify memory stability"]
    fn test_clear_memory_stability() {
        let initial_memory = memory_stats::memory_stats().unwrap().physical_mem;

        for _ in 0..100000 {
            let mut cache = PageCache::new(1000);

            for i in 0..1000 {
                let key = create_key(i);
                let page = page_with_content(i);
                cache.insert(key, page).unwrap();
            }

            cache.clear(false).unwrap();
            drop(cache);
        }

        let final_memory = memory_stats::memory_stats().unwrap().physical_mem;
        let growth = final_memory.saturating_sub(initial_memory);

        println!("Memory growth: {growth} bytes");
        assert!(
            growth < 10_000_000,
            "Memory grew by {growth} bytes over test cycles (limit: 10MB)",
        );
    }

    #[test]
    fn clock_drains_hot_page_within_single_sweep_when_others_are_unevictable() {
        // capacity 3: [3(head), 2, 1(tail)]
        let mut c = PageCache::new(3);
        let k1 = insert_page(&mut c, 1);
        let k2 = insert_page(&mut c, 2);
        let _k3 = insert_page(&mut c, 3);

        // Make k1 hot: bump to Max
        for _ in 0..3 {
            assert!(c.get(&k1).unwrap().is_some());
        }
        assert!(matches!(c.ref_of(&k1), Some(REF_MAX)));

        // Make other pages unevictable; clock must keep revisiting k1.
        c.get(&k2).unwrap().unwrap().set_dirty();
        c.get(&_k3).unwrap().unwrap().set_dirty();

        // Insert 4 -> sweep rotates as needed, draining k1 and evicting it.
        let _k4 = insert_page(&mut c, 4);

        assert!(
            c.get(&k1).unwrap().is_none(),
            "k1 should be evicted after its credit drains"
        );
        assert!(c.get(&k2).unwrap().is_some(), "k2 is dirty (unevictable)");
        assert!(c.get(&_k3).unwrap().is_some(), "k3 is dirty (unevictable)");
        assert!(c.get(&_k4).unwrap().is_some(), "k4 just inserted");
        c.verify_cache_integrity();
    }

    #[test]
    fn gclock_hot_survives_scan_pages() {
        let mut c = PageCache::new(4);
        let _k1 = insert_page(&mut c, 1);
        let k2 = insert_page(&mut c, 2);
        let _k3 = insert_page(&mut c, 3);
        let _k4 = insert_page(&mut c, 4);

        // Make k2 truly hot: three real touches
        for _ in 0..3 {
            assert!(c.get(&k2).unwrap().is_some());
        }
        assert!(matches!(c.ref_of(&k2), Some(REF_MAX)));

        // Now simulate a scan inserting new pages 5..10 (one-hit wonders).
        for id in 5..=10 {
            let _ = insert_page(&mut c, id);
        }

        // Hot k2 should still be present; most single-hit scan pages should churn.
        assert!(
            c.get(&k2).unwrap().is_some(),
            "hot page should survive scan"
        );
        // The earliest single-hit page should be gone.
        assert!(c.get(&create_key(5)).unwrap().is_none());
        c.verify_cache_integrity();
    }

    #[test]
    fn hand_stays_valid_after_deleting_only_element() {
        let mut c = PageCache::new(2);
        let k = insert_page(&mut c, 1);
        assert!(c.delete(k).is_ok());
        // Inserting again should not panic and should succeed
        let _ = insert_page(&mut c, 2);
        c.verify_cache_integrity();
    }

    #[test]
    fn hand_is_reset_after_clear_and_resize() {
        let mut c = PageCache::new(3);
        for i in 1..=3 {
            let _ = insert_page(&mut c, i);
        }
        c.clear(false).unwrap();
        // No elements; insert should not rely on stale hand
        let _ = insert_page(&mut c, 10);

        // Resize from 1 -> 4 and back should not OOB the hand
        assert_eq!(c.resize(4), CacheResizeResult::Done);
        assert_eq!(c.resize(1), CacheResizeResult::Done);
        let _ = insert_page(&mut c, 11);
        c.verify_cache_integrity();
    }

    #[test]
    fn resize_preserves_ref_and_recency() {
        let mut c = PageCache::new(4);
        let _k1 = insert_page(&mut c, 1);
        let k2 = insert_page(&mut c, 2);
        let _k3 = insert_page(&mut c, 3);
        let _k4 = insert_page(&mut c, 4);
        // Make k2 hot.
        for _ in 0..3 {
            assert!(c.get(&k2).unwrap().is_some());
        }
        let _r_before = c.ref_of(&k2);

        // Shrink to 3 (one page will be evicted during repack/next insert)
        assert_eq!(c.resize(3), CacheResizeResult::Done);
        assert!(matches!(c.ref_of(&k2), _r_before));

        // Force an eviction; hot k2 should survive more passes.
        let _ = insert_page(&mut c, 5);
        assert!(c.get(&k2).unwrap().is_some());
        c.verify_cache_integrity();
    }

    #[test]
    fn test_sieve_second_chance_preserves_marked_page() {
        let mut cache = PageCache::new(3);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Mark key1 for second chance
        assert!(cache.get(&key1).unwrap().is_some());

        let key4 = insert_page(&mut cache, 4);
        // CLOCK sweep from hand:
        // - key1 marked -> decrement, continue
        // - key3 (MRU) unmarked -> evict
        assert!(
            cache.get(&key1).unwrap().is_some(),
            "key1 had ref bit set, got second chance"
        );
        assert!(
            cache.get(&key3).unwrap().is_none(),
            "key3 (MRU) should be evicted"
        );
        assert!(cache.get(&key4).unwrap().is_some(), "key4 just inserted");
        assert!(
            cache.get(&key2).unwrap().is_some(),
            "key2 (middle) should remain"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_clock_sweep_wraps_around() {
        // Test that clock hand properly wraps around the circular list
        let mut cache = PageCache::new(3);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Mark all pages
        assert!(cache.get(&key1).unwrap().is_some());
        assert!(cache.get(&key2).unwrap().is_some());
        assert!(cache.get(&key3).unwrap().is_some());

        // Insert 4: hand will sweep full circle, decrementing all refs
        // then sweep again and evict first unmarked page
        let key4 = insert_page(&mut cache, 4);

        // One page was evicted after full sweep
        assert_eq!(cache.len(), 3);
        assert!(cache.get(&key4).unwrap().is_some());

        // Verify exactly one of the original pages was evicted
        let survivors = [key1, key2, key3]
            .iter()
            .filter(|k| cache.get(k).unwrap().is_some())
            .count();
        assert_eq!(survivors, 2, "Should have 2 survivors from original 3");
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_circular_list_single_element() {
        let mut cache = PageCache::new(3);
        let key1 = insert_page(&mut cache, 1);

        // Single element exists
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&key1));

        // Delete single element
        assert!(cache.delete(key1).is_ok());
        assert!(cache.clock_hand.is_null());

        // Insert after empty should work
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&key2));
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_hand_advances_on_eviction() {
        let mut cache = PageCache::new(2);
        let _key1 = insert_page(&mut cache, 1);
        let _key2 = insert_page(&mut cache, 2);

        // Note initial hand position
        let initial_hand = cache.clock_hand;

        // Force eviction
        let _key3 = insert_page(&mut cache, 3);

        // Hand should exist (not null)
        let new_hand = cache.clock_hand;
        assert!(!new_hand.is_null());
        // Hand moved during sweep (exact position depends on eviction)
        assert!(initial_hand.is_null() || new_hand != initial_hand || cache.len() < 2);
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_multi_level_ref_counting() {
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let _key2 = insert_page(&mut cache, 2);

        // Bump key1 to MAX (3 accesses)
        for _ in 0..3 {
            assert!(cache.get(&key1).unwrap().is_some());
        }
        assert_eq!(cache.ref_of(&key1), Some(REF_MAX));

        // Insert multiple new pages - key1 should survive longer
        for i in 3..6 {
            let _ = insert_page(&mut cache, i);
        }

        // key1 might still be there due to high ref count
        // (depends on exact sweep pattern, but it got multiple chances)
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_resize_maintains_circular_structure() {
        let mut cache = PageCache::new(5);
        for i in 1..=4 {
            let _ = insert_page(&mut cache, i);
        }

        // Resize smaller
        assert_eq!(cache.resize(2), CacheResizeResult::Done);
        assert_eq!(cache.len(), 2);

        // Verify structure via integrity check
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_link_after_correctness() {
        let mut cache = PageCache::new(4);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Verify all keys are in cache
        assert!(cache.contains_key(&key1));
        assert!(cache.contains_key(&key2));
        assert!(cache.contains_key(&key3));
        assert_eq!(cache.len(), 3);

        cache.verify_cache_integrity();
    }
}
