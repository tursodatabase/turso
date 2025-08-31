use std::cell::{Cell, RefCell};
use std::sync::atomic::Ordering;

use super::pager::PageRef;
use crate::turso_assert;
use std::sync::Arc;
use tracing::trace;

/// FIXME: https://github.com/tursodatabase/turso/issues/1661
const DEFAULT_PAGE_CACHE_SIZE_IN_PAGES_MAKE_ME_SMALLER_ONCE_WAL_SPILL_IS_IMPLEMENTED: usize =
    100000;

#[derive(Debug, Copy, Eq, Hash, PartialEq, Clone)]
#[repr(transparent)]
pub struct PageCacheKey(usize);

#[derive(Clone)]
struct PageCacheEntry {
    key: PageCacheKey,
    page: Option<PageRef>,
    ref_bit: Cell<bool>,
}

type SlotIndex = usize;

/// Represents a "null" index in the linked lists
const NULL: SlotIndex = usize::MAX;

/// SIEVE-based page cache
///
/// The cache uses a slot-based design with three main components:
/// 1. Slot array (`entries`): Fixed-size vec holding page entries
/// 2. SIEVE queue: Doubly-linked list for eviction order (head=MRU, tail=LRU)
/// 3. Singly-linked free list for tracking available slots
///
/// The slot-based design avoids heap allocations during operation and provides
/// better cache locality
pub struct PageCache {
    /// Maximum number of pages the cache can hold
    capacity: usize,

    /// Hash map for page lookups by key
    map: RefCell<PageHashMap>,

    /// queue pointers:
    /// most recently used position (new pages go here)
    head: Cell<SlotIndex>,
    /// least recently used position (eviction happens here)
    tail: Cell<SlotIndex>,

    /// Next pointers for both SIEVE queue and free list
    /// If a slot is occupied, next[i] = “next older page in the SIEVE queue”
    /// If a slot is free, next[i] = “next free slot”
    /// Whether a slot is in the SIEVE list or the freelist is determined by entries[i]
    /// entries[i] = NULL ? freelist : SIEVE list
    next: RefCell<Vec<SlotIndex>>,

    /// Previous pointers for SIEVE queue only
    /// NONE for free slots (does not participate in free-list)
    prev: RefCell<Vec<SlotIndex>>,

    /// Slot array containing the actual page entries
    /// None = free slot, Some = occupied slot
    entries: RefCell<Vec<PageCacheEntry>>,

    /// Head of the free list (NONE if no free slots)
    free_head: Cell<SlotIndex>,
}

unsafe impl Send for PageCache {}
unsafe impl Sync for PageCache {}

struct PageHashMap {
    buckets: Vec<Vec<HashMapNode>>,
    capacity: usize,
    size: usize,
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
        Self(pgno)
    }
}

impl PageCacheEntry {
    #[inline]
    fn empty() -> Self {
        Self::default()
    }
}

impl Default for PageCacheEntry {
    fn default() -> Self {
        Self {
            key: PageCacheKey(0),
            page: None,
            ref_bit: false.into(),
        }
    }
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        let mut next = vec![NULL; capacity];
        for (i, item) in next.iter_mut().take(capacity - 1).enumerate() {
            *item = i + 1
        }
        Self {
            capacity,
            map: RefCell::new(PageHashMap::new(capacity)),
            head: Cell::new(NULL),
            tail: Cell::new(NULL),
            next: RefCell::new(next),
            prev: RefCell::new(vec![NULL; capacity]),
            entries: RefCell::new(vec![PageCacheEntry::empty(); capacity]),
            free_head: Cell::new(0),
        }
    }

    #[inline]
    /// Links a slot to the front (MRU position) of the SIEVE queue
    fn link_front(&self, slot: usize) {
        let mut next = self.next.borrow_mut();
        let mut prev = self.prev.borrow_mut();
        let old_head = self.head.replace(slot);

        next[slot] = old_head;
        prev[slot] = NULL;

        if old_head != NULL {
            prev[old_head] = slot;
        } else {
            // was empty
            self.tail.set(slot);
        }
    }

    #[inline]
    /// Unlinks a slot from the SIEVE queue
    fn unlink(&self, slot: usize) {
        let mut next = self.next.borrow_mut();
        let mut prev = self.prev.borrow_mut();

        let p = prev[slot];
        let n = next[slot];

        if p != NULL {
            next[p] = n;
        } else {
            self.head.set(n);
        }
        if n != NULL {
            prev[n] = p;
        } else {
            self.tail.set(p);
        }

        prev[slot] = NULL;
        next[slot] = NULL;
    }

    #[inline]
    /// Moves a slot from its current position to the front of the queue
    /// Used when giving a page a "second chance" during eviction
    fn move_to_front(&self, slot: usize) {
        if self.head.get() == slot {
            return;
        } // already head
          // Only unlink if currently linked (prev != NONE or is head)
        let linked = self.prev.borrow()[slot] != NULL;
        if linked {
            self.unlink(slot);
        } else {
            // was free/unlinked; just link it (defensive)
            self.link_front(slot);
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
        if !ignore_exists {
            if let Some(existing) = self.get(&key) {
                assert!(
                    Arc::ptr_eq(&value, &existing),
                    "Attempted to insert different page with same key: {key:?}"
                );
                return Err(CacheError::KeyExists);
            }
        }

        self.make_room_for(1)?;
        let slot_index = self.find_free_slot()?;

        {
            let mut entries = self.entries.borrow_mut();
            let s = &mut entries[slot_index];
            turso_assert!(s.page.is_none(), "page must be None in free slot");
            s.key = key;
            s.page = Some(value);
        }

        // new entries go to the head, unmarked
        self.map.borrow_mut().insert(key, slot_index);
        self.link_front(slot_index);
        Ok(())
    }

    /// Attempts to find a free slot in the entries array by scanning
    /// from the current clock hand position.
    fn find_free_slot(&self) -> Result<usize, CacheError> {
        let fh = self.free_head.get();
        if fh == NULL {
            return Err(CacheError::InternalError(
                "No free slots available after make_room_for".into(),
            ));
        }
        let mut next = self.next.borrow_mut();
        // pop head
        self.free_head.set(next[fh]);
        next[fh] = NULL;
        self.prev.borrow_mut()[fh] = NULL;
        Ok(fh)
    }

    #[inline]
    /// Deletes a page from the cache
    pub fn delete(&mut self, key: PageCacheKey) -> Result<(), CacheError> {
        trace!("cache_delete(key={:?})", key);
        self._delete(key, true)
    }

    fn _delete(&mut self, key: PageCacheKey, clean_page: bool) -> Result<(), CacheError> {
        if !self.contains_key(&key) {
            return Ok(());
        }
        let (entry, slot_idx) = {
            let map = self.map.borrow();
            let idx = map.get(&key).ok_or_else(|| {
                CacheError::InternalError("Key exists but not found in map".into())
            })?;
            (
                self.entries.borrow()[idx]
                    .page
                    .as_ref()
                    .expect("page in map was None")
                    .clone(),
                idx,
            )
        };

        if entry.is_locked() {
            return Err(CacheError::Locked {
                pgno: entry.get().id,
            });
        }
        if entry.is_dirty() {
            return Err(CacheError::Dirty {
                pgno: entry.get().id,
            });
        }
        if entry.is_pinned() {
            return Err(CacheError::Pinned {
                pgno: entry.get().id,
            });
        }

        if clean_page {
            entry.clear_loaded();
            let _ = entry.get().contents.take();
        }

        // unlink from SIEVE list
        self.unlink(slot_idx);

        // remove from map/entries
        self.map.borrow_mut().remove(&key);
        self.entries.borrow_mut()[slot_idx].page = None;
        self.entries.borrow_mut()[slot_idx].ref_bit.set(false);

        // push onto freelist: slot -> free_head
        {
            let mut next = self.next.borrow_mut();
            self.prev.borrow_mut()[slot_idx] = NULL;
            next[slot_idx] = self.free_head.get();
            self.free_head.set(slot_idx);
        }
        Ok(())
    }

    #[inline]
    pub fn get(&self, key: &PageCacheKey) -> Option<PageRef> {
        self.peek(key, true)
    }

    #[inline]
    pub fn peek(&self, key: &PageCacheKey, touch: bool) -> Option<PageRef> {
        let slot = self.map.borrow().get(key)?;
        // avoid holding the borrow across clone
        let page = {
            let entries = self.entries.borrow();
            entries[slot].page.as_ref()?.clone()
        };
        if touch {
            self.entries.borrow_mut()[slot].ref_bit.set(true);
        }
        Some(page)
    }

    /// Resizes the cache to a new capacity
    ///
    /// If shrinking, attempts to evict pages using the SIEVE algorithm.
    /// If growing, simply increases capacity.
    pub fn resize(&mut self, new_cap: usize) -> CacheResizeResult {
        if new_cap == self.capacity {
            return CacheResizeResult::Done;
        }
        assert!(new_cap > 0, "capacity must be > 0");

        // shrink by evicting from tail
        if new_cap < self.len() {
            let mut need = self.len() - new_cap;
            let start = self.tail.get();
            let mut wrapped = false;
            let mut progress = false;
            while need > 0 {
                let tail_idx = match self.tail.get() {
                    i if i != NULL => i,
                    _ => break,
                };
                let (was_marked, key) = {
                    let mut entries = self.entries.borrow_mut();
                    let s = &mut entries[tail_idx];
                    turso_assert!(s.page.is_some(), "tail points to empty slot");
                    let marked = s.ref_bit.replace(false);
                    (marked, s.key)
                };

                if was_marked {
                    self.move_to_front(tail_idx);
                } else {
                    match self._delete(key, true) {
                        Ok(_) => {
                            need -= 1;
                            progress = true;
                        }
                        Err(
                            CacheError::Dirty { .. }
                            | CacheError::Locked { .. }
                            | CacheError::Pinned { .. },
                        ) => {
                            self.move_to_front(tail_idx);
                        }
                        Err(e) => {
                            tracing::error!("resize: unexpected error during eviction: {e:?}");
                            return CacheResizeResult::PendingEvictions;
                        }
                    }
                }
                if self.tail.get() == start {
                    if wrapped {
                        if !progress {
                            return CacheResizeResult::PendingEvictions;
                        }
                        progress = false;
                    }
                    wrapped = true;
                }
            }
            if self.len() > new_cap {
                return CacheResizeResult::PendingEvictions;
            }
        }

        // collect survivors (tail..head) then repack
        let survivors: Vec<PageCacheEntry> = {
            let entries_b = self.entries.borrow();
            let prev_b = self.prev.borrow();
            let mut v = Vec::with_capacity(self.len());
            let mut cur = self.tail.get();
            while cur != NULL {
                let entry = &entries_b[cur];
                if entry.page.is_some() {
                    v.push(entry.clone());
                }
                cur = prev_b[cur];
            }
            v
        };

        // resize arrays
        self.entries
            .borrow_mut()
            .resize(new_cap, PageCacheEntry::empty());
        self.next.borrow_mut().resize(new_cap, NULL);
        self.prev.borrow_mut().resize(new_cap, NULL);
        self.capacity = new_cap;

        // rebuild map + list compactly
        self.map.borrow_mut().clear();
        self.head.set(NULL);
        self.tail.set(NULL);

        {
            let mut entries_mut = self.entries.borrow_mut();
            for (slot, entry) in survivors.iter().rev().enumerate().take(new_cap) {
                entries_mut[slot] = entry.clone();
                self.map.borrow_mut().insert(entry.key, slot);
                self.link_front(slot);
            }
        }
        // rebuild freelist from first unused slot
        let used = survivors.len().min(new_cap);
        {
            let mut next = self.next.borrow_mut();
            let mut prev = self.prev.borrow_mut();
            if used < new_cap {
                self.free_head.set(used);
                for i in used..new_cap - 1 {
                    // freelist links in next
                    next[i] = i + 1;
                    // keep free slots unlinked in SIEVE list
                    prev[i] = NULL;
                }
                next[new_cap - 1] = NULL;
                prev[new_cap - 1] = NULL;
            } else {
                self.free_head.set(NULL);
            }
        }
        CacheResizeResult::Done
    }

    /// Ensures at least `n` free slots are available
    ///
    /// Uses the SIEVE algorithm to evict pages if necessary:
    /// 1. Start at tail (LRU position)
    /// 2. If page is marked, unmark and move to head (second chance)
    /// 3. If page is unmarked, evict it
    /// 4. If page is unevictable (dirty/locked/pinned), move to head
    ///
    /// Returns `CacheError::Full` if not enough pages can be evicted
    pub fn make_room_for(&mut self, n: usize) -> Result<(), CacheError> {
        if n > self.capacity {
            return Err(CacheError::Full);
        }

        let len = self.len();
        let available = self.capacity.saturating_sub(len);
        if n <= available {
            return Ok(());
        }
        let mut need = n - available;

        // evict from tail, if marked, unmark and move to front, otherwise evict
        let start = self.tail.get();
        let mut wrapped = false;
        let mut progress = false;

        while need > 0 {
            let tail_idx = match self.tail.get() {
                NULL => {
                    return Err(CacheError::InternalError(
                        "Tail is None but map not empty".into(),
                    ))
                }
                t => t,
            };

            let (was_marked, key) = {
                let mut entries = self.entries.borrow_mut();
                let s = &mut entries[tail_idx];
                turso_assert!(s.page.is_some(), "tail points to empty slot");
                let marked = s.ref_bit.replace(false);
                (marked, s.key)
            };

            if was_marked {
                self.move_to_front(tail_idx);
            } else {
                match self._delete(key, true) {
                    Ok(_) => {
                        need -= 1;
                        progress = true;
                    }
                    Err(
                        CacheError::Dirty { .. }
                        | CacheError::Locked { .. }
                        | CacheError::Pinned { .. },
                    ) => {
                        self.move_to_front(tail_idx);
                    }
                    Err(e) => return Err(e),
                }
            }

            // detect wrap: if we came back to the original tail and made no progress, give up
            if self.tail.get() == start {
                if wrapped {
                    if !progress {
                        return Err(CacheError::Full);
                    }
                    progress = false;
                }
                wrapped = true;
            }
        }
        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), CacheError> {
        // Check for dirty pages
        for e in self.entries.borrow().iter() {
            if e.page.as_ref().is_some_and(|p| p.is_dirty()) {
                return Err(CacheError::Dirty {
                    pgno: e.page.as_ref().unwrap().get().id,
                });
            }
        }

        // Clear all pages, even if locked or pinned
        for e in self.entries.borrow().iter() {
            match e.page {
                Some(ref p) => {
                    p.clear_loaded();
                    let _ = p.get().contents.take();
                }
                None => continue,
            }
        }
        // Reset all data structures
        self.entries.borrow_mut().fill(PageCacheEntry::empty());
        self.prev.borrow_mut().fill(NULL);
        {
            let mut next = self.next.borrow_mut();
            for i in 0..self.capacity {
                next[i] = if i + 1 < self.capacity { i + 1 } else { NULL };
            }
        }
        self.map.borrow_mut().clear();
        self.head.set(NULL);
        self.tail.set(NULL);
        self.free_head.set(if self.capacity > 0 { 0 } else { NULL });

        Ok(())
    }

    /// Removes all pages from the cache with pgno greater than len
    pub fn truncate(&mut self, len: usize) -> Result<(), CacheError> {
        let entries = self.entries.borrow().clone();

        for entry in entries.iter() {
            match entry.page {
                None => continue,
                Some(ref page) => {
                    if entry.key.0 > len {
                        turso_assert!(!page.is_dirty(), "page must be clean");
                        turso_assert!(!page.is_locked(), "page must be unlocked");
                        turso_assert!(!page.is_pinned(), "page must be unpinned");

                        self.delete(entry.key)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn print(&self) {
        tracing::debug!("page_cache_len={}", self.map.borrow().len());
        let entries = self.entries.borrow();

        for (i, entry_opt) in entries.iter().enumerate() {
            if let Some(ref page) = entry_opt.page {
                tracing::debug!(
                    "slot={}, page={:?}, flags={}, pin_count={}, ref_bit={}",
                    i,
                    entry_opt.key,
                    page.get().flags.load(Ordering::Relaxed),
                    page.get().pin_count.load(Ordering::Relaxed),
                    entry_opt.ref_bit.get(),
                );
            }
        }
    }

    #[cfg(test)]
    pub fn keys(&mut self) -> Vec<PageCacheKey> {
        let mut keys = Vec::new();
        let entries = self.entries.borrow();

        for entry in entries.iter() {
            if entry.page.is_none() {
                continue;
            }
            keys.push(entry.key);
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
        for entry in entries.iter() {
            if entry.page.is_none() {
                continue;
            }
            entry.page.as_ref().unwrap().clear_dirty();
        }
    }

    #[cfg(test)]
    fn verify_cache_integrity(&self) {
        let entries = self.entries.borrow();
        let mut cnt = 0usize;
        let mut seen = vec![false; self.capacity];

        // walk forward
        let mut cur = self.head.get();
        let mut prev = NULL;
        while cur != NULL {
            cnt += 1;
            assert!(
                entries[cur].page.is_some(),
                "list points to empty slot {cur}"
            );
            assert_eq!(self.prev.borrow()[cur], prev, "prev mismatch at {cur}");
            seen[cur] = true;
            prev = cur;
            cur = self.next.borrow()[cur];
        }
        assert_eq!(self.tail.get(), prev, "tail mismatch");
        assert_eq!(cnt, self.len(), "list length != map size");

        // every non-None entry must be on the list
        for (i, e) in entries.iter().enumerate() {
            if e.page.is_some() {
                assert!(seen[i], "slot {i} not in list");
            }
        }

        // Map slot indices should point to matching entries
        for node in self.map.borrow().iter() {
            assert!(entries[node.slot_index].page.is_some());
            assert_eq!(entries[node.slot_index].key, node.key);
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

#[derive(Clone)]
struct HashMapNode {
    key: PageCacheKey,
    slot_index: SlotIndex,
}

#[allow(dead_code)]
impl PageHashMap {
    pub fn new(capacity: usize) -> PageHashMap {
        PageHashMap {
            buckets: vec![vec![]; capacity],
            capacity,
            size: 0,
        }
    }

    pub fn insert(&mut self, key: PageCacheKey, slot_index: SlotIndex) -> Option<SlotIndex> {
        let bucket = self.hash(&key);
        let bucket = &mut self.buckets[bucket];
        let mut idx = 0;
        while let Some(node) = bucket.get_mut(idx) {
            if node.key == key {
                node.slot_index = slot_index;
                node.key = key;
                return Some(node.slot_index);
            }
            idx += 1;
        }
        bucket.push(HashMapNode { key, slot_index });
        self.size += 1;
        None
    }

    pub fn contains_key(&self, key: &PageCacheKey) -> bool {
        let bucket = self.hash(key);
        self.buckets[bucket].iter().any(|node| node.key == *key)
    }

    pub fn get(&self, key: &PageCacheKey) -> Option<SlotIndex> {
        let bucket = self.hash(key);
        let bucket = &self.buckets[bucket];
        for node in bucket {
            if node.key == *key {
                return Some(node.slot_index);
            }
        }
        None
    }

    pub fn remove(&mut self, key: &PageCacheKey) -> Option<SlotIndex> {
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
            Some(v.slot_index)
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

    fn iter(&self) -> impl Iterator<Item = &HashMapNode> {
        self.buckets.iter().flat_map(|b| b.iter())
    }

    fn hash(&self, key: &PageCacheKey) -> usize {
        if self.capacity.is_power_of_two() {
            key.0 & (self.capacity - 1)
        } else {
            key.0 % self.capacity
        }
    }

    fn rehash(&self, new_capacity: usize) -> PageHashMap {
        let mut new_hash_map = PageHashMap::new(new_capacity);
        for node in self.iter() {
            new_hash_map.insert(node.key, node.slot_index);
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
        assert!(cache.insert(key, page).is_ok());
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
    fn test_delete_multiple_elements() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);
        cache.verify_cache_integrity();
        assert_eq!(cache.len(), 3);

        // Delete middle element
        assert!(cache.delete(key2).is_ok());
        assert_eq!(cache.len(), 2, "Length should be 2 after deleting one");
        assert!(!cache.contains_key(&key2), "Should not contain deleted key");
        cache.verify_cache_integrity();

        // Delete another
        assert!(cache.delete(key1).is_ok());
        assert_eq!(cache.len(), 1, "Length should be 1 after deleting two");
        assert!(!cache.contains_key(&key1), "Should not contain deleted key");
        cache.verify_cache_integrity();

        // Delete last
        assert!(cache.delete(key3).is_ok());
        assert_eq!(cache.len(), 0, "Length should be 0 after deleting all");
        cache.verify_cache_integrity();
    }

    #[test]
    #[ignore = "for now let's not track active refs"]
    fn test_delete_with_active_ref() {
        let mut cache = PageCache::default();
        let key1 = create_key(1);
        let page1 = page_with_content(1);
        assert!(cache.insert(key1, page1.clone()).is_ok());
        assert!(page_has_content(&page1));
        cache.verify_cache_integrity();

        let result = cache.delete(key1);
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
        assert!(cache.insert(key1, page1_v1.clone()).is_ok());
        assert_eq!(cache.len(), 1);
        cache.verify_cache_integrity();
        let _ = cache.insert(key1, page1_v2.clone()); // Panic
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let mut cache = PageCache::default();
        let key_nonexist = create_key(99);

        assert!(cache.delete(key_nonexist).is_ok()); // no-op
    }

    #[test]
    fn test_page_cache_evict() {
        let mut cache = PageCache::new(1);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.get(&key2).unwrap().get().id, 2);
        assert!(
            cache.get(&key1).is_none(),
            "capacity=1 should evict the older page"
        );
    }

    #[test]
    fn test_sieve_touch_non_tail_does_not_affect_immediate_eviction() {
        // Insert 1,2,3 -> [3,2,1], tail=1
        let mut cache = PageCache::new(3);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Touch key2 (not tail) to mark it.
        assert!(cache.get(&key2).is_some());

        // Insert 4: tail is still 1 (unmarked) -> evict 1 (not 2).
        let key4 = insert_page(&mut cache, 4);

        assert!(cache.get(&key2).is_some(), "marked non-tail should remain");
        assert!(cache.get(&key3).is_some());
        assert!(cache.get(&key4).is_some());
        assert!(
            cache.get(&key1).is_none(),
            "unmarked tail should be evicted"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_sieve_second_chance_preserves_marked_tail() {
        // Capacity 3, insert 1,2,3 → order(head..tail) = [3,2,1]
        let mut cache = PageCache::new(3);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);
        assert_eq!(cache.len(), 3);

        // Mark the TAIL (key1). SIEVE should not move it yet - only mark.
        assert!(cache.get(&key1).is_some());

        // Insert 4: must evict exactly one unmarked page.
        // Tail is 1 (marked): give second chance: move 1 to head & clear its bit.
        // New tail becomes 2 (unmarked): evict 2. Final set: (1,3,4).
        let key4 = insert_page(&mut cache, 4);

        assert!(
            cache.get(&key1).is_some(),
            "key1 is marked tail and should survive"
        );
        assert!(cache.get(&key3).is_some(), "key3 should remain");
        assert!(cache.get(&key4).is_some(), "key4 just inserted");
        assert!(
            cache.get(&key2).is_none(),
            "key2 should be the one evicted by SIEVE"
        );
        assert_eq!(cache.len(), 3);
        cache.verify_cache_integrity();
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
        // Capacity 2, insert 1,2 -> [2,1] with tail=1 (unmarked)
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Insert 3 -> tail(1) is unmarked → evict 1; keep 2.
        let key3 = insert_page(&mut cache, 3);

        assert_eq!(cache.len(), 2);
        assert!(cache.get(&key2).is_some(), "key2 should remain");
        assert!(cache.get(&key3).is_some(), "key3 just inserted");
        assert!(
            cache.get(&key1).is_none(),
            "key1 (tail, unmarked) must be evicted"
        );
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_page_cache_delete() {
        let mut cache = PageCache::default();
        let key1 = insert_page(&mut cache, 1);
        assert!(cache.delete(key1).is_ok());
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
                    match cache.insert(key, page.clone()) {
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
        // Capacity 2: insert 1,2 -> [2,1] tail=1
        let mut cache = PageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);

        // Peek without touching DOES NOT mark key1
        assert!(cache.peek(&key1, false).is_some());

        // Insert 3 -> tail(1) still unmarked → evict 1.
        let key3 = insert_page(&mut cache, 3);

        assert!(cache.get(&key3).is_some());
        assert!(cache.get(&key2).is_some());
        assert!(
            cache.get(&key1).is_none(),
            "key1 should be evicted since peek(false) didn't mark"
        );
        assert_eq!(cache.len(), 2);
        cache.verify_cache_integrity();
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
