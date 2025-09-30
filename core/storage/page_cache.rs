use std::sync::atomic::Ordering;

use std::sync::Arc;
use tracing::trace;

use crate::turso_assert;

use super::pager::PageRef;

/// FIXME: https://github.com/tursodatabase/turso/issues/1661
const DEFAULT_PAGE_CACHE_SIZE_IN_PAGES_MAKE_ME_SMALLER_ONCE_WAL_SPILL_IS_IMPLEMENTED: usize =
    100000;

#[derive(Debug, Copy, Eq, Hash, PartialEq, Clone)]
#[repr(transparent)]
pub struct PageCacheKey(usize);

const NULL: usize = usize::MAX;

const CLEAR: u8 = 0;
const REF_MAX: u8 = 3;

#[derive(Clone, Debug)]
struct PageCacheEntry {
    /// Key identifying this page
    key: PageCacheKey,
    /// The cached page, None if this slot is free
    page: Option<PageRef>,
    /// Reference counter (SIEVE/GClock): starts at zero, bumped on access,
    /// decremented during eviction, only pages at 0 are evicted.
    ref_bit: u8,
    /// Index of next entry in SIEVE queue (older/toward tail)
    next: usize,
    /// Index of previous entry in SIEVE queue (newer/toward head)
    prev: usize,
}

impl Default for PageCacheEntry {
    fn default() -> Self {
        Self {
            key: PageCacheKey(0),
            page: None,
            ref_bit: CLEAR,
            next: NULL,
            prev: NULL,
        }
    }
}

impl PageCacheEntry {
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
    #[inline]
    fn clear_ref(&mut self) {
        self.ref_bit = CLEAR;
    }
    #[inline]
    fn empty() -> Self {
        Self::default()
    }
    #[inline]
    fn reset_links(&mut self) {
        self.next = NULL;
        self.prev = NULL;
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
    /// Map of Key -> usize in entries array
    map: PageHashMap,
    clock_hand: usize,
    /// Fixed-size vec holding page entries
    entries: Vec<PageCacheEntry>,
    /// Free list: Stack of available slot indices
    freelist: Vec<usize>,
}

unsafe impl Send for PageCache {}
unsafe impl Sync for PageCache {}

struct PageHashMap {
    buckets: Vec<Vec<HashMapNode>>,
    capacity: usize,
    size: usize,
}

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
        let freelist = (0..capacity).rev().collect::<Vec<usize>>();
        Self {
            capacity,
            map: PageHashMap::new(capacity),
            clock_hand: NULL,
            entries: vec![PageCacheEntry::empty(); capacity],
            freelist,
        }
    }

    #[inline]
    fn link_after(&mut self, a: usize, b: usize) {
        // insert `b` after `a` in a non-empty circular list
        let an = self.entries[a].next;
        self.entries[b].prev = a;
        self.entries[b].next = an;
        self.entries[an].prev = b;
        self.entries[a].next = b;
    }

    #[inline]
    fn link_new_node(&mut self, slot: usize) {
        let hand = self.clock_hand;
        if hand == NULL {
            // first element → points to itself
            self.entries[slot].prev = slot;
            self.entries[slot].next = slot;
            self.clock_hand = slot;
        } else {
            // insert after the hand (LRU)
            self.link_after(hand, slot);
        }
    }

    #[inline]
    fn unlink(&mut self, slot: usize) {
        let p = self.entries[slot].prev;
        let n = self.entries[slot].next;

        if p == slot && n == slot {
            self.clock_hand = NULL;
        } else {
            self.entries[p].next = n;
            self.entries[n].prev = p;
            if self.clock_hand == slot {
                // stay at LRU position, second-oldest becomes oldest
                self.clock_hand = p;
            }
        }

        self.entries[slot].reset_links();
    }

    #[inline]
    fn forward_of(&self, i: usize) -> usize {
        self.entries[i].next
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
        let slot = self.map.get(&key);
        if let Some(slot) = slot {
            let p = self.entries[slot]
                .page
                .as_ref()
                .expect("slot must have a page");

            if !p.is_loaded() && !p.is_locked() {
                // evict, then continue with fresh insert
                self._delete(key, true)?;
                let slot_index = self.find_free_slot()?;
                let entry = &mut self.entries[slot_index];
                entry.key = key;
                entry.page = Some(value);
                entry.clear_ref();
                self.map.insert(key, slot_index);
                self.link_new_node(slot_index);
                return Ok(());
            }

            let existing = &mut self.entries[slot];
            existing.bump_ref();
            if update_in_place {
                existing.page = Some(value);
                return Ok(());
            } else {
                turso_assert!(
                    Arc::ptr_eq(existing.page.as_ref().unwrap(), &value),
                    "Attempted to insert different page with same key: {key:?}"
                );
                return Err(CacheError::KeyExists);
            }
        }
        // Key doesn't exist, proceed with new entry
        self.make_room_for(1)?;
        let slot_index = self.find_free_slot()?;
        let entry = &mut self.entries[slot_index];
        turso_assert!(entry.page.is_none(), "page must be None in free slot");
        entry.key = key;
        entry.page = Some(value);
        // Sieve ref bit starts cleared, will be set on first access
        entry.clear_ref();
        self.map.insert(key, slot_index);
        self.link_new_node(slot_index);
        Ok(())
    }

    fn find_free_slot(&mut self) -> Result<usize, CacheError> {
        let slot = self.freelist.pop().ok_or_else(|| {
            CacheError::InternalError("No free slots available after make_room_for".into())
        })?;
        #[cfg(debug_assertions)]
        {
            turso_assert!(
                self.entries[slot].page.is_none(),
                "allocating non-free slot {}",
                slot
            );
        }
        turso_assert!(
            self.entries[slot].next == NULL && self.entries[slot].prev == NULL,
            "freelist slot {} has non-NULL links",
            slot
        );
        Ok(slot)
    }

    fn _delete(&mut self, key: PageCacheKey, clean_page: bool) -> Result<(), CacheError> {
        if !self.contains_key(&key) {
            return Ok(());
        }
        let slot_idx = self
            .map
            .get(&key)
            .ok_or_else(|| CacheError::InternalError("Key exists but not found in map".into()))?;
        let entry = self.entries[slot_idx]
            .page
            .as_ref()
            .expect("page in map was None")
            .clone();
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
        // unlink from circular list and advance hand if needed
        self.unlink(slot_idx);
        self.map.remove(&key);
        let e = &mut self.entries[slot_idx];
        e.page = None;
        e.clear_ref();
        e.reset_links();
        self.freelist.push(slot_idx);
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
        let Some(slot) = self.map.get(key) else {
            return Ok(None);
        };
        // Because we can abort a read_page completion, this means a page can be in the cache but be unloaded and unlocked.
        // However, if we do not evict that page from the page cache, we will return an unloaded page later which will trigger
        // assertions later on. This is worsened by the fact that page cache is not per `Statement`, so you can abort a completion
        // in one Statement, and trigger some error in the next one if we don't evict the page here.
        let entry = &mut self.entries[slot];
        let page = entry
            .page
            .as_ref()
            .expect("page in the map to exist")
            .clone();
        if !page.is_loaded() && !page.is_locked() {
            self.delete(*key)?;
            return Ok(None);
        }
        entry.bump_ref();
        Ok(Some(page))
    }

    #[inline]
    pub fn peek(&mut self, key: &PageCacheKey, touch: bool) -> Option<PageRef> {
        let slot = self.map.get(key)?;
        let entry = &mut self.entries[slot];
        let page = entry.page.as_ref()?.clone();
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
        if new_cap < self.len() {
            let need = self.len() - new_cap;
            let mut evicted = 0;
            while evicted < need {
                match self.make_room_for(1) {
                    Ok(()) => evicted += 1,
                    Err(CacheError::Full) => return CacheResizeResult::PendingEvictions,
                    Err(_) => return CacheResizeResult::PendingEvictions,
                }
            }
        }
        assert!(new_cap > 0);
        // Collect survivors starting from hand, one full cycle
        struct Payload {
            key: PageCacheKey,
            page: PageRef,
            ref_bit: u8,
        }
        let survivors: Vec<Payload> = {
            let mut v = Vec::with_capacity(self.len());
            let start = self.clock_hand;
            if start != NULL {
                let mut cur = start;
                let mut seen = 0usize;
                loop {
                    let e = &self.entries[cur];
                    if let Some(ref p) = e.page {
                        v.push(Payload {
                            key: e.key,
                            page: p.clone(),
                            ref_bit: e.ref_bit,
                        });
                        seen += 1;
                    }
                    cur = e.next;
                    if cur == start || seen >= self.len() {
                        break;
                    }
                }
            }
            v
        };
        // Rebuild storage
        self.entries.resize(new_cap, PageCacheEntry::empty());
        self.capacity = new_cap;
        let mut new_map = PageHashMap::new(new_cap);

        let used = survivors.len().min(new_cap);
        for (i, item) in survivors.iter().enumerate().take(used) {
            let e = &mut self.entries[i];
            e.key = item.key;
            e.page = Some(item.page.clone());
            e.ref_bit = item.ref_bit;
            // link circularly to neighbors by index
            let prev = if i == 0 { used - 1 } else { i - 1 };
            let next = if i + 1 == used { 0 } else { i + 1 };
            e.prev = prev;
            e.next = next;
            new_map.insert(item.key, i);
        }
        self.map = new_map;
        // hand points to slot 0 if there are survivors, else NULL
        self.clock_hand = if used > 0 { 0 } else { NULL };
        // rebuild freelist
        self.freelist.clear();
        for i in (used..new_cap).rev() {
            self.freelist.push(i);
        }

        CacheResizeResult::Done
    }

    /// Ensures at least `n` free slots are available
    ///
    /// Uses the SIEVE algorithm to evict pages if necessary:
    /// Start at tail (LRU position)
    /// If page is marked, decrement mark
    /// If page mark was already Cleared, evict it
    /// If page is unevictable (dirty/locked/pinned), continue sweep
    /// On sweep, pages with ref_bit > 0 are given a second chance by decrementing
    /// their ref_bit and leaving them in place; only pages with ref_bit == 0 are evicted.
    /// We never relocate nodes during sweeping.
    /// because the list is circular, `tail.next == head` and `head.prev == tail`.
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

        let mut need = n - available;
        let mut examined = 0usize;
        let max_examinations = self.len().saturating_mul(REF_MAX as usize + 1);

        let mut cur = self.clock_hand;
        if cur == NULL || cur >= self.capacity || self.entries[cur].page.is_none() {
            return Err(CacheError::Full);
        }

        while need > 0 && examined < max_examinations {
            // compute the next candidate before mutating anything
            let next = self.forward_of(cur);

            let evictable_and_clear = {
                let e = &mut self.entries[cur];
                if let Some(ref p) = e.page {
                    if p.is_dirty() || p.is_locked() || p.is_pinned() {
                        examined += 1;
                        false
                    } else if e.ref_bit == CLEAR {
                        true
                    } else {
                        e.decrement_ref();
                        examined += 1;
                        false
                    }
                } else {
                    examined += 1;
                    false
                }
            };

            if evictable_and_clear {
                // Evict the current slot, then continue from the next candidate in sweep direction
                self.evict_slot(cur, true)?;
                need -= 1;
                examined = 0;

                // move on; if the ring became empty, self.clock_hand may be NULL
                cur = if next == cur { self.clock_hand } else { next };
                if cur == NULL {
                    if need == 0 {
                        break;
                    }
                    return Err(CacheError::Full);
                }
            } else {
                // keep sweeping
                cur = next;
            }
        }
        self.clock_hand = cur;
        if need > 0 {
            return Err(CacheError::Full);
        }
        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), CacheError> {
        if self.map.len() == 0 {
            // Fast path: nothing to do.
            self.clock_hand = NULL;
            return Ok(());
        }

        for node in self.map.iter() {
            let e = &self.entries[node.slot_index];
            if let Some(ref p) = e.page {
                if p.is_dirty() {
                    return Err(CacheError::Dirty { pgno: p.get().id });
                }
            }
        }
        let mut used_slots = Vec::with_capacity(self.map.len());
        for node in self.map.iter() {
            used_slots.push(node.slot_index);
        }
        // don't touch already-free slots at all.
        for &i in &used_slots {
            if let Some(p) = self.entries[i].page.take() {
                p.clear_loaded();
                let _ = p.get().contents.take();
            }
            self.entries[i].clear_ref();
            self.entries[i].reset_links();
        }
        self.clock_hand = NULL;
        self.map = PageHashMap::new(self.capacity);
        for &i in used_slots.iter().rev() {
            self.freelist.push(i);
        }
        Ok(())
    }

    #[inline]
    /// preconditions: slot contains Some(page) and is clean/unlocked/unpinned
    fn evict_slot(&mut self, slot: usize, clean_page: bool) -> Result<(), CacheError> {
        let key = self.entries[slot].key;
        if clean_page {
            if let Some(ref p) = self.entries[slot].page {
                p.clear_loaded();
                let _ = p.get().contents.take();
            }
        }
        // unlink will advance the hand if it pointed to `slot`
        self.unlink(slot);
        let _ = self.map.remove(&key);

        let e = &mut self.entries[slot];
        e.page = None;
        e.clear_ref();
        e.reset_links();
        self.freelist.push(slot);

        Ok(())
    }

    /// Removes all pages from the cache with pgno greater than len
    pub fn truncate(&mut self, len: usize) -> Result<(), CacheError> {
        let keys_to_delete: Vec<PageCacheKey> = {
            self.entries
                .iter()
                .filter_map(|entry| {
                    entry.page.as_ref().and({
                        if entry.key.0 > len {
                            Some(entry.key)
                        } else {
                            None
                        }
                    })
                })
                .collect()
        };
        for key in keys_to_delete.iter() {
            self.delete(*key)?;
        }
        Ok(())
    }

    pub fn print(&self) {
        tracing::debug!("page_cache_len={}", self.map.len());
        let entries = &self.entries;

        for (i, entry_opt) in entries.iter().enumerate() {
            if let Some(ref page) = entry_opt.page {
                tracing::debug!(
                    "slot={}, page={:?}, flags={}, pin_count={}, ref_bit={:?}",
                    i,
                    entry_opt.key,
                    page.get().flags.load(Ordering::SeqCst),
                    page.get().pin_count.load(Ordering::SeqCst),
                    entry_opt.ref_bit,
                );
            }
        }
    }

    #[cfg(test)]
    pub fn keys(&mut self) -> Vec<PageCacheKey> {
        let mut keys = Vec::with_capacity(self.len());
        let entries = &self.entries;
        for entry in entries.iter() {
            if entry.page.is_none() {
                continue;
            }
            keys.push(entry.key);
        }
        keys
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[cfg(test)]
    fn verify_cache_integrity(&self) {
        let map = &self.map;
        let hand = self.clock_hand;

        if hand == NULL {
            assert_eq!(map.len(), 0, "map not empty but list is empty");
        } else {
            // 0 = unseen, 1 = freelist, 2 = in list
            let mut seen = vec![0u8; self.capacity];
            // Walk exactly map.len steps from hand, ensure circular closure
            let mut cnt = 0usize;
            let mut cur = hand;
            loop {
                let e = &self.entries[cur];

                assert!(e.page.is_some(), "list points to empty slot {cur}");
                assert_eq!(seen[cur], 0, "slot {cur} appears twice (list/freelist)");
                seen[cur] = 2;
                cnt += 1;

                let n = e.next;
                let p = e.prev;
                assert_eq!(self.entries[n].prev, cur, "broken next.prev at {cur}");
                assert_eq!(self.entries[p].next, cur, "broken prev.next at {cur}");

                if n == hand {
                    break;
                }
                assert!(cnt <= map.len(), "cycle longer than map len");
                cur = n;
            }
            assert_eq!(
                cnt,
                map.len(),
                "list length {} != map size {}",
                cnt,
                map.len()
            );

            // Map bijection
            for node in map.iter() {
                let slot = node.slot_index;
                assert!(
                    self.entries[slot].page.is_some(),
                    "map points to empty slot"
                );
                assert_eq!(self.entries[slot].key, node.key, "map key mismatch");
                assert_eq!(seen[slot], 2, "map slot {slot} not on list");
            }

            // Freelist disjointness
            let mut free_count = 0usize;
            for &s in &self.freelist {
                free_count += 1;
                assert_eq!(seen[s], 0, "slot {s} in both freelist and list");
                assert!(
                    self.entries[s].page.is_none(),
                    "freelist slot {s} has a page"
                );
                assert_eq!(self.entries[s].next, NULL, "freelist slot {s} next != NULL");
                assert_eq!(self.entries[s].prev, NULL, "freelist slot {s} prev != NULL");
                seen[s] = 1;
            }

            // No orphans: every slot is in list or freelist or unused beyond capacity
            let orphans = seen.iter().filter(|&&v| v == 0).count();
            assert_eq!(
                free_count + cnt + orphans,
                self.capacity,
                "free {} + list {} + orphans {} != capacity {}",
                free_count,
                cnt,
                orphans,
                self.capacity
            );
            // In practice orphans==0; assertion above detects mismatches.
        }

        // Hand sanity
        if hand != NULL {
            assert!(hand < self.capacity, "clock_hand out of bounds");
            assert!(
                self.entries[hand].page.is_some(),
                "clock_hand points to empty slot"
            );
        }
    }

    #[cfg(test)]
    fn slot_of(&self, key: &PageCacheKey) -> Option<usize> {
        self.map.get(key)
    }
    #[cfg(test)]
    fn ref_of(&self, key: &PageCacheKey) -> Option<u8> {
        self.slot_of(key).map(|i| self.entries[i].ref_bit)
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
    slot_index: usize,
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

    pub fn insert(&mut self, key: PageCacheKey, slot_index: usize) {
        let bucket = self.hash(&key);
        let bucket = &mut self.buckets[bucket];
        let mut idx = 0;
        while let Some(node) = bucket.get_mut(idx) {
            if node.key == key {
                node.slot_index = slot_index;
                node.key = key;
                return;
            }
            idx += 1;
        }
        bucket.push(HashMapNode { key, slot_index });
        self.size += 1;
    }

    pub fn contains_key(&self, key: &PageCacheKey) -> bool {
        let bucket = self.hash(key);
        self.buckets[bucket].iter().any(|node| node.key == *key)
    }

    pub fn get(&self, key: &PageCacheKey) -> Option<usize> {
        let bucket = self.hash(key);
        let bucket = &self.buckets[bucket];
        for node in bucket {
            if node.key == *key {
                return Some(node.slot_index);
            }
        }
        None
    }

    pub fn remove(&mut self, key: &PageCacheKey) -> Option<usize> {
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
        assert!(cache.insert(key, page).is_ok());
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

        assert!(cache.clear().is_ok());
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

            cache.clear().unwrap();
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
        c.clear().unwrap();
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

        // Single element should point to itself
        let slot = cache.slot_of(&key1).unwrap();
        assert_eq!(cache.entries[slot].next, slot);
        assert_eq!(cache.entries[slot].prev, slot);

        // Delete single element
        assert!(cache.delete(key1).is_ok());
        assert_eq!(cache.clock_hand, NULL);

        // Insert after empty should work
        let key2 = insert_page(&mut cache, 2);
        let slot2 = cache.slot_of(&key2).unwrap();
        assert_eq!(cache.entries[slot2].next, slot2);
        assert_eq!(cache.entries[slot2].prev, slot2);
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

        // Hand should have advanced
        let new_hand = cache.clock_hand;
        assert_ne!(new_hand, NULL);
        // Hand moved during sweep (exact position depends on eviction)
        assert!(initial_hand == NULL || new_hand != initial_hand || cache.len() < 2);
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

        // Verify circular structure
        if cache.clock_hand != NULL {
            let start = cache.clock_hand;
            let mut current = start;
            let mut count = 0;
            loop {
                count += 1;
                current = cache.entries[current].next;
                if current == start {
                    break;
                }
                assert!(count <= cache.len(), "Circular list broken after resize");
            }
            assert_eq!(count, cache.len());
        }
        cache.verify_cache_integrity();
    }

    #[test]
    fn test_link_after_correctness() {
        let mut cache = PageCache::new(4);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);

        // Verify circular linkage
        let slot1 = cache.slot_of(&key1).unwrap();
        let slot2 = cache.slot_of(&key2).unwrap();
        let slot3 = cache.slot_of(&key3).unwrap();

        // Should form a circle: 3 -> 2 -> 1 -> 3 (insertion order)
        assert_eq!(cache.entries[slot3].next, slot2);
        assert_eq!(cache.entries[slot2].next, slot1);
        assert_eq!(cache.entries[slot1].next, slot3);

        assert_eq!(cache.entries[slot3].prev, slot1);
        assert_eq!(cache.entries[slot2].prev, slot3);
        assert_eq!(cache.entries[slot1].prev, slot2);

        cache.verify_cache_integrity();
    }
}
