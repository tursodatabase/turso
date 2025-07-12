use std::{cell::RefCell, ptr::NonNull};

use std::sync::Arc;
use tracing::{debug, trace};

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
    prev: Option<NonNull<PageCacheEntry>>,
    next: Option<NonNull<PageCacheEntry>>,
}

pub struct DumbLruPageCache {
    capacity: usize,
    map: RefCell<PageHashMap>,
    head: RefCell<Option<NonNull<PageCacheEntry>>>,
    tail: RefCell<Option<NonNull<PageCacheEntry>>>,
}
unsafe impl Send for DumbLruPageCache {}
unsafe impl Sync for DumbLruPageCache {}

struct PageHashMap {
    // FIXME: do we prefer array buckets or list? Deletes will be slower here which I guess happens often. I will do this for now to test how well it does.
    buckets: Vec<Vec<HashMapNode>>,
    capacity: usize,
    size: usize,
}

#[derive(Clone)]
struct HashMapNode {
    key: PageCacheKey,
    value: NonNull<PageCacheEntry>,
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
        Self {
            capacity,
            map: RefCell::new(PageHashMap::new(capacity)),
            head: RefCell::new(None),
            tail: RefCell::new(None),
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
        let entry = Box::new(PageCacheEntry {
            key: key.clone(),
            next: None,
            prev: None,
            page: value,
        });
        let ptr_raw = Box::into_raw(entry);
        let ptr = unsafe { NonNull::new_unchecked(ptr_raw) };
        self.touch(ptr);

        self.map.borrow_mut().insert(key, ptr);
        Ok(())
    }

    pub fn delete(&mut self, key: PageCacheKey) -> Result<(), CacheError> {
        trace!("cache_delete(key={:?})", key);
        self._delete(key, true)
    }

    // Returns Ok if key is not found
    pub fn _delete(&mut self, key: PageCacheKey, clean_page: bool) -> Result<(), CacheError> {
        if !self.contains_key(&key) {
            return Ok(());
        }

        let ptr = *self.map.borrow().get(&key).unwrap();
        // Try to detach from LRU list first, can fail
        self.detach(ptr, clean_page)?;
        let ptr = self.map.borrow_mut().remove(&key).unwrap();
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
        self.peek(key, true)
    }

    /// Get page without promoting entry
    pub fn peek(&mut self, key: &PageCacheKey, touch: bool) -> Option<PageRef> {
        trace!("cache_get(key={:?})", key);
        let mut ptr = self.get_ptr(key)?;
        let page = unsafe { ptr.as_mut().page.clone() };
        if touch {
            self.unlink(ptr);
            self.touch(ptr);
        }
        Some(page)
    }

    // To match SQLite behavior, just set capacity and try to shrink as much as possible.
    // In case of failure, the caller should request further evictions (e.g. after I/O).
    pub fn resize(&mut self, capacity: usize) -> CacheResizeResult {
        let new_map = self.map.borrow().rehash(capacity);
        self.map.replace(new_map);
        self.capacity = capacity;
        match self.make_room_for(0) {
            Ok(_) => CacheResizeResult::Done,
            Err(_) => CacheResizeResult::PendingEvictions,
        }
    }

    fn detach(
        &mut self,
        mut entry: NonNull<PageCacheEntry>,
        clean_page: bool,
    ) -> Result<(), CacheError> {
        let entry_mut = unsafe { entry.as_mut() };
        if entry_mut.page.is_locked() {
            return Err(CacheError::Locked);
        }
        if entry_mut.page.is_dirty() {
            return Err(CacheError::Dirty {
                pgno: entry_mut.page.get().id,
            });
        }

        if clean_page {
            entry_mut.page.clear_loaded();
            debug!("cleaning up page {}", entry_mut.page.get().id);
            let _ = entry_mut.page.get().contents.take();
        }
        self.unlink(entry);
        Ok(())
    }

    fn unlink(&mut self, mut entry: NonNull<PageCacheEntry>) {
        let (next, prev) = unsafe {
            let c = entry.as_mut();
            let next = c.next;
            let prev = c.prev;
            c.prev = None;
            c.next = None;
            (next, prev)
        };

        match (prev, next) {
            (None, None) => {
                self.head.replace(None);
                self.tail.replace(None);
            }
            (None, Some(mut n)) => {
                unsafe { n.as_mut().prev = None };
                self.head.borrow_mut().replace(n);
            }
            (Some(mut p), None) => {
                unsafe { p.as_mut().next = None };
                self.tail = RefCell::new(Some(p));
            }
            (Some(mut p), Some(mut n)) => unsafe {
                let p_mut = p.as_mut();
                p_mut.next = Some(n);
                let n_mut = n.as_mut();
                n_mut.prev = Some(p);
            },
        };
    }

    /// inserts into head, assuming we detached first
    fn touch(&mut self, mut entry: NonNull<PageCacheEntry>) {
        if let Some(mut head) = *self.head.borrow_mut() {
            unsafe {
                entry.as_mut().next.replace(head);
                let head = head.as_mut();
                head.prev = Some(entry);
            }
        }

        if self.tail.borrow().is_none() {
            self.tail.borrow_mut().replace(entry);
        }
        self.head.borrow_mut().replace(entry);
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

        let tail = self.tail.borrow().ok_or_else(|| {
            CacheError::InternalError(format!(
                "Page cache of len {} expected to have a tail pointer",
                self.len()
            ))
        })?;

        // Handle len > capacity, too
        let available = self.capacity.saturating_sub(len);
        let x = n.saturating_sub(available);
        let mut need_to_evict = x.saturating_add(len.saturating_sub(self.capacity));

        let mut current_opt = Some(tail);
        while need_to_evict > 0 && current_opt.is_some() {
            let current = current_opt.unwrap();
            let entry = unsafe { current.as_ref() };
            current_opt = entry.prev; // Pick prev before modifying entry
            match self.delete(entry.key.clone()) {
                Err(_) => {}
                Ok(_) => need_to_evict -= 1,
            }
        }

        match need_to_evict > 0 {
            true => Err(CacheError::Full),
            false => Ok(()),
        }
    }

    pub fn clear(&mut self) -> Result<(), CacheError> {
        let mut current = *self.head.borrow();
        while let Some(current_entry) = current {
            unsafe {
                self.map.borrow_mut().remove(&current_entry.as_ref().key);
            }
            let next = unsafe { current_entry.as_ref().next };
            self.detach(current_entry, true)?;
            unsafe {
                assert!(!current_entry.as_ref().page.is_dirty());
            }
            unsafe {
                let _ = Box::from_raw(current_entry.as_ptr());
            };
            current = next;
        }
        let _ = self.head.take();
        let _ = self.tail.take();

        assert!(self.head.borrow().is_none());
        assert!(self.tail.borrow().is_none());
        assert!(self.map.borrow().is_empty());
        Ok(())
    }

    pub fn print(&self) {
        tracing::debug!("page_cache_len={}", self.map.borrow().len());
        let head_ptr = *self.head.borrow();
        let mut current = head_ptr;
        while let Some(node) = current {
            unsafe {
                tracing::debug!("page={:?}", node.as_ref().key);
                let node_ref = node.as_ref();
                current = node_ref.next;
            }
        }
    }

    #[cfg(test)]
    pub fn keys(&mut self) -> Vec<PageCacheKey> {
        let mut this_keys = Vec::new();
        let head_ptr = *self.head.borrow();
        let mut current = head_ptr;
        while let Some(node) = current {
            unsafe {
                this_keys.push(node.as_ref().key.clone());
                let node_ref = node.as_ref();
                current = node_ref.next;
            }
        }
        this_keys
    }

    pub fn len(&self) -> usize {
        self.map.borrow().len()
    }

    #[cfg(test)]
    fn get_entry_ptr(&self, key: &PageCacheKey) -> Option<NonNull<PageCacheEntry>> {
        self.map.borrow().get(key).copied()
    }

    #[cfg(test)]
    fn verify_list_integrity(&self) {
        let map_len = self.map.borrow().len();
        let head_ptr = *self.head.borrow();
        let tail_ptr: Option<NonNull<PageCacheEntry>> = *self.tail.borrow();

        if map_len == 0 {
            assert!(head_ptr.is_none(), "Head should be None when map is empty");
            assert!(tail_ptr.is_none(), "Tail should be None when map is empty");
            return;
        }

        assert!(
            head_ptr.is_some(),
            "Head should be Some when map is not empty"
        );
        assert!(
            tail_ptr.is_some(),
            "Tail should be Some when map is not empty"
        );

        unsafe {
            assert!(
                head_ptr.unwrap().as_ref().prev.is_none(),
                "Head's prev pointer mismatch"
            );
        }

        unsafe {
            assert!(
                tail_ptr.unwrap().as_ref().next.is_none(),
                "Tail's next pointer mismatch"
            );
        }

        // Forward traversal
        let mut forward_count = 0;
        let mut current = head_ptr;
        let mut last_ptr: Option<NonNull<PageCacheEntry>> = None;
        while let Some(node) = current {
            forward_count += 1;
            unsafe {
                let node_ref = node.as_ref();
                assert_eq!(
                    node_ref.prev, last_ptr,
                    "Backward pointer mismatch during forward traversal for key {:?}",
                    node_ref.key
                );
                assert!(
                    self.map.borrow().contains_key(&node_ref.key),
                    "Node key {:?} not found in map during forward traversal",
                    node_ref.key
                );
                assert_eq!(
                    self.map.borrow().get(&node_ref.key).copied(),
                    Some(node),
                    "Map pointer mismatch for key {:?}",
                    node_ref.key
                );

                last_ptr = Some(node);
                current = node_ref.next;
            }

            if forward_count > map_len + 5 {
                panic!(
                    "Infinite loop suspected in forward integrity check. Size {map_len}, count {forward_count}"
                );
            }
        }
        assert_eq!(
            forward_count, map_len,
            "Forward count mismatch (counted {forward_count}, map has {map_len})"
        );
        assert_eq!(
            tail_ptr, last_ptr,
            "Tail pointer mismatch after forward traversal"
        );

        // Backward traversal
        let mut backward_count = 0;
        current = tail_ptr;
        last_ptr = None;
        while let Some(node) = current {
            backward_count += 1;
            unsafe {
                let node_ref = node.as_ref();
                assert_eq!(
                    node_ref.next, last_ptr,
                    "Forward pointer mismatch during backward traversal for key {:?}",
                    node_ref.key
                );
                assert!(
                    self.map.borrow().contains_key(&node_ref.key),
                    "Node key {:?} not found in map during backward traversal",
                    node_ref.key
                );

                last_ptr = Some(node);
                current = node_ref.prev;
            }
            if backward_count > map_len + 5 {
                panic!(
                    "Infinite loop suspected in backward integrity check. Size {map_len}, count {backward_count}"
                );
            }
        }
        assert_eq!(
            backward_count, map_len,
            "Backward count mismatch (counted {backward_count}, map has {map_len})"
        );
        assert_eq!(
            head_ptr, last_ptr,
            "Head pointer mismatch after backward traversal"
        );
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

    /// Insert page into hashmap. If a key was already in the hashmap, then update it and return the previous value.
    pub fn insert(
        &mut self,
        key: PageCacheKey,
        value: NonNull<PageCacheEntry>,
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
        bucket.push(HashMapNode { key, value });
        self.size += 1;
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

    pub fn remove(&mut self, key: &PageCacheKey) -> Option<NonNull<PageCacheEntry>> {
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
        if self.capacity.is_power_of_two() {
            key.pgno & (self.capacity - 1)
        } else {
            key.pgno % self.capacity
        }
    }

    pub fn rehash(&self, new_capacity: usize) -> PageHashMap {
        let mut new_hash_map = PageHashMap::new(new_capacity);
        for node in self.iter() {
            new_hash_map.insert(node.key.clone(), node.value);
        }
        new_hash_map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{Buffer, BufferData};
    use crate::storage::page_cache::CacheError;
    use crate::storage::pager::{Page, PageRef};
    use crate::storage::sqlite3_ondisk::PageContent;
    use std::ptr::NonNull;
    use std::{cell::RefCell, num::NonZeroUsize, pin::Pin, rc::Rc, sync::Arc};

    use lru::LruCache;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

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

    fn insert_and_get_entry(
        cache: &mut DumbLruPageCache,
        id: usize,
    ) -> (PageCacheKey, NonNull<PageCacheEntry>) {
        let key = create_key(id);
        let page = page_with_content(id);
        assert!(cache.insert(key.clone(), page).is_ok());
        let entry = cache.get_ptr(&key).expect("Entry should exist");
        (key, entry)
    }

    #[test]
    fn test_detach_only_element() {
        let mut cache = DumbLruPageCache::default();
        let key1 = insert_page(&mut cache, 1);
        cache.verify_list_integrity();
        assert_eq!(cache.len(), 1);
        assert!(cache.head.borrow().is_some());
        assert!(cache.tail.borrow().is_some());
        assert_eq!(*cache.head.borrow(), *cache.tail.borrow());

        assert!(cache.delete(key1.clone()).is_ok());

        assert_eq!(
            cache.len(),
            0,
            "Length should be 0 after deleting only element"
        );
        assert!(
            cache.map.borrow().get(&key1).is_none(),
            "Map should not contain key after delete"
        );
        assert!(cache.head.borrow().is_none(), "Head should be None");
        assert!(cache.tail.borrow().is_none(), "Tail should be None");
        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_head() {
        let mut cache = DumbLruPageCache::default();
        let _key1 = insert_page(&mut cache, 1); // Tail
        let key2 = insert_page(&mut cache, 2); // Middle
        let key3 = insert_page(&mut cache, 3); // Head
        cache.verify_list_integrity();
        assert_eq!(cache.len(), 3);

        let head_ptr_before = cache.head.borrow().unwrap();
        assert_eq!(
            unsafe { &head_ptr_before.as_ref().key },
            &key3,
            "Initial head check"
        );

        assert!(cache.delete(key3.clone()).is_ok());

        assert_eq!(cache.len(), 2, "Length should be 2 after deleting head");
        assert!(
            cache.map.borrow().get(&key3).is_none(),
            "Map should not contain deleted head key"
        );
        cache.verify_list_integrity();

        let new_head_ptr = cache.head.borrow().unwrap();
        assert_eq!(
            unsafe { &new_head_ptr.as_ref().key },
            &key2,
            "New head should be key2"
        );
        assert!(
            unsafe { new_head_ptr.as_ref().prev.is_none() },
            "New head's prev should be None"
        );

        let tail_ptr = cache.tail.borrow().unwrap();
        assert_eq!(
            unsafe { new_head_ptr.as_ref().next },
            Some(tail_ptr),
            "New head's next should point to tail (key1)"
        );
    }

    #[test]
    fn test_detach_tail() {
        let mut cache = DumbLruPageCache::default();
        let key1 = insert_page(&mut cache, 1); // Tail
        let key2 = insert_page(&mut cache, 2); // Middle
        let _key3 = insert_page(&mut cache, 3); // Head
        cache.verify_list_integrity();
        assert_eq!(cache.len(), 3);

        let tail_ptr_before = cache.tail.borrow().unwrap();
        assert_eq!(
            unsafe { &tail_ptr_before.as_ref().key },
            &key1,
            "Initial tail check"
        );

        assert!(cache.delete(key1.clone()).is_ok()); // Delete tail

        assert_eq!(cache.len(), 2, "Length should be 2 after deleting tail");
        assert!(
            cache.map.borrow().get(&key1).is_none(),
            "Map should not contain deleted tail key"
        );
        cache.verify_list_integrity();

        let new_tail_ptr = cache.tail.borrow().unwrap();
        assert_eq!(
            unsafe { &new_tail_ptr.as_ref().key },
            &key2,
            "New tail should be key2"
        );
        assert!(
            unsafe { new_tail_ptr.as_ref().next.is_none() },
            "New tail's next should be None"
        );

        let head_ptr = cache.head.borrow().unwrap();
        assert_eq!(
            unsafe { head_ptr.as_ref().prev },
            None,
            "Head's prev should point to new tail (key2)"
        );
        assert_eq!(
            unsafe { head_ptr.as_ref().next },
            Some(new_tail_ptr),
            "Head's next should point to new tail (key2)"
        );
        assert_eq!(
            unsafe { new_tail_ptr.as_ref().next },
            None,
            "Double check new tail's next is None"
        );
    }

    #[test]
    fn test_detach_middle() {
        let mut cache = DumbLruPageCache::default();
        let key1 = insert_page(&mut cache, 1); // Tail
        let key2 = insert_page(&mut cache, 2); // Middle
        let key3 = insert_page(&mut cache, 3); // Middle
        let _key4 = insert_page(&mut cache, 4); // Head
        cache.verify_list_integrity();
        assert_eq!(cache.len(), 4);

        let head_ptr_before = cache.head.borrow().unwrap();
        let tail_ptr_before = cache.tail.borrow().unwrap();

        assert!(cache.delete(key2.clone()).is_ok()); // Detach a middle element (key2)

        assert_eq!(cache.len(), 3, "Length should be 3 after deleting middle");
        assert!(
            cache.map.borrow().get(&key2).is_none(),
            "Map should not contain deleted middle key2"
        );
        cache.verify_list_integrity();

        // Check neighbors
        let key1_ptr = cache.get_entry_ptr(&key1).expect("Key1 should still exist");
        let key3_ptr = cache.get_entry_ptr(&key3).expect("Key3 should still exist");
        assert_eq!(
            unsafe { key3_ptr.as_ref().next },
            Some(key1_ptr),
            "Key3's next should point to key1"
        );
        assert_eq!(
            unsafe { key1_ptr.as_ref().prev },
            Some(key3_ptr),
            "Key1's prev should point to key2"
        );

        assert_eq!(
            cache.head.borrow().unwrap(),
            head_ptr_before,
            "Head should remain key4"
        );
        assert_eq!(
            cache.tail.borrow().unwrap(),
            tail_ptr_before,
            "Tail should remain key1"
        );
    }

    #[test]
    #[ignore = "for now let's not track active refs"]
    fn test_detach_via_delete() {
        let mut cache = DumbLruPageCache::default();
        let key1 = create_key(1);
        let page1 = page_with_content(1);
        assert!(cache.insert(key1.clone(), page1.clone()).is_ok());
        assert!(page_has_content(&page1));
        cache.verify_list_integrity();

        let result = cache.delete(key1.clone());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), CacheError::ActiveRefs);
        assert_eq!(cache.len(), 1);

        drop(page1);

        assert!(cache.delete(key1).is_ok());
        assert_eq!(cache.len(), 0);
        cache.verify_list_integrity();
    }

    #[test]
    #[should_panic(expected = "Attempted to insert different page with same key")]
    fn test_insert_existing_key_fail() {
        let mut cache = DumbLruPageCache::default();
        let key1 = create_key(1);
        let page1_v1 = page_with_content(1);
        let page1_v2 = page_with_content(1);
        assert!(cache.insert(key1.clone(), page1_v1.clone()).is_ok());
        assert_eq!(cache.len(), 1);
        cache.verify_list_integrity();
        let _ = cache.insert(key1.clone(), page1_v2.clone()); // Panic
    }

    #[test]
    fn test_detach_nonexistent_key() {
        let mut cache = DumbLruPageCache::default();
        let key_nonexist = create_key(99);

        assert!(cache.delete(key_nonexist.clone()).is_ok()); // no-op
    }

    #[test]
    fn test_page_cache_evict() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.get(&key2).unwrap().get().id, 2);
        assert!(cache.get(&key1).is_none());
    }

    #[test]
    fn test_detach_locked_page() {
        let mut cache = DumbLruPageCache::default();
        let (_, mut entry) = insert_and_get_entry(&mut cache, 1);
        unsafe { entry.as_mut().page.set_locked() };
        assert_eq!(cache.detach(entry, false), Err(CacheError::Locked));
        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_dirty_page() {
        let mut cache = DumbLruPageCache::default();
        let (key, mut entry) = insert_and_get_entry(&mut cache, 1);
        cache.get(&key).expect("Page should exist");
        unsafe { entry.as_mut().page.set_dirty() };
        assert_eq!(
            cache.detach(entry, false),
            Err(CacheError::Dirty { pgno: 1 })
        );
        cache.verify_list_integrity();
    }

    #[test]
    #[ignore = "for now let's not track active refs"]
    fn test_detach_with_active_reference_clean() {
        let mut cache = DumbLruPageCache::default();
        let (key, entry) = insert_and_get_entry(&mut cache, 1);
        let page_ref = cache.get(&key);
        assert_eq!(cache.detach(entry, true), Err(CacheError::ActiveRefs));
        drop(page_ref);
        cache.verify_list_integrity();
    }

    #[test]
    #[ignore = "for now let's not track active refs"]
    fn test_detach_with_active_reference_no_clean() {
        let mut cache = DumbLruPageCache::default();
        let (key, entry) = insert_and_get_entry(&mut cache, 1);
        cache.get(&key).expect("Page should exist");
        assert!(cache.detach(entry, false).is_ok());
        assert!(cache.map.borrow_mut().remove(&key).is_some());
        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_without_cleaning() {
        let mut cache = DumbLruPageCache::default();
        let (key, entry) = insert_and_get_entry(&mut cache, 1);
        assert!(cache.detach(entry, false).is_ok());
        assert!(cache.map.borrow_mut().remove(&key).is_some());
        cache.verify_list_integrity();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_detach_with_cleaning() {
        let mut cache = DumbLruPageCache::default();
        let (key, entry) = insert_and_get_entry(&mut cache, 1);
        let page = cache.get(&key).expect("Page should exist");
        assert!(page_has_content(&page));
        drop(page);
        assert!(cache.detach(entry, true).is_ok());
        // Internal testing: the page is still in map, so we use it to check content
        let page = cache.peek(&key, false).expect("Page should exist in map");
        assert!(!page_has_content(&page));
        assert!(cache.map.borrow_mut().remove(&key).is_some());
        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_only_element_preserves_integrity() {
        let mut cache = DumbLruPageCache::default();
        let (_, entry) = insert_and_get_entry(&mut cache, 1);
        assert!(cache.detach(entry, false).is_ok());
        assert!(
            cache.head.borrow().is_none(),
            "Head should be None after detaching only element"
        );
        assert!(
            cache.tail.borrow().is_none(),
            "Tail should be None after detaching only element"
        );
    }

    #[test]
    fn test_detach_with_multiple_pages() {
        let mut cache = DumbLruPageCache::default();
        let (key1, _) = insert_and_get_entry(&mut cache, 1);
        let (key2, entry2) = insert_and_get_entry(&mut cache, 2);
        let (key3, _) = insert_and_get_entry(&mut cache, 3);
        let head_key = unsafe { cache.head.borrow().unwrap().as_ref().key.clone() };
        let tail_key = unsafe { cache.tail.borrow().unwrap().as_ref().key.clone() };
        assert_eq!(head_key, key3, "Head should be key3");
        assert_eq!(tail_key, key1, "Tail should be key1");
        assert!(cache.detach(entry2, false).is_ok());
        let head_entry = unsafe { cache.head.borrow().unwrap().as_ref() };
        let tail_entry = unsafe { cache.tail.borrow().unwrap().as_ref() };
        assert_eq!(head_entry.key, key3, "Head should still be key3");
        assert_eq!(tail_entry.key, key1, "Tail should still be key1");
        assert_eq!(
            unsafe { head_entry.next.unwrap().as_ref().key.clone() },
            key1,
            "Head's next should point to tail after middle element detached"
        );
        assert_eq!(
            unsafe { tail_entry.prev.unwrap().as_ref().key.clone() },
            key3,
            "Tail's prev should point to head after middle element detached"
        );
        assert!(cache.map.borrow_mut().remove(&key2).is_some());
        cache.verify_list_integrity();
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
        let mut cache = DumbLruPageCache::new(10);
        let mut lru = LruCache::new(NonZeroUsize::new(10).unwrap());

        for _ in 0..10000 {
            cache.print();
            for (key, _) in &lru {
                tracing::debug!("lru_page={:?}", key);
            }
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
                            // Any other error should fail the test
                            panic!("Cache insertion failed: {err:?}");
                        }
                        Ok(_) => {
                            lru.push(key, page);
                        }
                    }
                    assert!(cache.len() <= 10);
                }
                1 => {
                    // remove
                    let random = rng.next_u64() % 2 == 0;
                    let key = if random || lru.is_empty() {
                        let id_page: u64 = rng.next_u64() % max_pages;

                        PageCacheKey::new(id_page as usize)
                    } else {
                        let i = rng.next_u64() as usize % lru.len();
                        let key: PageCacheKey = lru.iter().nth(i).unwrap().0.clone();
                        key
                    };
                    tracing::debug!("removing page {:?}", key);
                    lru.pop(&key);
                    assert!(cache.delete(key).is_ok());
                }
                _ => unreachable!(),
            }
            compare_to_lru(&mut cache, &lru);
            cache.print();
            for (key, _) in &lru {
                tracing::debug!("lru_page={:?}", key);
            }
            cache.verify_list_integrity();
            for (key, page) in &lru {
                println!("getting page {key:?}");
                cache.peek(key, false).unwrap();
                assert_eq!(page.get().id, key.pgno);
            }
        }
    }

    pub fn compare_to_lru(cache: &mut DumbLruPageCache, lru: &LruCache<PageCacheKey, PageRef>) {
        let this_keys = cache.keys();
        let mut lru_keys = Vec::new();
        for (lru_key, _) in lru {
            lru_keys.push(lru_key.clone());
        }
        if this_keys != lru_keys {
            cache.print();
            for (lru_key, _) in lru {
                tracing::debug!("lru_page={:?}", lru_key);
            }
            assert_eq!(&this_keys, &lru_keys)
        }
    }

    #[test]
    fn test_page_cache_insert_and_get() {
        let mut cache = DumbLruPageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert_eq!(cache.get(&key1).unwrap().get().id, 1);
        assert_eq!(cache.get(&key2).unwrap().get().id, 2);
    }

    #[test]
    fn test_page_cache_over_capacity() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        let key3 = insert_page(&mut cache, 3);
        assert!(cache.get(&key1).is_none());
        assert_eq!(cache.get(&key2).unwrap().get().id, 2);
        assert_eq!(cache.get(&key3).unwrap().get().id, 3);
    }

    #[test]
    fn test_page_cache_delete() {
        let mut cache = DumbLruPageCache::default();
        let key1 = insert_page(&mut cache, 1);
        assert!(cache.delete(key1.clone()).is_ok());
        assert!(cache.get(&key1).is_none());
    }

    #[test]
    fn test_page_cache_clear() {
        let mut cache = DumbLruPageCache::default();
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert!(cache.clear().is_ok());
        assert!(cache.get(&key1).is_none());
        assert!(cache.get(&key2).is_none());
    }

    #[test]
    fn test_page_cache_insert_sequential() {
        let mut cache = DumbLruPageCache::default();
        for i in 0..10000 {
            let key = insert_page(&mut cache, i);
            assert_eq!(cache.peek(&key, false).unwrap().get().id, i);
        }
    }

    #[test]
    fn test_resize_smaller_success() {
        let mut cache = DumbLruPageCache::default();
        for i in 1..=5 {
            let _ = insert_page(&mut cache, i);
        }
        assert_eq!(cache.len(), 5);
        let result = cache.resize(3);
        assert_eq!(result, CacheResizeResult::Done);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.capacity, 3);
        assert!(cache.insert(create_key(6), page_with_content(6)).is_ok());
    }

    #[test]
    #[should_panic(expected = "Attempted to insert different page with same key")]
    fn test_resize_larger() {
        let mut cache = DumbLruPageCache::default();
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
        // FIXME: For now this will assert because we cannot insert a page with same id but different contents of page.
        assert!(cache.insert(create_key(4), page_with_content(4)).is_err());
        cache.verify_list_integrity();
    }

    #[test]
    #[ignore = "for now let's not track active refs"]
    fn test_resize_with_active_references() {
        let mut cache = DumbLruPageCache::default();
        let page1 = page_with_content(1);
        let page2 = page_with_content(2);
        let page3 = page_with_content(3);
        assert!(cache.insert(create_key(1), page1.clone()).is_ok());
        assert!(cache.insert(create_key(2), page2.clone()).is_ok());
        assert!(cache.insert(create_key(3), page3.clone()).is_ok());
        assert_eq!(cache.len(), 3);
        cache.verify_list_integrity();
        assert_eq!(cache.resize(2), CacheResizeResult::PendingEvictions);
        assert_eq!(cache.capacity, 2);
        assert_eq!(cache.len(), 3);
        drop(page2);
        drop(page3);
        assert_eq!(cache.resize(1), CacheResizeResult::Done); // Evicted 2 and 3
        assert_eq!(cache.len(), 1);
        assert!(cache.insert(create_key(4), page_with_content(4)).is_err());
        cache.verify_list_integrity();
    }

    #[test]
    fn test_resize_same_capacity() {
        let mut cache = DumbLruPageCache::new(3);
        for i in 1..=3 {
            let _ = insert_page(&mut cache, i);
        }
        let result = cache.resize(3);
        assert_eq!(result, CacheResizeResult::Done); // no-op
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.capacity, 3);
        cache.verify_list_integrity();
        assert!(cache.insert(create_key(4), page_with_content(4)).is_ok());
    }

    #[test]
    #[ignore = "long running test, remove to verify"]
    fn test_clear_memory_stability() {
        let initial_memory = memory_stats::memory_stats().unwrap().physical_mem;

        for _ in 0..100000 {
            let mut cache = DumbLruPageCache::new(1000);

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
