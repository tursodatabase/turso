use std::{cell::RefCell, collections::HashMap, ptr::NonNull};

use std::sync::Arc;
use tracing::{debug, trace};

use super::pager::PageRef;

// In limbo, page cache is shared by default, meaning that multiple frames from WAL can reside in
// the cache, meaning, we need a way to differentiate between pages cached in different
// connections. For this we include the max_frame that a connection will read from so that if two
// connections have different max_frames, they might or not have different frame read from WAL.
//
// WAL was introduced after Shared cache in SQLite, so this is why these two features don't work
// well together because pages with different snapshots may collide.
#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct PageCacheKey {
    pgno: usize,
    max_frame: Option<u64>,
}

#[allow(dead_code)]
struct PageCacheEntry {
    key: PageCacheKey,
    page: PageRef,
    prev: Option<NonNull<PageCacheEntry>>,
    next: Option<NonNull<PageCacheEntry>>,
}

impl PageCacheEntry {
    fn as_non_null(&mut self) -> NonNull<PageCacheEntry> {
        NonNull::new(&mut *self).unwrap()
    }
}

pub struct DumbLruPageCache {
    capacity: usize,
    map: RefCell<HashMap<PageCacheKey, NonNull<PageCacheEntry>>>,
    head: RefCell<Option<NonNull<PageCacheEntry>>>,
    tail: RefCell<Option<NonNull<PageCacheEntry>>>,
}
unsafe impl Send for DumbLruPageCache {}
unsafe impl Sync for DumbLruPageCache {}

#[derive(Debug, PartialEq)]
pub enum CacheError {
    Locked,
    Dirty,
    ActiveRefs,
}

impl PageCacheKey {
    pub fn new(pgno: usize, max_frame: Option<u64>) -> Self {
        Self { pgno, max_frame }
    }
}
impl DumbLruPageCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: RefCell::new(HashMap::new()),
            head: RefCell::new(None),
            tail: RefCell::new(None),
        }
    }

    pub fn contains_key(&mut self, key: &PageCacheKey) -> bool {
        self.map.borrow().contains_key(key)
    }

    pub fn insert(&mut self, key: PageCacheKey, value: PageRef) -> Result<(), CacheError> {
        trace!("cache_insert(key={:?})", key);
        self._delete(key.clone(), false)?;
        if self.len() >= self.capacity {
            // Make room before trying to insert
            self.pop_if_not_dirty()?;
        }
        let entry = Box::new(PageCacheEntry {
            key: key.clone(),
            next: None,
            prev: None,
            page: value,
        });
        let ptr_raw = Box::into_raw(entry);
        let ptr = unsafe { ptr_raw.as_mut().unwrap().as_non_null() };
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
        unsafe { std::ptr::drop_in_place(ptr.as_ptr()) };
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

    pub fn resize(&mut self, capacity: usize) {
        let _ = capacity;
        todo!();
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
            return Err(CacheError::Dirty);
        }

        if clean_page {
            if let Some(page_mut) = Arc::get_mut(&mut entry_mut.page) {
                page_mut.clear_loaded();
                debug!("cleaning up page {}", page_mut.get().id);
                let _ = page_mut.get().contents.take();
            } else {
                let page_id = unsafe { &entry.as_mut().page.get().id };
                debug!(
                    "detach page {}: can't clean, there are other references",
                    page_id
                );
                return Err(CacheError::ActiveRefs);
            }
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

    fn pop_if_not_dirty(&mut self) -> Result<(), CacheError> {
        let tail = *self.tail.borrow();
        if tail.is_none() {
            return Ok(());
        }

        let mut tail = tail.unwrap();
        let tail_entry = unsafe { tail.as_mut() };
        tracing::debug!("pop_if_not_dirty(key={:?})", tail_entry.key);
        let key = tail_entry.key.clone();

        // TODO: drop from another clean entry?
        self.detach(tail, true)?;

        assert!(self.map.borrow_mut().remove(&key).is_some());
        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), CacheError> {
        let keys_to_remove: Vec<PageCacheKey> = self.map.borrow().keys().cloned().collect();
        for key in keys_to_remove {
            self.delete(key)?;
        }
        assert!(self.head.borrow().is_none());
        assert!(self.tail.borrow().is_none());
        assert!(self.map.borrow().is_empty());
        Ok(())
    }

    pub fn print(&mut self) {
        println!("page_cache={}", self.map.borrow().len());
        println!("page_cache={:?}", self.map.borrow())
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
        let tail_ptr = *self.tail.borrow();

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
                    "Infinite loop suspected in forward integrity check. Size {}, count {}",
                    map_len, forward_count
                );
            }
        }
        assert_eq!(
            forward_count, map_len,
            "Forward count mismatch (counted {}, map has {})",
            forward_count, map_len
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
                    "Infinite loop suspected in backward integrity check. Size {}, count {}",
                    map_len, backward_count
                );
            }
        }
        assert_eq!(
            backward_count, map_len,
            "Backward count mismatch (counted {}, map has {})",
            backward_count, map_len
        );
        assert_eq!(
            head_ptr, last_ptr,
            "Head pointer mismatch after backward traversal"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{Buffer, BufferData};
    use crate::storage::page_cache::CacheError;
    use crate::storage::pager::{Page, PageRef};
    use crate::storage::sqlite3_ondisk::PageContent;
    use std::{cell::RefCell, num::NonZeroUsize, pin::Pin, rc::Rc, sync::Arc};

    use lru::LruCache;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    fn create_key(id: usize) -> PageCacheKey {
        PageCacheKey::new(id, Some(id as u64))
    }

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
        let mut cache = DumbLruPageCache::new(5);
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
        let mut cache = DumbLruPageCache::new(5);
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
        let mut cache = DumbLruPageCache::new(5);
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
        let mut cache = DumbLruPageCache::new(5);
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
    fn test_detach_via_delete() {
        let mut cache = DumbLruPageCache::new(5);
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
    fn test_detach_via_insert() {
        let mut cache = DumbLruPageCache::new(5);
        let key1 = create_key(1);
        let page1_v1 = page_with_content(1);
        let page1_v2 = page_with_content(1);

        assert!(cache.insert(key1.clone(), page1_v1.clone()).is_ok());
        assert_eq!(cache.len(), 1);
        cache.verify_list_integrity();

        // Fail to insert v2 as v1 is still referenced
        let result = cache.insert(key1.clone(), page1_v2.clone());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), CacheError::ActiveRefs);
        assert_eq!(cache.len(), 1); // Page v1 should remain in cache
        assert!(page_has_content(&page1_v1));

        drop(page1_v1);
        assert!(cache.insert(key1.clone(), page1_v2.clone()).is_ok());
        assert_eq!(cache.len(), 1);
        assert!(page_has_content(&page1_v2));

        let current_page = cache.get(&key1).unwrap();
        assert!(
            Arc::ptr_eq(&current_page, &page1_v2),
            "Cache should now hold page1 V2"
        );

        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_nonexistent_key() {
        let mut cache = DumbLruPageCache::new(5);
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
        let mut cache = DumbLruPageCache::new(5);
        let (_, mut entry) = insert_and_get_entry(&mut cache, 1);
        unsafe { entry.as_mut().page.set_locked() };
        assert_eq!(cache.detach(entry, false), Err(CacheError::Locked));
        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_dirty_page() {
        let mut cache = DumbLruPageCache::new(5);
        let (key, mut entry) = insert_and_get_entry(&mut cache, 1);
        cache.get(&key).expect("Page should exist");
        unsafe { entry.as_mut().page.set_dirty() };
        assert_eq!(cache.detach(entry, false), Err(CacheError::Dirty));
        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_with_active_reference_clean() {
        let mut cache = DumbLruPageCache::new(5);
        let (key, entry) = insert_and_get_entry(&mut cache, 1);
        let page_ref = cache.get(&key);
        assert_eq!(cache.detach(entry, true), Err(CacheError::ActiveRefs));
        drop(page_ref);
        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_with_active_reference_no_clean() {
        let mut cache = DumbLruPageCache::new(5);
        let (key, entry) = insert_and_get_entry(&mut cache, 1);
        cache.get(&key).expect("Page should exist");
        assert!(cache.detach(entry, false).is_ok());
        assert!(cache.map.borrow_mut().remove(&key).is_some());
        cache.verify_list_integrity();
    }

    #[test]
    fn test_detach_without_cleaning() {
        let mut cache = DumbLruPageCache::new(5);
        let (key, entry) = insert_and_get_entry(&mut cache, 1);
        assert!(cache.detach(entry, false).is_ok());
        assert!(cache.map.borrow_mut().remove(&key).is_some());
        cache.verify_list_integrity();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_detach_with_cleaning() {
        let mut cache = DumbLruPageCache::new(5);
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
        let mut cache = DumbLruPageCache::new(5);
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
        let mut cache = DumbLruPageCache::new(5);
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
            match rng.next_u64() % 3 {
                0 => {
                    // add
                    let id_page = rng.next_u64() % max_pages;
                    let id_frame = rng.next_u64() % max_pages;
                    let key = PageCacheKey::new(id_page as usize, Some(id_frame));
                    #[allow(clippy::arc_with_non_send_sync)]
                    let page = Arc::new(Page::new(id_page as usize));
                    lru.push(key.clone(), page.clone());
                    // println!("inserting page {:?}", key);
                    cache.insert(key.clone(), page); // move page so there's no reference left here
                    assert!(cache.len() <= 10);
                }
                1 => {
                    // remove
                    let random = rng.next_u64() % 2 == 0;
                    let key = if random || lru.is_empty() {
                        let id_page = rng.next_u64() % max_pages;
                        let id_frame = rng.next_u64() % max_pages;
                        let key = PageCacheKey::new(id_page as usize, Some(id_frame));
                        key
                    } else {
                        let i = rng.next_u64() as usize % lru.len();
                        let key = lru.iter().skip(i).next().unwrap().0.clone();
                        key
                    };
                    // println!("removing page {:?}", key);
                    lru.pop(&key);
                    cache.delete(key);
                }
                2 => {
                    // test contents
                    for (key, page) in &lru {
                        // println!("getting page {:?}", key);
                        cache.peek(&key, false).unwrap();
                        assert_eq!(page.get().id, key.pgno);
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_page_cache_insert_and_get() {
        let mut cache = DumbLruPageCache::new(2);
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
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        assert!(cache.delete(key1.clone()).is_ok());
        assert!(cache.get(&key1).is_none());
    }

    #[test]
    fn test_page_cache_clear() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1);
        let key2 = insert_page(&mut cache, 2);
        assert!(cache.clear().is_ok());
        assert!(cache.get(&key1).is_none());
        assert!(cache.get(&key2).is_none());
    }

    #[test]
    fn test_page_cache_insert_sequential() {
        let mut cache = DumbLruPageCache::new(2);
        for i in 0..10000 {
            let key = insert_page(&mut cache, i);
            assert_eq!(cache.peek(&key, false).unwrap().get().id, i);
        }
    }
}
