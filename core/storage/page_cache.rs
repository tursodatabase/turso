use std::{cell::RefCell, collections::HashMap, ptr::NonNull};

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

    pub fn insert(&mut self, key: PageCacheKey, value: PageRef) {
        self._delete(key.clone(), false);
        trace!("cache_insert(key={:?})", key);
        // TODO: fail to insert if couldn't make room for page
        let _ = self.evict();
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
    }

    pub fn delete(&mut self, key: PageCacheKey) {
        trace!("cache_delete(key={:?})", key);
        self._delete(key, true)
    }

    pub fn _delete(&mut self, key: PageCacheKey, clean_page: bool) {
        let ptr = self.map.borrow_mut().remove(&key);
        if ptr.is_none() {
            return;
        }
        let ptr = ptr.unwrap();
        self.detach(ptr, clean_page);
        unsafe { std::ptr::drop_in_place(ptr.as_ptr()) };
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
            self.detach(ptr, false);
            self.touch(ptr);
        }
        Some(page)
    }

    pub fn resize(&mut self, capacity: usize) {
        let _ = capacity;
        todo!();
    }

    fn detach(&mut self, mut entry: NonNull<PageCacheEntry>, clean_page: bool) {
        if clean_page {
            // evict buffer
            let page = unsafe { &entry.as_mut().page };
            page.clear_loaded();
            debug!("cleaning up page {}", page.get().id);
            let _ = page.get().contents.take();
        }

        let (next, prev) = unsafe {
            let c = entry.as_mut();
            let next = c.next;
            let prev = c.prev;
            c.prev = None;
            c.next = None;
            (next, prev)
        };

        // detach
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

    // Tries to evict pages until there's capacity for one more page
    #[must_use]
    fn evict(&mut self) -> bool {
        if self.len() < self.capacity {
            return true;
        }

        let mut current = match *self.tail.borrow() {
            Some(tail) => tail,
            None => {
                debug!("evict() nothing at tail");
                return false;
            }
        };

        while self.len() >= self.capacity {
            let current_entry = unsafe { current.as_mut() };

            if current_entry.page.is_dirty() || current_entry.page.is_locked() {
                match current_entry.prev {
                    Some(prev) => current = prev,
                    None => {
                        debug!("evict() not enough clean and unlocked pages to evict");
                        return false;
                    }
                }
                continue;
            }

            debug!("evict(key={:?})", current_entry.key);

            let prev = current_entry.prev;
            let key_to_remove = current_entry.key.clone();
            self.detach(current, true);
            assert!(self.map.borrow_mut().remove(&key_to_remove).is_some());
            if prev.is_none() {
                break;
            }
            current = prev.unwrap();
        }
        true
    }

    pub fn clear(&mut self) {
        let to_remove: Vec<PageCacheKey> = self.map.borrow().iter().map(|v| v.0.clone()).collect();
        for key in to_remove {
            self.delete(key);
        }
    }

    pub fn print(&mut self) {
        println!("page_cache={}", self.map.borrow().len());
        println!("page_cache={:?}", self.map.borrow())
    }

    pub fn len(&self) -> usize {
        self.map.borrow().len()
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::Arc};

    use lru::LruCache;
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };

    use crate::{storage::page_cache::DumbLruPageCache, Page};

    use super::PageCacheKey;

    #[derive(Clone, Copy)]
    pub enum DirtyState {
        Clean,
        Dirty,
    }

    #[derive(Clone, Copy)]
    pub enum LockState {
        Unlocked,
        Locked,
    }

    #[must_use]
    fn insert_page(
        cache: &mut DumbLruPageCache,
        id: usize,
        dirty: DirtyState,
        lock: LockState,
    ) -> PageCacheKey {
        let key = PageCacheKey::new(id, None);
        #[allow(clippy::arc_with_non_send_sync)]
        let page = Arc::new(Page::new(id));

        match dirty {
            DirtyState::Dirty => page.set_dirty(),
            DirtyState::Clean => (),
        }

        match lock {
            LockState::Locked => page.set_locked(),
            LockState::Unlocked => (),
        }
        cache.insert(key.clone(), page.clone());
        key
    }

    #[test]
    fn test_page_cache_evict() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page(&mut cache, 1, DirtyState::Clean, LockState::Unlocked);
        let key2 = insert_page(&mut cache, 2, DirtyState::Clean, LockState::Unlocked);
        assert_eq!(cache.get(&key2).unwrap().get().id, 2);
        assert!(cache.get(&key1).is_none());
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
                    // println!("inserting page {:?}", key);
                    cache.insert(key.clone(), page.clone());
                    lru.push(key, page);
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
        let key1 = insert_page(&mut cache, 1, DirtyState::Clean, LockState::Unlocked);
        let key2 = insert_page(&mut cache, 2, DirtyState::Clean, LockState::Unlocked);
        assert_eq!(cache.get(&key1).unwrap().get().id, 1);
        assert_eq!(cache.get(&key2).unwrap().get().id, 2);
    }

    #[test]
    fn test_page_cache_over_capacity() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1, DirtyState::Clean, LockState::Unlocked);
        let key2 = insert_page(&mut cache, 2, DirtyState::Clean, LockState::Unlocked);
        let key3 = insert_page(&mut cache, 3, DirtyState::Clean, LockState::Unlocked);
        assert!(cache.get(&key1).is_none());
        assert_eq!(cache.get(&key2).unwrap().get().id, 2);
        assert_eq!(cache.get(&key3).unwrap().get().id, 3);
    }

    #[test]
    fn test_page_cache_delete() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1, DirtyState::Clean, LockState::Unlocked);
        cache.delete(key1.clone());
        assert!(cache.get(&key1).is_none());
    }

    #[test]
    fn test_page_cache_clear() {
        let mut cache = DumbLruPageCache::new(2);
        let key1 = insert_page(&mut cache, 1, DirtyState::Clean, LockState::Unlocked);
        let key2 = insert_page(&mut cache, 2, DirtyState::Clean, LockState::Unlocked);
        cache.clear();
        assert!(cache.get(&key1).is_none());
        assert!(cache.get(&key2).is_none());
    }

    #[test]
    fn test_page_cache_insert_sequential() {
        let mut cache = DumbLruPageCache::new(2);
        for i in 0..10000 {
            let key = insert_page(&mut cache, i, DirtyState::Clean, LockState::Unlocked);
            assert_eq!(cache.peek(&key, false).unwrap().get().id, i);
        }
    }

    #[test]
    fn test_page_cache_evict_one() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page(&mut cache, 1, DirtyState::Clean, LockState::Unlocked);
        let _ = insert_page(&mut cache, 2, DirtyState::Clean, LockState::Unlocked);
        assert!(cache.get(&key1).is_none());
    }

    #[test]
    fn test_page_cache_no_evict_dirty() {
        let mut cache = DumbLruPageCache::new(1);
        let _ = insert_page(&mut cache, 1, DirtyState::Dirty, LockState::Unlocked);
        assert!(cache.evict() == false);
        assert!(cache.len() == 1);
    }

    #[test]
    fn test_page_cache_no_evict_locked() {
        let mut cache = DumbLruPageCache::new(1);
        let _ = insert_page(&mut cache, 1, DirtyState::Clean, LockState::Locked);
        assert!(cache.evict() == false);
        assert!(cache.len() == 1);
    }

    #[test]
    fn test_page_cache_no_evict_one_dirty() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page(&mut cache, 1, DirtyState::Dirty, LockState::Unlocked);
        let _ = insert_page(&mut cache, 2, DirtyState::Clean, LockState::Unlocked); // fails to evict 1
        assert!(cache.get(&key1).is_some());
    }

    #[test]
    fn test_page_cache_no_evict_one_locked() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page(&mut cache, 1, DirtyState::Clean, LockState::Locked);
        let _ = insert_page(&mut cache, 2, DirtyState::Clean, LockState::Unlocked); // fails to evict 1
        assert!(cache.get(&key1).is_some());
    }

    #[test]
    fn test_page_cache_evict_one_after_cleaned() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page(&mut cache, 1, DirtyState::Dirty, LockState::Unlocked);
        let _ = insert_page(&mut cache, 2, DirtyState::Clean, LockState::Unlocked);
        let entry1 = cache.get(&key1);
        assert!(entry1.is_some());
        entry1.unwrap().clear_dirty();
        let _ = insert_page(&mut cache, 3, DirtyState::Clean, LockState::Unlocked);
        assert!(cache.get(&key1).is_none());
    }

    #[test]
    fn test_page_cache_evict_one_after_unlocked() {
        let mut cache = DumbLruPageCache::new(1);
        let key1 = insert_page(&mut cache, 1, DirtyState::Clean, LockState::Locked);
        let _ = insert_page(&mut cache, 2, DirtyState::Clean, LockState::Unlocked);
        let entry1 = cache.get(&key1);
        assert!(entry1.is_some());
        entry1.unwrap().clear_locked();
        let _ = insert_page(&mut cache, 3, DirtyState::Clean, LockState::Unlocked);
        assert!(cache.get(&key1).is_none());
    }
}
