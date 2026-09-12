use crate::sync::Arc;

use crate::sync::RwLock;

use crate::io::{MemoryIO, OpenFlags, IO};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::DatabaseFile;
use crate::storage::page_cache::{PageCache, PageCacheKey};
use crate::storage::wal::{Wal, WalFile, WalFileShared};
use crate::util::IOExt;
use arc_swap::ArcSwapOption;

use super::{default_page1, CacheFlushState, CollectingState, Page, PageRef, Pager};
use crate::{Buffer, Completion, CompletionError, LimboError};

#[test]
fn page_id_changes_keep_header_access_at_the_correct_offset() {
    let mut page = super::PageInner::from_buffer(Buffer::new_temporary(4096));
    for id in [1, 2, 1, 0, usize::MAX] {
        page.set_id(id);
        assert_eq!(page.id(), id);
        let offset = if id == 1 { 100 } else { 0 };
        assert_eq!(page.offset(), offset);
        page.as_ptr().fill(0);
        page.write_page_type(super::PageType::TableLeaf as u8);
        assert_eq!(page.as_ptr()[offset], super::PageType::TableLeaf as u8);
        assert_eq!(page.page_type().unwrap(), super::PageType::TableLeaf);

        let unloaded = super::PageInner::unloaded(id);
        assert_eq!(unloaded.id(), id);
        assert_eq!(unloaded.offset(), offset);
    }
}

fn pager_with_cache_capacity(cache_capacity: usize, database_pages: u32) -> Arc<Pager> {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let buffer_pool = BufferPool::begin_init(&io, 4096 * 128);

    let db_file = Arc::new(DatabaseFile::new(
        io.open_file(":memory:", OpenFlags::Create, false).unwrap(),
    ));

    let wal_file = io.open_file("test.wal", OpenFlags::Create, false).unwrap();
    let wal_shared = WalFileShared::new_shared(wal_file).unwrap();
    let last_checksum_and_max_frame = wal_shared.read().last_checksum_and_max_frame();
    let wal: Arc<dyn Wal> = Arc::new(WalFile::new(
        io.clone(),
        wal_shared,
        last_checksum_and_max_frame,
        buffer_pool.clone(),
    ));

    let init_page_1 = Arc::new(ArcSwapOption::new(Some(default_page1(None))));
    let pager = Arc::new(
        Pager::new(
            db_file,
            Some(wal),
            io,
            PageCache::new(cache_capacity),
            buffer_pool,
            Arc::new(crate::sync::Mutex::new(())),
            init_page_1,
        )
        .unwrap(),
    );

    pager.io.step().unwrap();
    pager.io.block(|| pager.allocate_page1()).unwrap();
    for _ in 0..(database_pages - 1) {
        pager.io.block(|| pager.allocate_page()).unwrap();
    }
    pager
}

/// The page cache capacity is a soft limit, as in SQLite: when every
/// resident page is unevictable (held by cursors, dirty and unspillable),
/// a read must still succeed by admitting the page over capacity instead
/// of failing with Busy. The excess drains once pages become evictable.
#[test]
fn read_page_exceeds_capacity_when_cache_unevictable() {
    const CAP: usize = 5;
    let pager = pager_with_cache_capacity(CAP, 6);

    // Allocating 6 pages against a 5-page cache forces a spill and evicts
    // at least one spilled page; find one that is no longer resident.
    let missing = (2..=6)
        .find(|&id| !pager.page_cache.read().contains_key(&PageCacheKey::new(id)))
        .expect("allocating 6 pages with a 5-page cache must evict at least one page")
        as i64;

    // Hold strong references to every resident page so none can be
    // evicted or spilled.
    let held: Vec<PageRef> = (1..=6)
        .filter_map(|id| pager.cache_get(id).unwrap())
        .collect();
    assert_eq!(held.len(), CAP, "cache should be at capacity");

    let (page, c) = pager.io.block(|| pager.read_page(missing)).unwrap();
    if let Some(c) = c {
        pager.io.wait_for_completion(c).unwrap();
    }
    while page.is_locked() {
        pager.io.step().unwrap();
    }
    assert_eq!(page.get().id() as i64, missing);
    assert!(
        pager.page_cache.read().len() > CAP,
        "page must have been admitted over capacity"
    );

    // Once the strong references are gone, the next insert drains the
    // excess back under capacity.
    drop(held);
    drop(page);
    pager.io.block(|| pager.allocate_page()).unwrap();
    assert!(
        pager.page_cache.read().len() <= CAP,
        "excess over capacity must drain once pages become evictable"
    );
}

/// Same soft-limit guarantee for the write path: allocating a new page
/// while the cache is full of unevictable pages must not fail.
#[test]
fn allocate_page_exceeds_capacity_when_cache_unevictable() {
    const CAP: usize = 5;
    let pager = pager_with_cache_capacity(CAP, 5);

    // Hold strong references to all resident pages: dirty pages with
    // outstanding references can neither be spilled nor evicted.
    let held: Vec<PageRef> = (1..=5)
        .filter_map(|id| pager.cache_get(id).unwrap())
        .collect();
    assert_eq!(held.len(), CAP, "cache should be at capacity");

    let page = pager.io.block(|| pager.allocate_page()).unwrap();
    assert_eq!(page.get().id(), 6);
    assert!(
        pager.page_cache.read().len() > CAP,
        "page must have been admitted over capacity"
    );
}

/// Verifies that cacheflush returns a codec error when rereading an evicted page fails,
/// and resets its state so a later flush can retry.
#[test]
fn cacheflush_propagates_failed_page_codec_reread() {
    let pager = pager_with_cache_capacity(5, 2);
    let page = Arc::new(Page::new(2));
    page.set_loaded();
    let completion =
        Completion::new_read(Arc::new(Buffer::new_temporary(4096)), Box::new(|_| None));
    completion.error(CompletionError::PageCodecError { page_idx: 2 });
    *pager.cacheflush_state.write() = CacheFlushState::WaitingForRead {
        state: CollectingState::default(),
        page_id: 2,
        page,
        completion,
    };

    let err = pager.cacheflush().unwrap_err();
    assert!(matches!(
        *err,
        LimboError::CompletionError(CompletionError::PageCodecError { page_idx: 2 })
    ));
    assert!(matches!(
        *pager.cacheflush_state.read(),
        CacheFlushState::Init
    ));
}

#[test]
fn test_shared_cache() {
    // ensure cache can be shared between threads
    let cache = Arc::new(RwLock::new(PageCache::new(10)));

    let thread = {
        let cache = cache.clone();
        std::thread::spawn(move || {
            let mut cache = cache.write();
            let page_key = PageCacheKey::new(1);
            let page = Page::new(1);
            // Set loaded so that we avoid eviction, as we evict the page from cache if it is not locked and not loaded
            page.set_loaded();
            cache.insert(page_key, Arc::new(page)).unwrap();
        })
    };
    let _ = thread.join();
    let mut cache = cache.write();
    let page_key = PageCacheKey::new(1);
    let page = cache.get(&page_key).unwrap();
    assert_eq!(page.unwrap().get().id(), 1);
}
