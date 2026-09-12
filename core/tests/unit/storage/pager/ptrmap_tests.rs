use crate::sync::Arc;

use super::ptrmap::*;
use super::*;
use crate::io::{MemoryIO, OpenFlags, IO};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::database::DatabaseFile;
use crate::storage::page_cache::PageCache;
use crate::storage::pager::{default_page1, Pager};
use crate::storage::sqlite3_ondisk::PageSize;
use crate::storage::wal::{WalFile, WalFileShared};
use arc_swap::ArcSwapOption;

pub fn run_until_done<T>(mut action: impl FnMut() -> IOResultOr<T>, pager: &Pager) -> Result<T> {
    loop {
        match action()? {
            IOResult::Done(res) => {
                return Ok(res);
            }
            IOResult::IO(io) => io.wait(pager.io.as_ref())?,
        }
    }
}
// Helper to create a Pager for testing
fn test_pager_setup(page_size: u32, initial_db_pages: u32) -> Pager {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db_file: Arc<dyn DatabaseStorage> = Arc::new(DatabaseFile::new(
        io.open_file("test.db", OpenFlags::Create, true).unwrap(),
    ));

    //  Construct interfaces for the pager
    let pages = initial_db_pages + 10;
    let sz = std::cmp::max(std::cmp::min(pages, 64), pages);
    let buffer_pool = BufferPool::begin_init(&io, (sz * page_size) as usize);

    let wal_shared = WalFileShared::new_shared(
        io.open_file("test.db-wal", OpenFlags::Create, false)
            .unwrap(),
    )
    .unwrap();
    let last_checksum_and_max_frame = wal_shared.read().last_checksum_and_max_frame();
    let wal: Arc<dyn Wal> = Arc::new(WalFile::new(
        io.clone(),
        wal_shared,
        last_checksum_and_max_frame,
        buffer_pool.clone(),
    ));

    // For new empty databases, init_page_1 must be Some(page) so allocate_page1() can be called
    let init_page_1 = Arc::new(ArcSwapOption::new(Some(default_page1(None))));
    let pager = Pager::new(
        db_file,
        Some(wal),
        io,
        PageCache::new(sz as usize),
        buffer_pool,
        Arc::new(Mutex::new(())),
        init_page_1,
    )
    .unwrap();
    run_until_done(|| pager.allocate_page1(), &pager).unwrap();
    {
        let page_cache = pager.page_cache.read();
        println!(
            "Cache Len: {} Cap: {}",
            page_cache.len(),
            page_cache.capacity()
        );
    }
    pager
        .persist_auto_vacuum_mode(AutoVacuumMode::Full)
        .unwrap();

    //  Allocate all the pages as btree root pages
    const EXPECTED_FIRST_ROOT_PAGE_ID: u32 = 3; // page1 = 1,  first ptrmap page = 2, root page = 3
    for i in 0..initial_db_pages {
        let res = run_until_done(
            || pager.btree_create(&CreateBTreeFlags::new_table()),
            &pager,
        );
        {
            let page_cache = pager.page_cache.read();
            println!(
                "i: {} Cache Len: {} Cap: {}",
                i,
                page_cache.len(),
                page_cache.capacity()
            );
        }
        match res {
            Ok(root_page_id) => {
                assert_eq!(root_page_id, EXPECTED_FIRST_ROOT_PAGE_ID + i);
            }
            Err(e) => {
                panic!("test_pager_setup: btree_create failed: {e:?}");
            }
        }
    }

    pager
}

#[test]
fn persist_auto_vacuum_mode_updates_fresh_header_without_dirty_pages() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db_file: Arc<dyn DatabaseStorage> = Arc::new(DatabaseFile::new(
        io.open_file("fresh-auto-vacuum.db", OpenFlags::Create, true)
            .unwrap(),
    ));
    let buffer_pool = BufferPool::begin_init(&io, 65536);
    let pager = Pager::new(
        db_file,
        None,
        io,
        PageCache::new(4),
        buffer_pool,
        Arc::new(Mutex::new(())),
        Arc::new(ArcSwapOption::new(Some(default_page1(None)))),
    )
    .unwrap();

    pager
        .persist_auto_vacuum_mode(AutoVacuumMode::Incremental)
        .unwrap();

    let IOResult::Done((largest_root_page, incremental_vacuum_enabled)) = pager
        .with_header(|header| {
            (
                header.vacuum_mode_largest_root_page.get(),
                header.incremental_vacuum_enabled.get(),
            )
        })
        .unwrap()
    else {
        panic!("fresh database header reads should not do any IO");
    };

    assert_eq!(largest_root_page, 1);
    assert_eq!(incremental_vacuum_enabled, 1);
    assert_eq!(pager.get_auto_vacuum_mode(), AutoVacuumMode::Incremental);
    assert!(
        pager.dirty_pages.read().is_empty(),
        "fresh-db auto-vacuum setup must not leave dirty pages behind"
    );
}

#[test]
fn test_ptrmap_page_allocation() {
    let page_size = 4096;
    let initial_db_pages = 10;
    let pager = test_pager_setup(page_size, initial_db_pages);

    // Page 5 should be mapped by ptrmap page 2.
    let db_page_to_update: u32 = 5;
    let expected_ptrmap_pg_no =
        get_ptrmap_page_no_for_db_page(db_page_to_update, page_size as usize);
    assert_eq!(expected_ptrmap_pg_no, FIRST_PTRMAP_PAGE_NO);

    //  Ensure the pointer map page ref is created and loadable via the pager
    let ptrmap_page_ref = pager
        .io
        .block(|| pager.read_page(expected_ptrmap_pg_no as i64));
    assert!(ptrmap_page_ref.is_ok());

    //  Ensure that the database header size is correctly reflected
    assert_eq!(
        pager
            .io
            .block(|| pager.with_header(|header| header.database_size))
            .unwrap()
            .get(),
        initial_db_pages + 2
    ); // (1+1) -> (header + ptrmap)

    //  Read the entry from the ptrmap page and verify it
    let entry = pager
        .io
        .block(|| pager.ptrmap_get(db_page_to_update))
        .unwrap()
        .unwrap();
    assert_eq!(entry.entry_type, PtrmapType::RootPage);
    assert_eq!(entry.parent_page_no, 0);
}

#[test]
fn test_is_ptrmap_page_logic() {
    let page_size = PageSize::MIN as usize;
    let n_data_pages = entries_per_ptrmap_page(page_size);
    assert_eq!(n_data_pages, 102); //   512/5 = 102

    assert!(!is_ptrmap_page(1, page_size)); // Header
    assert!(is_ptrmap_page(2, page_size)); // P0
    assert!(!is_ptrmap_page(3, page_size)); // D0_1
    assert!(!is_ptrmap_page(4, page_size)); // D0_2
    assert!(!is_ptrmap_page(5, page_size)); // D0_3
    assert!(is_ptrmap_page(105, page_size)); // P1
    assert!(!is_ptrmap_page(106, page_size)); // D1_1
    assert!(!is_ptrmap_page(107, page_size)); // D1_2
    assert!(!is_ptrmap_page(108, page_size)); // D1_3
    assert!(is_ptrmap_page(208, page_size)); // P2
}

#[test]
fn test_get_ptrmap_page_no() {
    let page_size = PageSize::MIN as usize; // Maps 103 data pages

    // Test pages mapped by P0 (page 2)
    assert_eq!(get_ptrmap_page_no_for_db_page(3, page_size), 2); // D(3) -> P0(2)
    assert_eq!(get_ptrmap_page_no_for_db_page(4, page_size), 2); // D(4) -> P0(2)
    assert_eq!(get_ptrmap_page_no_for_db_page(5, page_size), 2); // D(5) -> P0(2)
    assert_eq!(get_ptrmap_page_no_for_db_page(104, page_size), 2); // D(104) -> P0(2)

    assert_eq!(get_ptrmap_page_no_for_db_page(105, page_size), 105); // Page 105 is a pointer map page.

    // Test pages mapped by P1 (page 6)
    assert_eq!(get_ptrmap_page_no_for_db_page(106, page_size), 105); // D(106) -> P1(105)
    assert_eq!(get_ptrmap_page_no_for_db_page(107, page_size), 105); // D(107) -> P1(105)
    assert_eq!(get_ptrmap_page_no_for_db_page(108, page_size), 105); // D(108) -> P1(105)

    assert_eq!(get_ptrmap_page_no_for_db_page(208, page_size), 208); // Page 208 is a pointer map page.
}

#[test]
fn test_get_ptrmap_offset() {
    let page_size = PageSize::MIN as usize; //  Maps 103 data pages

    assert_eq!(get_ptrmap_offset_in_page(3, 2, page_size).unwrap(), 0);
    assert_eq!(
        get_ptrmap_offset_in_page(4, 2, page_size).unwrap(),
        PTRMAP_ENTRY_SIZE
    );
    assert_eq!(
        get_ptrmap_offset_in_page(5, 2, page_size).unwrap(),
        2 * PTRMAP_ENTRY_SIZE
    );

    //  P1 (page 105) maps D(106)...D(207)
    // D(106) is index 0 on P1. Offset 0.
    // D(107) is index 1 on P1. Offset 5.
    // D(108) is index 2 on P1. Offset 10.
    assert_eq!(get_ptrmap_offset_in_page(106, 105, page_size).unwrap(), 0);
    assert_eq!(
        get_ptrmap_offset_in_page(107, 105, page_size).unwrap(),
        PTRMAP_ENTRY_SIZE
    );
    assert_eq!(
        get_ptrmap_offset_in_page(108, 105, page_size).unwrap(),
        2 * PTRMAP_ENTRY_SIZE
    );
}

/// Cache-hit fast path: `read_page_nonblock` must return `Done` with no
/// disk-read completion and must not touch `pending_reads`.
#[test]
fn read_page_nonblock_cache_hit_returns_done() {
    let pager = test_pager_setup(4096, 10);

    // Page 1 is unconditionally loaded into cache by `allocate_page1`.
    let res = pager.read_page(1).unwrap();
    match res {
        IOResult::Done((page, c)) => {
            assert_eq!(page.get().id(), 1);
            assert!(
                c.is_none(),
                "cache hit must not return a disk-read completion"
            );
        }
        IOResult::IO(_) => panic!("cache hit should not yield"),
    }
    assert!(
        pager.pending_reads.read().is_empty(),
        "pending_reads must stay empty on cache-hit path"
    );
}

/// Re-entry contract: if `pending_reads` already has a `PendingRead` for
/// this page (as happens after a previous call yielded for spill), the
/// next call must reuse that `(page, disk_read)` instead of allocating a
/// new page and issuing a duplicate disk read.
///
/// This test does NOT force a real spill yield — that requires an IO
/// backend that returns non-finished completions, which we don't have at
/// the core unit-test layer. We instead synthesize the post-yield state
/// directly and assert the function honors it.
#[test]
fn read_page_nonblock_reentry_reuses_pending_entry() {
    let pager = test_pager_setup(4096, 10);

    // Pick a page id well beyond the initialized DB so it is *not* in
    // the cache. We never actually issue IO against it (we short-circuit
    // via the pre-populated `pending_reads` entry), so the page id only
    // needs to be unique within the cache.
    let target_idx: i64 = 9999;
    assert!(
        pager.cache_get(target_idx as usize).unwrap().is_none(),
        "test precondition: target page must not be in cache"
    );

    // Synthesize the state that would exist after a previous call had to
    // yield on spill: a `PendingRead` entry whose `page` is the
    // PageRef we already handed back to the caller, and whose
    // `disk_read` is the in-flight disk-read completion.
    let synthetic_page: PageRef = Arc::new(Page::new(target_idx));
    // Mark loaded so cache eviction logic treats it as a normal page; the
    // contents don't matter for this test.
    synthetic_page.set_loaded();
    let stub_disk_read = Completion::new_yield();
    pager.insert_pending_read(
        target_idx,
        PendingRead {
            page: synthetic_page.clone(),
            disk_read: Some(stub_disk_read),
        },
    );

    let res = pager.read_page(target_idx).unwrap();
    let (page, c) = match res {
        IOResult::Done(v) => v,
        IOResult::IO(_) => panic!(
            "with pending entry present and cache space available, \
                 read_page_nonblock should complete without yielding"
        ),
    };

    assert!(
        Arc::ptr_eq(&page, &synthetic_page),
        "read_page_nonblock must reuse the PageRef from pending_reads, \
             not allocate a new page (this is the no-duplicate-IO invariant)"
    );
    assert!(
        c.is_some(),
        "the disk-read completion from pending_reads should be returned"
    );
    assert!(
        pager.pending_reads.read().get(&target_idx).is_none(),
        "pending_reads entry must be cleared once read_page_nonblock returns Done"
    );
}

/// Concurrency contract: a page can be cache-resident while its disk read
/// is still in flight (locked, not loaded) — `read_page` inserts into the
/// shared cache before the read completes, and `PageCache::get` hands out
/// such in-flight pages. A second reader hitting the cache-hit fast path
/// must NOT receive that unloaded page with `None` (no completion to wait
/// on); it must yield and re-enter until the read completes. Otherwise the
/// caller reads a torn / uninitialized buffer, or races a writer filling
/// the buffer underneath it.
#[test]
fn read_page_nonblock_inflight_cache_hit_yields_not_done() {
    let pager = test_pager_setup(4096, 10);

    let target_idx: i64 = 9999;
    assert!(
        pager.cache_get(target_idx as usize).unwrap().is_none(),
        "test precondition: target page must not be in cache"
    );

    // Synthesize an in-flight read that has already been published to the
    // shared cache: locked (a read is outstanding) but not loaded (the
    // buffer hasn't been filled yet). This is exactly the state a page is
    // in between `cache_insert` and the disk-read completion firing.
    let inflight: PageRef = Arc::new(Page::new(target_idx));
    inflight.set_locked();
    assert!(!inflight.is_loaded());
    pager
        .page_cache
        .write()
        .insert(PageCacheKey::new(target_idx as usize), inflight.clone())
        .unwrap();

    // The fast path finds the page in cache but must refuse to return it
    // without a completion, because it is not yet loaded.
    match pager.read_page(target_idx).unwrap() {
        IOResult::IO(_) => {}
        IOResult::Done((page, c)) => panic!(
            "read_page handed out an in-flight (locked, unloaded) page on the \
                 cache-hit fast path: loaded={}, completion={}",
            page.is_loaded(),
            c.is_some()
        ),
    }

    // Once the read completes (page becomes loaded), the same cache-hit
    // fast path returns Done with no completion, as before.
    inflight.set_loaded();
    match pager.read_page(target_idx).unwrap() {
        IOResult::Done((page, c)) => {
            assert!(Arc::ptr_eq(&page, &inflight));
            assert!(c.is_none(), "loaded cache hit must not return a completion");
        }
        IOResult::IO(_) => panic!("loaded cache hit must not yield"),
    }
}
