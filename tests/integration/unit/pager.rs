use turso_core::security::roles::SessionRole;
#[cfg(feature = "autovacuum")]
#[allow(unused_imports)]
use turso_core::storage::pager::ptrmap::*;
use turso_core::sync::Arc;
#[allow(unused_imports)]
use turso_core::util::IOExt as _;
use turso_sqlite::SqliteDialect;

use turso_core::sync::RwLock;

use turso_core::io::{MemoryIO, OpenFlags, IO};
use turso_core::storage::buffer_pool::BufferPool;
use turso_core::storage::database::DatabaseFile;
use turso_core::storage::page_cache::{PageCache, PageCacheKey};
use turso_core::storage::wal::{Wal, WalFile, WalFileShared};
use turso_core::util::IOExt;

use turso_core::storage::pager::{
    default_page1, CacheFlushState, CollectingState, Page, PageRef, Pager, SharedPagerState,
};
use turso_core::{Buffer, Completion, CompletionError, LimboError};

#[test]
fn record_pool_frees_buffers_of_rows_larger_than_a_page() {
    let io = Arc::new(MemoryIO::new());
    let db = turso_core::Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT NOT NULL, tag TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST(zeroblob(6 * 1024 * 1024) AS TEXT), 'big')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST(zeroblob(5000) AS TEXT), 'small')")
        .unwrap();
    conn.execute("SELECT body FROM t WHERE id = 1").unwrap();
    conn.execute("UPDATE t SET tag = 'updated' WHERE id = 1")
        .unwrap();
    conn.execute("UPDATE t SET tag = 'updated' WHERE id = 2")
        .unwrap();

    let pager = conn.get_pager();
    let pool = pager.record_pool.lock();
    assert!(!pool.is_empty());
    for buf in pool.iter() {
        assert!(buf.capacity() <= turso_core::storage::pager::PageSize::MAX as usize);
    }
}

#[test]
fn checkpoint_reset_does_not_reuse_failed_header_read() {
    let pager = pager_with_cache_capacity(10, 2);
    let completion =
        Completion::new_read(Arc::new(Buffer::new_temporary(4096)), Box::new(|_| None));
    completion.abort();
    *pager.header_ref_state.write() = turso_core::storage::pager::HeaderRefState::CreateHeader {
        page: default_page1(None),
        completion: Some(completion),
    };
    pager.reset_checkpoint_state();
    pager.io.block(|| pager.with_header(|_| ())).unwrap();
}

#[test]
fn page_id_changes_keep_header_access_at_the_correct_offset() {
    let mut page = turso_core::storage::pager::PageInner::from_buffer(Buffer::new_temporary(4096));
    for id in [1, 2, 1, 0, usize::MAX] {
        page.set_id(id);
        assert_eq!(page.id(), id);
        let offset = if id == 1 { 100 } else { 0 };
        assert_eq!(page.offset(), offset);
        page.as_ptr().fill(0);
        page.write_page_type(turso_core::storage::pager::PageType::TableLeaf as u8);
        assert_eq!(
            page.as_ptr()[offset],
            turso_core::storage::pager::PageType::TableLeaf as u8
        );
        assert_eq!(
            page.page_type().unwrap(),
            turso_core::storage::pager::PageType::TableLeaf
        );

        let unloaded = turso_core::storage::pager::PageInner::unloaded(id);
        assert_eq!(unloaded.id(), id);
        assert_eq!(unloaded.offset(), offset);
    }
}

#[test]
fn unloaded_page_exposes_an_empty_data_slice() {
    let page = turso_core::storage::pager::PageInner::unloaded(1);
    assert!(page.as_ptr().is_empty());
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

    let shared = Arc::new(SharedPagerState::new(Some(default_page1(None))));
    let pager = Arc::new(
        Pager::new(
            db_file,
            Some(wal),
            io,
            PageCache::new(cache_capacity),
            buffer_pool,
            shared,
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

#[test]
fn rollback_keeps_pages_the_transaction_did_not_change() {
    let (db, conn) = open_memory_database("rollback-keeps-unchanged-pages.db");
    conn.execute("CREATE TABLE changed(x)").unwrap();
    conn.execute("CREATE TABLE unchanged(x)").unwrap();
    conn.execute("INSERT INTO changed VALUES (1)").unwrap();
    conn.execute("INSERT INTO unchanged VALUES (1)").unwrap();
    let changed_root = root_page(&conn, "changed");
    let unchanged_root = root_page(&conn, "unchanged");
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM unchanged"), 1);
    let pager = conn.pager.load();
    let cached_unchanged_root = pager.cache_get(unchanged_root).unwrap().unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO changed VALUES (2)").unwrap();
    conn.execute("ROLLBACK").unwrap();

    assert!(!pager
        .page_cache
        .write()
        .contains_key(&PageCacheKey::new(changed_root)));
    let unchanged_root_after_rollback = pager.cache_get(unchanged_root).unwrap().unwrap();
    assert!(Arc::ptr_eq(
        &cached_unchanged_root,
        &unchanged_root_after_rollback
    ));
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM changed"), 1);
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM unchanged"), 1);
    drop(db);
}

#[test]
fn rollback_drops_pages_spilled_to_the_wal() {
    let (db, conn) = open_memory_database("rollback-drops-spilled-pages.db");
    conn.execute("PRAGMA cache_size = 200").unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t SELECT randomblob(1000) FROM generate_series(1, 500)")
        .unwrap();
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM t"), 500);
    let pager = conn.pager.load();
    let wal = pager.wal.as_ref().unwrap();
    let committed_max_frame = wal.get_max_frame();

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET x = randomblob(1000)").unwrap();
    conn.execute("INSERT INTO t SELECT randomblob(1000) FROM generate_series(1, 1000)")
        .unwrap();
    assert!(
        wal.get_max_frame() > committed_max_frame,
        "the transaction must spill pages to the WAL"
    );
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM t"), 1500);
    conn.execute("ROLLBACK").unwrap();

    assert_eq!(wal.get_max_frame(), committed_max_frame);
    let mut cache = pager.page_cache.write();
    for key in cache.keys() {
        let page = cache.peek(&key, false).unwrap();
        assert!(!page.is_dirty());
        assert!(!page.has_wal_tag() || page.wal_tag_pair().0 <= committed_max_frame);
    }
    drop(cache);
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM t"), 500);
    let mut integrity_check = conn.prepare("PRAGMA integrity_check").unwrap();
    assert_eq!(
        integrity_check.run_collect_rows().unwrap(),
        vec![vec![turso_core::Value::build_text("ok")]]
    );
    drop(db);
}

#[test]
fn rollback_drops_pinned_pages_that_are_not_loaded() {
    let (db, conn) = open_memory_database("rollback-drops-pinned-unloaded-pages.db");
    conn.execute("CREATE TABLE changed(x)").unwrap();
    conn.execute("CREATE TABLE unloaded(x)").unwrap();
    conn.execute("INSERT INTO unloaded VALUES (1)").unwrap();
    let unloaded_root = root_page(&conn, "unloaded");
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM unloaded"), 1);
    let pager = conn.pager.load();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO changed VALUES (1)").unwrap();
    let page = pager.cache_get(unloaded_root).unwrap().unwrap();
    page.pin();
    page.clear_loaded();
    conn.execute("ROLLBACK").unwrap();

    assert!(!pager
        .page_cache
        .write()
        .contains_key(&PageCacheKey::new(unloaded_root)));
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM unloaded"), 1);
    drop(db);
}

#[test]
#[should_panic(expected = "rollback must discard pages while holding the WAL write lock")]
fn rollback_without_the_write_lock_panics() {
    let (_db, conn) = open_memory_database("rollback-without-write-lock.db");
    conn.execute("CREATE TABLE t(x)").unwrap();

    conn.pager.load().rollback(false, &conn, true);
}

fn open_memory_database(path: &str) -> (Arc<turso_core::Database>, Arc<turso_core::Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = turso_core::Database::open_file(io, path, Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    (db, conn)
}

fn root_page(conn: &Arc<turso_core::Connection>, table: &str) -> usize {
    query_i64(
        conn,
        &format!("SELECT rootpage FROM sqlite_schema WHERE name = '{table}'"),
    ) as usize
}

fn query_i64(conn: &Arc<turso_core::Connection>, sql: &str) -> i64 {
    let mut stmt = conn.prepare(sql).unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    match rows.as_slice() {
        [row] => match row.as_slice() {
            [turso_core::Value::Numeric(turso_core::Numeric::Integer(value))] => *value,
            other => panic!("expected one integer, got {other:?}"),
        },
        other => panic!("expected one row, got {other:?}"),
    }
}
