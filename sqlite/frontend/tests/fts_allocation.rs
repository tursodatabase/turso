#![cfg(all(nightly, feature = "fts"))]
#![feature(allocator_api)]

use std::ptr::NonNull;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;
use turso_core::alloc::{AllocError, ApiAllocator, DynAllocator, Global, Layout};
use turso_core::index_method::fts::set_fts_retained_cache_bytes_for_test;
use turso_core::{
    ConnectOpts, Connection, Database, DatabaseAllocators, DatabaseOpts, LimboError, MemoryIO,
    OpenOptions, SessionRole, Value,
};
use turso_sqlite::SqliteDialect;

#[test]
fn database_fts_build_and_merge_use_only_the_fts_allocator() {
    // The FTS cache keeps segment data between statements; with no cache,
    // every statement must free all that it allocated.
    set_fts_retained_cache_bytes_for_test(Some(0));
    let allocations = Arc::new(AtomicUsize::new(0));
    let deallocations = Arc::new(AtomicUsize::new(0));
    let mv_allocations = Arc::new(AtomicUsize::new(0));
    let db = Database::open(
        Arc::new(MemoryIO::new()),
        ":memory:",
        OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(DatabaseOpts::default().with_index_method(true))
            .allocators(DatabaseAllocators {
                mv_store: DynAllocator::new(CountingAlloc {
                    allocations: mv_allocations.clone(),
                    deallocations: Arc::new(AtomicUsize::new(0)),
                }),
                fts: DynAllocator::new(CountingAlloc {
                    allocations: allocations.clone(),
                    deallocations: deallocations.clone(),
                }),
            }),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for sql in [
        "INSERT INTO docs VALUES (7, 'hello turso')",
        "INSERT INTO docs VALUES (19, 'hello world')",
        "OPTIMIZE INDEX docs_fts",
    ] {
        allocations.store(0, Ordering::Relaxed);
        deallocations.store(0, Ordering::Relaxed);
        conn.execute(sql).unwrap();
        assert!(allocations.load(Ordering::Relaxed) > 0, "{sql}");
        assert_eq!(
            allocations.load(Ordering::Relaxed),
            deallocations.load(Ordering::Relaxed),
            "{sql}"
        );
        assert_eq!(mv_allocations.load(Ordering::Relaxed), 0, "{sql}");
    }
    assert_fts_rows(&conn, &[7, 19]);
    set_fts_retained_cache_bytes_for_test(None);
}

#[test]
fn failed_fts_allocations_roll_back_statements_and_allow_retry() {
    for merge in [false, true] {
        for mvcc in [false, true] {
            let (conn, allocator) = database_with_failing_fts_allocator(mvcc);
            if merge {
                conn.execute("INSERT INTO docs VALUES (19, 'hello world')")
                    .unwrap();
            }
            let sql = if merge {
                "OPTIMIZE INDEX docs_fts"
            } else {
                "INSERT INTO docs VALUES (19, 'hello world')"
            };
            allocator.allocations.store(0, Ordering::Relaxed);
            conn.execute(sql).unwrap();
            let allocation_count = allocator.allocations.load(Ordering::Relaxed);
            assert!(allocation_count > 0);

            for fail_at in 0..allocation_count {
                let (conn, allocator) = database_with_failing_fts_allocator(mvcc);
                if merge {
                    conn.execute("INSERT INTO docs VALUES (19, 'hello world')")
                        .unwrap();
                }
                allocator.fail_after(fail_at);
                let result = conn.execute(sql);
                assert_eq!(
                    allocator.remaining.load(Ordering::Relaxed),
                    -1,
                    "{sql}, mvcc={mvcc}, fail_at={fail_at}"
                );
                assert!(
                    matches!(result, Err(LimboError::OutOfMemory)),
                    "{sql}, mvcc={mvcc}, fail_at={fail_at}: {result:?}"
                );

                let expected = if merge { vec![7, 19] } else { vec![7] };
                assert_fts_rows(&conn, &expected);
                conn.execute(sql).unwrap();
                assert_fts_rows(&conn, &[7, 19]);
            }
        }
    }
}

fn database_with_failing_fts_allocator(mvcc: bool) -> (Arc<Connection>, FailingAllocator) {
    let allocator = FailingAllocator::default();
    let db = Database::open(
        Arc::new(MemoryIO::new()),
        ":memory:",
        OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(DatabaseOpts::default().with_index_method(true))
            .allocators(DatabaseAllocators {
                fts: DynAllocator::new(allocator.clone()),
                ..Default::default()
            }),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    if mvcc {
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    }
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (7, 'hello turso')")
        .unwrap();
    (conn, allocator)
}

fn assert_fts_rows(conn: &Arc<Connection>, ids: &[i64]) {
    let expected: Vec<Vec<Value>> = ids.iter().map(|id| vec![Value::from_i64(*id)]).collect();
    for sql in [
        "SELECT id FROM docs ORDER BY id",
        "SELECT id FROM docs WHERE fts_match(body, 'hello') ORDER BY id",
    ] {
        let rows = conn.prepare(sql).unwrap().run_collect_rows().unwrap();
        assert_eq!(rows, expected, "{sql}");
    }
}

/// Counts the allocations and deallocations it passes on to the global
/// allocator.
struct CountingAlloc {
    allocations: Arc<AtomicUsize>,
    deallocations: Arc<AtomicUsize>,
}

unsafe impl ApiAllocator for CountingAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocations.fetch_add(1, Ordering::Relaxed);
        Global.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        self.deallocations.fetch_add(1, Ordering::Relaxed);
        unsafe { Global.deallocate(ptr, layout) }
    }
}

/// Counts allocations, and fails the one after `fail_after(n)` let `n`
/// more succeed.
#[derive(Clone)]
struct FailingAllocator {
    remaining: Arc<AtomicIsize>,
    allocations: Arc<AtomicUsize>,
}

impl Default for FailingAllocator {
    fn default() -> Self {
        Self {
            remaining: Arc::new(AtomicIsize::new(-1)),
            allocations: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl FailingAllocator {
    fn fail_after(&self, allocations: usize) {
        self.remaining
            .store(allocations.try_into().unwrap(), Ordering::Relaxed);
    }
}

unsafe impl ApiAllocator for FailingAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocations.fetch_add(1, Ordering::Relaxed);
        let previous =
            self.remaining
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                    (remaining >= 0).then(|| remaining - 1)
                });
        if previous == Ok(0) {
            return Err(AllocError);
        }
        Global.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe { Global.deallocate(ptr, layout) }
    }
}
