use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc as StdArc;
use turso_core::alloc::*;
use turso_core::security::roles::SessionRole;
use turso_core::ConnectOpts;
use turso_core::DatabaseAllocators;
use turso_sqlite::SqliteDialect;

#[derive(Clone)]
struct CountingAlloc {
    allocations: StdArc<AtomicUsize>,
    deallocations: StdArc<AtomicUsize>,
}

unsafe impl ApiAllocator for CountingAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.allocations.fetch_add(1, Ordering::Relaxed);
        <Global as ApiAllocator>::allocate(&Global, layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        self.deallocations.fetch_add(1, Ordering::Relaxed);
        unsafe {
            <Global as ApiAllocator>::deallocate(&Global, ptr, layout);
        }
    }
}

#[test]
fn database_open_with_allocator_uses_allocator_for_mvstore_skiplist() {
    let allocations = StdArc::new(AtomicUsize::new(0));
    let deallocations = StdArc::new(AtomicUsize::new(0));
    let fts_allocations = StdArc::new(AtomicUsize::new(0));
    let alloc = DynAllocator::new(CountingAlloc {
        allocations: allocations.clone(),
        deallocations,
    });
    let io = StdArc::new(turso_core::MemoryIO::new());
    let file = turso_core::IO::open_file(
        io.as_ref(),
        "open-with-allocator.db",
        turso_core::OpenFlags::Create,
        true,
    )
    .unwrap();
    let db_file = StdArc::new(turso_core::storage::database::DatabaseFile::new(file));
    let db = turso_core::Database::open(
        io,
        "open-with-allocator.db",
        turso_core::OpenOptions::new(StdArc::new(SqliteDialect))
            .storage(db_file)
            .allocators(DatabaseAllocators {
                mv_store: alloc,
                fts: DynAllocator::new(CountingAlloc {
                    allocations: fts_allocations.clone(),
                    deallocations: StdArc::new(AtomicUsize::new(0)),
                }),
            }),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    allocations.store(0, Ordering::Relaxed);
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();

    assert!(db.get_mv_store().is_some());
    assert!(allocations.load(Ordering::Relaxed) > 0);
    assert_eq!(fts_allocations.load(Ordering::Relaxed), 0);
}
