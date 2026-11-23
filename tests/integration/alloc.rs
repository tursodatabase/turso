use crate::common::limbo_exec_rows;
use rusqlite::types::Value;
use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc as StdArc;
use turso_core::alloc::{AllocError, ApiAllocator, DynAllocator, Global, Layout};
use turso_core::security::roles::SessionRole;
use turso_core::DatabaseAllocators;
use turso_core::{ConnectOpts, Connection, Database, LimboError};
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
                statement: DynAllocator::default(),
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

#[test]
fn statement_allocator_gives_the_memory_of_nested_programs() {
    let allocations = StdArc::new(AtomicUsize::new(0));
    let deallocations = StdArc::new(AtomicUsize::new(0));
    let (_db, conn) = open_with_statement_allocator(DynAllocator::new(CountingAlloc {
        allocations: allocations.clone(),
        deallocations: deallocations.clone(),
    }));
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("CREATE TABLE u(y)").unwrap();
    conn.execute("CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO u VALUES (new.x); END")
        .unwrap();

    // A statement that starts no trigger uses no memory of this allocator.
    conn.execute("INSERT INTO u VALUES (1)").unwrap();
    assert_eq!(allocations.load(Relaxed), 0);

    // The compilation of the trigger program uses this allocator, and it gives
    // back all the memory.
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    assert!(allocations.load(Relaxed) > 0);
    assert_eq!(deallocations.load(Relaxed), allocations.load(Relaxed));
}

#[test]
fn statement_allocator_failure_stops_only_the_statement() {
    let fail = StdArc::new(AtomicBool::new(false));
    let (_db, conn) =
        open_with_statement_allocator(DynAllocator::new(FailOnDemandAlloc(fail.clone())));
    for sql in [
        "CREATE TABLE t(x)",
        "CREATE TABLE u(y)",
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO u VALUES (new.x); END",
    ] {
        conn.execute(sql).unwrap();
    }

    // The compilation of the trigger program fails.
    fail.store(true, Relaxed);
    let Err(err) = conn.prepare("INSERT INTO t VALUES (1)") else {
        panic!("the compilation of the trigger program must fail");
    };
    assert!(matches!(err, LimboError::OutOfMemory), "{err}");

    // The failed statement changed nothing, and the connection continues to
    // work.
    fail.store(false, Relaxed);
    conn.execute("INSERT INTO t VALUES (3)").unwrap();
    for (sql, expected) in [("SELECT x FROM t", 3), ("SELECT y FROM u", 3)] {
        assert_eq!(
            limbo_exec_rows(&conn, sql),
            vec![vec![Value::Integer(expected)]],
            "{sql}"
        );
    }
}

/// Opens an in-memory database whose statements get their memory from
/// `statement`.
fn open_with_statement_allocator(
    statement: DynAllocator,
) -> (StdArc<Database>, StdArc<Connection>) {
    let db = Database::open(
        StdArc::new(turso_core::MemoryIO::new()),
        ":memory:",
        turso_core::OpenOptions::new(StdArc::new(SqliteDialect)).allocators(DatabaseAllocators {
            statement,
            ..Default::default()
        }),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    (db, conn)
}

/// An allocator that gives no memory while its flag is set.
#[derive(Clone)]
struct FailOnDemandAlloc(StdArc<AtomicBool>);

unsafe impl ApiAllocator for FailOnDemandAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if self.0.load(Relaxed) {
            return Err(AllocError);
        }
        <Global as ApiAllocator>::allocate(&Global, layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe {
            <Global as ApiAllocator>::deallocate(&Global, ptr, layout);
        }
    }
}
