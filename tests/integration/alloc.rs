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

    // Each statement makes the builder of its program with this allocator.
    let before = allocations.load(Relaxed);
    conn.execute("INSERT INTO u VALUES (1)").unwrap();
    let without_trigger = allocations.load(Relaxed) - before;
    assert!(without_trigger > 0);

    // A statement that starts a trigger also makes the builder of the trigger
    // program and the statement that executes it with this allocator.
    let before = allocations.load(Relaxed);
    conn.execute("INSERT INTO t VALUES (2)").unwrap();
    assert!(allocations.load(Relaxed) - before > without_trigger);

    // The statements give back all the memory when they end.
    assert_eq!(deallocations.load(Relaxed), allocations.load(Relaxed));
}

#[test]
fn statement_allocator_failure_stops_only_the_statement() {
    let fail = StdArc::new(AtomicBool::new(false));
    let (_db, conn) =
        open_with_statement_allocator(DynAllocator::new(FailOnDemandAlloc(fail.clone())));
    for sql in [
        "PRAGMA foreign_keys = ON",
        "CREATE TABLE t(x)",
        "CREATE TABLE u(y)",
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO u VALUES (new.x); END",
        "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
        "CREATE TABLE child(pid REFERENCES parent(id) ON DELETE CASCADE)",
        "INSERT INTO parent VALUES (1)",
        "INSERT INTO child VALUES (1)",
    ] {
        conn.execute(sql).unwrap();
    }

    // The compilation of the statement fails.
    fail.store(true, Relaxed);
    let Err(err) = conn.prepare("INSERT INTO t VALUES (1)") else {
        panic!("the compilation of the statement must fail");
    };
    assert!(matches!(err, LimboError::OutOfMemory), "{err}");

    // The trigger program compiles, but the statement that executes it fails.
    fail.store(false, Relaxed);
    let mut stmt = conn.prepare("INSERT INTO t VALUES (2)").unwrap();
    fail.store(true, Relaxed);
    let err = stmt.run_ignore_rows().unwrap_err();
    assert!(matches!(err, LimboError::OutOfMemory), "{err}");
    drop(stmt);

    // The statement that executes a foreign key action program fails too.
    fail.store(false, Relaxed);
    let mut stmt = conn.prepare("DELETE FROM parent").unwrap();
    fail.store(true, Relaxed);
    let err = stmt.run_ignore_rows().unwrap_err();
    assert!(matches!(err, LimboError::OutOfMemory), "{err}");
    drop(stmt);

    // The failed statements changed nothing, and the connection continues to
    // work.
    fail.store(false, Relaxed);
    conn.execute("INSERT INTO t VALUES (3)").unwrap();
    for (sql, expected) in [
        ("SELECT x FROM t", 3),
        ("SELECT y FROM u", 3),
        ("SELECT id FROM parent", 1),
        ("SELECT pid FROM child", 1),
    ] {
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
