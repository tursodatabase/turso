#![cfg_attr(nightly, feature(allocator_api))]

use std::{cell::Cell, ptr::NonNull, sync::Arc};
use turso_core::{
    alloc::{
        current_allocation_site, set_allocator, AllocError, AllocationSite, ApiAllocator, Global,
        Layout, StatementAllocationSite, TursoAllocBackend,
    },
    SessionRole,
};
use turso_core::{
    ConnectOpts, Connection, Database, DatabaseOpts, LimboError, MemoryIO, OpenFlags, StepResult,
    IO,
};
use turso_sqlite::SqliteDialect;

/// The statement allocator of a database uses `TursoAllocator` by default, so
/// a fault backend gets its allocations, also in stable builds, where `DynBox`
/// uses its allocator. A fault at each statement allocation site must stop
/// only the statement, roll it back, and leave the connection usable.
#[test]
fn statement_allocation_sites_fail_only_the_statement() {
    unsafe { set_allocator(&BACKEND).unwrap() };
    #[allow(clippy::arc_with_non_send_sync)]
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        "statement-site-fault.db",
        OpenFlags::default(),
        DatabaseOpts::new().with_views(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    for sql in [
        "PRAGMA foreign_keys = ON",
        "CREATE TABLE t(x)",
        "CREATE TABLE u(y)",
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO u VALUES (new.x); END",
        "CREATE TABLE parent(id INTEGER PRIMARY KEY)",
        "CREATE TABLE child(pid REFERENCES parent(id) ON DELETE CASCADE)",
        "INSERT INTO parent VALUES (1)",
        "INSERT INTO child VALUES (1)",
        "CREATE TABLE b(data BLOB)",
        "INSERT INTO b VALUES (zeroblob(4))",
        "CREATE TABLE w(z)",
        "CREATE MATERIALIZED VIEW mv AS SELECT z FROM w",
    ] {
        conn.execute(sql).unwrap();
    }

    // The compilation of a statement fails.
    arm_fault(StatementAllocationSite::ProgramBuilder);
    let Err(err) = conn.prepare("SELECT count(*) FROM t") else {
        panic!("the compilation of the statement must fail");
    };
    assert!(matches!(err, LimboError::OutOfMemory), "{err}");
    disarm_fault();

    // The opening of a blob handle fails when it makes the builder of its
    // program.
    arm_fault(StatementAllocationSite::ProgramBuilder);
    let Err(err) = conn.blob_open("b", "data", 1, false) else {
        panic!("the opening of the blob handle must fail");
    };
    assert!(matches!(err, LimboError::OutOfMemory), "{err}");
    disarm_fault();

    // The maintenance of a materialized view fails when it makes a builder, so
    // the statement that changes the table of the view fails.
    let mut stmt = conn.prepare("INSERT INTO w VALUES (1)").unwrap();
    arm_fault(StatementAllocationSite::ProgramBuilder);
    let err = stmt.run_ignore_rows().unwrap_err();
    assert!(matches!(err, LimboError::OutOfMemory), "{err}");
    disarm_fault();
    drop(stmt);

    // The preparation of a statement fails when it makes the state of the
    // statement.
    arm_fault(StatementAllocationSite::ProgramState);
    let Err(err) = conn.prepare("SELECT count(*) FROM t") else {
        panic!("the preparation of the statement must fail");
    };
    assert!(matches!(err, LimboError::OutOfMemory), "{err}");
    disarm_fault();

    // The compilation of the trigger program and of the foreign key action
    // program fails.
    for sql in ["INSERT INTO t VALUES (1)", "DELETE FROM parent"] {
        arm_fault(StatementAllocationSite::TriggerProgramBuilder);
        let Err(err) = conn.prepare(sql) else {
            panic!("{sql}: the compilation of the nested program must fail");
        };
        assert!(matches!(err, LimboError::OutOfMemory), "{sql}: {err}");
        disarm_fault();
    }

    // The statements that execute a trigger program and a foreign key action
    // program fail.
    for sql in ["INSERT INTO t VALUES (2)", "DELETE FROM parent"] {
        let mut stmt = conn.prepare(sql).unwrap();
        arm_fault(StatementAllocationSite::SubprogramStatement);
        let err = stmt.run_ignore_rows().unwrap_err();
        assert!(matches!(err, LimboError::OutOfMemory), "{sql}: {err}");
        disarm_fault();
    }

    // The failed statements changed nothing, and the connection continues to
    // work.
    conn.execute("INSERT INTO t VALUES (3)").unwrap();
    conn.execute("INSERT INTO w VALUES (2)").unwrap();
    for (sql, expected) in [
        ("SELECT count(*) FROM t", 1),
        ("SELECT count(*) FROM u", 1),
        ("SELECT count(*) FROM parent", 1),
        ("SELECT count(*) FROM child", 1),
        ("SELECT count(*) FROM w", 1),
        ("SELECT count(*) FROM mv", 1),
    ] {
        assert_eq!(query_single_i64(&conn, sql), expected, "{sql}");
    }
    drop(conn.blob_open("b", "data", 1, false).unwrap());
}

fn arm_fault(site: StatementAllocationSite) {
    FAIL_SITE.set(Some(AllocationSite::Statement(site)));
}

fn disarm_fault() {
    FAIL_SITE.set(None);
}

static BACKEND: SiteFaultBackend = SiteFaultBackend;

struct SiteFaultBackend;

unsafe impl TursoAllocBackend for SiteFaultBackend {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let target = FAIL_SITE.get();
        if target.is_some() && target == current_allocation_site() {
            return Err(AllocError);
        }
        Global.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe { Global.deallocate(ptr, layout) }
    }
}

thread_local! {
    static FAIL_SITE: Cell<Option<AllocationSite>> = const { Cell::new(None) };
}

fn query_single_i64(conn: &Arc<Connection>, sql: &str) -> i64 {
    let mut stmt = conn.prepare(sql).unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => stmt.row().unwrap().get::<i64>(0).unwrap(),
        other => panic!("expected a row, got {other:?}"),
    }
}
