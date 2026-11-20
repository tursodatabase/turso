#![cfg(nightly)]
#![feature(allocator_api)]

use std::{cell::Cell, ptr::NonNull, sync::Arc};
use turso_core::{
    alloc::{
        current_allocation_site, set_allocator, AllocError, AllocationSite, ApiAllocator,
        BTreeAllocationSite, Global, Layout, TursoAllocBackend,
    },
    SessionRole,
};
use turso_core::{ConnectOpts, Connection, Database, MemoryIO, StepResult, IO};
use turso_sqlite::SqliteDialect;

/// Deterministic coverage for the allocation-failure window created by routing
/// overflow-cell payloads through `TursoAllocator`: in
/// `OverwriteCellState::ClearOverflowPagesAndOverwrite`, `drop_cell` mutates the
/// page before `insert_into_cell`, whose overflow-cell branch can fail on
/// allocation. A fault there must not lose the row or corrupt the tree.
#[test]
fn overwrite_survives_overflow_cell_allocation_failure() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, "overflow-cell-fault.db", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    // Row 1 is the overwrite target; the filler rows pack the leaf page so
    // that growing row 1 cannot fit locally and must spill to an overflow
    // cell inside insert_into_cell.
    conn.execute(format!("INSERT INTO t VALUES (1, '{}')", "a".repeat(600)))
        .unwrap();
    for id in 2..=6 {
        conn.execute(format!(
            "INSERT INTO t VALUES ({id}, '{}')",
            "b".repeat(600)
        ))
        .unwrap();
    }
    let rows_before = query_single_i64(&conn, "SELECT count(*) FROM t");
    assert_eq!(rows_before, 6);

    arm_fault(BTreeAllocationSite::OverflowCell);
    let update = conn.execute(format!(
        "UPDATE t SET v = '{}' WHERE id = 1",
        "c".repeat(1400)
    ));
    disarm_fault();
    assert!(
        update.is_err(),
        "update should fail once the overflow-cell allocation is denied"
    );

    // The failed statement must not lose the row or corrupt the tree.
    assert_eq!(query_single_i64(&conn, "SELECT count(*) FROM t"), 6);
    assert_eq!(
        query_single_i64(&conn, "SELECT length(v) FROM t WHERE id = 1"),
        600
    );
    assert_eq!(query_single_text(&conn, "PRAGMA integrity_check"), "ok");
}

fn arm_fault(site: BTreeAllocationSite) {
    unsafe { set_allocator(&BACKEND).unwrap() };
    FAIL_SITE.set(Some(AllocationSite::BTree(site)));
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

fn query_single_text(conn: &Arc<Connection>, sql: &str) -> String {
    let mut stmt = conn.prepare(sql).unwrap();
    match stmt.step().unwrap() {
        StepResult::Row => stmt.row().unwrap().get::<String>(0).unwrap(),
        other => panic!("expected a row, got {other:?}"),
    }
}
