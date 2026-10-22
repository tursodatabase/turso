#![cfg(all(nightly, feature = "allocation_metric", feature = "fs"))]
#![feature(allocator_api)]

use std::{cell::Cell, ptr::NonNull, sync::Arc};
use turso_core::alloc::{
    current_allocation_site, set_allocator, AllocError, AllocationSite, ApiAllocator, Global,
    Layout, SchemaAllocationSite, TursoAllocBackend,
};
use turso_core::{
    Database, DatabaseOpts, LimboError, OpenOptions, PlatformIO, SqliteDialect, Value,
};

thread_local! {
    static FAIL_VIEW_COLUMNS: Cell<bool> = const { Cell::new(false) };
}

struct ViewColumnFaultBackend;

unsafe impl TursoAllocBackend for ViewColumnFaultBackend {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if FAIL_VIEW_COLUMNS.get()
            && current_allocation_site()
                == Some(AllocationSite::Schema(
                    SchemaAllocationSite::FlatViewColumns,
                ))
        {
            return Err(AllocError);
        }
        Global.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe { Global.deallocate(ptr, layout) }
    }
}

static BACKEND: ViewColumnFaultBackend = ViewColumnFaultBackend;

#[test]
fn view_column_allocation_failure_returns_out_of_memory() {
    unsafe { set_allocator(&BACKEND).unwrap() };

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("views.db");
    {
        let sqlite = rusqlite::Connection::open(&path).unwrap();
        sqlite
            .execute_batch(
                "CREATE TABLE t(a INTEGER, b INTEGER);
                 INSERT INTO t VALUES (7, 14);
                 CREATE VIEW v(second, first) AS SELECT b, a FROM t;",
            )
            .unwrap();
    }

    let io = Arc::new(PlatformIO::new().unwrap());
    let options =
        OpenOptions::new(Arc::new(SqliteDialect)).db_opts(DatabaseOpts::new().with_views(true));
    FAIL_VIEW_COLUMNS.set(true);
    let result = Database::open(io.clone(), path.to_str().unwrap(), options.clone());
    FAIL_VIEW_COLUMNS.set(false);
    assert!(matches!(result, Err(LimboError::OutOfMemory)));

    let db = Database::open(io.clone(), path.to_str().unwrap(), options.clone()).unwrap();
    let conn = db.connect().unwrap();
    let mut stmt = conn.prepare("SELECT * FROM v").unwrap();
    assert_eq!(stmt.get_column_name(0), "second");
    assert_eq!(stmt.get_column_name(1), "first");
    assert_eq!(
        stmt.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(14), Value::from_i64(7)]]
    );

    let create_view = "CREATE MATERIALIZED VIEW mv AS SELECT b, a FROM t";
    FAIL_VIEW_COLUMNS.set(true);
    let result = conn.prepare(create_view);
    FAIL_VIEW_COLUMNS.set(false);
    assert!(matches!(result, Err(LimboError::OutOfMemory)));
    conn.execute(create_view).unwrap();

    for sql in [
        "SELECT * FROM mv",
        "PRAGMA table_info(mv)",
        "PRAGMA table_xinfo(mv)",
    ] {
        FAIL_VIEW_COLUMNS.set(true);
        let mut stmt = conn.prepare(sql).unwrap();
        FAIL_VIEW_COLUMNS.set(false);
        let rows = stmt.run_collect_rows().unwrap();
        if sql.starts_with("SELECT") {
            assert_eq!(rows, vec![vec![Value::from_i64(14), Value::from_i64(7)]]);
        } else {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][1], Value::build_text("b"));
            assert_eq!(rows[1][1], Value::build_text("a"));
        }
    }

    conn.execute("DROP VIEW v").unwrap();
    drop(stmt);
    drop(conn);
    drop(db);
    FAIL_VIEW_COLUMNS.set(true);
    let result = Database::open(io.clone(), path.to_str().unwrap(), options.clone());
    FAIL_VIEW_COLUMNS.set(false);
    assert!(matches!(result, Err(LimboError::OutOfMemory)));

    let db = Database::open(io, path.to_str().unwrap(), options).unwrap();
    let conn = db.connect().unwrap();
    let mut stmt = conn.prepare("SELECT * FROM mv").unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(14), Value::from_i64(7)]]
    );
}
