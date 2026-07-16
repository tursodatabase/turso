//! Leak detection tests: after all user handles to a database are dropped,
//! no component (registry, MVCC store, pager, ...) may keep the
//! `Database` or `MvStore` alive, and repeated open/workload/drop cycles
//! must not accumulate per-database state in process-global structures.

use std::sync::{Arc, Weak};
use turso_core::{Database, OpenFlags, StepResult};

/// Run a small MVCC workload on a fresh database at `path`, then drop all
/// handles and return weak references to the `Database` and its `MvStore`.
fn open_run_drop_mvcc(
    io: &Arc<dyn turso_core::IO + Send>,
    path: &std::path::Path,
) -> (Weak<Database>, Weak<impl Sized>) {
    let db = Database::open_file_with_flags(
        io.clone(),
        path.to_str().unwrap(),
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
    )
    .unwrap();

    let conn = db.connect().unwrap();
    conn.pragma_update("journal_mode", "'mvcc'").unwrap();

    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT)")
        .unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    for i in 0..100 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'payload-{i}')"))
            .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("UPDATE t SET b = b || '-updated' WHERE a % 2 = 0")
        .unwrap();

    // Drain a SELECT so cursors/statements get exercised.
    let mut stmt = conn.prepare("SELECT count(*) FROM t").unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {}
            StepResult::IO => stmt.get_pager().io.step().unwrap(),
            _ => break,
        }
    }
    drop(stmt);

    let mv_store = db
        .get_mv_store()
        .as_ref()
        .cloned()
        .expect("MVCC store must exist after journal_mode=mvcc");
    let weak_mv = Arc::downgrade(&mv_store);
    drop(mv_store);
    let weak_db = Arc::downgrade(&db);

    conn.close().unwrap();
    drop(conn);
    drop(db);

    (weak_db, weak_mv)
}

/// After dropping every handle to an MVCC database, both the `Database`
/// and the `MvStore` must be deallocated. If this fails, something
/// process-global (e.g. the database registry) or a reference cycle is
/// keeping the database alive — a memory leak that grows with every
/// database opened in the process.
#[test]
fn test_mvcc_database_dropped_releases_memory() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());

    for cycle in 0..5 {
        let path = tmp_dir.path().join(format!("leak-{cycle}.db"));
        let (weak_db, weak_mv) = open_run_drop_mvcc(&io, &path);
        assert!(
            weak_db.upgrade().is_none(),
            "cycle {cycle}: Database still alive after dropping all handles \
             (strong count {})",
            weak_db.strong_count()
        );
        assert!(
            weak_mv.upgrade().is_none(),
            "cycle {cycle}: MvStore still alive after dropping all handles \
             (strong count {})",
            weak_mv.strong_count()
        );
        // The database files are deleted between cycles, like a server
        // dropping a database.
        for ext in ["", "-wal", "-log", "-shm"] {
            let f = tmp_dir.path().join(format!("leak-{cycle}.db{ext}"));
            if f.exists() {
                std::fs::remove_file(f).unwrap();
            }
        }
    }
}
