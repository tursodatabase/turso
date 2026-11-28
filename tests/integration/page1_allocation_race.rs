//! Regression tests for the page-1 allocation races (upstream PR #10117).
//!
//! When several connections do their first write to an empty database at the
//! same time, `Pager::maybe_allocate_page1` could let more than one of them
//! allocate page 1, and a connection that took `init_lock` right after another
//! one finished tripped the `!db_initialized()` assertion in `allocate_page1`:
//!
//! - The guard used to be a local of `maybe_allocate_page1`, released on every
//!   IO yield from the `Writing`/`Syncing` states. While the first pager waited
//!   for its page-1 write, every other pager found the lock free and ran the
//!   whole `Start` arm itself.
//! - `db_initialized()` was read before the lock was taken and never re-checked
//!   under it, so a pager that saw an empty database and then lost the lock to a
//!   pager that finished entered `allocate_page1` with its state still at
//!   `Start`.
//!
//! The fix takes `init_lock` across the whole page-1 write and re-checks
//! `db_initialized()` under it. These tests run real SQL, so they live in the
//! integration crate (which can depend on `turso_sqlite` for a parse-capable
//! dialect); they reach the pager internals they need through the `test_helper`
//! hooks (`arm_allocate_page1_gap_hook`, `allocating_page1`, `is_init_locked`).

use std::sync::Arc;

use turso_core::{
    ConnectOpts, Connection, Database, LimboError, MemoryIO, SessionRole, Statement, StepResult,
    Value, IO,
};
use turso_sqlite::SqliteDialect;

#[cfg(feature = "io_memory_yield")]
use turso_core::MemoryYieldIO;

fn open_test_database(io: Arc<dyn IO>, path: &str) -> Arc<Database> {
    Database::open_file(io, path, Arc::new(SqliteDialect)).unwrap()
}

#[cfg(feature = "io_memory_yield")]
fn drive_to_done(stmt: &mut Statement, io: &dyn IO) {
    loop {
        match stmt.step().unwrap() {
            StepResult::Done => return,
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                io.step().unwrap();
            }
            StepResult::Row => {}
            other => panic!("unexpected step result {other:?}"),
        }
    }
}

fn query_single(conn: &Arc<Connection>, sql: &str) -> Value {
    let rows = conn.prepare(sql).unwrap().run_collect_rows().unwrap();
    rows[0][0].clone()
}

/// Retries while another connection holds the write lock or has just changed
/// the schema.
fn execute_retrying(conn: &Arc<Connection>, sql: &str) {
    loop {
        match conn.execute(sql) {
            Ok(()) => return,
            Err(LimboError::Busy | LimboError::BusySnapshot | LimboError::SchemaUpdated) => {
                std::thread::yield_now();
            }
            Err(err) => panic!("{sql}: {err}"),
        }
    }
}

fn assert_two_tables_and_clean_pagers(conns: &[&Arc<Connection>]) {
    for conn in conns {
        assert!(conn.is_db_initialized());
        assert!(
            !conn.get_pager().allocating_page1(),
            "a pager is still allocating page 1 after its write completed"
        );
    }
    assert_eq!(
        query_single(
            conns[0],
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table'"
        ),
        Value::from_i64(2)
    );
    assert_eq!(
        query_single(conns[0], "PRAGMA integrity_check").to_string(),
        "ok"
    );
}

/// Steps `conn`'s first write until its pager has submitted the page-1 write,
/// and leaves that completion pending: `MemoryYieldIO` holds every completion
/// until the next `io.step()`.
#[cfg(feature = "io_memory_yield")]
fn park_in_page1_write(conn: &Arc<Connection>, io: &MemoryYieldIO) -> Statement {
    let mut stmt = conn.prepare("CREATE TABLE t(x INTEGER)").unwrap();
    for _ in 0..1000 {
        if conn.get_pager().allocating_page1() {
            break;
        }
        match stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {}
            other => panic!("finished without allocating page 1: {other:?}"),
        }
        if !conn.get_pager().allocating_page1() {
            io.step().unwrap();
        }
    }
    assert!(
        conn.get_pager().allocating_page1(),
        "never reached the page-1 write"
    );
    assert!(!conn.is_db_initialized());
    stmt
}

/// While one pager's page-1 write is in flight, another pager's first write
/// must get Busy rather than allocate page 1 again. `maybe_allocate_page1` used
/// to drop `init_lock` on every IO yield, so the second pager found the lock
/// free and ran the whole `Start` arm itself (a second page-1 image written and
/// fsynced, and a pager that never left `Writing`/`Syncing` once the first one
/// published page 1).
#[cfg(feature = "io_memory_yield")]
#[test]
fn second_writer_gets_busy_while_page1_write_is_in_flight() {
    let io = Arc::new(MemoryYieldIO::new());
    let db = open_test_database(io.clone(), "page1_in_flight.db");
    let a = db.connect(ConnectOpts::new(SessionRole::Superuser)).unwrap();
    let b = db.connect(ConnectOpts::new(SessionRole::Superuser)).unwrap();
    let mut stmt_a = park_in_page1_write(&a, &io);

    // B's first write gets Busy (B has no busy handler) every time it is
    // stepped. Driving the IO between B's steps completes A's pending write,
    // but A is not stepped, so page 1 stays unpublished and the database
    // uninitialized throughout.
    let mut stmt_b = b.prepare("CREATE TABLE u(y INTEGER)").unwrap();
    for _ in 0..20 {
        match stmt_b.step().unwrap() {
            StepResult::Busy => {}
            other => panic!("expected Busy while page 1 was being written, got {other:?}"),
        }
        assert!(
            !b.get_pager().allocating_page1(),
            "second pager started its own page-1 allocation while the first one's write was in flight"
        );
        io.step().unwrap();
    }
    assert!(!b.is_db_initialized());

    // Let A finish, then step the same statement of B again.
    drive_to_done(&mut stmt_a, io.as_ref());
    assert!(a.is_db_initialized());
    drive_to_done(&mut stmt_b, io.as_ref());
    assert_two_tables_and_clean_pagers(&[&a, &b]);
}

/// A pager that sees `!db_initialized()`, then loses the gap before it takes
/// `init_lock` to another pager that allocates page 1 and commits, must find
/// the database initialized once it holds the lock. `maybe_allocate_page1` used
/// not to check again, so the loser entered `allocate_page1` with its state
/// still at `Start` and tripped the `!db_initialized()` assertion there.
#[test]
fn writer_that_loses_the_check_then_lock_gap_sees_initialized_database() {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = open_test_database(io, "page1_gap.db");
    let a = db.connect(ConnectOpts::new(SessionRole::Superuser)).unwrap();
    let b = db.connect(ConnectOpts::new(SessionRole::Superuser)).unwrap();

    // Inside B's check-then-lock gap, A allocates page 1 and commits.
    let a_in_gap = a.clone();
    b.get_pager().arm_allocate_page1_gap_hook(move || {
        a_in_gap.execute("CREATE TABLE t(x INTEGER)").unwrap();
        assert!(a_in_gap.is_db_initialized());
    });

    let sql = "CREATE TABLE u(y INTEGER)";
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| b.execute(sql))) {
        Err(_) => panic!("second pager panicked after losing the check-then-lock gap"),
        // B's read snapshot predates A's commit; the statement is retried.
        Ok(Err(LimboError::Busy | LimboError::BusySnapshot | LimboError::SchemaUpdated)) => {
            execute_retrying(&b, sql);
        }
        Ok(result) => result.unwrap(),
    }
    assert_two_tables_and_clean_pagers(&[&a, &b]);
}

/// Steps B's first write, driving the IO in between, until it is done or
/// `max_steps` is reached.
#[cfg(feature = "io_memory_yield")]
fn first_write_completes(b: &Arc<Connection>, io: &MemoryYieldIO, max_steps: usize) {
    let mut stmt_b = b.prepare("CREATE TABLE u(y INTEGER)").unwrap();
    for _ in 0..max_steps {
        match stmt_b.step().unwrap() {
            StepResult::Done => return,
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } | StepResult::Busy => {}
            other => panic!("unexpected step result {other:?}"),
        }
        io.step().unwrap();
    }
    panic!("B's first write did not complete within {max_steps} steps: init_lock was never released");
}

/// A statement dropped while its pager is writing page 1 must release
/// `init_lock` on the way out: other pagers get Busy while it is held, so a
/// leaked lock would fail every other first write forever.
#[cfg(feature = "io_memory_yield")]
#[test]
fn dropped_statement_releases_init_lock_mid_allocation() {
    let io = Arc::new(MemoryYieldIO::new());
    let db = open_test_database(io.clone(), "page1_dropped.db");
    let a = db.connect(ConnectOpts::new(SessionRole::Superuser)).unwrap();
    let b = db.connect(ConnectOpts::new(SessionRole::Superuser)).unwrap();

    let stmt_a = park_in_page1_write(&a, &io);
    assert!(a.get_pager().is_init_locked());
    drop(stmt_a);

    first_write_completes(&b, &io, 100);
    assert!(!a.get_pager().allocating_page1());
    assert!(b.is_db_initialized());
    assert_eq!(query_single(&b, "PRAGMA integrity_check").to_string(), "ok");
}

/// Same as above, but the connection is closed while the statement is still
/// alive.
#[cfg(feature = "io_memory_yield")]
#[test]
fn closed_connection_releases_init_lock_mid_allocation() {
    let io = Arc::new(MemoryYieldIO::new());
    let db = open_test_database(io.clone(), "page1_closed.db");
    let a = db.connect(ConnectOpts::new(SessionRole::Superuser)).unwrap();
    let b = db.connect(ConnectOpts::new(SessionRole::Superuser)).unwrap();

    let stmt_a = park_in_page1_write(&a, &io);
    a.close().unwrap();
    drop(stmt_a);

    first_write_completes(&b, &io, 100);
    assert!(!a.get_pager().allocating_page1());
    assert!(b.is_db_initialized());
    assert_eq!(query_single(&b, "PRAGMA integrity_check").to_string(), "ok");
}

/// The busy handler applies to a first write that finds page 1 being allocated
/// by another connection: with a busy timeout, `execute` gives up with Busy
/// once it expires instead of waiting for an allocating statement that nobody
/// steps. Once that statement has finished, the same write succeeds.
#[cfg(feature = "io_memory_yield")]
#[test]
fn busy_timeout_applies_while_page1_write_is_in_flight() {
    let io = Arc::new(MemoryYieldIO::new());
    let db = open_test_database(io.clone(), "page1_busy_timeout.db");
    let a = db.connect(ConnectOpts::new(SessionRole::Superuser)).unwrap();
    let b = db.connect(ConnectOpts::new(SessionRole::Superuser)).unwrap();
    b.set_busy_timeout(std::time::Duration::from_millis(200));
    let mut stmt_a = park_in_page1_write(&a, &io);

    // On another thread, so that waiting forever fails the test instead of
    // hanging it.
    let (tx, rx) = std::sync::mpsc::channel();
    let b_thread = b.clone();
    std::thread::spawn(move || {
        let _ = tx.send(b_thread.execute("CREATE TABLE u(y INTEGER)"));
    });
    match rx.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(Err(LimboError::Busy)) => {}
        Ok(other) => panic!("expected Busy while page 1 was being written, got {other:?}"),
        Err(_) => panic!("second writer did not return within 10s"),
    }
    assert!(!b.is_db_initialized());
    assert!(!b.get_pager().allocating_page1());

    drive_to_done(&mut stmt_a, io.as_ref());
    b.execute("CREATE TABLE u(y INTEGER)").unwrap();
    assert_two_tables_and_clean_pagers(&[&a, &b]);
}

/// Many connections doing their first write to the same empty database at once,
/// each with a busy timeout: none trips an assertion, no pager is left
/// mid-allocation (which a second allocation of page 1 would leave behind), and
/// every write lands.
#[test]
fn concurrent_first_writes_on_an_empty_database() {
    const CONNECTIONS: usize = 32;
    for round in 0..10 {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = open_test_database(io, &format!("page1_concurrent_{round}.db"));
        let barrier = Arc::new(std::sync::Barrier::new(CONNECTIONS));
        let threads: Vec<_> = (0..CONNECTIONS)
            .map(|i| {
                let conn = db.connect(ConnectOpts::new(SessionRole::Superuser)).unwrap();
                conn.set_busy_timeout(std::time::Duration::from_secs(10));
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    execute_retrying(&conn, &format!("CREATE TABLE t{i}(x INTEGER)"));
                    conn
                })
            })
            .collect();
        let conns: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        for conn in &conns {
            assert!(conn.is_db_initialized());
            assert!(!conn.get_pager().allocating_page1());
        }
        assert_eq!(
            query_single(
                &conns[0],
                "SELECT count(*) FROM sqlite_schema WHERE type = 'table'"
            ),
            Value::from_i64(CONNECTIONS as i64)
        );
        assert_eq!(
            query_single(&conns[0], "PRAGMA integrity_check").to_string(),
            "ok"
        );
    }
}
