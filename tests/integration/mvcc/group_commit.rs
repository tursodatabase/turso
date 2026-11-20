use super::checkpoint::{get_rows, FixedYieldInjector};
use super::test_db::MvccTestDbNoConn;
use crate::mvcc::SqliteDialect;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use turso_core::io::{FileId, FileSyncType};
use turso_core::mvcc::database::CommitYieldPoint;
use turso_core::mvcc::yield_hooks::YieldPointMarker;
use turso_core::security::roles::SessionRole;
use turso_core::{
    Buffer, Clock, Completion, ConnectOpts, Connection, Database, File, LimboError, MemoryIO,
    MonotonicInstant, OpenFlags, Result, Statement, StepResult, SyncMode, Value, WallClockInstant,
    IO,
};

fn pragma_int(conn: &Arc<Connection>, query: &str) -> i64 {
    let rows = get_rows(conn, query);
    assert_eq!(rows.len(), 1, "{query} returned {rows:?}");
    match rows[0][0] {
        Value::Numeric(turso_core::Numeric::Integer(n)) => n,
        ref other => panic!("{query} returned {other:?}"),
    }
}

fn exec_retry(conn: &Arc<Connection>, sql: &str) -> Result<(), LimboError> {
    for _ in 0..100_000 {
        match conn.execute(sql) {
            Ok(()) => return Ok(()),
            Err(LimboError::Busy) => std::thread::yield_now(),
            Err(err) => return Err(err),
        }
    }
    Err(LimboError::Busy)
}

#[test]
fn group_commit_pragma_defaults_on_and_round_trips() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    assert_eq!(pragma_int(&conn, "PRAGMA mvcc_group_commit"), 1);

    for off in ["no", "off", "false", "0"] {
        conn.execute(format!("PRAGMA mvcc_group_commit = {off}"))
            .unwrap();
        assert_eq!(
            pragma_int(&conn, "PRAGMA mvcc_group_commit"),
            0,
            "`= {off}` should disable group commit"
        );
        conn.execute("PRAGMA mvcc_group_commit = on").unwrap();
        assert_eq!(pragma_int(&conn, "PRAGMA mvcc_group_commit"), 1);
    }
}

#[test]
fn group_commit_pragma_is_store_wide() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setter = db.connect();
    let observer = db.connect();

    setter.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    assert_eq!(pragma_int(&observer, "PRAGMA mvcc_group_commit"), 1);
}

#[test]
fn group_commit_pragma_needs_mvcc() {
    let io = Arc::new(turso_core::io::MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();

    let err = conn
        .execute("PRAGMA mvcc_group_commit = yes")
        .expect_err("group commit needs an MVCC store");
    assert!(
        err.to_string().contains("MVCC not enabled"),
        "unexpected error: {err}"
    );
    let err = conn
        .prepare("PRAGMA mvcc_group_commit")
        .expect_err("querying group commit needs an MVCC store");
    assert!(
        err.to_string().contains("MVCC not enabled"),
        "unexpected error: {err}"
    );
}

fn two_writers_both_commit(db: MvccTestDbNoConn, group_commit: bool) {
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    if group_commit {
        setup.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    }
    setup.close().unwrap();

    let conn_a = db.connect();
    let conn_b = db.connect();
    let rounds = 40i64;
    for round in 0..rounds {
        conn_a.execute("BEGIN CONCURRENT").unwrap();
        conn_b.execute("BEGIN CONCURRENT").unwrap();
        exec_retry(&conn_a, &format!("INSERT INTO t VALUES ({}, 1)", round * 2)).unwrap();
        exec_retry(
            &conn_b,
            &format!("INSERT INTO t VALUES ({}, 1)", round * 2 + 1),
        )
        .unwrap();
        exec_retry(&conn_a, "COMMIT").unwrap();
        exec_retry(&conn_b, "COMMIT").unwrap();
    }

    let reader = db.connect();
    assert_eq!(
        get_rows(&reader, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::from_i64(rounds * 2)]]
    );
}

#[test]
fn two_writers_both_commit_with_group_commit_truncate() {
    two_writers_both_commit(MvccTestDbNoConn::new_with_random_db(), true);
}

#[test]
fn two_writers_both_commit_with_group_commit_passive() {
    two_writers_both_commit(MvccTestDbNoConn::new_with_random_db_passive(), true);
}

#[test]
fn two_writers_both_commit_with_group_commit_off_truncate() {
    two_writers_both_commit(MvccTestDbNoConn::new_with_random_db(), false);
}

#[test]
fn two_writers_both_commit_with_group_commit_off_passive() {
    two_writers_both_commit(MvccTestDbNoConn::new_with_random_db_passive(), false);
}

#[test]
fn batched_records_survive_a_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    const WRITERS: usize = 4;
    let rounds = 25usize;
    {
        let setup = db.connect();
        setup
            .execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        setup
            .execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        setup.execute("PRAGMA mvcc_group_commit = yes").unwrap();
        setup.close().unwrap();

        let shared = Arc::new(db.get_db());
        let barrier = Arc::new(Barrier::new(WRITERS));
        let writers = (0..WRITERS)
            .map(|writer| {
                let shared = shared.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let conn = shared
                        .connect(ConnectOpts::new(SessionRole::Superuser))
                        .unwrap();
                    for round in 0..rounds {
                        let pk = (round * WRITERS + writer) as i64;
                        conn.execute("BEGIN CONCURRENT").unwrap();
                        exec_retry(&conn, &format!("INSERT INTO t VALUES ({pk}, 1)")).unwrap();
                        barrier.wait();
                        exec_retry(&conn, "COMMIT").unwrap();
                    }
                    conn.close().unwrap();
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }
    }

    db.restart();
    let reader = db.connect();
    assert_eq!(
        get_rows(&reader, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::from_i64((WRITERS * rounds) as i64)]]
    );
}

fn step_until_yield_or_done(stmt: &mut turso_core::Statement) -> StepResult {
    for _ in 0..10_000 {
        match stmt.step().unwrap() {
            StepResult::IO => continue,
            other => return other,
        }
    }
    panic!("statement kept returning IO")
}

#[test]
fn dropped_commit_after_log_record_is_written_still_commits() {
    dropped_after_log_record_written_still_commits(true);
}

#[test]
fn dropped_commit_after_log_record_is_written_still_commits_without_group() {
    dropped_after_log_record_written_still_commits(false);
}

fn dropped_after_log_record_written_still_commits(group_commit: bool) {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    if group_commit {
        conn.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    }

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordMarkedWritten.point(),
    ])));

    {
        let mut commit = conn.prepare("COMMIT").unwrap();
        assert!(
            matches!(step_until_yield_or_done(&mut commit), StepResult::Yield),
            "COMMIT should yield after owning the logical-log record"
        );
    }

    assert!(
        conn.get_mv_tx().map(|(tx_id, _)| tx_id).is_none(),
        "the dropped commit should be finished, not rolled back"
    );

    let rows = get_rows(&conn, "SELECT pk FROM t");
    assert_eq!(rows, vec![vec![Value::from_i64(1)]]);
}

#[test]
fn begin_immediate_still_commits_with_group_commit_on() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    conn.execute("BEGIN IMMEDIATE").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        get_rows(&conn, "SELECT pk FROM t"),
        vec![vec![Value::from_i64(1)]]
    );
}

#[test]
fn commits_batch_into_one_group() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    setup.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    setup.close().unwrap();

    const WRITERS: usize = 8;
    const MAX_ROUNDS: usize = 2000;
    let store = db.get_mvcc_store();
    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(WRITERS));
    let committed = Arc::new(AtomicUsize::new(0));
    let largest_group = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + Duration::from_secs(120);

    let writers = (0..WRITERS)
        .map(|writer| {
            let db = db.clone();
            let barrier = barrier.clone();
            let committed = committed.clone();
            let largest_group = largest_group.clone();
            let stop = stop.clone();
            let store = store.clone();
            std::thread::spawn(move || {
                let conn = db.connect();
                for round in 0..MAX_ROUNDS {
                    let pk = (round * WRITERS + writer) as i64;
                    conn.execute("BEGIN CONCURRENT").unwrap();
                    exec_retry(&conn, &format!("INSERT INTO t VALUES ({pk}, 1)")).unwrap();
                    barrier.wait();
                    exec_retry(&conn, "COMMIT").unwrap();
                    committed.fetch_add(1, Ordering::Relaxed);
                    largest_group.fetch_max(store.last_group_commit_size(), Ordering::Relaxed);
                    barrier.wait();
                    if writer == 0 {
                        let grouped = largest_group.load(Ordering::Relaxed) >= 2;
                        stop.store(grouped || Instant::now() >= deadline, Ordering::Release);
                    }
                    barrier.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                }
                conn.close().unwrap();
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }

    let largest = largest_group.load(Ordering::Relaxed);
    assert!(
        largest >= 2,
        "no commit ever appended another connection's record: largest batch was {largest}"
    );

    let reader = db.connect();
    assert_eq!(
        get_rows(&reader, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::from_i64(
            committed.load(Ordering::Relaxed) as i64
        )]]
    );
}

/// Regression test for MVCC concurrent commit spinning inside one step.
///
/// A COMMIT that finds the commit lock held waits on a completion the lock
/// holder finishes. `step()` must return right away with IO, never spin inside
/// a single call, or cooperative schedulers deadlock.
///
/// We simulate lock contention by taking the commit lock before calling
/// COMMIT.
#[test]
fn test_concurrent_commit_yield_spin() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();

    let mv_store = db.get_mvcc_store();
    assert!(mv_store.lock_commits_for_testing(), "should acquire lock");

    let mut stmt = conn.prepare("COMMIT").unwrap();
    assert!(
        matches!(stmt.step().unwrap(), StepResult::IO),
        "step() should wait when the commit lock is held"
    );
    assert_eq!(mv_store.waiting_commit_count_for_testing(), 1);

    mv_store.unlock_commits_for_testing();
    loop {
        match stmt.step().unwrap() {
            StepResult::Done => break,
            StepResult::IO => {}
            _ => {}
        }
    }

    // Verify the insert is visible
    let rows = get_rows(&conn, "SELECT COUNT(*) FROM t");
    assert_eq!(rows[0][0].as_int().unwrap(), 1);
}

/// if a txn made some inserts, then aborted (or abandoned due to some IO issue), then those
/// inserted rows should not be visible
#[test]
fn test_abandoned_commit_rolls_back_insert() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'new')").unwrap();

    abandon_commit_after_first_io(&conn, &db);

    let observer = db.connect();
    let rows = get_rows(&observer, "SELECT id FROM t WHERE id = 1");
    assert!(
        rows.is_empty(),
        "row from abandoned INSERT commit remained visible: {rows:?}",
    );
    observer.close().unwrap();
}

/// if a txn deleted some existing rows, but then aborted (or abandoned due to some IO issue), then
/// those rows should not become deleted
#[test]
fn test_abandoned_commit_rolls_back_delete() {
    let db = MvccTestDbNoConn::new();
    let conn = db.connect();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'seed')").unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("DELETE FROM t WHERE id = 1").unwrap();

    abandon_commit_after_first_io(&conn, &db);

    let observer = db.connect();
    let rows = get_rows(&observer, "SELECT id, v FROM t WHERE id = 1");
    assert_eq!(
        rows,
        vec![vec![Value::from_i64(1), Value::from_text("seed")]],
        "row disappeared after abandoned DELETE commit: {rows:?}",
    );
    observer.close().unwrap();
}

fn abandon_commit_after_first_io(conn: &Arc<Connection>, db: &MvccTestDbNoConn) {
    let mv_store = db.get_mvcc_store();
    assert!(
        mv_store.lock_commits_for_testing(),
        "should acquire commit lock"
    );

    let mut stmt = conn.prepare("COMMIT").unwrap();
    assert!(
        matches!(stmt.step().unwrap(), StepResult::IO),
        "COMMIT should wait while the commit lock is held",
    );
    assert_eq!(mv_store.waiting_commit_count_for_testing(), 1);

    drop(stmt);
    assert_eq!(
        mv_store.waiting_commit_count_for_testing(),
        0,
        "dropping a waiting COMMIT withdraws it from the queue"
    );
    mv_store.unlock_commits_for_testing();
    conn.close().unwrap();
}

#[test]
fn commit_parks_once_while_another_transaction_holds_the_commit_lock() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();

    let store = db.get_mvcc_store();
    assert!(store.lock_commits_for_testing());

    let mut commit = conn.prepare("COMMIT").unwrap();
    assert!(matches!(commit.step().unwrap(), StepResult::IO));
    assert_eq!(store.waiting_commit_count_for_testing(), 1);
    assert_eq!(store.commit_wait_count_for_testing(), 1);
    for _ in 0..100 {
        assert!(matches!(commit.step().unwrap(), StepResult::IO));
    }
    assert_eq!(
        store.commit_wait_count_for_testing(),
        1,
        "a waiting commit must not run its wait step again until it is woken"
    );

    store.unlock_commits_for_testing();
    assert_eq!(
        store.waiting_commit_count_for_testing(),
        0,
        "releasing the commit lock wakes every waiting commit"
    );
    step_until_done(&mut commit);
    assert_eq!(
        get_rows(&conn, "SELECT pk FROM t"),
        vec![vec![Value::from_i64(1)]]
    );
}

#[test]
fn parked_waiter_wakes_when_the_leader_makes_it_durable() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    setup.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    setup.close().unwrap();

    let conn_a = db.connect();
    let conn_b = db.connect();
    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_b.execute("BEGIN CONCURRENT").unwrap();
    conn_a.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    conn_b.execute("INSERT INTO t VALUES (2, 1)").unwrap();

    let store = db.get_mvcc_store();
    assert!(store.lock_commits_for_testing());

    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    let mut commit_b = conn_b.prepare("COMMIT").unwrap();
    assert!(matches!(commit_a.step().unwrap(), StepResult::IO));
    assert!(matches!(commit_b.step().unwrap(), StepResult::IO));
    assert_eq!(store.waiting_commit_count_for_testing(), 2);

    store.unlock_commits_for_testing();
    step_until_done(&mut commit_a);
    assert!(
        store.last_group_commit_size() >= 2,
        "the leader must write the waiter's record in its batch"
    );
    assert_eq!(
        store.waiting_commit_count_for_testing(),
        0,
        "the leader making the batch durable wakes the waiter"
    );
    step_until_done(&mut commit_b);

    let reader = db.connect();
    assert_eq!(
        get_rows(&reader, "SELECT pk FROM t ORDER BY pk"),
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]]
    );
}

#[test]
fn dropped_waiter_after_log_tx_still_commits() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    setup.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    setup.close().unwrap();
    drop(setup);

    let conn_a = db.connect();
    let conn_b = db.connect();
    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_b.execute("BEGIN CONCURRENT").unwrap();
    conn_a.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    conn_b.execute("INSERT INTO t VALUES (2, 1)").unwrap();

    conn_a.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
        CommitYieldPoint::LogicalLogWriteIssued.point(),
    ])));
    conn_b.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
        CommitYieldPoint::LogicalLogWriteIssued.point(),
    ])));

    let store = db.get_mvcc_store();
    assert!(
        store.lock_commits_for_testing(),
        "hold the commit lock so both enqueue"
    );

    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    let mut commit_b = conn_b.prepare("COMMIT").unwrap();
    assert!(matches!(
        step_until_yield_or_done(&mut commit_a),
        StepResult::Yield
    ));
    // Begins after conn_a's commit timestamp and before conn_b's.
    let ddl = db.connect();
    ddl.execute("BEGIN DEFERRED").unwrap();
    get_rows(&ddl, "SELECT COUNT(*) FROM t");
    assert!(matches!(
        step_until_yield_or_done(&mut commit_b),
        StepResult::Yield
    ));
    assert!(matches!(commit_a.step().unwrap(), StepResult::IO));
    assert!(matches!(commit_b.step().unwrap(), StepResult::IO));
    assert_eq!(
        store.waiting_commit_count_for_testing(),
        2,
        "both commits wait for the held lock"
    );

    store.unlock_commits_for_testing();

    assert!(
        matches!(step_until_yield_or_done(&mut commit_a), StepResult::Yield),
        "leader should yield after issuing log_tx for the waiter"
    );
    assert!(
        store.last_group_commit_size() >= 2,
        "both commits must be in one batch"
    );

    drop(commit_b);
    conn_a.set_yield_injector(None);
    loop {
        match commit_a.step().unwrap() {
            StepResult::Done => break,
            StepResult::IO | StepResult::Yield => continue,
            other => panic!("leader COMMIT ended with {other:?}"),
        }
    }

    let reader = db.connect();
    assert_eq!(
        get_rows(&reader, "SELECT pk FROM t ORDER BY pk"),
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]],
        "waiter dropped after log_tx must still commit"
    );
    let result = ddl.execute("CREATE INDEX idx_v ON t(v)");
    assert!(
        matches!(result, Err(LimboError::Busy)),
        "transaction older than the dropped waiter's commit should get Busy: {result:?}"
    );
    ddl.execute("ROLLBACK").unwrap();

    drop((commit_a, reader, ddl, conn_a, conn_b, store));
    db.restart();
    let reader = db.connect();
    assert_eq!(
        get_rows(&reader, "SELECT pk FROM t ORDER BY pk"),
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]],
        "recovery must not replay an aborted waiter"
    );
}

fn step_until_done(stmt: &mut Statement) {
    for _ in 0..10_000 {
        match stmt.step().unwrap() {
            StepResult::Done => return,
            StepResult::IO | StepResult::Yield => continue,
            other => panic!("COMMIT ended with {other:?}"),
        }
    }
    panic!("statement never finished")
}

#[test]
fn full_waiter_is_synced_when_group_leader_uses_sync_off() {
    let (_db, io, [mut leader, mut waiter]) =
        prepare_group_commit("group-commit-sync-off-leader.db", SyncMode::Off);
    let before = io.syncs.load(Ordering::Acquire);
    step_until_done(&mut leader);
    step_until_done(&mut waiter);
    assert!(
        io.syncs.load(Ordering::Acquire) > before,
        "FULL waiter returned success with zero sync calls"
    );
}

/// Opens an in-memory database in which two connections wait in COMMIT to be
/// written in the same group commit. The first one uses `leader_sync`, the
/// second one SyncMode::Full.
fn prepare_group_commit(
    path: &str,
    leader_sync: SyncMode,
) -> (Arc<Database>, Arc<SyncCountingIo>, [Statement; 2]) {
    let io = Arc::new(SyncCountingIo::default());
    let db = Database::open_file(io.clone(), path, Arc::new(SqliteDialect)).unwrap();
    let leader = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    leader.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    leader
        .execute("CREATE TABLE t(pk INTEGER PRIMARY KEY)")
        .unwrap();
    leader
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    let waiter = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    leader.set_sync_mode(leader_sync);
    waiter.set_sync_mode(SyncMode::Full);
    for (conn, pk) in [(&leader, 1), (&waiter, 2)] {
        conn.execute("BEGIN CONCURRENT").unwrap();
        conn.execute(format!("INSERT INTO t VALUES ({pk})"))
            .unwrap();
    }
    let store = db.get_mv_store().clone().unwrap();
    assert!(store.lock_commits_for_testing());
    let mut commits = [
        leader.prepare("COMMIT").unwrap(),
        waiter.prepare("COMMIT").unwrap(),
    ];
    for commit in &mut commits {
        assert!(matches!(commit.step().unwrap(), StepResult::IO));
    }
    store.unlock_commits_for_testing();
    (db, io, commits)
}

/// An in-memory IO that counts file syncs.
#[derive(Default)]
struct SyncCountingIo {
    inner: MemoryIO,
    syncs: Arc<AtomicUsize>,
}

impl Clock for SyncCountingIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for SyncCountingIo {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
        Ok(Arc::new(SyncCountingFile {
            inner: self.inner.open_file(path, flags, direct)?,
            syncs: self.syncs.clone(),
        }))
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn file_id(&self, path: &str) -> Result<FileId> {
        self.inner.file_id(path)
    }
}

struct SyncCountingFile {
    inner: Arc<dyn File>,
    syncs: Arc<AtomicUsize>,
}

impl File for SyncCountingFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.inner.pwrite(pos, buffer, c)
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        self.syncs.fetch_add(1, Ordering::AcqRel);
        self.inner.sync(c, sync_type)
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.inner.truncate(len, c)
    }
}
