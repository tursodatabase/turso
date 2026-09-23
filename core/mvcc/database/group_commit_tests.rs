use super::{get_rows, FixedYieldInjector, MvccTestDbNoConn};
use crate::io::{FileSyncType, PlatformIO, IO};
use crate::mvcc::database::{
    CommitCoordinator, CommitYieldPoint, GroupWork, LogRecord, RowVersion,
};
use crate::mvcc::persistent_storage::logical_log::{LogHeader, OnSerializationComplete};
use crate::mvcc::persistent_storage::{DurableStorage, LogicalLogTruncateOutcome, Storage};
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::storage::encryption::EncryptionContext;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crate::sync::Mutex;
use crate::{
    Completion, Connection, Database, DatabaseOpts, LimboError, OpenFlags, SqliteDialect,
    StepResult, Value,
};
use std::cell::RefCell;
use std::sync::{Arc, Barrier, Weak};
use std::time::{Duration, Instant};

fn pragma_int(conn: &Arc<Connection>, query: &str) -> i64 {
    let rows = get_rows(conn, query);
    assert_eq!(rows.len(), 1, "{query} returned {rows:?}");
    match rows[0][0] {
        Value::Numeric(crate::Numeric::Integer(n)) => n,
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
    let io = Arc::new(crate::io::MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(crate::SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();

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
                    let conn = shared.connect().unwrap();
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

fn empty_record(end_ts: u64) -> LogRecord {
    LogRecord::empty(end_ts, crate::alloc::DynAllocator::default())
}

#[test]
fn requeued_records_go_back_in_ticket_order() {
    let coordinator = CommitCoordinator::new();
    let first = coordinator.enqueue(1, empty_record(10));
    let second = coordinator.enqueue(2, empty_record(20));

    let batch = coordinator.take_pending();
    assert_eq!(
        batch.iter().map(|queued| queued.ticket).collect::<Vec<_>>(),
        vec![first, second]
    );

    let latecomer = coordinator.enqueue(3, empty_record(30));
    coordinator.requeue(batch.into_iter());
    assert_eq!(
        coordinator
            .take_pending()
            .iter()
            .map(|queued| queued.ticket)
            .collect::<Vec<_>>(),
        vec![first, second, latecomer]
    );
}

#[test]
fn leaving_removes_a_queued_record_and_withdraws_a_taken_one() {
    let coordinator = CommitCoordinator::new();

    let queued = coordinator.enqueue(1, empty_record(10));
    assert!(!coordinator.leave(1, Some(queued)));
    assert!(
        coordinator.take_pending().is_empty(),
        "a queued record leaves the queue"
    );

    let taken = coordinator.enqueue(2, empty_record(20));
    let GroupWork::Lead { writing, .. } = coordinator.take_work() else {
        panic!("the leader takes the queued record");
    };
    assert!(!coordinator.leave(2, Some(taken)));
    assert!(
        !coordinator.try_issue(writing.tx_id),
        "the leader skips a record whose commit left"
    );
}

#[test]
fn waiter_that_leaves_after_its_write_was_given_up_leaves_no_retry_hole() {
    let coordinator = CommitCoordinator::new();
    let ticket = coordinator.enqueue(1, empty_record(10));
    let GroupWork::Lead { writing, .. } = coordinator.take_work() else {
        panic!("the leader takes the queued record");
    };
    assert!(coordinator.try_issue(writing.tx_id));
    assert!(
        !coordinator.release_issued(&writing),
        "the waiter did not leave, so it must retry"
    );
    assert!(!coordinator.leave(1, Some(ticket)));

    coordinator.enqueue(2, empty_record(20));
    assert!(
        matches!(coordinator.take_work(), GroupWork::Lead { .. }),
        "a retry hole without a waiter must not stop later batches"
    );
}

#[test]
fn waiter_that_leaves_while_its_write_is_issued_is_abandoned_to_the_leader() {
    let coordinator = CommitCoordinator::new();
    let ticket = coordinator.enqueue(1, empty_record(10));
    let GroupWork::Lead { writing, .. } = coordinator.take_work() else {
        panic!("the leader takes the queued record");
    };
    assert!(coordinator.try_issue(writing.tx_id));
    assert!(
        coordinator.leave(1, Some(ticket)),
        "a waiter whose write is issued is abandoned to the leader"
    );
    assert!(
        coordinator.release_issued(&writing),
        "the leader rolls back the abandoned waiter"
    );

    coordinator.enqueue(2, empty_record(20));
    assert!(
        matches!(coordinator.take_work(), GroupWork::Lead { .. }),
        "giving up an abandoned write leaves no retry hole"
    );
}

#[test]
fn durability_watermark_only_moves_forward() {
    let coordinator = CommitCoordinator::new();
    coordinator.note_written(7);
    coordinator.mark_durable(7);
    coordinator.mark_durable(3);
    assert_eq!(coordinator.durable_through(), 7);
}

#[test]
fn failed_leader_does_not_publish_unsynced_prefix() {
    let coordinator = CommitCoordinator::new();
    let first = coordinator.enqueue(1, empty_record(10));
    let second = coordinator.enqueue(2, empty_record(20));
    let mut batch = coordinator.take_pending();
    let writing = batch.pop_front().unwrap();
    assert_eq!(writing.ticket, first);
    coordinator.note_written(first);
    coordinator.requeue(batch.into_iter());

    assert_eq!(
        coordinator.durable_through(),
        0,
        "owning log bytes is not the same as fsyncing them"
    );
    assert_eq!(coordinator.written_through(), first);

    coordinator.mark_durable(second);
    assert_eq!(
        coordinator.durable_through(),
        first,
        "durable cannot pass a ticket that was not owned"
    );
}

#[test]
fn failed_mid_batch_leader_does_not_cover_retry_hole() {
    let coordinator = CommitCoordinator::new();
    let t2 = coordinator.enqueue(2, empty_record(20));
    let t3 = coordinator.enqueue(3, empty_record(30));
    let t4 = coordinator.enqueue(4, empty_record(40));
    assert_eq!((t2, t3, t4), (1, 2, 3));

    let GroupWork::Lead { writing, mut rest } = coordinator.take_work() else {
        panic!("the leader takes the queued records");
    };
    assert!(coordinator.try_issue(writing.tx_id));
    coordinator.note_written(writing.ticket);
    assert!(!coordinator.finish_issue(writing.tx_id));
    let retried = rest.pop_front().unwrap();
    assert!(coordinator.try_issue(retried.tx_id));
    assert!(!coordinator.release_issued(&retried));
    coordinator.requeue(rest.into_iter());
    coordinator.note_written(t4);
    coordinator.mark_durable(coordinator.written_through());

    assert!(
        coordinator.durable_through() < t3,
        "T3 is a retry hole; durable must not cover it (durable={}, t3={t3})",
        coordinator.durable_through()
    );
    assert!(
        !matches!(
            coordinator.take_work(),
            GroupWork::Lead { writing, .. } if writing.ticket >= t3
        ),
        "later tickets cannot be taken while an earlier ticket is a retry hole"
    );
    assert!(
        coordinator.take_retry(t3),
        "T3 must still be retrying, not acked via the watermark"
    );
}

fn step_until_yield_or_done(stmt: &mut crate::Statement) -> StepResult {
    for _ in 0..10_000 {
        match stmt.step().unwrap() {
            StepResult::IO => continue,
            other => return other,
        }
    }
    panic!("statement kept returning IO")
}

#[test]
fn dropped_commit_after_log_record_is_owned_still_commits() {
    dropped_after_own_still_commits(true);
}

#[test]
fn dropped_commit_after_log_record_is_owned_still_commits_without_group() {
    dropped_after_own_still_commits(false);
}

fn dropped_after_own_still_commits(group_commit: bool) {
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
        CommitYieldPoint::LogicalLogOwned.point(),
    ])));

    {
        let mut commit = conn.prepare("COMMIT").unwrap();
        assert!(
            matches!(step_until_yield_or_done(&mut commit), StepResult::Yield),
            "COMMIT should yield after owning the logical-log record"
        );
    }

    assert!(
        conn.get_mv_tx_id().is_none(),
        "the dropped commit should be finished, not rolled back"
    );

    let rows = get_rows(&conn, "SELECT pk FROM t");
    assert_eq!(rows, vec![vec![Value::from_i64(1)]]);
}

fn step_until_done(stmt: &mut crate::Statement) {
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
fn commit_parks_once_while_another_transaction_holds_the_commit_lock() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();

    let store = db.get_mvcc_store();
    let coordinator = &store.commit_coordinator;
    assert!(coordinator.pager_commit_lock.write());

    let mut commit = conn.prepare("COMMIT").unwrap();
    assert!(matches!(commit.step().unwrap(), StepResult::IO));
    assert_eq!(coordinator.parked_tickets().len(), 1);
    assert_eq!(coordinator.park_calls(), 1);
    for _ in 0..100 {
        assert!(matches!(commit.step().unwrap(), StepResult::IO));
    }
    assert_eq!(
        coordinator.park_calls(),
        1,
        "a parked commit must not re-run its wait step until it is woken"
    );

    coordinator.unlock_pager_commit_lock();
    assert!(
        coordinator.parked_tickets().is_empty(),
        "releasing the commit lock wakes every parked commit"
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
    let coordinator = &store.commit_coordinator;
    assert!(coordinator.pager_commit_lock.write());

    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    let mut commit_b = conn_b.prepare("COMMIT").unwrap();
    assert!(matches!(commit_a.step().unwrap(), StepResult::IO));
    assert!(matches!(commit_b.step().unwrap(), StepResult::IO));
    assert_eq!(coordinator.parked_tickets().len(), 2);

    coordinator.unlock_pager_commit_lock();
    step_until_done(&mut commit_a);
    assert!(
        store.last_group_commit_size() >= 2,
        "the leader must write the waiter's record in its batch"
    );
    assert!(
        coordinator.parked_tickets().is_empty(),
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
fn dropped_waiter_after_log_tx_still_commits() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
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

    conn_a.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
        CommitYieldPoint::LogicalLogWriteIssued.point(),
    ])));
    conn_b.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
        CommitYieldPoint::LogicalLogWriteIssued.point(),
    ])));

    let store = db.get_mvcc_store();
    let coordinator = &store.commit_coordinator;
    assert!(
        coordinator.pager_commit_lock.write(),
        "hold the commit lock so both enqueue"
    );

    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    let mut commit_b = conn_b.prepare("COMMIT").unwrap();
    assert!(matches!(
        step_until_yield_or_done(&mut commit_a),
        StepResult::Yield
    ));
    assert!(matches!(
        step_until_yield_or_done(&mut commit_b),
        StepResult::Yield
    ));
    assert!(matches!(commit_a.step().unwrap(), StepResult::IO));
    assert!(matches!(commit_b.step().unwrap(), StepResult::IO));
    assert_eq!(
        coordinator.parked_tickets().len(),
        2,
        "both commits park on the held lock"
    );

    coordinator.unlock_pager_commit_lock();

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

    db.restart();
    let reader = db.connect();
    assert_eq!(
        get_rows(&reader, "SELECT pk FROM t ORDER BY pk"),
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]],
        "recovery must not replay an aborted waiter"
    );
}

thread_local! {
    static DROPPED_BY_STORAGE: RefCell<Option<crate::Statement>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageCall {
    UpgradeHeader,
    LogTx,
}

#[derive(Debug)]
struct DropStatementStorage {
    inner: Arc<dyn DurableStorage>,
    call: StorageCall,
    countdown: Mutex<usize>,
}

impl DropStatementStorage {
    fn drop_statement_on_call(&self, calls_until_drop: usize) {
        *self.countdown.lock() = calls_until_drop;
    }

    fn on_call(&self, call: StorageCall) {
        let drop_now = {
            let mut countdown = self.countdown.lock();
            if call == self.call && *countdown > 0 {
                *countdown -= 1;
                *countdown == 0
            } else {
                false
            }
        };
        if drop_now {
            DROPPED_BY_STORAGE.with(|slot| drop(slot.borrow_mut().take()));
        }
    }
}

impl DurableStorage for DropStatementStorage {
    fn serialize_row_version(
        &self,
        log_record: &mut LogRecord,
        row_version: &RowVersion,
        portable_extension: Option<&[u8]>,
    ) -> crate::Result<()> {
        self.inner
            .serialize_row_version(log_record, row_version, portable_extension)
    }
    fn serialize_database_header(
        &self,
        log_record: &mut LogRecord,
        header: &DatabaseHeader,
    ) -> crate::Result<()> {
        self.inner.serialize_database_header(log_record, header)
    }
    fn log_tx(
        &self,
        m: LogRecord,
        c: OnSerializationComplete<'_>,
    ) -> crate::Result<(Completion, u64)> {
        self.on_call(StorageCall::LogTx);
        self.inner.log_tx(m, c)
    }
    fn upgrade_header_for_log_tx(&self, m: &LogRecord) -> crate::Result<Option<Completion>> {
        self.on_call(StorageCall::UpgradeHeader);
        self.inner.upgrade_header_for_log_tx(m)
    }
    fn sync(&self, t: FileSyncType) -> crate::Result<Completion> {
        self.inner.sync(t)
    }
    fn update_header(&self) -> crate::Result<Completion> {
        self.inner.update_header()
    }
    fn truncate(
        &self,
        checkpointed_through_ts: u64,
    ) -> crate::Result<(Completion, LogicalLogTruncateOutcome)> {
        self.inner.truncate(checkpointed_through_ts)
    }
    fn reset_to_fresh_header(&self) -> crate::Result<Completion> {
        self.inner.reset_to_fresh_header()
    }
    fn get_logical_log_file(&self) -> Arc<dyn crate::File> {
        self.inner.get_logical_log_file()
    }
    fn logical_log_offset(&self) -> u64 {
        self.inner.logical_log_offset()
    }
    fn should_checkpoint(&self) -> bool {
        self.inner.should_checkpoint()
    }
    fn set_checkpoint_threshold(&self, t: i64) {
        self.inner.set_checkpoint_threshold(t)
    }
    fn checkpoint_threshold(&self) -> i64 {
        self.inner.checkpoint_threshold()
    }
    fn advance_logical_log_offset_after_success(&self, b: u64) -> crate::Result<()> {
        self.inner.advance_logical_log_offset_after_success(b)
    }
    fn discard_pending_log_write(&self) -> crate::Result<()> {
        self.inner.discard_pending_log_write()
    }
    fn restore_logical_log_state_after_recovery(&self, o: u64, c: u32) {
        self.inner.restore_logical_log_state_after_recovery(o, c)
    }
    fn set_header(&self, h: LogHeader) {
        self.inner.set_header(h)
    }
    fn on_checkpoint_start(&self) -> crate::Result<()> {
        self.inner.on_checkpoint_start()
    }
    fn on_checkpoint_end(&self, r: crate::Result<&crate::CheckpointResult>) -> crate::Result<()> {
        self.inner.on_checkpoint_end(r)
    }
    fn encryption_ctx(&self) -> Option<EncryptionContext> {
        self.inner.encryption_ctx()
    }
}

fn open_mvcc_file(path: &str) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap()
}

fn open_with_drop_statement_storage(
    path: &str,
    call: StorageCall,
) -> (Arc<Database>, Arc<DropStatementStorage>) {
    let first_open = {
        let db = open_mvcc_file(path);
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.close().unwrap();
        Arc::downgrade(&db)
    };
    assert!(first_open.upgrade().is_none());
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let log_path = format!("{path}-log");
    let log_file = io
        .open_file(&log_path, OpenFlags::default(), false)
        .unwrap();
    let storage = Arc::new(DropStatementStorage {
        inner: Arc::new(Storage::new(log_file, io.clone(), None)),
        call,
        countdown: Mutex::new(0),
    });
    let db = Database::open(
        io,
        path,
        crate::OpenOptions::new(Arc::new(SqliteDialect))
            .durable_storage(storage.clone() as Arc<dyn DurableStorage>),
    )
    .unwrap();
    (db, storage)
}

fn rows_after_restart(path: &str, closed: Weak<Database>) -> Vec<Vec<Value>> {
    assert!(
        closed.upgrade().is_none(),
        "the database must be closed before it is opened again"
    );
    let db = open_mvcc_file(path);
    let conn = db.connect().unwrap();
    get_rows(&conn, "SELECT pk FROM t ORDER BY pk")
}

fn waiter_dropped_while_leader_writes_it(call: StorageCall) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir.path().join("test.db");
    let path = path.to_str().unwrap();
    let (db, storage) = open_with_drop_statement_storage(path, call);
    let setup = db.connect().unwrap();
    setup
        .execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    setup
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    setup.execute("PRAGMA mvcc_group_commit = yes").unwrap();

    let conn_a = db.connect().unwrap();
    let conn_b = db.connect().unwrap();
    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_b.execute("BEGIN CONCURRENT").unwrap();
    conn_a.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    conn_b.execute("INSERT INTO t VALUES (2, 1)").unwrap();

    let store = db.get_mv_store().clone().unwrap();
    let coordinator = &store.commit_coordinator;
    assert!(coordinator.pager_commit_lock.write());
    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    let mut commit_b = conn_b.prepare("COMMIT").unwrap();
    assert!(matches!(commit_a.step().unwrap(), StepResult::IO));
    assert!(matches!(commit_b.step().unwrap(), StepResult::IO));
    DROPPED_BY_STORAGE.with(|slot| *slot.borrow_mut() = Some(commit_b));
    storage.drop_statement_on_call(2);
    coordinator.unlock_pager_commit_lock();
    step_until_done(&mut commit_a);
    assert!(
        DROPPED_BY_STORAGE.with(|slot| slot.borrow().is_none()),
        "the waiter must be dropped while the leader writes its record"
    );

    let before_restart = get_rows(&setup, "SELECT pk FROM t ORDER BY pk");
    let closed = Arc::downgrade(&db);
    drop(commit_a);
    drop((setup, conn_a, conn_b, store, db, storage));
    assert_eq!(
        rows_after_restart(path, closed),
        before_restart,
        "recovery must not change which commits happened"
    );
}

#[test]
fn waiter_dropped_while_its_log_write_is_issued_has_the_same_rows_after_restart() {
    waiter_dropped_while_leader_writes_it(StorageCall::LogTx);
}

#[test]
fn waiter_dropped_before_its_log_write_is_issued_has_the_same_rows_after_restart() {
    waiter_dropped_while_leader_writes_it(StorageCall::UpgradeHeader);
}

#[test]
fn abandoned_waiter_that_the_leader_commits_releases_its_dependents() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    setup.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    setup.close().unwrap();

    let conn_a = db.connect();
    let conn_b = db.connect();
    let reader = db.connect();
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
    ])));

    let store = db.get_mvcc_store();
    let coordinator = &store.commit_coordinator;
    assert!(coordinator.pager_commit_lock.write());
    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    let mut commit_b = conn_b.prepare("COMMIT").unwrap();
    assert!(matches!(
        step_until_yield_or_done(&mut commit_a),
        StepResult::Yield
    ));
    assert!(matches!(
        step_until_yield_or_done(&mut commit_b),
        StepResult::Yield
    ));
    assert!(matches!(commit_a.step().unwrap(), StepResult::IO));
    assert!(matches!(commit_b.step().unwrap(), StepResult::IO));

    reader.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        get_rows(&reader, "SELECT pk FROM t WHERE pk = 2"),
        vec![vec![Value::from_i64(2)]],
        "the reader reads the preparing waiter and depends on it"
    );

    coordinator.unlock_pager_commit_lock();
    assert!(matches!(
        step_until_yield_or_done(&mut commit_a),
        StepResult::Yield
    ));
    drop(commit_b);
    conn_a.set_yield_injector(None);
    step_until_done(&mut commit_a);

    let mut commit_reader = reader.prepare("COMMIT").unwrap();
    step_until_done(&mut commit_reader);
}

#[test]
fn dropped_leader_does_not_leave_its_own_record_in_the_queue() {
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
    let tx_a = conn_a.get_mv_tx_id().unwrap();
    conn_a.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogicalLogWriteIssued.point(),
    ])));

    let store = db.get_mvcc_store();
    let coordinator = &store.commit_coordinator;
    assert!(coordinator.pager_commit_lock.write());
    let mut commit_b = conn_b.prepare("COMMIT").unwrap();
    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    assert!(matches!(commit_b.step().unwrap(), StepResult::IO));
    assert!(matches!(commit_a.step().unwrap(), StepResult::IO));
    coordinator.unlock_pager_commit_lock();
    assert!(
        matches!(step_until_yield_or_done(&mut commit_a), StepResult::Yield),
        "the leader yields after it issued the write of the waiter"
    );
    drop(commit_a);

    let queued = coordinator.take_pending();
    let queued_txs = queued.iter().map(|q| q.tx_id).collect::<Vec<_>>();
    coordinator.requeue(queued.into_iter());
    assert!(
        !queued_txs.contains(&tx_a),
        "the rolled-back leader still has a record in the queue: {queued_txs:?}"
    );

    step_until_done(&mut commit_b);
    assert_eq!(
        get_rows(&conn_b, "SELECT pk FROM t ORDER BY pk"),
        vec![vec![Value::from_i64(2)]]
    );
}
