use super::get_rows;
use crate::io::{FileSyncType, PlatformIO};
use crate::mvcc::database::{CommitCoordinator, LogRecord, RowVersion, TransactionState};
use crate::mvcc::persistent_storage::logical_log::{LogHeader, OnSerializationComplete};
use crate::mvcc::persistent_storage::{DurableStorage, LogicalLogTruncateOutcome, Storage};
use crate::storage::encryption::EncryptionContext;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::wal::CheckpointMode;
use crate::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crate::sync::Mutex;
use crate::{
    CheckpointResult, Completion, CompletionError, Connection, Database, DatabaseOpts, File,
    LimboError, MvStore, OpenFlags, OpenOptions, Result, SqliteDialect, StepResult, IO,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[test]
fn commit_publishes_only_after_on_log_write_complete_finishes_with_group_commit() {
    commit_publishes_only_after_on_log_write_complete_finishes(true);
}

#[test]
fn commit_publishes_only_after_on_log_write_complete_finishes_without_group_commit() {
    commit_publishes_only_after_on_log_write_complete_finishes(false);
}

/// `on_log_write_complete` may return a completion for extra durability work.
/// Until that completion finishes the commit must stay parked: the logical-log
/// offset must not advance, and the transaction must stay in `Preparing`.
///
/// Readers are not used while the writer is `Preparing`: a reader that sees
/// the writer's row takes a commit dependency on it and would wait for the
/// held completion at its own commit.
fn commit_publishes_only_after_on_log_write_complete_finishes(group_commit: bool) {
    let fixture = HookDb::new(group_commit);
    let storage = fixture.storage.clone();
    let store = fixture.mv_store();
    let writer = fixture.connect();
    let reader = fixture.connect();
    let offset_before = storage.logical_log_offset();

    writer.execute("BEGIN CONCURRENT").unwrap();
    writer.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let tx_id = writer.get_mv_tx_id().unwrap();
    storage.hold_log_write_complete_after(0);
    let mut commit = writer.prepare("COMMIT").unwrap();
    let held = step_until_hook_completion_is_held(&mut commit, &storage, 0);

    for _ in 0..10 {
        assert!(
            matches!(commit.step().unwrap(), StepResult::IO),
            "COMMIT must stay parked until the on_log_write_complete completion finishes"
        );
    }
    assert_eq!(
        storage.calls(),
        vec![StorageCall::LogTx, StorageCall::LogWriteComplete]
    );
    assert_eq!(
        storage.logical_log_offset(),
        offset_before,
        "the logical-log offset must not advance while on_log_write_complete is pending"
    );
    assert!(
        matches!(
            store.txs.get(&tx_id).unwrap().value().state.load(),
            TransactionState::Preparing(_)
        ),
        "the transaction must not be committed while on_log_write_complete is pending"
    );

    held.complete(0);
    step_until_done(&mut commit);

    assert_eq!(
        storage.calls(),
        vec![
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
            StorageCall::AdvanceOffset,
            StorageCall::Sync,
        ]
    );
    assert!(storage.logical_log_offset() > offset_before);
    assert!(writer.get_mv_tx_id().is_none());
    assert_eq!(ids(&reader), vec![1]);
    storage.assert_no_violations();
}

#[test]
fn dropped_commit_waiting_for_on_log_write_complete_rolls_back_with_group_commit() {
    dropped_commit_waiting_for_on_log_write_complete_rolls_back(true);
}

#[test]
fn dropped_commit_waiting_for_on_log_write_complete_rolls_back_without_group_commit() {
    dropped_commit_waiting_for_on_log_write_complete_rolls_back(false);
}

/// Dropping a COMMIT while its `on_log_write_complete` completion is pending
/// must discard the unpublished write, roll the transaction back, and leave
/// the commit lock free for the next writer. Recovery must not replay the
/// discarded record once a later commit has reused its offset.
fn dropped_commit_waiting_for_on_log_write_complete_rolls_back(group_commit: bool) {
    let mut fixture = HookDb::new(group_commit);
    let storage = fixture.storage.clone();
    let writer = fixture.connect();
    let reader = fixture.connect();
    let offset_before = storage.logical_log_offset();

    writer.execute("BEGIN CONCURRENT").unwrap();
    writer.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    storage.hold_log_write_complete_after(0);
    let mut commit = writer.prepare("COMMIT").unwrap();
    let held = step_until_hook_completion_is_held(&mut commit, &storage, 0);
    drop(commit);

    assert!(
        writer.get_mv_tx_id().is_none(),
        "the dropped commit must end its transaction"
    );
    assert_eq!(
        storage.calls(),
        vec![
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
            StorageCall::DiscardPendingWrite,
        ]
    );
    assert_eq!(storage.logical_log_offset(), offset_before);
    assert_eq!(ids(&reader), Vec::<i64>::new());

    held.complete(0);
    assert_eq!(
        ids(&reader),
        Vec::<i64>::new(),
        "finishing the hook after the drop must not resurrect the write"
    );

    storage.stop_holding_log_write_complete();
    storage.clear_calls();
    writer.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    assert_eq!(
        storage.calls(),
        vec![
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
            StorageCall::AdvanceOffset,
            StorageCall::Sync,
        ]
    );
    assert_eq!(ids(&reader), vec![2]);
    storage.assert_no_violations();

    writer.close().unwrap();
    reader.close().unwrap();
    drop(writer);
    drop(reader);
    fixture.restart();
    let conn = fixture.connect();
    assert_eq!(
        ids(&conn),
        vec![2],
        "recovery must not replay the discarded record"
    );
}

/// A record discarded before publication was rolled back in this process, so
/// recovery must not bring it back even when no later commit overwrote it.
///
/// Fails today: `discard_pending_log_write` only forgets the staged CRC. The
/// frame bytes stay in the log file past the writer offset, and recovery
/// replays every frame with a valid CRC chain up to the end of the file.
#[test]
#[ignore = "the discarded frame stays in the log file and recovery replays it"]
fn discarded_record_is_not_replayed_by_recovery() {
    let mut fixture = HookDb::new(false);
    let storage = fixture.storage.clone();
    let writer = fixture.connect();

    writer.execute("BEGIN CONCURRENT").unwrap();
    writer.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    storage.hold_log_write_complete_after(0);
    let mut commit = writer.prepare("COMMIT").unwrap();
    let held = step_until_hook_completion_is_held(&mut commit, &storage, 0);
    drop(commit);
    held.complete(0);
    assert_eq!(ids(&writer), Vec::<i64>::new());
    storage.assert_no_violations();

    writer.close().unwrap();
    drop(writer);
    fixture.restart();
    let conn = fixture.connect();
    assert_eq!(
        ids(&conn),
        Vec::<i64>::new(),
        "a record discarded before publication must not come back after restart"
    );
}

/// A group-commit leader writes one record per transaction in the batch. Each
/// record gets its own `on_log_write_complete` call, and the leader must wait
/// for that call's completion before owning the record and moving to the next
/// one. The batch is covered by a single fsync after the last record.
#[test]
fn group_leader_waits_for_on_log_write_complete_of_every_record() {
    let fixture = HookDb::new(true);
    let storage = fixture.storage.clone();
    let store = fixture.mv_store();
    let coordinator = &store.commit_coordinator;
    let conn_a = fixture.connect();
    let conn_b = fixture.connect();
    let reader = fixture.connect();

    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_b.execute("BEGIN CONCURRENT").unwrap();
    conn_a.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn_b.execute("INSERT INTO t VALUES (2, 'b')").unwrap();

    assert!(
        coordinator.pager_commit_lock.write(),
        "hold the commit lock so both commits enqueue behind it"
    );
    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    let mut commit_b = conn_b.prepare("COMMIT").unwrap();
    step_until_parked(&mut commit_a, coordinator, 1);
    step_until_parked(&mut commit_b, coordinator, 2);

    storage.hold_log_write_complete_after(1);
    coordinator.unlock_pager_commit_lock();
    let held = step_until_hook_completion_is_held(&mut commit_a, &storage, 0);

    assert!(
        store.last_group_commit_size() >= 2,
        "both commits must be in one batch"
    );
    assert_eq!(
        storage.calls(),
        vec![
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
            StorageCall::AdvanceOffset,
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
        ]
    );
    assert!(
        !matches!(commit_b.step().unwrap(), StepResult::Done),
        "the waiter must not finish before its record is published"
    );

    held.complete(0);
    step_until_done(&mut commit_a);
    step_until_done(&mut commit_b);

    assert_eq!(
        storage.calls(),
        vec![
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
            StorageCall::AdvanceOffset,
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
            StorageCall::AdvanceOffset,
            StorageCall::Sync,
        ]
    );
    assert_eq!(ids(&reader), vec![1, 2]);
    storage.assert_no_violations();
}

/// Dropping a group-commit leader while it waits for the `on_log_write_complete`
/// completion of a waiter's record must keep the leader's own, already owned,
/// record committed, discard the waiter's unpublished record, and let the
/// waiter write its record again on its own.
#[test]
fn dropped_group_leader_waiting_for_a_waiters_on_log_write_complete_lets_the_waiter_retry() {
    let mut fixture = HookDb::new(true);
    let storage = fixture.storage.clone();
    let store = fixture.mv_store();
    let coordinator = &store.commit_coordinator;
    let conn_a = fixture.connect();
    let conn_b = fixture.connect();

    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_b.execute("BEGIN CONCURRENT").unwrap();
    conn_a.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    conn_b.execute("INSERT INTO t VALUES (2, 'b')").unwrap();

    assert!(
        coordinator.pager_commit_lock.write(),
        "hold the commit lock so both commits enqueue behind it"
    );
    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    let mut commit_b = conn_b.prepare("COMMIT").unwrap();
    step_until_parked(&mut commit_a, coordinator, 1);
    step_until_parked(&mut commit_b, coordinator, 2);

    storage.hold_log_write_complete_after(1);
    coordinator.unlock_pager_commit_lock();
    let held = step_until_hook_completion_is_held(&mut commit_a, &storage, 0);
    assert!(
        store.last_group_commit_size() >= 2,
        "both commits must be in one batch"
    );
    drop(commit_a);

    assert!(
        conn_a.get_mv_tx_id().is_none(),
        "the leader owned its own record, so dropping it must finish its transaction"
    );
    assert_eq!(
        storage.calls(),
        vec![
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
            StorageCall::AdvanceOffset,
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
            StorageCall::DiscardPendingWrite,
        ]
    );

    storage.stop_holding_log_write_complete();
    held.complete(0);
    step_until_done(&mut commit_b);

    assert_eq!(
        storage.calls(),
        vec![
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
            StorageCall::AdvanceOffset,
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
            StorageCall::DiscardPendingWrite,
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
            StorageCall::AdvanceOffset,
            StorageCall::Sync,
        ],
        "the waiter must write its record again after the leader discarded it"
    );
    let reader = fixture.connect();
    assert_eq!(ids(&reader), vec![1, 2]);
    storage.assert_no_violations();

    conn_a.close().unwrap();
    conn_b.close().unwrap();
    reader.close().unwrap();
    drop(commit_b);
    drop(conn_a);
    drop(conn_b);
    drop(reader);
    drop(store);
    fixture.restart();
    let conn = fixture.connect();
    assert_eq!(
        ids(&conn),
        vec![1, 2],
        "recovery must replay each commit exactly once"
    );
}

/// A failed `on_log_write_complete` completion fails the COMMIT: the write is
/// discarded, the transaction rolls back, and the next writer is not blocked.
#[test]
fn failed_on_log_write_complete_completion_fails_the_commit() {
    let fixture = HookDb::new(false);
    let storage = fixture.storage.clone();
    let writer = fixture.connect();
    let offset_before = storage.logical_log_offset();

    writer.execute("BEGIN CONCURRENT").unwrap();
    writer.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    storage.hold_log_write_complete_after(0);
    let mut commit = writer.prepare("COMMIT").unwrap();
    let held = step_until_hook_completion_is_held(&mut commit, &storage, 0);

    held.error(CompletionError::IOError(
        std::io::ErrorKind::Other,
        "remote log upload failed",
    ));
    let err = step_until_error(&mut commit);
    drop(commit);

    assert!(
        writer.get_mv_tx_id().is_none(),
        "COMMIT failed with {err}, so its transaction must be gone"
    );
    assert_eq!(
        storage.calls(),
        vec![
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
            StorageCall::DiscardPendingWrite,
        ]
    );
    assert_eq!(storage.logical_log_offset(), offset_before);
    assert_eq!(ids(&writer), Vec::<i64>::new());

    storage.stop_holding_log_write_complete();
    storage.clear_calls();
    writer.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    assert_eq!(
        storage.calls(),
        vec![
            StorageCall::LogTx,
            StorageCall::LogWriteComplete,
            StorageCall::AdvanceOffset,
            StorageCall::Sync,
        ]
    );
    assert_eq!(ids(&writer), vec![2]);
    storage.assert_no_violations();
}

/// A checkpoint calls `on_checkpoint_start` exactly once before it touches the
/// log, truncates the log exactly once, and calls `on_checkpoint_end` exactly
/// once, after the truncation, with the checkpoint result.
#[test]
fn checkpoint_hooks_surround_the_log_truncation() {
    let fixture = HookDb::new(false);
    let storage = fixture.storage.clone();
    let conn = fixture.connect();
    conn.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    storage.clear_calls();

    conn.checkpoint(CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    })
    .unwrap();

    let calls = storage.calls();
    let position = |call: StorageCall| calls.iter().position(|c| *c == call);
    let count = |call: StorageCall| calls.iter().filter(|c| **c == call).count();
    assert_eq!(
        calls.first(),
        Some(&StorageCall::CheckpointStart),
        "{calls:?}"
    );
    assert_eq!(
        calls.last(),
        Some(&StorageCall::CheckpointEnd { ok: true }),
        "{calls:?}"
    );
    assert_eq!(count(StorageCall::CheckpointStart), 1, "{calls:?}");
    assert_eq!(count(StorageCall::Truncate), 1, "{calls:?}");
    assert_eq!(
        count(StorageCall::CheckpointEnd { ok: true }),
        1,
        "{calls:?}"
    );
    assert!(
        position(StorageCall::CheckpointStart) < position(StorageCall::Truncate),
        "{calls:?}"
    );
    assert!(
        position(StorageCall::Truncate) < position(StorageCall::CheckpointEnd { ok: true }),
        "{calls:?}"
    );
    assert_eq!(ids(&conn), vec![1]);
    storage.assert_no_violations();
}

fn ids(conn: &Arc<Connection>) -> Vec<i64> {
    get_rows(conn, "SELECT id FROM t ORDER BY id")
        .iter()
        .map(|row| row[0].as_int().unwrap())
        .collect()
}

fn step_until_hook_completion_is_held(
    commit: &mut crate::Statement,
    storage: &RecordingStorage,
    index: usize,
) -> Completion {
    for _ in 0..10_000 {
        match commit.step().unwrap() {
            StepResult::IO | StepResult::Yield => {}
            other => panic!("COMMIT ended with {other:?} before on_log_write_complete was held"),
        }
        if let Some(completion) = storage.held_completions().get(index) {
            return completion.clone();
        }
    }
    panic!("COMMIT never reached on_log_write_complete");
}

fn step_until_parked(stmt: &mut crate::Statement, coordinator: &CommitCoordinator, parked: usize) {
    for _ in 0..10_000 {
        match stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield => {}
            other => panic!("COMMIT ended with {other:?} instead of parking on the commit lock"),
        }
        if coordinator.parked_tickets().len() == parked {
            return;
        }
    }
    panic!("COMMIT never parked on the held commit lock");
}

fn step_until_done(stmt: &mut crate::Statement) {
    for _ in 0..10_000 {
        match stmt.step().unwrap() {
            StepResult::Done => return,
            StepResult::IO | StepResult::Yield => {}
            other => panic!("statement ended with {other:?}"),
        }
    }
    panic!("statement never finished");
}

fn step_until_error(stmt: &mut crate::Statement) -> LimboError {
    for _ in 0..10_000 {
        match stmt.step() {
            Ok(StepResult::IO | StepResult::Yield) => {}
            Ok(other) => panic!("statement ended with {other:?} instead of an error"),
            Err(err) => return err,
        }
    }
    panic!("statement never failed");
}

struct HookDb {
    _dir: tempfile::TempDir,
    path: PathBuf,
    db: Option<Arc<Database>>,
    storage: Arc<RecordingStorage>,
}

impl HookDb {
    fn new(group_commit: bool) -> Self {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir
            .path()
            .join(format!("hooks_{}.db", rand::random::<u64>()));
        create_mvcc_db(&path);
        let (db, storage) = open_with_recording_storage(&path);
        let conn = db.connect().unwrap();
        conn.execute(format!(
            "PRAGMA mvcc_group_commit = {}",
            if group_commit { "on" } else { "off" }
        ))
        .unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.close().unwrap();
        storage.clear_calls();
        Self {
            _dir: dir,
            path,
            db: Some(db),
            storage,
        }
    }

    fn connect(&self) -> Arc<Connection> {
        self.db.as_ref().unwrap().connect().unwrap()
    }

    fn mv_store(&self) -> Arc<MvStore> {
        self.db.as_ref().unwrap().get_mv_store().clone().unwrap()
    }

    fn restart(&mut self) {
        let previous = Arc::downgrade(self.db.as_ref().unwrap());
        self.db = None;
        assert!(
            previous.upgrade().is_none(),
            "close every connection before restarting, otherwise the registry hands back the same database"
        );
        let (db, storage) = open_with_recording_storage(&self.path);
        self.db = Some(db);
        self.storage = storage;
    }
}

fn create_mvcc_db(path: &Path) {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.close().unwrap();
}

fn open_with_recording_storage(path: &Path) -> (Arc<Database>, Arc<RecordingStorage>) {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let log_path = path.with_extension("db-log");
    let log_file = io
        .open_file(log_path.to_str().unwrap(), OpenFlags::default(), false)
        .unwrap();
    let inner: Arc<dyn DurableStorage> = Arc::new(Storage::new(log_file, io.clone(), None));
    let storage = RecordingStorage::new(inner);
    let db = Database::open(
        io,
        path.to_str().unwrap(),
        OpenOptions::new(Arc::new(SqliteDialect))
            .durable_storage(storage.clone() as Arc<dyn DurableStorage>),
    )
    .unwrap();
    db.get_mv_store()
        .as_ref()
        .unwrap()
        .set_checkpoint_threshold(-1);
    (db, storage)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageCall {
    LogTx,
    LogWriteComplete,
    AdvanceOffset,
    DiscardPendingWrite,
    Sync,
    CheckpointStart,
    Truncate,
    CheckpointEnd { ok: bool },
}

/// Where the current logical-log write is in its life cycle. Every record must
/// go `Idle -> Written -> Acknowledged -> Idle`, or back to `Idle` through
/// `discard_pending_log_write`.
#[derive(Debug)]
enum LogWriteStage {
    Idle,
    Written(Completion),
    Acknowledged(Completion),
}

const NEVER_HOLD: usize = usize::MAX;

/// Wraps the real logical-log storage, records which trait methods the engine
/// calls, checks the ordering rules between them, and can hold the completion
/// returned from `on_log_write_complete` open until the test finishes it.
#[derive(Debug)]
struct RecordingStorage {
    inner: Arc<dyn DurableStorage>,
    calls: Mutex<Vec<StorageCall>>,
    violations: Mutex<Vec<String>>,
    write_stage: Mutex<LogWriteStage>,
    hook_calls_before_holding: AtomicUsize,
    held: Mutex<Vec<Completion>>,
    checkpoint_open: AtomicBool,
}

impl RecordingStorage {
    fn new(inner: Arc<dyn DurableStorage>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            calls: Mutex::new(Vec::new()),
            violations: Mutex::new(Vec::new()),
            write_stage: Mutex::new(LogWriteStage::Idle),
            hook_calls_before_holding: AtomicUsize::new(NEVER_HOLD),
            held: Mutex::new(Vec::new()),
            checkpoint_open: AtomicBool::new(false),
        })
    }

    /// Let `calls_to_pass` more `on_log_write_complete` calls return the real
    /// completion, then hold every later one open until the test completes it.
    fn hold_log_write_complete_after(&self, calls_to_pass: usize) {
        self.hook_calls_before_holding
            .store(calls_to_pass, Ordering::Release);
    }

    fn stop_holding_log_write_complete(&self) {
        self.hook_calls_before_holding
            .store(NEVER_HOLD, Ordering::Release);
    }

    fn held_completions(&self) -> Vec<Completion> {
        self.held.lock().clone()
    }

    fn calls(&self) -> Vec<StorageCall> {
        self.calls.lock().clone()
    }

    fn clear_calls(&self) {
        self.calls.lock().clear();
    }

    fn assert_no_violations(&self) {
        let violations = self.violations.lock();
        assert!(
            violations.is_empty(),
            "storage hook invariants violated: {violations:#?}"
        );
    }

    fn record(&self, call: StorageCall) {
        self.calls.lock().push(call);
    }

    fn violation(&self, message: impl Into<String>) {
        self.violations.lock().push(message.into());
    }

    fn next_log_write_complete_completion(&self) -> Result<Completion> {
        let calls_to_pass = self.hook_calls_before_holding.load(Ordering::Acquire);
        if calls_to_pass == NEVER_HOLD {
            return self.inner.on_log_write_complete();
        }
        if calls_to_pass > 0 {
            self.hook_calls_before_holding
                .store(calls_to_pass - 1, Ordering::Release);
            return self.inner.on_log_write_complete();
        }
        let completion = Completion::new_wait();
        self.held.lock().push(completion.clone());
        Ok(completion)
    }
}

impl DurableStorage for RecordingStorage {
    fn serialize_row_version(
        &self,
        log_record: &mut LogRecord,
        row_version: &RowVersion,
        portable_extension: Option<&[u8]>,
    ) -> Result<()> {
        self.inner
            .serialize_row_version(log_record, row_version, portable_extension)
    }

    fn serialize_database_header(
        &self,
        log_record: &mut LogRecord,
        header: &DatabaseHeader,
    ) -> Result<()> {
        self.inner.serialize_database_header(log_record, header)
    }

    fn log_tx(
        &self,
        m: LogRecord,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> Result<(Completion, u64)> {
        self.record(StorageCall::LogTx);
        let (completion, bytes) = self.inner.log_tx(m, on_serialization_complete)?;
        let mut stage = self.write_stage.lock();
        if !matches!(*stage, LogWriteStage::Idle) {
            self.violation(format!(
                "log_tx issued while the previous write was still {stage:?}"
            ));
        }
        *stage = LogWriteStage::Written(completion.clone());
        Ok((completion, bytes))
    }

    fn upgrade_header_for_log_tx(&self, m: &LogRecord) -> Result<Option<Completion>> {
        self.inner.upgrade_header_for_log_tx(m)
    }

    fn sync(&self, sync_type: FileSyncType) -> Result<Completion> {
        self.record(StorageCall::Sync);
        self.inner.sync(sync_type)
    }

    fn on_log_write_complete(&self) -> Result<Completion> {
        self.record(StorageCall::LogWriteComplete);
        let mut stage = self.write_stage.lock();
        match &*stage {
            LogWriteStage::Written(write) if write.succeeded() => {}
            LogWriteStage::Written(_) => {
                self.violation("on_log_write_complete called before the log_tx write finished")
            }
            other => self.violation(format!(
                "on_log_write_complete called while the write was {other:?}"
            )),
        }
        let completion = self.next_log_write_complete_completion()?;
        *stage = LogWriteStage::Acknowledged(completion.clone());
        Ok(completion)
    }

    fn update_header(&self) -> Result<Completion> {
        self.inner.update_header()
    }

    fn truncate(
        &self,
        checkpointed_through_ts: u64,
    ) -> Result<(Completion, LogicalLogTruncateOutcome)> {
        self.record(StorageCall::Truncate);
        if !self.checkpoint_open.load(Ordering::Acquire) {
            self.violation("truncate called outside of on_checkpoint_start/on_checkpoint_end");
        }
        self.inner.truncate(checkpointed_through_ts)
    }

    fn reset_to_fresh_header(&self) -> Result<Completion> {
        self.inner.reset_to_fresh_header()
    }

    fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.inner.get_logical_log_file()
    }

    fn logical_log_offset(&self) -> u64 {
        self.inner.logical_log_offset()
    }

    fn should_checkpoint(&self) -> bool {
        self.inner.should_checkpoint()
    }

    fn set_checkpoint_threshold(&self, threshold: i64) {
        self.inner.set_checkpoint_threshold(threshold)
    }

    fn checkpoint_threshold(&self) -> i64 {
        self.inner.checkpoint_threshold()
    }

    fn advance_logical_log_offset_after_success(&self, bytes: u64) -> Result<()> {
        self.record(StorageCall::AdvanceOffset);
        let mut stage = self.write_stage.lock();
        match &*stage {
            LogWriteStage::Acknowledged(hook) if hook.finished() => {}
            LogWriteStage::Acknowledged(_) => self.violation(
                "logical-log offset advanced while the on_log_write_complete completion was still pending",
            ),
            other => self.violation(format!(
                "logical-log offset advanced while the write was {other:?}"
            )),
        }
        *stage = LogWriteStage::Idle;
        self.inner.advance_logical_log_offset_after_success(bytes)
    }

    fn discard_pending_log_write(&self) -> Result<()> {
        self.record(StorageCall::DiscardPendingWrite);
        let mut stage = self.write_stage.lock();
        if matches!(*stage, LogWriteStage::Idle) {
            self.violation("discard_pending_log_write called with no write in flight");
        }
        *stage = LogWriteStage::Idle;
        self.inner.discard_pending_log_write()
    }

    fn restore_logical_log_state_after_recovery(&self, offset: u64, running_crc: u32) {
        self.inner
            .restore_logical_log_state_after_recovery(offset, running_crc)
    }

    fn set_header(&self, header: LogHeader) {
        self.inner.set_header(header)
    }

    fn on_checkpoint_start(&self) -> Result<()> {
        self.record(StorageCall::CheckpointStart);
        if self.checkpoint_open.swap(true, Ordering::AcqRel) {
            self.violation("on_checkpoint_start called while a checkpoint was already open");
        }
        self.inner.on_checkpoint_start()
    }

    fn on_checkpoint_end(&self, result: Result<&CheckpointResult>) -> Result<()> {
        self.record(StorageCall::CheckpointEnd { ok: result.is_ok() });
        if !self.checkpoint_open.swap(false, Ordering::AcqRel) {
            self.violation("on_checkpoint_end called without a matching on_checkpoint_start");
        }
        self.inner.on_checkpoint_end(result)
    }

    fn encryption_ctx(&self) -> Option<EncryptionContext> {
        self.inner.encryption_ctx()
    }
}
