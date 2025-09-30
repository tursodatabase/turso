use crate::mvcc::clock::LogicalClock;
use crate::mvcc::persistent_storage::Storage;
use crate::state_machine::StateMachine;
use crate::state_machine::StateTransition;
use crate::state_machine::TransitionResult;
use crate::storage::btree::BTreeCursor;
use crate::storage::btree::BTreeKey;
use crate::storage::btree::CursorValidState;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::wal::TursoRwLock;
use crate::turso_assert;
use crate::types::IOCompletions;
use crate::types::IOResult;
use crate::types::ImmutableRecord;
use crate::types::SeekResult;
use crate::Completion;
use crate::File;
use crate::IOExt;
use crate::LimboError;
use crate::Result;
use crate::{Connection, Pager};
use crossbeam_skiplist::{SkipMap, SkipSet};
use parking_lot::RwLock;
use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::instrument;
use tracing::Level;

pub mod checkpoint_state_machine;
pub use checkpoint_state_machine::{CheckpointState, CheckpointStateMachine};

use super::persistent_storage::logical_log::StreamingLogicalLogReader;
use super::persistent_storage::logical_log::StreamingResult;

#[cfg(test)]
pub mod tests;

/// Sentinel value for `MvStore::exclusive_tx` indicating no exclusive transaction is active.
const NO_EXCLUSIVE_TX: u64 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowID {
    /// The table ID. Analogous to table's root page number.
    pub table_id: i64,
    pub row_id: i64,
}

impl RowID {
    pub fn new(table_id: i64, row_id: i64) -> Self {
        Self { table_id, row_id }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]

pub struct Row {
    pub id: RowID,
    pub data: Vec<u8>,
    pub column_count: usize,
}

impl Row {
    pub fn new(id: RowID, data: Vec<u8>, column_count: usize) -> Self {
        Self {
            id,
            data,
            column_count,
        }
    }
}

/// A row version.
#[derive(Clone, Debug, PartialEq)]
pub struct RowVersion {
    pub begin: TxTimestampOrID,
    pub end: Option<TxTimestampOrID>,
    pub row: Row,
}

pub type TxID = u64;

/// A log record contains all the versions inserted and deleted by a transaction.
#[derive(Clone, Debug)]
pub struct LogRecord {
    pub(crate) tx_timestamp: TxID,
    pub row_versions: Vec<RowVersion>,
}

impl LogRecord {
    fn new(tx_timestamp: TxID) -> Self {
        Self {
            tx_timestamp,
            row_versions: Vec::new(),
        }
    }
}

/// A transaction timestamp or ID.
///
/// Versions either track a timestamp or a transaction ID, depending on the
/// phase of the transaction. During the active phase, new versions track the
/// transaction ID in the `begin` and `end` fields. After a transaction commits,
/// versions switch to tracking timestamps.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum TxTimestampOrID {
    /// A committed transaction's timestamp.
    Timestamp(u64),
    /// The ID of a non-committed transaction.
    TxID(TxID),
}

/// Transaction
#[derive(Debug)]
pub struct Transaction {
    /// The state of the transaction.
    state: AtomicTransactionState,
    /// The transaction ID.
    tx_id: u64,
    /// The transaction begin timestamp.
    begin_ts: u64,
    /// The transaction write set.
    write_set: SkipSet<RowID>,
    /// The transaction read set.
    read_set: SkipSet<RowID>,
    /// The transaction header.
    header: RwLock<DatabaseHeader>,
}

impl Transaction {
    fn new(tx_id: u64, begin_ts: u64, header: DatabaseHeader) -> Transaction {
        Transaction {
            state: TransactionState::Active.into(),
            tx_id,
            begin_ts,
            write_set: SkipSet::new(),
            read_set: SkipSet::new(),
            header: RwLock::new(header),
        }
    }

    fn insert_to_read_set(&self, id: RowID) {
        self.read_set.insert(id);
    }

    fn insert_to_write_set(&self, id: RowID) {
        self.write_set.insert(id);
    }
}

impl std::fmt::Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "{{ state: {}, id: {}, begin_ts: {}, write_set: [",
            self.state.load(),
            self.tx_id,
            self.begin_ts,
        )?;

        for (i, v) in self.write_set.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?
            }
            write!(f, "{:?}", *v.value())?;
        }

        write!(f, "], read_set: [")?;
        for (i, v) in self.read_set.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{:?}", *v.value())?;
        }

        write!(f, "] }}")
    }
}

/// Transaction state.
#[derive(Debug, Clone, PartialEq)]
enum TransactionState {
    Active,
    Preparing,
    Aborted,
    Terminated,
    Committed(u64),
}

impl TransactionState {
    pub fn encode(&self) -> u64 {
        match self {
            TransactionState::Active => 0,
            TransactionState::Preparing => 1,
            TransactionState::Aborted => 2,
            TransactionState::Terminated => 3,
            TransactionState::Committed(ts) => {
                // We only support 2*62 - 1 timestamps, because the extra bit
                // is used to encode the type.
                assert!(ts & 0x8000_0000_0000_0000 == 0);
                0x8000_0000_0000_0000 | ts
            }
        }
    }

    pub fn decode(v: u64) -> Self {
        match v {
            0 => TransactionState::Active,
            1 => TransactionState::Preparing,
            2 => TransactionState::Aborted,
            3 => TransactionState::Terminated,
            v if v & 0x8000_0000_0000_0000 != 0 => {
                TransactionState::Committed(v & 0x7fff_ffff_ffff_ffff)
            }
            _ => panic!("Invalid transaction state"),
        }
    }
}

// Transaction state encoded into a single 64-bit atomic.
#[derive(Debug)]
pub(crate) struct AtomicTransactionState {
    pub(crate) state: AtomicU64,
}

impl From<TransactionState> for AtomicTransactionState {
    fn from(state: TransactionState) -> Self {
        Self {
            state: AtomicU64::new(state.encode()),
        }
    }
}

impl From<AtomicTransactionState> for TransactionState {
    fn from(state: AtomicTransactionState) -> Self {
        let encoded = state.state.load(Ordering::Acquire);
        TransactionState::decode(encoded)
    }
}

impl std::cmp::PartialEq<TransactionState> for AtomicTransactionState {
    fn eq(&self, other: &TransactionState) -> bool {
        &self.load() == other
    }
}

impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            TransactionState::Active => write!(f, "Active"),
            TransactionState::Preparing => write!(f, "Preparing"),
            TransactionState::Committed(ts) => write!(f, "Committed({ts})"),
            TransactionState::Aborted => write!(f, "Aborted"),
            TransactionState::Terminated => write!(f, "Terminated"),
        }
    }
}

impl AtomicTransactionState {
    fn store(&self, state: TransactionState) {
        self.state.store(state.encode(), Ordering::Release);
    }

    fn load(&self) -> TransactionState {
        TransactionState::decode(self.state.load(Ordering::Acquire))
    }
}

#[derive(Debug)]
pub enum CommitState {
    Initial,
    Commit { end_ts: u64 },
    BeginCommitLogicalLog { end_ts: u64, log_record: LogRecord },
    EndCommitLogicalLog { end_ts: u64 },
    SyncLogicalLog { end_ts: u64 },
    CommitEnd { end_ts: u64 },
}

#[derive(Debug)]
pub enum WriteRowState {
    Initial,
    Seek,
    Insert,
    /// Move to the next record in order to leave the cursor in the next position, this is used for inserting multiple rows for optimizations.
    Next,
}

#[derive(Debug)]
struct CommitCoordinator {
    pager_commit_lock: Arc<TursoRwLock>,
}

pub struct CommitStateMachine<Clock: LogicalClock> {
    state: CommitState,
    is_finalized: bool,
    did_commit_schema_change: bool,
    tx_id: TxID,
    connection: Arc<Connection>,
    /// Write set sorted by table id and row id
    write_set: Vec<RowID>,
    commit_coordinator: Arc<CommitCoordinator>,
    header: Arc<RwLock<Option<DatabaseHeader>>>,
    _phantom: PhantomData<Clock>,
}

impl<Clock: LogicalClock> Debug for CommitStateMachine<Clock> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitStateMachine")
            .field("state", &self.state)
            .field("is_finalized", &self.is_finalized)
            .finish()
    }
}

pub struct WriteRowStateMachine {
    state: WriteRowState,
    is_finalized: bool,
    row: Row,
    record: Option<ImmutableRecord>,
    cursor: Arc<RwLock<BTreeCursor>>,
    requires_seek: bool,
}

#[derive(Debug)]
pub enum DeleteRowState {
    Initial,
    Seek,
    Delete,
}

pub struct DeleteRowStateMachine {
    state: DeleteRowState,
    is_finalized: bool,
    rowid: RowID,
    cursor: Arc<RwLock<BTreeCursor>>,
}

impl<Clock: LogicalClock> CommitStateMachine<Clock> {
    fn new(
        state: CommitState,
        tx_id: TxID,
        connection: Arc<Connection>,
        commit_coordinator: Arc<CommitCoordinator>,
        header: Arc<RwLock<Option<DatabaseHeader>>>,
    ) -> Self {
        Self {
            state,
            is_finalized: false,
            did_commit_schema_change: false,
            tx_id,
            connection,
            write_set: Vec::new(),
            commit_coordinator,
            header,
            _phantom: PhantomData,
        }
    }
}

impl WriteRowStateMachine {
    fn new(row: Row, cursor: Arc<RwLock<BTreeCursor>>, requires_seek: bool) -> Self {
        Self {
            state: WriteRowState::Initial,
            is_finalized: false,
            row,
            record: None,
            cursor,
            requires_seek,
        }
    }
}

impl<Clock: LogicalClock> StateTransition for CommitStateMachine<Clock> {
    type Context = MvStore<Clock>;
    type SMResult = ();

    #[tracing::instrument(fields(state = ?self.state), skip(self, mvcc_store), level = Level::DEBUG)]
    fn step(&mut self, mvcc_store: &Self::Context) -> Result<TransitionResult<Self::SMResult>> {
        tracing::trace!("step(state={:?})", self.state);
        match &self.state {
            CommitState::Initial => {
                let end_ts = mvcc_store.get_timestamp();
                // NOTICE: the first shadowed tx keeps the entry alive in the map
                // for the duration of this whole function, which is important for correctness!
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or(LimboError::TxTerminated)?;
                let tx = tx.value();
                match tx.state.load() {
                    TransactionState::Terminated => {
                        return Err(LimboError::TxTerminated);
                    }
                    _ => {
                        assert_eq!(tx.state, TransactionState::Active);
                    }
                }

                if mvcc_store
                    .last_committed_schema_change_ts
                    .load(Ordering::Acquire)
                    > tx.begin_ts
                {
                    // Schema changes made after the transaction began always cause a [SchemaUpdated] error and the tx must abort.
                    return Err(LimboError::SchemaUpdated);
                }

                tx.state.store(TransactionState::Preparing);
                tracing::trace!("prepare_tx(tx_id={})", self.tx_id);

                /* TODO: The code we have here is sufficient for snapshot isolation.
                ** In order to implement serializability, we need the following steps:
                **
                ** 1. Validate if all read versions are still visible by inspecting the read_set
                ** 2. Validate if there are no phantoms by walking the scans from scan_set (which we don't even have yet)
                **    - a phantom is a version that became visible in the middle of our transaction,
                **      but wasn't taken into account during one of the scans from the scan_set
                ** 3. Wait for commit dependencies, which we don't even track yet...
                **    Excerpt from what's a commit dependency and how it's tracked in the original paper:
                **    """
                        A transaction T1 has a commit dependency on another transaction
                        T2, if T1 is allowed to commit only if T2 commits. If T2 aborts,
                        T1 must also abort, so cascading aborts are possible. T1 acquires a
                        commit dependency either by speculatively reading or speculatively ignoring a version,
                        instead of waiting for T2 to commit.
                        We implement commit dependencies by a register-and-report
                        approach: T1 registers its dependency with T2 and T2 informs T1
                        when it has committed or aborted. Each transaction T contains a
                        counter, CommitDepCounter, that counts how many unresolved
                        commit dependencies it still has. A transaction cannot commit
                        until this counter is zero. In addition, T has a Boolean variable
                        AbortNow that other transactions can set to tell T to abort. Each
                        transaction T also has a set, CommitDepSet, that stores transaction IDs
                        of the transactions that depend on T.
                        To take a commit dependency on a transaction T2, T1 increments
                        its CommitDepCounter and adds its transaction ID to T2’s CommitDepSet.
                        When T2 has committed, it locates each transaction in
                        its CommitDepSet and decrements their CommitDepCounter. If
                        T2 aborted, it tells the dependent transactions to also abort by
                        setting their AbortNow flags. If a dependent transaction is not
                        found, this means that it has already aborted.
                        Note that a transaction with commit dependencies may not have to
                        wait at all - the dependencies may have been resolved before it is
                        ready to commit. Commit dependencies consolidate all waits into
                        a single wait and postpone the wait to just before commit.
                        Some transactions may have to wait before commit.
                        Waiting raises a concern of deadlocks.
                        However, deadlocks cannot occur because an older transaction never
                        waits on a younger transaction. In
                        a wait-for graph the direction of edges would always be from a
                        younger transaction (higher end timestamp) to an older transaction
                        (lower end timestamp) so cycles are impossible.
                    """
                **  If you're wondering when a speculative read happens, here you go:
                **  Case 1: speculative read of TB:
                    """
                        If transaction TB is in the Preparing state, it has acquired an end
                        timestamp TS which will be V’s begin timestamp if TB commits.
                        A safe approach in this situation would be to have transaction T
                        wait until transaction TB commits. However, we want to avoid all
                        blocking during normal processing so instead we continue with
                        the visibility test and, if the test returns true, allow T to
                        speculatively read V. Transaction T acquires a commit dependency on
                        TB, restricting the serialization order of the two transactions. That
                        is, T is allowed to commit only if TB commits.
                    """
                **  Case 2: speculative ignore of TE:
                    """
                        If TE’s state is Preparing, it has an end timestamp TS that will become
                        the end timestamp of V if TE does commit. If TS is greater than the read
                        time RT, it is obvious that V will be visible if TE commits. If TE
                        aborts, V will still be visible, because any transaction that updates
                        V after TE has aborted will obtain an end timestamp greater than
                        TS. If TS is less than RT, we have a more complicated situation:
                        if TE commits, V will not be visible to T but if TE aborts, it will
                        be visible. We could handle this by forcing T to wait until TE
                        commits or aborts but we want to avoid all blocking during normal processing.
                        Instead we allow T to speculatively ignore V and
                        proceed with its processing. Transaction T acquires a commit
                        dependency (see Section 2.7) on TE, that is, T is allowed to commit
                        only if TE commits.
                    """
                */
                tracing::trace!("commit_tx(tx_id={})", self.tx_id);
                self.write_set
                    .extend(tx.write_set.iter().map(|v| *v.value()));
                self.write_set
                    .sort_by(|a, b| a.table_id.cmp(&b.table_id).then(a.row_id.cmp(&b.row_id)));
                if self.write_set.is_empty() {
                    tx.state.store(TransactionState::Committed(end_ts));
                    if mvcc_store.is_exclusive_tx(&self.tx_id) {
                        mvcc_store.release_exclusive_tx(&self.tx_id);
                        self.commit_coordinator.pager_commit_lock.unlock();
                    }
                    mvcc_store.remove_tx(self.tx_id);
                    self.finalize(mvcc_store)?;
                    return Ok(TransitionResult::Done(()));
                }
                self.state = CommitState::Commit { end_ts };
                Ok(TransitionResult::Continue)
            }
            CommitState::Commit { end_ts } => {
                let mut log_record = LogRecord::new(*end_ts);
                if !mvcc_store.is_exclusive_tx(&self.tx_id) && mvcc_store.has_exclusive_tx() {
                    // A non-CONCURRENT transaction is holding the exclusive lock, we must abort.
                    return Err(LimboError::WriteWriteConflict);
                }
                for id in &self.write_set {
                    if let Some(row_versions) = mvcc_store.rows.get(id) {
                        let mut row_versions = row_versions.value().write();
                        for row_version in row_versions.iter_mut() {
                            if let TxTimestampOrID::TxID(id) = row_version.begin {
                                if id == self.tx_id {
                                    // New version is valid STARTING FROM committing transaction's end timestamp
                                    // See diagram on page 299: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf
                                    row_version.begin = TxTimestampOrID::Timestamp(*end_ts);
                                    mvcc_store.insert_version_raw(
                                        &mut log_record.row_versions,
                                        row_version.clone(),
                                    ); // FIXME: optimize cloning out

                                    if row_version.row.id.table_id == 1 {
                                        self.did_commit_schema_change = true;
                                    }
                                }
                            }
                            if let Some(TxTimestampOrID::TxID(id)) = row_version.end {
                                if id == self.tx_id {
                                    // Old version is valid UNTIL committing transaction's end timestamp
                                    // See diagram on page 299: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf
                                    row_version.end = Some(TxTimestampOrID::Timestamp(*end_ts));
                                    mvcc_store.insert_version_raw(
                                        &mut log_record.row_versions,
                                        row_version.clone(),
                                    ); // FIXME: optimize cloning out

                                    if row_version.row.id.table_id == 1 {
                                        self.did_commit_schema_change = true;
                                    }
                                }
                            }
                        }
                    }
                }
                tracing::trace!("updated(tx_id={})", self.tx_id);

                if log_record.row_versions.is_empty() {
                    // Nothing to do, just end commit.
                    self.state = CommitState::CommitEnd { end_ts: *end_ts };
                } else {
                    // We might need to serialize log writes
                    self.state = CommitState::BeginCommitLogicalLog {
                        end_ts: *end_ts,
                        log_record,
                    };
                }
                return Ok(TransitionResult::Continue);
            }
            CommitState::BeginCommitLogicalLog { end_ts, log_record } => {
                if !mvcc_store.is_exclusive_tx(&self.tx_id) {
                    // logical log needs to be serialized
                    let locked = self.commit_coordinator.pager_commit_lock.write();
                    if !locked {
                        return Ok(TransitionResult::Io(IOCompletions::Single(
                            Completion::new_dummy(),
                        )));
                    }
                }
                let result = mvcc_store.storage.log_tx(log_record)?;
                self.state = CommitState::SyncLogicalLog { end_ts: *end_ts };
                match result {
                    IOResult::Done(_) => {}
                    IOResult::IO(io) => {
                        if !io.finished() {
                            return Ok(TransitionResult::Io(io));
                        }
                    }
                }
                return Ok(TransitionResult::Continue);
            }
            CommitState::SyncLogicalLog { end_ts } => {
                let result = mvcc_store.storage.sync()?;
                self.state = CommitState::EndCommitLogicalLog { end_ts: *end_ts };
                if let IOResult::IO(io) = result {
                    if !io.finished() {
                        return Ok(TransitionResult::Io(io));
                    }
                }
                return Ok(TransitionResult::Continue);
            }
            CommitState::EndCommitLogicalLog { end_ts } => {
                let connection = self.connection.clone();
                let schema_did_change = self.did_commit_schema_change;
                if schema_did_change {
                    let schema = connection.schema.read().clone();
                    connection.db.update_schema_if_newer(schema)?;
                }
                let tx = mvcc_store.txs.get(&self.tx_id).unwrap();
                let tx_unlocked = tx.value();
                self.header.write().replace(*tx_unlocked.header.read());
                tracing::trace!("end_commit_logical_log(tx_id={})", self.tx_id);
                self.commit_coordinator.pager_commit_lock.unlock();
                self.state = CommitState::CommitEnd { end_ts: *end_ts };
                return Ok(TransitionResult::Continue);
            }
            CommitState::CommitEnd { end_ts } => {
                let tx = mvcc_store.txs.get(&self.tx_id).unwrap();
                let tx_unlocked = tx.value();
                tx_unlocked
                    .state
                    .store(TransactionState::Committed(*end_ts));
                mvcc_store
                    .global_header
                    .write()
                    .replace(*tx_unlocked.header.read());

                mvcc_store
                    .last_committed_tx_ts
                    .store(*end_ts, Ordering::Release);
                if self.did_commit_schema_change {
                    mvcc_store
                        .last_committed_schema_change_ts
                        .store(*end_ts, Ordering::Release);
                }

                // We have now updated all the versions with a reference to the
                // transaction ID to a timestamp and can, therefore, remove the
                // transaction. Please note that when we move to lockless, the
                // invariant doesn't necessarily hold anymore because another thread
                // might have speculatively read a version that we want to remove.
                // But that's a problem for another day.
                // FIXME: it actually just become a problem for today!!!
                // TODO: test that reproduces this failure, and then a fix
                mvcc_store.remove_tx(self.tx_id);

                if mvcc_store.is_exclusive_tx(&self.tx_id) {
                    mvcc_store.release_exclusive_tx(&self.tx_id);
                }
                tracing::trace!("logged(tx_id={}, end_ts={})", self.tx_id, *end_ts);
                self.finalize(mvcc_store)?;
                Ok(TransitionResult::Done(()))
            }
        }
    }

    fn finalize(&mut self, _context: &Self::Context) -> Result<()> {
        self.is_finalized = true;
        Ok(())
    }

    fn is_finalized(&self) -> bool {
        self.is_finalized
    }
}

impl StateTransition for WriteRowStateMachine {
    type Context = ();
    type SMResult = ();

    #[tracing::instrument(fields(state = ?self.state), skip(self, _context), level = Level::DEBUG)]
    fn step(&mut self, _context: &Self::Context) -> Result<TransitionResult<Self::SMResult>> {
        use crate::types::{IOResult, SeekKey, SeekOp};

        match self.state {
            WriteRowState::Initial => {
                // Create the record and key
                let mut record = ImmutableRecord::new(self.row.data.len());
                record.start_serialization(&self.row.data);
                self.record = Some(record);

                if self.requires_seek {
                    self.state = WriteRowState::Seek;
                } else {
                    self.state = WriteRowState::Insert;
                }
                Ok(TransitionResult::Continue)
            }
            WriteRowState::Seek => {
                // Position the cursor by seeking to the row position
                let seek_key = SeekKey::TableRowId(self.row.id.row_id);

                match self
                    .cursor
                    .write()
                    .seek(seek_key, SeekOp::GE { eq_only: true })?
                {
                    IOResult::Done(_) => {}
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
                assert_eq!(self.cursor.write().valid_state, CursorValidState::Valid);
                self.state = WriteRowState::Insert;
                Ok(TransitionResult::Continue)
            }
            WriteRowState::Insert => {
                // Insert the record into the B-tree
                let key = BTreeKey::new_table_rowid(self.row.id.row_id, self.record.as_ref());

                match self
                    .cursor
                    .write()
                    .insert(&key)
                    .map_err(|e: LimboError| LimboError::InternalError(e.to_string()))?
                {
                    IOResult::Done(()) => {}
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
                self.state = WriteRowState::Next;
                Ok(TransitionResult::Continue)
            }
            WriteRowState::Next => {
                match self
                    .cursor
                    .write()
                    .next()
                    .map_err(|e: LimboError| LimboError::InternalError(e.to_string()))?
                {
                    IOResult::Done(_) => {}
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
                self.finalize(&())?;
                Ok(TransitionResult::Done(()))
            }
        }
    }

    fn finalize(&mut self, _context: &Self::Context) -> Result<()> {
        self.is_finalized = true;
        Ok(())
    }

    fn is_finalized(&self) -> bool {
        self.is_finalized
    }
}

impl StateTransition for DeleteRowStateMachine {
    type Context = ();
    type SMResult = ();

    #[tracing::instrument(fields(state = ?self.state), skip(self, _context))]
    fn step(&mut self, _context: &Self::Context) -> Result<TransitionResult<Self::SMResult>> {
        use crate::types::{IOResult, SeekKey, SeekOp};

        match self.state {
            DeleteRowState::Initial => {
                self.state = DeleteRowState::Seek;
                Ok(TransitionResult::Continue)
            }
            DeleteRowState::Seek => {
                let seek_key = SeekKey::TableRowId(self.rowid.row_id);

                match self
                    .cursor
                    .write()
                    .seek(seek_key, SeekOp::GE { eq_only: true })?
                {
                    IOResult::Done(seek_res) => {
                        if seek_res == SeekResult::NotFound {
                            crate::bail_corrupt_error!(
                                "MVCC delete: rowid {} not found",
                                self.rowid.row_id
                            );
                        }
                        self.state = DeleteRowState::Delete;
                        Ok(TransitionResult::Continue)
                    }
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
            }
            DeleteRowState::Delete => {
                // Insert the record into the B-tree

                match self
                    .cursor
                    .write()
                    .delete()
                    .map_err(|e| LimboError::InternalError(e.to_string()))?
                {
                    IOResult::Done(()) => {}
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
                tracing::trace!(
                    "delete_row_from_pager(table_id={}, row_id={})",
                    self.rowid.table_id,
                    self.rowid.row_id
                );
                self.finalize(&())?;
                Ok(TransitionResult::Done(()))
            }
        }
    }

    fn finalize(&mut self, _context: &Self::Context) -> Result<()> {
        self.is_finalized = true;
        Ok(())
    }

    fn is_finalized(&self) -> bool {
        self.is_finalized
    }
}

impl DeleteRowStateMachine {
    fn new(rowid: RowID, cursor: Arc<RwLock<BTreeCursor>>) -> Self {
        Self {
            state: DeleteRowState::Initial,
            is_finalized: false,
            rowid,
            cursor,
        }
    }
}

/// A multi-version concurrency control database.
#[derive(Debug)]
pub struct MvStore<Clock: LogicalClock> {
    rows: SkipMap<RowID, RwLock<Vec<RowVersion>>>,
    txs: SkipMap<TxID, Transaction>,
    tx_ids: AtomicU64,
    next_rowid: AtomicU64,
    next_table_id: AtomicU64,
    clock: Clock,
    storage: Storage,
    loaded_tables: RwLock<HashSet<i64>>,

    /// The transaction ID of a transaction that has acquired an exclusive write lock, if any.
    ///
    /// An exclusive MVCC transaction is one that has a write lock on the pager, which means
    /// every other MVCC transaction must wait for it to commit before they can commit. We have
    /// exclusive transactions to support single-writer semantics for compatibility with SQLite.
    ///
    /// If there is no exclusive transaction, the field is set to `NO_EXCLUSIVE_TX`.
    exclusive_tx: AtomicU64,
    commit_coordinator: Arc<CommitCoordinator>,
    global_header: Arc<RwLock<Option<DatabaseHeader>>>,
    /// MVCC checkpoints are always TRUNCATE, plus they block all other transactions.
    /// This guarantees that never need to let transactions read from the SQLite WAL.
    /// In MVCC, the checkpoint procedure is roughly as follows:
    /// - Take the blocking_checkpoint_lock
    /// - Write everything in the logical log to the pager, and from there commit to the SQLite WAL.
    /// - Immediately TRUNCATE checkpoint the WAL into the database file.
    /// - Release the blocking_checkpoint_lock.
    blocking_checkpoint_lock: Arc<TursoRwLock>,
    /// The highest transaction ID that has been checkpointed.
    /// Used to skip checkpointing transactions that have already been checkpointed.
    checkpointed_txid_max: AtomicU64,
    /// The timestamp of the last committed schema change.
    /// Schema changes always cause a [SchemaUpdated] error.
    last_committed_schema_change_ts: AtomicU64,
    /// The timestamp of the last committed transaction.
    /// If there are two concurrent BEGIN (non-CONCURRENT) transactions, and one tries to promote
    /// to exclusive, it will abort if another transaction committed after its begin timestamp.
    last_committed_tx_ts: AtomicU64,

    /// Lock used while recovering a logical log file. We don't want multiple connections trying to
    /// load the file.
    recover_lock: RwLock<()>,
}

impl<Clock: LogicalClock> MvStore<Clock> {
    /// Creates a new database.
    pub fn new(clock: Clock, storage: Storage) -> Self {
        Self {
            rows: SkipMap::new(),
            txs: SkipMap::new(),
            tx_ids: AtomicU64::new(1), // let's reserve transaction 0 for special purposes
            next_rowid: AtomicU64::new(0), // TODO: determine this from B-Tree
            next_table_id: AtomicU64::new(2), // table id 1 / root page 1 is always sqlite_schema.
            clock,
            storage,
            loaded_tables: RwLock::new(HashSet::new()),
            exclusive_tx: AtomicU64::new(NO_EXCLUSIVE_TX),
            commit_coordinator: Arc::new(CommitCoordinator {
                pager_commit_lock: Arc::new(TursoRwLock::new()),
            }),
            global_header: Arc::new(RwLock::new(None)),
            blocking_checkpoint_lock: Arc::new(TursoRwLock::new()),
            checkpointed_txid_max: AtomicU64::new(0),
            last_committed_schema_change_ts: AtomicU64::new(0),
            last_committed_tx_ts: AtomicU64::new(0),
            recover_lock: RwLock::new(()),
        }
    }

    /// MVCC does not use the pager/btree cursors to create pages until checkpoint.
    /// This method is used to assign root page numbers when Insn::CreateBtree is used.
    /// NOTE: during MVCC recovery (not implemented yet), [MvStore::next_table_id] must be
    /// initialized to the current highest table id / root page number.
    pub fn get_next_table_id(&self) -> u64 {
        self.next_table_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn get_next_rowid(&self) -> i64 {
        self.next_rowid.fetch_add(1, Ordering::SeqCst) as i64
    }

    /// Inserts a new row into the database.
    ///
    /// This function inserts a new `row` into the database within the context
    /// of the transaction `tx_id`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - the ID of the transaction in which to insert the new row.
    /// * `row` - the row object containing the values to be inserted.
    ///
    pub fn insert(&self, tx_id: TxID, row: Row) -> Result<()> {
        tracing::trace!("insert(tx_id={}, row.id={:?})", tx_id, row.id);
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or(LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        assert_eq!(tx.state, TransactionState::Active);
        let id = row.id;
        let row_version = RowVersion {
            begin: TxTimestampOrID::TxID(tx.tx_id),
            end: None,
            row,
        };
        tx.insert_to_write_set(id);
        self.insert_version(id, row_version);
        Ok(())
    }

    /// Updates a row in the database with new values.
    ///
    /// This function updates an existing row in the database within the
    /// context of the transaction `tx_id`. The `row` argument identifies the
    /// row to be updated as `id` and contains the new values to be inserted.
    ///
    /// If the row identified by the `id` does not exist, this function does
    /// nothing and returns `false`. Otherwise, the function updates the row
    /// with the new values and returns `true`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - the ID of the transaction in which to update the new row.
    /// * `row` - the row object containing the values to be updated.
    ///
    /// # Returns
    ///
    /// Returns `true` if the row was successfully updated, and `false` otherwise.
    pub fn update(&self, tx_id: TxID, row: Row) -> Result<bool> {
        tracing::trace!("update(tx_id={}, row.id={:?})", tx_id, row.id);
        if !self.delete(tx_id, row.id)? {
            return Ok(false);
        }
        self.insert(tx_id, row)?;
        Ok(true)
    }

    /// Inserts a row in the database with new values, previously deleting
    /// any old data if it existed. Bails on a delete error, e.g. write-write conflict.
    pub fn upsert(&self, tx_id: TxID, row: Row) -> Result<()> {
        tracing::trace!("upsert(tx_id={}, row.id={:?})", tx_id, row.id);
        self.delete(tx_id, row.id)?;
        self.insert(tx_id, row)
    }

    /// Deletes a row from the table with the given `id`.
    ///
    /// This function deletes an existing row `id` in the database within the
    /// context of the transaction `tx_id`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - the ID of the transaction in which to delete the new row.
    /// * `id` - the ID of the row to delete.
    ///
    /// # Returns
    ///
    /// Returns `true` if the row was successfully deleted, and `false` otherwise.
    ///
    pub fn delete(&self, tx_id: TxID, id: RowID) -> Result<bool> {
        tracing::trace!("delete(tx_id={}, id={:?})", tx_id, id);
        let row_versions_opt = self.rows.get(&id);
        if let Some(ref row_versions) = row_versions_opt {
            let mut row_versions = row_versions.value().write();
            for rv in row_versions.iter_mut().rev() {
                let tx = self
                    .txs
                    .get(&tx_id)
                    .ok_or(LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                let tx = tx.value();
                assert_eq!(tx.state, TransactionState::Active);
                // A transaction cannot delete a version that it cannot see,
                // nor can it conflict with it.
                if !rv.is_visible_to(tx, &self.txs) {
                    continue;
                }
                if is_write_write_conflict(&self.txs, tx, rv) {
                    drop(row_versions);
                    drop(row_versions_opt);
                    return Err(LimboError::WriteWriteConflict);
                }

                rv.end = Some(TxTimestampOrID::TxID(tx.tx_id));
                drop(row_versions);
                drop(row_versions_opt);
                let tx = self
                    .txs
                    .get(&tx_id)
                    .ok_or(LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                let tx = tx.value();
                tx.insert_to_write_set(id);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Retrieves a row from the table with the given `id`.
    ///
    /// This operation is performed within the scope of the transaction identified
    /// by `tx_id`.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to perform the read operation in.
    /// * `id` - The ID of the row to retrieve.
    ///
    /// # Returns
    ///
    /// Returns `Some(row)` with the row data if the row with the given `id` exists,
    /// and `None` otherwise.
    pub fn read(&self, tx_id: TxID, id: RowID) -> Result<Option<Row>> {
        tracing::trace!("read(tx_id={}, id={:?})", tx_id, id);
        let tx = self.txs.get(&tx_id).unwrap();
        let tx = tx.value();
        assert_eq!(tx.state, TransactionState::Active);
        if let Some(row_versions) = self.rows.get(&id) {
            let row_versions = row_versions.value().read();
            if let Some(rv) = row_versions
                .iter()
                .rev()
                .find(|rv| rv.is_visible_to(tx, &self.txs))
            {
                tx.insert_to_read_set(id);
                return Ok(Some(rv.row.clone()));
            }
        }
        Ok(None)
    }

    /// Gets all row ids in the database.
    pub fn scan_row_ids(&self) -> Result<Vec<RowID>> {
        tracing::trace!("scan_row_ids");
        let keys = self.rows.iter().map(|entry| *entry.key());
        Ok(keys.collect())
    }

    pub fn get_row_id_range(
        &self,
        table_id: i64,
        start: i64,
        bucket: &mut Vec<RowID>,
        max_items: u64,
    ) -> Result<()> {
        tracing::trace!(
            "get_row_id_in_range(table_id={}, range_start={})",
            table_id,
            start,
        );
        let start_id = RowID {
            table_id,
            row_id: start,
        };

        let end_id = RowID {
            table_id,
            row_id: i64::MAX,
        };

        self.rows
            .range(start_id..end_id)
            .take(max_items as usize)
            .for_each(|entry| bucket.push(*entry.key()));

        Ok(())
    }

    pub fn get_next_row_id_for_table(
        &self,
        table_id: i64,
        start: i64,
        tx_id: TxID,
    ) -> Option<RowID> {
        tracing::trace!(
            "getting_next_id_for_table(table_id={}, range_start={})",
            table_id,
            start,
        );
        let min_bound = RowID {
            table_id,
            row_id: start,
        };

        let max_bound = RowID {
            table_id,
            row_id: i64::MAX,
        };

        let tx = self.txs.get(&tx_id).unwrap();
        let tx = tx.value();
        let mut rows = self.rows.range(min_bound..max_bound);
        loop {
            // We are moving forward, so if a row was deleted we just need to skip it. Therefore, we need
            // to loop either until we find a row that is not deleted or until we reach the end of the table.
            let next_row = rows.next();
            let row = next_row?;

            // We found a row, let's check if it's visible to the transaction.
            if let Some(visible_row) = self.find_last_visible_version(tx, row) {
                return Some(visible_row);
            }
            // If this row is not visible, continue to the next row
        }
    }

    fn find_last_visible_version(
        &self,
        tx: &Transaction,
        row: crossbeam_skiplist::map::Entry<
            '_,
            RowID,
            parking_lot::lock_api::RwLock<parking_lot::RawRwLock, Vec<RowVersion>>,
        >,
    ) -> Option<RowID> {
        row.value()
            .read()
            .iter()
            .rev()
            .find(|version| version.is_visible_to(tx, &self.txs))
            .map(|_| *row.key())
    }

    pub fn seek_rowid(
        &self,
        bound: Bound<&RowID>,
        lower_bound: bool,
        tx_id: TxID,
    ) -> Option<RowID> {
        tracing::trace!("seek_rowid(bound={:?}, lower_bound={})", bound, lower_bound,);

        let tx = self.txs.get(&tx_id).unwrap();
        let tx = tx.value();
        if lower_bound {
            self.rows
                .lower_bound(bound)
                .and_then(|entry| self.find_last_visible_version(tx, entry))
        } else {
            self.rows
                .upper_bound(bound)
                .and_then(|entry| self.find_last_visible_version(tx, entry))
        }
    }

    /// Begins an exclusive write transaction that prevents concurrent writes.
    ///
    /// This is used for IMMEDIATE and EXCLUSIVE transaction types where we need
    /// to ensure exclusive write access as per SQLite semantics.
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn begin_exclusive_tx(
        &self,
        pager: Arc<Pager>,
        maybe_existing_tx_id: Option<TxID>,
    ) -> Result<IOResult<TxID>> {
        if !self.blocking_checkpoint_lock.read() {
            // If there is a stop-the-world checkpoint in progress, we cannot begin any transaction at all.
            return Err(LimboError::Busy);
        }
        let unlock = || self.blocking_checkpoint_lock.unlock();
        let tx_id = maybe_existing_tx_id.unwrap_or_else(|| self.get_tx_id());
        let begin_ts = if let Some(tx_id) = maybe_existing_tx_id {
            self.txs.get(&tx_id).unwrap().value().begin_ts
        } else {
            self.get_timestamp()
        };

        self.acquire_exclusive_tx(&tx_id)
            .inspect_err(|_| unlock())?;

        let locked = self.commit_coordinator.pager_commit_lock.write();
        if !locked {
            tracing::debug!(
                "begin_exclusive_tx: tx_id={} failed with Busy on pager_commit_lock",
                tx_id
            );
            self.release_exclusive_tx(&tx_id);
            unlock();
            return Err(LimboError::Busy);
        }

        let header = self.get_new_transaction_database_header(&pager);

        let tx = Transaction::new(tx_id, begin_ts, header);
        tracing::trace!(
            "begin_exclusive_tx(tx_id={}) - exclusive write logical log transaction",
            tx_id
        );
        tracing::debug!("begin_exclusive_tx: tx_id={} succeeded", tx_id);
        self.txs.insert(tx_id, tx);
        Ok(IOResult::Done(tx_id))
    }

    /// Begins a new transaction in the database.
    ///
    /// This function starts a new transaction in the database and returns a `TxID` value
    /// that you can use to perform operations within the transaction. All changes made within the
    /// transaction are isolated from other transactions until you commit the transaction.
    pub fn begin_tx(&self, pager: Arc<Pager>) -> Result<TxID> {
        if !self.blocking_checkpoint_lock.read() {
            // If there is a stop-the-world checkpoint in progress, we cannot begin any transaction at all.
            return Err(LimboError::Busy);
        }
        let tx_id = self.get_tx_id();
        let begin_ts = self.get_timestamp();

        // Set txn's header to the global header
        let header = self.get_new_transaction_database_header(&pager);
        let tx = Transaction::new(tx_id, begin_ts, header);
        tracing::trace!("begin_tx(tx_id={})", tx_id);
        self.txs.insert(tx_id, tx);

        Ok(tx_id)
    }

    pub fn remove_tx(&self, tx_id: TxID) {
        self.txs.remove(&tx_id);
        self.blocking_checkpoint_lock.unlock();
    }

    /// Begins a loading transaction
    ///
    /// A loading transaction is one that happens while trying recover from a logical log file.
    /// This transaction will be stored on tx_id = 0.
    pub fn begin_load_tx(&self, pager: Arc<Pager>) -> Result<()> {
        let tx_id = 0;
        let begin_ts = self.get_timestamp();

        let header = self.get_new_transaction_database_header(&pager);
        let tx = Transaction::new(tx_id, begin_ts, header);
        tracing::trace!("begin_load_tx(tx_id={tx_id})");
        assert!(
            !self.txs.contains_key(&tx_id),
            "somehow we tried to call begin_load_tx twice"
        );
        self.txs.insert(tx_id, tx);

        Ok(())
    }

    fn get_new_transaction_database_header(&self, pager: &Arc<Pager>) -> DatabaseHeader {
        if self.global_header.read().is_none() {
            pager.io.block(|| pager.maybe_allocate_page1()).unwrap();
            let header = pager
                .io
                .block(|| pager.with_header(|header| *header))
                .unwrap();
            // TODO: We initialize header here, maybe this needs more careful handling
            self.global_header.write().replace(header);
            tracing::debug!(
                "get_transaction_database_header create: header={:?}",
                header
            );
            header
        } else {
            let header = self.global_header.read().unwrap();
            tracing::debug!("get_transaction_database_header read: header={:?}", header);
            header
        }
    }

    pub fn get_transaction_database_header(&self, tx_id: &TxID) -> DatabaseHeader {
        let tx = self
            .txs
            .get(tx_id)
            .expect("transaction not found when trying to get header");
        let header = tx.value();
        let header = header.header.read();
        tracing::debug!("get_transaction_database_header read: header={:?}", header);
        *header
    }

    pub fn with_header<T, F>(&self, f: F, tx_id: Option<&TxID>) -> Result<T>
    where
        F: Fn(&DatabaseHeader) -> T,
    {
        if let Some(tx_id) = tx_id {
            let tx = self.txs.get(tx_id).unwrap();
            let header = tx.value();
            let header = header.header.read();
            tracing::debug!("with_header read: header={:?}", header);
            Ok(f(&header))
        } else {
            let header = self.global_header.read();
            tracing::debug!("with_header read: header={:?}", header);
            Ok(f(header.as_ref().unwrap()))
        }
    }

    pub fn with_header_mut<T, F>(&self, f: F, tx_id: Option<&TxID>) -> Result<T>
    where
        F: Fn(&mut DatabaseHeader) -> T,
    {
        if let Some(tx_id) = tx_id {
            let tx = self.txs.get(tx_id).unwrap();
            let header = tx.value();
            let mut header = header.header.write();
            tracing::debug!("with_header_mut read: header={:?}", header);
            Ok(f(&mut header))
        } else {
            let mut header = self.global_header.write();
            let header = header.as_mut().unwrap();
            tracing::debug!("with_header_mut write: header={:?}", header);
            Ok(f(header))
        }
    }

    /// Commits a transaction with the specified transaction ID.
    ///
    /// This function commits the changes made within the specified transaction and finalizes the
    /// transaction. Once a transaction has been committed, all changes made within the transaction
    /// are visible to other transactions that access the same data.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to commit.
    pub fn commit_tx(
        &self,
        tx_id: TxID,
        connection: &Arc<Connection>,
    ) -> Result<StateMachine<CommitStateMachine<Clock>>> {
        let state_machine: StateMachine<CommitStateMachine<Clock>> =
            StateMachine::<CommitStateMachine<Clock>>::new(CommitStateMachine::new(
                CommitState::Initial,
                tx_id,
                connection.clone(),
                self.commit_coordinator.clone(),
                self.global_header.clone(),
            ));
        Ok(state_machine)
    }

    /// Commits a load transaction that is running while recovering from a logical log file.
    /// This will simply mark timestamps of row version correctly so they are now visible to new
    /// transactions.
    pub fn commit_load_tx(&self, tx_id: TxID) {
        let end_ts = self.get_timestamp();
        let tx = self.txs.get(&tx_id).unwrap();
        let tx = tx.value();
        for rowid in &tx.write_set {
            let rowid = rowid.value();
            if let Some(row_versions) = self.rows.get(rowid) {
                let mut row_versions = row_versions.value().write();
                // Find rows that were written by this transaction.
                // Hekaton uses oldest-to-newest order for row versions, so we reverse iterate to find the newest one
                // this transaction changed.
                for row_version in row_versions.iter_mut().rev() {
                    if let TxTimestampOrID::TxID(id) = row_version.begin {
                        turso_assert!(
                            id == tx_id,
                            "only one tx(0) should exist on loading logical log"
                        );
                        // New version is valid STARTING FROM committing transaction's end timestamp
                        // See diagram on page 299: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf
                        row_version.begin = TxTimestampOrID::Timestamp(end_ts);
                    }
                    if let Some(TxTimestampOrID::TxID(id)) = row_version.end {
                        turso_assert!(
                            id == tx_id,
                            "only one tx(0) should exist on loading logical log"
                        );
                        // Old version is valid UNTIL committing transaction's end timestamp
                        // See diagram on page 299: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf
                        row_version.end = Some(TxTimestampOrID::Timestamp(end_ts));
                    }
                }
            }
        }
    }

    /// Rolls back a transaction with the specified ID.
    ///
    /// This function rolls back a transaction with the specified `tx_id` by
    /// discarding any changes made by the transaction.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to abort.
    pub fn rollback_tx(
        &self,
        tx_id: TxID,
        _pager: Arc<Pager>,
        connection: &Connection,
    ) -> Result<()> {
        let tx_unlocked = self.txs.get(&tx_id).unwrap();
        let tx = tx_unlocked.value();
        *connection.mv_tx.write() = None;
        assert!(tx.state == TransactionState::Active || tx.state == TransactionState::Preparing);
        tx.state.store(TransactionState::Aborted);
        tracing::trace!("abort(tx_id={})", tx_id);
        let write_set: Vec<RowID> = tx.write_set.iter().map(|v| *v.value()).collect();

        if self.is_exclusive_tx(&tx_id) {
            self.commit_coordinator.pager_commit_lock.unlock();
            self.release_exclusive_tx(&tx_id);
        }

        for ref id in write_set {
            if let Some(row_versions) = self.rows.get(id) {
                let mut row_versions = row_versions.value().write();
                for rv in row_versions.iter_mut() {
                    if rv.end == Some(TxTimestampOrID::TxID(tx_id)) {
                        // undo deletions by this transaction
                        rv.end = None;
                    }
                }
                // remove insertions by this transaction
                row_versions.retain(|rv| rv.begin != TxTimestampOrID::TxID(tx_id));
                if row_versions.is_empty() {
                    self.rows.remove(id);
                }
            }
        }

        if connection.schema.read().schema_version
            > connection.db.schema.lock().unwrap().schema_version
        {
            // Connection made schema changes during tx and rolled back -> revert connection-local schema.
            *connection.schema.write() = connection.db.clone_schema()?;
        }

        let tx = tx_unlocked.value();
        tx.state.store(TransactionState::Terminated);
        tracing::trace!("terminate(tx_id={})", tx_id);
        // FIXME: verify that we can already remove the transaction here!
        // Maybe it's fine for snapshot isolation, but too early for serializable?
        self.remove_tx(tx_id);

        Ok(())
    }

    /// Returns true if the given transaction is the exclusive transaction.
    #[inline]
    pub fn is_exclusive_tx(&self, tx_id: &TxID) -> bool {
        self.exclusive_tx.load(Ordering::Acquire) == *tx_id
    }

    /// Returns true if there is an exclusive transaction ongoing.
    #[inline]
    fn has_exclusive_tx(&self) -> bool {
        self.exclusive_tx.load(Ordering::Acquire) != NO_EXCLUSIVE_TX
    }

    /// Acquires the exclusive transaction lock to the given transaction ID.
    fn acquire_exclusive_tx(&self, tx_id: &TxID) -> Result<()> {
        if let Some(tx) = self.txs.get(tx_id) {
            let tx = tx.value();
            if tx.begin_ts < self.last_committed_tx_ts.load(Ordering::Acquire) {
                // Another transaction committed after this transaction's begin timestamp, do not allow exclusive lock.
                // This mimics regular (non-CONCURRENT) sqlite transaction behavior.
                return Err(LimboError::Busy);
            }
        }
        match self.exclusive_tx.compare_exchange(
            NO_EXCLUSIVE_TX,
            *tx_id,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(_) => {
                // Another transaction already holds the exclusive lock
                Err(LimboError::Busy)
            }
        }
    }

    /// Release the exclusive transaction lock if held by the this transaction.
    fn release_exclusive_tx(&self, tx_id: &TxID) {
        tracing::trace!("release_exclusive_tx(tx_id={})", tx_id);
        let prev = self.exclusive_tx.swap(NO_EXCLUSIVE_TX, Ordering::Release);
        assert_eq!(
            prev, *tx_id,
            "Tried to release exclusive lock for tx {tx_id} but it was held by tx {prev}"
        );
    }

    /// Generates next unique transaction id
    pub fn get_tx_id(&self) -> u64 {
        self.tx_ids.fetch_add(1, Ordering::SeqCst)
    }

    /// Gets current timestamp
    pub fn get_timestamp(&self) -> u64 {
        self.clock.get_timestamp()
    }

    /// Removes unused row  versions with very loose heuristics,
    /// which sometimes leaves versions intact for too long.
    /// Returns the number of removed versions.
    pub fn drop_unused_row_versions(&self) -> usize {
        tracing::trace!(
            "drop_unused_row_versions() -> txs: {}; rows: {}",
            self.txs.len(),
            self.rows.len()
        );
        let mut dropped = 0;
        let mut to_remove = Vec::new();
        for entry in self.rows.iter() {
            let mut row_versions = entry.value().write();
            row_versions.retain(|rv| {
                // FIXME: should take rv.begin into account as well
                let should_stay = match rv.end {
                    Some(TxTimestampOrID::Timestamp(version_end_ts)) => {
                        // a transaction started before this row version ended, ergo row version is needed
                        // NOTICE: O(row_versions x transactions), but also lock-free, so sounds acceptable
                        self.txs.iter().any(|tx| {
                            let tx = tx.value();
                            // FIXME: verify!
                            match tx.state.load() {
                                TransactionState::Active | TransactionState::Preparing => {
                                    version_end_ts > tx.begin_ts
                                }
                                _ => false,
                            }
                        })
                    }
                    // Let's skip potentially complex logic if the transafction is still
                    // active/tracked. We will drop the row version when the transaction
                    // gets garbage-collected itself, it will always happen eventually.
                    Some(TxTimestampOrID::TxID(tx_id)) => !self.txs.contains_key(&tx_id),
                    // this row version is current, ergo visible
                    None => true,
                };
                if !should_stay {
                    dropped += 1;
                    tracing::trace!(
                        "Dropping row version {:?} {:?}-{:?}",
                        entry.key(),
                        rv.begin,
                        rv.end
                    );
                }
                should_stay
            });
            if row_versions.is_empty() {
                to_remove.push(*entry.key());
            }
        }
        for id in to_remove {
            self.rows.remove(&id);
        }
        dropped
    }

    pub fn recover(&self) -> Result<()> {
        let tx_log = self.storage.read_tx_log()?;
        for record in tx_log {
            tracing::debug!("recover() -> tx_timestamp={}", record.tx_timestamp);
            for version in record.row_versions {
                self.insert_version(version.row.id, version);
            }
            self.clock.reset(record.tx_timestamp);
        }
        Ok(())
    }

    // Extracts the begin timestamp from a transaction
    #[inline]
    fn get_begin_timestamp(&self, ts_or_id: &TxTimestampOrID) -> u64 {
        match ts_or_id {
            TxTimestampOrID::Timestamp(ts) => *ts,
            TxTimestampOrID::TxID(tx_id) => self.txs.get(tx_id).unwrap().value().begin_ts,
        }
    }

    /// Inserts a new row version into the database, while making sure that
    /// the row version is inserted in the correct order.
    fn insert_version(&self, id: RowID, row_version: RowVersion) {
        let versions = self.rows.get_or_insert_with(id, || RwLock::new(Vec::new()));
        let mut versions = versions.value().write();
        self.insert_version_raw(&mut versions, row_version)
    }

    /// Inserts a new row version into the internal data structure for versions,
    /// while making sure that the row version is inserted in the correct order.
    pub fn insert_version_raw(&self, versions: &mut Vec<RowVersion>, row_version: RowVersion) {
        // NOTICE: this is an insert a'la insertion sort, with pessimistic linear complexity.
        // However, we expect the number of versions to be nearly sorted, so we deem it worthy
        // to search linearly for the insertion point instead of paying the price of using
        // another data structure, e.g. a BTreeSet. If it proves to be too quadratic empirically,
        // we can either switch to a tree-like structure, or at least use partition_point()
        // which performs a binary search for the insertion point.
        let mut position = 0_usize;
        for (i, v) in versions.iter().enumerate().rev() {
            if self.get_begin_timestamp(&v.begin) <= self.get_begin_timestamp(&row_version.begin) {
                position = i + 1;
                break;
            }
        }
        if versions.len() - position > 3 {
            tracing::debug!(
                "Inserting a row version {} positions from the end",
                versions.len() - position
            );
        }
        versions.insert(position, row_version);
    }

    pub fn write_row_to_pager(
        &self,
        row: &Row,
        cursor: Arc<RwLock<BTreeCursor>>,
        requires_seek: bool,
    ) -> Result<StateMachine<WriteRowStateMachine>> {
        let state_machine: StateMachine<WriteRowStateMachine> =
            StateMachine::<WriteRowStateMachine>::new(WriteRowStateMachine::new(
                row.clone(),
                cursor,
                requires_seek,
            ));

        Ok(state_machine)
    }

    pub fn delete_row_from_pager(
        &self,
        rowid: RowID,
        cursor: Arc<RwLock<BTreeCursor>>,
    ) -> Result<StateMachine<DeleteRowStateMachine>> {
        let state_machine: StateMachine<DeleteRowStateMachine> =
            StateMachine::<DeleteRowStateMachine>::new(DeleteRowStateMachine::new(rowid, cursor));

        Ok(state_machine)
    }

    /// Try to scan for row ids in the table.
    ///
    /// This function loads all row ids of a table if the rowids of table were not populated yet.
    /// TODO: This is quite expensive so we should try and load rowids in a lazy way.
    ///
    /// # Arguments
    ///
    pub fn maybe_initialize_table(&self, table_id: i64, pager: Arc<Pager>) -> Result<()> {
        tracing::trace!("scan_row_ids_for_table(table_id={})", table_id);

        // First, check if the table is already loaded.
        if self.loaded_tables.read().contains(&table_id) {
            return Ok(());
        }

        // Then, scan the disk B-tree to find existing rows
        self.scan_load_table(table_id, pager)?;

        self.mark_table_as_loaded(table_id);

        Ok(())
    }

    // Mark table as loaded
    pub fn mark_table_as_loaded(&self, table_id: i64) {
        self.loaded_tables.write().insert(table_id);
    }

    /// Scans the table and inserts the rows into the database.
    ///
    /// This is initialization step for a table, where we still don't have any rows so we need to insert them if there are.
    fn scan_load_table(&self, table_id: i64, pager: Arc<Pager>) -> Result<()> {
        let root_page = table_id;
        let mut cursor = BTreeCursor::new_table(
            None, // No MVCC cursor for scanning
            pager.clone(),
            root_page,
            1, // We'll adjust this as needed
        );
        loop {
            match cursor
                .rewind()
                .map_err(|e| LimboError::InternalError(e.to_string()))?
            {
                IOResult::Done(()) => break,
                IOResult::IO(io) => {
                    io.wait(pager.io.as_ref())?;
                    continue;
                }
            }
        }
        loop {
            let rowid_result = cursor
                .rowid()
                .map_err(|e| LimboError::InternalError(e.to_string()))?;
            let row_id = match rowid_result {
                IOResult::Done(Some(row_id)) => row_id,
                IOResult::Done(None) => break,
                IOResult::IO(io) => {
                    io.wait(pager.io.as_ref())?;
                    continue;
                }
            };
            'record: loop {
                match cursor.record()? {
                    IOResult::Done(Some(record)) => {
                        let id = RowID { table_id, row_id };
                        let column_count = record.column_count();
                        // We insert row with 0 timestamp, because it's the only version we have on initialization.
                        self.insert_version(
                            id,
                            RowVersion {
                                begin: TxTimestampOrID::Timestamp(0),
                                end: None,
                                row: Row::new(id, record.get_payload().to_vec(), column_count),
                            },
                        );
                        break 'record;
                    }
                    IOResult::Done(None) => break,
                    IOResult::IO(io) => {
                        io.wait(pager.io.as_ref())?;
                    } // FIXME: lazy me not wanting to do state machine right now
                }
            }

            // Move to next record
            'next: loop {
                match cursor.next()? {
                    IOResult::Done(has_next) => {
                        if !has_next {
                            break;
                        }
                        break 'next;
                    }
                    IOResult::IO(io) => {
                        io.wait(pager.io.as_ref())?;
                    } // FIXME: lazy me not wanting to do state machine right now
                }
            }
        }
        Ok(())
    }

    pub fn get_last_rowid(&self, table_id: i64) -> Option<i64> {
        let last_rowid = self
            .rows
            .upper_bound(Bound::Included(&RowID {
                table_id,
                row_id: i64::MAX,
            }))
            .map(|entry| Some(entry.key().row_id))
            .unwrap_or(None);
        last_rowid
    }

    pub fn needs_recover(&self) -> bool {
        self.storage.needs_recover()
    }

    pub fn mark_recovered(&self) {
        self.storage.mark_recovered();
    }

    pub fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.storage.get_logical_log_file()
    }

    pub fn recover_logical_log(&self, io: &Arc<dyn crate::IO>, pager: &Arc<Pager>) -> Result<()> {
        // Get lock, if we don't get it we will wait until recover finishes in another connection
        // and then return.
        let _recover_guard = self.recover_lock.write();
        if !self.storage.needs_recover() {
            // another connection completed recover
            return Ok(());
        }
        let file = self.get_logical_log_file();
        let mut reader = StreamingLogicalLogReader::new(file.clone());

        let c = reader.read_header()?;
        io.wait_for_completion(c)?;
        let tx_id = 0;
        self.begin_load_tx(pager.clone())?;
        loop {
            match reader.next_record(io).unwrap() {
                StreamingResult::InsertRow { row, rowid } => {
                    tracing::trace!("read {rowid:?}");
                    self.insert(tx_id, row)?;
                }
                StreamingResult::DeleteRow { rowid } => {
                    self.delete(tx_id, rowid)?;
                }
                StreamingResult::Eof => {
                    break;
                }
            }
        }
        self.commit_load_tx(tx_id);
        self.mark_recovered();
        Ok(())
    }
}

/// A write-write conflict happens when transaction T_current attempts to update a
/// row version that is:
/// a) currently being updated by an active transaction T_previous, or
/// b) was updated by an ended transaction T_previous that committed AFTER T_current started
/// but BEFORE T_previous commits.
///
/// "Suppose transaction T wants to update a version V. V is updatable
/// only if it is the latest version, that is, it has an end timestamp equal
/// to infinity or its End field contains the ID of a transaction TE and
/// TE’s state is Aborted"
/// Ref: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf , page 301,
/// 2.6. Updating a Version.
pub(crate) fn is_write_write_conflict(
    txs: &SkipMap<TxID, Transaction>,
    tx: &Transaction,
    rv: &RowVersion,
) -> bool {
    match rv.end {
        Some(TxTimestampOrID::TxID(rv_end)) => {
            let te = txs.get(&rv_end).unwrap();
            let te = te.value();
            if te.tx_id == tx.tx_id {
                return false;
            }
            te.state.load() != TransactionState::Aborted
        }
        // A non-"infinity" end timestamp (here modeled by Some(ts)) functions as a write lock
        // on the row, so it can never be updated by another transaction.
        // Ref: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf , page 301,
        // 2.6. Updating a Version.
        Some(TxTimestampOrID::Timestamp(_)) => true,
        None => false,
    }
}

impl RowVersion {
    pub fn is_visible_to(&self, tx: &Transaction, txs: &SkipMap<TxID, Transaction>) -> bool {
        is_begin_visible(txs, tx, self) && is_end_visible(txs, tx, self)
    }
}

fn is_begin_visible(txs: &SkipMap<TxID, Transaction>, tx: &Transaction, rv: &RowVersion) -> bool {
    match rv.begin {
        TxTimestampOrID::Timestamp(rv_begin_ts) => tx.begin_ts >= rv_begin_ts,
        TxTimestampOrID::TxID(rv_begin) => {
            let tb = txs.get(&rv_begin).unwrap();
            let tb = tb.value();
            let visible = match tb.state.load() {
                TransactionState::Active => tx.tx_id == tb.tx_id && rv.end.is_none(),
                TransactionState::Preparing => false, // NOTICE: makes sense for snapshot isolation, not so much for serializable!
                TransactionState::Committed(committed_ts) => tx.begin_ts >= committed_ts,
                TransactionState::Aborted => false,
                TransactionState::Terminated => {
                    tracing::debug!("TODO: should reread rv's end field - it should have updated the timestamp in the row version by now");
                    false
                }
            };
            tracing::trace!(
                "is_begin_visible: tx={tx}, tb={tb} rv = {:?}-{:?} visible = {visible}",
                rv.begin,
                rv.end
            );
            visible
        }
    }
}

fn is_end_visible(
    txs: &SkipMap<TxID, Transaction>,
    current_tx: &Transaction,
    row_version: &RowVersion,
) -> bool {
    match row_version.end {
        Some(TxTimestampOrID::Timestamp(rv_end_ts)) => current_tx.begin_ts < rv_end_ts,
        Some(TxTimestampOrID::TxID(rv_end)) => {
            let other_tx = txs
                .get(&rv_end)
                .unwrap_or_else(|| panic!("Transaction {rv_end} not found"));
            let other_tx = other_tx.value();
            let visible = match other_tx.state.load() {
                // V's sharp mind discovered an issue with the hekaton paper which basically states that a
                // transaction can see a row version if the end is a TXId only if it isn't the same transaction.
                // Source: https://avi.im/blag/2023/hekaton-paper-typo/
                TransactionState::Active => current_tx.tx_id != other_tx.tx_id,
                TransactionState::Preparing => false, // NOTICE: makes sense for snapshot isolation, not so much for serializable!
                TransactionState::Committed(committed_ts) => current_tx.begin_ts < committed_ts,
                TransactionState::Aborted => false,
                TransactionState::Terminated => {
                    tracing::debug!("TODO: should reread rv's end field - it should have updated the timestamp in the row version by now");
                    false
                }
            };
            tracing::trace!(
                "is_end_visible: tx={current_tx}, te={other_tx} rv = {:?}-{:?}  visible = {visible}",
                row_version.begin,
                row_version.end
            );
            visible
        }
        None => true,
    }
}
