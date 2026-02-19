use crate::mvcc::clock::LogicalClock;
use crate::mvcc::cursor::{static_iterator_hack, MvccIterator};
use crate::mvcc::persistent_storage::Storage;
use crate::schema::{Schema, Table};
use crate::state_machine::StateMachine;
use crate::state_machine::StateTransition;
use crate::state_machine::TransitionResult;
use crate::storage::btree::BTreeCursor;
use crate::storage::btree::BTreeKey;
use crate::storage::btree::CursorTrait;
use crate::storage::btree::CursorValidState;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::wal::{CheckpointMode, CheckpointResult, TursoRwLock};
use crate::sync::atomic::{AtomicBool, AtomicI64};
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Arc;
use crate::sync::{Mutex, RwLock};
use crate::translate::plan::IterationDirection;
use crate::types::compare_immutable;
use crate::types::IOCompletions;
use crate::types::IOResult;
use crate::types::ImmutableRecord;
use crate::types::IndexInfo;
use crate::types::SeekResult;
use crate::File;
use crate::IOExt;
use crate::LimboError;
use crate::Result;
use crate::ValueRef;
use crate::{
    contains_ignore_ascii_case, eq_ignore_ascii_case, match_ignore_ascii_case, Completion,
};
use crate::{
    turso_assert, turso_assert_eq, turso_assert_less_than, turso_assert_reachable, Numeric,
};
use crate::{Connection, Pager, SyncMode};
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::{SkipMap, SkipSet};
use rustc_hash::FxHashMap as HashMap;
use rustc_hash::FxHashSet as HashSet;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Bound;
use tracing::instrument;
use tracing::Level;

pub mod checkpoint_state_machine;
pub use checkpoint_state_machine::{CheckpointState, CheckpointStateMachine};
pub mod version_chain;
pub use version_chain::{RowVersionChain, RowVersionNode};

use super::persistent_storage::logical_log::{
    HeaderReadResult, StreamingLogicalLogReader, StreamingResult, LOG_HDR_SIZE,
};

#[cfg(test)]
pub mod hermitage_tests;
#[cfg(test)]
pub mod tests;

/// Sentinel value for `MvStore::exclusive_tx` indicating no exclusive transaction is active.
const NO_EXCLUSIVE_TX: u64 = 0;

/// A table ID for MVCC.
/// MVCC table IDs are always negative. Their corresponding rootpage entry in sqlite_schema
/// is the same negative value if the table has not been checkpointed yet. Otherwise, the root page
/// will be positive and corresponds to the actual physical page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct MVTableId(i64);

impl MVTableId {
    pub fn new(value: i64) -> Self {
        turso_assert_less_than!(value, 0, "MVCC table IDs are always negative");
        Self(value)
    }
}

impl From<i64> for MVTableId {
    fn from(value: i64) -> Self {
        turso_assert_less_than!(value, 0, "MVCC table IDs are always negative");
        Self(value)
    }
}

impl From<MVTableId> for i64 {
    fn from(value: MVTableId) -> Self {
        value.0
    }
}

impl std::fmt::Display for MVTableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MVTableId({})", self.0)
    }
}

/// Wrapper for index keys that implements collation-aware, ASC/DESC-aware ordering.
#[derive(Debug, Clone)]
pub struct SortableIndexKey {
    /// The key as bytes.
    pub key: ImmutableRecord,
    /// Index metadata containing sort orders and collations
    pub metadata: Arc<IndexInfo>,
}

impl SortableIndexKey {
    pub fn new_from_bytes(key_bytes: Vec<u8>, metadata: Arc<IndexInfo>) -> Self {
        Self {
            key: ImmutableRecord::from_bin_record(key_bytes),
            metadata,
        }
    }

    pub fn new_from_record(key: ImmutableRecord, metadata: Arc<IndexInfo>) -> Self {
        Self { key, metadata }
    }

    pub fn new_from_values(values: Vec<ValueRef>, metadata: Arc<IndexInfo>) -> Self {
        let len = values.len();
        Self {
            key: ImmutableRecord::from_values(values, len),
            metadata,
        }
    }

    fn compare(&self, other: &Self) -> Result<std::cmp::Ordering> {
        // We sometimes need to compare a shorter key to a longer one,
        // for example when seeking with an index key that is a prefix of the full key.
        let num_cols = self.metadata.num_cols.min(other.metadata.num_cols);

        let mut lhs = self.key.iter()?;
        let mut rhs = other.key.iter()?;

        for i in 0..num_cols {
            let lhs_value = lhs.next().expect("we already checked length")?;
            let rhs_value = rhs.next().expect("we already checked length")?;

            let cmp = compare_immutable(
                std::iter::once(&lhs_value),
                std::iter::once(&rhs_value),
                &self.metadata.key_info[i..i + 1],
            );

            if cmp != std::cmp::Ordering::Equal {
                return Ok(cmp);
            }
        }

        Ok(std::cmp::Ordering::Equal)
    }

    /// Check if the index key contains any NULL values (excluding the rowid column).
    /// In SQLite, NULLs don't violate UNIQUE constraints, so we skip conflict checks for NULL keys.
    pub fn contains_null(&self, num_indexed_cols: usize) -> Result<bool> {
        let mut iter = self.key.iter()?;
        // Only check the indexed columns, not the rowid at the end
        for _ in 0..num_indexed_cols {
            if let Some(value) = iter.next() {
                if matches!(value?, crate::types::ValueRef::Null) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Check if the first `num_cols` columns of this key match another key.
    /// Used for UNIQUE index conflict detection where we need to compare only
    /// the indexed columns, not the rowid suffix.
    pub fn matches_prefix(&self, other: &Self, num_cols: usize) -> Result<bool> {
        let mut lhs = self.key.iter()?;
        let mut rhs = other.key.iter()?;

        for i in 0..num_cols {
            let lhs_value = match lhs.next() {
                Some(v) => v?,
                None => return Ok(false),
            };
            let rhs_value = match rhs.next() {
                Some(v) => v?,
                None => return Ok(false),
            };

            let cmp = compare_immutable(
                std::iter::once(&lhs_value),
                std::iter::once(&rhs_value),
                &self.metadata.key_info[i..i + 1],
            );

            if cmp != std::cmp::Ordering::Equal {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

impl PartialEq for SortableIndexKey {
    fn eq(&self, other: &Self) -> bool {
        if self.key == other.key {
            return true;
        }

        self.compare(other)
            .map(|ord| ord == std::cmp::Ordering::Equal)
            .unwrap_or(false)
    }
}

impl Eq for SortableIndexKey {}

impl PartialOrd for SortableIndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortableIndexKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.compare(other).expect("Failed to compare IndexKeys")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RowKey {
    Int(i64),
    Record(SortableIndexKey),
}

impl RowKey {
    pub fn to_int_or_panic(&self) -> i64 {
        match self {
            RowKey::Int(row_id) => *row_id,
            _ => panic!("RowKey is not an integer"),
        }
    }

    pub fn is_int_key(&self) -> bool {
        matches!(self, RowKey::Int(_))
    }
}

impl std::fmt::Display for RowKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowKey::Int(row_id) => write!(f, "{row_id}"),
            RowKey::Record(record) => write!(f, "{record:?}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowID {
    /// The table ID. Analogous to table's root page number.
    pub table_id: MVTableId,
    pub row_id: RowKey,
}

impl RowID {
    pub fn new(table_id: MVTableId, row_id: RowKey) -> Self {
        Self { table_id, row_id }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]

pub struct Row {
    pub id: RowID,
    /// Data is None for index rows because the key holds all the data.
    pub data: Option<Vec<u8>>,
    pub column_count: usize,
}

impl Row {
    pub fn new_table_row(id: RowID, data: Vec<u8>, column_count: usize) -> Self {
        Self {
            id,
            data: Some(data),
            column_count,
        }
    }

    pub fn new_index_row(id: RowID, column_count: usize) -> Self {
        Self {
            id,
            data: None,
            column_count,
        }
    }

    pub fn is_index_row(&self) -> bool {
        self.data.is_none()
    }

    pub fn payload(&self) -> &[u8] {
        match self.id.row_id {
            RowKey::Int(_) => self.data.as_ref().expect("table rows should have data"),
            RowKey::Record(ref sortable_key) => sortable_key.key.as_blob(),
        }
    }
}

/// A row version.
/// TODO: we can optimize this by using bitpacking for the begin and end fields.
#[derive(Clone, Debug, PartialEq)]
pub struct RowVersion {
    /// Unique identifier for this version within the MvStore.
    /// Used for savepoint tracking to identify specific versions to rollback.
    pub id: u64,
    pub begin: Option<TxTimestampOrID>,
    pub end: Option<TxTimestampOrID>,
    pub row: Row,
    /// Indicates this version was created for a row that existed in B-tree before
    /// MVCC was enabled (e.g., after switching from WAL to MVCC journal mode).
    /// This flag helps the checkpoint logic determine if a delete should be
    /// checkpointed to the B-tree file.
    pub btree_resident: bool,
}

#[derive(Debug)]
pub enum RowVersionState {
    LiveVersion,
    NotFound,
    Deleted,
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

/// Tracks versions created/modified during a savepoint for rollback.
/// Used for statement-level savepoints in interactive transactions.
#[derive(Default, Debug)]
pub struct Savepoint {
    /// Versions CREATED during this savepoint (insert operations).
    /// On rollback: these versions are removed from their chains.
    created_table_versions: Vec<(RowID, u64)>,
    created_index_versions: Vec<((MVTableId, Arc<SortableIndexKey>), u64)>,
    /// Versions DELETED during this savepoint (end timestamp set).
    /// On rollback: clear end timestamp to restore visibility.
    deleted_table_versions: Vec<(RowID, u64)>,
    deleted_index_versions: Vec<((MVTableId, Arc<SortableIndexKey>), u64)>,
    /// RowIDs that were NEWLY added to write_set by this savepoint.
    /// On rollback: only these should be removed from write_set.
    newly_added_to_write_set: Vec<RowID>,
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
    /// Stack of savepoints for statement-level rollback.
    /// Each savepoint tracks versions created/deleted during that statement.
    savepoint_stack: RwLock<Vec<Savepoint>>,
    /// True when this transaction currently holds the serialized logical-log commit lock.
    pager_commit_lock_held: AtomicBool,
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
            savepoint_stack: RwLock::new(Vec::new()),
            pager_commit_lock_held: AtomicBool::new(false),
        }
    }

    fn insert_to_read_set(&self, id: RowID) {
        self.read_set.insert(id);
    }

    fn insert_to_write_set(&self, id: RowID) {
        // Check if this is a new addition to write_set
        let is_new = !self.write_set.contains(&id);
        self.write_set.insert(id.clone());
        // If new, record in the current savepoint so we can remove on rollback
        if is_new {
            if let Some(savepoint) = self.savepoint_stack.write().last_mut() {
                savepoint.newly_added_to_write_set.push(id);
            }
        }
    }

    /// Begin a new savepoint for statement-level tracking.
    fn begin_savepoint(&self) {
        let depth = self.savepoint_stack.read().len();
        tracing::debug!("begin_savepoint(tx_id={}, depth={})", self.tx_id, depth);
        self.savepoint_stack.write().push(Savepoint::default());
    }

    /// Release the newest savepoint (statement completed successfully).
    fn release_savepoint(&self) {
        let depth = self.savepoint_stack.read().len();
        tracing::debug!("release_savepoint(tx_id={}, depth={})", self.tx_id, depth);
        self.savepoint_stack.write().pop();
    }

    /// Pop and return the newest savepoint for rollback.
    fn pop_savepoint(&self) -> Option<Savepoint> {
        self.savepoint_stack.write().pop()
    }

    /// Record a version that was created during the current savepoint.
    fn record_created_table_version(&self, rowid: RowID, version_id: u64) {
        if let Some(savepoint) = self.savepoint_stack.write().last_mut() {
            tracing::debug!(
                "record_created_table_version(tx_id={}, table_id={}, row_id={}, version_id={})",
                self.tx_id,
                rowid.table_id,
                rowid.row_id,
                version_id
            );
            savepoint.created_table_versions.push((rowid, version_id));
        }
    }

    /// Record an index version that was created during the current savepoint.
    fn record_created_index_version(
        &self,
        key: (MVTableId, Arc<SortableIndexKey>),
        version_id: u64,
    ) {
        if let Some(savepoint) = self.savepoint_stack.write().last_mut() {
            tracing::debug!(
                "record_created_index_version(tx_id={}, table_id={}, version_id={})",
                self.tx_id,
                key.0,
                version_id
            );
            savepoint.created_index_versions.push((key, version_id));
        }
    }

    /// Record a version that was deleted during the current savepoint.
    fn record_deleted_table_version(&self, rowid: RowID, version_id: u64) {
        if let Some(savepoint) = self.savepoint_stack.write().last_mut() {
            tracing::debug!(
                "record_deleted_table_version(tx_id={}, table_id={}, row_id={}, version_id={})",
                self.tx_id,
                rowid.table_id,
                rowid.row_id,
                version_id
            );
            savepoint.deleted_table_versions.push((rowid, version_id));
        }
    }

    /// Record an index version that was deleted during the current savepoint.
    fn record_deleted_index_version(
        &self,
        key: (MVTableId, Arc<SortableIndexKey>),
        version_id: u64,
    ) {
        if let Some(savepoint) = self.savepoint_stack.write().last_mut() {
            tracing::debug!(
                "record_deleted_index_version(tx_id={}, table_id={}, version_id={})",
                self.tx_id,
                key.0,
                version_id
            );
            savepoint.deleted_index_versions.push((key, version_id));
        }
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
    /// Preparing state includes the end_ts so other transactions can compare
    /// timestamps during validation to resolve races (first-committer-wins).
    Preparing(u64),
    Aborted,
    Terminated,
    Committed(u64),
}

impl TransactionState {
    // Bit patterns for encoding states with timestamps
    const PREPARING_BIT: u64 = 0x4000_0000_0000_0000;
    const COMMITTED_BIT: u64 = 0x8000_0000_0000_0000;
    const TIMESTAMP_MASK: u64 = 0x3fff_ffff_ffff_ffff;

    pub fn encode(&self) -> u64 {
        match self {
            TransactionState::Active => 0,
            TransactionState::Preparing(ts) => {
                // We only support 2^62 - 1 timestamps
                assert!(ts & !Self::TIMESTAMP_MASK == 0);
                Self::PREPARING_BIT | ts
            }
            TransactionState::Aborted => 1,
            TransactionState::Terminated => 2,
            TransactionState::Committed(ts) => {
                // We only support 2^62 - 1 timestamps
                turso_assert_eq!(ts & !Self::TIMESTAMP_MASK, 0);
                Self::COMMITTED_BIT | ts
            }
        }
    }

    pub fn decode(v: u64) -> Self {
        match v {
            0 => TransactionState::Active,
            1 => TransactionState::Aborted,
            2 => TransactionState::Terminated,
            v if v & Self::COMMITTED_BIT != 0 => {
                TransactionState::Committed(v & Self::TIMESTAMP_MASK)
            }
            v if v & Self::PREPARING_BIT != 0 => {
                TransactionState::Preparing(v & Self::TIMESTAMP_MASK)
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
            TransactionState::Preparing(ts) => write!(f, "Preparing({ts})"),
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

#[allow(clippy::large_enum_variant)]
pub enum CommitState<Clock: LogicalClock> {
    Initial,
    Commit {
        end_ts: u64,
    },
    BeginCommitLogicalLog {
        end_ts: u64,
        log_record: LogRecord,
    },
    EndCommitLogicalLog {
        end_ts: u64,
    },
    SyncLogicalLog {
        end_ts: u64,
    },
    Checkpoint {
        // TODO: if and when we transform this code to async we won't be needing this explicit state machine nor
        // the mutex
        state_machine: Mutex<StateMachine<CheckpointStateMachine<Clock>>>,
    },
    CommitEnd {
        end_ts: u64,
    },
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
    state: CommitState<Clock>,
    is_finalized: bool,
    did_commit_schema_change: bool,
    tx_id: TxID,
    connection: Arc<Connection>,
    /// Write set sorted by table id and row id
    write_set: Vec<RowID>,
    commit_coordinator: Arc<CommitCoordinator>,
    header: Arc<RwLock<Option<DatabaseHeader>>>,
    pager: Arc<Pager>,
    /// Bytes appended to the logical log for this commit; applied to writer offset only after durability and before lock release.
    pending_log_append_bytes: Option<u64>,
    /// The synchronous mode for fsync operations. When set to Off, fsync is skipped.
    sync_mode: SyncMode,
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
    /// After seek returns TryAdvance (key found in interior node, not leaf),
    /// advance the cursor to position it on the interior cell.
    Advance,
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
        state: CommitState<Clock>,
        tx_id: TxID,
        connection: Arc<Connection>,
        commit_coordinator: Arc<CommitCoordinator>,
        header: Arc<RwLock<Option<DatabaseHeader>>>,
        sync_mode: SyncMode,
    ) -> Self {
        let pager = connection.pager.load().clone();
        Self {
            state,
            is_finalized: false,
            did_commit_schema_change: false,
            tx_id,
            connection,
            write_set: Vec::new(),
            commit_coordinator,
            pager,
            header,
            pending_log_append_bytes: None,
            sync_mode,
            _phantom: PhantomData,
        }
    }

    fn check_rowid_for_conflicts(
        &self,
        rowid: &RowID,
        end_ts: u64,
        tx: &Transaction,
        mvcc_store: &Arc<MvStore<Clock>>,
    ) -> Result<()> {
        let row_versions = mvcc_store.rows.get(rowid);
        if row_versions.is_none() {
            return Ok(());
        }

        let chain_entry = row_versions.unwrap();
        let chain = chain_entry.value();

        self.check_chain_conflicts(end_ts, tx, mvcc_store, chain)?;
        Ok(())
    }

    fn check_index_for_conflicts(
        &self,
        rowid: &RowID,
        end_ts: u64,
        tx: &Transaction,
        mvcc_store: &Arc<MvStore<Clock>>,
    ) -> Result<()> {
        let RowKey::Record(record) = &rowid.row_id else {
            panic!("invalid index row_id type, should be Record")
        };
        if !record.metadata.is_unique {
            // Skip indexes which are not unique or not primary key
            return Ok(());
        }
        // In SQLite, NULLs don't violate UNIQUE constraints - skip conflict check for keys containing NULL
        let num_indexed_cols = record.metadata.num_cols.saturating_sub(1); // exclude rowid column
        if record.contains_null(num_indexed_cols)? {
            return Ok(());
        }

        // Create a prefix key with num_cols - 1 for range lookup.
        // Due to SortableIndexKey's Ord using min(num_cols), this key compares Equal
        // to all entries with the same indexed columns (regardless of rowid).
        let prefix_key = {
            let mut index_info = record.metadata.as_ref().clone();
            turso_assert!(index_info.has_rowid, "not supported yet without rowid");
            index_info.num_cols -= 1;
            SortableIndexKey {
                key: record.key.clone(),
                metadata: Arc::new(index_info),
            }
        };

        let table_id = rowid.table_id;
        let index_rows = mvcc_store
            .index_rows
            .get(&table_id)
            .unwrap_or_else(|| panic!("expected index {table_id:?}"));
        let index_rows = index_rows.value();

        // Use range to efficiently find all entries that match the prefix.
        // Since entries are ordered by Ord, all entries with the same indexed columns
        // are contiguous. We start from the prefix_key and stop when prefix no longer matches.
        for entry in index_rows.range::<SortableIndexKey, _>(&prefix_key..) {
            let other_key = entry.key();
            // Check if prefix still matches - if not, we've passed all matching entries
            if !record.matches_prefix(other_key, num_indexed_cols)? {
                break;
            }
            let chain = entry.value();
            self.check_chain_conflicts(end_ts, tx, mvcc_store, chain)?;
        }

        Ok(())
    }

    fn check_chain_conflicts(
        &self,
        end_ts: u64,
        tx: &Transaction,
        mvcc_store: &Arc<MvStore<Clock>>,
        chain: &RowVersionChain,
    ) -> Result<()> {
        // Chain is newest-first, so iterating forward is equivalent to reverse
        for node in chain.iter() {
            if node.is_garbage() {
                continue;
            }
            let begin = node.begin();
            let end = node.end();

            if let Some(TxTimestampOrID::Timestamp(end_ts_val)) = end {
                if end_ts_val >= tx.begin_ts {
                    return Err(LimboError::WriteWriteConflict);
                }
            }

            if end.is_some() {
                continue;
            }

            match begin {
                Some(TxTimestampOrID::TxID(other_tx_id)) => {
                    if other_tx_id == self.tx_id {
                        continue;
                    }
                    let other_tx = mvcc_store.txs.get(&other_tx_id);
                    if let Some(other_tx) = other_tx {
                        let other_tx = other_tx.value();
                        match other_tx.state.load() {
                            TransactionState::Committed(_) => {
                                return Err(LimboError::WriteWriteConflict);
                            }
                            TransactionState::Preparing(other_end_ts) => {
                                if other_end_ts < end_ts {
                                    return Err(LimboError::WriteWriteConflict);
                                }
                            }
                            TransactionState::Active => {}
                            TransactionState::Aborted | TransactionState::Terminated => {}
                        }
                    }
                }
                Some(TxTimestampOrID::Timestamp(_)) => {
                    return Err(LimboError::WriteWriteConflict);
                }
                None => {}
            }
        }
        Ok(())
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
    type Context = Arc<MvStore<Clock>>;
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
                        turso_assert_eq!(tx.state, TransactionState::Active);
                    }
                }

                if mvcc_store
                    .last_committed_schema_change_ts
                    .load(Ordering::Acquire)
                    > tx.begin_ts
                {
                    // Schema changes made after the transaction began always cause a [SchemaConflict] error and the tx must abort.
                    return Err(LimboError::SchemaConflict);
                }

                tx.state.store(TransactionState::Preparing(end_ts));
                tracing::trace!("prepare_tx(tx_id={}, end_ts={})", self.tx_id, end_ts);

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
                    .extend(tx.write_set.iter().map(|v| v.value().clone()));
                self.write_set.sort_by(|a, b| {
                    // table ids are negative, and sqlite_schema has id -1 so we want to sort in descending order of table id
                    b.table_id.cmp(&a.table_id).then(a.row_id.cmp(&b.row_id))
                });
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
                if !mvcc_store.is_exclusive_tx(&self.tx_id) && mvcc_store.has_exclusive_tx() {
                    // A non-CONCURRENT transaction is holding the exclusive lock, we must abort.
                    turso_assert_reachable!("commit aborted due to exclusive tx conflict");
                    return Err(LimboError::WriteWriteConflict);
                }
                // Check for rowid conflicts before committing (pure optimistic, first-committer-wins)
                // Ref: Hekaton paper Section 3.2 - validation uses end_ts comparison
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or(LimboError::TxTerminated)?;
                let tx = tx.value();

                for id in &self.write_set {
                    if id.row_id.is_int_key() {
                        self.check_rowid_for_conflicts(id, *end_ts, tx, mvcc_store)?;
                    } else {
                        self.check_index_for_conflicts(id, *end_ts, tx, mvcc_store)?;
                    }
                }

                // Now update timestamps
                let mut log_record = LogRecord::new(*end_ts);
                for id in &self.write_set {
                    if let Some(chain_entry) = mvcc_store.rows.get(id) {
                        let chain = chain_entry.value();
                        for node in chain.iter() {
                            if let Some(TxTimestampOrID::TxID(id)) = node.begin() {
                                if id == self.tx_id {
                                    // New version is valid STARTING FROM committing transaction's end timestamp
                                    // See diagram on page 299: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf
                                    node.set_begin(Some(TxTimestampOrID::Timestamp(*end_ts)));
                                    mvcc_store.insert_version_raw(
                                        &mut log_record.row_versions,
                                        node.to_row_version(),
                                    );

                                    if node.row.id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
                                        self.did_commit_schema_change = true;
                                    }
                                }
                            }
                            if let Some(TxTimestampOrID::TxID(id)) = node.end() {
                                if id == self.tx_id {
                                    // Old version is valid UNTIL committing transaction's end timestamp
                                    // See diagram on page 299: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf
                                    node.set_end(Some(TxTimestampOrID::Timestamp(*end_ts)));
                                    mvcc_store.insert_version_raw(
                                        &mut log_record.row_versions,
                                        node.to_row_version(),
                                    );

                                    if node.row.id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
                                        self.did_commit_schema_change = true;
                                    }
                                }
                            }
                        }
                    }
                    if let Some(index) = mvcc_store.index_rows.get(&id.table_id) {
                        let index = index.value();
                        let RowKey::Record(ref index_key) = id.row_id else {
                            panic!("Index writes must have a record key");
                        };
                        if let Some(chain_entry) = index.get(index_key) {
                            let chain = chain_entry.value();
                            for node in chain.iter() {
                                if let Some(TxTimestampOrID::TxID(id)) = node.begin() {
                                    if id == self.tx_id {
                                        // New version is valid STARTING FROM committing transaction's end timestamp
                                        // See diagram on page 299: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf
                                        node.set_begin(Some(TxTimestampOrID::Timestamp(*end_ts)));
                                        mvcc_store.insert_version_raw(
                                            &mut log_record.row_versions,
                                            node.to_row_version(),
                                        );
                                    }
                                }
                                if let Some(TxTimestampOrID::TxID(id)) = node.end() {
                                    if id == self.tx_id {
                                        // Old version is valid UNTIL committing transaction's end timestamp
                                        // See diagram on page 299: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf
                                        node.set_end(Some(TxTimestampOrID::Timestamp(*end_ts)));
                                        mvcc_store.insert_version_raw(
                                            &mut log_record.row_versions,
                                            node.to_row_version(),
                                        );
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
                            Completion::new_yield(),
                        )));
                    }
                    let tx = mvcc_store
                        .txs
                        .get(&self.tx_id)
                        .ok_or_else(|| LimboError::NoSuchTransactionID(self.tx_id.to_string()))?;
                    tx.value()
                        .pager_commit_lock_held
                        .store(true, Ordering::Release);
                }
                let (c, append_bytes) = mvcc_store.storage.log_tx(log_record)?;
                self.pending_log_append_bytes = Some(append_bytes);
                self.state = CommitState::SyncLogicalLog { end_ts: *end_ts };
                // if Completion Completed without errors we can continue
                if c.succeeded() {
                    Ok(TransitionResult::Continue)
                } else {
                    Ok(TransitionResult::Io(IOCompletions::Single(c)))
                }
            }

            CommitState::SyncLogicalLog { end_ts } => {
                // Skip fsync when synchronous mode is not FULL.
                // NORMAL mode skips fsync on commit (but still fsyncs on checkpoint).
                if self.sync_mode != SyncMode::Full {
                    tracing::debug!("Skipping fsync of logical log (synchronous!=full)");
                    self.state = CommitState::EndCommitLogicalLog { end_ts: *end_ts };
                    return Ok(TransitionResult::Continue);
                }
                let c = mvcc_store.storage.sync(self.pager.get_sync_type())?;
                self.state = CommitState::EndCommitLogicalLog { end_ts: *end_ts };
                // if Completion Completed without errors we can continue
                if c.succeeded() {
                    Ok(TransitionResult::Continue)
                } else {
                    Ok(TransitionResult::Io(IOCompletions::Single(c)))
                }
            }
            CommitState::EndCommitLogicalLog { end_ts } => {
                let connection = self.connection.clone();
                let schema_did_change = self.did_commit_schema_change;
                if schema_did_change {
                    let schema = connection.schema.read().clone();
                    connection.db.update_schema_if_newer(schema);
                }
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or_else(|| LimboError::NoSuchTransactionID(self.tx_id.to_string()))?;
                let tx_unlocked = tx.value();
                self.header.write().replace(*tx_unlocked.header.read());
                tracing::trace!("end_commit_logical_log(tx_id={})", self.tx_id);
                self.state = CommitState::CommitEnd { end_ts: *end_ts };
                return Ok(TransitionResult::Continue);
            }
            CommitState::CommitEnd { end_ts } => {
                // Order of operations matters here:
                // 1. Advance logical log writer offset (makes the written bytes "owned")
                // 2. Mark transaction Committed (publishes versions to readers)
                // 3. Release commit lock (allows next committer)
                // 4. Update cached global header
                //
                // (1) must precede (3): the commit lock serializes log writes, and
                // log_tx() writes at the current offset. If we released the lock before
                // advancing, the next committer would overwrite our bytes.
                //
                // (2) must precede (3): the next committer's validation (CommitState::Commit)
                // checks our transaction state. If it still sees Preparing instead of
                // Committed, the tie-breaking logic (lower end_ts wins) applies instead
                // of the definitive "already committed = conflict" path.
                //
                // pending_log_append_bytes is set in BeginCommitLogicalLog after log_tx
                // writes to disk. If the commit fails before reaching here (e.g. during
                // sync), the bytes are never consumed and the in-memory writer offset
                // stays behind — the next write overwrites the uncommitted bytes.
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or_else(|| LimboError::NoSuchTransactionID(self.tx_id.to_string()))?;
                let tx_unlocked = tx.value();
                if let Some(append_bytes) = self.pending_log_append_bytes.take() {
                    mvcc_store
                        .storage
                        .advance_logical_log_offset_after_success(append_bytes);
                }
                tx_unlocked
                    .state
                    .store(TransactionState::Committed(*end_ts));
                mvcc_store.unlock_commit_lock_if_held(tx_unlocked);
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
                if mvcc_store.storage.should_checkpoint() {
                    let state_machine = StateMachine::new(CheckpointStateMachine::new(
                        self.pager.clone(),
                        mvcc_store.clone(),
                        self.connection.clone(),
                        false,
                        self.connection.get_sync_mode(),
                    ));
                    let state_machine = Mutex::new(state_machine);
                    self.state = CommitState::Checkpoint { state_machine };
                    return Ok(TransitionResult::Continue);
                }
                tracing::trace!("logged(tx_id={}, end_ts={})", self.tx_id, *end_ts);
                self.finalize(mvcc_store)?;
                Ok(TransitionResult::Done(()))
            }
            CommitState::Checkpoint { state_machine } => {
                let step_result = {
                    let mut sm = state_machine.lock();
                    sm.step(&())
                };
                match step_result {
                    Ok(IOResult::Done(_)) => {}
                    Ok(IOResult::IO(iocompletions)) => {
                        return Ok(TransitionResult::Io(iocompletions));
                    }
                    Err(err) => {
                        // Auto-checkpoint errors should not surface to the committed statement.
                        tracing::debug!("MVCC auto-checkpoint failed: {err}");
                        self.finalize(mvcc_store)?;
                        return Ok(TransitionResult::Done(()));
                    }
                }
                self.finalize(mvcc_store)?;
                return Ok(TransitionResult::Done(()));
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
                self.record = if self.row.is_index_row() {
                    None
                } else {
                    let row_data = self.row.data.as_ref().expect("table rows should have data");
                    let mut record = ImmutableRecord::new(row_data.len());
                    record.start_serialization(row_data);
                    Some(record)
                };
                if self.requires_seek {
                    self.state = WriteRowState::Seek;
                } else {
                    self.state = WriteRowState::Insert;
                }
                Ok(TransitionResult::Continue)
            }
            WriteRowState::Seek => {
                // Position the cursor by seeking to the row position
                let seek_key = match &self.row.id.row_id {
                    RowKey::Int(row_id) => SeekKey::TableRowId(*row_id),
                    RowKey::Record(record) => SeekKey::IndexKey(&record.key),
                };

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
                turso_assert_eq!(self.cursor.write().valid_state, CursorValidState::Valid);
                self.state = WriteRowState::Insert;
                Ok(TransitionResult::Continue)
            }
            WriteRowState::Insert => {
                // Insert the record into the B-tree
                let key = match &self.row.id.row_id {
                    RowKey::Int(row_id) => BTreeKey::new_table_rowid(*row_id, self.record.as_ref()),
                    RowKey::Record(record) => BTreeKey::new_index_key(&record.key),
                };

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
                let seek_key = match &self.rowid.row_id {
                    RowKey::Int(row_id) => SeekKey::TableRowId(*row_id),
                    RowKey::Record(record) => SeekKey::IndexKey(&record.key),
                };

                match self
                    .cursor
                    .write()
                    .seek(seek_key, SeekOp::GE { eq_only: true })?
                {
                    IOResult::Done(seek_res) => {
                        match seek_res {
                            SeekResult::Found => {
                                self.state = DeleteRowState::Delete;
                            }
                            SeekResult::TryAdvance => {
                                // In index B-trees, the key can reside in an interior node
                                // rather than a leaf. The seek descends to the leaf but
                                // doesn't find it there, returning TryAdvance. Advancing
                                // the cursor will move up to the interior cell.
                                self.state = DeleteRowState::Advance;
                            }
                            SeekResult::NotFound => {
                                crate::bail_corrupt_error!(
                                    "MVCC delete: rowid {} not found",
                                    self.rowid.row_id
                                );
                            }
                        }
                        Ok(TransitionResult::Continue)
                    }
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
            }
            DeleteRowState::Advance => {
                let next_result = self.cursor.write().next()?;
                match next_result {
                    IOResult::Done(()) => {
                        if !self.cursor.read().has_record() {
                            crate::bail_corrupt_error!(
                                "MVCC delete: rowid {} not found after advance",
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

pub const SQLITE_SCHEMA_MVCC_TABLE_ID: MVTableId = MVTableId(-1);
pub(crate) const MVCC_META_TABLE_NAME: &str = "__turso_internal_mvcc_meta";
/// Indicates the maximum transaction timestamp that has been made durable in the WAL.
/// Used to determine the replay boundary for recovery; only records with a higher timestamp
/// are replayed.
pub(crate) const MVCC_META_KEY_PERSISTENT_TX_TS_MAX: &str = "persistent_tx_ts_max";

#[derive(Debug)]
pub struct RowidAllocator {
    /// Lock to ensure that only one thread can initialize the rowid allocator at a time.
    /// In case of unitialized values, we will look for last rowid first.
    lock: TursoRwLock,
    /// last_rowid is the last rowid that was allocated.
    max_rowid: RwLock<Option<i64>>,
    initialized: AtomicBool,
}

/// A multi-version concurrency control database.
#[derive(Debug)]
pub struct MvStore<Clock: LogicalClock> {
    pub rows: SkipMap<RowID, RowVersionChain>,
    /// Table ID is an opaque identifier that is only meaningful to the MV store.
    /// Each checkpointed MVCC table corresponds to a single B-tree on the pager,
    /// which naturally has a root page.
    /// We cannot use root page as the MVCC table ID directly because:
    /// - We assign table IDs during MVCC commit, but
    /// - we commit pages to the pager only during checkpoint
    ///
    /// which means the root page is not easily knowable ahead of time.
    /// Hence, we store the mapping here.
    /// The value is Option because tables created in an MVCC commit that have not
    /// been checkpointed yet have no real root page assigned yet.
    pub table_id_to_rootpage: SkipMap<MVTableId, Option<u64>>,
    /// Unlike table rows which are stored in a single map, we have a separate map for every index
    /// because operations like last() on an index are much easier when we don't have to take the
    /// table identifier into account.
    pub index_rows: SkipMap<MVTableId, SkipMap<Arc<SortableIndexKey>, RowVersionChain>>,
    txs: SkipMap<TxID, Transaction>,
    tx_ids: AtomicU64,
    version_id_counter: AtomicU64,
    next_rowid: AtomicU64,
    next_table_id: AtomicI64,
    clock: Clock,
    storage: Storage,

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
    /// The highest transaction ID that has been made durable in the WAL.
    /// Used to skip checkpointing transactions from mv store to WAL that have already been processed.
    durable_txid_max: AtomicU64,
    /// The timestamp of the last committed schema change.
    /// Schema changes always cause a [SchemaUpdated] error.
    last_committed_schema_change_ts: AtomicU64,
    /// The timestamp of the last committed transaction.
    /// If there are two concurrent BEGIN (non-CONCURRENT) transactions, and one tries to promote
    /// to exclusive, it will abort if another transaction committed after its begin timestamp.
    last_committed_tx_ts: AtomicU64,
    table_id_to_last_rowid: RwLock<HashMap<MVTableId, Arc<RowidAllocator>>>,
}

impl<Clock: LogicalClock> MvStore<Clock> {
    fn uses_durable_mvcc_metadata(&self, connection: &Arc<Connection>) -> bool {
        !connection.db.path.starts_with(":memory:")
    }

    /// Captures table-valued functions (e.g. generate_series) from the schema before
    /// reparse_schema() drops them. Built-in TVFs are registered programmatically and
    /// don't survive schema re-parsing from sqlite_schema; we save and re-inject them.
    fn capture_table_valued_functions(schema: &Schema) -> Vec<Arc<crate::vtab::VirtualTable>> {
        schema
            .tables
            .values()
            .filter_map(|table| match table.as_ref() {
                Table::Virtual(vtab)
                    if matches!(vtab.kind, turso_ext::VTabKind::TableValuedFunction) =>
                {
                    Some(vtab.clone())
                }
                _ => None,
            })
            .collect()
    }

    fn rehydrate_table_valued_functions(
        schema: &mut Schema,
        table_valued_functions: &[Arc<crate::vtab::VirtualTable>],
    ) {
        for vtab in table_valued_functions {
            let normalized_name = crate::util::normalize_ident(&vtab.name);
            schema
                .tables
                .entry(normalized_name)
                .or_insert_with(|| Arc::new(Table::Virtual(vtab.clone())));
        }
    }

    fn rehydrate_connection_table_valued_functions(
        &self,
        connection: &Arc<Connection>,
        table_valued_functions: &[Arc<crate::vtab::VirtualTable>],
    ) {
        connection.with_schema_mut(|schema| {
            Self::rehydrate_table_valued_functions(schema, table_valued_functions);
        });
        *connection.db.schema.lock() = connection.schema.read().clone();
    }

    /// Creates a new database.
    pub fn new(clock: Clock, storage: Storage) -> Self {
        Self {
            rows: SkipMap::new(),
            table_id_to_rootpage: SkipMap::from_iter(vec![(SQLITE_SCHEMA_MVCC_TABLE_ID, Some(1))]), // table id 1 / root page 1 is always sqlite_schema.
            index_rows: SkipMap::new(),
            txs: SkipMap::new(),
            tx_ids: AtomicU64::new(1), // let's reserve transaction 0 for special purposes
            version_id_counter: AtomicU64::new(1), // Reserve 0 for special purposes
            next_rowid: AtomicU64::new(0), // TODO: determine this from B-Tree
            next_table_id: AtomicI64::new(-2), // table id -1 / root page 1 is always sqlite_schema.
            clock,
            storage,
            exclusive_tx: AtomicU64::new(NO_EXCLUSIVE_TX),
            commit_coordinator: Arc::new(CommitCoordinator {
                pager_commit_lock: Arc::new(TursoRwLock::new()),
            }),
            global_header: Arc::new(RwLock::new(None)),
            blocking_checkpoint_lock: Arc::new(TursoRwLock::new()),
            durable_txid_max: AtomicU64::new(0),
            last_committed_schema_change_ts: AtomicU64::new(0),
            last_committed_tx_ts: AtomicU64::new(0),
            table_id_to_last_rowid: RwLock::new(HashMap::default()),
        }
    }

    /// Get the table ID from the root page.
    /// If the root page is negative, it is a non-checkpointed table and the table ID and root page are both the same negative value.
    /// If the root page is positive, it is a checkpointed table and there should be a corresponding table ID.
    pub fn get_table_id_from_root_page(&self, root_page: i64) -> MVTableId {
        if root_page < 0 {
            // Not checkpointed table - table ID and root_page are both the same negative value
            root_page.into()
        } else {
            // Root page is positive: it is a checkpointed table and there should be a corresponding table ID
            let root_page = root_page as u64;
            let table_id = self
                .table_id_to_rootpage
                .iter()
                .find(|entry| entry.value().is_some_and(|value| value == root_page))
                .map(|entry| *entry.key())
                .unwrap_or_else(|| {
                    panic!("Positive root page is not mapped to a table id: {root_page}")
                });
            table_id
        }
    }

    /// Insert a table ID and root page mapping.
    /// Root page must be positive here, because we only invoke this method with Some() for checkpointed tables.
    pub fn insert_table_id_to_rootpage(&self, table_id: MVTableId, root_page: Option<u64>) {
        self.table_id_to_rootpage.insert(table_id, root_page);
        let minimum: i64 = if let Some(root_page) = root_page {
            // On recovery, we assign table_id = -root_page. Let's make sure we don't get any clashes between checkpointed and non-checkpointed tables
            // E.g. if we checkpoint a table that has physical root page 7, let's require the next table_id to be less than -7 (or if table_id is already smaller, then smaller than that.)
            let root_page_as_table_id = MVTableId::from(-(root_page as i64));
            table_id.min(root_page_as_table_id).into()
        } else {
            table_id.into()
        };
        if minimum <= self.next_table_id.load(Ordering::SeqCst) {
            self.next_table_id.store(minimum - 1, Ordering::SeqCst);
        }
    }

    /// Creates the `__turso_internal_mvcc_meta` table and seeds it with
    /// `persistent_tx_ts_max` (initialized to 0). This table stores the durable replay
    /// boundary: on recovery, only logical-log frames with `commit_ts > persistent_tx_ts_max`
    /// are replayed. Called once during first MVCC bootstrap.
    fn initialize_mvcc_metadata_table(&self, connection: &Arc<Connection>) -> Result<()> {
        connection.execute(format!(
            "CREATE TABLE IF NOT EXISTS {MVCC_META_TABLE_NAME}(k TEXT, v INTEGER NOT NULL)"
        ))?;
        connection.execute(format!(
            "INSERT OR IGNORE INTO {MVCC_META_TABLE_NAME}(rowid, k, v) VALUES (1, '{MVCC_META_KEY_PERSISTENT_TX_TS_MAX}', 0)"
        ))?;
        Ok(())
    }

    /// Read the persistent transaction timestamp maximum from the MVCC metadata table.
    fn try_read_persistent_tx_ts_max(&self, connection: &Arc<Connection>) -> Result<Option<u64>> {
        let query_result = connection.query(format!(
            "SELECT v FROM {MVCC_META_TABLE_NAME}
             WHERE k = '{MVCC_META_KEY_PERSISTENT_TX_TS_MAX}'"
        ));
        let maybe_stmt = match query_result {
            Ok(stmt) => stmt,
            Err(LimboError::ParseError(msg)) if msg.contains("no such table") => return Ok(None),
            Err(err) => {
                return Err(LimboError::Corrupt(format!(
                    "Failed to read MVCC metadata table: {err}"
                )))
            }
        };
        let mut value: Option<i64> = None;
        if let Some(mut stmt) = maybe_stmt {
            stmt.run_with_row_callback(|row| {
                value = Some(row.get::<i64>(0)?);
                Ok(())
            })?;
        }

        let value = value.ok_or_else(|| {
            LimboError::Corrupt(format!(
                "Missing MVCC metadata row for key {MVCC_META_KEY_PERSISTENT_TX_TS_MAX}"
            ))
        })?;

        if value < 0 {
            return Err(LimboError::Corrupt(format!(
                "Invalid MVCC metadata value for {MVCC_META_KEY_PERSISTENT_TX_TS_MAX}: {value}"
            )));
        }
        Ok(Some(value as u64))
    }

    /// Bootstrap the MV store from the SQLite schema table and logical log.
    /// 1. Get all root pages from the already parsed schema object
    /// 2. Assign table IDs to the root pages (table_id = -1 * root_page)
    /// 3. Complete interrupted WAL/log checkpoint reconciliation, if needed
    /// 4. Promote the bootstrap connection to a regular connection so that it reads from the MV store again
    /// 5. Recover the logical log
    /// 6. Make sure schema changes reflected from deserialized logical log are captured in the schema
    pub fn bootstrap(&self, bootstrap_conn: Arc<Connection>) -> Result<()> {
        let preserved_table_valued_functions =
            Self::capture_table_valued_functions(&bootstrap_conn.schema.read());
        self.maybe_complete_interrupted_checkpoint(&bootstrap_conn)?;
        bootstrap_conn.reparse_schema()?;
        self.rehydrate_connection_table_valued_functions(
            &bootstrap_conn,
            &preserved_table_valued_functions,
        );

        if self.uses_durable_mvcc_metadata(&bootstrap_conn) {
            match self.try_read_persistent_tx_ts_max(&bootstrap_conn)? {
                Some(_) => {}
                None => {
                    let log_size = self.get_logical_log_file().size()?;
                    let pager = bootstrap_conn.pager.load().clone();
                    if !pager.is_encryption_enabled() {
                        if bootstrap_conn.db.is_readonly() {
                            return Err(LimboError::Corrupt(
                                "Missing MVCC metadata table in read-only mode".to_string(),
                            ));
                        }
                        if log_size > LOG_HDR_SIZE as u64 {
                            return Err(LimboError::Corrupt(
                                "Missing MVCC metadata table while logical log state exists"
                                    .to_string(),
                            ));
                        }
                        // First-time MVCC bootstrap: ensure a durable logical-log header exists
                        // before any metadata-table writes can commit into WAL.
                        // If a previous crash left a torn header tail (0 < size < LOG_HDR_SIZE),
                        // clear it before rewriting the header.
                        if log_size > 0 && log_size < LOG_HDR_SIZE as u64 {
                            let log_file = self.get_logical_log_file();
                            let c = log_file.truncate(0, Completion::new_trunc(|_| {}))?;
                            bootstrap_conn.db.io.wait_for_completion(c)?;
                        }
                        if log_size <= LOG_HDR_SIZE as u64 {
                            let pager = bootstrap_conn.pager.load().clone();
                            let c = self.storage.update_header()?;
                            pager.io.wait_for_completion(c)?;
                            if bootstrap_conn.get_sync_mode() != SyncMode::Off {
                                let c = self.storage.sync(pager.get_sync_type())?;
                                pager.io.wait_for_completion(c)?;
                            }
                        }
                        self.initialize_mvcc_metadata_table(&bootstrap_conn)?;
                        // Metadata bootstrap writes land in SQLite WAL first; reconcile immediately so
                        // subsequent opens (including read-only opens) do not depend on WAL replay.
                        self.maybe_complete_interrupted_checkpoint(&bootstrap_conn)?;
                    }
                }
            }
        }

        {
            let schema = bootstrap_conn.schema.read();
            let sqlite_schema_root_pages = {
                schema
                    .tables
                    .values()
                    .filter_map(|t| {
                        if let Table::BTree(btree) = t.as_ref() {
                            Some(btree.root_page)
                        } else {
                            None
                        }
                    })
                    .chain(
                        schema
                            .indexes
                            .values()
                            .flatten()
                            .map(|index| index.root_page),
                    )
            };
            // Map all existing checkpointed root pages to table ids so that if root_page=R, table_id=-R
            for root_page in sqlite_schema_root_pages {
                turso_assert!(root_page > 0, "root_page={root_page} must be positive");
                let root_page_as_table_id = MVTableId::from(-(root_page));
                self.insert_table_id_to_rootpage(root_page_as_table_id, Some(root_page as u64));
            }
        }

        // Recover logical log while bootstrap connection still reads from pager-backed schema.
        // This lets recovery merge checkpointed sqlite_schema rows with non-checkpointed rows from log replay.
        // Return value indicates whether recovery replayed any frames; unused here because
        // global_header initialization below is unconditional (guarded by is_none() instead).
        // The return value is still used by tests to verify recovery behavior.
        let _recovered = self.maybe_recover_logical_log(bootstrap_conn.clone())?;

        // Recovery is done, switch back to regular MVCC reads.
        bootstrap_conn.promote_to_regular_connection();
        if self.global_header.read().is_none() {
            let pager = bootstrap_conn.pager.load();
            let header = pager
                .io
                .block(|| pager.with_header(|header| *header))
                .expect("failed to read database header");
            self.global_header.write().replace(header);
        }

        Ok(())
    }

    /// MVCC does not use the pager/btree cursors to create pages until checkpoint.
    /// This method is used to assign root page numbers when Insn::CreateBtree is used.
    /// MVCC table ids are always negative. Their corresponding rootpage entry in sqlite_schema
    /// is the same negative value if the table has not been checkpointed yet. Otherwise, the root page
    /// will be positive and corresponds to the actual physical page.
    pub fn get_next_table_id(&self) -> i64 {
        self.next_table_id.fetch_sub(1, Ordering::SeqCst)
    }

    pub fn get_next_rowid(&self) -> i64 {
        self.next_rowid.fetch_add(1, Ordering::SeqCst) as i64
    }

    /// Inserts a new row into a table in the database.
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
        self.insert_to_table_or_index(tx_id, row, None)
    }

    /// Same as insert() but can insert to a table or an index, indicated by the `maybe_index_id` argument.
    pub fn insert_to_table_or_index(
        &self,
        tx_id: TxID,
        row: Row,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<()> {
        tracing::trace!("insert(tx_id={}, row.id={:?})", tx_id, row.id);
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        turso_assert_eq!(tx.state, TransactionState::Active);
        let id = row.id.clone();
        match maybe_index_id {
            Some(index_id) => {
                let version_id = self.get_version_id();
                let row_version = RowVersion {
                    id: version_id,
                    begin: Some(TxTimestampOrID::TxID(tx.tx_id)),
                    end: None,
                    row: row.clone(),
                    btree_resident: false,
                };
                let RowKey::Record(sortable_key) = row.id.row_id else {
                    panic!("Index writes must be to a record");
                };
                let sortable_key = self.get_or_create_index_key_arc(index_id, sortable_key);
                tx.insert_to_write_set(id);
                tx.record_created_index_version((index_id, sortable_key.clone()), version_id);
                self.insert_index_version(index_id, sortable_key, row_version);
            }
            None => {
                // NOTE: We do NOT check for conflicts at insert time (pure optimistic).
                // Conflicts are detected at commit time using end_ts comparison.
                // This allows multiple transactions to insert the same rowid,
                // with first-committer-wins semantics.

                let version_id = self.get_version_id();
                let row_version = RowVersion {
                    id: version_id,
                    begin: Some(TxTimestampOrID::TxID(tx.tx_id)),
                    end: None,
                    row,
                    btree_resident: false,
                };
                tx.insert_to_write_set(id.clone());
                tx.record_created_table_version(id.clone(), version_id);
                let allocator = self.get_rowid_allocator(&id.table_id);
                allocator.insert_row_id_maybe_update(id.row_id.to_int_or_panic());
                self.insert_version(id, row_version);
            }
        }
        Ok(())
    }

    /// Inserts a deletion record for a row that does not currently have any versions in the MV store.
    /// This is used in cases where the BTree contains that record, but it is logically deleted.
    pub fn insert_tombstone_to_table_or_index(
        &self,
        tx_id: TxID,
        id: RowID,
        row: Row,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<()> {
        let version_id = self.get_version_id();
        let row_version = RowVersion {
            id: version_id,
            // Tombstones over B-tree-resident rows have no MVCC creator begin.
            // They invalidate B-tree visibility via end timestamp only.
            begin: None,
            end: Some(TxTimestampOrID::TxID(tx_id)),
            row: row.clone(),
            btree_resident: true,
        };
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        tx.insert_to_write_set(id.clone());
        match maybe_index_id {
            Some(index_id) => {
                let RowKey::Record(sortable_key) = row.id.row_id else {
                    panic!("Index writes must be to a record");
                };
                let sortable_key = self.get_or_create_index_key_arc(index_id, sortable_key);
                tx.record_created_index_version((index_id, sortable_key.clone()), version_id);
                self.insert_index_version(index_id, sortable_key, row_version);
            }
            None => {
                tx.record_created_table_version(id.clone(), version_id);
                self.insert_version(id, row_version);
            }
        }
        Ok(())
    }

    /// Inserts a row that was read from the B-tree (not in MvStore).
    /// This is used when updating a row that exists in B-tree but hasn't been
    /// modified in MVCC yet. The btree_resident flag helps the checkpoint logic
    /// determine if subsequent deletes should be checkpointed to the B-tree file.
    pub fn insert_btree_resident_to_table_or_index(
        &self,
        tx_id: TxID,
        row: Row,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<()> {
        tracing::trace!(
            "insert_btree_resident(tx_id={}, row.id={:?})",
            tx_id,
            row.id
        );
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        turso_assert_eq!(tx.state, TransactionState::Active);
        let id = row.id.clone();
        match maybe_index_id {
            Some(index_id) => {
                let version_id = self.get_version_id();
                let row_version = RowVersion {
                    id: version_id,
                    begin: Some(TxTimestampOrID::TxID(tx.tx_id)),
                    end: None,
                    row: row.clone(),
                    btree_resident: true,
                };
                let RowKey::Record(sortable_key) = row.id.row_id else {
                    panic!("Index writes must be to a record");
                };
                let sortable_key = self.get_or_create_index_key_arc(index_id, sortable_key);
                tx.insert_to_write_set(id);
                tx.record_created_index_version((index_id, sortable_key.clone()), version_id);
                self.insert_index_version(index_id, sortable_key, row_version);
            }
            None => {
                let version_id = self.get_version_id();
                let row_version = RowVersion {
                    id: version_id,
                    begin: Some(TxTimestampOrID::TxID(tx.tx_id)),
                    end: None,
                    row,
                    btree_resident: true,
                };
                tx.insert_to_write_set(id.clone());
                tx.record_created_table_version(id.clone(), version_id);
                self.insert_version(id, row_version);
            }
        }
        Ok(())
    }

    /// Updates a row in a table in the database with new values.
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
        self.update_to_table_or_index(tx_id, row, None)
    }

    /// Same as update() but can update a table or an index, indicated by the `maybe_index_id` argument.    
    pub fn update_to_table_or_index(
        &self,
        tx_id: TxID,
        row: Row,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<bool> {
        tracing::trace!("update(tx_id={}, row.id={:?})", tx_id, row.id);
        if !self.delete_from_table_or_index(tx_id, row.id.clone(), maybe_index_id)? {
            return Ok(false);
        }
        self.insert_to_table_or_index(tx_id, row, maybe_index_id)?;
        Ok(true)
    }

    /// Inserts a row into a table in the database with new values, previously deleting
    /// any old data if it existed. Bails on a delete error, e.g. write-write conflict.
    pub fn upsert(&self, tx_id: TxID, row: Row) -> Result<()> {
        self.upsert_to_table_or_index(tx_id, row, None)
    }

    /// Same as upsert() but can upsert to a table or an index, indicated by the `maybe_index_id` argument.
    pub fn upsert_to_table_or_index(
        &self,
        tx_id: TxID,
        row: Row,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<()> {
        tracing::trace!("upsert(tx_id={}, row.id={:?})", tx_id, row.id);
        self.delete_from_table_or_index(tx_id, row.id.clone(), maybe_index_id)?;
        self.insert_to_table_or_index(tx_id, row, maybe_index_id)?;
        Ok(())
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
        self.delete_from_table_or_index(tx_id, id, None)
    }

    /// Same as delete() but can delete from a table or an index, indicated by the `maybe_index_id` argument.
    pub fn delete_from_table_or_index(
        &self,
        tx_id: TxID,
        id: RowID,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<bool> {
        tracing::trace!("delete(tx_id={}, id={:?})", tx_id, id);
        match maybe_index_id {
            Some(index_id) => {
                let rows = self.index_rows.get_or_insert_with(index_id, SkipMap::new);
                let rows = rows.value();
                let RowKey::Record(sortable_key) = id.row_id.clone() else {
                    panic!("Index deletes must have a record row_id");
                };
                let chain_entry = rows.get(&sortable_key);
                if let Some(ref chain_entry) = chain_entry {
                    let arc_key = chain_entry.key().clone();
                    let chain = chain_entry.value();
                    for node in chain.iter() {
                        let tx = self
                            .txs
                            .get(&tx_id)
                            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                        let tx = tx.value();
                        turso_assert_eq!(tx.state, TransactionState::Active);
                        if !is_node_visible_to(node, tx, &self.txs) {
                            continue;
                        }
                        let end = node.end();
                        if is_write_write_conflict_ts(&self.txs, tx, &end) {
                            turso_assert_reachable!("write-write conflict on delete");
                            return Err(LimboError::WriteWriteConflict);
                        }

                        let version_id = node.id;
                        node.set_end(Some(TxTimestampOrID::TxID(tx.tx_id)));
                        let tx = self
                            .txs
                            .get(&tx_id)
                            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                        let tx = tx.value();
                        tx.insert_to_write_set(id);
                        tx.record_deleted_index_version((index_id, arc_key), version_id);
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            None => {
                let chain_entry = self.rows.get(&id);
                if let Some(ref chain_entry) = chain_entry {
                    let chain = chain_entry.value();
                    for node in chain.iter() {
                        let tx = self
                            .txs
                            .get(&tx_id)
                            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                        let tx = tx.value();
                        turso_assert_eq!(tx.state, TransactionState::Active);
                        if !is_node_visible_to(node, tx, &self.txs) {
                            continue;
                        }
                        let end = node.end();
                        if is_write_write_conflict_ts(&self.txs, tx, &end) {
                            turso_assert_reachable!("write-write conflict on delete");
                            return Err(LimboError::WriteWriteConflict);
                        }

                        let version_id = node.id;
                        node.set_end(Some(TxTimestampOrID::TxID(tx.tx_id)));
                        let tx = self
                            .txs
                            .get(&tx_id)
                            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                        let tx = tx.value();
                        tx.insert_to_write_set(id.clone());
                        tx.record_deleted_table_version(id, version_id);
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
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
        self.read_from_table_or_index(tx_id, id, None)
    }

    /// Same as read() but can read from a table or an index, indicated by the `maybe_index_id` argument.
    pub fn read_from_table_or_index(
        &self,
        tx_id: TxID,
        id: RowID,
        maybe_index_id: Option<MVTableId>,
    ) -> Result<Option<Row>> {
        tracing::trace!("read(tx_id={}, id={:?})", tx_id, id);

        let tx = self
            .txs
            .get(&tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        turso_assert_eq!(tx.state, TransactionState::Active);
        match maybe_index_id {
            Some(index_id) => {
                let rows = self.index_rows.get_or_insert_with(index_id, SkipMap::new);
                let rows = rows.value();
                let RowKey::Record(sortable_key) = id.row_id else {
                    panic!("Index reads must have a record row_id");
                };
                if let Some(chain_entry) = rows.get(&sortable_key) {
                    let chain = chain_entry.value();
                    if let Some(node) = chain
                        .iter()
                        .find(|node| is_node_visible_to(node, tx, &self.txs))
                    {
                        return Ok(Some(node.row.clone()));
                    }
                }
                Ok(None)
            }
            None => {
                if let Some(chain_entry) = self.rows.get(&id) {
                    let chain = chain_entry.value();
                    if let Some(node) = chain
                        .iter()
                        .find(|node| is_node_visible_to(node, tx, &self.txs))
                    {
                        tx.insert_to_read_set(id);
                        return Ok(Some(node.row.clone()));
                    }
                }
                Ok(None)
            }
        }
    }

    /// Gets all row ids in the database.
    pub fn scan_row_ids(&self) -> Result<Vec<RowID>> {
        tracing::trace!("scan_row_ids");
        let keys = self.rows.iter().map(|entry| entry.key().clone());
        Ok(keys.collect())
    }

    pub fn get_row_id_range(
        &self,
        table_id: MVTableId,
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
            row_id: RowKey::Int(start),
        };

        let end_id = RowID {
            table_id,
            row_id: RowKey::Int(i64::MAX),
        };

        self.rows
            .range(start_id..end_id)
            .take(max_items as usize)
            .for_each(|entry| bucket.push(entry.key().clone()));

        Ok(())
    }

    pub(crate) fn advance_cursor_and_get_row_id_for_table(
        &self,
        table_id: MVTableId,
        mv_store_iterator: &mut Option<MvccIterator<'static, RowID>>,
        tx_id: TxID,
    ) -> Option<RowID> {
        let mv_store_iterator = mv_store_iterator.as_mut().expect(
            "mv_store_iterator must be initialized when calling get_row_id_for_table_in_direction",
        );

        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();
        loop {
            // We are moving forward, so if a row was deleted we just need to skip it. Therefore, we need
            // to loop either until we find a row that is not deleted or until we reach the end of the table.
            let next_row = mv_store_iterator.next();
            let row = next_row?;
            if row.key().table_id != table_id {
                // In case of table rows, we store the rows of all tables in a single map,
                // so we must stop iteration if we reach a row that is on a different table.
                // In the case of indexes we have a separate map per table so this is not
                // relevant.
                return None;
            }

            // We found a row, let's check if it's visible to the transaction.
            if let Some(visible_row) = self.find_last_visible_version(tx, &row) {
                return Some(visible_row);
            }
            // If this row is not visible, continue to the next row
        }
    }

    pub(crate) fn advance_cursor_and_get_row_id_for_index(
        &self,
        mv_store_iterator: &mut Option<MvccIterator<'static, Arc<SortableIndexKey>>>,
        tx_id: TxID,
    ) -> Option<RowID> {
        let mv_store_iterator = mv_store_iterator.as_mut().expect(
            "mv_store_iterator must be initialized when calling get_row_id_for_index_in_direction",
        );

        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();

        self.find_next_visible_index_row(tx, mv_store_iterator)
    }

    /// Check if the B-tree version of a row should be shown to the given transaction.
    ///
    /// Returns true if the B-tree version is valid (should be shown).
    /// Returns false if the B-tree version is shadowed or deleted by MVCC.
    pub fn query_btree_version_is_valid(
        &self,
        table_id: MVTableId,
        row_id: &RowKey,
        tx_id: TxID,
    ) -> bool {
        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();

        match row_id {
            RowKey::Int(_) => {
                let row_id_full = RowID {
                    table_id,
                    row_id: row_id.clone(),
                };
                let Some(chain_entry) = self.rows.get(&row_id_full) else {
                    // No MVCC version -> B-tree is valid
                    return true;
                };
                let chain = chain_entry.value();

                // Check if any version invalidates the B-tree row
                let btree_is_invalid = chain
                    .iter()
                    .any(|node| is_node_btree_invalidating(node, tx, &self.txs));

                !btree_is_invalid
            }
            RowKey::Record(record) => {
                let index_rows = self.index_rows.get_or_insert_with(table_id, SkipMap::new);
                let index_rows = index_rows.value();
                let Some(chain_entry) = index_rows.get(record) else {
                    // No MVCC version -> B-tree is valid
                    return true;
                };
                let chain = chain_entry.value();

                // Check if any version invalidates the B-tree row
                let btree_is_invalid = chain
                    .iter()
                    .any(|node| is_node_btree_invalidating(node, tx, &self.txs));

                !btree_is_invalid
            }
        }
    }

    fn find_last_visible_version(
        &self,
        tx: &Transaction,
        row: &crossbeam_skiplist::map::Entry<'_, RowID, RowVersionChain>,
    ) -> Option<RowID> {
        row.value()
            .iter()
            .find(|node| is_node_visible_to(node, tx, &self.txs))
            .map(|_| row.key().clone())
    }

    fn find_last_visible_index_version(
        &self,
        tx: &Transaction,
        row: crossbeam_skiplist::map::Entry<'_, Arc<SortableIndexKey>, RowVersionChain>,
    ) -> Option<RowID> {
        row.value()
            .iter()
            .find(|node| is_node_visible_to(node, tx, &self.txs))
            .map(|node| node.row.id.clone())
    }

    fn find_next_visible_index_row<'a, I>(&self, tx: &Transaction, mut rows: I) -> Option<RowID>
    where
        I: Iterator<
            Item = crossbeam_skiplist::map::Entry<'a, Arc<SortableIndexKey>, RowVersionChain>,
        >,
    {
        loop {
            let row = rows.next()?;
            if let Some(visible_row) = self.find_last_visible_index_version(tx, row) {
                return Some(visible_row);
            }
        }
    }

    fn find_next_visible_table_row<'a, I>(
        &self,
        tx: &Transaction,
        mut rows: I,
        table_id: MVTableId,
    ) -> Option<RowID>
    where
        I: Iterator<Item = crossbeam_skiplist::map::Entry<'a, RowID, RowVersionChain>>,
    {
        loop {
            let row = rows.next()?;
            if row.key().table_id != table_id {
                return None;
            }
            if let Some(visible_row) = self.find_last_visible_version(tx, &row) {
                return Some(visible_row);
            }
        }
    }

    pub fn seek_rowid(
        &self,
        start: RowID,
        inclusive: bool,
        direction: IterationDirection,
        tx_id: TxID,
        table_iterator: &mut Option<MvccIterator<'static, RowID>>,
    ) -> Option<RowID> {
        let table_id = start.table_id;
        let iter_box = {
            let start = if inclusive {
                Bound::Included(start)
            } else {
                Bound::Excluded(start)
            };
            let range = create_seek_range(start, direction);
            match direction {
                IterationDirection::Forwards => Box::new(self.rows.range(range))
                    as Box<dyn Iterator<Item = Entry<'_, RowID, RowVersionChain>> + Send + Sync>,
                IterationDirection::Backwards => Box::new(self.rows.range(range).rev())
                    as Box<dyn Iterator<Item = Entry<'_, RowID, RowVersionChain>> + Send + Sync>,
            }
        };
        *table_iterator = Some(static_iterator_hack!(iter_box, RowID));

        let mv_store_iterator = table_iterator
            .as_mut()
            .expect("table_iterator was assigned above if it was None");

        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();

        self.find_next_visible_table_row(tx, mv_store_iterator, table_id)
    }

    pub fn seek_index(
        &self,
        index_id: MVTableId,
        start: SortableIndexKey,
        inclusive: bool,
        direction: IterationDirection,
        tx_id: TxID,
        index_iterator: &mut Option<MvccIterator<'static, Arc<SortableIndexKey>>>,
    ) -> Option<RowID> {
        let index_rows = self.index_rows.get_or_insert_with(index_id, SkipMap::new);
        let index_rows = index_rows.value();
        let start = if inclusive {
            Bound::Included(start)
        } else {
            Bound::Excluded(start)
        };
        let range = create_seek_range(start, direction);
        let iter_box = match direction {
            IterationDirection::Forwards => Box::new(index_rows.range(range))
                as Box<
                    dyn Iterator<Item = Entry<'_, Arc<SortableIndexKey>, RowVersionChain>>
                        + Send
                        + Sync,
                >,
            IterationDirection::Backwards => Box::new(index_rows.range(range).rev())
                as Box<
                    dyn Iterator<Item = Entry<'_, Arc<SortableIndexKey>, RowVersionChain>>
                        + Send
                        + Sync,
                >,
        };
        *index_iterator = Some(static_iterator_hack!(iter_box, Arc<SortableIndexKey>));
        let mv_store_iterator = index_iterator
            .as_mut()
            .expect("index_iterator was assigned above if it was None");

        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();
        self.find_next_visible_index_row(tx, mv_store_iterator)
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
    ) -> Result<TxID> {
        if !self.blocking_checkpoint_lock.read() {
            // If there is a stop-the-world checkpoint in progress, we cannot begin any transaction at all.
            return Err(LimboError::Busy);
        }
        let unlock = || self.blocking_checkpoint_lock.unlock();
        let tx_id = maybe_existing_tx_id.unwrap_or_else(|| self.get_tx_id());
        let begin_ts = if let Some(tx_id) = maybe_existing_tx_id {
            self.txs
                .get(&tx_id)
                .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?
                .value()
                .begin_ts
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
        tx.pager_commit_lock_held.store(true, Ordering::Release);
        tracing::trace!(
            "begin_exclusive_tx(tx_id={}) - exclusive write logical log transaction",
            tx_id
        );
        tracing::debug!("begin_exclusive_tx: tx_id={} succeeded", tx_id);
        self.txs.insert(tx_id, tx);
        Ok(tx_id)
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

    fn get_new_transaction_database_header(&self, pager: &Arc<Pager>) -> DatabaseHeader {
        if self.global_header.read().is_none() {
            pager
                .io
                .block(|| pager.maybe_allocate_page1())
                .expect("failed to allocate page1");
            let header = pager
                .io
                .block(|| pager.with_header(|header| *header))
                .expect("failed to read database header");
            // TODO: We initialize header here, maybe this needs more careful handling
            self.global_header.write().replace(header);
            tracing::debug!(
                "get_transaction_database_header create: header={:?}",
                header
            );
            header
        } else {
            let header = self
                .global_header
                .read()
                .expect("global_header should be initialized");
            // The header could be stored, but not persisted yet
            pager
                .io
                .block(|| pager.maybe_allocate_page1())
                .expect("failed to allocate page1");
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
            let tx = self
                .txs
                .get(tx_id)
                .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
            let header = tx.value();
            let header = header.header.read();
            tracing::debug!("with_header read: header={:?}", header);
            Ok(f(&header))
        } else {
            let header = self.global_header.read();
            tracing::debug!("with_header read: header={:?}", header);
            Ok(f(header.as_ref().ok_or_else(|| {
                LimboError::InternalError("global_header not initialized".to_string())
            })?))
        }
    }

    pub fn with_header_mut<T, F>(&self, f: F, tx_id: Option<&TxID>) -> Result<T>
    where
        F: Fn(&mut DatabaseHeader) -> T,
    {
        if let Some(tx_id) = tx_id {
            let tx = self
                .txs
                .get(tx_id)
                .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
            let header = tx.value();
            let mut header = header.header.write();
            tracing::debug!("with_header_mut read: header={:?}", header);
            Ok(f(&mut header))
        } else {
            let mut header = self.global_header.write();
            let header = header.as_mut().ok_or_else(|| {
                LimboError::InternalError("global_header not initialized".to_string())
            })?;
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
                connection.get_sync_mode(),
            ));
        Ok(state_machine)
    }

    /// Rolls back a transaction with the specified ID.
    ///
    /// This function rolls back a transaction with the specified `tx_id` by
    /// discarding any changes made by the transaction.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to abort.
    pub fn rollback_tx(&self, tx_id: TxID, _pager: Arc<Pager>, connection: &Connection) {
        let tx_unlocked = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx_unlocked.value();
        connection.set_mv_tx(None);
        turso_assert!(matches!(
            tx.state.load(),
            TransactionState::Active | TransactionState::Preparing(_)
        ));
        tx.state.store(TransactionState::Aborted);
        tracing::trace!("abort(tx_id={})", tx_id);
        self.unlock_commit_lock_if_held(tx);

        if self.is_exclusive_tx(&tx_id) {
            self.release_exclusive_tx(&tx_id);
        }

        for rowid in &tx.write_set {
            let rowid = rowid.value();
            self.rollback_rowid(tx_id, rowid);
        }

        if connection.schema.read().schema_version > connection.db.schema.lock().schema_version {
            // Connection made schema changes during tx and rolled back -> revert connection-local schema.
            *connection.schema.write() = connection.db.clone_schema();
        }

        let tx = tx_unlocked.value();
        tx.state.store(TransactionState::Terminated);
        tracing::trace!("terminate(tx_id={})", tx_id);
        // FIXME: verify that we can already remove the transaction here!
        // Maybe it's fine for snapshot isolation, but too early for serializable?
        self.remove_tx(tx_id);
    }

    fn rollback_rowid(&self, tx_id: u64, rowid: &RowID) {
        if rowid.row_id.is_int_key() {
            self.rollback_table_rowid(tx_id, rowid);
        } else {
            self.rollback_index_rowid(tx_id, rowid);
        }
    }

    fn unlock_commit_lock_if_held(&self, tx: &Transaction) {
        if tx.pager_commit_lock_held.swap(false, Ordering::AcqRel) {
            self.commit_coordinator.pager_commit_lock.unlock();
        }
    }

    fn rollback_index_rowid(&self, tx_id: u64, rowid: &RowID) {
        if let Some(index) = self.index_rows.get(&rowid.table_id) {
            let index = index.value();
            let RowKey::Record(ref index_key) = rowid.row_id else {
                panic!("Index writes must have a record key");
            };
            if let Some(chain_entry) = index.get(index_key) {
                let chain = chain_entry.value();
                for node in chain.iter() {
                    rollback_node_version(tx_id, node);
                }
            }
        }
    }

    fn rollback_table_rowid(&self, tx_id: u64, rowid: &RowID) {
        if let Some(chain_entry) = self.rows.get(rowid) {
            let chain = chain_entry.value();
            for node in chain.iter() {
                rollback_node_version(tx_id, node);
            }
        }
    }

    /// Begin a savepoint for the transaction.
    /// This should be called at the start of a statement in an interactive transaction.
    pub fn begin_savepoint(&self, tx_id: TxID) {
        let tx = self
            .txs
            .get(&tx_id)
            .unwrap_or_else(|| panic!("Transaction {tx_id} not found while beginning savepoint"));
        tx.value().begin_savepoint();
    }

    /// Release the newest savepoint for the transaction.
    /// This should be called when a statement completes successfully.
    /// Silently returns if the transaction doesn't exist (e.g., already committed).
    pub fn release_savepoint(&self, tx_id: TxID) {
        if let Some(tx) = self.txs.get(&tx_id) {
            tx.value().release_savepoint();
        }
        // If transaction doesn't exist, it was already committed - nothing to release
    }

    /// Rolls back a savepoint within a transaction.
    /// Returns true if a savepoint was rolled back, false if no savepoint was active.
    pub fn rollback_first_savepoint(&self, tx_id: u64) -> Result<bool> {
        let tx = self.txs.get(&tx_id).unwrap_or_else(|| {
            panic!("Transaction {tx_id} not found while rolling back savepoint")
        });

        let tx = tx.value();
        let savepoint = tx.pop_savepoint();

        if let Some(savepoint) = savepoint {
            tracing::debug!("rollback_savepoint(tx_id={}, created_table={}, created_index={}, deleted_table={}, deleted_index={})",
                tx_id,
                savepoint.created_table_versions.len(),
                savepoint.created_index_versions.len(),
                savepoint.deleted_table_versions.len(),
                savepoint.deleted_index_versions.len());

            // Collect table rowids affected by this savepoint for write set cleanup.
            let mut affected_table_rowids: BTreeSet<RowID> = BTreeSet::new();

            // Mark created table versions as garbage (begin=None, end=None)
            for (rowid, version_id) in &savepoint.created_table_versions {
                if let Some(entry) = self.rows.get(rowid) {
                    let chain = entry.value();
                    for node in chain.iter() {
                        if node.id == *version_id {
                            node.mark_garbage();
                            tracing::debug!("rollback_savepoint: removed table version(table_id={}, row_id={}, version_id={})",
                                rowid.table_id, rowid.row_id, version_id);
                            break;
                        }
                    }
                }
                affected_table_rowids.insert(rowid.clone());
            }

            // Mark created index versions as garbage
            for ((table_id, key), version_id) in &savepoint.created_index_versions {
                if let Some(index) = self.index_rows.get(table_id) {
                    if let Some(entry) = index.value().get(key) {
                        let chain = entry.value();
                        for node in chain.iter() {
                            if node.id == *version_id {
                                node.mark_garbage();
                                tracing::debug!(
                                    "rollback_savepoint: removed index version(table_id={}, version_id={})",
                                    table_id,
                                    version_id
                                );
                                break;
                            }
                        }
                    }
                }
            }

            // Restore deleted table versions (clear end timestamp)
            for (rowid, version_id) in &savepoint.deleted_table_versions {
                if let Some(entry) = self.rows.get(rowid) {
                    let chain = entry.value();
                    for node in chain.iter() {
                        if node.id == *version_id {
                            node.set_end(None);
                            tracing::debug!("rollback_savepoint: restored table version(table_id={}, row_id={}, version_id={})",
                                rowid.table_id, rowid.row_id, version_id);
                            break;
                        }
                    }
                }
                affected_table_rowids.insert(rowid.clone());
            }

            // Restore deleted index versions
            for ((table_id, key), version_id) in &savepoint.deleted_index_versions {
                if let Some(index) = self.index_rows.get(table_id) {
                    if let Some(entry) = index.value().get(key) {
                        let chain = entry.value();
                        for node in chain.iter() {
                            if node.id == *version_id {
                                node.set_end(None);
                                tracing::debug!("rollback_savepoint: restored index version(table_id={}, version_id={})",
                                    table_id, version_id);
                                break;
                            }
                        }
                    }
                }
            }

            // Clean up write set: remove rowids that no longer have any
            // version belonging to this transaction. Without this, commit
            // validation would find stale write set entries pointing to
            // pre-existing committed versions and panic.
            for rowid in &affected_table_rowids {
                let has_tx_version = if let Some(entry) = self.rows.get(rowid) {
                    let chain = entry.value();
                    chain.iter().any(|node| {
                        matches!(node.begin(), Some(TxTimestampOrID::TxID(id)) if id == tx_id)
                            || matches!(node.end(), Some(TxTimestampOrID::TxID(id)) if id == tx_id)
                    })
                } else {
                    false
                };
                if !has_tx_version {
                    tx.write_set.remove(rowid);
                }
            }

            // Same cleanup for index rowids that were newly added by this savepoint.
            for rowid in &savepoint.newly_added_to_write_set {
                if rowid.row_id.is_int_key() {
                    // Table rowids are already handled above via affected_table_rowids.
                    continue;
                }
                let RowKey::Record(ref record) = rowid.row_id else {
                    unreachable!()
                };
                let has_tx_version = if let Some(index) = self.index_rows.get(&rowid.table_id) {
                    if let Some(entry) = index.value().get(record) {
                        let chain = entry.value();
                        chain.iter().any(|node| {
                            matches!(node.begin(), Some(TxTimestampOrID::TxID(id)) if id == tx_id)
                                || matches!(node.end(), Some(TxTimestampOrID::TxID(id)) if id == tx_id)
                        })
                    } else {
                        false
                    }
                } else {
                    false
                };
                if !has_tx_version {
                    tx.write_set.remove(rowid);
                }
            }

            Ok(true)
        } else {
            tracing::debug!(
                "rollback_savepoint(tx_id={}): no savepoint was active",
                tx_id
            );
            Ok(false)
        }
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
        turso_assert_eq!(prev, *tx_id, "exclusive lock released by wrong tx", { "expected_tx_id": *tx_id, "actual_tx_id": prev });
    }

    /// Generates next unique transaction id
    pub fn get_tx_id(&self) -> u64 {
        self.tx_ids.fetch_add(1, Ordering::SeqCst)
    }

    /// Generates next unique version ID for RowVersion tracking.
    pub fn get_version_id(&self) -> u64 {
        self.version_id_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Gets current timestamp
    pub fn get_timestamp(&self) -> u64 {
        self.clock.get_timestamp()
    }

    /// Compute the low-water mark: the minimum begin_ts of all active or
    /// preparing transactions. Returns u64::MAX if no transactions are active.
    /// Used by GC to determine which row versions are safe to reclaim.
    pub fn compute_lwm(&self) -> u64 {
        self.txs
            .iter()
            .filter_map(|entry| {
                let tx = entry.value();
                match tx.state.load() {
                    TransactionState::Active | TransactionState::Preparing(_) => Some(tx.begin_ts),
                    _ => None,
                }
            })
            .min()
            .unwrap_or(u64::MAX)
    }

    /// Garbage-collects row versions that are invisible to all active transactions.
    /// Uses the low-water mark (LWM) to determine reclaimability in O(1) per version.
    /// Covers both table rows (`self.rows`) and index rows (`self.index_rows`).
    /// Returns the number of removed versions.
    pub fn drop_unused_row_versions(&self) -> usize {
        let lwm = self.compute_lwm();
        let ckpt_max = self.durable_txid_max.load(Ordering::SeqCst);

        let dropped =
            self.gc_table_row_versions(lwm, ckpt_max) + self.gc_index_row_versions(lwm, ckpt_max);

        tracing::trace!(
            "drop_unused_row_versions() -> dropped {dropped}, txs: {}, rows: {}",
            self.txs.len(),
            self.rows.len()
        );
        dropped
    }

    fn gc_table_row_versions(&self, lwm: u64, ckpt_max: u64) -> usize {
        let mut dropped = 0;

        for entry in self.rows.iter() {
            let chain = entry.value();
            dropped += chain.gc_mark(lwm, ckpt_max);
            // Empty entries are left in the SkipMap (lazy removal). This avoids
            // a TOCTOU race where a concurrent writer inserts a version between
            // the emptiness check and SkipMap::remove(). Empty entries are reused
            // by get_or_insert_with on subsequent inserts and cleaned up by
            // checkpoint-time GC which runs under the blocking lock.
        }
        dropped
    }

    fn gc_index_row_versions(&self, lwm: u64, ckpt_max: u64) -> usize {
        let mut dropped = 0;

        for outer_entry in self.index_rows.iter() {
            let inner_map = outer_entry.value();

            for inner_entry in inner_map.iter() {
                let chain = inner_entry.value();
                dropped += chain.gc_mark(lwm, ckpt_max);
            }
            // Empty entries left in place — same TOCTOU rationale as table rows.
        }
        dropped
    }

    /// Apply GC rules to a single version chain (Vec variant for log records).
    /// Returns number of versions removed.
    ///
    /// Rule 1: Aborted garbage (begin=None, end=None) — always remove.
    /// Rule 2: Superseded (end=Timestamp(e), e <= lwm) — remove unless it's a
    ///         tombstone (no committed current version) whose deletion hasn't
    ///         been checkpointed (e > ckpt_max).
    /// Rule 3: Current checkpointed sole-survivor (end=None, b <= ckpt_max,
    ///         b < lwm, no other versions remain) — remove.
    ///
    pub fn gc_version_chain_vec(versions: &mut Vec<RowVersion>, lwm: u64, ckpt_max: u64) -> usize {
        let before = versions.len();

        // Rule 1: aborted garbage
        versions.retain(|rv| !matches!((&rv.begin, &rv.end), (None, None)));

        // Rule 2: superseded versions below LWM, with tombstone guard.
        let has_current = versions
            .iter()
            .any(|rv| rv.end.is_none() && matches!(&rv.begin, Some(TxTimestampOrID::Timestamp(_))));
        versions.retain(|rv| match &rv.end {
            Some(TxTimestampOrID::Timestamp(e)) if *e <= lwm => !has_current && *e > ckpt_max,
            _ => true,
        });

        // Rule 3: checkpointed sole-survivor current version.
        if versions.len() == 1 {
            if let (Some(TxTimestampOrID::Timestamp(b)), None) =
                (&versions[0].begin, &versions[0].end)
            {
                if *b <= ckpt_max && *b < lwm {
                    versions.clear();
                }
            }
        }

        before - versions.len()
    }

    pub fn recover(&self) -> Result<()> {
        let tx_log = self.storage.read_tx_log()?;
        for record in tx_log {
            tracing::debug!("recover() -> tx_timestamp={}", record.tx_timestamp);
            for version in record.row_versions {
                self.insert_version_sorted(version.row.id.clone(), version);
            }
            self.clock.reset(record.tx_timestamp);
        }
        Ok(())
    }

    // Extracts the begin timestamp from a transaction
    #[inline]
    fn get_begin_timestamp(&self, ts_or_id: &Option<TxTimestampOrID>) -> u64 {
        match ts_or_id {
            Some(TxTimestampOrID::Timestamp(ts)) => *ts,
            Some(TxTimestampOrID::TxID(tx_id)) => {
                self.txs
                    .get(tx_id)
                    .expect("transaction should exist in txs map")
                    .value()
                    .begin_ts
            }
            // This function is intended to be used in the ordering of row versions within the row version chain in `insert_version_raw`.
            //
            // The row version chain should be append-only (aside from garbage collection),
            // so the specific ordering handled by this function may not be critical. We might
            // be able to append directly to the row version chain in the future.
            //
            // The value 0 is used here to represent an infinite timestamp value. This is a deliberate
            // choice for a planned future bitpacking optimization, reserving 0 for this purpose,
            // while actual timestamps will start from 1.
            None => 0,
        }
    }

    /// Inserts a new row version into the database. Lock-free via CAS on chain head.
    fn insert_version(&self, id: RowID, row_version: RowVersion) {
        let chain = self.rows.get_or_insert_with(id, RowVersionChain::new);
        let chain = chain.value();
        chain.prepend(Box::new(RowVersionNode::from_row_version(row_version)));
    }

    /// Inserts a row version using sorted insertion (for recovery/log replay only).
    fn insert_version_sorted(&self, id: RowID, row_version: RowVersion) {
        let chain = self.rows.get_or_insert_with(id, RowVersionChain::new);
        let chain = chain.value();
        let get_begin_ts = |ts: &Option<TxTimestampOrID>| self.get_begin_timestamp(ts);
        chain.insert_sorted(
            Box::new(RowVersionNode::from_row_version(row_version)),
            get_begin_ts,
        );
    }

    /// Gets an existing Arc<SortableIndexKey> from the index if the key exists,
    /// otherwise creates a new Arc. This ensures we reuse Arc instances for the same key.
    fn get_or_create_index_key_arc(
        &self,
        index_id: MVTableId,
        key: SortableIndexKey,
    ) -> Arc<SortableIndexKey> {
        let index = self.index_rows.get_or_insert_with(index_id, SkipMap::new);
        let index = index.value();
        // Check if key exists and get the Arc if so
        let existing = index.get(&key).map(|entry| entry.key().clone());
        existing.unwrap_or_else(|| Arc::new(key))
    }

    pub fn insert_index_version(
        &self,
        index_id: MVTableId,
        key: Arc<SortableIndexKey>,
        row_version: RowVersion,
    ) {
        let index = self.index_rows.get_or_insert_with(index_id, SkipMap::new);
        let index = index.value();
        let chain = index.get_or_insert_with(key, RowVersionChain::new);
        let chain = chain.value();
        chain.prepend(Box::new(RowVersionNode::from_row_version(row_version)));
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
            let existing_begin = self.get_begin_timestamp(&v.begin);
            let new_begin = self.get_begin_timestamp(&row_version.begin);
            if existing_begin <= new_begin {
                // Recovery can replay multiple operations for the same row from one transaction
                // (e.g. insert then delete), which share the same begin timestamp.
                // Keep only the latest version for that begin timestamp so visibility checks don't
                // surface a stale intermediate version.
                // Only collapse duplicate "begin" values when both are concrete begins.
                // `begin=None` is used for committed tombstones over B-tree-resident rows and
                // must never be conflated with a later statement's transient tombstone.
                if versions[i].row.id == row_version.row.id
                    && matches!(
                        (&versions[i].begin, &row_version.begin),
                        (Some(existing), Some(new)) if existing == new
                    )
                {
                    versions[i] = row_version;
                    return;
                }
                position = i + 1;
                break;
            }
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

    pub fn get_last_table_rowid(
        &self,
        table_id: MVTableId,
        table_iterator: &mut Option<MvccIterator<'static, RowID>>,
        tx_id: TxID,
    ) -> Option<RowKey> {
        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();
        let max_rowid = RowID {
            table_id,
            row_id: RowKey::Int(i64::MAX),
        };
        let range = create_seek_range(Bound::Included(max_rowid), IterationDirection::Backwards);
        let iter_box = Box::new(self.rows.range(range).rev());
        *table_iterator = Some(static_iterator_hack!(iter_box, RowID));
        let iter = table_iterator
            .as_mut()
            .expect("table_iterator was assigned above");
        loop {
            let entry = iter.next()?;
            // Rowid is not part of the table, therefore we already reached the end of the table.
            // NOTE: Shouldn't range already prevent this?
            tracing::trace!(
                "get_last_table_rowid: entry.key().table_id={}, table_id={}, row_id={}",
                entry.key().table_id,
                table_id,
                entry.key().row_id
            );
            if entry.key().table_id != table_id {
                tracing::trace!("get_last_table_rowid: reached end of table");
                return None;
            }
            if let Some(_visible_row) = self.find_last_visible_version(tx, &entry) {
                tracing::trace!(
                    "get_last_table_rowid: found visible row: {:?}",
                    _visible_row
                );
                // There is a visible version for this rowid, so we return it
                return Some(RowKey::Int(match &entry.key().row_id {
                    RowKey::Int(i) => *i,
                    _ => panic!("Expected RowKey::Int for table rowid"),
                }));
            }
        }
    }

    pub fn get_last_table_rowid_without_visibility_check(
        &self,
        table_id: MVTableId,
    ) -> Option<RowKey> {
        let max_rowid = RowID {
            table_id,
            row_id: RowKey::Int(i64::MAX),
        };
        let range = create_seek_range(Bound::Included(max_rowid), IterationDirection::Backwards);
        let mut range = self.rows.range(range).rev();
        let entry = range.next()?;
        if entry.key().table_id != table_id {
            return None;
        }
        Some(entry.key().row_id.clone())
    }

    pub fn get_last_index_rowid(
        &self,
        index_id: MVTableId,
        index_iterator: &mut Option<MvccIterator<'static, Arc<SortableIndexKey>>>,
    ) -> Option<RowKey> {
        let index = self.index_rows.get_or_insert_with(index_id, SkipMap::new);
        let index = index.value();
        let iter_box = Box::new(index.iter().rev());
        *index_iterator = Some(static_iterator_hack!(iter_box, Arc<SortableIndexKey>));
        let iter = index_iterator
            .as_mut()
            .expect("index_iterator was assigned above");
        iter.next()
            .map(|entry| RowKey::Record((**entry.key()).clone()))
    }

    pub fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.storage.get_logical_log_file()
    }

    fn logical_log_header_crc_valid(&self, pager: &Arc<Pager>) -> Result<bool> {
        let file = self.get_logical_log_file();
        let mut reader = StreamingLogicalLogReader::new(file);
        match reader.try_read_header(&pager.io)? {
            HeaderReadResult::Valid(_) => Ok(true),
            HeaderReadResult::NoLog | HeaderReadResult::Invalid => Ok(false),
        }
    }

    /// Runs during bootstrap to reconcile WAL state left by a prior crash or incomplete
    /// checkpoint. Classifies startup state by WAL frame count and logical-log header
    /// validity, then either completes the interrupted checkpoint (backfill WAL → DB,
    /// sync, truncate) or fails closed on corrupt/inconsistent artifacts.
    /// See RECOVERY_SEMANTICS.md "Startup Case Classification" for the full case table.
    fn maybe_complete_interrupted_checkpoint(&self, connection: &Arc<Connection>) -> Result<()> {
        let pager = connection.pager.load().clone();
        let Some(wal) = &pager.wal else {
            return Ok(());
        };
        // The bootstrap connection may have acquired a WAL read lock during earlier
        // bootstrap steps (e.g. schema parsing). Drop it so the TRUNCATE checkpoint
        // below isn't blocked by our own read lock.
        if wal.holds_read_lock() {
            wal.end_read_tx();
        }

        let wal_max_frame = wal.get_max_frame_in_wal();
        let file = self.get_logical_log_file();
        let mut reader = StreamingLogicalLogReader::new(file);
        let header_result = reader.try_read_header(&pager.io)?;

        let is_readonly = connection.db.is_readonly();
        if wal_max_frame == 0 {
            if !is_readonly {
                let mut checkpoint_result = CheckpointResult::new(0, 0, 0);
                pager
                    .io
                    .block(|| wal.truncate_wal(&mut checkpoint_result, pager.get_sync_type()))?;
                if let HeaderReadResult::Valid(header) = &header_result {
                    self.storage.logical_log.write().set_header(header.clone());
                }
            }
            return Ok(());
        }

        if is_readonly {
            return Err(LimboError::Corrupt(
                "Cannot reconcile interrupted MVCC checkpoint in read-only mode".to_string(),
            ));
        }

        let header = match header_result {
            HeaderReadResult::Valid(header) => header,
            HeaderReadResult::NoLog => {
                return Err(LimboError::Corrupt(
                    "WAL has committed frames but logical log header is missing".to_string(),
                ))
            }
            HeaderReadResult::Invalid => {
                return Err(LimboError::Corrupt(
                    "WAL has committed frames but logical log header is invalid".to_string(),
                ))
            }
        };
        self.storage.logical_log.write().set_header(header);

        // NOTE: this uses `CheckpointMode::Truncate` to drive WAL backfill only; we still
        // truncate the WAL explicitly below to preserve WAL-last ordering in recovery.
        let mut checkpoint_result = pager.io.block(|| {
            wal.checkpoint(
                &pager,
                CheckpointMode::Truncate {
                    upper_bound_inclusive: None,
                },
            )
        })?;
        if !checkpoint_result.everything_backfilled() {
            return Err(LimboError::Corrupt(
                "Unable to fully backfill committed WAL frames during MVCC recovery".to_string(),
            ));
        }

        if connection.get_sync_mode() != SyncMode::Off
            && checkpoint_result.wal_checkpoint_backfilled > 0
        {
            let c = pager
                .db_file
                .sync(Completion::new_sync(|_| {}), pager.get_sync_type())?;
            pager.io.wait_for_completion(c)?;
        }

        // Write a fresh log header (distinct from the checkpoint state machine which no
        // longer rewrites the header). This is bootstrap-only: we need a valid header on
        // disk before truncating WAL. CRC verify + retry guards against torn header writes.
        let mut retried_crc = false;
        loop {
            let c = self.storage.update_header()?;
            pager.io.wait_for_completion(c)?;

            if connection.get_sync_mode() != SyncMode::Off {
                let c = self.storage.sync(pager.get_sync_type())?;
                pager.io.wait_for_completion(c)?;
            }

            if self.logical_log_header_crc_valid(&pager)? {
                break;
            }

            if retried_crc {
                return Err(LimboError::Corrupt(
                    "Logical log header CRC mismatch after retry".to_string(),
                ));
            }
            retried_crc = true;
        }

        pager
            .io
            .block(|| wal.truncate_wal(&mut checkpoint_result, pager.get_sync_type()))?;
        Ok(())
    }

    /// Replays committed logical-log frames into the in-memory MVCC store.
    /// Only frames with `commit_ts > persistent_tx_ts_max` (the durable replay boundary
    /// from the metadata table) are applied; earlier frames were already checkpointed.
    /// On success, reseeds the MVCC clock to `max(persistent_tx_ts_max, max_replayed_commit_ts) + 1`
    /// and sets the log writer offset to `last_valid_offset` so torn-tail bytes are overwritten.
    /// Returns true if any frames were replayed, false otherwise.
    pub fn maybe_recover_logical_log(&self, connection: Arc<Connection>) -> Result<bool> {
        let pager = connection.pager.load().clone();
        let file = self.get_logical_log_file();
        let mut reader = StreamingLogicalLogReader::new(file.clone());
        let preserved_table_valued_functions =
            Self::capture_table_valued_functions(&connection.schema.read());

        let header = match reader.try_read_header(&pager.io)? {
            HeaderReadResult::Valid(header) => Some(header),
            HeaderReadResult::NoLog => None,
            HeaderReadResult::Invalid => {
                return Err(LimboError::Corrupt(
                    "Logical log header corrupt and no WAL recovery available".to_string(),
                ))
            }
        };

        if let Some(header) = &header {
            self.storage.logical_log.write().set_header(header.clone());
        }
        let persistent_tx_ts_max = if self.uses_durable_mvcc_metadata(&connection) {
            match self.try_read_persistent_tx_ts_max(&connection)? {
                Some(ts) => ts,
                None if pager.is_encryption_enabled() => 0,
                None if header.is_none() => 0,
                None => {
                    return Err(LimboError::Corrupt(
                        "Missing MVCC metadata table".to_string(),
                    ))
                }
            }
        } else {
            0
        };
        self.durable_txid_max
            .store(persistent_tx_ts_max, Ordering::SeqCst);
        self.clock.reset(persistent_tx_ts_max + 1);

        if header.is_none() || file.size()? <= LOG_HDR_SIZE as u64 {
            return Ok(false);
        }

        let mut max_commit_ts_seen = persistent_tx_ts_max;
        let replay_cutoff_ts = persistent_tx_ts_max;
        let mut schema_rows: HashMap<i64, ImmutableRecord> = HashMap::default();
        let mut dropped_root_pages: HashSet<i64> = HashSet::default();
        if let Some(mut stmt) = connection
            .query("SELECT rowid, type, name, tbl_name, rootpage, sql FROM sqlite_schema")?
        {
            stmt.run_with_row_callback(|row| {
                let rowid = row.get::<i64>(0)?;
                let values = (1..=5)
                    .map(|i| row.get_value(i).clone())
                    .collect::<Vec<_>>();
                schema_rows.insert(rowid, ImmutableRecord::from_values(&values, values.len()));
                Ok(())
            })?;
        }
        let mut index_infos: HashMap<MVTableId, Arc<IndexInfo>> = HashMap::default();

        // Track whether we have pending schema changes that need a rebuild.
        // We defer rebuild_schema() until all consecutive schema rows have been
        // inserted, because an intermediate rebuild (e.g. after inserting the
        // renamed table row but before inserting the renamed index row) can see
        // an inconsistent state and panic in populate_indices().
        // Cell is used so the get_index_info closure can flush the pending rebuild.
        let needs_schema_rebuild = std::cell::Cell::new(false);

        let rebuild_schema =
            |connection: &Arc<Connection>, schema_rows: &HashMap<i64, ImmutableRecord>| {
                let pager = connection.pager.load().clone();
                let cookie = pager
                    .io
                    .block(|| pager.with_header(|header| header.schema_cookie))?
                    .get();
                let mut fresh = Schema::new();
                fresh.schema_version = cookie;
                let mut from_sql_indexes = Vec::with_capacity(10);
                let mut automatic_indices: HashMap<String, Vec<(String, i64)>> = HashMap::default();
                let mut dbsp_state_roots: HashMap<String, i64> = HashMap::default();
                let mut dbsp_state_index_roots: HashMap<String, i64> = HashMap::default();
                let mut materialized_view_info: HashMap<String, (String, i64)> = HashMap::default();
                let syms = connection.syms.read();
                let mv_store = connection.db.get_mv_store().clone();

                for record in schema_rows.values() {
                    let ty = match record.get_value_opt(0) {
                        Some(ValueRef::Text(v)) => v.as_str(),
                        _ => {
                            return Err(LimboError::Corrupt(
                                "sqlite_schema type must be text".to_string(),
                            ))
                        }
                    };
                    let name = match record.get_value_opt(1) {
                        Some(ValueRef::Text(v)) => v.as_str(),
                        _ => {
                            return Err(LimboError::Corrupt(
                                "sqlite_schema name must be text".to_string(),
                            ))
                        }
                    };
                    let table_name = match record.get_value_opt(2) {
                        Some(ValueRef::Text(v)) => v.as_str(),
                        _ => {
                            return Err(LimboError::Corrupt(
                                "sqlite_schema tbl_name must be text".to_string(),
                            ))
                        }
                    };
                    let root_page = match record.get_value_opt(3) {
                        Some(ValueRef::Numeric(Numeric::Integer(v))) => v,
                        _ => {
                            return Err(LimboError::Corrupt(
                                "sqlite_schema root_page must be integer".to_string(),
                            ))
                        }
                    };
                    let sql = match record.get_value_opt(4) {
                        Some(ValueRef::Text(v)) => Some(v.as_str()),
                        _ => None,
                    };
                    fresh.handle_schema_row(
                        ty,
                        name,
                        table_name,
                        root_page,
                        sql,
                        &syms,
                        &mut from_sql_indexes,
                        &mut automatic_indices,
                        &mut dbsp_state_roots,
                        &mut dbsp_state_index_roots,
                        &mut materialized_view_info,
                        mv_store.as_ref(),
                        connection.experimental_triggers_enabled(),
                    )?;
                }
                fresh.populate_indices(
                    &syms,
                    from_sql_indexes,
                    automatic_indices,
                    mv_store.is_some(),
                )?;
                fresh.populate_materialized_views(
                    materialized_view_info,
                    dbsp_state_roots,
                    dbsp_state_index_roots,
                )?;
                Self::rehydrate_table_valued_functions(
                    &mut fresh,
                    &preserved_table_valued_functions,
                );

                let fresh = Arc::new(fresh);
                *connection.schema.write() = fresh.clone();
                *connection.db.schema.lock() = fresh;
                Ok(())
            };

        loop {
            let mut get_index_info = |index_id: MVTableId| -> Result<Arc<IndexInfo>> {
                if let Some(index_info) = index_infos.get(&index_id) {
                    Ok(index_info.clone())
                } else {
                    // Flush any pending schema rebuild so we can see newly created indexes.
                    if needs_schema_rebuild.get() {
                        rebuild_schema(&connection, &schema_rows)?;
                        index_infos.clear();
                        needs_schema_rebuild.set(false);
                    }
                    let schema = connection.schema.read();
                    let root_page = self
                        .table_id_to_rootpage
                        .get(&index_id)
                        .and_then(|entry| *entry.value())
                        .map(|value| value as i64)
                        .unwrap_or_else(|| i64::from(index_id)); // this can be negative for non-checkpointed indexes

                    let index = schema
                        .indexes
                        .values()
                        .flatten()
                        .find(|idx| idx.root_page == root_page)
                        .ok_or_else(|| {
                            LimboError::InternalError(format!(
                                "Index with root page {root_page} not found in schema",
                            ))
                        })?;
                    let index_info = Arc::new(IndexInfo::new_from_index(index));
                    index_infos.insert(index_id, index_info.clone());
                    Ok(index_info)
                }
            };
            let next_rec = reader.next_record(&pager.io, &mut get_index_info)?;

            tracing::trace!("next_rec {next_rec:?}");

            match next_rec {
                StreamingResult::UpsertTableRow {
                    row,
                    rowid,
                    commit_ts,
                    btree_resident,
                } => {
                    max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                    if commit_ts <= replay_cutoff_ts {
                        continue;
                    }
                    let is_schema_row = rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID;
                    if is_schema_row {
                        let row_data = row.payload().to_vec();
                        let record = ImmutableRecord::from_bin_record(row_data);
                        if record.column_count() < 5 {
                            return Err(LimboError::Corrupt(format!(
                                "sqlite_schema row must have at least 5 columns, got {}",
                                record.column_count()
                            )));
                        }
                        let Some(ValueRef::Text(row_type)) = record.get_value_opt(0) else {
                            return Err(LimboError::Corrupt(
                                "sqlite_schema type must be text".to_string(),
                            ));
                        };
                        let row_type = row_type.as_str();
                        let val = match record.get_value_opt(3) {
                            Some(v) => v,
                            None => {
                                return Err(LimboError::InternalError(
                                    "Expected at least 5 columns in sqlite_schema".to_string(),
                                ));
                            }
                        };
                        let ValueRef::Numeric(crate::numeric::Numeric::Integer(root_page)) = val
                        else {
                            panic!("Expected integer value for root page, got {val:?}");
                        };
                        let sql = match record.get_value_opt(4) {
                            Some(ValueRef::Text(v)) => Some(v.as_str()),
                            _ => None,
                        };
                        let is_virtual_table = row_type == "table"
                            && sql.is_some_and(|sql| {
                                contains_ignore_ascii_case!(sql.as_bytes(), b"create virtual")
                            });
                        let has_btree = match row_type {
                            "index" => true,
                            "table" => !is_virtual_table,
                            _ => false,
                        };
                        if has_btree {
                            if root_page == 0 {
                                return Err(LimboError::Corrupt(format!(
                                    "sqlite_schema root_page=0 for btree {row_type}"
                                )));
                            }
                            if root_page < 0 {
                                let table_id = self.get_table_id_from_root_page(root_page);
                                if let Some(entry) = self.table_id_to_rootpage.get(&table_id) {
                                    if let Some(value) = *entry.value() {
                                        panic!("Logical log contains an insertion of a sqlite_schema record that has both a negative root page and a positive root page: {root_page} & {value}");
                                    }
                                }
                                self.insert_table_id_to_rootpage(table_id, None);
                            } else {
                                let table_id = self.get_table_id_from_root_page(root_page);
                                let Some(entry) = self.table_id_to_rootpage.get(&table_id) else {
                                    panic!("Logical log contains root page reference {root_page} that does not exist in the table_id_to_rootpage map");
                                };
                                let Some(value) = *entry.value() else {
                                    panic!("Logical log contains root page reference {root_page} that does not have a root page in the table_id_to_rootpage map");
                                };
                                turso_assert_eq!(value, root_page as u64, "logical log root page does not match table_id_to_rootpage map", { "root_page": root_page, "map_value": value });
                            }
                        } else if root_page != 0 {
                            return Err(LimboError::Corrupt(format!(
                                "sqlite_schema root_page must be 0 for {row_type}, got {root_page}"
                            )));
                        }
                        let rowid_int = rowid.row_id.to_int_or_panic();
                        schema_rows.insert(rowid_int, record);
                        needs_schema_rebuild.set(true);
                    } else {
                        turso_assert!(self.table_id_to_rootpage.get(&rowid.table_id).is_some(),
                        "Logical log contains a row version insert with a table id that does not exist in the table_id_to_rootpage map",
                        {"table_id": rowid.table_id,
                            "table_id_to_rootpage_map": format!("{:?}", self.table_id_to_rootpage.iter().collect::<Vec<_>>())
                        });
                    }

                    let version_id = self.get_version_id();
                    let row_version = RowVersion {
                        id: version_id,
                        begin: Some(TxTimestampOrID::Timestamp(commit_ts)),
                        end: None,
                        row: row.clone(),
                        btree_resident,
                    };
                    self.insert_version_sorted(rowid.clone(), row_version);
                    let allocator = self.get_rowid_allocator(&rowid.table_id);
                    allocator.insert_row_id_maybe_update(rowid.row_id.to_int_or_panic());
                }
                StreamingResult::DeleteTableRow {
                    rowid,
                    commit_ts,
                    btree_resident,
                } => {
                    max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                    if commit_ts <= replay_cutoff_ts {
                        continue;
                    }
                    turso_assert!(self.table_id_to_rootpage.get(&rowid.table_id).is_some(),
                        "Logical log contains a row version delete with a table id that does not exist in the table_id_to_rootpage map",
                        {"rootpage_map": rowid.table_id});
                    if let Some(chain_entry) = self.rows.get(&rowid) {
                        // Row exists in memory — try to find the current (non-ended) version
                        // that was committed before this delete, and mark it as ended. If no
                        // such version exists (e.g. it was already GC'd or this is a B-tree
                        // resident row not yet in memory), insert a tombstone instead.
                        let chain = chain_entry.value();
                        let found = chain.iter().find(|node| {
                            node.end().is_none()
                                && matches!(node.begin(), Some(TxTimestampOrID::Timestamp(b)) if b < commit_ts)
                        });
                        if let Some(existing) = found {
                            existing.set_end(Some(TxTimestampOrID::Timestamp(commit_ts)));
                        } else {
                            let version_id = self.get_version_id();
                            let row = Row::new_table_row(rowid.clone(), Vec::new(), 0);
                            let row_version = RowVersion {
                                id: version_id,
                                begin: None,
                                end: Some(TxTimestampOrID::Timestamp(commit_ts)),
                                row,
                                btree_resident,
                            };
                            self.insert_version_sorted(rowid.clone(), row_version);
                        }
                    } else {
                        let version_id = self.get_version_id();
                        let row = Row::new_table_row(rowid.clone(), Vec::new(), 0);
                        let row_version = RowVersion {
                            id: version_id,
                            begin: None,
                            end: Some(TxTimestampOrID::Timestamp(commit_ts)),
                            row,
                            btree_resident,
                        };
                        self.insert_version_sorted(rowid.clone(), row_version);
                    }
                    if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
                        let rowid_int = rowid.row_id.to_int_or_panic();
                        let record = schema_rows.get(&rowid_int).ok_or_else(|| {
                            LimboError::Corrupt(format!(
                                "Logical log deletes sqlite_schema rowid {rowid_int} that does not exist in merged schema state"
                            ))
                        })?;
                        if record.column_count() < 5 {
                            return Err(LimboError::Corrupt(format!(
                                "sqlite_schema row must have at least 5 columns, got {}",
                                record.column_count()
                            )));
                        }
                        let Some(ValueRef::Text(row_type)) = record.get_value_opt(0) else {
                            return Err(LimboError::Corrupt(
                                "sqlite_schema type must be text".to_string(),
                            ));
                        };
                        let row_type = row_type.as_str();
                        let Some(ValueRef::Numeric(Numeric::Integer(root_page))) =
                            record.get_value_opt(3)
                        else {
                            return Err(LimboError::Corrupt(
                                "sqlite_schema root_page must be integer".to_string(),
                            ));
                        };
                        if (row_type == "table" || row_type == "index") && root_page > 0 {
                            dropped_root_pages.insert(root_page);
                        }
                        schema_rows.remove(&rowid_int);
                        needs_schema_rebuild.set(true);
                    }
                }
                StreamingResult::UpsertIndexRow {
                    row,
                    rowid,
                    commit_ts,
                    btree_resident,
                } => {
                    max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                    if commit_ts <= replay_cutoff_ts {
                        continue;
                    }
                    let version_id = self.get_version_id();
                    let row_version = RowVersion {
                        id: version_id,
                        begin: Some(TxTimestampOrID::Timestamp(commit_ts)),
                        end: None,
                        row: row.clone(),
                        btree_resident,
                    };
                    let RowKey::Record(sortable_key) = rowid.row_id.clone() else {
                        panic!("Index writes must be to a record");
                    };
                    let sortable_key =
                        self.get_or_create_index_key_arc(rowid.table_id, sortable_key);
                    self.insert_index_version(rowid.table_id, sortable_key, row_version);
                }
                StreamingResult::DeleteIndexRow {
                    row,
                    rowid,
                    commit_ts,
                    btree_resident,
                } => {
                    max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                    if commit_ts <= replay_cutoff_ts {
                        continue;
                    }
                    let RowKey::Record(sortable_key) = rowid.row_id.clone() else {
                        panic!("Index writes must be to a record");
                    };
                    let sortable_key =
                        self.get_or_create_index_key_arc(rowid.table_id, sortable_key);
                    if let Some(index_map) = self.index_rows.get(&rowid.table_id) {
                        if let Some(chain_entry) = index_map.value().get(&sortable_key) {
                            let chain = chain_entry.value();
                            let found = chain.iter().find(|node| {
                                node.end().is_none()
                                    && matches!(node.begin(), Some(TxTimestampOrID::Timestamp(b)) if b < commit_ts)
                            });
                            if let Some(existing) = found {
                                existing.set_end(Some(TxTimestampOrID::Timestamp(commit_ts)));
                                continue;
                            }
                        }
                    }
                    let version_id = self.get_version_id();
                    let row_version = RowVersion {
                        id: version_id,
                        begin: None,
                        end: Some(TxTimestampOrID::Timestamp(commit_ts)),
                        row: row.clone(),
                        btree_resident,
                    };
                    self.insert_index_version(rowid.table_id, sortable_key, row_version);
                }
                StreamingResult::Eof => {
                    let mut log = self.storage.logical_log.write();
                    log.offset = reader.last_valid_offset() as u64;
                    log.running_crc = reader.running_crc();
                    break;
                }
            }
        }
        // Flush any remaining pending schema rebuild after processing all log records.
        if needs_schema_rebuild.get() {
            rebuild_schema(&connection, &schema_rows)?;
        }

        assert!(
            max_commit_ts_seen >= persistent_tx_ts_max,
            "replay clock would rewind below metadata boundary: max_commit_ts_seen={max_commit_ts_seen} persistent_tx_ts_max={persistent_tx_ts_max}"
        );
        connection.with_schema_mut(|schema| {
            schema.dropped_root_pages = dropped_root_pages;
        });
        *connection.db.schema.lock() = connection.schema.read().clone();
        self.clock.reset(max_commit_ts_seen + 1);
        self.last_committed_tx_ts
            .store(max_commit_ts_seen, Ordering::SeqCst);
        Ok(true)
    }

    pub fn set_checkpoint_threshold(&self, threshold: i64) {
        self.storage.set_checkpoint_threshold(threshold)
    }

    pub fn checkpoint_threshold(&self) -> i64 {
        self.storage.checkpoint_threshold()
    }

    pub fn get_real_table_id(&self, table_id: i64) -> i64 {
        let entry = self.table_id_to_rootpage.get(&MVTableId::from(table_id));
        if let Some(entry) = entry {
            entry.value().map_or(table_id, |value| value as i64)
        } else {
            table_id
        }
    }

    pub fn get_rowid_allocator(&self, table_id: &MVTableId) -> Arc<RowidAllocator> {
        let mut map = self.table_id_to_last_rowid.write();
        if map.contains_key(table_id) {
            map.get(table_id).unwrap().clone()
        } else {
            let allocator = Arc::new(RowidAllocator {
                lock: TursoRwLock::new(),
                max_rowid: RwLock::new(None),
                initialized: AtomicBool::new(false),
            });
            map.insert(*table_id, allocator.clone());
            allocator
        }
    }

    pub fn is_btree_allocated(&self, table_id: &MVTableId) -> bool {
        let maybe_root_page = self.table_id_to_rootpage.get(table_id);
        maybe_root_page.is_some_and(|entry| entry.value().is_some())
    }
}

/// Atomic rollback of a RowVersionNode. Same logic as rollback_row_version
/// but uses atomic set_begin/set_end.
fn rollback_node_version(tx_id: u64, node: &RowVersionNode) {
    if node.begin() == Some(TxTimestampOrID::TxID(tx_id)) {
        node.mark_garbage();
    } else if node.end() == Some(TxTimestampOrID::TxID(tx_id)) {
        node.set_end(None);
    }
}

impl RowidAllocator {
    /// Returns None if no rowid could be allocated.
    /// Returns Some((new_rowid, prev_last_rowid)) where prev_last_rowid is None if table was empty
    pub fn get_next_rowid(&self) -> Option<(i64, Option<i64>)> {
        let mut last_rowid_guard = self.max_rowid.write();
        if last_rowid_guard.is_none() {
            // Case 1. Table is empty
            // Database is empty because there is no last rowid
            *last_rowid_guard = Some(1);
            tracing::trace!("get_next_rowid(empty, 1)");
            Some((1, None))
        } else {
            // Case 2. Table is not empty
            let last_rowid = last_rowid_guard.unwrap();
            if last_rowid == i64::MAX {
                tracing::trace!("get_next_rowid(max)");
                None
            } else {
                let next_rowid = last_rowid + 1;
                *last_rowid_guard = Some(next_rowid);
                tracing::trace!("get_next_rowid({next_rowid})");
                Some((next_rowid, Some(last_rowid)))
            }
        }
    }

    pub fn insert_row_id_maybe_update(&self, rowid: i64) {
        let mut last_rowid = self.max_rowid.write();
        if let Some(last_rowid) = last_rowid.as_mut() {
            *last_rowid = (*last_rowid).max(rowid);
        } else {
            *last_rowid = Some(rowid);
        }
    }

    pub fn is_uninitialized(&self) -> bool {
        !self.initialized.load(Ordering::SeqCst)
    }

    pub fn initialize(&self, rowid: Option<i64>) {
        tracing::trace!("initialize({rowid:?})");
        let mut last_rowid = self.max_rowid.write();
        *last_rowid = rowid;
        self.initialized.store(true, Ordering::SeqCst);
    }

    pub fn lock(&self) -> bool {
        self.lock.write()
    }

    pub fn unlock(&self) {
        self.lock.unlock()
    }
}

pub fn create_seek_range<K: Ord>(
    limit_boundary: Bound<K>,
    direction: IterationDirection,
) -> (Bound<K>, Bound<K>) {
    if direction == IterationDirection::Forwards {
        (limit_boundary, Bound::Unbounded)
    } else {
        (Bound::Unbounded, limit_boundary)
    }
}

fn is_write_write_conflict_ts(
    txs: &SkipMap<TxID, Transaction>,
    tx: &Transaction,
    end: &Option<TxTimestampOrID>,
) -> bool {
    match end {
        Some(TxTimestampOrID::TxID(rv_end)) => {
            let te = txs
                .get(rv_end)
                .expect("transaction should exist in txs map");
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
    /// A row is visible to a transaction if:
    /// * Begin is visible to the transaction
    /// * End timestamp is not applicable yet, meaning deletion of row is not visible to this transaction
    pub fn is_visible_to(&self, tx: &Transaction, txs: &SkipMap<TxID, Transaction>) -> bool {
        is_begin_visible(txs, tx, self) && is_end_visible(txs, tx, self)
    }

    /// Check if this version indicates the B-tree row has been modified (updated or deleted).
    ///
    /// A version is "relevant" to a transaction if:
    /// 1. The version is fully visible (begin visible AND end visible), OR
    /// 2. The version has an end timestamp that indicates the row was deleted before/at the transaction's begin, OR
    /// 3. The current transaction itself has deleted/updated this row (end = current tx_id)
    ///
    /// This is used by dual-cursor to determine if a B-tree row should be shown or hidden.
    pub fn is_btree_invalidating_version(
        &self,
        tx: &Transaction,
        txs: &SkipMap<TxID, Transaction>,
    ) -> bool {
        // If the version is fully visible, it invalidates the B-tree
        if self.is_visible_to(tx, txs) {
            return true;
        }

        // Check if this version represents a deletion/update that affects us
        match self.end {
            Some(TxTimestampOrID::Timestamp(end_ts)) => {
                // Row was deleted at end_ts. If we started at or after end_ts, we shouldn't see it
                tx.begin_ts >= end_ts
            }
            Some(TxTimestampOrID::TxID(end_tx_id)) => {
                // Row is being deleted/updated by another transaction
                // If it's OUR transaction, the B-tree row is invalid (we deleted/updated it)
                end_tx_id == tx.tx_id
            }
            None => false,
        }
    }
}

fn is_begin_visible(txs: &SkipMap<TxID, Transaction>, tx: &Transaction, rv: &RowVersion) -> bool {
    is_begin_visible_ts(txs, tx, &rv.begin, &rv.end)
}

fn is_end_visible(
    txs: &SkipMap<TxID, Transaction>,
    current_tx: &Transaction,
    row_version: &RowVersion,
) -> bool {
    is_end_visible_ts(txs, current_tx, &row_version.begin, &row_version.end)
}

/// Core begin-visibility check operating on decoded timestamp values.
fn is_begin_visible_ts(
    txs: &SkipMap<TxID, Transaction>,
    tx: &Transaction,
    begin: &Option<TxTimestampOrID>,
    end: &Option<TxTimestampOrID>,
) -> bool {
    match begin {
        Some(TxTimestampOrID::Timestamp(rv_begin_ts)) => tx.begin_ts >= *rv_begin_ts,
        Some(TxTimestampOrID::TxID(rv_begin)) => {
            let tb = txs
                .get(rv_begin)
                .unwrap_or_else(|| panic!("transaction {rv_begin:?} should exist in txs map"));
            let tb = tb.value();
            let visible = match tb.state.load() {
                TransactionState::Active => tx.tx_id == tb.tx_id && end.is_none(),
                TransactionState::Preparing(_) => false, // NOTICE: makes sense for snapshot isolation, not so much for serializable!
                TransactionState::Committed(committed_ts) => tx.begin_ts >= committed_ts,
                TransactionState::Aborted => false,
                TransactionState::Terminated => {
                    tracing::debug!("TODO: should reread rv's end field - it should have updated the timestamp in the row version by now");
                    false
                }
            };
            tracing::trace!(
                "is_begin_visible: tx={tx}, tb={tb} rv = {:?}-{:?} visible = {visible}",
                begin,
                end
            );
            visible
        }
        None => false,
    }
}

/// Core end-visibility check operating on decoded timestamp values.
fn is_end_visible_ts(
    txs: &SkipMap<TxID, Transaction>,
    current_tx: &Transaction,
    begin: &Option<TxTimestampOrID>,
    end: &Option<TxTimestampOrID>,
) -> bool {
    match end {
        Some(TxTimestampOrID::Timestamp(rv_end_ts)) => current_tx.begin_ts < *rv_end_ts,
        Some(TxTimestampOrID::TxID(rv_end)) => {
            let other_tx = txs
                .get(rv_end)
                .unwrap_or_else(|| panic!("Transaction {rv_end} not found"));
            let other_tx = other_tx.value();
            let visible = match other_tx.state.load() {
                // V's sharp mind discovered an issue with the hekaton paper which basically states that a
                // transaction can see a row version if the end is a TXId only if it isn't the same transaction.
                // Source: https://avi.im/blag/2023/hekaton-paper-typo/
                TransactionState::Active => current_tx.tx_id != other_tx.tx_id,
                // Table 2 (Hekaton): If TS > RT, V is visible. If TS < RT, T speculatively ignores V.
                TransactionState::Preparing(end_ts) => current_tx.begin_ts < end_ts,
                TransactionState::Committed(committed_ts) => current_tx.begin_ts < committed_ts,
                TransactionState::Aborted => true,
                // Table 2 (Hekaton): Reread V's End field. In this codebase Terminated is only
                // reachable from Aborted, and abort rollback resets end to None → visible.
                TransactionState::Terminated => true,
            };
            tracing::trace!(
                "is_end_visible: tx={current_tx}, te={other_tx} rv = {:?}-{:?}  visible = {visible}",
                begin,
                end
            );
            visible
        }
        None => true,
    }
}

/// Check if a `RowVersionNode` is visible to the given transaction.
/// Atomically loads begin/end and checks visibility.
pub fn is_node_visible_to(
    node: &RowVersionNode,
    tx: &Transaction,
    txs: &SkipMap<TxID, Transaction>,
) -> bool {
    let begin = node.begin();
    let end = node.end();
    is_begin_visible_ts(txs, tx, &begin, &end) && is_end_visible_ts(txs, tx, &begin, &end)
}

/// Check if a `RowVersionNode` indicates the B-tree row has been modified.
pub fn is_node_btree_invalidating(
    node: &RowVersionNode,
    tx: &Transaction,
    txs: &SkipMap<TxID, Transaction>,
) -> bool {
    if is_node_visible_to(node, tx, txs) {
        return true;
    }
    match node.end() {
        Some(TxTimestampOrID::Timestamp(end_ts)) => tx.begin_ts >= end_ts,
        Some(TxTimestampOrID::TxID(end_tx_id)) => end_tx_id == tx.tx_id,
        None => false,
    }
}

impl<Clock: LogicalClock> Debug for CommitState<Clock> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initial => write!(f, "Initial"),
            Self::Commit { end_ts } => f.debug_struct("Commit").field("end_ts", end_ts).finish(),
            Self::BeginCommitLogicalLog { end_ts, log_record } => f
                .debug_struct("BeginCommitLogicalLog")
                .field("end_ts", end_ts)
                .field("log_record", log_record)
                .finish(),
            Self::EndCommitLogicalLog { end_ts } => f
                .debug_struct("EndCommitLogicalLog")
                .field("end_ts", end_ts)
                .finish(),
            Self::SyncLogicalLog { end_ts } => f
                .debug_struct("SyncLogicalLog")
                .field("end_ts", end_ts)
                .finish(),
            Self::Checkpoint { state_machine: _ } => f.debug_struct("Checkpoint").finish(),
            Self::CommitEnd { end_ts } => {
                f.debug_struct("CommitEnd").field("end_ts", end_ts).finish()
            }
        }
    }
}

impl PartialOrd for RowID {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowID {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Make sure table id is first comparison so that we sort first by table_id and then by
        // rowid. Due to order of the struct, table_id is first which is fine but if we were to
        // change it we would bring chaos.
        match self.table_id.cmp(&other.table_id) {
            std::cmp::Ordering::Equal => self.row_id.cmp(&other.row_id),
            ord => ord,
        }
    }
}
