use crate::alloc::{ConcurrentAllocator, TursoAllocator, TursoTryWithCapacityExt};
use crate::mvcc::clock::LogicalClock;
use crate::mvcc::cursor::{static_iterator_hack, MvccIterator};
#[cfg(any(test, injected_yields))]
use crate::mvcc::yield_hooks::{ProvidesYieldContext, YieldContext, YieldPointMarker};
use crate::mvcc::yield_points::{inject_transition_failure, inject_transition_yield};
use crate::schema::{Schema, Sequence, Table};
use crate::skiplist::comparator::BasicComparator;
use crate::skiplist::map::Entry;
use crate::skiplist::SkipMap;
use crate::state_machine::StateMachine;
use crate::state_machine::StateTransition;
use crate::state_machine::TransitionResult;
use crate::storage::btree::BTreeCursor;
use crate::storage::btree::BTreeKey;
use crate::storage::btree::CursorTrait;
use crate::storage::btree::CursorValidState;
use crate::storage::pager::SavepointResult;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::wal::{CheckpointMode, CheckpointResult, TursoRwLock};
use crate::sync::atomic::{AtomicBool, AtomicI64};
use crate::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
use crate::PageSize;
use crate::Result;
#[cfg(feature = "conn_raw_api")]
use crate::Value;
use crate::ValueRef;
use crate::{
    contains_ignore_ascii_case, eq_ignore_ascii_case, match_ignore_ascii_case, Completion,
};
use crate::{io::FileSyncType, io_yield_one, return_if_io};
use crate::{
    turso_assert, turso_assert_eq, turso_assert_less_than, turso_assert_reachable, Numeric,
};
use crate::{Connection, Pager, SyncMode};
use rustc_hash::FxHashMap as HashMap;
use rustc_hash::FxHashSet as HashSet;
use std::collections::{BTreeSet, HashMap as StdHashMap};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Bound;
#[cfg(any(test, injected_yields))]
use strum::EnumCount;
use tracing::instrument;
use tracing::Level;

pub mod checkpoint_state_machine;
pub use checkpoint_state_machine::{
    sqlite_schema_btree_identity, CheckpointState, CheckpointStateMachine,
};

#[cfg(feature = "conn_raw_api")]
use super::persistent_storage::logical_log::{
    encode_delete_portable_extension, parse_ops_from_plaintext, LOG_RECORD_PREFIX_SIZE,
};
use super::persistent_storage::logical_log::{
    HeaderReadResult, IndexOpKind, ParsedOp, StreamingLogicalLogReader, StreamingResult,
    LOG_HDR_SIZE,
};
#[cfg(feature = "conn_raw_api")]
use super::portable_logical::{
    is_portable_logical_name, is_portable_table_schema_row, portable_schema_row_from_record,
    PortableLogicalBuilder, PortableObjectMapEntry,
};

#[cfg(test)]
pub mod hermitage_tests;
#[cfg(test)]
pub mod tests;

/// Sentinel value for `MvStore::exclusive_tx` indicating no exclusive transaction is active.
const NO_EXCLUSIVE_TX: u64 = 0;

/// Convert a sequence backing-table row into the exclusive upper bound used by
/// sync scans. This is intentionally tailored to ascending non-CYCLE sequences,
/// which is the shape used by AUTOINCREMENT CDC ids.
pub(crate) fn first_unsafe_sequence_watermark(seq: &Sequence, value: i64, is_called: bool) -> i64 {
    if !is_called {
        return value;
    }
    if seq.increment_by > 0 && !seq.cycle {
        value.checked_add(seq.increment_by).unwrap_or(value)
    } else {
        value
    }
}

#[cfg(not(any(test, injected_yields)))]
struct YieldContext;

/// A table ID for MVCC.
/// MVCC table IDs are always negative. Their corresponding rootpage entry in sqlite_schema
/// is the same negative value if the table has not been checkpointed yet. Otherwise, the root page
/// will be positive and corresponds to the actual physical page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct MVTableId(i64);

/// The versions of a single row
pub type RowVersions = Arc<RwLock<Vec<RowVersion>>>;
type TableRowEntry<'a, A = TursoAllocator> = Entry<'a, RowID, RowVersions, BasicComparator, A>;
type IndexRowEntry<'a, A = TursoAllocator> =
    Entry<'a, Arc<SortableIndexKey>, RowVersions, BasicComparator, A>;
type TableRowIterator<'a, A = TursoAllocator> =
    Box<dyn Iterator<Item = TableRowEntry<'a, A>> + Send + Sync + 'a>;
type IndexRowIterator<'a, A = TursoAllocator> =
    Box<dyn Iterator<Item = IndexRowEntry<'a, A>> + Send + Sync + 'a>;

/// Per-index map of sortable keys to their version chains, stored as the
/// values of [`MvStore::index_rows`].
pub type IndexRowsMap<A = TursoAllocator> =
    SkipMap<Arc<SortableIndexKey>, RowVersions, BasicComparator, A>;

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

    pub fn new_from_values(values: Vec<ValueRef>, metadata: Arc<IndexInfo>) -> Result<Self> {
        let len = values.len();
        Ok(Self {
            key: ImmutableRecord::from_values(values, len)?,
            metadata,
        })
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
    Record(Arc<SortableIndexKey>),
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
    pub data: Option<Arc<[u8]>>,
    pub column_count: usize,
}

impl Row {
    pub fn new_table_row(id: RowID, data: Vec<u8>, column_count: usize) -> Self {
        Self {
            id,
            data: Some(Arc::from(data)),
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
            RowKey::Int(_) => self.data.as_deref().expect("table rows should have data"),
            RowKey::Record(ref sortable_key) => sortable_key.key.as_blob(),
        }
    }
}

/// Packed representation of `Option<TxTimestampOrID>` in a single `u64`,
/// halving the size of the `begin`/`end` fields (16 bytes each → 8).
///
/// Layout (top two bits are the tag):
/// * `0`                  → `None`
/// * `(1 << 62) | value`  → `Some(Timestamp(value))`
/// * `(1 << 63) | value`  → `Some(TxID(value))`
///
/// `value` occupies the low 62 bits. Timestamps and transaction IDs are
/// monotonic counters that start near zero, so 62 bits (~4.6e18) is never
/// exhausted; `pack` asserts this invariant. The two distinct tag bits (rather
/// than a zero sentinel) are required because `Timestamp(0)` is a real value —
/// the logical clock hands out timestamp 0 to the first transaction.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct PackedTs(u64);

impl PackedTs {
    const TIMESTAMP_TAG: u64 = 1 << 62;
    const TXID_TAG: u64 = 1 << 63;
    const VALUE_MASK: u64 = (1 << 62) - 1;
    const NONE: PackedTs = PackedTs(0);

    #[inline]
    pub(crate) fn pack(value: Option<TxTimestampOrID>) -> Self {
        match value {
            None => Self::NONE,
            Some(TxTimestampOrID::Timestamp(ts)) => {
                turso_assert!(ts <= Self::VALUE_MASK, "timestamp exceeds 62-bit range");
                PackedTs(Self::TIMESTAMP_TAG | ts)
            }
            Some(TxTimestampOrID::TxID(id)) => {
                turso_assert!(id <= Self::VALUE_MASK, "tx id exceeds 62-bit range");
                PackedTs(Self::TXID_TAG | id)
            }
        }
    }

    #[inline]
    fn unpack(self) -> Option<TxTimestampOrID> {
        if self.0 == 0 {
            None
        } else if self.0 & Self::TXID_TAG != 0 {
            Some(TxTimestampOrID::TxID(self.0 & Self::VALUE_MASK))
        } else {
            Some(TxTimestampOrID::Timestamp(self.0 & Self::VALUE_MASK))
        }
    }
}

impl std::fmt::Debug for PackedTs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.unpack(), f)
    }
}

/// A row version.
#[derive(Clone, Debug, PartialEq)]
pub struct RowVersion {
    /// Unique identifier for this version within the MvStore.
    /// Used for savepoint tracking to identify specific versions to rollback.
    pub id: u64,
    /// `begin`/`end` timestamps are bit-packed. Read them through the
    /// [`RowVersion::begin`]/[`RowVersion::end`] accessors and write them with
    /// [`RowVersion::set_begin`]/[`RowVersion::set_end`]; the raw `PackedTs`
    /// fields are `pub(crate)` only so they can be set in struct literals (via
    /// `PackedTs::pack`).
    pub(crate) begin: PackedTs,
    pub(crate) end: PackedTs,
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

/// A log record contains all durable effects of a committed transaction,
/// pre-serialized into a frame buffer that the logical-log flush path
/// finalizes (backfills the TX header, appends the CRC trailer, optionally
/// chunk-encrypts the payload) and writes to disk.
#[derive(Clone, Debug)]
pub struct LogRecord {
    pub(crate) tx_timestamp: TxID,
    /// Frame buffer that grows in place into the on-disk representation.
    /// The first `LOG_HDR_SIZE + TX_HEADER_SIZE` bytes are pre-reserved
    /// (zeros) so that op-entry appends land at the correct on-disk
    /// offset; the flush path backfills the framing prefix and appends
    /// the trailer.
    pub buf: Vec<u8>,
    /// Number of op entries appended to `buf`. Includes any header op.
    pub op_count: u32,
    /// True once a `DatabaseHeader` op has been appended. At most one
    /// header op is allowed per transaction.
    pub has_header: bool,
    /// Portable logical-change metadata stored alongside the MVCC recovery log.
    ///
    /// Recovery ignores this field. Raw-log consumers use it to resolve the
    /// recovery ops' MVCC table ids and read transaction-level metadata.
    #[cfg(feature = "conn_raw_api")]
    pub portable_changes: Vec<u8>,
    /// True when the committing connection requested portable logical-change
    /// frames, even if this transaction has no client-visible metadata.
    #[cfg(feature = "conn_raw_api")]
    pub portable_changes_enabled: bool,
}

impl LogRecord {
    pub(crate) fn new(tx_timestamp: TxID) -> Self {
        Self {
            tx_timestamp,
            // Pre-reserve the framing prefix at the front of buf:
            //   [LOG_HDR slot (56B) | TX_HEADER slot (24B) | <ops here>]
            // The log-header slot is only filled on the very first write to
            // a log file; otherwise it stays zero and the flush path wraps
            // the buf with `Buffer::new_with_start(..., LOG_HDR_SIZE)` so
            // those 56 bytes never reach disk.
            buf: vec![0u8; crate::mvcc::persistent_storage::logical_log::LOG_RECORD_PREFIX_SIZE],
            op_count: 0,
            has_header: false,
            #[cfg(feature = "conn_raw_api")]
            portable_changes: Vec::new(),
            #[cfg(feature = "conn_raw_api")]
            portable_changes_enabled: false,
        }
    }

    /// True iff no ops (row versions or header) have been appended.
    pub fn is_empty(&self) -> bool {
        let empty = self.op_count == 0;
        turso_assert!(
            !empty || !self.has_header,
            "header shouldn't have been written"
        );
        empty
    }

    /// Test-only constructor that eagerly serializes a list of row versions
    /// and an optional `DatabaseHeader` into the payload buffer using the
    /// production wire format. Production code uses
    /// [`DurableStorage::serialize_row_version`] and
    /// [`DurableStorage::serialize_database_header`] instead so the bytes are
    /// appended one op at a time.
    #[cfg(test)]
    pub(crate) fn for_test(
        tx_timestamp: TxID,
        row_versions: &[RowVersion],
        header: Option<DatabaseHeader>,
    ) -> Self {
        let mut record = Self::new(tx_timestamp);
        for rv in row_versions {
            record.push_row_version_for_test(rv);
        }
        if let Some(hdr) = header {
            record.set_header_for_test(&hdr);
        }
        record
    }

    /// Test-only: append one row-version op to the payload buffer.
    #[cfg(test)]
    pub(crate) fn push_row_version_for_test(&mut self, row_version: &RowVersion) {
        crate::mvcc::persistent_storage::logical_log::serialize_op_entry(
            &mut self.buf,
            row_version,
            None,
        )
        .expect("failed to serialize row version in test");
        self.op_count += 1;
    }

    /// Test-only: append a `DatabaseHeader` op to the payload buffer.
    #[cfg(test)]
    pub(crate) fn set_header_for_test(&mut self, header: &DatabaseHeader) {
        assert!(!self.has_header, "header op appended twice in test");
        crate::mvcc::persistent_storage::logical_log::serialize_header_entry(&mut self.buf, header);
        self.has_header = true;
        self.op_count += 1;
    }
}

#[cfg(feature = "conn_raw_api")]
fn portable_table_id_from_rootpage(rootpage: i64) -> MVTableId {
    if rootpage > 0 {
        MVTableId::from(-rootpage)
    } else {
        MVTableId::from(rootpage)
    }
}

#[derive(Clone, Debug)]
#[cfg(feature = "conn_raw_api")]
struct PortableTableRef {
    name: String,
}

#[cfg(feature = "conn_raw_api")]
fn rootpage_for_mv_table_id<Clock: LogicalClock, A: ConcurrentAllocator>(
    mvcc_store: &MvStore<Clock, A>,
    table_id: MVTableId,
) -> i64 {
    mvcc_store
        .table_id_to_rootpage
        .get(&table_id)
        .and_then(|entry| *entry.value())
        .map(|rootpage| rootpage as i64)
        .unwrap_or_else(|| i64::from(table_id))
}

#[cfg(feature = "conn_raw_api")]
fn portable_table_name_for_mv_table_id<Clock: LogicalClock, A: ConcurrentAllocator>(
    connection: &Connection,
    mvcc_store: &MvStore<Clock, A>,
    table_id: MVTableId,
) -> Option<String> {
    let rootpage = rootpage_for_mv_table_id(mvcc_store, table_id);
    if rootpage == 0 {
        return None;
    }
    let schema = connection.schema.read();
    if let Some(name) = schema.table_name_for_root_page(rootpage) {
        return Some(name.to_string());
    }
    let alternate_rootpage = -rootpage;
    if alternate_rootpage == 0 {
        return None;
    }
    schema
        .table_name_for_root_page(alternate_rootpage)
        .map(ToString::to_string)
}

#[cfg(feature = "conn_raw_api")]
fn portable_delete_op_extension_for_row_version<Clock: LogicalClock, A: ConcurrentAllocator>(
    connection: &Connection,
    mvcc_store: &MvStore<Clock, A>,
    row_version: &RowVersion,
) -> Result<Option<Vec<u8>>> {
    if !connection.portable_logical_changes_enabled() {
        return Ok(None);
    }
    if !matches!(row_version.end(), Some(TxTimestampOrID::Timestamp(_))) {
        return Ok(None);
    }
    let RowKey::Int(rowid) = row_version.row.id.row_id else {
        return Ok(None);
    };

    if row_version.row.id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
        let extension =
            encode_delete_portable_extension(Some(row_version.row.payload()), None, Some(rowid));
        return Ok((!extension.is_empty()).then_some(extension));
    }

    let Some(table_name) =
        portable_table_name_for_mv_table_id(connection, mvcc_store, row_version.row.id.table_id)
    else {
        return Ok(None);
    };
    if !is_portable_logical_name(&table_name) {
        return Ok(None);
    }

    let schema = connection.schema.read();
    let Some(table) = schema.get_btree_table(&table_name) else {
        return Ok(None);
    };

    let mut record_values = None;
    let mut pk_values = Vec::with_capacity(table.primary_key_columns.len());
    for (pk_name, _) in &table.primary_key_columns {
        let Some((logical_column, column)) = table.get_column(pk_name) else {
            return Err(LimboError::InternalError(format!(
                "primary key column {pk_name} not found for table {table_name}"
            )));
        };
        if column.is_rowid_alias() {
            pk_values.push(Value::from_i64(rowid));
            continue;
        }
        let values = match &record_values {
            Some(values) => values,
            None => record_values.insert(
                ImmutableRecord::from_bin_record(row_version.row.payload().to_vec())
                    .get_values_owned()?,
            ),
        };
        let physical_column = table.logical_to_physical_column(logical_column);
        let Some(value) = values.get(physical_column).cloned() else {
            return Err(LimboError::Corrupt(format!(
                "DELETE_TABLE record for {table_name} missing primary key column {pk_name}"
            )));
        };
        pk_values.push(value);
    }

    let pk_record = if pk_values.is_empty() {
        Vec::new()
    } else {
        ImmutableRecord::from_values(&pk_values, pk_values.len())?.into_payload()
    };
    let extension = encode_delete_portable_extension(None, Some(&pk_record), Some(rowid));
    Ok((!extension.is_empty()).then_some(extension))
}

/// A transaction timestamp or ID.
///
/// Versions either track a timestamp or a transaction ID, depending on the
/// phase of the transaction. During the active phase, new versions track the
/// transaction ID in the `begin` and `end` fields. After a transaction commits,
/// versions switch to tracking timestamps.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub enum TxTimestampOrID {
    /// A committed transaction's timestamp.
    Timestamp(u64),
    /// The ID of a non-committed transaction.
    TxID(TxID),
}

/// Tracks versions created/modified during a savepoint for rollback.
/// Used for statement-level savepoints in interactive transactions.
#[derive(Debug, Default)]
enum SavepointKind {
    /// Internal savepoint used for statement-level rollback.
    #[default]
    Statement,
    /// User-visible named savepoint.
    Named {
        name: String,
        starts_transaction: bool,
    },
}

/// Tracks row/index version deltas created inside a single savepoint scope.
#[derive(Debug, Default)]
pub struct Savepoint {
    kind: SavepointKind,
    deferred_fk_violations: isize,
    header: DatabaseHeader,
    header_dirty: bool,
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
    newly_added_to_write_set: Vec<(RowID, RowVersions)>,
}

impl Savepoint {
    /// Creates an internal statement savepoint used for per-statement rollback.
    fn statement(header: DatabaseHeader, header_dirty: bool) -> Self {
        Self {
            header,
            header_dirty,
            ..Default::default()
        }
    }

    /// Creates a user-visible named savepoint snapshot.
    fn named(
        name: String,
        starts_transaction: bool,
        deferred_fk_violations: isize,
        header: DatabaseHeader,
        header_dirty: bool,
    ) -> Self {
        Self {
            kind: SavepointKind::Named {
                name,
                starts_transaction,
            },
            deferred_fk_violations,
            header,
            header_dirty,
            ..Default::default()
        }
    }

    /// Merges child savepoint deltas into this savepoint.
    ///
    /// Called when releasing nested savepoints so outer rollback still has a full undo set.
    fn merge_from(&mut self, mut other: Savepoint) {
        self.created_table_versions
            .append(&mut other.created_table_versions);
        self.created_index_versions
            .append(&mut other.created_index_versions);
        self.deleted_table_versions
            .append(&mut other.deleted_table_versions);
        self.deleted_index_versions
            .append(&mut other.deleted_index_versions);
        self.newly_added_to_write_set
            .append(&mut other.newly_added_to_write_set);
    }
}

struct SavepointRollbackResult {
    /// Savepoints that were rolled back, in the order they were created (oldest to newest).
    rolledback_savepoints: Vec<Savepoint>,
    /// Deferred FK counter snapshot captured at the target named savepoint.
    deferred_fk_violations: isize,
}

#[derive(Debug, Default)]
struct WriteSet {
    entries: Vec<(RowID, RowVersions)>,
    /// A set of the pointer addresses of the `RowVersions`. Used to deduplicate entries.
    ///
    /// This is correct because instances of [RowVersions] are created once per [RowID] and then
    /// reused by cloning the [Arc]. It would be nice to encode this in the type system, but I'm
    /// not sure how.
    seen: HashSet<usize>,
}

impl WriteSet {
    fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if this `RowVersions` was not already contained in the write set.
    fn insert(&mut self, id: RowID, row_versions: RowVersions) -> bool {
        let ptr = Arc::as_ptr(&row_versions) as usize;
        if self.seen.insert(ptr) {
            self.entries.push((id, row_versions));
            true
        } else {
            false
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn iter(&self) -> std::slice::Iter<'_, (RowID, RowVersions)> {
        self.entries.iter()
    }

    /// Retain entries where `keep(rowid, row_versions)` returns true.
    fn retain<F: FnMut(&RowID, &RowVersions) -> bool>(&mut self, mut keep: F) {
        let seen = &mut self.seen;
        self.entries.retain(|(rowid, rv)| {
            if keep(rowid, rv) {
                true
            } else {
                seen.remove(&(Arc::as_ptr(rv) as usize));
                false
            }
        });
    }

    /// Clones the write set into a [Vec].
    fn to_vec(&self) -> Vec<(RowID, RowVersions)> {
        self.entries.clone()
    }
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
    /// The transaction write set. Only writer is the [Transaction]'s own connection.
    write_set: Mutex<WriteSet>,
    /// The transaction header.
    header: RwLock<DatabaseHeader>,
    /// True when the transaction mutated its local database header snapshot.
    header_dirty: AtomicBool,
    /// Stack of savepoints for statement-level rollback.
    /// Each savepoint tracks versions created/deleted during that statement.
    savepoint_stack: RwLock<Vec<Savepoint>>,
    /// True when this transaction currently holds the serialized logical-log commit lock.
    pager_commit_lock_held: AtomicBool,
    /// Number of unresolved commit dependencies (must reach 0 before commit).
    /// i.e the number of transactions this transaction is dependent on and waiting for
    /// commit or abort.
    /// Hekaton Section 2.7: "A transaction cannot commit until this counter is zero."
    commit_dep_counter: AtomicU64,
    /// Flag: a depended-on transaction aborted; this transaction must abort too.
    /// Hekaton Section 2.7: "AbortNow that other transactions can set to tell T to abort."
    abort_now: AtomicBool,
    /// Transaction IDs that depend on this transaction (notified on commit/abort).
    /// Hekaton Section 2.7: "CommitDepSet, that stores transaction IDs of the
    /// transactions that depend on T."
    commit_dep_set: Mutex<HashSet<TxID>>,
}

impl Transaction {
    fn new(tx_id: u64, begin_ts: u64, header: DatabaseHeader) -> Transaction {
        Transaction {
            state: TransactionState::Active.into(),
            tx_id,
            begin_ts,
            write_set: Mutex::new(WriteSet::new()),
            header: RwLock::new(header),
            header_dirty: AtomicBool::new(false),
            savepoint_stack: RwLock::new(Vec::new()),
            pager_commit_lock_held: AtomicBool::new(false),
            commit_dep_counter: AtomicU64::new(0),
            abort_now: AtomicBool::new(false),
            commit_dep_set: Mutex::new(HashSet::default()),
        }
    }

    fn insert_to_write_set(&self, id: RowID, row_versions: RowVersions) {
        // Always record in the current savepoint's `newly_added_to_write_set`.
        // Duplicates here are harmless: `rollback_savepoint_changes` collects
        // touched rowids into a BTreeSet (dedup), and the actual write_set
        // removal is gated by `row_has_uncommitted_version_for_tx`, so a row
        // already pinned by a parent savepoint won't be evicted on inner
        // rollback.
        if let Some(savepoint) = self.savepoint_stack.write().last_mut() {
            savepoint
                .newly_added_to_write_set
                .push((id.clone(), row_versions.clone()));
        }
        self.write_set.lock().insert(id, row_versions);
    }

    /// Begin a new savepoint for statement-level tracking.
    fn begin_savepoint(&self) {
        let depth = self.savepoint_stack.read().len();
        tracing::debug!("begin_savepoint(tx_id={}, depth={})", self.tx_id, depth);
        let header = *self.header.read();
        let header_dirty = self.header_dirty.load(Ordering::Acquire);
        self.savepoint_stack
            .write()
            .push(Savepoint::statement(header, header_dirty));
    }

    /// Begin a new named savepoint. If `starts_transaction` is true, this savepoint represents the
    /// beginning of an interactive transaction and will be used to track deferred FK violations
    /// for that transaction.
    fn begin_named_savepoint(
        &self,
        name: String,
        starts_transaction: bool,
        deferred_fk_violations: isize,
    ) {
        let depth = self.savepoint_stack.read().len();
        tracing::debug!(
            "begin_named_savepoint(tx_id={}, depth={}, name={})",
            self.tx_id,
            depth,
            name
        );
        let header = *self.header.read();
        let header_dirty = self.header_dirty.load(Ordering::Acquire);
        self.savepoint_stack.write().push(Savepoint::named(
            name,
            starts_transaction,
            deferred_fk_violations,
            header,
            header_dirty,
        ));
    }

    /// Release the newest savepoint (statement completed successfully).
    fn release_savepoint(&self) {
        let depth = self.savepoint_stack.read().len();
        tracing::debug!("release_savepoint(tx_id={}, depth={})", self.tx_id, depth);
        let mut savepoints = self.savepoint_stack.write();
        if !matches!(
            savepoints.last().map(|savepoint| &savepoint.kind),
            Some(SavepointKind::Statement)
        ) {
            return;
        }
        let savepoint = savepoints.pop().expect("savepoint must exist");
        if let Some(parent) = savepoints.last_mut() {
            parent.merge_from(savepoint);
        }
    }

    fn pop_statement_savepoint(&self) -> Option<Savepoint> {
        let mut savepoints = self.savepoint_stack.write();
        if !matches!(
            savepoints.last().map(|savepoint| &savepoint.kind),
            Some(SavepointKind::Statement)
        ) {
            return None;
        }
        savepoints.pop()
    }

    /// Release a named savepoint. If this savepoint starts a transaction, returns
    /// [SavepointResult::Commit] to indicate the transaction should be committed.
    fn release_named_savepoint(&self, name: &str) -> SavepointResult {
        let mut savepoints = self.savepoint_stack.write();
        let Some(target_idx) = savepoints.iter().rposition(|savepoint| {
            matches!(
                savepoint.kind,
                SavepointKind::Named {
                    name: ref savepoint_name,
                    ..
                } if savepoint_name == name
            )
        }) else {
            return SavepointResult::NotFound;
        };

        let commits_transaction = if matches!(
            savepoints[target_idx].kind,
            SavepointKind::Named {
                starts_transaction: true,
                ..
            }
        ) && target_idx == 0
        {
            SavepointResult::Commit
        } else {
            SavepointResult::Release
        };
        if matches!(commits_transaction, SavepointResult::Commit) {
            // Defer mutation until transaction commit succeeds. If commit fails
            // (e.g. deferred FK violation), savepoints must remain intact.
            return commits_transaction;
        }

        let drained: Vec<Savepoint> = savepoints.drain(target_idx..).collect();
        if let Some(parent) = savepoints.last_mut() {
            for savepoint in drained {
                parent.merge_from(savepoint);
            }
        }
        commits_transaction
    }

    /// Find the named savepoint to rollback to and pop all savepoints above it. Returns the rolled
    /// back savepoints and net change in deferred FK violations for undoing changes to transaction
    /// state.
    fn rollback_to_named_savepoint(&self, name: &str) -> Option<SavepointRollbackResult> {
        let mut savepoints = self.savepoint_stack.write();
        let target_idx = savepoints.iter().rposition(|savepoint| {
            matches!(
                savepoint.kind,
                SavepointKind::Named {
                    name: ref savepoint_name,
                    ..
                } if savepoint_name == name
            )
        })?;

        let target_name = match &savepoints[target_idx].kind {
            SavepointKind::Named { name, .. } => name.clone(),
            SavepointKind::Statement => unreachable!("target idx points to named savepoint"),
        };
        let starts_transaction = matches!(
            savepoints[target_idx].kind,
            SavepointKind::Named {
                starts_transaction: true,
                ..
            }
        );
        let deferred_fk_violations = savepoints[target_idx].deferred_fk_violations;
        let header = savepoints[target_idx].header;
        let header_dirty = savepoints[target_idx].header_dirty;

        let drained: Vec<Savepoint> = savepoints.drain(target_idx..).collect();
        savepoints.push(Savepoint::named(
            target_name,
            starts_transaction,
            deferred_fk_violations,
            header,
            header_dirty,
        ));
        Some(SavepointRollbackResult {
            rolledback_savepoints: drained,
            deferred_fk_violations,
        })
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
            "{{ state: {}, id: {}, begin_ts: {}, write_set: ",
            self.state.load(),
            self.tx_id,
            self.begin_ts,
        )?;

        match self.write_set.try_lock() {
            Some(write_set) => {
                write!(f, "[")?;
                for (i, (id, _chain)) in write_set.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?
                    }
                    write!(f, "{id:?}")?;
                }
                write!(f, "]")?;
            }
            None => write!(f, "<locked>")?,
        }

        write!(f, " }}")
    }
}

/// Transaction state.
#[derive(Debug, Clone, PartialEq, Copy)]
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
pub enum CommitState<Clock: LogicalClock, A: ConcurrentAllocator = TursoAllocator> {
    Initial,
    Commit {
        end_ts: u64,
    },
    /// Wait for unresolved commit dependencies before building the durable
    /// committed view for the logical log.
    /// Hekaton Section 3.2: "If T passes validation, it must wait for outstanding
    /// commit dependencies to be resolved."
    WaitForDependencies {
        end_ts: u64,
    },
    /// Build the committed log record incrementally, yielding every
    /// `MVCC_COMMIT_BATCH_SIZE` rowids so that very large write sets
    /// (e.g. CREATE INDEX on a multi-million row table) don't monopolize
    /// the executor.
    BuildLogRecord(BuildLogRecordCtx),
    BeginCommitLogicalLog {
        end_ts: u64,
        log_record: LogRecord,
    },
    UpgradeLogicalLogHeader {
        end_ts: u64,
        log_record: LogRecord,
    },
    WriteLogicalLog {
        end_ts: u64,
        log_record: LogRecord,
    },
    FinishLogicalLogWrite {
        end_ts: u64,
    },
    SyncLogicalLog {
        end_ts: u64,
    },
    EndCommitLogicalLog {
        end_ts: u64,
    },
    Checkpoint {
        // TODO: if and when we transform this code to async we won't be needing this explicit state machine nor
        // the mutex
        state_machine: Mutex<StateMachine<CheckpointStateMachine<Clock, A>>>,
    },
    CommitEnd {
        end_ts: u64,
    },
    /// Publish committed timestamps into the live MVCC chains in chunks
    /// of `MVCC_COMMIT_BATCH_SIZE` rowids, yielding between chunks. The
    /// transaction is already in the Committed state at this point, so
    /// readers consult `txs[tx_id]` to resolve any TxID references that
    /// haven't been rewritten yet.
    RewriteLiveVersions(RewriteLiveVersionsCtx),
    /// Final post-rewrite cleanup: drain commit dependents, release the
    /// commit lock, update the global header, finish the tx, and start
    /// auto-checkpoint if needed.
    FinalizeCommit {
        end_ts: u64,
    },
}

/// Iteration state for the chunked `BuildLogRecord` step.
#[derive(Debug)]
pub struct BuildLogRecordCtx {
    pub end_ts: u64,
    pub log_record: LogRecord,
    /// Index into `CommitStateMachine::write_set` for the current pass.
    pub cursor: usize,
    /// True while emitting schema rows (sqlite_schema), false during the
    /// data-row pass. Schema rows are emitted first so log replay sees
    /// CREATE TABLE before related INSERTs.
    pub schema_process: bool,
    /// Snapshot of `tx.header` taken when `header_dirty` was observed
    /// at the start of BuildLogRecord. Appended as an `OP_UPDATE_HEADER`
    /// op after both row-version passes complete (preserves on-disk
    /// order: ops first, header last).
    pub pending_header: Option<DatabaseHeader>,
}

/// Iteration state for the chunked `RewriteLiveVersions` step.
#[derive(Debug)]
pub struct RewriteLiveVersionsCtx {
    pub end_ts: u64,
    /// Index into `CommitStateMachine::write_set`.
    pub cursor: usize,
}

/// How many rowids `BuildLogRecord` / `RewriteLiveVersions` process before
/// yielding to the executor. Picked to amortize state-machine overhead
/// while keeping a CREATE INDEX on a 2M-row table responsive.
const MVCC_COMMIT_BATCH_SIZE: usize = 1024;

#[derive(Debug)]
pub enum WriteRowState {
    Initial,
    Seek,
    /// After seek returns TryAdvance for an index key stored in an interior node,
    /// advance the cursor to that interior cell so insert overwrites it.
    Advance,
    Insert,
    /// Move to the next record in order to leave the cursor in the next position, this is used for inserting multiple rows for optimizations.
    Next,
}

#[derive(Debug)]
struct CommitCoordinator {
    pager_commit_lock: Arc<TursoRwLock>,
}

impl CommitCoordinator {
    fn new() -> Self {
        Self {
            pager_commit_lock: Arc::new(TursoRwLock::new()),
        }
    }
}

#[cfg(any(test, injected_yields))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::EnumCount)]
#[repr(u8)]
pub(crate) enum CommitYieldPoint {
    CommitValidation,
    WaitForDependencies,
    /// Fires once on the first entry into `step_build_log_record` (cursor=0,
    /// schema_process=true), before any chunk processing. Pairs with
    /// `LogRecordPrepared` to bracket the BuildLogRecord chunked yields.
    BuildLogRecordStart,
    LogRecordPrepared,
    /// Fires after commit dependencies are released and the commit lock is
    /// dropped, but before publishing the cached global header / committed
    /// timestamp watermark.
    BeforeGlobalHeaderUpdate,
    BeforeFinishCommittedTx,
    /// Boundary right after `remove_tx` runs but before the connection cache
    /// is cleared by the caller at vdbe/mod.rs. Used for failure injection
    /// to reproduce divergence between `mv_store.txs` and `connection.mv_tx_id`.
    AfterRemoveTx,
}

#[cfg(any(test, injected_yields))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::EnumCount)]
#[repr(u8)]
pub(crate) enum ExclusiveTxYieldPoint {
    AfterTimestampCheckBeforeCas,
}

#[cfg(any(test, injected_yields))]
impl YieldPointMarker for ExclusiveTxYieldPoint {
    const POINT_COUNT: u8 = Self::COUNT as u8;

    fn ordinal(self) -> u8 {
        self as u8
    }
}

#[cfg(any(test, injected_yields))]
impl YieldPointMarker for CommitYieldPoint {
    const POINT_COUNT: u8 = Self::COUNT as u8;

    fn ordinal(self) -> u8 {
        self as u8
    }
}

#[cfg(any(test, injected_yields))]
fn commit_yield_key(tx_id: u64) -> u64 {
    // any large number will do
    const COMMIT_SELECTION_TAG: u64 = 0xC011_C011_C011_C011;
    tx_id ^ COMMIT_SELECTION_TAG
}

#[cfg(any(test, injected_yields))]
impl<Clock: LogicalClock, A: ConcurrentAllocator> ProvidesYieldContext
    for CommitStateMachine<Clock, A>
{
    fn yield_context(&self) -> YieldContext {
        YieldContext::new(
            self.connection.yield_injector(),
            self.connection.failure_injector(),
            self.yield_instance_id,
            commit_yield_key(self.tx_id),
        )
    }
}

pub struct CommitStateMachine<Clock: LogicalClock, A: ConcurrentAllocator = TursoAllocator> {
    state: CommitState<Clock, A>,
    is_finalized: bool,
    #[cfg(any(test, injected_yields))]
    yield_instance_id: u64,
    did_commit_schema_change: bool,
    tx_id: TxID,
    mvcc_store: Arc<MvStore<Clock, A>>,
    connection: Arc<Connection>,
    /// Database index this commit is for (`MAIN_DB_ID` or an attached-db id).
    /// Threaded through so that `finish_committed_tx` can clear the matching
    /// connection-level mv_tx slot atomically with `remove_tx`.
    db_id: usize,
    commit_coordinator: Arc<CommitCoordinator>,
    header: Arc<RwLock<Option<DatabaseHeader>>>,
    pager: Arc<Pager>,
    /// Bytes appended to the logical log for this commit; applied to writer offset only after durability and before lock release.
    pending_log_append_bytes: Option<u64>,
    /// The synchronous mode for fsync operations. When set to Off, fsync is skipped.
    sync_mode: SyncMode,
    _phantom: PhantomData<Clock>,
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> Debug for CommitStateMachine<Clock, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitStateMachine")
            .field("state", &self.state)
            .field("is_finalized", &self.is_finalized)
            .finish()
    }
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> Drop for CommitStateMachine<Clock, A> {
    fn drop(&mut self) {
        self.cleanup_unfinished_commit();
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

impl<Clock: LogicalClock, A: ConcurrentAllocator> CommitStateMachine<Clock, A> {
    pub(crate) fn cleanup_mvcc_checkpoint_state(&mut self) {
        if let CommitState::Checkpoint { state_machine } = &mut self.state {
            let _ = state_machine
                .lock()
                .inner_mut()
                .cleanup_after_external_io_error(LimboError::InternalError(
                    "mvcc: cleanup_unfinished_commit".to_string(),
                ))
                .inspect_err(|e| tracing::error!("cleanup_after_external_io_error failed: {e}"));
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        state: CommitState<Clock, A>,
        tx_id: TxID,
        mvcc_store: Arc<MvStore<Clock, A>>,
        connection: Arc<Connection>,
        db_id: usize,
        commit_coordinator: Arc<CommitCoordinator>,
        header: Arc<RwLock<Option<DatabaseHeader>>>,
        sync_mode: SyncMode,
    ) -> Self {
        let pager = connection.pager.load().clone();
        // Use the connection's tx-level schema_did_change flag as the
        // single source of truth.  This flag is set by SetCookie(SchemaVersion)
        // which every DDL emits, so it covers all schema-changing operations
        // including ones that don't write to sqlite_schema (e.g. AddType
        // writing to __turso_internal_types for custom types).
        let schema_did_change_from_tx = matches!(
            connection.get_tx_state(),
            crate::connection::TransactionState::Write {
                schema_did_change: true
            }
        );
        Self {
            state,
            is_finalized: false,
            #[cfg(any(test, injected_yields))]
            yield_instance_id: connection.next_yield_instance_id(),
            did_commit_schema_change: schema_did_change_from_tx,
            tx_id,
            mvcc_store,
            connection,
            db_id,
            commit_coordinator,
            pager,
            header,
            pending_log_append_bytes: None,
            sync_mode,
            _phantom: PhantomData,
        }
    }

    fn cleanup_unfinished_commit(&mut self) {
        if !self.is_finalized {
            self.cleanup_mvcc_checkpoint_state();
            if self.pending_log_append_bytes.take().is_some() {
                if let Err(err) = self.mvcc_store.storage.discard_pending_log_write() {
                    tracing::error!("failed to discard pending MVCC logical-log write: {err}");
                }
            }
            if !matches!(self.state, CommitState::Checkpoint { .. }) {
                self.mvcc_store.cleanup_dropped_commit(
                    self.tx_id,
                    self.connection.as_ref(),
                    self.db_id,
                );
            }
            self.end_read_tx_for_db();
            if self.db_id == crate::MAIN_DB_ID {
                self.connection
                    .set_tx_state(crate::connection::TransactionState::None);
            }
        }

        let tx_id = self.tx_id;
        let db_id = self.db_id;
        turso_assert!(
            self.mvcc_store.txs.get(&tx_id).is_none(),
            "MVCC tx should be removed from txs after a successful commit",
            { "tx_id": tx_id }
        );
        turso_assert!(
            !self.mvcc_store.is_exclusive_tx(&tx_id),
            "MVCC tx should not still hold the exclusive slot after a successful commit",
            { "tx_id": tx_id }
        );
        turso_assert!(
            self.connection.get_mv_tx_id_for_db(db_id) != Some(tx_id),
            "Connection should not still reference an MVCC tx after a successful commit",
            { "tx_id": tx_id, "db_id": db_id }
        );
    }

    fn end_read_tx_for_db(&self) {
        if let Ok(pager) = self.connection.get_pager_from_database_index(&self.db_id) {
            pager.end_read_tx();
            return;
        }
        self.pager.end_read_tx();
    }

    /// Validates commit-time write-write conflicts for one table row key.
    ///
    /// Returns [LimboError::WriteWriteConflict] when another transaction committed or is
    /// preparing a conflicting version according to first-committer-wins.
    fn check_rowid_for_conflicts(
        &self,
        rowid: &RowID,
        end_ts: u64,
        tx: &Transaction,
        mvcc_store: &Arc<MvStore<Clock, A>>,
    ) -> Result<()> {
        let row_versions = mvcc_store.rows.get(rowid);
        if row_versions.is_none() {
            return Ok(());
        }

        let row_versions = row_versions.unwrap();
        let row_versions = row_versions.value();
        let row_versions = row_versions.read();

        self.check_version_conflicts(end_ts, tx, mvcc_store, &row_versions)?;
        Ok(())
    }

    /// Validates commit-time write-write conflicts for one index key.
    ///
    /// Returns [LimboError::WriteWriteConflict] when another transaction committed or is
    /// preparing a conflicting index version according to first-committer-wins.
    fn check_index_for_conflicts(
        &self,
        rowid: &RowID,
        end_ts: u64,
        tx: &Transaction,
        mvcc_store: &Arc<MvStore<Clock, A>>,
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
            let row_versions = entry.value();
            let row_versions = row_versions.read();
            self.check_version_conflicts(end_ts, tx, mvcc_store, &row_versions)?;
        }

        Ok(())
    }

    /// Validates a single version chain against the current transaction's commit timestamp.
    ///
    /// This enforces snapshot-isolation conflict checks for both:
    /// 1. versions ended by concurrent commits (`end > tx.begin_ts`), and
    /// 2. live versions owned by concurrent transactions (state/ts tie-breaking).
    fn check_version_conflicts(
        &self,
        end_ts: u64,
        tx: &Transaction,
        mvcc_store: &Arc<MvStore<Clock, A>>,
        row_versions: &[RowVersion],
    ) -> Result<()> {
        // Check for conflicts - iterate in reverse for faster early termination
        for version in row_versions.iter().rev() {
            // A row that we are trying to commit was deleted/updated by another
            // committed transaction after our begin timestamp. Even if that
            // version is now "ended", this is still a write-write conflict.
            if let Some(TxTimestampOrID::Timestamp(end_ts)) = version.end() {
                turso_assert!(
                    end_ts != tx.begin_ts,
                    "committed end_ts and begin_ts cannot be equal: txn timestamps are strictly monotonic"
                );
                if end_ts > tx.begin_ts {
                    return Err(LimboError::WriteWriteConflict);
                }
            }

            // B-tree tombstones (begin: None, end: TxID) act as write locks.
            // When another transaction has created a tombstone to delete a
            // B-tree-resident row, that tombstone is effectively a write lock
            // on the row — same as Hekaton's End field. We must detect this
            // as a write-write conflict using the same state-based logic used
            // for begin: TxID checks below.
            if version.begin().is_none() {
                // Committed tombstones (end: Timestamp) are already handled by
                // the check above at lines 1070-1074. Here we only need to check
                // in-flight tombstones (end: TxID) from other transactions.
                if let Some(TxTimestampOrID::TxID(other_tx_id)) = version.end() {
                    if other_tx_id != self.tx_id {
                        let other_tx = mvcc_store.txs.get(&other_tx_id).expect(
                            "check_version_conflicts: tombstone end TxID not found in txn map",
                        );
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
                // Tombstones have no meaningful begin field — skip begin checks
                continue;
            }

            match version.end() {
                Some(TxTimestampOrID::Timestamp(end_ts)) => {
                    // Committed deletion. If end_ts > our begin_ts, the conflict
                    // would have been already caught earlier when we iterate through
                    // the row versions in reverse. If end_ts < our
                    // begin_ts, the deletion predates our snapshot — no conflict.
                    turso_assert!(
                        end_ts < tx.begin_ts,
                        "row version's end_ts cannot be greater than txns begin_ts"
                    );
                    continue;
                }
                Some(TxTimestampOrID::TxID(end_tx_id)) => {
                    // Deletion not yet finalized; the deleting transaction may still be in Preparing.
                    if end_tx_id == self.tx_id {
                        // We deleted this version ourselves, so it cannot conflict with our commit.
                        continue;
                    }

                    match lookup_tx_state(
                        &mvcc_store.txs,
                        &mvcc_store.finalized_tx_states,
                        end_tx_id,
                    ) {
                        Some(TransactionState::Committed(committed_end_ts)) => {
                            turso_assert!(
                                committed_end_ts != tx.begin_ts,
                                "committed end_ts and begin_ts cannot be equal: txn timestamps are strictly monotonic"
                            );
                            if committed_end_ts > tx.begin_ts {
                                return Err(LimboError::WriteWriteConflict);
                            }
                            continue;
                        }
                        _ => {
                            // Deleting tx is Active, Preparing, Aborted, or gone.
                            // The deletion may not stick, so this version may still be live.
                            // Fall through to check begin for conflicts.
                        }
                    }
                }
                None => {
                    // No end — version is live. Fall through to check begin.
                }
            }

            match version.begin() {
                Some(TxTimestampOrID::TxID(other_tx_id)) => {
                    // Skip our own version
                    if other_tx_id == self.tx_id {
                        continue;
                    }
                    // Another transaction's uncommitted version - check their state
                    match lookup_tx_state(
                        &mvcc_store.txs,
                        &mvcc_store.finalized_tx_states,
                        other_tx_id,
                    ) {
                        // Other tx already committed = conflict
                        Some(TransactionState::Committed(_)) => {
                            return Err(LimboError::WriteWriteConflict);
                        }
                        // Both preparing - compare end_ts (lower wins)
                        Some(TransactionState::Preparing(other_end_ts)) => {
                            if other_end_ts < end_ts {
                                // Other tx has lower end_ts, they win
                                return Err(LimboError::WriteWriteConflict);
                            }
                            // We have lower end_ts, we win - they'll abort when they validate
                        }
                        // Other tx still active - we're already Preparing so we're ahead
                        // They'll see us in Preparing/Committed when they try to commit
                        Some(TransactionState::Active) => {}
                        // Other tx aborted - no conflict
                        Some(TransactionState::Aborted) | Some(TransactionState::Terminated) => {}
                        None => {
                            // TODO: an aborted txn should not affect another one.. properly handle
                            // this case, but for now be conservative and treat as conflict to avoid
                            // potential correctness issues
                            tracing::debug!(
                                "check_version_conflicts: missing tx {} for row version {:?}; conservatively treating as conflict",
                                other_tx_id,
                                version
                            );
                            return Err(LimboError::WriteWriteConflict);
                        }
                    }
                }
                Some(TxTimestampOrID::Timestamp(begin_ts)) => {
                    // A live committed version with this rowid exists.
                    // begin_ts >= tx.begin_ts: a concurrent transaction committed a row
                    //   with this rowid after our snapshot — invisible to NotExists.
                    // begin_ts < tx.begin_ts: the row predates our snapshot. NotExists
                    //   should have seen it at INSERT time, so this is a defensive guard.
                    let _ = begin_ts;
                    return Err(LimboError::WriteWriteConflict);
                }
                None => {
                    // Invalid version
                }
            }
        }
        Ok(())
    }

    /// Run one chunked step of `BuildLogRecord`. Processes up to
    /// `MVCC_COMMIT_BATCH_SIZE` rowids per call, then yields. Schema rows
    /// (table_id == SQLITE_SCHEMA_MVCC_TABLE_ID) are emitted before data rows
    /// in two passes so that log replay sees CREATE TABLE before INSERTs.
    fn step_build_log_record(
        &mut self,
        mvcc_store: &Arc<MvStore<Clock, A>>,
    ) -> Result<TransitionResult<()>> {
        // First entry into BuildLogRecord (no chunk processed yet): a yield-
        // point to bracket the chunked yields so tests can count them exactly.
        // Must run before the `&mut self.state` re-bind below — the macro
        // calls `self.yield_context()` which needs `&self`.
        let is_first_entry = matches!(
            self.state,
            CommitState::BuildLogRecord(BuildLogRecordCtx {
                cursor: 0,
                schema_process: true,
                ..
            })
        );
        if is_first_entry {
            inject_transition_yield!(self, CommitYieldPoint::BuildLogRecordStart);
        }

        let tx_id = self.tx_id;
        let tx_entry = mvcc_store.txs.get(&tx_id);
        let tx = tx_entry
            .as_ref()
            .map(|entry| entry.value())
            .ok_or_else(|| {
                LimboError::NoSuchTransactionID(format!(
                    "tx id {tx_id} not found in step_build_logical_record"
                ))
            })?;
        let write_set_len = tx.write_set.lock().entries.len();
        #[cfg(feature = "conn_raw_api")]
        let connection = Arc::clone(&self.connection);
        let CommitState::BuildLogRecord(ctx) = &mut self.state else {
            unreachable!("step_build_log_record requires BuildLogRecord state")
        };
        let end_ts = ctx.end_ts;

        // A sqlite_schema row that is inserted and deleted inside one
        // transaction does not always mean the underlying table or index was
        // inserted and deleted. ALTER TABLE can rewrite sqlite_schema several
        // times for an existing root page. We only treat a root page as
        // transaction-local when it has no schema row before the transaction and
        // no schema row after it.

        let is_our_begin = |row_version: &RowVersion| {
            matches!(
                row_version.begin(),
                Some(TxTimestampOrID::TxID(vid)) if vid == tx_id
            )
        };
        let is_our_end = |row_version: &RowVersion| {
            matches!(
                row_version.end(),
                Some(TxTimestampOrID::TxID(vid)) if vid == tx_id
            )
        };

        let mut btree_ids_created_and_dropped_in_tx: HashSet<MVTableId> = HashSet::default();
        let mut btree_ids_removed_from_schema_by_tx: HashSet<MVTableId> = HashSet::default();
        {
            let write_set = tx.write_set.lock();
            let frame_writes_schema = write_set
                .entries
                .iter()
                .any(|(id, _)| id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID);

            // Decoding sqlite_schema records is only needed for DDL frames.
            // Normal DML frames do not touch sqlite_schema, so avoid parsing
            // schema records while committing the common path.
            if frame_writes_schema {
                let mut schema_roots_created_and_deleted_in_tx: HashSet<i64> = HashSet::default();
                let mut schema_roots_deleted_by_tx: HashSet<i64> = HashSet::default();
                let mut schema_roots_present_before_tx: HashSet<i64> = HashSet::default();
                let mut schema_roots_present_after_tx: HashSet<i64> = HashSet::default();

                for (id, row_versions) in &write_set.entries {
                    // Only sqlite_schema entries can yield a btree identity. The
                    // common path (CREATE INDEX on a populated table, bulk DML)
                    // has tens of thousands of write_set entries that are NOT
                    // sqlite_schema; locking each and parsing every version was
                    // the dominant cost of MVCC commit on this branch.
                    if id.table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
                        continue;
                    }
                    for row_version in row_versions.read().iter() {
                        let Some(identity) = sqlite_schema_btree_identity(row_version) else {
                            continue;
                        };
                        let our_begin = is_our_begin(row_version);
                        let our_end = is_our_end(row_version);
                        if !our_begin {
                            schema_roots_present_before_tx.insert(identity.root_page);
                        }
                        if our_end {
                            schema_roots_deleted_by_tx.insert(identity.root_page);
                        }
                        if !our_end {
                            schema_roots_present_after_tx.insert(identity.root_page);
                        }
                        if our_begin && our_end && !row_version.btree_resident {
                            schema_roots_created_and_deleted_in_tx.insert(identity.root_page);
                        }
                    }
                }

                for root_page in schema_roots_deleted_by_tx {
                    if !schema_roots_present_after_tx.contains(&root_page) {
                        btree_ids_removed_from_schema_by_tx
                            .insert(mvcc_store.get_table_id_from_root_page(root_page));
                    }
                }

                for root_page in schema_roots_created_and_deleted_in_tx {
                    if !schema_roots_present_before_tx.contains(&root_page)
                        && !schema_roots_present_after_tx.contains(&root_page)
                    {
                        btree_ids_created_and_dropped_in_tx
                            .insert(mvcc_store.get_table_id_from_root_page(root_page));
                    }
                }
            }
        }

        // Remap a table_id to its canonical form for the log. After checkpoint,
        // a table's in-memory table_id (e.g. -53) may differ from -(root_page)
        // (e.g. -58). On recovery, bootstrap reconstructs the map using
        // -(root_page), so log records must use that canonical form to be found.
        let canonicalize_table_id = |version: &mut RowVersion| {
            let table_id = version.row.id.table_id;
            if table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
                return;
            }
            if let Some(entry) = mvcc_store.table_id_to_rootpage.get(&table_id) {
                if let Some(root_page) = *entry.value() {
                    let canonical = MVTableId::from(-(root_page as i64));
                    if canonical != table_id {
                        version.row.id.table_id = canonical;
                    }
                }
            }
        };

        let collect_versions = |row_versions: &Arc<RwLock<Vec<RowVersion>>>,
                                log_record: &mut LogRecord|
         -> Result<()> {
            // `log_record.row_versions` is the logical transaction log. Recovery
            // replays it in this order. `insert_version_raw` is for the versions
            // of one MVCC entry: one table row, one sqlite_schema row, or one
            // index entry. Its timestamp sort is correct for that one entry, but
            // it is not a rule for ordering the whole transaction.
            //
            // Example: the db file already has table t, index idx, and
            // t(rowid=1). One transaction runs `DELETE FROM t`, then
            // `ALTER TABLE t ADD COLUMN x`. ALTER TABLE records a DELETE for the
            // old sqlite_schema row for t and an UPSERT for the replacement row.
            // DELETE FROM t records a DELETE for t(rowid=1) and a DELETE_INDEX
            // for the idx entry that pointed at that row. The schema DELETE,
            // table-row DELETE, and DELETE_INDEX are all for entries already in
            // the db file, so their `begin` is None. Sorting the whole
            // transaction with `insert_version_raw` can put those three deletes
            // before the schema UPSERT:
            // `[DELETE schema(t), DELETE t(rowid=1), DELETE_INDEX idx(rowid=1),
            //   UPSERT schema(t)]`.
            //
            // Index log ops contain serialized index keys, not CREATE INDEX SQL.
            // Recovery now decodes every index op using schema snapshots for the
            // whole transaction frame, so it does not install a half-updated
            // schema while the frame is still being replayed. The writer still
            // must not scramble a sqlite_schema DELETE+UPSERT pair with unrelated
            // table/index entries; keeping each write-set entry together gives
            // recovery a frame whose final schema can be understood.
            //
            // The filtering below has four separate jobs:
            //
            // 1. Omit all entries for a table/index root page that had no
            //    sqlite_schema row before this transaction and has no
            //    sqlite_schema row after it. Example: CREATE INDEX followed by
            //    DROP INDEX in one transaction.
            // 2. Omit inserts and updates for a table/index root page that is
            //    removed from sqlite_schema by this transaction. Example:
            //    UPDATE writes a new entry into idx_old, then DROP INDEX
            //    idx_old runs before COMMIT. The old index-entry deletes still
            //    matter, but new entries for idx_old cannot survive the frame.
            // 3. Omit one version that was created and deleted by this
            //    transaction before it reached the database file. Such a version
            //    does not change durable state, and recovery may not have enough
            //    schema information to decode a delete for it.
            // 4. If this entry has a delete for the row that already existed in
            //    the database file, do not also log a same-transaction
            //    create/delete replacement for the same write-set entry. Example:
            //    ALTER TABLE rewrites an index sqlite_schema row, then DROP INDEX
            //    deletes that replacement in the same transaction. The durable
            //    change is one delete of the original sqlite_schema row.
            //
            // What the code below does after that filtering:
            // - look only at this one write-set entry (`row_versions`);
            // - copy the versions written or ended by the committing transaction;
            // - if this same entry appears twice with the same
            //   `begin=Timestamp(...)`, keep the later one, because recovery should
            //   not replay an intermediate value for the same table row,
            //   sqlite_schema row, or index entry;
            // - append those versions to `log_record.row_versions` without sorting
            //   them against versions from other write-set entries.

            let mut entry_versions: Vec<RowVersion> = Vec::new();
            let row_versions = row_versions.read();

            // A tombstone over a row that was already in the B-tree before this
            // tx (begin=None, end=tx_id, btree_resident=true) is the canonical
            // log record for deleting that durable row. If the same entry also
            // contains a version this tx both began and ended over a B-tree row
            // (e.g. DELETE; INSERT; DELETE on a btree-resident rowid), the
            // tombstone already covers the durable delete and the begun+ended
            // version must be suppressed to avoid logging the same delete twice.
            let has_tombstone_for_btree_row = row_versions.iter().any(|row_version| {
                !is_our_begin(row_version) && is_our_end(row_version) && row_version.btree_resident
            });

            // Helper that returns Some(row_version) if our tx contributed to it and if we must therefore log it.
            let our_committed_image = |row_version: &RowVersion| -> Option<RowVersion> {
                let our_begin = is_our_begin(row_version);
                let our_end = is_our_end(row_version);
                if !our_begin && !our_end {
                    // row_version belongs to another tx
                    return None;
                }
                if btree_ids_created_and_dropped_in_tx.contains(&row_version.row.id.table_id) {
                    // This table or index has no sqlite_schema row before the
                    // transaction and no sqlite_schema row after it. It was created
                    // and dropped inside this commit, so its table/index entries do
                    // not change durable state.
                    return None;
                }
                if btree_ids_removed_from_schema_by_tx.contains(&row_version.row.id.table_id)
                    && our_begin
                    && !our_end
                {
                    // This transaction created or rewrote an entry for a table or
                    // index that is gone from sqlite_schema by COMMIT. Example:
                    // UPDATE writes a new entry into idx_old, then DROP INDEX
                    // idx_old runs before COMMIT. Keep deletes for entries that
                    // existed before the transaction, but do not log new entries
                    // that cannot exist after the transaction.
                    return None;
                }
                if our_begin && our_end {
                    // A begun+ended version is purely in-memory unless it
                    // shadows a B-tree row (insert_btree_resident_to_table_or_index
                    // then delete). For btree_resident=true we still need to log
                    // the delete of the durable row, UNLESS a sibling tombstone
                    // in this same entry already covers it.
                    if !row_version.btree_resident || has_tombstone_for_btree_row {
                        return None;
                    }
                }

                let mut committed = row_version.clone();
                if our_begin {
                    // New version is valid STARTING FROM the committing
                    // transaction's end timestamp. See Hekaton page 299.
                    committed.set_begin(Some(TxTimestampOrID::Timestamp(end_ts)));

                    if !our_end {
                        // A version row_version we inserted may have row_version.end() == tx_b.tx_id,
                        // where tx_b is a concurrent tx. This is because when a tx transitions to
                        // Preparing, its row_version becomes *speculatively updatable*, and a tx tx_b
                        // is allowed to change row_version.end() from None to tx_b.tx_id to delete it
                        // (see the Hekaton paper, §3.1, heading "check updatability").
                        //
                        // That deletion is tx_b's contribution, and tx_b will log it on its own commit.
                        // Our log record must capture our own contribution, but if the `end` field is
                        // set, it will be serialized as a OP_DELETE_* in the logical log, so we unset
                        // it so that it will be serialized as an OP_UPSERT_*. tx_b will take care of
                        // logging the deletion.
                        committed.set_end(None);
                    }
                }
                if our_end {
                    // Old version is valid UNTIL the committing
                    // transaction's end timestamp. See Hekaton page 299.
                    committed.set_end(Some(TxTimestampOrID::Timestamp(end_ts)));
                }
                Some(committed)
            };

            for row_version in row_versions.iter() {
                let Some(mut committed_version) = our_committed_image(row_version) else {
                    continue;
                };
                canonicalize_table_id(&mut committed_version);
                let replaces_last = entry_versions.last().is_some_and(|last| {
                    last.row.id == committed_version.row.id
                        && matches!(
                            (&last.begin(), &committed_version.begin()),
                            (
                                Some(TxTimestampOrID::Timestamp(existing)),
                                Some(TxTimestampOrID::Timestamp(new))
                            ) if existing == new
                        )
                });
                if replaces_last {
                    *entry_versions
                        .last_mut()
                        .expect("last version checked above") = committed_version;
                    continue;
                }
                #[cfg(debug_assertions)]
                {
                    let same_row_and_begin = |existing: &RowVersion| {
                        existing.row.id == committed_version.row.id
                            && matches!(
                                (&existing.begin(), &committed_version.begin()),
                                (
                                    Some(TxTimestampOrID::Timestamp(existing)),
                                    Some(TxTimestampOrID::Timestamp(new))
                                ) if existing == new
                            )
                    };
                    turso_assert!(
                        !entry_versions.iter().any(same_row_and_begin),
                        "one write-set entry produced non-adjacent log versions with the same row id and commit timestamp"
                    );
                }
                entry_versions.push(committed_version);
            }
            for committed_version in &entry_versions {
                #[cfg(feature = "conn_raw_api")]
                let portable_extension = portable_delete_op_extension_for_row_version(
                    &connection,
                    mvcc_store,
                    committed_version,
                )?;
                #[cfg(not(feature = "conn_raw_api"))]
                let portable_extension: Option<Vec<u8>> = None;
                mvcc_store.storage.serialize_row_version(
                    log_record,
                    committed_version,
                    portable_extension.as_deref(),
                )?;
            }
            Ok(())
        };

        // Process schema rows (sqlite_schema) before data/index rows so that
        // replay sees table_id_to_rootpage updates before row ops reference
        // those ids. `tx.write_set` preserves first-touch order, so mixed DDL
        // and DML in one transaction cannot rely on write-set order alone.
        let mut iterations = 0;

        let write_set = tx.write_set.lock();
        while ctx.cursor < write_set_len && iterations < MVCC_COMMIT_BATCH_SIZE {
            let (id, row_versions) = &write_set.entries[ctx.cursor];
            let is_schema = id.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID;
            // schema_process=true: schema rows only, false: data rows only.
            let process = if ctx.schema_process {
                is_schema
            } else {
                !is_schema
            };
            if process {
                collect_versions(row_versions, &mut ctx.log_record)?;
            }
            ctx.cursor += 1;
            iterations += 1;
        }
        if ctx.cursor < write_set_len {
            // More work remains in the current pass: yield and resume.
            return Ok(TransitionResult::Io(IOCompletions::Single(
                Completion::new_yield(),
            )));
        }

        if ctx.schema_process {
            // Schema pass done; start the data pass from the top.
            ctx.schema_process = false;
            ctx.cursor = 0;
            return Ok(TransitionResult::Continue);
        }

        if let Some(header) = ctx.pending_header.take() {
            mvcc_store
                .storage
                .serialize_database_header(&mut ctx.log_record, &header)?;
        }

        // Move the assembled log record out and transition to
        // BeginCommitLogicalLog (or directly to CommitEnd if there is nothing
        // to log).
        let mut log_record = std::mem::replace(&mut ctx.log_record, LogRecord::new(end_ts));
        self.populate_portable_changes(mvcc_store, &mut log_record)?;
        tracing::trace!("prepared_log_record(tx_id={})", self.tx_id);

        if log_record.is_empty() {
            // Nothing to log. We still need to release the commit lock here
            // if this is an exclusive tx, mirroring the pre-chunk path
            // through WaitForDependencies.
            if mvcc_store.is_exclusive_tx(&self.tx_id) {
                if let Some(tx_entry) = mvcc_store.txs.get(&self.tx_id) {
                    mvcc_store.unlock_commit_lock_if_held(tx_entry.value());
                }
            }
            self.state = CommitState::CommitEnd { end_ts };
        } else {
            self.state = CommitState::BeginCommitLogicalLog { end_ts, log_record };
        }
        inject_transition_yield!(self, CommitYieldPoint::LogRecordPrepared);
        Ok(TransitionResult::Continue)
    }

    fn populate_portable_changes(
        &self,
        mvcc_store: &Arc<MvStore<Clock, A>>,
        log_record: &mut LogRecord,
    ) -> Result<()> {
        #[cfg(not(feature = "conn_raw_api"))]
        {
            let _ = mvcc_store;
            let _ = log_record;
            return Ok(());
        }

        #[cfg(feature = "conn_raw_api")]
        {
            if !self.connection.portable_logical_changes_enabled() {
                return Ok(());
            }
            log_record.portable_changes_enabled = true;

            let mut builder = PortableLogicalBuilder::new();
            let mut metadata: Vec<_> = self
                .connection
                .mvcc_log_meta_snapshot()
                .into_iter()
                .collect();
            metadata.sort_by(|a, b| a.0.cmp(&b.0));
            for (key, value) in metadata {
                builder.add_metadata(&key, &value);
            }

            // The recovery payload is the single durable operation stream.
            // The portable extension only adds the metadata required to interpret
            // recovery table ids outside this database instance.
            let mut table_refs_by_id = HashMap::default();
            let recovery_payload = &log_record.buf[LOG_RECORD_PREFIX_SIZE..];
            let parsed_ops = parse_ops_from_plaintext(
                recovery_payload,
                recovery_payload.len(),
                log_record.op_count,
                log_record.tx_timestamp,
            )?;

            let mut schema_upserts = HashMap::default();
            let mut schema_deletes = HashMap::default();
            let mut schema_rowids = Vec::new();
            let mut data_table_ids = HashSet::default();
            for op in &parsed_ops {
                match op {
                    ParsedOp::UpsertTable {
                        table_id,
                        rowid,
                        record_bytes,
                        ..
                    } if *table_id == SQLITE_SCHEMA_MVCC_TABLE_ID => {
                        let rowid = rowid.row_id.to_int_or_panic();
                        let row = portable_schema_row_from_record(record_bytes)?;
                        schema_rowids.push(rowid);
                        schema_upserts.insert(rowid, row);
                    }
                    ParsedOp::DeleteTable {
                        rowid,
                        record_bytes,
                        ..
                    } if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID => {
                        if record_bytes.is_empty() {
                            return Err(LimboError::Corrupt(
                                "sqlite_schema DELETE_TABLE missing old record".to_string(),
                            ));
                        }
                        let schema_rowid = rowid.row_id.to_int_or_panic();
                        let row = portable_schema_row_from_record(record_bytes)?;
                        schema_rowids.push(schema_rowid);
                        schema_deletes.insert(schema_rowid, row);
                    }
                    ParsedOp::UpsertTable { table_id, .. } => {
                        data_table_ids.insert(*table_id);
                    }
                    ParsedOp::DeleteTable { rowid, .. } => {
                        if rowid.table_id != SQLITE_SCHEMA_MVCC_TABLE_ID {
                            data_table_ids.insert(rowid.table_id);
                        }
                    }
                    ParsedOp::UpsertIndex { .. }
                    | ParsedOp::DeleteIndex { .. }
                    | ParsedOp::UpdateHeader { .. } => {}
                }
            }

            schema_rowids.sort_unstable();
            schema_rowids.dedup();
            for rowid in schema_rowids {
                let old_row = schema_deletes.get(&rowid);
                let new_row = schema_upserts.get(&rowid);
                match (old_row, new_row) {
                    (Some(old_row), Some(new_row)) => {
                        if is_portable_table_schema_row(old_row) {
                            table_refs_by_id.insert(
                                portable_table_id_from_rootpage(old_row.rootpage),
                                PortableTableRef {
                                    name: old_row.name.clone(),
                                },
                            );
                        }
                        if is_portable_table_schema_row(new_row) {
                            table_refs_by_id.insert(
                                portable_table_id_from_rootpage(new_row.rootpage),
                                PortableTableRef {
                                    name: new_row.name.clone(),
                                },
                            );
                        }
                    }
                    (None, Some(new_row)) => {
                        if is_portable_table_schema_row(new_row) {
                            table_refs_by_id.insert(
                                portable_table_id_from_rootpage(new_row.rootpage),
                                PortableTableRef {
                                    name: new_row.name.clone(),
                                },
                            );
                        }
                    }
                    (Some(old_row), None) => {
                        if is_portable_table_schema_row(old_row) {
                            table_refs_by_id.insert(
                                portable_table_id_from_rootpage(old_row.rootpage),
                                PortableTableRef {
                                    name: old_row.name.clone(),
                                },
                            );
                        }
                    }
                    (None, None) => {}
                }
            }

            let rootpage_for_table_id = |table_id: MVTableId| -> i64 {
                mvcc_store
                    .table_id_to_rootpage
                    .get(&table_id)
                    .and_then(|entry| *entry.value())
                    .map(|rootpage| rootpage as i64)
                    .unwrap_or_else(|| i64::from(table_id))
            };

            let mut unresolved_data_tables = Vec::new();
            let mut needed_rootpages = HashSet::default();
            for table_id in &data_table_ids {
                if table_refs_by_id.contains_key(table_id) {
                    continue;
                }
                let rootpage = rootpage_for_table_id(*table_id);
                if rootpage == 0 {
                    continue;
                }
                let root_table_id = portable_table_id_from_rootpage(rootpage);
                if let Some(table_ref) = table_refs_by_id.get(&root_table_id).cloned() {
                    table_refs_by_id.insert(*table_id, table_ref);
                    continue;
                }
                needed_rootpages.insert(rootpage);
                unresolved_data_tables.push((*table_id, root_table_id));
            }

            if !needed_rootpages.is_empty() {
                let schema = self.connection.schema.read();
                for rootpage in needed_rootpages {
                    let (resolved_table_id, table_ref) = if let Some(name) =
                        schema.table_name_for_root_page(rootpage)
                    {
                        (
                            portable_table_id_from_rootpage(rootpage),
                            PortableTableRef {
                                name: name.to_string(),
                            },
                        )
                    } else if rootpage < 0 {
                        let checkpointed_rootpage = -rootpage;
                        let Some(name) = schema.table_name_for_root_page(checkpointed_rootpage)
                        else {
                            continue;
                        };
                        (
                            portable_table_id_from_rootpage(checkpointed_rootpage),
                            PortableTableRef {
                                name: name.to_string(),
                            },
                        )
                    } else if rootpage > 0 {
                        let uncheckpointed_rootpage = -rootpage;
                        let Some(name) = schema.table_name_for_root_page(uncheckpointed_rootpage)
                        else {
                            continue;
                        };
                        (
                            portable_table_id_from_rootpage(rootpage),
                            PortableTableRef {
                                name: name.to_string(),
                            },
                        )
                    } else {
                        continue;
                    };
                    table_refs_by_id.insert(resolved_table_id, table_ref);
                }

                for (table_id, root_table_id) in unresolved_data_tables {
                    if let Some(table_ref) = table_refs_by_id.get(&root_table_id).cloned() {
                        table_refs_by_id.insert(table_id, table_ref);
                    }
                }
            }

            for (table_id, table_ref) in &table_refs_by_id {
                if !is_portable_logical_name(&table_ref.name) {
                    continue;
                }
                let added = builder.add_object_map(PortableObjectMapEntry {
                    mv_table_id: i64::from(*table_id),
                    name: &table_ref.name,
                });
                turso_assert!(
                    added,
                    "portable object map unexpectedly rejected a user object"
                );
            }

            for table_id in data_table_ids {
                let Some(table_ref) = table_refs_by_id.get(&table_id) else {
                    return Err(LimboError::InternalError(format!(
                        "unable to resolve MVCC table id for portable changes: table_id={table_id}"
                    )));
                };
                if !is_portable_logical_name(&table_ref.name) {
                    continue;
                }
            }

            log_record.portable_changes = builder.finish();
            Ok(())
        }
    }

    /// Run one chunked step of `RewriteLiveVersions`. Processes up to
    /// `MVCC_COMMIT_BATCH_SIZE` rowids per call, then yields. The transaction
    /// is already in the Committed state at this point; un-rewritten TxID
    /// references resolve via `txs[tx_id]` for visibility/conflict checks.
    fn step_rewrite_live_versions(
        &mut self,
        mvcc_store: &Arc<MvStore<Clock, A>>,
    ) -> Result<TransitionResult<()>> {
        let tx_id = self.tx_id;
        let tx_entry = mvcc_store.txs.get(&tx_id);
        let tx = tx_entry
            .as_ref()
            .map(|entry| entry.value())
            .ok_or_else(|| {
                LimboError::NoSuchTransactionID(format!(
                    "tx id {tx_id} not found in step_build_logical_record"
                ))
            })?;
        let write_set = tx.write_set.lock();
        let write_set_len = write_set.entries.len();
        let CommitState::RewriteLiveVersions(ctx) = &mut self.state else {
            unreachable!("step_rewrite_live_versions requires RewriteLiveVersions state")
        };
        let end_ts = ctx.end_ts;
        if ctx.cursor == 0 {
            let tx_state = mvcc_store
                .txs
                .get(&tx_id)
                .map(|entry| entry.value().state.load());
            turso_assert!(
                matches!(tx_state, Some(TransactionState::Committed(ts)) if ts == end_ts),
                "RewriteLiveVersions requires a committed transaction state"
            );
        }
        let mut iterations = 0;
        while ctx.cursor < write_set_len && iterations < MVCC_COMMIT_BATCH_SIZE {
            let (_id, row_versions) = &write_set.entries[ctx.cursor];
            let mut row_versions = row_versions.write();
            for row_version in row_versions.iter_mut() {
                if let Some(TxTimestampOrID::TxID(rv_id)) = row_version.begin() {
                    if rv_id == tx_id {
                        row_version.set_begin(Some(TxTimestampOrID::Timestamp(end_ts)));
                    }
                }
                if let Some(TxTimestampOrID::TxID(rv_id)) = row_version.end() {
                    if rv_id == tx_id {
                        row_version.set_end(Some(TxTimestampOrID::Timestamp(end_ts)));
                    }
                }
            }
            ctx.cursor += 1;
            iterations += 1;
        }
        if ctx.cursor < write_set_len {
            return Ok(TransitionResult::Io(IOCompletions::Single(
                Completion::new_yield(),
            )));
        }
        self.state = CommitState::FinalizeCommit { end_ts };
        Ok(TransitionResult::Continue)
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

impl<Clock: LogicalClock, A: ConcurrentAllocator> StateTransition for CommitStateMachine<Clock, A> {
    type Context = Arc<MvStore<Clock, A>>;
    type SMResult = ();

    #[tracing::instrument(fields(state = ?self.state), skip(self, mvcc_store), level = Level::DEBUG)]
    fn step(&mut self, mvcc_store: &Self::Context) -> Result<TransitionResult<Self::SMResult>> {
        tracing::trace!("step(state={:?})", self.state);
        match &self.state {
            CommitState::Initial => {
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

                // Atomically generate end_ts and publish Preparing(end_ts) while the
                // clock lock is held. This closes the TOCTOU window
                // Consider the example:
                //
                // tx1 (Active): get_ts for end - 10
                // tx2 (Active): got begin_ts - 11
                // tx2 (Active): does queries but does not see changes by tx1
                // tx1 (Preparing): now stores `end_ts(10)`
                // tx2 (Active): queries again, but now it can see changes by tx1
                //
                // hence we want to guard the timestamp generation by a mutex, only allow next
                // ts to generate when the previous one is used / discarded

                let write_set_is_empty = tx.write_set.lock().is_empty();
                let header_write = tx.header_dirty.load(Ordering::Acquire);
                // Read only is not only exclusive to empty write set, we could be writing the
                // database header here.
                let read_only = write_set_is_empty && !header_write;

                let mut schema_conflict = false;
                let mut exclusive_conflict = false;

                let end_ts = mvcc_store.get_commit_timestamp(|ts| {
                    turso_assert!(
                        ts > tx.begin_ts,
                        "end_ts must be strictly greater than begin_ts"
                    );

                    // First we check if there is exclusive conflict, if there is then we won't
                    // commit txn.
                    if !mvcc_store.is_exclusive_tx(&self.tx_id) && mvcc_store.has_exclusive_tx() {
                        // A non-CONCURRENT transaction is holding the exclusive lock, we must abort.
                        turso_assert_reachable!("commit aborted due to exclusive tx conflict");
                        exclusive_conflict = true;
                    }
                    // Now check if we saw schema change which would require reprepare. Let's note
                    // that we check this right after exlusive tx because we update this before we release
                    // exclusive lock.
                    let schema_updated = mvcc_store
                        .last_committed_schema_change_ts
                        .load(Ordering::Acquire)
                        > tx.begin_ts;
                    // last_committed_schema_ts is not enough, we need to check schema cookie
                    // is the same because e.g:
                    // T1 CREATE INDEX
                    // T1 Yield somewhere in middle of commit
                    // T1 end_ts = x
                    // T2 BEGIN CONCURRENT; INSERT (same table); COMMIT
                    // T2 begin_ts = x+1
                    // T2 begin_ts > end_ts
                    //
                    // Therefore even if exclusive tx (T1) finishes right in time
                    // we will see schema was not updated because we see an younger timestamp and
                    // ours is older!
                    //
                    let our_cookie = tx.header.read().schema_cookie.get();
                    let global_cookie = {
                        let h = mvcc_store.global_header.read();
                        let h = h.as_ref();
                        turso_assert!(h.is_some(), "global_header should be initialized");
                        h.unwrap().schema_cookie.get()
                    };

                    let header_dirty = tx.header_dirty.load(Ordering::Acquire);
                    turso_assert!(!header_dirty || mvcc_store.is_exclusive_tx(&tx.tx_id), "header_dirty=true implies that tx is exclusive");
                    if our_cookie != global_cookie && !header_dirty {
                        tracing::debug!("cookie mismatch in CommitState::Initial tx({our_cookie}) != global(!{global_cookie})");
                        schema_conflict = true;
                    }

                    if schema_updated {
                        tracing::debug!("schema ts is older than our ts");
                        // Schema changes made after the transaction began always cause a [SchemaConflict] error and the tx must abort.
                        schema_conflict = true;
                    }

                    let can_commit_tx = !(exclusive_conflict || schema_conflict);
                    if can_commit_tx || read_only {
                        tx.state.store(TransactionState::Preparing(ts));
                    }
                });
                // We allow reads from happening. Exlusive means there is a single writer.
                if exclusive_conflict && !read_only {
                    return Err(LimboError::WriteWriteConflict);
                }
                if schema_conflict && !read_only {
                    return Err(LimboError::SchemaConflict);
                }
                tracing::trace!("prepare_tx(tx_id={}, end_ts={})", self.tx_id, end_ts);
                /* In order to implement serializability, we need the following steps:
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
                    """If transaction TB is in the Preparing state, it has acquired an end
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
                /* NOTE: Commit dependencies (Hekaton Section 2.7) are implemented via
                 ** the register-and-report protocol:
                 ** - Speculative reads/ignores call register_commit_dependency, which
                 **   increments CommitDepCounter and adds to CommitDepSet.
                 ** - WaitForDependencies checks AbortNow and waits for counter == 0.
                 ** - CommitEnd / rollback_tx drain CommitDepSet, notifying dependents.
                 **
                 ** TODO: For full serializability (beyond snapshot isolation), we still need:
                 ** 1. Validate if all read versions are still visible by inspecting the read_set
                 ** 2. Validate if there are no phantoms by walking the scans from scan_set
                 */
                tracing::trace!("commit_tx(tx_id={})", self.tx_id);
                // Header-only writes must not take this fast path; they need durable log records.
                if read_only {
                    turso_assert!(
                        tx.commit_dep_set.lock().is_empty(),
                        "MVCC read only transaction should not have commit dependencies on other txns"
                    );
                    // Abort eagerly if requested
                    if tx.abort_now.load(Ordering::Acquire) {
                        return Err(LimboError::CommitDependencyAborted);
                    }
                    // Even read-only transactions must honour commit dependencies.
                    // A SELECT during normal processing may have speculatively read
                    // from a Preparing transaction (Hekaton §2.7), incrementing our
                    // CommitDepCounter. We must wait for those to resolve.
                    if tx.commit_dep_counter.load(Ordering::Acquire) > 0 {
                        // Unresolved dependencies — skip validation (no writes)
                        // and go straight to WaitForDependencies.
                        self.state = CommitState::WaitForDependencies { end_ts };
                        return Ok(TransitionResult::Continue);
                    }
                    // Check abort_now AFTER counter: rollback_tx stores abort_now
                    // (Release) before fetch_sub (AcqRel). Once counter == 0, all
                    // decrements have completed and the abort_now flag is visible.
                    if tx.abort_now.load(Ordering::Acquire) {
                        return Err(LimboError::CommitDependencyAborted);
                    }
                    tx.state.store(TransactionState::Committed(end_ts));
                    if mvcc_store.is_exclusive_tx(&self.tx_id) {
                        mvcc_store.release_exclusive_tx(&self.tx_id);
                    }
                    mvcc_store.unlock_commit_lock_if_held(tx);
                    mvcc_store.finish_committed_tx(self.tx_id, &self.connection, self.db_id);
                    inject_transition_failure!(self, CommitYieldPoint::AfterRemoveTx);
                    self.finalize(mvcc_store)?;
                    return Ok(TransitionResult::Done(()));
                }
                self.state = CommitState::Commit { end_ts };
                inject_transition_yield!(self, CommitYieldPoint::CommitValidation);
                Ok(TransitionResult::Continue)
            }
            CommitState::Commit { end_ts } => {
                // Check for rowid conflicts before committing (pure optimistic, first-committer-wins)
                // Ref: Hekaton paper Section 3.2 - validation uses end_ts comparison
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or(LimboError::TxTerminated)?;
                let tx = tx.value();

                for (id, _chain) in tx.write_set.lock().iter() {
                    if id.row_id.is_int_key() {
                        self.check_rowid_for_conflicts(id, *end_ts, tx, mvcc_store)?;
                    } else {
                        self.check_index_for_conflicts(id, *end_ts, tx, mvcc_store)?;
                    }
                }

                // Validation passed. Wait for commit dependencies before building
                // the durable commit record. The live row versions must stay on
                // TxID references until CommitEnd so an abandoned commit can
                // still be rolled back by matching on TxID(self.tx_id).
                self.state = CommitState::WaitForDependencies { end_ts: *end_ts };
                inject_transition_yield!(self, CommitYieldPoint::WaitForDependencies);
                return Ok(TransitionResult::Continue);
            }
            CommitState::WaitForDependencies { end_ts } => {
                let end_ts = *end_ts;
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or(LimboError::TxTerminated)?;
                let tx = tx.value();

                // Eagarly check for abort_now
                if tx.abort_now.load(Ordering::Acquire) {
                    return Err(LimboError::CommitDependencyAborted);
                }
                // Hekaton Section 2.7: "A transaction cannot commit until this
                // counter is zero." Deadlock impossible: edges always go from higher
                // end_ts to lower end_ts, so the wait graph is acyclic.
                if tx.commit_dep_counter.load(Ordering::Acquire) > 0 {
                    return Ok(TransitionResult::Io(IOCompletions::Single(
                        Completion::new_yield(),
                    )));
                }

                // Check abort_now AFTER counter reaches 0. Memory ordering:
                // rollback_tx does abort_now.store(true, Release) BEFORE
                // counter.fetch_sub(1, AcqRel). Our Acquire load of counter==0
                // synchronizes-with that fetch_sub, making the abort_now store
                // visible. Checking in the opposite order (abort_now first) has a
                // TOCTOU race: an aborting dep can set abort_now and decrement
                // between our two reads, letting us see (false, 0) and commit.
                if tx.abort_now.load(Ordering::Acquire) {
                    return Err(LimboError::CommitDependencyAborted);
                }

                // Read-only fast path: if write_set is empty and header was not mutated, commit without
                // going through CommitEnd. CommitEnd updates last_committed_tx_ts
                // which would make a read-only transaction look like a write,
                // causing spurious Busy errors from acquire_exclusive_tx.
                if tx.write_set.lock().is_empty() && !tx.header_dirty.load(Ordering::Acquire) {
                    turso_assert!(
                        tx.commit_dep_set.lock().is_empty(),
                        "MVCC read-only transaction should not have other transactions depending on it"
                    );
                    tx.state.store(TransactionState::Committed(end_ts));
                    if mvcc_store.is_exclusive_tx(&self.tx_id) {
                        mvcc_store.release_exclusive_tx(&self.tx_id);
                        self.commit_coordinator.pager_commit_lock.unlock();
                    }
                    mvcc_store.finish_committed_tx(self.tx_id, &self.connection, self.db_id);
                    inject_transition_failure!(self, CommitYieldPoint::AfterRemoveTx);
                    self.finalize(mvcc_store)?;
                    return Ok(TransitionResult::Done(()));
                }

                // All dependencies resolved. Initialize an empty log record
                // (`buf` is grown incrementally during BuildLogRecord) and
                // snapshot the dirty header for the tail of the payload.
                // Live row versions stay on TxID references until CommitEnd
                // so rollback of an abandoned commit can still match them.
                let pending_header = if tx.header_dirty.load(Ordering::Acquire) {
                    Some(*tx.header.read())
                } else {
                    None
                };
                self.state = CommitState::BuildLogRecord(BuildLogRecordCtx {
                    end_ts,
                    log_record: LogRecord::new(end_ts),
                    cursor: 0,
                    schema_process: true,
                    pending_header,
                });
                return Ok(TransitionResult::Continue);
            }
            // Chunked: yields every MVCC_COMMIT_BATCH_SIZE rowids. Pulled out
            // to a helper because the arm body needs `&mut self` to mutate
            // the cursor / pass / log_record inside the variant, which
            // conflicts with the outer `&self.state` match borrow.
            CommitState::BuildLogRecord(_) => self.step_build_log_record(mvcc_store),
            CommitState::BeginCommitLogicalLog { end_ts, .. } => {
                if !mvcc_store.is_exclusive_tx(&self.tx_id) {
                    // logical log needs to be serialized.
                    let tx = mvcc_store
                        .txs
                        .get(&self.tx_id)
                        .ok_or_else(|| LimboError::NoSuchTransactionID(self.tx_id.to_string()))?;
                    let locked = self.commit_coordinator.pager_commit_lock.write();
                    if !locked {
                        return Ok(TransitionResult::Io(IOCompletions::Single(
                            Completion::new_yield(),
                        )));
                    }
                    tx.value()
                        .pager_commit_lock_held
                        .store(true, Ordering::Release);
                }
                let end_ts = *end_ts;
                let log_record = match std::mem::replace(
                    &mut self.state,
                    CommitState::UpgradeLogicalLogHeader {
                        end_ts,
                        log_record: LogRecord::new(end_ts),
                    },
                ) {
                    CommitState::BeginCommitLogicalLog { log_record, .. } => log_record,
                    _ => unreachable!(),
                };
                self.state = CommitState::UpgradeLogicalLogHeader { end_ts, log_record };
                Ok(TransitionResult::Continue)
            }
            CommitState::UpgradeLogicalLogHeader { end_ts, log_record } => {
                if let Some(c) = mvcc_store.storage.upgrade_header_for_log_tx(log_record)? {
                    if !c.succeeded() {
                        return Ok(TransitionResult::Io(IOCompletions::Single(c)));
                    }
                }
                let end_ts = *end_ts;
                let log_record = match std::mem::replace(
                    &mut self.state,
                    CommitState::WriteLogicalLog {
                        end_ts,
                        log_record: LogRecord::new(end_ts),
                    },
                ) {
                    CommitState::UpgradeLogicalLogHeader { log_record, .. } => log_record,
                    _ => unreachable!(),
                };
                self.state = CommitState::WriteLogicalLog { end_ts, log_record };
                Ok(TransitionResult::Continue)
            }
            CommitState::WriteLogicalLog { end_ts, .. } => {
                let end_ts = *end_ts;
                let log_record = match std::mem::replace(
                    &mut self.state,
                    CommitState::FinishLogicalLogWrite { end_ts },
                ) {
                    CommitState::WriteLogicalLog { log_record, .. } => log_record,
                    _ => unreachable!(),
                };
                let (c, append_bytes) = mvcc_store.storage.log_tx(log_record, None)?;
                self.pending_log_append_bytes = Some(append_bytes);
                // if Completion Completed without errors we can continue
                if c.succeeded() {
                    Ok(TransitionResult::Continue)
                } else {
                    Ok(TransitionResult::Io(IOCompletions::Single(c)))
                }
            }

            CommitState::FinishLogicalLogWrite { end_ts } => {
                let c = mvcc_store.storage.on_log_write_complete()?;
                self.state = CommitState::SyncLogicalLog { end_ts: *end_ts };
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
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or_else(|| LimboError::NoSuchTransactionID(self.tx_id.to_string()))?;
                let tx_unlocked = tx.value();
                let tx_header = *tx_unlocked.header.read();
                let schema_did_change = self.did_commit_schema_change
                    || self
                        .header
                        .read()
                        .as_ref()
                        .map(|header| header.schema_cookie.get())
                        != Some(tx_header.schema_cookie.get());
                self.did_commit_schema_change = schema_did_change;
                if schema_did_change {
                    let schema = self.connection.schema.read().clone();
                    self.connection.db.update_schema_if_newer(schema);
                }
                // Guard the global_header write against out-of-order
                // completion. An exclusive tx can bypass `pager_commit_lock`
                // (the conditional at the top of BeginCommitLogicalLog) and
                // race past us into EndCommitLogicalLog; if its end_ts is
                // higher, its header has already been published, and our
                // older write would regress `global_header.schema_cookie`
                // below the latest committed value. The same monotonicity
                // applies in FinalizeCommit below.
                let prev_hdr_ts = mvcc_store
                    .last_global_header_ts
                    .fetch_max(*end_ts, Ordering::AcqRel);
                if prev_hdr_ts <= *end_ts {
                    self.header.write().replace(tx_header);
                }
                tracing::trace!("end_commit_logical_log(tx_id={})", self.tx_id);
                self.state = CommitState::CommitEnd { end_ts: *end_ts };
                return Ok(TransitionResult::Continue);
            }
            CommitState::CommitEnd { end_ts } => {
                // Order of operations matters here:
                // 1. Advance logical log writer offset (makes the written bytes "owned")
                // 2. Mark transaction Committed
                // 3. Rewrite live row versions from TxID to Timestamp (chunked
                //    in `RewriteLiveVersions` so 2M-row write sets don't stall)
                // 4. Notify dependents
                // 5. Release commit lock (allows next committer)
                // 6. Update cached global header
                //
                // (1) must precede (5): the commit lock serializes log writes, and
                // log_tx() writes at the current offset. If we released the lock before
                // advancing, the next committer would overwrite our bytes.
                //
                // (2) must precede (3): rewriting before marking Committed would
                // publish the transaction's effects to readers before its fate is
                // decided, which breaks rollback of abandoned commits.
                //
                // (2) must also precede (5): the next committer's validation (CommitState::Commit)
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
                        .advance_logical_log_offset_after_success(append_bytes)?;
                }
                tx_unlocked
                    .state
                    .store(TransactionState::Committed(*end_ts));

                // Hand off to the chunked rewriter. Between chunks readers
                // resolve any unwritten TxID refs via `txs[tx_id]` which now
                // reports Committed(end_ts).
                self.state = CommitState::RewriteLiveVersions(RewriteLiveVersionsCtx {
                    end_ts: *end_ts,
                    cursor: 0,
                });
                Ok(TransitionResult::Continue)
            }
            // Chunked: yields every MVCC_COMMIT_BATCH_SIZE rowids. Same
            // helper-dispatch reason as BuildLogRecord above.
            CommitState::RewriteLiveVersions(_) => self.step_rewrite_live_versions(mvcc_store),
            CommitState::FinalizeCommit { end_ts } => {
                let tx = mvcc_store
                    .txs
                    .get(&self.tx_id)
                    .ok_or_else(|| LimboError::NoSuchTransactionID(self.tx_id.to_string()))?;
                let tx_unlocked = tx.value();

                // Hekaton Section 3.3: "The transaction then processes all outgoing
                // commit dependencies listed in its CommitDepSet. If it committed, it
                // decrements the target transaction's CommitDepCounter."
                // IOW since this txn committed, let's signal waiting transactions.
                let dependents = std::mem::take(&mut *tx_unlocked.commit_dep_set.lock());
                for dep_tx_id in dependents {
                    if let Some(dep_tx_entry) = mvcc_store.txs.get(&dep_tx_id) {
                        dep_tx_entry
                            .value()
                            .commit_dep_counter
                            .fetch_sub(1, Ordering::AcqRel);
                    }
                }

                mvcc_store.unlock_commit_lock_if_held(tx_unlocked);

                inject_transition_yield!(self, CommitYieldPoint::BeforeGlobalHeaderUpdate);

                let tx_header = *tx_unlocked.header.read();
                {
                    // Hold the header lock across the watermark update and header
                    // publish so the guard decision and replacement are serialized.
                    let mut global_header = mvcc_store.global_header.write();
                    // Since we assign a commit timestamp and then we drive the commit to completion,
                    // it is totally possible for so an older transaction can finish after a newer one.
                    // In such case, we should not let older commit to set lower value than previous.
                    // This value is used in checkpointing as a watermark boundary, and an incorrect
                    // lower value can cause data loss / corruption.
                    let last_committed_ts = mvcc_store
                        .last_committed_tx_ts
                        .fetch_max(*end_ts, Ordering::AcqRel);
                    if last_committed_ts <= *end_ts {
                        global_header.replace(tx_header);
                    }
                }
                if self.did_commit_schema_change {
                    mvcc_store
                        .last_committed_schema_change_ts
                        .fetch_max(*end_ts, Ordering::AcqRel);
                }

                // We have now updated all the versions with a reference to the
                // transaction ID to a timestamp and can, therefore, remove the
                // transaction. Pair removal with the connection cache clear so
                // an IO yield + abandon during the upcoming checkpoint cannot
                // strand `conn.mv_tx_id` referencing a tx that's gone from `txs`.
                //
                // Release `exclusive_tx` BEFORE `finish_committed_tx` /
                // `inject_transition_failure!` so an Err at `AfterRemoveTx`
                // cannot strand the atomic — matches the ordering at the
                // two fast-path CommitEnd sites.
                if mvcc_store.is_exclusive_tx(&self.tx_id) {
                    mvcc_store.release_exclusive_tx(&self.tx_id);
                }
                inject_transition_yield!(self, CommitYieldPoint::BeforeFinishCommittedTx);
                mvcc_store.finish_committed_tx(self.tx_id, &self.connection, self.db_id);
                inject_transition_failure!(self, CommitYieldPoint::AfterRemoveTx);
                if mvcc_store.storage.should_checkpoint() {
                    let auto_checkpoint_mode = if self
                        .connection
                        .experimental_mvcc_passive_checkpoint_enabled()
                    {
                        crate::storage::wal::CheckpointMode::Passive {
                            upper_bound_inclusive: None,
                        }
                    } else {
                        crate::storage::wal::CheckpointMode::Truncate {
                            upper_bound_inclusive: None,
                        }
                    };
                    let state_machine = StateMachine::new(CheckpointStateMachine::new(
                        self.pager.clone(),
                        mvcc_store.clone(),
                        self.connection.clone(),
                        false,
                        self.connection.get_sync_mode(),
                        self.db_id,
                        auto_checkpoint_mode,
                    ));
                    let state_machine = Mutex::new(state_machine);
                    self.state = CommitState::Checkpoint { state_machine };
                    return Ok(TransitionResult::Continue);
                }
                // Not checkpointing this commit. Reclaim invisible versions
                // inline with a bounded, non-blocking GC pass so steady-state
                // memory stays flat between checkpoints (which only fire on
                // logical-log byte growth, not version accumulation). The pass
                // does no I/O and is capped at MAX_CHAINS_PER_GC chains, so it
                // doesn't meaningfully slow this committing connection. See
                // `gc_incremental`.
                if mvcc_store.should_gc() {
                    mvcc_store.gc_incremental(MvStore::<Clock>::MAX_CHAINS_PER_GC);
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
                        tracing::info!("MVCC auto-checkpoint failed: {err}");
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
                    let mut record = ImmutableRecord::new(row_data.len())?;
                    record.start_serialization(row_data)?;
                    Some(record)
                };
                // `requires_seek == false` is a write_set-level *candidate* for the
                // sequential-write optimization (this row's key is exactly previous
                // row's key + 1, so the cursor — left on the previous row and advanced
                // by WriteRowState::Next — is usually already at the insert position).
                // It is only sound if the cursor is still PAST the start of its leaf:
                // if the previous row was the last cell of its leaf, next() crossed
                // into the following leaf (cell 0), and this row may belong on the
                // other side of the parent divider. Dividers keep their key when the
                // checkpoint deletes their row, so the divider can
                // be >= this row's key, meaning the row MUST go into the left leaf
                // even though the cursor is in the right one. Writing it at the
                // cursor would keep the leaf locally sorted but break the interior
                // ordering invariant, making the row invisible to point lookups
                // ("Rowid N out of order" under sqlite3 integrity_check). Fall back
                // to a real seek, which resolves the divider comparison correctly.
                if self.requires_seek || !self.cursor.read().is_positioned_past_page_start() {
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
                    IOResult::Done(seek_result) => {
                        if self.row.is_index_row() && matches!(seek_result, SeekResult::TryAdvance)
                        {
                            self.state = WriteRowState::Advance;
                            return Ok(TransitionResult::Continue);
                        }
                    }
                    IOResult::IO(io) => {
                        return Ok(TransitionResult::Io(io));
                    }
                }
                turso_assert_eq!(self.cursor.write().valid_state, CursorValidState::Valid);
                self.state = WriteRowState::Insert;
                Ok(TransitionResult::Continue)
            }
            WriteRowState::Advance => {
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
                turso_assert!(
                    self.cursor.read().has_record(),
                    "MVCC checkpoint index insert did not land on the matched interior record"
                );
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

    #[tracing::instrument(fields(state = ?self.state), skip(self, _context), level = Level::TRACE)]
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
    /// Exclusive lock serializing initialization (btree max read → store).
    /// Only held during the first NewRowid for a table; after that, the
    /// fast path is lock-free (atomic CAS on max_rowid).
    lock: TursoRwLock,
    /// Monotonically increasing counter. 0 = empty table (rowids start at 1).
    /// Updated via atomic CAS — no RwLock needed on the fast path.
    max_rowid: AtomicI64,
    /// True after the first btree-max scan. Never reset to false.
    initialized: AtomicBool,
}

/// Sub state machine for [`MvStore::bootstrap_nonblock`]. Carried by the
/// open state machine (`OpenDbAsyncPhase::BootstrapMvStore`) across the
/// metadata-bootstrap IO chain so opening an MVCC database does not block on
/// the log-header truncate / write / fsync sequence, the interrupted-checkpoint
/// reconciliation, the schema reparse, or the metadata-table reads/writes.
#[derive(Default)]
pub enum BootstrapState {
    #[default]
    Start,
    /// Pre-metadata: reconcile an interrupted checkpoint (non-blocking).
    PreCheckpoint {
        tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
        checkpoint_st: CompleteCheckpointState,
    },
    /// Pre-metadata: reparse the schema (non-blocking).
    PreReparse {
        tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
        reparse_st: crate::connection::ReparseSchemaState,
    },
    /// Reading the persistent tx-ts-max from the MVCC metadata table to decide
    /// whether the metadata-bootstrap IO chain is needed.
    BeginReadTxTs {
        read_st: ReadPersistentTxTsMaxState,
    },
    MetadataIo(MetadataIoInFlight),
    /// Finish: create/seed the MVCC metadata table (non-blocking).
    FinishInit {
        init_st: InitMetadataTableState,
    },
    /// Finish: reconcile the interrupted checkpoint again after metadata writes.
    FinishCheckpoint {
        checkpoint_st: CompleteCheckpointState,
    },
    /// Replay the logical log into the MVCC store (non-blocking), then promote
    /// the bootstrap connection to a regular MVCC connection.
    Recover {
        recover_st: RecoverLogicalLogState,
    },
    /// Recover sequence descriptors from each backing table (non-blocking).
    /// The pre-replay reparse missed backing tables created by committed-but-
    /// not-checkpointed CREATE SEQUENCE statements; this pass registers pure
    /// descriptors via the MVCC-aware SQL path. The runtime watermark is never
    /// read — every nextval queries disk on demand.
    LoadSequences {
        load_st: crate::connection::LoadSequenceDescriptorsState,
    },
    /// Compatibility sync for AUTOINCREMENT tables created in WAL mode (where
    /// the watermark lives in sqlite_sequence) and then reopened in MVCC mode
    /// (where the disk-only allocation path reads backing tables). Non-blocking.
    SyncAutoincrement {
        sync_st: crate::connection::SyncAutoincrementState,
    },
    AwaitingGlobalHeader,
}

#[doc(hidden)]
pub struct MetadataIoInFlight {
    pub completion: Completion,
    pub sync_type: FileSyncType,
    pub next: MetadataIoStep,
}

#[doc(hidden)]
#[derive(Clone, Copy)]
pub enum MetadataIoStep {
    /// `log_file.truncate(0)` just finished. If the log was <= header size we
    /// still need to write a fresh header (and maybe fsync). `log_size` is the
    /// original on-disk size; `sync_mode_off` records whether sync is disabled.
    AfterTruncate { log_size: u64, sync_mode_off: bool },
    /// `storage.update_header` just finished. Maybe issue `storage.sync`.
    AfterUpdateHeader { sync_mode_off: bool },
    /// `storage.sync` just finished — metadata IO chain done.
    AfterSync,
}

/// Sub state machine for
/// [`MvStore::maybe_complete_interrupted_checkpoint_nonblock`]. Tracks the
/// sequence of IO yields needed to reconcile an interrupted MVCC checkpoint
/// without blocking: read log header → optional early WAL truncate, or
/// WAL→DB backfill + db_file.sync + log-header rewrite (with a single-shot
/// CRC retry) + final WAL truncate.
#[derive(Default)]
pub enum CompleteCheckpointState {
    #[default]
    Start,
    /// Reading the log header via the streaming reader.
    ReadingHeader {
        reader: Box<StreamingLogicalLogReader>,
    },
    /// `wal_max_frame == 0` branch: driving the early `wal.truncate_wal`.
    /// `checkpoint_result` must persist across IO yields because
    /// `truncate_wal`/`truncate_log` tracks its truncate/sync progress through
    /// its `wal_truncate_sent` / `wal_sync_sent` flags; recreating it each
    /// re-entry would re-issue the truncate forever.
    DriveEarlyTruncate {
        header_result: HeaderReadResult,
        checkpoint_result: CheckpointResult,
    },
    /// Main path: driving `wal.checkpoint(Truncate)`. Reached from either a
    /// Valid header (already reused via `set_header`) or a `NoLog` log (the
    /// normal Passive steady state) — the fresh header is (re)written later in
    /// `RetryHeader`, so no header payload is needed here.
    DriveCheckpoint,
    /// Awaiting the `db_file.sync` completion after a successful backfill.
    AwaitDbFileSync {
        completion: Completion,
        checkpoint_result: CheckpointResult,
    },
    /// Retry loop: rewriting the log header + verifying CRC. `retried_crc`
    /// allows a single retry on torn-tail mismatch before failing closed.
    RetryHeader {
        checkpoint_result: CheckpointResult,
        retried_crc: bool,
        phase: RetryHeaderPhase,
    },
    /// Driving the final `wal.truncate_wal`.
    DriveFinalTruncate { checkpoint_result: CheckpointResult },
}

#[doc(hidden)]
pub enum RetryHeaderPhase {
    /// Need to issue `storage.update_header`.
    NeedUpdateHeader,
    /// Awaiting the `storage.update_header` write.
    AwaitUpdateHeader(Completion),
    /// Awaiting `storage.sync` (only when `sync_mode != Off`).
    AwaitLogSync(Completion),
    /// Reading the log header to verify the CRC.
    AwaitCrcCheck {
        reader: Box<StreamingLogicalLogReader>,
    },
}

/// Sub state machine for [`MvStore::try_read_persistent_tx_ts_max_nonblock`].
/// Holds the prepared metadata-read statement + accumulated value across IO
/// yields while the SELECT runs cooperatively.
#[derive(Default)]
pub enum ReadPersistentTxTsMaxState {
    #[default]
    Start,
    Running {
        stmt: Box<crate::Statement>,
        value: Option<i64>,
    },
}

/// Sub state machine for [`MvStore::initialize_mvcc_metadata_table_nonblock`].
/// Sequences the CREATE TABLE then INSERT statements, holding each prepared
/// statement across IO yields.
#[derive(Default)]
pub enum InitMetadataTableState {
    #[default]
    Start,
    CreateTable {
        stmt: Box<crate::Statement>,
    },
    Insert {
        stmt: Box<crate::Statement>,
    },
}

/// Sub state machine for [`MvStore::maybe_recover_logical_log`]. The
/// setup phases (header read, persistent-tx-ts read, schema-cookie read,
/// sqlite_schema scan) each yield IO; the `Replay` phase carries the loop
/// accumulators in [`RecoverCtx`] across per-frame `next_frame` yields.
#[derive(Default)]
pub enum RecoverLogicalLogState {
    #[default]
    Start,
    ReadHeader {
        reader: Box<StreamingLogicalLogReader>,
        preserved_tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
    },
    ReadTxTs {
        reader: Box<StreamingLogicalLogReader>,
        preserved_tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
        header_present: bool,
        txts_st: ReadPersistentTxTsMaxState,
    },
    ReadCookie {
        reader: Box<StreamingLogicalLogReader>,
        preserved_tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
        persistent_tx_ts_max: u64,
    },
    QuerySchema {
        reader: Box<StreamingLogicalLogReader>,
        preserved_tvfs: Vec<Arc<crate::vtab::VirtualTable>>,
        persistent_tx_ts_max: u64,
        cookie: u32,
        stmt: Option<Box<crate::Statement>>,
        schema_rows: HashMap<i64, ImmutableRecord>,
    },
    Replay {
        ctx: Box<RecoverCtx>,
    },
    Done,
}

/// Accumulated state of the logical-log replay loop, persisted across
/// `next_frame` IO yields. See [`MvStore::maybe_recover_logical_log`].
pub struct RecoverCtx {
    reader: Box<StreamingLogicalLogReader>,
    preserved_table_valued_functions: Vec<Arc<crate::vtab::VirtualTable>>,
    /// Fallback schema cookie (pre-read) used by `recover_build_schema` only
    /// when `global_header` is unset.
    cookie: u32,
    persistent_tx_ts_max: u64,
    replay_cutoff_ts: u64,
    max_commit_ts_seen: u64,
    schema_rows: HashMap<i64, ImmutableRecord>,
    dropped_root_pages: HashSet<i64>,
    current_schema: Arc<Schema>,
    index_infos: HashMap<(MVTableId, IndexOpKind), Arc<IndexInfo>>,
}

/// A multi-version concurrency control database.
#[derive(Debug)]
pub struct MvStore<Clock: LogicalClock, A: ConcurrentAllocator = TursoAllocator> {
    pub rows: SkipMap<RowID, Arc<RwLock<Vec<RowVersion>>>, BasicComparator, A>,
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
    pub table_id_to_rootpage: SkipMap<MVTableId, Option<u64>, BasicComparator, A>,
    /// Unlike table rows which are stored in a single map, we have a separate map for every index
    /// because operations like last() on an index are much easier when we don't have to take the
    /// table identifier into account.
    pub index_rows: SkipMap<MVTableId, IndexRowsMap<A>, BasicComparator, A>,
    txs: SkipMap<TxID, Transaction, BasicComparator, A>,
    /// Final state for removed transactions. Readers may still race with stale TxID
    /// references in row versions after a transaction is removed from `txs`.
    finalized_tx_states: SkipMap<TxID, TransactionState, BasicComparator, A>,
    /// Allocator backing every skiplist in this store, including lazily
    /// created per-index maps in `index_rows`.
    alloc: A,
    tx_ids: AtomicU64,
    version_id_counter: AtomicU64,
    next_rowid: AtomicU64,
    next_table_id: AtomicI64,
    clock: Clock,

    /// MVCC durable storage (logical log writes, checkpoint thresholding, recovery state).
    ///
    /// Stored behind a trait object so callers can inject their own implementation
    /// per database (via `Database::durable_storage`) for testing or custom durability.
    storage: Arc<dyn crate::mvcc::persistent_storage::DurableStorage>,

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
    /// MVCC checkpoints lock readers and writers out of the engine only during the
    /// in-memory marker-publication and metadata-mutation phase. The MvStore → WAL
    /// write-out (BeginPagerTxn → WriteRow → WriteIndexRow → CommitPagerTxn) runs
    /// WITHOUT this lock, so concurrent `BEGIN CONCURRENT`s don't observe the writer
    /// flag and don't return Busy during the I/O-heavy portion of the checkpoint.
    /// The procedure is:
    /// - (unlocked) Snapshot MvStore via snapshot_ts; collect committed versions;
    ///   begin pager txn; write rows; commit pager txn (WAL has the data, fsynced).
    /// - (locked) Take the blocking_checkpoint_lock; publish durable_txid_max,
    ///   global_header and schema roots; release the lock.
    /// - (unlocked) GC; CheckpointWal → SyncDbFile → truncate logical log → TruncateWal.
    blocking_checkpoint_lock: Arc<TursoRwLock>,
    /// Single-orchestrator gate for MVCC checkpoints. Set when a CheckpointStateMachine
    /// is actively running its unlocked write-out phase; cleared on completion or
    /// error. Multiple commits triggering `should_checkpoint()` race to set this;
    /// only one wins and runs the checkpoint, the others skip it. Necessary because
    /// the previously-implicit single-orchestrator invariant (provided by
    /// `blocking_checkpoint_lock` being acquired in AcquireLock as the *first* state)
    /// no longer holds once the lock is moved past the pager-write phase.
    checkpoint_in_progress: AtomicBool,
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
    /// `end_ts` of the most recent tx whose header was written into
    /// `global_header`. Used to gate header writes at both
    /// `EndCommitLogicalLog` and `FinalizeCommit` so an older commit
    /// finishing after a newer one cannot regress
    /// `global_header.schema_cookie` below the latest committed value
    /// — which would break `maybe_reparse_schema`'s cookie-mismatch
    /// early-exit and strand readers in a reparse-WaitForDependencies
    /// deadlock.
    last_global_header_ts: AtomicU64,
    table_id_to_last_rowid: RwLock<HashMap<MVTableId, Arc<RowidAllocator>>>,
    /// Per-sequence first value not guaranteed safe to read past based only on
    /// durable/current sequence state. Active allocations can lower this.
    sequence_watermarks: Mutex<HashMap<String, i64>>,
    /// Per-sequence minimum allocated value for each active transaction.
    ///
    /// This is in-memory and therefore only correct while all MVCC writers for a
    /// database live in one process. Multi-process MVCC will need a shared
    /// coordination mechanism before sync can rely on this watermark.
    sequence_allocations: Mutex<HashMap<String, StdHashMap<TxID, i64>>>,

    /// Approximate count of live row versions across `rows` + `index_rows`.
    /// Incremented on every inserted version, decremented when versions are
    /// reclaimed (GC) or physically removed (savepoint rollback, checkpoint
    /// purge). This is a *heuristic* used only to decide when to run an
    /// incremental GC pass (`should_gc`) — never a correctness input. Aborted
    /// versions are left counted until GC reclaims them (they still occupy
    /// memory). See [`Self::live_version_count_approx`].
    live_version_count_approx: AtomicUsize,
    /// Snapshot of `live_version_count_approx` taken at the end of the last
    /// `gc_incremental` pass. `should_gc` fires once growth since this snapshot
    /// crosses `gc_version_threshold`, giving a roughly fixed GC cadence per N
    /// new versions regardless of how many a single pass reclaims.
    live_versions_at_last_gc: AtomicUsize,
    /// Growth in `live_version_count_approx` (number of newly inserted versions since
    /// the last GC pass) that triggers an incremental GC pass on the commit
    /// path. Negative disables inline GC entirely. Configurable via the
    /// `mvcc_gc_threshold` PRAGMA; mirrors `checkpoint_threshold`.
    gc_version_threshold: AtomicI64,
    /// Resume cursor for the incremental table-row GC sweep: the last `RowID`
    /// processed by the previous `gc_incremental` pass. `None` restarts the
    /// sweep from the beginning of `rows` (and marks the point where index
    /// chains are swept — see `gc_incremental`).
    gc_table_cursor: Mutex<Option<RowID>>,
    /// Resume cursor for the incremental index-row GC sweep: the last
    /// `(index id, key)` processed by the previous `gc_incremental` pass.
    /// `None` restarts from the beginning of `index_rows`. Mirrors
    /// `gc_table_cursor` but over the nested `index_rows` maps, so a single
    /// huge index can't force an unbounded pass.
    gc_index_cursor: Mutex<Option<(MVTableId, Arc<SortableIndexKey>)>>,
    /// Single-flight gate for inline GC. Many connections commit concurrently
    /// (each shares this `MvStore`), and the commit path calls
    /// `gc_incremental` without holding any global lock — so without this gate
    /// several threads would run overlapping GC passes at once. That is *safe*
    /// (per-chain write locks + idempotent reclamation) but wasteful: redundant
    /// scans plus thrashing of `gc_table_cursor`. This flag ensures at most one
    /// inline pass runs at a time; a committer that loses the race simply skips
    /// GC for that commit.
    gc_in_progress: AtomicBool,
    /// LWM observed by the last inline GC pass that actually ran. When a
    /// long-running transaction pins the LWM, re-scanning at the same LWM
    /// reclaims nothing new — any version superseded since then has
    /// `end_ts > LWM` (the pinning txn is older) and stays visible. So an inline
    /// pass short-circuits when the LWM hasn't advanced, avoiding wasted scans
    /// while a long txn is open. `u64::MAX` means "no pass has run yet" and also
    /// the no-active-txn state, which never short-circuits (there is always
    /// potential garbage to reclaim then). Aborted garbage and post-checkpoint
    /// sole-survivors that a skipped pass leaves behind are still collected by
    /// the checkpoint's full sweep.
    gc_last_lwm: AtomicU64,
    experimental_mvcc_passive_checkpoint: bool,
}

impl<Clock: LogicalClock> MvStore<Clock> {
    /// Creates a new database backed by the default [`TursoAllocator`].
    pub fn new(
        clock: Clock,
        storage: Arc<dyn crate::mvcc::persistent_storage::DurableStorage>,
        experimental_mvcc_passive_checkpoint: bool,
    ) -> Self {
        Self::new_in(
            clock,
            storage,
            TursoAllocator,
            experimental_mvcc_passive_checkpoint,
        )
    }
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> MvStore<Clock, A> {
    fn uses_durable_mvcc_metadata(&self, connection: &Arc<Connection>) -> bool {
        !connection.db.is_in_memory_db()
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

    /// Creates a new database whose skiplists allocate through `alloc`.
    pub fn new_in(
        clock: Clock,
        storage: Arc<dyn crate::mvcc::persistent_storage::DurableStorage>,
        alloc: A,
        experimental_mvcc_passive_checkpoint: bool,
    ) -> Self {
        let table_id_to_rootpage = SkipMap::new_in(alloc.clone());
        // table id 1 / root page 1 is always sqlite_schema.
        table_id_to_rootpage.insert(SQLITE_SCHEMA_MVCC_TABLE_ID, Some(1));
        Self {
            rows: SkipMap::new_in(alloc.clone()),
            table_id_to_rootpage,
            index_rows: SkipMap::new_in(alloc.clone()),
            txs: SkipMap::new_in(alloc.clone()),
            finalized_tx_states: SkipMap::new_in(alloc.clone()),
            alloc,
            tx_ids: AtomicU64::new(1), // let's reserve transaction 0 for special purposes
            version_id_counter: AtomicU64::new(1), // Reserve 0 for special purposes
            next_rowid: AtomicU64::new(0), // TODO: determine this from B-Tree
            next_table_id: AtomicI64::new(-2), // table id -1 / root page 1 is always sqlite_schema.
            clock,
            storage,
            exclusive_tx: AtomicU64::new(NO_EXCLUSIVE_TX),
            commit_coordinator: Arc::new(CommitCoordinator::new()),
            global_header: Arc::new(RwLock::new(None)),
            blocking_checkpoint_lock: Arc::new(TursoRwLock::new()),
            checkpoint_in_progress: AtomicBool::new(false),
            durable_txid_max: AtomicU64::new(0),
            last_committed_schema_change_ts: AtomicU64::new(0),
            last_committed_tx_ts: AtomicU64::new(0),
            last_global_header_ts: AtomicU64::new(0),
            table_id_to_last_rowid: RwLock::new(HashMap::default()),
            sequence_watermarks: Mutex::new(HashMap::default()),
            sequence_allocations: Mutex::new(HashMap::default()),
            live_version_count_approx: AtomicUsize::new(0),
            live_versions_at_last_gc: AtomicUsize::new(0),
            gc_version_threshold: AtomicI64::new(Self::DEFAULT_GC_VERSION_THRESHOLD),
            gc_table_cursor: Mutex::new(None),
            gc_index_cursor: Mutex::new(None),
            gc_in_progress: AtomicBool::new(false),
            gc_last_lwm: AtomicU64::new(u64::MAX),
            experimental_mvcc_passive_checkpoint,
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

    pub fn remove_table_id_to_rootpage(&self, table_id: &MVTableId) {
        self.table_id_to_rootpage.remove(table_id);
        self.table_id_to_last_rowid.write().remove(table_id);
    }

    /// Acquire MVCC's stop-the-world gate for VACUUM.
    ///
    /// This is the same lock used by MVCC checkpointing. All MVCC transactions
    /// hold it in read mode for their whole lifetime, so acquiring it in write
    /// mode proves there are no active MVCC transactions and prevents new ones
    /// from starting until VACUUM releases it.
    pub(crate) fn try_begin_vacuum_gate(&self) -> Result<()> {
        if !self.blocking_checkpoint_lock.write() {
            return Err(LimboError::Busy);
        }
        turso_assert!(
            self.txs.is_empty(),
            "MVCC vacuum gate acquired while transactions are still active"
        );
        Ok(())
    }

    /// Release the MVCC stop-the-world gate acquired by `try_begin_vacuum_gate`.
    pub(crate) fn release_vacuum_gate(&self) {
        self.blocking_checkpoint_lock.unlock();
    }

    /// VACUUM copies the physical DB image, so any MVCC logical-log bytes must
    /// be checkpointed first.
    pub(crate) fn has_uncheckpointed_log(&self) -> Result<bool> {
        Ok(self.get_logical_log_file().size()? != 0)
    }

    /// Rebuild MVCC's physical root-page metadata after in-place VACUUM
    /// reparses the rewritten B-tree image and stages the committed page-1
    /// header that now owns the physical schema cookie.
    ///
    /// The caller must hold the MVCC vacuum gate and pass both the committed
    /// page-1 header and the schema parsed from the post-VACUUM physical
    /// database image.
    pub(crate) fn reset_after_vacuum(&self, header: DatabaseHeader, schema: &Schema) {
        turso_assert!(
            self.txs.is_empty(),
            "MVCC VACUUM reset requires no active transactions"
        );
        // see the test `test_mvcc_plain_vacuum_active_write_tx_returns_busy`
        self.drop_unused_row_versions();
        let has_table_versions = self
            .rows
            .iter()
            .any(|entry| !entry.value().read().is_empty());
        turso_assert!(
            !has_table_versions,
            "MVCC VACUUM reset requires checkpointed table versions to be cleared"
        );
        let has_index_versions = self.index_rows.iter().any(|index_entry| {
            index_entry
                .value()
                .iter()
                .any(|entry| !entry.value().read().is_empty())
        });
        turso_assert!(
            !has_index_versions,
            "MVCC VACUUM reset requires checkpointed index versions to be cleared"
        );
        turso_assert!(
            self.finalized_tx_states.is_empty(),
            "MVCC VACUUM reset requires finalized transaction cache to be cleared"
        );
        // Drop empty buckets left by checkpoint GC: their table_ids reference
        // pre-VACUUM root pages and can alias new objects after root-page
        // reuse, corrupting `index_rows` lookups and SkipMap ordering.
        self.rows.clear();
        self.index_rows.clear();
        let root_pages = schema
            .tables
            .values()
            .filter_map(|table| match table.as_ref() {
                Table::BTree(btree) => Some(btree.root_page),
                _ => None,
            })
            .chain(
                schema
                    .indexes
                    .values()
                    .flatten()
                    .map(|index| index.root_page),
            )
            .collect::<Vec<_>>();
        for &root_page in &root_pages {
            turso_assert!(
                root_page > 0,
                "post-VACUUM B-tree root page must be positive"
            );
        }
        self.table_id_to_rootpage.clear();
        self.table_id_to_last_rowid.write().clear();
        self.insert_table_id_to_rootpage(SQLITE_SCHEMA_MVCC_TABLE_ID, Some(1));
        for root_page in root_pages {
            let table_id = MVTableId::from(-root_page);
            self.insert_table_id_to_rootpage(table_id, Some(root_page as u64));
        }
        self.global_header.write().replace(header);
    }

    /// Creates the `__turso_internal_mvcc_meta` table and seeds it with
    /// `persistent_tx_ts_max` (initialized to 0). This table stores the durable replay
    /// boundary: on recovery, only logical-log frames with `commit_ts > persistent_tx_ts_max`
    /// are replayed. Called once during first MVCC bootstrap.
    /// Creates and seeds the MVCC metadata table, driving the CREATE TABLE then
    /// INSERT statements cooperatively via the supplied [`InitMetadataTableState`]
    /// so bootstrap does not block on backends without a synchronous IO pump.
    fn initialize_mvcc_metadata_table_nonblock(
        &self,
        connection: &Arc<Connection>,
        st: &mut InitMetadataTableState,
    ) -> Result<IOResult<()>> {
        loop {
            match st {
                InitMetadataTableState::Start => {
                    let stmt = connection.prepare(format!(
                        "CREATE TABLE IF NOT EXISTS {MVCC_META_TABLE_NAME}(k TEXT, v INTEGER NOT NULL)"
                    ))?;
                    *st = InitMetadataTableState::CreateTable {
                        stmt: Box::new(stmt),
                    };
                }
                InitMetadataTableState::CreateTable { stmt } => {
                    return_if_io!(stmt.run_ignore_rows_nonblock());
                    let stmt = connection.prepare(format!(
                        "INSERT OR IGNORE INTO {MVCC_META_TABLE_NAME}(rowid, k, v) VALUES (1, '{MVCC_META_KEY_PERSISTENT_TX_TS_MAX}', 0)"
                    ))?;
                    *st = InitMetadataTableState::Insert {
                        stmt: Box::new(stmt),
                    };
                }
                InitMetadataTableState::Insert { stmt } => {
                    return_if_io!(stmt.run_ignore_rows_nonblock());
                    *st = InitMetadataTableState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Shared validation/normalization for the persistent-tx-ts-max metadata
    /// value used by both the blocking and non-blocking readers.
    fn validate_persistent_tx_ts_max(value: Option<i64>) -> Result<Option<u64>> {
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

    /// Non-blocking variant of [`Self::try_read_persistent_tx_ts_max`]: runs the
    /// metadata SELECT cooperatively via the supplied
    /// [`ReadPersistentTxTsMaxState`], yielding IO instead of pumping it. Returns
    /// `None` if the metadata table does not exist yet.
    fn try_read_persistent_tx_ts_max_nonblock(
        &self,
        connection: &Arc<Connection>,
        st: &mut ReadPersistentTxTsMaxState,
    ) -> Result<IOResult<Option<u64>>> {
        loop {
            match st {
                ReadPersistentTxTsMaxState::Start => {
                    let query_result = connection.query(format!(
                        "SELECT v FROM {MVCC_META_TABLE_NAME}
                         WHERE k = '{MVCC_META_KEY_PERSISTENT_TX_TS_MAX}'"
                    ));
                    let maybe_stmt = match query_result {
                        Ok(stmt) => stmt,
                        Err(LimboError::ParseError(msg)) if msg.contains("no such table") => {
                            return Ok(IOResult::Done(None))
                        }
                        Err(err) => {
                            return Err(LimboError::Corrupt(format!(
                                "Failed to read MVCC metadata table: {err}"
                            )))
                        }
                    };
                    match maybe_stmt {
                        Some(stmt) => {
                            *st = ReadPersistentTxTsMaxState::Running {
                                stmt: Box::new(stmt),
                                value: None,
                            };
                        }
                        // No statement to run — fall through to the missing-row
                        // error path (matches the blocking variant).
                        None => {
                            return Self::validate_persistent_tx_ts_max(None).map(IOResult::Done)
                        }
                    }
                }
                ReadPersistentTxTsMaxState::Running { stmt, value } => {
                    return_if_io!(stmt.run_with_row_callback_nonblock(|row| {
                        *value = Some(row.get::<i64>(0)?);
                        Ok(())
                    }));
                    let value = *value;
                    *st = ReadPersistentTxTsMaxState::Start;
                    return Self::validate_persistent_tx_ts_max(value).map(IOResult::Done);
                }
            }
        }
    }

    /// Bootstrap the MV store from the SQLite schema table and logical log.
    /// 1. Get all root pages from the already parsed schema object
    /// 2. Assign table IDs to the root pages (table_id = -1 * root_page)
    /// 3. Complete interrupted WAL/log checkpoint reconciliation, if needed
    /// 4. Promote the bootstrap connection to a regular connection so that it reads from the MV store again
    /// 5. Recover the logical log
    /// 6. Make sure schema changes reflected from deserialized logical log are captured in the schema
    ///
    /// Blocking shim retained for [`Connection::attach_database_inner`] and the
    /// `op_journal_mode_inner` VDBE opcode — both of which are synchronous
    /// public/VDBE entry points. The open state machine drives
    /// [`MvStore::bootstrap_nonblock`] directly so a WAL/log header write
    /// during open does not block.
    pub fn bootstrap(&self, bootstrap_conn: Arc<Connection>) -> Result<()> {
        let mut st = BootstrapState::default();
        let io = bootstrap_conn.db.io.clone();
        io.block(|| self.bootstrap_nonblock(&bootstrap_conn, &mut st))
    }

    #[doc(hidden)]
    pub fn bootstrap_nonblock(
        &self,
        bootstrap_conn: &Arc<Connection>,
        st: &mut BootstrapState,
    ) -> Result<IOResult<()>> {
        loop {
            match st {
                BootstrapState::Start => {
                    // Capture built-in table-valued functions before the schema
                    // reparse drops them (sync, no IO).
                    let tvfs = Self::capture_table_valued_functions(&bootstrap_conn.schema.read());
                    *st = BootstrapState::PreCheckpoint {
                        tvfs,
                        checkpoint_st: CompleteCheckpointState::default(),
                    };
                }
                BootstrapState::PreCheckpoint {
                    tvfs,
                    checkpoint_st,
                } => {
                    return_if_io!(self.maybe_complete_interrupted_checkpoint_nonblock(
                        bootstrap_conn,
                        checkpoint_st
                    ));
                    let tvfs = std::mem::take(tvfs);
                    *st = BootstrapState::PreReparse {
                        tvfs,
                        reparse_st: crate::connection::ReparseSchemaState::default(),
                    };
                }
                BootstrapState::PreReparse { tvfs, reparse_st } => {
                    return_if_io!(bootstrap_conn.reparse_schema_nonblock(reparse_st));
                    self.rehydrate_connection_table_valued_functions(bootstrap_conn, tvfs);
                    // pre_metadata done. Decide whether metadata bootstrap IO is needed.
                    if !self.uses_durable_mvcc_metadata(bootstrap_conn) {
                        self.bootstrap_map_root_pages(bootstrap_conn)?;
                        *st = BootstrapState::Recover {
                            recover_st: RecoverLogicalLogState::default(),
                        };
                    } else {
                        *st = BootstrapState::BeginReadTxTs {
                            read_st: ReadPersistentTxTsMaxState::default(),
                        };
                    }
                }
                BootstrapState::BeginReadTxTs { read_st } => {
                    let persistent_tx_ts = return_if_io!(
                        self.try_read_persistent_tx_ts_max_nonblock(bootstrap_conn, read_st)
                    );
                    if persistent_tx_ts.is_some() {
                        // Metadata already durable — no bootstrap IO chain needed.
                        self.bootstrap_map_root_pages(bootstrap_conn)?;
                        *st = BootstrapState::Recover {
                            recover_st: RecoverLogicalLogState::default(),
                        };
                    } else if let Some(next) = self.bootstrap_issue_metadata_io(bootstrap_conn)? {
                        *st = BootstrapState::MetadataIo(next);
                    } else {
                        self.bootstrap_map_root_pages(bootstrap_conn)?;
                        *st = BootstrapState::Recover {
                            recover_st: RecoverLogicalLogState::default(),
                        };
                    }
                }
                BootstrapState::MetadataIo(phase) => {
                    if !phase.completion.succeeded() {
                        let c = phase.completion.clone();
                        io_yield_one!(c);
                    }
                    let sync_type = phase.sync_type;
                    match phase.next {
                        MetadataIoStep::AfterTruncate {
                            log_size,
                            sync_mode_off,
                        } => {
                            // Truncate done. Maybe issue update_header next.
                            if log_size <= LOG_HDR_SIZE as u64 {
                                let c = self.storage.update_header()?;
                                *phase = MetadataIoInFlight {
                                    completion: c,
                                    sync_type,
                                    next: MetadataIoStep::AfterUpdateHeader { sync_mode_off },
                                };
                                // Loop to re-check succeeded immediately
                            } else {
                                *st = BootstrapState::FinishInit {
                                    init_st: InitMetadataTableState::default(),
                                };
                            }
                        }
                        MetadataIoStep::AfterUpdateHeader { sync_mode_off } => {
                            if !sync_mode_off {
                                let c = self.storage.sync(sync_type)?;
                                *phase = MetadataIoInFlight {
                                    completion: c,
                                    sync_type,
                                    next: MetadataIoStep::AfterSync,
                                };
                            } else {
                                *st = BootstrapState::FinishInit {
                                    init_st: InitMetadataTableState::default(),
                                };
                            }
                        }
                        MetadataIoStep::AfterSync => {
                            *st = BootstrapState::FinishInit {
                                init_st: InitMetadataTableState::default(),
                            };
                        }
                    }
                }
                BootstrapState::FinishInit { init_st } => {
                    return_if_io!(
                        self.initialize_mvcc_metadata_table_nonblock(bootstrap_conn, init_st)
                    );
                    *st = BootstrapState::FinishCheckpoint {
                        checkpoint_st: CompleteCheckpointState::default(),
                    };
                }
                BootstrapState::FinishCheckpoint { checkpoint_st } => {
                    return_if_io!(self.maybe_complete_interrupted_checkpoint_nonblock(
                        bootstrap_conn,
                        checkpoint_st
                    ));
                    self.bootstrap_map_root_pages(bootstrap_conn)?;
                    *st = BootstrapState::Recover {
                        recover_st: RecoverLogicalLogState::default(),
                    };
                }
                BootstrapState::Recover { recover_st } => {
                    // Recover the logical log while the bootstrap connection still
                    // reads from the pager-backed schema, so recovery can merge
                    // checkpointed sqlite_schema rows with non-checkpointed rows
                    // from log replay.
                    return_if_io!(self.maybe_recover_logical_log(bootstrap_conn, recover_st));
                    // Recovery done; switch back to regular MVCC reads.
                    bootstrap_conn.promote_to_regular_connection();
                    *st = BootstrapState::LoadSequences {
                        load_st: crate::connection::LoadSequenceDescriptorsState::default(),
                    };
                }
                BootstrapState::LoadSequences { load_st } => {
                    // After log replay, recover sequence descriptors from each
                    // backing table (non-blocking). A read failure on an internal
                    // backing table is on-disk corruption, surfaced as a hard
                    // bootstrap failure rather than a misleading "sequence does
                    // not exist" on the next nextval.
                    return_if_io!(
                        bootstrap_conn.load_sequence_descriptors_via_sql_nonblock(load_st)
                    );
                    *st = BootstrapState::SyncAutoincrement {
                        sync_st: crate::connection::SyncAutoincrementState::default(),
                    };
                }
                BootstrapState::SyncAutoincrement { sync_st } => {
                    // WAL→MVCC AUTOINCREMENT watermark compatibility sync (non-
                    // blocking). A failure here cannot be downgraded: it would
                    // leave the next AUTOINCREMENT INSERT able to re-emit a rowid
                    // already on disk, so it propagates as a bootstrap failure.
                    return_if_io!(bootstrap_conn
                        .sync_autoincrement_backing_tables_from_sqlite_sequence_nonblock(sync_st));
                    *bootstrap_conn.db.schema.lock() = bootstrap_conn.schema.read().clone();
                    *st = BootstrapState::AwaitingGlobalHeader;
                }
                BootstrapState::AwaitingGlobalHeader => {
                    if self.global_header.read().is_none() {
                        let pager = bootstrap_conn.pager.load();
                        let header = return_if_io!(pager.with_header(|header| *header));
                        self.global_header.write().replace(header);
                    }
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Issue the first completion of the metadata-bootstrap IO chain, if one is
    /// needed. Called from the bootstrap state machine only after it has already
    /// confirmed durable MVCC metadata is in use and the persistent tx-ts max is
    /// absent. Returns `Some(MetadataIoInFlight)` with the in-flight completion
    /// and next step, or `None` if the log is already in a clean state.
    fn bootstrap_issue_metadata_io(
        &self,
        bootstrap_conn: &Arc<Connection>,
    ) -> Result<Option<MetadataIoInFlight>> {
        let log_size = self.get_logical_log_file().size()?;
        let pager = bootstrap_conn.pager.load().clone();
        if bootstrap_conn.db.is_readonly() {
            return Err(LimboError::Corrupt(
                "Missing MVCC metadata table in read-only mode".to_string(),
            ));
        }
        if log_size > LOG_HDR_SIZE as u64 {
            return Err(LimboError::Corrupt(
                "Missing MVCC metadata table while logical log state exists".to_string(),
            ));
        }
        let sync_type = pager.get_sync_type();
        let sync_mode_off = bootstrap_conn.get_sync_mode() == SyncMode::Off;

        // First-time MVCC bootstrap: ensure a durable logical-log header exists
        // before any metadata-table writes can commit into WAL. If a previous
        // crash left a torn header tail (0 < size < LOG_HDR_SIZE), clear it
        // before rewriting the header.
        if log_size > 0 && log_size < LOG_HDR_SIZE as u64 {
            let log_file = self.get_logical_log_file();
            let completion = log_file.truncate(0, Completion::new_trunc(|_| {}))?;
            return Ok(Some(MetadataIoInFlight {
                completion,
                sync_type,
                next: MetadataIoStep::AfterTruncate {
                    log_size,
                    sync_mode_off,
                },
            }));
        }
        if log_size <= LOG_HDR_SIZE as u64 {
            let completion = self.storage.update_header()?;
            return Ok(Some(MetadataIoInFlight {
                completion,
                sync_type,
                next: MetadataIoStep::AfterUpdateHeader { sync_mode_off },
            }));
        }
        // Shouldn't reach here given the earlier error returns, but be safe.
        Ok(None)
    }

    /// Sync prelude to logical-log recovery: map all existing checkpointed
    /// sqlite_schema root pages to MVCC table ids (root_page=R → table_id=-R).
    /// Recovery itself (and the connection promotion) is driven separately by
    /// the bootstrap state machine's `Recover` phase so it can yield IO.
    fn bootstrap_map_root_pages(&self, bootstrap_conn: &Arc<Connection>) -> Result<()> {
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
        for root_page in sqlite_schema_root_pages {
            turso_assert!(root_page > 0, "root_page={root_page} must be positive");
            let root_page_as_table_id = MVTableId::from(-(root_page));
            self.insert_table_id_to_rootpage(root_page_as_table_id, Some(root_page as u64));
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
                    begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(
                        tx.tx_id,
                    ))),
                    end: crate::mvcc::database::PackedTs::pack(None),
                    row: row.clone(),
                    btree_resident: false,
                };
                let RowKey::Record(sortable_key) = row.id.row_id else {
                    panic!("Index writes must be to a record");
                };
                // Single SkipMap traversal: pass in a fresh Arc; the SkipMap
                // returns the canonical Arc (ours on miss, an existing one
                // on hit), which we hand to savepoint tracking.
                let (canonical_key, row_versions) =
                    self.insert_index_version(index_id, sortable_key, row_version);
                tx.insert_to_write_set(
                    RowID::new(id.table_id, RowKey::Record(canonical_key.clone())),
                    row_versions,
                );
                tx.record_created_index_version((index_id, canonical_key), version_id);
            }
            None => {
                // NOTE: We do NOT check for conflicts at insert time (pure optimistic).
                // Conflicts are detected at commit time using end_ts comparison.
                // This allows multiple transactions to insert the same rowid,
                // with first-committer-wins semantics.

                let version_id = self.get_version_id();
                let row_version = RowVersion {
                    id: version_id,
                    begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(
                        tx.tx_id,
                    ))),
                    end: crate::mvcc::database::PackedTs::pack(None),
                    row,
                    btree_resident: false,
                };
                tx.record_created_table_version(id.clone(), version_id);
                let allocator = self.get_rowid_allocator(&id.table_id);
                allocator.insert_row_id_maybe_update(id.row_id.to_int_or_panic());
                let row_versions = self.insert_version(id.clone(), row_version);
                tx.insert_to_write_set(id, row_versions);
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
            begin: crate::mvcc::database::PackedTs::pack(None),
            end: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(tx_id))),
            row: row.clone(),
            btree_resident: true,
        };
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        match maybe_index_id {
            Some(index_id) => {
                let RowKey::Record(sortable_key) = row.id.row_id else {
                    panic!("Index writes must be to a record");
                };
                let (canonical_key, row_versions) =
                    self.insert_index_version(index_id, sortable_key, row_version);
                tx.insert_to_write_set(
                    RowID::new(id.table_id, RowKey::Record(canonical_key.clone())),
                    row_versions,
                );
                tx.record_created_index_version((index_id, canonical_key), version_id);
            }
            None => {
                tx.record_created_table_version(id.clone(), version_id);
                let row_versions = self.insert_version(id.clone(), row_version);
                tx.insert_to_write_set(id, row_versions);
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
                    begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(
                        tx.tx_id,
                    ))),
                    end: crate::mvcc::database::PackedTs::pack(None),
                    row: row.clone(),
                    btree_resident: true,
                };
                let RowKey::Record(sortable_key) = row.id.row_id else {
                    panic!("Index writes must be to a record");
                };
                let (canonical_key, row_versions) =
                    self.insert_index_version(index_id, sortable_key, row_version);
                tx.insert_to_write_set(
                    RowID::new(id.table_id, RowKey::Record(canonical_key.clone())),
                    row_versions,
                );
                tx.record_created_index_version((index_id, canonical_key), version_id);
            }
            None => {
                let version_id = self.get_version_id();
                let row_version = RowVersion {
                    id: version_id,
                    begin: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::TxID(
                        tx.tx_id,
                    ))),
                    end: crate::mvcc::database::PackedTs::pack(None),
                    row,
                    btree_resident: true,
                };
                tx.record_created_table_version(id.clone(), version_id);
                let row_versions = self.insert_version(id.clone(), row_version);
                tx.insert_to_write_set(id, row_versions);
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
                let alloc = self.alloc.clone();
                let rows = self
                    .index_rows
                    .get_or_insert_with(index_id, move || SkipMap::new_in(alloc));
                let rows = rows.value();
                let RowKey::Record(sortable_key) = id.row_id.clone() else {
                    panic!("Index deletes must have a record row_id");
                };
                if let Some(ref row_versions_entry) = rows.get(&sortable_key) {
                    // Get the Arc key from the map entry for savepoint tracking
                    let arc_key = row_versions_entry.key().clone();
                    let row_versions = row_versions_entry.value().clone();
                    for rv in row_versions.write().iter_mut().rev() {
                        let tx = self
                            .txs
                            .get(&tx_id)
                            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                        let tx = tx.value();
                        turso_assert_eq!(tx.state, TransactionState::Active);
                        // A transaction cannot delete a version that it cannot see,
                        // nor can it conflict with it.
                        if !rv.is_visible_to(tx, &self.txs, &self.finalized_tx_states) {
                            continue;
                        }
                        if is_write_write_conflict(&self.txs, &self.finalized_tx_states, tx, rv) {
                            turso_assert_reachable!("write-write conflict on delete");
                            return Err(LimboError::WriteWriteConflict);
                        }

                        let version_id = rv.id;
                        rv.set_end(Some(TxTimestampOrID::TxID(tx.tx_id)));
                        let tx = self
                            .txs
                            .get(&tx_id)
                            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                        let tx = tx.value();
                        tx.insert_to_write_set(id, row_versions.clone());
                        tx.record_deleted_index_version((index_id, arc_key), version_id);
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            None => {
                let row_versions_opt = self.rows.get(&id);
                if let Some(ref row_versions_entry) = row_versions_opt {
                    let row_versions = row_versions_entry.value().clone();
                    let mut locked_row_versions = row_versions.write();
                    for rv in locked_row_versions.iter_mut().rev() {
                        let tx = self
                            .txs
                            .get(&tx_id)
                            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                        let tx = tx.value();
                        turso_assert_eq!(tx.state, TransactionState::Active);
                        // A transaction cannot delete a version that it cannot see,
                        // nor can it conflict with it.
                        if !rv.is_visible_to(tx, &self.txs, &self.finalized_tx_states) {
                            continue;
                        }
                        if is_write_write_conflict(&self.txs, &self.finalized_tx_states, tx, rv) {
                            turso_assert_reachable!("write-write conflict on delete");
                            return Err(LimboError::WriteWriteConflict);
                        }

                        let version_id = rv.id;
                        rv.set_end(Some(TxTimestampOrID::TxID(tx.tx_id)));
                        drop(locked_row_versions);
                        drop(row_versions_opt);
                        let tx = self
                            .txs
                            .get(&tx_id)
                            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
                        let tx = tx.value();
                        tx.insert_to_write_set(id.clone(), row_versions.clone());
                        tx.record_deleted_table_version(id.clone(), version_id);
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
    pub fn read(&self, tx_id: TxID, id: &RowID) -> Result<Option<Row>> {
        self.read_from_table_or_index(tx_id, id, None)
    }

    /// Same as read() but can read from a table or an index, indicated by the `maybe_index_id` argument.
    pub fn read_from_table_or_index(
        &self,
        tx_id: TxID,
        id: &RowID,
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
                let alloc = self.alloc.clone();
                let rows = self
                    .index_rows
                    .get_or_insert_with(index_id, move || SkipMap::new_in(alloc));
                let rows = rows.value();
                let RowKey::Record(sortable_key) = &id.row_id else {
                    panic!("Index reads must have a record row_id");
                };
                let row_versions_opt = rows.get(sortable_key);
                if let Some(ref row_versions) = row_versions_opt {
                    let row_versions = row_versions.value().read();
                    if let Some(rv) = row_versions
                        .iter()
                        .rev()
                        .find(|rv| rv.is_visible_to(tx, &self.txs, &self.finalized_tx_states))
                    {
                        return Ok(Some(rv.row.clone()));
                    }
                }
                Ok(None)
            }
            None => {
                if let Some(row_versions) = self.rows.get(id) {
                    let row_versions = row_versions.value().read();
                    if let Some(rv) = row_versions
                        .iter()
                        .rev()
                        .find(|rv| rv.is_visible_to(tx, &self.txs, &self.finalized_tx_states))
                    {
                        return Ok(Some(rv.row.clone()));
                    }
                }
                Ok(None)
            }
        }
    }

    /// Like the table branch of [`read_from_table_or_index`], but reads from an
    /// already-resolved version chain instead of looking the row up in
    /// `self.rows`. Used on the scan path where the cursor's range iterator
    /// already located the `Arc`, so the second skiplist traversal is avoided.
    pub(crate) fn read_visible_from_versions(
        &self,
        tx_id: TxID,
        versions: &RowVersions,
    ) -> Result<Option<Row>> {
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        turso_assert_eq!(tx.state, TransactionState::Active);
        let versions = versions.read();
        if let Some(rv) = versions
            .iter()
            .rev()
            .find(|rv| rv.is_visible_to(tx, &self.txs, &self.finalized_tx_states))
        {
            return Ok(Some(rv.row.clone()));
        }
        Ok(None)
    }

    /// Like [`read_visible_from_versions`] but serializes the visible row's
    /// payload directly into `record` instead of cloning a `Row`. Mirrors the
    /// btree cursor, which serializes a cell straight into its reusable record.
    /// Returns true if a visible version was found. The version-chain read lock
    /// is held only for the serialization copy.
    pub(crate) fn read_visible_into_record(
        &self,
        tx_id: TxID,
        versions: &RowVersions,
        record: &mut ImmutableRecord,
    ) -> Result<bool> {
        let tx = self
            .txs
            .get(&tx_id)
            .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?;
        let tx = tx.value();
        turso_assert_eq!(tx.state, TransactionState::Active);
        let versions = versions.read();
        if let Some(rv) = versions
            .iter()
            .rev()
            .find(|rv| rv.is_visible_to(tx, &self.txs, &self.finalized_tx_states))
        {
            record.invalidate();
            record.start_serialization(rv.row.payload())?;
            return Ok(true);
        }
        Ok(false)
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
        mv_store_iterator: &mut Option<MvccIterator<'static, RowID, A>>,
        tx_id: TxID,
    ) -> Option<(RowID, RowVersions)> {
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
        mv_store_iterator: &mut Option<MvccIterator<'static, Arc<SortableIndexKey>, A>>,
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

    /// Whether an already-resolved index version chain shadows (invalidates) the
    /// corresponding B-tree row for `tx_id`.
    ///
    /// This is exactly the predicate used by the `RowKey::Record` branch of
    /// [`Self::query_btree_version_is_valid`], but it takes the version chain
    /// directly instead of looking it up by key. A forward index scan keeps a
    /// skiplist finger co-positioned with the B-tree and calls this on the
    /// chain the finger already points at, replacing one `index_rows.get()`
    /// (O(log N)) per scanned row with an amortized-O(1) merge step.
    pub(crate) fn index_chain_invalidates_btree(
        &self,
        versions: &RwLock<Vec<RowVersion>>,
        tx_id: TxID,
    ) -> bool {
        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();
        let versions = versions.read();
        versions.iter().rev().any(|version| {
            version.is_btree_invalidating_version(tx, &self.txs, &self.finalized_tx_states)
        })
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
                let Some(versions) = self.rows.get(&row_id_full) else {
                    // No MVCC version -> B-tree is valid
                    return true;
                };
                let versions = versions.value().read();

                // Check if any version invalidates the B-tree row
                let btree_is_invalid = versions.iter().rev().any(|version| {
                    version.is_btree_invalidating_version(tx, &self.txs, &self.finalized_tx_states)
                });

                !btree_is_invalid
            }
            RowKey::Record(record) => {
                let alloc = self.alloc.clone();
                let index_rows = self
                    .index_rows
                    .get_or_insert_with(table_id, move || SkipMap::new_in(alloc));
                let index_rows = index_rows.value();
                let Some(versions) = index_rows.get(record.as_ref()) else {
                    // No MVCC version -> B-tree is valid
                    return true;
                };
                let versions = versions.value().read();

                // Check if any version invalidates the B-tree row
                let btree_is_invalid = versions.iter().rev().any(|version| {
                    version.is_btree_invalidating_version(tx, &self.txs, &self.finalized_tx_states)
                });

                !btree_is_invalid
            }
        }
    }

    fn find_last_visible_version(
        &self,
        tx: &Transaction,
        row: &TableRowEntry<'_, A>,
    ) -> Option<(RowID, RowVersions)> {
        row.value()
            .read()
            .iter()
            .rev()
            .find(|version| version.is_visible_to(tx, &self.txs, &self.finalized_tx_states))
            .map(|_| (row.key().clone(), row.value().clone()))
    }

    fn find_last_visible_index_version(
        &self,
        tx: &Transaction,
        row: IndexRowEntry<'_, A>,
    ) -> Option<RowID> {
        row.value()
            .read()
            .iter()
            .rev()
            .find(|version| version.is_visible_to(tx, &self.txs, &self.finalized_tx_states))
            .map(|version| version.row.id.clone())
    }

    fn find_next_visible_index_row<'a, I>(&self, tx: &Transaction, mut rows: I) -> Option<RowID>
    where
        I: Iterator<Item = IndexRowEntry<'a, A>>,
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
    ) -> Option<(RowID, RowVersions)>
    where
        I: Iterator<Item = TableRowEntry<'a, A>>,
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
        table_iterator: &mut Option<MvccIterator<'static, RowID, A>>,
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
                IterationDirection::Forwards => {
                    Box::new(self.rows.range(range)) as TableRowIterator<'_, A>
                }
                IterationDirection::Backwards => {
                    Box::new(self.rows.range(range).rev()) as TableRowIterator<'_, A>
                }
            }
        };
        *table_iterator = Some(static_iterator_hack!(iter_box, RowID, A));

        let mv_store_iterator = table_iterator
            .as_mut()
            .expect("table_iterator was assigned above if it was None");

        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();

        self.find_next_visible_table_row(tx, mv_store_iterator, table_id)
            .map(|(row_id, _versions)| row_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn seek_index(
        &self,
        index_id: MVTableId,
        start: SortableIndexKey,
        inclusive: bool,
        eq_only: bool,
        direction: IterationDirection,
        tx_id: TxID,
        index_iterator: &mut Option<MvccIterator<'static, Arc<SortableIndexKey>, A>>,
    ) -> Option<RowID> {
        let alloc = self.alloc.clone();
        let index_rows = self
            .index_rows
            .get_or_insert_with(index_id, move || SkipMap::new_in(alloc));
        let index_rows = index_rows.value();
        let range = if eq_only {
            // An eq-only seek (point lookup, NoConflict, unique-constraint probe,
            // index delete) only cares about entries whose key matches `start`.
            // Bound BOTH ends of the range to the probe key so the skiplist walk
            // stops at the matching cluster instead of scanning forward over every
            // invisible neighbor until it happens to find the next visible row.
            //
            // `SortableIndexKey` ordering compares only the probe's columns (see
            // `SortableIndexKey::compare`, which clamps to `min(num_cols)`), so
            // `start..=start` captures all entries sharing the probed prefix
            // regardless of their trailing rowid — exactly the set an eq-only seek
            // may match. Without this bound a single seek costs O(pending invisible
            // versions), which compounds into quadratic work when each row of a
            // concurrent batch insert pays it. The cursor's position after the seek
            // is still set by `PickWinner` from the merged MVCC/B-tree peeks, so
            // returning early here does not change where the cursor lands.
            (Bound::Included(start.clone()), Bound::Included(start))
        } else {
            let start = if inclusive {
                Bound::Included(start)
            } else {
                Bound::Excluded(start)
            };
            create_seek_range(start, direction)
        };
        let iter_box = match direction {
            IterationDirection::Forwards => {
                Box::new(index_rows.range(range)) as IndexRowIterator<'_, A>
            }
            IterationDirection::Backwards => {
                Box::new(index_rows.range(range).rev()) as IndexRowIterator<'_, A>
            }
        };
        *index_iterator = Some(static_iterator_hack!(iter_box, Arc<SortableIndexKey>, A));
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
        connection: &Connection,
    ) -> Result<TxID> {
        #[cfg(not(any(test, injected_yields)))]
        let _ = connection;
        // Existing transactions already hold one blocking-checkpoint read guard
        // from begin_tx(). When upgrading read->write, do not acquire another one.
        let acquires_checkpoint_guard = maybe_existing_tx_id.is_none();
        if acquires_checkpoint_guard && !self.blocking_checkpoint_lock.read() {
            // If there is a stop-the-world checkpoint in progress, we cannot begin any transaction at all.
            return Err(LimboError::Busy);
        }
        let unlock_checkpoint_guard = || {
            if acquires_checkpoint_guard {
                self.blocking_checkpoint_lock.unlock();
            }
        };
        let tx_id = maybe_existing_tx_id.unwrap_or_else(|| self.get_tx_id());
        let begin_ts = if let Some(tx_id) = maybe_existing_tx_id {
            // Upgrade path: the transaction is already published in `txs`
            // (from begin_tx), so it is already visible to compute_lwm().
            self.txs
                .get(&tx_id)
                .ok_or_else(|| LimboError::NoSuchTransactionID(tx_id.to_string()))?
                .value()
                .begin_ts
        } else {
            // Fresh path: publish the transaction into `txs` atomically with
            // begin_ts allocation, under the clock lock — same begin-publish
            // window fix as begin_tx(). Without this, inline GC on a concurrent
            // committer's commit path could compute an LWM above our begin_ts
            // and reclaim a version this snapshot still needs. The header read
            // (possibly blocking I/O) happens before the clock lock is taken;
            // the error paths below remove this tx if begin fails. The real
            // header is set here, so the tail only flips `pager_commit_lock_held`.
            let header = self.get_new_transaction_database_header(&pager);
            self.clock.get_timestamp(|ts| {
                self.txs.insert(tx_id, Transaction::new(tx_id, ts, header));
            })
        };
        #[cfg(any(test, injected_yields))]
        let exclusive_yield_context = YieldContext::new(
            connection.yield_injector(),
            None,
            connection.next_yield_instance_id(),
            tx_id,
        );
        #[cfg(any(test, injected_yields))]
        let exclusive_yield_context = Some(&exclusive_yield_context);
        #[cfg(not(any(test, injected_yields)))]
        let exclusive_yield_context: Option<&YieldContext> = None;

        let already_exclusive = self.is_exclusive_tx(&tx_id);
        if !already_exclusive {
            self.acquire_exclusive_tx(&tx_id, exclusive_yield_context)
                .inspect_err(|_| {
                    // Fresh txns were already published into `txs` above; undo
                    // that so a failed begin doesn't leave a phantom Active txn
                    // pinning the LWM forever.
                    if maybe_existing_tx_id.is_none() {
                        self.txs.remove(&tx_id);
                    }
                    unlock_checkpoint_guard();
                })?;
        }

        // Hoist: validate the existing tx still exists and snapshot the
        // `pager_commit_lock_held` flag BEFORE acquiring `pager_commit_lock`,
        // so a vanished tx cannot strand the lock (#6905).
        let already_holds_commit_lock = match maybe_existing_tx_id {
            Some(existing_tx_id) => {
                let tx = self.txs.get(&existing_tx_id).ok_or_else(|| {
                    if !already_exclusive {
                        self.release_exclusive_tx(&tx_id);
                    }
                    unlock_checkpoint_guard();
                    LimboError::NoSuchTransactionID(existing_tx_id.to_string())
                })?;
                tx.value().pager_commit_lock_held.load(Ordering::Acquire)
            }
            None => false,
        };

        if !already_holds_commit_lock {
            let locked = self.commit_coordinator.pager_commit_lock.write();
            if !locked {
                tracing::debug!(
                    "begin_exclusive_tx: tx_id={} failed with Busy on pager_commit_lock",
                    tx_id
                );
                if maybe_existing_tx_id.is_none() {
                    self.txs.remove(&tx_id);
                }
                if !already_exclusive {
                    self.release_exclusive_tx(&tx_id);
                }
                unlock_checkpoint_guard();
                return Err(LimboError::Busy);
            }
        }

        if let Some(existing_tx_id) = maybe_existing_tx_id {
            // Upgrade path: read the (possibly blocking) header now that all
            // locks are held, then apply it to the already-published txn.
            let header = self.get_new_transaction_database_header(&pager);
            // Re-fetch the Ref now that all blocking I/O is done. If the tx
            // vanished between the earlier validation and now (extraordinarily
            // narrow window — only checkpoint can remove a tx and it cannot
            // race a commit-lock holder), release what we acquired and bail.
            let tx = self.txs.get(&existing_tx_id).ok_or_else(|| {
                if !already_holds_commit_lock {
                    self.commit_coordinator.pager_commit_lock.unlock();
                }
                if !already_exclusive {
                    self.release_exclusive_tx(&tx_id);
                }
                unlock_checkpoint_guard();
                LimboError::NoSuchTransactionID(existing_tx_id.to_string())
            })?;
            tx.value()
                .pager_commit_lock_held
                .store(true, Ordering::Release);
            *tx.value().header.write() = header;
            tracing::trace!(
                "begin_exclusive_tx(tx_id={}, begin_ts={}) - upgraded existing transaction",
                tx_id,
                begin_ts
            );
            tracing::debug!("begin_exclusive_tx: tx_id={} succeeded", tx_id);
            return Ok(tx_id);
        }

        // Fresh path: the transaction (with its header) was already published
        // into `txs` under the clock lock during begin_ts allocation. Now that
        // we hold the commit lock, just record that.
        let tx = self
            .txs
            .get(&tx_id)
            .expect("fresh exclusive tx was published during begin_ts allocation");
        tx.value()
            .pager_commit_lock_held
            .store(true, Ordering::Release);
        tracing::trace!(
            "begin_exclusive_tx(tx_id={}, begin_ts={}) - exclusive write logical log transaction",
            tx_id,
            begin_ts
        );
        tracing::debug!("begin_exclusive_tx: tx_id={} succeeded", tx_id);
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

        // Set txn's header to the global header. Do the (possibly blocking)
        // header read BEFORE taking the clock lock below.
        let header = self.get_new_transaction_database_header(&pager);

        // Allocate begin_ts and publish the transaction into `txs` atomically
        // under the clock lock. This closes the "begin-publish window": between
        // allocating a snapshot timestamp and inserting into `txs`, the txn is
        // invisible to `compute_lwm`. Inline GC runs on the commit path holding
        // only `blocking_checkpoint_lock.read()` (same as us), so a writer that
        // commits in that window could compute an LWM above our begin_ts and
        // reclaim a version this snapshot still needs — a snapshot-isolation
        // violation. Publishing under the clock lock orders us strictly before
        // or after any commit timestamp (commits also take the clock lock), so
        // any GC that runs after a later commit already sees our begin_ts.
        let begin_ts = self.clock.get_timestamp(|ts| {
            self.txs.insert(tx_id, Transaction::new(tx_id, ts, header));
        });
        tracing::trace!("begin_tx(tx_id={}, begin_ts={})", tx_id, begin_ts);

        Ok(tx_id)
    }

    pub fn remove_tx(&self, tx_id: TxID) {
        self.remove_sequence_allocations(tx_id);
        if let Some(entry) = self.txs.get(&tx_id) {
            let tx = entry.value();
            if let TransactionState::Committed(commit_ts) = tx.state.load() {
                // Read-only transactions cannot leave row versions with stale TxID
                // references, so they do not need finalized-state caching.
                if !tx.write_set.lock().is_empty() {
                    self.finalized_tx_states
                        .insert(tx_id, TransactionState::Committed(commit_ts));
                }
            }
            let dep_set = std::mem::take(&mut *tx.commit_dep_set.lock());
            // Invariant: commit_dep_set must be drained before removing the transaction.
            // CommitEnd and rollback_tx both drain the commit_dep_set to notify dependencies.
            // If we remove a transaction with non-empty commit_dep_set, those dependencies will wait
            // forever (deadlock).
            turso_assert!(
                dep_set.is_empty(),
                "remove_tx({tx_id}): commit_dep_set is not empty"
            );
        }
        self.txs.remove(&tx_id);
        self.blocking_checkpoint_lock.unlock();
    }

    pub fn register_sequence_allocation(
        &self,
        tx_id: TxID,
        sequence_name: &str,
        sequence_value: i64,
    ) -> Result<()> {
        let Some(tx) = self.txs.get(&tx_id) else {
            return Err(LimboError::NoSuchTransactionID(tx_id.to_string()));
        };
        turso_assert!(
            matches!(
                tx.value().state.load(),
                TransactionState::Active | TransactionState::Preparing(_)
            ),
            "sequence allocation must be registered while the transaction is active or preparing"
        );

        let sequence_name = crate::util::normalize_ident(sequence_name);
        let mut allocations = self.sequence_allocations.lock();
        let tx_allocations = allocations.entry(sequence_name).or_default();
        tx_allocations
            .entry(tx_id)
            .and_modify(|value| *value = (*value).min(sequence_value))
            .or_insert(sequence_value);
        Ok(())
    }

    pub fn set_sequence_watermark(&self, sequence_name: &str, watermark: i64) {
        let sequence_name = crate::util::normalize_ident(sequence_name);
        self.sequence_watermarks
            .lock()
            .insert(sequence_name, watermark);
    }

    /// Returns the first sequence value that is not safe for cursor scans to pass.
    ///
    /// Readers can safely consume rows with sequence values less than this
    /// watermark. The value is the minimum of the current sequence boundary and
    /// any lower value already allocated by an active transaction.
    pub fn sequence_watermark(&self, sequence_name: &str) -> Option<i64> {
        let sequence_name = crate::util::normalize_ident(sequence_name);
        let mut allocations = self.sequence_allocations.lock();
        let mut remove_allocations = false;
        let active_watermark = {
            allocations
                .get_mut(&sequence_name)
                .and_then(|tx_allocations| {
                    tx_allocations.retain(|tx_id, _| {
                        self.txs.get(tx_id).is_some_and(|tx| {
                            matches!(
                                tx.value().state.load(),
                                TransactionState::Active | TransactionState::Preparing(_)
                            )
                        })
                    });
                    let watermark = tx_allocations.values().copied().min();
                    if tx_allocations.is_empty() {
                        remove_allocations = true;
                    }
                    watermark
                })
        };
        if remove_allocations {
            allocations.remove(&sequence_name);
        }
        let current_watermark = self.sequence_watermarks.lock().get(&sequence_name).copied();
        match (current_watermark, active_watermark) {
            (Some(current), Some(active)) => Some(current.min(active)),
            (Some(current), None) => Some(current),
            (None, active) => active,
        }
    }

    fn remove_sequence_allocations(&self, tx_id: TxID) {
        let mut allocations = self.sequence_allocations.lock();
        allocations.retain(|_, tx_allocations| {
            tx_allocations.remove(&tx_id);
            !tx_allocations.is_empty()
        });
    }

    /// Atomically retire a committed tx: clear the connection's mv_tx_id cache
    /// for `db_id`, then remove the tx from `txs`. Pairs the two mutations so
    /// no concurrent observer (or in-flight statement) can see the divergent
    /// `(cache=Some, txs=None)` state — the production-panic shape from
    /// `release_named_savepoint` and `NoSuchTransactionID` read-path errors.
    ///
    /// Order matches `rollback_tx` (cache cleared before `remove_tx`) so the
    /// commit and rollback paths are symmetric.
    ///
    /// Use this anywhere the commit state machine would otherwise call
    /// `remove_tx` directly. Other call sites that don't have a connection
    /// context (e.g. tests poking internal state) keep using `remove_tx`.
    pub fn finish_committed_tx(&self, tx_id: TxID, conn: &Connection, db_id: usize) {
        conn.set_mv_tx_for_db(db_id, None);
        self.remove_tx(tx_id);
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

    /// Update the cached global header's page size to match a fresh `PRAGMA page_size`.
    ///
    /// `global_header` is captured from the pager during MVCC bootstrap, before any PRAGMA
    /// can run, so it always starts at the default 4 KiB. Subsequent transactions copy from
    /// it, which means a `PRAGMA page_size = N` issued on the connection would otherwise be
    /// invisible to MVCC header lookups even though the pager itself honors `N` for on-disk
    /// page allocation. Only valid before any data has been written; matches the same
    /// precondition `Connection::reset_page_size` enforces via `db.initialized()`.
    pub fn set_global_page_size(&self, size: PageSize) {
        let mut header = self.global_header.write();
        if let Some(header) = header.as_mut() {
            header.page_size = size;
        }
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
            let out = f(&mut header);
            // Commit path consults this flag to decide whether a header-only logical-log record
            // is required even when write_set stays empty.
            tx.value().header_dirty.store(true, Ordering::Release);
            Ok(out)
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
        self: &Arc<Self>,
        tx_id: TxID,
        connection: &Arc<Connection>,
        db_id: usize,
    ) -> Result<StateMachine<Box<CommitStateMachine<Clock, A>>>> {
        let state = Box::new(CommitStateMachine::new(
            CommitState::Initial,
            tx_id,
            self.clone(),
            connection.clone(),
            db_id,
            self.commit_coordinator.clone(),
            self.global_header.clone(),
            connection.get_sync_mode(),
        ));
        let state_machine = StateMachine::new(state);
        Ok(state_machine)
    }

    /// Returns true if the transaction can be rolled back (Active or Preparing).
    pub fn is_tx_rollbackable(&self, tx_id: TxID) -> bool {
        self.txs.get(&tx_id).is_some_and(|tx| {
            matches!(
                tx.value().state.load(),
                TransactionState::Active | TransactionState::Preparing(_)
            )
        })
    }

    /// Rolls back a transaction with the specified ID.
    ///
    /// This function rolls back a transaction with the specified `tx_id` by
    /// discarding any changes made by the transaction.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - The ID of the transaction to abort.
    /// * `db` - The database index this transaction belongs to.
    pub fn rollback_tx(&self, tx_id: TxID, _pager: Arc<Pager>, connection: &Connection, db: usize) {
        self.rollback_tx_inner(tx_id, Some(connection), db);
    }

    fn rollback_tx_inner(&self, tx_id: TxID, connection: Option<&Connection>, db: usize) {
        let tx_unlocked = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx_unlocked.value();
        if let Some(connection) = connection {
            connection.set_mv_tx_for_db(db, None);
        }
        turso_assert!(matches!(
            tx.state.load(),
            TransactionState::Active | TransactionState::Preparing(_)
        ));
        tx.state.store(TransactionState::Aborted);
        tracing::trace!("abort(tx_id={})", tx_id);
        self.unlock_commit_lock_if_held(tx);

        // Hekaton Section 3.3: "If it aborted, it forces the dependent transactions
        // to also abort by setting their AbortNow flags."
        let dependents = std::mem::take(&mut *tx.commit_dep_set.lock());
        // a txn cannot depend on itself
        turso_assert!(
            !dependents.contains(&tx_id),
            "rollback_tx: transaction has itself in its own commit_dep_set"
        );
        for dep_tx_id in dependents {
            if let Some(dep_tx_entry) = self.txs.get(&dep_tx_id) {
                let dep_tx = dep_tx_entry.value();
                dep_tx.abort_now.store(true, Ordering::Release);
                dep_tx.commit_dep_counter.fetch_sub(1, Ordering::AcqRel);
            }
        }

        if self.is_exclusive_tx(&tx_id) {
            self.release_exclusive_tx(&tx_id);
        }

        // Snapshot under the lock so we can drop it before recursing into
        // `rollback_rowid` (which may take other locks).
        let write_set_snapshot: Vec<(RowID, RowVersions)> = tx.write_set.lock().to_vec();
        for (_rowid, row_versions) in &write_set_snapshot {
            for rv in row_versions.write().iter_mut() {
                rollback_row_version(tx_id, rv);
            }
        }

        if let Some(connection) = connection {
            if connection.schema.read().schema_version > connection.db.schema.lock().schema_version
            {
                // Connection made schema changes during tx and rolled back -> revert connection-local schema.
                *connection.schema.write() = connection.db.clone_schema();
            }
        }

        let tx = tx_unlocked.value();
        tx.state.store(TransactionState::Terminated);
        tracing::trace!("terminate(tx_id={})", tx_id);
        // Safe to remove here: the rollback loop above acquired the write lock on
        // every row version chain in the write set, clearing all TxID references.
        // Any concurrent reader that held a read lock on one of those chains has
        // already completed its register_commit_dependency call (it runs under the
        // read lock), so no future txs.get() for this tx_id can come from a
        // speculative read path.
        self.remove_tx(tx_id);
    }

    fn cleanup_dropped_commit(&self, tx_id: TxID, connection: &Connection, db_id: usize) {
        let tx_state = self.txs.get(&tx_id).map(|tx| tx.value().state.load());
        match tx_state {
            Some(TransactionState::Active | TransactionState::Preparing(_)) => {
                self.rollback_tx_inner(tx_id, Some(connection), db_id);
            }
            Some(TransactionState::Committed(_)) => {
                if let Some(tx) = self.txs.get(&tx_id) {
                    self.unlock_commit_lock_if_held(tx.value());
                }
                if self.is_exclusive_tx(&tx_id) {
                    self.release_exclusive_tx(&tx_id);
                }
                self.finish_committed_tx(tx_id, connection, db_id);
            }
            Some(TransactionState::Aborted | TransactionState::Terminated) | None => {
                if connection.get_mv_tx_id_for_db(db_id) == Some(tx_id) {
                    connection.set_mv_tx_for_db(db_id, None);
                }
                if self.is_exclusive_tx(&tx_id) {
                    self.release_exclusive_tx(&tx_id);
                }
            }
        }
    }

    fn unlock_commit_lock_if_held(&self, tx: &Transaction) {
        if tx.pager_commit_lock_held.swap(false, Ordering::AcqRel) {
            self.commit_coordinator.pager_commit_lock.unlock();
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

    /// Begin a user-visible named savepoint inside an existing transaction.
    ///
    /// `starts_transaction` is true when the savepoint was opened in autocommit mode and therefore
    /// releasing the root savepoint should commit the transaction.
    pub fn begin_named_savepoint(
        &self,
        tx_id: TxID,
        name: String,
        starts_transaction: bool,
        deferred_fk_violations: isize,
    ) {
        let tx = self.txs.get(&tx_id).unwrap_or_else(|| {
            panic!("Transaction {tx_id} not found while beginning named savepoint")
        });
        tx.value()
            .begin_named_savepoint(name, starts_transaction, deferred_fk_violations);
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

    /// Releases a named savepoint and nested savepoints above it.
    ///
    /// Returns [SavepointResult::Commit] when releasing the root savepoint should commit the
    /// transaction.
    pub fn release_named_savepoint(&self, tx_id: TxID, name: &str) -> Result<SavepointResult> {
        let tx = self
            .txs
            .get(&tx_id)
            .unwrap_or_else(|| panic!("Transaction {tx_id} not found while releasing savepoint"));
        Ok(tx.value().release_named_savepoint(name))
    }

    /// Rolls back a savepoint within a transaction.
    /// Returns true if a savepoint was rolled back, false if no savepoint was active.
    pub fn rollback_first_savepoint(&self, tx_id: u64) -> Result<bool> {
        let tx = self.txs.get(&tx_id).unwrap_or_else(|| {
            panic!("Transaction {tx_id} not found while rolling back savepoint")
        });

        let tx = tx.value();
        let savepoint = tx.pop_statement_savepoint();

        if let Some(savepoint) = savepoint {
            self.rollback_savepoint_changes(tx_id, savepoint);
            Ok(true)
        } else {
            tracing::debug!(
                "rollback_savepoint(tx_id={}): no savepoint was active",
                tx_id
            );
            Ok(false)
        }
    }

    /// Rolls back to the newest matching named savepoint while keeping that savepoint active.
    ///
    /// Returns the deferred FK snapshot stored on the named savepoint, or `None` if no matching
    /// savepoint exists.
    pub fn rollback_to_named_savepoint(&self, tx_id: TxID, name: &str) -> Result<Option<isize>> {
        let tx = self.txs.get(&tx_id).unwrap_or_else(|| {
            panic!("Transaction {tx_id} not found while rolling back named savepoint")
        });
        let Some(SavepointRollbackResult {
            rolledback_savepoints,
            deferred_fk_violations,
        }) = tx.value().rollback_to_named_savepoint(name)
        else {
            return Ok(None);
        };

        for savepoint in rolledback_savepoints.into_iter().rev() {
            self.rollback_savepoint_changes(tx_id, savepoint);
        }

        Ok(Some(deferred_fk_violations))
    }

    fn rollback_savepoint_changes(&self, tx_id: TxID, savepoint: Savepoint) {
        let Savepoint {
            header,
            header_dirty,
            created_table_versions,
            created_index_versions,
            deleted_table_versions,
            deleted_index_versions,
            newly_added_to_write_set,
            ..
        } = savepoint;

        tracing::debug!(
            "rollback_savepoint(tx_id={}, created_table={}, created_index={}, deleted_table={}, deleted_index={})",
            tx_id,
            created_table_versions.len(),
            created_index_versions.len(),
            deleted_table_versions.len(),
            deleted_index_versions.len()
        );

        let mut touched_rowids = BTreeSet::new();

        for (rowid, version_id) in created_table_versions {
            touched_rowids.insert(rowid.clone());
            if let Some(entry) = self.rows.get(&rowid) {
                let mut versions = entry.value().write();
                let before = versions.len();
                versions.retain(|rv| rv.id != version_id);
                self.dec_live_version_count_approx(before - versions.len());
                tracing::debug!(
                    "rollback_savepoint: removed table version(table_id={}, row_id={}, version_id={})",
                    rowid.table_id,
                    rowid.row_id,
                    version_id
                );
            }
        }

        for ((table_id, key), version_id) in created_index_versions {
            if let Some(index) = self.index_rows.get(&table_id) {
                if let Some(entry) = index.value().get(&key) {
                    let mut versions = entry.value().write();
                    let before = versions.len();
                    versions.retain(|rv| rv.id != version_id);
                    self.dec_live_version_count_approx(before - versions.len());
                    tracing::debug!(
                        "rollback_savepoint: removed index version(table_id={}, version_id={})",
                        table_id,
                        version_id
                    );
                }
            }
        }

        for (rowid, version_id) in deleted_table_versions {
            touched_rowids.insert(rowid.clone());
            if let Some(entry) = self.rows.get(&rowid) {
                let mut versions = entry.value().write();
                for rv in versions.iter_mut() {
                    if rv.id == version_id {
                        rv.set_end(None);
                        tracing::debug!(
                            "rollback_savepoint: restored table version(table_id={}, row_id={}, version_id={})",
                            rowid.table_id,
                            rowid.row_id,
                            version_id
                        );
                        break;
                    }
                }
            }
        }

        for ((table_id, key), version_id) in deleted_index_versions {
            if let Some(index) = self.index_rows.get(&table_id) {
                if let Some(entry) = index.value().get(&key) {
                    let mut versions = entry.value().write();
                    for rv in versions.iter_mut() {
                        if rv.id == version_id {
                            rv.set_end(None);
                            tracing::debug!(
                                "rollback_savepoint: restored index version(table_id={}, version_id={})",
                                table_id,
                                version_id
                            );
                            break;
                        }
                    }
                }
            }
        }

        touched_rowids.extend(newly_added_to_write_set.into_iter().map(|(id, _)| id));
        self.remove_rolled_back_rows_from_write_set(tx_id, touched_rowids.clone());

        let tx = self
            .txs
            .get(&tx_id)
            .unwrap_or_else(|| panic!("Transaction {tx_id} not found while restoring savepoint"));
        let tx = tx.value();
        *tx.header.write() = header;
        tx.header_dirty.store(header_dirty, Ordering::Release);
    }

    fn row_has_uncommitted_version_for_tx(&self, rowid: &RowID, tx_id: TxID) -> bool {
        if rowid.row_id.is_int_key() {
            let Some(entry) = self.rows.get(rowid) else {
                return false;
            };
            let versions = entry.value().read();
            return versions.iter().any(|rv| {
                rv.begin() == Some(TxTimestampOrID::TxID(tx_id))
                    || rv.end() == Some(TxTimestampOrID::TxID(tx_id))
            });
        }

        let RowKey::Record(ref record) = rowid.row_id else {
            return false;
        };
        let Some(index) = self.index_rows.get(&rowid.table_id) else {
            return false;
        };
        let Some(entry) = index.value().get(record.as_ref()) else {
            return false;
        };
        let versions = entry.value().read();
        versions.iter().any(|rv| {
            rv.begin() == Some(TxTimestampOrID::TxID(tx_id))
                || rv.end() == Some(TxTimestampOrID::TxID(tx_id))
        })
    }

    fn remove_rolled_back_rows_from_write_set(&self, tx_id: TxID, rowids: BTreeSet<RowID>) {
        if rowids.is_empty() {
            return;
        }
        let Some(tx) = self.txs.get(&tx_id) else {
            return;
        };
        let tx = tx.value();
        // Single pass: drop entries that appear in `rowids` AND have no
        // surviving uncommitted version (parent savepoints may still pin
        // them).
        let mut write_set = tx.write_set.lock();
        write_set.retain(|rowid, _rv| {
            if !rowids.contains(rowid) {
                return true;
            }
            self.row_has_uncommitted_version_for_tx(rowid, tx_id)
        });
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

    fn has_preparing_tx_other_than(&self, tx_id: TxID) -> bool {
        self.txs.iter().any(|entry| {
            *entry.key() != tx_id
                && matches!(entry.value().state.load(), TransactionState::Preparing(_))
        })
    }

    /// Acquires the exclusive transaction lock to the given transaction ID.
    fn acquire_exclusive_tx(
        &self,
        tx_id: &TxID,
        yield_context: Option<&YieldContext>,
    ) -> Result<()> {
        #[cfg(not(any(test, injected_yields)))]
        let _ = yield_context;
        if self.exclusive_tx.load(Ordering::Acquire) == *tx_id {
            // Re-entrant upgrade attempt for the same transaction.
            return Ok(());
        }
        // if some other transaction is in preparing state, then we cannot let this tx to
        // continue, as the preparing txn will eventually commit
        if self.has_preparing_tx_other_than(*tx_id) {
            return Err(LimboError::Busy);
        }
        // after we acquired begin_ts, we will check if some other txn committed in the meantime.
        // If so, no point in letting this txn to progress as it's begin_ts is less than
        // other txn's commit ts.
        // do note that this is an optimistic / early check. We need to check this again after this
        // txn gets exclusive txn status. check below after the CAS
        if let Some(tx) = self.txs.get(tx_id) {
            let tx = tx.value();
            if tx.begin_ts < self.last_committed_tx_ts.load(Ordering::Acquire) {
                // Another transaction committed after this transaction's begin timestamp, do not allow exclusive lock.
                // This mimics regular (non-CONCURRENT) sqlite transaction behavior.
                return Err(LimboError::Busy);
            }
        }
        #[cfg(any(test, injected_yields))]
        if let Some(yield_context) = yield_context {
            if yield_context.injector.as_ref().is_some_and(|injector| {
                injector.should_yield(
                    yield_context.instance_id,
                    yield_context.selection_key,
                    ExclusiveTxYieldPoint::AfterTimestampCheckBeforeCas.point(),
                )
            }) {
                tracing::debug!(
                    tx_id,
                    "injected exclusive acquisition interleaving before CAS"
                );
            }
        }
        match self.exclusive_tx.compare_exchange(
            NO_EXCLUSIVE_TX,
            *tx_id,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                if self.has_preparing_tx_other_than(*tx_id) {
                    self.release_exclusive_tx(tx_id);
                    return Err(LimboError::Busy);
                }
                // we will check again, if some other txn committed in the meantime.
                // we did this check previously too, but we will have to do this again.
                // consider this timeline of events:
                //
                // t0 - no txn is preparing, and last_committed_ts is smaller than begin_ts
                //      we proceed to do CAS
                // t1 - we are yet to do CAS, but another txn comes in, goes into
                //      preparing state, and commits with commit_ts greater than our begin_ts
                // t3 - we proceed with CAS and get exclusive txn status
                //
                // at this point, we have got exclusive txn but last_committed_tx_ts is greater than
                // our begin_ts, violating the isolation guarantee.
                //
                // we want to prevent this. so we acquire the exclusive txn and then check again.
                // we can also be sure that once we get the exclusive txn status, no other txn can sneak in and commit,
                // because to commit, we make sure that there is no other exclusive txn. Check
                // `CommitState::Initial`.
                if let Some(tx) = self.txs.get(tx_id) {
                    let tx = tx.value();
                    if tx.begin_ts < self.last_committed_tx_ts.load(Ordering::Acquire) {
                        self.release_exclusive_tx(tx_id);
                        return Err(LimboError::Busy);
                    }
                }
                Ok(())
            }
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

    /// Generate a begin timestamp. No side-effect needed alongside generation.
    pub fn get_begin_timestamp(&self) -> u64 {
        self.clock.get_timestamp(crate::mvcc::clock::no_op)
    }

    /// Generate a commit timestamp and call `f` with it while the clock
    /// lock is held, atomically publishing the timestamp before release.
    /// See [`MvccClock`] for the full explanation.
    pub fn get_commit_timestamp<F: FnOnce(u64)>(&self, f: F) -> u64 {
        self.clock.get_timestamp(f)
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

    /// Default `mvcc_gc_threshold`: run an incremental GC pass roughly every
    /// this many newly inserted versions. Small enough that steady-state
    /// memory stays bounded under heavy short-txn concurrency, large enough
    /// that small workloads (and most unit tests) never trigger a pass.
    pub const DEFAULT_GC_VERSION_THRESHOLD: i64 = 16 * 1024;

    /// Upper bound on table-row chains scanned by one inline `gc_incremental`
    /// pass on the commit path. Keeps a pass cheap (sub-millisecond) so it
    /// doesn't noticeably slow the committing connection; steady state relies
    /// on frequent passes resuming via `gc_table_cursor`.
    pub const MAX_CHAINS_PER_GC: usize = 4096;

    /// Current approximate live row-version count. Heuristic only (see the
    /// `live_version_count_approx` field) — never use for correctness decisions.
    pub fn live_version_count_approx(&self) -> usize {
        self.live_version_count_approx.load(Ordering::Relaxed)
    }

    /// Saturating decrement of the live-version heuristic. The counter is
    /// approximate, so clamp at zero rather than risk an underflow wrap that
    /// would make `should_gc` fire on every commit.
    fn dec_live_version_count_approx(&self, n: usize) {
        if n == 0 {
            return;
        }
        let mut current = self.live_version_count_approx.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(n);
            match self.live_version_count_approx.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Set the inline-GC trigger threshold (growth in live versions since the
    /// last GC pass). Negative disables inline GC. Mirrors
    /// `set_checkpoint_threshold`; wired to the `mvcc_gc_threshold` PRAGMA.
    pub fn set_gc_threshold(&self, threshold: i64) {
        self.gc_version_threshold
            .store(threshold, Ordering::Relaxed);
    }

    pub fn gc_threshold(&self) -> i64 {
        self.gc_version_threshold.load(Ordering::Relaxed)
    }

    /// Whether an incremental GC pass should run now: inline GC is enabled
    /// (threshold >= 0) and `live_version_count_approx` has grown past the threshold
    /// since the last pass. Heuristic — drift only changes GC frequency.
    pub fn should_gc(&self) -> bool {
        let threshold = self.gc_version_threshold.load(Ordering::Relaxed);
        if threshold < 0 {
            return false;
        }
        let current = self.live_version_count_approx.load(Ordering::Relaxed);
        let at_last = self.live_versions_at_last_gc.load(Ordering::Relaxed);
        current.saturating_sub(at_last) >= threshold as usize
    }

    /// Garbage-collects row versions that are invisible to all active transactions.
    /// Uses the low-water mark (LWM) to determine reclaimability in O(1) per version.
    /// Covers both table rows (`self.rows`) and index rows (`self.index_rows`).
    /// Returns the number of removed versions.
    pub fn drop_unused_row_versions(&self) -> usize {
        self.drop_unused_row_versions_inner(false)
    }

    /// Like [`Self::drop_unused_row_versions`], but additionally removes chain
    /// slots that end up empty from the skip maps, bounding their entry counts.
    ///
    /// The caller must hold the blocking checkpoint lock (or otherwise guarantee
    /// no concurrent writers): slot removal happens after the chain write lock
    /// is dropped, so without that guarantee it races a concurrent
    /// `get_or_insert_with` on the same key — see the TOCTOU note in
    /// `gc_table_row_versions`.
    pub fn drop_unused_row_versions_and_slots(&self) -> usize {
        self.drop_unused_row_versions_inner(true)
    }

    /// Incremental, non-blocking GC pass — the inline counterpart to
    /// [`Self::drop_unused_row_versions`], driven from the commit path.
    ///
    /// Reclaims invisible versions (same rules as `gc_version_chain`) from up
    /// to `max_chains` table-row chains, resuming from where the previous pass
    /// stopped (`gc_table_cursor`) so repeated calls eventually cover the whole
    /// `rows` map without scanning it all at once.
    ///
    /// Safety / design notes:
    /// - **Lazy mode only.** Empty SkipMap slots are left in place (no
    ///   `entry.remove()`), so the pass needs no blocking checkpoint lock — it
    ///   races no concurrent `get_or_insert_with` (see the TOCTOU note in
    ///   `gc_table_row_versions`). Physical slot removal stays exclusive to the
    ///   checkpoint's `_and_slots` sweep.
    /// - `finalized_tx_states` pruning is intentionally skipped here: it needs
    ///   the *complete* referenced-txid set across all chains, which a partial
    ///   sweep cannot produce. The checkpoint path still prunes it.
    pub fn gc_incremental(&self, max_chains: usize) -> usize {
        // Hold the checkpoint read lock for the whole pass so a stop-the-world
        // checkpoint cannot run concurrently.
        if !self.blocking_checkpoint_lock.read() {
            return 0;
        }
        struct CheckpointReadGuard<'a>(&'a TursoRwLock);
        impl Drop for CheckpointReadGuard<'_> {
            fn drop(&mut self) {
                self.0.unlock();
            }
        }
        let _ckpt_guard = CheckpointReadGuard(&self.blocking_checkpoint_lock);

        // Single-flight: only one inline GC pass runs at a time across all
        // connections (see `gc_in_progress`). Losing the race is a no-op — the
        // growth that triggered this commit's `should_gc` will retrigger soon,
        // or the in-flight pass already covers the same chains. The RAII guard
        // releases the gate even if the body panics, so a stuck flag can never
        // wedge GC for the lifetime of the store.
        if self
            .gc_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return 0;
        }
        struct GcGate<'a>(&'a AtomicBool);
        impl Drop for GcGate<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _gate = GcGate(&self.gc_in_progress);

        let lwm = self.compute_lwm();

        // Short-circuit when a long-running transaction has pinned the LWM at
        // the same value since the last pass: nothing newly reclaimable can
        // exist below it (any version superseded since then ends above the
        // pinning txn's begin_ts). `u64::MAX` (no active txns) never
        // short-circuits — that is exactly when the most is reclaimable. Reset
        // the trigger baseline so `should_gc` doesn't spin on every commit
        // while the LWM is stuck.
        if lwm != u64::MAX && lwm == self.gc_last_lwm.load(Ordering::Relaxed) {
            self.live_versions_at_last_gc.store(
                self.live_version_count_approx.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            return 0;
        }

        let ckpt_max = self.durable_txid_max.load(Ordering::SeqCst);

        let mut dropped = 0;
        let mut processed = 0;
        let mut last_key: Option<RowID> = None;

        // Resume strictly after the last key processed by the previous pass.
        let start_bound = match self.gc_table_cursor.lock().clone() {
            Some(k) => Bound::Excluded(k),
            None => Bound::Unbounded,
        };
        for entry in self.rows.range((start_bound, Bound::Unbounded)) {
            if processed >= max_chains {
                break;
            }
            {
                let mut versions = entry.value().write();
                dropped += Self::gc_version_chain(
                    &mut versions,
                    lwm,
                    ckpt_max,
                    self.experimental_mvcc_passive_checkpoint,
                );
            }
            last_key = Some(entry.key().clone());
            processed += 1;
        }

        // If we processed fewer chains than the budget, the range was
        // exhausted — wrap the cursor to the start for the next pass.
        let table_wrapped = processed < max_chains;
        *self.gc_table_cursor.lock() = if table_wrapped { None } else { last_key };

        // Sweep index chains with their own bounded, resumable budget (see
        // `gc_index_incremental`). Each map has an independent cursor so a
        // single huge index can't force an unbounded pass.
        dropped += self.gc_index_incremental(lwm, ckpt_max, max_chains);

        self.dec_live_version_count_approx(dropped);
        // Reset the trigger baseline so `should_gc` measures growth from here.
        self.live_versions_at_last_gc.store(
            self.live_version_count_approx.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );

        // Record the LWM only once a full cycle (both cursors wrapped back to
        // the start) has completed at this LWM, so the short-circuit above
        // never skips chains we haven't yet swept at the current LWM. Until the
        // cycle finishes, leave `gc_last_lwm` unchanged so subsequent passes
        // keep scanning.
        let full_cycle =
            self.gc_table_cursor.lock().is_none() && self.gc_index_cursor.lock().is_none();
        if full_cycle {
            self.gc_last_lwm.store(lwm, Ordering::Relaxed);
        }

        if dropped > 0 {
            tracing::trace!(
                "gc_incremental: reclaimed {dropped} versions ({processed} table chains, table_wrapped={table_wrapped}), live~{}",
                self.live_version_count_approx.load(Ordering::Relaxed)
            );
        }
        dropped
    }

    /// Resumable index-chain GC: the index-map counterpart to the table sweep
    /// in [`Self::gc_incremental`]. Applies `gc_version_chain` to up to
    /// `max_chains` index version chains, resuming strictly after the
    /// `(index id, key)` the previous pass stopped at (`gc_index_cursor`) and
    /// wrapping to the start when the nested maps are exhausted. Lazy mode: it
    /// never removes empty slots (no blocking lock), exactly like the table
    /// sweep. Returns the number of versions reclaimed.
    ///
    /// `index_rows` is nested (`MVTableId -> key -> chain`), so the cursor is a
    /// `(MVTableId, key)` pair: the outer scan resumes at the saved index id
    /// (inclusive, to finish its remaining keys) and the inner scan resumes
    /// after the saved key; later indexes start from their first key.
    fn gc_index_incremental(&self, lwm: u64, ckpt_max: u64, max_chains: usize) -> usize {
        let mut dropped = 0;
        let mut processed = 0;
        let mut last: Option<(MVTableId, Arc<SortableIndexKey>)> = None;

        let cursor = self.gc_index_cursor.lock().clone();
        let outer_start = match &cursor {
            Some((index_id, _)) => Bound::Included(*index_id),
            None => Bound::Unbounded,
        };
        'outer: for outer in self.index_rows.range((outer_start, Bound::Unbounded)) {
            if processed >= max_chains {
                break;
            }
            let index_id = *outer.key();
            let inner = outer.value();
            // Resume after the saved key only within the index that the cursor
            // pointed into; every later index starts from its first key.
            let inner_start = match &cursor {
                Some((cursor_id, key)) if *cursor_id == index_id => Bound::Excluded(key.clone()),
                _ => Bound::Unbounded,
            };
            for inner_entry in inner.range((inner_start, Bound::Unbounded)) {
                if processed >= max_chains {
                    break 'outer;
                }
                {
                    let mut versions = inner_entry.value().write();
                    dropped += Self::gc_version_chain(
                        &mut versions,
                        lwm,
                        ckpt_max,
                        self.experimental_mvcc_passive_checkpoint,
                    );
                }
                last = Some((index_id, inner_entry.key().clone()));
                processed += 1;
            }
        }

        // Fewer chains than the budget => nested maps exhausted; wrap to start.
        let wrapped = processed < max_chains;
        *self.gc_index_cursor.lock() = if wrapped { None } else { last };
        dropped
    }

    fn drop_unused_row_versions_inner(&self, remove_empty_slots: bool) -> usize {
        let lwm = self.compute_lwm();
        let ckpt_max = self.durable_txid_max.load(Ordering::SeqCst);
        let mut referenced_tx_ids = HashSet::default();

        let dropped =
            self.gc_table_row_versions(lwm, ckpt_max, &mut referenced_tx_ids, remove_empty_slots)
                + self.gc_index_row_versions(
                    lwm,
                    ckpt_max,
                    &mut referenced_tx_ids,
                    remove_empty_slots,
                );
        self.dec_live_version_count_approx(dropped);
        let pruned_finalized = self.prune_finalized_tx_states(&referenced_tx_ids);

        tracing::trace!(
            "drop_unused_row_versions() -> dropped {dropped}, pruned_finalized={pruned_finalized}, txs: {}, finalized_tx_states: {}, rows: {}",
            self.txs.len(),
            self.finalized_tx_states.len(),
            self.rows.len()
        );
        dropped
    }

    fn gc_table_row_versions(
        &self,
        lwm: u64,
        ckpt_max: u64,
        referenced_tx_ids: &mut HashSet<TxID>,
        remove_empty_slots: bool,
    ) -> usize {
        let mut dropped = 0;

        for entry in self.rows.iter() {
            let is_now_empty = {
                let mut versions = entry.value().write();
                dropped += Self::gc_version_chain(
                    &mut versions,
                    lwm,
                    ckpt_max,
                    self.experimental_mvcc_passive_checkpoint,
                );
                Self::collect_referenced_txids(&versions, referenced_tx_ids);
                versions.is_empty()
            };
            // Unless the caller holds the blocking checkpoint lock
            // (`remove_empty_slots`), empty entries are left in the SkipMap
            // (lazy removal). This avoids a TOCTOU race where a concurrent
            // writer inserts a version between the emptiness check and
            // SkipMap::remove(). Empty entries are reused by
            // get_or_insert_with on subsequent inserts and cleaned up by
            // checkpoint-time GC which runs under the blocking lock.
            if remove_empty_slots && is_now_empty {
                entry.remove();
            }
        }
        dropped
    }

    fn gc_index_row_versions(
        &self,
        lwm: u64,
        ckpt_max: u64,
        referenced_tx_ids: &mut HashSet<TxID>,
        remove_empty_slots: bool,
    ) -> usize {
        let mut dropped = 0;

        for outer_entry in self.index_rows.iter() {
            let inner_map = outer_entry.value();

            for inner_entry in inner_map.iter() {
                let is_now_empty = {
                    let mut versions = inner_entry.value().write();
                    dropped += Self::gc_version_chain(
                        &mut versions,
                        lwm,
                        ckpt_max,
                        self.experimental_mvcc_passive_checkpoint,
                    );
                    Self::collect_referenced_txids(&versions, referenced_tx_ids);
                    versions.is_empty()
                };
                // Same TOCTOU rationale as table rows. The outer per-index map
                // is kept even when emptied — it is bounded by index count.
                if remove_empty_slots && is_now_empty {
                    inner_entry.remove();
                }
            }
        }
        dropped
    }

    fn collect_referenced_txids(versions: &[RowVersion], referenced_tx_ids: &mut HashSet<TxID>) {
        for version in versions {
            if let Some(TxTimestampOrID::TxID(tx_id)) = version.begin() {
                referenced_tx_ids.insert(tx_id);
            }
            if let Some(TxTimestampOrID::TxID(tx_id)) = version.end() {
                referenced_tx_ids.insert(tx_id);
            }
        }
    }

    fn prune_finalized_tx_states(&self, referenced_tx_ids: &HashSet<TxID>) -> usize {
        if self.finalized_tx_states.is_empty() {
            return 0;
        }

        let to_remove: Vec<TxID> = self
            .finalized_tx_states
            .iter()
            .filter_map(|entry| {
                let tx_id = *entry.key();
                (!referenced_tx_ids.contains(&tx_id)).then_some(tx_id)
            })
            .collect();

        for tx_id in &to_remove {
            self.finalized_tx_states.remove(tx_id);
        }

        to_remove.len()
    }

    /// Apply GC rules to a single version chain. Returns number of versions removed.
    ///
    /// Rule 1: Aborted garbage (begin=None, end=None) — always remove.
    /// Rule 2: Superseded (end=Timestamp(e)) — remove once no reader can see it
    ///         (e <= lwm) AND its supersession is durable (e <= ckpt_max).
    /// Rule 3: Current checkpointed sole-survivor (end=None, b <= ckpt_max,
    ///         b < lwm, no other versions remain) — remove.
    ///
    fn gc_version_chain(
        versions: &mut Vec<RowVersion>,
        lwm: u64,
        ckpt_max: u64,
        passive: bool,
    ) -> usize {
        let before = versions.len();

        // Rule 1: aborted garbage
        versions.retain(|rv| !matches!((&rv.begin(), &rv.end()), (None, None)));

        let has_current = versions.iter().any(|rv| {
            matches!(rv.begin(), Some(TxTimestampOrID::Timestamp(_))) && rv.end().is_none()
        });

        // Rule 2: superseded versions are removable only when BOTH consumers of
        // the chain are done with them:
        //   - readers:    e <= lwm — no present or future snapshot can land
        //     inside the version's [begin, end) interval;
        //   - checkpoint: e <= ckpt_max — the durable boundary has passed the
        //     supersession, so the B-tree no longer reflects this version.
        //
        // The second condition must hold even when a newer current version
        // exists in the chain. The off-lock checkpoint can durably write a
        // version and publish boundary < e while a concurrent UPDATE/DELETE has
        // already superseded it in memory; until a later checkpoint catches up
        // past e, this version's `begin <= boundary` is the chain's only proof
        // (for exists_in_db_file) that the B-tree physically contains the row.
        // Dropping it strands the B-tree copy forever: a later committed DELETE
        // classifies as "row never durably existed" and is silently skipped,
        // desyncing the table from its indexes (an UPDATE keeps the table rowid
        // in the same chain but moves the index entry to a new key chain, so
        // only the table side loses its anchor).
        versions.retain(|rv| match &rv.end() {
            Some(TxTimestampOrID::Timestamp(e)) if *e <= lwm => {
                if passive {
                    *e > ckpt_max
                } else {
                    !has_current && *e > ckpt_max
                }
            }
            _ => true,
        });

        // Rule 3: checkpointed sole-survivor current version.
        // Safe to remove only when the B-tree has the data (b <= ckpt_max),
        // no reader needs the MVCC copy (b < lwm), and no superseded versions
        // remain that would poison is_btree_invalidating_version.
        if versions.len() == 1 {
            if let (Some(TxTimestampOrID::Timestamp(b)), None) =
                (&versions[0].begin(), &versions[0].end())
            {
                if *b <= ckpt_max && *b < lwm {
                    versions.clear();
                }
            }
        }

        Self::shrink_version_chain_allocation(versions);

        before - versions.len()
    }

    /// Chains with capacity at or below this are never shrunk — the
    /// allocation is too small to be worth a realloc.
    const CHAIN_SHRINK_MIN_CAPACITY: usize = 16;

    /// Release excess version-chain capacity after GC trimmed the chain.
    /// `retain`/`clear` keep the Vec's allocation, so a one-off burst of
    /// versions (e.g. a hot row between checkpoints) would otherwise pin its
    /// peak allocation forever. Capacity drops to a quarter of its current
    /// value — deliberately not to fit — when the survivors occupy less than
    /// a quarter of it, so steady-state chains keep slack for new versions.
    fn shrink_version_chain_allocation(versions: &mut Vec<RowVersion>) {
        let capacity = versions.capacity();
        if capacity > Self::CHAIN_SHRINK_MIN_CAPACITY && versions.len() < capacity / 4 {
            versions.shrink_to(capacity / 4);
        }
    }

    // Extracts the begin timestamp from a transaction
    #[inline]
    fn resolve_begin_timestamp(&self, ts_or_id: &Option<TxTimestampOrID>) -> u64 {
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

    /// Inserts a new row version into the database, while making sure that the row version
    /// is inserted in the correct order. Returns a reference to the modified version chain.
    fn insert_version(&self, id: RowID, row_version: RowVersion) -> RowVersions {
        let versions = self
            .rows
            .get_or_insert_with(id, || Arc::new(RwLock::new(Vec::new())));
        let row_versions = versions.value().clone();
        self.insert_version_raw(&mut row_versions.write(), row_version);
        row_versions
    }

    /// Gets an existing Arc<SortableIndexKey> from the index if the key exists,
    /// otherwise creates a new Arc. This ensures we reuse Arc instances for the same key.
    fn get_or_create_index_key_arc(
        &self,
        index_id: MVTableId,
        key: Arc<SortableIndexKey>,
    ) -> Arc<SortableIndexKey> {
        let alloc = self.alloc.clone();
        let index = self
            .index_rows
            .get_or_insert_with(index_id, move || SkipMap::new_in(alloc));
        let index = index.value();
        let existing = index.get(&*key).map(|entry| entry.key().clone());
        existing.unwrap_or(key)
    }

    /// Inserts (or appends to) the version chain for an index entry and returns
    /// the id and versions of the modified row.
    pub fn insert_index_version(
        &self,
        index_id: MVTableId,
        key: Arc<SortableIndexKey>,
        mut row_version: RowVersion,
    ) -> (Arc<SortableIndexKey>, RowVersions) {
        let alloc = self.alloc.clone();
        let index = self
            .index_rows
            .get_or_insert_with(index_id, move || SkipMap::new_in(alloc));
        let index = index.value();
        let entry = index.get_or_insert_with(key, || Arc::new(RwLock::new(Vec::new())));
        // The Arc that's actually stored in the SkipMap may be the one we
        // passed in (on miss) or a pre-existing one (on hit). Return that
        // canonical Arc so savepoint tracking and the SkipMap stay in sync.
        let canonical_key = entry.key().clone();
        row_version.row.id.row_id = RowKey::Record(canonical_key.clone());
        let row_versions = entry.value().clone();
        self.insert_version_raw(&mut row_versions.write(), row_version);
        (canonical_key, row_versions)
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
            let existing_begin = self.resolve_begin_timestamp(&v.begin());
            let new_begin = self.resolve_begin_timestamp(&row_version.begin());
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
                        (&versions[i].begin(), &row_version.begin()),
                        (
                            Some(TxTimestampOrID::Timestamp(existing)),
                            Some(TxTimestampOrID::Timestamp(new))
                        ) if existing == new
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
        // A genuine insert (the collapse branch above `return`s without
        // reaching here). Track it for the GC trigger heuristic.
        self.live_version_count_approx
            .fetch_add(1, Ordering::Relaxed);
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

    /// Clear every version-chain entry for `rowid`. Used by checkpoint-time
    /// compaction that deletes the corresponding B-tree row outside the
    /// normal MVCC delete path (e.g. `SeqCompactDriver` for sequence
    /// backing tables) so the two layers stay in sync — without this the
    /// version chain would keep `RowVersion { begin: Timestamp(T), end:
    /// None, btree_resident: true }` entries pointing at B-tree rows that
    /// no longer exist, until `drop_unused_row_versions` Rule 3 caught up.
    ///
    /// **Caller contract** — the caller must hold a guarantee that no
    /// concurrent reader can observe mid-purge state. Today the only
    /// caller is `SeqCompactDriver`, which runs inside the checkpoint
    /// while the `pager_commit_lock` is held; nextval allocators serialize
    /// through that same lock, so they cannot see the chain in a partially
    /// purged state. Callers without that guarantee must add proper
    /// tombstones via the normal write path instead.
    ///
    /// Empty chain slots are left in the `SkipMap` (lazy removal). The
    /// same TOCTOU rationale as `gc_table_row_versions` applies: removing
    /// the slot would race a concurrent `get_or_insert_with` from a future
    /// write to the same key.
    pub fn purge_row_versions_during_checkpoint(&self, rowid: RowID) {
        if let Some(entry) = self.rows.get(&rowid) {
            let mut versions = entry.value().write();
            self.dec_live_version_count_approx(versions.len());
            versions.clear();
            Self::shrink_version_chain_allocation(&mut versions);
        }
    }

    pub fn get_last_table_rowid(
        &self,
        table_id: MVTableId,
        table_iterator: &mut Option<MvccIterator<'static, RowID, A>>,
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
        *table_iterator = Some(static_iterator_hack!(iter_box, RowID, A));
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
        tx_id: TxID,
        index_iterator: &mut Option<MvccIterator<'static, Arc<SortableIndexKey>, A>>,
    ) -> Option<RowKey> {
        let alloc = self.alloc.clone();
        let index = self
            .index_rows
            .get_or_insert_with(index_id, move || SkipMap::new_in(alloc));
        let index = index.value();
        let iter_box = Box::new(index.iter().rev());
        *index_iterator = Some(static_iterator_hack!(iter_box, Arc<SortableIndexKey>, A));
        let iter = index_iterator
            .as_mut()
            .expect("index_iterator was assigned above");
        let tx = self
            .txs
            .get(&tx_id)
            .expect("transaction should exist in txs map");
        let tx = tx.value();
        self.find_next_visible_index_row(tx, iter)
            .map(|row| row.row_id)
    }

    pub fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.storage.get_logical_log_file()
    }

    pub fn logical_log_offset(&self) -> u64 {
        self.storage.logical_log_offset()
    }

    /// Replace the logical log with a fresh valid header after the database
    /// file was restored outside MVCC.
    ///
    /// The returned completion must finish before reopening/recovering MVCC
    /// state. Otherwise recovery could replay stale local logical-log frames on
    /// top of the restored database image.
    pub fn reset_logical_log_after_external_restore(&self) -> Result<Completion> {
        self.storage.reset_to_fresh_header()
    }

    /// Return the durable sync completion for the freshly reset logical log.
    ///
    /// This is separate from `reset_logical_log_after_external_restore` so
    /// callers can drive the reset completion cooperatively, then issue the
    /// ordered sync only after the header/truncate group has completed.
    pub fn sync_logical_log_after_external_restore(
        &self,
        connection: &Arc<Connection>,
    ) -> Result<Option<Completion>> {
        if connection.get_sync_mode() != SyncMode::Off {
            let pager = connection.pager.load().clone();
            return Ok(Some(self.storage.sync(pager.get_sync_type())?));
        }
        Ok(None)
    }

    /// Reconcile WAL state left by a prior crash or incomplete checkpoint.
    /// Classifies startup state by WAL frame count and logical-log header
    /// validity, then either completes the interrupted checkpoint
    /// (backfill WAL → DB, sync, truncate) or fails closed on corrupt /
    /// inconsistent artifacts. Non-blocking: all IO yields through the
    /// supplied [`CompleteCheckpointState`].
    /// See RECOVERY_SEMANTICS.md "Startup Case Classification" for the full case table.
    pub(crate) fn maybe_complete_interrupted_checkpoint_nonblock(
        &self,
        connection: &Arc<Connection>,
        st: &mut CompleteCheckpointState,
    ) -> Result<IOResult<()>> {
        let pager = connection.pager.load().clone();
        let Some(wal) = &pager.wal else {
            return Ok(IOResult::Done(()));
        };
        loop {
            match st {
                CompleteCheckpointState::Start => {
                    // The bootstrap connection may have acquired a WAL read lock during
                    // earlier bootstrap steps (e.g. schema parsing). Drop it so the
                    // TRUNCATE checkpoint below isn't blocked by our own read lock.
                    // Idempotent across re-entry (holds_read_lock is checked first).
                    if wal.holds_read_lock() {
                        wal.end_read_tx();
                    }
                    let file = self.get_logical_log_file();
                    // Header is never encrypted; no EncryptionContext needed.
                    *st = CompleteCheckpointState::ReadingHeader {
                        reader: Box::new(StreamingLogicalLogReader::new(file, None)),
                    };
                }
                CompleteCheckpointState::ReadingHeader { reader } => {
                    let header_result = return_if_io!(reader.try_read_header_nonblock());
                    let wal_max_frame = wal.get_max_frame_in_wal();
                    let is_readonly = connection.db.is_readonly();
                    if wal_max_frame == 0 {
                        if is_readonly {
                            // Nothing to reconcile in read-only mode with no committed frames.
                            *st = CompleteCheckpointState::Start;
                            return Ok(IOResult::Done(()));
                        }
                        *st = CompleteCheckpointState::DriveEarlyTruncate {
                            header_result,
                            checkpoint_result: CheckpointResult::new(0, 0, 0),
                        };
                        continue;
                    }
                    if is_readonly {
                        return Err(LimboError::Corrupt(
                            "Cannot reconcile interrupted MVCC checkpoint in read-only mode"
                                .to_string(),
                        ));
                    }
                    match header_result {
                        HeaderReadResult::Valid(header) => {
                            // Interrupted checkpoint with the logical log still
                            // present: reuse its header so the fresh-header write in
                            // RetryHeader keeps the existing salt chain.
                            self.storage.set_header(header);
                        }
                        HeaderReadResult::NoLog => {
                            if !connection.experimental_mvcc_passive_checkpoint_enabled() {
                                return Err(LimboError::Corrupt(
                                    "WAL has committed frames but logical log header is missing"
                                        .to_string(),
                                ));
                            }
                        }
                        HeaderReadResult::Invalid => {
                            // A present-but-undecodable header is a torn header write
                            // / genuine corruption, not a clean truncation — fail
                            // closed.
                            return Err(LimboError::Corrupt(
                                "WAL has committed frames but logical log header is invalid"
                                    .to_string(),
                            ));
                        }
                    }
                    // Enter the checkpoint lifecycle before any WAL→DB backfill so
                    // that `DurableStorage` implementations observing the
                    // start/end pairing (e.g. the diskless server, which arms its
                    // next-generation metadata here) see a checkpoint in progress
                    // when `wal.checkpoint` writes pages into the DB file. Called
                    // exactly once at this transition (never on `DriveCheckpoint`
                    // re-entry) since `on_checkpoint_start` is not idempotent.
                    self.storage.on_checkpoint_start()?;
                    *st = CompleteCheckpointState::DriveCheckpoint;
                }
                CompleteCheckpointState::DriveEarlyTruncate {
                    header_result,
                    checkpoint_result,
                } => {
                    return_if_io!(wal.truncate_wal(checkpoint_result, pager.get_sync_type()));
                    if let HeaderReadResult::Valid(header) = header_result {
                        self.storage.set_header(header.clone());
                    }
                    *st = CompleteCheckpointState::Start;
                    return Ok(IOResult::Done(()));
                }
                CompleteCheckpointState::DriveCheckpoint => {
                    // NOTE: uses `CheckpointMode::Truncate` to drive WAL backfill
                    // only; we still truncate the WAL explicitly below to preserve
                    // WAL-last ordering in recovery.
                    let checkpoint_result = return_if_io!(wal.checkpoint(
                        &pager,
                        CheckpointMode::Truncate {
                            upper_bound_inclusive: None,
                        },
                    ));
                    if !checkpoint_result.everything_backfilled() {
                        let err = LimboError::Corrupt(
                            "Unable to fully backfill committed WAL frames during MVCC recovery"
                                .to_string(),
                        );
                        // Close out the lifecycle opened in `ReadingHeader` so the
                        // start/end pairing stays balanced on this error path.
                        self.storage.on_checkpoint_end(Err(err.clone()))?;
                        return Err(err);
                    }
                    let need_db_sync = connection.get_sync_mode() != SyncMode::Off
                        && checkpoint_result.wal_checkpoint_backfilled > 0;
                    if need_db_sync {
                        let c = match pager
                            .db_file
                            .sync(Completion::new_sync(|_| {}), pager.get_sync_type())
                        {
                            Ok(c) => c,
                            Err(err) => {
                                self.storage.on_checkpoint_end(Err(err.clone()))?;
                                return Err(err);
                            }
                        };
                        *st = CompleteCheckpointState::AwaitDbFileSync {
                            completion: c,
                            checkpoint_result,
                        };
                    } else {
                        *st = CompleteCheckpointState::RetryHeader {
                            checkpoint_result,
                            retried_crc: false,
                            phase: RetryHeaderPhase::NeedUpdateHeader,
                        };
                    }
                }
                CompleteCheckpointState::AwaitDbFileSync {
                    completion,
                    checkpoint_result,
                } => {
                    if !completion.succeeded() {
                        let c = completion.clone();
                        io_yield_one!(c);
                    }
                    let checkpoint_result =
                        std::mem::replace(checkpoint_result, CheckpointResult::new(0, 0, 0));
                    *st = CompleteCheckpointState::RetryHeader {
                        checkpoint_result,
                        retried_crc: false,
                        phase: RetryHeaderPhase::NeedUpdateHeader,
                    };
                }
                CompleteCheckpointState::RetryHeader {
                    checkpoint_result,
                    retried_crc,
                    phase,
                } => match phase {
                    RetryHeaderPhase::NeedUpdateHeader => {
                        let c = match self.storage.update_header() {
                            Ok(c) => c,
                            Err(err) => {
                                self.storage.on_checkpoint_end(Err(err.clone()))?;
                                return Err(err);
                            }
                        };
                        *phase = RetryHeaderPhase::AwaitUpdateHeader(c);
                    }
                    RetryHeaderPhase::AwaitUpdateHeader(completion) => {
                        if !completion.succeeded() {
                            let c = completion.clone();
                            io_yield_one!(c);
                        }
                        if connection.get_sync_mode() != SyncMode::Off {
                            let c = match self.storage.sync(pager.get_sync_type()) {
                                Ok(c) => c,
                                Err(err) => {
                                    self.storage.on_checkpoint_end(Err(err.clone()))?;
                                    return Err(err);
                                }
                            };
                            *phase = RetryHeaderPhase::AwaitLogSync(c);
                        } else {
                            let file = self.get_logical_log_file();
                            *phase = RetryHeaderPhase::AwaitCrcCheck {
                                reader: Box::new(StreamingLogicalLogReader::new(file, None)),
                            };
                        }
                    }
                    RetryHeaderPhase::AwaitLogSync(completion) => {
                        if !completion.succeeded() {
                            let c = completion.clone();
                            io_yield_one!(c);
                        }
                        let file = self.get_logical_log_file();
                        *phase = RetryHeaderPhase::AwaitCrcCheck {
                            reader: Box::new(StreamingLogicalLogReader::new(file, None)),
                        };
                    }
                    RetryHeaderPhase::AwaitCrcCheck { reader } => {
                        let header_result = return_if_io!(reader.try_read_header_nonblock());
                        let crc_valid = matches!(header_result, HeaderReadResult::Valid(_));
                        if crc_valid {
                            let checkpoint_result = std::mem::replace(
                                checkpoint_result,
                                CheckpointResult::new(0, 0, 0),
                            );
                            *st = CompleteCheckpointState::DriveFinalTruncate { checkpoint_result };
                        } else {
                            if *retried_crc {
                                let err = LimboError::Corrupt(
                                    "Logical log header CRC mismatch after retry".to_string(),
                                );
                                self.storage.on_checkpoint_end(Err(err.clone()))?;
                                return Err(err);
                            }
                            *retried_crc = true;
                            *phase = RetryHeaderPhase::NeedUpdateHeader;
                        }
                    }
                },
                CompleteCheckpointState::DriveFinalTruncate { checkpoint_result } => {
                    match wal.truncate_wal(checkpoint_result, pager.get_sync_type()) {
                        Ok(IOResult::Done(())) => {
                            self.storage.on_checkpoint_end(Ok(checkpoint_result))?;
                        }
                        Ok(IOResult::IO(c)) => {
                            return Ok(IOResult::IO(c));
                        }
                        Err(err) => {
                            self.storage.on_checkpoint_end(Err(err.clone()))?;
                            return Err(err);
                        }
                    }
                    *st = CompleteCheckpointState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Build an `Arc<Schema>` from a set of sqlite_schema rows (keyed by rowid).
    /// Shared by recovery (replayed rows) and the checkpoint's snapshot-consistent
    /// `BuildLocalSchemaView` (btree + MVCC-delta merge at snapshot_ts).
    pub(crate) fn build_schema_from_rows(
        &self,
        connection: &Arc<Connection>,
        schema_rows: &HashMap<i64, ImmutableRecord>,
        preserved_table_valued_functions: &[Arc<crate::vtab::VirtualTable>],
    ) -> Result<Arc<Schema>> {
        let pager = connection.pager.load().clone();
        let cookie = self
            .global_header
            .read()
            .as_ref()
            .map(|header| header.schema_cookie.get())
            .unwrap_or(
                pager
                    .io
                    .block(|| pager.with_header(|header| header.schema_cookie))?
                    .get(),
            );
        let mut fresh = Schema::new();
        fresh.generated_columns_enabled = connection.db.experimental_generated_columns_enabled();
        fresh.schema_version = cookie;
        let mut from_sql_indexes = crate::alloc::vec![];
        let mut automatic_indices = HashMap::default();
        let mut dbsp_state_roots: HashMap<String, i64> = HashMap::default();
        let mut dbsp_state_index_roots: HashMap<String, i64> = HashMap::default();
        let mut materialized_view_info: HashMap<String, (String, i64)> = HashMap::default();
        let syms = connection.syms.read();
        let mv_store = connection.db.get_mv_store().clone();

        let mut sorted_rowids: Vec<i64> = schema_rows.keys().copied().collect();
        sorted_rowids.sort_unstable();
        for rowid in &sorted_rowids {
            let record = &schema_rows[rowid];
            let ty = match record.get_value_opt(0) {
                Some(ValueRef::Text(v)) => v.as_str(),
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema type must be text".to_string(),
                    ));
                }
            };
            let name = match record.get_value_opt(1) {
                Some(ValueRef::Text(v)) => v.as_str(),
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema name must be text".to_string(),
                    ));
                }
            };
            let table_name = match record.get_value_opt(2) {
                Some(ValueRef::Text(v)) => v.as_str(),
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema tbl_name must be text".to_string(),
                    ));
                }
            };
            let root_page = match record.get_value_opt(3) {
                Some(ValueRef::Numeric(Numeric::Integer(v))) => v,
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema root_page must be integer".to_string(),
                    ));
                }
            };
            let sql = match record.get_value_opt(4) {
                Some(ValueRef::Text(v)) => Some(v.as_str()),
                _ => None,
            };
            let attached_resolver = |alias: &str| -> Option<usize> {
                connection
                    .attached_databases()
                    .read()
                    .get_database_by_name(&crate::util::normalize_ident(alias))
                    .map(|(idx, _)| idx)
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
                &attached_resolver,
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
        Self::rehydrate_table_valued_functions(&mut fresh, preserved_table_valued_functions);

        Ok(Arc::new(fresh))
    }

    /// Replays committed logical-log frames into the in-memory MVCC store.
    /// Only frames with `commit_ts > persistent_tx_ts_max` (the durable replay
    /// boundary from the metadata table) are applied; earlier frames were
    /// already checkpointed. On success, reseeds the MVCC clock and sets the log
    /// writer offset so torn-tail bytes are overwritten. Returns true if any
    /// frames were replayed.
    pub fn maybe_recover_logical_log(
        &self,
        connection: &Arc<Connection>,
        st: &mut RecoverLogicalLogState,
    ) -> Result<IOResult<bool>> {
        loop {
            match st {
                RecoverLogicalLogState::Start => {
                    let file = self.get_logical_log_file();
                    let enc_ctx = self.storage.encryption_ctx();
                    let reader = Box::new(StreamingLogicalLogReader::new(file, enc_ctx));
                    let preserved_tvfs =
                        Self::capture_table_valued_functions(&connection.schema.read());
                    *st = RecoverLogicalLogState::ReadHeader {
                        reader,
                        preserved_tvfs,
                    };
                }
                RecoverLogicalLogState::ReadHeader { reader, .. } => {
                    let header = match return_if_io!(reader.try_read_header_nonblock()) {
                        HeaderReadResult::Valid(header) => Some(header),
                        HeaderReadResult::NoLog => None,
                        HeaderReadResult::Invalid => {
                            return Err(LimboError::Corrupt(
                                "Logical log header corrupt and no WAL recovery available"
                                    .to_string(),
                            ));
                        }
                    };
                    if let Some(header) = &header {
                        self.storage.set_header(header.clone());
                    }
                    let header_present = header.is_some();
                    let RecoverLogicalLogState::ReadHeader {
                        reader,
                        preserved_tvfs,
                    } = std::mem::take(st)
                    else {
                        unreachable!("state is ReadHeader");
                    };
                    *st = RecoverLogicalLogState::ReadTxTs {
                        reader,
                        preserved_tvfs,
                        header_present,
                        txts_st: ReadPersistentTxTsMaxState::default(),
                    };
                }
                RecoverLogicalLogState::ReadTxTs {
                    header_present,
                    txts_st,
                    ..
                } => {
                    let header_present = *header_present;
                    let persistent_tx_ts_max = if self.uses_durable_mvcc_metadata(connection) {
                        match return_if_io!(
                            self.try_read_persistent_tx_ts_max_nonblock(connection, txts_st)
                        ) {
                            Some(ts) => ts,
                            None if !header_present => 0,
                            None => {
                                return Err(LimboError::Corrupt(
                                    "Missing MVCC metadata table".to_string(),
                                ));
                            }
                        }
                    } else {
                        0
                    };
                    self.durable_txid_max
                        .store(persistent_tx_ts_max, Ordering::SeqCst);
                    self.clock.reset(persistent_tx_ts_max + 1);

                    if !header_present || self.get_logical_log_file().size()? <= LOG_HDR_SIZE as u64
                    {
                        *st = RecoverLogicalLogState::Done;
                        return Ok(IOResult::Done(false));
                    }
                    let RecoverLogicalLogState::ReadTxTs {
                        reader,
                        preserved_tvfs,
                        ..
                    } = std::mem::take(st)
                    else {
                        unreachable!("state is ReadTxTs");
                    };
                    *st = RecoverLogicalLogState::ReadCookie {
                        reader,
                        preserved_tvfs,
                        persistent_tx_ts_max,
                    };
                }
                RecoverLogicalLogState::ReadCookie { .. } => {
                    let fallback_cookie = if self.global_header.read().is_some() {
                        0
                    } else {
                        let pager = connection.pager.load().clone();
                        return_if_io!(pager.with_header(|header| header.schema_cookie)).get()
                    };
                    let RecoverLogicalLogState::ReadCookie {
                        reader,
                        preserved_tvfs,
                        persistent_tx_ts_max,
                    } = std::mem::take(st)
                    else {
                        unreachable!("state is ReadCookie");
                    };
                    let stmt = connection
                        .query(
                            "SELECT rowid, type, name, tbl_name, rootpage, sql FROM sqlite_schema",
                        )?
                        .map(Box::new);
                    *st = RecoverLogicalLogState::QuerySchema {
                        reader,
                        preserved_tvfs,
                        persistent_tx_ts_max,
                        cookie: fallback_cookie,
                        stmt,
                        schema_rows: HashMap::default(),
                    };
                }
                RecoverLogicalLogState::QuerySchema {
                    stmt, schema_rows, ..
                } => {
                    if let Some(stmt) = stmt {
                        return_if_io!(stmt.run_with_row_callback_nonblock(|row| {
                            let rowid = row.get::<i64>(0)?;
                            let values = (1..=5)
                                .map(|i| row.get_value(i).clone())
                                .collect::<Vec<_>>();
                            schema_rows.insert(
                                rowid,
                                ImmutableRecord::from_values(&values, values.len())?,
                            );
                            Ok(())
                        }));
                    }
                    let RecoverLogicalLogState::QuerySchema {
                        reader,
                        preserved_tvfs,
                        persistent_tx_ts_max,
                        cookie,
                        schema_rows,
                        ..
                    } = std::mem::take(st)
                    else {
                        unreachable!("state is QuerySchema");
                    };
                    let current_schema = connection.schema.read().clone();
                    *st = RecoverLogicalLogState::Replay {
                        ctx: Box::new(RecoverCtx {
                            reader,
                            preserved_table_valued_functions: preserved_tvfs,
                            cookie,
                            persistent_tx_ts_max,
                            replay_cutoff_ts: persistent_tx_ts_max,
                            max_commit_ts_seen: persistent_tx_ts_max,
                            schema_rows,
                            dropped_root_pages: HashSet::default(),
                            current_schema,
                            index_infos: HashMap::default(),
                        }),
                    };
                }
                RecoverLogicalLogState::Replay { ctx } => {
                    loop {
                        let Some(frame) = return_if_io!(ctx.reader.next_frame()) else {
                            let recovered_offset = ctx.reader.last_valid_offset() as u64;
                            let recovered_running_crc = ctx.reader.running_crc();
                            self.storage.restore_logical_log_state_after_recovery(
                                recovered_offset,
                                recovered_running_crc,
                            );
                            break;
                        };
                        self.recover_process_frame(connection, ctx, frame)?;
                    }
                    let max_commit_ts_seen = ctx.max_commit_ts_seen;
                    let persistent_tx_ts_max = ctx.persistent_tx_ts_max;
                    let dropped_root_pages = std::mem::take(&mut ctx.dropped_root_pages);
                    assert!(
                        max_commit_ts_seen >= persistent_tx_ts_max,
                        "replay clock would rewind below metadata boundary: max_commit_ts_seen={max_commit_ts_seen} persistent_tx_ts_max={persistent_tx_ts_max}"
                    );
                    connection.with_schema_mut(|schema| {
                        schema.dropped_root_pages = dropped_root_pages;
                    });
                    if let Some(header) = self.global_header.read().as_ref() {
                        connection.with_schema_mut(|schema| {
                            schema.schema_version = header.schema_cookie.get();
                        });
                    }
                    *connection.db.schema.lock() = connection.schema.read().clone();
                    self.clock.reset(max_commit_ts_seen + 1);
                    self.last_committed_tx_ts
                        .store(max_commit_ts_seen, Ordering::SeqCst);
                    *st = RecoverLogicalLogState::Done;
                    return Ok(IOResult::Done(true));
                }
                RecoverLogicalLogState::Done => {
                    return Ok(IOResult::Done(false));
                }
            }
        }
    }

    /// Replay a single committed logical-log transaction frame into the MVCC
    /// store. Fully synchronous (the only recovery IO is reading the next frame,
    /// driven by the caller); operates on accumulators borrowed from `ctx`.
    fn recover_process_frame(
        &self,
        connection: &Arc<Connection>,
        ctx: &mut RecoverCtx,
        frame: Vec<ParsedOp>,
    ) -> Result<()> {
        let passive = connection.experimental_mvcc_passive_checkpoint_enabled();
        let mut max_commit_ts_seen = ctx.max_commit_ts_seen;
        let replay_cutoff_ts = ctx.replay_cutoff_ts;
        let cookie = ctx.cookie;
        let mut schema_rows = std::mem::take(&mut ctx.schema_rows);
        let mut dropped_root_pages = std::mem::take(&mut ctx.dropped_root_pages);
        let mut current_schema = ctx.current_schema.clone();
        let mut index_infos = std::mem::take(&mut ctx.index_infos);

        let install_schema = |schema: Arc<Schema>| {
            *connection.schema.write() = schema.clone();
            *connection.db.schema.lock() = schema;
        };

        let root_page_for_index = |index_id: MVTableId| -> i64 {
            self.table_id_to_rootpage
                .get(&index_id)
                .and_then(|entry| *entry.value())
                .map(|value| value as i64)
                .unwrap_or_else(|| i64::from(index_id))
        };

        let find_index_info = |schema: &Schema, root_page: i64| -> Result<Option<Arc<IndexInfo>>> {
            schema
                .indexes
                .values()
                .flatten()
                .find(|idx| idx.root_page == root_page)
                .map(|idx| IndexInfo::new_from_index(idx.as_ref()).map(Arc::new))
                .transpose()
        };

        let schema_has_index_root = |schema: &Schema, root_page: i64| -> bool {
            schema
                .indexes
                .values()
                .flatten()
                .any(|idx| idx.root_page == root_page)
        };

        let parsed_op_commit_ts = |op: &ParsedOp| match op {
            ParsedOp::UpsertTable { commit_ts, .. }
            | ParsedOp::DeleteTable { commit_ts, .. }
            | ParsedOp::UpsertIndex { commit_ts, .. }
            | ParsedOp::DeleteIndex { commit_ts, .. }
            | ParsedOp::UpdateHeader { commit_ts, .. } => *commit_ts,
        };
        'frame: {
            let frame_commit_ts = parsed_op_commit_ts(
                frame
                    .first()
                    .expect("next_frame should not return an empty frame"),
            );
            max_commit_ts_seen = max_commit_ts_seen.max(frame_commit_ts);
            if frame_commit_ts <= replay_cutoff_ts {
                break 'frame;
            }

            // Work out what sqlite_schema will look like after this transaction,
            // before applying any row/index changes from the transaction.
            //
            // Why this is necessary:
            // - CREATE INDEX can log index-entry inserts in the same frame as the
            //   sqlite_schema insert for the new index. Those index-entry inserts
            //   need the post-frame schema.
            // - DROP INDEX can log DELETE_INDEX entries for b-tree index entries
            //   that existed before the transaction. Those deletes still need the
            //   pre-frame schema, because the final schema no longer has idx.
            // - ALTER TABLE can delete and reinsert the same table's sqlite_schema
            //   row while also logging table/index DML. Installing the schema after
            //   only the delete would create an impossible in-between schema:
            //   sqlite_schema may still contain idx while the table row for t is
            //   temporarily absent.
            //
            // So recovery stages sqlite_schema into `schema_rows_after`, builds a
            // post-frame Schema from that staged map, and keeps the installed
            // `current_schema` unchanged until every op in the frame has replayed.
            // Most transaction frames do not change sqlite_schema, so clone the
            // schema row map only if this frame actually writes sqlite_schema.
            let mut schema_rows_after: Option<HashMap<i64, ImmutableRecord>> = None;
            for parsed_op in &frame {
                match parsed_op {
                    ParsedOp::UpsertTable {
                        rowid,
                        record_bytes,
                        ..
                    } if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID => {
                        let schema_rows_after =
                            schema_rows_after.get_or_insert_with(|| schema_rows.clone());
                        let record = ImmutableRecord::from_bin_record(record_bytes.clone());
                        if record.column_count() < 5 {
                            return Err(LimboError::Corrupt(format!(
                                "sqlite_schema row must have at least 5 columns, got {}",
                                record.column_count()
                            )));
                        }
                        schema_rows_after.insert(rowid.row_id.to_int_or_panic(), record);
                    }
                    ParsedOp::DeleteTable { rowid, .. }
                        if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID =>
                    {
                        let schema_rows_after =
                            schema_rows_after.get_or_insert_with(|| schema_rows.clone());
                        schema_rows_after.remove(&rowid.row_id.to_int_or_panic());
                    }
                    _ => {}
                }
            }
            // schema_rows_after is Some if the frame changes the schema
            let schema_rows_after = schema_rows_after;

            let schema_after = match schema_rows_after.as_ref() {
                Some(schema_rows_after) => Some(self.recover_build_schema(
                    connection,
                    schema_rows_after,
                    cookie,
                    &ctx.preserved_table_valued_functions,
                )?),
                None => None,
            };

            if schema_rows_after.is_some() {
                // Cached IndexInfo values are tied to a specific schema. Clear
                // before decoding a schema-changing frame so this frame chooses
                // from its own before/after schema pair.
                index_infos.clear();
            }

            {
                let should_skip_index_op = |parsed_op: &ParsedOp| -> bool {
                    let Some(schema_after) = schema_after.as_ref() else {
                        return false;
                    };

                    match parsed_op {
                        ParsedOp::UpsertIndex { table_id, .. } => {
                            let root_page = root_page_for_index(*table_id);
                            !schema_has_index_root(schema_after, root_page)
                        }
                        ParsedOp::DeleteIndex { table_id, .. } => {
                            let root_page = root_page_for_index(*table_id);
                            !schema_has_index_root(&current_schema, root_page)
                        }
                        _ => false,
                    }
                };

                let mut get_index_info = |index_id: MVTableId,
                                          op_kind: IndexOpKind|
                 -> Result<Arc<IndexInfo>> {
                    if let Some(index_info) = index_infos.get(&(index_id, op_kind)) {
                        return Ok(index_info.clone());
                    }

                    let root_page = root_page_for_index(index_id);
                    let before = find_index_info(&current_schema, root_page)?;
                    let after = schema_after
                        .as_ref()
                        .map(|schema| find_index_info(schema, root_page))
                        .transpose()?
                        .flatten();

                    // The logical log tells us whether an index entry is being
                    // inserted or deleted, but it stores only encoded key bytes
                    // plus the index root page. The recovery loop below skips
                    // index ops that cannot affect the final state of this
                    // frame before those bytes are decoded. For the remaining
                    // ops, pick the schema view that owns the entry at the
                    // frame boundary:
                    //
                    // - UPSERT_INDEX writes an entry that survives after the
                    //   transaction. In a schema-changing frame, the index must
                    //   exist in the post-frame schema.
                    // - DELETE_INDEX removes an entry that existed before the
                    //   transaction. In a schema-changing frame, the index must
                    //   exist in the pre-frame schema.
                    // - If the frame does not change schema, `current_schema` is
                    //   both the before and after schema.
                    let index_info = match op_kind {
                        IndexOpKind::Upsert if schema_after.is_some() => after,
                        IndexOpKind::Delete if schema_after.is_some() => before,
                        IndexOpKind::Upsert | IndexOpKind::Delete => before,
                    }
                    .ok_or_else(|| {
                        let expected_schema = match op_kind {
                            IndexOpKind::Upsert if schema_after.is_some() => "post-frame",
                            IndexOpKind::Delete if schema_after.is_some() => "pre-frame",
                            IndexOpKind::Upsert | IndexOpKind::Delete => "current",
                        };
                        LimboError::InternalError(format!(
                            "Index with root page {root_page} not found in {expected_schema} schema while recovering logical log",
                        ))
                    })?;
                    index_infos.insert((index_id, op_kind), index_info.clone());
                    Ok(index_info)
                };

                for parsed_op in frame {
                    // Some index writes are real while the transaction is
                    // running, but have no meaning at either durable boundary.
                    //
                    // Example: UPDATE a row so it writes a new entry into
                    // idx_old, then DROP INDEX idx_old before COMMIT. The
                    // UPSERT_INDEX for idx_old is not part of the database
                    // after the transaction, and the post-frame schema no
                    // longer has CREATE INDEX text for idx_old. Decoding it
                    // against the pre-frame schema would preserve an index
                    // entry for an index that was dropped. Decoding it against
                    // the post-frame schema is impossible. The correct action
                    // is to skip it.
                    //
                    // The opposite case is a DELETE_INDEX for an index created
                    // earlier in the same frame. There was no pre-frame index
                    // entry in the database file, so that delete also has no
                    // durable work to do.
                    if should_skip_index_op(&parsed_op) {
                        continue;
                    }

                    let next_rec = ctx
                        .reader
                        .parsed_op_to_streaming(parsed_op, &mut get_index_info)?;

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
                                            "Expected at least 5 columns in sqlite_schema"
                                                .to_string(),
                                        ));
                                    }
                                };
                                let ValueRef::Numeric(crate::numeric::Numeric::Integer(root_page)) =
                                    val
                                else {
                                    panic!("Expected integer value for root page, got {val:?}");
                                };
                                let sql = match record.get_value_opt(4) {
                                    Some(ValueRef::Text(v)) => Some(v.as_str()),
                                    _ => None,
                                };
                                let is_virtual_table = row_type == "table"
                                    && sql.is_some_and(|sql| {
                                        contains_ignore_ascii_case!(
                                            sql.as_bytes(),
                                            b"create virtual"
                                        )
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
                                        if let Some(entry) =
                                            self.table_id_to_rootpage.get(&table_id)
                                        {
                                            if let Some(value) = *entry.value() {
                                                panic!("Logical log contains an insertion of a sqlite_schema record that has both a negative root page and a positive root page: {root_page} & {value}");
                                            }
                                        }
                                        self.insert_table_id_to_rootpage(table_id, None);
                                    } else {
                                        dropped_root_pages.remove(&root_page);
                                        let table_id = self.get_table_id_from_root_page(root_page);
                                        let Some(entry) = self.table_id_to_rootpage.get(&table_id)
                                        else {
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
                            } else if self.table_id_to_rootpage.get(&rowid.table_id).is_none() {
                                // Data row references a table_id not yet in the map. This can happen
                                // with logs written before the schema-first serialization fix: in a
                                // same-transaction CREATE TABLE + INSERT + DROP TABLE, data rows were
                                // serialized before the schema INSERT that registers the table_id.
                                // The schema INSERT (or DELETE) for this table will follow later in
                                // this transaction frame, so we register the table_id now.
                                self.insert_table_id_to_rootpage(rowid.table_id, None);
                            }

                            let version_id = self.get_version_id();
                            let row_version = RowVersion {
                                id: version_id,
                                begin: crate::mvcc::database::PackedTs::pack(Some(
                                    TxTimestampOrID::Timestamp(commit_ts),
                                )),
                                end: crate::mvcc::database::PackedTs::pack(None),
                                row: row.clone(),
                                btree_resident,
                            };
                            {
                                let versions = self.rows.get_or_insert_with(rowid.clone(), || {
                                    Arc::new(RwLock::new(Vec::new()))
                                });
                                let mut versions = versions.value().write();
                                self.insert_version_raw(&mut versions, row_version);
                            }
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
                            if self.table_id_to_rootpage.get(&rowid.table_id).is_none() {
                                // See comment in UpsertTableRow: old logs may have data rows
                                // serialized before the schema INSERT that registers the table_id.
                                self.insert_table_id_to_rootpage(rowid.table_id, None);
                            }
                            let tombstone_row = if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
                                let rowid_int = rowid.row_id.to_int_or_panic();
                                if let Some(record) = schema_rows.get(&rowid_int) {
                                    // Preserve the pre-delete sqlite_schema record in recovered
                                    // tombstones so checkpoint can still recover B-tree identity.
                                    Row::new_table_row(
                                        rowid.clone(),
                                        record.as_blob().clone(),
                                        record.column_count(),
                                    )
                                } else {
                                    Row::new_table_row(rowid.clone(), Vec::new(), 0)
                                }
                            } else {
                                Row::new_table_row(rowid.clone(), Vec::new(), 0)
                            };
                            if let Some(versions) = self.rows.get(&rowid) {
                                // Row exists in memory — try to find the current (non-ended) version
                                // that was committed before this delete, and mark it as ended. If no
                                // such version exists (e.g. it was already GC'd or this is a B-tree
                                // resident row not yet in memory), insert a tombstone instead.
                                let mut versions = versions.value().write();
                                if let Some(existing) = versions.iter_mut().rev().find(|rv| {
                            rv.end().is_none()
                                && matches!(rv.begin(), Some(TxTimestampOrID::Timestamp(b)) if b < commit_ts)
                        }) {
                            existing.set_end(Some(TxTimestampOrID::Timestamp(commit_ts)));
                        } else {
                            let version_id = self.get_version_id();
                            let row_version = RowVersion {
                                id: version_id,
                                begin: crate::mvcc::database::PackedTs::pack(None),
                                end: crate::mvcc::database::PackedTs::pack(Some(TxTimestampOrID::Timestamp(commit_ts))),
                                row: tombstone_row.clone(),
                                btree_resident: if passive { true } else { btree_resident },
                            };
                            self.insert_version_raw(&mut versions, row_version);
                        }
                            } else {
                                let version_id = self.get_version_id();
                                let row_version = RowVersion {
                                    id: version_id,
                                    begin: crate::mvcc::database::PackedTs::pack(None),
                                    end: crate::mvcc::database::PackedTs::pack(Some(
                                        TxTimestampOrID::Timestamp(commit_ts),
                                    )),
                                    row: tombstone_row,
                                    btree_resident: if passive { true } else { btree_resident },
                                };
                                let versions = self.rows.get_or_insert_with(rowid.clone(), || {
                                    Arc::new(RwLock::new(Vec::new()))
                                });
                                let mut versions = versions.value().write();
                                self.insert_version_raw(&mut versions, row_version);
                            }
                            if rowid.table_id == SQLITE_SCHEMA_MVCC_TABLE_ID {
                                let rowid_int = rowid.row_id.to_int_or_panic();
                                let Some(record) = schema_rows.get(&rowid_int) else {
                                    // this can happen if a row in sqlite_schema was inserted and then
                                    // deleted in the same transaction (ex: a CREATE TABLE followed by a DROP TABLE)
                                    continue;
                                };
                                if record.column_count() < 5 {
                                    return Err(LimboError::Corrupt(format!(
                                        "sqlite_schema row must have at least 5 columns, got {}",
                                        record.column_count()
                                    )));
                                }
                                let (
                                    ValueRef::Text(row_type),
                                    ValueRef::Numeric(Numeric::Integer(root_page)),
                                ) = record.get_two_values(0, 3)?
                                else {
                                    return Err(LimboError::Corrupt(
                                        "sqlite_schema type and root_page must be text and integer"
                                            .to_string(),
                                    ));
                                };
                                let row_type = row_type.as_str();
                                if (row_type == "table" || row_type == "index") && root_page > 0 {
                                    dropped_root_pages.insert(root_page);
                                }
                                schema_rows.remove(&rowid_int);
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
                                begin: crate::mvcc::database::PackedTs::pack(Some(
                                    TxTimestampOrID::Timestamp(commit_ts),
                                )),
                                end: crate::mvcc::database::PackedTs::pack(None),
                                row: row.clone(),
                                btree_resident,
                            };
                            let RowKey::Record(sortable_key) = rowid.row_id.clone() else {
                                panic!("Index writes must be to a record");
                            };
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
                                if let Some(versions) = index_map.value().get(&sortable_key) {
                                    let mut versions = versions.value().write();
                                    if let Some(existing) = versions.iter_mut().rev().find(|rv| {
                                rv.end().is_none()
                                    && matches!(rv.begin(), Some(TxTimestampOrID::Timestamp(b)) if b < commit_ts)
                            }) {
                                existing.set_end(Some(TxTimestampOrID::Timestamp(commit_ts)));
                                continue;
                            }
                                }
                            }
                            let version_id = self.get_version_id();
                            let row_version = RowVersion {
                                id: version_id,
                                begin: crate::mvcc::database::PackedTs::pack(None),
                                end: crate::mvcc::database::PackedTs::pack(Some(
                                    TxTimestampOrID::Timestamp(commit_ts),
                                )),
                                row: row.clone(),
                                btree_resident: if passive { true } else { btree_resident },
                            };
                            self.insert_index_version(rowid.table_id, sortable_key, row_version);
                        }
                        StreamingResult::UpdateHeader { header, commit_ts } => {
                            max_commit_ts_seen = max_commit_ts_seen.max(commit_ts);
                            if commit_ts <= replay_cutoff_ts {
                                continue;
                            }
                            // Recovery applies only post-boundary header ops; the same value is later
                            // staged to pager page-1 during checkpoint.
                            self.global_header.write().replace(header);
                        }
                        StreamingResult::Eof => {
                            unreachable!("next_frame does not return EOF records");
                        }
                    }
                }
            }

            if schema_rows_after.is_some() {
                // Now that all table and index ops from this transaction have
                // been replayed, publish the frame's final schema. No later index
                // op from this frame can observe a half-applied sqlite_schema.
                let schema_rows_after =
                    schema_rows_after.expect("schema_rows_after must exist when schema changed");
                let schema_after = schema_after
                    .expect("schema_after must exist when frame_changes_schema is true");
                schema_rows = schema_rows_after;
                install_schema(schema_after.clone());
                current_schema = schema_after;
                // The frame may have decoded DROP INDEX entries using the
                // pre-frame schema. Do not carry those IndexInfo values into
                // later frames after current_schema has changed.
                index_infos.clear();
            }
        }

        ctx.max_commit_ts_seen = max_commit_ts_seen;
        ctx.schema_rows = schema_rows;
        ctx.dropped_root_pages = dropped_root_pages;
        ctx.current_schema = current_schema;
        ctx.index_infos = index_infos;
        Ok(())
    }

    /// Build a fresh `Schema` from the recovered `sqlite_schema` rows. Sync: the
    /// schema cookie comes from `global_header` (in-memory) or `fallback_cookie`
    /// (pre-read by the caller); no IO here.
    fn recover_build_schema(
        &self,
        connection: &Arc<Connection>,
        schema_rows: &HashMap<i64, ImmutableRecord>,
        fallback_cookie: u32,
        preserved_table_valued_functions: &[Arc<crate::vtab::VirtualTable>],
    ) -> Result<Arc<Schema>> {
        let cookie = self
            .global_header
            .read()
            .as_ref()
            .map(|header| header.schema_cookie.get())
            .unwrap_or(fallback_cookie);
        let mut fresh = Schema::new();
        fresh.generated_columns_enabled = connection.db.experimental_generated_columns_enabled();
        fresh.schema_version = cookie;
        let mut from_sql_indexes =
            crate::alloc::Vec::try_with_capacity_ext(10).expect("TODO: fallible allocations");
        let mut automatic_indices: HashMap<String, crate::alloc::Vec<(String, i64)>> =
            HashMap::default();
        let mut dbsp_state_roots: HashMap<String, i64> = HashMap::default();
        let mut dbsp_state_index_roots: HashMap<String, i64> = HashMap::default();
        let mut materialized_view_info: HashMap<String, (String, i64)> = HashMap::default();
        let syms = connection.syms.read();
        let mv_store = connection.db.get_mv_store().clone();

        let mut sorted_rowids: Vec<i64> = schema_rows.keys().copied().collect();
        sorted_rowids.sort_unstable();
        for rowid in &sorted_rowids {
            let record = &schema_rows[rowid];
            let ty = match record.get_value_opt(0) {
                Some(ValueRef::Text(v)) => v.as_str(),
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema type must be text".to_string(),
                    ));
                }
            };
            let name = match record.get_value_opt(1) {
                Some(ValueRef::Text(v)) => v.as_str(),
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema name must be text".to_string(),
                    ));
                }
            };
            let table_name = match record.get_value_opt(2) {
                Some(ValueRef::Text(v)) => v.as_str(),
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema tbl_name must be text".to_string(),
                    ));
                }
            };
            let root_page = match record.get_value_opt(3) {
                Some(ValueRef::Numeric(Numeric::Integer(v))) => v,
                _ => {
                    return Err(LimboError::Corrupt(
                        "sqlite_schema root_page must be integer".to_string(),
                    ));
                }
            };
            let sql = match record.get_value_opt(4) {
                Some(ValueRef::Text(v)) => Some(v.as_str()),
                _ => None,
            };
            let attached_resolver = |alias: &str| -> Option<usize> {
                connection
                    .attached_databases()
                    .read()
                    .get_database_by_name(&crate::util::normalize_ident(alias))
                    .map(|(idx, _)| idx)
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
                &attached_resolver,
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
        Self::rehydrate_table_valued_functions(&mut fresh, preserved_table_valued_functions);

        Ok(Arc::new(fresh))
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
                max_rowid: AtomicI64::new(0),
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

fn rollback_row_version(tx_id: u64, rv: &mut RowVersion) {
    if rv.begin() == Some(TxTimestampOrID::TxID(tx_id)) {
        // If the transaction has aborted,
        // it marks all its new versions as garbage and sets their Begin
        // and End timestamps to infinity to make them invisible
        // See section 2.4: https://www.cs.cmu.edu/~15721-f24/papers/Hekaton.pdf
        rv.set_begin(None);
        rv.set_end(None);
    } else if rv.end() == Some(TxTimestampOrID::TxID(tx_id)) {
        // undo deletions by this transaction
        rv.set_end(None);
    }
}

impl RowidAllocator {
    /// Lock-free rowid allocation via atomic CAS.
    /// Returns None only when at i64::MAX (triggers random fallback).
    /// Returns Some((new_rowid, prev_rowid)) where prev_rowid is None if table was empty.
    pub fn get_next_rowid(&self) -> Option<(i64, Option<i64>)> {
        loop {
            let cur = self.max_rowid.load(Ordering::SeqCst);
            if cur == i64::MAX {
                tracing::trace!("get_next_rowid(max)");
                return None;
            }
            let next = cur + 1;
            if self
                .max_rowid
                .compare_exchange(cur, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let prev = if cur == 0 { None } else { Some(cur) };
                tracing::trace!("get_next_rowid({next})");
                return Some((next, prev));
            }
        }
    }

    /// Bump the counter to at least `rowid`. Used for user-specified rowids
    /// (e.g. INSERT INTO t(rowid,...) VALUES(1000,...)).
    pub fn insert_row_id_maybe_update(&self, rowid: i64) {
        loop {
            let cur = self.max_rowid.load(Ordering::SeqCst);
            if rowid <= cur {
                return;
            }
            if self
                .max_rowid
                .compare_exchange(cur, rowid, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return;
            }
        }
    }

    pub fn is_uninitialized(&self) -> bool {
        !self.initialized.load(Ordering::SeqCst)
    }

    /// Initialize from btree max. Called once per table, under lock.
    pub fn initialize(&self, rowid: Option<i64>) {
        tracing::trace!("initialize({rowid:?})");
        let _ = self
            .max_rowid
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |cur| {
                let next = match rowid {
                    // max_rowid starts at 0, but a B-tree whose largest rowid is
                    // negative still needs to seed automatic allocation from that value.
                    Some(rowid) if cur == 0 => rowid,
                    Some(rowid) => cur.max(rowid),
                    None => cur,
                };
                (next != cur).then_some(next)
            });
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
fn is_write_write_conflict<A: ConcurrentAllocator>(
    txs: &SkipMap<TxID, Transaction, BasicComparator, A>,
    finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    tx: &Transaction,
    rv: &RowVersion,
) -> bool {
    match rv.end() {
        Some(TxTimestampOrID::TxID(rv_end)) => {
            if rv_end == tx.tx_id {
                return false;
            }
            match lookup_tx_state(txs, finalized_tx_states, rv_end) {
                Some(TransactionState::Aborted) | Some(TransactionState::Terminated) => false,
                Some(TransactionState::Active)
                | Some(TransactionState::Preparing(_))
                | Some(TransactionState::Committed(_)) => true,
                None => {
                    tracing::debug!(
                        "is_write_write_conflict: missing tx {} for row version {:?}; treating as conflict",
                        rv_end,
                        rv
                    );
                    true
                }
            }
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
    /// Construct a row version. `begin`/`end` are bit-packed internally.
    pub fn new(
        id: u64,
        begin: Option<TxTimestampOrID>,
        end: Option<TxTimestampOrID>,
        row: Row,
        btree_resident: bool,
    ) -> Self {
        Self {
            id,
            begin: crate::mvcc::database::PackedTs::pack(begin),
            end: crate::mvcc::database::PackedTs::pack(end),
            row,
            btree_resident,
        }
    }

    /// The begin timestamp/tx-id, or `None`.
    #[inline]
    pub fn begin(&self) -> Option<TxTimestampOrID> {
        self.begin.unpack()
    }

    /// The end timestamp/tx-id, or `None`.
    #[inline]
    pub fn end(&self) -> Option<TxTimestampOrID> {
        self.end.unpack()
    }

    #[inline]
    pub fn set_begin(&mut self, value: Option<TxTimestampOrID>) {
        self.begin = crate::mvcc::database::PackedTs::pack(value);
    }

    #[inline]
    pub fn set_end(&mut self, value: Option<TxTimestampOrID>) {
        self.end = crate::mvcc::database::PackedTs::pack(value);
    }

    /// A row is visible to a transaction if:
    /// * Begin is visible to the transaction
    /// * End timestamp is not applicable yet, meaning deletion of row is not visible to this transaction
    fn is_visible_to<A: ConcurrentAllocator>(
        &self,
        tx: &Transaction,
        txs: &SkipMap<TxID, Transaction, BasicComparator, A>,
        finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    ) -> bool {
        is_begin_visible(txs, finalized_tx_states, tx, self)
            && is_end_visible(txs, finalized_tx_states, tx, self)
    }

    /// Check if this version indicates the B-tree row has been modified (updated or deleted).
    ///
    /// A version is "relevant" to a transaction if:
    /// 1. The version is fully visible (begin visible AND end visible), OR
    /// 2. The version has an end timestamp that indicates the row was deleted before/at the transaction's begin, OR
    /// 3. The current transaction itself has deleted/updated this row (end = current tx_id)
    ///
    /// This is used by dual-cursor to determine if a B-tree row should be shown or hidden.
    fn is_btree_invalidating_version<A: ConcurrentAllocator>(
        &self,
        tx: &Transaction,
        txs: &SkipMap<TxID, Transaction, BasicComparator, A>,
        finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    ) -> bool {
        // If the version is fully visible, it invalidates the B-tree
        if self.is_visible_to(tx, txs, finalized_tx_states) {
            return true;
        }

        // Check if this version represents a deletion/update that affects us
        match self.end() {
            Some(TxTimestampOrID::Timestamp(end_ts)) => {
                // Row was deleted at end_ts. If we started after end_ts, we shouldn't see it
                turso_assert!(
                    tx.begin_ts != end_ts,
                    "begin_ts and committed end_ts cannot be equal: txn timestamps are strictly monotonic"
                );
                tx.begin_ts > end_ts
            }
            Some(TxTimestampOrID::TxID(end_tx_id)) => {
                // Row is being deleted/updated by another transaction.
                // Consult the deleting tx's state so we don't race with the
                // post-commit rewrite that turns TxID(W) into Timestamp(W.end_ts).
                if end_tx_id == tx.tx_id {
                    return true;
                }
                match lookup_tx_state(txs, finalized_tx_states, end_tx_id) {
                    Some(TransactionState::Committed(committed_ts)) => {
                        // Same predicate as the Timestamp arm above.
                        tx.begin_ts > committed_ts
                    }
                    Some(TransactionState::Preparing(end_ts)) => {
                        // Hekaton speculative read: treat as if W will commit at
                        // its prepared end_ts. When we speculatively invalidate
                        // the B-tree row, register a commit dependency on W —
                        // for tombstones (begin=None) we are the only place that
                        // decides this, since `is_visible_to` short-circuits at
                        // `is_begin_visible` and never calls `is_end_visible`.
                        // If W aborts, we must cascade-abort to avoid letting
                        // the reader observe the row reappear.
                        let speculatively_invalidated = tx.begin_ts > end_ts;
                        if speculatively_invalidated {
                            register_commit_dependency(txs, tx, end_tx_id);
                        }
                        speculatively_invalidated
                    }
                    Some(TransactionState::Active) => false,
                    Some(TransactionState::Aborted) | Some(TransactionState::Terminated) => false,
                    None => false,
                }
            }
            None => false,
        }
    }
}

/// Hekaton Section 2.7 — register-and-report protocol:
/// "To take a commit dependency on a transaction T2, T1 increments its
/// CommitDepCounter and adds its transaction ID to T2's CommitDepSet."
///
/// The lock on `commit_dep_set` serializes with the drain in commit/abort
/// resolution, preventing the race where we push an entry after the drain.
fn register_commit_dependency<A: ConcurrentAllocator>(
    txs: &SkipMap<TxID, Transaction, BasicComparator, A>,
    dependent_tx: &Transaction,
    depended_on_tx_id: TxID,
) {
    turso_assert!(
        dependent_tx.tx_id != depended_on_tx_id,
        "transaction cannot depend on itself"
    );
    let Some(depended_on) = txs.get(&depended_on_tx_id) else {
        // Transaction was already committed and removed from the map
        // (CommitEnd calls remove_tx after setting Committed and draining
        // CommitDepSet). Dependency is trivially resolved.
        return;
    };
    let depended_on = depended_on.value();

    // Hold lock while checking state to serialize with the drain in
    // commit/abort postprocessing.
    let mut dep_set = depended_on.commit_dep_set.lock();
    match depended_on.state.load() {
        TransactionState::Preparing(_) => {
            // Increment counter BEFORE inserting into dep_set and BEFORE dropping
            // the lock. This prevents underflow: if we inserted first and
            // released the lock, the depended-on tx could drain the dep_set
            // and call fetch_sub before we increment, wrapping the counter
            // from 0 to u64::MAX. Only increment on first insertion (dedup).
            if dep_set.insert(dependent_tx.tx_id) {
                dependent_tx
                    .commit_dep_counter
                    .fetch_add(1, Ordering::AcqRel);
            }
            drop(dep_set);
            tracing::trace!(
                "register_commit_dependency: tx {} depends on tx {}",
                dependent_tx.tx_id,
                depended_on_tx_id
            );
        }
        TransactionState::Active => {
            turso_assert!(false, "a txn found dependent on active txn");
        }
        TransactionState::Committed(_) => {
            // Already committed — dependency trivially resolved.
        }
        TransactionState::Aborted | TransactionState::Terminated => {
            // Already aborted — cascade abort to dependent.
            drop(dep_set);
            dependent_tx.abort_now.store(true, Ordering::Release);
            tracing::trace!(
                "register_commit_dependency: tx {} must abort (dep tx {} aborted)",
                dependent_tx.tx_id,
                depended_on_tx_id
            );
        }
    }
}

fn lookup_tx_state<A: ConcurrentAllocator>(
    txs: &SkipMap<TxID, Transaction, BasicComparator, A>,
    finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    tx_id: TxID,
) -> Option<TransactionState> {
    txs.get(&tx_id)
        .map(|entry| entry.value().state.load())
        .or_else(|| finalized_tx_states.get(&tx_id).map(|entry| *entry.value()))
}

fn lookup_finalized_tx_state<A: ConcurrentAllocator>(
    finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    tx_id: TxID,
) -> Option<TransactionState> {
    finalized_tx_states.get(&tx_id).map(|entry| {
        let state = *entry.value();
        turso_assert!(
            !matches!(
                state,
                TransactionState::Active | TransactionState::Preparing(_)
            ),
            "finalized_tx_states contains non-final state for tx {tx_id}: {state:?}"
        );
        state
    })
}

fn is_begin_visible<A: ConcurrentAllocator>(
    txs: &SkipMap<TxID, Transaction, BasicComparator, A>,
    finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    tx: &Transaction,
    rv: &RowVersion,
) -> bool {
    match rv.begin() {
        Some(TxTimestampOrID::Timestamp(rv_begin_ts)) => {
            turso_assert!(
                tx.begin_ts != rv_begin_ts,
                "begin_ts and committed rv_begin_ts cannot be equal: txn timestamps are strictly monotonic"
            );
            tx.begin_ts > rv_begin_ts
        }
        Some(TxTimestampOrID::TxID(rv_begin)) => {
            let visible = match txs.get(&rv_begin) {
                Some(tb_entry) => {
                    let tb = tb_entry.value();
                    let visible = match tb.state.load() {
                        TransactionState::Active => tx.tx_id == tb.tx_id && rv.end().is_none(),
                        TransactionState::Preparing(end_ts) => {
                            // Hekaton Table 1 / Section 2.5: speculative read of TB.
                            // If begin_ts > end_ts, the version would be visible once TB
                            // commits. Speculatively return true and register a dependency.
                            // Fixes partial commit visibility (Bug #8).
                            turso_assert!(
                                tx.tx_id != tb.tx_id,
                                "a txn cannot read its own row versions during prepare"
                            );
                            turso_assert!(
                                tx.begin_ts != end_ts,
                                "begin_ts and preparing end_ts cannot be equal: txn timestamps are strictly monotonic"
                            );
                            if tx.begin_ts > end_ts {
                                register_commit_dependency(txs, tx, rv_begin);
                                true
                            } else {
                                false
                            }
                        }
                        TransactionState::Committed(committed_ts) => {
                            turso_assert!(
                                tx.begin_ts != committed_ts,
                                "begin_ts and committed_ts cannot be equal: txn timestamps are strictly monotonic"
                            );
                            tx.begin_ts > committed_ts
                        }
                        TransactionState::Aborted => false,
                        TransactionState::Terminated => {
                            tracing::debug!(
                                "TODO: should reread rv's end field - it should have updated the timestamp in the row version by now"
                            );
                            false
                        }
                    };
                    tracing::trace!(
                        "is_begin_visible: tx={tx}, tb={tb} rv = {:?}-{:?} visible = {visible}",
                        rv.begin(),
                        rv.end()
                    );
                    visible
                }
                None => match lookup_finalized_tx_state(finalized_tx_states, rv_begin) {
                    Some(TransactionState::Committed(committed_ts)) => {
                        turso_assert!(
                            tx.begin_ts != committed_ts,
                            "begin_ts and committed_ts cannot be equal: txn timestamps are strictly monotonic"
                        );
                        tx.begin_ts > committed_ts
                    }
                    Some(TransactionState::Aborted) | Some(TransactionState::Terminated) => false,
                    Some(TransactionState::Active) | Some(TransactionState::Preparing(_)) => {
                        unreachable!(
                            "is_begin_visible: live tx {} missing from txs but present in finalized cache",
                            rv_begin
                        );
                    }
                    None => {
                        // Transaction was removed from the map after converting its TxID refs
                        // to Timestamps. The begin field should have been updated but we still
                        // see the stale TxID. Conservative fallback.
                        false
                    }
                },
            };
            visible
        }
        None => false,
    }
}

fn is_end_visible<A: ConcurrentAllocator>(
    txs: &SkipMap<TxID, Transaction, BasicComparator, A>,
    finalized_tx_states: &SkipMap<TxID, TransactionState, BasicComparator, A>,
    current_tx: &Transaction,
    row_version: &RowVersion,
) -> bool {
    match row_version.end() {
        Some(TxTimestampOrID::Timestamp(rv_end_ts)) => current_tx.begin_ts < rv_end_ts,
        Some(TxTimestampOrID::TxID(rv_end)) => {
            let visible = match txs.get(&rv_end) {
                Some(other_tx_entry) => {
                    let other_tx = other_tx_entry.value();
                    let visible = match other_tx.state.load() {
                        // V's sharp mind discovered an issue with the hekaton paper which basically states that a
                        // transaction can see a row version if the end is a TXId only if it isn't the same transaction.
                        // Source: https://avi.im/blag/2023/hekaton-paper-typo/
                        TransactionState::Active => current_tx.tx_id != other_tx.tx_id,
                        // Hekaton Table 2: speculative ignore of TE. If end_ts < begin_ts,
                        // we speculatively ignore V (treat deletion as committed). Register a
                        // dependency in case TE aborts (then V should have been visible).
                        TransactionState::Preparing(end_ts) => {
                            turso_assert!(
                                current_tx.tx_id != other_tx.tx_id,
                                "a txn is reading itself while preparing"
                            );
                            let visible = current_tx.begin_ts < end_ts;
                            if !visible {
                                register_commit_dependency(txs, current_tx, rv_end);
                            }
                            visible
                        }
                        TransactionState::Committed(committed_ts) => {
                            current_tx.begin_ts < committed_ts
                        }
                        TransactionState::Aborted => true,
                        // Table 2 (Hekaton): Reread V's End field. In this codebase Terminated is only
                        // reachable from Aborted, and abort rollback resets end to None → visible.
                        TransactionState::Terminated => true,
                    };
                    tracing::trace!(
                        "is_end_visible: tx={current_tx}, te={other_tx} rv = {:?}-{:?}  visible = {visible}",
                        row_version.begin(),
                        row_version.end()
                    );
                    visible
                }
                None => match lookup_finalized_tx_state(finalized_tx_states, rv_end) {
                    Some(TransactionState::Committed(committed_ts)) => {
                        current_tx.begin_ts < committed_ts
                    }
                    Some(TransactionState::Aborted) | Some(TransactionState::Terminated) => true,
                    Some(TransactionState::Active) | Some(TransactionState::Preparing(_)) => {
                        unreachable!(
                            "is_end_visible: live tx {rv_end} missing from txs but present in finalized cache"
                        );
                    }
                    None => {
                        // Transaction was removed after converting its TxID refs to Timestamps.
                        // The end field should have been updated. Conservative fallback.
                        true
                    }
                },
            };
            visible
        }
        None => true,
    }
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> Debug for CommitState<Clock, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initial => write!(f, "Initial"),
            Self::Commit { end_ts } => f.debug_struct("Commit").field("end_ts", end_ts).finish(),
            Self::WaitForDependencies { end_ts } => f
                .debug_struct("WaitForDependencies")
                .field("end_ts", end_ts)
                .finish(),
            Self::BuildLogRecord(ctx) => f.debug_tuple("BuildLogRecord").field(ctx).finish(),
            Self::BeginCommitLogicalLog { end_ts, log_record } => f
                .debug_struct("BeginCommitLogicalLog")
                .field("end_ts", end_ts)
                .field("log_record", log_record)
                .finish(),
            Self::UpgradeLogicalLogHeader { end_ts, log_record } => f
                .debug_struct("UpgradeLogicalLogHeader")
                .field("end_ts", end_ts)
                .field("log_record", log_record)
                .finish(),
            Self::WriteLogicalLog { end_ts, log_record } => f
                .debug_struct("WriteLogicalLog")
                .field("end_ts", end_ts)
                .field("log_record", log_record)
                .finish(),
            Self::FinishLogicalLogWrite { end_ts } => f
                .debug_struct("FinishLogicalLogWrite")
                .field("end_ts", end_ts)
                .finish(),
            Self::SyncLogicalLog { end_ts } => f
                .debug_struct("SyncLogicalLog")
                .field("end_ts", end_ts)
                .finish(),
            Self::EndCommitLogicalLog { end_ts } => f
                .debug_struct("EndCommitLogicalLog")
                .field("end_ts", end_ts)
                .finish(),
            Self::Checkpoint { state_machine: _ } => f.debug_struct("Checkpoint").finish(),
            Self::CommitEnd { end_ts } => {
                f.debug_struct("CommitEnd").field("end_ts", end_ts).finish()
            }
            Self::RewriteLiveVersions(ctx) => {
                f.debug_tuple("RewriteLiveVersions").field(ctx).finish()
            }
            Self::FinalizeCommit { end_ts } => f
                .debug_struct("FinalizeCommit")
                .field("end_ts", end_ts)
                .finish(),
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
