use crate::alloc::{ConcurrentAllocator, TryReserveError, TursoAllocator};
use crate::skiplist::{comparator::BasicComparator, map::Entry};
use crate::turso_assert;
use crate::types::IOResultOr;

use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{
    CursorTransaction, MVTableId, MvStore, MvccReadSnapshot, Row, RowID, RowKey, RowVersions,
    SortableIndexKey,
};
#[cfg(any(test, injected_yields))]
use crate::mvcc::yield_hooks::{ProvidesYieldContext, YieldContext, YieldPointMarker};
use crate::mvcc::yield_points::inject_io_yield;
use crate::storage::btree::{BTreeCursor, BTreeKey, CursorTrait};
use crate::sync::Arc;
use crate::translate::plan::IterationDirection;
use crate::types::{
    compare_immutable, IOCompletions, IOResult, ImmutableRecord, IndexInfo, SeekKey, SeekOp,
    SeekResult, Value, ValueRef,
};
use crate::vdbe::Register;
use crate::{return_if_io, Completion, Connection, LimboError, Pager, Result};
use std::any::Any;
use std::fmt::Debug;
use std::ops::Bound;
#[cfg(any(test, injected_yields))]
use strum::EnumCount;

#[derive(Clone)]
enum CursorPosition<A: ConcurrentAllocator = TursoAllocator> {
    /// We haven't loaded any row yet.
    BeforeFirst,
    /// We have loaded a row. This position points to a rowid in either MVCC index or in BTree.
    Loaded {
        row_id: RowID,
        /// Indicates whether the rowid is pointing BTreeCursor or MVCC index.
        in_btree: bool,
        /// Resolved MVCC version chain for this row, captured from the range
        /// iterator so `read_mvcc_current_row` can skip a second `self.rows.get`.
        /// `Some` only for MVCC table rows reached via the scan path; `None`
        /// (btree rows, index rows, seek/insert positions) falls back to a lookup.
        versions: Option<RowVersions<A>>,
    },
    /// We have reached the end of the table.
    End,
}

impl<A: ConcurrentAllocator> Debug for CursorPosition<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BeforeFirst => f.write_str("BeforeFirst"),
            Self::Loaded {
                row_id, in_btree, ..
            } => f
                .debug_struct("Loaded")
                .field("row_id", row_id)
                .field("in_btree", in_btree)
                .finish_non_exhaustive(),
            Self::End => f.write_str("End"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ExistsState {
    ExistsBtree,
}

#[derive(Debug, Clone, Copy)]
/// State machine for advancing the btree cursor.
/// Advancing means advancing the btree iterator that could be going either forwards or backwards.
enum AdvanceBtreeState {
    RewindCheckBtreeKey, // Check if first key found is valid
    NextBtree,           // Advance to next key
    NextCheckBtreeKey,   // Check if next key found is valid, if it isn't go back to NextBtree
}

#[derive(Debug, Clone, Copy)]
/// Rewind state is used to track the state of the rewind **AND** last operation. Since both seem to do similiar
/// operations we can use the same enum for both.
enum RewindState {
    Advance,
}

#[derive(Debug, Clone, Copy)]
enum LastMove {
    Other,
    Next,
    Prev,
    ExactSeek,
}

#[derive(Debug, Clone, Copy)]
enum NextState {
    AdvanceUnitialized,
    CheckNeedsAdvance,
    Advance,
}
#[derive(Debug, Clone, Copy)]
enum PrevState {
    AdvanceUnitialized,
    CheckNeedsAdvance,
    Advance,
}

#[derive(Debug, Clone, Copy)]
enum SeekBtreeState {
    /// Seeking in btree (MVCC seek already done)
    SeekBtree,
    /// Advance to next key in btree (if we got [SeekResult::TryAdvance], or the current row is shadowed by MVCC)
    AdvanceBTree,
    /// Check if current row is visible (not shadowed by MVCC)
    CheckRow,
}

#[derive(Debug, Clone, Copy)]
enum SeekState {
    PointBtree,
    PointCheckBtreeFound,
    PointCheckBtreeMissing,
    /// Seeking in btree (MVCC seek already done)
    SeekBtree(SeekBtreeState),
    /// Pick winner and finalize
    PickWinner,
}

#[derive(Debug, Clone, Copy)]
enum CountState {
    Btree { generation: u64 },
    Rewind,
    NextBtree { count: usize },
    CheckBtreeKey { count: usize },
}
#[derive(Debug, Clone)]
enum MvccLazyCursorState {
    Next(NextState),
    Prev(PrevState),
    Rewind(RewindState),
    Exists(ExistsState),
    Seek(SeekState, IterationDirection),
}

#[cfg(any(test, injected_yields))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::EnumCount)]
#[repr(u8)]
pub(crate) enum CursorYieldPoint {
    NextStart,
    NextBtreeAdvance,
    PrevBtreeAdvance,
    SeekStart,
    SeekBtreeProgress,
    ExistsBtreeFallback,
    CountProgress,
    AdvanceBtreeForwardProgress,
    AdvanceBtreeBackwardProgress,
}

#[cfg(any(test, injected_yields))]
impl YieldPointMarker for CursorYieldPoint {
    const POINT_COUNT: u8 = Self::COUNT as u8;

    fn ordinal(self) -> u8 {
        self as u8
    }
}

#[cfg(any(test, injected_yields))]
impl<Clock: LogicalClock + 'static, A: ConcurrentAllocator> ProvidesYieldContext
    for MvccLazyCursor<Clock, A>
{
    fn yield_context(&self) -> YieldContext {
        let connection = self
            .connection
            .upgrade()
            .expect("yield context requires a live connection");
        YieldContext::new(
            connection.yield_injector(),
            connection.failure_injector(),
            self.yield_instance_id,
            cursor_yield_key(self.snapshot.tx_id, self.table_id),
        )
    }
}

fn current_pos_is_only_row_of_seek_key(
    current_row_id: &RowKey,
    seek_key: &SeekKey<'_>,
    mv_cursor_type: &MvccCursorType,
) -> Result<bool> {
    Ok(match (current_row_id, seek_key, mv_cursor_type) {
        (RowKey::Int(current), SeekKey::TableRowId(target), MvccCursorType::Table) => {
            *current == *target
        }
        (RowKey::Record(current), SeekKey::IndexKey(target), MvccCursorType::Index(index_info)) => {
            let target_values = target.get_values()?;
            index_key_matches_at_most_one_row(&target_values, index_info)
                && compare_immutable(
                    target_values,
                    current.key.get_values()?,
                    &index_info.key_info[..target.column_count()],
                )
                .is_eq()
        }
        _ => false,
    })
}

fn index_key_matches_at_most_one_row(key: &[ValueRef<'_>], index_info: &IndexInfo) -> bool {
    // num_cols counts the rowid when the index stores one. So a key this long
    // ends with the rowid, and two rows never share a rowid. An index without
    // a rowid (an index method's backing B-tree) stores each whole key once.
    if key.len() == index_info.num_cols {
        return true;
    }
    // In a UNIQUE index, the indexed columns alone also name one row, unless a
    // value is NULL: a UNIQUE index can hold any number of rows with NULL there.
    let indexed_columns = index_info.num_cols - usize::from(index_info.has_rowid);
    index_info.is_unique
        && key.len() == indexed_columns
        && !key.iter().any(|value| matches!(value, ValueRef::Null))
}

#[cfg(any(test, injected_yields))]
fn cursor_yield_key(tx_id: u64, table_id: MVTableId) -> u64 {
    // ASCII-ish "CURSORCR"
    // any large number will do
    const CURSOR_SELECTION_TAG: u64 = 0x4355_5253_4F52_4352;
    // Mix tx/table identity and add a per-family tag (here Cursor tag), so that we get a nice
    // yield plans
    // 17 here is arbitrary, any number would do.
    tx_id ^ (i64::from(table_id) as u64).rotate_left(17) ^ CURSOR_SELECTION_TAG
}

/// We read rows from MVCC index or BTree in a dual-cursor approach.
/// This means we read rows from both cursors and then advance the cursor that was just consumed.
/// With DualCursorPeek we track the "peeked" next value for each cursor in the dual-cursor iteration,
/// so that we always return the correct 'next' value (e.g. if mvcc has 1 and 3 and btree has 2 and 4,
/// we should return 1, 2, 3, 4 in order).
#[derive(Debug, Clone)]
struct DualCursorPeek<A: ConcurrentAllocator = TursoAllocator> {
    /// Next row available from MVCC
    mvcc_peek: CursorPeek<A>,
    /// Next row available from btree
    btree_peek: CursorPeek<A>,
}

impl<A: ConcurrentAllocator> Default for DualCursorPeek<A> {
    fn default() -> Self {
        Self {
            mvcc_peek: CursorPeek::default(),
            btree_peek: CursorPeek::default(),
        }
    }
}

impl<A: ConcurrentAllocator> DualCursorPeek<A> {
    /// Returns the next row key, whether the row is from the BTree, and (for
    /// MVCC winners) the resolved version chain captured during iteration.
    fn get_next(&self, dir: IterationDirection) -> Option<(RowKey, bool, Option<RowVersions<A>>)> {
        tracing::trace!(
            "get_next: mvcc_key: {:?}, btree_key: {:?}",
            self.mvcc_peek.get_row_key(),
            self.btree_peek.get_row_key()
        );
        match (self.mvcc_peek.get_row_key(), self.btree_peek.get_row_key()) {
            (Some(mvcc_key), Some(btree_key)) => {
                if dir == IterationDirection::Forwards {
                    // In forwards iteration we want the smaller of the two keys
                    if mvcc_key <= btree_key {
                        Some((mvcc_key.clone(), false, self.mvcc_peek.get_versions()))
                    } else {
                        Some((btree_key.clone(), true, None))
                    }
                // In backwards iteration we want the larger of the two keys
                } else if mvcc_key >= btree_key {
                    Some((mvcc_key.clone(), false, self.mvcc_peek.get_versions()))
                } else {
                    Some((btree_key.clone(), true, None))
                }
            }
            (Some(mvcc_key), None) => {
                Some((mvcc_key.clone(), false, self.mvcc_peek.get_versions()))
            }
            (None, Some(btree_key)) => Some((btree_key.clone(), true, None)),
            (None, None) => None,
        }
    }

    /// Returns a new [CursorPosition] based on the next row key
    pub fn cursor_position_from_next(
        &self,
        table_id: MVTableId,
        dir: IterationDirection,
    ) -> CursorPosition<A> {
        match self.get_next(dir) {
            Some((row_key, in_btree, versions)) => CursorPosition::Loaded {
                row_id: RowID {
                    table_id,
                    row_id: row_key,
                },
                in_btree,
                versions,
            },
            None => match dir {
                IterationDirection::Forwards => CursorPosition::End,
                IterationDirection::Backwards => CursorPosition::BeforeFirst,
            },
        }
    }

    pub fn both_uninitialized(&self) -> bool {
        matches!(self.mvcc_peek, CursorPeek::Uninitialized)
            && matches!(self.btree_peek, CursorPeek::Uninitialized)
    }

    pub fn btree_uninitialized(&self) -> bool {
        matches!(self.btree_peek, CursorPeek::Uninitialized)
    }

    pub fn mvcc_exhausted(&self) -> bool {
        matches!(self.mvcc_peek, CursorPeek::Exhausted)
    }
    pub fn btree_exhausted(&self) -> bool {
        matches!(self.btree_peek, CursorPeek::Exhausted)
    }
}

#[derive(Debug, Clone)]
enum CursorPeek<A: ConcurrentAllocator = TursoAllocator> {
    Uninitialized,
    Row {
        key: RowKey,
        /// Resolved MVCC version chain, set when this peek came from the MVCC
        /// table or index iterator. `None` for btree peeks.
        versions: Option<RowVersions<A>>,
    },
    Exhausted,
}

impl<A: ConcurrentAllocator> Default for CursorPeek<A> {
    fn default() -> Self {
        Self::Uninitialized
    }
}

impl<A: ConcurrentAllocator> CursorPeek<A> {
    pub fn get_row_key(&self) -> Option<&RowKey> {
        match self {
            CursorPeek::Row { key, .. } => Some(key),
            _ => None,
        }
    }

    pub fn get_versions(&self) -> Option<RowVersions<A>> {
        match self {
            CursorPeek::Row { versions, .. } => versions.clone(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MvccCursorType {
    Table,
    Index(Arc<IndexInfo>),
}

pub(crate) type MvccEntry<'l, T, A = TursoAllocator> =
    Entry<'l, T, RowVersions<A>, BasicComparator, A>;

pub(crate) type MvccIterator<'l, T, A = TursoAllocator> =
    Box<dyn Iterator<Item = MvccEntry<'l, T, A>> + Send + Sync>;

/// Extends the lifetime of a SkipMap iterator to `'static`.
///
/// # Why a macro instead of a function?
///
/// Rust's `crate::skiplist::map::Entry<'a, K, V>` is *invariant* over `K`, meaning
/// the lifetime `'a` cannot be coerced through a function boundary. When we try to pass
/// `Box<dyn Iterator<Item = Entry<'_, K, V>>>` to a function expecting a generic lifetime,
/// the compiler cannot unify the lifetimes across the function call.
///
/// A macro expands inline at the call site, avoiding the function boundary entirely and
/// allowing the explicit transmute with both source and destination types specified.
///
/// # Safety
///
/// The caller must ensure that the underlying `SkipMap` from which the iterator was created
/// outlives the returned iterator. This is guaranteed when:
/// - For table iterators: The `MvStore.rows` SkipMap is held in an `Arc<MvStore>` that
///   outlives the cursor.
/// - For index iterators: The `MvStore.index_rows` SkipMap is held in an `Arc<MvStore>`
///   that outlives the cursor.
macro_rules! static_iterator_hack {
    ($iter:expr, $key_type:ty) => {
        static_iterator_hack!($iter, $key_type, crate::alloc::TursoAllocator)
    };
    ($iter:expr, $key_type:ty, $alloc:ty) => {
        // SAFETY: See macro documentation above.
        unsafe {
            std::mem::transmute::<
                Box<
                    dyn Iterator<Item = crate::mvcc::cursor::MvccEntry<'_, $key_type, $alloc>>
                        + Send
                        + Sync,
                >,
                Box<
                    dyn Iterator<Item = crate::mvcc::cursor::MvccEntry<'static, $key_type, $alloc>>
                        + Send
                        + Sync,
                >,
            >($iter)
        }
    };
}

pub(crate) use static_iterator_hack;

/// Forward scan over `index_rows`, co-advanced with the B-tree cursor so the
/// per-row "is this B-tree row shadowed by MVCC?" check is an amortized-O(1)
/// merge step instead of an `index_rows.get()` (O(log N)) per scanned row.
/// Forward index cursors only. Call [`reset`](Self::reset) on any reposition.
/// The scan is monotonic.
///
/// Owns the [`MvStore::index_rows_epoch`] snapshot used to detect keys created
/// mid-scan (#7578). A mismatch reseeds from the current B-tree key.
pub(crate) struct IndexShadowScan<A: ConcurrentAllocator = TursoAllocator> {
    state: IndexShadowScanState<A>,
    epoch: u64,
}

impl<A: ConcurrentAllocator> Default for IndexShadowScan<A> {
    fn default() -> Self {
        Self {
            state: IndexShadowScanState::default(),
            epoch: 0,
        }
    }
}

#[derive(Default)]
enum IndexShadowScanState<A: ConcurrentAllocator = TursoAllocator> {
    /// Not yet created; built lazily on the next shadow check.
    #[default]
    Uninitialized,
    /// Positioned at `key`, holding its version chain. The shadow bit is resolved
    /// lazily (only when a B-tree row matches this key exactly).
    Peeked {
        iter: MvccIterator<'static, Arc<SortableIndexKey>, A>,
        key: Arc<SortableIndexKey>,
        versions: RowVersions<A>,
    },
    /// Ran past the last version; every remaining B-tree row is visible.
    Exhausted,
}

impl<A: ConcurrentAllocator> IndexShadowScan<A> {
    /// Drop the current position so the next shadow check reseeds. Required on
    /// any B-tree reposition (seek/rewind). A scan left ahead of the new
    /// position would report a shadowed row as valid.
    fn reset(&mut self) {
        self.state = IndexShadowScanState::Uninitialized;
    }

    /// Advance `iter` to its next entry, cloning the key and version-chain `Arc`
    /// (both cheap) so no borrowed skiplist `Entry` is held afterward. The shadow
    /// bit is deliberately not resolved here. See [`IndexShadowScanState::Peeked`].
    fn advance(
        mut iter: MvccIterator<'static, Arc<SortableIndexKey>, A>,
    ) -> IndexShadowScanState<A> {
        match iter.next() {
            Some(entry) => IndexShadowScanState::Peeked {
                key: entry.key().clone(),
                versions: entry.value().clone(),
                iter,
            },
            None => IndexShadowScanState::Exhausted,
        }
    }

    /// Whether the B-tree row `key` is visible (not shadowed by an MVCC version),
    /// served from this co-positioned scan. Forward equivalent of
    /// [`MvStore::query_btree_version_is_valid`] for index keys.
    pub(crate) fn btree_row_is_valid<Clock: LogicalClock>(
        &mut self,
        db: &MvStore<Clock, A>,
        table_id: MVTableId,
        tx_id: u64,
        key: &Arc<SortableIndexKey>,
    ) -> Result<bool> {
        // Read the epoch before (re)seeding. If a key insert races past this
        // load, the next shadow check observes the mismatch and reseeds.
        let epoch = db.index_rows_epoch();
        if self.epoch != epoch {
            self.reset();
            self.epoch = epoch;
        }
        if matches!(self.state, IndexShadowScanState::Uninitialized) {
            // Scoped so the skiplist guard drops before later `db` borrows.
            let iter = {
                // Avoid allocating skiplist here with `try_get_or_insert_with`
                let index_rows = db.index_rows.get(&table_id);
                // Seed at the first index key >= the B-tree key rather than at
                // the start of `index_rows`, so a seek-initiated scan does not
                // re-walk every preceding version on its first row check.
                let iter_box: Box<
                    dyn Iterator<Item = MvccEntry<'_, Arc<SortableIndexKey>, A>> + Send + Sync,
                > = match index_rows {
                    Some(index_rows) => {
                        Box::new(index_rows.value().range::<SortableIndexKey, _>((
                            std::ops::Bound::Included(key.as_ref()),
                            std::ops::Bound::Unbounded,
                        )))
                    }
                    None => Box::new(std::iter::empty()),
                };
                static_iterator_hack!(iter_box, Arc<SortableIndexKey>, A)
            };
            self.state = Self::advance(iter);
        }
        loop {
            match &self.state {
                // No version at or after this key -> B-tree row is visible.
                IndexShadowScanState::Exhausted => return Ok(true),
                IndexShadowScanState::Uninitialized => unreachable!("created just above"),
                IndexShadowScanState::Peeked {
                    key: scan_key,
                    versions,
                    ..
                } => match scan_key.as_ref().cmp(key.as_ref()) {
                    // No version exactly at this key -> visible.
                    std::cmp::Ordering::Greater => return Ok(true),
                    // Version present at this key -> resolve the shadow bit now,
                    // on the one key that actually matches a B-tree row.
                    std::cmp::Ordering::Equal => {
                        return Ok(!db.index_chain_invalidates_btree(versions, table_id, tx_id)?);
                    }
                    // Scan is behind the B-tree (a version-only key). Catch up below.
                    std::cmp::Ordering::Less => {}
                },
            }
            // Step the scan forward. Only the `Less` arm above falls through here.
            let IndexShadowScanState::Peeked { iter, .. } =
                std::mem::replace(&mut self.state, IndexShadowScanState::Uninitialized)
            else {
                unreachable!("Less arm matched Peeked")
            };
            self.state = Self::advance(iter);
        }
    }
}

pub struct MvccLazyCursor<Clock: LogicalClock + 'static, A: ConcurrentAllocator = TursoAllocator> {
    table_walk: Option<Box<TableChainWalk<A>>>,
    pub db: Arc<MvStore<Clock, A>>,
    /// Weak so a cursor retained past its statement (an index-method cursor
    /// parked on its connection) cannot keep the connection alive.
    #[cfg(any(test, injected_yields))]
    connection: crate::sync::Weak<Connection>,
    #[cfg(any(test, injected_yields))]
    yield_instance_id: u64,
    current_pos: CursorPosition<A>,
    /// Stateful MVCC table iterator if this is a table cursor.
    table_iterator: Option<MvccIterator<'static, RowID, A>>,
    /// Stateful MVCC index iterator if this is an index cursor.
    index_iterator: Option<MvccIterator<'static, Arc<SortableIndexKey>, A>>,
    mv_cursor_type: MvccCursorType,
    table_id: MVTableId,
    transaction: Box<CursorTransaction<Clock, A>>,
    snapshot: MvccReadSnapshot,
    /// Reusable immutable record, used to allow better allocation strategy.
    reusable_immutable_record: Option<ImmutableRecord>,
    payload_row: Option<Row>,
    payload_generation: u64,
    table_scan: TableScan,
    last_move: LastMove,
    /// Eq-only table seek copies the occupying payload under the version lock.
    /// Passive GC can empty the live chain before Column.
    eq_seek_row: Option<Row>,
    /// Set once the B-tree is readable at this cursor's snapshot. It stays
    /// readable for the cursor's lifetime.
    btree_readable: bool,
    btree_cursor: Box<dyn CursorTrait>,
    null_flag: bool,
    creating_new_rowid: bool,
    state: Option<MvccLazyCursorState>,
    // we keep count_state separate to be able to call other public functions like rewind and next
    count_state: Option<CountState>,
    btree_advance_state: Option<AdvanceBtreeState>,
    /// Dual-cursor peek state for proper iteration
    dual_peek: DualCursorPeek<A>,
    /// Forward scan over `index_rows`; see [`IndexShadowScan`].
    index_shadow_scan: IndexShadowScan<A>,
    /// [`MvStore::index_rows_epoch`] read before the MVCC index peek was taken.
    /// See [`Self::reseek_index_if_index_rows_changed`].
    index_peek_epoch: u64,
}

pub enum NextRowidResult {
    /// We need to go to the last rowid and intialize allocator
    Uninitialized,
    /// It was initialized, so we get a new rowid
    Next {
        new_rowid: i64,
        prev_rowid: Option<i64>,
    },
    /// We reached end of available rowids (i64::MAX), so we will have to try and find a random rowid.
    FindRandom,
}

impl<Clock: LogicalClock + 'static, A: ConcurrentAllocator> MvccLazyCursor<Clock, A> {
    pub fn new(
        db: Arc<MvStore<Clock, A>>,
        connection: &Arc<Connection>,
        tx_id: u64,
        root_page_or_table_id: i64,
        mv_cursor_type: MvccCursorType,
        btree_cursor: Box<dyn CursorTrait>,
    ) -> Result<MvccLazyCursor<Clock, A>> {
        turso_assert!(
            (&*btree_cursor as &dyn Any).is::<BTreeCursor>(),
            "BTreeCursor expected for mvcc cursor"
        );
        // Resolve the root page against this reader's snapshot: a PASSIVE checkpoint may have
        // dropped (and possibly reused) the page during collection while we still reference it at an
        // older snapshot. The WAL read mark keeps the pages readable; this keeps the in-memory
        // root_page -> table_id reverse lookup snapshot-consistent. See `retired_rootpages`.
        let snapshot = db.read_snapshot(tx_id)?;
        let table_id = if connection.experimental_mvcc_passive_checkpoint_enabled() {
            // Under PASSIVE checkpointing a transaction can capture a schema cookie older than
            // the drop committed within its own snapshot (the drop publishes its cookie after
            // the transaction reads the header, even though the drop's commit ts precedes the
            // transaction's begin ts). The compiled cursor then points at a positive root page
            // its snapshot already sees dropped. That is a stale-schema read, not an invariant
            // violation: reprepare against the current schema instead of panicking.
            db.try_get_table_id_from_root_page_at(root_page_or_table_id, snapshot.begin_ts)
                .ok_or(LimboError::SchemaUpdated)?
        } else {
            db.get_table_id_from_root_page_at(root_page_or_table_id, snapshot.begin_ts)
        };
        let transaction = db.cursor_transaction(tx_id)?;
        let table_scan = TableScan::new(transaction.write_generation());
        Ok(Self {
            db,
            #[cfg(any(test, injected_yields))]
            yield_instance_id: connection.next_yield_instance_id(),
            #[cfg(any(test, injected_yields))]
            connection: Arc::downgrade(connection),
            transaction,
            snapshot,
            table_iterator: None,
            index_iterator: None,
            mv_cursor_type,
            current_pos: CursorPosition::BeforeFirst,
            table_id,
            reusable_immutable_record: None,
            payload_generation: 0,
            table_walk: None,
            table_scan,
            payload_row: None,
            last_move: LastMove::Other,
            eq_seek_row: None,
            btree_readable: false,
            btree_cursor,
            null_flag: false,
            creating_new_rowid: false,
            state: None,
            count_state: None,
            btree_advance_state: None,
            dual_peek: DualCursorPeek::default(),
            index_shadow_scan: IndexShadowScan::default(),
            index_peek_epoch: 0,
        })
    }

    fn btree_row_is_valid_forward(&mut self, key: &RowKey) -> Result<bool> {
        let rec = match key {
            RowKey::Int(rowid) => {
                return self.table_row_is_valid(*rowid, IterationDirection::Forwards)
            }
            RowKey::Record(rec) => rec,
        };
        let valid = self.index_shadow_scan.btree_row_is_valid(
            &self.db,
            self.table_id,
            self.snapshot.tx_id,
            rec,
        )?;
        // Debug-only cross-check: any scan divergence (e.g. a missed reset)
        // fails the test suite instead of shipping.
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            valid,
            self.db.query_btree_version_is_valid(
                self.table_id,
                &RowKey::Record(rec.clone()),
                self.snapshot.tx_id
            )?,
            "index shadow scan diverged from query_btree_version_is_valid"
        );
        Ok(valid)
    }

    /// Returns the current row as an immutable record.
    pub fn current_row(&mut self) -> IOResultOr<Option<&crate::types::ImmutableRecord>> {
        if self.transaction.should_abort() {
            return Err(LimboError::CommitDependencyAborted.into());
        }
        if self.get_null_flag() {
            return Ok(IOResult::Done(None));
        }
        tracing::trace!("current_row({:?})", self.current_pos);
        match &self.current_pos {
            CursorPosition::Loaded { in_btree: true, .. } => self.btree_cursor.record(),
            CursorPosition::Loaded {
                in_btree: false,
                row_id,
                versions,
            } => {
                if self.reusable_immutable_record.is_none() {
                    self.reusable_immutable_record = Some(ImmutableRecord::new(1024)?);
                }
                let record = self.reusable_immutable_record.as_mut().unwrap();

                let found = if let Some(versions) = versions {
                    // Fast path: serialize the visible version straight into our
                    // reusable record — like the btree cursor does with a cell —
                    // instead of cloning a `Row` first.
                    self.db
                        .read_visible_into_record(self.snapshot, row_id, versions, record)?
                } else {
                    // Cold fallback (seek-positioned, no cached chain): point
                    // lookup, then serialize.
                    let maybe_index_id = match &self.mv_cursor_type {
                        MvccCursorType::Index(_) => Some(self.table_id),
                        MvccCursorType::Table => None,
                    };
                    match self.db.read_from_table_or_index(
                        self.snapshot.tx_id,
                        row_id,
                        maybe_index_id,
                    )? {
                        Some(row) => {
                            record.invalidate();
                            record.start_serialization(row.payload())?;
                            true
                        }
                        None => false,
                    }
                };

                if !found {
                    let Some(row) = self.eq_seek_row.as_ref().filter(|row| row.id == *row_id)
                    else {
                        if self.transaction.should_abort() {
                            return Err(LimboError::CommitDependencyAborted.into());
                        }
                        return Ok(IOResult::Done(None));
                    };
                    record.invalidate();
                    record.start_serialization(row.payload())?;
                }
                Ok(IOResult::Done(Some(record)))
            }
            CursorPosition::BeforeFirst => {
                // Before first is not a valid position, so we return none.
                Ok(IOResult::Done(None))
            }
            CursorPosition::End => Ok(IOResult::Done(None)),
        }
    }

    pub fn read_mvcc_current_row(&self) -> Result<Option<Row>> {
        let (row_id, versions) = match &self.current_pos {
            CursorPosition::Loaded {
                row_id,
                in_btree,
                versions,
            } if !in_btree => (row_id, versions),
            _ => panic!("invalid position to read current mvcc row"),
        };
        // Scan path: the range iterator already resolved this row's version
        // chain, so read it directly instead of a second skiplist lookup.
        if let Some(versions) = versions {
            return self
                .db
                .read_visible_from_versions(self.snapshot, row_id, versions);
        }
        let maybe_index_id = match &self.mv_cursor_type {
            MvccCursorType::Index(_) => Some(self.table_id),
            MvccCursorType::Table => None,
        };
        self.db
            .read_from_table_or_index(self.snapshot.tx_id, row_id, maybe_index_id)
    }

    pub fn close(self) -> Result<()> {
        Ok(())
    }

    pub fn start_new_rowid(&mut self) -> IOResultOr<NextRowidResult> {
        tracing::trace!("start_new_rowid");

        let allocator = self.db.get_rowid_allocator(&self.table_id);
        let locked = allocator.lock();
        if !locked {
            // Yield, some other cursor is generating new rowid
            return Ok(IOResult::IO(IOCompletions(Completion::new_yield())));
        }

        self.creating_new_rowid = true;
        let res = if allocator.is_uninitialized() {
            NextRowidResult::Uninitialized
        } else if let Some((next_rowid, prev_max_rowid)) = allocator.get_next_rowid() {
            NextRowidResult::Next {
                new_rowid: next_rowid,
                prev_rowid: prev_max_rowid,
            }
        } else {
            NextRowidResult::FindRandom
        };
        Ok(IOResult::Done(res))
    }

    pub fn initialize_max_rowid(&mut self, max_rowid: Option<i64>) -> Result<()> {
        let allocator = self.db.get_rowid_allocator(&self.table_id);
        turso_assert!(
            self.creating_new_rowid,
            "cursor didn't start creating new rowid"
        );
        allocator.initialize(max_rowid);
        Ok(())
    }

    /// Allocate the next rowid from the (already initialized) allocator.
    /// Must be called while holding the allocator lock.
    pub fn allocate_next_rowid(&self) -> Option<(i64, Option<i64>)> {
        let allocator = self.db.get_rowid_allocator(&self.table_id);
        allocator.get_next_rowid()
    }

    pub fn end_new_rowid(&mut self) {
        tracing::trace!(
            "end_new_rowid creating_new_rowid={}",
            self.creating_new_rowid
        );
        // if we started creating a new rowid, we need to unlock the allocator
        // this might be false if there was an error during `op_new_rowid` before calling `start_new_rowid` so we can call this function
        // in any case
        if self.creating_new_rowid {
            let allocator = self.db.get_rowid_allocator(&self.table_id);
            allocator.unlock();
            self.creating_new_rowid = false;
        }
    }

    fn get_current_pos(&self) -> CursorPosition<A> {
        self.current_pos.clone()
    }

    fn is_btree_allocated(&mut self) -> bool {
        // Dual gate (logical base-validity AND physical visibility): a PASSIVE checkpoint may
        // materialize this object's btree during collection. This cursor may read it only if the binding
        // covers our snapshot AND its pages were already durable when we pinned our read mark
        // (`visible_from <= observed_boundary`). A cursor that opened before checkpoint publish
        // materialization therefore stays version-store-only for its whole life and never seeks
        // the page its read mark can't see. See `MvStore::is_btree_readable_at`.
        if self.btree_readable {
            return true;
        }
        if !self.db.is_btree_readable_at(
            &self.table_id,
            self.snapshot.begin_ts,
            self.snapshot.read_mark,
        ) {
            return false;
        }
        if self.btree_cursor.root_page() < 0 {
            let Some(root) = self.db.current_root_page(&self.table_id) else {
                return false;
            };
            self.btree_cursor.set_root_page(root as i64);
        }
        self.btree_readable = true;
        true
    }

    fn query_btree_version_is_valid(&self, key: &RowKey) -> Result<bool> {
        self.db.cursor_btree_row_is_valid(
            &self.transaction,
            self.table_id,
            key,
            self.snapshot.tx_id,
        )
    }

    /// Advance MVCC iterator and return next visible row key in the direction that the iterator was initialized in.
    fn advance_mvcc_iterator(&mut self) {
        let new_peek_state = match &self.mv_cursor_type {
            MvccCursorType::Table => match self.db.advance_cursor_and_get_row_id_for_table(
                self.table_id,
                &mut self.table_iterator,
                self.snapshot,
            ) {
                Some((row_id, versions)) => CursorPeek::Row {
                    key: row_id.row_id,
                    versions: Some(versions),
                },
                None => CursorPeek::Exhausted,
            },
            MvccCursorType::Index(_) => match self.db.advance_cursor_and_get_row_id_for_index(
                &mut self.index_iterator,
                self.snapshot,
                self.table_id,
            ) {
                Some((row_id, versions)) => CursorPeek::Row {
                    key: row_id.row_id,
                    versions: Some(versions),
                },
                None => CursorPeek::Exhausted,
            },
        };
        self.dual_peek.mvcc_peek = new_peek_state;
    }

    /// Seeks the MVCC index iterator again, just past the current key, if index
    /// versions were inserted or deleted since the MVCC peek was taken. The
    /// iterator sees a key added ahead of its position, but not one added
    /// between the current key and the peek, nor any key once it is exhausted.
    /// And the peek itself may have been deleted since. Either can be a write
    /// of this cursor's own transaction.
    ///
    /// Returns true if it seeked: the MVCC peek is then already past the
    /// current key and must not be advanced again.
    fn reseek_index_if_index_rows_changed(&mut self, dir: IterationDirection) -> Result<bool> {
        let CursorPosition::Loaded {
            row_id:
                RowID {
                    row_id: RowKey::Record(current_key),
                    ..
                },
            ..
        } = &self.current_pos
        else {
            return Ok(false);
        };
        let epoch = self.db.index_rows_epoch();
        if epoch == self.index_peek_epoch {
            return Ok(false);
        }
        self.index_peek_epoch = epoch;
        let current_key = current_key.as_ref().clone();
        self.dual_peek.mvcc_peek = match self.db.seek_index(
            self.table_id,
            current_key,
            false,
            false,
            dir,
            self.snapshot,
            &mut self.index_iterator,
        )? {
            Some((row_id, versions)) => CursorPeek::Row {
                key: row_id.row_id,
                versions: Some(versions),
            },
            None => CursorPeek::Exhausted,
        };
        Ok(true)
    }

    /// Advance btree cursor forward and set btree peek to the first valid row key (skipping rows shadowed by MVCC)
    fn advance_btree_forward(&mut self) -> IOResultOr<()> {
        self._advance_btree_forward(true)
    }

    /// Advance btree cursor forward from current position (cursor already positioned by seek)
    fn advance_btree_forward_from_current(&mut self) -> IOResultOr<()> {
        self._advance_btree_forward(false)
    }

    fn _advance_btree_forward(&mut self, initialize: bool) -> IOResultOr<()> {
        loop {
            let state = self.btree_advance_state;
            match state {
                None => {
                    if !self.is_btree_allocated() {
                        self.dual_peek.btree_peek = CursorPeek::Exhausted;
                        self.btree_advance_state = None;
                        return Ok(IOResult::Done(()));
                    }
                    // If the btree is uninitialized AND we should initialize, do the equivalent of rewind() to find the first valid row
                    if initialize && self.dual_peek.btree_uninitialized() {
                        return_if_io!(self.btree_cursor.rewind());
                        self.btree_advance_state = Some(AdvanceBtreeState::RewindCheckBtreeKey);
                    } else {
                        self.btree_advance_state = Some(AdvanceBtreeState::NextBtree);
                    }
                    inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeForwardProgress);
                }
                Some(AdvanceBtreeState::RewindCheckBtreeKey) => {
                    let key = self.get_btree_current_key()?;
                    match key {
                        Some(k) if self.btree_row_is_valid_forward(&k)? => {
                            self.dual_peek.btree_peek = CursorPeek::Row {
                                key: k,
                                versions: None,
                            };
                            self.btree_advance_state = None;
                            return Ok(IOResult::Done(()));
                        }
                        Some(_) => {
                            // shadowed by MVCC, continue to next
                            self.btree_advance_state = Some(AdvanceBtreeState::NextBtree);
                        }
                        None => {
                            self.dual_peek.btree_peek = CursorPeek::Exhausted;
                            self.btree_advance_state = None;
                            return Ok(IOResult::Done(()));
                        }
                    }
                }
                Some(AdvanceBtreeState::NextBtree) => {
                    let peek = &mut self.dual_peek;
                    return_if_io!(self.btree_cursor.next());
                    let found = self.btree_cursor.has_record();
                    if !found {
                        peek.btree_peek = CursorPeek::Exhausted;
                        self.btree_advance_state = None;
                        return Ok(IOResult::Done(()));
                    }
                    self.btree_advance_state = Some(AdvanceBtreeState::NextCheckBtreeKey);
                    inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeForwardProgress);
                }
                Some(AdvanceBtreeState::NextCheckBtreeKey) => {
                    let key = self.get_btree_current_key()?;
                    if self.peek_btree_key::<true>(key)? {
                        return Ok(IOResult::Done(()));
                    }
                }
            }
        }
    }

    /// Advance btree cursor backward and set btree peek to the first valid row key (skipping rows shadowed by MVCC)
    fn advance_btree_backward(&mut self) -> IOResultOr<()> {
        self._advance_btree_backward(true)
    }

    /// Advance btree cursor backward from current position (cursor already positioned by seek)
    fn advance_btree_backward_from_current(&mut self) -> IOResultOr<()> {
        self._advance_btree_backward(false)
    }

    fn _advance_btree_backward(&mut self, initialize: bool) -> IOResultOr<()> {
        loop {
            let state = self.btree_advance_state;
            match state {
                None => {
                    if !self.is_btree_allocated() {
                        let peek = &mut self.dual_peek;
                        peek.btree_peek = CursorPeek::Exhausted;
                        self.btree_advance_state = None;
                        return Ok(IOResult::Done(()));
                    }
                    // If the btree is uninitialized AND we should initialize, do the equivalent of last() to find the last valid row
                    if initialize && self.dual_peek.btree_uninitialized() {
                        return_if_io!(self.btree_cursor.last());
                        self.btree_advance_state = Some(AdvanceBtreeState::RewindCheckBtreeKey);
                    } else {
                        self.btree_advance_state = Some(AdvanceBtreeState::NextBtree);
                    }
                    inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeBackwardProgress);
                }
                Some(AdvanceBtreeState::RewindCheckBtreeKey) => {
                    let key = self.get_btree_current_key()?;
                    match key {
                        Some(k) if self.btree_row_is_valid_backward(&k)? => {
                            self.dual_peek.btree_peek = CursorPeek::Row {
                                key: k,
                                versions: None,
                            };
                            self.btree_advance_state = None;
                            return Ok(IOResult::Done(()));
                        }
                        Some(_) => {
                            // shadowed by MVCC, continue to prev
                            self.btree_advance_state = Some(AdvanceBtreeState::NextBtree);
                        }
                        None => {
                            self.dual_peek.btree_peek = CursorPeek::Exhausted;
                            self.btree_advance_state = None;
                            return Ok(IOResult::Done(()));
                        }
                    }
                }
                Some(AdvanceBtreeState::NextBtree) => {
                    return_if_io!(self.btree_cursor.prev());
                    let peek = &mut self.dual_peek;
                    let found = self.btree_cursor.has_record();
                    if !found {
                        peek.btree_peek = CursorPeek::Exhausted;
                        self.btree_advance_state = None;
                        return Ok(IOResult::Done(()));
                    }
                    self.btree_advance_state = Some(AdvanceBtreeState::NextCheckBtreeKey);
                    inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeBackwardProgress);
                }
                Some(AdvanceBtreeState::NextCheckBtreeKey) => {
                    let key = self.get_btree_current_key()?;
                    if self.peek_btree_key::<false>(key)? {
                        return Ok(IOResult::Done(()));
                    }
                }
            }
        }
    }

    /// Get the current key from btree cursor
    fn get_btree_current_key(&mut self) -> Result<Option<RowKey>> {
        match &self.mv_cursor_type {
            MvccCursorType::Table => {
                let maybe_rowid = loop {
                    match self.btree_cursor.rowid()? {
                        IOResult::Done(maybe_rowid) => {
                            break maybe_rowid.map(RowKey::Int);
                        }
                        IOResult::IO(c) => {
                            c.wait(self.btree_cursor.get_pager().io.as_ref())?; // FIXME: sync IO hack
                        }
                    }
                };
                Ok(maybe_rowid)
            }
            MvccCursorType::Index(index_info) => {
                let maybe_record = loop {
                    match self.btree_cursor.record()? {
                        IOResult::Done(maybe_record) => {
                            break maybe_record;
                        }
                        IOResult::IO(c) => {
                            c.wait(self.btree_cursor.get_pager().io.as_ref())?; // FIXME: sync IO hack
                        }
                    }
                };
                let Some(record) = maybe_record else {
                    return Ok(None);
                };
                let key = SortableIndexKey::new_from_payload_in(
                    record,
                    index_info.clone(),
                    self.db.allocator(),
                )?;
                Ok(Some(RowKey::Record(Arc::new(key))))
            }
        }
    }

    /// Refresh the current position based on the peek values
    fn refresh_current_position(&mut self, dir: IterationDirection) {
        self.current_pos = self.position_from_peeks(dir);
    }

    fn position_from_peeks(&mut self, dir: IterationDirection) -> CursorPosition<A> {
        loop {
            let pos = self.dual_peek.cursor_position_from_next(self.table_id, dir);
            if matches!(self.mv_cursor_type, MvccCursorType::Index(_)) {
                return pos;
            }
            let CursorPosition::Loaded {
                row_id,
                in_btree: false,
                versions: Some(versions),
            } = &pos
            else {
                return pos;
            };
            let row_id = row_id.clone();
            let table_id = row_id.table_id;
            let btree_has_newest_version = {
                let chain = versions.read();
                self.db
                    .btree_covers_chain_for_snapshot(self.snapshot, table_id, &chain)
            };
            if !btree_has_newest_version {
                return pos;
            }
            if self.dual_peek.btree_peek.get_row_key() == Some(&row_id.row_id) {
                return CursorPosition::Loaded {
                    row_id,
                    in_btree: true,
                    versions: None,
                };
            }
            self.advance_mvcc_iterator();
        }
    }

    /// Reset dual peek state (called on rewind/last/seek)
    fn reset_dual_peek(&mut self) {
        self.dual_peek = DualCursorPeek::default();
        self.index_peek_epoch = self.db.index_rows_epoch();
        self.eq_seek_row = None;
        self.last_move = LastMove::Other;
        // The forward scan is monotonic; a reposition invalidates it.
        self.index_shadow_scan.reset();
    }

    /// Seek btree cursor and set btree_peek to the result.
    /// Skips rows that are shadowed by MVCC.
    /// Returns IOResult indicating if we need to yield for IO or are done.
    fn seek_btree_and_set_peek(&mut self, seek_key: SeekKey<'_>, op: SeekOp) -> IOResultOr<()> {
        // Fast path: btree not allocated
        if !self.is_btree_allocated() {
            self.dual_peek.btree_peek = CursorPeek::Exhausted;
            self.state = None;
            return Ok(IOResult::Done(()));
        }

        loop {
            let Some(MvccLazyCursorState::Seek(SeekState::SeekBtree(btree_seek_state), direction)) =
                self.state.clone()
            else {
                panic!(
                    "Invalid btree seek state in seek_btree_and_set_peek: {:?}",
                    self.state
                );
            };
            match btree_seek_state {
                SeekBtreeState::SeekBtree => {
                    let seek_result = return_if_io!(self.btree_cursor.seek(seek_key.clone(), op));

                    match seek_result {
                        SeekResult::NotFound => {
                            self.dual_peek.btree_peek = CursorPeek::Exhausted;
                            return Ok(IOResult::Done(()));
                        }
                        SeekResult::TryAdvance => {
                            // Need to advance to find actual matching entry
                            self.state.replace(MvccLazyCursorState::Seek(
                                SeekState::SeekBtree(SeekBtreeState::AdvanceBTree),
                                direction,
                            ));
                            inject_io_yield!(self, CursorYieldPoint::SeekBtreeProgress);
                        }
                        SeekResult::Found => {
                            self.state.replace(MvccLazyCursorState::Seek(
                                SeekState::SeekBtree(SeekBtreeState::CheckRow),
                                direction,
                            ));
                            inject_io_yield!(self, CursorYieldPoint::SeekBtreeProgress);
                        }
                    }
                }
                SeekBtreeState::AdvanceBTree => {
                    return_if_io!(match direction {
                        IterationDirection::Forwards => {
                            self.advance_btree_forward_from_current()
                        }
                        IterationDirection::Backwards => {
                            self.advance_btree_backward_from_current()
                        }
                    });
                    self.state.replace(MvccLazyCursorState::Seek(
                        SeekState::SeekBtree(SeekBtreeState::CheckRow),
                        direction,
                    ));
                    inject_io_yield!(self, CursorYieldPoint::SeekBtreeProgress);
                }
                SeekBtreeState::CheckRow => {
                    let key = self.get_btree_current_key()?;
                    match key {
                        Some(k) if self.query_btree_version_is_valid(&k)? => {
                            self.dual_peek.btree_peek = CursorPeek::Row {
                                key: k,
                                versions: None,
                            };
                            return Ok(IOResult::Done(()));
                        }
                        Some(_) => {
                            // shadowed by MVCC, continue to next
                            self.state.replace(MvccLazyCursorState::Seek(
                                SeekState::SeekBtree(SeekBtreeState::AdvanceBTree),
                                direction,
                            ));
                            inject_io_yield!(self, CursorYieldPoint::SeekBtreeProgress);
                        }
                        None => {
                            self.dual_peek.btree_peek = CursorPeek::Exhausted;
                            return Ok(IOResult::Done(()));
                        }
                    }
                }
            }
        }
    }

    /// Initialize MVCC iterator for forward iteration (used when next() is called without rewind())
    fn init_mvcc_iterator_forward(&mut self) -> Result<(), TryReserveError> {
        if self.table_iterator.is_some() || self.index_iterator.is_some() {
            return Ok(()); // Already initialized
        }
        match &self.mv_cursor_type {
            MvccCursorType::Table => {
                let start_rowid = RowID {
                    table_id: self.table_id,
                    row_id: RowKey::Int(i64::MIN),
                };
                self.table_iterator = Some(self.db.table_rows_iterator(
                    Bound::Included(start_rowid),
                    IterationDirection::Forwards,
                ));
            }
            MvccCursorType::Index(_) => {
                let index_rows = self.db.get_or_create_index_rows(self.table_id)?;
                let index_rows = index_rows.value();
                let iter_box: Box<
                    dyn Iterator<Item = MvccEntry<'_, Arc<SortableIndexKey>, A>> + Send + Sync,
                > = Box::new(index_rows.iter());
                self.index_iterator =
                    Some(static_iterator_hack!(iter_box, Arc<SortableIndexKey>, A));
            }
        }
        Ok(())
    }
}

impl<Clock: LogicalClock + 'static, A: ConcurrentAllocator> Drop for MvccLazyCursor<Clock, A> {
    fn drop(&mut self) {
        // Release the per-table RowidAllocator lock if a Statement was dropped
        // while paused at an op_new_rowid IO yield. end_new_rowid is a no-op
        // when creating_new_rowid is false, so this is safe in every case.
        self.end_new_rowid();
    }
}

impl<Clock: LogicalClock + 'static, A: ConcurrentAllocator> CursorTrait
    for MvccLazyCursor<Clock, A>
{
    fn last(&mut self) -> IOResultOr<()> {
        // A cursor may be NullRow'd during outer-join unmatched emission.
        // Repositioning to a real row must clear that synthetic NULL state.
        self.set_null_flag(false);
        let state = self.state.clone();
        if state.is_none() {
            self.reset_table_scan();
            let _ = self.table_iterator.take();
            let _ = self.index_iterator.take();
            self.reset_dual_peek();
            self.state
                .replace(MvccLazyCursorState::Rewind(RewindState::Advance));
        }

        turso_assert!(
            matches!(
                self.state
                    .as_ref()
                    .expect("rewind state is not initialized"),
                MvccLazyCursorState::Rewind(RewindState::Advance)
            ),
            "invalid last state",
            { "state": format!("{:?}", self.state) }
        );

        // Initialize btree cursor to last position
        return_if_io!(self.advance_btree_backward());

        self.invalidate_record();
        self.current_pos = CursorPosition::End;

        // Initialize MVCC iterator to last position
        match &self.mv_cursor_type {
            MvccCursorType::Table => match self.db.get_last_table_rowid(
                self.table_id,
                &mut self.table_iterator,
                self.snapshot.tx_id,
            ) {
                Some(k) => {
                    tracing::trace!("last: mvcc_key: {:?}", k);
                    self.dual_peek.mvcc_peek = CursorPeek::Row {
                        key: k,
                        versions: None,
                    };
                }
                None => {
                    self.dual_peek.mvcc_peek = CursorPeek::Exhausted;
                }
            },
            MvccCursorType::Index(_) => match self.db.get_last_index_rowid(
                self.table_id,
                self.snapshot.tx_id,
                &mut self.index_iterator,
            )? {
                Some(k) => {
                    self.dual_peek.mvcc_peek = CursorPeek::Row {
                        key: k,
                        versions: None,
                    };
                }
                None => {
                    self.dual_peek.mvcc_peek = CursorPeek::Exhausted;
                }
            },
        };

        self.refresh_current_position(IterationDirection::Backwards);
        self.invalidate_record();
        self.state = None;

        Ok(IOResult::Done(()))
    }

    /// Move the cursor to the next row. Returns true if the cursor moved to the next row, false if the cursor is at the end of the table.
    fn next(&mut self) -> IOResultOr<()> {
        if matches!(self.last_move, LastMove::Prev | LastMove::ExactSeek) {
            return self.seek_past_current_row(IterationDirection::Forwards);
        }
        if self.state.is_none() {
            // If BeforeFirst and peek not initialized, initialize the iterators and peek values
            if matches!(self.current_pos, CursorPosition::BeforeFirst) {
                let uninitialized = self.dual_peek.both_uninitialized();
                if uninitialized {
                    // Initialize MVCC iterator and get first peek
                    self.index_peek_epoch = self.db.index_rows_epoch();
                    self.init_mvcc_iterator_forward()?;
                    self.advance_mvcc_iterator();
                    self.state
                        .replace(MvccLazyCursorState::Next(NextState::AdvanceUnitialized));
                } else {
                    self.state
                        .replace(MvccLazyCursorState::Next(NextState::CheckNeedsAdvance));
                }
                inject_io_yield!(self, CursorYieldPoint::NextStart);
            } else {
                self.state
                    .replace(MvccLazyCursorState::Next(NextState::CheckNeedsAdvance));
                inject_io_yield!(self, CursorYieldPoint::NextStart);
            }
        }
        if self.table_scan.generation != self.transaction.write_generation() {
            return self.refresh_scan_after_write(IterationDirection::Forwards);
        }
        // If it was uninitialized, we need to advance the btree first
        if matches!(
            self.state.as_ref().expect("next state is not initialized"),
            MvccLazyCursorState::Next(NextState::AdvanceUnitialized)
        ) {
            return_if_io!(self.advance_btree_forward());
            self.state
                .replace(MvccLazyCursorState::Next(NextState::CheckNeedsAdvance));
        }

        if matches!(
            self.state.as_ref().expect("next state is not initialized"),
            MvccLazyCursorState::Next(NextState::CheckNeedsAdvance)
        ) {
            if self.can_step_table_row::<true>() {
                return self.step_table_row::<true>();
            }
            if self.can_step_version_row::<true>() {
                return self.step_version_row::<true>();
            }
            // Determine which cursor(s) need to be advanced based on current position
            let (need_advance_mvcc, need_advance_btree) = match &self.current_pos {
                CursorPosition::BeforeFirst => {
                    // First call after rewind - peek values should already be populated
                    // Just need to pick the smaller one
                    (false, false)
                }
                CursorPosition::Loaded {
                    row_id, in_btree, ..
                } => {
                    // Sorted-merge: if the other peek still holds the same key
                    // (GC made fallthrough valid under a live MVCC peek), advance
                    // both so we do not emit K twice.
                    let other_same_key = if *in_btree {
                        self.dual_peek.mvcc_peek.get_row_key() == Some(&row_id.row_id)
                    } else {
                        self.dual_peek.btree_peek.get_row_key() == Some(&row_id.row_id)
                    };
                    if *in_btree {
                        (other_same_key, true)
                    } else {
                        (true, other_same_key)
                    }
                }
                CursorPosition::End => {
                    self.state = None;
                    return Ok(IOResult::Done(()));
                }
            };

            // Advance cursors as needed and update peek state
            let reseeked = self.reseek_index_if_index_rows_changed(IterationDirection::Forwards)?;
            if need_advance_mvcc && !reseeked && !self.dual_peek.mvcc_exhausted() {
                self.advance_mvcc_iterator();
            }
            if need_advance_btree && !self.dual_peek.btree_exhausted() {
                self.state
                    .replace(MvccLazyCursorState::Next(NextState::Advance));
                inject_io_yield!(self, CursorYieldPoint::NextBtreeAdvance);
            }
        }

        if matches!(
            self.state.as_ref().expect("next state is not initialized"),
            MvccLazyCursorState::Next(NextState::Advance)
        ) {
            return_if_io!(self.advance_btree_forward());
        }

        self.finish_table_step::<true>()
    }

    /// Move the cursor to the previous row. Returns true if the cursor moved, false if at the beginning.
    fn prev(&mut self) -> IOResultOr<()> {
        if matches!(self.last_move, LastMove::Next | LastMove::ExactSeek) {
            return self.seek_past_current_row(IterationDirection::Backwards);
        }
        if self.state.is_none() {
            // If End and peek not initialized, initialize via last()
            if matches!(self.current_pos, CursorPosition::End) {
                let uninitialized = self.dual_peek.both_uninitialized();
                if uninitialized {
                    self.state
                        .replace(MvccLazyCursorState::Prev(PrevState::AdvanceUnitialized));
                    return_if_io!(self.last());
                } else {
                    self.state
                        .replace(MvccLazyCursorState::Prev(PrevState::CheckNeedsAdvance));
                }
            } else {
                self.state
                    .replace(MvccLazyCursorState::Prev(PrevState::CheckNeedsAdvance));
            }
        }

        if self.table_scan.generation != self.transaction.write_generation() {
            return self.refresh_scan_after_write(IterationDirection::Backwards);
        }
        if matches!(
            self.state.as_ref().expect("prev state is not initialized"),
            MvccLazyCursorState::Prev(PrevState::AdvanceUnitialized)
        ) {
            return_if_io!(self.last());
            self.state
                .replace(MvccLazyCursorState::Prev(PrevState::CheckNeedsAdvance));
        }

        if matches!(
            self.state.as_ref().expect("prev state is not initialized"),
            MvccLazyCursorState::Prev(PrevState::CheckNeedsAdvance)
        ) {
            if self.can_step_table_row::<false>() {
                return self.step_table_row::<false>();
            }
            if self.can_step_version_row::<false>() {
                return self.step_version_row::<false>();
            }
            // Determine which cursor(s) need to be advanced based on current position
            let (need_advance_mvcc, need_advance_btree) = match &self.current_pos {
                CursorPosition::End => {
                    // First call after last() - peek values should already be populated
                    (false, false)
                }
                CursorPosition::Loaded {
                    row_id, in_btree, ..
                } => {
                    // Sorted-merge: if the other peek still holds the same key
                    // (GC made fallthrough valid under a live MVCC peek), advance
                    // both so we do not emit K twice.
                    let other_same_key = if *in_btree {
                        self.dual_peek.mvcc_peek.get_row_key() == Some(&row_id.row_id)
                    } else {
                        self.dual_peek.btree_peek.get_row_key() == Some(&row_id.row_id)
                    };
                    if *in_btree {
                        (other_same_key, true)
                    } else {
                        (true, other_same_key)
                    }
                }
                CursorPosition::BeforeFirst => {
                    self.state = None;
                    return Ok(IOResult::Done(()));
                }
            };

            // Advance cursors as needed and update peek state
            let reseeked =
                self.reseek_index_if_index_rows_changed(IterationDirection::Backwards)?;
            if need_advance_mvcc && !reseeked && !self.dual_peek.mvcc_exhausted() {
                self.advance_mvcc_iterator();
            }
            if need_advance_btree && !self.dual_peek.btree_exhausted() {
                self.state
                    .replace(MvccLazyCursorState::Prev(PrevState::Advance));
                inject_io_yield!(self, CursorYieldPoint::PrevBtreeAdvance);
            }
        }

        if matches!(
            self.state.as_ref().expect("prev state is not initialized"),
            MvccLazyCursorState::Prev(PrevState::Advance)
        ) {
            return_if_io!(self.advance_btree_backward());
        }
        self.finish_table_step::<false>()
    }

    fn rowid(&mut self) -> IOResultOr<Option<i64>> {
        if self.get_null_flag() {
            return Ok(IOResult::Done(None));
        }
        let rowid = match &self.current_pos {
            CursorPosition::Loaded {
                row_id,
                in_btree: _,
                ..
            } => match &row_id.row_id {
                RowKey::Int(id) => Some(*id),
                RowKey::Record(sortable_key) => {
                    // For index cursors, the rowid is stored in the last column of the index record
                    let MvccCursorType::Index(index_info) = &self.mv_cursor_type else {
                        panic!("RowKey::Record requires Index cursor type");
                    };
                    if index_info.has_rowid {
                        match sortable_key.key.last_value() {
                            Some(Ok(crate::types::ValueRef::Numeric(
                                crate::numeric::Numeric::Integer(rowid),
                            ))) => Some(rowid),
                            _ => {
                                crate::bail_parse_error!("Failed to parse rowid from index record")
                            }
                        }
                    } else {
                        crate::bail_parse_error!("Indexes without rowid are not supported in MVCC");
                    }
                }
            },
            CursorPosition::BeforeFirst => None,
            CursorPosition::End => None,
        };
        Ok(IOResult::Done(rowid))
    }

    fn record(&mut self) -> IOResultOr<Option<&crate::types::ImmutableRecord>> {
        self.current_row()
    }

    fn record_payload(&mut self) -> IOResultOr<Option<&[u8]>> {
        if self.get_null_flag() {
            return Ok(IOResult::Done(None));
        }
        if self.transaction.should_abort() {
            return Err(LimboError::CommitDependencyAborted.into());
        }
        match &self.current_pos {
            CursorPosition::Loaded { in_btree: true, .. } => self.btree_cursor.record_payload(),
            CursorPosition::Loaded { row_id, .. } => {
                let generation = self.transaction.write_generation();
                let read_at_this_position = self.payload_generation == generation
                    && !self.transaction.is_removed()
                    && self
                        .payload_row
                        .as_ref()
                        .is_some_and(|row| row.id == *row_id);
                if !read_at_this_position {
                    self.payload_row = self.read_mvcc_current_row()?.or_else(|| {
                        self.eq_seek_row
                            .as_ref()
                            .filter(|row| row.id == *row_id)
                            .cloned()
                    });
                    self.payload_generation = generation;
                    if self.payload_row.is_none() && self.transaction.should_abort() {
                        return Err(LimboError::CommitDependencyAborted.into());
                    }
                }
                Ok(IOResult::Done(self.payload_row.as_ref().map(Row::payload)))
            }
            CursorPosition::BeforeFirst | CursorPosition::End => Ok(IOResult::Done(None)),
        }
    }

    fn seek_unpacked(&mut self, registers: &[Register], op: SeekOp) -> IOResultOr<SeekResult> {
        let record = ImmutableRecord::from_registers(registers, registers.len())?;
        self.seek(SeekKey::IndexKey(record.as_record_ref()), op)
    }

    fn seek(&mut self, seek_key: SeekKey<'_>, op: SeekOp) -> IOResultOr<SeekResult> {
        // gt -> lower_bound bound excluded, we want first row after row_id
        // ge -> lower_bound bound included, we want first row equal to row_id or first row after row_id
        // lt -> upper_bound bound excluded, we want last row before row_id
        // le -> upper_bound bound included, we want last row equal to row_id or first row before row_id

        // Skip the seek and short-circuit to SeekResult::Found if the following are true:
        //
        // - the seek is eq_only
        // - the seek key matches at most one row (a table rowid, the whole index entry, or every
        //   column of a UNIQUE index with no NULL), so the current row is the first and only match
        // - the cursor is already correctly positioned on a visible version
        //
        // This is because in the situation where the following are true:
        //
        // - the loop's seek is a range seek (not eq_only, ex: `DELETE ... WHERE a > 1000`)
        // - the seek_key for the current iteration is in MvStore, but not in the b-tree
        // - some matching rows are b-tree-resident. This can happen if there are inserts, then a
        //   checkpoint (moving all previous rows to the b-tree), and then more inserts (only in MvStore).
        //
        // then the following problem could happen:
        //
        // 1. we seek to the first matching key using `SeekOp::GT { eq_only: false }`, so far so good.
        // 2. op_idx_delete forces a eq_only seek on the cursor.
        //    In the case of a delete using an index, this is redundant,
        //    because the delete loop works by seeking the index and then Insn::DeferredSeek'ing the
        //    table, so the index cursor is already correctly positioned.
        // 3. we seek the mvcc cursor (self) and find the row
        // 4. we seek btree_cursor, don't find the row, and set it to Exhausted immediately because
        //    it's an eq_only seek, EVEN THOUGH the seek from step 1 would still have matched rows
        //    in the b-tree.
        // 5. eventually, the mvcc cursor runs out. When this happens, since btree_cursor is already
        //    exhausted, current_pos becomes CursorPosition::End, and the next Insn::Next
        //    INCORRECTLY finds the index cursor exhausted and breaks out of the delete loop, even
        //    though there are still b-tree-resident rows to delete.
        if self.state.is_none() && op.eq_only() {
            if let CursorPosition::Loaded {
                row_id, in_btree, ..
            } = &self.current_pos
            {
                if current_pos_is_only_row_of_seek_key(
                    &row_id.row_id,
                    &seek_key,
                    &self.mv_cursor_type,
                )? {
                    let maybe_index_id = match &self.mv_cursor_type {
                        MvccCursorType::Index(_) => Some(self.table_id),
                        MvccCursorType::Table => None,
                    };
                    // Prefer the B-tree copy when this cursor is already on it:
                    // a stale SkipMap read here would hide later checkpointed
                    // updates and change scan totals.
                    let visible = if *in_btree {
                        self.query_btree_version_is_valid(&row_id.row_id)?
                    } else {
                        self.db
                            .read_from_table_or_index(self.snapshot.tx_id, row_id, maybe_index_id)?
                            .is_some()
                    };
                    if visible {
                        self.invalidate_record();
                        // We need to clear the null flag for the table cursor before seeking,
                        // because it might have been set to false by an unmatched left-join row
                        // during the previous iteration on the outer loop.
                        self.set_null_flag(false);
                        return Ok(IOResult::Done(SeekResult::Found));
                    }
                }
            }
        }

        loop {
            let state = self.state.clone();
            match state {
                None => {
                    self.reset_table_scan();
                    // Initial state: Reset and do MVCC seek
                    let _ = self.table_iterator.take();
                    let _ = self.index_iterator.take();
                    self.reset_dual_peek();
                    self.invalidate_record();
                    // We need to clear the null flag for the table cursor before seeking,
                    // because it might have been set to false by an unmatched left-join row
                    // during the previous iteration on the outer loop.
                    self.set_null_flag(false);

                    let direction = op.iteration_direction();
                    let inclusive = matches!(op, SeekOp::GE { .. } | SeekOp::LE { .. });

                    match &seek_key {
                        SeekKey::TableRowId(row_id) => {
                            let rowid = RowID {
                                table_id: self.table_id,
                                row_id: RowKey::Int(*row_id),
                            };

                            // Seek in MVCC (synchronous)
                            let mvcc_rowid = self.db.seek_rowid(
                                rowid.clone(),
                                inclusive,
                                op.eq_only(),
                                op.eq_only(),
                                direction,
                                self.snapshot,
                                &mut self.table_iterator,
                            );

                            // Set MVCC peek
                            {
                                self.dual_peek.mvcc_peek = match mvcc_rowid {
                                    Some((rid, versions, payload)) => {
                                        self.eq_seek_row = payload;
                                        CursorPeek::Row {
                                            key: rid.row_id,
                                            versions: Some(versions),
                                        }
                                    }
                                    None => CursorPeek::Exhausted,
                                };
                            }
                        }
                        SeekKey::IndexKey(index_key) => {
                            let index_info = {
                                let MvccCursorType::Index(index_info) = &self.mv_cursor_type else {
                                    panic!("SeekKey::IndexKey requires Index cursor type");
                                };
                                Arc::new(IndexInfo::new_in(
                                    index_info.key_info.iter().cloned(),
                                    index_info.has_rowid,
                                    index_key.column_count(),
                                    index_info.is_unique,
                                    self.db.allocator(),
                                )?)
                            };
                            let sortable_key = SortableIndexKey::new_from_payload_in(
                                index_key,
                                index_info,
                                self.db.allocator(),
                            )?;

                            // Seek in MVCC (synchronous)
                            let mvcc_rowid = self.db.seek_index(
                                self.table_id,
                                sortable_key.clone(),
                                inclusive,
                                op.eq_only(),
                                direction,
                                self.snapshot,
                                &mut self.index_iterator,
                            )?;

                            // Set MVCC peek
                            {
                                self.dual_peek.mvcc_peek = match mvcc_rowid {
                                    Some((rid, versions)) => CursorPeek::Row {
                                        key: rid.row_id,
                                        versions: Some(versions),
                                    },
                                    None => CursorPeek::Exhausted,
                                };
                            }
                        }
                    }

                    let seek_state = if matches!(seek_key, SeekKey::TableRowId(_))
                        && op.eq_only()
                        && self.eq_seek_row.is_some()
                    {
                        SeekState::PointBtree
                    } else {
                        SeekState::SeekBtree(SeekBtreeState::SeekBtree)
                    };
                    self.state
                        .replace(MvccLazyCursorState::Seek(seek_state, direction));
                    inject_io_yield!(self, CursorYieldPoint::SeekStart);
                }
                Some(MvccLazyCursorState::Seek(SeekState::PointBtree, direction)) => {
                    let found = self.is_btree_allocated()
                        && matches!(
                            return_if_io!(self.btree_cursor.seek(seek_key.clone(), op)),
                            SeekResult::Found
                        );
                    self.state.replace(MvccLazyCursorState::Seek(
                        if found {
                            SeekState::PointCheckBtreeFound
                        } else {
                            SeekState::PointCheckBtreeMissing
                        },
                        direction,
                    ));
                    inject_io_yield!(self, CursorYieldPoint::SeekBtreeProgress);
                }
                Some(MvccLazyCursorState::Seek(
                    point_state @ (SeekState::PointCheckBtreeFound
                    | SeekState::PointCheckBtreeMissing),
                    direction,
                )) => {
                    let found = matches!(point_state, SeekState::PointCheckBtreeFound);
                    let SeekKey::TableRowId(rowid) = seek_key else {
                        unreachable!("point seek requires a table rowid");
                    };
                    let row_id = RowID {
                        table_id: self.table_id,
                        row_id: RowKey::Int(rowid),
                    };
                    let physical_valid = found
                        && self.get_btree_current_key()?.as_ref() == Some(&row_id.row_id)
                        && self.query_btree_version_is_valid(&row_id.row_id)?;
                    let versions = self
                        .dual_peek
                        .mvcc_peek
                        .get_versions()
                        .expect("point seek requires a version chain");
                    let btree_has_newest_version = {
                        let chain = versions.read();
                        self.db.btree_covers_chain_for_snapshot(
                            self.snapshot,
                            self.table_id,
                            &chain,
                        )
                    };
                    if btree_has_newest_version && !physical_valid {
                        self.eq_seek_row = None;
                        self.dual_peek.mvcc_peek = CursorPeek::Exhausted;
                        self.state.replace(MvccLazyCursorState::Seek(
                            SeekState::SeekBtree(SeekBtreeState::SeekBtree),
                            direction,
                        ));
                        continue;
                    }
                    self.current_pos = CursorPosition::Loaded {
                        row_id,
                        in_btree: btree_has_newest_version,
                        versions: (!btree_has_newest_version).then_some(versions),
                    };
                    self.last_move = LastMove::ExactSeek;
                    self.state = None;
                    return Ok(IOResult::Done(SeekResult::Found));
                }
                Some(MvccLazyCursorState::Seek(SeekState::SeekBtree(_), direction)) => {
                    return_if_io!(self.seek_btree_and_set_peek(seek_key.clone(), op));
                    self.state
                        .replace(MvccLazyCursorState::Seek(SeekState::PickWinner, direction));
                    inject_io_yield!(self, CursorYieldPoint::SeekBtreeProgress);
                }
                Some(MvccLazyCursorState::Seek(SeekState::PickWinner, direction)) => {
                    // Pick winner and return result
                    // Now pick the winner based on direction
                    let winner_pos = self.position_from_peeks(direction);
                    self.state = None;
                    if let CursorPosition::Loaded {
                        row_id,
                        in_btree,
                        versions,
                    } = winner_pos
                    {
                        let winner_key = row_id.row_id.clone();
                        self.current_pos = CursorPosition::Loaded {
                            row_id,
                            in_btree,
                            versions,
                        };

                        if op.eq_only() {
                            // Check if the winner matches the seek key
                            let found = match &seek_key {
                                SeekKey::TableRowId(row_id) => winner_key == RowKey::Int(*row_id),
                                SeekKey::IndexKey(index_key) => {
                                    let RowKey::Record(found_key) = &winner_key else {
                                        panic!("Found rowid is not a record");
                                    };
                                    let MvccCursorType::Index(index_info) = &self.mv_cursor_type
                                    else {
                                        panic!("Index cursor expected");
                                    };
                                    let key_info: Vec<_> = index_info
                                        .key_info
                                        .iter()
                                        .take(index_key.column_count())
                                        .cloned()
                                        .collect();
                                    let cmp = compare_immutable(
                                        index_key.get_values()?,
                                        found_key.key.get_values()?,
                                        &key_info,
                                    );
                                    cmp.is_eq()
                                }
                            };
                            if found {
                                self.last_move = if self.table_iterator.is_none()
                                    && matches!(seek_key, SeekKey::TableRowId(_))
                                {
                                    LastMove::ExactSeek
                                } else {
                                    LastMove::Other
                                };
                                return Ok(IOResult::Done(SeekResult::Found));
                            } else {
                                return Ok(IOResult::Done(SeekResult::NotFound));
                            }
                        } else {
                            return Ok(IOResult::Done(SeekResult::Found));
                        }
                    } else {
                        // Nothing found in either cursor
                        let forwards = matches!(op, SeekOp::GE { .. } | SeekOp::GT);
                        if forwards {
                            self.current_pos = CursorPosition::End;
                        } else {
                            self.current_pos = CursorPosition::BeforeFirst;
                        }
                        return Ok(IOResult::Done(SeekResult::NotFound));
                    }
                }
                _ => {
                    panic!("Invalid state in seek: {:?}", self.state);
                }
            }
        }
    }

    /// Insert a row into the table or index.
    /// Sets the cursor to the inserted row.
    fn insert(&mut self, key: &BTreeKey) -> IOResultOr<()> {
        let row_id = match key {
            BTreeKey::TableRowId((rowid, _)) => RowID::new(self.table_id, RowKey::Int(*rowid)),
            BTreeKey::IndexKey(record) => {
                let MvccCursorType::Index(index_info) = &self.mv_cursor_type else {
                    panic!("BTreeKey::IndexKey requires Index cursor type");
                };
                let sortable_key = Arc::new(SortableIndexKey::new_from_payload_in(
                    record,
                    index_info.clone(),
                    self.db.allocator(),
                )?);
                RowID::new(self.table_id, RowKey::Record(sortable_key))
            }
        };
        let row = match &self.mv_cursor_type {
            MvccCursorType::Table => {
                let BTreeKey::TableRowId((_, record)) = key else {
                    return Err(LimboError::InternalError(
                        "Table cursor requires a TableRowId key".to_string(),
                    )
                    .into());
                };
                let record = record.as_ref().ok_or_else(|| {
                    LimboError::InternalError("TableRowId should have a record".to_string())
                })?;
                crate::with_mv_store_allocation_site!(
                    RowPayload,
                    Row::new_table_row_in(row_id, record.get_payload(), self.db.allocator(),)
                )
            }
            MvccCursorType::Index(_) => {
                let BTreeKey::IndexKey(_) = key else {
                    return Err(LimboError::InternalError(
                        "Index cursor requires an IndexKey".to_string(),
                    )
                    .into());
                };
                Ok(Row::new_index_row(row_id))
            }
        }?;

        // Check if the cursor is currently positioned at a B-tree row that matches
        // the row we're inserting. This indicates we're updating a B-tree-resident row
        // that doesn't yet have an MVCC version.
        let was_btree_resident = match &self.current_pos {
            CursorPosition::Loaded {
                row_id: current_row_id,
                in_btree,
                ..
            } => *in_btree && *current_row_id == row.id,
            _ => false,
        };

        self.save_position_before_write();
        self.current_pos = CursorPosition::Loaded {
            row_id: row.id.clone(),
            in_btree: was_btree_resident,
            versions: None,
        };
        let maybe_index_id = match &self.mv_cursor_type {
            MvccCursorType::Index(_) => Some(self.table_id),
            MvccCursorType::Table => None,
        };
        // FIXME: set btree to somewhere close to this rowid?
        if self
            .db
            .read_from_table_or_index(self.snapshot.tx_id, &row.id, maybe_index_id)?
            .is_some()
        {
            let updated = self
                .db
                .update_to_table_or_index(self.snapshot.tx_id, row, maybe_index_id)
                .inspect_err(|_| {
                    self.current_pos = CursorPosition::BeforeFirst;
                })?;
            turso_assert!(
                updated,
                "read found a visible version but update could not supersede it"
            );
        } else if was_btree_resident {
            // The row exists in B-tree but not in MvStore - mark it as B-tree resident
            // so that checkpoint knows to write deletes to the B-tree file.
            self.db
                .insert_btree_resident_to_table_or_index(self.snapshot.tx_id, row, maybe_index_id)
                .inspect_err(|_| {
                    self.current_pos = CursorPosition::BeforeFirst;
                })?;
        } else {
            self.db
                .insert_to_table_or_index(self.snapshot.tx_id, row, maybe_index_id)
                .inspect_err(|_| {
                    self.current_pos = CursorPosition::BeforeFirst;
                })?;
        }
        self.invalidate_record();
        Ok(IOResult::Done(()))
    }

    fn delete(&mut self) -> IOResultOr<()> {
        let (rowid, in_btree) = match self.get_current_pos() {
            CursorPosition::Loaded {
                row_id, in_btree, ..
            } => (row_id, in_btree),
            _ => panic!("Cannot delete: no current row"),
        };
        if in_btree {
            turso_assert!(
                self.is_btree_allocated(),
                "MVCC cursor marked current row as B-tree resident without an allocated B-tree",
                { "row_id": &rowid }
            );
        }
        let maybe_index_id = match &self.mv_cursor_type {
            MvccCursorType::Index(_) => Some(self.table_id),
            MvccCursorType::Table => None,
        };
        // If the cursor is positioned at a btree-resident row, the VDBE may never
        // have materialized the row's record (e.g. UPDATE through a DeferredSeek
        // never calls Column on the table cursor). Pre-fetch it before
        // `delete_from_table_or_index` so no I/O can interrupt its side effects.
        let btree_record = if in_btree {
            let Some(record) = return_if_io!(self.record()) else {
                crate::bail_corrupt_error!(
                    "Btree cursor should have a record when deleting a row that only exists in the btree"
                );
            };
            // Clone the record so the store call does not borrow the cursor.
            Some(record.clone())
        } else {
            None
        };
        self.db.delete_from_table_or_index(
            self.snapshot.tx_id,
            rowid,
            maybe_index_id,
            btree_record.as_ref(),
        )?;
        self.invalidate_record();
        Ok(IOResult::Done(()))
    }

    fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    fn get_null_flag(&self) -> bool {
        self.null_flag
    }

    fn exists(&mut self, key: &Value) -> IOResultOr<bool> {
        if self.state.is_none() {
            self.reset_table_scan();
            let _ = self.table_iterator.take();
            let _ = self.index_iterator.take();
            self.reset_dual_peek();
            self.invalidate_record();
            self.set_null_flag(false);
            let int_key = match key {
                Value::Numeric(crate::numeric::Numeric::Integer(i)) => i,
                _ => unreachable!("btree tables are indexed by integers!"),
            };
            let inclusive = true;

            let rowid = self.db.seek_rowid(
                RowID {
                    table_id: self.table_id,
                    row_id: RowKey::Int(*int_key),
                },
                inclusive,
                true,
                false,
                IterationDirection::Forwards,
                self.snapshot,
                &mut self.table_iterator,
            );

            let mvcc_versions = match rowid {
                Some((rowid, versions, _)) => {
                    let RowKey::Int(rowid) = rowid.row_id else {
                        panic!("Rowid is not an integer in mvcc table cursor");
                    };
                    (rowid == *int_key).then_some(versions)
                }
                None => None,
            };

            tracing::trace!(
                "MVCC exists check: mvcc_exists={} find={int_key}",
                mvcc_versions.is_some()
            );

            // If found in MVCC, update dual_peek and return true
            if let Some(versions) = mvcc_versions {
                self.dual_peek.mvcc_peek = CursorPeek::Row {
                    key: RowKey::Int(*int_key),
                    versions: Some(versions.clone()),
                };
                self.current_pos = CursorPosition::Loaded {
                    row_id: RowID {
                        table_id: self.table_id,
                        row_id: RowKey::Int(*int_key),
                    },
                    in_btree: false,
                    versions: Some(versions),
                };
                self.last_move = LastMove::ExactSeek;
                self.state = None;
                return Ok(IOResult::Done(true));
            }

            // MVCC doesn't have it, but we need to check B-tree too
            if self.is_btree_allocated() {
                // Check if the B-tree version is valid (not shadowed/deleted by MVCC)
                let btree_is_valid = self.query_btree_version_is_valid(&RowKey::Int(*int_key))?;

                // If B-tree is invalid (row is deleted or shadowed), don't check B-tree
                if !btree_is_valid {
                    self.current_pos = CursorPosition::End;
                    self.state = None;
                    return Ok(IOResult::Done(false));
                }
                self.state
                    .replace(MvccLazyCursorState::Exists(ExistsState::ExistsBtree));
                inject_io_yield!(self, CursorYieldPoint::ExistsBtreeFallback);
            } else {
                // No B-tree allocated, row doesn't exist
                self.current_pos = CursorPosition::End;
                self.state = None;
                return Ok(IOResult::Done(false));
            }
        }

        let Some(MvccLazyCursorState::Exists(ExistsState::ExistsBtree)) = self.state.clone() else {
            panic!("Invalid state {:?}", self.state);
        };
        turso_assert!(
            self.is_btree_allocated(),
            "BTree should be allocated when we are in ExistsBtree state"
        );

        // Check if row exists in B-tree
        let found = return_if_io!(self.btree_cursor.exists(key));

        if found {
            // Found in B-tree, but need to verify it's not shadowed by MVCC tombstone
            let int_key = match key {
                Value::Numeric(crate::numeric::Numeric::Integer(i)) => *i,
                _ => unreachable!("btree tables are indexed by integers!"),
            };
            let row_key = RowKey::Int(int_key);

            // Check if this B-tree row is shadowed (deleted/updated) in MVCC
            let is_valid = self.query_btree_version_is_valid(&row_key)?;

            if is_valid {
                // B-tree row is visible (not shadowed), update dual_peek
                self.dual_peek.btree_peek = CursorPeek::Row {
                    key: row_key.clone(),
                    versions: None,
                };
                self.current_pos = CursorPosition::Loaded {
                    row_id: RowID {
                        table_id: self.table_id,
                        row_id: row_key,
                    },
                    in_btree: true,
                    versions: None,
                };
                self.last_move = LastMove::ExactSeek;
                self.state = None;
                Ok(IOResult::Done(true))
            } else {
                // B-tree row is shadowed by MVCC (tombstone or update), so it doesn't exist
                tracing::trace!("B-tree row {int_key} is shadowed by MVCC");
                self.current_pos = CursorPosition::End;
                self.state = None;
                Ok(IOResult::Done(false))
            }
        } else {
            // Not found in B-tree either
            self.current_pos = CursorPosition::End;
            self.state = None;
            Ok(IOResult::Done(false))
        }
    }

    fn clear_btree(&mut self) -> IOResultOr<u64> {
        todo!()
    }

    fn btree_destroy(&mut self) -> IOResultOr<Option<usize>> {
        todo!()
    }

    fn count(&mut self) -> IOResultOr<usize> {
        loop {
            let state = self.count_state;
            match state {
                None => {
                    self.set_null_flag(false);
                    let generation = self.transaction.write_generation();
                    let start = RowID::new(self.table_id, RowKey::Int(i64::MIN));
                    let end = RowID::new(self.table_id, RowKey::Int(i64::MAX));
                    self.count_state = Some(
                        if matches!(self.mv_cursor_type, MvccCursorType::Table)
                            && self.is_btree_allocated()
                            && self.db.rows.range(start..=end).next().is_none()
                        {
                            self.btree_cursor.invalidate_btree_cache();
                            CountState::Btree { generation }
                        } else {
                            CountState::Rewind
                        },
                    );
                    if matches!(self.count_state, Some(CountState::Rewind)) {
                        inject_io_yield!(self, CursorYieldPoint::CountProgress);
                    }
                }
                Some(CountState::Btree { generation }) => {
                    if self.transaction.write_generation() != generation {
                        self.btree_cursor.invalidate_btree_cache();
                        self.count_state = Some(CountState::Rewind);
                        continue;
                    }
                    let count = return_if_io!(self.btree_cursor.count());
                    if self.transaction.write_generation() != generation {
                        self.btree_cursor.invalidate_btree_cache();
                        self.count_state = Some(CountState::Rewind);
                        continue;
                    }
                    inject_io_yield!(self, CursorYieldPoint::CountProgress);
                    self.current_pos = CursorPosition::End;
                    self.dual_peek = DualCursorPeek {
                        mvcc_peek: CursorPeek::Exhausted,
                        btree_peek: CursorPeek::Exhausted,
                    };
                    self.table_iterator = None;
                    self.eq_seek_row = None;
                    self.last_move = LastMove::Other;
                    self.state = None;
                    self.count_state = None;
                    self.invalidate_record();
                    return Ok(IOResult::Done(count));
                }
                Some(CountState::Rewind) => {
                    return_if_io!(self.rewind());
                    self.count_state
                        .replace(CountState::CheckBtreeKey { count: 0 });
                    inject_io_yield!(self, CursorYieldPoint::CountProgress);
                }
                Some(CountState::CheckBtreeKey { count }) => {
                    if let CursorPosition::Loaded {
                        row_id: _,
                        in_btree: _,
                        ..
                    } = &self.current_pos
                    {
                        self.count_state
                            .replace(CountState::NextBtree { count: count + 1 });
                        inject_io_yield!(self, CursorYieldPoint::CountProgress);
                    } else {
                        self.count_state = None;
                        return Ok(IOResult::Done(count));
                    }
                }
                Some(CountState::NextBtree { count }) => {
                    // advance the btree cursor skips non valid keys
                    return_if_io!(self.next());
                    self.count_state
                        .replace(CountState::CheckBtreeKey { count });
                    inject_io_yield!(self, CursorYieldPoint::CountProgress);
                }
            }
        }
    }

    /// Returns true if the is not pointing to any row.
    fn is_empty(&self) -> bool {
        // If we reached the end of the table, it means we traversed the whole table therefore there must be something in the table.
        // If we have loaded a row, it means there is something in the table.
        match &self.current_pos {
            CursorPosition::Loaded { .. } => false,
            CursorPosition::BeforeFirst => true,
            CursorPosition::End => true,
        }
    }

    fn root_page(&self) -> i64 {
        self.table_id.into()
    }

    fn rewind(&mut self) -> IOResultOr<()> {
        // A cursor may be NullRow'd during outer-join unmatched emission.
        // Repositioning to a real row must clear that synthetic NULL state.
        self.set_null_flag(false);
        let state = self.state.clone();
        if state.is_none() {
            self.reset_table_scan();
            let _ = self.table_iterator.take();
            let _ = self.index_iterator.take();
            self.reset_dual_peek();
            self.state
                .replace(MvccLazyCursorState::Rewind(RewindState::Advance));
        }

        turso_assert!(
            matches!(
                self.state
                    .as_ref()
                    .expect("rewind state is not initialized"),
                MvccLazyCursorState::Rewind(RewindState::Advance)
            ),
            "invalid rewind state",
            { "state": format!("{:?}", self.state) }
        );
        // First run btree_cursor rewind so that we don't need a explicit state machine.
        return_if_io!(self.advance_btree_forward());

        self.invalidate_record();
        self.current_pos = CursorPosition::BeforeFirst;

        // Initialize MVCC iterators for rewind operation; in practice there is only one of these
        // depending on the cursor type, so we should at some point refactor the iterator thing to be
        // generic over the type instead of having two on the struct.
        match &self.mv_cursor_type {
            MvccCursorType::Table => {
                // For table cursors, initialize iterator from the correct table id + i64::MIN;
                // this is because table rows from all tables are stored in the same map
                let start_rowid = RowID {
                    table_id: self.table_id,
                    row_id: RowKey::Int(i64::MIN),
                };
                let range = (
                    std::ops::Bound::Included(start_rowid),
                    std::ops::Bound::Unbounded,
                );
                let iter_box = Box::new(self.db.rows.range(range));
                self.table_iterator = Some(static_iterator_hack!(iter_box, RowID, A));
            }
            MvccCursorType::Index(_) => {
                // For index cursors, initialize the iterator to the beginning
                let index_rows = self.db.get_or_create_index_rows(self.table_id)?;
                let index_rows = index_rows.value();
                let iter_box: Box<
                    dyn Iterator<Item = MvccEntry<'_, Arc<SortableIndexKey>, A>> + Send + Sync,
                > = Box::new(index_rows.iter());
                self.index_iterator =
                    Some(static_iterator_hack!(iter_box, Arc<SortableIndexKey>, A));
            }
        }

        // Rewind mvcc iterator
        self.advance_mvcc_iterator();

        self.refresh_current_position(IterationDirection::Forwards);

        self.invalidate_record();
        self.state = None;
        Ok(IOResult::Done(()))
    }

    fn has_record(&self) -> bool {
        matches!(&self.current_pos, CursorPosition::Loaded { .. })
    }

    fn set_has_record(&mut self, _has_record: bool) {
        todo!()
    }

    fn index_info(&self) -> Option<&Arc<crate::types::IndexInfo>> {
        match &self.mv_cursor_type {
            MvccCursorType::Index(index_info) => Some(index_info),
            MvccCursorType::Table => None,
        }
    }

    fn seek_end(&mut self) -> IOResultOr<()> {
        if self.is_btree_allocated() {
            // Defer to btree cursor's seek_end implementation
            self.btree_cursor.seek_end()
        } else {
            // SkipMap inserts don't require cursor positioning because
            // SeekEnd instruction is only used for insertions.
            Ok(IOResult::Done(()))
        }
    }

    fn seek_to_last(&mut self) -> IOResultOr<()> {
        match self.seek(SeekKey::TableRowId(i64::MAX), SeekOp::LE { eq_only: false })? {
            IOResult::Done(_) => Ok(IOResult::Done(())),
            IOResult::IO(iocompletions) => Ok(IOResult::IO(iocompletions)),
        }
    }

    fn invalidate_record(&mut self) {
        self.clear_row_caches();
    }

    fn has_rowid(&self) -> bool {
        match &self.mv_cursor_type {
            MvccCursorType::Index(index_info) => index_info.has_rowid,
            MvccCursorType::Table => true, // currently we don't support WITHOUT ROWID tables
        }
    }

    fn get_pager(&self) -> Arc<Pager> {
        self.btree_cursor.get_pager()
    }

    fn get_skip_advance(&self) -> bool {
        todo!()
    }

    /// Returns true if this cursor operates in MVCC mode.
    fn is_mvcc(&self) -> bool {
        true
    }
}

impl<Clock: LogicalClock, A: ConcurrentAllocator> Debug for MvccLazyCursor<Clock, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MvccLazyCursor")
            .field("current_pos", &self.current_pos)
            .field("table_id", &self.table_id)
            .field("tx_id", &self.snapshot.tx_id)
            .field("reusable_immutable_record", &self.reusable_immutable_record)
            .field("btree_cursor", &())
            .finish()
    }
}

impl<Clock: LogicalClock + 'static, A: ConcurrentAllocator> MvccLazyCursor<Clock, A> {
    fn can_step_table_row<const FORWARD: bool>(&self) -> bool {
        let CursorPosition::Loaded {
            row_id,
            in_btree: true,
            ..
        } = &self.current_pos
        else {
            return false;
        };
        let RowKey::Int(rowid) = row_id.row_id else {
            return false;
        };
        matches!(self.mv_cursor_type, MvccCursorType::Table)
            && self.btree_readable
            && !self.transaction.is_removed()
            && !self.dual_peek.btree_exhausted()
            && self.dual_peek.mvcc_peek.get_row_key() != Some(&row_id.row_id)
            && self
                .table_walk
                .as_deref()
                .is_some_and(|walk| walk.continues(rowid, Self::direction::<FORWARD>()))
    }

    fn step_table_row<const FORWARD: bool>(&mut self) -> IOResultOr<()> {
        if FORWARD {
            self.state
                .replace(MvccLazyCursorState::Next(NextState::Advance));
            inject_io_yield!(self, CursorYieldPoint::NextBtreeAdvance);
        } else {
            self.state
                .replace(MvccLazyCursorState::Prev(PrevState::Advance));
            inject_io_yield!(self, CursorYieldPoint::PrevBtreeAdvance);
        }
        self.btree_advance_state = Some(AdvanceBtreeState::NextBtree);
        if FORWARD {
            inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeForwardProgress);
        } else {
            inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeBackwardProgress);
        }
        if FORWARD {
            return_if_io!(self.btree_cursor.next());
        } else {
            return_if_io!(self.btree_cursor.prev());
        }
        if !self.btree_cursor.has_record() {
            self.dual_peek.btree_peek = CursorPeek::Exhausted;
            self.btree_advance_state = None;
            return self.finish_table_step::<FORWARD>();
        }
        self.btree_advance_state = Some(AdvanceBtreeState::NextCheckBtreeKey);
        if FORWARD {
            inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeForwardProgress);
        } else {
            inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeBackwardProgress);
        }
        let rowid = loop {
            match self.btree_cursor.rowid()? {
                IOResult::Done(rowid) => break rowid,
                IOResult::IO(c) => c.wait(self.btree_cursor.get_pager().io.as_ref())?,
            }
        };
        let Some(rowid) = rowid else {
            return self.finish_step_with_merge::<FORWARD>(None);
        };
        let at_chain = self
            .table_walk
            .as_deref_mut()
            .expect("a table step needs the table chain walk")
            .advance_to(self.table_id, rowid, Self::direction::<FORWARD>());
        let version_row_first = match self.dual_peek.mvcc_peek.get_row_key() {
            Some(RowKey::Int(key)) if FORWARD => *key <= rowid,
            Some(RowKey::Int(key)) => *key >= rowid,
            _ => false,
        };
        if at_chain || version_row_first {
            return self.finish_step_with_merge::<FORWARD>(Some(RowKey::Int(rowid)));
        }
        #[cfg(debug_assertions)]
        self.debug_assert_table_row_validity(rowid, true);
        self.set_btree_peek(RowKey::Int(rowid));
        self.current_pos = CursorPosition::Loaded {
            row_id: RowID::new(self.table_id, RowKey::Int(rowid)),
            in_btree: true,
            versions: None,
        };
        self.clear_row_caches();
        self.state = None;
        Ok(IOResult::Done(()))
    }

    fn can_step_version_row<const FORWARD: bool>(&self) -> bool {
        let CursorPosition::Loaded {
            row_id,
            in_btree: false,
            ..
        } = &self.current_pos
        else {
            return false;
        };
        matches!(self.mv_cursor_type, MvccCursorType::Table)
            && !self.transaction.is_removed()
            && !self.dual_peek.mvcc_exhausted()
            && self.dual_peek.btree_peek.get_row_key() != Some(&row_id.row_id)
    }

    fn step_version_row<const FORWARD: bool>(&mut self) -> IOResultOr<()> {
        self.advance_mvcc_iterator();
        let CursorPeek::Row {
            key: RowKey::Int(rowid),
            versions: Some(versions),
        } = &self.dual_peek.mvcc_peek
        else {
            return self.finish_table_step::<FORWARD>();
        };
        let btree_first = match self.dual_peek.btree_peek.get_row_key() {
            Some(RowKey::Int(btree_rowid)) if FORWARD => btree_rowid <= rowid,
            Some(RowKey::Int(btree_rowid)) => btree_rowid >= rowid,
            _ => false,
        };
        if btree_first {
            return self.finish_table_step::<FORWARD>();
        }
        let btree_has_newest_version = {
            let chain = versions.read();
            self.db
                .btree_covers_chain_for_snapshot(self.snapshot, self.table_id, &chain)
        };
        if btree_has_newest_version {
            return self.finish_table_step::<FORWARD>();
        }
        self.current_pos = CursorPosition::Loaded {
            row_id: RowID::new(self.table_id, RowKey::Int(*rowid)),
            in_btree: false,
            versions: Some(versions.clone()),
        };
        self.clear_row_caches();
        self.state = None;
        Ok(IOResult::Done(()))
    }

    fn finish_step_with_merge<const FORWARD: bool>(
        &mut self,
        key: Option<RowKey>,
    ) -> IOResultOr<()> {
        if !self.peek_btree_key::<FORWARD>(key)? {
            if FORWARD {
                return_if_io!(self.advance_btree_forward());
            } else {
                return_if_io!(self.advance_btree_backward());
            }
        }
        self.finish_table_step::<FORWARD>()
    }

    fn finish_table_step<const FORWARD: bool>(&mut self) -> IOResultOr<()> {
        self.refresh_current_position(Self::direction::<FORWARD>());
        self.invalidate_record();
        self.state = None;
        Ok(IOResult::Done(()))
    }

    fn direction<const FORWARD: bool>() -> IterationDirection {
        if FORWARD {
            IterationDirection::Forwards
        } else {
            IterationDirection::Backwards
        }
    }

    fn peek_btree_key<const FORWARD: bool>(&mut self, key: Option<RowKey>) -> Result<bool> {
        if let Some(key) = key {
            let valid = if FORWARD {
                self.btree_row_is_valid_forward(&key)?
            } else {
                self.btree_row_is_valid_backward(&key)?
            };
            if valid {
                self.set_btree_peek(key);
                return Ok(true);
            }
            // Row is shadowed by MVCC, continue scanning
            // FIXME: do we want to iterate over all shadowed rows? If every row is shadowed by MVCC, we will iterate the whole btree in a single `next`/`prev` call
            self.btree_advance_state = Some(AdvanceBtreeState::NextBtree);
            Ok(false)
        } else {
            self.dual_peek.btree_peek = CursorPeek::Exhausted;
            self.btree_advance_state = None;
            Ok(true)
        }
    }

    fn btree_row_is_valid_backward(&mut self, key: &RowKey) -> Result<bool> {
        match key {
            RowKey::Int(rowid) => self.table_row_is_valid(*rowid, IterationDirection::Backwards),
            RowKey::Record(_) => self.query_btree_version_is_valid(key),
        }
    }

    fn table_row_is_valid(&mut self, rowid: i64, direction: IterationDirection) -> Result<bool> {
        if self.transaction.is_removed() {
            let valid = self.db.cursor_btree_row_is_valid(
                &self.transaction,
                self.table_id,
                &RowKey::Int(rowid),
                self.snapshot.tx_id,
            )?;
            #[cfg(debug_assertions)]
            self.debug_assert_table_row_validity(rowid, valid);
            return Ok(valid);
        }
        let db = self.db.as_ref();
        let slot = &mut self.table_walk;
        match slot.as_deref_mut() {
            Some(walk) if walk.continues(rowid, direction) => {}
            Some(walk) => *walk = TableChainWalk::start(db, self.table_id, rowid, direction),
            None => {
                *slot = Some(Box::new(TableChainWalk::start(
                    db,
                    self.table_id,
                    rowid,
                    direction,
                )))
            }
        }
        let at_chain = self
            .table_walk
            .as_deref_mut()
            .expect("table chain walk starts above")
            .advance_to(self.table_id, rowid, direction);
        let valid = if at_chain {
            let versions = self
                .table_walk
                .as_deref()
                .expect("table chain walk starts above")
                .head_versions()
                .read();
            versions.is_empty()
                || self.db.cursor_chain_leaves_btree_row_valid(
                    &self.transaction,
                    self.table_id,
                    &versions,
                    self.snapshot.tx_id,
                )?
        } else {
            true
        };
        #[cfg(debug_assertions)]
        self.debug_assert_table_row_validity(rowid, valid);
        Ok(valid)
    }

    fn set_btree_peek(&mut self, key: RowKey) {
        self.dual_peek.btree_peek = CursorPeek::Row {
            key,
            versions: None,
        };
        self.btree_advance_state = None;
    }

    fn clear_row_caches(&mut self) {
        if let Some(record) = self.reusable_immutable_record.as_mut() {
            record.invalidate();
        }
        self.payload_row = None;
    }

    #[cfg(debug_assertions)]
    fn debug_assert_table_row_validity(&self, rowid: i64, valid: bool) {
        let expected = self.db.cursor_btree_row_is_valid(
            &self.transaction,
            self.table_id,
            &RowKey::Int(rowid),
            self.snapshot.tx_id,
        );
        debug_assert!(
            matches!(expected, Ok(expected) if expected == valid),
            "table chain walk diverged from the point lookup at rowid {rowid}: \
             walk {valid}, point lookup {expected:?}"
        );
    }

    fn seek_past_current_row(&mut self, direction: IterationDirection) -> IOResultOr<()> {
        if self.table_scan.generation != self.transaction.write_generation() {
            return self.refresh_scan_after_write(direction);
        }
        self.table_walk = None;
        let forwards = matches!(direction, IterationDirection::Forwards);
        let result = match &self.current_pos {
            CursorPosition::Loaded {
                row_id:
                    RowID {
                        row_id: RowKey::Int(rowid),
                        ..
                    },
                ..
            } => {
                let op = if forwards { SeekOp::GT } else { SeekOp::LT };
                self.seek(SeekKey::TableRowId(*rowid), op)
                    .map(|result| result.map(|_| ()))
            }
            CursorPosition::BeforeFirst if forwards => self.rewind(),
            CursorPosition::End if !forwards => self.last(),
            CursorPosition::BeforeFirst | CursorPosition::End => Ok(IOResult::Done(())),
            CursorPosition::Loaded { .. } => {
                unreachable!("range continuation requires a table row position")
            }
        };
        self.last_move = match &result {
            Ok(IOResult::IO(_)) => LastMove::ExactSeek,
            Ok(IOResult::Done(_)) if forwards => LastMove::Next,
            Ok(IOResult::Done(_)) => LastMove::Prev,
            Err(_) => LastMove::Other,
        };
        result
    }

    #[cold]
    fn refresh_scan_after_write(&mut self, direction: IterationDirection) -> IOResultOr<()> {
        let current_generation = self.transaction.write_generation();
        if !matches!(self.mv_cursor_type, MvccCursorType::Table) || self.transaction.is_removed() {
            self.table_scan.generation = current_generation;
            return self.move_once(direction);
        }
        let table_generation = self.transaction.table_write_generation(self.table_id);
        if self.table_scan.resume.is_none() && table_generation <= self.table_scan.generation {
            self.table_scan.generation = current_generation;
            return self.move_once(direction);
        }
        self.table_walk = None;
        let scan_generation = self.table_scan.generation;
        let movement_pending = self.btree_advance_state.is_some()
            || matches!(
                self.state,
                Some(
                    MvccLazyCursorState::Seek(..)
                        | MvccLazyCursorState::Rewind(..)
                        | MvccLazyCursorState::Exists(..)
                        | MvccLazyCursorState::Next(NextState::AdvanceUnitialized)
                        | MvccLazyCursorState::Prev(PrevState::AdvanceUnitialized)
                )
            );
        if self.table_scan.resume.is_none() && !movement_pending {
            self.state = None;
        }
        let mut resume = match self.table_scan.resume {
            Some(resume) => resume,
            None => PendingScanRefresh {
                position: self
                    .table_scan
                    .position_before_write
                    .unwrap_or_else(|| self.current_scan_position()),
                phase: if movement_pending {
                    PendingScanPhase::FinishPendingMove
                } else {
                    PendingScanPhase::SeekPastPosition
                },
                table_generation,
            },
        };
        loop {
            let generation = self.transaction.write_generation();
            self.table_scan.generation = generation;
            let result = match resume.phase {
                PendingScanPhase::FinishPendingMove => {
                    self.current_pos = match resume.position {
                        ScanPosition::BeforeFirst => CursorPosition::BeforeFirst,
                        ScanPosition::At { rowid, in_btree } => CursorPosition::Loaded {
                            row_id: RowID::new(self.table_id, RowKey::Int(rowid)),
                            in_btree,
                            versions: None,
                        },
                        ScanPosition::End => CursorPosition::End,
                    };
                    self.move_once(direction)
                }
                PendingScanPhase::SeekPastPosition => match (resume.position, direction) {
                    (ScanPosition::At { rowid, .. }, IterationDirection::Forwards) => self
                        .seek(SeekKey::TableRowId(rowid), SeekOp::GT)
                        .map(|r| r.map(|_| ())),
                    (ScanPosition::At { rowid, .. }, IterationDirection::Backwards) => self
                        .seek(SeekKey::TableRowId(rowid), SeekOp::LT)
                        .map(|r| r.map(|_| ())),
                    (ScanPosition::BeforeFirst, IterationDirection::Forwards) => self.rewind(),
                    (ScanPosition::End, IterationDirection::Backwards) => self.last(),
                    (ScanPosition::BeforeFirst, IterationDirection::Backwards) => {
                        self.current_pos = CursorPosition::BeforeFirst;
                        Ok(IOResult::Done(()))
                    }
                    (ScanPosition::End, IterationDirection::Forwards) => {
                        self.current_pos = CursorPosition::End;
                        Ok(IOResult::Done(()))
                    }
                },
            };
            self.table_scan.generation = scan_generation;
            match result? {
                IOResult::IO(io) => {
                    self.table_scan.resume = Some(resume);
                    return Ok(IOResult::IO(io));
                }
                IOResult::Done(()) => match resume.phase {
                    PendingScanPhase::FinishPendingMove => {
                        resume.phase = PendingScanPhase::SeekPastPosition;
                        resume.table_generation =
                            self.transaction.table_write_generation(self.table_id);
                    }
                    PendingScanPhase::SeekPastPosition => {
                        let current = self.transaction.table_write_generation(self.table_id);
                        if current != resume.table_generation {
                            resume.table_generation = current;
                            continue;
                        }
                        self.table_scan = TableScan::new(self.transaction.write_generation());
                        self.last_move = match direction {
                            IterationDirection::Forwards => LastMove::Next,
                            IterationDirection::Backwards => LastMove::Prev,
                        };
                        return Ok(IOResult::Done(()));
                    }
                },
            }
        }
    }

    fn move_once(&mut self, direction: IterationDirection) -> IOResultOr<()> {
        match direction {
            IterationDirection::Forwards => self.next(),
            IterationDirection::Backwards => self.prev(),
        }
    }

    fn save_position_before_write(&mut self) {
        if matches!(self.mv_cursor_type, MvccCursorType::Table)
            && !self.dual_peek.both_uninitialized()
            && self.table_scan.position_before_write.is_none()
            && self.table_scan.resume.is_none()
        {
            self.table_scan.position_before_write = Some(self.current_scan_position());
        }
    }

    fn reset_table_scan(&mut self) {
        if !matches!(self.mv_cursor_type, MvccCursorType::Table) {
            return;
        }
        self.table_scan = TableScan::new(self.transaction.write_generation());
        self.table_walk = None;
    }

    fn current_scan_position(&self) -> ScanPosition {
        match &self.current_pos {
            CursorPosition::BeforeFirst => ScanPosition::BeforeFirst,
            CursorPosition::End => ScanPosition::End,
            CursorPosition::Loaded {
                row_id, in_btree, ..
            } => ScanPosition::At {
                rowid: row_id.row_id.to_int_or_panic(),
                in_btree: *in_btree,
            },
        }
    }
}

#[derive(Default)]
struct TableScan {
    generation: u64,
    position_before_write: Option<ScanPosition>,
    resume: Option<PendingScanRefresh>,
}

impl TableScan {
    fn new(generation: u64) -> Self {
        Self {
            generation,
            ..Self::default()
        }
    }
}

#[derive(Clone, Copy)]
enum ScanPosition {
    BeforeFirst,
    At { rowid: i64, in_btree: bool },
    End,
}

#[derive(Clone, Copy)]
struct PendingScanRefresh {
    position: ScanPosition,
    phase: PendingScanPhase,
    table_generation: u64,
}

#[derive(Clone, Copy)]
enum PendingScanPhase {
    FinishPendingMove,
    SeekPastPosition,
}

enum TableChainWalk<A: ConcurrentAllocator = TursoAllocator> {
    Peeked {
        direction: IterationDirection,
        checked: i64,
        rowid: i64,
        versions: RowVersions<A>,
        iter: MvccIterator<'static, RowID, A>,
    },
    Exhausted {
        direction: IterationDirection,
        checked: i64,
    },
}

impl<A: ConcurrentAllocator> TableChainWalk<A> {
    fn advance_to(
        &mut self,
        table_id: MVTableId,
        rowid: i64,
        direction: IterationDirection,
    ) -> bool {
        loop {
            match self {
                Self::Exhausted { checked, .. } => {
                    *checked = rowid;
                    return false;
                }
                Self::Peeked {
                    checked,
                    rowid: head,
                    ..
                } => {
                    *checked = rowid;
                    let behind = match direction {
                        IterationDirection::Forwards => *head < rowid,
                        IterationDirection::Backwards => *head > rowid,
                    };
                    if !behind {
                        return *head == rowid;
                    }
                }
            }
            let finished = Self::Exhausted {
                direction,
                checked: rowid,
            };
            let Self::Peeked { iter, .. } = std::mem::replace(self, finished) else {
                unreachable!("only a peeked walk falls behind")
            };
            *self = Self::step(iter, table_id, direction, rowid);
        }
    }

    fn continues(&self, rowid: i64, direction: IterationDirection) -> bool {
        let (Self::Peeked {
            direction: walk_direction,
            checked,
            ..
        }
        | Self::Exhausted {
            direction: walk_direction,
            checked,
        }) = self;
        *walk_direction == direction
            && match direction {
                IterationDirection::Forwards => rowid >= *checked,
                IterationDirection::Backwards => rowid <= *checked,
            }
    }

    fn start<Clock: LogicalClock>(
        db: &MvStore<Clock, A>,
        table_id: MVTableId,
        rowid: i64,
        direction: IterationDirection,
    ) -> Self {
        let iter = db.table_rows_iterator(
            Bound::Included(RowID::new(table_id, RowKey::Int(rowid))),
            direction,
        );
        Self::step(iter, table_id, direction, rowid)
    }

    fn step(
        mut iter: MvccIterator<'static, RowID, A>,
        table_id: MVTableId,
        direction: IterationDirection,
        checked: i64,
    ) -> Self {
        match iter.next() {
            Some(entry) if entry.key().table_id == table_id => Self::Peeked {
                direction,
                checked,
                rowid: entry.key().row_id.to_int_or_panic(),
                versions: entry.value().clone(),
                iter,
            },
            _ => Self::Exhausted { direction, checked },
        }
    }

    fn head_versions(&self) -> &RowVersions<A> {
        let Self::Peeked { versions, .. } = self else {
            unreachable!("only a walk at a chain key has versions")
        };
        versions
    }
}
