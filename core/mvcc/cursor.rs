use crate::alloc::{ConcurrentAllocator, TursoAllocator};
use crate::skiplist::{comparator::BasicComparator, map::Entry};
use crate::turso_assert;
use crate::types::IOResultOr;

use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{
    create_seek_range, ChainView, MVTableId, MvStore, MvccReadSnapshot, Row, RowID, RowKey,
    RowVersions, SortableIndexKey,
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
use crate::vdbe::metrics::{cursor_work, new_cursor_work_handle, CursorWorkHandle, CursorWorkRef};
use crate::vdbe::Register;
use crate::{return_if_io, Completion, Connection, LimboError, Pager, Result};
use std::any::Any;
use std::cmp::Ordering;
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
enum SeekState {
    /// The MvStore head is set. Seek the B-tree next.
    SeekBtree,
    /// Both heads are set. Walk them to the first visible row.
    Walk,
}

#[derive(Debug, Clone, Copy)]
enum CountState {
    Rewind,
    NextBtree { count: usize },
    CheckBtreeKey { count: usize },
}
#[derive(Debug, Clone)]
enum MvccLazyCursorState {
    /// `next`, `prev`, `rewind` or `last` walks the heads in this direction.
    Walk(IterationDirection),
    Exists(ExistsState),
    Seek(SeekState, IterationDirection),
}

#[cfg(any(test, injected_yields))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::EnumCount)]
#[repr(u8)]
pub enum CursorYieldPoint {
    NextStart,
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
            cursor_yield_key(self.tx_id, self.table_id),
        )
    }
}

fn current_pos_is_only_row_of_seek_key(
    current_row_id: &RowKey,
    seek_key: &SeekKey<'_>,
    mv_cursor_type: &MvccCursorType,
) -> Result<bool> {
    if !seek_key_names_one_row(seek_key, mv_cursor_type)? {
        return Ok(false);
    }
    Ok(match (current_row_id, seek_key, mv_cursor_type) {
        (RowKey::Int(current), SeekKey::TableRowId(target), MvccCursorType::Table) => {
            *current == *target
        }
        (RowKey::Record(current), SeekKey::IndexKey(target), MvccCursorType::Index(index_info)) => {
            compare_immutable(
                target.get_values_inline()?.iter(),
                current.key.get_values_inline()?.iter(),
                &index_info.key_info[..target.column_count()],
            )
            .is_eq()
        }
        _ => false,
    })
}

/// Whether at most one row can match the equality seek key `seek_key`.
fn seek_key_names_one_row(seek_key: &SeekKey<'_>, mv_cursor_type: &MvccCursorType) -> Result<bool> {
    Ok(match (seek_key, mv_cursor_type) {
        (SeekKey::TableRowId(_), MvccCursorType::Table) => true,
        (SeekKey::IndexKey(key), MvccCursorType::Index(index_info)) => {
            index_key_matches_at_most_one_row(&key.get_values_inline()?, index_info)
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

/// The cursor merges the rows of the MvStore and of the B-tree in key order. Each source has
/// a head: its next physical entry in the walk direction. The walk checks the visibility of a
/// head only when that head comes first. See [`MvccLazyCursor::walk`].
#[derive(Debug, Clone)]
struct DualCursorPeek<A: ConcurrentAllocator = TursoAllocator> {
    /// Head of the MvStore
    mvcc_peek: CursorPeek<A>,
    /// Head of the B-tree
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

/// Where the heads are relative to the current row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Heads {
    /// The heads are not the entries next to the current row: after a reposition, an insert
    /// at another key or an existence probe. A step seeks past the current row first.
    Detached,
    /// The heads are the entries next to the current row in this direction.
    Next(IterationDirection),
    /// As `Next`. Also, the walk that moved the cursor to the current row started at an edge
    /// (`rewind` or `last`), and the cursor did not move since. So every entry between that
    /// edge and the current row is hidden.
    NextFromEdge(IterationDirection),
}

impl Heads {
    /// The direction in which the heads are the entries next to the current row.
    fn dir(self) -> Option<IterationDirection> {
        match self {
            Heads::Detached => None,
            Heads::Next(dir) | Heads::NextFromEdge(dir) => Some(dir),
        }
    }
}

/// The bound of an equality seek.
#[derive(Debug, Clone)]
struct EqBound {
    /// The seek key. The walk returns no row past it, and leaves a source past it
    /// unpositioned (`CursorPeek::SeekPast`).
    key: RowKey,
    /// Whether at most one row has the key. The B-tree, which holds a committed state, then
    /// has at most one entry with the key.
    one_row: bool,
    /// Whether the bound also limits the steps after the seek. See
    /// [`MvccLazyCursor::keep_eq_bound`].
    kept: bool,
}

#[derive(Debug, Clone)]
enum CursorPeek<A: ConcurrentAllocator = TursoAllocator> {
    /// The source is not positioned. The walk starts it at the edge it walks from.
    Uninitialized,
    /// The source is on an entry that the cursor returned or found hidden. The walk steps
    /// the source once.
    Stale,
    /// The next entry of the source. Its visibility is not checked yet.
    Raw {
        key: RowKey,
        /// The MvStore chain of the entry. `None` for the B-tree.
        versions: Option<RowVersions<A>>,
    },
    /// The source has no more entries in the walk direction.
    Exhausted,
    /// The source is not positioned. The walk positions it at the first entry past this
    /// key in the walk direction. An equality seek leaves the B-tree like this when it does
    /// not need the B-tree, so that only a later step pays for the B-tree seek.
    SeekPast(RowKey),
}

impl<A: ConcurrentAllocator> Default for CursorPeek<A> {
    fn default() -> Self {
        Self::Uninitialized
    }
}

impl<A: ConcurrentAllocator> CursorPeek<A> {
    fn raw_key(&self) -> Option<&RowKey> {
        match self {
            CursorPeek::Raw { key, .. } => Some(key),
            _ => None,
        }
    }

    fn versions(&self) -> Option<RowVersions<A>> {
        match self {
            CursorPeek::Raw { versions, .. } => versions.clone(),
            _ => None,
        }
    }

    /// Marks the head stale if it is on `key`, so that the next walk steps past it.
    fn consume(&mut self, key: &RowKey) {
        if self.raw_key() == Some(key) {
            *self = CursorPeek::Stale;
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

/// Counts the work of an MvStore iterator that a cursor uses: one seek when it is made,
/// and one examined entry for each entry that it returns. All MvStore iterators of
/// cursors go through this function, so the counts do not depend on each call site.
/// In release builds it returns `iter` unchanged.
pub(crate) fn counted_mvcc_iterator<T: 'static, A: ConcurrentAllocator>(
    iter: MvccIterator<'static, T, A>,
    work: CursorWorkRef<'_>,
) -> MvccIterator<'static, T, A> {
    work.mvcc_seek();
    #[cfg(debug_assertions)]
    {
        Box::new(CountedMvccIterator {
            inner: iter,
            work: work.handle(),
        })
    }
    #[cfg(not(debug_assertions))]
    {
        iter
    }
}

/// See [`counted_mvcc_iterator`].
#[cfg(debug_assertions)]
struct CountedMvccIterator<I> {
    inner: I,
    work: CursorWorkHandle,
}

#[cfg(debug_assertions)]
impl<I: Iterator> Iterator for CountedMvccIterator<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<I::Item> {
        let item = self.inner.next();
        if item.is_some() {
            cursor_work(&self.work).mvcc_entry_examined();
        }
        item
    }
}

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
        work: CursorWorkRef<'_>,
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
                counted_mvcc_iterator(
                    static_iterator_hack!(iter_box, Arc<SortableIndexKey>, A),
                    work,
                )
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
    tx_id: u64,
    snapshot: MvccReadSnapshot,
    /// Reusable immutable record, used to allow better allocation strategy.
    reusable_immutable_record: Option<ImmutableRecord>,
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
    /// The heads of the merge walk. See [`Self::walk`].
    dual_peek: DualCursorPeek<A>,
    /// Where the heads are relative to `current_pos`.
    heads: Heads,
    /// The bound of the last equality seek. The seek ends with its bound in place, and the
    /// next step or seek drops it, unless [`Self::keep_eq_bound`] kept it.
    eq_bound: Option<EqBound>,
    /// The last key of the bounded range of the MvStore iterator, if the range is bounded.
    /// The MvStore head is `SeekPast` this key when the range ends. The range can last
    /// longer than `eq_bound`.
    mvcc_range_end: Option<RowKey>,
    /// Forward scan over `index_rows`; see [`IndexShadowScan`].
    index_shadow_scan: IndexShadowScan<A>,
    /// [`MvStore::table_rows_epoch`] or [`MvStore::index_rows_epoch`], by cursor
    /// type, read before the MVCC peek was taken.
    /// See [`Self::step`].
    mvcc_peek_epoch: u64,
    /// The work counters of the statement that opened the cursor.
    /// See [`crate::vdbe::metrics::CursorWork`].
    work: CursorWorkHandle,
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
        Ok(Self {
            db,
            #[cfg(any(test, injected_yields))]
            yield_instance_id: connection.next_yield_instance_id(),
            #[cfg(any(test, injected_yields))]
            connection: Arc::downgrade(connection),
            tx_id,
            snapshot,
            table_iterator: None,
            index_iterator: None,
            mv_cursor_type,
            current_pos: CursorPosition::BeforeFirst,
            table_id,
            reusable_immutable_record: None,
            eq_seek_row: None,
            btree_readable: false,
            btree_cursor,
            null_flag: false,
            creating_new_rowid: false,
            state: None,
            count_state: None,
            dual_peek: DualCursorPeek::default(),
            heads: Heads::Detached,
            eq_bound: None,
            mvcc_range_end: None,
            index_shadow_scan: IndexShadowScan::default(),
            mvcc_peek_epoch: 0,
            work: new_cursor_work_handle(),
        })
    }

    /// Counts the work of this cursor in `work`, the counters of the statement that opens it.
    pub(crate) fn with_work_counters(mut self, work: CursorWorkHandle) -> Self {
        self.work = work;
        self
    }

    /// Forward-direction shadow check: `IndexShadowScan` fast-path for index
    /// cursors, the authoritative per-row lookup for table cursors.
    fn btree_row_is_valid_forward(&mut self, key: &RowKey) -> Result<bool> {
        let RowKey::Record(rec) = key else {
            return self.query_btree_version_is_valid(key);
        };
        cursor_work(&self.work).visibility_check();
        let valid = self.index_shadow_scan.btree_row_is_valid(
            &self.db,
            self.table_id,
            self.tx_id,
            rec,
            cursor_work(&self.work),
        )?;
        // Debug-only cross-check: any scan divergence (e.g. a missed reset)
        // fails the test suite instead of shipping.
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            valid,
            self.db.query_btree_version_is_valid(
                self.table_id,
                &RowKey::Record(rec.clone()),
                self.tx_id
            )?,
            "index shadow scan diverged from query_btree_version_is_valid"
        );
        Ok(valid)
    }

    /// Returns the current row as an immutable record.
    pub fn current_row(&mut self) -> IOResultOr<Option<&crate::types::ImmutableRecord>> {
        if self.db.tx_should_abort(self.tx_id) {
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
                    match self
                        .db
                        .read_from_table_or_index(self.tx_id, row_id, maybe_index_id)?
                    {
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
                        if self.db.tx_should_abort(self.tx_id) {
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
            .read_from_table_or_index(self.tx_id, row_id, maybe_index_id)
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

    /// Keeps the bound of the equality seek that just ended for the steps after it, until
    /// the next reposition. The VDBE calls this after `SeekGE` and `SeekLE` with `eq_only`:
    /// the loop that follows ends at the first key past the seek key (as with
    /// `OPFLAG_SEEKEQ` in SQLite). So the cursor does not position a source past the key.
    /// Other equality seeks, such as the seek of IdxDelete, do not keep the bound, because a
    /// later step can continue past the key.
    pub fn keep_eq_bound(&mut self) {
        if let Some(bound) = &mut self.eq_bound {
            bound.kept = true;
        }
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
        cursor_work(&self.work).visibility_check();
        cursor_work(&self.work).mvcc_point_lookup();
        self.db
            .query_btree_version_is_valid(self.table_id, key, self.tx_id)
    }

    fn mvcc_rows_epoch(&self) -> u64 {
        match self.mv_cursor_type {
            MvccCursorType::Table => self.db.table_rows_epoch(),
            MvccCursorType::Index(_) => self.db.index_rows_epoch(),
        }
    }

    /// Moves the cursor one row in `dir`. This is the body of `next` and `prev`.
    fn step(&mut self, dir: IterationDirection) -> IOResultOr<()> {
        match &self.state {
            Some(MvccLazyCursorState::Seek(..)) => return self.seek_past_current_row(dir),
            Some(MvccLazyCursorState::Walk(walk_dir)) => {
                turso_assert!(
                    *walk_dir == dir,
                    "a step continues a walk in the other direction"
                );
            }
            Some(state) => panic!("invalid state for a step: {state:?}"),
            None => {
                self.drop_eq_bound_unless_kept();
                match &self.current_pos {
                    CursorPosition::End if dir == IterationDirection::Forwards => {
                        return Ok(IOResult::Done(()));
                    }
                    CursorPosition::BeforeFirst if dir == IterationDirection::Backwards => {
                        return Ok(IOResult::Done(()));
                    }
                    CursorPosition::BeforeFirst | CursorPosition::End
                        if matches!(self.heads, Heads::NextFromEdge(walk_dir) if walk_dir != dir)
                            && self.mvcc_rows_epoch() == self.mvcc_peek_epoch =>
                    {
                        // The walk from the other edge found no visible row, and nothing
                        // changed since. NewRowid relies on this when all rows are hidden: it
                        // reads the last row, finds none, and then steps forward.
                        self.current_pos = match dir {
                            IterationDirection::Forwards => CursorPosition::End,
                            IterationDirection::Backwards => CursorPosition::BeforeFirst,
                        };
                        return Ok(IOResult::Done(()));
                    }
                    CursorPosition::Loaded { .. }
                        if matches!(self.heads, Heads::NextFromEdge(walk_dir) if walk_dir != dir)
                            && self.mvcc_rows_epoch() == self.mvcc_peek_epoch =>
                    {
                        // The walk to this row started at the edge in `dir` and found every
                        // entry before this row hidden. Nothing changed since, so no row is
                        // past this row in `dir`. NewRowid relies on this: it reads the last
                        // row and then steps past it.
                        if let Heads::NextFromEdge(walk_dir) = self.heads {
                            self.heads = Heads::Next(walk_dir);
                        }
                        self.current_pos = match dir {
                            IterationDirection::Forwards => CursorPosition::End,
                            IterationDirection::Backwards => CursorPosition::BeforeFirst,
                        };
                        return Ok(IOResult::Done(()));
                    }
                    CursorPosition::Loaded { row_id, .. } => {
                        if self.heads.dir() != Some(dir) {
                            return self.seek_past_current_row(dir);
                        }
                        self.heads = Heads::Next(dir);
                        let current = row_id.row_id.clone();
                        self.dual_peek.mvcc_peek.consume(&current);
                        self.dual_peek.btree_peek.consume(&current);
                    }
                    // A step from the other edge starts at the first row in `dir`.
                    _ => self.start_at_edge(dir)?,
                }
                self.state = Some(MvccLazyCursorState::Walk(dir));
                inject_io_yield!(self, CursorYieldPoint::NextStart);
            }
        }
        if self.mvcc_rows_epoch() != self.mvcc_peek_epoch {
            // A write of this cursor's own transaction can add a key between the current row
            // and the MvStore head, or remove the head, and the MvStore iterator sees neither.
            // The write can come before the step, or while the step waits on IO.
            match &self.current_pos {
                CursorPosition::Loaded { row_id, .. } => {
                    let current = row_id.row_id.clone();
                    self.position_mvcc_head(Bound::Excluded(current), dir, None)?;
                }
                _ => self.position_mvcc_head(Bound::Unbounded, dir, None)?,
            }
        }
        return_if_io!(self.walk(dir));
        self.state = None;
        self.invalidate_record();
        Ok(IOResult::Done(()))
    }

    /// Drops the bound of the last equality seek, unless [`Self::keep_eq_bound`] kept it.
    fn drop_eq_bound_unless_kept(&mut self) {
        if self.eq_bound.as_ref().is_some_and(|bound| !bound.kept) {
            self.eq_bound = None;
        }
    }

    /// Lands on the MvStore head if it matches the equality seek in progress and is visible.
    /// Call it only when the seek key matches at most one row: the B-tree then has no other
    /// visible match, so the seek does not read the B-tree.
    fn land_on_visible_mvcc_head(&mut self, dir: IterationDirection) -> Result<bool> {
        let Some(key) = self.walkable_key(&self.dual_peek.mvcc_peek).cloned() else {
            return Ok(false);
        };
        let row = match self.view_mvcc_head()? {
            ChainView::Visible(row) => row,
            // The B-tree check of the same key does not need this chain, so the walk need
            // not check it again.
            ChainView::Hidden => {
                self.dual_peek.mvcc_peek = CursorPeek::Stale;
                return Ok(false);
            }
            ChainView::BtreeCovers => return Ok(false),
        };
        if row.is_some() {
            self.eq_seek_row = row;
        }
        let versions = self.dual_peek.mvcc_peek.versions();
        self.land(key, false, versions, dir);
        Ok(true)
    }

    /// Moves to the first visible row past the current row in `dir`, with a seek of both
    /// sources. A step does this when the heads are not the entries next to the current
    /// row: after an insert, an existence probe, an equality seek that missed the B-tree,
    /// or a change of direction.
    fn seek_past_current_row(&mut self, dir: IterationDirection) -> IOResultOr<()> {
        let CursorPosition::Loaded { row_id, .. } = &self.current_pos else {
            unreachable!("only a cursor on a row seeks past it");
        };
        // seek() sets current_pos only when it is done, so a call that continues the
        // seek after IO uses the same key.
        let key = row_id.row_id.clone();
        let op = match dir {
            IterationDirection::Forwards => SeekOp::GT,
            IterationDirection::Backwards => SeekOp::LT,
        };
        return_if_io!(match &key {
            RowKey::Int(rowid) => CursorTrait::seek(self, SeekKey::TableRowId(*rowid), op),
            RowKey::Record(record) => {
                CursorTrait::seek(self, SeekKey::IndexKey(record.key.clone()), op)
            }
        });
        self.invalidate_record();
        Ok(IOResult::Done(()))
    }

    /// Moves the cursor to the first row in `dir`. This is the body of `rewind` and `last`.
    fn move_to_edge(&mut self, dir: IterationDirection) -> IOResultOr<()> {
        if self.state.is_none() {
            // A cursor may be NullRow'd during outer-join unmatched emission.
            // Repositioning to a real row must clear that synthetic NULL state.
            self.set_null_flag(false);
            self.start_at_edge(dir)?;
            self.state = Some(MvccLazyCursorState::Walk(dir));
        } else if self.mvcc_rows_epoch() != self.mvcc_peek_epoch {
            // A write of this cursor's own transaction while the walk waited on IO can add
            // an MvStore version of a row that the MvStore iterator already passed. The walk
            // returned no row yet, so it can start the MvStore at the edge again.
            self.position_mvcc_head(Bound::Unbounded, dir, None)?;
        }
        turso_assert!(
            matches!(self.state, Some(MvccLazyCursorState::Walk(walk_dir)) if walk_dir == dir),
            "invalid state to move to an edge",
            { "state": format!("{:?}", self.state) }
        );
        return_if_io!(self.walk(dir));
        self.state = None;
        self.heads = Heads::NextFromEdge(dir);
        self.invalidate_record();
        Ok(IOResult::Done(()))
    }

    /// Puts both heads before the first entry of their source in `dir`.
    fn start_at_edge(&mut self, dir: IterationDirection) -> Result<()> {
        self.reset_dual_peek();
        self.position_mvcc_head(Bound::Unbounded, dir, None)
    }

    /// Moves to the first visible row in `dir` from the heads, and sets `current_pos`.
    ///
    /// The walk checks the visibility of only the head that comes first. If that head is
    /// hidden, only its source steps, and the other source stays where it is. So a walk
    /// examines only the entries before its result, plus one entry for each source. A head
    /// past the bound of an equality seek ends its source for the walk. All the state of
    /// the walk is in the heads, so a call that returned IO continues where it stopped.
    fn walk(&mut self, dir: IterationDirection) -> IOResultOr<()> {
        loop {
            // A head that is not positioned is past the bound of an equality seek, so a walk
            // under that bound does not need it.
            if matches!(self.dual_peek.mvcc_peek, CursorPeek::Stale) {
                self.step_mvcc_head();
            }
            // The step above can end a bounded range, so this check comes after it.
            if let (CursorPeek::SeekPast(key), None) = (&self.dual_peek.mvcc_peek, &self.eq_bound) {
                let key = key.clone();
                self.position_mvcc_head(Bound::Excluded(key), dir, None)?;
            }
            let btree_past_bound = self.eq_bound.is_some()
                && matches!(self.dual_peek.btree_peek, CursorPeek::SeekPast(_));
            if !btree_past_bound {
                return_if_io!(self.step_btree_head(dir));
            }
            let mvcc_key = self.walkable_key(&self.dual_peek.mvcc_peek).cloned();
            let btree_key = self.walkable_key(&self.dual_peek.btree_peek).cloned();
            let order = match (&mvcc_key, &btree_key) {
                (None, None) => {
                    self.current_pos = match dir {
                        IterationDirection::Forwards => CursorPosition::End,
                        IterationDirection::Backwards => CursorPosition::BeforeFirst,
                    };
                    self.heads = Heads::Next(dir);
                    return Ok(IOResult::Done(()));
                }
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some(mvcc_key), Some(btree_key)) => match dir {
                    IterationDirection::Forwards => mvcc_key.cmp(btree_key),
                    IterationDirection::Backwards => btree_key.cmp(mvcc_key),
                },
            };
            // A tie checks a B-tree row that has an MvStore chain. That needs the
            // transaction, so a read after the transaction ended stops here.
            if order.is_eq() {
                self.db.check_tx_exists(self.tx_id)?;
            }
            // When the heads tie, the MvStore chain decides which row the key has.
            let chain = if order.is_gt() {
                None
            } else {
                Some(self.view_mvcc_head()?)
            };
            match chain {
                Some(ChainView::Visible(row)) => {
                    if row.is_some() {
                        self.eq_seek_row = row;
                    }
                    let versions = self.dual_peek.mvcc_peek.versions();
                    let key = mvcc_key.expect("the MvStore head comes first");
                    self.land(key, false, versions, dir);
                    return Ok(IOResult::Done(()));
                }
                Some(ChainView::BtreeCovers) if order.is_eq() => {
                    let key = btree_key.expect("the heads tie");
                    self.land(key, true, None, dir);
                    return Ok(IOResult::Done(()));
                }
                // Only the MvStore has this key, and no version of it is visible.
                Some(_) if order.is_lt() => self.dual_peek.mvcc_peek = CursorPeek::Stale,
                _ => {
                    let key = btree_key.expect("the B-tree head comes first or ties");
                    let valid = if order.is_eq() {
                        self.tied_btree_head_is_valid()?
                    } else {
                        self.btree_head_is_valid(&key, dir)?
                    };
                    if valid {
                        self.land(key, true, None, dir);
                        return Ok(IOResult::Done(()));
                    }
                    // The B-tree has no other entry with a key that names one row.
                    self.dual_peek.btree_peek = match &self.eq_bound {
                        Some(bound) if bound.one_row => CursorPeek::SeekPast(bound.key.clone()),
                        _ => CursorPeek::Stale,
                    };
                    if order.is_eq() {
                        self.dual_peek.mvcc_peek = CursorPeek::Stale;
                    }
                }
            }
        }
    }

    /// The key of a head that the walk can return: a head past the bound of an equality
    /// seek ends its source for the walk.
    fn walkable_key<'p>(&self, peek: &'p CursorPeek<A>) -> Option<&'p RowKey> {
        let key = peek.raw_key()?;
        match &self.eq_bound {
            // For an index, the bound can be a prefix of the key. The comparison of
            // SortableIndexKey compares only the columns that both keys have.
            Some(bound) if !key.cmp(&bound.key).is_eq() => None,
            _ => Some(key),
        }
    }

    /// What this cursor sees in the chain of the MvStore head.
    fn view_mvcc_head(&self) -> Result<ChainView> {
        let CursorPeek::Raw {
            key,
            versions: Some(versions),
        } = &self.dual_peek.mvcc_peek
        else {
            unreachable!("the MvStore head holds an entry");
        };
        cursor_work(&self.work).visibility_check();
        let row_id = RowID {
            table_id: self.table_id,
            row_id: key.clone(),
        };
        self.db
            .cursor_view_of_chain(self.snapshot, &row_id, versions, self.take_eq_payload())
    }

    /// Whether the walk copies the visible version of a table row into `eq_seek_row`: only
    /// under the bound of a rowid seek. Passive GC can empty the chain before Column reads it.
    fn take_eq_payload(&self) -> bool {
        matches!(
            self.eq_bound,
            Some(EqBound {
                key: RowKey::Int(_),
                ..
            })
        )
    }

    /// Whether the B-tree head is visible when the MvStore head has the same key. The
    /// MvStore head holds the chain of that key, so no skiplist search is necessary.
    fn tied_btree_head_is_valid(&self) -> Result<bool> {
        let CursorPeek::Raw {
            versions: Some(versions),
            ..
        } = &self.dual_peek.mvcc_peek
        else {
            unreachable!("the MvStore head holds an entry");
        };
        cursor_work(&self.work).visibility_check();
        self.db
            .btree_row_valid_under_chain(self.tx_id, self.table_id, versions)
    }

    /// Whether the B-tree row `key` is visible: no MvStore version hides it.
    fn btree_head_is_valid(&mut self, key: &RowKey, dir: IterationDirection) -> Result<bool> {
        // A seek usually checks one or two B-tree rows. A point lookup is cheaper for them
        // than a new forward scan, which seeks the MvStore again.
        let seeking = matches!(self.state, Some(MvccLazyCursorState::Seek(..)));
        match dir {
            IterationDirection::Forwards if !seeking => self.btree_row_is_valid_forward(key),
            _ => self.query_btree_version_is_valid(key),
        }
    }

    fn land(
        &mut self,
        key: RowKey,
        in_btree: bool,
        versions: Option<RowVersions<A>>,
        dir: IterationDirection,
    ) {
        self.current_pos = CursorPosition::Loaded {
            row_id: RowID {
                table_id: self.table_id,
                row_id: key,
            },
            in_btree,
            versions,
        };
        self.heads = Heads::Next(dir);
    }

    /// Positions the MvStore iterator at the first entry from `start` in `dir`, and sets the
    /// MvStore head to that entry. It does not check the visibility of the entry. With `end`,
    /// the iterator stops after the entries that are equal to `end`.
    fn position_mvcc_head(
        &mut self,
        start: Bound<RowKey>,
        dir: IterationDirection,
        end: Option<RowKey>,
    ) -> Result<()> {
        self.mvcc_peek_epoch = self.mvcc_rows_epoch();
        self.table_iterator = None;
        self.index_iterator = None;
        self.mvcc_range_end.clone_from(&end);
        fn range<K: Ord>(
            start: Bound<K>,
            end: Option<K>,
            dir: IterationDirection,
        ) -> (Bound<K>, Bound<K>) {
            match (end, dir) {
                (Some(end), IterationDirection::Forwards) => (start, Bound::Included(end)),
                (Some(end), IterationDirection::Backwards) => (Bound::Included(end), start),
                (None, _) => create_seek_range(start, dir),
            }
        }
        match &self.mv_cursor_type {
            MvccCursorType::Table => {
                let table_id = self.table_id;
                let row_id = |row_id| RowID { table_id, row_id };
                let start = match start {
                    Bound::Included(key) => Bound::Included(row_id(key)),
                    Bound::Excluded(key) => Bound::Excluded(row_id(key)),
                    // All tables share one map, so the range starts at the edge of this table.
                    Bound::Unbounded => Bound::Included(row_id(RowKey::Int(match dir {
                        IterationDirection::Forwards => i64::MIN,
                        IterationDirection::Backwards => i64::MAX,
                    }))),
                };
                let range = range(start, end.map(row_id), dir);
                let iter_box: Box<dyn Iterator<Item = MvccEntry<'_, RowID, A>> + Send + Sync> =
                    match dir {
                        IterationDirection::Forwards => Box::new(self.db.rows.range(range)),
                        IterationDirection::Backwards => Box::new(self.db.rows.range(range).rev()),
                    };
                self.table_iterator = Some(counted_mvcc_iterator(
                    static_iterator_hack!(iter_box, RowID, A),
                    cursor_work(&self.work),
                ));
            }
            MvccCursorType::Index(_) => {
                let start = match start {
                    Bound::Included(RowKey::Record(key)) => Bound::Included(key.as_ref().clone()),
                    Bound::Excluded(RowKey::Record(key)) => Bound::Excluded(key.as_ref().clone()),
                    Bound::Unbounded => Bound::Unbounded,
                    _ => unreachable!("index keys are records"),
                };
                let end = end.map(|key| match key {
                    RowKey::Record(key) => key.as_ref().clone(),
                    RowKey::Int(_) => unreachable!("index keys are records"),
                });
                let range = range(start, end, dir);
                let index_rows = self.db.get_or_create_index_rows(self.table_id)?;
                let index_rows = index_rows.value();
                let iter_box: Box<
                    dyn Iterator<Item = MvccEntry<'_, Arc<SortableIndexKey>, A>> + Send + Sync,
                > = match dir {
                    IterationDirection::Forwards => Box::new(index_rows.range(range)),
                    IterationDirection::Backwards => Box::new(index_rows.range(range).rev()),
                };
                self.index_iterator = Some(counted_mvcc_iterator(
                    static_iterator_hack!(iter_box, Arc<SortableIndexKey>, A),
                    cursor_work(&self.work),
                ));
            }
        }
        self.step_mvcc_head();
        Ok(())
    }

    /// Sets the MvStore head to the next entry of the MvStore iterator.
    fn step_mvcc_head(&mut self) {
        let table_id = self.table_id;
        let end = match &self.mvcc_range_end {
            Some(key) => CursorPeek::SeekPast(key.clone()),
            None => CursorPeek::Exhausted,
        };
        self.dual_peek.mvcc_peek = match &self.mv_cursor_type {
            MvccCursorType::Table => {
                let iter = self
                    .table_iterator
                    .as_mut()
                    .expect("the MvStore iterator is positioned");
                match iter.next() {
                    // All tables share one map, so an entry of another table ends this one.
                    Some(entry) if entry.key().table_id == table_id => CursorPeek::Raw {
                        key: entry.key().row_id.clone(),
                        versions: Some(entry.value().clone()),
                    },
                    _ => end,
                }
            }
            MvccCursorType::Index(_) => {
                let iter = self
                    .index_iterator
                    .as_mut()
                    .expect("the MvStore iterator is positioned");
                match iter.next() {
                    Some(entry) => CursorPeek::Raw {
                        key: RowKey::Record(entry.key().clone()),
                        versions: Some(entry.value().clone()),
                    },
                    None => end,
                }
            }
        };
    }

    /// Moves the B-tree one entry in `dir` if its head is stale, or to its first entry in
    /// `dir` if its head is not positioned, and sets the B-tree head to that entry.
    fn step_btree_head(&mut self, dir: IterationDirection) -> IOResultOr<()> {
        loop {
            let from_edge = match &self.dual_peek.btree_peek {
                CursorPeek::Uninitialized => true,
                CursorPeek::Stale => false,
                CursorPeek::SeekPast(_) => {
                    return_if_io!(self.seek_btree_past_key(dir));
                    continue;
                }
                CursorPeek::Raw { .. } | CursorPeek::Exhausted => {
                    return Ok(IOResult::Done(()));
                }
            };
            if !self.is_btree_allocated() {
                self.dual_peek.btree_peek = CursorPeek::Exhausted;
                return Ok(IOResult::Done(()));
            }
            match (from_edge, dir) {
                (true, IterationDirection::Forwards) => return_if_io!(self.btree_cursor.rewind()),
                (true, IterationDirection::Backwards) => return_if_io!(self.btree_cursor.last()),
                (false, IterationDirection::Forwards) => return_if_io!(self.btree_cursor.next()),
                (false, IterationDirection::Backwards) => return_if_io!(self.btree_cursor.prev()),
            }
            if from_edge {
                cursor_work(&self.work).btree_seek();
            }
            self.set_btree_head_from_cursor()?;
            match dir {
                IterationDirection::Forwards => {
                    inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeForwardProgress)
                }
                IterationDirection::Backwards => {
                    inject_io_yield!(self, CursorYieldPoint::AdvanceBtreeBackwardProgress)
                }
            }
        }
    }

    /// Positions the B-tree at the first entry past the key of a `SeekPast` head in `dir`.
    fn seek_btree_past_key(&mut self, dir: IterationDirection) -> IOResultOr<()> {
        let CursorPeek::SeekPast(key) = &self.dual_peek.btree_peek else {
            unreachable!("the B-tree head is not positioned");
        };
        let key = key.clone();
        if !self.is_btree_allocated() {
            self.dual_peek.btree_peek = CursorPeek::Exhausted;
            return Ok(IOResult::Done(()));
        }
        let op = match dir {
            IterationDirection::Forwards => SeekOp::GT,
            IterationDirection::Backwards => SeekOp::LT,
        };
        let result = return_if_io!(match &key {
            RowKey::Int(rowid) => self.btree_cursor.seek(SeekKey::TableRowId(*rowid), op),
            RowKey::Record(record) => self
                .btree_cursor
                .seek(SeekKey::IndexKey(record.key.clone()), op),
        });
        cursor_work(&self.work).btree_seek();
        match result {
            SeekResult::Found => self.set_btree_head_from_cursor()?,
            SeekResult::TryAdvance => self.dual_peek.btree_peek = CursorPeek::Stale,
            SeekResult::NotFound => self.dual_peek.btree_peek = CursorPeek::Exhausted,
        }
        Ok(IOResult::Done(()))
    }

    /// Sets the B-tree head to the entry that the B-tree cursor is on.
    fn set_btree_head_from_cursor(&mut self) -> Result<()> {
        let key = if self.btree_cursor.has_record() {
            self.get_btree_current_key()?
        } else {
            None
        };
        self.dual_peek.btree_peek = match key {
            Some(key) => CursorPeek::Raw {
                key,
                versions: None,
            },
            None => CursorPeek::Exhausted,
        };
        Ok(())
    }

    /// Get the current key from btree cursor
    fn get_btree_current_key(&mut self) -> Result<Option<RowKey>> {
        cursor_work(&self.work).btree_row_examined();
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

    /// The key of `seek_key` as the cursor compares it with its heads.
    fn seek_row_key(&self, seek_key: &SeekKey<'_>) -> Result<RowKey> {
        Ok(match seek_key {
            SeekKey::TableRowId(row_id) => RowKey::Int(*row_id),
            SeekKey::IndexKey(index_key) => {
                let MvccCursorType::Index(index_info) = &self.mv_cursor_type else {
                    panic!("SeekKey::IndexKey requires Index cursor type");
                };
                let num_cols = index_key.column_count();
                // A key with all the columns of the index, such as the key that IdxInsert
                // seeks with, has the same IndexInfo as the cursor, so the cursor shares its
                // own.
                let index_info = if num_cols == index_info.num_cols {
                    index_info.clone()
                } else {
                    Arc::new(IndexInfo::new_in(
                        index_info.key_info.iter().cloned(),
                        index_info.has_rowid,
                        num_cols,
                        index_info.is_unique,
                        self.db.allocator(),
                    )?)
                };
                RowKey::Record(Arc::new(SortableIndexKey::new_from_payload_in(
                    index_key,
                    index_info,
                    self.db.allocator(),
                )?))
            }
        })
    }

    /// Lands an equality seek on the B-tree head without a seek of either source, when the
    /// last walk ended at the bound of the last equality seek and the B-tree head that it
    /// left is the one row of `seek_key`. A loop that probes a UNIQUE index in key order
    /// does this: each `Next` stops at the bound, and the next probe asks for the key of
    /// the B-tree head.
    fn seek_parked_btree_head(&mut self, seek_key: &SeekKey<'_>, op: SeekOp) -> Result<bool> {
        let dir = op.iteration_direction();
        let walk_ended = match dir {
            IterationDirection::Forwards => matches!(self.current_pos, CursorPosition::End),
            IterationDirection::Backwards => {
                matches!(self.current_pos, CursorPosition::BeforeFirst)
            }
        };
        // Only a walk under a bound leaves a B-tree head at the end: without a bound, the
        // walk returns that head.
        if !walk_ended
            || self.eq_bound.is_none()
            || self.heads != Heads::Next(dir)
            || self.mvcc_rows_epoch() != self.mvcc_peek_epoch
            || !matches!(self.dual_peek.btree_peek, CursorPeek::Raw { .. })
            || !seek_key_names_one_row(seek_key, &self.mv_cursor_type)?
        {
            return Ok(false);
        }
        let key = self.seek_row_key(seek_key)?;
        let Some(btree_key) = self.dual_peek.btree_peek.raw_key().cloned() else {
            return Ok(false);
        };
        // For an index, the seek key can be a prefix of the B-tree key. The comparison of
        // SortableIndexKey compares only the columns that both keys have.
        if !btree_key.cmp(&key).is_eq() || !self.query_btree_version_is_valid(&btree_key)? {
            return Ok(false);
        }
        self.set_null_flag(false);
        self.invalidate_record();
        self.eq_bound = Some(EqBound {
            key: key.clone(),
            one_row: true,
            kept: false,
        });
        // The MvStore has no visible row with this key, because the B-tree row is valid and
        // the key names one row. A step past the bound positions the MvStore after the key.
        self.dual_peek.mvcc_peek = CursorPeek::SeekPast(key);
        self.land(btree_key, true, None, dir);
        Ok(true)
    }

    /// Reset dual peek state (called on rewind/last/seek)
    fn reset_dual_peek(&mut self) {
        self.dual_peek = DualCursorPeek::default();
        self.table_iterator = None;
        self.index_iterator = None;
        self.heads = Heads::Detached;
        self.eq_bound = None;
        self.mvcc_peek_epoch = self.mvcc_rows_epoch();
        self.eq_seek_row = None;
        // The forward scan is monotonic; a reposition invalidates it.
        self.index_shadow_scan.reset();
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
        self.move_to_edge(IterationDirection::Backwards)
    }

    /// Move the cursor to the next row. Returns true if the cursor moved to the next row, false if the cursor is at the end of the table.
    fn next(&mut self) -> IOResultOr<()> {
        self.step(IterationDirection::Forwards)
    }

    /// Move the cursor to the previous row. Returns true if the cursor moved, false if at the beginning.
    fn prev(&mut self) -> IOResultOr<()> {
        self.step(IterationDirection::Backwards)
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

    fn seek_unpacked(&mut self, registers: &[Register], op: SeekOp) -> IOResultOr<SeekResult> {
        let record = ImmutableRecord::from_registers(registers, registers.len())?;
        self.seek(SeekKey::IndexKey(record.as_record_ref()), op)
    }

    fn seek(&mut self, seek_key: SeekKey<'_>, op: SeekOp) -> IOResultOr<SeekResult> {
        if self.state.is_none() {
            self.drop_eq_bound_unless_kept();
        }
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
                            .read_from_table_or_index(self.tx_id, row_id, maybe_index_id)?
                            .is_some()
                    };
                    if visible {
                        // We need to clear the null flag for the table cursor before seeking,
                        // because it might have been set to false by an unmatched left-join row
                        // during the previous iteration on the outer loop.
                        self.set_null_flag(false);
                        return Ok(IOResult::Done(SeekResult::Found));
                    }
                }
            }
        }

        if self.state.is_none() && op.eq_only() && self.seek_parked_btree_head(&seek_key, op)? {
            return Ok(IOResult::Done(SeekResult::Found));
        }

        loop {
            match self.state.clone() {
                None => {
                    self.reset_dual_peek();
                    self.invalidate_record();
                    // We need to clear the null flag for the table cursor before seeking,
                    // because it might have been set to false by an unmatched left-join row
                    // during the previous iteration on the outer loop.
                    self.set_null_flag(false);

                    let direction = op.iteration_direction();
                    let key = self.seek_row_key(&seek_key)?;
                    let mut end = None;
                    if op.eq_only() {
                        // An equality seek returns only rows with this key, so the walk
                        // stops at the first entry past it in each source.
                        self.eq_bound = Some(EqBound {
                            key: key.clone(),
                            one_row: seek_key_names_one_row(&seek_key, &self.mv_cursor_type)?,
                            kept: false,
                        });
                        end = Some(key.clone());
                    }
                    let start = match op {
                        SeekOp::GE { .. } | SeekOp::LE { .. } => Bound::Included(key),
                        SeekOp::GT | SeekOp::LT => Bound::Excluded(key),
                    };
                    self.position_mvcc_head(start, direction, end)?;
                    self.state = Some(MvccLazyCursorState::Seek(SeekState::SeekBtree, direction));
                    inject_io_yield!(self, CursorYieldPoint::SeekStart);
                }
                Some(MvccLazyCursorState::Seek(SeekState::SeekBtree, direction)) => {
                    if op.eq_only()
                        && self.eq_bound.as_ref().is_some_and(|bound| bound.one_row)
                        && self.land_on_visible_mvcc_head(direction)?
                    {
                        self.state = None;
                        // A later step positions the B-tree past this row.
                        let key = self.dual_peek.mvcc_peek.raw_key().cloned();
                        self.dual_peek.btree_peek =
                            CursorPeek::SeekPast(key.expect("the seek landed on the MvStore head"));
                        return Ok(IOResult::Done(SeekResult::Found));
                    }
                    if self.is_btree_allocated() {
                        let seek_result =
                            return_if_io!(self.btree_cursor.seek(seek_key.clone(), op));
                        cursor_work(&self.work).btree_seek();
                        match seek_result {
                            SeekResult::Found => self.set_btree_head_from_cursor()?,
                            // The B-tree is on the entry before the first match.
                            SeekResult::TryAdvance => {
                                self.dual_peek.btree_peek = CursorPeek::Stale;
                            }
                            // No B-tree entry has the key, so the B-tree is past it.
                            SeekResult::NotFound if op.eq_only() => {
                                let key = self.eq_bound.as_ref().map(|bound| bound.key.clone());
                                self.dual_peek.btree_peek = CursorPeek::SeekPast(
                                    key.expect("an equality seek sets its bound"),
                                );
                            }
                            SeekResult::NotFound => {
                                self.dual_peek.btree_peek = CursorPeek::Exhausted;
                            }
                        }
                    } else {
                        self.dual_peek.btree_peek = CursorPeek::Exhausted;
                    }
                    self.state = Some(MvccLazyCursorState::Seek(SeekState::Walk, direction));
                    inject_io_yield!(self, CursorYieldPoint::SeekBtreeProgress);
                }
                Some(MvccLazyCursorState::Seek(SeekState::Walk, direction)) => {
                    return_if_io!(self.walk(direction));
                    self.state = None;
                    return Ok(IOResult::Done(match self.current_pos {
                        CursorPosition::Loaded { .. } => SeekResult::Found,
                        _ => SeekResult::NotFound,
                    }));
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

        // An insert at another key moves the cursor away from the heads. An insert at the
        // current key ends the walk from an edge.
        let on_same_row =
            matches!(&self.current_pos, CursorPosition::Loaded { row_id, .. } if *row_id == row.id);
        self.heads = match self.heads {
            _ if !on_same_row => Heads::Detached,
            Heads::NextFromEdge(dir) => Heads::Next(dir),
            heads => heads,
        };
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
            .read_from_table_or_index(self.tx_id, &row.id, maybe_index_id)?
            .is_some()
        {
            let updated = self
                .db
                .update_to_table_or_index(self.tx_id, row, maybe_index_id)
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
                .insert_btree_resident_to_table_or_index(self.tx_id, row, maybe_index_id)
                .inspect_err(|_| {
                    self.current_pos = CursorPosition::BeforeFirst;
                })?;
        } else {
            self.db
                .insert_to_table_or_index(self.tx_id, row, maybe_index_id)
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
            self.tx_id,
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
            self.reset_dual_peek();
            self.invalidate_record();
            self.set_null_flag(false);
            let int_key = match key {
                Value::Numeric(crate::numeric::Numeric::Integer(i)) => i,
                _ => unreachable!("btree tables are indexed by integers!"),
            };
            let inclusive = true;

            // Check MVCC first. This is a point existence probe, so it is
            // eq-only: bound the skiplist walk to the single rowid instead of
            // scanning forward over invisible concurrent rows.
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
                cursor_work(&self.work),
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
                self.dual_peek.mvcc_peek = CursorPeek::Raw {
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
        cursor_work(&self.work).btree_seek();

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
                self.dual_peek.btree_peek = CursorPeek::Raw {
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
                    self.count_state.replace(CountState::Rewind);
                    inject_io_yield!(self, CursorYieldPoint::CountProgress);
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
        self.move_to_edge(IterationDirection::Forwards)
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
        return_if_io!(self.seek(SeekKey::TableRowId(i64::MAX), SeekOp::LE { eq_only: false }));
        // The walk of this seek started at the end of the table, as the walk of last() does.
        self.heads = Heads::NextFromEdge(IterationDirection::Backwards);
        Ok(IOResult::Done(()))
    }

    fn invalidate_record(&mut self) {
        if let Some(record) = self.reusable_immutable_record.as_mut() {
            record.invalidate();
        }
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
            .field("tx_id", &self.tx_id)
            .field("reusable_immutable_record", &self.reusable_immutable_record)
            .field("btree_cursor", &())
            .finish()
    }
}
