use crate::alloc::{ConcurrentAllocator, TryReserveError, TursoAllocator};
use crate::coro::{with_handle, BoxedResumable, Co, Runner, StepContext, YieldSlot};
use crate::skiplist::{comparator::BasicComparator, map::Entry};
use crate::turso_assert;
use crate::types::IOResultOr;

use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{
    create_seek_range, MVTableId, MvStore, Row, RowID, RowKey, RowVersions, SortableIndexKey,
};
#[cfg(any(test, injected_yields))]
use crate::mvcc::yield_hooks::{ProvidesYieldContext, YieldContext, YieldPointMarker};
use crate::storage::btree::{BTreeCursor, BTreeKey, CursorTrait};
use crate::sync::Arc;
use crate::translate::plan::IterationDirection;
use crate::types::{
    compare_immutable, IOCompletions, IOResult, ImmutableRecord, IndexInfo, SeekKey, SeekOp,
    SeekResult, Value,
};
use crate::vdbe::Register;
use crate::{return_if_io, Completion, Connection, LimboError, Pager, Result};
use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
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
            cursor_yield_key(self.tx_id, self.table_id),
        )
    }
}

fn current_pos_matches_seek_key(
    current_row_id: &RowKey,
    seek_key: &SeekKey<'_>,
    mv_cursor_type: &MvccCursorType,
) -> Result<bool> {
    Ok(match (current_row_id, seek_key) {
        (RowKey::Int(current), SeekKey::TableRowId(target)) => *current == *target,
        (RowKey::Record(current), SeekKey::IndexKey(target)) => {
            let MvccCursorType::Index(index_info) = mv_cursor_type else {
                return Ok(false);
            };
            let key_info: Vec<_> = index_info
                .key_info
                .iter()
                .take(target.column_count())
                .cloned()
                .collect();
            compare_immutable(target.get_values()?, current.key.get_values()?, &key_info).is_eq()
        }
        _ => false,
    })
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
        /// table iterator. `None` for btree peeks and index peeks.
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
    ) -> bool {
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
                IndexShadowScanState::Exhausted => return true,
                IndexShadowScanState::Uninitialized => unreachable!("created just above"),
                IndexShadowScanState::Peeked {
                    key: scan_key,
                    versions,
                    ..
                } => match scan_key.as_ref().cmp(key.as_ref()) {
                    // No version exactly at this key -> visible.
                    std::cmp::Ordering::Greater => return true,
                    // Version present at this key -> resolve the shadow bit now,
                    // on the one key that actually matches a B-tree row.
                    std::cmp::Ordering::Equal => {
                        return !db.index_chain_invalidates_btree(versions, tx_id);
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

/// Names [`MvCursorCtx`] as the context type of the async cursor operations.
struct MvCursorStep<Clock, A>(PhantomData<fn() -> (Clock, A)>);

impl<Clock: LogicalClock + 'static, A: ConcurrentAllocator> StepContext for MvCursorStep<Clock, A> {
    type Error = Box<LimboError>;
    type Ctx<'a> = MvCursorCtx<'a, Clock, A>;
}

/// The context of one step of an async cursor operation. The async function
/// gets it back on every step, so it never keeps a reference across a yield.
struct MvCursorCtx<'a, Clock: LogicalClock + 'static, A: ConcurrentAllocator> {
    cursor: &'a mut MvccLazyCursor<Clock, A>,
    args: CursorArgs<'a>,
    io: Option<IOCompletions>,
    err: Option<Box<LimboError>>,
}

impl<Clock: LogicalClock + 'static, A: ConcurrentAllocator> YieldSlot<Box<LimboError>>
    for MvCursorCtx<'_, Clock, A>
{
    fn park_io(&mut self, io: IOCompletions) {
        self.io = Some(io);
    }

    fn take_io(&mut self) -> Option<IOCompletions> {
        self.io.take()
    }

    fn park_err(&mut self, err: Box<LimboError>) {
        self.err = Some(err);
    }

    fn take_err(&mut self) -> Option<Box<LimboError>> {
        self.err.take()
    }
}
/// The borrowed arguments of one step. A future cannot hold a borrowed
/// seek key across a yield, so the caller passes it again on every step.
enum CursorArgs<'a> {
    None,
    Seek { key: SeekKey<'a> },
    Exists { key: &'a Value },
}

impl<'a> CursorArgs<'a> {
    #[inline(always)]
    fn seek_key(&self) -> SeekKey<'a> {
        match self {
            CursorArgs::Seek { key } => key.clone(),
            _ => unreachable!("this step has no seek key"),
        }
    }

    #[inline(always)]
    fn exists_key(&self) -> &'a Value {
        match self {
            CursorArgs::Exists { key } => key,
            _ => unreachable!("this step has no exists key"),
        }
    }
}

type CursorRunner<Clock, A, Args, Out> = BoxedResumable<MvCursorStep<Clock, A>, Args, Out>;

/// The runner of each async cursor operation, boxed on first use and reused.
struct CursorOps<Clock: LogicalClock + 'static, A: ConcurrentAllocator> {
    rewind: Option<CursorRunner<Clock, A, IterationDirection, ()>>,
    move_row: Option<CursorRunner<Clock, A, IterationDirection, ()>>,
    seek: Option<CursorRunner<Clock, A, SeekOp, SeekResult>>,
    exists: Option<CursorRunner<Clock, A, (), bool>>,
    count: Option<CursorRunner<Clock, A, (), usize>>,
}

impl<Clock: LogicalClock + 'static, A: ConcurrentAllocator> Default for CursorOps<Clock, A> {
    fn default() -> Self {
        Self {
            rewind: None,
            move_row: None,
            seek: None,
            exists: None,
            count: None,
        }
    }
}

/// What a move of the cursor still has to do after the MVCC peek advanced.
enum MoveStep {
    /// The cursor is already past the last row in this direction.
    AtEnd,
    /// The B-tree peek must advance before the cursor picks a row.
    AdvanceBtree,
    /// Both peeks are loaded: pick the next row from them.
    PickPosition,
}

/// The cursor operation that is suspended, if any. A cursor runs one
/// operation at a time: a call to another operation while one is suspended
/// is a bug in the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CursorOp {
    Rewind,
    Next,
    Prev,
    Seek,
    Exists,
    Count,
}

/// Runs one step of the async cursor operation `$op` whose runner lives in
/// `$slot`: starts it with `$args` when none is suspended and resumes it
/// otherwise. `$body` is the async function of the operation.
macro_rules! run_cursor_op {
    ($self:ident, $op:expr, $slot:ident, $body:path, $args:expr) => {
        run_cursor_op!($self, $op, $slot, $body, $args, CursorArgs::None)
    };
    ($self:ident, $op:expr, $slot:ident, $body:path, $args:expr, $ctx_args:expr) => {{
        turso_assert!(
            $self.active.is_none_or(|active| active == $op),
            "another cursor operation is suspended",
            { "active": format!("{:?}", $self.active), "op": format!("{:?}", $op) }
        );
        let mut runner = $self.ops.$slot.take().unwrap_or_else(|| {
            Runner::boxed(|co, args| {
                with_handle(co, args, async |co, args| $body(co, args).await)
            })
        });
        let mut ctx = MvCursorCtx {
            cursor: $self,
            args: $ctx_args,
            io: None,
            err: None,
        };
        let result = runner.resume(&mut ctx, $args);
        $self.active = if runner.is_active() { Some($op) } else { None };
        $self.ops.$slot = Some(runner);
        result
    }};
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
    /// Reusable immutable record, used to allow better allocation strategy.
    reusable_immutable_record: Option<ImmutableRecord>,
    /// Eq-only table seek copies the occupying payload under the version lock.
    /// Passive GC can empty the live chain before Column.
    eq_seek_row: Option<Row>,
    btree_cursor: Box<dyn CursorTrait>,
    null_flag: bool,
    creating_new_rowid: bool,
    /// The runners of the async operations of this cursor.
    ops: CursorOps<Clock, A>,
    /// The async operation that is suspended, if any.
    active: Option<CursorOp>,
    /// Dual-cursor peek state for proper iteration
    dual_peek: DualCursorPeek<A>,
    /// Forward scan over `index_rows`; see [`IndexShadowScan`].
    index_shadow_scan: IndexShadowScan<A>,
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
        let snapshot_ts = db.read_snapshot_ts(tx_id);
        let table_id = if connection.experimental_mvcc_passive_checkpoint_enabled() {
            // Under PASSIVE checkpointing a transaction can capture a schema cookie older than
            // the drop committed within its own snapshot (the drop publishes its cookie after
            // the transaction reads the header, even though the drop's commit ts precedes the
            // transaction's begin ts). The compiled cursor then points at a positive root page
            // its snapshot already sees dropped. That is a stale-schema read, not an invariant
            // violation: reprepare against the current schema instead of panicking.
            db.try_get_table_id_from_root_page_at(root_page_or_table_id, snapshot_ts)
                .ok_or(LimboError::SchemaUpdated)?
        } else {
            db.get_table_id_from_root_page_at(root_page_or_table_id, snapshot_ts)
        };
        Ok(Self {
            db,
            #[cfg(any(test, injected_yields))]
            yield_instance_id: connection.next_yield_instance_id(),
            #[cfg(any(test, injected_yields))]
            connection: Arc::downgrade(connection),
            tx_id,
            table_iterator: None,
            index_iterator: None,
            mv_cursor_type,
            current_pos: CursorPosition::BeforeFirst,
            table_id,
            reusable_immutable_record: None,
            eq_seek_row: None,
            btree_cursor,
            null_flag: false,
            creating_new_rowid: false,
            ops: CursorOps::default(),
            active: None,
            dual_peek: DualCursorPeek::default(),
            index_shadow_scan: IndexShadowScan::default(),
        })
    }

    /// Forward-direction shadow check: `IndexShadowScan` fast-path for index
    /// cursors, the authoritative per-row lookup for table cursors.
    fn btree_row_is_valid_forward(&mut self, key: &RowKey) -> bool {
        let RowKey::Record(rec) = key else {
            return self.query_btree_version_is_valid(key);
        };
        let valid =
            self.index_shadow_scan
                .btree_row_is_valid(&self.db, self.table_id, self.tx_id, rec);
        // Debug-only cross-check: any scan divergence (e.g. a missed reset)
        // fails the test suite instead of shipping.
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            valid,
            self.db.query_btree_version_is_valid(
                self.table_id,
                &RowKey::Record(rec.clone()),
                self.tx_id
            ),
            "index shadow scan diverged from query_btree_version_is_valid"
        );
        valid
    }

    /// Returns the current row as an immutable record.
    pub fn current_row(&mut self) -> IOResultOr<Option<&crate::types::ImmutableRecord>> {
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
                // Owned copies so the rest of the arm can mutably borrow the
                // reusable record.
                let row_id = row_id.clone();
                let versions = versions.clone();
                let snapshot = self.eq_seek_row.clone().filter(|row| row.id == row_id);

                let found = if let Some(versions) = &versions {
                    // Fast path: serialize the visible version straight into our
                    // reusable record — like the btree cursor does with a cell —
                    // instead of cloning a `Row` first.
                    if self.reusable_immutable_record.is_none() {
                        self.reusable_immutable_record = Some(ImmutableRecord::new(1024)?);
                    }
                    let record = self.reusable_immutable_record.as_mut().unwrap();
                    self.db
                        .read_visible_into_record(self.tx_id, versions, record)?
                } else {
                    // Cold fallback (seek-positioned, no cached chain): point
                    // lookup, then serialize.
                    let maybe_index_id = match &self.mv_cursor_type {
                        MvccCursorType::Index(_) => Some(self.table_id),
                        MvccCursorType::Table => None,
                    };
                    match self
                        .db
                        .read_from_table_or_index(self.tx_id, &row_id, maybe_index_id)?
                    {
                        Some(row) => {
                            let record = self.get_immutable_record_or_create()?;
                            record.invalidate();
                            record.start_serialization(row.payload())?;
                            true
                        }
                        None => false,
                    }
                };

                if !found {
                    if let Some(row) = snapshot {
                        let record = self.get_immutable_record_or_create()?;
                        record.invalidate();
                        record.start_serialization(row.payload())?;
                        let record_ref =
                            self.reusable_immutable_record.as_ref().ok_or_else(|| {
                                LimboError::InternalError(
                                    "immutable record not initialized".to_string(),
                                )
                            })?;
                        return Ok(IOResult::Done(Some(record_ref)));
                    }
                    return Ok(IOResult::Done(None));
                }
                let record_ref = self.reusable_immutable_record.as_ref().ok_or_else(|| {
                    LimboError::InternalError("immutable record not initialized".to_string())
                })?;
                Ok(IOResult::Done(Some(record_ref)))
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
            return self.db.read_visible_from_versions(self.tx_id, versions);
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

    fn get_immutable_record_or_create(&mut self) -> Result<&mut ImmutableRecord> {
        if self.reusable_immutable_record.is_none() {
            self.reusable_immutable_record = Some(ImmutableRecord::new(1024)?);
        }
        Ok(self.reusable_immutable_record.as_mut().unwrap())
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
        let begin_ts = self.db.read_snapshot_ts(self.tx_id);
        let read_mark = self.db.read_tx_mark(self.tx_id);
        if !self
            .db
            .is_btree_readable_at(&self.table_id, begin_ts, read_mark)
        {
            return false;
        }
        if self.btree_cursor.root_page() < 0 {
            let Some(root) = self.db.current_root_page(&self.table_id) else {
                return false;
            };
            self.btree_cursor.set_root_page(root as i64);
        }
        true
    }

    fn query_btree_version_is_valid(&self, key: &RowKey) -> bool {
        self.db
            .query_btree_version_is_valid(self.table_id, key, self.tx_id)
    }

    /// Advance MVCC iterator and return next visible row key in the direction that the iterator was initialized in.
    fn advance_mvcc_iterator(&mut self) {
        let new_peek_state = match &self.mv_cursor_type {
            MvccCursorType::Table => match self.db.advance_cursor_and_get_row_id_for_table(
                self.table_id,
                &mut self.table_iterator,
                self.tx_id,
            ) {
                Some((row_id, versions)) => CursorPeek::Row {
                    key: row_id.row_id,
                    versions: Some(versions),
                },
                None => CursorPeek::Exhausted,
            },
            MvccCursorType::Index(_) => match self
                .db
                .advance_cursor_and_get_row_id_for_index(&mut self.index_iterator, self.tx_id)
            {
                Some(row_id) => CursorPeek::Row {
                    key: row_id.row_id,
                    versions: None,
                },
                None => CursorPeek::Exhausted,
            },
        };
        self.dual_peek.mvcc_peek = new_peek_state;
    }

    /// Starts a forward move. Before the first row with no peek loaded, it
    /// loads the MVCC peek and returns true: the B-tree peek must be loaded
    /// too before the cursor picks a row.
    #[inline(always)]
    fn begin_forward_move(&mut self) -> Result<bool> {
        if !matches!(self.current_pos, CursorPosition::BeforeFirst)
            || !self.dual_peek.both_uninitialized()
        {
            return Ok(false);
        }
        self.init_mvcc_iterator_forward()?;
        self.advance_mvcc_iterator();
        Ok(true)
    }

    /// Advances the MVCC peek when the current row came from it, and says
    /// whether the B-tree peek must be advanced too.
    #[inline(always)]
    fn plan_move(&mut self, dir: IterationDirection) -> MoveStep {
        let (need_advance_mvcc, need_advance_btree) = match (&self.current_pos, dir) {
            // First move after a rewind or last: both peeks are loaded, so
            // the cursor only has to pick one of them.
            (CursorPosition::BeforeFirst, IterationDirection::Forwards)
            | (CursorPosition::End, IterationDirection::Backwards) => (false, false),
            (
                CursorPosition::Loaded {
                    row_id, in_btree, ..
                },
                _,
            ) => {
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
            (CursorPosition::End, IterationDirection::Forwards)
            | (CursorPosition::BeforeFirst, IterationDirection::Backwards) => {
                return MoveStep::AtEnd;
            }
        };
        if need_advance_mvcc && !self.dual_peek.mvcc_exhausted() {
            self.advance_mvcc_iterator();
        }
        if need_advance_btree && !self.dual_peek.btree_exhausted() {
            MoveStep::AdvanceBtree
        } else {
            MoveStep::PickPosition
        }
    }

    /// Picks the cursor position from the two peeks after a move.
    #[inline(always)]
    fn finish_move(&mut self, dir: IterationDirection) {
        self.refresh_current_position(dir);
        self.invalidate_record();
    }

    /// Looks for `key` in the MVCC store. `Some` is the answer when the
    /// store alone can give it. `None` means the B-tree must be asked.
    #[inline(always)]
    fn begin_exists(&mut self, key: &Value) -> Option<bool> {
        self.invalidate_record();
        let int_key = match key {
            Value::Numeric(crate::numeric::Numeric::Integer(i)) => *i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let inclusive = true;

        // Check MVCC first. This is a point existence probe, so it is
        // eq-only: bound the skiplist walk to the single rowid instead of
        // scanning forward over invisible concurrent rows.
        let rowid = self.db.seek_rowid(
            RowID {
                table_id: self.table_id,
                row_id: RowKey::Int(int_key),
            },
            inclusive,
            true,
            IterationDirection::Forwards,
            self.tx_id,
            &mut self.table_iterator,
        );

        let mvcc_exists = if let Some((rowid, _)) = &rowid {
            let RowKey::Int(rowid) = rowid.row_id else {
                panic!("Rowid is not an integer in mvcc table cursor");
            };
            rowid == int_key
        } else {
            false
        };

        tracing::trace!(
            "MVCC exists check: mvcc_exists={mvcc_exists} find={int_key} got={rowid:?}"
        );

        if mvcc_exists {
            self.dual_peek.mvcc_peek = CursorPeek::Row {
                key: RowKey::Int(int_key),
                versions: None,
            };
            self.current_pos = CursorPosition::Loaded {
                row_id: RowID {
                    table_id: self.table_id,
                    row_id: RowKey::Int(int_key),
                },
                in_btree: false,
                versions: None,
            };
            return Some(true);
        }

        if !self.is_btree_allocated() {
            // No B-tree allocated, row doesn't exist
            return Some(false);
        }
        // If the B-tree version is invalid (row is deleted or shadowed), don't check the B-tree
        if !self.query_btree_version_is_valid(&RowKey::Int(int_key)) {
            return Some(false);
        }
        None
    }

    /// Looks for `key` in the B-tree, and checks that MVCC does not shadow
    /// the row it finds.
    #[inline(always)]
    fn exists_in_btree(&mut self, key: &Value) -> IOResultOr<bool> {
        turso_assert!(
            self.is_btree_allocated(),
            "BTree should be allocated when we are in ExistsBtree state"
        );
        let found = return_if_io!(self.btree_cursor.exists(key));
        if !found {
            return Ok(IOResult::Done(false));
        }
        let int_key = match key {
            Value::Numeric(crate::numeric::Numeric::Integer(i)) => *i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let row_key = RowKey::Int(int_key);
        if !self.query_btree_version_is_valid(&row_key) {
            tracing::trace!("B-tree row {int_key} is shadowed by MVCC");
            return Ok(IOResult::Done(false));
        }
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
        Ok(IOResult::Done(true))
    }

    /// Resets the cursor and seeks the MVCC iterator to `seek_key`.
    #[inline(always)]
    fn begin_seek(&mut self, seek_key: SeekKey<'_>, op: SeekOp) -> Result<()> {
        self.begin_rewind();
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
                let mvcc_rowid = self.db.seek_rowid(
                    rowid,
                    inclusive,
                    op.eq_only(),
                    direction,
                    self.tx_id,
                    &mut self.table_iterator,
                );
                self.dual_peek.mvcc_peek = match mvcc_rowid {
                    Some((rid, payload)) => {
                        self.eq_seek_row = payload;
                        CursorPeek::Row {
                            key: rid.row_id,
                            versions: None,
                        }
                    }
                    None => CursorPeek::Exhausted,
                };
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
                let mvcc_rowid = self.db.seek_index(
                    self.table_id,
                    sortable_key,
                    inclusive,
                    op.eq_only(),
                    direction,
                    self.tx_id,
                    &mut self.index_iterator,
                )?;
                self.dual_peek.mvcc_peek = match &mvcc_rowid {
                    Some(rid) => CursorPeek::Row {
                        key: rid.row_id.clone(),
                        versions: None,
                    },
                    None => CursorPeek::Exhausted,
                };
            }
        }
        Ok(())
    }

    /// Picks the row that comes first in the direction of the seek from the
    /// two peeks, and says whether it matches `seek_key`.
    #[inline(always)]
    fn finish_seek(&mut self, seek_key: SeekKey<'_>, op: SeekOp) -> Result<SeekResult> {
        let winner_pos = self.position_from_peeks(op.iteration_direction());
        let CursorPosition::Loaded {
            row_id,
            in_btree,
            versions,
        } = winner_pos
        else {
            // Nothing found in either cursor
            let forwards = matches!(op, SeekOp::GE { .. } | SeekOp::GT);
            self.current_pos = if forwards {
                CursorPosition::End
            } else {
                CursorPosition::BeforeFirst
            };
            return Ok(SeekResult::NotFound);
        };
        let winner_key = row_id.row_id.clone();
        self.current_pos = CursorPosition::Loaded {
            row_id,
            in_btree,
            versions,
        };
        if !op.eq_only() {
            return Ok(SeekResult::Found);
        }
        let found = match &seek_key {
            SeekKey::TableRowId(row_id) => winner_key == RowKey::Int(*row_id),
            SeekKey::IndexKey(index_key) => {
                let RowKey::Record(found_key) = &winner_key else {
                    panic!("Found rowid is not a record");
                };
                let MvccCursorType::Index(index_info) = &self.mv_cursor_type else {
                    panic!("Index cursor expected");
                };
                let key_info: Vec<_> = index_info
                    .key_info
                    .iter()
                    .take(index_key.column_count())
                    .cloned()
                    .collect();
                compare_immutable(
                    index_key.get_values()?,
                    found_key.key.get_values()?,
                    &key_info,
                )
                .is_eq()
            }
        };
        if found {
            Ok(SeekResult::Found)
        } else {
            Ok(SeekResult::NotFound)
        }
    }

    /// Seeks the B-tree cursor. `None` means the B-tree has no row for this
    /// cursor.
    #[inline(always)]
    fn begin_btree_seek(
        &mut self,
        seek_key: SeekKey<'_>,
        op: SeekOp,
    ) -> IOResultOr<Option<SeekResult>> {
        if !self.is_btree_allocated() {
            self.dual_peek.btree_peek = CursorPeek::Exhausted;
            return Ok(IOResult::Done(None));
        }
        let seek_result = return_if_io!(self.btree_cursor.seek(seek_key, op));
        Ok(IOResult::Done(Some(seek_result)))
    }

    /// Reads the row under the B-tree cursor after a seek. Returns true when
    /// the seek is done: the row is visible and stored in the B-tree peek,
    /// or the B-tree has no row left. Returns false when MVCC shadows it.
    #[inline(always)]
    fn check_seeked_btree_row(&mut self) -> IOResultOr<bool> {
        let Some(key) = self.get_btree_current_key()? else {
            self.dual_peek.btree_peek = CursorPeek::Exhausted;
            return Ok(IOResult::Done(true));
        };
        if !self.query_btree_version_is_valid(&key) {
            return Ok(IOResult::Done(false));
        }
        self.dual_peek.btree_peek = CursorPeek::Row {
            key,
            versions: None,
        };
        Ok(IOResult::Done(true))
    }

    /// Drops the MVCC iterators and the peeks before a rewind or a seek.
    #[inline(always)]
    fn begin_rewind(&mut self) {
        let _ = self.table_iterator.take();
        let _ = self.index_iterator.take();
        self.reset_dual_peek();
    }

    /// Loads the MVCC peek at the first or last row and picks the cursor
    /// position from both peeks, after the B-tree peek is loaded.
    #[inline(always)]
    fn finish_rewind(&mut self, dir: IterationDirection) -> Result<()> {
        self.invalidate_record();
        match dir {
            IterationDirection::Forwards => {
                self.current_pos = CursorPosition::BeforeFirst;
                self.init_mvcc_iterator_forward()?;
                self.advance_mvcc_iterator();
            }
            IterationDirection::Backwards => {
                self.current_pos = CursorPosition::End;
                let last_key = match &self.mv_cursor_type {
                    MvccCursorType::Table => self.db.get_last_table_rowid(
                        self.table_id,
                        &mut self.table_iterator,
                        self.tx_id,
                    ),
                    MvccCursorType::Index(_) => self.db.get_last_index_rowid(
                        self.table_id,
                        self.tx_id,
                        &mut self.index_iterator,
                    )?,
                };
                tracing::trace!("last: mvcc_key: {:?}", last_key);
                self.dual_peek.mvcc_peek = match last_key {
                    Some(key) => CursorPeek::Row {
                        key,
                        versions: None,
                    },
                    None => CursorPeek::Exhausted,
                };
            }
        }
        self.refresh_current_position(dir);
        self.invalidate_record();
        Ok(())
    }

    /// Starts one B-tree advance. `None` means the B-tree has no row for
    /// this cursor. `Some(true)` means the cursor was moved to the first or
    /// last row and that row must be checked before the cursor moves again.
    #[inline(always)]
    fn begin_btree_advance(
        &mut self,
        dir: IterationDirection,
        initialize: bool,
    ) -> IOResultOr<Option<bool>> {
        if !self.is_btree_allocated() {
            self.dual_peek.btree_peek = CursorPeek::Exhausted;
            return Ok(IOResult::Done(None));
        }
        if initialize && self.dual_peek.btree_uninitialized() {
            return_if_io!(match dir {
                IterationDirection::Forwards => self.btree_cursor.rewind(),
                IterationDirection::Backwards => self.btree_cursor.last(),
            });
            return Ok(IOResult::Done(Some(true)));
        }
        Ok(IOResult::Done(Some(false)))
    }

    /// Reads the key under the B-tree cursor. Returns true when the advance
    /// is done: the row is visible and stored in the B-tree peek, or the
    /// B-tree has no row left. Returns false when MVCC shadows the row.
    #[inline(always)]
    fn peek_btree_key(&mut self, dir: IterationDirection) -> IOResultOr<bool> {
        let Some(key) = self.get_btree_current_key()? else {
            self.dual_peek.btree_peek = CursorPeek::Exhausted;
            return Ok(IOResult::Done(true));
        };
        let valid = match dir {
            IterationDirection::Forwards => self.btree_row_is_valid_forward(&key),
            IterationDirection::Backwards => self.query_btree_version_is_valid(&key),
        };
        if valid {
            self.dual_peek.btree_peek = CursorPeek::Row {
                key,
                versions: None,
            };
        }
        Ok(IOResult::Done(valid))
    }

    /// Moves the B-tree cursor one row in `dir`. Returns whether the cursor
    /// is on a row afterwards.
    #[inline(always)]
    fn step_btree(&mut self, dir: IterationDirection) -> IOResultOr<bool> {
        return_if_io!(match dir {
            IterationDirection::Forwards => self.btree_cursor.next(),
            IterationDirection::Backwards => self.btree_cursor.prev(),
        });
        Ok(IOResult::Done(self.btree_cursor.has_record()))
    }

    /// The completion of an injected yield at `point`, when the yield
    /// injector of the connection asks for one.
    #[cfg(any(test, injected_yields))]
    fn injected_yield(&self, point: CursorYieldPoint) -> Option<IOCompletions> {
        let yield_context = self.yield_context();
        match crate::mvcc::yield_hooks::maybe_inject_io_yield::<(), _>(
            yield_context.injector.as_ref(),
            yield_context.instance_id,
            yield_context.selection_key,
            point,
        ) {
            Some(IOResult::IO(io)) => Some(io),
            _ => None,
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
            let falls_through = {
                let chain = versions.read();
                self.db
                    .chain_falls_through_for_tx(self.tx_id, table_id, &chain)
            };
            if !falls_through {
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
        self.eq_seek_row = None;
        // The forward scan is monotonic; a reposition invalidates it.
        self.index_shadow_scan.reset();
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
                let range =
                    create_seek_range(Bound::Included(start_rowid), IterationDirection::Forwards);
                let iter_box = Box::new(self.db.rows.range(range));
                self.table_iterator = Some(static_iterator_hack!(iter_box, RowID, A));
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
        run_cursor_op!(
            self,
            CursorOp::Rewind,
            rewind,
            rewind_cursor,
            IterationDirection::Backwards
        )
    }

    /// Move the cursor to the next row. Returns true if the cursor moved to the next row, false if the cursor is at the end of the table.
    ///
    /// Uses dual-cursor approach: only advances the cursor that was just consumed.
    fn next(&mut self) -> IOResultOr<()> {
        run_cursor_op!(
            self,
            CursorOp::Next,
            move_row,
            move_cursor,
            IterationDirection::Forwards
        )
    }

    /// Move the cursor to the previous row. Returns true if the cursor moved, false if at the beginning.
    ///
    /// Uses dual-cursor approach: only advances the cursor that was just consumed.
    fn prev(&mut self) -> IOResultOr<()> {
        run_cursor_op!(
            self,
            CursorOp::Prev,
            move_row,
            move_cursor,
            IterationDirection::Backwards
        )
    }

    fn rowid(&mut self) -> IOResultOr<Option<i64>> {
        if self.get_null_flag() {
            return Ok(IOResult::Done(None));
        }
        let rowid = match self.get_current_pos() {
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
        // gt -> lower_bound bound excluded, we want first row after row_id
        // ge -> lower_bound bound included, we want first row equal to row_id or first row after row_id
        // lt -> upper_bound bound excluded, we want last row before row_id
        // le -> upper_bound bound included, we want last row equal to row_id or first row before row_id

        // Skip the seek and short-circuit to SeekResult::Found if the following are true:
        //
        // - the seek is eq_only
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
        if self.active.is_none() && op.eq_only() {
            if let CursorPosition::Loaded {
                row_id, in_btree, ..
            } = &self.current_pos
            {
                if current_pos_matches_seek_key(&row_id.row_id, &seek_key, &self.mv_cursor_type)? {
                    let maybe_index_id = match &self.mv_cursor_type {
                        MvccCursorType::Index(_) => Some(self.table_id),
                        MvccCursorType::Table => None,
                    };
                    // Prefer the B-tree copy when this cursor is already on it:
                    // a stale SkipMap read here would hide later checkpointed
                    // updates and change scan totals.
                    let visible = if *in_btree {
                        self.query_btree_version_is_valid(&row_id.row_id)
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

        run_cursor_op!(
            self,
            CursorOp::Seek,
            seek,
            seek_cursor,
            op,
            CursorArgs::Seek { key: seek_key }
        )
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
                let num_columns = record.column_count();
                crate::with_mv_store_allocation_site!(
                    RowPayload,
                    Row::new_table_row_in(
                        row_id,
                        record.get_payload(),
                        num_columns,
                        self.db.allocator(),
                    )
                )
            }
            MvccCursorType::Index(_) => {
                let BTreeKey::IndexKey(record) = key else {
                    return Err(LimboError::InternalError(
                        "Index cursor requires an IndexKey".to_string(),
                    )
                    .into());
                };
                Ok(Row::new_index_row(row_id, record.column_count()))
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
        // never calls Column on the table cursor). Pre-fetch it here so the
        // later synchronous fetch used to build a tombstone doesn't have to
        // yield IO from inside this function, which is not IO-reentrant w.r.t.
        // `delete_from_table_or_index`'s side effects.
        if in_btree {
            return_if_io!(self.record());
        }
        let was_deleted =
            self.db
                .delete_from_table_or_index(self.tx_id, rowid.clone(), maybe_index_id)?;
        // If was_deleted is false, this can ONLY happen when we have a row that only exists
        // in the btree but not the mv store. In this case, we create a tombstone for the row
        // based on the btree row.
        if !was_deleted {
            // The cursor can also be positioned on a row that was rolled back
            // after seek. That row does not exist in either MVCC or the B-tree.
            if !in_btree {
                self.invalidate_record();
                return Ok(IOResult::Done(()));
            }
            // The btree cursor must be correctly positioned and cannot cause IO to happen
            // because we pre-fetched the record above when `in_btree` was true.
            let IOResult::Done(Some(record)) = self.record()? else {
                crate::bail_corrupt_error!(
                    "Btree cursor should have a record when deleting a row that only exists in the btree"
                );
            };
            // All operations below clone values so we can clone it here to circumvent the borrow checker
            let record = record.clone();
            let column_count = record.column_count();
            let row = match &self.mv_cursor_type {
                MvccCursorType::Table => crate::with_mv_store_allocation_site!(
                    RowPayload,
                    Row::new_table_row_in(
                        rowid.clone(),
                        record.get_payload(),
                        column_count,
                        self.db.allocator(),
                    )
                ),
                MvccCursorType::Index(_) => Ok(Row::new_index_row(rowid.clone(), column_count)),
            }?;
            self.db
                .insert_tombstone_to_table_or_index(self.tx_id, rowid, row, maybe_index_id)?;
        }
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
        run_cursor_op!(
            self,
            CursorOp::Exists,
            exists,
            exists_row,
            (),
            CursorArgs::Exists { key }
        )
    }

    fn clear_btree(&mut self) -> IOResultOr<Option<usize>> {
        todo!()
    }

    fn btree_destroy(&mut self) -> IOResultOr<Option<usize>> {
        todo!()
    }

    fn count(&mut self) -> IOResultOr<usize> {
        run_cursor_op!(self, CursorOp::Count, count, count_rows, ())
    }

    /// Returns true if the is not pointing to any row.
    fn is_empty(&self) -> bool {
        // If we reached the end of the table, it means we traversed the whole table therefore there must be something in the table.
        // If we have loaded a row, it means there is something in the table.
        match self.get_current_pos() {
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
        run_cursor_op!(
            self,
            CursorOp::Rewind,
            rewind,
            rewind_cursor,
            IterationDirection::Forwards
        )
    }

    fn has_record(&self) -> bool {
        matches!(self.get_current_pos(), CursorPosition::Loaded { .. })
    }

    fn set_has_record(&mut self, _has_record: bool) {
        todo!()
    }

    fn get_index_info(&self) -> &Arc<crate::types::IndexInfo> {
        match &self.mv_cursor_type {
            MvccCursorType::Index(index_info) => index_info,
            MvccCursorType::Table => panic!("get_index_info called on table cursor"),
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

/// Yields to the caller when the yield injector of the cursor asks for a
/// yield at `point`.
macro_rules! inject_cursor_yield {
    ($co:expr, $point:expr) => {{
        #[cfg(any(test, injected_yields))]
        if let Some(io) = $co.with(|ctx| ctx.cursor.injected_yield($point)) {
            $co.yield_io(io).await;
        }
    }};
}

/// Moves the cursor one row in `dir`. The cursor merges the MVCC iterator
/// and the B-tree cursor, so it advances the peek that the current row came
/// from and then picks the smaller (or larger) of the two peeks.
async fn move_cursor<Clock: LogicalClock + 'static, A: ConcurrentAllocator>(
    co: &mut Co<MvCursorStep<Clock, A>>,
    dir: IterationDirection,
) -> Result<(), Box<LimboError>> {
    match dir {
        IterationDirection::Forwards => {
            let advance_first = co
                .io(|ctx| ctx.cursor.begin_forward_move().map(IOResult::Done))
                .await;
            inject_cursor_yield!(co, CursorYieldPoint::NextStart);
            if advance_first {
                advance_btree(co, dir, true).await?;
            }
        }
        IterationDirection::Backwards => {
            let position_last = co.with(|ctx| {
                matches!(ctx.cursor.current_pos, CursorPosition::End)
                    && ctx.cursor.dual_peek.both_uninitialized()
            });
            if position_last {
                rewind_cursor(co, dir).await?;
            }
        }
    }
    match co.with(|ctx| ctx.cursor.plan_move(dir)) {
        MoveStep::AtEnd => return Ok(()),
        MoveStep::AdvanceBtree => {
            match dir {
                IterationDirection::Forwards => {
                    inject_cursor_yield!(co, CursorYieldPoint::NextBtreeAdvance)
                }
                IterationDirection::Backwards => {
                    inject_cursor_yield!(co, CursorYieldPoint::PrevBtreeAdvance)
                }
            }
            advance_btree(co, dir, true).await?;
        }
        MoveStep::PickPosition => {}
    }
    co.with(|ctx| ctx.cursor.finish_move(dir));
    Ok(())
}

/// Positions the cursor before the first row (`Forwards`) or after the last
/// row (`Backwards`) of the table or index, and loads the peek of both the
/// MVCC iterator and the B-tree cursor for the following `next` or `prev`.
async fn rewind_cursor<Clock: LogicalClock + 'static, A: ConcurrentAllocator>(
    co: &mut Co<MvCursorStep<Clock, A>>,
    dir: IterationDirection,
) -> Result<(), Box<LimboError>> {
    co.with(|ctx| ctx.cursor.begin_rewind());
    advance_btree(co, dir, true).await?;
    co.io(|ctx| ctx.cursor.finish_rewind(dir).map(IOResult::Done))
        .await;
    Ok(())
}

/// Counts the rows of the table: rewinds the cursor and moves it forward
/// until it runs past the last row.
async fn count_rows<Clock: LogicalClock + 'static, A: ConcurrentAllocator>(
    co: &mut Co<MvCursorStep<Clock, A>>,
    (): (),
) -> Result<usize, Box<LimboError>> {
    inject_cursor_yield!(co, CursorYieldPoint::CountProgress);
    co.with(|ctx| ctx.cursor.set_null_flag(false));
    rewind_cursor(co, IterationDirection::Forwards).await?;
    inject_cursor_yield!(co, CursorYieldPoint::CountProgress);
    let mut count = 0;
    while co.with(|ctx| ctx.cursor.has_record()) {
        count += 1;
        inject_cursor_yield!(co, CursorYieldPoint::CountProgress);
        move_cursor(co, IterationDirection::Forwards).await?;
        inject_cursor_yield!(co, CursorYieldPoint::CountProgress);
    }
    Ok(count)
}

/// Says whether the table has a visible row with the integer key that the
/// current step passes in its context. The MVCC store answers first. The
/// B-tree is asked only when the store has no version for the key.
async fn exists_row<Clock: LogicalClock + 'static, A: ConcurrentAllocator>(
    co: &mut Co<MvCursorStep<Clock, A>>,
    (): (),
) -> Result<bool, Box<LimboError>> {
    if let Some(found) = co.with(|ctx| {
        let key = ctx.args.exists_key();
        ctx.cursor.begin_exists(key)
    }) {
        return Ok(found);
    }
    inject_cursor_yield!(co, CursorYieldPoint::ExistsBtreeFallback);
    let found = co
        .io(|ctx| {
            let key = ctx.args.exists_key();
            ctx.cursor.exists_in_btree(key)
        })
        .await;
    Ok(found)
}

/// Seeks to the seek key that the current step passes in its context: the
/// MVCC iterator first, then the B-tree cursor, then picks the row that
/// comes first in the direction of the seek.
async fn seek_cursor<Clock: LogicalClock + 'static, A: ConcurrentAllocator>(
    co: &mut Co<MvCursorStep<Clock, A>>,
    op: SeekOp,
) -> Result<SeekResult, Box<LimboError>> {
    let dir = op.iteration_direction();
    co.io(|ctx| {
        let key = ctx.args.seek_key();
        ctx.cursor.begin_seek(key, op).map(IOResult::Done)
    })
    .await;
    inject_cursor_yield!(co, CursorYieldPoint::SeekStart);
    seek_btree(co, (dir, op)).await?;
    inject_cursor_yield!(co, CursorYieldPoint::SeekBtreeProgress);
    let result = co
        .io(|ctx| {
            let key = ctx.args.seek_key();
            ctx.cursor.finish_seek(key, op).map(IOResult::Done)
        })
        .await;
    Ok(result)
}

/// Seeks the B-tree cursor to the seek key that the current step passes in
/// its context, then stores the first row in `dir` that MVCC does not
/// shadow in the B-tree peek.
async fn seek_btree<Clock: LogicalClock + 'static, A: ConcurrentAllocator>(
    co: &mut Co<MvCursorStep<Clock, A>>,
    (dir, op): (IterationDirection, SeekOp),
) -> Result<(), Box<LimboError>> {
    let Some(seek_result) = co
        .io(|ctx| {
            let key = ctx.args.seek_key();
            ctx.cursor.begin_btree_seek(key, op)
        })
        .await
    else {
        return Ok(());
    };
    let mut need_advance = match seek_result {
        SeekResult::NotFound => {
            co.with(|ctx| ctx.cursor.dual_peek.btree_peek = CursorPeek::Exhausted);
            return Ok(());
        }
        SeekResult::TryAdvance => true,
        SeekResult::Found => false,
    };
    inject_cursor_yield!(co, CursorYieldPoint::SeekBtreeProgress);
    loop {
        if need_advance {
            advance_btree(co, dir, false).await?;
            inject_cursor_yield!(co, CursorYieldPoint::SeekBtreeProgress);
        }
        if co.io(|ctx| ctx.cursor.check_seeked_btree_row()).await {
            return Ok(());
        }
        inject_cursor_yield!(co, CursorYieldPoint::SeekBtreeProgress);
        need_advance = true;
    }
}

/// Moves the B-tree cursor in `dir` until it is on a row that MVCC does not
/// shadow, and stores that row in the B-tree peek. With `initialize`, an
/// uninitialized B-tree peek first moves the cursor to the first or last
/// row of the B-tree.
async fn advance_btree<Clock: LogicalClock + 'static, A: ConcurrentAllocator>(
    co: &mut Co<MvCursorStep<Clock, A>>,
    dir: IterationDirection,
    initialize: bool,
) -> Result<(), Box<LimboError>> {
    let Some(mut check_current) = co
        .io(|ctx| ctx.cursor.begin_btree_advance(dir, initialize))
        .await
    else {
        return Ok(());
    };
    inject_cursor_yield!(co, advance_yield_point(dir));
    loop {
        if check_current && co.io(|ctx| ctx.cursor.peek_btree_key(dir)).await {
            return Ok(());
        }
        let found = co.io(|ctx| ctx.cursor.step_btree(dir)).await;
        if !found {
            co.with(|ctx| ctx.cursor.dual_peek.btree_peek = CursorPeek::Exhausted);
            return Ok(());
        }
        inject_cursor_yield!(co, advance_yield_point(dir));
        check_current = true;
    }
}

#[cfg(any(test, injected_yields))]
fn advance_yield_point(dir: IterationDirection) -> CursorYieldPoint {
    match dir {
        IterationDirection::Forwards => CursorYieldPoint::AdvanceBtreeForwardProgress,
        IterationDirection::Backwards => CursorYieldPoint::AdvanceBtreeBackwardProgress,
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
