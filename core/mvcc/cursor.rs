use crate::sync::RwLock;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;

use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{
    create_seek_range, MVTableId, MvStore, Row, RowID, RowKey, RowVersion, RowVersionState,
    SortableIndexKey,
};
use crate::storage::btree::{BTreeCursor, BTreeKey, CursorTrait};
use crate::sync::Arc;
use crate::translate::plan::IterationDirection;
use crate::types::{
    compare_immutable, IOCompletions, IOResult, ImmutableRecord, IndexInfo, SeekKey, SeekOp,
    SeekResult, Value,
};
use crate::vdbe::make_record;
use crate::vdbe::Register;
use crate::{return_if_io, turso_assert, Completion, LimboError, Pager, Result};
use std::any::Any;
use std::fmt::Debug;
use std::ops::Bound;

#[derive(Debug, Clone)]
enum CursorPosition {
    /// We haven't loaded any row yet.
    BeforeFirst,
    /// We have loaded a row. This position points to a rowid in either MVCC index or in BTree.
    Loaded {
        row_id: RowID,
        /// Indicates whether the rowid is pointing BTreeCursor or MVCC index.
        in_btree: bool,
    },
    /// We have reached the end of the table.
    End,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
/// Rewind state is used to track the state of the rewind **AND** last operation. Since both seem to do similiar
/// operations we can use the same enum for both.
enum RewindState {
    Advance,
}

#[derive(Debug, Clone)]
enum NextState {
    AdvanceUnitialized,
    CheckNeedsAdvance,
    Advance,
}
#[derive(Debug, Clone)]
enum PrevState {
    AdvanceUnitialized,
    CheckNeedsAdvance,
    Advance,
}

#[derive(Debug, Clone)]
enum SeekBtreeState {
    /// Seeking in btree (MVCC seek already done)
    SeekBtree,
    /// Advance to next key in btree (if we got [SeekResult::TryAdvance], or the current row is shadowed by MVCC)
    AdvanceBTree,
    /// Check if current row is visible (not shadowed by MVCC)
    CheckRow,
}

#[derive(Debug, Clone)]
enum SeekState {
    /// Seeking in btree (MVCC seek already done)
    SeekBtree(SeekBtreeState),
    /// Pick winner and finalize
    PickWinner,
}

#[derive(Debug, Clone)]
enum CountState {
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

/// We read rows from MVCC index or BTree in a dual-cursor approach.
/// This means we read rows from both cursors and then advance the cursor that was just consumed.
/// With DualCursorPeek we track the "peeked" next value for each cursor in the dual-cursor iteration,
/// so that we always return the correct 'next' value (e.g. if mvcc has 1 and 3 and btree has 2 and 4,
/// we should return 1, 2, 3, 4 in order).
#[derive(Debug, Clone, Default)]
struct DualCursorPeek {
    /// Next row available from MVCC
    mvcc_peek: CursorPeek,
    /// Next row available from btree
    btree_peek: CursorPeek,
}

impl DualCursorPeek {
    /// Returns the next row key and whether the row is from the BTree.
    fn get_next(&self, dir: IterationDirection) -> Option<(RowKey, bool)> {
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
                        Some((mvcc_key.clone(), false))
                    } else {
                        Some((btree_key.clone(), true))
                    }
                // In backwards iteration we want the larger of the two keys
                } else if mvcc_key >= btree_key {
                    Some((mvcc_key.clone(), false))
                } else {
                    Some((btree_key.clone(), true))
                }
            }
            (Some(mvcc_key), None) => Some((mvcc_key.clone(), false)),
            (None, Some(btree_key)) => Some((btree_key.clone(), true)),
            (None, None) => None,
        }
    }

    /// Returns a new [CursorPosition] based on the next row key
    pub fn cursor_position_from_next(
        &self,
        table_id: MVTableId,
        dir: IterationDirection,
    ) -> CursorPosition {
        match self.get_next(dir) {
            Some((row_key, in_btree)) => CursorPosition::Loaded {
                row_id: RowID {
                    table_id,
                    row_id: row_key,
                },
                in_btree,
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

#[derive(Debug, Clone, Default)]
enum CursorPeek {
    #[default]
    Uninitialized,
    Row(RowKey),
    Exhausted,
}

impl CursorPeek {
    pub fn get_row_key(&self) -> Option<&RowKey> {
        match self {
            CursorPeek::Row(k) => Some(k),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MvccCursorType {
    Table,
    Index(Arc<IndexInfo>),
}

pub(crate) type MvccIterator<'l, T> =
    Box<dyn Iterator<Item = Entry<'l, T, RwLock<Vec<RowVersion>>>> + Send + Sync>;

/// Extends the lifetime of a SkipMap iterator to `'static`.
///
/// # Why a macro instead of a function?
///
/// Rust's `crossbeam_skiplist::map::Entry<'a, K, V>` is *invariant* over `K`, meaning
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
        // SAFETY: See macro documentation above.
        unsafe {
            std::mem::transmute::<
                Box<
                    dyn Iterator<Item = Entry<'_, $key_type, RwLock<Vec<RowVersion>>>>
                        + Send
                        + Sync,
                >,
                Box<
                    dyn Iterator<Item = Entry<'static, $key_type, RwLock<Vec<RowVersion>>>>
                        + Send
                        + Sync,
                >,
            >($iter)
        }
    };
}

pub(crate) use static_iterator_hack;

pub struct MvccLazyCursor<Clock: LogicalClock> {
    pub db: Arc<MvStore<Clock>>,
    current_pos: CursorPosition,
    /// Stateful MVCC table iterator if this is a table cursor.
    table_iterator: Option<MvccIterator<'static, RowID>>,
    /// Stateful MVCC index iterator if this is an index cursor.
    index_iterator: Option<MvccIterator<'static, Arc<SortableIndexKey>>>,
    mv_cursor_type: MvccCursorType,
    table_id: MVTableId,
    tx_id: u64,
    /// Reusable immutable record, used to allow better allocation strategy.
    reusable_immutable_record: Option<ImmutableRecord>,
    btree_cursor: Box<dyn CursorTrait>,
    null_flag: bool,
    creating_new_rowid: bool,
    state: Option<MvccLazyCursorState>,
    // we keep count_state separate to be able to call other public functions like rewind and next
    count_state: Option<CountState>,
    btree_advance_state: Option<AdvanceBtreeState>,
    /// Dual-cursor peek state for proper iteration
    dual_peek: DualCursorPeek,
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

impl<Clock: LogicalClock + 'static> MvccLazyCursor<Clock> {
    pub fn new(
        db: Arc<MvStore<Clock>>,
        tx_id: u64,
        root_page_or_table_id: i64,
        mv_cursor_type: MvccCursorType,
        btree_cursor: Box<dyn CursorTrait>,
    ) -> Result<MvccLazyCursor<Clock>> {
        assert!(
            (&*btree_cursor as &dyn Any).is::<BTreeCursor>(),
            "BTreeCursor expected for mvcc cursor"
        );
        let table_id = db.get_table_id_from_root_page(root_page_or_table_id);
        Ok(Self {
            db,
            tx_id,
            table_iterator: None,
            index_iterator: None,
            mv_cursor_type,
            current_pos: CursorPosition::BeforeFirst,
            table_id,
            reusable_immutable_record: None,
            btree_cursor,
            null_flag: false,
            creating_new_rowid: false,
            state: None,
            count_state: None,
            btree_advance_state: None,
            dual_peek: DualCursorPeek::default(),
        })
    }

    /// Returns the current row as an immutable record.
    pub fn current_row(&mut self) -> Result<IOResult<Option<&crate::types::ImmutableRecord>>> {
        let current_pos = &self.current_pos;
        tracing::trace!("current_row({:?})", current_pos);
        match current_pos {
            CursorPosition::Loaded {
                row_id: _,
                in_btree,
            } => {
                if *in_btree {
                    self.btree_cursor.record()
                } else {
                    let Some(row) = self.read_mvcc_current_row()? else {
                        return Ok(IOResult::Done(None));
                    };
                    {
                        let mut record = self.get_immutable_record_or_create();
                        let record = record.as_mut().ok_or_else(|| {
                            LimboError::InternalError(
                                "immutable record not initialized".to_string(),
                            )
                        })?;
                        record.invalidate();
                        record.start_serialization(row.payload());
                    }

                    let record_ref = self.reusable_immutable_record.as_ref().ok_or_else(|| {
                        LimboError::InternalError("immutable record not initialized".to_string())
                    })?;
                    Ok(IOResult::Done(Some(record_ref)))
                }
            }
            CursorPosition::BeforeFirst => {
                // Before first is not a valid position, so we return none.
                Ok(IOResult::Done(None))
            }
            CursorPosition::End => Ok(IOResult::Done(None)),
        }
    }

    pub fn read_mvcc_current_row(&self) -> Result<Option<Row>> {
        let row_id = match self.current_pos.clone() {
            CursorPosition::Loaded { row_id, in_btree } if !in_btree => row_id,
            _ => panic!("invalid position to read current mvcc row"),
        };
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

    pub fn start_new_rowid(&mut self) -> Result<IOResult<NextRowidResult>> {
        tracing::trace!("start_new_rowid");
        let allocator = self.db.get_rowid_allocator(&self.table_id);
        let locked = allocator.lock();
        if !locked {
            // Yield, some other cursor is generating new rowid
            return Ok(IOResult::IO(IOCompletions::Single(Completion::new_yield())));
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
        tracing::trace!("start_new_rowid");
        let allocator = self.db.get_rowid_allocator(&self.table_id);
        turso_assert!(
            self.creating_new_rowid,
            "cursor didn't start creating new rowid"
        );
        allocator.initialize(max_rowid);
        Ok(())
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

    fn get_immutable_record_or_create(&mut self) -> Option<&mut ImmutableRecord> {
        let reusable_immutable_record = &mut self.reusable_immutable_record;
        if reusable_immutable_record.is_none() {
            let record = ImmutableRecord::new(1024);
            reusable_immutable_record.replace(record);
        }
        reusable_immutable_record.as_mut()
    }

    fn get_current_pos(&self) -> CursorPosition {
        self.current_pos.clone()
    }

    fn is_btree_allocated(&self) -> bool {
        self.db.is_btree_allocated(&self.table_id)
    }

    fn query_btree_version_is_valid(&self, key: &RowKey) -> bool {
        let res = self
            .db
            .find_row_last_version_state(self.table_id, key, self.tx_id);
        tracing::trace!("query_btree_version_is_valid: {:?}, key: {:?}", res, key);
        // If the row is not found in MVCC index, this means row_id is valid in btree
        matches!(res, RowVersionState::NotFound)
    }

    /// Advance MVCC iterator and return next visible row key in the direction that the iterator was initialized in.
    fn advance_mvcc_iterator(&mut self) {
        let next = match &self.mv_cursor_type {
            MvccCursorType::Table => self.db.advance_cursor_and_get_row_id_for_table(
                self.table_id,
                &mut self.table_iterator,
                self.tx_id,
            ),
            MvccCursorType::Index(_) => self
                .db
                .advance_cursor_and_get_row_id_for_index(&mut self.index_iterator, self.tx_id),
        };
        let new_peek_state = match next {
            Some(k) => CursorPeek::Row(k.row_id),
            None => CursorPeek::Exhausted,
        };
        self.dual_peek.mvcc_peek = new_peek_state;
    }

    /// Advance btree cursor forward and set btree peek to the first valid row key (skipping rows shadowed by MVCC)
    fn advance_btree_forward(&mut self) -> Result<IOResult<()>> {
        self._advance_btree_forward(true)
    }

    /// Advance btree cursor forward from current position (cursor already positioned by seek)
    fn advance_btree_forward_from_current(&mut self) -> Result<IOResult<()>> {
        self._advance_btree_forward(false)
    }

    fn _advance_btree_forward(&mut self, initialize: bool) -> Result<IOResult<()>> {
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
                }
                Some(AdvanceBtreeState::RewindCheckBtreeKey) => {
                    let key = self.get_btree_current_key()?;
                    match key {
                        Some(k) if self.query_btree_version_is_valid(&k) => {
                            self.dual_peek.btree_peek = CursorPeek::Row(k);
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
                }
                Some(AdvanceBtreeState::NextCheckBtreeKey) => {
                    let key = self.get_btree_current_key()?;
                    if let Some(key) = key {
                        if self.query_btree_version_is_valid(&key) {
                            self.dual_peek.btree_peek = CursorPeek::Row(key);
                            self.btree_advance_state = None;
                            return Ok(IOResult::Done(()));
                        }
                        // Row is shadowed by MVCC, continue to next
                        // FIXME: do we want to iterate over all shadowed rows? If every row is shadowed by MVCC, we will iterate the whole btree in a single `next` call
                        self.btree_advance_state = Some(AdvanceBtreeState::NextBtree);
                    } else {
                        self.dual_peek.btree_peek = CursorPeek::Exhausted;
                        self.btree_advance_state = None;
                        return Ok(IOResult::Done(()));
                    }
                }
            }
        }
    }

    /// Advance btree cursor backward and set btree peek to the first valid row key (skipping rows shadowed by MVCC)
    fn advance_btree_backward(&mut self) -> Result<IOResult<()>> {
        self._advance_btree_backward(true)
    }

    /// Advance btree cursor backward from current position (cursor already positioned by seek)
    fn advance_btree_backward_from_current(&mut self) -> Result<IOResult<()>> {
        self._advance_btree_backward(false)
    }

    fn _advance_btree_backward(&mut self, initialize: bool) -> Result<IOResult<()>> {
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
                }
                Some(AdvanceBtreeState::RewindCheckBtreeKey) => {
                    let key = self.get_btree_current_key()?;
                    match key {
                        Some(k) if self.query_btree_version_is_valid(&k) => {
                            self.dual_peek.btree_peek = CursorPeek::Row(k);
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
                }
                Some(AdvanceBtreeState::NextCheckBtreeKey) => {
                    let key = self.get_btree_current_key()?;
                    match key {
                        Some(k) if self.query_btree_version_is_valid(&k) => {
                            self.dual_peek.btree_peek = CursorPeek::Row(k);
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
                Ok(maybe_record.map(|record| {
                    RowKey::Record(SortableIndexKey {
                        key: record.clone(),
                        metadata: index_info.clone(),
                    })
                }))
            }
        }
    }

    /// Refresh the current position based on the peek values
    fn refresh_current_position(&mut self, dir: IterationDirection) {
        let new_position = self.dual_peek.cursor_position_from_next(self.table_id, dir);
        self.current_pos = new_position;
    }

    /// Reset dual peek state (called on rewind/last/seek)
    fn reset_dual_peek(&mut self) {
        self.dual_peek = DualCursorPeek::default();
    }

    /// Seek btree cursor and set btree_peek to the result.
    /// Skips rows that are shadowed by MVCC.
    /// Returns IOResult indicating if we need to yield for IO or are done.
    fn seek_btree_and_set_peek(
        &mut self,
        seek_key: SeekKey<'_>,
        op: SeekOp,
    ) -> Result<IOResult<()>> {
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
                        }
                        SeekResult::Found => {
                            self.state.replace(MvccLazyCursorState::Seek(
                                SeekState::SeekBtree(SeekBtreeState::CheckRow),
                                direction,
                            ));
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
                }
                SeekBtreeState::CheckRow => {
                    let key = self.get_btree_current_key()?;
                    match key {
                        Some(k) if self.query_btree_version_is_valid(&k) => {
                            self.dual_peek.btree_peek = CursorPeek::Row(k);
                            return Ok(IOResult::Done(()));
                        }
                        Some(_) => {
                            // shadowed by MVCC, continue to next
                            self.state.replace(MvccLazyCursorState::Seek(
                                SeekState::SeekBtree(SeekBtreeState::AdvanceBTree),
                                direction,
                            ));
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
    fn init_mvcc_iterator_forward(&mut self) {
        if self.table_iterator.is_some() || self.index_iterator.is_some() {
            return; // Already initialized
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
                self.table_iterator = Some(static_iterator_hack!(iter_box, RowID));
            }
            MvccCursorType::Index(_) => {
                let index_rows = self
                    .db
                    .index_rows
                    .get_or_insert_with(self.table_id, SkipMap::new);
                let index_rows = index_rows.value();
                let iter_box = Box::new(index_rows.iter());
                self.index_iterator = Some(static_iterator_hack!(iter_box, Arc<SortableIndexKey>));
            }
        }
    }
}

impl<Clock: LogicalClock + 'static> CursorTrait for MvccLazyCursor<Clock> {
    fn last(&mut self) -> Result<IOResult<()>> {
        let state = self.state.clone();
        if state.is_none() {
            let _ = self.table_iterator.take();
            let _ = self.index_iterator.take();
            self.reset_dual_peek();
            self.state
                .replace(MvccLazyCursorState::Rewind(RewindState::Advance));
        }

        assert!(
            matches!(
                self.state
                    .as_ref()
                    .expect("rewind state is not initialized"),
                MvccLazyCursorState::Rewind(RewindState::Advance)
            ),
            "Invalid last state {state:?}"
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
                self.tx_id,
            ) {
                Some(k) => {
                    tracing::trace!("last: mvcc_key: {:?}", k);
                    self.dual_peek.mvcc_peek = CursorPeek::Row(k);
                }
                None => {
                    self.dual_peek.mvcc_peek = CursorPeek::Exhausted;
                }
            },
            MvccCursorType::Index(_) => match self
                .db
                .get_last_index_rowid(self.table_id, &mut self.index_iterator)
            {
                Some(k) => {
                    self.dual_peek.mvcc_peek = CursorPeek::Row(k);
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
    ///
    /// Uses dual-cursor approach: only advances the cursor that was just consumed.
    fn next(&mut self) -> Result<IOResult<()>> {
        if self.state.is_none() {
            // If BeforeFirst and peek not initialized, initialize the iterators and peek values
            let current_pos = self.get_current_pos();
            if matches!(current_pos, CursorPosition::BeforeFirst) {
                let uninitialized = self.dual_peek.both_uninitialized();
                if uninitialized {
                    // Initialize MVCC iterator and get first peek
                    self.init_mvcc_iterator_forward();
                    self.advance_mvcc_iterator();
                    self.state
                        .replace(MvccLazyCursorState::Next(NextState::AdvanceUnitialized));
                } else {
                    self.state
                        .replace(MvccLazyCursorState::Next(NextState::CheckNeedsAdvance));
                }
            } else {
                self.state
                    .replace(MvccLazyCursorState::Next(NextState::CheckNeedsAdvance));
            }
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
            // Determine which cursor(s) need to be advanced based on current position
            let current_pos = self.get_current_pos();
            let (need_advance_mvcc, need_advance_btree) = match &current_pos {
                CursorPosition::BeforeFirst => {
                    // First call after rewind - peek values should already be populated
                    // Just need to pick the smaller one
                    (false, false)
                }
                CursorPosition::Loaded { in_btree, .. } => {
                    // Advance whichever cursor we just consumed
                    if *in_btree {
                        (false, true) // Last row was from btree, advance btree
                    } else {
                        (true, false) // Last row was from MVCC, advance MVCC
                    }
                }
                CursorPosition::End => {
                    self.state = None;
                    return Ok(IOResult::Done(()));
                }
            };

            // Advance cursors as needed and update peek state
            if need_advance_mvcc && !self.dual_peek.mvcc_exhausted() {
                self.advance_mvcc_iterator();
            }
            if need_advance_btree && !self.dual_peek.btree_exhausted() {
                self.state
                    .replace(MvccLazyCursorState::Next(NextState::Advance));
            }
        }

        if matches!(
            self.state.as_ref().expect("next state is not initialized"),
            MvccLazyCursorState::Next(NextState::Advance)
        ) {
            return_if_io!(self.advance_btree_forward());
        }

        self.refresh_current_position(IterationDirection::Forwards);
        self.invalidate_record();
        self.state = None;

        Ok(IOResult::Done(()))
    }

    /// Move the cursor to the previous row. Returns true if the cursor moved, false if at the beginning.
    ///
    /// Uses dual-cursor approach: only advances the cursor that was just consumed.
    fn prev(&mut self) -> Result<IOResult<()>> {
        if self.state.is_none() {
            // If End and peek not initialized, initialize via last()
            let current_pos = self.get_current_pos();
            if matches!(current_pos, CursorPosition::End) {
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
            // Determine which cursor(s) need to be advanced based on current position
            let current_pos = self.get_current_pos();
            let (need_advance_mvcc, need_advance_btree) = match &current_pos {
                CursorPosition::End => {
                    // First call after last() - peek values should already be populated
                    (false, false)
                }
                CursorPosition::Loaded { in_btree, .. } => {
                    // Advance whichever cursor we just consumed
                    if *in_btree {
                        (false, true) // Last row was from btree, advance btree
                    } else {
                        (true, false) // Last row was from MVCC, advance MVCC
                    }
                }
                CursorPosition::BeforeFirst => {
                    self.state = None;
                    return Ok(IOResult::Done(()));
                }
            };

            // Advance cursors as needed and update peek state
            if need_advance_mvcc && !self.dual_peek.mvcc_exhausted() {
                self.advance_mvcc_iterator();
            }
            if need_advance_btree && !self.dual_peek.btree_exhausted() {
                self.state
                    .replace(MvccLazyCursorState::Prev(PrevState::Advance));
            }
        }

        if matches!(
            self.state.as_ref().expect("prev state is not initialized"),
            MvccLazyCursorState::Prev(PrevState::Advance)
        ) {
            return_if_io!(self.advance_btree_backward());
        }
        self.refresh_current_position(IterationDirection::Backwards);
        self.invalidate_record();
        self.state = None;

        Ok(IOResult::Done(()))
    }

    fn rowid(&mut self) -> Result<IOResult<Option<i64>>> {
        let rowid = match self.get_current_pos() {
            CursorPosition::Loaded {
                row_id,
                in_btree: _,
            } => match &row_id.row_id {
                RowKey::Int(id) => Some(*id),
                RowKey::Record(sortable_key) => {
                    // For index cursors, the rowid is stored in the last column of the index record
                    let MvccCursorType::Index(index_info) = &self.mv_cursor_type else {
                        panic!("RowKey::Record requires Index cursor type");
                    };
                    if index_info.has_rowid {
                        match sortable_key.key.last_value() {
                            Some(Ok(crate::types::ValueRef::Integer(rowid))) => Some(rowid),
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

    fn record(&mut self) -> Result<IOResult<Option<&crate::types::ImmutableRecord>>> {
        self.current_row()
    }

    fn seek_unpacked(
        &mut self,
        registers: &[Register],
        op: SeekOp,
    ) -> Result<IOResult<SeekResult>> {
        let record = make_record(registers, &0, &registers.len());
        self.seek(SeekKey::IndexKey(&record), op)
    }

    fn seek(&mut self, seek_key: SeekKey<'_>, op: SeekOp) -> Result<IOResult<SeekResult>> {
        // gt -> lower_bound bound excluded, we want first row after row_id
        // ge -> lower_bound bound included, we want first row equal to row_id or first row after row_id
        // lt -> upper_bound bound excluded, we want last row before row_id
        // le -> upper_bound bound included, we want last row equal to row_id or first row before row_id

        loop {
            let state = self.state.clone();
            match state {
                None => {
                    // Initial state: Reset and do MVCC seek
                    let _ = self.table_iterator.take();
                    let _ = self.index_iterator.take();
                    self.reset_dual_peek();
                    self.invalidate_record();
                    // We need to clear the null flag for the table cursor before seeking,
                    // because it might have been set to false by an unmatched left-join row during the previous iteration
                    // on the outer loop.
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
                                direction,
                                self.tx_id,
                                &mut self.table_iterator,
                            );

                            // Set MVCC peek
                            {
                                self.dual_peek.mvcc_peek = match &mvcc_rowid {
                                    Some(rid) => CursorPeek::Row(rid.row_id.clone()),
                                    None => CursorPeek::Exhausted,
                                };
                            }
                        }
                        SeekKey::IndexKey(index_key) => {
                            let index_info = {
                                let MvccCursorType::Index(index_info) = &self.mv_cursor_type else {
                                    panic!("SeekKey::IndexKey requires Index cursor type");
                                };
                                Arc::new(IndexInfo {
                                    key_info: index_info.key_info.clone(),
                                    has_rowid: index_info.has_rowid,
                                    num_cols: index_key.column_count(),
                                })
                            };
                            let sortable_key =
                                SortableIndexKey::new_from_record((*index_key).clone(), index_info);

                            // Seek in MVCC (synchronous)
                            let mvcc_rowid = self.db.seek_index(
                                self.table_id,
                                sortable_key.clone(),
                                inclusive,
                                direction,
                                self.tx_id,
                                &mut self.index_iterator,
                            );

                            // Set MVCC peek
                            {
                                self.dual_peek.mvcc_peek = match &mvcc_rowid {
                                    Some(rid) => CursorPeek::Row(rid.row_id.clone()),
                                    None => CursorPeek::Exhausted,
                                };
                            }
                        }
                    }

                    // Move to btree seek state
                    self.state.replace(MvccLazyCursorState::Seek(
                        SeekState::SeekBtree(SeekBtreeState::SeekBtree),
                        direction,
                    ));
                }
                Some(MvccLazyCursorState::Seek(SeekState::SeekBtree(_), direction)) => {
                    return_if_io!(self.seek_btree_and_set_peek(seek_key.clone(), op));
                    self.state
                        .replace(MvccLazyCursorState::Seek(SeekState::PickWinner, direction));
                }
                Some(MvccLazyCursorState::Seek(SeekState::PickWinner, direction)) => {
                    // Pick winner and return result
                    // Now pick the winner based on direction
                    let winner = self.dual_peek.get_next(direction);

                    // Clear seek state
                    self.state = None;

                    if let Some((winner_key, in_btree)) = winner {
                        self.current_pos = CursorPosition::Loaded {
                            row_id: RowID {
                                table_id: self.table_id,
                                row_id: winner_key.clone(),
                            },
                            in_btree,
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
    fn insert(&mut self, key: &BTreeKey) -> Result<IOResult<()>> {
        let row_id = match key {
            BTreeKey::TableRowId((rowid, _)) => RowID::new(self.table_id, RowKey::Int(*rowid)),
            BTreeKey::IndexKey(record) => {
                let MvccCursorType::Index(index_info) = &self.mv_cursor_type else {
                    panic!("BTreeKey::IndexKey requires Index cursor type");
                };
                let sortable_key =
                    SortableIndexKey::new_from_record((*record).clone(), index_info.clone());
                RowID::new(self.table_id, RowKey::Record(sortable_key))
            }
        };
        let record_buf = key
            .get_record()
            .ok_or_else(|| LimboError::InternalError("BTreeKey should have a record".to_string()))?
            .get_payload()
            .to_vec();
        let num_columns = match key {
            BTreeKey::IndexKey(record) => record.column_count(),
            BTreeKey::TableRowId((_, record)) => record
                .as_ref()
                .ok_or_else(|| {
                    LimboError::InternalError("TableRowId should have a record".to_string())
                })?
                .column_count(),
        };
        let row = match &self.mv_cursor_type {
            MvccCursorType::Table => Row::new_table_row(row_id, record_buf, num_columns),
            MvccCursorType::Index(_) => Row::new_index_row(row_id, num_columns),
        };

        // Check if the cursor is currently positioned at a B-tree row that matches
        // the row we're inserting. This indicates we're updating a B-tree-resident row
        // that doesn't yet have an MVCC version.
        let was_btree_resident = match &self.current_pos {
            CursorPosition::Loaded {
                row_id: current_row_id,
                in_btree,
            } => *in_btree && *current_row_id == row.id,
            _ => false,
        };

        self.current_pos = CursorPosition::Loaded {
            row_id: row.id.clone(),
            in_btree: false,
        };
        let maybe_index_id = match &self.mv_cursor_type {
            MvccCursorType::Index(_) => Some(self.table_id),
            MvccCursorType::Table => None,
        };
        // FIXME: set btree to somewhere close to this rowid?
        if self
            .db
            .read_from_table_or_index(self.tx_id, row.id.clone(), maybe_index_id)?
            .is_some()
        {
            self.db
                .update_to_table_or_index(self.tx_id, row, maybe_index_id)
                .inspect_err(|_| {
                    self.current_pos = CursorPosition::BeforeFirst;
                })?;
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

    fn delete(&mut self) -> Result<IOResult<()>> {
        let rowid = match self.get_current_pos() {
            CursorPosition::Loaded { row_id, .. } => row_id,
            _ => panic!("Cannot delete: no current row"),
        };
        let maybe_index_id = match &self.mv_cursor_type {
            MvccCursorType::Index(_) => Some(self.table_id),
            MvccCursorType::Table => None,
        };
        let was_deleted =
            self.db
                .delete_from_table_or_index(self.tx_id, rowid.clone(), maybe_index_id)?;
        // If was_deleted is false, this can ONLY happen when we have a row that only exists
        // in the btree but not the mv store. In this case, we create a tombstone for the row
        // based on the btree row.
        if !was_deleted {
            // The btree cursor must be correctly positioned and cannot cause IO to happen
            // because in order to get here, we must have read it already in the VDBE.
            let IOResult::Done(Some(record)) = self.record()? else {
                crate::bail_corrupt_error!("Btree cursor should have a record when deleting a row that only exists in the btree");
            };
            // All operations below clone values so we can clone it here to circumvent the borrow checker
            let record = record.clone();
            let column_count = record.column_count();
            let row = match &self.mv_cursor_type {
                MvccCursorType::Table => {
                    Row::new_table_row(rowid.clone(), record.into_payload(), column_count)
                }
                MvccCursorType::Index(_) => Row::new_index_row(rowid.clone(), column_count),
            };
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

    fn exists(&mut self, key: &Value) -> Result<IOResult<bool>> {
        if self.state.is_none() {
            self.invalidate_record();
            let int_key = match key {
                Value::Integer(i) => i,
                _ => unreachable!("btree tables are indexed by integers!"),
            };
            let inclusive = true;

            // Check MVCC first
            let rowid = self.db.seek_rowid(
                RowID {
                    table_id: self.table_id,
                    row_id: RowKey::Int(*int_key),
                },
                inclusive,
                IterationDirection::Forwards,
                self.tx_id,
                &mut self.table_iterator,
            );

            let mvcc_exists = if let Some(rowid) = &rowid {
                let RowKey::Int(rowid) = rowid.row_id else {
                    panic!("Rowid is not an integer in mvcc table cursor");
                };
                rowid == *int_key
            } else {
                false
            };

            tracing::trace!(
                "MVCC exists check: mvcc_exists={mvcc_exists} find={int_key} got={rowid:?}"
            );

            // If found in MVCC, update dual_peek and return true
            if mvcc_exists {
                self.dual_peek.mvcc_peek = CursorPeek::Row(RowKey::Int(*int_key));
                self.current_pos = CursorPosition::Loaded {
                    row_id: RowID {
                        table_id: self.table_id,
                        row_id: RowKey::Int(*int_key),
                    },
                    in_btree: false,
                };
                self.state = None;
                return Ok(IOResult::Done(true));
            }

            // MVCC doesn't have it, but we need to check B-tree too
            if self.is_btree_allocated() {
                let is_deleted = matches!(
                    self.db.find_row_last_version_state(
                        self.table_id,
                        &RowKey::Int(*int_key),
                        self.tx_id,
                    ),
                    RowVersionState::Deleted
                );
                // If we deleted the row we don't need to check B-tree.
                if is_deleted {
                    self.state = None;
                    return Ok(IOResult::Done(false));
                }
                self.state
                    .replace(MvccLazyCursorState::Exists(ExistsState::ExistsBtree));
            } else {
                // No B-tree allocated, row doesn't exist
                self.state = None;
                return Ok(IOResult::Done(false));
            }
        }

        let Some(MvccLazyCursorState::Exists(ExistsState::ExistsBtree)) = self.state.clone() else {
            panic!("Invalid state {:?}", self.state);
        };
        assert!(
            self.is_btree_allocated(),
            "BTree should be allocated when we are in ExistsBtree state"
        );

        // Check if row exists in B-tree
        let found = return_if_io!(self.btree_cursor.exists(key));

        if found {
            // Found in B-tree, but need to verify it's not shadowed by MVCC tombstone
            let int_key = match key {
                Value::Integer(i) => *i,
                _ => unreachable!("btree tables are indexed by integers!"),
            };
            let row_key = RowKey::Int(int_key);

            // Check if this B-tree row is shadowed (deleted/updated) in MVCC
            let is_valid = self.query_btree_version_is_valid(&row_key);

            if is_valid {
                // B-tree row is visible (not shadowed), update dual_peek
                self.dual_peek.btree_peek = CursorPeek::Row(row_key.clone());
                self.current_pos = CursorPosition::Loaded {
                    row_id: RowID {
                        table_id: self.table_id,
                        row_id: row_key,
                    },
                    in_btree: true,
                };
                self.state = None;
                Ok(IOResult::Done(true))
            } else {
                // B-tree row is shadowed by MVCC (tombstone or update), so it doesn't exist
                tracing::trace!("B-tree row {int_key} is shadowed by MVCC");
                self.state = None;
                Ok(IOResult::Done(false))
            }
        } else {
            // Not found in B-tree either
            self.state = None;
            Ok(IOResult::Done(false))
        }
    }

    fn clear_btree(&mut self) -> Result<IOResult<Option<usize>>> {
        todo!()
    }

    fn btree_destroy(&mut self) -> Result<IOResult<Option<usize>>> {
        todo!()
    }

    fn count(&mut self) -> Result<IOResult<usize>> {
        loop {
            let state = self.count_state.clone();
            match state {
                None => {
                    self.count_state.replace(CountState::Rewind);
                }
                Some(CountState::Rewind) => {
                    return_if_io!(self.rewind());
                    self.count_state
                        .replace(CountState::CheckBtreeKey { count: 0 });
                }
                Some(CountState::CheckBtreeKey { count }) => {
                    if let CursorPosition::Loaded {
                        row_id: _,
                        in_btree: _,
                    } = self.get_current_pos()
                    {
                        self.count_state
                            .replace(CountState::NextBtree { count: count + 1 });
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
                }
            }
        }
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

    fn rewind(&mut self) -> Result<IOResult<()>> {
        let state = self.state.clone();
        if state.is_none() {
            let _ = self.table_iterator.take();
            let _ = self.index_iterator.take();
            self.reset_dual_peek();
            self.state
                .replace(MvccLazyCursorState::Rewind(RewindState::Advance));
        }

        assert!(
            matches!(
                self.state
                    .as_ref()
                    .expect("rewind state is not initialized"),
                MvccLazyCursorState::Rewind(RewindState::Advance)
            ),
            "Invalid rewind state {state:?}",
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
                self.table_iterator = Some(static_iterator_hack!(iter_box, RowID));
            }
            MvccCursorType::Index(_) => {
                // For index cursors, initialize the iterator to the beginning
                let index_rows = self
                    .db
                    .index_rows
                    .get_or_insert_with(self.table_id, SkipMap::new);
                let index_rows = index_rows.value();
                let iter_box = Box::new(index_rows.iter());
                self.index_iterator = Some(static_iterator_hack!(iter_box, Arc<SortableIndexKey>));
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

    fn seek_end(&mut self) -> Result<IOResult<()>> {
        if self.is_btree_allocated() {
            // Defer to btree cursor's seek_end implementation
            self.btree_cursor.seek_end()
        } else {
            // SkipMap inserts don't require cursor positioning because
            // SeekEnd instruction is only used for insertions.
            Ok(IOResult::Done(()))
        }
    }

    fn seek_to_last(&mut self, _always_seek: bool) -> Result<IOResult<()>> {
        match self.seek(SeekKey::TableRowId(i64::MAX), SeekOp::LE { eq_only: false })? {
            IOResult::Done(_) => Ok(IOResult::Done(())),
            IOResult::IO(iocompletions) => Ok(IOResult::IO(iocompletions)),
        }
    }

    fn invalidate_record(&mut self) {
        self.get_immutable_record_or_create()
            .as_mut()
            .expect("immutable record should be initialized")
            .invalidate();
    }

    fn has_rowid(&self) -> bool {
        match &self.mv_cursor_type {
            MvccCursorType::Index(index_info) => index_info.has_rowid,
            MvccCursorType::Table => true, // currently we don't support WITHOUT ROWID tables
        }
    }

    fn get_pager(&self) -> Arc<Pager> {
        todo!()
    }

    fn get_skip_advance(&self) -> bool {
        todo!()
    }
}

impl<Clock: LogicalClock> Debug for MvccLazyCursor<Clock> {
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
