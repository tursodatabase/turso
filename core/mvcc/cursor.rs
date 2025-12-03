use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;

use crate::mvcc::clock::LogicalClock;
use crate::mvcc::database::{
    create_seek_range, MVTableId, MvStore, Row, RowID, RowKey, RowVersion, RowVersionState,
    SortableIndexKey,
};
use crate::storage::btree::{BTreeCursor, BTreeKey, CursorTrait};
use crate::translate::plan::IterationDirection;
use crate::types::{
    compare_immutable, IOResult, ImmutableRecord, IndexInfo, RecordCursor, SeekKey, SeekOp,
    SeekResult,
};
use crate::{return_if_io, LimboError, Result};
use crate::{Pager, Value};
use std::any::Any;
use std::cell::{Ref, RefCell};
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;

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

#[derive(Debug, Clone)]
enum MvccLazyCursorState {
    Exists(ExistsState),
}

/// We read rows from MVCC index or BTree in a dual-cursor approach.
/// This means we read rows from both cursors and then advance the cursor that was just consumed.
/// With DualCursorPeek we track the "peeked" next value for each cursor in the dual-cursor iteration,
/// so that we always return the correct 'next' value (e.g. if mvcc has 1 and 3 and btree has 2 and 4,
/// we should return 1, 2, 3, 4 in order).
#[derive(Debug, Clone)]
struct DualCursorPeek {
    /// Next row available from MVCC
    mvcc_peek: CursorPeek,
    /// Next row available from btree
    btree_peek: CursorPeek,
}

impl DualCursorPeek {
    /// Returns the next row key and whether the row is from the BTree.
    fn get_next(&self, dir: IterationDirection) -> Option<(RowKey, bool)> {
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

#[derive(Debug, Clone)]
enum CursorPeek {
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
    Box<dyn Iterator<Item = Entry<'l, T, RwLock<Vec<RowVersion>>>>>;

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
                Box<dyn Iterator<Item = Entry<'_, $key_type, RwLock<Vec<RowVersion>>>>>,
                Box<dyn Iterator<Item = Entry<'static, $key_type, RwLock<Vec<RowVersion>>>>>,
            >($iter)
        }
    };
}

pub(crate) use static_iterator_hack;

pub struct MvccLazyCursor<Clock: LogicalClock> {
    pub db: Arc<MvStore<Clock>>,
    current_pos: RefCell<CursorPosition>,
    /// Stateful MVCC table iterator if this is a table cursor.
    table_iterator: Option<MvccIterator<'static, RowID>>,
    /// Stateful MVCC index iterator if this is an index cursor.
    index_iterator: Option<MvccIterator<'static, SortableIndexKey>>,
    mv_cursor_type: MvccCursorType,
    table_id: MVTableId,
    tx_id: u64,
    /// Reusable immutable record, used to allow better allocation strategy.
    reusable_immutable_record: RefCell<Option<ImmutableRecord>>,
    btree_cursor: Box<dyn CursorTrait>,
    null_flag: bool,
    record_cursor: RefCell<RecordCursor>,
    next_rowid_lock: Arc<RwLock<()>>,
    state: RefCell<Option<MvccLazyCursorState>>,
    /// Dual-cursor peek state for proper iteration
    dual_peek: RefCell<DualCursorPeek>,
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
            current_pos: RefCell::new(CursorPosition::BeforeFirst),
            table_id,
            reusable_immutable_record: RefCell::new(None),
            btree_cursor,
            null_flag: false,
            record_cursor: RefCell::new(RecordCursor::new()),
            next_rowid_lock: Arc::new(RwLock::new(())),
            state: RefCell::new(None),
            dual_peek: RefCell::new(DualCursorPeek {
                mvcc_peek: CursorPeek::Uninitialized,
                btree_peek: CursorPeek::Uninitialized,
            }),
        })
    }

    /// Returns the current row as an immutable record.
    pub fn current_row(
        &self,
    ) -> Result<IOResult<Option<std::cell::Ref<'_, crate::types::ImmutableRecord>>>> {
        match *self.current_pos.borrow() {
            CursorPosition::Loaded {
                row_id: _,
                in_btree,
            } => {
                if in_btree {
                    let maybe_record = loop {
                        match self.btree_cursor.record()? {
                            IOResult::Done(maybe_record) => {
                                break maybe_record;
                            }
                            IOResult::IO(c) => {
                                c.wait(self.btree_cursor.get_pager().io.as_ref())?;
                                // FIXME: sync IO hack
                            }
                        }
                    };
                    Ok(IOResult::Done(maybe_record))
                } else {
                    let Some(row) = self.read_mvcc_current_row()? else {
                        return Ok(IOResult::Done(None));
                    };
                    {
                        let mut record = self.get_immutable_record_or_create();
                        let record = record.as_mut().ok_or(LimboError::InternalError(
                            "immutable record not initialized".to_string(),
                        ))?;
                        record.invalidate();
                        record.start_serialization(&row.data);
                    }

                    let record_ref =
                        Ref::filter_map(self.reusable_immutable_record.borrow(), |opt| {
                            opt.as_ref()
                        })
                        .ok()
                        .ok_or(LimboError::InternalError(
                            "immutable record not initialized".to_string(),
                        ))?;
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
        let row_id = match self.current_pos.borrow().clone() {
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

    pub fn get_next_rowid(&mut self) -> i64 {
        // lock so we don't get same two rowids
        let lock = self.next_rowid_lock.clone();
        let _lock = lock.write();
        let _ = self.last();
        match self.current_pos.borrow().clone() {
            CursorPosition::Loaded {
                row_id,
                in_btree: _,
            } => row_id.row_id.to_int_or_panic() + 1,
            CursorPosition::BeforeFirst => 1,
            CursorPosition::End => 1,
        }
    }

    fn get_immutable_record_or_create(&self) -> std::cell::RefMut<'_, Option<ImmutableRecord>> {
        let mut reusable_immutable_record = self.reusable_immutable_record.borrow_mut();
        if reusable_immutable_record.is_none() {
            let record = ImmutableRecord::new(1024);
            reusable_immutable_record.replace(record);
        }
        reusable_immutable_record
    }

    fn get_current_pos(&self) -> CursorPosition {
        self.current_pos.borrow().clone()
    }

    fn is_btree_allocated(&self) -> bool {
        let maybe_root_page = self.db.table_id_to_rootpage.get(&self.table_id);
        maybe_root_page.is_some_and(|entry| entry.value().is_some())
    }

    fn query_btree_version_is_valid(&self, key: &RowKey) -> bool {
        // If the row is not found in MVCC index, this means row_id is valid in btree
        matches!(
            self.db
                .find_row_last_version_state(self.table_id, key, self.tx_id),
            RowVersionState::NotFound
        )
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
        let mut peek = self.dual_peek.borrow_mut();
        peek.mvcc_peek = new_peek_state;
    }

    /// Advance btree cursor forward and set btree peek to the first valid row key (skipping rows shadowed by MVCC)
    fn advance_btree_forward(&mut self) -> Result<()> {
        if !self.is_btree_allocated() {
            let mut peek = self.dual_peek.borrow_mut();
            peek.btree_peek = CursorPeek::Exhausted;
            return Ok(());
        }
        let mut peek = self.dual_peek.borrow_mut();
        // If the btree is uninitialized, do the equivalent of rewind() to find the first valid row
        if peek.btree_uninitialized() {
            while let IOResult::IO(c) = self.btree_cursor.rewind()? {
                c.wait(self.btree_cursor.get_pager().io.as_ref())?; // FIXME: sync IO hack
            }
            let key = self.get_btree_current_key()?;
            match key {
                Some(k) if self.query_btree_version_is_valid(&k) => {
                    peek.btree_peek = CursorPeek::Row(k);
                    return Ok(());
                }
                Some(_) => {
                    // shadowed by MVCC, continue to next
                }
                None => {
                    peek.btree_peek = CursorPeek::Exhausted;
                    return Ok(());
                }
            }
        }
        loop {
            let found = loop {
                match self.btree_cursor.next()? {
                    IOResult::Done(f) => {
                        break f;
                    }
                    IOResult::IO(c) => {
                        c.wait(self.btree_cursor.get_pager().io.as_ref())?; // FIXME: sync IO hack
                    }
                }
            };
            if !found {
                peek.btree_peek = CursorPeek::Exhausted;
                return Ok(());
            }
            let key = self.get_btree_current_key()?;
            if let Some(key) = key {
                if self.query_btree_version_is_valid(&key) {
                    peek.btree_peek = CursorPeek::Row(key);
                    return Ok(());
                }
                // Row is shadowed by MVCC, continue to next
            } else {
                peek.btree_peek = CursorPeek::Exhausted;
                return Ok(());
            }
        }
    }

    /// Advance btree cursor backward and set btree peek to the first valid row key (skipping rows shadowed by MVCC)
    fn advance_btree_backward(&mut self) -> Result<()> {
        if !self.is_btree_allocated() {
            let mut peek = self.dual_peek.borrow_mut();
            peek.btree_peek = CursorPeek::Exhausted;
            return Ok(());
        }
        {
            let mut peek = self.dual_peek.borrow_mut();
            // If the btree is uninitialized, do the equivalent of last() to find the last valid row
            if peek.btree_uninitialized() {
                while let IOResult::IO(c) = self.btree_cursor.last()? {
                    c.wait(self.btree_cursor.get_pager().io.as_ref())?; // FIXME: sync IO hack
                }
                let key = self.get_btree_current_key()?;
                match key {
                    Some(k) if self.query_btree_version_is_valid(&k) => {
                        peek.btree_peek = CursorPeek::Row(k);
                        return Ok(());
                    }
                    Some(_) => {
                        // shadowed by MVCC, continue to prev
                    }
                    None => {
                        peek.btree_peek = CursorPeek::Exhausted;
                        return Ok(());
                    }
                }
            }
        }
        let mut peek = self.dual_peek.borrow_mut();
        loop {
            let found = loop {
                match self.btree_cursor.prev()? {
                    IOResult::Done(f) => {
                        break f;
                    }
                    IOResult::IO(c) => {
                        c.wait(self.btree_cursor.get_pager().io.as_ref())?; // FIXME: sync IO hack
                    }
                }
            };
            if !found {
                peek.btree_peek = CursorPeek::Exhausted;
                return Ok(());
            }
            let key = self.get_btree_current_key()?;
            if let Some(key) = key {
                if self.query_btree_version_is_valid(&key) {
                    peek.btree_peek = CursorPeek::Row(key);
                    return Ok(());
                }
                // Row is shadowed by MVCC, continue to prev
            } else {
                peek.btree_peek = CursorPeek::Exhausted;
                return Ok(());
            }
        }
    }

    /// Get the current key from btree cursor
    fn get_btree_current_key(&self) -> Result<Option<RowKey>> {
        match &self.mv_cursor_type {
            MvccCursorType::Table => {
                let maybe_rowid = loop {
                    match self.btree_cursor.rowid()? {
                        IOResult::Done(maybe_rowid) => {
                            break maybe_rowid;
                        }
                        IOResult::IO(c) => {
                            c.wait(self.btree_cursor.get_pager().io.as_ref())?; // FIXME: sync IO hack
                        }
                    }
                };
                Ok(maybe_rowid.map(RowKey::Int))
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
        let peek = self.dual_peek.borrow();
        let new_position = peek.cursor_position_from_next(self.table_id, dir);
        self.current_pos.replace(new_position);
    }

    /// Reset dual peek state (called on rewind/last/seek)
    fn reset_dual_peek(&mut self) {
        self.dual_peek.replace(DualCursorPeek {
            mvcc_peek: CursorPeek::Uninitialized,
            btree_peek: CursorPeek::Uninitialized,
        });
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
                self.index_iterator = Some(static_iterator_hack!(iter_box, SortableIndexKey));
            }
        }
    }
}

impl<Clock: LogicalClock + 'static> CursorTrait for MvccLazyCursor<Clock> {
    fn last(&mut self) -> Result<IOResult<()>> {
        let _ = self.table_iterator.take();
        let _ = self.index_iterator.take();
        self.reset_dual_peek();

        // Initialize btree cursor to last position
        self.advance_btree_backward()?;

        self.invalidate_record();
        self.current_pos.replace(CursorPosition::End);

        // Initialize MVCC iterator to last position
        match &self.mv_cursor_type {
            MvccCursorType::Table => match self
                .db
                .get_last_table_rowid(self.table_id, &mut self.table_iterator)
            {
                Some(k) => {
                    let mut peek = self.dual_peek.borrow_mut();
                    peek.mvcc_peek = CursorPeek::Row(k);
                }
                None => {
                    let mut peek = self.dual_peek.borrow_mut();
                    peek.mvcc_peek = CursorPeek::Exhausted;
                }
            },
            MvccCursorType::Index(_) => match self
                .db
                .get_last_index_rowid(self.table_id, &mut self.index_iterator)
            {
                Some(k) => {
                    let mut peek = self.dual_peek.borrow_mut();
                    peek.mvcc_peek = CursorPeek::Row(k);
                }
                None => {
                    let mut peek = self.dual_peek.borrow_mut();
                    peek.mvcc_peek = CursorPeek::Exhausted;
                }
            },
        };

        self.refresh_current_position(IterationDirection::Backwards);
        self.invalidate_record();

        Ok(IOResult::Done(()))
    }

    /// Move the cursor to the next row. Returns true if the cursor moved to the next row, false if the cursor is at the end of the table.
    ///
    /// Uses dual-cursor approach: only advances the cursor that was just consumed.
    fn next(&mut self) -> Result<IOResult<bool>> {
        // If BeforeFirst and peek not initialized, initialize the iterators and peek values
        let current_pos = self.get_current_pos();
        if matches!(current_pos, CursorPosition::BeforeFirst) {
            let uninitialized = self.dual_peek.borrow().both_uninitialized();
            if uninitialized {
                // Initialize MVCC iterator and get first peek
                self.init_mvcc_iterator_forward();
                self.advance_mvcc_iterator();
                self.advance_btree_forward()?;
            }
        }

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
                return Ok(IOResult::Done(false));
            }
        };

        // Advance cursors as needed and update peek state
        if need_advance_mvcc && !self.dual_peek.borrow().mvcc_exhausted() {
            self.advance_mvcc_iterator();
        }
        if need_advance_btree && !self.dual_peek.borrow().btree_exhausted() {
            self.advance_btree_forward()?;
        }

        self.refresh_current_position(IterationDirection::Forwards);
        self.invalidate_record();

        Ok(IOResult::Done(matches!(
            self.get_current_pos(),
            CursorPosition::Loaded { .. }
        )))
    }

    /// Move the cursor to the previous row. Returns true if the cursor moved, false if at the beginning.
    ///
    /// Uses dual-cursor approach: only advances the cursor that was just consumed.
    fn prev(&mut self) -> Result<IOResult<bool>> {
        // If End and peek not initialized, initialize via last()
        let current_pos = self.get_current_pos();
        if matches!(current_pos, CursorPosition::End) {
            let uninitialized = self.dual_peek.borrow().both_uninitialized();
            if uninitialized {
                return_if_io!(self.last());
            }
        }

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
                return Ok(IOResult::Done(false));
            }
        };

        // Advance cursors as needed and update peek state
        if need_advance_mvcc && !self.dual_peek.borrow().mvcc_exhausted() {
            self.advance_mvcc_iterator();
        }
        if need_advance_btree && !self.dual_peek.borrow().btree_exhausted() {
            self.advance_btree_backward()?;
        }

        self.refresh_current_position(IterationDirection::Backwards);
        self.invalidate_record();

        Ok(IOResult::Done(matches!(
            self.get_current_pos(),
            CursorPosition::Loaded { .. }
        )))
    }

    fn rowid(&self) -> Result<IOResult<Option<i64>>> {
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
                        let mut record_cursor = RecordCursor::new();
                        match sortable_key.key.last_value(&mut record_cursor) {
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

    fn record(
        &self,
    ) -> Result<IOResult<Option<std::cell::Ref<'_, crate::types::ImmutableRecord>>>> {
        self.current_row()
    }

    fn seek(&mut self, seek_key: SeekKey<'_>, op: SeekOp) -> Result<IOResult<SeekResult>> {
        // gt -> lower_bound bound excluded, we want first row after row_id
        // ge -> lower_bound bound included, we want first row equal to row_id or first row after row_id
        // lt -> upper_bound bound excluded, we want last row before row_id
        // le -> upper_bound bound included, we want last row equal to row_id or first row before row_id
        match seek_key {
            SeekKey::TableRowId(row_id) => {
                let rowid = RowID {
                    table_id: self.table_id,
                    row_id: RowKey::Int(row_id),
                };
                let inclusive = match op {
                    SeekOp::GT => false,
                    SeekOp::GE { eq_only: _ } => true,
                    SeekOp::LT => false,
                    SeekOp::LE { eq_only: _ } => true,
                };
                self.invalidate_record();
                let found_rowid = self.db.seek_rowid(
                    rowid.clone(),
                    inclusive,
                    op.iteration_direction(),
                    self.tx_id,
                    &mut self.table_iterator,
                );
                if let Some(found_rowid) = found_rowid {
                    self.current_pos.replace(CursorPosition::Loaded {
                        row_id: found_rowid.clone(),
                        in_btree: false,
                    });
                    if op.eq_only() {
                        if found_rowid.row_id == rowid.row_id {
                            Ok(IOResult::Done(SeekResult::Found))
                        } else {
                            Ok(IOResult::Done(SeekResult::NotFound))
                        }
                    } else {
                        Ok(IOResult::Done(SeekResult::Found))
                    }
                } else {
                    let forwards = matches!(op, SeekOp::GE { eq_only: _ } | SeekOp::GT);
                    if forwards {
                        let _ = self.last()?;
                    } else {
                        let _ = self.rewind()?;
                    }
                    Ok(IOResult::Done(SeekResult::NotFound))
                }
            }
            SeekKey::IndexKey(index_key) => {
                // TODO: we should seek in both btree and mvcc
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
                let key_info = index_info
                    .key_info
                    .iter()
                    .take(index_key.column_count())
                    .cloned()
                    .collect::<Vec<_>>();
                let sortable_key = SortableIndexKey::new_from_record(index_key.clone(), index_info);
                let inclusive = match op {
                    SeekOp::GT => false,
                    SeekOp::GE { eq_only: _ } => true,
                    SeekOp::LT => false,
                    SeekOp::LE { eq_only: _ } => true,
                };
                let found_rowid = self.db.seek_index(
                    self.table_id,
                    sortable_key.clone(),
                    inclusive,
                    op.iteration_direction(),
                    self.tx_id,
                    &mut self.index_iterator,
                );
                if let Some(found_rowid) = found_rowid {
                    self.current_pos.replace(CursorPosition::Loaded {
                        row_id: found_rowid.clone(),
                        in_btree: false,
                    });
                    if op.eq_only() {
                        let RowKey::Record(found_rowid_key) = found_rowid.row_id else {
                            panic!("Found rowid is not a record");
                        };
                        let cmp = compare_immutable(
                            index_key.get_values(),
                            found_rowid_key.key.get_values(),
                            &key_info,
                        );
                        if cmp.is_eq() {
                            Ok(IOResult::Done(SeekResult::Found))
                        } else {
                            Ok(IOResult::Done(SeekResult::NotFound))
                        }
                    } else {
                        Ok(IOResult::Done(SeekResult::Found))
                    }
                } else {
                    let forwards = matches!(op, SeekOp::GE { eq_only: _ } | SeekOp::GT);
                    if forwards {
                        let _ = self.last()?;
                    } else {
                        let _ = self.rewind()?;
                    }
                    Ok(IOResult::Done(SeekResult::NotFound))
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
            .ok_or(LimboError::InternalError(
                "BTreeKey should have a record".to_string(),
            ))?
            .get_payload()
            .to_vec();
        let num_columns = match key {
            BTreeKey::IndexKey(record) => record.column_count(),
            BTreeKey::TableRowId((_, record)) => record
                .as_ref()
                .ok_or(LimboError::InternalError(
                    "TableRowId should have a record".to_string(),
                ))?
                .column_count(),
        };
        let row = crate::mvcc::database::Row::new(row_id, record_buf, num_columns);

        self.current_pos.replace(CursorPosition::Loaded {
            row_id: row.id.clone(),
            in_btree: false,
        });
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
                    self.current_pos.replace(CursorPosition::BeforeFirst);
                })?;
        } else {
            self.db
                .insert_to_table_or_index(self.tx_id, row, maybe_index_id)
                .inspect_err(|_| {
                    self.current_pos.replace(CursorPosition::BeforeFirst);
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
            let row = crate::mvcc::database::Row::new(
                rowid.clone(),
                record.get_payload().to_vec(),
                record.column_count(),
            );
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
        if self.state.borrow().is_none() {
            self.invalidate_record();
            let int_key = match key {
                Value::Integer(i) => i,
                _ => unreachable!("btree tables are indexed by integers!"),
            };
            let inclusive = true;
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
            let exists = if let Some(rowid) = rowid {
                let RowKey::Int(rowid) = rowid.row_id else {
                    panic!("Rowid is not an integer in mvcc table cursor");
                };
                rowid == *int_key
            } else {
                false
            };
            if exists {
                self.current_pos.replace(CursorPosition::Loaded {
                    row_id: RowID {
                        table_id: self.table_id,
                        row_id: RowKey::Int(*int_key),
                    },
                    in_btree: false,
                });
                return Ok(IOResult::Done(exists));
            } else if self.is_btree_allocated() {
                self.state
                    .replace(Some(MvccLazyCursorState::Exists(ExistsState::ExistsBtree)));
            } else {
                return Ok(IOResult::Done(false));
            }
        }

        let Some(MvccLazyCursorState::Exists(ExistsState::ExistsBtree)) =
            self.state.borrow().clone()
        else {
            panic!("Invalid state {:?}", self.state.borrow());
        };
        assert!(
            self.is_btree_allocated(),
            "BTree should be allocated when we are in ExistsBtree state"
        );
        self.state.replace(None);
        let found = return_if_io!(self.btree_cursor.exists(key));
        Ok(IOResult::Done(found))
    }

    fn clear_btree(&mut self) -> Result<IOResult<Option<usize>>> {
        todo!()
    }

    fn btree_destroy(&mut self) -> Result<IOResult<Option<usize>>> {
        todo!()
    }

    fn count(&mut self) -> Result<IOResult<usize>> {
        todo!()
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
        let _ = self.table_iterator.take();
        let _ = self.index_iterator.take();
        self.reset_dual_peek();

        // First run btree_cursor rewind so that we don't need a explicit state machine.
        self.advance_btree_forward()?;

        self.invalidate_record();
        self.current_pos.replace(CursorPosition::BeforeFirst);

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
                self.index_iterator = Some(static_iterator_hack!(iter_box, SortableIndexKey));
            }
        }

        // Rewind mvcc iterator
        self.advance_mvcc_iterator();

        self.refresh_current_position(IterationDirection::Forwards);

        self.invalidate_record();
        Ok(IOResult::Done(()))
    }

    fn has_record(&self) -> bool {
        todo!()
    }

    fn set_has_record(&self, _has_record: bool) {
        todo!()
    }

    fn get_index_info(&self) -> &crate::types::IndexInfo {
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

    fn seek_to_last(&mut self) -> Result<IOResult<()>> {
        self.invalidate_record();
        let max_rowid = RowID {
            table_id: self.table_id,
            row_id: RowKey::Int(i64::MAX),
        };
        let inclusive = true;
        let rowid = self.db.seek_rowid(
            max_rowid,
            inclusive,
            IterationDirection::Forwards,
            self.tx_id,
            &mut self.table_iterator,
        );
        if let Some(rowid) = rowid {
            self.current_pos.replace(CursorPosition::Loaded {
                row_id: rowid,
                in_btree: false,
            });
        } else {
            self.current_pos.replace(CursorPosition::End);
        }
        Ok(IOResult::Done(()))
    }

    fn invalidate_record(&mut self) {
        self.get_immutable_record_or_create()
            .as_mut()
            .expect("immutable record should be initialized")
            .invalidate();
        self.record_cursor.borrow_mut().invalidate();
    }

    fn has_rowid(&self) -> bool {
        match &self.mv_cursor_type {
            MvccCursorType::Index(index_info) => index_info.has_rowid,
            MvccCursorType::Table => true, // currently we don't support WITHOUT ROWID tables
        }
    }

    fn record_cursor_mut(&self) -> std::cell::RefMut<'_, crate::types::RecordCursor> {
        self.record_cursor.borrow_mut()
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
