use crate::{
    storage::{
        pager::Pager,
        sqlite3_ondisk::{
            read_u32, read_varint, BTreeCell, PageContent, PageType, TableInteriorCell,
            TableLeafCell,
        },
    },
    translate::plan::IterationDirection,
    MvCursor,
};

use crate::{
    return_corrupt,
    types::{
        compare_immutable, CursorResult, ImmutableRecord, OwnedValue, RefValue, SeekKey, SeekOp,
    },
    LimboError, Result,
};

#[cfg(debug_assertions)]
use std::collections::HashSet;
use std::{
    cell::{Cell, Ref, RefCell},
    cmp::Ordering,
    pin::Pin,
    rc::Rc,
};

use super::{
    pager::PageRef,
    sqlite3_ondisk::{
        read_record, write_varint_to_vec, IndexInteriorCell, IndexLeafCell, OverflowCell,
        DATABASE_HEADER_SIZE,
    },
};

/// The B-Tree page header is 12 bytes for interior pages and 8 bytes for leaf pages.
///
/// +--------+-----------------+-----------------+-----------------+--------+----- ..... ----+
/// | Page   | First Freeblock | Cell Count      | Cell Content    | Frag.  | Right-most     |
/// | Type   | Offset          |                 | Area Start      | Bytes  | pointer        |
/// +--------+-----------------+-----------------+-----------------+--------+----- ..... ----+
///     0        1        2        3        4        5        6        7        8       11
///
pub mod offset {
    /// Type of the B-Tree page (u8).
    pub const BTREE_PAGE_TYPE: usize = 0;

    /// A pointer to the first freeblock (u16).
    ///
    /// This field of the B-Tree page header is an offset to the first freeblock, or zero if
    /// there are no freeblocks on the page.  A freeblock is a structure used to identify
    /// unallocated space within a B-Tree page, organized as a chain.
    ///
    /// Please note that freeblocks do not mean the regular unallocated free space to the left
    /// of the cell content area pointer, but instead blocks of at least 4
    /// bytes WITHIN the cell content area that are not in use due to e.g.
    /// deletions.
    pub const BTREE_FIRST_FREEBLOCK: usize = 1;

    /// The number of cells in the page (u16).
    pub const BTREE_CELL_COUNT: usize = 3;

    /// A pointer to first byte of cell allocated content from top (u16).
    ///
    /// SQLite strives to place cells as far toward the end of the b-tree page as it can, in
    /// order to leave space for future growth of the cell pointer array. This means that the
    /// cell content area pointer moves leftward as cells are added to the page.
    pub const BTREE_CELL_CONTENT_AREA: usize = 5;

    /// The number of fragmented bytes (u8).
    ///
    /// Fragments are isolated groups of 1, 2, or 3 unused bytes within the cell content area.
    pub const BTREE_FRAGMENTED_BYTES_COUNT: usize = 7;

    /// The right-most pointer (saved separately from cells) (u32)
    pub const BTREE_RIGHTMOST_PTR: usize = 8;
}

/// Maximum depth of an SQLite B-Tree structure. Any B-Tree deeper than
/// this will be declared corrupt. This value is calculated based on a
/// maximum database size of 2^31 pages a minimum fanout of 2 for a
/// root-node and 3 for all other internal nodes.
///
/// If a tree that appears to be taller than this is encountered, it is
/// assumed that the database is corrupt.
pub const BTCURSOR_MAX_DEPTH: usize = 20;

/// Evaluate a Result<CursorResult<T>>, if IO return IO.
macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr? {
            CursorResult::Ok(v) => v,
            CursorResult::IO => return Ok(CursorResult::IO),
        }
    };
}

/// Check if the page is unlocked, if not return IO.
macro_rules! return_if_locked {
    ($expr:expr) => {{
        if $expr.is_locked() {
            return Ok(CursorResult::IO);
        }
    }};
}

/// Validate cells in a page are in a valid state. Only in debug mode.
macro_rules! debug_validate_cells {
    ($page_contents:expr, $usable_space:expr) => {
        #[cfg(debug_assertions)]
        {
            debug_validate_cells_core($page_contents, $usable_space);
        }
    };
}
/// Check if the page is unlocked, if not return IO. If the page is not locked but not loaded, then try to load it.
macro_rules! return_if_locked_maybe_load {
    ($pager:expr, $expr:expr) => {{
        if $expr.is_locked() {
            return Ok(CursorResult::IO);
        }
        if !$expr.is_loaded() {
            $pager.load_page($expr.clone())?;
            return Ok(CursorResult::IO);
        }
    }};
}

/// State machine of destroy operations
/// Keep track of traversal so that it can be resumed when IO is encountered
#[derive(Debug, Clone)]
enum DestroyState {
    Start,
    LoadPage,
    ProcessPage,
    ClearOverflowPages { cell: BTreeCell },
    FreePage,
}

struct DestroyInfo {
    state: DestroyState,
}

#[derive(Debug, Clone)]
enum DeleteState {
    Start,
    LoadPage,
    FindCell,
    ClearOverflowPages {
        cell_idx: usize,
        cell: BTreeCell,
        original_child_pointer: Option<u32>,
    },
    InteriorNodeReplacement {
        cell_idx: usize,
        original_child_pointer: Option<u32>,
    },
    DropCell {
        cell_idx: usize,
    },
    CheckNeedsBalancing,
    StartBalancing {
        target_rowid: u64,
    },
    WaitForBalancingToComplete {
        target_rowid: u64,
    },
    SeekAfterBalancing {
        target_rowid: u64,
    },
    StackRetreat,
    Finish,
}

#[derive(Clone)]
struct DeleteInfo {
    state: DeleteState,
    balance_write_info: Option<WriteInfo>,
}

/// State machine of a write operation.
/// May involve balancing due to overflow.
#[derive(Debug, Clone, Copy)]
enum WriteState {
    Start,
    BalanceStart,
    BalanceNonRoot,
    BalanceNonRootWaitLoadPages,
    Finish,
}

enum ReadPayloadOverflow {
    ProcessPage {
        payload: Vec<u8>,
        next_page: u32,
        remaining_to_read: usize,
        page: PageRef,
    },
}

#[derive(Clone, Debug)]
pub enum BTreeKey<'a> {
    TableRowId((u64, Option<&'a ImmutableRecord>)),
    IndexKey(&'a ImmutableRecord),
}

impl BTreeKey<'_> {
    /// Create a new table rowid key from a rowid and an optional immutable record.
    /// The record is optional because it may not be available when the key is created.
    pub fn new_table_rowid(rowid: u64, record: Option<&ImmutableRecord>) -> BTreeKey<'_> {
        BTreeKey::TableRowId((rowid, record))
    }

    /// Create a new index key from an immutable record.
    pub fn new_index_key(record: &ImmutableRecord) -> BTreeKey<'_> {
        BTreeKey::IndexKey(record)
    }

    /// Get the record, if present. Index will always be present,
    fn get_record(&self) -> Option<&'_ ImmutableRecord> {
        match self {
            BTreeKey::TableRowId((_, record)) => *record,
            BTreeKey::IndexKey(record) => Some(record),
        }
    }

    /// Get the rowid, if present. Index will never be present.
    fn maybe_rowid(&self) -> Option<u64> {
        match self {
            BTreeKey::TableRowId((rowid, _)) => Some(*rowid),
            BTreeKey::IndexKey(_) => None,
        }
    }

    /// Assert that the key is an integer rowid and return it.
    fn to_rowid(&self) -> u64 {
        match self {
            BTreeKey::TableRowId((rowid, _)) => *rowid,
            BTreeKey::IndexKey(_) => panic!("BTreeKey::to_rowid called on IndexKey"),
        }
    }

    /// Assert that the key is an index key and return it.
    fn to_index_key_values(&self) -> &'_ Vec<RefValue> {
        match self {
            BTreeKey::TableRowId(_) => panic!("BTreeKey::to_index_key called on TableRowId"),
            BTreeKey::IndexKey(key) => key.get_values(),
        }
    }
}

#[derive(Clone)]
struct BalanceInfo {
    /// Old pages being balanced.
    pages_to_balance: Vec<PageRef>,
    /// Bookkeeping of the rightmost pointer so the offset::BTREE_RIGHTMOST_PTR can be updated.
    rightmost_pointer: *mut u8,
    /// Divider cells of old pages
    divider_cells: Vec<Vec<u8>>,
    /// Number of siblings being used to balance
    sibling_count: usize,
    /// First divider cell to remove that marks the first sibling
    first_divider_cell: usize,
}

#[derive(Clone)]
struct WriteInfo {
    /// State of the write operation state machine.
    state: WriteState,
    balance_info: RefCell<Option<BalanceInfo>>,
}

impl WriteInfo {
    fn new() -> WriteInfo {
        WriteInfo {
            state: WriteState::Start,
            balance_info: RefCell::new(None),
        }
    }
}

/// Holds the state machine for the operation that was in flight when the cursor
/// was suspended due to IO.
enum CursorState {
    None,
    Read(ReadPayloadOverflow),
    Write(WriteInfo),
    Destroy(DestroyInfo),
    Delete(DeleteInfo),
}

impl CursorState {
    fn write_info(&self) -> Option<&WriteInfo> {
        match self {
            CursorState::Write(x) => Some(x),
            _ => None,
        }
    }
    fn mut_write_info(&mut self) -> Option<&mut WriteInfo> {
        match self {
            CursorState::Write(x) => Some(x),
            _ => None,
        }
    }

    fn destroy_info(&self) -> Option<&DestroyInfo> {
        match self {
            CursorState::Destroy(x) => Some(x),
            _ => None,
        }
    }
    fn mut_destroy_info(&mut self) -> Option<&mut DestroyInfo> {
        match self {
            CursorState::Destroy(x) => Some(x),
            _ => None,
        }
    }

    fn delete_info(&self) -> Option<&DeleteInfo> {
        match self {
            CursorState::Delete(x) => Some(x),
            _ => None,
        }
    }

    fn mut_delete_info(&mut self) -> Option<&mut DeleteInfo> {
        match self {
            CursorState::Delete(x) => Some(x),
            _ => None,
        }
    }
}

enum OverflowState {
    Start,
    ProcessPage { next_page: u32 },
    Done,
}

pub struct BTreeCursor {
    /// The multi-version cursor that is used to read and write to the database file.
    mv_cursor: Option<Rc<RefCell<MvCursor>>>,
    /// The pager that is used to read and write to the database file.
    pager: Rc<Pager>,
    /// Page id of the root page used to go back up fast.
    root_page: usize,
    /// Rowid and record are stored before being consumed.
    rowid: Cell<Option<u64>>,
    null_flag: bool,
    /// Index internal pages are consumed on the way up, so we store going upwards flag in case
    /// we just moved to a parent page and the parent page is an internal index page which requires
    /// to be consumed.
    going_upwards: bool,
    /// Information maintained across execution attempts when an operation yields due to I/O.
    state: CursorState,
    /// Information maintained while freeing overflow pages. Maintained separately from cursor state since
    /// any method could require freeing overflow pages
    overflow_state: Option<OverflowState>,
    /// Page stack used to traverse the btree.
    /// Each cursor has a stack because each cursor traverses the btree independently.
    stack: PageStack,
    /// Reusable immutable record, used to allow better allocation strategy.
    reusable_immutable_record: RefCell<Option<ImmutableRecord>>,
    empty_record: Cell<bool>,
}

/// Stack of pages representing the tree traversal order.
/// current_page represents the current page being used in the tree and current_page - 1 would be
/// the parent. Using current_page + 1 or higher is undefined behaviour.
struct PageStack {
    /// Pointer to the current page being consumed
    current_page: Cell<i32>,
    /// List of pages in the stack. Root page will be in index 0
    stack: RefCell<[Option<PageRef>; BTCURSOR_MAX_DEPTH + 1]>,
    /// List of cell indices in the stack.
    /// cell_indices[current_page] is the current cell index being consumed. Similarly
    /// cell_indices[current_page-1] is the cell index of the parent of the current page
    /// that we save in case of going back up.
    /// There are two points that need special attention:
    ///  If cell_indices[current_page] = -1, it indicates that the current iteration has reached the start of the current_page
    ///  If cell_indices[current_page] = `cell_count`, it means that the current iteration has reached the end of the current_page
    cell_indices: RefCell<[i32; BTCURSOR_MAX_DEPTH + 1]>,
}

struct CellArray {
    cells: Vec<&'static mut [u8]>, // TODO(pere): make this with references

    number_of_cells_per_page: Vec<u16>, // number of cells in each page
}

impl BTreeCursor {
    pub fn new(
        mv_cursor: Option<Rc<RefCell<MvCursor>>>,
        pager: Rc<Pager>,
        root_page: usize,
    ) -> Self {
        Self {
            mv_cursor,
            pager,
            root_page,
            rowid: Cell::new(None),
            null_flag: false,
            going_upwards: false,
            state: CursorState::None,
            overflow_state: None,
            stack: PageStack {
                current_page: Cell::new(-1),
                cell_indices: RefCell::new([0; BTCURSOR_MAX_DEPTH + 1]),
                stack: RefCell::new([const { None }; BTCURSOR_MAX_DEPTH + 1]),
            },
            reusable_immutable_record: RefCell::new(None),
            empty_record: Cell::new(true),
        }
    }

    /// Check if the table is empty.
    /// This is done by checking if the root page has no cells.
    fn is_empty_table(&self) -> Result<CursorResult<bool>> {
        if let Some(mv_cursor) = &self.mv_cursor {
            let mv_cursor = mv_cursor.borrow();
            return Ok(CursorResult::Ok(mv_cursor.is_empty()));
        }
        let page = self.pager.read_page(self.root_page)?;
        return_if_locked!(page);

        let cell_count = page.get().contents.as_ref().unwrap().cell_count();
        Ok(CursorResult::Ok(cell_count == 0))
    }

    /// Move the cursor to the previous record and return it.
    /// Used in backwards iteration.
    fn get_prev_record(
        &mut self,
        predicate: Option<(SeekKey<'_>, SeekOp)>,
    ) -> Result<CursorResult<Option<u64>>> {
        loop {
            let page = self.stack.top();
            let cell_idx = self.stack.current_cell_index();

            // moved to beginning of current page
            // todo: find a better way to flag moved to end or begin of page
            if self.stack.current_cell_index_less_than_min() {
                loop {
                    if self.stack.current_cell_index() >= 0 {
                        break;
                    }
                    if self.stack.has_parent() {
                        self.going_upwards = true;
                        self.stack.pop();
                    } else {
                        // moved to begin of btree
                        return Ok(CursorResult::Ok(None));
                    }
                }
                // continue to next loop to get record from the new page
                continue;
            }

            let cell_idx = cell_idx as usize;
            return_if_locked!(page);
            if !page.is_loaded() {
                self.pager.load_page(page.clone())?;
                return Ok(CursorResult::IO);
            }
            let contents = page.get().contents.as_ref().unwrap();

            let cell_count = contents.cell_count();

            // If we are at the end of the page and we haven't just come back from the right child,
            // we now need to move to the rightmost child.
            if cell_idx as i32 == i32::MAX && !self.going_upwards {
                let rightmost_pointer = contents.rightmost_pointer();
                if let Some(rightmost_pointer) = rightmost_pointer {
                    self.stack
                        .push_backwards(self.pager.read_page(rightmost_pointer as usize)?);
                    continue;
                }
            }

            let cell_idx = if cell_idx >= cell_count {
                self.stack.set_cell_index(cell_count as i32 - 1);
                cell_count - 1
            } else {
                cell_idx
            };
            let cell = contents.cell_get(
                cell_idx,
                payload_overflow_threshold_max(contents.page_type(), self.usable_space() as u16),
                payload_overflow_threshold_min(contents.page_type(), self.usable_space() as u16),
                self.usable_space(),
            )?;

            match cell {
                BTreeCell::TableInteriorCell(TableInteriorCell {
                    _left_child_page,
                    _rowid,
                }) => {
                    let mem_page = self.pager.read_page(_left_child_page as usize)?;
                    self.stack.push_backwards(mem_page);
                    continue;
                }
                BTreeCell::TableLeafCell(TableLeafCell {
                    _rowid,
                    _payload,
                    first_overflow_page,
                    payload_size,
                }) => {
                    if let Some(next_page) = first_overflow_page {
                        return_if_io!(self.process_overflow_read(_payload, next_page, payload_size))
                    } else {
                        crate::storage::sqlite3_ondisk::read_record(
                            _payload,
                            self.get_immutable_record_or_create().as_mut().unwrap(),
                        )?
                    };
                    self.stack.retreat();
                    return Ok(CursorResult::Ok(Some(_rowid)));
                }
                BTreeCell::IndexInteriorCell(IndexInteriorCell {
                    payload,
                    left_child_page,
                    first_overflow_page,
                    payload_size,
                }) => {
                    if !self.going_upwards {
                        // In backwards iteration, if we haven't just moved to this interior node from the
                        // right child, but instead are about to move to the left child, we need to retreat
                        // so that we don't come back to this node again.
                        // For example:
                        // this parent: key 666
                        // left child has: key 663, key 664, key 665
                        // we need to move to the previous parent (with e.g. key 662) when iterating backwards.
                        self.stack.retreat();
                        let mem_page = self.pager.read_page(left_child_page as usize)?;
                        self.stack.push(mem_page);
                        // use cell_index = i32::MAX to tell next loop to go to the end of the current page
                        self.stack.set_cell_index(i32::MAX);
                        continue;
                    }
                    if let Some(next_page) = first_overflow_page {
                        return_if_io!(self.process_overflow_read(payload, next_page, payload_size))
                    } else {
                        crate::storage::sqlite3_ondisk::read_record(
                            payload,
                            self.get_immutable_record_or_create().as_mut().unwrap(),
                        )?
                    };

                    // Going upwards = we just moved to an interior cell from the right child.
                    // On the first pass we must take the record from the interior cell (since unlike table btrees, index interior cells have payloads)
                    // We then mark going_upwards=false so that we go back down the tree on the next invocation.
                    self.going_upwards = false;
                    if predicate.is_none() {
                        let rowid = match self.get_immutable_record().as_ref().unwrap().last_value()
                        {
                            Some(RefValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok(Some(rowid)));
                    }

                    let (key, op) = predicate.as_ref().unwrap();
                    let SeekKey::IndexKey(index_key) = key else {
                        unreachable!("index seek key should be a record");
                    };
                    let order = {
                        let record = self.get_immutable_record();
                        let record = record.as_ref().unwrap();
                        let record_values = record.get_values();
                        let record_slice_same_num_cols =
                            &record_values[..index_key.get_values().len()];
                        let order =
                            compare_immutable(record_slice_same_num_cols, index_key.get_values());
                        order
                    };

                    let found = match op {
                        SeekOp::EQ => order.is_eq(),
                        SeekOp::LE => order.is_le(),
                        SeekOp::LT => order.is_lt(),
                        _ => unreachable!("Seek GT/GE should not happen in get_prev_record() because we are iterating backwards"),
                    };
                    if found {
                        let rowid = match self.get_immutable_record().as_ref().unwrap().last_value()
                        {
                            Some(RefValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok(Some(rowid)));
                    } else {
                        continue;
                    }
                }
                BTreeCell::IndexLeafCell(IndexLeafCell {
                    payload,
                    first_overflow_page,
                    payload_size,
                }) => {
                    if let Some(next_page) = first_overflow_page {
                        return_if_io!(self.process_overflow_read(payload, next_page, payload_size))
                    } else {
                        crate::storage::sqlite3_ondisk::read_record(
                            payload,
                            self.get_immutable_record_or_create().as_mut().unwrap(),
                        )?
                    };

                    self.stack.retreat();
                    if predicate.is_none() {
                        let rowid = match self.get_immutable_record().as_ref().unwrap().last_value()
                        {
                            Some(RefValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok(Some(rowid)));
                    }
                    let (key, op) = predicate.as_ref().unwrap();
                    let SeekKey::IndexKey(index_key) = key else {
                        unreachable!("index seek key should be a record");
                    };
                    let order = {
                        let record = self.get_immutable_record();
                        let record = record.as_ref().unwrap();
                        let record_values = record.get_values();
                        let record_slice_same_num_cols =
                            &record_values[..index_key.get_values().len()];
                        let order =
                            compare_immutable(record_slice_same_num_cols, index_key.get_values());
                        order
                    };
                    let found = match op {
                        SeekOp::EQ => order.is_eq(),
                        SeekOp::LE => order.is_le(),
                        SeekOp::LT => order.is_lt(),
                        _ => unreachable!("Seek GT/GE should not happen in get_prev_record() because we are iterating backwards"),
                    };
                    if found {
                        let rowid = match self.get_immutable_record().as_ref().unwrap().last_value()
                        {
                            Some(RefValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok(Some(rowid)));
                    } else {
                        continue;
                    }
                }
            }
        }
    }

    /// Reads the record of a cell that has overflow pages. This is a state machine that requires to be called until completion so everything
    /// that calls this function should be reentrant.
    fn process_overflow_read(
        &mut self,
        payload: &'static [u8],
        start_next_page: u32,
        payload_size: u64,
    ) -> Result<CursorResult<()>> {
        let res = match &mut self.state {
            CursorState::None => {
                tracing::debug!("start reading overflow page payload_size={}", payload_size);
                let page = self.pager.read_page(start_next_page as usize)?;
                self.state = CursorState::Read(ReadPayloadOverflow::ProcessPage {
                    payload: payload.to_vec(),
                    next_page: start_next_page,
                    remaining_to_read: payload_size as usize - payload.len(),
                    page,
                });
                CursorResult::IO
            }
            CursorState::Read(ReadPayloadOverflow::ProcessPage {
                payload,
                next_page,
                remaining_to_read,
                page,
            }) => {
                if page.is_locked() {
                    return Ok(CursorResult::IO);
                }
                tracing::debug!("reading overflow page {} {}", next_page, remaining_to_read);
                let contents = page.get_contents();
                // The first four bytes of each overflow page are a big-endian integer which is the page number of the next page in the chain, or zero for the final page in the chain.
                let next = contents.read_u32_no_offset(0);
                let buf = contents.as_ptr();
                let usable_space = self.pager.usable_space();
                let to_read = (*remaining_to_read).min(usable_space - 4);
                payload.extend_from_slice(&buf[4..4 + to_read]);
                *remaining_to_read -= to_read;
                if *remaining_to_read == 0 || next == 0 {
                    assert!(
                        *remaining_to_read == 0 && next == 0,
                        "we can't have more pages to read while also have read everything"
                    );
                    let mut payload_swap = Vec::new();
                    std::mem::swap(payload, &mut payload_swap);
                    CursorResult::Ok(payload_swap)
                } else {
                    let new_page = self.pager.read_page(next as usize)?;
                    *page = new_page;
                    *next_page = next;
                    CursorResult::IO
                }
            }
            _ => unreachable!(),
        };
        match res {
            CursorResult::Ok(payload) => {
                {
                    let mut reuse_immutable = self.get_immutable_record_or_create();
                    crate::storage::sqlite3_ondisk::read_record(
                        &payload,
                        reuse_immutable.as_mut().unwrap(),
                    )?;
                }
                self.state = CursorState::None;
                Ok(CursorResult::Ok(()))
            }
            CursorResult::IO => Ok(CursorResult::IO),
        }
    }

    /// Move the cursor to the next record and return it.
    /// Used in forwards iteration, which is the default.
    fn get_next_record(
        &mut self,
        predicate: Option<(SeekKey<'_>, SeekOp)>,
    ) -> Result<CursorResult<Option<u64>>> {
        if let Some(mv_cursor) = &self.mv_cursor {
            let mut mv_cursor = mv_cursor.borrow_mut();
            let rowid = mv_cursor.current_row_id();
            match rowid {
                Some(rowid) => {
                    let record = mv_cursor.current_row().unwrap().unwrap();
                    crate::storage::sqlite3_ondisk::read_record(
                        &record.data,
                        self.get_immutable_record_or_create().as_mut().unwrap(),
                    )?;
                    mv_cursor.forward();
                    return Ok(CursorResult::Ok(Some(rowid.row_id)));
                }
                None => return Ok(CursorResult::Ok(None)),
            }
        }
        loop {
            let mem_page_rc = self.stack.top();
            let cell_idx = self.stack.current_cell_index() as usize;

            tracing::trace!("current id={} cell={}", mem_page_rc.get().id, cell_idx);
            return_if_locked!(mem_page_rc);
            if !mem_page_rc.is_loaded() {
                self.pager.load_page(mem_page_rc.clone())?;
                return Ok(CursorResult::IO);
            }
            let mem_page = mem_page_rc.get();

            let contents = mem_page.contents.as_ref().unwrap();

            if cell_idx == contents.cell_count() {
                // do rightmost
                let has_parent = self.stack.has_parent();
                match contents.rightmost_pointer() {
                    Some(right_most_pointer) => {
                        self.stack.advance();
                        let mem_page = self.pager.read_page(right_most_pointer as usize)?;
                        self.stack.push(mem_page);
                        continue;
                    }
                    None => {
                        if has_parent {
                            tracing::trace!("moving simple upwards");
                            self.going_upwards = true;
                            self.stack.pop();
                            continue;
                        } else {
                            return Ok(CursorResult::Ok(None));
                        }
                    }
                }
            }

            if cell_idx > contents.cell_count() {
                // end
                let has_parent = self.stack.current() > 0;
                if has_parent {
                    tracing::debug!("moving upwards");
                    self.going_upwards = true;
                    self.stack.pop();
                    continue;
                } else {
                    return Ok(CursorResult::Ok(None));
                }
            }
            assert!(cell_idx < contents.cell_count());

            let cell = contents.cell_get(
                cell_idx,
                payload_overflow_threshold_max(contents.page_type(), self.usable_space() as u16),
                payload_overflow_threshold_min(contents.page_type(), self.usable_space() as u16),
                self.usable_space(),
            )?;
            match &cell {
                BTreeCell::TableInteriorCell(TableInteriorCell {
                    _left_child_page,
                    _rowid,
                }) => {
                    assert!(predicate.is_none());
                    self.stack.advance();
                    let mem_page = self.pager.read_page(*_left_child_page as usize)?;
                    self.stack.push(mem_page);
                    continue;
                }
                BTreeCell::TableLeafCell(TableLeafCell {
                    _rowid,
                    _payload,
                    payload_size,
                    first_overflow_page,
                }) => {
                    assert!(predicate.is_none());
                    if let Some(next_page) = first_overflow_page {
                        return_if_io!(self.process_overflow_read(
                            _payload,
                            *next_page,
                            *payload_size
                        ))
                    } else {
                        crate::storage::sqlite3_ondisk::read_record(
                            _payload,
                            self.get_immutable_record_or_create().as_mut().unwrap(),
                        )?
                    };
                    self.stack.advance();
                    return Ok(CursorResult::Ok(Some(*_rowid)));
                }
                BTreeCell::IndexInteriorCell(IndexInteriorCell {
                    payload,
                    left_child_page,
                    first_overflow_page,
                    payload_size,
                }) => {
                    if !self.going_upwards {
                        let mem_page = self.pager.read_page(*left_child_page as usize)?;
                        self.stack.push(mem_page);
                        continue;
                    }
                    if let Some(next_page) = first_overflow_page {
                        return_if_io!(self.process_overflow_read(
                            payload,
                            *next_page,
                            *payload_size
                        ))
                    } else {
                        crate::storage::sqlite3_ondisk::read_record(
                            payload,
                            self.get_immutable_record_or_create().as_mut().unwrap(),
                        )?
                    };

                    self.going_upwards = false;
                    self.stack.advance();
                    if predicate.is_none() {
                        let rowid = match self.get_immutable_record().as_ref().unwrap().last_value()
                        {
                            Some(RefValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok(Some(rowid)));
                    }

                    let (key, op) = predicate.as_ref().unwrap();
                    let SeekKey::IndexKey(index_key) = key else {
                        unreachable!("index seek key should be a record");
                    };
                    let order = compare_immutable(
                        &self.get_immutable_record().as_ref().unwrap().get_values(),
                        index_key.get_values(),
                    );
                    let found = match op {
                        SeekOp::GT => order.is_gt(),
                        SeekOp::GE => order.is_ge(),
                        SeekOp::EQ => order.is_eq(),
                        _ => unreachable!("Seek LE/LT should not happen in get_next_record() because we are iterating forwards"),
                    };
                    if found {
                        let rowid = match self.get_immutable_record().as_ref().unwrap().last_value()
                        {
                            Some(RefValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok(Some(rowid)));
                    } else {
                        continue;
                    }
                }
                BTreeCell::IndexLeafCell(IndexLeafCell {
                    payload,
                    first_overflow_page,
                    payload_size,
                }) => {
                    if let Some(next_page) = first_overflow_page {
                        return_if_io!(self.process_overflow_read(
                            payload,
                            *next_page,
                            *payload_size
                        ))
                    } else {
                        crate::storage::sqlite3_ondisk::read_record(
                            payload,
                            self.get_immutable_record_or_create().as_mut().unwrap(),
                        )?
                    };

                    self.stack.advance();
                    if predicate.is_none() {
                        let rowid = match self.get_immutable_record().as_ref().unwrap().last_value()
                        {
                            Some(RefValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok(Some(rowid)));
                    }
                    let (key, op) = predicate.as_ref().unwrap();
                    let SeekKey::IndexKey(index_key) = key else {
                        unreachable!("index seek key should be a record");
                    };
                    let order = compare_immutable(
                        &self.get_immutable_record().as_ref().unwrap().get_values(),
                        index_key.get_values(),
                    );
                    let found = match op {
                        SeekOp::GT => order.is_lt(),
                        SeekOp::GE => order.is_le(),
                        SeekOp::EQ => order.is_le(),
                        _ => todo!("not implemented: {:?}", op),
                    };
                    if found {
                        let rowid = match self.get_immutable_record().as_ref().unwrap().last_value()
                        {
                            Some(RefValue::Integer(rowid)) => *rowid as u64,
                            _ => unreachable!("index cells should have an integer rowid"),
                        };
                        return Ok(CursorResult::Ok(Some(rowid)));
                    } else {
                        continue;
                    }
                }
            }
        }
    }

    /// Move the cursor to the record that matches the seek key and seek operation.
    /// This may be used to seek to a specific record in a point query (e.g. SELECT * FROM table WHERE col = 10)
    /// or e.g. find the first record greater than the seek key in a range query (e.g. SELECT * FROM table WHERE col > 10).
    /// We don't include the rowid in the comparison and that's why the last value from the record is not included.
    fn do_seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<CursorResult<Option<u64>>> {
        let cell_iter_dir = op.iteration_direction();
        return_if_io!(self.move_to(key.clone(), op.clone()));

        {
            let page = self.stack.top();
            return_if_locked!(page);

            let contents = page.get().contents.as_ref().unwrap();

            let cell_count = contents.cell_count();
            let mut cell_idx: isize = if cell_iter_dir == IterationDirection::Forwards {
                0
            } else {
                cell_count as isize - 1
            };
            let end = if cell_iter_dir == IterationDirection::Forwards {
                cell_count as isize - 1
            } else {
                0
            };
            self.stack.set_cell_index(cell_idx as i32);
            while cell_count > 0
                && (if cell_iter_dir == IterationDirection::Forwards {
                    cell_idx <= end
                } else {
                    cell_idx >= end
                })
            {
                let cell = contents.cell_get(
                    cell_idx as usize,
                    payload_overflow_threshold_max(
                        contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    payload_overflow_threshold_min(
                        contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    self.usable_space(),
                )?;
                match &cell {
                    BTreeCell::TableLeafCell(TableLeafCell {
                        _rowid: cell_rowid,
                        _payload: payload,
                        first_overflow_page,
                        payload_size,
                    }) => {
                        let SeekKey::TableRowId(rowid_key) = key else {
                            unreachable!("table seek key should be a rowid");
                        };
                        let found = match op {
                            SeekOp::GT => *cell_rowid > rowid_key,
                            SeekOp::GE => *cell_rowid >= rowid_key,
                            SeekOp::EQ => *cell_rowid == rowid_key,
                            SeekOp::LE => *cell_rowid <= rowid_key,
                            SeekOp::LT => *cell_rowid < rowid_key,
                        };
                        if found {
                            if let Some(next_page) = first_overflow_page {
                                return_if_io!(self.process_overflow_read(
                                    payload,
                                    *next_page,
                                    *payload_size
                                ))
                            } else {
                                crate::storage::sqlite3_ondisk::read_record(
                                    payload,
                                    self.get_immutable_record_or_create().as_mut().unwrap(),
                                )?
                            };
                            self.stack.next_cell_in_direction(cell_iter_dir);
                            return Ok(CursorResult::Ok(Some(*cell_rowid)));
                        } else {
                            self.stack.next_cell_in_direction(cell_iter_dir);
                        }
                    }
                    BTreeCell::IndexLeafCell(IndexLeafCell {
                        payload,
                        first_overflow_page,
                        payload_size,
                    }) => {
                        let SeekKey::IndexKey(index_key) = key else {
                            unreachable!("index seek key should be a record");
                        };
                        if let Some(next_page) = first_overflow_page {
                            return_if_io!(self.process_overflow_read(
                                payload,
                                *next_page,
                                *payload_size
                            ))
                        } else {
                            crate::storage::sqlite3_ondisk::read_record(
                                payload,
                                self.get_immutable_record_or_create().as_mut().unwrap(),
                            )?
                        };
                        let record = self.get_immutable_record();
                        let record = record.as_ref().unwrap();
                        let record_slice_equal_number_of_cols =
                            &record.get_values().as_slice()[..index_key.get_values().len()];
                        let order = record_slice_equal_number_of_cols.cmp(index_key.get_values());
                        let found = match op {
                            SeekOp::GT => order.is_gt(),
                            SeekOp::GE => order.is_ge(),
                            SeekOp::EQ => order.is_eq(),
                            SeekOp::LE => order.is_le(),
                            SeekOp::LT => order.is_lt(),
                        };
                        self.stack.next_cell_in_direction(cell_iter_dir);
                        if found {
                            let rowid = match record.last_value() {
                                Some(RefValue::Integer(rowid)) => *rowid as u64,
                                _ => unreachable!("index cells should have an integer rowid"),
                            };
                            return Ok(CursorResult::Ok(Some(rowid)));
                        }
                    }
                    cell_type => {
                        unreachable!("unexpected cell type: {:?}", cell_type);
                    }
                }
                if cell_iter_dir == IterationDirection::Forwards {
                    cell_idx += 1;
                } else {
                    cell_idx -= 1;
                }
            }
        }

        // We have now iterated over all cells in the leaf page and found no match.
        let is_index = matches!(key, SeekKey::IndexKey(_));
        if is_index {
            // Unlike tables, indexes store payloads in interior cells as well. self.move_to() always moves to a leaf page, so there are cases where we need to
            // move back up to the parent interior cell and get the next record from there to perform a correct seek.
            // an example of how this can occur:
            //
            // we do an index seek for key K with cmp = SeekOp::GT, meaning we want to seek to the first key that is greater than K.
            // in self.move_to(), we encounter an interior cell with key K' = K+2, and move the left child page, which is a leaf page.
            // the reason we move to the left child page is that we know that in an index, all keys in the left child page are less than K' i.e. less than K+2,
            // meaning that the left subtree may contain a key greater than K, e.g. K+1. however, it is possible that it doesn't, in which case the correct
            // next key is K+2, which is in the parent interior cell.
            //
            // In the seek() method, once we have landed in the leaf page and find that there is no cell with a key greater than K,
            // if we were to return Ok(CursorResult::Ok((None, None))), self.record would be None, which is incorrect, because we already know
            // that there is a record with a key greater than K (K' = K+2) in the parent interior cell. Hence, we need to move back up the tree
            // and get the next matching record from there.
            match op.iteration_direction() {
                IterationDirection::Forwards => {
                    return self.get_next_record(Some((key, op)));
                }
                IterationDirection::Backwards => {
                    return self.get_prev_record(Some((key, op)));
                }
            }
        }

        Ok(CursorResult::Ok(None))
    }

    /// Move the cursor to the root page of the btree.
    fn move_to_root(&mut self) {
        tracing::trace!("move_to_root({})", self.root_page);
        let mem_page = self.pager.read_page(self.root_page).unwrap();
        self.stack.clear();
        self.stack.push(mem_page);
    }

    /// Move the cursor to the rightmost record in the btree.
    fn move_to_rightmost(&mut self) -> Result<CursorResult<()>> {
        self.move_to_root();

        loop {
            let mem_page = self.stack.top();
            let page_idx = mem_page.get().id;
            let page = self.pager.read_page(page_idx)?;
            return_if_locked!(page);
            let contents = page.get().contents.as_ref().unwrap();
            if contents.is_leaf() {
                if contents.cell_count() > 0 {
                    self.stack.set_cell_index(contents.cell_count() as i32 - 1);
                }
                return Ok(CursorResult::Ok(()));
            }

            match contents.rightmost_pointer() {
                Some(right_most_pointer) => {
                    self.stack.set_cell_index(contents.cell_count() as i32 + 1);
                    let mem_page = self.pager.read_page(right_most_pointer as usize)?;
                    self.stack.push(mem_page);
                    continue;
                }

                None => {
                    unreachable!("interior page should have a rightmost pointer");
                }
            }
        }
    }

    pub fn move_to(&mut self, key: SeekKey<'_>, cmp: SeekOp) -> Result<CursorResult<()>> {
        assert!(self.mv_cursor.is_none());
        tracing::trace!("move_to(key={:?} cmp={:?})", key, cmp);
        tracing::trace!("backtrace: {}", std::backtrace::Backtrace::force_capture());
        // For a table with N rows, we can find any row by row id in O(log(N)) time by starting at the root page and following the B-tree pointers.
        // B-trees consist of interior pages and leaf pages. Interior pages contain pointers to other pages, while leaf pages contain the actual row data.
        //
        // Conceptually, each Interior Cell in a interior page has a rowid and a left child node, and the page itself has a right-most child node.
        // Example: consider an interior page that contains cells C1(rowid=10), C2(rowid=20), C3(rowid=30).
        // - All rows with rowids <= 10 are in the left child node of C1.
        // - All rows with rowids > 10 and <= 20 are in the left child node of C2.
        // - All rows with rowids > 20 and <= 30 are in the left child node of C3.
        // - All rows with rowids > 30 are in the right-most child node of the page.
        //
        // There will generally be multiple levels of interior pages before we reach a leaf page,
        // so we need to follow the interior page pointers until we reach the leaf page that contains the row we are looking for (if it exists).
        //
        // Here's a high-level overview of the algorithm:
        // 1. Since we start at the root page, its cells are all interior cells.
        // 2. We scan the interior cells until we find a cell whose rowid is greater than or equal to the rowid we are looking for.
        // 3. Follow the left child pointer of the cell we found in step 2.
        //    a. In case none of the cells in the page have a rowid greater than or equal to the rowid we are looking for,
        //       we follow the right-most child pointer of the page instead (since all rows with rowids greater than the rowid we are looking for are in the right-most child node).
        // 4. We are now at a new page. If it's another interior page, we repeat the process from step 2. If it's a leaf page, we continue to step 5.
        // 5. We scan the leaf cells in the leaf page until we find the cell whose rowid is equal to the rowid we are looking for.
        //    This cell contains the actual data we are looking for.
        // 6. If we find the cell, we return the record. Otherwise, we return an empty result.
        self.move_to_root();

        let iter_dir = cmp.iteration_direction();

        loop {
            let page = self.stack.top();
            return_if_locked!(page);

            let contents = page.get().contents.as_ref().unwrap();
            if contents.is_leaf() {
                return Ok(CursorResult::Ok(()));
            }

            let mut found_cell = false;
            for cell_idx in 0..contents.cell_count() {
                let cell = contents.cell_get(
                    cell_idx,
                    payload_overflow_threshold_max(
                        contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    payload_overflow_threshold_min(
                        contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    self.usable_space(),
                )?;
                match &cell {
                    BTreeCell::TableInteriorCell(TableInteriorCell {
                        _left_child_page,
                        _rowid: cell_rowid,
                    }) => {
                        let SeekKey::TableRowId(rowid_key) = key else {
                            unreachable!("table seek key should be a rowid");
                        };
                        // in sqlite btrees left child pages have <= keys.
                        // table btrees can have a duplicate rowid in the interior cell, so for example if we are looking for rowid=10,
                        // and we find an interior cell with rowid=10, we need to move to the left page since (due to the <= rule of sqlite btrees)
                        // the left page may have a rowid=10.
                        // Logic table for determining if target leaf page is in left subtree
                        //
                        // Forwards iteration (looking for first match in tree):
                        // OP  | Current Cell vs Seek Key   | Action?  | Explanation
                        // GT  | >                          | go left  | First > key is in left subtree
                        // GT  | = or <                     | go right | First > key is in right subtree
                        // GE  | > or =                     | go left  | First >= key is in left subtree
                        // GE  | <                          | go right | First >= key is in right subtree
                        //
                        // Backwards iteration (looking for last match in tree):
                        // OP  | Current Cell vs Seek Key   | Action?  | Explanation
                        // LE  | > or =                     | go left  | Last <= key is in left subtree
                        // LE  | <                          | go right | Last <= key is in right subtree
                        // LT  | > or =                     | go left  | Last < key is in left subtree
                        // LT  | <                          | go right?| Last < key is in right subtree, except if cell rowid is exactly 1 less
                        //
                        // No iteration (point query):
                        // EQ  | > or =                     | go left  | Last = key is in left subtree
                        // EQ  | <                          | go right | Last = key is in right subtree
                        let target_leaf_page_is_in_left_subtree = match cmp {
                            SeekOp::GT => *cell_rowid > rowid_key,
                            SeekOp::GE => *cell_rowid >= rowid_key,
                            SeekOp::LE => *cell_rowid >= rowid_key,
                            SeekOp::LT => *cell_rowid + 1 >= rowid_key,
                            SeekOp::EQ => *cell_rowid >= rowid_key,
                        };
                        if target_leaf_page_is_in_left_subtree {
                            // If we found our target rowid in the left subtree,
                            // we need to move the parent cell pointer forwards or backwards depending on the iteration direction.
                            // For example: since the internal node contains the max rowid of the left subtree, we need to move the
                            // parent pointer backwards in backwards iteration so that we don't come back to the parent again.
                            // E.g.
                            // this parent: rowid 666
                            // left child has: 664,665,666
                            // we need to move to the previous parent (with e.g. rowid 663) when iterating backwards.
                            self.stack.next_cell_in_direction(iter_dir);
                            let mem_page = self.pager.read_page(*_left_child_page as usize)?;
                            self.stack.push(mem_page);
                            found_cell = true;
                            break;
                        } else {
                            self.stack.advance();
                        }
                    }
                    BTreeCell::TableLeafCell(TableLeafCell {
                        _rowid: _,
                        _payload: _,
                        first_overflow_page: _,
                        ..
                    }) => {
                        unreachable!(
                            "we don't iterate leaf cells while trying to move to a leaf cell"
                        );
                    }
                    BTreeCell::IndexInteriorCell(IndexInteriorCell {
                        left_child_page,
                        payload,
                        first_overflow_page,
                        payload_size,
                    }) => {
                        let SeekKey::IndexKey(index_key) = key else {
                            unreachable!("index seek key should be a record");
                        };
                        if let Some(next_page) = first_overflow_page {
                            return_if_io!(self.process_overflow_read(
                                payload,
                                *next_page,
                                *payload_size
                            ))
                        } else {
                            crate::storage::sqlite3_ondisk::read_record(
                                payload,
                                self.get_immutable_record_or_create().as_mut().unwrap(),
                            )?
                        };
                        let record = self.get_immutable_record();
                        let record = record.as_ref().unwrap();
                        let record_slice_equal_number_of_cols =
                            &record.get_values().as_slice()[..index_key.get_values().len()];
                        let interior_cell_vs_index_key = compare_immutable(
                            record_slice_equal_number_of_cols,
                            index_key.get_values(),
                        );
                        // in sqlite btrees left child pages have <= keys.
                        // in general, in forwards iteration we want to find the first key that matches the seek condition.
                        // in backwards iteration we want to find the last key that matches the seek condition.
                        //
                        // Logic table for determining if target leaf page is in left subtree.
                        // For index b-trees this is a bit more complicated since the interior cells contain payloads (the key is the payload).
                        // and for non-unique indexes there might be several cells with the same key.
                        //
                        // Forwards iteration (looking for first match in tree):
                        // OP  | Current Cell vs Seek Key  | Action?  | Explanation
                        // GT  | >                         | go left  | First > key could be exactly this one, or in left subtree
                        // GT  | = or <                    | go right | First > key must be in right subtree
                        // GE  | >                         | go left  | First >= key could be exactly this one, or in left subtree
                        // GE  | =                         | go left  | First >= key could be exactly this one, or in left subtree
                        // GE  | <                         | go right | First >= key must be in right subtree
                        //
                        // Backwards iteration (looking for last match in tree):
                        // OP  | Current Cell vs Seek Key  | Action?  | Explanation
                        // LE  | >                         | go left  | Last <= key must be in left subtree
                        // LE  | =                         | go right | Last <= key is either this one, or somewhere to the right of this one. So we need to go right to make sure
                        // LE  | <                         | go right | Last <= key must be in right subtree
                        // LT  | >                         | go left  | Last < key must be in left subtree
                        // LT  | =                         | go left  | Last < key must be in left subtree since we want strictly less than
                        // LT  | <                         | go right | Last < key could be exactly this one, or in right subtree
                        //
                        // No iteration (point query):
                        // EQ  | >                         | go left  | First = key must be in left subtree
                        // EQ  | =                         | go left  | First = key could be exactly this one, or in left subtree
                        // EQ  | <                         | go right | First = key must be in right subtree

                        let target_leaf_page_is_in_left_subtree = match cmp {
                            SeekOp::GT => interior_cell_vs_index_key.is_gt(),
                            SeekOp::GE => interior_cell_vs_index_key.is_ge(),
                            SeekOp::EQ => interior_cell_vs_index_key.is_ge(),
                            SeekOp::LE => interior_cell_vs_index_key.is_gt(),
                            SeekOp::LT => interior_cell_vs_index_key.is_ge(),
                        };
                        if target_leaf_page_is_in_left_subtree {
                            // we don't advance in case of forward iteration and index tree internal nodes because we will visit this node going up.
                            // in backwards iteration, we must retreat because otherwise we would unnecessarily visit this node again.
                            // Example:
                            // this parent: key 666, and we found the target key in the left child.
                            // left child has: key 663, key 664, key 665
                            // we need to move to the previous parent (with e.g. key 662) when iterating backwards so that we don't end up back here again.
                            if iter_dir == IterationDirection::Backwards {
                                self.stack.retreat();
                            }
                            let mem_page = self.pager.read_page(*left_child_page as usize)?;
                            self.stack.push(mem_page);
                            found_cell = true;
                            break;
                        } else {
                            self.stack.advance();
                        }
                    }
                    BTreeCell::IndexLeafCell(_) => {
                        unreachable!(
                            "we don't iterate leaf cells while trying to move to a leaf cell"
                        );
                    }
                }
            }

            if !found_cell {
                match contents.rightmost_pointer() {
                    Some(right_most_pointer) => {
                        self.stack.advance();
                        let mem_page = self.pager.read_page(right_most_pointer as usize)?;
                        self.stack.push(mem_page);
                        continue;
                    }
                    None => {
                        unreachable!("we shall not go back up! The only way is down the slope");
                    }
                }
            }
        }
    }

    /// Insert a record into the btree.
    /// If the insert operation overflows the page, it will be split and the btree will be balanced.
    fn insert_into_page(&mut self, bkey: &BTreeKey) -> Result<CursorResult<()>> {
        let record = bkey
            .get_record()
            .expect("expected record present on insert");

        if let CursorState::None = &self.state {
            self.state = CursorState::Write(WriteInfo::new());
        }
        let ret = loop {
            let write_state = {
                let write_info = self
                    .state
                    .mut_write_info()
                    .expect("can't insert while counting");
                write_info.state
            };
            match write_state {
                WriteState::Start => {
                    let page = self.stack.top();
                    return_if_locked_maybe_load!(self.pager, page);

                    // get page and find cell
                    let (cell_idx, page_type) = {
                        return_if_locked!(page);

                        page.set_dirty();
                        self.pager.add_dirty(page.get().id);

                        let page = page.get().contents.as_mut().unwrap();
                        assert!(matches!(
                            page.page_type(),
                            PageType::TableLeaf | PageType::IndexLeaf
                        ));

                        // find cell
                        (self.find_cell(page, bkey), page.page_type())
                    };
                    tracing::debug!("insert_into_page(cell_idx={})", cell_idx);

                    // if the cell index is less than the total cells, check: if its an existing
                    // rowid, we are going to update / overwrite the cell
                    if cell_idx < page.get_contents().cell_count() {
                        match page.get_contents().cell_get(
                            cell_idx,
                            payload_overflow_threshold_max(page_type, self.usable_space() as u16),
                            payload_overflow_threshold_min(page_type, self.usable_space() as u16),
                            self.usable_space(),
                        )? {
                         BTreeCell::TableLeafCell(tbl_leaf) => {
                            if tbl_leaf._rowid == bkey.to_rowid() {
                                tracing::debug!("insert_into_page: found exact match with cell_idx={cell_idx}, overwriting");
                                self.overwrite_cell(page.clone(), cell_idx, record)?;
                                self.state
                                    .mut_write_info()
                                    .expect("expected write info")
                                    .state = WriteState::Finish;
                                continue;
                            }
                        }
                      BTreeCell::IndexLeafCell(idx_leaf) => {
                        read_record(
                            idx_leaf.payload,
                            self.get_immutable_record_or_create().as_mut().unwrap(),
                        )
                        .expect("failed to read record");
                    if compare_immutable(
                                record.get_values(),
                                self.get_immutable_record()
                                    .as_ref()
                                    .unwrap()
                                    .get_values()
                        ) == Ordering::Equal {

                        tracing::debug!("insert_into_page: found exact match with cell_idx={cell_idx}, overwriting");
                        self.overwrite_cell(page.clone(), cell_idx, record)?;
                        self.state
                            .mut_write_info()
                            .expect("expected write info")
                            .state = WriteState::Finish;
                        continue;
                        }
                    }
                    other => panic!("unexpected cell type, expected TableLeaf or IndexLeaf, found: {:?}", other),
                   }
                    }
                    // insert cell
                    let mut cell_payload: Vec<u8> = Vec::with_capacity(record.len() + 4);
                    fill_cell_payload(
                        page_type,
                        bkey.maybe_rowid(),
                        &mut cell_payload,
                        record,
                        self.usable_space() as u16,
                        self.pager.clone(),
                    );

                    // insert
                    let overflow = {
                        let contents = page.get().contents.as_mut().unwrap();
                        tracing::debug!(
                            "insert_into_page(overflow, cell_count={})",
                            contents.cell_count()
                        );

                        insert_into_cell(
                            contents,
                            cell_payload.as_slice(),
                            cell_idx,
                            self.usable_space() as u16,
                        )?;
                        contents.overflow_cells.len()
                    };
                    let write_info = self
                        .state
                        .mut_write_info()
                        .expect("can't count while inserting");
                    if overflow > 0 {
                        write_info.state = WriteState::BalanceStart;
                    } else {
                        write_info.state = WriteState::Finish;
                    }
                }
                WriteState::BalanceStart
                | WriteState::BalanceNonRoot
                | WriteState::BalanceNonRootWaitLoadPages => {
                    return_if_io!(self.balance());
                }
                WriteState::Finish => {
                    break Ok(CursorResult::Ok(()));
                }
            };
        };
        self.state = CursorState::None;
        ret
    }

    /// Balance a leaf page.
    /// Balancing is done when a page overflows.
    /// see e.g. https://en.wikipedia.org/wiki/B-tree
    ///
    /// This is a naive algorithm that doesn't try to distribute cells evenly by content.
    /// It will try to split the page in half by keys not by content.
    /// Sqlite tries to have a page at least 40% full.
    fn balance(&mut self) -> Result<CursorResult<()>> {
        assert!(
            matches!(self.state, CursorState::Write(_)),
            "Cursor must be in balancing state"
        );
        loop {
            let state = self.state.write_info().expect("must be balancing").state;
            match state {
                WriteState::BalanceStart => {
                    assert!(
                        self.state
                            .write_info()
                            .unwrap()
                            .balance_info
                            .borrow()
                            .is_none(),
                        "BalanceInfo should be empty on start"
                    );
                    let current_page = self.stack.top();
                    {
                        // check if we don't need to balance
                        // don't continue if there are no overflow cells
                        let page = current_page.get().contents.as_mut().unwrap();
                        if page.overflow_cells.is_empty() {
                            let write_info = self.state.mut_write_info().unwrap();
                            write_info.state = WriteState::Finish;
                            return Ok(CursorResult::Ok(()));
                        }
                    }

                    if !self.stack.has_parent() {
                        self.balance_root();
                    }

                    let write_info = self.state.mut_write_info().unwrap();
                    write_info.state = WriteState::BalanceNonRoot;
                    self.stack.pop();
                    return_if_io!(self.balance_non_root());
                }
                WriteState::BalanceNonRoot | WriteState::BalanceNonRootWaitLoadPages => {
                    return_if_io!(self.balance_non_root());
                }
                WriteState::Finish => return Ok(CursorResult::Ok(())),
                _ => panic!("unexpected state on balance {:?}", state),
            }
        }
    }

    /// Balance a non root page by trying to balance cells between a maximum of 3 siblings that should be neighboring the page that overflowed/underflowed.
    fn balance_non_root(&mut self) -> Result<CursorResult<()>> {
        assert!(
            matches!(self.state, CursorState::Write(_)),
            "Cursor must be in balancing state"
        );
        let state = self.state.write_info().expect("must be balancing").state;
        tracing::debug!("balance_non_root(state={:?})", state);
        let (next_write_state, result) = match state {
            WriteState::Start => todo!(),
            WriteState::BalanceStart => todo!(),
            WriteState::BalanceNonRoot => {
                let parent_page = self.stack.top();
                return_if_locked_maybe_load!(self.pager, parent_page);
                // If `move_to` moved to rightmost page, cell index will be out of bounds. Meaning cell_count+1.
                // In any other case, `move_to` will stay in the correct index.
                if self.stack.current_cell_index() as usize
                    == parent_page.get_contents().cell_count() + 1
                {
                    self.stack.retreat();
                }
                parent_page.set_dirty();
                self.pager.add_dirty(parent_page.get().id);
                let parent_contents = parent_page.get().contents.as_ref().unwrap();
                let page_to_balance_idx = self.stack.current_cell_index() as usize;

                tracing::debug!(
                    "balance_non_root(parent_id={} page_to_balance_idx={})",
                    parent_page.get().id,
                    page_to_balance_idx
                );
                assert!(matches!(
                    parent_contents.page_type(),
                    PageType::IndexInterior | PageType::TableInterior
                ));
                // Part 1: Find the sibling pages to balance
                let mut pages_to_balance = vec![];
                let number_of_cells_in_parent =
                    parent_contents.cell_count() + parent_contents.overflow_cells.len();

                assert!(
                    parent_contents.overflow_cells.is_empty(),
                    "balancing child page with overflowed parent not yet implemented"
                );
                assert!(
                    page_to_balance_idx <= parent_contents.cell_count(),
                    "page_to_balance_idx={} is out of bounds for parent cell count {}",
                    page_to_balance_idx,
                    number_of_cells_in_parent
                );
                // As there will be at maximum 3 pages used to balance:
                // sibling_pointer is the index represeneting one of those 3 pages, and we initialize it to the last possible page.
                // next_divider is the first divider that contains the first page of the 3 pages.
                let (sibling_pointer, first_cell_divider) = match number_of_cells_in_parent {
                    n if n < 2 => (number_of_cells_in_parent, 0),
                    2 => (2, 0),
                    // Here we will have at lest 2 cells and one right pointer, therefore we can get 3 siblings.
                    // In case of 2 we will have all pages to balance.
                    _ => {
                        // In case of > 3 we have to check which ones to get
                        let next_divider = if page_to_balance_idx == 0 {
                            // first cell, take first 3
                            0
                        } else if page_to_balance_idx == number_of_cells_in_parent {
                            // Page corresponds to right pointer, so take last 3
                            number_of_cells_in_parent - 2
                        } else {
                            // Some cell in the middle, so we want to take sibling on left and right.
                            page_to_balance_idx - 1
                        };
                        (2, next_divider)
                    }
                };
                let sibling_count = sibling_pointer + 1;

                let last_sibling_is_right_pointer = sibling_pointer + first_cell_divider
                    - parent_contents.overflow_cells.len()
                    == parent_contents.cell_count();
                // Get the right page pointer that we will need to update later
                let right_pointer = if last_sibling_is_right_pointer {
                    parent_contents.rightmost_pointer_raw().unwrap()
                } else {
                    let (start_of_cell, _) = parent_contents.cell_get_raw_region(
                        first_cell_divider + sibling_pointer,
                        payload_overflow_threshold_max(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        payload_overflow_threshold_min(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        self.usable_space(),
                    );
                    let buf = parent_contents.as_ptr().as_mut_ptr();
                    unsafe { buf.add(start_of_cell) }
                };

                // load sibling pages
                // start loading right page first
                let mut pgno: u32 = unsafe { right_pointer.cast::<u32>().read().swap_bytes() };
                let current_sibling = sibling_pointer;
                for i in (0..=current_sibling).rev() {
                    let page = self.pager.read_page(pgno as usize)?;
                    debug_validate_cells!(&page.get_contents(), self.usable_space() as u16);
                    pages_to_balance.push(page);
                    assert_eq!(
                        parent_contents.overflow_cells.len(),
                        0,
                        "overflow in parent is not yet implented while balancing it"
                    );
                    if i == 0 {
                        break;
                    }
                    let next_cell_divider = i + first_cell_divider - 1;
                    pgno = match parent_contents.cell_get(
                        next_cell_divider,
                        payload_overflow_threshold_max(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        payload_overflow_threshold_min(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        self.usable_space(),
                    )? {
                        BTreeCell::TableInteriorCell(table_interior_cell) => {
                            table_interior_cell._left_child_page
                        }
                        BTreeCell::IndexInteriorCell(index_interior_cell) => {
                            index_interior_cell.left_child_page
                        }
                        BTreeCell::TableLeafCell(..) | BTreeCell::IndexLeafCell(..) => {
                            unreachable!()
                        }
                    };
                }
                // Reverse in order to keep the right order
                pages_to_balance.reverse();

                #[cfg(debug_assertions)]
                {
                    let page_type_of_siblings = pages_to_balance[0].get_contents().page_type();
                    for page in &pages_to_balance {
                        let contents = page.get_contents();
                        debug_validate_cells!(&contents, self.usable_space() as u16);
                        assert_eq!(contents.page_type(), page_type_of_siblings);
                    }
                }
                self.state
                    .write_info()
                    .unwrap()
                    .balance_info
                    .replace(Some(BalanceInfo {
                        pages_to_balance,
                        rightmost_pointer: right_pointer,
                        divider_cells: Vec::new(),
                        sibling_count,
                        first_divider_cell: first_cell_divider,
                    }));
                (
                    WriteState::BalanceNonRootWaitLoadPages,
                    Ok(CursorResult::IO),
                )
            }
            WriteState::BalanceNonRootWaitLoadPages => {
                let write_info = self.state.write_info().unwrap();
                let mut balance_info = write_info.balance_info.borrow_mut();
                let balance_info = balance_info.as_mut().unwrap();
                let all_loaded = balance_info
                    .pages_to_balance
                    .iter()
                    .all(|page| !page.is_locked());
                if !all_loaded {
                    return Ok(CursorResult::IO);
                }
                // Now do real balancing
                let parent_page = self.stack.top();
                let parent_contents = parent_page.get_contents();

                assert!(
                    parent_contents.overflow_cells.is_empty(),
                    "overflow parent not yet implemented"
                );

                /* 1. Get divider cells and max_cells */
                let mut max_cells = 0;
                let mut pages_to_balance_new = Vec::new();
                for i in (0..balance_info.sibling_count).rev() {
                    let sibling_page = &balance_info.pages_to_balance[i];
                    let sibling_contents = sibling_page.get_contents();
                    sibling_page.set_dirty();
                    self.pager.add_dirty(sibling_page.get().id);
                    max_cells += sibling_contents.cell_count();
                    max_cells += sibling_contents.overflow_cells.len();

                    // Right pointer is not dropped, we simply update it at the end. This could be a divider cell that points
                    // to the last page in the list of pages to balance or this could be the rightmost pointer that points to a page.
                    if i == balance_info.sibling_count - 1 {
                        continue;
                    }
                    // Since we know we have a left sibling, take the divider that points to left sibling of this page
                    let cell_idx = balance_info.first_divider_cell + i;
                    let (cell_start, cell_len) = parent_contents.cell_get_raw_region(
                        cell_idx,
                        payload_overflow_threshold_max(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        payload_overflow_threshold_min(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        self.usable_space(),
                    );
                    let buf = parent_contents.as_ptr();
                    let cell_buf = &buf[cell_start..cell_start + cell_len];
                    max_cells += 1;

                    tracing::debug!(
                        "balance_non_root(drop_divider_cell, first_divider_cell={}, divider_cell={}, left_pointer={})",
                        balance_info.first_divider_cell,
                        i,
                        read_u32(cell_buf, 0)
                    );

                    // TODO(pere): make this reference and not copy
                    balance_info.divider_cells.push(cell_buf.to_vec());
                    tracing::trace!(
                        "dropping divider cell from parent cell_idx={} count={}",
                        cell_idx,
                        parent_contents.cell_count()
                    );
                    drop_cell(parent_contents, cell_idx, self.usable_space() as u16)?;
                }
                assert_eq!(
                    balance_info.divider_cells.len(),
                    balance_info.sibling_count - 1,
                    "the number of pages balancing must be divided by one less divider"
                );
                // Reverse divider cells to be in order
                balance_info.divider_cells.reverse();

                /* 2. Initialize CellArray with all the cells used for distribution, this includes divider cells if !leaf. */
                let mut cell_array = CellArray {
                    cells: Vec::with_capacity(max_cells),
                    number_of_cells_per_page: Vec::new(),
                };
                let cells_capacity_start = cell_array.cells.capacity();

                let mut total_cells_inserted = 0;
                // count_cells_in_old_pages is the prefix sum of cells of each page
                let mut count_cells_in_old_pages = Vec::new();

                let page_type = balance_info.pages_to_balance[0].get_contents().page_type();
                tracing::debug!("balance_non_root(page_type={:?})", page_type);
                let leaf_data = matches!(page_type, PageType::TableLeaf);
                let leaf = matches!(page_type, PageType::TableLeaf | PageType::IndexLeaf);
                for (i, old_page) in balance_info.pages_to_balance.iter().enumerate() {
                    let old_page_contents = old_page.get_contents();
                    debug_validate_cells!(&old_page_contents, self.usable_space() as u16);
                    for cell_idx in 0..old_page_contents.cell_count() {
                        let (cell_start, cell_len) = old_page_contents.cell_get_raw_region(
                            cell_idx,
                            payload_overflow_threshold_max(
                                old_page_contents.page_type(),
                                self.usable_space() as u16,
                            ),
                            payload_overflow_threshold_min(
                                old_page_contents.page_type(),
                                self.usable_space() as u16,
                            ),
                            self.usable_space(),
                        );
                        let buf = old_page_contents.as_ptr();
                        let cell_buf = &mut buf[cell_start..cell_start + cell_len];
                        // TODO(pere): make this reference and not copy
                        cell_array.cells.push(to_static_buf(cell_buf));
                    }
                    // Insert overflow cells into correct place
                    let offset = total_cells_inserted;
                    for overflow_cell in old_page_contents.overflow_cells.iter_mut() {
                        cell_array.cells.insert(
                            offset + overflow_cell.index,
                            to_static_buf(&mut Pin::as_mut(&mut overflow_cell.payload)),
                        );
                    }

                    count_cells_in_old_pages.push(cell_array.cells.len() as u16);

                    let mut cells_inserted =
                        old_page_contents.cell_count() + old_page_contents.overflow_cells.len();

                    if i < balance_info.pages_to_balance.len() - 1 && !leaf_data {
                        // If we are a index page or a interior table page we need to take the divider cell too.
                        // But we don't need the last divider as it will remain the same.
                        let mut divider_cell = balance_info.divider_cells[i].as_mut_slice();
                        // TODO(pere): in case of old pages are leaf pages, so index leaf page, we need to strip page pointers
                        // from divider cells in index interior pages (parent) because those should not be included.
                        cells_inserted += 1;
                        if !leaf {
                            // This divider cell needs to be updated with new left pointer,
                            let right_pointer = old_page_contents.rightmost_pointer().unwrap();
                            divider_cell[..4].copy_from_slice(&right_pointer.to_be_bytes());
                        } else {
                            // index leaf
                            assert!(divider_cell.len() >= 4);
                            // let's strip the page pointer
                            divider_cell = &mut divider_cell[4..];
                        }
                        cell_array.cells.push(to_static_buf(divider_cell));
                    }
                    total_cells_inserted += cells_inserted;
                }

                assert_eq!(
                    cell_array.cells.capacity(),
                    cells_capacity_start,
                    "calculation of max cells was wrong"
                );

                // Let's copy all cells for later checks
                #[cfg(debug_assertions)]
                let mut cells_debug = Vec::new();
                #[cfg(debug_assertions)]
                {
                    for cell in &cell_array.cells {
                        cells_debug.push(cell.to_vec());
                        if leaf {
                            assert!(cell[0] != 0)
                        }
                    }
                }

                #[cfg(debug_assertions)]
                validate_cells_after_insertion(&cell_array, leaf_data);

                /* 3. Initiliaze current size of every page including overflow cells and divider cells that might be included. */
                let mut new_page_sizes: Vec<i64> = Vec::new();
                let leaf_correction = if leaf { 4 } else { 0 };
                // number of bytes beyond header, different from global usableSapce which includes
                // header
                let usable_space = self.usable_space() - 12 + leaf_correction;
                for i in 0..balance_info.sibling_count {
                    cell_array
                        .number_of_cells_per_page
                        .push(count_cells_in_old_pages[i]);
                    let page = &balance_info.pages_to_balance[i];
                    let page_contents = page.get_contents();
                    let free_space = compute_free_space(page_contents, self.usable_space() as u16);

                    new_page_sizes.push(usable_space as i64 - free_space as i64);
                    for overflow in &page_contents.overflow_cells {
                        let size = new_page_sizes.last_mut().unwrap();
                        // 2 to account of pointer
                        *size += 2 + overflow.payload.len() as i64;
                    }
                    if !leaf && i < balance_info.sibling_count - 1 {
                        // Account for divider cell which is included in this page.
                        let size = new_page_sizes.last_mut().unwrap();
                        *size += cell_array.cells[cell_array.cell_count(i)].len() as i64;
                    }
                }

                /* 4. Now let's try to move cells to the left trying to stack them without exceeding the maximum size of a page.
                     There are two cases:
                       * If current page has too many cells, it will move them to the next page.
                       * If it still has space, and it can take a cell from the right it will take them.
                         Here there is a caveat. Taking a cell from the right might take cells from page i+1, i+2, i+3, so not necessarily
                         adjacent. But we decrease the size of the adjacent page if we move from the right. This might cause a intermitent state
                         where page can have size <0.
                    This will also calculate how many pages are required to balance the cells and store in sibling_count_new.
                */
                // Try to pack as many cells to the left
                let mut sibling_count_new = balance_info.sibling_count;
                let mut i = 0;
                while i < sibling_count_new {
                    // First try to move cells to the right if they do not fit
                    while new_page_sizes[i] > usable_space as i64 {
                        let needs_new_page = i + 1 >= sibling_count_new;
                        if needs_new_page {
                            sibling_count_new += 1;
                            new_page_sizes.push(0);
                            cell_array
                                .number_of_cells_per_page
                                .push(cell_array.cells.len() as u16);
                            assert!(
                                sibling_count_new <= 5,
                                "it is corrupt to require more than 5 pages to balance 3 siblings"
                            );
                        }
                        let size_of_cell_to_remove_from_left =
                            2 + cell_array.cells[cell_array.cell_count(i) - 1].len() as i64;
                        new_page_sizes[i] -= size_of_cell_to_remove_from_left;
                        let size_of_cell_to_move_right = if !leaf_data {
                            if cell_array.number_of_cells_per_page[i]
                                < cell_array.cells.len() as u16
                            {
                                // This means we move to the right page the divider cell and we
                                // promote left cell to divider
                                2 + cell_array.cells[cell_array.cell_count(i)].len() as i64
                            } else {
                                0
                            }
                        } else {
                            size_of_cell_to_remove_from_left
                        };
                        new_page_sizes[i + 1] += size_of_cell_to_move_right as i64;
                        cell_array.number_of_cells_per_page[i] -= 1;
                    }

                    // Now try to take from the right if we didn't have enough
                    while cell_array.number_of_cells_per_page[i] < cell_array.cells.len() as u16 {
                        let size_of_cell_to_remove_from_right =
                            2 + cell_array.cells[cell_array.cell_count(i)].len() as i64;
                        let can_take = new_page_sizes[i] + size_of_cell_to_remove_from_right
                            > usable_space as i64;
                        if can_take {
                            break;
                        }
                        new_page_sizes[i] += size_of_cell_to_remove_from_right;
                        cell_array.number_of_cells_per_page[i] += 1;

                        let size_of_cell_to_remove_from_right = if !leaf_data {
                            if cell_array.number_of_cells_per_page[i]
                                < cell_array.cells.len() as u16
                            {
                                2 + cell_array.cells[cell_array.cell_count(i)].len() as i64
                            } else {
                                0
                            }
                        } else {
                            size_of_cell_to_remove_from_right
                        };

                        new_page_sizes[i + 1] -= size_of_cell_to_remove_from_right;
                    }

                    // Check if this page contains up to the last cell. If this happens it means we really just need up to this page.
                    // Let's update the number of new pages to be up to this page (i+1)
                    let page_completes_all_cells =
                        cell_array.number_of_cells_per_page[i] >= cell_array.cells.len() as u16;
                    if page_completes_all_cells {
                        sibling_count_new = i + 1;
                        break;
                    }
                    i += 1;
                    if i >= sibling_count_new {
                        break;
                    }
                }
                new_page_sizes.truncate(sibling_count_new);
                cell_array
                    .number_of_cells_per_page
                    .truncate(sibling_count_new);

                tracing::debug!(
                    "balance_non_root(sibling_count={}, sibling_count_new={}, cells={})",
                    balance_info.sibling_count,
                    sibling_count_new,
                    cell_array.cells.len()
                );

                /* 5. Balance pages starting from a left stacked cell state and move them to right trying to maintain a balanced state
                where we only move from left to right if it will not unbalance both pages, meaning moving left to right won't make
                right page bigger than left page.
                */
                // Comment borrowed from SQLite src/btree.c
                // The packing computed by the previous block is biased toward the siblings
                // on the left side (siblings with smaller keys). The left siblings are
                // always nearly full, while the right-most sibling might be nearly empty.
                // The next block of code attempts to adjust the packing of siblings to
                // get a better balance.
                //
                // This adjustment is more than an optimization.  The packing above might
                // be so out of balance as to be illegal.  For example, the right-most
                // sibling might be completely empty.  This adjustment is not optional.
                for i in (1..sibling_count_new).rev() {
                    let mut size_right_page = new_page_sizes[i];
                    let mut size_left_page = new_page_sizes[i - 1];
                    let mut cell_left = cell_array.number_of_cells_per_page[i - 1] - 1;
                    // if leaf_data means we don't have divider, so the one we move from left is
                    // the same we add to right (we don't add divider to right).
                    let mut cell_right = cell_left + 1 - leaf_data as u16;
                    loop {
                        let cell_left_size = cell_array.cell_size(cell_left as usize) as i64;
                        let cell_right_size = cell_array.cell_size(cell_right as usize) as i64;
                        // TODO: add assert nMaxCells

                        let pointer_size = if i == sibling_count_new - 1 { 0 } else { 2 };
                        let would_not_improve_balance = size_right_page + cell_right_size + 2
                            > size_left_page - (cell_left_size + pointer_size);
                        if size_right_page != 0 && would_not_improve_balance {
                            break;
                        }

                        size_left_page -= cell_left_size + 2;
                        size_right_page += cell_right_size + 2;
                        cell_array.number_of_cells_per_page[i - 1] = cell_left;

                        if cell_left == 0 {
                            break;
                        }
                        cell_left -= 1;
                        cell_right -= 1;
                    }

                    new_page_sizes[i] = size_right_page;
                    new_page_sizes[i - 1] = size_left_page;
                    assert!(
                        cell_array.number_of_cells_per_page[i - 1]
                            > if i > 1 {
                                cell_array.number_of_cells_per_page[i - 2]
                            } else {
                                0
                            }
                    );
                }

                // Allocate pages or set dirty if not needed
                for i in 0..sibling_count_new {
                    if i < balance_info.sibling_count {
                        balance_info.pages_to_balance[i].set_dirty();
                        pages_to_balance_new.push(balance_info.pages_to_balance[i].clone());
                    } else {
                        let page = self.pager.do_allocate_page(page_type, 0);
                        pages_to_balance_new.push(page);
                        // Since this page didn't exist before, we can set it to cells length as it
                        // marks them as empty since it is a prefix sum of cells.
                        count_cells_in_old_pages.push(cell_array.cells.len() as u16);
                    }
                }

                // Reassign page numbers in increasing order
                let mut page_numbers = Vec::new();
                for page in pages_to_balance_new.iter() {
                    page_numbers.push(page.get().id);
                }
                page_numbers.sort();
                for (page, new_id) in pages_to_balance_new.iter().zip(page_numbers) {
                    if new_id != page.get().id {
                        page.get().id = new_id;
                        self.pager.put_loaded_page(new_id, page.clone());
                    }
                }

                #[cfg(debug_assertions)]
                {
                    tracing::debug!("balance_non_root(parent page_id={})", parent_page.get().id);
                    for page in &pages_to_balance_new {
                        tracing::debug!("balance_non_root(new_sibling page_id={})", page.get().id);
                    }
                }

                // pages_pointed_to helps us debug we did in fact create divider cells to all the new pages and the rightmost pointer,
                // also points to the last page.
                #[cfg(debug_assertions)]
                let mut pages_pointed_to = HashSet::new();

                // Write right pointer in parent page to point to new rightmost page. keep in mind
                // we update rightmost pointer first because inserting cells could defragment parent page,
                // therfore invalidating the pointer.
                let right_page_id = pages_to_balance_new.last().unwrap().get().id as u32;
                let rightmost_pointer = balance_info.rightmost_pointer;
                let rightmost_pointer =
                    unsafe { std::slice::from_raw_parts_mut(rightmost_pointer, 4) };
                rightmost_pointer[0..4].copy_from_slice(&right_page_id.to_be_bytes());

                #[cfg(debug_assertions)]
                pages_pointed_to.insert(right_page_id);
                tracing::debug!(
                    "balance_non_root(rightmost_pointer_update, rightmost_pointer={})",
                    right_page_id
                );

                /* 6. Update parent pointers. Update right pointer and insert divider cells with newly created distribution of cells */
                // Ensure right-child pointer of the right-most new sibling pge points to the page
                // that was originally on that place.
                let is_leaf_page = matches!(page_type, PageType::TableLeaf | PageType::IndexLeaf);
                if !is_leaf_page {
                    let last_page = balance_info.pages_to_balance.last().unwrap();
                    let right_pointer = last_page.get_contents().rightmost_pointer().unwrap();
                    let new_last_page = pages_to_balance_new.last().unwrap();
                    new_last_page
                        .get_contents()
                        .write_u32(offset::BTREE_RIGHTMOST_PTR, right_pointer);
                }
                // TODO: pointer map update (vacuum support)
                // Update divider cells in parent
                for (i, page) in pages_to_balance_new
                    .iter()
                    .enumerate()
                    .take(sibling_count_new - 1)
                /* do not take last page */
                {
                    let divider_cell_idx = cell_array.cell_count(i);
                    let mut divider_cell = &mut cell_array.cells[divider_cell_idx];
                    // FIXME: dont use auxiliary space, could be done without allocations
                    let mut new_divider_cell = Vec::new();
                    if !is_leaf_page {
                        // Interior
                        // Make this page's rightmost pointer point to pointer of divider cell before modification
                        let previous_pointer_divider = read_u32(&divider_cell, 0);
                        page.get_contents()
                            .write_u32(offset::BTREE_RIGHTMOST_PTR, previous_pointer_divider);
                        // divider cell now points to this page
                        new_divider_cell.extend_from_slice(&(page.get().id as u32).to_be_bytes());
                        // now copy the rest of the divider cell:
                        // Table Interior page:
                        //   * varint rowid
                        // Index Interior page:
                        //   * varint payload size
                        //   * payload
                        //   * first overflow page (u32 optional)
                        new_divider_cell.extend_from_slice(&divider_cell[4..]);
                    } else if leaf_data {
                        // Leaf table
                        // FIXME: not needed conversion
                        // FIXME: need to update cell size in order to free correctly?
                        // insert into cell with correct range should be enough
                        divider_cell = &mut cell_array.cells[divider_cell_idx - 1];
                        let (_, n_bytes_payload) = read_varint(divider_cell)?;
                        let (rowid, _) = read_varint(&divider_cell[n_bytes_payload..])?;
                        new_divider_cell.extend_from_slice(&(page.get().id as u32).to_be_bytes());
                        write_varint_to_vec(rowid, &mut new_divider_cell);
                    } else {
                        // Leaf index
                        new_divider_cell.extend_from_slice(&(page.get().id as u32).to_be_bytes());
                        new_divider_cell.extend_from_slice(divider_cell);
                    }

                    let left_pointer = read_u32(&new_divider_cell[..4], 0);
                    assert!(left_pointer != parent_page.get().id as u32);
                    #[cfg(debug_assertions)]
                    pages_pointed_to.insert(left_pointer);
                    tracing::debug!(
                        "balance_non_root(insert_divider_cell, first_divider_cell={}, divider_cell={}, left_pointer={})",
                        balance_info.first_divider_cell,
                        i,
                        left_pointer
                    );
                    assert_eq!(left_pointer, page.get().id as u32);
                    // FIXME: remove this lock
                    assert!(
                        left_pointer <= self.pager.db_header.lock().database_size,
                        "invalid page number divider left pointer {} > database number of pages",
                        left_pointer,
                    );
                    // FIXME: defragment shouldn't be needed
                    // defragment_page(parent_contents, self.usable_space() as u16);
                    insert_into_cell(
                        parent_contents,
                        &new_divider_cell,
                        balance_info.first_divider_cell + i,
                        self.usable_space() as u16,
                    )
                    .unwrap();
                    #[cfg(debug_assertions)]
                    self.validate_balance_non_root_divider_cell_insertion(
                        balance_info,
                        parent_contents,
                        i,
                        page,
                    );
                }
                tracing::debug!(
                    "balance_non_root(parent_overflow={})",
                    parent_contents.overflow_cells.len()
                );

                #[cfg(debug_assertions)]
                {
                    // Let's ensure every page is pointed to by the divider cell or the rightmost pointer.
                    for page in &pages_to_balance_new {
                        assert!(
                            pages_pointed_to.contains(&(page.get().id as u32)),
                            "page {} not pointed to by divider cell or rightmost pointer",
                            page.get().id
                        );
                    }
                }
                /* 7. Start real movement of cells. Next comment is borrowed from SQLite: */
                /* Now update the actual sibling pages. The order in which they are updated
                 ** is important, as this code needs to avoid disrupting any page from which
                 ** cells may still to be read. In practice, this means:
                 **
                 **  (1) If cells are moving left (from apNew[iPg] to apNew[iPg-1])
                 **      then it is not safe to update page apNew[iPg] until after
                 **      the left-hand sibling apNew[iPg-1] has been updated.
                 **
                 **  (2) If cells are moving right (from apNew[iPg] to apNew[iPg+1])
                 **      then it is not safe to update page apNew[iPg] until after
                 **      the right-hand sibling apNew[iPg+1] has been updated.
                 **
                 ** If neither of the above apply, the page is safe to update.
                 **
                 ** The iPg value in the following loop starts at nNew-1 goes down
                 ** to 0, then back up to nNew-1 again, thus making two passes over
                 ** the pages.  On the initial downward pass, only condition (1) above
                 ** needs to be tested because (2) will always be true from the previous
                 ** step.  On the upward pass, both conditions are always true, so the
                 ** upwards pass simply processes pages that were missed on the downward
                 ** pass.
                 */
                let mut done = vec![false; sibling_count_new];
                for i in (1 - sibling_count_new as i64)..sibling_count_new as i64 {
                    let page_idx = i.unsigned_abs() as usize;
                    if done[page_idx] {
                        continue;
                    }
                    if i >= 0
                        || count_cells_in_old_pages[page_idx - 1]
                            >= cell_array.number_of_cells_per_page[page_idx - 1]
                    {
                        let (start_old_cells, start_new_cells, number_new_cells) = if page_idx == 0
                        {
                            (0, 0, cell_array.cell_count(0))
                        } else {
                            let this_was_old_page = page_idx < balance_info.sibling_count;
                            // We add !leaf_data because we want to skip 1 in case of divider cell which is encountared between pages assigned
                            let start_old_cells = if this_was_old_page {
                                count_cells_in_old_pages[page_idx - 1] as usize
                                    + (!leaf_data) as usize
                            } else {
                                cell_array.cells.len()
                            };
                            let start_new_cells =
                                cell_array.cell_count(page_idx - 1) + (!leaf_data) as usize;
                            (
                                start_old_cells,
                                start_new_cells,
                                cell_array.cell_count(page_idx) - start_new_cells,
                            )
                        };
                        let page = &pages_to_balance_new[page_idx];
                        tracing::debug!("pre_edit_page(page={})", page.get().id);
                        let page = page.get_contents();
                        edit_page(
                            page,
                            start_old_cells,
                            start_new_cells,
                            number_new_cells,
                            &cell_array,
                            self.usable_space() as u16,
                        )?;
                        debug_validate_cells!(page, self.usable_space() as u16);
                        tracing::trace!(
                            "edit_page page={} cells={}",
                            pages_to_balance_new[page_idx].get().id,
                            page.cell_count()
                        );
                        page.overflow_cells.clear();

                        done[page_idx] = true;
                    }
                }

                #[cfg(debug_assertions)]
                self.post_balance_non_root_validation(
                    &parent_page,
                    balance_info,
                    parent_contents,
                    pages_to_balance_new,
                    page_type,
                    leaf_data,
                    cells_debug,
                    sibling_count_new,
                    rightmost_pointer,
                );
                // TODO: balance root
                // We have to free pages that are not used anymore
                for i in sibling_count_new..balance_info.sibling_count {
                    let page = &balance_info.pages_to_balance[i];
                    self.pager.free_page(Some(page.clone()), page.get().id)?;
                }
                (WriteState::BalanceStart, Ok(CursorResult::Ok(())))
            }
            WriteState::Finish => todo!(),
        };
        if matches!(next_write_state, WriteState::BalanceStart) {
            // reset balance state
            let _ = self.state.mut_write_info().unwrap().balance_info.take();
        }
        let write_info = self.state.mut_write_info().unwrap();
        write_info.state = next_write_state;
        result
    }

    #[cfg(debug_assertions)]
    fn validate_balance_non_root_divider_cell_insertion(
        &self,
        balance_info: &mut BalanceInfo,
        parent_contents: &mut PageContent,
        i: usize,
        page: &std::sync::Arc<crate::Page>,
    ) {
        let left_pointer = if parent_contents.overflow_cells.len() == 0 {
            let (cell_start, cell_len) = parent_contents.cell_get_raw_region(
                balance_info.first_divider_cell + i,
                payload_overflow_threshold_max(
                    parent_contents.page_type(),
                    self.usable_space() as u16,
                ),
                payload_overflow_threshold_min(
                    parent_contents.page_type(),
                    self.usable_space() as u16,
                ),
                self.usable_space(),
            );
            tracing::debug!(
                "balance_non_root(cell_start={}, cell_len={})",
                cell_start,
                cell_len
            );

            let left_pointer = read_u32(
                &parent_contents.as_ptr()[cell_start..cell_start + cell_len],
                0,
            );
            left_pointer
        } else {
            let mut left_pointer = None;
            for cell in parent_contents.overflow_cells.iter() {
                if cell.index == balance_info.first_divider_cell + i {
                    left_pointer = Some(read_u32(&cell.payload, 0))
                }
            }
            left_pointer.expect("overflow cell with divider cell was not found")
        };
        assert_eq!(left_pointer, page.get().id as u32, "the cell we just inserted doesn't point to the correct page. points to {}, should point to {}",
           left_pointer,
            page.get().id as u32
         );
    }

    #[cfg(debug_assertions)]
    fn post_balance_non_root_validation(
        &self,
        parent_page: &PageRef,
        balance_info: &mut BalanceInfo,
        parent_contents: &mut PageContent,
        pages_to_balance_new: Vec<std::sync::Arc<crate::Page>>,
        page_type: PageType,
        leaf_data: bool,
        mut cells_debug: Vec<Vec<u8>>,
        sibling_count_new: usize,
        rightmost_pointer: &mut [u8],
    ) {
        let mut valid = true;
        let mut current_index_cell = 0;
        for cell_idx in 0..parent_contents.cell_count() {
            let cell = parent_contents
                .cell_get(
                    cell_idx,
                    payload_overflow_threshold_max(
                        parent_contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    payload_overflow_threshold_min(
                        parent_contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    self.usable_space(),
                )
                .unwrap();
            match cell {
                BTreeCell::TableInteriorCell(table_interior_cell) => {
                    let left_child_page = table_interior_cell._left_child_page;
                    if left_child_page == parent_page.get().id as u32 {
                        tracing::error!("balance_non_root(parent_divider_points_to_same_page, page_id={}, cell_left_child_page={})",
                                parent_page.get().id,
                                left_child_page,
                            );
                        valid = false;
                    }
                }
                BTreeCell::IndexInteriorCell(index_interior_cell) => {
                    let left_child_page = index_interior_cell.left_child_page;
                    if left_child_page == parent_page.get().id as u32 {
                        tracing::error!("balance_non_root(parent_divider_points_to_same_page, page_id={}, cell_left_child_page={})",
                                parent_page.get().id,
                                left_child_page,
                            );
                        valid = false;
                    }
                }
                _ => {}
            }
        }
        // Let's now make a in depth check that we in fact added all possible cells somewhere and they are not lost
        for (page_idx, page) in pages_to_balance_new.iter().enumerate() {
            let contents = page.get_contents();
            debug_validate_cells!(contents, self.usable_space() as u16);
            // Cells are distributed in order
            for cell_idx in 0..contents.cell_count() {
                let (cell_start, cell_len) = contents.cell_get_raw_region(
                    cell_idx,
                    payload_overflow_threshold_max(
                        contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    payload_overflow_threshold_min(
                        contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    self.usable_space(),
                );
                let buf = contents.as_ptr();
                let cell_buf = to_static_buf(&mut buf[cell_start..cell_start + cell_len]);
                let cell_buf_in_array = &cells_debug[current_index_cell];
                if cell_buf != cell_buf_in_array {
                    tracing::error!("balance_non_root(cell_not_found_debug, page_id={}, cell_in_cell_array_idx={})",
                        page.get().id,
                        current_index_cell,
                    );
                    valid = false;
                }

                let cell = crate::storage::sqlite3_ondisk::read_btree_cell(
                    cell_buf,
                    &page_type,
                    0,
                    payload_overflow_threshold_max(
                        parent_contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    payload_overflow_threshold_min(
                        parent_contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    self.usable_space(),
                )
                .unwrap();
                match &cell {
                    BTreeCell::TableInteriorCell(table_interior_cell) => {
                        let left_child_page = table_interior_cell._left_child_page;
                        if left_child_page == page.get().id as u32 {
                            tracing::error!("balance_non_root(child_page_points_same_page, page_id={}, cell_left_child_page={}, page_idx={})",
                                page.get().id,
                                left_child_page,
                                page_idx
                            );
                            valid = false;
                        }
                        if left_child_page == parent_page.get().id as u32 {
                            tracing::error!("balance_non_root(child_page_points_parent_of_child, page_id={}, cell_left_child_page={}, page_idx={})",
                                page.get().id,
                                left_child_page,
                                page_idx
                            );
                            valid = false;
                        }
                    }
                    BTreeCell::IndexInteriorCell(index_interior_cell) => {
                        let left_child_page = index_interior_cell.left_child_page;
                        if left_child_page == page.get().id as u32 {
                            tracing::error!("balance_non_root(child_page_points_same_page, page_id={}, cell_left_child_page={}, page_idx={})",
                                page.get().id,
                                left_child_page,
                                page_idx
                            );
                            valid = false;
                        }
                        if left_child_page == parent_page.get().id as u32 {
                            tracing::error!("balance_non_root(child_page_points_parent_of_child, page_id={}, cell_left_child_page={}, page_idx={})",
                                page.get().id,
                                left_child_page,
                                page_idx
                            );
                            valid = false;
                        }
                    }
                    _ => {}
                }
                current_index_cell += 1;
            }
            // Now check divider cells and their pointers.
            let parent_buf = parent_contents.as_ptr();
            let cell_divider_idx = balance_info.first_divider_cell + page_idx;
            if page_idx == sibling_count_new - 1 {
                // We will only validate rightmost pointer of parent page, we will not validate rightmost if it's a cell and not the last pointer because,
                // insert cell could've defragmented the page and invalidated the pointer.
                // right pointer, we just check right pointer points to this page.
                if cell_divider_idx == parent_contents.cell_count() {
                    let rightmost = read_u32(rightmost_pointer, 0);
                    if rightmost != page.get().id as u32 {
                        tracing::error!("balance_non_root(cell_divider_right_pointer, should point to {}, but points to {})",
                        page.get().id,
                        rightmost
                    );
                        valid = false;
                    }
                }
            } else {
                // divider cell might be an overflow cell
                let mut was_overflow = false;
                for overflow_cell in &parent_contents.overflow_cells {
                    if overflow_cell.index == cell_divider_idx {
                        let left_pointer = read_u32(&overflow_cell.payload, 0);
                        if left_pointer != page.get().id as u32 {
                            tracing::error!("balance_non_root(cell_divider_left_pointer_overflow, should point to page_id={}, but points to {}, divider_cell={}, overflow_cells_parent={})",
                        page.get().id,
                        left_pointer,
                        page_idx,
                        parent_contents.overflow_cells.len()
                    );
                            valid = false;
                        }
                        was_overflow = true;
                        break;
                    }
                }
                if was_overflow {
                    continue;
                }
                // check if overflow
                // check if right pointer, this is the last page. Do we update rightmost pointer and defragment moves it?
                let (cell_start, cell_len) = parent_contents.cell_get_raw_region(
                    cell_divider_idx,
                    payload_overflow_threshold_max(
                        parent_contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    payload_overflow_threshold_min(
                        parent_contents.page_type(),
                        self.usable_space() as u16,
                    ),
                    self.usable_space(),
                );
                let cell_left_pointer = read_u32(&parent_buf[cell_start..cell_start + cell_len], 0);
                if cell_left_pointer != page.get().id as u32 {
                    tracing::error!("balance_non_root(cell_divider_left_pointer, should point to page_id={}, but points to {}, divider_cell={}, overflow_cells_parent={})",
                        page.get().id,
                        cell_left_pointer,
                        page_idx,
                        parent_contents.overflow_cells.len()
                    );
                    valid = false;
                }
                if leaf_data {
                    // If we are in a table leaf page, we just need to check that this cell that should be a divider cell is in the parent
                    // This means we already check cell in leaf pages but not on parent so we don't advance current_index_cell
                    if page_idx >= balance_info.sibling_count - 1 {
                        // This means we are in the last page and we don't need to check anything
                        continue;
                    }
                    let cell_buf: &'static mut [u8] =
                        to_static_buf(&mut cells_debug[current_index_cell - 1]);
                    let cell = crate::storage::sqlite3_ondisk::read_btree_cell(
                        cell_buf,
                        &page_type,
                        0,
                        payload_overflow_threshold_max(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        payload_overflow_threshold_min(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        self.usable_space(),
                    )
                    .unwrap();
                    let parent_cell = parent_contents
                        .cell_get(
                            cell_divider_idx,
                            payload_overflow_threshold_max(
                                parent_contents.page_type(),
                                self.usable_space() as u16,
                            ),
                            payload_overflow_threshold_min(
                                parent_contents.page_type(),
                                self.usable_space() as u16,
                            ),
                            self.usable_space(),
                        )
                        .unwrap();
                    let rowid = match cell {
                        BTreeCell::TableLeafCell(table_leaf_cell) => table_leaf_cell._rowid,
                        _ => unreachable!(),
                    };
                    let rowid_parent = match parent_cell {
                        BTreeCell::TableInteriorCell(table_interior_cell) => {
                            table_interior_cell._rowid
                        }
                        _ => unreachable!(),
                    };
                    if rowid_parent != rowid {
                        tracing::error!("balance_non_root(cell_divider_rowid, page_id={}, cell_divider_idx={}, rowid_parent={}, rowid={})",
                            page.get().id,
                            cell_divider_idx,
                            rowid_parent,
                            rowid
                        );
                        valid = false;
                    }
                } else {
                    // In any other case, we need to check that this cell was moved to parent as divider cell
                    let mut was_overflow = false;
                    for overflow_cell in &parent_contents.overflow_cells {
                        if overflow_cell.index == cell_divider_idx {
                            let left_pointer = read_u32(&overflow_cell.payload, 0);
                            if left_pointer != page.get().id as u32 {
                                tracing::error!("balance_non_root(cell_divider_divider_cell_overflow should point to page_id={}, but points to {}, divider_cell={}, overflow_cells_parent={})",
                                    page.get().id,
                                    left_pointer,
                                    page_idx,
                                    parent_contents.overflow_cells.len()
                                );
                                valid = false;
                            }
                            was_overflow = true;
                            break;
                        }
                    }
                    if was_overflow {
                        continue;
                    }
                    let (parent_cell_start, parent_cell_len) = parent_contents.cell_get_raw_region(
                        cell_divider_idx,
                        payload_overflow_threshold_max(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        payload_overflow_threshold_min(
                            parent_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        self.usable_space(),
                    );
                    let cell_buf_in_array = &cells_debug[current_index_cell];
                    let left_pointer = read_u32(
                        &parent_buf[parent_cell_start..parent_cell_start + parent_cell_len],
                        0,
                    );
                    if left_pointer != page.get().id as u32 {
                        tracing::error!("balance_non_root(divider_cell_left_pointer_interior should point to page_id={}, but points to {}, divider_cell={}, overflow_cells_parent={})",
                                    page.get().id,
                                    left_pointer,
                                    page_idx,
                                    parent_contents.overflow_cells.len()
                                );
                        valid = false;
                    }
                    match page_type {
                        PageType::TableInterior | PageType::IndexInterior => {
                            let parent_cell_buf =
                                &parent_buf[parent_cell_start..parent_cell_start + parent_cell_len];
                            if parent_cell_buf[4..] != cell_buf_in_array[4..] {
                                tracing::error!("balance_non_root(cell_divider_cell, page_id={}, cell_divider_idx={})",
                                    page.get().id,
                                    cell_divider_idx,
                                );
                                valid = false;
                            }
                        }
                        PageType::IndexLeaf => {
                            let parent_cell_buf =
                                &parent_buf[parent_cell_start..parent_cell_start + parent_cell_len];
                            if parent_cell_buf[4..] != cell_buf_in_array[..] {
                                tracing::error!("balance_non_root(cell_divider_cell_index_leaf, page_id={}, cell_divider_idx={})",
                                    page.get().id,
                                    cell_divider_idx,
                                );
                                valid = false;
                            }
                        }
                        _ => {
                            unreachable!()
                        }
                    }
                    current_index_cell += 1;
                }
            }
        }
        assert!(valid, "corrupted database, cells were to balanced properly");
    }

    /// Balance the root page.
    /// This is done when the root page overflows, and we need to create a new root page.
    /// See e.g. https://en.wikipedia.org/wiki/B-tree
    fn balance_root(&mut self) {
        /* todo: balance deeper, create child and copy contents of root there. Then split root */
        /* if we are in root page then we just need to create a new root and push key there */

        let is_page_1 = {
            let current_root = self.stack.top();
            current_root.get().id == 1
        };

        let offset = if is_page_1 { DATABASE_HEADER_SIZE } else { 0 };

        let root = self.stack.top();
        let root_contents = root.get_contents();
        let child = self.pager.do_allocate_page(root_contents.page_type(), 0);

        tracing::debug!(
            "balance_root(root={}, rightmost={}, page_type={:?})",
            root.get().id,
            child.get().id,
            root.get_contents().page_type()
        );

        self.pager.add_dirty(root.get().id);
        self.pager.add_dirty(child.get().id);

        let root_buf = root_contents.as_ptr();
        let child_contents = child.get_contents();
        let child_buf = child_contents.as_ptr();
        let (root_pointer_start, root_pointer_len) =
            root_contents.cell_pointer_array_offset_and_size();
        let (child_pointer_start, _) = child.get_contents().cell_pointer_array_offset_and_size();

        let top = root_contents.cell_content_area() as usize;

        // 1. Modify child
        // Copy pointers
        child_buf[child_pointer_start..child_pointer_start + root_pointer_len]
            .copy_from_slice(&root_buf[root_pointer_start..root_pointer_start + root_pointer_len]);
        // Copy cell contents
        child_buf[top..].copy_from_slice(&root_buf[top..]);
        // Copy header
        child_buf[0..root_contents.header_size()]
            .copy_from_slice(&root_buf[offset..offset + root_contents.header_size()]);
        // Copy overflow cells
        std::mem::swap(
            &mut child_contents.overflow_cells,
            &mut root_contents.overflow_cells,
        );
        root_contents.overflow_cells.clear();

        // 2. Modify root
        let new_root_page_type = match root_contents.page_type() {
            PageType::IndexLeaf => PageType::IndexInterior,
            PageType::TableLeaf => PageType::TableInterior,
            other => other,
        } as u8;
        // set new page type
        root_contents.write_u8(offset::BTREE_PAGE_TYPE, new_root_page_type);
        root_contents.write_u32(offset::BTREE_RIGHTMOST_PTR, child.get().id as u32);
        root_contents.write_u16(offset::BTREE_CELL_CONTENT_AREA, self.usable_space() as u16);
        root_contents.write_u16(offset::BTREE_CELL_COUNT, 0);
        root_contents.write_u16(offset::BTREE_FIRST_FREEBLOCK, 0);

        root_contents.write_u8(offset::BTREE_FRAGMENTED_BYTES_COUNT, 0);
        root_contents.overflow_cells.clear();
        self.root_page = root.get().id;
        self.stack.clear();
        self.stack.push(root.clone());
        self.stack.push(child.clone());
    }

    fn usable_space(&self) -> usize {
        self.pager.usable_space()
    }

    /// Find the index of the cell in the page that contains the given rowid.
    fn find_cell(&self, page: &PageContent, key: &BTreeKey) -> usize {
        let mut cell_idx = 0;
        let cell_count = page.cell_count();
        while cell_idx < cell_count {
            match page
                .cell_get(
                    cell_idx,
                    payload_overflow_threshold_max(page.page_type(), self.usable_space() as u16),
                    payload_overflow_threshold_min(page.page_type(), self.usable_space() as u16),
                    self.usable_space(),
                )
                .unwrap()
            {
                BTreeCell::TableLeafCell(cell) => {
                    if key.to_rowid() <= cell._rowid {
                        break;
                    }
                }
                BTreeCell::TableInteriorCell(cell) => {
                    if key.to_rowid() <= cell._rowid {
                        break;
                    }
                }
                BTreeCell::IndexInteriorCell(IndexInteriorCell { payload, .. })
                | BTreeCell::IndexLeafCell(IndexLeafCell { payload, .. }) => {
                    // TODO: implement efficient comparison of records
                    // e.g. https://github.com/sqlite/sqlite/blob/master/src/vdbeaux.c#L4719
                    read_record(
                        payload,
                        self.get_immutable_record_or_create().as_mut().unwrap(),
                    )
                    .expect("failed to read record");
                    let order = compare_immutable(
                        key.to_index_key_values(),
                        self.get_immutable_record().as_ref().unwrap().get_values(),
                    );
                    match order {
                        Ordering::Less | Ordering::Equal => {
                            break;
                        }
                        Ordering::Greater => {}
                    }
                }
            }
            cell_idx += 1;
        }
        assert!(cell_idx <= cell_count);
        cell_idx
    }

    pub fn seek_end(&mut self) -> Result<CursorResult<()>> {
        assert!(self.mv_cursor.is_none()); // unsure about this -_-
        self.move_to_root();
        loop {
            let mem_page = self.stack.top();
            let page_id = mem_page.get().id;
            let page = self.pager.read_page(page_id)?;
            return_if_locked!(page);

            let contents = page.get().contents.as_ref().unwrap();
            if contents.is_leaf() {
                // set cursor just past the last cell to append
                self.stack.set_cell_index(contents.cell_count() as i32);
                return Ok(CursorResult::Ok(()));
            }

            match contents.rightmost_pointer() {
                Some(right_most_pointer) => {
                    self.stack.set_cell_index(contents.cell_count() as i32 + 1); // invalid on interior
                    let child = self.pager.read_page(right_most_pointer as usize)?;
                    self.stack.push(child);
                }
                None => unreachable!("interior page must have rightmost pointer"),
            }
        }
    }

    pub fn seek_to_last(&mut self) -> Result<CursorResult<()>> {
        return_if_io!(self.move_to_rightmost());
        let rowid = return_if_io!(self.get_next_record(None));
        if rowid.is_none() {
            let is_empty = return_if_io!(self.is_empty_table());
            assert!(is_empty);
            return Ok(CursorResult::Ok(()));
        }
        self.rowid.replace(rowid);
        Ok(CursorResult::Ok(()))
    }

    pub fn is_empty(&self) -> bool {
        self.empty_record.get()
    }

    pub fn root_page(&self) -> usize {
        self.root_page
    }

    pub fn rewind(&mut self) -> Result<CursorResult<()>> {
        if self.mv_cursor.is_some() {
            let rowid = return_if_io!(self.get_next_record(None));
            self.rowid.replace(rowid);
            self.empty_record.replace(rowid.is_none());
        } else {
            self.move_to_root();

            let rowid = return_if_io!(self.get_next_record(None));
            self.rowid.replace(rowid);
            self.empty_record.replace(rowid.is_none());
        }
        Ok(CursorResult::Ok(()))
    }

    pub fn last(&mut self) -> Result<CursorResult<()>> {
        assert!(self.mv_cursor.is_none());
        match self.move_to_rightmost()? {
            CursorResult::Ok(_) => self.prev(),
            CursorResult::IO => Ok(CursorResult::IO),
        }
    }

    pub fn next(&mut self) -> Result<CursorResult<()>> {
        let rowid = return_if_io!(self.get_next_record(None));
        self.rowid.replace(rowid);
        self.empty_record.replace(rowid.is_none());
        Ok(CursorResult::Ok(()))
    }

    pub fn prev(&mut self) -> Result<CursorResult<()>> {
        assert!(self.mv_cursor.is_none());
        match self.get_prev_record(None)? {
            CursorResult::Ok(rowid) => {
                self.rowid.replace(rowid);
                self.empty_record.replace(rowid.is_none());
                Ok(CursorResult::Ok(()))
            }
            CursorResult::IO => Ok(CursorResult::IO),
        }
    }

    pub fn rowid(&self) -> Result<Option<u64>> {
        if let Some(mv_cursor) = &self.mv_cursor {
            let mv_cursor = mv_cursor.borrow();
            return Ok(mv_cursor.current_row_id().map(|rowid| rowid.row_id));
        }
        Ok(self.rowid.get())
    }

    pub fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<CursorResult<bool>> {
        assert!(self.mv_cursor.is_none());
        let rowid = return_if_io!(self.do_seek(key, op));
        self.rowid.replace(rowid);
        self.empty_record.replace(rowid.is_none());
        Ok(CursorResult::Ok(rowid.is_some()))
    }

    pub fn record(&self) -> Ref<Option<ImmutableRecord>> {
        self.reusable_immutable_record.borrow()
    }

    pub fn insert(
        &mut self,
        key: &BTreeKey,
        moved_before: bool, /* Indicate whether it's necessary to traverse to find the leaf page */
    ) -> Result<CursorResult<()>> {
        tracing::trace!("insert");
        match &self.mv_cursor {
            Some(mv_cursor) => match key.maybe_rowid() {
                Some(rowid) => {
                    let row_id = crate::mvcc::database::RowID::new(self.table_id() as u64, rowid);
                    let record_buf = key.get_record().unwrap().get_payload().to_vec();
                    let row = crate::mvcc::database::Row::new(row_id, record_buf);
                    mv_cursor.borrow_mut().insert(row).unwrap();
                }
                None => todo!("Support mvcc inserts with index btrees"),
            },
            None => {
                tracing::trace!("moved {}", moved_before);
                if !moved_before {
                    match key {
                        BTreeKey::IndexKey(_) => {
                            return_if_io!(self
                                .move_to(SeekKey::IndexKey(key.get_record().unwrap()), SeekOp::GE))
                        }
                        BTreeKey::TableRowId(_) => return_if_io!(
                            self.move_to(SeekKey::TableRowId(key.to_rowid()), SeekOp::EQ)
                        ),
                    }
                }
                return_if_io!(self.insert_into_page(key));
                if key.maybe_rowid().is_some() {
                    let int_key = key.to_rowid();
                    self.rowid.replace(Some(int_key));
                }
            }
        };
        Ok(CursorResult::Ok(()))
    }

    /// Delete state machine flow:
    /// 1. Start -> check if the rowid to be delete is present in the page or not. If not we early return
    /// 2. LoadPage -> load the page.
    /// 3. FindCell -> find the cell to be deleted in the page.
    /// 4. ClearOverflowPages -> clear overflow pages associated with the cell. here if the cell is a leaf page go to DropCell state
    ///    or else go to InteriorNodeReplacement
    /// 5. InteriorNodeReplacement -> we copy the left subtree leaf node into the deleted interior node's place.
    /// 6. DropCell -> only for leaf nodes. drop the cell.
    /// 7. CheckNeedsBalancing -> check if balancing is needed. If yes, move to StartBalancing else move to StackRetreat
    /// 8. WaitForBalancingToComplete -> perform balancing
    /// 9. SeekAfterBalancing -> adjust the cursor to a node that is closer to the deleted value. go to Finish
    /// 10. StackRetreat -> perform stack retreat for cursor positioning. only when balancing is not needed. go to Finish
    /// 11. Finish -> Delete operation is done. Return CursorResult(Ok())
    pub fn delete(&mut self) -> Result<CursorResult<()>> {
        assert!(self.mv_cursor.is_none());

        if let CursorState::None = &self.state {
            self.state = CursorState::Delete(DeleteInfo {
                state: DeleteState::Start,
                balance_write_info: None,
            })
        }

        loop {
            let delete_state = {
                let delete_info = self.state.delete_info().expect("cannot get delete info");
                delete_info.state.clone()
            };

            match delete_state {
                DeleteState::Start => {
                    let _target_rowid = match self.rowid.get() {
                        Some(rowid) => rowid,
                        None => {
                            self.state = CursorState::None;
                            return Ok(CursorResult::Ok(()));
                        }
                    };

                    let delete_info = self.state.mut_delete_info().unwrap();
                    delete_info.state = DeleteState::LoadPage;
                }

                DeleteState::LoadPage => {
                    let page = self.stack.top();
                    return_if_locked_maybe_load!(self.pager, page);

                    let delete_info = self.state.mut_delete_info().unwrap();
                    delete_info.state = DeleteState::FindCell;
                }

                DeleteState::FindCell => {
                    let page = self.stack.top();
                    let mut cell_idx = self.stack.current_cell_index() as usize;
                    cell_idx -= 1;

                    let contents = page.get().contents.as_ref().unwrap();
                    if cell_idx >= contents.cell_count() {
                        return_corrupt!(format!(
                            "Corrupted page: cell index {} is out of bounds for page with {} cells",
                            cell_idx,
                            contents.cell_count()
                        ));
                    }

                    let cell = contents.cell_get(
                        cell_idx,
                        payload_overflow_threshold_max(
                            contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        payload_overflow_threshold_min(
                            contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        self.usable_space(),
                    )?;

                    let original_child_pointer = match &cell {
                        BTreeCell::TableInteriorCell(interior) => Some(interior._left_child_page),
                        _ => None,
                    };

                    let delete_info = self.state.mut_delete_info().unwrap();
                    delete_info.state = DeleteState::ClearOverflowPages {
                        cell_idx,
                        cell,
                        original_child_pointer,
                    };
                }

                DeleteState::ClearOverflowPages {
                    cell_idx,
                    cell,
                    original_child_pointer,
                } => {
                    return_if_io!(self.clear_overflow_pages(&cell));

                    let page = self.stack.top();
                    let contents = page.get().contents.as_ref().unwrap();

                    let delete_info = self.state.mut_delete_info().unwrap();
                    if !contents.is_leaf() {
                        delete_info.state = DeleteState::InteriorNodeReplacement {
                            cell_idx,
                            original_child_pointer,
                        };
                    } else {
                        delete_info.state = DeleteState::DropCell { cell_idx };
                    }
                }

                DeleteState::InteriorNodeReplacement {
                    cell_idx,
                    original_child_pointer,
                } => {
                    // This is an interior node, we need to handle deletion differently
                    // For interior nodes:
                    // 1. Move cursor to largest entry in left subtree
                    // 2. Copy that entry to replace the one being deleted
                    // 3. Delete the leaf entry

                    // Move to the largest key in the left subtree
                    return_if_io!(self.prev());

                    let leaf_page = self.stack.top();
                    return_if_locked!(leaf_page);

                    if !leaf_page.is_loaded() {
                        self.pager.load_page(leaf_page.clone())?;
                        return Ok(CursorResult::IO);
                    }

                    let parent_page = {
                        self.stack.pop();
                        let parent = self.stack.top();
                        self.stack.push(leaf_page.clone());
                        parent
                    };

                    if !parent_page.is_loaded() {
                        self.pager.load_page(parent_page.clone())?;
                        return Ok(CursorResult::IO);
                    }

                    let leaf_contents = leaf_page.get().contents.as_ref().unwrap();
                    let leaf_cell_idx = self.stack.current_cell_index() as usize - 1;
                    let predecessor_cell = leaf_contents.cell_get(
                        leaf_cell_idx,
                        payload_overflow_threshold_max(
                            leaf_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        payload_overflow_threshold_min(
                            leaf_contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        self.usable_space(),
                    )?;

                    parent_page.set_dirty();
                    self.pager.add_dirty(parent_page.get().id);

                    let parent_contents = parent_page.get().contents.as_mut().unwrap();

                    // Create an interior cell from the leaf cell
                    let mut cell_payload: Vec<u8> = Vec::new();
                    match predecessor_cell {
                        BTreeCell::TableLeafCell(leaf_cell) => {
                            if let Some(child_pointer) = original_child_pointer {
                                cell_payload.extend_from_slice(&child_pointer.to_be_bytes());
                                write_varint_to_vec(leaf_cell._rowid, &mut cell_payload);
                            }
                        }
                        _ => unreachable!("Expected table leaf cell"),
                    }

                    insert_into_cell(
                        parent_contents,
                        &cell_payload,
                        cell_idx,
                        self.usable_space() as u16,
                    )?;
                    drop_cell(parent_contents, cell_idx, self.usable_space() as u16)?;

                    let delete_info = self.state.mut_delete_info().unwrap();
                    delete_info.state = DeleteState::CheckNeedsBalancing;
                }

                DeleteState::DropCell { cell_idx } => {
                    let page = self.stack.top();
                    return_if_locked!(page);

                    if !page.is_loaded() {
                        self.pager.load_page(page.clone())?;
                        return Ok(CursorResult::IO);
                    }

                    page.set_dirty();
                    self.pager.add_dirty(page.get().id);

                    let contents = page.get().contents.as_mut().unwrap();
                    drop_cell(contents, cell_idx, self.usable_space() as u16)?;

                    let delete_info = self.state.mut_delete_info().unwrap();
                    delete_info.state = DeleteState::CheckNeedsBalancing;
                }

                DeleteState::CheckNeedsBalancing => {
                    let page = self.stack.top();
                    return_if_locked!(page);

                    if !page.is_loaded() {
                        self.pager.load_page(page.clone())?;
                        return Ok(CursorResult::IO);
                    }

                    let contents = page.get().contents.as_ref().unwrap();
                    let free_space = compute_free_space(contents, self.usable_space() as u16);
                    let needs_balancing = free_space as usize * 3 > self.usable_space() * 2;

                    let target_rowid = self.rowid.get().unwrap();

                    let delete_info = self.state.mut_delete_info().unwrap();
                    if needs_balancing {
                        delete_info.state = DeleteState::StartBalancing { target_rowid };
                    } else {
                        delete_info.state = DeleteState::StackRetreat;
                    }
                }

                DeleteState::StartBalancing { target_rowid } => {
                    let delete_info = self.state.mut_delete_info().unwrap();

                    if delete_info.balance_write_info.is_none() {
                        let mut write_info = WriteInfo::new();
                        write_info.state = WriteState::BalanceStart;
                        delete_info.balance_write_info = Some(write_info);
                    }

                    delete_info.state = DeleteState::WaitForBalancingToComplete { target_rowid }
                }

                DeleteState::WaitForBalancingToComplete { target_rowid } => {
                    let delete_info = self.state.mut_delete_info().unwrap();

                    // Switch the CursorState to Write state for balancing
                    let write_info = delete_info.balance_write_info.take().unwrap();
                    self.state = CursorState::Write(write_info);

                    match self.balance()? {
                        // TODO(Krishna): Add second balance in the case where deletion causes cursor to end up
                        // a level deeper.
                        CursorResult::Ok(()) => {
                            let write_info = match &self.state {
                                CursorState::Write(wi) => wi.clone(),
                                _ => unreachable!("Balance operation changed cursor state"),
                            };

                            // Move to seek state
                            self.state = CursorState::Delete(DeleteInfo {
                                state: DeleteState::SeekAfterBalancing { target_rowid },
                                balance_write_info: Some(write_info),
                            });
                        }

                        CursorResult::IO => {
                            // Save balance progress and return IO
                            let write_info = match &self.state {
                                CursorState::Write(wi) => wi.clone(),
                                _ => unreachable!("Balance operation changed cursor state"),
                            };

                            self.state = CursorState::Delete(DeleteInfo {
                                state: DeleteState::WaitForBalancingToComplete { target_rowid },
                                balance_write_info: Some(write_info),
                            });
                            return Ok(CursorResult::IO);
                        }
                    }
                }

                DeleteState::SeekAfterBalancing { target_rowid } => {
                    return_if_io!(self.move_to(SeekKey::TableRowId(target_rowid), SeekOp::EQ));

                    let delete_info = self.state.mut_delete_info().unwrap();
                    delete_info.state = DeleteState::Finish;
                    delete_info.balance_write_info = None;
                }

                DeleteState::StackRetreat => {
                    self.stack.retreat();
                    let delete_info = self.state.mut_delete_info().unwrap();
                    delete_info.state = DeleteState::Finish;
                    delete_info.balance_write_info = None;
                }

                DeleteState::Finish => {
                    self.state = CursorState::None;
                    return Ok(CursorResult::Ok(()));
                }
            }
        }
    }

    pub fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    pub fn get_null_flag(&self) -> bool {
        self.null_flag
    }

    /// Search for a key in an Index Btree. Looking up indexes that need to be unique, we cannot compare the rowid
    pub fn key_exists_in_index(&mut self, key: &ImmutableRecord) -> Result<CursorResult<bool>> {
        return_if_io!(self.seek(SeekKey::IndexKey(key), SeekOp::GE));

        let record_opt = self.record();
        match record_opt.as_ref() {
            Some(record) => {
                // Existing record found — compare prefix
                let existing_key = &record.get_values()[..record.count().saturating_sub(1)];
                let inserted_key_vals = &key.get_values();
                if existing_key
                    .iter()
                    .zip(inserted_key_vals.iter())
                    .all(|(a, b)| a == b)
                {
                    return Ok(CursorResult::Ok(true)); // duplicate
                }
            }
            None => {
                // Cursor not pointing at a record — table is empty or past last
                return Ok(CursorResult::Ok(false));
            }
        }

        Ok(CursorResult::Ok(false)) // not a duplicate
    }

    pub fn exists(&mut self, key: &OwnedValue) -> Result<CursorResult<bool>> {
        assert!(self.mv_cursor.is_none());
        let int_key = match key {
            OwnedValue::Integer(i) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let _ = return_if_io!(self.move_to(SeekKey::TableRowId(*int_key as u64), SeekOp::EQ));
        let page = self.stack.top();
        // TODO(pere): request load
        return_if_locked!(page);

        let contents = page.get().contents.as_ref().unwrap();

        // find cell
        let int_key = match key {
            OwnedValue::Integer(i) => *i as u64,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let cell_idx = self.find_cell(contents, &BTreeKey::new_table_rowid(int_key, None));
        if cell_idx >= contents.cell_count() {
            Ok(CursorResult::Ok(false))
        } else {
            let equals = match &contents.cell_get(
                cell_idx,
                payload_overflow_threshold_max(contents.page_type(), self.usable_space() as u16),
                payload_overflow_threshold_min(contents.page_type(), self.usable_space() as u16),
                self.usable_space(),
            )? {
                BTreeCell::TableLeafCell(l) => l._rowid == int_key,
                _ => unreachable!(),
            };
            Ok(CursorResult::Ok(equals))
        }
    }

    /// Clear the overflow pages linked to a specific page provided by the leaf cell
    /// Uses a state machine to keep track of it's operations so that traversal can be
    /// resumed from last point after IO interruption
    fn clear_overflow_pages(&mut self, cell: &BTreeCell) -> Result<CursorResult<()>> {
        loop {
            let state = self.overflow_state.take().unwrap_or(OverflowState::Start);

            match state {
                OverflowState::Start => {
                    let first_overflow_page = match cell {
                        BTreeCell::TableLeafCell(leaf_cell) => leaf_cell.first_overflow_page,
                        BTreeCell::IndexLeafCell(leaf_cell) => leaf_cell.first_overflow_page,
                        BTreeCell::IndexInteriorCell(interior_cell) => {
                            interior_cell.first_overflow_page
                        }
                        BTreeCell::TableInteriorCell(_) => return Ok(CursorResult::Ok(())), // No overflow pages
                    };

                    if let Some(page) = first_overflow_page {
                        self.overflow_state = Some(OverflowState::ProcessPage { next_page: page });
                        continue;
                    } else {
                        self.overflow_state = Some(OverflowState::Done);
                    }
                }
                OverflowState::ProcessPage { next_page } => {
                    if next_page < 2
                        || next_page as usize > self.pager.db_header.lock().database_size as usize
                    {
                        self.overflow_state = None;
                        return Err(LimboError::Corrupt("Invalid overflow page number".into()));
                    }
                    let page = self.pager.read_page(next_page as usize)?;
                    return_if_locked!(page);

                    let contents = page.get().contents.as_ref().unwrap();
                    let next = contents.read_u32(0);

                    self.pager.free_page(Some(page), next_page as usize)?;

                    if next != 0 {
                        self.overflow_state = Some(OverflowState::ProcessPage { next_page: next });
                    } else {
                        self.overflow_state = Some(OverflowState::Done);
                    }
                }
                OverflowState::Done => {
                    self.overflow_state = None;
                    return Ok(CursorResult::Ok(()));
                }
            };
        }
    }

    /// Destroys a B-tree by freeing all its pages in an iterative depth-first order.
    /// This ensures child pages are freed before their parents
    /// Uses a state machine to keep track of the operation to ensure IO doesn't cause repeated traversals
    ///
    /// # Example
    /// For a B-tree with this structure (where 4' is an overflow page):
    /// ```text
    ///            1 (root)
    ///           /        \
    ///          2          3
    ///        /   \      /   \
    /// 4' <- 4     5    6     7
    /// ```
    ///
    /// The destruction order would be: [4',4,5,2,6,7,3,1]
    pub fn btree_destroy(&mut self) -> Result<CursorResult<()>> {
        if let CursorState::None = &self.state {
            self.move_to_root();
            self.state = CursorState::Destroy(DestroyInfo {
                state: DestroyState::Start,
            });
        }

        loop {
            let destroy_state = {
                let destroy_info = self
                    .state
                    .destroy_info()
                    .expect("unable to get a mut reference to destroy state in cursor");
                destroy_info.state.clone()
            };

            match destroy_state {
                DestroyState::Start => {
                    let destroy_info = self
                        .state
                        .mut_destroy_info()
                        .expect("unable to get a mut reference to destroy state in cursor");
                    destroy_info.state = DestroyState::LoadPage;
                }
                DestroyState::LoadPage => {
                    let page = self.stack.top();
                    return_if_locked_maybe_load!(self.pager, page);

                    let destroy_info = self
                        .state
                        .mut_destroy_info()
                        .expect("unable to get a mut reference to destroy state in cursor");
                    destroy_info.state = DestroyState::ProcessPage;
                }
                DestroyState::ProcessPage => {
                    let page = self.stack.top();
                    assert!(page.is_loaded()); //  page should be loaded at this time

                    let contents = page.get().contents.as_ref().unwrap();
                    let cell_idx = self.stack.current_cell_index();

                    //  If we've processed all cells in this page, figure out what to do with this page
                    if cell_idx >= contents.cell_count() as i32 {
                        match (contents.is_leaf(), cell_idx) {
                            //  Leaf pages with all cells processed
                            (true, n) if n >= contents.cell_count() as i32 => {
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::FreePage;
                                continue;
                            }
                            //  Non-leaf page which has processed all children but not it's potential right child
                            (false, n) if n == contents.cell_count() as i32 => {
                                if let Some(rightmost) = contents.rightmost_pointer() {
                                    let rightmost_page =
                                        self.pager.read_page(rightmost as usize)?;
                                    self.stack.advance();
                                    self.stack.push(rightmost_page);
                                    let destroy_info = self.state.mut_destroy_info().expect(
                                        "unable to get a mut reference to destroy state in cursor",
                                    );
                                    destroy_info.state = DestroyState::LoadPage;
                                } else {
                                    let destroy_info = self.state.mut_destroy_info().expect(
                                        "unable to get a mut reference to destroy state in cursor",
                                    );
                                    destroy_info.state = DestroyState::FreePage;
                                }
                                continue;
                            }
                            //  Non-leaf page which has processed all children and it's right child
                            (false, n) if n > contents.cell_count() as i32 => {
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::FreePage;
                                continue;
                            }
                            _ => unreachable!("Invalid cell idx state"),
                        }
                    }

                    //  We have not yet processed all cells in this page
                    //  Get the current cell
                    let cell = contents.cell_get(
                        cell_idx as usize,
                        payload_overflow_threshold_max(
                            contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        payload_overflow_threshold_min(
                            contents.page_type(),
                            self.usable_space() as u16,
                        ),
                        self.usable_space(),
                    )?;

                    match contents.is_leaf() {
                        //  For a leaf cell, clear the overflow pages associated with this cell
                        true => {
                            let destroy_info = self
                                .state
                                .mut_destroy_info()
                                .expect("unable to get a mut reference to destroy state in cursor");
                            destroy_info.state = DestroyState::ClearOverflowPages { cell };
                            continue;
                        }
                        //  For interior cells, check the type of cell to determine what to do
                        false => match &cell {
                            //  For index interior cells, remove the overflow pages
                            BTreeCell::IndexInteriorCell(_) => {
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::ClearOverflowPages { cell };
                                continue;
                            }
                            //  For all other interior cells, load the left child page
                            _ => {
                                let child_page_id = match &cell {
                                    BTreeCell::TableInteriorCell(cell) => cell._left_child_page,
                                    BTreeCell::IndexInteriorCell(cell) => cell.left_child_page,
                                    _ => panic!("expected interior cell"),
                                };
                                let child_page = self.pager.read_page(child_page_id as usize)?;
                                self.stack.advance();
                                self.stack.push(child_page);
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::LoadPage;
                                continue;
                            }
                        },
                    }
                }
                DestroyState::ClearOverflowPages { cell } => {
                    match self.clear_overflow_pages(&cell)? {
                        CursorResult::Ok(_) => match cell {
                            //  For an index interior cell, clear the left child page now that overflow pages have been cleared
                            BTreeCell::IndexInteriorCell(index_int_cell) => {
                                let child_page = self
                                    .pager
                                    .read_page(index_int_cell.left_child_page as usize)?;
                                self.stack.advance();
                                self.stack.push(child_page);
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::LoadPage;
                                continue;
                            }
                            //  For any leaf cell, advance the index now that overflow pages have been cleared
                            BTreeCell::TableLeafCell(_) | BTreeCell::IndexLeafCell(_) => {
                                self.stack.advance();
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::LoadPage;
                            }
                            _ => panic!("unexpected cell type"),
                        },
                        CursorResult::IO => return Ok(CursorResult::IO),
                    }
                }
                DestroyState::FreePage => {
                    let page = self.stack.top();
                    let page_id = page.get().id;

                    self.pager.free_page(Some(page), page_id)?;

                    if self.stack.has_parent() {
                        self.stack.pop();
                        let destroy_info = self
                            .state
                            .mut_destroy_info()
                            .expect("unable to get a mut reference to destroy state in cursor");
                        destroy_info.state = DestroyState::ProcessPage;
                    } else {
                        self.state = CursorState::None;
                        return Ok(CursorResult::Ok(()));
                    }
                }
            }
        }
    }

    pub fn table_id(&self) -> usize {
        self.root_page
    }

    pub fn overwrite_cell(
        &mut self,
        page_ref: PageRef,
        cell_idx: usize,
        record: &ImmutableRecord,
    ) -> Result<CursorResult<()>> {
        // build the new payload
        let page_type = page_ref.get().contents.as_ref().unwrap().page_type();
        let mut new_payload = Vec::with_capacity(record.len());
        fill_cell_payload(
            page_type,
            self.rowid.get(),
            &mut new_payload,
            record,
            self.usable_space() as u16,
            self.pager.clone(),
        );

        // figure out old cell offset & size
        let (old_offset, old_local_size) = {
            let page = page_ref.get().contents.as_ref().unwrap();
            page.cell_get_raw_region(
                cell_idx,
                payload_overflow_threshold_max(page_type, self.usable_space() as u16),
                payload_overflow_threshold_min(page_type, self.usable_space() as u16),
                self.usable_space(),
            )
        };

        // if it all fits in local space and old_local_size is enough, do an in-place overwrite
        if new_payload.len() == old_local_size {
            self.overwrite_content(page_ref.clone(), old_offset, &new_payload)?;
            Ok(CursorResult::Ok(()))
        } else {
            // doesn't fit, drop it and insert a new one
            drop_cell(
                page_ref.get_contents(),
                cell_idx,
                self.usable_space() as u16,
            )?;
            insert_into_cell(
                page_ref.get_contents(),
                &new_payload,
                cell_idx,
                self.usable_space() as u16,
            )?;
            Ok(CursorResult::Ok(()))
        }
    }

    pub fn overwrite_content(
        &mut self,
        page_ref: PageRef,
        dest_offset: usize,
        new_payload: &[u8],
    ) -> Result<CursorResult<()>> {
        return_if_locked!(page_ref);
        let buf = page_ref.get().contents.as_mut().unwrap().as_ptr();
        buf[dest_offset..dest_offset + new_payload.len()].copy_from_slice(&new_payload);

        Ok(CursorResult::Ok(()))
    }

    fn get_immutable_record_or_create(&self) -> std::cell::RefMut<'_, Option<ImmutableRecord>> {
        if self.reusable_immutable_record.borrow().is_none() {
            let record = ImmutableRecord::new(4096, 10);
            self.reusable_immutable_record.replace(Some(record));
        }
        self.reusable_immutable_record.borrow_mut()
    }

    fn get_immutable_record(&self) -> std::cell::RefMut<'_, Option<ImmutableRecord>> {
        self.reusable_immutable_record.borrow_mut()
    }

    pub fn is_write_in_progress(&self) -> bool {
        match self.state {
            CursorState::Write(_) => true,
            _ => false,
        }
    }
}

#[cfg(debug_assertions)]
fn validate_cells_after_insertion(cell_array: &CellArray, leaf_data: bool) {
    for cell in &cell_array.cells {
        assert!(cell.len() >= 4);

        if leaf_data {
            assert!(cell[0] != 0, "payload is {:?}", cell);
        }
    }
}

impl PageStack {
    fn increment_current(&self) {
        self.current_page.set(self.current_page.get() + 1);
    }
    fn decrement_current(&self) {
        assert!(self.current_page.get() > 0);
        self.current_page.set(self.current_page.get() - 1);
    }
    /// Push a new page onto the stack.
    /// This effectively means traversing to a child page.
    fn _push(&self, page: PageRef, starting_cell_idx: i32) {
        tracing::trace!(
            "pagestack::push(current={}, new_page_id={})",
            self.current_page.get(),
            page.get().id
        );
        self.increment_current();
        let current = self.current_page.get();
        assert!(
            current < BTCURSOR_MAX_DEPTH as i32,
            "corrupted database, stack is bigger than expected"
        );
        assert!(current >= 0);
        self.stack.borrow_mut()[current as usize] = Some(page);
        self.cell_indices.borrow_mut()[current as usize] = starting_cell_idx;
    }

    fn push(&self, page: PageRef) {
        self._push(page, 0);
    }

    fn push_backwards(&self, page: PageRef) {
        self._push(page, i32::MAX);
    }

    /// Pop a page off the stack.
    /// This effectively means traversing back up to a parent page.
    fn pop(&self) {
        let current = self.current_page.get();
        assert!(current >= 0);
        tracing::trace!("pagestack::pop(current={})", current);
        self.cell_indices.borrow_mut()[current as usize] = 0;
        self.stack.borrow_mut()[current as usize] = None;
        self.decrement_current();
    }

    /// Get the top page on the stack.
    /// This is the page that is currently being traversed.
    fn top(&self) -> PageRef {
        let page = self.stack.borrow()[self.current()]
            .as_ref()
            .unwrap()
            .clone();
        tracing::trace!(
            "pagestack::top(current={}, page_id={})",
            self.current(),
            page.get().id
        );
        page
    }

    /// Current page pointer being used
    fn current(&self) -> usize {
        let current = self.current_page.get() as usize;
        assert!(self.current_page.get() >= 0);
        current
    }

    /// Cell index of the current page
    fn current_cell_index(&self) -> i32 {
        let current = self.current();
        self.cell_indices.borrow()[current]
    }

    /// Check if the current cell index is less than 0.
    /// This means we have been iterating backwards and have reached the start of the page.
    fn current_cell_index_less_than_min(&self) -> bool {
        let cell_idx = self.current_cell_index();
        cell_idx < 0
    }

    /// Advance the current cell index of the current page to the next cell.
    /// We usually advance after going traversing a new page
    fn advance(&self) {
        let current = self.current();
        tracing::trace!("pagestack::advance {}", self.cell_indices.borrow()[current],);
        self.cell_indices.borrow_mut()[current] += 1;
    }

    fn retreat(&self) {
        let current = self.current();
        tracing::trace!("pagestack::retreat {}", self.cell_indices.borrow()[current]);
        self.cell_indices.borrow_mut()[current] -= 1;
    }

    /// Move the cursor to the next cell in the current page according to the iteration direction.
    fn next_cell_in_direction(&self, iteration_direction: IterationDirection) {
        match iteration_direction {
            IterationDirection::Forwards => {
                self.advance();
            }
            IterationDirection::Backwards => {
                self.retreat();
            }
        }
    }

    fn set_cell_index(&self, idx: i32) {
        let current = self.current();
        self.cell_indices.borrow_mut()[current] = idx
    }

    fn has_parent(&self) -> bool {
        self.current_page.get() > 0
    }

    fn clear(&self) {
        self.current_page.set(-1);
    }
}

impl CellArray {
    pub fn cell_size(&self, cell_idx: usize) -> u16 {
        self.cells[cell_idx].len() as u16
    }

    pub fn cell_count(&self, page_idx: usize) -> usize {
        self.number_of_cells_per_page[page_idx] as usize
    }
}

/// Try to find a free block available and allocate it if found
fn find_free_cell(page_ref: &PageContent, usable_space: u16, amount: usize) -> Result<usize> {
    // NOTE: freelist is in ascending order of keys and pc
    // unuse_space is reserved bytes at the end of page, therefore we must substract from maxpc
    let mut prev_pc = page_ref.offset + offset::BTREE_FIRST_FREEBLOCK;
    let mut pc = page_ref.first_freeblock() as usize;
    let maxpc = usable_space as usize - amount;

    while pc <= maxpc {
        if pc + 4 > usable_space as usize {
            return_corrupt!("Free block header extends beyond page");
        }

        let next = page_ref.read_u16_no_offset(pc);
        let size = page_ref.read_u16_no_offset(pc + 2);

        if amount <= size as usize {
            let new_size = size as usize - amount;
            if new_size < 4 {
                // The code is checking if using a free slot that would leave behind a very small fragment (x < 4 bytes)
                // would cause the total fragmentation to exceed the limit of 60 bytes
                // check sqlite docs https://www.sqlite.org/fileformat.html#:~:text=A%20freeblock%20requires,not%20exceed%2060
                if page_ref.num_frag_free_bytes() > 57 {
                    return Ok(0);
                }
                // Delete the slot from freelist and update the page's fragment count.
                page_ref.write_u16_no_offset(prev_pc, next);
                let frag = page_ref.num_frag_free_bytes() + new_size as u8;
                page_ref.write_u8(offset::BTREE_FRAGMENTED_BYTES_COUNT, frag);
                return Ok(pc);
            } else if new_size + pc > maxpc {
                return_corrupt!("Free block extends beyond page end");
            } else {
                // Requested amount fits inside the current free slot so we reduce its size
                // to account for newly allocated space.
                page_ref.write_u16_no_offset(pc + 2, new_size as u16);
                return Ok(pc + new_size);
            }
        }

        prev_pc = pc;
        pc = next as usize;
        if pc <= prev_pc {
            if pc != 0 {
                return_corrupt!("Free list not in ascending order");
            }
            return Ok(0);
        }
    }
    if pc > maxpc + amount - 4 {
        return_corrupt!("Free block chain extends beyond page end");
    }
    Ok(0)
}

pub fn btree_init_page(page: &PageRef, page_type: PageType, offset: usize, usable_space: u16) {
    // setup btree page
    let contents = page.get();
    tracing::debug!("btree_init_page(id={}, offset={})", contents.id, offset);
    let contents = contents.contents.as_mut().unwrap();
    contents.offset = offset;
    let id = page_type as u8;
    contents.write_u8(offset::BTREE_PAGE_TYPE, id);
    contents.write_u16(offset::BTREE_FIRST_FREEBLOCK, 0);
    contents.write_u16(offset::BTREE_CELL_COUNT, 0);

    contents.write_u16(offset::BTREE_CELL_CONTENT_AREA, usable_space);

    contents.write_u8(offset::BTREE_FRAGMENTED_BYTES_COUNT, 0);
    contents.write_u32(offset::BTREE_RIGHTMOST_PTR, 0);
}

fn to_static_buf(buf: &mut [u8]) -> &'static mut [u8] {
    unsafe { std::mem::transmute::<&mut [u8], &'static mut [u8]>(buf) }
}

fn edit_page(
    page: &mut PageContent,
    start_old_cells: usize,
    start_new_cells: usize,
    number_new_cells: usize,
    cell_array: &CellArray,
    usable_space: u16,
) -> Result<()> {
    tracing::debug!(
        "edit_page start_old_cells={} start_new_cells={} number_new_cells={} cell_array={}",
        start_old_cells,
        start_new_cells,
        number_new_cells,
        cell_array.cells.len()
    );
    let end_old_cells = start_old_cells + page.cell_count() + page.overflow_cells.len();
    let end_new_cells = start_new_cells + number_new_cells;
    let mut count_cells = page.cell_count();
    if start_old_cells < start_new_cells {
        debug_validate_cells!(page, usable_space);
        let number_to_shift = page_free_array(
            page,
            start_old_cells,
            start_new_cells - start_old_cells,
            cell_array,
            usable_space,
        )?;
        // shift pointers left
        let buf = page.as_ptr();
        let (start, _) = page.cell_pointer_array_offset_and_size();
        buf.copy_within(
            start + (number_to_shift * 2)..start + (count_cells * 2),
            start,
        );
        count_cells -= number_to_shift;
        debug_validate_cells!(page, usable_space);
    }
    if end_new_cells < end_old_cells {
        debug_validate_cells!(page, usable_space);
        let number_tail_removed = page_free_array(
            page,
            end_new_cells,
            end_old_cells - end_new_cells,
            cell_array,
            usable_space,
        )?;
        assert!(count_cells >= number_tail_removed);
        count_cells -= number_tail_removed;
        debug_validate_cells!(page, usable_space);
    }
    // TODO: make page_free_array defragment, for now I'm lazy so this will work for now.
    defragment_page(page, usable_space);
    // TODO: add to start
    if start_new_cells < start_old_cells {
        let count = number_new_cells.min(start_old_cells - start_new_cells);
        page_insert_array(page, start_new_cells, count, cell_array, 0, usable_space)?;
        count_cells += count;
    }
    // TODO: overflow cells
    debug_validate_cells!(page, usable_space);
    for i in 0..page.overflow_cells.len() {
        let overflow_cell = &page.overflow_cells[i];
        // cell index in context of new list of cells that should be in the page
        if start_old_cells + overflow_cell.index >= start_new_cells {
            let cell_idx = start_old_cells + overflow_cell.index - start_new_cells;
            if cell_idx < number_new_cells {
                count_cells += 1;
                page_insert_array(
                    page,
                    start_new_cells + cell_idx,
                    1,
                    cell_array,
                    cell_idx,
                    usable_space,
                )?;
            }
        }
    }
    debug_validate_cells!(page, usable_space);
    // TODO: append cells to end
    page_insert_array(
        page,
        start_new_cells + count_cells,
        number_new_cells - count_cells,
        cell_array,
        count_cells,
        usable_space,
    )?;
    debug_validate_cells!(page, usable_space);
    // TODO: noverflow
    page.write_u16(offset::BTREE_CELL_COUNT, number_new_cells as u16);
    Ok(())
}

fn page_free_array(
    page: &mut PageContent,
    first: usize,
    count: usize,
    cell_array: &CellArray,
    usable_space: u16,
) -> Result<usize> {
    tracing::debug!("page_free_array {}..{}", first, first + count);
    let buf = &mut page.as_ptr()[page.offset..usable_space as usize];
    let buf_range = buf.as_ptr_range();
    let mut number_of_cells_removed = 0;
    // TODO: implement fancy smart free block coalescing procedure instead of dumb free to
    // then defragment
    for i in first..first + count {
        let cell = &cell_array.cells[i];
        let cell_pointer = cell.as_ptr_range();
        // check if not overflow cell
        if cell_pointer.start >= buf_range.start && cell_pointer.start < buf_range.end {
            assert!(
                cell_pointer.end >= buf_range.start && cell_pointer.end <= buf_range.end,
                "whole cell should be inside the page"
            );
            // TODO: remove pointer too
            let offset = (cell_pointer.start as usize - buf_range.start as usize) as u16;
            let len = (cell_pointer.end as usize - cell_pointer.start as usize) as u16;
            free_cell_range(page, offset, len, usable_space)?;
            page.write_u16(offset::BTREE_CELL_COUNT, page.cell_count() as u16 - 1);
            number_of_cells_removed += 1;
        }
    }
    Ok(number_of_cells_removed)
}
fn page_insert_array(
    page: &mut PageContent,
    first: usize,
    count: usize,
    cell_array: &CellArray,
    mut start_insert: usize,
    usable_space: u16,
) -> Result<()> {
    // TODO: implement faster algorithm, this is doing extra work that's not needed.
    // See pageInsertArray to understand faster way.
    tracing::debug!(
        "page_insert_array(cell_array.cells={}..{}, cell_count={}, page_type={:?})",
        first,
        first + count,
        page.cell_count(),
        page.page_type()
    );
    for i in first..first + count {
        debug_validate_cells!(page, usable_space);
        insert_into_cell(page, cell_array.cells[i], start_insert, usable_space)?;
        debug_validate_cells!(page, usable_space);
        start_insert += 1;
    }
    Ok(())
}

/// Free the range of bytes that a cell occupies.
/// This function also updates the freeblock list in the page.
/// Freeblocks are used to keep track of free space in the page,
/// and are organized as a linked list.
fn free_cell_range(
    page: &mut PageContent,
    mut offset: u16,
    len: u16,
    usable_space: u16,
) -> Result<()> {
    if len < 4 {
        return_corrupt!("Minimum cell size is 4");
    }

    if offset > usable_space.saturating_sub(4) {
        return_corrupt!("Start offset beyond usable space");
    }

    let mut size = len;
    let mut end = offset + len;
    let mut pointer_to_pc = page.offset as u16 + 1;
    // if the freeblock list is empty, we set this block as the first freeblock in the page header.
    let pc = if page.first_freeblock() == 0 {
        0
    } else {
        // if the freeblock list is not empty, and the offset is greater than the first freeblock,
        // then we need to do some more calculation to figure out where to insert the freeblock
        // in the freeblock linked list.
        let first_block = page.first_freeblock();

        let mut pc = first_block;

        while pc < offset {
            if pc <= pointer_to_pc {
                if pc == 0 {
                    break;
                }
                return_corrupt!("free cell range free block not in ascending order");
            }

            let next = page.read_u16_no_offset(pc as usize);
            pointer_to_pc = pc;
            pc = next;
        }

        if pc > usable_space - 4 {
            return_corrupt!("Free block beyond usable space");
        }
        let mut removed_fragmentation = 0;
        if pc > 0 && offset + len + 3 >= pc {
            removed_fragmentation = (pc - end) as u8;

            if end > pc {
                return_corrupt!("Invalid block overlap");
            }
            end = pc + page.read_u16_no_offset(pc as usize + 2);
            if end > usable_space {
                return_corrupt!("Coalesced block extends beyond page");
            }
            size = end - offset;
            pc = page.read_u16_no_offset(pc as usize);
        }

        if pointer_to_pc > page.offset as u16 + 1 {
            let prev_end = pointer_to_pc + page.read_u16_no_offset(pointer_to_pc as usize + 2);
            if prev_end + 3 >= offset {
                if prev_end > offset {
                    return_corrupt!("Invalid previous block overlap");
                }
                removed_fragmentation += (offset - prev_end) as u8;
                size = end - pointer_to_pc;
                offset = pointer_to_pc;
            }
        }
        if removed_fragmentation > page.num_frag_free_bytes() {
            return_corrupt!(format!(
                "Invalid fragmentation count. Had {} and removed {}",
                page.num_frag_free_bytes(),
                removed_fragmentation
            ));
        }
        let frag = page.num_frag_free_bytes() - removed_fragmentation;
        page.write_u8(offset::BTREE_FRAGMENTED_BYTES_COUNT, frag);
        pc
    };

    if offset <= page.cell_content_area() {
        if offset < page.cell_content_area() {
            return_corrupt!("Free block before content area");
        }
        if pointer_to_pc != page.offset as u16 + offset::BTREE_FIRST_FREEBLOCK as u16 {
            return_corrupt!("Invalid content area merge");
        }
        page.write_u16(offset::BTREE_FIRST_FREEBLOCK, pc);
        page.write_u16(offset::BTREE_CELL_CONTENT_AREA, end);
    } else {
        page.write_u16_no_offset(pointer_to_pc as usize, offset);
        page.write_u16_no_offset(offset as usize, pc);
        page.write_u16_no_offset(offset as usize + 2, size);
    }

    Ok(())
}

/// Defragment a page. This means packing all the cells to the end of the page.
fn defragment_page(page: &PageContent, usable_space: u16) {
    debug_validate_cells!(page, usable_space);
    tracing::debug!("defragment_page");
    let cloned_page = page.clone();
    // TODO(pere): usable space should include offset probably
    let mut cbrk = usable_space;

    // TODO: implement fast algorithm

    let last_cell = usable_space - 4;
    let first_cell = cloned_page.unallocated_region_start() as u16;

    if cloned_page.cell_count() > 0 {
        let read_buf = cloned_page.as_ptr();
        let write_buf = page.as_ptr();

        for i in 0..cloned_page.cell_count() {
            let (cell_offset, _) = page.cell_pointer_array_offset_and_size();
            let cell_idx = cell_offset + (i * 2);

            let pc = cloned_page.read_u16_no_offset(cell_idx);
            if pc > last_cell {
                unimplemented!("corrupted page");
            }

            assert!(pc <= last_cell);

            let (_, size) = cloned_page.cell_get_raw_region(
                i,
                payload_overflow_threshold_max(page.page_type(), usable_space),
                payload_overflow_threshold_min(page.page_type(), usable_space),
                usable_space as usize,
            );
            let size = size as u16;
            cbrk -= size;
            if cbrk < first_cell || pc + size > usable_space {
                todo!("corrupt");
            }
            assert!(cbrk + size <= usable_space && cbrk >= first_cell);
            // set new pointer
            page.write_u16_no_offset(cell_idx, cbrk);
            // copy payload
            write_buf[cbrk as usize..cbrk as usize + size as usize]
                .copy_from_slice(&read_buf[pc as usize..pc as usize + size as usize]);
        }
    }

    // assert!( nfree >= 0 );
    // if( data[hdr+7]+cbrk-iCellFirst!=pPage->nFree ){
    //   return SQLITE_CORRUPT_PAGE(pPage);
    // }
    assert!(cbrk >= first_cell);

    // set new first byte of cell content
    page.write_u16(offset::BTREE_CELL_CONTENT_AREA, cbrk);
    // set free block to 0, unused spaced can be retrieved from gap between cell pointer end and content start
    page.write_u16(offset::BTREE_FIRST_FREEBLOCK, 0);
    page.write_u8(offset::BTREE_FRAGMENTED_BYTES_COUNT, 0);
    debug_validate_cells!(page, usable_space);
}

#[cfg(debug_assertions)]
/// Only enabled in debug mode, where we ensure that all cells are valid.
fn debug_validate_cells_core(page: &PageContent, usable_space: u16) {
    for i in 0..page.cell_count() {
        let (offset, size) = page.cell_get_raw_region(
            i,
            payload_overflow_threshold_max(page.page_type(), usable_space),
            payload_overflow_threshold_min(page.page_type(), usable_space),
            usable_space as usize,
        );
        let buf = &page.as_ptr()[offset..offset + size];
        assert!(
            size >= 4,
            "cell size should be at least 4 bytes idx={}, cell={:?}, offset={}",
            i,
            buf,
            offset
        );
        if page.is_leaf() {
            assert!(page.as_ptr()[offset] != 0);
        }
        assert!(
            offset + size <= usable_space as usize,
            "cell spans out of usable space"
        );
    }
}

/// Insert a record into a cell.
/// If the cell overflows, an overflow cell is created.
/// insert_into_cell() is called from insert_into_page(),
/// and the overflow cell count is used to determine if the page overflows,
/// i.e. whether we need to balance the btree after the insert.
fn insert_into_cell(
    page: &mut PageContent,
    payload: &[u8],
    cell_idx: usize,
    usable_space: u16,
) -> Result<()> {
    debug_validate_cells!(page, usable_space);
    assert!(
        cell_idx <= page.cell_count() + page.overflow_cells.len(),
        "attempting to add cell to an incorrect place cell_idx={} cell_count={}",
        cell_idx,
        page.cell_count()
    );
    let free = compute_free_space(page, usable_space);
    const CELL_POINTER_SIZE_BYTES: usize = 2;
    let enough_space = payload.len() + CELL_POINTER_SIZE_BYTES <= free as usize;
    if !enough_space {
        // add to overflow cell
        page.overflow_cells.push(OverflowCell {
            index: cell_idx,
            payload: Pin::new(Vec::from(payload)),
        });
        return Ok(());
    }

    let new_cell_data_pointer = allocate_cell_space(page, payload.len() as u16, usable_space)?;
    tracing::debug!(
        "insert_into_cell(idx={}, pc={}, size={})",
        cell_idx,
        new_cell_data_pointer,
        payload.len()
    );
    assert!(new_cell_data_pointer + payload.len() as u16 <= usable_space);
    let buf = page.as_ptr();

    // copy data
    buf[new_cell_data_pointer as usize..new_cell_data_pointer as usize + payload.len()]
        .copy_from_slice(payload);
    //  memmove(pIns+2, pIns, 2*(pPage->nCell - i));
    let (cell_pointer_array_start, _) = page.cell_pointer_array_offset_and_size();
    let cell_pointer_cur_idx = cell_pointer_array_start + (CELL_POINTER_SIZE_BYTES * cell_idx);

    // move existing pointers forward by CELL_POINTER_SIZE_BYTES...
    let n_cells_forward = page.cell_count() - cell_idx;
    let n_bytes_forward = CELL_POINTER_SIZE_BYTES * n_cells_forward;
    if n_bytes_forward > 0 {
        buf.copy_within(
            cell_pointer_cur_idx..cell_pointer_cur_idx + n_bytes_forward,
            cell_pointer_cur_idx + CELL_POINTER_SIZE_BYTES,
        );
    }
    // ...and insert new cell pointer at the current index
    page.write_u16_no_offset(cell_pointer_cur_idx, new_cell_data_pointer);

    // update cell count
    let new_n_cells = (page.cell_count() + 1) as u16;
    page.write_u16(offset::BTREE_CELL_COUNT, new_n_cells);
    debug_validate_cells!(page, usable_space);
    Ok(())
}

/// Free blocks can be zero, meaning the "real free space" that can be used to allocate is expected to be between first cell byte
/// and end of cell pointer area.
#[allow(unused_assignments)]
fn compute_free_space(page: &PageContent, usable_space: u16) -> u16 {
    // TODO(pere): maybe free space is not calculated correctly with offset

    // Usable space, not the same as free space, simply means:
    // space that is not reserved for extensions by sqlite. Usually reserved_space is 0.
    let usable_space = usable_space as usize;

    let mut cell_content_area_start = page.cell_content_area();
    // A zero value for the cell content area pointer is interpreted as 65536.
    // See https://www.sqlite.org/fileformat.html
    // The max page size for a sqlite database is 64kiB i.e. 65536 bytes.
    // 65536 is u16::MAX + 1, and since cell content grows from right to left, this means
    // the cell content area pointer is at the end of the page,
    // i.e.
    // 1. the page size is 64kiB
    // 2. there are no cells on the page
    // 3. there is no reserved space at the end of the page
    if cell_content_area_start == 0 {
        cell_content_area_start = u16::MAX;
    }

    // The amount of free space is the sum of:
    // #1. the size of the unallocated region
    // #2. fragments (isolated 1-3 byte chunks of free space within the cell content area)
    // #3. freeblocks (linked list of blocks of at least 4 bytes within the cell content area that are not in use due to e.g. deletions)

    let pointer_size = if matches!(page.page_type(), PageType::TableLeaf | PageType::IndexLeaf) {
        0
    } else {
        4
    };
    let first_cell = page.offset + 8 + pointer_size + (2 * page.cell_count());
    let mut free_space_bytes =
        cell_content_area_start as usize + page.num_frag_free_bytes() as usize;

    // #3 is computed by iterating over the freeblocks linked list
    let mut cur_freeblock_ptr = page.first_freeblock() as usize;
    if cur_freeblock_ptr > 0 {
        if cur_freeblock_ptr < cell_content_area_start as usize {
            // Freeblocks exist in the cell content area e.g. after deletions
            // They should never exist in the unused area of the page.
            todo!("corrupted page");
        }

        let mut next = 0;
        let mut size = 0;
        loop {
            // TODO: check corruption icellast
            next = page.read_u16_no_offset(cur_freeblock_ptr) as usize; // first 2 bytes in freeblock = next freeblock pointer
            size = page.read_u16_no_offset(cur_freeblock_ptr + 2) as usize; // next 2 bytes in freeblock = size of current freeblock
            free_space_bytes += size;
            // Freeblocks are in order from left to right on the page,
            // so next pointer should > current pointer + its size, or 0 if no next block exists.
            if next <= cur_freeblock_ptr + size + 3 {
                break;
            }
            cur_freeblock_ptr = next;
        }

        // Next should always be 0 (NULL) at this point since we have reached the end of the freeblocks linked list
        assert!(
            next == 0,
            "corrupted page: freeblocks list not in ascending order"
        );

        assert!(
            cur_freeblock_ptr + size <= usable_space,
            "corrupted page: last freeblock extends last page end"
        );
    }

    assert!(
        free_space_bytes <= usable_space,
        "corrupted page: free space is greater than usable space"
    );

    // if( nFree>usableSize || nFree<iCellFirst ){
    //   return SQLITE_CORRUPT_PAGE(pPage);
    // }

    free_space_bytes as u16 - first_cell as u16
}

/// Allocate space for a cell on a page.
fn allocate_cell_space(page_ref: &PageContent, amount: u16, usable_space: u16) -> Result<u16> {
    let amount = amount as usize;

    let (cell_offset, _) = page_ref.cell_pointer_array_offset_and_size();
    let gap = cell_offset + 2 * page_ref.cell_count();
    let mut top = page_ref.cell_content_area() as usize;

    // there are free blocks and enough space
    if page_ref.first_freeblock() != 0 && gap + 2 <= top {
        // find slot
        let pc = find_free_cell(page_ref, usable_space, amount)?;
        if pc != 0 {
            return Ok(pc as u16);
        }
        /* fall through, we might need to defragment */
    }

    if gap + 2 + amount > top {
        // defragment
        defragment_page(page_ref, usable_space);
        top = page_ref.read_u16(offset::BTREE_CELL_CONTENT_AREA) as usize;
    }

    top -= amount;

    page_ref.write_u16(offset::BTREE_CELL_CONTENT_AREA, top as u16);

    assert!(top + amount <= usable_space as usize);
    Ok(top as u16)
}

/// Fill in the cell payload with the record.
/// If the record is too large to fit in the cell, it will spill onto overflow pages.
fn fill_cell_payload(
    page_type: PageType,
    int_key: Option<u64>,
    cell_payload: &mut Vec<u8>,
    record: &ImmutableRecord,
    usable_space: u16,
    pager: Rc<Pager>,
) {
    assert!(matches!(
        page_type,
        PageType::TableLeaf | PageType::IndexLeaf
    ));
    // TODO: make record raw from start, having to serialize is not good
    let record_buf = record.get_payload().to_vec();

    // fill in header
    if matches!(page_type, PageType::TableLeaf) {
        let int_key = int_key.unwrap();
        write_varint_to_vec(record_buf.len() as u64, cell_payload);
        write_varint_to_vec(int_key, cell_payload);
    } else {
        write_varint_to_vec(record_buf.len() as u64, cell_payload);
    }

    let payload_overflow_threshold_max = payload_overflow_threshold_max(page_type, usable_space);
    tracing::debug!(
        "fill_cell_payload(record_size={}, payload_overflow_threshold_max={})",
        record_buf.len(),
        payload_overflow_threshold_max
    );
    if record_buf.len() <= payload_overflow_threshold_max {
        // enough allowed space to fit inside a btree page
        cell_payload.extend_from_slice(record_buf.as_slice());
        return;
    }

    let payload_overflow_threshold_min = payload_overflow_threshold_min(page_type, usable_space);
    // see e.g. https://github.com/sqlite/sqlite/blob/9591d3fe93936533c8c3b0dc4d025ac999539e11/src/dbstat.c#L371
    let mut space_left = payload_overflow_threshold_min
        + (record_buf.len() - payload_overflow_threshold_min) % (usable_space as usize - 4);

    if space_left > payload_overflow_threshold_max {
        space_left = payload_overflow_threshold_min;
    }

    // cell_size must be equal to first value of space_left as this will be the bytes copied to non-overflow page.
    let cell_size = space_left + cell_payload.len() + 4; // 4 is the number of bytes of pointer to first overflow page
    let mut to_copy_buffer = record_buf.as_slice();

    let prev_size = cell_payload.len();
    cell_payload.resize(prev_size + space_left + 4, 0);
    let mut pointer = unsafe { cell_payload.as_mut_ptr().add(prev_size) };
    let mut pointer_to_next = unsafe { cell_payload.as_mut_ptr().add(prev_size + space_left) };
    let mut overflow_pages = Vec::new();

    loop {
        let to_copy = space_left.min(to_copy_buffer.len());
        unsafe { std::ptr::copy(to_copy_buffer.as_ptr(), pointer, to_copy) };

        let left = to_copy_buffer.len() - to_copy;
        if left == 0 {
            break;
        }

        // we still have bytes to add, we will need to allocate new overflow page
        let overflow_page = allocate_overflow_page(pager.clone());
        overflow_pages.push(overflow_page.clone());
        {
            let id = overflow_page.get().id as u32;
            let contents = overflow_page.get().contents.as_mut().unwrap();

            // TODO: take into account offset here?
            let buf = contents.as_ptr();
            let as_bytes = id.to_be_bytes();
            // update pointer to new overflow page
            unsafe { std::ptr::copy(as_bytes.as_ptr(), pointer_to_next, 4) };

            pointer = unsafe { buf.as_mut_ptr().add(4) };
            pointer_to_next = buf.as_mut_ptr();
            space_left = usable_space as usize - 4;
        }

        to_copy_buffer = &to_copy_buffer[to_copy..];
    }

    assert_eq!(cell_size, cell_payload.len());
}

/// Allocate a new overflow page.
/// This is done when a cell overflows and new space is needed.
fn allocate_overflow_page(pager: Rc<Pager>) -> PageRef {
    let page = pager.allocate_page().unwrap();
    tracing::debug!("allocate_overflow_page(id={})", page.get().id);

    // setup overflow page
    let contents = page.get().contents.as_mut().unwrap();
    let buf = contents.as_ptr();
    buf.fill(0);

    page
}

/// Returns the maximum payload size (X) that can be stored directly on a b-tree page without spilling to overflow pages.
///
/// For table leaf pages: X = usable_size - 35
/// For index pages: X = ((usable_size - 12) * 64/255) - 23
///
/// The usable size is the total page size less the reserved space at the end of each page.
/// These thresholds are designed to:
/// - Give a minimum fanout of 4 for index b-trees
/// - Ensure enough payload is on the b-tree page that the record header can usually be accessed
///   without consulting an overflow page
fn payload_overflow_threshold_max(page_type: PageType, usable_space: u16) -> usize {
    match page_type {
        PageType::IndexInterior | PageType::IndexLeaf => {
            ((usable_space as usize - 12) * 64 / 255) - 23 // Index page formula
        }
        PageType::TableInterior | PageType::TableLeaf => {
            usable_space as usize - 35 // Table leaf page formula
        }
    }
}

/// Returns the minimum payload size (M) that must be stored on the b-tree page before spilling to overflow pages is allowed.
///
/// For all page types: M = ((usable_size - 12) * 32/255) - 23
///
/// When payload size P exceeds max_local():
/// - If K = M + ((P-M) % (usable_size-4)) <= max_local(): store K bytes on page
/// - Otherwise: store M bytes on page
///
/// The remaining bytes are stored on overflow pages in both cases.
fn payload_overflow_threshold_min(_page_type: PageType, usable_space: u16) -> usize {
    // Same formula for all page types
    ((usable_space as usize - 12) * 32 / 255) - 23
}

/// Drop a cell from a page.
/// This is done by freeing the range of bytes that the cell occupies.
fn drop_cell(page: &mut PageContent, cell_idx: usize, usable_space: u16) -> Result<()> {
    debug_validate_cells!(page, usable_space);
    let (cell_start, cell_len) = page.cell_get_raw_region(
        cell_idx,
        payload_overflow_threshold_max(page.page_type(), usable_space),
        payload_overflow_threshold_min(page.page_type(), usable_space),
        usable_space as usize,
    );
    free_cell_range(page, cell_start as u16, cell_len as u16, usable_space)?;
    if page.cell_count() > 1 {
        shift_pointers_left(page, cell_idx);
    } else {
        page.write_u16(offset::BTREE_CELL_CONTENT_AREA, usable_space);
        page.write_u16(offset::BTREE_FIRST_FREEBLOCK, 0);
        page.write_u8(offset::BTREE_FRAGMENTED_BYTES_COUNT, 0);
    }
    page.write_u16(offset::BTREE_CELL_COUNT, page.cell_count() as u16 - 1);
    debug_validate_cells!(page, usable_space);
    Ok(())
}

/// Shift pointers to the left once starting from a cell position
/// This is useful when we remove a cell and we want to move left the cells from the right to fill
/// the empty space that's not needed
fn shift_pointers_left(page: &mut PageContent, cell_idx: usize) {
    assert!(page.cell_count() > 0);
    let buf = page.as_ptr();
    let (start, _) = page.cell_pointer_array_offset_and_size();
    let start = start + (cell_idx * 2) + 2;
    let right_cells = page.cell_count() - cell_idx - 1;
    let amount_to_shift = right_cells * 2;
    buf.copy_within(start..start + amount_to_shift, start - 2);
}

#[cfg(test)]
mod tests {
    use rand::{thread_rng, Rng};
    use rand_chacha::{
        rand_core::{RngCore, SeedableRng},
        ChaCha8Rng,
    };
    use test_log::test;

    use super::*;
    use crate::{
        fast_lock::SpinLock,
        io::{Buffer, Completion, MemoryIO, OpenFlags, IO},
        storage::{
            database::DatabaseFile, page_cache::DumbLruPageCache, sqlite3_ondisk,
            sqlite3_ondisk::DatabaseHeader,
        },
        types::Text,
        vdbe::Register,
        BufferPool, Connection, DatabaseStorage, WalFile, WalFileShared, WriteCompletion,
    };
    use std::{
        cell::RefCell, collections::HashSet, mem::transmute, ops::Deref, panic, rc::Rc, sync::Arc,
    };

    use tempfile::TempDir;

    use crate::{
        io::BufferData,
        storage::{
            btree::{
                compute_free_space, fill_cell_payload, payload_overflow_threshold_max,
                payload_overflow_threshold_min,
            },
            pager::PageRef,
            sqlite3_ondisk::{BTreeCell, PageContent, PageType},
        },
        types::OwnedValue,
        Database, Page, Pager, PlatformIO,
    };

    use super::{btree_init_page, defragment_page, drop_cell, insert_into_cell};

    #[allow(clippy::arc_with_non_send_sync)]
    fn get_page(id: usize) -> PageRef {
        let page = Arc::new(Page::new(id));

        let drop_fn = Rc::new(|_| {});
        let inner = PageContent::new(
            0,
            Arc::new(RefCell::new(Buffer::new(
                BufferData::new(vec![0; 4096]),
                drop_fn,
            ))),
        );
        page.get().contents.replace(inner);

        btree_init_page(&page, PageType::TableLeaf, 0, 4096);
        page
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn get_database() -> Arc<Database> {
        let mut path = TempDir::new().unwrap().into_path();
        path.push("test.db");
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .pragma_update(None, "journal_mode", "wal")
                .unwrap();
        }
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io.clone(), path.to_str().unwrap(), false).unwrap();

        db
    }

    fn ensure_cell(page: &mut PageContent, cell_idx: usize, payload: &Vec<u8>) {
        let cell = page.cell_get_raw_region(
            cell_idx,
            payload_overflow_threshold_max(page.page_type(), 4096),
            payload_overflow_threshold_min(page.page_type(), 4096),
            4096,
        );
        tracing::trace!("cell idx={} start={} len={}", cell_idx, cell.0, cell.1);
        let buf = &page.as_ptr()[cell.0..cell.0 + cell.1];
        assert_eq!(buf.len(), payload.len());
        assert_eq!(buf, payload);
    }

    fn add_record(
        id: usize,
        pos: usize,
        page: &mut PageContent,
        record: ImmutableRecord,
        conn: &Rc<Connection>,
    ) -> Vec<u8> {
        let mut payload: Vec<u8> = Vec::new();
        fill_cell_payload(
            page.page_type(),
            Some(id as u64),
            &mut payload,
            &record,
            4096,
            conn.pager.clone(),
        );
        insert_into_cell(page, &payload, pos, 4096).unwrap();
        payload
    }

    #[test]
    fn test_insert_cell() {
        let db = get_database();
        let conn = db.connect().unwrap();
        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;
        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(1))]);
        let payload = add_record(1, 0, page, record, &conn);
        assert_eq!(page.cell_count(), 1);
        let free = compute_free_space(page, 4096);
        assert_eq!(free, 4096 - payload.len() as u16 - 2 - header_size);

        let cell_idx = 0;
        ensure_cell(page, cell_idx, &payload);
    }

    struct Cell {
        pos: usize,
        payload: Vec<u8>,
    }

    #[test]
    fn test_drop_1() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        for i in 0..3 {
            let record = ImmutableRecord::from_registers(&[Register::OwnedValue(
                OwnedValue::Integer(i as i64),
            )]);
            let payload = add_record(i, i, page, record, &conn);
            assert_eq!(page.cell_count(), i + 1);
            let free = compute_free_space(page, usable_space);
            total_size += payload.len() as u16 + 2;
            assert_eq!(free, 4096 - total_size - header_size);
            cells.push(Cell { pos: i, payload });
        }

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }
        cells.remove(1);
        drop_cell(page, 1, usable_space).unwrap();

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }
    }

    fn validate_btree(pager: Rc<Pager>, page_idx: usize) -> (usize, bool) {
        let cursor = BTreeCursor::new(None, pager.clone(), page_idx);
        let page = pager.read_page(page_idx).unwrap();
        let page = page.get();
        let contents = page.contents.as_ref().unwrap();
        let page_type = contents.page_type();
        let mut previous_key = None;
        let mut valid = true;
        let mut depth = None;
        debug_validate_cells!(contents, pager.usable_space() as u16);
        let mut child_pages = Vec::new();
        for cell_idx in 0..contents.cell_count() {
            let cell = contents
                .cell_get(
                    cell_idx,
                    payload_overflow_threshold_max(page_type, 4096),
                    payload_overflow_threshold_min(page_type, 4096),
                    cursor.usable_space(),
                )
                .unwrap();
            let current_depth = match cell {
                BTreeCell::TableLeafCell(..) => 1,
                BTreeCell::TableInteriorCell(TableInteriorCell {
                    _left_child_page, ..
                }) => {
                    child_pages.push(pager.read_page(_left_child_page as usize).unwrap());
                    if _left_child_page == page.id as u32 {
                        valid = false;
                        tracing::error!(
                            "left child page is the same as parent {}",
                            _left_child_page
                        );
                        continue;
                    }
                    let (child_depth, child_valid) =
                        validate_btree(pager.clone(), _left_child_page as usize);
                    valid &= child_valid;
                    child_depth
                }
                _ => panic!("unsupported btree cell: {:?}", cell),
            };
            if current_depth >= 100 {
                tracing::error!("depth is too big");
                return (100, false);
            }
            depth = Some(depth.unwrap_or(current_depth + 1));
            if depth != Some(current_depth + 1) {
                tracing::error!("depth is different for child of page {}", page_idx);
                valid = false;
            }
            match cell {
                BTreeCell::TableInteriorCell(TableInteriorCell { _rowid, .. })
                | BTreeCell::TableLeafCell(TableLeafCell { _rowid, .. }) => {
                    if previous_key.is_some() && previous_key.unwrap() >= _rowid {
                        tracing::error!(
                            "keys are in bad order: prev={:?}, current={}",
                            previous_key,
                            _rowid
                        );
                        valid = false;
                    }
                    previous_key = Some(_rowid);
                }
                _ => panic!("unsupported btree cell: {:?}", cell),
            }
        }
        if let Some(right) = contents.rightmost_pointer() {
            let (right_depth, right_valid) = validate_btree(pager.clone(), right as usize);
            valid &= right_valid;
            depth = Some(depth.unwrap_or(right_depth + 1));
            if depth != Some(right_depth + 1) {
                tracing::error!("depth is different for child of page {}", page_idx);
                valid = false;
            }
        }
        let first_page_type = child_pages.first().map(|p| p.get_contents().page_type());
        if let Some(child_type) = first_page_type {
            for page in child_pages.iter().skip(1) {
                if page.get_contents().page_type() != child_type {
                    tracing::error!("child pages have different types");
                    valid = false;
                }
            }
        }
        if contents.rightmost_pointer().is_none() && contents.cell_count() == 0 {
            valid = false;
        }
        (depth.unwrap(), valid)
    }

    fn format_btree(pager: Rc<Pager>, page_idx: usize, depth: usize) -> String {
        let cursor = BTreeCursor::new(None, pager.clone(), page_idx);
        let page = pager.read_page(page_idx).unwrap();
        let page = page.get();
        let contents = page.contents.as_ref().unwrap();
        let page_type = contents.page_type();
        let mut current = Vec::new();
        let mut child = Vec::new();
        for cell_idx in 0..contents.cell_count() {
            let cell = contents
                .cell_get(
                    cell_idx,
                    payload_overflow_threshold_max(page_type, 4096),
                    payload_overflow_threshold_min(page_type, 4096),
                    cursor.usable_space(),
                )
                .unwrap();
            match cell {
                BTreeCell::TableInteriorCell(cell) => {
                    current.push(format!(
                        "node[rowid:{}, ptr(<=):{}]",
                        cell._rowid, cell._left_child_page
                    ));
                    child.push(format_btree(
                        pager.clone(),
                        cell._left_child_page as usize,
                        depth + 2,
                    ));
                }
                BTreeCell::TableLeafCell(cell) => {
                    current.push(format!(
                        "leaf[rowid:{}, len(payload):{}, overflow:{}]",
                        cell._rowid,
                        cell._payload.len(),
                        cell.first_overflow_page.is_some()
                    ));
                }
                _ => panic!("unsupported btree cell: {:?}", cell),
            }
        }
        if let Some(rightmost) = contents.rightmost_pointer() {
            child.push(format_btree(pager.clone(), rightmost as usize, depth + 2));
        }
        let current = format!(
            "{}-page:{}, ptr(right):{}\n{}+cells:{}",
            " ".repeat(depth),
            page_idx,
            contents.rightmost_pointer().unwrap_or(0),
            " ".repeat(depth),
            current.join(", ")
        );
        if child.is_empty() {
            current
        } else {
            current + "\n" + &child.join("\n")
        }
    }

    fn empty_btree() -> (Rc<Pager>, usize) {
        let db_header = DatabaseHeader::default();
        let page_size = db_header.page_size as usize;

        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let io_file = io.open_file("test.db", OpenFlags::Create, false).unwrap();
        let db_file = Arc::new(DatabaseFile::new(io_file));

        let buffer_pool = Rc::new(BufferPool::new(db_header.page_size as usize));
        let wal_shared = WalFileShared::open_shared(&io, "test.wal", db_header.page_size).unwrap();
        let wal_file = WalFile::new(io.clone(), page_size, wal_shared, buffer_pool.clone());
        let wal = Rc::new(RefCell::new(wal_file));

        let page_cache = Arc::new(parking_lot::RwLock::new(DumbLruPageCache::new(10)));
        let pager = {
            let db_header = Arc::new(SpinLock::new(db_header.clone()));
            Pager::finish_open(db_header, db_file, Some(wal), io, page_cache, buffer_pool).unwrap()
        };
        let pager = Rc::new(pager);
        let page1 = pager.allocate_page().unwrap();
        btree_init_page(&page1, PageType::TableLeaf, 0, 4096);
        (pager, page1.get().id)
    }

    #[test]
    #[ignore]
    pub fn btree_insert_fuzz_ex() {
        for sequence in [
            &[
                (777548915, 3364),
                (639157228, 3796),
                (709175417, 1214),
                (390824637, 210),
                (906124785, 1481),
                (197677875, 1305),
                (457946262, 3734),
                (956825466, 592),
                (835875722, 1334),
                (649214013, 1250),
                (531143011, 1788),
                (765057993, 2351),
                (510007766, 1349),
                (884516059, 822),
                (81604840, 2545),
            ]
            .as_slice(),
            &[
                (293471650, 2452),
                (163608869, 627),
                (544576229, 464),
                (705823748, 3441),
            ]
            .as_slice(),
            &[
                (987283511, 2924),
                (261851260, 1766),
                (343847101, 1657),
                (315844794, 572),
            ]
            .as_slice(),
            &[
                (987283511, 2924),
                (261851260, 1766),
                (343847101, 1657),
                (315844794, 572),
                (649272840, 1632),
                (723398505, 3140),
                (334416967, 3874),
            ]
            .as_slice(),
        ] {
            let (pager, root_page) = empty_btree();
            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            for (key, size) in sequence.iter() {
                run_until_done(
                    || {
                        let key = SeekKey::TableRowId(*key);
                        cursor.move_to(key, SeekOp::EQ)
                    },
                    pager.deref(),
                )
                .unwrap();
                let value = ImmutableRecord::from_registers(&[Register::OwnedValue(
                    OwnedValue::Blob(vec![0; *size]),
                )]);
                tracing::info!("insert key:{}", key);
                run_until_done(
                    || cursor.insert(&BTreeKey::new_table_rowid(*key, Some(&value)), true),
                    pager.deref(),
                )
                .unwrap();
                tracing::info!(
                    "=========== btree ===========\n{}\n\n",
                    format_btree(pager.clone(), root_page, 0)
                );
            }
            for (key, _) in sequence.iter() {
                let seek_key = SeekKey::TableRowId(*key);
                assert!(
                    matches!(
                        cursor.seek(seek_key, SeekOp::EQ).unwrap(),
                        CursorResult::Ok(true)
                    ),
                    "key {} is not found",
                    key
                );
            }
        }
    }

    fn rng_from_time() -> (ChaCha8Rng, u64) {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let rng = ChaCha8Rng::seed_from_u64(seed);
        (rng, seed)
    }

    fn btree_insert_fuzz_run(
        attempts: usize,
        inserts: usize,
        size: impl Fn(&mut ChaCha8Rng) -> usize,
    ) {
        let (mut rng, seed) = rng_from_time();
        let mut seen = HashSet::new();
        tracing::info!("super seed: {}", seed);
        for _ in 0..attempts {
            let (pager, root_page) = empty_btree();
            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            let mut keys = Vec::new();
            tracing::info!("seed: {}", seed);
            for insert_id in 0..inserts {
                let size = size(&mut rng);
                let key = {
                    let result;
                    loop {
                        let key = (rng.next_u64() % (1 << 30)) as i64;
                        if seen.contains(&key) {
                            continue;
                        } else {
                            seen.insert(key);
                        }
                        result = key;
                        break;
                    }
                    result
                };
                keys.push(key);
                tracing::info!(
                    "INSERT INTO t VALUES ({}, randomblob({})); -- {}",
                    key,
                    size,
                    insert_id
                );
                run_until_done(
                    || {
                        let key = SeekKey::TableRowId(key as u64);
                        cursor.move_to(key, SeekOp::EQ)
                    },
                    pager.deref(),
                )
                .unwrap();
                let value = ImmutableRecord::from_registers(&[Register::OwnedValue(
                    OwnedValue::Blob(vec![0; size]),
                )]);
                let btree_before = format_btree(pager.clone(), root_page, 0);
                run_until_done(
                    || cursor.insert(&BTreeKey::new_table_rowid(key as u64, Some(&value)), true),
                    pager.deref(),
                )
                .unwrap();
                // FIXME: add sorted vector instead, should be okay for small amounts of keys for now :P, too lazy to fix right now
                keys.sort();
                cursor.move_to_root();
                let mut valid = true;
                for key in keys.iter() {
                    tracing::trace!("seeking key: {}", key);
                    run_until_done(|| cursor.next(), pager.deref()).unwrap();
                    let cursor_rowid = cursor.rowid().unwrap().unwrap();
                    if *key as u64 != cursor_rowid {
                        valid = false;
                        println!("key {} is not found, got {}", key, cursor_rowid);
                        break;
                    }
                }
                // let's validate btree too so that we undertsand where the btree failed
                if matches!(validate_btree(pager.clone(), root_page), (_, false)) || !valid {
                    let btree_after = format_btree(pager.clone(), root_page, 0);
                    println!("btree before:\n{}", btree_before);
                    println!("btree after:\n{}", btree_after);
                    panic!("invalid btree");
                }
            }
            tracing::info!(
                "=========== btree ===========\n{}\n\n",
                format_btree(pager.clone(), root_page, 0)
            );
            if matches!(validate_btree(pager.clone(), root_page), (_, false)) {
                panic!("invalid btree");
            }
            keys.sort();
            cursor.move_to_root();
            for key in keys.iter() {
                tracing::trace!("seeking key: {}", key);
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
                let cursor_rowid = cursor.rowid().unwrap().unwrap();
                assert_eq!(
                    *key as u64, cursor_rowid,
                    "key {} is not found, got {}",
                    key, cursor_rowid
                );
            }
        }
    }
    #[test]
    pub fn test_drop_odd() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        let total_cells = 10;
        for i in 0..total_cells {
            let record = ImmutableRecord::from_registers(&[Register::OwnedValue(
                OwnedValue::Integer(i as i64),
            )]);
            let payload = add_record(i, i, page, record, &conn);
            assert_eq!(page.cell_count(), i + 1);
            let free = compute_free_space(page, usable_space);
            total_size += payload.len() as u16 + 2;
            assert_eq!(free, 4096 - total_size - header_size);
            cells.push(Cell { pos: i, payload });
        }

        let mut removed = 0;
        let mut new_cells = Vec::new();
        for cell in cells {
            if cell.pos % 2 == 1 {
                drop_cell(page, cell.pos - removed, usable_space).unwrap();
                removed += 1;
            } else {
                new_cells.push(cell);
            }
        }
        let cells = new_cells;
        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }
    }

    #[test]
    pub fn btree_insert_fuzz_run_equal_size() {
        for size in 1..8 {
            tracing::info!("======= size:{} =======", size);
            btree_insert_fuzz_run(2, 1024, |_| size);
        }
    }

    #[test]
    pub fn btree_insert_fuzz_run_random() {
        btree_insert_fuzz_run(128, 16, |rng| (rng.next_u32() % 4096) as usize);
    }

    #[test]
    pub fn btree_insert_fuzz_run_small() {
        btree_insert_fuzz_run(1, 100, |rng| (rng.next_u32() % 128) as usize);
    }

    #[test]
    pub fn btree_insert_fuzz_run_big() {
        btree_insert_fuzz_run(64, 32, |rng| 3 * 1024 + (rng.next_u32() % 1024) as usize);
    }

    #[test]
    pub fn btree_insert_fuzz_run_overflow() {
        btree_insert_fuzz_run(64, 32, |rng| (rng.next_u32() % 32 * 1024) as usize);
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn setup_test_env(database_size: u32) -> (Rc<Pager>, Arc<SpinLock<DatabaseHeader>>) {
        let page_size = 512;
        let mut db_header = DatabaseHeader::default();
        db_header.page_size = page_size;
        db_header.database_size = database_size;
        let db_header = Arc::new(SpinLock::new(db_header));

        let buffer_pool = Rc::new(BufferPool::new(10));

        // Initialize buffer pool with correctly sized buffers
        for _ in 0..10 {
            let vec = vec![0; page_size as usize]; // Initialize with correct length, not just capacity
            buffer_pool.put(Pin::new(vec));
        }

        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db_file = Arc::new(DatabaseFile::new(
            io.open_file("test.db", OpenFlags::Create, false).unwrap(),
        ));

        let drop_fn = Rc::new(|_buf| {});
        let buf = Arc::new(RefCell::new(Buffer::allocate(page_size as usize, drop_fn)));
        {
            let mut buf_mut = buf.borrow_mut();
            let buf_slice = buf_mut.as_mut_slice();
            sqlite3_ondisk::write_header_to_buf(buf_slice, &db_header.lock());
        }

        let write_complete = Box::new(|_| {});
        let c = Completion::Write(WriteCompletion::new(write_complete));
        db_file.write_page(1, buf.clone(), c).unwrap();

        let wal_shared = WalFileShared::open_shared(&io, "test.wal", page_size).unwrap();
        let wal = Rc::new(RefCell::new(WalFile::new(
            io.clone(),
            page_size as usize,
            wal_shared,
            buffer_pool.clone(),
        )));

        let pager = Rc::new(
            Pager::finish_open(
                db_header.clone(),
                db_file,
                Some(wal),
                io,
                Arc::new(parking_lot::RwLock::new(DumbLruPageCache::new(10))),
                buffer_pool,
            )
            .unwrap(),
        );

        pager.io.run_once().unwrap();

        (pager, db_header)
    }

    #[test]
    #[ignore]
    pub fn test_clear_overflow_pages() -> Result<()> {
        let (pager, db_header) = setup_test_env(5);
        let mut cursor = BTreeCursor::new(None, pager.clone(), 1);

        let max_local = payload_overflow_threshold_max(PageType::TableLeaf, 4096);
        let usable_size = cursor.usable_space();

        // Create a large payload that will definitely trigger overflow
        let large_payload = vec![b'A'; max_local + usable_size];

        // Setup overflow pages (2, 3, 4) with linking
        let mut current_page = 2u32;
        while current_page <= 4 {
            let drop_fn = Rc::new(|_buf| {});
            #[allow(clippy::arc_with_non_send_sync)]
            let buf = Arc::new(RefCell::new(Buffer::allocate(
                db_header.lock().page_size as usize,
                drop_fn,
            )));
            let write_complete = Box::new(|_| {});
            let c = Completion::Write(WriteCompletion::new(write_complete));
            pager
                .db_file
                .write_page(current_page as usize, buf.clone(), c)?;
            pager.io.run_once()?;

            let page = cursor.pager.read_page(current_page as usize)?;
            while page.is_locked() {
                cursor.pager.io.run_once()?;
            }

            {
                let contents = page.get().contents.as_mut().unwrap();

                let next_page = if current_page < 4 {
                    current_page + 1
                } else {
                    0
                };
                contents.write_u32(0, next_page); // Write pointer to next overflow page

                let buf = contents.as_ptr();
                buf[4..].fill(b'A');
            }

            current_page += 1;
        }
        pager.io.run_once()?;

        // Create leaf cell pointing to start of overflow chain
        let leaf_cell = BTreeCell::TableLeafCell(TableLeafCell {
            _rowid: 1,
            _payload: unsafe { transmute::<&[u8], &'static [u8]>(large_payload.as_slice()) },
            first_overflow_page: Some(2), // Point to first overflow page
            payload_size: large_payload.len() as u64,
        });

        let initial_freelist_pages = db_header.lock().freelist_pages;
        // Clear overflow pages
        let clear_result = cursor.clear_overflow_pages(&leaf_cell)?;
        match clear_result {
            CursorResult::Ok(_) => {
                // Verify proper number of pages were added to freelist
                assert_eq!(
                    db_header.lock().freelist_pages,
                    initial_freelist_pages + 3,
                    "Expected 3 pages to be added to freelist"
                );

                // If this is first trunk page
                let trunk_page_id = db_header.lock().freelist_trunk_page;
                if trunk_page_id > 0 {
                    // Verify trunk page structure
                    let trunk_page = cursor.pager.read_page(trunk_page_id as usize)?;
                    if let Some(contents) = trunk_page.get().contents.as_ref() {
                        // Read number of leaf pages in trunk
                        let n_leaf = contents.read_u32(4);
                        assert!(n_leaf > 0, "Trunk page should have leaf entries");

                        for i in 0..n_leaf {
                            let leaf_page_id = contents.read_u32(8 + (i as usize * 4));
                            assert!(
                                (2..=4).contains(&leaf_page_id),
                                "Leaf page ID {} should be in range 2-4",
                                leaf_page_id
                            );
                        }
                    }
                }
            }
            CursorResult::IO => {
                cursor.pager.io.run_once()?;
            }
        }

        Ok(())
    }

    #[test]
    pub fn test_clear_overflow_pages_no_overflow() -> Result<()> {
        let (pager, db_header) = setup_test_env(5);
        let mut cursor = BTreeCursor::new(None, pager.clone(), 1);

        let small_payload = vec![b'A'; 10];

        // Create leaf cell with no overflow pages
        let leaf_cell = BTreeCell::TableLeafCell(TableLeafCell {
            _rowid: 1,
            _payload: unsafe { transmute::<&[u8], &'static [u8]>(small_payload.as_slice()) },
            first_overflow_page: None,
            payload_size: small_payload.len() as u64,
        });

        let initial_freelist_pages = db_header.lock().freelist_pages;

        // Try to clear non-existent overflow pages
        let clear_result = cursor.clear_overflow_pages(&leaf_cell)?;
        match clear_result {
            CursorResult::Ok(_) => {
                // Verify freelist was not modified
                assert_eq!(
                    db_header.lock().freelist_pages,
                    initial_freelist_pages,
                    "Freelist should not change when no overflow pages exist"
                );

                // Verify trunk page wasn't created
                assert_eq!(
                    db_header.lock().freelist_trunk_page,
                    0,
                    "No trunk page should be created when no overflow pages exist"
                );
            }
            CursorResult::IO => {
                cursor.pager.io.run_once()?;
            }
        }

        Ok(())
    }

    #[test]
    fn test_btree_destroy() -> Result<()> {
        let initial_size = 3;
        let (pager, db_header) = setup_test_env(initial_size);
        let mut cursor = BTreeCursor::new(None, pager.clone(), 2);
        assert_eq!(
            db_header.lock().database_size,
            initial_size,
            "Database should initially have 3 pages"
        );

        // Initialize page 2 as a root page (interior)
        let root_page = cursor.pager.read_page(2)?;
        {
            btree_init_page(&root_page, PageType::TableInterior, 0, 512); // Use proper page size
        }

        // Allocate two leaf pages
        let page3 = cursor.pager.allocate_page()?;
        btree_init_page(&page3, PageType::TableLeaf, 0, 512);

        let page4 = cursor.pager.allocate_page()?;
        btree_init_page(&page4, PageType::TableLeaf, 0, 512);

        // Configure the root page to point to the two leaf pages
        {
            let contents = root_page.get().contents.as_mut().unwrap();

            // Set rightmost pointer to page4
            contents.write_u32(offset::BTREE_RIGHTMOST_PTR, page4.get().id as u32);

            // Create a cell with pointer to page3
            let cell_content = vec![
                // First 4 bytes: left child pointer (page3)
                (page3.get().id >> 24) as u8,
                (page3.get().id >> 16) as u8,
                (page3.get().id >> 8) as u8,
                page3.get().id as u8,
                // Next byte: rowid as varint (simple value 100)
                100,
            ];

            // Insert the cell
            insert_into_cell(contents, &cell_content, 0, 512)?;
        }

        // Add a simple record to each leaf page
        for page in [&page3, &page4] {
            let contents = page.get().contents.as_mut().unwrap();

            // Simple record with just a rowid and payload
            let record_bytes = vec![
                5,                   // Payload length (varint)
                page.get().id as u8, // Rowid (varint)
                b'h',
                b'e',
                b'l',
                b'l',
                b'o', // Payload
            ];

            insert_into_cell(contents, &record_bytes, 0, 512)?;
        }

        // Verify structure before destruction
        assert_eq!(
            db_header.lock().database_size,
            5, // We should have pages 0-4
            "Database should have 4 pages total"
        );

        // Track freelist state before destruction
        let initial_free_pages = db_header.lock().freelist_pages;
        assert_eq!(initial_free_pages, 0, "should start with no free pages");

        run_until_done(|| cursor.btree_destroy(), pager.deref())?;

        let pages_freed = db_header.lock().freelist_pages - initial_free_pages;
        assert_eq!(pages_freed, 3, "should free 3 pages (root + 2 leaves)");

        Ok(())
    }

    #[test]
    pub fn test_defragment() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        for i in 0..3 {
            let record = ImmutableRecord::from_registers(&[Register::OwnedValue(
                OwnedValue::Integer(i as i64),
            )]);
            let payload = add_record(i, i, page, record, &conn);
            assert_eq!(page.cell_count(), i + 1);
            let free = compute_free_space(page, usable_space);
            total_size += payload.len() as u16 + 2;
            assert_eq!(free, 4096 - total_size - header_size);
            cells.push(Cell { pos: i, payload });
        }

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }
        cells.remove(1);
        drop_cell(page, 1, usable_space).unwrap();

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }

        defragment_page(page, usable_space);

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }
    }

    #[test]
    pub fn test_drop_odd_with_defragment() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        let total_cells = 10;
        for i in 0..total_cells {
            let record = ImmutableRecord::from_registers(&[Register::OwnedValue(
                OwnedValue::Integer(i as i64),
            )]);
            let payload = add_record(i, i, page, record, &conn);
            assert_eq!(page.cell_count(), i + 1);
            let free = compute_free_space(page, usable_space);
            total_size += payload.len() as u16 + 2;
            assert_eq!(free, 4096 - total_size - header_size);
            cells.push(Cell { pos: i, payload });
        }

        let mut removed = 0;
        let mut new_cells = Vec::new();
        for cell in cells {
            if cell.pos % 2 == 1 {
                drop_cell(page, cell.pos - removed, usable_space).unwrap();
                removed += 1;
            } else {
                new_cells.push(cell);
            }
        }
        let cells = new_cells;
        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }

        defragment_page(page, usable_space);

        for (i, cell) in cells.iter().enumerate() {
            ensure_cell(page, i, &cell.payload);
        }
    }

    #[test]
    pub fn test_fuzz_drop_defragment_insert() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        let mut i = 100000;
        let seed = thread_rng().gen();
        tracing::info!("seed {}", seed);
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        while i > 0 {
            i -= 1;
            match rng.next_u64() % 4 {
                0 => {
                    // allow appends with extra place to insert
                    let cell_idx = rng.next_u64() as usize % (page.cell_count() + 1);
                    let free = compute_free_space(page, usable_space);
                    let record = ImmutableRecord::from_registers(&[Register::OwnedValue(
                        OwnedValue::Integer(i as i64),
                    )]);
                    let mut payload: Vec<u8> = Vec::new();
                    fill_cell_payload(
                        page.page_type(),
                        Some(i as u64),
                        &mut payload,
                        &record,
                        4096,
                        conn.pager.clone(),
                    );
                    if (free as usize) < payload.len() + 2 {
                        // do not try to insert overflow pages because they require balancing
                        continue;
                    }
                    insert_into_cell(page, &payload, cell_idx, 4096).unwrap();
                    assert!(page.overflow_cells.is_empty());
                    total_size += payload.len() as u16 + 2;
                    cells.insert(cell_idx, Cell { pos: i, payload });
                }
                1 => {
                    if page.cell_count() == 0 {
                        continue;
                    }
                    let cell_idx = rng.next_u64() as usize % page.cell_count();
                    let (_, len) = page.cell_get_raw_region(
                        cell_idx,
                        payload_overflow_threshold_max(page.page_type(), 4096),
                        payload_overflow_threshold_min(page.page_type(), 4096),
                        usable_space as usize,
                    );
                    drop_cell(page, cell_idx, usable_space).unwrap();
                    total_size -= len as u16 + 2;
                    cells.remove(cell_idx);
                }
                2 => {
                    defragment_page(page, usable_space);
                }
                3 => {
                    // check cells
                    for (i, cell) in cells.iter().enumerate() {
                        ensure_cell(page, i, &cell.payload);
                    }
                    assert_eq!(page.cell_count(), cells.len());
                }
                _ => unreachable!(),
            }
            let free = compute_free_space(page, usable_space);
            assert_eq!(free, 4096 - total_size - header_size);
        }
    }

    #[test]
    pub fn test_fuzz_drop_defragment_insert_issue_1085() {
        // This test is used to demonstrate that issue at https://github.com/tursodatabase/limbo/issues/1085
        // is FIXED.
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        let mut i = 1000;
        for seed in [15292777653676891381, 9261043168681395159] {
            tracing::info!("seed {}", seed);
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            while i > 0 {
                i -= 1;
                match rng.next_u64() % 3 {
                    0 => {
                        // allow appends with extra place to insert
                        let cell_idx = rng.next_u64() as usize % (page.cell_count() + 1);
                        let free = compute_free_space(page, usable_space);
                        let record = ImmutableRecord::from_registers(&[Register::OwnedValue(
                            OwnedValue::Integer(i as i64),
                        )]);
                        let mut payload: Vec<u8> = Vec::new();
                        fill_cell_payload(
                            page.page_type(),
                            Some(i as u64),
                            &mut payload,
                            &record,
                            4096,
                            conn.pager.clone(),
                        );
                        if (free as usize) < payload.len() - 2 {
                            // do not try to insert overflow pages because they require balancing
                            continue;
                        }
                        insert_into_cell(page, &payload, cell_idx, 4096).unwrap();
                        assert!(page.overflow_cells.is_empty());
                        total_size += payload.len() as u16 + 2;
                        cells.push(Cell { pos: i, payload });
                    }
                    1 => {
                        if page.cell_count() == 0 {
                            continue;
                        }
                        let cell_idx = rng.next_u64() as usize % page.cell_count();
                        let (_, len) = page.cell_get_raw_region(
                            cell_idx,
                            payload_overflow_threshold_max(page.page_type(), 4096),
                            payload_overflow_threshold_min(page.page_type(), 4096),
                            usable_space as usize,
                        );
                        drop_cell(page, cell_idx, usable_space).unwrap();
                        total_size -= len as u16 + 2;
                        cells.remove(cell_idx);
                    }
                    2 => {
                        defragment_page(page, usable_space);
                    }
                    _ => unreachable!(),
                }
                let free = compute_free_space(page, usable_space);
                assert_eq!(free, 4096 - total_size - header_size);
            }
        }
    }

    #[test]
    pub fn test_free_space() {
        let db = get_database();
        let conn = db.connect().unwrap();
        let page = get_page(2);
        let page = page.get_contents();
        let header_size = 8;
        let usable_space = 4096;

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let payload = add_record(0, 0, page, record, &conn);
        let free = compute_free_space(page, usable_space);
        assert_eq!(free, 4096 - payload.len() as u16 - 2 - header_size);
    }

    #[test]
    pub fn test_defragment_1() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let usable_space = 4096;

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let payload = add_record(0, 0, page, record, &conn);

        assert_eq!(page.cell_count(), 1);
        defragment_page(page, usable_space);
        assert_eq!(page.cell_count(), 1);
        let (start, len) = page.cell_get_raw_region(
            0,
            payload_overflow_threshold_max(page.page_type(), 4096),
            payload_overflow_threshold_min(page.page_type(), 4096),
            usable_space as usize,
        );
        let buf = page.as_ptr();
        assert_eq!(&payload, &buf[start..start + len]);
    }

    #[test]
    pub fn test_insert_drop_insert() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let usable_space = 4096;

        let record = ImmutableRecord::from_registers(&[
            Register::OwnedValue(OwnedValue::Integer(0)),
            Register::OwnedValue(OwnedValue::Text(Text::new("aaaaaaaa"))),
        ]);
        let _ = add_record(0, 0, page, record, &conn);

        assert_eq!(page.cell_count(), 1);
        drop_cell(page, 0, usable_space).unwrap();
        assert_eq!(page.cell_count(), 0);

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let payload = add_record(0, 0, page, record, &conn);
        assert_eq!(page.cell_count(), 1);

        let (start, len) = page.cell_get_raw_region(
            0,
            payload_overflow_threshold_max(page.page_type(), 4096),
            payload_overflow_threshold_min(page.page_type(), 4096),
            usable_space as usize,
        );
        let buf = page.as_ptr();
        assert_eq!(&payload, &buf[start..start + len]);
    }

    #[test]
    pub fn test_insert_drop_insert_multiple() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let usable_space = 4096;

        let record = ImmutableRecord::from_registers(&[
            Register::OwnedValue(OwnedValue::Integer(0)),
            Register::OwnedValue(OwnedValue::Text(Text::new("aaaaaaaa"))),
        ]);
        let _ = add_record(0, 0, page, record, &conn);

        for _ in 0..100 {
            assert_eq!(page.cell_count(), 1);
            drop_cell(page, 0, usable_space).unwrap();
            assert_eq!(page.cell_count(), 0);

            let record =
                ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
            let payload = add_record(0, 0, page, record, &conn);
            assert_eq!(page.cell_count(), 1);

            let (start, len) = page.cell_get_raw_region(
                0,
                payload_overflow_threshold_max(page.page_type(), 4096),
                payload_overflow_threshold_min(page.page_type(), 4096),
                usable_space as usize,
            );
            let buf = page.as_ptr();
            assert_eq!(&payload, &buf[start..start + len]);
        }
    }

    #[test]
    pub fn test_drop_a_few_insert() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let usable_space = 4096;

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let payload = add_record(0, 0, page, record, &conn);
        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(1))]);
        let _ = add_record(1, 1, page, record, &conn);
        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(2))]);
        let _ = add_record(2, 2, page, record, &conn);

        drop_cell(page, 1, usable_space).unwrap();
        drop_cell(page, 1, usable_space).unwrap();

        ensure_cell(page, 0, &payload);
    }

    #[test]
    pub fn test_fuzz_victim_1() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get_contents();
        let usable_space = 4096;

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let _ = add_record(0, 0, page, record, &conn);

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let _ = add_record(0, 0, page, record, &conn);
        drop_cell(page, 0, usable_space).unwrap();

        defragment_page(page, usable_space);

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let _ = add_record(0, 1, page, record, &conn);

        drop_cell(page, 0, usable_space).unwrap();

        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let _ = add_record(0, 1, page, record, &conn);
    }

    #[test]
    pub fn test_fuzz_victim_2() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let usable_space = 4096;
        let insert = |pos, page| {
            let record =
                ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
            let _ = add_record(0, pos, page, record, &conn);
        };
        let drop = |pos, page| {
            drop_cell(page, pos, usable_space).unwrap();
        };
        let defragment = |page| {
            defragment_page(page, usable_space);
        };
        defragment(page.get_contents());
        defragment(page.get_contents());
        insert(0, page.get_contents());
        drop(0, page.get_contents());
        insert(0, page.get_contents());
        drop(0, page.get_contents());
        insert(0, page.get_contents());
        defragment(page.get_contents());
        defragment(page.get_contents());
        drop(0, page.get_contents());
        defragment(page.get_contents());
        insert(0, page.get_contents());
        drop(0, page.get_contents());
        insert(0, page.get_contents());
        insert(1, page.get_contents());
        insert(1, page.get_contents());
        insert(0, page.get_contents());
        drop(3, page.get_contents());
        drop(2, page.get_contents());
        compute_free_space(page.get_contents(), usable_space);
    }

    #[test]
    pub fn test_fuzz_victim_3() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let usable_space = 4096;
        let insert = |pos, page| {
            let record =
                ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
            let _ = add_record(0, pos, page, record, &conn);
        };
        let drop = |pos, page| {
            drop_cell(page, pos, usable_space).unwrap();
        };
        let defragment = |page| {
            defragment_page(page, usable_space);
        };
        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(0))]);
        let mut payload: Vec<u8> = Vec::new();
        fill_cell_payload(
            page.get_contents().page_type(),
            Some(0),
            &mut payload,
            &record,
            4096,
            conn.pager.clone(),
        );
        insert(0, page.get_contents());
        defragment(page.get_contents());
        insert(0, page.get_contents());
        defragment(page.get_contents());
        insert(0, page.get_contents());
        drop(2, page.get_contents());
        drop(0, page.get_contents());
        let free = compute_free_space(page.get_contents(), usable_space);
        let total_size = payload.len() + 2;
        assert_eq!(
            free,
            usable_space - page.get_contents().header_size() as u16 - total_size as u16
        );
        dbg!(free);
    }

    #[test]
    pub fn btree_insert_sequential() {
        let (pager, root_page) = empty_btree();
        let mut keys = Vec::new();
        for i in 0..10000 {
            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            tracing::info!("INSERT INTO t VALUES ({});", i,);
            let value =
                ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Integer(i))]);
            tracing::trace!("before insert {}", i);
            run_until_done(
                || {
                    let key = SeekKey::TableRowId(i as u64);
                    cursor.move_to(key, SeekOp::EQ)
                },
                pager.deref(),
            )
            .unwrap();
            run_until_done(
                || cursor.insert(&BTreeKey::new_table_rowid(i as u64, Some(&value)), true),
                pager.deref(),
            )
            .unwrap();
            keys.push(i);
        }
        if matches!(validate_btree(pager.clone(), root_page), (_, false)) {
            panic!("invalid btree");
        }
        tracing::trace!(
            "=========== btree ===========\n{}\n\n",
            format_btree(pager.clone(), root_page, 0)
        );
        for key in keys.iter() {
            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            let key = OwnedValue::Integer(*key);
            let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
            assert!(exists, "key not found {}", key);
        }
    }

    #[test]
    pub fn test_big_payload_compute_free() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let usable_space = 4096;
        let record =
            ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Blob(vec![
                0;
                3600
            ]))]);
        let mut payload: Vec<u8> = Vec::new();
        fill_cell_payload(
            page.get_contents().page_type(),
            Some(0),
            &mut payload,
            &record,
            4096,
            conn.pager.clone(),
        );
        insert_into_cell(page.get_contents(), &payload, 0, 4096).unwrap();
        let free = compute_free_space(page.get_contents(), usable_space);
        let total_size = payload.len() + 2;
        assert_eq!(
            free,
            usable_space - page.get_contents().header_size() as u16 - total_size as u16
        );
        dbg!(free);
    }

    #[test]
    pub fn test_delete_balancing() {
        // What does this test do:
        // 1. Insert 10,000 rows of ~15 byte payload each. This creates
        //    nearly 40 pages (10,000 * 15 / 4096) and 240 rows per page.
        // 2. Delete enough rows to create empty/ nearly empty pages to trigger balancing
        //    (verified this in SQLite).
        // 3. Verify validity/integrity of btree after deleting and also verify that these
        //    values are actually deleted.

        let (pager, root_page) = empty_btree();

        // Insert 10,000 records in to the BTree.
        for i in 1..=10000 {
            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            let value = ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Text(
                Text::new("hello world"),
            ))]);

            run_until_done(
                || {
                    let key = SeekKey::TableRowId(i as u64);
                    cursor.move_to(key, SeekOp::EQ)
                },
                pager.deref(),
            )
            .unwrap();

            run_until_done(
                || cursor.insert(&BTreeKey::new_table_rowid(i as u64, Some(&value)), true),
                pager.deref(),
            )
            .unwrap();
        }

        match validate_btree(pager.clone(), root_page) {
            (_, false) => panic!("Invalid B-tree after insertion"),
            _ => {}
        }

        // Delete records with 500 <= key <= 3500
        for i in 500..=3500 {
            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            let seek_key = SeekKey::TableRowId(i as u64);

            let found = run_until_done(|| cursor.seek(seek_key.clone(), SeekOp::EQ), pager.deref())
                .unwrap();

            if found {
                run_until_done(|| cursor.delete(), pager.deref()).unwrap();
            }
        }

        // Verify that records with key < 500 and key > 3500 still exist in the BTree.
        for i in 1..=10000 {
            if i >= 500 && i <= 3500 {
                continue;
            }

            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            let key = OwnedValue::Integer(i);
            let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
            assert!(exists, "Key {} should exist but doesn't", i);
        }

        // Verify the deleted records don't exist.
        for i in 500..=3500 {
            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            let key = OwnedValue::Integer(i);
            let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
            assert!(!exists, "Deleted key {} still exists", i);
        }
    }

    #[test]
    pub fn test_overflow_cells() {
        let iterations = 10_usize;
        let mut huge_texts = Vec::new();
        for i in 0..iterations {
            let mut huge_text = String::new();
            for _j in 0..8192 {
                huge_text.push((b'A' + i as u8) as char);
            }
            huge_texts.push(huge_text);
        }

        let (pager, root_page) = empty_btree();

        for i in 0..iterations {
            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            tracing::info!("INSERT INTO t VALUES ({});", i,);
            let value =
                ImmutableRecord::from_registers(&[Register::OwnedValue(OwnedValue::Text(Text {
                    value: huge_texts[i].as_bytes().to_vec(),
                    subtype: crate::types::TextSubtype::Text,
                }))]);
            tracing::trace!("before insert {}", i);
            tracing::debug!(
                "=========== btree before ===========\n{}\n\n",
                format_btree(pager.clone(), root_page, 0)
            );
            run_until_done(
                || {
                    let key = SeekKey::TableRowId(i as u64);
                    cursor.move_to(key, SeekOp::EQ)
                },
                pager.deref(),
            )
            .unwrap();
            run_until_done(
                || cursor.insert(&BTreeKey::new_table_rowid(i as u64, Some(&value)), true),
                pager.deref(),
            )
            .unwrap();
            tracing::debug!(
                "=========== btree after ===========\n{}\n\n",
                format_btree(pager.clone(), root_page, 0)
            );
        }
        let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
        cursor.move_to_root();
        for i in 0..iterations {
            let rowid = run_until_done(|| cursor.get_next_record(None), pager.deref()).unwrap();
            assert_eq!(rowid.unwrap(), i as u64, "got!=expected");
        }
    }

    fn run_until_done<T>(
        mut action: impl FnMut() -> Result<CursorResult<T>>,
        pager: &Pager,
    ) -> Result<T> {
        loop {
            match action()? {
                CursorResult::Ok(res) => {
                    return Ok(res);
                }
                CursorResult::IO => pager.io.run_once().unwrap(),
            }
        }
    }
}
