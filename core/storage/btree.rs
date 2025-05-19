use crate::{
    schema::Index,
    storage::{
        pager::Pager,
        sqlite3_ondisk::{
            read_u32, read_varint, BTreeCell, PageContent, PageType, TableInteriorCell,
            TableLeafCell,
        },
    },
    translate::plan::IterationDirection,
    types::IndexKeySortOrder,
    MvCursor,
};

use crate::{
    return_corrupt,
    types::{compare_immutable, CursorResult, ImmutableRecord, RefValue, SeekKey, SeekOp, Value},
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
enum DeleteSavepoint {
    Rowid(u64),
    Payload(ImmutableRecord),
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
    CheckNeedsBalancing,
    WaitForBalancingToComplete {
        target_key: DeleteSavepoint,
    },
    SeekAfterBalancing {
        target_key: DeleteSavepoint,
    },
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

enum PayloadOverflowWithOffset {
    SkipOverflowPages {
        next_page: u32,
        pages_left_to_skip: u32,
        page_offset: u32,
        amount: u32,
        buffer_offset: usize,
        is_write: bool,
    },
    ProcessPage {
        next_page: u32,
        remaining_to_read: u32,
        page: PageRef,
        current_offset: usize,
        buffer_offset: usize,
        is_write: bool,
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
    /// Old pages being balanced. We can have maximum 3 pages being balanced at the same time.
    pages_to_balance: [Option<PageRef>; 3],
    /// Bookkeeping of the rightmost pointer so the offset::BTREE_RIGHTMOST_PTR can be updated.
    rightmost_pointer: *mut u8,
    /// Divider cells of old pages. We can have maximum 2 divider cells because of 3 pages.
    divider_cells: [Option<Vec<u8>>; 2],
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
    ReadWritePayload(PayloadOverflowWithOffset),
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

/// Holds a Record or RowId, so that these can be transformed into a SeekKey to restore
/// cursor position to its previous location.
enum CursorContext {
    TableRowId(u64),

    /// If we are in an index tree we can then reuse this field to save
    /// our cursor information
    IndexKeyRowId(ImmutableRecord),
}

/// In the future, we may expand these general validity states
enum CursorValidState {
    /// Cursor is pointing a to an existing location/cell in the Btree
    Valid,
    /// Cursor may be pointing to a non-existent location/cell. This can happen after balancing operations
    RequireSeek,
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
    pub index_key_sort_order: IndexKeySortOrder,
    /// Maintain count of the number of records in the btree. Used for the `Count` opcode
    count: usize,
    /// Stores the cursor context before rebalancing so that a seek can be done later
    context: Option<CursorContext>,
    /// Store whether the Cursor is in a valid state. Meaning if it is pointing to a valid cell index or not
    valid_state: CursorValidState,
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
            index_key_sort_order: IndexKeySortOrder::default(),
            count: 0,
            context: None,
            valid_state: CursorValidState::Valid,
        }
    }

    pub fn new_index(
        mv_cursor: Option<Rc<RefCell<MvCursor>>>,
        pager: Rc<Pager>,
        root_page: usize,
        index: &Index,
    ) -> Self {
        let index_key_sort_order = IndexKeySortOrder::from_index(index);
        let mut cursor = Self::new(mv_cursor, pager, root_page);
        cursor.index_key_sort_order = index_key_sort_order;
        cursor
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
                        let order = compare_immutable(
                            record_slice_same_num_cols,
                            index_key.get_values(),
                            self.index_key_sort_order,
                        );
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
                        let order = compare_immutable(
                            record_slice_same_num_cols,
                            index_key.get_values(),
                            self.index_key_sort_order,
                        );
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

    /// Calculates how much of a cell's payload should be stored locally vs in overflow pages
    ///
    /// Parameters:
    /// - payload_len: Total length of the payload data
    /// - page_type: Type of the B-tree page (affects local storage thresholds)
    ///
    /// Returns:
    /// - A tuple of (n_local, payload_len) where:
    ///   - n_local: Amount of payload to store locally on the page
    ///   - payload_len: Total payload length (unchanged from input)
    pub fn parse_cell_info(
        &self,
        payload_len: usize,
        page_type: PageType,
        usable_size: usize,
    ) -> Result<(usize, usize)> {
        let max_local = payload_overflow_threshold_max(page_type, usable_size as u16);
        let min_local = payload_overflow_threshold_min(page_type, usable_size as u16);

        // This matches btreeParseCellAdjustSizeForOverflow logic
        let n_local = if payload_len <= max_local {
            // Common case - everything fits locally
            payload_len
        } else {
            // For payloads that need overflow pages:
            // Calculate how much should be stored locally using the following formula:
            // surplus = min_local + (payload_len - min_local) % (usable_space - 4)
            //
            // This tries to minimize unused space on overflow pages while keeping
            // the local storage between min_local and max_local thresholds.
            // The (usable_space - 4) factor accounts for overhead in overflow pages.
            let surplus = min_local + (payload_len - min_local) % (self.usable_space() - 4);
            if surplus <= max_local {
                surplus
            } else {
                min_local
            }
        };

        Ok((n_local, payload_len))
    }

    /// This function is used to read/write into the payload of a cell that
    /// cursor is pointing to.
    /// Parameters:
    /// - offset: offset in the payload to start reading/writing
    /// - buffer: buffer to read/write into
    /// - amount: amount of bytes to read/write
    /// - is_write: true if writing, false if reading
    ///
    /// If the cell has overflow pages, it will skip till the overflow page which
    /// is at the offset given.
    pub fn read_write_payload_with_offset(
        &mut self,
        mut offset: u32,
        buffer: &mut Vec<u8>,
        mut amount: u32,
        is_write: bool,
    ) -> Result<CursorResult<()>> {
        if let CursorState::ReadWritePayload(PayloadOverflowWithOffset::SkipOverflowPages {
            ..
        })
        | CursorState::ReadWritePayload(PayloadOverflowWithOffset::ProcessPage { .. }) =
            &self.state
        {
            return self.continue_payload_overflow_with_offset(buffer, self.usable_space());
        }

        let page = self.stack.top();
        return_if_locked_maybe_load!(self.pager, page);

        let contents = page.get().contents.as_ref().unwrap();
        let cell_idx = self.stack.current_cell_index() as usize - 1;

        if cell_idx >= contents.cell_count() {
            return Err(LimboError::Corrupt("Invalid cell index".into()));
        }

        let usable_size = self.usable_space();
        let cell = contents
            .cell_get(
                cell_idx,
                payload_overflow_threshold_max(contents.page_type(), usable_size as u16),
                payload_overflow_threshold_min(contents.page_type(), usable_size as u16),
                usable_size,
            )
            .unwrap();

        let (payload, payload_size, first_overflow_page) = match cell {
            BTreeCell::TableLeafCell(cell) => {
                (cell._payload, cell.payload_size, cell.first_overflow_page)
            }
            BTreeCell::IndexLeafCell(cell) => {
                (cell.payload, cell.payload_size, cell.first_overflow_page)
            }
            BTreeCell::IndexInteriorCell(cell) => {
                (cell.payload, cell.payload_size, cell.first_overflow_page)
            }
            BTreeCell::TableInteriorCell(_) => {
                return Err(LimboError::Corrupt(
                    "Cannot access payload of table interior cell".into(),
                ));
            }
        };
        assert!(offset + amount <= payload_size as u32);

        let (local_size, _) =
            self.parse_cell_info(payload_size as usize, contents.page_type(), usable_size)?;
        let mut bytes_processed: u32 = 0;
        if offset < local_size as u32 {
            let mut local_amount: u32 = amount;
            if local_amount + offset > local_size as u32 {
                local_amount = local_size as u32 - offset;
            }
            if is_write {
                self.write_payload_to_page(offset, local_amount, payload, buffer, page.clone());
            } else {
                self.read_payload_from_page(offset, local_amount, payload, buffer);
            }
            offset = 0;
            amount -= local_amount;
            bytes_processed += local_amount;
        } else {
            offset -= local_size as u32;
        }

        if amount > 0 {
            if first_overflow_page.is_none() {
                return Err(LimboError::Corrupt(
                    "Expected overflow page but none found".into(),
                ));
            }

            let overflow_size = usable_size - 4;
            let pages_to_skip = offset / overflow_size as u32;
            let page_offset = offset % overflow_size as u32;

            self.state =
                CursorState::ReadWritePayload(PayloadOverflowWithOffset::SkipOverflowPages {
                    next_page: first_overflow_page.unwrap(),
                    pages_left_to_skip: pages_to_skip,
                    page_offset: page_offset,
                    amount: amount,
                    buffer_offset: bytes_processed as usize,
                    is_write,
                });

            return Ok(CursorResult::IO);
        }
        Ok(CursorResult::Ok(()))
    }

    pub fn continue_payload_overflow_with_offset(
        &mut self,
        buffer: &mut Vec<u8>,
        usable_space: usize,
    ) -> Result<CursorResult<()>> {
        loop {
            let mut state = std::mem::replace(&mut self.state, CursorState::None);

            match &mut state {
                CursorState::ReadWritePayload(PayloadOverflowWithOffset::SkipOverflowPages {
                    next_page,
                    pages_left_to_skip,
                    page_offset,
                    amount,
                    buffer_offset,
                    is_write,
                }) => {
                    if *pages_left_to_skip == 0 {
                        let page = self.pager.read_page(*next_page as usize)?;
                        return_if_locked_maybe_load!(self.pager, page);
                        self.state =
                            CursorState::ReadWritePayload(PayloadOverflowWithOffset::ProcessPage {
                                next_page: *next_page,
                                remaining_to_read: *amount,
                                page: page,
                                current_offset: *page_offset as usize,
                                buffer_offset: *buffer_offset,
                                is_write: *is_write,
                            });

                        continue;
                    }

                    let page = self.pager.read_page(*next_page as usize)?;
                    return_if_locked_maybe_load!(self.pager, page);
                    let contents = page.get_contents();
                    let next = contents.read_u32_no_offset(0);

                    if next == 0 {
                        return Err(LimboError::Corrupt(
                            "Overflow chain ends prematurely".into(),
                        ));
                    }
                    *next_page = next;
                    *pages_left_to_skip -= 1;

                    self.state = CursorState::ReadWritePayload(
                        PayloadOverflowWithOffset::SkipOverflowPages {
                            next_page: next,
                            pages_left_to_skip: *pages_left_to_skip,
                            page_offset: *page_offset,
                            amount: *amount,
                            buffer_offset: *buffer_offset,
                            is_write: *is_write,
                        },
                    );

                    return Ok(CursorResult::IO);
                }

                CursorState::ReadWritePayload(PayloadOverflowWithOffset::ProcessPage {
                    next_page,
                    remaining_to_read,
                    page,
                    current_offset,
                    buffer_offset,
                    is_write,
                }) => {
                    if page.is_locked() {
                        self.state =
                            CursorState::ReadWritePayload(PayloadOverflowWithOffset::ProcessPage {
                                next_page: *next_page,
                                remaining_to_read: *remaining_to_read,
                                page: page.clone(),
                                current_offset: *current_offset,
                                buffer_offset: *buffer_offset,
                                is_write: *is_write,
                            });

                        return Ok(CursorResult::IO);
                    }

                    let contents = page.get_contents();
                    let overflow_size = usable_space - 4;

                    let page_offset = *current_offset;
                    let bytes_to_process = std::cmp::min(
                        *remaining_to_read,
                        overflow_size as u32 - page_offset as u32,
                    );

                    let payload_offset = 4 + page_offset;
                    let page_payload = contents.as_ptr();
                    if *is_write {
                        self.write_payload_to_page(
                            payload_offset as u32,
                            bytes_to_process,
                            page_payload,
                            buffer,
                            page.clone(),
                        );
                    } else {
                        self.read_payload_from_page(
                            payload_offset as u32,
                            bytes_to_process,
                            page_payload,
                            buffer,
                        );
                    }
                    *remaining_to_read -= bytes_to_process;
                    *buffer_offset += bytes_to_process as usize;

                    if *remaining_to_read == 0 {
                        self.state = CursorState::None;
                        return Ok(CursorResult::Ok(()));
                    }
                    let next = contents.read_u32_no_offset(0);
                    if next == 0 {
                        return Err(LimboError::Corrupt(
                            "Overflow chain ends prematurely".into(),
                        ));
                    }

                    // Load next page
                    *next_page = next;
                    *current_offset = 0; // Reset offset for new page
                    *page = self.pager.read_page(next as usize)?;

                    // Return IO to allow other operations
                    return Ok(CursorResult::IO);
                }
                _ => {
                    return Err(LimboError::InternalError(
                        "Invalid state for continue_payload_overflow_with_offset".into(),
                    ))
                }
            }
        }
    }

    fn read_payload_from_page(
        &self,
        payload_offset: u32,
        num_bytes: u32,
        payload: &[u8],
        buffer: &mut Vec<u8>,
    ) {
        buffer.extend_from_slice(
            &payload[payload_offset as usize..(payload_offset + num_bytes) as usize],
        );
    }

    /// This function write from a buffer into a page.
    /// SAFETY: This function uses unsafe in the write path to write to the page payload directly.
    /// - Make sure the page is pointing to valid data ie the page is not evicted from the page-cache.
    fn write_payload_to_page(
        &mut self,
        payload_offset: u32,
        num_bytes: u32,
        payload: &[u8],
        buffer: &mut Vec<u8>,
        page: PageRef,
    ) {
        page.set_dirty();
        self.pager.add_dirty(page.get().id);
        // SAFETY: This is safe as long as the page is not evicted from the cache.
        let payload_mut =
            unsafe { std::slice::from_raw_parts_mut(payload.as_ptr() as *mut u8, payload.len()) };
        payload_mut[payload_offset as usize..payload_offset as usize + num_bytes as usize]
            .copy_from_slice(&buffer[..num_bytes as usize]);
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
            let cell_count = contents.cell_count();

            if cell_count == 0 || cell_idx == cell_count {
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
                    let order = {
                        let record = self.get_immutable_record();
                        let record = record.as_ref().unwrap();
                        let record_slice_same_num_cols =
                            &record.get_values()[..index_key.get_values().len()];
                        let order = compare_immutable(
                            record_slice_same_num_cols,
                            index_key.get_values(),
                            self.index_key_sort_order,
                        );
                        order
                    };
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
                    let order = {
                        let record = self.get_immutable_record();
                        let record = record.as_ref().unwrap();
                        let record_slice_same_num_cols =
                            &record.get_values()[..index_key.get_values().len()];
                        let order = compare_immutable(
                            record_slice_same_num_cols,
                            index_key.get_values(),
                            self.index_key_sort_order,
                        );
                        order
                    };
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
        match key {
            SeekKey::TableRowId(rowid) => {
                return self.tablebtree_seek(rowid, op);
            }
            SeekKey::IndexKey(index_key) => {
                return self.indexbtree_seek(index_key, op);
            }
        }
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

    /// Specialized version of move_to() for table btrees.
    fn tablebtree_move_to(&mut self, rowid: u64, seek_op: SeekOp) -> Result<CursorResult<()>> {
        let iter_dir = seek_op.iteration_direction();
        'outer: loop {
            let page = self.stack.top();
            return_if_locked!(page);
            let contents = page.get().contents.as_ref().unwrap();
            if contents.is_leaf() {
                return Ok(CursorResult::Ok(()));
            }

            let cell_count = contents.cell_count();
            let mut min: isize = 0;
            let mut max: isize = cell_count as isize - 1;
            let mut leftmost_matching_cell = None;
            loop {
                if min > max {
                    if let Some(leftmost_matching_cell) = leftmost_matching_cell {
                        let left_child_page = contents.cell_table_interior_read_left_child_page(
                            leftmost_matching_cell as usize,
                        )?;
                        // If we found our target rowid in the left subtree,
                        // we need to move the parent cell pointer forwards or backwards depending on the iteration direction.
                        // For example: since the internal node contains the max rowid of the left subtree, we need to move the
                        // parent pointer backwards in backwards iteration so that we don't come back to the parent again.
                        // E.g.
                        // this parent: rowid 666
                        // left child has: 664,665,666
                        // we need to move to the previous parent (with e.g. rowid 663) when iterating backwards.
                        let index_change =
                            -1 + (iter_dir == IterationDirection::Forwards) as i32 * 2;
                        self.stack
                            .set_cell_index(leftmost_matching_cell as i32 + index_change);
                        let mem_page = self.pager.read_page(left_child_page as usize)?;
                        self.stack.push(mem_page);
                        continue 'outer;
                    }
                    self.stack.set_cell_index(cell_count as i32 + 1);
                    match contents.rightmost_pointer() {
                        Some(right_most_pointer) => {
                            let mem_page = self.pager.read_page(right_most_pointer as usize)?;
                            self.stack.push(mem_page);
                            continue 'outer;
                        }
                        None => {
                            unreachable!("we shall not go back up! The only way is down the slope");
                        }
                    }
                }
                let cur_cell_idx = (min + max) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
                let cell_rowid = contents.cell_table_interior_read_rowid(cur_cell_idx as usize)?;
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
                let is_on_left = match seek_op {
                    SeekOp::GT => cell_rowid > rowid,
                    SeekOp::GE => cell_rowid >= rowid,
                    SeekOp::LE => cell_rowid >= rowid,
                    SeekOp::LT => cell_rowid + 1 >= rowid,
                    SeekOp::EQ => cell_rowid >= rowid,
                };
                if is_on_left {
                    leftmost_matching_cell = Some(cur_cell_idx as usize);
                    max = cur_cell_idx - 1;
                } else {
                    min = cur_cell_idx + 1;
                }
            }
        }
    }

    /// Specialized version of move_to() for index btrees.
    fn indexbtree_move_to(
        &mut self,
        index_key: &ImmutableRecord,
        cmp: SeekOp,
    ) -> Result<CursorResult<()>> {
        let iter_dir = cmp.iteration_direction();
        'outer: loop {
            let page = self.stack.top();
            return_if_locked!(page);
            let contents = page.get().contents.as_ref().unwrap();
            if contents.is_leaf() {
                return Ok(CursorResult::Ok(()));
            }

            let cell_count = contents.cell_count();
            let mut min: isize = 0;
            let mut max: isize = cell_count as isize - 1;
            let mut leftmost_matching_cell = None;
            loop {
                if min > max {
                    let Some(leftmost_matching_cell) = leftmost_matching_cell else {
                        self.stack.set_cell_index(contents.cell_count() as i32 + 1);
                        match contents.rightmost_pointer() {
                            Some(right_most_pointer) => {
                                let mem_page = self.pager.read_page(right_most_pointer as usize)?;
                                self.stack.push(mem_page);
                                continue 'outer;
                            }
                            None => {
                                unreachable!(
                                    "we shall not go back up! The only way is down the slope"
                                );
                            }
                        }
                    };
                    let matching_cell = contents.cell_get(
                        leftmost_matching_cell,
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
                    self.stack.set_cell_index(leftmost_matching_cell as i32);
                    // we don't advance in case of forward iteration and index tree internal nodes because we will visit this node going up.
                    // in backwards iteration, we must retreat because otherwise we would unnecessarily visit this node again.
                    // Example:
                    // this parent: key 666, and we found the target key in the left child.
                    // left child has: key 663, key 664, key 665
                    // we need to move to the previous parent (with e.g. key 662) when iterating backwards so that we don't end up back here again.
                    if iter_dir == IterationDirection::Backwards {
                        self.stack.retreat();
                    }
                    let BTreeCell::IndexInteriorCell(IndexInteriorCell {
                        left_child_page, ..
                    }) = &matching_cell
                    else {
                        unreachable!("unexpected cell type: {:?}", matching_cell);
                    };

                    let mem_page = self.pager.read_page(*left_child_page as usize)?;
                    self.stack.push(mem_page);
                    continue 'outer;
                }

                let cur_cell_idx = (min + max) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
                self.stack.set_cell_index(cur_cell_idx as i32);
                let cell = contents.cell_get(
                    cur_cell_idx as usize,
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
                let BTreeCell::IndexInteriorCell(IndexInteriorCell {
                    payload,
                    payload_size,
                    first_overflow_page,
                    ..
                }) = &cell
                else {
                    unreachable!("unexpected cell type: {:?}", cell);
                };

                if let Some(next_page) = first_overflow_page {
                    return_if_io!(self.process_overflow_read(payload, *next_page, *payload_size))
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
                    self.index_key_sort_order,
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
                    leftmost_matching_cell = Some(cur_cell_idx as usize);
                    max = cur_cell_idx - 1;
                } else {
                    min = cur_cell_idx + 1;
                }
            }
        }
    }

    /// Specialized version of do_seek() for table btrees that uses binary search instead
    /// of iterating cells in order.
    fn tablebtree_seek(
        &mut self,
        rowid: u64,
        seek_op: SeekOp,
    ) -> Result<CursorResult<Option<u64>>> {
        assert!(self.mv_cursor.is_none());
        self.move_to_root();
        return_if_io!(self.tablebtree_move_to(rowid, seek_op));
        let page = self.stack.top();
        return_if_locked!(page);
        let contents = page.get().contents.as_ref().unwrap();
        assert!(
            contents.is_leaf(),
            "tablebtree_seek() called on non-leaf page"
        );
        let iter_dir = seek_op.iteration_direction();

        let cell_count = contents.cell_count();
        let mut min: isize = 0;
        let mut max: isize = cell_count as isize - 1;

        // If iter dir is forwards, we want the first cell that matches;
        // If iter dir is backwards, we want the last cell that matches.
        let mut nearest_matching_cell = None;
        loop {
            if min > max {
                let Some(nearest_matching_cell) = nearest_matching_cell else {
                    return Ok(CursorResult::Ok(None));
                };
                let matching_cell = contents.cell_get(
                    nearest_matching_cell,
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
                let BTreeCell::TableLeafCell(TableLeafCell {
                    _rowid: cell_rowid,
                    _payload,
                    first_overflow_page,
                    payload_size,
                    ..
                }) = matching_cell
                else {
                    unreachable!("unexpected cell type: {:?}", matching_cell);
                };

                return_if_io!(self.read_record_w_possible_overflow(
                    _payload,
                    first_overflow_page,
                    payload_size
                ));
                let cell_idx = if iter_dir == IterationDirection::Forwards {
                    nearest_matching_cell as i32 + 1
                } else {
                    nearest_matching_cell as i32 - 1
                };
                self.stack.set_cell_index(cell_idx as i32);
                return Ok(CursorResult::Ok(Some(cell_rowid)));
            }

            let cur_cell_idx = (min + max) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
            let cell_rowid = contents.cell_table_leaf_read_rowid(cur_cell_idx as usize)?;

            let cmp = cell_rowid.cmp(&rowid);

            let found = match seek_op {
                SeekOp::GT => cmp.is_gt(),
                SeekOp::GE => cmp.is_ge(),
                SeekOp::EQ => cmp.is_eq(),
                SeekOp::LE => cmp.is_le(),
                SeekOp::LT => cmp.is_lt(),
            };

            // rowids are unique, so we can return the rowid immediately
            if found && SeekOp::EQ == seek_op {
                let cur_cell = contents.cell_get(
                    cur_cell_idx as usize,
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
                let BTreeCell::TableLeafCell(TableLeafCell {
                    _rowid: _,
                    _payload,
                    first_overflow_page,
                    payload_size,
                    ..
                }) = cur_cell
                else {
                    unreachable!("unexpected cell type: {:?}", cur_cell);
                };
                return_if_io!(self.read_record_w_possible_overflow(
                    _payload,
                    first_overflow_page,
                    payload_size
                ));
                let cell_idx = if iter_dir == IterationDirection::Forwards {
                    cur_cell_idx + 1
                } else {
                    cur_cell_idx - 1
                };
                self.stack.set_cell_index(cell_idx as i32);
                return Ok(CursorResult::Ok(Some(cell_rowid)));
            }

            if found {
                match iter_dir {
                    IterationDirection::Forwards => {
                        nearest_matching_cell = Some(cur_cell_idx as usize);
                        max = cur_cell_idx - 1;
                    }
                    IterationDirection::Backwards => {
                        nearest_matching_cell = Some(cur_cell_idx as usize);
                        min = cur_cell_idx + 1;
                    }
                }
            } else {
                if cmp.is_gt() {
                    max = cur_cell_idx - 1;
                } else if cmp.is_lt() {
                    min = cur_cell_idx + 1;
                } else {
                    match iter_dir {
                        IterationDirection::Forwards => {
                            min = cur_cell_idx + 1;
                        }
                        IterationDirection::Backwards => {
                            max = cur_cell_idx - 1;
                        }
                    }
                }
            }
        }
    }

    fn indexbtree_seek(
        &mut self,
        key: &ImmutableRecord,
        seek_op: SeekOp,
    ) -> Result<CursorResult<Option<u64>>> {
        self.move_to_root();
        return_if_io!(self.indexbtree_move_to(key, seek_op));

        let page = self.stack.top();
        return_if_locked!(page);

        let contents = page.get().contents.as_ref().unwrap();

        let cell_count = contents.cell_count();
        let mut min: isize = 0;
        let mut max: isize = cell_count as isize - 1;

        let iter_dir = seek_op.iteration_direction();

        // If iter dir is forwards, we want the first cell that matches;
        // If iter dir is backwards, we want the last cell that matches.
        let mut nearest_matching_cell = None;
        loop {
            if min > max {
                let Some(nearest_matching_cell) = nearest_matching_cell else {
                    // We have now iterated over all cells in the leaf page and found no match.
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
                    match seek_op.iteration_direction() {
                        IterationDirection::Forwards => {
                            self.stack.set_cell_index(cell_count as i32);
                            return self.get_next_record(Some((SeekKey::IndexKey(key), seek_op)));
                        }
                        IterationDirection::Backwards => {
                            self.stack.set_cell_index(-1);
                            return self.get_prev_record(Some((SeekKey::IndexKey(key), seek_op)));
                        }
                    }
                };
                let cell = contents.cell_get(
                    nearest_matching_cell as usize,
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

                let BTreeCell::IndexLeafCell(IndexLeafCell {
                    payload,
                    first_overflow_page,
                    payload_size,
                }) = &cell
                else {
                    unreachable!("unexpected cell type: {:?}", cell);
                };

                if let Some(next_page) = first_overflow_page {
                    return_if_io!(self.process_overflow_read(payload, *next_page, *payload_size))
                } else {
                    crate::storage::sqlite3_ondisk::read_record(
                        payload,
                        self.get_immutable_record_or_create().as_mut().unwrap(),
                    )?
                }
                let record = self.get_immutable_record();
                let record = record.as_ref().unwrap();
                let rowid = match record.last_value() {
                    Some(RefValue::Integer(rowid)) => *rowid as u64,
                    _ => unreachable!("index cells should have an integer rowid"),
                };
                self.stack.set_cell_index(nearest_matching_cell as i32);
                self.stack.next_cell_in_direction(iter_dir);
                return Ok(CursorResult::Ok(Some(rowid)));
            }

            let cur_cell_idx = (min + max) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
            self.stack.set_cell_index(cur_cell_idx as i32);

            let cell = contents.cell_get(
                cur_cell_idx as usize,
                payload_overflow_threshold_max(contents.page_type(), self.usable_space() as u16),
                payload_overflow_threshold_min(contents.page_type(), self.usable_space() as u16),
                self.usable_space(),
            )?;
            let BTreeCell::IndexLeafCell(IndexLeafCell {
                payload,
                first_overflow_page,
                payload_size,
            }) = &cell
            else {
                unreachable!("unexpected cell type: {:?}", cell);
            };

            if let Some(next_page) = first_overflow_page {
                return_if_io!(self.process_overflow_read(payload, *next_page, *payload_size))
            } else {
                crate::storage::sqlite3_ondisk::read_record(
                    payload,
                    self.get_immutable_record_or_create().as_mut().unwrap(),
                )?
            };
            let record = self.get_immutable_record();
            let record = record.as_ref().unwrap();
            let record_slice_equal_number_of_cols =
                &record.get_values().as_slice()[..key.get_values().len()];
            let cmp = compare_immutable(
                record_slice_equal_number_of_cols,
                key.get_values(),
                self.index_key_sort_order,
            );
            let found = match seek_op {
                SeekOp::GT => cmp.is_gt(),
                SeekOp::GE => cmp.is_ge(),
                SeekOp::EQ => cmp.is_eq(),
                SeekOp::LE => cmp.is_le(),
                SeekOp::LT => cmp.is_lt(),
            };
            if found {
                match iter_dir {
                    IterationDirection::Forwards => {
                        nearest_matching_cell = Some(cur_cell_idx as usize);
                        max = cur_cell_idx - 1;
                    }
                    IterationDirection::Backwards => {
                        nearest_matching_cell = Some(cur_cell_idx as usize);
                        min = cur_cell_idx + 1;
                    }
                }
            } else {
                if cmp.is_gt() {
                    max = cur_cell_idx - 1;
                } else if cmp.is_lt() {
                    min = cur_cell_idx + 1;
                } else {
                    match iter_dir {
                        IterationDirection::Forwards => {
                            min = cur_cell_idx + 1;
                        }
                        IterationDirection::Backwards => {
                            max = cur_cell_idx - 1;
                        }
                    }
                }
            }
        }
    }

    fn read_record_w_possible_overflow(
        &mut self,
        payload: &'static [u8],
        next_page: Option<u32>,
        payload_size: u64,
    ) -> Result<CursorResult<()>> {
        if let Some(next_page) = next_page {
            self.process_overflow_read(payload, next_page, payload_size)
        } else {
            crate::storage::sqlite3_ondisk::read_record(
                payload,
                self.get_immutable_record_or_create().as_mut().unwrap(),
            )?;
            Ok(CursorResult::Ok(()))
        }
    }

    pub fn move_to(&mut self, key: SeekKey<'_>, cmp: SeekOp) -> Result<CursorResult<()>> {
        assert!(self.mv_cursor.is_none());
        tracing::trace!("move_to(key={:?} cmp={:?})", key, cmp);
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

        match key {
            SeekKey::TableRowId(rowid_key) => {
                return self.tablebtree_move_to(rowid_key, cmp);
            }
            SeekKey::IndexKey(index_key) => {
                return self.indexbtree_move_to(index_key, cmp);
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
                                    .get_values(),
                        self.index_key_sort_order,
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
                        // don't continue if:
                        // - current page is not overfull root
                        // OR
                        // - current page is not overfull and the amount of free space on the page
                        // is less than 2/3rds of the total usable space on the page
                        //
                        // https://github.com/sqlite/sqlite/blob/0aa95099f5003dc99f599ab77ac0004950b281ef/src/btree.c#L9064-L9071
                        let page = current_page.get().contents.as_mut().unwrap();
                        let usable_space = self.usable_space();
                        let free_space = compute_free_space(page, usable_space as u16);
                        if page.overflow_cells.is_empty()
                            && (!self.stack.has_parent()
                                || free_space as usize * 3 <= usable_space * 2)
                        {
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
                let mut pages_to_balance: [Option<PageRef>; 3] = [const { None }; 3];
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
                    #[cfg(debug_assertions)]
                    {
                        return_if_locked!(page);
                        debug_validate_cells!(&page.get_contents(), self.usable_space() as u16);
                    }
                    pages_to_balance[i].replace(page);
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

                #[cfg(debug_assertions)]
                {
                    let page_type_of_siblings = pages_to_balance[0]
                        .as_ref()
                        .unwrap()
                        .get_contents()
                        .page_type();
                    for page in pages_to_balance.iter().take(sibling_count) {
                        let contents = page.as_ref().unwrap().get_contents();
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
                        divider_cells: [const { None }; 2],
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
                    .take(balance_info.sibling_count)
                    .all(|page| !page.as_ref().unwrap().is_locked());
                if !all_loaded {
                    return Ok(CursorResult::IO);
                }
                // Now do real balancing
                let parent_page = self.stack.top();
                let parent_contents = parent_page.get_contents();
                let parent_is_root = !self.stack.has_parent();

                assert!(
                    parent_contents.overflow_cells.is_empty(),
                    "overflow parent not yet implemented"
                );

                /* 1. Get divider cells and max_cells */
                let mut max_cells = 0;
                // we only need maximum 5 pages to balance 3 pages
                let mut pages_to_balance_new: [Option<PageRef>; 5] = [const { None }; 5];
                for i in (0..balance_info.sibling_count).rev() {
                    let sibling_page = balance_info.pages_to_balance[i].as_ref().unwrap();
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
                    balance_info.divider_cells[i].replace(cell_buf.to_vec());
                    tracing::trace!(
                        "dropping divider cell from parent cell_idx={} count={}",
                        cell_idx,
                        parent_contents.cell_count()
                    );
                    drop_cell(parent_contents, cell_idx, self.usable_space() as u16)?;
                }

                /* 2. Initialize CellArray with all the cells used for distribution, this includes divider cells if !leaf. */
                let mut cell_array = CellArray {
                    cells: Vec::with_capacity(max_cells),
                    number_of_cells_per_page: [0; 5],
                };
                let cells_capacity_start = cell_array.cells.capacity();

                let mut total_cells_inserted = 0;
                // count_cells_in_old_pages is the prefix sum of cells of each page
                let mut count_cells_in_old_pages: [u16; 5] = [0; 5];

                let page_type = balance_info.pages_to_balance[0]
                    .as_ref()
                    .unwrap()
                    .get_contents()
                    .page_type();
                tracing::debug!("balance_non_root(page_type={:?})", page_type);
                let leaf_data = matches!(page_type, PageType::TableLeaf);
                let leaf = matches!(page_type, PageType::TableLeaf | PageType::IndexLeaf);
                for (i, old_page) in balance_info
                    .pages_to_balance
                    .iter()
                    .take(balance_info.sibling_count)
                    .enumerate()
                {
                    let old_page_contents = old_page.as_ref().unwrap().get_contents();
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

                    count_cells_in_old_pages[i] = cell_array.cells.len() as u16;

                    let mut cells_inserted =
                        old_page_contents.cell_count() + old_page_contents.overflow_cells.len();

                    if i < balance_info.sibling_count - 1 && !leaf_data {
                        // If we are a index page or a interior table page we need to take the divider cell too.
                        // But we don't need the last divider as it will remain the same.
                        let mut divider_cell = balance_info.divider_cells[i]
                            .as_mut()
                            .unwrap()
                            .as_mut_slice();
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
                let mut new_page_sizes: [i64; 5] = [0; 5];
                let leaf_correction = if leaf { 4 } else { 0 };
                // number of bytes beyond header, different from global usableSapce which includes
                // header
                let usable_space = self.usable_space() - 12 + leaf_correction;
                for i in 0..balance_info.sibling_count {
                    cell_array.number_of_cells_per_page[i] = count_cells_in_old_pages[i];
                    let page = &balance_info.pages_to_balance[i].as_ref().unwrap();
                    let page_contents = page.get_contents();
                    let free_space = compute_free_space(page_contents, self.usable_space() as u16);

                    new_page_sizes[i] = usable_space as i64 - free_space as i64;
                    for overflow in &page_contents.overflow_cells {
                        // 2 to account of pointer
                        new_page_sizes[i] += 2 + overflow.payload.len() as i64;
                    }
                    if !leaf && i < balance_info.sibling_count - 1 {
                        // Account for divider cell which is included in this page.
                        new_page_sizes[i] +=
                            cell_array.cells[cell_array.cell_count(i)].len() as i64;
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
                            sibling_count_new = i + 2;
                            assert!(
                                sibling_count_new <= 5,
                                "it is corrupt to require more than 5 pages to balance 3 siblings"
                            );

                            new_page_sizes[sibling_count_new - 1] = 0;
                            cell_array.number_of_cells_per_page[sibling_count_new - 1] =
                                cell_array.cells.len() as u16;
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
                        let page = balance_info.pages_to_balance[i].as_ref().unwrap();
                        page.set_dirty();
                        pages_to_balance_new[i].replace(page.clone());
                    } else {
                        let page = self.pager.do_allocate_page(page_type, 0);
                        pages_to_balance_new[i].replace(page);
                        // Since this page didn't exist before, we can set it to cells length as it
                        // marks them as empty since it is a prefix sum of cells.
                        count_cells_in_old_pages[i] = cell_array.cells.len() as u16;
                    }
                }

                // Reassign page numbers in increasing order
                {
                    let mut page_numbers: [usize; 5] = [0; 5];
                    for (i, page) in pages_to_balance_new
                        .iter()
                        .take(sibling_count_new)
                        .enumerate()
                    {
                        page_numbers[i] = page.as_ref().unwrap().get().id;
                    }
                    page_numbers.sort();
                    for (page, new_id) in pages_to_balance_new
                        .iter()
                        .take(sibling_count_new)
                        .rev()
                        .zip(page_numbers.iter().rev().take(sibling_count_new))
                    {
                        let page = page.as_ref().unwrap();
                        if *new_id != page.get().id {
                            page.get().id = *new_id;
                            self.pager.put_loaded_page(*new_id, page.clone());
                        }
                    }

                    #[cfg(debug_assertions)]
                    {
                        tracing::debug!(
                            "balance_non_root(parent page_id={})",
                            parent_page.get().id
                        );
                        for page in pages_to_balance_new.iter().take(sibling_count_new) {
                            tracing::debug!(
                                "balance_non_root(new_sibling page_id={})",
                                page.as_ref().unwrap().get().id
                            );
                        }
                    }
                }

                // pages_pointed_to helps us debug we did in fact create divider cells to all the new pages and the rightmost pointer,
                // also points to the last page.
                #[cfg(debug_assertions)]
                let mut pages_pointed_to = HashSet::new();

                // Write right pointer in parent page to point to new rightmost page. keep in mind
                // we update rightmost pointer first because inserting cells could defragment parent page,
                // therfore invalidating the pointer.
                let right_page_id = pages_to_balance_new[sibling_count_new - 1]
                    .as_ref()
                    .unwrap()
                    .get()
                    .id as u32;
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
                    let last_page = balance_info.pages_to_balance[balance_info.sibling_count - 1]
                        .as_ref()
                        .unwrap();
                    let right_pointer = last_page.get_contents().rightmost_pointer().unwrap();
                    let new_last_page = pages_to_balance_new[sibling_count_new - 1]
                        .as_ref()
                        .unwrap();
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
                    let page = page.as_ref().unwrap();
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
                    for page in pages_to_balance_new.iter().take(sibling_count_new) {
                        let page = page.as_ref().unwrap();
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
                let mut done = [false; 5];
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
                        let page = pages_to_balance_new[page_idx].as_ref().unwrap();
                        tracing::debug!("pre_edit_page(page={})", page.get().id);
                        let page_contents = page.get_contents();
                        edit_page(
                            page_contents,
                            start_old_cells,
                            start_new_cells,
                            number_new_cells,
                            &cell_array,
                            self.usable_space() as u16,
                        )?;
                        debug_validate_cells!(page_contents, self.usable_space() as u16);
                        tracing::trace!(
                            "edit_page page={} cells={}",
                            page.get().id,
                            page_contents.cell_count()
                        );
                        page_contents.overflow_cells.clear();

                        done[page_idx] = true;
                    }
                }

                // TODO: vacuum support
                let first_child_page = pages_to_balance_new[0].as_ref().unwrap();
                let first_child_contents = first_child_page.get_contents();
                if parent_is_root
                    && parent_contents.cell_count() == 0

                    // this check to make sure we are not having negative free space
                    && parent_contents.offset
                        <= compute_free_space(first_child_contents, self.usable_space() as u16)
                            as usize
                {
                    // From SQLite:
                    // The root page of the b-tree now contains no cells. The only sibling
                    // page is the right-child of the parent. Copy the contents of the
                    // child page into the parent, decreasing the overall height of the
                    // b-tree structure by one. This is described as the "balance-shallower"
                    // sub-algorithm in some documentation.
                    assert!(sibling_count_new == 1);
                    let parent_offset = if parent_page.get().id == 1 {
                        DATABASE_HEADER_SIZE
                    } else {
                        0
                    };

                    // From SQLite:
                    // It is critical that the child page be defragmented before being
                    // copied into the parent, because if the parent is page 1 then it will
                    // by smaller than the child due to the database header, and so
                    // all the free space needs to be up front.
                    defragment_page(first_child_contents, self.usable_space() as u16);

                    let child_top = first_child_contents.cell_content_area() as usize;
                    let parent_buf = parent_contents.as_ptr();
                    let child_buf = first_child_contents.as_ptr();
                    let content_size = self.usable_space() - child_top;

                    // Copy cell contents
                    parent_buf[child_top..child_top + content_size]
                        .copy_from_slice(&child_buf[child_top..child_top + content_size]);

                    // Copy header and pointer
                    // NOTE: don't use .cell_pointer_array_offset_and_size() because of different
                    // header size
                    let header_and_pointer_size = first_child_contents.header_size()
                        + first_child_contents.cell_pointer_array_size();
                    parent_buf[parent_offset..parent_offset + header_and_pointer_size]
                        .copy_from_slice(
                            &child_buf[first_child_contents.offset
                                ..first_child_contents.offset + header_and_pointer_size],
                        );

                    self.stack.set_cell_index(0); // reset cell index, top is already parent
                    sibling_count_new -= 1; // decrease sibling count for debugging and free at the end
                    assert!(sibling_count_new < balance_info.sibling_count);
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

                // We have to free pages that are not used anymore
                for i in sibling_count_new..balance_info.sibling_count {
                    let page = balance_info.pages_to_balance[i].as_ref().unwrap();
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
        pages_to_balance_new: [Option<std::sync::Arc<crate::Page>>; 5],
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
        for (page_idx, page) in pages_to_balance_new
            .iter()
            .take(sibling_count_new)
            .enumerate()
        {
            let page = page.as_ref().unwrap();
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
            if sibling_count_new == 0 {
                // Balance-shallower case
                // We need to check data in parent page
                let rightmost = read_u32(rightmost_pointer, 0);
                debug_validate_cells!(parent_contents, self.usable_space() as u16);

                if !pages_to_balance_new[0].is_some() {
                    tracing::error!(
                        "balance_non_root(balance_shallower_incorrect_page, page_idx={})",
                        0
                    );
                    valid = false;
                }

                for i in 1..sibling_count_new {
                    if pages_to_balance_new[i].is_some() {
                        tracing::error!(
                            "balance_non_root(balance_shallower_incorrect_page, page_idx={})",
                            i
                        );
                        valid = false;
                    }
                }

                if current_index_cell != cells_debug.len()
                    || cells_debug.len() != contents.cell_count()
                    || contents.cell_count() != parent_contents.cell_count()
                {
                    tracing::error!("balance_non_root(balance_shallower_incorrect_cell_count, current_index_cell={}, cells_debug={}, cell_count={}, parent_cell_count={})",
                        current_index_cell,
                        cells_debug.len(),
                        contents.cell_count(),
                        parent_contents.cell_count()
                    );
                    valid = false;
                }

                if rightmost == page.get().id as u32 || rightmost == parent_page.get().id as u32 {
                    tracing::error!("balance_non_root(balance_shallower_rightmost_pointer, page_id={}, parent_page_id={}, rightmost={})",
                        page.get().id,
                        parent_page.get().id,
                        rightmost,
                    );
                    valid = false;
                }

                if let Some(rm) = contents.rightmost_pointer() {
                    if rm != rightmost {
                        tracing::error!("balance_non_root(balance_shallower_rightmost_pointer, page_rightmost={}, rightmost={})",
                            rm,
                            rightmost,
                        );
                        valid = false;
                    }
                }

                if let Some(rm) = parent_contents.rightmost_pointer() {
                    if rm != rightmost {
                        tracing::error!("balance_non_root(balance_shallower_rightmost_pointer, parent_rightmost={}, rightmost={})",
                            rm,
                            rightmost,
                        );
                        valid = false;
                    }
                }

                if parent_contents.page_type() != page_type {
                    tracing::error!("balance_non_root(balance_shallower_parent_page_type, page_type={:?}, parent_page_type={:?})",
                        page_type,
                        parent_contents.page_type()
                    );
                    valid = false
                }

                for parent_cell_idx in 0..contents.cell_count() {
                    let (parent_cell_start, parent_cell_len) = parent_contents.cell_get_raw_region(
                        parent_cell_idx,
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

                    let (cell_start, cell_len) = contents.cell_get_raw_region(
                        parent_cell_idx,
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
                    let parent_cell_buf = to_static_buf(
                        &mut parent_buf[parent_cell_start..parent_cell_start + parent_cell_len],
                    );
                    let cell_buf_in_array = &cells_debug[parent_cell_idx];

                    if cell_buf != cell_buf_in_array || cell_buf != parent_cell_buf {
                        tracing::error!("balance_non_root(balance_shallower_cell_not_found_debug, page_id={}, cell_in_cell_array_idx={})",
                            page.get().id,
                            parent_cell_idx,
                        );
                        valid = false;
                    }
                }
            } else if page_idx == sibling_count_new - 1 {
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
                    if !leaf_data {
                        // remember to increase cell if this cell was moved to parent
                        current_index_cell += 1;
                    }
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
                        if !leaf_data {
                            // remember to increase cell if this cell was moved to parent
                            current_index_cell += 1;
                        }
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
                        self.index_key_sort_order,
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
        let _ = self.restore_context()?;
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
        // We need to clear the null flag for the table cursor before seeking,
        // because it might have been set to false by an unmatched left-join row during the previous iteration
        // on the outer loop.
        self.set_null_flag(false);
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
    /// 4. ClearOverflowPages -> Clear the overflow pages if there are any before dropping the cell, then if we are in a leaf page we just drop the cell in place.
    /// if we are in interior page, we need to rotate keys in order to replace current cell (InteriorNodeReplacement).
    /// 5. InteriorNodeReplacement -> we copy the left subtree leaf node into the deleted interior node's place.
    /// 6. WaitForBalancingToComplete -> perform balancing
    /// 7. SeekAfterBalancing -> adjust the cursor to a node that is closer to the deleted value. go to Finish
    /// 8. Finish -> Delete operation is done. Return CursorResult(Ok())
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
            tracing::debug!("delete state: {:?}", delete_state);

            match delete_state {
                DeleteState::Start => {
                    let page = self.stack.top();
                    page.set_dirty();
                    self.pager.add_dirty(page.get().id);
                    if matches!(
                        page.get_contents().page_type(),
                        PageType::TableLeaf | PageType::TableInterior
                    ) {
                        let _target_rowid = match self.rowid.get() {
                            Some(rowid) => rowid,
                            None => {
                                self.state = CursorState::None;
                                return Ok(CursorResult::Ok(()));
                            }
                        };
                    } else {
                        if self.reusable_immutable_record.borrow().is_none() {
                            self.state = CursorState::None;
                            return Ok(CursorResult::Ok(()));
                        }
                    }

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
                    if cell_idx > 0 {
                        cell_idx -= 1;
                    }

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
                        BTreeCell::IndexInteriorCell(interior) => Some(interior.left_child_page),
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
                        let contents = page.get().contents.as_mut().unwrap();
                        drop_cell(contents, cell_idx, self.usable_space() as u16)?;

                        let delete_info = self.state.mut_delete_info().unwrap();
                        delete_info.state = DeleteState::CheckNeedsBalancing;
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
                    return_if_locked_maybe_load!(self.pager, leaf_page);
                    assert!(
                        matches!(
                            leaf_page.get_contents().page_type(),
                            PageType::TableLeaf | PageType::IndexLeaf
                        ),
                        "self.prev should have returned a leaf page"
                    );

                    let parent_page = self.stack.parent_page().unwrap();
                    assert!(parent_page.is_loaded(), "parent page");

                    let leaf_contents = leaf_page.get().contents.as_ref().unwrap();
                    // The index of the cell to removed must be the last one.
                    let leaf_cell_idx = leaf_contents.cell_count() - 1;
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

                    // Create an interior cell from a predecessor
                    let mut cell_payload: Vec<u8> = Vec::new();
                    let child_pointer = original_child_pointer.expect("there should be a pointer");
                    match predecessor_cell {
                        BTreeCell::TableLeafCell(leaf_cell) => {
                            cell_payload.extend_from_slice(&child_pointer.to_be_bytes());
                            write_varint_to_vec(leaf_cell._rowid, &mut cell_payload);
                        }
                        BTreeCell::IndexLeafCell(leaf_cell) => {
                            cell_payload.extend_from_slice(&child_pointer.to_be_bytes());
                            cell_payload.extend_from_slice(leaf_cell.payload);
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

                DeleteState::CheckNeedsBalancing => {
                    let page = self.stack.top();
                    return_if_locked_maybe_load!(self.pager, page);

                    let contents = page.get().contents.as_ref().unwrap();
                    let free_space = compute_free_space(contents, self.usable_space() as u16);
                    let needs_balancing = free_space as usize * 3 > self.usable_space() * 2;

                    let target_key = if page.is_index() {
                        DeleteSavepoint::Payload(self.record().as_ref().unwrap().clone())
                    } else {
                        DeleteSavepoint::Rowid(self.rowid.get().unwrap())
                    };

                    let delete_info = self.state.mut_delete_info().unwrap();
                    if needs_balancing {
                        if delete_info.balance_write_info.is_none() {
                            let mut write_info = WriteInfo::new();
                            write_info.state = WriteState::BalanceStart;
                            delete_info.balance_write_info = Some(write_info);
                        }
                        delete_info.state = DeleteState::WaitForBalancingToComplete { target_key }
                    } else {
                        self.stack.retreat();
                        self.state = CursorState::None;
                        return Ok(CursorResult::Ok(()));
                    }
                    // Only reaches this function call if state = DeleteState::WaitForBalancingToComplete
                    self.save_context();
                }

                DeleteState::WaitForBalancingToComplete { target_key } => {
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
                                state: DeleteState::SeekAfterBalancing { target_key },
                                balance_write_info: Some(write_info),
                            });
                        }

                        CursorResult::IO => {
                            // Move to seek state
                            // Save balance progress and return IO
                            let write_info = match &self.state {
                                CursorState::Write(wi) => wi.clone(),
                                _ => unreachable!("Balance operation changed cursor state"),
                            };

                            self.state = CursorState::Delete(DeleteInfo {
                                state: DeleteState::WaitForBalancingToComplete { target_key },
                                balance_write_info: Some(write_info),
                            });
                            return Ok(CursorResult::IO);
                        }
                    }
                }

                DeleteState::SeekAfterBalancing { target_key } => {
                    let key = match &target_key {
                        DeleteSavepoint::Rowid(rowid) => SeekKey::TableRowId(*rowid),
                        DeleteSavepoint::Payload(immutable_record) => {
                            SeekKey::IndexKey(immutable_record)
                        }
                    };
                    return_if_io!(self.seek(key, SeekOp::EQ));

                    self.state = CursorState::None;
                    return Ok(CursorResult::Ok(()));
                }
            }
        }
    }

    /// In outer joins, whenever the right-side table has no matching row, the query must still return a row
    /// for each left-side row. In order to achieve this, we set the null flag on the right-side table cursor
    /// so that it returns NULL for all columns until cleared.
    #[inline(always)]
    pub fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    #[inline(always)]
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
                // Need this check because .all returns True on an empty iterator,
                // So when record_opt is invalidated, it would always indicate show up as a duplicate key
                if existing_key.len() != inserted_key_vals.len() {
                    return Ok(CursorResult::Ok(false));
                }

                Ok(CursorResult::Ok(
                    existing_key
                        .iter()
                        .zip(inserted_key_vals.iter())
                        .all(|(a, b)| a == b),
                ))
            }
            None => {
                // Cursor not pointing at a record — table is empty or past last
                Ok(CursorResult::Ok(false))
            }
        }
    }

    pub fn exists(&mut self, key: &Value) -> Result<CursorResult<bool>> {
        assert!(self.mv_cursor.is_none());
        let int_key = match key {
            Value::Integer(i) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let _ = return_if_io!(self.move_to(SeekKey::TableRowId(*int_key as u64), SeekOp::EQ));
        let page = self.stack.top();
        // TODO(pere): request load
        return_if_locked!(page);

        let contents = page.get().contents.as_ref().unwrap();

        // find cell
        let int_key = match key {
            Value::Integer(i) => *i as u64,
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

    /// Count the number of entries in the b-tree
    ///
    /// Only supposed to be used in the context of a simple Count Select Statement
    pub fn count(&mut self) -> Result<CursorResult<usize>> {
        if self.count == 0 {
            self.move_to_root();
        }

        if let Some(_mv_cursor) = &self.mv_cursor {
            todo!("Implement count for mvcc");
        }

        let mut mem_page_rc;
        let mut mem_page;
        let mut contents;

        loop {
            mem_page_rc = self.stack.top();
            return_if_locked_maybe_load!(self.pager, mem_page_rc);
            mem_page = mem_page_rc.get();
            contents = mem_page.contents.as_ref().unwrap();

            /* If this is a leaf page or the tree is not an int-key tree, then
             ** this page contains countable entries. Increment the entry counter
             ** accordingly.
             */
            if !matches!(contents.page_type(), PageType::TableInterior) {
                self.count += contents.cell_count();
            }

            let cell_idx = self.stack.current_cell_index() as usize;

            // Second condition is necessary in case we return if the page is locked in the loop below
            if contents.is_leaf() || cell_idx > contents.cell_count() {
                loop {
                    if !self.stack.has_parent() {
                        // All pages of the b-tree have been visited. Return successfully
                        self.move_to_root();

                        return Ok(CursorResult::Ok(self.count));
                    }

                    // Move to parent
                    self.going_upwards = true;
                    self.stack.pop();

                    mem_page_rc = self.stack.top();
                    return_if_locked_maybe_load!(self.pager, mem_page_rc);
                    mem_page = mem_page_rc.get();
                    contents = mem_page.contents.as_ref().unwrap();

                    let cell_idx = self.stack.current_cell_index() as usize;

                    if cell_idx <= contents.cell_count() {
                        break;
                    }
                }
            }

            let cell_idx = self.stack.current_cell_index() as usize;

            assert!(cell_idx <= contents.cell_count(),);
            assert!(!contents.is_leaf());

            if cell_idx == contents.cell_count() {
                // Move to right child
                // should be safe as contents is not a leaf page
                let right_most_pointer = contents.rightmost_pointer().unwrap();
                self.stack.advance();
                let mem_page = self.pager.read_page(right_most_pointer as usize)?;
                self.going_upwards = false;
                self.stack.push(mem_page);
            } else {
                // Move to child left page
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

                match cell {
                    BTreeCell::TableInteriorCell(TableInteriorCell {
                        _left_child_page: left_child_page,
                        ..
                    })
                    | BTreeCell::IndexInteriorCell(IndexInteriorCell {
                        left_child_page, ..
                    }) => {
                        self.stack.advance();
                        let mem_page = self.pager.read_page(left_child_page as usize)?;
                        self.going_upwards = false;
                        self.stack.push(mem_page);
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    /// Save cursor context, to be restored later
    pub fn save_context(&mut self) {
        if let Some(rowid) = self.rowid.get() {
            self.valid_state = CursorValidState::RequireSeek;
            match self.stack.top().get_contents().page_type() {
                PageType::TableInterior | PageType::TableLeaf => {
                    self.context = Some(CursorContext::TableRowId(rowid));
                }
                PageType::IndexInterior | PageType::IndexLeaf => {
                    self.context = Some(CursorContext::IndexKeyRowId(
                        self.reusable_immutable_record
                            .borrow()
                            .as_ref()
                            .unwrap()
                            .clone(),
                    ));
                }
            }
        }
    }

    /// If context is defined, restore it and set it None on success
    fn restore_context(&mut self) -> Result<CursorResult<()>> {
        if self.context.is_none() || !matches!(self.valid_state, CursorValidState::RequireSeek) {
            return Ok(CursorResult::Ok(()));
        }
        let ctx = self.context.take().unwrap();
        let seek_key = match ctx {
            CursorContext::TableRowId(rowid) => SeekKey::TableRowId(rowid),
            CursorContext::IndexKeyRowId(ref record) => SeekKey::IndexKey(record),
        };
        let res = self.seek(seek_key, SeekOp::EQ)?;
        match res {
            CursorResult::Ok(_) => {
                self.valid_state = CursorValidState::Valid;
                Ok(CursorResult::Ok(()))
            }
            CursorResult::IO => {
                self.context = Some(ctx);
                Ok(CursorResult::IO)
            }
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
        tracing::trace!(
            "pagestack::advance {}, cell_indices={:?}",
            self.cell_indices.borrow()[current],
            self.cell_indices
        );
        self.cell_indices.borrow_mut()[current] += 1;
    }

    fn retreat(&self) {
        let current = self.current();
        tracing::trace!(
            "pagestack::retreat {}, cell_indices={:?}",
            self.cell_indices.borrow()[current],
            self.cell_indices
        );
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
        self.cell_indices.borrow_mut()[current] = idx;
    }

    fn has_parent(&self) -> bool {
        self.current_page.get() > 0
    }

    fn clear(&self) {
        self.current_page.set(-1);
    }
    pub fn parent_page(&self) -> Option<PageRef> {
        if self.current_page.get() > 0 {
            Some(
                self.stack.borrow()[self.current() - 1]
                    .as_ref()
                    .unwrap()
                    .clone(),
            )
        } else {
            None
        }
    }
}

/// Used for redistributing cells during a balance operation.
struct CellArray {
    cells: Vec<&'static mut [u8]>, // TODO(pere): make this with references

    number_of_cells_per_page: [u16; 5], // number of cells in each page
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
        shift_cells_left(page, count_cells, number_to_shift);
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

/// Shifts the cell pointers in the B-tree page to the left by a specified number of positions.
///
/// # Parameters
/// - `page`: A mutable reference to the `PageContent` representing the B-tree page.
/// - `count_cells`: The total number of cells currently in the page.
/// - `number_to_shift`: The number of cell pointers to shift to the left.
///
/// # Behavior
/// This function modifies the cell pointer array within the page by copying memory regions.
/// It shifts the pointers starting from `number_to_shift` to the beginning of the array,
/// effectively removing the first `number_to_shift` pointers.
fn shift_cells_left(page: &mut PageContent, count_cells: usize, number_to_shift: usize) {
    let buf = page.as_ptr();
    let (start, _) = page.cell_pointer_array_offset_and_size();
    buf.copy_within(
        start + (number_to_shift * 2)..start + (count_cells * 2),
        start,
    );
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
    let mut number_of_cells_buffered = 0;
    let mut buffered_cells_offsets: [u16; 10] = [0; 10];
    let mut buffered_cells_ends: [u16; 10] = [0; 10];
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
            assert!(len > 0, "cell size should be greater than 0");
            let end = offset + len;

            /* Try to merge the current cell with a contiguous buffered cell to reduce the number of
             * `free_cell_range()` operations. Break on the first merge to avoid consuming too much time,
             * `free_cell_range()` will try to merge contiguous cells anyway. */
            let mut j = 0;
            while j < number_of_cells_buffered {
                // If the buffered cell is immediately after the current cell
                if buffered_cells_offsets[j] == end {
                    // Merge them by updating the buffered cell's offset to the current cell's offset
                    buffered_cells_offsets[j] = offset;
                    break;
                // If the buffered cell is immediately before the current cell
                } else if buffered_cells_ends[j] == offset {
                    // Merge them by updating the buffered cell's end offset to the current cell's end offset
                    buffered_cells_ends[j] = end;
                    break;
                }
                j += 1;
            }
            // If no cells were merged
            if j >= number_of_cells_buffered {
                // If the buffered cells array is full, flush the buffered cells using `free_cell_range()` to empty the array
                if number_of_cells_buffered >= buffered_cells_offsets.len() {
                    for j in 0..number_of_cells_buffered {
                        free_cell_range(
                            page,
                            buffered_cells_offsets[j],
                            buffered_cells_ends[j] - buffered_cells_offsets[j],
                            usable_space,
                        )?;
                    }
                    number_of_cells_buffered = 0; // Reset array counter
                }
                // Buffer the current cell
                buffered_cells_offsets[number_of_cells_buffered] = offset;
                buffered_cells_ends[number_of_cells_buffered] = end;
                number_of_cells_buffered += 1;
            }
            number_of_cells_removed += 1;
        }
    }
    for j in 0..number_of_cells_buffered {
        free_cell_range(
            page,
            buffered_cells_offsets[j],
            buffered_cells_ends[j] - buffered_cells_offsets[j],
            usable_space,
        )?;
    }
    page.write_u16(
        offset::BTREE_CELL_COUNT,
        page.cell_count() as u16 - number_of_cells_removed as u16,
    );
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
        let overflow_page = pager.allocate_overflow_page();
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
            database::DatabaseFile,
            page_cache::DumbLruPageCache,
            pager::CreateBTreeFlags,
            sqlite3_ondisk::{self, DatabaseHeader},
        },
        types::Text,
        vdbe::Register,
        BufferPool, Connection, DatabaseStorage, StepResult, WalFile, WalFileShared,
        WriteCompletion,
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
        types::Value,
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
        let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(1))]);
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
            let record =
                ImmutableRecord::from_registers(&[Register::Value(Value::Integer(i as i64))]);
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
        let page_size = db_header.get_page_size();

        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let io_file = io.open_file("test.db", OpenFlags::Create, false).unwrap();
        let db_file = Arc::new(DatabaseFile::new(io_file));

        let buffer_pool = Rc::new(BufferPool::new(page_size as usize));
        let wal_shared = WalFileShared::open_shared(&io, "test.wal", page_size).unwrap();
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
                let value = ImmutableRecord::from_registers(&[Register::Value(Value::Blob(vec![
                        0;
                        *size
                    ]))]);
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

    fn rng_from_time_or_env() -> (ChaCha8Rng, u64) {
        let seed = std::env::var("SEED").map_or(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            |v| {
                v.parse()
                    .expect("Failed to parse SEED environment variable as u64")
            },
        );
        let rng = ChaCha8Rng::seed_from_u64(seed);
        (rng, seed)
    }

    fn btree_insert_fuzz_run(
        attempts: usize,
        inserts: usize,
        size: impl Fn(&mut ChaCha8Rng) -> usize,
    ) {
        let (mut rng, seed) = rng_from_time_or_env();
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
                let value =
                    ImmutableRecord::from_registers(&[Register::Value(Value::Blob(vec![0; size]))]);
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

    fn btree_index_insert_fuzz_run(attempts: usize, inserts: usize) {
        let (mut rng, seed) = if std::env::var("SEED").is_ok() {
            let seed = std::env::var("SEED").unwrap();
            let seed = seed.parse::<u64>().unwrap();
            let rng = ChaCha8Rng::seed_from_u64(seed);
            (rng, seed)
        } else {
            rng_from_time_or_env()
        };
        let mut seen = HashSet::new();
        tracing::info!("super seed: {}", seed);
        for _ in 0..attempts {
            let (pager, _) = empty_btree();
            let index_root_page = pager.btree_create(&CreateBTreeFlags::new_index());
            let index_root_page = index_root_page as usize;
            let mut cursor = BTreeCursor::new(None, pager.clone(), index_root_page);
            let mut keys = Vec::new();
            tracing::info!("seed: {}", seed);
            for _ in 0..inserts {
                let key = {
                    let result;
                    loop {
                        let cols = (0..10)
                            .map(|_| (rng.next_u64() % (1 << 30)) as i64)
                            .collect::<Vec<_>>();
                        if seen.contains(&cols) {
                            continue;
                        } else {
                            seen.insert(cols.clone());
                        }
                        result = cols;
                        break;
                    }
                    result
                };
                keys.push(key.clone());
                let value = ImmutableRecord::from_registers(
                    &key.iter()
                        .map(|col| Register::Value(Value::Integer(*col)))
                        .collect::<Vec<_>>(),
                );
                run_until_done(
                    || {
                        cursor.insert(
                            &BTreeKey::new_index_key(&value),
                            cursor.is_write_in_progress(),
                        )
                    },
                    pager.deref(),
                )
                .unwrap();
                keys.sort();
                cursor.move_to_root();
            }
            keys.sort();
            cursor.move_to_root();
            for key in keys.iter() {
                tracing::trace!("seeking key: {:?}", key);
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
                let record = cursor.record();
                let record = record.as_ref().unwrap();
                let cursor_key = record.get_values();
                assert_eq!(
                    cursor_key,
                    &key.iter()
                        .map(|col| RefValue::Integer(*col))
                        .collect::<Vec<_>>(),
                    "key {:?} is not found",
                    key
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
            let record =
                ImmutableRecord::from_registers(&[Register::Value(Value::Integer(i as i64))]);
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
    pub fn btree_index_insert_fuzz_run_equal_size() {
        btree_index_insert_fuzz_run(2, 1024 * 32);
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

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_equal_size() {
        for size in 1..8 {
            tracing::info!("======= size:{} =======", size);
            btree_insert_fuzz_run(2, 10_000, |_| size);
        }
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_index_insert_fuzz_run_equal_size() {
        btree_index_insert_fuzz_run(2, 10_000);
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_random() {
        btree_insert_fuzz_run(128, 10_000, |rng| (rng.next_u32() % 4096) as usize);
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_small() {
        btree_insert_fuzz_run(1, 10_000, |rng| (rng.next_u32() % 128) as usize);
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_big() {
        btree_insert_fuzz_run(64, 10_000, |rng| {
            3 * 1024 + (rng.next_u32() % 1024) as usize
        });
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_overflow() {
        btree_insert_fuzz_run(64, 10_000, |rng| (rng.next_u32() % 32 * 1024) as usize);
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn setup_test_env(database_size: u32) -> (Rc<Pager>, Arc<SpinLock<DatabaseHeader>>) {
        let page_size = 512;
        let mut db_header = DatabaseHeader::default();
        db_header.update_page_size(page_size);
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
            page_size,
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
                db_header.lock().get_page_size() as usize,
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
            let record =
                ImmutableRecord::from_registers(&[Register::Value(Value::Integer(i as i64))]);
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
            let record =
                ImmutableRecord::from_registers(&[Register::Value(Value::Integer(i as i64))]);
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
                    let record = ImmutableRecord::from_registers(&[Register::Value(
                        Value::Integer(i as i64),
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
                        let record = ImmutableRecord::from_registers(&[Register::Value(
                            Value::Integer(i as i64),
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

    // this test will create a tree like this:
    // -page:2, ptr(right):4
    // +cells:node[rowid:14, ptr(<=):3]
    //   -page:3, ptr(right):0
    //   +cells:leaf[rowid:11, len(payload):137, overflow:false]
    //   -page:4, ptr(right):0
    //   +cells:
    #[test]
    pub fn test_drop_page_in_balancing_issue_1203() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let queries = vec![
"CREATE TABLE lustrous_petit (awesome_nomous TEXT,ambitious_amargi TEXT,fantastic_daniels BLOB,stupendous_highleyman TEXT,relaxed_crane TEXT,elegant_bromma INTEGER,proficient_castro BLOB,ambitious_liman TEXT,responsible_lusbert BLOB);",
"INSERT INTO lustrous_petit VALUES ('funny_sarambi', 'hardworking_naoumov', X'666561726C6573735F68696C6C', 'elegant_iafd', 'rousing_flag', 681399778772406122, X'706572736F6E61626C655F676F6477696E6772696D6D', 'insightful_anonymous', X'706F77657266756C5F726F636861'), ('personable_holmes', 'diligent_pera', X'686F6E6573745F64696D656E73696F6E', 'energetic_raskin', 'gleaming_federasyon', -2778469859573362611, X'656666696369656E745F6769617A', 'sensible_skirda', X'66616E7461737469635F6B656174696E67'), ('inquisitive_baedan', 'brave_sphinx', X'67656E65726F75735F6D6F6E7473656E79', 'inquisitive_syndicate', 'amiable_room', 6954857961525890638, X'7374756E6E696E675F6E6965747A73636865', 'glowing_coordinator', X'64617A7A6C696E675F7365766572696E65'), ('upbeat_foxtale', 'engaging_aktimon', X'63726561746976655F6875746368696E6773', 'ample_locura', 'creative_barrett', 6413352509911171593, X'6772697070696E675F6D696E7969', 'competitive_parissi', X'72656D61726B61626C655F77696E7374616E6C6579');",
"INSERT INTO lustrous_petit VALUES ('ambitious_berry', 'devoted_marshall', X'696E7175697369746976655F6C6172657661', 'flexible_pramen', 'outstanding_stauch', 6936508362673228293, X'6C6F76696E675F6261756572', 'charming_anonymous', X'68617264776F726B696E675F616E6E6973'), ('enchanting_cohen', 'engaging_rubel', X'686F6E6573745F70726F766F63617A696F6E65', 'humorous_robin', 'imaginative_shuzo', 4762266264295288131, X'726F7573696E675F6261796572', 'vivid_bolling', X'6F7267616E697A65645F7275696E73'), ('affectionate_resistance', 'gripping_rustamova', X'6B696E645F6C61726B696E', 'bright_boulanger', 'upbeat_ashirov', -1726815435854320541, X'61646570745F66646361', 'dazzling_tashjian', X'68617264776F726B696E675F6D6F72656C'), ('zestful_ewald', 'favorable_lewis', X'73747570656E646F75735F7368616C6966', 'bright_combustion', 'blithesome_harding', 8408539013935554176, X'62726176655F737079726F706F756C6F75', 'hilarious_finnegan', X'676976696E675F6F7267616E697A696E67'), ('blithesome_picqueray', 'sincere_william', X'636F75726167656F75735F6D69746368656C6C', 'rousing_atan', 'mirthful_katie', -429232313453215091, X'6C6F76656C795F776174616E616265', 'stupendous_mcmillan', X'666F63757365645F6B61666568'), ('incredible_kid', 'friendly_yvetot', X'706572666563745F617A697A', 'helpful_manhattan', 'shining_horrox', -4318061095860308846, X'616D626974696F75735F726F7765', 'twinkling_anarkiya', X'696D6167696E61746976655F73756D6E6572');",
"INSERT INTO lustrous_petit VALUES ('sleek_graeber', 'approachable_ghazzawi', X'62726176655F6865776974747768697465', 'adaptable_zimmer', 'polite_cohn', -5464225138957223865, X'68756D6F726F75735F736E72', 'adaptable_igualada', X'6C6F76656C795F7A686F75'), ('imaginative_rautiainen', 'magnificent_ellul', X'73706C656E6469645F726F6361', 'responsible_brown', 'upbeat_uruguaya', -1185340834321792223, X'616D706C655F6D6470', 'philosophical_kelly', X'676976696E675F6461676865726D6172676F7369616E'), ('blithesome_darkness', 'creative_newell', X'6C757374726F75735F61706174726973', 'engaging_kids', 'charming_wark', -1752453819873942466, X'76697669645F6162657273', 'independent_barricadas', X'676C697374656E696E675F64686F6E6474'), ('productive_chardronnet', 'optimistic_karnage', X'64696C6967656E745F666F72657374', 'engaging_beggar', 'sensible_wolke', 784341549042407442, X'656E676167696E675F6265726B6F7769637A', 'blithesome_zuzenko', X'6E6963655F70726F766F63617A696F6E65');",
"INSERT INTO lustrous_petit VALUES ('shining_sagris', 'considerate_mother', X'6F70656E5F6D696E6465645F72696F74', 'polite_laufer', 'patient_mink', 2240393952789100851, X'636F75726167656F75735F6D636D696C6C616E', 'glowing_robertson', X'68656C7066756C5F73796D6F6E6473'), ('dazzling_glug', 'stupendous_poznan', X'706572736F6E61626C655F6672616E6B73', 'open_minded_ruins', 'qualified_manes', 2937238916206423261, X'696E736967687466756C5F68616B69656C', 'passionate_borl', X'616D6961626C655F6B7570656E647561'), ('wondrous_parry', 'knowledgeable_giovanni', X'6D6F76696E675F77696E6E', 'shimmering_aberlin', 'affectionate_calhoun', 702116954493913499, X'7265736F7572636566756C5F62726F6D6D61', 'propitious_mezzagarcia', X'746563686E6F6C6F676963616C5F6E6973686974616E69');",
"INSERT INTO lustrous_petit VALUES ('kind_room', 'hilarious_crow', X'6F70656E5F6D696E6465645F6B6F74616E7969', 'hardworking_petit', 'adaptable_zarrow', 2491343172109894986, X'70726F647563746976655F646563616C6F677565', 'willing_sindikalis', X'62726561746874616B696E675F6A6F7264616E');",
"INSERT INTO lustrous_petit VALUES ('confident_etrebilal', 'agreeable_shifu', X'726F6D616E7469635F7363687765697A6572', 'loving_debs', 'gripping_spooner', -3136910055229112693, X'677265676172696F75735F736B726F7A6974736B79', 'ample_ontiveros', X'7175616C69666965645F726F6D616E69656E6B6F'), ('competitive_call', 'technological_egoumenides', X'6469706C6F6D617469635F6D6F6E616768616E', 'willing_stew', 'frank_neal', -5973720171570031332, X'6C6F76696E675F6465737461', 'dazzling_gambone', X'70726F647563746976655F6D656E64656C676C6565736F6E'), ('favorable_delesalle', 'sensible_atterbury', X'666169746866756C5F64617861', 'bountiful_aldred', 'marvelous_malgraith', 5330463874397264493, X'706572666563745F7765726265', 'lustrous_anti', X'6C6F79616C5F626F6F6B6368696E'), ('stellar_corlu', 'loyal_espana', X'6D6F76696E675F7A6167', 'efficient_nelson', 'qualified_shepard', 1015518116803600464, X'737061726B6C696E675F76616E6469766572', 'loving_scoffer', X'686F6E6573745F756C72696368'), ('adaptable_taylor', 'shining_yasushi', X'696D6167696E61746976655F776974746967', 'alluring_blackmore', 'zestful_coeurderoy', -7094136731216188999, X'696D6167696E61746976655F757A63617465677569', 'gleaming_hernandez', X'6672616E6B5F646F6D696E69636B'), ('competitive_luis', 'stellar_fredericks', X'616772656561626C655F6D696368656C', 'optimistic_navarro', 'funny_hamilton', 4003895682491323194, X'6F70656E5F6D696E6465645F62656C6D6173', 'incredible_thorndycraft', X'656C6567616E745F746F6C6B69656E'), ('remarkable_parsons', 'sparkling_ulrich', X'737061726B6C696E675F6D6172696E636561', 'technological_leighlais', 'warmhearted_konok', -5789111414354869563, X'676976696E675F68657272696E67', 'adept_dabtara', X'667269656E646C795F72617070');",
"INSERT INTO lustrous_petit VALUES ('hardworking_norberg', 'approachable_winter', X'62726176655F68617474696E6768', 'imaginative_james', 'open_minded_capital', -5950508516718821688, X'6C757374726F75735F72616E7473', 'warmhearted_limanov', X'696E736967687466756C5F646F637472696E65'), ('generous_shatz', 'generous_finley', X'726176697368696E675F6B757A6E6574736F76', 'stunning_arrigoni', 'favorable_volcano', -8442328990977069526, X'6D6972746866756C5F616C7467656C64', 'thoughtful_zurbrugg', X'6D6972746866756C5F6D6F6E726F65'), ('frank_kerr', 'splendid_swain', X'70617373696F6E6174655F6D6470', 'flexible_dubey', 'sensible_tj', 6352949260574274181, X'656666696369656E745F6B656D736B79', 'vibrant_ege', X'736C65656B5F6272696768746F6E'), ('organized_neal', 'glistening_sugar', X'656E676167696E675F6A6F72616D', 'romantic_krieger', 'qualified_corr', -4774868512022958085, X'706572666563745F6B6F7A6172656B', 'bountiful_zaikowska', X'74686F7567687466756C5F6C6F6767616E73'), ('excellent_lydiettcarrion', 'diligent_denslow', X'666162756C6F75735F6D616E68617474616E', 'confident_tomar', 'glistening_ligt', -1134906665439009896, X'7175616C69666965645F6F6E6B656E', 'remarkable_anarkiya', X'6C6F79616C5F696E64616261'), ('passionate_melis', 'loyal_xsilent', X'68617264776F726B696E675F73637564', 'lustrous_barnes', 'nice_sugako', -4097897163377829983, X'726F6D616E7469635F6461686572', 'bright_imrie', X'73656E7369626C655F6D61726B'), ('giving_mlb', 'breathtaking_fourier', X'736C65656B5F616E61726368697374', 'glittering_malet', 'brilliant_crew', 8791228049111405793, X'626F756E746966756C5F626576656E736565', 'lovely_swords', X'70726F706974696F75735F696E656469746173'), ('honest_wright', 'qualified_rabble', X'736C65656B5F6D6172656368616C', 'shimmering_marius', 'blithesome_mckelvie', -1330737263592370654, X'6F70656E5F6D696E6465645F736D616C6C', 'energetic_gorman', X'70726F706974696F75735F6B6F74616E7969');",
"DELETE FROM lustrous_petit WHERE (ambitious_liman > 'adept_dabtaqu');",
"INSERT INTO lustrous_petit VALUES ('technological_dewey', 'fabulous_st', X'6F7074696D69737469635F73687562', 'considerate_levy', 'adaptable_kernis', 4195134012457716562, X'61646570745F736F6C6964617269646164', 'vibrant_crump', X'6C6F79616C5F72796E6572'), ('super_marjan', 'awesome_gethin', X'736C65656B5F6F737465727765696C', 'diplomatic_loidl', 'qualified_bokani', -2822676417968234733, X'6272696768745F64756E6C6170', 'creative_en', X'6D6972746866756C5F656C6F6666'), ('philosophical_malet', 'unique_garcia', X'76697669645F6E6F7262657267', 'spellbinding_fire', 'faithful_barringtonbush', -7293711848773657758, X'6272696C6C69616E745F6F6B65656665', 'gripping_guillon', X'706572736F6E61626C655F6D61726C696E7370696B65'), ('thoughtful_morefus', 'lustrous_rodriguez', X'636F6E666964656E745F67726F73736D616E726F73686368696E', 'devoted_jackson', 'propitious_karnage', -7802999054396485709, X'63617061626C655F64', 'enchanting_orwell', X'7477696E6B6C696E675F64616C616B6F676C6F75'), ('alluring_guillon', 'brilliant_pinotnoir', X'706572736F6E61626C655F6A6165636B6C65', 'open_minded_azeez', 'courageous_romania', 2126962403055072268, X'746563686E6F6C6F676963616C5F6962616E657A', 'open_minded_rosa', X'6C757374726F75735F6575726F7065'), ('courageous_kolokotronis', 'inquisitive_gahman', X'677265676172696F75735F626172726574', 'ambitious_shakur', 'fantastic_apatris', -1232732971861520864, X'737061726B6C696E675F7761746368', 'captivating_clover', X'636F6E666964656E745F736574686E65737363617374726F'), ('charming_sullivan', 'focused_congress', X'7368696D6D6572696E675F636C7562', 'wondrous_skrbina', 'giving_mendanlioglu', -6837337053772308333, X'636861726D696E675F73616C696E6173', 'rousing_hedva', X'6469706C6F6D617469635F7061796E');",
        ];

        for query in queries {
            let mut stmt = conn.query(query).unwrap().unwrap();
            loop {
                let row = stmt.step().expect("step");
                match row {
                    StepResult::Done => {
                        break;
                    }
                    _ => {
                        tracing::debug!("row {:?}", row);
                    }
                }
            }
        }
    }

    // this test will create a tree like this:
    // -page:2, ptr(right):3
    // +cells:
    //   -page:3, ptr(right):0
    //   +cells:
    #[test]
    pub fn test_drop_page_in_balancing_issue_1203_2() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let queries = vec![
"CREATE TABLE super_becky (engrossing_berger BLOB,plucky_chai BLOB,mirthful_asbo REAL,bountiful_jon REAL,competitive_petit REAL,engrossing_rexroth REAL);",
"INSERT INTO super_becky VALUES (X'636861726D696E675F6261796572', X'70726F647563746976655F70617269737369', 6847793643.408741, 7330361375.924953, -6586051582.891455, -6921021872.711397), (X'657863656C6C656E745F6F7267616E697A696E67', X'6C757374726F75735F73696E64696B616C6973', 9905774996.48619, 570325205.2246342, 5852346465.53047, 728566012.1968269), (X'7570626561745F73656174746C65', X'62726176655F6661756E', -2202725836.424899, 5424554426.388281, 2625872085.917082, -6657362503.808359), (X'676C6F77696E675F6D617877656C6C', X'7761726D686561727465645F726F77616E', -9610936969.793116, 4886606277.093559, -3414536174.7928505, 6898267795.317778), (X'64796E616D69635F616D616E', X'7374656C6C61725F7374657073', 3918935692.153696, 151068445.947237, 4582065669.356403, -3312668220.4789667), (X'64696C6967656E745F64757272757469', X'7175616C69666965645F6D726163686E696B', 5527271629.262201, 6068855126.044355, 289904657.13490677, 2975774820.0877323), (X'6469706C6F6D617469635F726F76657363696F', X'616C6C7572696E675F626F7474696369', 9844748192.66119, -6180276383.305578, -4137330511.025565, -478754566.79494476), (X'776F6E64726F75735F6173686572', X'6465766F7465645F6176657273696F6E', 2310211470.114773, -6129166761.628184, -2865371645.3145514, 7542428654.8645935), (X'617070726F61636861626C655F6B686F6C61', X'6C757374726F75735F6C696E6E656C6C', -4993113161.458349, 7356727284.362968, -3228937035.568404, -1779334005.5067253);",
"INSERT INTO super_becky VALUES (X'74686F7567687466756C5F726576696577', X'617765736F6D655F63726F73736579', 9401977997.012783, 8428201961.643898, 2822821303.052643, 4555601220.718847), (X'73706563746163756C61725F6B686179617469', X'616772656561626C655F61646F6E696465', 7414547022.041355, 365016845.73330307, 50682963.055828094, -9258802584.962656), (X'6C6F79616C5F656D6572736F6E', X'676C6F77696E675F626174616C6F', -5522070106.765736, 2712536599.6384163, 6631385631.869345, 1242757880.7583427), (X'68617264776F726B696E675F6F6B656C6C79', X'666162756C6F75735F66696C697373', 6682622809.9778805, 4233900041.917185, 9017477903.795563, -756846353.6034946), (X'68617264776F726B696E675F626C61756D616368656E', X'616666656374696F6E6174655F6B6F736D616E', -1146438175.3174362, -7545123696.438596, -6799494012.403366, 5646913977.971333), (X'66616E7461737469635F726F77616E', X'74686F7567687466756C5F7465727269746F72696573', -4414529784.916277, -6209371635.279242, 4491104121.288605, 2590223842.117277);",
"INSERT INTO super_becky VALUES (X'676C697374656E696E675F706F72746572', X'696E7175697369746976655F656D', 2986144164.3676434, 3495899172.5935287, -849280584.9386635, 6869709150.2699375), (X'696D6167696E61746976655F6D65726C696E6F', X'676C6F77696E675F616B74696D6F6E', 8733490615.829357, 6782649864.719433, 6926744218.74107, 1532081022.4379768), (X'6E6963655F726F73736574', X'626C69746865736F6D655F66696C697373', -839304300.0706863, 6155504968.705227, -2951592321.950267, -6254186334.572437), (X'636F6E666964656E745F6C69626574', X'676C696D6D6572696E675F6B6F74616E7969', -5344675223.37533, -8703794729.211002, 3987472096.020382, -7678989974.961197), (X'696D6167696E61746976655F6B61726162756C7574', X'64796E616D69635F6D6367697272', 2028227065.6995697, -7435689525.030833, 7011220815.569796, 5526665697.213846), (X'696E7175697369746976655F636C61726B', X'616666656374696F6E6174655F636C6561766572', 3016598350.546356, -3686782925.383732, 9671422351.958004, 9099319829.078941), (X'63617061626C655F746174616E6B61', X'696E6372656469626C655F6F746F6E6F6D61', 6339989259.432795, -8888997534.102034, 6855868409.475763, -2565348887.290493), (X'676F7267656F75735F6265726E657269', X'65647563617465645F6F6D6F77616C69', 6992467657.527826, -3538089391.748543, -7103111660.146708, 4019283237.3740463), (X'616772656561626C655F63756C74757265', X'73706563746163756C61725F657370616E61', 189387871.06959534, 6211851191.361202, 1786455196.9768047, 7966404387.318119);",
"INSERT INTO super_becky VALUES (X'7068696C6F736F70686963616C5F6C656967686C616973', X'666162756C6F75735F73656D696E61746F7265', 8688321500.141502, -7855144036.024546, -5234949709.573349, -9937638367.366447), (X'617070726F61636861626C655F726F677565', X'676C65616D696E675F6D7574696E79', -5351540099.744092, -3614025150.9013805, -2327775310.276925, 2223379997.077526), (X'676C696D6D6572696E675F63617263686961', X'696D6167696E61746976655F61737379616E6E', 4104832554.8371887, -5531434716.627781, 1652773397.4099865, 3884980522.1830273);",
"DELETE FROM super_becky WHERE (plucky_chai != X'7761726D686561727465645F6877616E67' AND mirthful_asbo != 9537234687.183533 AND bountiful_jon = -3538089391.748543);",
"INSERT INTO super_becky VALUES (X'706C75636B795F6D617263616E74656C', X'696D6167696E61746976655F73696D73', 9535651632.375484, 92270815.0720501, 1299048084.6248207, 6460855331.572151), (X'726F6D616E7469635F706F746C61746368', X'68756D6F726F75735F63686165686F', 9345375719.265533, 7825332230.247925, -7133157299.39028, -6939677879.6597), (X'656666696369656E745F6261676E696E69', X'63726561746976655F67726168616D', -2615470560.1954746, 6790849074.977201, -8081732985.448849, -8133707792.312794), (X'677265676172696F75735F73637564', X'7368696E696E675F67726F7570', -7996394978.2610035, -9734939565.228964, 1108439333.8481388, -5420483517.169478), (X'6C696B61626C655F6B616E6176616C6368796B', X'636F75726167656F75735F7761726669656C64', -1959869609.656724, 4176668769.239971, -8423220404.063669, 9987687878.685959), (X'657863656C6C656E745F68696C6473646F74746572', X'676C6974746572696E675F7472616D7564616E61', -5220160777.908238, 3892402687.8826714, 9803857762.617172, -1065043714.0265541), (X'6D61676E69666963656E745F717565657273', X'73757065725F717565657273', -700932053.2006226, -4706306995.253335, -5286045811.046467, 1954345265.5250092), (X'676976696E675F6275636B65726D616E6E', X'667269656E646C795F70697A7A6F6C61746F', -2186859620.9089565, -6098492099.446075, -7456845586.405931, 8796967674.444252);",
"DELETE FROM super_becky WHERE TRUE;",
"INSERT INTO super_becky VALUES (X'6F7074696D69737469635F6368616E69616C', X'656E657267657469635F6E65677261', 1683345860.4208698, 4163199322.9289455, -4192968616.7868404, -7253371206.571701), (X'616C6C7572696E675F686176656C', X'7477696E6B6C696E675F626965627579636B', -9947019174.287437, 5975899640.893995, 3844707723.8570194, -9699970750.513876), (X'6F7074696D69737469635F7A686F75', X'616D626974696F75735F636F6E6772657373', 4143738484.1081524, -2138255286.170598, 9960750454.03466, 5840575852.80299), (X'73706563746163756C61725F6A6F6E67', X'73656E7369626C655F616269646F72', -1767611042.9716015, -7684260477.580351, 4570634429.188147, -9222640121.140202), (X'706F6C6974655F6B657272', X'696E736967687466756C5F63686F646F726B6F6666', -635016769.5123329, -4359901288.494518, -7531565119.905825, -1180410948.6572971), (X'666C657869626C655F636F6D756E69656C6C6F', X'6E6963655F6172636F73', 8708423014.802425, -6276712625.559328, -771680766.2485523, 8639486874.113342);",
"DELETE FROM super_becky WHERE (mirthful_asbo < 9730384310.536528 AND plucky_chai < X'6E6963655F61726370B2');",
"DELETE FROM super_becky WHERE (mirthful_asbo > 6248699554.426553 AND bountiful_jon > 4124481472.333034);",
"INSERT INTO super_becky VALUES (X'676C696D6D6572696E675F77656C7368', X'64696C6967656E745F636F7262696E', 8217054003.369003, 8745594518.77864, 1928172803.2261295, -8375115534.050233), (X'616772656561626C655F6463', X'6C6F76696E675F666F72656D616E', -5483889804.871533, -8264576639.127487, 4770567289.404846, -3409172927.2573576), (X'6D617276656C6F75735F6173696D616B6F706F756C6F73', X'746563686E6F6C6F676963616C5F6A61637175696572', 2694858779.206814, -1703227425.3442516, -4504989231.263319, -3097265869.5230227), (X'73747570656E646F75735F64757075697364657269', X'68696C6172696F75735F6D75697268656164', 568174708.66469, -4878260547.265669, -9579691520.956625, 73507727.8100338), (X'626C69746865736F6D655F626C6F6B', X'61646570745F6C65696572', 7772117077.916897, 4590608571.321514, -881713470.657032, -9158405774.647465);",
"INSERT INTO super_becky VALUES (X'6772697070696E675F6573736578', X'67656E65726F75735F636875726368696C6C', -4180431825.598956, 7277443000.677654, 2499796052.7878246, -2858339306.235305), (X'756E697175655F6D6172656368616C', X'62726561746874616B696E675F636875726368696C6C', 1401354536.7625294, -611427440.2796707, -4621650430.463729, 1531473111.7482872), (X'657863656C6C656E745F66696E6C6579', X'666169746866756C5F62726F636B', -4020697828.0073624, -2833530733.19637, -7766170050.654022, 8661820959.434689);",
"INSERT INTO super_becky VALUES (X'756E697175655F6C617061797265', X'6C6F76696E675F7374617465', 7063237787.258968, -5425712581.365798, -7750509440.0141945, -7570954710.892544), (X'62726561746874616B696E675F6E65616C', X'636F75726167656F75735F61727269676F6E69', 289862394.2028198, 9690362375.014446, -4712463267.033899, 2474917855.0973473), (X'7477696E6B6C696E675F7368616B7572', X'636F75726167656F75735F636F6D6D6974746565', 5449035403.229155, -2159678989.597906, 3625606019.1150894, -3752010405.4475393);",
"INSERT INTO super_becky VALUES (X'70617373696F6E6174655F73686970776179', X'686F6E6573745F7363687765697A6572', 4193384746.165228, -2232151704.896323, 8615245520.962444, -9789090953.995636);",
"INSERT INTO super_becky VALUES (X'6C696B61626C655F69', X'6661766F7261626C655F6D626168', 6581403690.769894, 3260059398.9544716, -407118859.046051, -3155853965.2700634), (X'73696E636572655F6F72', X'616772656561626C655F617070656C6261756D', 9402938544.308651, -7595112171.758331, -7005316716.211025, -8368210960.419411);",
"INSERT INTO super_becky VALUES (X'6D617276656C6F75735F6B61736864616E', X'6E6963655F636F7272', -5976459640.85817, -3177550476.2092276, 2073318650.736992, -1363247319.9978447);",
"INSERT INTO super_becky VALUES (X'73706C656E6469645F6C616D656E646F6C61', X'677265676172696F75735F766F6E6E65677574', 6898259773.050102, 8973519699.707073, -25070632.280548096, -1845922497.9676847), (X'617765736F6D655F7365766572', X'656E657267657469635F706F746C61746368', -8750678407.717808, 5130907533.668898, -6778425327.111566, 3718982135.202587);",
"INSERT INTO super_becky VALUES (X'70726F706974696F75735F6D616C617465737461', X'657863656C6C656E745F65766572657474', -8846855772.62094, -6168969732.697067, -8796372709.125793, 9983557891.544613), (X'73696E636572655F6C6177', X'696E7175697369746976655F73616E647374726F6D', -6366985697.975358, 3838628702.6652164, 3680621713.3371124, -786796486.8049564), (X'706F6C6974655F676C6561736F6E', X'706C75636B795F677579616E61', -3987946379.104308, -2119148244.413993, -1448660343.6888638, -1264195510.1611118), (X'676C6974746572696E675F6C6975', X'70657273697374656E745F6F6C6976696572', 6741779968.943846, -3239809989.227495, -1026074003.5506897, 4654600514.871752);",
"DELETE FROM super_becky WHERE (engrossing_berger < X'6566651A3C70278D4E200657551D8071A1' AND competitive_petit > 1236742147.9451914);",
"INSERT INTO super_becky VALUES (X'6661766F7261626C655F726569746D616E', X'64657465726D696E65645F726974746572', -7412553243.829927, -7572665195.290464, 7879603411.222157, 3706943306.5691853), (X'70657273697374656E745F6E6F6C616E', X'676C6974746572696E675F73686570617264', 7028261282.277422, -2064164782.3494844, -5244048504.507779, -2399526243.005843), (X'6B6E6F776C6564676561626C655F70617474656E', X'70726F66696369656E745F726F7365627261756768', 3713056763.583538, 3919834206.566164, -6306779387.430006, -9939464323.995546), (X'616461707461626C655F7172757A', X'696E7175697369746976655F68617261776179', 6519349690.299835, -9977624623.820414, 7500579325.440605, -8118341251.362242);",
"INSERT INTO super_becky VALUES (X'636F6E73696465726174655F756E696F6E', X'6E6963655F6573736578', -1497385534.8720198, 9957688503.242973, 9191804202.566128, -179015615.7117195), (X'666169746866756C5F626F776C656773', X'6361707469766174696E675F6D6367697272', 893707300.1576138, 3381656294.246702, 6884723724.381908, 6248331214.701559), (X'6B6E6F776C6564676561626C655F70656E6E61', X'6B696E645F616A697468', -3335162603.6574974, 1812878172.8505402, 5115606679.658335, -5690100280.808182), (X'617765736F6D655F77696E7374616E6C6579', X'70726F706974696F75735F6361726173736F', -7395576292.503981, 4956546102.029215, -1468521769.7486448, -2968223925.60355), (X'636F75726167656F75735F77617266617265', X'74686F7567687466756C5F7361707068697265', 7052982930.566017, -9806098174.104418, -6910398936.377775, -4041963031.766964), (X'657863656C6C656E745F6B62', X'626C69746865736F6D655F666F75747A6F706F756C6F73', 6142173202.994768, 5193126957.544125, -7522202722.983735, -1659088056.594862), (X'7374756E6E696E675F6E6576616461', X'626F756E746966756C5F627572746F6E', -3822097036.7628613, -3458840259.240303, 2544472236.86788, 6928890176.466003);",
"INSERT INTO super_becky VALUES (X'706572736F6E61626C655F646D69747269', X'776F6E64726F75735F6133796F', 2651932559.0077076, 811299402.3174248, -8271909238.671928, 6761098864.189909);",
"INSERT INTO super_becky VALUES (X'726F7573696E675F6B6C6166657461', X'64617A7A6C696E675F6B6E617070', 9370628891.439335, -5923332007.253168, -2763161830.5880013, -9156194881.875952), (X'656666696369656E745F6C6576656C6C6572', X'616C6C7572696E675F706561636F7474', 3102641409.8314342, 2838360181.628153, 2466271662.169607, 1015942181.844162), (X'6469706C6F6D617469635F7065726B696E73', X'726F7573696E675F6172616269', -1551071129.022499, -8079487600.186886, 7832984580.070087, -6785993247.895652), (X'626F756E746966756C5F6D656D62657273', X'706F77657266756C5F70617269737369', 9226031830.72445, 7012021503.536997, -2297349030.108919, -2738320055.4710903), (X'676F7267656F75735F616E6172636F7469636F', X'68656C7066756C5F7765696C616E64', -8394163480.676959, -2978605095.699134, -6439355448.021704, 9137308022.281273), (X'616666656374696F6E6174655F70726F6C65696E666F', X'706C75636B795F73616E7A', 3546758708.3524914, -1870964264.9353771, 338752565.3643894, -3908023657.299715), (X'66756E6E795F706F70756C61697265', X'6F75747374616E64696E675F626576696E67746F6E', -1533858145.408224, 6164225076.710373, 8419445987.622173, 584555253.6852646), (X'76697669645F6D7474', X'7368696D6D6572696E675F70616F6E65737361', 5512251366.193035, -8680583180.123213, -4445968638.153208, -3274009935.4229546);",
"INSERT INTO super_becky VALUES (X'7068696C6F736F70686963616C5F686F7264', X'657863656C6C656E745F67757373656C7370726F757473', -816909447.0240917, -3614686681.8786583, 7701617524.26067, -4541962047.183721), (X'616D6961626C655F69676E6174696576', X'6D61676E69666963656E745F70726F76696E6369616C69', -1318532883.847702, -4918966075.976474, -7601723171.33518, -3515747704.3847466), (X'70726F66696369656E745F32303137', X'66756E6E795F6E77', -1264540201.518032, 8227396547.578808, 6245093925.183641, -8368355328.110817);",
"INSERT INTO super_becky VALUES (X'77696C6C696E675F6E6F6B6B65', X'726F6D616E7469635F677579616E61', 6618610796.3707695, -3814565359.1524105, 1663106272.4565296, -4175107840.768817), (X'72656C617865645F7061766C6F76', X'64657465726D696E65645F63686F646F726B6F6666', -3350029338.034504, -3520837855.4619064, 3375167499.631817, -8866806483.714607), (X'616D706C655F67696464696E6773', X'667269656E646C795F6A6F686E', 1458864959.9942684, 1344208968.0486107, 9335156635.91314, -6180643697.918882), (X'72656C617865645F6C65726F79', X'636F75726167656F75735F6E6F72646772656E', -5164986537.499656, 8820065797.720875, 6146530425.891005, 6949241471.958189), (X'666F63757365645F656D6D61', X'696D6167696E61746976655F6C6F6E67', -9587619060.80035, 6128068142.184402, 6765196076.956905, 800226302.7983418);",
"INSERT INTO super_becky VALUES (X'616D626974696F75735F736F6E67', X'706572666563745F6761686D616E', 4989979180.706432, -9374266591.537058, 314459621.2820797, -3200029490.9553604), (X'666561726C6573735F626C6174', X'676C697374656E696E675F616374696F6E', -8512203612.903147, -7625581186.013805, -9711122307.234787, -301590929.32751083), (X'617765736F6D655F6669646573', X'666169746866756C5F63756E6E696E6768616D', -1428228887.9205084, 7669883854.400173, 5604446195.905277, -1509311057.9653416), (X'68756D6F726F75735F77697468647261776E', X'62726561746874616B696E675F7472617562656C', -7292778713.676636, -6728132503.529593, 2805341768.7252483, 330416975.2300949);",
"INSERT INTO super_becky VALUES (X'677265676172696F75735F696873616E', X'7374656C6C61725F686172746D616E', 8819210651.1988, 5298459883.813452, 7293544377.958424, 460475869.72971725), (X'696E736967687466756C5F62657765726E69747A', X'676C65616D696E675F64656E736C6F77', -6911957282.193239, 1754196756.2193146, -6316860403.693853, -3094020672.236368), (X'6D6972746866756C5F616D6265727261656B656C6C79', X'68756D6F726F75735F6772617665', 1785574023.0269203, -372056983.82761574, 4133719439.9538956, 9374053482.066044), (X'76697669645F736169747461', X'7761726D686561727465645F696E656469746173', 2787071361.6099434, 9663839418.553448, -5934098589.901047, -9774745509.608858), (X'61646570745F6F6375727279', X'6C696B61626C655F726569746D616E', -3098540915.1310825, 5460848322.672174, -6012867197.519758, 6769770087.661135), (X'696E646570656E64656E745F6F', X'656C6567616E745F726F6F726461', 1462542860.3143978, 3360904654.2464733, 5458876201.665213, -5522844849.529962), (X'72656D61726B61626C655F626F6B616E69', X'6F70656E5F6D696E6465645F686F72726F78', 7589481760.867031, 7970075121.546291, 7513467575.5213585, 9663061478.289227), (X'636F6E666964656E745F6C616479', X'70617373696F6E6174655F736B726F7A6974736B79', 8266917234.53915, -7172933478.625412, 309854059.94031143, -8309837814.497616);",
"DELETE FROM super_becky WHERE (competitive_petit != 8725256604.165474 OR engrossing_rexroth > -3607424615.7839313 OR plucky_chai < X'726F7573696E675F6216E20375');",
"INSERT INTO super_becky VALUES (X'7368696E696E675F736F6C69646169726573', X'666561726C6573735F63617264616E', -170727879.20838165, 2744601113.384678, 5676912434.941502, 6757573601.657997), (X'636F75726167656F75735F706C616E636865', X'696E646570656E64656E745F636172736F6E', -6271723086.761938, -180566679.7470188, -1285774632.134449, 1359665735.7842407), (X'677265676172696F75735F7374616D61746F76', X'7374756E6E696E675F77696C64726F6F7473', -6210238866.953484, 2492683045.8287067, -9688894361.68205, 5420275482.048567), (X'696E646570656E64656E745F6F7267616E697A6572', X'676C6974746572696E675F736F72656C', 9291163783.3073, -6843003475.769236, -1320245894.772686, -5023483808.044955), (X'676C6F77696E675F6E65736963', X'676C65616D696E675F746F726D6579', 829526382.8027191, 9365690945.1316, 4761505764.826195, -4149154965.0024815), (X'616C6C7572696E675F646F637472696E65', X'6E6963655F636C6561766572', 3896644979.981762, -288600448.8016701, 9462856570.130062, -909633752.5993862);",
        ];

        for query in queries {
            let mut stmt = conn.query(query).unwrap().unwrap();
            loop {
                let row = stmt.step().expect("step");
                match row {
                    StepResult::Done => {
                        break;
                    }
                    _ => {
                        tracing::debug!("row {:?}", row);
                    }
                }
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

        let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(0))]);
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

        let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(0))]);
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
            Register::Value(Value::Integer(0)),
            Register::Value(Value::Text(Text::new("aaaaaaaa"))),
        ]);
        let _ = add_record(0, 0, page, record, &conn);

        assert_eq!(page.cell_count(), 1);
        drop_cell(page, 0, usable_space).unwrap();
        assert_eq!(page.cell_count(), 0);

        let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(0))]);
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
            Register::Value(Value::Integer(0)),
            Register::Value(Value::Text(Text::new("aaaaaaaa"))),
        ]);
        let _ = add_record(0, 0, page, record, &conn);

        for _ in 0..100 {
            assert_eq!(page.cell_count(), 1);
            drop_cell(page, 0, usable_space).unwrap();
            assert_eq!(page.cell_count(), 0);

            let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(0))]);
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

        let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(0))]);
        let payload = add_record(0, 0, page, record, &conn);
        let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(1))]);
        let _ = add_record(1, 1, page, record, &conn);
        let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(2))]);
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

        let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(0))]);
        let _ = add_record(0, 0, page, record, &conn);

        let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(0))]);
        let _ = add_record(0, 0, page, record, &conn);
        drop_cell(page, 0, usable_space).unwrap();

        defragment_page(page, usable_space);

        let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(0))]);
        let _ = add_record(0, 1, page, record, &conn);

        drop_cell(page, 0, usable_space).unwrap();

        let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(0))]);
        let _ = add_record(0, 1, page, record, &conn);
    }

    #[test]
    pub fn test_fuzz_victim_2() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let usable_space = 4096;
        let insert = |pos, page| {
            let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(0))]);
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
            let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(0))]);
            let _ = add_record(0, pos, page, record, &conn);
        };
        let drop = |pos, page| {
            drop_cell(page, pos, usable_space).unwrap();
        };
        let defragment = |page| {
            defragment_page(page, usable_space);
        };
        let record = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(0))]);
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
            let value = ImmutableRecord::from_registers(&[Register::Value(Value::Integer(i))]);
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
            let key = Value::Integer(*key);
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
            ImmutableRecord::from_registers(&[Register::Value(Value::Blob(vec![0; 3600]))]);
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
            let value = ImmutableRecord::from_registers(&[Register::Value(Value::Text(
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
            let key = Value::Integer(i);
            let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
            assert!(exists, "Key {} should exist but doesn't", i);
        }

        // Verify the deleted records don't exist.
        for i in 500..=3500 {
            let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
            let key = Value::Integer(i);
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
            let value = ImmutableRecord::from_registers(&[Register::Value(Value::Text(Text {
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

    #[test]
    pub fn test_read_write_payload_with_offset() {
        let (pager, root_page) = empty_btree();
        let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
        let offset = 2; // blobs data starts at offset 2
        let initial_text = "hello world";
        let initial_blob = initial_text.as_bytes().to_vec();
        let value =
            ImmutableRecord::from_registers(&[Register::Value(Value::Blob(initial_blob.clone()))]);

        run_until_done(
            || {
                let key = SeekKey::TableRowId(1);
                cursor.move_to(key, SeekOp::EQ)
            },
            pager.deref(),
        )
        .unwrap();

        run_until_done(
            || cursor.insert(&BTreeKey::new_table_rowid(1, Some(&value)), true),
            pager.deref(),
        )
        .unwrap();

        cursor
            .stack
            .set_cell_index(cursor.stack.current_cell_index() + 1);

        let mut read_buffer = Vec::new();
        run_until_done(
            || {
                cursor.read_write_payload_with_offset(
                    offset,
                    &mut read_buffer,
                    initial_blob.len() as u32,
                    false,
                )
            },
            pager.deref(),
        )
        .unwrap();

        assert_eq!(
            std::str::from_utf8(&read_buffer).unwrap(),
            initial_text,
            "Read data doesn't match expected data"
        );

        let mut modified_hello = "olleh".as_bytes().to_vec();
        run_until_done(
            || cursor.read_write_payload_with_offset(offset, &mut modified_hello, 5, true),
            pager.deref(),
        )
        .unwrap();
        let mut verification_buffer = Vec::new();
        run_until_done(
            || {
                cursor.read_write_payload_with_offset(
                    offset,
                    &mut verification_buffer,
                    initial_blob.len() as u32,
                    false,
                )
            },
            pager.deref(),
        )
        .unwrap();

        assert_eq!(
            std::str::from_utf8(&verification_buffer).unwrap(),
            "olleh world",
            "Modified data doesn't match expected result"
        );
    }

    #[test]
    pub fn test_read_write_payload_with_overflow_page() {
        let (pager, root_page) = empty_btree();
        let mut cursor = BTreeCursor::new(None, pager.clone(), root_page);
        let mut large_blob = vec![b'A'; 40960 - 11]; // insert large blob. 40960 = 10 page long.
        let hello_world = b"hello world";
        large_blob.extend_from_slice(hello_world);
        let value =
            ImmutableRecord::from_registers(&[Register::Value(Value::Blob(large_blob.clone()))]);

        run_until_done(
            || {
                let key = SeekKey::TableRowId(1);
                cursor.move_to(key, SeekOp::EQ)
            },
            pager.deref(),
        )
        .unwrap();

        run_until_done(
            || cursor.insert(&BTreeKey::new_table_rowid(1, Some(&value)), true),
            pager.deref(),
        )
        .unwrap();

        cursor
            .stack
            .set_cell_index(cursor.stack.current_cell_index() + 1);

        let offset_to_hello_world = 4 + (large_blob.len() - 11) as u32; // this offset depends on the records type.
        let mut read_buffer = Vec::new();
        run_until_done(
            || {
                cursor.read_write_payload_with_offset(
                    offset_to_hello_world,
                    &mut read_buffer,
                    11,
                    false,
                )
            },
            pager.deref(),
        )
        .unwrap();
        assert_eq!(
            std::str::from_utf8(&read_buffer).unwrap(),
            "hello world",
            "Failed to read 'hello world' from overflow page"
        );

        let mut modified_hello = "olleh".as_bytes().to_vec();
        run_until_done(
            || {
                cursor.read_write_payload_with_offset(
                    offset_to_hello_world,
                    &mut modified_hello,
                    5,
                    true,
                )
            },
            pager.deref(),
        )
        .unwrap();

        let mut verification_buffer = Vec::new();
        run_until_done(
            || {
                cursor.read_write_payload_with_offset(
                    offset_to_hello_world,
                    &mut verification_buffer,
                    hello_world.len() as u32,
                    false,
                )
            },
            pager.deref(),
        )
        .unwrap();

        assert_eq!(
            std::str::from_utf8(&verification_buffer).unwrap(),
            "olleh world",
            "Modified data doesn't match expected result"
        );
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

    #[test]
    fn test_free_array() {
        let (mut rng, seed) = rng_from_time_or_env();
        tracing::info!("seed={}", seed);

        const ITERATIONS: usize = 10000;
        for _ in 0..ITERATIONS {
            let mut cell_array = CellArray {
                cells: Vec::new(),
                number_of_cells_per_page: [0; 5],
            };
            let mut cells_cloned = Vec::new();
            let (pager, _) = empty_btree();
            let page_type = PageType::TableLeaf;
            let page = pager.allocate_page().unwrap();
            btree_init_page(&page, page_type, 0, pager.usable_space() as u16);
            let mut size = (rng.next_u64() % 100) as u16;
            let mut i = 0;
            // add a bunch of cells
            while compute_free_space(page.get_contents(), pager.usable_space() as u16) >= size + 10
            {
                insert_cell(i, size, page.get_contents(), pager.clone(), page_type);
                i += 1;
                size = (rng.next_u64() % 1024) as u16;
            }

            // Create cell array with references to cells inserted
            let contents = page.get_contents();
            for cell_idx in 0..contents.cell_count() {
                let buf = contents.as_ptr();
                let (start, len) = contents.cell_get_raw_region(
                    cell_idx,
                    payload_overflow_threshold_max(contents.page_type(), 4096),
                    payload_overflow_threshold_min(contents.page_type(), 4096),
                    pager.usable_space(),
                );
                cell_array
                    .cells
                    .push(to_static_buf(&mut buf[start..start + len]));
                cells_cloned.push(buf[start..start + len].to_vec());
            }

            debug_validate_cells!(contents, pager.usable_space() as u16);

            // now free a prefix or suffix of cells added
            let cells_before_free = contents.cell_count();
            let size = rng.next_u64() as usize % cells_before_free;
            let prefix = rng.next_u64() % 2 == 0;
            let start = if prefix {
                0
            } else {
                contents.cell_count() - size
            };
            let removed = page_free_array(
                contents,
                start,
                size as usize,
                &cell_array,
                pager.usable_space() as u16,
            )
            .unwrap();
            // shift if needed
            if prefix {
                shift_cells_left(contents, cells_before_free, removed);
            }

            assert_eq!(removed, size);
            assert_eq!(contents.cell_count(), cells_before_free - size);
            #[cfg(debug_assertions)]
            debug_validate_cells_core(contents, pager.usable_space() as u16);
            // check cells are correct
            let mut cell_idx_cloned = if prefix { size } else { 0 };
            for cell_idx in 0..contents.cell_count() {
                let buf = contents.as_ptr();
                let (start, len) = contents.cell_get_raw_region(
                    cell_idx,
                    payload_overflow_threshold_max(contents.page_type(), 4096),
                    payload_overflow_threshold_min(contents.page_type(), 4096),
                    pager.usable_space(),
                );
                let cell_in_page = &buf[start..start + len];
                let cell_in_array = &cells_cloned[cell_idx_cloned];
                assert_eq!(cell_in_page, cell_in_array);
                cell_idx_cloned += 1;
            }
        }
    }

    fn insert_cell(
        i: u64,
        size: u16,
        contents: &mut PageContent,
        pager: Rc<Pager>,
        page_type: PageType,
    ) {
        let mut payload = Vec::new();
        let record = ImmutableRecord::from_registers(&[Register::Value(Value::Blob(vec![
                0;
                size as usize
            ]))]);
        fill_cell_payload(
            page_type,
            Some(i),
            &mut payload,
            &record,
            pager.usable_space() as u16,
            pager.clone(),
        );
        insert_into_cell(contents, &payload, i as usize, pager.usable_space() as u16).unwrap();
    }
}
