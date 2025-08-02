use parking_lot::Mutex;
use tracing::{instrument, Level};

use crate::{
    schema::Index,
    storage::{
        pager::{BtreePageAllocMode, Pager},
        sqlite3_ondisk::{
            read_u32, read_varint, BTreeCell, DatabaseHeader, PageContent, PageType,
            TableInteriorCell, TableLeafCell, CELL_PTR_SIZE_BYTES, INTERIOR_PAGE_HEADER_SIZE_BYTES,
            LEAF_PAGE_HEADER_SIZE_BYTES, LEFT_CHILD_PTR_SIZE_BYTES,
        },
        state_machines::{EmptyTableState, MoveToRightState, SeekToLastState},
    },
    translate::plan::IterationDirection,
    turso_assert,
    types::{
        find_compare, get_tie_breaker_from_seek_op, IndexInfo, ParseRecordState, RecordCompare,
        RecordCursor, SeekResult,
    },
    util::IOExt,
    Completion, MvCursor,
};

use crate::{
    return_corrupt, return_if_io,
    types::{compare_immutable, IOResult, ImmutableRecord, RefValue, SeekKey, SeekOp, Value},
    LimboError, Result,
};

use super::{
    pager::PageRef,
    sqlite3_ondisk::{
        write_varint_to_vec, IndexInteriorCell, IndexLeafCell, OverflowCell, MINIMUM_CELL_SIZE,
    },
};
#[cfg(debug_assertions)]
use std::collections::HashSet;
use std::{
    cell::{Cell, Ref, RefCell},
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    fmt::Debug,
    ops::DerefMut,
    pin::Pin,
    rc::Rc,
    sync::Arc,
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

    /// A pointer to the first byte of cell allocated content from top (u16).
    ///
    /// A zero value for this integer is interpreted as 65,536.
    /// If a page contains no cells (which is only possible for a root page of a table that
    /// contains no rows) then the offset to the cell content area will equal the page size minus
    /// the bytes of reserved space. If the database uses a 65536-byte page size and the
    /// reserved space is zero (the usual value for reserved space) then the cell content offset of
    /// an empty page wants to be 6,5536
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

/// Maximum number of sibling pages that balancing is performed on.
pub const MAX_SIBLING_PAGES_TO_BALANCE: usize = 3;

/// We only need maximum 5 pages to balance 3 pages, because we can guarantee that cells from 3 pages will fit in 5 pages.
pub const MAX_NEW_SIBLING_PAGES_AFTER_BALANCE: usize = 5;

/// Check if the page is unlocked, if not return IO.
macro_rules! return_if_locked {
    ($expr:expr) => {{
        if $expr.is_locked() {
            return Ok(IOResult::IO);
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
    ($pager:expr, $btree_page:expr) => {{
        if $btree_page.get().is_locked() {
            return Ok(IOResult::IO);
        }
        if !$btree_page.get().is_loaded() {
            let (page, _c) = $pager.read_page($btree_page.get().get().id)?;
            $btree_page.page.replace(page);
            return Ok(IOResult::IO);
        }
    }};
}

/// Wrapper around a page reference used in order to update the reference in case page was unloaded
/// and we need to update the reference.
#[derive(Debug)]
pub struct BTreePageInner {
    pub page: RefCell<PageRef>,
}

pub type BTreePage = Arc<BTreePageInner>;
unsafe impl Send for BTreePageInner {}
unsafe impl Sync for BTreePageInner {}
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
    Rowid(i64),
    Payload(ImmutableRecord),
}

#[derive(Debug, Clone)]
enum DeleteState {
    Start,
    DeterminePostBalancingSeekKey,
    LoadPage {
        post_balancing_seek_key: Option<DeleteSavepoint>,
    },
    FindCell {
        post_balancing_seek_key: Option<DeleteSavepoint>,
    },
    ClearOverflowPages {
        cell_idx: usize,
        cell: BTreeCell,
        original_child_pointer: Option<u32>,
        post_balancing_seek_key: Option<DeleteSavepoint>,
    },
    InteriorNodeReplacement {
        page: PageRef,
        /// the btree level of the page where the cell replacement happened.
        /// if the replacement causes the page to overflow/underflow, we need to remember it and balance it
        /// after the deletion process is otherwise complete.
        btree_depth: usize,
        cell_idx: usize,
        original_child_pointer: Option<u32>,
        post_balancing_seek_key: Option<DeleteSavepoint>,
    },
    CheckNeedsBalancing {
        /// same as `InteriorNodeReplacement::btree_depth`
        btree_depth: usize,
        post_balancing_seek_key: Option<DeleteSavepoint>,
    },
    WaitForBalancingToComplete {
        /// If provided, will also balance an ancestor page at depth `balance_ancestor_at_depth`.
        /// If not provided, balancing will stop as soon as a level is encountered where no balancing is required.
        balance_ancestor_at_depth: Option<usize>,
        target_key: DeleteSavepoint,
    },
    SeekAfterBalancing {
        target_key: DeleteSavepoint,
    },
    /// If the seek performed in [DeleteState::SeekAfterBalancing] returned a [SeekResult::TryAdvance] we need to call next()/prev() to get to the right location.
    /// We need to have this separate state for re-entrancy as calling next()/prev() might yield on IO.
    /// FIXME: refactor DeleteState not to have SeekAfterBalancing and instead use save_context() and restore_context()
    TryAdvance,
}

#[derive(Clone)]
struct DeleteInfo {
    state: DeleteState,
    balance_write_info: Option<WriteInfo>,
}

#[derive(Debug, Clone)]
pub enum OverwriteCellState {
    /// Allocate a new payload for the cell.
    AllocatePayload,
    /// Fill the cell payload with the new payload.
    FillPayload {
        /// Dumb double-indirection via Arc because we clone [WriteState] for some reason and we use unsafe in [FillCellPayloadState::AllocateOverflowPages]
        /// so the underlying Vec must not be cloned in upper layers.
        new_payload: Arc<Mutex<Vec<u8>>>,
        rowid: Option<i64>,
        fill_cell_payload_state: FillCellPayloadState,
    },
    /// Clear the old cell's overflow pages and add them to the freelist.
    /// Overwrite the cell with the new payload.
    ClearOverflowPagesAndOverwrite {
        new_payload: Arc<Mutex<Vec<u8>>>,
        old_offset: usize,
        old_local_size: usize,
    },
}

/// State machine of a write operation.
/// May involve balancing due to overflow.
#[derive(Debug, Clone)]
enum WriteState {
    Start,
    /// Overwrite an existing cell.
    /// In addition to deleting the old cell and writing a new one,
    /// we may also need to clear the old cell's overflow pages
    /// and add them to the freelist.
    Overwrite {
        page: Arc<BTreePageInner>,
        cell_idx: usize,
        state: OverwriteCellState,
    },
    /// Insert a new cell. This path is taken when inserting a new row.
    Insert {
        page: Arc<BTreePageInner>,
        cell_idx: usize,
        new_payload: Vec<u8>,
        fill_cell_payload_state: FillCellPayloadState,
    },
    BalanceStart,
    BalanceFreePages {
        curr_page: usize,
        sibling_count_new: usize,
    },
    /// Choose which sibling pages to balance (max 3).
    /// Generally, the siblings involved will be the page that triggered the balancing and its left and right siblings.
    /// The exceptions are:
    /// 1. If the leftmost page triggered balancing, up to 3 leftmost pages will be balanced.
    /// 2. If the rightmost page triggered balancing, up to 3 rightmost pages will be balanced.
    BalanceNonRootPickSiblings,
    /// Perform the actual balancing. This will result in 1-5 pages depending on the number of total cells to be distributed
    /// from the source pages.
    BalanceNonRootDoBalancing,
    Finish,
}

struct ReadPayloadOverflow {
    payload: Vec<u8>,
    next_page: u32,
    remaining_to_read: usize,
    page: BTreePage,
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
        page: BTreePage,
        current_offset: usize,
        buffer_offset: usize,
        is_write: bool,
    },
}

#[derive(Clone, Debug)]
pub enum BTreeKey<'a> {
    TableRowId((i64, Option<&'a ImmutableRecord>)),
    IndexKey(&'a ImmutableRecord),
}

impl BTreeKey<'_> {
    /// Create a new table rowid key from a rowid and an optional immutable record.
    /// The record is optional because it may not be available when the key is created.
    pub fn new_table_rowid(rowid: i64, record: Option<&ImmutableRecord>) -> BTreeKey<'_> {
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
    fn maybe_rowid(&self) -> Option<i64> {
        match self {
            BTreeKey::TableRowId((rowid, _)) => Some(*rowid),
            BTreeKey::IndexKey(_) => None,
        }
    }

    /// Assert that the key is an integer rowid and return it.
    fn to_rowid(&self) -> i64 {
        match self {
            BTreeKey::TableRowId((rowid, _)) => *rowid,
            BTreeKey::IndexKey(_) => panic!("BTreeKey::to_rowid called on IndexKey"),
        }
    }
}

#[derive(Clone)]
struct BalanceInfo {
    /// Old pages being balanced. We can have maximum 3 pages being balanced at the same time.
    pages_to_balance: [Option<BTreePage>; MAX_SIBLING_PAGES_TO_BALANCE],
    /// Bookkeeping of the rightmost pointer so the offset::BTREE_RIGHTMOST_PTR can be updated.
    rightmost_pointer: *mut u8,
    /// Divider cells of old pages. We can have maximum 2 divider cells because of 3 pages.
    divider_cell_payloads: [Option<Vec<u8>>; MAX_SIBLING_PAGES_TO_BALANCE - 1],
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

impl Debug for CursorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Delete(..) => write!(f, "Delete"),
            Self::Destroy(..) => write!(f, "Destroy"),
            Self::None => write!(f, "None"),
            Self::ReadWritePayload(..) => write!(f, "ReadWritePayload"),
            Self::Write(..) => write!(f, "Write"),
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
pub enum CursorContext {
    TableRowId(i64),

    /// If we are in an index tree we can then reuse this field to save
    /// our cursor information
    IndexKeyRowId(ImmutableRecord),
}

/// In the future, we may expand these general validity states
#[derive(Debug, PartialEq, Eq)]
pub enum CursorValidState {
    /// Cursor is pointing a to an existing location/cell in the Btree
    Valid,
    /// Cursor may be pointing to a non-existent location/cell. This can happen after balancing operations
    RequireSeek,
    /// Cursor requires an advance after a seek
    RequireAdvance(IterationDirection),
}

#[derive(Debug)]
/// State used for seeking
pub enum CursorSeekState {
    Start,
    MovingBetweenPages {
        eq_seen: Cell<bool>,
    },
    InteriorPageBinarySearch {
        min_cell_idx: Cell<isize>,
        max_cell_idx: Cell<isize>,
        nearest_matching_cell: Cell<Option<usize>>,
        eq_seen: Cell<bool>,
    },
    FoundLeaf {
        eq_seen: Cell<bool>,
    },
    LeafPageBinarySearch {
        min_cell_idx: Cell<isize>,
        max_cell_idx: Cell<isize>,
        nearest_matching_cell: Cell<Option<usize>>,
        /// Indicates if we have seen an exact match during the downwards traversal of the btree.
        /// This is only needed in index seeks, in cases where we need to determine whether we call
        /// an additional next()/prev() to fetch a matching record from an interior node. We will not
        /// do that if both are true:
        /// 1. We have not seen an EQ during the traversal
        /// 2. We are looking for an exact match ([SeekOp::GE] or [SeekOp::LE] with eq_only: true)
        eq_seen: Cell<bool>,
        /// In multiple places, we do a seek that checks for an exact match (SeekOp::EQ) in the tree.
        /// In those cases, we need to know where to land if we don't find an exact match in the leaf page.
        /// For non-eq-only conditions (GT, LT, GE, LE), this is pretty simple:
        /// - If we are looking for GT/GE and don't find a match, we should end up beyond the end of the page (idx=cell count).
        /// - If we are looking for LT/LE and don't find a match, we should end up before the beginning of the page (idx=-1).
        ///
        /// For eq-only conditions (GE { eq_only: true } or LE { eq_only: true }), we need to know where to land if we don't find an exact match.
        /// For GE, we want to land at the first cell that is greater than the seek key.
        /// For LE, we want to land at the last cell that is less than the seek key.
        /// This is because e.g. when we attempt to insert rowid 666, we first check if it exists.
        /// If it doesn't, we want to land in the place where rowid 666 WOULD be inserted.
        target_cell_when_not_found: Cell<i32>,
    },
}

pub struct BTreeCursor {
    /// The multi-version cursor that is used to read and write to the database file.
    mv_cursor: Option<Rc<RefCell<MvCursor>>>,
    /// The pager that is used to read and write to the database file.
    pager: Rc<Pager>,
    /// Page id of the root page used to go back up fast.
    root_page: usize,
    /// Rowid and record are stored before being consumed.
    has_record: Cell<bool>,
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
    /// Reusable immutable record, used to allow better allocation strategy.
    parse_record_state: RefCell<ParseRecordState>,
    /// Information about the index key structure (sort order, collation, etc)
    pub index_info: Option<IndexInfo>,
    /// Maintain count of the number of records in the btree. Used for the `Count` opcode
    count: usize,
    /// Stores the cursor context before rebalancing so that a seek can be done later
    context: Option<CursorContext>,
    /// Store whether the Cursor is in a valid state. Meaning if it is pointing to a valid cell index or not
    pub valid_state: CursorValidState,
    seek_state: CursorSeekState,
    /// Separate state to read a record with overflow pages. This separation from `state` is necessary as
    /// we can be in a function that relies on `state`, but also needs to process overflow pages
    read_overflow_state: RefCell<Option<ReadPayloadOverflow>>,
    /// `RecordCursor` is used to parse SQLite record format data retrieved from B-tree
    /// leaf pages. It provides incremental parsing, only deserializing the columns that are
    /// actually accessed, which is crucial for performance when dealing with wide tables
    /// where only a subset of columns are needed.
    ///
    /// - Record parsing is logically a read operation from the caller's perspective
    /// - But internally requires updating the cursor's cached parsing state
    /// - Multiple methods may need to access different columns from the same record
    ///
    /// # Lifecycle
    ///
    /// The cursor is invalidated and reset when:
    /// - Moving to a different record/row
    /// - The underlying `ImmutableRecord` is modified
    pub record_cursor: RefCell<RecordCursor>,
    /// State machine for [BTreeCursor::is_empty_table]
    is_empty_table_state: RefCell<EmptyTableState>,
    /// State machine for [BTreeCursor::move_to_rightmost] and, optionally, the id of the rightmost page in the btree.
    /// If we know the rightmost page id and are already on that page, we can skip a seek.
    move_to_right_state: (MoveToRightState, Option<usize>),
    seek_to_last_state: SeekToLastState,
}

/// We store the cell index and cell count for each page in the stack.
/// The reason we store the cell count is because we need to know when we are at the end of the page,
/// without having to perform IO to get the ancestor pages.
#[derive(Debug, Clone, Copy, Default)]
struct BTreeNodeState {
    cell_idx: i32,
    cell_count: Option<i32>,
}

impl BTreeNodeState {
    /// Check if the current cell index is at the end of the page.
    /// This information is used to determine whether a child page should move up to its parent.
    /// If the child page is the rightmost leaf page and it has reached the end, this means all of its ancestors have
    /// already reached the end, so it should not go up because there are no more records to traverse.
    fn is_at_end(&self) -> bool {
        let cell_count = self.cell_count.expect("cell_count is not set");
        // cell_idx == cell_count means: we will traverse to the rightmost pointer next.
        // cell_idx == cell_count + 1 means: we have already gone down to the rightmost pointer.
        self.cell_idx == cell_count + 1
    }
}

impl BTreeCursor {
    pub fn new(
        mv_cursor: Option<Rc<RefCell<MvCursor>>>,
        pager: Rc<Pager>,
        root_page: usize,
        num_columns: usize,
    ) -> Self {
        Self {
            mv_cursor,
            pager,
            root_page,
            has_record: Cell::new(false),
            null_flag: false,
            going_upwards: false,
            state: CursorState::None,
            overflow_state: None,
            stack: PageStack {
                current_page: Cell::new(-1),
                node_states: RefCell::new([BTreeNodeState::default(); BTCURSOR_MAX_DEPTH + 1]),
                stack: RefCell::new([const { None }; BTCURSOR_MAX_DEPTH + 1]),
            },
            reusable_immutable_record: RefCell::new(None),
            index_info: None,
            count: 0,
            context: None,
            valid_state: CursorValidState::Valid,
            seek_state: CursorSeekState::Start,
            read_overflow_state: RefCell::new(None),
            parse_record_state: RefCell::new(ParseRecordState::Init),
            record_cursor: RefCell::new(RecordCursor::with_capacity(num_columns)),
            is_empty_table_state: RefCell::new(EmptyTableState::Start),
            move_to_right_state: (MoveToRightState::Start, None),
            seek_to_last_state: SeekToLastState::Start,
        }
    }

    pub fn new_table(
        mv_cursor: Option<Rc<RefCell<MvCursor>>>,
        pager: Rc<Pager>,
        root_page: usize,
        num_columns: usize,
    ) -> Self {
        Self::new(mv_cursor, pager, root_page, num_columns)
    }

    pub fn new_index(
        mv_cursor: Option<Rc<RefCell<MvCursor>>>,
        pager: Rc<Pager>,
        root_page: usize,
        index: &Index,
        num_columns: usize,
    ) -> Self {
        let mut cursor = Self::new(mv_cursor, pager, root_page, num_columns);
        cursor.index_info = Some(IndexInfo::new_from_index(index));
        cursor
    }

    pub fn has_rowid(&self) -> bool {
        match &self.index_info {
            Some(index_key_info) => index_key_info.has_rowid,
            None => true, // currently we don't support WITHOUT ROWID tables
        }
    }

    pub fn get_index_rowid_from_record(&self) -> Option<i64> {
        if !self.has_rowid() {
            return None;
        }
        let mut record_cursor_ref = self.record_cursor.borrow_mut();
        let record_cursor = record_cursor_ref.deref_mut();
        let rowid = match self
            .get_immutable_record()
            .as_ref()
            .unwrap()
            .last_value(record_cursor)
        {
            Some(Ok(RefValue::Integer(rowid))) => rowid,
            _ => unreachable!(
                "index where has_rowid() is true should have an integer rowid as the last value"
            ),
        };
        Some(rowid)
    }

    /// Check if the table is empty.
    /// This is done by checking if the root page has no cells.
    #[instrument(skip_all, level = Level::DEBUG)]
    fn is_empty_table(&self) -> Result<IOResult<bool>> {
        let state = self.is_empty_table_state.borrow().clone();
        match state {
            EmptyTableState::Start => {
                if let Some(mv_cursor) = &self.mv_cursor {
                    let mv_cursor = mv_cursor.borrow();
                    return Ok(IOResult::Done(mv_cursor.is_empty()));
                }
                let (page, _c) = self.pager.read_page(self.root_page)?;
                *self.is_empty_table_state.borrow_mut() = EmptyTableState::ReadPage { page };
                Ok(IOResult::IO)
            }
            EmptyTableState::ReadPage { page } => {
                // TODO: Remove this line after we start awaiting for completions
                return_if_locked!(page);
                let cell_count = page.get().contents.as_ref().unwrap().cell_count();
                Ok(IOResult::Done(cell_count == 0))
            }
        }
    }

    /// Move the cursor to the previous record and return it.
    /// Used in backwards iteration.
    #[instrument(skip(self), level = Level::DEBUG, name = "prev")]
    fn get_prev_record(&mut self) -> Result<IOResult<bool>> {
        loop {
            let page = self.stack.top();

            return_if_locked_maybe_load!(self.pager, page);
            let page = page.get();
            let contents = page.get().contents.as_ref().unwrap();
            let page_type = contents.page_type();
            let is_index = page.is_index();

            let cell_count = contents.cell_count();
            let cell_idx = self.stack.current_cell_index();

            // If we are at the end of the page and we haven't just come back from the right child,
            // we now need to move to the rightmost child.
            if self.stack.current_cell_index() == i32::MAX && !self.going_upwards {
                let rightmost_pointer = contents.rightmost_pointer();
                if let Some(rightmost_pointer) = rightmost_pointer {
                    let past_rightmost_pointer = cell_count as i32 + 1;
                    self.stack.set_cell_index(past_rightmost_pointer);
                    let (page, _c) = self.read_page(rightmost_pointer as usize)?;
                    self.stack.push_backwards(page);
                    continue;
                }
            }
            if cell_idx >= cell_count as i32 {
                self.stack.set_cell_index(cell_count as i32 - 1);
            } else if !self.stack.current_cell_index_less_than_min() {
                // skip retreat in case we still haven't visited this cell in index
                let should_visit_internal_node = is_index && self.going_upwards; // we are going upwards, this means we still need to visit divider cell in an index
                if should_visit_internal_node {
                    self.going_upwards = false;
                    return Ok(IOResult::Done(true));
                } else if matches!(
                    page_type,
                    PageType::IndexLeaf | PageType::TableLeaf | PageType::TableInterior
                ) {
                    self.stack.retreat();
                }
            }
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
                        // dbg!(false);
                        return Ok(IOResult::Done(false));
                    }
                }
                // continue to next loop to get record from the new page
                continue;
            }
            if contents.is_leaf() {
                return Ok(IOResult::Done(true));
            }

            if is_index && self.going_upwards {
                // If we are going upwards, we need to visit the divider cell before going back to another child page.
                // This is because index interior cells have payloads, so unless we do this we will be skipping an entry when traversing the tree.
                self.going_upwards = false;
                return Ok(IOResult::Done(true));
            }

            let cell_idx = self.stack.current_cell_index() as usize;
            let left_child_page = contents.cell_interior_read_left_child_page(cell_idx);

            if page_type == PageType::IndexInterior {
                // In backwards iteration, if we haven't just moved to this interior node from the
                // right child, but instead are about to move to the left child, we need to retreat
                // so that we don't come back to this node again.
                // For example:
                // this parent: key 666
                // left child has: key 663, key 664, key 665
                // we need to move to the previous parent (with e.g. key 662) when iterating backwards.
                self.stack.retreat();
            }

            let (mem_page, _c) = self.read_page(left_child_page as usize)?;
            self.stack.push_backwards(mem_page);
            continue;
        }
    }

    /// Reads the record of a cell that has overflow pages. This is a state machine that requires to be called until completion so everything
    /// that calls this function should be reentrant.
    #[instrument(skip_all, level = Level::DEBUG)]
    fn process_overflow_read(
        &self,
        payload: &'static [u8],
        start_next_page: u32,
        payload_size: u64,
    ) -> Result<IOResult<()>> {
        if self.read_overflow_state.borrow().is_none() {
            let (page, _c) = self.read_page(start_next_page as usize)?;
            *self.read_overflow_state.borrow_mut() = Some(ReadPayloadOverflow {
                payload: payload.to_vec(),
                next_page: start_next_page,
                remaining_to_read: payload_size as usize - payload.len(),
                page,
            });
            return Ok(IOResult::IO);
        }
        let mut read_overflow_state = self.read_overflow_state.borrow_mut();
        let ReadPayloadOverflow {
            payload,
            next_page,
            remaining_to_read,
            page: page_btree,
        } = read_overflow_state.as_mut().unwrap();

        if page_btree.get().is_locked() {
            return Ok(IOResult::IO);
        }
        tracing::debug!(next_page, remaining_to_read, "reading overflow page");
        let page = page_btree.get();
        let contents = page.get_contents();
        // The first four bytes of each overflow page are a big-endian integer which is the page number of the next page in the chain, or zero for the final page in the chain.
        let next = contents.read_u32_no_offset(0);
        let buf = contents.as_ptr();
        let usable_space = self.pager.usable_space();
        let to_read = (*remaining_to_read).min(usable_space - 4);
        payload.extend_from_slice(&buf[4..4 + to_read]);
        *remaining_to_read -= to_read;

        if *remaining_to_read != 0 && next != 0 {
            let (new_page, _c) = self.pager.read_page(next as usize).map(|(page, c)| {
                (
                    Arc::new(BTreePageInner {
                        page: RefCell::new(page),
                    }),
                    c,
                )
            })?;
            *page_btree = new_page;
            *next_page = next;
            return Ok(IOResult::IO);
        }
        turso_assert!(
            *remaining_to_read == 0 && next == 0,
            "we can't have more pages to read while also have read everything"
        );
        let mut payload_swap = Vec::new();
        std::mem::swap(payload, &mut payload_swap);

        let mut reuse_immutable = self.get_immutable_record_or_create();
        reuse_immutable.as_mut().unwrap().invalidate();

        reuse_immutable
            .as_mut()
            .unwrap()
            .start_serialization(&payload_swap);
        self.record_cursor.borrow_mut().invalidate();

        let _ = read_overflow_state.take();
        Ok(IOResult::Done(()))
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
        let max_local = payload_overflow_threshold_max(page_type, usable_size);
        let min_local = payload_overflow_threshold_min(page_type, usable_size);

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
    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn read_write_payload_with_offset(
        &mut self,
        mut offset: u32,
        buffer: &mut Vec<u8>,
        mut amount: u32,
        is_write: bool,
    ) -> Result<IOResult<()>> {
        if let CursorState::ReadWritePayload(PayloadOverflowWithOffset::SkipOverflowPages {
            ..
        })
        | CursorState::ReadWritePayload(PayloadOverflowWithOffset::ProcessPage { .. }) =
            &self.state
        {
            return self.continue_payload_overflow_with_offset(buffer, self.usable_space());
        }

        let page_btree = self.stack.top();
        return_if_locked_maybe_load!(self.pager, page_btree);

        let page = page_btree.get();
        let contents = page.get().contents.as_ref().unwrap();
        let cell_idx = self.stack.current_cell_index() as usize - 1;

        if cell_idx >= contents.cell_count() {
            return Err(LimboError::Corrupt("Invalid cell index".into()));
        }

        let usable_size = self.usable_space();
        let cell = contents.cell_get(cell_idx, usable_size).unwrap();

        let (payload, payload_size, first_overflow_page) = match cell {
            BTreeCell::TableLeafCell(cell) => {
                (cell.payload, cell.payload_size, cell.first_overflow_page)
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
        turso_assert!(
            offset + amount <= payload_size as u32,
            "offset + amount <= payload_size"
        );

        let (local_size, _) =
            self.parse_cell_info(payload_size as usize, contents.page_type(), usable_size)?;
        let mut bytes_processed: u32 = 0;
        if offset < local_size as u32 {
            let mut local_amount: u32 = amount;
            if local_amount + offset > local_size as u32 {
                local_amount = local_size as u32 - offset;
            }
            if is_write {
                self.write_payload_to_page(
                    offset,
                    local_amount,
                    payload,
                    buffer,
                    page_btree.clone(),
                );
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
                    page_offset,
                    amount,
                    buffer_offset: bytes_processed as usize,
                    is_write,
                });

            return Ok(IOResult::IO);
        }
        Ok(IOResult::Done(()))
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn continue_payload_overflow_with_offset(
        &mut self,
        buffer: &mut Vec<u8>,
        usable_space: usize,
    ) -> Result<IOResult<()>> {
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
                        let (page, _c) = self.read_page(*next_page as usize)?;
                        return_if_locked_maybe_load!(self.pager, page);
                        self.state =
                            CursorState::ReadWritePayload(PayloadOverflowWithOffset::ProcessPage {
                                next_page: *next_page,
                                remaining_to_read: *amount,
                                page,
                                current_offset: *page_offset as usize,
                                buffer_offset: *buffer_offset,
                                is_write: *is_write,
                            });

                        continue;
                    }

                    let (page, _c) = self.read_page(*next_page as usize)?;
                    return_if_locked_maybe_load!(self.pager, page);
                    let page = page.get();
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

                    return Ok(IOResult::IO);
                }

                CursorState::ReadWritePayload(PayloadOverflowWithOffset::ProcessPage {
                    next_page,
                    remaining_to_read,
                    page: page_btree,
                    current_offset,
                    buffer_offset,
                    is_write,
                }) => {
                    if page_btree.get().is_locked() {
                        self.state =
                            CursorState::ReadWritePayload(PayloadOverflowWithOffset::ProcessPage {
                                next_page: *next_page,
                                remaining_to_read: *remaining_to_read,
                                page: page_btree.clone(),
                                current_offset: *current_offset,
                                buffer_offset: *buffer_offset,
                                is_write: *is_write,
                            });

                        return Ok(IOResult::IO);
                    }

                    let page = page_btree.get();
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
                            page_btree.clone(),
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
                        return Ok(IOResult::Done(()));
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
                    let (page, _c) = self.read_page(next as usize)?;
                    *page_btree = page;

                    // Return IO to allow other operations
                    return Ok(IOResult::IO);
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
        buffer: &mut [u8],
        page: BTreePage,
    ) {
        self.pager.add_dirty(&page.get());
        // SAFETY: This is safe as long as the page is not evicted from the cache.
        let payload_mut =
            unsafe { std::slice::from_raw_parts_mut(payload.as_ptr() as *mut u8, payload.len()) };
        payload_mut[payload_offset as usize..payload_offset as usize + num_bytes as usize]
            .copy_from_slice(&buffer[..num_bytes as usize]);
    }

    /// Check if any ancestor pages still have cells to iterate.
    /// If not, traversing back up to parent is of no use because we are at the end of the tree.
    fn ancestor_pages_have_more_children(&self) -> bool {
        let node_states = self.stack.node_states.borrow();
        (0..self.stack.current())
            .rev()
            .any(|idx| !node_states[idx].is_at_end())
    }

    /// Move the cursor to the next record and return it.
    /// Used in forwards iteration, which is the default.
    #[instrument(skip(self), level = Level::DEBUG, name = "next")]
    fn get_next_record(&mut self) -> Result<IOResult<bool>> {
        if let Some(mv_cursor) = &self.mv_cursor {
            let mut mv_cursor = mv_cursor.borrow_mut();
            mv_cursor.forward();
            let rowid = mv_cursor.current_row_id();
            match rowid {
                Some(_rowid) => {
                    return Ok(IOResult::Done(true));
                }
                None => return Ok(IOResult::Done(false)),
            }
        }
        loop {
            let mem_page_rc = self.stack.top();
            return_if_locked_maybe_load!(self.pager, mem_page_rc);
            let mem_page = mem_page_rc.get();

            let contents = mem_page.get().contents.as_ref().unwrap();
            let cell_count = contents.cell_count();
            tracing::debug!(
                id = mem_page_rc.get().get().id,
                cell = self.stack.current_cell_index(),
                cell_count,
                "current_before_advance",
            );

            let is_index = mem_page_rc.get().is_index();
            let should_skip_advance = is_index
                && self.going_upwards // we are going upwards, this means we still need to visit divider cell in an index
                && self.stack.current_cell_index() >= 0 && self.stack.current_cell_index() < cell_count as i32; // if we weren't on a
                                                                                                                // valid cell then it means we will have to move upwards again or move to right page,
                                                                                                                // anyways, we won't visit this invalid cell index
            if should_skip_advance {
                tracing::debug!(
                    going_upwards = self.going_upwards,
                    page = mem_page_rc.get().get().id,
                    cell_idx = self.stack.current_cell_index(),
                    "skipping advance",
                );
                self.going_upwards = false;
                return Ok(IOResult::Done(true));
            }

            // Important to advance only after loading the page in order to not advance > 1 times
            self.stack.advance();
            let cell_idx = self.stack.current_cell_index() as usize;
            tracing::debug!(id = mem_page_rc.get().get().id, cell = cell_idx, "current");

            if cell_idx >= cell_count {
                let rightmost_already_traversed = cell_idx > cell_count;
                match (contents.rightmost_pointer(), rightmost_already_traversed) {
                    (Some(right_most_pointer), false) => {
                        // do rightmost
                        self.stack.advance();
                        let (mem_page, _c) = self.read_page(right_most_pointer as usize)?;
                        self.stack.push(mem_page);
                        continue;
                    }
                    _ => {
                        if self.ancestor_pages_have_more_children() {
                            tracing::trace!("moving simple upwards");
                            self.going_upwards = true;
                            self.stack.pop();
                            continue;
                        } else {
                            // If none of the ancestor pages have more children to iterate, that means we are at the end of the btree and should stop iterating.
                            return Ok(IOResult::Done(false));
                        }
                    }
                }
            }

            turso_assert!(
                cell_idx < contents.cell_count(),
                "cell index out of bounds: cell_idx={}, cell_count={}, page_type={:?} page_id={}",
                cell_idx,
                contents.cell_count(),
                contents.page_type(),
                mem_page_rc.get().get().id
            );

            if contents.is_leaf() {
                return Ok(IOResult::Done(true));
            }
            if is_index && self.going_upwards {
                // This means we just came up from a child, so now we need to visit the divider cell before going back to another child page.
                // This is because index interior cells have payloads, so unless we do this we will be skipping an entry when traversing the tree.
                self.going_upwards = false;
                return Ok(IOResult::Done(true));
            }

            let left_child_page = contents.cell_interior_read_left_child_page(cell_idx);
            let (mem_page, _c) = self.read_page(left_child_page as usize)?;
            self.stack.push(mem_page);
            continue;
        }
    }

    /// Move the cursor to the record that matches the seek key and seek operation.
    /// This may be used to seek to a specific record in a point query (e.g. SELECT * FROM table WHERE col = 10)
    /// or e.g. find the first record greater than the seek key in a range query (e.g. SELECT * FROM table WHERE col > 10).
    /// We don't include the rowid in the comparison and that's why the last value from the record is not included.
    fn do_seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<IOResult<SeekResult>> {
        let ret = return_if_io!(match key {
            SeekKey::TableRowId(rowid) => {
                self.tablebtree_seek(rowid, op)
            }
            SeekKey::IndexKey(index_key) => {
                self.indexbtree_seek(index_key, op)
            }
        });
        self.valid_state = CursorValidState::Valid;
        Ok(IOResult::Done(ret))
    }

    /// Move the cursor to the root page of the btree.
    #[instrument(skip_all, level = Level::DEBUG)]
    fn move_to_root(&mut self) -> Result<Completion> {
        self.seek_state = CursorSeekState::Start;
        self.going_upwards = false;
        tracing::trace!(root_page = self.root_page);
        let (mem_page, c) = self.read_page(self.root_page)?;
        self.stack.clear();
        self.stack.push(mem_page);
        Ok(c)
    }

    /// Move the cursor to the rightmost record in the btree.
    #[instrument(skip(self), level = Level::DEBUG)]
    fn move_to_rightmost(&mut self) -> Result<IOResult<bool>> {
        let (move_to_right_state, rightmost_page_id) = &self.move_to_right_state;
        match *move_to_right_state {
            MoveToRightState::Start => {
                if let Some(rightmost_page_id) = rightmost_page_id {
                    // If we know the rightmost page and are already on it, we can skip a seek.
                    let current_page = self.stack.top();
                    return_if_locked_maybe_load!(self.pager, current_page);
                    let current_page = current_page.get();
                    if current_page.get().id == *rightmost_page_id {
                        let contents = current_page.get_contents();
                        let cell_count = contents.cell_count();
                        self.stack.set_cell_index(cell_count as i32 - 1);
                        return Ok(IOResult::Done(cell_count > 0));
                    }
                }
                let rightmost_page_id = *rightmost_page_id;
                let _c = self.move_to_root()?;
                self.move_to_right_state = (MoveToRightState::ProcessPage, rightmost_page_id);
                return Ok(IOResult::IO);
            }
            MoveToRightState::ProcessPage => {
                let mem_page = self.stack.top();
                let page_idx = mem_page.get().get().id;
                let (page, _c) = self.read_page(page_idx)?;
                return_if_locked_maybe_load!(self.pager, page);
                let page = page.get();
                let contents = page.get().contents.as_ref().unwrap();
                if contents.is_leaf() {
                    self.move_to_right_state = (MoveToRightState::Start, Some(page_idx));
                    if contents.cell_count() > 0 {
                        self.stack.set_cell_index(contents.cell_count() as i32 - 1);
                        return Ok(IOResult::Done(true));
                    }
                    return Ok(IOResult::Done(false));
                }

                match contents.rightmost_pointer() {
                    Some(right_most_pointer) => {
                        self.stack.set_cell_index(contents.cell_count() as i32 + 1);
                        let (mem_page, _c) = self.read_page(right_most_pointer as usize)?;
                        self.stack.push(mem_page);
                        return Ok(IOResult::IO);
                    }

                    None => {
                        unreachable!("interior page should have a rightmost pointer");
                    }
                }
            }
        }
    }

    /// Specialized version of move_to() for table btrees.
    #[instrument(skip(self), level = Level::DEBUG)]
    fn tablebtree_move_to(&mut self, rowid: i64, seek_op: SeekOp) -> Result<IOResult<()>> {
        'outer: loop {
            let page = self.stack.top();
            return_if_locked_maybe_load!(self.pager, page);
            let page = page.get();
            let contents = page.get().contents.as_ref().unwrap();
            if contents.is_leaf() {
                self.seek_state = CursorSeekState::FoundLeaf {
                    eq_seen: Cell::new(false),
                };
                return Ok(IOResult::Done(()));
            }

            let cell_count = contents.cell_count();
            if matches!(
                self.seek_state,
                CursorSeekState::Start | CursorSeekState::MovingBetweenPages { .. }
            ) {
                let eq_seen = match &self.seek_state {
                    CursorSeekState::MovingBetweenPages { eq_seen } => eq_seen.get(),
                    _ => false,
                };
                let min_cell_idx = Cell::new(0);
                let max_cell_idx = Cell::new(cell_count as isize - 1);
                let nearest_matching_cell = Cell::new(None);

                self.seek_state = CursorSeekState::InteriorPageBinarySearch {
                    min_cell_idx,
                    max_cell_idx,
                    nearest_matching_cell,
                    eq_seen: Cell::new(eq_seen),
                };
            }

            let CursorSeekState::InteriorPageBinarySearch {
                min_cell_idx,
                max_cell_idx,
                nearest_matching_cell,
                eq_seen,
                ..
            } = &self.seek_state
            else {
                unreachable!("we must be in an interior binary search state");
            };

            loop {
                let min = min_cell_idx.get();
                let max = max_cell_idx.get();
                if min > max {
                    if let Some(nearest_matching_cell) = nearest_matching_cell.get() {
                        let left_child_page =
                            contents.cell_interior_read_left_child_page(nearest_matching_cell);
                        self.stack.set_cell_index(nearest_matching_cell as i32);
                        let (mem_page, _c) = self.read_page(left_child_page as usize)?;
                        self.stack.push(mem_page);
                        self.seek_state = CursorSeekState::MovingBetweenPages {
                            eq_seen: Cell::new(eq_seen.get()),
                        };
                        continue 'outer;
                    }
                    self.stack.set_cell_index(cell_count as i32 + 1);
                    match contents.rightmost_pointer() {
                        Some(right_most_pointer) => {
                            let (mem_page, _c) = self.read_page(right_most_pointer as usize)?;
                            self.stack.push(mem_page);
                            self.seek_state = CursorSeekState::MovingBetweenPages {
                                eq_seen: Cell::new(eq_seen.get()),
                            };
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
                    SeekOp::GE { .. } => cell_rowid >= rowid,
                    SeekOp::LE { .. } => cell_rowid >= rowid,
                    SeekOp::LT => cell_rowid + 1 >= rowid,
                };
                if is_on_left {
                    nearest_matching_cell.set(Some(cur_cell_idx as usize));
                    max_cell_idx.set(cur_cell_idx - 1);
                } else {
                    min_cell_idx.set(cur_cell_idx + 1);
                }
            }
        }
    }

    /// Specialized version of move_to() for index btrees.
    #[instrument(skip(self, index_key), level = Level::DEBUG)]
    fn indexbtree_move_to(
        &mut self,
        index_key: &ImmutableRecord,
        cmp: SeekOp,
    ) -> Result<IOResult<()>> {
        let iter_dir = cmp.iteration_direction();

        let key_values = index_key.get_values();
        let record_comparer = {
            let index_info = self
                .index_info
                .as_ref()
                .expect("indexbtree_move_to without index_info");
            find_compare(&key_values, index_info)
        };
        tracing::debug!("Using record comparison strategy: {:?}", record_comparer);
        let tie_breaker = get_tie_breaker_from_seek_op(cmp);

        'outer: loop {
            let page = self.stack.top();
            return_if_locked_maybe_load!(self.pager, page);
            let page = page.get();
            let contents = page.get().contents.as_ref().unwrap();
            if contents.is_leaf() {
                let eq_seen = match &self.seek_state {
                    CursorSeekState::MovingBetweenPages { eq_seen } => eq_seen.get(),
                    _ => false,
                };
                self.seek_state = CursorSeekState::FoundLeaf {
                    eq_seen: Cell::new(eq_seen),
                };
                return Ok(IOResult::Done(()));
            }

            if matches!(
                self.seek_state,
                CursorSeekState::Start | CursorSeekState::MovingBetweenPages { .. }
            ) {
                let eq_seen = match &self.seek_state {
                    CursorSeekState::MovingBetweenPages { eq_seen } => eq_seen.get(),
                    _ => false,
                };
                let cell_count = contents.cell_count();
                let min_cell_idx = Cell::new(0);
                let max_cell_idx = Cell::new(cell_count as isize - 1);
                let nearest_matching_cell = Cell::new(None);

                self.seek_state = CursorSeekState::InteriorPageBinarySearch {
                    min_cell_idx,
                    max_cell_idx,
                    nearest_matching_cell,
                    eq_seen: Cell::new(eq_seen),
                };
            }

            let CursorSeekState::InteriorPageBinarySearch {
                min_cell_idx,
                max_cell_idx,
                nearest_matching_cell,
                eq_seen,
            } = &self.seek_state
            else {
                unreachable!(
                    "we must be in an interior binary search state, got {:?}",
                    self.seek_state
                );
            };

            loop {
                let min = min_cell_idx.get();
                let max = max_cell_idx.get();
                if min > max {
                    let Some(leftmost_matching_cell) = nearest_matching_cell.get() else {
                        self.stack.set_cell_index(contents.cell_count() as i32 + 1);
                        match contents.rightmost_pointer() {
                            Some(right_most_pointer) => {
                                let (mem_page, _c) = self.read_page(right_most_pointer as usize)?;
                                self.stack.push(mem_page);
                                self.seek_state = CursorSeekState::MovingBetweenPages {
                                    eq_seen: Cell::new(eq_seen.get()),
                                };
                                continue 'outer;
                            }
                            None => {
                                unreachable!(
                                    "we shall not go back up! The only way is down the slope"
                                );
                            }
                        }
                    };
                    let matching_cell =
                        contents.cell_get(leftmost_matching_cell, self.usable_space())?;
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

                    turso_assert!(
                        page.get().id != *left_child_page as usize,
                        "corrupt: current page and left child page of cell {} are both {}",
                        leftmost_matching_cell,
                        page.get().id
                    );

                    let (mem_page, _c) = self.read_page(*left_child_page as usize)?;
                    self.stack.push(mem_page);
                    self.seek_state = CursorSeekState::MovingBetweenPages {
                        eq_seen: Cell::new(eq_seen.get()),
                    };
                    continue 'outer;
                }

                let cur_cell_idx = (min + max) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
                self.stack.set_cell_index(cur_cell_idx as i32);
                let cell = contents.cell_get(cur_cell_idx as usize, self.usable_space())?;
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
                    self.get_immutable_record_or_create()
                        .as_mut()
                        .unwrap()
                        .invalidate();
                    self.get_immutable_record_or_create()
                        .as_mut()
                        .unwrap()
                        .start_serialization(payload);
                    self.record_cursor.borrow_mut().invalidate();
                };
                let (target_leaf_page_is_in_left_subtree, is_eq) = {
                    let record = self.get_immutable_record();
                    let record = record.as_ref().unwrap();

                    let interior_cell_vs_index_key = record_comparer
                        .compare(
                            record,
                            &key_values,
                            self.index_info
                                .as_ref()
                                .expect("indexbtree_move_to without index_info"),
                            0,
                            tie_breaker,
                        )
                        .unwrap();

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

                    (
                        match cmp {
                            SeekOp::GT => interior_cell_vs_index_key.is_gt(),
                            SeekOp::GE { .. } => interior_cell_vs_index_key.is_ge(),
                            SeekOp::LE { .. } => interior_cell_vs_index_key.is_gt(),
                            SeekOp::LT => interior_cell_vs_index_key.is_ge(),
                        },
                        interior_cell_vs_index_key.is_eq(),
                    )
                };

                if is_eq {
                    eq_seen.set(true);
                }

                if target_leaf_page_is_in_left_subtree {
                    nearest_matching_cell.set(Some(cur_cell_idx as usize));
                    max_cell_idx.set(cur_cell_idx - 1);
                } else {
                    min_cell_idx.set(cur_cell_idx + 1);
                }
            }
        }
    }

    /// Specialized version of do_seek() for table btrees that uses binary search instead
    /// of iterating cells in order.
    #[instrument(skip_all, level = Level::DEBUG)]
    fn tablebtree_seek(&mut self, rowid: i64, seek_op: SeekOp) -> Result<IOResult<SeekResult>> {
        turso_assert!(
            self.mv_cursor.is_none(),
            "attempting to seek with MV cursor"
        );
        let iter_dir = seek_op.iteration_direction();

        if matches!(
            self.seek_state,
            CursorSeekState::Start
                | CursorSeekState::MovingBetweenPages { .. }
                | CursorSeekState::InteriorPageBinarySearch { .. }
        ) {
            // No need for another move_to_root. Move_to already moves to root
            return_if_io!(self.move_to(SeekKey::TableRowId(rowid), seek_op));
            let page = self.stack.top();
            return_if_locked_maybe_load!(self.pager, page);
            let page = page.get();
            let contents = page.get().contents.as_ref().unwrap();
            turso_assert!(
                contents.is_leaf(),
                "tablebtree_seek() called on non-leaf page"
            );

            let cell_count = contents.cell_count();
            if cell_count == 0 {
                self.stack.set_cell_index(0);
                return Ok(IOResult::Done(SeekResult::NotFound));
            }
            let min_cell_idx = Cell::new(0);
            let max_cell_idx = Cell::new(cell_count as isize - 1);

            // If iter dir is forwards, we want the first cell that matches;
            // If iter dir is backwards, we want the last cell that matches.
            let nearest_matching_cell = Cell::new(None);

            self.seek_state = CursorSeekState::LeafPageBinarySearch {
                min_cell_idx,
                max_cell_idx,
                nearest_matching_cell,
                eq_seen: Cell::new(false), // not relevant for table btrees
                target_cell_when_not_found: Cell::new(match seek_op.iteration_direction() {
                    IterationDirection::Forwards => cell_count as i32,
                    IterationDirection::Backwards => -1,
                }),
            };
        }

        let CursorSeekState::LeafPageBinarySearch {
            min_cell_idx,
            max_cell_idx,
            nearest_matching_cell,
            target_cell_when_not_found,
            ..
        } = &self.seek_state
        else {
            unreachable!("we must be in a leaf binary search state");
        };

        let page = self.stack.top();
        return_if_locked_maybe_load!(self.pager, page);
        let page = page.get();
        let contents = page.get().contents.as_ref().unwrap();

        loop {
            let min = min_cell_idx.get();
            let max = max_cell_idx.get();
            if min > max {
                if let Some(nearest_matching_cell) = nearest_matching_cell.get() {
                    self.stack.set_cell_index(nearest_matching_cell as i32);
                    self.has_record.set(true);
                    return Ok(IOResult::Done(SeekResult::Found));
                } else {
                    // if !eq_only - matching entry can exist in neighbour leaf page
                    // this can happen if key in the interiour page was deleted - but divider kept untouched
                    // in such case BTree can navigate to the leaf which no longer has matching key for seek_op
                    // in this case, caller must advance cursor if necessary
                    return Ok(IOResult::Done(if seek_op.eq_only() {
                        let has_record = target_cell_when_not_found.get() >= 0
                            && target_cell_when_not_found.get() < contents.cell_count() as i32;
                        self.has_record.set(has_record);
                        self.stack.set_cell_index(target_cell_when_not_found.get());
                        SeekResult::NotFound
                    } else {
                        // set cursor to the position where which would hold the op-boundary if it were present
                        self.stack.set_cell_index(target_cell_when_not_found.get());
                        SeekResult::TryAdvance
                    }));
                };
            }

            let cur_cell_idx = (min + max) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
            let cell_rowid = contents.cell_table_leaf_read_rowid(cur_cell_idx as usize)?;

            let cmp = cell_rowid.cmp(&rowid);

            let found = match seek_op {
                SeekOp::GT => cmp.is_gt(),
                SeekOp::GE { eq_only: true } => cmp.is_eq(),
                SeekOp::GE { eq_only: false } => cmp.is_ge(),
                SeekOp::LE { eq_only: true } => cmp.is_eq(),
                SeekOp::LE { eq_only: false } => cmp.is_le(),
                SeekOp::LT => cmp.is_lt(),
            };

            // rowids are unique, so we can return the rowid immediately
            if found && seek_op.eq_only() {
                self.stack.set_cell_index(cur_cell_idx as i32);
                self.has_record.set(true);
                return Ok(IOResult::Done(SeekResult::Found));
            }

            if found {
                nearest_matching_cell.set(Some(cur_cell_idx as usize));
                match iter_dir {
                    IterationDirection::Forwards => {
                        max_cell_idx.set(cur_cell_idx - 1);
                    }
                    IterationDirection::Backwards => {
                        min_cell_idx.set(cur_cell_idx + 1);
                    }
                }
            } else if cmp.is_gt() {
                if matches!(seek_op, SeekOp::GE { eq_only: true }) {
                    target_cell_when_not_found
                        .set(target_cell_when_not_found.get().min(cur_cell_idx as i32));
                }
                max_cell_idx.set(cur_cell_idx - 1);
            } else if cmp.is_lt() {
                if matches!(seek_op, SeekOp::LE { eq_only: true }) {
                    target_cell_when_not_found
                        .set(target_cell_when_not_found.get().max(cur_cell_idx as i32));
                }
                min_cell_idx.set(cur_cell_idx + 1);
            } else {
                match iter_dir {
                    IterationDirection::Forwards => {
                        min_cell_idx.set(cur_cell_idx + 1);
                    }
                    IterationDirection::Backwards => {
                        max_cell_idx.set(cur_cell_idx - 1);
                    }
                }
            }
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn indexbtree_seek(
        &mut self,
        key: &ImmutableRecord,
        seek_op: SeekOp,
    ) -> Result<IOResult<SeekResult>> {
        let key_values = key.get_values();
        let record_comparer = {
            let index_info = self
                .index_info
                .as_ref()
                .expect("indexbtree_seek without index_info");
            find_compare(&key_values, index_info)
        };

        tracing::debug!(
            "Using record comparison strategy for seek: {:?}",
            record_comparer
        );

        if matches!(
            self.seek_state,
            CursorSeekState::Start
                | CursorSeekState::MovingBetweenPages { .. }
                | CursorSeekState::InteriorPageBinarySearch { .. }
        ) {
            // No need for another move_to_root. Move_to already moves to root
            return_if_io!(self.move_to(SeekKey::IndexKey(key), seek_op));
            let CursorSeekState::FoundLeaf { eq_seen } = &self.seek_state else {
                unreachable!(
                    "We must still be in FoundLeaf state after move_to, got: {:?}",
                    self.seek_state
                );
            };
            let eq_seen = eq_seen.get();
            let page = self.stack.top();
            return_if_locked_maybe_load!(self.pager, page);

            let page = page.get();
            let contents = page.get().contents.as_ref().unwrap();
            let cell_count = contents.cell_count();
            if cell_count == 0 {
                return Ok(IOResult::Done(SeekResult::NotFound));
            }

            let min = Cell::new(0);
            let max = Cell::new(cell_count as isize - 1);

            // If iter dir is forwards, we want the first cell that matches;
            // If iter dir is backwards, we want the last cell that matches.
            let nearest_matching_cell = Cell::new(None);

            self.seek_state = CursorSeekState::LeafPageBinarySearch {
                min_cell_idx: min,
                max_cell_idx: max,
                nearest_matching_cell,
                eq_seen: Cell::new(eq_seen),
                target_cell_when_not_found: Cell::new(match seek_op.iteration_direction() {
                    IterationDirection::Forwards => cell_count as i32,
                    IterationDirection::Backwards => -1,
                }),
            };
        }

        let CursorSeekState::LeafPageBinarySearch {
            min_cell_idx,
            max_cell_idx,
            nearest_matching_cell,
            eq_seen,
            target_cell_when_not_found,
        } = &self.seek_state
        else {
            unreachable!(
                "we must be in a leaf binary search state, got: {:?}",
                self.seek_state
            );
        };

        let page = self.stack.top();
        return_if_locked_maybe_load!(self.pager, page);
        let page = page.get();
        let contents = page.get().contents.as_ref().unwrap();

        let iter_dir = seek_op.iteration_direction();

        loop {
            let min = min_cell_idx.get();
            let max = max_cell_idx.get();
            if min > max {
                if let Some(nearest_matching_cell) = nearest_matching_cell.get() {
                    self.stack.set_cell_index(nearest_matching_cell as i32);
                    self.has_record.set(true);
                    return Ok(IOResult::Done(SeekResult::Found));
                } else {
                    // set cursor to the position where which would hold the op-boundary if it were present
                    let target_cell = target_cell_when_not_found.get();
                    self.stack.set_cell_index(target_cell);
                    let has_record = target_cell >= 0 && target_cell < contents.cell_count() as i32;
                    self.has_record.set(has_record);

                    // Similar logic as in tablebtree_seek(), but for indexes.
                    // The difference is that since index keys are not necessarily unique, we need to TryAdvance
                    // even when eq_only=true and we have seen an EQ match up in the tree in an interior node.
                    if seek_op.eq_only() && !eq_seen.get() {
                        return Ok(IOResult::Done(SeekResult::NotFound));
                    }
                    return Ok(IOResult::Done(SeekResult::TryAdvance));
                };
            }

            let cur_cell_idx = (min + max) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
            self.stack.set_cell_index(cur_cell_idx as i32);

            let cell = contents.cell_get(cur_cell_idx as usize, self.usable_space())?;
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
                self.get_immutable_record_or_create()
                    .as_mut()
                    .unwrap()
                    .invalidate();
                self.get_immutable_record_or_create()
                    .as_mut()
                    .unwrap()
                    .start_serialization(payload);

                self.record_cursor.borrow_mut().invalidate();
            };
            let (cmp, found) = self.compare_with_current_record(
                key_values.as_slice(),
                seek_op,
                &record_comparer,
                self.index_info
                    .as_ref()
                    .expect("indexbtree_seek without index_info"),
            );
            if found {
                nearest_matching_cell.set(Some(cur_cell_idx as usize));
                match iter_dir {
                    IterationDirection::Forwards => {
                        max_cell_idx.set(cur_cell_idx - 1);
                    }
                    IterationDirection::Backwards => {
                        min_cell_idx.set(cur_cell_idx + 1);
                    }
                }
            } else if cmp.is_gt() {
                if matches!(seek_op, SeekOp::GE { eq_only: true }) {
                    target_cell_when_not_found
                        .set(target_cell_when_not_found.get().min(cur_cell_idx as i32));
                }
                max_cell_idx.set(cur_cell_idx - 1);
            } else if cmp.is_lt() {
                if matches!(seek_op, SeekOp::LE { eq_only: true }) {
                    target_cell_when_not_found
                        .set(target_cell_when_not_found.get().max(cur_cell_idx as i32));
                }
                min_cell_idx.set(cur_cell_idx + 1);
            } else {
                match iter_dir {
                    IterationDirection::Forwards => {
                        min_cell_idx.set(cur_cell_idx + 1);
                    }
                    IterationDirection::Backwards => {
                        max_cell_idx.set(cur_cell_idx - 1);
                    }
                }
            }
        }
    }

    fn compare_with_current_record(
        &self,
        key_values: &[RefValue],
        seek_op: SeekOp,
        record_comparer: &RecordCompare,
        index_info: &IndexInfo,
    ) -> (Ordering, bool) {
        let record = self.get_immutable_record();
        let record = record.as_ref().unwrap();

        let tie_breaker = get_tie_breaker_from_seek_op(seek_op);
        let cmp = record_comparer
            .compare(record, key_values, index_info, 0, tie_breaker)
            .unwrap();

        let found = match seek_op {
            SeekOp::GT => cmp.is_gt(),
            SeekOp::GE { eq_only: true } => cmp.is_eq(),
            SeekOp::GE { eq_only: false } => cmp.is_ge(),
            SeekOp::LE { eq_only: true } => cmp.is_eq(),
            SeekOp::LE { eq_only: false } => cmp.is_le(),
            SeekOp::LT => cmp.is_lt(),
        };

        (cmp, found)
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn move_to(&mut self, key: SeekKey<'_>, cmp: SeekOp) -> Result<IOResult<()>> {
        turso_assert!(
            self.mv_cursor.is_none(),
            "attempting to move with MV cursor"
        );
        tracing::trace!(?key, ?cmp);
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

        // If we are at the beginning/end of seek state, start a new move from the root.
        if matches!(
            self.seek_state,
            // these are stages that happen at the leaf page, so we can consider that the previous seek finished and we can start a new one.
            CursorSeekState::LeafPageBinarySearch { .. } | CursorSeekState::FoundLeaf { .. }
        ) {
            self.seek_state = CursorSeekState::Start;
        }
        if matches!(self.seek_state, CursorSeekState::Start) {
            let _c = self.move_to_root()?;
        }

        let ret = match key {
            SeekKey::TableRowId(rowid_key) => self.tablebtree_move_to(rowid_key, cmp),
            SeekKey::IndexKey(index_key) => self.indexbtree_move_to(index_key, cmp),
        };
        return_if_io!(ret);
        Ok(IOResult::Done(()))
    }

    /// Insert a record into the btree.
    /// If the insert operation overflows the page, it will be split and the btree will be balanced.
    #[instrument(skip_all, level = Level::DEBUG)]
    fn insert_into_page(&mut self, bkey: &BTreeKey) -> Result<IOResult<()>> {
        let record = bkey
            .get_record()
            .expect("expected record present on insert");
        let record_values = record.get_values();
        if let CursorState::None = &self.state {
            self.state = CursorState::Write(WriteInfo::new());
        }
        let ret = loop {
            let write_state = {
                let write_info = self
                    .state
                    .mut_write_info()
                    .expect("can't insert while counting");
                write_info.state.clone()
            };
            match write_state {
                WriteState::Start => {
                    let page = self.stack.top();
                    return_if_locked_maybe_load!(self.pager, page);

                    // get page and find cell
                    let cell_idx = {
                        return_if_locked!(page.get());
                        let page = page.get();

                        self.pager.add_dirty(&page);

                        self.stack.current_cell_index()
                    };
                    if cell_idx == -1 {
                        // This might be a brand new table and the cursor hasn't moved yet. Let's advance it to the first slot.
                        self.stack.set_cell_index(0);
                    }
                    let cell_idx = self.stack.current_cell_index() as usize;
                    tracing::debug!(cell_idx);

                    // if the cell index is less than the total cells, check: if its an existing
                    // rowid, we are going to update / overwrite the cell
                    if cell_idx < page.get().get_contents().cell_count() {
                        let cell = page
                            .get()
                            .get_contents()
                            .cell_get(cell_idx, self.usable_space())?;
                        match cell {
                            BTreeCell::TableLeafCell(tbl_leaf) => {
                                if tbl_leaf.rowid == bkey.to_rowid() {
                                    tracing::debug!("TableLeafCell: found exact match with cell_idx={cell_idx}, overwriting");
                                    self.has_record.set(true);
                                    let write_info = self
                                        .state
                                        .mut_write_info()
                                        .expect("expected write info");
                                    write_info.state = WriteState::Overwrite {
                                        page: page.clone(),
                                        cell_idx,
                                        state: OverwriteCellState::AllocatePayload,
                                    };
                                    continue;
                                }
                            }
                            BTreeCell::IndexLeafCell(..) | BTreeCell::IndexInteriorCell(..) => {
                                return_if_io!(self.record());
                                let cmp = compare_immutable(
                                    record_values.as_slice(),
                                    self.get_immutable_record()
                                        .as_ref()
                                        .unwrap()
                                        .get_values().as_slice(),
                                        &self.index_info.as_ref().unwrap().key_info,
                                );
                                if cmp == Ordering::Equal {
                                    tracing::debug!("IndexLeafCell: found exact match with cell_idx={cell_idx}, overwriting");
                                    self.has_record.set(true);
                                    let write_info = self
                                        .state
                                        .mut_write_info()
                                        .expect("expected write info");
                                    write_info.state = WriteState::Overwrite {
                                        page: page.clone(),
                                        cell_idx,
                                        state: OverwriteCellState::AllocatePayload,
                                    };
                                    continue;
                                } else {
                                    turso_assert!(
                                        !matches!(cell, BTreeCell::IndexInteriorCell(..)),
                                         "we should not be inserting a new index interior cell. the only valid operation on an index interior cell is an overwrite!"
                                    );
                                }
                            }
                            other => panic!("unexpected cell type, expected TableLeaf or IndexLeaf, found: {other:?}"),
                        }
                    }

                    let write_info = self
                        .state
                        .mut_write_info()
                        .expect("write info should be present");
                    write_info.state = WriteState::Insert {
                        page: page.clone(),
                        cell_idx,
                        new_payload: Vec::with_capacity(record_values.len() + 4),
                        fill_cell_payload_state: FillCellPayloadState::Start,
                    };
                    continue;
                }
                WriteState::Insert {
                    page,
                    cell_idx,
                    mut new_payload,
                    mut fill_cell_payload_state,
                } => {
                    return_if_io!(fill_cell_payload(
                        page.get().get().contents.as_ref().unwrap(),
                        bkey.maybe_rowid(),
                        &mut new_payload,
                        cell_idx,
                        record,
                        self.usable_space(),
                        self.pager.clone(),
                        &mut fill_cell_payload_state,
                    ));

                    {
                        let page = page.get();
                        let contents = page.get().contents.as_mut().unwrap();
                        tracing::debug!(name: "overflow", cell_count = contents.cell_count());

                        insert_into_cell(
                            contents,
                            new_payload.as_slice(),
                            cell_idx,
                            self.usable_space() as u16,
                        )?;
                    };
                    self.stack.set_cell_index(cell_idx as i32);
                    let write_info = self
                        .state
                        .mut_write_info()
                        .expect("write info should be present");
                    let overflows = !page.get().get_contents().overflow_cells.is_empty();
                    if overflows {
                        write_info.state = WriteState::BalanceStart;
                        // If we balance, we must save the cursor position and seek to it later.
                        // FIXME: we shouldn't have both DeleteState::SeekAfterBalancing and
                        // save_context()/restore/context(), they are practically the same thing.
                        self.save_context(match bkey {
                            BTreeKey::TableRowId(rowid) => CursorContext::TableRowId(rowid.0),
                            BTreeKey::IndexKey(record) => {
                                CursorContext::IndexKeyRowId((*record).clone())
                            }
                        });
                    } else {
                        write_info.state = WriteState::Finish;
                    }
                    continue;
                }
                WriteState::Overwrite {
                    page,
                    cell_idx,
                    mut state,
                } => {
                    turso_assert!(
                        page.get().is_loaded(),
                        "page {}is not loaded",
                        page.get().get().id
                    );
                    if matches!(
                        self.overwrite_cell(page.clone(), cell_idx, record, &mut state)?,
                        IOResult::IO
                    ) {
                        let write_info = self
                            .state
                            .mut_write_info()
                            .expect("write info should be present");
                        let WriteState::Overwrite {
                            state: old_state, ..
                        } = &mut write_info.state
                        else {
                            panic!("expected overwrite state");
                        };
                        *old_state = state;
                        return Ok(IOResult::IO);
                    }
                    let usable_space = self.usable_space();
                    let write_info = self
                        .state
                        .mut_write_info()
                        .expect("write info should be present");
                    let overflows = !page.get().get_contents().overflow_cells.is_empty();
                    let underflows = !overflows && {
                        let free_space =
                            compute_free_space(page.get().get_contents(), usable_space as u16);
                        free_space as usize * 3 > usable_space * 2
                    };
                    if overflows || underflows {
                        write_info.state = WriteState::BalanceStart;
                        // If we balance, we must save the cursor position and seek to it later.
                        // FIXME: we shouldn't have both DeleteState::SeekAfterBalancing and
                        // save_context()/restore/context(), they are practically the same thing.
                        self.save_context(match bkey {
                            BTreeKey::TableRowId(rowid) => CursorContext::TableRowId(rowid.0),
                            BTreeKey::IndexKey(record) => {
                                CursorContext::IndexKeyRowId((*record).clone())
                            }
                        });
                    } else {
                        write_info.state = WriteState::Finish;
                    }
                    continue;
                }
                WriteState::BalanceStart
                | WriteState::BalanceFreePages { .. }
                | WriteState::BalanceNonRootPickSiblings
                | WriteState::BalanceNonRootDoBalancing => {
                    return_if_io!(self.balance(None));
                }
                WriteState::Finish => {
                    break Ok(IOResult::Done(()));
                }
            };
        };
        if matches!(self.state.write_info().unwrap().state, WriteState::Finish) {
            // if there was a balance triggered, the cursor position is invalid.
            // it's probably not the greatest idea in the world to do this eagerly here,
            // but at least it works.
            return_if_io!(self.restore_context());
        }
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
    ///
    /// `balance_ancestor_at_depth` specifies whether to balance an ancestor page at a specific depth.
    /// If `None`, balancing stops when a level is encountered that doesn't need balancing.
    /// If `Some(depth)`, the page on the stack at depth `depth` will be rebalanced after balancing the current page.
    #[instrument(skip(self), level = Level::DEBUG)]
    fn balance(&mut self, balance_ancestor_at_depth: Option<usize>) -> Result<IOResult<()>> {
        turso_assert!(
            matches!(self.state, CursorState::Write(_)),
            "Cursor must be in balancing state"
        );

        loop {
            let state = self
                .state
                .write_info()
                .expect("must be balancing")
                .state
                .clone();
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
                    let next_balance_depth =
                        balance_ancestor_at_depth.unwrap_or(self.stack.current());
                    {
                        // check if we don't need to balance
                        // don't continue if:
                        // - current page is not overfull root
                        // OR
                        // - current page is not overfull and the amount of free space on the page
                        // is less than 2/3rds of the total usable space on the page
                        //
                        // https://github.com/sqlite/sqlite/blob/0aa95099f5003dc99f599ab77ac0004950b281ef/src/btree.c#L9064-L9071
                        let current_page = current_page.get();
                        let page = current_page.get().contents.as_mut().unwrap();
                        let usable_space = self.usable_space();
                        let free_space = compute_free_space(page, usable_space as u16);
                        let this_level_is_already_balanced = page.overflow_cells.is_empty()
                            && (!self.stack.has_parent()
                                || free_space as usize * 3 <= usable_space * 2);
                        if this_level_is_already_balanced {
                            if self.stack.current() > next_balance_depth {
                                while self.stack.current() > next_balance_depth {
                                    // Even though this level is already balanced, we know there's an upper level that needs balancing.
                                    // So we pop the stack and continue.
                                    self.stack.pop();
                                }
                                continue;
                            }
                            // Otherwise, we're done.
                            let write_info = self.state.mut_write_info().unwrap();
                            write_info.state = WriteState::Finish;
                            return Ok(IOResult::Done(()));
                        }
                    }

                    if !self.stack.has_parent() {
                        let _res = self.balance_root()?;
                    }

                    let write_info = self.state.mut_write_info().unwrap();
                    write_info.state = WriteState::BalanceNonRootPickSiblings;
                    self.stack.pop();
                    return_if_io!(self.balance_non_root());
                }
                WriteState::BalanceNonRootPickSiblings
                | WriteState::BalanceNonRootDoBalancing
                | WriteState::BalanceFreePages { .. } => {
                    return_if_io!(self.balance_non_root());
                }
                WriteState::Finish => return Ok(IOResult::Done(())),
                _ => panic!("unexpected state on balance {state:?}"),
            }
        }
    }

    /// Balance a non root page by trying to balance cells between a maximum of 3 siblings that should be neighboring the page that overflowed/underflowed.
    #[instrument(skip_all, level = Level::DEBUG)]
    fn balance_non_root(&mut self) -> Result<IOResult<()>> {
        turso_assert!(
            matches!(self.state, CursorState::Write(_)),
            "Cursor must be in balancing state"
        );
        let state = self
            .state
            .write_info()
            .expect("must be balancing")
            .state
            .clone();
        tracing::debug!(?state);
        let (next_write_state, result) = match state {
            WriteState::Start
            | WriteState::Overwrite { .. }
            | WriteState::Insert { .. }
            | WriteState::BalanceStart
            | WriteState::Finish => panic!("balance_non_root: unexpected state {state:?}"),
            WriteState::BalanceNonRootPickSiblings => {
                // Since we are going to change the btree structure, let's forget our cached knowledge of the rightmost page.
                let _ = self.move_to_right_state.1.take();
                let parent_page = self.stack.top();
                return_if_locked_maybe_load!(self.pager, parent_page);
                let parent_page = parent_page.get();
                let parent_contents = parent_page.get_contents();
                let page_type = parent_contents.page_type();
                turso_assert!(
                    matches!(page_type, PageType::IndexInterior | PageType::TableInterior),
                    "expected index or table interior page"
                );
                let number_of_cells_in_parent =
                    parent_contents.cell_count() + parent_contents.overflow_cells.len();

                // If `seek` moved to rightmost page, cell index will be out of bounds. Meaning cell_count+1.
                // In any other case, `seek` will stay in the correct index.
                let past_rightmost_pointer =
                    self.stack.current_cell_index() as usize == number_of_cells_in_parent + 1;
                if past_rightmost_pointer {
                    self.stack.retreat();
                } else if !parent_contents.overflow_cells.is_empty() {
                    // The ONLY way we can have an overflow cell in the parent is if we replaced an interior cell from a cell in the child, and that replacement did not fit.
                    // This can only happen on index btrees.
                    if matches!(page_type, PageType::IndexInterior) {
                        turso_assert!(parent_contents.overflow_cells.len() == 1, "index interior page must have no more than 1 overflow cell, as a result of InteriorNodeReplacement");
                    } else {
                        turso_assert!(false, "{page_type:?} must have no overflow cells");
                    }
                    let overflow_cell = parent_contents.overflow_cells.first().unwrap();
                    let parent_page_cell_idx = self.stack.current_cell_index() as usize;
                    // Parent page must be positioned at the divider cell that overflowed due to the replacement.
                    turso_assert!(
                        overflow_cell.index == parent_page_cell_idx,
                        "overflow cell index must be the result of InteriorNodeReplacement that leaves both child and parent (id={}) unbalanced, and hence parent page's position must = overflow_cell.index. Instead got: parent_page_cell_idx={parent_page_cell_idx} overflow_cell.index={}",
                        parent_page.get().id,
                        overflow_cell.index
                    );
                }
                self.pager.add_dirty(&parent_page);
                let parent_contents = parent_page.get().contents.as_ref().unwrap();
                let page_to_balance_idx = self.stack.current_cell_index() as usize;

                tracing::debug!(
                    "balance_non_root(parent_id={} page_to_balance_idx={})",
                    parent_page.get().id,
                    page_to_balance_idx
                );
                // Part 1: Find the sibling pages to balance
                let mut pages_to_balance: [Option<BTreePage>; MAX_SIBLING_PAGES_TO_BALANCE] =
                    [const { None }; MAX_SIBLING_PAGES_TO_BALANCE];
                turso_assert!(
                    page_to_balance_idx <= parent_contents.cell_count(),
                    "page_to_balance_idx={page_to_balance_idx} is out of bounds for parent cell count {number_of_cells_in_parent}"
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
                    let max_overflow_cells = if matches!(page_type, PageType::IndexInterior) {
                        1
                    } else {
                        0
                    };
                    turso_assert!(
                        parent_contents.overflow_cells.len() <= max_overflow_cells,
                        "must have at most {max_overflow_cells} overflow cell in the parent"
                    );
                    // OVERFLOW CELL ADJUSTMENT:
                    // Let there be parent with cells [0,1,2,3,4].
                    // Let's imagine the cell at idx 2 gets replaced with a new payload that causes it to overflow.
                    // See handling of InteriorNodeReplacement in btree.rs.
                    //
                    // In this case the rightmost divider is going to be 3 (2 is the middle one and we pick neighbors 1-3).
                    // drop_cell(): [0,1,2,3,4] -> [0,1,3,4]   <-- cells on right side get shifted left!
                    // insert_into_cell(): [0,1,3,4] -> [0,1,3,4] + overflow cell (2)  <-- crucially, no physical shifting happens, overflow cell is stored separately
                    //
                    // This means '3' is actually physically located at index '2'.
                    // So IF the parent has an overflow cell, we need to subtract 1 to get the actual rightmost divider cell idx to physically read from.
                    // The formula for the actual cell idx is:
                    // first_cell_divider + sibling_pointer - parent_contents.overflow_cells.len()
                    // so in the above case:
                    // actual_cell_idx = 1 + 2 - 1 = 2
                    //
                    // In the case where the last divider cell is the overflow cell, there would be no left-shifting of cells in drop_cell(),
                    // because they are still positioned correctly (imagine .pop() from a vector).
                    // However, note that we are always looking for the _rightmost_ child page pointer between the (max 2) dividers, and for any case where the last divider cell is the overflow cell,
                    // the 'last_sibling_is_right_pointer' condition will also be true (since the overflow cell's left child will be the middle page), so we won't enter this code branch.
                    //
                    // Hence: when we enter this branch with overflow_cells.len() == 1, we know that left-shifting has happened and we need to subtract 1.
                    let actual_cell_idx =
                        first_cell_divider + sibling_pointer - parent_contents.overflow_cells.len();
                    let (start_of_cell, _) =
                        parent_contents.cell_get_raw_region(actual_cell_idx, self.usable_space());
                    let buf = parent_contents.as_ptr().as_mut_ptr();
                    unsafe { buf.add(start_of_cell) }
                };

                // load sibling pages
                // start loading right page first
                let mut pgno: u32 = unsafe { right_pointer.cast::<u32>().read().swap_bytes() };
                let current_sibling = sibling_pointer;
                for i in (0..=current_sibling).rev() {
                    let (page, _c) = self.read_page(pgno as usize)?;
                    {
                        // mark as dirty
                        let sibling_page = page.get();
                        self.pager.add_dirty(&sibling_page);
                    }
                    #[cfg(debug_assertions)]
                    {
                        return_if_locked!(page.get());
                        debug_validate_cells!(
                            &page.get().get_contents(),
                            self.usable_space() as u16
                        );
                    }
                    pages_to_balance[i].replace(page);
                    if i == 0 {
                        break;
                    }
                    let next_cell_divider = i + first_cell_divider - 1;
                    let divider_is_overflow_cell = parent_contents
                        .overflow_cells
                        .first()
                        .is_some_and(|overflow_cell| overflow_cell.index == next_cell_divider);
                    if divider_is_overflow_cell {
                        turso_assert!(
                            matches!(parent_contents.page_type(), PageType::IndexInterior),
                            "expected index interior page, got {:?}",
                            parent_contents.page_type()
                        );
                        turso_assert!(
                            parent_contents.overflow_cells.len() == 1,
                            "must have a single overflow cell in the parent, as a result of InteriorNodeReplacement"
                        );
                        let overflow_cell = parent_contents.overflow_cells.first().unwrap();
                        pgno = u32::from_be_bytes(overflow_cell.payload[0..4].try_into().unwrap());
                    } else {
                        // grep for 'OVERFLOW CELL ADJUSTMENT' for explanation.
                        // here we only subtract 1 if the divider cell has been shifted left, i.e. the overflow cell was placed to the left
                        // this cell.
                        let actual_cell_idx =
                            if let Some(overflow_cell) = parent_contents.overflow_cells.first() {
                                if next_cell_divider < overflow_cell.index {
                                    next_cell_divider
                                } else {
                                    next_cell_divider - 1
                                }
                            } else {
                                next_cell_divider
                            };
                        pgno =
                            match parent_contents.cell_get(actual_cell_idx, self.usable_space())? {
                                BTreeCell::TableInteriorCell(TableInteriorCell {
                                    left_child_page,
                                    ..
                                })
                                | BTreeCell::IndexInteriorCell(IndexInteriorCell {
                                    left_child_page,
                                    ..
                                }) => left_child_page,
                                other => {
                                    crate::bail_corrupt_error!(
                                        "expected interior cell, got {:?}",
                                        other
                                    )
                                }
                            };
                    }
                }

                #[cfg(debug_assertions)]
                {
                    let page_type_of_siblings = pages_to_balance[0]
                        .as_ref()
                        .unwrap()
                        .get()
                        .get_contents()
                        .page_type();
                    for page in pages_to_balance.iter().take(sibling_count) {
                        return_if_locked_maybe_load!(self.pager, page.as_ref().unwrap());
                        let page = page.as_ref().unwrap().get();
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
                        divider_cell_payloads: [const { None }; MAX_SIBLING_PAGES_TO_BALANCE - 1],
                        sibling_count,
                        first_divider_cell: first_cell_divider,
                    }));
                (WriteState::BalanceNonRootDoBalancing, Ok(IOResult::IO))
            }
            WriteState::BalanceNonRootDoBalancing => {
                // Ensure all involved pages are in memory.
                let write_info = self.state.write_info().unwrap();
                let mut balance_info = write_info.balance_info.borrow_mut();
                let balance_info = balance_info.as_mut().unwrap();
                for page in balance_info
                    .pages_to_balance
                    .iter()
                    .take(balance_info.sibling_count)
                {
                    let page = page.as_ref().unwrap();
                    return_if_locked_maybe_load!(self.pager, page);
                }
                // Start balancing.
                let parent_page_btree = self.stack.top();
                let parent_page = parent_page_btree.get();

                let parent_contents = parent_page.get_contents();
                let parent_is_root = !self.stack.has_parent();

                // 1. Collect cell data from divider cells, and count the total number of cells to be distributed.
                // The count includes: all cells and overflow cells from the sibling pages, and divider cells from the parent page,
                // excluding the rightmost divider, which will not be dropped from the parent; instead it will be updated at the end.
                let mut total_cells_to_redistribute = 0;
                let mut pages_to_balance_new: [Option<BTreePage>;
                    MAX_NEW_SIBLING_PAGES_AFTER_BALANCE] =
                    [const { None }; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE];
                for i in (0..balance_info.sibling_count).rev() {
                    let sibling_page = balance_info.pages_to_balance[i].as_ref().unwrap();
                    let sibling_page = sibling_page.get();
                    turso_assert!(sibling_page.is_loaded(), "sibling page is not loaded");
                    let sibling_contents = sibling_page.get_contents();
                    total_cells_to_redistribute += sibling_contents.cell_count();
                    total_cells_to_redistribute += sibling_contents.overflow_cells.len();

                    // Right pointer is not dropped, we simply update it at the end. This could be a divider cell that points
                    // to the last page in the list of pages to balance or this could be the rightmost pointer that points to a page.
                    let is_last_sibling = i == balance_info.sibling_count - 1;
                    if is_last_sibling {
                        continue;
                    }
                    // Since we know we have a left sibling, take the divider that points to left sibling of this page
                    let cell_idx = balance_info.first_divider_cell + i;
                    let divider_is_overflow_cell = parent_contents
                        .overflow_cells
                        .first()
                        .is_some_and(|overflow_cell| overflow_cell.index == cell_idx);
                    let cell_buf = if divider_is_overflow_cell {
                        turso_assert!(
                            matches!(parent_contents.page_type(), PageType::IndexInterior),
                            "expected index interior page, got {:?}",
                            parent_contents.page_type()
                        );
                        turso_assert!(
                            parent_contents.overflow_cells.len() == 1,
                            "must have a single overflow cell in the parent, as a result of InteriorNodeReplacement"
                        );
                        let overflow_cell = parent_contents.overflow_cells.first().unwrap();
                        &overflow_cell.payload
                    } else {
                        // grep for 'OVERFLOW CELL ADJUSTMENT' for explanation.
                        // here we can subtract overflow_cells.len() every time, because we are iterating right-to-left,
                        // so if we are to the left of the overflow cell, it has already been cleared from the parent and overflow_cells.len() is 0.
                        let actual_cell_idx = cell_idx - parent_contents.overflow_cells.len();
                        let (cell_start, cell_len) = parent_contents
                            .cell_get_raw_region(actual_cell_idx, self.usable_space());
                        let buf = parent_contents.as_ptr();
                        &buf[cell_start..cell_start + cell_len]
                    };

                    // Count the divider cell itself (which will be dropped from the parent)
                    total_cells_to_redistribute += 1;

                    tracing::debug!(
                        "balance_non_root(drop_divider_cell, first_divider_cell={}, divider_cell={}, left_pointer={})",
                        balance_info.first_divider_cell,
                        i,
                        read_u32(cell_buf, 0)
                    );

                    // TODO(pere): make this reference and not copy
                    balance_info.divider_cell_payloads[i].replace(cell_buf.to_vec());
                    if divider_is_overflow_cell {
                        tracing::debug!(
                            "clearing overflow cells from parent cell_idx={}",
                            cell_idx
                        );
                        parent_contents.overflow_cells.clear();
                    } else {
                        // grep for 'OVERFLOW CELL ADJUSTMENT' for explanation.
                        // here we can subtract overflow_cells.len() every time, because we are iterating right-to-left,
                        // so if we are to the left of the overflow cell, it has already been cleared from the parent and overflow_cells.len() is 0.
                        let actual_cell_idx = cell_idx - parent_contents.overflow_cells.len();
                        tracing::trace!(
                            "dropping divider cell from parent cell_idx={} count={}",
                            actual_cell_idx,
                            parent_contents.cell_count()
                        );
                        drop_cell(parent_contents, actual_cell_idx, self.usable_space() as u16)?;
                    }
                }

                /* 2. Initialize CellArray with all the cells used for distribution, this includes divider cells if !leaf. */
                let mut cell_array = CellArray {
                    cell_payloads: Vec::with_capacity(total_cells_to_redistribute),
                    cell_count_per_page_cumulative: [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
                };
                let cells_capacity_start = cell_array.cell_payloads.capacity();

                let mut total_cells_inserted = 0;
                // This is otherwise identical to CellArray.cell_count_per_page_cumulative,
                // but we exclusively track what the prefix sums were _before_ we started redistributing cells.
                let mut old_cell_count_per_page_cumulative: [u16;
                    MAX_NEW_SIBLING_PAGES_AFTER_BALANCE] = [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE];

                let page_type = balance_info.pages_to_balance[0]
                    .as_ref()
                    .unwrap()
                    .get()
                    .get_contents()
                    .page_type();
                tracing::debug!("balance_non_root(page_type={:?})", page_type);
                let is_table_leaf = matches!(page_type, PageType::TableLeaf);
                let is_leaf = matches!(page_type, PageType::TableLeaf | PageType::IndexLeaf);
                for (i, old_page) in balance_info
                    .pages_to_balance
                    .iter()
                    .take(balance_info.sibling_count)
                    .enumerate()
                {
                    let old_page = old_page.as_ref().unwrap().get();
                    let old_page_contents = old_page.get_contents();
                    debug_validate_cells!(&old_page_contents, self.usable_space() as u16);
                    for cell_idx in 0..old_page_contents.cell_count() {
                        let (cell_start, cell_len) =
                            old_page_contents.cell_get_raw_region(cell_idx, self.usable_space());
                        let buf = old_page_contents.as_ptr();
                        let cell_buf = &mut buf[cell_start..cell_start + cell_len];
                        // TODO(pere): make this reference and not copy
                        cell_array.cell_payloads.push(to_static_buf(cell_buf));
                    }
                    // Insert overflow cells into correct place
                    let offset = total_cells_inserted;
                    for overflow_cell in old_page_contents.overflow_cells.iter_mut() {
                        cell_array.cell_payloads.insert(
                            offset + overflow_cell.index,
                            to_static_buf(&mut Pin::as_mut(&mut overflow_cell.payload)),
                        );
                    }

                    old_cell_count_per_page_cumulative[i] = cell_array.cell_payloads.len() as u16;

                    let mut cells_inserted =
                        old_page_contents.cell_count() + old_page_contents.overflow_cells.len();

                    let is_last_sibling = i == balance_info.sibling_count - 1;
                    if !is_last_sibling && !is_table_leaf {
                        // If we are a index page or a interior table page we need to take the divider cell too.
                        // But we don't need the last divider as it will remain the same.
                        let mut divider_cell = balance_info.divider_cell_payloads[i]
                            .as_mut()
                            .unwrap()
                            .as_mut_slice();
                        // TODO(pere): in case of old pages are leaf pages, so index leaf page, we need to strip page pointers
                        // from divider cells in index interior pages (parent) because those should not be included.
                        cells_inserted += 1;
                        if !is_leaf {
                            // This divider cell needs to be updated with new left pointer,
                            let right_pointer = old_page_contents.rightmost_pointer().unwrap();
                            divider_cell[..LEFT_CHILD_PTR_SIZE_BYTES]
                                .copy_from_slice(&right_pointer.to_be_bytes());
                        } else {
                            // index leaf
                            turso_assert!(
                                divider_cell.len() >= LEFT_CHILD_PTR_SIZE_BYTES,
                                "divider cell is too short"
                            );
                            // let's strip the page pointer
                            divider_cell = &mut divider_cell[LEFT_CHILD_PTR_SIZE_BYTES..];
                        }
                        cell_array.cell_payloads.push(to_static_buf(divider_cell));
                    }
                    total_cells_inserted += cells_inserted;
                }

                turso_assert!(
                    cell_array.cell_payloads.capacity() == cells_capacity_start,
                    "calculation of max cells was wrong"
                );

                // Let's copy all cells for later checks
                #[cfg(debug_assertions)]
                let mut cells_debug = Vec::new();
                #[cfg(debug_assertions)]
                {
                    for cell in &cell_array.cell_payloads {
                        cells_debug.push(cell.to_vec());
                        if is_leaf {
                            assert!(cell[0] != 0)
                        }
                    }
                }

                #[cfg(debug_assertions)]
                validate_cells_after_insertion(&cell_array, is_table_leaf);

                /* 3. Initiliaze current size of every page including overflow cells and divider cells that might be included. */
                let mut new_page_sizes: [i64; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE] =
                    [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE];
                let header_size = if is_leaf {
                    LEAF_PAGE_HEADER_SIZE_BYTES
                } else {
                    INTERIOR_PAGE_HEADER_SIZE_BYTES
                };
                // number of bytes beyond header, different from global usableSapce which includes
                // header
                let usable_space = self.usable_space() - header_size;
                for i in 0..balance_info.sibling_count {
                    cell_array.cell_count_per_page_cumulative[i] =
                        old_cell_count_per_page_cumulative[i];
                    let page = &balance_info.pages_to_balance[i].as_ref().unwrap();
                    let page = page.get();
                    let page_contents = page.get_contents();
                    let free_space = compute_free_space(page_contents, self.usable_space() as u16);

                    new_page_sizes[i] = usable_space as i64 - free_space as i64;
                    for overflow in &page_contents.overflow_cells {
                        // 2 to account of pointer
                        new_page_sizes[i] += 2 + overflow.payload.len() as i64;
                    }
                    let is_last_sibling = i == balance_info.sibling_count - 1;
                    if !is_leaf && !is_last_sibling {
                        // Account for divider cell which is included in this page.
                        new_page_sizes[i] += cell_array.cell_payloads
                            [cell_array.cell_count_up_to_page(i)]
                        .len() as i64;
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
                            turso_assert!(
                                sibling_count_new <= 5,
                                "it is corrupt to require more than 5 pages to balance 3 siblings"
                            );

                            new_page_sizes[sibling_count_new - 1] = 0;
                            cell_array.cell_count_per_page_cumulative[sibling_count_new - 1] =
                                cell_array.cell_payloads.len() as u16;
                        }
                        let size_of_cell_to_remove_from_left =
                            2 + cell_array.cell_payloads[cell_array.cell_count_up_to_page(i) - 1]
                                .len() as i64;
                        new_page_sizes[i] -= size_of_cell_to_remove_from_left;
                        let size_of_cell_to_move_right = if !is_table_leaf {
                            if cell_array.cell_count_per_page_cumulative[i]
                                < cell_array.cell_payloads.len() as u16
                            {
                                // This means we move to the right page the divider cell and we
                                // promote left cell to divider
                                CELL_PTR_SIZE_BYTES as i64
                                    + cell_array.cell_payloads[cell_array.cell_count_up_to_page(i)]
                                        .len() as i64
                            } else {
                                0
                            }
                        } else {
                            size_of_cell_to_remove_from_left
                        };
                        new_page_sizes[i + 1] += size_of_cell_to_move_right;
                        cell_array.cell_count_per_page_cumulative[i] -= 1;
                    }

                    // Now try to take from the right if we didn't have enough
                    while cell_array.cell_count_per_page_cumulative[i]
                        < cell_array.cell_payloads.len() as u16
                    {
                        let size_of_cell_to_remove_from_right = CELL_PTR_SIZE_BYTES as i64
                            + cell_array.cell_payloads[cell_array.cell_count_up_to_page(i)].len()
                                as i64;
                        let can_take = new_page_sizes[i] + size_of_cell_to_remove_from_right
                            > usable_space as i64;
                        if can_take {
                            break;
                        }
                        new_page_sizes[i] += size_of_cell_to_remove_from_right;
                        cell_array.cell_count_per_page_cumulative[i] += 1;

                        let size_of_cell_to_remove_from_right = if !is_table_leaf {
                            if cell_array.cell_count_per_page_cumulative[i]
                                < cell_array.cell_payloads.len() as u16
                            {
                                CELL_PTR_SIZE_BYTES as i64
                                    + cell_array.cell_payloads[cell_array.cell_count_up_to_page(i)]
                                        .len() as i64
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
                    let page_completes_all_cells = cell_array.cell_count_per_page_cumulative[i]
                        >= cell_array.cell_payloads.len() as u16;
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
                    cell_array.cell_payloads.len()
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
                    let mut cell_left = cell_array.cell_count_per_page_cumulative[i - 1] - 1;
                    // When table leaves are being balanced, divider cells are not part of the balancing,
                    // because table dividers don't have payloads unlike index dividers.
                    // Hence:
                    // - For table leaves: the same cell that is removed from left is added to right.
                    // - For all other page types: the divider cell is added to right, and the last non-divider cell is removed from left;
                    //   the cell removed from the left will later become a new divider cell in the parent page.
                    // TABLE LEAVES BALANCING:
                    // =======================
                    // Before balancing:
                    // LEFT                          RIGHT
                    // +-----+-----+-----+-----+    +-----+-----+
                    // | C1  | C2  | C3  | C4  |    | C5  | C6  |
                    // +-----+-----+-----+-----+    +-----+-----+
                    //         ^                           ^
                    //    (too full)                  (has space)
                    // After balancing:
                    // LEFT                     RIGHT
                    // +-----+-----+-----+      +-----+-----+-----+
                    // | C1  | C2  | C3  |      | C4  | C5  | C6  |
                    // +-----+-----+-----+      +-----+-----+-----+
                    //                               ^
                    //                          (C4 moved directly)
                    //
                    // (C3's rowid also becomes the divider cell's rowid in the parent page
                    //
                    // OTHER PAGE TYPES BALANCING:
                    // ===========================
                    // Before balancing:
                    // PARENT: [...|D1|...]
                    //            |
                    // LEFT                          RIGHT
                    // +-----+-----+-----+-----+    +-----+-----+
                    // | K1  | K2  | K3  | K4  |    | K5  | K6  |
                    // +-----+-----+-----+-----+    +-----+-----+
                    //         ^                           ^
                    //    (too full)                  (has space)
                    // After balancing:
                    // PARENT: [...|K4|...]  <-- K4 becomes new divider
                    //            |
                    // LEFT                     RIGHT
                    // +-----+-----+-----+      +-----+-----+-----+
                    // | K1  | K2  | K3  |      | D1  | K5  | K6  |
                    // +-----+-----+-----+      +-----+-----+-----+
                    //                               ^
                    //                     (old divider D1 added to right)
                    // Legend:
                    // - C# = Cell (table leaf)
                    // - K# = Key cell (index/internal node)
                    // - D# = Divider cell
                    let mut cell_right = if is_table_leaf {
                        cell_left
                    } else {
                        cell_left + 1
                    };
                    loop {
                        let cell_left_size = cell_array.cell_size_bytes(cell_left as usize) as i64;
                        let cell_right_size =
                            cell_array.cell_size_bytes(cell_right as usize) as i64;
                        // TODO: add assert nMaxCells

                        let is_last_sibling = i == sibling_count_new - 1;
                        let pointer_size = if is_last_sibling {
                            0
                        } else {
                            CELL_PTR_SIZE_BYTES as i64
                        };
                        // As mentioned, this step rebalances the siblings so that cells are moved from left to right, since the previous step just
                        // packed as much as possible to the left. However, if the right-hand-side page would become larger than the left-hand-side page,
                        // we stop.
                        let would_not_improve_balance =
                            size_right_page + cell_right_size + (CELL_PTR_SIZE_BYTES as i64)
                                > size_left_page - (cell_left_size + pointer_size);
                        if size_right_page != 0 && would_not_improve_balance {
                            break;
                        }

                        size_left_page -= cell_left_size + (CELL_PTR_SIZE_BYTES as i64);
                        size_right_page += cell_right_size + (CELL_PTR_SIZE_BYTES as i64);
                        cell_array.cell_count_per_page_cumulative[i - 1] = cell_left;

                        if cell_left == 0 {
                            break;
                        }
                        cell_left -= 1;
                        cell_right -= 1;
                    }

                    new_page_sizes[i] = size_right_page;
                    new_page_sizes[i - 1] = size_left_page;
                    assert!(
                        cell_array.cell_count_per_page_cumulative[i - 1]
                            > if i > 1 {
                                cell_array.cell_count_per_page_cumulative[i - 2]
                            } else {
                                0
                            }
                    );
                }

                // Allocate pages or set dirty if not needed
                for i in 0..sibling_count_new {
                    if i < balance_info.sibling_count {
                        let page = balance_info.pages_to_balance[i].as_ref().unwrap();
                        turso_assert!(
                            page.get().is_dirty(),
                            "sibling page must be already marked dirty"
                        );
                        pages_to_balance_new[i].replace(page.clone());
                    } else {
                        // FIXME: handle page cache is full
                        let mut page = self.allocate_page(page_type, 0)?;
                        // FIXME: add new state machine state instead of this sync IO hack
                        while matches!(page, IOResult::IO) {
                            self.pager.io.run_once()?;
                            page = self.allocate_page(page_type, 0)?;
                        }
                        let IOResult::Done(page) = page else {
                            return Err(LimboError::InternalError(
                                "Failed to allocate page".into(),
                            ));
                        };
                        pages_to_balance_new[i].replace(page);
                        // Since this page didn't exist before, we can set it to cells length as it
                        // marks them as empty since it is a prefix sum of cells.
                        old_cell_count_per_page_cumulative[i] =
                            cell_array.cell_payloads.len() as u16;
                    }
                }

                // Reassign page numbers in increasing order
                {
                    let mut page_numbers: [usize; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE] =
                        [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE];
                    for (i, page) in pages_to_balance_new
                        .iter()
                        .take(sibling_count_new)
                        .enumerate()
                    {
                        page_numbers[i] = page.as_ref().unwrap().get().get().id;
                    }
                    page_numbers.sort();
                    for (page, new_id) in pages_to_balance_new
                        .iter()
                        .take(sibling_count_new)
                        .rev()
                        .zip(page_numbers.iter().rev().take(sibling_count_new))
                    {
                        let page = page.as_ref().unwrap();
                        if *new_id != page.get().get().id {
                            page.get().get().id = *new_id;
                            self.pager
                                .update_dirty_loaded_page_in_cache(*new_id, page.get())?;
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
                                page.as_ref().unwrap().get().get().id
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
                    let last_sibling_idx = balance_info.sibling_count - 1;
                    let last_page = balance_info.pages_to_balance[last_sibling_idx]
                        .as_ref()
                        .unwrap();
                    let right_pointer = last_page.get().get_contents().rightmost_pointer().unwrap();
                    let new_last_page = pages_to_balance_new[sibling_count_new - 1]
                        .as_ref()
                        .unwrap();
                    new_last_page
                        .get()
                        .get_contents()
                        .write_u32(offset::BTREE_RIGHTMOST_PTR, right_pointer);
                }
                turso_assert!(
                    parent_contents.overflow_cells.is_empty(),
                    "parent page overflow cells should be empty before divider cell reinsertion"
                );
                // TODO: pointer map update (vacuum support)
                // Update divider cells in parent
                for (sibling_page_idx, page) in pages_to_balance_new
                    .iter()
                    .enumerate()
                    .take(sibling_count_new - 1)
                /* do not take last page */
                {
                    let page = page.as_ref().unwrap();
                    // e.g. if we have 3 pages and the leftmost child page has 3 cells,
                    // then the divider cell idx is 3 in the flat cell array.
                    let divider_cell_idx = cell_array.cell_count_up_to_page(sibling_page_idx);
                    let mut divider_cell = &mut cell_array.cell_payloads[divider_cell_idx];
                    // FIXME: dont use auxiliary space, could be done without allocations
                    let mut new_divider_cell = Vec::new();
                    if !is_leaf_page {
                        // Interior
                        // Make this page's rightmost pointer point to pointer of divider cell before modification
                        let previous_pointer_divider = read_u32(divider_cell, 0);
                        page.get()
                            .get_contents()
                            .write_u32(offset::BTREE_RIGHTMOST_PTR, previous_pointer_divider);
                        // divider cell now points to this page
                        new_divider_cell
                            .extend_from_slice(&(page.get().get().id as u32).to_be_bytes());
                        // now copy the rest of the divider cell:
                        // Table Interior page:
                        //   * varint rowid
                        // Index Interior page:
                        //   * varint payload size
                        //   * payload
                        //   * first overflow page (u32 optional)
                        new_divider_cell.extend_from_slice(&divider_cell[4..]);
                    } else if is_table_leaf {
                        // For table leaves, divider_cell_idx effectively points to the last cell of the old left page.
                        // The new divider cell's rowid becomes the second-to-last cell's rowid.
                        // i.e. in the diagram above, the new divider cell's rowid becomes the rowid of C3.
                        // FIXME: not needed conversion
                        // FIXME: need to update cell size in order to free correctly?
                        // insert into cell with correct range should be enough
                        divider_cell = &mut cell_array.cell_payloads[divider_cell_idx - 1];
                        let (_, n_bytes_payload) = read_varint(divider_cell)?;
                        let (rowid, _) = read_varint(&divider_cell[n_bytes_payload..])?;
                        new_divider_cell
                            .extend_from_slice(&(page.get().get().id as u32).to_be_bytes());
                        write_varint_to_vec(rowid, &mut new_divider_cell);
                    } else {
                        // Leaf index
                        new_divider_cell
                            .extend_from_slice(&(page.get().get().id as u32).to_be_bytes());
                        new_divider_cell.extend_from_slice(divider_cell);
                    }

                    let left_pointer = read_u32(&new_divider_cell[..LEFT_CHILD_PTR_SIZE_BYTES], 0);
                    turso_assert!(
                        left_pointer != parent_page.get().id as u32,
                        "left pointer is the same as parent page id"
                    );
                    #[cfg(debug_assertions)]
                    pages_pointed_to.insert(left_pointer);
                    tracing::debug!(
                        "balance_non_root(insert_divider_cell, first_divider_cell={}, divider_cell={}, left_pointer={})",
                        balance_info.first_divider_cell,
                        sibling_page_idx,
                        left_pointer
                    );
                    turso_assert!(
                        left_pointer == page.get().get().id as u32,
                        "left pointer is not the same as page id"
                    );
                    // FIXME: remove this lock
                    let database_size = self
                        .pager
                        .io
                        .block(|| self.pager.with_header(|header| header.database_size))?
                        .get();
                    turso_assert!(
                        left_pointer <= database_size,
                        "invalid page number divider left pointer {} > database number of pages {}",
                        left_pointer,
                        database_size
                    );
                    // FIXME: defragment shouldn't be needed
                    // defragment_page(parent_contents, self.usable_space() as u16);
                    let divider_cell_insert_idx_in_parent =
                        balance_info.first_divider_cell + sibling_page_idx;
                    let overflow_cell_count_before = parent_contents.overflow_cells.len();
                    insert_into_cell(
                        parent_contents,
                        &new_divider_cell,
                        divider_cell_insert_idx_in_parent,
                        self.usable_space() as u16,
                    )?;
                    let overflow_cell_count_after = parent_contents.overflow_cells.len();
                    let divider_cell_is_overflow_cell =
                        overflow_cell_count_after > overflow_cell_count_before;
                    #[cfg(debug_assertions)]
                    self.validate_balance_non_root_divider_cell_insertion(
                        balance_info,
                        parent_contents,
                        divider_cell_insert_idx_in_parent,
                        divider_cell_is_overflow_cell,
                        &page.get(),
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
                            pages_pointed_to.contains(&(page.get().get().id as u32)),
                            "page {} not pointed to by divider cell or rightmost pointer",
                            page.get().get().id
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
                let mut done = [false; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE];
                let rightmost_page_negative_idx = 1 - sibling_count_new as i64;
                let rightmost_page_positive_idx = sibling_count_new as i64 - 1;
                for i in rightmost_page_negative_idx..=rightmost_page_positive_idx {
                    // As mentioned above, we do two passes over the pages:
                    // 1. Downward pass: Process pages in decreasing order
                    // 2. Upward pass: Process pages in increasing order
                    // Hence if we have 3 siblings:
                    // the order of 'i' will be: -2, -1, 0, 1, 2.
                    // and the page processing order is: 2, 1, 0, 1, 2.
                    let page_idx = i.unsigned_abs() as usize;
                    if done[page_idx] {
                        continue;
                    }
                    // As outlined above, this condition ensures we process pages in the correct order to avoid disrupting cells that still need to be read.
                    // 1. i >= 0 handles the upward pass where we process any pages not processed in the downward pass.
                    //    - condition (1) is not violated: if cells are moving right-to-left, righthand sibling has not been updated yet.
                    //    - condition (2) is not violated: if cells are moving left-to-right, righthand sibling has already been updated in the downward pass.
                    // 2. The second condition checks if it's safe to process a page during the downward pass.
                    //    - condition (1) is not violated: if cells are moving right-to-left, we do nothing.
                    //    - condition (2) is not violated: if cells are moving left-to-right, we are allowed to update.
                    if i >= 0
                        || old_cell_count_per_page_cumulative[page_idx - 1]
                            >= cell_array.cell_count_per_page_cumulative[page_idx - 1]
                    {
                        let (start_old_cells, start_new_cells, number_new_cells) = if page_idx == 0
                        {
                            (0, 0, cell_array.cell_count_up_to_page(0))
                        } else {
                            let this_was_old_page = page_idx < balance_info.sibling_count;
                            // We add !is_table_leaf because we want to skip 1 in case of divider cell which is encountared between pages assigned
                            let start_old_cells = if this_was_old_page {
                                old_cell_count_per_page_cumulative[page_idx - 1] as usize
                                    + (!is_table_leaf) as usize
                            } else {
                                cell_array.cell_payloads.len()
                            };
                            let start_new_cells = cell_array.cell_count_up_to_page(page_idx - 1)
                                + (!is_table_leaf) as usize;
                            (
                                start_old_cells,
                                start_new_cells,
                                cell_array.cell_count_up_to_page(page_idx) - start_new_cells,
                            )
                        };
                        let page = pages_to_balance_new[page_idx].as_ref().unwrap();
                        let page = page.get();
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
                let first_child_page = first_child_page.get();
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
                        DatabaseHeader::SIZE
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
                    &parent_page_btree,
                    balance_info,
                    parent_contents,
                    pages_to_balance_new,
                    page_type,
                    is_table_leaf,
                    cells_debug,
                    sibling_count_new,
                    right_page_id,
                );

                (
                    WriteState::BalanceFreePages {
                        curr_page: sibling_count_new,
                        sibling_count_new,
                    },
                    Ok(IOResult::Done(())),
                )
            }
            WriteState::BalanceFreePages {
                curr_page,
                sibling_count_new,
            } => {
                let write_info = self.state.write_info().unwrap();
                let mut balance_info: std::cell::RefMut<'_, Option<BalanceInfo>> =
                    write_info.balance_info.borrow_mut();
                let balance_info = balance_info.as_mut().unwrap();
                // We have to free pages that are not used anymore
                if !((sibling_count_new..balance_info.sibling_count).contains(&curr_page)) {
                    (WriteState::BalanceStart, Ok(IOResult::Done(())))
                } else {
                    let page = balance_info.pages_to_balance[curr_page].as_ref().unwrap();
                    return_if_io!(self
                        .pager
                        .free_page(Some(page.get().clone()), page.get().get().id));
                    (
                        WriteState::BalanceFreePages {
                            curr_page: curr_page + 1,
                            sibling_count_new,
                        },
                        Ok(IOResult::Done(())),
                    )
                }
            }
        };
        if matches!(next_write_state, WriteState::BalanceStart) {
            // reset balance state
            let _ = self.state.mut_write_info().unwrap().balance_info.take();
        }
        let write_info = self.state.mut_write_info().unwrap();
        write_info.state = next_write_state;
        result
    }

    /// Validates that a divider cell was correctly inserted into the parent page
    /// during B-tree balancing and that it points to the correct child page.
    #[cfg(debug_assertions)]
    fn validate_balance_non_root_divider_cell_insertion(
        &self,
        balance_info: &mut BalanceInfo,
        parent_contents: &mut PageContent,
        divider_cell_insert_idx_in_parent: usize,
        divider_cell_is_overflow_cell: bool,
        child_page: &std::sync::Arc<crate::Page>,
    ) {
        let left_pointer = if divider_cell_is_overflow_cell {
            parent_contents.overflow_cells
                .iter()
                .find(|cell| cell.index == divider_cell_insert_idx_in_parent)
                .map(|cell| read_u32(&cell.payload, 0))
                .unwrap_or_else(|| {
                    panic!(
                        "overflow cell with divider cell was not found (divider_cell_idx={}, balance_info.first_divider_cell={}, overflow_cells.len={})",
                        divider_cell_insert_idx_in_parent,
                        balance_info.first_divider_cell,
                        parent_contents.overflow_cells.len(),
                    )
                })
        } else if divider_cell_insert_idx_in_parent < parent_contents.cell_count() {
            let (cell_start, cell_len) = parent_contents
                .cell_get_raw_region(divider_cell_insert_idx_in_parent, self.usable_space());
            read_u32(
                &parent_contents.as_ptr()[cell_start..cell_start + cell_len],
                0,
            )
        } else {
            panic!(
                "divider cell is not in the parent page (divider_cell_idx={}, balance_info.first_divider_cell={}, overflow_cells.len={})",
                divider_cell_insert_idx_in_parent,
                balance_info.first_divider_cell,
                parent_contents.overflow_cells.len(),
            )
        };

        // Verify the left pointer points to the correct page
        assert_eq!(
            left_pointer,
            child_page.get().id as u32,
            "the cell we just inserted doesn't point to the correct page. points to {}, should point to {}",
            left_pointer,
            child_page.get().id as u32
        );
    }

    #[cfg(debug_assertions)]
    #[allow(clippy::too_many_arguments)]
    fn post_balance_non_root_validation(
        &self,
        parent_page: &BTreePage,
        balance_info: &mut BalanceInfo,
        parent_contents: &mut PageContent,
        pages_to_balance_new: [Option<BTreePage>; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
        page_type: PageType,
        is_table_leaf: bool,
        mut cells_debug: Vec<Vec<u8>>,
        sibling_count_new: usize,
        right_page_id: u32,
    ) {
        let mut valid = true;
        let mut current_index_cell = 0;
        for cell_idx in 0..parent_contents.cell_count() {
            let cell = parent_contents
                .cell_get(cell_idx, self.usable_space())
                .unwrap();
            match cell {
                BTreeCell::TableInteriorCell(table_interior_cell) => {
                    let left_child_page = table_interior_cell.left_child_page;
                    if left_child_page == parent_page.get().get().id as u32 {
                        tracing::error!("balance_non_root(parent_divider_points_to_same_page, page_id={}, cell_left_child_page={})",
                                parent_page.get().get().id,
                                left_child_page,
                            );
                        valid = false;
                    }
                }
                BTreeCell::IndexInteriorCell(index_interior_cell) => {
                    let left_child_page = index_interior_cell.left_child_page;
                    if left_child_page == parent_page.get().get().id as u32 {
                        tracing::error!("balance_non_root(parent_divider_points_to_same_page, page_id={}, cell_left_child_page={})",
                                parent_page.get().get().id,
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
            let page = page.get();
            let contents = page.get_contents();
            debug_validate_cells!(contents, self.usable_space() as u16);
            // Cells are distributed in order
            for cell_idx in 0..contents.cell_count() {
                let (cell_start, cell_len) =
                    contents.cell_get_raw_region(cell_idx, self.usable_space());
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
                    contents,
                    0,
                    self.usable_space(),
                )
                .unwrap();
                match &cell {
                    BTreeCell::TableInteriorCell(table_interior_cell) => {
                        let left_child_page = table_interior_cell.left_child_page;
                        if left_child_page == page.get().id as u32 {
                            tracing::error!("balance_non_root(child_page_points_same_page, page_id={}, cell_left_child_page={}, page_idx={})",
                                page.get().id,
                                left_child_page,
                                page_idx
                            );
                            valid = false;
                        }
                        if left_child_page == parent_page.get().get().id as u32 {
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
                        if left_child_page == parent_page.get().get().id as u32 {
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
                debug_validate_cells!(parent_contents, self.usable_space() as u16);

                if pages_to_balance_new[0].is_none() {
                    tracing::error!(
                        "balance_non_root(balance_shallower_incorrect_page, page_idx={})",
                        0
                    );
                    valid = false;
                }

                for (i, value) in pages_to_balance_new
                    .iter()
                    .enumerate()
                    .take(sibling_count_new)
                    .skip(1)
                {
                    if value.is_some() {
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

                if right_page_id == page.get().id as u32
                    || right_page_id == parent_page.get().get().id as u32
                {
                    tracing::error!("balance_non_root(balance_shallower_rightmost_pointer, page_id={}, parent_page_id={}, rightmost={})",
                        page.get().id,
                        parent_page.get().get().id,
                        right_page_id,
                    );
                    valid = false;
                }

                if let Some(rm) = contents.rightmost_pointer() {
                    if rm != right_page_id {
                        tracing::error!("balance_non_root(balance_shallower_rightmost_pointer, page_rightmost={}, rightmost={})",
                            rm,
                            right_page_id,
                        );
                        valid = false;
                    }
                }

                if let Some(rm) = parent_contents.rightmost_pointer() {
                    if rm != right_page_id {
                        tracing::error!("balance_non_root(balance_shallower_rightmost_pointer, parent_rightmost={}, rightmost={})",
                            rm,
                            right_page_id,
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

                for (parent_cell_idx, cell_buf_in_array) in
                    cells_debug.iter().enumerate().take(contents.cell_count())
                {
                    let (parent_cell_start, parent_cell_len) =
                        parent_contents.cell_get_raw_region(parent_cell_idx, self.usable_space());

                    let (cell_start, cell_len) =
                        contents.cell_get_raw_region(parent_cell_idx, self.usable_space());

                    let buf = contents.as_ptr();
                    let cell_buf = to_static_buf(&mut buf[cell_start..cell_start + cell_len]);
                    let parent_cell_buf = to_static_buf(
                        &mut parent_buf[parent_cell_start..parent_cell_start + parent_cell_len],
                    );

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
                if cell_divider_idx == parent_contents.cell_count()
                    && right_page_id != page.get().id as u32
                {
                    tracing::error!("balance_non_root(cell_divider_right_pointer, should point to {}, but points to {})",
                        page.get().id,
                        right_page_id
                    );
                    valid = false;
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
                    if !is_table_leaf {
                        // remember to increase cell if this cell was moved to parent
                        current_index_cell += 1;
                    }
                    continue;
                }
                // check if overflow
                // check if right pointer, this is the last page. Do we update rightmost pointer and defragment moves it?
                let (cell_start, cell_len) =
                    parent_contents.cell_get_raw_region(cell_divider_idx, self.usable_space());
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
                if is_table_leaf {
                    // If we are in a table leaf page, we just need to check that this cell that should be a divider cell is in the parent
                    // This means we already check cell in leaf pages but not on parent so we don't advance current_index_cell
                    let last_sibling_idx = balance_info.sibling_count - 1;
                    if page_idx >= last_sibling_idx {
                        // This means we are in the last page and we don't need to check anything
                        continue;
                    }
                    let cell_buf: &'static mut [u8] =
                        to_static_buf(&mut cells_debug[current_index_cell - 1]);
                    let cell = crate::storage::sqlite3_ondisk::read_btree_cell(
                        cell_buf,
                        contents,
                        0,
                        self.usable_space(),
                    )
                    .unwrap();
                    let parent_cell = parent_contents
                        .cell_get(cell_divider_idx, self.usable_space())
                        .unwrap();
                    let rowid = match cell {
                        BTreeCell::TableLeafCell(table_leaf_cell) => table_leaf_cell.rowid,
                        _ => unreachable!(),
                    };
                    let rowid_parent = match parent_cell {
                        BTreeCell::TableInteriorCell(table_interior_cell) => {
                            table_interior_cell.rowid
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
                        if !is_table_leaf {
                            // remember to increase cell if this cell was moved to parent
                            current_index_cell += 1;
                        }
                        continue;
                    }
                    let (parent_cell_start, parent_cell_len) =
                        parent_contents.cell_get_raw_region(cell_divider_idx, self.usable_space());
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
        assert!(
            valid,
            "corrupted database, cells were not balanced properly"
        );
    }

    /// Balance the root page.
    /// This is done when the root page overflows, and we need to create a new root page.
    /// See e.g. https://en.wikipedia.org/wiki/B-tree
    fn balance_root(&mut self) -> Result<IOResult<()>> {
        /* todo: balance deeper, create child and copy contents of root there. Then split root */
        /* if we are in root page then we just need to create a new root and push key there */

        // Since we are going to change the btree structure, let's forget our cached knowledge of the rightmost page.
        let _ = self.move_to_right_state.1.take();

        let is_page_1 = {
            let current_root = self.stack.top();
            current_root.get().get().id == 1
        };

        let offset = if is_page_1 { DatabaseHeader::SIZE } else { 0 };

        let root_btree = self.stack.top();
        let root = root_btree.get();
        let root_contents = root.get_contents();
        // FIXME: handle page cache is full
        // FIXME: remove sync IO hack
        let child_btree = loop {
            match self.pager.do_allocate_page(
                root_contents.page_type(),
                0,
                BtreePageAllocMode::Any,
            )? {
                IOResult::IO => {
                    self.pager.io.run_once()?;
                }
                IOResult::Done(page) => break page,
            }
        };

        tracing::debug!(
            "balance_root(root={}, rightmost={}, page_type={:?})",
            root.get().id,
            child_btree.get().get().id,
            root.get_contents().page_type()
        );

        turso_assert!(root.is_dirty(), "root must be marked dirty");
        turso_assert!(
            child_btree.get().is_dirty(),
            "child must be marked dirty as freshly allocated page"
        );

        let root_buf = root_contents.as_ptr();
        let child = child_btree.get();
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
        self.stack.push(root_btree.clone());
        self.stack.set_cell_index(0); // leave parent pointing at the rightmost pointer (in this case 0, as there are no cells), since we will be balancing the rightmost child page.
        self.stack.push(child_btree.clone());
        Ok(IOResult::Done(()))
    }

    fn usable_space(&self) -> usize {
        self.pager.usable_space()
    }

    pub fn seek_end(&mut self) -> Result<IOResult<()>> {
        assert!(self.mv_cursor.is_none()); // unsure about this -_-
        let _c = self.move_to_root()?;
        loop {
            let mem_page = self.stack.top();
            let page_id = mem_page.get().get().id;
            let (page, _c) = self.read_page(page_id)?;
            return_if_locked_maybe_load!(self.pager, page);

            let page = page.get();
            let contents = page.get().contents.as_ref().unwrap();
            if contents.is_leaf() {
                // set cursor just past the last cell to append
                self.stack.set_cell_index(contents.cell_count() as i32);
                return Ok(IOResult::Done(()));
            }

            match contents.rightmost_pointer() {
                Some(right_most_pointer) => {
                    self.stack.set_cell_index(contents.cell_count() as i32 + 1); // invalid on interior
                    let (child, _c) = self.read_page(right_most_pointer as usize)?;
                    self.stack.push(child);
                }
                None => unreachable!("interior page must have rightmost pointer"),
            }
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn seek_to_last(&mut self) -> Result<IOResult<()>> {
        loop {
            match self.seek_to_last_state {
                SeekToLastState::Start => {
                    assert!(self.mv_cursor.is_none());
                    let has_record = return_if_io!(self.move_to_rightmost());
                    self.invalidate_record();
                    self.has_record.replace(has_record);
                    if !has_record {
                        self.seek_to_last_state = SeekToLastState::IsEmpty;
                        continue;
                    }
                    return Ok(IOResult::Done(()));
                }
                SeekToLastState::IsEmpty => {
                    let is_empty = return_if_io!(self.is_empty_table());
                    assert!(is_empty);
                    self.seek_to_last_state = SeekToLastState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        !self.has_record.get()
    }

    pub fn root_page(&self) -> usize {
        self.root_page
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn rewind(&mut self) -> Result<IOResult<()>> {
        if let Some(mv_cursor) = &self.mv_cursor {
            {
                let mut mv_cursor = mv_cursor.borrow_mut();
                mv_cursor.rewind();
            }
            let cursor_has_record = return_if_io!(self.get_next_record());
            self.invalidate_record();
            self.has_record.replace(cursor_has_record);
        } else {
            let _c = self.move_to_root()?;

            let cursor_has_record = return_if_io!(self.get_next_record());
            self.invalidate_record();
            self.has_record.replace(cursor_has_record);
        }
        Ok(IOResult::Done(()))
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn last(&mut self) -> Result<IOResult<()>> {
        assert!(self.mv_cursor.is_none());
        let cursor_has_record = return_if_io!(self.move_to_rightmost());
        self.has_record.replace(cursor_has_record);
        self.invalidate_record();
        Ok(IOResult::Done(()))
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn next(&mut self) -> Result<IOResult<bool>> {
        return_if_io!(self.restore_context());
        let cursor_has_record = return_if_io!(self.get_next_record());
        self.has_record.replace(cursor_has_record);
        self.invalidate_record();
        Ok(IOResult::Done(cursor_has_record))
    }

    fn invalidate_record(&mut self) {
        self.get_immutable_record_or_create()
            .as_mut()
            .unwrap()
            .invalidate();
        self.record_cursor.borrow_mut().invalidate();
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn prev(&mut self) -> Result<IOResult<bool>> {
        assert!(self.mv_cursor.is_none());
        return_if_io!(self.restore_context());
        let cursor_has_record = return_if_io!(self.get_prev_record());
        self.has_record.replace(cursor_has_record);
        self.invalidate_record();
        Ok(IOResult::Done(cursor_has_record))
    }

    #[instrument(skip(self), level = Level::DEBUG)]
    pub fn rowid(&mut self) -> Result<IOResult<Option<i64>>> {
        if let Some(mv_cursor) = &self.mv_cursor {
            if self.has_record.get() {
                let mut mv_cursor = mv_cursor.borrow_mut();
                return Ok(IOResult::Done(
                    mv_cursor.current_row_id().map(|rowid| rowid.row_id),
                ));
            } else {
                return Ok(IOResult::Done(None));
            }
        }
        if self.has_record.get() {
            let page = self.stack.top();
            return_if_locked_maybe_load!(self.pager, page);
            let page = page.get();
            let contents = page.get_contents();
            let page_type = contents.page_type();
            if page_type.is_table() {
                let cell_idx = self.stack.current_cell_index();
                let rowid = contents.cell_table_leaf_read_rowid(cell_idx as usize)?;
                Ok(IOResult::Done(Some(rowid)))
            } else {
                let _ = return_if_io!(self.record());
                Ok(IOResult::Done(self.get_index_rowid_from_record()))
            }
        } else {
            Ok(IOResult::Done(None))
        }
    }

    #[instrument(skip(self), level = Level::DEBUG)]
    pub fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<IOResult<SeekResult>> {
        assert!(self.mv_cursor.is_none());
        // Empty trace to capture the span information
        tracing::trace!("");
        // We need to clear the null flag for the table cursor before seeking,
        // because it might have been set to false by an unmatched left-join row during the previous iteration
        // on the outer loop.
        self.set_null_flag(false);
        let seek_result = return_if_io!(self.do_seek(key, op));
        self.invalidate_record();
        // Reset seek state
        self.seek_state = CursorSeekState::Start;
        self.valid_state = CursorValidState::Valid;
        Ok(IOResult::Done(seek_result))
    }

    /// Return a reference to the record the cursor is currently pointing to.
    /// If record was not parsed yet, then we have to parse it and in case of I/O we yield control
    /// back.
    #[instrument(skip(self), level = Level::DEBUG)]
    pub fn record(&self) -> Result<IOResult<Option<Ref<ImmutableRecord>>>> {
        if !self.has_record.get() {
            return Ok(IOResult::Done(None));
        }
        let invalidated = self
            .reusable_immutable_record
            .borrow()
            .as_ref()
            .is_none_or(|record| record.is_invalidated());
        if !invalidated {
            *self.parse_record_state.borrow_mut() = ParseRecordState::Init;
            let record_ref =
                Ref::filter_map(self.reusable_immutable_record.borrow(), |opt| opt.as_ref())
                    .unwrap();
            return Ok(IOResult::Done(Some(record_ref)));
        }
        if self.mv_cursor.is_some() {
            let mut mv_cursor = self.mv_cursor.as_ref().unwrap().borrow_mut();
            let row = mv_cursor.current_row().unwrap().unwrap();
            self.get_immutable_record_or_create()
                .as_mut()
                .unwrap()
                .invalidate();
            self.get_immutable_record_or_create()
                .as_mut()
                .unwrap()
                .start_serialization(&row.data);
            self.record_cursor.borrow_mut().invalidate();
            let record_ref =
                Ref::filter_map(self.reusable_immutable_record.borrow(), |opt| opt.as_ref())
                    .unwrap();
            return Ok(IOResult::Done(Some(record_ref)));
        }

        if *self.parse_record_state.borrow() == ParseRecordState::Init {
            *self.parse_record_state.borrow_mut() = ParseRecordState::Parsing {
                payload: Vec::new(),
            };
        }
        let page = self.stack.top();
        return_if_locked_maybe_load!(self.pager, page);
        let page = page.get();
        let contents = page.get_contents();
        let cell_idx = self.stack.current_cell_index();
        let cell = contents.cell_get(cell_idx as usize, self.usable_space())?;
        let (payload, payload_size, first_overflow_page) = match cell {
            BTreeCell::TableLeafCell(TableLeafCell {
                payload,
                payload_size,
                first_overflow_page,
                ..
            }) => (payload, payload_size, first_overflow_page),
            BTreeCell::IndexInteriorCell(IndexInteriorCell {
                payload,
                payload_size,
                first_overflow_page,
                ..
            }) => (payload, payload_size, first_overflow_page),
            BTreeCell::IndexLeafCell(IndexLeafCell {
                payload,
                first_overflow_page,
                payload_size,
            }) => (payload, payload_size, first_overflow_page),
            _ => unreachable!("unexpected page_type"),
        };
        if let Some(next_page) = first_overflow_page {
            return_if_io!(self.process_overflow_read(payload, next_page, payload_size))
        } else {
            self.get_immutable_record_or_create()
                .as_mut()
                .unwrap()
                .invalidate();
            self.get_immutable_record_or_create()
                .as_mut()
                .unwrap()
                .start_serialization(payload);
            self.record_cursor.borrow_mut().invalidate();
        };

        *self.parse_record_state.borrow_mut() = ParseRecordState::Init;
        let record_ref =
            Ref::filter_map(self.reusable_immutable_record.borrow(), |opt| opt.as_ref()).unwrap();
        Ok(IOResult::Done(Some(record_ref)))
    }

    #[instrument(skip(self), level = Level::DEBUG)]
    pub fn insert(
        &mut self,
        key: &BTreeKey,
        // Indicate whether it's necessary to traverse to find the leaf page
        // FIXME: refactor this out into a state machine, these ad-hoc state
        // variables are very hard to reason about
        mut moved_before: bool,
    ) -> Result<IOResult<()>> {
        tracing::debug!(valid_state = ?self.valid_state, cursor_state = ?self.state, is_write_in_progress = self.is_write_in_progress());
        match &self.mv_cursor {
            Some(mv_cursor) => match key.maybe_rowid() {
                Some(rowid) => {
                    let row_id = crate::mvcc::database::RowID::new(self.table_id() as u64, rowid);
                    let record_buf = key.get_record().unwrap().get_payload().to_vec();
                    let num_columns = match key {
                        BTreeKey::IndexKey(record) => record.column_count(),
                        BTreeKey::TableRowId((_, record)) => {
                            record.as_ref().unwrap().column_count()
                        }
                    };
                    let row = crate::mvcc::database::Row::new(row_id, record_buf, num_columns);
                    mv_cursor.borrow_mut().insert(row).unwrap();
                }
                None => todo!("Support mvcc inserts with index btrees"),
            },
            None => {
                match (&self.valid_state, self.is_write_in_progress()) {
                    (CursorValidState::Valid, _) => {
                        // consider the current position valid unless the caller explicitly asks us to seek.
                    }
                    (CursorValidState::RequireSeek, false) => {
                        // we must seek.
                        moved_before = false;
                    }
                    (CursorValidState::RequireSeek, true) => {
                        // illegal to seek during a write no matter what CursorValidState or caller says -- we might e.g. move to the wrong page during balancing
                        moved_before = true;
                    }
                    (CursorValidState::RequireAdvance(direction), _) => {
                        // FIXME: this is a hack to support the case where we need to advance the cursor after a seek.
                        // We should have a proper state machine for this.
                        return_if_io!(match direction {
                            IterationDirection::Forwards => self.next(),
                            IterationDirection::Backwards => self.prev(),
                        });
                        self.valid_state = CursorValidState::Valid;
                        self.seek_state = CursorSeekState::Start;
                        moved_before = true;
                    }
                };
                if !moved_before {
                    let seek_result = match key {
                        BTreeKey::IndexKey(_) => {
                            return_if_io!(self.seek(
                                SeekKey::IndexKey(key.get_record().unwrap()),
                                SeekOp::GE { eq_only: true }
                            ))
                        }
                        BTreeKey::TableRowId(_) => {
                            return_if_io!(self.seek(
                                SeekKey::TableRowId(key.to_rowid()),
                                SeekOp::GE { eq_only: true }
                            ))
                        }
                    };
                    if SeekResult::TryAdvance == seek_result {
                        self.valid_state =
                            CursorValidState::RequireAdvance(IterationDirection::Forwards);
                        return_if_io!(self.next());
                    }
                    self.context.take(); // we know where we wanted to move so if there was any saved context, discard it.
                    self.valid_state = CursorValidState::Valid;
                    self.seek_state = CursorSeekState::Start;
                    tracing::debug!(
                        "seeked to the right place, page is now {:?}",
                        self.stack.top().get().get().id
                    );
                }
                return_if_io!(self.insert_into_page(key));
                if key.maybe_rowid().is_some() {
                    self.has_record.replace(true);
                }
            }
        };
        Ok(IOResult::Done(()))
    }

    /// Delete state machine flow:
    /// 1. Start -> check if the rowid to be delete is present in the page or not. If not we early return
    /// 2. DeterminePostBalancingSeekKey -> determine the key to seek to after balancing.
    /// 3. LoadPage -> load the page.
    /// 4. FindCell -> find the cell to be deleted in the page.
    /// 5. ClearOverflowPages -> Clear the overflow pages if there are any before dropping the cell, then if we are in a leaf page we just drop the cell in place.
    /// if we are in interior page, we need to rotate keys in order to replace current cell (InteriorNodeReplacement).
    /// 6. InteriorNodeReplacement -> we copy the left subtree leaf node into the deleted interior node's place.
    /// 7. WaitForBalancingToComplete -> perform balancing
    /// 8. SeekAfterBalancing -> adjust the cursor to a node that is closer to the deleted value. go to Finish
    /// 9. Finish -> Delete operation is done. Return CursorResult(Ok())
    #[instrument(skip(self), level = Level::DEBUG)]
    pub fn delete(&mut self) -> Result<IOResult<()>> {
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
            tracing::debug!(?delete_state);

            match delete_state {
                DeleteState::Start => {
                    let page = self.stack.top();
                    self.pager.add_dirty(&page.get());
                    if matches!(
                        page.get().get_contents().page_type(),
                        PageType::TableLeaf | PageType::TableInterior
                    ) {
                        if return_if_io!(self.rowid()).is_none() {
                            self.state = CursorState::None;
                            return Ok(IOResult::Done(()));
                        }
                    } else if self.reusable_immutable_record.borrow().is_none() {
                        self.state = CursorState::None;
                        return Ok(IOResult::Done(()));
                    }

                    let delete_info = self.state.mut_delete_info().unwrap();
                    delete_info.state = DeleteState::DeterminePostBalancingSeekKey;
                }

                DeleteState::DeterminePostBalancingSeekKey => {
                    // FIXME: skip this work if we determine deletion wont result in balancing
                    // Right now we calculate the key every time for simplicity/debugging
                    // since it won't affect correctness which is more important
                    let page = self.stack.top();
                    return_if_locked_maybe_load!(self.pager, page);
                    let target_key = if page.get().is_index() {
                        let record = match return_if_io!(self.record()) {
                            Some(record) => record.clone(),
                            None => unreachable!("there should've been a record"),
                        };
                        DeleteSavepoint::Payload(record)
                    } else {
                        let Some(rowid) = return_if_io!(self.rowid()) else {
                            panic!("cursor should be pointing to a record with a rowid");
                        };
                        DeleteSavepoint::Rowid(rowid)
                    };

                    let delete_info = self.state.mut_delete_info().unwrap();
                    delete_info.state = DeleteState::LoadPage {
                        post_balancing_seek_key: Some(target_key),
                    };
                }

                DeleteState::LoadPage {
                    post_balancing_seek_key,
                } => {
                    let page = self.stack.top();
                    return_if_locked_maybe_load!(self.pager, page);

                    let delete_info = self.state.mut_delete_info().unwrap();
                    delete_info.state = DeleteState::FindCell {
                        post_balancing_seek_key,
                    };
                }

                DeleteState::FindCell {
                    post_balancing_seek_key,
                } => {
                    let page = self.stack.top();
                    let cell_idx = self.stack.current_cell_index() as usize;

                    let page = page.get();
                    let contents = page.get().contents.as_ref().unwrap();
                    if cell_idx >= contents.cell_count() {
                        return_corrupt!(format!(
                            "Corrupted page: cell index {} is out of bounds for page with {} cells",
                            cell_idx,
                            contents.cell_count()
                        ));
                    }

                    tracing::debug!(
                        "DeleteState::FindCell: page_id: {}, cell_idx: {}",
                        page.get().id,
                        cell_idx
                    );

                    let cell = contents.cell_get(cell_idx, self.usable_space())?;

                    let original_child_pointer = match &cell {
                        BTreeCell::TableInteriorCell(interior) => Some(interior.left_child_page),
                        BTreeCell::IndexInteriorCell(interior) => Some(interior.left_child_page),
                        _ => None,
                    };

                    let delete_info = self.state.mut_delete_info().unwrap();
                    delete_info.state = DeleteState::ClearOverflowPages {
                        cell_idx,
                        cell,
                        original_child_pointer,
                        post_balancing_seek_key,
                    };
                }

                DeleteState::ClearOverflowPages {
                    cell_idx,
                    cell,
                    original_child_pointer,
                    post_balancing_seek_key,
                } => {
                    return_if_io!(self.clear_overflow_pages(&cell));

                    let page = self.stack.top();
                    let page = page.get();
                    let contents = page.get_contents();

                    let delete_info = self.state.mut_delete_info().unwrap();
                    if !contents.is_leaf() {
                        delete_info.state = DeleteState::InteriorNodeReplacement {
                            page: page.clone(),
                            btree_depth: self.stack.current(),
                            cell_idx,
                            original_child_pointer,
                            post_balancing_seek_key,
                        };
                    } else {
                        drop_cell(contents, cell_idx, self.usable_space() as u16)?;

                        let delete_info = self.state.mut_delete_info().unwrap();
                        delete_info.state = DeleteState::CheckNeedsBalancing {
                            btree_depth: self.stack.current(),
                            post_balancing_seek_key,
                        };
                    }
                }

                DeleteState::InteriorNodeReplacement {
                    page,
                    btree_depth,
                    cell_idx,
                    original_child_pointer,
                    post_balancing_seek_key,
                } => {
                    // This is an interior node, we need to handle deletion differently.
                    // 1. Move cursor to the largest key in the left subtree.
                    // 2. Replace the cell in the interior (parent) node with that key.
                    // 3. Delete that key from the child page.

                    // Step 1: Move cursor to the largest key in the left subtree.
                    // The largest key is always in a leaf, and so this traversal may involvegoing multiple pages downwards,
                    // so we store the page we are currently on.

                    // avoid calling prev() because it internally calls restore_context() which may cause unintended behavior.
                    return_if_io!(self.get_prev_record());

                    // Ensure we keep the parent page at the same position as before the replacement.
                    self.stack
                        .node_states
                        .borrow_mut()
                        .get_mut(btree_depth)
                        .expect("parent page should be on the stack")
                        .cell_idx = cell_idx as i32;
                    let (cell_payload, leaf_cell_idx) = {
                        let leaf_page_ref = self.stack.top();
                        let leaf_page = leaf_page_ref.get();
                        let leaf_contents = leaf_page.get().contents.as_ref().unwrap();
                        assert!(leaf_contents.is_leaf());
                        assert!(leaf_contents.cell_count() > 0);
                        let leaf_cell_idx = leaf_contents.cell_count() - 1;
                        let last_cell_on_child_page =
                            leaf_contents.cell_get(leaf_cell_idx, self.usable_space())?;

                        let mut cell_payload: Vec<u8> = Vec::new();
                        let child_pointer =
                            original_child_pointer.expect("there should be a pointer");
                        // Rewrite the old leaf cell as an interior cell depending on type.
                        match last_cell_on_child_page {
                            BTreeCell::TableLeafCell(leaf_cell) => {
                                // Table interior cells contain the left child pointer and the rowid as varint.
                                cell_payload.extend_from_slice(&child_pointer.to_be_bytes());
                                write_varint_to_vec(leaf_cell.rowid as u64, &mut cell_payload);
                            }
                            BTreeCell::IndexLeafCell(leaf_cell) => {
                                // Index interior cells contain:
                                // 1. The left child pointer
                                // 2. The payload size as varint
                                // 3. The payload
                                // 4. The first overflow page as varint, omitted if no overflow.
                                cell_payload.extend_from_slice(&child_pointer.to_be_bytes());
                                write_varint_to_vec(leaf_cell.payload_size, &mut cell_payload);
                                cell_payload.extend_from_slice(leaf_cell.payload);
                                if let Some(first_overflow_page) = leaf_cell.first_overflow_page {
                                    cell_payload
                                        .extend_from_slice(&first_overflow_page.to_be_bytes());
                                }
                            }
                            _ => unreachable!("Expected table leaf cell"),
                        }
                        (cell_payload, leaf_cell_idx)
                    };

                    let leaf_page = self.stack.top();

                    self.pager.add_dirty(&page);
                    self.pager.add_dirty(&leaf_page.get());

                    // Step 2: Replace the cell in the parent (interior) page.
                    {
                        let parent_contents = page.get_contents();
                        let parent_page_id = page.get().id;
                        let left_child_page = u32::from_be_bytes(
                            cell_payload[..4].try_into().expect("invalid cell payload"),
                        );
                        turso_assert!(
                            left_child_page as usize != parent_page_id,
                            "corrupt: current page and left child page of cell {} are both {}",
                            left_child_page,
                            parent_page_id
                        );

                        // First, drop the old cell that is being replaced.
                        drop_cell(parent_contents, cell_idx, self.usable_space() as u16)?;
                        // Then, insert the new cell (the predecessor) in its place.
                        insert_into_cell(
                            parent_contents,
                            &cell_payload,
                            cell_idx,
                            self.usable_space() as u16,
                        )?;
                    }

                    // Step 3: Delete the predecessor cell from the leaf page.
                    {
                        let leaf_page_ref = leaf_page.get();
                        let leaf_contents = leaf_page_ref.get_contents();
                        drop_cell(leaf_contents, leaf_cell_idx, self.usable_space() as u16)?;
                    }

                    let delete_info = self.state.mut_delete_info().unwrap();
                    delete_info.state = DeleteState::CheckNeedsBalancing {
                        btree_depth,
                        post_balancing_seek_key,
                    };
                }

                DeleteState::CheckNeedsBalancing {
                    btree_depth,
                    post_balancing_seek_key,
                } => {
                    let page = self.stack.top();
                    return_if_locked_maybe_load!(self.pager, page);
                    // Check if either the leaf page we took the replacement cell from underflows, or if the interior page we inserted it into overflows OR underflows.
                    // If the latter is true, we must always balance that level regardless of whether the leaf page (or any ancestor pages in between) need balancing.

                    let leaf_underflows = {
                        let leaf_page = page.get();
                        let leaf_contents = leaf_page.get_contents();
                        let free_space =
                            compute_free_space(leaf_contents, self.usable_space() as u16);
                        free_space as usize * 3 > self.usable_space() * 2
                    };

                    let interior_overflows_or_underflows = {
                        // Invariant: ancestor pages on the stack are pinned to the page cache,
                        // so we don't need return_if_locked_maybe_load! any ancestor,
                        // and we already loaded the current page above.
                        let interior_page = self
                            .stack
                            .get_page_at_level(btree_depth)
                            .expect("ancestor page should be on the stack");
                        let interior_page = interior_page.get();
                        let interior_contents = interior_page.get_contents();
                        let overflows = !interior_contents.overflow_cells.is_empty();
                        if overflows {
                            true
                        } else {
                            let free_space =
                                compute_free_space(interior_contents, self.usable_space() as u16);
                            free_space as usize * 3 > self.usable_space() * 2
                        }
                    };

                    let needs_balancing = leaf_underflows || interior_overflows_or_underflows;

                    if needs_balancing {
                        let delete_info = self.state.mut_delete_info().unwrap();
                        if delete_info.balance_write_info.is_none() {
                            let mut write_info = WriteInfo::new();
                            write_info.state = WriteState::BalanceStart;
                            delete_info.balance_write_info = Some(write_info);
                        }
                        let balance_only_ancestor =
                            !leaf_underflows && interior_overflows_or_underflows;
                        if balance_only_ancestor {
                            // Only need to balance the ancestor page; move there immediately.
                            while self.stack.current() > btree_depth {
                                self.stack.pop();
                            }
                        }
                        let balance_both = leaf_underflows && interior_overflows_or_underflows;
                        delete_info.state = DeleteState::WaitForBalancingToComplete {
                            balance_ancestor_at_depth: if balance_both {
                                Some(btree_depth)
                            } else {
                                None
                            },
                            target_key: post_balancing_seek_key.unwrap(),
                        }
                    } else {
                        // No balancing needed, we're done
                        self.stack.retreat();
                        self.state = CursorState::None;
                        return Ok(IOResult::Done(()));
                    }
                }

                DeleteState::WaitForBalancingToComplete {
                    target_key,
                    balance_ancestor_at_depth,
                } => {
                    let delete_info = self.state.mut_delete_info().unwrap();

                    // Switch the CursorState to Write state for balancing
                    let write_info = delete_info.balance_write_info.take().unwrap();
                    self.state = CursorState::Write(write_info);

                    match self.balance(balance_ancestor_at_depth)? {
                        IOResult::Done(()) => {
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

                        IOResult::IO => {
                            // Move to seek state
                            // Save balance progress and return IO
                            let write_info = match &self.state {
                                CursorState::Write(wi) => wi.clone(),
                                _ => unreachable!("Balance operation changed cursor state"),
                            };

                            self.state = CursorState::Delete(DeleteInfo {
                                state: DeleteState::WaitForBalancingToComplete {
                                    target_key,
                                    balance_ancestor_at_depth,
                                },
                                balance_write_info: Some(write_info),
                            });
                            return Ok(IOResult::IO);
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
                    // We want to end up pointing at the row to the left of the position of the row we deleted, so
                    // that after we call next() in the loop,the next row we delete will again be the same position as this one.
                    let seek_result = return_if_io!(self.seek(key, SeekOp::LT));

                    if let SeekResult::TryAdvance = seek_result {
                        let CursorState::Delete(delete_info) = &self.state else {
                            unreachable!("expected delete state");
                        };
                        self.state = CursorState::Delete(DeleteInfo {
                            state: DeleteState::TryAdvance,
                            balance_write_info: delete_info.balance_write_info.clone(),
                        });
                        continue;
                    }

                    self.state = CursorState::None;
                    return Ok(IOResult::Done(()));
                }
                DeleteState::TryAdvance => {
                    // we use LT always for post-delete seeks, which uses backwards iteration, so we always call prev() here.
                    return_if_io!(self.prev());
                    self.state = CursorState::None;
                    return Ok(IOResult::Done(()));
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

    #[instrument(skip_all, level = Level::DEBUG)]
    pub fn exists(&mut self, key: &Value) -> Result<IOResult<bool>> {
        assert!(self.mv_cursor.is_none());
        let int_key = match key {
            Value::Integer(i) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let seek_result =
            return_if_io!(self.seek(SeekKey::TableRowId(*int_key), SeekOp::GE { eq_only: true }));
        let exists = matches!(seek_result, SeekResult::Found);
        self.invalidate_record();
        Ok(IOResult::Done(exists))
    }

    /// Clear the overflow pages linked to a specific page provided by the leaf cell
    /// Uses a state machine to keep track of it's operations so that traversal can be
    /// resumed from last point after IO interruption
    #[instrument(skip_all, level = Level::DEBUG)]
    fn clear_overflow_pages(&mut self, cell: &BTreeCell) -> Result<IOResult<()>> {
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
                        BTreeCell::TableInteriorCell(_) => return Ok(IOResult::Done(())), // No overflow pages
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
                        || next_page as usize
                            > self
                                .pager
                                .io
                                .block(|| self.pager.with_header(|header| header.database_size))?
                                .get() as usize
                    {
                        self.overflow_state = None;
                        return Err(LimboError::Corrupt("Invalid overflow page number".into()));
                    }
                    let (page, _c) = self.read_page(next_page as usize)?;
                    return_if_locked_maybe_load!(self.pager, page);

                    let page = page.get();
                    let contents = page.get().contents.as_ref().unwrap();
                    let next = contents.read_u32(0);

                    return_if_io!(self.pager.free_page(Some(page), next_page as usize));

                    if next != 0 {
                        self.overflow_state = Some(OverflowState::ProcessPage { next_page: next });
                    } else {
                        self.overflow_state = Some(OverflowState::Done);
                    }
                }
                OverflowState::Done => {
                    self.overflow_state = None;
                    return Ok(IOResult::Done(()));
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
    #[instrument(skip(self), level = Level::DEBUG)]
    pub fn btree_destroy(&mut self) -> Result<IOResult<Option<usize>>> {
        if let CursorState::None = &self.state {
            let _c = self.move_to_root()?;
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
                    self.stack.advance();
                    assert!(page.get().is_loaded()); //  page should be loaded at this time
                    let page = page.get();
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
                                    let (rightmost_page, _c) =
                                        self.read_page(rightmost as usize)?;
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
                    let cell = contents.cell_get(cell_idx as usize, self.usable_space())?;

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
                                    BTreeCell::TableInteriorCell(cell) => cell.left_child_page,
                                    BTreeCell::IndexInteriorCell(cell) => cell.left_child_page,
                                    _ => panic!("expected interior cell"),
                                };
                                let (child_page, _c) = self.read_page(child_page_id as usize)?;
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
                        IOResult::Done(_) => match cell {
                            //  For an index interior cell, clear the left child page now that overflow pages have been cleared
                            BTreeCell::IndexInteriorCell(index_int_cell) => {
                                let (child_page, _c) =
                                    self.read_page(index_int_cell.left_child_page as usize)?;
                                self.stack.push(child_page);
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::LoadPage;
                                continue;
                            }
                            //  For any leaf cell, advance the index now that overflow pages have been cleared
                            BTreeCell::TableLeafCell(_) | BTreeCell::IndexLeafCell(_) => {
                                let destroy_info = self.state.mut_destroy_info().expect(
                                    "unable to get a mut reference to destroy state in cursor",
                                );
                                destroy_info.state = DestroyState::LoadPage;
                            }
                            _ => panic!("unexpected cell type"),
                        },
                        IOResult::IO => return Ok(IOResult::IO),
                    }
                }
                DestroyState::FreePage => {
                    let page = self.stack.top();
                    let page_id = page.get().get().id;

                    return_if_io!(self.pager.free_page(Some(page.get()), page_id));

                    if self.stack.has_parent() {
                        self.stack.pop();
                        let destroy_info = self
                            .state
                            .mut_destroy_info()
                            .expect("unable to get a mut reference to destroy state in cursor");
                        destroy_info.state = DestroyState::ProcessPage;
                    } else {
                        self.state = CursorState::None;
                        //  TODO: For now, no-op the result return None always. This will change once [AUTO_VACUUM](https://www.sqlite.org/lang_vacuum.html) is introduced
                        //  At that point, the last root page(call this x) will be moved into the position of the root page of this table and the value returned will be x
                        return Ok(IOResult::Done(None));
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
        page_ref: BTreePage,
        cell_idx: usize,
        record: &ImmutableRecord,
        state: &mut OverwriteCellState,
    ) -> Result<IOResult<()>> {
        loop {
            turso_assert!(
                page_ref.get().is_loaded(),
                "page {} is not loaded",
                page_ref.get().get().id
            );
            match state {
                OverwriteCellState::AllocatePayload => {
                    let serial_types_len = self.record_cursor.borrow_mut().len(record);
                    let new_payload = Vec::with_capacity(serial_types_len);
                    let rowid = return_if_io!(self.rowid());
                    *state = OverwriteCellState::FillPayload {
                        new_payload: Arc::new(Mutex::new(new_payload)),
                        rowid,
                        fill_cell_payload_state: FillCellPayloadState::Start,
                    };
                    continue;
                }
                OverwriteCellState::FillPayload {
                    new_payload,
                    rowid,
                    fill_cell_payload_state,
                } => {
                    let page = page_ref.get();
                    let page_contents = page.get().contents.as_ref().unwrap();
                    {
                        let mut new_payload_mut = new_payload.lock();
                        let new_payload_mut = &mut *new_payload_mut;
                        return_if_io!(fill_cell_payload(
                            page_contents,
                            *rowid,
                            new_payload_mut,
                            cell_idx,
                            record,
                            self.usable_space(),
                            self.pager.clone(),
                            fill_cell_payload_state,
                        ));
                    }
                    // figure out old cell offset & size
                    let (old_offset, old_local_size) = {
                        let page_ref = page_ref.get();
                        let page = page_ref.get().contents.as_ref().unwrap();
                        page.cell_get_raw_region(cell_idx, self.usable_space())
                    };

                    *state = OverwriteCellState::ClearOverflowPagesAndOverwrite {
                        new_payload: new_payload.clone(),
                        old_offset,
                        old_local_size,
                    };
                    continue;
                }
                OverwriteCellState::ClearOverflowPagesAndOverwrite {
                    new_payload,
                    old_offset,
                    old_local_size,
                } => {
                    let page = page_ref.get();
                    let page_contents = page.get().contents.as_ref().unwrap();
                    let cell = page_contents.cell_get(cell_idx, self.usable_space())?;
                    return_if_io!(self.clear_overflow_pages(&cell));

                    let mut new_payload = new_payload.lock();
                    let new_payload = &mut *new_payload;
                    // if it all fits in local space and old_local_size is enough, do an in-place overwrite
                    if new_payload.len() == *old_local_size {
                        let _res =
                            self.overwrite_content(page_ref.clone(), *old_offset, new_payload)?;
                        return Ok(IOResult::Done(()));
                    }

                    drop_cell(
                        page_ref.get().get_contents(),
                        cell_idx,
                        self.usable_space() as u16,
                    )?;
                    insert_into_cell(
                        page_ref.get().get_contents(),
                        new_payload,
                        cell_idx,
                        self.usable_space() as u16,
                    )?;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    pub fn overwrite_content(
        &mut self,
        page_ref: BTreePage,
        dest_offset: usize,
        new_payload: &[u8],
    ) -> Result<IOResult<()>> {
        return_if_locked!(page_ref.get());
        let page_ref = page_ref.get();
        let buf = page_ref.get().contents.as_mut().unwrap().as_ptr();
        buf[dest_offset..dest_offset + new_payload.len()].copy_from_slice(new_payload);

        Ok(IOResult::Done(()))
    }

    fn get_immutable_record_or_create(&self) -> std::cell::RefMut<'_, Option<ImmutableRecord>> {
        if self.reusable_immutable_record.borrow().is_none() {
            let record = ImmutableRecord::new(4096);
            self.reusable_immutable_record.replace(Some(record));
        }
        self.reusable_immutable_record.borrow_mut()
    }

    fn get_immutable_record(&self) -> std::cell::RefMut<'_, Option<ImmutableRecord>> {
        self.reusable_immutable_record.borrow_mut()
    }

    pub fn is_write_in_progress(&self) -> bool {
        matches!(self.state, CursorState::Write(_))
    }

    /// Count the number of entries in the b-tree
    ///
    /// Only supposed to be used in the context of a simple Count Select Statement
    #[instrument(skip(self), level = Level::DEBUG)]
    pub fn count(&mut self) -> Result<IOResult<usize>> {
        if self.count == 0 {
            let _c = self.move_to_root()?;
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
            contents = mem_page.get().contents.as_ref().unwrap();

            /* If this is a leaf page or the tree is not an int-key tree, then
             ** this page contains countable entries. Increment the entry counter
             ** accordingly.
             */
            if !matches!(contents.page_type(), PageType::TableInterior) {
                self.count += contents.cell_count();
            }

            self.stack.advance();
            let cell_idx = self.stack.current_cell_index() as usize;

            // Second condition is necessary in case we return if the page is locked in the loop below
            if contents.is_leaf() || cell_idx > contents.cell_count() {
                loop {
                    if !self.stack.has_parent() {
                        // All pages of the b-tree have been visited. Return successfully
                        let _c = self.move_to_root()?;

                        return Ok(IOResult::Done(self.count));
                    }

                    // Move to parent
                    self.stack.pop();

                    mem_page_rc = self.stack.top();
                    return_if_locked_maybe_load!(self.pager, mem_page_rc);
                    mem_page = mem_page_rc.get();
                    contents = mem_page.get().contents.as_ref().unwrap();

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
                let (mem_page, _c) = self.read_page(right_most_pointer as usize)?;
                self.stack.push(mem_page);
            } else {
                // Move to child left page
                let cell = contents.cell_get(cell_idx, self.usable_space())?;

                match cell {
                    BTreeCell::TableInteriorCell(TableInteriorCell {
                        left_child_page, ..
                    })
                    | BTreeCell::IndexInteriorCell(IndexInteriorCell {
                        left_child_page, ..
                    }) => {
                        self.stack.advance();
                        let (mem_page, _c) = self.read_page(left_child_page as usize)?;
                        self.stack.push(mem_page);
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    // Save cursor context, to be restored later
    pub fn save_context(&mut self, cursor_context: CursorContext) {
        self.valid_state = CursorValidState::RequireSeek;
        self.context = Some(cursor_context);
    }

    /// If context is defined, restore it and set it None on success
    #[instrument(skip_all, level = Level::DEBUG)]
    fn restore_context(&mut self) -> Result<IOResult<()>> {
        if self.context.is_none() || matches!(self.valid_state, CursorValidState::Valid) {
            return Ok(IOResult::Done(()));
        }
        if let CursorValidState::RequireAdvance(direction) = self.valid_state {
            let has_record = return_if_io!(match direction {
                // Avoid calling next()/prev() directly because they immediately call restore_context()
                IterationDirection::Forwards => self.get_next_record(),
                IterationDirection::Backwards => self.get_prev_record(),
            });
            self.has_record.set(has_record);
            self.invalidate_record();
            self.context = None;
            self.valid_state = CursorValidState::Valid;
            return Ok(IOResult::Done(()));
        }
        let ctx = self.context.take().unwrap();
        let seek_key = match ctx {
            CursorContext::TableRowId(rowid) => SeekKey::TableRowId(rowid),
            CursorContext::IndexKeyRowId(ref record) => SeekKey::IndexKey(record),
        };
        let res = self.seek(seek_key, SeekOp::GE { eq_only: true })?;
        match res {
            IOResult::Done(res) => {
                if let SeekResult::TryAdvance = res {
                    self.valid_state =
                        CursorValidState::RequireAdvance(IterationDirection::Forwards);
                    self.context = Some(ctx);
                    return Ok(IOResult::IO);
                }
                self.valid_state = CursorValidState::Valid;
                Ok(IOResult::Done(()))
            }
            IOResult::IO => {
                self.context = Some(ctx);
                Ok(IOResult::IO)
            }
        }
    }

    pub fn read_page(&self, page_idx: usize) -> Result<(BTreePage, Completion)> {
        btree_read_page(&self.pager, page_idx)
    }

    pub fn allocate_page(&self, page_type: PageType, offset: usize) -> Result<IOResult<BTreePage>> {
        self.pager
            .do_allocate_page(page_type, offset, BtreePageAllocMode::Any)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IntegrityCheckError {
    #[error("Cell {cell_idx} in page {page_id} is out of range. cell_range={cell_start}..{cell_end}, content_area={content_area}, usable_space={usable_space}")]
    CellOutOfRange {
        cell_idx: usize,
        page_id: usize,
        cell_start: usize,
        cell_end: usize,
        content_area: usize,
        usable_space: usize,
    },
    #[error("Cell {cell_idx} in page {page_id} extends out of page. cell_range={cell_start}..{cell_end}, content_area={content_area}, usable_space={usable_space}")]
    CellOverflowsPage {
        cell_idx: usize,
        page_id: usize,
        cell_start: usize,
        cell_end: usize,
        content_area: usize,
        usable_space: usize,
    },
    #[error("Page {page_id} cell {cell_idx} has rowid={rowid} in wrong order. Parent cell has parent_rowid={max_intkey} and next_rowid={next_rowid}")]
    CellRowidOutOfRange {
        page_id: usize,
        cell_idx: usize,
        rowid: i64,
        max_intkey: i64,
        next_rowid: i64,
    },
    #[error("Page {page_id} is at different depth from another leaf page this_page_depth={this_page_depth}, other_page_depth={other_page_depth} ")]
    LeafDepthMismatch {
        page_id: usize,
        this_page_depth: usize,
        other_page_depth: usize,
    },
    #[error("Page {page_id} detected freeblock that extends page start={start} end={end}")]
    FreeBlockOutOfRange {
        page_id: usize,
        start: usize,
        end: usize,
    },
    #[error("Page {page_id} cell overlap detected at position={start} with previous_end={prev_end}. content_area={content_area}, is_free_block={is_free_block}")]
    CellOverlap {
        page_id: usize,
        start: usize,
        prev_end: usize,
        content_area: usize,
        is_free_block: bool,
    },
    #[error("Page {page_id} unexpected fragmentation got={got}, expected={expected}")]
    UnexpectedFragmentation {
        page_id: usize,
        got: usize,
        expected: usize,
    },
}

#[derive(Clone)]
struct IntegrityCheckPageEntry {
    page_idx: usize,
    level: usize,
    max_intkey: i64,
}
pub struct IntegrityCheckState {
    pub current_page: usize,
    page_stack: Vec<IntegrityCheckPageEntry>,
    first_leaf_level: Option<usize>,
}

impl IntegrityCheckState {
    pub fn new(page_idx: usize) -> Self {
        Self {
            current_page: page_idx,
            page_stack: vec![IntegrityCheckPageEntry {
                page_idx,
                level: 0,
                max_intkey: i64::MAX,
            }],
            first_leaf_level: None,
        }
    }
}
impl std::fmt::Debug for IntegrityCheckState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IntegrityCheckState")
            .field("current_page", &self.current_page)
            .field("first_leaf_level", &self.first_leaf_level)
            .finish()
    }
}

/// Perform integrity check on a whole table/index. We check for:
/// 1. Correct order of keys in case of rowids.
/// 2. There are no overlap between cells.
/// 3. Cells do not scape outside expected range.
/// 4. Depth of leaf pages are equal.
/// 5. Overflow pages are correct (TODO)
///
/// In order to keep this reentrant, we keep a stack of pages we need to check. Ideally, like in
/// SQLlite, we would have implemented a recursive solution which would make it easier to check the
/// depth.
pub fn integrity_check(
    state: &mut IntegrityCheckState,
    errors: &mut Vec<IntegrityCheckError>,
    pager: &Rc<Pager>,
) -> Result<IOResult<()>> {
    let Some(IntegrityCheckPageEntry {
        page_idx,
        level,
        max_intkey,
    }) = state.page_stack.last().cloned()
    else {
        return Ok(IOResult::Done(()));
    };
    let (page, _c) = btree_read_page(pager, page_idx)?;
    return_if_locked_maybe_load!(pager, page);
    state.page_stack.pop();

    let page = page.get();
    let contents = page.get_contents();
    let usable_space = pager.usable_space() as u16;
    let mut coverage_checker = CoverageChecker::new(page.get().id);

    // Now we check every cell for few things:
    // 1. Check cell is in correct range. Not exceeds page and not starts before we have marked
    //    (cell content area).
    // 2. We add the cell to coverage checker in order to check if cells do not overlap.
    // 3. We check order of rowids in case of table pages. We iterate backwards in order to check
    //    if current cell's rowid is less than the next cell. We also check rowid is less than the
    //    parent's divider cell. In case of this page being root page max rowid will be i64::MAX.
    // 4. We append pages to the stack to check later.
    // 5. In case of leaf page, check if the current level(depth) is equal to other leaf pages we
    //    have seen.
    let mut next_rowid = max_intkey;
    for cell_idx in (0..contents.cell_count()).rev() {
        let (cell_start, cell_length) =
            contents.cell_get_raw_region(cell_idx, usable_space as usize);
        if cell_start < contents.cell_content_area() as usize
            || cell_start > usable_space as usize - 4
        {
            errors.push(IntegrityCheckError::CellOutOfRange {
                cell_idx,
                page_id: page.get().id,
                cell_start,
                cell_end: cell_start + cell_length,
                content_area: contents.cell_content_area() as usize,
                usable_space: usable_space as usize,
            });
        }
        if cell_start + cell_length > usable_space as usize {
            errors.push(IntegrityCheckError::CellOverflowsPage {
                cell_idx,
                page_id: page.get().id,
                cell_start,
                cell_end: cell_start + cell_length,
                content_area: contents.cell_content_area() as usize,
                usable_space: usable_space as usize,
            });
        }
        coverage_checker.add_cell(cell_start, cell_start + cell_length);
        let cell = contents.cell_get(cell_idx, usable_space as usize)?;
        match cell {
            BTreeCell::TableInteriorCell(table_interior_cell) => {
                state.page_stack.push(IntegrityCheckPageEntry {
                    page_idx: table_interior_cell.left_child_page as usize,
                    level: level + 1,
                    max_intkey: table_interior_cell.rowid,
                });
                let rowid = table_interior_cell.rowid;
                if rowid > max_intkey || rowid > next_rowid {
                    errors.push(IntegrityCheckError::CellRowidOutOfRange {
                        page_id: page.get().id,
                        cell_idx,
                        rowid,
                        max_intkey,
                        next_rowid,
                    });
                }
                next_rowid = rowid;
            }
            BTreeCell::TableLeafCell(table_leaf_cell) => {
                // check depth of leaf pages are equal
                if let Some(expected_leaf_level) = state.first_leaf_level {
                    if expected_leaf_level != level {
                        errors.push(IntegrityCheckError::LeafDepthMismatch {
                            page_id: page.get().id,
                            this_page_depth: level,
                            other_page_depth: expected_leaf_level,
                        });
                    }
                } else {
                    state.first_leaf_level = Some(level);
                }
                let rowid = table_leaf_cell.rowid;
                if rowid > max_intkey || rowid > next_rowid {
                    errors.push(IntegrityCheckError::CellRowidOutOfRange {
                        page_id: page.get().id,
                        cell_idx,
                        rowid,
                        max_intkey,
                        next_rowid,
                    });
                }
                next_rowid = rowid;
            }
            BTreeCell::IndexInteriorCell(index_interior_cell) => {
                state.page_stack.push(IntegrityCheckPageEntry {
                    page_idx: index_interior_cell.left_child_page as usize,
                    level: level + 1,
                    max_intkey, // we don't care about intkey in non-table pages
                });
            }
            BTreeCell::IndexLeafCell(_) => {
                // check depth of leaf pages are equal
                if let Some(expected_leaf_level) = state.first_leaf_level {
                    if expected_leaf_level != level {
                        errors.push(IntegrityCheckError::LeafDepthMismatch {
                            page_id: page.get().id,
                            this_page_depth: level,
                            other_page_depth: expected_leaf_level,
                        });
                    }
                } else {
                    state.first_leaf_level = Some(level);
                }
            }
        }
    }

    // Now we add free blocks to the coverage checker
    let first_freeblock = contents.first_freeblock();
    if first_freeblock > 0 {
        let mut pc = first_freeblock;
        while pc > 0 {
            let next = contents.read_u16_no_offset(pc as usize);
            let size = contents.read_u16_no_offset(pc as usize + 2) as usize;
            // check it doesn't go out of range
            if pc > usable_space - 4 {
                errors.push(IntegrityCheckError::FreeBlockOutOfRange {
                    page_id: page.get().id,
                    start: pc as usize,
                    end: pc as usize + size,
                });
                break;
            }
            coverage_checker.add_free_block(pc as usize, pc as usize + size);
            pc = next;
        }
    }

    // Let's check the overlap of freeblocks and cells now that we have collected them all.
    coverage_checker.analyze(
        usable_space,
        contents.cell_content_area() as usize,
        errors,
        contents.num_frag_free_bytes() as usize,
    );

    Ok(IOResult::Done(()))
}

pub fn btree_read_page(pager: &Rc<Pager>, page_idx: usize) -> Result<(BTreePage, Completion)> {
    pager.read_page(page_idx).map(|(page, c)| {
        (
            Arc::new(BTreePageInner {
                page: RefCell::new(page),
            }),
            c,
        )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IntegrityCheckCellRange {
    start: usize,
    end: usize,
    is_free_block: bool,
}

// Implement ordering for min-heap (smallest start address first)
impl Ord for IntegrityCheckCellRange {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.start.cmp(&other.start)
    }
}

impl PartialOrd for IntegrityCheckCellRange {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(debug_assertions)]
fn validate_cells_after_insertion(cell_array: &CellArray, leaf_data: bool) {
    for cell in &cell_array.cell_payloads {
        assert!(cell.len() >= 4);

        if leaf_data {
            assert!(cell[0] != 0, "payload is {cell:?}");
        }
    }
}

pub struct CoverageChecker {
    /// Min-heap ordered by cell start
    heap: BinaryHeap<Reverse<IntegrityCheckCellRange>>,
    page_idx: usize,
}

impl CoverageChecker {
    pub fn new(page_idx: usize) -> Self {
        Self {
            heap: BinaryHeap::new(),
            page_idx,
        }
    }

    fn add_range(&mut self, cell_start: usize, cell_end: usize, is_free_block: bool) {
        self.heap.push(Reverse(IntegrityCheckCellRange {
            start: cell_start,
            end: cell_end,
            is_free_block,
        }));
    }

    pub fn add_cell(&mut self, cell_start: usize, cell_end: usize) {
        self.add_range(cell_start, cell_end, false);
    }

    pub fn add_free_block(&mut self, cell_start: usize, cell_end: usize) {
        self.add_range(cell_start, cell_end, true);
    }

    pub fn analyze(
        &mut self,
        usable_space: u16,
        content_area: usize,
        errors: &mut Vec<IntegrityCheckError>,
        expected_fragmentation: usize,
    ) {
        let mut fragmentation = 0;
        let mut prev_end = content_area;
        while let Some(cell) = self.heap.pop() {
            let start = cell.0.start;
            if prev_end > start {
                errors.push(IntegrityCheckError::CellOverlap {
                    page_id: self.page_idx,
                    start,
                    prev_end,
                    content_area,
                    is_free_block: cell.0.is_free_block,
                });
                break;
            } else {
                fragmentation += start - prev_end;
                prev_end = cell.0.end;
            }
        }
        fragmentation += usable_space as usize - prev_end;
        if fragmentation != expected_fragmentation {
            errors.push(IntegrityCheckError::UnexpectedFragmentation {
                page_id: self.page_idx,
                got: fragmentation,
                expected: expected_fragmentation,
            });
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
    pub stack: RefCell<[Option<BTreePage>; BTCURSOR_MAX_DEPTH + 1]>,
    /// List of cell indices in the stack.
    /// node_states[current_page] is the current cell index being consumed. Similarly
    /// node_states[current_page-1] is the cell index of the parent of the current page
    /// that we save in case of going back up.
    /// There are two points that need special attention:
    ///  If node_states[current_page] = -1, it indicates that the current iteration has reached the start of the current_page
    ///  If node_states[current_page] = `cell_count`, it means that the current iteration has reached the end of the current_page
    node_states: RefCell<[BTreeNodeState; BTCURSOR_MAX_DEPTH + 1]>,
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
    #[instrument(skip_all, level = Level::DEBUG, name = "pagestack::push")]
    fn _push(&self, page: BTreePage, starting_cell_idx: i32) {
        tracing::trace!(
            current = self.current_page.get(),
            new_page_id = page.get().get().id,
        );
        'validate: {
            let current = self.current_page.get();
            if current == -1 {
                break 'validate;
            }
            let stack = self.stack.borrow();
            let current_top = stack[current as usize].as_ref();
            if let Some(current_top) = current_top {
                turso_assert!(
                    current_top.get().get().id != page.get().get().id,
                    "about to push page {} twice",
                    page.get().get().id
                );
            }
        }
        self.populate_parent_cell_count();
        self.increment_current();
        let current = self.current_page.get();
        assert!(
            current < BTCURSOR_MAX_DEPTH as i32,
            "corrupted database, stack is bigger than expected"
        );
        assert!(current >= 0);

        // Pin the page to prevent it from being evicted while on the stack
        page.get().pin();

        self.stack.borrow_mut()[current as usize] = Some(page);
        self.node_states.borrow_mut()[current as usize] = BTreeNodeState {
            cell_idx: starting_cell_idx,
            cell_count: None, // we don't know the cell count yet, so we set it to None. any code pushing a child page onto the stack MUST set the parent page's cell_count.
        };
    }

    /// Populate the parent page's cell count.
    /// This is needed so that we can, from a child page, check of ancestor pages' position relative to its cell index
    /// without having to perform IO to get the ancestor page contents.
    ///
    /// This rests on the assumption that the parent page is already in memory whenever a child is pushed onto the stack.
    /// We currently ensure this by pinning all the pages on [PageStack] to the page cache so that they cannot be evicted.
    fn populate_parent_cell_count(&self) {
        let stack_empty = self.current_page.get() == -1;
        if stack_empty {
            return;
        }
        let current = self.current();
        let stack = self.stack.borrow();
        let page = stack[current].as_ref().unwrap();
        let page = page.get();
        turso_assert!(
            page.is_pinned(),
            "parent page {} is not pinned",
            page.get().id
        );
        turso_assert!(
            page.is_loaded(),
            "parent page {} is not loaded",
            page.get().id
        );
        let contents = page.get_contents();
        let cell_count = contents.cell_count() as i32;
        self.node_states.borrow_mut()[current].cell_count = Some(cell_count);
    }

    fn push(&self, page: BTreePage) {
        self._push(page, -1);
    }

    fn push_backwards(&self, page: BTreePage) {
        self._push(page, i32::MAX);
    }

    /// Pop a page off the stack.
    /// This effectively means traversing back up to a parent page.
    #[instrument(skip_all, level = Level::DEBUG, name = "pagestack::pop")]
    fn pop(&self) {
        let current = self.current_page.get();
        assert!(current >= 0);
        tracing::trace!(current);

        // Unpin the page before removing it from the stack
        if let Some(page) = &self.stack.borrow()[current as usize] {
            page.get().unpin();
        }

        self.node_states.borrow_mut()[current as usize] = BTreeNodeState::default();
        self.stack.borrow_mut()[current as usize] = None;
        self.decrement_current();
    }

    /// Get the top page on the stack.
    /// This is the page that is currently being traversed.
    #[instrument(skip(self), level = Level::DEBUG, name = "pagestack::top", )]
    fn top(&self) -> BTreePage {
        let page = self.stack.borrow()[self.current()]
            .as_ref()
            .unwrap()
            .clone();
        tracing::trace!(current = self.current(), page_id = page.get().get().id);
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
        self.node_states.borrow()[current].cell_idx
    }

    /// Check if the current cell index is less than 0.
    /// This means we have been iterating backwards and have reached the start of the page.
    fn current_cell_index_less_than_min(&self) -> bool {
        let cell_idx = self.current_cell_index();
        cell_idx < 0
    }

    /// Advance the current cell index of the current page to the next cell.
    /// We usually advance after going traversing a new page
    #[instrument(skip(self), level = Level::DEBUG, name = "pagestack::advance",)]
    fn advance(&self) {
        let current = self.current();
        tracing::trace!(
            curr_cell_index = self.node_states.borrow()[current].cell_idx,
            node_states = ?self.node_states.borrow().iter().map(|state| state.cell_idx).collect::<Vec<_>>(),
        );
        self.node_states.borrow_mut()[current].cell_idx += 1;
    }

    #[instrument(skip(self), level = Level::DEBUG, name = "pagestack::retreat")]
    fn retreat(&self) {
        let current = self.current();
        tracing::trace!(
            curr_cell_index = self.node_states.borrow()[current].cell_idx,
            node_states = ?self.node_states.borrow().iter().map(|state| state.cell_idx).collect::<Vec<_>>(),
        );
        self.node_states.borrow_mut()[current].cell_idx -= 1;
    }

    fn set_cell_index(&self, idx: i32) {
        let current = self.current();
        self.node_states.borrow_mut()[current].cell_idx = idx;
    }

    fn has_parent(&self) -> bool {
        self.current_page.get() > 0
    }

    /// Get a page at a specific level in the stack (0 = root, 1 = first child, etc.)
    fn get_page_at_level(&self, level: usize) -> Option<BTreePage> {
        let stack = self.stack.borrow();
        if level < stack.len() {
            stack[level].clone()
        } else {
            None
        }
    }

    fn unpin_all_if_pinned(&self) {
        self.stack
            .borrow_mut()
            .iter_mut()
            .flatten()
            .for_each(|page| {
                let _ = page.get().try_unpin();
            });
    }

    fn clear(&self) {
        self.unpin_all_if_pinned();

        self.current_page.set(-1);
    }
}

impl Drop for PageStack {
    fn drop(&mut self) {
        self.unpin_all_if_pinned();
    }
}

/// Used for redistributing cells during a balance operation.
struct CellArray {
    /// The actual cell data.
    /// For all other page types except table leaves, this will also contain the associated divider cell from the parent page.
    cell_payloads: Vec<&'static mut [u8]>,

    /// Prefix sum of cells in each page.
    /// For example, if three pages have 1, 2, and 3 cells, respectively,
    /// then cell_count_per_page_cumulative will be [1, 3, 6].
    cell_count_per_page_cumulative: [u16; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
}

impl CellArray {
    pub fn cell_size_bytes(&self, cell_idx: usize) -> u16 {
        self.cell_payloads[cell_idx].len() as u16
    }

    /// Returns the number of cells up to and including the given page.
    pub fn cell_count_up_to_page(&self, page_idx: usize) -> usize {
        self.cell_count_per_page_cumulative[page_idx] as usize
    }
}

impl BTreePageInner {
    pub fn get(&self) -> PageRef {
        self.page.borrow().clone()
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

pub fn btree_init_page(page: &BTreePage, page_type: PageType, offset: usize, usable_space: u16) {
    // setup btree page
    let contents = page.get();
    tracing::debug!(
        "btree_init_page(id={}, offset={})",
        contents.get().id,
        offset
    );
    let contents = contents.get().contents.as_mut().unwrap();
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
        cell_array.cell_payloads.len()
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
        let cell = &cell_array.cell_payloads[i];
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
        insert_into_cell_during_balance(
            page,
            cell_array.cell_payloads[i],
            start_insert,
            usable_space,
        )?;
        start_insert += 1;
    }
    debug_validate_cells!(page, usable_space);
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

    if (offset as u32) <= page.cell_content_area() {
        if (offset as u32) < page.cell_content_area() {
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

            let (_, size) = cloned_page.cell_get_raw_region(i, usable_space as usize);
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
        let (offset, size) = page.cell_get_raw_region(i, usable_space as usize);
        let buf = &page.as_ptr()[offset..offset + size];
        // E.g. the following table btree cell may just have two bytes:
        // Payload size 0 (stored as SerialTypeKind::ConstInt0)
        // Rowid 1 (stored as SerialTypeKind::ConstInt1)
        assert!(
            size >= 2,
            "cell size should be at least 2 bytes idx={i}, cell={buf:?}, offset={offset}"
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
fn _insert_into_cell(
    page: &mut PageContent,
    payload: &[u8],
    cell_idx: usize,
    usable_space: u16,
    allow_regular_insert_despite_overflow: bool, // see [insert_into_cell_during_balance()]
) -> Result<()> {
    assert!(
        cell_idx <= page.cell_count() + page.overflow_cells.len(),
        "attempting to add cell to an incorrect place cell_idx={} cell_count={} page_type={:?}",
        cell_idx,
        page.cell_count(),
        page.page_type()
    );
    let already_has_overflow = !page.overflow_cells.is_empty();
    let enough_space = if already_has_overflow && !allow_regular_insert_despite_overflow {
        false
    } else {
        // otherwise, we need to check if we have enough space
        let free = compute_free_space(page, usable_space);
        payload.len() + CELL_PTR_SIZE_BYTES <= free as usize
    };
    if !enough_space {
        // add to overflow cell
        page.overflow_cells.push(OverflowCell {
            index: cell_idx,
            payload: Pin::new(Vec::from(payload)),
        });
        return Ok(());
    }
    assert!(
        cell_idx <= page.cell_count(),
        "cell_idx > page.cell_count() without overflow cells"
    );

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
    let cell_pointer_cur_idx = cell_pointer_array_start + (CELL_PTR_SIZE_BYTES * cell_idx);

    // move existing pointers forward by CELL_PTR_SIZE_BYTES...
    let n_cells_forward = page.cell_count() - cell_idx;
    let n_bytes_forward = CELL_PTR_SIZE_BYTES * n_cells_forward;
    if n_bytes_forward > 0 {
        buf.copy_within(
            cell_pointer_cur_idx..cell_pointer_cur_idx + n_bytes_forward,
            cell_pointer_cur_idx + CELL_PTR_SIZE_BYTES,
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

fn insert_into_cell(
    page: &mut PageContent,
    payload: &[u8],
    cell_idx: usize,
    usable_space: u16,
) -> Result<()> {
    _insert_into_cell(page, payload, cell_idx, usable_space, false)
}

/// Normally in [insert_into_cell()], if a page already has overflow cells, all
/// new insertions are also added to the overflow cells vector.
/// SQLite doesn't use regular [insert_into_cell()] during balancing,
/// so we have a specialized function for use during balancing that allows regular cell insertion
/// despite the presence of existing overflow cells (overflow cells are one of the reasons we are balancing in the first place).
/// During balancing cells are first repositioned with [edit_page()]
/// and then inserted via [page_insert_array()] which calls [insert_into_cell_during_balance()],
/// and finally the existing overflow cells are cleared.
/// If we would not allow the cell insert to proceed normally despite overflow cells being present,
/// the new insertions would also be added as overflow cells which defeats the point of balancing.
fn insert_into_cell_during_balance(
    page: &mut PageContent,
    payload: &[u8],
    cell_idx: usize,
    usable_space: u16,
) -> Result<()> {
    _insert_into_cell(page, payload, cell_idx, usable_space, true)
}

/// The amount of free space is the sum of:
///  #1. The size of the unallocated region
///  #2. Fragments (isolated 1-3 byte chunks of free space within the cell content area)
///  #3. freeblocks (linked list of blocks of at least 4 bytes within the cell content area that
///      are not in use due to e.g. deletions)
/// Free blocks can be zero, meaning the "real free space" that can be used to allocate is expected
/// to be between first cell byte and end of cell pointer area.
#[allow(unused_assignments)]
fn compute_free_space(page: &PageContent, usable_space: u16) -> u16 {
    // TODO(pere): maybe free space is not calculated correctly with offset

    // Usable space, not the same as free space, simply means:
    // space that is not reserved for extensions by sqlite. Usually reserved_space is 0.
    let usable_space = usable_space as usize;

    let first_cell = page.offset + page.header_size() + (2 * page.cell_count());
    let cell_content_area_start = page.cell_content_area() as usize;
    let mut free_space_bytes = cell_content_area_start + page.num_frag_free_bytes() as usize;

    // #3 is computed by iterating over the freeblocks linked list
    let mut cur_freeblock_ptr = page.first_freeblock() as usize;
    if cur_freeblock_ptr > 0 {
        if cur_freeblock_ptr < cell_content_area_start {
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
            // so the next pointer should > current pointer + its size, or 0 if no next block exists.
            if next <= cur_freeblock_ptr + size + 3 {
                break;
            }
            cur_freeblock_ptr = next;
        }

        // Next should always be 0 (NULL) at this point since we have reached the end of the freeblocks linked list
        assert_eq!(
            next, 0,
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

    free_space_bytes as u16 - first_cell as u16
}

/// Allocate space for a cell on a page.
fn allocate_cell_space(page_ref: &PageContent, amount: u16, usable_space: u16) -> Result<u16> {
    let mut amount = amount as usize;
    if amount < MINIMUM_CELL_SIZE {
        amount = MINIMUM_CELL_SIZE;
    }

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

#[derive(Debug, Clone)]
pub enum FillCellPayloadState {
    Start,
    AllocateOverflowPages {
        /// Arc because we clone [WriteState] for some reason and we use unsafe pointer dereferences in [FillCellPayloadState::AllocateOverflowPages]
        /// so the underlying bytes must not be cloned in upper layers.
        record_buf: Arc<[u8]>,
        space_left: usize,
        to_copy_buffer_ptr: *const u8,
        to_copy_buffer_len: usize,
        pointer: *mut u8,
        pointer_to_next: *mut u8,
    },
}

/// Fill in the cell payload with the record.
/// If the record is too large to fit in the cell, it will spill onto overflow pages.
/// This function needs a separate [FillCellPayloadState] because allocating overflow pages
/// may require I/O.
#[allow(clippy::too_many_arguments)]
fn fill_cell_payload(
    page_contents: &PageContent,
    int_key: Option<i64>,
    cell_payload: &mut Vec<u8>,
    cell_idx: usize,
    record: &ImmutableRecord,
    usable_space: usize,
    pager: Rc<Pager>,
    state: &mut FillCellPayloadState,
) -> Result<IOResult<()>> {
    loop {
        match state {
            FillCellPayloadState::Start => {
                // TODO: make record raw from start, having to serialize is not good
                let record_buf: Arc<[u8]> = Arc::from(record.get_payload());

                let page_type = page_contents.page_type();
                // fill in header
                if matches!(page_type, PageType::IndexInterior) {
                    // if a write happened on an index interior page, it is always an overwrite.
                    // we must copy the left child pointer of the replaced cell to the new cell.
                    let left_child_page =
                        page_contents.cell_interior_read_left_child_page(cell_idx);
                    cell_payload.extend_from_slice(&left_child_page.to_be_bytes());
                }
                if matches!(page_type, PageType::TableLeaf) {
                    let int_key = int_key.unwrap();
                    write_varint_to_vec(record_buf.len() as u64, cell_payload);
                    write_varint_to_vec(int_key as u64, cell_payload);
                } else {
                    write_varint_to_vec(record_buf.len() as u64, cell_payload);
                }

                let payload_overflow_threshold_max =
                    payload_overflow_threshold_max(page_type, usable_space);
                tracing::debug!(
                    "fill_cell_payload(record_size={}, payload_overflow_threshold_max={})",
                    record_buf.len(),
                    payload_overflow_threshold_max
                );
                if record_buf.len() <= payload_overflow_threshold_max {
                    // enough allowed space to fit inside a btree page
                    cell_payload.extend_from_slice(record_buf.as_ref());
                    return Ok(IOResult::Done(()));
                }

                let payload_overflow_threshold_min =
                    payload_overflow_threshold_min(page_type, usable_space);
                // see e.g. https://github.com/sqlite/sqlite/blob/9591d3fe93936533c8c3b0dc4d025ac999539e11/src/dbstat.c#L371
                let mut space_left = payload_overflow_threshold_min
                    + (record_buf.len() - payload_overflow_threshold_min) % (usable_space - 4);

                if space_left > payload_overflow_threshold_max {
                    space_left = payload_overflow_threshold_min;
                }

                // cell_size must be equal to first value of space_left as this will be the bytes copied to non-overflow page.
                let cell_size = space_left + cell_payload.len() + 4; // 4 is the number of bytes of pointer to first overflow page
                let to_copy_buffer = record_buf.as_ref();

                let prev_size = cell_payload.len();
                cell_payload.resize(prev_size + space_left + 4, 0);
                assert_eq!(
                    cell_size,
                    cell_payload.len(),
                    "cell_size={} != cell_payload.len()={}",
                    cell_size,
                    cell_payload.len()
                );

                // SAFETY: this pointer is valid because it points to a buffer in an Arc<Mutex<Vec<u8>>> that lives at least as long as this function,
                // and the Vec will not be mutated in FillCellPayloadState::AllocateOverflowPages, which we will move to next.
                let pointer = unsafe { cell_payload.as_mut_ptr().add(prev_size) };
                let pointer_to_next =
                    unsafe { cell_payload.as_mut_ptr().add(prev_size + space_left) };

                let to_copy_buffer_ptr = to_copy_buffer.as_ptr();
                let to_copy_buffer_len = to_copy_buffer.len();

                *state = FillCellPayloadState::AllocateOverflowPages {
                    record_buf,
                    space_left,
                    to_copy_buffer_ptr,
                    to_copy_buffer_len,
                    pointer,
                    pointer_to_next,
                };
                continue;
            }
            FillCellPayloadState::AllocateOverflowPages {
                record_buf: _record_buf,
                space_left,
                to_copy_buffer_ptr,
                to_copy_buffer_len,
                pointer,
                pointer_to_next,
            } => {
                let to_copy;
                {
                    let to_copy_buffer_ptr = *to_copy_buffer_ptr;
                    let to_copy_buffer_len = *to_copy_buffer_len;
                    let pointer = *pointer;
                    let space_left = *space_left;

                    // SAFETY: we know to_copy_buffer_ptr is valid because it refers to record_buf which lives at least as long as this function,
                    // and the underlying bytes are not mutated in FillCellPayloadState::AllocateOverflowPages.
                    let to_copy_buffer = unsafe {
                        std::slice::from_raw_parts(to_copy_buffer_ptr, to_copy_buffer_len)
                    };
                    to_copy = space_left.min(to_copy_buffer_len);
                    // SAFETY: we know 'pointer' is valid because it refers to cell_payload which lives at least as long as this function,
                    // and the underlying bytes are not mutated in FillCellPayloadState::AllocateOverflowPages.
                    unsafe { std::ptr::copy(to_copy_buffer_ptr, pointer, to_copy) };

                    let left = to_copy_buffer.len() - to_copy;
                    if left == 0 {
                        break;
                    }
                }

                // we still have bytes to add, we will need to allocate new overflow page
                // FIXME: handle page cache is full
                let overflow_page = return_if_io!(pager.allocate_overflow_page());
                turso_assert!(overflow_page.is_loaded(), "overflow page is not loaded");
                {
                    let id = overflow_page.get().id as u32;
                    let contents = overflow_page.get_contents();

                    // TODO: take into account offset here?
                    let buf = contents.as_ptr();
                    let as_bytes = id.to_be_bytes();
                    // update pointer to new overflow page
                    // SAFETY: we know 'pointer_to_next' is valid because it refers to an offset in cell_payload which is less than space_left + 4,
                    // and the underlying bytes are not mutated in FillCellPayloadState::AllocateOverflowPages.
                    unsafe { std::ptr::copy(as_bytes.as_ptr(), *pointer_to_next, 4) };

                    *pointer = unsafe { buf.as_mut_ptr().add(4) };
                    *pointer_to_next = buf.as_mut_ptr();
                    *space_left = usable_space - 4;
                }

                *to_copy_buffer_len -= to_copy;
                // SAFETY: we know 'to_copy_buffer_ptr' is valid because it refers to record_buf which lives at least as long as this function,
                // and that the offset is less than its length, and the underlying bytes are not mutated in FillCellPayloadState::AllocateOverflowPages.
                *to_copy_buffer_ptr = unsafe { to_copy_buffer_ptr.add(to_copy) };
            }
        }
    }
    Ok(IOResult::Done(()))
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
pub fn payload_overflow_threshold_max(page_type: PageType, usable_space: usize) -> usize {
    match page_type {
        PageType::IndexInterior | PageType::IndexLeaf => {
            ((usable_space - 12) * 64 / 255) - 23 // Index page formula
        }
        PageType::TableInterior | PageType::TableLeaf => {
            usable_space - 35 // Table leaf page formula
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
pub fn payload_overflow_threshold_min(_page_type: PageType, usable_space: usize) -> usize {
    // Same formula for all page types
    ((usable_space - 12) * 32 / 255) - 23
}

/// Drop a cell from a page.
/// This is done by freeing the range of bytes that the cell occupies.
fn drop_cell(page: &mut PageContent, cell_idx: usize, usable_space: u16) -> Result<()> {
    let (cell_start, cell_len) = page.cell_get_raw_region(cell_idx, usable_space as usize);
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
    use sorted_vec::SortedVec;
    use test_log::test;
    use turso_sqlite3_parser::ast::SortOrder;

    use super::*;
    use crate::{
        io::{Buffer, MemoryIO, OpenFlags, IO},
        schema::IndexColumn,
        storage::{
            database::DatabaseFile,
            page_cache::DumbLruPageCache,
            pager::{AtomicDbState, DbState},
            sqlite3_ondisk::PageSize,
        },
        types::Text,
        vdbe::Register,
        BufferPool, Completion, Connection, StepResult, WalFile, WalFileShared,
    };
    use std::{
        cell::RefCell,
        collections::HashSet,
        mem::transmute,
        ops::Deref,
        rc::Rc,
        sync::{Arc, Mutex},
    };

    use tempfile::TempDir;

    use crate::{
        io::BufferData,
        storage::{
            btree::{compute_free_space, fill_cell_payload, payload_overflow_threshold_max},
            sqlite3_ondisk::{BTreeCell, PageContent, PageType},
        },
        types::Value,
        Database, Page, Pager, PlatformIO,
    };

    use super::{btree_init_page, defragment_page, drop_cell, insert_into_cell};

    #[allow(clippy::arc_with_non_send_sync)]
    fn get_page(id: usize) -> BTreePage {
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
        let page = Arc::new(BTreePageInner {
            page: RefCell::new(page),
        });

        btree_init_page(&page, PageType::TableLeaf, 0, 4096);
        page
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn get_database() -> Arc<Database> {
        let mut path = TempDir::new().unwrap().keep();
        path.push("test.db");
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .pragma_update(None, "journal_mode", "wal")
                .unwrap();
        }
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io.clone(), path.to_str().unwrap(), false, false).unwrap();

        db
    }

    fn ensure_cell(page: &mut PageContent, cell_idx: usize, payload: &Vec<u8>) {
        let cell = page.cell_get_raw_region(cell_idx, 4096);
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
        conn: &Arc<Connection>,
    ) -> Vec<u8> {
        let mut payload: Vec<u8> = Vec::new();
        let mut fill_cell_payload_state = FillCellPayloadState::Start;
        run_until_done(
            || {
                fill_cell_payload(
                    page,
                    Some(id as i64),
                    &mut payload,
                    pos,
                    &record,
                    4096,
                    conn.pager.borrow().clone(),
                    &mut fill_cell_payload_state,
                )
            },
            &conn.pager.borrow().clone(),
        )
        .unwrap();
        insert_into_cell(page, &payload, pos, 4096).unwrap();
        payload
    }

    #[test]
    fn test_insert_cell() {
        let db = get_database();
        let conn = db.connect().unwrap();
        let page = get_page(2);
        let page = page.get();
        let page = page.get_contents();
        let header_size = 8;
        let regs = &[Register::Value(Value::Integer(1))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
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
        let page = page.get();
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        for i in 0..3 {
            let regs = &[Register::Value(Value::Integer(i as i64))];
            let record = ImmutableRecord::from_registers(regs, regs.len());
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
        let num_columns = 5;
        let cursor = BTreeCursor::new_table(None, pager.clone(), page_idx, num_columns);
        let (page, _c) = cursor.read_page(page_idx).unwrap();
        while page.get().is_locked() {
            pager.io.run_once().unwrap();
        }
        let page = page.get();
        // Pin page in order to not drop it in between
        page.set_dirty();
        let contents = page.get().contents.as_ref().unwrap();
        let mut previous_key = None;
        let mut valid = true;
        let mut depth = None;
        debug_validate_cells!(contents, pager.usable_space() as u16);
        let mut child_pages = Vec::new();
        for cell_idx in 0..contents.cell_count() {
            let cell = contents.cell_get(cell_idx, cursor.usable_space()).unwrap();
            let current_depth = match cell {
                BTreeCell::TableLeafCell(..) => 1,
                BTreeCell::TableInteriorCell(TableInteriorCell {
                    left_child_page, ..
                }) => {
                    let (child_page, _c) = cursor.read_page(left_child_page as usize).unwrap();
                    while child_page.get().is_locked() {
                        pager.io.run_once().unwrap();
                    }
                    child_pages.push(child_page);
                    if left_child_page == page.get().id as u32 {
                        valid = false;
                        tracing::error!(
                            "left child page is the same as parent {}",
                            left_child_page
                        );
                        continue;
                    }
                    let (child_depth, child_valid) =
                        validate_btree(pager.clone(), left_child_page as usize);
                    valid &= child_valid;
                    child_depth
                }
                _ => panic!("unsupported btree cell: {cell:?}"),
            };
            if current_depth >= 100 {
                tracing::error!("depth is too big");
                page.clear_dirty();
                return (100, false);
            }
            depth = Some(depth.unwrap_or(current_depth + 1));
            if depth != Some(current_depth + 1) {
                tracing::error!("depth is different for child of page {}", page_idx);
                valid = false;
            }
            match cell {
                BTreeCell::TableInteriorCell(TableInteriorCell { rowid, .. })
                | BTreeCell::TableLeafCell(TableLeafCell { rowid, .. }) => {
                    if previous_key.is_some() && previous_key.unwrap() >= rowid {
                        tracing::error!(
                            "keys are in bad order: prev={:?}, current={}",
                            previous_key,
                            rowid
                        );
                        valid = false;
                    }
                    previous_key = Some(rowid);
                }
                _ => panic!("unsupported btree cell: {cell:?}"),
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
        let first_page_type = child_pages.first().map(|p| {
            if !p.get().is_loaded() {
                let (new_page, _c) = pager.read_page(p.get().get().id).unwrap();
                p.page.replace(new_page);
            }
            while p.get().is_locked() {
                pager.io.run_once().unwrap();
            }
            p.get().get_contents().page_type()
        });
        if let Some(child_type) = first_page_type {
            for page in child_pages.iter().skip(1) {
                if !page.get().is_loaded() {
                    let (new_page, _c) = pager.read_page(page.get().get().id).unwrap();
                    page.page.replace(new_page);
                }
                while page.get().is_locked() {
                    pager.io.run_once().unwrap();
                }
                if page.get().get_contents().page_type() != child_type {
                    tracing::error!("child pages have different types");
                    valid = false;
                }
            }
        }
        if contents.rightmost_pointer().is_none() && contents.cell_count() == 0 {
            valid = false;
        }
        page.clear_dirty();
        (depth.unwrap(), valid)
    }

    fn format_btree(pager: Rc<Pager>, page_idx: usize, depth: usize) -> String {
        let num_columns = 5;

        let cursor = BTreeCursor::new_table(None, pager.clone(), page_idx, num_columns);
        let (page, _c) = cursor.read_page(page_idx).unwrap();
        while page.get().is_locked() {
            pager.io.run_once().unwrap();
        }
        let page = page.get();
        // Pin page in order to not drop it in between loading of different pages. If not contents will be a dangling reference.
        page.set_dirty();
        let contents = page.get().contents.as_ref().unwrap();
        let mut current = Vec::new();
        let mut child = Vec::new();
        for cell_idx in 0..contents.cell_count() {
            let cell = contents.cell_get(cell_idx, cursor.usable_space()).unwrap();
            match cell {
                BTreeCell::TableInteriorCell(cell) => {
                    current.push(format!(
                        "node[rowid:{}, ptr(<=):{}]",
                        cell.rowid, cell.left_child_page
                    ));
                    child.push(format_btree(
                        pager.clone(),
                        cell.left_child_page as usize,
                        depth + 2,
                    ));
                }
                BTreeCell::TableLeafCell(cell) => {
                    current.push(format!(
                        "leaf[rowid:{}, len(payload):{}, overflow:{}]",
                        cell.rowid,
                        cell.payload.len(),
                        cell.first_overflow_page.is_some()
                    ));
                }
                _ => panic!("unsupported btree cell: {cell:?}"),
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
        page.clear_dirty();
        if child.is_empty() {
            current
        } else {
            current + "\n" + &child.join("\n")
        }
    }

    fn empty_btree() -> (Rc<Pager>, usize, Arc<Database>, Arc<Connection>) {
        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io.clone(), ":memory:", false, false).unwrap();
        let conn = db.connect().unwrap();
        let pager = conn.pager.borrow().clone();

        // FIXME: handle page cache is full
        let _ = run_until_done(|| pager.allocate_page1(), &pager);
        let page2 = run_until_done(|| pager.allocate_page(), &pager).unwrap();
        let page2 = Arc::new(BTreePageInner {
            page: RefCell::new(page2),
        });
        btree_init_page(&page2, PageType::TableLeaf, 0, 4096);
        (pager, page2.get().get().id, db, conn)
    }

    #[test]
    pub fn btree_test_overflow_pages_are_cleared_on_overwrite() {
        // Create a database with a table
        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;

        // Get the maximum local payload size for table leaf pages
        let max_local = payload_overflow_threshold_max(PageType::TableLeaf, 4096);
        let usable_size = 4096;

        // Create a payload that is definitely larger than the maximum local size
        // This will force the creation of overflow pages
        let large_payload_size = max_local + usable_size * 2;
        let large_payload = vec![b'X'; large_payload_size];

        // Create a record with the large payload
        let regs = &[Register::Value(Value::Blob(large_payload.clone()))];
        let large_record = ImmutableRecord::from_registers(regs, regs.len());

        // Create cursor for the table
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page, num_columns);

        let initial_pagecount = pager
            .io
            .block(|| pager.with_header(|header| header.database_size.get()))
            .unwrap();
        assert_eq!(
            initial_pagecount, 2,
            "Page count should be 2 after initial insert, was {initial_pagecount}"
        );

        // Insert the large record with rowid 1
        run_until_done(
            || {
                let key = SeekKey::TableRowId(1);
                cursor.seek(key, SeekOp::GE { eq_only: true })
            },
            pager.deref(),
        )
        .unwrap();
        let key = BTreeKey::new_table_rowid(1, Some(&large_record));
        run_until_done(|| cursor.insert(&key, true), pager.deref()).unwrap();

        // Verify that overflow pages were created by checking freelist count
        // The freelist count should be 0 initially, and after inserting a large record,
        // some pages should be allocated for overflow, but they won't be in freelist yet
        let freelist_after_insert = pager
            .io
            .block(|| pager.with_header(|header| header.freelist_pages.get()))
            .unwrap();
        assert_eq!(
            freelist_after_insert, 0,
            "Freelist count should be 0 after insert, was {freelist_after_insert}"
        );
        let pagecount_after_insert = pager
            .io
            .block(|| pager.with_header(|header| header.database_size.get()))
            .unwrap();
        const EXPECTED_OVERFLOW_PAGES: u32 = 3;
        assert_eq!(
            pagecount_after_insert,
            initial_pagecount + EXPECTED_OVERFLOW_PAGES,
            "Page count should be {} after insert, was {pagecount_after_insert}",
            initial_pagecount + EXPECTED_OVERFLOW_PAGES
        );

        // Create a smaller record to overwrite with
        let small_payload = vec![b'Y'; 100]; // Much smaller payload
        let regs = &[Register::Value(Value::Blob(small_payload.clone()))];
        let small_record = ImmutableRecord::from_registers(regs, regs.len());

        // Seek to the existing record
        run_until_done(
            || {
                let key = SeekKey::TableRowId(1);
                cursor.seek(key, SeekOp::GE { eq_only: true })
            },
            pager.deref(),
        )
        .unwrap();

        // Overwrite the record with the same rowid
        let key = BTreeKey::new_table_rowid(1, Some(&small_record));
        run_until_done(|| cursor.insert(&key, true), pager.deref()).unwrap();

        // Check that the freelist count has increased, indicating overflow pages were cleared
        let freelist_after_overwrite = pager
            .io
            .block(|| pager.with_header(|header| header.freelist_pages.get()))
            .unwrap();
        assert_eq!(freelist_after_overwrite, EXPECTED_OVERFLOW_PAGES, "Freelist count should be {EXPECTED_OVERFLOW_PAGES} after overwrite, was {freelist_after_overwrite}");

        // Verify the record was actually overwritten by reading it back
        run_until_done(
            || {
                let key = SeekKey::TableRowId(1);
                cursor.seek(key, SeekOp::GE { eq_only: true })
            },
            pager.deref(),
        )
        .unwrap();

        let record = run_until_done(|| cursor.record(), pager.deref()).unwrap();
        let record = record.unwrap();

        // The record should now contain the smaller payload
        let record_payload = record.get_payload();
        const RECORD_HEADER_SIZE: usize = 1;
        const ROWID_VARINT_SIZE: usize = 1;
        const ROWID_PAYLOAD_SIZE: usize = 0; // const int 1 doesn't take any space
        const BLOB_PAYLOAD_SIZE: usize = 1; // the size '100 bytes' can be expressed as 1 byte
        assert_eq!(
            record_payload.len(),
            RECORD_HEADER_SIZE
                + ROWID_VARINT_SIZE
                + ROWID_PAYLOAD_SIZE
                + BLOB_PAYLOAD_SIZE
                + small_payload.len(),
            "Record should now contain smaller payload after overwrite"
        );
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
            let (pager, root_page, _, _) = empty_btree();
            let num_columns = 5;

            let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page, num_columns);
            for (key, size) in sequence.iter() {
                run_until_done(
                    || {
                        let key = SeekKey::TableRowId(*key);
                        cursor.seek(key, SeekOp::GE { eq_only: true })
                    },
                    pager.deref(),
                )
                .unwrap();
                let regs = &[Register::Value(Value::Blob(vec![0; *size]))];
                let value = ImmutableRecord::from_registers(regs, regs.len());
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
                        cursor.seek(seek_key, SeekOp::GE { eq_only: true }).unwrap(),
                        IOResult::Done(SeekResult::Found)
                    ),
                    "key {key} is not found"
                );
            }
        }
    }

    fn rng_from_time_or_env() -> (ChaCha8Rng, u64) {
        let seed = std::env::var("SEED").map_or(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            |v| {
                v.parse()
                    .expect("Failed to parse SEED environment variable as u64")
            },
        );
        let rng = ChaCha8Rng::seed_from_u64(seed as u64);
        (rng, seed as u64)
    }

    fn btree_insert_fuzz_run(
        attempts: usize,
        inserts: usize,
        size: impl Fn(&mut ChaCha8Rng) -> usize,
    ) {
        const VALIDATE_INTERVAL: usize = 1000;
        let do_validate_btree = std::env::var("VALIDATE_BTREE")
            .is_ok_and(|v| v.parse().expect("validate should be bool"));
        let (mut rng, seed) = rng_from_time_or_env();
        let mut seen = HashSet::new();
        tracing::info!("super seed: {}", seed);
        let num_columns = 5;

        for _ in 0..attempts {
            let (pager, root_page, _db, conn) = empty_btree();
            let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page, num_columns);
            let mut keys = SortedVec::new();
            tracing::info!("seed: {seed}");
            for insert_id in 0..inserts {
                let do_validate = do_validate_btree || (insert_id % VALIDATE_INTERVAL == 0);
                pager.begin_read_tx().unwrap();
                run_until_done(|| pager.begin_write_tx(), &pager).unwrap();
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
                        let key = SeekKey::TableRowId(key);
                        cursor.seek(key, SeekOp::GE { eq_only: true })
                    },
                    pager.deref(),
                )
                .unwrap();
                let regs = &[Register::Value(Value::Blob(vec![0; size]))];
                let value = ImmutableRecord::from_registers(regs, regs.len());
                let btree_before = if do_validate {
                    format_btree(pager.clone(), root_page, 0)
                } else {
                    "".to_string()
                };
                run_until_done(
                    || cursor.insert(&BTreeKey::new_table_rowid(key, Some(&value)), true),
                    pager.deref(),
                )
                .unwrap();
                loop {
                    match pager.end_tx(false, false, &conn, false).unwrap() {
                        IOResult::Done(_) => break,
                        IOResult::IO => {
                            pager.io.run_once().unwrap();
                        }
                    }
                }
                pager.begin_read_tx().unwrap();
                // FIXME: add sorted vector instead, should be okay for small amounts of keys for now :P, too lazy to fix right now
                let _c = cursor.move_to_root().unwrap();
                let mut valid = true;
                if do_validate {
                    let _c = cursor.move_to_root().unwrap();
                    for key in keys.iter() {
                        tracing::trace!("seeking key: {}", key);
                        run_until_done(|| cursor.next(), pager.deref()).unwrap();
                        let cursor_rowid = run_until_done(|| cursor.rowid(), pager.deref())
                            .unwrap()
                            .unwrap();
                        if *key != cursor_rowid {
                            valid = false;
                            println!("key {key} is not found, got {cursor_rowid}");
                            break;
                        }
                    }
                }
                // let's validate btree too so that we undertsand where the btree failed
                if do_validate
                    && (!valid || matches!(validate_btree(pager.clone(), root_page), (_, false)))
                {
                    let btree_after = format_btree(pager.clone(), root_page, 0);
                    println!("btree before:\n{btree_before}");
                    println!("btree after:\n{btree_after}");
                    panic!("invalid btree");
                }
                pager.end_read_tx().unwrap();
            }
            pager.begin_read_tx().unwrap();
            tracing::info!(
                "=========== btree ===========\n{}\n\n",
                format_btree(pager.clone(), root_page, 0)
            );
            if matches!(validate_btree(pager.clone(), root_page), (_, false)) {
                panic!("invalid btree");
            }
            let _c = cursor.move_to_root().unwrap();
            for key in keys.iter() {
                tracing::trace!("seeking key: {}", key);
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
                let cursor_rowid = run_until_done(|| cursor.rowid(), pager.deref())
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    *key, cursor_rowid,
                    "key {key} is not found, got {cursor_rowid}"
                );
            }
            pager.end_read_tx().unwrap();
        }
    }

    fn btree_index_insert_fuzz_run(attempts: usize, inserts: usize) {
        use crate::storage::pager::CreateBTreeFlags;

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
            let (pager, _, _db, conn) = empty_btree();
            let index_root_page_result =
                pager.btree_create(&CreateBTreeFlags::new_index()).unwrap();
            let index_root_page = match index_root_page_result {
                crate::types::IOResult::Done(id) => id as usize,
                crate::types::IOResult::IO => {
                    panic!("btree_create returned IO in test, unexpected")
                }
            };
            let index_def = Index {
                name: "testindex".to_string(),
                columns: (0..10)
                    .map(|i| IndexColumn {
                        name: format!("test{i}"),
                        order: SortOrder::Asc,
                        collation: None,
                        pos_in_table: i,
                        default: None,
                    })
                    .collect(),
                table_name: "test".to_string(),
                root_page: index_root_page,
                unique: false,
                ephemeral: false,
                has_rowid: false,
            };
            let num_columns = index_def.columns.len();
            let mut cursor = BTreeCursor::new_index(
                None,
                pager.clone(),
                index_root_page,
                &index_def,
                num_columns,
            );
            let mut keys = SortedVec::new();
            tracing::info!("seed: {seed}");
            for i in 0..inserts {
                pager.begin_read_tx().unwrap();
                let _res = pager.begin_write_tx().unwrap();
                let key = {
                    let result;
                    loop {
                        let cols = (0..num_columns)
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
                tracing::info!("insert {}/{}: {:?}", i + 1, inserts, key);
                keys.push(key.clone());
                let regs = key
                    .iter()
                    .map(|col| Register::Value(Value::Integer(*col)))
                    .collect::<Vec<_>>();
                let value = ImmutableRecord::from_registers(&regs, regs.len());
                run_until_done(
                    || {
                        let record = ImmutableRecord::from_registers(&regs, regs.len());
                        let key = SeekKey::IndexKey(&record);
                        cursor.seek(key, SeekOp::GE { eq_only: true })
                    },
                    pager.deref(),
                )
                .unwrap();
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
                let _c = cursor.move_to_root().unwrap();
                loop {
                    match pager.end_tx(false, false, &conn, false).unwrap() {
                        IOResult::Done(_) => break,
                        IOResult::IO => {
                            pager.io.run_once().unwrap();
                        }
                    }
                }
            }

            // Check that all keys can be found by seeking
            pager.begin_read_tx().unwrap();
            let _c = cursor.move_to_root().unwrap();
            for (i, key) in keys.iter().enumerate() {
                tracing::info!("seeking key {}/{}: {:?}", i + 1, keys.len(), key);
                let exists = run_until_done(
                    || {
                        let regs = key
                            .iter()
                            .map(|col| Register::Value(Value::Integer(*col)))
                            .collect::<Vec<_>>();
                        cursor.seek(
                            SeekKey::IndexKey(&ImmutableRecord::from_registers(&regs, regs.len())),
                            SeekOp::GE { eq_only: true },
                        )
                    },
                    pager.deref(),
                )
                .unwrap();
                let mut found = matches!(exists, SeekResult::Found);
                if matches!(exists, SeekResult::TryAdvance) {
                    found = run_until_done(|| cursor.next(), pager.deref()).unwrap();
                }
                assert!(found, "key {key:?} is not found");
            }
            // Check that key count is right
            let _c = cursor.move_to_root().unwrap();
            let mut count = 0;
            while run_until_done(|| cursor.next(), pager.deref()).unwrap() {
                count += 1;
            }
            assert_eq!(
                count,
                keys.len(),
                "key count is not right, got {}, expected {}",
                count,
                keys.len()
            );
            // Check that all keys can be found in-order, by iterating the btree
            let _c = cursor.move_to_root().unwrap();
            let mut prev = None;
            for (i, key) in keys.iter().enumerate() {
                tracing::info!("iterating key {}/{}: {:?}", i + 1, keys.len(), key);
                run_until_done(|| cursor.next(), pager.deref()).unwrap();
                let record = run_until_done(|| cursor.record(), &pager).unwrap();
                let record = record.as_ref().unwrap();
                let cur = record.get_values().clone();
                if let Some(prev) = prev {
                    if prev >= cur {
                        println!("Seed: {seed}");
                    }
                    assert!(
                        prev < cur,
                        "keys are not in ascending order: {prev:?} < {cur:?}",
                    );
                }
                prev = Some(cur);
            }
            pager.end_read_tx().unwrap();
        }
    }

    fn btree_index_insert_delete_fuzz_run(
        attempts: usize,
        operations: usize,
        size: impl Fn(&mut ChaCha8Rng) -> usize,
        insert_chance: f64,
    ) {
        use crate::storage::pager::CreateBTreeFlags;

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
            let (pager, _, _db, conn) = empty_btree();
            let index_root_page_result =
                pager.btree_create(&CreateBTreeFlags::new_index()).unwrap();
            let index_root_page = match index_root_page_result {
                crate::types::IOResult::Done(id) => id as usize,
                crate::types::IOResult::IO => {
                    panic!("btree_create returned IO in test, unexpected")
                }
            };
            let index_def = Index {
                name: "testindex".to_string(),
                columns: vec![IndexColumn {
                    name: "testcol".to_string(),
                    order: SortOrder::Asc,
                    collation: None,
                    pos_in_table: 0,
                    default: None,
                }],
                table_name: "test".to_string(),
                root_page: index_root_page,
                unique: false,
                ephemeral: false,
                has_rowid: false,
            };
            let mut cursor =
                BTreeCursor::new_index(None, pager.clone(), index_root_page, &index_def, 1);

            // Track expected keys that should be present in the tree
            let mut expected_keys = Vec::new();

            tracing::info!("seed: {seed}");
            for i in 0..operations {
                let print_progress = i % 100 == 0;
                pager.begin_read_tx().unwrap();
                let _res = pager.begin_write_tx().unwrap();

                // Decide whether to insert or delete (80% chance of insert)
                let is_insert = rng.next_u64() % 100 < (insert_chance * 100.0) as u64;

                if is_insert {
                    // Generate a unique key for insertion
                    let key = {
                        let result;
                        loop {
                            let sizeof_blob = size(&mut rng);
                            let blob = (0..sizeof_blob)
                                .map(|_| (rng.next_u64() % 256) as u8)
                                .collect::<Vec<_>>();
                            if seen.contains(&blob) {
                                continue;
                            } else {
                                seen.insert(blob.clone());
                            }
                            result = blob;
                            break;
                        }
                        result
                    };

                    if print_progress {
                        tracing::info!("insert {}/{}, seed: {seed}", i + 1, operations);
                    }
                    expected_keys.push(key.clone());

                    let regs = vec![Register::Value(Value::Blob(key))];
                    let value = ImmutableRecord::from_registers(&regs, regs.len());

                    let seek_result = run_until_done(
                        || {
                            let record = ImmutableRecord::from_registers(&regs, regs.len());
                            let key = SeekKey::IndexKey(&record);
                            cursor.seek(key, SeekOp::GE { eq_only: true })
                        },
                        pager.deref(),
                    )
                    .unwrap();
                    if let SeekResult::TryAdvance = seek_result {
                        run_until_done(|| cursor.next(), pager.deref()).unwrap();
                    }
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
                } else {
                    // Delete a random existing key
                    if !expected_keys.is_empty() {
                        let delete_idx = rng.next_u64() as usize % expected_keys.len();
                        let key_to_delete = expected_keys[delete_idx].clone();

                        if print_progress {
                            tracing::info!("delete {}/{}, seed: {seed}", i + 1, operations);
                        }

                        let regs = vec![Register::Value(Value::Blob(key_to_delete.clone()))];
                        let record = ImmutableRecord::from_registers(&regs, regs.len());

                        // Seek to the key to delete
                        let seek_result = run_until_done(
                            || {
                                cursor
                                    .seek(SeekKey::IndexKey(&record), SeekOp::GE { eq_only: true })
                            },
                            pager.deref(),
                        )
                        .unwrap();
                        let mut found = matches!(seek_result, SeekResult::Found);
                        if matches!(seek_result, SeekResult::TryAdvance) {
                            found = run_until_done(|| cursor.next(), pager.deref()).unwrap();
                        }
                        assert!(found, "expected key {key_to_delete:?} is not found");

                        // Delete the key
                        run_until_done(|| cursor.delete(), pager.deref()).unwrap();

                        // Remove from expected keys
                        expected_keys.remove(delete_idx);
                    }
                }

                let _c = cursor.move_to_root().unwrap();
                loop {
                    match pager.end_tx(false, false, &conn, false).unwrap() {
                        IOResult::Done(_) => break,
                        IOResult::IO => {
                            pager.io.run_once().unwrap();
                        }
                    }
                }
            }

            // Final validation
            let mut sorted_keys = expected_keys.clone();
            sorted_keys.sort();
            validate_expected_keys(&pager, &mut cursor, &sorted_keys, seed);

            pager.end_read_tx().unwrap();
        }
    }

    fn validate_expected_keys(
        pager: &Rc<Pager>,
        cursor: &mut BTreeCursor,
        expected_keys: &[Vec<u8>],
        seed: u64,
    ) {
        // Check that all expected keys can be found by seeking
        pager.begin_read_tx().unwrap();
        let _c = cursor.move_to_root().unwrap();
        for (i, key) in expected_keys.iter().enumerate() {
            tracing::info!(
                "validating key {}/{}, seed: {seed}",
                i + 1,
                expected_keys.len()
            );
            let exists = run_until_done(
                || {
                    let regs = vec![Register::Value(Value::Blob(key.clone()))];
                    cursor.seek(
                        SeekKey::IndexKey(&ImmutableRecord::from_registers(&regs, regs.len())),
                        SeekOp::GE { eq_only: true },
                    )
                },
                pager.deref(),
            )
            .unwrap();
            let mut found = matches!(exists, SeekResult::Found);
            if matches!(exists, SeekResult::TryAdvance) {
                found = run_until_done(|| cursor.next(), pager.deref()).unwrap();
            }
            assert!(found, "expected key {key:?} is not found");
        }

        // Check key count
        let _c = cursor.move_to_root().unwrap();
        run_until_done(|| cursor.rewind(), pager.deref()).unwrap();
        if !cursor.has_record.get() {
            panic!("no keys in tree");
        }
        let mut count = 1;
        loop {
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
            if !cursor.has_record.get() {
                break;
            }
            count += 1;
        }
        assert_eq!(
            count,
            expected_keys.len(),
            "key count is not right, got {}, expected {}, seed: {seed}",
            count,
            expected_keys.len()
        );

        // Check that all keys can be found in-order, by iterating the btree
        let _c = cursor.move_to_root().unwrap();
        for (i, key) in expected_keys.iter().enumerate() {
            run_until_done(|| cursor.next(), pager.deref()).unwrap();
            tracing::info!(
                "iterating key {}/{}, cursor stack cur idx: {:?}, cursor stack depth: {:?}, seed: {seed}",
                i + 1,
                expected_keys.len(),
                cursor.stack.current_cell_index(),
                cursor.stack.current()
            );
            let record = run_until_done(|| cursor.record(), pager).unwrap();
            let record = record.as_ref().unwrap();
            let cur = record.get_values().clone();
            let cur = cur.first().unwrap();
            let RefValue::Blob(ref cur) = cur else {
                panic!("expected blob, got {cur:?}");
            };
            assert_eq!(
                cur.to_slice(),
                key,
                "key {key:?} is not found, seed: {seed}"
            );
        }
        pager.end_read_tx().unwrap();
    }

    #[test]
    pub fn test_drop_odd() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get();
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        let total_cells = 10;
        for i in 0..total_cells {
            let regs = &[Register::Value(Value::Integer(i as i64))];
            let record = ImmutableRecord::from_registers(regs, regs.len());
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
        btree_index_insert_fuzz_run(2, 1024);
    }

    #[test]
    pub fn btree_index_insert_delete_fuzz_run_test() {
        btree_index_insert_delete_fuzz_run(
            2,
            2000,
            |rng| {
                let min: u32 = 4;
                let size = min + rng.next_u32() % (1024 - min);
                size as usize
            },
            0.65,
        );
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
    pub fn fuzz_long_btree_index_insert_delete_fuzz_run() {
        btree_index_insert_delete_fuzz_run(
            2,
            10000,
            |rng| {
                let min: u32 = 4;
                let size = min + rng.next_u32() % (1024 - min);
                size as usize
            },
            0.65,
        );
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_random() {
        btree_insert_fuzz_run(2, 10_000, |rng| (rng.next_u32() % 4096) as usize);
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_small() {
        btree_insert_fuzz_run(2, 10_000, |rng| (rng.next_u32() % 128) as usize);
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_big() {
        btree_insert_fuzz_run(2, 10_000, |rng| 3 * 1024 + (rng.next_u32() % 1024) as usize);
    }

    #[test]
    #[ignore]
    pub fn fuzz_long_btree_insert_fuzz_run_overflow() {
        btree_insert_fuzz_run(2, 5_000, |rng| (rng.next_u32() % 32 * 1024) as usize);
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn setup_test_env(database_size: u32) -> Rc<Pager> {
        let page_size = 512;

        let buffer_pool = Arc::new(BufferPool::new(Some(page_size as usize)));

        // Initialize buffer pool with correctly sized buffers
        for _ in 0..10 {
            let vec = vec![0; page_size as usize]; // Initialize with correct length, not just capacity
            buffer_pool.put(Pin::new(vec));
        }

        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db_file = Arc::new(DatabaseFile::new(
            io.open_file(":memory:", OpenFlags::Create, false).unwrap(),
        ));

        let wal_file = io.open_file("test.wal", OpenFlags::Create, false).unwrap();
        let wal_shared = WalFileShared::new_shared(page_size, &io, wal_file).unwrap();
        let wal = Rc::new(RefCell::new(WalFile::new(
            io.clone(),
            wal_shared,
            buffer_pool.clone(),
        )));

        let pager = Rc::new(
            Pager::new(
                db_file,
                Some(wal),
                io,
                Arc::new(parking_lot::RwLock::new(DumbLruPageCache::new(10))),
                buffer_pool,
                Arc::new(AtomicDbState::new(DbState::Uninitialized)),
                Arc::new(Mutex::new(())),
            )
            .unwrap(),
        );

        pager.io.run_once().unwrap();

        let _ = run_until_done(|| pager.allocate_page1(), &pager);
        for _ in 0..(database_size - 1) {
            let _res = pager.allocate_page().unwrap();
        }

        pager
            .io
            .block(|| {
                pager.with_header_mut(|header| header.page_size = PageSize::new(page_size).unwrap())
            })
            .unwrap();

        pager
    }

    #[test]
    pub fn test_clear_overflow_pages() -> Result<()> {
        let pager = setup_test_env(5);
        let num_columns = 5;

        let mut cursor = BTreeCursor::new_table(None, pager.clone(), 1, num_columns);

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
                pager
                    .io
                    .block(|| pager.with_header(|header| header.page_size))?
                    .get() as usize,
                drop_fn,
            )));
            let c = Completion::new_write(|_| {});
            #[allow(clippy::arc_with_non_send_sync)]
            let _c = pager
                .db_file
                .write_page(current_page as usize, buf.clone(), c)?;
            pager.io.run_once()?;

            let (page, _c) = cursor.read_page(current_page as usize)?;
            while page.get().is_locked() {
                cursor.pager.io.run_once()?;
            }

            {
                let page = page.get();
                let contents = page.get_contents();

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
            rowid: 1,
            payload: unsafe { transmute::<&[u8], &'static [u8]>(large_payload.as_slice()) },
            first_overflow_page: Some(2), // Point to first overflow page
            payload_size: large_payload.len() as u64,
        });

        let initial_freelist_pages = pager
            .io
            .block(|| pager.with_header(|header| header.freelist_pages))?
            .get();
        // Clear overflow pages
        let clear_result = cursor.clear_overflow_pages(&leaf_cell)?;
        match clear_result {
            IOResult::Done(_) => {
                let (freelist_pages, freelist_trunk_page) = pager
                    .io
                    .block(|| {
                        pager.with_header(|header| {
                            (
                                header.freelist_pages.get(),
                                header.freelist_trunk_page.get(),
                            )
                        })
                    })
                    .unwrap();

                // Verify proper number of pages were added to freelist
                assert_eq!(
                    freelist_pages,
                    initial_freelist_pages + 3,
                    "Expected 3 pages to be added to freelist"
                );

                // If this is first trunk page
                let trunk_page_id = freelist_trunk_page;
                if trunk_page_id > 0 {
                    // Verify trunk page structure
                    let (trunk_page, _c) = cursor.read_page(trunk_page_id as usize)?;
                    if let Some(contents) = trunk_page.get().get().contents.as_ref() {
                        // Read number of leaf pages in trunk
                        let n_leaf = contents.read_u32(4);
                        assert!(n_leaf > 0, "Trunk page should have leaf entries");

                        for i in 0..n_leaf {
                            let leaf_page_id = contents.read_u32(8 + (i as usize * 4));
                            assert!(
                                (2..=4).contains(&leaf_page_id),
                                "Leaf page ID {leaf_page_id} should be in range 2-4"
                            );
                        }
                    }
                }
            }
            IOResult::IO => {
                cursor.pager.io.run_once()?;
            }
        }

        Ok(())
    }

    #[test]
    pub fn test_clear_overflow_pages_no_overflow() -> Result<()> {
        let pager = setup_test_env(5);
        let num_columns = 5;

        let mut cursor = BTreeCursor::new_table(None, pager.clone(), 1, num_columns);

        let small_payload = vec![b'A'; 10];

        // Create leaf cell with no overflow pages
        let leaf_cell = BTreeCell::TableLeafCell(TableLeafCell {
            rowid: 1,
            payload: unsafe { transmute::<&[u8], &'static [u8]>(small_payload.as_slice()) },
            first_overflow_page: None,
            payload_size: small_payload.len() as u64,
        });

        let initial_freelist_pages = pager
            .io
            .block(|| pager.with_header(|header| header.freelist_pages))?
            .get() as usize;

        // Try to clear non-existent overflow pages
        let clear_result = cursor.clear_overflow_pages(&leaf_cell)?;
        match clear_result {
            IOResult::Done(_) => {
                let (freelist_pages, freelist_trunk_page) = pager.io.block(|| {
                    pager.with_header(|header| {
                        (
                            header.freelist_pages.get(),
                            header.freelist_trunk_page.get(),
                        )
                    })
                })?;

                // Verify freelist was not modified
                assert_eq!(
                    freelist_pages as usize, initial_freelist_pages,
                    "Freelist should not change when no overflow pages exist"
                );

                // Verify trunk page wasn't created
                assert_eq!(
                    freelist_trunk_page, 0,
                    "No trunk page should be created when no overflow pages exist"
                );
            }
            IOResult::IO => {
                cursor.pager.io.run_once()?;
            }
        }

        Ok(())
    }

    #[test]
    fn test_btree_destroy() -> Result<()> {
        let initial_size = 1;
        let pager = setup_test_env(initial_size);
        let num_columns = 5;

        let mut cursor = BTreeCursor::new_table(None, pager.clone(), 2, num_columns);

        // Initialize page 2 as a root page (interior)
        let root_page = run_until_done(
            || cursor.allocate_page(PageType::TableInterior, 0),
            &cursor.pager,
        )?;

        // Allocate two leaf pages
        let page3 = run_until_done(
            || cursor.allocate_page(PageType::TableLeaf, 0),
            &cursor.pager,
        )?;
        let page4 = run_until_done(
            || cursor.allocate_page(PageType::TableLeaf, 0),
            &cursor.pager,
        )?;

        // Configure the root page to point to the two leaf pages
        {
            let root_page = root_page.get();
            let contents = root_page.get().contents.as_mut().unwrap();

            // Set rightmost pointer to page4
            contents.write_u32(offset::BTREE_RIGHTMOST_PTR, page4.get().get().id as u32);

            // Create a cell with pointer to page3
            let cell_content = vec![
                // First 4 bytes: left child pointer (page3)
                (page3.get().get().id >> 24) as u8,
                (page3.get().get().id >> 16) as u8,
                (page3.get().get().id >> 8) as u8,
                page3.get().get().id as u8,
                // Next byte: rowid as varint (simple value 100)
                100,
            ];

            // Insert the cell
            insert_into_cell(contents, &cell_content, 0, 512)?;
        }

        // Add a simple record to each leaf page
        for page in [&page3, &page4] {
            let page = page.get();
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
            pager
                .io
                .block(|| pager.with_header(|header| header.database_size))?
                .get(),
            4, // We should have pages 1-4
            "Database should have 4 pages total"
        );

        // Track freelist state before destruction
        let initial_free_pages = pager
            .io
            .block(|| pager.with_header(|header| header.freelist_pages))?
            .get();
        assert_eq!(initial_free_pages, 0, "should start with no free pages");

        run_until_done(|| cursor.btree_destroy(), pager.deref())?;

        let pages_freed = pager
            .io
            .block(|| pager.with_header(|header| header.freelist_pages))?
            .get()
            - initial_free_pages;
        assert_eq!(pages_freed, 3, "should free 3 pages (root + 2 leaves)");

        Ok(())
    }

    #[test]
    pub fn test_defragment() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get();
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        for i in 0..3 {
            let regs = &[Register::Value(Value::Integer(i as i64))];
            let record = ImmutableRecord::from_registers(regs, regs.len());
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
        let page = page.get();
        let page = page.get_contents();
        let header_size = 8;

        let mut total_size = 0;
        let mut cells = Vec::new();
        let usable_space = 4096;
        let total_cells = 10;
        for i in 0..total_cells {
            let regs = &[Register::Value(Value::Integer(i as i64))];
            let record = ImmutableRecord::from_registers(regs, regs.len());
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
        let page = page.get();
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
                    let regs = &[Register::Value(Value::Integer(i as i64))];
                    let record = ImmutableRecord::from_registers(regs, regs.len());
                    let mut payload: Vec<u8> = Vec::new();
                    let mut fill_cell_payload_state = FillCellPayloadState::Start;
                    run_until_done(
                        || {
                            fill_cell_payload(
                                page,
                                Some(i as i64),
                                &mut payload,
                                cell_idx,
                                &record,
                                4096,
                                conn.pager.borrow().clone(),
                                &mut fill_cell_payload_state,
                            )
                        },
                        &conn.pager.borrow().clone(),
                    )
                    .unwrap();
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
                    let (_, len) = page.cell_get_raw_region(cell_idx, usable_space as usize);
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
        // This test is used to demonstrate that issue at https://github.com/tursodatabase/turso/issues/1085
        // is FIXED.
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get();
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
                        let regs = &[Register::Value(Value::Integer(i))];
                        let record = ImmutableRecord::from_registers(regs, regs.len());
                        let mut payload: Vec<u8> = Vec::new();
                        let mut fill_cell_payload_state = FillCellPayloadState::Start;
                        run_until_done(
                            || {
                                fill_cell_payload(
                                    page,
                                    Some(i),
                                    &mut payload,
                                    cell_idx,
                                    &record,
                                    4096,
                                    conn.pager.borrow().clone(),
                                    &mut fill_cell_payload_state,
                                )
                            },
                            &conn.pager.borrow().clone(),
                        )
                        .unwrap();
                        if (free as usize) < payload.len() - 2 {
                            // do not try to insert overflow pages because they require balancing
                            continue;
                        }
                        insert_into_cell(page, &payload, cell_idx, 4096).unwrap();
                        assert!(page.overflow_cells.is_empty());
                        total_size += payload.len() as u16 + 2;
                        cells.push(Cell {
                            pos: i as usize,
                            payload,
                        });
                    }
                    1 => {
                        if page.cell_count() == 0 {
                            continue;
                        }
                        let cell_idx = rng.next_u64() as usize % page.cell_count();
                        let (_, len) = page.cell_get_raw_region(cell_idx, usable_space as usize);
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
        let page = page.get();
        let page = page.get_contents();
        let header_size = 8;
        let usable_space = 4096;

        let regs = &[Register::Value(Value::Integer(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let payload = add_record(0, 0, page, record, &conn);
        let free = compute_free_space(page, usable_space);
        assert_eq!(free, 4096 - payload.len() as u16 - 2 - header_size);
    }

    #[test]
    pub fn test_defragment_1() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get();
        let page = page.get_contents();
        let usable_space = 4096;

        let regs = &[Register::Value(Value::Integer(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let payload = add_record(0, 0, page, record, &conn);

        assert_eq!(page.cell_count(), 1);
        defragment_page(page, usable_space);
        assert_eq!(page.cell_count(), 1);
        let (start, len) = page.cell_get_raw_region(0, usable_space as usize);
        let buf = page.as_ptr();
        assert_eq!(&payload, &buf[start..start + len]);
    }

    #[test]
    pub fn test_insert_drop_insert() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get();
        let page = page.get_contents();
        let usable_space = 4096;

        let regs = &[
            Register::Value(Value::Integer(0)),
            Register::Value(Value::Text(Text::new("aaaaaaaa"))),
        ];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let _ = add_record(0, 0, page, record, &conn);

        assert_eq!(page.cell_count(), 1);
        drop_cell(page, 0, usable_space).unwrap();
        assert_eq!(page.cell_count(), 0);

        let regs = &[Register::Value(Value::Integer(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let payload = add_record(0, 0, page, record, &conn);
        assert_eq!(page.cell_count(), 1);

        let (start, len) = page.cell_get_raw_region(0, usable_space as usize);
        let buf = page.as_ptr();
        assert_eq!(&payload, &buf[start..start + len]);
    }

    #[test]
    pub fn test_insert_drop_insert_multiple() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get();
        let page = page.get_contents();
        let usable_space = 4096;

        let regs = &[
            Register::Value(Value::Integer(0)),
            Register::Value(Value::Text(Text::new("aaaaaaaa"))),
        ];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let _ = add_record(0, 0, page, record, &conn);

        for _ in 0..100 {
            assert_eq!(page.cell_count(), 1);
            drop_cell(page, 0, usable_space).unwrap();
            assert_eq!(page.cell_count(), 0);

            let regs = &[Register::Value(Value::Integer(0))];
            let record = ImmutableRecord::from_registers(regs, regs.len());
            let payload = add_record(0, 0, page, record, &conn);
            assert_eq!(page.cell_count(), 1);

            let (start, len) = page.cell_get_raw_region(0, usable_space as usize);
            let buf = page.as_ptr();
            assert_eq!(&payload, &buf[start..start + len]);
        }
    }

    #[test]
    pub fn test_drop_a_few_insert() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let page = page.get();
        let page = page.get_contents();
        let usable_space = 4096;

        let regs = &[Register::Value(Value::Integer(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let payload = add_record(0, 0, page, record, &conn);
        let regs = &[Register::Value(Value::Integer(1))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let _ = add_record(1, 1, page, record, &conn);
        let regs = &[Register::Value(Value::Integer(2))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
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
        let page = page.get();
        let page = page.get_contents();
        let usable_space = 4096;

        let regs = &[Register::Value(Value::Integer(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let _ = add_record(0, 0, page, record, &conn);

        let regs = &[Register::Value(Value::Integer(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let _ = add_record(0, 0, page, record, &conn);
        drop_cell(page, 0, usable_space).unwrap();

        defragment_page(page, usable_space);

        let regs = &[Register::Value(Value::Integer(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let _ = add_record(0, 1, page, record, &conn);

        drop_cell(page, 0, usable_space).unwrap();

        let regs = &[Register::Value(Value::Integer(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let _ = add_record(0, 1, page, record, &conn);
    }

    #[test]
    pub fn test_fuzz_victim_2() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let usable_space = 4096;
        let insert = |pos, page| {
            let regs = &[Register::Value(Value::Integer(0))];
            let record = ImmutableRecord::from_registers(regs, regs.len());
            let _ = add_record(0, pos, page, record, &conn);
        };
        let drop = |pos, page| {
            drop_cell(page, pos, usable_space).unwrap();
        };
        let defragment = |page| {
            defragment_page(page, usable_space);
        };
        let page = page.get();
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
            let regs = &[Register::Value(Value::Integer(0))];
            let record = ImmutableRecord::from_registers(regs, regs.len());
            let _ = add_record(0, pos, page, record, &conn);
        };
        let drop = |pos, page| {
            drop_cell(page, pos, usable_space).unwrap();
        };
        let defragment = |page| {
            defragment_page(page, usable_space);
        };
        let regs = &[Register::Value(Value::Integer(0))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let mut payload: Vec<u8> = Vec::new();
        let mut fill_cell_payload_state = FillCellPayloadState::Start;
        run_until_done(
            || {
                fill_cell_payload(
                    page.get().get_contents(),
                    Some(0),
                    &mut payload,
                    0,
                    &record,
                    4096,
                    conn.pager.borrow().clone(),
                    &mut fill_cell_payload_state,
                )
            },
            &conn.pager.borrow().clone(),
        )
        .unwrap();
        let page = page.get();
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
        let (pager, root_page, _, _) = empty_btree();
        let mut keys = Vec::new();
        let num_columns = 5;

        for i in 0..10000 {
            let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page, num_columns);
            tracing::info!("INSERT INTO t VALUES ({});", i,);
            let regs = &[Register::Value(Value::Integer(i))];
            let value = ImmutableRecord::from_registers(regs, regs.len());
            tracing::trace!("before insert {}", i);
            run_until_done(
                || {
                    let key = SeekKey::TableRowId(i);
                    cursor.seek(key, SeekOp::GE { eq_only: true })
                },
                pager.deref(),
            )
            .unwrap();
            run_until_done(
                || cursor.insert(&BTreeKey::new_table_rowid(i, Some(&value)), true),
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
            let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page, num_columns);
            let key = Value::Integer(*key);
            let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
            assert!(exists, "key not found {key}");
        }
    }

    #[test]
    pub fn test_big_payload_compute_free() {
        let db = get_database();
        let conn = db.connect().unwrap();

        let page = get_page(2);
        let usable_space = 4096;
        let regs = &[Register::Value(Value::Blob(vec![0; 3600]))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let mut payload: Vec<u8> = Vec::new();
        let mut fill_cell_payload_state = FillCellPayloadState::Start;
        run_until_done(
            || {
                fill_cell_payload(
                    page.get().get_contents(),
                    Some(0),
                    &mut payload,
                    0,
                    &record,
                    4096,
                    conn.pager.borrow().clone(),
                    &mut fill_cell_payload_state,
                )
            },
            &conn.pager.borrow().clone(),
        )
        .unwrap();
        insert_into_cell(page.get().get_contents(), &payload, 0, 4096).unwrap();
        let free = compute_free_space(page.get().get_contents(), usable_space);
        let total_size = payload.len() + 2;
        assert_eq!(
            free,
            usable_space - page.get().get_contents().header_size() as u16 - total_size as u16
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

        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;

        // Insert 10,000 records in to the BTree.
        for i in 1..=10000 {
            let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page, num_columns);
            let regs = &[Register::Value(Value::Text(Text::new("hello world")))];
            let value = ImmutableRecord::from_registers(regs, regs.len());

            run_until_done(
                || {
                    let key = SeekKey::TableRowId(i);
                    cursor.seek(key, SeekOp::GE { eq_only: true })
                },
                pager.deref(),
            )
            .unwrap();

            run_until_done(
                || cursor.insert(&BTreeKey::new_table_rowid(i, Some(&value)), true),
                pager.deref(),
            )
            .unwrap();
        }

        if let (_, false) = validate_btree(pager.clone(), root_page) {
            panic!("Invalid B-tree after insertion");
        }
        let num_columns = 5;

        // Delete records with 500 <= key <= 3500
        for i in 500..=3500 {
            let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page, num_columns);
            let seek_key = SeekKey::TableRowId(i);

            let seek_result = run_until_done(
                || cursor.seek(seek_key.clone(), SeekOp::GE { eq_only: true }),
                pager.deref(),
            )
            .unwrap();

            if matches!(seek_result, SeekResult::Found) {
                run_until_done(|| cursor.delete(), pager.deref()).unwrap();
            }
        }

        // Verify that records with key < 500 and key > 3500 still exist in the BTree.
        for i in 1..=10000 {
            if (500..=3500).contains(&i) {
                continue;
            }

            let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page, num_columns);
            let key = Value::Integer(i);
            let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
            assert!(exists, "Key {i} should exist but doesn't");
        }

        // Verify the deleted records don't exist.
        for i in 500..=3500 {
            let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page, num_columns);
            let key = Value::Integer(i);
            let exists = run_until_done(|| cursor.exists(&key), pager.deref()).unwrap();
            assert!(!exists, "Deleted key {i} still exists");
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

        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;

        for (i, huge_text) in huge_texts.iter().enumerate().take(iterations) {
            let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page, num_columns);
            tracing::info!("INSERT INTO t VALUES ({});", i,);
            let regs = &[Register::Value(Value::Text(Text {
                value: huge_text.as_bytes().to_vec(),
                subtype: crate::types::TextSubtype::Text,
            }))];
            let value = ImmutableRecord::from_registers(regs, regs.len());
            tracing::trace!("before insert {}", i);
            tracing::debug!(
                "=========== btree before ===========\n{}\n\n",
                format_btree(pager.clone(), root_page, 0)
            );
            run_until_done(
                || {
                    let key = SeekKey::TableRowId(i as i64);
                    cursor.seek(key, SeekOp::GE { eq_only: true })
                },
                pager.deref(),
            )
            .unwrap();
            run_until_done(
                || cursor.insert(&BTreeKey::new_table_rowid(i as i64, Some(&value)), true),
                pager.deref(),
            )
            .unwrap();
            tracing::debug!(
                "=========== btree after ===========\n{}\n\n",
                format_btree(pager.clone(), root_page, 0)
            );
        }
        let mut cursor = BTreeCursor::new_table(None, pager.clone(), root_page, num_columns);
        let _c = cursor.move_to_root().unwrap();
        for i in 0..iterations {
            let has_next = run_until_done(|| cursor.next(), pager.deref()).unwrap();
            if !has_next {
                panic!("expected Some(rowid) but got {:?}", cursor.has_record.get());
            };
            let rowid = run_until_done(|| cursor.rowid(), pager.deref())
                .unwrap()
                .unwrap();
            assert_eq!(rowid, i as i64, "got!=expected");
        }
    }

    #[test]
    pub fn test_read_write_payload_with_offset() {
        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;
        let mut cursor = BTreeCursor::new(None, pager.clone(), root_page, num_columns);
        let offset = 2; // blobs data starts at offset 2
        let initial_text = "hello world";
        let initial_blob = initial_text.as_bytes().to_vec();
        let regs = &[Register::Value(Value::Blob(initial_blob.clone()))];
        let value = ImmutableRecord::from_registers(regs, regs.len());

        run_until_done(
            || {
                let key = SeekKey::TableRowId(1);
                cursor.seek(key, SeekOp::GE { eq_only: true })
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
        let (pager, root_page, _, _) = empty_btree();
        let num_columns = 5;
        let mut cursor = BTreeCursor::new(None, pager.clone(), root_page, num_columns);
        let mut large_blob = vec![b'A'; 40960 - 11]; // insert large blob. 40960 = 10 page long.
        let hello_world = b"hello world";
        large_blob.extend_from_slice(hello_world);
        let regs = &[Register::Value(Value::Blob(large_blob.clone()))];
        let value = ImmutableRecord::from_registers(regs, regs.len());

        run_until_done(
            || {
                let key = SeekKey::TableRowId(1);
                cursor.seek(key, SeekOp::GE { eq_only: true })
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

    fn run_until_done<T>(action: impl FnMut() -> Result<IOResult<T>>, pager: &Pager) -> Result<T> {
        pager.io.block(action)
    }

    #[test]
    fn test_free_array() {
        let (mut rng, seed) = rng_from_time_or_env();
        tracing::info!("seed={}", seed);

        const ITERATIONS: usize = 10000;
        for _ in 0..ITERATIONS {
            let mut cell_array = CellArray {
                cell_payloads: Vec::new(),
                cell_count_per_page_cumulative: [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
            };
            let mut cells_cloned = Vec::new();
            let (pager, _, _, _) = empty_btree();
            let page_type = PageType::TableLeaf;
            let page = run_until_done(|| pager.allocate_page(), &pager).unwrap();
            let page = Arc::new(BTreePageInner {
                page: RefCell::new(page),
            });
            btree_init_page(&page, page_type, 0, pager.usable_space() as u16);
            let page = page.get();
            let mut size = (rng.next_u64() % 100) as u16;
            let mut i = 0;
            // add a bunch of cells
            while compute_free_space(page.get_contents(), pager.usable_space() as u16) >= size + 10
            {
                insert_cell(i, size, page.get_contents(), pager.clone());
                i += 1;
                size = (rng.next_u64() % 1024) as u16;
            }

            // Create cell array with references to cells inserted
            let contents = page.get_contents();
            for cell_idx in 0..contents.cell_count() {
                let buf = contents.as_ptr();
                let (start, len) = contents.cell_get_raw_region(cell_idx, pager.usable_space());
                cell_array
                    .cell_payloads
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
                size,
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
                let (start, len) = contents.cell_get_raw_region(cell_idx, pager.usable_space());
                let cell_in_page = &buf[start..start + len];
                let cell_in_array = &cells_cloned[cell_idx_cloned];
                assert_eq!(cell_in_page, cell_in_array);
                cell_idx_cloned += 1;
            }
        }
    }

    fn insert_cell(cell_idx: u64, size: u16, contents: &mut PageContent, pager: Rc<Pager>) {
        let mut payload = Vec::new();
        let regs = &[Register::Value(Value::Blob(vec![0; size as usize]))];
        let record = ImmutableRecord::from_registers(regs, regs.len());
        let mut fill_cell_payload_state = FillCellPayloadState::Start;
        run_until_done(
            || {
                fill_cell_payload(
                    contents,
                    Some(cell_idx as i64),
                    &mut payload,
                    cell_idx as usize,
                    &record,
                    pager.usable_space(),
                    pager.clone(),
                    &mut fill_cell_payload_state,
                )
            },
            &pager,
        )
        .unwrap();
        insert_into_cell(
            contents,
            &payload,
            cell_idx as usize,
            pager.usable_space() as u16,
        )
        .unwrap();
    }
}
