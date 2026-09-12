use crate::types::IOResultOr;
use branches::{mark_unlikely, unlikely};
use rustc_hash::FxHashMap as HashMap;
#[cfg(debug_assertions)]
use rustc_hash::FxHashSet as HashSet;
use smallvec::SmallVec;
use tracing::{instrument, Level};

use super::{
    pager::PageRef,
    sqlite3_ondisk::{IndexInteriorCell, OverflowCell, MINIMUM_CELL_SIZE},
};
use crate::alloc::{TursoFromIterator, TursoSliceExt, TursoVecExt};
#[cfg(any(test, injected_yields))]
use crate::mvcc::yield_hooks::{ProvidesYieldContext, YieldContext, YieldPointMarker};
use crate::mvcc::yield_points::inject_io_yield;
use crate::{
    io::CompletionGroup,
    io_yield_one,
    schema::{BTreeTable, Index},
    storage::{
        pager::{BtreePageAllocMode, Pager},
        sqlite3_ondisk::{
            payload_overflows, read_u32, read_varint, write_varint, BTreeCell, DatabaseHeader,
            PageContent, PageSize, PageType, TableInteriorCell, CELL_PTR_SIZE_BYTES,
            FREELIST_LEAF_PTR_SIZE, FREELIST_TRUNK_HEADER_SIZE,
            FREELIST_TRUNK_OFFSET_FIRST_LEAF_PTR, FREELIST_TRUNK_OFFSET_LEAF_COUNT,
            FREELIST_TRUNK_OFFSET_NEXT_TRUNK_PTR, INTERIOR_PAGE_HEADER_SIZE_BYTES,
            LEAF_PAGE_HEADER_SIZE_BYTES, LEFT_CHILD_PTR_SIZE_BYTES,
        },
        state_machines::{
            AdvanceState, CountState, EmptyTableState, MoveToRightState, MoveToState, RewindState,
            SeekEndState, SeekToLastState,
        },
    },
    translate::plan::IterationDirection,
    turso_assert,
    types::{
        find_compare, get_tie_breaker_from_seek_op, IOCompletions, IndexInfo, RecordCompare,
        SeekResult,
    },
    util::IOExt,
    vdbe::Register,
    Completion, MvStore,
};
use crate::{
    numeric::Numeric,
    return_corrupt, return_if_io,
    types::{
        compare_immutable_iter, AsValueRef, IOResult, ImmutableRecord, ImmutableRecordRef, SeekKey,
        SeekOp, Value, ValueRef,
    },
    LimboError, Result,
};
use crate::{
    turso_assert_eq, turso_assert_greater_than, turso_assert_greater_than_or_equal,
    turso_assert_less_than, turso_assert_less_than_or_equal, turso_debug_assert,
};
use std::{
    any::Any,
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    fmt::Debug,
    ops::ControlFlow,
    pin::Pin,
    sync::Arc,
};

/// Maximum number of key values to store on the stack when converting registers to ValueRefs
/// during seeking. Since we use a SmallVec it'll gracefully fall back to heap allocating beyond
/// this threshold.
const STACK_ALLOC_KEY_VALS_MAX: usize = 16;

fn write_varint_to_vec(value: u64, payload: &mut crate::alloc::Vec<u8>) -> Result<()> {
    let mut varint = [0u8; 9];
    let len = write_varint(&mut varint, value);
    crate::with_btree_allocation_site!(
        CellPayload,
        payload.try_extend(varint[..len].iter().copied())
    )?;
    Ok(())
}

fn take_vec<T>(values: &mut crate::alloc::Vec<T>) -> crate::alloc::Vec<T> {
    std::mem::replace(values, crate::alloc::vec![])
}

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

/// Validate cells in a page are in a valid state. Only in debug mode.
macro_rules! debug_validate_cells {
    ($page_contents:expr, $usable_space:expr) => {
        #[cfg(debug_assertions)]
        {
            debug_validate_cells_core($page_contents, $usable_space);
        }
    };
}

/// State machine of destroy operations
/// Keep track of traversal so that it can be resumed when IO is encountered
#[derive(Debug, Clone)]
enum DestroyState {
    Start,
    LoadPage,
    ProcessPage,
    ClearOverflowPages {
        cell: BTreeCell,
    },
    /// Transitional state used after a spill yield from one of the descent
    /// reads inside `ProcessPage` or after `ClearOverflowPages` returned
    /// `Done`. We've committed to descending into `target` (its parent's
    /// cell_idx has already been advanced or `clear_overflow_pages` has
    /// already cleared the overflow chain for the divider cell), so on
    /// re-entry we just retry the read + push + transition to `LoadPage`
    /// without re-running those prior steps.
    PendingDescent {
        target: i64,
    },
    FreePage,
}

struct DestroyInfo {
    state: DestroyState,
}

#[derive(Debug)]
enum DeleteState {
    Start,
    DeterminePostBalancingSeekKey,
    LoadPage {
        post_balancing_seek_key: Option<CursorContext>,
    },
    FindCell {
        post_balancing_seek_key: Option<CursorContext>,
    },
    ClearOverflowPages {
        cell_idx: usize,
        cell: BTreeCell,
        original_child_pointer: Option<u32>,
        post_balancing_seek_key: Option<CursorContext>,
    },
    InteriorNodeReplacement {
        page: PageRef,
        /// the btree level of the page where the cell replacement happened.
        /// if the replacement causes the page to overflow/underflow, we need to remember it and balance it
        /// after the deletion process is otherwise complete.
        btree_depth: usize,
        cell_idx: usize,
        original_child_pointer: Option<u32>,
        post_balancing_seek_key: Option<CursorContext>,
    },
    CheckNeedsBalancing {
        /// same as `InteriorNodeReplacement::btree_depth`
        btree_depth: usize,
        post_balancing_seek_key: Option<CursorContext>,
        interior_node_was_replaced: bool,
    },
    /// If an interior node was replaced, we need to move back up from the subtree to the interior cell
    /// that now has the replaced content, so that the next invocation of BTreeCursor::next() does not
    /// stop at that cell.
    /// The reason it is important to land here is that the replaced cell was smaller (LT) than the deleted cell,
    /// so we must ensure we skip over it. I.e., when BTreeCursor::next() is called, it will move past the cell
    /// that holds the replaced content.
    /// See: https://github.com/tursodatabase/turso/issues/3045
    PostInteriorNodeReplacement,
    Balancing {
        /// If provided, will also balance an ancestor page at depth `balance_ancestor_at_depth`.
        /// If not provided, balancing will stop as soon as a level is encountered where no balancing is required.
        balance_ancestor_at_depth: Option<usize>,
    },
    RestoreContextAfterBalancing,
}

#[derive(Debug)]
pub enum OverwriteCellState {
    /// Allocate a new payload for the cell.
    AllocatePayload,
    /// Fill the cell payload with the new payload.
    FillPayload {
        new_payload: crate::alloc::Vec<u8>,
        rowid: Option<i64>,
        fill_cell_payload_state: FillCellPayloadState,
    },
    /// Clear the old cell's overflow pages and add them to the freelist.
    /// Overwrite the cell with the new payload.
    ClearOverflowPagesAndOverwrite {
        new_payload: crate::alloc::Vec<u8>,
        old_offset: usize,
        old_local_size: usize,
    },
}

struct BalanceContext {
    pages_to_balance_new: [Option<PinGuard>; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
    sibling_count_new: usize,
    cell_array: CellArray,
    old_cell_count_per_page_cumulative: [u16; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
    #[cfg(debug_assertions)]
    cells_debug: crate::alloc::Vec<crate::alloc::Vec<u8>>,
}

impl std::fmt::Debug for BalanceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BalanceContext")
            .field("pages_to_balance_new", &self.pages_to_balance_new)
            .field("sibling_count_new", &self.sibling_count_new)
            .field("cell_array", &self.cell_array)
            .field(
                "old_cell_count_per_page_cumulative",
                &self.old_cell_count_per_page_cumulative,
            )
            .finish()
    }
}

#[derive(Debug, Default)]
/// State machine of a btree rebalancing operation.
enum BalanceSubState {
    #[default]
    Start,
    BalanceRoot,
    Decide,
    Quick,
    /// Choose which sibling pages to balance (max 3).
    /// Generally, the siblings involved will be the page that triggered the balancing and its left and right siblings.
    /// The exceptions are:
    /// 1. If the leftmost page triggered balancing, up to 3 leftmost pages will be balanced.
    /// 2. If the rightmost page triggered balancing, up to 3 rightmost pages will be balanced.
    NonRootPickSiblings,
    /// Perform the actual balancing. This will result in 1-5 pages depending on the number of total cells to be distributed
    /// from the source pages.
    NonRootDoBalancing,
    NonRootDoBalancingAllocate {
        i: usize,
        context: Option<BalanceContext>,
    },
    NonRootDoBalancingFinish {
        context: BalanceContext,
    },
    /// Free pages that are not used anymore after balancing.
    FreePages {
        curr_page: usize,
        sibling_count_new: usize,
    },
}

#[derive(Debug)]
struct BalanceState {
    sub_state: BalanceSubState,
    balance_info: Option<BalanceInfo>,
    /// Reusable buffers for divider cell payloads.
    /// These persist across balance operations to avoid repeated allocations.
    /// We use Vec<u8> with clear/resize instead of allocating new each time.
    reusable_divider_buffers: [crate::alloc::Vec<u8>; MAX_SIBLING_PAGES_TO_BALANCE - 1],
    /// Reusable Vec for CellArray cell_payloads to avoid per-balance allocation.
    /// Cleared before each use; grows as needed and retains capacity across operations.
    reusable_cell_payloads: crate::alloc::Vec<&'static mut [u8]>,
    /// Group for the sibling page reads issued by `NonRootPickSiblings`.
    /// It lives in `BalanceState` rather than on the stack so that when
    /// the loop yields for spill IO and is re-entered, reads from earlier
    /// iterations are still waited on before `NonRootDoBalancing` looks at
    /// page contents. Taken and built when the loop completes.
    sibling_load_group: Option<CompletionGroup>,
}

impl Default for BalanceState {
    fn default() -> Self {
        Self {
            sub_state: BalanceSubState::default(),
            balance_info: None,
            reusable_divider_buffers: std::array::from_fn(|_| crate::alloc::vec![]),
            reusable_cell_payloads: crate::alloc::vec![],
            sibling_load_group: None,
        }
    }
}

/// State machine of a write operation.
/// May involve balancing due to overflow.
#[derive(Debug)]
enum WriteState {
    Start,
    /// Overwrite an existing cell.
    /// In addition to deleting the old cell and writing a new one,
    /// we may also need to clear the old cell's overflow pages
    /// and add them to the freelist.
    Overwrite {
        page: PageRef,
        cell_idx: usize,
        // This is an Option although it's not optional; we `take` it as owned for [BTreeCursor::overwrite_cell]
        // to work around the borrow checker, and then insert it back if overwriting returns IO.
        state: Option<OverwriteCellState>,
    },
    /// Insert a new cell. This path is taken when inserting a new row.
    Insert {
        page: PageRef,
        cell_idx: usize,
        new_payload: crate::alloc::Vec<u8>,
        fill_cell_payload_state: FillCellPayloadState,
    },
    Balancing,
    Finish,
}

#[cfg(any(test, injected_yields))]
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub(crate) enum BTreeWriteYieldPoint {
    AfterInsertOverflowCellBeforeBalance,
}

#[cfg(any(test, injected_yields))]
pub(crate) const BTREE_WRITE_YIELD_FAMILY: u64 = 0x4254_5245_5752_4954;

#[cfg(any(test, injected_yields))]
impl YieldPointMarker for BTreeWriteYieldPoint {
    const POINT_COUNT: u8 = 1;

    fn ordinal(self) -> u8 {
        self as u8
    }
}

struct ReadPayloadOverflow {
    payload: crate::alloc::Vec<u8>,
    next_page: u32,
    remaining_to_read: usize,
    page: PageRef,
}

#[derive(Debug)]
pub struct PinGuard(PageRef);
impl PinGuard {
    pub fn new(p: PageRef) -> Self {
        p.pin();
        Self(p)
    }
}

// Since every Drop will unpin, every clone
// needs to add to the pin count
impl Clone for PinGuard {
    fn clone(&self) -> Self {
        self.0.pin();
        Self(self.0.clone())
    }
}

impl PinGuard {
    pub fn to_page(&self) -> PageRef {
        self.0.clone()
    }
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        self.0.try_unpin();
    }
}

impl std::ops::Deref for PinGuard {
    type Target = PageRef;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub enum BTreeKey<'a> {
    TableRowId((i64, Option<&'a ImmutableRecord>)),
    IndexKey(ImmutableRecordRef<'a>),
}

impl<'a> BTreeKey<'a> {
    /// Create a new table rowid key from a rowid and an optional immutable record.
    /// The record is optional because it may not be available when the key is created.
    pub fn new_table_rowid(rowid: i64, record: Option<&'a ImmutableRecord>) -> Self {
        BTreeKey::TableRowId((rowid, record))
    }

    /// Create a new index key from an immutable record.
    pub fn new_index_key(record: ImmutableRecordRef<'a>) -> Self {
        BTreeKey::IndexKey(record)
    }

    /// Get the record, if present. Index will always be present,
    pub fn get_record(&self) -> Option<ImmutableRecordRef<'_>> {
        match self {
            BTreeKey::TableRowId((_, record)) => record.map(|record| record.as_record_ref()),
            BTreeKey::IndexKey(record) => Some(record.reborrow()),
        }
    }

    /// Get the rowid, if present. Index will never be present.
    pub fn maybe_rowid(&self) -> Option<i64> {
        match self {
            BTreeKey::TableRowId((rowid, _)) => Some(*rowid),
            BTreeKey::IndexKey(_) => None,
        }
    }

    /// Assert that the key is an integer rowid and return it.
    fn to_rowid(&self) -> i64 {
        match self {
            BTreeKey::TableRowId((rowid, _)) => *rowid,
            BTreeKey::IndexKey(_) => {
                panic!("BTreeKey::to_rowid called on IndexKey")
            }
        }
    }
}

#[derive(Debug, Clone)]
struct BalanceInfo {
    /// Old pages being balanced. We can have maximum 3 pages being balanced at the same time.
    pages_to_balance: [Option<PinGuard>; MAX_SIBLING_PAGES_TO_BALANCE],
    /// Bookkeeping of the rightmost pointer so the offset::BTREE_RIGHTMOST_PTR can be updated.
    rightmost_pointer: *mut u8,
    /// Number of siblings being used to balance
    sibling_count: usize,
    /// First divider cell to remove that marks the first sibling
    first_divider_cell: usize,
    /// Reusable buffer for constructing new divider cells during balance.
    /// Avoids allocating a new Vec for each sibling during balance_non_root.
    reusable_divider_cell: crate::alloc::Vec<u8>,
}

// SAFETY: Need to guarantee during balancing that we do not modify the rightmost pointer on the pointee `PageContent`
// safe as long as the Balance Algorithm does not modify the pointer
unsafe impl Send for BalanceInfo {}
unsafe impl Sync for BalanceInfo {}

/// Holds the state machine for the operation that was in flight when the cursor
/// was suspended due to IO.
enum CursorState {
    None,
    /// The cursor is in a write operation.
    Write(WriteState),
    Destroy(DestroyInfo),
    Delete(DeleteState),
}

impl CursorState {
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
}

impl Debug for CursorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Delete(..) => write!(f, "Delete"),
            Self::Destroy(..) => write!(f, "Destroy"),
            Self::None => write!(f, "None"),
            Self::Write(..) => write!(f, "Write"),
        }
    }
}

#[derive(Debug, Clone)]
enum OverflowState {
    Start,
    ProcessPage {
        next_page: PageRef,
    },
    /// Transitional state used to make `OverflowState::ProcessPage`
    /// re-entry-safe across yields. Once `free_page` has returned `Done` for
    /// the current page, we move to this state before validating or reading
    /// the next page so `free_page` cannot be invoked a second time on a page
    /// that is already in the freelist.
    ReadNext {
        next: u32,
    },
    Done,
}

/// Holds a Record or RowId, so that these can be transformed into a SeekKey to restore
/// cursor position to its previous location.
#[derive(Debug)]
pub enum CursorContextKey {
    TableRowId(i64),

    /// If we are in an index tree we can then reuse this field to save
    /// our cursor information
    IndexKeyRowId(ImmutableRecordRef<'static>),
}

#[derive(Debug)]
pub struct CursorContext {
    pub key: CursorContextKey,
    pub seek_op: SeekOp,
}

impl CursorContext {
    fn seek_eq_only(key: &BTreeKey<'_>) -> Self {
        let key = match key {
            BTreeKey::TableRowId((rowid, _)) => CursorContextKey::TableRowId(*rowid),
            BTreeKey::IndexKey(record) => {
                let payload = crate::types::value_blob_from_slice(record.get_payload())
                    .expect(crate::alloc::ALLOC_ERR_MSG);
                let owned = ImmutableRecord::from_bin_record(payload);
                CursorContextKey::IndexKeyRowId(ImmutableRecordRef::from_owned_record(owned))
            }
        };
        Self {
            key,
            seek_op: SeekOp::GE { eq_only: true },
        }
    }
}

/// In the future, we may expand these general validity states
#[derive(Debug, PartialEq, Eq)]
pub enum CursorValidState {
    /// Cursor does not point to a valid entry, and Btree will never yield a record.
    Invalid,
    /// Cursor is pointing a to an existing location/cell in the Btree
    Valid,
    /// Cursor may be pointing to a non-existent location/cell. This can happen after balancing operations
    RequireSeek,
    /// Cursor requires an advance after a seek
    RequireAdvance(IterationDirection),
}

#[derive(Debug, Clone, Copy)]
pub struct InteriorPageBinarySearchState {
    min_cell_idx: isize,
    max_cell_idx: isize,
    nearest_matching_cell: Option<usize>,
    eq_seen: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct LeafPageBinarySearchState {
    min_cell_idx: isize,
    max_cell_idx: isize,
    nearest_matching_cell: Option<usize>,
    /// Indicates if we have seen an exact match during the downwards traversal of the btree.
    /// This is only needed in index seeks, in cases where we need to determine whether we call
    /// an additional next()/prev() to fetch a matching record from an interior node. We will not
    /// do that if both are true:
    /// 1. We have not seen an EQ during the traversal
    /// 2. We are looking for an exact match ([SeekOp::GE] or [SeekOp::LE] with eq_only: true)
    eq_seen: bool,
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
    target_cell_when_not_found: i32,
}

#[derive(Debug)]
/// State used for seeking
pub enum CursorSeekState {
    Start,
    MovingBetweenPages {
        eq_seen: bool,
    },
    InteriorPageBinarySearch {
        state: InteriorPageBinarySearchState,
    },
    FoundLeaf {
        eq_seen: bool,
    },
    LeafPageBinarySearch {
        state: LeafPageBinarySearchState,
    },
}

/// Outcome of [`CursorTrait::try_save_position_for_external_balance`]. Mirrors
/// the two paths SQLite's saveCursorPosition takes (btree.c:756) — succeed and
/// have the caller skip invalidation, or fall through to clearing the cached
/// page stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SavePositionResult {
    /// Position captured via [`CursorTrait::save_context`]; cursor will re-seek
    /// on next use. Caller does not need to invalidate.
    Saved,
    /// Position cannot be represented (MVCC cursor, mid-operation page stack,
    /// stale record state). Caller must fall back to `invalidate_btree_cache`.
    MustInvalidate,
}

/// The result value of advancing a cursor.
///
/// This is an optimization for [CursorTrait::next_row] and [CursorTrait::prev_row]. Combining the
/// return type into the [Result] lets the compiler use a direct return (through registers) instead
/// of a structure return (LLVM `sret`).
#[repr(u64)]
#[derive(Debug)]
#[must_use]
pub enum CursorStep {
    Row,
    Empty,
    IO(IOCompletions),
    Error(Box<LimboError>),
}

impl CursorStep {
    #[inline]
    fn at_row(has_row: bool) -> Self {
        if has_row {
            CursorStep::Row
        } else {
            CursorStep::Empty
        }
    }
}

pub trait CursorTrait: Any + Send + Sync {
    /// Move cursor to last entry.
    fn last(&mut self) -> IOResultOr<()>;
    /// Move cursor to next entry.
    fn next(&mut self) -> IOResultOr<()>;
    /// Move cursor to previous entry.
    fn prev(&mut self) -> IOResultOr<()>;
    /// The `Next` opcode in one virtual call: clears the null-row flag and,
    /// unless it was set, moves to the next entry. Returns whether the cursor
    /// points at a row afterwards. A NullRow cursor does not advance, like
    /// SQLite's OP_Next when btreeNext() sees CURSOR_INVALID.
    fn next_row(&mut self) -> CursorStep {
        let was_null_row = self.get_null_flag();
        self.set_null_flag(false);
        if was_null_row {
            return CursorStep::Empty;
        }
        match self.next() {
            Ok(IOResult::IO(io)) => CursorStep::IO(io),
            Err(err) => CursorStep::Error(err),
            Ok(IOResult::Done(())) => CursorStep::at_row(!self.is_empty()),
        }
    }
    /// The `Prev` opcode counterpart of [`CursorTrait::next_row`].
    fn prev_row(&mut self) -> CursorStep {
        let was_null_row = self.get_null_flag();
        self.set_null_flag(false);
        if was_null_row {
            return CursorStep::Empty;
        }
        match self.prev() {
            Ok(IOResult::IO(io)) => CursorStep::IO(io),
            Err(err) => CursorStep::Error(err),
            Ok(IOResult::Done(())) => CursorStep::at_row(!self.is_empty()),
        }
    }
    /// Get the rowid of the entry the cursor is poiting to if any
    fn rowid(&mut self) -> IOResultOr<Option<i64>>;

    /// Incremental blob I/O — read `len` bytes at `off` within column `column`'s value
    /// into `out`. Only table (rowid) b-tree cursors support this; the default errors.
    fn blob_read_column(
        &mut self,
        _column: usize,
        _off: usize,
        _len: usize,
        _out: &mut crate::ValueBlob,
    ) -> IOResultOr<()> {
        Err(LimboError::InternalError(
            "incremental blob I/O is only supported on table rows".to_string(),
        )
        .into())
    }
    /// Incremental blob I/O — write `data` at `off` within column `column`'s value.
    fn blob_write_column(&mut self, _column: usize, _off: usize, _data: &[u8]) -> IOResultOr<()> {
        Err(LimboError::InternalError(
            "incremental blob I/O is only supported on table rows".to_string(),
        )
        .into())
    }
    /// Byte length of column `column`'s value on the current row, validating that
    /// the value is byte-addressable (TEXT or BLOB).
    fn blob_column_len(&mut self, _column: usize) -> IOResultOr<usize> {
        Err(LimboError::InternalError(
            "incremental blob I/O is only supported on table rows".to_string(),
        )
        .into())
    }
    /// Notification, delivered during a peer's saveAllCursors pass, that the peer is
    /// about to write the row `rowid` in this cursor's b-tree (`None` when the rowid
    /// is unknown, which must be treated as "could be any row"). Lets cursors backing
    /// incremental blob handles expire when their own row is written — SQLite's
    /// invalidateIncrblobCursors — while surviving writes to other rows. Default no-op.
    fn note_external_row_write(&mut self, _rowid: Option<i64>) {}
    /// Get the record of the entry the cursor is poiting to if any
    fn record(&mut self) -> IOResultOr<Option<&ImmutableRecord>>;
    /// The serialized record of the entry the cursor points to, if any, for
    /// decoding in place. A b-tree cursor hands out the bytes of the pinned
    /// page when the cell has no overflow, so the caller must not move the
    /// cursor while it holds the slice. None for a cursor in the null-row
    /// state, like the rowid. Default: the bytes of `record`.
    fn record_payload(&mut self) -> IOResultOr<Option<&[u8]>> {
        if self.get_null_flag() {
            return Ok(IOResult::Done(None));
        }
        Ok(match self.record()? {
            IOResult::Done(record) => IOResult::Done(record.map(ImmutableRecord::get_payload)),
            IOResult::IO(io) => IOResult::IO(io),
        })
    }
    /// Move the cursor based on the key and the type of operation (op).
    fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> IOResultOr<SeekResult>;
    /// Seek using registers directly without serializing them into an ImmutableRecord first.
    /// This avoids heap allocation and serialization overhead in hot paths like index lookups.
    fn seek_unpacked(&mut self, registers: &[Register], op: SeekOp) -> IOResultOr<SeekResult>;
    /// Insert a record in the position the cursor is at.
    fn insert(&mut self, key: &BTreeKey) -> IOResultOr<()>;
    /// Delete a record in the position the cursor is at.
    fn delete(&mut self) -> IOResultOr<()>;
    fn set_null_flag(&mut self, flag: bool);
    fn get_null_flag(&self) -> bool;
    /// Check if a key exists.
    fn exists(&mut self, key: &Value) -> IOResultOr<bool>;
    fn clear_btree(&mut self) -> IOResultOr<Option<usize>>;
    fn btree_destroy(&mut self) -> IOResultOr<Option<usize>>;
    /// Count the number of entries in the b-tree
    ///
    /// Only supposed to be used in the context of a simple Count Select Statement
    fn count(&mut self) -> IOResultOr<usize>;
    fn is_empty(&self) -> bool;
    fn root_page(&self) -> i64;
    /// Move cursor at the start.
    fn rewind(&mut self) -> IOResultOr<()>;
    /// Check if cursor is poiting at a valid entry with a record.
    fn has_record(&self) -> bool;
    fn set_has_record(&mut self, has_record: bool);
    fn get_index_info(&self) -> &Arc<IndexInfo>;

    fn seek_end(&mut self) -> IOResultOr<()>;
    fn seek_to_last(&mut self) -> IOResultOr<()>;

    /// Returns true if this cursor operates in MVCC mode.
    fn is_mvcc(&self) -> bool {
        false
    }

    // --- start: BTreeCursor specific functions ----
    fn invalidate_record(&mut self);
    fn has_rowid(&self) -> bool;
    fn get_pager(&self) -> Arc<Pager>;
    fn get_skip_advance(&self) -> bool;
    /// Invalidate cached navigation state. Must be called on cursors that
    /// share a btree (e.g. OpenDup cursors) when the btree structure is
    /// modified by another cursor (e.g. clear_btree via ResetSorter).
    fn invalidate_btree_cache(&mut self) {}
    /// Opt into the pager's cursor_registry. Default no-op so non-BTreeCursor
    /// impls (MvccLazyCursor) stay out. Opt-in impls must unregister in Drop.
    fn register_with_pager(&self) {}
    /// Drops the cursor at statement reset. BTreeCursor keeps its heap
    /// allocation in the pager's pool for the next cursor; the default is a
    /// plain drop.
    fn recycle(self: Box<Self>) {}
    /// Mirror of SQLite's BTCF_Multiple flag; toggled by Pager when a bucket
    /// crosses the 1↔2 threshold.
    fn set_has_peers_for_external_writes(&self, _has_peers: bool) {}
    /// Save position so the cursor can re-seek after a peer write. Returns
    /// [`SavePositionResult::MustInvalidate`] when the position can't be
    /// represented (MVCC cursors, stale page stack); the caller falls back to
    /// invalidate_btree_cache.
    fn try_save_position_for_external_balance(&mut self) -> IOResultOr<SavePositionResult> {
        Ok(IOResult::Done(SavePositionResult::MustInvalidate))
    }
    // --- end: BTreeCursor specific functions ----
}

pub struct BTreeCursor {
    /// The pager that is used to read and write to the database file.
    pub pager: Arc<Pager>,
    /// Cached value of the usable space of a BTree page, since it is very expensive to call in a hot loop via pager.usable_space().
    /// This is OK to cache because both 'PRAGMA page_size' and '.filectrl reserve_bytes' only have an effect on:
    /// 1. an uninitialized database,
    /// 2. an initialized database when the command is immediately followed by VACUUM.
    usable_space_cached: usize,
    /// The overflow limits for the usable space above
    payload_limits: PayloadLimits,
    /// Page id of the root page used to go back up fast.
    root_page: i64,
    /// Rowid and record are stored before being consumed.
    pub has_record: bool,
    null_flag: bool,
    /// Index internal pages are consumed on the way up, so we store going upwards flag in case
    /// we just moved to a parent page and the parent page is an internal index page which requires
    /// to be consumed.
    going_upwards: bool,
    /// Information maintained across execution attempts when an operation yields due to I/O.
    state: CursorState,
    /// State machine for balancing.
    balance_state: BalanceState,
    /// Information maintained while freeing overflow pages. Maintained separately from cursor state since
    /// any method could require freeing overflow pages
    overflow_state: OverflowState,
    /// Page stack used to traverse the btree.
    /// Each cursor has a stack because each cursor traverses the btree independently.
    stack: PageStack,
    /// Reusable immutable record, used to allow better allocation strategy.
    reusable_immutable_record: Option<ImmutableRecord>,
    /// Where the payload of the cell under the cursor starts and how big it
    /// is, noted by the rowid read and by a column read of a payload without
    /// overflow, so that the next column read on the same row does not parse
    /// the cell again (SQLite keeps the same in BtCursor.info). Cleared
    /// wherever the reusable record is invalidated and before every write
    /// through the cursor, because it holds an offset into the page.
    noted_payload: NotedPayload,
    /// Information about the index key structure (sort order, collation, etc)
    pub index_info: Option<Arc<IndexInfo>>,
    /// Maintain count of the number of records in the btree. Used for the `Count` opcode
    count: usize,
    /// Stores the cursor context before rebalancing so that a seek can be done later
    context: Option<CursorContext>,
    /// Store whether the Cursor is in a valid state. Meaning if it is pointing to a valid cell index or not
    pub valid_state: CursorValidState,
    seek_state: CursorSeekState,
    /// Separate state to read a record with overflow pages. This separation from `state` is necessary as
    /// we can be in a function that relies on `state`, but also needs to process overflow pages
    read_overflow_state: Option<ReadPayloadOverflow>,
    /// State machine for [BTreeCursor::is_empty_table]
    is_empty_table_state: EmptyTableState,
    /// State machine for [BTreeCursor::move_to_rightmost] and, optionally, the id of the rightmost page in the btree.
    /// If we know the rightmost page id and are already on that page, we can skip a seek.
    move_to_right_state: (MoveToRightState, Option<usize>),
    /// State machine for [BTreeCursor::seek_to_last]
    seek_to_last_state: SeekToLastState,
    /// State machine for [BTreeCursor::rewind]
    rewind_state: RewindState,
    /// State machine for [BTreeCursor::next] and [BTreeCursor::prev]
    advance_state: AdvanceState,
    /// State machine for [BTreeCursor::count]
    count_state: CountState,
    /// State machine for [BTreeCursor::seek_end]
    seek_end_state: SeekEndState,
    /// State machine for [BTreeCursor::move_to]
    move_to_state: MoveToState,
    /// Whether the next call to [BTreeCursor::next()] should be a no-op.
    /// This is currently only used after a delete operation causes a rebalancing.
    /// Advancing is only skipped if the cursor is currently pointing to a valid record
    /// when next() is called.
    pub skip_advance: bool,
    /// Reusable buffer for cell payloads during insert/update operations.
    /// This avoids allocating a new Vec for each write operation.
    reusable_cell_payload: crate::alloc::Vec<u8>,
    /// Per-cell access cache for incremental blob I/O. Caches the leaf cell's payload
    /// layout, the overflow-page-number array (Turso's runtime reconstruction of
    /// SQLite's `aOverflow`), and the byte range of the most recently accessed column,
    /// so repeated byte accesses to the same row avoid re-parsing the cell and record
    /// header and re-walking the overflow chain — turning each access into an O(1)
    /// page lookup. Invalidated when the cursor moves to a different (page, cell). The
    /// on-disk format is unchanged; this index lives only in RAM.
    blob_cache: BlobCellCache,
    /// Rowid the incremental-blob machinery last addressed. Unlike `blob_cache` it
    /// survives position saves: it is what `note_external_row_write` compares against
    /// to decide whether a peer's write hit *this* handle's row (expire, like SQLite's
    /// invalidateIncrblobCursors) or a different row (survivable via re-seek).
    blob_pinned_rowid: Option<i64>,
    /// Latched when an external write hits the pinned row or the position becomes
    /// unrecoverable. Every subsequent blob operation fails with
    /// [`LimboError::BlobHandleExpired`]; nothing ever clears it — SQLite's expired
    /// blob handles behave the same way until closed.
    blob_expired: bool,
    /// If `Some(page_idx)`, a previous call to [`BTreeCursor::get_next_record`]
    /// or [`BTreeCursor::get_prev_record`] yielded mid-descent into `page_idx`
    /// for spill IO, AFTER the loop-top `stack.advance()` / `stack.retreat()`
    /// mutations had already been applied. On re-entry, the traversal loop
    /// short-circuits to retry the read+descend rather than re-running those
    /// mutations and corrupting the cursor's cell-index state.
    iteration_pending_descent: Option<IterationPendingDescent>,
    /// (peers, idx) snapshot for the saveAllCursors pass driven from
    /// insert/delete. Carries iteration progress across IO re-entry
    /// (index records can yield via the overflow chain walk).
    pending_peer_save: Option<(
        smallvec::SmallVec<[crate::storage::pager::RegisteredCursor; 4]>,
        usize,
    )>,
    /// Mirrors SQLite's BTCF_Multiple. Toggled by Pager::register_cursor /
    /// unregister_cursor when the bucket crosses the 1↔2 threshold; lets
    /// drive_pending_peer_save skip the registry mutex in the common case.
    has_peers: crate::sync::atomic::AtomicBool,
    /// True if this cursor went through Cursor::new_btree (and thus pushed
    /// itself into the registry). Direct BTreeCursor::new callers (tests,
    /// internal utilities) bypass that path; their Drop skips unregister.
    did_register: crate::sync::atomic::AtomicBool,
    #[cfg(any(test, injected_yields))]
    yield_injector: Option<Arc<dyn crate::mvcc::yield_points::YieldInjector>>,
    #[cfg(any(test, injected_yields))]
    yield_instance_id: u64,
}

/// See [`BTreeCursor::noted_payload`]. A payload is never empty (its header
/// takes at least one byte), so a size of 0 means nothing is noted.
#[derive(Clone, Copy)]
struct NotedPayload {
    start: u32,
    size: u32,
}

impl NotedPayload {
    const NONE: Self = Self { start: 0, size: 0 };
}

/// Records the in-flight descent for `iteration_pending_descent`. The direction
/// determines which `descend*` helper to apply once the page is read.
#[derive(Clone, Copy)]
enum IterationPendingDescent {
    Forwards(i64),
    Backwards(i64),
}

/// Cache backing the incremental-blob-I/O fast path (see [`BTreeCursor::blob_cache`]).
/// `valid`/`col_valid` gate the two tiers: the cell payload layout and the parsed byte
/// range of the last-accessed column. `overflow_pages[i]` is the i-th overflow page
/// number (`overflow_pages[0]` == the value's first overflow page); it grows lazily as
/// deeper offsets are touched. All offsets are payload-relative (0 == first payload byte)
/// except `local_off`, which is the leaf page byte offset of the local payload.
struct BlobCellCache {
    valid: bool,
    leaf_id: usize,
    cell_idx: usize,
    /// Byte offset of the local payload within the leaf page.
    local_off: usize,
    /// Number of payload bytes held locally on the leaf (the rest are in overflow).
    local_len: usize,
    /// Total payload size of the cell.
    payload_size: usize,
    /// Data bytes per overflow page (`usable - 4`).
    per: usize,
    first_overflow: Option<u32>,
    overflow_pages: crate::alloc::Vec<u32>,
    /// Most recently accessed overflow page and its index in `overflow_pages`, held
    /// for spatial locality: consecutive accesses landing on the same page reuse it
    /// instead of going back through the pager's page cache. The [`PinGuard`] type is
    /// load-bearing, not incidental: any `PageRef` kept live across a blob operation
    /// MUST be pinned, or the pager can evict it and take its buffer out from under
    /// the still-held reference (eviction does `buffer.take()` regardless of live
    /// `Arc<Page>` refs). Storing a `PinGuard` — which pins on construction and unpins
    /// on drop — makes an unpinned held page unrepresentable rather than a discipline
    /// to remember. Storing a raw page NUMBER (like `overflow_pages`) is the other
    /// safe option, because it is re-fetched through the pager on each use.
    last_ov_idx: usize,
    last_ov_page: Option<PinGuard>,
    col_valid: bool,
    col: usize,
    /// Payload-relative byte offset of column `col`'s value.
    col_body_off: usize,
    col_len: usize,
    /// Serial type of column `col`, kept so every access can re-assert the value is
    /// TEXT or BLOB (byte-addressable) without re-parsing the record header.
    col_serial: u64,
}

impl Default for BlobCellCache {
    fn default() -> Self {
        Self {
            valid: false,
            leaf_id: 0,
            cell_idx: 0,
            local_off: 0,
            local_len: 0,
            payload_size: 0,
            per: 0,
            first_overflow: None,
            overflow_pages: crate::alloc::vec![],
            last_ov_idx: 0,
            last_ov_page: None,
            col_valid: false,
            col: 0,
            col_body_off: 0,
            col_len: 0,
            col_serial: 0,
        }
    }
}

impl BlobCellCache {
    /// Drop every cached tier, including the pinned overflow page. Called whenever the
    /// cursor's position stops being trustworthy (external writes, stack invalidation)
    /// so no stale offset or page number can ever back a byte-level read or write.
    fn reset(&mut self) {
        self.valid = false;
        self.col_valid = false;
        self.first_overflow = None;
        self.overflow_pages.clear();
        self.release_pinned_overflow();
    }

    /// Pin `page` and remember it as the spatial-locality overflow page for `idx`,
    /// releasing whatever was pinned before. The [`PinGuard`] keeps the pager from
    /// evicting the page (and taking its buffer) while a blob handle still holds this
    /// reference — the same contract the cursor's page stack relies on, applied to
    /// the one overflow page kept live between blob operations.
    fn pin_overflow_page(&mut self, idx: usize, page: PageRef) {
        self.last_ov_idx = idx;
        // Assigning a fresh guard drops the previous one, unpinning the old page.
        self.last_ov_page = Some(PinGuard::new(page));
    }

    /// Forget the cached overflow page; dropping its [`PinGuard`] unpins the page.
    /// Idempotent.
    fn release_pinned_overflow(&mut self) {
        self.last_ov_page = None;
        self.last_ov_idx = 0;
    }
}

/// Upper bound on a well-formed record header: the header-size varint plus one
/// maximum-width (9 byte) serial-type varint per column, at SQLite's hard column
/// limit of 32767. Anything larger is corruption, and rejecting it also bounds the
/// allocation made when a spilled header is materialized from the overflow chain.
const MAX_RECORD_HEADER_SIZE: usize = 9 + 32767 * 9;

/// Where a run of payload bytes physically lives. A payload byte range maps to a
/// sequence of these; reading and writing are the same traversal with opposite copy
/// directions, so the (bug-prone) offset arithmetic lives here once, in
/// [`BTreeCursor::blob_span`], rather than being transcribed per operation.
enum BlobSpan {
    /// Bytes resident on the leaf page at byte offset `page_off` within it.
    Local { page_off: usize, take: usize },
    /// Bytes on overflow page number `idx` in the chain, `within` bytes past that
    /// page's 4-byte next-pointer.
    Overflow {
        idx: usize,
        within: usize,
        take: usize,
    },
}

/// Walk the serial types of a record header and locate `column`'s value. `header`
/// must be exactly the header bytes (so a varint can never stray into the body) and
/// `hpos0` the offset of the first serial type, i.e. the width of the header-size
/// varint. Returns the value's payload-relative byte offset, its byte length, and
/// its serial type, after checking the range lies inside `payload_size`.
fn blob_locate_column_in_header(
    header: &[u8],
    hpos0: usize,
    payload_size: usize,
    column: usize,
) -> Result<(usize, usize, u64)> {
    let header_size = header.len();
    let mut hpos = hpos0;
    let mut body = header_size;
    let mut col = 0usize;
    loop {
        if hpos >= header_size {
            return Err(LimboError::InternalError(format!(
                "blob column {column} out of range for row with {col} columns"
            )));
        }
        let (serial, n) = crate::storage::sqlite3_ondisk::read_varint(&header[hpos..])?;
        hpos += n;
        let size = crate::types::get_serial_type_size(serial)?;
        let end = body
            .checked_add(size)
            .ok_or_else(|| LimboError::Corrupt("record body offsets overflow usize".to_string()))?;
        if end > payload_size {
            return Err(LimboError::Corrupt(format!(
                "column {col} claims bytes {body}..{end} beyond payload size {payload_size}"
            )));
        }
        if col == column {
            return Ok((body, size, serial));
        }
        body = end;
        col += 1;
    }
}

crate::assert::assert_send!(BTreeCursor);
crate::assert::assert_sync!(BTreeCursor);

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
    pub fn new(pager: Arc<Pager>, root_page: i64, num_columns: usize) -> Self {
        Self::new_with_index_info(pager, root_page, num_columns, None)
    }

    fn new_with_index_info(
        pager: Arc<Pager>,
        root_page: i64,
        _num_columns: usize,
        index_info: Option<Arc<IndexInfo>>,
    ) -> Self {
        let valid_state = if root_page == 1 && !pager.db_initialized() {
            CursorValidState::Invalid
        } else {
            CursorValidState::Valid
        };
        let usable_space = pager.usable_space();
        Self {
            pager,
            root_page,
            usable_space_cached: usable_space,
            payload_limits: PayloadLimits::new(usable_space),
            has_record: false,
            null_flag: false,
            going_upwards: false,
            state: CursorState::None,
            balance_state: BalanceState::default(),
            overflow_state: OverflowState::Start,
            stack: PageStack {
                current_page: -1,
                node_states: [BTreeNodeState::default(); BTCURSOR_MAX_DEPTH + 1],
                stack: std::mem::ManuallyDrop::new([const { None }; BTCURSOR_MAX_DEPTH + 1]),
            },
            reusable_immutable_record: None,
            noted_payload: NotedPayload::NONE,
            index_info,
            count: 0,
            context: None,
            valid_state,
            seek_state: CursorSeekState::Start,
            read_overflow_state: None,
            is_empty_table_state: EmptyTableState::Start,
            move_to_right_state: (MoveToRightState::Start, None),
            seek_to_last_state: SeekToLastState::Start,
            rewind_state: RewindState::Start,
            advance_state: AdvanceState::Start,
            count_state: CountState::Start,
            seek_end_state: SeekEndState::Start,
            move_to_state: MoveToState::Start,
            skip_advance: false,
            reusable_cell_payload: crate::alloc::vec![],
            blob_cache: BlobCellCache::default(),
            blob_pinned_rowid: None,
            blob_expired: false,
            iteration_pending_descent: None,
            pending_peer_save: None,
            has_peers: crate::sync::atomic::AtomicBool::new(false),
            did_register: crate::sync::atomic::AtomicBool::new(false),
            #[cfg(any(test, injected_yields))]
            yield_injector: None,
            #[cfg(any(test, injected_yields))]
            yield_instance_id: 0,
        }
    }

    #[cfg(any(test, injected_yields))]
    pub(crate) fn install_yield_context(&mut self, connection: &crate::Connection) {
        self.yield_injector = connection.yield_injector();
        self.yield_instance_id = connection.next_yield_instance_id();
    }

    pub fn new_table(pager: Arc<Pager>, root_page: i64, num_columns: usize) -> Self {
        Self::new(pager, root_page, num_columns)
    }

    /// Moves the cursor to the heap, into an allocation retired by an earlier
    /// cursor on the same pager when the pool has one.
    pub fn into_boxed(self) -> Box<Self> {
        match self.pager.take_cursor_allocation() {
            Some(allocation) => Box::write(allocation, self),
            None => Box::new(self),
        }
    }

    pub fn new_without_rowid_table(
        pager: Arc<Pager>,
        root_page: i64,
        table: &BTreeTable,
        num_columns: usize,
    ) -> Self {
        let key_info = table.primary_key_columns.iter().map(|(col_name, order)| {
            let (_, column) = table
                .get_column(col_name)
                .expect("WITHOUT ROWID primary key column should exist");
            crate::types::KeyInfo {
                sort_order: *order,
                collation: column.collation_opt().unwrap_or_default(),
                nulls_order: None,
            }
        });
        let index_info = Arc::new(
            IndexInfo::new(key_info, false, table.primary_key_columns.len(), true)
                .expect(crate::alloc::ALLOC_ERR_MSG),
        );
        Self::new_with_index_info(pager, root_page, num_columns, Some(index_info))
    }

    pub fn new_index(
        pager: Arc<Pager>,
        root_page: i64,
        index: &Index,
        num_columns: usize,
    ) -> Result<Self> {
        let index_info = Arc::new(IndexInfo::new_from_index(index)?);
        Ok(Self::new_with_index_info(
            pager,
            root_page,
            num_columns,
            Some(index_info),
        ))
    }

    pub fn new_index_boxed(
        pager: Arc<Pager>,
        root_page: i64,
        index: &Index,
        num_columns: usize,
    ) -> Result<Box<Self>> {
        let index_info = Arc::new(IndexInfo::new_from_index(index)?);
        Ok(Self::new_with_index_info(pager, root_page, num_columns, Some(index_info)).into_boxed())
    }

    /// Resets the cached count state so the next `count()` call re-traverses the
    /// btree. Must be called after any mutation (insert, delete, clear) that may
    /// change the number of rows in the tree.
    fn invalidate_count_cache(&mut self) {
        self.count_state = CountState::Start;
        self.count = 0;
    }

    pub fn get_index_rowid_from_record(&self) -> Option<i64> {
        if !self.has_rowid() {
            return None;
        }
        let rowid = match self.get_immutable_record().as_ref().unwrap().last_value() {
            Some(Ok(ValueRef::Numeric(Numeric::Integer(rowid)))) => rowid,
            _ => unreachable!(
                "index where has_rowid() is true should have an integer rowid as the last value"
            ),
        };
        Some(rowid)
    }

    /// Check if the table is empty.
    /// This is done by checking if the root page has no cells.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn is_empty_table(&mut self) -> IOResultOr<bool> {
        loop {
            let state = self.is_empty_table_state.clone();
            match state {
                EmptyTableState::Start => {
                    // On spill `return_if_io!` propagates `IO` up unchanged —
                    // we have not produced a page yet, the state stays at
                    // `Start`, and re-entry resumes here with the pager's
                    // pending-read tracking returning the same PageRef.
                    let (page, c) = return_if_io!(self.pager.read_page(self.root_page));
                    self.is_empty_table_state = EmptyTableState::ReadPage { page };
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                EmptyTableState::ReadPage { page } => {
                    turso_assert!(page.is_loaded(), "page should be loaded");
                    let cell_count = page.get_contents().cell_count();
                    break Ok(IOResult::Done(cell_count == 0));
                }
            }
        }
    }

    /// Move the cursor to the previous record and return it.
    /// Used in backwards iteration.
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG, name = "prev"))]
    pub fn get_prev_record(&mut self) -> IOResultOr<()> {
        let mut inner = || {
            loop {
                // Resume hook: if a previous backwards-iteration call yielded
                // for spill IO mid-descent, the loop-top mutations
                // (cell_idx set, `stack.retreat()` for IndexInterior) have
                // already been applied. Retry the read+descend without
                // re-running them.
                if let Some(IterationPendingDescent::Backwards(target)) =
                    self.iteration_pending_descent
                {
                    let (mem_page, c) = return_if_io!(self.pager.read_page(target));
                    self.iteration_pending_descent = None;
                    self.descend_backwards(mem_page);
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                    continue;
                }
                let (old_top_idx, page_type, is_index, is_leaf, cell_count) = {
                    let page = self.stack.top_ref();
                    let contents = page.get_contents();
                    (
                        self.stack.current(),
                        contents.page_type()?,
                        page.is_index()?,
                        contents.is_leaf(),
                        contents.cell_count(),
                    )
                };

                let cell_idx = self.stack.current_cell_index();

                // If we are at the end of the page and we haven't just come back from the right child,
                // we now need to move to the rightmost child.
                if cell_idx == i32::MAX && !self.going_upwards {
                    let rightmost_pointer =
                        self.stack.top_ref().get_contents().rightmost_pointer()?;
                    if let Some(rightmost_pointer) = rightmost_pointer {
                        let past_rightmost_pointer = cell_count as i32 + 1;
                        // On `IO(spill_c)` we must NOT mutate `cell_idx` or
                        // descend; the loop's outer match on `cell_idx ==
                        // i32::MAX` would not re-fire if `set_cell_index`
                        // had moved us past it.
                        let (page, c) = return_if_io!(self.read_page(rightmost_pointer as i64));
                        self.stack.set_cell_index(past_rightmost_pointer);
                        self.descend_backwards(page);
                        if let Some(c) = c {
                            io_yield_one!(c);
                        }
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
                        return Ok::<_, Box<crate::LimboError>>(IOResult::Done(true));
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
                            self.pop_upwards();
                        } else {
                            // moved to begin of btree
                            return Ok(IOResult::Done(false));
                        }
                    }
                    // continue to next loop to get record from the new page
                    continue;
                }
                if is_leaf {
                    return Ok(IOResult::Done(true));
                }

                if is_index && self.going_upwards {
                    // If we are going upwards, we need to visit the divider cell before going back to another child page.
                    // This is because index interior cells have payloads, so unless we do this we will be skipping an entry when traversing the tree.
                    self.going_upwards = false;
                    return Ok(IOResult::Done(true));
                }

                let cell_idx = self.stack.current_cell_index() as usize;
                let left_child_page = self
                    .stack
                    .get_page_contents_at_level(old_top_idx)
                    .unwrap()
                    .cell_interior_read_left_child_page(cell_idx)?;

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

                // The loop-top mutations (cell_idx set above, optional
                // `stack.retreat()` for IndexInterior) have already been
                // applied for this step. Route a spill yield through
                // `iteration_pending_descent` so the resume hook at the top
                // of the loop replays only the read+descend on re-entry.
                match self.pager.read_page(left_child_page as i64)? {
                    IOResult::Done((mem_page, c)) => {
                        self.descend_backwards(mem_page);
                        if let Some(c) = c {
                            io_yield_one!(c);
                        }
                    }
                    IOResult::IO(IOCompletions(spill_c)) => {
                        self.iteration_pending_descent =
                            Some(IterationPendingDescent::Backwards(left_child_page as i64));
                        io_yield_one!(spill_c);
                    }
                }
            }
        };

        let has_record = return_if_io!(inner());
        self.invalidate_record();
        self.set_has_record(has_record);
        Ok(IOResult::Done(()))
    }

    /// Reads the record of a cell that has overflow pages.
    ///
    /// After this has returned `Ok(IOResult::Done)`, the result can be retrieved with `self.get_immutable_record()`.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn process_overflow_read(
        &mut self,
        payload: &'static [u8],
        start_next_page: u32,
        payload_size: u64,
    ) -> IOResultOr<()> {
        loop {
            if self.read_overflow_state.is_none() {
                let remaining_to_read =
                    payload_size
                        .checked_sub(payload.len() as u64)
                        .ok_or_else(|| {
                            LimboError::Corrupt(
                                "payload size is smaller than local payload bytes".to_string(),
                            )
                        })? as usize;
                // We must not populate `read_overflow_state` before the page
                // is actually produced — otherwise an `IO(spill_c)` yield
                // would leave `read_overflow_state` half-initialized on a
                // page that doesn't exist yet, and re-entry would skip the
                // `is_none()` branch entirely.
                let (page, c) = return_if_io!(self.read_page(start_next_page as i64));
                let payload =
                    crate::with_btree_allocation_site!(OverflowRead, payload.try_to_vec())?;
                self.read_overflow_state.replace(ReadPayloadOverflow {
                    payload,
                    next_page: start_next_page,
                    remaining_to_read,
                    page,
                });
                if let Some(c) = c {
                    io_yield_one!(c);
                }
                continue;
            }
            // Compute `next` / `to_read` and fetch the next chain page (if
            // any) BEFORE applying the loop body's non-idempotent mutations,
            // so that a spill yield from `read_page(next)` leaves
            // `read_overflow_state` untouched and is safe to re-enter.
            let (next, to_read, need_next_page) = {
                let state = self.read_overflow_state.as_ref().unwrap();
                turso_assert!(state.page.is_loaded(), "page should be loaded");
                tracing::debug!(
                    next_page = state.next_page,
                    remaining_to_read = state.remaining_to_read,
                    "reading overflow page"
                );
                // The first four bytes of each overflow page are a big-endian integer which is the page number of the next page in the chain, or zero for the final page in the chain.
                let next = state.page.get_contents().read_u32_no_offset(0);
                let to_read = state.remaining_to_read.min(self.pager.usable_space() - 4);
                (
                    next,
                    to_read,
                    state.remaining_to_read > to_read && next != 0,
                )
            };
            let new_page_and_c = if need_next_page {
                Some(return_if_io!(self.read_page(next as i64)))
            } else {
                None
            };

            let ReadPayloadOverflow {
                payload,
                remaining_to_read,
                next_page,
                page,
            } = self.read_overflow_state.as_mut().unwrap();
            let buf = page.get_contents().as_ptr();
            crate::with_btree_allocation_site!(
                OverflowRead,
                payload.try_extend(buf[4..4 + to_read].iter().copied())
            )?;
            *remaining_to_read -= to_read;

            if let Some((new_page, c)) = new_page_and_c {
                *page = new_page;
                *next_page = next;
                // Re-entrancy: the four mutations above (payload extend,
                // remaining decrement, page swap, next_page swap) together
                // advance the state to "current page consumed, positioned on
                // new_page". Yielding on `c` here is safe because re-entry
                // resumes one iteration forward — the loop top reads from
                // the new page, not the old one — so none of these mutations
                // re-fire against the page they were applied to.
                if let Some(c) = c {
                    io_yield_one!(c);
                }
                continue;
            }
            if *remaining_to_read != 0 || next != 0 {
                let chain_page = *next_page;
                let remaining = *remaining_to_read;
                self.read_overflow_state.take();
                tracing::warn!(
                    chain_page,
                    next,
                    remaining,
                    "inconsistent overflow chain observed during payload read"
                );
                return Err(LimboError::Corrupt(
                    "inconsistent overflow chain observed during payload read".to_string(),
                )
                .into());
            }
            // Take the whole state before the fallible record allocations below,
            // like the inconsistent-chain branch above: an error must not leave
            // behind resumable state whose payload was already moved out, or a
            // retry would silently complete with an empty record.
            let payload_swap = self
                .read_overflow_state
                .take()
                .expect("read_overflow_state was checked above")
                .payload;

            let mut reuse_immutable = self.get_immutable_record_or_create()?;
            reuse_immutable.as_mut().unwrap().invalidate();

            crate::with_btree_allocation_site!(
                RecordPayload,
                reuse_immutable
                    .as_mut()
                    .unwrap()
                    .start_serialization(&payload_swap)
            )?;

            break Ok(IOResult::Done(()));
        }
    }

    /// Check if any ancestor pages still have cells to iterate.
    /// If not, traversing back up to parent is of no use because we are at the end of the tree.
    fn ancestor_pages_have_more_children(&self) -> bool {
        self.stack.node_states[..self.stack.current()]
            .iter()
            .rev()
            .any(|node_state| !node_state.is_at_end())
    }

    /// Move the cursor to the next record and return it.
    /// Used in forwards iteration, which is the default.
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG, name = "next"))]
    pub fn get_next_record(&mut self) -> IOResultOr<()> {
        let mut inner = || {
            if self.stack.current_page == -1 {
                // This can happen in nested left joins. See:
                // https://github.com/tursodatabase/turso/issues/2924
                return Ok::<_, Box<crate::LimboError>>(IOResult::Done(false));
            }
            loop {
                // Resume hook: if a previous call yielded for spill IO mid-
                // descent, the loop-top mutations (stack.advance) have
                // already been applied. Retry the read+descend without
                // re-running them. If the spill is still pending the pager's
                // `pending_reads` memoization will return IO again; otherwise
                // it returns Done immediately and we descend.
                if let Some(IterationPendingDescent::Forwards(target)) =
                    self.iteration_pending_descent
                {
                    let (mem_page, c) = return_if_io!(self.pager.read_page(target));
                    self.iteration_pending_descent = None;
                    self.descend(mem_page);
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                    continue;
                }
                let mem_page = self.stack.top_ref();
                let contents = mem_page.get_contents();
                let cell_idx = self.stack.current_cell_index();
                let cell_count = contents.cell_count();
                let is_leaf = contents.is_leaf();
                if cell_idx != -1 && is_leaf && cell_idx as usize + 1 < cell_count {
                    self.stack.advance();
                    return Ok(IOResult::Done(true));
                }

                let mem_page = mem_page.clone();
                let contents = mem_page.get_contents();
                tracing::debug!(
                    id = mem_page.get().id(),
                    cell = self.stack.current_cell_index(),
                    cell_count,
                    "current_before_advance",
                );

                let is_index = mem_page.is_index()?;
                let should_skip_advance = is_index
                && self.going_upwards // we are going upwards, this means we still need to visit divider cell in an index
                && self.stack.current_cell_index() >= 0 && self.stack.current_cell_index() < cell_count as i32; // if we weren't on a
                                                                                                                // valid cell then it means we will have to move upwards again or move to right page,
                                                                                                                // anyways, we won't visit this invalid cell index
                if should_skip_advance {
                    tracing::debug!(
                        going_upwards = self.going_upwards,
                        page = mem_page.get().id(),
                        cell_idx = self.stack.current_cell_index(),
                        "skipping advance",
                    );
                    self.going_upwards = false;
                    return Ok(IOResult::Done(true));
                }

                // Important to advance only after loading the page in order to not advance > 1 times
                self.stack.advance();
                let cell_idx = self.stack.current_cell_index() as usize;
                tracing::debug!(id = mem_page.get().id(), cell = cell_idx, "current");

                if cell_idx >= cell_count {
                    let rightmost_already_traversed = cell_idx > cell_count;
                    match (contents.rightmost_pointer()?, rightmost_already_traversed) {
                        (Some(right_most_pointer), false) => {
                            // do rightmost
                            self.stack.advance();
                            // Spill yield from here would re-enter the loop
                            // top with cell_idx already advanced twice; we
                            // record the descent target in
                            // `iteration_pending_descent` so the resume hook
                            // above skips the loop-top advances on re-entry.
                            match self.pager.read_page(right_most_pointer as i64)? {
                                IOResult::Done((mem_page, c)) => {
                                    self.descend(mem_page);
                                    if let Some(c) = c {
                                        io_yield_one!(c);
                                    }
                                    continue;
                                }
                                IOResult::IO(IOCompletions(spill_c)) => {
                                    self.iteration_pending_descent =
                                        Some(IterationPendingDescent::Forwards(
                                            right_most_pointer as i64,
                                        ));
                                    io_yield_one!(spill_c);
                                }
                            }
                        }
                        _ => {
                            if self.ancestor_pages_have_more_children() {
                                tracing::trace!("moving simple upwards");
                                self.pop_upwards();
                                continue;
                            } else {
                                // If none of the ancestor pages have more children to iterate, that means we are at the end of the btree and should stop iterating.
                                return Ok(IOResult::Done(false));
                            }
                        }
                    }
                }

                turso_assert!(
                    cell_idx < cell_count,
                    "cell index out of bounds",
                    { "cell_idx": cell_idx, "cell_count": cell_count, "page_type": contents.page_type().ok(), "page_id": mem_page.get().id() }
                );

                if is_leaf {
                    return Ok(IOResult::Done(true));
                }
                if is_index && self.going_upwards {
                    // This means we just came up from a child, so now we need to visit the divider cell before going back to another child page.
                    // This is because index interior cells have payloads, so unless we do this we will be skipping an entry when traversing the tree.
                    self.going_upwards = false;
                    return Ok(IOResult::Done(true));
                }

                let left_child_page = contents.cell_interior_read_left_child_page(cell_idx)?;
                // Same re-entry handling as the rightmost branch above —
                // the loop-top `stack.advance()` has already fired for this
                // step, so we route a spill yield through
                // `iteration_pending_descent`.
                match self.pager.read_page(left_child_page as i64)? {
                    IOResult::Done((mem_page, c)) => {
                        self.descend(mem_page);
                        if let Some(c) = c {
                            io_yield_one!(c);
                        }
                    }
                    IOResult::IO(IOCompletions(spill_c)) => {
                        self.iteration_pending_descent =
                            Some(IterationPendingDescent::Forwards(left_child_page as i64));
                        io_yield_one!(spill_c);
                    }
                }
            }
        };
        let has_record = return_if_io!(inner());
        self.invalidate_record();
        self.set_has_record(has_record);
        Ok(IOResult::Done(()))
    }

    /// Move the cursor to the record that matches the seek key and seek operation.
    /// This may be used to seek to a specific record in a point query (e.g. SELECT * FROM table WHERE col = 10)
    /// or e.g. find the first record greater than the seek key in a range query (e.g. SELECT * FROM table WHERE col > 10).
    /// We don't include the rowid in the comparison and that's why the last value from the record is not included.
    fn do_seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> IOResultOr<SeekResult> {
        let ret = return_if_io!(match &key {
            SeekKey::TableRowId(rowid) => self.tablebtree_seek(*rowid, op),
            SeekKey::IndexKey(index_key) => {
                self.indexbtree_seek(index_key, op)
            }
        });
        self.valid_state = CursorValidState::Valid;
        Ok(IOResult::Done(ret))
    }

    fn do_seek_unpacked(&mut self, registers: &[Register], op: SeekOp) -> IOResultOr<SeekResult> {
        let ret = return_if_io!(self.indexbtree_seek_unpacked(registers, op));
        self.valid_state = CursorValidState::Valid;
        Ok(IOResult::Done(ret))
    }

    /// Pop the stack and mark that we are going upwards in the B-tree.
    /// This is the only place where `going_upwards` should be set to `true`.
    fn pop_upwards(&mut self) {
        self.going_upwards = true;
        self.stack.pop();
    }

    /// Descend into a child page during forward iteration.
    /// Clears the `going_upwards` flag — once we descend, we are no longer going upwards.
    fn descend(&mut self, page: PageRef) {
        self.going_upwards = false;
        self.stack.push(page);
    }

    /// Descend into a child page during backward iteration.
    /// Clears the `going_upwards` flag — once we descend, we are no longer going upwards.
    fn descend_backwards(&mut self, page: PageRef) {
        self.going_upwards = false;
        self.stack.push_backwards(page);
    }

    /// Move the cursor to the root page of the btree.
    ///
    /// Blocking shim retained for tests and any caller that doesn't have an
    /// outer state machine yet. Production state machines should prefer
    /// [`BTreeCursor::move_to_root_nonblock`] so they can yield through spill
    /// IO instead of blocking inside the pager.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn move_to_root(&mut self) -> Result<Option<Completion>> {
        let io = self.pager.io.clone();
        io.block(|| self.move_to_root_nonblock())
    }

    /// Non-blocking variant of [`BTreeCursor::move_to_root`].
    ///
    /// Re-entrancy: `seek_state`, `going_upwards`, `stack.clear()` and
    /// `stack.push(root)` are all idempotent across re-entry — clearing an
    /// already-cleared stack and pushing the same root yields the same state.
    /// The disk-read completion (if any) is returned as the `Done` payload for
    /// the caller to yield itself, matching the contract of the legacy
    /// `move_to_root`.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn move_to_root_nonblock(&mut self) -> IOResultOr<Option<Completion>> {
        self.seek_state = CursorSeekState::Start;
        self.going_upwards = false;
        tracing::trace!(root_page = self.root_page);
        // The stack still holds the pinned root page from the last descent
        // unless the cursor was invalidated, which clears the stack. Keep it
        // and drop the pages below it, like SQLite's moveToRoot, instead of
        // going through the page cache again.
        if self.stack.holds_root(self.root_page) {
            self.stack.pop_to_root();
            return Ok(IOResult::Done(None));
        }
        let (mem_page, c) = return_if_io!(self.read_page(self.root_page));
        self.stack.clear();
        self.stack.push(mem_page);
        Ok(IOResult::Done(c))
    }

    /// Move the cursor to the rightmost record in the btree.
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn move_to_rightmost(&mut self) -> IOResultOr<bool> {
        loop {
            let (move_to_right_state, rightmost_page_id) = &self.move_to_right_state;
            match *move_to_right_state {
                MoveToRightState::Start => {
                    if let Some(rightmost_page_id) = rightmost_page_id {
                        // If we know the rightmost page and are already on it, we can skip a seek.
                        // The cache is safe to trust: any modification of this btree — our own
                        // balancing, or a peer cursor's write (e.g. a trigger subprogram's, via
                        // the saveAllCursors pass) — invalidates it.
                        let current_page = self.stack.top_ref();
                        if current_page.get().id() == *rightmost_page_id {
                            let contents = current_page.get_contents();
                            let cell_count = contents.cell_count();
                            self.stack.set_cell_index(cell_count as i32 - 1);
                            return Ok(IOResult::Done(cell_count > 0));
                        }
                    }
                    let rightmost_page_id = *rightmost_page_id;
                    let c = return_if_io!(self.move_to_root_nonblock());
                    self.move_to_right_state = (MoveToRightState::ProcessPage, rightmost_page_id);
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                MoveToRightState::ProcessPage => {
                    let mem_page = self.stack.top_ref();
                    let page_idx = mem_page.get().id();
                    let contents = mem_page.get_contents();
                    if contents.is_leaf() {
                        self.move_to_right_state = (MoveToRightState::Start, Some(page_idx));
                        if contents.cell_count() > 0 {
                            self.stack.set_cell_index(contents.cell_count() as i32 - 1);
                            return Ok(IOResult::Done(true));
                        }
                        return Ok(IOResult::Done(false));
                    }

                    match contents.rightmost_pointer()? {
                        Some(right_most_pointer) => {
                            // On `IO(spill_c)` the stack is unchanged, so re-entry
                            // re-reads the same parent contents and retries the
                            // descent — the disk-read for this child is memoized
                            // in `pending_reads`, so no duplicate IO is issued.
                            let (mem_page, c) =
                                return_if_io!(self.read_page(right_most_pointer as i64));
                            self.stack.set_cell_index(contents.cell_count() as i32 + 1);
                            self.stack.push(mem_page);
                            if let Some(c) = c {
                                io_yield_one!(c);
                            }
                        }
                        None => {
                            unreachable!("interior page should have a rightmost pointer");
                        }
                    }
                }
            }
        }
    }

    /// Specialized version of move_to() for table btrees.
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn tablebtree_move_to(&mut self, rowid: i64, seek_op: SeekOp) -> IOResultOr<()> {
        loop {
            let (old_top_idx, is_leaf, cell_count) = {
                let page = self.stack.top_ref();
                let contents = page.get_contents();
                (
                    self.stack.current(),
                    contents.is_leaf(),
                    contents.cell_count(),
                )
            };

            if is_leaf {
                self.seek_state = CursorSeekState::FoundLeaf { eq_seen: false };
                return Ok(IOResult::Done(()));
            }

            if matches!(
                self.seek_state,
                CursorSeekState::Start | CursorSeekState::MovingBetweenPages { .. }
            ) {
                let eq_seen = match &self.seek_state {
                    CursorSeekState::MovingBetweenPages { eq_seen } => *eq_seen,
                    _ => false,
                };
                let min_cell_idx = 0;
                let max_cell_idx = cell_count as isize - 1;
                let nearest_matching_cell = None;

                self.seek_state = CursorSeekState::InteriorPageBinarySearch {
                    state: InteriorPageBinarySearchState {
                        min_cell_idx,
                        max_cell_idx,
                        nearest_matching_cell,
                        eq_seen,
                    },
                };
            }

            let CursorSeekState::InteriorPageBinarySearch { state } = &self.seek_state else {
                unreachable!("we must be in an interior binary search state");
            };

            let mut state = *state;

            let control =
                self.tablebtree_move_inner(rowid, seek_op, old_top_idx, cell_count, &mut state)?;
            // Persist state if inner function didn't change seek_state to something else (e.g., MovingBetweenPages)
            if matches!(
                self.seek_state,
                CursorSeekState::InteriorPageBinarySearch { .. }
            ) {
                self.seek_state = CursorSeekState::InteriorPageBinarySearch { state };
            }
            match control {
                ControlFlow::Continue(_) => {}
                ControlFlow::Break(result) => {
                    return Ok(result);
                }
            }
        }
    }

    fn tablebtree_move_inner(
        &mut self,
        rowid: i64,
        seek_op: SeekOp,
        old_top_idx: usize,
        cell_count: usize,
        state: &mut InteriorPageBinarySearchState,
    ) -> Result<ControlFlow<IOResult<()>>> {
        // The compares need no I/O, so narrow the range on this page in one
        // go. The caller persists the state once afterwards, before the child
        // page read below, which is the only step here that can yield.
        {
            let contents = self.stack.get_page_contents_at_level(old_top_idx).unwrap();
            if matches!(seek_op, SeekOp::GE { .. } | SeekOp::LE { .. }) {
                tablebtree_search_interior::<true>(contents, rowid, seek_op, state)?;
            } else {
                tablebtree_search_interior::<false>(contents, rowid, seek_op, state)?;
            }

            #[inline]
            fn tablebtree_search_interior<const INCLUSIVE: bool>(
                contents: &PageContent,
                rowid: i64,
                seek_op: SeekOp,
                state: &mut InteriorPageBinarySearchState,
            ) -> Result<()> {
                let mut min = state.min_cell_idx;
                let mut max = state.max_cell_idx;
                while min <= max {
                    let cur_cell_idx = (min + max) >> 1;
                    let cell_rowid =
                        contents.cell_table_interior_read_rowid(cur_cell_idx as usize)?;
                    let is_on_left = if INCLUSIVE {
                        cell_rowid >= rowid
                    } else {
                        match seek_op {
                            SeekOp::GT => cell_rowid > rowid,
                            SeekOp::GE { .. } | SeekOp::LE { .. } => cell_rowid >= rowid,
                            SeekOp::LT => cell_rowid + 1 >= rowid,
                        }
                    };
                    if is_on_left {
                        state.nearest_matching_cell = Some(cur_cell_idx as usize);
                        max = cur_cell_idx - 1;
                    } else {
                        min = cur_cell_idx + 1;
                    }
                }
                state.min_cell_idx = min;
                state.max_cell_idx = max;
                Ok(())
            }
        }

        if let Some(nearest_matching_cell) = state.nearest_matching_cell {
            let left_child_page = self
                .stack
                .get_page_contents_at_level(old_top_idx)
                .unwrap()
                .cell_interior_read_left_child_page(nearest_matching_cell)?;
            // On `IO(spill_c)` we keep `seek_state` at
            // `InteriorPageBinarySearch` (the caller persists `state` to
            // it after we return), so re-entry retries this same step
            // with no double-push and no cell-index drift.
            match self.read_page(left_child_page as i64)? {
                IOResult::Done((mem_page, c)) => {
                    self.stack.set_cell_index(nearest_matching_cell as i32);
                    self.stack.push(mem_page);
                    self.seek_state = CursorSeekState::MovingBetweenPages {
                        eq_seen: state.eq_seen,
                    };
                    if let Some(c) = c {
                        return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(c))));
                    }
                    return Ok(ControlFlow::Continue(()));
                }
                IOResult::IO(IOCompletions(spill_c)) => {
                    return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(spill_c))));
                }
            }
        }
        match self
            .stack
            .get_page_contents_at_level(old_top_idx)
            .unwrap()
            .rightmost_pointer()?
        {
            Some(right_most_pointer) => match self.read_page(right_most_pointer as i64)? {
                IOResult::Done((mem_page, c)) => {
                    self.stack.set_cell_index(cell_count as i32 + 1);
                    self.stack.push(mem_page);
                    self.seek_state = CursorSeekState::MovingBetweenPages {
                        eq_seen: state.eq_seen,
                    };
                    if let Some(c) = c {
                        return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(c))));
                    }
                    Ok(ControlFlow::Continue(()))
                }
                IOResult::IO(IOCompletions(spill_c)) => {
                    Ok(ControlFlow::Break(IOResult::IO(IOCompletions(spill_c))))
                }
            },
            None => {
                unreachable!("we shall not go back up! The only way is down the slope");
            }
        }
    }

    /// Specialized version of move_to() for index btrees.
    #[cfg_attr(debug_assertions, instrument(skip(self, index_key), level = Level::DEBUG))]
    fn indexbtree_move_to(
        &mut self,
        index_key: &ImmutableRecordRef<'_>,
        cmp: SeekOp,
    ) -> IOResultOr<()> {
        let key_values = index_key.get_values()?;
        let record_comparer = {
            let index_info = self
                .index_info
                .as_ref()
                .expect("indexbtree_move_to: index_info required");
            find_compare(key_values.iter().peekable(), index_info)
        };
        self.indexbtree_move_to_internal(cmp, record_comparer, &key_values)
    }

    /// Move cursor to position using registers directly, avoiding record serialization.
    /// See `seek_unpacked` for rationale.
    #[instrument(skip(self, registers), level = Level::DEBUG)]
    fn indexbtree_move_to_unpacked(
        &mut self,
        registers: &[Register],
        cmp: SeekOp,
    ) -> IOResultOr<()> {
        if matches!(
            self.seek_state,
            CursorSeekState::LeafPageBinarySearch { .. } | CursorSeekState::FoundLeaf { .. }
        ) {
            self.seek_state = CursorSeekState::Start;
        }

        if matches!(self.seek_state, CursorSeekState::Start) {
            if let Some(c) = return_if_io!(self.move_to_root_nonblock()) {
                return Ok(IOResult::IO(IOCompletions(c)));
            }
        }

        let index_info = self
            .index_info
            .as_ref()
            .expect("indexbtree_move_to_unpacked: index_info required");

        let key_values: SmallVec<[ValueRef<'_>; STACK_ALLOC_KEY_VALS_MAX]> = registers
            .iter()
            .map(|r| r.get_value().as_value_ref())
            .collect();
        let record_comparer = find_compare(key_values.iter().peekable(), index_info);
        self.indexbtree_move_to_internal(cmp, record_comparer, &key_values)
    }

    fn indexbtree_move_to_internal(
        &mut self,
        cmp: SeekOp,
        record_comparer: RecordCompare,
        key_values: &[ValueRef<'_>],
    ) -> IOResultOr<()> {
        tracing::debug!("Using record comparison strategy: {:?}", record_comparer);
        let tie_breaker = get_tie_breaker_from_seek_op(cmp);

        loop {
            let (old_top_idx, is_leaf, cell_count) = {
                let page = self.stack.top_ref();
                let contents = page.get_contents();
                (
                    self.stack.current(),
                    contents.is_leaf(),
                    contents.cell_count(),
                )
            };

            if is_leaf {
                let eq_seen = match &self.seek_state {
                    CursorSeekState::MovingBetweenPages { eq_seen } => *eq_seen,
                    _ => false,
                };
                self.seek_state = CursorSeekState::FoundLeaf { eq_seen };
                return Ok(IOResult::Done(()));
            }

            if matches!(
                self.seek_state,
                CursorSeekState::Start | CursorSeekState::MovingBetweenPages { .. }
            ) {
                let eq_seen = match &self.seek_state {
                    CursorSeekState::MovingBetweenPages { eq_seen } => *eq_seen,
                    _ => false,
                };
                let min_cell_idx = 0;
                let max_cell_idx = cell_count as isize - 1;
                let nearest_matching_cell = None;

                self.seek_state = CursorSeekState::InteriorPageBinarySearch {
                    state: InteriorPageBinarySearchState {
                        min_cell_idx,
                        max_cell_idx,
                        nearest_matching_cell,
                        eq_seen,
                    },
                };
            }

            let CursorSeekState::InteriorPageBinarySearch { state } = &self.seek_state else {
                unreachable!(
                    "we must be in an interior binary search state, got {:?}",
                    self.seek_state
                );
            };

            let mut state = *state;

            let control = self.indexbtree_move_to_inner(
                cmp,
                old_top_idx,
                cell_count,
                record_comparer,
                key_values,
                tie_breaker,
                &mut state,
            )?;
            // Persist state if inner function didn't change seek_state to something else (e.g., MovingBetweenPages)
            if matches!(
                self.seek_state,
                CursorSeekState::InteriorPageBinarySearch { .. }
            ) {
                self.seek_state = CursorSeekState::InteriorPageBinarySearch { state };
            }
            match control {
                ControlFlow::Continue(_) => {}
                ControlFlow::Break(result) => {
                    return Ok(result);
                }
            }
        }
    }

    #[expect(clippy::too_many_arguments)]
    fn indexbtree_move_to_inner(
        &mut self,
        cmp: SeekOp,
        old_top_idx: usize,
        cell_count: usize,
        record_comparer: RecordCompare,
        key_values: &[ValueRef<'_>],
        tie_breaker: Ordering,
        state: &mut InteriorPageBinarySearchState,
    ) -> Result<ControlFlow<IOResult<()>>> {
        let iter_dir = cmp.iteration_direction();
        // Compare cells until the range is exhausted: only an overflow key
        // read can yield here, and it returns before the range changes, so
        // re-entry retries the same cell. The caller persists the state once
        // per call instead of once per compare.
        let payload_limits = self.payload_limits;
        while state.min_cell_idx <= state.max_cell_idx {
            let cur_cell_idx = (state.min_cell_idx + state.max_cell_idx) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
            self.stack.set_cell_index(cur_cell_idx as i32);

            let (payload, payload_size, first_overflow_page) = self
                .stack
                .get_page_contents_at_level(old_top_idx)
                .unwrap()
                .cell_read_payload_ptr(cur_cell_idx as usize, payload_limits)?;

            let cell_payload: &[u8] = if let Some(next_page) = first_overflow_page {
                let res = self.process_overflow_read(payload, next_page, payload_size)?;
                if res.is_io() {
                    return Ok(ControlFlow::Break(res));
                }
                self.get_immutable_record()
                    .expect("the overflow read filled the reusable record")
                    .get_payload()
            } else {
                payload
            };

            let (target_leaf_page_is_in_left_subtree, is_eq) = {
                let interior_cell_vs_index_key = record_comparer.compare_payload(
                    cell_payload,
                    key_values,
                    self.index_info
                        .as_ref()
                        .expect("indexbtree_move_to: index_info required"),
                    tie_breaker,
                )?;

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
                state.eq_seen = true;
            }

            if target_leaf_page_is_in_left_subtree {
                state.nearest_matching_cell = Some(cur_cell_idx as usize);
                state.max_cell_idx = cur_cell_idx - 1;
            } else {
                state.min_cell_idx = cur_cell_idx + 1;
            }
        }

        let Some(leftmost_matching_cell) = state.nearest_matching_cell else {
            match self
                .stack
                .get_page_contents_at_level(old_top_idx)
                .unwrap()
                .rightmost_pointer()?
            {
                Some(right_most_pointer) => {
                    // On `IO(spill_c)` keep seek_state at the binary
                    // search so re-entry retries this same step. None
                    // of the cursor mutations have happened yet.
                    match self.read_page(right_most_pointer as i64)? {
                        IOResult::Done((mem_page, c)) => {
                            self.stack.set_cell_index(cell_count as i32 + 1);
                            self.stack.push(mem_page);
                            self.seek_state = CursorSeekState::MovingBetweenPages {
                                eq_seen: state.eq_seen,
                            };
                            if let Some(c) = c {
                                return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(c))));
                            }
                            return Ok(ControlFlow::Continue(()));
                        }
                        IOResult::IO(IOCompletions(spill_c)) => {
                            return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(spill_c))));
                        }
                    }
                }
                None => {
                    unreachable!("we shall not go back up! The only way is down the slope");
                }
            }
        };
        let left_child_page = self
            .stack
            .get_page_contents_at_level(old_top_idx)
            .unwrap()
            .cell_interior_read_left_child_page(leftmost_matching_cell)?;
        // We don't advance in case of forward iteration and index tree
        // internal nodes because we will visit this node going up.
        // In backwards iteration, we must retreat because otherwise we
        // would unnecessarily visit this node again. Example:
        //   parent:     key 666 (target found in left child)
        //   left child: key 663, key 664, key 665
        // we need to move to the previous parent (e.g. key 662) when
        // iterating backwards so that we don't end up back here again.
        //
        // On `IO(spill_c)` we MUST NOT mutate `cell_idx` (set or
        // retreat) — see the Done branch.
        {
            let page = self.stack.get_page_at_level(old_top_idx).unwrap();
            turso_assert!(
                page.get().id() != left_child_page as usize,
                "corrupt: current page and left child page are the same",
                { "cell": leftmost_matching_cell, "page_id": page.get().id() }
            );
        }

        match self.read_page(left_child_page as i64)? {
            IOResult::Done((mem_page, c)) => {
                self.stack.set_cell_index(leftmost_matching_cell as i32);
                if iter_dir == IterationDirection::Backwards {
                    self.stack.retreat();
                }
                self.stack.push(mem_page);
                self.seek_state = CursorSeekState::MovingBetweenPages {
                    eq_seen: state.eq_seen,
                };
                if let Some(c) = c {
                    return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(c))));
                }
            }
            IOResult::IO(IOCompletions(spill_c)) => {
                return Ok(ControlFlow::Break(IOResult::IO(IOCompletions(spill_c))));
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    /// Specialized version of do_seek() for table btrees that uses binary search instead
    /// of iterating cells in order.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn tablebtree_seek(&mut self, rowid: i64, seek_op: SeekOp) -> IOResultOr<SeekResult> {
        if matches!(
            self.seek_state,
            CursorSeekState::Start
                | CursorSeekState::MovingBetweenPages { .. }
                | CursorSeekState::InteriorPageBinarySearch { .. }
        ) {
            // No need for another move_to_root. Move_to already moves to root
            return_if_io!(self.move_to(SeekKey::TableRowId(rowid), seek_op));
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            turso_assert!(
                contents.is_leaf(),
                "tablebtree_seek() called on non-leaf page"
            );

            let cell_count = contents.cell_count();
            if cell_count == 0 {
                self.stack.set_cell_index(0);
                return Ok(IOResult::Done(SeekResult::NotFound));
            }
            let min_cell_idx = 0;
            let max_cell_idx = cell_count as isize - 1;

            // If iter dir is forwards, we want the first cell that matches;
            // If iter dir is backwards, we want the last cell that matches.
            let nearest_matching_cell = None;

            self.seek_state = CursorSeekState::LeafPageBinarySearch {
                state: LeafPageBinarySearchState {
                    min_cell_idx,
                    max_cell_idx,
                    nearest_matching_cell,
                    eq_seen: false, // not relevant for table btrees
                    target_cell_when_not_found: match seek_op.iteration_direction() {
                        IterationDirection::Forwards => cell_count as i32,
                        IterationDirection::Backwards => -1,
                    },
                },
            };
        }

        let CursorSeekState::LeafPageBinarySearch { state } = &self.seek_state else {
            unreachable!("we must be in a leaf binary search state");
        };

        let page = self.stack.top_ref().clone();
        let contents = page.get_contents();
        let mut state = *state;

        loop {
            let control = self.tablebtree_seek_inner(rowid, seek_op, contents, &mut state)?;
            // Persist state after each iteration since inner function modifies it
            if matches!(
                self.seek_state,
                CursorSeekState::LeafPageBinarySearch { .. }
            ) {
                self.seek_state = CursorSeekState::LeafPageBinarySearch { state };
            }
            match control {
                ControlFlow::Continue(_) => {}
                ControlFlow::Break(res) => {
                    return Ok(res);
                }
            }
        }
    }

    fn tablebtree_seek_inner(
        &mut self,
        rowid: i64,
        seek_op: SeekOp,
        contents: &mut PageContent,
        state: &mut LeafPageBinarySearchState,
    ) -> Result<ControlFlow<IOResult<SeekResult>>> {
        if matches!(seek_op, SeekOp::GE { eq_only: true }) {
            return tablebtree_seek_impl::<true>(self, rowid, seek_op, contents, state);
        } else {
            return tablebtree_seek_impl::<false>(self, rowid, seek_op, contents, state);
        }

        #[inline]
        fn tablebtree_seek_impl<const EXACT_FORWARD: bool>(
            cursor: &mut BTreeCursor,
            rowid: i64,
            seek_op: SeekOp,
            contents: &mut PageContent,
            state: &mut LeafPageBinarySearchState,
        ) -> Result<ControlFlow<IOResult<SeekResult>>> {
            let seek_op = if EXACT_FORWARD {
                SeekOp::GE { eq_only: true }
            } else {
                seek_op
            };
            let iter_dir = seek_op.iteration_direction();
            // The compares need no I/O, so narrow the range on this leaf in one
            // go; the caller persists the state once afterwards.
            let mut min = state.min_cell_idx;
            let mut max = state.max_cell_idx;
            while min <= max {
                let cur_cell_idx = (min + max) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
                let cell_rowid = contents.cell_table_leaf_read_rowid(cur_cell_idx as usize)?;

                let cmp = cell_rowid.cmp(&rowid);

                let found = if EXACT_FORWARD {
                    cell_rowid == rowid
                } else {
                    match seek_op {
                        SeekOp::GT => cmp.is_gt(),
                        SeekOp::GE { eq_only: true } => cmp.is_eq(),
                        SeekOp::GE { eq_only: false } => cmp.is_ge(),
                        SeekOp::LE { eq_only: true } => cmp.is_eq(),
                        SeekOp::LE { eq_only: false } => cmp.is_le(),
                        SeekOp::LT => cmp.is_lt(),
                    }
                };

                // rowids are unique, so we can return the rowid immediately
                if found && seek_op.eq_only() {
                    state.min_cell_idx = min;
                    state.max_cell_idx = max;
                    cursor.stack.set_cell_index(cur_cell_idx as i32);
                    cursor.set_has_record(true);
                    return Ok(ControlFlow::Break(IOResult::Done(SeekResult::Found)));
                }

                if found {
                    state.nearest_matching_cell = Some(cur_cell_idx as usize);
                    match iter_dir {
                        IterationDirection::Forwards => {
                            max = cur_cell_idx - 1;
                        }
                        IterationDirection::Backwards => {
                            min = cur_cell_idx + 1;
                        }
                    }
                } else if cmp.is_gt() {
                    if !EXACT_FORWARD && matches!(seek_op, SeekOp::GE { eq_only: true }) {
                        state.target_cell_when_not_found =
                            state.target_cell_when_not_found.min(cur_cell_idx as i32);
                    }
                    max = cur_cell_idx - 1;
                } else if EXACT_FORWARD || cmp.is_lt() {
                    if matches!(seek_op, SeekOp::LE { eq_only: true }) {
                        state.target_cell_when_not_found =
                            state.target_cell_when_not_found.max(cur_cell_idx as i32);
                    }
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

            state.min_cell_idx = min;
            state.max_cell_idx = max;
            if EXACT_FORWARD {
                state.target_cell_when_not_found = min as i32;
            }
            let target_cell_when_not_found = state.target_cell_when_not_found;
            if !EXACT_FORWARD {
                if let Some(nearest_matching_cell) = state.nearest_matching_cell {
                    cursor.stack.set_cell_index(nearest_matching_cell as i32);
                    cursor.set_has_record(true);
                    return Ok(ControlFlow::Break(IOResult::Done(SeekResult::Found)));
                }
            }
            // if !eq_only - matching entry can exist in neighbour leaf page
            // this can happen if key in the interiour page was deleted - but divider kept untouched
            // in such case BTree can navigate to the leaf which no longer has matching key for seek_op
            // in this case, caller must advance cursor if necessary
            Ok(ControlFlow::Break(IOResult::Done(if seek_op.eq_only() {
                let has_record = target_cell_when_not_found >= 0
                    && target_cell_when_not_found < contents.cell_count() as i32;
                cursor.has_record = has_record;
                cursor.stack.set_cell_index(target_cell_when_not_found);
                SeekResult::NotFound
            } else {
                // set cursor to the position where which would hold the op-boundary if it were present
                cursor.stack.set_cell_index(target_cell_when_not_found);
                SeekResult::TryAdvance
            })))
        }
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn indexbtree_seek(
        &mut self,
        key: &ImmutableRecordRef<'_>,
        seek_op: SeekOp,
    ) -> IOResultOr<SeekResult> {
        let key_values = key.get_values()?;
        let record_comparer = {
            let index_info = self
                .index_info
                .as_ref()
                .expect("indexbtree_seek: index_info required");
            find_compare(key_values.iter().peekable(), index_info)
        };

        tracing::debug!(
            "Using record comparison strategy for seek: {:?}",
            record_comparer
        );

        self.indexbtree_seek_internal(seek_op, record_comparer, &key_values)
    }

    /// Seek using registers directly, avoiding record serialization overhead.
    /// See `seek_unpacked` trait method for rationale.
    #[instrument(skip_all, level = Level::DEBUG)]
    fn indexbtree_seek_unpacked(
        &mut self,
        registers: &[Register],
        seek_op: SeekOp,
    ) -> IOResultOr<SeekResult> {
        let index_info = self
            .index_info
            .as_ref()
            .expect("indexbtree_seek_unpacked: index_info required");

        // SmallVec stores up to MAX_STACK_KEY_VALUES on the stack, spilling to heap only if exceeded
        let key_values: SmallVec<[ValueRef<'_>; STACK_ALLOC_KEY_VALS_MAX]> = registers
            .iter()
            .map(|r| r.get_value().as_value_ref())
            .collect();
        let record_comparer = find_compare(key_values.iter().peekable(), index_info);
        tracing::debug!(
            "Using record comparison strategy for seek: {:?}",
            record_comparer
        );
        self.indexbtree_seek_internal(seek_op, record_comparer, &key_values)
    }

    fn indexbtree_seek_internal(
        &mut self,
        seek_op: SeekOp,
        record_comparer: RecordCompare,
        key_values: &[ValueRef<'_>],
    ) -> IOResultOr<SeekResult> {
        if matches!(
            self.seek_state,
            CursorSeekState::Start
                | CursorSeekState::MovingBetweenPages { .. }
                | CursorSeekState::InteriorPageBinarySearch { .. }
        ) {
            if matches!(self.seek_state, CursorSeekState::Start) {
                if let Some(c) = return_if_io!(self.move_to_root_nonblock()) {
                    return Ok(IOResult::IO(IOCompletions(c)));
                }
            }
            return_if_io!(self.indexbtree_move_to_internal(seek_op, record_comparer, key_values));
            let CursorSeekState::FoundLeaf { eq_seen } = &self.seek_state else {
                unreachable!(
                    "We must still be in FoundLeaf state after indexbtree_move_to_internal, got: {:?}",
                    self.seek_state
                );
            };
            let eq_seen = *eq_seen;
            let page = self.stack.top_ref();

            let contents = page.get_contents();
            let cell_count = contents.cell_count();
            if cell_count == 0 {
                return Ok(IOResult::Done(SeekResult::NotFound));
            }

            let min = 0;
            let max = cell_count as isize - 1;

            // If iter dir is forwards, we want the first cell that matches;
            // If iter dir is backwards, we want the last cell that matches.
            let nearest_matching_cell = None;

            self.seek_state = CursorSeekState::LeafPageBinarySearch {
                state: LeafPageBinarySearchState {
                    min_cell_idx: min,
                    max_cell_idx: max,
                    nearest_matching_cell,
                    eq_seen,
                    target_cell_when_not_found: match seek_op.iteration_direction() {
                        IterationDirection::Forwards => cell_count as i32,
                        IterationDirection::Backwards => -1,
                    },
                },
            };
        }

        let CursorSeekState::LeafPageBinarySearch { state } = &self.seek_state else {
            unreachable!(
                "we must be in a leaf binary search state, got: {:?}",
                self.seek_state
            );
        };

        let old_top_idx = self.stack.current();

        let mut state = *state;

        let result = self.indexbtree_seek_inner(
            seek_op,
            old_top_idx,
            key_values,
            record_comparer,
            &mut state,
        )?;
        // The search state goes back to the cursor once per call, not once
        // per compare, as for the interior pages: only an overflow key read
        // yields, and it returns before the range changes, so re-entry
        // retries the same cell.
        self.seek_state = CursorSeekState::LeafPageBinarySearch { state };
        Ok(result)
    }

    fn indexbtree_seek_inner(
        &mut self,
        seek_op: SeekOp,
        old_top_idx: usize,
        key_values: &[ValueRef<'_>],
        record_comparer: RecordCompare,
        state: &mut LeafPageBinarySearchState,
    ) -> IOResultOr<SeekResult> {
        if matches!(seek_op, SeekOp::GE { eq_only: true }) {
            self.indexbtree_seek_impl::<true>(
                seek_op,
                old_top_idx,
                key_values,
                record_comparer,
                state,
            )
        } else {
            self.indexbtree_seek_impl::<false>(
                seek_op,
                old_top_idx,
                key_values,
                record_comparer,
                state,
            )
        }
    }

    fn indexbtree_seek_impl<const EXACT_FORWARD: bool>(
        &mut self,
        seek_op: SeekOp,
        old_top_idx: usize,
        key_values: &[ValueRef<'_>],
        record_comparer: RecordCompare,
        state: &mut LeafPageBinarySearchState,
    ) -> IOResultOr<SeekResult> {
        let seek_op = if EXACT_FORWARD {
            SeekOp::GE { eq_only: true }
        } else {
            seek_op
        };
        let iter_dir = seek_op.iteration_direction();
        let eq_seen = state.eq_seen;
        let payload_limits = self.payload_limits;
        loop {
            let min = state.min_cell_idx;
            let max = state.max_cell_idx;
            if min > max {
                if let Some(nearest_matching_cell) = state.nearest_matching_cell {
                    self.stack.set_cell_index(nearest_matching_cell as i32);
                    self.set_has_record(true);

                    return Ok(IOResult::Done(SeekResult::Found));
                } else {
                    // set cursor to the position where which would hold the op-boundary if it were present
                    let target_cell = state.target_cell_when_not_found;
                    self.stack.set_cell_index(target_cell);
                    let has_record = target_cell >= 0
                        && target_cell
                            < self
                                .stack
                                .get_page_contents_at_level(old_top_idx)
                                .unwrap()
                                .cell_count() as i32;
                    self.has_record = has_record;

                    // Similar logic as in tablebtree_seek(), but for indexes.
                    // The difference is that since index keys are not necessarily unique, we need to TryAdvance
                    // even when eq_only=true and we have seen an EQ match up in the tree in an interior node.
                    if seek_op.eq_only() && !eq_seen {
                        return Ok(IOResult::Done(SeekResult::NotFound));
                    }
                    return Ok(IOResult::Done(SeekResult::TryAdvance));
                };
            }

            let cur_cell_idx = (min + max) >> 1; // rustc generates extra insns for (min+max)/2 due to them being isize. we know min&max are >=0 here.
            self.stack.set_cell_index(cur_cell_idx as i32);

            let (payload, payload_size, first_overflow_page) = self
                .stack
                .get_page_contents_at_level(old_top_idx)
                .unwrap()
                .cell_read_payload_ptr(cur_cell_idx as usize, payload_limits)?;

            let cell_payload: &[u8] = if let Some(next_page) = first_overflow_page {
                let res = self.process_overflow_read(payload, next_page, payload_size)?;
                if let IOResult::IO(io) = res {
                    return Ok(IOResult::IO(io));
                }
                self.get_immutable_record()
                    .expect("the overflow read filled the reusable record")
                    .get_payload()
            } else {
                payload
            };

            let (cmp, found) = compare_cell_with_key(
                cell_payload,
                key_values,
                seek_op,
                &record_comparer,
                self.index_info
                    .as_ref()
                    .expect("indexbtree_seek: index_info required"),
            )?;
            if found {
                state.nearest_matching_cell.replace(cur_cell_idx as usize);
                match iter_dir {
                    IterationDirection::Forwards => {
                        state.max_cell_idx = cur_cell_idx - 1;
                    }
                    IterationDirection::Backwards => {
                        state.min_cell_idx = cur_cell_idx + 1;
                    }
                }
            } else if cmp.is_gt() {
                if matches!(seek_op, SeekOp::GE { eq_only: true }) {
                    state.target_cell_when_not_found =
                        state.target_cell_when_not_found.min(cur_cell_idx as i32);
                }
                state.max_cell_idx = cur_cell_idx - 1;
            } else if cmp.is_lt() {
                if matches!(seek_op, SeekOp::LE { eq_only: true }) {
                    state.target_cell_when_not_found =
                        state.target_cell_when_not_found.max(cur_cell_idx as i32);
                }
                state.min_cell_idx = cur_cell_idx + 1;
            } else {
                match iter_dir {
                    IterationDirection::Forwards => {
                        state.min_cell_idx = cur_cell_idx + 1;
                    }
                    IterationDirection::Backwards => {
                        state.max_cell_idx = cur_cell_idx - 1;
                    }
                }
            }
        }

        #[inline(always)]
        fn compare_cell_with_key(
            payload: &[u8],
            key_values: &[ValueRef],
            seek_op: SeekOp,
            record_comparer: &RecordCompare,
            index_info: &IndexInfo,
        ) -> Result<(Ordering, bool)> {
            let tie_breaker = get_tie_breaker_from_seek_op(seek_op);
            let cmp =
                record_comparer.compare_payload(payload, key_values, index_info, tie_breaker)?;

            let found = match seek_op {
                SeekOp::GT => cmp.is_gt(),
                SeekOp::GE { eq_only: true } => cmp.is_eq(),
                SeekOp::GE { eq_only: false } => cmp.is_ge(),
                SeekOp::LE { eq_only: true } => cmp.is_eq(),
                SeekOp::LE { eq_only: false } => cmp.is_le(),
                SeekOp::LT => cmp.is_lt(),
            };
            Ok((cmp, found))
        }
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    pub fn move_to(&mut self, key: SeekKey<'_>, cmp: SeekOp) -> IOResultOr<()> {
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
        loop {
            match self.move_to_state {
                MoveToState::Start => {
                    if matches!(self.seek_state, CursorSeekState::Start) {
                        let c = return_if_io!(self.move_to_root_nonblock());
                        self.move_to_state = MoveToState::MoveToPage;
                        if let Some(c) = c {
                            io_yield_one!(c);
                        }
                    } else {
                        self.move_to_state = MoveToState::MoveToPage;
                    }
                }
                MoveToState::MoveToPage => {
                    let ret = match &key {
                        SeekKey::TableRowId(rowid_key) => self.tablebtree_move_to(*rowid_key, cmp),
                        SeekKey::IndexKey(index_key) => self.indexbtree_move_to(index_key, cmp),
                    };
                    return_if_io!(ret);
                    self.move_to_state = MoveToState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Insert a record into the btree.
    /// If the insert operation overflows the page, it will be split and the btree will be balanced.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn insert_into_page(&mut self, bkey: &BTreeKey) -> IOResultOr<()> {
        let record = bkey
            .get_record()
            .expect("expected record present on insert");
        if let CursorState::None = &self.state {
            std::mem::forget(std::mem::replace(
                &mut self.state,
                CursorState::Write(WriteState::Start),
            ));
        }
        let usable_space = self.usable_space();
        let ret = loop {
            let CursorState::Write(write_state) = &mut self.state else {
                panic!("expected write state");
            };
            match write_state {
                WriteState::Start => {
                    let page = self.stack.top();

                    // get page and find cell
                    let cell_idx = {
                        self.pager.add_dirty(&page)?;
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
                    if cell_idx < page.get_contents().cell_count() {
                        let cell = page.get_contents().cell_get(cell_idx, usable_space)?;
                        match cell {
                            BTreeCell::TableLeafCell(tbl_leaf) => {
                                if tbl_leaf.rowid == bkey.to_rowid() {
                                    tracing::debug!("TableLeafCell: found exact match with cell_idx={cell_idx}, overwriting");
                                    self.has_record = true;
                                    *write_state = WriteState::Overwrite {
                                        page,
                                        cell_idx,
                                        state: Some(OverwriteCellState::AllocatePayload),
                                    };
                                    continue;
                                }
                            }
                            BTreeCell::IndexLeafCell(..) | BTreeCell::IndexInteriorCell(..) => {
                                return_if_io!(self.record());
                                let cmp = compare_immutable_iter(
                                    record.iter()?,
                                    self.get_immutable_record()
                                        .as_ref()
                                        .unwrap()
                                        .iter()?,
                                        &self.index_info.as_ref().unwrap().key_info,
                                )?;
                                if cmp == Ordering::Equal {
                                    tracing::debug!("IndexLeafCell: found exact match with cell_idx={cell_idx}, overwriting");
                                    self.set_has_record(true);
                                    let CursorState::Write(write_state) = &mut self.state else {
                                        panic!("expected write state");
                                    };
                                    *write_state = WriteState::Overwrite {
                                        page,
                                        cell_idx,
                                        state: Some(OverwriteCellState::AllocatePayload),
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

                    let CursorState::Write(write_state) = &mut self.state else {
                        panic!("expected write state");
                    };
                    // Reuse the cell payload buffer to avoid allocations
                    let mut payload = take_vec(&mut self.reusable_cell_payload);
                    payload.clear();
                    // Reserve capacity if needed (typical cell is small)
                    // child pointer (4) + payload size varint (up to 9) + rowid varint (up to 9)
                    const MAX_CELL_HEADER: usize = 22;
                    let needed_capacity = record.get_payload().len() + MAX_CELL_HEADER;
                    if payload.capacity() < needed_capacity {
                        crate::with_btree_allocation_site!(
                            CellPayload,
                            payload.try_reserve(needed_capacity - payload.capacity())
                        )?;
                    }
                    // The current state (`WriteState::Start`) has no allocations, so
                    // we std::mem::forget it to save on drop glue
                    std::mem::forget(std::mem::replace(
                        write_state,
                        WriteState::Insert {
                            page,
                            cell_idx,
                            new_payload: payload,
                            fill_cell_payload_state: FillCellPayloadState::Start,
                        },
                    ));
                    continue;
                }
                WriteState::Insert {
                    page,
                    cell_idx,
                    new_payload,
                    ref mut fill_cell_payload_state,
                } => {
                    return_if_io!(fill_cell_payload(
                        &PinGuard::new(page.clone()),
                        bkey.maybe_rowid(),
                        new_payload,
                        *cell_idx,
                        &record,
                        usable_space,
                        &self.pager,
                        fill_cell_payload_state,
                    ));

                    {
                        let contents = page.get_contents();
                        tracing::debug!(name: "overflow", cell_count = contents.cell_count());

                        insert_into_cell(
                            contents,
                            new_payload.as_slice(),
                            *cell_idx,
                            usable_space,
                        )?;
                    };
                    self.stack.set_cell_index(*cell_idx as i32);
                    let overflows = !page.get_contents().overflow_cells.is_empty();

                    // Recover the reusable buffer before transitioning state
                    let recovered_payload = take_vec(new_payload);
                    self.reusable_cell_payload = recovered_payload;

                    if overflows {
                        *write_state = WriteState::Balancing;
                        turso_assert!(matches!(self.balance_state.sub_state, BalanceSubState::Start), "no balancing operation should be in progress during insert", { "state": self.state, "sub_state": self.balance_state.sub_state });
                        // If we balance, we must save the cursor position and seek to it later.
                        self.save_context(CursorContext::seek_eq_only(bkey));
                        inject_io_yield!(
                            self,
                            BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance
                        );
                    } else {
                        *write_state = WriteState::Finish;
                    }
                    continue;
                }
                WriteState::Overwrite {
                    page,
                    cell_idx,
                    ref mut state,
                } => {
                    turso_assert!(page.is_loaded(), "page is not loaded", { "page_id": page.get().id() });
                    let page = page.clone();

                    // Currently it's necessary to .take() here to prevent double-borrow of `self` in `overwrite_cell`.
                    // We insert the state back if overwriting returns IO.
                    let mut state = state.take().expect("state should be present");
                    let cell_idx = *cell_idx;
                    if let IOResult::IO(io) =
                        self.overwrite_cell(&page, cell_idx, &record, &mut state)?
                    {
                        let CursorState::Write(write_state) = &mut self.state else {
                            panic!("expected write state");
                        };
                        *write_state = WriteState::Overwrite {
                            page,
                            cell_idx,
                            state: Some(state),
                        };
                        return Ok(IOResult::IO(io));
                    }
                    let overflows = !page.get_contents().overflow_cells.is_empty();
                    let underflows = !overflows && {
                        let free_space = compute_free_space(page.get_contents(), usable_space)?;
                        free_space * 3 > usable_space * 2
                    };
                    let CursorState::Write(write_state) = &mut self.state else {
                        panic!("expected write state");
                    };
                    if overflows || underflows {
                        *write_state = WriteState::Balancing;
                        turso_assert!(matches!(self.balance_state.sub_state, BalanceSubState::Start), "no balancing operation should be in progress during overwrite", { "state": self.state, "sub_state": self.balance_state.sub_state });
                        // If we balance, we must save the cursor position and seek to it later.
                        self.save_context(CursorContext::seek_eq_only(bkey));
                    } else {
                        *write_state = WriteState::Finish;
                    }
                    continue;
                }
                WriteState::Balancing => {
                    return_if_io!(self.balance(None));
                    let CursorState::Write(write_state) = &mut self.state else {
                        panic!("expected write state");
                    };
                    *write_state = WriteState::Finish;
                }
                WriteState::Finish => {
                    break Ok(IOResult::Done(()));
                }
            };
        };
        if matches!(self.state, CursorState::Write(WriteState::Finish)) {
            // if there was a balance triggered, the cursor position is invalid.
            // it's probably not the greatest idea in the world to do this eagerly here,
            // but at least it works.
            return_if_io!(self.restore_context());
            // WriteState::Finish owns nothing: std::mem::forget it to skip the drop glue
            std::mem::forget(std::mem::replace(&mut self.state, CursorState::None));
        } else {
            self.state = CursorState::None;
        }
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
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn balance(&mut self, balance_ancestor_at_depth: Option<usize>) -> IOResultOr<()> {
        loop {
            let usable_space = self.usable_space();
            let BalanceState {
                sub_state,
                balance_info,
                ..
            } = &mut self.balance_state;
            match sub_state {
                BalanceSubState::Start => {
                    turso_assert!(
                        balance_info.is_none(),
                        "BalanceInfo should be empty on start"
                    );
                    let current_page = self.stack.top_ref();
                    let next_balance_depth =
                        balance_ancestor_at_depth.unwrap_or_else(|| self.stack.current());
                    {
                        // check if we don't need to balance
                        // don't continue if:
                        // - current page is not overfull root
                        // OR
                        // - current page is not overfull and the amount of free space on the page
                        // is less than 2/3rds of the total usable space on the page
                        //
                        // https://github.com/sqlite/sqlite/blob/0aa95099f5003dc99f599ab77ac0004950b281ef/src/btree.c#L9064-L9071
                        let page = current_page.get_contents();
                        let free_space = compute_free_space(page, usable_space)?;
                        let this_level_is_already_balanced = page.overflow_cells.is_empty()
                            && (!self.stack.has_parent() || free_space * 3 <= usable_space * 2);
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
                            *sub_state = BalanceSubState::Start;
                            return Ok(IOResult::Done(()));
                        }
                    }
                    if !self.stack.has_parent() {
                        *sub_state = BalanceSubState::BalanceRoot;
                    } else {
                        *sub_state = BalanceSubState::Decide;
                    }
                }
                BalanceSubState::BalanceRoot => {
                    return_if_io!(self.balance_root());

                    let BalanceState { sub_state, .. } = &mut self.balance_state;
                    *sub_state = BalanceSubState::Decide;
                }
                BalanceSubState::Decide => {
                    let cur_page = self.stack.top_ref();
                    let cur_page_contents = cur_page.get_contents();

                    // Check if we can use the balance_quick() fast path.
                    let mut do_quick = false;
                    if cur_page_contents.page_type()? == PageType::TableLeaf
                        && cur_page_contents.overflow_cells.len() == 1
                    {
                        let overflow_cell_is_last =
                            cur_page_contents.overflow_cells.first().unwrap().index
                                == cur_page_contents.cell_count();
                        if overflow_cell_is_last {
                            let parent = self
                                .stack
                                .get_page_at_level(self.stack.current() - 1)
                                .expect("parent page should be on the stack");
                            let parent_contents = parent.get_contents();
                            let parent_rightmost =
                                parent_contents.rightmost_pointer()?.ok_or_else(|| {
                                    mark_unlikely();
                                    LimboError::Corrupt(format!(
                                        "parent page {} is a leaf page, expected interior page",
                                        parent.get().id()
                                    ))
                                })?;
                            if parent.get().id() != 1
                                && parent_rightmost == cur_page.get().id() as u32
                            {
                                // If all of the following are true, we can use the balance_quick() fast path:
                                // - The page is a table leaf page
                                // - The overflow cell would be the last cell on the leaf page
                                // - The parent page is not page 1
                                // - The leaf page is the rightmost page in the subtree
                                do_quick = true;
                            }
                        }
                    }

                    let BalanceState { sub_state, .. } = &mut self.balance_state;
                    if do_quick {
                        *sub_state = BalanceSubState::Quick;
                    } else {
                        *sub_state = BalanceSubState::NonRootPickSiblings;
                        self.stack.pop();
                    }
                }
                BalanceSubState::Quick => {
                    return_if_io!(self.balance_quick());
                }
                BalanceSubState::NonRootPickSiblings
                | BalanceSubState::NonRootDoBalancing
                | BalanceSubState::NonRootDoBalancingAllocate { .. }
                | BalanceSubState::NonRootDoBalancingFinish { .. }
                | BalanceSubState::FreePages { .. } => {
                    return_if_io!(self.balance_non_root());
                }
            }
        }
    }

    /// Fast balancing routine for the common special case where the rightmost leaf page of a given subtree overflows (= an append).
    /// In this case we just add a new leaf page as the right sibling of that page, and insert a new divider cell into the parent.
    /// The high level steps are:
    /// 1. Allocate a new leaf page and insert the overflow cell payload in it.
    /// 2. Create a new divider cell in the parent - it contains the page number of the old rightmost leaf, plus the largest rowid on that page.
    /// 3. Update the rightmost pointer of the parent to point to the new leaf page.
    /// 4. Continue balance from the parent page (inserting the new divider cell may have overflowed the parent)
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn balance_quick(&mut self) -> IOResultOr<()> {
        // Since we are going to change the btree structure, let's forget our cached knowledge of the rightmost page.
        let _ = self.move_to_right_state.1.take();

        // Allocate a new leaf page and insert the overflow cell payload in it.
        let new_rightmost_leaf = return_if_io!(self.pager.do_allocate_page(
            PageType::TableLeaf,
            0,
            BtreePageAllocMode::Any
        ));
        self.pager.add_dirty(&new_rightmost_leaf)?;

        let usable_space = self.usable_space();
        let old_rightmost_leaf = self.stack.top_ref();
        let old_rightmost_leaf_contents = old_rightmost_leaf.get_contents();
        turso_assert!(
            old_rightmost_leaf_contents.overflow_cells.len() == 1,
            "expected 1 overflow cell",
            { "overflow_cell_count": old_rightmost_leaf_contents.overflow_cells.len() }
        );

        let parent = self
            .stack
            .get_page_at_level(self.stack.current() - 1)
            .expect("parent page should be on the stack");
        self.pager.add_dirty(parent)?;
        let parent_contents = parent.get_contents();
        let rightmost_pointer = parent_contents
            .rightmost_pointer()?
            .expect("parent should have a rightmost pointer");
        turso_assert!(
            rightmost_pointer == old_rightmost_leaf.get().id() as u32,
            "leaf should be the rightmost page in the subtree"
        );

        let overflow_cell = old_rightmost_leaf_contents
            .overflow_cells
            .pop()
            .expect("overflow cell should be present");
        turso_assert!(
            overflow_cell.index == old_rightmost_leaf_contents.cell_count(),
            "overflow cell must be the last cell in the leaf"
        );

        let new_rightmost_leaf_contents = new_rightmost_leaf.get_contents();
        insert_into_cell(
            new_rightmost_leaf_contents,
            &overflow_cell.payload.as_ref(),
            0,
            usable_space,
        )?;

        // Create a new divider cell in the parent - it contains the page number of the old rightmost leaf, plus the largest rowid on that page.
        let mut new_divider: [u8; 13] = [0; 13]; // 4 bytes for page number, max 9 bytes for rowid (varint)
        new_divider[0..4].copy_from_slice(&(old_rightmost_leaf.get().id() as u32).to_be_bytes());
        let largest_rowid = old_rightmost_leaf_contents
            .cell_table_leaf_read_rowid(old_rightmost_leaf_contents.cell_count() - 1)?;
        let n = write_varint(&mut new_divider[4..], largest_rowid as u64);
        let divider_length = 4 + n;

        // Insert the new divider cell into the parent.
        insert_into_cell(
            parent_contents,
            &new_divider[..divider_length],
            parent_contents.cell_count(),
            usable_space,
        )?;
        parent_contents.write_rightmost_ptr(new_rightmost_leaf.get().id() as u32);
        // Continue balance from the parent page (inserting the new divider cell may have overflowed the parent)
        self.stack.pop();

        let BalanceState { sub_state, .. } = &mut self.balance_state;
        *sub_state = BalanceSubState::Start;
        Ok(IOResult::Done(()))
    }

    /// Balance a non root page by trying to balance cells between a maximum of 3 siblings that should be neighboring the page that overflowed/underflowed.
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn balance_non_root(&mut self) -> IOResultOr<()> {
        loop {
            let usable_space = self.usable_space();
            let BalanceState {
                sub_state,
                balance_info,
                reusable_divider_buffers,
                reusable_cell_payloads,
                sibling_load_group,
            } = &mut self.balance_state;
            tracing::debug!(?sub_state);

            match sub_state {
                BalanceSubState::Start
                | BalanceSubState::BalanceRoot
                | BalanceSubState::Decide
                | BalanceSubState::Quick => {
                    panic!("balance_non_root: unexpected state {sub_state:?}")
                }
                BalanceSubState::NonRootPickSiblings => {
                    // Since we are going to change the btree structure, let's forget our cached knowledge of the rightmost page.
                    let _ = self.move_to_right_state.1.take();

                    let (parent_page_idx, page_type, cell_count, over_cell_count) = {
                        let parent_page = self.stack.top_ref();
                        let parent_contents = parent_page.get_contents();
                        (
                            self.stack.current(),
                            parent_contents.page_type()?,
                            parent_contents.cell_count(),
                            parent_contents.overflow_cells.len(),
                        )
                    };

                    turso_assert!(
                        matches!(page_type, PageType::IndexInterior | PageType::TableInterior),
                        "expected index or table interior page"
                    );
                    let number_of_cells_in_parent = cell_count + over_cell_count;

                    // If `seek` moved to rightmost page, cell index will be out of bounds. Meaning cell_count+1.
                    // In any other case, `seek` will stay in the correct index.
                    let past_rightmost_pointer =
                        self.stack.current_cell_index() as usize == number_of_cells_in_parent + 1;
                    if past_rightmost_pointer {
                        self.stack.retreat();
                    }

                    let parent_page = self.stack.get_page_at_level(parent_page_idx).unwrap();
                    let parent_contents = parent_page.get_contents();
                    if !past_rightmost_pointer && over_cell_count > 0 {
                        // The ONLY way we can have an overflow cell in the parent is if we replaced an interior cell from a cell in the child, and that replacement did not fit.
                        // This can only happen on index btrees.
                        if matches!(page_type, PageType::IndexInterior) {
                            turso_assert!(parent_contents.overflow_cells.len() == 1, "index interior page must have no more than 1 overflow cell, as a result of InteriorNodeReplacement");
                        } else {
                            turso_assert!(false, "page type must have no overflow cells", {
                                "page_type": page_type
                            });
                        }
                        let overflow_cell = parent_contents.overflow_cells.first().unwrap();
                        let parent_page_cell_idx = self.stack.current_cell_index() as usize;
                        // Parent page must be positioned at the divider cell that overflowed due to the replacement.
                        turso_assert!(
                            overflow_cell.index == parent_page_cell_idx,
                            "overflow cell index must be the result of InteriorNodeReplacement that leaves both child and parent unbalanced, and hence parent page's position must equal overflow_cell.index",
                            { "parent_page_id": parent_page.get().id(), "parent_page_cell_idx": parent_page_cell_idx, "overflow_cell_index": overflow_cell.index }
                        );
                    }
                    self.pager.add_dirty(parent_page)?;
                    let parent_contents = parent_page.get_contents();
                    let page_to_balance_idx = self.stack.current_cell_index() as usize;

                    tracing::debug!(
                        "balance_non_root(parent_id={} page_to_balance_idx={})",
                        parent_page.get().id(),
                        page_to_balance_idx
                    );
                    // Part 1: Find the sibling pages to balance
                    let mut pages_to_balance: [Option<PinGuard>; MAX_SIBLING_PAGES_TO_BALANCE] =
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
                        parent_contents.rightmost_pointer_raw()?.unwrap()
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
                        let actual_cell_idx = first_cell_divider + sibling_pointer
                            - parent_contents.overflow_cells.len();
                        let start_of_cell =
                            parent_contents.cell_get_raw_start_offset(actual_cell_idx);
                        let buf = parent_contents.as_ptr().as_mut_ptr();
                        unsafe { buf.add(start_of_cell) }
                    };

                    // load sibling pages
                    // start loading right page first
                    let mut pgno: u32 =
                        unsafe { right_pointer.cast::<u32>().read_unaligned().swap_bytes() };
                    let current_sibling = sibling_pointer;
                    let group =
                        sibling_load_group.get_or_insert_with(|| CompletionGroup::new(|_| {}));
                    for i in (0..=current_sibling).rev() {
                        match self.pager.read_page_into(pgno as i64, Some(group)) {
                            Err(e) => {
                                mark_unlikely();
                                tracing::error!("error reading page {}: {}", pgno, e);
                                // Drain any in-flight reads we accumulated
                                // across previous iterations / yields so the
                                // IO scheduler can finalize them before we
                                // bail out.
                                self.pager.io.drain_completions(group.completions())?;
                                *sibling_load_group = None;
                                return Err(e);
                            }
                            Ok(IOResult::Done((page, _))) => {
                                pages_to_balance[i].replace(PinGuard::new(page));
                            }
                            Ok(IOResult::IO(IOCompletions(spill_c))) => {
                                // Spill yield. The loop is fully re-entrant:
                                // on re-entry we re-execute from the top of
                                // `NonRootPickSiblings`, the pager's
                                // `pending_reads` returns the same PageRef
                                // for the in-flight sibling (its read is
                                // already in the group), and cache hits for
                                // previously-loaded siblings need no read.
                                io_yield_one!(spill_c);
                            }
                        }
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
                                matches!(
                                    parent_contents.page_type().ok(),
                                    Some(PageType::IndexInterior)
                                ),
                                "expected index interior page",
                                { "page_type": parent_contents.page_type().ok() }
                            );
                            turso_assert!(
                                parent_contents.overflow_cells.len() == 1,
                                "must have a single overflow cell in the parent, as a result of InteriorNodeReplacement"
                            );
                            let overflow_cell = parent_contents.overflow_cells.first().unwrap();
                            pgno =
                                u32::from_be_bytes(overflow_cell.payload[0..4].try_into().unwrap());
                        } else {
                            // grep for 'OVERFLOW CELL ADJUSTMENT' for explanation.
                            // here we only subtract 1 if the divider cell has been shifted left, i.e. the overflow cell was placed to the left
                            // this cell.
                            let actual_cell_idx = if let Some(overflow_cell) =
                                parent_contents.overflow_cells.first()
                            {
                                if next_cell_divider < overflow_cell.index {
                                    next_cell_divider
                                } else {
                                    next_cell_divider - 1
                                }
                            } else {
                                next_cell_divider
                            };
                            pgno = match parent_contents.cell_get(actual_cell_idx, usable_space)? {
                                BTreeCell::TableInteriorCell(TableInteriorCell {
                                    left_child_page,
                                    ..
                                })
                                | BTreeCell::IndexInteriorCell(IndexInteriorCell {
                                    left_child_page,
                                    ..
                                }) => left_child_page,
                                other => {
                                    mark_unlikely();
                                    crate::bail_corrupt_error!(
                                        "expected interior cell, got {:?}",
                                        other
                                    )
                                }
                            };
                        }
                    }

                    balance_info.replace(BalanceInfo {
                        pages_to_balance,
                        rightmost_pointer: right_pointer,
                        sibling_count,
                        first_divider_cell: first_cell_divider,
                        reusable_divider_cell: crate::alloc::vec![],
                    });
                    *sub_state = BalanceSubState::NonRootDoBalancing;
                    // Wait for the sibling reads issued across (possibly
                    // multiple) entries into this state. Take the group so
                    // the next balance starts fresh.
                    let completion = sibling_load_group
                        .take()
                        .expect("the sibling loop above created the group")
                        .build();
                    if !completion.finished() {
                        io_yield_one!(completion);
                    }
                }
                BalanceSubState::NonRootDoBalancing => {
                    // Ensure all involved pages are in memory.
                    let balance_info = balance_info.as_mut().unwrap();
                    for page in balance_info
                        .pages_to_balance
                        .iter()
                        .take(balance_info.sibling_count)
                    {
                        let page = page.as_ref().unwrap();
                        self.pager.add_dirty(page)?;

                        #[cfg(debug_assertions)]
                        let page_type_of_siblings = balance_info.pages_to_balance[0]
                            .as_ref()
                            .unwrap()
                            .get_contents()
                            .page_type()
                            .ok();

                        #[cfg(debug_assertions)]
                        {
                            let contents = page.get_contents();
                            debug_validate_cells!(&contents, usable_space);
                            turso_assert_eq!(contents.page_type().ok(), page_type_of_siblings);
                        }
                    }
                    // Start balancing.
                    let parent_page = PinGuard::new(self.stack.top_ref().clone());
                    let parent_contents = parent_page.get_contents();

                    // Pre-compute parent page parameters for faster cell region lookups.
                    // Note: cell_count cannot be pre-computed as it changes during the loop via drop_cell.
                    let parent_page_type = parent_contents.page_type()?;
                    let parent_max_local =
                        payload_overflow_threshold_max(parent_page_type, usable_space);
                    let parent_min_local =
                        payload_overflow_threshold_min(parent_page_type, usable_space);

                    // 1. Collect cell data from divider cells, and count the total number of cells to be distributed.
                    // The count includes: all cells and overflow cells from the sibling pages, and divider cells from the parent page,
                    // excluding the rightmost divider, which will not be dropped from the parent; instead it will be updated at the end.
                    let mut total_cells_to_redistribute = 0;
                    let pages_to_balance_new: [Option<PinGuard>;
                        MAX_NEW_SIBLING_PAGES_AFTER_BALANCE] =
                        [const { None }; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE];
                    for i in (0..balance_info.sibling_count).rev() {
                        let sibling_page = balance_info.pages_to_balance[i].as_ref().unwrap();
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
                                matches!(
                                    parent_contents.page_type().ok(),
                                    Some(PageType::IndexInterior)
                                ),
                                "expected index interior page",
                                { "page_type": parent_contents.page_type().ok() }
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
                            // Use pre-computed page parameters for faster lookup.
                            // Note: cell_count must be fresh as it changes during the loop.
                            let (cell_start, cell_len) = parent_contents
                                ._cell_get_raw_region_faster(
                                    actual_cell_idx,
                                    usable_space,
                                    parent_contents.cell_count(),
                                    parent_max_local,
                                    parent_min_local,
                                    parent_page_type,
                                )?;
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

                        // Reuse the divider buffer to avoid allocation per balance operation.
                        // The buffer is cleared and filled with the new cell data.
                        reusable_divider_buffers[i].clear();
                        reusable_divider_buffers[i].extend_from_slice(cell_buf);
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
                            drop_cell(parent_contents, actual_cell_idx, usable_space)?;
                        }
                    }

                    /* 2. Initialize CellArray with all the cells used for distribution, this includes divider cells if !leaf. */
                    // Reuse the cell_payloads Vec from previous balance operations to avoid allocation.
                    let mut cell_payloads_vec = take_vec(reusable_cell_payloads);
                    cell_payloads_vec.clear();
                    // Ensure we have at least total_cells_to_redistribute capacity.
                    // Since len=0 after clear, reserve(n) ensures capacity >= n.
                    cell_payloads_vec.reserve(total_cells_to_redistribute);
                    let mut cell_array = CellArray {
                        cell_payloads: cell_payloads_vec,
                        cell_count_per_page_cumulative: [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
                    };
                    let cells_capacity_start = cell_array.cell_payloads.capacity();

                    let mut total_cells_inserted = 0;
                    // This is otherwise identical to CellArray.cell_count_per_page_cumulative,
                    // but we exclusively track what the prefix sums were _before_ we started redistributing cells.
                    let mut old_cell_count_per_page_cumulative: [u16;
                        MAX_NEW_SIBLING_PAGES_AFTER_BALANCE] =
                        [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE];

                    let page_type = balance_info.pages_to_balance[0]
                        .as_ref()
                        .unwrap()
                        .get_contents()
                        .page_type()?;
                    tracing::debug!("balance_non_root(page_type={:?})", page_type);
                    let is_table_leaf = matches!(page_type, PageType::TableLeaf);
                    let is_leaf = matches!(page_type, PageType::TableLeaf | PageType::IndexLeaf);
                    for (i, old_page) in balance_info
                        .pages_to_balance
                        .iter()
                        .take(balance_info.sibling_count)
                        .enumerate()
                    {
                        let old_page = old_page.as_ref().unwrap();
                        let old_page_contents = old_page.get_contents();
                        let page_type = old_page_contents.page_type()?;
                        let max_local = payload_overflow_threshold_max(page_type, usable_space);
                        let min_local = payload_overflow_threshold_min(page_type, usable_space);
                        let cell_count = old_page_contents.cell_count();
                        debug_validate_cells!(&old_page_contents, usable_space);
                        for cell_idx in 0..cell_count {
                            let (cell_start, cell_len) = old_page_contents
                                ._cell_get_raw_region_faster(
                                    cell_idx,
                                    usable_space,
                                    cell_count,
                                    max_local,
                                    min_local,
                                    page_type,
                                )?;
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

                        old_cell_count_per_page_cumulative[i] =
                            cell_array.cell_payloads.len() as u16;

                        let mut cells_inserted =
                            old_page_contents.cell_count() + old_page_contents.overflow_cells.len();

                        let is_last_sibling = i == balance_info.sibling_count - 1;
                        if !is_last_sibling && !is_table_leaf {
                            // If we are a index page or a interior table page we need to take the divider cell too.
                            // But we don't need the last divider as it will remain the same.
                            if is_leaf {
                                // The divider holds the leaf cell's real size after its child pointer;
                                // back on a leaf the cell takes the minimum size again.
                                ensure_min_cell_size(
                                    &mut reusable_divider_buffers[i],
                                    LEFT_CHILD_PTR_SIZE_BYTES,
                                );
                            }
                            let mut divider_cell = reusable_divider_buffers[i].as_mut_slice();
                            // TODO(pere): in case of old pages are leaf pages, so index leaf page, we need to strip page pointers
                            // from divider cells in index interior pages (parent) because those should not be included.
                            cells_inserted += 1;
                            if !is_leaf {
                                // This divider cell needs to be updated with new left pointer,
                                let right_pointer = old_page_contents.rightmost_pointer()?.unwrap();
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

                    // Verify that all cells were collected correctly.
                    // Note: For table leaf pages, dividers are counted in total_cells_to_redistribute
                    // but are NOT included in cell_array (they stay in parent as bookkeeping).
                    // For index/interior pages, dividers ARE included in cell_array.
                    let dividers_in_parent_only = if is_table_leaf {
                        // Table leaf: dividers are NOT added to cell_array
                        balance_info.sibling_count.saturating_sub(1)
                    } else {
                        // Index/interior: dividers ARE added to cell_array
                        0
                    };
                    let expected_cells_in_array =
                        total_cells_to_redistribute - dividers_in_parent_only;
                    turso_assert!(
                        cell_array.cell_payloads.len() == expected_cells_in_array,
                        "cell count mismatch after collection",
                        { "collected": cell_array.cell_payloads.len(), "expected": expected_cells_in_array, "total_cells_to_redistribute": total_cells_to_redistribute, "dividers_in_parent_only": dividers_in_parent_only, "is_table_leaf": is_table_leaf }
                    );
                    turso_assert!(
                        total_cells_inserted == expected_cells_in_array,
                        "cell count mismatch between total cells inserted and expected",
                        { "total_cells_inserted": total_cells_inserted, "expected_cells_in_array": expected_cells_in_array, "total_cells_to_redistribute": total_cells_to_redistribute, "dividers_in_parent_only": dividers_in_parent_only }
                    );

                    // Let's copy all cells for later checks
                    #[cfg(debug_assertions)]
                    let mut cells_debug: crate::alloc::Vec<
                        crate::alloc::Vec<u8>,
                    > = crate::alloc::vec![];
                    #[cfg(debug_assertions)]
                    {
                        for cell in &cell_array.cell_payloads {
                            crate::with_btree_allocation_site!(Balance, {
                                let cell = cell.try_to_vec()?;
                                cells_debug.try_push(cell)
                            })?;
                            if is_leaf {
                                crate::turso_assert_ne!(cell[0], 0);
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
                    let usable_space_without_header = usable_space - header_size;
                    for i in 0..balance_info.sibling_count {
                        cell_array.cell_count_per_page_cumulative[i] =
                            old_cell_count_per_page_cumulative[i];
                        let page = &balance_info.pages_to_balance[i].as_ref().unwrap();
                        let page_contents = page.get_contents();
                        let free_space = compute_free_space(page_contents, usable_space)?;

                        new_page_sizes[i] = usable_space_without_header as i64 - free_space as i64;
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
                        while new_page_sizes[i] > usable_space_without_header as i64 {
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
                            let size_of_cell_to_remove_from_left = 2 + cell_array.cell_payloads
                                [cell_array.cell_count_up_to_page(i) - 1]
                                .len()
                                as i64;
                            new_page_sizes[i] -= size_of_cell_to_remove_from_left;
                            let size_of_cell_to_move_right = if !is_table_leaf {
                                if cell_array.cell_count_per_page_cumulative[i]
                                    < cell_array.cell_payloads.len() as u16
                                {
                                    // This means we move to the right page the divider cell and we
                                    // promote left cell to divider
                                    CELL_PTR_SIZE_BYTES as i64
                                        + cell_array.cell_payloads
                                            [cell_array.cell_count_up_to_page(i)]
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
                                + cell_array.cell_payloads[cell_array.cell_count_up_to_page(i)]
                                    .len() as i64;
                            let can_take = new_page_sizes[i] + size_of_cell_to_remove_from_right
                                > usable_space_without_header as i64;
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
                                        + cell_array.cell_payloads
                                            [cell_array.cell_count_up_to_page(i)]
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
                            let cell_left_size =
                                cell_array.cell_size_bytes(cell_left as usize) as i64;
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
                        turso_assert_greater_than!(
                            cell_array.cell_count_per_page_cumulative[i - 1],
                            if i > 1 {
                                cell_array.cell_count_per_page_cumulative[i - 2]
                            } else {
                                0
                            }
                        );
                    }

                    *sub_state = BalanceSubState::NonRootDoBalancingAllocate {
                        i: 0,
                        context: Some(BalanceContext {
                            pages_to_balance_new,
                            sibling_count_new,
                            cell_array,
                            old_cell_count_per_page_cumulative,
                            #[cfg(debug_assertions)]
                            cells_debug,
                        }),
                    };
                }
                BalanceSubState::NonRootDoBalancingAllocate { i, context } => {
                    let BalanceContext {
                        pages_to_balance_new,
                        old_cell_count_per_page_cumulative,
                        cell_array,
                        sibling_count_new,
                        ..
                    } = context.as_mut().unwrap();
                    let pager = self.pager.clone();
                    let balance_info = balance_info.as_mut().unwrap();
                    let page_type = balance_info.pages_to_balance[0]
                        .as_ref()
                        .unwrap()
                        .get_contents()
                        .page_type()?;
                    // Allocate pages or set dirty if not needed
                    if *i < balance_info.sibling_count {
                        let page = balance_info.pages_to_balance[*i].as_ref().unwrap();
                        turso_assert!(page.is_dirty(), "sibling page must be already marked dirty");
                        pages_to_balance_new[*i].replace(page.clone());
                    } else {
                        let page = return_if_io!(pager.do_allocate_page(
                            page_type,
                            0,
                            BtreePageAllocMode::Any
                        ));
                        pages_to_balance_new[*i].replace(PinGuard::new(page));
                        // Since this page didn't exist before, we can set it to cells length as it
                        // marks them as empty since it is a prefix sum of cells.
                        old_cell_count_per_page_cumulative[*i] =
                            cell_array.cell_payloads.len() as u16;
                    }
                    if *i + 1 < *sibling_count_new {
                        *i += 1;
                        continue;
                    } else {
                        *sub_state = BalanceSubState::NonRootDoBalancingFinish {
                            context: context.take().unwrap(),
                        };
                    }
                }
                BalanceSubState::NonRootDoBalancingFinish {
                    context:
                        BalanceContext {
                            pages_to_balance_new,
                            sibling_count_new,
                            cell_array,
                            old_cell_count_per_page_cumulative,
                            #[cfg(debug_assertions)]
                            cells_debug,
                        },
                } => {
                    let balance_info = balance_info.as_mut().unwrap();
                    let page_type = balance_info.pages_to_balance[0]
                        .as_ref()
                        .unwrap()
                        .get_contents()
                        .page_type()?;
                    let parent_is_root = !self.stack.has_parent();
                    let parent_page = PinGuard::new(self.stack.top_ref().clone());
                    let parent_contents = parent_page.get_contents();
                    let mut sibling_count_new = *sibling_count_new;
                    let is_table_leaf = matches!(page_type, PageType::TableLeaf);
                    // Reassign page numbers in increasing order
                    {
                        let mut page_numbers: [usize; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE] =
                            [0; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE];
                        for (i, page) in pages_to_balance_new
                            .iter()
                            .take(sibling_count_new)
                            .enumerate()
                        {
                            page_numbers[i] = page.as_ref().unwrap().get().id();
                        }
                        page_numbers.sort_unstable();
                        for (page, new_id) in pages_to_balance_new
                            .iter()
                            .take(sibling_count_new)
                            .rev()
                            .zip(page_numbers.iter().rev().take(sibling_count_new))
                        {
                            let page = page.as_ref().unwrap();
                            if *new_id != page.get().id() {
                                page.get().set_id(*new_id);
                                self.pager
                                    .upsert_page_in_cache(*new_id, page.0.clone(), true)?;
                            }
                        }

                        #[cfg(debug_assertions)]
                        {
                            tracing::debug!(
                                "balance_non_root(parent page_id={})",
                                parent_page.get().id()
                            );
                            for page in pages_to_balance_new.iter().take(sibling_count_new) {
                                tracing::debug!(
                                    "balance_non_root(new_sibling page_id={})",
                                    page.as_ref().unwrap().get().id()
                                );
                            }
                        }
                    }

                    // pages_pointed_to helps us debug we did in fact create divider cells to all the new pages and the rightmost pointer,
                    // also points to the last page.
                    #[cfg(debug_assertions)]
                    let mut pages_pointed_to = HashSet::default();

                    // Write right pointer in parent page to point to new rightmost page. keep in mind
                    // we update rightmost pointer first because inserting cells could defragment parent page,
                    // therfore invalidating the pointer.
                    let right_page_id = pages_to_balance_new[sibling_count_new - 1]
                        .as_ref()
                        .unwrap()
                        .get()
                        .id() as u32;
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
                    let is_leaf_page =
                        matches!(page_type, PageType::TableLeaf | PageType::IndexLeaf);
                    if !is_leaf_page {
                        let last_sibling_idx = balance_info.sibling_count - 1;
                        let last_page = balance_info.pages_to_balance[last_sibling_idx]
                            .as_ref()
                            .unwrap();
                        let right_pointer = last_page.get_contents().rightmost_pointer()?.unwrap();
                        let new_last_page = pages_to_balance_new[sibling_count_new - 1]
                            .as_ref()
                            .unwrap();
                        new_last_page
                            .get_contents()
                            .write_rightmost_ptr(right_pointer);
                    }
                    turso_assert!(
                        parent_contents.overflow_cells.is_empty(),
                        "parent page overflow cells should be empty before divider cell reinsertion"
                    );
                    // TODO: pointer map update (vacuum support)
                    // Update divider cells in parent
                    // Cache first_divider_cell to allow mutable access to reusable_divider_cell
                    let first_divider_cell_cached = balance_info.first_divider_cell;
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
                        // Reuse the buffer for constructing new divider cell to avoid allocation per iteration
                        balance_info.reusable_divider_cell.clear();
                        if !is_leaf_page {
                            // Interior
                            // Make this page's rightmost pointer point to pointer of divider cell before modification
                            let previous_pointer_divider = read_u32(divider_cell, 0);
                            page.get_contents()
                                .write_rightmost_ptr(previous_pointer_divider);
                            // divider cell now points to this page
                            balance_info
                                .reusable_divider_cell
                                .extend_from_slice(&(page.get().id() as u32).to_be_bytes());
                            // now copy the rest of the divider cell:
                            // Table Interior page:
                            //   * varint rowid
                            // Index Interior page:
                            //   * varint payload size
                            //   * payload
                            //   * first overflow page (u32 optional)
                            balance_info
                                .reusable_divider_cell
                                .extend_from_slice(&divider_cell[4..]);
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
                            balance_info
                                .reusable_divider_cell
                                .extend_from_slice(&(page.get().id() as u32).to_be_bytes());
                            write_varint_to_vec(rowid, &mut balance_info.reusable_divider_cell)?;
                        } else {
                            // Leaf index
                            // A leaf cell is read as at least MINIMUM_CELL_SIZE bytes, so a 2-byte
                            // record carries one byte of padding here. The parent stores the cell's
                            // real size after the child pointer, so drop the padding when promoting.
                            let (payload_len, n_payload) = read_varint(divider_cell)?;
                            let real_len = n_payload + payload_len as usize;
                            let divider_cell = if real_len < divider_cell.len() {
                                turso_assert!(
                                    real_len < MINIMUM_CELL_SIZE,
                                    "only cells below the minimum cell size carry padding",
                                    { "real_len": real_len, "cell_len": divider_cell.len() }
                                );
                                &divider_cell[..real_len]
                            } else {
                                &divider_cell[..]
                            };
                            balance_info
                                .reusable_divider_cell
                                .extend_from_slice(&(page.get().id() as u32).to_be_bytes());
                            balance_info
                                .reusable_divider_cell
                                .extend_from_slice(divider_cell);
                        }

                        let left_pointer = read_u32(
                            &balance_info.reusable_divider_cell[..LEFT_CHILD_PTR_SIZE_BYTES],
                            0,
                        );
                        turso_assert!(
                            left_pointer != parent_page.get().id() as u32,
                            "left pointer is the same as parent page id"
                        );
                        #[cfg(debug_assertions)]
                        {
                            pages_pointed_to.insert(left_pointer);
                            tracing::debug!(
                                "balance_non_root(insert_divider_cell, first_divider_cell={}, divider_cell={}, left_pointer={})",
                                first_divider_cell_cached,
                                sibling_page_idx,
                                left_pointer
                            );
                        }
                        turso_assert!(
                            left_pointer == page.get().id() as u32,
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
                            "invalid page number divider left pointer exceeds database number of pages",
                            { "left_pointer": left_pointer, "database_size": database_size }
                        );
                        let divider_cell_insert_idx_in_parent =
                            first_divider_cell_cached + sibling_page_idx;
                        #[cfg(debug_assertions)]
                        let overflow_cell_count_before = parent_contents.overflow_cells.len();
                        insert_into_cell(
                            parent_contents,
                            &balance_info.reusable_divider_cell,
                            divider_cell_insert_idx_in_parent,
                            usable_space,
                        )?;
                        #[cfg(debug_assertions)]
                        {
                            let overflow_cell_count_after = parent_contents.overflow_cells.len();
                            let divider_cell_is_overflow_cell =
                                overflow_cell_count_after > overflow_cell_count_before;

                            BTreeCursor::validate_balance_non_root_divider_cell_insertion(
                                balance_info,
                                parent_contents,
                                divider_cell_insert_idx_in_parent,
                                divider_cell_is_overflow_cell,
                                page,
                                usable_space,
                            );
                        }
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
                            turso_assert!(
                                pages_pointed_to.contains(&(page.get().id() as u32)),
                                "page not pointed to by divider cell or rightmost pointer",
                                { "page_id": page.get().id() }
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
                            let (start_old_cells, start_new_cells, number_new_cells) = if page_idx
                                == 0
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
                                let start_new_cells = cell_array
                                    .cell_count_up_to_page(page_idx - 1)
                                    + (!is_table_leaf) as usize;
                                (
                                    start_old_cells,
                                    start_new_cells,
                                    cell_array.cell_count_up_to_page(page_idx) - start_new_cells,
                                )
                            };
                            let page = pages_to_balance_new[page_idx].as_ref().unwrap();
                            tracing::debug!("pre_edit_page(page={})", page.get().id());
                            let page_contents = page.get_contents();
                            edit_page(
                                page_contents,
                                start_old_cells,
                                start_new_cells,
                                number_new_cells,
                                cell_array,
                                usable_space,
                            )?;
                            debug_validate_cells!(page_contents, usable_space);
                            tracing::trace!(
                                "edit_page page={} cells={}",
                                page.get().id(),
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
                        && parent_contents.offset()
                            <= compute_free_space(first_child_contents, usable_space)?
                    {
                        // From SQLite:
                        // The root page of the b-tree now contains no cells. The only sibling
                        // page is the right-child of the parent. Copy the contents of the
                        // child page into the parent, decreasing the overall height of the
                        // b-tree structure by one. This is described as the "balance-shallower"
                        // sub-algorithm in some documentation.
                        turso_assert_eq!(sibling_count_new, 1);
                        let parent_offset = if parent_page.get().id() == 1 {
                            DatabaseHeader::SIZE
                        } else {
                            0
                        };
                        #[cfg(debug_assertions)]
                        turso_assert_eq!(parent_offset, parent_contents.offset());

                        // From SQLite:
                        // It is critical that the child page be defragmented before being
                        // copied into the parent, because if the parent is page 1 then it will
                        // by smaller than the child due to the database header, and so
                        // all the free space needs to be up front.
                        defragment_page_full(first_child_contents, usable_space)?;

                        let child_top = first_child_contents.cell_content_area() as usize;
                        let parent_buf = parent_contents.as_ptr();
                        let child_buf = first_child_contents.as_ptr();
                        let content_size = usable_space - child_top;

                        // Copy cell contents
                        parent_buf[child_top..child_top + content_size]
                            .copy_from_slice(&child_buf[child_top..child_top + content_size]);

                        // Copy header and pointer
                        // NOTE: don't use .cell_pointer_array_offset_and_size() because of different
                        // header size
                        let header_and_pointer_size = first_child_contents.header_size()
                            + first_child_contents.cell_pointer_array_size();
                        let first_child_offset = first_child_contents.offset();
                        parent_buf[parent_offset..parent_offset + header_and_pointer_size]
                            .copy_from_slice(
                                &child_buf[first_child_offset
                                    ..first_child_offset + header_and_pointer_size],
                            );

                        sibling_count_new -= 1; // decrease sibling count for debugging and free at the end
                        turso_assert_less_than!(sibling_count_new, balance_info.sibling_count);
                    }

                    #[cfg(debug_assertions)]
                    BTreeCursor::post_balance_non_root_validation(
                        &parent_page,
                        balance_info,
                        parent_contents,
                        pages_to_balance_new,
                        page_type,
                        is_table_leaf,
                        cells_debug,
                        sibling_count_new,
                        right_page_id,
                        usable_space,
                    );

                    // Balance-shallower case
                    if sibling_count_new == 0 {
                        self.stack.set_cell_index(0); // reset cell index, top is already parent
                    }

                    // Restore the cell_payloads Vec to BalanceState for reuse in future operations.
                    // This avoids allocation on subsequent balance operations.
                    let mut recovered_vec = take_vec(&mut cell_array.cell_payloads);
                    recovered_vec.clear();
                    *reusable_cell_payloads = recovered_vec;

                    *sub_state = BalanceSubState::FreePages {
                        curr_page: sibling_count_new,
                        sibling_count_new,
                    };
                }
                BalanceSubState::FreePages {
                    curr_page,
                    sibling_count_new,
                } => {
                    let sibling_count = {
                        balance_info
                            .as_ref()
                            .expect("must be balancing")
                            .sibling_count
                    };
                    // We have to free pages that are not used anymore
                    if !((*sibling_count_new..sibling_count).contains(curr_page)) {
                        *sub_state = BalanceSubState::Start;
                        let _ = balance_info.take();
                        return Ok(IOResult::Done(()));
                    } else {
                        let balance_info = balance_info.as_ref().expect("must be balancing");
                        let page = balance_info.pages_to_balance[*curr_page].as_ref().unwrap();
                        return_if_io!(self.pager.free_page(Some(page.0.clone()), page.get().id()));
                        *sub_state = BalanceSubState::FreePages {
                            curr_page: *curr_page + 1,
                            sibling_count_new: *sibling_count_new,
                        };
                    }
                }
            }
        }
    }

    /// Validates that a divider cell was correctly inserted into the parent page
    /// during B-tree balancing and that it points to the correct child page.
    #[cfg(debug_assertions)]
    fn validate_balance_non_root_divider_cell_insertion(
        balance_info: &BalanceInfo,
        parent_contents: &mut PageContent,
        divider_cell_insert_idx_in_parent: usize,
        divider_cell_is_overflow_cell: bool,
        child_page: &PageRef,
        usable_space: usize,
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
                .cell_get_raw_region(divider_cell_insert_idx_in_parent, usable_space)
                .unwrap();
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
        turso_assert_eq!(
            left_pointer,
            child_page.get().id() as u32,
            "inserted cell doesn't point to correct page",
            { "left_pointer": left_pointer, "child_page_id": child_page.get().id() }
        );
    }

    #[cfg(debug_assertions)]
    #[allow(clippy::too_many_arguments)]
    fn post_balance_non_root_validation(
        parent_page: &PageRef,
        balance_info: &BalanceInfo,
        parent_contents: &mut PageContent,
        pages_to_balance_new: &[Option<PinGuard>; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
        page_type: PageType,
        is_table_leaf: bool,
        cells_debug: &mut [crate::alloc::Vec<u8>],
        sibling_count_new: usize,
        right_page_id: u32,
        usable_space: usize,
    ) {
        let mut valid = true;
        let mut current_index_cell = 0;
        for cell_idx in 0..parent_contents.cell_count() {
            let cell = parent_contents.cell_get(cell_idx, usable_space).unwrap();
            match cell {
                BTreeCell::TableInteriorCell(table_interior_cell) => {
                    let left_child_page = table_interior_cell.left_child_page;
                    if left_child_page == parent_page.get().id() as u32 {
                        tracing::error!("balance_non_root(parent_divider_points_to_same_page, page_id={}, cell_left_child_page={})",
                                parent_page.get().id(),
                                left_child_page,
                            );
                        valid = false;
                    }
                }
                BTreeCell::IndexInteriorCell(index_interior_cell) => {
                    let left_child_page = index_interior_cell.left_child_page;
                    if left_child_page == parent_page.get().id() as u32 {
                        tracing::error!("balance_non_root(parent_divider_points_to_same_page, page_id={}, cell_left_child_page={})",
                                parent_page.get().id(),
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
            debug_validate_cells!(contents, usable_space);
            // Cells are distributed in order
            for cell_idx in 0..contents.cell_count() {
                let (cell_start, cell_len) = contents
                    .cell_get_raw_region(cell_idx, usable_space)
                    .unwrap();
                let buf = contents.as_ptr();
                let cell_buf = to_static_buf(&mut buf[cell_start..cell_start + cell_len]);
                let cell_buf_in_array = &cells_debug[current_index_cell];
                if cell_buf != cell_buf_in_array {
                    tracing::error!("balance_non_root(cell_not_found_debug, page_id={}, cell_in_cell_array_idx={})",
                        page.get().id(),
                        current_index_cell,
                    );
                    valid = false;
                }

                let cell = crate::storage::sqlite3_ondisk::read_btree_cell(
                    cell_buf,
                    contents,
                    0,
                    usable_space,
                )
                .unwrap();
                match &cell {
                    BTreeCell::TableInteriorCell(table_interior_cell) => {
                        let left_child_page = table_interior_cell.left_child_page;
                        if left_child_page == page.get().id() as u32 {
                            tracing::error!("balance_non_root(child_page_points_same_page, page_id={}, cell_left_child_page={}, page_idx={})",
                                page.get().id(),
                                left_child_page,
                                page_idx
                            );
                            valid = false;
                        }
                        if left_child_page == parent_page.get().id() as u32 {
                            tracing::error!("balance_non_root(child_page_points_parent_of_child, page_id={}, cell_left_child_page={}, page_idx={})",
                                page.get().id(),
                                left_child_page,
                                page_idx
                            );
                            valid = false;
                        }
                    }
                    BTreeCell::IndexInteriorCell(index_interior_cell) => {
                        let left_child_page = index_interior_cell.left_child_page;
                        if left_child_page == page.get().id() as u32 {
                            tracing::error!("balance_non_root(child_page_points_same_page, page_id={}, cell_left_child_page={}, page_idx={})",
                                page.get().id(),
                                left_child_page,
                                page_idx
                            );
                            valid = false;
                        }
                        if left_child_page == parent_page.get().id() as u32 {
                            tracing::error!("balance_non_root(child_page_points_parent_of_child, page_id={}, cell_left_child_page={}, page_idx={})",
                                page.get().id(),
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
                debug_validate_cells!(parent_contents, usable_space);

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

                if right_page_id == page.get().id() as u32
                    || right_page_id == parent_page.get().id() as u32
                {
                    tracing::error!("balance_non_root(balance_shallower_rightmost_pointer, page_id={}, parent_page_id={}, rightmost={})",
                        page.get().id(),
                        parent_page.get().id(),
                        right_page_id,
                    );
                    valid = false;
                }

                if let Some(rm) = contents.rightmost_pointer().ok().flatten() {
                    if rm != right_page_id {
                        tracing::error!("balance_non_root(balance_shallower_rightmost_pointer, page_rightmost={}, rightmost={})",
                            rm,
                            right_page_id,
                        );
                        valid = false;
                    }
                }

                if let Some(rm) = parent_contents.rightmost_pointer().ok().flatten() {
                    if rm != right_page_id {
                        tracing::error!("balance_non_root(balance_shallower_rightmost_pointer, parent_rightmost={}, rightmost={})",
                            rm,
                            right_page_id,
                        );
                        valid = false;
                    }
                }

                if parent_contents.page_type().ok() != Some(page_type) {
                    tracing::error!("balance_non_root(balance_shallower_parent_page_type, page_type={:?}, parent_page_type={:?})",
                        page_type,
                        parent_contents.page_type().ok()
                    );
                    valid = false
                }

                for (parent_cell_idx, cell_buf_in_array) in
                    cells_debug.iter().enumerate().take(contents.cell_count())
                {
                    let (parent_cell_start, parent_cell_len) = parent_contents
                        .cell_get_raw_region(parent_cell_idx, usable_space)
                        .unwrap();

                    let (cell_start, cell_len) = contents
                        .cell_get_raw_region(parent_cell_idx, usable_space)
                        .unwrap();

                    let buf = contents.as_ptr();
                    let cell_buf = to_static_buf(&mut buf[cell_start..cell_start + cell_len]);
                    let parent_cell_buf = to_static_buf(
                        &mut parent_buf[parent_cell_start..parent_cell_start + parent_cell_len],
                    );

                    if cell_buf != cell_buf_in_array || cell_buf != parent_cell_buf {
                        tracing::error!("balance_non_root(balance_shallower_cell_not_found_debug, page_id={}, cell_in_cell_array_idx={})",
                            page.get().id(),
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
                    && right_page_id != page.get().id() as u32
                {
                    tracing::error!("balance_non_root(cell_divider_right_pointer, should point to {}, but points to {})",
                        page.get().id(),
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
                        if left_pointer != page.get().id() as u32 {
                            tracing::error!("balance_non_root(cell_divider_left_pointer_overflow, should point to page_id={}, but points to {}, divider_cell={}, overflow_cells_parent={})",
                        page.get().id(),
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
                let (cell_start, cell_len) = parent_contents
                    .cell_get_raw_region(cell_divider_idx, usable_space)
                    .unwrap();
                let cell_left_pointer = read_u32(&parent_buf[cell_start..cell_start + cell_len], 0);
                if cell_left_pointer != page.get().id() as u32 {
                    tracing::error!("balance_non_root(cell_divider_left_pointer, should point to page_id={}, but points to {}, divider_cell={}, overflow_cells_parent={})",
                        page.get().id(),
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
                        usable_space,
                    )
                    .unwrap();
                    let parent_cell = parent_contents
                        .cell_get(cell_divider_idx, usable_space)
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
                            page.get().id(),
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
                            if left_pointer != page.get().id() as u32 {
                                tracing::error!("balance_non_root(cell_divider_divider_cell_overflow should point to page_id={}, but points to {}, divider_cell={}, overflow_cells_parent={})",
                                    page.get().id(),
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
                    let (parent_cell_start, parent_cell_len) = parent_contents
                        .cell_get_raw_region(cell_divider_idx, usable_space)
                        .unwrap();
                    let cell_buf_in_array = &cells_debug[current_index_cell];
                    let left_pointer = read_u32(
                        &parent_buf[parent_cell_start..parent_cell_start + parent_cell_len],
                        0,
                    );
                    if left_pointer != page.get().id() as u32 {
                        tracing::error!("balance_non_root(divider_cell_left_pointer_interior should point to page_id={}, but points to {}, divider_cell={}, overflow_cells_parent={})",
                                    page.get().id(),
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
                                    page.get().id(),
                                    cell_divider_idx,
                                );
                                valid = false;
                            }
                        }
                        PageType::IndexLeaf => {
                            let parent_cell_buf =
                                &parent_buf[parent_cell_start..parent_cell_start + parent_cell_len];
                            // The parent stores the cell's real size; the cell array pads leaf
                            // cells up to MINIMUM_CELL_SIZE.
                            let parent_payload = &parent_cell_buf[4..];
                            let padded = parent_payload.len() < MINIMUM_CELL_SIZE
                                && cell_buf_in_array.len() == MINIMUM_CELL_SIZE;
                            let matches = cell_buf_in_array.len() >= parent_payload.len()
                                && cell_buf_in_array[..parent_payload.len()] == *parent_payload
                                && (cell_buf_in_array.len() == parent_payload.len() || padded);
                            if !matches {
                                tracing::error!("balance_non_root(cell_divider_cell_index_leaf, page_id={}, cell_divider_idx={})",
                                    page.get().id(),
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

        // Verify all cells were accounted for (non-shallower case)
        if sibling_count_new > 0 && current_index_cell != cells_debug.len() {
            tracing::error!(
                "balance_non_root(cell_count_mismatch, current_index_cell={}, cells_debug_len={}, sibling_count_new={})",
                current_index_cell,
                cells_debug.len(),
                sibling_count_new
            );
            valid = false;
        }

        turso_assert!(
            valid,
            "corrupted database, cells were not balanced properly"
        );
    }

    /// Balance the root page.
    /// This is done when the root page overflows, and we need to create a new root page.
    /// See e.g. https://en.wikipedia.org/wiki/B-tree
    fn balance_root(&mut self) -> IOResultOr<()> {
        /* todo: balance deeper, create child and copy contents of root there. Then split root */
        /* if we are in root page then we just need to create a new root and push key there */

        // Since we are going to change the btree structure, let's forget our cached knowledge of the rightmost page.
        let _ = self.move_to_right_state.1.take();

        let root = self.stack.top();
        let root_contents = root.get_contents();
        let child = return_if_io!(self.pager.do_allocate_page(
            root_contents.page_type()?,
            0,
            BtreePageAllocMode::Any
        ));

        let is_page_1 = root.get().id() == 1;
        let offset = if is_page_1 { DatabaseHeader::SIZE } else { 0 };
        #[cfg(debug_assertions)]
        turso_assert_eq!(offset, root_contents.offset());

        tracing::debug!(
            "balance_root(root={}, rightmost={}, page_type={:?})",
            root.get().id(),
            child.get().id(),
            root_contents.page_type().ok()
        );

        turso_assert!(root.is_dirty(), "root must be marked dirty");
        turso_assert!(
            child.is_dirty(),
            "child must be marked dirty as freshly allocated page"
        );

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
        let new_root_page_type = match root_contents.page_type()? {
            PageType::IndexLeaf => PageType::IndexInterior,
            PageType::TableLeaf => PageType::TableInterior,
            other => other,
        } as u8;
        // set new page type
        root_contents.write_page_type(new_root_page_type);
        root_contents.write_rightmost_ptr(child.get().id() as u32);
        root_contents.write_cell_content_area(self.usable_space());
        root_contents.write_cell_count(0);
        root_contents.write_first_freeblock(0);

        root_contents.write_fragmented_bytes_count(0);
        root_contents.overflow_cells.clear();
        self.root_page = root.get().id() as i64;
        self.stack.clear();
        self.stack.push(root);
        self.stack.set_cell_index(0); // leave parent pointing at the rightmost pointer (in this case 0, as there are no cells), since we will be balancing the rightmost child page.
        self.stack.push(child);
        Ok(IOResult::Done(()))
    }

    #[inline(always)]
    /// Returns the usable space of the current page (which is computed as: page_size - reserved_bytes).
    /// This is cached to avoid calling `pager.usable_space()` in a hot loop.
    fn usable_space(&self) -> usize {
        self.usable_space_cached
    }

    /// Clear the overflow pages linked to a specific page provided by the leaf cell
    /// Uses a state machine to keep track of it's operations so that traversal can be
    /// resumed from last point after IO interruption
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn clear_overflow_pages(&mut self, cell: &BTreeCell) -> IOResultOr<()> {
        // `database_size` is invariant for the duration of this invocation, so
        // read the page-1 header at most once and reuse it for every overflow
        // page validation below instead of re-reading it per `ReadNext`.
        let mut database_size: Option<u32> = None;
        loop {
            match self.overflow_state.clone() {
                OverflowState::Start => {
                    let first_overflow_page = match cell {
                        BTreeCell::TableLeafCell(leaf_cell) => leaf_cell.first_overflow_page,
                        BTreeCell::IndexLeafCell(leaf_cell) => leaf_cell.first_overflow_page,
                        BTreeCell::IndexInteriorCell(interior_cell) => {
                            interior_cell.first_overflow_page
                        }
                        BTreeCell::TableInteriorCell(_) => return Ok(IOResult::Done(())), // No overflow pages
                    };

                    if let Some(next_page) = first_overflow_page {
                        let database_size =
                            return_if_io!(self.overflow_database_size(&mut database_size));
                        if unlikely(!Self::valid_overflow_page_id(next_page, database_size)) {
                            self.overflow_state = OverflowState::Start;
                            return Err(
                                LimboError::Corrupt("Invalid overflow page number".into()).into()
                            );
                        }
                        // No mutations precede this read in the Start branch,
                        // so a spill yield safely re-enters here.
                        let (page, c) = return_if_io!(self.read_page(next_page as i64));
                        self.overflow_state = OverflowState::ProcessPage { next_page: page };
                        if let Some(c) = c {
                            io_yield_one!(c);
                        }
                    } else {
                        self.overflow_state = OverflowState::Done;
                    }
                }
                OverflowState::ProcessPage { next_page: page } => {
                    turso_assert!(page.is_loaded(), "page should be loaded");

                    let contents = page.get_contents();
                    let next = contents.read_u32_no_offset(0);
                    let next_page_id = page.get().id();

                    return_if_io!(self.pager.free_page(Some(page), next_page_id));

                    // free_page returned `Done` — commit `next` to state
                    // BEFORE any fallible IO so re-entry cannot invoke
                    // `free_page` again on the now-freed page.
                    if next != 0 {
                        self.overflow_state = OverflowState::ReadNext { next };
                    } else {
                        self.overflow_state = OverflowState::Done;
                    }
                }
                OverflowState::ReadNext { next } => {
                    let database_size =
                        return_if_io!(self.overflow_database_size(&mut database_size));
                    if unlikely(!Self::valid_overflow_page_id(next, database_size)) {
                        self.overflow_state = OverflowState::Start;
                        return Err(
                            LimboError::Corrupt("Invalid overflow page number".into()).into()
                        );
                    }
                    let (page, c) = return_if_io!(self.pager.read_page(next as i64));
                    self.overflow_state = OverflowState::ProcessPage { next_page: page };
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                OverflowState::Done => {
                    self.overflow_state = OverflowState::Start;
                    return Ok(IOResult::Done(()));
                }
            };
        }
    }

    /// Read `database_size` from the page-1 header at most once per
    /// `clear_overflow_pages` invocation, memoizing it in `cache`.
    fn overflow_database_size(&self, cache: &mut Option<u32>) -> IOResultOr<u32> {
        if let Some(database_size) = cache {
            return Ok(IOResult::Done(*database_size));
        }
        let database_size =
            return_if_io!(self.pager.with_header(|header| header.database_size)).get();
        *cache = Some(database_size);
        Ok(IOResult::Done(database_size))
    }

    fn valid_overflow_page_id(page_id: u32, database_size: u32) -> bool {
        page_id >= 2 && page_id <= database_size
    }

    /// Deletes all contents of the B-tree by freeing all its pages in an iterative depth-first order.
    /// This ensures child pages are freed before their parents
    /// Uses a state machine to keep track of the operation to ensure IO doesn't cause repeated traversals
    ///
    /// Depending on the caller, the root page may either be freed as well or left allocated but emptied.
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
    fn destroy_btree_contents(&mut self, keep_root: bool) -> IOResultOr<Option<usize>> {
        if let CursorState::None = &self.state {
            let c = return_if_io!(self.move_to_root_nonblock());
            self.state = CursorState::Destroy(DestroyInfo {
                state: DestroyState::Start,
            });
            if let Some(c) = c {
                io_yield_one!(c);
            }
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
                    let _page = self.stack.top_ref();

                    let destroy_info = self
                        .state
                        .mut_destroy_info()
                        .expect("unable to get a mut reference to destroy state in cursor");
                    destroy_info.state = DestroyState::ProcessPage;
                }
                DestroyState::ProcessPage => {
                    self.stack.advance();
                    let page = self.stack.top_ref();
                    let contents = page.get_contents();
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
                                if let Some(rightmost) = contents.rightmost_pointer()? {
                                    // Spill yield here would re-enter
                                    // `ProcessPage` and re-fire the loop-top
                                    // `stack.advance()`. Route through
                                    // `PendingDescent` so re-entry resumes
                                    // there.
                                    match self.pager.read_page(rightmost as i64)? {
                                        IOResult::Done((rightmost_page, c)) => {
                                            self.stack.push(rightmost_page);
                                            let destroy_info =
                                                self.state.mut_destroy_info().expect(
                                                    "unable to get a mut reference to destroy state in cursor",
                                                );
                                            destroy_info.state = DestroyState::LoadPage;
                                            if let Some(c) = c {
                                                io_yield_one!(c);
                                            }
                                        }
                                        IOResult::IO(IOCompletions(spill_c)) => {
                                            let destroy_info =
                                                self.state.mut_destroy_info().expect(
                                                    "unable to get a mut reference to destroy state in cursor",
                                                );
                                            destroy_info.state = DestroyState::PendingDescent {
                                                target: rightmost as i64,
                                            };
                                            io_yield_one!(spill_c);
                                        }
                                    }
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
                                // Spill yield routed through `PendingDescent`
                                // — see the rightmost branch comment above.
                                match self.pager.read_page(child_page_id as i64)? {
                                    IOResult::Done((child_page, c)) => {
                                        self.stack.push(child_page);
                                        let destroy_info =
                                            self.state.mut_destroy_info().expect(
                                                "unable to get a mut reference to destroy state in cursor",
                                            );
                                        destroy_info.state = DestroyState::LoadPage;
                                        if let Some(c) = c {
                                            io_yield_one!(c);
                                        }
                                    }
                                    IOResult::IO(IOCompletions(spill_c)) => {
                                        let destroy_info =
                                            self.state.mut_destroy_info().expect(
                                                "unable to get a mut reference to destroy state in cursor",
                                            );
                                        destroy_info.state = DestroyState::PendingDescent {
                                            target: child_page_id as i64,
                                        };
                                        io_yield_one!(spill_c);
                                    }
                                }
                            }
                        },
                    }
                }
                DestroyState::ClearOverflowPages { cell } => {
                    return_if_io!(self.clear_overflow_pages(&cell));
                    match cell {
                        //  For an index interior cell, clear the left child page now that overflow pages have been cleared
                        BTreeCell::IndexInteriorCell(index_int_cell) => {
                            // `clear_overflow_pages` has returned `Done` and
                            // reset its internal state to `Start`. Re-entry
                            // into `ClearOverflowPages` would re-run it
                            // against the same (already-cleared) cell, so
                            // route a spill yield from the read through
                            // `PendingDescent` to skip back into the descent
                            // path.
                            let target = index_int_cell.left_child_page as i64;
                            match self.pager.read_page(target)? {
                                IOResult::Done((child_page, c)) => {
                                    self.stack.push(child_page);
                                    let destroy_info = self.state.mut_destroy_info().expect(
                                        "unable to get a mut reference to destroy state in cursor",
                                    );
                                    destroy_info.state = DestroyState::LoadPage;
                                    if let Some(c) = c {
                                        io_yield_one!(c);
                                    }
                                }
                                IOResult::IO(IOCompletions(spill_c)) => {
                                    let destroy_info = self.state.mut_destroy_info().expect(
                                        "unable to get a mut reference to destroy state in cursor",
                                    );
                                    destroy_info.state = DestroyState::PendingDescent { target };
                                    io_yield_one!(spill_c);
                                }
                            }
                        }
                        //  For any leaf cell, advance the index now that overflow pages have been cleared
                        BTreeCell::TableLeafCell(_) | BTreeCell::IndexLeafCell(_) => {
                            let destroy_info = self
                                .state
                                .mut_destroy_info()
                                .expect("unable to get a mut reference to destroy state in cursor");
                            destroy_info.state = DestroyState::LoadPage;
                        }
                        _ => panic!("unexpected cell type"),
                    }
                }
                DestroyState::PendingDescent { target } => {
                    let (child_page, c) = return_if_io!(self.pager.read_page(target));
                    self.stack.push(child_page);
                    let destroy_info = self
                        .state
                        .mut_destroy_info()
                        .expect("unable to get a mut reference to destroy state in cursor");
                    destroy_info.state = DestroyState::LoadPage;
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                DestroyState::FreePage => {
                    let page = self.stack.top();
                    let page_id = page.get().id();

                    if self.stack.has_parent() {
                        return_if_io!(self.pager.free_page(Some(page), page_id));

                        self.stack.pop();
                        let destroy_info = self
                            .state
                            .mut_destroy_info()
                            .expect("unable to get a mut reference to destroy state in cursor");
                        destroy_info.state = DestroyState::ProcessPage;
                    } else {
                        if keep_root {
                            self.clear_root(&page)?;
                        } else {
                            return_if_io!(self.pager.free_page(Some(page), page_id));
                        }

                        self.state = CursorState::None;
                        //  TODO: For now, no-op the result return None always. This will change once [AUTO_VACUUM](https://www.sqlite.org/lang_vacuum.html) is introduced
                        //  At that point, the last root page(call this x) will be moved into the position of the root page of this table and the value returned will be x
                        return Ok(IOResult::Done(None));
                    }
                }
            }
        }
    }

    fn clear_root(&mut self, root_page: &PageRef) -> Result<()> {
        let contents = root_page.get_contents();

        let page_type = match contents.page_type()? {
            PageType::TableLeaf | PageType::TableInterior => PageType::TableLeaf,
            PageType::IndexLeaf | PageType::IndexInterior => PageType::IndexLeaf,
        };

        self.pager.add_dirty(root_page)?;
        btree_init_page(root_page, page_type, 0, self.pager.usable_space());
        Ok(())
    }

    /// True iff the cursor sits cleanly on a row: valid, positioned, and not mid-way
    /// through some other operation's state machine.
    fn blob_position_is_live(&self) -> bool {
        self.valid_state == CursorValidState::Valid
            && self.has_record
            && matches!(self.state, CursorState::None)
            && self.stack.current_page >= 0
    }

    /// Gate for every incremental-blob entry point: ensure the cursor owns a live
    /// position on its row before any byte-level access.
    ///
    /// A peer write to the same table saves this cursor's position first
    /// (drive_pending_peer_save), and the same pass expires the handle if the write
    /// hit the pinned row (note_external_row_write) — SQLite's
    /// invalidateIncrblobCursors. An expired handle fails here with
    /// [`LimboError::BlobHandleExpired`] (SQLITE_ABORT at the C API), permanently. A
    /// position merely disturbed by a write to a *different* row is restored by
    /// re-seeking the pinned rowid, exactly like SQLite's restoreCursorPosition for
    /// incrblob cursors; if the row cannot be found again the handle expires too.
    fn blob_ensure_position(&mut self) -> IOResultOr<()> {
        if self.blob_expired {
            return Err(LimboError::BlobHandleExpired.into());
        }
        if self.blob_position_is_live() {
            return Ok(IOResult::Done(()));
        }
        if self.needs_restore() {
            return_if_io!(self.restore_context());
        }
        if !self.blob_position_is_live() {
            self.blob_expired = true;
            return Err(LimboError::BlobHandleExpired.into());
        }
        Ok(IOResult::Done(()))
    }

    /// Populate the cell-layout tier of `blob_cache` for the cursor's current
    /// table-leaf cell, unless it is already cached for this (page, cell), and pin
    /// the cell's rowid for expiry tracking. Restoring a disturbed position may
    /// yield IO; the layout parse itself reads only the resident leaf page.
    fn blob_ensure_layout(&mut self) -> IOResultOr<()> {
        return_if_io!(self.blob_ensure_position());
        let usable = self.pager.usable_space();
        turso_assert!(usable > 4, "usable space must exceed overflow header");
        let cell_idx = self.stack.current_cell_index();
        if cell_idx < 0 {
            return Err(LimboError::BlobHandleExpired.into());
        }
        let cell_idx = cell_idx as usize;
        let leaf_id = self.stack.top_ref().get().id();
        if self.blob_cache.valid
            && self.blob_cache.leaf_id == leaf_id
            && self.blob_cache.cell_idx == cell_idx
        {
            return Ok(IOResult::Done(()));
        }
        // Extract owned scalars from the cell before mutating the cache (the cell borrows
        // the page, which borrows `self`).
        let (rowid, local_off, local_len, payload_size, first_overflow) = {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            let base = contents.as_ptr().as_ptr() as usize;
            let BTreeCell::TableLeafCell(cell) = contents.cell_get(cell_idx, usable)? else {
                return Err(LimboError::InternalError(
                    "incremental blob I/O requires a table (rowid) row".to_string(),
                )
                .into());
            };
            (
                cell.rowid,
                cell.payload.as_ptr() as usize - base,
                cell.payload.len(),
                cell.payload_size as usize,
                cell.first_overflow_page,
            )
        };
        if local_len > payload_size {
            return Err(LimboError::Corrupt(format!(
                "cell claims {payload_size} payload bytes but holds {local_len} locally"
            ))
            .into());
        }
        let c = &mut self.blob_cache;
        c.valid = false;
        c.leaf_id = leaf_id;
        c.cell_idx = cell_idx;
        c.local_off = local_off;
        c.local_len = local_len;
        c.payload_size = payload_size;
        c.per = usable - 4;
        c.first_overflow = first_overflow;
        c.overflow_pages.clear();
        if let Some(fo) = first_overflow {
            crate::with_btree_allocation_site!(OverflowRead, c.overflow_pages.try_push(fo))?;
        }
        self.blob_pinned_rowid = Some(rowid);
        c.valid = true;
        // Moving to a different cell: unpin the previous cell's cached overflow page.
        c.release_pinned_overflow();
        c.col_valid = false;
        Ok(IOResult::Done(()))
    }

    /// Fetch the `idx`-th overflow page, reusing the pinned last page on a spatial-
    /// locality hit and otherwise going through the O(1) overflow cache + pager, then
    /// pinning the result for the next access.
    fn blob_overflow_page(&mut self, idx: usize) -> IOResultOr<PageRef> {
        if self.blob_cache.last_ov_idx == idx {
            if let Some(p) = &self.blob_cache.last_ov_page {
                // Pinned while cached, so the pager can't evict it and take its
                // buffer; the asserts guard that (as_ptr would panic otherwise —
                // crash over corrupt).
                turso_debug_assert!(p.is_pinned(), "cached blob overflow page must be pinned");
                turso_debug_assert!(p.is_loaded(), "pinned blob overflow page must stay loaded");
                return Ok(IOResult::Done(p.to_page()));
            }
        }
        return_if_io!(self.ensure_overflow_cached(idx));
        let pageno = *self.blob_cache.overflow_pages.get(idx).ok_or_else(|| {
            LimboError::Corrupt(format!(
                "overflow page {idx} requested but chain holds only {}",
                self.blob_cache.overflow_pages.len()
            ))
        })?;
        let (page, c) = return_if_io!(self.read_page(pageno as i64));
        if let Some(c) = c {
            io_yield_one!(c);
        }
        self.blob_cache.pin_overflow_page(idx, page.clone());
        Ok(IOResult::Done(page))
    }

    /// Populate the column tier of `blob_cache` for `column`, unless already cached.
    /// Parses the record header to find the column value's payload-relative byte
    /// offset, length, and serial type. The header usually sits in the local payload
    /// (no IO), but a wide row on a small page can spill it into the overflow chain,
    /// in which case just the header bytes are streamed in (with IO yields).
    fn blob_ensure_column(&mut self, column: usize) -> IOResultOr<()> {
        return_if_io!(self.blob_ensure_layout());
        if self.blob_cache.col_valid && self.blob_cache.col == column {
            return Ok(IOResult::Done(()));
        }
        let local_off = self.blob_cache.local_off;
        let local_len = self.blob_cache.local_len;
        let payload_size = self.blob_cache.payload_size;
        // The header-size varint itself is always local: either the record fits
        // entirely in the local payload, or SQLite's minimum-local formula keeps
        // well over 9 bytes (one max-size varint) on the leaf. A truncated varint
        // here therefore means a corrupt cell, and read_varint reports it as such.
        let (header_size, hpos0) = {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            let local = &contents.as_ptr()[local_off..local_off + local_len];
            crate::storage::sqlite3_ondisk::read_varint(local)?
        };
        let header_size = usize::try_from(header_size)
            .map_err(|_| LimboError::Corrupt("record header size does not fit".to_string()))?;
        if header_size < hpos0 || header_size > payload_size || header_size > MAX_RECORD_HEADER_SIZE
        {
            return Err(LimboError::Corrupt(format!(
                "record header size {header_size} out of bounds (payload {payload_size})"
            ))
            .into());
        }
        let (body_off, len, serial) = if header_size <= local_len {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            let local = &contents.as_ptr()[local_off..local_off + local_len];
            blob_locate_column_in_header(&local[..header_size], hpos0, payload_size, column)?
        } else {
            // Header spills into the overflow chain: materialize exactly the header
            // bytes (bounded above by MAX_RECORD_HEADER_SIZE) and parse those. On an
            // IO yield the whole function restarts; every step up to here is a pure
            // cached read, so the restart is cheap and idempotent.
            let mut header = crate::with_btree_allocation_site!(
                BlobRecordHeader,
                crate::alloc::try_vec![0u8; header_size]
            )?;
            return_if_io!(self.blob_read_range(0, &mut header));
            blob_locate_column_in_header(&header, hpos0, payload_size, column)?
        };
        let c = &mut self.blob_cache;
        c.col = column;
        c.col_body_off = body_off;
        c.col_len = len;
        c.col_serial = serial;
        c.col_valid = true;
        Ok(IOResult::Done(()))
    }

    /// Error unless the cached column value is byte-addressable, i.e. stored as TEXT
    /// or BLOB. SQLite refuses incremental I/O on NULL/integer/real values because
    /// their on-disk bytes are an encoding, not the value: "writing" into them would
    /// silently manufacture a different number. Callers must run
    /// [`Self::blob_ensure_column`] first.
    fn blob_require_text_or_blob(&self) -> Result<()> {
        turso_assert!(
            self.blob_cache.col_valid,
            "column tier must be cached before the type check"
        );
        let serial = self.blob_cache.col_serial;
        if serial >= 12 {
            return Ok(());
        }
        let type_name = match serial {
            0 => "null",
            7 => "real",
            _ => "integer",
        };
        Err(LimboError::InternalError(format!(
            "cannot open value of type {type_name}"
        )))
    }

    /// Ensure `blob_cache.overflow_pages` holds at least `upto + 1` entries, extending
    /// it by walking the chain from the last cached page. Turso's runtime `aOverflow`:
    /// the first access pays the O(n) walk, after which any offset is located in O(1).
    ///
    /// Idempotent and IO-reentrant: the array lives on the cursor and grows append-only,
    /// so on a yield the operation restarts and this resumes where it left off.
    fn ensure_overflow_cached(&mut self, upto: usize) -> IOResultOr<()> {
        loop {
            let last = {
                let pages = &self.blob_cache.overflow_pages;
                if pages.len() > upto {
                    return Ok(IOResult::Done(()));
                }
                match pages.last() {
                    Some(p) => *p,
                    None => {
                        return Err(LimboError::Corrupt(
                            "blob value needs overflow pages but has none".to_string(),
                        )
                        .into())
                    }
                }
            };
            let (page, c) = return_if_io!(self.read_page(last as i64));
            if let Some(c) = c {
                io_yield_one!(c);
            }
            let next = page.get_contents().read_u32_no_offset(0);
            if next == 0 {
                // The loop only reaches here while the chain is still shorter than the
                // requested index, so a terminated chain means the record header
                // promises more payload than the pages can hold.
                return Err(LimboError::Corrupt(format!(
                    "overflow chain ends after {} pages but the record needs at least {}",
                    self.blob_cache.overflow_pages.len(),
                    upto + 1
                ))
                .into());
            }
            crate::with_btree_allocation_site!(
                OverflowRead,
                self.blob_cache.overflow_pages.try_push(next)
            )?;
        }
    }

    /// Map payload offset `pos` (0 = first payload byte) to the physical span that
    /// begins there, yielding at most `remaining` bytes. Pure arithmetic over the
    /// cached cell layout; the caller fetches the page and copies. Requires
    /// `blob_ensure_layout` to have populated the layout tier.
    fn blob_span(&self, pos: usize, remaining: usize) -> BlobSpan {
        // Overflow spans divide by `per` (bytes per overflow page); it is `usable - 4`,
        // established > 0 by blob_ensure_layout. Assert it so the invariant is explicit.
        turso_debug_assert!(
            self.blob_cache.per > 0,
            "overflow bytes-per-page must be > 0"
        );
        let local_len = self.blob_cache.local_len;
        if pos < local_len {
            let take = (local_len - pos).min(remaining);
            BlobSpan::Local {
                page_off: self.blob_cache.local_off + pos,
                take,
            }
        } else {
            let per = self.blob_cache.per;
            let ov = pos - local_len;
            let within = ov % per;
            let take = (per - within).min(remaining);
            BlobSpan::Overflow {
                idx: ov / per,
                within,
                take,
            }
        }
    }

    /// Copy `buf.len()` bytes starting at payload offset `off` (0 = first byte of the
    /// value) into `buf`, spanning the leaf-local payload and the overflow chain. Uses
    /// the O(1) overflow cache to locate each page. Requires `blob_ensure_layout` first.
    fn blob_read_range(&mut self, off: usize, buf: &mut [u8]) -> IOResultOr<()> {
        let len = buf.len();
        let mut done = 0usize;
        while done < len {
            match self.blob_span(off + done, len - done) {
                // Local bytes are on the resident leaf page — no IO.
                BlobSpan::Local { page_off, take } => {
                    let src = self.stack.top_ref().get_contents().as_ptr();
                    buf[done..done + take].copy_from_slice(&src[page_off..page_off + take]);
                    done += take;
                }
                BlobSpan::Overflow { idx, within, take } => {
                    let page = return_if_io!(self.blob_overflow_page(idx));
                    let src = page.get_contents().as_ptr();
                    buf[done..done + take].copy_from_slice(&src[4 + within..4 + within + take]);
                    done += take;
                }
            }
        }
        Ok(IOResult::Done(()))
    }

    /// Write `data` starting at payload offset `off` in place, spanning the leaf-local
    /// payload and the overflow chain. Each mutated page is registered dirty so the
    /// write is journaled. Requires `blob_ensure_layout` first.
    fn blob_write_range(&mut self, off: usize, data: &[u8]) -> IOResultOr<()> {
        let len = data.len();
        let mut done = 0usize;
        while done < len {
            match self.blob_span(off + done, len - done) {
                BlobSpan::Local { page_off, take } => {
                    let page = self.stack.top_ref().clone();
                    self.pager.add_dirty(&page)?;
                    let dst = page.get_contents().as_ptr();
                    dst[page_off..page_off + take].copy_from_slice(&data[done..done + take]);
                    done += take;
                }
                BlobSpan::Overflow { idx, within, take } => {
                    let page = return_if_io!(self.blob_overflow_page(idx));
                    self.pager.add_dirty(&page)?;
                    let dst = page.get_contents().as_ptr();
                    dst[4 + within..4 + within + take].copy_from_slice(&data[done..done + take]);
                    done += take;
                }
            }
        }
        Ok(IOResult::Done(()))
    }

    /// Shared entry validation for every incremental-blob column access: bring the
    /// column tier of the cache up to date and confirm the value is byte-addressable
    /// (TEXT or BLOB). Reads/writes/length all funnel through here so the type rule
    /// is enforced in exactly one place.
    fn blob_typed_column(&mut self, column: usize) -> IOResultOr<()> {
        return_if_io!(self.blob_ensure_column(column));
        self.blob_require_text_or_blob()?;
        Ok(IOResult::Done(()))
    }

    /// Validate the byte range `[off, off+len)` against column `column`'s value and
    /// return its payload-relative start offset (for `blob_read_range` /
    /// `blob_write_range`). One bounds check backs both read and write.
    fn blob_resolve_range(&mut self, column: usize, off: usize, len: usize) -> IOResultOr<usize> {
        return_if_io!(self.blob_typed_column(column));
        if off.saturating_add(len) > self.blob_cache.col_len {
            return Err(LimboError::InternalError(
                "blob access past end of column value".to_string(),
            )
            .into());
        }
        Ok(IOResult::Done(self.blob_cache.col_body_off + off))
    }

    /// Total byte length of column `column`'s value in the current row, validating
    /// that the value is byte-addressable (TEXT or BLOB). This backs
    /// `sqlite3_blob_open`, so the type error surfaces at open time.
    pub fn blob_column_len_inherent(&mut self, column: usize) -> IOResultOr<usize> {
        return_if_io!(self.blob_typed_column(column));
        Ok(IOResult::Done(self.blob_cache.col_len))
    }

    /// Read `len` bytes at `off` within column `column`'s value into `out`.
    ///
    /// Reentrant: on an IO yield it restarts from the beginning. Every step is a pure
    /// read and therefore idempotent, so no per-call state is saved — a restart simply
    /// re-reads (now-cached) pages and reproduces the same output.
    pub fn blob_read_column_inherent(
        &mut self,
        column: usize,
        off: usize,
        len: usize,
        out: &mut crate::ValueBlob,
    ) -> IOResultOr<()> {
        let payload_off = return_if_io!(self.blob_resolve_range(column, off, len));
        crate::with_btree_allocation_site!(
            OverflowRead,
            out.try_reserve(len.saturating_sub(out.len()))
        )?;
        out.clear();
        if len == 0 {
            return Ok(IOResult::Done(()));
        }
        out.resize(len, 0);
        return_if_io!(self.blob_read_range(payload_off, out));
        Ok(IOResult::Done(()))
    }

    /// Write `data` at `off` within column `column`'s value, in place across the
    /// local page and overflow chain; the value's size cannot change. Each mutated
    /// page is registered dirty so the write is journaled and persisted.
    ///
    /// Reentrant: on an IO yield it restarts from the beginning. Every write is
    /// idempotent (the same bytes to the same offsets), so a restart re-applies
    /// already-written bytes harmlessly.
    pub fn blob_write_column_inherent(
        &mut self,
        column: usize,
        off: usize,
        data: &[u8],
    ) -> IOResultOr<()> {
        let payload_off = return_if_io!(self.blob_resolve_range(column, off, data.len()));
        if data.is_empty() {
            return Ok(IOResult::Done(()));
        }
        return_if_io!(self.blob_write_range(payload_off, data));
        Ok(IOResult::Done(()))
    }

    pub fn overwrite_cell(
        &mut self,
        page: &PageRef,
        cell_idx: usize,
        record: &ImmutableRecordRef<'_>,
        state: &mut OverwriteCellState,
    ) -> IOResultOr<()> {
        loop {
            turso_assert!(page.is_loaded(), "page is not loaded", { "page_id": page.get().id() });
            match state {
                OverwriteCellState::AllocatePayload => {
                    let serial_types_len = record.column_count();
                    // Reuse the cell payload buffer to avoid allocations
                    let mut new_payload = take_vec(&mut self.reusable_cell_payload);
                    new_payload.clear();
                    if new_payload.capacity() < serial_types_len {
                        crate::with_btree_allocation_site!(
                            CellPayload,
                            new_payload.try_reserve(serial_types_len - new_payload.capacity())
                        )?;
                    }
                    let rowid = return_if_io!(self.rowid());
                    *state = OverwriteCellState::FillPayload {
                        new_payload,
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
                    {
                        return_if_io!(fill_cell_payload(
                            &PinGuard::new(page.clone()),
                            *rowid,
                            new_payload,
                            cell_idx,
                            record,
                            self.usable_space(),
                            &self.pager,
                            fill_cell_payload_state,
                        ));
                    }
                    // figure out old cell offset & size
                    let (old_offset, old_local_size) = {
                        let contents = page.get_contents();
                        contents.cell_get_raw_region(cell_idx, self.usable_space())?
                    };

                    *state = OverwriteCellState::ClearOverflowPagesAndOverwrite {
                        new_payload: take_vec(new_payload),
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
                    let contents = page.get_contents();
                    let cell = contents.cell_get(cell_idx, self.usable_space())?;
                    return_if_io!(self.clear_overflow_pages(&cell));

                    // if it all fits in local space and old_local_size is enough, do an in-place overwrite
                    if new_payload.len() == *old_local_size {
                        Self::overwrite_content(page, *old_offset, new_payload)?;
                        // Recover the reusable buffer
                        self.reusable_cell_payload = take_vec(new_payload);
                        return Ok(IOResult::Done(()));
                    }

                    drop_cell(contents, cell_idx, self.usable_space())?;
                    insert_into_cell(contents, new_payload, cell_idx, self.usable_space())?;
                    // Recover the reusable buffer
                    self.reusable_cell_payload = take_vec(new_payload);
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    pub fn overwrite_content(page: &PageRef, dest_offset: usize, new_payload: &[u8]) -> Result<()> {
        turso_assert!(page.is_loaded(), "page should be loaded");
        let buf = page.get_contents().as_ptr();
        buf[dest_offset..dest_offset + new_payload.len()].copy_from_slice(new_payload);
        Ok(())
    }

    #[inline]
    fn get_immutable_record_or_create(&mut self) -> Result<Option<&mut ImmutableRecord>> {
        if self.reusable_immutable_record.is_none() {
            self.allocate_reusable_record()?;
        }
        Ok(self.reusable_immutable_record.as_mut())
    }

    #[cold]
    #[inline(never)]
    fn allocate_reusable_record(&mut self) -> Result<()> {
        let record = match self.pager.take_record_buf() {
            Some(buf) => ImmutableRecord::from_buf(buf),
            None => {
                let page_size = self.pager.get_page_size_unchecked().get();
                crate::with_btree_allocation_site!(
                    RecordPayload,
                    ImmutableRecord::new(page_size as usize)
                )?
            }
        };
        self.reusable_immutable_record.replace(record);
        Ok(())
    }

    fn get_immutable_record(&self) -> Option<&ImmutableRecord> {
        self.reusable_immutable_record.as_ref()
    }

    pub fn is_write_in_progress(&self) -> bool {
        matches!(self.state, CursorState::Write(_))
    }

    /// True iff the cursor sits on a valid record that is NOT the first cell of
    /// its page. Used by the MVCC checkpoint's sequential-write optimization:
    /// only at such a position is "insert the next adjacent rowid here, without
    /// re-seeking" provably within the page's divider bounds (the previous,
    /// strictly smaller rowid is in this same page, and so is a strictly larger
    /// one). At cell 0 the cursor has just crossed a leaf boundary, and the
    /// adjacent rowid may belong on the LEFT side of the parent divider — a
    /// divider whose row was deleted keeps its key, so the divider can
    /// be >= the rowid being inserted — in which case the caller must re-seek
    /// from the root.
    pub fn is_positioned_past_page_start(&self) -> bool {
        self.has_record() && self.stack.current_cell_index() > 0
    }

    /// True iff the cursor has run off the last cell of the table's rightmost
    /// leaf: `next()` left it on that leaf with the cell index one past the
    /// last cell and no record, and no ancestor has a child to the right.
    ///
    /// That is the append slot. A key larger than every key in the table
    /// belongs there, and there is no right-hand divider that could put it
    /// anywhere else, so the MVCC checkpoint's sequential-write optimization
    /// can insert here without re-seeking from the root. Without this check
    /// every append pays a full root-to-leaf seek: `next()` after inserting
    /// the last cell always runs off the end.
    pub fn is_at_end_of_rightmost_leaf(&self) -> bool {
        if self.has_record()
            || self.valid_state != CursorValidState::Valid
            || self.stack.current_page < 0
        {
            return false;
        }
        let contents = self.stack.top_ref().get_contents();
        contents.is_leaf()
            && self.stack.current_cell_index() == contents.cell_count() as i32
            && !self.ancestor_pages_have_more_children()
    }

    /// Rowid of the table-leaf cell the cursor currently sits on, or `None` when the
    /// cursor is not cleanly positioned on a table leaf (index cursors, sentinel
    /// stacks, mid-operation states). Callers that use `None` must treat it as
    /// "unknown row" and act conservatively.
    fn current_table_leaf_rowid(&self) -> Option<i64> {
        if self.valid_state != CursorValidState::Valid || !self.has_record {
            return None;
        }
        if self.stack.current_page < 0
            || (self.stack.current_page as usize) >= self.stack.stack.len()
            || self.stack.stack[self.stack.current_page as usize].is_none()
        {
            return None;
        }
        let cell_idx = self.stack.current_cell_index();
        if cell_idx < 0 {
            return None;
        }
        let page = self.stack.top_ref();
        let contents = page.get_contents();
        if !matches!(contents.page_type(), Ok(PageType::TableLeaf)) {
            return None;
        }
        if cell_idx as usize >= contents.cell_count() {
            return None;
        }
        contents.cell_table_leaf_read_rowid(cell_idx as usize).ok()
    }

    /// saveAllCursors pass for this cursor's insert/delete entry. Iteration
    /// state lives in `pending_peer_save` so we can resume across IO yields
    /// from per-peer overflow-chain reads. `written_rowid` is the row about to be
    /// written (`None` when unknown); peers backing incremental blob handles use it
    /// to expire exactly when their own row is hit (sqlite3's
    /// invalidateIncrblobCursors, btree.c:672).
    fn drive_pending_peer_save(&mut self, written_rowid: Option<i64>) -> IOResultOr<()> {
        if self.pending_peer_save.is_none() && matches!(self.state, CursorState::None) {
            // BTCF_Multiple fast path (sqlite3 btree.c:9348).
            if !self.has_peers.load(crate::sync::atomic::Ordering::Relaxed) {
                return Ok(IOResult::Done(()));
            }
            let dyn_ref: &dyn CursorTrait = self;
            let peers = self.pager.snapshot_peers_for_root(dyn_ref);
            if peers.is_empty() {
                return Ok(IOResult::Done(()));
            }
            self.pending_peer_save = Some((peers, 0));
        }
        let Some((peers, idx)) = self.pending_peer_save.as_mut() else {
            return Ok(IOResult::Done(()));
        };
        while *idx < peers.len() {
            let peer = peers[*idx];
            // Idempotent, so safe to re-run for the same peer after an IO yield
            // below. SAFETY: see RegisteredCursor's invariant.
            unsafe { peer.as_mut().note_external_row_write(written_rowid) };
            // SAFETY: see RegisteredCursor's invariant.
            let outcome =
                unsafe { return_if_io!(peer.as_mut().try_save_position_for_external_balance()) };
            if outcome == SavePositionResult::MustInvalidate {
                // SAFETY: see RegisteredCursor's invariant.
                unsafe { peer.as_mut().invalidate_btree_cache() };
            }
            *idx += 1;
        }
        self.pending_peer_save = None;
        Ok(IOResult::Done(()))
    }

    // Save cursor context, to be restored later
    pub fn save_context(&mut self, cursor_context: CursorContext) {
        self.valid_state = CursorValidState::RequireSeek;
        self.context = Some(cursor_context);
        self.noted_payload = NotedPayload::NONE;
        // The tree is about to change under this cursor (that is the only reason a
        // position ever gets saved), so cached payload offsets and overflow page
        // numbers must not survive: blob I/O through them would touch relocated or
        // freed pages. The next blob access re-seeks via restore_context and
        // re-parses the layout from scratch (see blob_ensure_position).
        self.blob_cache.reset();
    }

    /// Drop any pending saved seek-context; used by callers that re-navigate
    /// from the root and don't want restore_context to clobber them.
    #[inline]
    fn clear_saved_seek(&mut self) {
        self.context = None;
        self.valid_state = CursorValidState::Valid;
    }

    #[inline]
    fn needs_restore(&self) -> bool {
        self.context.is_some() && !matches!(self.valid_state, CursorValidState::Valid)
    }

    /// If context is defined, restore it and set it None on success. Parallels
    /// SQLite's btreeRestoreCursorPosition (btree.c:896). NotFound stays at
    /// Valid with has_record=false rather than transitioning to Invalid: a
    /// peer-deleted cursor is still recoverable via Rewind/Seek, whereas our
    /// Invalid is reserved for cursors that can never be repositioned.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn restore_context(&mut self) -> IOResultOr<()> {
        if !self.needs_restore() {
            return Ok(IOResult::Done(()));
        }
        if let CursorValidState::RequireAdvance(direction) = self.valid_state {
            return_if_io!(match direction {
                // Avoid calling next()/prev() directly because they immediately call restore_context()
                IterationDirection::Forwards => self.get_next_record(),
                IterationDirection::Backwards => self.get_prev_record(),
            });
            self.context = None;
            self.valid_state = CursorValidState::Valid;
            return Ok(IOResult::Done(()));
        }
        let ctx = self.context.take().unwrap();
        let seek_key = match ctx.key {
            CursorContextKey::TableRowId(rowid) => SeekKey::TableRowId(rowid),
            CursorContextKey::IndexKeyRowId(ref record) => SeekKey::IndexKey(record.reborrow()),
        };
        let res = self.seek(seek_key, ctx.seek_op)?;
        match res {
            IOResult::Done(res) => {
                match res {
                    SeekResult::Found => {
                        self.valid_state = CursorValidState::Valid;
                        Ok(IOResult::Done(()))
                    }
                    SeekResult::TryAdvance => {
                        self.valid_state =
                            CursorValidState::RequireAdvance(ctx.seek_op.iteration_direction());
                        self.context = Some(ctx);
                        io_yield_one!(Completion::new_yield());
                    }
                    SeekResult::NotFound => {
                        // Saved row is gone (deleted by us, or by a peer).
                        // The seek positioned the stack at the next-greater
                        // cell, which is the correct iteration target for
                        // forward iteration after the deletion. Signal that
                        // via skip_advance so the next next() returns the
                        // landed cell instead of advancing past it — mirrors
                        // SQLite's CURSOR_SKIPNEXT (btree.c:915).
                        self.skip_advance = true;
                        self.valid_state = CursorValidState::Valid;
                        Ok(IOResult::Done(()))
                    }
                }
            }
            IOResult::IO(io) => {
                self.context = Some(ctx);
                Ok(IOResult::IO(io))
            }
        }
    }

    pub fn read_page_blocking(&self, page_idx: i64) -> Result<(PageRef, Option<Completion>)> {
        self.pager.io.block(|| self.pager.read_page(page_idx))
    }

    pub fn read_page(&self, page_idx: i64) -> IOResultOr<(PageRef, Option<Completion>)> {
        self.pager.read_page(page_idx)
    }

    pub fn allocate_page(&self, page_type: PageType, offset: usize) -> IOResultOr<PageRef> {
        self.pager
            .do_allocate_page(page_type, offset, BtreePageAllocMode::Any)
    }
}

#[cfg(any(test, injected_yields))]
impl ProvidesYieldContext for BTreeCursor {
    fn yield_context(&self) -> YieldContext {
        YieldContext::new(
            self.yield_injector.clone(),
            None,
            self.yield_instance_id,
            BTREE_WRITE_YIELD_FAMILY ^ self.root_page as u64,
        )
    }
}

impl BTreeCursor {
    fn clear_transient_overflow_cells(&mut self) {
        // Overflow cells are page-local scratch for the cursor's in-flight balance.
        // If the cursor is abandoned after queueing them, cached pages may outlive
        // the cursor and must not carry that scratch into later writes.
        if matches!(self.state, CursorState::None)
            && matches!(self.balance_state.sub_state, BalanceSubState::Start)
        {
            turso_assert!(
                self.balance_state.balance_info.is_none(),
                "idle cursor has balance info"
            );
            // No write or balance operation is in progress, so this cursor has no
            // transient overflow cells to clean up.
            return;
        }

        for page in self.stack.stack.iter().flatten() {
            page.get().overflow_cells.clear();
        }

        // Insert/overwrite can stage overflow cells before balance_info is populated.
        // If the cursor is dropped in that window, this page handle is the only owner
        // of that transient state.
        match &self.state {
            CursorState::Write(WriteState::Insert { page, .. })
            | CursorState::Write(WriteState::Overwrite { page, .. }) => {
                page.get().overflow_cells.clear();
            }
            CursorState::Write(WriteState::Start)
            | CursorState::Write(WriteState::Balancing)
            | CursorState::Write(WriteState::Finish)
            | CursorState::Destroy(_)
            | CursorState::Delete(_)
            | CursorState::None => {}
        }

        if let Some(balance_info) = &self.balance_state.balance_info {
            for page in balance_info.pages_to_balance.iter().flatten() {
                page.get().overflow_cells.clear();
            }
        }

        // Newly allocated/reused sibling pages are tracked only by BalanceContext until
        // non-root balancing finishes. If the cursor is dropped before then, clear any
        // overflow scratch from those pages explicitly.
        match &self.balance_state.sub_state {
            BalanceSubState::NonRootDoBalancingAllocate {
                context: Some(context),
                ..
            }
            | BalanceSubState::NonRootDoBalancingFinish { context } => {
                for page in context.pages_to_balance_new.iter().flatten() {
                    page.get().overflow_cells.clear();
                }
            }
            BalanceSubState::Start
            | BalanceSubState::BalanceRoot
            | BalanceSubState::Decide
            | BalanceSubState::Quick
            | BalanceSubState::NonRootPickSiblings
            | BalanceSubState::NonRootDoBalancing
            | BalanceSubState::NonRootDoBalancingAllocate { context: None, .. }
            | BalanceSubState::FreePages { .. } => {}
        }
    }
}

impl Drop for BTreeCursor {
    fn drop(&mut self) {
        self.clear_transient_overflow_cells();
        if let Some(record) = self.reusable_immutable_record.take() {
            self.pager.recycle_record_buf(record.retire());
        }
        if !self
            .did_register
            .load(crate::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        let dyn_ref: &dyn CursorTrait = self;
        self.pager.unregister_cursor(dyn_ref);
    }
}

impl CursorTrait for BTreeCursor {
    fn blob_read_column(
        &mut self,
        column: usize,
        off: usize,
        len: usize,
        out: &mut crate::ValueBlob,
    ) -> IOResultOr<()> {
        self.blob_read_column_inherent(column, off, len, out)
    }
    fn blob_write_column(&mut self, column: usize, off: usize, data: &[u8]) -> IOResultOr<()> {
        self.blob_write_column_inherent(column, off, data)
    }
    fn blob_column_len(&mut self, column: usize) -> IOResultOr<usize> {
        self.blob_column_len_inherent(column)
    }
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn next(&mut self) -> IOResultOr<()> {
        if self.can_advance_within_leaf() {
            self.stack.advance();
            self.invalidate_record();
            return Ok(IOResult::Done(()));
        }
        if self.is_on_last_cell_of_tree() {
            self.stack.advance();
            self.invalidate_record();
            self.set_has_record(false);
            return Ok(IOResult::Done(()));
        }
        if self.valid_state == CursorValidState::Invalid {
            return Ok(IOResult::Done(()));
        }
        loop {
            match self.advance_state {
                AdvanceState::Start => {
                    return_if_io!(self.restore_context());
                    // Set by DeleteState::RestoreContextAfterBalancing and by
                    // restore_context on NotFound: the cursor is already at
                    // the right iteration target, so return it without
                    // advancing. If the landed cell has no record (past
                    // EOF), fall through to Advance.
                    if self.skip_advance {
                        self.skip_advance = false;
                        if self.stack.current_page >= 0 {
                            let mem_page = self.stack.top_ref();
                            let contents = mem_page.get_contents();
                            let cell_idx = self.stack.current_cell_index();
                            let cell_count = contents.cell_count();
                            let has_record = cell_idx >= 0 && cell_idx < cell_count as i32;
                            if has_record {
                                self.set_has_record(true);
                                self.read_overflow_state = None;
                                return Ok(IOResult::Done(()));
                            }
                        }
                    }
                    self.advance_state = AdvanceState::Advance;
                }
                AdvanceState::Advance => {
                    return_if_io!(self.get_next_record());
                    self.advance_state = AdvanceState::Start;
                    self.read_overflow_state = None;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    #[inline(always)]
    fn next_row(&mut self) -> CursorStep {
        if self.null_flag {
            self.null_flag = false;
            return CursorStep::Empty;
        }
        if self.can_advance_within_leaf() {
            self.stack.advance();
            self.invalidate_record();
            return CursorStep::Row;
        }
        match self.next() {
            Ok(IOResult::IO(io)) => CursorStep::IO(io),
            Err(err) => CursorStep::Error(err),
            Ok(IOResult::Done(())) => CursorStep::at_row(self.has_record),
        }
    }

    fn prev_row(&mut self) -> CursorStep {
        if self.null_flag {
            self.null_flag = false;
            return CursorStep::Empty;
        }
        match self.prev() {
            Ok(IOResult::IO(io)) => CursorStep::IO(io),
            Err(err) => CursorStep::Error(err),
            Ok(IOResult::Done(())) => CursorStep::at_row(self.has_record),
        }
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn last(&mut self) -> IOResultOr<()> {
        self.set_null_flag(false);
        if self.valid_state == CursorValidState::Invalid {
            return Ok(IOResult::Done(()));
        }
        self.clear_saved_seek();
        let cursor_has_record = return_if_io!(self.move_to_rightmost());
        self.set_has_record(cursor_has_record);
        self.invalidate_record();
        self.read_overflow_state = None;
        Ok(IOResult::Done(()))
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn prev(&mut self) -> IOResultOr<()> {
        loop {
            match self.advance_state {
                AdvanceState::Start => {
                    return_if_io!(self.restore_context());
                    self.advance_state = AdvanceState::Advance;
                }
                AdvanceState::Advance => {
                    return_if_io!(self.get_prev_record());
                    self.advance_state = AdvanceState::Start;
                    self.read_overflow_state = None;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    #[inline(always)]
    fn rowid(&mut self) -> IOResultOr<Option<i64>> {
        if self.needs_restore() {
            return rowid_general(self);
        }
        if self.get_null_flag() {
            return Ok(IOResult::Done(None));
        }
        return if self.has_record() {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            if contents.is_table() {
                let cell_idx = self.stack.current_cell_index();
                let cell = contents.cell_table_leaf_read_header(cell_idx as usize)?;
                self.noted_payload = NotedPayload {
                    start: cell.payload_start as u32,
                    size: u32::try_from(cell.payload_size).unwrap_or(0),
                };
                Ok(IOResult::Done(Some(cell.rowid)))
            } else {
                index_rowid(self)
            }
        } else {
            Ok(IOResult::Done(None))
        };

        #[inline(never)]
        fn rowid_general(cursor: &mut BTreeCursor) -> IOResultOr<Option<i64>> {
            return_if_io!(cursor.restore_context());
            cursor.rowid()
        }

        #[inline(never)]
        fn index_rowid(cursor: &mut BTreeCursor) -> IOResultOr<Option<i64>> {
            let _ = return_if_io!(cursor.record());
            Ok(IOResult::Done(cursor.get_index_rowid_from_record()))
        }
    }

    #[cfg_attr(debug_assertions, instrument(skip(self, key), level = Level::DEBUG))]
    fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> IOResultOr<SeekResult> {
        self.skip_advance = false;
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
        self.read_overflow_state = None;
        Ok(IOResult::Done(seek_result))
    }

    #[cfg_attr(debug_assertions, instrument(skip(self, registers), level = Level::DEBUG))]
    fn seek_unpacked(&mut self, registers: &[Register], op: SeekOp) -> IOResultOr<SeekResult> {
        self.skip_advance = false;
        // Empty trace to capture the span information
        tracing::trace!("");
        // We need to clear the null flag for the table cursor before seeking,
        // because it might have been set to false by an unmatched left-join row during the previous iteration
        // on the outer loop.
        self.set_null_flag(false);
        let seek_result = return_if_io!(self.do_seek_unpacked(registers, op));
        self.invalidate_record();
        // Reset seek state
        self.seek_state = CursorSeekState::Start;
        self.valid_state = CursorValidState::Valid;
        self.read_overflow_state = None;
        Ok(IOResult::Done(seek_result))
    }

    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn record(&mut self) -> IOResultOr<Option<&ImmutableRecord>> {
        // Mirrors sqlite3BtreeRestoreCursorPosition called at btree read
        // entry points (btree.c:5315, etc).
        if self.needs_restore() {
            return_if_io!(self.restore_context());
        }
        if !self.has_record() {
            return Ok(IOResult::Done(None));
        }
        let invalidated = self
            .reusable_immutable_record
            .as_ref()
            .is_none_or(|record| record.is_invalidated());
        if !invalidated {
            return Ok(IOResult::Done(self.reusable_immutable_record.as_ref()));
        }

        let page = self.stack.top_ref();
        let contents = page.get_contents();
        let cell_idx = self.stack.current_cell_index();
        let (payload, payload_size, first_overflow_page) =
            contents.cell_read_payload_ptr(cell_idx as usize, self.payload_limits)?;
        if let Some(next_page) = first_overflow_page {
            return_if_io!(self.process_overflow_read(payload, next_page, payload_size))
        } else {
            let record = self
                .get_immutable_record_or_create()?
                .expect("record was allocated above");
            record.invalidate();
            crate::with_btree_allocation_site!(RecordPayload, record.start_serialization(payload))?;
        };

        Ok(IOResult::Done(self.reusable_immutable_record.as_ref()))
    }

    #[inline(always)]
    fn record_payload(&mut self) -> IOResultOr<Option<&[u8]>> {
        if self.needs_restore() {
            return restore_record_payload(self);
        }
        if self.null_flag || !self.has_record() {
            return Ok(IOResult::Done(None));
        }
        let noted = self.noted_payload;
        if noted.size != 0 {
            let size = noted.size as usize;
            // A cell that keeps its whole payload on the page: the rowid
            // read already found where it starts.
            if size <= self.payload_limits.max_local_table {
                let start = noted.start as usize;
                let contents = self.stack.top_ref().get_contents();
                if let Some(payload) = contents.payload_on_page(start, size) {
                    return Ok(IOResult::Done(Some(payload)));
                }
            }
        }
        let contents = self.stack.top_ref().get_contents();
        let cell_idx = self.stack.current_cell_index();
        // Optimistically use a faster decoder that only handles leaf cells without overflow pages.
        // If this fails, we'll degrade to the slower path.
        if let Some((payload, start)) =
            contents.decode_leaf_cell_without_overflow(cell_idx as usize, &self.payload_limits)
        {
            self.noted_payload = NotedPayload {
                start: start as u32,
                size: payload.len() as u32,
            };
            return Ok(IOResult::Done(Some(payload)));
        }
        return self.record_payload_general();

        #[inline(never)]
        fn restore_record_payload(cursor: &mut BTreeCursor) -> IOResultOr<Option<&[u8]>> {
            return_if_io!(cursor.restore_context());
            cursor.record_payload()
        }
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn insert(&mut self, key: &BTreeKey) -> IOResultOr<()> {
        tracing::debug!(valid_state = ?self.valid_state, cursor_state = ?self.state, is_write_in_progress = self.is_write_in_progress());
        self.noted_payload = NotedPayload::NONE;
        // saveAllCursors at the head of sqlite3BtreeInsert (btree.c:9348).
        return_if_io!(self.drive_pending_peer_save(key.maybe_rowid()));
        return_if_io!(self.insert_into_page(key));
        self.invalidate_count_cache();
        if key.maybe_rowid().is_some() {
            self.set_has_record(true);
        }
        Ok(IOResult::Done(()))
    }

    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    /// Delete state machine flow:
    /// 1. Start -> check if the rowid to be delete is present in the page or not. If not we early return
    /// 2. DeterminePostBalancingSeekKey -> determine the key to seek to after balancing.
    /// 3. LoadPage -> load the page.
    /// 4. FindCell -> find the cell to be deleted in the page.
    /// 5. ClearOverflowPages -> Clear the overflow pages if there are any before dropping the cell, then if we are in a leaf page we just drop the cell in place.
    /// if we are in interior page, we need to rotate keys in order to replace current cell (InteriorNodeReplacement).
    /// 6. InteriorNodeReplacement -> we copy the left subtree leaf node into the deleted interior node's place.
    /// 7. Balancing -> perform balancing
    /// 8. PostInteriorNodeReplacement -> if an interior node was replaced, we need to advance the cursor once.
    /// 9. SeekAfterBalancing -> adjust the cursor to a node that is closer to the deleted value. go to Finish
    /// 10. Finish -> Delete operation is done. Return CursorResult(Ok())
    fn delete(&mut self) -> IOResultOr<()> {
        self.noted_payload = NotedPayload::NONE;
        if let CursorState::None = &self.state {
            // saveAllCursors at the head of sqlite3BtreeDelete (btree.c:9841). The
            // cursor is positioned on the row being deleted, so its rowid tells peer
            // blob cursors whether the deletion hits their pinned row.
            let deleted_rowid = self.current_table_leaf_rowid();
            return_if_io!(self.drive_pending_peer_save(deleted_rowid));
            self.invalidate_count_cache();
            self.state = CursorState::Delete(DeleteState::Start);
        }

        loop {
            let usable_space = self.usable_space();
            let delete_state = match &mut self.state {
                CursorState::Delete(x) => x,
                _ => unreachable!("expected delete state"),
            };
            tracing::debug!(?delete_state);

            match delete_state {
                DeleteState::Start => {
                    let page = self.stack.top_ref();
                    self.pager.add_dirty(page)?;
                    if matches!(
                        page.get_contents().page_type()?,
                        PageType::TableLeaf | PageType::TableInterior
                    ) {
                        if return_if_io!(self.rowid()).is_none() {
                            self.state = CursorState::None;
                            return Ok(IOResult::Done(()));
                        }
                    } else if !self.has_record() {
                        self.state = CursorState::None;
                        return Ok(IOResult::Done(()));
                    }

                    self.state = CursorState::Delete(DeleteState::DeterminePostBalancingSeekKey);
                }

                DeleteState::DeterminePostBalancingSeekKey => {
                    // FIXME: skip this work if we determine deletion wont result in balancing
                    // Right now we calculate the key every time for simplicity/debugging
                    // since it won't affect correctness which is more important
                    let page = self.stack.top_ref();
                    let target_key = if page.is_index()? {
                        let record = match return_if_io!(self.record()) {
                            Some(record) => record.clone(),
                            None => unreachable!("there should've been a record"),
                        };
                        CursorContext {
                            key: CursorContextKey::IndexKeyRowId(
                                ImmutableRecordRef::from_owned_record(record),
                            ),
                            seek_op: SeekOp::GE { eq_only: true },
                        }
                    } else {
                        let Some(rowid) = return_if_io!(self.rowid()) else {
                            panic!("cursor should be pointing to a record with a rowid");
                        };
                        CursorContext {
                            key: CursorContextKey::TableRowId(rowid),
                            seek_op: SeekOp::GE { eq_only: true },
                        }
                    };

                    self.state = CursorState::Delete(DeleteState::LoadPage {
                        post_balancing_seek_key: Some(target_key),
                    });
                }

                DeleteState::LoadPage {
                    post_balancing_seek_key,
                } => {
                    self.state = CursorState::Delete(DeleteState::FindCell {
                        post_balancing_seek_key: post_balancing_seek_key.take(),
                    });
                }

                DeleteState::FindCell {
                    post_balancing_seek_key,
                } => {
                    let page = self.stack.top_ref();
                    let cell_idx = self.stack.current_cell_index() as usize;
                    let contents = page.get_contents();
                    if unlikely(cell_idx >= contents.cell_count()) {
                        return_corrupt!(
                            "Corrupted page: cell index {} is out of bounds for page with {} cells",
                            cell_idx,
                            contents.cell_count()
                        );
                    }

                    tracing::debug!(
                        "DeleteState::FindCell: page_id: {}, cell_idx: {}",
                        page.get().id(),
                        cell_idx
                    );

                    let cell = contents.cell_get(cell_idx, usable_space)?;

                    let original_child_pointer = match &cell {
                        BTreeCell::TableInteriorCell(interior) => Some(interior.left_child_page),
                        BTreeCell::IndexInteriorCell(interior) => Some(interior.left_child_page),
                        _ => None,
                    };

                    self.state = CursorState::Delete(DeleteState::ClearOverflowPages {
                        cell_idx,
                        cell,
                        original_child_pointer,
                        post_balancing_seek_key: post_balancing_seek_key.take(),
                    });
                }

                DeleteState::ClearOverflowPages { cell, .. } => {
                    let cell = cell.clone();
                    return_if_io!(self.clear_overflow_pages(&cell));

                    let CursorState::Delete(DeleteState::ClearOverflowPages {
                        cell_idx,
                        original_child_pointer,
                        ref mut post_balancing_seek_key,
                        ..
                    }) = self.state
                    else {
                        unreachable!("expected clear overflow pages state");
                    };

                    let page = self.stack.top_ref();
                    let contents = page.get_contents();

                    if !contents.is_leaf() {
                        self.state = CursorState::Delete(DeleteState::InteriorNodeReplacement {
                            page: page.clone(),
                            btree_depth: self.stack.current(),
                            cell_idx,
                            original_child_pointer,
                            post_balancing_seek_key: post_balancing_seek_key.take(),
                        });
                    } else {
                        drop_cell(contents, cell_idx, usable_space)?;

                        self.state = CursorState::Delete(DeleteState::CheckNeedsBalancing {
                            btree_depth: self.stack.current(),
                            post_balancing_seek_key: post_balancing_seek_key.take(),
                            interior_node_was_replaced: false,
                        });
                    }
                }

                DeleteState::InteriorNodeReplacement { .. } => {
                    // This is an interior node, we need to handle deletion differently.
                    // 1. Move cursor to the largest key in the left subtree.
                    // 2. Replace the cell in the interior (parent) node with that key.
                    // 3. Delete that key from the child page.

                    // Step 1: Move cursor to the largest key in the left subtree.
                    // The largest key is always in a leaf, and so this traversal may involvegoing multiple pages downwards,
                    // so we store the page we are currently on.

                    // avoid calling prev() because it internally calls restore_context() which may cause unintended behavior.
                    return_if_io!(self.get_prev_record());

                    let CursorState::Delete(DeleteState::InteriorNodeReplacement {
                        ref page,
                        btree_depth,
                        cell_idx,
                        original_child_pointer,
                        ref mut post_balancing_seek_key,
                        ..
                    }) = self.state
                    else {
                        unreachable!("expected interior node replacement state");
                    };

                    // Ensure we keep the parent page at the same position as before the replacement.
                    self.stack
                        .node_states
                        .get_mut(btree_depth)
                        .expect("parent page should be on the stack")
                        .cell_idx = cell_idx as i32;
                    let (cell_payload, leaf_cell_idx) = {
                        let leaf_page = self.stack.top_ref();
                        let leaf_contents = leaf_page.get_contents();
                        turso_assert!(leaf_contents.is_leaf());
                        turso_assert_greater_than!(leaf_contents.cell_count(), 0);
                        let leaf_cell_idx = leaf_contents.cell_count() - 1;
                        let last_cell_on_child_page =
                            leaf_contents.cell_get(leaf_cell_idx, usable_space)?;

                        let mut cell_payload: crate::alloc::Vec<u8> = crate::alloc::vec![];
                        let child_pointer =
                            original_child_pointer.expect("there should be a pointer");
                        // Rewrite the old leaf cell as an interior cell depending on type.
                        match last_cell_on_child_page {
                            BTreeCell::TableLeafCell(leaf_cell) => {
                                // Table interior cells contain the left child pointer and the rowid as varint.
                                crate::with_btree_allocation_site!(
                                    CellPayload,
                                    cell_payload.try_extend(child_pointer.to_be_bytes())
                                )?;
                                write_varint_to_vec(leaf_cell.rowid as u64, &mut cell_payload)?;
                            }
                            BTreeCell::IndexLeafCell(leaf_cell) => {
                                // Index interior cells contain:
                                // 1. The left child pointer
                                // 2. The payload size as varint
                                // 3. The payload
                                // 4. The first overflow page as varint, omitted if no overflow.
                                crate::with_btree_allocation_site!(
                                    CellPayload,
                                    cell_payload.try_extend(child_pointer.to_be_bytes())
                                )?;
                                write_varint_to_vec(leaf_cell.payload_size, &mut cell_payload)?;
                                crate::with_btree_allocation_site!(
                                    CellPayload,
                                    cell_payload.try_extend(leaf_cell.payload.iter().copied())
                                )?;
                                if let Some(first_overflow_page) = leaf_cell.first_overflow_page {
                                    crate::with_btree_allocation_site!(
                                        CellPayload,
                                        cell_payload.try_extend(first_overflow_page.to_be_bytes())
                                    )?;
                                }
                            }
                            _ => unreachable!("Expected table leaf cell"),
                        }
                        (cell_payload, leaf_cell_idx)
                    };

                    let leaf_page = self.stack.top_ref();

                    self.pager.add_dirty(page)?;
                    self.pager.add_dirty(leaf_page)?;

                    // Step 2: Replace the cell in the parent (interior) page.
                    {
                        let parent_contents = page.get_contents();
                        let parent_page_id = page.get().id();
                        let left_child_page = u32::from_be_bytes(
                            cell_payload[..4].try_into().expect("invalid cell payload"),
                        );
                        turso_assert!(
                            left_child_page as usize != parent_page_id,
                            "corrupt: current page and left child page are the same",
                            { "left_child_page": left_child_page, "parent_page_id": parent_page_id }
                        );

                        // First, drop the old cell that is being replaced.
                        drop_cell(parent_contents, cell_idx, usable_space)?;
                        // Then, insert the new cell (the predecessor) in its place.
                        insert_into_cell(parent_contents, &cell_payload, cell_idx, usable_space)?;
                    }

                    // Step 3: Delete the predecessor cell from the leaf page.
                    {
                        let leaf_contents = leaf_page.get_contents();
                        drop_cell(leaf_contents, leaf_cell_idx, usable_space)?;
                    }

                    self.state = CursorState::Delete(DeleteState::CheckNeedsBalancing {
                        btree_depth,
                        post_balancing_seek_key: post_balancing_seek_key.take(),
                        interior_node_was_replaced: true,
                    });
                }

                DeleteState::CheckNeedsBalancing { btree_depth, .. } => {
                    let page = self.stack.top_ref();
                    // Check if either the leaf page we took the replacement cell from underflows, or if the interior page we inserted it into overflows OR underflows.
                    // If the latter is true, we must always balance that level regardless of whether the leaf page (or any ancestor pages in between) need balancing.

                    let leaf_underflows = {
                        let leaf_contents = page.get_contents();
                        let free_space = compute_free_space(leaf_contents, usable_space)?;
                        free_space * 3 > usable_space * 2
                    };

                    let interior_overflows_or_underflows = {
                        // Invariant: ancestor pages on the stack are pinned to the page cache,
                        // so we don't need return_if_locked_maybe_load! any ancestor,
                        // and we already loaded the current page above.
                        let interior_page = self
                            .stack
                            .get_page_at_level(*btree_depth)
                            .expect("ancestor page should be on the stack");
                        let interior_contents = interior_page.get_contents();
                        let overflows = !interior_contents.overflow_cells.is_empty();
                        if overflows {
                            true
                        } else {
                            let free_space = compute_free_space(interior_contents, usable_space)?;
                            free_space * 3 > usable_space * 2
                        }
                    };

                    let needs_balancing = leaf_underflows || interior_overflows_or_underflows;

                    let CursorState::Delete(DeleteState::CheckNeedsBalancing {
                        btree_depth,
                        ref mut post_balancing_seek_key,
                        interior_node_was_replaced,
                        ..
                    }) = self.state
                    else {
                        unreachable!("expected check needs balancing state");
                    };

                    if needs_balancing {
                        let balance_only_ancestor =
                            !leaf_underflows && interior_overflows_or_underflows;
                        if balance_only_ancestor {
                            // Only need to balance the ancestor page; move there immediately.
                            while self.stack.current() > btree_depth {
                                self.stack.pop();
                            }
                        }
                        let balance_both = leaf_underflows && interior_overflows_or_underflows;
                        turso_assert!(matches!(self.balance_state.sub_state, BalanceSubState::Start), "no balancing operation should be in progress during delete", { "sub_state": self.balance_state.sub_state });
                        let post_balancing_seek_key = post_balancing_seek_key
                            .take()
                            .expect("post_balancing_seek_key should be Some");
                        self.save_context(post_balancing_seek_key);
                        self.state = CursorState::Delete(DeleteState::Balancing {
                            balance_ancestor_at_depth: if balance_both {
                                Some(btree_depth)
                            } else {
                                None
                            },
                        });
                    } else {
                        // No balancing needed.
                        if interior_node_was_replaced {
                            // If we did replace an interior node, we need to advance the cursor once to
                            // get back at the interior node that now has the replaced content.
                            // The reason it is important to land here is that the replaced cell was smaller (LT) than the deleted cell,
                            // so we must ensure we skip over it. I.e., when BTreeCursor::next() is called, it will move past the cell
                            // that holds the replaced content.
                            self.state =
                                CursorState::Delete(DeleteState::PostInteriorNodeReplacement);
                        } else {
                            // If we didn't replace an interior node, we are done,
                            // except we need to retreat, so that the next call to BTreeCursor::next() lands at the next record (because we deleted the current one)
                            self.stack.retreat();
                            self.state = CursorState::None;
                            return Ok(IOResult::Done(()));
                        }
                    }
                }
                DeleteState::PostInteriorNodeReplacement => {
                    return_if_io!(self.get_next_record());
                    self.state = CursorState::None;
                    return Ok(IOResult::Done(()));
                }

                DeleteState::Balancing {
                    balance_ancestor_at_depth,
                } => {
                    let balance_ancestor_at_depth = *balance_ancestor_at_depth;
                    return_if_io!(self.balance(balance_ancestor_at_depth));
                    self.state = CursorState::Delete(DeleteState::RestoreContextAfterBalancing);
                }
                DeleteState::RestoreContextAfterBalancing => {
                    return_if_io!(self.restore_context());

                    // We deleted key K, and performed a seek to: GE { eq_only: true } K.
                    // This means that the cursor is now pointing to the next key after K.
                    // We need to make the next call to BTreeCursor::next() a no-op so that we don't skip over
                    // a row when deleting rows in a loop.
                    self.skip_advance = true;
                    self.state = CursorState::None;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    #[inline(always)]
    /// In outer joins, whenever the right-side table has no matching row, the query must still return a row
    /// for each left-side row. In order to achieve this, we set the null flag on the right-side table cursor
    /// so that it returns NULL for all columns until cleared.
    fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    #[inline(always)]
    fn get_null_flag(&self) -> bool {
        self.null_flag
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn exists(&mut self, key: &Value) -> IOResultOr<bool> {
        let int_key = match key {
            Value::Numeric(Numeric::Integer(i)) => i,
            _ => unreachable!("btree tables are indexed by integers!"),
        };
        let seek_result =
            return_if_io!(self.seek(SeekKey::TableRowId(*int_key), SeekOp::GE { eq_only: true }));
        let exists = matches!(seek_result, SeekResult::Found);
        self.invalidate_record();
        Ok(IOResult::Done(exists))
    }

    /// Deletes all content from the B-Tree but preserves the root page.
    ///
    /// Unlike [`btree_destroy`], which frees all pages including the root,
    /// this method only clears the tree’s contents. The root page remains
    /// allocated and is reset to an empty leaf page.
    fn clear_btree(&mut self) -> IOResultOr<Option<usize>> {
        // First entry only — destroy_btree_contents yields IO and resumes
        // through this method, so guard with the same state==None gate it
        // uses for its own state machine. Every page in this btree is about
        // to be freed; peers must drop their page stacks rather than save
        // positions that wouldn't outlive the clear (cf. sqlite3BtreeClearTable,
        // btree.c:10194).
        if matches!(self.state, CursorState::None) {
            self.pager.invalidate_peer_cursors(self);
            self.invalidate_count_cache();
            // Every page in this btree is about to be freed, so our own cached
            // rightmost page id is meaningless too (the id may even be
            // reallocated to an unrelated page after a refill).
            self.move_to_right_state.1 = None;
        }
        self.destroy_btree_contents(true)
    }

    /// Destroys the entire B-Tree, including the root page.
    ///
    /// All pages belonging to the tree are freed, leaving no trace of the B-Tree.
    /// Use this when the structure itself is no longer needed.
    ///
    /// For cases where the B-Tree should remain allocated but emptied, see [`btree_clear`].
    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    fn btree_destroy(&mut self) -> IOResultOr<Option<usize>> {
        // See clear_btree for the state==None gate rationale.
        if matches!(self.state, CursorState::None) {
            self.pager.invalidate_peer_cursors(self);
        }
        self.destroy_btree_contents(false)
    }

    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG))]
    /// Count the number of entries in the b-tree
    ///
    /// Only supposed to be used in the context of a simple Count Select Statement
    fn count(&mut self) -> IOResultOr<usize> {
        let mut mem_page;
        let mut contents;

        if self.valid_state == CursorValidState::Invalid {
            return Ok(IOResult::Done(0));
        }

        'outer: loop {
            let state = self.count_state;
            match state {
                CountState::Start => {
                    self.clear_saved_seek();
                    let c = return_if_io!(self.move_to_root_nonblock());
                    self.count_state = CountState::Loop;
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                CountState::Loop => {
                    self.stack.advance();
                    mem_page = self.stack.top_ref();
                    contents = mem_page.get_contents();

                    /* If this is a leaf page or the tree is not an int-key tree, then
                     ** this page contains countable entries. Increment the entry counter
                     ** accordingly.
                     */
                    if !matches!(contents.page_type()?, PageType::TableInterior) {
                        self.count += contents.cell_count();
                    }

                    let cell_idx = self.stack.current_cell_index() as usize;

                    // Second condition is necessary in case we return if the page is locked in the loop below
                    if contents.is_leaf() || cell_idx > contents.cell_count() {
                        loop {
                            if !self.stack.has_parent() {
                                // All pages of the b-tree have been visited. Return successfully.
                                // Move the `move_to_root_nonblock` call into `Finish` so a spill
                                // yield from it can't re-enter `Loop`'s `count += cell_count()`.
                                self.count_state = CountState::Finish;
                                continue 'outer;
                            }

                            // Move to parent
                            self.stack.pop();

                            mem_page = self.stack.top_ref();
                            turso_assert!(mem_page.is_loaded(), "page should be loaded");
                            contents = mem_page.get_contents();

                            let cell_idx = self.stack.current_cell_index() as usize;

                            if cell_idx <= contents.cell_count() {
                                break;
                            }
                        }
                    }

                    let cell_idx = self.stack.current_cell_index() as usize;

                    turso_assert_less_than_or_equal!(cell_idx, contents.cell_count());
                    turso_assert!(!contents.is_leaf());

                    if cell_idx == contents.cell_count() {
                        // Move to right child
                        // should be safe as contents is not a leaf page
                        let right_most_pointer = contents.rightmost_pointer()?.unwrap();
                        // Spill yield here would re-enter `CountState::Loop`,
                        // which re-runs `stack.advance()` and the leaf-count
                        // increment. Transition to `CountState::Descend` so
                        // re-entry skips those mutations and only retries the
                        // read + (second) advance + push.
                        match self.pager.read_page(right_most_pointer as i64)? {
                            IOResult::Done((child, c)) => {
                                self.stack.advance();
                                self.stack.push(child);
                                if let Some(c) = c {
                                    io_yield_one!(c);
                                }
                            }
                            IOResult::IO(IOCompletions(spill_c)) => {
                                self.count_state = CountState::Descend {
                                    target: right_most_pointer as i64,
                                };
                                io_yield_one!(spill_c);
                            }
                        }
                    } else {
                        // Move to child left page
                        let cell = contents.cell_get(cell_idx, self.usable_space())?;

                        match cell {
                            BTreeCell::TableInteriorCell(TableInteriorCell {
                                left_child_page,
                                ..
                            })
                            | BTreeCell::IndexInteriorCell(IndexInteriorCell {
                                left_child_page,
                                ..
                            }) => {
                                // Same re-entry handling as the rightmost
                                // branch above.
                                match self.pager.read_page(left_child_page as i64)? {
                                    IOResult::Done((child, c)) => {
                                        self.stack.advance();
                                        self.stack.push(child);
                                        if let Some(c) = c {
                                            io_yield_one!(c);
                                        }
                                    }
                                    IOResult::IO(IOCompletions(spill_c)) => {
                                        self.count_state = CountState::Descend {
                                            target: left_child_page as i64,
                                        };
                                        io_yield_one!(spill_c);
                                    }
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                CountState::Descend { target } => {
                    // Resume after a spill yield from `CountState::Loop` mid-
                    // descent. The loop-top mutations are already applied for
                    // this step; finish the descent and return to `Loop`.
                    let (child, c) = return_if_io!(self.pager.read_page(target));
                    self.stack.advance();
                    self.stack.push(child);
                    self.count_state = CountState::Loop;
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                CountState::Finish => {
                    // Idempotent: a spill yield re-enters this same arm.
                    let c = return_if_io!(self.move_to_root_nonblock());
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                    return Ok(IOResult::Done(self.count));
                }
            }
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        !self.has_record
    }

    #[inline]
    fn root_page(&self) -> i64 {
        self.root_page
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn rewind(&mut self) -> IOResultOr<()> {
        self.set_null_flag(false);
        if self.valid_state == CursorValidState::Invalid {
            return Ok(IOResult::Done(()));
        }
        self.clear_saved_seek();
        self.skip_advance = false;
        loop {
            match self.rewind_state {
                RewindState::Start => {
                    let c = return_if_io!(self.move_to_root_nonblock());
                    self.rewind_state = RewindState::NextRecord;
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                RewindState::NextRecord => {
                    return_if_io!(self.get_next_record());
                    self.rewind_state = RewindState::Start;
                    self.read_overflow_state = None;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    #[inline]
    fn has_rowid(&self) -> bool {
        match &self.index_info {
            Some(index_key_info) => index_key_info.has_rowid,
            None => true,
        }
    }

    #[inline]
    fn invalidate_record(&mut self) {
        self.noted_payload = NotedPayload::NONE;
        if let Some(record) = self.reusable_immutable_record.as_mut() {
            record.invalidate();
        }
    }

    #[inline]
    fn get_pager(&self) -> Arc<Pager> {
        self.pager.clone()
    }

    #[inline]
    fn get_skip_advance(&self) -> bool {
        self.skip_advance
    }

    /// Drop the page stack and auxiliary caches so the cursor will re-navigate
    /// from the root on its next access. Used when saving the position via
    /// try_save_position_for_external_balance isn't applicable (the peer
    /// btree was cleared/destroyed, or the position can't be expressed).
    /// valid_state stays Valid — only rewind/seek-style entry points are safe
    /// next; next/prev land on `current_page == -1` and return Done(false).
    fn invalidate_btree_cache(&mut self) {
        self.stack.clear();
        self.has_record = false;
        self.noted_payload = NotedPayload::NONE;
        self.move_to_right_state.1 = None;
        self.invalidate_count_cache();
        self.blob_cache.reset();
    }

    fn note_external_row_write(&mut self, rowid: Option<i64>) {
        // Only cursors that have served incremental blob I/O carry a pin. An unknown
        // rowid could be the pinned row, so it must expire the handle too — better a
        // spurious SQLITE_ABORT than byte access to a rewritten value.
        if let Some(pinned) = self.blob_pinned_rowid {
            if rowid.is_none_or(|r| r == pinned) {
                self.blob_expired = true;
            }
        }
    }

    fn register_with_pager(&self) {
        // Store before the push so a panic inside register_cursor still
        // triggers an (idempotent) unregister via Drop.
        self.did_register
            .store(true, crate::sync::atomic::Ordering::Relaxed);
        self.pager.register_cursor(self);
    }

    fn recycle(self: Box<Self>) {
        let pager = self.pager.clone();
        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from a Box of a live cursor. Its contents are
        // dropped exactly once here (Drop unregisters the cursor and retires
        // its record buffer), and the allocation goes on as uninitialized
        // memory of the same layout.
        let allocation = unsafe {
            std::ptr::drop_in_place(raw);
            Box::from_raw(raw.cast::<std::mem::MaybeUninit<BTreeCursor>>())
        };
        pager.recycle_cursor_allocation(allocation);
    }

    fn set_has_peers_for_external_writes(&self, has_peers: bool) {
        self.has_peers
            .store(has_peers, crate::sync::atomic::Ordering::Relaxed);
    }

    /// Mirrors SQLite's saveCursorPosition (btree.c:756). Saves rowid for
    /// table btrees, the cell record for index btrees; index records can
    /// yield IO via the overflow chain walk (`record()`). Returns
    /// [`SavePositionResult::MustInvalidate`] when the page stack is in a
    /// sentinel/dirty state we can't save from — has_record can lag the stack
    /// across an in-flight Insert/Delete — in which case the caller falls back
    /// to invalidate_btree_cache.
    fn try_save_position_for_external_balance(&mut self) -> IOResultOr<SavePositionResult> {
        // The peer is about to modify this btree's structure, so any cached
        // knowledge of the tree shape goes stale: the rightmost page id
        // (move_to_rightmost's skip-a-seek optimization) and the memoized
        // count. Positional state is preserved separately via save_context.
        // Idempotent, so safe across IO re-entry into this function.
        self.move_to_right_state.1 = None;
        self.invalidate_count_cache();
        if self.valid_state != CursorValidState::Valid || !self.has_record() {
            // Nothing to save: cursor has no live position. No invalidation
            // needed either — the stack is already in a state where the next
            // entry point will re-navigate from the root.
            return Ok(IOResult::Done(SavePositionResult::Saved));
        }
        // A peer mid-Insert/Delete may not yet have reached its own
        // save_context-at-balance point, so we can't claim it's saved. Fall
        // back to invalidation; the peer will re-navigate on next use.
        if !matches!(self.state, CursorState::None) {
            return Ok(IOResult::Done(SavePositionResult::MustInvalidate));
        }
        if self.stack.current_page < 0
            || (self.stack.current_page as usize) >= self.stack.stack.len()
            || self.stack.stack[self.stack.current_page as usize].is_none()
        {
            return Ok(IOResult::Done(SavePositionResult::MustInvalidate));
        }
        let cell_idx = self.stack.current_cell_index();
        if cell_idx < 0 {
            return Ok(IOResult::Done(SavePositionResult::MustInvalidate));
        }
        let (is_table, cell_count) = {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            (contents.page_type()?.is_table(), contents.cell_count())
        };
        if (cell_idx as usize) >= cell_count {
            return Ok(IOResult::Done(SavePositionResult::MustInvalidate));
        }
        if is_table {
            let page = self.stack.top_ref();
            let contents = page.get_contents();
            debug_assert!(
                matches!(contents.page_type(), Ok(PageType::TableLeaf)),
                "save_position: table cursor with has_record=true must be on a leaf"
            );
            let rowid = contents.cell_table_leaf_read_rowid(cell_idx as usize)?;
            self.save_context(CursorContext {
                key: CursorContextKey::TableRowId(rowid),
                seek_op: SeekOp::GE { eq_only: true },
            });
            return Ok(IOResult::Done(SavePositionResult::Saved));
        }
        // Index btree: yield IO for overflow chains. Allocate to the actual
        // payload size so wide-key indexes don't keep a page-sized buffer
        // per saved cursor.
        let cloned = {
            let record = return_if_io!(self.record());
            let record = record.expect("has_record=true but record() returned None");
            let payload = record.get_payload();
            let mut owned = crate::with_btree_allocation_site!(
                SavedCursorRecord,
                ImmutableRecord::new(payload.len())
            )?;
            crate::with_btree_allocation_site!(
                SavedCursorRecord,
                owned.start_serialization(payload)
            )?;
            owned
        };
        self.save_context(CursorContext {
            key: CursorContextKey::IndexKeyRowId(ImmutableRecordRef::from_owned_record(cloned)),
            seek_op: SeekOp::GE { eq_only: true },
        });
        Ok(IOResult::Done(SavePositionResult::Saved))
    }

    #[inline]
    fn has_record(&self) -> bool {
        self.has_record
    }

    #[inline]
    fn set_has_record(&mut self, has_record: bool) {
        self.has_record = has_record
    }

    #[inline]
    fn get_index_info(&self) -> &Arc<IndexInfo> {
        self.index_info.as_ref().unwrap()
    }

    fn seek_end(&mut self) -> IOResultOr<()> {
        if self.valid_state == CursorValidState::Invalid {
            return Ok(IOResult::Done(()));
        }
        loop {
            match self.seek_end_state {
                SeekEndState::Start => {
                    self.clear_saved_seek();
                    let c = return_if_io!(self.move_to_root_nonblock());
                    self.seek_end_state = SeekEndState::ProcessPage;
                    if let Some(c) = c {
                        io_yield_one!(c);
                    }
                }
                SeekEndState::ProcessPage => {
                    let mem_page = self.stack.top_ref();
                    let contents = mem_page.get_contents();
                    if contents.is_leaf() {
                        // set cursor just past the last cell to append
                        self.stack.set_cell_index(contents.cell_count() as i32);
                        self.seek_end_state = SeekEndState::Start;
                        return Ok(IOResult::Done(()));
                    }

                    match contents.rightmost_pointer()? {
                        Some(right_most_pointer) => {
                            let (child, c) =
                                return_if_io!(self.read_page(right_most_pointer as i64));
                            self.stack.set_cell_index(contents.cell_count() as i32 + 1); // invalid on interior
                            self.stack.push(child);
                            if let Some(c) = c {
                                io_yield_one!(c);
                            }
                        }
                        None => unreachable!("interior page must have rightmost pointer"),
                    }
                }
            }
        }
    }

    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG))]
    fn seek_to_last(&mut self) -> IOResultOr<()> {
        loop {
            match self.seek_to_last_state {
                SeekToLastState::Start => {
                    // A write through another cursor may save this cursor's old
                    // position. We need the current largest rowid, not that old
                    // position. Otherwise rowid() restores the old position.
                    self.clear_saved_seek();
                    let has_record = return_if_io!(self.move_to_rightmost());
                    self.invalidate_record();
                    self.set_has_record(has_record);
                    self.read_overflow_state = None;
                    if !has_record {
                        self.seek_to_last_state = SeekToLastState::IsEmpty;
                        continue;
                    }
                    return Ok(IOResult::Done(()));
                }
                SeekToLastState::IsEmpty => {
                    let is_empty = return_if_io!(self.is_empty_table());
                    turso_assert!(is_empty);
                    self.seek_to_last_state = SeekToLastState::Start;
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }
}

impl BTreeCursor {
    #[inline(never)]
    fn record_payload_general(&mut self) -> IOResultOr<Option<&[u8]>> {
        let page = self.stack.top_ref();
        let contents = page.get_contents();
        let cell_idx = self.stack.current_cell_index();
        let (payload, payload_start, _payload_size, first_overflow_page) =
            contents.cell_read_payload_at(cell_idx as usize, self.payload_limits)?;
        if first_overflow_page.is_none() {
            self.noted_payload = NotedPayload {
                start: payload_start as u32,
                size: payload.len() as u32,
            };
            return Ok(IOResult::Done(Some(payload)));
        }
        let record = return_if_io!(self.record());
        Ok(IOResult::Done(record.map(ImmutableRecord::get_payload)))
    }

    /// True when the next cell is on the same leaf page and no resumable
    /// state is pending, so advancing cannot yield and `next()` can skip
    /// its state machine. Every pending flag routes to the full path,
    /// which owns its handling: `skip_advance` (restore landed on the
    /// iteration target; advancing would skip a row), an abandoned
    /// overflow read, and an in-flight spill descent.
    #[inline(always)]
    fn can_advance_within_leaf(&self) -> bool {
        if self.has_pending_advance_state() {
            return false;
        }
        let contents = self.stack.top_ref().get_contents();
        let cell_idx = self.stack.current_cell_index();
        cell_idx >= 0 && contents.is_leaf() && cell_idx as usize + 1 < contents.cell_count()
    }

    /// True when the cursor sits on the last cell of the rightmost leaf and
    /// nothing is pending: the tree has no next record, so `next()` only has
    /// to step past the cell. This is what every NewRowid does before an
    /// append, and what a scan does once at its end.
    #[inline(always)]
    fn is_on_last_cell_of_tree(&self) -> bool {
        if self.has_pending_advance_state() {
            return false;
        }
        let contents = self.stack.top_ref().get_contents();
        let cell_idx = self.stack.current_cell_index();
        cell_idx >= 0
            && contents.is_leaf()
            && cell_idx as usize + 1 == contents.cell_count()
            && !self.ancestor_pages_have_more_children()
    }

    #[inline(always)]
    fn has_pending_advance_state(&self) -> bool {
        !matches!(self.advance_state, AdvanceState::Start)
            || !matches!(self.valid_state, CursorValidState::Valid)
            || self.needs_restore()
            || self.skip_advance
            || !self.has_record
            || self.read_overflow_state.is_some()
            || self.iteration_pending_descent.is_some()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IntegrityCheckError {
    #[error("Cell {cell_idx} in page {page_id} is out of range. cell_range={cell_start}..{cell_end}, content_area={content_area}, usable_space={usable_space}")]
    CellOutOfRange {
        cell_idx: usize,
        page_id: i64,
        cell_start: usize,
        cell_end: usize,
        content_area: usize,
        usable_space: usize,
    },
    #[error("Cell {cell_idx} in page {page_id} extends out of page. cell_range={cell_start}..{cell_end}, content_area={content_area}, usable_space={usable_space}")]
    CellOverflowsPage {
        cell_idx: usize,
        page_id: i64,
        cell_start: usize,
        cell_end: usize,
        content_area: usize,
        usable_space: usize,
    },
    #[error("Page {page_id} ({page_category:?}) cell {cell_idx} has rowid={rowid} in wrong order. Parent cell has parent_rowid={max_intkey} and next_rowid={next_rowid}")]
    CellRowidOutOfRange {
        page_id: i64,
        page_category: PageCategory,
        cell_idx: usize,
        rowid: i64,
        max_intkey: i64,
        next_rowid: i64,
    },
    #[error("Page {page_id} is at different depth from another leaf page this_page_depth={this_page_depth}, other_page_depth={other_page_depth} ")]
    LeafDepthMismatch {
        page_id: i64,
        this_page_depth: usize,
        other_page_depth: usize,
    },
    #[error("Page {page_id} detected freeblock that extends page start={start} end={end}")]
    FreeBlockOutOfRange {
        page_id: i64,
        start: usize,
        end: usize,
    },
    #[error("Page {page_id} cell overlap detected at position={start} with previous_end={prev_end}. content_area={content_area}, is_free_block={is_free_block}")]
    CellOverlap {
        page_id: i64,
        start: usize,
        prev_end: usize,
        content_area: usize,
        is_free_block: bool,
    },
    #[error("Page {page_id} unexpected fragmentation got={got}, expected={expected}")]
    UnexpectedFragmentation {
        page_id: i64,
        got: usize,
        expected: usize,
    },
    #[error("Page {page_id} referenced multiple times (references={references:?}, page_category={page_category:?})")]
    PageReferencedMultipleTimes {
        page_id: i64,
        references: crate::alloc::Vec<i64>,
        page_category: PageCategory,
    },
    #[error("Freelist: size is {actual_count} but should be {expected_count}")]
    FreelistCountMismatch {
        actual_count: usize,
        expected_count: usize,
    },
    #[error("Page {page_id}: never used")]
    PageNeverUsed { page_id: i64 },
    #[error("Pending byte page {page_id} is being used")]
    PendingBytePageUsed { page_id: i64 },
    #[error("Freelist: freelist leaf count too big on page {page_id}")]
    FreelistTrunkCorrupt {
        page_id: i64,
        page_pointers: u32,
        max_pointers: usize,
    },
    #[error("Freelist: invalid page number {pointer}")]
    FreelistPointerOutOfRange { page_id: i64, pointer: i64 },
    #[error("overflow list length is {got} but should be {expected}")]
    OverflowListLengthMismatch { got: usize, expected: usize },
}

fn push_integrity_error(
    errors: &mut crate::alloc::Vec<IntegrityCheckError>,
    error: IntegrityCheckError,
) -> Result<()> {
    crate::with_btree_allocation_site!(IntegrityCheck, errors.try_push(error))?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageCategory {
    Normal,
    Overflow,
    FreeListTrunk,
    FreePage,
}

#[derive(Clone)]
pub struct CheckFreelist {
    pub expected_count: usize,
    pub actual_count: usize,
}

#[derive(Clone)]
struct IntegrityCheckPageEntry {
    page_idx: i64,
    level: usize,
    max_intkey: i64,
    page_category: PageCategory,
    overflow_pages_expected: Option<usize>,
    overflow_pages_seen: usize,
}
pub struct IntegrityCheckState {
    page_stack: crate::alloc::Vec<IntegrityCheckPageEntry>,
    pub db_size: usize,
    first_leaf_level: Option<usize>,
    pub page_reference: HashMap<i64, i64>,
    page: Option<PageRef>,
    pub freelist_count: CheckFreelist,
}

impl IntegrityCheckState {
    pub fn new(db_size: usize) -> Self {
        Self {
            page_stack: crate::alloc::vec![],
            db_size,
            page_reference: HashMap::default(),
            first_leaf_level: None,
            page: None,
            freelist_count: CheckFreelist {
                expected_count: 0,
                actual_count: 0,
            },
        }
    }

    pub fn set_expected_freelist_count(&mut self, count: usize) {
        self.freelist_count.expected_count = count;
    }

    pub fn start(
        &mut self,
        page_idx: i64,
        page_category: PageCategory,
        errors: &mut crate::alloc::Vec<IntegrityCheckError>,
    ) -> Result<()> {
        turso_assert!(
            self.page_stack.is_empty(),
            "stack should be empty before integrity check for new root"
        );
        // root can't be referenced from anywhere - so we insert "zero entry" for it
        self.push_page(
            IntegrityCheckPageEntry {
                page_idx,
                level: 0,
                max_intkey: i64::MAX,
                page_category,
                overflow_pages_expected: None,
                overflow_pages_seen: 0,
            },
            0,
            errors,
        )?;
        self.first_leaf_level = None;
        let _ = self.page.take();
        Ok(())
    }

    fn push_page(
        &mut self,
        entry: IntegrityCheckPageEntry,
        referenced_by: i64,
        errors: &mut crate::alloc::Vec<IntegrityCheckError>,
    ) -> Result<()> {
        let page_id = entry.page_idx;
        let Some(previous) = self.page_reference.get(&page_id).copied() else {
            crate::with_btree_allocation_site!(IntegrityCheck, self.page_stack.try_reserve(1))?;
            let previous = self.page_reference.insert(page_id, referenced_by);
            turso_assert!(
                previous.is_none(),
                "page reference changed during insertion"
            );
            self.page_stack
                .push_within_capacity(entry)
                .unwrap_or_else(|_| unreachable!("reserved page stack slot was unavailable"));
            return Ok(());
        };
        let references = crate::with_btree_allocation_site!(
            IntegrityCheck,
            crate::alloc::try_vec![previous, referenced_by]
        )?;
        push_integrity_error(
            errors,
            IntegrityCheckError::PageReferencedMultipleTimes {
                page_id,
                page_category: entry.page_category,
                references,
            },
        )?;
        Ok(())
    }
}
impl std::fmt::Debug for IntegrityCheckState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IntegrityCheckState")
            .field("first_leaf_level", &self.first_leaf_level)
            .finish()
    }
}

fn overflow_pages_expected_for_cell(
    payload_size: u64,
    local_payload_size: usize,
    usable_space: usize,
) -> usize {
    let payload_size = usize::try_from(payload_size).unwrap_or(usize::MAX);
    let remaining_payload = payload_size.saturating_sub(local_payload_size);
    if remaining_payload == 0 {
        return 0;
    }
    let overflow_page_payload = usable_space.saturating_sub(4).max(1);
    remaining_payload.div_ceil(overflow_page_payload)
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
    errors: &mut crate::alloc::Vec<IntegrityCheckError>,
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
) -> IOResultOr<()> {
    if let Some(mv_store) = mv_store {
        let Some(IntegrityCheckPageEntry {
            page_idx: root_page,
            ..
        }) = state.page_stack.last().cloned()
        else {
            return Ok(IOResult::Done(()));
        };
        if root_page < 0 {
            let table_id = mv_store.get_table_id_from_root_page(root_page);
            turso_assert!(
                !mv_store.is_btree_allocated(&table_id),
                "we got a negative page index that is reported as allocated"
            );
            state.page_stack.pop();
            return Ok(IOResult::Done(()));
        }
    }
    if state.db_size == 0 {
        state.page_stack.pop();
        return Ok(IOResult::Done(()));
    }
    loop {
        let Some(IntegrityCheckPageEntry {
            page_idx,
            page_category,
            level,
            max_intkey,
            overflow_pages_expected,
            overflow_pages_seen,
        }) = state.page_stack.last().cloned()
        else {
            return Ok(IOResult::Done(()));
        };
        turso_assert!(
            page_idx >= 0,
            "pages should be positive during integrity check"
        );
        let page = match state.page.take() {
            Some(page) => page,
            None => {
                // On `IO(spill_c)` we leave `state.page = None` so re-entry
                // re-takes this None branch and resumes via the pager's
                // `pending_reads` memoization.
                let (page, c) = return_if_io!(pager.read_page(page_idx));
                state.page = Some(page);
                if let Some(c) = c {
                    io_yield_one!(c);
                }
                state.page.take().expect("page should be present")
            }
        };
        turso_assert!(page.is_loaded(), "page should be loaded");
        state.page_stack.pop();

        let contents = page.get_contents();
        if page_category == PageCategory::FreeListTrunk {
            state.freelist_count.actual_count += 1;
            let next_freelist_trunk_page =
                contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_NEXT_TRUNK_PTR);
            if next_freelist_trunk_page != 0 {
                if next_freelist_trunk_page as usize > state.db_size {
                    tracing::error!(
                        "integrity_check: freelist trunk page {} has invalid next pointer {}. header_bytes={:02x?}",
                        page.get().id(),
                        next_freelist_trunk_page,
                        &contents.as_ptr()[0..16]
                    );
                    push_integrity_error(
                        errors,
                        IntegrityCheckError::FreelistPointerOutOfRange {
                            page_id: page.get().id() as i64,
                            pointer: next_freelist_trunk_page as i64,
                        },
                    )?;
                    continue;
                }
                state.push_page(
                    IntegrityCheckPageEntry {
                        page_idx: next_freelist_trunk_page as i64,
                        level,
                        max_intkey,
                        page_category: PageCategory::FreeListTrunk,
                        overflow_pages_expected: None,
                        overflow_pages_seen: 0,
                    },
                    page.get().id() as i64,
                    errors,
                )?;
            }
            let page_pointers = contents.read_u32_no_offset(FREELIST_TRUNK_OFFSET_LEAF_COUNT);
            let page_size = contents.as_ptr().len();
            let max_pointers =
                page_size.saturating_sub(FREELIST_TRUNK_HEADER_SIZE) / FREELIST_LEAF_PTR_SIZE;
            if unlikely(page_pointers as usize > max_pointers) {
                tracing::error!(
                    "integrity_check: freelist trunk page {} has invalid leaf count {} (max {}). header_bytes={:02x?}",
                    page.get().id(),
                    page_pointers,
                    max_pointers,
                    &contents.as_ptr()[0..16]
                );
                push_integrity_error(
                    errors,
                    IntegrityCheckError::FreelistTrunkCorrupt {
                        page_id: page.get().id() as i64,
                        page_pointers,
                        max_pointers,
                    },
                )?;
                continue;
            }
            for i in 0..page_pointers {
                let offset =
                    FREELIST_TRUNK_OFFSET_FIRST_LEAF_PTR + FREELIST_LEAF_PTR_SIZE * i as usize;
                if unlikely(offset + FREELIST_LEAF_PTR_SIZE > page_size) {
                    tracing::error!(
                        "integrity_check: freelist trunk page {} has invalid leaf offset {}. header_bytes={:02x?}",
                        page.get().id(),
                        offset,
                        &contents.as_ptr()[0..16]
                    );
                    push_integrity_error(
                        errors,
                        IntegrityCheckError::FreelistTrunkCorrupt {
                            page_id: page.get().id() as i64,
                            page_pointers,
                            max_pointers,
                        },
                    )?;
                    break;
                }
                let page_pointer = contents.read_u32_no_offset(offset);
                if page_pointer as usize > state.db_size {
                    tracing::error!(
                        "integrity_check: freelist trunk page {} has invalid leaf pointer {}. header_bytes={:02x?}",
                        page.get().id(),
                        page_pointer,
                        &contents.as_ptr()[0..16]
                    );
                    push_integrity_error(
                        errors,
                        IntegrityCheckError::FreelistPointerOutOfRange {
                            page_id: page.get().id() as i64,
                            pointer: page_pointer as i64,
                        },
                    )?;
                    continue;
                }
                state.push_page(
                    IntegrityCheckPageEntry {
                        page_idx: page_pointer as i64,
                        level,
                        max_intkey,
                        page_category: PageCategory::FreePage,
                        overflow_pages_expected: None,
                        overflow_pages_seen: 0,
                    },
                    page.get().id() as i64,
                    errors,
                )?;
            }
            continue;
        }
        if page_category == PageCategory::FreePage {
            state.freelist_count.actual_count += 1;
            continue;
        }
        if page_category == PageCategory::Overflow {
            let overflow_pages_seen = overflow_pages_seen.saturating_add(1);
            let next_overflow_page = contents.read_u32_no_offset(0);
            if next_overflow_page != 0 {
                state.push_page(
                    IntegrityCheckPageEntry {
                        page_idx: next_overflow_page as i64,
                        level,
                        max_intkey,
                        page_category: PageCategory::Overflow,
                        overflow_pages_expected,
                        overflow_pages_seen,
                    },
                    page.get().id() as i64,
                    errors,
                )?;
            } else if let Some(expected) = overflow_pages_expected {
                if overflow_pages_seen != expected {
                    push_integrity_error(
                        errors,
                        IntegrityCheckError::OverflowListLengthMismatch {
                            got: overflow_pages_seen,
                            expected,
                        },
                    )?;
                }
            }
            continue;
        }

        let usable_space = pager.usable_space();
        let mut coverage_checker = CoverageChecker::new(page.get().id() as i64);

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
            let (cell_start, cell_length) = contents.cell_get_raw_region(cell_idx, usable_space)?;
            if cell_start < contents.cell_content_area() as usize || cell_start > usable_space - 4 {
                push_integrity_error(
                    errors,
                    IntegrityCheckError::CellOutOfRange {
                        cell_idx,
                        page_id: page.get().id() as i64,
                        cell_start,
                        cell_end: cell_start + cell_length,
                        content_area: contents.cell_content_area() as usize,
                        usable_space,
                    },
                )?;
            }
            if cell_start + cell_length > usable_space {
                push_integrity_error(
                    errors,
                    IntegrityCheckError::CellOverflowsPage {
                        cell_idx,
                        page_id: page.get().id() as i64,
                        cell_start,
                        cell_end: cell_start + cell_length,
                        content_area: contents.cell_content_area() as usize,
                        usable_space,
                    },
                )?;
            }
            coverage_checker.add_cell(cell_start, cell_start + cell_length);
            let cell = contents.cell_get(cell_idx, usable_space)?;
            match cell {
                BTreeCell::TableInteriorCell(table_interior_cell) => {
                    state.push_page(
                        IntegrityCheckPageEntry {
                            page_idx: table_interior_cell.left_child_page as i64,
                            level: level + 1,
                            max_intkey: table_interior_cell.rowid,
                            page_category: PageCategory::Normal,
                            overflow_pages_expected: None,
                            overflow_pages_seen: 0,
                        },
                        page.get().id() as i64,
                        errors,
                    )?;
                    let rowid = table_interior_cell.rowid;
                    if rowid > max_intkey || rowid > next_rowid {
                        push_integrity_error(
                            errors,
                            IntegrityCheckError::CellRowidOutOfRange {
                                page_id: page.get().id() as i64,
                                page_category,
                                cell_idx,
                                rowid,
                                max_intkey,
                                next_rowid,
                            },
                        )?;
                    }
                    next_rowid = rowid;
                }
                BTreeCell::TableLeafCell(table_leaf_cell) => {
                    // check depth of leaf pages are equal
                    if let Some(expected_leaf_level) = state.first_leaf_level {
                        if expected_leaf_level != level {
                            push_integrity_error(
                                errors,
                                IntegrityCheckError::LeafDepthMismatch {
                                    page_id: page.get().id() as i64,
                                    this_page_depth: level,
                                    other_page_depth: expected_leaf_level,
                                },
                            )?;
                        }
                    } else {
                        state.first_leaf_level = Some(level);
                    }
                    let rowid = table_leaf_cell.rowid;
                    if rowid > max_intkey || rowid > next_rowid {
                        push_integrity_error(
                            errors,
                            IntegrityCheckError::CellRowidOutOfRange {
                                page_id: page.get().id() as i64,
                                page_category,
                                cell_idx,
                                rowid,
                                max_intkey,
                                next_rowid,
                            },
                        )?;
                    }
                    next_rowid = rowid;
                    if let Some(first_overflow_page) = table_leaf_cell.first_overflow_page {
                        let expected_pages = overflow_pages_expected_for_cell(
                            table_leaf_cell.payload_size,
                            table_leaf_cell.payload.len(),
                            usable_space,
                        );
                        state.push_page(
                            IntegrityCheckPageEntry {
                                page_idx: first_overflow_page as i64,
                                level,
                                max_intkey,
                                page_category: PageCategory::Overflow,
                                overflow_pages_expected: Some(expected_pages),
                                overflow_pages_seen: 0,
                            },
                            page.get().id() as i64,
                            errors,
                        )?;
                    }
                }
                BTreeCell::IndexInteriorCell(index_interior_cell) => {
                    state.push_page(
                        IntegrityCheckPageEntry {
                            page_idx: index_interior_cell.left_child_page as i64,
                            level: level + 1,
                            max_intkey, // we don't care about intkey in non-table pages
                            page_category: PageCategory::Normal,
                            overflow_pages_expected: None,
                            overflow_pages_seen: 0,
                        },
                        page.get().id() as i64,
                        errors,
                    )?;
                    if let Some(first_overflow_page) = index_interior_cell.first_overflow_page {
                        let expected_pages = overflow_pages_expected_for_cell(
                            index_interior_cell.payload_size,
                            index_interior_cell.payload.len(),
                            usable_space,
                        );
                        state.push_page(
                            IntegrityCheckPageEntry {
                                page_idx: first_overflow_page as i64,
                                level,
                                max_intkey,
                                page_category: PageCategory::Overflow,
                                overflow_pages_expected: Some(expected_pages),
                                overflow_pages_seen: 0,
                            },
                            page.get().id() as i64,
                            errors,
                        )?;
                    }
                }
                BTreeCell::IndexLeafCell(index_leaf_cell) => {
                    // check depth of leaf pages are equal
                    if let Some(expected_leaf_level) = state.first_leaf_level {
                        if expected_leaf_level != level {
                            push_integrity_error(
                                errors,
                                IntegrityCheckError::LeafDepthMismatch {
                                    page_id: page.get().id() as i64,
                                    this_page_depth: level,
                                    other_page_depth: expected_leaf_level,
                                },
                            )?;
                        }
                    } else {
                        state.first_leaf_level = Some(level);
                    }
                    if let Some(first_overflow_page) = index_leaf_cell.first_overflow_page {
                        let expected_pages = overflow_pages_expected_for_cell(
                            index_leaf_cell.payload_size,
                            index_leaf_cell.payload.len(),
                            usable_space,
                        );
                        state.push_page(
                            IntegrityCheckPageEntry {
                                page_idx: first_overflow_page as i64,
                                level,
                                max_intkey,
                                page_category: PageCategory::Overflow,
                                overflow_pages_expected: Some(expected_pages),
                                overflow_pages_seen: 0,
                            },
                            page.get().id() as i64,
                            errors,
                        )?;
                    }
                }
            }
        }

        if let Some(rightmost) = contents.rightmost_pointer()? {
            state.push_page(
                IntegrityCheckPageEntry {
                    page_idx: rightmost as i64,
                    level: level + 1,
                    max_intkey,
                    page_category: PageCategory::Normal,
                    overflow_pages_expected: None,
                    overflow_pages_seen: 0,
                },
                page.get().id() as i64,
                errors,
            )?;
        }

        // Now we add free blocks to the coverage checker
        let first_freeblock = contents.first_freeblock() as usize;
        if first_freeblock > 0 {
            let mut pc = first_freeblock;
            while pc > 0 {
                let next = contents.read_u16_no_offset(pc) as usize;
                let size = contents.read_u16_no_offset(pc + 2) as usize;
                // check it doesn't go out of range
                if pc > usable_space - 4 {
                    push_integrity_error(
                        errors,
                        IntegrityCheckError::FreeBlockOutOfRange {
                            page_id: page.get().id() as i64,
                            start: pc,
                            end: pc + size,
                        },
                    )?;
                    break;
                }
                coverage_checker.add_free_block(pc, pc + size);
                pc = next;
            }
        }

        // Let's check the overlap of freeblocks and cells now that we have collected them all.
        coverage_checker.analyze(
            usable_space,
            contents.cell_content_area() as usize,
            errors,
            contents.num_frag_free_bytes() as usize,
        )?;
    }
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

/// Pads the cell that starts at `cell_start` in `buf` with zero bytes until it takes
/// MINIMUM_CELL_SIZE bytes, the least space any cell takes on a page (a freed cell must be
/// able to hold a 4-byte freeblock header).
///
/// Only index cells can be smaller: a one-column record of 0, 1, '' or X'' is 2 bytes, so
/// its cell is 3. A leaf page stores such a cell padded and cell_get_raw_region reports the
/// padded size, while a divider in the parent stores the cell's real size after its child
/// pointer. Padding the cell whenever it is headed for a leaf keeps balancing working with
/// one size; the record's own length prefix keeps readers from ever looking at the padding.
fn ensure_min_cell_size(buf: &mut crate::alloc::Vec<u8>, cell_start: usize) {
    const ZEROS: [u8; MINIMUM_CELL_SIZE] = [0; MINIMUM_CELL_SIZE];
    let padding = (cell_start + MINIMUM_CELL_SIZE).saturating_sub(buf.len());
    buf.extend_from_slice(&ZEROS[..padding]);
}

#[cfg(debug_assertions)]
fn validate_cells_after_insertion(cell_array: &CellArray, leaf_data: bool) {
    for cell in &cell_array.cell_payloads {
        turso_assert_greater_than_or_equal!(cell.len(), 4);

        if leaf_data {
            turso_assert!(cell[0] != 0);
        }
    }
}

pub struct CoverageChecker {
    /// Min-heap ordered by cell start
    heap: BinaryHeap<Reverse<IntegrityCheckCellRange>>,
    page_idx: i64,
}

impl CoverageChecker {
    pub fn new(page_idx: i64) -> Self {
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
        usable_space: usize,
        content_area: usize,
        errors: &mut crate::alloc::Vec<IntegrityCheckError>,
        expected_fragmentation: usize,
    ) -> Result<()> {
        let mut fragmentation = 0;
        let mut prev_end = content_area;
        while let Some(cell) = self.heap.pop() {
            let start = cell.0.start;
            if prev_end > start {
                push_integrity_error(
                    errors,
                    IntegrityCheckError::CellOverlap {
                        page_id: self.page_idx,
                        start,
                        prev_end,
                        content_area,
                        is_free_block: cell.0.is_free_block,
                    },
                )?;
                break;
            } else {
                fragmentation += start - prev_end;
                prev_end = cell.0.end;
            }
        }
        fragmentation += usable_space - prev_end;
        if fragmentation != expected_fragmentation {
            push_integrity_error(
                errors,
                IntegrityCheckError::UnexpectedFragmentation {
                    page_id: self.page_idx,
                    got: fragmentation,
                    expected: expected_fragmentation,
                },
            )?;
        }
        Ok(())
    }
}

/// Stack of pages representing the tree traversal order.
/// current_page represents the current page being used in the tree and current_page - 1 would be
/// the parent. Using current_page + 1 or higher is undefined behaviour.
struct PageStack {
    /// Pointer to the current page being consumed
    current_page: i32,
    /// List of pages in the stack. Root page will be in index 0
    ///
    /// [ManuallyDrop] because as an optimization, [Self::drop] clears only the slots that actually hold pages.
    pub stack: std::mem::ManuallyDrop<[Option<PageRef>; BTCURSOR_MAX_DEPTH + 1]>,
    /// List of cell indices in the stack.
    /// node_states[current_page] is the current cell index being consumed. Similarly
    /// node_states[current_page-1] is the cell index of the parent of the current page
    /// that we save in case of going back up.
    /// There are two points that need special attention:
    ///  If node_states[current_page] = -1, it indicates that the current iteration has reached the start of the current_page
    ///  If node_states[current_page] = `cell_count`, it means that the current iteration has reached the end of the current_page
    node_states: [BTreeNodeState; BTCURSOR_MAX_DEPTH + 1],
}

impl PageStack {
    /// Push a new page onto the stack.
    /// This effectively means traversing to a child page.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG, name = "pagestack::push"))]
    fn _push(&mut self, page: PageRef, starting_cell_idx: i32) {
        tracing::trace!(current = self.current_page, new_page_id = page.get().id(),);
        'validate: {
            let current = self.current_page;
            if current == -1 {
                break 'validate;
            }
            let current_top = self.stack[current as usize].as_ref();
            if let Some(current_top) = current_top {
                turso_assert!(
                    current_top.get().id() != page.get().id(),
                    "about to push page twice",
                    { "page_id": page.get().id() }
                );
            }
        }
        self.populate_parent_cell_count();
        self.current_page += 1;
        turso_assert_greater_than_or_equal!(self.current_page, 0);
        let current = self.current_page as usize;
        turso_assert_less_than!(
            current,
            BTCURSOR_MAX_DEPTH,
            "corrupted database, stack is bigger than expected"
        );

        // Pin the page to prevent it from being evicted while on the stack
        page.pin();

        self.stack[current] = Some(page);
        self.node_states[current] = BTreeNodeState {
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
    fn populate_parent_cell_count(&mut self) {
        let stack_empty = self.current_page == -1;
        if stack_empty {
            return;
        }
        let current = self.current();
        let page = self.stack[current].as_ref().unwrap();
        turso_assert!(
            page.is_pinned(),
            "parent page is not pinned",
            { "page_id": page.get().id() }
        );
        turso_assert!(
            page.is_loaded(),
            "parent page is not loaded",
            { "page_id": page.get().id() }
        );
        let contents = page.get_contents();
        let cell_count = contents.cell_count() as i32;
        self.node_states[current].cell_count = Some(cell_count);
    }

    fn push(&mut self, page: PageRef) {
        self._push(page, -1);
    }

    fn push_backwards(&mut self, page: PageRef) {
        self._push(page, i32::MAX);
    }

    /// Pop a page off the stack.
    /// This effectively means traversing back up to a parent page.
    #[cfg_attr(debug_assertions, instrument(skip_all, level = Level::DEBUG, name = "pagestack::pop"))]
    fn pop(&mut self) {
        let current = self.current_page;
        turso_assert_greater_than_or_equal!(current, 0);
        tracing::trace!(current);
        let current = current as usize;

        // Unpin the page before removing it from the stack
        if let Some(page) = &self.stack[current] {
            page.unpin();
        }

        turso_assert_greater_than!(current, 0);
        self.node_states[current] = BTreeNodeState::default();
        self.stack[current] = None;
        self.current_page -= 1;
    }

    /// Get the top page on the stack.
    /// This is the page that is currently being traversed.
    fn top(&self) -> PageRef {
        let current = self.current();
        let page = self.stack[current].clone().unwrap();
        turso_assert!(page.is_loaded(), "page should be loaded");
        page
    }

    /// The page at the top of the stack. Pages on the stack are pinned, and
    /// every read of the page asserts that its buffer is present, so the
    /// loaded flag is not tested here again.
    #[inline(always)]
    fn top_ref(&self) -> &PageRef {
        let current = self.current();
        self.stack[current].as_ref().unwrap()
    }

    /// Current page pointer being used
    #[inline(always)]
    fn current(&self) -> usize {
        turso_assert_greater_than_or_equal!(self.current_page, 0);
        self.current_page as usize
    }

    /// Cell index of the current page
    #[inline(always)]
    fn current_cell_index(&self) -> i32 {
        let current = self.current();
        self.node_states[current].cell_idx
    }

    /// Check if the current cell index is less than 0.
    /// This means we have been iterating backwards and have reached the start of the page.
    fn current_cell_index_less_than_min(&self) -> bool {
        let cell_idx = self.current_cell_index();
        cell_idx < 0
    }

    /// Advance the current cell index of the current page to the next cell.
    /// We usually advance after going traversing a new page
    #[inline(always)]
    fn advance(&mut self) {
        let current = self.current();
        self.node_states[current].cell_idx += 1;
    }

    #[cfg_attr(debug_assertions, instrument(skip(self), level = Level::DEBUG, name = "pagestack::retreat"))]
    fn retreat(&mut self) {
        let current = self.current();
        #[cfg(debug_assertions)]
        {
            let node_states: [i32; BTCURSOR_MAX_DEPTH + 1] =
                std::array::from_fn(|index| self.node_states[index].cell_idx);
            tracing::trace!(
                curr_cell_index = self.node_states[current].cell_idx,
                ?node_states,
            );
        }
        self.node_states[current].cell_idx -= 1;
    }

    fn set_cell_index(&mut self, idx: i32) {
        let current = self.current();
        self.node_states[current].cell_idx = idx;
    }

    fn has_parent(&self) -> bool {
        self.current_page > 0
    }

    /// Get a page at a specific level in the stack (0 = root, 1 = first child, etc.)
    fn get_page_at_level(&self, level: usize) -> Option<&PageRef> {
        if level < self.stack.len() {
            self.stack[level].as_ref()
        } else {
            None
        }
    }

    fn get_page_contents_at_level(&self, level: usize) -> Option<&mut PageContent> {
        self.get_page_at_level(level)
            .map(|page| page.get_contents())
    }

    /// Unpin and remove every page currently held in the stack.
    /// Slots are taken so that stale page references cannot survive past a
    /// reset — a leftover `Some(page)` after a clear could otherwise be
    /// unpinned again on the next reset, decrementing the pin count of a
    /// page another cursor's stack still relies on.
    fn unpin_all_and_clear_slots(&mut self) {
        // Only the slots up to the current page hold pages and states: push
        // fills the slot above the top and pop clears the top slot.
        let used = (self.current_page + 1).max(0) as usize;
        debug_assert!(
            self.stack[used..].iter().all(|slot| slot.is_none()),
            "page stack holds a page above its top"
        );
        for slot in self.stack[..used].iter_mut() {
            if let Some(page) = slot.take() {
                let _ = page.try_unpin();
            }
        }
        for state in self.node_states[..used].iter_mut() {
            *state = BTreeNodeState::default();
        }
    }

    fn clear(&mut self) {
        self.unpin_all_and_clear_slots();
        self.current_page = -1;
    }

    /// Whether slot 0 holds the loaded root page of the btree.
    fn holds_root(&self, root_page: i64) -> bool {
        self.current_page >= 0
            && self.stack[0]
                .as_ref()
                .is_some_and(|page| page.get().id() as i64 == root_page && page.is_loaded())
    }

    /// Drop every page below the root and leave the stack on the root as a
    /// fresh push of it would.
    fn pop_to_root(&mut self) {
        let used = (self.current_page + 1) as usize;
        for slot in self.stack[1..used].iter_mut() {
            if let Some(page) = slot.take() {
                let _ = page.try_unpin();
            }
        }
        for state in self.node_states[1..used].iter_mut() {
            *state = BTreeNodeState::default();
        }
        self.node_states[0] = BTreeNodeState {
            cell_idx: -1,
            cell_count: None,
        };
        self.current_page = 0;
    }
}

impl Drop for PageStack {
    fn drop(&mut self) {
        self.unpin_all_and_clear_slots();
    }
}

/// Used for redistributing cells during a balance operation.
struct CellArray {
    /// The actual cell data.
    /// For all other page types except table leaves, this will also contain the associated divider cell from the parent page.
    cell_payloads: crate::alloc::Vec<&'static mut [u8]>,

    /// Prefix sum of cells in each page.
    /// For example, if three pages have 1, 2, and 3 cells, respectively,
    /// then cell_count_per_page_cumulative will be [1, 3, 6].
    cell_count_per_page_cumulative: [u16; MAX_NEW_SIBLING_PAGES_AFTER_BALANCE],
}

impl std::fmt::Debug for CellArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellArray").finish()
    }
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

/// Try to find a freeblock inside the cell content area that is large enough to fit the given amount of bytes.
/// Used to check if a cell can be inserted into a freeblock to reduce fragmentation.
/// Returns the absolute byte offset of the freeblock if found.
fn find_free_slot(
    page_ref: &PageContent,
    usable_space: usize,
    amount: usize,
) -> Result<Option<usize>> {
    const CELL_SIZE_MIN: usize = 4;
    // NOTE: freelist is in ascending order of keys and pc
    // unuse_space is reserved bytes at the end of page, therefore we must substract from maxpc
    let mut prev_block = None;
    let mut cur_block = match page_ref.first_freeblock() {
        0 => None,
        first_block => Some(first_block as usize),
    };

    let max_start_offset = usable_space - amount;

    while let Some(cur) = cur_block {
        if unlikely(cur + CELL_SIZE_MIN > usable_space) {
            return_corrupt!("Free block header extends beyond page");
        }

        let (next, size) = {
            let cur_u16: u16 = cur
                .try_into()
                .unwrap_or_else(|_| panic!("cur={cur} is too large to fit in a u16"));
            let (next, size) = page_ref.read_freeblock(cur_u16);
            (next as usize, size as usize)
        };

        // Doesn't fit in this freeblock, try the next one.
        if amount > size {
            if next == 0 {
                // No next -> can't fit.
                return Ok(None);
            }

            prev_block = cur_block;
            if unlikely(next <= cur) {
                return_corrupt!("Free list not in ascending order");
            }
            cur_block = Some(next);
            continue;
        }

        let new_size = size - amount;
        // If the freeblock's new size is < CELL_SIZE_MIN, the freeblock is deleted and the remaining bytes
        // become fragmented free bytes.
        if new_size < CELL_SIZE_MIN {
            if page_ref.num_frag_free_bytes() > 57 {
                // SQLite has a fragmentation limit of 60 bytes.
                // check sqlite docs https://www.sqlite.org/fileformat.html#:~:text=A%20freeblock%20requires,not%20exceed%2060
                return Ok(None);
            }
            // Delete the slot from freelist and update the page's fragment count.
            match prev_block {
                Some(prev) => {
                    let prev_u16: u16 = prev
                        .try_into()
                        .unwrap_or_else(|_| panic!("prev={prev} is too large to fit in a u16"));
                    let next_u16: u16 = next
                        .try_into()
                        .unwrap_or_else(|_| panic!("next={next} is too large to fit in a u16"));
                    page_ref.write_freeblock_next_ptr(prev_u16, next_u16);
                }
                None => {
                    let next_u16: u16 = next
                        .try_into()
                        .unwrap_or_else(|_| panic!("next={next} is too large to fit in a u16"));
                    page_ref.write_first_freeblock(next_u16);
                }
            }
            let new_size_u8: u8 = new_size
                .try_into()
                .unwrap_or_else(|_| panic!("new_size={new_size} is too large to fit in a u8"));
            let frag = page_ref.num_frag_free_bytes() + new_size_u8;
            page_ref.write_fragmented_bytes_count(frag);
            return Ok(cur_block);
        } else if unlikely(new_size + cur > max_start_offset) {
            return_corrupt!("Free block extends beyond page end");
        } else {
            // Requested amount fits inside the current free slot so we reduce its size
            // to account for newly allocated space.
            let cur_u16: u16 = cur
                .try_into()
                .unwrap_or_else(|_| panic!("cur={cur} is too large to fit in a u16"));
            let new_size_u16: u16 = new_size
                .try_into()
                .unwrap_or_else(|_| panic!("new_size={new_size} is too large to fit in a u16"));
            page_ref.write_freeblock_size(cur_u16, new_size_u16);
            // Return the offset immediately after the shrunk freeblock.
            return Ok(Some(cur + new_size));
        }
    }

    Ok(None)
}

pub fn btree_init_page(page: &PageRef, page_type: PageType, offset: usize, usable_space: usize) {
    // setup btree page
    let contents = page.get_contents();
    contents.overflow_cells.clear();
    tracing::debug!(
        "btree_init_page(id={}, offset={}, usable_space={})",
        page.get().id(),
        offset,
        usable_space
    );
    #[cfg(debug_assertions)]
    //TODO restore format args (as the "details" last arg)
    turso_assert_eq!(
        offset,
        contents.offset(),
        "offset doesn't match computed offset for page"
    );
    let id = page_type as u8;
    contents.write_page_type(id);
    contents.write_first_freeblock(0);
    contents.write_cell_count(0);

    contents.write_cell_content_area(usable_space);

    contents.write_fragmented_bytes_count(0);
    contents.write_rightmost_ptr(0);

    #[cfg(debug_assertions)]
    {
        // we might get already used page from the pool. generally this is not a problem because
        // b tree access is very controlled. However, for encrypted pages (and also checksums) we want
        // to ensure that there are no reserved bytes that contain old data.
        let buf = contents.as_ptr();
        let buffer_len = buf.len();
        turso_assert!(
            usable_space <= buffer_len,
            "usable_space must be <= buffer_len"
        );
        // this is no op if usable_space == buffer_len
        buf[usable_space..buffer_len].fill(0);
    }
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
    usable_space: usize,
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
        turso_assert_greater_than_or_equal!(count_cells, number_tail_removed);
        count_cells -= number_tail_removed;
        debug_validate_cells!(page, usable_space);
    }
    // TODO: make page_free_array defragment, for now I'm lazy so this will work for now.
    let mut defragmented_page = defragment_page_for_insert(page, usable_space, 0)?;
    // TODO: add to start
    if start_new_cells < start_old_cells {
        let count = number_new_cells.min(start_old_cells - start_new_cells);
        page_insert_array(
            &mut defragmented_page,
            start_new_cells,
            count,
            cell_array,
            0,
            usable_space,
        )?;
        count_cells += count;
    }
    // TODO: overflow cells
    debug_validate_cells!(defragmented_page.0, usable_space);
    for i in 0..defragmented_page.0.overflow_cells.len() {
        let overflow_cell = &defragmented_page.0.overflow_cells[i];
        // cell index in context of new list of cells that should be in the page
        if start_old_cells + overflow_cell.index >= start_new_cells {
            let cell_idx = start_old_cells + overflow_cell.index - start_new_cells;
            if cell_idx < number_new_cells {
                count_cells += 1;
                page_insert_array(
                    &mut defragmented_page,
                    start_new_cells + cell_idx,
                    1,
                    cell_array,
                    cell_idx,
                    usable_space,
                )?;
            }
        }
    }
    debug_validate_cells!(defragmented_page.0, usable_space);
    // TODO: append cells to end
    page_insert_array(
        &mut defragmented_page,
        start_new_cells + count_cells,
        number_new_cells - count_cells,
        cell_array,
        count_cells,
        usable_space,
    )?;
    debug_validate_cells!(defragmented_page.0, usable_space);
    // TODO: noverflow
    page.write_cell_count(number_new_cells as u16);
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
    usable_space: usize,
) -> Result<usize> {
    tracing::debug!("page_free_array {}..{}", first, first + count);
    let buf = &mut page.as_ptr()[page.offset()..usable_space];
    let buf_range = buf.as_ptr_range();
    let mut number_of_cells_removed = 0;
    let mut number_of_cells_buffered = 0;
    let mut buffered_cells_offsets: [usize; 10] = [0; 10];
    let mut buffered_cells_ends: [usize; 10] = [0; 10];
    for i in first..first + count {
        let cell = &cell_array.cell_payloads[i];
        let cell_pointer = cell.as_ptr_range();
        // check if not overflow cell
        if cell_pointer.start >= buf_range.start && cell_pointer.start < buf_range.end {
            turso_assert!(
                cell_pointer.end >= buf_range.start && cell_pointer.end <= buf_range.end,
                "whole cell should be inside the page"
            );
            // TODO: remove pointer too
            let offset = cell_pointer.start as usize - buf_range.start as usize;
            let len = cell_pointer.end as usize - cell_pointer.start as usize;
            turso_assert_greater_than!(len, 0, "cell size should be greater than 0");
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
    page.write_cell_count(page.cell_count() as u16 - number_of_cells_removed as u16);
    Ok(number_of_cells_removed)
}

/// A proof type that guarantees a page has been defragmented.
///
/// This type can only be constructed by calling [`defragment_page_for_insert`],
/// which ensures the page has been defragmented before any insert operations.
/// Functions like [`page_insert_array`] require this type to enforce at compile-time
/// that defragmentation has occurred.
pub struct DefragmentedPage<'a>(&'a mut PageContent);

impl std::ops::Deref for DefragmentedPage<'_> {
    type Target = PageContent;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl std::ops::DerefMut for DefragmentedPage<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

/// Insert multiple cells into a page in a single batch operation.
///
/// This is an optimized version that avoids O(N²) complexity by:
/// 1. Computing total space needed upfront
/// 2. Allocating all space at once
/// 3. Copying all cell payloads sequentially
/// 4. Shifting existing cell pointers once
/// 5. Writing all new cell pointers in one pass
/// 6. Updating cell count once
fn page_insert_array(
    page: &mut DefragmentedPage,
    first: usize,
    count: usize,
    cell_array: &CellArray,
    start_insert: usize,
    _usable_space: usize,
) -> Result<()> {
    if count == 0 {
        return Ok(());
    }

    tracing::debug!(
        "page_insert_array(first={}, count={}, start_insert={}, cell_count={}, page_type={:?})",
        first,
        count,
        start_insert,
        page.cell_count(),
        page.page_type().ok()
    );

    turso_assert!(first <= cell_array.cell_payloads.len(), "first OOB");
    turso_assert!(
        count <= cell_array.cell_payloads.len().saturating_sub(first),
        "first+count OOB"
    );
    // Calculate total space needed for all cell payloads
    // We read from cell_array at indices [first, first+count)
    let mut total_payload_size: usize = 0;
    for i in 0..count {
        let payload = &cell_array.cell_payloads[first + i];
        let cell_size = payload.len().max(MINIMUM_CELL_SIZE);
        total_payload_size += cell_size;
    }

    // Total space needed includes cell pointers
    let total_ptr_space = count.checked_mul(CELL_PTR_SIZE_BYTES).ok_or_else(|| {
        mark_unlikely();
        LimboError::Corrupt("page_insert_array: ptr space overflow".into())
    })?;

    // After defragmentation, all free space is in the unallocated region
    // between the cell pointer array and the cell content area.
    let current_cell_count = page.cell_count();
    let mut cell_content_area = page.cell_content_area() as usize;
    let unallocated_start = page.unallocated_region_start();
    turso_assert!(
        start_insert <= current_cell_count,
        "start_insert beyond cell_count"
    );
    turso_assert!(
        // we cast to u16 later so assert no overflow
        current_cell_count + count <= u16::MAX as usize,
        "cell_count overflow"
    );
    // Verify we have enough space
    // The new cell pointers will extend the cell pointer array by `total_ptr_space`
    // The new cell content will reduce cell_content_area by `total_payload_size`
    let new_unallocated_start =
        unallocated_start
            .checked_add(total_ptr_space)
            .ok_or_else(|| {
                mark_unlikely();
                LimboError::Corrupt("page_insert_array: unalloc start overflow".into())
            })?;
    let new_cell_content_area = cell_content_area
        .checked_sub(total_payload_size)
        .ok_or_else(|| {
            mark_unlikely();
            LimboError::Corrupt("page_insert_array: payload underflow".to_string())
        })?;

    turso_assert!(
        new_unallocated_start <= new_cell_content_area,
        "page_insert_array: not enough space for pointers and payloads in unallocated region",
        { "total_ptr_space": total_ptr_space, "total_payload_size": total_payload_size, "unallocated_start": unallocated_start, "cell_content_area": cell_content_area, "unallocated_region_size": cell_content_area - unallocated_start }
    );

    let buf = page.as_ptr();
    let (cell_pointer_array_start, _) = page.cell_pointer_array_offset_and_size();

    // Shift existing cell pointers to make room for new ones
    // We're inserting `count` cells at position `start_insert`, so we need to shift
    // all cell pointers from position `start_insert` onwards to the right by `count * 2` bytes
    if start_insert < current_cell_count {
        let cells_to_shift = current_cell_count - start_insert;
        let shift_src_start = cell_pointer_array_start + (start_insert * CELL_PTR_SIZE_BYTES);
        let shift_dst_start = shift_src_start + total_ptr_space;
        let shift_size = cells_to_shift * CELL_PTR_SIZE_BYTES;
        buf.copy_within(
            shift_src_start..shift_src_start + shift_size,
            shift_dst_start,
        );
    }

    // Allocate space for all cells and write payloads + pointers
    // We allocate space from the content area (which grows downward)
    // Read from cell_array[first..first+count], insert at page positions [start_insert..start_insert+count]
    for i in 0..count {
        let payload = &cell_array.cell_payloads[first + i];
        let cell_size = payload.len().max(MINIMUM_CELL_SIZE);

        // Allocate space for this cell (grow content area downward)
        cell_content_area = cell_content_area.checked_sub(cell_size).ok_or_else(|| {
            mark_unlikely();
            LimboError::Corrupt("page_insert_array: cell allocation underflow".to_string())
        })?;

        // Copy cell payload
        buf[cell_content_area..cell_content_area + payload.len()].copy_from_slice(payload);

        // Write cell pointer at position (start_insert + i)
        let ptr_offset = cell_pointer_array_start + ((start_insert + i) * CELL_PTR_SIZE_BYTES);
        page.write_u16_no_offset(ptr_offset, cell_content_area as u16);
    }

    // Update page header
    page.write_cell_content_area(cell_content_area);
    page.write_cell_count((current_cell_count + count) as u16);

    debug_validate_cells!(page, _usable_space);
    Ok(())
}

/// Free the range of bytes that a cell occupies.
/// This function also updates the freeblock list in the page.
/// Freeblocks are used to keep track of free space in the page,
/// and are organized as a linked list.
///
/// This function may merge the freed cell range into either the next freeblock,
/// previous freeblock, or both.
fn free_cell_range(
    page: &mut PageContent,
    mut offset: usize,
    len: usize,
    usable_space: usize,
) -> Result<()> {
    const CELL_SIZE_MIN: usize = 4;
    if unlikely(len < CELL_SIZE_MIN) {
        return_corrupt!("free_cell_range: minimum cell size is {CELL_SIZE_MIN}");
    }
    if unlikely(offset > usable_space.saturating_sub(CELL_SIZE_MIN)) {
        return_corrupt!("free_cell_range: start offset beyond usable space: offset={offset} usable_space={usable_space}");
    }

    let mut size = len;
    let mut end = offset + len;
    if unlikely(end > usable_space) {
        return_corrupt!("free_cell_range: freed range extends beyond usable space: offset={offset} len={len} end={end} usable_space={usable_space}");
    }
    let cur_content_area = page.cell_content_area() as usize;
    let first_block = page.first_freeblock() as usize;
    if first_block == 0 {
        if unlikely(offset < cur_content_area) {
            return_corrupt!("free_cell_range: free block before content area: offset={offset} cell_content_area={cur_content_area}");
        }
        if offset == cur_content_area {
            // if the freeblock list is empty and the freed range is exactly at the beginning of the content area,
            // we are not creating a freeblock; instead we are just extending the unallocated region.
            page.write_cell_content_area(end);
        } else {
            // otherwise we set it as the first freeblock in the page header.
            let offset_u16: u16 = offset
                .try_into()
                .unwrap_or_else(|_| panic!("offset={offset} is too large to fit in a u16"));
            page.write_first_freeblock(offset_u16);
            let size_u16: u16 = size
                .try_into()
                .unwrap_or_else(|_| panic!("size={size} is too large to fit in a u16"));
            page.write_freeblock(offset_u16, size_u16, None);
        }
        return Ok(());
    }

    // if the freeblock list is not empty, we need to find the correct position to insert the new freeblock
    // resulting from the freeing of this cell range; we may be also able to merge the freed range into existing freeblocks.
    let mut prev_block = None;
    let mut next_block = Some(first_block);

    while let Some(next) = next_block {
        if unlikely(prev_block.is_some_and(|prev| next <= prev)) {
            return_corrupt!("free_cell_range: freeblocks not in ascending order: next_block={next} prev_block={prev_block:?}");
        }
        if next >= offset {
            break;
        }
        prev_block = Some(next);
        next_block = match page.read_u16_no_offset(next) {
            // Freed range extends beyond the last freeblock, so we are creating a new freeblock.
            0 => None,
            next => Some(next as usize),
        };
    }

    if let Some(next) = next_block {
        if unlikely(next + CELL_SIZE_MIN > usable_space) {
            return_corrupt!("free_cell_range: free block beyond usable space: next_block={next} usable_space={usable_space}");
        }
    }
    let mut removed_fragmentation = 0;
    const SINGLE_FRAGMENT_SIZE_MAX: usize = CELL_SIZE_MIN - 1;

    // If the freed range extends into the next freeblock, we will merge the freed range into it.
    // If there is a 1-3 byte gap between the freed range and the next freeblock, we are effectively
    // clearing that amount of fragmented bytes, since a 1-3 byte range cannot be a valid cell.
    if let Some(next) = next_block {
        if end + SINGLE_FRAGMENT_SIZE_MAX >= next {
            if unlikely(end > next) {
                return_corrupt!("free_cell_range: freed range overlaps next freeblock: end={end} next_block={next}");
            }
            removed_fragmentation = (next - end) as u8;
            let next_size = page.read_u16_no_offset(next + 2) as usize;
            end = next + next_size;
            if unlikely(end > usable_space) {
                return_corrupt!("free_cell_range: coalesced block extends beyond page: offset={offset} len={len} end={end} usable_space={usable_space}");
            }
            size = end - offset;
            // Since we merged the two freeblocks, we need to update the next_block to the next freeblock in the list.
            next_block = match page.read_u16_no_offset(next) {
                0 => None,
                next => Some(next as usize),
            };
        }
    }

    // If the freed range extends into the previous freeblock, we will merge them similarly as above.
    if let Some(prev) = prev_block {
        let prev_size = page.read_u16_no_offset(prev + 2) as usize;
        let prev_end = prev + prev_size;
        if unlikely(prev_end > offset) {
            return_corrupt!(
                "free_cell_range: previous block overlap: prev_end={prev_end} offset={offset}"
            );
        }
        // If the previous freeblock extends into the freed range, we will merge the freed range into the
        // previous freeblock and clear any 1-3 byte fragmentation in between, similarly as above
        if prev_end + SINGLE_FRAGMENT_SIZE_MAX >= offset {
            removed_fragmentation += (offset - prev_end) as u8;
            size = end - prev;
            offset = prev;
        }
    }

    let cur_frag_free_bytes = page.num_frag_free_bytes();
    if unlikely(removed_fragmentation > cur_frag_free_bytes) {
        return_corrupt!("free_cell_range: invalid fragmentation count: removed_fragmentation={removed_fragmentation} num_frag_free_bytes={cur_frag_free_bytes}");
    }
    let frag = cur_frag_free_bytes - removed_fragmentation;
    page.write_fragmented_bytes_count(frag);

    if unlikely(offset < cur_content_area) {
        return_corrupt!("free_cell_range: free block before content area: offset={offset} cell_content_area={cur_content_area}");
    }

    // As above, if the freed range is exactly at the beginning of the content area, we are not creating a freeblock;
    // instead we are just extending the unallocated region.
    if offset == cur_content_area {
        if unlikely(prev_block.is_some_and(|prev| prev != first_block)) {
            return_corrupt!("free_cell_range: invalid content area merge - freed range should have been merged with previous freeblock: prev={prev_block:?} first_block={first_block}");
        }
        // If we get here, we are freeing data from the left end of the content area,
        // so we are extending the unallocated region instead of creating a freeblock.
        // We update the first freeblock to be the next one, and shrink the content area to start from the end
        // of the freed range.
        match next_block {
            Some(next) => {
                if unlikely(next <= end) {
                    return_corrupt!("free_cell_range: invalid content area merge - first freeblock should either be 0 or greater than the content area start: next_block={next} end={end}");
                }
                let next_u16: u16 = next
                    .try_into()
                    .unwrap_or_else(|_| panic!("next={next} is too large to fit in a u16"));
                page.write_first_freeblock(next_u16);
            }
            None => {
                page.write_first_freeblock(0);
            }
        }
        page.write_cell_content_area(end);
    } else {
        // If we are creating a new freeblock:
        // a) if it's the first one, we update the header to indicate so,
        // b) if it's not the first one, we update the previous freeblock to point to the new one,
        //    and the new one to point to the next one.
        let offset_u16: u16 = offset
            .try_into()
            .unwrap_or_else(|_| panic!("offset={offset} is too large to fit in a u16"));
        if let Some(prev) = prev_block {
            page.write_u16_no_offset(prev, offset_u16);
        } else {
            page.write_first_freeblock(offset_u16);
        }
        let size_u16: u16 = size
            .try_into()
            .unwrap_or_else(|_| panic!("size={size} is too large to fit in a u16"));
        let next_block_u16 = next_block.map(|b| {
            b.try_into()
                .unwrap_or_else(|_| panic!("next_block={b} is too large to fit in a u16"))
        });
        page.write_freeblock(offset_u16, size_u16, next_block_u16);
    }

    Ok(())
}

/// This function handles pages with two or fewer freeblocks and max_frag_bytes (parameter to defragment_page())
/// or fewer fragmented bytes. In this case it is faster to move the two (or one)
/// blocks of cells using memmove() and add the required offsets to each pointer
/// in the cell-pointer array than it is to reconstruct the entire page.
/// Note that this function will leave max_frag_bytes as is, it will not try to reduce it.
fn defragment_page_fast(
    page: &PageContent,
    usable_space: usize,
    freeblock_1st: usize,
    freeblock_2nd: usize,
) -> Result<()> {
    if unlikely(freeblock_1st == 0) {
        return_corrupt!("defragment_page_fast: expected at least one freeblock");
    }
    if unlikely(freeblock_2nd > 0 && freeblock_1st >= freeblock_2nd) {
        return_corrupt!(
            "defragment_page_fast: first freeblock must be before second freeblock: freeblock_1st={freeblock_1st} freeblock_2nd={freeblock_2nd}"
        );
    }
    const FREEBLOCK_SIZE_MIN: usize = 4;
    if unlikely(freeblock_1st > usable_space - FREEBLOCK_SIZE_MIN) {
        return_corrupt!(
            "defragment_page_fast: first freeblock beyond usable space: freeblock_1st={freeblock_1st} usable_space={usable_space}"
        );
    }
    if unlikely(freeblock_2nd > usable_space - FREEBLOCK_SIZE_MIN) {
        return_corrupt!(
            "defragment_page_fast: second freeblock beyond usable space: freeblock_2nd={freeblock_2nd} usable_space={usable_space}"
        );
    }

    let freeblock_1st_size = page.read_u16_no_offset(freeblock_1st + 2) as usize;
    let freeblock_2nd_size = if freeblock_2nd > 0 {
        page.read_u16_no_offset(freeblock_2nd + 2) as usize
    } else {
        0
    };
    let freeblocks_total_size = freeblock_1st_size + freeblock_2nd_size;

    let cell_content_area = page.cell_content_area() as usize;

    if freeblock_2nd > 0 {
        // If there's 2 freeblocks, merge them into one first.
        if unlikely(freeblock_1st + freeblock_1st_size > freeblock_2nd) {
            return_corrupt!(
                "defragment_page_fast: overlapping freeblocks: freeblock_1st={freeblock_1st} freeblock_1st_size={freeblock_1st_size} freeblock_2nd={freeblock_2nd}"
            );
        }
        if unlikely(freeblock_2nd + freeblock_2nd_size > usable_space) {
            return_corrupt!(
                "defragment_page_fast: second freeblock extends beyond usable space: freeblock_2nd={freeblock_2nd} freeblock_2nd_size={freeblock_2nd_size} usable_space={usable_space}"
            );
        }
        let buf = page.as_ptr();
        // Effectively moves everything in between the two freeblocks rightwards by the length of the 2nd freeblock,
        // so that the first freeblock size becomes `freeblocks_total_size` (merging the two freeblocks)
        // and the second freeblock gets overwritten by non-free cell data.
        // Illustrative doodle:
        // | content area start |--cell content A--| 1st free |--cell content B--| 2nd free |--cell content C--|
        // ->
        // | content area start |--cell content A--|      merged free    |--cell content B--|--cell content C--|
        let after_first_freeblock = freeblock_1st + freeblock_1st_size;
        let copy_amount = freeblock_2nd - after_first_freeblock;
        buf.copy_within(
            after_first_freeblock..after_first_freeblock + copy_amount,
            freeblock_1st + freeblocks_total_size,
        );
    } else if unlikely(freeblock_1st + freeblock_1st_size > usable_space) {
        return_corrupt!(
            "defragment_page_fast: first freeblock extends beyond usable space: freeblock_1st={freeblock_1st} freeblock_1st_size={freeblock_1st_size} usable_space={usable_space}"
        );
    }

    // Now we have one freeblock somewhere in the middle of the content area, e.g.:
    // content area start |-----------| merged freeblock |-----------|
    // By moving the cells from the left of the merged free block to where the merged freeblock was, we effectively move the freeblock to the very left end of the content area,
    // meaning, it's no longer a freeblock, it's just plain old free space.
    // content area start | free space | ----------- cells ----------|
    let new_cell_content_area = cell_content_area + freeblocks_total_size;
    if unlikely(new_cell_content_area + (freeblock_1st - cell_content_area) > usable_space) {
        return_corrupt!(
            "defragment_page_fast: new cell content area extends beyond usable space: new_cell_content_area={new_cell_content_area} freeblock_1st={freeblock_1st} cell_content_area={cell_content_area} usable_space={usable_space}"
        );
    }

    let copy_amount = freeblock_1st - cell_content_area; // cells to the left of the first freeblock
    let buf = page.as_ptr();
    buf.copy_within(
        cell_content_area..cell_content_area + copy_amount,
        new_cell_content_area,
    );

    // Freeblocks are now erased since the free space is at the beginning, but we must update the cell pointer array to point to the right locations.
    let cell_count = page.cell_count();
    let cell_pointer_array_offset = page.cell_pointer_array_offset_and_size().0;
    for i in 0..cell_count {
        let ptr_offset = cell_pointer_array_offset + (i * CELL_PTR_SIZE_BYTES);
        let cell_ptr = page.read_u16_no_offset(ptr_offset) as usize;
        if cell_ptr < freeblock_1st {
            // If the cell pointer was located before the first freeblock, we need to shift it right by the size of the merged freeblock
            // since the space occupied by both the 1st and 2nd freeblocks was now moved to its left.
            let new_offset = cell_ptr + freeblocks_total_size;
            if unlikely(new_offset > usable_space) {
                return_corrupt!(
                    "defragment_page_fast: shifted cell pointer beyond usable space: new_offset={new_offset} usable_space={usable_space}"
                );
            }
            page.write_u16_no_offset(ptr_offset, (cell_ptr + freeblocks_total_size) as u16);
        } else if freeblock_2nd > 0 && cell_ptr < freeblock_2nd {
            // If the cell pointer was located between the first and second freeblock, we need to shift it right by the size of only the second freeblock,
            // since the first one was already on its left.
            let new_offset = cell_ptr + freeblock_2nd_size;
            if unlikely(new_offset > usable_space) {
                return_corrupt!(
                    "defragment_page_fast: shifted cell pointer beyond usable space: new_offset={new_offset} usable_space={usable_space}"
                );
            }
            page.write_u16_no_offset(ptr_offset, (cell_ptr + freeblock_2nd_size) as u16);
        }
    }

    // Update page header
    page.write_cell_content_area(new_cell_content_area);
    page.write_first_freeblock(0);

    debug_validate_cells!(page, usable_space);

    Ok(())
}

/// Defragment a page, and never use the fast-path algorithm.
fn defragment_page_full(page: &PageContent, usable_space: usize) -> Result<()> {
    defragment_page(page, usable_space, -1)
}

/// Defragment a page and return a proof that can be used with [`page_insert_array`].
///
/// This is the entry point for defragmentation when you need to perform insert
/// operations afterward. The returned [`DefragmentedPage`] proves at compile-time
/// that defragmentation has occurred.
///
/// For defragmentation without the type-state proof (e.g., in `allocate_cell_space`),
/// use [`defragment_page`] directly.
#[inline]
fn defragment_page_for_insert(
    page: &mut PageContent,
    usable_space: usize,
    max_frag_bytes: isize,
) -> Result<DefragmentedPage<'_>> {
    defragment_page(page, usable_space, max_frag_bytes)?;
    Ok(DefragmentedPage(page))
}

/// Defragment a page. This means packing all the cells to the end of the page.
fn defragment_page(page: &PageContent, usable_space: usize, max_frag_bytes: isize) -> Result<()> {
    debug_validate_cells!(page, usable_space);
    tracing::debug!("defragment_page (optimized in-place)");

    let cell_count = page.cell_count();
    if cell_count == 0 {
        page.write_cell_content_area(usable_space);
        page.write_first_freeblock(0);
        page.write_fragmented_bytes_count(0);
        debug_validate_cells!(page, usable_space);
        return Ok(());
    }

    // Use fast algorithm if there are at most 2 freeblocks and the total fragmented free space is less than max_frag_bytes.
    if page.num_frag_free_bytes() as isize <= max_frag_bytes {
        let freeblock_1st = page.first_freeblock() as usize;
        if freeblock_1st == 0 {
            // No freeblocks and very little if any fragmented free bytes -> no need to defragment.
            return Ok(());
        }
        let freeblock_2nd = page.read_u16_no_offset(freeblock_1st) as usize;
        if freeblock_2nd == 0 {
            return defragment_page_fast(page, usable_space, freeblock_1st, 0);
        }
        let freeblock_3rd = page.read_u16_no_offset(freeblock_2nd) as usize;
        if freeblock_3rd == 0 {
            return defragment_page_fast(page, usable_space, freeblock_1st, freeblock_2nd);
        }
    }

    // A small struct to hold cell metadata for sorting.
    // Size: 2 + 2 + 8 = 12 bytes, with alignment likely 16 bytes.
    #[derive(Clone, Copy)]
    struct CellInfo {
        old_offset: u16,
        size: u16,
        pointer_index: usize,
    }

    // Use stack allocation for the common case (most pages have <256 cells).
    // This avoids heap allocation in the hot path.
    // MAX_STACK_CELLS * 16 bytes = 4KB of stack space.
    const MAX_STACK_CELLS: usize = 256;

    // Helper function to process cells and defragment the page.
    // This is generic over the slice type to work with both stack and heap storage.
    // Cells must already be sorted by old physical offset in descending order.
    #[inline]
    fn process_cells(page: &PageContent, usable_space: usize, cells: &[CellInfo]) -> Result<()> {
        // Get direct mutable access to the page buffer.
        let buffer = page.as_ptr();
        let cell_pointer_area_offset = page.cell_pointer_array_offset();
        let first_cell_content_byte = page.unallocated_region_start();

        // Move data and update pointers.
        let mut cbrk = usable_space;
        for cell in cells.iter() {
            cbrk -= cell.size as usize;
            let new_offset = cbrk;
            let old_offset = cell.old_offset as usize;

            // Basic corruption check
            turso_assert!(
                new_offset >= first_cell_content_byte && old_offset + cell.size as usize <= usable_space,
                "corrupt page detected during defragmentation",
                { "new_offset": new_offset, "first_cell_content_byte": first_cell_content_byte, "old_offset": old_offset, "cell_size": cell.size, "usable_space": usable_space }
            );

            // Move the cell data. `copy_within` is the idiomatic and safe
            // way to perform a `memmove` operation on a slice.
            if new_offset != old_offset {
                let src_range = old_offset..(old_offset + cell.size as usize);
                buffer.copy_within(src_range, new_offset);
            }

            // Update the pointer in the cell pointer array to the new offset.
            let pointer_location = cell_pointer_area_offset + (cell.pointer_index * 2);
            turso_assert!(
                new_offset < PageSize::MAX as usize,
                "new_offset exceeds PageSize::MAX",
                { "new_offset": new_offset, "page_size_max": PageSize::MAX }
            );
            page.write_u16_no_offset(pointer_location, new_offset as u16);
        }

        page.write_cell_content_area(cbrk);
        page.write_first_freeblock(0);
        page.write_fragmented_bytes_count(0);
        Ok(())
    }

    // Gather cell metadata.
    let cell_offset = page.cell_pointer_array_offset();
    let mut is_physically_sorted = true;
    let mut last_offset = u16::MAX;

    // Pre-compute page-level constants for cell_get_raw_region_faster.
    // These are the same for all cells on the page, so computing them once
    // avoids redundant work in the loop.
    let page_type = page.page_type()?;
    let max_local = payload_overflow_threshold_max(page_type, usable_space);
    let min_local = payload_overflow_threshold_min(page_type, usable_space);

    let mut cells = SmallVec::<[CellInfo; MAX_STACK_CELLS]>::with_capacity(cell_count);
    for i in 0..cell_count {
        let pc = page.read_u16_no_offset(cell_offset + (i * 2));
        let (_, size) = page._cell_get_raw_region_faster(
            i,
            usable_space,
            cell_count,
            max_local,
            min_local,
            page_type,
        )?;

        if pc > last_offset {
            is_physically_sorted = false;
        }
        last_offset = pc;

        cells.push(CellInfo {
            old_offset: pc,
            size: size as u16,
            pointer_index: i,
        });
    }

    // Sort once, descending by old physical offset — the order the copy
    // loop in `process_cells` needs. Using unstable sort is fine as the
    // original order doesn't matter.
    if !is_physically_sorted {
        cells.sort_unstable_by(|a, b| b.old_offset.cmp(&a.old_offset));
    }

    // Cells below the pointer array or overlapping each other mean the
    // page is corrupt (each region was individually bounds-checked above).
    // Moving them would compound the damage: defragmentation interleaves
    // pointer-array writes with cell copies, so a cell sourced from inside
    // the pointer array — or from another cell's region — gets its bytes
    // rewritten before or while it is copied. Walking the descending sort
    // in reverse checks the regions in ascending offset order.
    let first_allowed_cell_offset = cell_offset + 2 * cell_count;
    let mut previous_end = first_allowed_cell_offset;
    for cell in cells.iter().rev() {
        if (cell.old_offset as usize) < previous_end {
            return Err(LimboError::Corrupt(format!(
                "page has overlapping or misplaced cells at offset {}",
                cell.old_offset
            )));
        }
        previous_end = cell.old_offset as usize + cell.size as usize;
    }

    process_cells(page, usable_space, &cells)?;
    debug_validate_cells!(page, usable_space);
    Ok(())
}

#[cfg(debug_assertions)]
/// Only enabled in debug mode, where we ensure that all cells are valid.
fn debug_validate_cells_core(page: &PageContent, usable_space: usize) {
    let pointer_array_end = page.offset() + page.header_size() + 2 * page.cell_count();
    turso_assert_greater_than_or_equal!(
        page.cell_content_area() as usize,
        pointer_array_end,
        "cell content area overlaps cell pointer array"
    );
    for i in 0..page.cell_count() {
        let (offset, size) = page.cell_get_raw_region(i, usable_space).unwrap();
        turso_assert_greater_than_or_equal!(
            offset,
            pointer_array_end,
            "cell overlaps cell pointer array",
            { "idx": i }
        );
        let _buf = &page.as_ptr()[offset..offset + size];
        // E.g. the following table btree cell may just have two bytes:
        // Payload size 0 (stored as SerialTypeKind::ConstInt0)
        // Rowid 1 (stored as SerialTypeKind::ConstInt1)
        turso_assert_greater_than_or_equal!(
            size, 2,
            "cell size should be at least 2 bytes",
            { "idx": i, "offset": offset, "buf": _buf }
        );
        if page.is_leaf() {
            turso_assert!(page.as_ptr()[offset] != 0);
        }
        turso_assert_less_than_or_equal!(
            offset + size,
            usable_space,
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
    usable_space: usize,
    allow_regular_insert_despite_overflow: bool, // used during balancing to allow regular insert despite overflow cells
) -> Result<()> {
    turso_assert_less_than_or_equal!(
        cell_idx, page.cell_count() + page.overflow_cells.len(),
        "attempting to add cell to incorrect place",
        { "cell_idx": cell_idx, "cell_count": page.cell_count(), "overflow_count": page.overflow_cells.len(), "page_type": format!("{:?}", page.page_type()) }
    );
    let already_has_overflow = !page.overflow_cells.is_empty();
    let free = compute_free_space(page, usable_space)?;
    let enough_space = if already_has_overflow && !allow_regular_insert_despite_overflow {
        false
    } else {
        // otherwise, we need to check if we have enough space.
        // allocate_cell_space() never allocates less than MINIMUM_CELL_SIZE
        // bytes, so a smaller payload must reserve that much or the cell
        // content area slides into the cell pointer array.
        payload.len().max(MINIMUM_CELL_SIZE) + CELL_PTR_SIZE_BYTES <= free
    };
    if !enough_space {
        #[cfg(debug_assertions)]
        {
            if let Some(overflow_cell) = page.overflow_cells.last() {
                turso_assert!(overflow_cell.index + 1 == cell_idx, "multiple overflow cells can only occur when a parent overflows during balancing as divider cells are inserted into it. those cells should always be in-order and sequential", { "page_id": page.id(), "last_overflow_index": overflow_cell.index, "cell_idx": cell_idx, "cell_count": page.cell_count(), "overflow_count": page.overflow_cells.len() });
            }
        }
        let mut payload = crate::with_btree_allocation_site!(OverflowCell, payload.try_to_vec())?;
        if page.page_type()? == PageType::IndexLeaf {
            // Balancing must see one size for every leaf cell, whether it sits on the page
            // (where cell_get_raw_region reports at least the minimum size) or overflowed.
            ensure_min_cell_size(&mut payload, 0);
        }
        let overflow_cell = OverflowCell {
            index: cell_idx,
            payload: Pin::new(payload),
        };
        crate::with_btree_allocation_site!(
            OverflowCell,
            page.overflow_cells.try_push(overflow_cell)
        )?;
        return Ok(());
    }
    turso_assert_less_than_or_equal!(
        cell_idx,
        page.cell_count(),
        "cell_idx > cell_count without overflow cells"
    );

    let new_cell_data_pointer = allocate_cell_space(page, payload.len(), usable_space, free)?;
    tracing::debug!(
        "insert_into_cell(idx={}, pc={}, size={})",
        cell_idx,
        new_cell_data_pointer,
        payload.len()
    );
    turso_assert_less_than_or_equal!(new_cell_data_pointer as usize + payload.len(), usable_space);
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
    page.write_cell_count(new_n_cells);
    debug_validate_cells!(page, usable_space);
    Ok(())
}

fn insert_into_cell(
    page: &mut PageContent,
    payload: &[u8],
    cell_idx: usize,
    usable_space: usize,
) -> Result<()> {
    _insert_into_cell(page, payload, cell_idx, usable_space, false)
}

/// Normally in [insert_into_cell()], if a page already has overflow cells, all
/// new insertions are also added to the overflow cells vector.
/// The amount of free space is the sum of:
///  #1. The size of the unallocated region
///  #2. Fragments (isolated 1-3 byte chunks of free space within the cell content area)
///  #3. freeblocks (linked list of blocks of at least 4 bytes within the cell content area that
///      are not in use due to e.g. deletions)
/// Free blocks can be zero, meaning the "real free space" that can be used to allocate is expected
/// to be between first cell byte and end of cell pointer area.
#[allow(unused_assignments)]
#[inline(always)]
fn compute_free_space(page: &PageContent, usable_space: usize) -> Result<usize> {
    // TODO(pere): maybe free space is not calculated correctly with offset

    // Usable space, not the same as free space, simply means:
    // space that is not reserved for extensions by sqlite. Usually reserved_space is 0.

    let first_cell = page.offset() + page.header_size() + (2 * page.cell_count());
    if unlikely(first_cell > usable_space) {
        return_corrupt!(
            "compute_free_space: first_cell beyond usable space: first_cell={first_cell} usable_space={usable_space}"
        );
    }

    let cell_content_area_start = page.cell_content_area() as usize;
    if unlikely(cell_content_area_start > usable_space) {
        return_corrupt!(
            "compute_free_space: cell content area beyond usable space: cell_content_area_start={cell_content_area_start} usable_space={usable_space}"
        );
    }

    let mut free_space_bytes = cell_content_area_start + page.num_frag_free_bytes() as usize;

    // #3 is computed by iterating over the freeblocks linked list
    let mut cur_freeblock_ptr = page.first_freeblock() as usize;
    if cur_freeblock_ptr > 0 {
        if unlikely(cur_freeblock_ptr < cell_content_area_start) {
            return_corrupt!(
                "compute_free_space: first freeblock before content area: first_freeblock={cur_freeblock_ptr} cell_content_area_start={cell_content_area_start}"
            );
        }

        let mut next = 0usize;
        let mut size = 0usize;
        loop {
            if unlikely(cur_freeblock_ptr + 4 > usable_space) {
                return_corrupt!(
                    "compute_free_space: freeblock header out of bounds: cur_freeblock_ptr={cur_freeblock_ptr} usable_space={usable_space}"
                );
            }
            next = page.read_u16_no_offset(cur_freeblock_ptr) as usize; // first 2 bytes in freeblock = next freeblock pointer
            size = page.read_u16_no_offset(cur_freeblock_ptr + 2) as usize; // next 2 bytes in freeblock = size of current freeblock
            if unlikely(size < 4) {
                return_corrupt!(
                    "compute_free_space: freeblock too small: cur_freeblock_ptr={cur_freeblock_ptr} size={size}"
                );
            }
            if unlikely(cur_freeblock_ptr + size > usable_space) {
                return_corrupt!(
                    "compute_free_space: freeblock extends beyond page: cur_freeblock_ptr={cur_freeblock_ptr} size={size} usable_space={usable_space}"
                );
            }
            free_space_bytes += size;

            if next == 0 {
                break;
            }
            // Freeblocks are in order from left to right on the page.
            if unlikely(next <= cur_freeblock_ptr + size + 3) {
                return_corrupt!(
                    "compute_free_space: freeblocks list not in ascending order: cur_freeblock_ptr={cur_freeblock_ptr} size={size} next={next}"
                );
            }
            cur_freeblock_ptr = next;
        }
    }

    if unlikely(free_space_bytes > usable_space) {
        return_corrupt!(
            "compute_free_space: free space greater than usable space: free_space_bytes={free_space_bytes} usable_space={usable_space}"
        );
    }
    if unlikely(free_space_bytes < first_cell) {
        return_corrupt!(
            "compute_free_space: free space underflow: free_space_bytes={free_space_bytes} first_cell={first_cell} usable_space={usable_space}"
        );
    }

    Ok(free_space_bytes - first_cell)
}

/// Allocate space for a cell on a page.
#[inline]
fn allocate_cell_space(
    page_ref: &PageContent,
    mut amount: usize,
    usable_space: usize,
    free_space: usize,
) -> Result<u16> {
    if amount < MINIMUM_CELL_SIZE {
        amount = MINIMUM_CELL_SIZE;
    }

    let unallocated_region_start = page_ref.unallocated_region_start();
    let mut cell_content_area_start = page_ref.cell_content_area() as usize;

    // there are free blocks and enough space to fit a new 2-byte cell pointer
    if page_ref.first_freeblock() != 0
        && unallocated_region_start + CELL_PTR_SIZE_BYTES <= cell_content_area_start
    {
        // find slot
        if let Some(pc) = find_free_slot(page_ref, usable_space, amount)? {
            // we can fit the cell in a freeblock.
            return Ok(pc as u16);
        }
        /* fall through, we might need to defragment */
    }

    // We know at this point that we have no freeblocks in the middle of the cell content area
    // that can fit the cell, but we do know we have enough space to _somehow_ fit it.
    // The check below sees whether we can just put the cell in the unallocated region.
    if unallocated_region_start + CELL_PTR_SIZE_BYTES + amount > cell_content_area_start {
        // There's no room in the unallocated region, so we need to defragment.
        // max_frag_bytes is a parameter to defragment_page() that controls whether we are able to use
        // the fast-path defragmentation. The calculation here is done to see whether we can merge 1-2 freeblocks
        // and move them to the unallocated region and fit the cell that way.
        // Basically: if we have exactly enough space for the cell and the cell pointer on the page,
        // we cannot have any fragmented space because then the freeblocks would not fit the cell.
        let max_frag_bytes = 4.min(free_space as isize - (CELL_PTR_SIZE_BYTES + amount) as isize);
        defragment_page(page_ref, usable_space, max_frag_bytes)?;
        cell_content_area_start = page_ref.cell_content_area() as usize;
    }

    // insert the cell -> content area start moves left by that amount.
    cell_content_area_start -= amount;
    page_ref.write_cell_content_area(cell_content_area_start);

    turso_assert_less_than_or_equal!(cell_content_area_start + amount, usable_space);
    // we can just return the start of the cell content area, since the cell is inserted to the very left of the cell content area.
    Ok(cell_content_area_start as u16)
}

#[derive(Debug, Clone)]
pub enum FillCellPayloadState {
    /// Determine whether we can fit the record on the current page.
    /// If yes, return immediately after copying the data.
    /// Otherwise move to [CopyData] state.
    Start,
    /// Copy the next chunk of data from the record buffer to the cell payload.
    /// If we can't fit all of the remaining data on the current page,
    /// move the internal state to [CopyDataState::AllocateOverflowPage]
    CopyData {
        /// Internal state of the copy data operation.
        /// We can either be copying data or allocating an overflow page.
        state: CopyDataState,
        /// Track how much space we have left on the current page we are copying data into.
        /// This is reset whenever a new overflow page is allocated.
        space_left_on_cur_page: usize,
        /// Offset into the record buffer to copy from.
        src_data_offset: usize,
        /// Offset into the destination buffer we are copying data into.
        /// This is either:
        /// - an offset in the btree page where the cell is, or
        /// - an offset in an overflow page
        dst_data_offset: usize,
        /// If this is Some, we will copy data into this overflow page.
        /// If this is None, we will copy data into the cell payload on the btree page.
        /// Also: to safely form a chain of overflow pages, the current page must be pinned to the page cache
        /// so that e.g. a spilling operation does not evict it to disk.
        current_overflow_page: Option<PinGuard>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum CopyDataState {
    /// Copy the next chunk of data from the record buffer to the cell payload.
    Copy,
    /// Allocate a new overflow page if we couldn't fit all data to the current page.
    AllocateOverflowPage,
}

/// Fill in the cell payload with the record.
/// If the record is too large to fit in the cell, it will spill onto overflow pages.
/// This function needs a separate [FillCellPayloadState] because allocating overflow pages
/// may require I/O.
#[allow(clippy::too_many_arguments)]
fn fill_cell_payload(
    page: &PinGuard,
    int_key: Option<i64>,
    cell_payload: &mut crate::alloc::Vec<u8>,
    cell_idx: usize,
    record: &impl AsRef<[u8]>,
    usable_space: usize,
    pager: &Pager,
    fill_cell_payload_state: &mut FillCellPayloadState,
) -> IOResultOr<()> {
    let overflow_page_pointer_size = 4;
    let overflow_page_data_size = usable_space - overflow_page_pointer_size;
    let result = loop {
        let record_buf = record.as_ref();
        match fill_cell_payload_state {
            FillCellPayloadState::Start => {
                let page_contents = page.get_contents();

                let page_type = page_contents.page_type()?;
                // fill in header
                if matches!(page_type, PageType::IndexInterior) {
                    // if a write happened on an index interior page, it is always an overwrite.
                    // we must copy the left child pointer of the replaced cell to the new cell.
                    let left_child_page =
                        page_contents.cell_interior_read_left_child_page(cell_idx)?;
                    crate::with_btree_allocation_site!(
                        CellPayload,
                        cell_payload.try_extend(left_child_page.to_be_bytes())
                    )?;
                }
                if matches!(page_type, PageType::TableLeaf) {
                    let int_key = int_key.unwrap();
                    write_varint_to_vec(record_buf.len() as u64, cell_payload)?;
                    write_varint_to_vec(int_key as u64, cell_payload)?;
                } else {
                    write_varint_to_vec(record_buf.len() as u64, cell_payload)?;
                }

                let max_local = payload_overflow_threshold_max(page_type, usable_space);
                let min_local = payload_overflow_threshold_min(page_type, usable_space);

                let Some(local_payload_size) =
                    payload_overflows(record_buf.len(), max_local, min_local, usable_space)
                else {
                    // enough allowed space to fit inside a btree page
                    crate::with_btree_allocation_site!(
                        CellPayload,
                        cell_payload.try_reserve(record_buf.len())
                    )?;
                    cell_payload.extend_from_slice(record_buf);
                    break Ok(IOResult::Done(()));
                };

                // so far we've written any of: left child page, rowid, payload size (depending on page type)
                let cell_non_payload_elems_size = cell_payload.len();
                let new_total_local_size = cell_non_payload_elems_size + local_payload_size;
                crate::with_btree_allocation_site!(
                    CellPayload,
                    cell_payload.try_reserve(new_total_local_size - cell_payload.len())
                )?;
                cell_payload.resize(new_total_local_size, 0);

                *fill_cell_payload_state = FillCellPayloadState::CopyData {
                    state: CopyDataState::Copy,
                    space_left_on_cur_page: local_payload_size - overflow_page_pointer_size,
                    src_data_offset: 0,
                    dst_data_offset: cell_non_payload_elems_size,
                    current_overflow_page: None,
                };
                continue;
            }
            FillCellPayloadState::CopyData {
                state,
                src_data_offset,
                space_left_on_cur_page,
                dst_data_offset,
                current_overflow_page,
            } => {
                match state {
                    CopyDataState::Copy => {
                        turso_assert!(*src_data_offset < record_buf.len(), "trying to read past end of record buffer", { "src_data_offset": src_data_offset, "record_buf_len": record_buf.len() });
                        let record_offset_slice = &record_buf[*src_data_offset..];
                        let amount_to_copy =
                            (*space_left_on_cur_page).min(record_offset_slice.len());
                        let record_offset_slice_to_copy = &record_offset_slice[..amount_to_copy];
                        if let Some(cur_page) = current_overflow_page {
                            // Copy data into the current overflow page.
                            turso_assert!(
                                cur_page.is_loaded(),
                                "current overflow page is not loaded"
                            );
                            turso_assert!(*dst_data_offset == overflow_page_pointer_size, "data must be copied to overflow page pointer offset on overflow pages", { "dst_data_offset": dst_data_offset, "overflow_page_pointer_size": overflow_page_pointer_size });
                            let contents = cur_page.get_contents();
                            let buf = &mut contents.as_ptr()
                                [*dst_data_offset..*dst_data_offset + amount_to_copy];
                            buf.copy_from_slice(record_offset_slice_to_copy);
                        } else {
                            // Copy data into the cell payload on the btree page.
                            let buf = &mut cell_payload
                                [*dst_data_offset..*dst_data_offset + amount_to_copy];
                            buf.copy_from_slice(record_offset_slice_to_copy);
                        }

                        if record_offset_slice.len() - amount_to_copy == 0 {
                            break Ok(IOResult::Done(()));
                        }
                        *state = CopyDataState::AllocateOverflowPage;
                        *src_data_offset += amount_to_copy;
                    }
                    CopyDataState::AllocateOverflowPage => {
                        let new_overflow_page = match pager.allocate_overflow_page() {
                            Ok(IOResult::Done(new_overflow_page)) => {
                                PinGuard::new(new_overflow_page)
                            }
                            Ok(IOResult::IO(io_result)) => return Ok(IOResult::IO(io_result)),
                            Err(e) => {
                                mark_unlikely();
                                break Err(e);
                            }
                        };
                        turso_assert!(
                            new_overflow_page.is_loaded(),
                            "new overflow page is not loaded"
                        );
                        let new_overflow_page_id = new_overflow_page.get().id() as u32;

                        if let Some(prev_page) = current_overflow_page {
                            // Update the previous overflow page's "next overflow page" pointer to point to the new overflow page.
                            turso_assert!(
                                prev_page.is_loaded(),
                                "previous overflow page is not loaded"
                            );
                            let contents = prev_page.get_contents();
                            let buf = &mut contents.as_ptr()[..overflow_page_pointer_size];
                            buf.copy_from_slice(&new_overflow_page_id.to_be_bytes());
                        } else {
                            // Update the cell payload's "next overflow page" pointer to point to the new overflow page.
                            let first_overflow_page_ptr_offset =
                                cell_payload.len() - overflow_page_pointer_size;
                            let buf = &mut cell_payload[first_overflow_page_ptr_offset
                                ..first_overflow_page_ptr_offset + overflow_page_pointer_size];
                            buf.copy_from_slice(&new_overflow_page_id.to_be_bytes());
                        }

                        *dst_data_offset = overflow_page_pointer_size;
                        *space_left_on_cur_page = overflow_page_data_size;
                        *current_overflow_page = Some(new_overflow_page.clone());
                        *state = CopyDataState::Copy;
                    }
                }
            }
        }
    };
    result
}

/// The payload sizes at which a cell spills to overflow pages
#[derive(Clone, Copy, Debug)]
pub struct PayloadLimits {
    pub usable_size: usize,
    pub max_local_table: usize,
    pub max_local_index: usize,
    pub min_local: usize,
}

impl PayloadLimits {
    pub fn new(usable_size: usize) -> Self {
        Self {
            usable_size,
            max_local_table: payload_overflow_threshold_max(PageType::TableLeaf, usable_size),
            max_local_index: payload_overflow_threshold_max(PageType::IndexLeaf, usable_size),
            min_local: payload_overflow_threshold_min(PageType::TableLeaf, usable_size),
        }
    }

    #[inline(always)]
    pub fn max_local(&self, page_type: PageType) -> usize {
        match page_type {
            PageType::IndexInterior | PageType::IndexLeaf => self.max_local_index,
            PageType::TableInterior | PageType::TableLeaf => self.max_local_table,
        }
    }
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
#[inline]
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
#[inline]
pub fn payload_overflow_threshold_min(_page_type: PageType, usable_space: usize) -> usize {
    // Same formula for all page types
    ((usable_space - 12) * 32 / 255) - 23
}

/// Drop a cell from a page.
/// This is done by freeing the range of bytes that the cell occupies.
#[inline]
fn drop_cell(page: &mut PageContent, cell_idx: usize, usable_space: usize) -> Result<()> {
    let (cell_start, cell_len) = page.cell_get_raw_region(cell_idx, usable_space)?;
    free_cell_range(page, cell_start, cell_len, usable_space)?;
    if page.cell_count() > 1 {
        shift_pointers_left(page, cell_idx);
    } else {
        page.write_cell_content_area(usable_space);
        page.write_first_freeblock(0);
        page.write_fragmented_bytes_count(0);
    }
    page.write_cell_count(page.cell_count() as u16 - 1);

    for overflow_cell in page.overflow_cells.iter() {
        turso_debug_assert!(
            overflow_cell.index <= cell_idx,
            "drop_cell: pending overflow cell positioned after dropped cell",
            { "overflow_index": overflow_cell.index, "cell_idx": cell_idx }
        );
    }

    debug_validate_cells!(page, usable_space);
    Ok(())
}

/// Shift pointers to the left once starting from a cell position
/// This is useful when we remove a cell and we want to move left the cells from the right to fill
/// the empty space that's not needed
#[inline]
fn shift_pointers_left(page: &mut PageContent, cell_idx: usize) {
    turso_assert_greater_than!(page.cell_count(), 0);
    let buf = page.as_ptr();
    let (start, _) = page.cell_pointer_array_offset_and_size();
    let start = start + (cell_idx * 2) + 2;
    let right_cells = page.cell_count() - cell_idx - 1;
    let amount_to_shift = right_cells * 2;
    buf.copy_within(start..start + amount_to_shift, start - 2);
}

#[cfg(test)]
#[path = "../tests/unit/storage/btree/tests.rs"]
mod tests;
