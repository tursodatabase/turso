use crate::{
    storage::{
        btree::{BTreePage, DeleteSavepoint},
        sqlite3_ondisk::BTreeCell,
    },
    PageRef,
};

/// State machine of destroy operations
/// Keep track of traversal so that it can be resumed when IO is encountered
#[derive(Debug, Clone)]
pub enum DestroyState {
    Start,
    ProcessPage,
    ClearOverflowPages { cell: BTreeCell },
    FreePage,
}

#[derive(Debug, Clone)]
pub enum DeleteState {
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

pub enum PayloadOverflowWithOffset {
    SkipOverflowPages {
        next_page: BTreePage,
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

#[derive(Debug, Clone)]
pub enum OverflowState {
    Start,
    ProcessPage { next_page: PageRef },
    Done,
}

#[derive(Debug, Clone)]
pub enum EmptyTableState {
    Start,
    ReadPage { page: PageRef },
}

#[derive(Debug, Clone, Copy)]
pub enum MoveToState {
    Start,
    ProcessPage,
}

#[derive(Debug, Clone, Copy)]
pub enum NextPrevState {
    Start,
    GetRecord,
}

#[derive(Debug, Clone, Copy)]
pub enum SeekToState {
    Start,
    ProcessPage,
}

#[derive(Debug, Clone, Copy)]
pub enum RewindState {
    Start,
    NextRecord,
}

#[derive(Debug, Clone, Copy)]
pub enum InsertState {
    Start,
    Seek,
    Advance,
    InsertIntoPage,
}

#[derive(Clone)]
pub enum InsertIntoPageState {
    Start,
    /// Calls `record` function for the current record
    Record {
        page: BTreePage,
        cell_idx: usize,
    },
    OverwriteCell {
        page: BTreePage,
        cell_idx: usize,
    },
    InsertCell {
        page: PageRef,
        cell_idx: usize,
    },
    Balance,
    Finish,
}

#[derive(Debug, Clone, Copy)]
/// May involve balancing due to overflow.
pub enum BalanceState {
    Start,
    FreePages {
        curr_page: usize,
        sibling_count_new: usize,
    },
    /// Choose which sibling pages to balance (max 3).
    /// Generally, the siblings involved will be the page that triggered the balancing and its left and right siblings.
    /// The exceptions are:
    /// 1. If the leftmost page triggered balancing, up to 3 leftmost pages will be balanced.
    /// 2. If the rightmost page triggered balancing, up to 3 rightmost pages will be balanced.
    NonRootPickSiblings,
    /// Perform the actual balancing. This will result in 1-5 pages depending on the number of total cells to be distributed
    /// from the source pages.
    NonRootDoBalancing,
}

#[derive(Debug, Clone, Copy)]
pub enum CountState {
    Start,
    Loop,
    Finish,
}
