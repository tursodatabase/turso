use crate::PageRef;

#[derive(Debug, Clone)]
pub enum EmptyTableState {
    Start,
    ReadPage { page: PageRef },
}

#[derive(Debug, Clone, Copy)]
pub enum MoveToRightState {
    Start,
    ProcessPage,
}

#[derive(Debug, Clone, Copy)]
pub enum SeekToLastState {
    Start,
    IsEmpty,
}

#[derive(Debug, Clone, Copy)]
pub enum RewindState {
    Start,
    NextRecord,
}

#[derive(Debug, Clone, Copy)]
pub enum AdvanceState {
    Start,
    Advance,
}

#[derive(Debug, Clone, Copy)]
pub enum CountState {
    Start,
    Loop,
    /// Resume state used after `CountState::Loop` yielded for spill IO
    /// mid-descent. The loop-top `stack.advance()` and `self.count +=
    /// cell_count()` mutations have already been applied for this step,
    /// so on re-entry we retry only the read + (second-)advance + push,
    /// then transition back to `Loop`.
    Descend {
        target: i64,
    },
    Finish,
}

#[derive(Debug, Clone, Copy)]
pub enum SeekEndState {
    Start,
    ProcessPage,
}

#[derive(Debug, Clone, Copy)]
pub enum MoveToState {
    Start,
    MoveToPage,
}
