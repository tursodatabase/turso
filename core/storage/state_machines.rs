use crate::{types::IOCompletions, PageRef};

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

#[derive(Debug)]
pub enum RewindState {
    Start,
    Wait(IOCompletions),
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
