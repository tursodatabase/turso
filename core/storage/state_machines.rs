#[derive(Debug, Clone, Copy)]
pub enum AdvanceState {
    Start,
    Advance,
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
