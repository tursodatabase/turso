use crate::{
    types::{IOCompletions, IOResult},
    Result,
};

pub enum TransitionResult<Result> {
    Io(IOCompletions),
    Continue,
    Done(Result),
}

/// A generic trait for state machines.
pub trait StateTransition {
    type Context;
    type SMResult;

    /// Transition the state machine to the next state.
    ///
    /// Returns `TransitionResult::Io` if the state machine needs to perform an IO operation.
    /// Returns `TransitionResult::Continue` if the state machine needs to continue.
    /// Returns `TransitionResult::Done` if the state machine is done.
    fn step(&mut self, context: &Self::Context) -> Result<TransitionResult<Self::SMResult>>;

    /// Finalize the state machine.
    ///
    /// This is called when the state machine is done.
    fn finalize(&mut self, context: &Self::Context) -> Result<()>;

    /// Check if the state machine is finalized.
    fn is_finalized(&self) -> bool;
}

#[derive(Debug)]
pub struct StateMachine<State: StateTransition> {
    state: State,
    is_finalized: bool,
}

/// A generic state machine that loops calling `transition` until it returns `TransitionResult::Done` or `TransitionResult::Io`.
impl<State: StateTransition> StateMachine<State> {
    pub fn new(state: State) -> Self {
        Self {
            state,
            is_finalized: false,
        }
    }

    pub fn step(&mut self, context: &State::Context) -> Result<IOResult<State::SMResult>> {
        loop {
            if self.is_finalized {
                unreachable!("StateMachine::transition: state machine is finalized");
            }
            match self.state.step(context)? {
                TransitionResult::Io(io) => {
                    return Ok(IOResult::IO(io));
                }
                TransitionResult::Continue => {
                    continue;
                }
                TransitionResult::Done(result) => {
                    assert!(self.state.is_finalized());
                    self.is_finalized = true;
                    return Ok(IOResult::Done(result));
                }
            }
        }
    }

    pub fn finalize(&mut self, context: &State::Context) -> Result<()> {
        self.state.finalize(context)?;
        self.is_finalized = true;
        Ok(())
    }

    pub fn is_finalized(&self) -> bool {
        self.is_finalized
    }
}
