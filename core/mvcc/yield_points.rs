use std::fmt::Debug;

/// YieldPoint is a descriptor for one safe yield boundary in a state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct YieldPoint {
    pub ordinal: u8,
    pub point_count: u8,
}

/// External hook consulted at safe state machine boundaries to decide whether to synthesize a yield.
pub trait YieldInjector: Debug + Send + Sync {
    /// Returns whether to synthetically yield at the current `YieldPoint`.
    /// `selection_key` picks the deterministic yield plan for this logical operation.
    /// `instance_id` distinguishes one live state machine/cursor from another so
    /// they do not share yield bookkeeping.
    fn should_yield(&self, instance_id: u64, selection_key: u64, point: YieldPoint) -> bool;
}

// At a safe resumable boundary, ask the active yield injector whether this
// state machine should return a synthetic TransitionResult::Io yield here.
macro_rules! inject_transition_yield {
    ($state_machine:expr, $point:expr) => {{
        #[cfg(any(test, injected_yields))]
        {
            use $crate::mvcc::yield_hooks::ProvidesYieldContext;
            let yield_context = $state_machine.yield_context();
            if let Some(result) = crate::mvcc::yield_hooks::maybe_inject_transition_yield(
                yield_context.injector.as_ref(),
                yield_context.instance_id,
                yield_context.selection_key,
                $point,
            ) {
                return Ok(result);
            }
        }
    }};
}

pub(crate) use inject_transition_yield;

// At a safe resumable boundary, ask the active yield injector whether this
// state machine should return a synthetic IOResult::IO yield here.
macro_rules! inject_io_yield {
    ($state_machine:expr, $point:expr) => {{
        #[cfg(any(test, injected_yields))]
        {
            use $crate::mvcc::yield_hooks::ProvidesYieldContext;
            let yield_context = $state_machine.yield_context();
            if let Some(result) = crate::mvcc::yield_hooks::maybe_inject_io_yield(
                yield_context.injector.as_ref(),
                yield_context.instance_id,
                yield_context.selection_key,
                $point,
            ) {
                return Ok(result);
            }
        }
    }};
}

pub(crate) use inject_io_yield;
