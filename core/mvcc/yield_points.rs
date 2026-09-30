use std::fmt::Debug;

use crate::LimboError;

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

/// External hook consulted at safe state machine boundaries to decide whether to synthesize
/// an error return. Mirrors `YieldInjector` but produces an `Err` instead of a yield, so tests
/// can reproduce mid-state-machine failures (e.g. an I/O error after `remove_tx` ran but before
/// the connection cache was cleared) without requiring a real fault in the I/O layer.
pub trait FailureInjector: Debug + Send + Sync {
    /// Returns `Some(err)` if the state machine should synthetically fail at this point.
    /// Same selection_key / instance_id semantics as `YieldInjector`.
    fn should_fail(
        &self,
        instance_id: u64,
        selection_key: u64,
        point: YieldPoint,
    ) -> Option<LimboError>;
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

// At a safe resumable boundary, ask the active failure injector whether this
// state machine should return Err here. Used to reproduce mid-commit failures
// in tests without requiring a real I/O fault.
macro_rules! inject_transition_failure {
    ($state_machine:expr, $point:expr) => {{
        #[cfg(any(test, injected_yields))]
        {
            use $crate::mvcc::yield_hooks::ProvidesYieldContext;
            let yield_context = $state_machine.yield_context();
            if let Some(err) = crate::mvcc::yield_hooks::maybe_inject_transition_failure(
                yield_context.failure_injector.as_ref(),
                yield_context.instance_id,
                yield_context.selection_key,
                $point,
            ) {
                return Err(err);
            }
        }
    }};
}

pub(crate) use inject_transition_failure;
