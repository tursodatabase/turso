use std::fmt::Debug;

#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
use crate::state_machine::TransitionResult;
#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
use crate::sync::Arc;
#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
use crate::types::IOCompletions;
#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
use crate::types::IOResult;
#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
use crate::Completion;

/// YieldPoint is a descriptor for one safe yield boundary in a state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct YieldPoint {
    pub ordinal: u8,
    pub point_count: u8,
}

#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
pub(crate) trait YieldPointMarker: Copy + Debug {
    const POINT_COUNT: u8;

    fn ordinal(self) -> u8;

    fn point(self) -> YieldPoint {
        YieldPoint::new(self.ordinal(), Self::POINT_COUNT)
    }
}

#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
impl YieldPoint {
    pub(crate) fn new(ordinal: u8, point_count: u8) -> Self {
        Self { ordinal, point_count }
    }
}

/// External hook consulted at safe state machine boundaries to decide whether to synthesize a yield.
pub trait YieldInjector: Debug + Send + Sync {
    /// Returns whether to synthetically yield at the current `YieldPoint`.
    /// `selection_key` picks the deterministic yield plan for this logical operation.
    /// `instance_id` distinguishes one live state machine/cursor from another so
    /// they do not share yield bookkeeping.
    fn should_yield(&self, instance_id: u64, selection_key: u64, point: YieldPoint) -> bool;
}

#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
pub(crate) struct YieldContext {
    pub(crate) injector: Option<Arc<dyn YieldInjector>>,
    pub(crate) instance_id: u64,
    pub(crate) selection_key: u64,
}

#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
impl YieldContext {
    pub(crate) fn new(
        injector: Option<Arc<dyn YieldInjector>>,
        instance_id: u64,
        selection_key: u64,
    ) -> Self {
        Self {
            injector,
            instance_id,
            selection_key,
        }
    }
}

#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
pub(crate) trait ProvidesYieldContext {
    fn yield_context(&self) -> YieldContext;
}

#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
pub(crate) fn maybe_inject_transition_yield<T, P: YieldPointMarker>(
    injector: Option<&Arc<dyn YieldInjector>>,
    instance_id: u64,
    selection_key: u64,
    point: P,
) -> Option<TransitionResult<T>> {
    let should_yield = injector
        .is_some_and(|injector| injector.should_yield(instance_id, selection_key, point.point()));
    if should_yield {
        tracing::debug!(?point, "injecting MVCC yield");
        return Some(TransitionResult::Io(IOCompletions::Single(
            Completion::new_yield(),
        )));
    }
    None
}

#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
pub(crate) fn maybe_inject_io_yield<T, P: YieldPointMarker>(
    injector: Option<&Arc<dyn YieldInjector>>,
    instance_id: u64,
    selection_key: u64,
    point: P,
) -> Option<IOResult<T>> {
    let should_yield = injector
        .is_some_and(|injector| injector.should_yield(instance_id, selection_key, point.point()));
    if should_yield {
        tracing::debug!(?point, "injecting MVCC yield");
        return Some(IOResult::IO(IOCompletions::Single(Completion::new_yield())));
    }
    None
}

// At a safe resumable boundary, ask the active yield injector whether this
// state machine should return a synthetic TransitionResult::Io yield here.
macro_rules! inject_transition_yield {
    ($state_machine:expr, $point:expr) => {{
        #[cfg(any(test, feature = "test_helper", feature = "simulator"))]
        {
            use $crate::mvcc::yield_points::ProvidesYieldContext;
            let yield_context = $state_machine.yield_context();
            if let Some(result) = crate::mvcc::yield_points::maybe_inject_transition_yield(
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
        #[cfg(any(test, feature = "test_helper", feature = "simulator"))]
        {
            use $crate::mvcc::yield_points::ProvidesYieldContext;
            let yield_context = $state_machine.yield_context();
            if let Some(result) = crate::mvcc::yield_points::maybe_inject_io_yield(
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
