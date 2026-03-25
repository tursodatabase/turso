use crate::state_machine::TransitionResult;
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::types::IOCompletions;
use crate::types::IOResult;
use crate::Completion;
use std::fmt::Debug;

/// The state machines on which we can safely inject yield points
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum YieldKind {
    Commit,
    Cursor,
}

/// YieldPoint is a descriptor for one safe yield boundary in a state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct YieldPoint {
    pub kind: YieldKind,
    pub ordinal: u8,
    pub point_count: u8,
}

pub(crate) trait YieldPointMarker: Copy + Debug {
    const KIND: YieldKind;
    const POINT_COUNT: u8;

    fn ordinal(self) -> u8;

    fn point(self) -> YieldPoint {
        YieldPoint {
            kind: Self::KIND,
            ordinal: self.ordinal(),
            point_count: Self::POINT_COUNT,
        }
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

pub(crate) type YieldInjectorSlot = RwLock<Option<Arc<dyn YieldInjector>>>;

#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
pub(crate) struct YieldContext {
    pub injector: Option<Arc<dyn YieldInjector>>,
    pub instance_id: u64,
    pub selection_key: u64,
}

#[cfg(any(test, feature = "test_helper", feature = "simulator"))]
pub(crate) trait ProvidesYieldContext {
    fn yield_context(&self) -> YieldContext;
}

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
