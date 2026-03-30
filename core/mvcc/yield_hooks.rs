use std::fmt::Debug;

use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
use crate::state_machine::TransitionResult;
use crate::sync::Arc;
use crate::types::IOCompletions;
use crate::types::IOResult;
use crate::Completion;

pub(crate) trait YieldPointMarker: Copy + Debug {
    const POINT_COUNT: u8;

    fn ordinal(self) -> u8;

    fn point(self) -> YieldPoint {
        YieldPoint::new(self.ordinal(), Self::POINT_COUNT)
    }
}

impl YieldPoint {
    pub(crate) fn new(ordinal: u8, point_count: u8) -> Self {
        Self {
            ordinal,
            point_count,
        }
    }
}

pub(crate) struct YieldContext {
    pub(crate) injector: Option<Arc<dyn YieldInjector>>,
    pub(crate) instance_id: u64,
    pub(crate) selection_key: u64,
}

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
