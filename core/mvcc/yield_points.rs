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

/// YieldSite is a descriptor which specify the safe yield boundaries in state machines
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct YieldSite {
    pub kind: YieldKind,
    pub ordinal: u8,
    pub point_count: u8,
}

pub(crate) trait YieldSiteMarker: Copy + Debug {
    const KIND: YieldKind;
    const POINT_COUNT: u8;

    fn ordinal(self) -> u8;

    fn site(self) -> YieldSite {
        YieldSite {
            kind: Self::KIND,
            ordinal: self.ordinal(),
            point_count: Self::POINT_COUNT,
        }
    }
}

/// External hook consulted at safe state machine boundaries to decide whether to synthesize a yield.
pub trait YieldInjector: Debug + Send + Sync {
    /// Returns whether to synthetically yield at the current `YieldSite`.
    /// `selection_key` picks the deterministic yield plan for this logical operation.
    /// `instance_id` distinguishes one live state machine/cursor from another so
    /// they do not share yield bookkeeping.
    fn should_yield(&self, instance_id: u64, selection_key: u64, site: YieldSite) -> bool;
}

pub(crate) type YieldInjectorSlot = RwLock<Option<Arc<dyn YieldInjector>>>;

pub(crate) fn maybe_inject_transition_yield<T, P: YieldSiteMarker>(
    injector: Option<&Arc<dyn YieldInjector>>,
    instance_id: u64,
    selection_key: u64,
    point: P,
) -> Option<TransitionResult<T>> {
    let should_yield = injector
        .is_some_and(|injector| injector.should_yield(instance_id, selection_key, point.site()));
    if should_yield {
        tracing::debug!(?point, "injecting MVCC yield");
        return Some(TransitionResult::Io(IOCompletions::Single(
            Completion::new_yield(),
        )));
    }
    None
}

pub(crate) fn maybe_inject_io_yield<T, P: YieldSiteMarker>(
    injector: Option<&Arc<dyn YieldInjector>>,
    instance_id: u64,
    selection_key: u64,
    point: P,
) -> Option<IOResult<T>> {
    let should_yield = injector
        .is_some_and(|injector| injector.should_yield(instance_id, selection_key, point.site()));
    if should_yield {
        tracing::debug!(?point, "injecting MVCC yield");
        return Some(IOResult::IO(IOCompletions::Single(Completion::new_yield())));
    }
    None
}

macro_rules! inject_transition_yield {
    ($injector:expr, $instance_id:expr, $selection_key:expr, $point:expr) => {{
        #[cfg(any(test, feature = "test_helper", feature = "simulator"))]
        {
            let injector = $injector;
            let instance_id = $instance_id;
            let selection_key = $selection_key;
            if let Some(result) = crate::mvcc::yield_points::maybe_inject_transition_yield(
                injector.as_ref(),
                instance_id,
                selection_key,
                $point,
            ) {
                return Ok(result);
            }
        }
    }};
}

pub(crate) use inject_transition_yield;

macro_rules! inject_io_yield {
    ($injector:expr, $instance_id:expr, $selection_key:expr, $point:expr) => {{
        #[cfg(any(test, feature = "test_helper", feature = "simulator"))]
        {
            let injector = $injector;
            let instance_id = $instance_id;
            let selection_key = $selection_key;
            if let Some(result) = crate::mvcc::yield_points::maybe_inject_io_yield(
                injector.as_ref(),
                instance_id,
                selection_key,
                $point,
            ) {
                return Ok(result);
            }
        }
    }};
}

pub(crate) use inject_io_yield;
