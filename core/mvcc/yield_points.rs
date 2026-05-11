use std::fmt::Debug;

use crate::LimboError;

#[cfg(any(test, injected_yields))]
use std::sync::OnceLock;

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

// At a safe resumable boundary inside an IO-returning code path, ask the
// active failure injector whether this code path should return Err here.
// Mirrors `inject_transition_failure!` but is callable from sites whose
// outer return type is `Result<IOResult<T>>` rather than
// `Result<TransitionResult<T>>`.
macro_rules! inject_io_failure {
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

pub(crate) use inject_io_failure;

/// Stable name → `YieldPoint` table for the MVCC state-machine yield points.
///
/// Used by external tooling (e.g. the MVCC REPL's `.fault` command) to drive
/// fault injection from text input without exposing the private yield-point
/// enums.
///
/// Only available when `injected_yields` is on (set automatically by the
/// `test_helper` and `simulator` Cargo features via `core/build.rs`).
#[cfg(any(test, injected_yields))]
pub fn all_yield_points() -> &'static [(&'static str, YieldPoint)] {
    use crate::mvcc::cursor::CursorYieldPoint;
    use crate::mvcc::database::checkpoint_state_machine::CheckpointYieldPoint;
    use crate::mvcc::database::CommitYieldPoint;
    use crate::mvcc::yield_hooks::YieldPointMarker;

    static POINTS: OnceLock<Vec<(&'static str, YieldPoint)>> = OnceLock::new();
    POINTS.get_or_init(|| {
        vec![
            // Commit state machine
            (
                "commit.validation",
                CommitYieldPoint::CommitValidation.point(),
            ),
            (
                "commit.wait_for_dependencies",
                CommitYieldPoint::WaitForDependencies.point(),
            ),
            (
                "commit.log_record_prepared",
                CommitYieldPoint::LogRecordPrepared.point(),
            ),
            (
                "commit.after_remove_tx",
                CommitYieldPoint::AfterRemoveTx.point(),
            ),
            // Checkpoint state machine
            (
                "checkpoint.before_acquire_lock",
                CheckpointYieldPoint::BeforeAcquireLock.point(),
            ),
            (
                "checkpoint.after_durable_boundary_advanced",
                CheckpointYieldPoint::AfterDurableBoundaryAdvanced.point(),
            ),
            // Cursor I/O
            ("cursor.next_start", CursorYieldPoint::NextStart.point()),
            (
                "cursor.next_btree_advance",
                CursorYieldPoint::NextBtreeAdvance.point(),
            ),
            (
                "cursor.prev_btree_advance",
                CursorYieldPoint::PrevBtreeAdvance.point(),
            ),
            ("cursor.seek_start", CursorYieldPoint::SeekStart.point()),
            (
                "cursor.seek_btree_progress",
                CursorYieldPoint::SeekBtreeProgress.point(),
            ),
            (
                "cursor.exists_btree_fallback",
                CursorYieldPoint::ExistsBtreeFallback.point(),
            ),
            (
                "cursor.count_progress",
                CursorYieldPoint::CountProgress.point(),
            ),
            (
                "cursor.advance_btree_forward_progress",
                CursorYieldPoint::AdvanceBtreeForwardProgress.point(),
            ),
            (
                "cursor.advance_btree_backward_progress",
                CursorYieldPoint::AdvanceBtreeBackwardProgress.point(),
            ),
        ]
    })
}

/// Look up a yield point by its canonical name (see `all_yield_points`).
#[cfg(any(test, injected_yields))]
pub fn yield_point_by_name(name: &str) -> Option<YieldPoint> {
    all_yield_points()
        .iter()
        .find_map(|(n, p)| (*n == name).then_some(*p))
}
