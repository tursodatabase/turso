use super::{AtomicU64, CursorYieldPoint, Ordering, YieldInjector, YieldPoint, YieldPointMarker};

/// Counts the B-tree steps MVCC cursors take to seek and to advance, and never yields.
#[derive(Debug, Default)]
pub(super) struct SeekBtreeSteps(pub(super) AtomicU64);

impl YieldInjector for SeekBtreeSteps {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        if point == CursorYieldPoint::SeekBtreeProgress.point()
            || point == CursorYieldPoint::AdvanceBtreeForwardProgress.point()
            || point == CursorYieldPoint::AdvanceBtreeBackwardProgress.point()
        {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        false
    }
}
