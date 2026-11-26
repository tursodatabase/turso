use super::{AtomicU64, CursorYieldPoint, Ordering, YieldInjector, YieldPoint, YieldPointMarker};

/// Counts the B-tree steps MVCC cursors take to seek and to advance, and never yields.
#[derive(Debug, Default)]
pub(super) struct SeekBtreeSteps {
    pub(super) seek_starts: AtomicU64,
    pub(super) seek_progress: AtomicU64,
    pub(super) total: AtomicU64,
}

impl YieldInjector for SeekBtreeSteps {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        if point == CursorYieldPoint::SeekStart.point() {
            self.seek_starts.fetch_add(1, Ordering::Relaxed);
        }
        let is_seek = point == CursorYieldPoint::SeekBtreeProgress.point();
        if is_seek {
            self.seek_progress.fetch_add(1, Ordering::Relaxed);
        }
        if is_seek
            || point == CursorYieldPoint::AdvanceBtreeForwardProgress.point()
            || point == CursorYieldPoint::AdvanceBtreeBackwardProgress.point()
        {
            self.total.fetch_add(1, Ordering::Relaxed);
        }
        false
    }
}
