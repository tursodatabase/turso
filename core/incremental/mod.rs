pub mod aggregate_operator;
pub mod compiler;
pub mod cursor;
pub mod dbsp;
pub mod expr_compiler;
pub mod filter_operator;
pub mod input_operator;
pub mod join_operator;
pub mod merge_operator;
pub mod operator;
pub mod persistence;
pub mod project_operator;
pub mod view;

#[cfg(test)]
pub(crate) mod yield_test_support {
    use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
    use crate::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Fires the requested yield point exactly once, for the btree whose write
    /// selection key matches.
    #[derive(Debug)]
    pub(crate) struct OneShotYieldInjector {
        point: YieldPoint,
        selection_key: u64,
        fired: AtomicBool,
    }

    impl OneShotYieldInjector {
        pub(crate) fn new(point: YieldPoint, selection_key: u64) -> Arc<Self> {
            Arc::new(Self {
                point,
                selection_key,
                fired: AtomicBool::new(false),
            })
        }

        pub(crate) fn fired(&self) -> bool {
            self.fired.load(Ordering::Acquire)
        }
    }

    impl YieldInjector for OneShotYieldInjector {
        fn should_yield(&self, _instance_id: u64, selection_key: u64, point: YieldPoint) -> bool {
            point == self.point
                && selection_key == self.selection_key
                && !self.fired.swap(true, Ordering::AcqRel)
        }
    }
}
