use rand::{Rng, SeedableRng, rngs::StdRng};
use std::collections::HashMap;
use std::sync::Mutex;
use turso_core::mvcc::yield_points::{YieldInjector, YieldPoint};

/// Following specifies the max number of yields per instance. 20 here is arbitrary, smaller means
/// less interleaving but having large number can slow down the execution.
const MAX_YIELDS: usize = 20;

pub(crate) fn fiber_yield_seed(seed: u64, fiber_idx: usize) -> u64 {
    seed.wrapping_add(fiber_idx as u64)
}

// Selected ordinals for one in-flight instance; slots are cleared as yields are consumed.
// Ordinals may repeat so the same yield point can fire multiple times in one statement.
type InstanceYieldPlan = [Option<u8>; MAX_YIELDS];

// Namespaces one cached per-instance selection by instance identity and logical selection key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct InstancePlanKey {
    instance_id: u64,
    selection_key: u64,
    point_count: u8,
}

// Simulator-owned policy: picks 0..=4 ordinals deterministically and consumes them once.
#[derive(Debug, Default)]
pub(crate) struct SimulatorYieldInjector {
    seed: u64,
    plans: Mutex<HashMap<InstancePlanKey, InstanceYieldPlan>>,
}

impl SimulatorYieldInjector {
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            seed,
            plans: Mutex::new(HashMap::new()),
        }
    }

    fn plan_for(&self, selection_key: u64, point_count: u8) -> InstanceYieldPlan {
        simulator_yield_plan(self.seed, selection_key, point_count)
    }
}

impl YieldInjector for SimulatorYieldInjector {
    fn should_yield(&self, instance_id: u64, selection_key: u64, point: YieldPoint) -> bool {
        let plan_key = InstancePlanKey {
            instance_id,
            selection_key,
            point_count: point.point_count,
        };
        let mut plans = self.plans.lock().unwrap();
        let plan = plans
            .entry(plan_key)
            .or_insert_with(|| self.plan_for(selection_key, point.point_count));
        for slot in plan.iter_mut() {
            if *slot == Some(point.ordinal) {
                *slot = None;
                return true;
            }
        }
        false
    }
}

fn simulator_yield_plan(seed: u64, selection_key: u64, point_count: u8) -> InstanceYieldPlan {
    let mut plan = [None; MAX_YIELDS];
    if point_count == 0 {
        return plan;
    }

    let mut rng = StdRng::seed_from_u64(seed ^ selection_key);
    // randomly select total yield counts for this plan, upto MAX_YIELDS
    let count = rng.random_range(0..=MAX_YIELDS);
    // select the array slots upto `count` and fill them with the yield point ordinals
    for dst in plan.iter_mut().take(count) {
        *dst = Some(rng.random_range(0..point_count));
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seeded_plan_covers_zero_and_multiple_yields() {
        let plans = (0..4096_u64)
            .map(|seed| simulator_yield_plan(seed, 42, 5))
            .collect::<Vec<_>>();
        assert!(
            plans.iter().any(|plan| plan.iter().flatten().count() == 0),
            "expected at least one seed with zero injected yields",
        );
        assert!(
            plans.iter().any(|plan| plan.iter().flatten().count() > 1),
            "expected at least one seed with multiple injected yields",
        );
        assert!(
            plans
                .iter()
                .all(|plan| plan.iter().flatten().count() <= MAX_YIELDS),
            "expected plans to stay within the fixed yield cap",
        );
    }

    #[test]
    fn test_seeded_plan_can_repeat_same_yield_point() {
        let plans = (0..4096_u64)
            .map(|seed| simulator_yield_plan(seed, 42, 1))
            .collect::<Vec<_>>();
        assert!(
            plans.iter().any(|plan| plan.iter().flatten().count() > 1),
            "expected at least one seed to repeat the same yield point",
        );
    }
}
