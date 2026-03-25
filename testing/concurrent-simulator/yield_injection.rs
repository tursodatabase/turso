use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};
use std::collections::HashMap;
use std::sync::Mutex;
use turso_core::mvcc::yield_points::{YieldInjector, YieldKind, YieldPoint};

const MAX_YIELDS: usize = 4;

pub(crate) fn fiber_yield_seed(seed: u64, fiber_idx: usize) -> u64 {
    seed.wrapping_add(fiber_idx as u64)
}

// Selected ordinals for one in-flight instance; slots are cleared as yields are consumed.
type InstanceYieldPlan = [Option<u8>; MAX_YIELDS];

// Namespaces one cached per-instance selection by instance identity and yield-site family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct InstancePlanKey {
    instance_id: u64,
    selection_key: u64,
    kind: YieldKind,
    point_count: u8,
}

// Simulator-owned policy: picks 0..=4 ordinals deterministically and consumes them once.
#[derive(Debug, Default)]
pub struct SimulatorYieldInjector {
    seed: u64,
    plans: Mutex<HashMap<InstancePlanKey, InstanceYieldPlan>>,
}

impl SimulatorYieldInjector {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            plans: Mutex::new(HashMap::new()),
        }
    }

    fn plan_for(&self, key: u64, point: YieldPoint) -> InstanceYieldPlan {
        simulator_yield_plan(self.seed, mix_site_key(key, point.kind), point.point_count)
    }
}

impl YieldInjector for SimulatorYieldInjector {
    fn should_yield(&self, instance_id: u64, selection_key: u64, point: YieldPoint) -> bool {
        let plan_key = InstancePlanKey {
            instance_id,
            selection_key,
            kind: point.kind,
            point_count: point.point_count,
        };
        let mut plans = self.plans.lock().unwrap();
        let plan = plans
            .entry(plan_key)
            .or_insert_with(|| self.plan_for(selection_key, point));
        for slot in plan.iter_mut() {
            if *slot == Some(point.ordinal) {
                *slot = None;
                return true;
            }
        }
        false
    }
}

fn mix_site_key(key: u64, kind: YieldKind) -> u64 {
    key ^ match kind {
        // just random constants, anything would do as long as they differ between kinds
        // to diversify yield plans
        YieldKind::Commit => 0xC011_C011_C011_C011,
        YieldKind::Cursor => 0x4355_5253_4F52_4352, // ASCII-ish "CURSORCR"
    }
}

// Derive a deterministic pseudo-random seed from the simulator seed and the
// logical selection key. This is SplitMix64-style mixing: the 64-bit golden-ratio
// increment spreads nearby inputs apart, and the two multiply/xor rounds avalanche
// the bits so small key changes produce unrelated plans.
fn simulator_yield_seed(seed: u64, key: u64) -> u64 {
    let mut z = key.wrapping_add(seed).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn simulator_yield_plan(seed: u64, key: u64, point_count: u8) -> InstanceYieldPlan {
    let mut plan = [None; MAX_YIELDS];
    if point_count == 0 {
        return plan;
    }

    let max_points = usize::min(point_count as usize, MAX_YIELDS);
    let mut rng = StdRng::seed_from_u64(simulator_yield_seed(seed, key));
    let count = rng.random_range(0..=max_points);
    let mut choices = (0..point_count).collect::<Vec<_>>();
    choices.shuffle(&mut rng);
    for (dst, point) in plan.iter_mut().zip(choices.into_iter().take(count)) {
        *dst = Some(point);
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
}
