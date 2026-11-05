//! Swarm testing: each run uses a different mix of workloads and faults.
//!
//! A run that uses all workloads with fixed weights cannot get to some states.
//! For example, DELETE keeps tables small, ROLLBACK discards writes, and
//! frequent checkpoints keep the WAL short. With `--swarm`, each run turns off
//! a random set of workloads, changes the weights of the other workloads, and
//! picks its own connection count and fault probabilities. All choices come
//! from the seed, so a run with the same seed and flags makes the same choices.

use std::any::Any;
use std::ops::RangeInclusive;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::workloads::{BeginWorkload, CommitWorkload, RollbackWorkload, Workload};

/// The simulation uses stream 0 of the seed. The swarm uses other streams, so
/// its choices are independent of the random values of the simulation.
const SETTINGS_STREAM: u64 = 1;
const WORKLOAD_STREAM: u64 = 2;

const CONNECTIONS: RangeInclusive<usize> = 1..=8;
const PROCESSES: RangeInclusive<usize> = 2..=4;
const CONNECTIONS_PER_PROCESS: RangeInclusive<usize> = 1..=4;
/// No 0.01: with MVCC, each reopen replays the logical log, and some swarm
/// runs with 0.01 took more than 25 minutes.
const REOPEN_PROBABILITIES: [f64; 3] = [0.0, 0.0001, 0.001];
const ALLOCATION_FAULT_PROBABILITIES: [f64; 4] = [0.0, 0.001, 0.01, 0.05];
const CHECKPOINT_PROBE_PROBABILITIES: [f64; 3] = [0.0, 0.01, 0.1];
const KILL_PROBABILITIES: [f64; 3] = [0.0, 0.001, 0.01];
const MVCC_CHECKPOINT_THRESHOLDS: [Option<i64>; 3] = [None, Some(1024), Some(64 * 1024)];
/// Weight multipliers in quarters: 1/4, 1/2, 1, 2 and 4.
const WEIGHT_QUARTERS: [u32; 5] = [1, 2, 4, 8, 16];

/// The settings that `--swarm` picks from the seed. Each field has the meaning
/// of the command-line flag with the same name.
///
/// The swarm does not turn on the experimental passive MVCC checkpoint. It has
/// open recovery bugs, for example
/// <https://github.com/tursodatabase/turso/issues/11501>. With it, most MVCC
/// swarm jobs in CI fail.
#[derive(Debug, Clone, PartialEq)]
pub struct SwarmConfig {
    seed: u64,
    pub max_connections: usize,
    pub processes: usize,
    pub connections_per_process: usize,
    pub reopen_probability: f64,
    pub allocation_fault_probability: f64,
    pub checkpoint_probe_probability: f64,
    pub kill_probability: f64,
    pub mvcc_checkpoint_threshold: Option<i64>,
}

impl SwarmConfig {
    pub fn new(seed: u64) -> Self {
        let mut rng = seeded_stream(seed, SETTINGS_STREAM);
        Self {
            seed,
            max_connections: rng.random_range(CONNECTIONS),
            processes: rng.random_range(PROCESSES),
            connections_per_process: rng.random_range(CONNECTIONS_PER_PROCESS),
            reopen_probability: pick(&mut rng, &REOPEN_PROBABILITIES),
            allocation_fault_probability: pick(&mut rng, &ALLOCATION_FAULT_PROBABILITIES),
            checkpoint_probe_probability: pick(&mut rng, &CHECKPOINT_PROBE_PROBABILITIES),
            kill_probability: pick(&mut rng, &KILL_PROBABILITIES),
            mvcc_checkpoint_threshold: pick(&mut rng, &MVCC_CHECKPOINT_THRESHOLDS),
        }
    }

    /// Turns off each workload with a probability of 1/2, and multiplies the
    /// weight of each other workload by 1/4, 1/2, 1, 2 or 4. A workload that
    /// is off stays in the list with weight 0. Two rules apply:
    ///
    /// - A read workload that checks results ([`Workload::checks_results`])
    ///   always stays on. Without it, the run cannot find the bugs that the
    ///   check finds.
    /// - If BEGIN stays on, COMMIT or ROLLBACK also stays on. Otherwise a
    ///   transaction never ends, and it blocks all other writers.
    pub fn pick_workloads(
        &self,
        workloads: Vec<(u32, Box<dyn Workload>)>,
    ) -> Vec<(u32, Box<dyn Workload>)> {
        assert!(
            workloads
                .iter()
                .any(|(_, workload)| workload.checks_results()),
            "swarm testing needs a workload that checks results"
        );
        let mut rng = seeded_stream(self.seed, WORKLOAD_STREAM);
        let base_weights: Vec<u32> = workloads.iter().map(|(weight, _)| *weight).collect();
        let mut workloads: Vec<(u32, Box<dyn Workload>)> = workloads
            .into_iter()
            .map(|(weight, workload)| {
                let on = rng.random_bool(0.5) || workload.checks_results();
                let weight = if on { scale(weight, &mut rng) } else { 0 };
                (weight, workload)
            })
            .collect();

        if is_on::<BeginWorkload>(&workloads)
            && !is_on::<CommitWorkload>(&workloads)
            && !is_on::<RollbackWorkload>(&workloads)
        {
            let ends: Vec<usize> = workloads
                .iter()
                .enumerate()
                .filter(|(_, (_, workload))| {
                    is::<CommitWorkload>(workload.as_ref())
                        || is::<RollbackWorkload>(workload.as_ref())
                })
                .map(|(index, _)| index)
                .collect();
            assert!(
                !ends.is_empty(),
                "the workload list has BEGIN, but no COMMIT or ROLLBACK"
            );
            let index = ends[rng.random_range(0..ends.len())];
            workloads[index].0 = scale(base_weights[index], &mut rng);
        }
        workloads
    }
}

fn seeded_stream(seed: u64, stream: u64) -> ChaCha8Rng {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    rng.set_stream(stream);
    rng
}

fn pick<T: Copy>(rng: &mut ChaCha8Rng, values: &[T]) -> T {
    values[rng.random_range(0..values.len())]
}

fn scale(weight: u32, rng: &mut ChaCha8Rng) -> u32 {
    (weight * pick(rng, &WEIGHT_QUARTERS)).div_ceil(4)
}

fn is_on<W: Workload>(workloads: &[(u32, Box<dyn Workload>)]) -> bool {
    workloads
        .iter()
        .any(|(weight, workload)| *weight > 0 && is::<W>(workload.as_ref()))
}

fn is<W: Workload>(workload: &dyn Workload) -> bool {
    let workload: &dyn Any = workload;
    workload.is::<W>()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::workloads::{
        CurrValWorkload, DeleteWorkload, FtsMatchWorkload, IntegrityCheckWorkload, NextValWorkload,
        SimpleInsertWorkload, SimpleSelectWorkload, WalCheckpointWorkload,
    };

    fn workloads() -> Vec<(u32, Box<dyn Workload>)> {
        vec![
            (
                5,
                Box::new(WalCheckpointWorkload {
                    allow_passive: true,
                }),
            ),
            (20, Box::new(SimpleSelectWorkload)),
            (20, Box::new(SimpleInsertWorkload)),
            (15, Box::new(DeleteWorkload)),
            (30, Box::new(BeginWorkload)),
            (10, Box::new(CommitWorkload)),
            (10, Box::new(RollbackWorkload)),
            (10, Box::new(IntegrityCheckWorkload)),
        ]
    }

    fn weights(seed: u64) -> Vec<u32> {
        SwarmConfig::new(seed)
            .pick_workloads(workloads())
            .into_iter()
            .map(|(weight, _)| weight)
            .collect()
    }

    fn position<W: Workload>() -> usize {
        workloads()
            .iter()
            .position(|(_, workload)| is::<W>(workload.as_ref()))
            .expect("the test list has the workload")
    }

    #[test]
    fn same_seed_makes_the_same_choices() {
        for seed in 0..100 {
            assert_eq!(SwarmConfig::new(seed), SwarmConfig::new(seed));
            assert_eq!(weights(seed), weights(seed));
        }
    }

    #[test]
    fn seeds_change_the_settings() {
        let configs: Vec<SwarmConfig> = (0..100).map(SwarmConfig::new).collect();
        let connections: BTreeSet<usize> = configs
            .iter()
            .map(|config| config.max_connections)
            .collect();
        assert_eq!(connections, CONNECTIONS.collect::<BTreeSet<_>>());
        for probabilities in [
            configs
                .iter()
                .map(|config| config.reopen_probability)
                .collect::<Vec<_>>(),
            configs
                .iter()
                .map(|config| config.allocation_fault_probability)
                .collect(),
            configs
                .iter()
                .map(|config| config.kill_probability)
                .collect(),
        ] {
            assert!(probabilities.contains(&0.0));
            assert!(probabilities.iter().any(|probability| *probability > 0.0));
        }
    }

    #[test]
    fn seeds_turn_each_workload_on_and_off_except_checks() {
        let all_weights: Vec<Vec<u32>> = (0..100).map(weights).collect();
        for (index, (_, workload)) in workloads().iter().enumerate() {
            let runs_on = all_weights
                .iter()
                .filter(|weights| weights[index] > 0)
                .count();
            if workload.checks_results() {
                assert_eq!(runs_on, 100, "{} must stay on", workload.name());
            } else {
                assert!(
                    (20..=80).contains(&runs_on),
                    "{} is on in {runs_on} of 100 runs",
                    workload.name()
                );
            }
        }
    }

    #[test]
    fn only_read_workloads_that_check_results_always_stay_on() {
        let reads: [&dyn Workload; 4] = [
            &IntegrityCheckWorkload,
            &SimpleSelectWorkload,
            &FtsMatchWorkload,
            &CurrValWorkload,
        ];
        for workload in reads {
            assert!(workload.checks_results(), "{}", workload.name());
        }
        let writes: [&dyn Workload; 2] = [&NextValWorkload, &SimpleInsertWorkload];
        for workload in writes {
            assert!(!workload.checks_results(), "{}", workload.name());
        }
    }

    #[test]
    fn begin_always_has_a_commit_or_rollback() {
        let begin = position::<BeginWorkload>();
        let commit = position::<CommitWorkload>();
        let rollback = position::<RollbackWorkload>();
        let mut runs_with_begin = 0;
        for seed in 0..1000 {
            let weights = weights(seed);
            if weights[begin] > 0 {
                runs_with_begin += 1;
                assert!(
                    weights[commit] > 0 || weights[rollback] > 0,
                    "seed {seed}: BEGIN is on, but COMMIT and ROLLBACK are off"
                );
            }
        }
        assert!(runs_with_begin > 0);
    }
}
