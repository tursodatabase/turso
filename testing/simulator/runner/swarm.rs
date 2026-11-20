//! Swarm testing: each run uses a different mix of statements, properties and faults.
//!
//! A run that uses all statements with fixed weights cannot get to some states.
//! For example, DELETE keeps tables small, savepoint rollbacks discard writes,
//! and frequent checkpoints keep the WAL short. With `--swarm`, each run turns
//! off a random set of statements, changes the weights of the other statements,
//! turns off a random set of properties and faults, and picks its own
//! connection count and I/O latency. All choices come from the seed, so a run
//! with the same seed and flags makes the same choices.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::profiles::{Profile, query::QueryProfile};
use crate::runner::env::SimulatorOpts;

/// The simulator uses stream 0 of the seed and stream 0 of the seed plus one.
/// The swarm uses other streams of the seed, so its choices are independent of
/// the random values of the simulation.
const SETTINGS_STREAM: u64 = 1;
const QUERY_STREAM: u64 = 2;
const SWITCH_STREAM: u64 = 3;

/// The percent of I/O operations that the simulated I/O delays.
const LATENCY_PROBABILITIES: [u8; 3] = [0, 1, 10];
/// Weight multipliers in quarters: 1/4, 1/2, 1, 2 and 4.
const WEIGHT_QUARTERS: [u32; 5] = [1, 2, 4, 8, 16];

/// The statements that stay on in each run. The read-only properties, for
/// example Select-Limit, take their weight from the SELECT weight. Without
/// INSERT, the tables stay empty, and the other statements have no rows to use.
const ALWAYS_ON_QUERIES: [&str; 2] = ["select", "insert"];

/// Changes the profile and the options of a run to the choices that the seed
/// makes. For statements, properties and faults, the swarm only turns off or
/// reweights what the profile and the flags turn on: a weight of 0 stays 0,
/// and a property or a fault that a flag turns off stays off. The connection
/// count is 1 to the connection count of the profile. If `latency_given` is
/// true, `--latency-prob` keeps its value.
pub(crate) fn apply(
    seed: u64,
    profile: &mut Profile,
    opts: &mut SimulatorOpts,
    latency_given: bool,
) {
    let mut rng = seeded_stream(seed, SETTINGS_STREAM);
    profile.max_connections = rng.random_range(1..=profile.max_connections);
    let latency_probability = pick(&mut rng, &LATENCY_PROBABILITIES);
    if !latency_given {
        profile.io.latency.latency_probability = latency_probability;
    }
    pick_query_weights(&mut seeded_stream(seed, QUERY_STREAM), &mut profile.query);
    turn_off(&mut seeded_stream(seed, SWITCH_STREAM), switches(opts));
}

/// The lines that show the settings of a swarm run.
pub(crate) fn describe(profile: &Profile, opts: &SimulatorOpts) -> Vec<String> {
    let mut flags = vec![format!(
        "--latency-prob {}",
        profile.io.latency.latency_probability
    )];
    let mut opts = opts.clone();
    flags.extend(
        switches(&mut opts)
            .into_iter()
            .filter(|(_, disabled)| **disabled)
            .map(|(flag, _)| flag.to_string()),
    );
    let mut query = profile.query.clone();
    let (on, off): (Vec<_>, Vec<_>) = query
        .weights_mut()
        .into_iter()
        .partition(|(_, weight)| **weight > 0);
    let on: Vec<String> = on
        .into_iter()
        .map(|(name, weight)| format!("{name}:{weight}"))
        .collect();
    let off: Vec<&str> = off.into_iter().map(|(name, _)| name).collect();
    vec![
        format!("swarm: connections {}", profile.max_connections),
        format!("swarm: {}", flags.join(" ")),
        format!("swarm: queries {}", on.join(" ")),
        format!("swarm: queries off {}", off.join(" ")),
    ]
}

/// The properties and the faults that the swarm can turn off, each with the
/// flag that turns it off. Each property in this list writes to the database.
/// The read-only properties, for example Select-Limit, are not in the list:
/// they do not change the states that a run gets to, and without them a run
/// finds fewer bugs.
fn switches(opts: &mut SimulatorOpts) -> [(&'static str, &mut bool); 8] {
    [
        (
            "--disable-insert-values-select",
            &mut opts.disable_insert_values_select,
        ),
        (
            "--disable-double-create-failure",
            &mut opts.disable_double_create_failure,
        ),
        ("--disable-delete-select", &mut opts.disable_delete_select),
        ("--disable-drop-select", &mut opts.disable_drop_select),
        (
            "--disable-savepoint-rollback",
            &mut opts.disable_savepoint_rollback,
        ),
        ("--disable-fsync-no-wait", &mut opts.disable_fsync_no_wait),
        ("--disable-faulty-query", &mut opts.disable_faulty_query),
        (
            "--disable-reopen-database",
            &mut opts.disable_reopen_database,
        ),
    ]
}

/// Turns off each statement with a probability of 1/2, and multiplies the
/// weight of each other statement by 1/4, 1/2, 1, 2 or 4. Two rules apply:
///
/// - SELECT and INSERT always stay on ([`ALWAYS_ON_QUERIES`]).
/// - If DROP TABLE stays on, CREATE TABLE also stays on. Otherwise a run can
///   drop all of its tables, and then it has no table to write to.
fn pick_query_weights(rng: &mut ChaCha8Rng, query: &mut QueryProfile) {
    let create_table_weight = query.create_table_weight;
    for (name, weight) in query.weights_mut() {
        let on = rng.random_bool(0.5) || ALWAYS_ON_QUERIES.contains(&name);
        *weight = if on { scale(*weight, rng) } else { 0 };
    }
    if query.drop_table_weight > 0 && query.create_table_weight == 0 {
        query.create_table_weight = scale(create_table_weight, rng);
    }
}

/// Turns off each switch with a probability of 1/2. A switch that is off stays off.
fn turn_off<'a>(rng: &mut ChaCha8Rng, switches: impl IntoIterator<Item = (&'a str, &'a mut bool)>) {
    for (_, disabled) in switches {
        let off = rng.random_bool(0.5);
        *disabled |= off;
    }
}

fn seeded_stream(seed: u64, stream: u64) -> ChaCha8Rng {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    rng.set_stream(stream);
    rng
}

fn scale(weight: u32, rng: &mut ChaCha8Rng) -> u32 {
    (weight * pick(rng, &WEIGHT_QUARTERS)).div_ceil(4)
}

fn pick<T: Copy>(rng: &mut ChaCha8Rng, values: &[T]) -> T {
    values[rng.random_range(0..values.len())]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use clap::Parser as _;

    use super::*;
    use crate::runner::cli::SimulatorCLI;
    use crate::runner::env::{Paths, SimulationType, SimulatorEnv};

    fn env(seed: u64, args: &[&str], profile: &Profile) -> SimulatorEnv {
        let output = tempfile::tempdir().expect("create a directory for the run");
        let args = ["limbo_sim", "--io-backend", "memory"]
            .iter()
            .chain(args)
            .copied();
        SimulatorEnv::new(
            seed,
            &SimulatorCLI::parse_from(args),
            Paths::new(output.path()),
            SimulationType::Default,
            profile,
        )
    }

    /// The profile and the options of a run of the default profile without `--swarm`.
    fn base() -> (Profile, SimulatorOpts) {
        let env = env(1, &[], &Profile::default());
        (env.profile, env.opts)
    }

    fn swarm(seed: u64, (profile, opts): &(Profile, SimulatorOpts)) -> (Profile, SimulatorOpts) {
        let (mut profile, mut opts) = (profile.clone(), opts.clone());
        apply(seed, &mut profile, &mut opts, false);
        (profile, opts)
    }

    fn weights(profile: &Profile) -> Vec<(&'static str, u32)> {
        let mut query = profile.query.clone();
        query
            .weights_mut()
            .into_iter()
            .map(|(name, weight)| (name, *weight))
            .collect()
    }

    fn disabled(opts: &SimulatorOpts) -> Vec<(&'static str, bool)> {
        let mut opts = opts.clone();
        switches(&mut opts)
            .into_iter()
            .map(|(flag, disabled)| (flag, *disabled))
            .collect()
    }

    #[test]
    fn same_seed_makes_the_same_choices() {
        let base = base();
        for seed in 0..100 {
            let (first, second) = (swarm(seed, &base), swarm(seed, &base));
            assert_eq!(describe(&first.0, &first.1), describe(&second.0, &second.1));
        }
    }

    #[test]
    fn seeds_change_the_connections_and_the_latency() {
        let base = base();
        let runs: Vec<(Profile, SimulatorOpts)> = (0..100).map(|seed| swarm(seed, &base)).collect();
        let connections: BTreeSet<usize> = runs
            .iter()
            .map(|(profile, _)| profile.max_connections)
            .collect();
        assert_eq!(connections, (1..=base.0.max_connections).collect());
        let latencies: BTreeSet<u8> = runs
            .iter()
            .map(|(profile, _)| profile.io.latency.latency_probability)
            .collect();
        assert_eq!(latencies, LATENCY_PROBABILITIES.into_iter().collect());
    }

    #[test]
    fn seeds_turn_each_query_on_and_off_except_select_and_insert() {
        let base = base();
        let runs: Vec<Vec<(&str, u32)>> = (0..100)
            .map(|seed| weights(&swarm(seed, &base).0))
            .collect();
        for (index, (name, base_weight)) in weights(&base.0).into_iter().enumerate() {
            assert!(base_weight > 0, "{name} is off in the default profile");
            let runs_on = runs.iter().filter(|run| run[index].1 > 0).count();
            if ["select", "insert"].contains(&name) {
                assert_eq!(runs_on, 100, "{name} must stay on");
            } else {
                assert!(
                    (20..=85).contains(&runs_on),
                    "{name} is on in {runs_on} of 100 runs"
                );
            }
        }
    }

    #[test]
    fn drop_table_always_has_create_table() {
        let base = base();
        let mut runs_with_drop = 0;
        for seed in 0..1000 {
            let query = swarm(seed, &base).0.query;
            if query.drop_table_weight > 0 {
                runs_with_drop += 1;
                assert!(
                    query.create_table_weight > 0,
                    "seed {seed}: DROP TABLE is on, but CREATE TABLE is off"
                );
            }
        }
        assert!(runs_with_drop > 0);
    }

    #[test]
    fn seeds_turn_each_write_property_and_fault_on_and_off() {
        let base = base();
        assert!(disabled(&base.1).iter().all(|(_, disabled)| !disabled));
        let runs: Vec<SimulatorOpts> = (0..100).map(|seed| swarm(seed, &base).1).collect();
        for (index, (flag, _)) in disabled(&base.1).into_iter().enumerate() {
            let runs_off = runs.iter().filter(|opts| disabled(opts)[index].1).count();
            assert!(
                (20..=80).contains(&runs_off),
                "{flag} turns off its feature in {runs_off} of 100 runs"
            );
        }
        for opts in &runs {
            assert!(!opts.disable_select_limit);
            assert!(!opts.disable_select_optimizer);
            assert!(!opts.disable_where_true_false_null);
            assert!(!opts.disable_union_all_preserves_cardinality);
            assert!(!opts.disable_lateral_matches_json_each);
        }
    }

    /// A flag that the command line gives keeps its value, a weight of 0 in the
    /// profile stays 0, and the connection count stays at most the count of the profile.
    #[test]
    fn swarm_keeps_the_flags_and_the_profile_limits() {
        let profile = Profile {
            max_connections: 3,
            ..Profile::write_heavy()
        };
        assert_eq!(profile.query.update_weight, 0);
        assert_eq!(profile.query.delete_weight, 0);
        let args = [
            "--swarm",
            "--latency-prob",
            "7",
            "--disable-reopen-database",
        ];
        let mut connections = BTreeSet::new();
        for seed in 0..20 {
            let env = env(seed, &args, &profile);
            connections.insert(env.profile.max_connections);
            assert_eq!(env.profile.io.latency.latency_probability, 7);
            assert!(env.opts.disable_reopen_database);
            assert_eq!(env.profile.query.update_weight, 0);
            assert_eq!(env.profile.query.delete_weight, 0);
            assert_eq!(env.connections.len(), env.profile.max_connections);
        }
        assert_eq!(connections, (1..=3).collect());
    }
}
