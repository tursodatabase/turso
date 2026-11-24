//! Swarm testing: each run uses a different mix of statements, properties and faults.
//!
//! A run that uses all statements with fixed weights cannot get to some states.
//! For example, DELETE keeps tables small, savepoint rollbacks discard writes,
//! and frequent checkpoints keep the WAL short. With `--swarm`, each run turns
//! off a random set of statements, changes the weights of the other statements,
//! turns off a random set of properties and faults, and picks its own
//! connection count, I/O backend, I/O latency and clock ticks. All choices come
//! from the seed, so a run with the same seed and flags makes the same choices.

use clap::ValueEnum;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use sql_generation::generation::{
    pick,
    swarm::{scale_weight, seeded_stream},
};

use crate::profiles::{Profile, query::QueryProfile};
use crate::runner::cli::{IoBackend, SimulatorCLI};
use crate::runner::env::SimulatorOpts;

/// The simulator uses stream 0 of the seed and stream 0 of the seed plus one.
/// The swarm uses other streams of the seed, so its choices are independent of
/// the random values of the simulation.
const SETTINGS_STREAM: u64 = 1;
const QUERY_STREAM: u64 = 2;
const SWITCH_STREAM: u64 = 3;

/// The percent of I/O operations that the simulated I/O delays.
const LATENCY_PROBABILITIES: [u8; 3] = [0, 1, 10];
/// Only the memory backend can simulate a power loss, so the other backends
/// run without the power loss faults and without FsyncNoWait. The list has no
/// io_uring: io_uring exists only on Linux, and some hosts block it, so a seed
/// that picks it cannot run on all hosts.
const IO_BACKENDS: [IoBackend; 2] = [IoBackend::Memory, IoBackend::Default];
/// Ranges of `--min-tick` and `--max-tick`, in microseconds. Each read of the
/// simulated clock moves it forward by a tick from the range, and a delayed
/// I/O completes 5 to 20 ms later. Thus small ticks let more work run before a
/// delayed I/O completes.
const TICK_RANGES: [(u64, u64); 3] = [(1, 10), (10, 50), (50, 500)];

/// The properties and the faults that the swarm can turn off, each with the
/// flag that turns it off. Each property in this list writes to the database.
/// The read-only properties, for example Select-Limit, are not in the list:
/// they do not change the states that a run gets to, and without them a run
/// finds fewer bugs.
///
/// `$opts` is a `&SimulatorOpts` or a `&mut SimulatorOpts`, and each switch is
/// a `&bool` or a `&mut bool` to match. Thus one list serves to read and to
/// change the switches. A function cannot do both, and a macro must come
/// before its first use in the file.
macro_rules! switches {
    ($opts:expr) => {{
        let SimulatorOpts {
            disable_insert_values_select,
            disable_double_create_failure,
            disable_delete_select,
            disable_drop_select,
            disable_savepoint_rollback,
            disable_fsync_no_wait,
            disable_faulty_query,
            disable_reopen_database,
            ..
        } = $opts;
        [
            (
                "--disable-insert-values-select",
                disable_insert_values_select,
            ),
            (
                "--disable-double-create-failure",
                disable_double_create_failure,
            ),
            ("--disable-delete-select", disable_delete_select),
            ("--disable-drop-select", disable_drop_select),
            ("--disable-savepoint-rollback", disable_savepoint_rollback),
            ("--disable-fsync-no-wait", disable_fsync_no_wait),
            ("--disable-faulty-query", disable_faulty_query),
            ("--disable-reopen-database", disable_reopen_database),
        ]
    }};
}

/// Changes the profile, the options and the I/O backend of a run to the
/// choices that the seed makes. For statements, properties and faults, the
/// swarm only turns off or reweights what the profile and the flags turn on: a
/// weight of 0 stays 0, and a property or a fault that a flag turns off stays
/// off. The connection count is 1 to the connection count of the profile.
/// `--io-backend` and `--latency-prob` keep the values that `cli_opts` gives.
/// If `cli_opts` gives `--min-tick` or `--max-tick`, the tick range does not
/// change.
pub(crate) fn apply(
    seed: u64,
    cli_opts: &SimulatorCLI,
    profile: &mut Profile,
    opts: &mut SimulatorOpts,
    io_backend: &mut IoBackend,
) {
    // The draws come before the checks of the flags, so that a flag does not
    // change the draws that come after it.
    let mut rng = seeded_stream(seed, SETTINGS_STREAM);
    profile.max_connections = rng.random_range(1..=profile.max_connections);
    let latency_probability = *pick(&LATENCY_PROBABILITIES, &mut rng);
    if cli_opts.latency_probability.is_none() {
        profile.io.latency.latency_probability = latency_probability;
    }
    let picked_io_backend = *pick(&IO_BACKENDS, &mut rng);
    if cli_opts.io_backend.is_none() {
        *io_backend = picked_io_backend;
    }
    let (min_tick, max_tick) = *pick(&TICK_RANGES, &mut rng);
    if cli_opts.min_tick.is_none() && cli_opts.max_tick.is_none() {
        profile.io.latency.min_tick = min_tick;
        profile.io.latency.max_tick = max_tick;
    }
    pick_query_weights(&mut seeded_stream(seed, QUERY_STREAM), &mut profile.query);
    turn_off(&mut seeded_stream(seed, SWITCH_STREAM), switches!(opts));
}

/// The lines that show the settings of a swarm run.
pub(crate) fn describe(
    profile: &Profile,
    opts: &SimulatorOpts,
    io_backend: IoBackend,
) -> Vec<String> {
    let latency = &profile.io.latency;
    let io_backend = io_backend
        .to_possible_value()
        .expect("each I/O backend has a flag value");
    let mut flags = vec![
        format!("--io-backend {}", io_backend.get_name()),
        format!("--latency-prob {}", latency.latency_probability),
        format!("--min-tick {}", latency.min_tick),
        format!("--max-tick {}", latency.max_tick),
    ];
    flags.extend(
        switches!(opts)
            .into_iter()
            .filter(|(_, disabled)| **disabled)
            .map(|(flag, _)| flag.to_string()),
    );
    let (on, off): (Vec<_>, Vec<_>) = profile
        .query
        .weights()
        .into_iter()
        .partition(|(_, weight)| *weight > 0);
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

/// Turns off each statement with a probability of 1/2, and multiplies the
/// weight of each other statement by 1/4, 1/2, 1, 2 or 4. Three rules apply:
///
/// - SELECT always stays on. The read-only properties, for example
///   Select-Limit, take their weight from the SELECT weight.
/// - INSERT always stays on. Without it, the tables stay empty, and the other
///   statements have no rows to use.
/// - If DROP TABLE stays on, CREATE TABLE also stays on. Otherwise a run can
///   drop all of its tables, and then it has no table to write to.
///
/// A statement that a rule turns on again gets a new multiplier of its weight
/// in the profile.
fn pick_query_weights(rng: &mut ChaCha8Rng, query: &mut QueryProfile) {
    let select_weight = query.select_weight;
    let insert_weight = query.insert_weight;
    let create_table_weight = query.create_table_weight;
    for (_, weight) in query.weights_mut() {
        *weight = if rng.random_bool(0.5) {
            scale_weight(*weight, rng)
        } else {
            0
        };
    }
    if query.select_weight == 0 {
        query.select_weight = scale_weight(select_weight, rng);
    }
    if query.insert_weight == 0 {
        query.insert_weight = scale_weight(insert_weight, rng);
    }
    if query.drop_table_weight > 0 && query.create_table_weight == 0 {
        query.create_table_weight = scale_weight(create_table_weight, rng);
    }
}

/// Turns off each switch with a probability of 1/2. A switch that is off stays off.
fn turn_off<'a>(rng: &mut ChaCha8Rng, switches: impl IntoIterator<Item = (&'a str, &'a mut bool)>) {
    for (_, disabled) in switches {
        let off = rng.random_bool(0.5);
        *disabled |= off;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use clap::Parser as _;

    use super::*;
    use crate::runner::env::{Paths, SimulationType, SimulatorEnv};

    /// The settings of a run that `apply` changes.
    #[derive(Clone)]
    struct Settings {
        profile: Profile,
        opts: SimulatorOpts,
        io_backend: IoBackend,
    }

    /// An environment with memory I/O, so that the test writes no database files.
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

    /// The settings of a run of the default profile without `--swarm`.
    fn base() -> Settings {
        let env = env(1, &[], &Profile::default());
        Settings {
            profile: env.profile,
            opts: env.opts,
            io_backend: env.io_backend,
        }
    }

    /// The settings that `--swarm` picks for `seed` when the command line gives no other flag.
    fn swarm(seed: u64, base: &Settings) -> Settings {
        let mut settings = base.clone();
        let cli_opts = SimulatorCLI::parse_from(["limbo_sim", "--swarm"]);
        apply(
            seed,
            &cli_opts,
            &mut settings.profile,
            &mut settings.opts,
            &mut settings.io_backend,
        );
        settings
    }

    fn lines(settings: &Settings) -> Vec<String> {
        describe(&settings.profile, &settings.opts, settings.io_backend)
    }

    fn weights(profile: &Profile) -> Vec<(&'static str, u32)> {
        profile.query.weights().to_vec()
    }

    fn disabled(opts: &SimulatorOpts) -> Vec<(&'static str, bool)> {
        switches!(opts)
            .into_iter()
            .map(|(flag, disabled)| (flag, *disabled))
            .collect()
    }

    #[test]
    fn same_seed_makes_the_same_choices() {
        let base = base();
        for seed in 0..100 {
            assert_eq!(lines(&swarm(seed, &base)), lines(&swarm(seed, &base)));
        }
    }

    #[test]
    fn seeds_change_the_connections_the_io_and_the_ticks() {
        let base = base();
        let runs: Vec<Settings> = (0..100).map(|seed| swarm(seed, &base)).collect();
        let connections: BTreeSet<usize> =
            runs.iter().map(|run| run.profile.max_connections).collect();
        assert_eq!(connections, (1..=base.profile.max_connections).collect());
        let latencies: BTreeSet<u8> = runs
            .iter()
            .map(|run| run.profile.io.latency.latency_probability)
            .collect();
        assert_eq!(latencies, LATENCY_PROBABILITIES.into_iter().collect());
        let io_backends: BTreeSet<IoBackend> = runs.iter().map(|run| run.io_backend).collect();
        assert_eq!(
            io_backends,
            BTreeSet::from([IoBackend::Memory, IoBackend::Default])
        );
        let ticks: BTreeSet<(u64, u64)> = runs
            .iter()
            .map(|run| {
                (
                    run.profile.io.latency.min_tick,
                    run.profile.io.latency.max_tick,
                )
            })
            .collect();
        assert_eq!(ticks, TICK_RANGES.into_iter().collect());
    }

    #[test]
    fn seeds_turn_each_query_on_and_off_except_select_and_insert() {
        let base = base();
        let runs: Vec<Vec<(&str, u32)>> = (0..100)
            .map(|seed| weights(&swarm(seed, &base).profile))
            .collect();
        for (index, (name, base_weight)) in weights(&base.profile).into_iter().enumerate() {
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
            let query = swarm(seed, &base).profile.query;
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
        assert!(disabled(&base.opts).iter().all(|(_, disabled)| !disabled));
        let runs: Vec<SimulatorOpts> = (0..100).map(|seed| swarm(seed, &base).opts).collect();
        for (index, (flag, _)) in disabled(&base.opts).into_iter().enumerate() {
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
            "--min-tick",
            "3",
            "--max-tick",
            "7",
            "--disable-reopen-database",
        ];
        let mut connections = BTreeSet::new();
        for seed in 0..20 {
            let env = env(seed, &args, &profile);
            connections.insert(env.profile.max_connections);
            assert_eq!(env.io_backend, IoBackend::Memory);
            assert_eq!(env.profile.io.latency.latency_probability, 7);
            assert_eq!(env.profile.io.latency.min_tick, 3);
            assert_eq!(env.profile.io.latency.max_tick, 7);
            assert!(env.opts.disable_reopen_database);
            assert_eq!(env.profile.query.update_weight, 0);
            assert_eq!(env.profile.query.delete_weight, 0);
            assert_eq!(env.connections.len(), env.profile.max_connections);
        }
        assert_eq!(connections, (1..=3).collect());
    }

    /// The swarm picks the minimum and the maximum tick together, so one tick
    /// flag keeps both ticks. Otherwise the picked maximum could be smaller than
    /// the given minimum.
    #[test]
    fn one_tick_flag_keeps_both_ticks() {
        let profile = Profile::default();
        for seed in 0..20 {
            let env = env(seed, &["--swarm", "--min-tick", "20"], &profile);
            assert_eq!(env.profile.io.latency.min_tick, 20);
            assert_eq!(env.profile.io.latency.max_tick, profile.io.latency.max_tick);
        }
    }
}
