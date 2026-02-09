#![allow(clippy::arc_with_non_send_sync)]
use anyhow::anyhow;
use clap::Parser;
use rand::prelude::*;
use runner::bugbase::BugBase;
use runner::cli::{SimulatorCLI, SimulatorCommand};
use runner::differential;
use runner::env::SimulatorEnv;
use runner::execution::{Execution, ExecutionHistory, ExecutionResult, execute_interactions};
use std::any::Any;
use std::backtrace::Backtrace;
use std::fs::OpenOptions;
use std::io::{IsTerminal, Write};
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::field::MakeExt;
use tracing_subscriber::fmt::format;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::model::interactions::{
    ConnectionState, InteractionPlan, InteractionPlanIterator, InteractionPlanState,
};
use crate::profiles::Profile;
use crate::runner::doublecheck;
use crate::runner::env::{Paths, SimulationPhase, SimulationType};

mod common;
mod generation;
mod model;
mod profiles;
mod runner;
mod shrink;

fn main() -> anyhow::Result<()> {
    init_logger()?;
    let mut cli_opts = SimulatorCLI::parse();
    cli_opts.validate()?;

    let profile = Profile::parse_from_type(cli_opts.profile.clone())?;
    tracing::debug!(sim_profile = ?profile);

    if let Some(command) = cli_opts.subcommand.take() {
        match command {
            SimulatorCommand::List => {
                let mut bugbase = BugBase::load()?;
                bugbase.list_bugs()
            }
            SimulatorCommand::Loop { n, short_circuit } => {
                banner();
                for i in 0..n {
                    println!("iteration {i}");
                    let result = testing_main(&mut cli_opts, &profile);
                    if result.is_err() && short_circuit {
                        println!("short circuiting after {i} iterations");
                        return result;
                    } else if result.is_err() {
                        println!("iteration {i} failed");
                    } else {
                        println!("iteration {i} succeeded");
                    }
                }
                Ok(())
            }
            SimulatorCommand::Test { filter } => {
                let bugbase = BugBase::load()?;
                let bugs = bugbase.load_bugs()?;
                let mut bugs = bugs
                    .into_iter()
                    .flat_map(|bug| {
                        let runs = bug
                            .runs
                            .into_iter()
                            .filter_map(|run| run.error.clone().map(|_| run))
                            .filter(|run| run.error.as_ref().unwrap().contains(&filter))
                            .map(|run| run.cli_options)
                            .collect::<Vec<_>>();

                        runs.into_iter()
                            .map(|mut cli_opts| {
                                cli_opts.seed = Some(bug.seed);
                                cli_opts.load = None;
                                cli_opts
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();

                bugs.sort();
                bugs.dedup_by(|a, b| a == b);

                println!(
                    "found {} previously triggered configurations with {}",
                    bugs.len(),
                    filter
                );

                let results = bugs
                    .into_iter()
                    .map(|mut cli_opts| testing_main(&mut cli_opts, &profile))
                    .collect::<Vec<_>>();

                let (successes, failures): (Vec<_>, Vec<_>) =
                    results.into_iter().partition(|result| result.is_ok());
                println!("the results of the change are:");
                println!("\t{} successful runs", successes.len());
                println!("\t{} failed runs", failures.len());
                Ok(())
            }
            SimulatorCommand::PrintSchema => {
                let schema = schemars::schema_for!(crate::Profile);
                println!("{}", serde_json::to_string_pretty(&schema).unwrap());
                Ok(())
            }
        }
    } else {
        banner();
        testing_main(&mut cli_opts, &profile)
    }
}

fn testing_main(cli_opts: &mut SimulatorCLI, profile: &Profile) -> anyhow::Result<()> {
    let mut bugbase = if cli_opts.disable_bugbase {
        None
    } else {
        tracing::trace!("loading bugbase");
        Some(BugBase::load()?)
    };

    let (seed, mut env, plans) = setup_simulation(bugbase.as_mut(), cli_opts, profile);

    if cli_opts.watch {
        anyhow::bail!("watch mode is disabled for now");
    }

    let paths = env.paths.clone();

    if cli_opts.differential {
        env.type_ = SimulationType::Differential;
    } else if cli_opts.doublecheck {
        env.type_ = SimulationType::Doublecheck;
    }

    let result = run_simulator(bugbase.as_mut(), cli_opts, env, plans);

    // Print the seed, the locations of the database and the plan file at the end again for easily accessing them.
    println!("seed: {seed}");
    println!("path: {}", paths.base.display());

    if !cli_opts.keep_files && result.is_ok() {
        paths.delete_all_files();
    }

    result
}

fn run_simulator(
    mut bugbase: Option<&mut BugBase>,
    cli_opts: &SimulatorCLI,
    env: SimulatorEnv,
    plan: InteractionPlan,
) -> anyhow::Result<()> {
    std::panic::set_hook(Box::new(move |info| {
        tracing::error!("panic occurred");

        let payload = info.payload();
        if let Some(s) = payload.downcast_ref::<&str>() {
            tracing::error!("{}", s);
        } else if let Some(s) = payload.downcast_ref::<String>() {
            tracing::error!("{}", s);
        } else {
            tracing::error!("unknown panic payload");
        }

        let bt = Backtrace::force_capture();
        tracing::error!("captured backtrace:\n{}", bt);
    }));

    let last_execution = Arc::new(Mutex::new(Execution::new(0, 0)));
    let mut gen_rng = env.gen_rng();

    let env = Arc::new(Mutex::new(env));
    // Need to wrap in Rc Mutex due to the UnwindSafe barrier
    let plan = Rc::new(Mutex::new(plan));

    let result = {
        let sim_execution = last_execution.clone();
        let sim_plan = plan.clone();
        let sim_env = env.clone();

        SandboxedResult::from(
            std::panic::catch_unwind(move || {
                let mut sim_plan = sim_plan.lock().unwrap();
                let plan = sim_plan.generator(&mut gen_rng);
                run_simulation(sim_env, plan, sim_execution)
            }),
            last_execution.clone(),
        )
    };
    env.clear_poison();
    plan.clear_poison();

    let env = env.lock().unwrap();
    let plan = plan.lock().unwrap();

    tracing::info!("{}", plan.stats());
    std::fs::write(env.get_plan_path(), plan.to_string()).unwrap();

    // No doublecheck, run shrinking if panicking or found a bug.
    match &result {
        SandboxedResult::Correct => {
            tracing::info!("simulation succeeded");
            println!("simulation succeeded");
            Ok(())
        }
        SandboxedResult::Panicked {
            error,
            last_execution,
        }
        | SandboxedResult::FoundBug {
            error,
            last_execution,
            ..
        } => {
            if let SandboxedResult::FoundBug { history, .. } = &result {
                // No panic occurred, so write the history to a file
                let f = std::fs::File::create(&env.paths.history).unwrap();
                let mut f = std::io::BufWriter::new(f);
                for execution in history.history.iter() {
                    writeln!(
                        f,
                        "{} {}",
                        execution.connection_index, execution.interaction_index,
                    )
                    .unwrap();
                }
            }

            tracing::error!("simulation failed: '{}'", error);

            if cli_opts.disable_heuristic_shrinking && !cli_opts.enable_brute_force_shrinking {
                tracing::info!("shrinking is disabled, skipping shrinking");
                if let Some(bugbase) = bugbase.as_deref_mut() {
                    bugbase
                        .add_bug(env.opts.seed, plan.clone(), Some(error.clone()), cli_opts)
                        .unwrap();
                }
                return Err(anyhow!("failed with error: '{}'", error));
            }

            tracing::info!("Starting to shrink");
            let (shrunk_plan, shrunk) = if !cli_opts.disable_heuristic_shrinking {
                let shrunk_plan = plan.shrink_interaction_plan(last_execution);
                tracing::info!("{}", shrunk_plan.stats());
                // Write the shrunk plan to a file
                let shrunk_plan_path = env
                    .paths
                    .plan(&SimulationType::Default, &SimulationPhase::Shrink);
                let mut f = std::fs::File::create(&shrunk_plan_path).unwrap();
                tracing::trace!("writing shrunk plan to {}", shrunk_plan_path.display());
                f.write_all(shrunk_plan.to_string().as_bytes()).unwrap();

                let last_execution = Arc::new(Mutex::new(*last_execution));
                let env = env.clone_at_phase(SimulationPhase::Shrink);
                let env = Arc::new(Mutex::new(env));
                let shrunk = SandboxedResult::from(
                    std::panic::catch_unwind(|| {
                        let plan = shrunk_plan.static_iterator();

                        run_simulation(env.clone(), plan, last_execution.clone())
                    }),
                    last_execution,
                );
                (shrunk_plan, shrunk)
            } else {
                (plan.clone(), result.clone())
            };

            match (&shrunk, &result) {
                (
                    SandboxedResult::Panicked { error: e1, .. },
                    SandboxedResult::Panicked { error: e2, .. },
                )
                | (
                    SandboxedResult::FoundBug { error: e1, .. },
                    SandboxedResult::FoundBug { error: e2, .. },
                ) => {
                    if let Some(bugbase) = bugbase.as_deref_mut() {
                        tracing::trace!(
                            "adding bug to bugbase, seed: {}, plan: {}, error: {}",
                            env.opts.seed,
                            plan.len_properties(),
                            error
                        );
                        bugbase
                            .add_bug(env.opts.seed, plan.clone(), Some(error.clone()), cli_opts)
                            .unwrap();
                    }

                    if e1 != e2 {
                        tracing::error!(
                            ?shrunk,
                            ?result,
                            "shrinking failed, the error was not properly reproduced"
                        );
                        Err(anyhow!("failed with error: '{}'", error))
                    } else {
                        let seed = env.opts.seed;
                        let env = env.clone_at_phase(SimulationPhase::Shrink);
                        let env = Arc::new(Mutex::new(env));

                        let final_plan = if cli_opts.enable_brute_force_shrinking {
                            let brute_shrunk_plan =
                                shrunk_plan.brute_shrink_interaction_plan(&shrunk, env.clone());
                            tracing::info!("Brute force shrinking completed");
                            brute_shrunk_plan
                        } else {
                            shrunk_plan
                        };

                        tracing::info!(
                            "shrinking succeeded, reduced the plan from {} to {}",
                            plan.len(),
                            final_plan.len()
                        );
                        // Save the shrunk database
                        if let Some(bugbase) = bugbase.as_deref_mut() {
                            bugbase.save_shrunk(
                                seed,
                                cli_opts,
                                final_plan.clone(),
                                Some(e1.clone()),
                            )?;
                        }
                        Err(anyhow!("failed with error: '{}'", e1))
                    }
                }
                (_, SandboxedResult::Correct) => {
                    unreachable!("shrinking should never be called on a correct simulation")
                }
                _ => {
                    tracing::error!(
                        ?shrunk,
                        ?result,
                        "shrinking failed, the error was not properly reproduced"
                    );
                    if let Some(bugbase) = bugbase {
                        bugbase
                            .add_bug(env.opts.seed, plan.clone(), Some(error.clone()), cli_opts)
                            .unwrap();
                    }
                    Err(anyhow!("failed with error: '{}'", error))
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
enum SandboxedResult {
    Panicked {
        error: String,
        last_execution: Execution,
    },
    #[expect(dead_code)]
    FoundBug {
        error: String,
        history: ExecutionHistory,
        last_execution: Execution,
    },
    Correct,
}

impl SandboxedResult {
    fn from(
        result: Result<ExecutionResult, Box<dyn Any + Send>>,
        last_execution: Arc<Mutex<Execution>>,
    ) -> Self {
        match result {
            Ok(ExecutionResult { error: None, .. }) => SandboxedResult::Correct,
            Ok(ExecutionResult { error: Some(e), .. }) => {
                let error = format!("{e:?}");
                let last_execution = last_execution.lock().unwrap();
                SandboxedResult::Panicked {
                    error,
                    last_execution: *last_execution,
                }
            }
            Err(payload) => {
                tracing::error!("panic occurred");
                let err = if let Some(s) = payload.downcast_ref::<&str>() {
                    tracing::error!("{}", s);
                    s.to_string()
                } else if let Some(s) = payload.downcast_ref::<String>() {
                    tracing::error!("{}", s);
                    s.to_string()
                } else {
                    tracing::error!("unknown panic payload");
                    "unknown panic payload".to_string()
                };

                last_execution.clear_poison();

                SandboxedResult::Panicked {
                    error: err,
                    last_execution: *last_execution.lock().unwrap(),
                }
            }
        }
    }
}

fn setup_simulation(
    mut bugbase: Option<&mut BugBase>,
    cli_opts: &mut SimulatorCLI,
    profile: &Profile,
) -> (u64, SimulatorEnv, InteractionPlan) {
    if let Some(seed) = cli_opts.load {
        let bugbase = bugbase
            .as_mut()
            .expect("BugBase must be enabled to load a bug");
        let paths = bugbase.paths(seed);
        if !paths.base.exists() {
            std::fs::create_dir_all(&paths.base).unwrap();
        }

        let bug = bugbase
            .get_or_load_bug(seed)
            .unwrap()
            .unwrap_or_else(|| panic!("bug '{seed}' not found in bug base"));

        // run the simulation with the same CLI options as the loaded bug
        *cli_opts = bug.last_cli_opts();
    }
    let seed = cli_opts.seed.unwrap_or_else(|| {
        let mut rng = rand::rng();
        rng.next_u64()
    });

    tracing::info!("seed={}", seed);
    cli_opts.seed = Some(seed);

    let paths = if let Some(bugbase) = bugbase {
        let paths = bugbase.paths(seed);
        // Create the output directory if it doesn't exist
        if !paths.base.exists() {
            std::fs::create_dir_all(&paths.base)
                .map_err(|e| format!("{e:?}"))
                .unwrap();
        }
        paths
    } else {
        let dir = std::env::current_dir().unwrap().join("simulator-output");
        std::fs::create_dir_all(&dir).unwrap();
        Paths::new(&dir)
    };

    let env = SimulatorEnv::new(seed, cli_opts, paths, SimulationType::Default, profile);

    tracing::info!("Generating database interaction plan...");

    let plan = InteractionPlan::new(env.profile.experimental_mvcc);

    (seed, env, plan)
}

fn run_simulation(
    env: Arc<Mutex<SimulatorEnv>>,
    plan: impl InteractionPlanIterator,
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let simulation_type = {
        env.clear_poison();
        let mut env = env.lock().unwrap();
        env.clear();
        env.type_
    };

    match simulation_type {
        SimulationType::Default => run_simulation_default(env, plan, last_execution),
        SimulationType::Differential => {
            let limbo_env = {
                let env = env.lock().unwrap();
                env.clone_as(SimulationType::Default)
            };
            let limbo_env = Arc::new(Mutex::new(limbo_env));
            differential::run_simulation(limbo_env, env, plan, last_execution)
        }
        SimulationType::Doublecheck => {
            let limbo_env = {
                let env = env.lock().unwrap();
                env.clone_as(SimulationType::Default)
            };
            let limbo_env = Arc::new(Mutex::new(limbo_env));
            doublecheck::run_simulation(limbo_env, env, plan, last_execution)
        }
    }
}

fn run_simulation_default(
    env: Arc<Mutex<SimulatorEnv>>,
    plan: impl InteractionPlanIterator,
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    tracing::info!("Executing database interaction plan...");

    let num_conns = {
        let env = env.lock().unwrap();
        env.connections.len()
    };

    let mut conn_states = (0..num_conns)
        .map(|_| ConnectionState::default())
        .collect::<Vec<_>>();

    let mut state = InteractionPlanState {
        interaction_pointer: 0,
    };

    let mut result = execute_interactions(
        env.clone(),
        plan,
        &mut state,
        &mut conn_states,
        last_execution,
    );

    let env = env.lock().unwrap();
    env.io.print_stats();

    tracing::info!("Simulation completed");

    env.io.persist_files().unwrap();

    if result.error.is_none() && !env.opts.disable_integrity_check {
        match integrity_check(&env.get_db_path()) {
            Ok(conn) => {
                tracing::info!("integrity check passed");
                // Verify committed data is actually present in the persisted database.
                // This catches bugs where Turso claims "commit success" but never fsynced.
                if let Err(err) = env.verify_committed_data(&conn, &env.committed_tables) {
                    tracing::error!("committed data verification failed: {}", err);
                    result.error = Some(turso_core::LimboError::InternalError(err));
                }
            }
            Err(err) => {
                tracing::error!("integrity check failed: {}", err);
                result.error = Some(turso_core::LimboError::InternalError(err.to_string()));
            }
        }
    } else if env.opts.disable_integrity_check {
        tracing::info!("skipping integrity check (disabled by configuration)");
    }

    result
}

fn init_logger() -> anyhow::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("simulator.log")?;

    let requires_ansi = std::io::stdout().is_terminal();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(requires_ansi)
                .with_line_number(true)
                .without_time()
                .with_thread_ids(false),
        )
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(file)
                .with_ansi(false)
                .fmt_fields(format::PrettyFields::new())
                .with_line_number(true)
                .without_time()
                .with_thread_ids(false)
                .map_fmt_fields(|f| f.debug_alt()),
        )
        .try_init()?;
    Ok(())
}

fn banner() {
    println!("{BANNER}");
}

const BANNER: &str = r#"
  ,_______________________________.
  | ,___________________________. |
  | |                           | |
  | | >HELLO                    | |
  | |                           | |
  | | >A STRANGE GAME.          | |
  | | >THE ONLY WINNING MOVE IS | |
  | | >NOT TO PLAY.             | |
  | |___________________________| |
  |                               |
  |                               |
  `-------------------------------`
          |              |
          |______________|
      ,______________________.
     / /====================\ \
    / /======================\ \
   /____________________________\
   \____________________________/

"#;

fn integrity_check(db_path: &Path) -> anyhow::Result<rusqlite::Connection> {
    assert!(db_path.exists());
    let conn = rusqlite::Connection::open(db_path)?;
    let result: Vec<String> = {
        let mut stmt = conn.prepare("SELECT * FROM pragma_integrity_check;")?;
        let mut rows = stmt.query(())?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            result.push(row.get(0)?);
        }
        result
    };
    if result.is_empty() {
        anyhow::bail!("simulation failed: integrity_check should return `ok` or a list of problems")
    }
    if !result[0].eq_ignore_ascii_case("ok") {
        // Build a list of problems
        let problems: Vec<String> = result.iter().map(|row| format!("- {row}")).collect();
        anyhow::bail!("simulation failed: {}", problems.join("\n"))
    }
    Ok(conn)
}
