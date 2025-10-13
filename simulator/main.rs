#![allow(clippy::arc_with_non_send_sync)]
use anyhow::anyhow;
use clap::Parser;
use generation::plan::{InteractionPlan, InteractionPlanState};
use notify::event::{DataChange, ModifyKind};
use notify::{EventKind, RecursiveMode, Watcher};
use rand::prelude::*;
use runner::bugbase::{Bug, BugBase, LoadedBug};
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
use std::sync::{Arc, Mutex, mpsc};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::field::MakeExt;
use tracing_subscriber::fmt::format;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::generation::plan::{ConnectionState, InteractionPlanIterator};
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
    setup_panic_hook();

    let profile = Profile::parse_from_type(cli_opts.profile.clone())?;
    tracing::debug!(sim_profile = ?profile);

    if let Some(ref command) = cli_opts.subcommand {
        match command {
            SimulatorCommand::List => {
                let mut bugbase = BugBase::load()?;
                bugbase.list_bugs()
            }
            SimulatorCommand::Loop { n, short_circuit } => {
                banner();
                for i in 0..*n {
                    println!("iteration {i}");
                    let result = testing_main(&cli_opts, &profile);
                    if result.is_err() && *short_circuit {
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
                let mut bugbase = BugBase::load()?;
                let bugs = bugbase.load_bugs()?;
                let mut bugs = bugs
                    .into_iter()
                    .flat_map(|bug| {
                        let runs = bug
                            .runs
                            .into_iter()
                            .filter_map(|run| run.error.clone().map(|_| run))
                            .filter(|run| run.error.as_ref().unwrap().contains(filter))
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
                    .map(|cli_opts| testing_main(&cli_opts, &profile))
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
        testing_main(&cli_opts, &profile)
    }
}

fn testing_main(cli_opts: &SimulatorCLI, profile: &Profile) -> anyhow::Result<()> {
    let mut bugbase = if cli_opts.disable_bugbase {
        None
    } else {
        tracing::trace!("loading bugbase");
        Some(BugBase::load()?)
    };

    let (seed, mut env, plans) = setup_simulation(bugbase.as_mut(), cli_opts, profile);

    if cli_opts.watch {
        watch_mode(env).unwrap();
        return Ok(());
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

fn watch_mode(env: SimulatorEnv) -> notify::Result<()> {
    let (tx, rx) = mpsc::channel::<notify::Result<notify::Event>>();
    println!("watching {:?}", env.get_plan_path());
    // Use recommended_watcher() to automatically select the best implementation
    // for your platform. The `EventHandler` passed to this constructor can be a
    // closure, a `std::sync::mpsc::Sender`, a `crossbeam_channel::Sender`, or
    // another type the trait is implemented for.
    let mut watcher = notify::recommended_watcher(tx)?;

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    watcher.watch(&env.get_plan_path(), RecursiveMode::NonRecursive)?;
    // Block forever, printing out events as they come in
    let last_execution = Arc::new(Mutex::new(Execution::new(0, 0)));
    for res in rx {
        match res {
            Ok(event) => {
                if let EventKind::Modify(ModifyKind::Data(DataChange::Content)) = event.kind {
                    tracing::info!("plan file modified, rerunning simulation");
                    let env = env.clone_without_connections();
                    let last_execution_ = last_execution.clone();
                    let result = SandboxedResult::from(
                        std::panic::catch_unwind(move || {
                            let mut env = env;
                            let plan_path = env.get_plan_path();
                            let plan = InteractionPlan::compute_via_diff(&plan_path);
                            env.clear();

                            let env = Arc::new(Mutex::new(env.clone_without_connections()));
                            run_simulation_default(env, plan, last_execution_.clone())
                        }),
                        last_execution.clone(),
                    );
                    match result {
                        SandboxedResult::Correct => {
                            tracing::info!("simulation succeeded");
                            println!("simulation succeeded");
                        }
                        SandboxedResult::Panicked { error, .. }
                        | SandboxedResult::FoundBug { error, .. } => {
                            tracing::error!("simulation failed: '{}'", error);
                        }
                    }
                }
            }
            Err(e) => println!("watch error: {e:?}"),
        }
    }

    Ok(())
}

fn setup_panic_hook() {
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
    std::fs::write(
        env.get_plan_path().with_extension("json"),
        serde_json::to_string_pretty(&*plan).unwrap(),
    )
    .unwrap();

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
                            plan.len(),
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
                            bugbase.make_shrunk(
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
    bugbase: Option<&mut BugBase>,
    cli_opts: &SimulatorCLI,
    profile: &Profile,
) -> (u64, SimulatorEnv, InteractionPlan) {
    if let Some(seed) = &cli_opts.load {
        let seed = seed.parse::<u64>().expect("seed should be a number");
        let bugbase = bugbase.expect("BugBase must be enabled to load a bug");
        tracing::info!("seed={}", seed);
        let bug = bugbase
            .get_bug(seed)
            .unwrap_or_else(|| panic!("bug '{seed}' not found in bug base"));

        let paths = bugbase.paths(seed);
        if !paths.base.exists() {
            std::fs::create_dir_all(&paths.base).unwrap();
        }
        let env = SimulatorEnv::new(
            bug.seed(),
            cli_opts,
            paths,
            SimulationType::Default,
            profile,
        );

        let plan = match bug {
            Bug::Loaded(LoadedBug { plan, .. }) => plan.clone(),
            Bug::Unloaded { seed } => {
                let seed = *seed;
                bugbase
                    .load_bug(seed)
                    .unwrap_or_else(|_| panic!("could not load bug '{seed}' in bug base"))
                    .plan
                    .clone()
            }
        };

        std::fs::write(env.get_plan_path(), plan.to_string()).unwrap();
        std::fs::write(
            env.get_plan_path().with_extension("json"),
            serde_json::to_string_pretty(&plan).unwrap(),
        )
        .unwrap();
        (seed, env, plan)
    } else {
        let seed = cli_opts.seed.unwrap_or_else(|| {
            let mut rng = rand::rng();
            rng.next_u64()
        });
        tracing::info!("seed={}", seed);

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

        let mut env = SimulatorEnv::new(seed, cli_opts, paths, SimulationType::Default, profile);

        tracing::info!("Generating database interaction plan...");

        let plan = InteractionPlan::init_plan(&mut env);

        (seed, env, plan)
    }
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

    if result.error.is_none() {
        let ic = integrity_check(&env.get_db_path());
        if let Err(err) = ic {
            tracing::error!("integrity check failed: {}", err);
            result.error = Some(turso_core::LimboError::InternalError(err.to_string()));
        } else {
            tracing::info!("integrity check passed");
        }
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

fn integrity_check(db_path: &Path) -> anyhow::Result<()> {
    assert!(db_path.exists());
    let conn = rusqlite::Connection::open(db_path)?;
    let mut stmt = conn.prepare("SELECT * FROM pragma_integrity_check;")?;
    let mut rows = stmt.query(())?;
    let mut result: Vec<String> = Vec::new();

    while let Some(row) = rows.next()? {
        result.push(row.get(0)?);
    }
    if result.is_empty() {
        anyhow::bail!("simulation failed: integrity_check should return `ok` or a list of problems")
    }
    if !result[0].eq_ignore_ascii_case("ok") {
        // Build a list of problems
        result.iter_mut().for_each(|row| *row = format!("- {row}"));
        anyhow::bail!("simulation failed: {}", result.join("\n"))
    }
    Ok(())
}
