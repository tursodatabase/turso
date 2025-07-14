#![allow(clippy::arc_with_non_send_sync, dead_code)]
use anyhow::{anyhow, Context};
use clap::Parser;
use generation::plan::{Interaction, InteractionPlan, InteractionPlanState};
use generation::ArbitraryFrom;
use notify::event::{DataChange, ModifyKind};
use notify::{EventKind, RecursiveMode, Watcher};
use rand::prelude::*;
use runner::bugbase::{Bug, BugBase, LoadedBug};
use runner::cli::{SimulatorCLI, SimulatorCommand};
use runner::env::SimulatorEnv;
use runner::execution::{execute_plans, Execution, ExecutionHistory, ExecutionResult};
use runner::{differential, watch};
use std::any::Any;
use std::backtrace::Backtrace;
use std::fs::OpenOptions;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use tracing_subscriber::field::MakeExt;
use tracing_subscriber::fmt::format;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod generation;
mod model;
mod runner;
mod shrink;
struct Paths {
    base: PathBuf,
    db: PathBuf,
    plan: PathBuf,
    shrunk_plan: PathBuf,
    history: PathBuf,
    doublecheck_db: PathBuf,
    shrunk_db: PathBuf,
    diff_db: PathBuf,
}

impl Paths {
    fn new(output_dir: &Path) -> Self {
        Paths {
            base: output_dir.to_path_buf(),
            db: PathBuf::from(output_dir).join("test.db"),
            plan: PathBuf::from(output_dir).join("plan.sql"),
            shrunk_plan: PathBuf::from(output_dir).join("shrunk.sql"),
            history: PathBuf::from(output_dir).join("history.txt"),
            doublecheck_db: PathBuf::from(output_dir).join("double.db"),
            shrunk_db: PathBuf::from(output_dir).join("shrunk.db"),
            diff_db: PathBuf::from(output_dir).join("diff.db"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    init_logger();
    let mut cli_opts = SimulatorCLI::parse();
    cli_opts.validate()?;

    match cli_opts.subcommand {
        Some(SimulatorCommand::List) => {
            let mut bugbase = BugBase::load()?;
            bugbase.list_bugs()
        }
        Some(SimulatorCommand::Loop { n, short_circuit }) => {
            banner();
            for i in 0..n {
                println!("iteration {}", i);
                let result = testing_main(&cli_opts);
                if result.is_err() && short_circuit {
                    println!("short circuiting after {} iterations", i);
                    return result;
                } else if result.is_err() {
                    println!("iteration {} failed", i);
                } else {
                    println!("iteration {} succeeded", i);
                }
            }
            Ok(())
        }
        Some(SimulatorCommand::Test { filter }) => {
            let mut bugbase = BugBase::load()?;
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
                .map(|cli_opts| testing_main(&cli_opts))
                .collect::<Vec<_>>();

            let (successes, failures): (Vec<_>, Vec<_>) =
                results.into_iter().partition(|result| result.is_ok());
            println!("the results of the change are:");
            println!("\t{} successful runs", successes.len());
            println!("\t{} failed runs", failures.len());
            Ok(())
        }
        None => {
            banner();
            testing_main(&cli_opts)
        }
    }
}

fn testing_main(cli_opts: &SimulatorCLI) -> anyhow::Result<()> {
    let mut bugbase = if cli_opts.disable_bugbase {
        None
    } else {
        Some(BugBase::load()?)
    };

    let last_execution = Arc::new(Mutex::new(Execution::new(0, 0, 0)));
    let (seed, env, plans, paths) =
        setup_simulation(bugbase.as_mut(), cli_opts, |p| &p.plan, |p| &p.db);

    if cli_opts.watch {
        watch_mode(seed, cli_opts, &paths, last_execution.clone()).unwrap();
        return Ok(());
    }

    let mut result = if cli_opts.differential {
        differential_testing(
            seed,
            bugbase.as_mut(),
            cli_opts,
            &paths,
            plans,
            last_execution.clone(),
        )
    } else {
        run_simulator(
            seed,
            bugbase.as_mut(),
            cli_opts,
            &paths,
            env,
            plans,
            last_execution.clone(),
        )
    };

    if let Err(err) = integrity_check(&paths.db) {
        tracing::error!("simulation failed: integrity check failed:\n{}", err);
        if result.is_err() {
            result = result
                .with_context(|| anyhow!("simulation failed: integrity check failed:\n{}", err));
        } else {
            result = Err(err);
        }
    } else if let Some(bugbase) = bugbase.as_mut() {
        bugbase.mark_successful_run(seed, cli_opts).unwrap();
    };

    // Print the seed, the locations of the database and the plan file at the end again for easily accessing them.
    println!("seed: {}", seed);
    println!("path: {}", paths.base.display());

    result
}

fn watch_mode(
    seed: u64,
    cli_opts: &SimulatorCLI,
    paths: &Paths,
    last_execution: Arc<Mutex<Execution>>,
) -> notify::Result<()> {
    let (tx, rx) = mpsc::channel::<notify::Result<notify::Event>>();
    println!("watching {:?}", paths.plan);
    // Use recommended_watcher() to automatically select the best implementation
    // for your platform. The `EventHandler` passed to this constructor can be a
    // closure, a `std::sync::mpsc::Sender`, a `crossbeam_channel::Sender`, or
    // another type the trait is implemented for.
    let mut watcher = notify::recommended_watcher(tx)?;

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    watcher.watch(&paths.plan, RecursiveMode::NonRecursive)?;
    // Block forever, printing out events as they come in
    for res in rx {
        match res {
            Ok(event) => {
                if let EventKind::Modify(ModifyKind::Data(DataChange::Content)) = event.kind {
                    tracing::info!("plan file modified, rerunning simulation");

                    let result = SandboxedResult::from(
                        std::panic::catch_unwind(|| {
                            let plan: Vec<Vec<Interaction>> =
                                InteractionPlan::compute_via_diff(&paths.plan);
                            let mut env = SimulatorEnv::new(seed, cli_opts, &paths.db);
                            plan.iter().for_each(|is| {
                                is.iter().for_each(|i| {
                                    i.shadow(&mut env);
                                });
                            });
                            let env = Arc::new(Mutex::new(env.clone_without_connections()));
                            watch::run_simulation(env, &mut [plan], last_execution.clone())
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
            Err(e) => println!("watch error: {:?}", e),
        }
    }

    Ok(())
}

fn run_simulator(
    seed: u64,
    bugbase: Option<&mut BugBase>,
    cli_opts: &SimulatorCLI,
    paths: &Paths,
    env: SimulatorEnv,
    plans: Vec<InteractionPlan>,
    last_execution: Arc<Mutex<Execution>>,
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

    let env = Arc::new(Mutex::new(env));
    let result = SandboxedResult::from(
        std::panic::catch_unwind(|| {
            run_simulation(env.clone(), &mut plans.clone(), last_execution.clone())
        }),
        last_execution.clone(),
    );

    if cli_opts.doublecheck {
        doublecheck(
            seed,
            bugbase,
            cli_opts,
            paths,
            &plans,
            last_execution.clone(),
            result,
        )
    } else {
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
                    let f = std::fs::File::create(&paths.history).unwrap();
                    let mut f = std::io::BufWriter::new(f);
                    for execution in history.history.iter() {
                        writeln!(
                            f,
                            "{} {} {}",
                            execution.connection_index,
                            execution.interaction_index,
                            execution.secondary_index
                        )
                        .unwrap();
                    }
                }

                tracing::error!("simulation failed: '{}'", error);
                tracing::info!("Starting to shrink");

                let shrunk_plans = plans
                    .iter()
                    .map(|plan| {
                        let shrunk = plan.shrink_interaction_plan(last_execution);
                        tracing::info!("{}", shrunk.stats());
                        shrunk
                    })
                    .collect::<Vec<_>>();

                // Write the shrunk plan to a file
                let mut f = std::fs::File::create(&paths.shrunk_plan).unwrap();
                f.write_all(shrunk_plans[0].to_string().as_bytes()).unwrap();

                let last_execution = Arc::new(Mutex::new(*last_execution));
                let env = SimulatorEnv::new(seed, cli_opts, &paths.shrunk_db);

                let env = Arc::new(Mutex::new(env));
                let shrunk = SandboxedResult::from(
                    std::panic::catch_unwind(|| {
                        run_simulation(
                            env.clone(),
                            &mut shrunk_plans.clone(),
                            last_execution.clone(),
                        )
                    }),
                    last_execution,
                );

                match (&shrunk, &result) {
                    (
                        SandboxedResult::Panicked { error: e1, .. },
                        SandboxedResult::Panicked { error: e2, .. },
                    )
                    | (
                        SandboxedResult::FoundBug { error: e1, .. },
                        SandboxedResult::FoundBug { error: e2, .. },
                    ) => {
                        if e1 != e2 {
                            tracing::error!(
                                ?shrunk,
                                ?result,
                                "shrinking failed, the error was not properly reproduced"
                            );
                            if let Some(bugbase) = bugbase {
                                bugbase
                                    .add_bug(seed, plans[0].clone(), Some(error.clone()), cli_opts)
                                    .unwrap();
                            }
                            Err(anyhow!("failed with error: '{}'", error))
                        } else {
                            tracing::info!(
                                "shrinking succeeded, reduced the plan from {} to {}",
                                plans[0].plan.len(),
                                shrunk_plans[0].plan.len()
                            );
                            // Save the shrunk database
                            if let Some(bugbase) = bugbase {
                                bugbase
                                    .add_bug(
                                        seed,
                                        shrunk_plans[0].clone(),
                                        Some(e1.clone()),
                                        cli_opts,
                                    )
                                    .unwrap();
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
                                .add_bug(seed, plans[0].clone(), Some(error.clone()), cli_opts)
                                .unwrap();
                        }
                        Err(anyhow!("failed with error: '{}'", error))
                    }
                }
            }
        }
    }
}

fn doublecheck(
    seed: u64,
    bugbase: Option<&mut BugBase>,
    cli_opts: &SimulatorCLI,
    paths: &Paths,
    plans: &[InteractionPlan],
    last_execution: Arc<Mutex<Execution>>,
    result: SandboxedResult,
) -> anyhow::Result<()> {
    let env = SimulatorEnv::new(seed, cli_opts, &paths.doublecheck_db);
    let env = Arc::new(Mutex::new(env));

    // Run the simulation again
    let result2 = SandboxedResult::from(
        std::panic::catch_unwind(|| {
            run_simulation(env.clone(), &mut plans.to_owned(), last_execution.clone())
        }),
        last_execution.clone(),
    );

    let doublecheck_result = match (result, result2) {
        (SandboxedResult::Correct, SandboxedResult::Panicked { .. }) => {
            Err("first run succeeded, but second run panicked.".to_string())
        }
        (SandboxedResult::FoundBug { .. }, SandboxedResult::Panicked { .. }) => {
            Err("first run failed an assertion, but second run panicked.".to_string())
        }
        (SandboxedResult::Panicked { .. }, SandboxedResult::Correct) => {
            Err("first run panicked, but second run succeeded.".to_string())
        }
        (SandboxedResult::Panicked { .. }, SandboxedResult::FoundBug { .. }) => {
            Err("first run panicked, but second run failed an assertion.".to_string())
        }
        (SandboxedResult::Correct, SandboxedResult::FoundBug { .. }) => {
            Err("first run succeeded, but second run failed an assertion.".to_string())
        }
        (SandboxedResult::FoundBug { .. }, SandboxedResult::Correct) => {
            Err("first run failed an assertion, but second run succeeded.".to_string())
        }
        (SandboxedResult::Correct, SandboxedResult::Correct)
        | (SandboxedResult::FoundBug { .. }, SandboxedResult::FoundBug { .. })
        | (SandboxedResult::Panicked { .. }, SandboxedResult::Panicked { .. }) => {
            // Compare the two database files byte by byte
            let db_bytes = std::fs::read(&paths.db).unwrap();
            let doublecheck_db_bytes = std::fs::read(&paths.doublecheck_db).unwrap();
            if db_bytes != doublecheck_db_bytes {
                Err(
                    "database files are different, check binary diffs for more details."
                        .to_string(),
                )
            } else {
                Ok(())
            }
        }
    };

    match doublecheck_result {
        Ok(_) => {
            tracing::info!("doublecheck succeeded");
            println!("doublecheck succeeded");
            Ok(())
        }
        Err(e) => {
            tracing::error!("doublecheck failed: '{}'", e);
            if let Some(bugbase) = bugbase {
                bugbase
                    .add_bug(seed, plans[0].clone(), Some(e.clone()), cli_opts)
                    .unwrap();
            }
            Err(anyhow!("doublecheck failed: '{}'", e))
        }
    }
}

fn differential_testing(
    seed: u64,
    bugbase: Option<&mut BugBase>,
    cli_opts: &SimulatorCLI,
    paths: &Paths,
    plans: Vec<InteractionPlan>,
    last_execution: Arc<Mutex<Execution>>,
) -> anyhow::Result<()> {
    let env = Arc::new(Mutex::new(SimulatorEnv::new(seed, cli_opts, &paths.db)));
    let rusqlite_env = Arc::new(Mutex::new(SimulatorEnv::new(
        seed,
        cli_opts,
        &paths.diff_db,
    )));

    let result = SandboxedResult::from(
        std::panic::catch_unwind(|| {
            let plan = plans[0].clone();
            differential::run_simulation(
                env,
                rusqlite_env,
                &|| rusqlite::Connection::open(paths.diff_db.clone()).unwrap(),
                &mut [plan],
                last_execution.clone(),
            )
        }),
        last_execution.clone(),
    );

    match result {
        SandboxedResult::Correct => {
            tracing::info!("simulation succeeded, output of Limbo conforms to SQLite");
            println!("simulation succeeded, output of Limbo conforms to SQLite");
            Ok(())
        }
        SandboxedResult::Panicked { error, .. } | SandboxedResult::FoundBug { error, .. } => {
            tracing::error!("simulation failed: '{}'", error);
            if let Some(bugbase) = bugbase {
                bugbase
                    .add_bug(seed, plans[0].clone(), Some(error.clone()), cli_opts)
                    .unwrap();
            }
            Err(anyhow!("simulation failed: '{}'", error))
        }
    }
}

#[derive(Debug)]
enum SandboxedResult {
    Panicked {
        error: String,
        last_execution: Execution,
    },
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
                let error = format!("{:?}", e);
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
    plan_path: fn(&Paths) -> &Path,
    db_path: fn(&Paths) -> &Path,
) -> (u64, SimulatorEnv, Vec<InteractionPlan>, Paths) {
    if let Some(seed) = &cli_opts.load {
        let seed = seed.parse::<u64>().expect("seed should be a number");
        let bugbase = bugbase.expect("BugBase must be enabled to load a bug");
        tracing::info!("seed={}", seed);
        let bug = bugbase
            .get_bug(seed)
            .unwrap_or_else(|| panic!("bug '{}' not found in bug base", seed));

        let paths = bugbase.paths(seed);
        if !paths.base.exists() {
            std::fs::create_dir_all(&paths.base).unwrap();
        }
        let env = SimulatorEnv::new(bug.seed(), cli_opts, db_path(&paths));

        let plan = match bug {
            Bug::Loaded(LoadedBug { plan, .. }) => plan.clone(),
            Bug::Unloaded { seed } => {
                let seed = *seed;
                bugbase
                    .load_bug(seed)
                    .unwrap_or_else(|_| panic!("could not load bug '{}' in bug base", seed))
                    .plan
                    .clone()
            }
        };

        std::fs::write(plan_path(&paths), plan.to_string()).unwrap();
        std::fs::write(
            plan_path(&paths).with_extension("json"),
            serde_json::to_string_pretty(&plan).unwrap(),
        )
        .unwrap();
        let plans = vec![plan];
        (seed, env, plans, paths)
    } else {
        let seed = cli_opts.seed.unwrap_or_else(|| {
            let mut rng = rand::thread_rng();
            rng.next_u64()
        });
        tracing::info!("seed={}", seed);

        let paths = if let Some(bugbase) = bugbase {
            let paths = bugbase.paths(seed);
            // Create the output directory if it doesn't exist
            if !paths.base.exists() {
                std::fs::create_dir_all(&paths.base)
                    .map_err(|e| format!("{:?}", e))
                    .unwrap();
            }
            paths
        } else {
            let dir = std::env::current_dir().unwrap().join("simulator-output");
            std::fs::create_dir_all(&dir).unwrap();
            Paths::new(&dir)
        };

        let mut env = SimulatorEnv::new(seed, cli_opts, &paths.db);

        tracing::info!("Generating database interaction plan...");

        let plans = (1..=env.opts.max_connections)
            .map(|_| InteractionPlan::arbitrary_from(&mut env.rng.clone(), &mut env))
            .collect::<Vec<_>>();

        // todo: for now, we only use 1 connection, so it's safe to use the first plan.
        let plan = &plans[0];
        tracing::info!("{}", plan.stats());
        std::fs::write(plan_path(&paths), plan.to_string()).unwrap();
        std::fs::write(
            plan_path(&paths).with_extension("json"),
            serde_json::to_string_pretty(&plan).unwrap(),
        )
        .unwrap();

        (seed, env, plans, paths)
    }
}

fn run_simulation(
    env: Arc<Mutex<SimulatorEnv>>,
    plans: &mut [InteractionPlan],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    tracing::info!("Executing database interaction plan...");

    let mut states = plans
        .iter()
        .map(|_| InteractionPlanState {
            stack: vec![],
            interaction_pointer: 0,
            secondary_pointer: 0,
        })
        .collect::<Vec<_>>();
    let result = execute_plans(env.clone(), plans, &mut states, last_execution);

    let env = env.lock().unwrap();
    env.io.print_stats();

    tracing::info!("Simulation completed");

    result
}

fn init_logger() {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("simulator.log")
        .unwrap();

    let requires_ansi = std::io::stdout().is_terminal();

    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(requires_ansi)
                .with_line_number(true)
                .without_time()
                .with_thread_ids(false),
        )
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(
            #[allow(deprecated)]
            tracing_subscriber::fmt::layer()
                .with_writer(file)
                .with_ansi(false)
                .fmt_fields(format::PrettyFields::new().with_ansi(false)) // with_ansi is deprecated, but I cannot find another way to remove ansi codes
                .with_line_number(true)
                .without_time()
                .with_thread_ids(false)
                .map_fmt_fields(|f| f.debug_alt()),
        )
        .try_init();
}

fn banner() {
    println!("{}", BANNER);
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
        result
            .iter_mut()
            .for_each(|row| *row = format!("- {}", row));
        anyhow::bail!("simulation failed: {}", result.join("\n"))
    }
    Ok(())
}
