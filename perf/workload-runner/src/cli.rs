use crate::{
    db::{Database, Engine, Journal, Settings, Transaction},
    process::{run_child, Outcome},
    retry::Retry,
    run, runtime,
    workloads::{self, Config, Scenario},
    Load, Report,
};
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    path::PathBuf,
    process::{Command, Stdio},
    sync::atomic::Ordering,
    time::Duration,
};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, ValueEnum)]
pub enum Target {
    SqliteWal,
    TursoWal,
    TursoMvcc,
}

#[derive(Parser)]
#[command(about = "Run compiled Rust workloads in isolated engine processes")]
struct Args {
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "sqlite-wal,turso-wal,turso-mvcc"
    )]
    targets: Vec<Target>,
    #[arg(long, value_enum, default_value = "insert")]
    workload: Scenario,
    #[arg(short = 'c', long, default_value_t = 1)]
    connections: usize,
    #[arg(short = 'b', long, default_value_t = 100)]
    batch_size: u64,
    #[arg(short = 'd', long, default_value_t = 10)]
    duration: u64,
    #[arg(long, default_value_t = 1)]
    warmup: u64,
    #[arg(long)]
    count: Option<u64>,
    #[arg(long)]
    rate: Option<f64>,
    #[arg(long)]
    poisson: bool,
    #[arg(long, default_value_t = 1)]
    repetitions: u64,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long, default_value_t = 120)]
    hard_timeout: u64,
    #[arg(long, default_value_t = 100)]
    max_attempts: u64,
    #[arg(long, default_value_t = 5000)]
    retry_timeout_ms: u64,
    #[arg(long, default_value_t = 1)]
    backoff_ms: u64,
    #[arg(long, default_value_t = 100)]
    busy_timeout_ms: u64,
    #[arg(long, default_value_t = 100)]
    checkpoint_ms: u64,
    #[arg(long, default_value = "syscall")]
    io: String,
    #[arg(long)]
    raw_samples: bool,
    #[arg(long, default_value = "workload-results.json")]
    output: PathBuf,
    #[arg(long, hide = true)]
    child: Option<PathBuf>,
    #[arg(long, hide = true)]
    result: Option<PathBuf>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Request {
    config: Config,
    settings: Settings,
    database: PathBuf,
    repetition: u64,
}

#[derive(Serialize, Deserialize)]
pub struct RunResult {
    pub repetition: u64,
    pub config: Config,
    pub settings: Settings,
    pub sqlite_version: String,
    pub turso_version: String,
    pub git_revision: String,
    pub git_dirty: bool,
    pub rustc: String,
    pub actual_settings: std::collections::BTreeMap<String, String>,
    pub report: Option<Report>,
    pub verified: bool,
    pub error: Option<String>,
}

#[derive(Serialize)]
struct Results {
    schema_version: u32,
    runs: Vec<RunResult>,
}

pub fn main() {
    finish(entry(Args::parse()));
}

pub fn latency_main() {
    let mut args = Args::parse();
    args.rate.get_or_insert(1000.0);
    finish(entry(args));
}

fn finish(result: std::result::Result<(), Box<dyn std::error::Error>>) {
    if let Err(e) = result {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn entry(args: Args) -> std::result::Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = args.child {
        let request: Request = serde_json::from_reader(File::open(path)?)?;
        let result = execute(&request);
        let path = args.result.ok_or("missing child result path")?;
        serde_json::to_writer(File::create_new(path)?, &result)?;
        return Ok(());
    }
    if args.repetitions == 0 || args.hard_timeout == 0 || args.targets.is_empty() {
        return Err("repetitions, hard timeout and target list must be nonempty".into());
    }
    if args.poisson && args.rate.is_none() {
        return Err("--poisson requires --rate".into());
    }
    let output = File::create_new(&args.output)?;
    let directory = tempfile::tempdir()?;
    let mut runs = Vec::new();
    for repetition in 0..args.repetitions {
        let mut targets = args.targets.clone();
        if repetition % 2 == 1 {
            targets.reverse();
        }
        for (index, target) in targets.into_iter().enumerate() {
            let tag = format!("{repetition}-{index}");
            let config = Config {
                scenario: args.workload.clone(),
                connections: args.connections,
                batch_size: args.batch_size,
                warmup: Duration::from_secs(args.warmup),
                duration: Duration::from_secs(args.duration),
                count: args.count,
                load: match args.rate {
                    None => Load::Continuous,
                    Some(per_second) if args.poisson => Load::Poisson { per_second },
                    Some(per_second) => Load::Fixed { per_second },
                },
                seed: args.seed.wrapping_add(repetition),
                retry: Retry {
                    max_attempts: args.max_attempts,
                    timeout: Duration::from_millis(args.retry_timeout_ms),
                    backoff: Duration::from_millis(args.backoff_ms),
                },
                checkpoint_interval: Duration::from_millis(args.checkpoint_ms),
                raw_samples: args.raw_samples,
            };
            let mut settings = settings(target);
            settings.io.clone_from(&args.io);
            settings.busy_timeout_ms = args.busy_timeout_ms;
            if matches!(args.workload, Scenario::HeldSnapshot) {
                settings.wal_autocheckpoint_pages = 0;
                settings.mvcc_checkpoint_bytes = -1;
            }
            let request = Request {
                config,
                settings,
                database: directory.path().join(format!("{tag}.db")),
                repetition,
            };
            let plan = workloads::build(request.config.clone(), request.settings.transaction)?;
            plan.plan.validate(&plan.groups)?;
            request.settings.validate()?;
            let input = directory.path().join(format!("{tag}-request.json"));
            let result = directory.path().join(format!("{tag}-result.json"));
            serde_json::to_writer(File::create_new(&input)?, &request)?;
            let mut child = Command::new(std::env::current_exe()?);
            child
                .arg("--child")
                .arg(&input)
                .arg("--result")
                .arg(&result)
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit());
            let failure = match run_child(&mut child, Duration::from_secs(args.hard_timeout))? {
                Outcome::Exited(status) if status.success() => None,
                Outcome::Exited(status) => Some(format!("child exited with {status}")),
                Outcome::TimedOut => {
                    Some("hard timeout: child terminated; results incomplete".into())
                }
            };
            let run = if let Some(error) = failure {
                let mut run = empty_result(&request);
                run.error = Some(error);
                run
            } else {
                match File::open(&result)
                    .and_then(|f| serde_json::from_reader(f).map_err(std::io::Error::other))
                {
                    Ok(result) => result,
                    Err(e) => {
                        let mut run = empty_result(&request);
                        run.error = Some(format!("invalid child result: {e}"));
                        run
                    }
                }
            };
            display(target, &run);
            runs.push(run);
        }
    }
    let complete = runs
        .iter()
        .all(|r| r.error.is_none() && r.verified && r.report.as_ref().is_some_and(|r| r.complete));
    serde_json::to_writer_pretty(
        output,
        &Results {
            schema_version: 1,
            runs,
        },
    )?;
    if !complete {
        return Err("one or more runs were incomplete; see JSON results".into());
    }
    Ok(())
}

fn execute(request: &Request) -> RunResult {
    let mut result = empty_result(request);
    let outcome = (|| -> crate::db::Result<()> {
        let rt = runtime();
        let db = rt.block_on(Database::create(
            &request.database,
            request.settings.clone(),
        ))?;
        result.actual_settings = rt.block_on(async {
            let conn = db.connect().await?;
            let mut actual = std::collections::BTreeMap::new();
            for pragma in ["journal_mode", "synchronous", "busy_timeout"] {
                actual.insert(
                    pragma.into(),
                    format!("{:?}", conn.query(&format!("PRAGMA {pragma}"), &[]).await?),
                );
            }
            let checkpoint = if request.settings.journal == Journal::Mvcc {
                "mvcc_checkpoint_threshold"
            } else {
                "wal_autocheckpoint"
            };
            actual.insert(
                checkpoint.into(),
                format!(
                    "{:?}",
                    conn.query(&format!("PRAGMA {checkpoint}"), &[]).await?
                ),
            );
            Ok::<_, crate::db::Error>(actual)
        })?;
        let workload = workloads::build(request.config.clone(), request.settings.transaction)?;
        result.report = Some(run(db, workload.plan, workload.groups)?);
        result.verified = workload.verified.load(Ordering::Acquire);
        Ok(())
    })();
    if let Err(e) = outcome {
        result.error = Some(e.to_string());
    }
    result
}

fn empty_result(request: &Request) -> RunResult {
    RunResult {
        repetition: request.repetition,
        config: request.config.clone(),
        settings: request.settings.clone(),
        sqlite_version: rusqlite::version().into(),
        turso_version: env!("CARGO_PKG_VERSION").into(),
        git_revision: env!("WORKLOAD_GIT_REVISION").into(),
        git_dirty: env!("WORKLOAD_GIT_DIRTY") == "true",
        rustc: env!("WORKLOAD_RUSTC").into(),
        actual_settings: Default::default(),
        report: None,
        verified: false,
        error: None,
    }
}

pub fn settings(target: Target) -> Settings {
    Settings {
        engine: match target {
            Target::SqliteWal => Engine::Sqlite,
            _ => Engine::Turso,
        },
        journal: match target {
            Target::TursoMvcc => Journal::Mvcc,
            _ => Journal::Wal,
        },
        transaction: match target {
            Target::TursoMvcc => Transaction::Concurrent,
            _ => Transaction::Immediate,
        },
        busy_timeout_ms: 100,
        io: "syscall".into(),
        wal_autocheckpoint_pages: 1000,
        mvcc_checkpoint_bytes: 4_120_000,
        mvcc_passive_checkpoint: matches!(target, Target::TursoMvcc),
    }
}

fn display(target: Target, result: &RunResult) {
    eprintln!(
        "{target:?} repetition={} verified={} error={:?}",
        result.repetition, result.verified, result.error
    );
    if let Some(report) = &result.report {
        for stage in &report.stages {
            for (name, metrics) in &stage.operations {
                eprintln!("  {} {}: {} ok, {} failures, {} retries, {:.1}/s, response p50/p95/p99/max {:.3}/{:.3}/{:.3}/{:.3} ms, drain={}",
                        stage.name, name, metrics.successes, metrics.failures, metrics.retries, metrics.throughput_per_second,
                        metrics.response.p50_ns as f64 / 1e6, metrics.response.p95_ns as f64 / 1e6, metrics.response.p99_ns as f64 / 1e6, metrics.response.max_ns as f64 / 1e6, metrics.drain_completions);
            }
            eprintln!(
                "  {} complete={} skipped={} backlog={} unfinished_workers={} unfinished_operations={} drain_ms={:.3}",
                stage.name,
                stage.complete,
                stage.skipped,
                stage.backlog_at_deadline,
                stage.unfinished_workers,
                stage.unfinished_operations,
                stage.drain_ns as f64 / 1e6
            );
        }
        for error in &report.errors {
            eprintln!("  {error}");
        }
    }
}
