use std::fs::File;
use std::io::{self, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use mvcc_write_bench::{
    run_with_csv_sink, BenchError, CheckpointPolicy, Engine, LogThreshold, RunSpec, StopWhen,
    Topology, Turso, CSV_HEADER,
};

#[derive(Parser, Debug)]
#[command(name = "mvcc-write-bench")]
#[command(about = "MVCC write-throughput bench (turso_core vs rusqlite)")]
struct Args {
    #[arg(long)]
    engine: String,

    #[arg(long)]
    checkpoint: Option<String>,

    #[arg(long)]
    topology: Option<String>,

    #[arg(long)]
    workers: Option<NonZeroUsize>,

    #[arg(long)]
    threads: Option<NonZeroUsize>,

    #[arg(long = "workers-per-thread")]
    workers_per_thread: Option<NonZeroUsize>,

    #[arg(long = "batch")]
    batch: NonZeroUsize,

    #[arg(long, conflicts_with = "txns")]
    duration: Option<String>,

    #[arg(long)]
    txns: Option<NonZeroU64>,

    #[arg(long)]
    repeats: NonZeroUsize,

    #[arg(long)]
    out: PathBuf,

    #[arg(long)]
    threshold: Option<String>,

    #[arg(long)]
    warmup: Option<String>,

    #[arg(long, default_value = "mvcc-write-bench.db")]
    path: PathBuf,

    #[arg(long = "busy-timeout", default_value = "30s")]
    busy_timeout: String,
}

fn main() {
    if let Err(err) = run_main() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run_main() -> Result<(), BenchError> {
    let args = Args::parse();
    let out = args.out.clone();
    let spec = spec_from_args(args)?;
    let mut file = File::create(&out)?;
    writeln!(file, "{CSV_HEADER}")?;
    file.flush()?;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "{CSV_HEADER}")?;
    stdout.flush()?;
    let mut tee = Tee {
        a: &mut file,
        b: &mut stdout,
    };
    let _report = run_with_csv_sink(&spec, Some(&mut tee))?;
    Ok(())
}

struct Tee<'a> {
    a: &'a mut dyn Write,
    b: &'a mut dyn Write,
}

impl Write for Tee<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.a.write_all(buf)?;
        self.b.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.a.flush()?;
        self.b.flush()
    }
}

fn spec_from_args(args: Args) -> Result<RunSpec, BenchError> {
    let engine = parse_engine(&args)?;
    let stop = parse_stop(args.duration.as_deref(), args.txns)?;
    let warmup = match args.warmup.as_deref() {
        None => StopWhen::Duration(Duration::ZERO),
        Some(s) => StopWhen::Duration(parse_duration(s)?),
    };
    let busy_timeout = parse_duration(&args.busy_timeout)?;
    Ok(RunSpec {
        engine,
        batch_size: args.batch,
        stop,
        warmup,
        repeats: args.repeats,
        path: args.path,
        busy_timeout,
    })
}

fn parse_engine(args: &Args) -> Result<Engine, BenchError> {
    match args.engine.as_str() {
        "sqlite" => {
            if args.checkpoint.is_some() || args.topology.is_some() {
                return Err(BenchError::invalid_spec(
                    "sqlite has no checkpoint or topology",
                ));
            }
            Ok(Engine::Sqlite)
        }
        "turso" => {
            let checkpoint =
                parse_checkpoint(args.checkpoint.as_deref(), args.threshold.as_deref())?;
            let topology = parse_topology(args)?;
            Ok(Engine::Turso(Turso {
                checkpoint,
                topology,
            }))
        }
        _ => Err(BenchError::invalid_spec("engine must be turso or sqlite")),
    }
}

fn parse_checkpoint(
    checkpoint: Option<&str>,
    threshold: Option<&str>,
) -> Result<CheckpointPolicy, BenchError> {
    let th = parse_threshold(threshold)?;
    match checkpoint {
        Some("truncate") => Ok(CheckpointPolicy::Truncate(th)),
        Some("passive") => Ok(CheckpointPolicy::Passive(th)),
        None => Err(BenchError::invalid_spec(
            "turso requires --checkpoint truncate|passive",
        )),
        Some(_) => Err(BenchError::invalid_spec(
            "checkpoint must be truncate or passive",
        )),
    }
}

fn parse_threshold(raw: Option<&str>) -> Result<LogThreshold, BenchError> {
    match raw {
        None | Some("default") => Ok(LogThreshold::Default),
        Some("every-commit") => Ok(LogThreshold::EveryCommit),
        Some("disabled") => Ok(LogThreshold::Disabled),
        Some(s) => {
            let n: u64 = s.parse().map_err(|_| {
                BenchError::invalid_spec("threshold must be default|every-commit|disabled|integer")
            })?;
            match NonZeroU64::new(n) {
                Some(nz) => Ok(LogThreshold::Bytes(nz)),
                None => Ok(LogThreshold::EveryCommit),
            }
        }
    }
}

fn parse_topology(args: &Args) -> Result<Topology, BenchError> {
    match args.topology.as_deref() {
        Some("coop") => {
            let workers = args
                .workers
                .ok_or_else(|| BenchError::invalid_spec("coop topology requires --workers"))?;
            Ok(Topology::Cooperative { workers })
        }
        Some("io-pump") => {
            let workers = args
                .workers
                .ok_or_else(|| BenchError::invalid_spec("io-pump topology requires --workers"))?;
            Ok(Topology::IoPump { workers })
        }
        Some("threads") => Ok(Topology::Threads {
            threads: args
                .threads
                .ok_or_else(|| BenchError::invalid_spec("threads topology requires --threads"))?,
            workers_per_thread: args.workers_per_thread.ok_or_else(|| {
                BenchError::invalid_spec("threads topology requires --workers-per-thread")
            })?,
        }),
        Some("threads-pump") => Ok(Topology::ThreadsPump {
            threads: args.threads.ok_or_else(|| {
                BenchError::invalid_spec("threads-pump topology requires --threads")
            })?,
            workers_per_thread: args.workers_per_thread.ok_or_else(|| {
                BenchError::invalid_spec("threads-pump topology requires --workers-per-thread")
            })?,
        }),
        None => Err(BenchError::invalid_spec(
            "turso requires --topology coop|io-pump|threads|threads-pump",
        )),
        Some(_) => Err(BenchError::invalid_spec(
            "topology must be coop, io-pump, threads, or threads-pump",
        )),
    }
}

fn parse_stop(duration: Option<&str>, txns: Option<NonZeroU64>) -> Result<StopWhen, BenchError> {
    match (duration, txns) {
        (Some(d), None) => Ok(StopWhen::Duration(parse_duration(d)?)),
        (None, Some(n)) => Ok(StopWhen::Transactions(n)),
        _ => Err(BenchError::invalid_spec(
            "exactly one of --duration or --txns is required",
        )),
    }
}

fn parse_duration(raw: &str) -> Result<Duration, BenchError> {
    if let Some(ms) = raw.strip_suffix("ms") {
        let n: u64 = ms
            .parse()
            .map_err(|_| BenchError::invalid_spec("duration must look like 10s or 100ms"))?;
        return Ok(Duration::from_millis(n));
    }
    if let Some(s) = raw.strip_suffix('s') {
        let n: u64 = s
            .parse()
            .map_err(|_| BenchError::invalid_spec("duration must look like 10s or 100ms"))?;
        return Ok(Duration::from_secs(n));
    }
    if let Some(m) = raw.strip_suffix('m') {
        let n: u64 = m
            .parse()
            .map_err(|_| BenchError::invalid_spec("duration must look like 10s or 100ms"))?;
        return Ok(Duration::from_secs(n.saturating_mul(60)));
    }
    Err(BenchError::invalid_spec(
        "duration must look like 10s or 100ms",
    ))
}
