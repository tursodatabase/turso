use std::sync::{Arc, Barrier, OnceLock};
use std::thread;
use std::time::Instant;

use turso_core::{Connection, Database, DatabaseOpts, OpenOptions, SqliteDialect, UringIO, Value};

use crate::coop::{drive_coop, CoopStats, IoPump, RingOwner, StopClock, ThreadStats};
use crate::latency::TxnLatency;
use crate::observe::{LogTrace, LogWatch};
use crate::run::unlink_db_tree;
use crate::spec::{
    BenchError, CheckpointPolicy, ResultRow, RunSpec, StopWhen, Topology, Turso, SCHEMA,
};
use crate::worker::{Worker, WorkerId};

struct TursoSession {
    db: Arc<Database>,
}

pub(crate) fn run_turso_once(spec: &RunSpec, turso: &Turso) -> Result<ResultRow, BenchError> {
    unlink_db_tree(&spec.path)?;
    let session = open_turso(spec, turso)?;
    let batch_size = spec.batch_size.get();
    let (stats, elapsed, log_bytes, wal_bytes, trace) = match turso.topology {
        Topology::Cooperative { workers } => run_coop(&session, spec, workers.get(), false)?,
        Topology::IoPump { workers } => run_coop(&session, spec, workers.get(), true)?,
        Topology::Threads {
            threads,
            workers_per_thread,
        } => run_threads(
            &session,
            spec,
            threads.get(),
            workers_per_thread.get(),
            batch_size,
            false,
        )?,
        Topology::ThreadsPump {
            threads,
            workers_per_thread,
        } => run_threads(
            &session,
            spec,
            threads.get(),
            workers_per_thread.get(),
            batch_size,
            true,
        )?,
    };
    let (engine, topology, workers, threads, checkpoint, threshold) = spec.engine.labels();
    Ok(ResultRow {
        engine,
        topology,
        workers,
        threads,
        batch_size,
        checkpoint,
        threshold,
        stop: spec.stop,
        repeat: 0,
        inserts: stats.inserts,
        committed_txns: stats.txns,
        elapsed,
        busy: stats.busy,
        busy_snapshot: stats.busy_snapshots,
        schema_updated: stats.schema_updated,
        log_bytes,
        wal_bytes,
        log_peak_bytes: trace.peak_bytes,
        checkpoints_observed: trace.checkpoints_observed,
        sampled: trace.sampled,
        latency: TxnLatency::from_nanos(&stats.latencies_ns),
    })
}

fn open_turso(spec: &RunSpec, turso: &Turso) -> Result<TursoSession, BenchError> {
    let path = spec
        .path
        .to_str()
        .ok_or_else(|| BenchError::engine("database path is not valid UTF-8"))?;
    let io = match UringIO::new() {
        Ok(io) => Arc::new(io),
        Err(err) => {
            return Err(BenchError::engine(format!("UringIO::new failed: {err}")));
        }
    };
    let passive = matches!(turso.checkpoint, CheckpointPolicy::Passive(_));
    let opts = DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(passive);
    let db = Database::open(
        io,
        path,
        OpenOptions::new(Arc::new(SqliteDialect)).db_opts(opts),
    )?;
    let setup = db.connect()?;
    setup.set_busy_timeout(spec.busy_timeout);
    setup.execute("PRAGMA journal_mode = 'mvcc'")?;
    assert_journal_mode_mvcc(&setup)?;
    setup.execute("PRAGMA synchronous = FULL")?;
    let threshold = match turso.checkpoint {
        CheckpointPolicy::Truncate(th) | CheckpointPolicy::Passive(th) => th.to_pragma_i64(),
    };
    setup.execute(format!("PRAGMA mvcc_checkpoint_threshold = {threshold}"))?;
    setup.execute(SCHEMA)?;
    Ok(TursoSession { db })
}

fn assert_journal_mode_mvcc(conn: &Arc<Connection>) -> Result<(), BenchError> {
    let mut stmt = conn.prepare("PRAGMA journal_mode")?;
    let rows = stmt.run_collect_rows()?;
    let mode = rows
        .first()
        .and_then(|row| row.first())
        .and_then(Value::to_text)
        .unwrap_or("");
    if !mode.eq_ignore_ascii_case("mvcc") {
        return Err(BenchError::engine(format!(
            "PRAGMA journal_mode read-back was {mode:?}, expected mvcc"
        )));
    }
    Ok(())
}

fn make_workers(
    db: &Arc<Database>,
    spec: &RunSpec,
    n: usize,
    id_base: u32,
    concurrency: usize,
) -> Result<Vec<Worker>, BenchError> {
    let mut workers = Vec::with_capacity(n);
    for i in 0..n {
        let conn = db.connect()?;
        workers.push(Worker::new(
            WorkerId(id_base + i as u32),
            conn,
            spec.batch_size.get(),
            concurrency,
            spec.busy_timeout,
        )?);
    }
    Ok(workers)
}

fn drive_maybe_pump(
    io: &Arc<dyn turso_core::IO>,
    workers: &mut [Worker],
    stop: &StopClock,
    watch: &mut LogWatch,
    pump: bool,
) -> Result<CoopStats, BenchError> {
    if pump {
        let p = IoPump::spawn(Arc::clone(io));
        let stats = drive_coop(RingOwner::Pump(p.waker()), workers, stop, watch);
        let joined = p.join();
        let stats = stats?;
        joined?;
        Ok(stats)
    } else {
        drive_coop(RingOwner::Inline(io.as_ref()), workers, stop, watch)
    }
}

fn run_coop(
    session: &TursoSession,
    spec: &RunSpec,
    n_workers: usize,
    pump: bool,
) -> Result<(ThreadStats, std::time::Duration, u64, u64, LogTrace), BenchError> {
    let mut workers = make_workers(&session.db, spec, n_workers, 0, n_workers)?;
    if !warmup_is_skip(spec.warmup) {
        let warmup = StopClock::from_stop(spec.warmup);
        let mut silent = LogWatch::silent();
        drive_maybe_pump(&session.db.io, &mut workers, &warmup, &mut silent, pump)?;
        for w in &mut workers {
            w.reset_counters();
        }
    }
    let mut watch = LogWatch::open(&spec.path);
    watch.sample();
    let t0 = Instant::now();
    let measure = StopClock::from_stop(spec.stop);
    let stats = drive_maybe_pump(&session.db.io, &mut workers, &measure, &mut watch, pump)?;
    let elapsed = t0.elapsed();
    let (log_bytes, wal_bytes, trace) = watch.finish();
    Ok((stats.into(), elapsed, log_bytes, wal_bytes, trace))
}

fn warmup_is_skip(warmup: StopWhen) -> bool {
    matches!(warmup, StopWhen::Duration(d) if d.is_zero())
}

fn run_threads(
    session: &TursoSession,
    spec: &RunSpec,
    threads: usize,
    workers_per_thread: usize,
    batch_size: usize,
    pump: bool,
) -> Result<(ThreadStats, std::time::Duration, u64, u64, LogTrace), BenchError> {
    let concurrency = threads * workers_per_thread;
    let start_measure = Arc::new(Barrier::new(threads));
    let t0: Arc<OnceLock<Instant>> = Arc::new(OnceLock::new());
    let skip_warmup = warmup_is_skip(spec.warmup);
    let shared_txn_clock = match spec.stop {
        StopWhen::Transactions(_) => Some(StopClock::from_stop(spec.stop)),
        StopWhen::Duration(_) => None,
    };
    let io_pump = if pump {
        Some(IoPump::spawn(Arc::clone(&session.db.io)))
    } else {
        None
    };
    let pump_wake = io_pump.as_ref().map(|p| p.waker().clone());
    let mut joins = Vec::with_capacity(threads);
    for thread_idx in 0..threads {
        let db = Arc::clone(&session.db);
        let path = spec.path.clone();
        let busy_timeout = spec.busy_timeout;
        let warmup = spec.warmup;
        let stop = spec.stop;
        let start_measure = Arc::clone(&start_measure);
        let t0 = Arc::clone(&t0);
        let shared_txn_clock = shared_txn_clock.clone();
        let pump_wake = pump_wake.clone();
        let handle = thread::spawn(
            move || -> Result<(ThreadStats, LogTrace, u64, u64), BenchError> {
                let id_base = (thread_idx * workers_per_thread) as u32;
                let mut workers = Vec::with_capacity(workers_per_thread);
                for i in 0..workers_per_thread {
                    let conn = db.connect()?;
                    workers.push(Worker::new(
                        WorkerId(id_base + i as u32),
                        conn,
                        batch_size,
                        concurrency,
                        busy_timeout,
                    )?);
                }
                let drive = |workers: &mut [Worker],
                             clock: &StopClock,
                             watch: &mut LogWatch|
                 -> Result<CoopStats, BenchError> {
                    match pump_wake.as_ref() {
                        Some(wake) => drive_coop(RingOwner::Pump(wake), workers, clock, watch),
                        None => {
                            drive_coop(RingOwner::Inline(db.io.as_ref()), workers, clock, watch)
                        }
                    }
                };
                if !skip_warmup {
                    let warmup_clock = StopClock::from_stop(warmup);
                    let mut silent = LogWatch::silent();
                    drive(&mut workers, &warmup_clock, &mut silent)?;
                    for w in &mut workers {
                        w.reset_counters();
                    }
                }
                let mut watch = LogWatch::open(&path);
                start_measure.wait();
                t0.get_or_init(Instant::now);
                watch.sample();
                let measure = match shared_txn_clock {
                    Some(clock) => clock,
                    None => StopClock::from_stop(stop),
                };
                let stats = drive(&mut workers, &measure, &mut watch)?;
                let (log_bytes, wal_bytes, trace) = watch.finish();
                Ok((stats.into(), trace, log_bytes, wal_bytes))
            },
        );
        joins.push(handle);
    }
    let mut totals = ThreadStats {
        inserts: 0,
        txns: 0,
        busy: 0,
        busy_snapshots: 0,
        schema_updated: 0,
        latencies_ns: Vec::new(),
    };
    let mut trace = LogTrace::default();
    let mut log_bytes = 0u64;
    let mut wal_bytes = 0u64;
    let mut first_err: Option<BenchError> = None;
    for handle in joins {
        match handle.join() {
            Ok(Ok((stats, t, lb, wb))) => {
                totals.inserts += stats.inserts;
                totals.txns += stats.txns;
                totals.busy += stats.busy;
                totals.busy_snapshots += stats.busy_snapshots;
                totals.schema_updated += stats.schema_updated;
                totals.latencies_ns.extend(stats.latencies_ns);
                trace = trace.merge(t);
                log_bytes = log_bytes.max(lb);
                wal_bytes = wal_bytes.max(wb);
            }
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(_) => {
                if first_err.is_none() {
                    first_err = Some(BenchError::thread_panicked());
                }
            }
        }
    }
    if let Some(p) = io_pump {
        p.join()?;
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    let elapsed = t0.get().map(Instant::elapsed).unwrap_or_default();
    Ok((totals, elapsed, log_bytes, wal_bytes, trace))
}
