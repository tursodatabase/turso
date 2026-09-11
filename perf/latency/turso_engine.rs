use crate::{Checkpoint, CheckpointMode, Config, Pacer, Run, Sample, TxnMode};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc, Barrier,
    },
    thread,
    time::{Duration, Instant},
};
use turso::{Builder, Database, Value};

static NEXT_ID: AtomicI64 = AtomicI64::new(1);

pub fn run(config: &Config) -> Run {
    // One OS thread per connection, each with its own single-threaded tokio
    // runtime, mirroring the SQLite driver. A shared multi-thread runtime
    // does not work here: with the syscall IO backend nothing in a
    // transaction ever yields, so a task that keeps a worker busy can leave
    // the runtime's timer undriven and the other connections asleep past
    // their slots, sometimes for the rest of the run.
    let db = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(setup(config));

    let ready = Arc::new(Barrier::new(config.connections + 1));
    let pacer = Arc::new(Pacer::new(config));
    let restarts = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    let stop = Arc::new(AtomicBool::new(false));
    let checkpointer = config.checkpointer.map(|interval| {
        let db = db.clone();
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(checkpointer(db, interval, stop))
        })
    });

    for _ in 0..config.connections {
        let db = db.clone();
        let ready = Arc::clone(&ready);
        let pacer = Arc::clone(&pacer);
        let restarts = Arc::clone(&restarts);
        let batch_size = config.batch_size;
        let timeout = config.timeout;
        let mode = config.mode;

        handles.push(thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(writer(
                db, ready, pacer, restarts, batch_size, timeout, mode,
            ))
        }));
    }

    // Release the connections. The pacer's clock starts when the first one
    // asks for work.
    ready.wait();

    let per_thread: Vec<Vec<Sample>> = handles
        .into_iter()
        .map(|h| h.join().expect("writer thread panicked"))
        .collect();
    let elapsed = pacer.elapsed();

    stop.store(true, Ordering::Relaxed);
    let start = pacer.started_at();
    let checkpoints = checkpointer
        .map(|h| h.join().expect("checkpointer thread panicked"))
        .unwrap_or_default()
        .into_iter()
        .map(|(started, took)| Checkpoint {
            at: started.saturating_duration_since(start),
            took,
        })
        .collect();

    eprintln!(
        "[turso] io backend {}, checkpoint mode {:?}, {} transaction restarts",
        config.io,
        config.checkpoint_mode,
        restarts.load(Ordering::Relaxed)
    );

    Run {
        per_thread,
        overran: pacer.overran(),
        elapsed,
        checkpoints,
    }
}

/// Runs a passive checkpoint every `interval` until told to stop, and returns
/// when each one started and how long it took. The writer's auto-checkpoint
/// is turned off in `setup` when this runs, so this connection is the only
/// one checkpointing.
async fn checkpointer(
    db: Database,
    interval: Duration,
    stop: Arc<AtomicBool>,
) -> Vec<(Instant, Duration)> {
    let conn = db.connect().unwrap();
    let mut checkpoints = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(interval).await;
        let started = Instant::now();
        // Drain the pragma's result row. An error (typically Busy) means this
        // round could not checkpoint; the next round tries again.
        if let Ok(mut rows) = conn.query("PRAGMA wal_checkpoint(PASSIVE)", ()).await {
            while let Ok(Some(_)) = rows.next().await {}
        }
        checkpoints.push((started, started.elapsed()));
    }
    checkpoints
}

async fn writer(
    db: Database,
    ready: Arc<Barrier>,
    pacer: Arc<Pacer>,
    restarts: Arc<AtomicU64>,
    batch_size: usize,
    timeout: Duration,
    mode: TxnMode,
) -> Vec<Sample> {
    let conn = db.connect().unwrap();
    conn.busy_timeout(timeout).unwrap();
    conn.execute("PRAGMA synchronous = FULL", ()).await.unwrap();
    let mut stmt = conn
        .prepare("INSERT INTO test_table (id, data) VALUES (?, ?)")
        .await
        .unwrap();

    // Prepared once: `Connection::execute` would parse and compile the
    // SQL again on every call, and that would be charged to the
    // transaction.
    let begin_sql = match mode {
        TxnMode::Immediate => "BEGIN IMMEDIATE",
        TxnMode::Concurrent => "BEGIN CONCURRENT",
    };
    let mut begin_stmt = conn.prepare(begin_sql).await.unwrap();
    let mut commit_stmt = conn.prepare("COMMIT").await.unwrap();

    let mut samples = Vec::new();

    ready.wait();

    while let Some(arrival) = pacer.next() {
        wait_until(arrival.scheduled).await;
        let t0 = Instant::now();

        // A concurrent transaction can find its snapshot stale, and the
        // only cure is to start over. The restarts stay inside the
        // sample, because the caller is still waiting through them.
        let mut attempts = 0u32;
        let (t1, t2, t3) = 'txn: loop {
            attempts += 1;
            begin_stmt.execute(()).await.unwrap();
            let t1 = Instant::now();

            for _ in 0..batch_size {
                let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
                let params = turso::params::Params::Positional(vec![
                    Value::Integer(id),
                    Value::Text(format!("data_{id}")),
                ]);
                match stmt.execute(params).await {
                    Ok(_) => {}
                    Err(turso::Error::BusySnapshot(_)) => {
                        restarts.fetch_add(1, Ordering::Relaxed);
                        conn.execute("ROLLBACK", ()).await.unwrap();
                        continue 'txn;
                    }
                    Err(e) => panic!("INSERT failed: {e}"),
                }
            }
            let t2 = Instant::now();

            match commit_stmt.execute(()).await {
                Ok(_) => {}
                Err(turso::Error::BusySnapshot(_)) => {
                    restarts.fetch_add(1, Ordering::Relaxed);
                    conn.execute("ROLLBACK", ()).await.unwrap();
                    continue 'txn;
                }
                Err(e) => panic!("COMMIT failed: {e}"),
            }
            break 'txn (t1, t2, Instant::now());
        };

        samples.push(Sample {
            scheduled_ns: arrival.offset.as_nanos() as u64,
            warmup: arrival.warmup,
            restarts: attempts - 1,
            queue_ns: t0.saturating_duration_since(arrival.scheduled).as_nanos() as u64,
            begin_ns: (t1 - t0).as_nanos() as u64,
            work_ns: (t2 - t1).as_nanos() as u64,
            commit_ns: (t3 - t2).as_nanos() as u64,
            total_ns: t3.saturating_duration_since(arrival.scheduled).as_nanos() as u64,
        });
    }

    samples
}

/// Same idea as `crate::wait_until`, but the long part of the wait is a tokio
/// sleep so the runtime gets to drive IO completions in the meantime.
///
/// Tokio's timer only ticks once a millisecond, so a task asked to wake up
/// at `deadline` can come back up to a millisecond late. That lateness would
/// be charged to the database as queueing time. So the tokio sleep stops
/// well short of the deadline, and the last stretch is a thread sleep plus a
/// spin, which land within microseconds.
async fn wait_until(deadline: Instant) {
    const TIMER_SLOP: Duration = Duration::from_micros(1500);
    let now = Instant::now();
    if deadline <= now {
        return;
    }
    if deadline - now > TIMER_SLOP {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline - TIMER_SLOP)).await;
    }
    crate::wait_until(deadline);
}

async fn setup(config: &Config) -> Database {
    let builder = Builder::new_local(&config.db_path).with_io(config.io.as_str());
    let builder = match (config.mode, config.checkpoint_mode) {
        (TxnMode::Concurrent, CheckpointMode::Passive) => {
            builder.experimental_mvcc_passive_checkpoint(true)
        }
        (TxnMode::Concurrent, CheckpointMode::Truncate) | (TxnMode::Immediate, _) => builder,
    };
    let db = builder.build().await.unwrap();

    let conn = db.connect().unwrap();
    if config.mode == TxnMode::Concurrent {
        conn.pragma_update("journal_mode", "mvcc").await.unwrap();
        if let Some(threshold) = config.mvcc_checkpoint_threshold {
            conn.pragma_update("mvcc_checkpoint_threshold", threshold)
                .await
                .unwrap();
        }
        if config.checkpointer.is_some() {
            // -1 turns the writer's auto-checkpoint off; the checkpointer
            // connection does it instead.
            conn.pragma_update("mvcc_checkpoint_threshold", -1)
                .await
                .unwrap();
        }
    }
    conn.execute("PRAGMA synchronous = FULL", ()).await.unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        )",
        (),
    )
    .await
    .unwrap();

    db
}
