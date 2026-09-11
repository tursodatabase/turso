use crate::{Checkpoint, CheckpointMode, Clock, Config, Run, Sample, TxnMode, COMMITTED, NEXT_ID};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    },
    thread,
    time::{Duration, Instant},
};
use turso::{Builder, Database, Value};

pub fn run(config: &Config) -> Run {
    // One OS thread per connection, each with its own single-threaded tokio
    // runtime. A shared multi-thread runtime does not work here: with the
    // syscall IO backend nothing in a transaction ever yields, so a task
    // that keeps a worker busy can starve the others.
    let db = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(setup(config));

    let ready = Arc::new(Barrier::new(config.connections + 1));
    let clock = Arc::new(Clock::new(config));
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

    let mut handles = Vec::new();
    for _ in 0..config.connections {
        let db = db.clone();
        let ready = Arc::clone(&ready);
        let clock = Arc::clone(&clock);
        let batch_size = config.batch_size;
        let timeout = config.timeout;
        let mode = config.mode;
        let run = config.run;

        handles.push(thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(connection(db, ready, clock, batch_size, timeout, mode, run))
        }));
    }

    ready.wait();

    let per_connection: Vec<Vec<Sample>> = handles
        .into_iter()
        .map(|h| h.join().expect("connection thread panicked"))
        .collect();
    let elapsed = clock.elapsed();

    stop.store(true, Ordering::Relaxed);
    let start = clock.started_at();
    let checkpoints = checkpointer
        .map(|h| h.join().expect("checkpointer thread panicked"))
        .unwrap_or_default()
        .into_iter()
        .map(|(started, took)| Checkpoint {
            at: started.saturating_duration_since(start),
            took,
        })
        .collect();

    let rows_in_table = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(count_rows(&db));

    eprintln!(
        "[turso] io backend {}, checkpoint mode {:?}",
        config.io, config.checkpoint_mode
    );

    Run {
        per_connection,
        checkpoints,
        elapsed,
        rows_in_table,
    }
}

/// Runs a passive checkpoint every `interval` until told to stop, and returns
/// when each one started and how long it took. The writers' auto-checkpoint
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

#[allow(clippy::too_many_arguments)]
async fn connection(
    db: Database,
    ready: Arc<Barrier>,
    clock: Arc<Clock>,
    batch_size: usize,
    timeout: Duration,
    mode: TxnMode,
    run: usize,
) -> Vec<Sample> {
    let conn = db.connect().unwrap();
    conn.busy_timeout(timeout).unwrap();
    conn.execute("PRAGMA synchronous = FULL", ()).await.unwrap();
    let mut stmt = conn
        .prepare("INSERT INTO test_table (id, data) VALUES (?, ?)")
        .await
        .unwrap();
    let begin_sql = match mode {
        TxnMode::Immediate => "BEGIN IMMEDIATE",
        TxnMode::Concurrent => "BEGIN CONCURRENT",
    };
    let mut begin_stmt = conn.prepare(begin_sql).await.unwrap();
    let mut commit_stmt = conn.prepare("COMMIT").await.unwrap();

    let mut samples = Vec::new();

    ready.wait();

    while let Some(start) = clock.next() {
        // A concurrent transaction can find its snapshot stale, and the
        // only cure is to start over. The restarts stay inside the sample,
        // because the caller is still waiting through them.
        let mut attempts = 0u32;
        let (t1, t2, t3) = 'txn: loop {
            attempts += 1;
            begin_stmt.execute(()).await.unwrap();
            let t1 = Instant::now();

            for _ in 0..batch_size {
                let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
                let params = turso::params::Params::Positional(vec![
                    Value::Integer(id as i64),
                    Value::Text(format!("run{run}_{id}")),
                ]);
                match stmt.execute(params).await {
                    Ok(_) => {}
                    Err(turso::Error::BusySnapshot(_)) => {
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
                    conn.execute("ROLLBACK", ()).await.unwrap();
                    continue 'txn;
                }
                Err(e) => panic!("COMMIT failed: {e}"),
            }
            break 'txn (t1, t2, Instant::now());
        };
        COMMITTED.fetch_add(1, Ordering::Relaxed);

        samples.push(Sample {
            started_ns: start.offset.as_nanos() as u64,
            warmup: start.warmup,
            restarts: attempts - 1,
            begin_ns: (t1 - start.at).as_nanos() as u64,
            work_ns: (t2 - t1).as_nanos() as u64,
            commit_ns: (t3 - t2).as_nanos() as u64,
            total_ns: (t3 - start.at).as_nanos() as u64,
        });
    }

    samples
}

async fn count_rows(db: &Database) -> u64 {
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query("SELECT count(*) FROM test_table", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().expect("count(*) returns a row");
    match row.get_value(0).unwrap() {
        Value::Integer(n) => n as u64,
        other => panic!("count(*) returned {other:?}"),
    }
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
            // -1 turns the writers' auto-checkpoint off; the checkpointer
            // connection does it instead.
            conn.pragma_update("mvcc_checkpoint_threshold", -1)
                .await
                .unwrap();
        }
    }
    conn.execute("PRAGMA synchronous = FULL", ()).await.unwrap();
    conn.execute(
        "CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        )",
        (),
    )
    .await
    .unwrap();

    db
}
