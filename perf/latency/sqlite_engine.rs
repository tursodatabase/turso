use crate::{wait_until, Checkpoint, Config, Pacer, Run, Sample};
use rusqlite::Connection;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc, Barrier,
    },
    thread,
    time::{Duration, Instant},
};

static NEXT_ID: AtomicI64 = AtomicI64::new(1);

pub fn run(config: &Config) -> Run {
    setup(config);

    let ready = Arc::new(Barrier::new(config.connections + 1));
    let pacer = Arc::new(Pacer::new(config));
    let mut handles = Vec::new();

    let stop = Arc::new(AtomicBool::new(false));
    let checkpointer = config.checkpointer.map(|interval| {
        let stop = Arc::clone(&stop);
        let db_path = config.db_path.clone();
        let timeout = config.timeout;
        thread::spawn(move || checkpointer(&db_path, timeout, interval, &stop))
    });

    for _ in 0..config.connections {
        let ready = Arc::clone(&ready);
        let pacer = Arc::clone(&pacer);
        let db_path = config.db_path.clone();
        let batch_size = config.batch_size;
        let timeout = config.timeout;
        let own_checkpoints = config.checkpointer.is_none();

        handles.push(thread::spawn(move || {
            let conn = Connection::open(&db_path).unwrap();
            conn.busy_timeout(timeout).unwrap();
            conn.execute_batch("PRAGMA synchronous = FULL").unwrap();
            if !own_checkpoints {
                // The checkpointer connection does it instead.
                conn.execute_batch("PRAGMA wal_autocheckpoint = 0").unwrap();
            }
            let mut stmt = conn
                .prepare("INSERT INTO test_table (id, data) VALUES (?, ?)")
                .unwrap();
            // Prepared once, so parsing the SQL is not charged to the
            // transaction. A deferred BEGIN takes the write lock at the first
            // INSERT instead, which buries the wait in the work phase.
            // Concurrent SQLite writers use BEGIN IMMEDIATE anyway.
            let mut begin_stmt = conn.prepare("BEGIN IMMEDIATE").unwrap();
            let mut commit_stmt = conn.prepare("COMMIT").unwrap();

            let mut samples = Vec::new();

            ready.wait();

            while let Some(arrival) = pacer.next() {
                wait_until(arrival.scheduled);
                let t0 = Instant::now();

                begin_stmt.execute([]).unwrap();
                let t1 = Instant::now();

                for _ in 0..batch_size {
                    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
                    stmt.execute((id, format!("data_{id}"))).unwrap();
                }
                let t2 = Instant::now();

                commit_stmt.execute([]).unwrap();
                let t3 = Instant::now();

                samples.push(Sample {
                    scheduled_ns: arrival.offset.as_nanos() as u64,
                    warmup: arrival.warmup,
                    restarts: 0,
                    queue_ns: t0.saturating_duration_since(arrival.scheduled).as_nanos() as u64,
                    begin_ns: (t1 - t0).as_nanos() as u64,
                    work_ns: (t2 - t1).as_nanos() as u64,
                    commit_ns: (t3 - t2).as_nanos() as u64,
                    total_ns: t3.saturating_duration_since(arrival.scheduled).as_nanos() as u64,
                });
            }

            samples
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

    Run {
        per_thread,
        overran: pacer.overran(),
        elapsed,
        checkpoints,
    }
}

/// Runs a passive checkpoint every `interval` until told to stop, and returns
/// when each one started and how long it took. PASSIVE copies whatever WAL
/// frames it can without waiting for the writer, which is how a production
/// SQLite server checkpoints.
fn checkpointer(
    db_path: &str,
    timeout: Duration,
    interval: Duration,
    stop: &AtomicBool,
) -> Vec<(Instant, Duration)> {
    let conn = Connection::open(db_path).unwrap();
    conn.busy_timeout(timeout).unwrap();
    let mut checkpoints = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        thread::sleep(interval);
        let started = Instant::now();
        // The pragma returns one row (busy, log, checkpointed). Busy just means
        // it could not finish this time; the next round picks it up.
        let _ = conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()));
        checkpoints.push((started, started.elapsed()));
    }
    checkpoints
}

fn setup(config: &Config) {
    let conn = Connection::open(&config.db_path).unwrap();
    conn.busy_timeout(config.timeout).unwrap();
    conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;")
        .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
}
