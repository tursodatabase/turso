use crate::{Checkpoint, Clock, Config, Run, Sample, COMMITTED, NEXT_ID};
use rusqlite::Connection;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier,
    },
    thread,
    time::{Duration, Instant},
};

pub fn run(config: &Config) -> Run {
    setup(config);

    let ready = Arc::new(Barrier::new(config.connections + 1));
    let clock = Arc::new(Clock::new(config));
    let stop = Arc::new(AtomicBool::new(false));

    let checkpointer = config.checkpointer.map(|interval| {
        let stop = Arc::clone(&stop);
        let db_path = config.db_path.clone();
        let timeout = config.timeout;
        thread::spawn(move || checkpointer(&db_path, timeout, interval, &stop))
    });

    let mut handles = Vec::new();
    for _ in 0..config.connections {
        let ready = Arc::clone(&ready);
        let clock = Arc::clone(&clock);
        let db_path = config.db_path.clone();
        let batch_size = config.batch_size;
        let timeout = config.timeout;
        let run = config.run;
        let own_checkpoints = config.checkpointer.is_none();

        handles.push(thread::spawn(move || {
            let conn = Connection::open(&db_path).unwrap();
            conn.busy_timeout(timeout).unwrap();
            conn.execute_batch("PRAGMA synchronous = FULL").unwrap();
            if !own_checkpoints {
                conn.execute_batch("PRAGMA wal_autocheckpoint = 0").unwrap();
            }
            let mut stmt = conn
                .prepare("INSERT INTO test_table (id, data) VALUES (?, ?)")
                .unwrap();
            // Prepared once, so parsing is not charged to the transaction.
            // BEGIN IMMEDIATE takes the write lock up front. A deferred
            // BEGIN would take it at the first INSERT and, when another
            // writer committed in the meantime, fail with
            // SQLITE_BUSY_SNAPSHOT instead of waiting.
            let mut begin_stmt = conn.prepare("BEGIN IMMEDIATE").unwrap();
            let mut commit_stmt = conn.prepare("COMMIT").unwrap();

            let mut samples = Vec::new();

            ready.wait();

            while let Some(start) = clock.next() {
                begin_stmt.execute([]).unwrap();
                let t1 = Instant::now();

                for _ in 0..batch_size {
                    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
                    stmt.execute((id, format!("run{run}_{id}"))).unwrap();
                }
                let t2 = Instant::now();

                commit_stmt.execute([]).unwrap();
                let t3 = Instant::now();
                COMMITTED.fetch_add(1, Ordering::Relaxed);

                samples.push(Sample {
                    started_ns: start.offset.as_nanos() as u64,
                    warmup: start.warmup,
                    restarts: 0,
                    begin_ns: (t1 - start.at).as_nanos() as u64,
                    work_ns: (t2 - t1).as_nanos() as u64,
                    commit_ns: (t3 - t2).as_nanos() as u64,
                    total_ns: (t3 - start.at).as_nanos() as u64,
                });
            }

            samples
        }));
    }

    // Release the connections. The clock starts when the first one asks for work.
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

    Run {
        per_connection,
        checkpoints,
        elapsed,
        rows_in_table: count_rows(config),
    }
}

/// Runs a passive checkpoint every `interval` until told to stop, and returns
/// when each one started and how long it took. PASSIVE copies whatever WAL
/// frames it can without waiting for the writers, which is how a production
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

fn count_rows(config: &Config) -> u64 {
    let conn = Connection::open(&config.db_path).unwrap();
    conn.busy_timeout(config.timeout).unwrap();
    conn.query_row("SELECT count(*) FROM test_table", [], |row| {
        row.get::<_, i64>(0)
    })
    .unwrap() as u64
}

fn setup(config: &Config) {
    let conn = Connection::open(&config.db_path).unwrap();
    conn.busy_timeout(config.timeout).unwrap();
    conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;")
        .unwrap();
    conn.execute(
        "CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
}
