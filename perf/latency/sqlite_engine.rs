use crate::{wait_until, Config, Pacer, Run, Sample};
use rusqlite::Connection;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

static NEXT_ID: AtomicI64 = AtomicI64::new(1);

pub fn run(config: &Config) -> Run {
    setup(config);

    let ready = Arc::new(Barrier::new(config.connections + 1));
    let pacer = Arc::new(Pacer::new(config));
    let mut handles = Vec::new();

    for _ in 0..config.connections {
        let ready = Arc::clone(&ready);
        let pacer = Arc::clone(&pacer);
        let db_path = config.db_path.clone();
        let batch_size = config.batch_size;
        let timeout = config.timeout;

        handles.push(thread::spawn(move || {
            let conn = Connection::open(&db_path).unwrap();
            conn.busy_timeout(timeout).unwrap();
            conn.execute_batch("PRAGMA synchronous = FULL").unwrap();
            let mut stmt = conn
                .prepare("INSERT INTO test_table (id, data) VALUES (?, ?)")
                .unwrap();

            let mut samples = Vec::new();

            ready.wait();

            while let Some(arrival) = pacer.next() {
                wait_until(arrival.scheduled);
                let t0 = Instant::now();

                // A deferred BEGIN takes the write lock at the first INSERT
                // instead, which buries the wait in the work phase. Concurrent
                // SQLite writers use BEGIN IMMEDIATE anyway.
                conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                let t1 = Instant::now();

                for _ in 0..batch_size {
                    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
                    stmt.execute((id, format!("data_{id}"))).unwrap();
                }
                let t2 = Instant::now();

                conn.execute_batch("COMMIT").unwrap();
                let t3 = Instant::now();

                if arrival.record {
                    samples.push(Sample {
                        queue_ns: t0.saturating_duration_since(arrival.scheduled).as_nanos() as u64,
                        begin_ns: (t1 - t0).as_nanos() as u64,
                        work_ns: (t2 - t1).as_nanos() as u64,
                        commit_ns: (t3 - t2).as_nanos() as u64,
                        total_ns: t3.saturating_duration_since(arrival.scheduled).as_nanos() as u64,
                    });
                }
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

    Run {
        per_thread,
        overran: pacer.overran(),
        elapsed: pacer.elapsed(),
    }
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
