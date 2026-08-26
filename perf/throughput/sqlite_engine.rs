use crate::{perform_compute, row_id, Args, Measurement, DB_PATH};
use rusqlite::{Connection, Result};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

pub fn run(args: &Args) -> anyhow::Result<Measurement> {
    let _conn = setup_database(DB_PATH)?;

    let start_barrier = Arc::new(Barrier::new(args.threads));
    let mut handles = Vec::new();

    let overall_start = Instant::now();

    for thread_id in 0..args.threads {
        let barrier = Arc::clone(&start_barrier);
        let batch_size = args.batch_size;
        let iterations = args.iterations;
        let compute = args.compute;
        let timeout = args.busy_timeout();

        handles.push(thread::spawn(move || {
            worker_thread(
                thread_id, batch_size, iterations, barrier, compute, timeout,
            )
        }));
    }

    let mut rows = 0;
    for handle in handles {
        match handle.join() {
            Ok(Ok(inserts)) => rows += inserts,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => anyhow::bail!("thread panicked"),
        }
    }

    Ok(Measurement {
        rows,
        elapsed: overall_start.elapsed(),
    })
}

fn setup_database(db_path: &str) -> Result<Connection> {
    let conn = Connection::open(db_path)?;

    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.pragma_update(None, "fullfsync", "true")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        )",
        [],
    )?;

    Ok(conn)
}

fn worker_thread(
    thread_id: usize,
    batch_size: usize,
    iterations: usize,
    start_barrier: Arc<Barrier>,
    compute_usec: u64,
    timeout: std::time::Duration,
) -> Result<u64> {
    start_barrier.wait();

    let mut total_inserts = 0;

    for iteration in 0..iterations {
        let conn = Connection::open(DB_PATH)?;

        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "fullfsync", "true")?;

        conn.busy_timeout(timeout)?;

        let mut stmt = conn.prepare("INSERT INTO test_table (id, data) VALUES (?, ?)")?;

        conn.execute("BEGIN", [])?;

        let result = perform_compute(thread_id, compute_usec);
        std::hint::black_box(result);

        for i in 0..batch_size {
            let id = row_id(thread_id, iterations, batch_size, iteration, i);
            stmt.execute([&id.to_string(), &format!("data_{id}")])?;
            total_inserts += 1;
        }

        conn.execute("COMMIT", [])?;
    }

    Ok(total_inserts)
}
