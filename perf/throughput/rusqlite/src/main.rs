use clap::Parser;
use rusqlite::{Connection, Result};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "write-throughput")]
#[command(about = "Write throughput benchmark using rusqlite")]
struct Args {
    #[arg(short = 't', long = "threads", default_value = "1")]
    threads: usize,

    #[arg(short = 'b', long = "batch-size", default_value = "100")]
    batch_size: usize,

    #[arg(short = 'i', long = "iterations", default_value = "10")]
    iterations: usize,

    #[arg(
        long = "compute",
        default_value = "0",
        help = "Per transaction compute time (us)"
    )]
    compute: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let db_path = "write_throughput_test.db";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_file(db_path).expect("Failed to remove existing database");
    }

    let wal_path = "write_throughput_test.db-wal";
    if std::path::Path::new(wal_path).exists() {
        std::fs::remove_file(wal_path).expect("Failed to remove existing database");
    }

    let _conn = setup_database(db_path)?;

    let start_barrier = Arc::new(Barrier::new(args.threads));
    let mut handles = Vec::new();

    let overall_start = Instant::now();

    for thread_id in 0..args.threads {
        let db_path = db_path.to_string();
        let barrier = Arc::clone(&start_barrier);

        let handle = thread::spawn(move || {
            worker_thread(
                thread_id,
                db_path,
                args.batch_size,
                args.iterations,
                barrier,
                args.compute,
            )
        });

        handles.push(handle);
    }

    let mut total_inserts = 0;
    for handle in handles {
        match handle.join() {
            Ok(Ok(inserts)) => total_inserts += inserts,
            Ok(Err(e)) => {
                eprintln!("Thread error: {e}");
                return Err(e);
            }
            Err(_) => {
                eprintln!("Thread panicked");
                std::process::exit(1);
            }
        }
    }

    let overall_elapsed = overall_start.elapsed();
    let overall_throughput = (total_inserts as f64) / overall_elapsed.as_secs_f64();

    println!(
        "SQLite,{},{},{},{:.2}",
        args.threads, args.batch_size, args.compute, overall_throughput
    );

    Ok(())
}

fn setup_database(db_path: &str) -> Result<Connection> {
    let conn = Connection::open(db_path)?;

    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "FULL")?;

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
    db_path: String,
    batch_size: usize,
    iterations: usize,
    start_barrier: Arc<Barrier>,
    compute_usec: u64,
) -> Result<u64> {
    let conn = Connection::open(&db_path)?;

    conn.busy_timeout(std::time::Duration::from_secs(30))?;

    start_barrier.wait();

    let mut total_inserts = 0;

    for iteration in 0..iterations {
        let mut stmt = conn.prepare("INSERT INTO test_table (id, data) VALUES (?, ?)")?;

        conn.execute("BEGIN", [])?;

        let result = perform_compute(thread_id, compute_usec);

        std::hint::black_box(result);

        for i in 0..batch_size {
            let id = thread_id * iterations * batch_size + iteration * batch_size + i;
            stmt.execute([&id.to_string(), &format!("data_{id}")])?;
            total_inserts += 1;
        }

        conn.execute("COMMIT", [])?;
    }

    Ok(total_inserts)
}

// Busy loop to simulate CPU or GPU bound computation (for example, parsing,
// data aggregation or ML inference).
fn perform_compute(thread_id: usize, usec: u64) -> u64 {
    if usec == 0 {
        return 0;
    }
    let start = Instant::now();
    let mut sum: u64 = 0;
    while start.elapsed().as_micros() < usec as u128 {
        sum = sum.wrapping_add(thread_id as u64);
    }
    sum
}
