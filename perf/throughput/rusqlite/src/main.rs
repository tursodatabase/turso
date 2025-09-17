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
        long = "think",
        default_value = "0",
        help = "Per transaction think time (ms)"
    )]
    think: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!(
        "Running write throughput benchmark with {} threads, {} batch size, {} iterations",
        args.threads, args.batch_size, args.iterations
    );

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
                args.think,
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

    println!("\n=== BENCHMARK RESULTS ===");
    println!("Total inserts: {total_inserts}",);
    println!("Total time: {:.2}s", overall_elapsed.as_secs_f64());
    println!("Overall throughput: {overall_throughput:.2} inserts/sec");
    println!("Threads: {}", args.threads);
    println!("Batch size: {}", args.batch_size);
    println!("Iterations per thread: {}", args.iterations);

    println!(
        "Database file exists: {}",
        std::path::Path::new(db_path).exists()
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

    println!("Database created at: {db_path}");
    Ok(conn)
}

fn worker_thread(
    thread_id: usize,
    db_path: String,
    batch_size: usize,
    iterations: usize,
    start_barrier: Arc<Barrier>,
    think_ms: u64,
) -> Result<u64> {
    let conn = Connection::open(&db_path)?;

    conn.busy_timeout(std::time::Duration::from_secs(30))?;

    start_barrier.wait();

    let start_time = Instant::now();
    let mut total_inserts = 0;

    for iteration in 0..iterations {
        let mut stmt = conn.prepare("INSERT INTO test_table (id, data) VALUES (?, ?)")?;

        conn.execute("BEGIN", [])?;

        for i in 0..batch_size {
            let id = thread_id * iterations * batch_size + iteration * batch_size + i;
            stmt.execute([&id.to_string(), &format!("data_{id}")])?;
            total_inserts += 1;
        }
        if think_ms > 0 {
            thread::sleep(std::time::Duration::from_millis(think_ms));
        }
        conn.execute("COMMIT", [])?;
    }

    let elapsed = start_time.elapsed();
    let throughput = (total_inserts as f64) / elapsed.as_secs_f64();

    println!(
        "Thread {}: {} inserts in {:.2}s ({:.2} inserts/sec)",
        thread_id,
        total_inserts,
        elapsed.as_secs_f64(),
        throughput
    );

    Ok(total_inserts)
}
