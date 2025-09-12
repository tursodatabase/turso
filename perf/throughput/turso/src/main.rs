use clap::{Parser, ValueEnum};
use std::sync::{Arc, Barrier};
use std::time::Instant;
use tokio::runtime::Runtime;
use turso::{Builder, Database, Result};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TransactionMode {
    Legacy,
    Mvcc,
    Concurrent,
}

#[derive(Parser)]
#[command(name = "write-throughput")]
#[command(about = "Write throughput benchmark using turso")]
struct Args {
    #[arg(short = 't', long = "threads", default_value = "1")]
    threads: usize,

    #[arg(short = 'b', long = "batch-size", default_value = "100")]
    batch_size: usize,

    #[arg(short = 'i', long = "iterations", default_value = "10")]
    iterations: usize,

    #[arg(short = 'm', long = "mode", default_value = "legacy")]
    mode: TransactionMode,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!(
        "Running write throughput benchmark with {} threads, {} batch size, {} iterations, mode: {:?}",
        args.threads, args.batch_size, args.iterations, args.mode
    );

    let db_path = "write_throughput_test.db";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_file(db_path).expect("Failed to remove existing database");
    }
    let wal_path = "write_throughput_test.db-wal";
    if std::path::Path::new(wal_path).exists() {
        std::fs::remove_file(wal_path).expect("Failed to remove existing database");
    }

    let db = setup_database(db_path, args.mode).await?;

    let start_barrier = Arc::new(Barrier::new(args.threads));
    let mut handles = Vec::new();

    let overall_start = Instant::now();

    for thread_id in 0..args.threads {
        let db_clone = db.clone();
        let barrier = Arc::clone(&start_barrier);

        let handle = tokio::task::spawn_blocking(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(worker_thread(
                thread_id,
                db_clone,
                args.batch_size,
                args.iterations,
                barrier,
                args.mode,
            ))
        });

        handles.push(handle);
    }

    let mut total_inserts = 0;
    for handle in handles {
        match handle.await {
            Ok(Ok(inserts)) => total_inserts += inserts,
            Ok(Err(e)) => {
                eprintln!("Thread error: {}", e);
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
    println!("Total inserts: {}", total_inserts);
    println!("Total time: {:.2}s", overall_elapsed.as_secs_f64());
    println!("Overall throughput: {:.2} inserts/sec", overall_throughput);
    println!("Threads: {}", args.threads);
    println!("Batch size: {}", args.batch_size);
    println!("Iterations per thread: {}", args.iterations);

    println!(
        "Database file exists: {}",
        std::path::Path::new(db_path).exists()
    );
    if let Ok(metadata) = std::fs::metadata(db_path) {
        println!("Database file size: {} bytes", metadata.len());
    }

    Ok(())
}

async fn setup_database(db_path: &str, mode: TransactionMode) -> Result<Database> {
    let builder = Builder::new_local(db_path);
    let db = match mode {
        TransactionMode::Legacy => builder.build().await?,
        TransactionMode::Mvcc | TransactionMode::Concurrent => {
            builder.with_mvcc(true).build().await?
        }
    };
    let conn = db.connect()?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        )",
        (),
    )
    .await?;

    println!("Database created at: {}", db_path);
    Ok(db)
}

async fn worker_thread(
    thread_id: usize,
    db: Database,
    batch_size: usize,
    iterations: usize,
    start_barrier: Arc<Barrier>,
    mode: TransactionMode,
) -> Result<u64> {
    let conn = db.connect()?;

    let mut stmt = conn
        .prepare("INSERT INTO test_table (id, data) VALUES (?, ?)")
        .await?;

    start_barrier.wait();

    let start_time = Instant::now();
    let mut total_inserts = 0;

    for iteration in 0..iterations {
        let begin_stmt = match mode {
            TransactionMode::Legacy | TransactionMode::Mvcc => "BEGIN",
            TransactionMode::Concurrent => "BEGIN CONCURRENT",
        };
        conn.execute(begin_stmt, ()).await?;

        for i in 0..batch_size {
            let id = thread_id * iterations * batch_size + iteration * batch_size + i;
            stmt.execute(turso::params::Params::Positional(vec![
                turso::Value::Integer(id as i64),
                turso::Value::Text(format!("data_{}", id)),
            ]))
            .await?;
            total_inserts += 1;
        }

        conn.execute("COMMIT", ()).await?;
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
