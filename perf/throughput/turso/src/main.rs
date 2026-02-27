use clap::{Parser, ValueEnum};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};
use turso::{Builder, Database, Result};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TransactionMode {
    Legacy,
    Mvcc,
    Concurrent,
    LogicalLog,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum IoOption {
    IoUring,
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

    #[arg(
        long = "compute",
        default_value = "0",
        help = "Per transaction compute time (us)"
    )]
    compute: u64,

    #[arg(
        long = "timeout",
        default_value = "30000",
        help = "Busy timeout in milliseconds"
    )]
    timeout: u64,

    #[arg(long = "io", help = "IO backend")]
    io: Option<IoOption>,
}

fn main() -> Result<()> {
    #[cfg(feature = "console")]
    let console_layer = console_subscriber::spawn();
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_thread_ids(true)
        .with_filter(EnvFilter::from_default_env());
    let registry = tracing_subscriber::registry();
    #[cfg(feature = "console")]
    let registry = registry.with(console_layer);
    let registry = registry.with(fmt_layer);

    registry.init();
    let args = Args::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads)
        .build()
        .unwrap();

    rt.block_on(async_main(args))
}

async fn async_main(args: Args) -> Result<()> {
    let db_path = "write_throughput_test.db";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_file(db_path).expect("Failed to remove existing database");
    }
    let wal_path = "write_throughput_test.db-wal";
    if std::path::Path::new(wal_path).exists() {
        std::fs::remove_file(wal_path).expect("Failed to remove existing database");
    }

    let db = setup_database(db_path, args.mode, args.io).await?;

    let start_barrier = Arc::new(Barrier::new(args.threads));
    let mut handles = Vec::new();

    let timeout = Duration::from_millis(args.timeout);

    let overall_start = Instant::now();

    for thread_id in 0..args.threads {
        let db_clone = db.clone();
        let barrier = Arc::clone(&start_barrier);

        let handle = tokio::task::spawn(worker_thread(
            thread_id,
            db_clone,
            args.batch_size,
            args.iterations,
            barrier,
            args.mode,
            args.compute,
            timeout,
        ));

        handles.push(handle);
    }

    let mut total_inserts = 0;
    for (idx, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(inserts)) => total_inserts += inserts,
            Ok(Err(e)) => {
                eprintln!("Thread error {idx}: {e}");
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
        "Turso,{},{},{},{:.2}",
        args.threads, args.batch_size, args.compute, overall_throughput
    );

    Ok(())
}

async fn setup_database(
    db_path: &str,
    mode: TransactionMode,
    io: Option<IoOption>,
) -> Result<Database> {
    let builder = Builder::new_local(db_path);

    let builder = if let Some(io) = io {
        match io {
            IoOption::IoUring => builder.with_io("io_uring".to_string()),
        }
    } else {
        builder
    };
    let db = builder.build().await?;
    let conn = db.connect()?;

    // Enable MVCC for modes that require it
    if matches!(
        mode,
        TransactionMode::Mvcc | TransactionMode::Concurrent | TransactionMode::LogicalLog
    ) {
        conn.pragma_update("journal_mode", "'mvcc'").await?;
    }

    conn.execute(
        "CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        )",
        (),
    )
    .await?;

    Ok(db)
}

#[allow(clippy::too_many_arguments)]
async fn worker_thread(
    thread_id: usize,
    db: Database,
    batch_size: usize,
    iterations: usize,
    start_barrier: Arc<Barrier>,
    mode: TransactionMode,
    compute_usec: u64,
    timeout: Duration,
) -> Result<u64> {
    start_barrier.wait();

    let total_inserts = Arc::new(AtomicU64::new(0));

    let mut tx_futs = vec![];

    for iteration in 0..iterations {
        let conn = db.connect()?;
        conn.busy_timeout(timeout)?;
        let total_inserts = Arc::clone(&total_inserts);
        let tx_fut = async move {
            let mut stmt = conn
                .prepare("INSERT INTO test_table (id, data) VALUES (?, ?)")
                .await?;

            let begin_stmt = match mode {
                TransactionMode::Legacy | TransactionMode::Mvcc => "BEGIN",
                TransactionMode::Concurrent | TransactionMode::LogicalLog => "BEGIN CONCURRENT",
            };

            // Retry loop for BusySnapshot errors (stale snapshot requires full tx restart)
            'tx: loop {
                conn.execute(begin_stmt, ()).await?;

                let result = perform_compute(thread_id, compute_usec);
                std::hint::black_box(result);

                let mut insert_count = 0u64;
                for i in 0..batch_size {
                    let id = thread_id * iterations * batch_size + iteration * batch_size + i;
                    match stmt
                        .execute(turso::params::Params::Positional(vec![
                            turso::Value::Integer(id as i64),
                            turso::Value::Text(format!("data_{id}")),
                        ]))
                        .await
                    {
                        Ok(_) => insert_count += 1,
                        Err(turso::Error::BusySnapshot(_)) => {
                            eprintln!("[Thread {thread_id}] Snapshot is stale during INSERT, rolling back transaction");
                            conn.execute("ROLLBACK", ())
                                .await
                                .expect("Failed to rollback transaction");
                            continue 'tx;
                        }
                        Err(e) => return Err(e),
                    }
                }

                conn.execute("COMMIT", ()).await?;
                total_inserts.fetch_add(insert_count, Ordering::Relaxed);
                break 'tx;
            }

            Ok::<_, turso::Error>(())
        };
        match mode {
            TransactionMode::Concurrent => tx_futs.push(tx_fut),
            _ => tx_fut.await?,
        };
    }

    let results = futures::future::join_all(tx_futs).await;
    for result in results {
        result?;
    }

    let final_inserts = total_inserts.load(Ordering::Relaxed);

    eprintln!("[Thread {thread_id}] Final inserts: {final_inserts}");

    Ok(final_inserts)
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
