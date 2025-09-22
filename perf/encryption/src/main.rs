use clap::Parser;
use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use turso::{Builder, Database, Result};

#[derive(Debug, Clone)]
struct EncryptionOpts {
    cipher: String,
    hexkey: String,
}

#[derive(Parser)]
#[command(name = "encryption-throughput")]
#[command(about = "Encryption throughput benchmark on Turso DB")]
struct Args {
    /// More than one thread does not work yet
    #[arg(short = 't', long = "threads", default_value = "1")]
    threads: usize,

    /// the number operations per transaction
    #[arg(short = 'b', long = "batch-size", default_value = "100")]
    batch_size: usize,

    /// number of transactions per thread
    #[arg(short = 'i', long = "iterations", default_value = "10")]
    iterations: usize,

    #[arg(
        short = 'r',
        long = "read-ratio",
        help = "Percentage of operations that should be reads (0-100)"
    )]
    read_ratio: Option<u8>,

    #[arg(
        short = 'w',
        long = "write-ratio",
        help = "Percentage of operations that should be writes (0-100)"
    )]
    write_ratio: Option<u8>,

    #[arg(
        long = "encryption",
        action = clap::ArgAction::SetTrue,
        help = "Enable database encryption"
    )]
    encryption: bool,

    #[arg(
        long = "cipher",
        default_value = "aegis-256",
        help = "Encryption cipher to use (only relevant if --encryption is set)"
    )]
    cipher: String,

    #[arg(
        long = "think",
        default_value = "0",
        help = "Per transaction think time (ms)"
    )]
    think: u64,

    #[arg(
        long = "timeout",
        default_value = "30000",
        help = "Busy timeout in milliseconds"
    )]
    timeout: u64,

    #[arg(
        long = "seed",
        default_value = "2167532792061351037",
        help = "Random seed for reproducible workloads"
    )]
    seed: u64,
}

#[derive(Debug)]
struct WorkerStats {
    transactions_completed: u64,
    reads_completed: u64,
    writes_completed: u64,
    reads_found: u64,
    reads_not_found: u64,
    total_transaction_time: Duration,
}

#[derive(Debug, Clone)]
struct SharedState {
    max_inserted_id: Arc<AtomicU64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let args = Args::parse();

    let read_ratio = match (args.read_ratio, args.write_ratio) {
        (Some(_), Some(_)) => {
            eprintln!("Error: Cannot specify both --read-ratio and --write-ratio");
            std::process::exit(1);
        }
        (Some(r), None) => {
            if r > 100 {
                eprintln!("Error: read-ratio must be between 0 and 100");
                std::process::exit(1);
            }
            r
        }
        (None, Some(w)) => {
            if w > 100 {
                eprintln!("Error: write-ratio must be between 0 and 100");
                std::process::exit(1);
            }
            100 - w
        }
        // lets default to 0% reads (100% writes)
        (None, None) => 0,
    };

    println!(
        "Running encryption throughput benchmark with {} threads, {} batch size, {} iterations",
        args.threads, args.batch_size, args.iterations
    );
    println!(
        "Read/Write ratio: {}% reads, {}% writes",
        read_ratio,
        100 - read_ratio
    );
    println!("Encryption enabled: {}", args.encryption);
    println!("Random seed: {}", args.seed);

    let encryption_opts = if args.encryption {
        let mut key_rng = SmallRng::seed_from_u64(args.seed);
        let key_size = get_key_size_for_cipher(&args.cipher);
        let mut key = vec![0u8; key_size];
        key_rng.fill_bytes(&mut key);

        let config = EncryptionOpts {
            cipher: args.cipher.clone(),
            hexkey: hex::encode(&key),
        };

        println!("Cipher: {}", config.cipher);
        println!("Hexkey: {}", config.hexkey);
        Some(config)
    } else {
        None
    };

    let db_path = "encryption_throughput_test.db";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_file(db_path).expect("Failed to remove existing database");
    }
    let wal_path = "encryption_throughput_test.db-wal";
    if std::path::Path::new(wal_path).exists() {
        std::fs::remove_file(wal_path).expect("Failed to remove existing WAL file");
    }

    let db = setup_database(db_path, &encryption_opts).await?;

    // for create a var which is shared between all the threads, this we use to track the
    // max inserted id so that we only read these
    let shared_state = SharedState {
        max_inserted_id: Arc::new(AtomicU64::new(0)),
    };

    let start_barrier = Arc::new(Barrier::new(args.threads));
    let mut handles = Vec::new();

    let timeout = Duration::from_millis(args.timeout);
    let overall_start = Instant::now();

    for thread_id in 0..args.threads {
        let db_clone = db.clone();
        let barrier = Arc::clone(&start_barrier);
        let encryption_opts_clone = encryption_opts.clone();
        let shared_state_clone = shared_state.clone();

        let handle = tokio::task::spawn(worker_thread(
            thread_id,
            db_clone,
            args.batch_size,
            args.iterations,
            barrier,
            read_ratio,
            encryption_opts_clone,
            args.think,
            timeout,
            shared_state_clone,
            args.seed,
        ));

        handles.push(handle);
    }

    let mut total_transactions = 0;
    let mut total_reads = 0;
    let mut total_writes = 0;
    let mut total_reads_found = 0;
    let mut total_reads_not_found = 0;

    for (idx, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(stats)) => {
                total_transactions += stats.transactions_completed;
                total_reads += stats.reads_completed;
                total_writes += stats.writes_completed;
                total_reads_found += stats.reads_found;
                total_reads_not_found += stats.reads_not_found;
            }
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
    let total_operations = total_reads + total_writes;

    let transaction_throughput = (total_transactions as f64) / overall_elapsed.as_secs_f64();
    let operation_throughput = (total_operations as f64) / overall_elapsed.as_secs_f64();
    let read_throughput = if total_reads > 0 {
        (total_reads as f64) / overall_elapsed.as_secs_f64()
    } else {
        0.0
    };
    let write_throughput = if total_writes > 0 {
        (total_writes as f64) / overall_elapsed.as_secs_f64()
    } else {
        0.0
    };
    let avg_ops_per_txn = (total_operations as f64) / (total_transactions as f64);

    println!("\n=== BENCHMARK RESULTS ===");
    println!("Total transactions: {total_transactions}");
    println!("Total operations: {total_operations}");
    println!("Operations per transaction: {avg_ops_per_txn:.1}");
    println!("Total time: {:.2}s", overall_elapsed.as_secs_f64());
    println!();
    println!("Transaction throughput: {transaction_throughput:.2} txns/sec");
    println!("Operation throughput: {operation_throughput:.2} ops/sec");

    // not found should be zero since track the max inserted id
    // todo(v): probably handle the not found error and remove max id
    if total_reads > 0 {
        println!(
            "  - Read operations: {total_reads} ({total_reads_found} found, {total_reads_not_found} not found)"
        );
        println!("  - Read throughput: {read_throughput:.2} reads/sec");
    }
    if total_writes > 0 {
        println!("  - Write operations: {total_writes}");
        println!("  - Write throughput: {write_throughput:.2} writes/sec");
    }

    println!("\nConfiguration:");
    println!("Threads: {}", args.threads);
    println!("Batch size: {}", args.batch_size);
    println!("Iterations per thread: {}", args.iterations);
    println!("Encryption: {}", args.encryption);
    println!("Seed: {}", args.seed);

    if let Ok(metadata) = std::fs::metadata(db_path) {
        println!("Database file size: {} bytes", metadata.len());
    }

    Ok(())
}

fn get_key_size_for_cipher(cipher: &str) -> usize {
    match cipher.to_lowercase().as_str() {
        "aes-128-gcm" | "aegis-128l" | "aegis-128x2" | "aegis-128x4" => 16,
        "aes-256-gcm" | "aegis-256" | "aegis-256x2" | "aegis-256x4" => 32,
        _ => 32, // default to 256-bit key
    }
}

async fn setup_database(
    db_path: &str,
    encryption_opts: &Option<EncryptionOpts>,
) -> Result<Database> {
    let builder = Builder::new_local(db_path);
    let db = builder.build().await?;
    let conn = db.connect()?;

    if let Some(config) = encryption_opts {
        conn.execute(&format!("PRAGMA cipher='{}'", config.cipher), ())
            .await?;
        conn.execute(&format!("PRAGMA hexkey='{}'", config.hexkey), ())
            .await?;
    }

    // todo(v): probably store blobs and then have option of randomblob size
    conn.execute(
        "CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        )",
        (),
    )
    .await?;

    println!("Database created at: {db_path}");
    Ok(db)
}

#[allow(clippy::too_many_arguments)]
async fn worker_thread(
    thread_id: usize,
    db: Database,
    batch_size: usize,
    iterations: usize,
    start_barrier: Arc<Barrier>,
    read_ratio: u8,
    encryption_opts: Option<EncryptionOpts>,
    think_ms: u64,
    timeout: Duration,
    shared_state: SharedState,
    base_seed: u64,
) -> Result<WorkerStats> {
    start_barrier.wait();

    let start_time = Instant::now();
    let mut stats = WorkerStats {
        transactions_completed: 0,
        reads_completed: 0,
        writes_completed: 0,
        reads_found: 0,
        reads_not_found: 0,
        total_transaction_time: Duration::ZERO,
    };

    let thread_seed = base_seed.wrapping_add(thread_id as u64);
    let mut rng = SmallRng::seed_from_u64(thread_seed);

    for iteration in 0..iterations {
        let conn = db.connect()?;

        if let Some(config) = &encryption_opts {
            conn.execute(&format!("PRAGMA cipher='{}'", config.cipher), ())
                .await?;
            conn.execute(&format!("PRAGMA hexkey='{}'", config.hexkey), ())
                .await?;
        }

        conn.busy_timeout(timeout)?;

        let mut insert_stmt = conn
            .prepare("INSERT INTO test_table (id, data) VALUES (?, ?)")
            .await?;

        let transaction_start = Instant::now();
        conn.execute("BEGIN", ()).await?;

        for i in 0..batch_size {
            let should_read = rng.random_range(0..100) < read_ratio;

            if should_read {
                // only attempt reads if we have inserted some data
                let max_id = shared_state.max_inserted_id.load(Ordering::Relaxed);
                if max_id > 0 {
                    let read_id = rng.random_range(1..=max_id);
                    let row = conn
                        .query(
                            "SELECT data FROM test_table WHERE id = ?",
                            turso::params::Params::Positional(vec![turso::Value::Integer(
                                read_id as i64,
                            )]),
                        )
                        .await;

                    match row {
                        Ok(_) => stats.reads_found += 1,
                        Err(turso::Error::QueryReturnedNoRows) => stats.reads_not_found += 1,
                        Err(e) => return Err(e),
                    };
                    stats.reads_completed += 1;
                } else {
                    // if no data inserted yet, convert to a write
                    let id = thread_id * iterations * batch_size + iteration * batch_size + i + 1;
                    insert_stmt
                        .execute(turso::params::Params::Positional(vec![
                            turso::Value::Integer(id as i64),
                            turso::Value::Text(format!("data_{id}")),
                        ]))
                        .await?;

                    shared_state
                        .max_inserted_id
                        .fetch_max(id as u64, Ordering::Relaxed);
                    stats.writes_completed += 1;
                }
            } else {
                let id = thread_id * iterations * batch_size + iteration * batch_size + i + 1;
                insert_stmt
                    .execute(turso::params::Params::Positional(vec![
                        turso::Value::Integer(id as i64),
                        turso::Value::Text(format!("data_{id}")),
                    ]))
                    .await?;

                shared_state
                    .max_inserted_id
                    .fetch_max(id as u64, Ordering::Relaxed);
                stats.writes_completed += 1;
            }
        }

        if think_ms > 0 {
            tokio::time::sleep(Duration::from_millis(think_ms)).await;
        }

        conn.execute("COMMIT", ()).await?;

        let transaction_elapsed = transaction_start.elapsed();
        stats.transactions_completed += 1;
        stats.total_transaction_time += transaction_elapsed;
    }

    let elapsed = start_time.elapsed();
    let total_ops = stats.reads_completed + stats.writes_completed;
    let transaction_throughput = (stats.transactions_completed as f64) / elapsed.as_secs_f64();
    let operation_throughput = (total_ops as f64) / elapsed.as_secs_f64();
    let avg_txn_latency =
        stats.total_transaction_time.as_secs_f64() * 1000.0 / stats.transactions_completed as f64;

    println!(
        "Thread {}: {} txns ({} ops: {} reads, {} writes) in {:.2}s ({:.2} txns/sec, {:.2} ops/sec, {:.2}ms avg latency)",
        thread_id,
        stats.transactions_completed,
        total_ops,
        stats.reads_completed,
        stats.writes_completed,
        elapsed.as_secs_f64(),
        transaction_throughput,
        operation_throughput,
        avg_txn_latency
    );

    if stats.reads_completed > 0 {
        println!(
            "  Thread {} reads: {} found, {} not found",
            thread_id, stats.reads_found, stats.reads_not_found
        );
    }

    Ok(stats)
}
