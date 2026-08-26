use crate::{perform_compute, row_id, Args, IoOption, Measurement, TransactionMode, DB_PATH};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use turso::{Builder, Database, IoBackend, Result};

pub fn run(args: &Args) -> anyhow::Result<Measurement> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads)
        .build()?;

    rt.block_on(run_async(
        args.threads,
        args.batch_size,
        args.iterations,
        args.mode,
        args.compute,
        args.busy_timeout(),
        args.io,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_async(
    threads: usize,
    batch_size: usize,
    iterations: usize,
    mode: TransactionMode,
    compute: u64,
    timeout: Duration,
    io: Option<IoOption>,
) -> anyhow::Result<Measurement> {
    let db = setup_database(DB_PATH, mode, io).await?;

    let start_barrier = Arc::new(Barrier::new(threads));
    let mut handles = Vec::new();

    let overall_start = Instant::now();

    for thread_id in 0..threads {
        handles.push(tokio::task::spawn(worker_thread(
            thread_id,
            db.clone(),
            batch_size,
            iterations,
            Arc::clone(&start_barrier),
            mode,
            compute,
            timeout,
        )));
    }

    let mut rows = 0;
    for (idx, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(inserts)) => rows += inserts,
            Ok(Err(e)) => return Err(anyhow::anyhow!("thread error {idx}: {e}")),
            Err(_) => anyhow::bail!("thread panicked"),
        }
    }

    Ok(Measurement {
        rows,
        elapsed: overall_start.elapsed(),
    })
}

async fn setup_database(
    db_path: &str,
    mode: TransactionMode,
    io: Option<IoOption>,
) -> Result<Database> {
    let builder = Builder::new_local(db_path);

    let builder = if let Some(io) = io {
        match io {
            IoOption::IoUring => builder.with_io(IoBackend::IoUring),
        }
    } else {
        builder
    };
    let builder = match mode {
        TransactionMode::MvccPassive => builder.experimental_mvcc_passive_checkpoint(true),
        TransactionMode::MvccTruncate => builder.experimental_mvcc_passive_checkpoint(false),
        _ => builder,
    };
    let db = builder.build().await?;
    let conn = db.connect()?;

    if mode.needs_mvcc() {
        conn.pragma_update("journal_mode", "mvcc").await?;
    }

    // Match the durability the SQLite engine asks for, so a commit costs the
    // same on both sides.
    conn.pragma_update("synchronous", "full").await?;
    #[cfg(target_vendor = "apple")]
    conn.pragma_update("fullfsync", "true").await?;

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

            // Retry loop for BusySnapshot errors (stale snapshot requires full tx restart)
            'tx: loop {
                conn.execute(mode.begin_statement(), ()).await?;

                let result = perform_compute(thread_id, compute_usec);
                std::hint::black_box(result);

                let mut insert_count = 0u64;
                for i in 0..batch_size {
                    let id = row_id(thread_id, iterations, batch_size, iteration, i);
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
        if mode.overlaps_transactions() {
            tx_futs.push(tx_fut);
        } else {
            tx_fut.await?;
        }
    }

    let results = futures::future::join_all(tx_futs).await;
    for result in results {
        result?;
    }

    let final_inserts = total_inserts.load(Ordering::Relaxed);

    eprintln!("[Thread {thread_id}] Final inserts: {final_inserts}");

    Ok(final_inserts)
}
