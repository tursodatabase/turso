use crate::{spin_until, Config, Pacer, Run, Sample, TxnMode};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;
use turso::{Builder, Database, Value};

static NEXT_ID: AtomicI64 = AtomicI64::new(1);

pub fn run(config: &Config) -> Run {
    // Two spare workers on top of the connections, so the pacing timer and the
    // background IO work never queue behind a transaction.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.connections + 2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run_async(config))
}

async fn run_async(config: &Config) -> Run {
    let db = setup(config).await;

    let ready = Arc::new(Barrier::new(config.connections + 1));
    let pacer = Arc::new(Pacer::new(config));
    let restarts = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    for _ in 0..config.connections {
        let db = db.clone();
        let ready = Arc::clone(&ready);
        let pacer = Arc::clone(&pacer);
        let restarts = Arc::clone(&restarts);
        let batch_size = config.batch_size;
        let timeout = config.timeout;
        let mode = config.mode;

        handles.push(tokio::spawn(async move {
            let conn = db.connect().unwrap();
            conn.busy_timeout(timeout).unwrap();
            conn.execute("PRAGMA synchronous = FULL", ()).await.unwrap();
            let mut stmt = conn
                .prepare("INSERT INTO test_table (id, data) VALUES (?, ?)")
                .await
                .unwrap();

            let begin_stmt = match mode {
                TxnMode::Immediate => "BEGIN IMMEDIATE",
                TxnMode::Concurrent => "BEGIN CONCURRENT",
            };

            let mut samples = Vec::new();

            ready.wait().await;

            while let Some(arrival) = pacer.next() {
                wait_until(arrival.scheduled).await;
                let t0 = Instant::now();

                // A concurrent transaction can find its snapshot stale, and the
                // only cure is to start over. The restarts stay inside the
                // sample, because the caller is still waiting through them.
                let (t1, t2, t3) = 'txn: loop {
                    conn.execute(begin_stmt, ()).await.unwrap();
                    let t1 = Instant::now();

                    for _ in 0..batch_size {
                        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
                        let params = turso::params::Params::Positional(vec![
                            Value::Integer(id),
                            Value::Text(format!("data_{id}")),
                        ]);
                        match stmt.execute(params).await {
                            Ok(_) => {}
                            Err(turso::Error::BusySnapshot(_)) => {
                                restarts.fetch_add(1, Ordering::Relaxed);
                                conn.execute("ROLLBACK", ()).await.unwrap();
                                continue 'txn;
                            }
                            Err(e) => panic!("INSERT failed: {e}"),
                        }
                    }
                    let t2 = Instant::now();

                    match conn.execute("COMMIT", ()).await {
                        Ok(_) => {}
                        Err(turso::Error::BusySnapshot(_)) => {
                            restarts.fetch_add(1, Ordering::Relaxed);
                            conn.execute("ROLLBACK", ()).await.unwrap();
                            continue 'txn;
                        }
                        Err(e) => panic!("COMMIT failed: {e}"),
                    }
                    break 'txn (t1, t2, Instant::now());
                };

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
    ready.wait().await;

    let mut per_thread = Vec::new();
    for handle in handles {
        per_thread.push(handle.await.expect("writer task panicked"));
    }
    let elapsed = pacer.elapsed();

    eprintln!(
        "[turso] {} transaction restarts",
        restarts.load(Ordering::Relaxed)
    );

    Run {
        per_thread,
        overran: pacer.overran(),
        elapsed,
    }
}

/// Same idea as `crate::wait_until`, but it parks the task instead of the
/// runtime thread for the long part of the wait.
async fn wait_until(deadline: Instant) {
    const SPIN: Duration = Duration::from_micros(300);
    let now = Instant::now();
    if deadline <= now {
        return;
    }
    if deadline - now > SPIN {
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline - SPIN)).await;
    }
    spin_until(deadline);
}

async fn setup(config: &Config) -> Database {
    let builder = Builder::new_local(&config.db_path);
    let builder = match config.mode {
        TxnMode::Concurrent => builder.experimental_mvcc_passive_checkpoint(true),
        TxnMode::Immediate => builder,
    };
    let db = builder.build().await.unwrap();

    let conn = db.connect().unwrap();
    if config.mode == TxnMode::Concurrent {
        conn.pragma_update("journal_mode", "mvcc").await.unwrap();
    }
    conn.execute("PRAGMA synchronous = FULL", ()).await.unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
        )",
        (),
    )
    .await
    .unwrap();

    db
}
