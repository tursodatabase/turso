//! Barrier-free MVCC checkpoint starvation probe.
//!
//! N tasks run continuous BEGIN CONCURRENT / INSERT 2KB blobs / COMMIT loops
//! against one database until a shared row budget is exhausted — no round
//! barriers, so with N > 1 some transaction is almost always active. A sampler
//! prints heap (dhat) and logical-log size over time. Auto-checkpoint failures
//! are visible on stderr via `tracing` at INFO ("MVCC auto-checkpoint failed").
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use turso::Value;
use turso::params::Params;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const BATCH: i64 = 20;

fn heap_bytes() -> u64 {
    dhat::HeapStats::get().curr_bytes as u64
}

fn main() {
    let _profiler = dhat::Profiler::new_heap();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(std::io::stderr)
        .init();

    let args: Vec<String> = std::env::args().collect();
    let n_conns: usize = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(4);
    let total_rows: i64 = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(30_000);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(n_conns.max(2))
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run(n_conns, total_rows));
    drop(rt);
    println!("FINAL after runtime drop: heap={}", heap_bytes());
}

async fn run(n_conns: usize, total_rows: i64) {
    let db_path = "/tmp/mvcc_starvation_probe.db";
    for suffix in ["", "-wal", "-log"] {
        let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
    }

    let db = turso::Builder::new_local(db_path).build().await.unwrap();
    let setup = db.connect().unwrap();
    setup.busy_timeout(Duration::from_secs(30)).unwrap();
    setup.pragma_update("journal_mode", "'mvcc'").await.unwrap();
    setup
        .execute("CREATE TABLE bench(id INTEGER PRIMARY KEY, data BLOB)", ())
        .await
        .unwrap();

    let next_id = Arc::new(AtomicI64::new(0));
    let start = Instant::now();
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Sampler: heap + log size every 500 ms.
    let sampler = {
        let done = done.clone();
        let next_id = next_id.clone();
        tokio::spawn(async move {
            let log_path = format!("{db_path}-log");
            while !done.load(Ordering::Relaxed) {
                let log = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
                println!(
                    "t={:>6}ms rows={:>6} heap={:>11} log={:>11}",
                    start.elapsed().as_millis(),
                    next_id.load(Ordering::Relaxed).min(total_rows),
                    heap_bytes(),
                    log
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
    };

    let mut workers = Vec::new();
    for _ in 0..n_conns {
        let conn = db.connect().unwrap();
        conn.busy_timeout(Duration::from_secs(30)).unwrap();
        let next_id = next_id.clone();
        workers.push(tokio::spawn(async move {
            let blob = vec![0u8; 2048];
            loop {
                let base = next_id.fetch_add(BATCH, Ordering::Relaxed);
                if base >= total_rows {
                    break;
                }
                loop {
                    match conn.execute("BEGIN CONCURRENT", ()).await {
                        Ok(_) => break,
                        Err(_) => tokio::time::sleep(Duration::from_millis(1)).await,
                    }
                }
                for i in 0..BATCH {
                    conn.execute(
                        "INSERT INTO bench(id, data) VALUES (?, ?)",
                        Params::Positional(vec![
                            Value::Integer(base + i),
                            Value::Blob(blob.clone()),
                        ]),
                    )
                    .await
                    .unwrap();
                }
                conn.execute("COMMIT", ()).await.unwrap();
            }
        }));
    }
    for w in workers {
        w.await.unwrap();
    }
    done.store(true, Ordering::Relaxed);
    sampler.await.unwrap();

    let log = std::fs::metadata(format!("{db_path}-log"))
        .map(|m| m.len())
        .unwrap_or(0);
    let db_size = std::fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);
    println!(
        "DONE   conns={n_conns} rows={total_rows} elapsed={}ms heap={} log={} db={}",
        start.elapsed().as_millis(),
        heap_bytes(),
        log,
        db_size
    );
    drop(setup);
    drop(db);
    println!("FINAL after db drop: heap={}", heap_bytes());
}
