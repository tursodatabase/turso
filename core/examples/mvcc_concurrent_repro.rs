//! Non-bun reproducer for the concurrent BEGIN CONCURRENT failure pattern
//! observed in `bun:turso`'s multi-worker writes bench
//! (`bench/sqlite/turso-multi-worker.mjs`).
//!
//! Topology — exactly mirrors the bun:turso bench:
//!   - One process, two threads sharing an `Arc<Database>`.
//!   - Each thread opens its own `Connection` off the shared Database.
//!   - Each thread runs a tight loop of `BEGIN CONCURRENT` / `INSERT` /
//!     `COMMIT` against a single table, with disjoint INTEGER PRIMARY KEY
//!     ranges (worker `w` writes ids in `[w*ROWS+1, (w+1)*ROWS]`).
//!   - MVCC is on (`PRAGMA journal_mode = 'mvcc'`).
//!   - No retries on `LimboError::Busy`. We count failures and report.
//!
//! Goal — answer: does the Busy-storm we see in bun:turso reproduce
//! purely from the Rust + turso_core side, or only when the Bun async
//! event-loop / Worker-threads harness is in the picture?
//!
//! Run:
//!   cargo run --release --example mvcc_concurrent_repro
//!   cargo run --release --example mvcc_concurrent_repro -- 100000
//!   ROWS_PER_WORKER=200000 cargo run --release --example mvcc_concurrent_repro
//!
//! Default ROWS_PER_WORKER is 50_000, matching the bun:turso bench
//! datapoint where worker 0 had ~23% Busy failures at BEGIN.

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use turso_core::{Database, LimboError, PlatformIO, IO};

#[derive(Default, Debug)]
struct Tally {
    committed: u64,
    busy_begin: u64,
    busy_insert: u64,
    busy_commit: u64,
    other_begin: u64,
    other_insert: u64,
    other_commit: u64,
    first_err: Option<String>,
}

fn worker(db: Arc<Database>, id: u64, rows: u64) -> Tally {
    let conn = db.connect().expect("connect failed");
    let id_base = id * rows + 1;
    let mut t = Tally::default();
    let record_err = |t: &mut Tally, stage: &str, e: &LimboError| {
        if t.first_err.is_none() {
            t.first_err = Some(format!("{stage}: {e:?}"));
        }
    };

    for i in 0..rows {
        // BEGIN CONCURRENT
        match conn.execute("BEGIN CONCURRENT") {
            Ok(()) => {}
            Err(LimboError::Busy) => {
                t.busy_begin += 1;
                continue;
            }
            Err(e) => {
                t.other_begin += 1;
                record_err(&mut t, "BEGIN", &e);
                continue;
            }
        }

        // INSERT — disjoint id range, no row-level collisions possible.
        let sql = format!(
            "INSERT INTO t (id, worker, v) VALUES ({}, {}, 'v{}-{}')",
            id_base + i,
            id,
            id,
            i
        );
        match conn.execute(&sql) {
            Ok(()) => {}
            Err(LimboError::Busy) => {
                t.busy_insert += 1;
                let _ = conn.execute("ROLLBACK");
                continue;
            }
            Err(e) => {
                t.other_insert += 1;
                record_err(&mut t, "INSERT", &e);
                let _ = conn.execute("ROLLBACK");
                continue;
            }
        }

        // COMMIT
        match conn.execute("COMMIT") {
            Ok(()) => t.committed += 1,
            Err(LimboError::Busy) => {
                t.busy_commit += 1;
                let _ = conn.execute("ROLLBACK");
            }
            Err(e) => {
                t.other_commit += 1;
                record_err(&mut t, "COMMIT", &e);
                let _ = conn.execute("ROLLBACK");
            }
        }
    }
    t
}

fn main() {
    let rows: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .or_else(|| {
            std::env::var("ROWS_PER_WORKER")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(50_000);
    let n_workers: u64 = std::env::var("N_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);

    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().expect("PlatformIO::new failed"));
    let tmp = std::env::temp_dir().join(format!("mvcc_concurrent_repro_{}.db", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    let _ = std::fs::remove_file(tmp.with_extension("db-wal"));
    let _ = std::fs::remove_file(tmp.with_extension("db-shm"));
    let db_path = tmp.to_string_lossy().into_owned();
    let cleanup_path = tmp;
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            let _ = std::fs::remove_file(self.0.with_extension("db-wal"));
            let _ = std::fs::remove_file(self.0.with_extension("db-shm"));
        }
    }
    let _cleanup = Cleanup(cleanup_path);
    eprintln!("DB path: {db_path}");
    let db = Database::open_file(io, &db_path).expect("open_file failed");

    // Bootstrap MVCC and schema on a setup connection — same shape as
    // bun:turso's `Database.open(":memory:", { concurrent: true })`.
    let setup = db.connect().expect("setup connect failed");
    setup
        .execute("PRAGMA journal_mode = 'mvcc'")
        .expect("PRAGMA journal_mode = mvcc failed");
    setup
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, worker INTEGER, v TEXT)")
        .expect("CREATE TABLE failed");
    drop(setup);

    println!(
        "Multi-thread MVCC concurrent-writes repro\n  workers={n_workers}, rows/worker={rows}, total_rows={}\n",
        n_workers * rows
    );

    let t0 = Instant::now();
    let handles: Vec<_> = (0..n_workers)
        .map(|id| {
            let db = db.clone();
            thread::spawn(move || worker(db, id, rows))
        })
        .collect();

    let tallies: Vec<Tally> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let elapsed = t0.elapsed();

    let total_committed: u64 = tallies.iter().map(|t| t.committed).sum();
    let total_busy: u64 = tallies
        .iter()
        .map(|t| t.busy_begin + t.busy_insert + t.busy_commit)
        .sum();
    let total_other: u64 = tallies
        .iter()
        .map(|t| t.other_begin + t.other_insert + t.other_commit)
        .sum();

    for (i, t) in tallies.iter().enumerate() {
        println!(
            "  worker {i}: committed={committed}/{rows}  busy(begin/insert/commit)={bb}/{bi}/{bc}  other(begin/insert/commit)={ob}/{oi}/{oc}",
            committed = t.committed,
            bb = t.busy_begin,
            bi = t.busy_insert,
            bc = t.busy_commit,
            ob = t.other_begin,
            oi = t.other_insert,
            oc = t.other_commit,
        );
        if let Some(err) = &t.first_err {
            println!("              first non-Busy error: {err}");
        }
    }

    let rate = total_committed as f64 / elapsed.as_secs_f64();
    println!(
        "\nTotal: committed={total_committed}/{}  busy={total_busy}  other_fail={total_other}  in {:.3}s ({:.0} rows/s)",
        n_workers * rows,
        elapsed.as_secs_f64(),
        rate
    );
}
