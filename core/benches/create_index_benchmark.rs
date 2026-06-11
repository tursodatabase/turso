//! CREATE INDEX on large pre-populated table.
//!
//! Reproduces a production pain point where running `CREATE INDEX` against an
//! already-large table makes the surrounding transaction commit "giga slow".
//! The user-reported scenario is a 2M-row table; we exercise a range of sizes
//! to surface scaling behavior and compare against SQLite.
//!
//! tursodb runs in MVCC mode (`PRAGMA journal_mode = 'mvcc'`) since that is
//! the production deployment. SQLite stays on WAL — the comparison is not
//! apples-to-apples but it shows the absolute target the user is hitting.
//!
//! Each sample:
//!   1. Pre-populated table is reused (insertion is one-time setup).
//!   2. `CREATE INDEX` is timed end-to-end, including the implicit commit.
//!   3. `DROP INDEX` runs untimed to restore the starting state.
//!
//! Run with:
//!   cargo bench --bench create_index_benchmark --profile bench-profile
//!
//! `bench-profile` is the workspace profile that gives release optimizations
//! plus debug symbols (defined in the root Cargo.toml). The default `bench`
//! profile also works; `bench-profile` is preferred when profiling.

#[cfg(not(feature = "codspeed"))]
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
#[cfg(not(feature = "codspeed"))]
use pprof::criterion::{Output, PProfProfiler};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};

use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{Database, PlatformIO, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(not(feature = "codspeed"))]
macro_rules! iter_custom_or_iter {
    ($b:expr, |$iters:ident| $body:block) => {
        $b.iter_custom(|$iters| $body)
    };
}

#[cfg(feature = "codspeed")]
macro_rules! iter_custom_or_iter {
    ($b:expr, |$iters:ident| $body:block) => {
        $b.iter(|| {
            let $iters = 1;
            $body
        })
    };
}

/// Step a statement to completion, draining IO callbacks.
fn run_to_completion(
    stmt: &mut turso_core::Statement,
    db: &Arc<Database>,
) -> turso_core::Result<()> {
    loop {
        match stmt.step()? {
            StepResult::IO | StepResult::Yield => db.io.step()?,
            StepResult::Done => break,
            StepResult::Row => {}
            StepResult::Interrupt | StepResult::Busy => {
                panic!("Unexpected step result");
            }
        }
    }
    Ok(())
}

fn exec_turso(conn: &Arc<turso_core::Connection>, db: &Arc<Database>, sql: &str) {
    let mut stmt = conn.query(sql).unwrap().unwrap();
    run_to_completion(&mut stmt, db).unwrap();
}

/// Open a fresh turso db in MVCC mode (the production deployment target).
/// `PRAGMA journal_mode = 'mvcc'` must be set before any other DML.
/// Auto-checkpoint is disabled (`mvcc_checkpoint_threshold = -1`) so the
/// timed CREATE INDEX work doesn't get interleaved with checkpoint flushes;
/// this isolates the index-build cost from the checkpoint cost.
fn open_turso(temp_dir: &TempDir) -> (Arc<Database>, Arc<turso_core::Connection>) {
    let db_path = temp_dir.path().join("create_index_bench.db");
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, db_path.to_str().unwrap()).unwrap();
    let conn = db.connect().unwrap();
    exec_turso(&conn, &db, "PRAGMA journal_mode = 'mvcc'");
    exec_turso(&conn, &db, "PRAGMA mvcc_checkpoint_threshold = -1");
    exec_turso(&conn, &db, "PRAGMA synchronous = FULL");
    assert!(
        db.get_mv_store().is_some(),
        "MVCC store should be initialized after PRAGMA journal_mode = 'mvcc'"
    );
    (db, conn)
}

/// Open a fresh rusqlite db matching the turso configuration.
fn open_rusqlite(temp_dir: &TempDir) -> rusqlite::Connection {
    let db_path = temp_dir.path().join("create_index_bench.db");
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "synchronous", "FULL").unwrap();
    conn.pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    conn
}

/// Build a table with `row_count` rows of `(id INTEGER PK, val INTEGER, payload TEXT)`.
/// `val` is non-monotonic so that index sort + B-tree fill is not trivial.
fn populate_turso(db: &Arc<Database>, conn: &Arc<turso_core::Connection>, row_count: usize) {
    exec_turso(
        conn,
        db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, payload TEXT)",
    );

    // Bulk insert in batches inside a single transaction. Smaller batches
    // keep the parser/VDBE work bounded; the surrounding txn amortizes commit.
    let batch_size: usize = 1000;
    exec_turso(conn, db, "BEGIN");
    let mut i = 0usize;
    while i < row_count {
        let end = (i + batch_size).min(row_count);
        let mut sql = String::with_capacity((end - i) * 48);
        sql.push_str("INSERT INTO t (id, val, payload) VALUES ");
        for j in i..end {
            if j > i {
                sql.push(',');
            }
            // Pseudo-random val so index keys are not pre-sorted.
            let val = (j as i64).wrapping_mul(2654435761) & 0x7fff_ffff;
            sql.push_str(&format!("({j}, {val}, 'payload_{j}')"));
        }
        exec_turso(conn, db, &sql);
        i = end;
    }
    exec_turso(conn, db, "COMMIT");
}

fn populate_rusqlite(conn: &rusqlite::Connection, row_count: usize) {
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, payload TEXT)",
        [],
    )
    .unwrap();

    let batch_size: usize = 1000;
    conn.execute("BEGIN", []).unwrap();
    let mut i = 0usize;
    while i < row_count {
        let end = (i + batch_size).min(row_count);
        let mut sql = String::with_capacity((end - i) * 48);
        sql.push_str("INSERT INTO t (id, val, payload) VALUES ");
        for j in i..end {
            if j > i {
                sql.push(',');
            }
            let val = (j as i64).wrapping_mul(2654435761) & 0x7fff_ffff;
            sql.push_str(&format!("({j}, {val}, 'payload_{j}')"));
        }
        conn.execute(&sql, []).unwrap();
        i = end;
    }
    conn.execute("COMMIT", []).unwrap();
}

/// Sizes to bench. Huge sizes are gated behind env vars because each sample
/// can take many seconds.
fn row_counts() -> Vec<usize> {
    let mut v = vec![100_000];
    v.push(500_000);
    v.push(2_000_000);
    v
}

fn should_bench_create_index_turso(row_count: usize) -> bool {
    !(cfg!(feature = "codspeed") && row_count > 100_000)
}

/// Benchmark CREATE INDEX on an already-populated table.
///
/// Indexes the non-monotonic `val` column to force a real sort + B-tree build
/// rather than the fast-append path that monotonic keys can take.
fn bench_create_index(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");

    let mut group = criterion.benchmark_group("CREATE INDEX on populated table");
    // Each sample creates one index, but throughput is in rows so the report
    // shows rows/s of index build.
    group.sampling_mode(SamplingMode::Flat);

    for &row_count in &row_counts() {
        group.throughput(Throughput::Elements(row_count as u64));
        // The bigger the table, the fewer samples we can afford.
        let samples = match row_count {
            n if n <= 10_000 => 30,
            n if n <= 100_000 => 20,
            n if n <= 500_000 => 10,
            _ => 10,
        };
        group.sample_size(samples);
        // Per-iteration cost grows roughly linearly with row_count, so the
        // default 5s measurement budget is too small for the larger sizes.
        let (warm_up, measurement) = match row_count {
            n if n <= 100_000 => (
                std::time::Duration::from_secs(3),
                std::time::Duration::from_secs(10),
            ),
            n if n <= 500_000 => (
                std::time::Duration::from_secs(5),
                std::time::Duration::from_secs(20),
            ),
            _ => (
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(60),
            ),
        };
        group.warm_up_time(warm_up);
        group.measurement_time(measurement);

        if should_bench_create_index_turso(row_count) {
            // ---- Turso total latency ----
            {
                let temp_dir = tempfile::tempdir().unwrap();
                let (db, conn) = open_turso(&temp_dir);
                populate_turso(&db, &conn, row_count);

                group.bench_function(BenchmarkId::new("turso_total", row_count), |b| {
                    iter_custom_or_iter!(b, |iters| {
                        let mut total = std::time::Duration::ZERO;
                        for _ in 0..iters {
                            let start = std::time::Instant::now();
                            exec_turso(&conn, &db, "BEGIN");
                            exec_turso(&conn, &db, "CREATE INDEX idx_val ON t(val)");
                            exec_turso(&conn, &db, "COMMIT");
                            total += start.elapsed();
                            // Restore the un-indexed state for the next sample.
                            exec_turso(&conn, &db, "DROP INDEX idx_val");
                        }
                        total
                    });
                });
            }

            // ---- Turso commit latency ----
            {
                let temp_dir = tempfile::tempdir().unwrap();
                let (db, conn) = open_turso(&temp_dir);
                populate_turso(&db, &conn, row_count);

                group.bench_function(BenchmarkId::new("turso_commit_only", row_count), |b| {
                    iter_custom_or_iter!(b, |iters| {
                        let mut total = std::time::Duration::ZERO;
                        for _ in 0..iters {
                            exec_turso(&conn, &db, "BEGIN");
                            exec_turso(&conn, &db, "CREATE INDEX idx_val ON t(val)");
                            let start = std::time::Instant::now();
                            exec_turso(&conn, &db, "COMMIT");
                            total += start.elapsed();
                            exec_turso(&conn, &db, "DROP INDEX idx_val");
                        }
                        total
                    });
                });
            }
        }

        // ---- sqlite ----
        if enable_rusqlite {
            let temp_dir = tempfile::tempdir().unwrap();
            let conn = open_rusqlite(&temp_dir);
            populate_rusqlite(&conn, row_count);

            group.bench_function(BenchmarkId::new("sqlite", row_count), |b| {
                iter_custom_or_iter!(b, |iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        conn.execute("CREATE INDEX idx_val ON t(val)", []).unwrap();
                        total += start.elapsed();
                        conn.execute("DROP INDEX idx_val", []).unwrap();
                    }
                    total
                });
            });
            drop(conn);
            drop(temp_dir);
        }
    }

    group.finish();
}

/// Benchmark CREATE INDEX inside an explicit BEGIN/COMMIT, isolating the
/// commit cost the user is hitting in production.
#[cfg(feature = "codspeed")]
fn bench_create_index_commit(criterion: &mut Criterion) {
    let enable_rusqlite =
        std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() && !cfg!(feature = "codspeed");

    let mut group = criterion.benchmark_group("CREATE INDEX explicit commit");
    group.sampling_mode(SamplingMode::Flat);

    for &row_count in &row_counts() {
        if !should_bench_create_index_turso(row_count) {
            continue;
        }

        group.throughput(Throughput::Elements(row_count as u64));
        let samples = match row_count {
            n if n <= 10_000 => 30,
            n if n <= 100_000 => 20,
            _ => 10,
        };
        group.sample_size(samples);
        let (warm_up, measurement) = match row_count {
            n if n <= 100_000 => (
                std::time::Duration::from_secs(3),
                std::time::Duration::from_secs(10),
            ),
            n if n <= 500_000 => (
                std::time::Duration::from_secs(5),
                std::time::Duration::from_secs(20),
            ),
            _ => (
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(60),
            ),
        };
        group.warm_up_time(warm_up);
        group.measurement_time(measurement);

        {
            let temp_dir = tempfile::tempdir().unwrap();
            let (db, conn) = open_turso(&temp_dir);
            populate_turso(&db, &conn, row_count);

            group.bench_function(
                BenchmarkId::new("turso_create_index_commit", row_count),
                |b| {
                    iter_custom_or_iter!(b, |iters| {
                        let mut total = std::time::Duration::ZERO;
                        for _ in 0..iters {
                            let start = std::time::Instant::now();
                            exec_turso(&conn, &db, "BEGIN");
                            exec_turso(&conn, &db, "CREATE INDEX idx_val ON t(val)");
                            exec_turso(&conn, &db, "COMMIT");
                            total += start.elapsed();
                            exec_turso(&conn, &db, "DROP INDEX idx_val");
                        }
                        total
                    });
                },
            );
            drop(conn);
            drop(db);
            drop(temp_dir);
        }

        if enable_rusqlite {
            let temp_dir = tempfile::tempdir().unwrap();
            let conn = open_rusqlite(&temp_dir);
            populate_rusqlite(&conn, row_count);

            group.bench_function(BenchmarkId::new("sqlite", row_count), |b| {
                iter_custom_or_iter!(b, |iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        conn.execute("BEGIN", []).unwrap();
                        conn.execute("CREATE INDEX idx_val ON t(val)", []).unwrap();
                        conn.execute("COMMIT", []).unwrap();
                        total += start.elapsed();
                        conn.execute("DROP INDEX idx_val", []).unwrap();
                    }
                    total
                });
            });
        }
    }

    group.finish();
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = create_index_benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_create_index
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = create_index_benches;
    config = Criterion::default();
    targets = bench_create_index, bench_create_index_commit
}

criterion_main!(create_index_benches);
