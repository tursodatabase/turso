//! One `PRAGMA wal_checkpoint(PASSIVE)` after inserting N rows.
//! Insert time is not measured. No helper racing writers.
//!
//! A second measurement opens a read snapshot, checkpoints N rows while the
//! snapshot keeps their versions in the store, inserts DELTA more rows, and
//! times the next checkpoint. That checkpoint only has DELTA rows to write,
//! so its time shows how much the collect pass depends on N.
//!
//! ```text
//! cargo bench -p turso_core --bench checkpoint_n_rows --profile bench-profile
//! CHECKPOINT_N_ROWS_OUT=/path.csv cargo bench -p turso_core --bench checkpoint_n_rows --profile bench-profile
//! CHECKPOINT_N_ROWS_BENCH_LARGE=1  also runs N=2_000_000
//! ```

#[cfg(not(feature = "codspeed"))]
use criterion::{criterion_group, criterion_main, Criterion};
#[cfg(not(feature = "codspeed"))]
use pprof::criterion::{Output, PProfProfiler};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};

use std::hint::black_box;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use turso_core::{
    Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, StepResult,
};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

struct Loaded {
    db: Arc<Database>,
    conn: Arc<Connection>,
    _dir: TempDir,
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_checkpoint_passive_n_rows(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("checkpoint-passive-n-rows");

    let out_path = std::env::var("CHECKPOINT_N_ROWS_OUT").ok();
    let mut out = out_path.as_ref().map(|path| {
        let mut f = std::fs::File::create(path).expect("CHECKPOINT_N_ROWS_OUT");
        writeln!(f, "n,samples,p50_ms,p99_ms,min_ms,max_ms,mean_ms").unwrap();
        f
    });
    let mut samples_out = out_path.as_ref().map(|path| {
        let p = std::path::Path::new(path);
        let samples_path = p.with_file_name(format!(
            "{}-samples.csv",
            p.file_stem().unwrap_or_default().to_string_lossy()
        ));
        let mut f = std::fs::File::create(samples_path).expect("samples csv");
        writeln!(f, "n,kind,i,took_ns").unwrap();
        f
    });

    eprintln!("n,samples,p50_ms,p99_ms,min_ms,max_ms,mean_ms");

    for n in row_counts() {
        let (warmup, samples) = sample_plan(n);
        let mut times_ns = Vec::with_capacity(samples);
        for i in 0..warmup {
            let ns = one_checkpoint_ns(n);
            eprintln!("n={n} warmup {i} {:.1}ms", ns as f64 / 1e6);
            if let Some(f) = samples_out.as_mut() {
                writeln!(f, "{n},warmup,{i},{ns}").unwrap();
            }
        }
        for i in 0..samples {
            let ns = one_checkpoint_ns(n);
            eprintln!("n={n} sample {i} {:.1}ms", ns as f64 / 1e6);
            times_ns.push(ns);
            if let Some(f) = samples_out.as_mut() {
                writeln!(f, "{n},sample,{i},{ns}").unwrap();
            }
        }
        if let Some(f) = samples_out.as_mut() {
            f.flush().unwrap();
        }
        times_ns.sort_unstable();
        let p50 = nearest_rank_ms(&times_ns, 0.5);
        let p99 = nearest_rank_ms(&times_ns, 0.99);
        let min = times_ns[0] as f64 / 1e6;
        let max = times_ns[times_ns.len() - 1] as f64 / 1e6;
        let mean = times_ns.iter().sum::<u64>() as f64 / times_ns.len() as f64 / 1e6;
        eprintln!("n={n},{samples},{p50:.1},{p99:.1},{min:.1},{max:.1},{mean:.1}");
        if let Some(f) = out.as_mut() {
            writeln!(
                f,
                "{n},{samples},{p50:.3},{p99:.3},{min:.3},{max:.3},{mean:.3}"
            )
            .unwrap();
            f.flush().unwrap();
        }
    }

    let mut delta_out = out_path.as_ref().map(|path| {
        let p = std::path::Path::new(path);
        let delta_path = p.with_file_name(format!(
            "{}-delta.csv",
            p.file_stem().unwrap_or_default().to_string_lossy()
        ));
        let mut f = std::fs::File::create(delta_path).expect("delta csv");
        writeln!(f, "n,delta,samples,p50_ms,p99_ms,min_ms,max_ms,mean_ms").unwrap();
        f
    });
    eprintln!("n,delta,samples,p50_ms,p99_ms,min_ms,max_ms,mean_ms");
    for n in row_counts() {
        let (warmup, samples) = sample_plan(n);
        let samples = samples.min(6);
        let mut times_ns = Vec::with_capacity(samples);
        for _ in 0..warmup {
            one_delta_checkpoint_ns(n, DELTA_ROWS);
        }
        for i in 0..samples {
            let ns = one_delta_checkpoint_ns(n, DELTA_ROWS);
            eprintln!(
                "n={n} delta={DELTA_ROWS} sample {i} {:.1}ms",
                ns as f64 / 1e6
            );
            times_ns.push(ns);
        }
        times_ns.sort_unstable();
        let p50 = nearest_rank_ms(&times_ns, 0.5);
        let p99 = nearest_rank_ms(&times_ns, 0.99);
        let min = times_ns[0] as f64 / 1e6;
        let max = times_ns[times_ns.len() - 1] as f64 / 1e6;
        let mean = times_ns.iter().sum::<u64>() as f64 / times_ns.len() as f64 / 1e6;
        eprintln!(
            "n={n},delta={DELTA_ROWS},{samples},{p50:.1},{p99:.1},{min:.1},{max:.1},{mean:.1}"
        );
        if let Some(f) = delta_out.as_mut() {
            writeln!(
                f,
                "{n},{DELTA_ROWS},{samples},{p50:.3},{p99:.3},{min:.3},{max:.3},{mean:.3}"
            )
            .unwrap();
            f.flush().unwrap();
        }
    }

    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_secs(1));
    group.bench_function("report", |b| {
        b.iter(|| black_box(1u8));
    });
    group.finish();
}

const DELTA_ROWS: usize = 10_000;

fn row_counts() -> Vec<usize> {
    if cfg!(feature = "codspeed") {
        return vec![10_000];
    }
    let mut n = vec![10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000];
    if std::env::var_os("CHECKPOINT_N_ROWS_BENCH_LARGE").is_some() {
        n.push(2_000_000);
    }
    n
}

fn sample_plan(n: usize) -> (usize, usize) {
    match n {
        n if n <= 10_000 => (2, 20),
        n if n <= 50_000 => (2, 15),
        n if n <= 100_000 => (1, 12),
        n if n <= 250_000 => (1, 10),
        n if n <= 500_000 => (1, 8),
        _ => (1, 6),
    }
}

fn nearest_rank_ms(sorted_ns: &[u64], q: f64) -> f64 {
    assert!(!sorted_ns.is_empty());
    let idx = ((sorted_ns.len() as f64 - 1.0) * q).round() as usize;
    sorted_ns[idx] as f64 / 1e6
}

fn one_checkpoint_ns(n: usize) -> u64 {
    let loaded = load_n_rows(n);
    let started = Instant::now();
    exec(&loaded.conn, &loaded.db, "PRAGMA wal_checkpoint(PASSIVE)");
    let ns = started.elapsed().as_nanos() as u64;
    black_box(ns)
}

/// Holds a read snapshot so the store keeps every version, checkpoints the N
/// rows, inserts `delta` more rows, and times the checkpoint that writes them.
fn one_delta_checkpoint_ns(n: usize, delta: usize) -> u64 {
    let loaded = load_n_rows(n);
    let reader = loaded.db.connect().unwrap();
    exec(&reader, &loaded.db, "BEGIN CONCURRENT");
    let seen = count_rows(&reader, &loaded.db);
    assert_eq!(seen, n as i64, "reader snapshot must see the N rows");
    exec(&loaded.conn, &loaded.db, "PRAGMA wal_checkpoint(PASSIVE)");
    insert_rows(&loaded.conn, &loaded.db, n, n + delta);
    let started = Instant::now();
    exec(&loaded.conn, &loaded.db, "PRAGMA wal_checkpoint(PASSIVE)");
    let ns = started.elapsed().as_nanos() as u64;
    exec(&reader, &loaded.db, "COMMIT");
    black_box(ns)
}

fn load_n_rows(n: usize) -> Loaded {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("checkpoint_n_rows.db");
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    exec(&conn, &db, "PRAGMA journal_mode = 'mvcc'");
    exec(&conn, &db, "PRAGMA mvcc_checkpoint_threshold = -1");
    exec(&conn, &db, "PRAGMA synchronous = OFF");
    assert!(
        db.get_mv_store().is_some(),
        "PRAGMA journal_mode = 'mvcc' must create an MvStore"
    );
    exec(
        &conn,
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    insert_rows(&conn, &db, 0, n);
    let counted = count_rows(&conn, &db);
    assert_eq!(counted, n as i64, "inserted row count must equal N");
    Loaded {
        db,
        conn,
        _dir: dir,
    }
}

/// Inserts rows with ids in `from..to` in one transaction, 1000 rows per statement.
fn insert_rows(conn: &Arc<Connection>, db: &Arc<Database>, from: usize, to: usize) {
    exec(conn, db, "BEGIN CONCURRENT");
    let batch: usize = 1000;
    let mut i = from;
    while i < to {
        let end = (i + batch).min(to);
        let mut sql = String::with_capacity((end - i) * 24);
        sql.push_str("INSERT INTO t (id, v) VALUES ");
        for j in i..end {
            if j > i {
                sql.push(',');
            }
            sql.push_str(&format!("({j}, {j})"));
        }
        exec(conn, db, &sql);
        i = end;
    }
    exec(conn, db, "COMMIT");
}

fn count_rows(conn: &Arc<Connection>, db: &Arc<Database>) -> i64 {
    let mut stmt = conn.query("SELECT COUNT(*) FROM t").unwrap().unwrap();
    let mut count = None;
    loop {
        match stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                db.io.step().unwrap();
            }
            StepResult::Row => {
                count = Some(stmt.row().unwrap().get::<i64>(0).unwrap());
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => panic!("COUNT(*) busy"),
        }
    }
    count.expect("COUNT(*) row")
}

fn exec(conn: &Arc<Connection>, db: &Arc<Database>, sql: &str) {
    let mut stmt = conn.query(sql).unwrap().unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Row => {}
            StepResult::Interrupt | StepResult::Busy => panic!("unexpected step: {sql}"),
        }
    }
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = checkpoint_n_rows_benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_checkpoint_passive_n_rows
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = checkpoint_n_rows_benches;
    config = Criterion::default();
    targets = bench_checkpoint_passive_n_rows
}

criterion_main!(checkpoint_n_rows_benches);
