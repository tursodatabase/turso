//! Microbenchmark: a GROUP BY whose projection runs a correlated scalar subquery
//! per outer row -- a "latest related row" index lookup (ORDER BY id DESC LIMIT 1)
//! over a full scan of the outer table -- at a couple of row counts, vs SQLite.
//!
//! Run: cargo bench -p turso_core --bench correlated_subquery_benchmark
//!      (set DISABLE_RUSQLITE_BENCHMARK=1 to skip the SQLite comparison)

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{Database, PlatformIO, SqliteDialect, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const SCALES: [usize; 2] = [50_000, 200_000];

const QUERY: &str = "SELECT o.grp, \
     COALESCE((SELECT i.val FROM inner_t i \
               WHERE i.k = o.k ORDER BY i.id DESC LIMIT 1), 'none') AS latest, \
     COUNT(*) FROM outer_t o GROUP BY o.grp, latest";

fn seed_db(n: usize) -> TempDir {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bench.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE outer_t(id INTEGER PRIMARY KEY, k INTEGER NOT NULL, grp TEXT);
         CREATE TABLE inner_t(id INTEGER PRIMARY KEY, k INTEGER NOT NULL, val TEXT);",
    )
    .unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    {
        let mut ins_o = tx
            .prepare("INSERT INTO outer_t VALUES (?1, ?2, ?3)")
            .unwrap();
        let mut ins_i = tx
            .prepare("INSERT INTO inner_t VALUES (?1, ?2, ?3)")
            .unwrap();
        for i in 1..=n as i64 {
            ins_o
                .execute((i, i, if i % 3 == 0 { "a" } else { "b" }))
                .unwrap();
            ins_i
                .execute((i, i, if i % 2 == 0 { "x" } else { "y" }))
                .unwrap();
        }
    }
    tx.commit().unwrap();
    conn.execute_batch("CREATE INDEX idx_inner_k ON inner_t(k);")
        .unwrap();
    dir
}

fn drain_turso(db: &Database, stmt: &mut turso_core::Statement) {
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row());
            }
            StepResult::IO | StepResult::Yield => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_correlated(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();
    let mut group = criterion.benchmark_group("correlated_subquery");
    group.sample_size(10);

    for &n in SCALES.iter() {
        let dir = seed_db(n);
        let path = dir.path().join("bench.db");

        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        group.bench_with_input(BenchmarkId::new("turso", n), &n, |b, _| {
            let mut stmt = conn.prepare(QUERY).unwrap();
            b.iter(|| drain_turso(&db, &mut stmt));
        });

        if enable_rusqlite {
            let sconn = rusqlite::Connection::open(&path).unwrap();
            sconn
                .pragma_update(None, "locking_mode", "EXCLUSIVE")
                .unwrap();
            group.bench_with_input(BenchmarkId::new("sqlite", n), &n, |b, _| {
                let mut stmt = sconn.prepare(QUERY).unwrap();
                b.iter(|| {
                    let mut rows = stmt.raw_query();
                    while let Some(row) = rows.next().unwrap() {
                        black_box(row);
                    }
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_correlated);
criterion_main!(benches);
