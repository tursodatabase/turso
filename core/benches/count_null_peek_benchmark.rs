//! Microbenchmark for the peek-only NULL-check opcode (tursodatabase/turso#4921):
//! compares COUNT(col), WHERE col IS [NOT] NULL, and FILTER (WHERE col IS NULL)
//! across a NULL-density sweep, for both a TEXT and a BLOB column (to separate
//! UTF-8 validation cost from allocation cost). Densities are deterministic via
//! id % k, not RANDOM(), so runs are reproducible and diffable across builds.
//!
//! Run:  cargo bench -p turso_core --bench count_null_peek_benchmark

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use turso_core::{Database, PlatformIO, SqliteDialect, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const N: usize = 1_000_000;

// NULL density expressed as "1 in K rows is non-NULL" so that 0% density is exactly
// representable (k = 0 => always NULL) alongside the sparser end of the sweep.
const DENSITIES: &[(&str, u64)] = &[
    ("d00", 0),   // 0% non-null (always NULL)
    ("d10", 10),  // 10% non-null (id % 10 == 0)
    ("d50", 2),   // 50% non-null (id % 2 == 0)
    ("d90", 10),  // 90% non-null (id % 10 != 0)
    ("d99", 100), // 99% non-null (id % 100 != 0)
];

const QUERY_SHAPES: &[(&str, &str)] = &[
    ("count_col", "SELECT COUNT({col}) FROM t"),
    ("where_is_null", "SELECT id FROM t WHERE {col} IS NULL"),
    (
        "where_is_not_null",
        "SELECT id FROM t WHERE {col} IS NOT NULL",
    ),
    (
        "filter_is_null",
        "SELECT COUNT(*) FILTER (WHERE {col} IS NULL) FROM t",
    ),
];

const COLUMNS: &[&str] = &["nullable_text", "nullable_blob"];

/// d90/d99 flip to != 0 since k alone can't express >50% non-null with == 0.
fn seed_db(n: usize, label: &str, k: u64) -> TempDir {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("count_null_peek.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE t(
             id INTEGER PRIMARY KEY,
             g TEXT,
             nullable_text TEXT,
             nullable_blob BLOB
         );",
    )
    .unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    {
        let mut ins = tx
            .prepare("INSERT INTO t(id, g, nullable_text, nullable_blob) VALUES (?1, ?2, ?3, ?4)")
            .unwrap();
        for i in 1..=n as i64 {
            let g = format!("group-{:02}", i % 16);
            let is_non_null = if label == "d00" {
                false
            } else if label == "d90" || label == "d99" {
                (i as u64) % k != 0
            } else {
                (i as u64) % k == 0
            };
            if is_non_null {
                let text = format!("row-text-payload-{i}");
                let blob = vec![(i % 251) as u8; 64];
                ins.execute((i, &g, Some(text), Some(blob))).unwrap();
            } else {
                ins.execute((i, &g, Option::<String>::None, Option::<Vec<u8>>::None))
                    .unwrap();
            }
        }
    }
    tx.commit().unwrap();
    dir
}

fn drain_turso(db: &Database, stmt: &mut turso_core::Statement) {
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row());
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_count_null_peek(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("count_null_peek");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(8));
    group.warm_up_time(Duration::from_secs(2));

    for (density_label, k) in DENSITIES {
        let dir = seed_db(N, density_label, *k);
        let path = dir.path().join("count_null_peek.db");

        for col in COLUMNS {
            #[allow(clippy::arc_with_non_send_sync)]
            let io = Arc::new(PlatformIO::new().unwrap());
            let db =
                Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
            let conn = db.connect().unwrap();
            {
                let mut p = conn.prepare("PRAGMA cache_size=-65536").unwrap();
                while !matches!(p.step().unwrap(), StepResult::Done) {
                    db.io.step().unwrap();
                }
            }

            for (shape_label, sql_template) in QUERY_SHAPES {
                let sql = sql_template.replace("{col}", col);
                let bench_id = format!("{shape_label}/{col}/{density_label}");
                group.bench_with_input(BenchmarkId::new(bench_id, N), &N, |b, _| {
                    let mut stmt = conn.prepare(&sql).unwrap();
                    b.iter(|| drain_turso(&db, &mut stmt));
                });
            }
        }
    }
    group.finish();
}

criterion_group!(benches, bench_count_null_peek);
criterion_main!(benches);
