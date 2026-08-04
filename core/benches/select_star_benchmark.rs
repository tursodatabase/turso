//! Benchmark for SELECT * over different shapes of tables
//!
//!   - select_star_106 : SELECT * over a 106-column table
//!   - select_star_21  : SELECT * over a 21-column table
//!   - select_star_10  : SELECT * over a 10-column table
//!
//! Run:  cargo bench -p turso_core --bench column_fetch_benchmark

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

/// (table name, column count, row count). Row counts are sized so each scan is
/// long enough to measure while the whole database stays cache-resident.
const TABLES: &[(&str, usize, usize)] = &[
    ("t106", 106, 20_000),
    ("t21", 21, 50_000),
    ("t10", 10, 100_000),
];

const QUERIES: &[(&str, &str, usize)] = &[
    ("select_star_106", "SELECT * FROM t106", 20_000),
    ("select_star_21", "SELECT * FROM t21", 50_000),
    ("select_star_10", "SELECT * FROM t10", 100_000),
];

//TODO use Turso for seeding
fn seed_db() -> TempDir {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("columns.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=DELETE;").unwrap();
    for (table, ncols, nrows) in TABLES {
        let cols = (0..*ncols)
            .map(|c| {
                if c % 2 == 0 {
                    format!("c{c} INTEGER")
                } else {
                    format!("c{c} TEXT")
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!("CREATE TABLE {table}({cols});"))
            .unwrap();
        let placeholders = (1..=*ncols)
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let tx = conn.unchecked_transaction().unwrap();
        {
            let mut ins = tx
                .prepare(&format!("INSERT INTO {table} VALUES ({placeholders})"))
                .unwrap();
            for i in 0..*nrows as i64 {
                let row = (0..*ncols)
                    .map(|c| {
                        if c % 2 == 0 {
                            rusqlite::types::Value::Integer(i.wrapping_mul(c as i64 + 1))
                        } else {
                            rusqlite::types::Value::Text(format!("v{}-{c}", i % 97))
                        }
                    })
                    .collect::<Vec<_>>();
                ins.execute(rusqlite::params_from_iter(row)).unwrap();
            }
        }
        tx.commit().unwrap();
    }
    dir
}

fn drive_stmt_to_completion(db: &Database, stmt: &mut turso_core::Statement) {
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
fn bench_select_star(criterion: &mut Criterion) {
    let dir = seed_db();
    let path = dir.path().join("columns.db");

    let mut group = criterion.benchmark_group("column_fetch");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(3));

    for (label, sql, nrows) in QUERIES {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        {
            let mut p = conn.prepare("PRAGMA cache_size=-65536").unwrap(); //negative means kibibytes (https://sqlite.org/pragma.html#pragma_cache_size)
            while !matches!(p.step().unwrap(), StepResult::Done) {
                db.io.step().unwrap();
            }
        }
        group.bench_with_input(BenchmarkId::new(*label, nrows), nrows, |b, _| {
            let mut stmt = conn.prepare(sql).unwrap();
            b.iter(|| drive_stmt_to_completion(&db, &mut stmt));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_select_star);
criterion_main!(benches);
