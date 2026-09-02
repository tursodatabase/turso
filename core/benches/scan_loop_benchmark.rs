//! Scan-loop microbenchmark over a 100k-row table.
//!
//! `SELECT 1 FROM t` is the scan analogue of the existing `SELECT 1`
//! benchmark: the loop body is just ResultRow + Next, so it isolates
//! per-instruction dispatch and cursor-advance overhead with no decode
//! work. `SELECT * FROM t` adds column decoding on top.
//!
//! Run:  cargo bench -p turso_core --bench scan_loop_benchmark

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{black_box, criterion_group, criterion_main, Criterion};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use turso_core::{Connection, Database, PlatformIO, SqliteDialect, Statement, StepResult, Value};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const NROWS: usize = 100_000;

struct Fixture {
    _dir: TempDir,
    db: Arc<Database>,
    conn: Arc<Connection>,
}

fn seed_db() -> Fixture {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("scan.db");
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();

    conn.execute("PRAGMA cache_size=-65536").unwrap(); //negative means kibibytes (https://sqlite.org/pragma.html#pragma_cache_size)
    conn.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value INTEGER NOT NULL)",
    )
    .unwrap();

    conn.execute("BEGIN").unwrap();
    let mut insert = conn.prepare("INSERT INTO t VALUES (?1, ?2, ?3)").unwrap();
    for i in 0..NROWS as i64 {
        insert
            .bind_at(1usize.try_into().unwrap(), Value::from_i64(i))
            .unwrap();
        insert
            .bind_at(
                2usize.try_into().unwrap(),
                Value::build_text(format!("name_{i}")),
            )
            .unwrap();
        insert
            .bind_at(3usize.try_into().unwrap(), Value::from_i64(i))
            .unwrap();
        drive_stmt_to_completion(&db, &mut insert);
    }
    conn.execute("COMMIT").unwrap();

    Fixture {
        _dir: dir,
        db,
        conn,
    }
}

fn drive_stmt_to_completion(db: &Database, stmt: &mut Statement) -> usize {
    let mut rows = 0;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row());
                rows += 1;
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
    rows
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_scan_loop(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("scan_loop");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(3));

    let fixture = seed_db();
    for (label, sql) in [
        ("select_one_100k", "SELECT 1 FROM t"),
        ("select_star_100k", "SELECT * FROM t"),
    ] {
        group.bench_function(label, |b| {
            let mut stmt = fixture.conn.prepare(sql).unwrap();
            assert_eq!(
                drive_stmt_to_completion(&fixture.db, &mut stmt),
                NROWS,
                "t should have been seeded with {NROWS} rows"
            );
            b.iter(|| drive_stmt_to_completion(&fixture.db, &mut stmt));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_scan_loop);
criterion_main!(benches);
