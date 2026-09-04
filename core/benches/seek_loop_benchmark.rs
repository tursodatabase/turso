//! Index-seek microbenchmark over a 100k-row table.
//!
//! Each iteration runs point lookups through a secondary index. The hot
//! path is the b-tree descent, the leaf binary search, and the
//! deferred-seek rowid fetch. There is no scan loop. This isolates
//! per-seek overhead the same way scan_loop_benchmark isolates per-row
//! overhead.
//!
//! Run:  cargo bench -p turso_core --bench seek_loop_benchmark

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
const LOOKUPS_PER_ITER: usize = 1000;

struct Fixture {
    _dir: TempDir,
    db: Arc<Database>,
    conn: Arc<Connection>,
}

fn seed_db() -> Fixture {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("seek.db");
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();

    conn.execute("PRAGMA cache_size=-65536").unwrap();
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
                Value::build_text(format!("name_{i:07}")),
            )
            .unwrap();
        insert
            .bind_at(3usize.try_into().unwrap(), Value::from_i64(i))
            .unwrap();
        drive_stmt_to_completion(&db, &mut insert);
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("CREATE INDEX t_name ON t (name)").unwrap();

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
fn bench_seek_loop(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("seek_loop");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(3));

    let fixture = seed_db();
    group.bench_function("index_point_lookup_1k", |b| {
        let mut stmt = fixture
            .conn
            .prepare("SELECT value FROM t WHERE name = ?1")
            .unwrap();
        b.iter(|| {
            let mut rows = 0;
            for i in 0..LOOKUPS_PER_ITER as i64 {
                // Spread the probes across the key space. Then each lookup
                // descends a different path instead of replaying one leaf.
                let key = (i * 97) % NROWS as i64;
                stmt.bind_at(
                    1usize.try_into().unwrap(),
                    Value::build_text(format!("name_{key:07}")),
                )
                .unwrap();
                rows += drive_stmt_to_completion(&fixture.db, &mut stmt);
            }
            assert_eq!(rows, LOOKUPS_PER_ITER);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_seek_loop);
criterion_main!(benches);
