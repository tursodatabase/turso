#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{black_box, criterion_group, criterion_main, Criterion};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use std::sync::Arc;
use std::time::Duration;
use turso_core::{Connection, Database, MemoryIO, SqliteDialect, Statement, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const ROWS: i64 = 20_000;

const QUERIES: &[(&str, &str)] = &[
    ("concat_two_columns", "SELECT name || payload FROM t"),
    (
        "concat_chain",
        "SELECT g || '-' || name || '-' || payload FROM t",
    ),
    (
        "concat_mixed_types",
        "SELECT 'id=' || id || ' v=' || v || ' f=' || f FROM t",
    ),
    (
        "upper_concat",
        "SELECT upper(name) || '-' || payload FROM t",
    ),
    (
        "substr_concat",
        "SELECT substr(payload, 2, 20) || name FROM t",
    ),
];

fn seed(conn: &Arc<Connection>) {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, g TEXT, name TEXT, payload TEXT, v INTEGER, f REAL)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 0..ROWS {
        conn.execute(format!(
            "INSERT INTO t VALUES ({i}, 'group-{}', 'name_{i}', 'payload-value-{i}', {i}, {i}.5)",
            i % 16
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_string_expr(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("string_expr");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(3));

    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    seed(&conn);

    for (label, sql) in QUERIES {
        group.bench_function(*label, |b| {
            let mut stmt = conn.prepare(sql).unwrap();
            assert_eq!(
                run(&db, &mut stmt),
                ROWS as usize,
                "{label} should give back every row"
            );
            b.iter(|| run(&db, &mut stmt));
        });
    }
    group.finish();
}

fn run(db: &Database, stmt: &mut Statement) -> usize {
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

criterion_group!(benches, bench_string_expr);
criterion_main!(benches);
