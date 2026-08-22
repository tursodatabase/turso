//! Interpreter-vs-JIT benchmarks on TPC-H-shaped queries.
//!
//! The dataset is generated in memory, so unlike the tpc_h_benchmark this
//! runs on CodSpeed. Each query is measured twice on identical data: once on
//! a connection with the JIT disabled and once with it enabled, so the two
//! benchmark series track the interpreter and the JIT independently. Without
//! the `jit` feature both series measure the interpreter.

use std::sync::Arc;

#[cfg(not(feature = "codspeed"))]
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion, SamplingMode};

use turso_core::{Database, MemoryIO, SqliteDialect, StepResult};

const ROW_COUNT: usize = 100_000;

fn setup() -> (Arc<Database>, Arc<turso_core::Connection>) {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    run_all(&db, &conn, "CREATE TABLE lineitem(quantity REAL, extendedprice REAL, discount REAL, tax REAL, returnflag TEXT, linestatus TEXT, shipdate TEXT)");
    run_all(
        &db,
        &conn,
        &format!(
            "WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM g WHERE x < {ROW_COUNT}) \
             INSERT INTO lineitem \
             SELECT (x * 7) % 50 + 1, \
                    ((x * 937) % 90000) / 100.0 + 100.0, \
                    ((x * 13) % 11) / 100.0, \
                    ((x * 3) % 9) / 100.0, \
                    CASE (x * 11) % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'N' ELSE 'R' END, \
                    CASE (x * 5) % 2 WHEN 0 THEN 'F' ELSE 'O' END, \
                    printf('19%02d-%02d-%02d', 92 + (x % 7), 1 + (x * 5) % 12, 1 + (x * 9) % 28) \
             FROM g"
        ),
    );
    (db, conn)
}

fn run_all(db: &Arc<Database>, conn: &Arc<turso_core::Connection>, sql: &str) -> u64 {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = 0;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => rows += 1,
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            other => panic!("unexpected step result {other:?}"),
        }
    }
    rows
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_queries(criterion: &mut Criterion) {
    let queries = [
        (
            "q1_pricing_summary",
            "SELECT returnflag, linestatus, sum(quantity), sum(extendedprice), \
             sum(extendedprice * (1 - discount)), \
             sum(extendedprice * (1 - discount) * (1 + tax)), \
             avg(quantity), avg(extendedprice), avg(discount), count(*) \
             FROM lineitem WHERE shipdate <= '1998-09-02' \
             GROUP BY returnflag, linestatus ORDER BY returnflag, linestatus",
        ),
        (
            "q6_forecast_revenue",
            "SELECT sum(extendedprice * discount) FROM lineitem \
             WHERE shipdate >= '1994-01-01' AND shipdate < '1995-01-01' \
             AND discount BETWEEN 0.03 AND 0.05 AND quantity < 24",
        ),
    ];
    let (db, conn) = setup();
    for (name, sql) in queries {
        let mut group = criterion.benchmark_group(format!("JIT `{name}`"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);
        for (label, jit) in [("interp", false), ("jit", true)] {
            conn.set_jit_enabled(jit);
            // Prepare outside the measurement so the JIT series measures
            // steady-state execution of already-compiled code; compilation
            // happens once during warmup and is cached on the program.
            let mut stmt = conn.prepare(sql).unwrap();
            group.bench_function(label, |b| {
                b.iter(|| {
                    stmt.reset().unwrap();
                    let mut rows = 0u64;
                    loop {
                        match stmt.step().unwrap() {
                            StepResult::Row => rows += 1,
                            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                                db.io.step().unwrap();
                            }
                            StepResult::Done => break,
                            other => panic!("unexpected step result {other:?}"),
                        }
                    }
                    rows
                });
            });
        }
        conn.set_jit_enabled(true);
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_queries
}

criterion_main!(benches);
