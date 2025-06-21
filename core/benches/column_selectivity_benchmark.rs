use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use limbo_core::{Database, PlatformIO, IO};
use pprof::criterion::{Output, PProfProfiler};
use std::sync::Arc;

// Title: Column Selectivity and Lazy Parsing Benchmarks

fn rusqlite_open() -> rusqlite::Connection {
    let sqlite_conn = rusqlite::Connection::open("../testing/testing.db").unwrap();
    sqlite_conn
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    sqlite_conn
}

fn bench_column_selectivity(criterion: &mut Criterion) {
    // https://github.com/tursodatabase/limbo/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());

    // Test different column selectivity scenarios on the users table
    let scenarios = vec![
        ("1_column", "SELECT id FROM users LIMIT 1000"),
        ("2_columns", "SELECT id, email FROM users LIMIT 1000"),
        (
            "3_columns",
            "SELECT id, email, first_name FROM users LIMIT 1000",
        ),
        (
            "5_columns",
            "SELECT id, email, first_name, last_name, age FROM users LIMIT 1000",
        ),
        ("all_columns", "SELECT * FROM users LIMIT 1000"),
    ];

    for (name, query) in scenarios {
        let mut group =
            criterion.benchmark_group(format!("Execute `{}` (Column Selectivity)", query));
        group.sample_size(50);

        // Benchmark Limbo
        let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
        let limbo_conn = db.connect().unwrap();

        group.bench_with_input(BenchmarkId::new("limbo", name), &query, |b, query| {
            let mut stmt = limbo_conn.prepare(query).unwrap();
            let io = io.clone();
            b.iter(|| {
                let mut row_count = 0;
                loop {
                    match stmt.step().unwrap() {
                        limbo_core::StepResult::Row => {
                            black_box(stmt.row());
                            row_count += 1;
                        }
                        limbo_core::StepResult::IO => {
                            let _ = io.run_once();
                        }
                        limbo_core::StepResult::Done => {
                            break;
                        }
                        limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => {
                            unreachable!();
                        }
                    }
                }
                stmt.reset();
                black_box(row_count);
            });
        });

        // Benchmark SQLite
        if enable_rusqlite {
            let sqlite_conn = rusqlite_open();

            group.bench_with_input(BenchmarkId::new("rusqlite", name), &query, |b, query| {
                let mut stmt = sqlite_conn.prepare(query).unwrap();
                b.iter(|| {
                    let mut row_count = 0;
                    let mut rows = stmt.raw_query();
                    while let Some(row) = rows.next().unwrap() {
                        black_box(row);
                        row_count += 1;
                    }
                    black_box(row_count);
                });
            });
        }

        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_column_selectivity
}
criterion_main!(benches);
