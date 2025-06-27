use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use limbo_core::{Database, PlatformIO, IO};
use pprof::criterion::{Output, PProfProfiler};
use std::sync::Arc;

fn rusqlite_open() -> rusqlite::Connection {
    let sqlite_conn = rusqlite::Connection::open("../testing/testing.db").unwrap();
    sqlite_conn
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    sqlite_conn
}

fn bench_prepare_query(criterion: &mut Criterion) {
    // https://github.com/tursodatabase/limbo/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false, false).unwrap();
    let limbo_conn = db.connect().unwrap();

    let queries = [
        "SELECT 1",
        "SELECT * FROM users LIMIT 1",
        "SELECT first_name, count(1) FROM users GROUP BY first_name HAVING count(1) > 1 ORDER BY count(1)  LIMIT 1",
    ];

    for query in queries.iter() {
        let mut group = criterion.benchmark_group(format!("Prepare `{}`", query));

        group.bench_with_input(
            BenchmarkId::new("limbo_parse_query", query),
            query,
            |b, query| {
                b.iter(|| {
                    limbo_conn.prepare(query).unwrap();
                });
            },
        );

        if enable_rusqlite {
            let sqlite_conn = rusqlite_open();

            group.bench_with_input(
                BenchmarkId::new("sqlite_parse_query", query),
                query,
                |b, query| {
                    b.iter(|| {
                        sqlite_conn.prepare(query).unwrap();
                    });
                },
            );
        }

        group.finish();
    }
}

fn bench_execute_select_rows(criterion: &mut Criterion) {
    // https://github.com/tursodatabase/limbo/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false, false).unwrap();
    let limbo_conn = db.connect().unwrap();

    let mut group = criterion.benchmark_group("Execute `SELECT * FROM users LIMIT ?`");

    for i in [1, 10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::new("limbo_execute_select_rows", i),
            &i,
            |b, i| {
                // TODO: LIMIT doesn't support query parameters.
                let mut stmt = limbo_conn
                    .prepare(format!("SELECT * FROM users LIMIT {}", *i))
                    .unwrap();
                let io = io.clone();
                b.iter(|| {
                    loop {
                        match stmt.step().unwrap() {
                            limbo_core::StepResult::Row => {
                                black_box(stmt.row());
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
                });
            },
        );

        if enable_rusqlite {
            let sqlite_conn = rusqlite_open();

            group.bench_with_input(
                BenchmarkId::new("sqlite_execute_select_rows", i),
                &i,
                |b, i| {
                    // TODO: Use parameters once we fix the above.
                    let mut stmt = sqlite_conn
                        .prepare(&format!("SELECT * FROM users LIMIT {}", *i))
                        .unwrap();
                    b.iter(|| {
                        let mut rows = stmt.raw_query();
                        while let Some(row) = rows.next().unwrap() {
                            black_box(row);
                        }
                    });
                },
            );
        }
    }

    group.finish();
}

fn bench_execute_select_1(criterion: &mut Criterion) {
    // https://github.com/tursodatabase/limbo/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false, false).unwrap();
    let limbo_conn = db.connect().unwrap();

    let mut group = criterion.benchmark_group("Execute `SELECT 1`");

    group.bench_function("limbo_execute_select_1", |b| {
        let mut stmt = limbo_conn.prepare("SELECT 1").unwrap();
        let io = io.clone();
        b.iter(|| {
            loop {
                match stmt.step().unwrap() {
                    limbo_core::StepResult::Row => {
                        black_box(stmt.row());
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
        });
    });

    if enable_rusqlite {
        let sqlite_conn = rusqlite_open();

        group.bench_function("sqlite_execute_select_1", |b| {
            let mut stmt = sqlite_conn.prepare("SELECT 1").unwrap();
            b.iter(|| {
                let mut rows = stmt.raw_query();
                while let Some(row) = rows.next().unwrap() {
                    black_box(row);
                }
            });
        });
    }

    group.finish();
}

fn bench_execute_select_count(criterion: &mut Criterion) {
    // https://github.com/tursodatabase/limbo/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false, false).unwrap();
    let limbo_conn = db.connect().unwrap();

    let mut group = criterion.benchmark_group("Execute `SELECT count() FROM users`");

    group.bench_function("limbo_execute_select_count", |b| {
        let mut stmt = limbo_conn.prepare("SELECT count() FROM users").unwrap();
        let io = io.clone();
        b.iter(|| {
            loop {
                match stmt.step().unwrap() {
                    limbo_core::StepResult::Row => {
                        black_box(stmt.row());
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
        });
    });

    if enable_rusqlite {
        let sqlite_conn = rusqlite_open();

        group.bench_function("sqlite_execute_select_count", |b| {
            let mut stmt = sqlite_conn.prepare("SELECT count() FROM users").unwrap();
            b.iter(|| {
                let mut rows = stmt.raw_query();
                while let Some(row) = rows.next().unwrap() {
                    black_box(row);
                }
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_prepare_query, bench_execute_select_1, bench_execute_select_rows, bench_execute_select_count
}
criterion_main!(benches);
