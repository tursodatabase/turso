use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use pprof::criterion::{Output, PProfProfiler};
use turso_core::{Database, PlatformIO};

const TPC_H_PATH: &str = "../perf/tpc-h/TPC-H.db";

macro_rules! tpc_query {
    ($num:literal) => {
        (
            $num,
            include_str!(concat!("../../perf/tpc-h/queries/", $num, ".sql")),
        )
    };
}

fn rusqlite_open_tpc_h() -> rusqlite::Connection {
    let sqlite_conn = rusqlite::Connection::open(TPC_H_PATH).unwrap();
    sqlite_conn
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    sqlite_conn
}

fn bench_tpc_h_queries(criterion: &mut Criterion) {
    // https://github.com/tursodatabase/turso/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), TPC_H_PATH, false, true).unwrap();
    let limbo_conn = db.connect().unwrap();

    let queries = [
        tpc_query!(1),
        // tpc_query!(2), // Skipped as subquery in bind_column references is todo!
        tpc_query!(3),
        // thread 'main' panicked at core/translate/planner.rs:256:28:
        // not yet implemented
        // tpc_query!(4),
        tpc_query!(5),
        tpc_query!(6),
        tpc_query!(7),
        tpc_query!(8),
        tpc_query!(9),
        tpc_query!(10),
        // tpc_query!(11), // Skipped not implemented
        tpc_query!(12),
        // thread 'main' panicked at core/storage/btree.rs:3233:26:
        // overflow cell with divider cell was not found
        // tpc_query!(13),
        tpc_query!(14),
        // thread 'main' panicked at core/benches/tpc_h_benchmark.rs:71:62:
        // called `Result::unwrap()` on an `Err` value: ParseError("CREATE VIEW not supported yet")
        // tpc_query!(15),

        // thread 'main' panicked at core/translate/planner.rs:267:34:
        // not yet implemented
        // tpc_query!(16),

        // thread 'main' panicked at core/translate/planner.rs:291:30:
        // not yet implemented
        // tpc_query!(17),

        // thread 'main' panicked at core/translate/planner.rs:267:34:
        // not yet implemented
        // tpc_query!(18),
        tpc_query!(19),
        // thread 'main' panicked at core/translate/planner.rs:267:34:
        // not yet implemented
        // tpc_query!(20),

        // thread 'main' panicked at core/translate/planner.rs:256:28:
        // not yet implemented
        // tpc_query!(21),
        // thread 'main' panicked at core/translate/planner.rs:291:30:
        // not yet implemented
        // tpc_query!(22),
    ];

    for (idx, query) in queries.iter() {
        let mut group = criterion.benchmark_group(format!("Query `{idx}` "));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        group.bench_with_input(
            BenchmarkId::new("limbo_tpc_h_query", idx),
            query,
            |b, query| {
                b.iter(|| {
                    let mut stmt = limbo_conn.prepare(query).unwrap();
                    loop {
                        match stmt.step().unwrap() {
                            turso_core::StepResult::Row => {
                                black_box(stmt.row());
                            }
                            turso_core::StepResult::IO => {
                                stmt.run_once().unwrap();
                            }
                            turso_core::StepResult::Done => {
                                break;
                            }
                            turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => {
                                unreachable!();
                            }
                        }
                    }
                    stmt.reset();
                });
            },
        );

        if enable_rusqlite {
            let sqlite_conn = rusqlite_open_tpc_h();

            group.bench_with_input(
                BenchmarkId::new("sqlite_tpc_h_query", idx),
                query,
                |b, query| {
                    let mut stmt = sqlite_conn.prepare(query).unwrap();
                    b.iter(|| {
                        let mut rows = stmt.raw_query();
                        while let Some(row) = rows.next().unwrap() {
                            black_box(row);
                        }
                    });
                },
            );
        }

        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_tpc_h_queries
}
criterion_main!(benches);
