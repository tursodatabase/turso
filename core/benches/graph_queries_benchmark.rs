use std::sync::Arc;

#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
#[cfg(not(feature = "codspeed"))]
use pprof::criterion::{Output, PProfProfiler};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode,
};

use turso_core::{Database, PlatformIO};

const DB_PATH: &str = "../perf/graph-queries/graph-queries.db";
const DB_PATH_ANALYZED: &str = "../perf/graph-queries/graph-queries-analyzed.db";

macro_rules! gq_query {
    ($name:literal) => {
        (
            $name,
            include_str!(concat!("../../perf/graph-queries/queries/", $name, ".sql")),
        )
    };
}

fn rusqlite_open(path: &str) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    conn
}

fn bench_graph_queries(criterion: &mut Criterion) {
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), DB_PATH).unwrap();
    let limbo_conn = db.connect().unwrap();

    let db_analyzed = Database::open_file(io, DB_PATH_ANALYZED).unwrap();
    let limbo_conn_analyzed = db_analyzed.connect().unwrap();

    let queries = [
        gq_query!("a_cooccurrence"),
        gq_query!("b_or_join"),
        gq_query!("c_edge_counts"),
        gq_query!("d_inlist_union"),
        gq_query!("e_activity_agg"),
        gq_query!("f1_streak_current"),
        gq_query!("f2_streak_longest"),
        gq_query!("3_aggregate_or_in"),
    ];

    for (name, query) in queries.iter() {
        let mut group = criterion.benchmark_group(format!("GraphQuery `{name}`"));
        group.sampling_mode(SamplingMode::Flat);
        group.sample_size(10);

        group.bench_with_input(BenchmarkId::new("limbo", name), query, |b, query| {
            let mut stmt = limbo_conn.prepare(query).unwrap();
            b.iter(|| {
                loop {
                    match stmt.step().unwrap() {
                        turso_core::StepResult::Row => {
                            black_box(stmt.row());
                        }
                        turso_core::StepResult::IO => {
                            db.io.step().unwrap();
                        }
                        turso_core::StepResult::Done => {
                            break;
                        }
                        turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => {
                            unreachable!();
                        }
                    }
                }
                stmt.reset().unwrap();
            });
        });

        group.bench_with_input(
            BenchmarkId::new("limbo_analyzed", name),
            query,
            |b, query| {
                let mut stmt = limbo_conn_analyzed.prepare(query).unwrap();
                b.iter(|| {
                    loop {
                        match stmt.step().unwrap() {
                            turso_core::StepResult::Row => {
                                black_box(stmt.row());
                            }
                            turso_core::StepResult::IO => {
                                db_analyzed.io.step().unwrap();
                            }
                            turso_core::StepResult::Done => {
                                break;
                            }
                            turso_core::StepResult::Interrupt | turso_core::StepResult::Busy => {
                                unreachable!();
                            }
                        }
                    }
                    stmt.reset().unwrap();
                });
            },
        );

        if enable_rusqlite {
            let sqlite_conn = rusqlite_open(DB_PATH);

            group.bench_with_input(BenchmarkId::new("sqlite", name), query, |b, query| {
                let mut stmt = sqlite_conn.prepare(query).unwrap();
                b.iter(|| {
                    let mut rows = stmt.raw_query();
                    while let Some(row) = rows.next().unwrap() {
                        black_box(row);
                    }
                });
            });

            let sqlite_conn_analyzed = rusqlite_open(DB_PATH_ANALYZED);

            group.bench_with_input(
                BenchmarkId::new("sqlite_analyzed", name),
                query,
                |b, query| {
                    let mut stmt = sqlite_conn_analyzed.prepare(query).unwrap();
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

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_graph_queries
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_graph_queries
}

criterion_main!(benches);
