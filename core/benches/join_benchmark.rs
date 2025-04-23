use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use limbo_core::{Database, PlatformIO, IO};
use pprof::criterion::{Output, PProfProfiler};
use std::sync::Arc;

fn rusqlite_open() -> rusqlite::Connection {
    let sqlite_conn = rusqlite::Connection::open("../testing/database.db").unwrap();
    sqlite_conn
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    sqlite_conn
}

fn bench_join_query(criterion: &mut Criterion) {
    // https://github.com/tursodatabase/limbo/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    let enable_rusqlite = std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err();

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/database.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();

    let queries = [
        "SELECT u.first_name, u.last_name, p.name AS product_name, o.quantity, o.order_date \
         FROM users u \
         JOIN orders o ON u.id = o.user_id \
         JOIN products p ON o.product_id = p.id \
         LIMIT 10",
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

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_join_query
}
criterion_main!(benches);
