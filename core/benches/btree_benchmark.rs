use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use limbo_core::{Database, MemoryIO, IO};
use pprof::criterion::{Output, PProfProfiler};
use std::sync::Arc;

fn bench_insert(criterion: &mut Criterion) {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io.clone(), "../testing/testing.db", false).unwrap();
    let limbo_conn = db.connect().unwrap();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_insert
}
criterion_main!(benches);
