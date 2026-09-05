//! Sorter benchmarks: insert, sort, and drain records through the ORDER BY
//! sorter with the key shapes that matter for the common prefix skipping
//! adaptive sort: integers, text with a long shared prefix, few distinct
//! values, multi-column keys, DESC keys, and presorted input.
//!
//! The sort buffer is large enough that nothing spills, so the numbers cover
//! key conditioning and the in-memory sort, not the external merge.
//!
//! Run with:
//!   cargo bench --bench sorter_benchmark --features bench

#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
#[cfg(not(feature = "codspeed"))]
use pprof::criterion::{Output, PProfProfiler};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};

use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaCha8Rng,
};
use std::sync::Arc;
use turso_core::types::{ImmutableRecord, Value};
use turso_core::vdbe::sorter::Sorter;
use turso_core::vdbe::CollationSeq;
use turso_core::{IOExt, MemoryIO, TempStore};
use turso_parser::ast::SortOrder;

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const SIZES: [usize; 2] = [10_000, 50_000];

struct Workload {
    name: &'static str,
    orders: Vec<SortOrder>,
    records: Vec<ImmutableRecord>,
}

fn build(
    name: &'static str,
    orders: Vec<SortOrder>,
    count: usize,
    mut key: impl FnMut(&mut ChaCha8Rng, usize) -> Vec<Value>,
) -> Workload {
    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let records = (0..count)
        .map(|i| {
            let mut values = key(&mut rng, i);
            values.push(Value::from_i64(i as i64));
            ImmutableRecord::from_values(&values, values.len()).unwrap()
        })
        .collect();
    Workload {
        name,
        orders,
        records,
    }
}

fn random_letters(rng: &mut ChaCha8Rng, len: usize) -> String {
    (0..len)
        .map(|_| (b'a' + (rng.next_u64() % 26) as u8) as char)
        .collect()
}

fn workloads(count: usize) -> Vec<Workload> {
    const CITIES: [&str; 8] = [
        "Amsterdam",
        "Berlin",
        "Copenhagen",
        "Dublin",
        "Edinburgh",
        "Florence",
        "Geneva",
        "Helsinki",
    ];
    vec![
        build("int_random", vec![SortOrder::Asc], count, |rng, _| {
            vec![Value::from_i64(rng.next_u64() as i64)]
        }),
        build("int_presorted", vec![SortOrder::Asc], count, |_, i| {
            vec![Value::from_i64(i as i64)]
        }),
        build("int_few_distinct", vec![SortOrder::Asc], count, |rng, _| {
            vec![Value::from_i64((rng.next_u64() % 16) as i64)]
        }),
        build("float_random", vec![SortOrder::Asc], count, |rng, _| {
            vec![Value::from_f64(rng.next_u64() as f64 / 1e12)]
        }),
        build(
            "text_shared_prefix",
            vec![SortOrder::Asc],
            count,
            |rng, _| {
                vec![Value::build_text(format!(
                    "https://www.example.com/catalog/products/item-{:010}",
                    rng.next_u64() % 10_000_000_000
                ))]
            },
        ),
        build(
            "text_shared_prefix_desc",
            vec![SortOrder::Desc],
            count,
            |rng, _| {
                vec![Value::build_text(format!(
                    "https://www.example.com/catalog/products/item-{:010}",
                    rng.next_u64() % 10_000_000_000
                ))]
            },
        ),
        build("text_random", vec![SortOrder::Asc], count, |rng, _| {
            vec![Value::build_text(random_letters(rng, 12))]
        }),
        build(
            "text_few_distinct",
            vec![SortOrder::Asc],
            count,
            |rng, _| {
                vec![Value::build_text(
                    CITIES[(rng.next_u64() % CITIES.len() as u64) as usize].to_string(),
                )]
            },
        ),
        build(
            "multi_column_country_name",
            vec![SortOrder::Asc, SortOrder::Asc],
            count,
            |rng, _| {
                vec![
                    Value::build_text(random_letters(rng, 2).to_uppercase()),
                    Value::build_text(random_letters(rng, 8)),
                ]
            },
        ),
    ]
}

fn sort_workload(io: &Arc<MemoryIO>, workload: &Workload) {
    let columns = workload.orders.len();
    let mut sorter = Sorter::new(
        &workload.orders,
        turso_core::alloc::vec![CollationSeq::Binary; columns],
        turso_core::alloc::vec![None; columns],
        turso_core::alloc::vec![None; columns],
        1 << 30,
        64,
        io.clone(),
        TempStore::Default,
    )
    .unwrap();
    for record in &workload.records {
        io.block(|| sorter.insert(record)).unwrap();
    }
    io.block(|| sorter.sort()).unwrap();
    let mut count = 0;
    while sorter.has_more() {
        black_box(sorter.record());
        io.block(|| sorter.next()).unwrap();
        count += 1;
    }
    assert_eq!(count, workload.records.len());
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_sorter(criterion: &mut Criterion) {
    let io = Arc::new(MemoryIO::new());
    let mut group = criterion.benchmark_group("Sorter insert, sort, and drain");
    for count in SIZES {
        group.throughput(Throughput::Elements(count as u64));
        for workload in workloads(count) {
            group.bench_with_input(
                BenchmarkId::new(workload.name, count),
                &workload,
                |b, workload| b.iter(|| sort_workload(&io, workload)),
            );
        }
    }
    group.finish();
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_sorter
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_sorter
}

criterion_main!(benches);
