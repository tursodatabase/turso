#[cfg(not(feature = "codspeed"))]
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};

use rand::seq::SliceRandom;
use rand_chacha::{rand_core::SeedableRng, ChaCha8Rng};
use std::sync::Arc;
use turso_core::types::{ImmutableRecord, Value};
use turso_core::vdbe::sorter::{BenchSortAlgorithm, Sorter};
use turso_core::vdbe::CollationSeq;
use turso_core::IOExt;
use turso_core::{MemoryIO, TempStore};
use turso_parser::ast::SortOrder;

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

struct SortInput {
    records: Vec<ImmutableRecord>,
    total_bytes: usize,
    key_count: usize,
}

fn build_records(count: usize, key_count: usize) -> SortInput {
    let mut records = Vec::with_capacity(count);
    let mut total_bytes = 0;

    for i in 0..count {
        let mut values = Vec::with_capacity(key_count);
        for key_idx in 0..key_count {
            values.push(value_for_key(i, key_idx));
        }
        let record = ImmutableRecord::from_values(&values, values.len());
        total_bytes += record.get_payload().len();
        records.push(record);
    }

    let seed = 0x0A05_5EED_u64 ^ (count as u64) ^ ((key_count as u64) << 16);
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    records.shuffle(&mut rng);

    SortInput {
        records,
        total_bytes,
        key_count,
    }
}

fn value_for_key(row_idx: usize, key_idx: usize) -> Value {
    match key_idx % 5 {
        0 => Value::Text(format!("t{row_idx:08}").into()),
        1 => Value::Integer((row_idx as i64).wrapping_mul(31) - 7),
        2 => Value::Float(((row_idx as f64) * 0.25) - 1234.5),
        3 => Value::Blob(vec![
            (row_idx & 0xFF) as u8,
            ((row_idx >> 8) & 0xFF) as u8,
            (row_idx as u8).wrapping_add(key_idx as u8),
        ]),
        _ => Value::Null,
    }
}

fn setup_sorter(input: &SortInput, algorithm: BenchSortAlgorithm) -> (Arc<MemoryIO>, Sorter) {
    let io = Arc::new(MemoryIO::new());
    let max_buffer_size = input.total_bytes + 1024;
    let mut sorter = Sorter::new(
        &vec![SortOrder::Asc; input.key_count],
        vec![CollationSeq::Binary; input.key_count],
        max_buffer_size,
        64,
        io.clone(),
        TempStore::Default,
    );
    sorter.set_bench_sort_algorithm(algorithm);

    for record in &input.records {
        io.block(|| sorter.insert(record))
            .expect("Failed to insert record");
    }

    (io, sorter)
}

fn run_sort(io: &Arc<MemoryIO>, sorter: &mut Sorter) {
    io.block(|| sorter.sort()).expect("Sorter::sort failed");
    black_box(sorter.record());
}

// Compare sorting performance between ValueRef::cmp and binary sort key encoding.
fn bench_sorter_key_compare(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sorter sort-key vs std");

    for count in [10_000, 100_000] {
        for key_count in [1, 2, 4] {
            let input = build_records(count, key_count);
            group.throughput(Throughput::Elements(count as u64));

            group.bench_with_input(
                BenchmarkId::new("std-cmp", format!("{count}/k{key_count}")),
                &input,
                |b, input| {
                    b.iter_batched(
                        || setup_sorter(input, BenchSortAlgorithm::StdCmp),
                        |(io, mut sorter)| run_sort(&io, &mut sorter),
                        BatchSize::LargeInput,
                    );
                },
            );

            group.bench_with_input(
                BenchmarkId::new("std-key", format!("{count}/k{key_count}")),
                &input,
                |b, input| {
                    b.iter_batched(
                        || setup_sorter(input, BenchSortAlgorithm::StdSortKey),
                        |(io, mut sorter)| run_sort(&io, &mut sorter),
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_sorter_key_compare);
criterion_main!(benches);
