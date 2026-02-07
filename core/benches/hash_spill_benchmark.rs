#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
#[cfg(not(feature = "codspeed"))]
use pprof::criterion::{Output, PProfProfiler};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};

use std::sync::Arc;
use turso_core::types::Value;
use turso_core::vdbe::hash_table::{HashTable, HashTableConfig};
use turso_core::vdbe::CollationSeq;
use turso_core::MemoryIO;

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Create a hash table with the given memory budget
fn create_hash_table(mem_budget: usize) -> HashTable {
    let io = Arc::new(MemoryIO::new());
    let config = HashTableConfig {
        initial_buckets: 64,
        mem_budget,
        num_keys: 1,
        collations: vec![CollationSeq::Binary],
        temp_store: turso_core::TempStore::Default,
        track_matched: false,
    };
    HashTable::new(config, io)
}

/// Insert entries with integer keys and no payload
fn insert_integer_entries(ht: &mut HashTable, count: usize) {
    for i in 0..count {
        let key = vec![Value::from_i64(i as i64)];
        let _ = ht.insert(key, i as i64, vec![]);
    }
}

/// Insert entries with integer keys and text payload
fn insert_entries_with_text_payload(ht: &mut HashTable, count: usize, text_size: usize) {
    let payload_text: String = "x".repeat(text_size);
    for i in 0..count {
        let key = vec![Value::from_i64(i as i64)];
        let payload = vec![Value::Text(payload_text.clone().into())];
        let _ = ht.insert(key, i as i64, payload);
    }
}

/// Insert entries with text keys (for NOCASE hash testing)
fn insert_text_key_entries(ht: &mut HashTable, count: usize) {
    for i in 0..count {
        let key = vec![Value::Text(format!("key_{i}").into())];
        let _ = ht.insert(key, i as i64, vec![]);
    }
}

/// Benchmark: Build phase with tight memory budget (forces frequent spilling)
fn bench_build_tight_budget(c: &mut Criterion) {
    let mut group = c.benchmark_group("HashTable Build (Tight Budget)");

    for count in [1000, 5000, 10000] {
        group.throughput(Throughput::Elements(count as u64));

        // 32KB budget - will spill frequently
        group.bench_with_input(
            BenchmarkId::new("integer_keys", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let mut ht = create_hash_table(32 * 1024);
                    insert_integer_entries(&mut ht, count);
                    let _ = ht.finalize_build();
                    black_box(ht.has_spilled())
                });
            },
        );

        // With 100-byte text payload per entry
        group.bench_with_input(
            BenchmarkId::new("with_100b_payload", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let mut ht = create_hash_table(32 * 1024);
                    insert_entries_with_text_payload(&mut ht, count, 100);
                    let _ = ht.finalize_build();
                    black_box(ht.has_spilled())
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Build phase with relaxed memory budget (occasional spilling)
fn bench_build_relaxed_budget(c: &mut Criterion) {
    let mut group = c.benchmark_group("HashTable Build (Relaxed Budget)");

    for count in [1000, 5000, 10000] {
        group.throughput(Throughput::Elements(count as u64));

        // 256KB budget - will spill less frequently
        group.bench_with_input(
            BenchmarkId::new("integer_keys", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let mut ht = create_hash_table(256 * 1024);
                    insert_integer_entries(&mut ht, count);
                    let _ = ht.finalize_build();
                    black_box(ht.has_spilled())
                });
            },
        );

        // With 100-byte text payload
        group.bench_with_input(
            BenchmarkId::new("with_100b_payload", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let mut ht = create_hash_table(256 * 1024);
                    insert_entries_with_text_payload(&mut ht, count, 100);
                    let _ = ht.finalize_build();
                    black_box(ht.has_spilled())
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Build + Probe with spilling
fn bench_build_and_probe(c: &mut Criterion) {
    let mut group = c.benchmark_group("HashTable Build+Probe");

    for count in [1000, 5000] {
        group.throughput(Throughput::Elements(count as u64 * 2)); // build + probe

        group.bench_with_input(
            BenchmarkId::new("tight_budget", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    // Build phase
                    let mut ht = create_hash_table(32 * 1024);
                    insert_integer_entries(&mut ht, count);
                    let _ = ht.finalize_build();

                    // Probe phase - look up every key
                    let mut found = 0;
                    for i in 0..count {
                        let key = vec![Value::from_i64(i as i64)];
                        let partition_idx = ht.partition_for_keys(&key);

                        // Load partition if spilled
                        if ht.has_spilled() && !ht.is_partition_loaded(partition_idx) {
                            let _ = ht.load_spilled_partition(partition_idx);
                        }

                        if ht.probe_partition(partition_idx, &key).is_some() {
                            found += 1;
                        }
                    }
                    black_box(found)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("relaxed_budget", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    // Build phase
                    let mut ht = create_hash_table(256 * 1024);
                    insert_integer_entries(&mut ht, count);
                    let _ = ht.finalize_build();

                    // Probe phase
                    let mut found = 0;
                    for i in 0..count {
                        let key = vec![Value::from_i64(i as i64)];
                        let partition_idx = ht.partition_for_keys(&key);

                        if ht.has_spilled() && !ht.is_partition_loaded(partition_idx) {
                            let _ = ht.load_spilled_partition(partition_idx);
                        }

                        if ht.probe_partition(partition_idx, &key).is_some() {
                            found += 1;
                        }
                    }
                    black_box(found)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Text key hashing (tests NOCASE optimization)
fn bench_text_key_hashing(c: &mut Criterion) {
    let mut group = c.benchmark_group("HashTable Text Keys");

    for count in [1000, 5000] {
        group.throughput(Throughput::Elements(count as u64));

        // Binary collation
        group.bench_with_input(
            BenchmarkId::new("binary_collation", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let io = Arc::new(MemoryIO::new());
                    let config = HashTableConfig {
                        initial_buckets: 64,
                        mem_budget: 64 * 1024,
                        num_keys: 1,
                        collations: vec![CollationSeq::Binary],
                        temp_store: turso_core::TempStore::Default,
        track_matched: false,
                    };
                    let mut ht = HashTable::new(config, io);
                    insert_text_key_entries(&mut ht, count);
                    let _ = ht.finalize_build();
                    black_box(ht.has_spilled())
                });
            },
        );

        // NOCASE collation (tests allocation-free hash optimization)
        group.bench_with_input(
            BenchmarkId::new("nocase_collation", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let io = Arc::new(MemoryIO::new());
                    let config = HashTableConfig {
                        initial_buckets: 64,
                        mem_budget: 64 * 1024,
                        num_keys: 1,
                        collations: vec![CollationSeq::NoCase],
                        temp_store: turso_core::TempStore::Default,
        track_matched: false,
                    };
                    let mut ht = HashTable::new(config, io);
                    insert_text_key_entries(&mut ht, count);
                    let _ = ht.finalize_build();
                    black_box(ht.has_spilled())
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Large payload serialization
fn bench_large_payload_spill(c: &mut Criterion) {
    let mut group = c.benchmark_group("HashTable Large Payload Spill");

    for payload_size in [100, 500, 1000] {
        let count = 1000;
        group.throughput(Throughput::Bytes((count * payload_size) as u64));

        group.bench_with_input(
            BenchmarkId::new("payload_bytes", payload_size),
            &payload_size,
            |b, &payload_size| {
                b.iter(|| {
                    let mut ht = create_hash_table(32 * 1024); // Tight budget to force spilling
                    insert_entries_with_text_payload(&mut ht, count, payload_size);
                    let _ = ht.finalize_build();
                    black_box(ht.has_spilled())
                });
            },
        );
    }

    group.finish();
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_build_tight_budget, bench_build_relaxed_budget, bench_build_and_probe, bench_text_key_hashing, bench_large_payload_spill
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_build_tight_budget, bench_build_relaxed_budget, bench_build_and_probe, bench_text_key_hashing, bench_large_payload_spill
}

criterion_main!(benches);
