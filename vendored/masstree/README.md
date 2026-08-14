# masstree

A high-performance concurrent ordered map for Rust. It stores keys as `&[u8]` and supports variable-length keys by building a trie of B+trees, based on the [Masstree paper](https://pdos.csail.mit.edu/papers/masstree:eurosys12.pdf)

**Disclaimer:** This is an independent implementation. It is not endorsed by, affiliated with, or connected to the original Masstree authors or their institutions.

## Status

Current Version: v0.9.5

Significantly reduced duplicate code through trait based abstractions, while improving performance further. All standard data structure features are implemented and streamlined, on top the specialized features like range scans and prefix queries. From latency analysis, it seems to have a surprisingly strong tail latency profile for a concurrent ordered map. The cache-line aligned layout design and aggressive prefetching seems to have paid off, on top of masstree's original excellent architecture.

The next issues to work on are the memory profile (some progress) and memcomparable mappings for general key types, before 1.0.

NOTE: All benchmarks were run with `mimalloc` feature.

## vs Rust Concurrent Maps (12T SMT)

> Source: `runs/run208_read_write.txt` (masstree), `runs/run184_read_write_reworked.txt` (competitors)
> **Config:** 12 threads on 6 physical cores (SMT/hyperthreading), 200 samples.

| Benchmark | masstree15 | tree_index | skipmap | indexset | MT vs Best |
|-----------|-----------|------------|---------|----------|------------|
| 01_uniform | 53.16 | 14.49 | 10.80 | 14.94 | 3.56x |
| 02_zipfian | 45.54 | 18.86 | 12.92 | 3.28 | 2.42x |
| 03_shared_prefix | 31.84 | 14.61 | 11.08 | 14.36 | 2.18x |
| 04_high_contention | 79.81 | 21.64 | 16.90 | 1.97 | 3.69x |
| 05_large_dataset | 18.72 | 11.35 | 7.50 | 9.17 | 1.65x |
| 06_single_hot_key | 15.58 | 6.82 | 7.46 | 2.52 | 2.09x |
| 07_mixed_50_50 | 40.41 | 8.73 | 6.46 | 14.86 | 2.72x |
| 08_8byte_keys | 62.47 | 24.97 | 14.06 | 19.69 | 2.50x |
| 09_pure_read | 56.77 | 24.12 | 14.40 | 17.40 | 2.35x |
| 10_remove_heavy | 26.69 | 11.18 | 7.18 | 4.26 | 2.39x |
| 13_insert_only_fair | 44.79 | 24.29 | 15.13 | 6.30 | 1.84x |
| 14_pure_insert | 15.28 | 11.96 | 9.86 | 2.30 | 1.28x |

## High-Impact Workloads (12T SMT)

> Source: `runs/run206_high_impact.txt` (masstree), `runs/run188_high_impact.txt` (competitors)
> **Config:** 12 threads on 6 physical cores (SMT), 200 samples

Benchmarks targeting Masstree's architectural advantages: long keys, variable-length keys,
hot key patterns, mixed operations, prefix queries, deep trie traversal, and mixed prefix workloads.

| Benchmark | masstree15 | indexset | tree_index | skipmap | MT vs Best |
|-----------|------------|----------|------------|---------|------------|
| 01_long_keys_128b | 44.53 | 14.58 | 13.19 | 11.11 | 3.05x |
| 02_multiple_hot_keys | 43.47 | 14.02 | 13.58 | 13.44 | 3.10x |
| 03_mixed_get_insert_remove | 30.61 | 5.96 | 10.24 | 8.767 | 2.99x |
| 04_variable_long_keys | 36.33 | 9.43 | 8.644 | 7.842 | 3.85x |
| 05_prefix_queries (Kitem/s) | 1115 | n/a | 508.4 | 145.2 | 2.19x |
| 06_deep_trie_traversal | 31.49 | 13.43 | 8.122 | 9.554 | 2.34x |
| 07_deep_trie_read_only | 41.23 | 14.99 | 14.77 | 13.77 | 2.75x |
| 08_variable_keys_arc | 36.31 | 11.79 | 10.60 | 9.047 | 3.08x |
| 09_prefix_realistic_mixed | 6.133 | n/a | 3.029 | 1.051 | 2.02x |

## Range Scans (6T Physical)

> Source: `runs/run191_range_scan.txt`
> **Config:** Physical cores only, 200 samples, performance governor

| Benchmark | masstree15_inline | tree_index | MT vs TI |
|-----------|-------------------|------------|----------|
| 01_sequential_full_scan | 26.73 | 13.07 | 2.04x |
| 02_reverse_scan | 27.87 | 13.27 | 2.10x |
| 03_clustered_scan | 25.16 | 13.08 | 1.92x |
| 04_sparse_scan | 22.28 | 12.78 | 1.74x |
| 05_shared_prefix_scan | 22.93 | 15.33 | 1.50x |
| 06_suffix_differ_scan | 21.28 | 14.55 | 1.46x |
| 07_hierarchical_scan | 24.47 | 14.60 | 1.68x |
| 08_adversarial_splits | 26.83 | 8.807 | 3.05x |
| 09_interleaved_scan | 22.41 | 12.86 | 1.74x |
| 10_blink_stress_scan | 24.48 | 12.81 | 1.91x |
| 11_random_keys_scan | 26.02 | 12.36 | 2.10x |
| 12_long_keys_64b_scan | 26.06 | 14.24 | 1.83x |
| 15_full_scan_aggregate | 183.1 | 84.59 | 2.16x |
| 16_insert_heavy | 23.18 | 17.38 | 1.33x |
| 17_hot_spot | 11.24 | 5.318 | 2.11x |

## Range Scans (12T SMT)

> Source: `runs/run191_range_scan.txt`
> **Config:** 12 threads on 6 physical cores (SMT), 200 samples

| Benchmark | masstree15_inline | tree_index | MT vs TI |
|-----------|-------------------|------------|----------|
| 01_sequential_full_scan | 31.77 | 14.49 | 2.19x |
| 02_reverse_scan | 29.96 | 14.66 | 2.04x |
| 03_clustered_scan | 31.85 | 14.54 | 2.19x |
| 04_sparse_scan | 30.95 | 14.58 | 2.12x |
| 05_shared_prefix_scan | 24.88 | 18.62 | 1.34x |
| 06_suffix_differ_scan | 23.41 | 18.16 | 1.29x |
| 07_hierarchical_scan | 26.99 | 18.46 | 1.46x |
| 08_adversarial_splits | 29.10 | 11.32 | 2.57x |
| 09_interleaved_scan | 24.82 | 15.34 | 1.62x |
| 10_blink_stress_scan | 30.05 | 15.12 | 1.99x |
| 11_random_keys_scan | 25.62 | 14.70 | 1.74x |
| 12_long_keys_64b_scan | 28.29 | 17.77 | 1.59x |
| 15_full_scan_aggregate | 230.8 | 111.9 | 2.06x |
| 16_insert_heavy | 25.09 | 21.88 | 1.15x |
| 17_hot_spot | 10.61 | 6.458 | 1.64x |

## Tail Latency (pbench, Per-Operation)

> Source: `runs/run213_tail_latency.txt` (8T), `runs/run207_tail_latency.txt` (1T)
> **Config:** 200k samples per benchmark, `sample_size=1` (unbatched), TSC timer (10 ns precision)

### Point Lookups

| Benchmark | masstree | treeindex | skipmap | indexset |
|-----------|----------|-----------|---------|----------|
| get 1T p50 | 140 ns | 250 ns | 350 ns | 270 ns |
| get 1T p99.9 | 501 ns | 411 ns | 801 ns | 621 ns |
| get 8T p50 | 340 ns | 551 ns | 621 ns | 591 ns |
| get 8T p99.9 | 922 ns | 1.15 us | 1.36 us | 1.34 us |
| get 1M 1T p50 | 321 ns | 461 ns | 691 ns | 521 ns |
| get deep 8T p50 | 661 ns | 741 ns | 831 ns | - |
| get long 8T p50 | 611 ns | 581 ns | 701 ns | - |

### Inserts

| Benchmark | masstree | treeindex | skipmap | indexset |
|-----------|----------|-----------|---------|----------|
| insert 1T p50 | 110 ns | 220 ns | 190 ns | 571 ns |
| insert 1T p99.9 | 601 ns | 1.21 us | 661 ns | 2.93 us |
| insert 8T p50 | 691 ns | 812 ns | 1.09 us | 5.93 us |
| insert 8T p99.9 | 80.3 us | 21.1 us | 5.70 us | 45.8 us |

### Mixed Read/Write (8T)

| Benchmark | masstree | treeindex | skipmap | indexset |
|-----------|----------|-----------|---------|----------|
| 90/10 p50 | 381 ns | 601 ns | 681 ns | 621 ns |
| 90/10 p99.9 | 1.04 us | 4.18 us | 5.95 us | 1.39 us |
| 50/50 p50 | 421 ns | 1.01 us | 1.09 us | 691 ns |
| 50/50 p99.9 | 1.04 us | 6.66 us | 10.3 us | 1.57 us |

### Range Scans (8T, 50-key scan)

| Benchmark | masstree | treeindex | skipmap |
|-----------|----------|-----------|---------|
| scan p50 | 972 ns | 862 ns | 3.90 us |
| scan+write p50 | 902 ns | 1.02 us | 3.97 us |
| scan+write p99.9 | 2.50 us | 6.66 us | 9.99 us |

## Install

```toml
[dependencies]
masstree = { version = "0.9.5", features = ["mimalloc"] }
```

MSRV is Rust 1.92+ (Edition 2024).

The `mimalloc` feature sets the global allocator. If your project already uses a custom allocator, omit this feature.

## Quick Start

```rust
use masstree::{MassTree, RangeBound};

let tree: MassTree<u64> = MassTree::new();
let guard = tree.guard();

// Insert (returns None for new keys, Some(old) for existing)
tree.insert_with_guard(b"hello", 123, &guard);
tree.insert_with_guard(b"world", 456, &guard);

// Point lookup (returns copy for inline storage)
assert_eq!(tree.get_with_guard(b"hello", &guard), Some(123));

// Remove
tree.remove_with_guard(b"hello", &guard).unwrap();
assert_eq!(tree.get_with_guard(b"hello", &guard), None);

// Range scan
tree.scan(
    RangeBound::Included(b"a"),
    RangeBound::Excluded(b"z"),
    |key, value| {
        println!("{:?} -> {}", key, value);
        true // continue scanning
    },
    &guard,
);

// Prefix scan
tree.scan_prefix(b"wor", |key, value| {
    println!("{:?} -> {}", key, value);
    true
}, &guard);
```

## Ergonomic APIs

For simpler use cases, auto-guard versions create guards internally:

```rust
use masstree::MassTree;

let tree: MassTree<u64> = MassTree::new();

// Auto-guard versions (simpler but slightly more overhead per call)
tree.insert(b"key1", 100);
tree.insert(b"key2", 200);

assert_eq!(tree.get(b"key1"), Some(100));
assert_eq!(tree.len(), 2);
assert!(!tree.is_empty());

tree.remove(b"key1").unwrap();
```

### Range Iteration

```rust
use masstree::{MassTree, RangeBound};

let tree: MassTree<u64> = MassTree::new();
let guard = tree.guard();

// Populate
for i in 0..100u64 {
    tree.insert_with_guard(&i.to_be_bytes(), i, &guard);
}

// Iterator-based range scan
for entry in tree.range(RangeBound::Included(b""), RangeBound::Unbounded, &guard) {
    println!("{:?} -> {:?}", entry.key(), entry.value());
}

// Full iteration
for entry in tree.iter(&guard) {
    println!("{:?}", entry.key());
}
```

## License

MIT. See `LICENSE`.

## References

- [Masstree Paper (EuroSys 2012)](https://pdos.csail.mit.edu/papers/masstree:eurosys12.pdf)
- [C++ Reference Implementation](https://github.com/kohler/masstree-beta)
