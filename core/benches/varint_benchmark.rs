//! Microbenchmarks for SQLite-format varint decoding.
//!
//! Varints show up in three hot shapes in the engine, and each gets a benchmark:
//!  - single-varint decode at each encoded length (predictable branch pattern)
//!  - a shuffled mixed-length stream (unpredictable lengths, like parsing many
//!    b-tree cells where payload sizes and rowids vary)
//!  - record-header runs: decoding every serial type of a record header, and the
//!    Column-opcode pattern of skipping k serial types to reach one column
//!
//! Run: cargo bench -p turso_core --bench varint_benchmark

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use turso_core::storage::sqlite3_ondisk::{read_varint, write_varint};
use turso_core::types::ValueIterator;

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Deterministic RNG (xorshift64*) so every run benches identical byte streams.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    /// Random value in [0, n).
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Smallest and largest value for each encoded varint length 1..=9.
fn value_range_for_len(len: usize) -> (u64, u64) {
    match len {
        1 => (0, 0x7f),
        2..=8 => (1u64 << (7 * (len - 1)), (1u64 << (7 * len)) - 1),
        9 => (1u64 << 56, u64::MAX),
        _ => unreachable!(),
    }
}

/// A byte stream of `count` varints plus the values they encode.
struct VarintStream {
    bytes: Vec<u8>,
    values: Vec<u64>,
}

fn build_stream(count: usize, mut pick_value: impl FnMut() -> u64) -> VarintStream {
    let mut bytes = Vec::new();
    let mut values = Vec::with_capacity(count);
    let mut buf = [0u8; 9];
    for _ in 0..count {
        let v = pick_value();
        let n = write_varint(&mut buf, v);
        bytes.extend_from_slice(&buf[..n]);
        values.push(v);
    }
    // Padding so decoding the final varint still sees a full-size read window,
    // like a varint in the middle of a page. Zero bytes never extend a varint.
    bytes.extend_from_slice(&[0u8; 16]);
    VarintStream { bytes, values }
}

/// Decode the whole stream front to back, folding the values so the decode
/// cannot be optimized away. Panics on any decode error: benchmarked streams
/// are always valid.
fn decode_stream(bytes: &[u8], count: usize) -> u64 {
    let mut pos = 0;
    let mut acc = 0u64;
    for _ in 0..count {
        let (v, n) = read_varint(&bytes[pos..]).unwrap();
        acc = acc.wrapping_add(v);
        pos += n;
    }
    acc
}

const STREAM_COUNT: usize = 8_192;

#[turso_macros::codspeed_criterion_benchmark]
fn bench_varint_single_lengths(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("varint_decode_fixed_len");
    for len in 1..=9usize {
        let (lo, hi) = value_range_for_len(len);
        let mut rng = Rng(0x9E3779B97F4A7C15);
        let stream = build_stream(STREAM_COUNT, || lo + rng.below(hi - lo + 1));
        let expected: u64 = stream.values.iter().fold(0u64, |a, &v| a.wrapping_add(v));
        group.throughput(Throughput::Elements(STREAM_COUNT as u64));
        group.bench_with_input(BenchmarkId::new("len", len), &stream, |b, s| {
            b.iter(|| {
                let acc = decode_stream(black_box(&s.bytes), STREAM_COUNT);
                assert_eq!(acc, expected);
                acc
            })
        });
    }
    group.finish();
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_varint_mixed_stream(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("varint_decode_mixed");

    // Length mix modeled on b-tree traffic: mostly tiny values (serial types,
    // small rowids), a solid share of 2-byte (payload sizes, medium rowids),
    // and a tail of larger ones. Order is shuffled, so the length of each
    // varint is unpredictable to the branch predictor.
    let mut rng = Rng(0xDEADBEEFCAFED00D);
    let stream = build_stream(STREAM_COUNT, || {
        let (lo, hi) = match rng.below(100) {
            0..=59 => value_range_for_len(1),
            60..=84 => value_range_for_len(2),
            85..=94 => value_range_for_len(3),
            95..=98 => value_range_for_len(5),
            _ => value_range_for_len(9),
        };
        lo + rng.below((hi - lo).wrapping_add(1).max(1))
    });
    let expected: u64 = stream.values.iter().fold(0u64, |a, &v| a.wrapping_add(v));

    group.throughput(Throughput::Elements(STREAM_COUNT as u64));
    group.bench_function("shuffled_lengths", |b| {
        b.iter(|| {
            let acc = decode_stream(black_box(&stream.bytes), STREAM_COUNT);
            assert_eq!(acc, expected);
            acc
        })
    });
    group.finish();
}

/// Build one record payload: header-size varint, serial-type varints, then data
/// bytes sized to match. Values are filler; the interesting part is the header.
fn build_record(serial_types: &[u64]) -> Vec<u8> {
    fn serial_type_data_size(st: u64) -> usize {
        match st {
            0 | 8 | 9 => 0,
            1 => 1,
            2 => 2,
            3 => 3,
            4 => 4,
            5 => 6,
            6 | 7 => 8,
            n if n >= 12 => ((n - 12) / 2) as usize,
            _ => unreachable!("reserved serial type"),
        }
    }

    let mut buf = [0u8; 9];
    let mut header_body = Vec::new();
    for &st in serial_types {
        let n = write_varint(&mut buf, st);
        header_body.extend_from_slice(&buf[..n]);
    }
    // Header size includes its own varint. One byte is always enough here:
    // benchmark headers stay far below 128 bytes.
    let header_size = header_body.len() + 1;
    assert!(header_size <= 0x7f);

    let mut payload = Vec::new();
    payload.push(header_size as u8);
    payload.extend_from_slice(&header_body);
    for &st in serial_types {
        let size = serial_type_data_size(st);
        // 0x41 = 'A': valid UTF-8 so text serial types decode cleanly.
        payload.extend(std::iter::repeat_n(0x41u8, size));
    }
    payload
}

/// Serial types for text of the given byte length.
fn text_st(len: usize) -> u64 {
    (len as u64) * 2 + 13
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_record_header_runs(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("varint_decode_header_run");

    // (label, serial types): three realistic table shapes.
    // narrow: small ints and short text, 1-byte serial types only.
    // wide: 32 columns, 1-byte serial types only.
    // text_heavy: large text columns force 2-byte serial-type varints.
    let narrow: Vec<u64> = vec![1, 4, text_st(20), 8];
    let wide: Vec<u64> = (0..32)
        .map(|i| match i % 4 {
            0 => 1,
            1 => 4,
            2 => text_st(24),
            _ => 6,
        })
        .collect();
    let text_heavy: Vec<u64> = (0..8)
        .map(|i| if i % 2 == 0 { text_st(100) } else { 2 })
        .collect();

    for (label, serial_types) in [
        ("narrow_4col", &narrow),
        ("wide_32col", &wide),
        ("text_heavy_8col", &text_heavy),
    ] {
        let payload = build_record(serial_types);
        let cols = serial_types.len();

        // Full iteration: decode every value of the record, like SELECT *.
        group.throughput(Throughput::Elements(cols as u64));
        group.bench_with_input(
            BenchmarkId::new("iterate_all", label),
            &payload,
            |b, payload| {
                b.iter(|| {
                    let iter = ValueIterator::new(black_box(payload.as_slice())).unwrap();
                    let mut n = 0usize;
                    for v in iter {
                        black_box(v.unwrap());
                        n += 1;
                    }
                    assert_eq!(n, cols);
                })
            },
        );

        // Column-opcode pattern: skip to the last column, decode just it.
        // Skipping is pure serial-type varint decoding plus size accumulation.
        group.throughput(Throughput::Elements(cols as u64));
        group.bench_with_input(
            BenchmarkId::new("skip_to_last", label),
            &payload,
            |b, payload| {
                b.iter(|| {
                    let mut iter = ValueIterator::new(black_box(payload.as_slice())).unwrap();
                    let v = iter.nth(cols - 1).unwrap().unwrap();
                    black_box(v);
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_varint_single_lengths,
    bench_varint_mixed_stream,
    bench_record_header_runs
);
criterion_main!(benches);
