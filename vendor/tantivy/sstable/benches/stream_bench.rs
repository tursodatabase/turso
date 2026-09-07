use std::collections::BTreeSet;
use std::io;

use common::file_slice::FileSlice;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy_sstable::{Dictionary, MonotonicU64SSTable};

const CHARSET: &[u8] = b"abcdefghij";

fn generate_key(rng: &mut impl Rng) -> String {
    let len = rng.random_range(3..12);
    std::iter::from_fn(|| {
        let idx = rng.random_range(0..CHARSET.len());
        Some(CHARSET[idx] as char)
    })
    .take(len)
    .collect()
}

fn prepare_sstable() -> io::Result<Dictionary<MonotonicU64SSTable>> {
    let mut rng = StdRng::from_seed([3u8; 32]);
    let mut els = BTreeSet::new();
    while els.len() < 100_000 {
        els.insert(generate_key(&mut rng));
    }
    let mut dictionary_builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new())?;
    for (ord, word) in els.iter().enumerate() {
        dictionary_builder.insert(word, &(ord as u64))?;
    }
    let buffer = dictionary_builder.finish()?;
    let dictionary = Dictionary::open(FileSlice::from(buffer))?;
    Ok(dictionary)
}

fn stream_bench(
    dictionary: &Dictionary<MonotonicU64SSTable>,
    lower: &[u8],
    upper: &[u8],
    do_scan: bool,
) -> usize {
    let mut stream = dictionary
        .range()
        .ge(lower)
        .lt(upper)
        .into_stream()
        .unwrap();
    if !do_scan {
        return 0;
    }
    let mut count = 0;
    while stream.advance() {
        count += 1;
    }
    count
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let dict = prepare_sstable().unwrap();
    c.bench_function("short_scan_init", |b| {
        b.iter(|| stream_bench(&dict, b"fa", b"fana", false))
    });
    c.bench_function("short_scan_init_and_scan", |b| {
        b.iter(|| {
            assert_eq!(stream_bench(&dict, b"fa", b"faz", true), 971);
        })
    });
    c.bench_function("full_scan_init_and_scan_full_with_bound", |b| {
        b.iter(|| {
            assert_eq!(stream_bench(&dict, b"", b"z", true), 100_000);
        })
    });
    c.bench_function("full_scan_init_and_scan_full_no_bounds", |b| {
        b.iter(|| {
            let mut stream = dict.stream().unwrap();
            let mut count = 0;
            while stream.advance() {
                count += 1;
            }
            count
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
