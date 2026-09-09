use std::sync::Arc;

use common::OwnedBytes;
use common::file_slice::FileSlice;
use criterion::{Criterion, criterion_group, criterion_main};
use tantivy_sstable::{Dictionary, MonotonicU64SSTable};

fn make_test_sstable(suffix: &str) -> FileSlice {
    let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();

    // 125 mio elements
    for elem in 0..125_000_000 {
        let key = format!("prefix.{elem:07X}{suffix}").into_bytes();
        builder.insert(&key, &elem).unwrap();
    }

    let table = builder.finish().unwrap();
    let table = Arc::new(OwnedBytes::new(table));
    common::file_slice::FileSlice::new(table.clone())
}

pub fn criterion_benchmark(c: &mut Criterion) {
    {
        let slice = make_test_sstable(".suffix");
        let dict = Dictionary::<MonotonicU64SSTable>::open(slice.clone()).unwrap();
        c.bench_function("ord_to_term_suffix", |b| {
            let mut res = Vec::new();
            b.iter(|| {
                assert!(dict.ord_to_term(100_000, &mut res).unwrap());
                assert!(dict.ord_to_term(19_000_000, &mut res).unwrap());
            })
        });
        c.bench_function("open_and_ord_to_term_suffix", |b| {
            let mut res = Vec::new();
            b.iter(|| {
                let dict = Dictionary::<MonotonicU64SSTable>::open(slice.clone()).unwrap();
                assert!(dict.ord_to_term(100_000, &mut res).unwrap());
                assert!(dict.ord_to_term(19_000_000, &mut res).unwrap());
            })
        });
        c.bench_function("term_ord_suffix", |b| {
            b.iter(|| {
                assert_eq!(
                    dict.term_ord(b"prefix.00186A0.suffix").unwrap().unwrap(),
                    100_000
                );
                assert_eq!(
                    dict.term_ord(b"prefix.121EAC0.suffix").unwrap().unwrap(),
                    19_000_000
                );
            })
        });
        c.bench_function("open_and_term_ord_suffix", |b| {
            b.iter(|| {
                let dict = Dictionary::<MonotonicU64SSTable>::open(slice.clone()).unwrap();
                assert_eq!(
                    dict.term_ord(b"prefix.00186A0.suffix").unwrap().unwrap(),
                    100_000
                );
                assert_eq!(
                    dict.term_ord(b"prefix.121EAC0.suffix").unwrap().unwrap(),
                    19_000_000
                );
            })
        });
    }
    {
        let slice = make_test_sstable("");
        let dict = Dictionary::<MonotonicU64SSTable>::open(slice.clone()).unwrap();
        c.bench_function("ord_to_term", |b| {
            let mut res = Vec::new();
            b.iter(|| {
                assert!(dict.ord_to_term(100_000, &mut res).unwrap());
                assert!(dict.ord_to_term(19_000_000, &mut res).unwrap());
            })
        });
        c.bench_function("open_and_ord_to_term", |b| {
            let mut res = Vec::new();
            b.iter(|| {
                let dict = Dictionary::<MonotonicU64SSTable>::open(slice.clone()).unwrap();
                assert!(dict.ord_to_term(100_000, &mut res).unwrap());
                assert!(dict.ord_to_term(19_000_000, &mut res).unwrap());
            })
        });
        c.bench_function("term_ord", |b| {
            b.iter(|| {
                assert_eq!(dict.term_ord(b"prefix.00186A0").unwrap().unwrap(), 100_000);
                assert_eq!(
                    dict.term_ord(b"prefix.121EAC0").unwrap().unwrap(),
                    19_000_000
                );
            })
        });
        c.bench_function("open_and_term_ord", |b| {
            b.iter(|| {
                let dict = Dictionary::<MonotonicU64SSTable>::open(slice.clone()).unwrap();
                assert_eq!(dict.term_ord(b"prefix.00186A0").unwrap().unwrap(), 100_000);
                assert_eq!(
                    dict.term_ord(b"prefix.121EAC0").unwrap().unwrap(),
                    19_000_000
                );
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
