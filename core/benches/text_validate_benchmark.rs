#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, Criterion, Throughput,
};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

use asserting::prelude::*;
use std::time::Duration;

const CALLS_PER_ITERATION: usize = 10_000;
const CALLGRIND_SIZES: &[usize] = &[
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257,
    511, 512, 513, 1024,
];

struct Fixture {
    bytes: Vec<u8>,
    offset: usize,
}

struct Case {
    name: String,
    fixtures: Vec<Fixture>,
}

#[derive(Clone, Copy)]
enum Content {
    Ascii,
    UnicodeFirst,
    UnicodeLast,
    UnicodeDense,
    InvalidFirst,
    InvalidLast,
    Truncated,
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_text_validate(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("text_validate");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.throughput(Throughput::Elements(CALLS_PER_ITERATION as u64));

    for case in cases() {
        assert_that!(case.fixtures.len().is_power_of_two()).is_true();
        for fixture in &case.fixtures {
            let data = &fixture.bytes[fixture.offset..];
            let expected = std::str::from_utf8(data).is_ok();
            assert_that!(read_production(data))
                .described_as(case.name.as_str())
                .is_equal_to(expected);
        }
        group.bench_function(format!("{}/production", case.name), |b| {
            b.iter(|| text_validate_batch(&case.fixtures));
        });
    }
    group.finish();
}

fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    for len in [1, 16, 64, 512, 4096] {
        cases.push(fixed_case("ascii", Content::Ascii, len));
    }
    cases.push(fixed_case("unicode_first", Content::UnicodeFirst, 64));
    cases.push(fixed_case("unicode_last", Content::UnicodeLast, 64));
    cases.push(fixed_case("unicode_dense", Content::UnicodeDense, 64));
    cases.push(fixed_case("invalid_first", Content::InvalidFirst, 64));
    cases.push(fixed_case("invalid_last", Content::InvalidLast, 64));
    cases.push(fixed_case("truncated", Content::Truncated, 64));
    cases.push(mixed_case("ascii", 0));
    cases.push(mixed_case("unicode", 1));
    cases.push(mixed_case("ascii_unicode", 5));
    cases
}

fn fixed_case(name: &str, content: Content, len: usize) -> Case {
    Case {
        name: format!("fixed/{name}/{len}"),
        fixtures: (0..8).map(|offset| fixture(content, len, offset)).collect(),
    }
}

fn mixed_case(name: &str, unicode_every: usize) -> Case {
    let fixtures = (0..256)
        .map(|i| {
            let hash = (i as u64).wrapping_mul(2_654_435_761) ^ ((i as u64) >> 2);
            let len = CALLGRIND_SIZES[(hash % CALLGRIND_SIZES.len() as u64) as usize];
            let content = if len >= 2 && unicode_every != 0 && i % unicode_every == 0 {
                Content::UnicodeFirst
            } else {
                Content::Ascii
            };
            fixture(content, len, i % 8)
        })
        .collect();
    Case {
        name: format!("mixed/{name}"),
        fixtures,
    }
}

fn fixture(content: Content, len: usize, offset: usize) -> Fixture {
    let mut bytes = vec![b'a'; len + offset];
    let data = &mut bytes[offset..];
    match content {
        Content::Ascii => {}
        Content::UnicodeFirst => data[..2].copy_from_slice("é".as_bytes()),
        Content::UnicodeLast => data[len - 2..].copy_from_slice("é".as_bytes()),
        Content::UnicodeDense => {
            for pair in data.chunks_exact_mut(2) {
                pair.copy_from_slice("é".as_bytes());
            }
        }
        Content::InvalidFirst => data[0] = 0xff,
        Content::InvalidLast => data[len - 1] = 0xff,
        Content::Truncated => data[len - 1] = 0xc3,
    }
    Fixture { bytes, offset }
}

#[inline(never)]
#[no_mangle]
fn text_validate_batch(fixtures: &[Fixture]) {
    let mask = fixtures.len() - 1;
    for i in 0..CALLS_PER_ITERATION {
        let fixture = &fixtures[i & mask];
        black_box(read_production(black_box(&fixture.bytes[fixture.offset..])));
    }
}

#[inline(never)]
fn read_production(data: &[u8]) -> bool {
    turso_core::storage::sqlite3_ondisk::read_text(data).is_ok()
}

criterion_group!(benches, bench_text_validate);
criterion_main!(benches);
