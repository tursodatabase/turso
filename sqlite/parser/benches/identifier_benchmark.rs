#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, Criterion};
#[cfg(not(feature = "codspeed"))]
use pprof::criterion::{Output, PProfProfiler};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{black_box, criterion_group, criterion_main, Criterion};

use rustc_hash::FxHashMap;
use turso_parser::identifier::Identifier;

const SHORT: (&str, &str) = ("created_at", "CREATED_AT");
const LONG: (&str, &str) = ("sqlite_autoindex_users_1", "SQLITE_AUTOINDEX_USERS_1");

#[turso_macros::codspeed_criterion_benchmark]
#[allow(clippy::manual_ignore_case_cmp)]
fn bench_identifier_eq(criterion: &mut Criterion) {
    for (label, (lhs, rhs)) in [("short", SHORT), ("long", LONG)] {
        let mut group = criterion.benchmark_group(format!("identifier_eq_{label}"));
        let (id_lhs, id_rhs) = (Identifier::from(lhs), Identifier::from(rhs));
        group.bench_function("identifier", |b| {
            b.iter(|| black_box(&id_lhs) == black_box(&id_rhs));
        });
        group.bench_function("str_eq_ignore_ascii_case", |b| {
            b.iter(|| black_box(lhs).eq_ignore_ascii_case(black_box(rhs)));
        });
        group.bench_function("to_ascii_lowercase", |b| {
            b.iter(|| black_box(lhs).to_ascii_lowercase() == black_box(rhs).to_ascii_lowercase());
        });
        group.finish();
    }
}

#[turso_macros::codspeed_criterion_benchmark]
#[allow(clippy::manual_ignore_case_cmp)]
fn bench_column_lookup(criterion: &mut Criterion) {
    let names: Vec<String> = (0..31)
        .map(|i| format!("column_{i}"))
        .chain(std::iter::once("placed_at".to_string()))
        .collect();
    let needle = "PLACED_AT";
    let mut group = criterion.benchmark_group("column_lookup_32");

    let ids: Vec<Identifier> = names.iter().map(Identifier::from).collect();
    let needle_id = Identifier::from(needle);
    group.bench_function("identifier", |b| {
        b.iter(|| {
            let (ids, needle) = (black_box(&ids), black_box(&needle_id));
            ids.iter().position(|c| c == needle)
        });
    });
    group.bench_function("str_eq_ignore_ascii_case", |b| {
        b.iter(|| {
            let (names, needle) = (black_box(&names), black_box(needle));
            names.iter().position(|c| c.eq_ignore_ascii_case(needle))
        });
    });
    group.bench_function("to_ascii_lowercase", |b| {
        b.iter(|| {
            let (names, needle) = (black_box(&names), black_box(needle));
            let wanted = needle.to_ascii_lowercase();
            names.iter().position(|c| c.to_ascii_lowercase() == wanted)
        });
    });
    group.finish();
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_table_map_lookup(criterion: &mut Criterion) {
    let names: Vec<String> = (0..49)
        .map(|i| format!("table_{i}"))
        .chain(std::iter::once("orders".to_string()))
        .collect();
    let needle = "Orders";
    let mut group = criterion.benchmark_group("table_map_lookup_50");

    let id_map: FxHashMap<Identifier, usize> = names
        .iter()
        .enumerate()
        .map(|(i, n)| (Identifier::from(n), i))
        .collect();
    let needle_id = Identifier::from(needle);
    group.bench_function("identifier", |b| {
        b.iter(|| black_box(&id_map).get(black_box(&needle_id)));
    });
    let str_map: FxHashMap<String, usize> = names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.clone(), i))
        .collect();
    group.bench_function("string_to_ascii_lowercase", |b| {
        b.iter(|| black_box(&str_map).get(&black_box(needle).to_ascii_lowercase()));
    });
    group.finish();
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_identifier_eq, bench_column_lookup, bench_table_map_lookup
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_identifier_eq, bench_column_lookup, bench_table_map_lookup
}

criterion_main!(benches);
