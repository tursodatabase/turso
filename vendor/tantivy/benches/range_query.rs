use std::fmt::Display;
use std::net::Ipv6Addr;
use std::ops::RangeInclusive;

use binggan::plugins::PeakMemAllocPlugin;
use binggan::{black_box, BenchRunner, OutputValue, PeakMemAlloc, INSTRUMENTED_SYSTEM};
use columnar::MonotonicallyMappableToU128;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy::collector::{Count, TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{doc, Index};

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

fn main() {
    bench_range_query();
}

fn bench_range_query() {
    let index = get_index_0_to_100();
    let mut runner = BenchRunner::new();
    runner.add_plugin(PeakMemAllocPlugin::new(GLOBAL));

    runner.set_name("range_query on u64");
    let field_name_and_descr: Vec<_> = vec![
        ("id", "Single Valued Range Field"),
        ("ids", "Multi Valued Range Field"),
    ];
    let range_num_hits = vec![
        ("90_percent", get_90_percent()),
        ("10_percent", get_10_percent()),
        ("1_percent", get_1_percent()),
    ];

    test_range(&mut runner, &index, &field_name_and_descr, range_num_hits);

    runner.set_name("range_query on ip");
    let field_name_and_descr: Vec<_> = vec![
        ("ip", "Single Valued Range Field"),
        ("ips", "Multi Valued Range Field"),
    ];
    let range_num_hits = vec![
        ("90_percent", get_90_percent_ip()),
        ("10_percent", get_10_percent_ip()),
        ("1_percent", get_1_percent_ip()),
    ];

    test_range(&mut runner, &index, &field_name_and_descr, range_num_hits);
}

fn test_range<T: Display>(
    runner: &mut BenchRunner,
    index: &Index,
    field_name_and_descr: &[(&str, &str)],
    range_num_hits: Vec<(&str, RangeInclusive<T>)>,
) {
    for (field, suffix) in field_name_and_descr {
        let term_num_hits = vec![
            ("", ""),
            ("1_percent", "veryfew"),
            ("10_percent", "few"),
            ("90_percent", "most"),
        ];
        let mut group = runner.new_group();
        group.set_name(suffix);
        // all intersect combinations
        for (range_name, range) in &range_num_hits {
            for (term_name, term) in &term_num_hits {
                let index = &index;
                let test_name = if term_name.is_empty() {
                    format!("id_range_hit_{}", range_name)
                } else {
                    format!(
                        "id_range_hit_{}_intersect_with_term_{}",
                        range_name, term_name
                    )
                };
                group.register(test_name, move |_| {
                    let query = if term_name.is_empty() {
                        "".to_string()
                    } else {
                        format!("AND id_name:{}", term)
                    };
                    black_box(execute_query(field, range, &query, index));
                });
            }
        }
        group.run();
    }
}

fn get_index_0_to_100() -> Index {
    let mut rng = StdRng::from_seed([1u8; 32]);
    let num_vals = 100_000;
    let docs: Vec<_> = (0..num_vals)
        .map(|_i| {
            let id_name = if rng.random_bool(0.01) {
                "veryfew".to_string() // 1%
            } else if rng.random_bool(0.1) {
                "few".to_string() // 9%
            } else {
                "most".to_string() // 90%
            };
            Doc {
                id_name,
                id: rng.random_range(0..100),
                // Multiply by 1000, so that we create most buckets in the compact space
                // The benches depend on this range to select n-percent of elements with the
                // methods below.
                ip: Ipv6Addr::from_u128(rng.random_range(0..100) * 1000),
            }
        })
        .collect();

    create_index_from_docs(&docs)
}

#[derive(Clone, Debug)]
pub struct Doc {
    pub id_name: String,
    pub id: u64,
    pub ip: Ipv6Addr,
}

pub fn create_index_from_docs(docs: &[Doc]) -> Index {
    let mut schema_builder = Schema::builder();
    let id_u64_field = schema_builder.add_u64_field("id", INDEXED | STORED | FAST);
    let ids_u64_field =
        schema_builder.add_u64_field("ids", NumericOptions::default().set_fast().set_indexed());

    let id_f64_field = schema_builder.add_f64_field("id_f64", INDEXED | STORED | FAST);
    let ids_f64_field = schema_builder.add_f64_field(
        "ids_f64",
        NumericOptions::default().set_fast().set_indexed(),
    );

    let id_i64_field = schema_builder.add_i64_field("id_i64", INDEXED | STORED | FAST);
    let ids_i64_field = schema_builder.add_i64_field(
        "ids_i64",
        NumericOptions::default().set_fast().set_indexed(),
    );

    let text_field = schema_builder.add_text_field("id_name", STRING | STORED);
    let text_field2 = schema_builder.add_text_field("id_name_fast", STRING | STORED | FAST);

    let ip_field = schema_builder.add_ip_addr_field("ip", FAST);
    let ips_field = schema_builder.add_ip_addr_field("ips", FAST);

    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema);

    {
        let mut index_writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
        for doc in docs.iter() {
            index_writer
                .add_document(doc!(
                    ids_i64_field => doc.id as i64,
                    ids_i64_field => doc.id as i64,
                    ids_f64_field => doc.id as f64,
                    ids_f64_field => doc.id as f64,
                    ids_u64_field => doc.id,
                    ids_u64_field => doc.id,
                    id_u64_field => doc.id,
                    id_f64_field => doc.id as f64,
                    id_i64_field => doc.id as i64,
                    text_field => doc.id_name.to_string(),
                    text_field2 => doc.id_name.to_string(),
                    ips_field => doc.ip,
                    ips_field => doc.ip,
                    ip_field => doc.ip,
                ))
                .unwrap();
        }

        index_writer.commit().unwrap();
    }
    index
}

fn get_90_percent() -> RangeInclusive<u64> {
    0..=90
}

fn get_10_percent() -> RangeInclusive<u64> {
    0..=10
}

fn get_1_percent() -> RangeInclusive<u64> {
    10..=10
}

fn get_90_percent_ip() -> RangeInclusive<Ipv6Addr> {
    let start = Ipv6Addr::from_u128(0);
    let end = Ipv6Addr::from_u128(90 * 1000);
    start..=end
}

fn get_10_percent_ip() -> RangeInclusive<Ipv6Addr> {
    let start = Ipv6Addr::from_u128(0);
    let end = Ipv6Addr::from_u128(10 * 1000);
    start..=end
}

fn get_1_percent_ip() -> RangeInclusive<Ipv6Addr> {
    let start = Ipv6Addr::from_u128(10 * 1000);
    let end = Ipv6Addr::from_u128(10 * 1000);
    start..=end
}

struct NumHits {
    count: usize,
}
impl OutputValue for NumHits {
    fn column_title() -> &'static str {
        "NumHits"
    }
    fn format(&self) -> Option<String> {
        Some(self.count.to_string())
    }
}

fn execute_query<T: Display>(
    field: &str,
    id_range: &RangeInclusive<T>,
    suffix: &str,
    index: &Index,
) -> NumHits {
    let gen_query_inclusive = |from: &T, to: &T| {
        format!(
            "{}:[{} TO {}] {}",
            field,
            &from.to_string(),
            &to.to_string(),
            suffix
        )
    };

    let query = gen_query_inclusive(id_range.start(), id_range.end());
    execute_query_(&query, index)
}

fn execute_query_(query: &str, index: &Index) -> NumHits {
    let query_from_text = |text: &str| {
        QueryParser::for_index(index, vec![])
            .parse_query(text)
            .unwrap()
    };
    let query = query_from_text(query);
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    let num_hits = searcher
        .search(&query, &(TopDocs::with_limit(10).order_by_score(), Count))
        .unwrap()
        .1;
    NumHits { count: num_hits }
}
