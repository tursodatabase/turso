use std::ops::Bound;

use binggan::{black_box, BenchGroup, BenchRunner};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tantivy::collector::{Count, DocSetCollector, TopDocs};
use tantivy::query::RangeQuery;
use tantivy::schema::{Schema, FAST, INDEXED};
use tantivy::{doc, Index, Order, ReloadPolicy, Searcher, Term};

#[derive(Clone)]
struct BenchIndex {
    #[allow(dead_code)]
    index: Index,
    searcher: Searcher,
}

fn build_shared_indices(num_docs: usize, distribution: &str) -> BenchIndex {
    // Schema with fast fields only
    let mut schema_builder = Schema::builder();
    let f_num_rand_fast = schema_builder.add_u64_field("num_rand_fast", INDEXED | FAST);
    let f_num_asc_fast = schema_builder.add_u64_field("num_asc_fast", INDEXED | FAST);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());

    // Populate index with stable RNG for reproducibility.
    let mut rng = StdRng::from_seed([7u8; 32]);

    {
        let mut writer = index.writer_with_num_threads(1, 4_000_000_000).unwrap();

        match distribution {
            "dense" => {
                for doc_id in 0..num_docs {
                    let num_rand = rng.random_range(0u64..1000u64);
                    let num_asc = (doc_id / 10000) as u64;

                    writer
                        .add_document(doc!(
                            f_num_rand_fast=>num_rand,
                            f_num_asc_fast=>num_asc,
                        ))
                        .unwrap();
                }
            }
            "sparse" => {
                for doc_id in 0..num_docs {
                    let num_rand = rng.random_range(0u64..10000000u64);
                    let num_asc = doc_id as u64;

                    writer
                        .add_document(doc!(
                            f_num_rand_fast=>num_rand,
                            f_num_asc_fast=>num_asc,
                        ))
                        .unwrap();
                }
            }
            _ => {
                panic!("Unsupported distribution type");
            }
        }
        writer.commit().unwrap();
    }

    // Prepare reader/searcher once.
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    let searcher = reader.searcher();

    BenchIndex { index, searcher }
}

fn main() {
    // Prepare corpora with varying scenarios
    let scenarios = vec![
        // Dense distribution - random values in small range (0-999)
        (
            "dense_values_search_low_value_range".to_string(),
            10_000_000,
            "dense",
            0,
            9,
        ),
        (
            "dense_values_search_high_value_range".to_string(),
            10_000_000,
            "dense",
            990,
            999,
        ),
        (
            "dense_values_search_out_of_range".to_string(),
            10_000_000,
            "dense",
            1000,
            1002,
        ),
        (
            "sparse_values_search_low_value_range".to_string(),
            10_000_000,
            "sparse",
            0,
            9,
        ),
        (
            "sparse_values_search_high_value_range".to_string(),
            10_000_000,
            "sparse",
            9_999_990,
            9_999_999,
        ),
        (
            "sparse_values_search_out_of_range".to_string(),
            10_000_000,
            "sparse",
            10_000_000,
            10_000_002,
        ),
    ];

    let mut runner = BenchRunner::new();
    for (scenario_id, n, num_rand_distribution, range_low, range_high) in scenarios {
        // Build index for this scenario
        let bench_index = build_shared_indices(n, num_rand_distribution);

        // Create benchmark group
        let mut group = runner.new_group();

        // Now set the name (this moves scenario_id)
        group.set_name(scenario_id);

        // Define fast field types
        let field_names = ["num_rand_fast", "num_asc_fast"];

        // Generate range queries for fast fields
        for &field_name in &field_names {
            // Create the range query
            let field = bench_index.searcher.schema().get_field(field_name).unwrap();
            let lower_term = Term::from_field_u64(field, range_low);
            let upper_term = Term::from_field_u64(field, range_high);

            let query = RangeQuery::new(Bound::Included(lower_term), Bound::Included(upper_term));

            run_benchmark_tasks(
                &mut group,
                &bench_index,
                query,
                field_name,
                range_low,
                range_high,
            );
        }

        group.run();
    }
}

/// Run all benchmark tasks for a given range query and field name
fn run_benchmark_tasks(
    bench_group: &mut BenchGroup,
    bench_index: &BenchIndex,
    query: RangeQuery,
    field_name: &str,
    range_low: u64,
    range_high: u64,
) {
    // Test count
    add_bench_task_count(
        bench_group,
        bench_index,
        query.clone(),
        "count",
        field_name,
        range_low,
        range_high,
    );

    // Test top 100 by the field (ascending order)
    {
        let collector_name = format!("top100_by_{}_asc", field_name);
        let field_name_owned = field_name.to_string();
        add_bench_task_top100_asc(
            bench_group,
            bench_index,
            query.clone(),
            &collector_name,
            field_name,
            range_low,
            range_high,
            field_name_owned,
        );
    }

    // Test top 100 by the field (descending order)
    {
        let collector_name = format!("top100_by_{}_desc", field_name);
        let field_name_owned = field_name.to_string();
        add_bench_task_top100_desc(
            bench_group,
            bench_index,
            query,
            &collector_name,
            field_name,
            range_low,
            range_high,
            field_name_owned,
        );
    }
}

fn add_bench_task_count(
    bench_group: &mut BenchGroup,
    bench_index: &BenchIndex,
    query: RangeQuery,
    collector_name: &str,
    field_name: &str,
    range_low: u64,
    range_high: u64,
) {
    let task_name = format!(
        "range_{}_[{} TO {}]_{}",
        field_name, range_low, range_high, collector_name
    );

    let search_task = CountSearchTask {
        searcher: bench_index.searcher.clone(),
        query,
    };
    bench_group.register(task_name, move |_| black_box(search_task.run()));
}

fn add_bench_task_docset(
    bench_group: &mut BenchGroup,
    bench_index: &BenchIndex,
    query: RangeQuery,
    collector_name: &str,
    field_name: &str,
    range_low: u64,
    range_high: u64,
) {
    let task_name = format!(
        "range_{}_[{} TO {}]_{}",
        field_name, range_low, range_high, collector_name
    );

    let search_task = DocSetSearchTask {
        searcher: bench_index.searcher.clone(),
        query,
    };
    bench_group.register(task_name, move |_| black_box(search_task.run()));
}

fn add_bench_task_top100_asc(
    bench_group: &mut BenchGroup,
    bench_index: &BenchIndex,
    query: RangeQuery,
    collector_name: &str,
    field_name: &str,
    range_low: u64,
    range_high: u64,
    field_name_owned: String,
) {
    let task_name = format!(
        "range_{}_[{} TO {}]_{}",
        field_name, range_low, range_high, collector_name
    );

    let search_task = Top100AscSearchTask {
        searcher: bench_index.searcher.clone(),
        query,
        field_name: field_name_owned,
    };
    bench_group.register(task_name, move |_| black_box(search_task.run()));
}

fn add_bench_task_top100_desc(
    bench_group: &mut BenchGroup,
    bench_index: &BenchIndex,
    query: RangeQuery,
    collector_name: &str,
    field_name: &str,
    range_low: u64,
    range_high: u64,
    field_name_owned: String,
) {
    let task_name = format!(
        "range_{}_[{} TO {}]_{}",
        field_name, range_low, range_high, collector_name
    );

    let search_task = Top100DescSearchTask {
        searcher: bench_index.searcher.clone(),
        query,
        field_name: field_name_owned,
    };
    bench_group.register(task_name, move |_| black_box(search_task.run()));
}

struct CountSearchTask {
    searcher: Searcher,
    query: RangeQuery,
}

impl CountSearchTask {
    #[inline(never)]
    pub fn run(&self) -> usize {
        self.searcher.search(&self.query, &Count).unwrap()
    }
}

struct DocSetSearchTask {
    searcher: Searcher,
    query: RangeQuery,
}

impl DocSetSearchTask {
    #[inline(never)]
    pub fn run(&self) -> usize {
        let result = self.searcher.search(&self.query, &DocSetCollector).unwrap();
        result.len()
    }
}

struct Top100AscSearchTask {
    searcher: Searcher,
    query: RangeQuery,
    field_name: String,
}

impl Top100AscSearchTask {
    #[inline(never)]
    pub fn run(&self) -> usize {
        let collector =
            TopDocs::with_limit(100).order_by_fast_field::<u64>(&self.field_name, Order::Asc);
        let result = self.searcher.search(&self.query, &collector).unwrap();
        for (_score, doc_address) in &result {
            let _doc: tantivy::TantivyDocument = self.searcher.doc(*doc_address).unwrap();
        }
        result.len()
    }
}

struct Top100DescSearchTask {
    searcher: Searcher,
    query: RangeQuery,
    field_name: String,
}

impl Top100DescSearchTask {
    #[inline(never)]
    pub fn run(&self) -> usize {
        let collector =
            TopDocs::with_limit(100).order_by_fast_field::<u64>(&self.field_name, Order::Desc);
        let result = self.searcher.search(&self.query, &collector).unwrap();
        for (_score, doc_address) in &result {
            let _doc: tantivy::TantivyDocument = self.searcher.doc(*doc_address).unwrap();
        }
        result.len()
    }
}
