// This benchmark compares different approaches for retrieving string values:
//
// 1. Fast Field Approach: retrieves string values via term_ords() and ord_to_str()
//
// 2. Doc Store Approach: retrieves string values via searcher.doc() and field extraction
//
// The benchmark includes various data distributions:
// - Dense Sequential: Sequential document IDs with dense data
// - Dense Random: Random document IDs with dense data
// - Sparse Sequential: Sequential document IDs with sparse data
// - Sparse Random: Random document IDs with sparse data
use std::ops::Bound;

use binggan::{black_box, BenchGroup, BenchRunner};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tantivy::collector::{Count, DocSetCollector};
use tantivy::query::RangeQuery;
use tantivy::schema::document::TantivyDocument;
use tantivy::schema::{Schema, Value, FAST, STORED, STRING};
use tantivy::{doc, Index, ReloadPolicy, Searcher, Term};

#[derive(Clone)]
struct BenchIndex {
    #[allow(dead_code)]
    index: Index,
    searcher: Searcher,
}

fn build_shared_indices(num_docs: usize, distribution: &str) -> BenchIndex {
    // Schema with string fast field and stored field for doc access
    let mut schema_builder = Schema::builder();
    let f_str_fast = schema_builder.add_text_field("str_fast", STRING | STORED | FAST);
    let f_str_stored = schema_builder.add_text_field("str_stored", STRING | STORED);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());

    // Populate index with stable RNG for reproducibility.
    let mut rng = StdRng::from_seed([7u8; 32]);

    {
        let mut writer = index.writer_with_num_threads(1, 4_000_000_000).unwrap();

        match distribution {
            "dense_random" => {
                for _doc_id in 0..num_docs {
                    let suffix = rng.random_range(0u64..1000u64);
                    let str_val = format!("str_{:03}", suffix);

                    writer
                        .add_document(doc!(
                            f_str_fast=>str_val.clone(),
                            f_str_stored=>str_val,
                        ))
                        .unwrap();
                }
            }
            "dense_sequential" => {
                for doc_id in 0..num_docs {
                    let suffix = doc_id as u64 % 1000;
                    let str_val = format!("str_{:03}", suffix);

                    writer
                        .add_document(doc!(
                            f_str_fast=>str_val.clone(),
                            f_str_stored=>str_val,
                        ))
                        .unwrap();
                }
            }
            "sparse_random" => {
                for _doc_id in 0..num_docs {
                    let suffix = rng.random_range(0u64..1000000u64);
                    let str_val = format!("str_{:07}", suffix);

                    writer
                        .add_document(doc!(
                            f_str_fast=>str_val.clone(),
                            f_str_stored=>str_val,
                        ))
                        .unwrap();
                }
            }
            "sparse_sequential" => {
                for doc_id in 0..num_docs {
                    let suffix = doc_id as u64;
                    let str_val = format!("str_{:07}", suffix);

                    writer
                        .add_document(doc!(
                            f_str_fast=>str_val.clone(),
                            f_str_stored=>str_val,
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
        (
            "dense_random_search_low_range".to_string(),
            1_000_000,
            "dense_random",
            0,
            9,
        ),
        (
            "dense_random_search_high_range".to_string(),
            1_000_000,
            "dense_random",
            990,
            999,
        ),
        (
            "dense_sequential_search_low_range".to_string(),
            1_000_000,
            "dense_sequential",
            0,
            9,
        ),
        (
            "dense_sequential_search_high_range".to_string(),
            1_000_000,
            "dense_sequential",
            990,
            999,
        ),
        (
            "sparse_random_search_low_range".to_string(),
            1_000_000,
            "sparse_random",
            0,
            9999,
        ),
        (
            "sparse_random_search_high_range".to_string(),
            1_000_000,
            "sparse_random",
            990_000,
            999_999,
        ),
        (
            "sparse_sequential_search_low_range".to_string(),
            1_000_000,
            "sparse_sequential",
            0,
            9999,
        ),
        (
            "sparse_sequential_search_high_range".to_string(),
            1_000_000,
            "sparse_sequential",
            990_000,
            999_999,
        ),
    ];

    let mut runner = BenchRunner::new();
    for (scenario_id, n, distribution, range_low, range_high) in scenarios {
        let bench_index = build_shared_indices(n, distribution);
        let mut group = runner.new_group();
        group.set_name(scenario_id);

        let field = bench_index.searcher.schema().get_field("str_fast").unwrap();

        let (lower_str, upper_str) =
            if distribution == "dense_sequential" || distribution == "dense_random" {
                (
                    format!("str_{:03}", range_low),
                    format!("str_{:03}", range_high),
                )
            } else {
                (
                    format!("str_{:07}", range_low),
                    format!("str_{:07}", range_high),
                )
            };

        let lower_term = Term::from_field_text(field, &lower_str);
        let upper_term = Term::from_field_text(field, &upper_str);

        let query = RangeQuery::new(Bound::Included(lower_term), Bound::Included(upper_term));

        run_benchmark_tasks(&mut group, &bench_index, query, range_low, range_high);

        group.run();
    }
}

/// Run all benchmark tasks for a given range query
fn run_benchmark_tasks(
    bench_group: &mut BenchGroup,
    bench_index: &BenchIndex,
    query: RangeQuery,
    range_low: u64,
    range_high: u64,
) {
    // Test count of matching documents
    add_bench_task_count(
        bench_group,
        bench_index,
        query.clone(),
        range_low,
        range_high,
    );

    // Test fetching all DocIds of matching documents
    add_bench_task_docset(
        bench_group,
        bench_index,
        query.clone(),
        range_low,
        range_high,
    );

    // Test fetching all string fast field values of matching documents
    add_bench_task_fetch_all_strings(
        bench_group,
        bench_index,
        query.clone(),
        range_low,
        range_high,
    );

    // Test fetching all string values of matching documents through doc() method
    add_bench_task_fetch_all_strings_from_doc(
        bench_group,
        bench_index,
        query,
        range_low,
        range_high,
    );
}

fn add_bench_task_count(
    bench_group: &mut BenchGroup,
    bench_index: &BenchIndex,
    query: RangeQuery,
    range_low: u64,
    range_high: u64,
) {
    let task_name = format!("string_search_count_[{}-{}]", range_low, range_high);

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
    range_low: u64,
    range_high: u64,
) {
    let task_name = format!("string_fetch_all_docset_[{}-{}]", range_low, range_high);

    let search_task = DocSetSearchTask {
        searcher: bench_index.searcher.clone(),
        query,
    };
    bench_group.register(task_name, move |_| black_box(search_task.run()));
}

fn add_bench_task_fetch_all_strings(
    bench_group: &mut BenchGroup,
    bench_index: &BenchIndex,
    query: RangeQuery,
    range_low: u64,
    range_high: u64,
) {
    let task_name = format!(
        "string_fastfield_fetch_all_strings_[{}-{}]",
        range_low, range_high
    );

    let search_task = FetchAllStringsSearchTask {
        searcher: bench_index.searcher.clone(),
        query,
    };

    bench_group.register(task_name, move |_| {
        let result = black_box(search_task.run());
        result.len()
    });
}

fn add_bench_task_fetch_all_strings_from_doc(
    bench_group: &mut BenchGroup,
    bench_index: &BenchIndex,
    query: RangeQuery,
    range_low: u64,
    range_high: u64,
) {
    let task_name = format!(
        "string_doc_fetch_all_strings_[{}-{}]",
        range_low, range_high
    );

    let search_task = FetchAllStringsFromDocTask {
        searcher: bench_index.searcher.clone(),
        query,
    };

    bench_group.register(task_name, move |_| {
        let result = black_box(search_task.run());
        result.len()
    });
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

struct FetchAllStringsSearchTask {
    searcher: Searcher,
    query: RangeQuery,
}

impl FetchAllStringsSearchTask {
    #[inline(never)]
    pub fn run(&self) -> Vec<String> {
        let doc_addresses = self.searcher.search(&self.query, &DocSetCollector).unwrap();
        let mut docs = doc_addresses.into_iter().collect::<Vec<_>>();
        docs.sort();
        let mut strings = Vec::with_capacity(docs.len());

        for doc_address in docs {
            let segment_reader = &self.searcher.segment_readers()[doc_address.segment_ord as usize];
            let str_column_opt = segment_reader.fast_fields().str("str_fast");

            if let Ok(Some(str_column)) = str_column_opt {
                let doc_id = doc_address.doc_id;
                let term_ord = str_column.term_ords(doc_id).next().unwrap();
                let mut str_buffer = String::new();
                if str_column.ord_to_str(term_ord, &mut str_buffer).is_ok() {
                    strings.push(str_buffer);
                }
            }
        }

        strings
    }
}

struct FetchAllStringsFromDocTask {
    searcher: Searcher,
    query: RangeQuery,
}

impl FetchAllStringsFromDocTask {
    #[inline(never)]
    pub fn run(&self) -> Vec<String> {
        let doc_addresses = self.searcher.search(&self.query, &DocSetCollector).unwrap();
        let mut docs = doc_addresses.into_iter().collect::<Vec<_>>();
        docs.sort();
        let mut strings = Vec::with_capacity(docs.len());

        let str_stored_field = self
            .searcher
            .schema()
            .get_field("str_stored")
            .expect("str_stored field should exist");

        for doc_address in docs {
            // Get the document from the doc store (row store access)
            if let Ok(doc) = self.searcher.doc::<TantivyDocument>(doc_address) {
                // Extract string values from the stored field
                if let Some(field_value) = doc.get_first(str_stored_field) {
                    if let Some(text) = field_value.as_value().as_str() {
                        strings.push(text.to_string());
                    }
                }
            }
        }

        strings
    }
}
