use binggan::{black_box, BenchGroup, BenchRunner};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tantivy::collector::{Collector, Count, DocSetCollector, TopDocs};
use tantivy::query::{Query, QueryParser};
use tantivy::schema::{Schema, FAST, INDEXED, TEXT};
use tantivy::{doc, Index, Order, ReloadPolicy, Searcher};

#[derive(Clone)]
struct BenchIndex {
    #[allow(dead_code)]
    index: Index,
    searcher: Searcher,
    query_parser: QueryParser,
}

fn build_shared_indices(num_docs: usize, p_title_a: f32, distribution: &str) -> BenchIndex {
    // Unified schema
    let mut schema_builder = Schema::builder();
    let f_title = schema_builder.add_text_field("title", TEXT);
    let f_num_rand = schema_builder.add_u64_field("num_rand", INDEXED);
    let f_num_asc = schema_builder.add_u64_field("num_asc", INDEXED);
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
                    // Always add title to avoid empty documents
                    let title_token = if rng.random_bool(p_title_a as f64) {
                        "a"
                    } else {
                        "b"
                    };

                    let num_rand = rng.random_range(0u64..1000u64);

                    let num_asc = (doc_id / 10000) as u64;

                    writer
                        .add_document(doc!(
                            f_title=>title_token,
                            f_num_rand=>num_rand,
                            f_num_asc=>num_asc,
                            f_num_rand_fast=>num_rand,
                            f_num_asc_fast=>num_asc,
                        ))
                        .unwrap();
                }
            }
            "sparse" => {
                for doc_id in 0..num_docs {
                    // Always add title to avoid empty documents
                    let title_token = if rng.random_bool(p_title_a as f64) {
                        "a"
                    } else {
                        "b"
                    };

                    let num_rand = rng.random_range(0u64..10000000u64);

                    let num_asc = doc_id as u64;

                    writer
                        .add_document(doc!(
                            f_title=>title_token,
                            f_num_rand=>num_rand,
                            f_num_asc=>num_asc,
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

    // Build query parser for title field
    let qp_title = QueryParser::for_index(&index, vec![f_title]);

    BenchIndex {
        index,
        searcher,
        query_parser: qp_title,
    }
}

fn main() {
    // Prepare corpora with varying scenarios
    let scenarios = vec![
        (
            "dense and 99% a".to_string(),
            10_000_000,
            0.99,
            "dense",
            0,
            9,
        ),
        (
            "dense and 99% a".to_string(),
            10_000_000,
            0.99,
            "dense",
            990,
            999,
        ),
        (
            "sparse and 99% a".to_string(),
            10_000_000,
            0.99,
            "sparse",
            0,
            9,
        ),
        (
            "sparse and 99% a".to_string(),
            10_000_000,
            0.99,
            "sparse",
            9_999_990,
            9_999_999,
        ),
    ];

    let mut runner = BenchRunner::new();
    for (scenario_id, n, p_title_a, num_rand_distribution, range_low, range_high) in scenarios {
        // Build index for this scenario
        let bench_index = build_shared_indices(n, p_title_a, num_rand_distribution);

        // Create benchmark group
        let mut group = runner.new_group();

        // Now set the name (this moves scenario_id)
        group.set_name(scenario_id);

        // Define all four field types
        let field_names = ["num_rand", "num_asc", "num_rand_fast", "num_asc_fast"];

        // Define the three terms we want to test with
        let terms = ["a", "b", "z"];

        // Generate all combinations of terms and field names
        let mut queries = Vec::new();
        for &term in &terms {
            for &field_name in &field_names {
                let query_str = format!(
                    "{} AND {}:[{} TO {}]",
                    term, field_name, range_low, range_high
                );
                queries.push((query_str, field_name.to_string()));
            }
        }

        let query_str = format!(
            "{}:[{} TO {}] AND {}:[{} TO {}]",
            "num_rand_fast", range_low, range_high, "num_asc_fast", range_low, range_high
        );
        queries.push((query_str, "num_asc_fast".to_string()));

        // Run all benchmark tasks for each query and its corresponding field name
        for (query_str, field_name) in queries {
            run_benchmark_tasks(&mut group, &bench_index, &query_str, &field_name);
        }

        group.run();
    }
}

/// Run all benchmark tasks for a given query string and field name
fn run_benchmark_tasks(
    bench_group: &mut BenchGroup,
    bench_index: &BenchIndex,
    query_str: &str,
    field_name: &str,
) {
    // Test count
    add_bench_task(bench_group, bench_index, query_str, Count, "count");

    // Test all results
    add_bench_task(
        bench_group,
        bench_index,
        query_str,
        DocSetCollector,
        "all results",
    );

    // Test top 100 by the field (if it's a FAST field)
    if field_name.ends_with("_fast") {
        // Ascending order
        {
            let collector_name = format!("top100_by_{}_asc", field_name);
            let field_name_owned = field_name.to_string();
            add_bench_task(
                bench_group,
                bench_index,
                query_str,
                TopDocs::with_limit(100).order_by_fast_field::<u64>(field_name_owned, Order::Asc),
                &collector_name,
            );
        }

        // Descending order
        {
            let collector_name = format!("top100_by_{}_desc", field_name);
            let field_name_owned = field_name.to_string();
            add_bench_task(
                bench_group,
                bench_index,
                query_str,
                TopDocs::with_limit(100).order_by_fast_field::<u64>(field_name_owned, Order::Desc),
                &collector_name,
            );
        }
    }
}

fn add_bench_task<C: Collector + 'static>(
    bench_group: &mut BenchGroup,
    bench_index: &BenchIndex,
    query_str: &str,
    collector: C,
    collector_name: &str,
) {
    let task_name = format!("{}_{}", query_str.replace(" ", "_"), collector_name);
    let query = bench_index.query_parser.parse_query(query_str).unwrap();
    let search_task = SearchTask {
        searcher: bench_index.searcher.clone(),
        collector,
        query,
    };
    bench_group.register(task_name, move |_| black_box(search_task.run()));
}

struct SearchTask<C: Collector> {
    searcher: Searcher,
    collector: C,
    query: Box<dyn Query>,
}

impl<C: Collector> SearchTask<C> {
    #[inline(never)]
    pub fn run(&self) -> usize {
        let result = self.searcher.search(&self.query, &self.collector).unwrap();
        if let Some(count) = (&result as &dyn std::any::Any).downcast_ref::<usize>() {
            *count
        } else if let Some(top_docs) = (&result as &dyn std::any::Any)
            .downcast_ref::<Vec<(Option<u64>, tantivy::DocAddress)>>()
        {
            top_docs.len()
        } else if let Some(top_docs) =
            (&result as &dyn std::any::Any).downcast_ref::<Vec<(u64, tantivy::DocAddress)>>()
        {
            top_docs.len()
        } else if let Some(doc_set) = (&result as &dyn std::any::Any)
            .downcast_ref::<std::collections::HashSet<tantivy::DocAddress>>()
        {
            doc_set.len()
        } else {
            eprintln!(
                "Unknown collector result type: {:?}",
                std::any::type_name::<C::Fruit>()
            );
            0
        }
    }
}
