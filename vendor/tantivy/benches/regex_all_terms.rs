// Benchmarks regex query that matches all terms in a synthetic index.
//
// Corpus model:
// - N unique terms: t000000, t000001, ...
// - M docs
// - K tokens per doc: doc i gets terms derived from (i, token_index)
//
// Query:
// - Regex "t.*" to match all terms
//
// Run with:
// - cargo bench --bench regex_all_terms
//

use std::fmt::Write;

use binggan::{black_box, BenchRunner};
use tantivy::collector::Count;
use tantivy::query::RegexQuery;
use tantivy::schema::{Schema, TEXT};
use tantivy::{doc, Index, ReloadPolicy};

const HEAP_SIZE_BYTES: usize = 200_000_000;

#[derive(Clone, Copy)]
struct BenchConfig {
    num_terms: usize,
    num_docs: usize,
    tokens_per_doc: usize,
}

fn main() {
    let configs = default_configs();

    let mut runner = BenchRunner::new();
    for config in configs {
        let (index, text_field) = build_index(config, HEAP_SIZE_BYTES);
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .expect("reader");
        let searcher = reader.searcher();
        let query = RegexQuery::from_pattern("t.*", text_field).expect("regex query");

        let mut group = runner.new_group();
        group.set_name(format!(
            "regex_all_terms_t{}_d{}_k{}",
            config.num_terms, config.num_docs, config.tokens_per_doc
        ));
        group.register("regex_count", move |_| {
            let count = searcher.search(&query, &Count).expect("search");
            black_box(count);
        });
        group.run();
    }
}

fn default_configs() -> Vec<BenchConfig> {
    vec![
        BenchConfig {
            num_terms: 10_000,
            num_docs: 100_000,
            tokens_per_doc: 1,
        },
        BenchConfig {
            num_terms: 10_000,
            num_docs: 100_000,
            tokens_per_doc: 8,
        },
        BenchConfig {
            num_terms: 100_000,
            num_docs: 100_000,
            tokens_per_doc: 1,
        },
        BenchConfig {
            num_terms: 100_000,
            num_docs: 100_000,
            tokens_per_doc: 8,
        },
    ]
}

fn build_index(config: BenchConfig, heap_size_bytes: usize) -> (Index, tantivy::schema::Field) {
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text", TEXT);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    let term_width = config.num_terms.to_string().len();
    {
        let mut writer = index
            .writer_with_num_threads(1, heap_size_bytes)
            .expect("writer");
        let mut buffer = String::new();
        for doc_id in 0..config.num_docs {
            buffer.clear();
            for token_idx in 0..config.tokens_per_doc {
                if token_idx > 0 {
                    buffer.push(' ');
                }
                let term_id = (doc_id * config.tokens_per_doc + token_idx) % config.num_terms;
                write!(&mut buffer, "t{term_id:0term_width$}").expect("write token");
            }
            writer
                .add_document(doc!(text_field => buffer.as_str()))
                .expect("add_document");
        }
        writer.commit().expect("commit");
    }

    (index, text_field)
}
