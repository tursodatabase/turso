use criterion::{criterion_group, criterion_main, BatchSize, Bencher, Criterion, Throughput};
use tantivy::schema::{TantivyDocument, FAST, INDEXED, STORED, STRING, TEXT};
use tantivy::{tokenizer, Index, IndexWriter};

const HDFS_LOGS: &str = include_str!("hdfs.json");
const GH_LOGS: &str = include_str!("gh.json");
const WIKI: &str = include_str!("wiki.json");

fn benchmark(
    b: &mut Bencher,
    input: &str,
    schema: tantivy::schema::Schema,
    commit: bool,
    parse_json: bool,
    is_dynamic: bool,
) {
    if is_dynamic {
        benchmark_dynamic_json(b, input, schema, commit, parse_json)
    } else {
        _benchmark(b, input, schema, commit, parse_json, |schema, doc_json| {
            TantivyDocument::parse_json(schema, doc_json).unwrap()
        })
    }
}

fn get_index(schema: tantivy::schema::Schema) -> Index {
    let mut index = Index::create_in_ram(schema.clone());
    let ff_tokenizer_manager = tokenizer::TokenizerManager::default();
    ff_tokenizer_manager.register(
        "raw",
        tokenizer::TextAnalyzer::builder(tokenizer::RawTokenizer::default())
            .filter(tokenizer::RemoveLongFilter::limit(255))
            .build(),
    );
    index.set_fast_field_tokenizers(ff_tokenizer_manager.clone());
    index
}

fn _benchmark(
    b: &mut Bencher,
    input: &str,
    schema: tantivy::schema::Schema,
    commit: bool,
    include_json_parsing: bool,
    create_doc: impl Fn(&tantivy::schema::Schema, &str) -> TantivyDocument,
) {
    if include_json_parsing {
        let lines: Vec<&str> = input.trim().split('\n').collect();
        b.iter(|| {
            let index = get_index(schema.clone());
            let mut index_writer: IndexWriter =
                index.writer_with_num_threads(1, 100_000_000).unwrap();
            for doc_json in &lines {
                let doc = create_doc(&schema, doc_json);
                index_writer.add_document(doc).unwrap();
            }
            if commit {
                index_writer.commit().unwrap();
            }
        })
    } else {
        let docs: Vec<_> = input
            .trim()
            .split('\n')
            .map(|doc_json| create_doc(&schema, doc_json))
            .collect();
        b.iter_batched(
            || docs.clone(),
            |docs| {
                let index = get_index(schema.clone());
                let mut index_writer: IndexWriter =
                    index.writer_with_num_threads(1, 100_000_000).unwrap();
                for doc in docs {
                    index_writer.add_document(doc).unwrap();
                }
                if commit {
                    index_writer.commit().unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    }
}
fn benchmark_dynamic_json(
    b: &mut Bencher,
    input: &str,
    schema: tantivy::schema::Schema,
    commit: bool,
    parse_json: bool,
) {
    let json_field = schema.get_field("json").unwrap();
    _benchmark(b, input, schema, commit, parse_json, |_schema, doc_json| {
        let json_val: serde_json::Value = serde_json::from_str(doc_json).unwrap();
        tantivy::doc!(json_field=>json_val)
    })
}

pub fn hdfs_index_benchmark(c: &mut Criterion) {
    let schema = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_u64_field("timestamp", INDEXED);
        schema_builder.add_text_field("body", TEXT);
        schema_builder.add_text_field("severity", STRING);
        schema_builder.build()
    };
    let schema_only_fast = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_u64_field("timestamp", FAST);
        schema_builder.add_text_field("body", FAST);
        schema_builder.add_text_field("severity", FAST);
        schema_builder.build()
    };
    let _schema_with_store = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_u64_field("timestamp", INDEXED | STORED);
        schema_builder.add_text_field("body", TEXT | STORED);
        schema_builder.add_text_field("severity", STRING | STORED);
        schema_builder.build()
    };
    let dynamic_schema = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_json_field("json", TEXT | FAST);
        schema_builder.build()
    };

    let mut group = c.benchmark_group("index-hdfs");
    group.throughput(Throughput::Bytes(HDFS_LOGS.len() as u64));
    group.sample_size(20);

    let benches = [
        ("only-indexed-".to_string(), schema, false),
        //("stored-".to_string(), _schema_with_store, false),
        ("only-fast-".to_string(), schema_only_fast, false),
        ("dynamic-".to_string(), dynamic_schema, true),
    ];

    for (prefix, schema, is_dynamic) in benches {
        for commit in [false, true] {
            let suffix = if commit { "with-commit" } else { "no-commit" };
            {
                let parse_json = false;
                // for parse_json in [false, true] {
                let suffix = if parse_json {
                    format!("{suffix}-with-json-parsing")
                } else {
                    suffix.to_string()
                };

                let bench_name = format!("{prefix}{suffix}");
                group.bench_function(bench_name, |b| {
                    benchmark(b, HDFS_LOGS, schema.clone(), commit, parse_json, is_dynamic)
                });
            }
        }
    }
}

pub fn gh_index_benchmark(c: &mut Criterion) {
    let dynamic_schema = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_json_field("json", TEXT | FAST);
        schema_builder.build()
    };
    let dynamic_schema_fast = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_json_field("json", FAST);
        schema_builder.build()
    };

    let mut group = c.benchmark_group("index-gh");
    group.throughput(Throughput::Bytes(GH_LOGS.len() as u64));

    group.bench_function("index-gh-no-commit", |b| {
        benchmark_dynamic_json(b, GH_LOGS, dynamic_schema.clone(), false, false)
    });
    group.bench_function("index-gh-fast", |b| {
        benchmark_dynamic_json(b, GH_LOGS, dynamic_schema_fast.clone(), false, false)
    });

    group.bench_function("index-gh-fast-with-commit", |b| {
        benchmark_dynamic_json(b, GH_LOGS, dynamic_schema_fast.clone(), true, false)
    });
}

pub fn wiki_index_benchmark(c: &mut Criterion) {
    let dynamic_schema = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_json_field("json", TEXT | FAST);
        schema_builder.build()
    };

    let mut group = c.benchmark_group("index-wiki");
    group.throughput(Throughput::Bytes(WIKI.len() as u64));

    group.bench_function("index-wiki-no-commit", |b| {
        benchmark_dynamic_json(b, WIKI, dynamic_schema.clone(), false, false)
    });
    group.bench_function("index-wiki-with-commit", |b| {
        benchmark_dynamic_json(b, WIKI, dynamic_schema.clone(), true, false)
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = hdfs_index_benchmark
}
criterion_group! {
    name = gh_benches;
    config = Criterion::default();
    targets = gh_index_benchmark
}
criterion_group! {
    name = wiki_benches;
    config = Criterion::default();
    targets = wiki_index_benchmark
}
criterion_main!(benches, gh_benches, wiki_benches);
