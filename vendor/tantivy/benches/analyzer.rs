use criterion::{criterion_group, criterion_main, Criterion};
use tantivy::tokenizer::{
    LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer, TokenizerManager,
};

const ALICE_TXT: &str = include_str!("alice.txt");

pub fn criterion_benchmark(c: &mut Criterion) {
    let tokenizer_manager = TokenizerManager::default();
    let mut tokenizer = tokenizer_manager.get("default").unwrap();
    c.bench_function("default-tokenize-alice", |b| {
        b.iter(|| {
            let mut word_count = 0;
            let mut token_stream = tokenizer.token_stream(ALICE_TXT);
            while token_stream.advance() {
                word_count += 1;
            }
            assert_eq!(word_count, 30_731);
        })
    });
    let mut dynamic_analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
        .dynamic()
        .filter_dynamic(RemoveLongFilter::limit(40))
        .filter_dynamic(LowerCaser)
        .build();
    c.bench_function("dynamic-tokenize-alice", |b| {
        b.iter(|| {
            let mut word_count = 0;
            let mut token_stream = dynamic_analyzer.token_stream(ALICE_TXT);
            while token_stream.advance() {
                word_count += 1;
            }
            assert_eq!(word_count, 30_731);
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(200);
    targets = criterion_benchmark
}
criterion_main!(benches);
