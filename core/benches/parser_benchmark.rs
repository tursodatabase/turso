use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fallible_iterator::FallibleIterator;
use pprof::criterion::{Output, PProfProfiler};
use turso_core::parser::{lexer::Lexer, parser::Parser};
use turso_sqlite3_parser::lexer::{
    sql::{Parser as OldParser, Tokenizer},
    Scanner,
};

fn bench_parser(criterion: &mut Criterion) {
    let queries = ["SELECT 1"];

    for query in queries.iter() {
        let mut group = criterion.benchmark_group(format!("Parser `{query}`"));
        let qb = query.as_bytes();

        group.bench_function(BenchmarkId::new("limbo_parser_query", ""), |b| {
            b.iter(|| Parser::new(black_box(qb)).next().unwrap());
        });

        group.bench_function(BenchmarkId::new("limbo_old_parser_query", ""), |b| {
            b.iter(|| {
                OldParser::new(black_box(qb)).next().unwrap().unwrap();
            });
        });

        group.finish();
    }
}

fn bench_lexer(criterion: &mut Criterion) {
    let queries = [
        "SELECT 1",
        "SELECT * FROM users LIMIT 1",
        "SELECT first_name, count(1) FROM users GROUP BY first_name HAVING count(1) > 1 ORDER BY count(1)  LIMIT 1",
    ];

    for query in queries.iter() {
        let mut group = criterion.benchmark_group(format!("Lexer `{query}`"));
        let qb = query.as_bytes();

        group.bench_function(BenchmarkId::new("limbo_lexer_query", ""), |b| {
            b.iter(|| {
                for token in Lexer::new(black_box(qb)) {
                    token.unwrap();
                }
            });
        });

        group.bench_function(BenchmarkId::new("limbo_old_lexer_query", ""), |b| {
            b.iter(|| {
                let tokenizer = Tokenizer::new();
                let mut scanner = Scanner::new(black_box(tokenizer));
                loop {
                    match scanner.scan(black_box(qb)).unwrap() {
                        (_, None, _) => break,
                        _ => {}
                    }
                }
            });
        });

        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_parser, bench_lexer
}
criterion_main!(benches);
