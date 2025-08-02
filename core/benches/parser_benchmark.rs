use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fallible_iterator::FallibleIterator;
use pprof::criterion::{Output, PProfProfiler};
use turso_core::parser::{lexer::Lexer, parser::Parser};
use turso_sqlite3_parser::lexer::{
    sql::{Parser as OldParser, Tokenizer},
    Scanner,
};

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

fn bench_parser(criterion: &mut Criterion) {
    let queries = [
        "BEGIN",
        "BEGIN EXCLUSIVE TRANSACTION my_trans",
        "COMMIT",
        "COMMIT TRANSACTION my_trans",
        "ROLLBACK",
        "ROLLBACK TRANSACTION my_transaction TO my_savepoint",
        "SAVEPOINT my_savepoint",
        "RELEASE SAVEPOINT my_savepoint",
    ];

    for query in queries.iter() {
        let mut group = criterion.benchmark_group(format!("Parser `{query}`"));
        let qb = query.as_bytes();

        group.bench_function(BenchmarkId::new("limbo_parser_query", ""), |b| {
            b.iter(|| {
                for stmt in Parser::new(black_box(qb)) {
                    stmt.unwrap();
                }
            });
        });

        group.bench_function(BenchmarkId::new("limbo_old_parser_query", ""), |b| {
            b.iter(|| {
                let mut parser = OldParser::new(black_box(qb));
                parser.next().unwrap().unwrap()
            });
        });

        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_lexer, bench_parser
}
criterion_main!(benches);
