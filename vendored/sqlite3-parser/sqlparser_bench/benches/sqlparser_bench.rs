// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use criterion::{criterion_group, criterion_main, Criterion};
use fallible_iterator::FallibleIterator;
use turso_sqlite3_parser::{dialect::keyword_token, lexer::sql::Parser};

fn basic_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlparser-rs parsing benchmark");

    let string = b"SELECT * FROM `table` WHERE 1 = 1";
    group.bench_with_input("sqlparser::select", &string, |b, &s| {
        b.iter(|| {
            let mut parser = Parser::new(s);
            assert!(parser.next().unwrap().unwrap().readonly())
        });
    });

    let with_query = b"
        WITH derived AS (
            SELECT MAX(a) AS max_a,
                   COUNT(b) AS b_num,
                   user_id
            FROM `TABLE`
            GROUP BY user_id
        )
        SELECT * FROM `table`
        LEFT JOIN derived USING (user_id)
    ";
    group.bench_with_input("sqlparser::with_select", &with_query, |b, &s| {
        b.iter(|| {
            let mut parser = Parser::new(s);
            assert!(parser.next().unwrap().unwrap().readonly())
        });
    });

    static VALUES: [&[u8]; 136] = [
        b"ABORT",
        b"ACTION",
        b"ADD",
        b"AFTER",
        b"ALL",
        b"ALTER",
        b"ANALYZE",
        b"AND",
        b"AS",
        b"ASC",
        b"ATTACH",
        b"AUTOINCREMENT",
        b"BEFORE",
        b"BEGIN",
        b"BETWEEN",
        b"BY",
        b"CASCADE",
        b"CASE",
        b"CAST",
        b"CHECK",
        b"COLLATE",
        b"COLUMN",
        b"COMMIT",
        b"CONFLICT",
        b"CONSTRAINT",
        b"CREATE",
        b"CROSS",
        b"CURRENT",
        b"CURRENT_DATE",
        b"CURRENT_TIME",
        b"CURRENT_TIMESTAMP",
        b"DATABASE",
        b"DEFAULT",
        b"DEFERRABLE",
        b"DEFERRED",
        b"DELETE",
        b"DESC",
        b"DETACH",
        b"DISTINCT",
        b"DO",
        b"DROP",
        b"EACH",
        b"ELSE",
        b"END",
        b"ESCAPE",
        b"EXCEPT",
        b"EXCLUSIVE",
        b"EXISTS",
        b"EXPLAIN",
        b"FAIL",
        b"FILTER",
        b"FOLLOWING",
        b"FOR",
        b"FOREIGN",
        b"FROM",
        b"FULL",
        b"GLOB",
        b"GROUP",
        b"HAVING",
        b"IF",
        b"IGNORE",
        b"IMMEDIATE",
        b"IN",
        b"INDEX",
        b"INDEXED",
        b"INITIALLY",
        b"INNER",
        b"INSERT",
        b"INSTEAD",
        b"INTERSECT",
        b"INTO",
        b"IS",
        b"ISNULL",
        b"JOIN",
        b"KEY",
        b"LEFT",
        b"LIKE",
        b"LIMIT",
        b"MATCH",
        b"NATURAL",
        b"NO",
        b"NOT",
        b"NOTHING",
        b"NOTNULL",
        b"NULL",
        b"OF",
        b"OFFSET",
        b"ON",
        b"OR",
        b"ORDER",
        b"OUTER",
        b"OVER",
        b"PARTITION",
        b"PLAN",
        b"PRAGMA",
        b"PRECEDING",
        b"PRIMARY",
        b"QUERY",
        b"RAISE",
        b"RANGE",
        b"RECURSIVE",
        b"REFERENCES",
        b"REGEXP",
        b"REINDEX",
        b"RELEASE",
        b"RENAME",
        b"REPLACE",
        b"RESTRICT",
        b"RIGHT",
        b"ROLLBACK",
        b"ROW",
        b"ROWS",
        b"SAVEPOINT",
        b"SELECT",
        b"SET",
        b"TABLE",
        b"TEMP",
        b"TEMPORARY",
        b"THEN",
        b"TO",
        b"TRANSACTION",
        b"TRIGGER",
        b"UNBOUNDED",
        b"UNION",
        b"UNIQUE",
        b"UPDATE",
        b"USING",
        b"VACUUM",
        b"VALUES",
        b"VIEW",
        b"VIRTUAL",
        b"WHEN",
        b"WHERE",
        b"WINDOW",
        b"WITH",
        b"WITHOUT",
    ];
    group.bench_with_input("keyword_token", &VALUES, |b, &s| {
        b.iter(|| {
            for value in &s {
                assert!(keyword_token(value).is_some())
            }
        });
    });
}

criterion_group!(benches, basic_queries);
criterion_main!(benches);
