//! Reproduces a per-row latency gap between Turso and SQLite on small batch
//! reads.
//!
//! The query returns every row of a table whose rows all share one flag value:
//!
//! ```sql
//! SELECT id, name, value, is_active FROM test_table WHERE is_active = ?
//! ```
//!

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::runtime::Runtime;

const ROW_COUNTS: &[usize] = &[10, 100, 1000];

const QUERY: &str = "SELECT id, name, value, is_active FROM test_table WHERE is_active = ?";

/// One materialized result row. Both lanes produce the same shape so the mapper
/// work is identical.
#[derive(Debug, PartialEq)]
struct Record {
    id: i64,
    name: String,
    value: i64,
    is_active: bool,
}

fn schema_sql() -> &'static str {
    "CREATE TABLE test_table (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER NOT NULL,
        is_active INTEGER NOT NULL
    )"
}

fn build_sqlite(rows: usize) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(schema_sql()).unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    {
        let mut stmt = tx
            .prepare("INSERT INTO test_table (id, name, value, is_active) VALUES (?, ?, ?, 1)")
            .unwrap();
        for i in 0..rows {
            stmt.execute(rusqlite::params![i as i64, format!("name_{i}"), i as i64])
                .unwrap();
        }
    }
    tx.commit().unwrap();
    conn
}

async fn build_turso(rows: usize) -> (turso::Database, turso::Connection) {
    let db = turso::Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();
    conn.pragma_update("journal_mode", "'mvcc'").await.unwrap();
    conn.execute(schema_sql(), ()).await.unwrap();
    conn.execute("BEGIN", ()).await.unwrap();
    let mut stmt = conn
        .prepare("INSERT INTO test_table (id, name, value, is_active) VALUES (?, ?, ?, 1)")
        .await
        .unwrap();
    for i in 0..rows {
        stmt.execute((i as i64, format!("name_{i}"), i as i64))
            .await
            .unwrap();
    }
    conn.execute("COMMIT", ()).await.unwrap();
    (db, conn)
}

/// SQLite lane: a single sync iteration over all rows, mapping each into a
/// `Record`.
fn query_sqlite(conn: &rusqlite::Connection) -> Vec<Record> {
    let mut stmt = conn.prepare_cached(QUERY).unwrap();
    let mapped = stmt
        .query_map([1i64], |row| {
            Ok(Record {
                id: row.get(0)?,
                name: row.get(1)?,
                value: row.get(2)?,
                is_active: row.get::<_, i64>(3)? != 0,
            })
        })
        .unwrap();
    let mut out = Vec::new();
    for r in mapped {
        out.push(r.unwrap());
    }
    out
}

/// Turso lane: one `.await` per row, mapping each into a `Record`.
async fn query_turso(conn: &turso::Connection) -> Vec<Record> {
    let mut stmt = conn.prepare_cached(QUERY).await.unwrap();
    let mut rows = stmt.query((1i64,)).await.unwrap();
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        out.push(Record {
            id: int_value(&row, 0),
            name: text_value(&row, 1),
            value: int_value(&row, 2),
            is_active: int_value(&row, 3) != 0,
        });
    }
    out
}

fn int_value(row: &turso::Row, idx: usize) -> i64 {
    match row.get_value(idx).unwrap() {
        turso::Value::Integer(i) => i,
        other => panic!("expected integer at column {idx}, got {other:?}"),
    }
}

fn text_value(row: &turso::Row, idx: usize) -> String {
    match row.get_value(idx).unwrap() {
        turso::Value::Text(t) => t,
        other => panic!("expected text at column {idx}, got {other:?}"),
    }
}

fn bench(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("query_batch");

    for &rows in ROW_COUNTS {
        group.throughput(Throughput::Elements(rows as u64));

        let sqlite_conn = build_sqlite(rows);
        group.bench_with_input(BenchmarkId::new("sqlite", rows), &rows, |b, _| {
            b.iter(|| {
                let out = query_sqlite(&sqlite_conn);
                assert_eq!(out.len(), rows);
                out
            });
        });

        let (_turso_db, turso_conn) = rt.block_on(build_turso(rows));
        group.bench_with_input(BenchmarkId::new("turso", rows), &rows, |b, _| {
            b.iter(|| {
                let out = rt.block_on(query_turso(&turso_conn));
                assert_eq!(out.len(), rows);
                out
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
