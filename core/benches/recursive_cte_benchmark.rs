//! Recursive CTE benchmark.
//!
//! A recursive CTE keeps the rows its next step has not read yet in a work
//! queue, so every row is written to that queue once and taken out once. These
//! workloads separate the cost of the queue from the cost of the expressions
//! that fill it:
//! - series: one row in the queue at a time, almost nothing else.
//! - tree_walk: two rows out for every row in, so the queue grows to thousands
//!   of rows and its b-tree goes over one page.
//! - union_dedup: UNION instead of UNION ALL, which adds the index of rows the
//!   CTE has already given back.
//! - ordered_queue: ORDER BY in the recursive CTE, which keys the queue by the
//!   sort columns instead of by insertion order.
//! - sudoku: the solver from the SQLite documentation. The queue holds the
//!   partly filled boards, and each step does a lot of substr/instr work on
//!   top.
//!
//! Run:  cargo bench -p turso_core --bench recursive_cte_benchmark

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{black_box, criterion_group, criterion_main, Criterion};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use std::sync::Arc;
use std::time::Duration;
use turso_core::{Database, MemoryIO, SqliteDialect, Statement, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const SERIES: &str = "WITH RECURSIVE c(i) AS (
    VALUES(1) UNION ALL SELECT i + 1 FROM c WHERE i < 50000
) SELECT count(*), sum(i) FROM c";

const TREE_WALK: &str = "WITH RECURSIVE walk(id, depth) AS (
    SELECT 1, 0
    UNION ALL
    SELECT id * 2 + child, depth + 1
      FROM walk, (SELECT 0 AS child UNION ALL SELECT 1)
     WHERE depth < 14
) SELECT count(*), max(id) FROM walk";

const UNION_DEDUP: &str = "WITH RECURSIVE ring(i) AS (
    VALUES(0) UNION SELECT (i + 7) % 5000 FROM ring
) SELECT count(*), sum(i) FROM ring";

const ORDERED_QUEUE: &str = "WITH RECURSIVE c(i) AS (
    VALUES(1) UNION ALL SELECT i + 1 FROM c WHERE i < 20000 ORDER BY 1 DESC
) SELECT count(*), sum(i) FROM c";

/// The sudoku solver from https://sqlite.org/lang_with.html, on the puzzle
/// that page uses.
const SUDOKU: &str = "WITH RECURSIVE
  input(sud) AS (
    VALUES('53..7....6..195....98....6.8...6...34..8.3..17...2...6.6....28....419..5....8..79')
  ),
  digits(z, lp) AS (
    VALUES('1', 1)
    UNION ALL SELECT CAST(lp+1 AS TEXT), lp+1 FROM digits WHERE lp<9
  ),
  x(s, ind) AS (
    SELECT sud, instr(sud, '.') FROM input
    UNION ALL
    SELECT
      substr(s, 1, ind-1) || z || substr(s, ind+1),
      instr( substr(s, 1, ind-1) || z || substr(s, ind+1), '.' )
     FROM x, digits AS z
    WHERE ind>0
      AND NOT EXISTS (
            SELECT 1
              FROM digits AS lp
             WHERE z.z = substr(s, ((ind-1)/9)*9 + lp, 1)
                OR z.z = substr(s, ((ind-1)%9) + (lp-1)*9 + 1, 1)
                OR z.z = substr(s, (((ind-1)/3) % 3) * 3
                        + ((ind-1)/27) * 27 + lp
                        + ((lp-1) / 3) * 6, 1)
         )
  )
SELECT s FROM x WHERE ind=0";

const QUERIES: &[(&str, &str)] = &[
    ("series", SERIES),
    ("tree_walk", TREE_WALK),
    ("union_dedup", UNION_DEDUP),
    ("ordered_queue", ORDERED_QUEUE),
    ("sudoku", SUDOKU),
];

#[turso_macros::codspeed_criterion_benchmark]
fn bench_recursive_cte(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("recursive_cte");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(3));

    // Nothing is read from storage: the CTEs make every row they need, so the
    // measurement is all query execution.
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();

    for (label, sql) in QUERIES {
        group.bench_function(*label, |b| {
            let mut stmt = conn.prepare(sql).unwrap();
            assert_eq!(
                run(&db, &mut stmt),
                1,
                "{label} should give back exactly one row"
            );
            b.iter(|| run(&db, &mut stmt));
        });
    }
    group.finish();
}

fn run(db: &Database, stmt: &mut Statement) -> usize {
    let mut rows = 0;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row());
                rows += 1;
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
    rows
}

criterion_group!(benches, bench_recursive_cte);
criterion_main!(benches);
