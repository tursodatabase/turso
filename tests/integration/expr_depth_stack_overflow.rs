use std::sync::Arc;
use turso_core::SqliteDialect;

use turso_core::{ConnectOpts, Database, MemoryIO, IO};
use turso_parser::MAX_EXPR_DEPTH;

struct GenExpr {
    name: &'static str,
    build: fn(depth: usize) -> String,
}

fn nest(prefix: &str, leaf: &str, suffix: &str, depth: usize) -> String {
    format!(
        "SELECT {}{leaf}{}",
        prefix.repeat(depth),
        suffix.repeat(depth)
    )
}

fn chain(op: &str, depth: usize) -> String {
    format!("SELECT 1{}", format!(" {op} 1").repeat(depth))
}

const GEN_EXPRS: &[GenExpr] = &[
    GenExpr {
        name: "or-chain",
        build: |depth| chain("OR", depth),
    },
    GenExpr {
        name: "and-chain",
        build: |depth| chain("AND", depth),
    },
    GenExpr {
        name: "arithmetic-chain",
        build: |depth| chain("+", depth),
    },
    GenExpr {
        name: "parentheses",
        build: |depth| nest("(", "1", ")", depth),
    },
    GenExpr {
        name: "case",
        build: |depth| nest("CASE WHEN 1 THEN ", "1", " ELSE 0 END", depth),
    },
    GenExpr {
        name: "scalar-subquery",
        build: |depth| nest("(SELECT ", "1", ")", depth),
    },
    GenExpr {
        name: "function-call",
        build: |depth| nest("abs(", "1", ")", depth),
    },
    // The shape from issue #6655: a chain of unary operators. Each operator is
    // one more `parse_expr` recursion and one more `translate_expr` level, so
    // this is the cheapest way to get deep parse and translate recursion.
    // The space after `-` matters: `--` starts a comment.
    GenExpr {
        name: "unary-chain",
        build: |depth| format!("SELECT {}1", "- ".repeat(depth)),
    },
    // A wide chain wrapped in a single node whose parse recursion is shallow: the
    // parser consumes the `OR` chain iteratively (so the recursion guard never
    // fires) and the resulting operand is not followed by another operator, so it
    // is only rejected if the operand's own height is checked up front.
    GenExpr {
        name: "parenthesized-or-chain",
        build: |depth| format!("SELECT (1{})", " OR 1".repeat(depth - 1)),
    },
    // Deep expressions that live inside FILTER / OVER (ORDER BY | PARTITION BY)
    // clauses: later passes walk these, so they must count toward the enclosing
    // function call's height even though they are not plain arguments.
    GenExpr {
        name: "filter-clause-star",
        build: |depth| {
            format!(
                "SELECT count(*) FILTER (WHERE 1{})",
                " OR 1".repeat(depth - 1)
            )
        },
    },
    GenExpr {
        name: "filter-clause-args",
        build: |depth| {
            format!(
                "SELECT count(1) FILTER (WHERE 1{})",
                " OR 1".repeat(depth - 1)
            )
        },
    },
    GenExpr {
        name: "window-order-by",
        build: |depth| {
            format!(
                "SELECT sum(1) OVER (ORDER BY 1{})",
                " OR 1".repeat(depth - 1)
            )
        },
    },
    GenExpr {
        name: "window-partition-by",
        build: |depth| {
            format!(
                "SELECT count(*) OVER (PARTITION BY 1{})",
                " OR 1".repeat(depth - 1)
            )
        },
    },
];

/// Shapes whose per-level stack cost comes from SELECT planning rather than
/// from parsing or translating an expression: every level is another nested
/// scalar subquery, so the whole planner runs recursively. Those levels are far
/// more expensive than expression levels and still do not fit a default thread
/// stack in a debug build, so they are only exercised on the big stack below.
const NEEDS_BIG_STACK: &[&str] = &["scalar-subquery"];

const WORKER_STACK: usize = 64 << 20;

/// The stack size a spawned thread gets by default, both from Rust's
/// `thread::spawn` and from a tokio worker thread.
const DEFAULT_STACK: usize = 2 << 20;

/// Nesting depth for the default-stack test: well above the ~16 levels that
/// overflowed the stack in issue #6655, and low enough that the measured worst
/// case in a debug build (~768 KiB) leaves plenty of headroom.
const MODERATE_DEPTH: usize = 32;

fn run_on_big_stack(sql: String) -> turso_core::Result<()> {
    run_with_stack(WORKER_STACK, vec![sql])
}

fn run_with_stack(stack_size: usize, statements: Vec<String>) -> turso_core::Result<()> {
    std::thread::Builder::new()
        .stack_size(stack_size)
        .spawn(move || -> turso_core::Result<()> {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))?;
            let conn = db.connect(ConnectOpts::new())?;
            for sql in &statements {
                conn.execute(sql)?;
            }
            Ok(())
        })
        .expect("failed to spawn worker thread")
        .join()
        .expect("worker thread panicked while executing expression depth test")
}

#[test]
fn over_limit_is_a_graceful_depth_error() {
    let expected =
        format!("Parse error: Expression tree is too large (maximum depth {MAX_EXPR_DEPTH})");

    for gen_expr in GEN_EXPRS {
        let err = run_on_big_stack((gen_expr.build)(MAX_EXPR_DEPTH)).expect_err(gen_expr.name);
        assert_eq!(err.to_string(), expected, "{}: {err:?}", gen_expr.name);

        run_on_big_stack((gen_expr.build)(MAX_EXPR_DEPTH - 1))
            .unwrap_or_else(|err| panic!("{}: under-limit query failed: {err:?}", gen_expr.name));
    }
}

#[test]
fn longest_allowed_operator_chains_run_on_a_default_thread_stack() {
    for gen_expr in GEN_EXPRS
        .iter()
        .filter(|g| ["or-chain", "and-chain", "arithmetic-chain"].contains(&g.name))
    {
        run_with_stack(DEFAULT_STACK, vec![(gen_expr.build)(MAX_EXPR_DEPTH - 1)])
            .unwrap_or_else(|err| panic!("{}: {err:?}", gen_expr.name));
    }
}

/// ORMs generate this "update only if something changed" upsert: for every
/// column, `typeof(x) IS NOT typeof(excluded.x) OR x IS NOT excluded.x`.
/// See https://github.com/tursodatabase/turso/issues/9499.
#[test]
fn upsert_with_a_change_check_per_column_runs_on_a_1_mib_stack() {
    let columns: Vec<String> = (0..14).map(|i| format!("c{i}")).collect();
    let create = format!(
        "CREATE TABLE t ({} PRIMARY KEY, {})",
        columns[0],
        columns[1..].join(", ")
    );
    let set = columns
        .iter()
        .map(|c| format!("{c} = excluded.{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    let changed = columns
        .iter()
        .map(|c| {
            format!(
                "typeof({c}) IS NOT typeof(excluded.{c}) OR {c} IS NOT excluded.{c} COLLATE BINARY"
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    let values = (0..14)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let upsert = format!(
        "INSERT INTO t VALUES ({values}) ON CONFLICT ({}) DO UPDATE SET {set} WHERE {changed}",
        columns[0]
    );

    run_with_stack(1 << 20, vec![create, upsert.clone(), upsert]).unwrap();
}

/// Issue #6655: a ~16 level arithmetic expression, far below `MAX_EXPR_DEPTH`,
/// overflowed the stack of a default-sized thread in a debug build, because the
/// functions on the parse and translate recursion paths had ~100 KiB frames.
/// A regression here aborts the test binary with "has overflowed its stack"
/// instead of failing an assertion, which is exactly how the bug showed up.
#[test]
fn moderately_nested_expressions_fit_a_default_thread_stack() {
    for gen_expr in GEN_EXPRS {
        if NEEDS_BIG_STACK.contains(&gen_expr.name) {
            continue;
        }

        run_with_stack(DEFAULT_STACK, vec![(gen_expr.build)(MODERATE_DEPTH)])
            .unwrap_or_else(|err| panic!("{}: {err:?}", gen_expr.name));
    }
}
