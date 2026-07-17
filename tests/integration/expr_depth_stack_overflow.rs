use std::sync::Arc;
use turso_core::SqliteDialect;

use turso_core::{Database, MemoryIO, IO};
use turso_parser::{MAX_EXPR_DEPTH, MAX_EXPR_NESTING};

/// Which of the two expression-size limits a generated shape exhausts, and
/// therefore which graceful error it must produce at its limit.
enum Limit {
    /// True tree depth ([`MAX_EXPR_DEPTH`]): flat operator chains, which the
    /// parser and translator both consume iteratively.
    Depth,
    /// Nesting ([`MAX_EXPR_NESTING`]): shapes that add a recursion level per
    /// step in the parser/translator.
    Nesting,
}

struct GenExpr {
    name: &'static str,
    build: fn(depth: usize) -> String,
    limit: Limit,
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
        limit: Limit::Depth,
    },
    GenExpr {
        name: "and-chain",
        build: |depth| chain("AND", depth),
        limit: Limit::Depth,
    },
    GenExpr {
        name: "arithmetic-chain",
        build: |depth| chain("+", depth),
        limit: Limit::Depth,
    },
    GenExpr {
        name: "comparison-chain",
        build: |depth| chain("=", depth),
        limit: Limit::Depth,
    },
    GenExpr {
        name: "concat-chain",
        build: |depth| chain("||", depth),
        limit: Limit::Depth,
    },
    // IS TRUE forms are Binary in the AST but the translator emits them
    // through a dedicated non-foldable path, so they count as nesting.
    GenExpr {
        name: "is-true-chain",
        build: |depth| format!("SELECT 1{}", " IS TRUE".repeat(depth)),
        limit: Limit::Nesting,
    },
    GenExpr {
        name: "parentheses",
        build: |depth| nest("(", "1", ")", depth),
        limit: Limit::Nesting,
    },
    GenExpr {
        name: "case",
        build: |depth| nest("CASE WHEN 1 THEN ", "1", " ELSE 0 END", depth),
        limit: Limit::Nesting,
    },
    GenExpr {
        name: "scalar-subquery",
        build: |depth| nest("(SELECT ", "1", ")", depth),
        limit: Limit::Nesting,
    },
    GenExpr {
        name: "function-call",
        build: |depth| nest("abs(", "1", ")", depth),
        limit: Limit::Nesting,
    },
    GenExpr {
        name: "unary-not",
        build: |depth| format!("SELECT {}1", "NOT ".repeat(depth)),
        limit: Limit::Nesting,
    },
    // A wide chain wrapped in a single node whose parse recursion is shallow: the
    // parser consumes the `OR` chain iteratively (so the recursion guard never
    // fires) and the resulting operand is not followed by another operator, so it
    // is only rejected if the operand's own size is checked up front.
    GenExpr {
        name: "parenthesized-or-chain",
        build: |depth| format!("SELECT (1{})", " OR 1".repeat(depth - 1)),
        limit: Limit::Depth,
    },
    // Deep expressions that live inside FILTER / OVER (ORDER BY | PARTITION BY)
    // clauses: later passes walk these, so they must count toward the enclosing
    // function call's size even though they are not plain arguments.
    GenExpr {
        name: "filter-clause-star",
        build: |depth| {
            format!(
                "SELECT count(*) FILTER (WHERE 1{})",
                " OR 1".repeat(depth - 1)
            )
        },
        limit: Limit::Depth,
    },
    GenExpr {
        name: "filter-clause-args",
        build: |depth| {
            format!(
                "SELECT count(1) FILTER (WHERE 1{})",
                " OR 1".repeat(depth - 1)
            )
        },
        limit: Limit::Depth,
    },
    GenExpr {
        name: "window-order-by",
        build: |depth| {
            format!(
                "SELECT sum(1) OVER (ORDER BY 1{})",
                " OR 1".repeat(depth - 1)
            )
        },
        limit: Limit::Depth,
    },
    GenExpr {
        name: "window-partition-by",
        build: |depth| {
            format!(
                "SELECT count(*) OVER (PARTITION BY 1{})",
                " OR 1".repeat(depth - 1)
            )
        },
        limit: Limit::Depth,
    },
];

fn run_on_stack(stack_size: usize, sqls: Vec<String>) -> turso_core::Result<()> {
    std::thread::Builder::new()
        .stack_size(stack_size)
        .spawn(move || -> turso_core::Result<()> {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))?;
            let conn = db.connect()?;
            for sql in &sqls {
                conn.execute(sql)?;
            }
            Ok(())
        })
        .expect("failed to spawn worker thread")
        .join()
        .expect("worker thread panicked while executing expression depth test")
}

const WORKER_STACK: usize = 64 << 20;

#[test]
fn over_limit_is_a_graceful_depth_error() {
    for gen_expr in GEN_EXPRS {
        let (limit, expected) = match gen_expr.limit {
            Limit::Depth => (
                MAX_EXPR_DEPTH,
                format!("Parse error: Expression tree is too large (maximum depth {MAX_EXPR_DEPTH})"),
            ),
            Limit::Nesting => (
                MAX_EXPR_NESTING,
                format!(
                    "Parse error: Expression tree is too deeply nested (maximum nesting {MAX_EXPR_NESTING})"
                ),
            ),
        };

        let err =
            run_on_stack(WORKER_STACK, vec![(gen_expr.build)(limit)]).expect_err(gen_expr.name);
        assert_eq!(err.to_string(), expected, "{}: {err:?}", gen_expr.name);

        run_on_stack(WORKER_STACK, vec![(gen_expr.build)(limit - 1)])
            .unwrap_or_else(|err| panic!("{}: under-limit query failed: {err:?}", gen_expr.name));
    }
}

/// A reproducer-shaped disjunctive predicate: `code = k AND (g1 OR g2 OR ...)`
/// where every group is a small conjunction. Wide, not nested.
fn or_of_and_groups(groups: usize) -> Vec<String> {
    let ors = (0..groups)
        .map(|i| format!("(p1 = {i} AND p2 = {i} AND p3 = {i})"))
        .collect::<Vec<_>>()
        .join(" OR ");
    vec![
        "CREATE TABLE parts (code, p1, p2, p3)".to_string(),
        "INSERT INTO parts VALUES (1, 5, 5, 5)".to_string(),
        format!("SELECT * FROM parts WHERE code = 1 AND ({ors})"),
    ]
}

// real SQLITE_SUSPENDABLE_STACK_SIZE found in the wild
#[cfg(not(debug_assertions))]
const EXAMPLE_STACK_SIZE: usize = 147_456;

/// Chain-shaped expressions stay within a coroutine-sized stack all
/// the way to the depth limit: the parser and translator consume binary
/// operator spines iteratively, so their stack use scales with nesting, not
/// chain length. Debug builds inflate the fixed per-statement frames past
/// this budget, so the exact server budget is only asserted in release.
#[cfg(not(debug_assertions))]
#[test]
fn chains_at_limit_prepare_on_server_sized_stack() {
    // Each OR group adds one link to the OR spine (one depth level), so this
    // is close to the widest predicate of this shape that fits MAX_EXPR_DEPTH.
    let groups = MAX_EXPR_DEPTH - 8;
    run_on_stack(EXAMPLE_STACK_SIZE, or_of_and_groups(groups))
        .unwrap_or_else(|err| panic!("customer-shaped OR-of-ANDs failed: {err:?}"));

    for gen_expr in GEN_EXPRS {
        if matches!(gen_expr.limit, Limit::Nesting) {
            continue;
        }
        run_on_stack(
            EXAMPLE_STACK_SIZE,
            vec![(gen_expr.build)(MAX_EXPR_DEPTH - 1)],
        )
        .unwrap_or_else(|err| panic!("{}: chain at limit failed: {err:?}", gen_expr.name));
    }
}

/// the same chain shapes must prepare on a modest fixed stack even with debug frame
/// inflation, so a regression that makes stack use scale with chain length
/// again fails loudly in regular CI too.
#[cfg(debug_assertions)]
#[test]
fn chains_at_limit_prepare_on_small_stack() {
    const DEBUG_SMALL_STACK: usize = 4 << 20;
    let groups = MAX_EXPR_DEPTH - 8;
    run_on_stack(DEBUG_SMALL_STACK, or_of_and_groups(groups))
        .unwrap_or_else(|err| panic!("customer-shaped OR-of-ANDs failed: {err:?}"));

    for gen_expr in GEN_EXPRS {
        if matches!(gen_expr.limit, Limit::Nesting) {
            continue;
        }
        run_on_stack(
            DEBUG_SMALL_STACK,
            vec![(gen_expr.build)(MAX_EXPR_DEPTH - 1)],
        )
        .unwrap_or_else(|err| panic!("{}: chain at limit failed: {err:?}", gen_expr.name));
    }
}
