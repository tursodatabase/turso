use std::sync::Arc;
use turso_sqlite::SqliteDialect;

use rusqlite::types::Value as SqliteValue;
use turso_core::{ConnectOpts, Connection, Database, MemoryIO, SessionRole, IO};
use turso_parser::MAX_EXPR_DEPTH;

use crate::common::limbo_exec_rows;

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

const SMALL_STACK: usize = 1 << 20;

fn run_on_small_stack(sql: String) -> turso_core::Result<()> {
    run_with_stack(SMALL_STACK, vec![sql])
}

fn run_with_stack(stack_size: usize, statements: Vec<String>) -> turso_core::Result<()> {
    on_thread_with_stack(stack_size, move |conn| {
        for sql in &statements {
            conn.execute(sql)?;
        }
        Ok(())
    })
}

#[test]
fn over_limit_is_a_graceful_depth_error() {
    let expected = format!("Expression tree is too large (maximum depth {MAX_EXPR_DEPTH})");

    for gen_expr in GEN_EXPRS {
        let err = run_on_small_stack((gen_expr.build)(MAX_EXPR_DEPTH)).expect_err(gen_expr.name);
        assert_eq!(err.to_string(), expected, "{}: {err:?}", gen_expr.name);

        run_on_small_stack((gen_expr.build)(MAX_EXPR_DEPTH - 1))
            .unwrap_or_else(|err| panic!("{}: under-limit query failed: {err:?}", gen_expr.name));
    }
}

/// Rust gives spawned threads a 2 MiB stack unless told otherwise.
const DEFAULT_THREAD_STACK: usize = 2 << 20;

#[test]
fn longest_allowed_operator_chains_run_on_a_default_thread_stack() {
    for gen_expr in GEN_EXPRS
        .iter()
        .filter(|g| ["or-chain", "and-chain", "arithmetic-chain"].contains(&g.name))
    {
        run_with_stack(
            DEFAULT_THREAD_STACK,
            vec![(gen_expr.build)(MAX_EXPR_DEPTH - 1)],
        )
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

#[test]
fn long_unary_operator_chains_do_not_overflow_the_stack() {
    let plus_chain = format!("SELECT {}1", "+".repeat(MAX_EXPR_DEPTH - 1));
    let minus_chain = format!("SELECT {}1", "- ".repeat(MAX_EXPR_DEPTH - 1));

    let rows = on_thread_with_stack(SMALL_STACK, move |conn| {
        let mut rows = limbo_exec_rows(conn, &plus_chain);
        rows.extend(limbo_exec_rows(conn, &minus_chain));
        rows
    });

    assert_eq!(
        rows,
        vec![
            vec![SqliteValue::Integer(1)],
            vec![SqliteValue::Integer(-1)]
        ]
    );
}

#[test]
fn nested_subqueries_do_not_overflow_the_stack() {
    let depth = 500;
    let query = format!(
        "SELECT * FROM {}(SELECT 1){}",
        "(SELECT * FROM ".repeat(depth),
        ")".repeat(depth)
    );

    let rows = on_thread_with_stack(SMALL_STACK, move |conn| limbo_exec_rows(conn, &query));

    assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
}

#[test]
fn nested_ctes_do_not_overflow_the_stack() {
    let query = (0..100).fold("SELECT 1".to_string(), |inner, level| {
        format!("WITH n{level} AS ({inner}) SELECT * FROM n{level}")
    });

    let rows = on_thread_with_stack(SMALL_STACK, move |conn| limbo_exec_rows(conn, &query));

    assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
}

#[test]
fn chain_of_ctes_in_one_with_clause_does_not_overflow_the_stack() {
    let ctes = (1..300)
        .map(|level| format!("c{level} AS (SELECT x FROM c{})", level - 1))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!("WITH c0 AS (SELECT 1 AS x), {ctes} SELECT x FROM c299");

    let rows = on_thread_with_stack(SMALL_STACK, move |conn| limbo_exec_rows(conn, &query));

    assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
}

#[test]
fn view_over_nested_subqueries_does_not_overflow_the_stack() {
    let depth = 300;
    let create_view = format!(
        "CREATE VIEW v AS SELECT * FROM {}(SELECT 1 AS x){}",
        "(SELECT * FROM ".repeat(depth),
        ")".repeat(depth)
    );

    let rows = on_thread_with_stack(SMALL_STACK, move |conn| {
        conn.execute(&create_view).unwrap();
        limbo_exec_rows(conn, "SELECT x FROM v")
    });

    assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
}

#[test]
fn many_different_windows_do_not_overflow_the_stack() {
    let windows = 200;
    let columns = (0..windows)
        .map(|i| format!("sum(x) OVER (ORDER BY x + {i})"))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!("SELECT count(*) FROM (SELECT {columns} FROM t)");

    let rows = on_thread_with_stack(SMALL_STACK, move |conn| {
        conn.execute("CREATE TABLE t (x)").unwrap();
        conn.execute("INSERT INTO t VALUES (1), (2)").unwrap();
        limbo_exec_rows(conn, &query)
    });

    assert_eq!(rows, vec![vec![SqliteValue::Integer(2)]]);
}

#[test]
fn compound_select_with_many_terms_does_not_overflow_the_stack() {
    let terms = 1000;
    let query = vec!["SELECT 1"; terms].join(" UNION ALL ");

    let rows = on_thread_with_stack(SMALL_STACK, move |conn| limbo_exec_rows(conn, &query));

    assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]; terms]);
}

#[test]
fn alter_table_with_a_view_over_nested_subqueries_does_not_overflow_the_stack() {
    let depth = 500;
    let create_view = format!(
        "CREATE VIEW v AS SELECT x FROM {}src{}",
        "(SELECT * FROM ".repeat(depth),
        ")".repeat(depth)
    );

    let rows = on_thread_with_stack(SMALL_STACK, move |conn| {
        conn.execute("CREATE TABLE src (x, y)").unwrap();
        conn.execute(&create_view).unwrap();
        conn.execute("ALTER TABLE src RENAME TO dst").unwrap();
        conn.execute("ALTER TABLE dst RENAME COLUMN x TO z")
            .unwrap();
        conn.execute("ALTER TABLE dst DROP COLUMN y").unwrap();
        limbo_exec_rows(
            conn,
            "SELECT sql LIKE '%(SELECT * FROM dst)%' AND sql LIKE 'CREATE VIEW v AS SELECT z FROM%' \
             FROM sqlite_schema WHERE name = 'v'",
        )
    });

    assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
}

#[test]
fn trigger_with_nested_subqueries_does_not_overflow_the_stack() {
    let depth = 300;
    let create_trigger = format!(
        "CREATE TRIGGER tr AFTER INSERT ON a BEGIN \
         INSERT INTO b SELECT * FROM {}(SELECT NEW.x){}; END",
        "(SELECT * FROM ".repeat(depth),
        ")".repeat(depth)
    );

    let rows = on_thread_with_stack(SMALL_STACK, move |conn| {
        conn.execute("CREATE TABLE a (x)").unwrap();
        conn.execute("CREATE TABLE b (x)").unwrap();
        conn.execute(&create_trigger).unwrap();
        conn.execute("INSERT INTO a VALUES (7)").unwrap();
        limbo_exec_rows(conn, "SELECT x FROM b")
    });

    assert_eq!(rows, vec![vec![SqliteValue::Integer(7)]]);
}

#[test]
fn trigger_chain_does_not_overflow_the_stack() {
    let depth = 200;
    let mut schema = String::new();
    for level in 0..=depth {
        schema.push_str(&format!("CREATE TABLE t{level}(x);"));
    }
    for level in 0..depth {
        let next = level + 1;
        schema.push_str(&format!(
            "CREATE TRIGGER tr{level} AFTER INSERT ON t{level} BEGIN INSERT INTO t{next} VALUES (NEW.x); END;"
        ));
    }

    let rows = on_thread_with_stack(SMALL_STACK, move |conn| {
        conn.execute(&schema).unwrap();
        conn.execute("INSERT INTO t0 VALUES (7)").unwrap();
        limbo_exec_rows(conn, &format!("SELECT x FROM t{depth}"))
    });

    assert_eq!(rows, vec![vec![SqliteValue::Integer(7)]]);
}

#[test]
fn cascading_delete_through_a_chain_of_rows_does_not_overflow_the_stack() {
    let rows = on_thread_with_stack(SMALL_STACK, |conn| {
        conn.execute("PRAGMA foreign_keys = ON").unwrap();
        conn.execute(
            "CREATE TABLE node (id INTEGER PRIMARY KEY, \
             parent INTEGER REFERENCES node(id) ON DELETE CASCADE)",
        )
        .unwrap();
        conn.execute("INSERT INTO node VALUES (0, NULL)").unwrap();
        conn.execute(
            "WITH RECURSIVE n(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM n WHERE id < 500) \
             INSERT INTO node SELECT id, id - 1 FROM n",
        )
        .unwrap();
        conn.execute("DELETE FROM node WHERE id = 0").unwrap();
        limbo_exec_rows(conn, "SELECT count(*) FROM node")
    });

    assert_eq!(rows, vec![vec![SqliteValue::Integer(0)]]);
}

fn on_thread_with_stack<T: Send + 'static>(
    stack_size: usize,
    work: impl FnOnce(&Arc<Connection>) -> T + Send + 'static,
) -> T {
    std::thread::Builder::new()
        .stack_size(stack_size)
        .spawn(move || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))
                .expect("failed to open database");
            let conn = db
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .expect("failed to connect");
            work(&conn)
        })
        .expect("failed to spawn worker thread")
        .join()
        .expect("worker thread panicked while running the stack depth test")
}
