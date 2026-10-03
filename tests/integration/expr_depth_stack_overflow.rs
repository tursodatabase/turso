use std::sync::Arc;
use turso_core::SqliteDialect;

use rusqlite::types::Value as SqliteValue;
use turso_core::{Connection, Database, MemoryIO, Value, IO};
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

const WORKER_STACK: usize = 1 << 20;

fn execute_on_small_stack(sql: String) -> turso_core::Result<()> {
    run_on_small_stack(move |conn| conn.execute(&sql))
}

#[test]
fn over_limit_is_a_graceful_depth_error() {
    let expected =
        format!("Parse error: Expression tree is too large (maximum depth {MAX_EXPR_DEPTH})");

    for gen_expr in GEN_EXPRS {
        let err =
            execute_on_small_stack((gen_expr.build)(MAX_EXPR_DEPTH)).expect_err(gen_expr.name);
        assert_eq!(err.to_string(), expected, "{}: {err:?}", gen_expr.name);

        execute_on_small_stack((gen_expr.build)(MAX_EXPR_DEPTH - 1))
            .unwrap_or_else(|err| panic!("{}: under-limit query failed: {err:?}", gen_expr.name));
    }
}

#[test]
fn long_unary_operator_chains_do_not_overflow_the_stack() {
    let plus_chain = format!("SELECT {}1", "+".repeat(MAX_EXPR_DEPTH - 1));
    let minus_chain = format!("SELECT {}1", "- ".repeat(MAX_EXPR_DEPTH - 1));

    let rows = run_on_small_stack(move |conn| {
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

    let rows = run_on_small_stack(move |conn| limbo_exec_rows(conn, &query));

    assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
}

#[test]
fn nested_ctes_do_not_overflow_the_stack() {
    let query = (0..100).fold("SELECT 1".to_string(), |inner, level| {
        format!("WITH n{level} AS ({inner}) SELECT * FROM n{level}")
    });

    let rows = run_on_small_stack(move |conn| limbo_exec_rows(conn, &query));

    assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]]);
}

#[test]
fn compound_select_with_many_terms_does_not_overflow_the_stack() {
    let terms = 1000;
    let query = vec!["SELECT 1"; terms].join(" UNION ALL ");

    let rows = run_on_small_stack(move |conn| limbo_exec_rows(conn, &query));

    assert_eq!(rows, vec![vec![SqliteValue::Integer(1)]; terms]);
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

    let rows = run_on_small_stack(move |conn| {
        conn.execute(&schema).unwrap();
        conn.execute("INSERT INTO t0 VALUES (7)").unwrap();
        limbo_exec_rows(conn, &format!("SELECT x FROM t{depth}"))
    });

    assert_eq!(rows, vec![vec![SqliteValue::Integer(7)]]);
}

#[test]
fn upsert_with_long_where_clause_does_not_overflow_the_stack() {
    let rows = run_on_small_stack(|conn| {
        conn.execute(create_treasure_table()).unwrap();
        let mut upsert = conn.prepare(upsert_treasure()).unwrap();
        for value in ["first", "second"] {
            upsert
                .bind_at(1.try_into().unwrap(), Value::from_text("map"))
                .unwrap();
            for index in 2..=TREASURE_COLUMNS.len() {
                upsert
                    .bind_at(index.try_into().unwrap(), Value::from_text(value))
                    .unwrap();
            }
            upsert.run_ignore_rows().unwrap();
            upsert.reset().unwrap();
        }
        limbo_exec_rows(conn, "SELECT map_id, rival_ship_uid FROM treasure")
    });

    assert_eq!(
        rows,
        vec![vec![
            SqliteValue::Text("map".to_string()),
            SqliteValue::Text("second".to_string()),
        ]]
    );
}

const TREASURE_COLUMNS: [&str; 14] = [
    "map_id",
    "rival_ship_uid",
    "our_ship_uid",
    "parrot_name",
    "loot",
    "is_plundered",
    "curse_level",
    "shipwreck_code",
    "stolen_from_map_id",
    "sunk_at",
    "buried_at",
    "found_at",
    "dig_attempt_id",
    "dig_started_at",
];

fn create_treasure_table() -> String {
    let other_columns = TREASURE_COLUMNS[1..]
        .iter()
        .map(|column| format!("\"{column}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TABLE \"treasure\" (\"map_id\" TEXT PRIMARY KEY, {other_columns})")
}

fn upsert_treasure() -> String {
    let columns = TREASURE_COLUMNS
        .iter()
        .map(|column| format!("\"{column}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let parameters = vec!["?"; TREASURE_COLUMNS.len()].join(", ");
    let assignments = TREASURE_COLUMNS
        .iter()
        .map(|column| format!("\"{column}\" = excluded.\"{column}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let changed_column_checks = TREASURE_COLUMNS
        .iter()
        .map(|column| {
            format!(
                "typeof(\"{column}\") IS NOT typeof(excluded.\"{column}\") \
                 OR \"{column}\" IS NOT excluded.\"{column}\" COLLATE BINARY"
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(
        "INSERT INTO \"treasure\" ({columns}) VALUES ({parameters}) \
         ON CONFLICT (\"map_id\") DO UPDATE SET {assignments} \
         WHERE {changed_column_checks}"
    )
}

fn run_on_small_stack<T: Send + 'static>(
    work: impl FnOnce(&Arc<Connection>) -> T + Send + 'static,
) -> T {
    std::thread::Builder::new()
        .stack_size(WORKER_STACK)
        .spawn(move || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))
                .expect("failed to open database");
            let conn = db.connect().expect("failed to connect");
            work(&conn)
        })
        .expect("failed to spawn worker thread")
        .join()
        .expect("worker thread panicked while running the stack depth test")
}
