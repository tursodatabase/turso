use std::sync::Arc;
use turso_sqlite::SqliteDialect;

use turso_core::{ConnectOpts, Database, MemoryIO, SessionRole, IO};
use turso_parser::{MAX_COMPOUND_SELECT, MAX_EXPR_DEPTH, TOO_MANY_COMPOUND_TERMS_MESSAGE};

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

const WORKER_STACK: usize = 64 << 20;

fn run_on_big_stack(sql: String) -> turso_core::Result<()> {
    run_with_stack(WORKER_STACK, vec![sql])
}

fn run_with_stack(stack_size: usize, statements: Vec<String>) -> turso_core::Result<()> {
    std::thread::Builder::new()
        .stack_size(stack_size)
        .spawn(move || -> turso_core::Result<()> {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))?;
            let conn = db.connect(ConnectOpts::new(SessionRole::Superuser))?;
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
    let expected = format!("Expression tree is too large (maximum depth {MAX_EXPR_DEPTH})");

    for gen_expr in GEN_EXPRS {
        let err = run_on_big_stack((gen_expr.build)(MAX_EXPR_DEPTH)).expect_err(gen_expr.name);
        assert_eq!(err.to_string(), expected, "{}: {err:?}", gen_expr.name);

        run_on_big_stack((gen_expr.build)(MAX_EXPR_DEPTH - 1))
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

/// SQLite does not count the terms of a compound whose last term is VALUES. So
/// a database that it wrote can store a view that goes past the compound limit
/// of the Turso parser. Turso opens such a database. A statement that uses the
/// view gets the error of the limit from the planner.
#[test]
fn stored_sql_over_the_parse_limits_still_loads() {
    on_worker_thread(|| {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("deep.db");
        write_database_over_the_parse_limits(&path);

        let db = crate::common::TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        let names =
            crate::common::limbo_exec_rows(&conn, "SELECT name FROM sqlite_schema ORDER BY name");
        assert_eq!(names.len(), 3, "{names:?}");
        let rows = crate::common::limbo_exec_rows(&conn, "SELECT count(*) FROM w");
        assert_eq!(
            rows,
            vec![vec![rusqlite::types::Value::Integer(
                MAX_COMPOUND_SELECT as i64
            )]]
        );
        let err = conn.execute("SELECT count(*) FROM x").unwrap_err();
        assert_eq!(err.to_string(), TOO_MANY_COMPOUND_TERMS_MESSAGE);
    });
}

/// ALTER TABLE rewrites the stored SQL that names the renamed table or column.
/// It parses all stored SQL, also the SQL past the parse limits, as the schema
/// loader does.
#[test]
fn alter_table_rewrites_stored_sql_over_the_parse_limits() {
    on_worker_thread(|| {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("deep.db");
        write_database_over_the_parse_limits(&path);

        let db = crate::common::TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        for sql in [
            "ALTER TABLE t RENAME TO t2",
            "ALTER TABLE t2 RENAME COLUMN x TO y",
        ] {
            conn.execute(sql)
                .unwrap_or_else(|err| panic!("{sql}: {err}"));
        }
        let names = crate::common::limbo_exec_rows(
            &conn,
            "SELECT name FROM sqlite_schema WHERE sql LIKE '%t2%' ORDER BY name",
        );
        let text = |name: &str| vec![rusqlite::types::Value::Text(name.to_string())];
        assert_eq!(names, vec![text("t2")]);
    });
}

/// Writes a database with SQLite. It has the table `t`, and the views `w` and
/// `x`, which are compounds of `MAX_COMPOUND_SELECT` and `MAX_COMPOUND_SELECT + 1`
/// terms whose last term is VALUES.
fn write_database_over_the_parse_limits(path: &std::path::Path) {
    let compound = |terms: usize| {
        format!(
            "{} UNION ALL VALUES (2)",
            vec!["SELECT 1"; terms - 1].join(" UNION ALL ")
        )
    };
    let compound_at_the_limit = compound(MAX_COMPOUND_SELECT);
    let compound_over_the_limit = compound(MAX_COMPOUND_SELECT + 1);
    let sqlite = rusqlite::Connection::open(path).unwrap();
    sqlite.pragma_update(None, "journal_mode", "wal").unwrap();
    sqlite
        .execute_batch(&format!(
            "CREATE TABLE t(x); CREATE VIEW w AS {compound_at_the_limit}; \
             CREATE VIEW x AS {compound_over_the_limit};"
        ))
        .unwrap();
}

/// Runs `f` on a thread with [WORKER_STACK] bytes of stack.
fn on_worker_thread(f: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(WORKER_STACK)
        .spawn(f)
        .expect("failed to spawn worker thread")
        .join()
        .expect("worker thread panicked");
}
