use std::sync::Arc;
use turso_sqlite::SqliteDialect;

use turso_core::ast::MAX_QUERY_DEPTH;
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
    // https://github.com/tursodatabase/turso/issues/4197
    GenExpr {
        name: "unary-plus",
        build: |depth| nest("+", "1", "", depth),
    },
    GenExpr {
        name: "unary-minus",
        build: |depth| nest("- ", "1", "", depth),
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

/// A statement with `depth` levels of one kind of nested query. Each statement
/// reads the table that [`CREATE_T`] makes.
struct NestedQuery {
    name: &'static str,
    build: fn(depth: usize) -> String,
}

const CREATE_T: &str = "CREATE TABLE t(x)";

const NESTED_QUERIES: &[NestedQuery] = &[
    // https://github.com/tursodatabase/turso/issues/4900
    NestedQuery {
        name: "from-subquery",
        build: |depth| {
            format!(
                "SELECT * FROM {}t{}",
                "(SELECT * FROM ".repeat(depth),
                ")".repeat(depth)
            )
        },
    },
    NestedQuery {
        name: "from-parentheses",
        build: |depth| format!("SELECT * FROM {}t{}", "(".repeat(depth), ")".repeat(depth)),
    },
    // https://github.com/tursodatabase/turso/issues/8105
    NestedQuery {
        name: "cte",
        build: |depth| {
            let mut sql = String::new();
            for i in (1..depth).rev() {
                sql.push_str(&format!("WITH n{i} AS ("));
            }
            sql.push_str("WITH n0 AS (SELECT x FROM t) SELECT * FROM n0");
            for i in 1..depth {
                sql.push_str(&format!(") SELECT * FROM n{i}"));
            }
            sql
        },
    },
];

fn query_nested_too_deeply() -> String {
    format!("Query is nested too deeply (maximum depth {MAX_QUERY_DEPTH})")
}

#[test]
fn query_nesting_over_the_limit_is_an_error() {
    for nested in NESTED_QUERIES {
        for depth in [MAX_QUERY_DEPTH + 1, 100_000] {
            let err = run_with_stack(
                WORKER_STACK,
                vec![CREATE_T.to_string(), (nested.build)(depth)],
            )
            .expect_err(nested.name);
            assert_eq!(
                err.to_string(),
                query_nested_too_deeply(),
                "{} at depth {depth}: {err:?}",
                nested.name
            );
        }
    }
}

#[test]
fn query_nesting_at_the_limit_completes() {
    for nested in NESTED_QUERIES {
        run_with_stack(
            WORKER_STACK,
            vec![CREATE_T.to_string(), (nested.build)(MAX_QUERY_DEPTH)],
        )
        .unwrap_or_else(|err| panic!("{}: {err:?}", nested.name));
    }
}

#[test]
fn view_and_trigger_over_the_query_nesting_limit_are_errors() {
    let select = (NESTED_QUERIES[0].build)(MAX_QUERY_DEPTH + 1);
    for sql in [
        format!("CREATE VIEW v AS {select}"),
        format!("CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t {select}; END"),
    ] {
        let err = run_with_stack(WORKER_STACK, vec![CREATE_T.to_string(), sql])
            .expect_err("statement over the limit");
        assert_eq!(err.to_string(), query_nested_too_deeply(), "{err:?}");
    }
}

#[test]
fn sibling_subqueries_do_not_count_toward_the_query_nesting_limit() {
    let columns = vec!["(SELECT 1)"; MAX_QUERY_DEPTH + 100].join(", ");
    run_with_stack(WORKER_STACK, vec![format!("SELECT {columns}")]).unwrap();
}

/// Statements whose SQL text is flat, but whose plan nests one query in another
/// `depth` times. Only the planner can count this depth.
struct PlannedNesting {
    name: &'static str,
    build: fn(depth: usize) -> Vec<String>,
}

const PLANNED_NESTINGS: &[PlannedNesting] = &[
    PlannedNesting {
        name: "cte-chain",
        build: |depth| {
            let ctes = std::iter::once("n0 AS (SELECT x FROM t)".to_string())
                .chain((1..depth).map(|i| format!("n{i} AS (SELECT * FROM n{})", i - 1)))
                .collect::<Vec<_>>()
                .join(", ");
            vec![
                CREATE_T.to_string(),
                format!("WITH {ctes} SELECT * FROM n{}", depth - 1),
            ]
        },
    },
    PlannedNesting {
        name: "view-chain",
        build: |depth| {
            let mut statements = vec![
                CREATE_T.to_string(),
                "CREATE VIEW v0 AS SELECT x FROM t".to_string(),
            ];
            statements.extend(
                (1..depth).map(|i| format!("CREATE VIEW v{i} AS SELECT * FROM v{}", i - 1)),
            );
            statements.push(format!("SELECT * FROM v{}", depth - 1));
            statements
        },
    },
    // Each different window adds one level of subquery to the plan.
    PlannedNesting {
        name: "windows",
        build: |depth| {
            let columns = (0..depth)
                .map(|i| format!("sum(x) OVER (ORDER BY x ROWS {i} PRECEDING)"))
                .collect::<Vec<_>>()
                .join(", ");
            vec![CREATE_T.to_string(), format!("SELECT {columns} FROM t")]
        },
    },
];

#[test]
fn planned_nesting_over_the_limit_is_an_error() {
    for nesting in PLANNED_NESTINGS {
        let err = run_with_stack(WORKER_STACK, (nesting.build)(MAX_QUERY_DEPTH + 1))
            .expect_err(nesting.name);
        assert_eq!(
            err.to_string(),
            query_nested_too_deeply(),
            "{}: {err:?}",
            nesting.name
        );
    }
}

#[test]
fn planned_nesting_at_the_limit_completes() {
    for nesting in PLANNED_NESTINGS.iter().filter(|n| n.name != "windows") {
        run_with_stack(WORKER_STACK, (nesting.build)(MAX_QUERY_DEPTH))
            .unwrap_or_else(|err| panic!("{}: {err:?}", nesting.name));
    }
}

/// A separate test, because 500 windows take long to plan in a debug build,
/// and separate tests run in parallel.
#[test]
fn windows_at_the_query_nesting_limit_complete() {
    let windows = PLANNED_NESTINGS
        .iter()
        .find(|n| n.name == "windows")
        .unwrap();
    run_with_stack(WORKER_STACK, (windows.build)(MAX_QUERY_DEPTH)).unwrap();
}

/// SQLite's `SQLITE_MAX_TRIGGER_DEPTH`, which Turso also uses.
const MAX_TRIGGER_DEPTH: usize = 1000;

const TRIGGER_DEPTH_ERROR: &str = "too many levels of trigger recursion";

/// Statements that make a chain of `triggers` triggers: an insert into `t0`
/// fires the first one, and each one inserts into the table of the next one.
fn trigger_chain(triggers: usize) -> Vec<String> {
    let tables = (0..=triggers).map(|i| format!("CREATE TABLE t{i}(x)"));
    let chain = (0..triggers).map(|i| {
        format!(
            "CREATE TRIGGER tr{i} AFTER INSERT ON t{i} BEGIN INSERT INTO t{} VALUES (NEW.x); END",
            i + 1
        )
    });
    tables.chain(chain).collect()
}

/// Statements that make a table of `rows` rows, where each row is the parent of
/// the next one, with ON DELETE CASCADE.
fn cascade_chain(rows: usize) -> Vec<String> {
    vec![
        "PRAGMA foreign_keys = ON".to_string(),
        "CREATE TABLE p(id INTEGER PRIMARY KEY, parent INTEGER REFERENCES p(id) ON DELETE CASCADE)"
            .to_string(),
        format!(
            "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < {rows}) \
             INSERT INTO p SELECT i, CASE WHEN i = 1 THEN NULL ELSE i - 1 END FROM c"
        ),
    ]
}

// https://github.com/tursodatabase/turso/issues/9431
#[test]
fn trigger_chain_at_the_trigger_depth_limit_completes() {
    let (result, rows) = run_then_count(
        WORKER_STACK,
        trigger_chain(MAX_TRIGGER_DEPTH),
        "INSERT INTO t0 VALUES (1)",
        format!("SELECT count(*) FROM t{MAX_TRIGGER_DEPTH}"),
    );
    result.unwrap();
    assert_eq!(rows, 1);
}

#[test]
fn trigger_chain_over_the_trigger_depth_limit_is_an_error_and_rolls_back() {
    let (result, rows) = run_then_count(
        WORKER_STACK,
        trigger_chain(MAX_TRIGGER_DEPTH + 1),
        "INSERT INTO t0 VALUES (1)",
        "SELECT count(*) FROM t0".to_string(),
    );
    assert_eq!(result.unwrap_err().to_string(), TRIGGER_DEPTH_ERROR);
    assert_eq!(rows, 0);
}

#[test]
fn cascade_at_the_trigger_depth_limit_completes() {
    let (result, rows) = run_then_count(
        WORKER_STACK,
        cascade_chain(MAX_TRIGGER_DEPTH),
        "DELETE FROM p WHERE id = 1",
        "SELECT count(*) FROM p".to_string(),
    );
    result.unwrap();
    assert_eq!(rows, 0);
}

#[test]
fn cascade_over_the_trigger_depth_limit_is_an_error_and_rolls_back() {
    let (result, rows) = run_then_count(
        WORKER_STACK,
        cascade_chain(MAX_TRIGGER_DEPTH + 1),
        "DELETE FROM p WHERE id = 1",
        "SELECT count(*) FROM p".to_string(),
    );
    assert_eq!(result.unwrap_err().to_string(), TRIGGER_DEPTH_ERROR);
    assert_eq!(rows, (MAX_TRIGGER_DEPTH + 1) as i64);
}

/// Runs `setup`, then `statement`, then `count`, on a thread with `stack_size`
/// bytes of stack. Returns the result of `statement`, and the integer that
/// `count` returns after it.
fn run_then_count(
    stack_size: usize,
    setup: Vec<String>,
    statement: &'static str,
    count: String,
) -> (turso_core::Result<()>, i64) {
    std::thread::Builder::new()
        .stack_size(stack_size)
        .spawn(move || {
            let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
            let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
            let conn = db
                .connect(ConnectOpts::new(SessionRole::Superuser))
                .unwrap();
            for sql in &setup {
                conn.execute(sql).unwrap();
            }
            let result = conn.execute(statement);
            let rows = crate::common::limbo_exec_rows(&conn, &count);
            let rusqlite::types::Value::Integer(n) = rows[0][0] else {
                panic!("{count} did not return an integer: {rows:?}");
            };
            (result, n)
        })
        .expect("failed to spawn worker thread")
        .join()
        .expect("worker thread panicked")
}

/// SQLite has no limit on the nesting of queries, and it does not count the
/// terms of a compound whose last term is VALUES. So a database that it wrote
/// can store views past the limits of Turso. Turso reads the SQL of these views
/// with the same limits as the SQL of statements, so such a view is
/// unavailable, and `ALTER TABLE` stops with the error of the limit. The other
/// views and tables stay available, and `DROP VIEW` removes such a view.
#[test]
fn stored_views_past_the_limits_are_unavailable() {
    on_worker_thread(|| {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("deep.db");
        let nested = (NESTED_QUERIES[0].build)(MAX_QUERY_DEPTH + 100);
        let compound_at_the_limit = compound_ending_in_values(MAX_COMPOUND_SELECT);
        let compound_over_the_limit = compound_ending_in_values(MAX_COMPOUND_SELECT + 1);
        write_database_with_sqlite(
            &path,
            &format!(
                "CREATE TABLE t(x); CREATE VIEW v AS {nested}; \
                 CREATE VIEW w AS {compound_at_the_limit}; \
                 CREATE VIEW x AS {compound_over_the_limit};"
            ),
        );

        let db = crate::common::TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        let rows = crate::common::limbo_exec_rows(&conn, "SELECT count(*) FROM w");
        assert_eq!(
            rows,
            vec![vec![rusqlite::types::Value::Integer(
                MAX_COMPOUND_SELECT as i64
            )]]
        );
        for view in ["v", "x"] {
            let err = conn.execute(format!("SELECT * FROM {view}")).unwrap_err();
            assert!(
                err.to_string()
                    .contains(&format!("view '{view}' could not be loaded")),
                "{view}: {err}"
            );
        }
        let err = conn.execute("ALTER TABLE t RENAME TO t2").unwrap_err();
        assert!(
            err.to_string().contains(&query_nested_too_deeply()),
            "{err}"
        );
        conn.execute("DROP VIEW v").unwrap();
        let err = conn.execute("ALTER TABLE t RENAME TO t2").unwrap_err();
        assert!(
            err.to_string().contains(TOO_MANY_COMPOUND_TERMS_MESSAGE),
            "{err}"
        );
        conn.execute("DROP VIEW x").unwrap();
        conn.execute("ALTER TABLE t RENAME TO t2").unwrap();
    });
}

/// A trigger past a limit stops the open of the database, as other trigger SQL
/// that does not parse does.
#[test]
fn stored_trigger_past_the_nesting_limit_stops_the_open() {
    on_worker_thread(|| {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("deep.db");
        let nested = (NESTED_QUERIES[0].build)(MAX_QUERY_DEPTH + 100);
        write_database_with_sqlite(
            &path,
            &format!(
                "CREATE TABLE t(x); CREATE TABLE u(x); \
                 CREATE TRIGGER tr AFTER INSERT ON u BEGIN INSERT INTO t {nested}; END;"
            ),
        );

        let io: Arc<dyn IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let Err(err) = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect))
        else {
            panic!("the open must fail");
        };
        assert!(err.to_string().starts_with("invalid trigger sql"), "{err}");
    });
}

/// A compound SELECT of `terms` terms whose last term is VALUES.
fn compound_ending_in_values(terms: usize) -> String {
    format!(
        "{} UNION ALL VALUES (2)",
        vec!["SELECT 1"; terms - 1].join(" UNION ALL ")
    )
}

/// Writes a database in WAL mode with SQLite, and runs `sql` in it.
fn write_database_with_sqlite(path: &std::path::Path, sql: &str) {
    let sqlite = rusqlite::Connection::open(path).unwrap();
    sqlite.pragma_update(None, "journal_mode", "wal").unwrap();
    sqlite.execute_batch(sql).unwrap();
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
