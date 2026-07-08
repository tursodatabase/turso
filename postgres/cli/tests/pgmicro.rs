use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Output, Stdio};

fn run_pgmicro(input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pgmicro"))
        .arg(":memory:")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run pgmicro");

    let mut stdin = child.stdin.take().expect("failed to take stdin");
    stdin.write_all(input).expect("failed to write stdin");
    drop(stdin);

    child.wait_with_output().expect("failed to wait for output")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

// ---------------------------------------------------------------------------
// DDL execution
// ---------------------------------------------------------------------------

/// The SQL standard `POSITION(needle IN haystack)` is parsed by libpg_query
/// into a regular function call with operands swapped to `(haystack, needle)`.
/// pgmicro's PG translator rewrites the function name from `position` to
/// `strpos` (which Turso core already implements as an alias of `instr`) so
/// the alias does not need to live in core. This test pins the end-to-end
/// behavior: needle found → 1-based index, needle not found → 0, empty
/// needle → 1 (matches PostgreSQL).
#[test]
fn position_in_form_returns_index() {
    let output = run_pgmicro(b"SELECT POSITION('world' IN 'hello world') AS a, POSITION('xyz' IN 'hello') AS b, POSITION('' IN 'hello') AS c;\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains('7'),
        "expected position=7 for 'world' in 'hello world': {out}"
    );
    assert!(
        out.contains('0'),
        "expected position=0 for not-found needle: {out}"
    );
    assert!(
        out.contains('1'),
        "expected position=1 for empty needle: {out}"
    );
}

#[test]
fn create_table_then_select() {
    let output = run_pgmicro(
        b"CREATE TABLE kv(k TEXT, v INT);\nINSERT INTO kv VALUES ('hello', 42);\nSELECT * FROM kv;\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("hello"), "expected 'hello' in: {out}");
    assert!(out.contains("42"), "expected '42' in: {out}");
}

#[test]
fn create_multiple_tables() {
    let output = run_pgmicro(
        b"CREATE TABLE a(x INT);\nCREATE TABLE b(y INT);\nCREATE TABLE c(z INT);\nSELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("a"), "expected table 'a' in: {out}");
    assert!(out.contains("b"), "expected table 'b' in: {out}");
    assert!(out.contains("c"), "expected table 'c' in: {out}");
}

// ---------------------------------------------------------------------------
// Meta-commands: \dt
// ---------------------------------------------------------------------------

#[test]
fn dt_lists_created_tables() {
    let output = run_pgmicro(b"CREATE TABLE foo(bar TEXT);\n\\dt\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("foo"), "\\dt should list 'foo', got: {out}");
}

#[test]
fn dt_lists_multiple_tables() {
    let output = run_pgmicro(b"CREATE TABLE alpha(x INT);\nCREATE TABLE beta(y TEXT);\n\\dt\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("alpha"), "\\dt should list alpha");
    assert!(out.contains("beta"), "\\dt should list beta");
}

#[test]
fn dt_empty_database() {
    let output = run_pgmicro(b"\\dt\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("No tables found"),
        "expected 'No tables found', got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \d <table>
// ---------------------------------------------------------------------------

#[test]
fn d_describes_table_columns() {
    let output =
        run_pgmicro(b"CREATE TABLE users(id INT PRIMARY KEY, name TEXT, age INT);\n\\d users\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("id"), "should show column 'id'");
    assert!(out.contains("name"), "should show column 'name'");
    assert!(out.contains("age"), "should show column 'age'");
    assert!(out.contains("text"), "should show type 'text'");
}

#[test]
fn d_nonexistent_table() {
    let output = run_pgmicro(b"\\d nonexistent\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("not found"),
        "should report not found, got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \l
// ---------------------------------------------------------------------------

#[test]
fn l_lists_database() {
    let output = run_pgmicro(b"\\l\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains(":memory:"),
        "\\l should show :memory:, got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \conninfo
// ---------------------------------------------------------------------------

#[test]
fn conninfo_shows_database_and_dialect() {
    let output = run_pgmicro(b"\\conninfo\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains(":memory:"), "should show database path");
    assert!(out.contains("PostgreSQL"), "should show dialect");
}

// ---------------------------------------------------------------------------
// Meta-commands: \?
// ---------------------------------------------------------------------------

#[test]
fn help_lists_commands() {
    let output = run_pgmicro(b"\\?\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("\\dt"), "help should mention \\dt");
    assert!(out.contains("\\d"), "help should mention \\d");
    assert!(out.contains("\\l"), "help should mention \\l");
    assert!(out.contains("\\q"), "help should mention \\q");
}

// ---------------------------------------------------------------------------
// Meta-commands: unknown
// ---------------------------------------------------------------------------

#[test]
fn unknown_command_reports_error() {
    let output = run_pgmicro(b"\\bogus\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("Unknown command"),
        "should report unknown command, got: {out}"
    );
}

// ---------------------------------------------------------------------------
// PG catalog access
// ---------------------------------------------------------------------------

#[test]
fn pg_class_shows_created_table() {
    let output = run_pgmicro(
        b"CREATE TABLE test_tbl(id INT, name TEXT);\nSELECT relname FROM pg_class WHERE relkind = 'r';\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("test_tbl"),
        "pg_class should show test_tbl, got: {out}"
    );
}

// ---------------------------------------------------------------------------
// SQL dialect enforcement
// ---------------------------------------------------------------------------

#[test]
fn rejects_sqlite_syntax() {
    let output = run_pgmicro(b"SELECT * FROM sqlite_schema;\n");
    assert_ne!(
        output.status.code(),
        Some(0),
        "sqlite_schema should fail in PG mode"
    );
}

// ---------------------------------------------------------------------------
// Exit codes
// ---------------------------------------------------------------------------

#[test]
fn success_returns_zero() {
    let output = run_pgmicro(b"SELECT 1;\n");
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn error_returns_nonzero() {
    let output = run_pgmicro(b"SELECT * FROM nonexistent;\n");
    assert_eq!(output.status.code(), Some(1));
}

#[test]
fn empty_input_returns_zero() {
    let output = run_pgmicro(b"");
    assert_eq!(output.status.code(), Some(0));
}

// ---------------------------------------------------------------------------
// DEFAULT functions
// ---------------------------------------------------------------------------

#[test]
fn default_now_produces_value() {
    let output = run_pgmicro(
        b"CREATE TABLE t(id INT, ts TEXT DEFAULT now());\n\
          INSERT INTO t(id) VALUES (1);\n\
          SELECT ts FROM t;\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    // now() produces a timestamp like "2026-04-13 ..."
    assert!(
        out.contains("20"),
        "expected timestamp from now(), got: {out}"
    );
}

#[test]
fn default_gen_random_uuid_produces_value() {
    let output = run_pgmicro(
        b"CREATE TABLE t(id INT, uid TEXT DEFAULT gen_random_uuid());\n\
          INSERT INTO t(id) VALUES (1);\n\
          INSERT INTO t(id) VALUES (2);\n\
          SELECT uid FROM t ORDER BY id;\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    // UUID contains hyphens
    assert!(
        out.matches('-').count() >= 4,
        "expected UUID with hyphens, got: {out}"
    );
}

#[test]
fn describe_table_shows_default_expressions() {
    let output = run_pgmicro(
        b"CREATE TABLE t(id INT, ts TEXT DEFAULT now(), uid TEXT DEFAULT gen_random_uuid());\n\
          \\d t\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("now"),
        "\\d should show now() default, got: {out}"
    );
    assert!(
        out.contains("gen_random_uuid"),
        "\\d should show gen_random_uuid() default, got: {out}"
    );
}

#[test]
fn default_casted_expression() {
    let output = run_pgmicro(
        b"CREATE TABLE config(id INT, data jsonb DEFAULT '{}'::jsonb, tags jsonb DEFAULT '[]'::jsonb);\n\
          INSERT INTO config(id) VALUES (1);\n\
          SELECT data, tags FROM config;\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("{}"),
        "expected '{{}}' from casted default, got: {out}"
    );
    assert!(
        out.contains("[]"),
        "expected '[]' from casted default, got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \di
// ---------------------------------------------------------------------------

#[test]
fn di_lists_created_indexes() {
    let output = run_pgmicro(
        b"CREATE TABLE t(id INT PRIMARY KEY, name TEXT);\nCREATE INDEX idx_name ON t(name);\n\\di\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("idx_name"),
        "\\di should list idx_name, got: {out}"
    );
}

#[test]
fn di_empty_database() {
    let output = run_pgmicro(b"\\di\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("No indexes found"),
        "expected 'No indexes found', got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \dv
// ---------------------------------------------------------------------------

#[test]
fn dv_empty_database() {
    let output = run_pgmicro(b"\\dv\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("No views found"),
        "expected 'No views found', got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \dn
// ---------------------------------------------------------------------------

#[test]
fn dn_lists_schemas() {
    let output = run_pgmicro(b"\\dn\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("public"),
        "\\dn should list 'public', got: {out}"
    );
}

#[test]
fn dn_lists_created_schema() {
    let output = run_pgmicro(b"CREATE SCHEMA foo;\n\\dn\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("foo"), "\\dn should list 'foo', got: {out}");
}

// ---------------------------------------------------------------------------
// Meta-commands: \dT
// ---------------------------------------------------------------------------

#[test]
fn d_upper_t_lists_types() {
    let output = run_pgmicro(b"CREATE TYPE mood AS ENUM ('happy', 'sad');\n\\dT\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("mood"), "\\dT should list 'mood', got: {out}");
}

#[test]
fn d_upper_t_empty() {
    let output = run_pgmicro(b"\\dT\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("No types found"),
        "expected 'No types found', got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \du
// ---------------------------------------------------------------------------

#[test]
fn du_lists_roles() {
    let output = run_pgmicro(b"\\du\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("turso"),
        "\\du should list 'turso', got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \df
// ---------------------------------------------------------------------------

#[test]
fn df_lists_functions() {
    let output = run_pgmicro(b"\\df\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("abs") || out.contains("length"),
        "\\df should list some builtin function, got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \d+ (extended describe)
// ---------------------------------------------------------------------------

#[test]
fn d_plus_describes_table_extended() {
    let output = run_pgmicro(
        b"CREATE TABLE tbl(id INT PRIMARY KEY, name TEXT);\nCREATE INDEX idx_tbl_name ON tbl(name);\n\\d+ tbl\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("id"), "should show column 'id', got: {out}");
    assert!(
        out.contains("idx_tbl_name"),
        "should show index, got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \dt+
// ---------------------------------------------------------------------------

#[test]
fn dt_plus_lists_tables_extended() {
    let output = run_pgmicro(b"CREATE TABLE tbl(id INT);\n\\dt+\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("tbl"),
        "\\dt+ should list table name, got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \x
// ---------------------------------------------------------------------------

#[test]
fn x_toggles_expanded() {
    let output = run_pgmicro(b"\\x\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("Expanded display is on"),
        "expected toggle message, got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \timing
// ---------------------------------------------------------------------------

#[test]
fn timing_toggles() {
    let output = run_pgmicro(b"\\timing\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("Timing is on"),
        "expected timing toggle message, got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \echo
// ---------------------------------------------------------------------------

#[test]
fn echo_prints_text() {
    let output = run_pgmicro(b"\\echo hello world\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("hello world"),
        "expected 'hello world', got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Meta-commands: \? (updated help)
// ---------------------------------------------------------------------------

#[test]
fn help_lists_new_commands() {
    let output = run_pgmicro(b"\\?\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("\\di"), "help should mention \\di, got: {out}");
    assert!(out.contains("\\dn"), "help should mention \\dn, got: {out}");
    assert!(out.contains("\\dT"), "help should mention \\dT, got: {out}");
}

// ---------------------------------------------------------------------------
// Array constructor and subscripting
// ---------------------------------------------------------------------------

#[test]
fn array_constructor_and_subscript() {
    let output = run_pgmicro(
        b"CREATE TABLE t(id INT, tags TEXT[]);\n\
          INSERT INTO t VALUES (1, ARRAY['a','b','c']);\n\
          SELECT tags[1], tags[2], tags[3] FROM t;\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("a"), "expected 'a' in output, got: {out}");
    assert!(out.contains("b"), "expected 'b' in output, got: {out}");
    assert!(out.contains("c"), "expected 'c' in output, got: {out}");
}

#[test]
fn array_slice() {
    let output = run_pgmicro(
        b"CREATE TABLE t(id INT, tags TEXT[]);\n\
          INSERT INTO t VALUES (1, ARRAY['a','b','c','d']);\n\
          SELECT tags[2:3] FROM t;\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("b"),
        "expected 'b' in slice output, got: {out}"
    );
    assert!(
        out.contains("c"),
        "expected 'c' in slice output, got: {out}"
    );
}

#[test]
fn array_in_where_clause() {
    let output = run_pgmicro(
        b"CREATE TABLE t(id INT, vals INT[]);\n\
          INSERT INTO t VALUES (1, ARRAY[10,20,30]);\n\
          INSERT INTO t VALUES (2, ARRAY[40,50,60]);\n\
          SELECT id FROM t WHERE vals[1] = 40;\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("2"), "expected id=2, got: {out}");
    assert!(
        !out.contains("1") || out.contains("2"),
        "should only return id=2"
    );
}

// ---------------------------------------------------------------------------
// Dollar-quoted and escape strings
// ---------------------------------------------------------------------------

#[test]
fn dollar_quoted_string() {
    let output = run_pgmicro(b"SELECT $$hello world$$;\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("hello world"),
        "expected 'hello world', got: {out}"
    );
}

#[test]
fn dollar_quoted_with_embedded_quote() {
    let output = run_pgmicro(b"SELECT $$it's fine$$;\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("it's fine"),
        "expected embedded quote, got: {out}"
    );
}

#[test]
fn tagged_dollar_quoted_string() {
    let output = run_pgmicro(b"SELECT $tag$content$tag$;\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("content"), "expected 'content', got: {out}");
}

#[test]
fn escape_string_backslash_n() {
    let output = run_pgmicro(b"SELECT E'line1\\nline2';\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("line1") && out.contains("line2"),
        "expected two lines, got: {out}"
    );
}

#[test]
fn escape_string_backslash_t() {
    let output = run_pgmicro(b"SELECT E'col1\\tcol2';\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("col1") && out.contains("col2"),
        "expected tab-separated, got: {out}"
    );
}

// ---------------------------------------------------------------------------
// Materialized views
// ---------------------------------------------------------------------------

#[test]
fn create_materialized_view_basic() {
    let output = run_pgmicro(
        b"CREATE TABLE items(id INT, name TEXT, price INT);\n\
          INSERT INTO items VALUES (1, 'Laptop', 1200), (2, 'Mouse', 25), (3, 'Monitor', 400);\n\
          CREATE MATERIALIZED VIEW expensive AS SELECT * FROM items WHERE price > 100;\n\
          SELECT name FROM expensive ORDER BY name;\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("Laptop"), "expected Laptop, got: {out}");
    assert!(out.contains("Monitor"), "expected Monitor, got: {out}");
    assert!(
        !out.contains("Mouse"),
        "Mouse should be filtered out: {out}"
    );
}

#[test]
fn materialized_view_with_aggregation() {
    let output = run_pgmicro(
        b"CREATE TABLE sales(product TEXT, amount INT);\n\
          INSERT INTO sales VALUES ('A', 10), ('B', 20), ('A', 30), ('B', 5);\n\
          CREATE MATERIALIZED VIEW totals AS SELECT product, SUM(amount) as total FROM sales GROUP BY product;\n\
          SELECT * FROM totals ORDER BY product;\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("A"), "expected product A, got: {out}");
    assert!(out.contains("40"), "expected total 40 for A, got: {out}");
    assert!(out.contains("B"), "expected product B, got: {out}");
    assert!(out.contains("25"), "expected total 25 for B, got: {out}");
}

#[test]
fn materialized_view_live_update() {
    let output = run_pgmicro(
        b"CREATE TABLE counters(grp TEXT, val INT);\n\
          INSERT INTO counters VALUES ('x', 1), ('y', 2);\n\
          CREATE MATERIALIZED VIEW sums AS SELECT grp, SUM(val) as total FROM counters GROUP BY grp;\n\
          INSERT INTO counters VALUES ('x', 10);\n\
          SELECT * FROM sums WHERE grp = 'x';\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    // After inserting (x, 10), the total for x should be 11 (live update, no REFRESH needed)
    assert!(
        out.contains("11"),
        "expected live-updated total 11, got: {out}"
    );
}

#[test]
fn materialized_view_duplicate_errors() {
    let output = run_pgmicro(
        b"CREATE TABLE t(id INT);\n\
          CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;\n\
          CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;\n",
    );
    let out = stdout(&output);
    let err = String::from_utf8_lossy(&output.stderr).to_string();
    let combined = format!("{out}{err}");
    assert!(
        combined.contains("already exists"),
        "duplicate should error: {combined}"
    );
}

#[test]
fn drop_materialized_view() {
    let output = run_pgmicro(
        b"CREATE TABLE t(id INT);\n\
          CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;\n\
          DROP MATERIALIZED VIEW mv;\n\
          SELECT 'dropped';\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("dropped"), "DROP should succeed: {out}");
}

#[test]
fn drop_materialized_view_if_exists() {
    let output = run_pgmicro(
        b"DROP MATERIALIZED VIEW IF EXISTS nonexistent;\n\
          SELECT 'ok';\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("ok"), "IF EXISTS should not error: {out}");
}

#[test]
fn refresh_materialized_view_is_noop() {
    let output = run_pgmicro(
        b"CREATE TABLE t(id INT);\n\
          CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;\n\
          REFRESH MATERIALIZED VIEW mv;\n\
          SELECT 'ok';\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("ok"), "REFRESH should be a no-op: {out}");
}

// ---------------------------------------------------------------------------
// Named windows (WINDOW clause)
// ---------------------------------------------------------------------------

#[test]
fn named_window_basic() {
    let output = run_pgmicro(
        b"CREATE TABLE emp(id INT, dept TEXT, salary INT);\n\
          INSERT INTO emp VALUES (1, 'eng', 100), (2, 'eng', 200), (3, 'sales', 150);\n\
          SELECT dept, salary, SUM(salary) OVER w FROM emp WINDOW w AS (PARTITION BY dept);\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    // eng partition total = 300
    assert!(out.contains("300"), "expected window sum 300, got: {out}");
    // sales partition total = 150
    assert!(out.contains("150"), "expected window sum 150, got: {out}");
}

#[test]
fn named_window_row_number() {
    let output = run_pgmicro(
        b"CREATE TABLE items(id INT, name TEXT);\n\
          INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c');\n\
          SELECT name, ROW_NUMBER() OVER w FROM items WINDOW w AS (ORDER BY id);\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("1"), "expected row_number 1, got: {out}");
    assert!(out.contains("2"), "expected row_number 2, got: {out}");
    assert!(out.contains("3"), "expected row_number 3, got: {out}");
}

#[test]
fn named_window_multiple_functions_same_window() {
    let output = run_pgmicro(
        b"CREATE TABLE vals(x INT);\n\
          INSERT INTO vals VALUES (10), (20), (30);\n\
          SELECT x, SUM(x) OVER w, AVG(x) OVER w FROM vals WINDOW w AS (ORDER BY x);\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    // Running sums: 10, 30, 60
    assert!(out.contains("10"), "expected running sum 10, got: {out}");
    assert!(out.contains("30"), "expected running sum 30, got: {out}");
    assert!(out.contains("60"), "expected running sum 60, got: {out}");
}

#[test]
fn named_window_multiple_definitions() {
    let output = run_pgmicro(
        b"CREATE TABLE data(grp TEXT, val INT);\n\
          INSERT INTO data VALUES ('a', 1), ('a', 2), ('b', 3);\n\
          SELECT grp, SUM(val) OVER w1, COUNT(*) OVER w2 \
          FROM data \
          WINDOW w1 AS (PARTITION BY grp), w2 AS ();\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    // w1 partitioned sums: a=3, b=3
    assert!(out.contains("3"), "expected partition sum, got: {out}");
    // w2 unpartitioned count = 3 for all rows
    assert!(out.contains("3"), "expected total count 3, got: {out}");
}

#[test]
fn named_window_running_total() {
    // ORDER BY in a named window produces a running total (default RANGE UNBOUNDED PRECEDING)
    let output = run_pgmicro(
        b"CREATE TABLE seq(id INT, val INT);\n\
          INSERT INTO seq VALUES (1, 10), (2, 20), (3, 30);\n\
          SELECT id, SUM(val) OVER w FROM seq WINDOW w AS (ORDER BY id);\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    // Running totals: 10, 30, 60
    assert!(out.contains("10"), "expected running total 10, got: {out}");
    assert!(out.contains("30"), "expected running total 30, got: {out}");
    assert!(out.contains("60"), "expected running total 60, got: {out}");
}

// ---------------------------------------------------------------------------
// COPY FROM via REPL
// ---------------------------------------------------------------------------

/// Write content to a temp file and return its path (file is kept alive via the path).
fn write_temp_copy_file(name: &str, content: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("pgmicro_test_{name}_{}.tsv", std::process::id()));
    std::fs::write(&path, content).expect("failed to write temp file");
    path
}

#[test]
fn copy_from_basic_repl() {
    let path = write_temp_copy_file("basic", "1\tAlice\n2\tBob\n");
    let input = format!(
        "CREATE TABLE users(id INT, name TEXT);\nCOPY users FROM '{}';\nSELECT id, name FROM users ORDER BY id;\n",
        path.display()
    );
    let output = run_pgmicro(input.as_bytes());
    std::fs::remove_file(&path).ok();

    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("Alice"), "expected Alice, got: {out}");
    assert!(out.contains("Bob"), "expected Bob, got: {out}");
}

#[test]
fn copy_from_with_options_repl() {
    let path = write_temp_copy_file("opts", "id|name\n1|Alice\n2|<nil>\n");
    let input = format!(
        "CREATE TABLE t(id INT, name TEXT);\nCOPY t FROM '{}' WITH (DELIMITER '|', NULL '<nil>', HEADER true);\nSELECT id, name FROM t ORDER BY id;\n",
        path.display()
    );
    let output = run_pgmicro(input.as_bytes());
    std::fs::remove_file(&path).ok();

    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("Alice"), "expected Alice, got: {out}");
    // Row 2 has NULL name — should not show <nil> as text
    assert!(
        !out.contains("<nil>"),
        "NULL should not appear as <nil>: {out}"
    );
}

#[test]
fn copy_from_file_not_found_repl() {
    let output =
        run_pgmicro(b"CREATE TABLE t(id INT);\nCOPY t FROM '/nonexistent/path/data.tsv';\n");
    // Should fail with nonzero exit
    assert_ne!(output.status.code(), Some(0));
}

// ---------------------------------------------------------------------------
// Wire protocol: COPY FROM returns "COPY N"
// ---------------------------------------------------------------------------

/// Start pgmicro with --server and wait for it to be ready.
fn start_pgmicro_server(port: u16) -> Child {
    let addr = format!("127.0.0.1:{port}");
    let mut child = Command::new(env!("CARGO_BIN_EXE_pgmicro"))
        .arg(":memory:")
        .arg("--server")
        .arg(&addr)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start pgmicro server");

    // Wait for the server to be ready by polling TCP connect
    for _ in 0..50 {
        if TcpStream::connect(&addr).is_ok() {
            return child;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    child.kill().ok();
    child.wait().ok();
    panic!("pgmicro server did not start on {addr}");
}

/// Minimal PG wire protocol client for testing.
/// Sends startup + simple query and reads responses.
struct PgTestClient {
    stream: TcpStream,
}

impl PgTestClient {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut client = Self { stream };
        client.send_startup();
        client.read_until_ready();
        client
    }

    /// Send StartupMessage (protocol v3.0)
    fn send_startup(&mut self) {
        let mut buf = Vec::new();
        // protocol version 3.0
        buf.extend_from_slice(&196608i32.to_be_bytes());
        // user=turso
        buf.extend_from_slice(b"user\0turso\0");
        // database=main
        buf.extend_from_slice(b"database\0main\0");
        // terminator
        buf.push(0);

        // Length prefix (4 bytes for length + payload)
        let len = (4 + buf.len()) as i32;
        self.stream.write_all(&len.to_be_bytes()).unwrap();
        self.stream.write_all(&buf).unwrap();
        self.stream.flush().unwrap();
    }

    /// Send a simple query message ('Q')
    fn send_query(&mut self, sql: &str) {
        let payload = format!("{sql}\0");
        let len = (4 + payload.len()) as i32;
        self.stream.write_all(b"Q").unwrap();
        self.stream.write_all(&len.to_be_bytes()).unwrap();
        self.stream.write_all(payload.as_bytes()).unwrap();
        self.stream.flush().unwrap();
    }

    /// Read all messages until ReadyForQuery ('Z'), return raw bytes.
    fn read_until_ready(&mut self) -> Vec<u8> {
        let mut all_bytes = Vec::new();
        loop {
            let mut tag = [0u8; 1];
            if self.stream.read_exact(&mut tag).is_err() {
                break;
            }
            let mut len_buf = [0u8; 4];
            self.stream.read_exact(&mut len_buf).unwrap();
            let len = i32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len - 4];
            if !body.is_empty() {
                self.stream.read_exact(&mut body).unwrap();
            }
            all_bytes.push(tag[0]);
            all_bytes.extend_from_slice(&len_buf);
            all_bytes.extend_from_slice(&body);

            // 'Z' = ReadyForQuery
            if tag[0] == b'Z' {
                break;
            }
        }
        all_bytes
    }

    /// Send query and return command tag strings from CommandComplete ('C') messages.
    fn query_command_tags(&mut self, sql: &str) -> Vec<String> {
        self.send_query(sql);
        let response = self.read_until_ready();
        extract_command_tags(&response)
    }

    /// Send query and return the column type OIDs from the first
    /// `RowDescription` (`'T'`) message in the response. Used to assert what
    /// the PG wire protocol reports for each result column — separate from
    /// the runtime value, which the existing column-value tests cover.
    fn query_column_oids(&mut self, sql: &str) -> Vec<u32> {
        self.send_query(sql);
        let response = self.read_until_ready();
        extract_row_description_oids(&response)
    }
}

/// Walk raw PG wire bytes, find the first `RowDescription` (`'T'`), and
/// return the data type OID of each described column. RowDescription body
/// layout per the PG protocol docs: `int16` column count, then for each
/// column: cstring name, `int32` tableOID, `int16` columnAttrNum,
/// `int32` dataTypeOID, `int16` dataTypeSize, `int32` typeModifier,
/// `int16` formatCode.
fn extract_row_description_oids(data: &[u8]) -> Vec<u32> {
    let mut pos = 0;
    while pos < data.len() {
        let tag = data[pos];
        pos += 1;
        if pos + 4 > data.len() {
            break;
        }
        let len =
            i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;
        let body_len = len - 4;
        let body_end = pos + body_len;
        if body_end > data.len() {
            break;
        }
        if tag == b'T' {
            let body = &data[pos..body_end];
            let ncols = u16::from_be_bytes([body[0], body[1]]) as usize;
            let mut p = 2;
            let mut oids = Vec::with_capacity(ncols);
            for _ in 0..ncols {
                // cstring name
                let name_end = body[p..]
                    .iter()
                    .position(|&b| b == 0)
                    .expect("RowDescription column name missing nul terminator")
                    + p;
                p = name_end + 1;
                // tableOID(4) + columnAttrNum(2)
                p += 6;
                let oid = u32::from_be_bytes([body[p], body[p + 1], body[p + 2], body[p + 3]]);
                oids.push(oid);
                // dataTypeOID(4) + dataTypeSize(2) + typeModifier(4) + formatCode(2)
                p += 4 + 2 + 4 + 2;
            }
            return oids;
        }
        pos = body_end;
    }
    Vec::new()
}

// PG type OIDs we assert against. Values from `pg_type.h` — stable across
// PostgreSQL versions, so test expectations stay valid. Only the OIDs the
// current wire tests assert against are declared; add more here as new
// tests need them.
const OID_INT4: u32 = 23;
const OID_TEXT: u32 = 25;
const OID_FLOAT8: u32 = 701;

/// Extract CommandComplete ('C') tag strings from raw PG wire bytes.
fn extract_command_tags(data: &[u8]) -> Vec<String> {
    let mut tags = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        let tag = data[pos];
        pos += 1;
        if pos + 4 > data.len() {
            break;
        }
        let len =
            i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;
        let body_len = len - 4;
        if pos + body_len > data.len() {
            break;
        }
        if tag == b'C' {
            // CommandComplete body is a null-terminated string
            let s = String::from_utf8_lossy(&data[pos..pos + body_len]);
            let s = s.trim_end_matches('\0').to_string();
            tags.push(s);
        }
        pos += body_len;
    }
    tags
}

#[test]
fn wire_copy_from_returns_copy_n() {
    // Use a unique port to avoid conflicts with parallel tests
    let port = 15432 + (std::process::id() % 1000) as u16;
    let mut server = start_pgmicro_server(port);

    let path = write_temp_copy_file("wire", "1\tAlice\n2\tBob\n3\tCharlie\n");

    let mut client = PgTestClient::connect(port);

    // Create table
    let tags = client.query_command_tags("CREATE TABLE users(id INT, name TEXT)");
    assert!(
        tags.iter().any(|t| t.contains("CREATE")),
        "expected CREATE tag, got: {tags:?}"
    );

    // COPY FROM
    let copy_sql = format!("COPY users FROM '{}'", path.display());
    let tags = client.query_command_tags(&copy_sql);
    assert!(
        tags.iter().any(|t| t == "COPY 3"),
        "expected 'COPY 3' tag, got: {tags:?}"
    );

    // Verify data via SELECT
    let tags = client.query_command_tags("SELECT id, name FROM users ORDER BY id");
    // SELECT produces a CommandComplete like "SELECT 3"
    assert!(
        tags.iter().any(|t| t.starts_with("SELECT")),
        "expected SELECT tag, got: {tags:?}"
    );

    std::fs::remove_file(&path).ok();
    server.kill().ok();
    server.wait().ok();
}

/// Wire-protocol fixture: spin up pgmicro, hand the caller a connected
/// client, run their assertions, then shut the server down. Each test
/// gets its own port so they can run in parallel without TCP collisions.
fn with_pg_client<F: FnOnce(&mut PgTestClient)>(port_seed: u16, f: F) {
    // Compose a port from the test-supplied seed and the test process id so
    // multiple test files don't collide on a shared port range either.
    let port = 16000 + port_seed + (std::process::id() % 100) as u16;
    let mut server = start_pgmicro_server(port);
    let mut client = PgTestClient::connect(port);
    f(&mut client);
    server.kill().ok();
    server.wait().ok();
}

/// `SELECT 42` MUST report INT4 over the wire. PostgreSQL itself does, and
/// PG clients (libpq, JDBC, psycopg2, node-postgres) drive value decoding
/// off the column OID — reporting TEXT here would silently turn integer
/// literals into strings at the client. Verified against the API change
/// where integer literals previously fell through to TEXT.
#[test]
fn wire_integer_literal_reports_int4() {
    with_pg_client(1, |c| {
        assert_eq!(c.query_column_oids("SELECT 42"), vec![OID_INT4]);
    });
}

/// `SELECT 3.14` reports FLOAT8. PG normally returns NUMERIC for unannotated
/// numeric literals; FLOAT8 is the pgmicro choice because Turso stores
/// reals as 64-bit floats and the client decodes the wire bytes directly.
#[test]
fn wire_real_literal_reports_float8() {
    with_pg_client(2, |c| {
        assert_eq!(c.query_column_oids("SELECT 3.14"), vec![OID_FLOAT8]);
    });
}

/// `SELECT 'hello'` reports TEXT. Already correct before the API change;
/// asserting here to lock in the contract.
#[test]
fn wire_text_literal_reports_text() {
    with_pg_client(3, |c| {
        assert_eq!(c.query_column_oids("SELECT 'hello'"), vec![OID_TEXT]);
    });
}

/// Arithmetic over integer operands MUST report INT4, matching PostgreSQL.
/// SQLite's own affinity machinery deliberately stops at binary operators
/// (column-affinity model), so we explicitly walk arithmetic to propagate
/// the operand type — this test pins that walker down.
#[test]
fn wire_integer_arithmetic_reports_int4() {
    with_pg_client(4, |c| {
        assert_eq!(c.query_column_oids("SELECT 42 + 1"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 1 + 1 + 1"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 100 - 7"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 6 * 7"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 13 % 5"), vec![OID_INT4]);
    });
}

/// Mixed numeric arithmetic widens INTEGER+REAL → REAL → wire FLOAT8.
#[test]
fn wire_mixed_arithmetic_widens_to_float8() {
    with_pg_client(5, |c| {
        assert_eq!(c.query_column_oids("SELECT 42 + 1.5"), vec![OID_FLOAT8]);
        assert_eq!(c.query_column_oids("SELECT 3.14 * 2"), vec![OID_FLOAT8]);
    });
}

/// Bitwise ops always report INT4 — matches PostgreSQL.
#[test]
fn wire_bitwise_ops_report_int4() {
    with_pg_client(6, |c| {
        assert_eq!(c.query_column_oids("SELECT 1 << 4"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 256 >> 2"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 12 & 10"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 1 | 2"), vec![OID_INT4]);
    });
}

/// Comparison and logical ops return INT4 — SQLite returns 0/1 as INTEGER
/// at runtime; pgmicro's wire layer reports INT4 here. A future change
/// could map these to BOOL OID, but for now stable + assertable.
#[test]
fn wire_comparison_and_logical_report_int4() {
    with_pg_client(7, |c| {
        assert_eq!(c.query_column_oids("SELECT 1 = 1"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 2 < 3"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 5 > 4"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 1 AND 0"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 1 OR 0"), vec![OID_INT4]);
        // Nested: arithmetic feeding a comparison still propagates correctly.
        assert_eq!(c.query_column_oids("SELECT 42 + 1 = 43"), vec![OID_INT4]);
    });
}

/// Concat (`||`) always reports TEXT.
#[test]
fn wire_concat_reports_text() {
    with_pg_client(8, |c| {
        assert_eq!(c.query_column_oids("SELECT 'a' || 'b'"), vec![OID_TEXT]);
        assert_eq!(
            c.query_column_oids("SELECT 'x' || 'y' || 'z'"),
            vec![OID_TEXT]
        );
    });
}

/// Unary +/- preserves operand affinity. Parenthesised expressions
/// (`(1+2)*3`) still propagate types — the walker recurses through.
#[test]
fn wire_unary_and_parens_propagate() {
    with_pg_client(9, |c| {
        assert_eq!(c.query_column_oids("SELECT -42"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT +5"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT (1 + 2) * 3"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT NOT 1"), vec![OID_INT4]);
    });
}

/// `CAST(... AS type)` lands the inferred primitive in declared_name and
/// the wire layer picks the matching PG OID.
#[test]
fn wire_cast_reports_target_type() {
    with_pg_client(10, |c| {
        assert_eq!(
            c.query_column_oids("SELECT CAST('42' AS INTEGER)"),
            vec![OID_INT4]
        );
        assert_eq!(
            c.query_column_oids("SELECT CAST(42 AS TEXT)"),
            vec![OID_TEXT]
        );
        assert_eq!(
            c.query_column_oids("SELECT CAST(1 AS REAL)"),
            vec![OID_FLOAT8]
        );
    });
}

/// Direct table-column references take the schema-tagged path: a column
/// declared INTEGER reports INT4, TEXT reports TEXT, and so on. The wire
/// layer must not regress to TEXT here.
#[test]
fn wire_table_columns_report_declared_type() {
    with_pg_client(11, |c| {
        c.query_command_tags("CREATE TABLE t(id INTEGER, label TEXT, score REAL)");
        assert_eq!(
            c.query_column_oids("SELECT id, label, score FROM t"),
            vec![OID_INT4, OID_TEXT, OID_FLOAT8]
        );
    });
}

/// Multi-column SELECT mixing literals and arithmetic — each column is
/// classified independently, and the wire layer surfaces all of them with
/// the correct OID rather than collapsing the row to a single type.
#[test]
fn wire_multi_column_select_classifies_each() {
    with_pg_client(12, |c| {
        assert_eq!(
            c.query_column_oids("SELECT 1, 'two', 3.0, 1 + 1, 'a' || 'b'"),
            vec![OID_INT4, OID_TEXT, OID_FLOAT8, OID_INT4, OID_TEXT]
        );
    });
}
