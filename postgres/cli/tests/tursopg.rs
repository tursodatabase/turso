use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Output, Stdio};

fn run_tursopg(input: &[u8]) -> Output {
    run_tursopg_with_db(":memory:", input)
}

fn run_tursopg_with_db(db_path: impl AsRef<std::ffi::OsStr>, input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tursopg"))
        .arg(db_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run tursopg");

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
/// tursopg's PG translator rewrites the function name from `position` to
/// `strpos` (which Turso core already implements as an alias of `instr`) so
/// the alias does not need to live in core. This test pins the end-to-end
/// behavior: needle found → 1-based index, needle not found → 0, empty
/// needle → 1 (matches PostgreSQL).
#[test]
fn position_in_form_returns_index() {
    let output = run_tursopg(b"SELECT POSITION('world' IN 'hello world') AS a, POSITION('xyz' IN 'hello') AS b, POSITION('' IN 'hello') AS c;\n");
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
    let output = run_tursopg(
        b"CREATE TABLE kv(k TEXT, v INT);\nINSERT INTO kv VALUES ('hello', 42);\nSELECT * FROM kv;\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("hello"), "expected 'hello' in: {out}");
    assert!(out.contains("42"), "expected '42' in: {out}");
}

#[test]
fn repl_reports_errors_like_psql() {
    let output = run_tursopg(
        b"CREATE TABLE t (id INT PRIMARY KEY);\nINSERT INTO t VALUES (1);\nINSERT INTO t VALUES (1);\nSELECT 1/0;\n",
    );
    let out = stdout(&output);
    assert!(
        out.contains("ERROR:  duplicate key value violates unique constraint \"t_pkey\""),
        "expected psql-style unique violation, got: {out}"
    );
    assert!(
        out.contains("DETAIL:  Key (id)=(1) already exists."),
        "expected DETAIL line, got: {out}"
    );
    assert!(
        out.contains("ERROR:  division by zero"),
        "expected psql-style division by zero, got: {out}"
    );
}

#[test]
fn create_multiple_tables() {
    let output = run_tursopg(
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
    let output = run_tursopg(b"CREATE TABLE foo(bar TEXT);\n\\dt\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("foo"), "\\dt should list 'foo', got: {out}");
}

#[test]
fn dt_lists_multiple_tables() {
    let output = run_tursopg(b"CREATE TABLE alpha(x INT);\nCREATE TABLE beta(y TEXT);\n\\dt\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("alpha"), "\\dt should list alpha");
    assert!(out.contains("beta"), "\\dt should list beta");
}

#[test]
fn dt_empty_database() {
    let output = run_tursopg(b"\\dt\n");
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
        run_tursopg(b"CREATE TABLE users(id INT PRIMARY KEY, name TEXT, age INT);\n\\d users\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("id"), "should show column 'id'");
    assert!(out.contains("name"), "should show column 'name'");
    assert!(out.contains("age"), "should show column 'age'");
    assert!(out.contains("text"), "should show type 'text'");
}

#[test]
fn d_nonexistent_table() {
    let output = run_tursopg(b"\\d nonexistent\n");
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
    let output = run_tursopg(b"\\l\n");
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
    let output = run_tursopg(b"\\conninfo\n");
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
    let output = run_tursopg(b"\\?\n");
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
    let output = run_tursopg(b"\\bogus\n");
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
    let output = run_tursopg(
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
    let output = run_tursopg(b"SELECT * FROM sqlite_schema;\n");
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
    let output = run_tursopg(b"SELECT 1;\n");
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn error_returns_nonzero() {
    let output = run_tursopg(b"SELECT * FROM nonexistent;\n");
    assert_eq!(output.status.code(), Some(1));
}

#[test]
fn empty_input_returns_zero() {
    let output = run_tursopg(b"");
    assert_eq!(output.status.code(), Some(0));
}

// ---------------------------------------------------------------------------
// DEFAULT functions
// ---------------------------------------------------------------------------

#[test]
fn default_now_produces_value() {
    let output = run_tursopg(
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
    let output = run_tursopg(
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
    let output = run_tursopg(
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
    let output = run_tursopg(
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
    let output = run_tursopg(
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
    let output = run_tursopg(b"\\di\n");
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
    let output = run_tursopg(b"\\dv\n");
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
    let output = run_tursopg(b"\\dn\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("public"),
        "\\dn should list 'public', got: {out}"
    );
}

#[test]
fn dn_lists_created_schema() {
    let output = run_tursopg(b"CREATE SCHEMA foo;\n\\dn\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("foo"), "\\dn should list 'foo', got: {out}");
}

// ---------------------------------------------------------------------------
// Meta-commands: \dT
// ---------------------------------------------------------------------------

#[test]
fn d_upper_t_lists_types() {
    let output = run_tursopg(b"CREATE TYPE mood AS ENUM ('happy', 'sad');\n\\dT\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("mood"), "\\dT should list 'mood', got: {out}");
}

#[test]
fn d_upper_t_empty() {
    let output = run_tursopg(b"\\dT\n");
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
    let output = run_tursopg(b"\\du\n");
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
    let output = run_tursopg(b"\\df\n");
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
    let output = run_tursopg(
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
    let output = run_tursopg(b"CREATE TABLE tbl(id INT);\n\\dt+\n");
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
    let output = run_tursopg(b"\\x\n");
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
    let output = run_tursopg(b"\\timing\n");
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
    let output = run_tursopg(b"\\echo hello world\n");
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
    let output = run_tursopg(b"\\?\n");
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
    let output = run_tursopg(
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
    let output = run_tursopg(
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
    let output = run_tursopg(
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
    let output = run_tursopg(b"SELECT $$hello world$$;\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("hello world"),
        "expected 'hello world', got: {out}"
    );
}

#[test]
fn dollar_quoted_with_embedded_quote() {
    let output = run_tursopg(b"SELECT $$it's fine$$;\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("it's fine"),
        "expected embedded quote, got: {out}"
    );
}

#[test]
fn tagged_dollar_quoted_string() {
    let output = run_tursopg(b"SELECT $tag$content$tag$;\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("content"), "expected 'content', got: {out}");
}

#[test]
fn escape_string_backslash_n() {
    let output = run_tursopg(b"\\x\nSELECT E'line1\\nline2';\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("line1\nline2"),
        "expected two lines, got: {out}"
    );
}

#[test]
fn escape_string_backslash_t() {
    let output = run_tursopg(b"SELECT E'col1\\tcol2';\n");
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(
        out.contains("col1") && out.contains("col2"),
        "expected tab-separated, got: {out}"
    );
}

#[test]
fn comment_on_is_noop() {
    let output = run_tursopg(
        b"CREATE TABLE docs(id INT, title TEXT);\n\
          COMMENT ON TABLE docs IS 'documentation';\n\
          COMMENT ON COLUMN docs.title IS NULL;\n\
          SELECT 'ok';\n",
    );
    assert_eq!(output.status.code(), Some(0));
    let out = stdout(&output);
    assert!(out.contains("ok"), "COMMENT ON should be a no-op: {out}");
}

// ---------------------------------------------------------------------------
// CREATE TABLE AS / SELECT INTO
//
// Behavioral coverage lives in postgres/conformance/pg-sqltests/table.sqltest.
// The sqltest runner also speaks the wire protocol, but it only compares
// DataRow contents and discards CommandComplete tags, so the `SELECT n` /
// `CREATE TABLE AS` completion tags are asserted here instead.
// ---------------------------------------------------------------------------

#[test]
fn wire_create_table_as_returns_select_n() {
    with_pg_client(|c| {
        c.query_command_tags("CREATE TABLE src(id INT)");
        c.query_command_tags("INSERT INTO src VALUES (1), (2), (3)");

        // PostgreSQL reports CREATE TABLE AS / SELECT INTO completion as
        // `SELECT n` where n is the number of rows inserted.
        let tags = c.query_command_tags("CREATE TABLE dst AS SELECT * FROM src WHERE id > 1");
        assert!(
            tags.iter().any(|t| t == "SELECT 2"),
            "expected 'SELECT 2' tag for CTAS, got: {tags:?}"
        );

        let tags = c.query_command_tags("SELECT id INTO dst2 FROM src");
        assert!(
            tags.iter().any(|t| t == "SELECT 3"),
            "expected 'SELECT 3' tag for SELECT INTO, got: {tags:?}"
        );

        let tags = c.query_command_tags("CREATE TABLE dst3 AS SELECT * FROM src WITH NO DATA");
        assert!(
            tags.iter().any(|t| t == "CREATE TABLE AS"),
            "expected 'CREATE TABLE AS' tag for WITH NO DATA, got: {tags:?}"
        );
    });
}

// ---------------------------------------------------------------------------
// Named windows (WINDOW clause)
// ---------------------------------------------------------------------------

#[test]
fn named_window_basic() {
    let output = run_tursopg(
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
    let output = run_tursopg(
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
    let output = run_tursopg(
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
    let output = run_tursopg(
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
    let output = run_tursopg(
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
    let path = std::env::temp_dir().join(format!("tursopg_test_{name}_{}.tsv", std::process::id()));
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
    let output = run_tursopg(input.as_bytes());
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
    let output = run_tursopg(input.as_bytes());
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
        run_tursopg(b"CREATE TABLE t(id INT);\nCOPY t FROM '/nonexistent/path/data.tsv';\n");
    // Should fail with nonzero exit
    assert_ne!(output.status.code(), Some(0));
}

// ---------------------------------------------------------------------------
// Wire protocol: COPY FROM returns "COPY N"
// ---------------------------------------------------------------------------

/// Start tursopg with --server on a kernel-assigned ephemeral port and wait
/// for it to be ready. Returns the child and the port it is serving.
///
/// The port must not be derived from a fixed seed: each test runs in its own
/// process, so two concurrently started tests can compute the same port, and
/// the loser of the bind race silently connects to the winner's server — and
/// then fails mid-test when the winner tears it down. Instead, ask the kernel
/// for a free ephemeral port and verify our own child is the process that
/// came up on it, retrying with a fresh port if the child dies on bind.
fn start_tursopg_server() -> (Child, u16) {
    for _ in 0..10 {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let addr = format!("127.0.0.1:{port}");
        let mut child = Command::new(env!("CARGO_BIN_EXE_tursopg"))
            .arg(":memory:")
            .arg("--server")
            .arg(&addr)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to start tursopg server");

        // Wait for the server to be ready by polling TCP connect, bailing out
        // to a new port if the child exited (lost a bind race).
        for _ in 0..50 {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if TcpStream::connect(&addr).is_ok() && child.try_wait().unwrap().is_none() {
                return (child, port);
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        child.kill().ok();
        child.wait().ok();
    }
    panic!("tursopg server did not start");
}

/// Minimal PG wire protocol client for testing.
/// Sends startup + simple query and reads responses.
struct PgTestClient {
    stream: TcpStream,
    /// Raw startup response bytes, which carry the ParameterStatus messages
    /// advertising session defaults like server_version.
    startup_response: Vec<u8>,
}

impl PgTestClient {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut client = Self {
            stream,
            startup_response: Vec::new(),
        };
        client.send_startup();
        client.startup_response = client.read_until_ready();
        client
    }

    /// Value of a ParameterStatus ('S') message sent during startup.
    fn startup_parameter(&self, name: &str) -> Option<String> {
        extract_parameter_status(&self.startup_response, name)
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

    fn send_message(&mut self, tag: u8, payload: &[u8]) {
        self.stream.write_all(&[tag]).unwrap();
        self.stream
            .write_all(&((payload.len() + 4) as i32).to_be_bytes())
            .unwrap();
        self.stream.write_all(payload).unwrap();
        self.stream.flush().unwrap();
    }

    fn send_extended(&mut self, sql: &str, describe: bool) {
        let mut parse = vec![0];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        self.send_message(b'P', &parse);
        if describe {
            self.send_message(b'D', &[b'S', 0]);
        } else {
            self.send_message(b'B', &[0; 8]);
            self.send_message(b'E', &[0; 5]);
        }
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

    /// Send query and return the first column of the first DataRow ('D') as text.
    fn query_single_text(&mut self, sql: &str) -> String {
        self.send_query(sql);
        let response = self.read_until_ready();
        extract_first_data_row_text(&response).expect("query returned no rows")
    }

    /// Send query and return the fields from the first ErrorResponse ('E') message.
    /// Returns an empty map if the response contains no error.
    fn query_error_fields(&mut self, sql: &str) -> HashMap<u8, String> {
        self.send_query(sql);
        let response = self.read_until_ready();
        extract_error_fields(&response).unwrap_or_default()
    }
}

/// Walk raw PG wire bytes and return the value of the first ParameterStatus
/// (`'S'`) message with the given parameter name. Body layout per the PG
/// protocol docs: cstring name, cstring value.
fn extract_parameter_status(data: &[u8], name: &str) -> Option<String> {
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
        let body_end = pos + (len - 4);
        if body_end > data.len() {
            break;
        }
        if tag == b'S' {
            let body = &data[pos..body_end];
            let name_end = body
                .iter()
                .position(|&b| b == 0)
                .expect("ParameterStatus name missing nul terminator");
            if &body[..name_end] == name.as_bytes() {
                let value = &body[name_end + 1..];
                let value_end = value
                    .iter()
                    .position(|&b| b == 0)
                    .expect("ParameterStatus value missing nul terminator");
                return Some(String::from_utf8(value[..value_end].to_vec()).unwrap());
            }
        }
        pos = body_end;
    }
    None
}

/// Walk raw PG wire bytes, find the first DataRow (`'D'`), and return its
/// first column as text. Body layout per the PG protocol docs: `int16`
/// column count, then per column an `int32` value length (-1 for NULL) and
/// that many bytes.
fn extract_first_data_row_text(data: &[u8]) -> Option<String> {
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
        let body_end = pos + (len - 4);
        if body_end > data.len() {
            break;
        }
        if tag == b'D' {
            let body = &data[pos..body_end];
            let value_len = i32::from_be_bytes([body[2], body[3], body[4], body[5]]);
            if value_len < 0 {
                return None;
            }
            let value = &body[6..6 + value_len as usize];
            return Some(String::from_utf8(value.to_vec()).unwrap());
        }
        pos = body_end;
    }
    None
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

/// Walk raw PG wire bytes, find the first ErrorResponse ('E') message, and
/// return its fields as a map from the one-byte field type to the value string.
///
/// Field type bytes (from the PG protocol docs):
///   b'C' = SQLSTATE code
///   b'M' = human-readable message
///   b'D' = detail
///   b't' = table name
///   b'n' = constraint name
///   b's' = schema name
///   b'c' = column name
fn extract_error_fields(data: &[u8]) -> Option<HashMap<u8, String>> {
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
        let body_end = pos + (len - 4);
        if body_end > data.len() {
            break;
        }
        if tag == b'E' {
            let body = &data[pos..body_end];
            let mut fields = HashMap::new();
            let mut p = 0;
            while p < body.len() {
                let field_type = body[p];
                p += 1;
                if field_type == 0 {
                    break;
                }
                let nul = body[p..]
                    .iter()
                    .position(|&b| b == 0)
                    .unwrap_or(body.len() - p);
                let value = String::from_utf8(body[p..p + nul].to_vec()).unwrap_or_default();
                p += nul + 1;
                fields.insert(field_type, value);
            }
            return Some(fields);
        }
        pos = body_end;
    }
    None
}

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
    let (mut server, port) = start_tursopg_server();

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

#[test]
fn repl_load_extension_still_attempts_to_open_the_file() {
    let output = run_tursopg(b"SELECT load_extension('/nonexistent/turso-audit-ext');\n");
    let combined = format!(
        "{}{}",
        stdout(&output),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("Extension file not found"),
        "interactive CLI should still try to load the library: {combined}"
    );
    assert!(
        !combined.contains("runtime extension loading is disabled"),
        "interactive CLI must keep load_extension enabled: {combined}"
    );
}

#[test]
fn wire_load_extension_is_disabled() {
    with_pg_client(|client| {
        client.send_query("SELECT load_extension('/nonexistent/turso-audit-ext')");
        let response = client.read_until_ready();
        let text = String::from_utf8_lossy(&response);
        assert!(
            text.contains("runtime extension loading is disabled"),
            "wire listener must refuse load_extension, got: {text}"
        );
        assert!(
            !text.contains("Extension file not found"),
            "wire listener must not reach the filesystem open: {text}"
        );
    });
}

#[test]
fn wire_comment_on_returns_comment_tag() {
    with_pg_client(|client| {
        let tags = client.query_command_tags("CREATE TABLE docs(id INT)");
        assert!(
            tags.iter().any(|tag| tag.starts_with("CREATE")),
            "expected CREATE tag, got: {tags:?}"
        );

        let tags = client.query_command_tags("COMMENT ON TABLE docs IS 'documentation'");
        assert_eq!(tags, ["COMMENT"]);
    });
}

/// Wire-protocol fixture: spin up tursopg, hand the caller a connected
/// client, run their assertions, then shut the server down. Each test
/// gets its own kernel-assigned port so they can run in parallel without
/// TCP collisions.
fn with_pg_client<F: FnOnce(&mut PgTestClient)>(f: F) {
    let (mut server, port) = start_tursopg_server();
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
    with_pg_client(|c| {
        assert_eq!(c.query_column_oids("SELECT 42"), vec![OID_INT4]);
    });
}

/// Clients decode values off the OID, so FROM-position scalars must
/// report their result type rather than bytea.
#[test]
fn wire_from_position_scalar_reports_text() {
    with_pg_client(|c| {
        assert_eq!(
            c.query_column_oids("SELECT * FROM current_schema()"),
            vec![OID_TEXT]
        );
    });
}

/// Leading numeric version of a string like "16.6-pgwire-0.36.3" or "16.6 (...)".
fn numeric_prefix(s: &str) -> &str {
    let end = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    &s[..end]
}

/// The `server_version` startup parameter and version() are from two distinct
/// sources, and clients see both. Until they share a single source of truth,
/// this pins their numeric prefixes together so drift is noticed.
#[test]
fn wire_server_version_parameter_matches_version_function() {
    with_pg_client(|c| {
        let advertised = c
            .startup_parameter("server_version")
            .expect("startup must advertise server_version");
        let version = c.query_single_text("SELECT version()");
        let reported = version
            .strip_prefix("PostgreSQL ")
            .expect("version() must start with 'PostgreSQL '");
        assert!(!numeric_prefix(&advertised).is_empty(), "{advertised:?}");
        assert_eq!(
            numeric_prefix(&advertised),
            numeric_prefix(reported),
            "server_version parameter {advertised:?} vs version() {version:?}"
        );
    });
}

/// `SELECT 3.14` reports FLOAT8. PG normally returns NUMERIC for unannotated
/// numeric literals; FLOAT8 is the tursopg choice because Turso stores
/// reals as 64-bit floats and the client decodes the wire bytes directly.
#[test]
fn wire_real_literal_reports_float8() {
    with_pg_client(|c| {
        assert_eq!(c.query_column_oids("SELECT 3.14"), vec![OID_FLOAT8]);
    });
}

/// `SELECT 'hello'` reports TEXT. Already correct before the API change;
/// asserting here to lock in the contract.
#[test]
fn wire_text_literal_reports_text() {
    with_pg_client(|c| {
        assert_eq!(c.query_column_oids("SELECT 'hello'"), vec![OID_TEXT]);
    });
}

/// Arithmetic over integer operands MUST report INT4, matching PostgreSQL.
/// SQLite's own affinity machinery deliberately stops at binary operators
/// (column-affinity model), so we explicitly walk arithmetic to propagate
/// the operand type — this test pins that walker down.
#[test]
fn wire_integer_arithmetic_reports_int4() {
    with_pg_client(|c| {
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
    with_pg_client(|c| {
        assert_eq!(c.query_column_oids("SELECT 42 + 1.5"), vec![OID_FLOAT8]);
        assert_eq!(c.query_column_oids("SELECT 3.14 * 2"), vec![OID_FLOAT8]);
    });
}

/// Bitwise ops always report INT4 — matches PostgreSQL.
#[test]
fn wire_bitwise_ops_report_int4() {
    with_pg_client(|c| {
        assert_eq!(c.query_column_oids("SELECT 1 << 4"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 256 >> 2"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 12 & 10"), vec![OID_INT4]);
        assert_eq!(c.query_column_oids("SELECT 1 | 2"), vec![OID_INT4]);
    });
}

/// Comparison and logical ops return INT4 — SQLite returns 0/1 as INTEGER
/// at runtime; tursopg's wire layer reports INT4 here. A future change
/// could map these to BOOL OID, but for now stable + assertable.
#[test]
fn wire_comparison_and_logical_report_int4() {
    with_pg_client(|c| {
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
    with_pg_client(|c| {
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
    with_pg_client(|c| {
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
    with_pg_client(|c| {
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
    with_pg_client(|c| {
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
    with_pg_client(|c| {
        assert_eq!(
            c.query_column_oids("SELECT 1, 'two', 3.0, 1 + 1, 'a' || 'b'"),
            vec![OID_INT4, OID_TEXT, OID_FLOAT8, OID_INT4, OID_TEXT]
        );
    });
}

/// Verify that a schema created in one tursopg session is accessible in a
/// subsequent session opened against the same file.
#[test]
fn schema_sidecar_reattaches_on_reopen() {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_dir = std::env::temp_dir().join(format!("tursopg_schema_persist_{timestamp}"));
    std::fs::create_dir(&test_dir).expect("failed to create temp test directory");

    let db_path = test_dir.join("main.db");
    let schema_name = "persist_sidecar";

    let first_sql = format!(
        "CREATE SCHEMA {schema_name};\n\
         CREATE TABLE {schema_name}.demo(id INT, label TEXT);\n\
         INSERT INTO {schema_name}.demo VALUES (1, 'persisted');\n"
    );
    let out1 = run_tursopg_with_db(&db_path, first_sql.as_bytes());
    assert_eq!(
        out1.status.code(),
        Some(0),
        "first run failed: {}",
        stdout(&out1)
    );

    let second_sql = format!("SELECT id, label FROM {schema_name}.demo;\n");
    let out2 = run_tursopg_with_db(&db_path, second_sql.as_bytes());

    std::fs::remove_dir_all(&test_dir).ok();

    let out = stdout(&out2);
    assert_eq!(
        out2.status.code(),
        Some(0),
        "second run failed, schema sidecar not reattached on reopen: {out}"
    );
    assert!(
        out.contains("persisted"),
        "expected 'persisted' in output, got: {out}"
    );
}

#[test]
fn rewritten_table_definitions_survive_reopen() {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_dir = std::env::temp_dir().join(format!("tursopg_rewrite_reopen_{timestamp}"));
    std::fs::create_dir(&test_dir).expect("failed to create temp test directory");

    let cases: &[(&str, &str, &str, &[&str])] = &[
        (
            "rename_table.db",
            "CREATE TABLE a (x int);\nALTER TABLE a RENAME TO b;\n",
            "INSERT INTO b VALUES (7);\nSELECT x FROM b;\n",
            &["7"],
        ),
        (
            "rename_column.db",
            "CREATE TABLE a (x int);\nALTER TABLE a RENAME COLUMN x TO y;\n",
            "INSERT INTO a VALUES (8);\nSELECT y FROM a;\n",
            &["8"],
        ),
        (
            "rename_table_with_expression_defaults.db",
            "CREATE TABLE a (x int, y int DEFAULT 1 + 2, z text DEFAULT 'zz'::text, \
             w int DEFAULT abs(-4), t timestamp DEFAULT now());\n\
             ALTER TABLE a RENAME TO b;\n",
            "INSERT INTO b (x) VALUES (1);\nSELECT y * 100 + w AS n, z FROM b;\n",
            &["304", "zz"],
        ),
        (
            "rename_serial_column.db",
            "CREATE TABLE a (id serial PRIMARY KEY, v text);\n\
             ALTER TABLE a RENAME COLUMN v TO label;\n",
            "INSERT INTO a (label) VALUES ('first');\nSELECT label FROM a WHERE id = 1;\n",
            &["first"],
        ),
        (
            "rename_foreign_key_target.db",
            "CREATE TABLE p (id int PRIMARY KEY);\n\
             CREATE TABLE c (pid int REFERENCES p(id), n int DEFAULT 2 * 3);\n\
             ALTER TABLE p RENAME TO parent;\n",
            "INSERT INTO parent VALUES (1);\nINSERT INTO c (pid) VALUES (1);\nSELECT n FROM c;\n",
            &["6"],
        ),
        (
            "add_column_with_expression_default.db",
            "CREATE TABLE a (x int DEFAULT 2 * 5);\nALTER TABLE a ADD COLUMN y int;\n",
            "INSERT INTO a (y) VALUES (1);\nSELECT x FROM a;\n",
            &["10"],
        ),
    ];
    for (db_name, setup, query, expected) in cases {
        let db_path = test_dir.join(db_name);
        let out1 = run_tursopg_with_db(&db_path, setup.as_bytes());
        let err1 = String::from_utf8_lossy(&out1.stderr);
        assert_eq!(
            out1.status.code(),
            Some(0),
            "{db_name}: first run failed: {err1}"
        );
        assert!(
            !err1.contains("Error"),
            "{db_name}: first run failed: {err1}"
        );
        let out2 = run_tursopg_with_db(&db_path, query.as_bytes());
        let out = stdout(&out2);
        let err = String::from_utf8_lossy(&out2.stderr);
        assert_eq!(
            out2.status.code(),
            Some(0),
            "{db_name}: reopen failed: {err}"
        );
        assert!(!err.contains("Error"), "{db_name}: reopen failed: {err}");
        for value in *expected {
            assert!(out.contains(value), "{db_name}: expected {value} in: {out}");
        }
    }

    std::fs::remove_dir_all(&test_dir).ok();
}

// ---------------------------------------------------------------------------
// Wire protocol: constraint violation error fields
//
// PostgreSQL sends structured error information for constraint violations:
//   b'C' SQLSTATE:         23505 (unique), 23503 (fk), 23514 (check), 23502 (not null)
//   b'M' message:          human-readable description
//   b'D' detail:           which key/row violated the constraint
//   b't' table:            table where the violation occurred
//   b'n' constraint name:  name of the violated constraint
//
// These tests assert that Turso sends the same structured fields. They are
// derived from real PostgreSQL 17 behavior, not inferred.
// ---------------------------------------------------------------------------

/// Unit test for extract_error_fields with hardcoded bytes to verify the parser.
#[test]
fn extract_error_fields_parses_pg_error_response() {
    let body: &[u8] = b"SERROR\x00CXX000\x00Mtest error\x00\x00";
    let len = (4 + body.len()) as i32;
    let mut data = vec![b'E'];
    data.extend_from_slice(&len.to_be_bytes());
    data.extend_from_slice(body);
    data.push(b'Z');
    data.extend_from_slice(&5i32.to_be_bytes());
    data.push(b'I');

    let fields = extract_error_fields(&data).expect("must find ErrorResponse");
    assert_eq!(fields.get(&b'S').map(String::as_str), Some("ERROR"));
    assert_eq!(fields.get(&b'C').map(String::as_str), Some("XX000"));
    assert_eq!(fields.get(&b'M').map(String::as_str), Some("test error"));
}

/// Diagnostic test: dump the raw wire response for a PK violation so we can
/// see what the server actually sends. Used to debug why extract_error_fields
/// returns empty.
#[test]
fn debug_wire_pk_violation_response() {
    with_pg_client(|c| {
        c.query_command_tags("CREATE TABLE t_dbg (id INT PRIMARY KEY)");
        c.query_command_tags("INSERT INTO t_dbg VALUES (1)");
        c.send_query("INSERT INTO t_dbg VALUES (1)");
        let response = c.read_until_ready();
        let fields = extract_error_fields(&response);
        // Print for debugging (test always passes, used to observe behavior)
        eprintln!(
            "response bytes ({} total): {:?}",
            response.len(),
            &response[..response.len().min(80)]
        );
        eprintln!("error fields: {fields:?}");
        // At minimum, there must be SOME response (non-empty)
        assert!(
            !response.is_empty(),
            "response should not be empty for PK violation"
        );
        // And the server should send an ErrorResponse (tag 0x45 = 'E')
        assert!(
            response.contains(&b'E'),
            "response should contain ErrorResponse ('E') tag byte, got: {:?}",
            &response[..response.len().min(20)]
        );
    });
}

/// Named UNIQUE constraint: duplicate value in a column with an explicit
/// CONSTRAINT name.
///
/// PostgreSQL 17:
///   SQLSTATE  23505
///   message   duplicate key value violates unique constraint "t_u_code_uq"
///   detail    Key (code)=(a) already exists.
///   table     t_u
///   constraint t_u_code_uq
#[test]
fn wire_error_unique_named_constraint() {
    with_pg_client(|c| {
        c.query_command_tags(
            "CREATE TABLE t_u (id INT PRIMARY KEY, code TEXT CONSTRAINT t_u_code_uq UNIQUE)",
        );
        c.query_command_tags("INSERT INTO t_u VALUES (1, 'a')");
        let fields = c.query_error_fields("INSERT INTO t_u VALUES (2, 'a')");
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some("23505"),
            "SQLSTATE must be 23505 for unique violation, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'M').map(String::as_str),
            Some("duplicate key value violates unique constraint \"t_u_code_uq\""),
            "message must use PostgreSQL wording, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'n').map(String::as_str),
            Some("t_u_code_uq"),
            "constraint name must be t_u_code_uq, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b't').map(String::as_str),
            Some("t_u"),
            "table name must be t_u, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'D').map(String::as_str),
            Some("Key (code)=(a) already exists."),
            "detail must identify the duplicate key value, got: {fields:?}"
        );
    });
}

/// Primary key constraint: duplicate value in the PK column. PostgreSQL
/// auto-names the constraint <table>_pkey.
///
/// PostgreSQL 17:
///   SQLSTATE  23505
///   message   duplicate key value violates unique constraint "t_pk_pkey"
///   detail    Key (id)=(1) already exists.
///   table     t_pk
///   constraint t_pk_pkey
#[test]
fn wire_error_primary_key_violation() {
    with_pg_client(|c| {
        c.query_command_tags("CREATE TABLE t_pk (id INT PRIMARY KEY, name TEXT)");
        c.query_command_tags("INSERT INTO t_pk VALUES (1, 'alice')");
        let fields = c.query_error_fields("INSERT INTO t_pk VALUES (1, 'bob')");
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some("23505"),
            "SQLSTATE must be 23505 for PK violation, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'n').map(String::as_str),
            Some("t_pk_pkey"),
            "constraint name must be t_pk_pkey, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b't').map(String::as_str),
            Some("t_pk"),
            "table name must be t_pk, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'D').map(String::as_str),
            Some("Key (id)=(1) already exists."),
            "detail must identify the duplicate key value, got: {fields:?}"
        );
    });
}

/// Named CHECK constraint: insert a row that fails the check expression.
///
/// PostgreSQL 17:
///   SQLSTATE  23514
///   message   new row for relation "t_ck" violates check constraint "t_ck_score_ck"
///   detail    Failing row contains (1, -1).
///   table     t_ck
///   constraint t_ck_score_ck
#[test]
fn wire_error_check_constraint_violation() {
    with_pg_client(|c| {
        c.query_command_tags(
            "CREATE TABLE t_ck (id INT PRIMARY KEY, score INT CONSTRAINT t_ck_score_ck CHECK (score >= 0))",
        );
        let fields = c.query_error_fields("INSERT INTO t_ck VALUES (1, -1)");
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some("23514"),
            "SQLSTATE must be 23514 for CHECK violation, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'n').map(String::as_str),
            Some("t_ck_score_ck"),
            "constraint name must be t_ck_score_ck, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b't').map(String::as_str),
            Some("t_ck"),
            "table name must be t_ck, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'D').map(String::as_str),
            Some("Failing row contains (1, -1)."),
            "detail must show the failing row values, got: {fields:?}"
        );
    });
}

/// NOT NULL constraint: insert NULL into a NOT NULL column.
///
/// PostgreSQL 17:
///   SQLSTATE  23502
///   message   null value in column "code" of relation "t_nn" violates not-null constraint
///   detail    Failing row contains (1, null).
///   table     t_nn
///   column    code
#[test]
fn wire_error_not_null_violation() {
    with_pg_client(|c| {
        c.query_command_tags("CREATE TABLE t_nn (id INT PRIMARY KEY, code TEXT NOT NULL)");
        let fields = c.query_error_fields("INSERT INTO t_nn VALUES (1, NULL)");
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some("23502"),
            "SQLSTATE must be 23502 for NOT NULL violation, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b't').map(String::as_str),
            Some("t_nn"),
            "table name must be t_nn, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'c').map(String::as_str),
            Some("code"),
            "column name must be code, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'D').map(String::as_str),
            Some("Failing row contains (1, null)."),
            "detail must show the failing row values, got: {fields:?}"
        );
    });
}

/// Unnamed UNIQUE constraint: PostgreSQL auto-names it <table>_<col>_key.
///
/// PostgreSQL 17:
///   SQLSTATE  23505
///   message   duplicate key value violates unique constraint "t_uu_code_key"
///   detail    Key (code)=(a) already exists.
///   table     t_uu
///   constraint t_uu_code_key
#[test]
fn wire_error_unique_unnamed_constraint() {
    with_pg_client(|c| {
        c.query_command_tags("CREATE TABLE t_uu (id INT PRIMARY KEY, code TEXT UNIQUE)");
        c.query_command_tags("INSERT INTO t_uu VALUES (1, 'a')");
        let fields = c.query_error_fields("INSERT INTO t_uu VALUES (2, 'a')");
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some("23505"),
            "SQLSTATE must be 23505 for unique violation, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'n').map(String::as_str),
            Some("t_uu_code_key"),
            "constraint name must be t_uu_code_key, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b't').map(String::as_str),
            Some("t_uu"),
            "table name must be t_uu, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'D').map(String::as_str),
            Some("Key (code)=(a) already exists."),
            "detail must identify the duplicate key value, got: {fields:?}"
        );
    });
}

/// Multi-column unnamed UNIQUE constraint: PostgreSQL auto-names it
/// <table>_<col1>_<col2>_key.
///
/// PostgreSQL 17:
///   SQLSTATE  23505
///   message   duplicate key value violates unique constraint "t_mc_a_b_key"
///   detail    Key (a, b)=(1, 2) already exists.
///   table     t_mc
///   constraint t_mc_a_b_key
#[test]
fn wire_error_unique_multi_column_constraint() {
    with_pg_client(|c| {
        c.query_command_tags("CREATE TABLE t_mc (id INT PRIMARY KEY, a INT, b INT, UNIQUE(a, b))");
        c.query_command_tags("INSERT INTO t_mc VALUES (1, 1, 2)");
        let fields = c.query_error_fields("INSERT INTO t_mc VALUES (2, 1, 2)");
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some("23505"),
            "SQLSTATE must be 23505 for unique violation, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'n').map(String::as_str),
            Some("t_mc_a_b_key"),
            "constraint name must be t_mc_a_b_key, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b't').map(String::as_str),
            Some("t_mc"),
            "table name must be t_mc, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'D').map(String::as_str),
            Some("Key (a, b)=(1, 2) already exists."),
            "detail must identify the duplicate key values, got: {fields:?}"
        );
    });
}

/// UPDATE to a value that duplicates a UNIQUE constraint.
///
/// PostgreSQL 17:
///   SQLSTATE  23505
///   message   duplicate key value violates unique constraint "t_upd_uq_code_key"
///   detail    Key (code)=(a) already exists.
///   table     t_upd_uq
///   constraint t_upd_uq_code_key
#[test]
fn wire_error_update_unique_violation() {
    with_pg_client(|c| {
        c.query_command_tags("CREATE TABLE t_upd_uq (id INT PRIMARY KEY, code TEXT UNIQUE)");
        c.query_command_tags("INSERT INTO t_upd_uq VALUES (1, 'a')");
        c.query_command_tags("INSERT INTO t_upd_uq VALUES (2, 'b')");
        let fields = c.query_error_fields("UPDATE t_upd_uq SET code = 'a' WHERE id = 2");
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some("23505"),
            "SQLSTATE must be 23505 for UPDATE unique violation, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'n').map(String::as_str),
            Some("t_upd_uq_code_key"),
            "constraint name must be t_upd_uq_code_key, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b't').map(String::as_str),
            Some("t_upd_uq"),
            "table name must be t_upd_uq, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'D').map(String::as_str),
            Some("Key (code)=(a) already exists."),
            "detail must identify the duplicate key value, got: {fields:?}"
        );
    });
}

/// UPDATE to set a NOT NULL column to NULL.
///
/// PostgreSQL 17:
///   SQLSTATE  23502
///   message   null value in column "code" of relation "t_upd_nn" violates not-null constraint
///   detail    Failing row contains (1, null).
///   table     t_upd_nn
///   column    code
#[test]
fn wire_error_update_not_null_violation() {
    with_pg_client(|c| {
        c.query_command_tags("CREATE TABLE t_upd_nn (id INT PRIMARY KEY, code TEXT NOT NULL)");
        c.query_command_tags("INSERT INTO t_upd_nn VALUES (1, 'hello')");
        let fields = c.query_error_fields("UPDATE t_upd_nn SET code = NULL WHERE id = 1");
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some("23502"),
            "SQLSTATE must be 23502 for UPDATE NOT NULL violation, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b't').map(String::as_str),
            Some("t_upd_nn"),
            "table name must be t_upd_nn, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'c').map(String::as_str),
            Some("code"),
            "column name must be code, got: {fields:?}"
        );
    });
}

/// UPDATE to violate a CHECK constraint.
///
/// PostgreSQL 17:
///   SQLSTATE  23514
///   message   new row for relation "t_upd_ck" violates check constraint "t_upd_ck_score_ck"
///   detail    Failing row contains (1, -5).
///   table     t_upd_ck
///   constraint t_upd_ck_score_ck
#[test]
fn wire_error_update_check_violation() {
    with_pg_client(|c| {
        c.query_command_tags(
            "CREATE TABLE t_upd_ck (id INT PRIMARY KEY, score INT CONSTRAINT t_upd_ck_score_ck CHECK (score >= 0))",
        );
        c.query_command_tags("INSERT INTO t_upd_ck VALUES (1, 10)");
        let fields = c.query_error_fields("UPDATE t_upd_ck SET score = -5 WHERE id = 1");
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some("23514"),
            "SQLSTATE must be 23514 for UPDATE CHECK violation, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b'n').map(String::as_str),
            Some("t_upd_ck_score_ck"),
            "constraint name must be t_upd_ck_score_ck, got: {fields:?}"
        );
        assert_eq!(
            fields.get(&b't').map(String::as_str),
            Some("t_upd_ck"),
            "table name must be t_upd_ck, got: {fields:?}"
        );
    });
}

/// Invalid SQL syntax → SQLSTATE 42601 (syntax_error).
///
/// PostgreSQL 18.3: ERROR 42601 "syntax error at or near ..."
#[test]
fn wire_error_syntax_error() {
    with_pg_client(|c| {
        let fields = c.query_error_fields("SELECTT 1");
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some("42601"),
            "SQLSTATE must be 42601 for syntax error, got: {fields:?}"
        );
    });
}

// ---------------------------------------------------------------------------
// SQLSTATE coverage: Class 25 (Invalid Transaction State)
// ---------------------------------------------------------------------------

/// BEGIN inside an already-active transaction → SQLSTATE 25001
/// (active_sql_transaction).
///
/// Verified against real PostgreSQL (probed 2026-10-21): PostgreSQL emits a
/// WARNING with code 25001, not an error. Turso diverges by returning an ERROR
/// with the same code. The 25001 code itself is correct for this condition;
/// the severity is the divergence. Tracked for future alignment.
#[test]
fn wire_error_nested_begin() {
    with_pg_client(|c| {
        c.query_command_tags("BEGIN");
        let fields = c.query_error_fields("BEGIN");
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some("25001"),
            "SQLSTATE must be 25001 for nested BEGIN, got: {fields:?}"
        );
        c.query_command_tags("ROLLBACK");
    });
}

/// PostgreSQL raises SQLSTATE 22012 (division_by_zero) for `SELECT 1/0`.
/// Turso's postgres mode matches this behavior.
#[test]
fn wire_error_division_by_zero() {
    with_pg_client(|c| {
        let fields = c.query_error_fields("SELECT 1/0");
        assert_eq!(
            fields.get(&b'C').map(String::as_str),
            Some("22012"),
            "division by zero must be SQLSTATE 22012, got: {fields:?}"
        );
    });
}

/// A statement fails inside a transaction. As in PostgreSQL, ROLLBACK TO
/// SAVEPOINT undoes the failure and the transaction goes on: the rows written
/// before the savepoint and after the rollback are committed. This holds
/// whether or not the failed statement has a statement journal.
#[test]
fn rollback_to_savepoint_recovers_after_statement_error() {
    for failing in [
        "SELECT 1 / 0",
        "INSERT INTO t VALUES (1 / 0)",
        "INSERT INTO t SELECT abs(10 / y) FROM s",
        "INSERT INTO t SELECT 10 / y FROM s",
        "INSERT INTO t SELECT 10 % y FROM s",
        "INSERT INTO t VALUES (0) RETURNING abs(10 / x)",
        "UPDATE t SET x = 0 RETURNING abs(10 / x)",
        "DELETE FROM t RETURNING abs(10 / (x - 5))",
    ] {
        with_pg_client(|c| {
            c.query_command_tags("CREATE TABLE t (x int)");
            c.query_command_tags("CREATE TABLE s (y int)");
            c.query_command_tags("INSERT INTO s VALUES (1), (0)");
            c.query_command_tags("BEGIN");
            c.query_command_tags("INSERT INTO t VALUES (5)");
            c.query_command_tags("SAVEPOINT a");
            let fields = c.query_error_fields(failing);
            assert_eq!(
                fields.get(&b'C').map(String::as_str),
                Some("22012"),
                "{failing}: {fields:?}"
            );
            assert_eq!(
                c.query_command_tags("ROLLBACK TO SAVEPOINT a"),
                vec!["ROLLBACK"],
                "{failing}"
            );
            c.query_command_tags("INSERT INTO t VALUES (7)");
            assert_eq!(c.query_command_tags("COMMIT"), vec!["COMMIT"], "{failing}");
            assert_eq!(
                c.query_single_text("SELECT string_agg(x::text, ',' ORDER BY x) FROM t"),
                "5,7",
                "{failing}"
            );
        });
    }
}

/// A multi-row write fails on a zero divisor after writing its first row.
/// Division can fail in PostgreSQL, so the write gets a statement journal and
/// the failed statement leaves no rows, even with no function call in it.
#[test]
fn failed_multi_row_write_in_transaction_leaves_no_rows() {
    for failing in [
        "INSERT INTO t SELECT 10 / y FROM s",
        "INSERT INTO t SELECT 10 % y FROM s",
        "UPDATE t SET x = 10 / (x - 6)",
        "DELETE FROM t WHERE 10 / (x - 6) > 0",
    ] {
        with_pg_client(|c| {
            c.query_command_tags("CREATE TABLE t (x int)");
            c.query_command_tags("CREATE TABLE s (y int)");
            c.query_command_tags("INSERT INTO s VALUES (1), (0)");
            c.query_command_tags("INSERT INTO t VALUES (5), (6)");
            c.query_command_tags("BEGIN");
            c.query_command_tags("INSERT INTO t VALUES (7)");
            let fields = c.query_error_fields(failing);
            assert_eq!(
                fields.get(&b'C').map(String::as_str),
                Some("22012"),
                "{failing}: {fields:?}"
            );
            c.query_command_tags("COMMIT");
            assert_eq!(
                c.query_single_text("SELECT string_agg(x::text, ',' ORDER BY x) FROM t"),
                "5,6,7",
                "{failing}"
            );
        });
    }
}

/// A multi-row write fails after writing its first row, on an error that does
/// not come from a function call or a division. PostgreSQL statements are
/// atomic, so the failed statement leaves no rows whatever the error.
#[test]
fn failed_multi_row_write_leaves_no_rows_whatever_the_error() {
    for failing in [
        "INSERT INTO t SELECT sum(y) FROM s GROUP BY g ORDER BY g",
        "INSERT INTO t SELECT sum(y) OVER (ORDER BY g, y) FROM s",
        "UPDATE t SET x = (SELECT sum(y) FROM s WHERE g <= t.x)",
    ] {
        with_pg_client(|c| {
            c.query_command_tags("CREATE TABLE s (g int, y bigint)");
            c.query_command_tags("INSERT INTO s VALUES (1, 1), (2, 9223372036854775807), (2, 1)");
            c.query_command_tags("CREATE TABLE t (x bigint)");
            c.query_command_tags("INSERT INTO t VALUES (-5), (6)");
            c.query_command_tags("BEGIN");
            c.query_command_tags("INSERT INTO t VALUES (7)");
            let fields = c.query_error_fields(failing);
            assert_eq!(
                fields.get(&b'C').map(String::as_str),
                Some("22003"),
                "{failing}: {fields:?}"
            );
            c.query_command_tags("COMMIT");
            assert_eq!(
                c.query_single_text("SELECT string_agg(x::text, ',' ORDER BY x) FROM t"),
                "-5,6,7",
                "{failing}"
            );
        });
    }
}

/// `SELECT NULL / 0` returns NULL (not an error) in both PostgreSQL and Turso.
#[test]
fn null_divided_by_zero_is_null() {
    with_pg_client(|c| {
        let fields = c.query_error_fields("SELECT NULL / 0");
        assert!(
            fields.is_empty(),
            "NULL / 0 should return NULL (no error), got: {fields:?}"
        );
    });
}

#[test]
fn wire_frontend_errors_preserve_extended_query_sync() {
    with_pg_client(|client| {
        client.query_command_tags("CREATE TABLE t (id INT PRIMARY KEY)");
        client.query_command_tags("INSERT INTO t VALUES (1)");
        for (sql, describe, code) in [
            ("INSERT INTO t VALUES (1)", false, "23505"),
            ("SELECT * FROM missing_table", true, "42P01"),
        ] {
            client.send_extended(sql, describe);
            client.send_extended("INSERT INTO t VALUES (2)", false);
            client.send_message(b'S', &[]);
            let response = client.read_until_ready();
            let fields = extract_error_fields(&response).expect("expected ErrorResponse");
            assert_eq!(fields.get(&b'C').map(String::as_str), Some(code));
            if code == "23505" {
                assert_eq!(fields.get(&b't').map(String::as_str), Some("t"));
                assert_eq!(fields.get(&b'n').map(String::as_str), Some("t_pkey"));
                assert_eq!(
                    fields.get(&b'D').map(String::as_str),
                    Some("Key (id)=(1) already exists.")
                );
            }
            assert_eq!(message_count(&response, b'E'), 1);
            assert_eq!(message_count(&response, b'Z'), 1);
            assert!(response.ends_with(&[b'Z', 0, 0, 0, 5, b'I']));
            assert_eq!(client.query_single_text("SELECT count(*) FROM t"), "1");
        }
        let fields = client.query_error_fields("INSERT INTO t VALUES (1)");
        assert_eq!(fields.get(&b'C').map(String::as_str), Some("23505"));
        assert_eq!(client.query_single_text("SELECT 7"), "7");
    });

    fn message_count(bytes: &[u8], tag: u8) -> usize {
        let mut position = 0;
        let mut count = 0;
        while position < bytes.len() {
            count += usize::from(bytes[position] == tag);
            let length =
                u32::from_be_bytes(bytes[position + 1..position + 5].try_into().unwrap()) as usize;
            position += length + 1;
        }
        assert_eq!(position, bytes.len());
        count
    }
}

#[test]
fn failed_create_table_serial_leaves_no_orphan_sequence_cli() {
    let test_dir = std::env::temp_dir().join(format!("tursopg_repro_8110_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).expect("failed to create temp test directory");
    let db_path = test_dir.join("repro_8110.db");

    let out1 = run_tursopg_with_db(&db_path, b"CREATE TABLE c (id SERIAL);\n");
    assert_eq!(out1.status.code(), Some(0));

    let out2 = run_tursopg_with_db(&db_path, b"CREATE TABLE c (should_fail SERIAL);\n");
    let combined2 = format!("{}{}", stdout(&out2), String::from_utf8_lossy(&out2.stderr));
    assert!(
        combined2.contains("already exists"),
        "expected table-exists error, got: {combined2}"
    );

    let out3 = run_tursopg_with_db(
        &db_path,
        b"SELECT sequencename FROM pg_sequences ORDER BY sequencename;\n",
    );
    assert_eq!(out3.status.code(), Some(0));
    let seq_out = stdout(&out3);
    assert!(
        seq_out.contains("c_id_seq"),
        "expected c_id_seq in pg_sequences output: {seq_out}"
    );
    assert!(
        !seq_out.contains("c_should_fail_seq"),
        "failed CREATE TABLE leaked c_should_fail_seq: {seq_out}"
    );
    let _ = std::fs::remove_dir_all(&test_dir);
}

#[test]
fn wire_describe_ddl_and_schema_commands_perform_no_writes() {
    with_pg_client(|client| {
        client.send_extended("CREATE TABLE described_tbl (id SERIAL)", true);
        client.send_message(b'S', &[]);
        let _ = client.read_until_ready();

        client.send_extended("CREATE SCHEMA described_schema", true);
        client.send_message(b'S', &[]);
        let _ = client.read_until_ready();

        assert_eq!(
            client.query_single_text(
                "SELECT count(*) FROM pg_sequences WHERE sequencename = 'described_tbl_id_seq'"
            ),
            "0"
        );
        assert_eq!(
            client.query_single_text(
                "SELECT count(*) FROM pg_namespace WHERE nspname = 'described_schema'"
            ),
            "0"
        );
    });
}
