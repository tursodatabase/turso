use super::*;
use crate::SqliteDialect;
use crate::{Database, DatabaseOpts, MemoryIO, OpenFlags, IO};

fn open_test_connection() -> crate::Result<Arc<crate::Connection>> {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    db.connect()
}

#[test]
fn test_expanded_sql() {
    let conn = open_test_connection().unwrap();
    let mut stmt = conn
        .prepare("SELECT ?1, :nm, ':nm' /* ? */, -- ?\n?")
        .unwrap();

    // Unbound parameters render as NULL; markers inside string
    // literals and comments are untouched.
    assert_eq!(
        stmt.expanded_sql(),
        "SELECT NULL, NULL, ':nm' /* ? */, -- ?\nNULL"
    );

    stmt.bind_at(1.try_into().unwrap(), Value::from_i64(42))
        .unwrap();
    let nm = stmt.parameter_index(":nm").unwrap();
    stmt.bind_at(nm, Value::build_text("it's")).unwrap();
    stmt.bind_at(3.try_into().unwrap(), Value::from_slice(&[1, 2]).unwrap())
        .unwrap();
    assert_eq!(
        stmt.expanded_sql(),
        "SELECT 42, 'it''s', ':nm' /* ? */, -- ?\nx'0102'"
    );

    // Bindings survive reset; clear_bindings reverts them to NULL.
    stmt.reset().unwrap();
    assert_eq!(
        stmt.expanded_sql(),
        "SELECT 42, 'it''s', ':nm' /* ? */, -- ?\nx'0102'"
    );
    stmt.clear_bindings();
    assert_eq!(
        stmt.expanded_sql(),
        "SELECT NULL, NULL, ':nm' /* ? */, -- ?\nNULL"
    );

    // A marker can occur several times and renders its value at every
    // occurrence; a bare ? after ?2 takes index 3.
    let mut stmt = conn.prepare("SELECT ?2, :a, ?2, ?").unwrap();
    stmt.bind_at(2.try_into().unwrap(), Value::from_i64(7))
        .unwrap();
    stmt.bind_at(3.try_into().unwrap(), Value::build_text("x"))
        .unwrap();
    stmt.bind_at(4.try_into().unwrap(), Value::from_i64(9))
        .unwrap();
    assert_eq!(stmt.expanded_sql(), "SELECT 7, 'x', 7, 9");
}

#[test]
fn test_tcl_style_parameter_names_bind_and_expand() {
    // The TCL binding passes namespace-qualified variables ($::x,
    // $ns::y) and array elements ($arr(k)) as parameter names. Each
    // spelling is one parameter, found by its full text, and expanded
    // SQL — which re-lexes the statement text — sees the same markers
    // the parse did, so the bound values land in the right places.
    let conn = open_test_connection().unwrap();
    let mut stmt = conn
        .prepare("SELECT $::x, $ns::y, $arr(k), $::x, '$::x'")
        .unwrap();
    let x = stmt.parameter_index("$::x").unwrap();
    let y = stmt.parameter_index("$ns::y").unwrap();
    let k = stmt.parameter_index("$arr(k)").unwrap();
    assert_eq!(stmt.parameters_count(), 3);
    stmt.bind_at(x, Value::from_i64(1)).unwrap();
    stmt.bind_at(y, Value::build_text("two")).unwrap();
    stmt.bind_at(k, Value::from_i64(3)).unwrap();

    assert_eq!(stmt.expanded_sql(), "SELECT 1, 'two', 3, 1, '$::x'");
    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(
        rows,
        vec![vec![
            Value::from_i64(1),
            Value::build_text("two"),
            Value::from_i64(3),
            Value::from_i64(1),
            Value::build_text("$::x"),
        ]]
    );
}

#[test]
fn test_metrics_persist_across_reset() {
    let conn = open_test_connection().unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.metrics.write().reset();

    let mut stmt = conn.prepare("INSERT INTO t VALUES (1)").unwrap();
    stmt.run_ignore_rows().unwrap();
    assert_eq!(stmt.metrics().rows_written, 1);

    stmt.reset().unwrap();
    assert_eq!(stmt.metrics().rows_written, 1);

    stmt.run_ignore_rows().unwrap();
    assert_eq!(stmt.metrics().rows_written, 2);

    stmt.reset_metrics();
    assert_eq!(stmt.metrics().rows_written, 0);
}

#[test]
fn test_seek_metrics_separate_index_and_table_work() {
    let conn = open_test_connection().unwrap();
    conn.execute("CREATE TABLE t(a, b)").unwrap();
    conn.execute("CREATE INDEX t_a ON t(a)").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let mut stmt = conn.prepare("SELECT b FROM t WHERE a = 2").unwrap();
    stmt.run_collect_rows().unwrap();
    let metrics = stmt.metrics();

    assert_eq!(metrics.btree_seeks, 2);
    assert_eq!(metrics.btree_table_seeks, 1);
    assert_eq!(metrics.btree_index_seeks, 1);
    assert_eq!(metrics.btree_deferred_seeks, 1);
}

#[test]
fn test_run_with_row_callback_nonblock_collects_all_rows() {
    let conn = open_test_connection().unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let io = conn.db.io.clone();
    let mut stmt = conn.prepare("SELECT x FROM t ORDER BY x").unwrap();

    // Drive the non-blocking runner via the IOResult loop, exactly as a
    // state-machine caller would: collect into an accumulator that persists
    // across yields and wait on each yielded completion.
    let mut collected: Vec<i64> = Vec::new();
    loop {
        let res = stmt
            .run_with_row_callback_nonblock(|row| {
                collected.push(row.get::<i64>(0)?);
                Ok(())
            })
            .unwrap();
        match res {
            crate::IOResult::Done(()) => break,
            crate::IOResult::IO(c) => c.wait(io.as_ref()).unwrap(),
        }
    }
    assert_eq!(collected, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_run_ignore_rows_nonblock_completes() {
    let conn = open_test_connection().unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();

    let io = conn.db.io.clone();
    let mut stmt = conn.prepare("INSERT INTO t VALUES (1), (2)").unwrap();
    loop {
        match stmt.run_ignore_rows_nonblock().unwrap() {
            crate::IOResult::Done(()) => break,
            crate::IOResult::IO(c) => c.wait(io.as_ref()).unwrap(),
        }
    }
    assert_eq!(stmt.metrics().rows_written, 2);
}

#[test]
fn test_metrics_include_subprogram_writes() {
    let conn = open_test_connection().unwrap();
    conn.execute("CREATE TABLE src(x)").unwrap();
    conn.execute("CREATE TABLE log(x)").unwrap();
    conn.execute(
        "CREATE TRIGGER src_log AFTER INSERT ON src BEGIN INSERT INTO log VALUES (new.x); END",
    )
    .unwrap();

    let mut stmt = conn.prepare("INSERT INTO src VALUES (1), (2)").unwrap();
    stmt.run_ignore_rows().unwrap();

    assert_eq!(
        stmt.metrics().rows_written,
        6,
        "cumulative metrics should include root and trigger writes"
    );
}
