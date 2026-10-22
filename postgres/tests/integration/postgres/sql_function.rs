use crate::common::TempDatabase;
use turso_core::{StatementStatusCounter, Value};

const ADD_ONE: &str =
    "CREATE FUNCTION add_one(a integer) RETURNS integer LANGUAGE sql AS 'SELECT a + 1'";

#[turso_macros::test(mvcc)]
fn test_sql_function_call_works_after_reprepare(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(ADD_ONE).unwrap();
    conn.execute("CREATE TABLE t (x integer)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();
    let mut stmt = conn.prepare("SELECT add_one(x) FROM t").unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(2)]]
    );
    stmt.reset().unwrap();
    conn.execute("CREATE TABLE other (y integer)").unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(2)]]
    );
    assert_eq!(stmt.stmt_status(StatementStatusCounter::Reprepare), 1);
}

#[turso_macros::test(mvcc)]
fn test_sql_function_call_column_is_named_after_function(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(ADD_ONE).unwrap();
    for (sql, expected) in [
        ("SELECT add_one(1)", "add_one"),
        ("SELECT public.add_one(1)", "add_one"),
        ("SELECT add_one(1) AS answer", "answer"),
    ] {
        let stmt = conn.prepare(sql).unwrap();
        assert_eq!(stmt.get_column_name(0), expected, "{sql}");
    }
}

#[turso_macros::test(mvcc)]
fn test_sql_function_created_on_other_connection_can_be_called(db: TempDatabase) {
    let caller = db.connect_postgres();
    caller.execute("SELECT 1").unwrap();
    db.connect_postgres().execute(ADD_ONE).unwrap();
    let mut stmt = caller.prepare("SELECT add_one(41)").unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(42)]]
    );
}

#[turso_macros::test]
fn test_sql_function_is_kept_after_reopen(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(ADD_ONE).unwrap();
    let path = db.path.clone();
    let io = db.io.clone();
    drop(conn);
    drop(db);

    let db = turso_pg::open_database_with_io(
        io,
        path.to_str().unwrap(),
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true),
    )
    .unwrap();
    let conn = turso_pg::Connection::connect(&db).unwrap();
    let mut stmt = conn.prepare("SELECT add_one(41)").unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(42)]]
    );
}

#[turso_macros::test(mvcc)]
fn test_sql_function_connection_closes_after_calls(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute(ADD_ONE).unwrap();
    conn.execute("SELECT add_one(1)").unwrap();
    conn.close().unwrap();
    drop(conn);
    let conn = db.connect_postgres();
    let mut stmt = conn.prepare("SELECT add_one(1)").unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(2)]]
    );
}
