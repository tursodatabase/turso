use crate::common::TempDatabase;
use turso_core::Value;
use turso_pg::PgConnection;

fn query_rows(conn: &PgConnection, sql: &str) -> Vec<Vec<Value>> {
    conn.prepare(sql).unwrap().run_collect_rows().unwrap()
}

fn function_names_and_arg_counts(conn: &PgConnection) -> Vec<Vec<Value>> {
    query_rows(
        conn,
        "SELECT proname, pronargs FROM pg_proc WHERE proname LIKE 'stored_%' ORDER BY proname",
    )
}

#[turso_macros::test(mvcc)]
fn test_create_function_is_visible_to_other_connections(db: TempDatabase) {
    let conn = db.connect_postgres();
    let other = db.connect_postgres();
    conn.execute(
        "CREATE FUNCTION stored_add_tax(price numeric) RETURNS numeric LANGUAGE sql \
         AS 'SELECT price * 1.2'",
    )
    .unwrap();
    assert_eq!(
        function_names_and_arg_counts(&other),
        vec![vec![
            Value::build_text("stored_add_tax"),
            Value::from_i64(1)
        ]]
    );

    other
        .execute("DROP FUNCTION stored_add_tax(numeric)")
        .unwrap();
    assert!(function_names_and_arg_counts(&conn).is_empty());
}

#[turso_macros::test]
fn test_create_function_survives_a_reopen(db: TempDatabase) {
    let path = db.path.to_str().unwrap().to_string();
    let io = db.io.clone();
    {
        let conn = db.connect_postgres();
        conn.execute("CREATE FUNCTION stored_two() RETURNS int LANGUAGE sql AS 'SELECT 2'")
            .unwrap();
        conn.execute("CREATE FUNCTION stored_two(a int) RETURNS int LANGUAGE sql AS 'SELECT a'")
            .unwrap();
        conn.execute("DROP FUNCTION stored_two(int)").unwrap();
        conn.close().unwrap();
    }
    drop(db);

    let db = turso_pg::open_database_with_io(
        io,
        &path,
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_custom_types(true),
    )
    .unwrap();
    let conn = PgConnection::connect(&db).unwrap();
    assert_eq!(
        function_names_and_arg_counts(&conn),
        vec![vec![Value::build_text("stored_two"), Value::from_i64(0)]]
    );
    assert_eq!(
        query_rows(
            &conn,
            "SELECT prosrc FROM pg_proc WHERE proname = 'stored_two'"
        ),
        vec![vec![Value::build_text("SELECT 2")]]
    );
}

#[turso_macros::test(mvcc)]
fn test_prepared_statement_uses_the_replaced_function_body(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE FUNCTION stored_v() RETURNS int LANGUAGE sql AS 'SELECT 1'")
        .unwrap();
    let mut stmt = conn.prepare("SELECT stored_v()").unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(1)]]
    );
    stmt.reset().unwrap();

    let other = db.connect_postgres();
    other
        .execute("CREATE OR REPLACE FUNCTION stored_v() RETURNS int LANGUAGE sql AS 'SELECT 2'")
        .unwrap();
    assert_eq!(
        stmt.run_collect_rows().unwrap(),
        vec![vec![Value::from_i64(2)]]
    );
    stmt.reset().unwrap();

    other.execute("DROP FUNCTION stored_v()").unwrap();
    let err = stmt.run_collect_rows().unwrap_err();
    assert!(err.to_string().contains("stored_v"), "{err}");
}
