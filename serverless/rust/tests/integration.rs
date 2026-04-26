/// Integration tests for turso_serverless.
///
/// These require a running libsql-server:
///   docker run -d -p 8080:8080 ghcr.io/tursodatabase/libsql-server:latest
///
/// `remote` is not in workspace `default-members`, so `cargo test` from
/// the workspace root won't run these. They run via `cargo test -p turso_serverless`
/// or in CI against a real server.
use turso_serverless::{Builder, Value};

fn server_url() -> String {
    std::env::var("TURSO_DATABASE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string())
}

fn auth_token() -> Option<String> {
    std::env::var("TURSO_AUTH_TOKEN").ok()
}

async fn connect() -> turso_serverless::Connection {
    let mut builder = Builder::new_remote(server_url());
    if let Some(token) = auth_token() {
        builder = builder.with_auth_token(token);
    }
    let db = builder.build().await.unwrap();
    db.connect().unwrap()
}

// ---------------------------------------------------------------------------
// execute()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execute_query_single_value() {
    let conn = connect().await;
    let mut rows = conn.query("SELECT 42", ()).await.unwrap();
    assert_eq!(rows.column_count(), 1);
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(42));
}

#[tokio::test]
async fn execute_query_single_row() {
    let conn = connect().await;
    let mut rows = conn
        .query("SELECT 1 AS one, 'two' AS two, 0.5 AS three", ())
        .await
        .unwrap();

    let names = rows.column_names();
    assert_eq!(names, vec!["one", "two", "three"]);

    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(1));
    assert_eq!(row.get_value(1).unwrap(), Value::Text("two".to_string()));
    assert_eq!(row.get_value(2).unwrap(), Value::Real(0.5));
}

#[tokio::test]
async fn execute_query_multiple_rows() {
    let conn = connect().await;
    let mut rows = conn
        .query("VALUES (1, 'one'), (2, 'two'), (3, 'three')", ())
        .await
        .unwrap();

    let r0 = rows.next().await.unwrap().unwrap();
    assert_eq!(r0.get_value(0).unwrap(), Value::Integer(1));
    assert_eq!(r0.get_value(1).unwrap(), Value::Text("one".to_string()));

    let r1 = rows.next().await.unwrap().unwrap();
    assert_eq!(r1.get_value(0).unwrap(), Value::Integer(2));

    let r2 = rows.next().await.unwrap().unwrap();
    assert_eq!(r2.get_value(0).unwrap(), Value::Integer(3));

    assert!(rows.next().await.unwrap().is_none());
}

#[tokio::test]
async fn execute_error_on_invalid_sql() {
    let conn = connect().await;
    let result = conn.query("SELECT foobar", ()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn execute_rows_affected_insert() {
    let conn = connect().await;
    conn.execute("DROP TABLE IF EXISTS t_insert_test", ())
        .await
        .unwrap();
    conn.execute("CREATE TABLE t_insert_test (a)", ())
        .await
        .unwrap();
    let affected = conn
        .execute("INSERT INTO t_insert_test VALUES (1), (2)", ())
        .await
        .unwrap();
    assert_eq!(affected, 2);
}

#[tokio::test]
async fn execute_rows_affected_delete() {
    let conn = connect().await;
    conn.execute("DROP TABLE IF EXISTS t_del_test", ())
        .await
        .unwrap();
    conn.execute("CREATE TABLE t_del_test (a)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO t_del_test VALUES (1), (2), (3), (4), (5)", ())
        .await
        .unwrap();
    let affected = conn
        .execute("DELETE FROM t_del_test WHERE a >= 3", ())
        .await
        .unwrap();
    assert_eq!(affected, 3);
}

#[tokio::test]
async fn execute_insert_returning() {
    let conn = connect().await;
    conn.execute("DROP TABLE IF EXISTS t_ret_test", ())
        .await
        .unwrap();
    conn.execute("CREATE TABLE t_ret_test (a)", ())
        .await
        .unwrap();
    let mut rows = conn
        .query(
            "INSERT INTO t_ret_test VALUES (1) RETURNING 42 AS x, 'foo' AS y",
            (),
        )
        .await
        .unwrap();

    let names = rows.column_names();
    assert_eq!(names, vec!["x", "y"]);

    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(42));
    assert_eq!(row.get_value(1).unwrap(), Value::Text("foo".to_string()));
}

// ---------------------------------------------------------------------------
// values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn value_roundtrip_string() {
    let conn = connect().await;
    let mut rows = conn.query("SELECT ?", vec![Value::from("boomerang")]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(
        row.get_value(0).unwrap(),
        Value::Text("boomerang".to_string())
    );
}

#[tokio::test]
async fn value_roundtrip_unicode() {
    let conn = connect().await;
    let unicode = "žluťoučký kůň úpěl ďábelské ódy";
    let mut rows = conn.query("SELECT ?", vec![Value::from(unicode)]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Text(unicode.to_string()));
}

#[tokio::test]
async fn value_roundtrip_integer() {
    let conn = connect().await;
    let mut rows = conn.query("SELECT ?", vec![Value::Integer(-2023)]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(-2023));
}

#[tokio::test]
async fn value_roundtrip_float() {
    let conn = connect().await;
    let mut rows = conn.query("SELECT ?", vec![Value::Real(12.345)]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Real(12.345));
}

#[tokio::test]
async fn value_roundtrip_null() {
    let conn = connect().await;
    let mut rows = conn.query("SELECT ?", vec![Value::Null]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Null);
}

#[tokio::test]
async fn value_roundtrip_bool_true() {
    let conn = connect().await;
    let mut rows = conn.query("SELECT ?", vec![Value::from(true)]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(1));
}

#[tokio::test]
async fn value_roundtrip_bool_false() {
    let conn = connect().await;
    let mut rows = conn.query("SELECT ?", vec![Value::from(false)]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(0));
}

#[tokio::test]
async fn value_roundtrip_blob() {
    let conn = connect().await;
    let blob: Vec<u8> = (0u8..=255).map(|i| i ^ 0xab).collect();
    let mut rows = conn
        .query("SELECT ?", vec![Value::Blob(blob.clone())])
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Blob(blob));
}

#[tokio::test]
async fn value_roundtrip_large_integer() {
    let conn = connect().await;
    let mut rows = conn.query("SELECT ?||''", vec![Value::Integer(i64::MAX)]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Text(i64::MAX.to_string()));
}

// ---------------------------------------------------------------------------
// arguments
// ---------------------------------------------------------------------------

#[tokio::test]
async fn args_positional() {
    let conn = connect().await;
    let mut rows = conn.query("SELECT ?, ?", vec![Value::from("one"), Value::from("two")]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Text("one".to_string()));
    assert_eq!(row.get_value(1).unwrap(), Value::Text("two".to_string()));
}

#[tokio::test]
async fn args_named() {
    let conn = connect().await;
    let mut rows = conn
        .query(
            "SELECT :a, :b",
            [
                (":a", Value::Text("one".to_string())),
                (":b", Value::Text("two".to_string())),
            ],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Text("one".to_string()));
    assert_eq!(row.get_value(1).unwrap(), Value::Text("two".to_string()));
}

// ---------------------------------------------------------------------------
// execute_batch() (sequence)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exec_multiple_statements() {
    let conn = connect().await;
    conn.execute_batch(
        "DROP TABLE IF EXISTS t_batch;
         CREATE TABLE t_batch (a);
         INSERT INTO t_batch VALUES (1), (2), (4), (8);",
    )
    .await
    .unwrap();

    let mut rows = conn.query("SELECT SUM(a) FROM t_batch", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(15));
}

#[tokio::test]
async fn exec_error_stops_execution() {
    let conn = connect().await;
    let result = conn
        .execute_batch(
            "DROP TABLE IF EXISTS t_batch_err;
             CREATE TABLE t_batch_err (a);
             INSERT INTO t_batch_err VALUES (1), (2), (4);
             INSERT INTO t_batch_err VALUES (foo());
             INSERT INTO t_batch_err VALUES (8), (16);",
        )
        .await;

    assert!(result.is_err());

    let mut rows = conn
        .query("SELECT SUM(a) FROM t_batch_err", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(7));
}

#[tokio::test]
async fn exec_manual_transaction() {
    let conn = connect().await;
    conn.execute_batch(
        "DROP TABLE IF EXISTS t_batch_tx;
         CREATE TABLE t_batch_tx (a);
         BEGIN;
         INSERT INTO t_batch_tx VALUES (1), (2), (4);
         INSERT INTO t_batch_tx VALUES (8), (16);
         COMMIT;",
    )
    .await
    .unwrap();

    let mut rows = conn
        .query("SELECT SUM(a) FROM t_batch_tx", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(31));
}

// ---------------------------------------------------------------------------
// transaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transaction_commit() {
    let conn = connect().await;
    conn.execute("DROP TABLE IF EXISTS t_tx_commit", ())
        .await
        .unwrap();
    conn.execute("CREATE TABLE t_tx_commit (a)", ())
        .await
        .unwrap();

    let mut conn = conn;
    let tx = conn.transaction().await.unwrap();
    tx.conn()
        .execute("INSERT INTO t_tx_commit VALUES ('one')", ())
        .await
        .unwrap();
    tx.conn()
        .execute("INSERT INTO t_tx_commit VALUES ('two')", ())
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut rows = conn
        .query("SELECT COUNT(*) FROM t_tx_commit", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(2));
}

#[tokio::test]
async fn transaction_rollback() {
    let conn = connect().await;
    conn.execute("DROP TABLE IF EXISTS t_tx_rb", ())
        .await
        .unwrap();
    conn.execute("CREATE TABLE t_tx_rb (a)", ()).await.unwrap();

    let mut conn = conn;
    let tx = conn.transaction().await.unwrap();
    tx.conn()
        .execute("INSERT INTO t_tx_rb VALUES ('one')", ())
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    let mut rows = conn
        .query("SELECT COUNT(*) FROM t_tx_rb", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(0));
}

// ---------------------------------------------------------------------------
// prepare / Statement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn prepare_and_query() {
    let conn = connect().await;
    conn.execute("DROP TABLE IF EXISTS t_prep", ())
        .await
        .unwrap();
    conn.execute(
        "CREATE TABLE t_prep (id INTEGER PRIMARY KEY, name TEXT)",
        (),
    )
    .await
    .unwrap();
    conn.execute("INSERT INTO t_prep VALUES (1, 'Alice'), (2, 'Bob')", ())
        .await
        .unwrap();

    let mut stmt = conn
        .prepare("SELECT * FROM t_prep WHERE id = ?")
        .await
        .unwrap();
    let row = stmt.query_row(vec![Value::Integer(1)]).await.unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(1));
    assert_eq!(row.get_value(1).unwrap(), Value::Text("Alice".to_string()));
}

#[tokio::test]
async fn prepare_query_no_results() {
    let conn = connect().await;
    conn.execute("DROP TABLE IF EXISTS t_prep_empty", ())
        .await
        .unwrap();
    conn.execute("CREATE TABLE t_prep_empty (a)", ())
        .await
        .unwrap();

    let mut stmt = conn
        .prepare("SELECT * FROM t_prep_empty WHERE a = ?")
        .await
        .unwrap();
    let result = stmt.query_row(vec![Value::Integer(999)]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn prepare_execute() {
    let conn = connect().await;
    conn.execute("DROP TABLE IF EXISTS t_prep_exec", ())
        .await
        .unwrap();
    conn.execute("CREATE TABLE t_prep_exec (a)", ())
        .await
        .unwrap();

    let mut stmt = conn
        .prepare("INSERT INTO t_prep_exec VALUES (?)")
        .await
        .unwrap();
    let affected = stmt.execute(vec![Value::Integer(42)]).await.unwrap();
    assert_eq!(affected, 1);
}

#[tokio::test]
async fn prepare_columns() {
    let conn = connect().await;
    conn.execute("DROP TABLE IF EXISTS t_prep_cols", ())
        .await
        .unwrap();
    conn.execute("CREATE TABLE t_prep_cols (id INTEGER, name TEXT)", ())
        .await
        .unwrap();

    let stmt = conn
        .prepare("SELECT id, name FROM t_prep_cols")
        .await
        .unwrap();
    let cols = stmt.columns();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].name(), "id");
    assert_eq!(cols[1].name(), "name");
}

// ---------------------------------------------------------------------------
// column types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn column_types_from_table() {
    let conn = connect().await;
    conn.execute("DROP TABLE IF EXISTS t_coltypes", ())
        .await
        .unwrap();
    conn.execute(
        "CREATE TABLE t_coltypes (i INTEGER, f FLOAT, t TEXT, b BLOB)",
        (),
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO t_coltypes VALUES (42, 0.5, 'foo', X'626172')",
        (),
    )
    .await
    .unwrap();

    let rows = conn
        .query("SELECT i, f, t, b FROM t_coltypes LIMIT 1", ())
        .await
        .unwrap();

    let names = rows.column_names();
    assert_eq!(names, vec!["i", "f", "t", "b"]);

    let cols = rows.columns();
    assert_eq!(cols[0].decl_type(), Some("INTEGER"));
    assert_eq!(cols[1].decl_type(), Some("FLOAT"));
    assert_eq!(cols[2].decl_type(), Some("TEXT"));
    assert_eq!(cols[3].decl_type(), Some("BLOB"));
}

// ---------------------------------------------------------------------------
// error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn error_nonexistent_table() {
    let conn = connect().await;
    let result = conn.query("SELECT * FROM nonexistent_table", ()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn error_connection_recovers() {
    let conn = connect().await;
    let err = conn.query("SELECT foobar", ()).await;
    assert!(err.is_err());

    // Connection should still be usable
    let mut rows = conn.query("SELECT 42", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(42));
}

#[tokio::test]
async fn error_pk_constraint() {
    let conn = connect().await;
    conn.execute("DROP TABLE IF EXISTS t_pk_err", ())
        .await
        .unwrap();
    conn.execute(
        "CREATE TABLE t_pk_err (id INTEGER PRIMARY KEY, name TEXT)",
        (),
    )
    .await
    .unwrap();
    conn.execute("INSERT INTO t_pk_err VALUES (1, 'first')", ())
        .await
        .unwrap();

    let result = conn
        .execute("INSERT INTO t_pk_err VALUES (1, 'duplicate')", ())
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn error_unique_constraint() {
    let conn = connect().await;
    conn.execute("DROP TABLE IF EXISTS t_uq_err", ())
        .await
        .unwrap();
    conn.execute("CREATE TABLE t_uq_err (id INTEGER, name TEXT UNIQUE)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO t_uq_err VALUES (1, 'unique_name')", ())
        .await
        .unwrap();

    let result = conn
        .execute("INSERT INTO t_uq_err VALUES (2, 'unique_name')", ())
        .await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// close
// ---------------------------------------------------------------------------

#[tokio::test]
async fn close_connection() {
    let conn = connect().await;
    conn.execute("SELECT 1", ()).await.unwrap();
    conn.close().await.unwrap();
}
