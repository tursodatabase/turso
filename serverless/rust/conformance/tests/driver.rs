//! Driver-level conformance tests: exercise the public `turso_serverless`
//! API against a live Turso Cloud database.
//!
//! Requires `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN`; every test skips
//! when they are not set.

use turso_serverless::transaction::{DropBehavior, TransactionBehavior};
use turso_serverless::{named_params, Builder, Connection, Error, Value};
use turso_serverless_conformance::{config_or_skip, unique_name};

async fn connect(url: &str, token: &str) -> Connection {
    Builder::new_remote(url)
        .with_auth_token(token)
        .build()
        .await
        .expect("failed to build database handle")
        .connect()
        .expect("failed to connect")
}

async fn drop_table(conn: &Connection, table: &str) {
    let _ = conn
        .execute(format!("DROP TABLE IF EXISTS {table}"), ())
        .await;
}

#[tokio::test]
async fn query_scalar_values() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;

    let mut rows = conn
        .query("SELECT 42, 1.5, 'hello', x'deadbeef', NULL", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.column_count(), 5);
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(42));
    assert_eq!(row.get_value(1).unwrap(), Value::Real(1.5));
    assert_eq!(row.get_value(2).unwrap(), Value::Text("hello".to_string()));
    assert_eq!(
        row.get_value(3).unwrap(),
        Value::Blob(vec![0xde, 0xad, 0xbe, 0xef])
    );
    assert_eq!(row.get_value(4).unwrap(), Value::Null);
    assert!(rows.next().await.unwrap().is_none());
}

#[tokio::test]
async fn positional_params_roundtrip() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;

    let mut rows = conn
        .query(
            "SELECT ?, ?, ?, ?, ?, ?",
            (
                i64::MAX,
                i64::MIN,
                3.25,
                "žluťoučký kůň",
                vec![0u8, 1, 255],
                Value::Null,
            ),
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(i64::MAX));
    assert_eq!(row.get_value(1).unwrap(), Value::Integer(i64::MIN));
    assert_eq!(row.get_value(2).unwrap(), Value::Real(3.25));
    assert_eq!(
        row.get_value(3).unwrap(),
        Value::Text("žluťoučký kůň".to_string())
    );
    assert_eq!(row.get_value(4).unwrap(), Value::Blob(vec![0u8, 1, 255]));
    assert_eq!(row.get_value(5).unwrap(), Value::Null);
}

#[tokio::test]
async fn empty_blob_roundtrip() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;

    let mut rows = conn.query("SELECT ?", (Vec::<u8>::new(),)).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Blob(vec![]));
}

#[tokio::test]
async fn named_params_all_prefix_forms() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;

    let mut rows = conn
        .query(
            "SELECT :a, @b, $c",
            named_params![":a": 1, "@b": "x", "$c": 2.5],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Integer(1));
    assert_eq!(row.get_value(1).unwrap(), Value::Text("x".to_string()));
    assert_eq!(row.get_value(2).unwrap(), Value::Real(2.5));
}

#[tokio::test]
async fn non_finite_float_decodes_as_nan() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;

    // The multiplication overflows to infinity, which JSON cannot
    // represent; the server encodes it as a null float and the client
    // must decode it as NaN.
    let mut rows = conn.query("SELECT 1e308 * 10", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    match row.get_value(0).unwrap() {
        Value::Real(f) => assert!(f.is_nan()),
        other => panic!("expected a float, got {other:?}"),
    }
}

#[tokio::test]
async fn infinite_float_param_is_rejected_client_side() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;

    let result = conn.query("SELECT ?", (f64::INFINITY,)).await;
    assert!(matches!(result, Err(Error::ToSqlConversionFailure(_))));
}

#[tokio::test]
async fn execute_reports_affected_rows_and_last_insert_rowid() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_exec");

    conn.execute(
        format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, v TEXT)"),
        (),
    )
    .await
    .unwrap();
    let affected = conn
        .execute(format!("INSERT INTO {table} (id, v) VALUES (7, 'a')"), ())
        .await
        .unwrap();
    assert_eq!(affected, 1);
    assert_eq!(conn.last_insert_rowid(), 7);

    let affected = conn
        .execute(format!("INSERT INTO {table} (v) VALUES ('b'), ('c')"), ())
        .await
        .unwrap();
    assert_eq!(affected, 2);
    assert_eq!(conn.last_insert_rowid(), 9);

    let affected = conn
        .execute(format!("UPDATE {table} SET v = 'z'"), ())
        .await
        .unwrap();
    assert_eq!(affected, 3);

    drop_table(&conn, &table).await;
}

#[tokio::test]
async fn prepared_statement_metadata_and_execution() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_prep");

    conn.execute(
        format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)"),
        (),
    )
    .await
    .unwrap();

    let mut stmt = conn
        .prepare(format!("INSERT INTO {table} (name) VALUES (?)"))
        .await
        .unwrap();
    stmt.execute(("alice",)).await.unwrap();
    assert_eq!(stmt.n_change(), 1);

    let mut stmt = conn
        .prepare(format!("SELECT id, name FROM {table} WHERE name = ?"))
        .await
        .unwrap();
    assert_eq!(stmt.column_count(), 2);
    assert_eq!(stmt.column_name(0).unwrap(), "id");
    assert_eq!(stmt.column_name(1).unwrap(), "name");
    assert_eq!(stmt.column_index("name").unwrap(), 1);
    let columns = stmt.columns();
    assert_eq!(columns[0].decl_type(), Some("INTEGER"));
    assert_eq!(columns[1].decl_type(), Some("TEXT"));

    let row = stmt.query_row(("alice",)).await.unwrap();
    assert_eq!(row.get::<String>(1).unwrap(), "alice");

    // query_row on an empty result reports QueryReturnedNoRows.
    let result = stmt.query_row(("nobody",)).await;
    assert!(matches!(result, Err(Error::QueryReturnedNoRows)));

    drop_table(&conn, &table).await;
}

#[tokio::test]
async fn prepare_invalid_sql_fails() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;

    let result = conn.prepare("SELECT FROM WHERE").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn execute_batch_runs_statements_in_order() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_batch");

    conn.execute_batch(format!(
        "CREATE TABLE {table} (x INTEGER); \
         INSERT INTO {table} VALUES (1); \
         INSERT INTO {table} VALUES (2)"
    ))
    .await
    .unwrap();

    let mut rows = conn
        .query(format!("SELECT count(*), sum(x) FROM {table}"), ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 2);
    assert_eq!(row.get::<i64>(1).unwrap(), 3);

    drop_table(&conn, &table).await;
}

#[tokio::test]
async fn execute_batch_stops_at_first_error() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_batcherr");

    let result = conn
        .execute_batch(format!(
            "CREATE TABLE {table} (x INTEGER); \
             INSERT INTO {table} VALUES (1); \
             INSERT INTO no_such_table_{table} VALUES (2); \
             INSERT INTO {table} VALUES (3)"
        ))
        .await;
    assert!(result.is_err());

    // Statements before the failing one keep their effects; statements
    // after it never ran.
    let mut rows = conn
        .query(format!("SELECT count(*) FROM {table}"), ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);

    drop_table(&conn, &table).await;
}

#[tokio::test]
async fn transaction_commit_spans_multiple_requests() {
    let config = config_or_skip!();
    let mut conn = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_txc");

    conn.execute(format!("CREATE TABLE {table} (x INTEGER)"), ())
        .await
        .unwrap();
    assert!(conn.is_autocommit().unwrap());

    let tx = conn.transaction().await.unwrap();
    tx.execute(format!("INSERT INTO {table} VALUES (1)"), ())
        .await
        .unwrap();
    tx.execute(format!("INSERT INTO {table} VALUES (2)"), ())
        .await
        .unwrap();
    assert!(!tx.is_autocommit().unwrap());
    tx.commit().await.unwrap();
    assert!(conn.is_autocommit().unwrap());

    // The committed rows are visible from a fresh connection.
    let other = connect(&config.url, &config.auth_token).await;
    let mut rows = other
        .query(format!("SELECT count(*) FROM {table}"), ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 2);

    drop_table(&conn, &table).await;
}

#[tokio::test]
async fn transaction_rollback_discards_changes() {
    let config = config_or_skip!();
    let mut conn = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_txr");

    conn.execute(format!("CREATE TABLE {table} (x INTEGER)"), ())
        .await
        .unwrap();

    let tx = conn.transaction().await.unwrap();
    tx.execute(format!("INSERT INTO {table} VALUES (1)"), ())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(conn.is_autocommit().unwrap());

    let mut rows = conn
        .query(format!("SELECT count(*) FROM {table}"), ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 0);

    drop_table(&conn, &table).await;
}

#[tokio::test]
async fn dropped_transaction_rolls_back() {
    let config = config_or_skip!();
    let mut conn = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_txd");

    conn.execute(format!("CREATE TABLE {table} (x INTEGER)"), ())
        .await
        .unwrap();

    {
        let tx = conn.transaction().await.unwrap();
        tx.execute(format!("INSERT INTO {table} VALUES (1)"), ())
            .await
            .unwrap();
        // Dropped without commit.
    }

    let mut rows = conn
        .query(format!("SELECT count(*) FROM {table}"), ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 0);
    assert!(conn.is_autocommit().unwrap());

    drop_table(&conn, &table).await;
}

#[tokio::test]
async fn dropped_transaction_with_commit_drop_behavior_commits() {
    let config = config_or_skip!();
    let mut conn = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_txdc");

    conn.execute(format!("CREATE TABLE {table} (x INTEGER)"), ())
        .await
        .unwrap();

    {
        let mut tx = conn.transaction().await.unwrap();
        assert_eq!(tx.drop_behavior(), DropBehavior::Rollback);
        tx.execute(format!("INSERT INTO {table} VALUES (1)"), ())
            .await
            .unwrap();
        tx.set_drop_behavior(DropBehavior::Commit);
        // Dropped without commit; the drop behavior commits on next use.
    }

    let mut rows = conn
        .query(format!("SELECT count(*) FROM {table}"), ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert!(conn.is_autocommit().unwrap());

    drop_table(&conn, &table).await;
}

#[tokio::test]
async fn transaction_finish_applies_drop_behavior() {
    let config = config_or_skip!();
    let mut conn = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_txf");

    conn.execute(format!("CREATE TABLE {table} (x INTEGER)"), ())
        .await
        .unwrap();

    // Default drop behavior: finish rolls back.
    let tx = conn.transaction().await.unwrap();
    tx.execute(format!("INSERT INTO {table} VALUES (1)"), ())
        .await
        .unwrap();
    tx.finish().await.unwrap();
    assert!(conn.is_autocommit().unwrap());

    // Commit drop behavior: finish commits.
    let mut tx = conn.transaction().await.unwrap();
    tx.execute(format!("INSERT INTO {table} VALUES (2)"), ())
        .await
        .unwrap();
    tx.set_drop_behavior(DropBehavior::Commit);
    tx.finish().await.unwrap();

    let mut rows = conn
        .query(format!("SELECT sum(x) FROM {table}"), ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 2);

    drop_table(&conn, &table).await;
}

#[tokio::test]
async fn unchecked_transaction_rejects_nesting_at_runtime() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_txu");

    conn.execute(format!("CREATE TABLE {table} (x INTEGER)"), ())
        .await
        .unwrap();

    let tx = conn.unchecked_transaction().await.unwrap();
    tx.execute(format!("INSERT INTO {table} VALUES (1)"), ())
        .await
        .unwrap();
    // A nested transaction fails at runtime without disturbing the outer
    // transaction.
    tx.unchecked_transaction().await.unwrap_err();
    tx.execute(format!("INSERT INTO {table} VALUES (2)"), ())
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut rows = conn
        .query(format!("SELECT sum(x) FROM {table}"), ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 3);

    drop_table(&conn, &table).await;
}

#[tokio::test]
async fn set_transaction_behavior_applies_to_new_transactions() {
    let config = config_or_skip!();
    let mut conn = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_txb");

    conn.execute(format!("CREATE TABLE {table} (x INTEGER)"), ())
        .await
        .unwrap();

    conn.set_transaction_behavior(TransactionBehavior::Immediate);
    let tx = conn.transaction().await.unwrap();
    let mut stmt = tx.prepare(&format!("INSERT INTO {table} VALUES (?)")).await.unwrap();
    stmt.execute((1,)).await.unwrap();
    tx.commit().await.unwrap();

    let mut rows = conn
        .query(format!("SELECT count(*) FROM {table}"), ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);

    drop_table(&conn, &table).await;
}

#[tokio::test]
async fn transactions_on_separate_connections_are_independent() {
    let config = config_or_skip!();
    let mut conn1 = connect(&config.url, &config.auth_token).await;
    let conn2 = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_iso");

    conn1
        .execute(format!("CREATE TABLE {table} (x INTEGER)"), ())
        .await
        .unwrap();

    let tx = conn1.transaction().await.unwrap();
    tx.execute(format!("INSERT INTO {table} VALUES (1)"), ())
        .await
        .unwrap();

    // The other connection is in autocommit and does not see the
    // uncommitted row.
    assert!(conn2.is_autocommit().unwrap());
    let mut rows = conn2
        .query(format!("SELECT count(*) FROM {table}"), ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 0);

    tx.commit().await.unwrap();

    let mut rows = conn2
        .query(format!("SELECT count(*) FROM {table}"), ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);

    drop_table(&conn1, &table).await;
}

#[tokio::test]
async fn constraint_violation_maps_to_constraint_error() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_uniq");

    conn.execute(format!("CREATE TABLE {table} (x INTEGER UNIQUE)"), ())
        .await
        .unwrap();
    conn.execute(format!("INSERT INTO {table} VALUES (1)"), ())
        .await
        .unwrap();
    let result = conn
        .execute(format!("INSERT INTO {table} VALUES (1)"), ())
        .await;
    match result {
        Err(Error::Constraint(message)) => {
            assert!(
                message.to_lowercase().contains("unique"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected a constraint error, got {other:?}"),
    }

    drop_table(&conn, &table).await;
}

#[tokio::test]
async fn sql_parse_error_is_reported() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;

    assert!(conn.query("SELECT FROM WHERE", ()).await.is_err());
    assert!(conn.execute("NOT VALID SQL", ()).await.is_err());
    // The connection remains usable after an error.
    let mut rows = conn.query("SELECT 1", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
}

#[tokio::test]
async fn large_result_set_streams_completely() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;
    let table = unique_name("t_large");

    conn.execute(format!("CREATE TABLE {table} (x INTEGER)"), ())
        .await
        .unwrap();
    let values = (1..=1000)
        .map(|x| format!("({x})"))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute(format!("INSERT INTO {table} VALUES {values}"), ())
        .await
        .unwrap();

    let mut rows = conn
        .query(format!("SELECT x FROM {table} ORDER BY x"), ())
        .await
        .unwrap();
    let mut expected = 1i64;
    while let Some(row) = rows.next().await.unwrap() {
        assert_eq!(row.get::<i64>(0).unwrap(), expected);
        expected += 1;
    }
    assert_eq!(expected, 1001);

    drop_table(&conn, &table).await;
}

#[tokio::test]
async fn rows_metadata_from_query() {
    let config = config_or_skip!();
    let conn = connect(&config.url, &config.auth_token).await;

    let rows = conn
        .query("SELECT 1 AS one, 'two' AS two", ())
        .await
        .unwrap();
    assert_eq!(rows.column_count(), 2);
    assert_eq!(rows.column_name(0).unwrap(), "one");
    assert_eq!(rows.column_name(1).unwrap(), "two");
    assert_eq!(rows.column_names(), vec!["one", "two"]);
    assert_eq!(rows.column_index("two").unwrap(), 1);
}

#[tokio::test]
async fn connection_close_releases_stream() {
    let config = config_or_skip!();
    let mut conn = connect(&config.url, &config.auth_token).await;

    // Open a transaction so the stream carries state, then close.
    let tx = conn.transaction().await.unwrap();
    tx.execute("SELECT 1", ()).await.unwrap();
    drop(tx);
    conn.close().await.unwrap();

    // The connection opens a fresh stream on next use.
    let mut rows = conn.query("SELECT 1", ()).await.unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1
    );
    assert!(conn.is_autocommit().unwrap());
}
