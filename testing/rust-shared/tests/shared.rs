// Shared integration tests for turso (local) and turso_serverless.
//
// Uses a macro to generate identical test bodies for both backends.
// Local tests use `:memory:`, remote tests require a running libsql-server:
//   docker run -d -p 8080:8080 ghcr.io/tursodatabase/libsql-server:latest

// ---------------------------------------------------------------------------
// Shared test macro
// ---------------------------------------------------------------------------

// Generates test functions for a single backend. Each module must provide:
// - `$Value`: the crate's Value enum path
// - `$connect`: an async expression returning a Connection
// - `$tx_exec`: a macro that executes SQL on a transaction
macro_rules! shared_tests {
    (Value = $Value:path, connect = $connect:expr, tx_exec = $tx_exec:ident) => {
        use $Value as Value;

        // -- Query execution -----------------------------------------------

        #[tokio::test]
        async fn query_single_value() {
            let conn = $connect.await;
            let mut rows = conn.query("SELECT 42", ()).await.unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Integer(42));
        }

        #[tokio::test]
        async fn query_single_row() {
            let conn = $connect.await;
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
        async fn query_multiple_rows() {
            let conn = $connect.await;
            let mut rows = conn
                .query("VALUES (1, 'one'), (2, 'two'), (3, 'three')", ())
                .await
                .unwrap();

            let r = rows.next().await.unwrap().unwrap();
            assert_eq!(r.get_value(0).unwrap(), Value::Integer(1));
            assert_eq!(r.get_value(1).unwrap(), Value::Text("one".to_string()));

            let r = rows.next().await.unwrap().unwrap();
            assert_eq!(r.get_value(0).unwrap(), Value::Integer(2));

            let r = rows.next().await.unwrap().unwrap();
            assert_eq!(r.get_value(0).unwrap(), Value::Integer(3));

            assert!(rows.next().await.unwrap().is_none());
        }

        #[tokio::test]
        async fn query_error_on_invalid_sql() {
            let conn = $connect.await;
            assert!(conn.query("SELECT foobar", ()).await.is_err());
        }

        #[tokio::test]
        async fn insert_returning() {
            let conn = $connect.await;
            conn.execute("DROP TABLE IF EXISTS t_shared_ret", ())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t_shared_ret (a)", ())
                .await
                .unwrap();
            let mut rows = conn
                .query(
                    "INSERT INTO t_shared_ret VALUES (1) RETURNING 42 AS x, 'foo' AS y",
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

        // -- Rows affected -------------------------------------------------

        #[tokio::test]
        async fn rows_affected_insert() {
            let conn = $connect.await;
            conn.execute("DROP TABLE IF EXISTS t_shared_ins", ())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t_shared_ins (a)", ())
                .await
                .unwrap();
            let affected = conn
                .execute("INSERT INTO t_shared_ins VALUES (1), (2)", ())
                .await
                .unwrap();
            assert_eq!(affected, 2);
        }

        #[tokio::test]
        async fn rows_affected_delete() {
            let conn = $connect.await;
            conn.execute("DROP TABLE IF EXISTS t_shared_del", ())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t_shared_del (a)", ())
                .await
                .unwrap();
            conn.execute(
                "INSERT INTO t_shared_del VALUES (1), (2), (3), (4), (5)",
                (),
            )
            .await
            .unwrap();
            let affected = conn
                .execute("DELETE FROM t_shared_del WHERE a >= 3", ())
                .await
                .unwrap();
            assert_eq!(affected, 3);
        }

        // -- Value roundtrip ------------------------------------------------

        #[tokio::test]
        async fn value_roundtrip_string() {
            let conn = $connect.await;
            let mut rows = conn.query("SELECT ?", vec![Value::from("boomerang")]).await.unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(
                row.get_value(0).unwrap(),
                Value::Text("boomerang".to_string())
            );
        }

        #[tokio::test]
        async fn value_roundtrip_unicode() {
            let conn = $connect.await;
            let unicode = "žluťoučký kůň úpěl ďábelské ódy";
            let mut rows = conn.query("SELECT ?", vec![Value::from(unicode)]).await.unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Text(unicode.to_string()));
        }

        #[tokio::test]
        async fn value_roundtrip_integer() {
            let conn = $connect.await;
            let mut rows = conn.query("SELECT ?", vec![Value::Integer(-2023)]).await.unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Integer(-2023));
        }

        #[tokio::test]
        async fn value_roundtrip_float() {
            let conn = $connect.await;
            let mut rows = conn.query("SELECT ?", vec![Value::Real(12.345)]).await.unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Real(12.345));
        }

        #[tokio::test]
        async fn value_roundtrip_null() {
            let conn = $connect.await;
            let mut rows = conn.query("SELECT ?", vec![Value::Null]).await.unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Null);
        }

        #[tokio::test]
        async fn value_roundtrip_bool_true() {
            let conn = $connect.await;
            let mut rows = conn.query("SELECT ?", vec![Value::from(true)]).await.unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Integer(1));
        }

        #[tokio::test]
        async fn value_roundtrip_bool_false() {
            let conn = $connect.await;
            let mut rows = conn.query("SELECT ?", vec![Value::from(false)]).await.unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Integer(0));
        }

        #[tokio::test]
        async fn value_roundtrip_blob() {
            let conn = $connect.await;
            let blob: Vec<u8> = (0u8..=255).map(|i| i ^ 0xab).collect();
            let mut rows = conn
                .query("SELECT ?", vec![Value::Blob(blob.clone())])
                .await
                .unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Blob(blob));
        }

        // -- Arguments ------------------------------------------------------

        #[tokio::test]
        async fn args_positional() {
            let conn = $connect.await;
            let mut rows = conn.query("SELECT ?, ?", vec![Value::from("one"), Value::from("two")]).await.unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Text("one".to_string()));
            assert_eq!(row.get_value(1).unwrap(), Value::Text("two".to_string()));
        }

        #[tokio::test]
        async fn args_named() {
            let conn = $connect.await;
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

        // -- execute_batch (sequence) ----------------------------------------

        #[tokio::test]
        async fn batch_multiple_statements() {
            let conn = $connect.await;
            conn.execute_batch(
                "DROP TABLE IF EXISTS t_shared_batch;
                 CREATE TABLE t_shared_batch (a);
                 INSERT INTO t_shared_batch VALUES (1), (2), (4), (8);",
            )
            .await
            .unwrap();
            let mut rows = conn
                .query("SELECT SUM(a) FROM t_shared_batch", ())
                .await
                .unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Integer(15));
        }

        #[tokio::test]
        async fn batch_error_stops_execution() {
            let conn = $connect.await;
            let result = conn
                .execute_batch(
                    "DROP TABLE IF EXISTS t_shared_batch_err;
                     CREATE TABLE t_shared_batch_err (a);
                     INSERT INTO t_shared_batch_err VALUES (1), (2), (4);
                     INSERT INTO t_shared_batch_err VALUES (foo());
                     INSERT INTO t_shared_batch_err VALUES (8), (16);",
                )
                .await;
            assert!(result.is_err());
            let mut rows = conn
                .query("SELECT SUM(a) FROM t_shared_batch_err", ())
                .await
                .unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Integer(7));
        }

        #[tokio::test]
        async fn batch_manual_transaction() {
            let conn = $connect.await;
            conn.execute_batch(
                "DROP TABLE IF EXISTS t_shared_batch_tx;
                 CREATE TABLE t_shared_batch_tx (a);
                 BEGIN;
                 INSERT INTO t_shared_batch_tx VALUES (1), (2), (4);
                 INSERT INTO t_shared_batch_tx VALUES (8), (16);
                 COMMIT;",
            )
            .await
            .unwrap();
            let mut rows = conn
                .query("SELECT SUM(a) FROM t_shared_batch_tx", ())
                .await
                .unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Integer(31));
        }

        // -- Transaction ---------------------------------------------------

        #[tokio::test]
        async fn transaction_commit() {
            let conn = $connect.await;
            conn.execute("DROP TABLE IF EXISTS t_shared_tx_commit", ())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t_shared_tx_commit (a)", ())
                .await
                .unwrap();

            let mut conn = conn;
            let tx = conn.transaction().await.unwrap();
            $tx_exec!(tx, "INSERT INTO t_shared_tx_commit VALUES ('one')", ());
            $tx_exec!(tx, "INSERT INTO t_shared_tx_commit VALUES ('two')", ());
            tx.commit().await.unwrap();

            let mut rows = conn
                .query("SELECT COUNT(*) FROM t_shared_tx_commit", ())
                .await
                .unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Integer(2));
        }

        #[tokio::test]
        async fn transaction_rollback() {
            let conn = $connect.await;
            conn.execute("DROP TABLE IF EXISTS t_shared_tx_rb", ())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t_shared_tx_rb (a)", ())
                .await
                .unwrap();

            let mut conn = conn;
            let tx = conn.transaction().await.unwrap();
            $tx_exec!(tx, "INSERT INTO t_shared_tx_rb VALUES ('one')", ());
            tx.rollback().await.unwrap();

            let mut rows = conn
                .query("SELECT COUNT(*) FROM t_shared_tx_rb", ())
                .await
                .unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Integer(0));
        }

        // -- Prepared statements -------------------------------------------

        #[tokio::test]
        async fn prepare_and_query() {
            let conn = $connect.await;
            conn.execute("DROP TABLE IF EXISTS t_shared_prep", ())
                .await
                .unwrap();
            conn.execute(
                "CREATE TABLE t_shared_prep (id INTEGER PRIMARY KEY, name TEXT)",
                (),
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO t_shared_prep VALUES (1, 'Alice'), (2, 'Bob')",
                (),
            )
            .await
            .unwrap();

            let mut stmt = conn
                .prepare("SELECT * FROM t_shared_prep WHERE id = ?")
                .await
                .unwrap();
            let row = stmt.query_row(vec![Value::Integer(1)]).await.unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Integer(1));
            assert_eq!(
                row.get_value(1).unwrap(),
                Value::Text("Alice".to_string())
            );
        }

        #[tokio::test]
        async fn prepare_execute() {
            let conn = $connect.await;
            conn.execute("DROP TABLE IF EXISTS t_shared_prep_exec", ())
                .await
                .unwrap();
            conn.execute("CREATE TABLE t_shared_prep_exec (a)", ())
                .await
                .unwrap();

            let mut stmt = conn
                .prepare("INSERT INTO t_shared_prep_exec VALUES (?)")
                .await
                .unwrap();
            let affected = stmt.execute(vec![Value::Integer(42)]).await.unwrap();
            assert_eq!(affected, 1);
        }

        #[tokio::test]
        async fn prepare_columns() {
            let conn = $connect.await;
            conn.execute("DROP TABLE IF EXISTS t_shared_prep_cols", ())
                .await
                .unwrap();
            conn.execute(
                "CREATE TABLE t_shared_prep_cols (id INTEGER, name TEXT)",
                (),
            )
            .await
            .unwrap();

            let stmt = conn
                .prepare("SELECT id, name FROM t_shared_prep_cols")
                .await
                .unwrap();
            let cols = stmt.columns();
            assert_eq!(cols.len(), 2);
            assert_eq!(cols[0].name(), "id");
            assert_eq!(cols[1].name(), "name");
        }

        // -- Error handling ------------------------------------------------

        #[tokio::test]
        async fn error_nonexistent_table() {
            let conn = $connect.await;
            assert!(conn.query("SELECT * FROM nonexistent_table_shared", ()).await.is_err());
        }

        #[tokio::test]
        async fn error_connection_recovers() {
            let conn = $connect.await;
            assert!(conn.query("SELECT foobar", ()).await.is_err());
            // Connection should still be usable
            let mut rows = conn.query("SELECT 42", ()).await.unwrap();
            let row = rows.next().await.unwrap().unwrap();
            assert_eq!(row.get_value(0).unwrap(), Value::Integer(42));
        }

        #[tokio::test]
        async fn error_pk_constraint() {
            let conn = $connect.await;
            conn.execute("DROP TABLE IF EXISTS t_shared_pk_err", ())
                .await
                .unwrap();
            conn.execute(
                "CREATE TABLE t_shared_pk_err (id INTEGER PRIMARY KEY, name TEXT)",
                (),
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t_shared_pk_err VALUES (1, 'first')", ())
                .await
                .unwrap();
            assert!(
                conn.execute("INSERT INTO t_shared_pk_err VALUES (1, 'duplicate')", ())
                    .await
                    .is_err()
            );
        }

        #[tokio::test]
        async fn error_unique_constraint() {
            let conn = $connect.await;
            conn.execute("DROP TABLE IF EXISTS t_shared_uq_err", ())
                .await
                .unwrap();
            conn.execute(
                "CREATE TABLE t_shared_uq_err (id INTEGER, name TEXT UNIQUE)",
                (),
            )
            .await
            .unwrap();
            conn.execute("INSERT INTO t_shared_uq_err VALUES (1, 'unique_name')", ())
                .await
                .unwrap();
            assert!(
                conn.execute("INSERT INTO t_shared_uq_err VALUES (2, 'unique_name')", ())
                    .await
                    .is_err()
            );
        }
    };
}

// ---------------------------------------------------------------------------
// Local driver module
// ---------------------------------------------------------------------------

mod local {
    async fn connect() -> turso::Connection {
        let db = turso::Builder::new_local(":memory:")
            .build()
            .await
            .unwrap();
        db.connect().unwrap()
    }

    /// Local Transaction derefs to Connection, so execute directly on tx.
    macro_rules! local_tx_exec {
        ($tx:expr, $sql:expr, $params:expr) => {
            $tx.execute($sql, $params).await.unwrap()
        };
    }

    shared_tests!(
        Value = turso::Value,
        connect = connect(),
        tx_exec = local_tx_exec
    );
}

// ---------------------------------------------------------------------------
// Remote driver module
// ---------------------------------------------------------------------------

mod remote {
    fn server_url() -> String {
        std::env::var("TURSO_DATABASE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string())
    }

    fn auth_token() -> Option<String> {
        std::env::var("TURSO_AUTH_TOKEN").ok()
    }

    async fn connect() -> turso_serverless::Connection {
        let mut builder = turso_serverless::Builder::new_remote(server_url());
        if let Some(token) = auth_token() {
            builder = builder.with_auth_token(token);
        }
        let db = builder.build().await.unwrap();
        db.connect().unwrap()
    }

    /// Remote Transaction exposes conn() accessor.
    macro_rules! remote_tx_exec {
        ($tx:expr, $sql:expr, $params:expr) => {
            $tx.conn().execute($sql, $params).await.unwrap()
        };
    }

    shared_tests!(
        Value = turso_serverless::Value,
        connect = connect(),
        tx_exec = remote_tx_exec
    );
}
