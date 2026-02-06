use tokio::fs;
use turso::{Builder, EncryptionOpts, Error, Value};

#[tokio::test]
async fn test_rows_next() {
    let builder = Builder::new_local(":memory:");
    let db = builder.build().await.unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE test (x INTEGER)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO test (x) VALUES (1)", ())
        .await
        .unwrap();
    assert_eq!(conn.last_insert_rowid(), 1);
    conn.execute("INSERT INTO test (x) VALUES (2)", ())
        .await
        .unwrap();
    assert_eq!(conn.last_insert_rowid(), 2);
    conn.execute(
        "INSERT INTO test (x) VALUES (:x)",
        vec![(":x".to_string(), Value::Integer(3))],
    )
    .await
    .unwrap();
    assert_eq!(conn.last_insert_rowid(), 3);
    conn.execute(
        "INSERT INTO test (x) VALUES (@x)",
        vec![("@x".to_string(), Value::Integer(4))],
    )
    .await
    .unwrap();
    assert_eq!(conn.last_insert_rowid(), 4);
    conn.execute(
        "INSERT INTO test (x) VALUES ($x)",
        vec![("$x".to_string(), Value::Integer(5))],
    )
    .await
    .unwrap();
    assert_eq!(conn.last_insert_rowid(), 5);
    let mut res = conn.query("SELECT * FROM test", ()).await.unwrap();
    assert_eq!(
        res.next().await.unwrap().unwrap().get_value(0).unwrap(),
        1.into()
    );
    assert_eq!(
        res.next().await.unwrap().unwrap().get_value(0).unwrap(),
        2.into()
    );
    assert_eq!(
        res.next().await.unwrap().unwrap().get_value(0).unwrap(),
        3.into()
    );
    assert_eq!(
        res.next().await.unwrap().unwrap().get_value(0).unwrap(),
        4.into()
    );
    assert_eq!(
        res.next().await.unwrap().unwrap().get_value(0).unwrap(),
        5.into()
    );
    assert!(res.next().await.unwrap().is_none());
}

#[tokio::test]
async fn test_cacheflush() {
    let builder = Builder::new_local("test.db");
    let db = builder.build().await.unwrap();

    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE IF NOT EXISTS asdf (x INTEGER)", ())
        .await
        .unwrap();

    // Tests if cache flush breaks transaction isolation
    conn.execute("BEGIN", ()).await.unwrap();
    conn.execute("INSERT INTO asdf (x) VALUES (1)", ())
        .await
        .unwrap();
    conn.cacheflush().unwrap();
    conn.execute("ROLLBACK", ()).await.unwrap();

    conn.execute("INSERT INTO asdf (x) VALUES (2)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO asdf (x) VALUES (3)", ())
        .await
        .unwrap();

    let mut res = conn.query("SELECT * FROM asdf", ()).await.unwrap();

    assert_eq!(
        res.next().await.unwrap().unwrap().get_value(0).unwrap(),
        2.into()
    );
    assert_eq!(
        res.next().await.unwrap().unwrap().get_value(0).unwrap(),
        3.into()
    );

    // Tests if cache flush doesn't break a committed transaction
    conn.execute("BEGIN", ()).await.unwrap();
    conn.execute("INSERT INTO asdf (x) VALUES (1)", ())
        .await
        .unwrap();
    conn.cacheflush().unwrap();
    conn.execute("COMMIT", ()).await.unwrap();

    let mut res = conn
        .query("SELECT * FROM asdf WHERE x = 1", ())
        .await
        .unwrap();

    assert_eq!(
        res.next().await.unwrap().unwrap().get_value(0).unwrap(),
        1.into()
    );

    fs::remove_file("test.db").await.unwrap();
    fs::remove_file("test.db-wal").await.unwrap();
}

#[tokio::test]
async fn test_rows_returned() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    //--- CRUD Operations ---//
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)", ())
        .await
        .unwrap();
    let changed = conn
        .execute("INSERT INTO t VALUES (1,'hello')", ())
        .await
        .unwrap();
    let changed1 = conn
        .execute("INSERT INTO t VALUES (2,'hi')", ())
        .await
        .unwrap();
    let changed2 = conn
        .execute("UPDATE t SET val='hi' WHERE id=1", ())
        .await
        .unwrap();
    let changed3 = conn
        .execute("DELETE FROM t WHERE val='hi'", ())
        .await
        .unwrap();
    assert_eq!(changed, 1);
    assert_eq!(changed1, 1);
    assert_eq!(changed2, 1);
    assert_eq!(changed3, 2);

    //--- A more complicated example of insert with a select join subquery ---//
    conn.execute(
        "CREATE TABLE authors ( id INTEGER PRIMARY KEY, name TEXT NOT NULL);
       ",
        (),
    )
    .await
    .unwrap();

    conn.execute(
       "CREATE TABLE books ( id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL REFERENCES authors(id), title TEXT NOT NULL); "
       ,()
   ).await.unwrap();

    conn.execute(
        "CREATE TABLE prize_winners ( book_id INTEGER PRIMARY KEY, author_name TEXT NOT NULL);",
        (),
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO authors (id, name) VALUES (1, 'Alice'), (2, 'Bob');",
        (),
    )
    .await
    .unwrap();

    conn.execute(
       "INSERT INTO books (id, author_id, title) VALUES (1, 1, 'Rust in Action'), (2, 1, 'Async Adventures'), (3, 1, 'Fearless Concurrency'), (4, 1, 'Unsafe Tales'), (5, 1, 'Zero-Cost Futures'), (6, 2, 'Learning SQL');",
       ()
   ).await.unwrap();

    let rows_changed = conn
        .execute(
            "
       INSERT INTO prize_winners (book_id, author_name)
       SELECT b.id, a.name
       FROM   books b
       JOIN   authors a ON a.id = b.author_id
       WHERE  a.id = 1;       -- Alice's five books
       ",
            (),
        )
        .await
        .unwrap();

    assert_eq!(rows_changed, 5);
}

#[tokio::test]
pub async fn test_execute_batch() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();
    conn.execute_batch("CREATE TABLE authors ( id INTEGER PRIMARY KEY, name TEXT NOT NULL);CREATE TABLE books ( id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL REFERENCES authors(id), title TEXT NOT NULL); INSERT INTO authors (id, name) VALUES (1, 'Alice'), (2, 'Bob');")
        .await
        .unwrap();
    let mut rows = conn
        .query("SELECT COUNT(*) FROM authors;", ())
        .await
        .unwrap();
    if let Some(row) = rows.next().await.unwrap() {
        assert_eq!(row.get_value(0).unwrap(), Value::Integer(2));
    }
}

#[tokio::test]
async fn test_query_row_returns_first_row() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE users (id INTEGER, name TEXT)", ())
        .await
        .unwrap();

    conn.execute("INSERT INTO users VALUES (1, 'Frodo')", ())
        .await
        .unwrap();

    let row = conn
        .prepare("SELECT id FROM users WHERE name = ?")
        .await
        .unwrap()
        .query_row(&["Frodo"])
        .await
        .unwrap();

    let id: i64 = row.get(0).unwrap();
    assert_eq!(id, 1);
}

#[tokio::test]
async fn test_query_row_returns_no_rows_error() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE users (id INTEGER, name TEXT)", ())
        .await
        .unwrap();

    let result = conn
        .prepare("SELECT id FROM users WHERE name = ?")
        .await
        .unwrap()
        .query_row(&["Ghost"])
        .await;

    assert!(matches!(result, Err(Error::QueryReturnedNoRows)));
}

#[tokio::test]
async fn test_row_get_column_typed() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE v (n INTEGER, label TEXT)", ())
        .await
        .unwrap();

    conn.execute("INSERT INTO v VALUES (42, 'answer')", ())
        .await
        .unwrap();

    let mut rows = conn.query("SELECT * FROM v", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();

    let n: i64 = row.get(0).unwrap();
    let label: String = row.get(1).unwrap();

    assert_eq!(n, 42);
    assert_eq!(label, "answer");
}

#[tokio::test]
async fn test_row_get_conversion_error() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t (x TEXT)", ()).await.unwrap();

    conn.execute("INSERT INTO t VALUES (NULL)", ())
        .await
        .unwrap();

    let mut rows = conn.query("SELECT x FROM t", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();

    // Attempt to convert TEXT into integer (should fail)
    let result: Result<u32, _> = row.get(0);
    assert!(matches!(result, Err(Error::ConversionFailure(_))));
}

#[tokio::test]
async fn test_index() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE users (name TEXT PRIMARY KEY, email TEXT)", ())
        .await
        .unwrap();
    conn.execute("CREATE INDEX email_idx ON users(email)", ())
        .await
        .unwrap();
    conn.execute(
        "INSERT INTO users VALUES ('alice', 'a@b.c'), ('bob', 'b@d.e')",
        (),
    )
    .await
    .unwrap();

    let mut rows = conn
        .query("SELECT * FROM users WHERE email = 'a@b.c'", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert!(row.get::<String>(0).unwrap() == "alice");
    assert!(row.get::<String>(1).unwrap() == "a@b.c");
    assert!(rows.next().await.unwrap().is_none());

    let mut rows = conn
        .query("SELECT * FROM users WHERE email = 'b@d.e'", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert!(row.get::<String>(0).unwrap() == "bob");
    assert!(row.get::<String>(1).unwrap() == "b@d.e");
    assert!(rows.next().await.unwrap().is_none());
}

#[tokio::test]
/// Tests that concurrent statements that error out and rollback can do so without panicking
async fn test_concurrent_unique_constraint_regression() {
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE,
            created_at DATETIME
        )",
        (),
    )
    .await
    .unwrap();

    // Insert initial seed data
    conn.execute(
        "INSERT INTO users (email, created_at) VALUES (:email, :created_at)",
        vec![
            (":email".to_string(), Value::Text("seed@example.com".into())),
            (":created_at".to_string(), Value::Text("whatever".into())),
        ],
    )
    .await
    .unwrap();

    let barrier = Arc::new(Barrier::new(8));
    let mut handles = Vec::new();

    // Spawn 8 concurrent workers
    for _ in 0..8 {
        let conn = db.connect().unwrap();
        let barrier = barrier.clone();

        handles.push(tokio::spawn(async move {
            barrier.wait().await;

            let mut prepared_stmt = conn
                .prepare("INSERT INTO users (email, created_at) VALUES (:email, :created_at)")
                .await
                .unwrap();
            for i in 0..1000 {
                let email = match i % 3 {
                    0 => "seed@example.com",
                    1 => "dup@example.com",
                    2 => "dapper@example.com",
                    _ => panic!("Invalid email index: {i}"),
                };
                let result = prepared_stmt
                    .execute(vec![
                        (":email".to_string(), Value::Text(email.into())),
                        (":created_at".to_string(), Value::Text("whatever".into())),
                    ])
                    .await;
                match result {
                    Ok(_) => (),
                    Err(Error::Constraint(e)) if e.contains("UNIQUE constraint failed") => {}
                    Err(Error::Busy(e)) if e.contains("database is locked") => {}
                    Err(e) => {
                        panic!("Error executing statement: {e:?}");
                    }
                }
            }
        }));
    }

    // Wait for all workers to complete
    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_statement_query_resets_before_execution() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)", ())
        .await
        .unwrap();

    for i in 0..5 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, 'value_{i}')"), ())
            .await
            .unwrap();
    }

    let mut stmt = conn
        .prepare("SELECT id, value FROM t ORDER BY id")
        .await
        .unwrap();

    let mut rows = stmt.query(()).await.unwrap();
    let mut count = 0;
    while let Some(row) = rows.next().await.unwrap() {
        let id: i64 = row.get(0).unwrap();
        assert_eq!(id, count);
        count += 1;
    }
    assert_eq!(count, 5);

    let mut rows = stmt.query(()).await.unwrap();
    let mut count = 0;
    while let Some(row) = rows.next().await.unwrap() {
        let id: i64 = row.get(0).unwrap();
        assert_eq!(id, count);
        count += 1;
    }
    // this will return 0 rows if query() does not reset the statement
    assert_eq!(count, 5, "Second query() should return all rows again");
}

#[tokio::test]
async fn test_encryption() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_file = temp_dir.path().join("test-encrypted.db");
    let db_file = db_file.to_str().unwrap();
    let hexkey = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
    let wrong_key = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    let encryption_opts = EncryptionOpts {
        hexkey: hexkey.to_string(),
        cipher: "aegis256".to_string(),
    };

    // 1. Create encrypted database and insert data
    {
        let builder = Builder::new_local(db_file)
            .experimental_encryption(true)
            .with_encryption(encryption_opts.clone());
        let db = builder.build().await.unwrap();
        let conn = db.connect().unwrap();
        conn.execute(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);",
            (),
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO test (value) VALUES ('secret_data')", ())
            .await
            .unwrap();
        let mut row_count = 0;
        let mut rows = conn.query("SELECT * FROM test", ()).await.unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "secret_data");
            row_count += 1;
        }
        assert_eq!(row_count, 1);

        // Checkpoint to ensure data is written to main db file
        let mut rows = conn
            .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
            .await
            .unwrap();
        while rows.next().await.unwrap().is_some() {}
    }

    // 2. Verify data is encrypted on disk
    let content = std::fs::read(db_file).unwrap();
    assert!(content.len() > 1024);
    assert!(
        !content.windows(11).any(|w| w == b"secret_data"),
        "Plaintext should not appear in encrypted database file"
    );

    // 3. Reopen with correct key and verify data
    {
        let builder = Builder::new_local(db_file)
            .experimental_encryption(true)
            .with_encryption(encryption_opts.clone());
        let db = builder.build().await.unwrap();
        let conn = db.connect().unwrap();

        let mut row_count = 0;
        let mut rows = conn.query("SELECT * FROM test", ()).await.unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "secret_data");
            row_count += 1;
        }
        assert_eq!(row_count, 1);
    }

    // 4. Verify opening with wrong key fails
    {
        let wrong_opts = EncryptionOpts {
            hexkey: wrong_key.to_string(),
            cipher: "aegis256".to_string(),
        };
        let builder = Builder::new_local(db_file)
            .experimental_encryption(true)
            .with_encryption(wrong_opts);
        let result = builder.build().await;
        assert!(result.is_err(), "Opening with wrong key should fail");
    }

    // 5. Verify opening without encryption fails
    {
        let builder = Builder::new_local(db_file).experimental_encryption(true);
        let result = builder.build().await;
        assert!(
            result.is_err(),
            "Opening encrypted database without key should fail"
        );
    }
}

#[tokio::test]
/// This results in a panic if the query isn't correctly reset
async fn test_query_without_reset_does_not_panic() {
    let tempfile = tempfile::NamedTempFile::new().unwrap();
    let db = Builder::new_local(tempfile.path().to_str().unwrap())
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)", ())
        .await
        .unwrap();

    for i in 0..10 {
        conn.execute(&format!("INSERT INTO t VALUES ({i}, 'val')"), ())
            .await
            .unwrap();
    }

    let mut stmts: Vec<Option<turso::Statement>> = Vec::new();

    for round in 0..4 {
        for i in 0..30 {
            let id = 100 + round * 100 + i;
            let sql = match i % 4 {
                0 => format!("INSERT INTO t VALUES ({id}, 'new')"),
                1 => "SELECT * FROM t".to_string(),
                2 => format!("UPDATE t SET value = 'upd' WHERE id = {}", i % 10),
                _ => format!("DELETE FROM t WHERE id = {}", 1000 + i),
            };
            if let Ok(s) = conn.prepare(&sql).await {
                stmts.push(Some(s));
            }
        }

        for i in (0..stmts.len()).step_by(7) {
            if let Some(Some(stmt)) = stmts.get_mut(i) {
                if let Ok(mut rows) = stmt.query(()).await {
                    let _ = rows.next().await;
                }
            }
        }

        for i in 0..3 {
            let _ = conn
                .execute(
                    &format!("INSERT INTO t VALUES ({}, 'x')", 2000 + round * 10 + i),
                    (),
                )
                .await;
        }

        for i in (0..stmts.len()).step_by(13) {
            stmts[i] = None;
        }

        for i in (0..stmts.len()).step_by(5) {
            if let Some(Some(stmt)) = stmts.get_mut(i) {
                if let Ok(mut rows) = stmt.query(()).await {
                    let _ = rows.next().await;
                }
            }
        }
    }
}

// Test Transaction.prepare
#[tokio::test]
async fn test_transaction_prepared_statement() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let mut conn = db.connect().unwrap();

    conn.execute("CREATE TABLE users (id INTEGER, name TEXT)", ())
        .await
        .unwrap();

    let tx = conn.transaction().await.unwrap();
    let mut stmt = tx
        .prepare("INSERT INTO users VALUES (?1, ?2)")
        .await
        .unwrap();
    stmt.execute(["1", "Frodo"]).await.unwrap();
    tx.commit().await.unwrap();

    let row = conn
        .prepare("SELECT id FROM users WHERE name = ?")
        .await
        .unwrap()
        .query_row(&["Frodo"])
        .await
        .unwrap();

    let id: i64 = row.get(0).unwrap();
    assert_eq!(id, 1);
}

#[tokio::test]
async fn test_row_get_value_out_of_bounds() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t (x INTEGER)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1)", ()).await.unwrap();

    let mut rows = conn.query("SELECT x FROM t", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();

    // Valid index works
    assert!(row.get_value(0).is_ok());

    // Out of bounds returns error instead of panicking
    let result = row.get_value(999);
    assert!(matches!(result, Err(Error::Misuse(_))));

    // Also test get<T>() for OOB
    let result: Result<i64, _> = row.get(999);
    assert!(matches!(result, Err(Error::Misuse(_))));
}

// Test Connection clone
#[tokio::test]
async fn test_connection_clone() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let mut conn = db.connect().unwrap();

    conn.execute("CREATE TABLE users (id INTEGER, name TEXT)", ())
        .await
        .unwrap();

    let tx = conn.transaction().await.unwrap();
    let mut stmt = tx
        .prepare("INSERT INTO users VALUES (?1, ?2)")
        .await
        .unwrap();
    stmt.execute(["1", "Frodo"]).await.unwrap();
    tx.commit().await.unwrap();

    let conn2 = conn.clone();
    let row = conn2
        .prepare("SELECT id FROM users WHERE name = ?")
        .await
        .unwrap()
        .query_row(&["Frodo"])
        .await
        .unwrap();

    let id: i64 = row.get(0).unwrap();
    assert_eq!(id, 1);
}

#[tokio::test]
async fn test_insert_returning_partial_consume() {
    // Regression test for: INSERT...RETURNING should insert all rows even if
    // only some RETURNING values are consumed before the statement is dropped/reset.
    // This matches the sqlite3 bindings fix in commit e39e60ef1.
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t (x INTEGER)", ())
        .await
        .unwrap();

    // Use query() to get RETURNING values, but only consume first row
    let mut stmt = conn
        .prepare("INSERT INTO t (x) VALUES (1), (2), (3) RETURNING x")
        .await
        .unwrap();
    let mut rows = stmt.query(()).await.unwrap();

    // Only consume first row
    let first_row = rows.next().await.unwrap().unwrap();
    assert_eq!(first_row.get::<i64>(0).unwrap(), 1);

    // Drop the rows iterator without consuming remaining rows
    drop(rows);
    drop(stmt);

    // All 3 rows should have been inserted despite only consuming 1 RETURNING value
    let mut count_rows = conn.query("SELECT COUNT(*) FROM t", ()).await.unwrap();
    let count: i64 = count_rows.next().await.unwrap().unwrap().get(0).unwrap();
    assert_eq!(
        count, 3,
        "All 3 rows should be inserted even if RETURNING was partially consumed"
    );
}

#[tokio::test]
async fn test_transaction_commit_without_mvcc() {
    // Regression test: COMMIT should work for non-MVCC transactions.
    // The op_auto_commit function must check TransactionState, not just MVCC tx.
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", ())
        .await
        .unwrap();

    // Begin explicit transaction
    conn.execute("BEGIN IMMEDIATE TRANSACTION", ())
        .await
        .unwrap();

    // Insert data within transaction
    conn.execute("INSERT INTO test (id, value) VALUES (1, 'hello')", ())
        .await
        .unwrap();

    // Commit should succeed
    conn.execute("COMMIT", ())
        .await
        .expect("COMMIT should succeed for non-MVCC transactions");

    // Verify data was committed
    let mut rows = conn
        .query("SELECT value FROM test WHERE id = 1", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let value: String = row.get(0).unwrap();
    assert_eq!(value, "hello", "Data should be committed");
}

#[tokio::test]
async fn test_transaction_with_insert_returning_then_commit() {
    // Regression test: Combining INSERT...RETURNING (partial consume) with explicit transaction.
    // This tests the interaction between the reset-to-completion fix and transaction commit.
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t (x INTEGER)", ())
        .await
        .unwrap();

    // Begin transaction
    conn.execute("BEGIN IMMEDIATE TRANSACTION", ())
        .await
        .unwrap();

    // INSERT...RETURNING, only consume first row
    let mut stmt = conn
        .prepare("INSERT INTO t (x) VALUES (1), (2), (3) RETURNING x")
        .await
        .unwrap();
    let mut rows = stmt.query(()).await.unwrap();
    let first = rows.next().await.unwrap().unwrap();
    assert_eq!(first.get::<i64>(0).unwrap(), 1);
    drop(rows);
    drop(stmt);

    // Commit should succeed even after partial RETURNING consumption
    conn.execute("COMMIT", ())
        .await
        .expect("COMMIT should succeed after INSERT...RETURNING");

    // Verify all 3 rows were inserted
    let mut count_rows = conn.query("SELECT COUNT(*) FROM t", ()).await.unwrap();
    let count: i64 = count_rows.next().await.unwrap().unwrap().get(0).unwrap();
    assert_eq!(count, 3, "All rows should be committed");
}

#[tokio::test]
async fn test_prepare_cached_basic() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ())
        .await
        .unwrap();

    // First call should cache the statement
    let mut stmt1 = conn
        .prepare_cached("SELECT * FROM users WHERE id = ?")
        .await
        .unwrap();

    // Insert some data and query
    conn.execute("INSERT INTO users VALUES (1, 'Alice')", ())
        .await
        .unwrap();

    let mut rows = stmt1.query(vec![Value::Integer(1)]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<String>(1).unwrap(), "Alice");
    drop(rows);
    drop(stmt1);

    // Second call should use cached statement
    let mut stmt2 = conn
        .prepare_cached("SELECT * FROM users WHERE id = ?")
        .await
        .unwrap();

    let mut rows = stmt2.query(vec![Value::Integer(1)]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<String>(1).unwrap(), "Alice");
}

#[tokio::test]
async fn test_prepare_cached_reprepare_on_query_only_change() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t (id INTEGER)", ())
        .await
        .unwrap();

    let mut stmt = conn
        .prepare_cached("INSERT INTO t VALUES (?)")
        .await
        .unwrap();

    conn.execute("PRAGMA query_only=1", ()).await.unwrap();

    let err = stmt.execute(vec![Value::Integer(1)]).await.unwrap_err();
    assert!(err.to_string().to_ascii_lowercase().contains("query_only"));

    let mut rows = conn.query("SELECT COUNT(*) FROM t", ()).await.unwrap();
    let count: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_prepare_cached_batch_insert_delete_pattern() {
    #[derive(Clone)]
    struct Host {
        name: String,
        app: String,
        address: String,
        namespace: String,
        cloud_cluster_name: String,
        allowed_ips: Vec<String>,
        updated_at: std::time::SystemTime,
        deleted: bool,
    }

    fn serialize_allowed_ips(allowed_ips: &[String]) -> String {
        allowed_ips.join(",")
    }

    fn system_time_to_unix_seconds(ts: std::time::SystemTime) -> i64 {
        let duration = ts
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after unix epoch");
        duration.as_secs() as i64
    }

    async fn insert_hosts(conn: &turso::Connection, hosts: &[Host]) -> Result<(), Error> {
        if hosts.is_empty() {
            return Ok(());
        }

        conn.execute("BEGIN", ()).await?;

        let mut insert_stmt = conn
            .prepare_cached(
                "INSERT INTO hosts (name, app, address, namespace, cloud_cluster_name, allowed_ips, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(name) DO UPDATE SET
                 app = excluded.app,
                 address = excluded.address,
                 namespace = excluded.namespace,
                 cloud_cluster_name = excluded.cloud_cluster_name,
                 allowed_ips = excluded.allowed_ips,
                 updated_at = excluded.updated_at",
            )
            .await?;
        let mut delete_stmt = conn
            .prepare_cached("DELETE FROM hosts WHERE name = ?1")
            .await?;

        let result = async {
            for host in hosts {
                if host.deleted {
                    delete_stmt.execute([host.name.as_str()]).await?;
                    continue;
                }

                let allowed_ips = serialize_allowed_ips(&host.allowed_ips);
                let updated_at = system_time_to_unix_seconds(host.updated_at);
                insert_stmt
                    .execute((
                        host.name.as_str(),
                        host.app.as_str(),
                        host.address.as_str(),
                        host.namespace.as_str(),
                        host.cloud_cluster_name.as_str(),
                        allowed_ips,
                        updated_at,
                    ))
                    .await?;
            }
            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                conn.execute("COMMIT", ()).await?;
                Ok(())
            }
            Err(err) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                Err(err)
            }
        }
    }

    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute(
        "CREATE TABLE hosts (
            name TEXT PRIMARY KEY,
            app TEXT,
            address TEXT,
            namespace TEXT,
            cloud_cluster_name TEXT,
            allowed_ips TEXT,
            updated_at INTEGER
        )",
        (),
    )
    .await
    .unwrap();

    let base_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
    let hosts = vec![
        Host {
            name: "a".to_string(),
            app: "app_a".to_string(),
            address: "10.0.0.1".to_string(),
            namespace: "ns".to_string(),
            cloud_cluster_name: "cluster".to_string(),
            allowed_ips: vec!["10.0.0.0/24".to_string()],
            updated_at: base_time,
            deleted: false,
        },
        Host {
            name: "b".to_string(),
            app: "app_b".to_string(),
            address: "10.0.0.2".to_string(),
            namespace: "ns".to_string(),
            cloud_cluster_name: "cluster".to_string(),
            allowed_ips: vec!["10.0.1.0/24".to_string()],
            updated_at: base_time,
            deleted: false,
        },
        Host {
            name: "a".to_string(),
            app: "app_a".to_string(),
            address: "10.0.0.1".to_string(),
            namespace: "ns".to_string(),
            cloud_cluster_name: "cluster".to_string(),
            allowed_ips: vec!["10.0.0.0/24".to_string()],
            updated_at: base_time,
            deleted: true,
        },
    ];

    insert_hosts(&conn, &hosts).await.unwrap();

    let mut rows = conn
        .query("SELECT name FROM hosts ORDER BY name", ())
        .await
        .unwrap();
    let first = rows
        .next()
        .await
        .unwrap()
        .unwrap()
        .get::<String>(0)
        .unwrap();
    assert_eq!(first, "b");
    assert!(rows.next().await.unwrap().is_none());
}

#[tokio::test]
async fn test_prepare_cached_multiple_statements() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t (id INTEGER, value TEXT)", ())
        .await
        .unwrap();

    // Cache multiple different statements
    let queries = vec![
        "SELECT * FROM t WHERE id = ?",
        "SELECT * FROM t WHERE value = ?",
        "INSERT INTO t VALUES (?, ?)",
    ];

    for query in &queries {
        let _ = conn.prepare_cached(*query).await.unwrap();
    }

    // All should be cached and work correctly
    let mut stmt1 = conn.prepare_cached(queries[0]).await.unwrap();
    let mut stmt2 = conn.prepare_cached(queries[1]).await.unwrap();
    let mut stmt3 = conn.prepare_cached(queries[2]).await.unwrap();

    // Insert data
    stmt3
        .execute(vec![Value::Integer(1), Value::Text("test".into())])
        .await
        .unwrap();

    // Query using both cached SELECT statements
    let mut rows = stmt1.query(vec![Value::Integer(1)]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    drop(rows);

    let mut rows = stmt2.query(vec![Value::Text("test".into())]).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(1).unwrap(), "test");
}

#[tokio::test]
async fn test_prepare_cached_independent_state() {
    // Verify that each cached statement has independent execution state
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t (id INTEGER)", ())
        .await
        .unwrap();

    for i in 1..=5 {
        conn.execute(&format!("INSERT INTO t VALUES ({i})"), ())
            .await
            .unwrap();
    }

    let query = "SELECT * FROM t ORDER BY id";

    // Get two statements from cache
    let mut stmt1 = conn.prepare_cached(query).await.unwrap();
    let mut stmt2 = conn.prepare_cached(query).await.unwrap();

    // Start iterating with stmt1
    let mut rows1 = stmt1.query(()).await.unwrap();
    let row1 = rows1.next().await.unwrap().unwrap();
    assert_eq!(row1.get::<i64>(0).unwrap(), 1);

    // Start iterating with stmt2 - should have its own state
    let mut rows2 = stmt2.query(()).await.unwrap();
    let row2 = rows2.next().await.unwrap().unwrap();
    assert_eq!(row2.get::<i64>(0).unwrap(), 1);

    // Continue with stmt1 - should be at next row
    let row1 = rows1.next().await.unwrap().unwrap();
    assert_eq!(row1.get::<i64>(0).unwrap(), 2);

    // Continue with stmt2 - should also be at next row (independent state)
    let row2 = rows2.next().await.unwrap().unwrap();
    assert_eq!(row2.get::<i64>(0).unwrap(), 2);
}

#[tokio::test]
async fn test_prepare_cached_with_parameters() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute(
        "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)",
        (),
    )
    .await
    .unwrap();

    conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO users VALUES (2, 'Bob', 25)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO users VALUES (3, 'Charlie', 35)", ())
        .await
        .unwrap();

    let query = "SELECT name FROM users WHERE age > ?";

    // Use cached statement with different parameters
    let mut stmt = conn.prepare_cached(query).await.unwrap();

    let mut rows = stmt.query(vec![Value::Integer(25)]).await.unwrap();
    let mut names = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(0).unwrap());
    }
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
    drop(rows);

    // Reuse cached statement with different parameter
    let mut rows = stmt.query(vec![Value::Integer(30)]).await.unwrap();
    let mut names = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        names.push(row.get::<String>(0).unwrap());
    }
    assert_eq!(names.len(), 1);
    assert_eq!(names[0], "Charlie");
}

#[tokio::test]
async fn test_prepare_cached_stress() {
    // Stress test to ensure cache works correctly under repeated use
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)", ())
        .await
        .unwrap();

    let insert_query = "INSERT INTO t (id, value) VALUES (?, ?)";
    let select_query = "SELECT value FROM t WHERE id = ?";

    // Insert many rows using cached statement
    for i in 0..100 {
        let mut stmt = conn.prepare_cached(insert_query).await.unwrap();
        stmt.execute(vec![Value::Integer(i), Value::Text(format!("value_{i}"))])
            .await
            .unwrap();
    }

    // Query many times using cached statement
    for i in 0..100 {
        let mut stmt = conn.prepare_cached(select_query).await.unwrap();
        let mut rows = stmt.query(vec![Value::Integer(i)]).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<String>(0).unwrap(), format!("value_{i}"));
    }
}

#[tokio::test]
async fn test_prepare_vs_prepare_cached_equivalence() {
    // Verify that prepare_cached produces same results as prepare
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE t (x INTEGER, y TEXT)", ())
        .await
        .unwrap();

    conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')", ())
        .await
        .unwrap();

    let query = "SELECT * FROM t ORDER BY x";

    // Results from prepare
    let mut stmt1 = conn.prepare(query).await.unwrap();
    let mut rows1 = stmt1.query(()).await.unwrap();
    let mut results1 = Vec::new();
    while let Some(row) = rows1.next().await.unwrap() {
        results1.push((row.get::<i64>(0).unwrap(), row.get::<String>(1).unwrap()));
    }

    // Results from prepare_cached
    let mut stmt2 = conn.prepare_cached(query).await.unwrap();
    let mut rows2 = stmt2.query(()).await.unwrap();
    let mut results2 = Vec::new();
    while let Some(row) = rows2.next().await.unwrap() {
        results2.push((row.get::<i64>(0).unwrap(), row.get::<String>(1).unwrap()));
    }

    // Should produce identical results
    assert_eq!(results1, results2);
    assert_eq!(
        results1,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
        ]
    );
}

/// This will fail if self.once is not reset in ProgramState::reset.
#[tokio::test]
async fn test_once_not_cleared_on_reset_with_coroutine() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    // This query generates bytecode with Once inside a coroutine:
    // The outer FROM-clause subquery creates a coroutine, and the inner
    // scalar subquery (SELECT 1) uses Once to evaluate only once per execution.
    let mut stmt = conn
        .prepare("SELECT * FROM (SELECT (SELECT 1))")
        .await
        .unwrap();

    let mut rows = stmt.query(()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let value: i64 = row.get(0).unwrap();
    assert_eq!(value, 1);
    assert!(rows.next().await.unwrap().is_none());
    drop(rows);

    stmt.reset().unwrap();

    let mut rows = stmt.query(()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();

    assert_eq!(
        row.get_value(0).unwrap(),
        Value::Integer(1),
        "Second execution should return 1, not Null. Bug: state.once not cleared in reset()"
    );
}

#[tokio::test]
async fn test_experimental_strict_tables() {
    // Test that STRICT tables work when the experimental flag is enabled
    let db = Builder::new_local(":memory:")
        .experimental_strict(true)
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();

    // Create a STRICT table
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT) STRICT",
        (),
    )
    .await
    .unwrap();


    // Insert valid data
    conn.execute("INSERT INTO users VALUES (1, 'Alice')", ())
        .await
        .unwrap();

    // Query the data
    let mut rows = conn.query("SELECT id, name FROM users", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<String>(1).unwrap(), "Alice");
}

#[tokio::test]
async fn test_strict_tables_without_experimental_flag() {
    // Test that STRICT tables fail without the experimental flag
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    // Attempt to create a STRICT table should fail
    let result = conn
        .execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT) STRICT",
            (),
        )
        .await;

    // Verify the error message mentions experimental feature
    assert!(matches!(result, Err(Error::Error(_))));
}

// Helper to collect all integer values from a single-column query.
async fn collect_ids(conn: &turso::Connection, sql: &str) -> Vec<i64> {
    let mut rows = conn.query(sql, ()).await.unwrap();
    let mut ids = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        let id: i64 = row.get(0).unwrap();
        ids.push(id);
    }
    ids
}

#[tokio::test]
async fn test_check_on_conflict_fail() {
    // FAIL: error on the violating statement, transaction stays active.
    // Prior inserts within the transaction are preserved and can be committed.
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, value INTEGER CHECK(value > 0))",
        (),
    )
    .await
    .unwrap();
    conn.execute("BEGIN", ()).await.unwrap();
    conn.execute("INSERT INTO t VALUES(1, 10)", ())
        .await
        .unwrap();

    // This should fail but keep the transaction active
    let err = conn
        .execute("INSERT OR FAIL INTO t VALUES(2, -5)", ())
        .await;
    assert!(
        err.is_err(),
        "INSERT OR FAIL should error on CHECK violation"
    );

    // Transaction is still active — commit it
    conn.execute("COMMIT", ()).await.unwrap();

    // Row 1 should have survived
    let ids = collect_ids(&conn, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(ids, vec![1]);
}

#[tokio::test]
async fn test_check_on_conflict_abort() {
    // ABORT (default): error on the violating statement, transaction stays active.
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, value INTEGER CHECK(value > 0))",
        (),
    )
    .await
    .unwrap();
    conn.execute("BEGIN", ()).await.unwrap();
    conn.execute("INSERT INTO t VALUES(1, 10)", ())
        .await
        .unwrap();

    let err = conn
        .execute("INSERT OR ABORT INTO t VALUES(2, -5)", ())
        .await;
    assert!(
        err.is_err(),
        "INSERT OR ABORT should error on CHECK violation"
    );

    conn.execute("COMMIT", ()).await.unwrap();

    let ids = collect_ids(&conn, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(ids, vec![1]);
}

#[tokio::test]
async fn test_check_on_conflict_rollback() {
    // ROLLBACK: rolls back the entire transaction.
    // Prior inserts within the transaction are lost, but committed rows survive.
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, value INTEGER CHECK(value > 0))",
        (),
    )
    .await
    .unwrap();
    // Commit row 1 outside the transaction
    conn.execute("INSERT INTO t VALUES(1, 10)", ())
        .await
        .unwrap();

    conn.execute("BEGIN", ()).await.unwrap();
    conn.execute("INSERT INTO t VALUES(2, 20)", ())
        .await
        .unwrap();

    // This should fail AND roll back the transaction
    let err = conn
        .execute("INSERT OR ROLLBACK INTO t VALUES(3, -5)", ())
        .await;
    assert!(
        err.is_err(),
        "INSERT OR ROLLBACK should error on CHECK violation"
    );

    // Transaction was rolled back — row 2 is lost, row 1 survives
    let ids = collect_ids(&conn, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(ids, vec![1]);
}

#[tokio::test]
async fn test_check_on_conflict_replace() {
    // REPLACE: for CHECK constraints, behaves like ABORT.
    // Error, transaction stays active.
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, value INTEGER CHECK(value > 0))",
        (),
    )
    .await
    .unwrap();
    conn.execute("BEGIN", ()).await.unwrap();
    conn.execute("INSERT INTO t VALUES(1, 10)", ())
        .await
        .unwrap();

    let err = conn
        .execute("INSERT OR REPLACE INTO t VALUES(1, -5)", ())
        .await;
    assert!(
        err.is_err(),
        "INSERT OR REPLACE should error on CHECK violation"
    );

    conn.execute("COMMIT", ()).await.unwrap();

    let ids = collect_ids(&conn, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(ids, vec![1]);
}
