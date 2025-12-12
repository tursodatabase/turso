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
       WHERE  a.id = 1;       -- Alice’s five books
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
                    Err(Error::SqlExecutionFailure(e))
                        if e.contains("UNIQUE constraint failed")
                            | e.contains("database is locked") => {}
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
async fn test_encryption() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_file = temp_dir.path().join("test-encrypted.db");
    let db_file = db_file.to_str().unwrap();
    let encryption_opts = EncryptionOpts {
        hexkey: "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327".to_string(),
        cipher: "aegis256".to_string(),
    };
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
        conn.execute("INSERT INTO test (value) VALUES ('Hello, World!')", ())
            .await
            .unwrap();
        let mut row_count = 0;
        let mut rows = conn.query("SELECT * FROM test", ()).await.unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, World!");
            row_count += 1;
        }
        assert_eq!(row_count, 1);
    }

    // lets verify we can open an existing encrypted db
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
            assert_eq!(row.get::<String>(1).unwrap(), "Hello, World!");
            row_count += 1;
        }
        assert_eq!(row_count, 1);
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
