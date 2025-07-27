use tokio::fs;
use turso::{Builder, Value};

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
    conn.execute("INSERT INTO test (x) VALUES (2)", ())
        .await
        .unwrap();
    conn.execute(
        "INSERT INTO test (x) VALUES (:x)",
        vec![(":x".to_string(), Value::Integer(3))],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO test (x) VALUES (@x)",
        vec![("@x".to_string(), Value::Integer(4))],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO test (x) VALUES ($x)",
        vec![("$x".to_string(), Value::Integer(5))],
    )
    .await
    .unwrap();
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
