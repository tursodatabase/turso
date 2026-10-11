//! Demo: hold a Rows handle open, COMMIT on the same connection, keep reading.
//! Reproduces "transaction should exist in txs map" (turso-server#2972)
//! through the public Rust client API.
use turso::Builder;

#[tokio::main]
async fn main() {
    let dir = std::env::temp_dir().join(format!("paused-read-demo-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("demo.db");
    let db = Builder::new_local(path.to_str().unwrap())
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();
    let mut pragma = conn.query("PRAGMA journal_mode='mvcc'", ()).await.unwrap();
    while pragma.next().await.unwrap().is_some() {}
    drop(pragma);
    conn.execute("CREATE TABLE a(x INTEGER)", ()).await.unwrap();
    conn.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO a VALUES (1), (2)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO b VALUES (1, 'one'), (2, 'two')", ())
        .await
        .unwrap();
    conn.execute("BEGIN CONCURRENT", ()).await.unwrap();

    // Hold the rows handle open after the first row.
    let mut rows = conn
        .query("SELECT b.v FROM a JOIN b ON b.id = a.x", ())
        .await
        .unwrap();
    let first = rows.next().await.unwrap();
    println!("first row: {first:?}");

    // A new statement on the SAME connection. Legal in SQLite (COMMIT is
    // allowed while read statements are pending since 3.7.11).
    conn.execute("COMMIT", ()).await.unwrap();
    println!("committed while rows handle still open");

    // Resume the held-open rows handle: panics today.
    let second = rows.next().await;
    println!("second row: {second:?}");
}
