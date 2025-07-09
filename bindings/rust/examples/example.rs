#![cfg(feature = "futures")]

use futures_util::stream::TryStreamExt;
use turso::Builder;

#[tokio::main]
async fn main() {
    let db = Builder::new_local(":memory:").build().await.unwrap();

    let conn = db.connect().unwrap();

    // `query` and other methods, only parse the first query given.
    let mut rows = conn.query("select 1; select 1;", ()).await.unwrap();
    // Iterate over the rows with the Stream iterator syntax
    while let Some(row) = rows.try_next().await.unwrap() {
        let val = row.get_value(0).unwrap();
        println!("{:?}", val);
    }

    // Contrary to `prepare` and `query`, `execute` is not lazy and will execute the query to completion
    conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ())
        .await
        .unwrap();

    conn.pragma_query("journal_mode", |row| {
        println!("{:?}", row.get_value(0));
        Ok(())
    })
    .unwrap();

    let mut stmt = conn
        .prepare("INSERT INTO users (email) VALUES (?1)")
        .await
        .unwrap();

    stmt.execute(["foo@example.com"]).await.unwrap();

    let mut stmt = conn
        .prepare("SELECT * FROM users WHERE email = ?1")
        .await
        .unwrap();

    let mut rows = stmt.query(["foo@example.com"]).await.unwrap();

    while let Some(row) = rows.try_next().await.unwrap() {
        let value = row.get_value(0).unwrap();
        println!("Row: {:?}", value);
    }

    let rows = stmt.query(["foo@example.com"]).await.unwrap();

    // As `Rows` implement streams you can easily map over it and apply other transformations you see fit
    println!("Using Stream map");
    rows.map_ok(|row| row.get_value(0).unwrap())
        .try_for_each(|val| {
            println!("Row: {:?}", val.as_text().unwrap());
            futures_util::future::ready(Ok(()))
        })
        .await
        .unwrap();
}
