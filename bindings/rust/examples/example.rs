use futures::StreamExt;
use turso::{Builder, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let db = Builder::new_local(":memory:")
        .build()
        .await
        .expect("Turso Failed to Build memory db");

    let conn = db.connect()?;

    let rows = conn.query("select 1; select 1;", ()).await?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS users (email TEXT, age INTEGER)",
        (),
    )
    .await?;

    conn.pragma_query("journal_mode", |row| {
        println!("{:?}", row.get_value(0));
        Ok(())
    })
    .await?;

    let mut stmt = conn
        .prepare("INSERT INTO users (email, age) VALUES (?1, ?2)")
        .await?;

    stmt.execute(["foo@example.com", &21.to_string()]).await?;

    let mut stmt = conn.prepare("SELECT * FROM users WHERE email = ?1").await?;

    let mut rows = stmt.query(["foo@example.com"]).await?;

    while let Some(row) = rows.next().await {
        let row = row?;
        let email = row.get_value(0)?;
        let age = row.get_value(1)?;
        println!("Row: {email:?} {age:?}");
    }

    Ok(())
}
