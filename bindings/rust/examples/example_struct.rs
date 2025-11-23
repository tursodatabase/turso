use turso::{Builder, Error};

#[derive(Debug)]
struct User {
    email: String,
    age: i32,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let db = Builder::new_local(":memory:")
        .build()
        .await
        .expect("Turso Failed to Build memory db");

    let conn = db.connect()?;

    conn.query("select 1; select 1;", ()).await?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS users (email TEXT, age INTEGER)",
        (),
    )
    .await?;

    conn.pragma_query("journal_mode", |row| {
        println!("{:?}", row.get_value(0));
        Ok(())
    })?;

    let mut stmt = conn
        .prepare("INSERT INTO users (email, age) VALUES (?1, ?2)")
        .await?;

    stmt.execute(["foo@example.com", &21.to_string()]).await?;
    stmt.execute(["bar@example.com", &22.to_string()]).await?;

    let mut stmt = conn
        .prepare("SELECT * FROM users WHERE email like ?1")
        .await?;

    let mut rows = stmt.query(["%@example.com"]).await?;

    loop {
        let row = rows.next().await?;
        if let Some(row_values) = row {
            let u: User = User {
                email: row_values.get(0)?,
                age: row_values.get(1)?,
            };
            println!("Row: {} {}", u.email, u.age);
        } else {
            break;
        }
    }

    Ok(())
}
