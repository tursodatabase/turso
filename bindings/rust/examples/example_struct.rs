use turso::{Builder, Connection, Error};

#[derive(Debug)]
struct User {
    email: String,
    age: i32,
}

async fn create_tables(conn: &Connection) -> Result<(), Error> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users (email TEXT, age INTEGER)",
        (),
    )
    .await?;
    Ok(())
}

// If I want to be able to flexibly combine this in the future then
// I should pass in a Transaction not a Connection so that it can
// be part of a bigger Transaction
// But if I pass in a transaction I can't prepare the statement here.
async fn insert_users(conn: &mut Connection) -> Result<(), Error> {
    let mut stmt = conn
        .prepare("INSERT INTO users (email, age) VALUES (?1, ?2)")
        .await?;
    let tx = conn.transaction().await?;
    stmt.execute(["foo@example.com", &21.to_string()]).await?;
    stmt.execute(["bar@example.com", &22.to_string()]).await?;
    tx.commit().await?;
    Ok(())
}

async fn list_users(conn: &Connection) -> Result<(), Error> {
    let mut stmt = conn
        .prepare("SELECT * FROM users WHERE email like ?1")
        .await?;

    let mut rows = stmt.query(["%@example.com"]).await?;

    while let Some(row) = rows.next().await? {
        let u: User = User {
            email: row.get(0)?,
            age: row.get(1)?,
        };
        println!("Row: {} {}", u.email, u.age);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let db = Builder::new_local(":memory:")
        .build()
        .await
        .expect("Turso Failed to Build memory db");

    let mut conn = db.connect()?;

    create_tables(&conn).await?;
    insert_users(&mut conn).await?;
    list_users(&conn).await?;

    Ok(())
}
