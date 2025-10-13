# turso

The next evolution of SQLite: A high-performance, SQLite-compatible database library for Rust

## Features

- **SQLite Compatible**: Similar interface to rusqlite with familiar API apart from using async Rust
- **High Performance**: Built with Rust for maximum speed and efficiency  
- **Async/Await Support**: Native async operations with tokio support
- **In-Process**: No network overhead, runs directly in your application
- **Cross-Platform**: Supports Linux, macOS, and Windows
- **Transaction Support**: Full ACID transactions with rollback support
- **Prepared Statements**: Optimized query execution with parameter binding

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
turso = "0.2"
tokio = { version = "1.0", features = ["full"] }
```

## Quick Start

### In-Memory Database

```rust
use turso::Builder;

#[tokio::main]
async fn main() -> turso::Result<()> {
    // Create an in-memory database
    let db = Builder::new_local(":memory:").build().await?;
    let conn = db.connect()?;

    // Create a table
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
        ()
    ).await?;

    // Insert data
    conn.execute(
        "INSERT INTO users (name, email) VALUES (?1, ?2)",
        ["Alice", "alice@example.com"]
    ).await?;

    conn.execute(
        "INSERT INTO users (name, email) VALUES (?1, ?2)", 
        ["Bob", "bob@example.com"]
    ).await?;

    // Query data
    let mut rows = conn.query("SELECT * FROM users", ()).await?;
    
    while let Some(row) = rows.try_next().await? {
        let id = row.get_value(0)?;
        let name = row.get_value(1)?;
        let email = row.get_value(2)?;
        println!("User: {} - {} ({})", 
            id.as_integer().unwrap_or(&0), 
            name.as_text().unwrap_or(&"".to_string()), 
            email.as_text().unwrap_or(&"".to_string())
        );
    }

    Ok(())
}
```

### File-Based Database

```rust
use turso::Builder;

#[tokio::main] 
async fn main() -> turso::Result<()> {
    // Create or open a database file
    let db = Builder::new_local("my-database.db").build().await?;
    let conn = db.connect()?;

    // Create a table
    conn.execute(
        r#"CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            content TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )"#,
        ()
    ).await?;

    // Insert a post
    let rows_affected = conn.execute(
        "INSERT INTO posts (title, content) VALUES (?1, ?2)",
        ["Hello World", "This is my first blog post!"]
    ).await?;

    println!("Inserted {} rows", rows_affected);

    Ok(())
}
```

## API Reference

### Builder

Create a new database:

```rust
let db = Builder::new_local(":memory:").build().await?;
let db = Builder::new_local("data.db").build().await?;
```

### Connection

Execute queries and statements:

```rust
// Execute SQL directly
let rows_affected = conn.execute("INSERT INTO users (name) VALUES (?1)", ["Alice"]).await?;

// Query for multiple rows
let mut rows = conn.query("SELECT * FROM users WHERE age > ?1", [18]).await?;

// Prepare statements for reuse
let mut stmt = conn.prepare("SELECT * FROM users WHERE id = ?1").await?;
let mut rows = stmt.query([42]).await?;

// Execute prepared statements
let rows_affected = stmt.execute(["Alice"]).await?;
```

### Working with Results

```rust
use futures_util::TryStreamExt;

let mut rows = conn.query("SELECT name, email FROM users", ()).await?;

while let Some(row) = rows.try_next().await? {
    let name = row.get_value(0)?.as_text().unwrap_or(&"".to_string());
    let email = row.get_value(1)?.as_text().unwrap_or(&"".to_string());
    println!("{}: {}", name, email);
}
```

## License

MIT

## Support

- [GitHub Issues](https://github.com/tursodatabase/turso/issues)
- [Discord Community](https://discord.gg/turso)
