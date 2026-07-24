# Turso Serverless Driver for Rust

A Rust driver for Turso Cloud that speaks the [SQL over HTTP
protocol](https://github.com/tursodatabase/turso/blob/main/serverless/PROTOCOL.md).
Designed for serverless and edge environments: no persistent connections,
no WebSockets, just HTTP requests.

The API mirrors the embedded [`turso`](https://crates.io/crates/turso)
driver, so the same application code can run against a local database or
Turso Cloud.

## Usage

```rust
use turso_serverless::Builder;

#[tokio::main]
async fn main() -> turso_serverless::Result<()> {
    let db = Builder::new_remote(std::env::var("TURSO_DATABASE_URL").unwrap())
        .with_auth_token(std::env::var("TURSO_AUTH_TOKEN").unwrap())
        .build()
        .await?;
    let conn = db.connect()?;

    conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)", ())
        .await?;
    conn.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
        .await?;

    let mut rows = conn.query("SELECT id, name FROM users", ()).await?;
    while let Some(row) = rows.next().await? {
        println!("{} {}", row.get::<i64>(0)?, row.get::<String>(1)?);
    }
    Ok(())
}
```

Interactive transactions span multiple HTTP requests; the server keeps the
connection state alive between them:

```rust
# async fn run(mut conn: turso_serverless::Connection) -> turso_serverless::Result<()> {
let tx = conn.transaction().await?;
tx.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1", ()).await?;
tx.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2", ()).await?;
tx.commit().await?;
# Ok(())
# }
```

## Conformance tests

The [`conformance`](https://github.com/tursodatabase/turso/tree/main/serverless/rust/conformance)
crate tests the driver and the wire
protocol against a live database. Point it at a Turso Cloud instance:

```console
$ export TURSO_DATABASE_URL=libsql://<your-db>.turso.io
$ export TURSO_AUTH_TOKEN=<your-token>
$ cargo test -p turso_serverless_conformance
```

The tests skip themselves when the environment variables are not set.

There are two suites:

* `tests/driver.rs` — exercises the public driver API: queries, parameter
  binding, prepared statements, transactions, error mapping.
* `tests/protocol.rs` — exercises the SQL over HTTP protocol directly with
  raw HTTP requests, asserting the wire behavior specified in
  [PROTOCOL.md](https://github.com/tursodatabase/turso/blob/main/serverless/PROTOCOL.md):
  pipelines, batons, batch conditions, cursors, value encodings, and error
  reporting.
