//! Turso Database Sync example with Turso Cloud (with optional remote encryption)
//!
//! Environment variables:
//!   TURSO_REMOTE_URL              - Remote database URL (default: http://localhost:8080)
//!   TURSO_AUTH_TOKEN              - Auth token (optional)
//!   TURSO_REMOTE_ENCRYPTION_KEY   - Base64-encoded encryption key (optional)
//!   TURSO_REMOTE_ENCRYPTION_CIPHER - Cipher name (default: aes256gcm)
//!

use std::env;

use turso::sync::Builder;
use turso::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let remote_url =
        env::var("TURSO_REMOTE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());
    let auth_token = env::var("TURSO_AUTH_TOKEN").ok();
    let encryption_key = env::var("TURSO_REMOTE_ENCRYPTION_KEY").ok();
    let encryption_cipher =
        env::var("TURSO_REMOTE_ENCRYPTION_CIPHER").unwrap_or_else(|_| "aes256gcm".to_string());

    println!("Remote URL: {remote_url}");
    println!("Auth Token: {}", auth_token.is_some());
    println!("Encryption: {}", encryption_key.is_some());
    if encryption_key.is_some() {
        println!("Cipher: {encryption_cipher}");
    }

    let mut builder = Builder::new_remote(":memory:").with_remote_url(&remote_url);

    if let Some(token) = auth_token {
        builder = builder.with_auth_token(token);
    }

    if let Some(key) = encryption_key {
        builder = builder.with_remote_encryption(key, &encryption_cipher);
    }

    let db = builder.build().await?;
    let conn = db.connect().await?;

    conn.execute("CREATE TABLE IF NOT EXISTS t (x TEXT)", ())
        .await?;

    let mut stmt = conn.prepare("SELECT COUNT(*) FROM t").await?;
    let mut rows = stmt.query(()).await?;
    let count: i64 = if let Some(row) = rows.next().await? {
        row.get(0)?
    } else {
        0
    };
    let next = count + 1;
    conn.execute(&format!("INSERT INTO t VALUES ('hello sync #{next}')"), ())
        .await?;
    db.push().await?;

    println!("\nTest table contents:");
    let mut stmt = conn.prepare("SELECT * FROM t").await?;
    let mut rows = stmt.query(()).await?;
    while let Some(row) = rows.next().await? {
        println!("  Row: {:?}", row.get_value(0)?);
    }

    // query sqlite_master for all tables
    println!("\nDatabase tables:");
    let mut stmt = conn
        .prepare("SELECT name, type FROM sqlite_master WHERE type='table'")
        .await?;
    let mut rows = stmt.query(()).await?;
    while let Some(row) = rows.next().await? {
        let name = row.get_value(0)?;
        let typ = row.get_value(1)?;
        println!("  - {typ:?}: {name:?}");
    }

    // sho database stats
    let stats = db.stats().await?;
    println!("\nDatabase stats:");
    println!("  Network received: {} bytes", stats.network_received_bytes);
    println!("  Network sent: {} bytes", stats.network_sent_bytes);
    println!("  Main WAL size: {} bytes", stats.main_wal_size);

    println!("\nDone!");
    Ok(())
}
