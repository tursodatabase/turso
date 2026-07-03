//! Key-value interface exploration (#375).
//!
//! Many applications only want get/put/delete/scan. This example shows the
//! smallest useful KV layer over turso: one ordinary table per store (so the
//! file stays plain SQLite and works with MVCC, CDC and sync for free) and
//! cached prepared statements so repeated operations skip SQL preparation.
//!
//! The point of the exercise is the API shape; a cursor-direct fast path
//! inside core that bypasses SQL entirely could later slot in behind this
//! same interface.
//!
//! Run with:
//!   cargo run --example kv_store

use turso::{Builder, Connection, Error, Value};

/// A named key-value store backed by a `kv_<name>` table with BLOB keys and
/// values. Keys are unique; `put` overwrites.
struct KvStore {
    conn: Connection,
    get_sql: String,
    put_sql: String,
    delete_sql: String,
    scan_sql: String,
}

impl KvStore {
    /// Open (creating if needed) the store `name` on this connection.
    async fn open(conn: Connection, name: &str) -> Result<Self, Error> {
        assert!(
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "store name must be [A-Za-z0-9_]"
        );
        let table = format!("kv_{name}");
        conn.execute(
            &format!("CREATE TABLE IF NOT EXISTS {table} (k BLOB PRIMARY KEY, v BLOB NOT NULL)"),
            (),
        )
        .await?;
        Ok(Self {
            get_sql: format!("SELECT v FROM {table} WHERE k = ?1"),
            put_sql: format!("INSERT INTO {table} (k, v) VALUES (?1, ?2) \
                              ON CONFLICT(k) DO UPDATE SET v = excluded.v"),
            delete_sql: format!("DELETE FROM {table} WHERE k = ?1"),
            scan_sql: format!("SELECT k, v FROM {table} WHERE k >= ?1 AND k < ?2 ORDER BY k"),
            conn,
        })
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let mut stmt = self.conn.prepare_cached(&self.put_sql).await?;
        stmt.execute((key.to_vec(), value.to_vec())).await?;
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let mut stmt = self.conn.prepare_cached(&self.get_sql).await?;
        let mut rows = stmt.query((key.to_vec(),)).await?;
        match rows.next().await? {
            Some(row) => match row.get_value(0)? {
                Value::Blob(v) => Ok(Some(v)),
                other => panic!("value column must be BLOB, got {other:?}"),
            },
            None => Ok(None),
        }
    }

    /// Returns true when the key existed.
    async fn delete(&self, key: &[u8]) -> Result<bool, Error> {
        let mut stmt = self.conn.prepare_cached(&self.delete_sql).await?;
        Ok(stmt.execute((key.to_vec(),)).await? > 0)
    }

    /// All pairs whose key starts with `prefix`, in key order.
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        // Half-open range [prefix, successor(prefix)): the successor bumps the
        // last byte, dropping trailing 0xff bytes that cannot be bumped.
        let mut upper = prefix.to_vec();
        while let Some(last) = upper.last_mut() {
            if *last < 0xff {
                *last += 1;
                break;
            }
            upper.pop();
        }
        if prefix.is_empty() || upper.is_empty() {
            // Full scan: every key is >= the empty prefix.
            let mut stmt = self
                .conn
                .prepare_cached(self.scan_sql.split(" WHERE").next().unwrap())
                .await?;
            return collect_pairs(stmt.query(()).await?).await;
        }
        let mut stmt = self.conn.prepare_cached(&self.scan_sql).await?;
        collect_pairs(stmt.query((prefix.to_vec(), upper)).await?).await
    }
}

async fn collect_pairs(mut rows: turso::Rows) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        match (row.get_value(0)?, row.get_value(1)?) {
            (Value::Blob(k), Value::Blob(v)) => out.push((k, v)),
            other => panic!("expected BLOB pair, got {other:?}"),
        }
    }
    Ok(out)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let db = Builder::new_local(":memory:").build().await?;
    let store = KvStore::open(db.connect()?, "settings").await?;

    // put / get / overwrite
    store.put(b"user:1:name", b"morgana").await?;
    store.put(b"user:1:lang", b"uk").await?;
    store.put(b"user:2:name", b"pizza").await?;
    store.put(b"user:1:name", b"morgana_future").await?;
    assert_eq!(
        store.get(b"user:1:name").await?.as_deref(),
        Some(b"morgana_future".as_ref())
    );
    assert_eq!(store.get(b"missing").await?, None);

    // prefix scan
    let user1 = store.scan_prefix(b"user:1:").await?;
    assert_eq!(
        user1.iter().map(|(k, _)| k.as_slice()).collect::<Vec<_>>(),
        vec![b"user:1:lang".as_ref(), b"user:1:name".as_ref()]
    );
    let all = store.scan_prefix(b"").await?;
    assert_eq!(all.len(), 3);

    // delete
    assert!(store.delete(b"user:2:name").await?);
    assert!(!store.delete(b"user:2:name").await?);
    assert_eq!(store.scan_prefix(b"").await?.len(), 2);

    // The store is a plain table: SQL and KV views stay consistent.
    let db_rows = db
        .connect()?
        .query("SELECT COUNT(*) FROM kv_settings", ())
        .await?
        .next()
        .await?
        .expect("count row");
    assert_eq!(db_rows.get::<i64>(0)?, 2);

    println!("kv_store example: all assertions passed");
    Ok(())
}
