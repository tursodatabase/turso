use tempfile::tempdir;
use turso::{Builder, Value};

async fn drain(conn: &turso::Connection, sql: &str) {
    let mut rows = conn.query(sql, ()).await.unwrap();
    while rows.next().await.unwrap().is_some() {}
}

async fn ints(conn: &turso::Connection, sql: &str) -> Vec<i64> {
    let mut rows = conn.query(sql, ()).await.unwrap();
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        match row.get_value(0).unwrap() {
            Value::Integer(i) => out.push(i),
            other => panic!("not an integer: {other:?}"),
        }
    }
    out
}

async fn open_db() -> (tempfile::TempDir, turso::Database) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("f1.db");
    let path = path.to_str().unwrap();
    {
        let db = Builder::new_local(path).build().await.unwrap();
        let conn = db.connect().unwrap();
        drain(&conn, "PRAGMA journal_mode = mvcc").await;
        conn.execute("PRAGMA mvcc_checkpoint_threshold = -1", ())
            .await
            .unwrap();
        conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, v TEXT)", ())
            .await
            .unwrap();
        conn.execute("CREATE TABLE child(pid INTEGER REFERENCES parent(id))", ())
            .await
            .unwrap();
        conn.execute("CREATE SEQUENCE s START 1 INCREMENT 1", ())
            .await
            .unwrap();
        conn.execute("INSERT INTO parent VALUES (1, 'a'), (100, 'z')", ())
            .await
            .unwrap();
        drain(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").await;
    }
    let db = Builder::new_local(path)
        .experimental_mvcc_passive_checkpoint(true)
        .build()
        .await
        .unwrap();
    (dir, db)
}

async fn poisoned_shared_cache(
    poisoner_probes_parent: bool,
) -> (tempfile::TempDir, turso::Database, turso::Connection) {
    let (dir, db) = open_db().await;
    let victim = db.connect().unwrap();
    let poisoner = db.connect().unwrap();
    let writer = db.connect().unwrap();
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = -1", ())
        .await
        .unwrap();
    victim
        .execute("PRAGMA foreign_keys = ON", ())
        .await
        .unwrap();

    victim.execute("BEGIN CONCURRENT", ()).await.unwrap();
    assert_eq!(
        ints(&victim, "SELECT id FROM parent WHERE id = 100").await,
        [100]
    );
    poisoner.execute("BEGIN CONCURRENT", ()).await.unwrap();
    assert_eq!(
        ints(&poisoner, "SELECT id FROM parent WHERE id = 100").await,
        [100]
    );

    writer
        .execute("DELETE FROM parent WHERE id = 100", ())
        .await
        .unwrap();

    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = 0", ())
        .await
        .unwrap();
    let next = ints(&poisoner, "SELECT nextval('s')").await;
    assert_eq!(next, [1]);
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = -1", ())
        .await
        .unwrap();

    if poisoner_probes_parent {
        poisoner
            .execute("INSERT INTO parent VALUES (7, 'seven')", ())
            .await
            .unwrap();
    }

    (dir, db, victim)
}

#[tokio::test]
async fn sql_only_fk_parent_visible_to_old_snapshot_is_reported_missing() {
    let (_dir, _db, victim) = poisoned_shared_cache(true).await;
    assert_eq!(
        ints(&victim, "SELECT id FROM parent WHERE id = 100").await,
        [100]
    );
    let result = victim
        .execute("INSERT INTO child(pid) VALUES (100)", ())
        .await;
    assert!(
        result.is_ok(),
        "parent rowid 100 is visible to this snapshot, but the FK check said it does not exist: {result:?}"
    );
}

#[tokio::test]
async fn sql_only_duplicate_rowid_visible_to_old_snapshot_is_accepted() {
    let (_dir, _db, victim) = poisoned_shared_cache(true).await;
    assert_eq!(
        ints(&victim, "SELECT id FROM parent WHERE id = 100").await,
        [100]
    );
    let result = victim
        .execute("INSERT INTO parent VALUES (100, 'dup')", ())
        .await;
    assert!(
        result.is_err(),
        "rowid 100 is visible to this snapshot, but the INSERT was accepted"
    );
}

#[tokio::test]
async fn control_sql_only_fk_parent_visible_without_poisoned_cache() {
    let (_dir, _db, victim) = poisoned_shared_cache(false).await;
    victim
        .execute("INSERT INTO child(pid) VALUES (100)", ())
        .await
        .unwrap();
}

#[tokio::test]
async fn control_sql_only_duplicate_rowid_rejected_without_poisoned_cache() {
    let (_dir, _db, victim) = poisoned_shared_cache(false).await;
    let err = victim
        .execute("INSERT INTO parent VALUES (100, 'dup')", ())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("UNIQUE"), "{err}");
}

#[tokio::test]
async fn control_sql_only_probe_without_inner_tx_checkpoint_is_harmless() {
    let (_dir, db) = open_db().await;
    let victim = db.connect().unwrap();
    let poisoner = db.connect().unwrap();
    let writer = db.connect().unwrap();
    writer
        .execute("PRAGMA mvcc_checkpoint_threshold = -1", ())
        .await
        .unwrap();
    victim
        .execute("PRAGMA foreign_keys = ON", ())
        .await
        .unwrap();
    victim.execute("BEGIN CONCURRENT", ()).await.unwrap();
    assert_eq!(
        ints(&victim, "SELECT id FROM parent WHERE id = 100").await,
        [100]
    );
    poisoner.execute("BEGIN CONCURRENT", ()).await.unwrap();
    assert_eq!(
        ints(&poisoner, "SELECT id FROM parent WHERE id = 100").await,
        [100]
    );
    writer
        .execute("DELETE FROM parent WHERE id = 100", ())
        .await
        .unwrap();
    drain(&writer, "PRAGMA wal_checkpoint(PASSIVE)").await;
    assert_eq!(ints(&poisoner, "SELECT nextval('s')").await, [1]);
    poisoner
        .execute("INSERT INTO parent VALUES (7, 'seven')", ())
        .await
        .unwrap();
    victim
        .execute("INSERT INTO child(pid) VALUES (100)", ())
        .await
        .unwrap();
}
