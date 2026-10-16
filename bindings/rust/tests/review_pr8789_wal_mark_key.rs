use std::sync::Arc;

use tempfile::tempdir;
use turso_core::{
    CheckpointMode, ConnectOpts, Connection, Database, DatabaseOpts, OpenFlags, PlatformIO,
    SqliteDialect, Value,
};

fn rows(conn: &Arc<Connection>, sql: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut out = Vec::new();
    stmt.run_with_row_callback(|row| {
        out.push(row.get_values().cloned().collect::<Vec<_>>());
        Ok(())
    })
    .unwrap();
    out
}

fn ints(conn: &Arc<Connection>, sql: &str) -> Vec<i64> {
    rows(conn, sql)
        .into_iter()
        .map(|r| match r[0] {
            Value::Numeric(turso_core::Numeric::Integer(i)) => i,
            ref other => panic!("not an integer: {other:?}"),
        })
        .collect()
}

fn open_db() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("mark.db");
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let setup = db.connect(ConnectOpts::new()).unwrap();
    rows(&setup, "PRAGMA journal_mode = mvcc");
    setup
        .execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    setup
        .execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    setup
        .execute("CREATE TABLE child(pid INTEGER REFERENCES parent(id))")
        .unwrap();
    setup
        .execute("INSERT INTO parent VALUES (1, 'a'), (100, 'z')")
        .unwrap();
    rows(&setup, "PRAGMA wal_checkpoint(TRUNCATE)");
    setup.close().unwrap();
    (dir, db)
}

fn poisoned_shared_cache() -> (tempfile::TempDir, Arc<Database>, Arc<Connection>) {
    let (dir, db) = open_db();
    let victim = db.connect(ConnectOpts::new()).unwrap();
    let poisoner = db.connect(ConnectOpts::new()).unwrap();
    let writer = db.connect(ConnectOpts::new()).unwrap();
    for conn in [&victim, &poisoner, &writer] {
        conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
    }
    victim.execute("PRAGMA foreign_keys = ON").unwrap();

    victim.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(ints(&victim, "SELECT id FROM parent WHERE id = 100"), [100]);
    poisoner.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        ints(&poisoner, "SELECT id FROM parent WHERE id = 100"),
        [100]
    );

    writer.execute("DELETE FROM parent WHERE id = 100").unwrap();

    poisoner
        .checkpoint(CheckpointMode::Passive {
            upper_bound_inclusive: None,
        })
        .unwrap();

    poisoner
        .execute("INSERT INTO parent VALUES (7, 'seven')")
        .unwrap();

    (dir, db, victim)
}

#[test]
fn fk_parent_visible_to_old_snapshot_is_reported_missing() {
    let (_dir, _db, victim) = poisoned_shared_cache();
    assert_eq!(ints(&victim, "SELECT id FROM parent WHERE id = 100"), [100]);
    let result = victim.execute("INSERT INTO child(pid) VALUES (100)");
    assert!(
        result.is_ok(),
        "parent rowid 100 is visible to this snapshot, but the FK check said it does not exist: {result:?}"
    );
}

#[test]
fn duplicate_rowid_visible_to_old_snapshot_is_accepted() {
    let (_dir, _db, victim) = poisoned_shared_cache();
    assert_eq!(ints(&victim, "SELECT id FROM parent WHERE id = 100"), [100]);
    let result = victim.execute("INSERT INTO parent VALUES (100, 'dup')");
    assert!(
        result.is_err(),
        "rowid 100 is visible to this snapshot, but the INSERT was accepted"
    );
}

fn unpoisoned_shared_cache() -> (tempfile::TempDir, Arc<Database>, Arc<Connection>) {
    let (dir, db) = open_db();
    let victim = db.connect(ConnectOpts::new()).unwrap();
    let poisoner = db.connect(ConnectOpts::new()).unwrap();
    let writer = db.connect(ConnectOpts::new()).unwrap();
    for conn in [&victim, &poisoner, &writer] {
        conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
    }
    victim.execute("PRAGMA foreign_keys = ON").unwrap();
    victim.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(ints(&victim, "SELECT id FROM parent WHERE id = 100"), [100]);
    poisoner.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        ints(&poisoner, "SELECT id FROM parent WHERE id = 100"),
        [100]
    );
    writer.execute("DELETE FROM parent WHERE id = 100").unwrap();
    poisoner
        .checkpoint(CheckpointMode::Passive {
            upper_bound_inclusive: None,
        })
        .unwrap();
    (dir, db, victim)
}

#[test]
fn control_fk_parent_visible_without_poisoned_cache() {
    let (_dir, _db, victim) = unpoisoned_shared_cache();
    victim
        .execute("INSERT INTO child(pid) VALUES (100)")
        .unwrap();
}

#[test]
fn control_duplicate_rowid_rejected_without_poisoned_cache() {
    let (_dir, _db, victim) = unpoisoned_shared_cache();
    let err = victim
        .execute("INSERT INTO parent VALUES (100, 'dup')")
        .unwrap_err();
    assert!(err.to_string().contains("UNIQUE"), "{err}");
}
