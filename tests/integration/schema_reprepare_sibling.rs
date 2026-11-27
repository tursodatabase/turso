use std::sync::Arc;
use turso_core::security::roles::SessionRole;
use turso_core::{
    ConnectOpts, Database, DatabaseOpts, OpenFlags, PlatformIO, Statement, StepResult, Value,
};
use turso_sqlite::SqliteDialect;

fn next(stmt: &mut Statement) -> Option<Vec<Value>> {
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => return Some(stmt.row().unwrap().get_values().cloned().collect()),
            StepResult::Done => return None,
            StepResult::IO => stmt._io().step().unwrap(),
            other => panic!("unexpected step: {other:?}"),
        }
    }
}

#[test]
fn stale_reader_does_not_rollback_a_suspended_returning_write() {
    for (mode, reset) in [
        ("wal", false),
        ("wal", true),
        ("mvcc", false),
        ("mvcc", true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = Database::open_file(
            Arc::new(PlatformIO::new().unwrap()),
            path.to_str().unwrap(),
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        if mode == "mvcc" {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
            .unwrap();
        conn.execute("CREATE TABLE u(y)").unwrap();
        let mut stale = conn.prepare("SELECT y FROM u").unwrap();
        conn.execute("CREATE TABLE v(z)").unwrap();
        let mut insert = conn
            .prepare("INSERT INTO t(x) VALUES (1) RETURNING id")
            .unwrap();
        assert_eq!(next(&mut insert), Some(vec![Value::from_i64(1)]), "{mode}");
        assert_eq!(next(&mut stale), None, "{mode}");
        if reset {
            insert.reset().unwrap();
        } else {
            assert_eq!(next(&mut insert), None, "{mode}");
        }
        let other = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        let mut count = other.prepare("SELECT count(*) FROM t").unwrap();
        assert_eq!(next(&mut count), Some(vec![Value::from_i64(1)]), "{mode}");
    }
}

#[test]
fn stale_reader_reprepares_without_a_sibling() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = Database::open_file(
        Arc::new(PlatformIO::new().unwrap()),
        path.to_str().unwrap(),
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    let mut stale = conn.prepare("SELECT x FROM t").unwrap();
    conn.execute("ALTER TABLE t ADD COLUMN y").unwrap();
    assert_eq!(next(&mut stale), None);
    conn.execute("INSERT INTO t VALUES (7, 8)").unwrap();
    let mut count = conn.prepare("SELECT count(*) FROM t").unwrap();
    assert_eq!(next(&mut count), Some(vec![Value::from_i64(1)]));
}

#[test]
fn stale_attached_reader_preserves_suspended_main_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.db");
    let aux = dir.path().join("aux.db");
    let db = Database::open_file_with_flags(
        Arc::new(PlatformIO::new().unwrap()),
        path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new().with_attach(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute(format!("ATTACH '{}' AS aux", aux.display()))
        .unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)")
        .unwrap();
    conn.execute("CREATE TABLE aux.u(y)").unwrap();
    let mut stale = conn.prepare("SELECT y FROM aux.u").unwrap();
    conn.execute("CREATE TABLE aux.v(z)").unwrap();
    let mut insert = conn
        .prepare("INSERT INTO t(x) VALUES (1) RETURNING id")
        .unwrap();
    assert_eq!(next(&mut insert), Some(vec![Value::from_i64(1)]));
    assert_eq!(next(&mut stale), None);
    assert_eq!(next(&mut insert), None);
    let other = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    let mut count = other.prepare("SELECT count(*) FROM t").unwrap();
    assert_eq!(next(&mut count), Some(vec![Value::from_i64(1)]));
}
