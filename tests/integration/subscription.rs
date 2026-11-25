use std::collections::{BTreeMap, BTreeSet};

use crate::common::TempDatabase;
use turso_core::{ChangeBatch, ChangeFilter, ChangedKeys, Value};

fn batch(table: &str, keys: impl IntoIterator<Item = Vec<Value>>) -> ChangeBatch {
    ChangeBatch::Tables(BTreeMap::from([(
        table.into(),
        ChangedKeys::Keys(keys.into_iter().collect()),
    )]))
}

fn membership(user: &'static str) -> Vec<Value> {
    vec![Value::from_i64(7), Value::build_text(user)]
}

#[turso_macros::test(mvcc)]
fn composite_key_changes(db: TempDatabase) {
    let writer = db.connect_limbo();
    writer
        .execute("CREATE TABLE members(user TEXT, role TEXT, org INTEGER, PRIMARY KEY(org,user))")
        .unwrap();
    let mut insert = writer
        .prepare("INSERT INTO members VALUES ('sam','reader',7)")
        .unwrap();
    let reader = db.connect_limbo();
    let sub = reader.db.subscribe_changes(ChangeFilter::Table {
        table: "MEMBERS".into(),
    });
    let watch = |name| {
        db.db.subscribe_changes(ChangeFilter::Row {
            table: "members".into(),
            primary_key: membership(name),
        })
    };
    let old = watch("sam");
    let new = watch("lee");
    let unrelated = watch("other");
    insert.run_ignore_rows().unwrap();
    assert_eq!(sub.try_recv(), Some(batch("members", [membership("sam")])));
    assert_eq!(old.try_recv(), Some(batch("members", [membership("sam")])));
    writer.execute("BEGIN").unwrap();
    writer
        .execute("UPDATE members SET user='lee' WHERE org=7 AND user='sam'")
        .unwrap();
    writer.execute("UPDATE members SET role='writer'").unwrap();
    assert_eq!(sub.try_recv(), None);
    writer.execute("COMMIT").unwrap();
    assert_eq!(
        sub.try_recv(),
        Some(batch("members", [membership("sam"), membership("lee")]))
    );
    assert_eq!(old.try_recv(), Some(batch("members", [membership("sam")])));
    assert_eq!(new.try_recv(), Some(batch("members", [membership("lee")])));
    assert_eq!(unrelated.try_recv(), None);
    writer
        .execute("DELETE FROM members WHERE user='lee'")
        .unwrap();
    assert_eq!(sub.try_recv(), Some(batch("members", [membership("lee")])));
}

#[turso_macros::test(mvcc)]
fn publish_only_kept_rows(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    let sub = db.db.subscribe_changes(ChangeFilter::All);
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES(1)").unwrap();
    conn.execute("SAVEPOINT s").unwrap();
    conn.execute("INSERT INTO t VALUES(2)").unwrap();
    conn.execute("ROLLBACK TO s").unwrap();
    conn.execute("RELEASE s").unwrap();
    conn.execute("INSERT INTO t VALUES(3),(1)").unwrap_err();
    conn.execute("INSERT OR FAIL INTO t VALUES(4),(1)")
        .unwrap_err();
    assert_eq!(sub.try_recv(), None);
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        sub.try_recv(),
        Some(batch(
            "t",
            [vec![Value::from_i64(1)], vec![Value::from_i64(4)]]
        ))
    );
    conn.execute("BEGIN").unwrap();
    conn.execute("DELETE FROM t").unwrap();
    conn.execute("ROLLBACK").unwrap();
    conn.execute("SELECT * FROM t").unwrap();
    assert_eq!(sub.try_recv(), None);
}

#[turso_macros::test(mvcc)]
fn mid_transaction_registration_and_trigger(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER log AFTER INSERT ON t BEGIN INSERT INTO audit VALUES(new.rowid); END",
    )
    .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO t VALUES('first')").unwrap();
    let sub = db.db.subscribe_changes(ChangeFilter::All);
    conn.execute("COMMIT").unwrap();
    assert_eq!(sub.try_recv(), Some(ChangeBatch::Refresh));
    conn.execute("INSERT INTO t VALUES('second')").unwrap();
    let Some(ChangeBatch::Tables(tables)) = sub.try_recv() else {
        panic!("missed trigger")
    };
    assert_eq!(
        tables["audit"],
        ChangedKeys::Keys(BTreeSet::from([vec![Value::from_i64(2)]]))
    );
    assert_eq!(tables["t"], tables["audit"]);
}

#[turso_macros::test]
fn conservative_invalidations(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a,b,PRIMARY KEY(a,b))")
        .unwrap();
    let sub = db.db.subscribe_changes(ChangeFilter::All);
    conn.execute("INSERT INTO t VALUES (NULL,1)").unwrap();
    let all = || {
        Some(ChangeBatch::Tables(BTreeMap::from([(
            "t".into(),
            ChangedKeys::All,
        )])))
    };
    assert_eq!(sub.try_recv(), all());
    conn.execute("DELETE FROM t").unwrap();
    assert_eq!(sub.try_recv(), all());
    conn.execute("ALTER TABLE t RENAME TO other").unwrap();
    assert_eq!(sub.try_recv(), Some(ChangeBatch::Refresh));
}

#[turso_macros::test]
fn bounded_changes_and_slow_receiver(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    let sub = db.db.subscribe_changes(ChangeFilter::All);
    conn.execute("BEGIN").unwrap();
    for id in 1..=256 {
        conn.execute(format!("INSERT INTO t VALUES({id})")).unwrap();
    }
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        sub.try_recv(),
        Some(ChangeBatch::Tables(BTreeMap::from([(
            "t".into(),
            ChangedKeys::All
        )])))
    );
    for _ in 0..16 {
        conn.execute("INSERT INTO t DEFAULT VALUES").unwrap();
    }
    assert_eq!(sub.try_recv(), Some(ChangeBatch::Refresh));
}

#[turso_macros::test]
fn without_rowid_composite(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE m(user BLOB NOT NULL, role BLOB, org BLOB NOT NULL, PRIMARY KEY(org,user)) WITHOUT ROWID").unwrap();
    let all = db.db.subscribe_changes(ChangeFilter::All);
    let sub = db.db.subscribe_changes(ChangeFilter::Row {
        table: "m".into(),
        primary_key: membership("sam"),
    });
    conn.execute("INSERT INTO m VALUES('sam','reader',7)")
        .unwrap();
    assert_eq!(all.try_recv(), Some(batch("m", [membership("sam")])));
    assert_eq!(sub.try_recv(), Some(batch("m", [membership("sam")])));
    conn.execute("INSERT INTO m VALUES('other','reader',7)")
        .unwrap();
    assert_eq!(sub.try_recv(), None);
}

#[turso_macros::test(mvcc)]
fn replace_and_cascade(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("PRAGMA foreign_keys=ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT UNIQUE)")
        .unwrap();
    conn.execute("CREATE TABLE child(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)").unwrap();
    conn.execute("INSERT INTO parent VALUES (1,'label')")
        .unwrap();
    conn.execute("INSERT INTO child VALUES (9,1)").unwrap();
    let sub = db.db.subscribe_changes(ChangeFilter::All);
    conn.execute("INSERT OR REPLACE INTO parent VALUES(2,'label')")
        .unwrap();
    let Some(ChangeBatch::Tables(tables)) = sub.try_recv() else {
        panic!("missing replace")
    };
    assert_eq!(
        tables["parent"],
        ChangedKeys::Keys(BTreeSet::from([
            vec![Value::from_i64(1)],
            vec![Value::from_i64(2)]
        ]))
    );
    assert_eq!(
        tables["child"],
        ChangedKeys::Keys(BTreeSet::from([vec![Value::from_i64(9)]]))
    );
    assert_eq!(sub.try_recv(), None);
}

#[turso_macros::test(mvcc)]
fn upsert_changes_key(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE m(user TEXT, org INT, handle INT UNIQUE, PRIMARY KEY(org,user))")
        .unwrap();
    conn.execute("INSERT INTO m VALUES ('sam',7,99)").unwrap();
    let sub = db.db.subscribe_changes(ChangeFilter::All);
    conn.execute(
        "INSERT INTO m VALUES('lee',7,99) ON CONFLICT(handle) DO UPDATE SET user=excluded.user",
    )
    .unwrap();
    assert_eq!(
        sub.try_recv(),
        Some(batch("m", [membership("sam"), membership("lee")]))
    );
}

#[turso_macros::test]
fn blob_and_temp_tables(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(data BLOB)").unwrap();
    conn.execute("INSERT INTO t VALUES(zeroblob(8))").unwrap();
    let sub = db
        .db
        .subscribe_changes(ChangeFilter::Table { table: "t".into() });
    let mut blob = conn.blob_open("t", "data", 1, true).unwrap();
    blob.write(0, &[1, 2]).unwrap();
    assert_eq!(sub.try_recv(), None);
    blob.close().unwrap();
    assert_eq!(
        sub.try_recv(),
        Some(ChangeBatch::Tables(BTreeMap::from([(
            "t".into(),
            ChangedKeys::All
        )])))
    );
    conn.execute("CREATE TEMP TABLE t(data BLOB)").unwrap();
    conn.execute("INSERT INTO temp.t VALUES (zeroblob(8))")
        .unwrap();
    assert_eq!(sub.try_recv(), None);
}

#[turso_macros::test]
fn uncertain_key_wakes_row_watcher(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE m(user TEXT NOT NULL, role TEXT, org INT NOT NULL, PRIMARY KEY(org,user)) WITHOUT ROWID").unwrap();
    let sub = db.db.subscribe_changes(ChangeFilter::Row {
        table: "m".into(),
        primary_key: membership("sam"),
    });
    conn.execute("INSERT INTO m VALUES('sam','reader',7)")
        .unwrap();
    // Depending on the storage affinity, the engine may need a table-wide
    // refresh. Either representation must wake the subscriber for (7, "sam").
    let notification = sub.try_recv();
    assert!(
        notification == Some(batch("m", [membership("sam")]))
            || notification
                == Some(ChangeBatch::Tables(BTreeMap::from([(
                    "m".into(),
                    ChangedKeys::All
                )])))
    );
}

#[turso_macros::test(mvcc)]
fn returning_waits_for_commit(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(x)").unwrap();
    let sub = db.db.subscribe_changes(ChangeFilter::All);
    let mut stmt = conn
        .prepare("INSERT INTO t VALUES (1) RETURNING rowid")
        .unwrap();
    assert!(stmt
        .run_one_step_blocking(|| Ok(()), || Ok(()))
        .unwrap()
        .is_some());
    assert_eq!(sub.try_recv(), None);
    stmt.run_ignore_rows().unwrap();
    assert_eq!(sub.try_recv(), Some(batch("t", [vec![Value::from_i64(1)]])));
}

#[turso_macros::test(mvcc)]
fn strict_update_keeps_changes_before_error(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(x INTEGER) STRICT").unwrap();
    conn.execute("INSERT INTO t VALUES(1),(2)").unwrap();
    let sub = db.db.subscribe_changes(ChangeFilter::All);
    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET x=CASE x WHEN 1 THEN 3 ELSE 'invalid' END")
        .unwrap_err();
    assert_eq!(sub.try_recv(), None);
    conn.execute("COMMIT").unwrap();
    let rows = conn
        .prepare("SELECT x FROM t ORDER BY rowid")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows,
        vec![vec![Value::from_i64(3)], vec![Value::from_i64(2)]]
    );
    assert_eq!(sub.try_recv(), Some(batch("t", [vec![Value::from_i64(1)]])));
}

#[cfg(all(feature = "fs", feature = "conn_raw_api"))]
#[turso_macros::test]
fn wal_session_sql_changes(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    let sub = db.db.subscribe_changes(ChangeFilter::All);
    conn.wal_insert_begin().unwrap();
    conn.execute("INSERT INTO t VALUES(1)").unwrap();
    conn.wal_insert_end(true).unwrap();
    assert_eq!(sub.try_recv(), Some(batch("t", [vec![Value::from_i64(1)]])));
    conn.wal_insert_begin().unwrap();
    conn.execute("INSERT INTO t VALUES(2)").unwrap();
    conn.wal_insert_end(false).unwrap();
    assert_eq!(sub.try_recv(), None);
    conn.execute("INSERT INTO t VALUES(3)").unwrap();
    assert_eq!(sub.try_recv(), Some(batch("t", [vec![Value::from_i64(3)]])));
    let rows = conn
        .prepare("SELECT id FROM t ORDER BY id")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(
        rows,
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(3)]]
    );
}

#[turso_macros::test]
fn bulk_delete_of_internal_table_is_silent(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT)")
        .unwrap();
    conn.execute("INSERT INTO t DEFAULT VALUES").unwrap();
    let sub = db.db.subscribe_changes(ChangeFilter::All);
    conn.execute("DELETE FROM sqlite_sequence").unwrap();
    assert_eq!(sub.try_recv(), None);
}

#[turso_macros::test(mvcc)]
fn in_place_vacuum_invalidates_hidden_rowids(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(rowid TEXT, oid TEXT, _rowid_ TEXT, payload INT)")
        .unwrap();
    conn.execute("INSERT INTO t(payload) VALUES(1),(2)")
        .unwrap();
    conn.execute("DELETE FROM t WHERE payload=1").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let sub = db.db.subscribe_changes(ChangeFilter::Row {
        table: "t".into(),
        primary_key: vec![Value::from_i64(2)],
    });
    conn.execute("BEGIN").unwrap();
    conn.execute("VACUUM").unwrap_err();
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(sub.try_recv(), None);
    conn.execute("VACUUM").unwrap();
    assert_eq!(sub.try_recv(), Some(ChangeBatch::Refresh));
    assert_eq!(sub.try_recv(), None);
}

#[turso_macros::test(mvcc)]
fn unrelated_tables_do_not_overflow_a_filter(db: TempDatabase) {
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE watched(id INTEGER PRIMARY KEY)")
        .unwrap();
    for i in 0..40 {
        conn.execute(format!("CREATE TABLE t{i}(id INTEGER PRIMARY KEY)"))
            .unwrap();
    }
    let existing = db.db.subscribe_changes(ChangeFilter::Table {
        table: "watched".into(),
    });
    conn.execute("BEGIN").unwrap();
    for i in 0..40 {
        conn.execute(format!("INSERT INTO t{i} VALUES(1)")).unwrap();
    }
    let late = db
        .db
        .subscribe_changes(ChangeFilter::Table { table: "t0".into() });
    conn.execute("COMMIT").unwrap();
    assert_eq!(existing.try_recv(), None);
    assert_eq!(late.try_recv(), Some(ChangeBatch::Refresh));
}

#[test]
fn async_receive_wakes_on_commit_and_close() {
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    #[derive(Default)]
    struct Flag(AtomicBool);
    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }
    let db = TempDatabase::new_empty();
    let writer = db.connect_limbo();
    writer
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    let mut sub = db.db.subscribe_changes(ChangeFilter::All);
    let flag = Arc::new(Flag::default());
    let waker = Waker::from(flag.clone());
    let mut cx = Context::from_waker(&waker);
    let mut read = Box::pin(sub.recv_async());
    assert!(read.as_mut().poll(&mut cx).is_pending());
    writer.execute("BEGIN").unwrap();
    writer.execute("INSERT INTO t VALUES(9)").unwrap();
    assert!(!flag.0.load(Ordering::Acquire));
    writer.execute("COMMIT").unwrap();
    assert!(flag.0.swap(false, Ordering::AcqRel));
    assert_eq!(
        read.as_mut().poll(&mut cx),
        Poll::Ready(Some(batch("t", [vec![Value::from_i64(9)]])))
    );
    drop(read);
    let mut cancelled = Box::pin(sub.recv_async());
    assert!(cancelled.as_mut().poll(&mut cx).is_pending());
    drop(cancelled);
    writer.execute("INSERT INTO t VALUES(10)").unwrap();
    assert!(!flag.0.load(Ordering::Acquire));
    assert_eq!(
        sub.try_recv(),
        Some(batch("t", [vec![Value::from_i64(10)]]))
    );
    let mut read = Box::pin(sub.recv_async());
    assert!(read.as_mut().poll(&mut cx).is_pending());
    drop(writer);
    drop(db);
    assert!(flag.0.load(Ordering::Acquire));
    assert_eq!(read.as_mut().poll(&mut cx), Poll::Ready(None));
}

#[cfg(all(feature = "fs", feature = "conn_raw_api"))]
#[test]
fn local_wal_import_refreshes_at_each_new_commit_frame() {
    let source = TempDatabase::new_empty();
    let src = source.connect_limbo();
    src.execute("CREATE TABLE imported(x)").unwrap();
    src.execute("INSERT INTO imported VALUES(1)").unwrap();
    let target = TempDatabase::new_empty();
    let dst = target.connect_limbo();
    let sub = target.db.subscribe_changes(ChangeFilter::Table {
        table: "imported".into(),
    });
    let mut frame = [0; 24 + 4096];
    dst.wal_insert_begin().unwrap();
    for n in 1..=src.wal_state().unwrap().max_frame {
        let info = src.wal_get_frame(n, &mut frame).unwrap();
        dst.wal_insert_frame(n, &frame).unwrap();
        assert_eq!(
            sub.try_recv(),
            info.is_commit_frame().then_some(ChangeBatch::Refresh)
        );
        dst.wal_insert_frame(n, &frame).unwrap();
        assert_eq!(sub.try_recv(), None);
    }
    dst.wal_insert_end(false).unwrap();
    assert_eq!(sub.try_recv(), None);
    let rows = dst
        .prepare("SELECT x FROM imported")
        .unwrap()
        .run_collect_rows()
        .unwrap();
    assert_eq!(rows, vec![vec![Value::from_i64(1)]]);
}

#[test]
fn mvcc_busy_schema_writer_can_retry_with_a_subscription() {
    let db = TempDatabase::builder().with_mvcc(true).build();
    let holder = db.connect_limbo();
    let contender = db.connect_limbo();
    holder
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    let sub = db.db.subscribe_changes(ChangeFilter::All);
    holder.execute("BEGIN CONCURRENT").unwrap();
    holder
        .execute("ALTER TABLE t ADD COLUMN held INTEGER")
        .unwrap();
    contender.execute("BEGIN CONCURRENT").unwrap();
    assert!(contender
        .execute("ALTER TABLE t ADD COLUMN retried INTEGER")
        .is_err());
    holder.execute("ROLLBACK").unwrap();
    assert_eq!(sub.try_recv(), None);
    contender
        .execute("ALTER TABLE t ADD COLUMN retried INTEGER")
        .unwrap();
    contender.execute("COMMIT").unwrap();
    assert_eq!(sub.try_recv(), Some(ChangeBatch::Refresh));
    contender.execute("INSERT INTO t(id) VALUES(7)").unwrap();
    assert_eq!(sub.try_recv(), Some(batch("t", [vec![Value::from_i64(7)]])));
}
