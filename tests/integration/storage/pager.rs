use turso_core::io::MemoryIO;
use turso_core::io::IO;
use turso_core::security::roles::SessionRole;
use turso_core::storage::sqlite3_ondisk::PageSize;
use turso_core::sync::Arc;
use turso_sqlite::SqliteDialect;

#[test]
#[should_panic(expected = "rollback must discard pages while holding the WAL write lock")]
fn rollback_without_the_write_lock_panics() {
    let (_db, conn) = open_memory_database("rollback-without-write-lock.db");
    conn.execute("CREATE TABLE t(x)").unwrap();

    conn.get_pager().rollback(false, &conn, true);
}

#[test]
fn rollback_keeps_pages_the_transaction_did_not_change() {
    let (db, conn) = open_memory_database("rollback-keeps-unchanged-pages.db");
    conn.execute("CREATE TABLE changed(x)").unwrap();
    conn.execute("CREATE TABLE unchanged(x)").unwrap();
    conn.execute("INSERT INTO changed VALUES (1)").unwrap();
    conn.execute("INSERT INTO unchanged VALUES (1)").unwrap();
    let changed_root = root_page(&conn, "changed");
    let unchanged_root = root_page(&conn, "unchanged");
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM unchanged"), 1);
    let pager = conn.get_pager();
    let cached_unchanged_root = pager.cache_get(unchanged_root).unwrap().unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO changed VALUES (2)").unwrap();
    conn.execute("ROLLBACK").unwrap();

    assert!(!pager.cache_contains(changed_root));
    let unchanged_root_after_rollback = pager.cache_get(unchanged_root).unwrap().unwrap();
    assert!(Arc::ptr_eq(
        &cached_unchanged_root,
        &unchanged_root_after_rollback
    ));
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM changed"), 1);
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM unchanged"), 1);
    drop(db);
}

#[test]
fn rollback_drops_pages_spilled_to_the_wal() {
    let (db, conn) = open_memory_database("rollback-drops-spilled-pages.db");
    conn.execute("PRAGMA cache_size = 200").unwrap();
    conn.execute("CREATE TABLE t(x)").unwrap();
    conn.execute("INSERT INTO t SELECT randomblob(1000) FROM generate_series(1, 500)")
        .unwrap();
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM t"), 500);
    let pager = conn.get_pager();
    let wal = pager.wal().unwrap();
    let committed_max_frame = wal.get_max_frame();

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET x = randomblob(1000)").unwrap();
    conn.execute("INSERT INTO t SELECT randomblob(1000) FROM generate_series(1, 1000)")
        .unwrap();
    assert!(
        wal.get_max_frame() > committed_max_frame,
        "the transaction must spill pages to the WAL"
    );
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM t"), 1500);
    let page_count = query_i64(&conn, "PRAGMA page_count") as usize;
    conn.execute("ROLLBACK").unwrap();

    assert_eq!(wal.get_max_frame(), committed_max_frame);
    for page_idx in 1..=page_count {
        let Some(page) = pager.cache_get(page_idx).unwrap() else {
            continue;
        };
        assert!(!page.is_dirty());
        assert!(!page.has_wal_tag() || page.wal_tag_pair().0 <= committed_max_frame);
    }
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM t"), 500);
    let mut integrity_check = conn.prepare("PRAGMA integrity_check").unwrap();
    assert_eq!(
        integrity_check.run_collect_rows().unwrap(),
        vec![vec![turso_core::Value::build_text("ok")]]
    );
    drop(db);
}

#[test]
fn rollback_drops_pinned_pages_that_are_not_loaded() {
    let (db, conn) = open_memory_database("rollback-drops-pinned-unloaded-pages.db");
    conn.execute("CREATE TABLE changed(x)").unwrap();
    conn.execute("CREATE TABLE unloaded(x)").unwrap();
    conn.execute("INSERT INTO unloaded VALUES (1)").unwrap();
    let unloaded_root = root_page(&conn, "unloaded");
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM unloaded"), 1);
    let pager = conn.get_pager();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO changed VALUES (1)").unwrap();
    let page = pager.cache_get(unloaded_root).unwrap().unwrap();
    page.pin();
    page.clear_loaded();
    conn.execute("ROLLBACK").unwrap();

    assert!(!pager.cache_contains(unloaded_root));
    assert_eq!(query_i64(&conn, "SELECT count(*) FROM unloaded"), 1);
    drop(db);
}

#[test]
fn record_pool_frees_buffers_of_rows_larger_than_a_page() {
    let (_db, conn) = open_memory_database("record-pool-large-rows.db");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT NOT NULL, tag TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, CAST(zeroblob(6 * 1024 * 1024) AS TEXT), 'big')")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (2, CAST(zeroblob(5000) AS TEXT), 'small')")
        .unwrap();
    conn.execute("SELECT body FROM t WHERE id = 1").unwrap();
    conn.execute("UPDATE t SET tag = 'updated' WHERE id = 1")
        .unwrap();
    conn.execute("UPDATE t SET tag = 'updated' WHERE id = 2")
        .unwrap();

    let capacities = conn.get_pager().record_pool_capacities();
    assert!(!capacities.is_empty());
    for capacity in capacities {
        assert!(capacity <= PageSize::MAX as usize);
    }
}

fn open_memory_database(path: &str) -> (Arc<turso_core::Database>, Arc<turso_core::Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = turso_core::Database::open_file(io, path, Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    (db, conn)
}

fn root_page(conn: &Arc<turso_core::Connection>, table: &str) -> usize {
    query_i64(
        conn,
        &format!("SELECT rootpage FROM sqlite_schema WHERE name = '{table}'"),
    ) as usize
}

fn query_i64(conn: &Arc<turso_core::Connection>, sql: &str) -> i64 {
    let mut stmt = conn.prepare(sql).unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    match rows.as_slice() {
        [row] => match row.as_slice() {
            [turso_core::Value::Numeric(turso_core::Numeric::Integer(value))] => *value,
            other => panic!("expected one integer, got {other:?}"),
        },
        other => panic!("expected one row, got {other:?}"),
    }
}
