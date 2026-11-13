use turso_core::database::{ConnectOpts, Database, DatabaseOpts};
use turso_core::security::roles::SessionRole;
use turso_core::stats::{parse_stat_numbers, AnalyzeStats, NamespaceId};
use turso_core::sync::Arc;
use turso_core::{OpenFlags, PlatformIO, IO};
use turso_sqlite::SqliteDialect;

#[test]
fn parse_stat_numbers_basic() {
    assert_eq!(parse_stat_numbers("10 5 3 1").unwrap(), vec![10, 5, 3, 1]);
    assert_eq!(parse_stat_numbers("  42\t7 ").unwrap(), vec![42, 7]);
    assert!(parse_stat_numbers("abc 1").is_none());
}

/// Open a fresh temporary database for the stats tests below.
fn open_db() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stats.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    (dir, db)
}

/// Create table `t` (2 rows, 2 indexes) and ANALYZE it on a fresh
/// connection, which is dropped on return.
fn create_table_and_analyze(db: &Arc<Database>) {
    let setup = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    setup
        .execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c INTEGER);
             CREATE INDEX t_a ON t(a, c);
             CREATE INDEX t_b ON t(b);
             INSERT INTO t (a, b, c) VALUES ('x', 'y', 1), ('z', 'w', 2);
             ANALYZE",
        )
        .unwrap();
}

/// The stats the tests expect for table `t` after ANALYZE on its 2 rows.
fn assert_table_t_stats(stats: &AnalyzeStats) {
    assert_eq!(stats.tables.len(), 1, "only table t should have stats");
    let t = stats
        .table_stats(NamespaceId::ROOT, "t")
        .expect("table t must have stats");
    assert_eq!(t.row_count, Some(2));
    let t_a = t.index_stats.get("t_a").expect("index t_a must have stats");
    assert_eq!(t_a.total_rows, Some(2));
    let t_b = t.index_stats.get("t_b").expect("index t_b must have stats");
    assert_eq!(t_b.total_rows, Some(2));
}

/// Regression test for #11581: once a database has `sqlite_stat1`, opening
/// a new connection must not deep-clone the whole schema just to install
/// the ANALYZE stats. Every connection keeps sharing the database schema.
#[test]
fn new_connections_share_schema_when_stat1_exists() {
    let (_dir, db) = open_db();
    create_table_and_analyze(&db);
    let mut connections = Vec::new();
    for _ in 0..5 {
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        assert!(
            Arc::ptr_eq(&conn.schema.read(), &db.schema.lock()),
            "new connection must share the database schema instead of deep-cloning it"
        );
        connections.push(conn);
    }
    assert_eq!(connections.len(), 5);
    assert_table_t_stats(&db.schema.lock().analyze_stats);
}

/// An existing connection that runs ANALYZE itself ends up sharing the
/// database schema and carrying the correct stats (#11581).
#[test]
fn existing_connection_shares_schema_after_analyze() {
    let (_dir, db) = open_db();
    let setup = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    setup
        .execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c INTEGER);
             CREATE INDEX t_a ON t(a, c);
             CREATE INDEX t_b ON t(b);
             INSERT INTO t (a, b, c) VALUES ('x', 'y', 1), ('z', 'w', 2);",
        )
        .unwrap();
    drop(setup);
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("ANALYZE").unwrap();
    assert!(
        Arc::ptr_eq(&conn.schema.read(), &db.schema.lock()),
        "connection that ran ANALYZE must share the database schema"
    );
    assert_table_t_stats(&conn.schema.read().analyze_stats);
    assert_table_t_stats(&db.schema.lock().analyze_stats);
}

/// When the stats actually changed, the shared schema is updated and the
/// connection is pointed at the new shared schema (#11581).
#[test]
fn analyze_after_more_rows_updates_shared_stats() {
    let (_dir, db) = open_db();
    create_table_and_analyze(&db);
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute(
        "INSERT INTO t (a, b, c) VALUES ('p', 'q', 3), ('r', 's', 4), ('u', 'v', 5);
         ANALYZE",
    )
    .unwrap();
    assert!(
        Arc::ptr_eq(&conn.schema.read(), &db.schema.lock()),
        "connection must share the database schema after re-ANALYZE"
    );
    let shared = db.schema.lock();
    assert_eq!(
        shared
            .analyze_stats
            .table_stats(NamespaceId::ROOT, "t")
            .unwrap()
            .row_count,
        Some(5)
    );
    drop(shared);
    let conn2 = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    assert!(Arc::ptr_eq(&conn2.schema.read(), &db.schema.lock()));
    assert_eq!(
        conn2
            .schema
            .read()
            .analyze_stats
            .table_stats(NamespaceId::ROOT, "t")
            .unwrap()
            .row_count,
        Some(5)
    );
}

/// A connection holding a private copy of the schema at the same version
/// (installed here by a reparse without publishing, the shape a sync
/// checkpoint leaves behind) is not pointed at the shared schema by a
/// stats refresh: it keeps its own schema and still gets the stats
/// (#11581).
#[test]
fn private_same_version_schema_survives_analyze() {
    let (_dir, db) = open_db();
    create_table_and_analyze(&db);
    let a = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    a.force_reparse_schema_without_publish().unwrap();
    assert!(
        !Arc::ptr_eq(&a.schema.read(), &db.schema.lock()),
        "test setup must leave a private schema on the connection"
    );
    a.execute("ANALYZE").unwrap();
    assert!(
        !Arc::ptr_eq(&a.schema.read(), &db.schema.lock()),
        "refresh must not discard a private same-version schema"
    );
    assert_table_t_stats(&a.schema.read().analyze_stats);
}

/// Control case for #11581: without `sqlite_stat1` there is nothing to
/// install, so new connections trivially keep sharing the database schema.
#[test]
fn connections_share_schema_without_stat1() {
    let (_dir, db) = open_db();
    let setup = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    setup
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    drop(setup);
    let mut connections = Vec::new();
    for _ in 0..5 {
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
        assert!(
            Arc::ptr_eq(&conn.schema.read(), &db.schema.lock()),
            "new connection must share the database schema"
        );
        connections.push(conn);
    }
    assert_eq!(connections.len(), 5);
}
