use turso_core::io::MemoryIO;
use turso_core::io::IO;
use turso_core::security::roles::SessionRole;
use turso_core::sync::Arc;
use turso_sqlite::SqliteDialect;

#[test]
#[should_panic(expected = "rollback must discard pages while holding the WAL write lock")]
fn rollback_without_the_write_lock_panics() {
    let (_db, conn) = open_memory_database("rollback-without-write-lock.db");
    conn.execute("CREATE TABLE t(x)").unwrap();

    conn.get_pager().rollback(false, &conn, true);
}

fn open_memory_database(path: &str) -> (Arc<turso_core::Database>, Arc<turso_core::Connection>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = turso_core::Database::open_file(io, path, Arc::new(SqliteDialect)).unwrap();
    let conn = db
        .connect(turso_core::ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    (db, conn)
}
