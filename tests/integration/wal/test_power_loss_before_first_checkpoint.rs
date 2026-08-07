//! Regression test for https://github.com/tursodatabase/turso/issues/7995
//!
//! With `synchronous=FULL`, every committed transaction is fsync-acknowledged
//! to the `-wal` file before the commit returns. If the process crashes (power
//! loss) before the *first* checkpoint, the committed data exists only in the
//! `-wal`. Recovery must replay that durable `-wal` on reopen — otherwise the
//! whole first WAL epoch of committed transactions is silently lost.
//!
//! The fix keeps the invariant SQLite guarantees: a WAL only ever exists next
//! to a database file with at least one page, because page 1 is fsync'd to
//! the main file before the first WAL commit can fsync frames. So the crash
//! image always has a non-empty main file, and ordinary WAL replay recovers
//! the committed data. The flip side is that a WAL holding frames next to an
//! *empty* database file does not belong to it, and is thrown away on open
//! the way SQLite throws it away.
//!
//! The crash model is the shared [`crate::unreliable_io::UnreliableIo`]:
//! writes only become durable when the file is fsync'd. After the "crash",
//! only the fsync-acknowledged bytes of each file are materialized on disk,
//! and the database is reopened with the regular platform IO. All committed
//! rows must be recovered.

use std::sync::Arc;

use turso_core::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};

use crate::common::limbo_exec_rows;
use crate::unreliable_io::UnreliableIo;

/// Committed (fsync-acknowledged) transactions must survive power loss even
/// when the crash happens before the first checkpoint, i.e. while the data
/// only exists in the `-wal` file.
#[test]
fn test_committed_wal_survives_power_loss_before_first_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("crash.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let io = Arc::new(UnreliableIo::new());

    // Phase 1: commit 10 transactions with synchronous=FULL, never checkpoint.
    {
        let db = Database::open_file(io.clone(), &db_path_str, Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        limbo_exec_rows(&conn, "PRAGMA synchronous=FULL");
        limbo_exec_rows(&conn, "CREATE TABLE t(x)");
        for i in 0..10 {
            // Autocommit: each INSERT is its own committed transaction whose
            // WAL frames are fsync'd before the commit returns.
            limbo_exec_rows(&conn, &format!("INSERT INTO t VALUES ({i})"));
        }
        // Crash here: no clean shutdown, no checkpoint. The Database/Connection
        // are intentionally leaked so no close-time cleanup runs.
        std::mem::forget(conn);
        std::mem::forget(db);
    }

    // Phase 2: materialize only the fsync-acknowledged bytes of every file,
    // exactly what a power loss would leave on disk.
    let durable = io.durable_files();
    let wal_path = format!("{db_path_str}-wal");
    let durable_wal = durable.get(&wal_path).map(|bytes| bytes.len()).unwrap_or(0);
    assert!(
        durable_wal > 0,
        "synchronous=FULL commits must have fsync'd frames into the -wal"
    );
    for (path, bytes) in &durable {
        std::fs::write(path, bytes).unwrap();
    }

    // Phase 3: reopen the crashed image with the regular platform IO and
    // verify that recovery replays the durable WAL.
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, &db_path_str, Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT count(*) FROM t");
    assert_eq!(
        rows,
        vec![vec![rusqlite::types::Value::Integer(10)]],
        "all 10 committed (fsync-acknowledged) transactions must be recovered \
         from the WAL after a crash before the first checkpoint"
    );
}

/// SQLite guarantees that a WAL never exists next to an empty main database
/// file: creating a database in WAL mode commits page 1 to the main file
/// before any WAL frame. SQLite relies on that invariant —
/// `pagerOpenWalIfPresent()` deletes a WAL it finds next to a zero-page
/// database — so if we ever left a pre-first-checkpoint crash image with a
/// 0-byte main file, stock SQLite would silently destroy all committed data.
///
/// Verify that we keep the same invariant: after a power loss before the
/// first checkpoint, the durable main database file already holds page 1, and
/// SQLite itself can open the crash image and read every committed row.
#[test]
fn test_page1_is_durable_in_main_file_before_first_wal_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("crash.db");
    let db_path_str = db_path.to_str().unwrap().to_string();

    let io = Arc::new(UnreliableIo::new());

    // Commit transactions with synchronous=FULL, never checkpoint, "crash".
    {
        let db = Database::open_file(io.clone(), &db_path_str, Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        limbo_exec_rows(&conn, "PRAGMA synchronous=FULL");
        limbo_exec_rows(&conn, "CREATE TABLE t(x)");
        for i in 0..10 {
            limbo_exec_rows(&conn, &format!("INSERT INTO t VALUES ({i})"));
        }
        std::mem::forget(conn);
        std::mem::forget(db);
    }

    // Only fsync-acknowledged bytes survive the power loss.
    let durable = io.durable_files();
    let durable_db = durable
        .get(&db_path_str)
        .map(|bytes| bytes.len())
        .unwrap_or(0);
    assert!(
        durable_db >= 4096,
        "page 1 must be durable in the main database file before the first \
         WAL commit (got {durable_db} durable bytes); a WAL next to a 0-byte \
         database file is a state SQLite deletes the WAL for"
    );
    for (path, bytes) in &durable {
        std::fs::write(path, bytes).unwrap();
    }

    // Stock SQLite must be able to open the crash image and recover every
    // committed transaction from the WAL.
    let sqlite = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = sqlite
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        count, 10,
        "SQLite must recover all committed transactions from the crash image"
    );
}

/// A WAL that holds frames next to a database file with zero pages does not
/// belong to it: page 1 is fsync'd to the main file before the first WAL
/// commit, so no crash can produce that state. It happens when a user deletes
/// the database file to reset it but forgets the `-wal`. Replaying such a WAL
/// would splice a dead database's pages into the new one once it grows past
/// zero pages, so the WAL is thrown away and the database opens empty —
/// exactly what SQLite's `pagerOpenWalIfPresent()` does, which the test
/// checks against stock SQLite on the same pair of files.
#[test]
fn test_orphan_wal_of_an_empty_database_is_discarded_like_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.db");
    let source_path_str = source_path.to_str().unwrap().to_string();

    // Commit data that still sits in the WAL (no checkpoint: leak the
    // connection so no close-time cleanup runs) and materialize the files.
    let io = Arc::new(UnreliableIo::new());
    {
        let db =
            Database::open_file(io.clone(), &source_path_str, Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        limbo_exec_rows(&conn, "PRAGMA synchronous=FULL");
        limbo_exec_rows(&conn, "CREATE TABLE t(x)");
        limbo_exec_rows(&conn, "INSERT INTO t VALUES (1)");
        std::mem::forget(conn);
        std::mem::forget(db);
    }
    for (path, bytes) in &io.durable_files() {
        std::fs::write(path, bytes).unwrap();
    }
    let source_wal = std::fs::read(format!("{source_path_str}-wal")).unwrap();
    assert!(
        !source_wal.is_empty(),
        "the committed data must sit in the -wal"
    );

    // Copy the WAL next to a database file that does not exist, which is what
    // a user leaves behind by deleting the `.db` to reset the database and
    // forgetting the `-wal`. Copies keep the leaked handles of the phase above
    // away from the files under test.
    let orphan_wal = |name: &str| -> (String, std::path::PathBuf) {
        let db_path = dir.path().join(format!("{name}.db"));
        let wal_path = dir.path().join(format!("{name}.db-wal"));
        std::fs::write(&wal_path, &source_wal).unwrap();
        (db_path.to_str().unwrap().to_string(), wal_path)
    };

    // Turso discards the orphan WAL and opens the database empty.
    let (turso_db_path, turso_wal_path) = orphan_wal("turso_reset");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, &turso_db_path, Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT count(*) FROM sqlite_schema");
    assert_eq!(
        rows,
        vec![vec![rusqlite::types::Value::Integer(0)]],
        "the orphan WAL must not be replayed into the fresh database"
    );
    // The orphan frames are gone for good: the WAL file is deleted and
    // immediately recreated empty by the open that follows the deletion.
    assert_eq!(
        std::fs::metadata(&turso_wal_path).unwrap().len(),
        0,
        "the orphan frames must be gone so they cannot come back"
    );

    // A read-only open also refuses to replay the orphan WAL, but writes
    // nothing: it leaves both files exactly as they are.
    let (ro_db_path, ro_wal_path) = orphan_wal("turso_reset_readonly");
    std::fs::write(&ro_db_path, b"").unwrap();
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        &ro_db_path,
        OpenFlags::ReadOnly,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT count(*) FROM sqlite_schema");
    assert_eq!(
        rows,
        vec![vec![rusqlite::types::Value::Integer(0)]],
        "a read-only open must not replay the orphan WAL either"
    );
    assert_eq!(
        std::fs::metadata(&ro_wal_path).unwrap().len(),
        source_wal.len() as u64,
        "a read-only open must leave the WAL file untouched"
    );

    // Stock SQLite, given the same files, behaves the same way.
    let (sqlite_db_path, sqlite_wal_path) = orphan_wal("sqlite_reset");
    let sqlite = rusqlite::Connection::open(&sqlite_db_path).unwrap();
    let count: i64 = sqlite
        .query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0, "SQLite also opens the database empty");
    assert!(
        !sqlite_wal_path.exists(),
        "SQLite also deletes the orphan WAL"
    );
}
