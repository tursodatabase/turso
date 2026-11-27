use crate::common::ExecRows;
use std::path::Path;
use std::sync::Arc;
use turso_core::io::{PlatformIO, IO};
use turso_core::{ConnectOpts, Connection, Database, SessionRole};
use turso_sqlite::SqliteDialect;

/// Two database files whose names differ only after the last dot must not
/// share a logical log. Before, `app.1` and `app.2` both used `app.db-log`,
/// so `app.2` replayed the commits of `app.1`.
#[test]
fn test_databases_that_differ_after_the_last_dot_use_separate_logs() {
    let dir = tempfile::TempDir::new().unwrap();
    let template = dir.path().join("template.db");
    create_template(&template);
    let one = dir.path().join("app.1");
    let two = dir.path().join("app.2");
    std::fs::copy(&template, &one).unwrap();
    std::fs::copy(&template, &two).unwrap();

    with_conn(&one, |conn| {
        conn.execute("INSERT INTO notes VALUES ('app.1')").unwrap();
    });
    with_conn(&two, |conn| {
        assert_eq!(notes(conn), vec![]);
        conn.execute("INSERT INTO notes VALUES ('app.2')").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    });

    assert_eq!(with_conn(&two, notes), vec![("app.2".to_string(),)]);
    assert_eq!(with_conn(&one, notes), vec![("app.1".to_string(),)]);
    let names = file_names(dir.path());
    assert!(names.contains(&"app.1-log".to_string()), "{names:?}");
    assert!(names.contains(&"app.2-log".to_string()), "{names:?}");
    assert!(!names.contains(&"app.db-log".to_string()), "{names:?}");
    // For a `.db` file, the new name is the same as the old name.
    assert!(names.contains(&"template.db-log".to_string()), "{names:?}");
}

/// Creates an MVCC database at `path` with an empty `notes` table and no
/// commits in its logical log.
fn create_template(path: &Path) {
    with_conn(path, |conn| {
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.execute("CREATE TABLE notes (owner TEXT)").unwrap();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    });
}

/// Opens the database at `path`, runs `f` on a connection, and closes the
/// database, so that the next open reads the files again.
fn with_conn<T>(path: &Path, f: impl FnOnce(&Arc<Connection>) -> T) -> T {
    let conn = try_open(path).unwrap();
    let result = f(&conn);
    conn.close().unwrap();
    result
}

fn try_open(path: &Path) -> turso_core::Result<Arc<Connection>> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect))?;
    db.connect(ConnectOpts::new(SessionRole::Superuser))
}

fn notes(conn: &Arc<Connection>) -> Vec<(String,)> {
    conn.exec_rows("SELECT owner FROM notes ORDER BY owner")
}

fn file_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}
