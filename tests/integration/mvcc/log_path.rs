use crate::common::ExecRows;
use std::path::{Path, PathBuf};
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

/// A database whose log an older version wrote with the old name continues to
/// use that log until a checkpoint empties it, and then uses the new name.
#[test]
fn test_database_continues_to_use_its_log_from_an_older_version() {
    let dir = tempfile::TempDir::new().unwrap();
    let template = dir.path().join("template.db");
    create_template(&template);
    let path = create_with_old_log(dir.path(), &template, "app.sqlite");
    let old_log = dir.path().join("app.db-log");
    let new_log = sidecar(&path, "-log");

    with_conn(&path, |conn| {
        assert_eq!(notes(conn), vec![("app.sqlite".to_string(),)]);
        // A commit after a checkpoint goes to the log that is open.
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        conn.execute("INSERT INTO notes VALUES ('second')").unwrap();
    });
    assert!(!new_log.exists());
    assert!(file_len(&old_log) > 0);

    with_conn(&path, |conn| {
        assert_eq!(
            notes(conn),
            vec![("app.sqlite".to_string(),), ("second".to_string(),)]
        );
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    });
    assert_eq!(file_len(&old_log), 0);

    with_conn(&path, |conn| {
        conn.execute("INSERT INTO notes VALUES ('third')").unwrap();
    });
    assert!(file_len(&new_log) > 0);
    assert_eq!(file_len(&old_log), 0);
    assert_eq!(
        with_conn(&path, notes),
        vec![
            ("app.sqlite".to_string(),),
            ("second".to_string(),),
            ("third".to_string(),)
        ]
    );
}

/// When an older version used one log for two MVCC databases, the log can hold
/// commits of each of them, so neither database opens.
#[test]
fn test_old_log_of_two_mvcc_databases_is_an_error() {
    let dir = tempfile::TempDir::new().unwrap();
    let template = dir.path().join("template.db");
    create_template(&template);
    let one = create_with_old_log(dir.path(), &template, "app.1");
    let two = dir.path().join("app.2");
    std::fs::copy(&template, &two).unwrap();

    for path in [&one, &two] {
        let err = try_open(path).map(|_| ()).unwrap_err();
        assert!(
            matches!(&err, turso_core::LimboError::Corrupt(msg) if msg.contains("app.db-log")),
            "{err:?}"
        );
        assert!(!sidecar(path, "-log").exists());
    }
}

/// A log with the old name does not belong to a database in WAL mode. Before,
/// a WAL database next to the log of another database failed to open with a
/// corruption error.
#[test]
fn test_old_log_next_to_a_wal_database() {
    let dir = tempfile::TempDir::new().unwrap();
    let template = dir.path().join("template.db");
    create_template(&template);
    let path = create_with_old_log(dir.path(), &template, "app.sqlite");
    let wal_db = dir.path().join("app.bak");
    create_wal_database(&wal_db);

    assert_eq!(with_conn(&wal_db, notes), vec![]);
    assert_eq!(with_conn(&path, notes), vec![("app.sqlite".to_string(),)]);
}

/// A database that enters MVCC mode next to the old log of another database
/// uses the new name, and the other database continues to own the old log.
#[test]
fn test_journal_mode_change_next_to_an_old_log() {
    let dir = tempfile::TempDir::new().unwrap();
    let template = dir.path().join("template.db");
    create_template(&template);
    let one = create_with_old_log(dir.path(), &template, "app.1");
    let two = dir.path().join("app.2");
    create_wal_database(&two);

    with_conn(&two, |conn| {
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        conn.execute("INSERT INTO notes VALUES ('app.2')").unwrap();
    });
    assert!(file_len(&sidecar(&two, "-log")) > 0);

    assert_eq!(with_conn(&two, notes), vec![("app.2".to_string(),)]);
    assert_eq!(with_conn(&one, notes), vec![("app.1".to_string(),)]);
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

/// Copies the template to `name` in `dir` and commits one row with owner
/// `name` that is only in the logical log. Then gives the log the name that
/// older versions used, as if an older version had written it.
fn create_with_old_log(dir: &Path, template: &Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::copy(template, &path).unwrap();
    with_conn(&path, |conn| {
        conn.execute(format!("INSERT INTO notes VALUES ('{name}')"))
            .unwrap();
    });
    let log_path = sidecar(&path, "-log");
    assert!(file_len(&log_path) > 0);

    // The row is only in the log: the database file without its log has no rows.
    let copy_dir = tempfile::TempDir::new().unwrap();
    let copy = copy_dir.path().join(name);
    std::fs::copy(&path, &copy).unwrap();
    assert_eq!(with_conn(&copy, notes), vec![]);

    std::fs::rename(&log_path, path.with_extension("db-log")).unwrap();
    path
}

/// Creates a database at `path` in WAL mode with an empty `notes` table and an
/// empty WAL.
fn create_wal_database(path: &Path) {
    with_conn(path, |conn| {
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

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}
