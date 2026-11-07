use crate::sync::Arc;
use crate::{ConnectOpts, Connection, Database, EngineDialect, OpenFlags, OpenOptions, IO};

fn fixture(readonly: bool) -> (tempfile::TempDir, Arc<Connection>, String) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory
        .path()
        .join("main.db")
        .to_string_lossy()
        .into_owned();
    let io: Arc<dyn IO> = Arc::new(crate::PlatformIO::new().unwrap());
    {
        let database = Database::open_file(io.clone(), &path, Arc::new(EngineDialect)).unwrap();
        let connection = database.connect(ConnectOpts::new()).unwrap();
        connection
            .execute("CREATE TABLE test(value); INSERT INTO test VALUES(1)")
            .unwrap();
        connection
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }
    let flags = if readonly {
        OpenFlags::ReadOnly
    } else {
        OpenFlags::None
    };
    let database = Database::open(
        io,
        &path,
        OpenOptions::new(Arc::new(EngineDialect)).flags(flags),
    )
    .unwrap();
    let connection = database.connect(ConnectOpts::new()).unwrap();
    connection
        .execute("CREATE TEMP TABLE scratch(a); INSERT INTO scratch VALUES(1)")
        .unwrap();
    (directory, connection, path)
}

fn scalar(connection: &Arc<Connection>, sql: &str) -> i64 {
    let mut result = None;
    connection
        .prepare(sql)
        .unwrap()
        .run_with_row_callback(|row| {
            result = Some(row.get::<i64>(0)?);
            Ok(())
        })
        .unwrap();
    result.unwrap()
}

#[test]
fn temp_schema_changes_preserve_readonly_main() {
    let cases = [
        ("temp trigger", "CREATE TEMP TRIGGER tg AFTER INSERT ON scratch BEGIN SELECT 1; END; INSERT INTO scratch VALUES(2); DROP TRIGGER tg", "SELECT count(*) FROM scratch", 2),
        ("temp trigger on main", "CREATE TEMP TRIGGER tg AFTER INSERT ON main.test BEGIN SELECT 1; END; DROP TRIGGER tg", "SELECT count(*) FROM temp.sqlite_schema WHERE type='trigger'", 0),
        ("rename table", "ALTER TABLE temp.scratch RENAME TO renamed", "SELECT a FROM temp.renamed", 1),
        ("rename column", "ALTER TABLE temp.scratch RENAME COLUMN a TO renamed", "SELECT renamed FROM temp.scratch", 1),
        ("add column", "ALTER TABLE temp.scratch ADD COLUMN extra; UPDATE temp.scratch SET extra=7", "SELECT extra FROM temp.scratch", 7),
        ("analyze", "ANALYZE temp", "SELECT count(*) FROM temp.scratch", 1),
        ("user version", "PRAGMA temp.user_version=7", "PRAGMA temp.user_version", 7),
    ];
    let mut errors = Vec::new();
    for (name, sql, verify, expected) in cases {
        let (_directory, connection, path) = fixture(true);
        let main_before = std::fs::read(&path).unwrap();
        let wal_path = format!("{path}-wal");
        let wal_before = std::fs::read(&wal_path).ok();
        match connection.execute(sql) {
            Ok(()) => assert_eq!(scalar(&connection, verify), expected, "{name}"),
            Err(error) => errors.push(format!("{name}: {error}")),
        }
        assert_eq!(std::fs::read(&path).unwrap(), main_before, "{name}");
        assert_eq!(std::fs::read(&wal_path).ok(), wal_before, "{name}");
        assert_eq!(scalar(&connection, "SELECT value FROM main.test"), 1);
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn temp_schema_changes_do_not_emit_main_write_transactions() {
    let cases = [
        "CREATE TEMP TRIGGER tg AFTER INSERT ON scratch BEGIN SELECT 1; END",
        "DROP TRIGGER existing",
        "ALTER TABLE temp.scratch RENAME TO renamed",
        "ALTER TABLE temp.scratch RENAME COLUMN a TO renamed",
        "ALTER TABLE temp.scratch ADD COLUMN extra",
        "ANALYZE temp",
        "PRAGMA temp.user_version=7",
    ];
    let mut errors = Vec::new();
    for sql in cases {
        let (_directory, connection, _) = fixture(false);
        connection
            .execute("CREATE TEMP TRIGGER existing AFTER INSERT ON scratch BEGIN SELECT 1; END")
            .unwrap();
        let mut transactions = Vec::new();
        connection
            .prepare(format!("EXPLAIN {sql}"))
            .unwrap()
            .run_with_row_callback(|row| {
                if row.get::<&str>(1)? == "Transaction" {
                    transactions.push((row.get::<i64>(2)?, row.get::<i64>(3)?));
                }
                Ok(())
            })
            .unwrap();
        if transactions.iter().any(|&(db, mode)| {
            db == crate::MAIN_DB_ID as i64 && mode == crate::TransactionMode::Write as i64
        }) {
            errors.push(format!("{sql}: {transactions:?}"));
        }
        assert!(
            transactions.contains(&(
                crate::TEMP_DB_ID as i64,
                crate::TransactionMode::Write as i64
            )),
            "{sql}: {transactions:?}"
        );
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}
