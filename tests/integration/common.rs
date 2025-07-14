use rand::{rng, RngCore};
use rusqlite::params;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use turso_core::{Connection, Database, PagerCacheFlushStatus, IO};

#[allow(dead_code)]
pub struct TempDatabase {
    pub path: PathBuf,
    pub io: Arc<dyn IO + Send>,
    pub db: Arc<Database>,
}
unsafe impl Send for TempDatabase {}

#[allow(dead_code, clippy::arc_with_non_send_sync)]
impl TempDatabase {
    pub fn new_empty(enable_indexes: bool) -> Self {
        Self::new(&format!("test-{}.db", rng().next_u32()), enable_indexes)
    }

    pub fn new(db_name: &str, enable_indexes: bool) -> Self {
        let mut path = TempDir::new().unwrap().keep();
        path.push(db_name);
        let io: Arc<dyn IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            false,
            enable_indexes,
        )
        .unwrap();
        Self { path, io, db }
    }

    pub fn new_with_existent(db_path: &Path, enable_indexes: bool) -> Self {
        Self::new_with_existent_with_flags(
            db_path,
            turso_core::OpenFlags::default(),
            enable_indexes,
        )
    }

    pub fn new_with_existent_with_flags(
        db_path: &Path,
        flags: turso_core::OpenFlags,
        enable_indexes: bool,
    ) -> Self {
        let io: Arc<dyn IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            flags,
            false,
            enable_indexes,
        )
        .unwrap();
        Self {
            path: db_path.to_path_buf(),
            io,
            db,
        }
    }

    pub fn new_with_rusqlite(table_sql: &str, enable_indexes: bool) -> Self {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .finish();
        let mut path = TempDir::new().unwrap().keep();
        path.push("test.db");
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .pragma_update(None, "journal_mode", "wal")
                .unwrap();
            connection.execute(table_sql, ()).unwrap();
        }
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            false,
            enable_indexes,
        )
        .unwrap();

        Self { path, io, db }
    }

    pub fn connect_limbo(&self) -> Arc<turso_core::Connection> {
        log::debug!("conneting to limbo");

        let conn = self.db.connect().unwrap();
        log::debug!("connected to limbo");
        conn
    }

    pub fn limbo_database(&self, enable_indexes: bool) -> Arc<turso_core::Database> {
        log::debug!("conneting to limbo");
        Database::open_file(
            self.io.clone(),
            self.path.to_str().unwrap(),
            false,
            enable_indexes,
        )
        .unwrap()
    }
}

pub(crate) fn do_flush(conn: &Arc<Connection>, tmp_db: &TempDatabase) -> anyhow::Result<()> {
    loop {
        match conn.cacheflush()? {
            PagerCacheFlushStatus::Done => {
                break;
            }
            PagerCacheFlushStatus::IO => {
                tmp_db.io.run_once()?;
            }
        }
    }
    Ok(())
}

pub(crate) fn compare_string(a: impl AsRef<str>, b: impl AsRef<str>) {
    let a = a.as_ref();
    let b = b.as_ref();

    assert_eq!(a.len(), b.len(), "Strings are not equal in size!");

    let a = a.as_bytes();
    let b = b.as_bytes();

    let len = a.len();
    for i in 0..len {
        if a[i] != b[i] {
            println!(
                "Bytes differ \n\t at index: dec -> {} hex -> {:#02x} \n\t values dec -> {}!={} hex -> {:#02x}!={:#02x}",
                i, i, a[i], b[i], a[i], b[i]
            );
            break;
        }
    }
}

pub fn maybe_setup_tracing() {
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_line_number(true)
                .with_thread_ids(true),
        )
        .with(EnvFilter::from_default_env())
        .try_init();
}

pub(crate) fn sqlite_exec_rows(
    conn: &rusqlite::Connection,
    query: &str,
) -> Vec<Vec<rusqlite::types::Value>> {
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = stmt.query(params![]).unwrap();
    let mut results = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        let mut result = Vec::new();
        for i in 0.. {
            let column: rusqlite::types::Value = match row.get(i) {
                Ok(column) => column,
                Err(rusqlite::Error::InvalidColumnIndex(_)) => break,
                Err(err) => panic!("unexpected rusqlite error: {err}"),
            };
            result.push(column);
        }
        results.push(result)
    }

    results
}

pub(crate) fn limbo_exec_rows(
    _db: &TempDatabase,
    conn: &Arc<turso_core::Connection>,
    query: &str,
) -> Vec<Vec<rusqlite::types::Value>> {
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = Vec::new();
    'outer: loop {
        let row = loop {
            let result = stmt.step().unwrap();
            match result {
                turso_core::StepResult::Row => {
                    let row = stmt.row().unwrap();
                    break row;
                }
                turso_core::StepResult::IO => {
                    stmt.run_once().unwrap();
                    continue;
                }

                turso_core::StepResult::Done => break 'outer,
                r => panic!("unexpected result {r:?}: expecting single row"),
            }
        };
        let row = row
            .get_values()
            .map(|x| match x {
                turso_core::Value::Null => rusqlite::types::Value::Null,
                turso_core::Value::Integer(x) => rusqlite::types::Value::Integer(*x),
                turso_core::Value::Float(x) => rusqlite::types::Value::Real(*x),
                turso_core::Value::Text(x) => rusqlite::types::Value::Text(x.as_str().to_string()),
                turso_core::Value::Blob(x) => rusqlite::types::Value::Blob(x.to_vec()),
            })
            .collect();
        rows.push(row);
    }
    rows
}

pub(crate) fn limbo_exec_rows_error(
    _db: &TempDatabase,
    conn: &Arc<turso_core::Connection>,
    query: &str,
) -> turso_core::Result<()> {
    let mut stmt = conn.prepare(query)?;
    loop {
        let result = stmt.step()?;
        match result {
            turso_core::StepResult::IO => {
                stmt.run_once()?;
                continue;
            }
            turso_core::StepResult::Done => return Ok(()),
            r => panic!("unexpected result {r:?}: expecting single row"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::vec;
    use tempfile::TempDir;

    use super::{limbo_exec_rows, limbo_exec_rows_error, TempDatabase};
    use rusqlite::types::Value;

    #[test]
    fn test_statement_columns() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tmp_db = TempDatabase::new_with_rusqlite(
            "create table test (foo integer, bar integer, baz integer);",
            false,
        );
        let conn = tmp_db.connect_limbo();

        let stmt = conn.prepare("select * from test;")?;

        let columns = stmt.num_columns();
        assert_eq!(columns, 3);
        assert_eq!(stmt.get_column_name(0), "foo");
        assert_eq!(stmt.get_column_name(1), "bar");
        assert_eq!(stmt.get_column_name(2), "baz");

        let stmt = conn.prepare("select foo, bar from test;")?;

        let columns = stmt.num_columns();
        assert_eq!(columns, 2);
        assert_eq!(stmt.get_column_name(0), "foo");
        assert_eq!(stmt.get_column_name(1), "bar");

        let stmt = conn.prepare("delete from test;")?;
        let columns = stmt.num_columns();
        assert_eq!(columns, 0);

        let stmt = conn.prepare("insert into test (foo, bar, baz) values (1, 2, 3);")?;
        let columns = stmt.num_columns();
        assert_eq!(columns, 0);

        let stmt = conn.prepare("delete from test where foo = 1")?;
        let columns = stmt.num_columns();
        assert_eq!(columns, 0);

        Ok(())
    }

    #[test]
    fn test_limbo_open_read_only() -> anyhow::Result<()> {
        let path = TempDir::new().unwrap().keep().join("temp_read_only");
        {
            let db = TempDatabase::new_with_existent_with_flags(
                &path,
                turso_core::OpenFlags::default(),
                false,
            );
            let conn = db.connect_limbo();
            let ret = limbo_exec_rows(&db, &conn, "CREATE table t(a)");
            assert!(ret.is_empty(), "{ret:?}");
            limbo_exec_rows(&db, &conn, "INSERT INTO t values (1)");
            conn.close().unwrap()
        }

        {
            let db = TempDatabase::new_with_existent_with_flags(
                &path,
                turso_core::OpenFlags::default() | turso_core::OpenFlags::ReadOnly,
                false,
            );
            let conn = db.connect_limbo();
            let ret = limbo_exec_rows(&db, &conn, "SELECT * from t");
            assert_eq!(ret, vec![vec![Value::Integer(1)]]);

            let err = limbo_exec_rows_error(&db, &conn, "INSERT INTO t values (1)").unwrap_err();
            assert!(matches!(err, turso_core::LimboError::ReadOnly), "{err:?}");
        }
        Ok(())
    }

    #[test]
    fn test_unique_index_ordering() -> anyhow::Result<()> {
        use rand::Rng;

        let db = TempDatabase::new_empty(true);
        let conn = db.connect_limbo();

        let _ = limbo_exec_rows(&db, &conn, "CREATE TABLE t(x INTEGER UNIQUE)");

        // Insert 100 random integers between -1000 and 1000
        let mut expected = Vec::new();
        let mut rng = rand::rng();
        let mut i = 0;
        while i < 100 {
            let val = rng.random_range(-1000..1000);
            if expected.contains(&val) {
                continue;
            }
            i += 1;
            expected.push(val);
            let ret = limbo_exec_rows(&db, &conn, &format!("INSERT INTO t VALUES ({val})"));
            assert!(ret.is_empty(), "Insert failed for value {val}: {ret:?}");
        }

        // Sort expected values to match index order
        expected.sort();

        // Query all values and verify they come back in sorted order
        let ret = limbo_exec_rows(&db, &conn, "SELECT x FROM t");
        let actual: Vec<i64> = ret
            .into_iter()
            .map(|row| match &row[0] {
                Value::Integer(i) => *i,
                _ => panic!("Expected integer value"),
            })
            .collect();

        assert_eq!(actual, expected, "Values not returned in sorted order");

        Ok(())
    }

    #[test]
    fn test_large_unique_blobs() -> anyhow::Result<()> {
        let path = TempDir::new().unwrap().keep().join("temp_read_only");
        let db = TempDatabase::new_with_existent(&path, true);
        let conn = db.connect_limbo();

        let _ = limbo_exec_rows(&db, &conn, "CREATE TABLE t(x BLOB UNIQUE)");

        // Insert 11 unique 1MB blobs
        for i in 0..11 {
            println!("Inserting blob #{i}");
            let ret = limbo_exec_rows(&db, &conn, "INSERT INTO t VALUES (randomblob(1024*1024))");
            assert!(ret.is_empty(), "Insert #{i} failed: {ret:?}");
        }

        // Verify we have 11 rows
        let ret = limbo_exec_rows(&db, &conn, "SELECT count(*) FROM t");
        assert_eq!(
            ret,
            vec![vec![Value::Integer(11)]],
            "Expected 11 rows but got {ret:?}",
        );

        Ok(())
    }
}
