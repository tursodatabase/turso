use limbo_core::{CheckpointStatus, Connection, Database, IO};
use rand::{rng, RngCore};
use rusqlite::params;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use tempfile::TempDir;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[allow(dead_code)]
pub struct TempDatabase {
    pub path: PathBuf,
    pub io: Arc<dyn IO + Send>,
}
unsafe impl Send for TempDatabase {}

#[allow(dead_code, clippy::arc_with_non_send_sync)]
impl TempDatabase {
    pub fn new_empty() -> Self {
        Self::new(&format!("test-{}.db", rng().next_u32()))
    }

    pub fn new(db_name: &str) -> Self {
        let mut path = TempDir::new().unwrap().into_path();
        path.push(db_name);
        let io: Arc<dyn IO + Send> = Arc::new(limbo_core::PlatformIO::new().unwrap());
        Self { path, io }
    }

    pub fn new_with_existent(db_path: &Path) -> Self {
        let io: Arc<dyn IO + Send> = Arc::new(limbo_core::PlatformIO::new().unwrap());
        Self {
            path: db_path.to_path_buf(),
            io,
        }
    }

    pub fn new_with_rusqlite(table_sql: &str) -> Self {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .finish();
        let mut path = TempDir::new().unwrap().into_path();
        path.push("test.db");
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .pragma_update(None, "journal_mode", "wal")
                .unwrap();
            connection.execute(table_sql, ()).unwrap();
        }
        let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::PlatformIO::new().unwrap());

        Self { path, io }
    }

    pub fn connect_limbo(&self) -> Rc<limbo_core::Connection> {
        Self::connect_limbo_with_flags(&self, limbo_core::OpenFlags::default())
    }

    pub fn connect_limbo_with_flags(
        &self,
        flags: limbo_core::OpenFlags,
    ) -> Rc<limbo_core::Connection> {
        log::debug!("conneting to limbo");
        let db = Database::open_file_with_flags(
            self.io.clone(),
            self.path.to_str().unwrap(),
            flags,
            false,
        )
        .unwrap();

        let conn = db.connect().unwrap();
        log::debug!("connected to limbo");
        conn
    }

    pub fn limbo_database(&self) -> Arc<limbo_core::Database> {
        log::debug!("conneting to limbo");
        Database::open_file(self.io.clone(), self.path.to_str().unwrap(), false).unwrap()
    }
}

pub(crate) fn do_flush(conn: &Rc<Connection>, tmp_db: &TempDatabase) -> anyhow::Result<()> {
    loop {
        match conn.cacheflush()? {
            CheckpointStatus::Done(_) => {
                break;
            }
            CheckpointStatus::IO => {
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
    let mut stmt = conn.prepare(&query).unwrap();
    let mut rows = stmt.query(params![]).unwrap();
    let mut results = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        let mut result = Vec::new();
        for i in 0.. {
            let column: rusqlite::types::Value = match row.get(i) {
                Ok(column) => column,
                Err(rusqlite::Error::InvalidColumnIndex(_)) => break,
                Err(err) => panic!("unexpected rusqlite error: {}", err),
            };
            result.push(column);
        }
        results.push(result)
    }

    results
}

pub(crate) fn limbo_exec_rows(
    db: &TempDatabase,
    conn: &Rc<limbo_core::Connection>,
    query: &str,
) -> Vec<Vec<rusqlite::types::Value>> {
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = Vec::new();
    'outer: loop {
        let row = loop {
            let result = stmt.step().unwrap();
            match result {
                limbo_core::StepResult::Row => {
                    let row = stmt.row().unwrap();
                    break row;
                }
                limbo_core::StepResult::IO => {
                    db.io.run_once().unwrap();
                    continue;
                }
                limbo_core::StepResult::Done => break 'outer,
                r => panic!("unexpected result {:?}: expecting single row", r),
            }
        };
        let row = row
            .get_values()
            .map(|x| match x {
                limbo_core::Value::Null => rusqlite::types::Value::Null,
                limbo_core::Value::Integer(x) => rusqlite::types::Value::Integer(*x),
                limbo_core::Value::Float(x) => rusqlite::types::Value::Real(*x),
                limbo_core::Value::Text(x) => rusqlite::types::Value::Text(x.as_str().to_string()),
                limbo_core::Value::Blob(x) => rusqlite::types::Value::Blob(x.to_vec()),
            })
            .collect();
        rows.push(row);
    }
    rows
}

pub(crate) fn limbo_exec_rows_error(
    db: &TempDatabase,
    conn: &Rc<limbo_core::Connection>,
    query: &str,
) -> limbo_core::Result<()> {
    let mut stmt = conn.prepare(query)?;
    loop {
        let result = stmt.step()?;
        match result {
            limbo_core::StepResult::IO => {
                db.io.run_once()?;
                continue;
            }
            limbo_core::StepResult::Done => return Ok(()),
            r => panic!("unexpected result {:?}: expecting single row", r),
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
        let path = TempDir::new().unwrap().into_path().join("temp_read_only");
        let db = TempDatabase::new_with_existent(&path);
        {
            let conn = db.connect_limbo();
            let ret = limbo_exec_rows(&db, &conn, "CREATE table t(a)");
            assert!(ret.is_empty(), "{:?}", ret);
            limbo_exec_rows(&db, &conn, "INSERT INTO t values (1)");
            conn.close().unwrap()
        }

        {
            let conn = db.connect_limbo_with_flags(
                limbo_core::OpenFlags::default() | limbo_core::OpenFlags::ReadOnly,
            );
            let ret = limbo_exec_rows(&db, &conn, "SELECT * from t");
            assert_eq!(ret, vec![vec![Value::Integer(1)]]);

            let err = limbo_exec_rows_error(&db, &conn, "INSERT INTO t values (1)").unwrap_err();
            assert!(matches!(err, limbo_core::LimboError::ReadOnly), "{:?}", err);
        }
        Ok(())
    }
}
