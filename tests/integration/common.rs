use limbo_core::{CheckpointStatus, Connection, Database, IO};
use rand::{rng, RngCore};
use regex::Regex;
use rusqlite::types::Value;
use rusqlite::{params, OpenFlags};
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

    pub fn new_existent(db_path: &Path) -> Self {
        let io: Arc<dyn IO + Send> = Arc::new(limbo_core::PlatformIO::new().unwrap());
        Self {
            path: db_path.to_path_buf(),
            io,
        }
    }

    pub fn new_in_memory() -> Self {
        let io: Arc<dyn IO + Send> = Arc::new(limbo_core::MemoryIO::new());
        // No path for Memory IO
        Self {
            path: "".into(),
            io,
        }
    }

    pub fn new_with_rusqlite(table_sql: &str) -> Self {
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

#[derive(Debug)]
enum TestMode {
    Single { sql: &'static str },
    Many { sql_queries: Vec<&'static str> },
    MemoryError { sql_queries: Vec<&'static str> },
    Regex { sql: &'static str, regex: Regex },
}

#[derive(Debug)]
pub(crate) struct SqlTester {
    mode: TestMode,
    values: Vec<Vec<Value>>,
    is_random_db: bool,
}

impl SqlTester {
    pub(crate) fn single(sql: &'static str, values: Vec<Vec<Value>>) -> Self {
        Self {
            mode: TestMode::Single { sql },
            values,
            is_random_db: false,
        }
    }

    pub(crate) fn many(sql_queries: Vec<&'static str>, values: Vec<Vec<Value>>) -> Self {
        Self {
            mode: TestMode::Many { sql_queries },
            values,
            is_random_db: false,
        }
    }

    pub(crate) fn memory_error(sql_queries: Vec<&'static str>) -> Self {
        Self {
            mode: TestMode::MemoryError { sql_queries },
            values: Vec::new(),
            is_random_db: false,
        }
    }

    pub(crate) fn regex(sql: &'static str, regex: Regex) -> Self {
        Self {
            mode: TestMode::Regex { sql, regex },
            values: Vec::new(),
            is_random_db: false,
        }
    }

    pub(crate) fn with_is_random_db(&mut self, is_random_db: bool) -> &mut Self {
        self.is_random_db = is_random_db;
        return self;
    }

    pub(crate) fn exec_sql(&self, db_path: Option<PathBuf>) {
        {
            let sqlite_conn = if let Some(ref db_path) = db_path {
                if !self.is_random_db {
                    rusqlite::Connection::open_with_flags(
                        db_path.clone(),
                        OpenFlags::SQLITE_OPEN_READ_ONLY,
                    )
                    .unwrap()
                } else {
                    // This can be a random db_path that was not created yet
                    rusqlite::Connection::open(db_path.clone()).unwrap()
                }
            } else {
                rusqlite::Connection::open_in_memory().unwrap()
            };
            self.exec_sql_sqlite(sqlite_conn, &db_path);
        }

        {
            let db = if let Some(ref db_path) = db_path {
                TempDatabase::new_existent(db_path)
            } else {
                TempDatabase::new_in_memory()
            };
            let limbo_conn = if self.is_random_db || db_path.is_none() {
                db.connect_limbo()
            } else {
                db.connect_limbo_with_flags(
                    limbo_core::OpenFlags::default() | limbo_core::OpenFlags::ReadOnly,
                )
            };
            self.exec_sql_limbo(db, limbo_conn, &db_path);
        }
    }

    fn exec_sql_sqlite(&self, sqlite_conn: rusqlite::Connection, db_path: &Option<PathBuf>) {
        let db_path = db_path.as_ref().map(|db_path| db_path.to_string_lossy());
        let db_path = db_path.unwrap_or(std::borrow::Cow::Borrowed("memory"));

        let sqlite = match &self.mode {
            TestMode::Single { sql } => sqlite_exec_rows(&sqlite_conn, sql),
            TestMode::Many { sql_queries } => {
                let mut sqlite_values = Vec::with_capacity(sql_queries.len());
                for sql in sql_queries {
                    let sqlite = sqlite_exec_rows(&sqlite_conn, &sql);
                    sqlite_values.extend(sqlite);
                }
                sqlite_values
            }
            TestMode::MemoryError { sql_queries } => {
                let contains_error = sql_queries
                    .iter()
                    .map(|sql| sqlite_exec_rows_error(&sqlite_conn, &sql))
                    .any(|res| res.is_err());
                assert!(contains_error, "no error thrown in SQLite");
                return;
            }
            TestMode::Regex { sql, regex } => {
                let sqlite = sqlite_exec_rows(&sqlite_conn, sql);
                for values in sqlite.iter() {
                    for val in values {
                        match val {
                            Value::Text(s) => {
                                assert!(
                                    regex.is_match(s),
                                    "regex `{}` did not match in {}
                                    query: {:#?}, 
                                    values: {:?}, 
                                    sqlite: {:?}, 
                                    db: {}",
                                    regex,
                                    s,
                                    sql,
                                    values,
                                    sqlite,
                                    db_path,
                                );
                            }
                            _ => panic!("only expected Text Value for regex test"),
                        }
                    }
                }
                return;
            }
        };

        let sql = match &self.mode {
            TestMode::Single { sql } => sql as &dyn std::fmt::Debug,
            TestMode::Many { sql_queries } => sql_queries as &dyn std::fmt::Debug,
            _ => unreachable!(),
        };

        assert_eq!(
            self.values, sqlite,
            "query: {:#?}, 
            values: {:?}, 
            sqlite: {:?}, 
            db: {}",
            sql, self.values, sqlite, db_path
        );
    }

    fn exec_sql_limbo(
        &self,
        db: TempDatabase,
        limbo_conn: std::rc::Rc<limbo_core::Connection>,
        db_path: &Option<PathBuf>,
    ) {
        let db_path = db_path.as_ref().map(|db_path| db_path.to_string_lossy());
        let db_path = db_path.unwrap_or(std::borrow::Cow::Borrowed("memory"));

        let limbo = match &self.mode {
            TestMode::Single { sql } => limbo_exec_rows(&db, &limbo_conn, sql),
            TestMode::Many { sql_queries } => {
                let mut limbo_values = Vec::with_capacity(sql_queries.len());
                for sql in sql_queries.iter() {
                    let limbo = limbo_exec_rows(&db, &limbo_conn, &sql);
                    limbo_values.extend(limbo);
                }
                limbo_values
            }
            TestMode::MemoryError { sql_queries } => {
                let contains_error = sql_queries
                    .iter()
                    .map(|sql| limbo_exec_rows_error(&db, &limbo_conn, &sql))
                    .any(|res| res.is_err());
                assert!(contains_error, "no error thrown in Limbo");
                return;
            }
            TestMode::Regex { sql, regex } => {
                let limbo = limbo_exec_rows(&db, &limbo_conn, sql);
                for values in limbo.iter() {
                    for val in values {
                        match val {
                            Value::Text(s) => {
                                assert!(
                                    regex.is_match(s),
                                    "regex `{}` did not match in {}
                                    query: {:#?}, 
                                    values: {:?}, 
                                    limbo: {:?}, 
                                    db: {}",
                                    regex,
                                    s,
                                    sql,
                                    values,
                                    limbo,
                                    db_path,
                                );
                            }
                            _ => panic!("only expected Text Value for regex test"),
                        }
                    }
                }
                return;
            }
        };

        let sql = match &self.mode {
            TestMode::Single { sql } => sql as &dyn std::fmt::Debug,
            TestMode::Many { sql_queries } => sql_queries as &dyn std::fmt::Debug,
            _ => unreachable!(),
        };

        assert_eq!(
            self.values, limbo,
            "query: {:#?}, 
            values: {:?}, 
            limbo: {:?}, 
            db: {}",
            sql, self.values, limbo, db_path
        );
    }
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

/// Exec sqlite and expect errors
pub(crate) fn sqlite_exec_rows_error(
    conn: &rusqlite::Connection,
    query: &str,
) -> anyhow::Result<()> {
    let mut stmt = conn.prepare(&query)?;
    let mut rows = stmt.query(params![])?;
    while let Some(row) = rows.next()? {
        for i in 0.. {
            let _: rusqlite::types::Value = row.get(i)?;
        }
    }
    Ok(())
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
                limbo_core::OwnedValue::Null => rusqlite::types::Value::Null,
                limbo_core::OwnedValue::Integer(x) => rusqlite::types::Value::Integer(*x),
                limbo_core::OwnedValue::Float(x) => rusqlite::types::Value::Real(*x),
                limbo_core::OwnedValue::Text(x) => {
                    rusqlite::types::Value::Text(x.as_str().to_string())
                }
                limbo_core::OwnedValue::Blob(x) => rusqlite::types::Value::Blob(x.to_vec()),
            })
            .collect();
        rows.push(row);
    }
    rows
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
        let db = TempDatabase::new_existent(&path);
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

            let conn_2 = db.connect_limbo_with_flags(
                limbo_core::OpenFlags::default() | limbo_core::OpenFlags::ReadOnly,
            );
            let ret = limbo_exec_rows(&db, &conn_2, "SELECT * from t");
            assert_eq!(ret, vec![vec![Value::Integer(1)]]);
        }
        Ok(())
    }
}
