use rand::{rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rusqlite::params;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use turso_core::{Connection, Database, FromValueRow, Row, IO};

pub struct TempDatabase {
    pub path: PathBuf,
    pub io: Arc<dyn IO + Send>,
    pub db: Arc<Database>,
    pub db_opts: turso_core::DatabaseOpts,
    #[allow(dead_code)]
    pub db_flags: turso_core::OpenFlags,
    #[allow(dead_code)]
    pub init_sql: Option<String>,
    #[allow(dead_code)]
    pub enable_mvcc: bool,
}
unsafe impl Send for TempDatabase {}

#[derive(Debug, Default, Clone)]
pub struct TempDatabaseBuilder {
    db_name: Option<String>,
    db_path: Option<PathBuf>,
    opts: Option<turso_core::DatabaseOpts>,
    flags: Option<turso_core::OpenFlags>,
    init_sql: Option<String>,
    enable_mvcc: bool,
}

impl TempDatabaseBuilder {
    pub const fn new() -> Self {
        Self {
            db_name: None,
            db_path: None,
            opts: None,
            flags: None,
            init_sql: None,
            enable_mvcc: false,
        }
    }

    /// Db Name is mutually exclusive with Db Path
    pub fn with_db_name(mut self, db_name: impl AsRef<str>) -> Self {
        assert!(
            self.db_path.is_none(),
            "DB Name and DB Path are mutually exclusive options"
        );

        self.db_name = Some(db_name.as_ref().to_string());
        self.db_path = None;
        self
    }

    /// Db Path is mutually exclusive with Db Name
    pub fn with_db_path(mut self, db_path: impl AsRef<Path>) -> Self {
        assert!(
            self.db_name.is_none(),
            "DB Name and DB Path are mutually exclusive options"
        );
        self.db_path = Some(db_path.as_ref().to_path_buf());
        self
    }

    pub fn with_opts(mut self, opts: turso_core::DatabaseOpts) -> Self {
        self.opts = Some(opts);
        self
    }

    pub fn with_flags(mut self, flags: turso_core::OpenFlags) -> Self {
        self.flags = Some(flags);
        self
    }

    pub fn with_init_sql(mut self, init_sql: impl AsRef<str>) -> Self {
        self.init_sql = Some(init_sql.as_ref().to_string());
        self
    }

    pub fn with_mvcc(mut self, enable: bool) -> Self {
        self.enable_mvcc = enable;
        self
    }

    pub fn build(self) -> TempDatabase {
        let opts = self
            .opts
            .unwrap_or_else(|| turso_core::DatabaseOpts::new().with_encryption(true));

        let flags = self.flags.unwrap_or_default();

        let db_path = match self.db_path {
            Some(db_path) => db_path,
            None => {
                let db_name = self
                    .db_name
                    .unwrap_or_else(|| format!("test-{}.db", rng().next_u32()));
                let mut db_path = TempDir::new().unwrap().keep();
                db_path.push(db_name);
                db_path
            }
        };

        if let Some(init_sql) = &self.init_sql {
            let connection = rusqlite::Connection::open(&db_path).unwrap();
            connection
                .pragma_update(None, "journal_mode", "wal")
                .unwrap();
            connection.execute(init_sql, ()).unwrap();
        }

        let io = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            flags,
            opts,
            None,
        )
        .unwrap();

        // Enable MVCC via turso connection if requested
        if self.enable_mvcc {
            let conn = db.connect().unwrap();
            conn.pragma_update("journal_mode", "'experimental_mvcc'")
                .expect("enable mvcc");
        }

        TempDatabase {
            path: db_path,
            io,
            db,
            db_opts: opts,
            db_flags: flags,
            init_sql: self.init_sql,
            enable_mvcc: self.enable_mvcc,
        }
    }
}

#[allow(clippy::arc_with_non_send_sync)]
impl TempDatabase {
    pub const fn builder() -> TempDatabaseBuilder {
        TempDatabaseBuilder::new()
    }

    pub fn new_empty() -> Self {
        Self::builder().build()
    }

    pub fn new(db_name: &str) -> Self {
        Self::builder().with_db_name(db_name).build()
    }

    /// Creates a new database with MVCC mode enabled.
    pub fn new_with_mvcc(db_name: &str) -> Self {
        let db = Self::new(db_name);
        let conn = db.connect_limbo();
        conn.pragma_update("journal_mode", "'experimental_mvcc'")
            .expect("enable mvcc");
        db
    }

    pub fn new_with_existent(db_path: &Path) -> Self {
        Self::builder().with_db_path(db_path).build()
    }

    pub fn new_with_existent_with_opts(db_path: &Path, opts: turso_core::DatabaseOpts) -> Self {
        Self::builder()
            .with_db_path(db_path)
            .with_opts(opts)
            .build()
    }

    pub fn new_with_existent_with_flags(db_path: &Path, flags: turso_core::OpenFlags) -> Self {
        Self::builder()
            .with_db_path(db_path)
            .with_flags(flags)
            .build()
    }

    pub fn new_with_rusqlite(table_sql: &str) -> Self {
        Self::builder().with_init_sql(table_sql).build()
    }

    pub fn connect_limbo(&self) -> Arc<turso_core::Connection> {
        log::debug!("conneting to limbo");

        let conn = self.db.connect().unwrap();
        log::debug!("connected to limbo");
        conn
    }

    pub fn limbo_database(&self) -> Arc<turso_core::Database> {
        log::debug!("conneting to limbo");
        Database::open_file(self.io.clone(), self.path.to_str().unwrap()).unwrap()
    }

    #[allow(dead_code)]
    #[cfg(feature = "test_helper")]
    pub fn get_pending_byte() -> u32 {
        let pending_byte_sqlite = unsafe {
            rusqlite::ffi::sqlite3_test_control(rusqlite::ffi::SQLITE_TESTCTRL_PENDING_BYTE, 0)
        } as u32;
        let pending_byte_turso = { Database::get_pending_byte() };
        assert_eq!(pending_byte_turso, pending_byte_sqlite);
        pending_byte_turso
    }

    #[allow(dead_code)]
    #[cfg(feature = "test_helper")]
    pub fn set_pending_byte(offset: u32) {
        unsafe {
            rusqlite::ffi::sqlite3_test_control(rusqlite::ffi::SQLITE_TESTCTRL_PENDING_BYTE, offset)
        };
        Database::set_pending_byte(offset);
    }

    #[allow(dead_code)]
    #[cfg(feature = "test_helper")]
    pub fn reset_pending_byte() {
        // 1 Gib
        const PENDING_BYTE: u32 = 2u32.pow(30);
        Self::set_pending_byte(PENDING_BYTE);
    }
}

pub fn do_flush(conn: &Arc<Connection>, tmp_db: &TempDatabase) -> anyhow::Result<()> {
    let completions = conn.cacheflush()?;
    for c in completions {
        tmp_db.io.wait_for_completion(c)?;
    }
    Ok(())
}

pub fn compare_string(a: impl AsRef<str>, b: impl AsRef<str>) {
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
                .with_ansi(true)
                .with_line_number(true)
                .with_thread_ids(true),
        )
        .with(EnvFilter::from_default_env())
        .try_init();
}

pub fn sqlite_exec_rows(
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

pub fn limbo_exec_rows(
    conn: &Arc<turso_core::Connection>,
    query: &str,
) -> Vec<Vec<rusqlite::types::Value>> {
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = Vec::new();

    stmt.run_with_row_callback(|row| {
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
        Ok(())
    })
    .unwrap();
    rows
}

/// Like `limbo_exec_rows`, but returns a Result instead of panicking on errors.
/// Useful for fuzz tests that may generate invalid SQL.
#[allow(dead_code)]
pub fn try_limbo_exec_rows(
    _db: &TempDatabase,
    conn: &Arc<turso_core::Connection>,
    query: &str,
) -> Result<Vec<Vec<rusqlite::types::Value>>, turso_core::LimboError> {
    let mut stmt = conn.prepare(query)?;
    let mut rows = Vec::new();

    stmt.run_with_row_callback(|row| {
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
        Ok(())
    })?;

    Ok(rows)
}

#[allow(dead_code)]
pub fn limbo_stmt_get_column_names(
    _db: &TempDatabase,
    conn: &Arc<turso_core::Connection>,
    query: &str,
) -> Vec<String> {
    let stmt = conn.prepare(query).unwrap();

    let mut names = vec![];
    for i in 0..stmt.num_columns() {
        names.push(stmt.get_column_name(i).to_string());
    }
    names
}

pub fn limbo_exec_rows_fallible(
    _db: &TempDatabase,
    conn: &Arc<turso_core::Connection>,
    query: &str,
) -> Result<Vec<Vec<rusqlite::types::Value>>, turso_core::LimboError> {
    let mut stmt = conn.prepare(query)?;
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
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
        Ok(())
    })?;
    Ok(rows)
}

pub fn rng_from_time() -> (ChaCha8Rng, u64) {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let rng = ChaCha8Rng::seed_from_u64(seed);
    (rng, seed)
}

pub fn rng_from_time_or_env() -> (ChaCha8Rng, u64) {
    let seed = std::env::var("SEED").map_or(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        |v| {
            v.parse()
                .expect("Failed to parse SEED environment variable as u64")
        },
    );
    let rng = ChaCha8Rng::seed_from_u64(seed as u64);
    (rng, seed as u64)
}

pub fn run_query(tmp_db: &TempDatabase, conn: &Arc<Connection>, query: &str) -> anyhow::Result<()> {
    run_query_core(tmp_db, conn, query, None::<fn(&Row)>)
}

pub fn run_query_on_row(
    tmp_db: &TempDatabase,
    conn: &Arc<Connection>,
    query: &str,
    on_row: impl FnMut(&Row),
) -> anyhow::Result<()> {
    run_query_core(tmp_db, conn, query, Some(on_row))
}

pub fn run_query_core(
    _tmp_db: &TempDatabase,
    conn: &Arc<Connection>,
    query: &str,
    mut on_row: Option<impl FnMut(&Row)>,
) -> anyhow::Result<()> {
    if let Some(ref mut rows) = conn.query(query)? {
        #[allow(clippy::type_complexity)]
        let handler: Box<dyn FnMut(&Row) -> turso_core::Result<()>> =
            if let Some(on_row) = on_row.as_mut() {
                Box::new(|row| {
                    on_row(row);
                    Ok(())
                })
            } else {
                Box::new(|_| Ok(()))
            };
        rows.run_with_row_callback(handler)?;
    };
    Ok(())
}

pub fn rusqlite_integrity_check(db_path: &Path) -> anyhow::Result<()> {
    let conn = rusqlite::Connection::open(db_path)?;
    let mut stmt = conn.prepare("SELECT * FROM pragma_integrity_check;")?;
    let mut rows = stmt.query(())?;
    let mut result: Vec<String> = Vec::new();

    while let Some(row) = rows.next()? {
        result.push(row.get(0)?);
    }
    if result.is_empty() {
        anyhow::bail!("integrity_check should return `ok` or a list of problems")
    }
    if !result[0].eq_ignore_ascii_case("ok") {
        // Build a list of problems
        result.iter_mut().for_each(|row| *row = format!("- {row}"));
        anyhow::bail!("integrity check returned: {}", result.join("\n"))
    }
    Ok(())
}

pub trait ExecRows<T> {
    #[allow(dead_code)]
    fn exec_rows(&self, query: &str) -> Vec<T>;
}

macro_rules! impl_exec_rows_for_tuple {
    ($($T:ident : $idx:tt),+) => {
        impl<$($T),+> ExecRows<($($T,)+)> for Arc<Connection>
        where
            $($T: for<'a> FromValueRow<'a> + 'static,)+
        {
            fn exec_rows(&self, query: &str) -> Vec<($($T,)+)> {
                let mut stmt = self.prepare(query).unwrap();
                let mut rows = Vec::new();
                stmt.run_with_row_callback(|row| {
                    rows.push(($(row.get($idx).unwrap(),)+));
                    Ok(())
                }).unwrap();
                rows
            }
        }
    };
}

impl_exec_rows_for_tuple!(T0: 0);
impl_exec_rows_for_tuple!(T0: 0, T1: 1);
impl_exec_rows_for_tuple!(T0: 0, T1: 1, T2: 2);
impl_exec_rows_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3);
impl_exec_rows_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3, T4: 4);
impl_exec_rows_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5);
impl_exec_rows_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5, T6: 6);
impl_exec_rows_for_tuple!(T0: 0, T1: 1, T2: 2, T3: 3, T4: 4, T5: 5, T6: 6, T7: 7);

#[cfg(test)]
mod tests {
    use std::{sync::Arc, vec};
    use tempfile::{NamedTempFile, TempDir};
    use turso_core::{Database, IO};

    use crate::common::{do_flush, ExecRows};

    use super::TempDatabase;

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
        let path = TempDir::new().unwrap().keep().join("temp_read_only");
        {
            let db =
                TempDatabase::new_with_existent_with_flags(&path, turso_core::OpenFlags::default());
            let conn = db.connect_limbo();
            conn.execute("CREATE table t (a)").unwrap();
            conn.execute("INSERT INTO t values (1)").unwrap();
            conn.close().unwrap()
        }

        {
            let db = TempDatabase::new_with_existent_with_flags(
                &path,
                turso_core::OpenFlags::default() | turso_core::OpenFlags::ReadOnly,
            );
            let conn = db.connect_limbo();
            let ret: Vec<(i64,)> = conn.exec_rows("SELECT * from t");
            assert_eq!(ret, vec![(1,)]);

            let err = conn.execute("INSERT INTO t values (1)").unwrap_err();
            assert!(matches!(err, turso_core::LimboError::ReadOnly), "{err:?}");
        }
        Ok(())
    }

    #[test]
    fn test_unique_index_ordering() -> anyhow::Result<()> {
        use rand::Rng;

        let db = TempDatabase::new_empty();
        let conn = db.connect_limbo();

        conn.execute("CREATE TABLE t (x INTEGER UNIQUE)").unwrap();

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
            conn.execute(format!("INSERT INTO t VALUES ({val})"))
                .unwrap();
        }

        // Sort expected values to match index order
        expected.sort();

        // Query all values and verify they come back in sorted order
        let ret: Vec<(i64,)> = conn.exec_rows("SELECT x FROM t");
        let actual: Vec<i64> = ret.into_iter().map(|row| row.0).collect();

        assert_eq!(actual, expected, "Values not returned in sorted order");

        Ok(())
    }

    #[test]
    fn test_large_unique_blobs() -> anyhow::Result<()> {
        let path = TempDir::new().unwrap().keep().join("temp_read_only");
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        conn.execute("CREATE TABLE t (x BLOB UNIQUE)").unwrap();

        // Insert 11 unique 1MB blobs
        for i in 0..11 {
            println!("Inserting blob #{i}");
            conn.execute("INSERT INTO t VALUES (randomblob(1024*1024))")
                .unwrap()
        }

        // Verify we have 11 rows
        let ret: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM t");
        assert_eq!(ret, vec![(11,)], "Expected 11 rows but got {ret:?}",);

        Ok(())
    }

    #[test]
    /// Test that a transaction cannot read uncommitted changes of another transaction (no: READ UNCOMMITTED)
    fn test_tx_isolation_no_dirty_reads() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_transaction_isolation");
        let db = TempDatabase::new_with_existent(&path);

        // Create two separate connections
        let conn1 = db.connect_limbo();

        // Create test table
        conn1.execute("CREATE TABLE t (x INTEGER)").unwrap();

        // Begin transaction on first connection and insert a value
        conn1.execute("BEGIN").unwrap();
        conn1.execute("INSERT INTO t VALUES (42)").unwrap();
        do_flush(&conn1, &db)?;

        // Second connection should not see uncommitted changes
        let conn2 = db.connect_limbo();
        let ret: Vec<(i64,)> = conn2.exec_rows("SELECT x FROM t");
        assert!(
            ret.is_empty(),
            "DIRTY READ: Second connection saw uncommitted changes: {ret:?}"
        );

        Ok(())
    }

    #[test]
    /// Test that a transaction cannot read committed changes that were committed after the transaction started (no: READ COMMITTED)
    fn test_tx_isolation_no_read_committed() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_transaction_isolation");
        let db = TempDatabase::new_with_existent(&path);

        // Create two separate connections
        let conn1 = db.connect_limbo();

        // Create test table
        conn1.execute("CREATE TABLE t (x INTEGER)").unwrap();

        // Begin transaction on first connection
        conn1.execute("BEGIN").unwrap();
        let ret: Vec<(i64,)> = conn1.exec_rows("SELECT x FROM t");
        assert!(ret.is_empty(), "Expected 0 rows but got {ret:?}");

        // Commit a value from the second connection
        let conn2 = db.connect_limbo();
        conn2.execute("BEGIN").unwrap();
        conn2.execute("INSERT INTO t VALUES (42)").unwrap();
        conn2.execute("COMMIT").unwrap();

        // First connection should not see the committed value
        let ret: Vec<(i64,)> = conn1.exec_rows("SELECT x FROM t");
        assert!(
            ret.is_empty(),
            "SNAPSHOT ISOLATION VIOLATION: Older txn saw committed changes from newer txn: {ret:?}"
        );

        Ok(())
    }

    #[test]
    /// Test that a txn can write a row, flush to WAL without committing, then rollback, and finally commit a second row.
    /// Reopening database should show only the second row.
    fn test_tx_isolation_cacheflush_rollback_commit() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_transaction_isolation");
        let db = TempDatabase::new_with_existent(&path);

        let conn = db.connect_limbo();

        // Create test table
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();

        // Begin transaction on first connection and insert a value
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES (42)").unwrap();
        do_flush(&conn, &db)?;

        // Rollback the transaction
        conn.execute("ROLLBACK").unwrap();

        // Now actually commit a row
        conn.execute("INSERT INTO t VALUES (69)").unwrap();

        // Reopen the database
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        // Should only see the last committed value
        let ret: Vec<(i64,)> = conn.exec_rows("SELECT x FROM t");
        assert_eq!(ret, vec![(69,)], "Expected 1 row but got {ret:?}");

        Ok(())
    }

    #[test]
    /// Test that a txn can write a row and flush to WAL without committing, then reopen DB and not see the row
    fn test_tx_isolation_cacheflush_reopen() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_transaction_isolation");
        let db = TempDatabase::new_with_existent(&path);

        let conn = db.connect_limbo();

        // Create test table
        conn.execute("CREATE TABLE t (x INTEGER)").unwrap();

        // Begin transaction and insert a value
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t VALUES (42)").unwrap();

        // Flush to WAL but don't commit
        do_flush(&conn, &db)?;

        // Reopen the database without committing
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        // Should see no rows since transaction was never committed
        let ret: Vec<(i64,)> = conn.exec_rows("SELECT x FROM t");
        assert!(ret.is_empty(), "Expected 0 rows but got {ret:?}");

        Ok(())
    }

    #[test]
    fn test_multi_connection_table_drop_persistence() -> Result<(), Box<dyn std::error::Error>> {
        // Create a temporary database file
        let temp_file = NamedTempFile::new()?;
        let db_path = temp_file.path().to_string_lossy().to_string();

        // Open database
        #[allow(clippy::arc_with_non_send_sync)]
        let io: Arc<dyn IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let db = Database::open_file(io, &db_path)?;

        const NUM_CONNECTIONS: usize = 5;
        let mut connections = Vec::new();

        // Create a new connection to verify persistence
        let verification_conn = db.connect()?;
        // Create multiple connections and create a table from each

        for i in 0..NUM_CONNECTIONS {
            let conn = db.connect()?;
            connections.push(conn);

            // Create a unique table name for this connection
            let table_name = format!("test_table_{i}");
            let create_sql = format!(
                "CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)"
            );

            // Execute CREATE TABLE
            verification_conn.execute(&create_sql)?;
        }

        for (i, conn) in connections.iter().enumerate().take(NUM_CONNECTIONS) {
            // Create a unique table name for this connection
            let table_name = format!("test_table_{i}");
            let create_sql = format!("DROP TABLE {table_name}");

            // Execute DROP TABLE
            conn.execute(&create_sql)?;
        }

        // Also verify via sqlite_schema table that all tables are present
        let stmt = verification_conn.query("SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'test_table_%' ORDER BY name")?;

        assert!(stmt.is_some(), "Should be able to query sqlite_schema");
        let mut stmt = stmt.unwrap();

        let mut found_tables = Vec::new();
        stmt.run_with_row_callback(|row| {
            let table_name = row.get::<String>(0)?;
            found_tables.push(table_name);
            Ok(())
        })?;

        // Verify we found all expected tables
        assert_eq!(found_tables.len(), 0, "Should find no tables in schema");

        Ok(())
    }

    #[turso_macros::test]
    /// https://github.com/tursodatabase/turso/issues/4146
    fn test_bit_out_of_bounds_minimal(tmp_db: TempDatabase) {
        let conn = tmp_db.connect_limbo();

        conn.execute("CREATE TABLE shimmering_l_361 (funny_aldred_362 TEXT, amiable_fides_363 INTEGER, stellar_ronan_364 REAL, agreeable_fdca_365 BLOB, remarkable_squat_366 REAL, dynamic_hoyt_367 INTEGER, educated_vega_368 TEXT, blithesome_turgenev_369 REAL, plucky_sheppard_370 TEXT, knowledgeable_bacca_371 INTEGER, hilarious_urcuchillay_372 INTEGER, amiable_bluestein_373 BLOB, perfect_leval_374 REAL, outstanding_yarros_375 REAL, philosophical_montgomery_376 REAL, plucky_french_377 REAL, engrossing_joyce_378 BLOB, persistent_maxwell_379 REAL, proficient_balaji_380 TEXT, ample_igualada_381 BLOB, agreeable_maryamdeluz_382 TEXT, glittering_hakiel_383 REAL, generous_odin_384 REAL, wondrous_paasen_385 INTEGER, determined_dawley_386 BLOB, spectacular_borders_387 REAL, giving_hapgood_388 REAL, proficient_murtaugh_389 REAL, lovely_kinna_390 BLOB, captivating_seymour_391 REAL, proficient_hoyt_392 INTEGER, glimmering_leighton_393 BLOB, optimistic_noche_394 REAL, open_minded_tcherkesoff_395 BLOB, patient_sills_396 BLOB, shining_gerson_397 REAL, romantic_ling_398 REAL, imaginative_barrio_399 REAL, powerful_suekama_400 INTEGER, gorgeous_perkins_401 REAL, propitious_driscoll_402 REAL, approachable_zhihui_403 BLOB, ample_chanial_404 REAL, excellent_burgos_405 TEXT, nice_calabrese_406 TEXT, glistening_res_407 TEXT, mirthful_greenrevolutionary_408 BLOB, magnificent_khola_409 REAL, spellbinding_pouget_410 INTEGER, patient_cascade_411 INTEGER, passionate_again_412 BLOB, productive_teacher_413 INTEGER, rousing_woodbine_414 TEXT, stunning_baverel_415 TEXT, sincere_tompsett_416 TEXT, bountiful_avrich_417 INTEGER, nice_gouldhawke_418 INTEGER, perfect_greenhead_419 BLOB, willing_brown_420 REAL, determined_g_421 TEXT, fortuitous_walia_422 REAL, zestful_fruge_423 TEXT, lovely_thorn_424 BLOB, glittering_rebelnet_425 INTEGER, vibrant_karamustafa_426 REAL, optimistic_jacquier_427 TEXT, qualified_lowens_428 BLOB, splendid_muller_429 INTEGER, honest_levy_430 REAL, warmhearted_gordon_431 INTEGER, gorgeous_jacquier_432 BLOB, technological_ludens_433 BLOB, imaginative_thropy_434 REAL, flexible_cairns_435 REAL, remarkable_mcclelland_436 TEXT, remarkable_brian_437 INTEGER, honest_kanavalchyk_438 REAL, passionate_qruz_439 REAL, sleek_monaghan_440 REAL, adaptable_ray_441 TEXT, mirthful_castoriadis_442 REAL, unique_tonak_443 INTEGER, gregarious_shantz_444 BLOB, excellent_lesoleil_445 REAL, marvelous_roca_446 BLOB, glistening_dent_447 INTEGER, splendid_abra_448 INTEGER, fearless_jasiewicz_449 BLOB, imaginative_gardell_450 REAL, persistent_dockes_451 BLOB, imaginative_a_452 INTEGER, kind_konok_453 REAL, awesome_orsetti_454 REAL, zestful_escalante_455 INTEGER, knowledgeable_giollamoir_456 INTEGER, elegant_mckernan_457 REAL, knowledgeable_lesoleil_458 TEXT, wondrous_agacino_459 INTEGER, glowing_mob_460 REAL, lustrous_obrien_461 REAL, bountiful_tzu_462 REAL, sincere_pointblank_463 TEXT, imaginative_wright_464 BLOB, productive_kumper_465 BLOB, organized_hs_466 BLOB, moving_omowali_467 TEXT, relaxed_mason_468 INTEGER);").unwrap();

        conn.execute("CREATE TABLE sensible_samudzi_342 (outstanding_dubovik_343 TEXT, captivating_comeau_344 TEXT, qualified_casteu_345 INTEGER, thoughtful_bee_346 REAL, spellbinding_budiati_347 REAL, thoughtful_bryant_348 REAL, hardworking_mother_349 TEXT, captivating_bulgaria_350 INTEGER);").unwrap();
        conn.execute("CREATE TABLE plucky_maximilienne_680 (creative_again_681 INTEGER);")
            .unwrap();
        conn.execute("CREATE TABLE super_vernet_712 (bountiful_cairns_713 REAL, super_correspondents_714 BLOB, fabulous_janeiro_715 BLOB, responsible_shilton_716 BLOB, loving_seaweed_717 TEXT);").unwrap();

        conn.execute("CREATE INDEX idx_shimmering_l_361_frank_st ON shimmering_l_361 (patient_cascade_411 DESC, zestful_fruge_423 ASC, ample_chanial_404 ASC, proficient_balaji_380 DESC, flexible_cairns_435 ASC, productive_kumper_465 ASC, gregarious_shantz_444 ASC, mirthful_castoriadis_442 ASC, gorgeous_perkins_401 ASC, captivating_seymour_391 ASC, warmhearted_gordon_431 DESC, imaginative_thropy_434 DESC, agreeable_fdca_365 DESC, lovely_kinna_390 DESC, imaginative_gardell_450 ASC, persistent_dockes_451 DESC, blithesome_turgenev_369 ASC, outstanding_yarros_375 ASC, willing_brown_420 DESC, patient_sills_396 ASC, sincere_pointblank_463 ASC, sincere_tompsett_416 DESC, fearless_jasiewicz_449 ASC, relaxed_mason_468 ASC, bountiful_avrich_417 DESC, lovely_thorn_424 ASC, remarkable_brian_437 DESC, vibrant_karamustafa_426 DESC, moving_omowali_467 DESC, imaginative_barrio_399 ASC, nice_calabrese_406 DESC, agreeable_maryamdeluz_382 ASC, stellar_ronan_364 DESC, propitious_driscoll_402 DESC, lustrous_obrien_461 ASC, plucky_french_377 DESC, amiable_bluestein_373 ASC, excellent_burgos_405 DESC, splendid_abra_448 ASC, excellent_lesoleil_445 DESC, perfect_greenhead_419 ASC, perfect_leval_374 ASC, glistening_dent_447 ASC, glittering_rebelnet_425 DESC, knowledgeable_bacca_371 DESC, productive_teacher_413 DESC, honest_kanavalchyk_438 ASC, optimistic_noche_394 DESC, magnificent_khola_409 DESC, honest_levy_430 ASC, remarkable_mcclelland_436 DESC, proficient_murtaugh_389 DESC, hilarious_urcuchillay_372 ASC, elegant_mckernan_457 ASC, philosophical_montgomery_376 ASC, plucky_sheppard_370 DESC, fortuitous_walia_422 DESC, nice_gouldhawke_418 ASC, funny_aldred_362 ASC, engrossing_joyce_378 ASC, knowledgeable_giollamoir_456 DESC, powerful_suekama_400 ASC, optimistic_jacquier_427 DESC, sleek_monaghan_440 DESC, spellbinding_pouget_410 ASC, stunning_baverel_415 ASC, imaginative_a_452 DESC, ample_igualada_381 ASC, approachable_zhihui_403 ASC, passionate_qruz_439 ASC, qualified_lowens_428 ASC, shining_gerson_397 ASC, amiable_fides_363 ASC, generous_odin_384 DESC, knowledgeable_lesoleil_458 ASC, giving_hapgood_388 ASC, splendid_muller_429 DESC, gorgeous_jacquier_432 DESC, bountiful_tzu_462 ASC);").unwrap();

        conn.execute("INSERT INTO plucky_maximilienne_680 VALUES (1);")
            .unwrap();

        conn.execute("CREATE TRIGGER trigger_plucky_maximilienne_680_1169180867 BEFORE UPDATE ON plucky_maximilienne_680 BEGIN UPDATE shimmering_l_361 SET determined_g_421 = 'diligent_marmol', lovely_thorn_424 = X'6361707469766174696E675F7072616461', adaptable_ray_441 = 'energetic_tee', rousing_woodbine_414 = 'stupendous_gethin', perfect_greenhead_419 = X'6272696C6C69616E745F6461727474', ample_igualada_381 = X'7368696E696E675F6E616F756D6F76', knowledgeable_bacca_371 = 8795766455619870255, productive_kumper_465 = X'666162756C6F75735F6261636B', spellbinding_pouget_410 = -2080320213985020508, mirthful_castoriadis_442 = -5622911538.309956, open_minded_tcherkesoff_395 = X'70657273697374656E745F6B657272', imaginative_wright_464 = X'70726F647563746976655F7061736F', amiable_bluestein_373 = X'617765736F6D655F626F7A6F6B69', zestful_escalante_455 = -3488495773897908929, proficient_hoyt_392 = 2109777389586581121, gregarious_shantz_444 = X'676F7267656F75735F68616E636F78', knowledgeable_giollamoir_456 = 1807255432535784487, fortuitous_walia_422 = 5716860416.539839, giving_hapgood_388 = -7945368599.58225, sincere_pointblank_463 = 'blithesome_moon', excellent_burgos_405 = 'fantastic_grey', glistening_dent_447 = -8720078206077868004, excellent_lesoleil_445 = -8308316719.976472, imaginative_barrio_399 = 2586704785.5247574, gorgeous_jacquier_432 = X'696E6372656469626C655F657272616E646F6E6561', nice_gouldhawke_418 = -9218489973071029860, stunning_baverel_415 = 'elegant_macsimoin', gorgeous_perkins_401 = 1858284188.5782166, patient_cascade_411 = 5434496617634431287, glimmering_leighton_393 = X'70617373696F6E6174655F636F6F7264696E61646F73', blithesome_turgenev_369 = 539105039.8303547, lovely_kinna_390 = X'67656E65726F75735F706F696E74626C616E6B', propitious_driscoll_402 = 6110709419.661383 WHERE (shimmering_l_361.flexible_cairns_435 != -2665602268.6225224); UPDATE super_vernet_712 SET responsible_shilton_716 = X'6D617276656C6F75735F6172636865676F6E6F73' WHERE (TRUE); INSERT INTO sensible_samudzi_342 VALUES ('frank_olympics', 'splendid_academy', 6786370686344360623, -8674635739.474007, -4807591805.499456, 2818384407.6066933, 'insightful_fiorina', -4425841829162377840), ('productive_duch', 'ravishing_asher', 957231539187121006, -9936535798.322428, 3542340933.6666107, 6847954059.14608, 'loving_seminatore', 3269958273313428337); END;").unwrap();

        conn.execute("UPDATE plucky_maximilienne_680 SET creative_again_681 = 2 WHERE creative_again_681 = 1;").unwrap();
    }
}
