use rand::{rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rusqlite::params;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use turso_core::{Clock, Connection, Database, FromValueRow, Row, SqliteDialect, IO};

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
    io_uring: bool,
    enable_views: bool,
}

struct TestIo {
    io: Arc<dyn IO>,
}

impl Clock for TestIo {
    fn current_time_monotonic(&self) -> turso_core::MonotonicInstant {
        self.io.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> turso_core::WallClockInstant {
        self.io.current_time_wall_clock()
    }
}

impl IO for TestIo {
    // we don't sleep in test io in order to make tests faster
    fn sleep(&self, _duration: std::time::Duration) {}

    fn open_file(
        &self,
        path: &str,
        flags: turso_core::OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn turso_core::File>> {
        self.io.open_file(path, flags, direct)
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        self.io.remove_file(path)
    }
    fn file_id(&self, path: &str) -> turso_core::Result<turso_core::io::FileId> {
        self.io.file_id(path)
    }
    fn cancel(&self, c: &[turso_core::Completion]) -> turso_core::Result<()> {
        self.io.cancel(c)
    }
    fn drain_completions(&self, completions: &[turso_core::Completion]) -> turso_core::Result<()> {
        self.io.drain_completions(completions)
    }
    fn fill_bytes(&self, dest: &mut [u8]) {
        self.io.fill_bytes(dest);
    }
    fn generate_random_number(&self) -> i64 {
        self.io.generate_random_number()
    }
    fn get_memory_io(&self) -> Arc<turso_core::MemoryIO> {
        self.io.get_memory_io()
    }
    fn register_fixed_buffer(
        &self,
        ptr: std::ptr::NonNull<u8>,
        len: usize,
    ) -> turso_core::Result<u32> {
        self.io.register_fixed_buffer(ptr, len)
    }
    fn step(&self) -> turso_core::Result<()> {
        self.io.step()
    }
    fn wait_for_completion(&self, c: turso_core::Completion) -> turso_core::Result<()> {
        self.io.wait_for_completion(c)
    }
    fn yield_now(&self) {
        self.io.yield_now();
    }
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
            io_uring: false,
            enable_views: false,
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

    #[cfg(target_os = "linux")]
    pub fn with_io_uring(mut self, enable: bool) -> Self {
        self.io_uring = enable;
        self
    }

    pub fn with_views(mut self, enable: bool) -> Self {
        self.enable_views = enable;
        self
    }

    pub fn build(self) -> TempDatabase {
        let mut opts = self
            .opts
            .unwrap_or_else(|| turso_core::DatabaseOpts::new().with_encryption(true));
        opts = opts.with_vacuum(true).with_without_rowid(true);

        if self.enable_views {
            opts = opts.with_views(true);
        }

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

        let io = if !self.io_uring {
            Arc::new(TestIo {
                io: Arc::new(turso_core::PlatformIO::new().unwrap()),
            })
        } else {
            #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
            {
                panic!("io_uring feature must be enable for testing with UringIO")
            }
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            {
                Arc::new(TestIo {
                    io: Arc::new(turso_core::UringIO::new().unwrap()),
                })
            }
        };
        let db = Database::open_file_with_flags(
            io.clone(),
            db_path.to_str().unwrap(),
            flags,
            opts,
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();

        // Enable MVCC via turso connection if requested
        if self.enable_mvcc {
            let conn = db.connect().unwrap();
            conn.pragma_update("journal_mode", "'mvcc'")
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
        conn.pragma_update("journal_mode", "'mvcc'")
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
        Database::open_file(
            self.io.clone(),
            self.path.to_str().unwrap(),
            Arc::new(SqliteDialect),
        )
        .unwrap()
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
                turso_core::Value::Numeric(turso_core::Numeric::Integer(x)) => {
                    rusqlite::types::Value::Integer(*x)
                }
                turso_core::Value::Numeric(turso_core::Numeric::Float(x)) => {
                    rusqlite::types::Value::Real(f64::from(*x))
                }
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
                turso_core::Value::Numeric(turso_core::Numeric::Integer(x)) => {
                    rusqlite::types::Value::Integer(*x)
                }
                turso_core::Value::Numeric(turso_core::Numeric::Float(x)) => {
                    rusqlite::types::Value::Real(f64::from(*x))
                }
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
                turso_core::Value::Numeric(turso_core::Numeric::Integer(x)) => {
                    rusqlite::types::Value::Integer(*x)
                }
                turso_core::Value::Numeric(turso_core::Numeric::Float(x)) => {
                    rusqlite::types::Value::Real(f64::from(*x))
                }
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

/// Compute dbhash of the test database.
pub fn compute_dbhash(tmp_db: &TempDatabase) -> turso_dbhash::DbHashResult {
    let path = tmp_db.path.to_str().unwrap();
    turso_dbhash::hash_database(path, &turso_dbhash::DbHashOptions::default())
        .expect("dbhash failed")
}

/// Compute dbhash with custom options.
#[allow(dead_code)]
pub fn compute_dbhash_with_options(
    tmp_db: &TempDatabase,
    options: &turso_dbhash::DbHashOptions,
) -> turso_dbhash::DbHashResult {
    let path = tmp_db.path.to_str().unwrap();
    turso_dbhash::hash_database(path, options).expect("dbhash failed")
}

/// Compute dbhash while opening the database with explicit feature flags.
pub fn compute_dbhash_with_database_opts(
    tmp_db: &TempDatabase,
    database_opts: turso_core::DatabaseOpts,
) -> turso_dbhash::DbHashResult {
    compute_dbhash_with_options_and_database_opts(
        tmp_db,
        &turso_dbhash::DbHashOptions::default(),
        database_opts,
    )
}

/// Compute dbhash with custom hash options and explicit database feature flags.
pub fn compute_dbhash_with_options_and_database_opts(
    tmp_db: &TempDatabase,
    options: &turso_dbhash::DbHashOptions,
    database_opts: turso_core::DatabaseOpts,
) -> turso_dbhash::DbHashResult {
    let path = tmp_db.path.to_str().unwrap();
    turso_dbhash::hash_database_with_database_opts(path, options, database_opts)
        .expect("dbhash failed")
}

/// Assert that checkpoint does not change database content.
/// Computes hash before and after checkpoint, asserts they match.
pub fn assert_checkpoint_preserves_content(conn: &Arc<Connection>, tmp_db: &TempDatabase) {
    do_flush(conn, tmp_db).unwrap();
    let hash_before = compute_dbhash(tmp_db);

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    do_flush(conn, tmp_db).unwrap();
    let hash_after = compute_dbhash(tmp_db);

    assert_eq!(
        hash_before.hash, hash_after.hash,
        "Checkpoint changed database content! before={}, after={}",
        hash_before.hash, hash_after.hash
    );
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
    use turso_core::SqliteDialect;
    use turso_core::{Database, StepResult, IO};

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
        const RANGE_MIN: i64 = -1000;
        const RANGE_MAX: i64 = 1000;
        while i < 100 {
            let val = rng.random_range(RANGE_MIN..RANGE_MAX);
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
        let ret: Vec<(i64,)> = conn.exec_rows(&format!("SELECT x FROM t WHERE x >= {RANGE_MIN}"));
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
        let db = Database::open_file(io, &db_path, Arc::new(SqliteDialect))?;

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
        loop {
            match stmt.step()? {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    let table_name = row.get::<String>(0)?;
                    found_tables.push(table_name);
                }
                StepResult::Done => break,
                StepResult::IO => {
                    stmt.get_pager().io.step()?;
                }
                _ => {}
            }
        }

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

    #[test]
    fn test_pragma_i_am_a_dummy() -> anyhow::Result<()> {
        let _ = env_logger::try_init();
        let tmp_db =
            TempDatabase::new_with_rusqlite("CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);");
        let conn = tmp_db.connect_limbo();

        // Off by default — DELETE/UPDATE without WHERE allowed
        conn.execute("INSERT INTO t VALUES (1, 'a')")?;
        conn.execute("DELETE FROM t")?;

        // Enable via i_am_a_dummy
        conn.execute("PRAGMA i_am_a_dummy = ON")?;
        let err = conn.execute("DELETE FROM t").unwrap_err();
        assert!(
            err.to_string().contains("DELETE without a WHERE clause"),
            "{err:?}"
        );
        let err = conn.execute("UPDATE t SET val = 'x'").unwrap_err();
        assert!(
            err.to_string().contains("UPDATE without a WHERE clause"),
            "{err:?}"
        );

        // With WHERE clause still works
        conn.execute("INSERT INTO t VALUES (2, 'b')")?;
        conn.execute("DELETE FROM t WHERE id = 2")?;
        conn.execute("INSERT INTO t VALUES (3, 'c')")?;
        conn.execute("UPDATE t SET val = 'd' WHERE id = 3")?;

        // Dummy WHERE 1=1 bypasses the check (syntactic only)
        conn.execute("DELETE FROM t WHERE 1=1")?;
        conn.execute("UPDATE t SET val = 'e' WHERE 1=1")?;

        // Alias require_where works too
        conn.execute("PRAGMA i_am_a_dummy = OFF")?;
        conn.execute("PRAGMA require_where = ON")?;
        let err = conn.execute("DELETE FROM t").unwrap_err();
        assert!(
            err.to_string().contains("DELETE without a WHERE clause"),
            "{err:?}"
        );

        Ok(())
    }

    /// Regression test: DROP SEQUENCE previously used ParseSchema with
    /// where_clause=None (full rescan) which re-encountered existing
    /// tables via handle_schema_row → add_btree_table → "already exists"
    /// error. That error leaked auto_commit=false, causing subsequent
    /// writes to be silently uncommitted. Fixed by removing the invalid
    /// ParseSchema call and relying on the schema cookie mechanism
    /// (same pattern as DROP TABLE).
    #[test]
    fn test_drop_sequence_with_existing_tables() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_drop_seq_with_tables");
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        // Create a table and a sequence — the table's presence in
        // sqlite_schema was what triggered the old bug.
        conn.execute("CREATE TABLE t(x)")?;
        conn.execute("CREATE SEQUENCE s1")?;

        // DROP SEQUENCE must succeed even with other tables present
        conn.execute("DROP SEQUENCE s1")?;

        // Writes after DROP SEQUENCE must be committed and visible
        conn.execute("INSERT INTO t VALUES (42)")?;
        let conn2 = db.connect_limbo();
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT x FROM t");
        assert_eq!(rows, vec![(42,)]);

        // The sequence must be gone — conn2 sees the updated schema
        let err = conn2.execute("SELECT nextval('s1')");
        assert!(err.is_err());

        Ok(())
    }

    #[test]
    fn test_currval_per_connection_isolation() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_currval_isolation");
        let db = TempDatabase::new_with_existent(&path);
        let conn1 = db.connect_limbo();
        conn1.execute("CREATE SEQUENCE iso_seq")?;

        // conn1 calls nextval → 1, currval → 1
        let rows: Vec<(i64,)> = conn1.exec_rows("SELECT nextval('iso_seq')");
        assert_eq!(rows, vec![(1,)]);
        let rows: Vec<(i64,)> = conn1.exec_rows("SELECT currval('iso_seq')");
        assert_eq!(rows, vec![(1,)]);

        // conn2 calls nextval → 2 (advances global counter)
        let conn2 = db.connect_limbo();
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT nextval('iso_seq')");
        assert_eq!(rows, vec![(2,)]);

        // conn1 currval must still be 1 — unaffected by conn2
        let rows: Vec<(i64,)> = conn1.exec_rows("SELECT currval('iso_seq')");
        assert_eq!(rows, vec![(1,)]);

        // conn1 calls nextval → 3, currval updates to 3
        let rows: Vec<(i64,)> = conn1.exec_rows("SELECT nextval('iso_seq')");
        assert_eq!(rows, vec![(3,)]);
        let rows: Vec<(i64,)> = conn1.exec_rows("SELECT currval('iso_seq')");
        assert_eq!(rows, vec![(3,)]);

        // conn2 currval is still 2
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT currval('iso_seq')");
        assert_eq!(rows, vec![(2,)]);

        // currval without prior nextval on a fresh connection must error
        let conn3 = db.connect_limbo();
        let err = conn3.execute("SELECT currval('iso_seq')");
        assert!(err.is_err());

        Ok(())
    }

    /// Sequences persist across database close/reopen and all functions
    /// (nextval, currval, setval) work correctly on the reopened database.
    #[test]
    fn test_sequence_file_persistence() -> anyhow::Result<()> {
        let path = TempDir::new().unwrap().keep().join("temp_seq_persist");

        // Phase 1: create sequences and advance them
        {
            let db = TempDatabase::new_with_existent(&path);
            let conn = db.connect_limbo();
            conn.execute("CREATE SEQUENCE test_seq START WITH 1 INCREMENT BY 1")?;
            conn.execute("CREATE SEQUENCE counter START WITH 100 INCREMENT BY 10")?;

            // Advance test_seq to 3
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('test_seq')");
            assert_eq!(rows, vec![(1,)]);
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('test_seq')");
            assert_eq!(rows, vec![(2,)]);
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('test_seq')");
            assert_eq!(rows, vec![(3,)]);

            // Advance counter to 110
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('counter')");
            assert_eq!(rows, vec![(100,)]);
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('counter')");
            assert_eq!(rows, vec![(110,)]);

            conn.close()?;
        }

        // Phase 2: reopen and verify sequences continue from persisted state
        {
            let db = TempDatabase::new_with_existent(&path);
            let conn = db.connect_limbo();

            // test_seq should continue from 3 → next is 4
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('test_seq')");
            assert_eq!(rows, vec![(4,)]);
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('test_seq')");
            assert_eq!(rows, vec![(5,)]);

            // counter should continue from 110 → next is 120
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('counter')");
            assert_eq!(rows, vec![(120,)]);
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('counter')");
            assert_eq!(rows, vec![(130,)]);

            // currval works after nextval
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT currval('test_seq')");
            assert_eq!(rows, vec![(5,)]);
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT currval('counter')");
            assert_eq!(rows, vec![(130,)]);

            // setval works on reopened sequences
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT setval('test_seq', 50)");
            assert_eq!(rows, vec![(50,)]);
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT currval('test_seq')");
            assert_eq!(rows, vec![(50,)]);
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('test_seq')");
            assert_eq!(rows, vec![(51,)]);

            // currval before nextval on a fresh connection errors
            let conn2 = db.connect_limbo();
            let err = conn2.execute("SELECT currval('test_seq')");
            assert!(err.is_err());

            conn.close()?;
        }

        Ok(())
    }

    /// Nextval must persist even when the connection is dropped without an
    /// explicit close — simulates a process crash after autocommit nextval.
    /// Guards against regressions where the backing-table write is somehow
    /// elided (or rolled back) and the next process opens to a stale
    /// watermark.
    #[test]
    fn test_sequence_nextval_persists_after_drop() -> anyhow::Result<()> {
        let path = TempDir::new().unwrap().keep().join("temp_seq_crash");

        // Phase 1: create sequence, call nextval, then DROP everything (no close).
        {
            let db = TempDatabase::new_with_existent(&path);
            let conn = db.connect_limbo();
            conn.execute("CREATE SEQUENCE crash_seq START 1 INCREMENT 1")?;

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('crash_seq')");
            assert_eq!(rows, vec![(1,)]);
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('crash_seq')");
            assert_eq!(rows, vec![(2,)]);

            // Intentionally no conn.close() — simulate unclean shutdown.
            drop(conn);
            drop(db);
        }

        // Phase 2: reopen — nextval must continue from 2, not restart at 1.
        {
            let db = TempDatabase::new_with_existent(&path);
            let conn = db.connect_limbo();

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('crash_seq')");
            assert_eq!(rows, vec![(3,)]);
        }

        Ok(())
    }

    /// setval inside an aborted tx must not affect post-rollback nextval:
    /// the user-visible value depends only on disk state, which the
    /// rollback reverted. This is structurally guaranteed by the disk-only
    /// design (no in-memory state to leak), but the test stays as a
    /// behavioral regression guard.
    #[test]
    fn test_setval_rollback_does_not_leak_to_nextval() -> anyhow::Result<()> {
        let path = TempDir::new().unwrap().keep().join("temp_setval_rollback");
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        conn.execute("CREATE SEQUENCE seq START 1 INCREMENT 1")?;

        // Consume the first value so is_called is true
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('seq')");
        assert_eq!(rows, vec![(1,)]);

        // setval inside a rolled-back transaction must not affect nextval
        conn.execute("BEGIN")?;
        conn.execute("SELECT setval('seq', 100)")?;
        conn.execute("ROLLBACK")?;

        // Next value must continue from 1, not from 100
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('seq')");
        assert_eq!(
            rows,
            vec![(2,)],
            "setval inside rolled-back tx must not affect post-rollback nextval"
        );

        Ok(())
    }

    #[test]
    fn test_sequence_cross_connection_visibility() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_sequence_cross_conn");
        let db = TempDatabase::new_with_existent(&path);
        let conn1 = db.connect_limbo();

        // Conn1 creates sequence
        conn1.execute("CREATE SEQUENCE cross_seq")?;

        // Conn2 must see it
        let conn2 = db.connect_limbo();
        let rows: Vec<(i64,)> = conn2.exec_rows("SELECT nextval('cross_seq')");
        assert_eq!(rows, vec![(1,)]);

        // Conn1 drops it
        conn1.execute("DROP SEQUENCE cross_seq")?;

        // Conn2 must get error
        let err = conn2.execute("SELECT nextval('cross_seq')");
        assert!(err.is_err());

        Ok(())
    }

    /// Sequences must produce globally unique values across connections.
    /// Each nextval() call — regardless of which connection issues it —
    /// must advance a single shared counter so no two calls ever return
    /// the same value.
    #[test]
    fn test_sequence_cross_connection_shared_counter() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_sequence_shared_counter");
        let db = TempDatabase::new_with_existent(&path);
        let conn1 = db.connect_limbo();

        conn1.execute("CREATE SEQUENCE shared_seq START 1 INCREMENT 1")?;

        let conn2 = db.connect_limbo();
        // Force conn2 to pick up the sequence via a schema refresh.
        conn2.execute("SELECT 1")?;

        // Interleave nextval across two connections.
        let rows1: Vec<(i64,)> = conn1.exec_rows("SELECT nextval('shared_seq')");
        assert_eq!(rows1, vec![(1,)]);

        let rows2: Vec<(i64,)> = conn2.exec_rows("SELECT nextval('shared_seq')");
        assert_eq!(
            rows2,
            vec![(2,)],
            "conn2 returned a duplicate — connections are not sharing the same sequence counter"
        );

        let rows1b: Vec<(i64,)> = conn1.exec_rows("SELECT nextval('shared_seq')");
        assert_eq!(rows1b, vec![(3,)]);

        Ok(())
    }

    /// MVCC AUTOINCREMENT must account for manual rowid UPDATEs.
    ///
    /// SQLite computes the next rowid as MAX(sqlite_sequence, max_rowid_in_table) + 1.
    /// After UPDATE t SET a=a+10, the max rowid in the table is 11, so the next
    /// auto-generated rowid must be 12, not 2.
    ///
    /// Currently fails in MVCC because the sequence is the sole source of truth
    /// and it doesn't know about UPDATEs to the rowid.
    #[test]
    fn test_mvcc_autoincrement_after_rowid_update() -> anyhow::Result<()> {
        let db = TempDatabase::new_with_mvcc("mvcc_autoinc_update");
        let conn = db.connect_limbo();

        conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY AUTOINCREMENT)")?;
        conn.execute("INSERT INTO t DEFAULT VALUES")?;
        conn.execute("UPDATE t SET a = a + 10")?;
        conn.execute("INSERT INTO t DEFAULT VALUES")?;

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT a FROM t ORDER BY a");
        assert_eq!(
            rows,
            vec![(11,), (12,)],
            "next autoincrement rowid must skip past manually updated rowids"
        );

        Ok(())
    }

    /// Two sequential autocommit nextvals on the same sequence in MVCC
    /// (different connections) must both succeed and produce monotonic
    /// values. The danger in the old design was commit-time compaction
    /// creating spurious conflicts; the disk-only design persists nothing
    /// to compact mid-call.
    #[test]
    fn test_mvcc_concurrent_nextval_no_conflict() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_concurrent_nextval");
        let db = TempDatabase::builder()
            .with_db_path(&path)
            .with_mvcc(true)
            .build();
        let conn1 = db.connect_limbo();
        let conn2 = db.connect_limbo();

        conn1.execute("CREATE SEQUENCE s START 1 INCREMENT 1")?;
        conn2.execute("SELECT 1")?;

        // First nextval bootstraps is_called=true on disk so subsequent
        // nextvals exercise the value+inc path rather than the start path.
        let _: Vec<(i64,)> = conn1.exec_rows("SELECT nextval('s')");

        // Each emits a new watermark row keyed by value (rowid alias);
        // two committed sequential calls produce distinct values, the
        // commit-time compaction collapses the trail to one row.
        let r1 = conn1.execute("SELECT nextval('s')");
        let r2 = conn2.execute("SELECT nextval('s')");

        assert!(
            r1.is_ok() && r2.is_ok(),
            "two concurrent autocommit nextvals should both succeed: \
             conn1={r1:?} conn2={r2:?}"
        );
        Ok(())
    }

    /// Two-connection MVCC durability with autonomous-inner-tx nextval:
    /// conn2's autocommit setval is durable, and conn1's nextval inside
    /// BEGIN CONCURRENT is also durable (committed autonomously) even
    /// though conn1's outer tx never commits — this is the Postgres
    /// semantic the autonomous design encodes (a nextval is "burned"
    /// the moment it's emitted; rolling back the surrounding tx does
    /// not un-burn it).
    #[test]
    fn test_mvcc_setval_persists_with_two_connections() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_setval_two_conn_mvcc");

        {
            let db = TempDatabase::builder()
                .with_db_path(&path)
                .with_mvcc(true)
                .build();
            let conn1 = db.connect_limbo();
            let conn2 = db.connect_limbo();

            conn1.execute("CREATE SEQUENCE s START 1 INCREMENT 1")?;
            conn2.execute("SELECT 1")?; // pick up schema

            // conn2 first: autocommit setval(50). Should be durable.
            let rows: Vec<(i64,)> = conn2.exec_rows("SELECT setval('s', 50)");
            assert_eq!(rows, vec![(50,)]);

            // conn1 opens a CONCURRENT tx and nextvals — autonomous-inner-tx
            // commits the watermark advance independently of conn1's outer.
            conn1.execute("BEGIN CONCURRENT")?;
            let rows: Vec<(i64,)> = conn1.exec_rows("SELECT nextval('s')");
            assert_eq!(rows, vec![(51,)]); // sees setval'd value
                                           // conn1 closes without commit; the nextval is still durable

            conn1.close()?;
            conn2.close()?;
        }

        {
            let db = TempDatabase::builder()
                .with_db_path(&path)
                .with_mvcc(true)
                .build();
            let conn = db.connect_limbo();
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('s')");
            assert_eq!(
                rows,
                vec![(52,)],
                "MVCC: autonomous nextval committed the 51 even though conn1's \
                 outer BEGIN CONCURRENT never committed; next nextval after \
                 reopen continues from 52"
            );
            conn.close()?;
        }

        Ok(())
    }

    /// MVCC version of test_setval_persists_across_reopen — verifies that
    /// a committed autocommit setval in MVCC mode is durably persisted
    /// across a close+reopen cycle, independent of the whopper restart
    /// machinery. If this fails, we have a minimal repro of a real
    /// durability gap in MVCC commit_txn.
    #[test]
    fn test_setval_persists_across_reopen_mvcc() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_setval_crash_mvcc");

        {
            let db = TempDatabase::builder()
                .with_db_path(&path)
                .with_mvcc(true)
                .build();
            let conn = db.connect_limbo();
            conn.execute("CREATE SEQUENCE s START 1 INCREMENT 1")?;

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('s')");
            assert_eq!(rows, vec![(1,)]);

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT setval('s', 50)");
            assert_eq!(rows, vec![(50,)]);

            conn.close()?;
        }

        {
            let db = TempDatabase::builder()
                .with_db_path(&path)
                .with_mvcc(true)
                .build();
            let conn = db.connect_limbo();

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('s')");
            assert_eq!(
                rows,
                vec![(51,)],
                "MVCC: nextval after reopen must continue from setval watermark, \
                 not regress — if it does, autocommit setval is not durable across \
                 close+reopen in MVCC mode"
            );

            conn.close()?;
        }

        Ok(())
    }

    /// setval() must be crash-safe: after setval(50) + close + reopen,
    /// nextval must return 51, not regress to the pre-setval watermark.
    #[test]
    fn test_setval_persists_across_reopen() -> anyhow::Result<()> {
        let path = TempDir::new().unwrap().keep().join("temp_setval_crash");

        {
            let db = TempDatabase::new_with_existent(&path);
            let conn = db.connect_limbo();
            conn.execute("CREATE SEQUENCE s START 1 INCREMENT 1")?;

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('s')");
            assert_eq!(rows, vec![(1,)]);

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT setval('s', 50)");
            assert_eq!(rows, vec![(50,)]);

            conn.close()?;
        }

        {
            let db = TempDatabase::new_with_existent(&path);
            let conn = db.connect_limbo();

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('s')");
            assert_eq!(
                rows,
                vec![(51,)],
                "nextval after reopen must continue from setval watermark, not regress"
            );

            conn.close()?;
        }

        Ok(())
    }

    /// setval is rejected inside BEGIN CONCURRENT — it requires an exclusive
    /// transaction.
    ///
    /// setval can move the sequence to an arbitrary value (including
    /// downward), so persisting it requires DELETE-then-INSERT on the
    /// backing table; a plain append plus commit-time MAX-compaction would
    /// silently lose a downward setval. Under BEGIN CONCURRENT, that DELETE
    /// races with another concurrent tx's nextval (Hekaton write-lock on
    /// the existing version chain), and an in-handler upgrade to exclusive
    /// is too late — `begin_exclusive_tx` only blocks future writers, it
    /// cannot revoke write locks already held by active concurrent txs.
    ///
    /// The same reason DDL (`CREATE TABLE`) and header-cookie writes
    /// (`PRAGMA application_id = ...`) reject `BEGIN CONCURRENT` applies
    /// to setval: it is global metadata with no row-level conflict key.
    #[test]
    fn test_setval_allowed_in_concurrent_tx() -> anyhow::Result<()> {
        // Under the disk-only sequence design, setval no longer requires an
        // exclusive transaction. The DELETE-all + INSERT bytecode is emitted
        // inline; concurrent nextval writers either land safely (different
        // backing-table keys) or hit a WriteWriteConflict that surfaces as
        // the standard Busy/conflict error — the outer tx decides what to
        // do. setval inside a solo BEGIN CONCURRENT therefore succeeds.
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_setval_concurrent_ok");
        let db = TempDatabase::builder()
            .with_db_path(&path)
            .with_mvcc(true)
            .build();
        let conn = db.connect_limbo();

        conn.execute("CREATE SEQUENCE s START 1 INCREMENT 1")?;

        conn.execute("BEGIN CONCURRENT")?;
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT setval('s', 50)");
        assert_eq!(
            rows,
            vec![(50,)],
            "setval inside BEGIN CONCURRENT should succeed when no other writer conflicts"
        );
        conn.execute("COMMIT")?;

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT setval('s', 100)");
        assert_eq!(rows, vec![(100,)]);

        conn.execute("BEGIN")?;
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT setval('s', 200)");
        assert_eq!(rows, vec![(200,)]);
        conn.execute("COMMIT")?;

        Ok(())
    }

    /// Returns true if a `Result` carries an MVCC-style conflict-class
    /// error — the engine's standard way of telling a concurrent writer
    /// to retry. Used by the setval/nextval concurrency test to allow
    /// either side of the race to abort cleanly.
    fn is_conflict_err<T>(r: &Result<T, turso_core::LimboError>) -> bool {
        matches!(
            r,
            Err(turso_core::LimboError::WriteWriteConflict
                | turso_core::LimboError::Busy
                | turso_core::LimboError::BusySnapshot
                | turso_core::LimboError::CommitDependencyAborted)
        )
    }

    /// MVCC: setval in one BEGIN CONCURRENT transaction races with nextval
    /// in another BEGIN CONCURRENT transaction. The engine must resolve the
    /// contention without corrupting the sequence — either both commits
    /// succeed and the final watermark is consistent with the commit order,
    /// or one tx aborts cleanly with a write-write conflict.
    ///
    /// What this test is really checking: the disk-only design's
    /// `setval = DELETE-all + INSERT` bytecode shares the backing table
    /// with nextval's RMW. Under MVCC, the two writers' commit-validation
    /// must detect the conflict. The forbidden outcome is silently
    /// accepting both commits and ending up with a sequence whose disk
    /// watermark doesn't match either intended value (corruption).
    ///
    /// Reviewer context: the whopper workload's policy comment in
    /// `testing/concurrent-simulator/workloads.rs` historically claimed
    /// setval was *rejected* inside BEGIN CONCURRENT and skipped it. The
    /// engine actually accepts the operation; this test asserts that the
    /// acceptance is sound under contention.
    #[test]
    fn test_setval_vs_nextval_concurrent_tx_mvcc_no_corruption() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_setval_vs_nextval_concurrent");
        let db = TempDatabase::builder()
            .with_db_path(&path)
            .with_mvcc(true)
            .build();

        let conn1 = db.connect_limbo();
        let conn2 = db.connect_limbo();

        conn1.execute("CREATE SEQUENCE s START 1 INCREMENT 1")?;
        conn2.execute("SELECT 1")?;

        // Bootstrap is_called=true so subsequent nextvals go through the
        // value+inc path rather than the start path.
        let _: Vec<(i64,)> = conn1.exec_rows("SELECT nextval('s')");

        conn1.execute("BEGIN CONCURRENT")?;
        conn2.execute("BEGIN CONCURRENT")?;

        // conn1 sets the watermark; this writes to the backing table.
        let setval_rows: Vec<(i64,)> = conn1.exec_rows("SELECT setval('s', 500)");
        assert_eq!(
            setval_rows,
            vec![(500,)],
            "conn1 setval returns the set value"
        );

        // conn2 attempts nextval — its autonomous-inner-tx targets the same
        // backing table that conn1 is mid-writing. The engine must surface
        // the conflict deterministically; either it fails here with a
        // conflict-class error (eager detection) or it succeeds and the
        // outer COMMIT below fails (deferred detection). Both shapes are
        // acceptable; what is NOT acceptable is silent acceptance that
        // ends up corrupting the watermark.
        let conn2_nextval = conn2.execute("SELECT nextval('s')");

        // Commit both; record what happened.
        let conn1_commit = conn1.execute("COMMIT");
        let conn2_commit = conn2.execute("COMMIT");

        // At least one of the two concurrent tx writers must have either
        // succeeded outright or aborted with a conflict-class error. A
        // panic in the engine code path, an unrelated error, or both
        // succeeding while the watermark drifts would all be bugs.
        let writers_resolved = (conn2_nextval.is_ok() || is_conflict_err(&conn2_nextval))
            && (conn1_commit.is_ok() || is_conflict_err(&conn1_commit))
            && (conn2_commit.is_ok() || is_conflict_err(&conn2_commit));
        assert!(
            writers_resolved,
            "concurrent setval/nextval must resolve via success or conflict-class error; \
             conn2_nextval={conn2_nextval:?} conn1_commit={conn1_commit:?} conn2_commit={conn2_commit:?}"
        );

        // Whichever side committed determines the post-resolution
        // watermark. Snapshot what's actually persisted on disk:
        let final_rows: Vec<(i64,)> = conn1.exec_rows("SELECT nextval('s')");
        let next = final_rows[0].0;

        // Enumerate the allowed outcomes:
        //   * conn1 (setval=500) committed: next nextval emits 501.
        //   * Both conn2's nextval AND its commit succeeded (so conn2 wrote
        //     watermark=2): next nextval emits 3.
        //   * conn1's commit was the only successful write but conn2 also
        //     committed an autonomous-inner nextval before failing: next
        //     emits 501 (autonomous-inner nextval is "burned" but the
        //     setval write supersedes any value below 500).
        //   * All concurrent writers conflict-aborted and the watermark
        //     remains at 1 from the bootstrap: next nextval emits 2.
        let allowed = [2_i64, 3, 501];
        assert!(
            allowed.contains(&next),
            "post-conflict nextval must be one of {allowed:?} (one of the writers' \
             intended states); got {next}. Anything else indicates the two writers \
             corrupted the watermark."
        );

        conn1.close()?;
        conn2.close()?;
        Ok(())
    }

    /// Descending sequences must recover from the MIN watermark, not MAX.
    ///
    /// A descending sequence (INCREMENT BY -1) advances downward, so the
    /// most-advanced value is the smallest. Recovery must use MIN(value)
    /// from the backing table, not MAX(value).
    ///
    /// This test simulates an uncompacted backing table (as would happen
    /// after a crash before commit-time compaction) by manually inserting
    /// extra rows into the backing table. With multiple rows (-1, -2, -3),
    /// MAX(value) returns -1 (least advanced) instead of -3 (most advanced).
    ///
    /// Gated off when the `checksum` feature is enabled — the test uses
    /// `rusqlite` to write rows directly into the backing table, which
    /// bypasses Turso's per-page checksum update. The next Turso open
    /// fails with `ChecksumMismatch` on the rewritten page; the
    /// recovery contract this test exercises is independent of the
    /// checksum feature, so we skip rather than rewrite the injection.
    #[cfg_attr(feature = "checksum", ignore)]
    #[test]
    fn test_descending_sequence_recovery() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_desc_seq_recovery");

        {
            let db = TempDatabase::new_with_existent(&path);
            let conn = db.connect_limbo();
            conn.execute(
                "CREATE SEQUENCE desc_seq START -1 INCREMENT BY -1 MINVALUE -1000 MAXVALUE -1",
            )?;

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('desc_seq')");
            assert_eq!(rows, vec![(-1,)]);
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('desc_seq')");
            assert_eq!(rows, vec![(-2,)]);
            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('desc_seq')");
            assert_eq!(rows, vec![(-3,)]);

            conn.close()?;
        }

        // Use rusqlite to inject uncompacted rows, simulating a crash
        // that left multiple rows in the backing table.
        {
            let sqlite_conn = rusqlite::Connection::open(&path)?;
            sqlite_conn.execute_batch(
                "INSERT OR IGNORE INTO \"__turso_internal_seq_desc_seq\" \
                 (value, is_called, start, inc, min, max, cycle) \
                 VALUES (-1, 1, -1, -1, -1000, -1, 0); \
                 INSERT OR IGNORE INTO \"__turso_internal_seq_desc_seq\" \
                 (value, is_called, start, inc, min, max, cycle) \
                 VALUES (-2, 1, -1, -1, -1000, -1, 0);",
            )?;
        }

        {
            let db = TempDatabase::new_with_existent(&path);
            let conn = db.connect_limbo();

            let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('desc_seq')");
            assert_eq!(
                rows,
                vec![(-4,)],
                "descending sequence must recover from MIN(value), not MAX(value)"
            );

            conn.close()?;
        }

        Ok(())
    }

    /// WAL rollback semantics for nextval: a rolled-back nextval is undone
    /// and its value will be re-emitted by the next call. This matches the
    /// SQLite AUTOINCREMENT contract — the watermark is part of the
    /// transaction and ROLLBACK reverts the backing-table row.
    #[test]
    fn test_wal_nextval_rolled_back_re_emits_value() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_wal_nextval_rollback");
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();

        conn.execute("CREATE SEQUENCE rb START 1 INCREMENT 1")?;

        conn.execute("BEGIN")?;
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('rb')");
        assert_eq!(rows, vec![(1,)]);
        conn.execute("ROLLBACK")?;

        // After rollback the backing table reverts; the next nextval
        // re-emits the value that was rolled back.
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('rb')");
        assert_eq!(
            rows,
            vec![(1,)],
            "WAL: rolled-back nextval must be re-emitted on the next call \
             (matches SQLite AUTOINCREMENT rollback semantics)"
        );

        Ok(())
    }

    /// MVCC rollback semantics for nextval (current behavior, intra-process).
    /// Today the nextval write is bundled with the outer tx — same as WAL —
    /// so ROLLBACK undoes the bump and the next nextval re-emits the value.
    ///
    /// True autonomous-tx semantics (where ROLLBACK does NOT undo the bump,
    /// matching Postgres) is a planned follow-up (task #29). When that lands
    /// this test will need to update its expectation.
    #[test]
    fn test_mvcc_nextval_rolled_back_re_emits_value_today() -> anyhow::Result<()> {
        let path = TempDir::new()
            .unwrap()
            .keep()
            .join("temp_mvcc_nextval_rollback");
        let db = TempDatabase::builder()
            .with_db_path(&path)
            .with_mvcc(true)
            .build();
        let conn = db.connect_limbo();

        conn.execute("CREATE SEQUENCE rbm START 1 INCREMENT 1")?;

        conn.execute("BEGIN")?;
        let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('rbm')");
        assert_eq!(rows, vec![(1,)]);
        conn.execute("ROLLBACK")?;

        let rows: Vec<(i64,)> = conn.exec_rows("SELECT nextval('rbm')");
        assert_eq!(
            rows,
            vec![(1,)],
            "MVCC (pre-autonomous-tx): bundled-with-outer-tx nextval is \
             reverted on rollback and the next call re-emits the value"
        );

        Ok(())
    }

    /// Stress: N threads, each pulling M values from the same sequence in WAL
    /// mode. Every value must be unique — duplicates would mean two threads
    /// observed the same disk watermark and computed the same next value
    /// (the bug fixed by disk-only RMW + WAL write-lock serialization).
    ///
    /// Threads retry on Busy: WAL's single-writer lock causes legitimate Busy
    /// returns when contention is high; retrying re-reads the disk watermark
    /// and computes the next value from the now-current state.
    #[test]
    fn test_stress_intraprocess_nextval_wal_no_duplicates() -> anyhow::Result<()> {
        use std::sync::Mutex;
        use std::thread;
        const THREADS: usize = 8;
        const PER_THREAD: usize = 50;

        let path = TempDir::new().unwrap().keep().join("stress_nextval_wal");
        let db = TempDatabase::new_with_existent(&path);
        let conn = db.connect_limbo();
        conn.execute("CREATE SEQUENCE s START 1 INCREMENT 1")?;
        conn.close()?;

        let db = Arc::new(TempDatabase::new_with_existent(&path));
        let collected: Arc<Mutex<Vec<i64>>> =
            Arc::new(Mutex::new(Vec::with_capacity(THREADS * PER_THREAD)));

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let db = Arc::clone(&db);
                let collected = Arc::clone(&collected);
                thread::spawn(move || {
                    let conn = db.connect_limbo();
                    let mut mine = Vec::with_capacity(PER_THREAD);
                    for _ in 0..PER_THREAD {
                        let mut tries = 0usize;
                        loop {
                            tries += 1;
                            if tries > 10_000 {
                                panic!("nextval gave up after 10k Busy retries");
                            }
                            match conn.prepare("SELECT nextval('s')") {
                                Ok(mut stmt) => {
                                    let mut got: Option<i64> = None;
                                    let r = stmt.run_with_row_callback(|row| {
                                        got = Some(row.get::<i64>(0).unwrap());
                                        Ok(())
                                    });
                                    match r {
                                        Ok(()) => {
                                            mine.push(got.expect("nextval row"));
                                            break;
                                        }
                                        Err(turso_core::LimboError::Busy) => continue,
                                        Err(e) => panic!("nextval failed: {e:?}"),
                                    }
                                }
                                Err(turso_core::LimboError::Busy) => continue,
                                Err(e) => panic!("prepare failed: {e:?}"),
                            }
                        }
                    }
                    collected.lock().unwrap().extend(mine);
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread join");
        }

        let values = collected.lock().unwrap().clone();
        assert_eq!(
            values.len(),
            THREADS * PER_THREAD,
            "expected {} nextvals to all succeed",
            THREADS * PER_THREAD
        );
        let unique: std::collections::HashSet<_> = values.iter().copied().collect();
        assert_eq!(
            unique.len(),
            values.len(),
            "expected all nextvals to return distinct values; duplicates present"
        );
        // Values should be exactly 1..=N*M (no gaps under WAL bundled semantics,
        // because every nextval that returned a value also committed under the
        // write lock).
        let mut sorted = values;
        sorted.sort();
        let expected: Vec<i64> = (1..=(THREADS * PER_THREAD) as i64).collect();
        assert_eq!(
            sorted, expected,
            "values must form a contiguous 1..N sequence under WAL"
        );

        Ok(())
    }

    /// Stress: N threads / M nextvals on the same MVCC database.
    /// Under autonomous-tx nextval, every call must commit independently
    /// of the others — no duplicates, no abort cascades. Gaps ARE allowed
    /// here (rolled-back outer txns leave their nextval bumps in place
    /// per Postgres semantics), so we only assert uniqueness and count.
    #[test]
    fn test_stress_intraprocess_nextval_mvcc_no_duplicates() -> anyhow::Result<()> {
        use std::sync::Mutex;
        use std::thread;
        const THREADS: usize = 8;
        const PER_THREAD: usize = 50;

        let path = TempDir::new().unwrap().keep().join("stress_nextval_mvcc");
        let db = TempDatabase::builder()
            .with_db_path(&path)
            .with_mvcc(true)
            .build();
        let conn = db.connect_limbo();
        conn.execute("CREATE SEQUENCE s START 1 INCREMENT 1")?;
        conn.close()?;

        let db = Arc::new(
            TempDatabase::builder()
                .with_db_path(&path)
                .with_mvcc(true)
                .build(),
        );
        let collected: Arc<Mutex<Vec<i64>>> =
            Arc::new(Mutex::new(Vec::with_capacity(THREADS * PER_THREAD)));

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let db = Arc::clone(&db);
                let collected = Arc::clone(&collected);
                thread::spawn(move || {
                    let conn = db.connect_limbo();
                    let mut mine = Vec::with_capacity(PER_THREAD);
                    for _ in 0..PER_THREAD {
                        let mut tries = 0usize;
                        loop {
                            tries += 1;
                            if tries > 10_000 {
                                panic!("nextval gave up after 10k retries");
                            }
                            match conn.prepare("SELECT nextval('s')") {
                                Ok(mut stmt) => {
                                    let mut got: Option<i64> = None;
                                    let r = stmt.run_with_row_callback(|row| {
                                        got = Some(row.get::<i64>(0).unwrap());
                                        Ok(())
                                    });
                                    match r {
                                        Ok(()) => {
                                            mine.push(got.expect("nextval row"));
                                            break;
                                        }
                                        Err(turso_core::LimboError::Busy)
                                        | Err(turso_core::LimboError::WriteWriteConflict) => {
                                            continue
                                        }
                                        Err(e) => panic!("nextval failed: {e:?}"),
                                    }
                                }
                                Err(turso_core::LimboError::Busy) => continue,
                                Err(e) => panic!("prepare failed: {e:?}"),
                            }
                        }
                    }
                    collected.lock().unwrap().extend(mine);
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread join");
        }

        let values = collected.lock().unwrap().clone();
        assert_eq!(
            values.len(),
            THREADS * PER_THREAD,
            "expected {} nextvals to all succeed",
            THREADS * PER_THREAD
        );
        let unique: std::collections::HashSet<_> = values.iter().copied().collect();
        assert_eq!(
            unique.len(),
            values.len(),
            "expected all MVCC nextvals to return distinct values; duplicates present"
        );

        Ok(())
    }

    /// Stress: one writer doing nextvals, another periodically setvals.
    /// After each setval(X), subsequent nextvals must produce values > X.
    /// No panics, no errors that escape the opcode's retry loop.
    #[test]
    fn test_stress_nextval_setval_mixed_mvcc() -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Mutex;
        use std::thread;
        use std::time::Duration;
        const NEXTVAL_THREADS: usize = 4;
        const NEXTVAL_PER_THREAD: usize = 100;

        let path = TempDir::new().unwrap().keep().join("stress_mixed_mvcc");
        let db = TempDatabase::builder()
            .with_db_path(&path)
            .with_mvcc(true)
            .build();
        let conn = db.connect_limbo();
        conn.execute("CREATE SEQUENCE s START 1 INCREMENT 1")?;
        conn.close()?;

        let db = Arc::new(
            TempDatabase::builder()
                .with_db_path(&path)
                .with_mvcc(true)
                .build(),
        );

        let stop = Arc::new(AtomicBool::new(false));
        let nextvals: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));

        // Setval thread: bumps the watermark to escalating values.
        let setval_handle = {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let conn = db.connect_limbo();
                let mut target: i64 = 1_000;
                while !stop.load(Ordering::Relaxed) {
                    let sql = format!("SELECT setval('s', {target})");
                    let _ = conn.execute(&sql);
                    target += 1_000;
                    thread::sleep(Duration::from_millis(5));
                }
            })
        };

        let handles: Vec<_> = (0..NEXTVAL_THREADS)
            .map(|_| {
                let db = Arc::clone(&db);
                let nextvals = Arc::clone(&nextvals);
                thread::spawn(move || {
                    let conn = db.connect_limbo();
                    let mut mine = Vec::with_capacity(NEXTVAL_PER_THREAD);
                    for _ in 0..NEXTVAL_PER_THREAD {
                        let mut tries = 0usize;
                        loop {
                            tries += 1;
                            if tries > 10_000 {
                                panic!("nextval gave up after 10k retries");
                            }
                            match conn.prepare("SELECT nextval('s')") {
                                Ok(mut stmt) => {
                                    let mut got: Option<i64> = None;
                                    let r = stmt.run_with_row_callback(|row| {
                                        got = Some(row.get::<i64>(0).unwrap());
                                        Ok(())
                                    });
                                    match r {
                                        Ok(()) => {
                                            mine.push(got.expect("nextval row"));
                                            break;
                                        }
                                        Err(turso_core::LimboError::Busy)
                                        | Err(turso_core::LimboError::WriteWriteConflict) => {
                                            continue
                                        }
                                        Err(e) => panic!("nextval failed: {e:?}"),
                                    }
                                }
                                Err(turso_core::LimboError::Busy) => continue,
                                Err(e) => panic!("prepare failed: {e:?}"),
                            }
                        }
                    }
                    nextvals.lock().unwrap().extend(mine);
                })
            })
            .collect();

        for h in handles {
            h.join().expect("nextval thread join");
        }
        stop.store(true, Ordering::Relaxed);
        setval_handle.join().expect("setval thread join");

        let values = nextvals.lock().unwrap().clone();
        assert_eq!(
            values.len(),
            NEXTVAL_THREADS * NEXTVAL_PER_THREAD,
            "every nextval must succeed"
        );
        let unique: std::collections::HashSet<_> = values.iter().copied().collect();
        assert_eq!(
            unique.len(),
            values.len(),
            "every nextval must return a distinct value even under concurrent setval"
        );

        Ok(())
    }
}
