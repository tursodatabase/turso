use rand::{rng, RngCore};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{Clock, Database, IO};
use turso_pg::Connection;

pub struct TempDatabase {
    pub path: PathBuf,
    #[allow(dead_code)]
    pub io: Arc<dyn IO + Send>,
    pub db: Arc<Database>,
}

unsafe impl Send for TempDatabase {}

#[derive(Debug, Default, Clone)]
pub struct TempDatabaseBuilder {
    db_name: Option<String>,
    opts: Option<turso_core::DatabaseOpts>,
    init_sql: Option<String>,
    enable_mvcc: bool,
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
            opts: None,
            init_sql: None,
            enable_mvcc: false,
            enable_views: false,
        }
    }

    pub fn with_db_name(mut self, db_name: impl AsRef<str>) -> Self {
        self.db_name = Some(db_name.as_ref().to_string());
        self
    }

    pub fn with_opts(mut self, opts: turso_core::DatabaseOpts) -> Self {
        self.opts = Some(opts);
        self
    }

    #[allow(dead_code)]
    pub fn with_init_sql(mut self, init_sql: impl AsRef<str>) -> Self {
        self.init_sql = Some(init_sql.as_ref().to_string());
        self
    }

    pub fn with_mvcc(mut self, enable: bool) -> Self {
        self.enable_mvcc = enable;
        self
    }

    #[allow(dead_code)]
    pub fn with_views(mut self, enable: bool) -> Self {
        self.enable_views = enable;
        self
    }

    pub fn build(self) -> TempDatabase {
        let mut opts = self
            .opts
            .unwrap_or_else(|| turso_core::DatabaseOpts::new().with_encryption(true));
        opts = opts
            .with_vacuum(true)
            .with_without_rowid(true)
            .with_custom_types(true)
            .with_udfs(true);

        if self.enable_views {
            opts = opts.with_views(true);
        }

        let db_name = self
            .db_name
            .unwrap_or_else(|| format!("test-{}.db", rng().next_u32()));
        let mut db_path = TempDir::new().unwrap().keep();
        db_path.push(db_name);

        if let Some(init_sql) = &self.init_sql {
            let connection = rusqlite::Connection::open(&db_path).unwrap();
            connection
                .pragma_update(None, "journal_mode", "wal")
                .unwrap();
            connection.execute(init_sql, ()).unwrap();
        }

        let io = Arc::new(TestIo {
            io: Arc::new(turso_core::PlatformIO::new().unwrap()),
        });
        let db = turso_pg::open_database_with_io(
            io.clone(),
            db_path.to_str().unwrap(),
            turso_core::OpenFlags::default(),
            opts,
        )
        .unwrap();

        if self.enable_mvcc {
            let conn = db.connect().unwrap();
            conn.pragma_update("journal_mode", "'mvcc'")
                .expect("enable mvcc");
        }

        TempDatabase {
            path: db_path,
            io,
            db,
        }
    }
}

#[allow(clippy::arc_with_non_send_sync)]
impl TempDatabase {
    pub const fn builder() -> TempDatabaseBuilder {
        TempDatabaseBuilder::new()
    }

    pub fn connect_limbo(&self) -> Arc<turso_core::Connection> {
        self.db.connect().unwrap()
    }

    pub fn connect_postgres(&self) -> Connection {
        Connection::new(self.connect_limbo())
    }
}
