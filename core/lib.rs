#![allow(clippy::arc_with_non_send_sync)]

mod assert;
mod error;
mod ext;
mod fast_lock;
mod function;
mod functions;
mod info;
mod io;
#[cfg(feature = "json")]
mod json;
pub mod mvcc;
mod parameters;
mod pragma;
mod pseudo;
pub mod result;
mod schema;
#[cfg(feature = "series")]
mod series;
mod storage;
#[allow(dead_code)]
#[cfg(feature = "time")]
mod time;
mod translate;
pub mod types;
mod util;
#[cfg(feature = "uuid")]
mod uuid;
mod vdbe;
mod vector;
mod vtab;

#[cfg(feature = "fuzz")]
pub mod numeric;

#[cfg(not(feature = "fuzz"))]
mod numeric;

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use crate::storage::{header_accessor, wal::DummyWAL};
use crate::translate::optimizer::optimize_plan;
use crate::translate::pragma::TURSO_CDC_DEFAULT_TABLE_NAME;
use crate::util::{OpenMode, OpenOptions};
use crate::vtab::VirtualTable;
use core::str;
pub use error::LimboError;
use fallible_iterator::FallibleIterator;
pub use io::clock::{Clock, Instant};
#[cfg(all(feature = "fs", target_family = "unix"))]
pub use io::UnixIO;
#[cfg(all(feature = "fs", target_os = "linux", feature = "io_uring"))]
pub use io::UringIO;
pub use io::{
    Buffer, Completion, CompletionType, File, MemoryIO, OpenFlags, PlatformIO, SyscallIO,
    WriteCompletion, IO,
};
use parking_lot::RwLock;
use schema::Schema;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::{
    borrow::Cow,
    cell::{Cell, RefCell, UnsafeCell},
    collections::HashMap,
    fmt::Display,
    io::Write,
    num::NonZero,
    ops::Deref,
    rc::Rc,
    sync::Arc,
};
#[cfg(feature = "fs")]
use storage::database::DatabaseFile;
use storage::page_cache::DumbLruPageCache;
pub use storage::pager::PagerCacheflushStatus;
use storage::pager::{DB_STATE_INITIALIZED, DB_STATE_UNITIALIZED};
pub use storage::{
    buffer_pool::BufferPool,
    database::DatabaseStorage,
    pager::PageRef,
    pager::{Page, Pager},
    wal::{CheckpointMode, CheckpointResult, CheckpointStatus, Wal, WalFile, WalFileShared},
};
use tracing::{instrument, Level};
use translate::select::prepare_select_plan;
use turso_sqlite3_parser::{ast, ast::Cmd, lexer::sql::Parser};
pub use types::RefValue;
pub use types::Value;
use util::parse_schema_rows;
use vdbe::builder::QueryMode;
use vdbe::builder::TableRefIdCounter;

pub type Result<T, E = LimboError> = std::result::Result<T, E>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TransactionState {
    Write { schema_did_change: bool },
    Read,
    None,
}

pub(crate) type MvStore = mvcc::MvStore<mvcc::LocalClock>;

pub(crate) type MvCursor = mvcc::cursor::ScanCursor<mvcc::LocalClock>;

pub struct Database {
    mv_store: Option<Rc<MvStore>>,
    schema: Arc<RwLock<Schema>>,
    db_file: Arc<dyn DatabaseStorage>,
    path: String,
    io: Arc<dyn IO>,
    // Shared structures of a Database are the parts that are common to multiple threads that might
    // create DB connections.
    _shared_page_cache: Arc<RwLock<DumbLruPageCache>>,
    maybe_shared_wal: RwLock<Option<Arc<UnsafeCell<WalFileShared>>>>,
    is_empty: Arc<AtomicUsize>,
    init_lock: Arc<Mutex<()>>,
    open_flags: OpenFlags,
}

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl Database {
    #[cfg(feature = "fs")]
    pub fn open_file(
        io: Arc<dyn IO>,
        path: &str,
        enable_mvcc: bool,
        enable_indexes: bool,
    ) -> Result<Arc<Database>> {
        Self::open_file_with_flags(io, path, OpenFlags::default(), enable_mvcc, enable_indexes)
    }

    #[cfg(feature = "fs")]
    pub fn open_file_with_flags(
        io: Arc<dyn IO>,
        path: &str,
        flags: OpenFlags,
        enable_mvcc: bool,
        enable_indexes: bool,
    ) -> Result<Arc<Database>> {
        let file = io.open_file(path, flags, true)?;
        let db_file = Arc::new(DatabaseFile::new(file));
        Self::open_with_flags(io, path, db_file, flags, enable_mvcc, enable_indexes)
    }

    #[allow(clippy::arc_with_non_send_sync)]
    pub fn open(
        io: Arc<dyn IO>,
        path: &str,
        db_file: Arc<dyn DatabaseStorage>,
        enable_mvcc: bool,
        enable_indexes: bool,
    ) -> Result<Arc<Database>> {
        Self::open_with_flags(
            io,
            path,
            db_file,
            OpenFlags::default(),
            enable_mvcc,
            enable_indexes,
        )
    }

    #[allow(clippy::arc_with_non_send_sync)]
    pub fn open_with_flags(
        io: Arc<dyn IO>,
        path: &str,
        db_file: Arc<dyn DatabaseStorage>,
        flags: OpenFlags,
        enable_mvcc: bool,
        enable_indexes: bool,
    ) -> Result<Arc<Database>> {
        let wal_path = format!("{path}-wal");
        let maybe_shared_wal = WalFileShared::open_shared_if_exists(&io, wal_path.as_str())?;
        let db_size = db_file.size()?;

        let mv_store = if enable_mvcc {
            Some(Rc::new(MvStore::new(
                mvcc::LocalClock::new(),
                mvcc::persistent_storage::Storage::new_noop(),
            )))
        } else {
            None
        };
        let wal_has_frames = maybe_shared_wal
            .as_ref()
            .is_some_and(|wal| unsafe { &*wal.get() }.max_frame.load(Ordering::SeqCst) > 0);

        let is_empty = if db_size == 0 && !wal_has_frames {
            DB_STATE_UNITIALIZED
        } else {
            DB_STATE_INITIALIZED
        };

        let shared_page_cache = Arc::new(RwLock::new(DumbLruPageCache::default()));
        let schema = Arc::new(RwLock::new(Schema::new(enable_indexes)));
        let db = Database {
            mv_store,
            path: path.to_string(),
            schema: schema.clone(),
            _shared_page_cache: shared_page_cache.clone(),
            maybe_shared_wal: RwLock::new(maybe_shared_wal),
            db_file,
            io: io.clone(),
            open_flags: flags,
            is_empty: Arc::new(AtomicUsize::new(is_empty)),
            init_lock: Arc::new(Mutex::new(())),
        };
        let db = Arc::new(db);

        // Check: https://github.com/tursodatabase/turso/pull/1761#discussion_r2154013123
        if is_empty == 2 {
            // parse schema
            let conn = db.connect()?;
            let schema_version = get_schema_version(&conn)?;
            schema.write().schema_version = schema_version;

            let mut schema = schema
                .try_write()
                .expect("lock on schema should succeed first try");

            let syms = conn.syms.borrow();

            if let Err(LimboError::ExtensionError(e)) =
                schema.make_from_btree(None, conn.pager.clone(), &syms)
            {
                // this means that a vtab exists and we no longer have the module loaded. we print
                // a warning to the user to load the module
                eprintln!("Warning: {e}");
            }
        }
        Ok(db)
    }

    pub fn connect(self: &Arc<Database>) -> Result<Arc<Connection>> {
        let buffer_pool = Arc::new(BufferPool::new(None));

        // Open existing WAL file if present
        if let Some(shared_wal) = self.maybe_shared_wal.read().clone() {
            // No pages in DB file or WAL -> empty database
            let is_empty = self.is_empty.clone();
            let wal = Rc::new(RefCell::new(WalFile::new(
                self.io.clone(),
                shared_wal,
                buffer_pool.clone(),
            )));
            let pager = Rc::new(Pager::new(
                self.db_file.clone(),
                wal,
                self.io.clone(),
                Arc::new(RwLock::new(DumbLruPageCache::default())),
                buffer_pool,
                is_empty,
                self.init_lock.clone(),
            )?);

            let page_size = header_accessor::get_page_size(&pager)
                .unwrap_or(storage::sqlite3_ondisk::DEFAULT_PAGE_SIZE)
                as u32;
            let default_cache_size = header_accessor::get_default_page_cache_size(&pager)
                .unwrap_or(storage::sqlite3_ondisk::DEFAULT_CACHE_SIZE);
            pager.buffer_pool.set_page_size(page_size as usize);
            let conn = Arc::new(Connection {
                _db: self.clone(),
                pager: pager.clone(),
                schema: RefCell::new(self.schema.read().clone()),
                last_insert_rowid: Cell::new(0),
                auto_commit: Cell::new(true),
                mv_transactions: RefCell::new(Vec::new()),
                transaction_state: Cell::new(TransactionState::None),
                last_change: Cell::new(0),
                syms: RefCell::new(SymbolTable::new()),
                total_changes: Cell::new(0),
                _shared_cache: false,
                cache_size: Cell::new(default_cache_size),
                readonly: Cell::new(false),
                wal_checkpoint_disabled: Cell::new(false),
                capture_data_changes: RefCell::new(CaptureDataChangesMode::Off),
                closed: Cell::new(false),
            });
            if let Err(e) = conn.register_builtins() {
                return Err(LimboError::ExtensionError(e));
            }
            return Ok(conn);
        };

        // No existing WAL; create one.
        // TODO: currently Pager needs to be instantiated with some implementation of trait Wal, so here's a workaround.
        let dummy_wal = Rc::new(RefCell::new(DummyWAL {}));
        let is_empty = self.is_empty.clone();
        let mut pager = Pager::new(
            self.db_file.clone(),
            dummy_wal,
            self.io.clone(),
            Arc::new(RwLock::new(DumbLruPageCache::default())),
            buffer_pool.clone(),
            is_empty,
            Arc::new(Mutex::new(())),
        )?;
        let page_size = header_accessor::get_page_size(&pager)
            .unwrap_or(storage::sqlite3_ondisk::DEFAULT_PAGE_SIZE) as u32;
        let default_cache_size = header_accessor::get_default_page_cache_size(&pager)
            .unwrap_or(storage::sqlite3_ondisk::DEFAULT_CACHE_SIZE);

        let wal_path = format!("{}-wal", self.path);
        let file = self.io.open_file(&wal_path, OpenFlags::Create, false)?;
        let real_shared_wal = WalFileShared::new_shared(page_size, &self.io, file)?;
        // Modify Database::maybe_shared_wal to point to the new WAL file so that other connections
        // can open the existing WAL.
        *self.maybe_shared_wal.write() = Some(real_shared_wal.clone());
        let wal = Rc::new(RefCell::new(WalFile::new(
            self.io.clone(),
            real_shared_wal,
            buffer_pool,
        )));
        pager.set_wal(wal);
        let conn = Arc::new(Connection {
            _db: self.clone(),
            pager: Rc::new(pager),
            schema: RefCell::new(self.schema.read().clone()),
            auto_commit: Cell::new(true),
            mv_transactions: RefCell::new(Vec::new()),
            transaction_state: Cell::new(TransactionState::None),
            last_insert_rowid: Cell::new(0),
            last_change: Cell::new(0),
            total_changes: Cell::new(0),
            syms: RefCell::new(SymbolTable::new()),
            _shared_cache: false,
            cache_size: Cell::new(default_cache_size),
            readonly: Cell::new(false),
            wal_checkpoint_disabled: Cell::new(false),
            capture_data_changes: RefCell::new(CaptureDataChangesMode::Off),
            closed: Cell::new(false),
        });

        if let Err(e) = conn.register_builtins() {
            return Err(LimboError::ExtensionError(e));
        }
        Ok(conn)
    }

    /// Open a new database file with optionally specifying a VFS without an existing database
    /// connection and symbol table to register extensions.
    #[cfg(feature = "fs")]
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn open_new<S>(
        path: &str,
        vfs: Option<S>,
        flags: OpenFlags,
        indexes: bool,
        mvcc: bool,
    ) -> Result<(Arc<dyn IO>, Arc<Database>)>
    where
        S: AsRef<str> + std::fmt::Display,
    {
        use crate::util::MEMORY_PATH;
        let vfsmods = ext::add_builtin_vfs_extensions(None)?;
        match vfs {
            Some(vfs) => {
                let io: Arc<dyn IO> = match vfsmods
                    .iter()
                    .find(|v| v.0 == vfs.as_ref())
                    .map(|v| v.1.clone())
                {
                    Some(vfs) => vfs,
                    None => match vfs.as_ref() {
                        "memory" => Arc::new(MemoryIO::new()),
                        "syscall" => Arc::new(SyscallIO::new()?),
                        #[cfg(all(target_os = "linux", feature = "io_uring"))]
                        "io_uring" => Arc::new(UringIO::new()?),
                        other => {
                            return Err(LimboError::InvalidArgument(format!(
                                "no such VFS: {other}"
                            )));
                        }
                    },
                };
                let db = Self::open_file_with_flags(io.clone(), path, flags, mvcc, indexes)?;
                Ok((io, db))
            }
            None => {
                let io: Arc<dyn IO> = match path.trim() {
                    MEMORY_PATH => Arc::new(MemoryIO::new()),
                    _ => Arc::new(PlatformIO::new()?),
                };
                let db = Self::open_file_with_flags(io.clone(), path, flags, mvcc, indexes)?;
                Ok((io, db))
            }
        }
    }
}

fn get_schema_version(conn: &Arc<Connection>) -> Result<u32> {
    let mut rows = conn
        .query("PRAGMA schema_version")?
        .ok_or(LimboError::InternalError(
            "failed to parse pragma schema_version on initialization".to_string(),
        ))?;
    let mut schema_version = None;
    loop {
        match rows.step()? {
            StepResult::Row => {
                let row = rows.row().unwrap();
                if schema_version.is_some() {
                    return Err(LimboError::InternalError(
                        "PRAGMA schema_version; returned more that one row".to_string(),
                    ));
                }
                schema_version = Some(row.get::<i64>(0)? as u32);
            }
            StepResult::IO => {
                rows.run_once()?;
            }
            StepResult::Interrupt => {
                return Err(LimboError::InternalError(
                    "PRAGMA schema_version; returned more that one row".to_string(),
                ));
            }
            StepResult::Done => {
                if let Some(version) = schema_version {
                    return Ok(version);
                } else {
                    return Err(LimboError::InternalError(
                        "failed to get schema_version".to_string(),
                    ));
                }
            }
            StepResult::Busy => {
                return Err(LimboError::InternalError(
                    "PRAGMA schema_version; returned more that one row".to_string(),
                ));
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CaptureDataChangesMode {
    Off,
    Id { table: String },
    Before { table: String },
    After { table: String },
    Full { table: String },
}

impl CaptureDataChangesMode {
    pub fn parse(value: &str) -> Result<CaptureDataChangesMode> {
        let (mode, table) = value
            .split_once(",")
            .unwrap_or((value, TURSO_CDC_DEFAULT_TABLE_NAME));
        match mode {
            "off" => Ok(CaptureDataChangesMode::Off),
            "id" => Ok(CaptureDataChangesMode::Id { table: table.to_string() }),
            "before" => Ok(CaptureDataChangesMode::Before { table: table.to_string() }),
            "after" => Ok(CaptureDataChangesMode::After { table: table.to_string() }),
            "full" => Ok(CaptureDataChangesMode::Full { table: table.to_string() }),
            _ => Err(LimboError::InvalidArgument(
                "unexpected pragma value: expected '<mode>' or '<mode>,<cdc-table-name>' parameter where mode is one of off|id|before|after|full".to_string(),
            ))
        }
    }
    pub fn has_after(&self) -> bool {
        matches!(
            self,
            CaptureDataChangesMode::After { .. } | CaptureDataChangesMode::Full { .. }
        )
    }
    pub fn has_before(&self) -> bool {
        matches!(
            self,
            CaptureDataChangesMode::Before { .. } | CaptureDataChangesMode::Full { .. }
        )
    }
    pub fn mode_name(&self) -> &str {
        match self {
            CaptureDataChangesMode::Off => "off",
            CaptureDataChangesMode::Id { .. } => "id",
            CaptureDataChangesMode::Before { .. } => "before",
            CaptureDataChangesMode::After { .. } => "after",
            CaptureDataChangesMode::Full { .. } => "full",
        }
    }
    pub fn table(&self) -> Option<&str> {
        match self {
            CaptureDataChangesMode::Off => None,
            CaptureDataChangesMode::Id { table }
            | CaptureDataChangesMode::Before { table }
            | CaptureDataChangesMode::After { table }
            | CaptureDataChangesMode::Full { table } => Some(table.as_str()),
        }
    }
}

pub struct Connection {
    _db: Arc<Database>,
    pager: Rc<Pager>,
    schema: RefCell<Schema>,
    /// Whether to automatically commit transaction
    auto_commit: Cell<bool>,
    mv_transactions: RefCell<Vec<crate::mvcc::database::TxID>>,
    transaction_state: Cell<TransactionState>,
    last_insert_rowid: Cell<i64>,
    last_change: Cell<i64>,
    total_changes: Cell<i64>,
    syms: RefCell<SymbolTable>,
    _shared_cache: bool,
    cache_size: Cell<i32>,
    readonly: Cell<bool>,
    wal_checkpoint_disabled: Cell<bool>,
    capture_data_changes: RefCell<CaptureDataChangesMode>,
    closed: Cell<bool>,
}

impl Connection {
    #[instrument(skip_all, level = Level::INFO)]
    pub fn prepare(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        if sql.as_ref().is_empty() {
            return Err(LimboError::InvalidArgument(
                "The supplied SQL string contains no statements".to_string(),
            ));
        }

        let sql = sql.as_ref();
        tracing::trace!("Preparing: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        let syms = self.syms.borrow();
        let cmd = cmd.expect("Successful parse on nonempty input string should produce a command");
        let byte_offset_end = parser.offset();
        let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
            .unwrap()
            .trim();
        self.maybe_update_schema();
        match cmd {
            Cmd::Stmt(stmt) => {
                let program = Rc::new(translate::translate(
                    self.schema.borrow().deref(),
                    stmt,
                    self.pager.clone(),
                    self.clone(),
                    &syms,
                    QueryMode::Normal,
                    input,
                )?);
                Ok(Statement::new(
                    program,
                    self._db.mv_store.clone(),
                    self.pager.clone(),
                ))
            }
            Cmd::Explain(_stmt) => todo!(),
            Cmd::ExplainQueryPlan(_stmt) => todo!(),
        }
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn query(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Option<Statement>> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let sql = sql.as_ref();
        tracing::trace!("Querying: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        let byte_offset_end = parser.offset();
        let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
            .unwrap()
            .trim();
        match cmd {
            Some(cmd) => self.run_cmd(cmd, input),
            None => Ok(None),
        }
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub(crate) fn run_cmd(
        self: &Arc<Connection>,
        cmd: Cmd,
        input: &str,
    ) -> Result<Option<Statement>> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let syms = self.syms.borrow();
        match cmd {
            Cmd::Stmt(ref stmt) | Cmd::Explain(ref stmt) => {
                let program = translate::translate(
                    self.schema.borrow().deref(),
                    stmt.clone(),
                    self.pager.clone(),
                    self.clone(),
                    &syms,
                    cmd.into(),
                    input,
                )?;
                let stmt = Statement::new(
                    program.into(),
                    self._db.mv_store.clone(),
                    self.pager.clone(),
                );
                Ok(Some(stmt))
            }
            Cmd::ExplainQueryPlan(stmt) => {
                let mut table_ref_counter = TableRefIdCounter::new();
                match stmt {
                    ast::Stmt::Select(select) => {
                        let mut plan = prepare_select_plan(
                            self.schema.borrow().deref(),
                            *select,
                            &syms,
                            &[],
                            &mut table_ref_counter,
                            translate::plan::QueryDestination::ResultRows,
                        )?;
                        optimize_plan(&mut plan, self.schema.borrow().deref())?;
                        let _ = std::io::stdout().write_all(plan.to_string().as_bytes());
                    }
                    _ => todo!(),
                }
                Ok(None)
            }
        }
    }

    pub fn query_runner<'a>(self: &'a Arc<Connection>, sql: &'a [u8]) -> QueryRunner<'a> {
        QueryRunner::new(self, sql)
    }

    /// Execute will run a query from start to finish taking ownership of I/O because it will run pending I/Os if it didn't finish.
    /// TODO: make this api async
    #[instrument(skip_all, level = Level::INFO)]
    pub fn execute(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let sql = sql.as_ref();
        let mut parser = Parser::new(sql.as_bytes());
        while let Some(cmd) = parser.next()? {
            let syms = self.syms.borrow();
            let byte_offset_end = parser.offset();
            let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
                .unwrap()
                .trim();
            self.maybe_update_schema();
            match cmd {
                Cmd::Explain(stmt) => {
                    let program = translate::translate(
                        self.schema.borrow().deref(),
                        stmt,
                        self.pager.clone(),
                        self.clone(),
                        &syms,
                        QueryMode::Explain,
                        input,
                    )?;
                    let _ = std::io::stdout().write_all(program.explain().as_bytes());
                }
                Cmd::ExplainQueryPlan(_stmt) => todo!(),
                Cmd::Stmt(stmt) => {
                    let program = translate::translate(
                        self.schema.borrow().deref(),
                        stmt,
                        self.pager.clone(),
                        self.clone(),
                        &syms,
                        QueryMode::Normal,
                        input,
                    )?;

                    let mut state =
                        vdbe::ProgramState::new(program.max_registers, program.cursor_ref.len());
                    loop {
                        let res = program.step(
                            &mut state,
                            self._db.mv_store.clone(),
                            self.pager.clone(),
                        )?;
                        if matches!(res, StepResult::Done) {
                            break;
                        }
                        self.run_once()?;
                    }
                }
            }
        }
        Ok(())
    }

    fn run_once(&self) -> Result<()> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let res = self._db.io.run_once();
        if res.is_err() {
            let state = self.transaction_state.get();
            if let TransactionState::Write { schema_did_change } = state {
                self.pager.rollback(schema_did_change, self)?
            }
        }
        res
    }

    #[cfg(feature = "fs")]
    pub fn from_uri(
        uri: &str,
        use_indexes: bool,
        mvcc: bool,
    ) -> Result<(Arc<dyn IO>, Arc<Connection>)> {
        use crate::util::MEMORY_PATH;
        let opts = OpenOptions::parse(uri)?;
        let flags = opts.get_flags()?;
        if opts.path == MEMORY_PATH || matches!(opts.mode, OpenMode::Memory) {
            let io = Arc::new(MemoryIO::new());
            let db =
                Database::open_file_with_flags(io.clone(), MEMORY_PATH, flags, mvcc, use_indexes)?;
            let conn = db.connect()?;
            return Ok((io, conn));
        }
        let (io, db) = Database::open_new(&opts.path, opts.vfs.as_ref(), flags, use_indexes, mvcc)?;
        if let Some(modeof) = opts.modeof {
            let perms = std::fs::metadata(modeof)?;
            std::fs::set_permissions(&opts.path, perms.permissions())?;
        }
        let conn = db.connect()?;
        conn.set_readonly(opts.immutable);
        Ok((io, conn))
    }

    pub fn set_readonly(&self, readonly: bool) {
        self.readonly.replace(readonly);
    }

    pub fn maybe_update_schema(&self) {
        let current_schema_version = self.schema.borrow().schema_version;
        if matches!(self.transaction_state.get(), TransactionState::None)
            && current_schema_version < self._db.schema.read().schema_version
        {
            let new_schema = self._db.schema.read();
            self.schema.replace(new_schema.clone());
        }
    }

    pub fn wal_frame_count(&self) -> Result<u64> {
        self.pager.wal_frame_count()
    }

    pub fn wal_get_frame(
        &self,
        frame_no: u32,
        p_frame: *mut u8,
        frame_len: u32,
    ) -> Result<Arc<Completion>> {
        self.pager.wal_get_frame(frame_no, p_frame, frame_len)
    }

    /// Flush dirty pages to disk.
    /// This will write the dirty pages to the WAL and then fsync the WAL.
    /// If the WAL size is over the checkpoint threshold, it will checkpoint the WAL to
    /// the database file and then fsync the database file.
    pub fn cacheflush(&self) -> Result<PagerCacheflushStatus> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        self.pager.cacheflush(self.wal_checkpoint_disabled.get())
    }

    pub fn clear_page_cache(&self) -> Result<()> {
        self.pager.clear_page_cache();
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<CheckpointResult> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        self.pager
            .wal_checkpoint(self.wal_checkpoint_disabled.get())
    }

    /// Close a connection and checkpoint.
    pub fn close(&self) -> Result<()> {
        if self.closed.get() {
            return Ok(());
        }
        self.closed.set(true);
        self.pager
            .checkpoint_shutdown(self.wal_checkpoint_disabled.get())
    }

    pub fn wal_disable_checkpoint(&self) {
        self.wal_checkpoint_disabled.set(true);
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid.get()
    }

    fn update_last_rowid(&self, rowid: i64) {
        self.last_insert_rowid.set(rowid);
    }

    pub fn set_changes(&self, nchange: i64) {
        self.last_change.set(nchange);
        let prev_total_changes = self.total_changes.get();
        self.total_changes.set(prev_total_changes + nchange);
    }

    pub fn total_changes(&self) -> i64 {
        self.total_changes.get()
    }

    pub fn get_cache_size(&self) -> i32 {
        self.cache_size.get()
    }
    pub fn set_cache_size(&self, size: i32) {
        self.cache_size.set(size);
    }

    pub fn get_capture_data_changes(&self) -> std::cell::Ref<'_, CaptureDataChangesMode> {
        self.capture_data_changes.borrow()
    }
    pub fn set_capture_data_changes(&self, opts: CaptureDataChangesMode) {
        self.capture_data_changes.replace(opts);
    }

    #[cfg(feature = "fs")]
    pub fn open_new(&self, path: &str, vfs: &str) -> Result<(Arc<dyn IO>, Arc<Database>)> {
        Database::open_with_vfs(&self._db, path, vfs)
    }

    pub fn list_vfs(&self) -> Vec<String> {
        let mut all_vfs = vec![String::from("memory")];
        #[cfg(feature = "fs")]
        {
            #[cfg(target_family = "unix")]
            {
                all_vfs.push("syscall".to_string());
            }
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            {
                all_vfs.push("io_uring".to_string());
            }
            all_vfs.extend(crate::ext::list_vfs_modules());
        }
        all_vfs
    }

    pub fn get_auto_commit(&self) -> bool {
        self.auto_commit.get()
    }

    pub fn parse_schema_rows(self: &Arc<Connection>) -> Result<()> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let rows = self.query("SELECT * FROM sqlite_schema")?;
        let mut schema = self.schema.borrow_mut();
        {
            let syms = self.syms.borrow();
            if let Err(LimboError::ExtensionError(e)) =
                parse_schema_rows(rows, &mut schema, &syms, None)
            {
                // this means that a vtab exists and we no longer have the module loaded. we print
                // a warning to the user to load the module
                eprintln!("Warning: {e}");
            }
        }
        Ok(())
    }

    // Clearly there is something to improve here, Vec<Vec<Value>> isn't a couple of tea
    /// Query the current rows/values of `pragma_name`.
    pub fn pragma_query(self: &Arc<Connection>, pragma_name: &str) -> Result<Vec<Vec<Value>>> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let pragma = format!("PRAGMA {pragma_name}");
        let mut stmt = self.prepare(pragma)?;
        let mut results = Vec::new();
        loop {
            match stmt.step()? {
                vdbe::StepResult::Row => {
                    let row: Vec<Value> = stmt.row().unwrap().get_values().cloned().collect();
                    results.push(row);
                }
                vdbe::StepResult::Interrupt | vdbe::StepResult::Busy => {
                    return Err(LimboError::Busy);
                }
                _ => break,
            }
        }

        Ok(results)
    }

    /// Set a new value to `pragma_name`.
    ///
    /// Some pragmas will return the updated value which cannot be retrieved
    /// with this method.
    pub fn pragma_update<V: Display>(
        self: &Arc<Connection>,
        pragma_name: &str,
        pragma_value: V,
    ) -> Result<Vec<Vec<Value>>> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let pragma = format!("PRAGMA {pragma_name} = {pragma_value}");
        let mut stmt = self.prepare(pragma)?;
        let mut results = Vec::new();
        loop {
            match stmt.step()? {
                vdbe::StepResult::Row => {
                    let row: Vec<Value> = stmt.row().unwrap().get_values().cloned().collect();
                    results.push(row);
                }
                vdbe::StepResult::Interrupt | vdbe::StepResult::Busy => {
                    return Err(LimboError::Busy);
                }
                _ => break,
            }
        }

        Ok(results)
    }

    /// Query the current value(s) of `pragma_name` associated to
    /// `pragma_value`.
    ///
    /// This method can be used with query-only pragmas which need an argument
    /// (e.g. `table_info('one_tbl')`) or pragmas which returns value(s)
    /// (e.g. `integrity_check`).
    pub fn pragma<V: Display>(
        self: &Arc<Connection>,
        pragma_name: &str,
        pragma_value: V,
    ) -> Result<Vec<Vec<Value>>> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let pragma = format!("PRAGMA {pragma_name}({pragma_value})");
        let mut stmt = self.prepare(pragma)?;
        let mut results = Vec::new();
        loop {
            match stmt.step()? {
                vdbe::StepResult::Row => {
                    let row: Vec<Value> = stmt.row().unwrap().get_values().cloned().collect();
                    results.push(row);
                }
                vdbe::StepResult::Interrupt | vdbe::StepResult::Busy => {
                    return Err(LimboError::Busy);
                }
                _ => break,
            }
        }

        Ok(results)
    }
}

pub struct Statement {
    program: Rc<vdbe::Program>,
    state: vdbe::ProgramState,
    mv_store: Option<Rc<MvStore>>,
    pager: Rc<Pager>,
}

impl Statement {
    pub fn new(
        program: Rc<vdbe::Program>,
        mv_store: Option<Rc<MvStore>>,
        pager: Rc<Pager>,
    ) -> Self {
        let state = vdbe::ProgramState::new(program.max_registers, program.cursor_ref.len());
        Self {
            program,
            state,
            mv_store,
            pager,
        }
    }

    pub fn set_mv_tx_id(&mut self, mv_tx_id: Option<u64>) {
        self.state.mv_tx_id = mv_tx_id;
    }

    pub fn interrupt(&mut self) {
        self.state.interrupt();
    }

    pub fn step(&mut self) -> Result<StepResult> {
        self.program
            .step(&mut self.state, self.mv_store.clone(), self.pager.clone())
    }

    pub fn run_once(&self) -> Result<()> {
        let res = self.pager.io.run_once();
        if res.is_err() {
            let state = self.program.connection.transaction_state.get();
            if let TransactionState::Write { schema_did_change } = state {
                self.pager
                    .rollback(schema_did_change, &self.program.connection)?
            }
        }
        res
    }

    pub fn num_columns(&self) -> usize {
        self.program.result_columns.len()
    }

    pub fn get_column_name(&self, idx: usize) -> Cow<str> {
        let column = &self.program.result_columns.get(idx).expect("No column");
        match column.name(&self.program.table_references) {
            Some(name) => Cow::Borrowed(name),
            None => Cow::Owned(column.expr.to_string()),
        }
    }

    pub fn parameters(&self) -> &parameters::Parameters {
        &self.program.parameters
    }

    pub fn parameters_count(&self) -> usize {
        self.program.parameters.count()
    }

    pub fn bind_at(&mut self, index: NonZero<usize>, value: Value) {
        self.state.bind_at(index, value);
    }

    pub fn reset(&mut self) {
        self.state.reset();
    }

    pub fn row(&self) -> Option<&Row> {
        self.state.result_row.as_ref()
    }

    pub fn explain(&self) -> String {
        self.program.explain()
    }
}

pub type Row = vdbe::Row;

pub type StepResult = vdbe::StepResult;

#[derive(Default)]
pub struct SymbolTable {
    pub functions: HashMap<String, Rc<function::ExternalFunc>>,
    pub vtabs: HashMap<String, Rc<VirtualTable>>,
    pub vtab_modules: HashMap<String, Rc<crate::ext::VTabImpl>>,
}

impl std::fmt::Debug for SymbolTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SymbolTable")
            .field("functions", &self.functions)
            .finish()
    }
}

fn is_shared_library(path: &std::path::Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext == "so" || ext == "dylib" || ext == "dll")
}

pub fn resolve_ext_path(extpath: &str) -> Result<std::path::PathBuf> {
    let path = std::path::Path::new(extpath);
    if !path.exists() {
        if is_shared_library(path) {
            return Err(LimboError::ExtensionError(format!(
                "Extension file not found: {extpath}"
            )));
        };
        let maybe = path.with_extension(std::env::consts::DLL_EXTENSION);
        maybe
            .exists()
            .then_some(maybe)
            .ok_or(LimboError::ExtensionError(format!(
                "Extension file not found: {extpath}"
            )))
    } else {
        Ok(path.to_path_buf())
    }
}

impl SymbolTable {
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
            vtabs: HashMap::new(),
            vtab_modules: HashMap::new(),
        }
    }
    pub fn resolve_function(
        &self,
        name: &str,
        _arg_count: usize,
    ) -> Option<Rc<function::ExternalFunc>> {
        self.functions.get(name).cloned()
    }
}

pub struct QueryRunner<'a> {
    parser: Parser<'a>,
    conn: &'a Arc<Connection>,
    statements: &'a [u8],
    last_offset: usize,
}

impl<'a> QueryRunner<'a> {
    pub(crate) fn new(conn: &'a Arc<Connection>, statements: &'a [u8]) -> Self {
        Self {
            parser: Parser::new(statements),
            conn,
            statements,
            last_offset: 0,
        }
    }
}

impl Iterator for QueryRunner<'_> {
    type Item = Result<Option<Statement>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.parser.next() {
            Ok(Some(cmd)) => {
                let byte_offset_end = self.parser.offset();
                let input = str::from_utf8(&self.statements[self.last_offset..byte_offset_end])
                    .unwrap()
                    .trim();
                self.last_offset = byte_offset_end;
                Some(self.conn.run_cmd(cmd, input))
            }
            Ok(None) => None,
            Err(err) => {
                self.parser.finalize();
                Some(Result::Err(LimboError::from(err)))
            }
        }
    }
}
