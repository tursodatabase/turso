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
mod state_machine;
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

use crate::translate::optimizer::optimize_plan;
use crate::translate::pragma::TURSO_CDC_DEFAULT_TABLE_NAME;
#[cfg(all(feature = "fs", feature = "conn_raw_api"))]
use crate::types::WalFrameInfo;
#[cfg(feature = "fs")]
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
use std::{
    borrow::Cow,
    cell::{Cell, RefCell, UnsafeCell},
    collections::HashMap,
    fmt::{self, Display},
    io::Write,
    num::NonZero,
    ops::Deref,
    rc::Rc,
    sync::{Arc, LazyLock, Mutex, Weak},
};
#[cfg(feature = "fs")]
use storage::database::DatabaseFile;
use storage::page_cache::DumbLruPageCache;
use storage::pager::{AtomicDbState, DbState};
use storage::sqlite3_ondisk::PageSize;
pub use storage::{
    buffer_pool::BufferPool,
    database::DatabaseStorage,
    pager::PageRef,
    pager::{Page, Pager},
    wal::{CheckpointMode, CheckpointResult, Wal, WalFile, WalFileShared},
};
use tracing::{instrument, Level};
use translate::select::prepare_select_plan;
use turso_sqlite3_parser::{ast, ast::Cmd, lexer::sql::Parser};
use types::IOResult;
pub use types::RefValue;
pub use types::Value;
use util::{parse_schema_rows, IOExt as _};
use vdbe::builder::QueryMode;
use vdbe::builder::TableRefIdCounter;

pub type Result<T, E = LimboError> = std::result::Result<T, E>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TransactionState {
    Write { schema_did_change: bool },
    Read,
    PendingUpgrade,
    None,
}

pub(crate) type MvStore = mvcc::MvStore<mvcc::LocalClock>;

pub(crate) type MvCursor = mvcc::cursor::MvccLazyCursor<mvcc::LocalClock>;

/// The database manager ensures that there is a single, shared
/// `Database` object per a database file. We need because it is not safe
/// to have multiple independent WAL files open because coordination
/// happens at process-level POSIX file advisory locks.
static DATABASE_MANAGER: LazyLock<Mutex<HashMap<String, Weak<Database>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The `Database` object contains per database file state that is shared
/// between multiple connections.
pub struct Database {
    mv_store: Option<Arc<MvStore>>,
    schema: Mutex<Arc<Schema>>,
    db_file: Arc<dyn DatabaseStorage>,
    path: String,
    io: Arc<dyn IO>,
    // Shared structures of a Database are the parts that are common to multiple threads that might
    // create DB connections.
    _shared_page_cache: Arc<RwLock<DumbLruPageCache>>,
    maybe_shared_wal: RwLock<Option<Arc<UnsafeCell<WalFileShared>>>>,
    db_state: Arc<AtomicDbState>,
    init_lock: Arc<Mutex<()>>,
    open_flags: OpenFlags,
    builtin_syms: RefCell<SymbolTable>,
}

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("Database");
        debug_struct
            .field("path", &self.path)
            .field("open_flags", &self.open_flags);

        // Database state information
        let db_state_value = match self.db_state.get() {
            DbState::Uninitialized => "uninitialized".to_string(),
            DbState::Initializing => "initializing".to_string(),
            DbState::Initialized => "initialized".to_string(),
        };
        debug_struct.field("db_state", &db_state_value);

        let mv_store_status = if self.mv_store.is_some() {
            "present"
        } else {
            "none"
        };
        debug_struct.field("mv_store", &mv_store_status);

        let init_lock_status = if self.init_lock.try_lock().is_ok() {
            "unlocked"
        } else {
            "locked"
        };
        debug_struct.field("init_lock", &init_lock_status);

        let wal_status = match self.maybe_shared_wal.try_read().as_deref() {
            Some(Some(_)) => "present",
            Some(None) => "none",
            None => "locked_for_write",
        };
        debug_struct.field("wal_state", &wal_status);

        // Page cache info (just basic stats, not full contents)
        let cache_info = match self._shared_page_cache.try_read() {
            Some(cache) => format!("( capacity {}, used: {} )", cache.capacity(), cache.len()),
            None => "locked".to_string(),
        };
        debug_struct.field("page_cache", &cache_info);

        debug_struct.finish()
    }
}

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
        if path == ":memory:" {
            return Self::do_open_with_flags(io, path, db_file, flags, enable_mvcc, enable_indexes);
        }

        let mut registry = DATABASE_MANAGER.lock().unwrap();

        let canonical_path = std::fs::canonicalize(path)
            .ok()
            .and_then(|p| p.to_str().map(|s| s.to_string()))
            .unwrap_or_else(|| path.to_string());

        if let Some(db) = registry.get(&canonical_path).and_then(Weak::upgrade) {
            return Ok(db);
        }
        let db = Self::do_open_with_flags(io, path, db_file, flags, enable_mvcc, enable_indexes)?;
        registry.insert(canonical_path, Arc::downgrade(&db));
        Ok(db)
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn do_open_with_flags(
        io: Arc<dyn IO>,
        path: &str,
        db_file: Arc<dyn DatabaseStorage>,
        flags: OpenFlags,
        enable_mvcc: bool,
        enable_indexes: bool,
    ) -> Result<Arc<Database>> {
        let wal_path = format!("{path}-wal");
        let maybe_shared_wal = WalFileShared::open_shared_if_exists(&io, wal_path.as_str())?;

        let mv_store = if enable_mvcc {
            Some(Arc::new(MvStore::new(
                mvcc::LocalClock::new(),
                mvcc::persistent_storage::Storage::new_noop(),
            )))
        } else {
            None
        };

        let db_size = db_file.size()?;
        let db_state = if db_size == 0 {
            DbState::Uninitialized
        } else {
            DbState::Initialized
        };

        let shared_page_cache = Arc::new(RwLock::new(DumbLruPageCache::default()));
        let syms = SymbolTable::new();
        let db = Arc::new(Database {
            mv_store,
            path: path.to_string(),
            schema: Mutex::new(Arc::new(Schema::new(enable_indexes))),
            _shared_page_cache: shared_page_cache.clone(),
            maybe_shared_wal: RwLock::new(maybe_shared_wal),
            db_file,
            builtin_syms: syms.into(),
            io: io.clone(),
            open_flags: flags,
            db_state: Arc::new(AtomicDbState::new(db_state)),
            init_lock: Arc::new(Mutex::new(())),
        });
        db.register_global_builtin_extensions()
            .expect("unable to register global extensions");

        // Check: https://github.com/tursodatabase/turso/pull/1761#discussion_r2154013123
        if db_state.is_initialized() {
            // parse schema
            let conn = db.connect()?;

            let syms = conn.syms.borrow();
            let pager = conn.pager.borrow().clone();

            db.with_schema_mut(|schema| {
                let header_schema_cookie = pager
                    .io
                    .block(|| pager.with_header(|header| header.schema_cookie.get()))?;
                schema.schema_version = header_schema_cookie;
                let result = schema
                    .make_from_btree(None, pager.clone(), &syms)
                    .or_else(|e| {
                        pager.end_read_tx()?;
                        Err(e)
                    });
                if let Err(LimboError::ExtensionError(e)) = result {
                    // this means that a vtab exists and we no longer have the module loaded. we print
                    // a warning to the user to load the module
                    eprintln!("Warning: {e}");
                }
                Ok(())
            })?;
        }
        Ok(db)
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn connect(self: &Arc<Database>) -> Result<Arc<Connection>> {
        let pager = self.init_pager(None)?;

        let page_size = pager
            .io
            .block(|| pager.with_header(|header| header.page_size))
            .unwrap_or_default()
            .get();

        let default_cache_size = pager
            .io
            .block(|| pager.with_header(|header| header.default_page_cache_size))
            .unwrap_or_default()
            .get();

        let conn = Arc::new(Connection {
            _db: self.clone(),
            pager: RefCell::new(Rc::new(pager)),
            schema: RefCell::new(
                self.schema
                    .lock()
                    .map_err(|_| LimboError::SchemaLocked)?
                    .clone(),
            ),
            database_schemas: RefCell::new(std::collections::HashMap::new()),
            auto_commit: Cell::new(true),
            mv_transactions: RefCell::new(Vec::new()),
            transaction_state: Cell::new(TransactionState::None),
            last_insert_rowid: Cell::new(0),
            last_change: Cell::new(0),
            total_changes: Cell::new(0),
            syms: RefCell::new(SymbolTable::new()),
            _shared_cache: false,
            cache_size: Cell::new(default_cache_size),
            page_size: Cell::new(page_size),
            wal_checkpoint_disabled: Cell::new(false),
            capture_data_changes: RefCell::new(CaptureDataChangesMode::Off),
            closed: Cell::new(false),
            attached_databases: RefCell::new(DatabaseCatalog::new()),
        });
        let builtin_syms = self.builtin_syms.borrow();
        // add built-in extensions symbols to the connection to prevent having to load each time
        conn.syms.borrow_mut().extend(&builtin_syms);
        Ok(conn)
    }

    pub fn is_readonly(&self) -> bool {
        self.open_flags.contains(OpenFlags::ReadOnly)
    }

    fn init_pager(&self, page_size: Option<usize>) -> Result<Pager> {
        // Open existing WAL file if present
        let mut maybe_shared_wal = self.maybe_shared_wal.write();
        if let Some(shared_wal) = maybe_shared_wal.clone() {
            let size = match page_size {
                None => unsafe { (*shared_wal.get()).page_size() as usize },
                Some(size) => size,
            };
            let buffer_pool = Arc::new(BufferPool::new(Some(size)));

            let db_state = self.db_state.clone();
            let wal = Rc::new(RefCell::new(WalFile::new(
                self.io.clone(),
                shared_wal,
                buffer_pool.clone(),
            )));
            let pager = Pager::new(
                self.db_file.clone(),
                Some(wal),
                self.io.clone(),
                Arc::new(RwLock::new(DumbLruPageCache::default())),
                buffer_pool.clone(),
                db_state,
                self.init_lock.clone(),
            )?;
            return Ok(pager);
        }

        let buffer_pool = Arc::new(BufferPool::new(page_size));
        // No existing WAL; create one.
        let db_state = self.db_state.clone();
        let mut pager = Pager::new(
            self.db_file.clone(),
            None,
            self.io.clone(),
            Arc::new(RwLock::new(DumbLruPageCache::default())),
            buffer_pool.clone(),
            db_state,
            Arc::new(Mutex::new(())),
        )?;

        let size = match page_size {
            Some(size) => size as u32,
            None => {
                let size = pager
                    .io
                    .block(|| pager.with_header(|header| header.page_size))
                    .unwrap_or_default()
                    .get();
                buffer_pool.set_page_size(size as usize);
                size
            }
        };

        let wal_path = format!("{}-wal", self.path);
        let file = self.io.open_file(&wal_path, OpenFlags::Create, false)?;
        let real_shared_wal = WalFileShared::new_shared(size, &self.io, file)?;
        // Modify Database::maybe_shared_wal to point to the new WAL file so that other connections
        // can open the existing WAL.
        *maybe_shared_wal = Some(real_shared_wal.clone());
        let wal = Rc::new(RefCell::new(WalFile::new(
            self.io.clone(),
            real_shared_wal,
            buffer_pool,
        )));
        pager.set_wal(wal);

        Ok(pager)
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

    #[inline]
    pub(crate) fn with_schema_mut<T>(&self, f: impl FnOnce(&mut Schema) -> Result<T>) -> Result<T> {
        let mut schema_ref = self.schema.lock().map_err(|_| LimboError::SchemaLocked)?;
        let schema = Arc::make_mut(&mut *schema_ref);
        f(schema)
    }
    pub(crate) fn clone_schema(&self) -> Result<Arc<Schema>> {
        let schema = self.schema.lock().map_err(|_| LimboError::SchemaLocked)?;
        Ok(schema.clone())
    }

    pub(crate) fn update_schema_if_newer(&self, another: Arc<Schema>) -> Result<()> {
        let mut schema = self.schema.lock().map_err(|_| LimboError::SchemaLocked)?;
        if schema.schema_version < another.schema_version {
            tracing::debug!(
                "DB schema is outdated: {} < {}",
                schema.schema_version,
                another.schema_version
            );
            *schema = another;
        } else {
            tracing::debug!(
                "DB schema is up to date: {} >= {}",
                schema.schema_version,
                another.schema_version
            );
        }
        Ok(())
    }

    pub fn get_mv_store(&self) -> Option<&Arc<MvStore>> {
        self.mv_store.as_ref()
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

// Optimized for fast get() operations and supports unlimited attached databases.
struct DatabaseCatalog {
    name_to_index: HashMap<String, usize>,
    allocated: Vec<u64>,
    index_to_data: HashMap<usize, (Arc<Database>, Rc<Pager>)>,
}

#[allow(unused)]
impl DatabaseCatalog {
    fn new() -> Self {
        Self {
            name_to_index: HashMap::new(),
            index_to_data: HashMap::new(),
            allocated: vec![3], // 0 | 1, as those are reserved for main and temp
        }
    }

    fn get_database_by_index(&self, index: usize) -> Option<Arc<Database>> {
        self.index_to_data
            .get(&index)
            .map(|(db, _pager)| db.clone())
    }

    fn get_database_by_name(&self, s: &str) -> Option<(usize, Arc<Database>)> {
        match self.name_to_index.get(s) {
            None => None,
            Some(idx) => self
                .index_to_data
                .get(idx)
                .map(|(db, _pager)| (*idx, db.clone())),
        }
    }

    fn get_pager_by_index(&self, idx: &usize) -> Rc<Pager> {
        let (_db, pager) = self
            .index_to_data
            .get(idx)
            .expect("If we are looking up a database by index, it must exist.");
        pager.clone()
    }

    fn add(&mut self, s: &str) -> usize {
        assert_eq!(self.name_to_index.get(s), None);

        let index = self.allocate_index();
        self.name_to_index.insert(s.to_string(), index);
        index
    }

    fn insert(&mut self, s: &str, data: (Arc<Database>, Rc<Pager>)) -> usize {
        let idx = self.add(s);
        self.index_to_data.insert(idx, data);
        idx
    }

    fn remove(&mut self, s: &str) -> Option<usize> {
        if let Some(index) = self.name_to_index.remove(s) {
            // Should be impossible to remove main or temp.
            assert!(index >= 2);
            self.deallocate_index(index);
            self.index_to_data.remove(&index);
            Some(index)
        } else {
            None
        }
    }

    #[inline(always)]
    fn deallocate_index(&mut self, index: usize) {
        let word_idx = index / 64;
        let bit_idx = index % 64;

        if word_idx < self.allocated.len() {
            self.allocated[word_idx] &= !(1u64 << bit_idx);
        }
    }

    fn allocate_index(&mut self) -> usize {
        for word_idx in 0..self.allocated.len() {
            let word = self.allocated[word_idx];

            if word != u64::MAX {
                let free_bit = Self::find_first_zero_bit(word);
                let index = word_idx * 64 + free_bit;

                self.allocated[word_idx] |= 1u64 << free_bit;

                return index;
            }
        }

        // Need to expand bitmap
        let word_idx = self.allocated.len();
        self.allocated.push(1u64); // Mark first bit as allocated
        word_idx * 64
    }

    #[inline(always)]
    fn find_first_zero_bit(word: u64) -> usize {
        // Invert to find first zero as first one
        let inverted = !word;

        // Use trailing zeros count (compiles to single instruction on most CPUs)
        inverted.trailing_zeros() as usize
    }
}

pub struct Connection {
    _db: Arc<Database>,
    pager: RefCell<Rc<Pager>>,
    schema: RefCell<Arc<Schema>>,
    /// Per-database schema cache (database_index -> schema)
    /// Loaded lazily to avoid copying all schemas on connection open
    database_schemas: RefCell<std::collections::HashMap<usize, Arc<Schema>>>,
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
    /// page size used for an uninitialized database or the next vacuum command.
    /// it's not always equal to the current page size of the database
    page_size: Cell<u32>,
    wal_checkpoint_disabled: Cell<bool>,
    capture_data_changes: RefCell<CaptureDataChangesMode>,
    closed: Cell<bool>,
    /// Attached databases
    attached_databases: RefCell<DatabaseCatalog>,
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
        self.maybe_update_schema()?;
        let pager = self.pager.borrow().clone();
        match cmd {
            Cmd::Stmt(stmt) => {
                let program = translate::translate(
                    self.schema.borrow().deref(),
                    stmt,
                    pager.clone(),
                    self.clone(),
                    &syms,
                    QueryMode::Normal,
                    input,
                )?;
                Ok(Statement::new(program, self._db.mv_store.clone(), pager))
            }
            _ => unreachable!(),
        }
    }

    /// Parse schema from scratch if version of schema for the connection differs from the schema cookie in the root page
    /// This function must be called outside of any transaction because internally it will start transaction session by itself
    #[allow(dead_code)]
    fn maybe_reparse_schema(self: &Arc<Connection>) -> Result<()> {
        let pager = self.pager.borrow().clone();
        let conn_schema_version = self.schema.borrow().schema_version;

        // first, quickly read schema_version from the root page in order to check if schema changed
        pager.begin_read_tx()?;
        let db_schema_version = pager
            .io
            .block(|| pager.with_header(|header| header.schema_cookie));

        let db_schema_version = match db_schema_version {
            Ok(db_schema_version) => db_schema_version.get(),
            Err(LimboError::Page1NotAlloc) => {
                // this means this is a fresh db, so return a schema version of 0
                0
            }
            Err(err) => {
                pager.end_read_tx().expect("read txn must be finished");
                return Err(err);
            }
        };
        turso_assert!(
            conn_schema_version <= db_schema_version,
            "connection schema_version can't be larger than db schema_version: {} vs {}",
            conn_schema_version,
            db_schema_version
        );
        pager.end_read_tx().expect("read txn must be finished");

        // if schema_versions matches - exit early
        if conn_schema_version == db_schema_version {
            return Ok(());
        }
        // maybe_reparse_schema must be called outside of any transaction
        turso_assert!(
            self.transaction_state.get() == TransactionState::None,
            "unexpected start transaction"
        );
        self.reparse_schema()
    }

    fn reparse_schema(self: &Arc<Connection>) -> Result<()> {
        let pager = self.pager.borrow().clone();

        // reparse logic extracted to the function in order to not accidentally propagate error from it before closing transaction
        let reparse = || -> Result<()> {
            // read cookie before consuming statement program - otherwise we can end up reading cookie with closed transaction state
            let cookie = pager
                .io
                .block(|| pager.with_header(|header| header.schema_cookie))?
                .get();

            // create fresh schema as some objects can be deleted
            let mut fresh = Schema::new(self.schema.borrow().indexes_enabled);
            fresh.schema_version = cookie;

            // TODO: this is hack to avoid a cyclical problem with schema reprepare
            // The problem here is that we prepare a statement here, but when the statement tries
            // to execute it, it first checks the schema cookie to see if it needs to reprepare the statement.
            // But in this occasion it will always reprepare, and we get an error. So we trick the statement by swapping our schema
            // with a new clean schema that has the same header cookie.
            self.with_schema_mut(|schema| {
                *schema = fresh.clone();
            });

            let stmt = self.prepare("SELECT * FROM sqlite_schema")?;

            // TODO: This function below is synchronous, make it async
            parse_schema_rows(stmt, &mut fresh, &self.syms.borrow(), None)?;

            self.with_schema_mut(|schema| {
                *schema = fresh;
            });
            Result::Ok(())
        };

        // start read transaction manually, because we will read schema cookie once again and
        // we must be sure that it will consistent with schema content
        //
        // from now on we must be very careful with errors propagation
        // in order to not accidentally keep read transaction opened
        pager.begin_read_tx()?;
        self.transaction_state.replace(TransactionState::Read);

        let reparse_result = reparse();

        let previous = self.transaction_state.replace(TransactionState::None);
        turso_assert!(
            matches!(previous, TransactionState::None | TransactionState::Read),
            "unexpected end transaction state"
        );
        // close opened transaction if it was kept open
        // (in most cases, it will be automatically closed if stmt was executed properly)
        if previous == TransactionState::Read {
            pager.end_read_tx().expect("read txn must be finished");
        }
        // now we can safely propagate error after ensured that transaction state is reset
        reparse_result?;

        let schema = self.schema.borrow().clone();
        self._db.update_schema_if_newer(schema)?;
        Ok(())
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn prepare_execute_batch(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        if sql.as_ref().is_empty() {
            return Err(LimboError::InvalidArgument(
                "The supplied SQL string contains no statements".to_string(),
            ));
        }
        let sql = sql.as_ref();
        tracing::trace!("Preparing and executing batch: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        while let Some(cmd) = parser.next()? {
            let syms = self.syms.borrow();
            let pager = self.pager.borrow().clone();
            let byte_offset_end = parser.offset();
            let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
                .unwrap()
                .trim();
            match cmd {
                Cmd::Stmt(stmt) => {
                    let program = translate::translate(
                        self.schema.borrow().deref(),
                        stmt,
                        pager.clone(),
                        self.clone(),
                        &syms,
                        QueryMode::Normal,
                        input,
                    )?;

                    let mut state =
                        vdbe::ProgramState::new(program.max_registers, program.cursor_ref.len());
                    loop {
                        let res =
                            program.step(&mut state, self._db.mv_store.clone(), pager.clone())?;
                        if matches!(res, StepResult::Done) {
                            break;
                        }
                        self.run_once()?;
                    }
                }
                _ => unreachable!(),
            }
        }
        Ok(())
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
        let pager = self.pager.borrow().clone();
        match cmd {
            Cmd::Stmt(ref stmt) | Cmd::Explain(ref stmt) => {
                let program = translate::translate(
                    self.schema.borrow().deref(),
                    stmt.clone(),
                    pager.clone(),
                    self.clone(),
                    &syms,
                    cmd.into(),
                    input,
                )?;
                let stmt = Statement::new(program, self._db.mv_store.clone(), pager);
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
                            &self.clone(),
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
            let pager = self.pager.borrow().clone();
            let byte_offset_end = parser.offset();
            let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
                .unwrap()
                .trim();
            self.maybe_update_schema()?;
            match cmd {
                Cmd::Explain(stmt) => {
                    let program = translate::translate(
                        self.schema.borrow().deref(),
                        stmt,
                        pager,
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
                        pager.clone(),
                        self.clone(),
                        &syms,
                        QueryMode::Normal,
                        input,
                    )?;

                    let mut state =
                        vdbe::ProgramState::new(program.max_registers, program.cursor_ref.len());
                    loop {
                        let res =
                            program.step(&mut state, self._db.mv_store.clone(), pager.clone())?;
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
        if let Err(ref e) = res {
            vdbe::handle_program_error(&self.pager.borrow(), self, e)?;
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
        Ok((io, conn))
    }

    #[cfg(feature = "fs")]
    fn from_uri_attached(uri: &str, use_indexes: bool, use_mvcc: bool) -> Result<Arc<Database>> {
        let mut opts = OpenOptions::parse(uri)?;
        // FIXME: for now, only support read only attach
        opts.mode = OpenMode::ReadOnly;
        let flags = opts.get_flags()?;
        let (_io, db) =
            Database::open_new(&opts.path, opts.vfs.as_ref(), flags, use_indexes, use_mvcc)?;
        if let Some(modeof) = opts.modeof {
            let perms = std::fs::metadata(modeof)?;
            std::fs::set_permissions(&opts.path, perms.permissions())?;
        }
        Ok(db)
    }

    pub fn maybe_update_schema(&self) -> Result<()> {
        let current_schema_version = self.schema.borrow().schema_version;
        let schema = self
            ._db
            .schema
            .lock()
            .map_err(|_| LimboError::SchemaLocked)?;
        if matches!(self.transaction_state.get(), TransactionState::None)
            && current_schema_version != schema.schema_version
        {
            self.schema.replace(schema.clone());
        }

        Ok(())
    }

    /// Try to read page with given ID with fixed WAL watermark position
    /// This method return false if page is not found (so, this is probably new page created after watermark position which wasn't checkpointed to the DB file yet)
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn try_wal_watermark_read_page(
        &self,
        page_idx: u32,
        page: &mut [u8],
        frame_watermark: Option<u64>,
    ) -> Result<bool> {
        let pager = self.pager.borrow();
        let (page_ref, c) = match pager.read_page_no_cache(page_idx as usize, frame_watermark, true)
        {
            Ok(result) => result,
            // on windows, zero read will trigger UnexpectedEof
            #[cfg(target_os = "windows")]
            Err(LimboError::IOError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(false)
            }
            Err(err) => return Err(err),
        };

        pager.io.wait_for_completion(c)?;

        let content = page_ref.get_contents();
        // empty read - attempt to read absent page
        if content.buffer.borrow().is_empty() {
            return Ok(false);
        }
        page.copy_from_slice(content.as_ptr());
        Ok(true)
    }

    /// Return unique set of page numbers changes after WAL watermark position in the current WAL session
    /// (so, if concurrent connection wrote something to the WAL - this method will not see this change)
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>> {
        self.pager.borrow().wal_changed_pages_after(frame_watermark)
    }

    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_frame_count(&self) -> Result<u64> {
        self.pager.borrow().wal_frame_count()
    }

    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_get_frame(&self, frame_no: u64, frame: &mut [u8]) -> Result<WalFrameInfo> {
        use crate::storage::sqlite3_ondisk::parse_wal_frame_header;

        let c = self.pager.borrow().wal_get_frame(frame_no, frame)?;
        self._db.io.wait_for_completion(c)?;
        let (header, _) = parse_wal_frame_header(frame);
        Ok(WalFrameInfo {
            page_no: header.page_number,
            db_size: header.db_size,
        })
    }

    /// Insert `frame` (header included) at the position `frame_no` in the WAL
    /// If WAL already has frame at that position - turso-db will compare content of the page and either report conflict or return OK
    /// If attempt to write frame at the position `frame_no` will create gap in the WAL - method will return error
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_insert_frame(&self, frame_no: u64, frame: &[u8]) -> Result<WalFrameInfo> {
        self.pager.borrow().wal_insert_frame(frame_no, frame)
    }

    /// Start WAL session by initiating read+write transaction for this connection
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_insert_begin(&self) -> Result<()> {
        let pager = self.pager.borrow();
        match pager.begin_read_tx()? {
            result::LimboResult::Busy => return Err(LimboError::Busy),
            result::LimboResult::Ok => {}
        }
        match pager.io.block(|| pager.begin_write_tx()).inspect_err(|_| {
            pager.end_read_tx().expect("read txn must be closed");
        })? {
            result::LimboResult::Busy => {
                pager.end_read_tx().expect("read txn must be closed");
                return Err(LimboError::Busy);
            }
            result::LimboResult::Ok => {}
        }
        Ok(())
    }

    /// Finish WAL session by ending read+write transaction taken in the [Self::wal_insert_begin] method
    /// All frames written after last commit frame (db_size > 0) within the session will be rolled back
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_insert_end(self: &Arc<Connection>) -> Result<()> {
        {
            let pager = self.pager.borrow();

            let Some(wal) = pager.wal.as_ref() else {
                return Err(LimboError::InternalError(
                    "wal_insert_end called without a wal".to_string(),
                ));
            };

            {
                let wal = wal.borrow_mut();
                wal.end_write_tx();
                wal.end_read_tx();
            }
            // remove all non-commited changes in case if WAL session left some suffix without commit frame
            pager.rollback(false, self, true)?;
        }

        // let's re-parse schema from scratch if schema cookie changed compared to the our in-memory view of schema
        self.maybe_reparse_schema()?;
        Ok(())
    }

    /// Flush dirty pages to disk.
    pub fn cacheflush(&self) -> Result<IOResult<()>> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        self.pager.borrow().cacheflush()
    }

    pub fn clear_page_cache(&self) -> Result<()> {
        self.pager.borrow().clear_page_cache();
        Ok(())
    }

    pub fn checkpoint(&self, mode: CheckpointMode) -> Result<CheckpointResult> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        self.pager
            .borrow()
            .wal_checkpoint(self.wal_checkpoint_disabled.get(), mode)
    }

    /// Close a connection and checkpoint.
    pub fn close(&self) -> Result<()> {
        if self.closed.get() {
            return Ok(());
        }
        self.closed.set(true);

        match self.transaction_state.get() {
            TransactionState::Write { schema_did_change } => {
                while let IOResult::IO = self.pager.borrow().end_tx(
                    true, // rollback = true for close
                    schema_did_change,
                    self,
                    self.wal_checkpoint_disabled.get(),
                )? {
                    self.run_once()?;
                }
                self.transaction_state.set(TransactionState::None);
            }
            TransactionState::PendingUpgrade | TransactionState::Read => {
                self.pager.borrow().end_read_tx()?;
                self.transaction_state.set(TransactionState::None);
            }
            TransactionState::None => {
                // No active transaction
            }
        }

        self.pager
            .borrow()
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

    pub fn changes(&self) -> i64 {
        self.last_change.get()
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
    pub fn get_page_size(&self) -> u32 {
        self.page_size.get()
    }

    pub fn get_database_canonical_path(&self) -> String {
        if self._db.path == ":memory:" {
            // For in-memory databases, SQLite shows empty string
            String::new()
        } else {
            // For file databases, try show the full absolute path if that doesn't fail
            match std::fs::canonicalize(&self._db.path) {
                Ok(abs_path) => abs_path.to_string_lossy().to_string(),
                Err(_) => self._db.path.to_string(),
            }
        }
    }

    /// Check if a specific attached database is read only or not, by its index
    pub fn is_readonly(&self, index: usize) -> bool {
        if index == 0 {
            self._db.is_readonly()
        } else {
            let db = self
                .attached_databases
                .borrow()
                .get_database_by_index(index);
            db.expect("Should never have called this without being sure the database exists")
                .is_readonly()
        }
    }

    /// Reset the page size for the current connection.
    ///
    /// Specifying a new page size does not change the page size immediately.
    /// Instead, the new page size is remembered and is used to set the page size when the database
    /// is first created, if it does not already exist when the page_size pragma is issued,
    /// or at the next VACUUM command that is run on the same database connection while not in WAL mode.
    pub fn reset_page_size(&self, size: u32) -> Result<()> {
        if PageSize::new(size).is_none() {
            return Ok(());
        }

        self.page_size.set(size);
        if self._db.db_state.get() != DbState::Uninitialized {
            return Ok(());
        }

        *self._db.maybe_shared_wal.write() = None;
        let pager = self._db.init_pager(Some(size as usize))?;
        self.pager.replace(Rc::new(pager));
        self.pager.borrow().set_initial_page_size(size);

        Ok(())
    }

    #[cfg(feature = "fs")]
    pub fn open_new(&self, path: &str, vfs: &str) -> Result<(Arc<dyn IO>, Arc<Database>)> {
        Database::open_with_vfs(&self._db, path, vfs)
    }

    pub fn list_vfs(&self) -> Vec<String> {
        #[allow(unused_mut)]
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
        let rows = self
            .query("SELECT * FROM sqlite_schema")?
            .expect("query must be parsed to statement");
        let syms = self.syms.borrow();
        self.with_schema_mut(|schema| {
            if let Err(LimboError::ExtensionError(e)) = parse_schema_rows(rows, schema, &syms, None)
            {
                // this means that a vtab exists and we no longer have the module loaded. we print
                // a warning to the user to load the module
                eprintln!("Warning: {e}");
            }
        });
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

    #[inline]
    pub fn with_schema_mut<T>(&self, f: impl FnOnce(&mut Schema) -> T) -> T {
        let mut schema_ref = self.schema.borrow_mut();
        let schema = Arc::make_mut(&mut *schema_ref);
        f(schema)
    }

    pub fn is_db_initialized(&self) -> bool {
        self._db.db_state.is_initialized()
    }

    fn get_pager_from_database_index(&self, index: &usize) -> Rc<Pager> {
        if *index < 2 {
            self.pager.borrow().clone()
        } else {
            self.attached_databases.borrow().get_pager_by_index(index)
        }
    }

    #[cfg(feature = "fs")]
    fn is_attached(&self, alias: &str) -> bool {
        self.attached_databases
            .borrow()
            .name_to_index
            .contains_key(alias)
    }

    /// Attach a database file with the given alias name
    #[cfg(not(feature = "fs"))]
    pub(crate) fn attach_database(&self, _path: &str, _alias: &str) -> Result<()> {
        return Err(LimboError::InvalidArgument(format!(
            "attach not available in this build (no-fs)"
        )));
    }

    /// Attach a database file with the given alias name
    #[cfg(feature = "fs")]
    pub(crate) fn attach_database(&self, path: &str, alias: &str) -> Result<()> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }

        if self.is_attached(alias) {
            return Err(LimboError::InvalidArgument(format!(
                "database {alias} is already in use"
            )));
        }

        // Check for reserved database names
        if alias.eq_ignore_ascii_case("main") || alias.eq_ignore_ascii_case("temp") {
            return Err(LimboError::InvalidArgument(format!(
                "reserved name {alias} is already in use"
            )));
        }

        let use_indexes = self
            ._db
            .schema
            .lock()
            .map_err(|_| LimboError::SchemaLocked)?
            .indexes_enabled();
        let use_mvcc = self._db.mv_store.is_some();

        let db = Self::from_uri_attached(path, use_indexes, use_mvcc)?;
        let pager = Rc::new(db.init_pager(None)?);

        self.attached_databases
            .borrow_mut()
            .insert(alias, (db, pager));

        Ok(())
    }

    // Detach a database by alias name
    fn detach_database(&self, alias: &str) -> Result<()> {
        if self.closed.get() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }

        if alias == "main" || alias == "temp" {
            return Err(LimboError::InvalidArgument(format!(
                "cannot detach database: {alias}"
            )));
        }

        // Remove from attached databases
        let mut attached_dbs = self.attached_databases.borrow_mut();
        if attached_dbs.remove(alias).is_none() {
            return Err(LimboError::InvalidArgument(format!(
                "no such database: {alias}"
            )));
        }

        Ok(())
    }

    // Get an attached database by alias name
    fn get_attached_database(&self, alias: &str) -> Option<(usize, Arc<Database>)> {
        self.attached_databases.borrow().get_database_by_name(alias)
    }

    /// List all attached database aliases
    pub fn list_attached_databases(&self) -> Vec<String> {
        self.attached_databases
            .borrow()
            .name_to_index
            .keys()
            .cloned()
            .collect()
    }

    /// Resolve database ID from a qualified name
    pub(crate) fn resolve_database_id(&self, qualified_name: &ast::QualifiedName) -> Result<usize> {
        use crate::util::normalize_ident;

        // Check if this is a qualified name (database.table) or unqualified
        if let Some(db_name) = &qualified_name.db_name {
            let db_name_normalized = normalize_ident(db_name.as_str());

            if db_name_normalized.eq_ignore_ascii_case("main") {
                Ok(0)
            } else if db_name_normalized.eq_ignore_ascii_case("temp") {
                Ok(1)
            } else {
                // Look up attached database
                if let Some((idx, _attached_db)) = self.get_attached_database(&db_name_normalized) {
                    Ok(idx)
                } else {
                    Err(LimboError::InvalidArgument(format!(
                        "no such database: {db_name_normalized}"
                    )))
                }
            }
        } else {
            // Unqualified table name - use main database
            Ok(0)
        }
    }

    /// Access schema for a database using a closure pattern to avoid cloning
    pub(crate) fn with_schema<T>(&self, database_id: usize, f: impl FnOnce(&Schema) -> T) -> T {
        if database_id == 0 {
            // Main database - use connection's schema which should be kept in sync
            let schema = self.schema.borrow();
            f(&schema)
        } else if database_id == 1 {
            // Temp database - uses same schema as main for now, but this will change later.
            let schema = self.schema.borrow();
            f(&schema)
        } else {
            // Attached database - check cache first, then load from database
            let mut schemas = self.database_schemas.borrow_mut();

            if let Some(cached_schema) = schemas.get(&database_id) {
                return f(cached_schema);
            }

            // Schema not cached, load it lazily from the attached database
            let attached_dbs = self.attached_databases.borrow();
            let (db, _pager) = attached_dbs
                .index_to_data
                .get(&database_id)
                .expect("Database ID should be valid after resolve_database_id");

            let schema = db
                .schema
                .lock()
                .expect("Schema lock should not fail")
                .clone();

            // Cache the schema for future use
            schemas.insert(database_id, schema.clone());

            f(&schema)
        }
    }

    // Get the canonical path for a database given its Database object
    fn get_canonical_path_for_database(db: &Database) -> String {
        if db.path == ":memory:" {
            // For in-memory databases, SQLite shows empty string
            String::new()
        } else {
            // For file databases, try to show the full absolute path if that doesn't fail
            match std::fs::canonicalize(&db.path) {
                Ok(abs_path) => abs_path.to_string_lossy().to_string(),
                Err(_) => db.path.to_string(),
            }
        }
    }

    /// List all databases (main + attached) with their sequence numbers, names, and file paths
    /// Returns a vector of tuples: (seq_number, name, file_path)
    pub fn list_all_databases(&self) -> Vec<(usize, String, String)> {
        let mut databases = Vec::new();

        // Add main database (always seq=0, name="main")
        let main_path = Self::get_canonical_path_for_database(&self._db);
        databases.push((0, "main".to_string(), main_path));

        // Add attached databases
        let attached_dbs = self.attached_databases.borrow();
        for (alias, &seq_number) in attached_dbs.name_to_index.iter() {
            let file_path = if let Some((db, _pager)) = attached_dbs.index_to_data.get(&seq_number)
            {
                Self::get_canonical_path_for_database(db)
            } else {
                String::new()
            };
            databases.push((seq_number, alias.clone(), file_path));
        }

        // Sort by sequence number to ensure consistent ordering
        databases.sort_by_key(|&(seq, _, _)| seq);
        databases
    }

    pub fn get_pager(&self) -> Rc<Pager> {
        self.pager.borrow().clone()
    }
}

pub struct Statement {
    program: vdbe::Program,
    state: vdbe::ProgramState,
    mv_store: Option<Arc<MvStore>>,
    pager: Rc<Pager>,
    /// Whether the statement accesses the database.
    /// Used to determine whether we need to check for schema changes when
    /// starting a transaction.
    accesses_db: bool,
}

impl Statement {
    pub fn new(program: vdbe::Program, mv_store: Option<Arc<MvStore>>, pager: Rc<Pager>) -> Self {
        let accesses_db = program.accesses_db;
        let state = vdbe::ProgramState::new(program.max_registers, program.cursor_ref.len());
        Self {
            program,
            state,
            mv_store,
            pager,
            accesses_db,
        }
    }

    pub fn n_change(&self) -> i64 {
        self.program.n_change.get()
    }

    pub fn set_mv_tx_id(&mut self, mv_tx_id: Option<u64>) {
        self.state.mv_tx_id = mv_tx_id;
    }

    pub fn interrupt(&mut self) {
        self.state.interrupt();
    }

    pub fn step(&mut self) -> Result<StepResult> {
        if !self.accesses_db {
            return self
                .program
                .step(&mut self.state, self.mv_store.clone(), self.pager.clone());
        }

        const MAX_SCHEMA_RETRY: usize = 50;
        let mut res = self
            .program
            .step(&mut self.state, self.mv_store.clone(), self.pager.clone());
        for _ in 0..MAX_SCHEMA_RETRY {
            // Only reprepare if we still need to update schema
            if !matches!(res, Err(LimboError::SchemaUpdated)) {
                break;
            }
            self.reprepare()?;
            res = self
                .program
                .step(&mut self.state, self.mv_store.clone(), self.pager.clone());
        }

        res
    }

    fn reprepare(&mut self) -> Result<()> {
        let conn = self.program.connection.clone();
        *conn.schema.borrow_mut() = conn._db.clone_schema()?;
        self.program = {
            let mut parser = Parser::new(self.program.sql.as_bytes());
            let cmd = parser.next()?;
            let cmd = cmd.expect("Same SQL string should be able to be parsed");

            let syms = conn.syms.borrow();

            match cmd {
                Cmd::Stmt(stmt) => translate::translate(
                    conn.schema.borrow().deref(),
                    stmt,
                    self.pager.clone(),
                    conn.clone(),
                    &syms,
                    QueryMode::Normal,
                    &self.program.sql,
                )?,
                Cmd::Explain(_stmt) => todo!(),
                Cmd::ExplainQueryPlan(_stmt) => todo!(),
            }
        };
        // Save parameters before they are reset
        let parameters = std::mem::take(&mut self.state.parameters);
        self.reset();
        // Load the parameters back into the state
        self.state.parameters = parameters;
        Ok(())
    }

    pub fn run_once(&self) -> Result<()> {
        let res = self.pager.io.run_once();
        if res.is_err() {
            let state = self.program.connection.transaction_state.get();
            if let TransactionState::Write { schema_did_change } = state {
                let end_tx_res =
                    self.pager
                        .end_tx(true, schema_did_change, &self.program.connection, true)?;
                self.program
                    .connection
                    .transaction_state
                    .set(TransactionState::None);
                assert!(
                    matches!(end_tx_res, IOResult::Done(_)),
                    "end_tx should not return IO as it should just end txn without flushing anything. Got {end_tx_res:?}"
                );
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
    pub functions: HashMap<String, Arc<function::ExternalFunc>>,
    pub vtabs: HashMap<String, Arc<VirtualTable>>,
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
    ) -> Option<Arc<function::ExternalFunc>> {
        self.functions.get(name).cloned()
    }

    pub fn extend(&mut self, other: &SymbolTable) {
        for (name, func) in &other.functions {
            self.functions.insert(name.clone(), func.clone());
        }
        for (name, vtab) in &other.vtabs {
            self.vtabs.insert(name.clone(), vtab.clone());
        }
        for (name, module) in &other.vtab_modules {
            self.vtab_modules.insert(name.clone(), module.clone());
        }
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
