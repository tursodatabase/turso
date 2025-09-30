#![allow(clippy::arc_with_non_send_sync)]
extern crate core;

mod assert;
mod error;
mod ext;
mod fast_lock;
mod function;
mod functions;
mod incremental;
mod info;
mod io;
#[cfg(feature = "json")]
mod json;
pub mod mvcc;
mod parameters;
mod pragma;
mod pseudo;
mod schema;
#[cfg(feature = "series")]
mod series;
pub mod state_machine;
pub mod storage;
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

use crate::storage::checksum::CHECKSUM_REQUIRED_RESERVED_BYTES;
use crate::translate::display::PlanContext;
use crate::translate::pragma::TURSO_CDC_DEFAULT_TABLE_NAME;
#[cfg(all(feature = "fs", feature = "conn_raw_api"))]
use crate::types::{WalFrameInfo, WalState};
#[cfg(feature = "fs")]
use crate::util::{OpenMode, OpenOptions};
use crate::vdbe::metrics::ConnectionMetrics;
use crate::vtab::VirtualTable;
use crate::{incremental::view::AllViewsTxState, translate::emitter::TransactionMode};
use core::str;
pub use error::{CompletionError, LimboError};
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
    cell::RefCell,
    collections::HashMap,
    fmt::{self, Display},
    num::NonZero,
    ops::Deref,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU16, AtomicUsize, Ordering},
        Arc, LazyLock, Mutex, Weak,
    },
    time::Duration,
};
#[cfg(feature = "fs")]
use storage::database::DatabaseFile;
pub use storage::database::IOContext;
pub use storage::encryption::{CipherMode, EncryptionContext, EncryptionKey};
use storage::page_cache::PageCache;
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
use turso_macros::match_ignore_ascii_case;
use turso_parser::ast::fmt::ToTokens;
use turso_parser::{ast, ast::Cmd, parser::Parser};
use types::IOResult;
pub use types::RefValue;
pub use types::Value;
use util::parse_schema_rows;
pub use util::IOExt;
pub use vdbe::{builder::QueryMode, explain::EXPLAIN_COLUMNS, explain::EXPLAIN_QUERY_PLAN_COLUMNS};

/// Configuration for database features
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseOpts {
    pub enable_mvcc: bool,
    pub enable_indexes: bool,
    pub enable_views: bool,
    pub enable_strict: bool,
    enable_load_extension: bool,
}

impl Default for DatabaseOpts {
    fn default() -> Self {
        Self {
            enable_mvcc: false,
            enable_indexes: true,
            enable_views: false,
            enable_strict: false,
            enable_load_extension: false,
        }
    }
}

impl DatabaseOpts {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(feature = "cli_only")]
    pub fn turso_cli(mut self) -> Self {
        self.enable_load_extension = true;
        self
    }

    pub fn with_mvcc(mut self, enable: bool) -> Self {
        self.enable_mvcc = enable;
        self
    }

    pub fn with_indexes(mut self, enable: bool) -> Self {
        self.enable_indexes = enable;
        self
    }

    pub fn with_views(mut self, enable: bool) -> Self {
        self.enable_views = enable;
        self
    }

    pub fn with_strict(mut self, enable: bool) -> Self {
        self.enable_strict = enable;
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct EncryptionOpts {
    pub cipher: String,
    pub hexkey: String,
}

impl EncryptionOpts {
    pub fn new() -> Self {
        Self::default()
    }
}

pub type Result<T, E = LimboError> = std::result::Result<T, E>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TransactionState {
    Write { schema_did_change: bool },
    Read,
    PendingUpgrade,
    None,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SyncMode {
    Off = 0,
    Full = 2,
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
    wal_path: String,
    pub io: Arc<dyn IO>,
    buffer_pool: Arc<BufferPool>,
    // Shared structures of a Database are the parts that are common to multiple threads that might
    // create DB connections.
    _shared_page_cache: Arc<RwLock<PageCache>>,
    shared_wal: Arc<RwLock<WalFileShared>>,
    db_state: Arc<AtomicDbState>,
    init_lock: Arc<Mutex<()>>,
    open_flags: OpenFlags,
    builtin_syms: RwLock<SymbolTable>,
    opts: DatabaseOpts,
    n_connections: AtomicUsize,
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

        let wal_status = match self.shared_wal.try_read() {
            Some(wal) if wal.enabled.load(Ordering::SeqCst) => "enabled",
            Some(_) => "disabled",
            None => "locked_for_write",
        };
        debug_struct.field("wal_state", &wal_status);

        // Page cache info (just basic stats, not full contents)
        let cache_info = match self._shared_page_cache.try_read() {
            Some(cache) => format!("( capacity {}, used: {} )", cache.capacity(), cache.len()),
            None => "locked".to_string(),
        };
        debug_struct.field("page_cache", &cache_info);

        debug_struct.field(
            "n_connections",
            &self.n_connections.load(std::sync::atomic::Ordering::SeqCst),
        );
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
        Self::open_file_with_flags(
            io,
            path,
            OpenFlags::default(),
            DatabaseOpts::new()
                .with_mvcc(enable_mvcc)
                .with_indexes(enable_indexes),
            None,
        )
    }

    #[cfg(feature = "fs")]
    pub fn open_file_with_flags(
        io: Arc<dyn IO>,
        path: &str,
        flags: OpenFlags,
        opts: DatabaseOpts,
        encryption_opts: Option<EncryptionOpts>,
    ) -> Result<Arc<Database>> {
        let file = io.open_file(path, flags, true)?;
        let db_file = Arc::new(DatabaseFile::new(file));
        Self::open_with_flags(io, path, db_file, flags, opts, encryption_opts)
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
            DatabaseOpts::new()
                .with_mvcc(enable_mvcc)
                .with_indexes(enable_indexes),
            None,
        )
    }

    #[allow(clippy::arc_with_non_send_sync)]
    pub fn open_with_flags(
        io: Arc<dyn IO>,
        path: &str,
        db_file: Arc<dyn DatabaseStorage>,
        flags: OpenFlags,
        opts: DatabaseOpts,
        encryption_opts: Option<EncryptionOpts>,
    ) -> Result<Arc<Database>> {
        // turso-sync-engine create 2 databases with different names in the same IO if MemoryIO is used
        // in this case we need to bypass registry (as this is MemoryIO DB) but also preserve original distinction in names (e.g. :memory:-draft and :memory:-synced)
        if path.starts_with(":memory:") {
            return Self::open_with_flags_bypass_registry_internal(
                io,
                path,
                &format!("{path}-wal"),
                db_file,
                flags,
                opts,
                None,
            );
        }

        let mut registry = DATABASE_MANAGER.lock().unwrap();

        let canonical_path = std::fs::canonicalize(path)
            .ok()
            .and_then(|p| p.to_str().map(|s| s.to_string()))
            .unwrap_or_else(|| path.to_string());

        if let Some(db) = registry.get(&canonical_path).and_then(Weak::upgrade) {
            return Ok(db);
        }
        let db = Self::open_with_flags_bypass_registry_internal(
            io,
            path,
            &format!("{path}-wal"),
            db_file,
            flags,
            opts,
            encryption_opts,
        )?;
        registry.insert(canonical_path, Arc::downgrade(&db));
        Ok(db)
    }

    #[allow(clippy::arc_with_non_send_sync)]
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn open_with_flags_bypass_registry(
        io: Arc<dyn IO>,
        path: &str,
        wal_path: &str,
        db_file: Arc<dyn DatabaseStorage>,
        flags: OpenFlags,
        opts: DatabaseOpts,
        encryption_opts: Option<EncryptionOpts>,
    ) -> Result<Arc<Database>> {
        Self::open_with_flags_bypass_registry_internal(
            io,
            path,
            wal_path,
            db_file,
            flags,
            opts,
            encryption_opts,
        )
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn open_with_flags_bypass_registry_internal(
        io: Arc<dyn IO>,
        path: &str,
        wal_path: &str,
        db_file: Arc<dyn DatabaseStorage>,
        flags: OpenFlags,
        opts: DatabaseOpts,
        encryption_opts: Option<EncryptionOpts>,
    ) -> Result<Arc<Database>> {
        let shared_wal = WalFileShared::open_shared_if_exists(&io, wal_path)?;

        let mv_store = if opts.enable_mvcc {
            let file = io.open_file(&format!("{path}-lg"), OpenFlags::default(), false)?;
            let storage = mvcc::persistent_storage::Storage::new(file);
            let mv_store = MvStore::new(mvcc::LocalClock::new(), storage);
            Some(Arc::new(mv_store))
        } else {
            None
        };

        let db_size = db_file.size()?;
        let db_state = if db_size == 0 {
            DbState::Uninitialized
        } else {
            DbState::Initialized
        };

        let shared_page_cache = Arc::new(RwLock::new(PageCache::default()));
        let syms = SymbolTable::new();
        let arena_size = if std::env::var("TESTING").is_ok_and(|v| v.eq_ignore_ascii_case("true")) {
            BufferPool::TEST_ARENA_SIZE
        } else {
            BufferPool::DEFAULT_ARENA_SIZE
        };
        // opts is now passed as parameter
        let db = Arc::new(Database {
            mv_store,
            path: path.to_string(),
            wal_path: wal_path.to_string(),
            schema: Mutex::new(Arc::new(Schema::new(opts.enable_indexes))),
            _shared_page_cache: shared_page_cache.clone(),
            shared_wal,
            db_file,
            builtin_syms: syms.into(),
            io: io.clone(),
            open_flags: flags,
            db_state: Arc::new(AtomicDbState::new(db_state)),
            init_lock: Arc::new(Mutex::new(())),
            opts,
            buffer_pool: BufferPool::begin_init(&io, arena_size),
            n_connections: AtomicUsize::new(0),
        });
        db.register_global_builtin_extensions()
            .expect("unable to register global extensions");

        // Check: https://github.com/tursodatabase/turso/pull/1761#discussion_r2154013123
        if db_state.is_initialized() {
            // parse schema
            let conn = db.connect()?;

            let syms = conn.syms.read();
            let pager = conn.pager.read().clone();

            if let Some(encryption_opts) = encryption_opts {
                conn.pragma_update("cipher", format!("'{}'", encryption_opts.cipher))?;
                conn.pragma_update("hexkey", format!("'{}'", encryption_opts.hexkey))?;
                // Clear page cache so the header page can be reread from disk and decrypted using the encryption context.
                pager.clear_page_cache();
            }
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

                if db.mvcc_enabled() && !schema.indexes.is_empty() {
                    return Err(LimboError::ParseError(
                        "Database contains indexes which are not supported when MVCC is enabled."
                            .to_string(),
                    ));
                }

                Ok(())
            })?;
        }
        Ok(db)
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn connect(self: &Arc<Database>) -> Result<Arc<Connection>> {
        let pager = self.init_pager(None)?;
        let pager = Arc::new(pager);

        if self.mv_store.is_some() {
            self.maybe_recover_logical_log(pager.clone())?;
        }

        let page_size = pager.get_page_size_unchecked();

        let default_cache_size = pager
            .io
            .block(|| pager.with_header(|header| header.default_page_cache_size))
            .unwrap_or_default()
            .get();
        let conn = Arc::new(Connection {
            db: self.clone(),
            pager: RwLock::new(pager),
            schema: RwLock::new(
                self.schema
                    .lock()
                    .map_err(|_| LimboError::SchemaLocked)?
                    .clone(),
            ),
            database_schemas: RwLock::new(std::collections::HashMap::new()),
            auto_commit: AtomicBool::new(true),
            transaction_state: RwLock::new(TransactionState::None),
            last_insert_rowid: AtomicI64::new(0),
            last_change: AtomicI64::new(0),
            total_changes: AtomicI64::new(0),
            syms: RwLock::new(SymbolTable::new()),
            _shared_cache: false,
            cache_size: AtomicI32::new(default_cache_size),
            page_size: AtomicU16::new(page_size.get_raw()),
            wal_auto_checkpoint_disabled: AtomicBool::new(false),
            capture_data_changes: RwLock::new(CaptureDataChangesMode::Off),
            closed: AtomicBool::new(false),
            attached_databases: RwLock::new(DatabaseCatalog::new()),
            query_only: AtomicBool::new(false),
            mv_tx: RwLock::new(None),
            view_transaction_states: AllViewsTxState::new(),
            metrics: RwLock::new(ConnectionMetrics::new()),
            is_nested_stmt: AtomicBool::new(false),
            encryption_key: RwLock::new(None),
            encryption_cipher_mode: RwLock::new(None),
            sync_mode: RwLock::new(SyncMode::Full),
            data_sync_retry: AtomicBool::new(false),
            busy_timeout: RwLock::new(Duration::new(0, 0)),
        });
        self.n_connections
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let builtin_syms = self.builtin_syms.read();
        // add built-in extensions symbols to the connection to prevent having to load each time
        conn.syms.write().extend(&builtin_syms);
        Ok(conn)
    }

    pub fn maybe_recover_logical_log(self: &Arc<Database>, pager: Arc<Pager>) -> Result<()> {
        let Some(mv_store) = self.mv_store.clone() else {
            panic!("tryign to recover logical log without mvcc");
        };
        if !mv_store.needs_recover() {
            return Ok(());
        }

        mv_store.recover_logical_log(&self.io, &pager)
    }

    pub fn is_readonly(&self) -> bool {
        self.open_flags.contains(OpenFlags::ReadOnly)
    }

    /// If we do not have a physical WAL file, but we know the database file is initialized on disk,
    /// we need to read the page_size from the database header.
    fn read_page_size_from_db_header(&self) -> Result<PageSize> {
        turso_assert!(
            self.db_state.is_initialized(),
            "read_page_size_from_db_header called on uninitialized database"
        );
        turso_assert!(
            PageSize::MIN % 512 == 0,
            "header read must be a multiple of 512 for O_DIRECT"
        );
        let buf = Arc::new(Buffer::new_temporary(PageSize::MIN as usize));
        let c = Completion::new_read(buf.clone(), move |_res| {});
        let c = self.db_file.read_header(c)?;
        self.io.wait_for_completion(c)?;
        let page_size = u16::from_be_bytes(buf.as_slice()[16..18].try_into().unwrap());
        let page_size = PageSize::new_from_header_u16(page_size)?;
        Ok(page_size)
    }

    fn read_reserved_space_bytes_from_db_header(&self) -> Result<u8> {
        turso_assert!(
            self.db_state.is_initialized(),
            "read_reserved_space_bytes_from_db_header called on uninitialized database"
        );
        turso_assert!(
            PageSize::MIN % 512 == 0,
            "header read must be a multiple of 512 for O_DIRECT"
        );
        let buf = Arc::new(Buffer::new_temporary(PageSize::MIN as usize));
        let c = Completion::new_read(buf.clone(), move |_res| {});
        let c = self.db_file.read_header(c)?;
        self.io.wait_for_completion(c)?;
        let reserved_bytes = u8::from_be_bytes(buf.as_slice()[20..21].try_into().unwrap());
        Ok(reserved_bytes)
    }

    /// Read the page size in order of preference:
    /// 1. From the WAL header if it exists and is initialized
    /// 2. From the database header if the database is initialized
    ///
    /// Otherwise, fall back to, in order of preference:
    /// 1. From the requested page size if it is provided
    /// 2. PageSize::default(), i.e. 4096
    fn determine_actual_page_size(
        &self,
        shared_wal: &WalFileShared,
        requested_page_size: Option<usize>,
    ) -> Result<PageSize> {
        if shared_wal.enabled.load(Ordering::SeqCst) {
            let size_in_wal = shared_wal.page_size();
            if size_in_wal != 0 {
                let Some(page_size) = PageSize::new(size_in_wal) else {
                    bail_corrupt_error!("invalid page size in WAL: {size_in_wal}");
                };
                return Ok(page_size);
            }
        }
        if self.db_state.is_initialized() {
            Ok(self.read_page_size_from_db_header()?)
        } else {
            let Some(size) = requested_page_size else {
                return Ok(PageSize::default());
            };
            let Some(page_size) = PageSize::new(size as u32) else {
                bail_corrupt_error!("invalid requested page size: {size}");
            };
            Ok(page_size)
        }
    }

    /// if the database is initialized i.e. it exists on disk, return the reserved space bytes from
    /// the header or None
    fn maybe_get_reserved_space_bytes(&self) -> Result<Option<u8>> {
        if self.db_state.is_initialized() {
            Ok(Some(self.read_reserved_space_bytes_from_db_header()?))
        } else {
            Ok(None)
        }
    }

    fn init_pager(&self, requested_page_size: Option<usize>) -> Result<Pager> {
        let reserved_bytes = self.maybe_get_reserved_space_bytes()?;
        let disable_checksums = if let Some(reserved_bytes) = reserved_bytes {
            // if the required reserved bytes for checksums is not present, disable checksums
            reserved_bytes != CHECKSUM_REQUIRED_RESERVED_BYTES
        } else {
            false
        };
        // Check if WAL is enabled
        let shared_wal = self.shared_wal.read();
        if shared_wal.enabled.load(Ordering::SeqCst) {
            let page_size = self.determine_actual_page_size(&shared_wal, requested_page_size)?;
            drop(shared_wal);

            let buffer_pool = self.buffer_pool.clone();
            if self.db_state.is_initialized() {
                buffer_pool.finalize_with_page_size(page_size.get() as usize)?;
            }

            let db_state = self.db_state.clone();
            let wal = Rc::new(RefCell::new(WalFile::new(
                self.io.clone(),
                self.shared_wal.clone(),
                buffer_pool.clone(),
            )));
            let pager = Pager::new(
                self.db_file.clone(),
                Some(wal),
                self.io.clone(),
                Arc::new(RwLock::new(PageCache::default())),
                buffer_pool.clone(),
                db_state,
                self.init_lock.clone(),
            )?;
            pager.set_page_size(page_size);
            if let Some(reserved_bytes) = reserved_bytes {
                pager.set_reserved_space_bytes(reserved_bytes);
            }
            if disable_checksums {
                pager.reset_checksum_context();
            }
            return Ok(pager);
        }
        let page_size = self.determine_actual_page_size(&shared_wal, requested_page_size)?;
        drop(shared_wal);

        let buffer_pool = self.buffer_pool.clone();

        if self.db_state.is_initialized() {
            buffer_pool.finalize_with_page_size(page_size.get() as usize)?;
        }

        // No existing WAL; create one.
        let db_state = self.db_state.clone();
        let mut pager = Pager::new(
            self.db_file.clone(),
            None,
            self.io.clone(),
            Arc::new(RwLock::new(PageCache::default())),
            buffer_pool.clone(),
            db_state,
            Arc::new(Mutex::new(())),
        )?;

        pager.set_page_size(page_size);
        if let Some(reserved_bytes) = reserved_bytes {
            pager.set_reserved_space_bytes(reserved_bytes);
        }
        if disable_checksums {
            pager.reset_checksum_context();
        }
        let file = self
            .io
            .open_file(&self.wal_path, OpenFlags::Create, false)?;

        // Enable WAL in the existing shared instance
        {
            let mut shared_wal = self.shared_wal.write();
            shared_wal.create(file)?;
        }

        let wal = Rc::new(RefCell::new(WalFile::new(
            self.io.clone(),
            self.shared_wal.clone(),
            buffer_pool,
        )));
        pager.set_wal(wal);

        Ok(pager)
    }

    #[cfg(feature = "fs")]
    pub fn io_for_path(path: &str) -> Result<Arc<dyn IO>> {
        use crate::util::MEMORY_PATH;
        let io: Arc<dyn IO> = match path.trim() {
            MEMORY_PATH => Arc::new(MemoryIO::new()),
            _ => Arc::new(PlatformIO::new()?),
        };
        Ok(io)
    }

    #[cfg(feature = "fs")]
    pub fn io_for_vfs<S: AsRef<str> + std::fmt::Display>(vfs: S) -> Result<Arc<dyn IO>> {
        let vfsmods = ext::add_builtin_vfs_extensions(None)?;
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
                    return Err(LimboError::InvalidArgument(format!("no such VFS: {other}")));
                }
            },
        };
        Ok(io)
    }

    /// Open a new database file with optionally specifying a VFS without an existing database
    /// connection and symbol table to register extensions.
    #[cfg(feature = "fs")]
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn open_new<S>(
        path: &str,
        vfs: Option<S>,
        flags: OpenFlags,
        opts: DatabaseOpts,
        encryption_opts: Option<EncryptionOpts>,
    ) -> Result<(Arc<dyn IO>, Arc<Database>)>
    where
        S: AsRef<str> + std::fmt::Display,
    {
        let io = vfs
            .map(|vfs| Self::io_for_vfs(vfs))
            .or_else(|| Some(Self::io_for_path(path)))
            .transpose()?
            .unwrap();
        let db = Self::open_file_with_flags(io.clone(), path, flags, opts, encryption_opts)?;
        Ok((io, db))
    }

    pub(crate) fn can_load_extensions(&self) -> bool {
        self.opts.enable_load_extension
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

    pub fn experimental_views_enabled(&self) -> bool {
        self.opts.enable_views
    }

    pub fn experimental_strict_enabled(&self) -> bool {
        self.opts.enable_strict
    }

    pub fn mvcc_enabled(&self) -> bool {
        self.opts.enable_mvcc
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
    pub fn has_updates(&self) -> bool {
        matches!(self, CaptureDataChangesMode::Full { .. })
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
    index_to_data: HashMap<usize, (Arc<Database>, Arc<Pager>)>,
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

    fn get_pager_by_index(&self, idx: &usize) -> Arc<Pager> {
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

    fn insert(&mut self, s: &str, data: (Arc<Database>, Arc<Pager>)) -> usize {
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
    db: Arc<Database>,
    pager: RwLock<Arc<Pager>>,
    schema: RwLock<Arc<Schema>>,
    /// Per-database schema cache (database_index -> schema)
    /// Loaded lazily to avoid copying all schemas on connection open
    database_schemas: RwLock<std::collections::HashMap<usize, Arc<Schema>>>,
    /// Whether to automatically commit transaction
    auto_commit: AtomicBool,
    transaction_state: RwLock<TransactionState>,
    last_insert_rowid: AtomicI64,
    last_change: AtomicI64,
    total_changes: AtomicI64,
    syms: RwLock<SymbolTable>,
    _shared_cache: bool,
    cache_size: AtomicI32,
    /// page size used for an uninitialized database or the next vacuum command.
    /// it's not always equal to the current page size of the database
    page_size: AtomicU16,
    /// Disable automatic checkpoint behaviour when DB is shutted down or WAL reach certain size
    /// Client still can manually execute PRAGMA wal_checkpoint(...) commands
    wal_auto_checkpoint_disabled: AtomicBool,
    capture_data_changes: RwLock<CaptureDataChangesMode>,
    closed: AtomicBool,
    /// Attached databases
    attached_databases: RwLock<DatabaseCatalog>,
    query_only: AtomicBool,
    pub(crate) mv_tx: RwLock<Option<(crate::mvcc::database::TxID, TransactionMode)>>,

    /// Per-connection view transaction states for uncommitted changes. This represents
    /// one entry per view that was touched in the transaction.
    view_transaction_states: AllViewsTxState,
    /// Connection-level metrics aggregation
    pub metrics: RwLock<ConnectionMetrics>,
    /// Whether the connection is executing a statement initiated by another statement.
    /// Generally this is only true for ParseSchema.
    is_nested_stmt: AtomicBool,
    encryption_key: RwLock<Option<EncryptionKey>>,
    encryption_cipher_mode: RwLock<Option<CipherMode>>,
    sync_mode: RwLock<SyncMode>,
    data_sync_retry: AtomicBool,
    /// User defined max accumulated Busy timeout duration
    /// Default is 0 (no timeout)
    busy_timeout: RwLock<std::time::Duration>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.is_closed() {
            // if connection wasn't properly closed, decrement the connection counter
            self.db
                .n_connections
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

impl Connection {
    #[instrument(skip_all, level = Level::INFO)]
    pub fn prepare(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
        if self.is_closed() {
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
        let cmd = parser.next_cmd()?;
        let syms = self.syms.read();
        let cmd = cmd.expect("Successful parse on nonempty input string should produce a command");
        let byte_offset_end = parser.offset();
        let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
            .unwrap()
            .trim();
        self.maybe_update_schema()?;
        let pager = self.pager.read().clone();
        let mode = QueryMode::new(&cmd);
        let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
        let program = translate::translate(
            self.schema.read().deref(),
            stmt,
            pager.clone(),
            self.clone(),
            &syms,
            mode,
            input,
        )?;
        Ok(Statement::new(
            program,
            self.db.mv_store.clone(),
            pager,
            mode,
        ))
    }

    /// Parse schema from scratch if version of schema for the connection differs from the schema cookie in the root page
    /// This function must be called outside of any transaction because internally it will start transaction session by itself
    #[allow(dead_code)]
    fn maybe_reparse_schema(self: &Arc<Connection>) -> Result<()> {
        let pager = self.pager.read().clone();

        // first, quickly read schema_version from the root page in order to check if schema changed
        pager.begin_read_tx()?;
        let on_disk_schema_version = pager
            .io
            .block(|| pager.with_header(|header| header.schema_cookie));

        let on_disk_schema_version = match on_disk_schema_version {
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
        pager.end_read_tx().expect("read txn must be finished");

        let db_schema_version = self.db.schema.lock().unwrap().schema_version;
        tracing::debug!(
            "path: {}, db_schema_version={} vs on_disk_schema_version={}",
            self.db.path,
            db_schema_version,
            on_disk_schema_version
        );
        // if schema_versions matches - exit early
        if db_schema_version == on_disk_schema_version {
            return Ok(());
        }
        // maybe_reparse_schema must be called outside of any transaction
        turso_assert!(
            self.get_tx_state() == TransactionState::None,
            "unexpected start transaction"
        );
        // start read transaction manually, because we will read schema cookie once again and
        // we must be sure that it will consistent with schema content
        //
        // from now on we must be very careful with errors propagation
        // in order to not accidentally keep read transaction opened
        pager.begin_read_tx()?;
        self.set_tx_state(TransactionState::Read);

        let reparse_result = self.reparse_schema();

        let previous =
            std::mem::replace(&mut *self.transaction_state.write(), TransactionState::None);
        turso_assert!(
            matches!(previous, TransactionState::None | TransactionState::Read),
            "unexpected end transaction state"
        );
        // close opened transaction if it was kept open
        // (in most cases, it will be automatically closed if stmt was executed properly)
        if previous == TransactionState::Read {
            pager.end_read_tx().expect("read txn must be finished");
        }

        reparse_result?;

        let schema = self.schema.read().clone();
        self.db.update_schema_if_newer(schema)
    }

    fn reparse_schema(self: &Arc<Connection>) -> Result<()> {
        let pager = self.pager.read().clone();

        // read cookie before consuming statement program - otherwise we can end up reading cookie with closed transaction state
        let cookie = pager
            .io
            .block(|| pager.with_header(|header| header.schema_cookie))?
            .get();

        // create fresh schema as some objects can be deleted
        let mut fresh = Schema::new(self.schema.read().indexes_enabled);
        fresh.schema_version = cookie;

        // Preserve existing views to avoid expensive repopulation.
        // TODO: We may not need to do this if we materialize our views.
        let existing_views = self.schema.read().incremental_views.clone();

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
        parse_schema_rows(
            stmt,
            &mut fresh,
            &self.syms.read(),
            None,
            existing_views,
            self.db.mv_store.as_ref(),
        )?;

        tracing::debug!(
            "reparse_schema: schema_version={}, tables={:?}",
            fresh.schema_version,
            fresh.tables.keys()
        );
        self.with_schema_mut(|schema| {
            *schema = fresh;
        });
        Result::Ok(())
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn prepare_execute_batch(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        if sql.as_ref().is_empty() {
            return Err(LimboError::InvalidArgument(
                "The supplied SQL string contains no statements".to_string(),
            ));
        }
        self.maybe_update_schema()?;
        let sql = sql.as_ref();
        tracing::trace!("Preparing and executing batch: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        while let Some(cmd) = parser.next_cmd()? {
            let syms = self.syms.read();
            let pager = self.pager.read().clone();
            let byte_offset_end = parser.offset();
            let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
                .unwrap()
                .trim();
            let mode = QueryMode::new(&cmd);
            let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
            let program = translate::translate(
                self.schema.read().deref(),
                stmt,
                pager.clone(),
                self.clone(),
                &syms,
                mode,
                input,
            )?;
            Statement::new(program, self.db.mv_store.clone(), pager.clone(), mode)
                .run_ignore_rows()?;
        }
        Ok(())
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn query(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Option<Statement>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let sql = sql.as_ref();
        self.maybe_update_schema()?;
        tracing::trace!("Querying: {}", sql);
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd()?;
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
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let syms = self.syms.read();
        let pager = self.pager.read().clone();
        let mode = QueryMode::new(&cmd);
        let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
        let program = translate::translate(
            self.schema.read().deref(),
            stmt,
            pager.clone(),
            self.clone(),
            &syms,
            mode,
            input,
        )?;
        let stmt = Statement::new(program, self.db.mv_store.clone(), pager, mode);
        Ok(Some(stmt))
    }

    pub fn query_runner<'a>(self: &'a Arc<Connection>, sql: &'a [u8]) -> QueryRunner<'a> {
        QueryRunner::new(self, sql)
    }

    /// Execute will run a query from start to finish taking ownership of I/O because it will run pending I/Os if it didn't finish.
    /// TODO: make this api async
    #[instrument(skip_all, level = Level::INFO)]
    pub fn execute(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let sql = sql.as_ref();
        self.maybe_update_schema()?;
        let mut parser = Parser::new(sql.as_bytes());
        while let Some(cmd) = parser.next_cmd()? {
            let syms = self.syms.read();
            let pager = self.pager.read().clone();
            let byte_offset_end = parser.offset();
            let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
                .unwrap()
                .trim();
            let mode = QueryMode::new(&cmd);
            let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
            let program = translate::translate(
                self.schema.read().deref(),
                stmt,
                pager.clone(),
                self.clone(),
                &syms,
                mode,
                input,
            )?;
            Statement::new(program, self.db.mv_store.clone(), pager.clone(), mode)
                .run_ignore_rows()?;
        }
        Ok(())
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn consume_stmt(self: &Arc<Connection>, sql: &str) -> Result<Option<(Statement, usize)>> {
        let mut parser = Parser::new(sql.as_bytes());
        let Some(cmd) = parser.next_cmd()? else {
            return Ok(None);
        };
        let syms = self.syms.read();
        let pager = self.pager.read().clone();
        let byte_offset_end = parser.offset();
        let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
            .unwrap()
            .trim();
        let mode = QueryMode::new(&cmd);
        let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
        let program = translate::translate(
            self.schema.read().deref(),
            stmt,
            pager.clone(),
            self.clone(),
            &syms,
            mode,
            input,
        )?;
        let stmt = Statement::new(program, self.db.mv_store.clone(), pager.clone(), mode);
        Ok(Some((stmt, parser.offset())))
    }

    #[cfg(feature = "fs")]
    pub fn from_uri(
        uri: &str,
        use_indexes: bool,
        mvcc: bool,
        views: bool,
        strict: bool,
    ) -> Result<(Arc<dyn IO>, Arc<Connection>)> {
        use crate::util::MEMORY_PATH;
        let opts = OpenOptions::parse(uri)?;
        let flags = opts.get_flags()?;
        if opts.path == MEMORY_PATH || matches!(opts.mode, OpenMode::Memory) {
            let io = Arc::new(MemoryIO::new());
            let db = Database::open_file_with_flags(
                io.clone(),
                MEMORY_PATH,
                flags,
                DatabaseOpts::new()
                    .with_mvcc(mvcc)
                    .with_indexes(use_indexes)
                    .with_views(views)
                    .with_strict(strict),
                None,
            )?;
            let conn = db.connect()?;
            return Ok((io, conn));
        }
        let encryption_opts = match (opts.cipher.clone(), opts.hexkey.clone()) {
            (Some(cipher), Some(hexkey)) => Some(EncryptionOpts { cipher, hexkey }),
            (Some(_), None) => {
                return Err(LimboError::InvalidArgument(
                    "hexkey is required when cipher is provided".to_string(),
                ))
            }
            (None, Some(_)) => {
                return Err(LimboError::InvalidArgument(
                    "cipher is required when hexkey is provided".to_string(),
                ))
            }
            (None, None) => None,
        };
        let (io, db) = Database::open_new(
            &opts.path,
            opts.vfs.as_ref(),
            flags,
            DatabaseOpts::new()
                .with_mvcc(mvcc)
                .with_indexes(use_indexes)
                .with_views(views)
                .with_strict(strict),
            encryption_opts.clone(),
        )?;
        if let Some(modeof) = opts.modeof {
            let perms = std::fs::metadata(modeof)?;
            std::fs::set_permissions(&opts.path, perms.permissions())?;
        }
        let conn = db.connect()?;
        if let Some(cipher) = opts.cipher {
            let _ = conn.pragma_update("cipher", format!("'{cipher}'"));
        }
        if let Some(hexkey) = opts.hexkey {
            let _ = conn.pragma_update("hexkey", format!("'{hexkey}'"));
        }
        if let Some(encryption_opts) = encryption_opts {
            let _ = conn.pragma_update("cipher", encryption_opts.cipher.to_string());
            let _ = conn.pragma_update("hexkey", encryption_opts.hexkey.to_string());
            let pager = conn.pager.read();
            if db.db_state.is_initialized() {
                // Clear page cache so the header page can be reread from disk and decrypted using the encryption context.
                pager.clear_page_cache();
            }
        }
        Ok((io, conn))
    }

    #[cfg(feature = "fs")]
    fn from_uri_attached(
        uri: &str,
        db_opts: DatabaseOpts,
        io: Arc<dyn IO>,
    ) -> Result<Arc<Database>> {
        let mut opts = OpenOptions::parse(uri)?;
        // FIXME: for now, only support read only attach
        opts.mode = OpenMode::ReadOnly;
        let flags = opts.get_flags()?;
        let io = opts.vfs.map(Database::io_for_vfs).unwrap_or(Ok(io))?;
        let db = Database::open_file_with_flags(io.clone(), &opts.path, flags, db_opts, None)?;
        if let Some(modeof) = opts.modeof {
            let perms = std::fs::metadata(modeof)?;
            std::fs::set_permissions(&opts.path, perms.permissions())?;
        }
        Ok(db)
    }

    pub fn maybe_update_schema(&self) -> Result<()> {
        let current_schema_version = self.schema.read().schema_version;
        let schema = self
            .db
            .schema
            .lock()
            .map_err(|_| LimboError::SchemaLocked)?;
        if matches!(self.get_tx_state(), TransactionState::None)
            && current_schema_version != schema.schema_version
        {
            *self.schema.write() = schema.clone();
        }

        Ok(())
    }

    /// Read schema version at current transaction
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn read_schema_version(&self) -> Result<u32> {
        let pager = self.pager.read();
        pager
            .io
            .block(|| pager.with_header(|header| header.schema_cookie))
            .map(|version| version.get())
    }

    /// Update schema version to the new value within opened write transaction
    ///
    /// New version of the schema must be strictly greater than previous one - otherwise method will panic
    /// Write transaction must be opened in advance - otherwise method will panic
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn write_schema_version(self: &Arc<Connection>, version: u32) -> Result<()> {
        let TransactionState::Write { .. } = self.get_tx_state() else {
            return Err(LimboError::InternalError(
                "write_schema_version must be called from within Write transaction".to_string(),
            ));
        };
        let pager = self.pager.read();
        pager.io.block(|| {
            pager.with_header_mut(|header| {
                turso_assert!(
                    header.schema_cookie.get() < version,
                    "cookie can't go back in time"
                );
                *self.transaction_state.write() = TransactionState::Write {
                    schema_did_change: true,
                };
                self.with_schema_mut(|schema| schema.schema_version = version);
                header.schema_cookie = version.into();
            })
        })?;
        self.reparse_schema()?;
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
        let pager = self.pager.read();
        let (page_ref, c) = match pager.read_page_no_cache(page_idx as i64, frame_watermark, true) {
            Ok(result) => result,
            // on windows, zero read will trigger UnexpectedEof
            #[cfg(target_os = "windows")]
            Err(LimboError::CompletionError(CompletionError::IOError(
                std::io::ErrorKind::UnexpectedEof,
            ))) => return Ok(false),
            Err(err) => return Err(err),
        };

        pager.io.wait_for_completion(c)?;

        let content = page_ref.get_contents();
        // empty read - attempt to read absent page
        if content.buffer.is_empty() {
            return Ok(false);
        }
        page.copy_from_slice(content.as_ptr());
        Ok(true)
    }

    /// Return unique set of page numbers changes after WAL watermark position in the current WAL session
    /// (so, if concurrent connection wrote something to the WAL - this method will not see this change)
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_changed_pages_after(&self, frame_watermark: u64) -> Result<Vec<u32>> {
        self.pager.read().wal_changed_pages_after(frame_watermark)
    }

    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_state(&self) -> Result<WalState> {
        self.pager.read().wal_state()
    }

    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_get_frame(&self, frame_no: u64, frame: &mut [u8]) -> Result<WalFrameInfo> {
        use crate::storage::sqlite3_ondisk::parse_wal_frame_header;

        let c = self.pager.read().wal_get_frame(frame_no, frame)?;
        self.db.io.wait_for_completion(c)?;
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
        self.pager.read().wal_insert_frame(frame_no, frame)
    }

    /// Start WAL session by initiating read+write transaction for this connection
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_insert_begin(&self) -> Result<()> {
        let pager = self.pager.read();
        pager.begin_read_tx()?;
        pager.io.block(|| pager.begin_write_tx()).inspect_err(|_| {
            pager.end_read_tx().expect("read txn must be closed");
        })?;

        // start write transaction and disable auto-commit mode as SQL can be executed within WAL session (at caller own risk)
        *self.transaction_state.write() = TransactionState::Write {
            schema_did_change: false,
        };
        self.auto_commit.store(false, Ordering::SeqCst);

        Ok(())
    }

    /// Finish WAL session by ending read+write transaction taken in the [Self::wal_insert_begin] method
    /// All frames written after last commit frame (db_size > 0) within the session will be rolled back
    #[cfg(all(feature = "fs", feature = "conn_raw_api"))]
    pub fn wal_insert_end(self: &Arc<Connection>, force_commit: bool) -> Result<()> {
        {
            let pager = self.pager.read();

            let Some(wal) = pager.wal.as_ref() else {
                return Err(LimboError::InternalError(
                    "wal_insert_end called without a wal".to_string(),
                ));
            };

            let commit_err = if force_commit {
                pager
                    .io
                    .block(|| {
                        pager.commit_dirty_pages(
                            true,
                            self.get_sync_mode(),
                            self.get_data_sync_retry(),
                        )
                    })
                    .err()
            } else {
                None
            };

            self.auto_commit.store(true, Ordering::SeqCst);
            self.set_tx_state(TransactionState::None);
            {
                let wal = wal.borrow_mut();
                wal.end_write_tx();
                wal.end_read_tx();
            }

            let rollback_err = if !force_commit {
                // remove all non-commited changes in case if WAL session left some suffix without commit frame
                pager.rollback(false, self, true).err()
            } else {
                None
            };
            if let Some(err) = commit_err.or(rollback_err) {
                return Err(err);
            }
        }

        // let's re-parse schema from scratch if schema cookie changed compared to the our in-memory view of schema
        self.maybe_reparse_schema()?;
        Ok(())
    }

    /// Flush dirty pages to disk.
    pub fn cacheflush(&self) -> Result<Vec<Completion>> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        self.pager.read().cacheflush()
    }

    pub fn clear_page_cache(&self) -> Result<()> {
        self.pager.read().clear_page_cache();
        Ok(())
    }

    pub fn checkpoint(&self, mode: CheckpointMode) -> Result<CheckpointResult> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        self.pager.read().wal_checkpoint(mode)
    }

    /// Close a connection and checkpoint.
    pub fn close(&self) -> Result<()> {
        if self.is_closed() {
            return Ok(());
        }
        self.closed.store(true, Ordering::SeqCst);

        match self.get_tx_state() {
            TransactionState::None => {
                // No active transaction
            }
            _ => {
                let pager = self.pager.read();
                pager.io.block(|| {
                    pager.end_tx(
                        true, // rollback = true for close
                        self,
                    )
                })?;
                self.set_tx_state(TransactionState::None);
            }
        }

        if self
            .db
            .n_connections
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            .eq(&1)
        {
            self.pager
                .read()
                .checkpoint_shutdown(self.is_wal_auto_checkpoint_disabled())?;
        };
        Ok(())
    }

    pub fn wal_auto_checkpoint_disable(&self) {
        self.wal_auto_checkpoint_disabled
            .store(true, Ordering::SeqCst);
    }

    pub fn is_wal_auto_checkpoint_disabled(&self) -> bool {
        self.wal_auto_checkpoint_disabled.load(Ordering::SeqCst)
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid.load(Ordering::SeqCst)
    }

    fn update_last_rowid(&self, rowid: i64) {
        self.last_insert_rowid.store(rowid, Ordering::SeqCst);
    }

    pub fn set_changes(&self, nchange: i64) {
        self.last_change.store(nchange, Ordering::SeqCst);
        self.total_changes.fetch_add(nchange, Ordering::SeqCst);
    }

    pub fn changes(&self) -> i64 {
        self.last_change.load(Ordering::SeqCst)
    }

    pub fn total_changes(&self) -> i64 {
        self.total_changes.load(Ordering::SeqCst)
    }

    pub fn get_cache_size(&self) -> i32 {
        self.cache_size.load(Ordering::SeqCst)
    }
    pub fn set_cache_size(&self, size: i32) {
        self.cache_size.store(size, Ordering::SeqCst);
    }

    pub fn get_capture_data_changes(
        &self,
    ) -> parking_lot::RwLockReadGuard<'_, CaptureDataChangesMode> {
        self.capture_data_changes.read()
    }
    pub fn set_capture_data_changes(&self, opts: CaptureDataChangesMode) {
        *self.capture_data_changes.write() = opts;
    }
    pub fn get_page_size(&self) -> PageSize {
        let value = self.page_size.load(Ordering::SeqCst);
        PageSize::new_from_header_u16(value).unwrap_or_default()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn is_query_only(&self) -> bool {
        self.query_only.load(Ordering::SeqCst)
    }

    pub fn get_database_canonical_path(&self) -> String {
        if self.db.path == ":memory:" {
            // For in-memory databases, SQLite shows empty string
            String::new()
        } else {
            // For file databases, try show the full absolute path if that doesn't fail
            match std::fs::canonicalize(&self.db.path) {
                Ok(abs_path) => abs_path.to_string_lossy().to_string(),
                Err(_) => self.db.path.to_string(),
            }
        }
    }

    /// Check if a specific attached database is read only or not, by its index
    pub fn is_readonly(&self, index: usize) -> bool {
        if index == 0 {
            self.db.is_readonly()
        } else {
            let db = self.attached_databases.read().get_database_by_index(index);
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
        let Some(size) = PageSize::new(size) else {
            return Ok(());
        };

        self.page_size.store(size.get_raw(), Ordering::SeqCst);
        if self.db.db_state.get() != DbState::Uninitialized {
            return Ok(());
        }

        {
            let mut shared_wal = self.db.shared_wal.write();
            shared_wal.enabled.store(false, Ordering::SeqCst);
            shared_wal.file = None;
        }
        self.pager.write().clear_page_cache();
        let pager = self.db.init_pager(Some(size.get() as usize))?;
        *self.pager.write() = Arc::new(pager);
        self.pager.read().set_initial_page_size(size);

        Ok(())
    }

    #[cfg(feature = "fs")]
    pub fn open_new(&self, path: &str, vfs: &str) -> Result<(Arc<dyn IO>, Arc<Database>)> {
        Database::open_with_vfs(&self.db, path, vfs)
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
        self.auto_commit.load(Ordering::SeqCst)
    }

    pub fn parse_schema_rows(self: &Arc<Connection>) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let rows = self
            .query("SELECT * FROM sqlite_schema")?
            .expect("query must be parsed to statement");
        let syms = self.syms.read();
        self.with_schema_mut(|schema| {
            let existing_views = schema.incremental_views.clone();
            if let Err(LimboError::ExtensionError(e)) = parse_schema_rows(
                rows,
                schema,
                &syms,
                None,
                existing_views,
                self.db.mv_store.as_ref(),
            ) {
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
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let pragma = format!("PRAGMA {pragma_name}");
        let mut stmt = self.prepare(pragma)?;
        stmt.run_collect_rows()
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
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        let pragma = format!("PRAGMA {pragma_name} = {pragma_value}");
        let mut stmt = self.prepare(pragma)?;
        stmt.run_collect_rows()
    }

    pub fn experimental_views_enabled(&self) -> bool {
        self.db.experimental_views_enabled()
    }

    pub fn experimental_strict_enabled(&self) -> bool {
        self.db.experimental_strict_enabled()
    }

    pub fn mvcc_enabled(&self) -> bool {
        self.db.mvcc_enabled()
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
        if self.is_closed() {
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
        let mut schema_ref = self.schema.write();
        let schema = Arc::make_mut(&mut *schema_ref);
        f(schema)
    }

    pub fn is_db_initialized(&self) -> bool {
        self.db.db_state.is_initialized()
    }

    fn get_pager_from_database_index(&self, index: &usize) -> Arc<Pager> {
        if *index < 2 {
            self.pager.read().clone()
        } else {
            self.attached_databases.read().get_pager_by_index(index)
        }
    }

    #[cfg(feature = "fs")]
    fn is_attached(&self, alias: &str) -> bool {
        self.attached_databases
            .read()
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
        if self.is_closed() {
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
            .db
            .schema
            .lock()
            .map_err(|_| LimboError::SchemaLocked)?
            .indexes_enabled();
        let use_mvcc = self.db.mv_store.is_some();
        let use_views = self.db.experimental_views_enabled();
        let use_strict = self.db.experimental_strict_enabled();

        let db_opts = DatabaseOpts::new()
            .with_mvcc(use_mvcc)
            .with_indexes(use_indexes)
            .with_views(use_views)
            .with_strict(use_strict);
        let db = Self::from_uri_attached(path, db_opts, self.db.io.clone())?;
        let pager = Arc::new(db.init_pager(None)?);

        self.attached_databases.write().insert(alias, (db, pager));

        Ok(())
    }

    // Detach a database by alias name
    fn detach_database(&self, alias: &str) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }

        if alias == "main" || alias == "temp" {
            return Err(LimboError::InvalidArgument(format!(
                "cannot detach database: {alias}"
            )));
        }

        // Remove from attached databases
        let mut attached_dbs = self.attached_databases.write();
        if attached_dbs.remove(alias).is_none() {
            return Err(LimboError::InvalidArgument(format!(
                "no such database: {alias}"
            )));
        }

        Ok(())
    }

    // Get an attached database by alias name
    fn get_attached_database(&self, alias: &str) -> Option<(usize, Arc<Database>)> {
        self.attached_databases.read().get_database_by_name(alias)
    }

    /// List all attached database aliases
    pub fn list_attached_databases(&self) -> Vec<String> {
        self.attached_databases
            .read()
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
            let name_bytes = db_name_normalized.as_bytes();
            match_ignore_ascii_case!(match name_bytes {
                b"main" => Ok(0),
                b"temp" => Ok(1),
                _ => {
                    // Look up attached database
                    if let Some((idx, _attached_db)) =
                        self.get_attached_database(&db_name_normalized)
                    {
                        Ok(idx)
                    } else {
                        Err(LimboError::InvalidArgument(format!(
                            "no such database: {db_name_normalized}"
                        )))
                    }
                }
            })
        } else {
            // Unqualified table name - use main database
            Ok(0)
        }
    }

    /// Access schema for a database using a closure pattern to avoid cloning
    pub(crate) fn with_schema<T>(&self, database_id: usize, f: impl FnOnce(&Schema) -> T) -> T {
        if database_id == 0 {
            // Main database - use connection's schema which should be kept in sync
            let schema = self.schema.read();
            f(&schema)
        } else if database_id == 1 {
            // Temp database - uses same schema as main for now, but this will change later.
            let schema = self.schema.read();
            f(&schema)
        } else {
            // Attached database - check cache first, then load from database
            let mut schemas = self.database_schemas.write();

            if let Some(cached_schema) = schemas.get(&database_id) {
                return f(cached_schema);
            }

            // Schema not cached, load it lazily from the attached database
            let attached_dbs = self.attached_databases.read();
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
        let main_path = Self::get_canonical_path_for_database(&self.db);
        databases.push((0, "main".to_string(), main_path));

        // Add attached databases
        let attached_dbs = self.attached_databases.read();
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

    pub fn get_pager(&self) -> Arc<Pager> {
        self.pager.read().clone()
    }

    pub fn get_query_only(&self) -> bool {
        self.is_query_only()
    }

    pub fn set_query_only(&self, value: bool) {
        self.query_only.store(value, Ordering::SeqCst);
    }

    pub fn get_sync_mode(&self) -> SyncMode {
        *self.sync_mode.read()
    }

    pub fn set_sync_mode(&self, mode: SyncMode) {
        *self.sync_mode.write() = mode;
    }

    pub fn get_data_sync_retry(&self) -> bool {
        self.data_sync_retry
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn set_data_sync_retry(&self, value: bool) {
        self.data_sync_retry
            .store(value, std::sync::atomic::Ordering::SeqCst);
    }

    /// Creates a HashSet of modules that have been loaded
    pub fn get_syms_vtab_mods(&self) -> std::collections::HashSet<String> {
        self.syms.read().vtab_modules.keys().cloned().collect()
    }

    pub fn set_encryption_key(&self, key: EncryptionKey) -> Result<()> {
        tracing::trace!("setting encryption key for connection");
        *self.encryption_key.write() = Some(key.clone());
        self.set_encryption_context()
    }

    pub fn set_encryption_cipher(&self, cipher_mode: CipherMode) -> Result<()> {
        tracing::trace!("setting encryption cipher for connection");
        *self.encryption_cipher_mode.write() = Some(cipher_mode);
        self.set_encryption_context()
    }

    pub fn get_encryption_cipher_mode(&self) -> Option<CipherMode> {
        *self.encryption_cipher_mode.read()
    }

    // if both key and cipher are set, set encryption context on pager
    fn set_encryption_context(&self) -> Result<()> {
        let key_guard = self.encryption_key.read();
        let Some(key) = key_guard.as_ref() else {
            return Ok(());
        };
        let cipher_guard = self.encryption_cipher_mode.read();
        let Some(cipher_mode) = *cipher_guard else {
            return Ok(());
        };
        tracing::trace!("setting encryption ctx for connection");
        let pager = self.pager.read();
        if pager.is_encryption_ctx_set() {
            return Err(LimboError::InvalidArgument(
                "cannot reset encryption attributes if already set in the session".to_string(),
            ));
        }
        pager.set_encryption_context(cipher_mode, key)
    }

    /// Sets maximum total accumuated timeout. If the duration is None or Zero, we unset the busy handler for this Connection
    ///
    /// This api defers slighty from: https://www.sqlite.org/c3ref/busy_timeout.html
    ///
    /// Instead of sleeping for linear amount of time specified by the user,
    /// we will sleep in phases, until the the total amount of time is reached.
    /// This means we first sleep of 1ms, then if we still return busy, we sleep for 2 ms, and repeat until a maximum of 100 ms per phase.
    ///
    /// Example:
    /// 1. Set duration to 5ms
    /// 2. Step through query -> returns Busy -> sleep/yield for 1 ms
    /// 3. Step through query -> returns Busy -> sleep/yield for 2 ms
    /// 4. Step through query -> returns Busy -> sleep/yield for 2 ms (totaling 5 ms of sleep)
    /// 5. Step through query -> returns Busy -> return Busy to user
    ///
    /// This slight api change demonstrated a better throughtput in `perf/throughput/turso` benchmark
    pub fn set_busy_timeout(&self, duration: std::time::Duration) {
        *self.busy_timeout.write() = duration;
    }

    pub fn get_busy_timeout(&self) -> std::time::Duration {
        *self.busy_timeout.read()
    }

    fn set_tx_state(&self, state: TransactionState) {
        *self.transaction_state.write() = state;
    }

    fn get_tx_state(&self) -> TransactionState {
        *self.transaction_state.read()
    }

    pub(crate) fn get_mv_tx_id(&self) -> Option<u64> {
        self.mv_tx.read().map(|(tx_id, _)| tx_id)
    }

    pub(crate) fn get_mv_tx(&self) -> Option<(u64, TransactionMode)> {
        *self.mv_tx.read()
    }
}

#[derive(Debug)]
struct BusyTimeout {
    /// Busy timeout instant
    timeout: Instant,
    /// Next iteration index for DELAYS
    iteration: usize,
}

impl BusyTimeout {
    const DELAYS: [std::time::Duration; 12] = [
        Duration::from_millis(1),
        Duration::from_millis(2),
        Duration::from_millis(5),
        Duration::from_millis(10),
        Duration::from_millis(15),
        Duration::from_millis(20),
        Duration::from_millis(25),
        Duration::from_millis(25),
        Duration::from_millis(25),
        Duration::from_millis(50),
        Duration::from_millis(50),
        Duration::from_millis(100),
    ];

    const TOTALS: [std::time::Duration; 12] = [
        Duration::from_millis(0),
        Duration::from_millis(1),
        Duration::from_millis(3),
        Duration::from_millis(8),
        Duration::from_millis(18),
        Duration::from_millis(33),
        Duration::from_millis(53),
        Duration::from_millis(78),
        Duration::from_millis(103),
        Duration::from_millis(128),
        Duration::from_millis(178),
        Duration::from_millis(228),
    ];

    pub fn new(now: Instant) -> Self {
        Self {
            timeout: now,
            iteration: 0,
        }
    }

    // implementation of sqliteDefaultBusyCallback
    pub fn busy_callback(&mut self, now: Instant, max_duration: Duration) {
        let idx = self.iteration.min(11);
        let mut delay = Self::DELAYS[idx];
        let mut prior = Self::TOTALS[idx];

        if self.iteration >= 12 {
            prior += delay * (self.iteration as u32 - 11);
        }

        if prior + delay > max_duration {
            delay = max_duration.saturating_sub(prior);
            // no more waiting after this
            if delay.is_zero() {
                return;
            }
        }

        self.iteration += 1;
        self.timeout = now + delay;
    }
}

pub struct Statement {
    program: vdbe::Program,
    state: vdbe::ProgramState,
    mv_store: Option<Arc<MvStore>>,
    pager: Arc<Pager>,
    /// Whether the statement accesses the database.
    /// Used to determine whether we need to check for schema changes when
    /// starting a transaction.
    accesses_db: bool,
    /// indicates if the statement is a NORMAL/EXPLAIN/EXPLAIN QUERY PLAN
    query_mode: QueryMode,
    /// Flag to show if the statement was busy
    busy: bool,
    /// Busy timeout instant
    /// We need Option here because `io.now()` is not a cheap call
    busy_timeout: Option<BusyTimeout>,
}

impl Statement {
    pub fn new(
        program: vdbe::Program,
        mv_store: Option<Arc<MvStore>>,
        pager: Arc<Pager>,
        query_mode: QueryMode,
    ) -> Self {
        let accesses_db = program.accesses_db;
        let (max_registers, cursor_count) = match query_mode {
            QueryMode::Normal => (program.max_registers, program.cursor_ref.len()),
            QueryMode::Explain => (EXPLAIN_COLUMNS.len(), 0),
            QueryMode::ExplainQueryPlan => (EXPLAIN_QUERY_PLAN_COLUMNS.len(), 0),
        };
        let state = vdbe::ProgramState::new(max_registers, cursor_count);
        Self {
            program,
            state,
            mv_store,
            pager,
            accesses_db,
            query_mode,
            busy: false,
            busy_timeout: None,
        }
    }
    pub fn get_query_mode(&self) -> QueryMode {
        self.query_mode
    }

    pub fn n_change(&self) -> i64 {
        self.program
            .n_change
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn set_mv_tx(&mut self, mv_tx: Option<(u64, TransactionMode)>) {
        *self.program.connection.mv_tx.write() = mv_tx;
    }

    pub fn interrupt(&mut self) {
        self.state.interrupt();
    }

    pub fn step(&mut self) -> Result<StepResult> {
        if let Some(busy_timeout) = self.busy_timeout.as_ref() {
            if self.pager.io.now() < busy_timeout.timeout {
                // Yield the query as the timeout has not been reached yet
                return Ok(StepResult::IO);
            }
        }

        let mut res = if !self.accesses_db {
            self.program.step(
                &mut self.state,
                self.mv_store.clone(),
                self.pager.clone(),
                self.query_mode,
            )
        } else {
            const MAX_SCHEMA_RETRY: usize = 50;
            let mut res = self.program.step(
                &mut self.state,
                self.mv_store.clone(),
                self.pager.clone(),
                self.query_mode,
            );
            for attempt in 0..MAX_SCHEMA_RETRY {
                // Only reprepare if we still need to update schema
                if !matches!(res, Err(LimboError::SchemaUpdated)) {
                    break;
                }
                tracing::debug!("reprepare: attempt={}", attempt);
                self.reprepare()?;
                res = self.program.step(
                    &mut self.state,
                    self.mv_store.clone(),
                    self.pager.clone(),
                    self.query_mode,
                );
            }
            res
        };

        // Aggregate metrics when statement completes
        if matches!(res, Ok(StepResult::Done)) {
            let mut conn_metrics = self.program.connection.metrics.write();
            conn_metrics.record_statement(self.state.metrics.clone());
            self.busy = false;
        } else {
            self.busy = true;
        }

        if matches!(res, Ok(StepResult::Busy)) {
            let now = self.pager.io.now();
            let max_duration = *self.program.connection.busy_timeout.read();
            self.busy_timeout = match self.busy_timeout.take() {
                None => {
                    let mut result = BusyTimeout::new(now);
                    result.busy_callback(now, max_duration);
                    Some(result)
                }
                Some(mut bt) => {
                    bt.busy_callback(now, max_duration);
                    Some(bt)
                }
            };

            if now < self.busy_timeout.as_ref().unwrap().timeout {
                res = Ok(StepResult::IO);
            }
        }

        res
    }

    pub(crate) fn run_ignore_rows(&mut self) -> Result<()> {
        loop {
            match self.step()? {
                vdbe::StepResult::Done => return Ok(()),
                vdbe::StepResult::IO => self.run_once()?,
                vdbe::StepResult::Row => continue,
                vdbe::StepResult::Interrupt | vdbe::StepResult::Busy => {
                    return Err(LimboError::Busy)
                }
            }
        }
    }

    pub(crate) fn run_collect_rows(&mut self) -> Result<Vec<Vec<Value>>> {
        let mut values = Vec::new();
        loop {
            match self.step()? {
                vdbe::StepResult::Done => return Ok(values),
                vdbe::StepResult::IO => self.run_once()?,
                vdbe::StepResult::Row => {
                    values.push(self.row().unwrap().get_values().cloned().collect());
                    continue;
                }
                vdbe::StepResult::Interrupt | vdbe::StepResult::Busy => {
                    return Err(LimboError::Busy)
                }
            }
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn reprepare(&mut self) -> Result<()> {
        tracing::trace!("repreparing statement");
        let conn = self.program.connection.clone();
        *conn.schema.write() = conn.db.clone_schema()?;
        self.program = {
            let mut parser = Parser::new(self.program.sql.as_bytes());
            let cmd = parser.next_cmd()?;
            let cmd = cmd.expect("Same SQL string should be able to be parsed");

            let syms = conn.syms.read();
            let mode = self.query_mode;
            debug_assert_eq!(QueryMode::new(&cmd), mode,);
            let (Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt)) = cmd;
            translate::translate(
                conn.schema.read().deref(),
                stmt,
                self.pager.clone(),
                conn.clone(),
                &syms,
                mode,
                &self.program.sql,
            )?
        };

        // Save parameters before they are reset
        let parameters = std::mem::take(&mut self.state.parameters);
        let (max_registers, cursor_count) = match self.query_mode {
            QueryMode::Normal => (self.program.max_registers, self.program.cursor_ref.len()),
            QueryMode::Explain => (EXPLAIN_COLUMNS.len(), 0),
            QueryMode::ExplainQueryPlan => (EXPLAIN_QUERY_PLAN_COLUMNS.len(), 0),
        };
        self._reset(Some(max_registers), Some(cursor_count));
        // Load the parameters back into the state
        self.state.parameters = parameters;
        Ok(())
    }

    pub fn run_once(&self) -> Result<()> {
        let res = self.pager.io.step();
        if self
            .program
            .connection
            .is_nested_stmt
            .load(Ordering::SeqCst)
        {
            return res;
        }
        if res.is_err() {
            if let Some(io) = &self.state.io_completions {
                io.abort();
            }
            let state = self.program.connection.get_tx_state();
            if let TransactionState::Write { .. } = state {
                let end_tx_res = self.pager.end_tx(true, &self.program.connection)?;
                self.program.connection.set_tx_state(TransactionState::None);
                assert!(
                    matches!(end_tx_res, IOResult::Done(_)),
                    "end_tx should not return IO as it should just end txn without flushing anything. Got {end_tx_res:?}"
                );
            }
        }
        res
    }

    pub fn num_columns(&self) -> usize {
        match self.query_mode {
            QueryMode::Normal => self.program.result_columns.len(),
            QueryMode::Explain => EXPLAIN_COLUMNS.len(),
            QueryMode::ExplainQueryPlan => EXPLAIN_QUERY_PLAN_COLUMNS.len(),
        }
    }

    pub fn get_column_name(&self, idx: usize) -> Cow<'_, str> {
        match self.query_mode {
            QueryMode::Normal => {
                let column = &self.program.result_columns.get(idx).expect("No column");
                match column.name(&self.program.table_references) {
                    Some(name) => Cow::Borrowed(name),
                    None => {
                        let tables = [&self.program.table_references];
                        let ctx = PlanContext(&tables);
                        Cow::Owned(column.expr.displayer(&ctx).to_string())
                    }
                }
            }
            QueryMode::Explain => Cow::Borrowed(EXPLAIN_COLUMNS[idx]),
            QueryMode::ExplainQueryPlan => Cow::Borrowed(EXPLAIN_QUERY_PLAN_COLUMNS[idx]),
        }
    }

    pub fn get_column_table_name(&self, idx: usize) -> Option<Cow<'_, str>> {
        let column = &self.program.result_columns.get(idx).expect("No column");
        match &column.expr {
            turso_parser::ast::Expr::Column { table, .. } => self
                .program
                .table_references
                .find_table_by_internal_id(*table)
                .map(|table_ref| Cow::Borrowed(table_ref.get_name())),
            _ => None,
        }
    }

    pub fn get_column_type(&self, idx: usize) -> Option<String> {
        let column = &self.program.result_columns.get(idx).expect("No column");
        match &column.expr {
            turso_parser::ast::Expr::Column {
                table,
                column: column_idx,
                ..
            } => {
                let table_ref = self
                    .program
                    .table_references
                    .find_table_by_internal_id(*table)?;
                let table_column = table_ref.get_column_at(*column_idx)?;
                match &table_column.ty {
                    crate::schema::Type::Integer => Some("INTEGER".to_string()),
                    crate::schema::Type::Real => Some("REAL".to_string()),
                    crate::schema::Type::Text => Some("TEXT".to_string()),
                    crate::schema::Type::Blob => Some("BLOB".to_string()),
                    crate::schema::Type::Numeric => Some("NUMERIC".to_string()),
                    crate::schema::Type::Null => None,
                }
            }
            _ => None,
        }
    }

    pub fn parameters(&self) -> &parameters::Parameters {
        &self.program.parameters
    }

    pub fn parameters_count(&self) -> usize {
        self.program.parameters.count()
    }

    pub fn parameter_index(&self, name: &str) -> Option<NonZero<usize>> {
        self.program.parameters.index(name)
    }

    pub fn bind_at(&mut self, index: NonZero<usize>, value: Value) {
        self.state.bind_at(index, value);
    }

    pub fn clear_bindings(&mut self) {
        self.state.clear_bindings();
    }

    pub fn reset(&mut self) {
        self._reset(None, None);
    }

    pub fn _reset(&mut self, max_registers: Option<usize>, max_cursors: Option<usize>) {
        self.state.reset(max_registers, max_cursors);
        self.busy = false;
        self.busy_timeout = None;
    }

    pub fn row(&self) -> Option<&Row> {
        self.state.result_row.as_ref()
    }

    pub fn get_sql(&self) -> &str {
        &self.program.sql
    }

    pub fn is_busy(&self) -> bool {
        self.busy
    }
}

pub type Row = vdbe::Row;

pub type StepResult = vdbe::StepResult;

#[derive(Default)]
pub struct SymbolTable {
    pub functions: HashMap<String, Arc<function::ExternalFunc>>,
    pub vtabs: HashMap<String, Arc<VirtualTable>>,
    pub vtab_modules: HashMap<String, Arc<crate::ext::VTabImpl>>,
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
        match self.parser.next_cmd() {
            Ok(Some(cmd)) => {
                let byte_offset_end = self.parser.offset();
                let input = str::from_utf8(&self.statements[self.last_offset..byte_offset_end])
                    .unwrap()
                    .trim();
                self.last_offset = byte_offset_end;
                Some(self.conn.run_cmd(cmd, input))
            }
            Ok(None) => None,
            Err(err) => Some(Result::Err(LimboError::from(err))),
        }
    }
}
