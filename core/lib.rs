#![allow(clippy::arc_with_non_send_sync)]
extern crate core;
mod assert;
pub mod busy;
mod connection;
mod error;
mod ext;
mod fast_lock;
mod function;
#[cfg(any(feature = "fuzz", feature = "bench"))]
pub mod functions;
#[cfg(not(any(feature = "fuzz", feature = "bench")))]
mod functions;
mod incremental;
pub mod index_method;
mod info;
pub mod io;
#[cfg(all(feature = "json", any(feature = "fuzz", feature = "bench")))]
pub mod json;
#[cfg(all(feature = "json", not(any(feature = "fuzz", feature = "bench"))))]
mod json;
pub mod mvcc;
mod parameters;
mod pragma;
mod pseudo;
pub mod schema;
#[cfg(feature = "series")]
mod series;
pub mod state_machine;
mod statement;
mod stats;
pub mod storage;
#[allow(dead_code)]
#[cfg(feature = "time")]
mod time;
mod translate;
pub mod types;
mod util;
#[cfg(feature = "uuid")]
mod uuid;
#[cfg(any(feature = "fuzz", feature = "bench"))]
pub mod vdbe;
#[cfg(not(any(feature = "fuzz", feature = "bench")))]
mod vdbe;
pub mod vector;
mod vtab;

#[cfg(any(feature = "fuzz", feature = "bench"))]
pub mod numeric;

#[cfg(not(any(feature = "fuzz", feature = "bench")))]
mod numeric;

#[cfg(any(feature = "fuzz", feature = "bench"))]
pub use function::MathFunc;

use crate::busy::{BusyHandler, BusyHandlerCallback};
use crate::index_method::IndexMethod;
use crate::schema::Trigger;
use crate::stats::refresh_analyze_stats;
use crate::storage::checksum::CHECKSUM_REQUIRED_RESERVED_BYTES;
use crate::storage::encryption::AtomicCipherMode;
use crate::storage::journal_mode;
use crate::storage::pager::{self, AutoVacuumMode, HeaderRef, HeaderRefMut};
use crate::storage::sqlite3_ondisk::{RawVersion, Version};
use crate::sync::{
    atomic::{
        AtomicBool, AtomicI32, AtomicI64, AtomicIsize, AtomicU16, AtomicU64, AtomicUsize, Ordering,
    },
    Arc, LazyLock, Weak,
};
use crate::sync::{Mutex, RwLock};
use crate::translate::pragma::TURSO_CDC_DEFAULT_TABLE_NAME;
use crate::vdbe::metrics::ConnectionMetrics;
use crate::vtab::VirtualTable;
use crate::{incremental::view::AllViewsTxState, translate::emitter::TransactionMode};
use arc_swap::{ArcSwap, ArcSwapOption};
pub use connection::{resolve_ext_path, Connection, Row, StepResult, SymbolTable};
use core::str;
pub use error::{CompletionError, LimboError};
pub use io::clock::{Clock, MonotonicInstant, WallClockInstant};
#[cfg(all(feature = "fs", target_family = "unix", not(miri)))]
pub use io::UnixIO;
#[cfg(all(feature = "fs", target_os = "linux", feature = "io_uring", not(miri)))]
pub use io::UringIO;
pub use io::{
    Buffer, Completion, CompletionType, File, GroupCompletion, MemoryIO, OpenFlags, PlatformIO,
    SyscallIO, WriteCompletion, IO,
};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use schema::Schema;
pub use statement::Statement;
use std::time::Duration;
use std::{
    fmt::{self},
    ops::Deref,
};
#[cfg(feature = "fs")]
use storage::database::DatabaseFile;
pub use storage::database::IOContext;
pub use storage::encryption::{CipherMode, EncryptionContext, EncryptionKey};
use storage::page_cache::PageCache;
use storage::sqlite3_ondisk::PageSize;
pub use storage::{
    buffer_pool::BufferPool,
    database::DatabaseStorage,
    pager::PageRef,
    pager::{Page, Pager},
    wal::{CheckpointMode, CheckpointResult, Wal, WalFile, WalFileShared},
};
use tracing::{instrument, Level};
use turso_macros::{match_ignore_ascii_case, AtomicEnum};
use turso_parser::{ast, ast::Cmd, parser::Parser};
pub use types::IOResult;
pub use types::Value;
pub use types::ValueRef;
use util::parse_schema_rows;
pub use util::IOExt;
pub use vdbe::{
    builder::QueryMode, explain::EXPLAIN_COLUMNS, explain::EXPLAIN_QUERY_PLAN_COLUMNS,
    FromValueRow, PrepareContext, PreparedProgram, Program, Register,
};

#[cfg(feature = "cli_only")]
pub mod dbpage;

pub(crate) mod sync;
pub(crate) mod thread;

/// Configuration for database features
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DatabaseOpts {
    pub enable_views: bool,
    pub enable_strict: bool,
    pub enable_encryption: bool,
    pub enable_index_method: bool,
    pub enable_autovacuum: bool,
    pub enable_triggers: bool,
    enable_load_extension: bool,
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

    pub fn with_views(mut self, enable: bool) -> Self {
        self.enable_views = enable;
        self
    }

    pub fn with_strict(mut self, enable: bool) -> Self {
        self.enable_strict = enable;
        self
    }

    pub fn with_encryption(mut self, enable: bool) -> Self {
        self.enable_encryption = enable;
        self
    }

    pub fn with_index_method(mut self, enable: bool) -> Self {
        self.enable_index_method = enable;
        self
    }

    pub fn with_autovacuum(mut self, enable: bool) -> Self {
        self.enable_autovacuum = enable;
        self
    }

    pub fn with_triggers(mut self, enable: bool) -> Self {
        self.enable_triggers = enable;
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

#[derive(Clone, AtomicEnum, Copy, PartialEq, Eq, Debug)]
enum TransactionState {
    Write {
        schema_did_change: bool,
    },
    Read,
    /// PendingUpgrade remembers what transaction state was before upgrade to write (has_read_txn is true if before transaction were in Read state)
    /// This is important, because if we failed to initialize write transaction immediatley - we need to end implicitly started read txn (e.g. for simiple INSERT INTO operation)
    /// But for late upgrade of transaction we should keep read transaction active (e.g. BEGIN; SELECT ...; INSERT INTO ...)
    PendingUpgrade {
        has_read_txn: bool,
    },
    None,
}

#[derive(Debug, AtomicEnum, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    Off = 0,
    Full = 2,
}

/// Control where temporary tables and indices are stored.
/// Matches SQLite's PRAGMA temp_store values:
/// - 0 = DEFAULT (use compile-time default, which is FILE)
/// - 1 = FILE (always use temp files on disk)
/// - 2 = MEMORY (always use in-memory storage)
#[derive(Debug, AtomicEnum, Clone, Copy, PartialEq, Eq, Default)]
pub enum TempStore {
    #[default]
    Default = 0,
    File = 1,
    Memory = 2,
}

pub(crate) type MvStore = mvcc::MvStore<mvcc::LocalClock>;

pub(crate) type MvCursor = mvcc::cursor::MvccLazyCursor<mvcc::LocalClock>;

/// Creates a read completion for database header reads that checks for short reads.
/// The header is always on page 1, so this function hardcodes that page index.
fn new_header_read_completion(buf: Arc<Buffer>) -> Completion {
    let expected = buf.len();
    Completion::new_read(buf, move |res| {
        let Ok((_buf, bytes_read)) = res else {
            return None; // IO error already captured in completion
        };
        if (bytes_read as usize) < expected {
            tracing::error!(
                "short read on database header: expected {expected} bytes, got {bytes_read}"
            );
            return Some(CompletionError::ShortRead {
                page_idx: 1, // header is on page 1
                expected,
                actual: bytes_read as usize,
            });
        }
        None
    })
}

/// Phase tracking for async database opening
#[derive(Default, Debug)]
pub enum OpenDbAsyncPhase {
    #[default]
    Init,
    ReadingHeader,
    LoadingSchema,
    BootstrapMvStore,
    Done,
}

/// State machine for async database opening
pub struct OpenDbAsyncState {
    phase: OpenDbAsyncPhase,
    db: Option<Arc<Database>>,
    pager: Option<Arc<Pager>>,
    conn: Option<Arc<Connection>>,
    encryption_key: Option<EncryptionKey>,
    make_from_btree_state: schema::MakeFromBtreeState,
}

impl Default for OpenDbAsyncState {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenDbAsyncState {
    pub fn new() -> Self {
        Self {
            phase: OpenDbAsyncPhase::Init,
            db: None,
            pager: None,
            conn: None,
            encryption_key: None,
            make_from_btree_state: schema::MakeFromBtreeState::new(),
        }
    }
}

/// The database manager ensures that there is a single, shared
/// `Database` object per a database file. We need because it is not safe
/// to have multiple independent WAL files open because coordination
/// happens at process-level POSIX file advisory locks.
///
/// Uses parking_lot::Mutex instead of crate::sync::Mutex because this static
/// must persist across shuttle test iterations. Shuttle resets its execution
/// state between iterations, but static variables persist - using shuttle's
/// Mutex here would cause panics when the second iteration tries to lock a
/// mutex that belongs to a stale execution context.
static DATABASE_MANAGER: LazyLock<parking_lot::Mutex<HashMap<String, Weak<Database>>>> =
    LazyLock::new(|| parking_lot::Mutex::new(HashMap::default()));

/// The `Database` object contains per database file state that is shared
/// between multiple connections.
///
/// Do that `Database` object is cached and can be long lived. DO NOT store anything sensitive like
/// encryption key here.
pub struct Database {
    mv_store: ArcSwapOption<MvStore>,
    schema: Mutex<Arc<Schema>>,
    pub db_file: Arc<dyn DatabaseStorage>,
    pub path: String,
    wal_path: String,
    pub io: Arc<dyn IO>,
    buffer_pool: Arc<BufferPool>,
    // Shared structures of a Database are the parts that are common to multiple threads that might
    // create DB connections.
    _shared_page_cache: Arc<RwLock<PageCache>>,
    shared_wal: Arc<RwLock<WalFileShared>>,
    init_lock: Arc<Mutex<()>>,
    open_flags: OpenFlags,
    // Use parking lot RwLock here and not `crate::sync::RwLock` because it relies on `data_ptr` and that is experimental
    // in std.
    builtin_syms: parking_lot::RwLock<SymbolTable>,
    opts: DatabaseOpts,
    n_connections: AtomicUsize,

    /// In Memory Page 1 for Empty Dbs
    init_page_1: Arc<ArcSwapOption<Page>>,

    // Encryption
    encryption_cipher_mode: AtomicCipherMode,
}

// SAFETY: This needs to be audited for thread safety.
// See: https://github.com/tursodatabase/turso/issues/1552
crate::assert::assert_send_sync!(Database);

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("Database");
        debug_struct
            .field("path", &self.path)
            .field("open_flags", &self.open_flags);

        // Database state information
        let db_state_value = match &*self.init_page_1.load() {
            // If init_page1 exists, this means the DB is empty
            Some(_) => "uninitialized",
            None => "initialized",
        };
        debug_struct.field("db_state", &db_state_value);

        let mv_store_status = if self.get_mv_store().is_some() {
            "present"
        } else {
            "none"
        };
        debug_struct.field("mv_store", &mv_store_status);

        let init_lock_status = if self.init_lock.try_lock().is_some() {
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
            &self
                .n_connections
                .load(crate::sync::atomic::Ordering::SeqCst),
        );
        debug_struct.finish()
    }
}

impl Database {
    fn new(
        opts: DatabaseOpts,
        flags: OpenFlags,
        path: impl Into<String>,
        wal_path: impl Into<String>,
        io: &Arc<dyn IO>,
        db_file: Arc<dyn DatabaseStorage>,
        encryption_opts: Option<EncryptionOpts>,
    ) -> Result<Self> {
        let shared_wal = WalFileShared::new_noop();
        let mv_store = ArcSwapOption::empty();

        let db_size = db_file.size()?;

        let shared_page_cache = Arc::new(RwLock::new(PageCache::default()));
        let syms = SymbolTable::new();
        let arena_size = if std::env::var("TESTING").is_ok_and(|v| v.eq_ignore_ascii_case("true")) {
            BufferPool::TEST_ARENA_SIZE
        } else {
            BufferPool::DEFAULT_ARENA_SIZE
        };

        let encryption_cipher_mode = if let Some(encryption_opts) = encryption_opts {
            Some(CipherMode::try_from(encryption_opts.cipher.as_str())?)
        } else {
            None
        };

        let init_page_1 = if db_size == 0 {
            let default_page_1 = pager::default_page1(encryption_cipher_mode.as_ref());

            Some(default_page_1)
        } else {
            None
        };

        // opts is now passed as parameter
        let db = Database {
            mv_store,
            path: path.into(),
            wal_path: wal_path.into(),
            schema: Mutex::new(Arc::new(Schema::new())),
            _shared_page_cache: shared_page_cache.clone(),
            shared_wal,
            db_file,
            builtin_syms: parking_lot::RwLock::new(syms),
            io: io.clone(),
            open_flags: flags,
            init_lock: Arc::new(Mutex::new(())),
            opts,
            buffer_pool: BufferPool::begin_init(io, arena_size),
            n_connections: AtomicUsize::new(0),

            init_page_1: Arc::new(ArcSwapOption::new(init_page_1)),

            encryption_cipher_mode: AtomicCipherMode::new(
                encryption_cipher_mode.unwrap_or(CipherMode::None),
            ),
        };

        db.register_global_builtin_extensions()
            .expect("unable to register global extensions");
        Ok(db)
    }

    #[cfg(feature = "fs")]
    pub fn open_file(io: Arc<dyn IO>, path: &str) -> Result<Arc<Database>> {
        Self::open_file_with_flags(io, path, OpenFlags::default(), DatabaseOpts::new(), None)
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
    ) -> Result<Arc<Database>> {
        Self::open_with_flags(
            io,
            path,
            db_file,
            OpenFlags::default(),
            DatabaseOpts::new(),
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

        let mut registry = DATABASE_MANAGER.lock();

        let canonical_path = std::fs::canonicalize(path)
            .ok()
            .and_then(|p| p.to_str().map(|s| s.to_string()))
            .unwrap_or_else(|| path.to_string());

        if let Some(db) = registry.get(&canonical_path).and_then(Weak::upgrade) {
            tracing::debug!("took database {canonical_path:?} from the registry");

            // Check encryption compatibility using cipher mode (key is not stored in Database for security)
            let db_is_encrypted = !matches!(db.encryption_cipher_mode.get(), CipherMode::None);

            if db_is_encrypted && encryption_opts.is_none() {
                return Err(LimboError::InvalidArgument(
                    "Database is encrypted but no encryption options provided".to_string(),
                ));
            }

            return Ok(db);
        }
        tracing::debug!("initialize database {canonical_path:?} and put in the registry");
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
        // Parse encryption key from encryption_opts if provided
        let encryption_key = if let Some(ref enc_opts) = encryption_opts {
            Some(EncryptionKey::from_hex_string(&enc_opts.hexkey)?)
        } else {
            None
        };

        let mut db = Self::new(
            opts,
            flags,
            path,
            wal_path,
            &io,
            db_file,
            encryption_opts.clone(),
        )?;

        let pager = db.header_validation(encryption_key.as_ref())?;
        #[cfg(debug_assertions)]
        {
            let wal_enabled = db.shared_wal.read().enabled.load(Ordering::SeqCst);
            let mv_store_enabled = db.get_mv_store().is_some();
            assert!(
                db.is_readonly() || wal_enabled || mv_store_enabled,
                "Either WAL or MVStore must be enabled"
            );
        }

        // Check: https://github.com/tursodatabase/turso/pull/1761#discussion_r2154013123

        // Wrap db in Arc before connecting
        let db = Arc::new(db);

        // parse schema
        let conn = db._connect(false, Some(pager.clone()), encryption_key.clone())?;
        let syms = conn.syms.read();
        let pager = conn.pager.load();

        let enable_triggers = db.opts.enable_triggers;
        db.with_schema_mut(|schema| {
            let header_schema_cookie = pager
                .io
                .block(|| pager.with_header(|header| header.schema_cookie.get()))?;
            schema.schema_version = header_schema_cookie;
            let mut make_from_btree_state = schema::MakeFromBtreeState::new();
            let result = io.block(|| {
                schema.make_from_btree(
                    &mut make_from_btree_state,
                    None,
                    pager.clone(),
                    &syms,
                    enable_triggers,
                )
            });
            match result {
                Err(LimboError::ExtensionError(e)) => {
                    // this means that a vtab exists and we no longer have the module loaded. we print
                    // a warning to the user to load the module
                    make_from_btree_state.cleanup(&pager);
                    eprintln!("Warning: {e}");
                }
                Err(e) => {
                    make_from_btree_state.cleanup(&pager);
                    return Err(e);
                }
                Ok(()) => {}
            }

            Ok(())
        })?;

        if let Some(mv_store) = db.get_mv_store().as_ref() {
            let mvcc_bootstrap_conn = db._connect(true, Some(pager.clone()), encryption_key)?;
            mv_store.bootstrap(mvcc_bootstrap_conn)?;
        }

        Ok(db)
    }

    /// Async version of database opening that returns IOResult.
    /// Caller must drive the IO loop and pass state between calls.
    /// This is useful for sync engine which needs to yield on IO.
    #[allow(clippy::arc_with_non_send_sync)]
    #[allow(clippy::too_many_arguments)]
    pub fn open_with_flags_bypass_registry_async(
        state: &mut OpenDbAsyncState,
        io: Arc<dyn IO>,
        path: &str,
        wal_path: &str,
        db_file: Arc<dyn DatabaseStorage>,
        flags: OpenFlags,
        opts: DatabaseOpts,
        encryption_opts: Option<EncryptionOpts>,
    ) -> Result<IOResult<Arc<Database>>> {
        use crate::return_if_io;

        loop {
            tracing::trace!(
                "open_with_flags_bypass_registry_async: state.phase={:?}",
                state.phase
            );
            match &state.phase {
                OpenDbAsyncPhase::Init => {
                    // Parse encryption key from encryption_opts if provided
                    let encryption_key = if let Some(ref enc_opts) = encryption_opts {
                        Some(EncryptionKey::from_hex_string(&enc_opts.hexkey)?)
                    } else {
                        None
                    };

                    let mut db = Self::new(
                        opts,
                        flags,
                        path,
                        wal_path,
                        &io,
                        db_file.clone(),
                        encryption_opts.clone(),
                    )?;

                    let pager = db.header_validation(encryption_key.as_ref())?;

                    #[cfg(debug_assertions)]
                    {
                        let wal_enabled = db.shared_wal.read().enabled.load(Ordering::SeqCst);
                        let mv_store_enabled = db.get_mv_store().is_some();
                        assert!(
                            db.is_readonly() || wal_enabled || mv_store_enabled,
                            "Either WAL or MVStore must be enabled"
                        );
                    }

                    // Wrap db in Arc before connecting
                    let db = Arc::new(db);
                    let conn = db._connect(false, Some(pager.clone()), encryption_key.clone())?;

                    state.db = Some(db);
                    state.pager = Some(pager);
                    state.conn = Some(conn);
                    state.encryption_key = encryption_key;
                    state.phase = OpenDbAsyncPhase::ReadingHeader;
                }

                OpenDbAsyncPhase::ReadingHeader => {
                    let pager = state.pager.as_ref().unwrap();
                    let header_schema_cookie =
                        return_if_io!(pager.with_header(|header| header.schema_cookie.get()));

                    let db = state.db.as_ref().unwrap();
                    db.with_schema_mut(|schema| {
                        schema.schema_version = header_schema_cookie;
                        Ok(())
                    })?;

                    state.phase = OpenDbAsyncPhase::LoadingSchema;
                }

                OpenDbAsyncPhase::LoadingSchema => {
                    let db = state.db.as_ref().unwrap();
                    let pager = state.pager.as_ref().unwrap();
                    let conn = state.conn.as_ref().unwrap();
                    let syms = conn.syms.read();
                    let enable_triggers = db.opts.enable_triggers;

                    let result = db.with_schema_mut(|schema| {
                        schema.make_from_btree(
                            &mut state.make_from_btree_state,
                            None,
                            pager.clone(),
                            &syms,
                            enable_triggers,
                        )
                    });

                    match result {
                        Ok(IOResult::IO(io)) => return Ok(IOResult::IO(io)),
                        Ok(IOResult::Done(())) => {}
                        Err(LimboError::ExtensionError(e)) => {
                            state.make_from_btree_state.cleanup(pager);
                            eprintln!("Warning: {e}");
                        }
                        Err(e) => {
                            state.make_from_btree_state.cleanup(pager);
                            return Err(e);
                        }
                    }

                    state.phase = OpenDbAsyncPhase::BootstrapMvStore;
                }

                OpenDbAsyncPhase::BootstrapMvStore => {
                    let db = state.db.as_ref().unwrap();
                    let pager = state.pager.as_ref().unwrap();

                    if let Some(mv_store) = db.get_mv_store().as_ref() {
                        let mvcc_bootstrap_conn =
                            db._connect(true, Some(pager.clone()), state.encryption_key.clone())?;
                        mv_store.bootstrap(mvcc_bootstrap_conn)?;
                    }

                    state.phase = OpenDbAsyncPhase::Done;
                    return Ok(IOResult::Done(state.db.take().unwrap()));
                }

                OpenDbAsyncPhase::Done => {
                    panic!("open_with_flags_bypass_registry_async called after completion");
                }
            }
        }
    }

    /// Necessary Pager initialization, so that we are prepared to read from Page 1.
    /// For encrypted databases, the encryption key must be provided to properly decrypt page 1.
    fn _init(&self, encryption_key: Option<&EncryptionKey>) -> Result<Pager> {
        let pager = self.init_pager(None)?;
        pager.enable_encryption(self.opts.enable_encryption);

        // Set up encryption context BEFORE reading the header page.
        // For encrypted databases, page 1 has:
        // - Bytes 0-15: Turso magic header (replaces SQLite magic)
        // - Bytes 16-100: Unencrypted header metadata
        // - Bytes 100+: Encrypted content
        // The encryption context is needed to properly decrypt page 1 when reopening.
        if let Some(key) = encryption_key {
            let cipher_mode = self.encryption_cipher_mode.get();
            pager.set_encryption_context(cipher_mode, key)?;
        }

        // Start read transaction before reading page 1 to acquire a read lock
        // that prevents concurrent checkpoints from truncating the WAL
        pager.begin_read_tx()?;

        // Read header within the read transaction, ensuring cleanup on error
        let result = (|| -> Result<AutoVacuumMode> {
            let header_ref = pager.io.block(|| HeaderRef::from_pager(&pager))?;
            let header = header_ref.borrow();

            let mode = if header.vacuum_mode_largest_root_page.get() > 0 {
                if header.incremental_vacuum_enabled.get() > 0 {
                    AutoVacuumMode::Incremental
                } else {
                    AutoVacuumMode::Full
                }
            } else {
                AutoVacuumMode::None
            };

            Ok(mode)
        })();

        // Always end read transaction, even on error
        pager.end_read_tx();

        let mode = result?;

        pager.set_auto_vacuum_mode(mode);

        Ok(pager)
    }

    /// Checks the Version numbers in the DatabaseHeader, and changes it according to the required options
    ///
    /// Will also open MVStore and WAL if needed
    fn header_validation(&mut self, encryption_key: Option<&EncryptionKey>) -> Result<Arc<Pager>> {
        let wal_exists = journal_mode::wal_exists(std::path::Path::new(&self.wal_path));
        let log_exists = journal_mode::logical_log_exists(std::path::Path::new(&self.path));
        let is_readonly = self.open_flags.contains(OpenFlags::ReadOnly);

        let mut pager = self._init(encryption_key)?;
        assert!(pager.wal.is_none(), "Pager should have no WAL yet");

        let is_autovacuumed_db = self.io.block(|| {
            pager.with_header(|header| {
                header.vacuum_mode_largest_root_page.get() > 0
                    || header.incremental_vacuum_enabled.get() > 0
            })
        })?;

        if is_autovacuumed_db && !self.opts.enable_autovacuum {
            tracing::warn!(
                        "Database has autovacuum enabled but --experimental-autovacuum flag is not set. Opening in readonly mode."
                    );
            self.open_flags |= OpenFlags::ReadOnly;
        }

        let header: HeaderRefMut = self.io.block(|| HeaderRefMut::from_pager(&pager))?;
        let header_mut = header.borrow_mut();
        let (read_version, write_version) = { (header_mut.read_version, header_mut.write_version) };
        // TODO: right now we don't support READ ONLY and no READ or WRITE in the Version header
        // https://www.sqlite.org/fileformat.html#file_format_version_numbers
        if read_version != write_version {
            return Err(LimboError::Corrupt(format!(
                "Read version `{read_version:?}` is not equal to Write version `{write_version:?} in database header`"
            )));
        }

        let (read_version, _write_version) = (
            read_version
                .to_version()
                .map_err(|val| LimboError::Corrupt(format!("Invalid read_version: {val}")))?,
            write_version
                .to_version()
                .map_err(|val| LimboError::Corrupt(format!("Invalid write_version: {val}")))?,
        );

        // Determine if we should open in MVCC mode based on the database header version
        // MVCC is controlled only by the database header (set via PRAGMA journal_mode)
        let open_mv_store = matches!(read_version, Version::Mvcc);

        // Now check the Header Version to see which mode the DB file really is on
        // Track if header was modified so we can write it to disk
        let header_modified = match read_version {
            Version::Legacy => {
                if is_readonly {
                    tracing::warn!("Database {} is opened in readonly mode, cannot convert Legacy mode to WAL. Running in Legacy mode.", self.path);
                    false
                } else {
                    // Convert Legacy to WAL mode
                    header_mut.read_version = RawVersion::from(Version::Wal);
                    header_mut.write_version = RawVersion::from(Version::Wal);
                    true
                }
            }
            Version::Wal => false,
            Version::Mvcc => false,
        };

        // Check for inconsistent state between database header and log files
        match (wal_exists, log_exists) {
            (true, true) => {
                return Err(LimboError::Corrupt(
                    "Both WAL and MVCC logical log file exist".to_string(),
                ));
            }
            (true, false) => {
                // If a WAL file exists but header says MVCC, that's an inconsistent state
                if open_mv_store {
                    return Err(LimboError::Corrupt(format!(
                        "WAL file exists for database {}, but database header indicates MVCC mode. Run PRAGMA wal_checkpoint(TRUNCATE) to truncate the WAL.",
                        self.path
                    )));
                }
            }
            (false, true) => {
                // If an MVCC log file exists but header says WAL, that's an inconsistent state
                if !open_mv_store {
                    return Err(LimboError::Corrupt(format!(
                        "MVCC logical log file exists for database {}, but database header indicates WAL mode. The database may be corrupted.",
                        self.path
                    )));
                }
            }
            (false, false) => {}
        };

        // If header was modified, write it directly to disk before we clear the cache
        // This must happen before WAL is attached since we need to write directly to the DB file
        if header_modified {
            let completion =
                storage::sqlite3_ondisk::begin_write_btree_page(&pager, header.page())?;
            self.io.wait_for_completion(completion)?;
        }

        drop(header);

        let flags = self.open_flags;

        // Always Open shared wal and set it in the Database and Pager.
        // MVCC currently requires a WAL open to function
        let shared_wal = WalFileShared::open_shared_if_exists(&self.io, &self.wal_path, flags)?;

        let last_checksum_and_max_frame = shared_wal.read().last_checksum_and_max_frame();
        let wal = Arc::new(WalFile::new(
            self.io.clone(),
            Arc::clone(&shared_wal),
            last_checksum_and_max_frame,
            pager.buffer_pool.clone(),
        ));

        self.shared_wal = shared_wal;
        pager.set_wal(wal);

        // Clear page cache after attaching WAL since pages may have been cached
        // from disk reads before WAL was attached. The WAL may contain newer
        // versions of these pages (e.g., page 1 with updated schema_cookie).
        pager.clear_page_cache(true);
        pager.set_schema_cookie(None);

        if open_mv_store {
            let mv_store =
                journal_mode::open_mv_store(self.io.as_ref(), &self.path, self.open_flags)?;
            self.mv_store.store(Some(mv_store));
        }

        Ok(Arc::new(pager))
    }

    #[instrument(skip_all, level = Level::INFO)]
    pub fn connect(self: &Arc<Database>) -> Result<Arc<Connection>> {
        self._connect(false, None, None)
    }

    /// Connect with an encryption key.
    /// Use this when opening an encrypted database where the key is known at connect time.
    #[instrument(skip_all, level = Level::INFO)]
    pub fn connect_with_encryption(
        self: &Arc<Database>,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Arc<Connection>> {
        self._connect(false, None, encryption_key)
    }

    #[instrument(skip_all, level = Level::INFO)]
    fn _connect(
        self: &Arc<Database>,
        is_mvcc_bootstrap_connection: bool,
        pager: Option<Arc<Pager>>,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Arc<Connection>> {
        let pager = if let Some(pager) = pager {
            pager
        } else {
            // Pass encryption key to _init so it can set up encryption context
            // before reading page 1. This is required for reopening encrypted databases.
            Arc::new(self._init(encryption_key.as_ref())?)
        };
        let page_size = pager.get_page_size_unchecked();

        let default_cache_size = pager
            .io
            .block(|| pager.with_header(|header| header.default_page_cache_size))
            .unwrap_or_default()
            .get();

        let encryption_cipher = self.encryption_cipher_mode.get();

        let conn = Arc::new(Connection {
            db: self.clone(),
            pager: ArcSwap::new(pager),
            schema: RwLock::new(self.schema.lock().clone()),
            database_schemas: RwLock::new(HashMap::default()),
            auto_commit: AtomicBool::new(true),
            transaction_state: AtomicTransactionState::new(TransactionState::None),
            last_insert_rowid: AtomicI64::new(0),
            last_change: AtomicI64::new(0),
            total_changes: AtomicI64::new(0),
            syms: parking_lot::RwLock::new(SymbolTable::new()),
            syms_generation: AtomicU64::new(0),
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
            nestedness: AtomicI32::new(0),
            compiling_triggers: RwLock::new(Vec::new()),
            executing_triggers: RwLock::new(Vec::new()),
            encryption_key: RwLock::new(encryption_key),
            encryption_cipher_mode: AtomicCipherMode::new(encryption_cipher),
            sync_mode: AtomicSyncMode::new(SyncMode::Full),
            temp_store: AtomicTempStore::new(TempStore::Default),
            data_sync_retry: AtomicBool::new(false),
            busy_handler: RwLock::new(BusyHandler::None),
            is_mvcc_bootstrap_connection: AtomicBool::new(is_mvcc_bootstrap_connection),
            fk_pragma: AtomicBool::new(false),
            fk_deferred_violations: AtomicIsize::new(0),
            vtab_txn_states: RwLock::new(HashSet::default()),
        });
        self.n_connections
            .fetch_add(1, crate::sync::atomic::Ordering::SeqCst);
        let builtin_syms = self.builtin_syms.read();
        // add built-in extensions symbols to the connection to prevent having to load each time
        conn.syms.write().extend(&builtin_syms);
        refresh_analyze_stats(&conn);
        Ok(conn)
    }

    pub fn is_readonly(&self) -> bool {
        self.open_flags.contains(OpenFlags::ReadOnly)
    }

    /// If we do not have a physical WAL file, but we know the database file is initialized on disk,
    /// we need to read the page_size from the database header.
    fn read_page_size_from_db_header(&self) -> Result<PageSize> {
        turso_assert!(
            self.initialized(),
            "read_reserved_space_bytes_from_db_header called on uninitialized database"
        );
        turso_assert!(
            PageSize::MIN % 512 == 0,
            "header read must be a multiple of 512 for O_DIRECT"
        );
        let buf = Arc::new(Buffer::new_temporary(PageSize::MIN as usize));
        let c = new_header_read_completion(buf.clone());
        let c = self.db_file.read_header(c)?;
        self.io.wait_for_completion(c)?;
        let page_size = u16::from_be_bytes(buf.as_slice()[16..18].try_into().unwrap());
        let page_size = PageSize::new_from_header_u16(page_size)?;
        Ok(page_size)
    }

    fn read_reserved_space_bytes_from_db_header(&self) -> Result<u8> {
        turso_assert!(
            self.initialized(),
            "read_reserved_space_bytes_from_db_header called on uninitialized database"
        );
        turso_assert!(
            PageSize::MIN % 512 == 0,
            "header read must be a multiple of 512 for O_DIRECT"
        );
        let buf = Arc::new(Buffer::new_temporary(PageSize::MIN as usize));
        let c = new_header_read_completion(buf.clone());
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
        if self.initialized() {
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
        if self.initialized() {
            Ok(Some(self.read_reserved_space_bytes_from_db_header()?))
        } else {
            Ok(None)
        }
    }

    fn init_pager(&self, requested_page_size: Option<usize>) -> Result<Pager> {
        let cipher = self.encryption_cipher_mode.get();
        let reserved_bytes = self.maybe_get_reserved_space_bytes()?.or_else(|| {
            if !matches!(cipher, CipherMode::None) {
                // For encryption, use the cipher's metadata size
                Some(cipher.metadata_size() as u8)
            } else {
                // For non-encrypted databases, don't set reserved_bytes here.
                // This allows checksums to be enabled by default (disable_checksums will be false).
                None
            }
        });
        let disable_checksums = if let Some(reserved_bytes) = reserved_bytes {
            // if the required reserved bytes for checksums is not present, disable checksums
            reserved_bytes != CHECKSUM_REQUIRED_RESERVED_BYTES
        } else {
            false
        };
        // Check if WAL is enabled
        let shared_wal = self.shared_wal.read();

        let page_size = self.determine_actual_page_size(&shared_wal, requested_page_size)?;

        let buffer_pool = self.buffer_pool.clone();
        if self.initialized() {
            buffer_pool.finalize_with_page_size(page_size.get() as usize)?;
        }

        let pager_wal: Option<Arc<dyn Wal>> = if shared_wal.enabled.load(Ordering::SeqCst) {
            Some(Arc::new(WalFile::new(
                self.io.clone(),
                self.shared_wal.clone(),
                shared_wal.last_checksum_and_max_frame(),
                buffer_pool.clone(),
            )))
        } else {
            None
        };

        let pager = Pager::new(
            self.db_file.clone(),
            pager_wal,
            self.io.clone(),
            PageCache::default(),
            buffer_pool.clone(),
            self.init_lock.clone(),
            self.init_page_1.clone(),
        )?;
        pager.set_page_size(page_size);
        if let Some(reserved_bytes) = reserved_bytes {
            pager.set_reserved_space_bytes(reserved_bytes);
        }
        if disable_checksums {
            pager.reset_checksum_context();
        }

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
                #[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
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

    #[inline]
    pub(crate) fn initialized(&self) -> bool {
        self.init_page_1.load().is_none()
    }

    pub(crate) fn can_load_extensions(&self) -> bool {
        self.opts.enable_load_extension
    }

    #[inline]
    pub(crate) fn with_schema_mut<T>(&self, f: impl FnOnce(&mut Schema) -> Result<T>) -> Result<T> {
        let mut schema_ref = self.schema.lock();
        let schema = Arc::make_mut(&mut *schema_ref);
        f(schema)
    }
    pub(crate) fn clone_schema(&self) -> Arc<Schema> {
        let schema = self.schema.lock();
        schema.clone()
    }

    pub(crate) fn update_schema_if_newer(&self, another: Arc<Schema>) {
        let mut schema = self.schema.lock();
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
    }

    pub fn get_mv_store(&self) -> impl Deref<Target = Option<Arc<MvStore>>> {
        self.mv_store.load()
    }

    pub fn experimental_views_enabled(&self) -> bool {
        self.opts.enable_views
    }

    pub fn experimental_index_method_enabled(&self) -> bool {
        self.opts.enable_index_method
    }

    pub fn experimental_strict_enabled(&self) -> bool {
        self.opts.enable_strict
    }

    pub fn experimental_triggers_enabled(&self) -> bool {
        self.opts.enable_triggers
    }

    /// check if database is currently in MVCC mode
    pub fn mvcc_enabled(&self) -> bool {
        self.mv_store.load().is_some()
    }

    #[cfg(feature = "test_helper")]
    pub fn set_pending_byte(val: u32) {
        Pager::set_pending_byte(val);
    }

    #[cfg(feature = "test_helper")]
    pub fn get_pending_byte() -> u32 {
        Pager::get_pending_byte()
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
            name_to_index: HashMap::default(),
            index_to_data: HashMap::default(),
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

#[cfg(test)]
mod tests {
    use crate::{Database, DatabaseOpts, OpenFlags, PlatformIO};
    use rusqlite::Connection;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_autovacuum_readonly_behavior() {
        // (autovacuum_mode, enable_autovacuum_flag, expected_readonly)
        // TODO: Add encrypted case ("NONE", false, false) after fixing https://github.com/tursodatabase/turso/issues/4519
        let test_cases = [
            ("NONE", false, false),
            ("NONE", true, false),
            ("FULL", false, true),
            ("FULL", true, false),
            ("INCREMENTAL", false, true),
            ("INCREMENTAL", true, false),
        ];

        for (autovacuum_mode, enable_autovacuum_flag, expected_readonly) in test_cases {
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("test.db");

            {
                let conn = Connection::open(&db_path).unwrap();
                conn.pragma_update(None, "auto_vacuum", autovacuum_mode)
                    .unwrap();
            }

            let io = Arc::new(PlatformIO::new().unwrap()) as Arc<dyn crate::IO>;
            let opts = DatabaseOpts::new().with_autovacuum(enable_autovacuum_flag);
            let db = Database::open_file_with_flags(
                io,
                db_path.to_str().unwrap(),
                OpenFlags::default(),
                opts,
                None,
            )
            .unwrap();

            assert_eq!(
                db.is_readonly(),
                expected_readonly,
                "autovacuum={autovacuum_mode}, flag={enable_autovacuum_flag}: expected readonly={expected_readonly}"
            );
        }
    }
}
