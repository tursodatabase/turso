#![allow(clippy::arc_with_non_send_sync)]

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
mod storage;
mod translate;
pub mod types;
#[allow(dead_code)]
mod util;
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

use crate::vtab::VirtualTable;
use crate::{fast_lock::SpinLock, translate::optimizer::optimize_plan};
use core::str;
pub use error::LimboError;
use fallible_iterator::FallibleIterator;
pub use io::clock::{Clock, Instant};
#[cfg(all(feature = "fs", target_family = "unix"))]
pub use io::UnixIO;
#[cfg(all(feature = "fs", target_os = "linux", feature = "io_uring"))]
pub use io::UringIO;
pub use io::{
    Buffer, Completion, File, MemoryIO, OpenFlags, PlatformIO, SyscallIO, WriteCompletion, IO,
};
use limbo_sqlite3_parser::{ast, ast::Cmd, lexer::sql::Parser};
use parking_lot::RwLock;
use schema::Schema;
use std::{
    borrow::Cow,
    cell::{Cell, RefCell, UnsafeCell},
    collections::HashMap,
    fmt::Display,
    io::Write,
    num::NonZero,
    ops::Deref,
    rc::Rc,
    sync::{Arc, OnceLock},
};
use storage::btree::{btree_init_page, BTreePageInner};
#[cfg(feature = "fs")]
use storage::database::DatabaseFile;
pub use storage::pager::PagerCacheflushStatus;
pub use storage::{
    buffer_pool::BufferPool,
    database::DatabaseStorage,
    pager::PageRef,
    pager::{Page, Pager},
    wal::{CheckpointMode, CheckpointResult, CheckpointStatus, Wal, WalFile, WalFileShared},
};
use storage::{
    page_cache::DumbLruPageCache,
    pager::allocate_page,
    sqlite3_ondisk::{DatabaseHeader, DATABASE_HEADER_SIZE},
};
use tracing::{instrument, Level};
use translate::select::prepare_select_plan;
pub use types::RefValue;
pub use types::Value;
use util::parse_schema_rows;
use vdbe::builder::QueryMode;
use vdbe::builder::TableRefIdCounter;

pub type Result<T, E = LimboError> = std::result::Result<T, E>;
pub static DATABASE_VERSION: OnceLock<String> = OnceLock::new();

#[derive(Clone, Copy, PartialEq, Eq)]
enum TransactionState {
    Write,
    Read,
    None,
}

pub(crate) type MvStore = mvcc::MvStore<mvcc::LocalClock>;

pub(crate) type MvCursor = mvcc::cursor::ScanCursor<mvcc::LocalClock>;

pub struct Database {
    mv_store: Option<Rc<MvStore>>,
    schema: Arc<RwLock<Schema>>,
    // TODO: make header work without lock
    header: Arc<SpinLock<DatabaseHeader>>,
    db_file: Arc<dyn DatabaseStorage>,
    io: Arc<dyn IO>,
    page_size: u32,
    // Shared structures of a Database are the parts that are common to multiple threads that might
    // create DB connections.
    _shared_page_cache: Arc<RwLock<DumbLruPageCache>>,
    shared_wal: Arc<UnsafeCell<WalFileShared>>,
    open_flags: OpenFlags,
}

unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl Database {
    #[cfg(feature = "fs")]
    pub fn open_file(io: Arc<dyn IO>, path: &str, enable_mvcc: bool) -> Result<Arc<Database>> {
        Self::open_file_with_flags(io, path, OpenFlags::default(), enable_mvcc)
    }

    #[cfg(feature = "fs")]
    pub fn open_file_with_flags(
        io: Arc<dyn IO>,
        path: &str,
        flags: OpenFlags,
        enable_mvcc: bool,
    ) -> Result<Arc<Database>> {
        let file = io.open_file(path, flags, true)?;
        maybe_init_database_file(&file, &io)?;
        let db_file = Arc::new(DatabaseFile::new(file));
        Self::open_with_flags(io, path, db_file, flags, enable_mvcc)
    }

    #[allow(clippy::arc_with_non_send_sync)]
    pub fn open(
        io: Arc<dyn IO>,
        path: &str,
        db_file: Arc<dyn DatabaseStorage>,
        enable_mvcc: bool,
    ) -> Result<Arc<Database>> {
        Self::open_with_flags(io, path, db_file, OpenFlags::default(), enable_mvcc)
    }

    #[allow(clippy::arc_with_non_send_sync)]
    pub fn open_with_flags(
        io: Arc<dyn IO>,
        path: &str,
        db_file: Arc<dyn DatabaseStorage>,
        flags: OpenFlags,
        enable_mvcc: bool,
    ) -> Result<Arc<Database>> {
        let db_header = Pager::begin_open(db_file.clone())?;
        // ensure db header is there
        io.run_once()?;

        let page_size = db_header.lock().get_page_size();
        let wal_path = format!("{}-wal", path);
        let shared_wal = WalFileShared::open_shared(&io, wal_path.as_str(), page_size)?;

        DATABASE_VERSION.get_or_init(|| {
            let version = db_header.lock().version_number;
            version.to_string()
        });

        let mv_store = if enable_mvcc {
            Some(Rc::new(MvStore::new(
                mvcc::LocalClock::new(),
                mvcc::persistent_storage::Storage::new_noop(),
            )))
        } else {
            None
        };

        let shared_page_cache = Arc::new(RwLock::new(DumbLruPageCache::default()));
        let schema = Arc::new(RwLock::new(Schema::new()));
        let db = Database {
            mv_store,
            schema: schema.clone(),
            header: db_header.clone(),
            _shared_page_cache: shared_page_cache.clone(),
            shared_wal: shared_wal.clone(),
            db_file,
            io: io.clone(),
            page_size,
            open_flags: flags,
        };
        let db = Arc::new(db);
        {
            // parse schema
            let conn = db.connect()?;
            let rows = conn.query("SELECT * FROM sqlite_schema")?;
            let mut schema = schema
                .try_write()
                .expect("lock on schema should succeed first try");
            let syms = conn.syms.borrow();
            if let Err(LimboError::ExtensionError(e)) =
                parse_schema_rows(rows, &mut schema, io, &syms, None)
            {
                // this means that a vtab exists and we no longer have the module loaded. we print
                // a warning to the user to load the module
                eprintln!("Warning: {}", e);
            }
        }
        Ok(db)
    }

    pub fn connect(self: &Arc<Database>) -> Result<Arc<Connection>> {
        let buffer_pool = Rc::new(BufferPool::new(self.page_size as usize));

        let wal = Rc::new(RefCell::new(WalFile::new(
            self.io.clone(),
            self.page_size,
            self.shared_wal.clone(),
            buffer_pool.clone(),
        )));
        // For now let's open database without shared cache by default.
        let pager = Rc::new(Pager::finish_open(
            self.header.clone(),
            self.db_file.clone(),
            wal,
            self.io.clone(),
            Arc::new(RwLock::new(DumbLruPageCache::default())),
            buffer_pool,
        )?);
        let conn = Arc::new(Connection {
            _db: self.clone(),
            pager: pager.clone(),
            schema: self.schema.clone(),
            header: self.header.clone(),
            last_insert_rowid: Cell::new(0),
            auto_commit: Cell::new(true),
            mv_transactions: RefCell::new(Vec::new()),
            transaction_state: Cell::new(TransactionState::None),
            last_change: Cell::new(0),
            syms: RefCell::new(SymbolTable::default()),
            total_changes: Cell::new(0),
            _shared_cache: false,
            cache_size: Cell::new(self.header.lock().default_page_cache_size),
        });
        if let Err(e) = conn.register_builtins() {
            return Err(LimboError::ExtensionError(e));
        }
        Ok(conn)
    }

    /// Open a new database file with a specified VFS without an existing database
    /// connection and symbol table to register extensions.
    #[cfg(feature = "fs")]
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn open_new(path: &str, vfs: &str) -> Result<(Arc<dyn IO>, Arc<Database>)> {
        let vfsmods = ext::add_builtin_vfs_extensions(None)?;
        let io: Arc<dyn IO> = match vfsmods.iter().find(|v| v.0 == vfs).map(|v| v.1.clone()) {
            Some(vfs) => vfs,
            None => match vfs.trim() {
                "memory" => Arc::new(MemoryIO::new()),
                "syscall" => Arc::new(SyscallIO::new()?),
                #[cfg(all(target_os = "linux", feature = "io_uring"))]
                "io_uring" => Arc::new(UringIO::new()?),
                other => {
                    return Err(LimboError::InvalidArgument(format!(
                        "no such VFS: {}",
                        other
                    )));
                }
            },
        };
        let db = Self::open_file(io.clone(), path, false)?;
        Ok((io, db))
    }
}

pub fn maybe_init_database_file(file: &Arc<dyn File>, io: &Arc<dyn IO>) -> Result<()> {
    if file.size()? == 0 {
        // init db
        let db_header = DatabaseHeader::default();
        let page1 = allocate_page(
            1,
            &Rc::new(BufferPool::new(db_header.get_page_size() as usize)),
            DATABASE_HEADER_SIZE,
        );
        let page1 = Arc::new(BTreePageInner {
            page: RefCell::new(page1),
        });
        {
            // Create the sqlite_schema table, for this we just need to create the btree page
            // for the first page of the database which is basically like any other btree page
            // but with a 100 byte offset, so we just init the page so that sqlite understands
            // this is a correct page.
            btree_init_page(
                &page1,
                storage::sqlite3_ondisk::PageType::TableLeaf,
                DATABASE_HEADER_SIZE,
                (db_header.get_page_size() - db_header.reserved_space as u32) as u16,
            );

            let page1 = page1.get();
            let contents = page1.get().contents.as_mut().unwrap();
            contents.write_database_header(&db_header);
            // write the first page to disk synchronously
            let flag_complete = Rc::new(RefCell::new(false));
            {
                let flag_complete = flag_complete.clone();
                let completion = Completion::Write(WriteCompletion::new(Box::new(move |_| {
                    *flag_complete.borrow_mut() = true;
                })));
                #[allow(clippy::arc_with_non_send_sync)]
                file.pwrite(0, contents.buffer.clone(), Arc::new(completion))?;
            }
            let mut limit = 100;
            loop {
                io.run_once()?;
                if *flag_complete.borrow() {
                    break;
                }
                limit -= 1;
                if limit == 0 {
                    panic!("Database file couldn't be initialized, io loop run for {} iterations and write didn't finish", limit);
                }
            }
        }
    };
    Ok(())
}

pub struct Connection {
    _db: Arc<Database>,
    pager: Rc<Pager>,
    schema: Arc<RwLock<Schema>>,
    header: Arc<SpinLock<DatabaseHeader>>,
    auto_commit: Cell<bool>,
    mv_transactions: RefCell<Vec<crate::mvcc::database::TxID>>,
    transaction_state: Cell<TransactionState>,
    last_insert_rowid: Cell<i64>,
    last_change: Cell<i64>,
    total_changes: Cell<i64>,
    syms: RefCell<SymbolTable>,
    _shared_cache: bool,
    cache_size: Cell<i32>,
}

impl Connection {
    #[instrument(skip_all, level = Level::TRACE)]
    pub fn prepare(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Statement> {
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
        match cmd {
            Cmd::Stmt(stmt) => {
                let program = Rc::new(translate::translate(
                    self.schema
                        .try_read()
                        .ok_or(LimboError::SchemaLocked)?
                        .deref(),
                    stmt,
                    self.header.clone(),
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

    #[instrument(skip_all, level = Level::TRACE)]
    pub fn query(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<Option<Statement>> {
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

    #[instrument(skip_all, level = Level::TRACE)]
    pub(crate) fn run_cmd(
        self: &Arc<Connection>,
        cmd: Cmd,
        input: &str,
    ) -> Result<Option<Statement>> {
        let syms = self.syms.borrow();
        match cmd {
            Cmd::Stmt(ref stmt) | Cmd::Explain(ref stmt) => {
                let program = translate::translate(
                    self.schema
                        .try_read()
                        .ok_or(LimboError::SchemaLocked)?
                        .deref(),
                    stmt.clone(),
                    self.header.clone(),
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
                            self.schema
                                .try_read()
                                .ok_or(LimboError::SchemaLocked)?
                                .deref(),
                            *select,
                            &syms,
                            &[],
                            &mut table_ref_counter,
                            translate::plan::QueryDestination::ResultRows,
                        )?;
                        optimize_plan(
                            &mut plan,
                            self.schema
                                .try_read()
                                .ok_or(LimboError::SchemaLocked)?
                                .deref(),
                        )?;
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
    #[instrument(skip_all, level = Level::TRACE)]
    pub fn execute(self: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
        let sql = sql.as_ref();
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next()?;
        let syms = self.syms.borrow();
        let byte_offset_end = parser.offset();
        let input = str::from_utf8(&sql.as_bytes()[..byte_offset_end])
            .unwrap()
            .trim();
        if let Some(cmd) = cmd {
            match cmd {
                Cmd::Explain(stmt) => {
                    let program = translate::translate(
                        self.schema
                            .try_read()
                            .ok_or(LimboError::SchemaLocked)?
                            .deref(),
                        stmt,
                        self.header.clone(),
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
                        self.schema
                            .try_read()
                            .ok_or(LimboError::SchemaLocked)?
                            .deref(),
                        stmt,
                        self.header.clone(),
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
                        self._db.io.run_once()?;
                    }
                }
            }
        }
        Ok(())
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
        self.pager.cacheflush()
    }

    pub fn clear_page_cache(&self) -> Result<()> {
        self.pager.clear_page_cache();
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<CheckpointResult> {
        let checkpoint_result = self.pager.wal_checkpoint();
        Ok(checkpoint_result)
    }

    /// Close a connection and checkpoint.
    pub fn close(&self) -> Result<()> {
        self.pager.checkpoint_shutdown()
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
        let rows = self.query("SELECT * FROM sqlite_schema")?;
        let mut schema = self
            .schema
            .try_write()
            .expect("lock on schema should succeed first try");
        {
            let syms = self.syms.borrow();
            if let Err(LimboError::ExtensionError(e)) =
                parse_schema_rows(rows, &mut schema, self.pager.io.clone(), &syms, None)
            {
                // this means that a vtab exists and we no longer have the module loaded. we print
                // a warning to the user to load the module
                eprintln!("Warning: {}", e);
            }
        }
        Ok(())
    }

    // Clearly there is something to improve here, Vec<Vec<Value>> isn't a couple of tea
    /// Query the current rows/values of `pragma_name`.
    pub fn pragma_query(self: &Arc<Connection>, pragma_name: &str) -> Result<Vec<Vec<Value>>> {
        let pragma = format!("PRAGMA {}", pragma_name);
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
        let pragma = format!("PRAGMA {} = {}", pragma_name, pragma_value);
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
        let pragma = format!("PRAGMA {}({})", pragma_name, pragma_value);
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
        self.pager.io.run_once()
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
        .map_or(false, |ext| ext == "so" || ext == "dylib" || ext == "dll")
}

pub fn resolve_ext_path(extpath: &str) -> Result<std::path::PathBuf> {
    let path = std::path::Path::new(extpath);
    if !path.exists() {
        if is_shared_library(path) {
            return Err(LimboError::ExtensionError(format!(
                "Extension file not found: {}",
                extpath
            )));
        };
        let maybe = path.with_extension(std::env::consts::DLL_EXTENSION);
        maybe
            .exists()
            .then_some(maybe)
            .ok_or(LimboError::ExtensionError(format!(
                "Extension file not found: {}",
                extpath
            )))
    } else {
        Ok(path.to_path_buf())
    }
}

impl SymbolTable {
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
