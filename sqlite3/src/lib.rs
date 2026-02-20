#![allow(clippy::missing_safety_doc)]
#![allow(non_camel_case_types)]

use std::ffi::{self, CStr, CString};
use std::num::{NonZero, NonZeroUsize};
use std::sync::{Arc, Mutex, OnceLock};
use tracing::trace;
use turso_core::{CheckpointMode, LimboError, Value};
use turso_ext::ScalarFunction;
use turso_ext::Value as ExtValue;

macro_rules! stub {
    () => {
        todo!("{} is not implemented", stringify!($fn));
    };
}

/* generic error-codes */
pub const SQLITE_OK: ffi::c_int = 0; /* Successful result */
pub const SQLITE_ERROR: ffi::c_int = 1; /* Generic error */
pub const SQLITE_INTERNAL: ffi::c_int = 2; /* Internal logic error in SQLite */
pub const SQLITE_PERM: ffi::c_int = 3; /* Access permission denied */
pub const SQLITE_ABORT: ffi::c_int = 4; /* Callback routine requested an abort */
pub const SQLITE_BUSY: ffi::c_int = 5; /* The database file is locked */
pub const SQLITE_LOCKED: ffi::c_int = 6; /* A table in the database is locked */
pub const SQLITE_NOMEM: ffi::c_int = 7; /* A malloc() failed */
pub const SQLITE_READONLY: ffi::c_int = 8; /* Attempt to write a readonly database */
pub const SQLITE_INTERRUPT: ffi::c_int = 9; /* Operation terminated by sqlite3_interrupt()*/
pub const SQLITE_IOERR: ffi::c_int = 10; /* Some kind of disk I/O error occurred */
pub const SQLITE_CORRUPT: ffi::c_int = 11; /* The database disk image is malformed */
pub const SQLITE_NOTFOUND: ffi::c_int = 12; /* Unknown opcode in sqlite3_file_control() */
pub const SQLITE_FULL: ffi::c_int = 13; /* Insertion failed because database is full */
pub const SQLITE_CANTOPEN: ffi::c_int = 14; /* Unable to open the database file */
pub const SQLITE_PROTOCOL: ffi::c_int = 15; /* Database lock protocol error */
pub const SQLITE_EMPTY: ffi::c_int = 16; /* Internal use only */
pub const SQLITE_SCHEMA: ffi::c_int = 17; /* The database schema changed */
pub const SQLITE_TOOBIG: ffi::c_int = 18; /* String or BLOB exceeds size limit */
pub const SQLITE_CONSTRAINT: ffi::c_int = 19; /* Abort due to constraint violation */
pub const SQLITE_MISMATCH: ffi::c_int = 20; /* Data type mismatch */
pub const SQLITE_MISUSE: ffi::c_int = 21; /* Library used incorrectly */
pub const SQLITE_NOLFS: ffi::c_int = 22; /* Uses OS features not supported on host */
pub const SQLITE_AUTH: ffi::c_int = 23; /* Authorization denied */
pub const SQLITE_FORMAT: ffi::c_int = 24; /* Not used */
pub const SQLITE_RANGE: ffi::c_int = 25; /* 2nd parameter to sqlite3_bind out of range */
pub const SQLITE_NOTADB: ffi::c_int = 26; /* File opened that is not a database file */
pub const SQLITE_NOTICE: ffi::c_int = 27; /* Notifications from sqlite3_log() */
pub const SQLITE_WARNING: ffi::c_int = 28; /* Warnings from sqlite3_log() */
pub const SQLITE_ROW: ffi::c_int = 100; /* sqlite3_step() has another row ready */
pub const SQLITE_DONE: ffi::c_int = 101; /* sqlite3_step() has finished executing */

/* extended error-codes */
pub const SQLITE_ABORT_ROLLBACK: ffi::c_int = SQLITE_ABORT | (2 << 8);

pub const SQLITE_STATE_OPEN: u8 = 0x76;
pub const SQLITE_STATE_SICK: u8 = 0xba;
pub const SQLITE_STATE_BUSY: u8 = 0x6d;

pub const SQLITE_CHECKPOINT_PASSIVE: ffi::c_int = 0;
pub const SQLITE_CHECKPOINT_FULL: ffi::c_int = 1;
pub const SQLITE_CHECKPOINT_RESTART: ffi::c_int = 2;
pub const SQLITE_CHECKPOINT_TRUNCATE: ffi::c_int = 3;

pub const SQLITE_INTEGER: ffi::c_int = 1;
pub const SQLITE_FLOAT: ffi::c_int = 2;
pub const SQLITE_TEXT: ffi::c_int = 3;
pub const SQLITE_BLOB: ffi::c_int = 4;
pub const SQLITE_NULL: ffi::c_int = 5;

pub struct sqlite3 {
    pub(crate) inner: Arc<Mutex<sqlite3Inner>>,
}

struct sqlite3Inner {
    pub(crate) _io: Arc<dyn turso_core::IO>,
    pub(crate) _db: Arc<turso_core::Database>,
    pub(crate) conn: Arc<turso_core::Connection>,
    pub(crate) err_code: ffi::c_int,
    pub(crate) err_mask: ffi::c_int,
    pub(crate) malloc_failed: bool,
    pub(crate) e_open_state: u8,
    pub(crate) p_err: *mut ffi::c_void,
    pub(crate) filename: CString,
    pub(crate) stmt_list: *mut sqlite3_stmt,
}

impl sqlite3 {
    pub fn new(
        io: Arc<dyn turso_core::IO>,
        db: Arc<turso_core::Database>,
        conn: Arc<turso_core::Connection>,
        filename: CString,
    ) -> Self {
        let inner = sqlite3Inner {
            _io: io,
            _db: db,
            conn,
            err_code: SQLITE_OK,
            err_mask: 0xFFFFFFFFu32 as i32,
            malloc_failed: false,
            e_open_state: SQLITE_STATE_OPEN,
            p_err: std::ptr::null_mut(),
            filename,
            stmt_list: std::ptr::null_mut(),
        };
        #[allow(clippy::arc_with_non_send_sync)]
        let inner = Arc::new(Mutex::new(inner));
        Self { inner }
    }
}

pub struct sqlite3_stmt {
    pub(crate) db: *mut sqlite3,
    pub(crate) stmt: turso_core::Statement,
    pub(crate) destructors: Vec<(
        usize,
        Option<unsafe extern "C" fn(*mut ffi::c_void)>,
        *mut ffi::c_void,
    )>,
    pub(crate) next: *mut sqlite3_stmt,
    pub(crate) text_cache: Vec<Vec<u8>>,
}

impl sqlite3_stmt {
    pub fn new(db: *mut sqlite3, stmt: turso_core::Statement) -> Self {
        let n_cols = stmt.num_columns();
        Self {
            db,
            stmt,
            destructors: Vec::new(),
            next: std::ptr::null_mut(),
            text_cache: vec![vec![]; n_cols],
        }
    }
    #[inline]
    fn clear_text_cache(&mut self) {
        // Drop per-column buffers for the previous row
        for r in &mut self.text_cache {
            r.clear();
        }
    }
}

// ===== Custom SQL function registration infrastructure =====

/// Context passed to scalar function callbacks registered via sqlite3_create_function_v2.
/// Exposed as an opaque `void*` to C callers.
pub struct SqliteContext {
    pub(crate) result: ExtValue,
    pub(crate) p_app: *mut ffi::c_void,
}

// SAFETY: SqliteContext is only used single-threaded within a function call.
unsafe impl Send for SqliteContext {}

struct FuncSlot {
    x_func: unsafe extern "C" fn(*mut ffi::c_void, ffi::c_int, *mut *mut ffi::c_void),
    p_app: usize,   // *mut c_void stored as usize for Send
    destroy: usize, // Option<unsafe extern "C" fn(*mut c_void)> stored as usize for Send
    name: String,
}

// SAFETY: p_app lifetime is the caller's responsibility (same as SQLite C API).
unsafe impl Send for FuncSlot {}

const MAX_CUSTOM_FUNCS: usize = 32;

static FUNC_SLOTS: OnceLock<Mutex<[Option<FuncSlot>; MAX_CUSTOM_FUNCS]>> = OnceLock::new();

fn func_slots() -> &'static Mutex<[Option<FuncSlot>; MAX_CUSTOM_FUNCS]> {
    FUNC_SLOTS.get_or_init(|| Mutex::new(std::array::from_fn(|_| None)))
}

unsafe fn dispatch_func_bridge(slot_id: usize, argc: i32, argv: *const ExtValue) -> ExtValue {
    let (x_func, p_app) = {
        let slots = func_slots().lock().unwrap();
        match slots[slot_id].as_ref() {
            Some(s) => (s.x_func, s.p_app),
            None => return ExtValue::null(),
        }
    };

    // Build array of *mut c_void each pointing into the argv slice.
    // The C callback reads these via sqlite3_value_* functions.
    let mut arg_ptrs: Vec<*mut ffi::c_void> = (0..argc as usize)
        .map(|i| argv.add(i) as *mut ffi::c_void)
        .collect();

    let mut ctx = SqliteContext {
        result: ExtValue::null(),
        p_app: p_app as *mut ffi::c_void,
    };

    x_func(
        &mut ctx as *mut SqliteContext as *mut ffi::c_void,
        argc,
        arg_ptrs.as_mut_ptr(),
    );

    ctx.result
}

// 32 pre-generated bridge functions — one per slot.
// Each bridges turso_core's ScalarFunction ABI to the C sqlite3_create_function_v2 callback.
macro_rules! func_bridge {
    ($id:literal, $name:ident) => {
        unsafe extern "C" fn $name(argc: i32, argv: *const ExtValue) -> ExtValue {
            dispatch_func_bridge($id, argc, argv)
        }
    };
}

func_bridge!(0, func_bridge_0);
func_bridge!(1, func_bridge_1);
func_bridge!(2, func_bridge_2);
func_bridge!(3, func_bridge_3);
func_bridge!(4, func_bridge_4);
func_bridge!(5, func_bridge_5);
func_bridge!(6, func_bridge_6);
func_bridge!(7, func_bridge_7);
func_bridge!(8, func_bridge_8);
func_bridge!(9, func_bridge_9);
func_bridge!(10, func_bridge_10);
func_bridge!(11, func_bridge_11);
func_bridge!(12, func_bridge_12);
func_bridge!(13, func_bridge_13);
func_bridge!(14, func_bridge_14);
func_bridge!(15, func_bridge_15);
func_bridge!(16, func_bridge_16);
func_bridge!(17, func_bridge_17);
func_bridge!(18, func_bridge_18);
func_bridge!(19, func_bridge_19);
func_bridge!(20, func_bridge_20);
func_bridge!(21, func_bridge_21);
func_bridge!(22, func_bridge_22);
func_bridge!(23, func_bridge_23);
func_bridge!(24, func_bridge_24);
func_bridge!(25, func_bridge_25);
func_bridge!(26, func_bridge_26);
func_bridge!(27, func_bridge_27);
func_bridge!(28, func_bridge_28);
func_bridge!(29, func_bridge_29);
func_bridge!(30, func_bridge_30);
func_bridge!(31, func_bridge_31);

static FUNC_BRIDGES: [ScalarFunction; MAX_CUSTOM_FUNCS] = [
    func_bridge_0,
    func_bridge_1,
    func_bridge_2,
    func_bridge_3,
    func_bridge_4,
    func_bridge_5,
    func_bridge_6,
    func_bridge_7,
    func_bridge_8,
    func_bridge_9,
    func_bridge_10,
    func_bridge_11,
    func_bridge_12,
    func_bridge_13,
    func_bridge_14,
    func_bridge_15,
    func_bridge_16,
    func_bridge_17,
    func_bridge_18,
    func_bridge_19,
    func_bridge_20,
    func_bridge_21,
    func_bridge_22,
    func_bridge_23,
    func_bridge_24,
    func_bridge_25,
    func_bridge_26,
    func_bridge_27,
    func_bridge_28,
    func_bridge_29,
    func_bridge_30,
    func_bridge_31,
];

// ===== End custom function infrastructure =====

static INIT_DONE: std::sync::Once = std::sync::Once::new();

#[no_mangle]
pub unsafe extern "C" fn sqlite3_initialize() -> ffi::c_int {
    INIT_DONE.call_once(|| {
        tracing_subscriber::fmt::init();
    });
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_shutdown() -> ffi::c_int {
    SQLITE_OK
}

#[no_mangle]
#[allow(clippy::arc_with_non_send_sync)]
pub unsafe extern "C" fn sqlite3_open(
    filename: *const ffi::c_char,
    db_out: *mut *mut sqlite3,
) -> ffi::c_int {
    trace!("sqlite3_open");
    let rc = sqlite3_initialize();
    if rc != SQLITE_OK {
        return rc;
    }
    if filename.is_null() {
        return SQLITE_MISUSE;
    }
    if db_out.is_null() {
        return SQLITE_MISUSE;
    }
    let filename_cstr = CStr::from_ptr(filename);
    let filename_str = match filename_cstr.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    let io: Arc<dyn turso_core::IO> = match filename_str {
        ":memory:" => Arc::new(turso_core::MemoryIO::new()),
        _ => match turso_core::PlatformIO::new() {
            Ok(io) => Arc::new(io),
            Err(_) => return SQLITE_CANTOPEN,
        },
    };
    match turso_core::Database::open_file(io.clone(), filename_str) {
        Ok(db) => {
            let conn = db.connect().unwrap();
            let filename = match filename_str {
                ":memory:" => CString::new("".to_string()).unwrap(),
                _ => CString::from(filename_cstr),
            };
            *db_out = Box::leak(Box::new(sqlite3::new(io, db, conn, filename)));
            SQLITE_OK
        }
        Err(e) => {
            trace!("error opening database {}: {:?}", filename_str, e);
            SQLITE_CANTOPEN
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_open_v2(
    filename: *const ffi::c_char,
    db_out: *mut *mut sqlite3,
    _flags: ffi::c_int,
    _z_vfs: *const ffi::c_char,
) -> ffi::c_int {
    trace!("sqlite3_open_v2");
    sqlite3_open(filename, db_out)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_close(db: *mut sqlite3) -> ffi::c_int {
    trace!("sqlite3_close");
    if db.is_null() {
        return SQLITE_OK;
    }
    let _ = Box::from_raw(db);
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_close_v2(db: *mut sqlite3) -> ffi::c_int {
    trace!("sqlite3_close_v2");
    sqlite3_close(db)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_db_filename(
    db: *mut sqlite3,
    db_name: *const ffi::c_char,
) -> *const ffi::c_char {
    if db.is_null() {
        return std::ptr::null();
    }
    if !db_name.is_null() {
        let name = CStr::from_ptr(db_name);
        if name.to_bytes() != b"main" {
            return std::ptr::null();
        }
    }
    let db = &*db;
    let inner = db.inner.lock().unwrap();
    inner.filename.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_trace_v2(
    _db: *mut sqlite3,
    _mask: ffi::c_uint,
    _callback: Option<
        unsafe extern "C" fn(ffi::c_uint, *mut ffi::c_void, *mut ffi::c_void, *mut ffi::c_void),
    >,
    _context: *mut ffi::c_void,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_progress_handler(
    _db: *mut sqlite3,
    _n: ffi::c_int,
    _callback: Option<unsafe extern "C" fn() -> ffi::c_int>,
    _context: *mut ffi::c_void,
) -> ffi::c_int {
    stub!();
}

/// Type for C busy handler callback function.
type BusyHandlerFn = unsafe extern "C" fn(*mut ffi::c_void, ffi::c_int) -> ffi::c_int;

/// Register a callback to handle SQLITE_BUSY errors.
///
/// The sqlite3_busy_handler(D,X,P) routine sets a callback function X that might be invoked
/// with argument P whenever an attempt is made to access a database table associated with
/// database connection D when another thread or process has the table locked.
///
/// If the busy callback is NULL, then SQLITE_BUSY is returned immediately upon encountering
/// the lock. If the busy callback is not NULL, then the callback might be invoked with two
/// arguments: the context pointer P and the number of times the busy handler has been invoked
/// previously for the same locking event.
///
/// If the busy callback returns 0, then no additional attempts are made to access the database
/// and SQLITE_BUSY is returned to the application. If the callback returns non-zero, then
/// another attempt is made to access the database and the cycle repeats.
///
/// There can only be a single busy handler defined for each database connection. Setting a new
/// busy handler clears any previously set handler. Note that calling sqlite3_busy_timeout()
/// will change the busy handler and thus clear any previously set busy handler.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_busy_handler(
    db: *mut sqlite3,
    callback: Option<BusyHandlerFn>,
    context: *mut ffi::c_void,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }

    let db_ref = &*db;
    let inner = match db_ref.inner.lock() {
        Ok(guard) => guard,
        Err(_) => return SQLITE_MISUSE,
    };

    match callback {
        None => {
            // Clear the busy handler
            inner.conn.set_busy_handler(None);
        }
        Some(c_callback) => {
            // We need a Rust wrapper for the C callback
            // The context pointer is captured by value and must remain valid for the
            // lifetime of the handler (caller's responsibility per SQLite spec)
            let ctx = context as usize; // Convert to usize for Send+Sync
            let cb = c_callback;
            inner
                .conn
                .set_busy_handler(Some(Box::new(move |count: i32| {
                    // SAFETY: Caller guarantees context validity for the handler's lifetime
                    unsafe { cb(ctx as *mut ffi::c_void, count as ffi::c_int) }
                })));
        }
    }

    SQLITE_OK
}

/// Set a busy timeout for the database connection.
///
/// This routine sets a busy handler that sleeps for a specified amount of time when a table
/// is locked. The handler will sleep multiple times until at least "ms" milliseconds of
/// sleeping have accumulated. After at least "ms" milliseconds of sleeping, the handler
/// returns 0 which causes sqlite3_step() to return SQLITE_BUSY.
///
/// Calling this routine with an argument less than or equal to zero turns off all busy
/// handlers and returns SQLITE_BUSY immediately upon encountering a lock.
///
/// There can only be a single busy handler for a database connection. Setting a busy timeout
/// clears any previously set busy handler.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_busy_timeout(db: *mut sqlite3, ms: ffi::c_int) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }

    let db_ref = &*db;
    let inner = match db_ref.inner.lock() {
        Ok(guard) => guard,
        Err(_) => return SQLITE_MISUSE,
    };

    let duration = if ms <= 0 {
        std::time::Duration::ZERO
    } else {
        std::time::Duration::from_millis(ms as u64)
    };

    inner.conn.set_busy_timeout(duration);
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_set_authorizer(
    _db: *mut sqlite3,
    _callback: Option<unsafe extern "C" fn() -> ffi::c_int>,
    _context: *mut ffi::c_void,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_context_db_handle(_context: *mut ffi::c_void) -> *mut ffi::c_void {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_prepare_v2(
    raw_db: *mut sqlite3,
    sql: *const ffi::c_char,
    _len: ffi::c_int,
    out_stmt: *mut *mut sqlite3_stmt,
    _tail: *mut *const ffi::c_char,
) -> ffi::c_int {
    if raw_db.is_null() || sql.is_null() || out_stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &mut sqlite3 = &mut *raw_db;
    let mut db = db.inner.lock().unwrap();
    let sql = CStr::from_ptr(sql);
    let sql = match sql.to_str() {
        Ok(s) => s,
        Err(_) => {
            db.err_code = SQLITE_MISUSE;
            return SQLITE_MISUSE;
        }
    };
    let stmt = match db.conn.prepare(sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            let code = handle_limbo_err(err, std::ptr::null_mut());
            db.err_code = code;
            return code;
        }
    };
    let new_stmt = Box::leak(Box::new(sqlite3_stmt::new(raw_db, stmt)));

    new_stmt.next = db.stmt_list;
    db.stmt_list = new_stmt;

    *out_stmt = new_stmt;
    SQLITE_OK
}

unsafe fn stmt_run_to_completion(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt_ref = &mut *stmt;
    while stmt_ref.stmt.execution_state().is_running() {
        let result = sqlite3_step(stmt);
        if result != SQLITE_DONE && result != SQLITE_ROW {
            return result;
        }
    }
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt_ref = &mut *stmt;

    // first, finalize any execution if it was unfinished
    // (for example, many drivers can consume just one row and finalize statement after that, while there still can be work to do)
    // (this is necessary because queries like INSERT INTO t VALUES (1), (2), (3) RETURNING id return values within a transaction)
    let result = stmt_run_to_completion(stmt);
    if result != SQLITE_OK {
        return result;
    }

    if !stmt_ref.db.is_null() {
        let db = &mut *stmt_ref.db;
        let mut db_inner = db.inner.lock().unwrap();

        if db_inner.stmt_list == stmt {
            db_inner.stmt_list = stmt_ref.next;
        } else {
            let mut current = db_inner.stmt_list;
            while !current.is_null() {
                let current_ref = &mut *current;
                if current_ref.next == stmt {
                    current_ref.next = stmt_ref.next;
                    break;
                }
                current = current_ref.next;
            }
        }
    }

    for (_idx, destructor_opt, ptr) in stmt_ref.destructors.drain(..) {
        if let Some(destructor_fn) = destructor_opt {
            destructor_fn(ptr);
        }
    }
    stmt_ref.clear_text_cache();
    let _ = Box::from_raw(stmt);
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_step(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt = &mut *stmt;
    let db = &mut *stmt.db;
    let _db = db.inner.lock().unwrap();
    let res = stmt.stmt.run_one_step_blocking(|| Ok(()), || Ok(()));
    match res {
        Ok(Some(_)) => {
            stmt.clear_text_cache();
            SQLITE_ROW
        }
        Ok(None) => {
            stmt.clear_text_cache();
            SQLITE_DONE
        }
        Err(LimboError::Busy) => SQLITE_BUSY,
        Err(LimboError::Interrupt) => SQLITE_INTERRUPT,
        Err(err) => handle_limbo_err(err, std::ptr::null_mut()),
    }
}

type exec_callback = Option<
    unsafe extern "C" fn(
        context: *mut ffi::c_void,
        n_column: ffi::c_int,
        argv: *mut *mut ffi::c_char,
        colv: *mut *mut ffi::c_char,
    ) -> ffi::c_int,
>;

#[no_mangle]
pub unsafe extern "C" fn sqlite3_exec(
    db: *mut sqlite3,
    sql: *const ffi::c_char,
    callback: exec_callback,
    context: *mut ffi::c_void,
    err: *mut *mut ffi::c_char,
) -> ffi::c_int {
    if db.is_null() || sql.is_null() {
        return SQLITE_MISUSE;
    }

    let db_ref: &mut sqlite3 = &mut *db;
    let sql_cstr = CStr::from_ptr(sql);
    let sql_str = match sql_cstr.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    trace!("sqlite3_exec(sql={})", sql_str);
    if !err.is_null() {
        *err = std::ptr::null_mut();
    }
    let statements = split_sql_statements(sql_str);
    for stmt_sql in statements {
        let trimmed = stmt_sql.trim();
        if trimmed.is_empty() {
            continue;
        }

        let is_dql = is_query_statement(trimmed);
        if !is_dql {
            // For DML/DDL, use normal execute path
            let db_inner = db_ref.inner.lock().unwrap();
            match db_inner.conn.execute(trimmed) {
                Ok(_) => continue,
                Err(e) => {
                    return handle_limbo_err(e, err);
                }
            }
        } else if callback.is_none() {
            // DQL without callback provided, still execute but discard any result rows
            let mut stmt_ptr: *mut sqlite3_stmt = std::ptr::null_mut();
            let rc = sqlite3_prepare_v2(
                db,
                CString::new(trimmed).unwrap().as_ptr(),
                -1,
                &mut stmt_ptr,
                std::ptr::null_mut(),
            );
            if rc != SQLITE_OK {
                if !err.is_null() {
                    let err_msg = format!("Prepare failed: {rc}");
                    *err = CString::new(err_msg).unwrap().into_raw();
                }
                return rc;
            }
            loop {
                let step_rc = sqlite3_step(stmt_ptr);
                match step_rc {
                    SQLITE_ROW => continue,
                    SQLITE_DONE => break,
                    _ => {
                        sqlite3_finalize(stmt_ptr);
                        if !err.is_null() {
                            let err_msg = format!("Step failed: {step_rc}");
                            *err = CString::new(err_msg).unwrap().into_raw();
                        }
                        return step_rc;
                    }
                }
            }
            sqlite3_finalize(stmt_ptr);
        } else {
            // DQL with callback
            let rc = execute_query_with_callback(db, trimmed, callback, context, err);
            if rc != SQLITE_OK {
                return rc;
            }
        }
    }
    SQLITE_OK
}

/// Detect if a SQL statement is DQL
fn is_query_statement(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    if trimmed.is_empty() {
        return false;
    }
    let bytes = trimmed.as_bytes();

    let starts_with_ignore_case = |keyword: &[u8]| -> bool {
        if bytes.len() < keyword.len() {
            return false;
        }
        // Check keyword matches
        if !bytes[..keyword.len()].eq_ignore_ascii_case(keyword) {
            return false;
        }
        // Ensure keyword is followed by whitespace or EOF
        bytes.len() == keyword.len() || bytes[keyword.len()].is_ascii_whitespace()
    };

    // Check DQL keywords
    if starts_with_ignore_case(b"SELECT")
        || starts_with_ignore_case(b"VALUES")
        || starts_with_ignore_case(b"WITH")
        || starts_with_ignore_case(b"PRAGMA")
        || starts_with_ignore_case(b"EXPLAIN")
    {
        return true;
    }

    // Look for RETURNING as a whole word, that's not part of another identifier
    let mut i = 0;
    while i < bytes.len() {
        if i + 9 <= bytes.len() && bytes[i..i + 9].eq_ignore_ascii_case(b"RETURNING") {
            // Check it's a word boundary before and after
            let is_word_start =
                i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
            let is_word_end = i + 9 == bytes.len()
                || !bytes[i + 9].is_ascii_alphanumeric() && bytes[i + 9] != b'_';
            if is_word_start && is_word_end {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Execute a query statement with callback for each row
/// Only called when we know callback is Some
unsafe fn execute_query_with_callback(
    db: *mut sqlite3,
    sql: &str,
    callback: exec_callback,
    context: *mut ffi::c_void,
    err: *mut *mut ffi::c_char,
) -> ffi::c_int {
    let sql_cstring = match CString::new(sql) {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };

    let mut stmt_ptr: *mut sqlite3_stmt = std::ptr::null_mut();
    let rc = sqlite3_prepare_v2(
        db,
        sql_cstring.as_ptr(),
        -1,
        &mut stmt_ptr,
        std::ptr::null_mut(),
    );

    if rc != SQLITE_OK {
        if !err.is_null() {
            let err_msg = format!("Prepare failed: {rc}");
            *err = CString::new(err_msg).unwrap().into_raw();
        }
        return rc;
    }

    let stmt_ref = &*stmt_ptr;
    let n_cols = stmt_ref.stmt.num_columns() as ffi::c_int;
    let mut column_names: Vec<CString> = Vec::with_capacity(n_cols as usize);

    for i in 0..n_cols {
        let name = stmt_ref.stmt.get_column_name(i as usize);
        column_names.push(CString::new(name.as_bytes()).unwrap());
    }

    loop {
        let step_rc = sqlite3_step(stmt_ptr);

        match step_rc {
            SQLITE_ROW => {
                // Safety: checked earlier
                let callback = callback.unwrap();

                let mut values: Vec<CString> = Vec::with_capacity(n_cols as usize);
                let mut value_ptrs: Vec<*mut ffi::c_char> = Vec::with_capacity(n_cols as usize);
                let mut col_ptrs: Vec<*mut ffi::c_char> = Vec::with_capacity(n_cols as usize);

                for i in 0..n_cols {
                    let val = stmt_ref.stmt.row().unwrap().get_value(i as usize);
                    values.push(CString::new(val.to_string().as_bytes()).unwrap());
                }

                for value in &values {
                    value_ptrs.push(value.as_ptr() as *mut ffi::c_char);
                }
                for name in &column_names {
                    col_ptrs.push(name.as_ptr() as *mut ffi::c_char);
                }

                let cb_rc = callback(
                    context,
                    n_cols,
                    value_ptrs.as_mut_ptr(),
                    col_ptrs.as_mut_ptr(),
                );

                if cb_rc != 0 {
                    sqlite3_finalize(stmt_ptr);
                    return SQLITE_ABORT;
                }
            }
            SQLITE_DONE => {
                break;
            }
            _ => {
                sqlite3_finalize(stmt_ptr);
                if !err.is_null() {
                    let err_msg = format!("Step failed: {step_rc}");
                    *err = CString::new(err_msg).unwrap().into_raw();
                }
                return step_rc;
            }
        }
    }

    sqlite3_finalize(stmt_ptr)
}

/// Split SQL string into individual statements
/// Handles quoted strings properly and skips comments
fn split_sql_statements(sql: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut current_start = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let bytes = sql.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            // Check for escaped quotes first
            b'\'' if !in_double_quote => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single_quote = !in_single_quote;
            }
            b'"' if !in_single_quote => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_double_quote = !in_double_quote;
            }
            b';' if !in_single_quote && !in_double_quote => {
                // we found the statement boundary
                statements.push(&sql[current_start..i]);
                current_start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }

    if current_start < sql.len() {
        statements.push(&sql[current_start..]);
    }

    statements
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_reset(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt = &mut *stmt;
    // first, finalize any execution if it was unfinished
    // (for example, many drivers can consume just one row and finalize statement after that, while there still can be work to do)
    // (this is necessary because queries like INSERT INTO t VALUES (1), (2), (3) RETURNING id return values within a transaction)
    let result = stmt_run_to_completion(stmt);
    if result != SQLITE_OK {
        return result;
    }
    stmt.stmt.reset();
    stmt.clear_text_cache();
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_changes64(db: *mut sqlite3) -> i64 {
    let db: &mut sqlite3 = &mut *db;
    let inner = db.inner.lock().unwrap();
    inner.conn.changes()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_changes(db: *mut sqlite3) -> ffi::c_int {
    sqlite3_changes64(db) as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_stmt_readonly(_stmt: *mut sqlite3_stmt) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_stmt_busy(_stmt: *mut sqlite3_stmt) -> ffi::c_int {
    stub!();
}

/// Iterate over all prepared statements in the database.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_next_stmt(
    db: *mut sqlite3,
    stmt: *mut sqlite3_stmt,
) -> *mut sqlite3_stmt {
    if db.is_null() {
        return std::ptr::null_mut();
    }
    if stmt.is_null() {
        let db = &*db;
        let db = db.inner.lock().unwrap();
        db.stmt_list
    } else {
        let stmt = &mut *stmt;
        stmt.next
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_serialize(
    _db: *mut sqlite3,
    _schema: *const ffi::c_char,
    _out: *mut *mut ffi::c_void,
    _out_bytes: *mut ffi::c_int,
    _flags: ffi::c_uint,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_deserialize(
    _db: *mut sqlite3,
    _schema: *const ffi::c_char,
    _in_: *const ffi::c_void,
    _in_bytes: ffi::c_int,
    _flags: ffi::c_uint,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_get_autocommit(db: *mut sqlite3) -> ffi::c_int {
    if db.is_null() {
        return 1;
    }
    let db: &mut sqlite3 = &mut *db;
    let inner = db.inner.lock().unwrap();
    if inner.conn.get_auto_commit() {
        1
    } else {
        0
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_total_changes(db: *mut sqlite3) -> ffi::c_int {
    let db: &mut sqlite3 = &mut *db;
    let inner = db.inner.lock().unwrap();
    inner.conn.total_changes() as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_last_insert_rowid(db: *mut sqlite3) -> ffi::c_int {
    let db: &mut sqlite3 = &mut *db;
    let inner = db.inner.lock().unwrap();
    inner.conn.last_insert_rowid() as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_interrupt(_db: *mut sqlite3) {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_db_config(_db: *mut sqlite3, _op: ffi::c_int) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_db_handle(_stmt: *mut sqlite3_stmt) -> *mut sqlite3 {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_sleep(_ms: ffi::c_int) {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_limit(
    _db: *mut sqlite3,
    _id: ffi::c_int,
    _new_value: ffi::c_int,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_malloc(n: ffi::c_int) -> *mut ffi::c_void {
    sqlite3_malloc64(n)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_malloc64(n: ffi::c_int) -> *mut ffi::c_void {
    if n <= 0 {
        return std::ptr::null_mut();
    }
    libc::malloc(n as usize)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_free(ptr: *mut ffi::c_void) {
    if ptr.is_null() {
        return;
    }
    libc::free(ptr);
}

/// Returns the error code for the most recent failed API call to connection.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_errcode(db: *mut sqlite3) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &mut sqlite3 = &mut *db;
    let db = db.inner.lock().unwrap();
    if !sqlite3_safety_check_sick_or_ok(&db) {
        return SQLITE_MISUSE;
    }
    if db.malloc_failed {
        return SQLITE_NOMEM;
    }
    db.err_code & db.err_mask
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_errstr(_err: ffi::c_int) -> *const ffi::c_char {
    sqlite3_errstr_impl(_err)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_user_data(context: *mut ffi::c_void) -> *mut ffi::c_void {
    if context.is_null() {
        return std::ptr::null_mut();
    }
    let ctx = &*(context as *const SqliteContext);
    ctx.p_app
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_init(
    _dest_db: *mut sqlite3,
    _dest_name: *const ffi::c_char,
    _source_db: *mut sqlite3,
    _source_name: *const ffi::c_char,
) -> *mut ffi::c_void {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_step(
    _backup: *mut ffi::c_void,
    _n_pages: ffi::c_int,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_remaining(_backup: *mut ffi::c_void) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_pagecount(_backup: *mut ffi::c_void) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_finish(_backup: *mut ffi::c_void) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_expanded_sql(_stmt: *mut sqlite3_stmt) -> *mut ffi::c_char {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_data_count(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt = &*stmt;
    let row = stmt.stmt.row().unwrap();
    row.len() as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_parameter_count(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt = &*stmt;
    stmt.stmt.parameters_count() as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_parameter_name(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *const ffi::c_char {
    let stmt = &*stmt;
    let index = NonZero::new_unchecked(idx as usize);

    if let Some(val) = stmt.stmt.parameters().name(index) {
        let c_string = CString::new(val).expect("CString::new failed");
        c_string.into_raw()
    } else {
        std::ptr::null()
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_parameter_index(
    stmt: *mut sqlite3_stmt,
    name: *const ffi::c_char,
) -> ffi::c_int {
    if stmt.is_null() || name.is_null() {
        return 0;
    }

    let stmt = &*stmt;
    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return 0,
    };

    if let Some(index) = stmt.stmt.parameter_index(name_str) {
        index.get() as ffi::c_int
    } else {
        0
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_null(stmt: *mut sqlite3_stmt, idx: ffi::c_int) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }

    if idx <= 0 {
        return SQLITE_RANGE;
    }
    let stmt = &mut *stmt;

    stmt.stmt
        .bind_at(NonZero::new_unchecked(idx as usize), Value::Null);
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_int(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
    val: i64,
) -> ffi::c_int {
    sqlite3_bind_int64(stmt, idx, val)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_int64(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
    val: i64,
) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    if idx <= 0 {
        return SQLITE_RANGE;
    }
    let stmt = &mut *stmt;

    stmt.stmt
        .bind_at(NonZero::new_unchecked(idx as usize), Value::from_i64(val));

    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_double(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
    val: f64,
) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    if idx <= 0 {
        return SQLITE_RANGE;
    }
    let stmt = &mut *stmt;

    stmt.stmt
        .bind_at(NonZero::new_unchecked(idx as usize), Value::from_f64(val));

    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_text(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
    text: *const ffi::c_char,
    len: ffi::c_int,
    destructor: Option<unsafe extern "C" fn(*mut ffi::c_void)>,
) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    if idx <= 0 {
        return SQLITE_RANGE;
    }
    if text.is_null() {
        return sqlite3_bind_null(stmt, idx);
    }

    let stmt_ref = &mut *stmt;

    let static_ptr = std::ptr::null();
    let transient_ptr = -1isize as usize as *const ffi::c_void;
    let ptr_val = destructor
        .map(|f| f as *const ffi::c_void)
        .unwrap_or(static_ptr);

    let str_value = if len < 0 {
        match CStr::from_ptr(text).to_str() {
            Ok(s) => s.to_owned(),
            Err(_) => return SQLITE_ERROR,
        }
    } else {
        let slice = std::slice::from_raw_parts(text as *const u8, len as usize);
        match std::str::from_utf8(slice) {
            Ok(s) => s.to_owned(),
            Err(_) => return SQLITE_ERROR,
        }
    };

    if ptr_val == transient_ptr {
        let val = Value::from_text(str_value);
        stmt_ref
            .stmt
            .bind_at(NonZero::new_unchecked(idx as usize), val);
    } else if ptr_val == static_ptr {
        let slice = std::slice::from_raw_parts(text as *const u8, str_value.len());
        let val = Value::from_text(std::str::from_utf8(slice).unwrap());
        stmt_ref
            .stmt
            .bind_at(NonZero::new_unchecked(idx as usize), val);
    } else {
        let slice = std::slice::from_raw_parts(text as *const u8, str_value.len());
        let val = Value::from_text(std::str::from_utf8(slice).unwrap());
        stmt_ref
            .stmt
            .bind_at(NonZero::new_unchecked(idx as usize), val);

        stmt_ref
            .destructors
            .push((idx as usize, destructor, text as *mut ffi::c_void));
    }

    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_blob(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
    blob: *const ffi::c_void,
    len: ffi::c_int,
    destructor: Option<unsafe extern "C" fn(*mut ffi::c_void)>,
) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    if idx <= 0 {
        return SQLITE_RANGE;
    }
    if blob.is_null() {
        return sqlite3_bind_null(stmt, idx);
    }

    let slice_blob = std::slice::from_raw_parts(blob as *const u8, len as usize).to_vec();

    let stmt_ref = &mut *stmt;
    let val_blob = Value::from_blob(slice_blob);

    if let Some(nz_idx) = NonZeroUsize::new(idx as usize) {
        stmt_ref.stmt.bind_at(nz_idx, val_blob);
    } else {
        return SQLITE_RANGE;
    }

    if let Some(destructor_fn) = destructor {
        let ptr_val = destructor_fn as *const ffi::c_void;
        let static_ptr = std::ptr::null();
        let transient_ptr = usize::MAX as *const ffi::c_void;

        if ptr_val != static_ptr && ptr_val != transient_ptr {
            destructor_fn(blob as *mut _);
        }
    }

    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_clear_bindings(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }

    let stmt_ref = &mut *stmt;
    stmt_ref.stmt.clear_bindings();

    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_type(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> ffi::c_int {
    let stmt = &mut *stmt;
    let row = stmt
        .stmt
        .row()
        .expect("Function should only be called after `SQLITE_ROW`");

    match row.get::<&Value>(idx as usize) {
        Ok(turso_core::Value::Numeric(turso_core::Numeric::Integer(_))) => SQLITE_INTEGER,
        Ok(turso_core::Value::Text(_)) => SQLITE_TEXT,
        Ok(turso_core::Value::Numeric(turso_core::Numeric::Float(_))) => SQLITE_FLOAT,
        Ok(turso_core::Value::Blob(_)) => SQLITE_BLOB,
        _ => SQLITE_NULL,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_count(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt = &mut *stmt;
    stmt.stmt.num_columns() as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_decltype(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *const ffi::c_char {
    let stmt = &mut *stmt;

    if let Some(val) = stmt.stmt.get_column_decltype(idx as usize) {
        let c_string = CString::new(val).expect("CString::new failed");
        c_string.into_raw()
    } else {
        std::ptr::null()
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_name(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *const ffi::c_char {
    let idx = idx.try_into().unwrap();
    let stmt = &mut *stmt;

    let binding = stmt.stmt.get_column_name(idx).into_owned();
    let val = binding.as_str();

    if val.is_empty() {
        return std::ptr::null();
    }

    let c_string = CString::new(val).expect("CString::new failed");
    c_string.into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_table_name(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *const ffi::c_char {
    let idx = idx.try_into().unwrap();
    let stmt = &mut *stmt;

    let binding = stmt
        .stmt
        .get_column_table_name(idx)
        .map(|cow| cow.into_owned())
        .unwrap_or_default();
    let val = binding.as_str();

    if val.is_empty() {
        return std::ptr::null();
    }

    let c_string = CString::new(val).expect("CString::new failed");
    c_string.into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_int64(stmt: *mut sqlite3_stmt, idx: ffi::c_int) -> i64 {
    // Attempt to convert idx to usize
    let idx = idx.try_into().unwrap();
    let stmt = &mut *stmt;
    let row = stmt
        .stmt
        .row()
        .expect("Function should only be called after `SQLITE_ROW`");
    row.get(idx).unwrap()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_int(stmt: *mut sqlite3_stmt, idx: ffi::c_int) -> i64 {
    sqlite3_column_int64(stmt, idx)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_double(stmt: *mut sqlite3_stmt, idx: ffi::c_int) -> f64 {
    let idx = idx.try_into().unwrap();
    let stmt = &mut *stmt;
    let row = stmt
        .stmt
        .row()
        .expect("Function should only be called after `SQLITE_ROW`");
    row.get(idx).unwrap()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_blob(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *const ffi::c_void {
    let stmt = &mut *stmt;
    let row = stmt.stmt.row();
    let row = match row.as_ref() {
        Some(row) => row,
        None => return std::ptr::null(),
    };
    match row.get::<&Value>(idx as usize) {
        Ok(turso_core::Value::Blob(blob)) => blob.as_ptr() as *const ffi::c_void,
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_bytes(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> ffi::c_int {
    let stmt = &mut *stmt;
    let row = stmt.stmt.row();
    let row = match row.as_ref() {
        Some(row) => row,
        None => return 0,
    };
    match row.get::<&Value>(idx as usize) {
        Ok(turso_core::Value::Text(text)) => text.as_str().len() as ffi::c_int,
        Ok(turso_core::Value::Blob(blob)) => blob.len() as ffi::c_int,
        _ => 0,
    }
}

// sqlite3_value_* functions interpret the void* as a pointer to turso_ext::Value,
// which is what the function bridge passes for custom scalar function arguments.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_type(value: *mut ffi::c_void) -> ffi::c_int {
    if value.is_null() {
        return SQLITE_NULL;
    }
    let v = &*(value as *const ExtValue);
    match v.value_type() {
        turso_ext::ValueType::Null => SQLITE_NULL,
        turso_ext::ValueType::Integer => SQLITE_INTEGER,
        turso_ext::ValueType::Float => SQLITE_FLOAT,
        turso_ext::ValueType::Text => SQLITE_TEXT,
        turso_ext::ValueType::Blob => SQLITE_BLOB,
        turso_ext::ValueType::Error => SQLITE_NULL,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_int64(value: *mut ffi::c_void) -> i64 {
    if value.is_null() {
        return 0;
    }
    let v = &*(value as *const ExtValue);
    v.to_integer().unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_double(value: *mut ffi::c_void) -> f64 {
    if value.is_null() {
        return 0.0;
    }
    let v = &*(value as *const ExtValue);
    v.to_float().unwrap_or(0.0)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_text(value: *mut ffi::c_void) -> *const ffi::c_uchar {
    if value.is_null() {
        return std::ptr::null();
    }
    let v = &*(value as *const ExtValue);
    match v.to_text() {
        Some(s) => s.as_ptr() as *const ffi::c_uchar,
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_blob(value: *mut ffi::c_void) -> *const ffi::c_void {
    if value.is_null() {
        return std::ptr::null();
    }
    let v = &*(value as *const ExtValue);
    match v.to_blob() {
        Some(b) => b.as_ptr() as *const ffi::c_void,
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_bytes(value: *mut ffi::c_void) -> ffi::c_int {
    if value.is_null() {
        return 0;
    }
    let v = &*(value as *const ExtValue);
    match v.value_type() {
        turso_ext::ValueType::Text => v.to_text().map(|s| s.len()).unwrap_or(0) as ffi::c_int,
        turso_ext::ValueType::Blob => v.to_blob().map(|b| b.len()).unwrap_or(0) as ffi::c_int,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_text(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *const ffi::c_uchar {
    if stmt.is_null() || idx < 0 {
        return std::ptr::null();
    }
    let stmt = &mut *stmt;
    let row = stmt.stmt.row();
    let row = match row.as_ref() {
        Some(row) => row,
        None => return std::ptr::null(),
    };
    let i = idx as usize;
    if i >= stmt.text_cache.len() {
        return std::ptr::null();
    }
    if !stmt.text_cache[i].is_empty() {
        // we have already cached this value
        return stmt.text_cache[i].as_ptr() as *const ffi::c_uchar;
    }
    match row.get::<&Value>(i) {
        Ok(turso_core::Value::Text(text)) => {
            let buf = &mut stmt.text_cache[i];
            buf.extend(text.as_str().as_bytes());
            buf.push(0);
            buf.as_ptr() as *const ffi::c_uchar
        }
        _ => std::ptr::null(),
    }
}

pub struct TabResult {
    az_result: Vec<*mut ffi::c_char>,
    n_row: usize,
    n_column: usize,
    z_err_msg: Option<CString>,
    rc: ffi::c_int,
}

impl TabResult {
    fn new(initial_capacity: usize) -> Self {
        Self {
            az_result: Vec::with_capacity(initial_capacity),
            n_row: 0,
            n_column: 0,
            z_err_msg: None,
            rc: SQLITE_OK,
        }
    }

    fn free(&mut self) {
        for &ptr in &self.az_result {
            if !ptr.is_null() {
                unsafe {
                    sqlite3_free(ptr as *mut _);
                }
            }
        }
        self.az_result.clear();
    }
}

#[no_mangle]
unsafe extern "C" fn sqlite_get_table_cb(
    context: *mut ffi::c_void,
    n_column: ffi::c_int,
    argv: *mut *mut ffi::c_char,
    colv: *mut *mut ffi::c_char,
) -> ffi::c_int {
    let res = &mut *(context as *mut TabResult);

    if res.n_row == 0 {
        res.n_column = n_column as usize;
        for i in 0..n_column {
            let col_name = *colv.add(i as usize);
            let col_name_cstring = if !col_name.is_null() {
                CStr::from_ptr(col_name).to_owned()
            } else {
                CString::new("NULL").unwrap()
            };
            res.az_result.push(col_name_cstring.into_raw());
        }
    } else if res.n_column != n_column as usize {
        res.z_err_msg = Some(
            CString::new("sqlite3_get_table() called with two or more incompatible queries")
                .unwrap(),
        );
        res.rc = SQLITE_ERROR;
        return SQLITE_ERROR;
    }

    for i in 0..n_column {
        let value = *argv.add(i as usize);
        let value_cstring = if !value.is_null() {
            let value_cstr = CStr::from_ptr(value).to_bytes();
            let len = value_cstr.len();
            let mut buf = vec![0u8; len + 1];
            buf[0..len].copy_from_slice(value_cstr);
            CString::from_vec_with_nul(buf).unwrap()
        } else {
            CString::new("NULL").unwrap()
        };
        res.az_result.push(value_cstring.into_raw());
    }

    res.n_row += 1;
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_get_table(
    db: *mut sqlite3,
    sql: *const ffi::c_char,
    paz_result: *mut *mut *mut ffi::c_char,
    pn_row: *mut ffi::c_int,
    pn_column: *mut ffi::c_int,
    pz_err_msg: *mut *mut ffi::c_char,
) -> ffi::c_int {
    if db.is_null() || sql.is_null() || paz_result.is_null() {
        return SQLITE_ERROR;
    }

    let mut res = TabResult::new(20);

    let rc = sqlite3_exec(
        db,
        sql,
        Some(sqlite_get_table_cb),
        &mut res as *mut _ as *mut _,
        pz_err_msg,
    );

    if rc != SQLITE_OK {
        res.free();
        if let Some(err_msg) = res.z_err_msg {
            if !pz_err_msg.is_null() {
                *pz_err_msg = err_msg.into_raw();
            }
        }
        return rc;
    }

    let n_data = res.az_result.len();

    // Allocate a raw C array with an extra slot at position 0 for the entry count,
    // following the SQLite convention. sqlite3_free_table will step back to read it.
    let array = libc::malloc(std::mem::size_of::<*mut ffi::c_char>() * (n_data + 1))
        as *mut *mut ffi::c_char;
    if array.is_null() {
        res.free();
        return SQLITE_NOMEM;
    }

    // Store entry count at position 0 (matching SQLite's SQLITE_INT_TO_PTR convention)
    *array = (n_data + 1) as *mut ffi::c_char;

    // Copy string pointers to positions 1..=n_data
    for (i, &ptr) in res.az_result.iter().enumerate() {
        *array.add(i + 1) = ptr;
    }

    // Return pointer past the count slot
    *paz_result = array.add(1);
    *pn_row = res.n_row as ffi::c_int;
    *pn_column = res.n_column as ffi::c_int;

    // Drop res without freeing the strings -- they are now owned by the raw array.
    // Vec<*mut c_char> drop just deallocates the Vec buffer, not the pointed-to strings.

    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_free_table(az_result: *mut *mut ffi::c_char) {
    if az_result.is_null() {
        return;
    }
    // Step back one slot to read the entry count stored by sqlite3_get_table
    let array = az_result.sub(1);
    let n = *array as usize;
    for i in 1..n {
        let ptr = *array.add(i);
        if !ptr.is_null() {
            sqlite3_free(ptr as *mut _);
        }
    }
    libc::free(array as *mut _);
}

// sqlite3_result_* functions set the return value of a custom SQL function.
// The context pointer is a *mut SqliteContext cast to void*.

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_null(context: *mut ffi::c_void) {
    if context.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    ctx.result = ExtValue::null();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_int64(context: *mut ffi::c_void, val: i64) {
    if context.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    ctx.result = ExtValue::from_integer(val);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_double(context: *mut ffi::c_void, val: f64) {
    if context.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    ctx.result = ExtValue::from_float(val);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_text(
    context: *mut ffi::c_void,
    text: *const ffi::c_char,
    len: ffi::c_int,
    _destroy: *mut ffi::c_void,
) {
    if context.is_null() || text.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    let s = if len < 0 {
        CStr::from_ptr(text).to_string_lossy().into_owned()
    } else {
        let bytes = std::slice::from_raw_parts(text as *const u8, len as usize);
        String::from_utf8_lossy(bytes).into_owned()
    };
    ctx.result = ExtValue::from_text(s);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_blob(
    context: *mut ffi::c_void,
    blob: *const ffi::c_void,
    len: ffi::c_int,
    _destroy: *mut ffi::c_void,
) {
    if context.is_null() || blob.is_null() || len < 0 {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    let bytes = std::slice::from_raw_parts(blob as *const u8, len as usize).to_vec();
    ctx.result = ExtValue::from_blob(bytes);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_error_nomem(context: *mut ffi::c_void) {
    if context.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    ctx.result = ExtValue::error(turso_ext::ResultCode::OoM);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_error_toobig(context: *mut ffi::c_void) {
    if context.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    ctx.result = ExtValue::error(turso_ext::ResultCode::Error);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_error(
    context: *mut ffi::c_void,
    err: *const ffi::c_char,
    len: ffi::c_int,
) {
    if context.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    let msg = if err.is_null() {
        String::new()
    } else if len < 0 {
        CStr::from_ptr(err).to_string_lossy().into_owned()
    } else {
        let bytes = std::slice::from_raw_parts(err as *const u8, len as usize);
        String::from_utf8_lossy(bytes).into_owned()
    };
    ctx.result = ExtValue::error_with_message(msg);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_aggregate_context(
    _context: *mut ffi::c_void,
    _n: ffi::c_int,
) -> *mut ffi::c_void {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_blob_open(
    _db: *mut sqlite3,
    _db_name: *const ffi::c_char,
    _table_name: *const ffi::c_char,
    _column_name: *const ffi::c_char,
    _rowid: i64,
    _flags: ffi::c_int,
    _blob_out: *mut *mut ffi::c_void,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_blob_read(
    _blob: *mut ffi::c_void,
    _data: *mut ffi::c_void,
    _n: ffi::c_int,
    _offset: ffi::c_int,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_blob_write(
    _blob: *mut ffi::c_void,
    _data: *const ffi::c_void,
    _n: ffi::c_int,
    _offset: ffi::c_int,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_blob_bytes(_blob: *mut ffi::c_void) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_blob_close(_blob: *mut ffi::c_void) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_stricmp(
    _a: *const ffi::c_char,
    _b: *const ffi::c_char,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_create_collation_v2(
    _db: *mut sqlite3,
    _name: *const ffi::c_char,
    _enc: ffi::c_int,
    _context: *mut ffi::c_void,
    _cmp: Option<unsafe extern "C" fn() -> ffi::c_int>,
    _destroy: Option<unsafe extern "C" fn()>,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_create_function_v2(
    db: *mut sqlite3,
    name: *const ffi::c_char,
    _n_args: ffi::c_int,
    _enc: ffi::c_int,
    context: *mut ffi::c_void,
    func: Option<unsafe extern "C" fn()>,
    _step: Option<unsafe extern "C" fn()>,
    _final_: Option<unsafe extern "C" fn()>,
    _destroy: Option<unsafe extern "C" fn()>,
) -> ffi::c_int {
    if db.is_null() || name.is_null() {
        return SQLITE_MISUSE;
    }
    // Only scalar functions (xFunc) are supported for now; skip aggregate registration.
    let x_func_raw = match func {
        Some(f) => f,
        None => return SQLITE_OK,
    };
    // Cast the opaque fn() pointer to the real scalar callback signature.
    let x_func: unsafe extern "C" fn(*mut ffi::c_void, ffi::c_int, *mut *mut ffi::c_void) =
        std::mem::transmute(x_func_raw);

    let func_name = match CStr::from_ptr(name).to_str() {
        Ok(s) => s.to_owned(),
        Err(_) => return SQLITE_MISUSE,
    };

    // Cast the destroy callback.
    let destroy_fn: Option<unsafe extern "C" fn(*mut ffi::c_void)> =
        _destroy.map(|f| std::mem::transmute(f));

    // Allocate a bridge slot.  Reuse an existing slot with the same name
    // (invoking the old destroy callback) before falling back to a free slot.
    let mut slots = func_slots().lock().unwrap();
    let slot_id = if let Some(id) = slots
        .iter()
        .position(|s| s.as_ref().is_some_and(|s| s.name == func_name))
    {
        // Reuse existing slot — invoke old destroy callback on old user data.
        if let Some(old) = slots[id].take() {
            if old.destroy != 0 {
                let old_destroy: unsafe extern "C" fn(*mut ffi::c_void) =
                    std::mem::transmute(old.destroy);
                old_destroy(old.p_app as *mut ffi::c_void);
            }
        }
        id
    } else {
        match slots.iter().position(|s| s.is_none()) {
            Some(id) => id,
            None => return SQLITE_ERROR, // all 32 slots used
        }
    };
    slots[slot_id] = Some(FuncSlot {
        x_func,
        p_app: context as usize,
        destroy: destroy_fn.map_or(0, |f| f as usize),
        name: func_name.clone(),
    });
    drop(slots);

    let bridge = FUNC_BRIDGES[slot_id];

    let func_name_c = match CString::new(func_name.as_str()) {
        Ok(s) => s,
        Err(_) => {
            func_slots().lock().unwrap()[slot_id] = None;
            return SQLITE_ERROR;
        }
    };

    let db_ref = &*db;
    let inner = db_ref.inner.lock().unwrap();
    let api = inner.conn._build_turso_ext();
    let rc = (api.register_scalar_function)(api.ctx, func_name_c.as_ptr(), bridge);
    inner.conn._free_extension_ctx(api);

    if rc != turso_ext::ResultCode::OK {
        func_slots().lock().unwrap()[slot_id] = None;
        return SQLITE_ERROR;
    }

    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_create_window_function(
    _db: *mut sqlite3,
    _name: *const ffi::c_char,
    _n_args: ffi::c_int,
    _enc: ffi::c_int,
    _context: *mut ffi::c_void,
    _x_step: Option<unsafe extern "C" fn()>,
    _x_final: Option<unsafe extern "C" fn()>,
    _x_value: Option<unsafe extern "C" fn()>,
    _x_inverse: Option<unsafe extern "C" fn()>,
    _destroy: Option<unsafe extern "C" fn()>,
) -> ffi::c_int {
    stub!();
}

/// Returns the error message for the most recent failed API call to connection.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_errmsg(db: *mut sqlite3) -> *const ffi::c_char {
    if db.is_null() {
        return sqlite3_errstr(SQLITE_NOMEM);
    }
    let db: &mut sqlite3 = &mut *db;
    let db = db.inner.lock().unwrap();
    if !sqlite3_safety_check_sick_or_ok(&db) {
        return sqlite3_errstr(SQLITE_MISUSE);
    }
    if db.malloc_failed {
        return sqlite3_errstr(SQLITE_NOMEM);
    }
    let err_msg = if db.err_code != SQLITE_OK {
        if !db.p_err.is_null() {
            db.p_err as *const ffi::c_char
        } else {
            std::ptr::null()
        }
    } else {
        std::ptr::null()
    };
    if err_msg.is_null() {
        return sqlite3_errstr(db.err_code);
    }
    err_msg
}

/// Returns the extended error code for the most recent failed API call to connection.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_extended_errcode(db: *mut sqlite3) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &mut sqlite3 = &mut *db;
    let db = db.inner.lock().unwrap();
    if !sqlite3_safety_check_sick_or_ok(&db) {
        return SQLITE_MISUSE;
    }
    if db.malloc_failed {
        return SQLITE_NOMEM;
    }
    db.err_code & db.err_mask
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_complete(_sql: *const ffi::c_char) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_threadsafe() -> ffi::c_int {
    1
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_libversion() -> *const ffi::c_char {
    c"3.42.0".as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_libversion_number() -> ffi::c_int {
    3042000
}

fn sqlite3_errstr_impl(rc: i32) -> *const ffi::c_char {
    static ERROR_MESSAGES: [&[u8]; 29] = [
        b"not an error\0",                         // SQLITE_OK
        b"SQL logic error\0",                      // SQLITE_ERROR
        b"\0",                                     // SQLITE_INTERNAL
        b"access permission denied\0",             // SQLITE_PERM
        b"query aborted\0",                        // SQLITE_ABORT
        b"database is locked\0",                   // SQLITE_BUSY
        b"database table is locked\0",             // SQLITE_LOCKED
        b"out of memory\0",                        // SQLITE_NOMEM
        b"attempt to write a readonly database\0", // SQLITE_READONLY
        b"interrupted\0",                          // SQLITE_INTERRUPT
        b"disk I/O error\0",                       // SQLITE_IOERR
        b"database disk image is malformed\0",     // SQLITE_CORRUPT
        b"unknown operation\0",                    // SQLITE_NOTFOUND
        b"database or disk is full\0",             // SQLITE_FULL
        b"unable to open database file\0",         // SQLITE_CANTOPEN
        b"locking protocol\0",                     // SQLITE_PROTOCOL
        b"\0",                                     // SQLITE_EMPTY
        b"database schema has changed\0",          // SQLITE_SCHEMA
        b"string or blob too big\0",               // SQLITE_TOOBIG
        b"constraint failed\0",                    // SQLITE_CONSTRAINT
        b"datatype mismatch\0",                    // SQLITE_MISMATCH
        b"bad parameter or other API misuse\0",    // SQLITE_MISUSE
        #[cfg(feature = "lfs")]
        b"\0",      // SQLITE_NOLFS
        #[cfg(not(feature = "lfs"))]
        b"large file support is disabled\0", // SQLITE_NOLFS
        b"authorization denied\0",                 // SQLITE_AUTH
        b"\0",                                     // SQLITE_FORMAT
        b"column index out of range\0",            // SQLITE_RANGE
        b"file is not a database\0",               // SQLITE_NOTADB
        b"notification message\0",                 // SQLITE_NOTICE
        b"warning message\0",                      // SQLITE_WARNING
    ];

    static UNKNOWN_ERROR: &[u8] = b"unknown error\0";
    static ABORT_ROLLBACK: &[u8] = b"abort due to ROLLBACK\0";
    static ANOTHER_ROW_AVAILABLE: &[u8] = b"another row available\0";
    static NO_MORE_ROWS_AVAILABLE: &[u8] = b"no more rows available\0";

    let msg = match rc {
        SQLITE_ABORT_ROLLBACK => ABORT_ROLLBACK,
        SQLITE_ROW => ANOTHER_ROW_AVAILABLE,
        SQLITE_DONE => NO_MORE_ROWS_AVAILABLE,
        _ => {
            let rc = rc & 0xff;
            let idx = rc & 0xff;
            if (idx as usize) < ERROR_MESSAGES.len() && !ERROR_MESSAGES[rc as usize].is_empty() {
                ERROR_MESSAGES[rc as usize]
            } else {
                UNKNOWN_ERROR
            }
        }
    };

    msg.as_ptr() as *const ffi::c_char
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_wal_checkpoint(
    db: *mut sqlite3,
    db_name: *const ffi::c_char,
) -> ffi::c_int {
    sqlite3_wal_checkpoint_v2(
        db,
        db_name,
        SQLITE_CHECKPOINT_PASSIVE,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    )
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_wal_checkpoint_v2(
    db: *mut sqlite3,
    _db_name: *const ffi::c_char,
    mode: ffi::c_int,
    log_size: *mut ffi::c_int,
    checkpoint_count: *mut ffi::c_int,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &mut sqlite3 = &mut *db;
    let db = db.inner.lock().unwrap();
    let chkptmode = match mode {
        SQLITE_CHECKPOINT_PASSIVE => CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
        SQLITE_CHECKPOINT_RESTART => CheckpointMode::Restart,
        SQLITE_CHECKPOINT_TRUNCATE => CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
        SQLITE_CHECKPOINT_FULL => CheckpointMode::Full,
        _ => return SQLITE_MISUSE, // Unsupported mode
    };
    match db.conn.checkpoint(chkptmode) {
        Ok(res) => {
            if !log_size.is_null() {
                (*log_size) = res.wal_max_frame as ffi::c_int;
            }
            if !checkpoint_count.is_null() {
                (*checkpoint_count) = res.wal_checkpoint_backfilled as ffi::c_int;
            }
            SQLITE_OK
        }
        Err(e) => {
            if matches!(e, turso_core::LimboError::Busy) {
                SQLITE_BUSY
            } else {
                SQLITE_ERROR
            }
        }
    }
}

/// Get the number of frames in the WAL.
///
/// The `libsql_wal_frame_count` function returns the number of frames
/// in the WAL in the `p_frame_count` parameter.
///
/// # Returns
///
/// - `SQLITE_OK` if the number of frames in the WAL file is
///   successfully returned.
/// - `SQLITE_MISUSE` if the `db` is `NULL`.
/// - `SQLITE_ERROR` if an error occurs while getting the number of frames
///   in the WAL file.
///
/// # Safety
///
/// - The `db` must be a valid pointer to a `sqlite3` database connection.
/// - The `p_frame_count` must be a valid pointer to a `u32` that will store
///   the number of frames in the WAL file.
#[no_mangle]
pub unsafe extern "C" fn libsql_wal_frame_count(
    db: *mut sqlite3,
    p_frame_count: *mut u32,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &mut sqlite3 = &mut *db;
    let db = db.inner.lock().unwrap();
    let frame_count = match db.conn.wal_state() {
        Ok(state) => state.max_frame as u32,
        Err(_) => return SQLITE_ERROR,
    };
    *p_frame_count = frame_count;
    SQLITE_OK
}

/// Get a frame from the WAL file
///
/// The `libsql_wal_get_frame` function extracts frame `frame_no` from
/// the WAL for database connection `db` into memory pointed to by `p_frame`
/// of size `frame_len`.
///
/// # Returns
///
/// - `SQLITE_OK` if the frame is successfully returned.
/// - `SQLITE_MISUSE` if the `db` is `NULL`.
/// - `SQLITE_ERROR` if an error occurs while getting the frame.
///
/// # Safety
///
/// - The `db` must be a valid pointer to a `sqlite3` database connection.
/// - The `frame_no` must be a valid frame index.
/// - The `p_frame` must be a valid pointer to a `u8` that will store
///   the frame data.
/// - The `frame_len` must be the size of the frame.
#[no_mangle]
pub unsafe extern "C" fn libsql_wal_get_frame(
    db: *mut sqlite3,
    frame_no: u32,
    p_frame: *mut u8,
    frame_len: u32,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &mut sqlite3 = &mut *db;
    let db = db.inner.lock().unwrap();
    let frame = std::slice::from_raw_parts_mut(p_frame, frame_len as usize);
    match db.conn.wal_get_frame(frame_no as u64, frame) {
        Ok(..) => SQLITE_OK,
        Err(_) => SQLITE_ERROR,
    }
}

/// Insert a frame into the WAL file
///
/// The `libsql_wal_insert_frame` function insert frame at position frame_no
/// with content specified into memory pointed by `p_frame` of size `frame_len`.
///
/// # Returns
///
/// - `SQLITE_OK` if the frame is successfully inserted.
/// - `SQLITE_MISUSE` if the `db` is `NULL`.
/// - `SQLITE_ERROR` if an error occurs while inserting the frame.
///
/// # Safety
///
/// - The `db` must be a valid pointer to a `sqlite3` database connection.
/// - The `frame_no` must be a valid frame index.
/// - The `p_frame` must be a valid pointer to a `u8` that stores the frame data.
/// - The `frame_len` must be the size of the frame.
#[no_mangle]
pub unsafe extern "C" fn libsql_wal_insert_frame(
    db: *mut sqlite3,
    frame_no: u32,
    p_frame: *const u8,
    frame_len: u32,
    p_conflict: *mut i32,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &mut sqlite3 = &mut *db;
    let db = db.inner.lock().unwrap();
    let frame = std::slice::from_raw_parts(p_frame, frame_len as usize);
    match db.conn.wal_insert_frame(frame_no as u64, frame) {
        Ok(_) => SQLITE_OK,
        Err(LimboError::Conflict(..)) => {
            if !p_conflict.is_null() {
                *p_conflict = 1;
            }
            SQLITE_ERROR
        }
        Err(_) => SQLITE_ERROR,
    }
}

/// Disable WAL checkpointing.
///
/// Note: This function disables WAL checkpointing entirely for the connection. This is different from
/// sqlite3_wal_autocheckpoint() which only disables automatic checkpoints
/// for the current connection, but still allows checkpointing when the
/// connection is closed.
#[no_mangle]
pub unsafe extern "C" fn libsql_wal_disable_checkpoint(db: *mut sqlite3) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &mut sqlite3 = &mut *db;
    let db = db.inner.lock().unwrap();
    db.conn.wal_auto_checkpoint_disable();
    SQLITE_OK
}

fn sqlite3_safety_check_sick_or_ok(db: &sqlite3Inner) -> bool {
    matches!(
        db.e_open_state,
        SQLITE_STATE_SICK | SQLITE_STATE_OPEN | SQLITE_STATE_BUSY
    )
}

// https://sqlite.org/c3ref/table_column_metadata.html
#[no_mangle]
pub unsafe extern "C" fn sqlite3_table_column_metadata(
    db: *mut sqlite3,
    z_db_name: *const ffi::c_char,
    z_table_name: *const ffi::c_char,
    z_column_name: *const ffi::c_char,
    pz_data_type: *mut *const ffi::c_char,
    pz_coll_seq: *mut *const ffi::c_char,
    p_not_null: *mut ffi::c_int,
    p_primary_key: *mut ffi::c_int,
    p_autoinc: *mut ffi::c_int,
) -> ffi::c_int {
    trace!("sqlite3_table_column_metadata");

    let mut rc = SQLITE_OK;
    let mut z_data_type: *const ffi::c_char = std::ptr::null();
    let mut z_coll_seq: *const ffi::c_char = std::ptr::null();
    let mut not_null = 0;
    let mut primary_key = 0;
    let mut autoinc = 0;

    // Safety checks
    if db.is_null() || z_table_name.is_null() {
        return SQLITE_MISUSE;
    }

    let db_inner = &(*db).inner.lock().unwrap();

    // Convert C strings to Rust strings
    let table_name = match CStr::from_ptr(z_table_name).to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };

    // Handle database name (can be NULL for main database)
    let db_name = if z_db_name.is_null() {
        "main"
    } else {
        match CStr::from_ptr(z_db_name).to_str() {
            Ok(s) => s,
            Err(_) => return SQLITE_MISUSE,
        }
    };

    // For now, we only support the main database
    if db_name != "main" {
        rc = SQLITE_ERROR;
    } else {
        // Handle column name (can be NULL to just check table existence)
        if !z_column_name.is_null() {
            let column_name = match CStr::from_ptr(z_column_name).to_str() {
                Ok(s) => s,
                Err(_) => return SQLITE_MISUSE,
            };

            // Use pragma table_info to get column information
            match db_inner
                .conn
                .pragma_query(&format!("table_info({table_name})"))
            {
                Ok(rows) => {
                    let mut found_column = false;
                    for row in rows {
                        let col_name: &str = match &row[1] {
                            turso_core::Value::Text(text) => text.as_str(),
                            _ => return SQLITE_ERROR,
                        }; // name column
                        if col_name == column_name {
                            // Found the column, extract metadata
                            let col_type: String = match &row[2] {
                                turso_core::Value::Text(text) => text.as_str().to_string(),
                                _ => return SQLITE_ERROR,
                            }; // type column
                            let col_notnull: i64 = row[3].as_int().unwrap(); // notnull column
                            let col_pk: i64 = row[5].as_int().unwrap(); // pk column

                            z_data_type = CString::new(col_type)
                                .expect("CString::new failed")
                                .into_raw();
                            z_coll_seq = CString::new("BINARY")
                                .expect("CString::new failed")
                                .into_raw();
                            not_null = if col_notnull != 0 { 1 } else { 0 };
                            primary_key = if col_pk != 0 { 1 } else { 0 };

                            // For now, we don't support auto-increment detection
                            autoinc = 0;

                            found_column = true;
                            break;
                        }
                    }

                    if !found_column {
                        // Check if it's a rowid reference
                        if column_name == "rowid"
                            || column_name == "oid"
                            || column_name == "_rowid_"
                        {
                            // For rowid columns, return INTEGER type
                            z_data_type = CString::new("INTEGER")
                                .expect("CString::new failed")
                                .into_raw();
                            z_coll_seq = CString::new("BINARY")
                                .expect("CString::new failed")
                                .into_raw();
                            not_null = 0;
                            primary_key = 1;
                            autoinc = 0;
                        } else {
                            rc = SQLITE_ERROR;
                        }
                    }
                }
                Err(_) => {
                    rc = SQLITE_ERROR;
                }
            }
        }
    }

    // Set output parameters
    if !pz_data_type.is_null() {
        *pz_data_type = z_data_type;
    }
    if !pz_coll_seq.is_null() {
        *pz_coll_seq = z_coll_seq;
    }
    if !p_not_null.is_null() {
        *p_not_null = not_null;
    }
    if !p_primary_key.is_null() {
        *p_primary_key = primary_key;
    }
    if !p_autoinc.is_null() {
        *p_autoinc = autoinc;
    }

    rc
}

fn handle_limbo_err(err: LimboError, container: *mut *mut ffi::c_char) -> i32 {
    if !container.is_null() {
        let err_msg = format!("{err}");
        unsafe { *container = CString::new(err_msg).unwrap().into_raw() };
    }
    match err {
        LimboError::Corrupt(..) => SQLITE_CORRUPT,
        LimboError::NotADB => SQLITE_NOTADB,
        LimboError::Constraint(_) | LimboError::ForeignKeyConstraint(_) => SQLITE_CONSTRAINT,
        LimboError::DatabaseFull(_) => SQLITE_FULL,
        LimboError::TableLocked => SQLITE_LOCKED,
        LimboError::ReadOnly => SQLITE_READONLY,
        LimboError::Busy => SQLITE_BUSY,
        _ => SQLITE_ERROR,
    }
}
