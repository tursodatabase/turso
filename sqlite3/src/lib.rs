#![allow(clippy::missing_safety_doc)]
#![allow(non_camel_case_types)]

use std::ffi::{self, CStr, CString};
use tracing::trace;
use turso_core::Value;

use std::sync::{Arc, Mutex};

macro_rules! stub {
    () => {
        todo!("{} is not implemented", stringify!($fn));
    };
}

pub const SQLITE_OK: ffi::c_int = 0;
pub const SQLITE_ERROR: ffi::c_int = 1;
pub const SQLITE_ABORT: ffi::c_int = 4;
pub const SQLITE_BUSY: ffi::c_int = 5;
pub const SQLITE_NOMEM: ffi::c_int = 7;
pub const SQLITE_INTERRUPT: ffi::c_int = 9;
pub const SQLITE_NOTFOUND: ffi::c_int = 12;
pub const SQLITE_CANTOPEN: ffi::c_int = 14;
pub const SQLITE_MISUSE: ffi::c_int = 21;
pub const SQLITE_ROW: ffi::c_int = 100;
pub const SQLITE_DONE: ffi::c_int = 101;
pub const SQLITE_ABORT_ROLLBACK: ffi::c_int = SQLITE_ABORT | (2 << 8);
pub const SQLITE_STATE_OPEN: u8 = 0x76;
pub const SQLITE_STATE_SICK: u8 = 0xba;
pub const SQLITE_STATE_BUSY: u8 = 0x6d;

pub const SQLITE_CHECKPOINT_PASSIVE: ffi::c_int = 0;
pub const SQLITE_CHECKPOINT_FULL: ffi::c_int = 1;
pub const SQLITE_CHECKPOINT_RESTART: ffi::c_int = 2;
pub const SQLITE_CHECKPOINT_TRUNCATE: ffi::c_int = 3;

pub struct sqlite3 {
    pub(crate) inner: Arc<Mutex<sqlite3Inner>>,
}

struct sqlite3Inner {
    pub(crate) io: Arc<dyn turso_core::IO>,
    pub(crate) _db: Arc<turso_core::Database>,
    pub(crate) conn: Arc<turso_core::Connection>,
    pub(crate) err_code: ffi::c_int,
    pub(crate) err_mask: ffi::c_int,
    pub(crate) malloc_failed: bool,
    pub(crate) e_open_state: u8,
    pub(crate) p_err: *mut ffi::c_void,
}

impl sqlite3 {
    pub fn new(
        io: Arc<dyn turso_core::IO>,
        db: Arc<turso_core::Database>,
        conn: Arc<turso_core::Connection>,
    ) -> Self {
        let inner = sqlite3Inner {
            io,
            _db: db,
            conn,
            err_code: SQLITE_OK,
            err_mask: 0xFFFFFFFFu32 as i32,
            malloc_failed: false,
            e_open_state: SQLITE_STATE_OPEN,
            p_err: std::ptr::null_mut(),
        };
        #[allow(clippy::arc_with_non_send_sync)]
        let inner = Arc::new(Mutex::new(inner));
        Self { inner }
    }
}

pub struct sqlite3_stmt {
    pub(crate) db: *mut sqlite3,
    pub(crate) stmt: turso_core::Statement,
}

impl sqlite3_stmt {
    pub fn new(db: *mut sqlite3, stmt: turso_core::Statement) -> Self {
        Self { db, stmt }
    }
}

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
    let filename = CStr::from_ptr(filename);
    let filename = match filename.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    let io: Arc<dyn turso_core::IO> = match filename {
        ":memory:" => Arc::new(turso_core::MemoryIO::new()),
        _ => match turso_core::PlatformIO::new() {
            Ok(io) => Arc::new(io),
            Err(_) => return SQLITE_CANTOPEN,
        },
    };
    match turso_core::Database::open_file(io.clone(), filename, false, false) {
        Ok(db) => {
            let conn = db.connect().unwrap();
            *db_out = Box::leak(Box::new(sqlite3::new(io, db, conn)));
            SQLITE_OK
        }
        Err(e) => {
            trace!("error opening database {}: {:?}", filename, e);
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

#[no_mangle]
pub unsafe extern "C" fn sqlite3_busy_timeout(_db: *mut sqlite3, _ms: ffi::c_int) -> ffi::c_int {
    stub!();
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
    let db = db.inner.lock().unwrap();
    let sql = CStr::from_ptr(sql);
    let sql = match sql.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    let stmt = match db.conn.prepare(sql) {
        Ok(stmt) => stmt,
        Err(_) => return SQLITE_ERROR,
    };
    *out_stmt = Box::leak(Box::new(sqlite3_stmt::new(raw_db, stmt)));
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let _ = Box::from_raw(stmt);
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_step(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt = &mut *stmt;
    let db = &mut *stmt.db;
    loop {
        let _db = db.inner.lock().unwrap();
        if let Ok(result) = stmt.stmt.step() {
            match result {
                turso_core::StepResult::IO => {
                    stmt.stmt.run_once().unwrap();
                    continue;
                }
                turso_core::StepResult::Done => return SQLITE_DONE,
                turso_core::StepResult::Interrupt => return SQLITE_INTERRUPT,
                turso_core::StepResult::Row => return SQLITE_ROW,
                turso_core::StepResult::Busy => return SQLITE_BUSY,
            }
        } else {
            return SQLITE_ERROR;
        }
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
    _callback: exec_callback,
    _context: *mut ffi::c_void,
    _err: *mut *mut ffi::c_char,
) -> ffi::c_int {
    if db.is_null() || sql.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &mut sqlite3 = &mut *db;
    let db = db.inner.lock().unwrap();
    let sql = CStr::from_ptr(sql);
    let sql = match sql.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    trace!("sqlite3_exec(sql={})", sql);
    match db.conn.execute(sql) {
        Ok(_) => SQLITE_OK,
        Err(_) => SQLITE_ERROR,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_reset(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt = &mut *stmt;
    stmt.stmt.reset();
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_changes(db: *mut sqlite3) -> ffi::c_int {
    if db.is_null() {
        return 0;
    }
    let db = &mut *db;
    let db = db.inner.lock().unwrap();
    db.conn.changes() as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_stmt_readonly(_stmt: *mut sqlite3_stmt) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_stmt_busy(_stmt: *mut sqlite3_stmt) -> ffi::c_int {
    stub!();
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
pub unsafe extern "C" fn sqlite3_get_autocommit(_db: *mut sqlite3) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_total_changes(_db: *mut sqlite3) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_last_insert_rowid(_db: *mut sqlite3) -> i64 {
    stub!();
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
pub unsafe extern "C" fn sqlite3_malloc64(_n: ffi::c_int) -> *mut ffi::c_void {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_free(_ptr: *mut ffi::c_void) {
    stub!();
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
pub unsafe extern "C" fn sqlite3_user_data(_context: *mut ffi::c_void) -> *mut ffi::c_void {
    stub!();
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
pub unsafe extern "C" fn sqlite3_bind_parameter_count(_stmt: *mut sqlite3_stmt) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_parameter_name(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> *const ffi::c_char {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_null(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_int64(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
    _val: i64,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_double(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
    _val: f64,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_text(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
    _text: *const ffi::c_char,
    _len: ffi::c_int,
    _destroy: *mut ffi::c_void,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_blob(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
    _blob: *const ffi::c_void,
    _len: ffi::c_int,
    _destroy: *mut ffi::c_void,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_type(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_count(_stmt: *mut sqlite3_stmt) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_decltype(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> *const ffi::c_char {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_name(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> *const ffi::c_char {
    stub!();
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
pub unsafe extern "C" fn sqlite3_column_double(_stmt: *mut sqlite3_stmt, _idx: ffi::c_int) -> f64 {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_blob(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> *const ffi::c_void {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_bytes(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_type(value: *mut ffi::c_void) -> ffi::c_int {
    let value = value as *mut turso_core::Value;
    let value = &*value;
    match value {
        turso_core::Value::Null => 0,
        turso_core::Value::Integer(_) => 1,
        turso_core::Value::Float(_) => 2,
        turso_core::Value::Text(_) => 3,
        turso_core::Value::Blob(_) => 4,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_int64(value: *mut ffi::c_void) -> i64 {
    let value = value as *mut turso_core::Value;
    let value = &*value;
    match value {
        turso_core::Value::Integer(i) => *i,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_double(value: *mut ffi::c_void) -> f64 {
    let value = value as *mut turso_core::Value;
    let value = &*value;
    match value {
        turso_core::Value::Float(f) => *f,
        _ => 0.0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_text(value: *mut ffi::c_void) -> *const ffi::c_uchar {
    let value = value as *mut turso_core::Value;
    let value = &*value;
    match value {
        turso_core::Value::Text(text) => text.as_str().as_ptr(),
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_blob(value: *mut ffi::c_void) -> *const ffi::c_void {
    let value = value as *mut turso_core::Value;
    let value = &*value;
    match value {
        turso_core::Value::Blob(blob) => blob.as_ptr() as *const ffi::c_void,
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_bytes(value: *mut ffi::c_void) -> ffi::c_int {
    let value = value as *mut turso_core::Value;
    let value = &*value;
    match value {
        turso_core::Value::Blob(blob) => blob.len() as ffi::c_int,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_text(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *const ffi::c_uchar {
    let stmt = &mut *stmt;
    let row = stmt.stmt.row();
    let row = match row.as_ref() {
        Some(row) => row,
        None => return std::ptr::null(),
    };
    match row.get::<&Value>(idx as usize) {
        Ok(turso_core::Value::Text(text)) => text.as_str().as_ptr(),
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
            let len = libc::strlen(value);
            let mut buf = Vec::with_capacity(len + 1);
            libc::strncpy(buf.as_mut_ptr() as *mut ffi::c_char, value, len);
            buf.set_len(len + 1);
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

    let total_results = res.az_result.len();
    if res.az_result.capacity() > total_results {
        res.az_result.shrink_to_fit();
    }

    *paz_result = res.az_result.as_mut_ptr();
    *pn_row = res.n_row as ffi::c_int;
    *pn_column = res.n_column as ffi::c_int;

    std::mem::forget(res);

    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_free_table(paz_result: *mut *mut *mut ffi::c_char) {
    let res = &mut *(paz_result as *mut TabResult);
    res.free();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_null(_context: *mut ffi::c_void) {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_int64(_context: *mut ffi::c_void, _val: i64) {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_double(_context: *mut ffi::c_void, _val: f64) {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_text(
    _context: *mut ffi::c_void,
    _text: *const ffi::c_char,
    _len: ffi::c_int,
    _destroy: *mut ffi::c_void,
) {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_blob(
    _context: *mut ffi::c_void,
    _blob: *const ffi::c_void,
    _len: ffi::c_int,
    _destroy: *mut ffi::c_void,
) {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_error_nomem(_context: *mut ffi::c_void) {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_error_toobig(_context: *mut ffi::c_void) {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_error(
    _context: *mut ffi::c_void,
    _err: *const ffi::c_char,
    _len: ffi::c_int,
) {
    stub!();
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
    _db: *mut sqlite3,
    _name: *const ffi::c_char,
    _n_args: ffi::c_int,
    _enc: ffi::c_int,
    _context: *mut ffi::c_void,
    _func: Option<unsafe extern "C" fn()>,
    _step: Option<unsafe extern "C" fn()>,
    _final_: Option<unsafe extern "C" fn()>,
    _destroy: Option<unsafe extern "C" fn()>,
) -> ffi::c_int {
    stub!();
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
    const ERROR_MESSAGES: [&str; 29] = [
        "not an error",                         // SQLITE_OK
        "SQL logic error",                      // SQLITE_ERROR
        "",                                     // SQLITE_INTERNAL
        "access permission denied",             // SQLITE_PERM
        "query aborted",                        // SQLITE_ABORT
        "database is locked",                   // SQLITE_BUSY
        "database table is locked",             // SQLITE_LOCKED
        "out of memory",                        // SQLITE_NOMEM
        "attempt to write a readonly database", // SQLITE_READONLY
        "interrupted",                          // SQLITE_INTERRUPT
        "disk I/O error",                       // SQLITE_IOERR
        "database disk image is malformed",     // SQLITE_CORRUPT
        "unknown operation",                    // SQLITE_NOTFOUND
        "database or disk is full",             // SQLITE_FULL
        "unable to open database file",         // SQLITE_CANTOPEN
        "locking protocol",                     // SQLITE_PROTOCOL
        "",                                     // SQLITE_EMPTY
        "database schema has changed",          // SQLITE_SCHEMA
        "string or blob too big",               // SQLITE_TOOBIG
        "constraint failed",                    // SQLITE_CONSTRAINT
        "datatype mismatch",                    // SQLITE_MISMATCH
        "bad parameter or other API misuse",    // SQLITE_MISUSE
        #[cfg(feature = "lfs")]
        "",      // SQLITE_NOLFS
        #[cfg(not(feature = "lfs"))]
        "large file support is disabled", // SQLITE_NOLFS
        "authorization denied",                 // SQLITE_AUTH
        "",                                     // SQLITE_FORMAT
        "column index out of range",            // SQLITE_RANGE
        "file is not a database",               // SQLITE_NOTADB
        "notification message",                 // SQLITE_NOTICE
        "warning message",                      // SQLITE_WARNING
    ];

    const UNKNOWN_ERROR: &str = "unknown error";
    const ABORT_ROLLBACK: &str = "abort due to ROLLBACK";
    const ANOTHER_ROW_AVAILABLE: &str = "another row available";
    const NO_MORE_ROWS_AVAILABLE: &str = "no more rows available";

    match rc {
        SQLITE_ABORT_ROLLBACK => ABORT_ROLLBACK.as_ptr() as *const ffi::c_char,
        SQLITE_ROW => ANOTHER_ROW_AVAILABLE.as_ptr() as *const ffi::c_char,
        SQLITE_DONE => NO_MORE_ROWS_AVAILABLE.as_ptr() as *const ffi::c_char,
        _ => {
            let rc = rc & 0xff;
            if rc >= 0
                && rc < ERROR_MESSAGES.len() as i32
                && !ERROR_MESSAGES[rc as usize].is_empty()
            {
                ERROR_MESSAGES[rc as usize].as_ptr() as *const ffi::c_char
            } else {
                UNKNOWN_ERROR.as_ptr() as *const ffi::c_char
            }
        }
    }
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
    _mode: ffi::c_int,
    _log_size: *mut ffi::c_int,
    _checkpoint_count: *mut ffi::c_int,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &mut sqlite3 = &mut *db;
    let db = db.inner.lock().unwrap();
    // TODO: Checkpointing modes and reporting back log size and checkpoint count to caller.
    if db.conn.checkpoint().is_err() {
        return SQLITE_ERROR;
    }
    SQLITE_OK
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
    let frame_count = match db.conn.wal_frame_count() {
        Ok(count) => count as u32,
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
    match db.conn.wal_get_frame(frame_no, p_frame, frame_len) {
        Ok(c) => match db.io.wait_for_completion(c) {
            Ok(_) => SQLITE_OK,
            Err(_) => SQLITE_ERROR,
        },
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
    db.conn.wal_disable_checkpoint();
    SQLITE_OK
}

fn sqlite3_safety_check_sick_or_ok(db: &sqlite3Inner) -> bool {
    match db.e_open_state {
        SQLITE_STATE_SICK | SQLITE_STATE_OPEN | SQLITE_STATE_BUSY => true,
        _ => {
            eprintln!("Invalid database state: {}", db.e_open_state);
            false
        }
    }
}
