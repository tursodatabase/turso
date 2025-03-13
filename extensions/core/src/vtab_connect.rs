use crate::{types::StepResult, ExtResult, ResultCode, Value};
use std::{
    ffi::{c_char, c_void, CStr, CString},
    num::NonZeroUsize,
    rc::Rc,
};

pub type ConnectFn = unsafe extern "C" fn(ctx: *mut c_void) -> *mut Conn;
pub type PrepareStmtFn = unsafe extern "C" fn(api: *mut Conn, sql: *const c_char) -> *const Stmt;
pub type GetColumnNamesFn =
    unsafe extern "C" fn(ctx: *mut Stmt, count: *mut i32) -> *mut *mut c_char;
pub type BindArgsFn =
    unsafe extern "C" fn(ctx: *mut Stmt, idx: i32, arg: *const Value) -> ResultCode;
pub type StmtStepFn = unsafe extern "C" fn(ctx: *mut Stmt) -> ResultCode;
pub type StmtGetRowValuesFn = unsafe extern "C" fn(ctx: *mut Stmt);
pub type FreeCurrentRowFn = unsafe extern "C" fn(ctx: *mut Stmt);
pub type CloseConnectionFn = unsafe extern "C" fn(ctx: *mut c_void);
pub type CloseStmtFn = unsafe extern "C" fn(ctx: *mut Stmt);

/// core database connection
// public fields for core only
pub struct Conn {
    // Rc::into_raw from core::Connection
    pub _ctx: *mut c_void,
    pub _prepare_stmt: PrepareStmtFn,
    pub _close: CloseConnectionFn,
}

impl Conn {
    pub fn new(ctx: *mut c_void, prepare_stmt: PrepareStmtFn, close: CloseConnectionFn) -> Self {
        Conn {
            _ctx: ctx,
            _prepare_stmt: prepare_stmt,
            _close: close,
        }
    }
    /// # Safety
    pub unsafe fn from_ptr(ptr: *mut Conn) -> crate::ExtResult<&'static mut Self> {
        if ptr.is_null() {
            return Err(ResultCode::Error);
        }
        Ok(unsafe { &mut *(ptr) })
    }

    pub fn close(&self) {
        unsafe { (self._close)(self._ctx) };
    }

    pub fn prepare_stmt(&self, sql: &str) -> *const Stmt {
        let sql = CString::new(sql).unwrap();
        unsafe { (self._prepare_stmt)(self as *const Conn as *mut Conn, sql.as_ptr()) }
    }
}

/// Prepared statement for querying a core database connection
/// public API with wrapper methods for extensions
#[derive(Debug)]
pub struct Statement {
    _ctx: *const Stmt,
}

/// The Database connection that opened the vtab:
/// Public API to expose methods for extensions
#[derive(Debug)]
pub struct Connection {
    _ctx: *const Conn,
}

impl Connection {
    pub fn new(ctx: *const Conn) -> Self {
        Connection { _ctx: ctx }
    }

    pub fn prepare(self: &Rc<Self>, sql: &str) -> ExtResult<Statement> {
        let stmt = unsafe { (*self._ctx).prepare_stmt(sql) };
        if stmt.is_null() {
            return Err(ResultCode::Error);
        }
        Ok(Statement { _ctx: stmt })
    }

    pub fn close(self) {
        unsafe { ((*self._ctx)._close)(self._ctx as *mut c_void) };
    }
}

impl Statement {
    /// Bind a value to a parameter in the prepared statement
    ///```ignore
    /// let stmt = conn.prepare_stmt("select * from users where name = ?");
    /// stmt.bind(1, Value::from_text("test".into()));
    pub fn bind(&self, idx: NonZeroUsize, arg: &Value) {
        let arg = arg as *const Value;
        unsafe { (*self._ctx).bind_args(idx, arg) }
    }

    /// Execute the statement and return the next row
    ///```ignore
    /// while stmt.step() == StepResult::Row {
    ///     let row = stmt.get_row();
    ///     println!("row: {:?}", row);
    /// }
    /// ```
    pub fn step(&self) -> StepResult {
        unsafe { (*self._ctx).step() }
    }

    // Get the current row values
    ///```ignore
    /// while stmt.step() == StepResult::Row {
    ///    let row = stmt.get_row();
    ///    println!("row: {:?}", row);
    ///```
    pub fn get_row(&mut self) -> &[Value] {
        unsafe { (*self._ctx).get_row() }
    }

    /// Get the result column names for the prepared statement
    pub fn get_column_names(&self) -> Vec<String> {
        unsafe { (*self._ctx).get_column_names() }
    }

    /// Close the statement
    pub fn close(&self) {
        unsafe { (*self._ctx).close() }
    }
}

/// Internal/core use _only_
/// Extensions should not import or use this type directly
#[repr(C)]
pub struct Stmt {
    // Rc::into_raw from core::Connection
    pub _conn: *mut c_void,
    // Rc::into_raw from core::Statement
    pub _ctx: *mut c_void,
    pub _bind_args_fn: BindArgsFn,
    pub _step: StmtStepFn,
    pub _get_row_values: StmtGetRowValuesFn,
    pub _get_column_names: GetColumnNamesFn,
    pub _free_current_row: FreeCurrentRowFn,
    pub _close: CloseStmtFn,
    pub current_row: *mut Value,
    pub current_row_len: i32,
}

impl Stmt {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        conn: *mut c_void,
        ctx: *mut c_void,
        bind: BindArgsFn,
        step: StmtStepFn,
        rows: StmtGetRowValuesFn,
        names: GetColumnNamesFn,
        free_row: FreeCurrentRowFn,
        close: CloseStmtFn,
    ) -> Self {
        Stmt {
            _conn: conn,
            _ctx: ctx,
            _bind_args_fn: bind,
            _step: step,
            _get_row_values: rows,
            _get_column_names: names,
            _free_current_row: free_row,
            _close: close,
            current_row: std::ptr::null_mut(),
            current_row_len: -1,
        }
    }

    pub fn close(&self) {
        unsafe { (self._close)(self as *const Stmt as *mut Stmt) };
    }

    /// # Safety
    /// Derefs a null ptr, does a null check first
    pub unsafe fn from_ptr(ptr: *mut Stmt) -> ExtResult<&'static mut Self> {
        if ptr.is_null() {
            return Err(ResultCode::Error);
        }
        Ok(unsafe { &mut *(ptr) })
    }

    pub fn to_ptr(&self) -> *const Stmt {
        self
    }

    fn bind_args(&self, idx: NonZeroUsize, arg: *const Value) {
        unsafe { (self._bind_args_fn)(self.to_ptr() as *mut Stmt, idx.get() as i32, arg) };
    }

    fn step(&self) -> StepResult {
        match unsafe { (self._step)(self.to_ptr() as *mut Stmt) } {
            ResultCode::Row => StepResult::Row,
            ResultCode::EOF => StepResult::Done,
            ResultCode::Busy => StepResult::Busy,
            ResultCode::Interrupt => StepResult::Interrupt,
            _ => StepResult::Error,
        }
    }

    /// # Safety
    /// Checks if the current row is null before attempting to free
    pub unsafe fn free_current_row(&mut self) {
        if self.current_row.is_null() {
            return;
        }
        if self.current_row_len <= 0 {
            return;
        }
        // free from the core side so we don't have to expose `__free_internal_type`
        (self._free_current_row)(self.to_ptr() as *mut Stmt);
        self.current_row = std::ptr::null_mut();
        self.current_row_len = -1;
    }

    pub fn get_row(&self) -> &[Value] {
        unsafe { (self._get_row_values)(self.to_ptr() as *mut Stmt) };
        if self.current_row.is_null() {
            return &[];
        }
        if self.current_row_len <= 0 {
            return &[];
        }
        let col_count = self.current_row_len;
        unsafe { std::slice::from_raw_parts(self.current_row, col_count as usize) }
    }

    pub fn get_column_names(&self) -> Vec<String> {
        let mut count_value: i32 = 0;
        let count: *mut i32 = &mut count_value;
        let col_names = unsafe { (self._get_column_names)(self.to_ptr() as *mut Stmt, count) };
        if col_names.is_null() || count_value == 0 {
            return Vec::new();
        }
        let mut names = Vec::new();
        let slice = unsafe { std::slice::from_raw_parts(col_names, count_value as usize) };
        for x in slice {
            let name = unsafe { CStr::from_ptr(*x) };
            names.push(name.to_str().unwrap().to_string());
        }
        unsafe { free_column_names(col_names, count_value) };
        names
    }
}

pub unsafe fn free_column_names(names: *mut *mut c_char, count: i32) {
    if names.is_null() {
        return;
    }
    let slice = std::slice::from_raw_parts_mut(names, count as usize);

    for name in slice {
        if !name.is_null() {
            let _ = CString::from_raw(*name);
        }
    }
    let _ = Box::from_raw(names);
}
