use crate::{types::StepResult, ExtResult, ResultCode, Value};
use std::{
    ffi::{c_char, c_void, CStr, CString},
    num::NonZeroUsize,
    sync::Arc,
};

pub type RegisterModuleFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    name: *const c_char,
    module: VTabModuleImpl,
    kind: VTabKind,
) -> ResultCode;

#[repr(C)]
#[derive(Clone, Debug)]
pub struct VTabModuleImpl {
    pub name: *const c_char,
    pub create: VtabFnCreate,
    pub open: VtabFnOpen,
    pub close: VtabFnClose,
    pub filter: VtabFnFilter,
    pub column: VtabFnColumn,
    pub next: VtabFnNext,
    pub eof: VtabFnEof,
    pub update: VtabFnUpdate,
    pub rowid: VtabRowIDFn,
    pub destroy: VtabFnDestroy,
    pub best_idx: BestIdxFn,
}

#[repr(C)]
pub struct VTabCreateResult {
    pub code: ResultCode,
    pub schema: *const c_char,
    pub table: *const c_void,
}

#[cfg(feature = "core_only")]
impl VTabModuleImpl {
    pub fn create(&self, args: Vec<Value>) -> crate::ExtResult<(String, *const c_void)> {
        let result = unsafe { (self.create)(args.as_ptr(), args.len() as i32) };
        for arg in args {
            unsafe { arg.__free_internal_type() };
        }
        if !result.code.is_ok() {
            return Err(result.code);
        }
        let schema = unsafe { std::ffi::CString::from_raw(result.schema as *mut _) };
        Ok((schema.to_string_lossy().to_string(), result.table))
    }

    // TODO: This function is temporary and should eventually be removed.
    //       The only difference from `create` is that it takes ownership of the table instance.
    //       Currently, it is used to generate virtual table column names that are stored in
    //       `sqlite_schema` alongside the table's schema.
    //       However, storing column names is not necessary to match SQLite's behavior.
    //       SQLite computes the list of columns dynamically each time the `.schema` command
    //       is executed, using the `shell_add_schema` UDF function.
    pub fn create_schema(&self, args: Vec<Value>) -> crate::ExtResult<String> {
        self.create(args).and_then(|(schema, table)| {
            // Drop the allocated table instance to avoid a memory leak.
            let result = unsafe { (self.destroy)(table) };
            if result.is_ok() {
                Ok(schema)
            } else {
                Err(result)
            }
        })
    }
}

pub type VtabFnCreate = unsafe extern "C" fn(args: *const Value, argc: i32) -> VTabCreateResult;

pub type VtabFnOpen = unsafe extern "C" fn(table: *const c_void, conn: *mut Conn) -> *const c_void;

pub type VtabFnClose = unsafe extern "C" fn(cursor: *const c_void) -> ResultCode;

pub type VtabFnFilter = unsafe extern "C" fn(
    cursor: *const c_void,
    argc: i32,
    argv: *const Value,
    idx_str: *const c_char,
    idx_num: i32,
) -> ResultCode;

pub type VtabFnColumn = unsafe extern "C" fn(cursor: *const c_void, idx: u32) -> Value;

pub type VtabFnNext = unsafe extern "C" fn(cursor: *const c_void) -> ResultCode;

pub type VtabFnEof = unsafe extern "C" fn(cursor: *const c_void) -> bool;

pub type VtabRowIDFn = unsafe extern "C" fn(cursor: *const c_void) -> i64;

pub type VtabFnUpdate = unsafe extern "C" fn(
    table: *const c_void,
    argc: i32,
    argv: *const Value,
    p_out_rowid: *mut i64,
) -> ResultCode;

pub type VtabFnDestroy = unsafe extern "C" fn(table: *const c_void) -> ResultCode;

pub type BestIdxFn = unsafe extern "C" fn(
    constraints: *const ConstraintInfo,
    constraint_len: i32,
    order_by: *const OrderByInfo,
    order_by_len: i32,
) -> ExtIndexInfo;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum VTabKind {
    VirtualTable,
    TableValuedFunction,
}

pub trait VTabModule: 'static {
    type Table: VTable;
    const VTAB_KIND: VTabKind;
    const NAME: &'static str;

    /// Creates a new instance of a virtual table.
    /// Returns a tuple where the first element is the table's schema.
    fn create(args: &[Value]) -> Result<(String, Self::Table), ResultCode>;
}

pub trait VTable {
    type Cursor: VTabCursor<Error = Self::Error>;
    type Error: std::fmt::Display;

    /// 'conn' is an Option to allow for testing. Otherwise a valid connection to the core database
    /// that created the virtual table will be available to use in your extension here.
    fn open(&self, _conn: Option<Arc<Connection>>) -> Result<Self::Cursor, Self::Error>;
    fn update(&mut self, _rowid: i64, _args: &[Value]) -> Result<(), Self::Error> {
        Ok(())
    }
    fn insert(&mut self, _args: &[Value]) -> Result<i64, Self::Error> {
        Ok(0)
    }
    fn delete(&mut self, _rowid: i64) -> Result<(), Self::Error> {
        Ok(())
    }
    fn destroy(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn best_index(_constraints: &[ConstraintInfo], _order_by: &[OrderByInfo]) -> IndexInfo {
        IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1_000_000.0,
            estimated_rows: u32::MAX,
            constraint_usages: _constraints
                .iter()
                .map(|_| ConstraintUsage {
                    argv_index: Some(0),
                    omit: false,
                })
                .collect(),
        }
    }
}

pub trait VTabCursor: Sized {
    type Error: std::fmt::Display;
    fn filter(&mut self, args: &[Value], idx_info: Option<(&str, i32)>) -> ResultCode;
    fn rowid(&self) -> i64;
    fn column(&self, idx: u32) -> Result<Value, Self::Error>;
    fn eof(&self) -> bool;
    fn next(&mut self) -> ResultCode;
    fn close(&self) -> ResultCode {
        ResultCode::OK
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ConstraintOp {
    Eq = 2,
    Lt = 4,
    Le = 8,
    Gt = 16,
    Ge = 32,
    Match = 64,
    Like = 65,
    Glob = 66,
    Regexp = 67,
    Ne = 68,
    IsNot = 69,
    IsNotNull = 70,
    IsNull = 71,
    Is = 72,
    In = 73,
}

#[repr(C)]
#[derive(Copy, Clone)]
/// Describes an ORDER BY clause in a query involving a virtual table.
/// Passed along with the constraints to xBestIndex.
pub struct OrderByInfo {
    /// The index of the column referenced in the ORDER BY clause.
    pub column_index: u32,
    /// Whether or not the clause is in descending order.
    pub desc: bool,
}

/// The internal (core) representation of an 'index' on a virtual table.
/// Returned from xBestIndex and then processed and passed to VFilter.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// The index number, used to identify the index internally by the VTab
    pub idx_num: i32,
    /// Optional index name. these are passed to vfilter in a tuple (idx_num, idx_str)
    pub idx_str: Option<String>,
    /// Whether the index is used for order by
    pub order_by_consumed: bool,
    /// TODO: for eventual cost based query planning
    pub estimated_cost: f64,
    /// Estimated number of rows that the query will return
    pub estimated_rows: u32,
    /// List of constraints that can be used to optimize the query.
    pub constraint_usages: Vec<ConstraintUsage>,
}
impl Default for IndexInfo {
    fn default() -> Self {
        Self {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1_000_000.0,
            estimated_rows: u32::MAX,
            constraint_usages: Vec::new(),
        }
    }
}

impl IndexInfo {
    ///
    /// Converts IndexInfo to an FFI-safe `ExtIndexInfo`.
    /// This method transfers ownership of `constraint_usages` and `idx_str`,
    /// which must later be reclaimed using `from_ffi` to prevent leaks.
    pub fn to_ffi(self) -> ExtIndexInfo {
        let len = self.constraint_usages.len();
        let ptr = Box::into_raw(self.constraint_usages.into_boxed_slice()) as *mut ConstraintUsage;
        let idx_str_len = self.idx_str.as_ref().map(|s| s.len()).unwrap_or(0);
        let c_idx_str = self
            .idx_str
            .map(|s| std::ffi::CString::new(s).unwrap().into_raw())
            .unwrap_or(std::ptr::null_mut());
        ExtIndexInfo {
            idx_num: self.idx_num,
            estimated_cost: self.estimated_cost,
            estimated_rows: self.estimated_rows,
            order_by_consumed: self.order_by_consumed,
            constraint_usages_ptr: ptr,
            constraint_usage_len: len,
            idx_str: c_idx_str as *mut _,
            idx_str_len,
        }
    }

    /// Reclaims ownership of `constraint_usages` and `idx_str` from an FFI-safe `ExtIndexInfo`.
    /// # Safety
    /// This method is unsafe because it can cause memory leaks if not used correctly.
    /// to_ffi and from_ffi are meant to send index info across ffi bounds then immediately reclaim it.
    pub unsafe fn from_ffi(ffi: ExtIndexInfo) -> Self {
        let constraint_usages = unsafe {
            Box::from_raw(std::slice::from_raw_parts_mut(
                ffi.constraint_usages_ptr,
                ffi.constraint_usage_len,
            ))
            .to_vec()
        };
        let idx_str = if ffi.idx_str.is_null() {
            None
        } else {
            Some(unsafe {
                std::ffi::CString::from_raw(ffi.idx_str as *mut _)
                    .to_string_lossy()
                    .into_owned()
            })
        };
        Self {
            idx_num: ffi.idx_num,
            idx_str,
            order_by_consumed: ffi.order_by_consumed,
            estimated_cost: ffi.estimated_cost,
            estimated_rows: ffi.estimated_rows,
            constraint_usages,
        }
    }
}

#[repr(C)]
#[derive(Clone, Debug)]
/// FFI representation of IndexInfo.
pub struct ExtIndexInfo {
    pub idx_num: i32,
    pub idx_str: *const u8,
    pub idx_str_len: usize,
    pub order_by_consumed: bool,
    pub estimated_cost: f64,
    pub estimated_rows: u32,
    pub constraint_usages_ptr: *mut ConstraintUsage,
    pub constraint_usage_len: usize,
}

/// Returned from xBestIndex to describe how the virtual table
/// can use the constraints in the WHERE clause of a query.
#[derive(Debug, Clone, Copy)]
pub struct ConstraintUsage {
    /// 1 based index of the argument passed
    pub argv_index: Option<u32>,
    /// If true, core can omit this constraint in the vdbe layer.
    pub omit: bool,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
/// The primary argument to xBestIndex, which describes a constraint
/// in a query involving a virtual table.
pub struct ConstraintInfo {
    /// The index of the column referenced in the WHERE clause.
    pub column_index: u32,
    /// The operator used in the clause.
    pub op: ConstraintOp,
    /// Whether or not constraint is garaunteed to be enforced.
    pub usable: bool,
    /// packed integer with the index of the constraint in the planner,
    /// and the side of the binary expr that the relevant column is on.
    pub plan_info: u32,
}

impl ConstraintInfo {
    #[inline(always)]
    pub fn pack_plan_info(pred_idx: u32, is_right_side: bool) -> u32 {
        ((pred_idx) << 1) | (is_right_side as u32)
    }
    #[inline(always)]
    pub fn unpack_plan_info(&self) -> (usize, bool) {
        ((self.plan_info >> 1) as usize, (self.plan_info & 1) != 0)
    }
}

pub type PrepareStmtFn = unsafe extern "C" fn(api: *mut Conn, sql: *const c_char) -> *mut Stmt;
pub type ExecuteFn = unsafe extern "C" fn(
    ctx: *mut Conn,
    sql: *const c_char,
    args: *mut Value,
    arg_count: i32,
    last_insert_rowid: *mut i64,
) -> ResultCode;
pub type GetColumnNamesFn =
    unsafe extern "C" fn(ctx: *mut Stmt, count: *mut i32) -> *mut *mut c_char;
pub type BindArgsFn = unsafe extern "C" fn(ctx: *mut Stmt, idx: i32, arg: Value) -> ResultCode;
pub type StmtStepFn = unsafe extern "C" fn(ctx: *mut Stmt) -> ResultCode;
pub type StmtGetRowValuesFn = unsafe extern "C" fn(ctx: *mut Stmt);
pub type FreeCurrentRowFn = unsafe extern "C" fn(ctx: *mut Stmt);
pub type CloseConnectionFn = unsafe extern "C" fn(ctx: *mut c_void);
pub type CloseStmtFn = unsafe extern "C" fn(ctx: *mut Stmt);

/// core database connection
/// public fields for core only
#[repr(C)]
#[derive(Debug, Clone)]
pub struct Conn {
    // boxed Rc::Weak from core::Connection
    pub _ctx: *mut c_void,
    pub _prepare_stmt: PrepareStmtFn,
    pub _execute: ExecuteFn,
    pub _close: CloseConnectionFn,
}

impl Conn {
    pub fn new(
        ctx: *mut c_void,
        prepare_stmt: PrepareStmtFn,
        exec_fn: ExecuteFn,
        close: CloseConnectionFn,
    ) -> Self {
        Conn {
            _ctx: ctx,
            _prepare_stmt: prepare_stmt,
            _execute: exec_fn,
            _close: close,
        }
    }

    /// # Safety
    /// Dereferences a null pointer with a null check
    pub unsafe fn from_ptr(ptr: *mut Conn) -> crate::ExtResult<&'static mut Self> {
        if ptr.is_null() {
            return Err(ResultCode::Error);
        }
        Ok(unsafe { &mut *(ptr) })
    }

    pub fn close(&mut self) {
        if self._ctx.is_null() {
            return;
        }
        unsafe { (self._close)(self._ctx) };
        self._ctx = std::ptr::null_mut();
    }

    /// execute a SQL statement with the given arguments.
    /// optionally returns the last inserted rowid for the query
    pub fn execute(&self, sql: &str, args: &[Value]) -> crate::ExtResult<Option<usize>> {
        let Ok(sql) = CString::new(sql) else {
            return Err(ResultCode::Error);
        };
        let arg_count = args.len() as i32;
        let args = args.as_ptr();
        let last_insert_rowid = 0;
        if let ResultCode::OK = unsafe {
            (self._execute)(
                self as *const _ as *mut Conn,
                sql.as_ptr(),
                args as *mut Value,
                arg_count,
                &last_insert_rowid as *const _ as *mut i64,
            )
        } {
            return Ok(Some(last_insert_rowid as usize));
        }
        Err(ResultCode::Error)
    }

    pub fn prepare_stmt(&self, sql: &str) -> *mut Stmt {
        let Ok(sql) = CString::new(sql) else {
            return std::ptr::null_mut();
        };
        unsafe { (self._prepare_stmt)(self as *const _ as *mut Conn, sql.as_ptr()) }
    }
}

/// Prepared statement for querying a core database connection public API for extensions
/// Statements can be manually closed.
#[derive(Debug)]
#[repr(transparent)]
pub struct Statement(*mut Stmt);

impl Drop for Statement {
    fn drop(&mut self) {
        if self.0.is_null() {
            return;
        }
        unsafe { (*self.0).close() }
    }
}

/// Public API for methods to allow extensions to query other tables for
/// the connection that opened the VTable. This value and its resources are cleaned up when
/// the VTable is dropped, so there is no need to manually close the connection.
#[derive(Debug)]
#[repr(transparent)]
pub struct Connection(*mut Conn);

impl Connection {
    pub fn new(ctx: *mut Conn) -> Self {
        Connection(ctx)
    }

    /// From the included SQL string, prepare a statement for execution.
    pub fn prepare(self: &Arc<Self>, sql: &str) -> ExtResult<Statement> {
        let stmt = unsafe { (*self.0).prepare_stmt(sql) };
        if stmt.is_null() {
            return Err(ResultCode::Error);
        }
        Ok(Statement(stmt))
    }

    /// Execute a SQL statement with the given arguments.
    /// Optionally returns the last inserted rowid for the query.
    pub fn execute(self: &Arc<Self>, sql: &str, args: &[Value]) -> crate::ExtResult<Option<usize>> {
        if self.0.is_null() {
            return Err(ResultCode::Error);
        }
        unsafe { (*self.0).execute(sql, args) }
    }
}

impl Statement {
    /// Bind a value to a parameter in the prepared statement.
    ///```ignore
    /// let stmt = conn.prepare_stmt("select * from users where name = ?");
    /// stmt.bind_at(1, Value::from_text("test".into()));
    ///```
    pub fn bind_at(&self, idx: NonZeroUsize, arg: Value) {
        unsafe {
            (*self.0).bind_args(idx, arg);
        }
    }

    /// Execute the statement and return the next row
    ///```ignore
    /// while stmt.step() == StepResult::Row {
    ///     let row = stmt.get_row();
    ///     println!("row: {:?}", row);
    /// }
    /// ```
    pub fn step(&self) -> StepResult {
        unsafe { (*self.0).step() }
    }

    // Get the current row values
    ///```ignore
    /// while stmt.step() == StepResult::Row {
    ///    let row = stmt.get_row();
    ///    println!("row: {:?}", row);
    ///```
    pub fn get_row(&mut self) -> &[Value] {
        unsafe { (*self.0).get_row() }
    }

    /// Get the result column names for the prepared statement
    pub fn get_column_names(&self) -> Vec<String> {
        unsafe { (*self.0).get_column_names() }
    }

    /// Close the statement and clean up resources.
    pub fn close(self) {
        if self.0.is_null() {
            return;
        }
        unsafe { (*self.0).close() }
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

    /// Close the statement
    pub fn close(&mut self) {
        // null check to prevent double free
        if self._ctx.is_null() {
            return;
        }
        unsafe { (self._close)(self as *const Stmt as *mut Stmt) };
        self._ctx = std::ptr::null_mut();
    }

    /// # Safety
    /// Derefs a null ptr, does a null check first
    pub unsafe fn from_ptr(ptr: *mut Stmt) -> ExtResult<&'static mut Self> {
        if ptr.is_null() {
            return Err(ResultCode::Error);
        }
        Ok(unsafe { &mut *(ptr) })
    }

    /// Returns the pointer to the statement.
    pub fn to_ptr(&self) -> *mut Stmt {
        self as *const Stmt as *mut Stmt
    }

    /// Bind a value to a parameter in the prepared statement
    /// Own the value so it can be freed in core
    fn bind_args(&self, idx: NonZeroUsize, arg: Value) {
        unsafe {
            (self._bind_args_fn)(self.to_ptr(), idx.get() as i32, arg);
        };
    }

    /// Execute the statement to attempt to retrieve the next result row.
    fn step(&self) -> StepResult {
        unsafe { (self._step)(self.to_ptr()) }.into()
    }

    /// Free the memory for the values obtained from the `get_row` method.
    /// This is easier done on core side because __free_internal_type is 'core_only'
    /// feature to prevent extensions causing memory issues.
    /// # Safety
    /// This fn is unsafe because it derefs a raw pointer after null and
    /// length checks. This fn should only be called with the pointer returned from get_row.
    pub unsafe fn free_current_row(&mut self) {
        if self.current_row.is_null() || self.current_row_len <= 0 {
            return;
        }
        // free from the core side so we don't have to expose `__free_internal_type`
        (self._free_current_row)(self.to_ptr());
        self.current_row = std::ptr::null_mut();
        self.current_row_len = -1;
    }

    /// Returns the values from the current row in the prepared statement, should
    /// be called after the step() method returns `StepResult::Row`
    pub fn get_row(&self) -> &[Value] {
        unsafe { (self._get_row_values)(self.to_ptr()) };
        if self.current_row.is_null() || self.current_row_len < 1 {
            return &[];
        }
        let col_count = self.current_row_len;
        unsafe { std::slice::from_raw_parts(self.current_row, col_count as usize) }
    }

    /// Returns the names of the result columns for the prepared statement.
    pub fn get_column_names(&self) -> Vec<String> {
        let mut count_value: i32 = 0;
        let count: *mut i32 = &mut count_value;
        let col_names = unsafe { (self._get_column_names)(self.to_ptr(), count) };
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

/// Free the column names returned from get_column_names
/// # Safety
/// This function is unsafe because it derefs a raw pointer, this fn
/// should only be called with the pointer returned from get_column_names
/// only when they will no longer be used.
pub unsafe fn free_column_names(names: *mut *mut c_char, count: i32) {
    if names.is_null() || count < 1 {
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
