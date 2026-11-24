use crate::alloc::{TryClone, TursoAllocExt, TursoTryWithCapacityExt, TursoVecExt};
use crate::ast;
use crate::pragma::{PragmaVirtualTable, PragmaVirtualTableCursor};
use crate::schema::{Column, FromDefinitionFlags};
use crate::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use crate::sync::{Arc, RwLock, Weak};
use crate::types::IOResultOr;
use crate::vdbe::Register;
use crate::{Connection, IOResult, LimboError, SymbolTable, Value};
use std::ffi::c_void;
use std::ptr::NonNull;
use turso_ext::{ConstraintInfo, IndexInfo, OrderByInfo, ResultCode, VTabKind, VTabModuleImpl};

#[derive(Debug, Clone)]
pub(crate) enum VirtualTableType {
    Pragma(PragmaVirtualTable),
    External(crate::ext::ExtensionTable),
    Internal(Arc<RwLock<dyn InternalVirtualTable>>),
}

#[derive(Clone, Debug)]
pub struct VirtualTable {
    pub(crate) name: String,
    pub columns: Vec<Column>,
    pub(crate) kind: VTabKind,
    pub(crate) vtab_type: VirtualTableType,
    // identifier to tie a cursor to a specific instantiated virtual table instance
    pub(crate) vtab_id: u64,
    /// Whether `DROP TABLE` may remove this table from its schema.
    pub(crate) is_droppable: bool,
    // Whether this virtual table is safe to use from within triggers and views.
    // Corresponds to SQLite's SQLITE_VTAB_INNOCUOUS flag.
    pub innocuous: bool,
}

impl VirtualTable {
    pub(crate) fn id(&self) -> u64 {
        self.vtab_id
    }
    pub(crate) fn readonly(&self) -> bool {
        match &self.vtab_type {
            VirtualTableType::Pragma(_) => true,
            VirtualTableType::External(table) => table.readonly(),
            VirtualTableType::Internal(_) => true,
        }
    }

    /// Wrap an `InternalVirtualTable` implementation so it can appear in a
    /// `Schema`'s catalog. The table's `name()` becomes the catalog name and
    /// its `columns()` give the column metadata.
    pub(crate) fn wrap_internal_table<T>(table: T) -> crate::Result<Arc<VirtualTable>>
    where
        T: InternalVirtualTable + 'static,
    {
        let name = table.name();
        let columns = table.columns();
        Ok(Arc::new(Self::new_internal(
            name,
            &columns,
            VTabKind::TableValuedFunction,
            Arc::new(RwLock::new(table)),
        )?))
    }

    /// Build a virtual table around an [`InternalVirtualTable`]
    /// implementation. Schema dialects use this to construct catalog tables
    /// (e.g. a `pg_catalog` surface) with a non-table-valued-function kind
    /// and install them through [`crate::dialect::Dialect::schema_builder`].
    pub fn new_internal(
        name: String,
        columns: &[ast::ColumnDefinition],
        kind: VTabKind,
        table: Arc<RwLock<dyn InternalVirtualTable>>,
    ) -> crate::Result<Self> {
        Ok(VirtualTable {
            name,
            columns: Self::columns_from_definitions(columns)?,
            kind,
            vtab_type: VirtualTableType::Internal(table),
            vtab_id: 0,
            is_droppable: false,
            innocuous: true,
        })
    }

    pub(crate) fn function(
        dialect: &dyn crate::dialect::Dialect,
        name: &str,
        syms: &SymbolTable,
    ) -> crate::Result<Arc<VirtualTable>> {
        let module = syms.resolve_vtab_module(name);
        let (vtab_type, schema) = if module.is_some() {
            crate::ext::create_virtual_table(
                name,
                module,
                Vec::new(),
                VTabKind::TableValuedFunction,
            )?
        } else {
            return Err(LimboError::ParseError(format!(
                "No such table-valued function: {name}"
            )));
        };

        let vtab = VirtualTable {
            name: name.to_owned(),
            columns: Self::resolve_columns(dialect, schema)?,
            kind: VTabKind::TableValuedFunction,
            vtab_type,
            vtab_id: 0,
            is_droppable: false,
            innocuous: module.is_some_and(|module| module.implementation.innocuous()),
        };
        Ok(Arc::new(vtab))
    }

    pub fn table(
        dialect: &dyn crate::dialect::Dialect,
        tbl_name: Option<&str>,
        module_name: &str,
        args: Vec<Value>,
        syms: &SymbolTable,
    ) -> crate::Result<Arc<VirtualTable>> {
        let module = syms.resolve_vtab_module(module_name);
        let (vtab_type, schema) =
            crate::ext::create_virtual_table(module_name, module, args, VTabKind::VirtualTable)?;
        let vtab = VirtualTable {
            name: tbl_name.unwrap_or(module_name).to_owned(),
            columns: Self::resolve_columns(dialect, schema)?,
            kind: VTabKind::VirtualTable,
            vtab_type,
            vtab_id: VTAB_ID_COUNTER.fetch_add(1, Ordering::Acquire),
            is_droppable: true,
            innocuous: module.is_some_and(|module| module.implementation.innocuous()),
        };
        Ok(Arc::new(vtab))
    }

    pub(crate) fn resolve_columns(
        dialect: &(impl crate::dialect::Dialect + ?Sized),
        schema: String,
    ) -> crate::Result<Vec<Column>> {
        if let ast::Cmd::Stmt(ast::Stmt::CreateTable {
            body: ast::CreateTableBody::ColumnsAndConstraints { columns, .. },
            ..
        }) = crate::dialect::parse_statement(dialect, &schema)?.ok_or_else(|| {
            LimboError::ParseError("Failed to parse schema from virtual table module".to_string())
        })? {
            Self::columns_from_definitions(&columns)
        } else {
            Err(LimboError::ParseError(
                "Failed to parse schema from virtual table module".to_string(),
            ))
        }
    }

    pub(crate) fn columns_from_definitions(
        definitions: &[ast::ColumnDefinition],
    ) -> crate::Result<Vec<Column>> {
        let mut columns = definitions
            .iter()
            .map(|definition| Column::from_definition(definition, FromDefinitionFlags::empty()))
            .collect::<crate::Result<Vec<Column>>>()?;
        // Like SQLite, HIDDEN marks the column and is not part of its declared type.
        for column in columns.iter_mut().filter(|column| column.hidden()) {
            column.ty_str = column
                .ty_str
                .split(' ')
                .filter(|word| !word.eq_ignore_ascii_case("HIDDEN"))
                .collect::<Vec<_>>()
                .join(" ");
        }
        Ok(columns)
    }

    pub fn open(&self, conn: Arc<Connection>) -> crate::Result<VirtualTableCursor> {
        match &self.vtab_type {
            VirtualTableType::Pragma(table) => {
                Ok(VirtualTableCursor::new_pragma(table.open(conn)?))
            }
            VirtualTableType::External(table) => Ok(VirtualTableCursor::new_external(
                table.open(conn, self.vtab_id)?,
            )),
            VirtualTableType::Internal(table) => {
                Ok(VirtualTableCursor::new_internal(table.read().open(conn)?))
            }
        }
    }

    pub(crate) fn update(
        &self,
        args: &[Value],
        state: &mut crate::native_ext::ExtensionState,
    ) -> IOResultOr<Option<i64>> {
        match &self.vtab_type {
            VirtualTableType::Pragma(_) => Err(LimboError::ReadOnly.into()),
            VirtualTableType::External(table) => table.update(args, state),
            VirtualTableType::Internal(_) => Err(LimboError::ReadOnly.into()),
        }
    }

    pub fn destroy(&self) -> crate::Result<()> {
        match &self.vtab_type {
            VirtualTableType::Pragma(_) => Ok(()),
            VirtualTableType::External(table) => table.destroy(),
            VirtualTableType::Internal(_) => Ok(()),
        }
    }

    pub(crate) fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        match &self.vtab_type {
            VirtualTableType::Pragma(table) => table.best_index(constraints),
            VirtualTableType::External(table) => table.best_index(constraints, order_by),
            VirtualTableType::Internal(table) => table.read().best_index(constraints, order_by),
        }
    }

    pub(crate) fn begin(&self) -> crate::Result<()> {
        match &self.vtab_type {
            VirtualTableType::Pragma(_) => Err(LimboError::ExtensionError(
                "Pragma virtual tables do not support transactions".to_string(),
            )),
            VirtualTableType::External(table) => table.begin(),
            VirtualTableType::Internal(_) => Err(LimboError::ExtensionError(
                "Internal virtual tables currently do not support transactions".to_string(),
            )),
        }
    }

    pub(crate) fn commit(&self) -> crate::Result<()> {
        match &self.vtab_type {
            VirtualTableType::Pragma(_) => Err(LimboError::ExtensionError(
                "Pragma virtual tables do not support transactions".to_string(),
            )),
            VirtualTableType::External(table) => table.commit(),
            VirtualTableType::Internal(_) => Err(LimboError::ExtensionError(
                "Internal virtual tables currently do not support transactions".to_string(),
            )),
        }
    }

    pub(crate) fn rollback(&self) -> crate::Result<()> {
        match &self.vtab_type {
            VirtualTableType::Pragma(_) => Err(LimboError::ExtensionError(
                "Pragma virtual tables do not support transactions".to_string(),
            )),
            VirtualTableType::External(table) => table.rollback(),
            VirtualTableType::Internal(_) => Err(LimboError::ExtensionError(
                "Internal virtual tables currently do not support transactions".to_string(),
            )),
        }
    }

    pub(crate) fn rename(&self, new_name: &str) -> crate::Result<()> {
        match &self.vtab_type {
            VirtualTableType::Pragma(_) => Err(LimboError::ExtensionError(
                "Pragma virtual tables do not support renaming".to_string(),
            )),
            VirtualTableType::External(table) => table.rename(new_name),
            VirtualTableType::Internal(_) => Err(LimboError::ExtensionError(
                "Internal virtual tables currently do not support renaming".to_string(),
            )),
        }
    }
}

enum VirtualTableCursorInner {
    Pragma(Box<PragmaVirtualTableCursor>),
    External(crate::ext::ExtensionCursor),
    Internal(Arc<RwLock<dyn InternalVirtualTableCursor>>),
}

pub struct VirtualTableCursor {
    inner: VirtualTableCursorInner,
    null_flag: bool,
    /// The vector that [Self::filter] lends the argument values to an internal
    /// cursor in. The cursor keeps it, so that a filter for each row of an
    /// outer loop does not allocate.
    filter_args: crate::alloc::Vec<Value>,
}

crate::assert::assert_send_sync!(VirtualTableCursor);

impl VirtualTableCursor {
    pub(crate) fn new_pragma(cursor: PragmaVirtualTableCursor) -> Self {
        Self {
            inner: VirtualTableCursorInner::Pragma(Box::new(cursor)),
            null_flag: false,
            filter_args: TursoAllocExt::new(),
        }
    }

    pub(crate) fn new_external(cursor: crate::ext::ExtensionCursor) -> Self {
        Self {
            inner: VirtualTableCursorInner::External(cursor),
            null_flag: false,
            filter_args: TursoAllocExt::new(),
        }
    }

    pub(crate) fn new_internal(cursor: Arc<RwLock<dyn InternalVirtualTableCursor>>) -> Self {
        Self {
            inner: VirtualTableCursorInner::Internal(cursor),
            null_flag: false,
            filter_args: TursoAllocExt::new(),
        }
    }

    pub(crate) fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    pub(crate) fn get_null_flag(&self) -> bool {
        self.null_flag
    }

    /// Whether this is the cursor of an internal virtual table, whose filter
    /// starts a new scan each time.
    pub(crate) fn is_internal(&self) -> bool {
        matches!(&self.inner, VirtualTableCursorInner::Internal(_))
    }

    pub(crate) fn needs_close_at_done(&self) -> bool {
        matches!(&self.inner, VirtualTableCursorInner::External(cursor) if cursor.needs_close_at_done())
    }

    pub(crate) fn next(&mut self) -> IOResultOr<bool> {
        self.null_flag = false;
        let result = match &mut self.inner {
            VirtualTableCursorInner::Pragma(cursor) => return cursor.next().map_err(Into::into),
            VirtualTableCursorInner::External(cursor) => return cursor.next(),
            VirtualTableCursorInner::Internal(cursor) => cursor.write().next(),
        };
        result.map(IOResult::Done).map_err(Into::into)
    }

    pub(crate) fn rowid(&self) -> Option<i64> {
        if self.null_flag {
            return None;
        }
        Some(match &self.inner {
            VirtualTableCursorInner::Pragma(cursor) => cursor.rowid(),
            VirtualTableCursorInner::External(cursor) => cursor.rowid(),
            VirtualTableCursorInner::Internal(cursor) => cursor.read().rowid(),
        })
    }

    /// Writes the value of `column` into the register `dest`. An internal
    /// cursor writes it into the value of the register, with
    /// [InternalVirtualTableCursor::column_into].
    pub(crate) fn column_into(&mut self, column: usize, dest: &mut Register) -> IOResultOr<()> {
        if let (false, VirtualTableCursorInner::Internal(cursor), Register::Value(value)) =
            (self.null_flag, &self.inner, &mut *dest)
        {
            cursor.read().column_into(column, value)?;
            return Ok(IOResult::Done(()));
        }
        match self.column(column)? {
            IOResult::Done(value) => {
                dest.set_value(value);
                Ok(IOResult::Done(()))
            }
            IOResult::IO(io) => Ok(IOResult::IO(io)),
        }
    }

    pub fn column(&mut self, column: usize) -> IOResultOr<Value> {
        if self.null_flag {
            return Ok(IOResult::Done(Value::Null));
        }
        let result = match &mut self.inner {
            VirtualTableCursorInner::Pragma(cursor) => cursor.column(column),
            VirtualTableCursorInner::External(cursor) => return cursor.column(column),
            VirtualTableCursorInner::Internal(cursor) => cursor.read().column(column),
        };
        result.map(IOResult::Done).map_err(Into::into)
    }

    /// Starts a scan with the values of `arg_registers` as the arguments.
    ///
    /// An internal cursor reads its arguments only during the call, so the
    /// values move out of their registers into a vector that this cursor
    /// keeps, and they move back after the call, also after an error. The
    /// registers must get their values back, because a constant argument,
    /// such as the path of `json_each(doc, '$.a')`, is loaded once for each
    /// statement. Pragma and extension cursors keep their arguments, so they
    /// get copies.
    pub fn filter(
        &mut self,
        idx_num: i32,
        idx_str: Option<String>,
        arg_registers: &mut [Register],
    ) -> IOResultOr<bool> {
        self.filter_with_json_cache(
            idx_num,
            idx_str,
            arg_registers,
            #[cfg(feature = "json")]
            &crate::json::JsonCacheCell::new(),
        )
    }

    /// Like [Self::filter], and an internal cursor can read its document from
    /// `json_cache`, the JSON cache of the statement, with
    /// [InternalVirtualTableCursor::filter_with_json_cache].
    pub(crate) fn filter_with_json_cache(
        &mut self,
        idx_num: i32,
        idx_str: Option<String>,
        arg_registers: &mut [Register],
        #[cfg(feature = "json")] json_cache: &crate::json::JsonCacheCell,
    ) -> IOResultOr<bool> {
        self.null_flag = false;
        let result = match &mut self.inner {
            VirtualTableCursorInner::Pragma(cursor) => {
                return cursor
                    .filter(copy_values(arg_registers)?)
                    .map_err(Into::into);
            }
            VirtualTableCursorInner::External(cursor) => {
                let arg_count = arg_registers.len();
                return cursor.filter(idx_num, idx_str, arg_count, copy_values(arg_registers)?);
            }
            VirtualTableCursorInner::Internal(cursor) => {
                let args = &mut self.filter_args;
                args.clear();
                for register in arg_registers.iter_mut() {
                    let value = match register {
                        Register::Value(value) => std::mem::replace(value, Value::Null),
                        register => register.get_value().try_clone()?,
                    };
                    args.try_push(value)?;
                }
                #[cfg(feature = "json")]
                let result = cursor
                    .write()
                    .filter_with_json_cache(args, idx_str, idx_num, json_cache);
                #[cfg(not(feature = "json"))]
                let result = cursor.write().filter(args, idx_str, idx_num);
                for (register, value) in arg_registers.iter_mut().zip(args.drain(..)) {
                    if let Register::Value(slot) = register {
                        *slot = value;
                    }
                }
                result
            }
        };
        result.map(IOResult::Done).map_err(Into::into)
    }

    pub(crate) fn vtab_id(&self) -> Option<u64> {
        match &self.inner {
            VirtualTableCursorInner::Pragma(_) => None,
            VirtualTableCursorInner::External(cursor) => Some(cursor.vtab_id()),
            VirtualTableCursorInner::Internal(_) => None,
        }
    }
}

/// Copies of the values of `registers`.
fn copy_values(registers: &[Register]) -> crate::Result<crate::alloc::Vec<Value>> {
    let mut values = crate::alloc::Vec::try_with_capacity_ext(registers.len())?;
    for register in registers {
        values.try_push(register.get_value().try_clone()?)?;
    }
    Ok(values)
}

#[derive(Debug)]
pub(crate) struct ExtVirtualTable {
    implementation: Arc<VTabModuleImpl>,
    table_ptr: AtomicPtr<c_void>,
}
static VTAB_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

impl Clone for ExtVirtualTable {
    fn clone(&self) -> Self {
        Self {
            implementation: self.implementation.clone(),
            table_ptr: AtomicPtr::new(self.table_ptr.load(Ordering::SeqCst)),
        }
    }
}

impl ExtVirtualTable {
    pub(crate) fn readonly(&self) -> bool {
        self.implementation.readonly
    }
    pub(crate) fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        unsafe {
            IndexInfo::from_ffi((self.implementation.best_idx)(
                constraints.as_ptr(),
                constraints.len() as i32,
                order_by.as_ptr(),
                order_by.len() as i32,
            ))
        }
    }

    /// takes ownership of the provided Args
    pub(crate) fn create(
        implementation: Arc<VTabModuleImpl>,
        args: Vec<Value>,
    ) -> crate::Result<(Self, String)> {
        let args = args.iter().map(Value::to_ffi).collect();
        let (schema, table_ptr) = implementation.create(args)?;
        let vtab = ExtVirtualTable {
            implementation,
            table_ptr: AtomicPtr::new(table_ptr as *mut c_void),
        };
        Ok((vtab, schema))
    }

    /// Accepts a pointer connection that owns the VTable, that the module
    /// can optionally use to query the other tables.
    pub(crate) fn open(
        &self,
        conn: Arc<Connection>,
        id: u64,
    ) -> crate::Result<ExtVirtualTableCursor> {
        // we need a Weak<Connection> to upgrade and call from the extension.
        let weak = Arc::downgrade(&conn);
        let weak_box = Box::into_raw(Box::new(weak));
        let conn = turso_ext::Conn::new(
            weak_box as *mut c_void,
            crate::ext::prepare_stmt,
            crate::ext::execute,
        );
        let ext_conn_ptr = NonNull::new(Box::into_raw(Box::new(conn))).expect("null pointer");
        // store the leaked connection pointer on the table so it can be freed on drop
        let Some(cursor) = NonNull::new(unsafe {
            (self.implementation.open)(
                self.table_ptr.load(Ordering::SeqCst) as *const c_void,
                ext_conn_ptr.as_ptr(),
            ) as *mut c_void
        }) else {
            return Err(LimboError::ExtensionError("Open returned null".to_string()));
        };
        ExtVirtualTableCursor::new(cursor, ext_conn_ptr, self.implementation.clone(), id)
    }

    pub(crate) fn update(&self, args: &[Value]) -> crate::Result<Option<i64>> {
        let arg_count = args.len();
        let ext_args = args.iter().map(|arg| arg.to_ffi()).collect::<Vec<_>>();
        let newrowid = 0i64;
        let rc = unsafe {
            (self.implementation.update)(
                self.table_ptr.load(Ordering::SeqCst) as *const c_void,
                arg_count as i32,
                ext_args.as_ptr(),
                &newrowid as *const _ as *mut i64,
            )
        };
        for arg in ext_args {
            unsafe {
                arg.__free_internal_type();
            }
        }
        match rc {
            ResultCode::OK => Ok(None),
            ResultCode::RowID => Ok(Some(newrowid)),
            _ => Err(LimboError::ExtensionError(rc.to_string())),
        }
    }

    pub(crate) fn destroy(&self) -> crate::Result<()> {
        let rc = unsafe {
            (self.implementation.destroy)(self.table_ptr.load(Ordering::SeqCst) as *const c_void)
        };
        match rc {
            ResultCode::OK => Ok(()),
            _ => Err(LimboError::ExtensionError(rc.to_string())),
        }
    }

    pub(crate) fn commit(&self) -> crate::Result<()> {
        let rc = unsafe { (self.implementation.commit)(self.table_ptr.load(Ordering::SeqCst)) };
        match rc {
            ResultCode::OK => Ok(()),
            _ => Err(LimboError::ExtensionError("Commit failed".to_string())),
        }
    }

    pub(crate) fn begin(&self) -> crate::Result<()> {
        let rc = unsafe { (self.implementation.begin)(self.table_ptr.load(Ordering::SeqCst)) };
        match rc {
            ResultCode::OK => Ok(()),
            _ => Err(LimboError::ExtensionError("Begin failed".to_string())),
        }
    }

    pub(crate) fn rollback(&self) -> crate::Result<()> {
        let rc = unsafe { (self.implementation.rollback)(self.table_ptr.load(Ordering::SeqCst)) };
        match rc {
            ResultCode::OK => Ok(()),
            _ => Err(LimboError::ExtensionError("Rollback failed".to_string())),
        }
    }

    pub(crate) fn rename(&self, new_name: &str) -> crate::Result<()> {
        let c_new_name = std::ffi::CString::new(new_name).unwrap();
        let rc = unsafe {
            (self.implementation.rename)(self.table_ptr.load(Ordering::SeqCst), c_new_name.as_ptr())
        };
        match rc {
            ResultCode::OK => Ok(()),
            _ => Err(LimboError::ExtensionError("Rename failed".to_string())),
        }
    }
}

pub struct ExtVirtualTableCursor {
    cursor: NonNull<c_void>,
    // the core `[Connection]` pointer the vtab module needs to
    // query other internal tables.
    conn_ptr: Option<NonNull<turso_ext::Conn>>,
    implementation: Arc<VTabModuleImpl>,
    pub(crate) vtab_id: u64,
}

// SAFETY: Extension provider must guarantee Send + Sync on their side
// we cannot properly infer Send + Sync for dynamic libraries
unsafe impl Send for ExtVirtualTableCursor {}
unsafe impl Sync for ExtVirtualTableCursor {}
crate::assert::assert_send_sync!(ExtVirtualTableCursor);

impl ExtVirtualTableCursor {
    fn new(
        cursor: NonNull<c_void>,
        conn_ptr: NonNull<turso_ext::Conn>,
        implementation: Arc<VTabModuleImpl>,
        id: u64,
    ) -> crate::Result<Self> {
        Ok(Self {
            cursor,
            conn_ptr: Some(conn_ptr),
            implementation,
            vtab_id: id,
        })
    }

    pub(crate) fn rowid(&self) -> i64 {
        unsafe { (self.implementation.rowid)(self.cursor.as_ptr()) }
    }

    #[tracing::instrument(skip(self))]
    pub(crate) fn filter(
        &self,
        idx_num: i32,
        idx_str: Option<String>,
        arg_count: usize,
        args: crate::alloc::Vec<Value>,
    ) -> crate::Result<bool> {
        tracing::trace!("xFilter");
        let ext_args = args.iter().map(|arg| arg.to_ffi()).collect::<Vec<_>>();
        let idx_str = match idx_str {
            Some(idx_str) => Some(std::ffi::CString::new(idx_str).map_err(|e| {
                crate::LimboError::InternalError(format!("failed to convert idx_str string: {e}"))
            })?),
            None => None,
        };
        let c_idx_str_ptr = idx_str
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(std::ptr::null_mut());
        let rc = unsafe {
            (self.implementation.filter)(
                self.cursor.as_ptr(),
                arg_count as i32,
                ext_args.as_ptr(),
                c_idx_str_ptr,
                idx_num,
            )
        };
        for arg in ext_args {
            unsafe {
                arg.__free_internal_type();
            }
        }
        match rc {
            ResultCode::OK => Ok(true),
            ResultCode::EOF => Ok(false),
            _ => Err(LimboError::ExtensionError(rc.to_string())),
        }
    }

    pub(crate) fn column(&self, column: usize) -> crate::Result<Value> {
        let val = unsafe { (self.implementation.column)(self.cursor.as_ptr(), column as u32) };
        Value::from_ffi(val)
    }

    pub(crate) fn next(&self) -> crate::Result<bool> {
        let rc = unsafe { (self.implementation.next)(self.cursor.as_ptr()) };
        match rc {
            ResultCode::OK => Ok(true),
            ResultCode::EOF => Ok(false),
            _ => Err(LimboError::ExtensionError("Next failed".to_string())),
        }
    }
}

impl Drop for ExtVirtualTableCursor {
    fn drop(&mut self) {
        if let Some(ptr) = self.conn_ptr.take() {
            // first free the boxed turso_ext::Conn pointer itself
            let conn = unsafe { Box::from_raw(ptr.as_ptr()) };
            if !conn._ctx.is_null() {
                // we also leaked the Weak 'ctx' pointer, so free this as well
                let _ = unsafe { Box::from_raw(conn._ctx as *mut Weak<Connection>) };
            }
        }
        let result = unsafe { (self.implementation.close)(self.cursor.as_ptr()) };
        if !result.is_ok() {
            tracing::error!("Failed to close virtual table cursor");
        }
    }
}

pub trait InternalVirtualTable: std::fmt::Debug + Send + Sync {
    fn name(&self) -> String;
    fn open(
        &self,
        conn: Arc<Connection>,
    ) -> crate::Result<Arc<RwLock<dyn InternalVirtualTableCursor>>>;
    /// best_index is used by the optimizer. See the comment on `Table::best_index`.
    fn best_index(
        &self,
        constraints: &[turso_ext::ConstraintInfo],
        order_by: &[turso_ext::OrderByInfo],
    ) -> Result<turso_ext::IndexInfo, ResultCode>;
    /// The columns of the table, as a `CREATE TABLE` statement would define
    /// them. [`column_definition`] makes one.
    fn columns(&self) -> Vec<ast::ColumnDefinition>;
    /// Whether only a superuser may read the table. True for tables that
    /// show the database file, because they show the rows row-level
    /// security hides. There is no default, so a new table must say.
    fn superuser_only(&self) -> bool;
}

/// The definition of a column named `name` with the declared type
/// `declared_type`, as `CREATE TABLE` would parse `name declared_type`. An
/// empty `declared_type` declares no type. As in SQLite, the word `HIDDEN`
/// in the type hides the column from `SELECT *`.
pub fn column_definition(name: &str, declared_type: &str) -> ast::ColumnDefinition {
    ast::ColumnDefinition {
        col_name: ast::Name::exact(name.to_string()),
        col_type: (!declared_type.is_empty()).then(|| ast::Type {
            name: declared_type.to_string(),
            size: None,
            array_dimensions: 0,
        }),
        constraints: Vec::new(),
    }
}

pub trait InternalVirtualTableCursor: Send + Sync {
    /// next returns `Ok(true)` if there are more rows, and `Ok(false)` otherwise.
    fn next(&mut self) -> Result<bool, LimboError>;
    fn rowid(&self) -> i64;
    fn column(&self, column: usize) -> Result<Value, LimboError>;
    /// Writes the value of `column` into `value`, the value that the register
    /// of the column holds. A cursor can write a text value into the buffer of
    /// `value`, so that each row does not allocate a new string. The default
    /// replaces `value` with the value of [Self::column].
    fn column_into(&self, column: usize, value: &mut Value) -> Result<(), LimboError> {
        *value = self.column(column)?;
        Ok(())
    }
    /// Starts a new scan with `args`, and returns `Ok(true)` when the scan has
    /// a first row. VOpen keeps an open cursor when it runs again, as in a
    /// correlated subquery, so a filter must reset all state of the cursor
    /// from an earlier scan, as SQLite's xFilter does.
    fn filter(
        &mut self,
        args: &[Value],
        idx_str: Option<String>,
        idx_num: i32,
    ) -> Result<bool, LimboError>;
    /// Like [Self::filter], for a cursor that can read its document from
    /// `json_cache`, the JSON cache of the statement. The default ignores the
    /// cache.
    #[cfg(feature = "json")]
    fn filter_with_json_cache(
        &mut self,
        args: &[Value],
        idx_str: Option<String>,
        idx_num: i32,
        _json_cache: &crate::json::JsonCacheCell,
    ) -> Result<bool, LimboError> {
        self.filter(args, idx_str, idx_num)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cursor that records the arguments of each filter.
    struct RecordingCursor {
        seen_args: Vec<Vec<Value>>,
    }

    impl InternalVirtualTableCursor for RecordingCursor {
        fn next(&mut self) -> Result<bool, LimboError> {
            Ok(false)
        }

        fn rowid(&self) -> i64 {
            0
        }

        fn column(&self, _column: usize) -> Result<Value, LimboError> {
            Ok(Value::Null)
        }

        fn filter(
            &mut self,
            args: &[Value],
            _idx_str: Option<String>,
            _idx_num: i32,
        ) -> Result<bool, LimboError> {
            self.seen_args.push(args.to_vec());
            Ok(false)
        }
    }

    #[test]
    fn filter_lends_argument_values_and_gives_them_back() {
        let recording = Arc::new(RwLock::new(RecordingCursor {
            seen_args: Vec::new(),
        }));
        let mut cursor = VirtualTableCursor::new_internal(recording.clone());
        for round in 0..2 {
            let mut registers = [
                Register::Value(Value::build_text(format!("{{\"round\":{round}}}"))),
                Register::Value(Value::build_text("$.round")),
            ];
            assert!(matches!(
                cursor.filter(0, None, &mut registers).unwrap(),
                IOResult::Done(false)
            ));
            assert_eq!(
                registers[0].get_value(),
                &Value::build_text(format!("{{\"round\":{round}}}"))
            );
            assert_eq!(registers[1].get_value(), &Value::build_text("$.round"));
        }
        let seen_args = &recording.read().seen_args;
        assert_eq!(seen_args.len(), 2);
        for (round, args) in seen_args.iter().enumerate() {
            assert_eq!(
                args,
                &vec![
                    Value::build_text(format!("{{\"round\":{round}}}")),
                    Value::build_text("$.round"),
                ]
            );
        }
    }
}
