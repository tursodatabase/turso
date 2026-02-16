use crate::pragma::{PragmaVirtualTable, PragmaVirtualTableCursor};
use crate::schema::Column;
use crate::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use crate::sync::{Arc, RwLock, Weak};
use crate::util::columns_from_create_table_body;
use crate::{Connection, LimboError, SymbolTable, Value};
use std::ffi::c_void;
use std::ptr::NonNull;
use turso_ext::{ConstraintInfo, IndexInfo, OrderByInfo, ResultCode, VTabKind, VTabModuleImpl};
use turso_parser::{ast, parser::Parser};

#[derive(Debug, Clone)]
pub(crate) enum VirtualTableType {
    Pragma(PragmaVirtualTable),
    External(ExtVirtualTable),
    Internal(Arc<RwLock<dyn InternalVirtualTable>>),
}

#[derive(Clone, Debug)]
pub struct VirtualTable {
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
    pub(crate) kind: VTabKind,
    vtab_type: VirtualTableType,
    // identifier to tie a cursor to a specific instantiated virtual table instance
    vtab_id: u64,
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

    #[cfg(feature = "cli_only")]
    fn dbpage_virtual_table() -> Arc<VirtualTable> {
        let dbpage_table = crate::dbpage::DbPageTable::new();
        let dbpage_vtab = VirtualTable {
            name: dbpage_table.name(),
            columns: Self::resolve_columns(dbpage_table.sql())
                .expect("sqlite_dbpage schema resolution should not fail"),
            kind: VTabKind::TableValuedFunction,
            vtab_type: VirtualTableType::Internal(Arc::new(RwLock::new(dbpage_table))),
            vtab_id: 0,
        };
        Arc::new(dbpage_vtab)
    }

    #[cfg(feature = "cli_only")]
    fn btree_dump_virtual_table() -> Arc<VirtualTable> {
        let btree_dump_table = crate::btree_dump::BtreeDumpTable::new();
        let vtab = VirtualTable {
            name: btree_dump_table.name(),
            columns: Self::resolve_columns(btree_dump_table.sql())
                .expect("btree_dump schema resolution should not fail"),
            kind: VTabKind::TableValuedFunction,
            vtab_type: VirtualTableType::Internal(Arc::new(RwLock::new(btree_dump_table))),
            vtab_id: 0,
        };
        Arc::new(vtab)
    }

    pub(crate) fn builtin_functions() -> Vec<Arc<VirtualTable>> {
        let mut vtables: Vec<Arc<VirtualTable>> = PragmaVirtualTable::functions()
            .into_iter()
            .map(|(tab, schema)| {
                let vtab = VirtualTable {
                    name: format!("pragma_{}", tab.pragma_name),
                    columns: Self::resolve_columns(schema)
                        .expect("pragma table-valued function schema resolution should not fail"),
                    kind: VTabKind::TableValuedFunction,
                    vtab_type: VirtualTableType::Pragma(tab),
                    vtab_id: 0,
                };
                Arc::new(vtab)
            })
            .collect();

        #[cfg(feature = "json")]
        vtables.extend(Self::json_virtual_tables());

        #[cfg(feature = "cli_only")]
        vtables.push(Self::dbpage_virtual_table());

        #[cfg(feature = "cli_only")]
        vtables.push(Self::btree_dump_virtual_table());

        vtables
    }

    #[cfg(feature = "json")]
    fn json_virtual_tables() -> Vec<Arc<VirtualTable>> {
        use crate::json::vtab::JsonVirtualTable;

        let json_each = JsonVirtualTable::json_each();

        let json_each_virtual_table = VirtualTable {
            name: json_each.name(),
            columns: Self::resolve_columns(json_each.sql())
                .expect("internal table-valued function schema resolution should not fail"),
            kind: VTabKind::TableValuedFunction,
            vtab_type: VirtualTableType::Internal(Arc::new(RwLock::new(json_each))),
            vtab_id: 0,
        };

        let json_tree = JsonVirtualTable::json_tree();

        let json_tree_virtual_table = VirtualTable {
            name: json_tree.name(),
            columns: Self::resolve_columns(json_tree.sql())
                .expect("internal table-valued function schema resolution should not fail"),
            kind: VTabKind::TableValuedFunction,
            vtab_type: VirtualTableType::Internal(Arc::new(RwLock::new(json_tree))),
            vtab_id: 0,
        };

        vec![
            Arc::new(json_each_virtual_table),
            Arc::new(json_tree_virtual_table),
        ]
    }

    pub(crate) fn function(name: &str, syms: &SymbolTable) -> crate::Result<Arc<VirtualTable>> {
        let module = syms.vtab_modules.get(name);
        let (vtab_type, schema) = if module.is_some() {
            ExtVirtualTable::create(name, module, Vec::new(), VTabKind::TableValuedFunction)
                .map(|(vtab, columns)| (VirtualTableType::External(vtab), columns))?
        } else {
            return Err(LimboError::ParseError(format!(
                "No such table-valued function: {name}"
            )));
        };

        let vtab = VirtualTable {
            name: name.to_owned(),
            columns: Self::resolve_columns(schema)?,
            kind: VTabKind::TableValuedFunction,
            vtab_type,
            vtab_id: 0,
        };
        Ok(Arc::new(vtab))
    }

    pub fn table(
        tbl_name: Option<&str>,
        module_name: &str,
        args: Vec<turso_ext::Value>,
        syms: &SymbolTable,
    ) -> crate::Result<Arc<VirtualTable>> {
        let module = syms.vtab_modules.get(module_name);
        let (table, schema) =
            ExtVirtualTable::create(module_name, module, args, VTabKind::VirtualTable)?;
        let vtab = VirtualTable {
            name: tbl_name.unwrap_or(module_name).to_owned(),
            columns: Self::resolve_columns(schema)?,
            kind: VTabKind::VirtualTable,
            vtab_type: VirtualTableType::External(table),
            vtab_id: VTAB_ID_COUNTER.fetch_add(1, Ordering::Acquire),
        };
        Ok(Arc::new(vtab))
    }

    fn resolve_columns(schema: String) -> crate::Result<Vec<Column>> {
        let mut parser = Parser::new(schema.as_bytes());
        if let ast::Cmd::Stmt(ast::Stmt::CreateTable { body, .. }) =
            parser.next_cmd()?.ok_or_else(|| {
                LimboError::ParseError(
                    "Failed to parse schema from virtual table module".to_string(),
                )
            })?
        {
            columns_from_create_table_body(&body)
        } else {
            Err(LimboError::ParseError(
                "Failed to parse schema from virtual table module".to_string(),
            ))
        }
    }

    pub(crate) fn open(&self, conn: Arc<Connection>) -> crate::Result<VirtualTableCursor> {
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

    pub(crate) fn update(&self, args: &[Value]) -> crate::Result<Option<i64>> {
        match &self.vtab_type {
            VirtualTableType::Pragma(_) => Err(LimboError::ReadOnly),
            VirtualTableType::External(table) => table.update(args),
            VirtualTableType::Internal(_) => Err(LimboError::ReadOnly),
        }
    }

    pub(crate) fn destroy(&self) -> crate::Result<()> {
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
    External(ExtVirtualTableCursor),
    Internal(Arc<RwLock<dyn InternalVirtualTableCursor>>),
}

pub struct VirtualTableCursor {
    inner: VirtualTableCursorInner,
    null_flag: bool,
}

crate::assert::assert_send_sync!(VirtualTableCursor);

impl VirtualTableCursor {
    pub(crate) fn new_pragma(cursor: PragmaVirtualTableCursor) -> Self {
        Self {
            inner: VirtualTableCursorInner::Pragma(Box::new(cursor)),
            null_flag: false,
        }
    }

    pub(crate) fn new_external(cursor: ExtVirtualTableCursor) -> Self {
        Self {
            inner: VirtualTableCursorInner::External(cursor),
            null_flag: false,
        }
    }

    pub(crate) fn new_internal(cursor: Arc<RwLock<dyn InternalVirtualTableCursor>>) -> Self {
        Self {
            inner: VirtualTableCursorInner::Internal(cursor),
            null_flag: false,
        }
    }

    pub(crate) fn set_null_flag(&mut self, flag: bool) {
        self.null_flag = flag;
    }

    pub(crate) fn next(&mut self) -> crate::Result<bool> {
        self.null_flag = false;
        match &mut self.inner {
            VirtualTableCursorInner::Pragma(cursor) => cursor.next(),
            VirtualTableCursorInner::External(cursor) => cursor.next(),
            VirtualTableCursorInner::Internal(cursor) => cursor.write().next(),
        }
    }

    pub(crate) fn rowid(&self) -> i64 {
        match &self.inner {
            VirtualTableCursorInner::Pragma(cursor) => cursor.rowid(),
            VirtualTableCursorInner::External(cursor) => cursor.rowid(),
            VirtualTableCursorInner::Internal(cursor) => cursor.read().rowid(),
        }
    }

    pub(crate) fn column(&self, column: usize) -> crate::Result<Value> {
        if self.null_flag {
            return Ok(Value::Null);
        }
        match &self.inner {
            VirtualTableCursorInner::Pragma(cursor) => cursor.column(column),
            VirtualTableCursorInner::External(cursor) => cursor.column(column),
            VirtualTableCursorInner::Internal(cursor) => cursor.read().column(column),
        }
    }

    pub(crate) fn filter(
        &mut self,
        idx_num: i32,
        idx_str: Option<String>,
        arg_count: usize,
        args: Vec<Value>,
    ) -> crate::Result<bool> {
        self.null_flag = false;
        match &mut self.inner {
            VirtualTableCursorInner::Pragma(cursor) => cursor.filter(args),
            VirtualTableCursorInner::External(cursor) => {
                cursor.filter(idx_num, idx_str, arg_count, args)
            }
            VirtualTableCursorInner::Internal(cursor) => {
                cursor.write().filter(&args, idx_str, idx_num)
            }
        }
    }

    pub(crate) fn vtab_id(&self) -> Option<u64> {
        match &self.inner {
            VirtualTableCursorInner::Pragma(_) => None,
            VirtualTableCursorInner::External(cursor) => cursor.vtab_id.into(),
            VirtualTableCursorInner::Internal(_) => None,
        }
    }
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
    fn best_index(
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
    fn create(
        module_name: &str,
        module: Option<&Arc<crate::ext::VTabImpl>>,
        args: Vec<turso_ext::Value>,
        kind: VTabKind,
    ) -> crate::Result<(Self, String)> {
        let module = module.ok_or_else(|| {
            LimboError::ExtensionError(format!("Virtual table module not found: {module_name}"))
        })?;
        if kind != module.module_kind {
            let expected = match kind {
                VTabKind::VirtualTable => "virtual table",
                VTabKind::TableValuedFunction => "table-valued function",
            };
            return Err(LimboError::ExtensionError(format!(
                "{module_name} is not a {expected} module"
            )));
        }
        let (schema, table_ptr) = module.implementation.create(args)?;
        let vtab = ExtVirtualTable {
            implementation: module.implementation.clone(),
            table_ptr: AtomicPtr::new(table_ptr as *mut c_void),
        };
        Ok((vtab, schema))
    }

    /// Accepts a pointer connection that owns the VTable, that the module
    /// can optionally use to query the other tables.
    fn open(&self, conn: Arc<Connection>, id: u64) -> crate::Result<ExtVirtualTableCursor> {
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

    fn update(&self, args: &[Value]) -> crate::Result<Option<i64>> {
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

    fn destroy(&self) -> crate::Result<()> {
        let rc = unsafe {
            (self.implementation.destroy)(self.table_ptr.load(Ordering::SeqCst) as *const c_void)
        };
        match rc {
            ResultCode::OK => Ok(()),
            _ => Err(LimboError::ExtensionError(rc.to_string())),
        }
    }

    fn commit(&self) -> crate::Result<()> {
        let rc = unsafe { (self.implementation.commit)(self.table_ptr.load(Ordering::SeqCst)) };
        match rc {
            ResultCode::OK => Ok(()),
            _ => Err(LimboError::ExtensionError("Commit failed".to_string())),
        }
    }

    fn begin(&self) -> crate::Result<()> {
        let rc = unsafe { (self.implementation.begin)(self.table_ptr.load(Ordering::SeqCst)) };
        match rc {
            ResultCode::OK => Ok(()),
            _ => Err(LimboError::ExtensionError("Begin failed".to_string())),
        }
    }

    fn rollback(&self) -> crate::Result<()> {
        let rc = unsafe { (self.implementation.rollback)(self.table_ptr.load(Ordering::SeqCst)) };
        match rc {
            ResultCode::OK => Ok(()),
            _ => Err(LimboError::ExtensionError("Rollback failed".to_string())),
        }
    }

    fn rename(&self, new_name: &str) -> crate::Result<()> {
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
    vtab_id: u64,
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

    fn rowid(&self) -> i64 {
        unsafe { (self.implementation.rowid)(self.cursor.as_ptr()) }
    }

    #[tracing::instrument(skip(self))]
    fn filter(
        &self,
        idx_num: i32,
        idx_str: Option<String>,
        arg_count: usize,
        args: Vec<Value>,
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

    fn column(&self, column: usize) -> crate::Result<Value> {
        let val = unsafe { (self.implementation.column)(self.cursor.as_ptr(), column as u32) };
        Value::from_ffi(val)
    }

    fn next(&self) -> crate::Result<bool> {
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
    fn sql(&self) -> String;
}

pub trait InternalVirtualTableCursor: Send + Sync {
    /// next returns `Ok(true)` if there are more rows, and `Ok(false)` otherwise.
    fn next(&mut self) -> Result<bool, LimboError>;
    fn rowid(&self) -> i64;
    fn column(&self, column: usize) -> Result<Value, LimboError>;
    fn filter(
        &mut self,
        args: &[Value],
        idx_str: Option<String>,
        idx_num: i32,
    ) -> Result<bool, LimboError>;
}
