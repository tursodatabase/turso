use crate::pragma::{PragmaVirtualTable, PragmaVirtualTableCursor};
use crate::schema::Column;
use crate::util::columns_from_create_table_body;
use crate::{Connection, LimboError, SymbolTable, Value};
use parking_lot::RwLock;
use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::atomic::AtomicPtr;
use std::sync::Arc;
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
}

impl VirtualTable {
    pub(crate) fn readonly(self: &Arc<VirtualTable>) -> bool {
        match &self.vtab_type {
            VirtualTableType::Pragma(_) => true,
            VirtualTableType::External(table) => table.readonly(),
            VirtualTableType::Internal(_) => true,
        }
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
                };
                Arc::new(vtab)
            })
            .collect();

        #[cfg(feature = "json")]
        vtables.extend(Self::json_virtual_tables());

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
        };

        let json_tree = JsonVirtualTable::json_tree();

        let json_tree_virtual_table = VirtualTable {
            name: json_tree.name(),
            columns: Self::resolve_columns(json_tree.sql())
                .expect("internal table-valued function schema resolution should not fail"),
            kind: VTabKind::TableValuedFunction,
            vtab_type: VirtualTableType::Internal(Arc::new(RwLock::new(json_tree))),
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
        };
        Ok(Arc::new(vtab))
    }

    fn resolve_columns(schema: String) -> crate::Result<Vec<Column>> {
        let mut parser = Parser::new(schema.as_bytes());
        if let ast::Cmd::Stmt(ast::Stmt::CreateTable { body, .. }) = parser.next_cmd()?.ok_or(
            LimboError::ParseError("Failed to parse schema from virtual table module".to_string()),
        )? {
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
                Ok(VirtualTableCursor::Pragma(Box::new(table.open(conn)?)))
            }
            VirtualTableType::External(table) => {
                Ok(VirtualTableCursor::External(table.open(conn.clone())?))
            }
            VirtualTableType::Internal(table) => {
                Ok(VirtualTableCursor::Internal(table.read().open(conn)?))
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
}

pub enum VirtualTableCursor {
    Pragma(Box<PragmaVirtualTableCursor>),
    External(ExtVirtualTableCursor),
    Internal(Arc<RwLock<dyn InternalVirtualTableCursor>>),
}

impl VirtualTableCursor {
    pub(crate) fn next(&mut self) -> crate::Result<bool> {
        match self {
            VirtualTableCursor::Pragma(cursor) => cursor.next(),
            VirtualTableCursor::External(cursor) => cursor.next(),
            VirtualTableCursor::Internal(cursor) => cursor.write().next(),
        }
    }

    pub(crate) fn rowid(&self) -> i64 {
        match self {
            VirtualTableCursor::Pragma(cursor) => cursor.rowid(),
            VirtualTableCursor::External(cursor) => cursor.rowid(),
            VirtualTableCursor::Internal(cursor) => cursor.read().rowid(),
        }
    }

    pub(crate) fn column(&self, column: usize) -> crate::Result<Value> {
        match self {
            VirtualTableCursor::Pragma(cursor) => cursor.column(column),
            VirtualTableCursor::External(cursor) => cursor.column(column),
            VirtualTableCursor::Internal(cursor) => cursor.read().column(column),
        }
    }

    pub(crate) fn filter(
        &mut self,
        idx_num: i32,
        idx_str: Option<String>,
        arg_count: usize,
        args: Vec<Value>,
    ) -> crate::Result<bool> {
        match self {
            VirtualTableCursor::Pragma(cursor) => cursor.filter(args),
            VirtualTableCursor::External(cursor) => {
                cursor.filter(idx_num, idx_str, arg_count, args)
            }
            VirtualTableCursor::Internal(cursor) => cursor.write().filter(&args, idx_str, idx_num),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ExtVirtualTable {
    implementation: Arc<VTabModuleImpl>,
    table_ptr: AtomicPtr<c_void>,
}

impl Clone for ExtVirtualTable {
    fn clone(&self) -> Self {
        Self {
            implementation: self.implementation.clone(),
            table_ptr: AtomicPtr::new(self.table_ptr.load(std::sync::atomic::Ordering::SeqCst)),
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
        let module = module.ok_or(LimboError::ExtensionError(format!(
            "Virtual table module not found: {module_name}"
        )))?;
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
    fn open(&self, conn: Arc<Connection>) -> crate::Result<ExtVirtualTableCursor> {
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
                self.table_ptr.load(std::sync::atomic::Ordering::SeqCst) as *const c_void,
                ext_conn_ptr.as_ptr(),
            ) as *mut c_void
        }) else {
            return Err(LimboError::ExtensionError("Open returned null".to_string()));
        };
        ExtVirtualTableCursor::new(cursor, ext_conn_ptr, self.implementation.clone())
    }

    fn update(&self, args: &[Value]) -> crate::Result<Option<i64>> {
        let arg_count = args.len();
        let ext_args = args.iter().map(|arg| arg.to_ffi()).collect::<Vec<_>>();
        let newrowid = 0i64;
        let rc = unsafe {
            (self.implementation.update)(
                self.table_ptr.load(std::sync::atomic::Ordering::SeqCst) as *const c_void,
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
            (self.implementation.destroy)(
                self.table_ptr.load(std::sync::atomic::Ordering::SeqCst) as *const c_void
            )
        };
        match rc {
            ResultCode::OK => Ok(()),
            _ => Err(LimboError::ExtensionError(rc.to_string())),
        }
    }
}

pub struct ExtVirtualTableCursor {
    cursor: NonNull<c_void>,
    // the core `[Connection]` pointer the vtab module needs to
    // query other internal tables.
    conn_ptr: Option<NonNull<turso_ext::Conn>>,
    implementation: Arc<VTabModuleImpl>,
}

impl ExtVirtualTableCursor {
    fn new(
        cursor: NonNull<c_void>,
        conn_ptr: NonNull<turso_ext::Conn>,
        implementation: Arc<VTabModuleImpl>,
    ) -> crate::Result<Self> {
        Ok(Self {
            cursor,
            conn_ptr: Some(conn_ptr),
            implementation,
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
        let c_idx_str = idx_str
            .map(|s| std::ffi::CString::new(s).unwrap())
            .map(|cstr| cstr.into_raw())
            .unwrap_or(std::ptr::null_mut());
        let rc = unsafe {
            (self.implementation.filter)(
                self.cursor.as_ptr(),
                arg_count as i32,
                ext_args.as_ptr(),
                c_idx_str,
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
                let _ = unsafe { Box::from_raw(conn._ctx as *mut std::sync::Weak<Connection>) };
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
