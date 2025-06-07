use crate::pragma::{PragmaVirtualTable, PragmaVirtualTableCursor};
use crate::schema::Column;
use crate::util::{columns_from_create_table_body, vtable_args};
use crate::{Connection, LimboError, SymbolTable, Value};
use fallible_iterator::FallibleIterator;
use limbo_ext::{ConstraintInfo, IndexInfo, OrderByInfo, ResultCode, VTabKind, VTabModuleImpl};
use limbo_sqlite3_parser::{ast, lexer::sql::Parser};
use std::cell::RefCell;
use std::ffi::c_void;
use std::rc::{Rc, Weak};

#[derive(Debug, Clone)]
enum VirtualTableType {
    Pragma(PragmaVirtualTable),
    External(ExtVirtualTable),
}

#[derive(Clone, Debug)]
pub struct VirtualTable {
    pub(crate) name: String,
    pub(crate) args: Option<Vec<ast::Expr>>,
    pub(crate) columns: Vec<Column>,
    pub(crate) kind: VTabKind,
    vtab_type: VirtualTableType,
}

impl VirtualTable {
    pub(crate) fn function(
        name: &str,
        args: Option<Vec<ast::Expr>>,
        syms: &SymbolTable,
    ) -> crate::Result<Rc<VirtualTable>> {
        let module = syms.vtab_modules.get(name);
        let (vtab_type, schema) = if let Some(_) = module {
            let ext_args = match args {
                Some(ref args) => vtable_args(args),
                None => vec![],
            };
            ExtVirtualTable::create(name, module, ext_args, VTabKind::TableValuedFunction)
                .map(|(vtab, columns)| (VirtualTableType::External(vtab), columns))?
        } else if let Some(pragma_name) = name.strip_prefix("pragma_") {
            PragmaVirtualTable::create(pragma_name)
                .map(|(vtab, columns)| (VirtualTableType::Pragma(vtab), columns))?
        } else {
            return Err(LimboError::ParseError(format!(
                "No such table-valued function: {}",
                name
            )));
        };

        let vtab = VirtualTable {
            name: name.to_owned(),
            args,
            columns: Self::resolve_columns(schema)?,
            kind: VTabKind::TableValuedFunction,
            vtab_type,
        };
        Ok(Rc::new(vtab))
    }

    pub fn table(
        tbl_name: Option<&str>,
        module_name: &str,
        args: Vec<limbo_ext::Value>,
        syms: &SymbolTable,
    ) -> crate::Result<Rc<VirtualTable>> {
        let module = syms.vtab_modules.get(module_name);
        let (table, schema) =
            ExtVirtualTable::create(module_name, module, args, VTabKind::VirtualTable)?;
        let vtab = VirtualTable {
            name: tbl_name.unwrap_or(module_name).to_owned(),
            args: None,
            columns: Self::resolve_columns(schema)?,
            kind: VTabKind::VirtualTable,
            vtab_type: VirtualTableType::External(table),
        };
        Ok(Rc::new(vtab))
    }

    fn resolve_columns(schema: String) -> crate::Result<Vec<Column>> {
        let mut parser = Parser::new(schema.as_bytes());
        if let ast::Cmd::Stmt(ast::Stmt::CreateTable { body, .. }) = parser.next()?.ok_or(
            LimboError::ParseError("Failed to parse schema from virtual table module".to_string()),
        )? {
            columns_from_create_table_body(&body)
        } else {
            Err(LimboError::ParseError(
                "Failed to parse schema from virtual table module".to_string(),
            ))
        }
    }

    pub(crate) fn open(&self, conn: Weak<Connection>) -> crate::Result<VirtualTableCursor> {
        match &self.vtab_type {
            VirtualTableType::Pragma(table) => Ok(VirtualTableCursor::Pragma(table.open(conn)?)),
            VirtualTableType::External(table) => {
                Ok(VirtualTableCursor::External(table.open(conn)?))
            }
        }
    }

    pub(crate) fn update(&self, args: &[Value]) -> crate::Result<Option<i64>> {
        match &self.vtab_type {
            VirtualTableType::Pragma(_) => Err(LimboError::ReadOnly),
            VirtualTableType::External(table) => table.update(args),
        }
    }

    pub(crate) fn destroy(&self) -> crate::Result<()> {
        match &self.vtab_type {
            VirtualTableType::Pragma(_) => Ok(()),
            VirtualTableType::External(table) => table.destroy(),
        }
    }

    pub(crate) fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        order_by: &[OrderByInfo],
    ) -> IndexInfo {
        match &self.vtab_type {
            VirtualTableType::Pragma(_) => {
                // SQLite tries to estimate cost and row count for pragma_ TVFs,
                // but since Limbo doesn't have cost-based planning yet, this
                // estimation is not currently implemented.
                Default::default()
            }
            VirtualTableType::External(table) => table.best_index(constraints, order_by),
        }
    }
}

pub enum VirtualTableCursor {
    Pragma(PragmaVirtualTableCursor),
    External(ExtVirtualTableCursor),
}

impl VirtualTableCursor {
    pub(crate) fn next(&mut self) -> crate::Result<bool> {
        match self {
            VirtualTableCursor::Pragma(cursor) => cursor.next(),
            VirtualTableCursor::External(cursor) => cursor.next(),
        }
    }

    pub(crate) fn rowid(&self) -> i64 {
        match self {
            VirtualTableCursor::Pragma(cursor) => cursor.rowid(),
            VirtualTableCursor::External(cursor) => cursor.rowid(),
        }
    }

    pub(crate) fn column(&self, column: usize) -> crate::Result<Value> {
        match self {
            VirtualTableCursor::Pragma(cursor) => cursor.column(column),
            VirtualTableCursor::External(cursor) => cursor.column(column),
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
        }
    }
}

#[derive(Clone, Debug)]
struct ExtVirtualTable {
    implementation: Rc<VTabModuleImpl>,
    table_ptr: *const c_void,
    connection_ptr: RefCell<Option<*mut limbo_ext::Conn>>,
}

impl Drop for ExtVirtualTable {
    fn drop(&mut self) {
        if let Some(conn) = self.connection_ptr.borrow_mut().take() {
            if conn.is_null() {
                return;
            }
            // free the memory for the limbo_ext::Conn itself
            let mut conn = unsafe { Box::from_raw(conn) };
            // frees the boxed Weak pointer
            conn.close();
        }
        *self.connection_ptr.borrow_mut() = None;
    }
}

impl ExtVirtualTable {
    fn best_index(&self, constraints: &[ConstraintInfo], order_by: &[OrderByInfo]) -> IndexInfo {
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
        module: Option<&Rc<crate::ext::VTabImpl>>,
        args: Vec<limbo_ext::Value>,
        kind: VTabKind,
    ) -> crate::Result<(Self, String)> {
        let module = module.ok_or(LimboError::ExtensionError(format!(
            "Virtual table module not found: {}",
            module_name
        )))?;
        if kind != module.module_kind {
            let expected = match kind {
                VTabKind::VirtualTable => "virtual table",
                VTabKind::TableValuedFunction => "table-valued function",
            };
            return Err(LimboError::ExtensionError(format!(
                "{} is not a {} module",
                module_name, expected
            )));
        }
        let (schema, table_ptr) = module.implementation.create(args)?;
        let vtab = ExtVirtualTable {
            connection_ptr: RefCell::new(None),
            implementation: module.implementation.clone(),
            table_ptr,
        };
        Ok((vtab, schema))
    }

    /// Accepts a Weak pointer to the connection that owns the VTable, that the module
    /// can optionally use to query the other tables.
    fn open(&self, conn: Weak<Connection>) -> crate::Result<ExtVirtualTableCursor> {
        // we need a Weak<Connection> to upgrade and call from the extension.
        let weak_box: *mut Weak<Connection> = Box::into_raw(Box::new(conn));
        let conn = limbo_ext::Conn::new(
            weak_box.cast(),
            crate::ext::prepare_stmt,
            crate::ext::execute,
            crate::ext::close,
        );
        let ext_conn_ptr = Box::into_raw(Box::new(conn));
        // store the leaked connection pointer on the table so it can be freed on drop
        *self.connection_ptr.borrow_mut() = Some(ext_conn_ptr);
        let cursor = unsafe { (self.implementation.open)(self.table_ptr, ext_conn_ptr) };
        ExtVirtualTableCursor::new(cursor, self.implementation.clone())
    }

    fn update(&self, args: &[Value]) -> crate::Result<Option<i64>> {
        let arg_count = args.len();
        let ext_args = args.iter().map(|arg| arg.to_ffi()).collect::<Vec<_>>();
        let newrowid = 0i64;
        let rc = unsafe {
            (self.implementation.update)(
                self.table_ptr,
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
        let rc = unsafe { (self.implementation.destroy)(self.table_ptr) };
        match rc {
            ResultCode::OK => Ok(()),
            _ => Err(LimboError::ExtensionError(rc.to_string())),
        }
    }
}

pub struct ExtVirtualTableCursor {
    cursor: *const c_void,
    implementation: Rc<VTabModuleImpl>,
}

impl ExtVirtualTableCursor {
    fn new(cursor: *const c_void, implementation: Rc<VTabModuleImpl>) -> crate::Result<Self> {
        if cursor.is_null() {
            return Err(LimboError::InternalError(
                "VirtualTableCursor: cursor is null".into(),
            ));
        }
        Ok(Self {
            cursor,
            implementation,
        })
    }

    fn rowid(&self) -> i64 {
        unsafe { (self.implementation.rowid)(self.cursor) }
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
                self.cursor,
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
        let val = unsafe { (self.implementation.column)(self.cursor, column as u32) };
        Value::from_ffi(val)
    }

    fn next(&self) -> crate::Result<bool> {
        let rc = unsafe { (self.implementation.next)(self.cursor) };
        match rc {
            ResultCode::OK => Ok(true),
            ResultCode::EOF => Ok(false),
            _ => Err(LimboError::ExtensionError("Next failed".to_string())),
        }
    }
}

impl Drop for ExtVirtualTableCursor {
    fn drop(&mut self) {
        let result = unsafe { (self.implementation.close)(self.cursor) };
        if !result.is_ok() {
            tracing::error!("Failed to close virtual table cursor");
        }
    }
}
