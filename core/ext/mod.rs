use crate::{
    function::ExternalFunc,
    schema::{Column, Type},
    Database, VirtualTable,
};
use fallible_iterator::FallibleIterator;
use limbo_ext::{ExtensionApi, InitAggFunction, ResultCode, ScalarFunction, VTabModuleImpl};
pub use limbo_ext::{FinalizeFunction, StepFunction, Value as ExtValue, ValueType as ExtValueType};
use sqlite3_parser::{
    ast::{Cmd, CreateTableBody, Stmt},
    lexer::sql::Parser,
};
use std::{
    ffi::{c_char, c_void, CStr},
    rc::Rc,
};
type ExternAggFunc = (InitAggFunction, StepFunction, FinalizeFunction);

unsafe extern "C" fn register_scalar_function(
    ctx: *mut c_void,
    name: *const c_char,
    func: ScalarFunction,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    println!("Scalar??");
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::InvalidArgs,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let db = unsafe { &*(ctx as *const Database) };
    db.register_scalar_function_impl(&name_str, func)
}

unsafe extern "C" fn register_aggregate_function(
    ctx: *mut c_void,
    name: *const c_char,
    args: i32,
    init_func: InitAggFunction,
    step_func: StepFunction,
    finalize_func: FinalizeFunction,
) -> ResultCode {
    println!("Aggregate??");
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::InvalidArgs,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let db = unsafe { &*(ctx as *const Database) };
    db.register_aggregate_function_impl(&name_str, args, (init_func, step_func, finalize_func))
}

unsafe extern "C" fn register_module(
    ctx: *mut c_void,
    name: *const c_char,
    module: VTabModuleImpl,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::Error,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let db = unsafe { &*(ctx as *const Database) };

    db.register_module_impl(&name_str, module)
}

unsafe extern "C" fn declare_vtab(
    ctx: *mut c_void,
    name: *const c_char,
    sql: *const c_char,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::Error,
    };

    let c_str = unsafe { CStr::from_ptr(sql) };
    let sql_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::Error,
    };

    if ctx.is_null() {
        return ResultCode::Error;
    }
    let db = unsafe { &*(ctx as *const Database) };
    db.declare_vtab_impl(&name_str, &sql_str)
}

impl Database {
    fn register_scalar_function_impl(&self, name: &str, func: ScalarFunction) -> ResultCode {
        self.syms.borrow_mut().functions.insert(
            name.to_string(),
            Rc::new(ExternalFunc::new_scalar(name.to_string(), func)),
        );
        ResultCode::OK
    }

    fn register_aggregate_function_impl(
        &self,
        name: &str,
        args: i32,
        func: ExternAggFunc,
    ) -> ResultCode {
        self.syms.borrow_mut().functions.insert(
            name.to_string(),
            Rc::new(ExternalFunc::new_aggregate(name.to_string(), args, func)),
        );
        ResultCode::OK
    }

    fn register_module_impl(&self, name: &str, module: VTabModuleImpl) -> ResultCode {
        self.vtab_modules
            .borrow_mut()
            .insert(name.to_string(), Rc::new(module));
        ResultCode::OK
    }

    fn declare_vtab_impl(&self, name: &str, sql: &str) -> ResultCode {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next().unwrap().unwrap();
        let Cmd::Stmt(stmt) = cmd else {
            return ResultCode::Error;
        };
        let Stmt::CreateTable { body, .. } = stmt else {
            return ResultCode::Error;
        };
        let CreateTableBody::ColumnsAndConstraints { columns, .. } = body else {
            return ResultCode::Error;
        };

        let columns = columns
            .into_iter()
            .filter_map(|(name, column_def)| {
                // if column_def.col_type includes HIDDEN, omit it for now
                if let Some(data_type) = column_def.col_type.as_ref() {
                    if data_type.name.as_str().contains("HIDDEN") {
                        return None;
                    }
                }
                let column = Column {
                    name: name.0.clone(),
                    // TODO extract to util, we use this elsewhere too.
                    ty: match column_def.col_type {
                        Some(data_type) => {
                            // https://www.sqlite.org/datatype3.html
                            let type_name = data_type.name.as_str().to_uppercase();
                            if type_name.contains("INT") {
                                Type::Integer
                            } else if type_name.contains("CHAR")
                                || type_name.contains("CLOB")
                                || type_name.contains("TEXT")
                            {
                                Type::Text
                            } else if type_name.contains("BLOB") || type_name.is_empty() {
                                Type::Blob
                            } else if type_name.contains("REAL")
                                || type_name.contains("FLOA")
                                || type_name.contains("DOUB")
                            {
                                Type::Real
                            } else {
                                Type::Numeric
                            }
                        }
                        None => Type::Null,
                    },
                    primary_key: column_def.constraints.iter().any(|c| {
                        matches!(
                            c.constraint,
                            sqlite3_parser::ast::ColumnConstraint::PrimaryKey { .. }
                        )
                    }),
                    is_rowid_alias: false,
                };
                Some(column)
            })
            .collect::<Vec<_>>();
        let vtab_module = self.vtab_modules.borrow().get(name).unwrap().clone();
        let vtab = VirtualTable {
            name: name.to_string(),
            implementation: vtab_module,
            columns,
        };
        self.syms
            .borrow_mut()
            .vtabs
            .insert(name.to_string(), Rc::new(vtab));
        ResultCode::OK
    }

    pub fn build_limbo_ext(&self) -> ExtensionApi {
        ExtensionApi {
            ctx: self as *const _ as *mut c_void,
            register_scalar_function,
            register_aggregate_function,
            register_module,
            declare_vtab,
        }
    }
}
