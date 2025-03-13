use crate::{types::OwnedValue, Connection, Statement, StepResult};
use limbo_ext::{Conn as ExtConn, ResultCode, Stmt, Value};
use std::{
    boxed::Box,
    ffi::{c_char, c_void, CStr, CString},
    num::NonZeroUsize,
    ptr,
    rc::Rc,
};

pub unsafe extern "C" fn connect(ctx: *mut c_void) -> *mut ExtConn {
    if ctx.is_null() {
        return ptr::null_mut();
    }
    Rc::increment_strong_count(ctx as *const Connection);
    let ext_conn = ExtConn::new(ctx, prepare_stmt, close);
    Box::into_raw(Box::new(ext_conn))
}

pub unsafe extern "C" fn close(ctx: *mut c_void) {
    let conn = Rc::from_raw(ctx as *const Connection);
    let _ = conn.close();
}

pub unsafe extern "C" fn prepare_stmt(ctx: *mut ExtConn, sql: *const c_char) -> *const Stmt {
    let c_str = unsafe { CStr::from_ptr(sql as *mut c_char) };
    let sql_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ptr::null_mut(),
    };
    if ctx.is_null() {
        return ptr::null_mut();
    }
    let Ok(extcon) = ExtConn::from_ptr(ctx) else {
        return ptr::null_mut();
    };
    Rc::increment_strong_count(extcon._ctx as *const Connection);
    let conn: Rc<Connection> = Rc::from_raw(extcon._ctx as *const Connection);
    match conn.prepare(&sql_str) {
        Ok(stmt) => {
            let stmt = Box::new(stmt);
            Box::into_raw(Box::new(Stmt::new(
                Rc::into_raw(conn.clone()) as *mut c_void,
                Box::into_raw(stmt) as *mut c_void,
                stmt_bind_args_fn,
                stmt_step,
                stmt_get_row,
                stmt_get_column_names,
                stmt_close,
            ))) as *const Stmt
        }
        Err(_) => ptr::null_mut(),
    }
}

pub unsafe extern "C" fn stmt_bind_args_fn(
    ctx: *mut Stmt,
    idx: i32,
    arg: *const Value,
) -> ResultCode {
    let Ok(stmt) = Stmt::from_ptr(ctx) else {
        return ResultCode::Error;
    };
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    let Ok(owned_val) = OwnedValue::from_ffi_ptr(arg) else {
        return ResultCode::Error;
    };
    let Some(idx) = NonZeroUsize::new(idx as usize) else {
        return ResultCode::Error;
    };
    stmt_ctx.bind_at(idx, owned_val);
    ResultCode::OK
}

pub unsafe extern "C" fn stmt_step(stmt: *mut Stmt) -> ResultCode {
    let Ok(stmt) = Stmt::from_ptr(stmt) else {
        return ResultCode::Error;
    };
    if stmt._conn.is_null() || stmt._ctx.is_null() {
        return ResultCode::Error;
    }
    let conn: &Connection = unsafe { &*(stmt._conn as *const Connection) };
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    loop {
        match stmt_ctx.step() {
            Ok(StepResult::Row) => return ResultCode::Row,
            Ok(StepResult::Done) => return ResultCode::OK,
            Ok(StepResult::IO) => {
                let _ = conn.pager.io.run_once();
                continue;
            }
            Ok(StepResult::Interrupt) => return ResultCode::Interrupt,
            Ok(StepResult::Busy) => return ResultCode::Busy,
            _ => {
                return ResultCode::Error;
            }
        }
    }
}

pub unsafe extern "C" fn stmt_get_row(ctx: *mut Stmt) {
    let Ok(stmt) = Stmt::from_ptr(ctx) else {
        return;
    };
    if !stmt.current_row.is_null() {
        stmt.free_current_row();
    }
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    if let Some(row) = stmt_ctx.row() {
        let values = row.get_values();
        let mut owned_values = Vec::with_capacity(values.len());
        for value in values {
            owned_values.push(OwnedValue::to_ffi(value));
        }
        stmt.current_row = Box::into_raw(owned_values.into_boxed_slice()) as *mut Value;
        stmt.current_row_len = values.len() as i32;
    } else {
        stmt.current_row_len = 0;
    }
}

pub unsafe extern "C" fn stmt_get_column_names(
    ctx: *mut Stmt,
    count: *mut i32,
) -> *mut *mut c_char {
    let Ok(stmt) = Stmt::from_ptr(ctx) else {
        *count = 0;
        return ptr::null_mut();
    };
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    let num_cols = stmt_ctx.num_columns();
    if num_cols == 0 {
        *count = 0;
        return ptr::null_mut();
    }
    let mut c_names: Vec<*mut c_char> = Vec::with_capacity(num_cols);
    for i in 0..num_cols {
        let name = stmt_ctx.get_column_name(i);
        let c_str = CString::new(name.as_bytes()).unwrap();
        c_names.push(c_str.into_raw());
    }

    *count = c_names.len() as i32;
    let names_array = c_names.into_boxed_slice();
    Box::into_raw(names_array) as *mut *mut c_char
}

pub unsafe extern "C" fn stmt_close(ctx: *mut Stmt) {
    let Ok(stmt) = Stmt::from_ptr(ctx) else {
        return;
    };
    if !stmt.current_row.is_null() {
        stmt.free_current_row();
    }
    // take ownership of internal statement
    let mut stmt: Box<Statement> = Box::from_raw(stmt._ctx as *mut Statement);
    stmt.reset()
}
