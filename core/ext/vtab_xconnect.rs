use crate::{types::Value, Connection, Statement, StepResult};
use std::{
    boxed::Box,
    ffi::{c_char, c_void, CStr, CString},
    num::NonZeroUsize,
    ptr,
    sync::Arc,
};
use turso_ext::{Conn as ExtConn, ResultCode, Stmt, Value as ExtValue};

/// Free memory for the internal context of the connection.
/// This function does not close the core connection itself,
/// it only frees the memory the table is responsible for.
pub unsafe extern "C" fn close(ctx: *mut c_void) {
    if ctx.is_null() {
        return;
    }
    // only free the memory for the boxed connection, we don't upgrade
    // or actually close the core connection, as we were 'sharing' it.
    let _ = Box::from_raw(ctx as *mut Arc<Connection>);
}

/// Wrapper around core Connection::execute with optional arguments to bind
/// to the statment This function takes ownership of the optional turso_ext::Value array if provided
pub unsafe extern "C" fn execute(
    ctx: *mut ExtConn,
    sql: *const c_char,
    args: *mut ExtValue,
    arg_count: i32,
    last_insert_rowid: *mut i64,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(sql as *mut c_char) };
    let sql_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => {
            tracing::error!("query: failed to convert sql to string");
            return ResultCode::Error;
        }
    };
    let Ok(extcon) = ExtConn::from_ptr(ctx) else {
        tracing::error!("query: null connection");
        return ResultCode::Error;
    };
    let conn_ptr = extcon._ctx as *const Arc<Connection>;
    let conn = &*conn_ptr;
    match conn.query(&sql_str) {
        Ok(Some(mut stmt)) => {
            if arg_count > 0 {
                let args_slice = &mut std::slice::from_raw_parts_mut(args, arg_count as usize);
                for (i, val) in args_slice.iter_mut().enumerate() {
                    stmt.bind_at(
                        NonZeroUsize::new(i + 1).unwrap(),
                        Value::from_ffi(std::mem::take(val)).unwrap_or(Value::Null),
                    );
                }
            }
            loop {
                match stmt.step() {
                    Ok(StepResult::Row) => {
                        tracing::error!("execute used for query returning a row");
                        return ResultCode::Error;
                    }
                    Ok(StepResult::Done) => {
                        *last_insert_rowid = conn.last_insert_rowid();
                        return ResultCode::OK;
                    }
                    Ok(StepResult::IO) => {
                        let res = stmt.run_once();
                        if res.is_err() {
                            return ResultCode::Error;
                        }
                        continue;
                    }
                    Ok(StepResult::Interrupt) => return ResultCode::Interrupt,
                    Ok(StepResult::Busy) => return ResultCode::Busy,
                    Err(e) => {
                        tracing::error!("execute: failed to execute query: {:?}", e);
                        return ResultCode::Error;
                    }
                }
            }
        }
        Ok(None) => tracing::error!("query: no statement returned"),
        Err(e) => tracing::error!("query: failed to execute query: {:?}", e),
    };
    ResultCode::Error
}

/// Wraps core Connection::prepare with a custom Stmt object with the necessary function pointers.
/// This object is boxed/leaked and the caller is responsible for freeing the memory.
pub unsafe extern "C" fn prepare_stmt(ctx: *mut ExtConn, sql: *const c_char) -> *mut Stmt {
    let c_str = unsafe { CStr::from_ptr(sql as *mut c_char) };
    let sql_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => {
            tracing::error!("prepare_stmt: failed to convert sql to string");
            return ptr::null_mut();
        }
    };
    let Ok(extcon) = ExtConn::from_ptr(ctx) else {
        tracing::error!("prepare_stmt: null connection");
        return ptr::null_mut();
    };
    let db_ptr = extcon._ctx as *const Arc<Connection>;
    let conn = &*db_ptr;
    match conn.prepare(&sql_str) {
        Ok(stmt) => {
            let raw_stmt = Box::into_raw(Box::new(stmt)) as *mut c_void;
            Box::into_raw(Box::new(Stmt::new(
                extcon._ctx,
                raw_stmt,
                stmt_bind_args_fn,
                stmt_step,
                stmt_get_row,
                stmt_get_column_names,
                stmt_free_current_row,
                stmt_close,
            )))
        }
        Err(e) => {
            tracing::error!("prepare_stmt: failed to prepare statement: {:?}", e);
            ptr::null_mut()
        }
    }
}

/// This function expects 1 based indexing. Wraps core statement bind_at functionality
/// this function does not take ownership of the provided arg value
pub unsafe extern "C" fn stmt_bind_args_fn(ctx: *mut Stmt, idx: i32, arg: ExtValue) -> ResultCode {
    let Ok(stmt) = Stmt::from_ptr(ctx) else {
        tracing::error!("prepare_stmt: null stmt pointer");
        return ResultCode::Error;
    };
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    // from_ffi takes ownership
    let Ok(owned_val) = Value::from_ffi(arg) else {
        tracing::error!("stmt_bind_args_fn: failed to convert arg to Value");
        return ResultCode::Error;
    };
    let Some(idx) = NonZeroUsize::new(idx as usize) else {
        tracing::error!("stmt_bind_args_fn: invalid index");
        return ResultCode::Error;
    };
    stmt_ctx.bind_at(idx, owned_val);
    ResultCode::OK
}

/// Wraps the functionality of the core Statement::step function,
/// preferring to handle the IO step result internally to prevent having to expose
/// run_once. Returns the equivalent ResultCode which then maps to an external StepResult.
pub unsafe extern "C" fn stmt_step(stmt: *mut Stmt) -> ResultCode {
    let Ok(stmt) = Stmt::from_ptr(stmt) else {
        tracing::error!("stmt_step: failed to convert stmt to Stmt");
        return ResultCode::Error;
    };
    if stmt._conn.is_null() || stmt._ctx.is_null() {
        tracing::error!("stmt_step: null connection or context");
        return ResultCode::Error;
    }
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    while let Ok(res) = stmt_ctx.step() {
        match res {
            StepResult::Row => return ResultCode::Row,
            StepResult::Done => return ResultCode::EOF,
            StepResult::IO => {
                // always handle IO step result internally.
                let res = stmt_ctx.run_once();
                if res.is_err() {
                    return ResultCode::Error;
                }
                continue;
            }
            StepResult::Interrupt => return ResultCode::Interrupt,
            StepResult::Busy => return ResultCode::Busy,
        }
    }
    ResultCode::Error
}

/// Instead of returning a pointer to the row, sets the Stmt's 'cursor'/current_row
/// to the next result row, and then the caller can access the resulting value on the Stmt.
pub unsafe extern "C" fn stmt_get_row(ctx: *mut Stmt) {
    let Ok(stmt) = Stmt::from_ptr(ctx) else {
        tracing::error!("stmt_get_row: failed to convert stmt to Stmt");
        return;
    };
    if !stmt.current_row.is_null() {
        stmt.free_current_row();
    }
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    if let Some(row) = stmt_ctx.row() {
        let values = row.get_values();
        let mut owned_values = Vec::with_capacity(row.len());
        for value in values {
            owned_values.push(Value::to_ffi(value));
        }
        stmt.current_row = Box::into_raw(owned_values.into_boxed_slice()) as *mut ExtValue;
        stmt.current_row_len = row.len() as i32;
    } else {
        stmt.current_row_len = 0;
    }
}

/// Free the memory of the current row/cursor of the Stmt object.
pub unsafe extern "C" fn stmt_free_current_row(ctx: *mut Stmt) {
    let Ok(stmt) = Stmt::from_ptr(ctx) else {
        return;
    };
    if !stmt.current_row.is_null() {
        let values: &mut [ExtValue] =
            std::slice::from_raw_parts_mut(stmt.current_row, stmt.current_row_len as usize);
        for value in values.iter_mut() {
            let owned_value = std::mem::take(value);
            owned_value.__free_internal_type();
        }
        let _ = Box::from_raw(stmt.current_row);
    }
}

/// Provides an easier API to get all the result column names associated with
/// the prepared Statement. The caller is responsible for freeing the memory
pub unsafe extern "C" fn stmt_get_column_names(
    ctx: *mut Stmt,
    count: *mut i32,
) -> *mut *mut c_char {
    if !count.is_null() {
        *count = 0;
    }
    let Ok(stmt) = Stmt::from_ptr(ctx) else {
        tracing::error!("stmt_get_column_names: null Stmt pointer");
        return ptr::null_mut();
    };
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    let num_cols = stmt_ctx.num_columns();
    if num_cols == 0 {
        tracing::info!("stmt_get_column_names: no columns");
        return ptr::null_mut();
    }
    let mut names: Vec<*mut c_char> = Vec::with_capacity(num_cols);
    // collect all the column names and convert them to C strings to send back
    for i in 0..num_cols {
        let name = stmt_ctx.get_column_name(i);
        match CString::new(name.as_bytes()) {
            Ok(cstr) => names.push(cstr.into_raw()),
            Err(_) => {
                // fall-back: free what we allocated so far
                for p in names {
                    let _ = CString::from_raw(p);
                }
                return std::ptr::null_mut();
            }
        }
    }

    if !count.is_null() {
        *count = names.len() as i32;
    }
    Box::into_raw(names.into_boxed_slice()) as *mut *mut c_char
}

/// Ffi/extension wrapper around core Statement::reset and
/// cleans up resources associated with the Statement
pub unsafe extern "C" fn stmt_close(stmt: *mut Stmt) {
    if stmt.is_null() {
        return;
    }
    let mut wrapper = Box::from_raw(stmt);
    if wrapper._ctx.is_null() {
        // already closed
        return;
    }
    // clean up the current row if it exists
    if !wrapper.current_row.is_null() {
        wrapper.free_current_row();
    }
    // free the managed internal context
    let mut internal = Box::<Statement>::from_raw(wrapper._ctx.cast());
    internal.reset();
}
