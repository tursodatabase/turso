use std::{fmt::Display, mem::ManuallyDrop, sync::Arc};

use turso_capi_macros::signature;
use turso_core::{types::Text, Connection, Database, LimboError, Statement, StepResult};

use crate::c::{turso_slice_ref_t, turso_status_t, turso_value_t, turso_value_union_t};

mod c {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]

    include!("bindings.rs");
}

fn err_to_c_string<E: Display>(err: &E) -> *const std::ffi::c_char {
    let message = format!("{err}");
    let message = std::ffi::CString::new(message).expect("string must be zero terminated");
    message.into_raw()
}

fn turso_status_limbo_err(err: &LimboError) -> turso_status_t {
    c::turso_status_t {
        error: err_to_c_string(err),
        code: match err {
            LimboError::Constraint(_) => c::turso_status_code_t::TURSO_CONSTRAINT,
            LimboError::Corrupt(..) => c::turso_status_code_t::TURSO_CORRUPT,
            LimboError::NotADB => c::turso_status_code_t::TURSO_NOTADB,
            LimboError::DatabaseFull(_) => c::turso_status_code_t::TURSO_DATABASE_FULL,
            LimboError::ReadOnly => c::turso_status_code_t::TURSO_READONLY,
            LimboError::Busy => c::turso_status_code_t::TURSO_BUSY,
            _ => c::turso_status_code_t::TURSO_ERROR,
        },
    }
}

fn turso_status_err<E: Display>(err: &E, code: c::turso_status_code_t) -> turso_status_t {
    c::turso_status_t {
        error: err_to_c_string(err),
        code,
    }
}

fn turso_status_ok() -> turso_status_t {
    c::turso_status_t {
        error: std::ptr::null(),
        code: c::turso_status_code_t::TURSO_OK,
    }
}

fn turso_status(code: c::turso_status_code_t) -> turso_status_t {
    c::turso_status_t {
        error: std::ptr::null(),
        code,
    }
}

fn turso_slice_from_bytes(bytes: &[u8]) -> turso_slice_ref_t {
    turso_slice_ref_t {
        ptr: bytes.as_ptr() as *const std::ffi::c_void,
        len: bytes.len(),
    }
}

fn bytes_from_turso_slice(slice: turso_slice_ref_t) -> &'static [u8] {
    unsafe { std::slice::from_raw_parts(slice.ptr as *const u8, slice.len) }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_setup(config: c::turso_config_t) -> c::turso_status_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_database_init(config: c::turso_database_config_t) -> c::turso_database_t {
    let filename_cstr = unsafe { std::ffi::CStr::from_ptr(config.path) };
    let filename_str = match filename_cstr.to_str() {
        Ok(s) => s,
        Err(err) => {
            return c::turso_database_t {
                status: turso_status_err(&err, c::turso_status_code_t::TURSO_ERROR),
                inner: std::ptr::null_mut(),
            }
        }
    };
    let io: Arc<dyn turso_core::IO> = match filename_str {
        ":memory:" => Arc::new(turso_core::MemoryIO::new()),
        _ => match turso_core::PlatformIO::new() {
            Ok(io) => Arc::new(io),
            Err(err) => {
                return c::turso_database_t {
                    status: turso_status_err(&err, c::turso_status_code_t::TURSO_ERROR),
                    inner: std::ptr::null_mut(),
                }
            }
        },
    };
    match turso_core::Database::open_file(io.clone(), filename_str, false, true) {
        Ok(db) => c::turso_database_t {
            status: turso_status_ok(),
            inner: Arc::into_raw(db) as *mut std::ffi::c_void,
        },
        Err(err) => c::turso_database_t {
            status: turso_status_err(&err, c::turso_status_code_t::TURSO_ERROR),
            inner: std::ptr::null_mut(),
        },
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_database_connect(database: c::turso_database_t) -> c::turso_connection_t {
    let db = unsafe { ManuallyDrop::new(Arc::from_raw(database.inner as *const Database)) };
    let connection = db.connect();

    match connection {
        Ok(connection) => c::turso_connection_t {
            status: turso_status_ok(),
            inner: Arc::into_raw(connection) as *mut std::ffi::c_void,
        },
        Err(err) => c::turso_connection_t {
            status: turso_status_limbo_err(&err),
            inner: std::ptr::null_mut(),
        },
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_connection_prepare(
    connection: c::turso_connection_t,
    sql: c::turso_slice_ref_t,
) -> c::turso_statement_t {
    let sql = unsafe { std::slice::from_raw_parts(sql.ptr as *const u8, sql.len) };
    let sql = match std::str::from_utf8(sql) {
        Ok(sql) => sql,
        Err(err) => {
            return c::turso_statement_t {
                status: turso_status_err(&err, c::turso_status_code_t::TURSO_ERROR),
                inner: std::ptr::null_mut(),
            }
        }
    };

    let connection =
        unsafe { ManuallyDrop::new(Arc::from_raw(connection.inner as *const Connection)) };
    let statement = connection.prepare(sql);

    let statement = match statement {
        Ok(statement) => statement,
        Err(err) => {
            return c::turso_statement_t {
                status: turso_status_limbo_err(&err),
                inner: std::ptr::null_mut(),
            }
        }
    };

    let statement = Box::new(statement);
    c::turso_statement_t {
        status: turso_status_ok(),
        inner: Box::into_raw(statement) as *mut std::ffi::c_void,
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_io(statement: c::turso_statement_t) -> c::turso_status_t {
    let statement =
        unsafe { ManuallyDrop::new(Arc::from_raw(statement.inner as *const Statement)) };
    let result = statement.run_once();

    match result {
        Ok(()) => turso_status_ok(),
        Err(err) => turso_status_limbo_err(&err),
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_execute(statement: c::turso_statement_t) -> c::turso_execute_t {
    let statement = unsafe { &mut *(statement.inner as *mut Statement) };
    loop {
        return match statement.step() {
            Ok(StepResult::Row) => continue,
            Ok(StepResult::Done) => c::turso_execute_t {
                status: turso_status_ok(),
                rows_changed: statement.n_change() as u64,
            },
            Ok(StepResult::IO) => c::turso_execute_t {
                status: turso_status(c::turso_status_code_t::TURSO_IO),
                rows_changed: 0,
            },
            Ok(StepResult::Interrupt) => c::turso_execute_t {
                status: turso_status(c::turso_status_code_t::TURSO_INTERRUPT),
                rows_changed: 0,
            },
            Ok(StepResult::Busy) => c::turso_execute_t {
                status: turso_status(c::turso_status_code_t::TURSO_BUSY),
                rows_changed: 0,
            },
            Err(err) => c::turso_execute_t {
                status: turso_status_limbo_err(&err),
                rows_changed: 0,
            },
        };
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_query(statement: c::turso_statement_t) -> c::turso_rows_t {
    c::turso_rows_t {
        status: turso_status_ok(),
        inner: statement.inner,
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_reset(statement: c::turso_statement_t) -> c::turso_status_t {
    let statement = unsafe { &mut *(statement.inner as *mut Statement) };
    statement.reset();
    turso_status_ok()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_column_name(
    rows: c::turso_statement_t,
    index: std::ffi::c_int,
) -> c::turso_slice_ref_t {
    let statement = unsafe { &mut *(rows.inner as *mut Statement) };
    let column = statement.get_column_name(index as usize);
    turso_slice_from_bytes(column.as_bytes())
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_column_count(rows: c::turso_statement_t) -> std::ffi::c_int {
    let statement = unsafe { &mut *(rows.inner as *mut Statement) };
    statement.num_columns() as _
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_rows_next(rows: c::turso_rows_t) -> c::turso_row_t {
    let statement = unsafe { &mut *(rows.inner as *mut Statement) };
    return match statement.step() {
        Ok(StepResult::Row) => c::turso_row_t {
            status: turso_status(c::turso_status_code_t::TURSO_ROW),
            inner: rows.inner,
        },
        Ok(StepResult::Done) => c::turso_row_t {
            status: turso_status(c::turso_status_code_t::TURSO_DONE),
            inner: std::ptr::null_mut(),
        },
        Ok(StepResult::IO) => c::turso_row_t {
            status: turso_status(c::turso_status_code_t::TURSO_IO),
            inner: std::ptr::null_mut(),
        },
        Ok(StepResult::Interrupt) => c::turso_row_t {
            status: turso_status(c::turso_status_code_t::TURSO_INTERRUPT),
            inner: std::ptr::null_mut(),
        },
        Ok(StepResult::Busy) => c::turso_row_t {
            status: turso_status(c::turso_status_code_t::TURSO_BUSY),
            inner: std::ptr::null_mut(),
        },
        Err(err) => c::turso_row_t {
            status: turso_status_limbo_err(&err),
            inner: std::ptr::null_mut(),
        },
    };
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_row_value(
    row: c::turso_row_t,
    index: std::ffi::c_int,
) -> c::turso_result_value_t {
    let statement = unsafe { &mut *(row.inner as *mut Statement) };
    let row = statement.row().unwrap();
    let value = row.get_value(index as usize);
    match value {
        turso_core::Value::Null => c::turso_result_value_t {
            status: turso_status_ok(),
            ok: turso_value_t {
                type_: c::turso_type_t::TURSO_TYPE_NULL,
                ..Default::default()
            },
        },
        turso_core::Value::Integer(value) => c::turso_result_value_t {
            status: turso_status_ok(),
            ok: turso_value_t {
                type_: c::turso_type_t::TURSO_TYPE_INTEGER,
                value: turso_value_union_t { integer: *value },
            },
        },
        turso_core::Value::Float(value) => c::turso_result_value_t {
            status: turso_status_ok(),
            ok: turso_value_t {
                type_: c::turso_type_t::TURSO_TYPE_REAL,
                value: turso_value_union_t { real: *value },
            },
        },
        turso_core::Value::Text(text) => c::turso_result_value_t {
            status: turso_status_ok(),
            ok: turso_value_t {
                type_: c::turso_type_t::TURSO_TYPE_TEXT,
                value: turso_value_union_t {
                    text: turso_slice_from_bytes(text.as_str().as_bytes()),
                },
            },
        },
        turso_core::Value::Blob(items) => c::turso_result_value_t {
            status: turso_status_ok(),
            ok: turso_value_t {
                type_: c::turso_type_t::TURSO_TYPE_BLOB,
                value: turso_value_union_t {
                    blob: turso_slice_from_bytes(&items),
                },
            },
        },
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_bind_named(
    stmt: c::turso_statement_t,
    name: c::turso_slice_ref_t,
    value: c::turso_value_t,
) -> c::turso_status_t {
    let name = unsafe { std::slice::from_raw_parts(name.ptr as *const u8, name.len) };
    let name = match std::str::from_utf8(name) {
        Ok(name) => name,
        Err(err) => {
            return turso_status_err(&err, c::turso_status_code_t::TURSO_ERROR);
        }
    };

    let statement = unsafe { &mut *(stmt.inner as *mut Statement) };
    let parameters = statement.parameters();
    for i in 1..=parameters.count() {
        let parameter = parameters.name(i.try_into().unwrap()).unwrap();
        assert!(
            parameter.starts_with(":")
                || parameter.starts_with("@")
                || parameter.starts_with("$")
                || parameter.starts_with("?")
        );
        if name == &parameter[1..] {
            return turso_statement_bind_positional(stmt, i as std::ffi::c_int, value);
        }
    }
    turso_status(c::turso_status_code_t::TURSO_ERROR)
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_bind_positional(
    stmt: c::turso_statement_t,
    position: std::ffi::c_int,
    value: c::turso_value_t,
) -> c::turso_status_t {
    let statement = unsafe { &mut *(stmt.inner as *mut Statement) };

    if position <= 0 {
        return turso_status(c::turso_status_code_t::TURSO_MISUSE);
    }

    statement.bind_at(
        (position as usize).try_into().unwrap(),
        match value.type_ {
            c::turso_type_t::TURSO_TYPE_NULL => turso_core::Value::Null,
            c::turso_type_t::TURSO_TYPE_INTEGER => {
                turso_core::Value::Integer(unsafe { value.value.integer })
            }
            c::turso_type_t::TURSO_TYPE_REAL => {
                turso_core::Value::Float(unsafe { value.value.real })
            }
            c::turso_type_t::TURSO_TYPE_TEXT => {
                let text = std::str::from_utf8(bytes_from_turso_slice(unsafe { value.value.text }));
                let text = match text {
                    Ok(text) => text,
                    Err(err) => {
                        return turso_status_err(&err, c::turso_status_code_t::TURSO_ERROR);
                    }
                };
                turso_core::Value::Text(Text::new(text))
            }
            c::turso_type_t::TURSO_TYPE_BLOB => {
                let blob = bytes_from_turso_slice(unsafe { value.value.blob });
                turso_core::Value::Blob(blob.to_vec())
            }
        },
    );

    turso_status_ok()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_integer(integer: i64) -> c::turso_value_t {
    turso_value_t {
        type_: c::turso_type_t::TURSO_TYPE_INTEGER,
        value: turso_value_union_t { integer },
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_real(real: f64) -> c::turso_value_t {
    turso_value_t {
        type_: c::turso_type_t::TURSO_TYPE_REAL,
        value: turso_value_union_t { real },
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_text(ptr: *const std::ffi::c_char, len: usize) -> c::turso_value_t {
    turso_value_t {
        type_: c::turso_type_t::TURSO_TYPE_TEXT,
        value: turso_value_union_t {
            text: turso_slice_ref_t {
                ptr: ptr as *const std::ffi::c_void,
                len,
            },
        },
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_blob(ptr: *const u8, len: usize) -> c::turso_value_t {
    turso_value_t {
        type_: c::turso_type_t::TURSO_TYPE_BLOB,
        value: turso_value_union_t {
            blob: turso_slice_ref_t {
                ptr: ptr as *const std::ffi::c_void,
                len,
            },
        },
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_null() -> c::turso_value_t {
    turso_value_t {
        type_: c::turso_type_t::TURSO_TYPE_NULL,
        ..Default::default()
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_status_deinit(status: c::turso_status_t) {
    if !status.error.is_null() {
        let _ = unsafe { std::ffi::CString::from_raw(status.error as *mut std::ffi::c_char) };
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_database_deinit(db: c::turso_database_t) {
    turso_status_deinit(db.status);
    if !db.inner.is_null() {
        let _ = unsafe { Arc::from_raw(db.inner as *const Database) };
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_connection_deinit(connection: c::turso_connection_t) {
    turso_status_deinit(connection.status);
    if !connection.inner.is_null() {
        let _ = unsafe { Arc::from_raw(connection.inner as *const Connection) };
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_deinit(statement: c::turso_statement_t) {
    turso_status_deinit(statement.status);
    if !statement.inner.is_null() {
        let _ = unsafe { Box::from_raw(statement.inner as *mut Statement) };
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_rows_deinit(rows: c::turso_rows_t) {
    turso_status_deinit(rows.status);
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_row_deinit(row: c::turso_row_t) {
    turso_status_deinit(row.status);
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use turso_core::types::Text;

    use crate::{
        bytes_from_turso_slice,
        c::{
            turso_connection_deinit, turso_connection_prepare, turso_database_config_t,
            turso_database_connect, turso_database_deinit, turso_database_init, turso_row_deinit,
            turso_row_value, turso_rows_deinit, turso_rows_next, turso_slice_ref_t,
            turso_statement_bind_named, turso_statement_deinit, turso_statement_execute,
            turso_statement_io, turso_statement_query, turso_status_code_t, turso_status_t,
            turso_value_t, turso_value_union_t,
        },
        turso_slice_from_bytes, turso_statement_bind_positional, turso_statement_column_count,
    };

    unsafe fn error(status: &turso_status_t) -> &str {
        std::ffi::CStr::from_ptr(status.error).to_str().unwrap()
    }

    fn convert_value(value: turso_value_t) -> turso_core::Value {
        match value.type_ {
            crate::c::turso_type_t::TURSO_TYPE_NULL => turso_core::Value::Null,
            crate::c::turso_type_t::TURSO_TYPE_INTEGER => {
                turso_core::Value::Integer(unsafe { value.value.integer })
            }
            crate::c::turso_type_t::TURSO_TYPE_REAL => {
                turso_core::Value::Float(unsafe { value.value.real })
            }
            crate::c::turso_type_t::TURSO_TYPE_TEXT => {
                let text = bytes_from_turso_slice(unsafe { value.value.text });
                let text = std::str::from_utf8(text).unwrap();
                turso_core::Value::Text(Text::new(text))
            }
            crate::c::turso_type_t::TURSO_TYPE_BLOB => {
                let blob = bytes_from_turso_slice(unsafe { value.value.blob });
                turso_core::Value::Blob(blob.to_vec())
            }
        }
    }

    #[test]
    pub fn test_db_init() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_init(turso_database_config_t {
                path: path.as_ptr(),
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);
            turso_database_deinit(db);
        }
    }

    #[test]
    pub fn test_db_error() {
        unsafe {
            let path = CString::new("not/existing/path").unwrap();
            let db = turso_database_init(turso_database_config_t {
                path: path.as_ptr(),
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_ERROR);
            assert_eq!(error(&db.status), "I/O error: entity not found");
            turso_database_deinit(db);
        }
    }

    #[test]
    pub fn test_db_conn_init() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_init(turso_database_config_t {
                path: path.as_ptr(),
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);
            let conn = turso_database_connect(db);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);
            turso_connection_deinit(conn);
            turso_database_deinit(db);
        }
    }

    #[test]
    pub fn test_db_stmt_prepare() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_init(turso_database_config_t {
                path: path.as_ptr(),
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);
            let conn = turso_database_connect(db);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "SELECT NULL, 2, 3.14, '5', x'06'";
            let stmt = turso_connection_prepare(
                conn,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);
            turso_statement_deinit(stmt);
            turso_connection_deinit(conn);
            turso_database_deinit(db);
        }
    }

    #[test]
    pub fn test_db_stmt_prepare_parse_error() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_init(turso_database_config_t {
                path: path.as_ptr(),
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);
            let conn = turso_database_connect(db);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "SELECT nil";
            let stmt = turso_connection_prepare(
                conn,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_ERROR);
            assert_eq!(error(&stmt.status), "Parse error: no such column: nil");
            turso_statement_deinit(stmt);
            turso_connection_deinit(conn);
            turso_database_deinit(db);
        }
    }

    #[test]
    pub fn test_db_stmt_execute() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_init(turso_database_config_t {
                path: path.as_ptr(),
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);
            let conn = turso_database_connect(db);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "CREATE TABLE t(x)";
            let stmt = turso_connection_prepare(
                conn,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);
            while turso_statement_execute(stmt).status.code != turso_status_code_t::TURSO_OK {
                let io_result = turso_statement_io(stmt).code;
                assert_eq!(io_result, turso_status_code_t::TURSO_OK);
            }
            turso_statement_deinit(stmt);

            let stmt = turso_connection_prepare(
                conn,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_ERROR);
            assert_eq!(error(&stmt.status), "Parse error: Table t already exists");
            turso_statement_deinit(stmt);

            turso_connection_deinit(conn);
            turso_database_deinit(db);
        }
    }

    #[test]
    pub fn test_db_stmt_query() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_init(turso_database_config_t {
                path: path.as_ptr(),
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);
            let conn = turso_database_connect(db);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "SELECT NULL, 2, 3.14, '5', x'06'";
            let stmt = turso_connection_prepare(
                conn,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);

            let columns = turso_statement_column_count(stmt);
            assert_eq!(columns, 5);

            let rows = turso_statement_query(stmt);
            assert_eq!(rows.status.code, turso_status_code_t::TURSO_OK);
            let mut collected = vec![];
            loop {
                let row = turso_rows_next(rows);
                if row.status.code == turso_status_code_t::TURSO_IO {
                    let io_result = turso_statement_io(stmt).code;
                    assert_eq!(io_result, turso_status_code_t::TURSO_OK);
                    turso_row_deinit(row);
                    continue;
                }
                if row.status.code == turso_status_code_t::TURSO_DONE {
                    turso_row_deinit(row);
                    break;
                }
                if row.status.code == turso_status_code_t::TURSO_ROW {
                    for i in 0..columns {
                        let row_value = turso_row_value(row, i);
                        assert_eq!(row_value.status.code, turso_status_code_t::TURSO_OK);
                        collected.push(convert_value(row_value.ok));
                    }
                    turso_row_deinit(row);
                    continue;
                }
                assert!(false);
            }

            assert_eq!(
                collected,
                vec![
                    turso_core::Value::Null,
                    turso_core::Value::Integer(2),
                    turso_core::Value::Float(3.14),
                    turso_core::Value::Text(Text::new("5")),
                    turso_core::Value::Blob(vec![6]),
                ]
            );

            turso_rows_deinit(rows);
            turso_statement_deinit(stmt);
        }
    }

    #[test]
    pub fn test_db_stmt_bind_positional() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_init(turso_database_config_t {
                path: path.as_ptr(),
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);
            let conn = turso_database_connect(db);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "SELECT ?, ?, ?, ?, ?";
            let stmt = turso_connection_prepare(
                conn,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);

            let columns = turso_statement_column_count(stmt);
            assert_eq!(columns, 5);

            assert_eq!(
                turso_statement_bind_positional(
                    stmt,
                    1,
                    turso_value_t {
                        type_: crate::c::turso_type_t::TURSO_TYPE_NULL,
                        ..Default::default()
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            assert_eq!(
                turso_statement_bind_positional(
                    stmt,
                    2,
                    turso_value_t {
                        type_: crate::c::turso_type_t::TURSO_TYPE_INTEGER,
                        value: turso_value_union_t { integer: 2 }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            assert_eq!(
                turso_statement_bind_positional(
                    stmt,
                    3,
                    turso_value_t {
                        type_: crate::c::turso_type_t::TURSO_TYPE_REAL,
                        value: turso_value_union_t { real: 3.14 }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            assert_eq!(
                turso_statement_bind_positional(
                    stmt,
                    4,
                    turso_value_t {
                        type_: crate::c::turso_type_t::TURSO_TYPE_TEXT,
                        value: turso_value_union_t {
                            text: turso_slice_from_bytes("5".as_bytes())
                        }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            let bytes = vec![6];
            assert_eq!(
                turso_statement_bind_positional(
                    stmt,
                    5,
                    turso_value_t {
                        type_: crate::c::turso_type_t::TURSO_TYPE_BLOB,
                        value: turso_value_union_t {
                            blob: turso_slice_from_bytes(&bytes)
                        }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );

            let rows = turso_statement_query(stmt);
            assert_eq!(rows.status.code, turso_status_code_t::TURSO_OK);
            let mut collected = vec![];
            loop {
                let row = turso_rows_next(rows);
                if row.status.code == turso_status_code_t::TURSO_IO {
                    let io_result = turso_statement_io(stmt).code;
                    assert_eq!(io_result, turso_status_code_t::TURSO_OK);
                    turso_row_deinit(row);
                    continue;
                }
                if row.status.code == turso_status_code_t::TURSO_DONE {
                    turso_row_deinit(row);
                    break;
                }
                if row.status.code == turso_status_code_t::TURSO_ROW {
                    for i in 0..columns {
                        let row_value = turso_row_value(row, i);
                        assert_eq!(row_value.status.code, turso_status_code_t::TURSO_OK);
                        collected.push(convert_value(row_value.ok));
                    }
                    turso_row_deinit(row);
                    continue;
                }
                assert!(false);
            }

            assert_eq!(
                collected,
                vec![
                    turso_core::Value::Null,
                    turso_core::Value::Integer(2),
                    turso_core::Value::Float(3.14),
                    turso_core::Value::Text(Text::new("5")),
                    turso_core::Value::Blob(vec![6]),
                ]
            );

            turso_rows_deinit(rows);
            turso_statement_deinit(stmt);
        }
    }

    #[test]
    pub fn test_db_stmt_bind_named() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_init(turso_database_config_t {
                path: path.as_ptr(),
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);
            let conn = turso_database_connect(db);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "SELECT :e, :d, :c, :b, :a";
            let stmt = turso_connection_prepare(
                conn,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);

            let columns = turso_statement_column_count(stmt);
            assert_eq!(columns, 5);

            assert_eq!(
                turso_statement_bind_named(
                    stmt,
                    turso_slice_from_bytes("e".as_bytes()),
                    turso_value_t {
                        type_: crate::c::turso_type_t::TURSO_TYPE_NULL,
                        ..Default::default()
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            assert_eq!(
                turso_statement_bind_named(
                    stmt,
                    turso_slice_from_bytes("d".as_bytes()),
                    turso_value_t {
                        type_: crate::c::turso_type_t::TURSO_TYPE_INTEGER,
                        value: turso_value_union_t { integer: 2 }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            assert_eq!(
                turso_statement_bind_named(
                    stmt,
                    turso_slice_from_bytes("c".as_bytes()),
                    turso_value_t {
                        type_: crate::c::turso_type_t::TURSO_TYPE_REAL,
                        value: turso_value_union_t { real: 3.14 }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            assert_eq!(
                turso_statement_bind_named(
                    stmt,
                    turso_slice_from_bytes("b".as_bytes()),
                    turso_value_t {
                        type_: crate::c::turso_type_t::TURSO_TYPE_TEXT,
                        value: turso_value_union_t {
                            text: turso_slice_from_bytes("5".as_bytes())
                        }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            let bytes = vec![6];
            assert_eq!(
                turso_statement_bind_named(
                    stmt,
                    turso_slice_from_bytes("a".as_bytes()),
                    turso_value_t {
                        type_: crate::c::turso_type_t::TURSO_TYPE_BLOB,
                        value: turso_value_union_t {
                            blob: turso_slice_from_bytes(&bytes)
                        }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );

            let rows = turso_statement_query(stmt);
            assert_eq!(rows.status.code, turso_status_code_t::TURSO_OK);
            let mut collected = vec![];
            loop {
                let row = turso_rows_next(rows);
                if row.status.code == turso_status_code_t::TURSO_IO {
                    let io_result = turso_statement_io(stmt).code;
                    assert_eq!(io_result, turso_status_code_t::TURSO_OK);
                    turso_row_deinit(row);
                    continue;
                }
                if row.status.code == turso_status_code_t::TURSO_DONE {
                    turso_row_deinit(row);
                    break;
                }
                if row.status.code == turso_status_code_t::TURSO_ROW {
                    for i in 0..columns {
                        let row_value = turso_row_value(row, i);
                        assert_eq!(row_value.status.code, turso_status_code_t::TURSO_OK);
                        collected.push(convert_value(row_value.ok));
                    }
                    turso_row_deinit(row);
                    continue;
                }
                assert!(false);
            }

            assert_eq!(
                collected,
                vec![
                    turso_core::Value::Null,
                    turso_core::Value::Integer(2),
                    turso_core::Value::Float(3.14),
                    turso_core::Value::Text(Text::new("5")),
                    turso_core::Value::Blob(vec![6]),
                ]
            );

            turso_rows_deinit(rows);
            turso_statement_deinit(stmt);
        }
    }
}
