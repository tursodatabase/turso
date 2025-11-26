use std::mem::ManuallyDrop;

use turso_sdk_kit_macros::signature;

use c::{turso_slice_ref_t, turso_status_t, turso_value_t, turso_value_union_t};

use crate::{
    capi::c::turso_database_t,
    rsapi::{
        self, c_string_to_str, str_from_turso_slice, str_to_c_string, turso_slice_from_bytes,
        value_from_c_value, TursoConnection, TursoDatabase, TursoStatement,
    },
};

pub mod c {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]

    include!("bindings.rs");
}

fn turso_status_ok() -> turso_status_t {
    c::turso_status_t {
        error: std::ptr::null(),
        code: c::turso_status_code_t::TURSO_OK,
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_setup(config: c::turso_config_t) -> c::turso_status_t {
    let config = match rsapi::TursoSetupConfig::from_capi(config) {
        Ok(config) => config,
        Err(err) => return err.to_capi(),
    };
    match rsapi::turso_setup(config) {
        Ok(()) => turso_status_ok(),
        Err(err) => err.to_capi(),
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_database_create(
    config: c::turso_database_config_t,
) -> c::turso_database_create_result_t {
    let config = match rsapi::TursoDatabaseConfig::from_capi(config) {
        Ok(config) => config,
        Err(err) => {
            return c::turso_database_create_result_t {
                status: err.to_capi(),
                ..Default::default()
            }
        }
    };
    c::turso_database_create_result_t {
        status: turso_status_ok(),
        database: rsapi::TursoDatabase::create(config).to_capi(),
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_database_open(database: turso_database_t) -> c::turso_status_t {
    let database = ManuallyDrop::new(unsafe { TursoDatabase::from_capi(database) });
    match database.open() {
        Ok(()) => turso_status_ok(),
        Err(err) => err.to_capi(),
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_database_connect(
    database: c::turso_database_t,
) -> c::turso_database_connect_result_t {
    let database = ManuallyDrop::new(unsafe { TursoDatabase::from_capi(database) });
    match database.connect() {
        Ok(connection) => c::turso_database_connect_result_t {
            status: turso_status_ok(),
            connection: connection.to_capi(),
        },
        Err(err) => c::turso_database_connect_result_t {
            status: err.to_capi(),
            ..Default::default()
        },
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_connection_prepare_single(
    connection: c::turso_connection_t,
    sql: c::turso_slice_ref_t,
) -> c::turso_connection_prepare_single_t {
    let connection = ManuallyDrop::new(unsafe { TursoConnection::from_capi(connection) });

    let sql = match str_from_turso_slice(sql) {
        Ok(sql) => sql,
        Err(err) => {
            return c::turso_connection_prepare_single_t {
                status: err.to_capi(),
                ..Default::default()
            }
        }
    };
    match connection.prepare_single(sql) {
        Ok(statement) => c::turso_connection_prepare_single_t {
            status: turso_status_ok(),
            statement: statement.to_capi(),
        },
        Err(err) => c::turso_connection_prepare_single_t {
            status: err.to_capi(),
            ..Default::default()
        },
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_connection_prepare_first(
    connection: c::turso_connection_t,
    sql: c::turso_slice_ref_t,
) -> c::turso_connection_prepare_first_t {
    let connection = ManuallyDrop::new(unsafe { TursoConnection::from_capi(connection) });

    let sql = match str_from_turso_slice(sql) {
        Ok(sql) => sql,
        Err(err) => {
            return c::turso_connection_prepare_first_t {
                status: err.to_capi(),
                ..Default::default()
            }
        }
    };
    match connection.prepare_first(sql) {
        Ok(Some((statement, tail_idx))) => c::turso_connection_prepare_first_t {
            status: turso_status_ok(),
            statement: statement.to_capi(),
            tail_idx: tail_idx,
        },
        Ok(None) => c::turso_connection_prepare_first_t {
            status: turso_status_ok(),
            ..Default::default()
        },
        Err(err) => c::turso_connection_prepare_first_t {
            status: err.to_capi(),
            ..Default::default()
        },
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_connection_prepare_first_result_empty(
    result: c::turso_connection_prepare_first_t,
) -> bool {
    result.statement.inner.is_null()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_run_io(statement: c::turso_statement_t) -> c::turso_status_t {
    let statement = ManuallyDrop::new(unsafe { TursoStatement::from_capi(statement) });
    match statement.run_io() {
        Ok(()) => turso_status_ok(),
        Err(err) => err.to_capi(),
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_execute(
    statement: c::turso_statement_t,
) -> c::turso_statement_execute_t {
    let mut statement = ManuallyDrop::new(unsafe { TursoStatement::from_capi(statement) });
    match statement.execute() {
        Ok(result) => c::turso_statement_execute_t {
            status: result.status.to_capi(),
            rows_changed: result.rows_changed,
        },
        Err(err) => c::turso_statement_execute_t {
            status: err.to_capi(),
            ..Default::default()
        },
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_step(statement: c::turso_statement_t) -> c::turso_status_t {
    let mut statement = ManuallyDrop::new(unsafe { TursoStatement::from_capi(statement) });
    match statement.step() {
        Ok(status) => status.to_capi(),
        Err(err) => err.to_capi(),
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_reset(statement: c::turso_statement_t) -> c::turso_status_t {
    let mut statement = ManuallyDrop::new(unsafe { TursoStatement::from_capi(statement) });
    match statement.reset() {
        Ok(()) => turso_status_ok(),
        Err(err) => err.to_capi(),
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_finalize(statement: c::turso_statement_t) -> c::turso_status_t {
    let mut statement = ManuallyDrop::new(unsafe { TursoStatement::from_capi(statement) });
    match statement.finalize() {
        Ok(status) => status.to_capi(),
        Err(err) => err.to_capi(),
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_column_name(
    statement: c::turso_statement_t,
    index: usize,
) -> *const std::ffi::c_char {
    let statement = ManuallyDrop::new(unsafe { TursoStatement::from_capi(statement) });
    let column = statement.column_name(index).to_string();
    str_to_c_string(&column)
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_column_count(statement: c::turso_statement_t) -> usize {
    let statement = ManuallyDrop::new(unsafe { TursoStatement::from_capi(statement) });
    statement.column_count()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_row_value(
    statement: c::turso_statement_t,
    index: usize,
) -> c::turso_statement_row_value_t {
    let statement = ManuallyDrop::new(unsafe { TursoStatement::from_capi(statement) });
    let value = statement.row_value(index);
    match value {
        Ok(turso_core::ValueRef::Null) => c::turso_statement_row_value_t {
            status: turso_status_ok(),
            value: turso_value_t {
                type_: c::turso_type_t::TURSO_TYPE_NULL,
                ..Default::default()
            },
        },
        Ok(turso_core::ValueRef::Integer(value)) => c::turso_statement_row_value_t {
            status: turso_status_ok(),
            value: turso_value_t {
                type_: c::turso_type_t::TURSO_TYPE_INTEGER,
                value: turso_value_union_t { integer: value },
            },
        },
        Ok(turso_core::ValueRef::Float(value)) => c::turso_statement_row_value_t {
            status: turso_status_ok(),
            value: turso_value_t {
                type_: c::turso_type_t::TURSO_TYPE_REAL,
                value: turso_value_union_t { real: value },
            },
        },
        Ok(turso_core::ValueRef::Text(text)) => c::turso_statement_row_value_t {
            status: turso_status_ok(),
            value: turso_value_t {
                type_: c::turso_type_t::TURSO_TYPE_TEXT,
                value: turso_value_union_t {
                    text: turso_slice_from_bytes(text.as_str().as_bytes()),
                },
            },
        },
        Ok(turso_core::ValueRef::Blob(items)) => c::turso_statement_row_value_t {
            status: turso_status_ok(),
            value: turso_value_t {
                type_: c::turso_type_t::TURSO_TYPE_BLOB,
                value: turso_value_union_t {
                    blob: turso_slice_from_bytes(&items),
                },
            },
        },
        Err(err) => c::turso_statement_row_value_t {
            status: err.to_capi(),
            ..Default::default()
        },
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_bind_named(
    statement: c::turso_statement_t,
    name: c::turso_slice_ref_t,
    value: c::turso_value_t,
) -> c::turso_status_t {
    let name = match str_from_turso_slice(name) {
        Ok(name) => name,
        Err(err) => return err.to_capi(),
    };
    let value = match value_from_c_value(value) {
        Ok(value) => value,
        Err(err) => return err.to_capi(),
    };
    let mut statement = ManuallyDrop::new(unsafe { TursoStatement::from_capi(statement) });
    match statement.bind_named(name, value) {
        Ok(()) => turso_status_ok(),
        Err(err) => err.to_capi(),
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_bind_positional(
    statement: c::turso_statement_t,
    position: usize,
    value: c::turso_value_t,
) -> c::turso_status_t {
    let value = match value_from_c_value(value) {
        Ok(value) => value,
        Err(err) => return err.to_capi(),
    };
    let mut statement = ManuallyDrop::new(unsafe { TursoStatement::from_capi(statement) });
    match statement.bind_positional(position, value) {
        Ok(()) => turso_status_ok(),
        Err(err) => err.to_capi(),
    }
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
    turso_str_deinit(status.error);
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_str_deinit(s: *const std::ffi::c_char) {
    if !s.is_null() {
        drop(c_string_to_str(s));
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_database_deinit(database: c::turso_database_t) {
    if !database.inner.is_null() {
        drop(unsafe { TursoDatabase::from_capi(database) })
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_connection_deinit(connection: c::turso_connection_t) {
    if !connection.inner.is_null() {
        drop(unsafe { TursoConnection::from_capi(connection) })
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_deinit(statement: c::turso_statement_t) {
    if !statement.inner.is_null() {
        drop(unsafe { TursoStatement::from_capi(statement) })
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use turso_core::types::Text;

    use crate::{
        capi::{
            self,
            c::{
                turso_config_t, turso_connection_deinit, turso_connection_prepare_first,
                turso_connection_prepare_first_result_empty, turso_connection_prepare_single,
                turso_database_config_t, turso_database_connect, turso_database_create,
                turso_database_deinit, turso_database_open, turso_log_t, turso_setup,
                turso_slice_ref_t, turso_statement_bind_named, turso_statement_bind_positional,
                turso_statement_column_count, turso_statement_deinit, turso_statement_execute,
                turso_statement_finalize, turso_statement_row_value, turso_statement_run_io,
                turso_statement_step, turso_status_code_t, turso_status_deinit, turso_status_t,
                turso_value_t, turso_value_union_t,
            },
        },
        rsapi::{turso_slice_from_bytes, value_from_c_value},
    };

    unsafe fn error(status: &turso_status_t) -> &str {
        std::ffi::CStr::from_ptr(status.error).to_str().unwrap()
    }

    extern "C" fn logger(log: turso_log_t) {
        println!("log: {:?}", unsafe {
            std::ffi::CStr::from_ptr(log.message)
        });
    }

    #[test]
    pub fn test_db_setup() {
        unsafe {
            turso_setup(turso_config_t {
                logger: Some(logger),
                log_level: b"debug\0".as_ptr(),
            })
        };
    }

    #[test]
    pub fn test_db_init() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_create(turso_database_config_t {
                path: path.as_ptr(),
                ..Default::default()
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);

            let status = turso_database_open(db.database);
            assert_eq!(status.code, turso_status_code_t::TURSO_OK);

            turso_database_deinit(db.database);
        }
    }

    #[test]
    pub fn test_db_error() {
        unsafe {
            let path = CString::new("not/existing/path").unwrap();
            let db = turso_database_create(turso_database_config_t {
                path: path.as_ptr(),
                ..Default::default()
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);

            let status = turso_database_open(db.database);

            assert_eq!(status.code, turso_status_code_t::TURSO_ERROR);
            assert_eq!(error(&status), "I/O error: entity not found");

            turso_status_deinit(status);
            turso_database_deinit(db.database);
        }
    }

    #[test]
    pub fn test_db_conn_init() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_create(turso_database_config_t {
                path: path.as_ptr(),
                ..Default::default()
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);

            let status = turso_database_open(db.database);
            assert_eq!(status.code, turso_status_code_t::TURSO_OK);

            let conn = turso_database_connect(db.database);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);
            turso_connection_deinit(conn.connection);
            turso_database_deinit(db.database);
        }
    }

    #[test]
    pub fn test_db_stmt_prepare() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_create(turso_database_config_t {
                path: path.as_ptr(),
                ..Default::default()
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);

            let status = turso_database_open(db.database);
            assert_eq!(status.code, turso_status_code_t::TURSO_OK);

            let conn = turso_database_connect(db.database);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "SELECT NULL, 2, 3.14, '5', x'06'";
            let stmt = turso_connection_prepare_single(
                conn.connection,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);
            turso_statement_deinit(stmt.statement);
            turso_connection_deinit(conn.connection);
            turso_database_deinit(db.database);
        }
    }

    #[test]
    pub fn test_db_stmt_prepare_parse_error() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_create(turso_database_config_t {
                path: path.as_ptr(),
                ..Default::default()
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);

            let status = turso_database_open(db.database);
            assert_eq!(status.code, turso_status_code_t::TURSO_OK);

            let conn = turso_database_connect(db.database);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "SELECT nil";
            let stmt = turso_connection_prepare_single(
                conn.connection,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_ERROR);
            assert_eq!(error(&stmt.status), "Parse error: no such column: nil");
            turso_status_deinit(stmt.status);

            turso_statement_deinit(stmt.statement);
            turso_connection_deinit(conn.connection);
            turso_database_deinit(db.database);
        }
    }

    #[test]
    pub fn test_db_stmt_execute() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_create(turso_database_config_t {
                path: path.as_ptr(),
                ..Default::default()
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);

            let status = turso_database_open(db.database);
            assert_eq!(status.code, turso_status_code_t::TURSO_OK);

            let conn = turso_database_connect(db.database);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "CREATE TABLE t(x)";
            let stmt = turso_connection_prepare_single(
                conn.connection,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);
            loop {
                let status = turso_statement_execute(stmt.statement).status.code;
                if status == turso_status_code_t::TURSO_OK {
                    break;
                }
                let io_result = turso_statement_run_io(stmt.statement).code;
                assert_eq!(io_result, turso_status_code_t::TURSO_OK);
            }
            turso_statement_deinit(stmt.statement);

            let stmt = turso_connection_prepare_single(
                conn.connection,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_ERROR);
            assert_eq!(error(&stmt.status), "Parse error: Table t already exists");
            turso_status_deinit(stmt.status);
            turso_statement_deinit(stmt.statement);
            turso_connection_deinit(conn.connection);
            turso_database_deinit(db.database);
        }
    }

    #[test]
    pub fn test_db_stmt_query() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_create(turso_database_config_t {
                path: path.as_ptr(),
                ..Default::default()
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);

            let status = turso_database_open(db.database);
            assert_eq!(status.code, turso_status_code_t::TURSO_OK);

            let conn = turso_database_connect(db.database);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "SELECT NULL, 2, 3.14, '5', x'06'";
            let stmt = turso_connection_prepare_single(
                conn.connection,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);

            let columns = turso_statement_column_count(stmt.statement);
            assert_eq!(columns, 5);

            let mut collected = vec![];
            loop {
                let status = turso_statement_step(stmt.statement);
                if status.code == turso_status_code_t::TURSO_IO {
                    let io_result = turso_statement_run_io(stmt.statement).code;
                    assert_eq!(io_result, turso_status_code_t::TURSO_OK);
                    continue;
                }
                if status.code == turso_status_code_t::TURSO_DONE {
                    break;
                }
                if status.code == turso_status_code_t::TURSO_ROW {
                    for i in 0..columns {
                        let row_value = turso_statement_row_value(stmt.statement, i);
                        assert_eq!(row_value.status.code, turso_status_code_t::TURSO_OK);
                        collected.push(value_from_c_value(row_value.value).unwrap());
                    }
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

            turso_statement_deinit(stmt.statement);
            turso_connection_deinit(conn.connection);
            turso_database_deinit(db.database);
        }
    }

    #[test]
    pub fn test_db_stmt_bind_positional() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_create(turso_database_config_t {
                path: path.as_ptr(),
                ..Default::default()
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);

            let status = turso_database_open(db.database);
            assert_eq!(status.code, turso_status_code_t::TURSO_OK);

            let conn = turso_database_connect(db.database);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "SELECT ?, ?, ?, ?, ?";
            let stmt = turso_connection_prepare_single(
                conn.connection,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);

            let columns = turso_statement_column_count(stmt.statement);
            assert_eq!(columns, 5);

            assert_eq!(
                turso_statement_bind_positional(
                    stmt.statement,
                    1,
                    turso_value_t {
                        type_: capi::c::turso_type_t::TURSO_TYPE_NULL,
                        ..Default::default()
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            assert_eq!(
                turso_statement_bind_positional(
                    stmt.statement,
                    2,
                    turso_value_t {
                        type_: capi::c::turso_type_t::TURSO_TYPE_INTEGER,
                        value: turso_value_union_t { integer: 2 }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            assert_eq!(
                turso_statement_bind_positional(
                    stmt.statement,
                    3,
                    turso_value_t {
                        type_: capi::c::turso_type_t::TURSO_TYPE_REAL,
                        value: turso_value_union_t { real: 3.14 }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            assert_eq!(
                turso_statement_bind_positional(
                    stmt.statement,
                    4,
                    turso_value_t {
                        type_: capi::c::turso_type_t::TURSO_TYPE_TEXT,
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
                    stmt.statement,
                    5,
                    turso_value_t {
                        type_: capi::c::turso_type_t::TURSO_TYPE_BLOB,
                        value: turso_value_union_t {
                            blob: turso_slice_from_bytes(&bytes)
                        }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );

            let mut collected = vec![];
            loop {
                let status = turso_statement_step(stmt.statement);
                if status.code == turso_status_code_t::TURSO_IO {
                    let io_result = turso_statement_run_io(stmt.statement).code;
                    assert_eq!(io_result, turso_status_code_t::TURSO_OK);
                    continue;
                }
                if status.code == turso_status_code_t::TURSO_DONE {
                    break;
                }
                if status.code == turso_status_code_t::TURSO_ROW {
                    for i in 0..columns {
                        let row_value = turso_statement_row_value(stmt.statement, i);
                        assert_eq!(row_value.status.code, turso_status_code_t::TURSO_OK);
                        collected.push(value_from_c_value(row_value.value).unwrap());
                    }
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

            turso_statement_deinit(stmt.statement);
            turso_connection_deinit(conn.connection);
            turso_database_deinit(db.database);
        }
    }

    #[test]
    pub fn test_db_stmt_bind_named() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_create(turso_database_config_t {
                path: path.as_ptr(),
                ..Default::default()
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);

            let status = turso_database_open(db.database);
            assert_eq!(status.code, turso_status_code_t::TURSO_OK);

            let conn = turso_database_connect(db.database);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "SELECT :e, :d, :c, :b, :a";
            let stmt = turso_connection_prepare_single(
                conn.connection,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);

            let columns = turso_statement_column_count(stmt.statement);
            assert_eq!(columns, 5);

            assert_eq!(
                turso_statement_bind_named(
                    stmt.statement,
                    turso_slice_from_bytes("e".as_bytes()),
                    turso_value_t {
                        type_: capi::c::turso_type_t::TURSO_TYPE_NULL,
                        ..Default::default()
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            assert_eq!(
                turso_statement_bind_named(
                    stmt.statement,
                    turso_slice_from_bytes("d".as_bytes()),
                    turso_value_t {
                        type_: capi::c::turso_type_t::TURSO_TYPE_INTEGER,
                        value: turso_value_union_t { integer: 2 }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            assert_eq!(
                turso_statement_bind_named(
                    stmt.statement,
                    turso_slice_from_bytes("c".as_bytes()),
                    turso_value_t {
                        type_: capi::c::turso_type_t::TURSO_TYPE_REAL,
                        value: turso_value_union_t { real: 3.14 }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );
            assert_eq!(
                turso_statement_bind_named(
                    stmt.statement,
                    turso_slice_from_bytes("b".as_bytes()),
                    turso_value_t {
                        type_: capi::c::turso_type_t::TURSO_TYPE_TEXT,
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
                    stmt.statement,
                    turso_slice_from_bytes("a".as_bytes()),
                    turso_value_t {
                        type_: capi::c::turso_type_t::TURSO_TYPE_BLOB,
                        value: turso_value_union_t {
                            blob: turso_slice_from_bytes(&bytes)
                        }
                    },
                )
                .code,
                turso_status_code_t::TURSO_OK
            );

            let mut collected = vec![];
            loop {
                let status = turso_statement_step(stmt.statement);
                if status.code == turso_status_code_t::TURSO_IO {
                    let io_result = turso_statement_run_io(stmt.statement).code;
                    assert_eq!(io_result, turso_status_code_t::TURSO_OK);
                    continue;
                }
                if status.code == turso_status_code_t::TURSO_DONE {
                    break;
                }
                if status.code == turso_status_code_t::TURSO_ROW {
                    for i in 0..columns {
                        let row_value = turso_statement_row_value(stmt.statement, i);
                        assert_eq!(row_value.status.code, turso_status_code_t::TURSO_OK);
                        collected.push(value_from_c_value(row_value.value).unwrap());
                    }
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

            turso_statement_deinit(stmt.statement);
            turso_connection_deinit(conn.connection);
            turso_database_deinit(db.database);
        }
    }

    #[test]
    pub fn test_db_stmt_insert_returning() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_create(turso_database_config_t {
                path: path.as_ptr(),
                ..Default::default()
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);

            let status = turso_database_open(db.database);
            assert_eq!(status.code, turso_status_code_t::TURSO_OK);

            let conn = turso_database_connect(db.database);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "CREATE TABLE t(x)";
            let stmt = turso_connection_prepare_single(
                conn.connection,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);
            while turso_statement_execute(stmt.statement).status.code
                != turso_status_code_t::TURSO_OK
            {
                let io_result = turso_statement_run_io(stmt.statement).code;
                assert_eq!(io_result, turso_status_code_t::TURSO_OK);
            }
            turso_statement_deinit(stmt.statement);

            let sql = "INSERT INTO t VALUES (1), (2), (3) RETURNING x";
            let stmt = turso_connection_prepare_single(
                conn.connection,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);

            let columns = turso_statement_column_count(stmt.statement);
            assert_eq!(columns, 1);

            let mut collected = vec![];
            loop {
                let status = turso_statement_step(stmt.statement);
                if status.code == turso_status_code_t::TURSO_IO {
                    let io_result = turso_statement_run_io(stmt.statement).code;
                    assert_eq!(io_result, turso_status_code_t::TURSO_OK);
                    continue;
                }
                if status.code == turso_status_code_t::TURSO_ROW {
                    for i in 0..columns {
                        let row_value = turso_statement_row_value(stmt.statement, i);
                        assert_eq!(row_value.status.code, turso_status_code_t::TURSO_OK);
                        collected.push(value_from_c_value(row_value.value).unwrap());
                    }
                    break;
                }
                assert!(false);
            }

            assert_eq!(collected, vec![turso_core::Value::Integer(1)]);

            while turso_statement_finalize(stmt.statement).code != turso_status_code_t::TURSO_OK {
                let io_result = turso_statement_run_io(stmt.statement).code;
                assert_eq!(io_result, turso_status_code_t::TURSO_OK);
            }
            turso_statement_deinit(stmt.statement);

            let sql = "SELECT COUNT(*) FROM t";
            let stmt = turso_connection_prepare_single(
                conn.connection,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);

            let columns = turso_statement_column_count(stmt.statement);
            assert_eq!(columns, 1);

            let mut collected = vec![];
            loop {
                let status = turso_statement_step(stmt.statement);
                if status.code == turso_status_code_t::TURSO_IO {
                    let io_result = turso_statement_run_io(stmt.statement).code;
                    assert_eq!(io_result, turso_status_code_t::TURSO_OK);
                    continue;
                }
                if status.code == turso_status_code_t::TURSO_ROW {
                    for i in 0..columns {
                        let row_value = turso_statement_row_value(stmt.statement, i);
                        assert_eq!(row_value.status.code, turso_status_code_t::TURSO_OK);
                        collected.push(value_from_c_value(row_value.value).unwrap());
                    }
                    continue;
                }
                if status.code == turso_status_code_t::TURSO_DONE {
                    break;
                }
                assert!(false);
            }

            assert_eq!(collected, vec![turso_core::Value::Integer(3)]);
            turso_statement_deinit(stmt.statement);
            turso_connection_deinit(conn.connection);
            turso_database_deinit(db.database);
        }
    }

    #[test]
    pub fn test_db_multi_stmt_exec() {
        unsafe {
            let path = CString::new(":memory:").unwrap();
            let db = turso_database_create(turso_database_config_t {
                path: path.as_ptr(),
                ..Default::default()
            });
            assert_eq!(db.status.code, turso_status_code_t::TURSO_OK);

            let status = turso_database_open(db.database);
            assert_eq!(status.code, turso_status_code_t::TURSO_OK);

            let conn = turso_database_connect(db.database);
            assert_eq!(conn.status.code, turso_status_code_t::TURSO_OK);

            let sql = "CREATE TABLE t(x); CREATE TABLE q(x); INSERT INTO t VALUES (1); INSERT INTO q VALUES (2);";
            let mut sql_slice = sql[..].as_bytes();
            loop {
                let stmt = turso_connection_prepare_first(
                    conn.connection,
                    turso_slice_ref_t {
                        ptr: sql_slice.as_ptr() as *const std::ffi::c_void,
                        len: sql_slice.len(),
                    },
                );
                assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);
                if turso_connection_prepare_first_result_empty(stmt) {
                    break;
                }
                sql_slice = &sql_slice[stmt.tail_idx..];

                while turso_statement_execute(stmt.statement).status.code
                    != turso_status_code_t::TURSO_OK
                {
                    let io_result = turso_statement_run_io(stmt.statement).code;
                    assert_eq!(io_result, turso_status_code_t::TURSO_OK);
                }
                turso_statement_deinit(stmt.statement);
            }

            let sql = "SELECT * FROM t UNION ALL SELECT * FROM q";
            let stmt = turso_connection_prepare_single(
                conn.connection,
                turso_slice_ref_t {
                    ptr: sql.as_ptr() as *const std::ffi::c_void,
                    len: sql.len(),
                },
            );
            assert_eq!(stmt.status.code, turso_status_code_t::TURSO_OK);

            let columns = turso_statement_column_count(stmt.statement);
            assert_eq!(columns, 1);

            let mut collected = vec![];
            loop {
                let status = turso_statement_step(stmt.statement);
                if status.code == turso_status_code_t::TURSO_IO {
                    let io_result = turso_statement_run_io(stmt.statement).code;
                    assert_eq!(io_result, turso_status_code_t::TURSO_OK);
                    continue;
                }
                if status.code == turso_status_code_t::TURSO_ROW {
                    for i in 0..columns {
                        let row_value = turso_statement_row_value(stmt.statement, i);
                        assert_eq!(row_value.status.code, turso_status_code_t::TURSO_OK);
                        collected.push(value_from_c_value(row_value.value).unwrap());
                    }
                    continue;
                }
                if status.code == turso_status_code_t::TURSO_DONE {
                    break;
                }
                assert!(false);
            }
            assert_eq!(
                collected,
                vec![turso_core::Value::Integer(1), turso_core::Value::Integer(2)]
            );
            turso_statement_deinit(stmt.statement);
            turso_connection_deinit(conn.connection);
            turso_database_deinit(db.database);
        }
    }
}
