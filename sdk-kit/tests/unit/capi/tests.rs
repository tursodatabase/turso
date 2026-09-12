use std::ffi::{CStr, CString};

use turso_core::types::Text;

use crate::capi::{
    c::{
        self, turso_connection_deinit, turso_connection_prepare_single, turso_database_connect,
        turso_database_deinit, turso_database_new, turso_database_open, turso_setup,
        turso_statement_bind_positional_blob, turso_statement_bind_positional_double,
        turso_statement_bind_positional_int, turso_statement_bind_positional_null,
        turso_statement_bind_positional_text, turso_statement_column_count, turso_statement_deinit,
        turso_statement_execute, turso_statement_n_change, turso_statement_named_position,
        turso_statement_parameters_count, turso_statement_run_io, turso_statement_step,
        turso_status_code_t, turso_str_deinit, turso_version,
    },
    value_from_c_value,
};

extern "C" fn logger(log: *const c::turso_log_t) {
    println!("log: {:?}", unsafe {
        std::ffi::CStr::from_ptr((*log).message)
    });
}

#[test]
pub fn test_version() {
    unsafe {
        let version = CStr::from_ptr(turso_version()).to_str().unwrap();
        println!("{version}");
        assert_eq!(version, env!("CARGO_PKG_VERSION"));
    }
}

#[test]
pub fn test_db_setup() {
    unsafe {
        let config = c::turso_config_t {
            logger: Some(logger),
            log_level: c"debug".as_ptr(),
        };
        turso_setup(&config, std::ptr::null_mut());
    }
}

#[test]
pub fn test_db_init() {
    unsafe {
        let path = CString::new(":memory:").unwrap();
        let config = c::turso_database_config_t {
            path: path.as_ptr(),
            ..Default::default()
        };
        let mut db = std::ptr::null();
        let status = turso_database_new(&config, &mut db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let status = turso_database_open(db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        turso_database_deinit(db);
    }
}

#[test]
pub fn test_db_error() {
    unsafe {
        let path = CString::new("not/existing/path").unwrap();
        let config = c::turso_database_config_t {
            path: path.as_ptr(),
            ..Default::default()
        };
        let mut db = std::ptr::null();
        let status = turso_database_new(&config, &mut db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let mut error = std::ptr::null();
        let status = turso_database_open(db, &mut error);

        assert_eq!(status, turso_status_code_t::TURSO_IOERR);
        assert_eq!(
            std::ffi::CStr::from_ptr(error).to_str().unwrap(),
            "I/O error (open): entity not found"
        );
        turso_str_deinit(error);
        turso_database_deinit(db);
    }
}

#[test]
pub fn test_db_conn_init() {
    unsafe {
        let path = CString::new(":memory:").unwrap();
        let config = c::turso_database_config_t {
            path: path.as_ptr(),
            ..Default::default()
        };
        let mut db = std::ptr::null();
        let status = turso_database_new(&config, &mut db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let status = turso_database_open(db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let mut connection = std::ptr::null_mut();
        let status = turso_database_connect(db, &mut connection, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        turso_connection_deinit(connection);
        turso_database_deinit(db);
    }
}

#[test]
pub fn test_db_stmt_prepare() {
    unsafe {
        let path = CString::new(":memory:").unwrap();
        let config = c::turso_database_config_t {
            path: path.as_ptr(),
            ..Default::default()
        };
        let mut db = std::ptr::null();
        let status = turso_database_new(&config, &mut db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let status = turso_database_open(db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let mut connection = std::ptr::null_mut();
        let status = turso_database_connect(db, &mut connection, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let sql = c"SELECT NULL, 2, 2.71, '5', x'06'";
        let mut statement = std::ptr::null_mut();
        let status = turso_connection_prepare_single(
            connection,
            sql.as_ptr(),
            &mut statement,
            std::ptr::null_mut(),
        );
        assert_eq!(status, turso_status_code_t::TURSO_OK);
        assert_eq!(turso_statement_n_change(statement), 0);

        turso_statement_deinit(statement);
        turso_connection_deinit(connection);
        turso_database_deinit(db);
    }
}

#[test]
pub fn test_db_stmt_prepare_parse_error() {
    unsafe {
        let path = CString::new(":memory:").unwrap();
        let config = c::turso_database_config_t {
            path: path.as_ptr(),
            ..Default::default()
        };
        let mut db = std::ptr::null();
        let status = turso_database_new(&config, &mut db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let status = turso_database_open(db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let mut connection = std::ptr::null_mut();
        let status = turso_database_connect(db, &mut connection, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let mut error = std::ptr::null();
        let sql = c"SELECT nil";
        let mut statement = std::ptr::null_mut();
        let status =
            turso_connection_prepare_single(connection, sql.as_ptr(), &mut statement, &mut error);
        assert_eq!(status, turso_status_code_t::TURSO_ERROR);
        assert_eq!(
            std::ffi::CStr::from_ptr(error).to_str().unwrap(),
            "Parse error: no such column: nil"
        );

        turso_str_deinit(error);
        turso_connection_deinit(connection);
        turso_database_deinit(db);
    }
}

#[test]
pub fn test_db_stmt_execute() {
    unsafe {
        let path = CString::new(":memory:").unwrap();
        let config = c::turso_database_config_t {
            path: path.as_ptr(),
            ..Default::default()
        };
        let mut db = std::ptr::null();
        let status = turso_database_new(&config, &mut db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let status = turso_database_open(db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let mut connection = std::ptr::null_mut();
        let status = turso_database_connect(db, &mut connection, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let sql = c"CREATE TABLE t(x)";
        let mut statement = std::ptr::null_mut();
        let status = turso_connection_prepare_single(
            connection,
            sql.as_ptr(),
            &mut statement,
            std::ptr::null_mut(),
        );
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        loop {
            let status =
                turso_statement_execute(statement, std::ptr::null_mut(), std::ptr::null_mut());
            if status == turso_status_code_t::TURSO_DONE {
                break;
            }
            let status = turso_statement_run_io(statement, std::ptr::null_mut());
            assert_eq!(status, turso_status_code_t::TURSO_DONE);
        }
        turso_statement_deinit(statement);

        let mut error = std::ptr::null();
        let status =
            turso_connection_prepare_single(connection, sql.as_ptr(), &mut statement, &mut error);
        assert_eq!(status, turso_status_code_t::TURSO_ERROR);
        assert_eq!(
            std::ffi::CStr::from_ptr(error).to_str().unwrap(),
            "Parse error: table t already exists"
        );

        turso_str_deinit(error);

        turso_connection_deinit(connection);
        turso_database_deinit(db);
    }
}

#[test]
pub fn test_db_stmt_insert() {
    unsafe {
        let path = CString::new(":memory:").unwrap();
        let config = c::turso_database_config_t {
            path: path.as_ptr(),
            ..Default::default()
        };
        let mut db = std::ptr::null();
        let status = turso_database_new(&config, &mut db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let status = turso_database_open(db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let mut connection = std::ptr::null_mut();
        let status = turso_database_connect(db, &mut connection, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let ddl = c"CREATE TABLE t(x)";
        let mut statement = std::ptr::null_mut();
        let status = turso_connection_prepare_single(
            connection,
            ddl.as_ptr(),
            &mut statement,
            std::ptr::null_mut(),
        );
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        loop {
            let status =
                turso_statement_execute(statement, std::ptr::null_mut(), std::ptr::null_mut());
            if status == turso_status_code_t::TURSO_DONE {
                break;
            }
            let status = turso_statement_run_io(statement, std::ptr::null_mut());
            assert_eq!(status, turso_status_code_t::TURSO_DONE);
        }
        turso_statement_deinit(statement);

        let dml = c"INSERT INTO t VALUES (1), (2), (3)";
        let mut statement = std::ptr::null_mut();
        let status = turso_connection_prepare_single(
            connection,
            dml.as_ptr(),
            &mut statement,
            std::ptr::null_mut(),
        );
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        loop {
            let status =
                turso_statement_execute(statement, std::ptr::null_mut(), std::ptr::null_mut());
            if status == turso_status_code_t::TURSO_DONE {
                break;
            }
            let status = turso_statement_run_io(statement, std::ptr::null_mut());
            assert_eq!(status, turso_status_code_t::TURSO_DONE);
        }
        assert_eq!(turso_statement_n_change(statement), 3);
        turso_statement_deinit(statement);

        turso_connection_deinit(connection);
        turso_database_deinit(db);
    }
}

#[test]
pub fn test_db_stmt_query() {
    unsafe {
        let path = CString::new(":memory:").unwrap();
        let config = c::turso_database_config_t {
            path: path.as_ptr(),
            ..Default::default()
        };
        let mut db = std::ptr::null();
        let status = turso_database_new(&config, &mut db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let status = turso_database_open(db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let mut connection = std::ptr::null_mut();
        let status = turso_database_connect(db, &mut connection, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let sql = c"SELECT NULL, 2, 2.71, '5', x'06'";
        let mut statement = std::ptr::null_mut();
        let status = turso_connection_prepare_single(
            connection,
            sql.as_ptr(),
            &mut statement,
            std::ptr::null_mut(),
        );
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let columns = turso_statement_column_count(statement);
        assert_eq!(columns, 5);
        let mut collected = Vec::new();
        loop {
            let status = turso_statement_step(statement, std::ptr::null_mut());
            if status == turso_status_code_t::TURSO_IO {
                let status = turso_statement_run_io(statement, std::ptr::null_mut());
                assert_eq!(status, turso_status_code_t::TURSO_OK);
                continue;
            }
            if status == turso_status_code_t::TURSO_DONE {
                break;
            }
            if status == turso_status_code_t::TURSO_ROW {
                for i in 0..columns {
                    collected.push(value_from_c_value(statement, i as usize));
                }
                continue;
            }
            panic!("unexpected");
        }

        turso_statement_deinit(statement);
        turso_connection_deinit(connection);
        turso_database_deinit(db);
        assert_eq!(
            collected,
            vec![
                turso_core::Value::Null,
                turso_core::Value::from_i64(2),
                turso_core::Value::from_f64(2.71),
                turso_core::Value::Text(Text::new("5")),
                turso_core::Value::Blob(vec![6]),
            ]
        );
    }
}

#[test]
pub fn test_db_stmt_bind_positional() {
    unsafe {
        let path = CString::new(":memory:").unwrap();
        let config = c::turso_database_config_t {
            path: path.as_ptr(),
            ..Default::default()
        };
        let mut db = std::ptr::null();
        let status = turso_database_new(&config, &mut db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let status = turso_database_open(db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let mut connection = std::ptr::null_mut();
        let status = turso_database_connect(db, &mut connection, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let sql = c"SELECT ?, ?, ?, ?, ?";
        let mut statement = std::ptr::null_mut();
        let status = turso_connection_prepare_single(
            connection,
            sql.as_ptr(),
            &mut statement,
            std::ptr::null_mut(),
        );
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        assert_eq!(
            turso_statement_bind_positional_null(statement, 1),
            turso_status_code_t::TURSO_OK
        );
        assert_eq!(
            turso_statement_bind_positional_int(statement, 2, 2),
            turso_status_code_t::TURSO_OK
        );
        assert_eq!(
            turso_statement_bind_positional_double(statement, 3, 2.71),
            turso_status_code_t::TURSO_OK
        );
        let text = "5";
        assert_eq!(
            turso_statement_bind_positional_text(
                statement,
                4,
                text.as_ptr() as *const std::ffi::c_char,
                text.len()
            ),
            turso_status_code_t::TURSO_OK
        );
        let blob = [6];
        assert_eq!(
            turso_statement_bind_positional_blob(statement, 5, blob.as_ptr(), blob.len()),
            turso_status_code_t::TURSO_OK
        );

        let columns = turso_statement_column_count(statement);
        assert_eq!(columns, 5);
        let mut collected = Vec::new();
        loop {
            let status = turso_statement_step(statement, std::ptr::null_mut());
            if status == turso_status_code_t::TURSO_IO {
                let status = turso_statement_run_io(statement, std::ptr::null_mut());
                assert_eq!(status, turso_status_code_t::TURSO_OK);
                continue;
            }
            if status == turso_status_code_t::TURSO_DONE {
                break;
            }
            if status == turso_status_code_t::TURSO_ROW {
                for i in 0..columns {
                    collected.push(value_from_c_value(statement, i as usize));
                }
                continue;
            }
            panic!("unexpected");
        }

        turso_statement_deinit(statement);
        turso_connection_deinit(connection);
        turso_database_deinit(db);

        assert_eq!(
            collected,
            vec![
                turso_core::Value::Null,
                turso_core::Value::from_i64(2),
                turso_core::Value::from_f64(2.71),
                turso_core::Value::Text(Text::new("5")),
                turso_core::Value::Blob(vec![6]),
            ]
        );
    }
}

#[test]
pub fn test_db_stmt_bind_named() {
    unsafe {
        let path = CString::new(":memory:").unwrap();
        let config = c::turso_database_config_t {
            path: path.as_ptr(),
            ..Default::default()
        };
        let mut db = std::ptr::null();
        let status = turso_database_new(&config, &mut db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let status = turso_database_open(db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let mut connection = std::ptr::null_mut();
        let status = turso_database_connect(db, &mut connection, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let sql = c"SELECT :e, :d, :c, :b, :a";
        let mut statement = std::ptr::null_mut();
        let status = turso_connection_prepare_single(
            connection,
            sql.as_ptr(),
            &mut statement,
            std::ptr::null_mut(),
        );
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        assert_eq!(
            turso_statement_bind_positional_null(
                statement,
                turso_statement_named_position(statement, c":e".as_ptr()) as usize
            ),
            turso_status_code_t::TURSO_OK
        );
        assert_eq!(
            turso_statement_bind_positional_int(
                statement,
                turso_statement_named_position(statement, c":d".as_ptr()) as usize,
                2
            ),
            turso_status_code_t::TURSO_OK
        );
        assert_eq!(
            turso_statement_bind_positional_double(
                statement,
                turso_statement_named_position(statement, c":c".as_ptr()) as usize,
                2.71
            ),
            turso_status_code_t::TURSO_OK
        );
        let text = "5";
        assert_eq!(
            turso_statement_bind_positional_text(
                statement,
                turso_statement_named_position(statement, c":b".as_ptr()) as usize,
                text.as_ptr() as *const std::ffi::c_char,
                text.len()
            ),
            turso_status_code_t::TURSO_OK
        );
        let blob = [6];
        assert_eq!(
            turso_statement_bind_positional_blob(
                statement,
                turso_statement_named_position(statement, c":a".as_ptr()) as usize,
                blob.as_ptr(),
                blob.len()
            ),
            turso_status_code_t::TURSO_OK
        );

        let columns = turso_statement_column_count(statement);
        assert_eq!(columns, 5);
        let mut collected = Vec::new();
        loop {
            let status = turso_statement_step(statement, std::ptr::null_mut());
            if status == turso_status_code_t::TURSO_IO {
                let status = turso_statement_run_io(statement, std::ptr::null_mut());
                assert_eq!(status, turso_status_code_t::TURSO_OK);
                continue;
            }
            if status == turso_status_code_t::TURSO_DONE {
                break;
            }
            if status == turso_status_code_t::TURSO_ROW {
                for i in 0..columns {
                    collected.push(value_from_c_value(statement, i as usize));
                }
                continue;
            }
            panic!("unexpected");
        }

        turso_statement_deinit(statement);
        turso_connection_deinit(connection);
        turso_database_deinit(db);

        assert_eq!(
            collected,
            vec![
                turso_core::Value::Null,
                turso_core::Value::from_i64(2),
                turso_core::Value::from_f64(2.71),
                turso_core::Value::Text(Text::new("5")),
                turso_core::Value::Blob(vec![6]),
            ]
        );
    }
}

#[test]
pub fn test_db_stmt_named_position_requires_prefixed_name() {
    unsafe {
        let path = CString::new(":memory:").unwrap();
        let config = c::turso_database_config_t {
            path: path.as_ptr(),
            ..Default::default()
        };
        let mut db = std::ptr::null();
        let status = turso_database_new(&config, &mut db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let status = turso_database_open(db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let mut connection = std::ptr::null_mut();
        let status = turso_database_connect(db, &mut connection, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let sql = c"SELECT :e";
        let mut statement = std::ptr::null_mut();
        let status = turso_connection_prepare_single(
            connection,
            sql.as_ptr(),
            &mut statement,
            std::ptr::null_mut(),
        );
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        assert_eq!(turso_statement_named_position(statement, c":e".as_ptr()), 1);
        assert_eq!(turso_statement_named_position(statement, c"e".as_ptr()), -1);

        turso_statement_deinit(statement);
        turso_connection_deinit(connection);
        turso_database_deinit(db);
    }
}

#[test]
pub fn test_db_stmt_bind_positional_out_of_bounds() {
    unsafe {
        let path = CString::new(":memory:").unwrap();
        let config = c::turso_database_config_t {
            path: path.as_ptr(),
            ..Default::default()
        };
        let mut db = std::ptr::null();
        let status = turso_database_new(&config, &mut db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let status = turso_database_open(db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let mut connection = std::ptr::null_mut();
        let status = turso_database_connect(db, &mut connection, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let sql = c"SELECT ?1";
        let mut statement = std::ptr::null_mut();
        let status = turso_connection_prepare_single(
            connection,
            sql.as_ptr(),
            &mut statement,
            std::ptr::null_mut(),
        );
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        assert_eq!(
            turso_statement_bind_positional_int(statement, 1, 1),
            turso_status_code_t::TURSO_OK
        );
        assert_eq!(
            turso_statement_bind_positional_int(statement, 2, 2),
            turso_status_code_t::TURSO_MISUSE
        );

        turso_statement_deinit(statement);
        turso_connection_deinit(connection);
        turso_database_deinit(db);
    }
}

#[test]
pub fn test_db_stmt_sparse_positional_slot_range_matches_sqlite() {
    unsafe {
        let path = CString::new(":memory:").unwrap();
        let config = c::turso_database_config_t {
            path: path.as_ptr(),
            ..Default::default()
        };
        let mut db = std::ptr::null();
        let status = turso_database_new(&config, &mut db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let status = turso_database_open(db, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let mut connection = std::ptr::null_mut();
        let status = turso_database_connect(db, &mut connection, std::ptr::null_mut());
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        let sql = c"SELECT ?3";
        let mut statement = std::ptr::null_mut();
        let status = turso_connection_prepare_single(
            connection,
            sql.as_ptr(),
            &mut statement,
            std::ptr::null_mut(),
        );
        assert_eq!(status, turso_status_code_t::TURSO_OK);

        assert_eq!(turso_statement_parameters_count(statement), 3);
        assert_eq!(
            turso_statement_bind_positional_int(statement, 1, 1),
            turso_status_code_t::TURSO_OK
        );
        assert_eq!(
            turso_statement_bind_positional_int(statement, 3, 3),
            turso_status_code_t::TURSO_OK
        );
        assert_eq!(
            turso_statement_bind_positional_int(statement, 4, 4),
            turso_status_code_t::TURSO_MISUSE
        );

        turso_statement_deinit(statement);
        turso_connection_deinit(connection);
        turso_database_deinit(db);
    }
}
