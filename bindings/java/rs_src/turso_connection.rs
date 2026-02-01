use crate::errors::{
    Result, TursoError, TURSO_ETC, TURSO_FAILED_TO_PARSE_BYTE_ARRAY,
    TURSO_FAILED_TO_PREPARE_STATEMENT,
};
use crate::turso_statement::TursoStatement;
use crate::utils::{set_err_msg_and_throw_exception, utf8_byte_arr_to_str};
use jni::objects::{JByteArray, JObject};
use jni::sys::jlong;
use jni::JNIEnv;
use std::sync::Arc;
use turso_core::Connection;

#[derive(Clone)]
pub struct TursoConnection {
    pub(crate) conn: Arc<Connection>,
    pub(crate) _io: Arc<dyn turso_core::IO>,
}

impl TursoConnection {
    pub fn new(conn: Arc<Connection>, io: Arc<dyn turso_core::IO>) -> Self {
        TursoConnection { conn, _io: io }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_ptr(self) -> jlong {
        Box::into_raw(Box::new(self)) as jlong
    }

    pub fn drop(ptr: jlong) {
        let _boxed = unsafe { Box::from_raw(ptr as *mut TursoConnection) };
    }
}

pub fn to_turso_connection(ptr: jlong) -> Result<&'static mut TursoConnection> {
    if ptr == 0 {
        Err(TursoError::InvalidConnectionPointer)
    } else {
        unsafe { Ok(&mut *(ptr as *mut TursoConnection)) }
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_turso_core_TursoConnection__1close<'local>(
    _env: JNIEnv<'local>,
    _obj: JObject<'local>,
    connection_ptr: jlong,
) {
    TursoConnection::drop(connection_ptr);
}

#[no_mangle]
pub extern "system" fn Java_tech_turso_core_TursoConnection_prepareUtf8<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    connection_ptr: jlong,
    sql_bytes: JByteArray<'local>,
) -> jlong {
    let connection = match to_turso_connection(connection_ptr) {
        Ok(conn) => conn,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e.to_string());
            return 0;
        }
    };

    let sql = match utf8_byte_arr_to_str(&env, sql_bytes) {
        Ok(sql) => sql,
        Err(e) => {
            set_err_msg_and_throw_exception(
                &mut env,
                obj,
                TURSO_FAILED_TO_PARSE_BYTE_ARRAY,
                e.to_string(),
            );
            return 0;
        }
    };

    match connection.conn.prepare(sql) {
        Ok(stmt) => TursoStatement::new(stmt, connection.clone()).to_ptr(),
        Err(e) => {
            set_err_msg_and_throw_exception(
                &mut env,
                obj,
                TURSO_FAILED_TO_PREPARE_STATEMENT,
                e.to_string(),
            );
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_turso_core_TursoConnection__1getAutoCommit<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    connection_ptr: jlong,
) -> jni::sys::jboolean {
    let connection = match to_turso_connection(connection_ptr) {
        Ok(conn) => conn,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e.to_string());
            return 0; // equivalent to false, but exception is thrown
        }
    };

    if connection.conn.get_auto_commit() {
        1
    } else {
        0
    }
}
