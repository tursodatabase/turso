use crate::errors::{Result, TursoError, TURSO_ETC};
use crate::turso_connection::TursoConnection;
use crate::utils::set_err_msg_and_throw_exception;
use jni::objects::{JByteArray, JObject};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use std::sync::Arc;
use turso_core::Database;

struct TursoDB {
    db: Arc<Database>,
    io: Arc<dyn turso_core::IO>,
}

impl TursoDB {
    pub fn new(db: Arc<Database>, io: Arc<dyn turso_core::IO>) -> Self {
        TursoDB { db, io }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_ptr(self) -> jlong {
        Box::into_raw(Box::new(self)) as jlong
    }

    pub fn drop(ptr: jlong) {
        let _boxed = unsafe { Box::from_raw(ptr as *mut TursoDB) };
    }
}

fn to_turso_db(ptr: jlong) -> Result<&'static mut TursoDB> {
    if ptr == 0 {
        Err(TursoError::InvalidDatabasePointer)
    } else {
        unsafe { Ok(&mut *(ptr as *mut TursoDB)) }
    }
}

#[no_mangle]
#[allow(clippy::arc_with_non_send_sync)]
pub extern "system" fn Java_tech_turso_core_TursoDB_openUtf8<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    file_path_byte_arr: JByteArray<'local>,
    _open_flags: jint,
) -> jlong {
    let io = match turso_core::PlatformIO::new() {
        Ok(io) => Arc::new(io),
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e.to_string());
            return -1;
        }
    };

    let path = match env
        .convert_byte_array(file_path_byte_arr)
        .map_err(|e| e.to_string())
    {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(e) => {
                set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e.to_string());
                return -1;
            }
        },
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e.to_string());
            return -1;
        }
    };

    let db = match Database::open_file(io.clone(), &path, false, true) {
        Ok(db) => db,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e.to_string());
            return -1;
        }
    };

    TursoDB::new(db, io).to_ptr()
}

#[no_mangle]
pub extern "system" fn Java_tech_turso_core_TursoDB_connect0<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    db_pointer: jlong,
) -> jlong {
    let db = match to_turso_db(db_pointer) {
        Ok(db) => db,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e.to_string());
            return 0;
        }
    };

    let conn = TursoConnection::new(db.db.connect().unwrap(), db.io.clone());
    conn.to_ptr()
}

#[no_mangle]
pub extern "system" fn Java_tech_turso_core_TursoDB_close0<'local>(
    _env: JNIEnv<'local>,
    _obj: JObject<'local>,
    db_pointer: jlong,
) {
    TursoDB::drop(db_pointer);
}

#[no_mangle]
pub extern "system" fn Java_tech_turso_core_TursoDB_throwJavaException<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    error_code: jint,
) {
    set_err_msg_and_throw_exception(
        &mut env,
        obj,
        error_code,
        "throw java exception".to_string(),
    );
}
