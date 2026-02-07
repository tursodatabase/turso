use crate::errors::{Result, TursoError, TURSO_ETC};
use crate::turso_connection::TursoConnection;
use crate::utils::set_err_msg_and_throw_exception;
use jni::objects::{JByteArray, JObject, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use std::sync::Arc;
use turso_core::{Database, DatabaseOpts, EncryptionKey, EncryptionOpts, OpenFlags};

struct TursoDB {
    db: Arc<Database>,
    io: Arc<dyn turso_core::IO>,
    /// Encryption info: (cipher, hexkey) - stored as strings for lazy parsing
    encryption_info: Option<(String, String)>,
}

impl TursoDB {
    pub fn new(
        db: Arc<Database>,
        io: Arc<dyn turso_core::IO>,
        encryption_info: Option<(String, String)>,
    ) -> Self {
        TursoDB {
            db,
            io,
            encryption_info,
        }
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
            set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e);
            return -1;
        }
    };

    let db = match Database::open_file(io.clone(), &path) {
        Ok(db) => db,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e.to_string());
            return -1;
        }
    };

    TursoDB::new(db, io, None).to_ptr()
}

/// Opens a database with encryption support.
/// cipher and hexkey can be null for unencrypted databases.
#[no_mangle]
#[allow(clippy::arc_with_non_send_sync)]
pub extern "system" fn Java_tech_turso_core_TursoDB_openWithEncryptionUtf8<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    file_path_byte_arr: JByteArray<'local>,
    _open_flags: jint,
    cipher: JString<'local>,
    hexkey: JString<'local>,
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
            set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e);
            return -1;
        }
    };

    // Parse encryption options if provided
    let encryption_opts = if !cipher.is_null() && !hexkey.is_null() {
        let cipher_str: String = match env.get_string(&cipher) {
            Ok(s) => s.into(),
            Err(e) => {
                set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e.to_string());
                return -1;
            }
        };
        let hexkey_str: String = match env.get_string(&hexkey) {
            Ok(s) => s.into(),
            Err(e) => {
                set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e.to_string());
                return -1;
            }
        };
        Some(EncryptionOpts {
            cipher: cipher_str,
            hexkey: hexkey_str,
        })
    } else {
        None
    };

    // Clone encryption info before encryption_opts is consumed
    let encryption_info = encryption_opts
        .as_ref()
        .map(|opts| (opts.cipher.clone(), opts.hexkey.clone()));

    let db_opts = if encryption_opts.is_some() {
        DatabaseOpts::new().with_encryption(true)
    } else {
        DatabaseOpts::new()
    };

    let db = match Database::open_file_with_flags(
        io.clone(),
        &path,
        OpenFlags::Create,
        db_opts,
        encryption_opts,
    ) {
        Ok(db) => db,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e.to_string());
            return -1;
        }
    };

    TursoDB::new(db, io, encryption_info).to_ptr()
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

    // Parse encryption key if encryption info is present
    let encryption_key = if let Some((_cipher, hexkey)) = &db.encryption_info {
        match EncryptionKey::from_hex_string(hexkey) {
            Ok(key) => Some(key),
            Err(e) => {
                set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e.to_string());
                return 0;
            }
        }
    } else {
        None
    };

    // Use connect_with_encryption to properly set up encryption context
    // before the pager reads page 1. This is required for encrypted databases.
    let conn = match db.db.connect_with_encryption(encryption_key) {
        Ok(conn) => conn,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, TURSO_ETC, e.to_string());
            return 0;
        }
    };

    TursoConnection::new(conn, db.io.clone()).to_ptr()
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
