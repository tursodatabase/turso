mod connection;
mod cursor;

use crate::connection::Connection;
use jni::errors::JniError;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
struct Description {
    name: String,
    type_code: String,
    display_size: Option<String>,
    internal_size: Option<String>,
    precision: Option<String>,
    scale: Option<String>,
    null_ok: Option<String>,
}

#[derive(Clone, Debug)]
struct Tuple<X, Y> {
    x: X,
    y: Y,
}


#[no_mangle]
pub extern "system" fn Java_limbo_Limbo_connect<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
) -> jlong {
    connect_internal(&mut env, path).unwrap_or_else(|e| -1)
}

fn connect_internal<'local>(
    env: &mut JNIEnv<'local>,
    path: JString<'local>,
) -> Result<jlong, JniError> {
    let io = Arc::new(limbo_core::PlatformIO::new().map_err(|e| {
        println!("IO initialization failed: {:?}", e);
        JniError::Unknown
    })?);

    let path: String = env
        .get_string(&path)
        .expect("Failed to convert JString to Rust String")
        .into();
    let db = limbo_core::Database::open_file(io.clone(), &path).map_err(|e| {
        println!("Failed to open database: {:?}", e);
        JniError::Unknown
    })?;

    let conn = db.connect().clone();
    let connection = Connection {
        conn: Arc::new(Mutex::new(conn)),
        io,
    };

    Ok(Box::into_raw(Box::new(connection)) as jlong)
}
