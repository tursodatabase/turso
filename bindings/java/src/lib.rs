mod connection;

use crate::connection::Connection;
use jni::errors::JniError;
use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use lazy_static::lazy_static;
use std::collections::HashMap;
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

#[derive(Clone)]
struct Cursor {
    /// This read/write attribute specifies the number of rows to fetch at a time with `.fetchmany()`.
    /// It defaults to `1`, meaning it fetches a single row at a time.
    array_size: i64,

    conn: Connection,

    /// The `.description` attribute is a read-only sequence of 7-item, each describing a column in the result set:
    ///
    /// - `name`: The column's name (always present).
    /// - `type_code`: The data type code (always present).
    /// - `display_size`: Column's display size (optional).
    /// - `internal_size`: Column's internal size (optional).
    /// - `precision`: Numeric precision (optional).
    /// - `scale`: Numeric scale (optional).
    /// - `null_ok`: Indicates if null values are allowed (optional).
    ///
    /// The `name` and `type_code` fields are mandatory; others default to `None` if not applicable.
    ///
    /// This attribute is `None` for operations that do not return rows or if no `.execute*()` method has been invoked.
    description: Option<Description>,

    /// Read-only attribute that provides the number of modified rows for `INSERT`, `UPDATE`, `DELETE`,
    /// and `REPLACE` statements; it is `-1` for other statements, including CTE queries.
    /// It is only updated by the `execute()` and `executemany()` methods after the statement has run to completion.
    /// This means any resulting rows must be fetched for `rowcount` to be updated.
    rowcount: i64,

    smt: Option<Arc<Mutex<limbo_core::Statement>>>,
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
        env.throw_new(
            "java/lang/Exception",
            format!("IO initialization failed: {:?}", e),
        )
        .unwrap();
        JniError::Unknown
    })?);

    let path: String = env
        .get_string(&path)
        .expect("Failed to convert JString to Rust String")
        .into();
    let db = limbo_core::Database::open_file(io.clone(), &path).map_err(|e| {
        env.throw_new(
            "java/lang/Exception",
            format!("Failed to open database: {:?}", e),
        )
        .unwrap();
        JniError::Unknown
    })?;

    let conn = db.connect().clone();
    let connection = Connection {
        hello: "hello seonwoo hehe".to_string(),
        conn: Arc::new(Mutex::new(conn)),
        io,
    };

    Ok(Box::into_raw(Box::new(connection)) as jlong)
}

#[no_mangle]
pub unsafe extern "system" fn Java_limbo_Limbo_test<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_ptr: jlong
) {
    let connection = &mut *(connection_ptr as *mut Connection);
    println!("test: {:?}", connection.hello);
}
