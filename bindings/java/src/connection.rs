use crate::Description;
use jni::objects::JClass;
use jni::sys::jlong;
use jni::JNIEnv;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use jni::errors::JniError;

#[derive(Clone)]
pub struct Connection {
    pub conn: Arc<Mutex<Rc<limbo_core::Connection>>>,
    pub io: Arc<limbo_core::PlatformIO>,
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

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_cursor<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_id: jlong,
) -> jlong {
    let connection = to_connection(connection_id);
    let cursor = Cursor {
        array_size: 1,
        conn: connection.clone(),
        description: None,
        rowcount: -1,
        smt: None,
    };
    Box::into_raw(Box::new(cursor)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_close<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_id: jlong,
) {
    let connection = to_connection(connection_id);
    drop(connection.conn.clone());
}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_commit<'local>(
    env: &mut JNIEnv<'local>,
    _class: JClass<'local>,
    connection_id: jlong,
) -> Result<(), JniError> {
    Err(JniError::Unknown)
}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_rollback<'local>(
    env: &mut JNIEnv<'local>,
    _class: JClass<'local>,
    connection_id: jlong,
) {
}

fn to_connection(connection_ptr: jlong) -> &'static mut Connection {
    unsafe { &mut *(connection_ptr as *mut Connection) }
}

fn to_cursor(cursor_ptr: jlong) -> &'static mut Cursor {
    unsafe { &mut *(cursor_ptr as *mut Cursor) }
}
