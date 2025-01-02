use crate::connection::Connection;
use crate::Description;
use jni::objects::{JClass, JList, JObject, JString};
use jni::sys::{jlong, jstring};
use jni::JNIEnv;
use std::fmt::{Debug, Formatter, Pointer};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Cursor {
    /// This read/write attribute specifies the number of rows to fetch at a time with `.fetchmany()`.
    /// It defaults to `1`, meaning it fetches a single row at a time.
    pub(crate) array_size: i64,

    pub(crate) conn: Connection,

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
    pub(crate) description: Option<Description>,

    /// Read-only attribute that provides the number of modified rows for `INSERT`, `UPDATE`, `DELETE`,
    /// and `REPLACE` statements; it is `-1` for other statements, including CTE queries.
    /// It is only updated by the `execute()` and `executemany()` methods after the statement has run to completion.
    /// This means any resulting rows must be fetched for `rowcount` to be updated.
    pub(crate) rowcount: i64,

    pub(crate) smt: Option<Arc<Mutex<limbo_core::Statement>>>,
}

impl Debug for Cursor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cursor")
            .field("array_size", &self.array_size)
            .field("description", &self.description)
            .field("rowcount", &self.rowcount)
            .finish()
    }
}

#[no_mangle]
pub extern "system" fn Java_limbo_Cursor_execute<'local>(
    env: &mut JNIEnv<'local>,
    _class: JClass<'local>,
    cursor_ptr: jlong,
    sql: jstring,
) {
    println!("sql: {:?}", sql);
}

fn to_cursor(cursor_ptr: jlong) -> &'static mut Cursor {
    unsafe { &mut *(cursor_ptr as *mut Cursor) }
}

fn stmt_is_dml(sql: &str) -> bool {
    let sql = sql.trim();
    let sql = sql.to_uppercase();
    sql.starts_with("INSERT") || sql.starts_with("UPDATE") || sql.starts_with("DELETE")
}
