use crate::connection::Connection;
use crate::{eprint_return, Description};
use jni::errors::JniError;
use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use limbo_core::IO;
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
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    cursor_ptr: jlong,
    sql: JString<'local>,
) -> Result<(), JniError> {
    let sql: String = env
        .get_string(&sql)
        .expect("Could not extract query")
        .into();

    let stmt_is_dml = stmt_is_dml(&sql);
    if stmt_is_dml {
        return eprint_return!(
            "DML statements (INSERT/UPDATE/DELETE) are not fully supported in this version",
            JniError::Other(-1)
        );
    }

    let cursor = to_cursor(cursor_ptr);
    let conn_lock = match cursor.conn.conn.lock() {
        Ok(lock) => lock,
        Err(_) => return eprint_return!("Failed to acquire connection lock", JniError::Other(-1)),
    };

    match conn_lock.prepare(&sql) {
        Ok(statement) => {
            cursor.smt = Some(Arc::new(Mutex::new(statement)));
            Ok(())
        }
        Err(e) => {
            eprint_return!(
                &format!("Failed to prepare statement: {:?}", e),
                JniError::Other(-1)
            )
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_limbo_Cursor_fetchOne<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    cursor_ptr: jlong,
) -> Result<(), JniError> {
    let cursor = to_cursor(cursor_ptr);

    if let Some(smt) = &cursor.smt {
        let mut smt_lock = match smt.lock() {
            Ok(lock) => lock,
            Err(_) => {
                return eprint_return!("Failed to acquire statement lock", JniError::Other(-1))
            }
        };

        loop {
            match smt_lock.step() {
                Ok(limbo_core::StepResult::Row(row)) => {
                    println!("Row result: {:?}", row.values);
                }
                Ok(limbo_core::StepResult::IO) => {
                    if let Err(e) = cursor.conn.io.run_once() {
                        return eprint_return!(&format!("IO Error: {:?}", e), JniError::Other(-1));
                    }
                }
                Ok(limbo_core::StepResult::Interrupt) => {
                    return Ok(());
                }
                Ok(limbo_core::StepResult::Done) => {
                    return Ok(());
                }
                Ok(limbo_core::StepResult::Busy) => {
                    return eprint_return!("Busy error", JniError::Other(-1));
                }
                Err(e) => {
                    return eprint_return!(&format!("Step error: {:?}", e), JniError::Other(-1));
                }
            }
        }
    } else {
        eprint_return!("No statement prepared for execution", JniError::Other(-1))
    }
}

fn to_cursor(cursor_ptr: jlong) -> &'static mut Cursor {
    unsafe { &mut *(cursor_ptr as *mut Cursor) }
}

fn stmt_is_dml(sql: &str) -> bool {
    let sql = sql.trim();
    let sql = sql.to_uppercase();
    sql.starts_with("INSERT") || sql.starts_with("UPDATE") || sql.starts_with("DELETE")
}
