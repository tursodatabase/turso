use crate::cursor::Cursor;
use jni::objects::JClass;
use jni::sys::jlong;
use jni::JNIEnv;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Connection {
    pub(crate) conn: Arc<Mutex<Rc<limbo_core::Connection>>>,
    pub(crate) io: Arc<limbo_core::PlatformIO>,
}

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_cursor<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_ptr: jlong,
) -> jlong {
    let connection = to_connection(connection_ptr);
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
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_id: jlong,
) {
    let connection = to_connection(connection_id);
    drop(connection.conn.clone());
}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_commit<'local>(
    _env: &mut JNIEnv<'local>,
    _class: JClass<'local>,
    _connection_id: jlong,
) {
    unimplemented!()
}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_rollback<'local>(
    _env: &mut JNIEnv<'local>,
    _class: JClass<'local>,
    _connection_id: jlong,
) {
    unimplemented!()
}

fn to_connection(connection_ptr: jlong) -> &'static mut Connection {
    unsafe { &mut *(connection_ptr as *mut Connection) }
}
