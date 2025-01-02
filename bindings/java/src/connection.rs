use jni::objects::{JClass, JObject};
use jni::sys::jlong;
use jni::JNIEnv;
use lazy_static::lazy_static;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Connection {
    pub hello: String,
    pub conn: Arc<Mutex<Rc<limbo_core::Connection>>>,
    pub io: Arc<limbo_core::PlatformIO>,
}

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

lazy_static! {
    static ref CONNECTIONS: Mutex<Vec<Arc<Connection>>> = Mutex::new(Vec::new());
}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_cursor<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_id: jlong,
) -> JObject<'local> {
    let connections = CONNECTIONS.lock().unwrap();
    if let Some(connection) = connections.get(connection_id as usize) {
        // Create a new cursor using the connection
        // Return the cursor object
        todo!()
    } else {
        // env.throw_new("java/lang/Exception", "Invalid connection ID").unwrap();
        JObject::null()
    }
}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_close<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_id: jlong,
) {
    let mut connections = CONNECTIONS.lock().unwrap();
    if connections.get(connection_id as usize).is_some() {
        connections.remove(connection_id as usize);
    } else {
        // env.throw_new("java/lang/Exception", "Invalid connection ID").unwrap();
    }
}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_commit<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_id: jlong,
) {
    let connections = CONNECTIONS.lock().unwrap();
    if let Some(connection) = connections.get(connection_id as usize) {
        let conn = connection.conn.lock().unwrap();
        // conn.commit().unwrap_or_else(|e| {
        //     env.throw_new("java/lang/Exception", format!("Commit failed: {:?}", e)).unwrap();
        // });
    } else {
        // env.throw_new("java/lang/Exception", "Invalid connection ID").unwrap();
    }
}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_rollback<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_id: jlong,
) {
    let connections = CONNECTIONS.lock().unwrap();
    if let Some(connection) = connections.get(connection_id as usize) {
        let conn = connection.conn.lock().unwrap();
        // conn.rollback().unwrap_or_else(|e| {
        //     env.throw_new("java/lang/Exception", format!("Rollback failed: {:?}", e)).unwrap();
        // });
    } else {
        // env.throw_new("java/lang/Exception", "Invalid connection ID").unwrap();
    }
}
