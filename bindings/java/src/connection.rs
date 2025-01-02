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
    println!("cursor: {:?}", to_connection(connection_id).hello);
    // env.throw_new("java/lang/Exception", "Invalid connection ID").unwrap();
    JObject::null()
}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_close<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_id: jlong,
) {
    println!("close: {:?}", to_connection(connection_id).hello);
}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_commit<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_id: jlong,
) {
    println!("commit: {:?}", to_connection(connection_id).hello);
}

#[no_mangle]
pub extern "system" fn Java_limbo_Connection_rollback<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_id: jlong,
) {
    println!("rollback: {:?}", to_connection(connection_id).hello);
}

fn to_connection(connection_ptr: jlong) -> &'static mut Connection {
    unsafe { &mut *(connection_ptr as *mut Connection) }
}
