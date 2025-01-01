use jni::objects::{JClass, JString};
use jni::JNIEnv;
use std::rc::Rc;
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
pub struct Connection {
    conn: Arc<Mutex<Rc<limbo_core::Connection>>>,
    io: Arc<limbo_core::PlatformIO>,
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

static mut CONNECTION: Option<Connection> = None;

#[no_mangle]
pub extern "system" fn Java_limbo_HelloWorld_hello<'local>(
    // Notice that this `env` argument is mutable. Any `JNIEnv` API that may
    // allocate new object references will take a mutable reference to the
    // environment.
    mut env: JNIEnv<'local>,
    // this is the class that owns our static method. Not going to be used, but
    // still needs to have an argument slot
    _class: JClass<'local>,
    input: JString<'local>,
) -> JString<'local> {
    // First, we have to get the string out of java. Check out the `strings`
    // module for more info on how this works.
    let input: String = env
        .get_string(&input)
        .expect("Couldn't get java string!")
        .into();

    // Then we have to create a new java string to return. Again, more info
    // in the `strings` module.
    let output = env
        .new_string(format!("Hello, {}!", input))
        .expect("Couldn't create java string!");
    output
}
