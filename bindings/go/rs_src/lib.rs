mod rows;
#[allow(dead_code)]
mod statement;
mod types;
use std::{
    ffi::{c_char, c_void},
    sync::Arc,
};
use turso_core::{Connection, LimboError};

/// # Safety
/// Safe to be called from Go with null terminated DSN string.
/// performs null check on the path.
#[no_mangle]
#[allow(clippy::arc_with_non_send_sync)]
pub unsafe extern "C" fn db_open(path: *const c_char) -> *mut c_void {
    if path.is_null() {
        println!("Path is null");
        return std::ptr::null_mut();
    }
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path = path.to_str().unwrap();
    let Ok((io, conn)) = Connection::from_uri(path, false, false) else {
        panic!("Failed to open connection with path: {path}");
    };
    LimboConn::new(conn, io).to_ptr()
}

#[allow(dead_code)]
struct LimboConn {
    conn: Arc<Connection>,
    io: Arc<dyn turso_core::IO>,
    err: Option<LimboError>,
}

impl LimboConn {
    fn new(conn: Arc<Connection>, io: Arc<dyn turso_core::IO>) -> Self {
        LimboConn {
            conn,
            io,
            err: None,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_ptr(self) -> *mut c_void {
        Box::into_raw(Box::new(self)) as *mut c_void
    }

    fn from_ptr(ptr: *mut c_void) -> &'static mut LimboConn {
        if ptr.is_null() {
            panic!("Null pointer");
        }
        unsafe { &mut *(ptr as *mut LimboConn) }
    }

    fn get_error(&mut self) -> *const c_char {
        if let Some(err) = &self.err {
            let err = format!("{err}");
            let c_str = std::ffi::CString::new(err).unwrap();
            self.err = None;
            c_str.into_raw() as *const c_char
        } else {
            std::ptr::null()
        }
    }
}
/// Get the error value from the connection, if any, as a null
/// terminated string. The caller is responsible for freeing the
/// memory with `free_string`.
#[no_mangle]
pub extern "C" fn db_get_error(ctx: *mut c_void) -> *const c_char {
    if ctx.is_null() {
        return std::ptr::null();
    }
    let conn = LimboConn::from_ptr(ctx);
    conn.get_error()
}

/// Close the database connection
/// # Safety
/// safely frees the connection's memory
#[no_mangle]
pub unsafe extern "C" fn db_close(db: *mut c_void) {
    if !db.is_null() {
        let _ = unsafe { Box::from_raw(db as *mut LimboConn) };
    }
}
