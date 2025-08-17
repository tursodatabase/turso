use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use crate::{sqlite3, sqlite3_stmt, SQLITE_OK, SQLITE_ERROR, sqlite3_open, sqlite3_close, sqlite3_prepare_v2, sqlite3_step, sqlite3_finalize, sqlite3_column_text, sqlite3_column_count, sqlite3_column_type, sqlite3_column_int, sqlite3_column_double, sqlite3_column_blob, sqlite3_column_bytes, SQLITE_ROW, SQLITE_DONE, SQLITE_NULL, SQLITE_INTEGER, SQLITE_FLOAT, SQLITE_TEXT, SQLITE_BLOB};

// Tcl version compatibility
#[cfg(feature = "tcl9")]
type Tcl_Size = usize;
#[cfg(not(feature = "tcl9"))]
type Tcl_Size = c_int;

pub struct TclDatabase {
    pub db: *mut sqlite3,
    pub interp: *mut c_void,
    pub z_busy: Option<CString>,
    pub z_trace: Option<CString>,
    pub z_commit: Option<CString>,
    pub z_null: Option<CString>,
    pub n_ref: c_int,
}

impl TclDatabase {
    pub fn new(db: *mut sqlite3, interp: *mut c_void) -> Self {
        Self {
            db,
            interp,
            z_busy: None,
            z_trace: None,
            z_commit: None,
            z_null: None,
            n_ref: 1,
        }
    }
}

// Tcl function types
type TclObjCmdProc = unsafe extern "C" fn(
    client_data: *mut c_void,
    interp: *mut c_void,
    objc: c_int,
    objv: *const *mut c_void,
) -> c_int;

type TclCmdDeleteProc = extern "C" fn(client_data: *mut c_void);

// Raw Tcl API bindings - using dynamic loading to avoid version conflicts
extern "C" {
    fn Tcl_CreateObjCommand(
        interp: *mut c_void,
        cmd_name: *const c_char,
        proc: TclObjCmdProc,
        client_data: *mut c_void,
        delete_proc: Option<TclCmdDeleteProc>,
    ) -> *mut c_void;

    fn Tcl_GetStringFromObj(obj: *mut c_void, length: *mut Tcl_Size) -> *const c_char;
    fn Tcl_NewStringObj(bytes: *const c_char, length: c_int) -> *mut c_void;
    fn Tcl_SetObjResult(interp: *mut c_void, obj: *mut c_void);
    fn Tcl_NewListObj(objc: c_int, objv: *const *mut c_void) -> *mut c_void;
    fn Tcl_NewIntObj(int_value: c_int) -> *mut c_void;
    fn Tcl_NewDoubleObj(double_value: f64) -> *mut c_void;
    fn Tcl_NewByteArrayObj(bytes: *const c_char, length: c_int) -> *mut c_void;
    fn Tcl_SetErrorCode(interp: *mut c_void, ...) -> c_int;
}

// Tcl-specific error handling
unsafe fn tcl_set_sqlite_error(interp: *mut c_void, db: *mut sqlite3, operation: &str) {
    let error_msg = if !db.is_null() {
        let error_ptr = crate::sqlite3_errmsg(db);
        if !error_ptr.is_null() {
            let error_str = CStr::from_ptr(error_ptr).to_string_lossy();
            format!("{}: {}", operation, error_str)
        } else {
            format!("{}: unknown SQLite error", operation)
        }
    } else {
        format!("{}: database is null", operation)
    };

    let error_cstr = CString::new(error_msg).unwrap();
    Tcl_SetErrorCode(interp, b"SQLITE\0".as_ptr() as *const c_char, ptr::null::<c_char>());
    Tcl_SetObjResult(interp, Tcl_NewStringObj(error_cstr.as_ptr(), -1));
}

// Tcl extension entry points
#[no_mangle]
pub extern "C" fn Sqlite3_Init(interp: *mut c_void) -> c_int {
    unsafe {
        // Register the sqlite3 command
        Tcl_CreateObjCommand(
            interp,
            b"sqlite3\0".as_ptr() as *const c_char,
            db_main_command,
            ptr::null_mut(),
            None,
        );
    }
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn Tclsqlite_Init(interp: *mut c_void) -> c_int {
    Sqlite3_Init(interp)
}

// Tcl expects this symbol based on the library name libturso_sqlite3.dylib
#[no_mangle]
pub extern "C" fn Turso_sqlite_Init(interp: *mut c_void) -> c_int {
    Sqlite3_Init(interp)
}

// Tcl appends _Init to symbol names, so we need these variants
#[no_mangle]
pub extern "C" fn Sqlite3_Init_Init(interp: *mut c_void) -> c_int {
    Sqlite3_Init(interp)
}

#[no_mangle]
pub extern "C" fn Tclsqlite_Init_Init(interp: *mut c_void) -> c_int {
    Sqlite3_Init(interp)
}

#[no_mangle]
pub extern "C" fn Turso_sqlite_Init_Init(interp: *mut c_void) -> c_int {
    Sqlite3_Init(interp)
}

// Main sqlite3 command handler
unsafe extern "C" fn db_main_command(
    _client_data: *mut c_void,
    interp: *mut c_void,
    objc: c_int,
    objv: *const *mut c_void,
) -> c_int {
    // Check argument count
    if objc < 2 || objc > 3 {
        return SQLITE_ERROR;
    }

    // Get database name from first argument
    let handle_obj = *objv.offset(1);
    let handle_str = Tcl_GetStringFromObj(handle_obj, ptr::null_mut());
    let handle_name = match CStr::from_ptr(handle_str).to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_ERROR,
    };

    // Get filename from second argument (optional)
    let filename = if objc == 3 {
        let filename_obj = *objv.offset(2);
        let filename_str = Tcl_GetStringFromObj(filename_obj, ptr::null_mut());
        match CStr::from_ptr(filename_str).to_str() {
            Ok(s) => s,
            Err(_) => ":memory:",
        }
    } else {
        ":memory:"
    };

    // Open the database
    let mut db_ptr: *mut sqlite3 = ptr::null_mut();
    let filename_cstr = match CString::new(filename) {
        Ok(s) => s,
        Err(_) => return SQLITE_ERROR,
    };

    let result = sqlite3_open(filename_cstr.as_ptr(), &mut db_ptr);
    if result != SQLITE_OK {
        return SQLITE_ERROR;
    }

    // Create TclDatabase and register the command
    let tcl_db = Box::new(TclDatabase::new(db_ptr, interp));
    let tcl_db_ptr = Box::into_raw(tcl_db);

    let handle_cstr = match CString::new(handle_name) {
        Ok(s) => s,
        Err(_) => return SQLITE_ERROR,
    };

    Tcl_CreateObjCommand(
        interp,
        handle_cstr.as_ptr(),
        db_object_command,
        tcl_db_ptr as *mut c_void,
        Some(db_delete_proc),
    );

    SQLITE_OK
}

// Database object command handler
unsafe extern "C" fn db_object_command(
    client_data: *mut c_void,
    interp: *mut c_void,
    objc: c_int,
    objv: *const *mut c_void,
) -> c_int {
    if objc < 2 {
        return SQLITE_ERROR;
    }

    let tcl_db = client_data as *mut TclDatabase;
    if tcl_db.is_null() {
        return SQLITE_ERROR;
    }

    let subcommand_obj = *objv.offset(1);
    let subcommand_str = Tcl_GetStringFromObj(subcommand_obj, ptr::null_mut());
    let subcommand = match CStr::from_ptr(subcommand_str).to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_ERROR,
    };

    match subcommand {
        "eval" => db_eval_command(tcl_db, interp, objc, objv),
        "close" => db_close_command(tcl_db, interp, objc, objv),
        _ => SQLITE_ERROR,
    }
}

// Execute SQL command
unsafe fn db_eval_command(
    _tcl_db: *mut TclDatabase,
    _interp: *mut c_void,
    _objc: c_int,
    _objv: *const *mut c_void,
) -> c_int {
    // For now, just return success without using any Tcl object functions
    // This helps us isolate where the issue is
    SQLITE_OK
}

// Close database
unsafe fn db_close_command(
    _tcl_db: *mut TclDatabase,
    _interp: *mut c_void,
    _objc: c_int,
    _objv: *const *mut c_void,
) -> c_int {
    // For now, just return success
    // We'll add the full functionality step by step
    SQLITE_OK
}

// Function command implementation (placeholder for now)
unsafe fn db_function_command(
    _tcl_db: *mut TclDatabase,
    interp: *mut c_void,
    objc: c_int,
    _objv: *const *mut c_void,
) -> c_int {
    if objc < 4 {
        Tcl_SetObjResult(
            interp,
            Tcl_NewStringObj(b"wrong # args: should be \"db function name script\"\0".as_ptr() as *const c_char, -1),
        );
        return SQLITE_ERROR;
    }

    // TODO: Implement Tcl function registration
    Tcl_SetObjResult(
        interp,
        Tcl_NewStringObj(b"function registration not yet implemented\0".as_ptr() as *const c_char, -1),
    );
    SQLITE_ERROR
}

// Delete procedure for cleanup
extern "C" fn db_delete_proc(client_data: *mut c_void) {
    unsafe {
        let tcl_db = client_data as *mut TclDatabase;
        if !tcl_db.is_null() {
            let db = (*tcl_db).db;
            if !db.is_null() {
                let _ = sqlite3_close(db);
            }
            let _ = Box::from_raw(tcl_db);
        }
    }
}
