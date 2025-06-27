extern "C" {
    fn sqlite3_kvstore_init(
        db: *mut std::ffi::c_void,
        err_msg: *mut *mut i8,
        api: *const std::ffi::c_void,
    ) -> i32;
}

#[no_mangle]
/// Initialize the Limbo SQLite Test Extension.
///
/// # Safety
///
/// This function is unsafe because it interacts with raw pointers and FFI.
/// Caller must ensure that `db`, `err_msg`, and `api` are valid pointers,
/// and that the SQLite database handle is properly initialized.
pub unsafe extern "C" fn sqlite3_limbosqlitetestext_init(
    db: *mut std::ffi::c_void,
    err_msg: *mut *mut i8,
    api: *const std::ffi::c_void,
) {
    let _ = sqlite3_kvstore_init(db, err_msg, api);
}
