extern "C" {
    fn sqlite3_kvstore_init(
        db: *mut std::ffi::c_void,
        err_msg: *mut *mut i8,
        api: *const std::ffi::c_void,
    ) -> i32;
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_limbosqlitetestext_init(
    db: *mut std::ffi::c_void,
    err_msg: *mut *mut i8,
    api: *const std::ffi::c_void,
) {
    let _ = sqlite3_kvstore_init(db, err_msg, api);
}
