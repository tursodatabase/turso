#![allow(clippy::not_unsafe_ptr_arg_deref)]

use turso_sdk_kit::{
    capi::{
        self,
        c::{turso_slice_ref_t, turso_status_code_t},
    },
    rsapi::{bytes_from_turso_slice, str_from_turso_slice},
};
use turso_sdk_kit_macros::signature;

use crate::{
    rsapi::{self, TursoDatabaseSyncChanges},
    sync_engine_io::SyncEngineIoQueueItem,
    turso_async_operation::TursoDatabaseAsyncOperation,
};

pub mod c {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]

    include!("bindings.rs");
}

type TursoDatabaseSync = rsapi::TursoDatabaseSync<Vec<u8>>;

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_new(
    db_config: *const capi::c::turso_database_config_t,
    sync_config: *const c::turso_sync_database_config_t,
    db_ref: *mut *const c::turso_sync_database_t,
    error_opt_out: *mut *const std::ffi::c_char,
) -> turso_sdk_kit::capi::c::turso_status_code_t {
    let db_config = match unsafe { turso_sdk_kit::rsapi::TursoDatabaseConfig::from_capi(db_config) }
    {
        Ok(sync_config) => sync_config,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    let sync_config = match unsafe { rsapi::TursoDatabaseSyncConfig::from_capi(sync_config) } {
        Ok(sync_config) => sync_config,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    let db = match TursoDatabaseSync::new(db_config, sync_config) {
        Ok(db) => db,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    unsafe { *db_ref = db.to_capi() };
    capi::c::turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_open(
    db: *const c::turso_sync_database_t,
    operation: *mut *const c::turso_sync_operation_t,
    error_opt_out: *mut *const std::ffi::c_char,
) -> turso_sdk_kit::capi::c::turso_status_code_t {
    let db = match unsafe { TursoDatabaseSync::ref_from_capi(db) } {
        Ok(db) => db,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    unsafe { *operation = db.open().to_capi() };
    capi::c::turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_create(
    db: *const c::turso_sync_database_t,
    operation: *mut *const c::turso_sync_operation_t,
    error_opt_out: *mut *const std::ffi::c_char,
) -> turso_sdk_kit::capi::c::turso_status_code_t {
    let db = match unsafe { TursoDatabaseSync::ref_from_capi(db) } {
        Ok(db) => db,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    unsafe { *operation = db.create().to_capi() };
    capi::c::turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_connect(
    db: *const c::turso_sync_database_t,
    operation: *mut *const c::turso_sync_operation_t,
    error_opt_out: *mut *const std::ffi::c_char,
) -> turso_sdk_kit::capi::c::turso_status_code_t {
    let db = match unsafe { TursoDatabaseSync::ref_from_capi(db) } {
        Ok(db) => db,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    unsafe { *operation = db.connect().to_capi() };
    capi::c::turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_stats(
    db: *const c::turso_sync_database_t,
    operation: *mut *const c::turso_sync_operation_t,
    error_opt_out: *mut *const std::ffi::c_char,
) -> turso_sdk_kit::capi::c::turso_status_code_t {
    let db = match unsafe { TursoDatabaseSync::ref_from_capi(db) } {
        Ok(db) => db,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    unsafe { *operation = db.stats().to_capi() };
    capi::c::turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_checkpoint(
    db: *const c::turso_sync_database_t,
    operation: *mut *const c::turso_sync_operation_t,
    error_opt_out: *mut *const std::ffi::c_char,
) -> turso_sdk_kit::capi::c::turso_status_code_t {
    let db = match unsafe { TursoDatabaseSync::ref_from_capi(db) } {
        Ok(db) => db,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    unsafe { *operation = db.checkpoint().to_capi() };
    capi::c::turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_push_changes(
    db: *const c::turso_sync_database_t,
    operation: *mut *const c::turso_sync_operation_t,
    error_opt_out: *mut *const std::ffi::c_char,
) -> turso_sdk_kit::capi::c::turso_status_code_t {
    let db = match unsafe { TursoDatabaseSync::ref_from_capi(db) } {
        Ok(db) => db,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    unsafe { *operation = db.push_changes().to_capi() };
    capi::c::turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_wait_changes(
    db: *const c::turso_sync_database_t,
    operation: *mut *const c::turso_sync_operation_t,
    error_opt_out: *mut *const std::ffi::c_char,
) -> turso_sdk_kit::capi::c::turso_status_code_t {
    let db = match unsafe { TursoDatabaseSync::ref_from_capi(db) } {
        Ok(db) => db,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    unsafe { *operation = db.wait_changes().to_capi() };
    capi::c::turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_apply_changes(
    db: *const c::turso_sync_database_t,
    changes: *const c::turso_sync_changes_t,
    operation: *mut *const c::turso_sync_operation_t,
    error_opt_out: *mut *const std::ffi::c_char,
) -> turso_sdk_kit::capi::c::turso_status_code_t {
    let db = match unsafe { TursoDatabaseSync::ref_from_capi(db) } {
        Ok(db) => db,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    unsafe {
        *operation = db
            .apply_changes(TursoDatabaseSyncChanges::box_from_capi(changes))
            .to_capi()
    };
    capi::c::turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_operation_resume(
    operation: *const c::turso_sync_operation_t,
    error_opt_out: *mut *const std::ffi::c_char,
) -> turso_status_code_t {
    let operation = match unsafe { TursoDatabaseAsyncOperation::ref_from_capi(operation) } {
        Ok(operation) => operation,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    let result = match operation.resume() {
        Ok(result) => result,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    result.to_capi()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_operation_result_kind(
    operation: *const c::turso_sync_operation_t,
) -> c::turso_sync_operation_result_type_t {
    let operation = match unsafe { TursoDatabaseAsyncOperation::ref_from_capi(operation) } {
        Ok(operation) => operation,
        Err(_) => return c::turso_sync_operation_result_type_t::TURSO_ASYNC_RESULT_NONE,
    };
    operation.result_kind_to_capi()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_operation_result_extract_connection(
    operation: *const c::turso_sync_operation_t,
    connection_ref: *mut *const turso_sdk_kit::capi::c::turso_connection_t,
) -> turso_status_code_t {
    let operation = match unsafe { TursoDatabaseAsyncOperation::ref_from_capi(operation) } {
        Ok(operation) => operation,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    let connection = match operation.take_connection_to_capi() {
        Ok(result) => result,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    unsafe { *connection_ref = connection };
    turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_operation_result_extract_changes(
    operation: *const c::turso_sync_operation_t,
    changes_ref: *mut *const c::turso_sync_changes_t,
) -> turso_status_code_t {
    let operation = match unsafe { TursoDatabaseAsyncOperation::ref_from_capi(operation) } {
        Ok(operation) => operation,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    let changes = match operation.take_changes_to_capi() {
        Ok(result) => result,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    unsafe { *changes_ref = changes };
    turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_operation_result_extract_stats(
    operation: *const c::turso_sync_operation_t,
    stats_ref: *mut c::turso_sync_stats_t,
) -> turso_status_code_t {
    let operation = match unsafe { TursoDatabaseAsyncOperation::ref_from_capi(operation) } {
        Ok(operation) => operation,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    let stats = match operation.get_stats_to_capi() {
        Ok(result) => result,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    unsafe { *stats_ref = stats };
    turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_io_take_item(
    db: *const c::turso_sync_database_t,
    item_ref: *mut *const c::turso_sync_io_item_t,
    error_opt_out: *mut *const std::ffi::c_char,
) -> turso_status_code_t {
    let db = match unsafe { TursoDatabaseSync::ref_from_capi(db) } {
        Ok(db) => db,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    match db.take_io_item() {
        Some(item) => {
            unsafe { *item_ref = item.to_capi() };
            turso_status_code_t::TURSO_OK
        }
        None => {
            unsafe { *item_ref = std::ptr::null_mut() };
            turso_status_code_t::TURSO_OK
        }
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_io_step_callbacks(
    db: *const c::turso_sync_database_t,
    error_opt_out: *mut *const std::ffi::c_char,
) -> turso_status_code_t {
    let db = match unsafe { TursoDatabaseSync::ref_from_capi(db) } {
        Ok(db) => db,
        Err(err) => return unsafe { err.to_capi(error_opt_out) },
    };
    db.step_io_callbacks();
    turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_io_request_kind(
    request: *const c::turso_sync_io_item_t,
) -> c::turso_sync_io_request_type_t {
    let request = match unsafe { SyncEngineIoQueueItem::<Vec<u8>>::ref_from_capi(request) } {
        Ok(request) => request,
        Err(_) => return c::turso_sync_io_request_type_t::TURSO_SYNC_IO_NONE,
    };
    request.get_request().kind_to_capi()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_io_request_http(
    request: *const c::turso_sync_io_item_t,
    http_ref: *mut c::turso_sync_io_http_request_t,
) -> turso_status_code_t {
    let request = match unsafe { SyncEngineIoQueueItem::<Vec<u8>>::ref_from_capi(request) } {
        Ok(request) => request,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    let http = match request.get_request().http_to_capi() {
        Ok(http) => http,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    unsafe { *http_ref = http };
    turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_io_request_http_header(
    request: *const c::turso_sync_io_item_t,
    index: usize,
    header_ref: *mut c::turso_sync_io_http_header_t,
) -> turso_status_code_t {
    let request = match unsafe { SyncEngineIoQueueItem::<Vec<u8>>::ref_from_capi(request) } {
        Ok(request) => request,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    let header = match request.get_request().header_to_capi(index) {
        Ok(heaeder) => heaeder,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    unsafe { *header_ref = header };
    turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_io_request_full_read(
    request: *const c::turso_sync_io_item_t,
    full_read_ref: *mut c::turso_sync_io_full_read_request_t,
) -> turso_status_code_t {
    let request = match unsafe { SyncEngineIoQueueItem::<Vec<u8>>::ref_from_capi(request) } {
        Ok(request) => request,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    let full_read = match request.get_request().full_read_to_capi() {
        Ok(full_read) => full_read,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    unsafe { *full_read_ref = full_read };
    turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_io_request_full_write(
    request: *const c::turso_sync_io_item_t,
    full_write_ref: *mut c::turso_sync_io_full_write_request_t,
) -> turso_status_code_t {
    let request = match unsafe { SyncEngineIoQueueItem::<Vec<u8>>::ref_from_capi(request) } {
        Ok(request) => request,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    let full_write = match request.get_request().full_write_to_capi() {
        Ok(full_write) => full_write,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    unsafe { *full_write_ref = full_write };
    turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_io_poison(
    request: *const c::turso_sync_io_item_t,
    error: *mut turso_slice_ref_t,
) -> turso_status_code_t {
    let request = match unsafe { SyncEngineIoQueueItem::<Vec<u8>>::ref_from_capi(request) } {
        Ok(request) => request,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    let error = match str_from_turso_slice(unsafe { *error }) {
        Ok(error) => error.to_string(),
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    request.get_completion().poison(error);
    turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_io_status(
    request: *const c::turso_sync_io_item_t,
    status: i32,
) -> turso_status_code_t {
    let request = match unsafe { SyncEngineIoQueueItem::<Vec<u8>>::ref_from_capi(request) } {
        Ok(request) => request,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    request.get_completion().status(status as u32);
    turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_io_push_buffer(
    request: *const c::turso_sync_io_item_t,
    buffer: *mut turso_slice_ref_t,
) -> turso_status_code_t {
    let request = match unsafe { SyncEngineIoQueueItem::<Vec<u8>>::ref_from_capi(request) } {
        Ok(request) => request,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    let buffer = match bytes_from_turso_slice(unsafe { *buffer }) {
        Ok(buffer) => buffer,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    request.get_completion().push_buffer(buffer.to_vec());
    turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_io_done(
    request: *const c::turso_sync_io_item_t,
) -> turso_status_code_t {
    let request = match unsafe { SyncEngineIoQueueItem::<Vec<u8>>::ref_from_capi(request) } {
        Ok(request) => request,
        Err(err) => return unsafe { err.to_capi(std::ptr::null_mut()) },
    };
    request.get_completion().done();
    turso_status_code_t::TURSO_OK
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_deinit(db: *const c::turso_sync_database_t) {
    if !db.is_null() {
        let _ = unsafe { TursoDatabaseSync::arc_from_capi(db) };
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_operation_deinit(operation: *const c::turso_sync_operation_t) {
    if !operation.is_null() {
        let _ = unsafe { TursoDatabaseAsyncOperation::box_from_capi(operation) };
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_database_io_item_deinit(item: *const c::turso_sync_io_item_t) {
    if !item.is_null() {
        let _ = unsafe { SyncEngineIoQueueItem::<Vec<u8>>::box_from_capi(item) };
    }
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_sync_changes_deinit(item: *const c::turso_sync_changes_t) {
    if !item.is_null() {
        let _ = unsafe { TursoDatabaseSyncChanges::box_from_capi(item) };
    }
}

#[cfg(test)]
#[path = "../tests/unit/capi/tests.rs"]
mod tests;
