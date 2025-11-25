use turso_capi_macros::signature;

mod c {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]

    include!("bindings.rs");
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_setup(config: c::turso_config_t) -> c::turso_status_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_database_init(config: c::turso_database_config_t) -> c::turso_database_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_database_connect(database: c::turso_database_t) -> c::turso_connection_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_connection_prepare(
    connection: c::turso_connection_t,
    sql: *const std::ffi::c_char,
) -> c::turso_statement_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_execute(statement: c::turso_statement_t) -> c::turso_execute_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_query(statement: c::turso_statement_t) -> c::turso_rows_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_reset(statement: c::turso_statement_t) -> c::turso_status_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_rows_next(rows: c::turso_rows_t) -> c::turso_row_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_rows_column_name(
    rows: c::turso_rows_t,
    index: std::ffi::c_int,
) -> c::turso_slice_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_rows_column_count(rows: c::turso_rows_t) -> std::ffi::c_int {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_row_value(
    row: c::turso_row_t,
    index: std::ffi::c_int,
) -> c::turso_result_value_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_row_name(row: c::turso_row_t, index: std::ffi::c_int) -> c::turso_slice_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_row_empty(row: c::turso_row_t) -> bool {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_bind_named(
    stmt: c::turso_statement_t,
    name: *const std::ffi::c_char,
    value: c::turso_value_t,
) -> c::turso_status_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_bind_positional(
    stmt: c::turso_statement_t,
    position: std::ffi::c_int,
    value: c::turso_value_t,
) -> c::turso_status_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_integer(integer: i64) -> c::turso_value_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_real(real: f64) -> c::turso_value_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_text(ptr: *const std::ffi::c_char, len: usize) -> c::turso_value_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_blob(ptr: *const u8, len: usize) -> c::turso_value_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_null() -> c::turso_value_t {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_status_deinit(err: c::turso_status_t) {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_database_deinit(db: c::turso_database_t) {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_connection_deinit(connection: c::turso_connection_t) {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_statement_deinit(statement: c::turso_statement_t) {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_rows_deinit(rows: c::turso_rows_t) {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_row_deinit(row: c::turso_row_t) {
    todo!()
}

#[no_mangle]
#[signature(c)]
pub extern "C" fn turso_slice_deinit(slice: c::turso_slice_t) {
    todo!()
}
