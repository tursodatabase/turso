use std::borrow::Cow;
use std::ffi::CStr;
use std::num::NonZero;
use std::os::raw::c_char;
use std::ptr::null;
use std::slice;
use std::sync::Arc;
use turso_core::types::Text;
use turso_core::{self, Connection, Statement, StepResult, Value, IO};

type Error = *const i8;

#[repr(C)]
pub struct Database {
    io: Arc<dyn IO>,
    connection: Arc<Connection>,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub enum ValueType {
    Empty = 0,
    Null = 1,
    Integer = 2,
    Float = 3,
    Text = 4,
    Blob = 5,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Array {
    ptr: *const u8,
    len: usize,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub union TursoValueUnion {
    int_val: i64,
    real_val: f64,
    text: Array,
    blob: Array,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct TursoValue {
    value_type: ValueType,
    value: TursoValueUnion,
}

pub fn allocate<T>(value: T) -> *const T {
    Box::into_raw(Box::new(value))
}

pub fn allocate_string(str: &str) -> *const c_char {
    std::ffi::CString::new(str).unwrap().into_raw()
}

pub fn to_vec(array: Array) -> Vec<u8> {
    unsafe {
        let slice = slice::from_raw_parts(array.ptr, array.len);
        slice.to_vec()
    }
}

pub fn to_value(value: TursoValue) -> Value {
    match value.value_type {
        ValueType::Empty => Value::Null,
        ValueType::Null => Value::Null,
        ValueType::Integer => Value::Integer(unsafe { value.value.int_val }),
        ValueType::Float => Value::Float(unsafe { value.value.real_val }),
        ValueType::Blob => Value::Blob(to_vec(unsafe { value.value.blob })),
        ValueType::Text => Value::Text(Text {
            value: to_vec(unsafe { value.value.text }),
            subtype: turso_core::types::TextSubtype::Text,
        }),
    }
}

/// Opens a database at the specified path and returns a pointer to the database.
/// If an error occurred, returns null and writes a pointer to a null-terminated string into `error_ptr`.
///
/// # Safety
///
/// - The returned database pointer must be freed with `db_close`.
/// - Any error string written to `error_ptr` must be freed with `free_string`.
/// - `path_ptr` must not be null and must point to a valid null-terminated UTF-8 string.
/// - `error_ptr` must not be null and must point to a valid writable location.
#[no_mangle]
pub unsafe extern "C" fn db_open(
    path_ptr: *const c_char,
    error_ptr: *mut Error,
) -> *const Database {
    let path_cstr: &CStr = unsafe { CStr::from_ptr(path_ptr) };
    let path_str = path_cstr.to_str();

    let connection_result =
        Connection::from_uri(path_str.unwrap(), true, false, false, false, false, false);
    match connection_result {
        Ok((io, val)) => allocate(Database {
            io,
            connection: val,
        }),
        Err(err) => {
            unsafe {
                *error_ptr =
                    allocate_string(format!("Error while opening database: {err}").as_str())
            }
            null()
        }
    }
}

/// Disposes the database pointer.
///
/// # Safety
///
/// - `db_ptr` must be a pointer allocated by `db_open`.
/// - Call `db_close` only once per `db_ptr`.
#[no_mangle]
pub unsafe extern "C" fn db_close(db_ptr: *mut Database) {
    let _ = unsafe { Box::from_raw(db_ptr) };
}

/// Frees a null-terminated string previously allocated by this library.
///
/// # Safety
///
/// - `string_ptr` must be a pointer returned by this library (e.g., error messages, column names).
/// - Call `free_string` only once per `string_ptr`.
#[no_mangle]
pub unsafe extern "C" fn free_string(string_ptr: *mut c_char) {
    unsafe { drop(std::ffi::CString::from_raw(string_ptr)) };
}

/// Prepares an SQL statement and returns a pointer to the prepared statement.
/// If an error occurred, returns null and writes a pointer to a null-terminated string into `error_ptr`.
///
/// # Safety
///
/// - `db_ptr` must not be null.
/// - `sql_ptr` must not be null and must point to a valid null-terminated UTF-8 string.
/// - `error_ptr` must not be null and must point to a valid writable location.
/// - When not null, the statement pointer must be freed with `free_statement` and any error string with `free_string`.
#[no_mangle]
pub unsafe extern "C" fn db_prepare_statement(
    db_ptr: *mut Database,
    sql_ptr: *const c_char,
    error_ptr: *mut Error,
) -> *const Statement {
    let sql = unsafe { CStr::from_ptr(sql_ptr) }.to_str();
    let db = unsafe { &mut (*db_ptr) };

    let prepare_result = db.connection.prepare(sql.unwrap());
    match prepare_result {
        Ok(statement) => allocate(statement),
        Err(e) => {
            unsafe {
                *error_ptr = allocate_string(format!("Unable to prepare statement: {e}").as_str())
            }
            null()
        }
    }
}

/// Binds a parameter to the statement by index.
///
/// # Safety
///
/// - `statement_ptr` must be a pointer returned by `db_prepare_statement`.
/// - `index` must be >= 1.
/// - `parameter_value` must be a valid pointer to a `TursoValue`.
#[no_mangle]
pub unsafe extern "C" fn bind_parameter(
    statement_ptr: *mut Statement,
    index: i32,
    parameter_value: *const TursoValue,
) {
    let statement = unsafe { &mut (*statement_ptr) };
    statement.bind_at(
        NonZero::new(index.try_into().unwrap()).unwrap(),
        to_value(*parameter_value),
    );
}

/// Binds a parameter to the statement by name.
///
/// # Safety
///
/// - `statement_ptr` must be a pointer returned by `db_prepare_statement`.
/// - `parameter_name` must not be null and must point to a valid null-terminated UTF-8 string.
/// - `parameter_value` must be a valid pointer to a `TursoValue`.
#[no_mangle]
pub unsafe extern "C" fn bind_named_parameter(
    statement_ptr: *mut Statement,
    parameter_name: *const c_char,
    parameter_value: *const TursoValue,
) {
    let statement = unsafe { &mut (*statement_ptr) };
    let parameter_name = unsafe { CStr::from_ptr(parameter_name) }.to_str().unwrap();

    for idx in 1..statement.parameters_count() + 1 {
        let non_zero_idx = NonZero::new(idx).unwrap();
        let param = statement.parameters().name(non_zero_idx);
        let Some(name) = param else {
            continue;
        };
        if parameter_name == name {
            statement.bind_at(non_zero_idx, to_value(*parameter_value));
            return;
        }
    }
}

/// Returns the number of rows changed by the statement.
///
/// # Safety
///
/// - `statement_ptr` must not be null.
#[no_mangle]
pub unsafe extern "C" fn db_statement_nchange(statement_ptr: *mut Statement) -> i64 {
    let statement = unsafe { &mut (*statement_ptr) };
    statement.n_change()
}

/// Executes the statement, advancing it by one step.
/// If an error occurred, sets `error_ptr` to a pointer to a null-terminated string.
///
/// # Safety
///
/// - `statement_ptr` must not be null.
/// - `error_ptr` must not be null and must point to a location that is valid for writing.
/// - If set, the error string must be freed with `free_string`.
#[no_mangle]
pub unsafe extern "C" fn db_statement_execute_step(
    statement_ptr: *mut Statement,
    error_ptr: *mut Error,
) -> bool {
    let statement = unsafe { &mut (*statement_ptr) };

    loop {
        match statement.step() {
            Ok(step_result) => match step_result {
                StepResult::Row => {
                    return true;
                }
                StepResult::Done => {
                    return false;
                }
                StepResult::IO => {
                    if let Err(err) = statement.run_once() {
                        unsafe { *error_ptr = allocate_string(err.to_string().as_str()) };
                        return false;
                    }
                    continue;
                }
                StepResult::Interrupt => {
                    unsafe { *error_ptr = allocate_string("Interrupted") };
                    return false;
                }
                StepResult::Busy => {
                    unsafe { *error_ptr = allocate_string("Database is busy") };
                    return false;
                }
            },
            Err(err) => {
                unsafe { *error_ptr = allocate_string(err.to_string().as_str()) };
                return false;
            }
        }
    }
}

/// Frees the statement pointer.
///
/// # Safety
///
/// - `statement_ptr` must not be null.
/// - Call `free_statement` only once per `statement_ptr`.
#[no_mangle]
pub unsafe extern "C" fn free_statement(statement_ptr: *mut Statement) {
    let mut statement = unsafe { Box::from_raw(statement_ptr) };
    statement.reset();
}

/// Gets the current value from the row at the specified column index.
///
/// # Safety
///
/// - `statement_ptr` must not be null.
/// - `col_idx` must be >= 0.
#[no_mangle]
pub unsafe extern "C" fn db_statement_get_value(
    statement_ptr: *mut Statement,
    col_idx: i32,
) -> TursoValue {
    let statement = unsafe { &mut (*statement_ptr) };
    if let Some(row) = statement.row() {
        let value = match row.get_value(col_idx.try_into().unwrap()) {
            Value::Null => TursoValue {
                value_type: ValueType::Null,
                value: TursoValueUnion { int_val: 0 },
            },
            Value::Integer(int_val) => TursoValue {
                value_type: ValueType::Integer,
                value: TursoValueUnion { int_val: *int_val },
            },
            Value::Float(float_value) => TursoValue {
                value_type: ValueType::Float,
                value: TursoValueUnion {
                    real_val: *float_value,
                },
            },
            Value::Text(text) => {
                let array = Array {
                    ptr: text.value.as_ptr(),
                    len: text.value.len(),
                };
                TursoValue {
                    value_type: ValueType::Text,
                    value: TursoValueUnion { text: array },
                }
            }
            Value::Blob(blob) => {
                let bytes = blob.as_ptr();
                let array = Array {
                    ptr: bytes,
                    len: blob.len(),
                };
                TursoValue {
                    value_type: ValueType::Blob,
                    value: TursoValueUnion { blob: array },
                }
            }
        };

        return value;
    }

    TursoValue {
        value_type: ValueType::Empty,
        value: TursoValueUnion { int_val: 0 },
    }
}

/// Gets the number of columns in the current statement.
///
/// # Safety
///
/// - `statement_ptr` must not be null.
#[no_mangle]
pub unsafe extern "C" fn db_statement_num_columns(statement_ptr: *mut Statement) -> i32 {
    let statement = unsafe { &mut (*statement_ptr) };
    statement.num_columns().try_into().unwrap()
}

/// Gets the column name for the specified index.
/// The returned string is heap-allocated; free it with `free_string` when no longer needed.
///
/// # Safety
///
/// - `statement_ptr` must not be null.
/// - `index` must be >= 0.
#[no_mangle]
pub unsafe extern "C" fn db_statement_column_name(
    statement_ptr: *mut Statement,
    index: i32,
) -> *const i8 {
    let statement = unsafe { &mut (*statement_ptr) };
    let col_name = statement.get_column_name(index.try_into().unwrap());
    match col_name {
        Cow::Borrowed(value) => allocate_string(value),
        Cow::Owned(value) => allocate_string(value.as_str()),
    }
}

/// Checks whether the statement currently points to a row.
///
/// # Safety
///
/// - `statement_ptr` must not be null.
#[no_mangle]
pub unsafe extern "C" fn db_statement_has_rows(statement_ptr: *mut Statement) -> bool {
    let statement = unsafe { &mut (*statement_ptr) };
    match statement.row() {
        Some(_val) => true,
        None => false,
    }
}
