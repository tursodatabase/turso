use std::ffi::{CStr, CString};
use std::num::NonZero;
use std::ops::Deref;
use std::os::raw::c_char;
use std::ptr::null;
use std::slice;
use std::sync::Arc;
use turso_core::types::Text;
use turso_core::{self, Connection, Statement, StepResult, Value, IO};


#[repr(C)]
pub enum ErrorCode {
    Ok = 0,
    Error = 1,
}

#[repr(C)]
pub struct Error {
    message: CString,
}

#[repr(C)]
pub struct ErrorHandle {
    error: *const Error,
}

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
    len: usize
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

pub fn allocate_error(err: String) -> *const Error {
    let message = std::ffi::CString::new(err).unwrap();
    let error = Error{message};
    Box::into_raw(Box::new(error))
}

pub fn to_vec(array: Array) ->  Vec<u8> {
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
        ValueType::Text => Value::Text(Text{value: to_vec(unsafe { value.value.text }), subtype: turso_core::types::TextSubtype::Text})
    }
}

#[no_mangle]
pub extern "C" fn db_open(path_ptr: *const c_char, db_ptr: *mut *const Database) -> *const Error {
    let path_cstr: &CStr = unsafe { CStr::from_ptr(path_ptr) };
    let path_str = path_cstr.to_str();

    let connection_result = Connection::from_uri(path_str.unwrap(), true, false, false, false, false);
    match connection_result {
        Ok((io, val)) => {
            unsafe { *db_ptr = allocate(Database{io, connection: val}) };
            null()
        },
        Err(err) =>  allocate_error(format!("Error while opening database: {err}"))
    }
}

#[no_mangle]
pub extern "C" fn db_close(db_ptr: *mut Database) {
    let _ = unsafe { Box::from_raw(db_ptr) };
}

#[no_mangle]
pub extern "C" fn free_error(error_ptr: *mut Error) {
    let _ = unsafe { Box::from_raw(error_ptr) };
}

#[no_mangle]
pub extern "C" fn db_prepare_statement(db_ptr: *mut Database, sql_ptr: *const c_char,  error_handle_ptr: *mut ErrorHandle) -> *const Statement {
    let sql = unsafe { CStr::from_ptr(sql_ptr) }.to_str();
    let db = unsafe {&mut (*db_ptr)};
    let error_handle = unsafe {&mut (*error_handle_ptr)};
    
    let prepare_result = db.connection.prepare(sql.unwrap());    
    match prepare_result {
        Ok(statement) => allocate(statement),
        Err(e) => {
            error_handle.error = allocate_error(format!("Unable to prepare statment: {e}"));
            null()
        },
    }
}

#[no_mangle]
pub extern "C" fn bind_parameter(statement_ptr: *mut Statement, index: usize, parameter_value: *mut TursoValue) {
    let statement = unsafe {&mut (*statement_ptr)};
    let parameter = unsafe {&mut (*parameter_value)};
    statement.bind_at(NonZero::new(index).unwrap(), to_value(*parameter));
}

#[no_mangle]
pub extern "C" fn bind_named_parameter(statement_ptr: *mut Statement, parameter_name: *const c_char, parameter_value_ptr: *mut TursoValue) {
    let statement = unsafe {&mut (*statement_ptr)};
    let parameter_value = unsafe {&mut (*parameter_value_ptr)};
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

#[no_mangle]
pub extern "C" fn db_statement_nchange(statement_ptr: *mut Statement) -> i64 {
    let statement = unsafe {&mut (*statement_ptr)};
    statement.n_change()
}

#[no_mangle]
pub extern "C" fn db_statement_execute_step(statement_ptr: *mut Statement, error_handle_ptr: *mut ErrorHandle) -> bool {
    let statement = unsafe {&mut (*statement_ptr)};
    let error_handle = unsafe {&mut (*error_handle_ptr)};

    loop {
        match statement.step() {
            Ok(step_result) => {
                match step_result {
                    StepResult::Row => { return true; }
                    StepResult::Done => { return false; }
                    StepResult::IO => {
                        if let Err(err) = statement.run_once() {
                            error_handle.error = allocate_error(err.to_string());
                            return false;
                        }
                        continue;
                    }
                    StepResult::Interrupt => {
                        error_handle.error = allocate_error("Interrupted".to_string());
                        return false; 
                    }
                    StepResult::Busy => { 
                        error_handle.error = allocate_error("Database is busy".to_string());
                        return false; 
                    }
                }
            },
            Err(err) => {
                error_handle.error = allocate_error(err.to_string());
                return false;
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn free_statement(statement_ptr: *mut Statement) {
    let mut statement = unsafe { Box::from_raw(statement_ptr) };
    statement.reset();
}

#[no_mangle]
pub extern "C" fn db_statement_get_value(statement_ptr: *mut Statement, col_idx: usize) -> TursoValue {
    let statement = unsafe {&mut (*statement_ptr)};
    if let Some(row) = statement.row() {
        let value = match row.get_value(col_idx) {
            Value::Null => TursoValue{value_type: ValueType::Null, value: TursoValueUnion{int_val: 0}},
            Value::Integer(int_val) => TursoValue{value_type: ValueType::Integer, value: TursoValueUnion{int_val: *int_val}},
            Value::Float(float_value) => TursoValue{value_type: ValueType::Float, value: TursoValueUnion{real_val: *float_value}},
            Value::Text(text) => {
                let array = Array{ptr: text.value.as_ptr(), len: text.value.len()};
                TursoValue{
                    value_type: ValueType::Text, 
                    value: TursoValueUnion {
                        text: array,
                    }
                }
            },
            Value::Blob(blob) => {
                let bytes = blob.as_ptr();
                let array = Array{ptr: bytes, len: blob.len()};
                TursoValue{
                    value_type: ValueType::Blob, 
                    value: TursoValueUnion {
                        blob: array,
                    }
                }
            },
        };

        return value;
    }

    TursoValue { value_type: ValueType::Empty, value: TursoValueUnion{int_val: 0}}
}


