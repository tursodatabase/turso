use crate::{ContextValue, ResultCode, Value};
use std::{
    ffi::{c_char, c_void},
    fmt::Display,
};

pub type ScalarFunction = unsafe extern "C" fn(argc: i32, *const Value) -> Value;
pub type ContextScalarFunction =
    unsafe extern "C" fn(context: usize, argc: i32, argv: *const Value, result: *mut ContextValue);
pub type ContextDestructor = unsafe extern "C" fn(context: usize);
pub type ContextValueDestructor = unsafe extern "C" fn(result: *mut ContextValue);

pub type RegisterScalarFn =
    unsafe extern "C" fn(ctx: *mut c_void, name: *const c_char, func: ScalarFunction) -> ResultCode;

pub type RegisterContextScalarFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    name: *const c_char,
    argc: i32,
    deterministic: bool,
    context: usize,
    callback: ContextScalarFunction,
    context_destructor: Option<ContextDestructor>,
    value_destructor: Option<ContextValueDestructor>,
) -> ResultCode;

pub type UnregisterFunctionFn =
    unsafe extern "C" fn(ctx: *mut c_void, name: *const c_char) -> ResultCode;

pub type RegisterAggFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    name: *const c_char,
    args: i32,
    init: InitAggFunction,
    step: StepFunction,
    finalize: FinalizeFunction,
) -> ResultCode;

pub type InitAggFunction = unsafe extern "C" fn() -> *mut AggCtx;
pub type StepFunction = unsafe extern "C" fn(ctx: *mut AggCtx, argc: i32, argv: *const Value);
pub type FinalizeFunction = unsafe extern "C" fn(ctx: *mut AggCtx) -> Value;

#[repr(C)]
pub struct AggCtx {
    pub state: *mut c_void,
}

pub trait AggFunc {
    type State: Default;
    type Error: Display;
    const NAME: &'static str;
    const ARGS: i32;

    fn step(state: &mut Self::State, args: &[Value]);
    fn finalize(state: Self::State) -> Result<Value, Self::Error>;
}
