use crate::{ResultCode, Value};
use std::{
    ffi::{c_char, c_void},
    fmt::Display,
};

pub type ContextDestructor = unsafe extern "C" fn(context: usize);
pub type ValueDestructor = unsafe extern "C" fn(result: *mut Value);
pub type ScalarFunction = unsafe extern "C" fn(
    context: usize,
    argc: i32,
    argv: *const Value,
    context_destructor: Option<ContextDestructor>,
    value_destructor: Option<ValueDestructor>,
) -> Value;

pub type RegisterScalarFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    name: *const c_char,
    argc: i32,
    deterministic: bool,
    context: usize,
    func: ScalarFunction,
    context_destructor: Option<ContextDestructor>,
    value_destructor: Option<ValueDestructor>,
) -> ResultCode;

pub type UnregisterFunctionFn =
    unsafe extern "C" fn(ctx: *mut c_void, name: *const c_char) -> ResultCode;

pub type RegisterAggFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    name: *const c_char,
    args: i32,
    context: usize,
    init: InitAggFunction,
    step: StepFunction,
    finalize: FinalizeFunction,
    context_destructor: Option<ContextDestructor>,
    aggregate_destructor: Option<ContextDestructor>,
    value_destructor: Option<ValueDestructor>,
) -> ResultCode;

pub type InitAggFunction = unsafe extern "C" fn(context: usize) -> *mut AggCtx;
pub type StepFunction =
    unsafe extern "C" fn(context: usize, ctx: *mut AggCtx, argc: i32, argv: *const Value) -> Value;
pub type FinalizeFunction = unsafe extern "C" fn(context: usize, ctx: *mut AggCtx) -> Value;

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

/// A scalar function that carries opaque, per-registration state.
///
/// `State` is constructed once per registration via [`ScalarFunc::init`], shared
/// by reference across every invocation, and dropped when the function is
/// unregistered or the owning connection is dropped. Because a single registration
/// is shared across connections and may be invoked concurrently, `State` must be
/// `Send + Sync`.
///
/// Stateless functions should use the `#[scalar]` attribute macro instead.
pub trait ScalarFunc {
    type State: Send + Sync;
    const NAME: &'static str;
    const ALIAS: Option<&'static str> = None;

    fn init() -> Self::State;
    fn call(state: &Self::State, args: &[Value]) -> Value;
}
