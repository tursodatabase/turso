mod types;
mod vfs_modules;
#[cfg(not(target_family = "wasm"))]
pub use limbo_macros::VfsDerive;
pub use limbo_macros::{
    register_extension, scalar, AggregateDerive, CustomTypeDerive, VTabModuleDerive,
};
use std::{
    ffi::{c_char, c_void},
    fmt::Display,
};
pub use types::{ResultCode, Value, ValueType};
pub use vfs_modules::{RegisterVfsFn, VfsFileImpl, VfsImpl};
#[cfg(not(target_family = "wasm"))]
pub use vfs_modules::{VfsExtension, VfsFile};

pub type ExtResult<T> = std::result::Result<T, ResultCode>;

#[repr(C)]
pub struct ExtensionApi {
    pub ctx: *mut c_void,
    pub register_scalar_function: RegisterScalarFn,
    pub register_aggregate_function: RegisterAggFn,
    pub register_module: RegisterModuleFn,
    pub register_vfs: RegisterVfsFn,
    pub register_custom_type: RegisterCustomTypeFn,
    pub builtin_vfs: *mut *const VfsImpl,
    pub builtin_vfs_count: i32,
}
unsafe impl Send for ExtensionApi {}
unsafe impl Send for ExtensionApiRef {}

#[repr(C)]
pub struct ExtensionApiRef {
    pub api: *const ExtensionApi,
}

impl ExtensionApi {
    /// Since we want the option to build in extensions at compile time as well,
    /// we add a slice of VfsImpls to the extension API, and this is called with any
    /// libraries that we load staticly that will add their VFS implementations to the list.
    pub fn add_builtin_vfs(&mut self, vfs: *const VfsImpl) -> ResultCode {
        if vfs.is_null() || self.builtin_vfs.is_null() {
            return ResultCode::Error;
        }
        let mut new = unsafe {
            let slice =
                std::slice::from_raw_parts_mut(self.builtin_vfs, self.builtin_vfs_count as usize);
            Vec::from(slice)
        };
        new.push(vfs);
        self.builtin_vfs = Box::into_raw(new.into_boxed_slice()) as *mut *const VfsImpl;
        self.builtin_vfs_count += 1;
        ResultCode::OK
    }
}

pub type ExtensionEntryPoint = unsafe extern "C" fn(api: *const ExtensionApi) -> ResultCode;

pub type ScalarFunction = unsafe extern "C" fn(argc: i32, *const Value) -> Value;

pub type RegisterScalarFn =
    unsafe extern "C" fn(ctx: *mut c_void, name: *const c_char, func: ScalarFunction) -> ResultCode;

pub type RegisterAggFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    name: *const c_char,
    args: i32,
    init: InitAggFunction,
    step: StepFunction,
    finalize: FinalizeFunction,
) -> ResultCode;

pub type RegisterModuleFn = unsafe extern "C" fn(
    ctx: *mut c_void,
    name: *const c_char,
    module: VTabModuleImpl,
    kind: VTabKind,
) -> ResultCode;

pub type RegisterCustomTypeFn =
    unsafe extern "C" fn(ctx: *mut c_void, module: *const CustomTypeImpl) -> ResultCode;

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

#[repr(C)]
#[derive(Clone, Debug)]
pub struct VTabModuleImpl {
    pub ctx: *const c_void,
    pub name: *const c_char,
    pub create_schema: VtabFnCreateSchema,
    pub open: VtabFnOpen,
    pub filter: VtabFnFilter,
    pub column: VtabFnColumn,
    pub next: VtabFnNext,
    pub eof: VtabFnEof,
    pub update: VtabFnUpdate,
    pub rowid: VtabRowIDFn,
}

#[cfg(feature = "core_only")]
impl VTabModuleImpl {
    pub fn init_schema(&self, args: Vec<Value>) -> ExtResult<String> {
        let schema = unsafe { (self.create_schema)(args.as_ptr(), args.len() as i32) };
        if schema.is_null() {
            return Err(ResultCode::InvalidArgs);
        }
        for arg in args {
            unsafe { arg.__free_internal_type() };
        }
        let schema = unsafe { std::ffi::CString::from_raw(schema) };
        Ok(schema.to_string_lossy().to_string())
    }
}

pub type VtabFnCreateSchema = unsafe extern "C" fn(args: *const Value, argc: i32) -> *mut c_char;

pub type VtabFnOpen = unsafe extern "C" fn(*const c_void) -> *const c_void;

pub type VtabFnFilter =
    unsafe extern "C" fn(cursor: *const c_void, argc: i32, argv: *const Value) -> ResultCode;

pub type VtabFnColumn = unsafe extern "C" fn(cursor: *const c_void, idx: u32) -> Value;

pub type VtabFnNext = unsafe extern "C" fn(cursor: *const c_void) -> ResultCode;

pub type VtabFnEof = unsafe extern "C" fn(cursor: *const c_void) -> bool;

pub type VtabRowIDFn = unsafe extern "C" fn(cursor: *const c_void) -> i64;

pub type VtabFnUpdate = unsafe extern "C" fn(
    vtab: *const c_void,
    argc: i32,
    argv: *const Value,
    p_out_rowid: *mut i64,
) -> ResultCode;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum VTabKind {
    VirtualTable,
    TableValuedFunction,
}

pub trait VTabModule: 'static {
    type VCursor: VTabCursor<Error = Self::Error>;
    const VTAB_KIND: VTabKind;
    const NAME: &'static str;
    type Error: std::fmt::Display;

    fn create_schema(args: &[Value]) -> String;
    fn open(&self) -> Result<Self::VCursor, Self::Error>;
    fn filter(cursor: &mut Self::VCursor, args: &[Value]) -> ResultCode;
    fn column(cursor: &Self::VCursor, idx: u32) -> Result<Value, Self::Error>;
    fn next(cursor: &mut Self::VCursor) -> ResultCode;
    fn eof(cursor: &Self::VCursor) -> bool;
    fn update(&mut self, _rowid: i64, _args: &[Value]) -> Result<(), Self::Error> {
        Ok(())
    }
    fn insert(&mut self, _args: &[Value]) -> Result<i64, Self::Error> {
        Ok(0)
    }
    fn delete(&mut self, _rowid: i64) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub trait VTabCursor: Sized {
    type Error: std::fmt::Display;
    fn rowid(&self) -> i64;
    fn column(&self, idx: u32) -> Result<Value, Self::Error>;
    fn eof(&self) -> bool;
    fn next(&mut self) -> ResultCode;
}

#[repr(C)]
#[derive(Clone, Debug)]
pub struct CustomTypeImpl {
    pub name: *const c_char,
    pub type_of: ValueType,
    pub on_insert: CustomTypeOnInsertFn,
}

pub trait CustomType: Default + Sized {
    const NAME: &'static str;
    const TYPE: ValueType;
    fn on_insert(col_name: Option<&str>, insert_val: Option<&Value>) -> Value;
}

pub type CustomTypeOnInsertFn =
    unsafe extern "C" fn(col_name: *const c_char, insert_val: *const Value) -> Value;
