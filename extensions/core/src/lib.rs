mod functions;
mod types;
#[cfg(feature = "vfs")]
mod vfs_modules;
mod vtabs;
pub use functions::{
    AggCtx, AggFunc, FinalizeFunction, InitAggFunction, ScalarFunction, StepFunction,
};
use functions::{RegisterAggFn, RegisterScalarFn};
use std::os::raw::c_void;
#[cfg(feature = "vfs")]
pub use turso_macros::VfsDerive;
pub use turso_macros::{register_extension, scalar, AggregateDerive, VTabModuleDerive};
pub use types::{ResultCode, StepResult, Value, ValueType};
#[cfg(feature = "vfs")]
pub use vfs_modules::{
    BufferRef, Callback, IOCallback, RegisterVfsFn, SendPtr, VfsExtension, VfsFile, VfsFileImpl,
    VfsImpl, VfsInterface,
};
use vtabs::RegisterModuleFn;
pub use vtabs::{
    Conn, Connection, ConstraintInfo, ConstraintOp, ConstraintUsage, ExtIndexInfo, IndexInfo,
    OrderByInfo, Statement, Stmt, VTabCreateResult, VTabCursor, VTabKind, VTabModule,
    VTabModuleImpl, VTable,
};

pub type ExtResult<T> = std::result::Result<T, ResultCode>;

pub type ExtensionEntryPoint = unsafe extern "C" fn(api: *const ExtensionApi) -> ResultCode;

#[repr(C)]
pub struct ExtensionApi {
    pub ctx: *mut c_void,
    pub register_scalar_function: RegisterScalarFn,
    pub register_aggregate_function: RegisterAggFn,
    pub register_vtab_module: RegisterModuleFn,
    #[cfg(feature = "vfs")]
    pub vfs_interface: VfsInterface,
}

unsafe impl Send for ExtensionApi {}
unsafe impl Send for ExtensionApiRef {}

#[repr(C)]
pub struct ExtensionApiRef {
    pub api: *const ExtensionApi,
}
