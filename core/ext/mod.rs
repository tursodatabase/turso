#[cfg(feature = "fs")]
mod dynamic;
mod vtab_xconnect;
use crate::schema::{Schema, Table};
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use crate::UringIO;
use crate::{function::ExternalFunc, Connection, Database};
use crate::{vtab::VirtualTable, SymbolTable};
#[cfg(feature = "fs")]
use crate::{LimboError, IO};
#[cfg(feature = "fs")]
pub use dynamic::{add_builtin_vfs_extensions, add_vfs_module, list_vfs_modules, VfsMod};
use std::{
    ffi::{c_char, c_void, CStr, CString},
    sync::{Arc, Mutex},
};
use turso_ext::{
    ExtensionApi, InitAggFunction, ResultCode, ScalarFunction, VTabKind, VTabModuleImpl,
};
pub use turso_ext::{FinalizeFunction, StepFunction, Value as ExtValue, ValueType as ExtValueType};
pub use vtab_xconnect::{execute, prepare_stmt};

/// The context passed to extensions to register with Core
/// along with the function pointers
#[repr(C)]
pub struct ExtensionCtx {
    syms: *mut SymbolTable,
    schema: *mut c_void,
}

pub(crate) unsafe extern "C" fn register_vtab_module(
    ctx: *mut c_void,
    name: *const c_char,
    module: VTabModuleImpl,
    kind: VTabKind,
) -> ResultCode {
    if name.is_null() || ctx.is_null() {
        return ResultCode::Error;
    }

    let c_str = unsafe { CString::from_raw(name as *mut c_char) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::Error,
    };

    let ext_ctx = unsafe { &mut *(ctx as *mut ExtensionCtx) };
    let module = Arc::new(module);
    let vmodule = VTabImpl {
        module_kind: kind,
        implementation: module,
    };

    unsafe {
        let syms = &mut *ext_ctx.syms;
        syms.vtab_modules.insert(name_str.clone(), vmodule.into());

        if kind == VTabKind::TableValuedFunction {
            if let Ok(vtab) = VirtualTable::function(&name_str, syms) {
                // Use the schema handler to insert the table
                let table = Arc::new(Table::Virtual(vtab));
                let mutex = &*(ext_ctx.schema as *mut Mutex<Arc<Schema>>);
                let Ok(guard) = mutex.lock() else {
                    return ResultCode::Error;
                };
                let schema_ptr = Arc::as_ptr(&*guard) as *mut Schema;
                (*schema_ptr).tables.insert(name_str, table);
            } else {
                return ResultCode::Error;
            }
        }
    }
    ResultCode::OK
}

#[derive(Clone)]
pub struct VTabImpl {
    pub module_kind: VTabKind,
    pub implementation: Arc<VTabModuleImpl>,
}

pub(crate) unsafe extern "C" fn register_scalar_function(
    ctx: *mut c_void,
    name: *const c_char,
    func: ScalarFunction,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::InvalidArgs,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let ext_ctx = unsafe { &mut *(ctx as *mut ExtensionCtx) };
    unsafe {
        (*ext_ctx.syms).functions.insert(
            name_str.clone(),
            Arc::new(ExternalFunc::new_scalar(name_str, func)),
        );
    }
    ResultCode::OK
}

pub(crate) unsafe extern "C" fn register_aggregate_function(
    ctx: *mut c_void,
    name: *const c_char,
    args: i32,
    init_func: InitAggFunction,
    step_func: StepFunction,
    finalize_func: FinalizeFunction,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::InvalidArgs,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let ext_ctx = unsafe { &mut *(ctx as *mut ExtensionCtx) };
    unsafe {
        (*ext_ctx.syms).functions.insert(
            name_str.clone(),
            Arc::new(ExternalFunc::new_aggregate(
                name_str,
                args,
                (init_func, step_func, finalize_func),
            )),
        );
    }
    ResultCode::OK
}

impl Database {
    #[cfg(feature = "fs")]
    #[allow(clippy::arc_with_non_send_sync, dead_code)]
    pub fn open_with_vfs(
        &self,
        path: &str,
        vfs: &str,
    ) -> crate::Result<(Arc<dyn IO>, Arc<Database>)> {
        use crate::{MemoryIO, SyscallIO};
        use dynamic::get_vfs_modules;

        let io: Arc<dyn IO> = match vfs {
            "memory" => Arc::new(MemoryIO::new()),
            "syscall" => Arc::new(SyscallIO::new()?),
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            "io_uring" => Arc::new(UringIO::new()?),
            other => match get_vfs_modules().iter().find(|v| v.0 == vfs) {
                Some((_, vfs)) => vfs.clone(),
                None => {
                    return Err(LimboError::InvalidArgument(format!("no such VFS: {other}")));
                }
            },
        };
        let db = Self::open_file(io.clone(), path, false, false)?;
        Ok((io, db))
    }

    /// Register any built-in extensions that can be stored on the Database so we do not have
    /// to register these once-per-connection, and the connection can just extend its symbol table
    pub fn register_global_builtin_extensions(&self) -> Result<(), String> {
        let syms = self.builtin_syms.data_ptr();
        // Pass the mutex pointer and the appropriate handler
        let schema_mutex_ptr = &self.schema as *const Mutex<Arc<Schema>> as *mut Mutex<Arc<Schema>>;
        let ctx = Box::into_raw(Box::new(ExtensionCtx {
            syms,
            schema: schema_mutex_ptr as *mut c_void,
        }));
        #[allow(unused)]
        let mut ext_api = ExtensionApi {
            ctx: ctx as *mut c_void,
            register_scalar_function,
            register_aggregate_function,
            register_vtab_module,
            #[cfg(feature = "fs")]
            vfs_interface: turso_ext::VfsInterface {
                register_vfs: dynamic::register_vfs,
                builtin_vfs: std::ptr::null_mut(),
                builtin_vfs_count: 0,
            },
        };

        #[cfg(feature = "uuid")]
        crate::uuid::register_extension(&mut ext_api);
        #[cfg(feature = "series")]
        crate::series::register_extension(&mut ext_api);
        #[cfg(feature = "fs")]
        {
            let vfslist = add_builtin_vfs_extensions(Some(ext_api)).map_err(|e| e.to_string())?;
            for (name, vfs) in vfslist {
                add_vfs_module(name, vfs);
            }
        }
        let _ = unsafe { Box::from_raw(ctx) };
        Ok(())
    }
}

impl Connection {
    /// Build the connection's extension api context for manually registering an extension.
    /// you probably want to use `Connection::load_extension(path)`.
    ///
    /// # Safety
    /// Only to be used when registering a staticly linked extension manually.
    /// You should only ever call this method on your applications startup,
    /// The caller is responsible for calling `_free_extension_ctx` after registering the
    /// extension.
    ///
    /// usage:
    /// ```ignore
    /// let ext_api = conn._build_turso_ext();
    /// unsafe {
    ///     my_extension::register_extension(&mut ext_api);
    ///     conn._free_extension_ctx(ext_api);
    /// }
    ///```
    pub unsafe fn _build_turso_ext(&self) -> ExtensionApi {
        let schema_mutex_ptr =
            &self.db.schema as *const Mutex<Arc<Schema>> as *mut Mutex<Arc<Schema>>;
        let ctx = ExtensionCtx {
            syms: self.syms.data_ptr(),
            schema: schema_mutex_ptr as *mut c_void,
        };
        let ctx = Box::into_raw(Box::new(ctx)) as *mut c_void;
        ExtensionApi {
            ctx,
            register_scalar_function,
            register_aggregate_function,
            register_vtab_module,
            #[cfg(feature = "fs")]
            vfs_interface: turso_ext::VfsInterface {
                register_vfs: dynamic::register_vfs,
                builtin_vfs: std::ptr::null_mut(),
                builtin_vfs_count: 0,
            },
        }
    }

    /// Free the connection's extension libary context after registering an extension manually.
    /// # Safety
    /// Only to be used if you have previously called Connection::build_turso_ext
    pub unsafe fn _free_extension_ctx(&self, api: ExtensionApi) {
        if api.ctx.is_null() {
            return;
        }
        let _ = unsafe { Box::from_raw(api.ctx as *mut ExtensionCtx) };
    }
}
