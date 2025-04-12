#[cfg(feature = "fs")]
mod dynamic;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use crate::UringIO;
use crate::{function::ExternalFunc, Connection, Database, LimboError, IO};
#[cfg(feature = "fs")]
pub use dynamic::{add_builtin_vfs_extensions, add_vfs_module, list_vfs_modules, VfsMod};
use limbo_ext::{
    ExtensionApi, InitAggFunction, ResultCode, ScalarFunction, VTabKind, VTabModuleImpl,
};
pub use limbo_ext::{FinalizeFunction, StepFunction, Value as ExtValue, ValueType as ExtValueType};
use std::{
    ffi::{c_char, c_void, CStr, CString},
    rc::Rc,
    sync::Arc,
};
type ExternAggFunc = (InitAggFunction, StepFunction, FinalizeFunction);

#[derive(Clone)]
pub struct VTabImpl {
    pub module_kind: VTabKind,
    pub implementation: Rc<VTabModuleImpl>,
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
    let conn = unsafe { &*(ctx as *const Connection) };
    conn.register_scalar_function_impl(&name_str, func)
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
    let conn = unsafe { &*(ctx as *const Connection) };
    conn.register_aggregate_function_impl(&name_str, args, (init_func, step_func, finalize_func))
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
    let c_str = unsafe { CString::from_raw(name as *mut _) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::Error,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let conn = unsafe { &mut *(ctx as *mut Connection) };

    conn.register_vtab_module_impl(&name_str, module, kind)
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
                    return Err(LimboError::InvalidArgument(format!(
                        "no such VFS: {}",
                        other
                    )));
                }
            },
        };
        let db = Self::open_file(io.clone(), path, false)?;
        Ok((io, db))
    }
}

impl Connection {
    fn register_scalar_function_impl(&self, name: &str, func: ScalarFunction) -> ResultCode {
        self.syms.borrow_mut().functions.insert(
            name.to_string(),
            Rc::new(ExternalFunc::new_scalar(name.to_string(), func)),
        );
        ResultCode::OK
    }

    fn register_aggregate_function_impl(
        &self,
        name: &str,
        args: i32,
        func: ExternAggFunc,
    ) -> ResultCode {
        self.syms.borrow_mut().functions.insert(
            name.to_string(),
            Rc::new(ExternalFunc::new_aggregate(name.to_string(), args, func)),
        );
        ResultCode::OK
    }

    fn register_vtab_module_impl(
        &mut self,
        name: &str,
        module: VTabModuleImpl,
        kind: VTabKind,
    ) -> ResultCode {
        let module = Rc::new(module);
        let vmodule = VTabImpl {
            module_kind: kind,
            implementation: module,
        };
        self.syms
            .borrow_mut()
            .vtab_modules
            .insert(name.to_string(), vmodule.into());
        ResultCode::OK
    }

    pub fn build_limbo_ext(&self) -> ExtensionApi {
        ExtensionApi {
            ctx: self as *const _ as *mut c_void,
            register_scalar_function,
            register_aggregate_function,
            register_vtab_module,
            #[cfg(feature = "fs")]
            vfs_interface: limbo_ext::VfsInterface {
                register_vfs: dynamic::register_vfs,
                builtin_vfs: std::ptr::null_mut(),
                builtin_vfs_count: 0,
            },
        }
    }

    pub fn register_builtins(&self) -> Result<(), String> {
        #[allow(unused_variables)]
        let mut ext_api = self.build_limbo_ext();
        #[cfg(feature = "uuid")]
        if unsafe { !limbo_uuid::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register uuid extension".to_string());
        }
        #[cfg(feature = "percentile")]
        if unsafe { !limbo_percentile::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register percentile extension".to_string());
        }
        #[cfg(feature = "regexp")]
        if unsafe { !limbo_regexp::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register regexp extension".to_string());
        }
        #[cfg(feature = "time")]
        if unsafe { !limbo_time::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register time extension".to_string());
        }
        #[cfg(feature = "crypto")]
        if unsafe { !limbo_crypto::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register crypto extension".to_string());
        }
        #[cfg(feature = "series")]
        if unsafe { !limbo_series::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register series extension".to_string());
        }
        #[cfg(feature = "ipaddr")]
        if unsafe { !limbo_ipaddr::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register ipaddr extension".to_string());
        }
        #[cfg(feature = "completion")]
        if unsafe { !limbo_completion::register_extension_static(&mut ext_api).is_ok() } {
            return Err("Failed to register completion extension".to_string());
        }
        #[cfg(feature = "fs")]
        {
            let vfslist = add_builtin_vfs_extensions(Some(ext_api)).map_err(|e| e.to_string())?;
            for (name, vfs) in vfslist {
                add_vfs_module(name, vfs);
            }
        }
        Ok(())
    }
}
