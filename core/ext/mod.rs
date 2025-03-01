#[cfg(not(target_family = "wasm"))]
mod dynamic;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use crate::UringIO;
use crate::IO;
use crate::{function::ExternalFunc, Connection, Database, LimboError};
pub mod foreign_types;
use limbo_ext::{
    CustomTypeImpl, ExtensionApi, InitAggFunction, ResultCode, ScalarFunction, VTabKind,
    VTabModuleImpl, VfsImpl,
};
pub use limbo_ext::{FinalizeFunction, StepFunction, Value as ExtValue, ValueType as ExtValueType};
use std::{
    ffi::{c_char, c_void, CStr, CString},
    rc::Rc,
    sync::{Arc, Mutex, OnceLock},
};
type ExternAggFunc = (InitAggFunction, StepFunction, FinalizeFunction);
type Vfs = (String, Arc<VfsMod>);

static VFS_MODULES: OnceLock<Mutex<Vec<Vfs>>> = OnceLock::new();

#[derive(Clone)]
pub struct VTabImpl {
    pub module_kind: VTabKind,
    pub implementation: Rc<VTabModuleImpl>,
}

#[derive(Clone, Debug)]
pub struct VfsMod {
    pub ctx: *const VfsImpl,
}

unsafe impl Send for VfsMod {}
unsafe impl Sync for VfsMod {}

unsafe extern "C" fn register_scalar_function(
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

unsafe extern "C" fn register_aggregate_function(
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

unsafe extern "C" fn register_module(
    ctx: *mut c_void,
    name: *const c_char,
    module: VTabModuleImpl,
    kind: VTabKind,
) -> ResultCode {
    if name.is_null() || ctx.is_null() {
        return ResultCode::Error;
    }
    let c_str = unsafe { CString::from_raw(name as *mut i8) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::Error,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let conn = unsafe { &mut *(ctx as *mut Connection) };

    conn.register_module_impl(&name_str, module, kind)
}

unsafe extern "C" fn register_custom_type(
    ctx: *mut c_void,
    module: *const CustomTypeImpl,
) -> ResultCode {
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let conn = unsafe { &mut *(ctx as *mut Connection) };
    conn.register_custom_type_impl(module)
}

#[allow(clippy::arc_with_non_send_sync)]
unsafe extern "C" fn register_vfs(name: *const c_char, vfs: *const VfsImpl) -> ResultCode {
    if name.is_null() || vfs.is_null() {
        return ResultCode::Error;
    }
    let c_str = unsafe { CString::from_raw(name as *mut i8) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::Error,
    };
    add_vfs_module(name_str, Arc::new(VfsMod { ctx: vfs }));
    ResultCode::OK
}

/// Get pointers to all the vfs extensions that need to be built in at compile time.
/// any other types that are defined in the same extension will not be registered
/// until the database file is opened and `register_builtins` is called.
#[cfg(feature = "fs")]
#[allow(clippy::arc_with_non_send_sync)]
pub fn add_builtin_vfs_extensions(
    api: Option<ExtensionApi>,
) -> crate::Result<Vec<(String, Arc<VfsMod>)>> {
    let mut vfslist: Vec<*const VfsImpl> = Vec::new();
    let mut api = match api {
        None => ExtensionApi {
            ctx: std::ptr::null_mut(),
            register_scalar_function,
            register_aggregate_function,
            register_vfs,
            register_module,
            builtin_vfs: vfslist.as_mut_ptr(),
            builtin_vfs_count: 0,
        },
        Some(mut api) => {
            api.builtin_vfs = vfslist.as_mut_ptr();
            api
        }
    };
    register_static_vfs_modules(&mut api);
    let mut vfslist = Vec::with_capacity(api.builtin_vfs_count as usize);
    let slice =
        unsafe { std::slice::from_raw_parts_mut(api.builtin_vfs, api.builtin_vfs_count as usize) };
    for vfs in slice {
        if vfs.is_null() {
            continue;
        }
        let vfsimpl = unsafe { &**vfs };
        let name = unsafe {
            CString::from_raw(vfsimpl.name as *mut i8)
                .to_str()
                .map_err(|_| {
                    LimboError::ExtensionError("unable to register vfs extension".to_string())
                })?
                .to_string()
        };
        vfslist.push((
            name,
            Arc::new(VfsMod {
                ctx: vfsimpl as *const _,
            }),
        ));
    }
    Ok(vfslist)
}

fn register_static_vfs_modules(_api: &mut ExtensionApi) {
    #[cfg(feature = "testvfs")]
    unsafe {
        limbo_ext_tests::register_extension_static(_api);
    }
}

impl Database {
    #[cfg(feature = "fs")]
    #[allow(clippy::arc_with_non_send_sync, dead_code)]
    pub fn open_with_vfs(
        &self,
        path: &str,
        vfs: &str,
    ) -> crate::Result<(Arc<dyn IO>, Arc<Database>)> {
        use crate::{MemoryIO, PlatformIO};

        let io: Arc<dyn IO> = match vfs {
            "memory" => Arc::new(MemoryIO::new()),
            "syscall" => Arc::new(PlatformIO::new()?),
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

    fn register_module_impl(
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

    fn register_custom_type_impl(&mut self, type_impl: *const CustomTypeImpl) -> ResultCode {
        let name = unsafe { CStr::from_ptr((*type_impl).name) }
            .to_str()
            .unwrap_or_default();
        {
            if self.syms.borrow_mut().type_registry.get(name).is_some() {
                // type already registered
                return ResultCode::OK;
            }
        }
        let ot = unsafe { (*type_impl).type_of } as ExtValueType;
        {
            self.syms
                .borrow_mut()
                .type_registry
                .register(name, type_impl, ot.into());
        }
        ResultCode::OK
    }

    pub fn build_limbo_ext(&self) -> ExtensionApi {
        ExtensionApi {
            ctx: self as *const _ as *mut c_void,
            register_scalar_function,
            register_aggregate_function,
            register_module,
<<<<<<< HEAD
            register_vfs,
            builtin_vfs: std::ptr::null_mut(),
            builtin_vfs_count: 0,
||||||| parent of 6b73c198 (Add custom type registration to extension api)
=======
            register_custom_type,
>>>>>>> 6b73c198 (Add custom type registration to extension api)
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

fn add_vfs_module(name: String, vfs: Arc<VfsMod>) {
    let mut modules = VFS_MODULES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap();
    if !modules.iter().any(|v| v.0 == name) {
        modules.push((name, vfs));
    }
}

pub fn list_vfs_modules() -> Vec<String> {
    VFS_MODULES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .iter()
        .map(|v| v.0.clone())
        .collect()
}

fn get_vfs_modules() -> Vec<Vfs> {
    VFS_MODULES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .clone()
}
