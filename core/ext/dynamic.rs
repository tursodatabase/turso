use crate::{
    ext::{register_aggregate_function, register_scalar_function, register_vtab_module},
    Connection, LimboError,
};
use libloading::{Library, Symbol};
use limbo_ext::{ExtensionApi, ExtensionApiRef, ExtensionEntryPoint, ResultCode, VfsImpl};
use std::{
    ffi::{c_char, CString},
    rc::Rc,
    sync::{Arc, Mutex, OnceLock},
};

type ExtensionStore = Vec<(Arc<Library>, ExtensionApiRef)>;
static EXTENSIONS: OnceLock<Arc<Mutex<ExtensionStore>>> = OnceLock::new();
pub fn get_extension_libraries() -> Arc<Mutex<ExtensionStore>> {
    EXTENSIONS
        .get_or_init(|| Arc::new(Mutex::new(Vec::new())))
        .clone()
}

type Vfs = (String, Arc<VfsMod>);
static VFS_MODULES: OnceLock<Mutex<Vec<Vfs>>> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct VfsMod {
    pub ctx: *const VfsImpl,
}

unsafe impl Send for VfsMod {}
unsafe impl Sync for VfsMod {}

impl Connection {
    pub fn load_extension<P: AsRef<std::ffi::OsStr>>(
        self: &Rc<Connection>,
        path: P,
    ) -> crate::Result<()> {
        use limbo_ext::ExtensionApiRef;

        let api = Box::new(self.build_limbo_ext());
        let lib =
            unsafe { Library::new(path).map_err(|e| LimboError::ExtensionError(e.to_string()))? };
        let entry: Symbol<ExtensionEntryPoint> = unsafe {
            lib.get(b"register_extension")
                .map_err(|e| LimboError::ExtensionError(e.to_string()))?
        };
        let api_ptr: *const ExtensionApi = Box::into_raw(api);
        let api_ref = ExtensionApiRef { api: api_ptr };
        let result_code = unsafe { entry(api_ptr) };
        if result_code.is_ok() {
            let extensions = get_extension_libraries();
            extensions
                .lock()
                .map_err(|_| {
                    LimboError::ExtensionError("Error locking extension libraries".to_string())
                })?
                .push((Arc::new(lib), api_ref));
            {
                self.parse_schema_rows()?;
            }
            Ok(())
        } else {
            if !api_ptr.is_null() {
                let _ = unsafe { Box::from_raw(api_ptr.cast_mut()) };
            }
            Err(LimboError::ExtensionError(
                "Extension registration failed".to_string(),
            ))
        }
    }
}

#[allow(clippy::arc_with_non_send_sync)]
pub(crate) unsafe extern "C" fn register_vfs(
    name: *const c_char,
    vfs: *const VfsImpl,
) -> ResultCode {
    if name.is_null() || vfs.is_null() {
        return ResultCode::Error;
    }
    let c_str = unsafe { CString::from_raw(name as *mut _) };
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
    use limbo_ext::VfsInterface;

    let mut vfslist: Vec<*const VfsImpl> = Vec::new();
    let mut api = match api {
        None => ExtensionApi {
            ctx: std::ptr::null_mut(),
            register_scalar_function,
            register_aggregate_function,
            register_vtab_module,
            vfs_interface: VfsInterface {
                register_vfs,
                builtin_vfs: vfslist.as_mut_ptr(),
                builtin_vfs_count: 0,
            },
        },
        Some(mut api) => {
            api.vfs_interface.builtin_vfs = vfslist.as_mut_ptr();
            api
        }
    };
    register_static_vfs_modules(&mut api);
    let mut vfslist = Vec::with_capacity(api.vfs_interface.builtin_vfs_count as usize);
    let slice = unsafe {
        std::slice::from_raw_parts_mut(
            api.vfs_interface.builtin_vfs,
            api.vfs_interface.builtin_vfs_count as usize,
        )
    };
    for vfs in slice {
        if vfs.is_null() {
            continue;
        }
        let vfsimpl = unsafe { &**vfs };
        let name = unsafe {
            CString::from_raw(vfsimpl.name as *mut _)
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

#[cfg(feature = "fs")]
fn register_static_vfs_modules(_api: &mut ExtensionApi) {
    #[cfg(feature = "testvfs")]
    unsafe {
        limbo_ext_tests::register_extension_static(_api);
    }
}

pub fn add_vfs_module(name: String, vfs: Arc<VfsMod>) {
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

pub fn get_vfs_modules() -> Vec<Vfs> {
    VFS_MODULES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .clone()
}
