use crate::{Connection, LimboError};
use libloading::{Library, Symbol};
use limbo_ext::{ExtensionApi, ExtensionApiRef, ExtensionEntryPoint, VfsImpl};
use std::{
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

#[derive(Clone, Debug)]
pub struct VfsMod {
    pub ctx: *const VfsImpl,
}

unsafe impl Send for VfsMod {}
unsafe impl Sync for VfsMod {}
type Vfs = (String, Arc<VfsMod>);

static VFS_MODULES: OnceLock<Mutex<Vec<Vfs>>> = OnceLock::new();

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
            extensions.lock().unwrap().push((Arc::new(lib), api_ref));
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

pub(crate) fn add_vfs_module(name: String, vfs: Arc<VfsMod>) {
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

pub(crate) fn get_vfs_modules() -> Vec<Vfs> {
    VFS_MODULES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .clone()
}
