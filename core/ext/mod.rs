#[cfg(feature = "fs")]
mod dynamic;
mod vtab_xconnect;
use crate::index_method::backing_btree::BackingBtreeIndexMethod;
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
use crate::index_method::fts::{FtsIndexMethod, FTS_INDEX_METHOD_NAME};
use crate::index_method::toy_vector_sparse_ivf::VectorSparseInvertedIndexMethod;
use crate::index_method::{
    BACKING_BTREE_INDEX_METHOD_NAME, TOY_VECTOR_SPARSE_IVF_INDEX_METHOD_NAME,
};
use crate::schema::{Schema, Table};
use crate::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Mutex;
#[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
use crate::UringIO;
#[cfg(all(target_os = "windows", feature = "experimental_win_iocp", not(miri)))]
use crate::WindowsIOCP;

use crate::{function::ExternalFunc, Connection, Database};
use crate::{vtab::VirtualTable, SymbolTable};
#[cfg(feature = "fs")]
use crate::{LimboError, IO};
#[cfg(feature = "fs")]
pub use dynamic::{add_builtin_vfs_extensions, add_vfs_module, list_vfs_modules, VfsMod};
use std::{
    ffi::{c_char, c_void, CStr, CString},
    sync::Arc,
};
use turso_ext::{
    ContextDestructor, ExtensionApi, InitAggFunction, ResultCode, ScalarFunction, VTabKind,
    VTabModuleImpl, ValueDestructor,
};
pub use turso_ext::{FinalizeFunction, StepFunction, Value as ExtValue, ValueType as ExtValueType};
pub use vtab_xconnect::{execute, prepare_stmt};

/// The context passed to extensions to register with Core
/// along with the function pointers
#[repr(C)]
pub struct ExtensionCtx {
    syms: *mut SymbolTable,
    schema: *mut c_void,
    /// We must bump the prepare context generation so prepared statements
    /// know they need to be reprepared after extension registration.
    prepare_context_generation: *const AtomicU64,
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
        if !ext_ctx.prepare_context_generation.is_null() {
            (*ext_ctx.prepare_context_generation).fetch_add(1, Ordering::Release);
        }

        if kind == VTabKind::TableValuedFunction {
            if let Ok(vtab) = VirtualTable::function(&name_str, syms) {
                let table = Arc::new(Table::Virtual(vtab));
                let mutex = &*(ext_ctx.schema as *mut Mutex<Arc<Schema>>);
                let mut guard = mutex.lock();
                let schema = Arc::make_mut(&mut *guard);
                schema.tables.insert(name_str, table);
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

type BoxedScalarFn = Box<dyn Fn(&[crate::Value]) -> crate::Result<crate::Value> + Send + Sync>;

struct UserScalarFunction {
    call: BoxedScalarFn,
}

unsafe extern "C" fn dispatch_user_scalar(
    context: usize,
    argc: i32,
    argv: *const ExtValue,
    _context_destructor: Option<ContextDestructor>,
    _value_destructor: Option<ValueDestructor>,
) -> ExtValue {
    let function = unsafe { &*(context as *const UserScalarFunction) };
    let mut args = Vec::with_capacity(argc.max(0) as usize);
    if argc > 0 && !argv.is_null() {
        for raw in unsafe { std::slice::from_raw_parts(argv, argc as usize) } {
            match crate::Value::from_ffi_ref(raw) {
                Ok(value) => args.push(value),
                Err(err) => return ExtValue::error_with_message(err.to_string()),
            }
        }
    }
    match (function.call)(&args) {
        Ok(value) => value.to_ffi(),
        Err(err) => ExtValue::error_with_message(err.to_string()),
    }
}

unsafe extern "C" fn drop_user_scalar(context: usize) {
    drop(unsafe { Box::from_raw(context as *mut UserScalarFunction) });
}

pub(crate) unsafe fn register_scalar_function(
    ctx: *mut c_void,
    name: *const c_char,
    func: ScalarFunction,
) -> ResultCode {
    unsafe { register_scalar_function_with_options(ctx, name, -1, false, 0, func, None, None) }
}

pub(crate) unsafe extern "C" fn register_scalar_function_with_options(
    ctx: *mut c_void,
    name: *const c_char,
    argc: i32,
    deterministic: bool,
    context: usize,
    callback: ScalarFunction,
    context_destructor: Option<ContextDestructor>,
    value_destructor: Option<ValueDestructor>,
) -> ResultCode {
    if ctx.is_null() || name.is_null() || argc < -1 {
        return ResultCode::InvalidArgs;
    }
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => crate::util::normalize_ident(s),
        Err(_) => return ResultCode::InvalidArgs,
    };
    let func = Arc::new(ExternalFunc::new_scalar(
        name_str.clone(),
        argc,
        deterministic,
        context,
        callback,
        context_destructor,
        value_destructor,
    ));
    let ext_ctx = unsafe { &mut *(ctx as *mut ExtensionCtx) };
    unsafe {
        let overloads = (*ext_ctx.syms).functions.entry(name_str).or_default();
        overloads.retain(|existing| existing.func.argc() != argc);
        overloads.push(func);
        if !ext_ctx.prepare_context_generation.is_null() {
            (*ext_ctx.prepare_context_generation).fetch_add(1, Ordering::Release);
        }
    }
    ResultCode::OK
}

pub(crate) unsafe extern "C" fn unregister_function(
    ctx: *mut c_void,
    name: *const c_char,
) -> ResultCode {
    if ctx.is_null() || name.is_null() {
        return ResultCode::InvalidArgs;
    }
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => crate::util::normalize_ident(s),
        Err(_) => return ResultCode::InvalidArgs,
    };
    let ext_ctx = unsafe { &mut *(ctx as *mut ExtensionCtx) };
    unsafe {
        if (*ext_ctx.syms).functions.remove(&name_str).is_none() {
            return ResultCode::NotFound;
        }
        if !ext_ctx.prepare_context_generation.is_null() {
            (*ext_ctx.prepare_context_generation).fetch_add(1, Ordering::Release);
        }
    }
    ResultCode::OK
}

pub(crate) unsafe extern "C" fn register_aggregate_function(
    ctx: *mut c_void,
    name: *const c_char,
    args: i32,
    context: usize,
    init_func: InitAggFunction,
    step_func: StepFunction,
    finalize_func: FinalizeFunction,
    context_destructor: Option<ContextDestructor>,
    aggregate_destructor: Option<ContextDestructor>,
    value_destructor: Option<ValueDestructor>,
) -> ResultCode {
    if ctx.is_null() || name.is_null() || args < -1 {
        return ResultCode::InvalidArgs;
    }
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => crate::util::normalize_ident(s),
        Err(_) => return ResultCode::InvalidArgs,
    };
    let func = Arc::new(ExternalFunc::new_aggregate(
        name_str.clone(),
        args,
        context,
        (init_func, step_func, finalize_func),
        context_destructor,
        aggregate_destructor,
        value_destructor,
    ));
    let ext_ctx = unsafe { &mut *(ctx as *mut ExtensionCtx) };
    unsafe {
        let overloads = (*ext_ctx.syms).functions.entry(name_str).or_default();
        overloads.retain(|existing| existing.func.argc() != args);
        overloads.push(func);
        if !ext_ctx.prepare_context_generation.is_null() {
            (*ext_ctx.prepare_context_generation).fetch_add(1, Ordering::Release);
        }
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
            #[cfg(feature = "io_memory_yield")]
            "memory_yield" => Arc::new(crate::MemoryYieldIO::new()),
            "syscall" => Arc::new(SyscallIO::new()?),
            #[cfg(all(target_os = "linux", feature = "io_uring", not(miri)))]
            "io_uring" => Arc::new(UringIO::new()?),
            #[cfg(all(target_os = "windows", feature = "experimental_win_iocp", not(miri)))]
            "experimental_win_iocp" => Arc::new(WindowsIOCP::new()?),
            other => match get_vfs_modules().iter().find(|v| v.0 == vfs) {
                Some((_, vfs)) => vfs.clone(),
                None => {
                    return Err(LimboError::InvalidArgument(format!("no such VFS: {other}")));
                }
            },
        };
        let db = Self::open_file(io.clone(), path)?;
        Ok((io, db))
    }

    /// Register any built-in extensions that can be stored on the Database so we do not have
    /// to register these once-per-connection, and the connection can just extend its symbol table
    pub fn register_global_builtin_extensions(&self) -> Result<(), String> {
        {
            let mut syms = self.builtin_syms.write();
            syms.index_methods.insert(
                TOY_VECTOR_SPARSE_IVF_INDEX_METHOD_NAME.to_string(),
                Arc::new(VectorSparseInvertedIndexMethod),
            );
            syms.index_methods.insert(
                BACKING_BTREE_INDEX_METHOD_NAME.to_string(),
                Arc::new(BackingBtreeIndexMethod),
            );
            #[cfg(all(feature = "fts", not(target_family = "wasm")))]
            syms.index_methods
                .insert(FTS_INDEX_METHOD_NAME.to_string(), Arc::new(FtsIndexMethod));
        }
        let syms = self.builtin_syms.data_ptr();
        // Pass the mutex pointer and the appropriate handler
        let schema_mutex_ptr =
            &*self.schema as *const Mutex<Arc<Schema>> as *mut Mutex<Arc<Schema>>;
        let ctx = Box::into_raw(Box::new(ExtensionCtx {
            syms,
            schema: schema_mutex_ptr as *mut c_void,
            prepare_context_generation: std::ptr::null(),
        }));
        #[allow(unused)]
        let mut ext_api = ExtensionApi {
            ctx: ctx as *mut c_void,
            register_scalar_function: register_scalar_function_with_options,
            register_aggregate_function,
            unregister_function,
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
        #[cfg(feature = "time")]
        crate::time::register_extension(&mut ext_api);
        #[cfg(feature = "percentile")]
        crate::percentile::register_extension(&mut ext_api);
        crate::regexp::register_extension(&mut ext_api);
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
            &*self.db.schema as *const Mutex<Arc<Schema>> as *mut Mutex<Arc<Schema>>;
        let ctx = ExtensionCtx {
            syms: self.syms.data_ptr(),
            schema: schema_mutex_ptr as *mut c_void,
            prepare_context_generation: &self.prepare_context_generation as *const _,
        };
        let ctx = Box::into_raw(Box::new(ctx)) as *mut c_void;
        ExtensionApi {
            ctx,
            register_scalar_function: register_scalar_function_with_options,
            register_aggregate_function,
            unregister_function,
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

    pub fn register_scalar_function<F>(
        &self,
        name: &str,
        arity: i32,
        deterministic: bool,
        function: F,
    ) -> crate::Result<()>
    where
        F: Fn(&[crate::Value]) -> crate::Result<crate::Value> + Send + Sync + 'static,
    {
        let name_c = CString::new(name).map_err(|_| {
            crate::LimboError::InvalidArgument(format!(
                "scalar function name must not contain a NUL byte: {name:?}"
            ))
        })?;
        let context = Box::into_raw(Box::new(UserScalarFunction {
            call: Box::new(function),
        })) as usize;
        let api = unsafe { self._build_turso_ext() };
        let result = unsafe {
            (api.register_scalar_function)(
                api.ctx,
                name_c.as_ptr(),
                arity,
                deterministic,
                context,
                dispatch_user_scalar,
                Some(drop_user_scalar),
                None,
            )
        };
        unsafe { self._free_extension_ctx(api) };
        if result == ResultCode::OK {
            Ok(())
        } else {
            unsafe { drop_user_scalar(context) };
            Err(crate::LimboError::InvalidArgument(format!(
                "failed to register scalar function {name:?}: {result}"
            )))
        }
    }

    pub fn unregister_scalar_function(&self, name: &str) -> crate::Result<()> {
        let name_c = CString::new(name).map_err(|_| {
            crate::LimboError::InvalidArgument(format!(
                "scalar function name must not contain a NUL byte: {name:?}"
            ))
        })?;
        let api = unsafe { self._build_turso_ext() };
        let result = unsafe { (api.unregister_function)(api.ctx, name_c.as_ptr()) };
        unsafe { self._free_extension_ctx(api) };
        match result {
            ResultCode::OK => Ok(()),
            ResultCode::NotFound => Err(crate::LimboError::InvalidArgument(format!(
                "no such function: {name}"
            ))),
            other => Err(crate::LimboError::InvalidArgument(format!(
                "failed to unregister function {name:?}: {other}"
            ))),
        }
    }
}
