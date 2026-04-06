//! WASM UDF runtime for native Node.js / Bun.
//!
//! Delegates to the host JS engine's `WebAssembly` API via raw napi calls.
//! All calls are synchronous and happen on the main JS thread during
//! `connectSync()` or `stepSync()`.

use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::HashMap;
use std::fmt;
use std::ptr;

use turso_core::wasm::{WasmInstanceApi, WasmRuntimeApi};
use turso_core::LimboError;

// ── Thread-local napi env ───────────────────────────────────────────────────
// Set at the entry of each #[napi] method, read by runtime/instance methods.

thread_local! {
    static NAPI_ENV: Cell<napi::sys::napi_env> = const { Cell::new(ptr::null_mut()) };
}

pub fn set_env(raw: napi::sys::napi_env) {
    NAPI_ENV.with(|c| c.set(raw));
}

pub(crate) fn env() -> Result<napi::sys::napi_env, LimboError> {
    NAPI_ENV.with(|c| {
        let e = c.get();
        if e.is_null() {
            Err(LimboError::InternalError(
                "WASM UDF: napi env not available (use connectSync for WASM UDF support)"
                    .to_string(),
            ))
        } else {
            Ok(e)
        }
    })
}

// ── napi helpers ────────────────────────────────────────────────────────────

pub(crate) fn check(status: napi::sys::napi_status) -> Result<(), LimboError> {
    if status == napi::sys::Status::napi_ok {
        Ok(())
    } else {
        Err(LimboError::InternalError(format!(
            "napi call failed (status {status})"
        )))
    }
}

unsafe fn get_global(e: napi::sys::napi_env) -> Result<napi::sys::napi_value, LimboError> {
    let mut v = ptr::null_mut();
    check(napi::sys::napi_get_global(e, &mut v))?;
    Ok(v)
}

pub(crate) unsafe fn get_named_property(
    e: napi::sys::napi_env,
    obj: napi::sys::napi_value,
    name: &str,
) -> Result<napi::sys::napi_value, LimboError> {
    let mut key = ptr::null_mut();
    check(napi::sys::napi_create_string_utf8(
        e,
        name.as_ptr().cast(),
        name.len() as isize,
        &mut key,
    ))?;
    let mut v = ptr::null_mut();
    check(napi::sys::napi_get_property(e, obj, key, &mut v))?;
    Ok(v)
}

pub(crate) unsafe fn create_ref(
    e: napi::sys::napi_env,
    value: napi::sys::napi_value,
) -> Result<napi::sys::napi_ref, LimboError> {
    let mut r = ptr::null_mut();
    check(napi::sys::napi_create_reference(e, value, 1, &mut r))?;
    Ok(r)
}

pub(crate) unsafe fn deref_ref(
    e: napi::sys::napi_env,
    r: napi::sys::napi_ref,
) -> Result<napi::sys::napi_value, LimboError> {
    let mut v = ptr::null_mut();
    check(napi::sys::napi_get_reference_value(e, r, &mut v))?;
    Ok(v)
}

pub(crate) unsafe fn delete_ref(e: napi::sys::napi_env, r: napi::sys::napi_ref) {
    let _ = napi::sys::napi_delete_reference(e, r);
}

unsafe fn js_undefined(e: napi::sys::napi_env) -> napi::sys::napi_value {
    let mut v = ptr::null_mut();
    let _ = napi::sys::napi_get_undefined(e, &mut v);
    v
}

pub(crate) unsafe fn js_i32(
    e: napi::sys::napi_env,
    val: i32,
) -> Result<napi::sys::napi_value, LimboError> {
    let mut v = ptr::null_mut();
    check(napi::sys::napi_create_int32(e, val, &mut v))?;
    Ok(v)
}

/// Get the `WebAssembly` global object.
unsafe fn get_wasm_global(e: napi::sys::napi_env) -> Result<napi::sys::napi_value, LimboError> {
    let global = get_global(e)?;
    get_named_property(e, global, "WebAssembly")
}

// ── NodeWasmRuntime ─────────────────────────────────────────────────────────

pub struct NodeWasmRuntime {
    /// name → (napi_ref to WebAssembly.Module, export_name)
    modules: RefCell<HashMap<String, (napi::sys::napi_ref, String)>>,
}

// Single-threaded JS runtime — same justification as browser.rs `Opfs`.
unsafe impl Send for NodeWasmRuntime {}
unsafe impl Sync for NodeWasmRuntime {}

impl Default for NodeWasmRuntime {
    fn default() -> Self {
        Self {
            modules: RefCell::new(HashMap::new()),
        }
    }
}

impl NodeWasmRuntime {
    pub fn new() -> Self {
        Self::default()
    }
}

impl fmt::Debug for NodeWasmRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeWasmRuntime").finish()
    }
}

impl Drop for NodeWasmRuntime {
    fn drop(&mut self) {
        let e = NAPI_ENV.with(|c| c.get());
        if e.is_null() {
            return;
        }
        for (r, _) in self.modules.borrow().values() {
            unsafe { delete_ref(e, *r) };
        }
    }
}

impl WasmRuntimeApi for NodeWasmRuntime {
    fn add_module(
        &self,
        name: &str,
        wasm_bytes: &[u8],
        export_name: &str,
    ) -> Result<(), LimboError> {
        let e = env()?;
        unsafe {
            let wasm = get_wasm_global(e)?;
            let module_ctor = get_named_property(e, wasm, "Module")?;

            // Create ArrayBuffer + Uint8Array from wasm_bytes
            let mut ab_data = ptr::null_mut();
            let mut ab = ptr::null_mut();
            check(napi::sys::napi_create_arraybuffer(
                e,
                wasm_bytes.len(),
                &mut ab_data,
                &mut ab,
            ))?;
            ptr::copy_nonoverlapping(wasm_bytes.as_ptr(), ab_data.cast(), wasm_bytes.len());

            let mut typed = ptr::null_mut();
            check(napi::sys::napi_create_typedarray(
                e,
                napi::sys::TypedarrayType::uint8_array,
                wasm_bytes.len(),
                ab,
                0,
                &mut typed,
            ))?;

            // new WebAssembly.Module(bytes)
            let args = [typed];
            let mut module = ptr::null_mut();
            check(napi::sys::napi_new_instance(
                e,
                module_ctor,
                1,
                args.as_ptr(),
                &mut module,
            ))?;

            let r = create_ref(e, module)?;
            self.modules
                .borrow_mut()
                .insert(name.to_string(), (r, export_name.to_string()));
        }
        Ok(())
    }

    fn remove_module(&self, name: &str) {
        if let Some((r, _)) = self.modules.borrow_mut().remove(name) {
            let e = NAPI_ENV.with(|c| c.get());
            if !e.is_null() {
                unsafe { delete_ref(e, r) };
            }
        }
    }

    fn create_instance(&self, name: &str) -> Result<Box<dyn WasmInstanceApi>, LimboError> {
        let e = env()?;
        let modules = self.modules.borrow();
        let (module_ref, export_name) = modules
            .get(name)
            .ok_or_else(|| LimboError::InternalError(format!("WASM module '{name}' not found")))?;

        unsafe {
            let module = deref_ref(e, *module_ref)?;
            let wasm = get_wasm_global(e)?;
            let instance_ctor = get_named_property(e, wasm, "Instance")?;

            // new WebAssembly.Instance(module)
            let args = [module];
            let mut instance = ptr::null_mut();
            check(napi::sys::napi_new_instance(
                e,
                instance_ctor,
                1,
                args.as_ptr(),
                &mut instance,
            ))?;

            let exports = get_named_property(e, instance, "exports")?;
            let func = get_named_property(e, exports, export_name)?;
            let malloc = get_named_property(e, exports, "turso_malloc")?;
            let memory = get_named_property(e, exports, "memory")?;

            Ok(Box::new(NodeWasmInstance {
                func_ref: create_ref(e, func)?,
                malloc_ref: create_ref(e, malloc)?,
                memory_ref: create_ref(e, memory)?,
                read_buf: UnsafeCell::new(Vec::new()),
            }))
        }
    }

    fn has_module(&self, name: &str) -> bool {
        self.modules.borrow().contains_key(name)
    }
}

// ── NodeWasmInstance ────────────────────────────────────────────────────────

pub struct NodeWasmInstance {
    func_ref: napi::sys::napi_ref,
    malloc_ref: napi::sys::napi_ref,
    memory_ref: napi::sys::napi_ref,
    read_buf: UnsafeCell<Vec<u8>>,
}

unsafe impl Send for NodeWasmInstance {}

impl fmt::Debug for NodeWasmInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeWasmInstance").finish()
    }
}

impl Drop for NodeWasmInstance {
    fn drop(&mut self) {
        let e = NAPI_ENV.with(|c| c.get());
        if e.is_null() {
            return;
        }
        unsafe {
            delete_ref(e, self.func_ref);
            delete_ref(e, self.malloc_ref);
            delete_ref(e, self.memory_ref);
        }
    }
}

/// Get a direct pointer to a WebAssembly.Memory's backing buffer.
unsafe fn memory_data(
    e: napi::sys::napi_env,
    memory_ref: napi::sys::napi_ref,
) -> Result<(*mut u8, usize), LimboError> {
    let memory = deref_ref(e, memory_ref)?;
    let buffer = get_named_property(e, memory, "buffer")?;
    let mut data = ptr::null_mut();
    let mut len = 0;
    check(napi::sys::napi_get_arraybuffer_info(
        e, buffer, &mut data, &mut len,
    ))?;
    Ok((data.cast(), len))
}

impl WasmInstanceApi for NodeWasmInstance {
    fn write_memory(&mut self, offset: usize, bytes: &[u8]) -> Result<(), LimboError> {
        let e = env()?;
        unsafe {
            let (data, len) = memory_data(e, self.memory_ref)?;
            if offset + bytes.len() > len {
                return Err(LimboError::InternalError(
                    "WASM memory write out of bounds".to_string(),
                ));
            }
            ptr::copy_nonoverlapping(bytes.as_ptr(), data.add(offset), bytes.len());
        }
        Ok(())
    }

    fn read_memory(&self, offset: usize, len: usize) -> Result<&[u8], LimboError> {
        let e = env()?;
        unsafe {
            let (data, mem_len) = memory_data(e, self.memory_ref)?;
            if offset + len > mem_len {
                return Err(LimboError::InternalError(
                    "WASM memory read out of bounds".to_string(),
                ));
            }
            let buf = &mut *self.read_buf.get();
            buf.resize(len, 0);
            ptr::copy_nonoverlapping(data.add(offset), buf.as_mut_ptr(), len);
            Ok(buf.as_slice())
        }
    }

    fn memory_size(&self) -> usize {
        let e = match env() {
            Ok(e) => e,
            Err(_) => return 0,
        };
        unsafe {
            memory_data(e, self.memory_ref)
                .map(|(_, len)| len)
                .unwrap_or(0)
        }
    }

    fn malloc(&mut self, size: i32) -> Result<i32, LimboError> {
        let e = env()?;
        unsafe {
            let func = deref_ref(e, self.malloc_ref)?;
            let undef = js_undefined(e);
            let arg = js_i32(e, size)?;
            let args = [arg];
            let mut result = ptr::null_mut();
            check(napi::sys::napi_call_function(
                e,
                undef,
                func,
                1,
                args.as_ptr(),
                &mut result,
            ))?;
            let mut val = 0i32;
            check(napi::sys::napi_get_value_int32(e, result, &mut val))?;
            Ok(val)
        }
    }

    fn call_raw(&mut self, argc: i32, argv_ptr: i32) -> Result<i64, LimboError> {
        let e = env()?;
        unsafe {
            let func = deref_ref(e, self.func_ref)?;
            let undef = js_undefined(e);
            let argc_val = js_i32(e, argc)?;
            let argv_val = js_i32(e, argv_ptr)?;
            let args = [argc_val, argv_val];
            let mut result = ptr::null_mut();
            check(napi::sys::napi_call_function(
                e,
                undef,
                func,
                2,
                args.as_ptr(),
                &mut result,
            ))?;

            // WASM i64 returns become BigInt in JS
            let mut valuetype = napi::sys::ValueType::napi_undefined;
            check(napi::sys::napi_typeof(e, result, &mut valuetype))?;

            if valuetype == napi::sys::ValueType::napi_bigint {
                let mut val = 0i64;
                let mut lossless = false;
                check(napi::sys::napi_get_value_bigint_int64(
                    e,
                    result,
                    &mut val,
                    &mut lossless,
                ))?;
                Ok(val)
            } else {
                let mut val = 0f64;
                check(napi::sys::napi_get_value_double(e, result, &mut val))?;
                Ok(val as i64)
            }
        }
    }
}
