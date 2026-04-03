//! Bridge WASM runtime that delegates to an external JS object via NAPI.
//!
//! Used when the user passes a custom runtime (e.g. wasmtime JS addon) to
//! the Database constructor. Methods are called on the JS object via raw NAPI.

use std::cell::UnsafeCell;
use std::fmt;
use std::ptr;

use turso_core::wasm::{WasmInstanceApi, WasmRuntimeApi};
use turso_core::LimboError;

use crate::node_wasm_udf::{
    check, create_ref, delete_ref, deref_ref, env, get_named_property, js_i32,
};

// ── Additional NAPI helpers ──────────────────────────────────────────────────

unsafe fn js_string(e: napi::sys::napi_env, s: &str) -> Result<napi::sys::napi_value, LimboError> {
    let mut v = ptr::null_mut();
    check(napi::sys::napi_create_string_utf8(
        e,
        s.as_ptr().cast(),
        s.len() as isize,
        &mut v,
    ))?;
    Ok(v)
}

unsafe fn js_buffer(
    e: napi::sys::napi_env,
    bytes: &[u8],
) -> Result<napi::sys::napi_value, LimboError> {
    let mut data_ptr: *mut std::ffi::c_void = ptr::null_mut();
    let mut buf = ptr::null_mut();
    check(napi::sys::napi_create_buffer_copy(
        e,
        bytes.len(),
        bytes.as_ptr().cast(),
        &mut data_ptr,
        &mut buf,
    ))?;
    Ok(buf)
}

unsafe fn call_method(
    e: napi::sys::napi_env,
    obj: napi::sys::napi_value,
    method_name: &str,
    args: &[napi::sys::napi_value],
) -> Result<napi::sys::napi_value, LimboError> {
    let method = get_named_property(e, obj, method_name)?;
    let mut result = ptr::null_mut();
    check(napi::sys::napi_call_function(
        e,
        obj,
        method,
        args.len(),
        args.as_ptr(),
        &mut result,
    ))?;
    Ok(result)
}

unsafe fn get_bool(
    e: napi::sys::napi_env,
    value: napi::sys::napi_value,
) -> Result<bool, LimboError> {
    let mut result = false;
    check(napi::sys::napi_get_value_bool(e, value, &mut result))?;
    Ok(result)
}

// ── JsBridgeWasmRuntime ─────────────────────────────────────────────────────

/// WASM runtime backed by a JS object conforming to the WasmRuntime interface.
/// Delegates addModule/createInstance/etc. to JS via raw NAPI calls.
pub struct JsBridgeWasmRuntime {
    runtime_ref: napi::sys::napi_ref,
}

// Single-threaded JS runtime — same justification as NodeWasmRuntime.
unsafe impl Send for JsBridgeWasmRuntime {}
unsafe impl Sync for JsBridgeWasmRuntime {}

impl JsBridgeWasmRuntime {
    pub fn new(runtime_ref: napi::sys::napi_ref) -> Self {
        Self { runtime_ref }
    }
}

impl fmt::Debug for JsBridgeWasmRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsBridgeWasmRuntime").finish()
    }
}

impl Drop for JsBridgeWasmRuntime {
    fn drop(&mut self) {
        if let Ok(e) = env() {
            unsafe { delete_ref(e, self.runtime_ref) };
        }
    }
}

impl WasmRuntimeApi for JsBridgeWasmRuntime {
    fn add_module(
        &self,
        name: &str,
        wasm_bytes: &[u8],
        export_name: &str,
    ) -> Result<(), LimboError> {
        let e = env()?;
        unsafe {
            let obj = deref_ref(e, self.runtime_ref)?;
            let name_val = js_string(e, name)?;
            let bytes_val = js_buffer(e, wasm_bytes)?;
            let export_val = js_string(e, export_name)?;
            call_method(e, obj, "addModule", &[name_val, bytes_val, export_val])?;
        }
        Ok(())
    }

    fn remove_module(&self, name: &str) {
        let Ok(e) = env() else { return };
        unsafe {
            let Ok(obj) = deref_ref(e, self.runtime_ref) else {
                return;
            };
            let Ok(name_val) = js_string(e, name) else {
                return;
            };
            let _ = call_method(e, obj, "removeModule", &[name_val]);
        }
    }

    fn create_instance(&self, name: &str) -> Result<Box<dyn WasmInstanceApi>, LimboError> {
        let e = env()?;
        unsafe {
            let obj = deref_ref(e, self.runtime_ref)?;
            let name_val = js_string(e, name)?;
            let instance_obj = call_method(e, obj, "createInstance", &[name_val])?;
            let instance_ref = create_ref(e, instance_obj)?;
            Ok(Box::new(JsBridgeWasmInstance {
                instance_ref,
                read_buf: UnsafeCell::new(Vec::new()),
            }))
        }
    }

    fn has_module(&self, name: &str) -> bool {
        let Ok(e) = env() else { return false };
        unsafe {
            let Ok(obj) = deref_ref(e, self.runtime_ref) else {
                return false;
            };
            let Ok(name_val) = js_string(e, name) else {
                return false;
            };
            let Ok(result) = call_method(e, obj, "hasModule", &[name_val]) else {
                return false;
            };
            get_bool(e, result).unwrap_or(false)
        }
    }
}

// ── JsBridgeWasmInstance ────────────────────────────────────────────────────

/// WASM instance backed by a JS object conforming to the WasmInstance interface.
pub struct JsBridgeWasmInstance {
    instance_ref: napi::sys::napi_ref,
    read_buf: UnsafeCell<Vec<u8>>,
}

unsafe impl Send for JsBridgeWasmInstance {}

impl fmt::Debug for JsBridgeWasmInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsBridgeWasmInstance").finish()
    }
}

impl Drop for JsBridgeWasmInstance {
    fn drop(&mut self) {
        if let Ok(e) = env() {
            unsafe { delete_ref(e, self.instance_ref) };
        }
    }
}

impl WasmInstanceApi for JsBridgeWasmInstance {
    fn write_memory(&mut self, offset: usize, bytes: &[u8]) -> Result<(), LimboError> {
        let e = env()?;
        unsafe {
            let obj = deref_ref(e, self.instance_ref)?;
            let offset_val = js_i32(e, offset as i32)?;
            let bytes_val = js_buffer(e, bytes)?;
            call_method(e, obj, "writeMemory", &[offset_val, bytes_val])?;
        }
        Ok(())
    }

    fn read_memory(&self, offset: usize, len: usize) -> Result<&[u8], LimboError> {
        let e = env()?;
        unsafe {
            let obj = deref_ref(e, self.instance_ref)?;
            let offset_val = js_i32(e, offset as i32)?;
            let len_val = js_i32(e, len as i32)?;
            let result = call_method(e, obj, "readMemory", &[offset_val, len_val])?;

            // Extract data from the returned Buffer
            let mut data: *mut std::ffi::c_void = ptr::null_mut();
            let mut buf_len = 0usize;
            check(napi::sys::napi_get_buffer_info(
                e,
                result,
                &mut data,
                &mut buf_len,
            ))?;

            let buf = &mut *self.read_buf.get();
            buf.resize(buf_len, 0);
            ptr::copy_nonoverlapping(data.cast::<u8>(), buf.as_mut_ptr(), buf_len);
            Ok(buf.as_slice())
        }
    }

    fn memory_size(&self) -> usize {
        let Ok(e) = env() else { return 0 };
        unsafe {
            let Ok(obj) = deref_ref(e, self.instance_ref) else {
                return 0;
            };
            let Ok(result) = call_method(e, obj, "memorySize", &[]) else {
                return 0;
            };
            let mut val = 0u32;
            if napi::sys::napi_get_value_uint32(e, result, &mut val) == napi::sys::Status::napi_ok {
                val as usize
            } else {
                0
            }
        }
    }

    fn malloc(&mut self, size: i32) -> Result<i32, LimboError> {
        let e = env()?;
        unsafe {
            let obj = deref_ref(e, self.instance_ref)?;
            let size_val = js_i32(e, size)?;
            let result = call_method(e, obj, "malloc", &[size_val])?;
            let mut val = 0i32;
            check(napi::sys::napi_get_value_int32(e, result, &mut val))?;
            Ok(val)
        }
    }

    fn call_raw(&mut self, argc: i32, argv_ptr: i32) -> Result<i64, LimboError> {
        let e = env()?;
        unsafe {
            let obj = deref_ref(e, self.instance_ref)?;
            let argc_val = js_i32(e, argc)?;
            let argv_val = js_i32(e, argv_ptr)?;
            let result = call_method(e, obj, "callRaw", &[argc_val, argv_val])?;

            // Handle BigInt or Number return (same as NodeWasmInstance::call_raw)
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
