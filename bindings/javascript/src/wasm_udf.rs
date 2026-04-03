use std::cell::{RefCell, UnsafeCell};
use std::collections::HashMap;
use std::fmt;

use turso_core::wasm::{WasmInstanceApi, WasmRuntimeApi};
use turso_core::LimboError;

// ── Host WASM imports ───────────────────────────────────────────────────────
//
// These functions are provided by the JavaScript host (browser / Node / edge)
// via the "env" WASM import module, following the same pattern as browser.rs
// for OPFS file I/O.
//
// i64 return values are split into two i32 halves because wasm32 imports
// cannot pass i64 directly.

#[link(wasm_import_module = "env")]
extern "C" {
    fn udf_compile_module(bytes_ptr: *const u8, bytes_len: u32) -> i32;
    fn udf_destroy_module(handle: i32);
    fn udf_instantiate(module_handle: i32, export_ptr: *const u8, export_len: u32) -> i32;
    fn udf_destroy_instance(handle: i32);
    fn udf_write_memory(instance: i32, offset: u32, src_ptr: *const u8, len: u32) -> i32;
    fn udf_read_memory(instance: i32, offset: u32, dst_ptr: *mut u8, len: u32) -> i32;
    fn udf_memory_size(instance: i32) -> u32;
    fn udf_malloc(instance: i32, size: i32) -> i32;
    fn udf_call(instance: i32, argc: i32, argv_ptr: i32, result_hi: *mut i32, result_lo: *mut i32);
}

// ── WebWasmRuntime ──────────────────────────────────────────────────────────

/// Browser/edge WASM runtime that delegates UDF execution to the host's
/// native WebAssembly engine via imported functions.
pub struct WebWasmRuntime {
    /// name → (module_handle, export_name)
    modules: RefCell<HashMap<String, (i32, String)>>,
}

// Browser WASM is single-threaded; same justification as `Opfs` in browser.rs.
unsafe impl Send for WebWasmRuntime {}
unsafe impl Sync for WebWasmRuntime {}

impl Default for WebWasmRuntime {
    fn default() -> Self {
        Self {
            modules: RefCell::new(HashMap::new()),
        }
    }
}

impl WebWasmRuntime {
    pub fn new() -> Self {
        Self::default()
    }
}

impl fmt::Debug for WebWasmRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebWasmRuntime").finish()
    }
}

impl Drop for WebWasmRuntime {
    fn drop(&mut self) {
        let modules = self.modules.borrow();
        for (handle, _) in modules.values() {
            unsafe { udf_destroy_module(*handle) };
        }
    }
}

impl WasmRuntimeApi for WebWasmRuntime {
    fn add_module(
        &self,
        name: &str,
        wasm_bytes: &[u8],
        export_name: &str,
    ) -> Result<(), LimboError> {
        let handle = unsafe { udf_compile_module(wasm_bytes.as_ptr(), wasm_bytes.len() as u32) };
        if handle < 0 {
            return Err(LimboError::InternalError(format!(
                "udf_compile_module failed for '{name}' (error {handle})"
            )));
        }
        self.modules
            .borrow_mut()
            .insert(name.to_string(), (handle, export_name.to_string()));
        Ok(())
    }

    fn remove_module(&self, name: &str) {
        if let Some((handle, _)) = self.modules.borrow_mut().remove(name) {
            unsafe { udf_destroy_module(handle) };
        }
    }

    fn create_instance(&self, name: &str) -> Result<Box<dyn WasmInstanceApi>, LimboError> {
        let modules = self.modules.borrow();
        let (module_handle, export_name) = modules
            .get(name)
            .ok_or_else(|| LimboError::InternalError(format!("WASM module '{name}' not found")))?;
        let instance_handle = unsafe {
            udf_instantiate(
                *module_handle,
                export_name.as_ptr(),
                export_name.len() as u32,
            )
        };
        if instance_handle < 0 {
            return Err(LimboError::InternalError(format!(
                "udf_instantiate failed for '{name}' (error {instance_handle})"
            )));
        }
        Ok(Box::new(WebWasmInstance {
            instance_handle,
            read_buf: UnsafeCell::new(Vec::new()),
        }))
    }

    fn has_module(&self, name: &str) -> bool {
        self.modules.borrow().contains_key(name)
    }
}

// ── WebWasmInstance ─────────────────────────────────────────────────────────

/// A single instantiated WASM UDF module, backed by the host engine.
pub struct WebWasmInstance {
    instance_handle: i32,
    /// Reusable buffer for `read_memory` (which takes `&self`).
    read_buf: UnsafeCell<Vec<u8>>,
}

// Single-threaded browser environment.
unsafe impl Send for WebWasmInstance {}

impl fmt::Debug for WebWasmInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebWasmInstance")
            .field("handle", &self.instance_handle)
            .finish()
    }
}

impl Drop for WebWasmInstance {
    fn drop(&mut self) {
        unsafe { udf_destroy_instance(self.instance_handle) };
    }
}

impl WasmInstanceApi for WebWasmInstance {
    fn write_memory(&mut self, offset: usize, bytes: &[u8]) -> Result<(), LimboError> {
        let rc = unsafe {
            udf_write_memory(
                self.instance_handle,
                offset as u32,
                bytes.as_ptr(),
                bytes.len() as u32,
            )
        };
        if rc < 0 {
            return Err(LimboError::InternalError(format!(
                "udf_write_memory failed (error {rc})"
            )));
        }
        Ok(())
    }

    fn read_memory(&self, offset: usize, len: usize) -> Result<&[u8], LimboError> {
        // SAFETY: single-threaded browser environment; no concurrent access.
        let buf = unsafe { &mut *self.read_buf.get() };
        buf.resize(len, 0);
        let rc = unsafe {
            udf_read_memory(
                self.instance_handle,
                offset as u32,
                buf.as_mut_ptr(),
                len as u32,
            )
        };
        if rc < 0 {
            return Err(LimboError::InternalError(format!(
                "udf_read_memory failed (error {rc})"
            )));
        }
        Ok(buf.as_slice())
    }

    fn memory_size(&self) -> usize {
        unsafe { udf_memory_size(self.instance_handle) as usize }
    }

    fn malloc(&mut self, size: i32) -> Result<i32, LimboError> {
        let ptr = unsafe { udf_malloc(self.instance_handle, size) };
        if ptr < 0 {
            return Err(LimboError::InternalError(format!(
                "udf_malloc failed (error {ptr})"
            )));
        }
        Ok(ptr)
    }

    fn call_raw(&mut self, argc: i32, argv_ptr: i32) -> Result<i64, LimboError> {
        let mut result_hi: i32 = 0;
        let mut result_lo: i32 = 0;
        unsafe {
            udf_call(
                self.instance_handle,
                argc,
                argv_ptr,
                &mut result_hi,
                &mut result_lo,
            );
        }
        // Reassemble i64 from two i32 halves.
        let result = ((result_hi as i64) << 32) | (result_lo as i64 & 0xFFFF_FFFF);
        Ok(result)
    }
}
