use napi::bindgen_prelude::*;
use napi_derive::napi;
use turso_core::wasm::{WasmInstanceApi, WasmRuntimeApi};

/// Wasmtime-based WASM runtime exposed as a NAPI class.
/// Implements the WasmRuntime JS interface expected by the Turso Database constructor.
#[napi(js_name = "WasmtimeRuntime")]
pub struct WasmtimeRuntimeJs {
    inner: turso_wasm_wasmtime::WasmtimeRuntime,
}

#[napi]
impl WasmtimeRuntimeJs {
    #[napi(constructor)]
    pub fn new() -> napi::Result<Self> {
        let runtime = turso_wasm_wasmtime::WasmtimeRuntime::new().map_err(|e| {
            napi::Error::new(
                napi::Status::GenericFailure,
                format!("failed to create wasmtime runtime: {e}"),
            )
        })?;
        Ok(Self { inner: runtime })
    }

    #[napi]
    pub fn add_module(
        &self,
        name: String,
        wasm_bytes: Buffer,
        export_name: String,
    ) -> napi::Result<()> {
        self.inner
            .add_module(&name, &wasm_bytes, &export_name)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{e}")))
    }

    #[napi]
    pub fn remove_module(&self, name: String) {
        self.inner.remove_module(&name);
    }

    #[napi]
    pub fn create_instance(&self, name: String) -> napi::Result<WasmtimeInstanceJs> {
        let instance = self
            .inner
            .create_instance(&name)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{e}")))?;
        Ok(WasmtimeInstanceJs { inner: instance })
    }

    #[napi]
    pub fn has_module(&self, name: String) -> bool {
        self.inner.has_module(&name)
    }
}

/// A single wasmtime WASM instance exposed as a NAPI class.
/// Implements the WasmInstance JS interface expected by JsBridgeWasmRuntime.
#[napi(js_name = "WasmtimeInstance")]
pub struct WasmtimeInstanceJs {
    inner: Box<dyn WasmInstanceApi>,
}

#[napi]
impl WasmtimeInstanceJs {
    #[napi]
    pub fn write_memory(&mut self, offset: u32, bytes: Buffer) -> napi::Result<()> {
        self.inner
            .write_memory(offset as usize, &bytes)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{e}")))
    }

    #[napi]
    pub fn read_memory(&self, offset: u32, len: u32) -> napi::Result<Buffer> {
        let data = self
            .inner
            .read_memory(offset as usize, len as usize)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{e}")))?;
        Ok(Buffer::from(data.to_vec()))
    }

    #[napi]
    pub fn memory_size(&self) -> u32 {
        self.inner.memory_size() as u32
    }

    #[napi]
    pub fn malloc(&mut self, size: i32) -> napi::Result<i32> {
        self.inner
            .malloc(size)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{e}")))
    }

    #[napi]
    pub fn call_raw(&mut self, argc: i32, argv_ptr: i32) -> napi::Result<i64> {
        self.inner
            .call_raw(argc, argv_ptr)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{e}")))
    }
}

/// Create a new WasmtimeRuntime instance. Convenience factory function.
#[napi]
pub fn create_wasmtime_runtime() -> napi::Result<WasmtimeRuntimeJs> {
    WasmtimeRuntimeJs::new()
}
