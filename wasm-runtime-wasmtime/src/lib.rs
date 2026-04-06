//! Wasmtime-based WASM runtime for Turso user-defined functions and extensions.
//!
//! This crate provides [`WasmtimeRuntime`], an implementation of the
//! [`WasmRuntimeApi`](turso_core::wasm::WasmRuntimeApi) trait that uses
//! [wasmtime](https://wasmtime.dev/) to compile and execute WebAssembly modules.
//!
//! # Architecture
//!
//! Two-tier design: a shared [`WasmtimeEngine`] owns the wasmtime `Engine`,
//! compiled module cache, and epoch ticker thread. Per-database
//! [`WasmtimeRuntime`] instances hold their own name→module maps but share
//! compiled modules via `Arc`. This means N databases with the same WASM binary
//! compile it once.
//!
//! ```text
//!  WasmtimeEngine (shared, 1 per process)
//!    ├── wasmtime::Engine (with fuel + epoch config)
//!    ├── compiled_cache: SHA-256(bytes) → Arc<Module>
//!    └── epoch ticker thread (100ms ticks)
//!
//!  WasmtimeRuntime (per database)
//!    ├── shared: Arc<WasmtimeEngine>
//!    └── modules: name → WasmModule { Arc<Module>, export, dispatch_info }
//! ```
//!
//! # Calling conventions
//!
//! Two conventions are supported:
//!
//! - **Turso-native UDFs** (`CREATE FUNCTION ... LANGUAGE wasm`): The WASM
//!   module exports a function directly. Turso calls it with `(argc, argv) →
//!   result_ptr` where argv points to serialized `turso_value` structs in
//!   linear memory. Instantiated as [`WasmtimeInstance`].
//!
//! - **sqlite3-compat extensions** (`init_extension`): The module exports
//!   `__turso_init` (called once to register functions) and `__turso_call`
//!   (dispatch). Function registration happens via imported host functions
//!   (`create_function`, `result_*`, `value_*`). Instantiated as
//!   [`WasmtimeDispatchInstance`].
//!
//! # Safety mechanisms
//!
//! - **Epoch-based interruption**: A background thread increments the engine
//!   epoch every 100ms. Each call sets a deadline of 20 ticks (2 seconds).
//!   Runaway WASM traps with "epoch deadline exceeded".
//!
//! - **Fuel counting**: Each call starts with `u64::MAX / 2` fuel. Not used
//!   for limiting — purely for instruction metering via `fuel_consumed()`.
//!
//! # Usage
//!
//! Used by the CLI sync server, Rust SDK, and Python binding. The JavaScript
//! binding uses its own `NodeWasmRuntime` (V8's WebAssembly API) instead.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use sha2::{Digest, Sha256};
use turso_core::wasm::{ExtFuncRegistration, WasmInstanceApi, WasmRuntimeApi};
use turso_core::LimboError;
use wasmtime::{Config, Engine, Linker, Memory, Module, Store, TypedFunc};

/// Epoch tick interval in milliseconds.
const EPOCH_TICK_MS: u64 = 100;
/// Number of epoch ticks before a UDF is interrupted (20 × 100ms = 2s).
const EPOCH_DEADLINE_TICKS: u64 = 20;

/// Initial fuel added before each call. Effectively unlimited —
/// we only use fuel for instruction counting.
const FUEL_QUANTUM: u64 = u64::MAX / 2;

/// Dispatch info for sqlite3-compat extensions that use `__turso_call`.
#[derive(Clone)]
struct DispatchInfo {
    func_table_idx: i32,
    user_data_ptr: i32,
}

/// A compiled WASM module with metadata.
struct WasmModule {
    module: Arc<Module>,
    export_name: String,
    dispatch_info: Option<DispatchInfo>,
}

/// State held by the Store during `init_extension` to capture registrations.
struct ExtInitState {
    registrations: Vec<ExtFuncRegistration>,
}

/// Shared wasmtime engine with content-addressed compiled module cache.
///
/// One `WasmtimeEngine` can back multiple `WasmtimeRuntime` instances (one per
/// database). Identical WASM bytes compile to one `Module` shared via `Arc`.
/// The epoch ticker thread lives here, so N databases share one thread.
pub struct WasmtimeEngine {
    engine: Engine,
    compiled_cache: RwLock<HashMap<[u8; 32], Arc<Module>>>,
    epoch_stop: Arc<AtomicBool>,
    epoch_thread: Option<std::thread::JoinHandle<()>>,
}

impl WasmtimeEngine {
    pub fn new() -> Result<Self, LimboError> {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        let engine = Engine::new(&config).map_err(|e| {
            LimboError::InternalError(format!("failed to create wasmtime engine: {e}"))
        })?;

        let epoch_stop = Arc::new(AtomicBool::new(false));
        let epoch_engine = engine.clone();
        let stop_flag = epoch_stop.clone();
        let epoch_thread = std::thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(EPOCH_TICK_MS));
                epoch_engine.increment_epoch();
            }
        });

        Ok(Self {
            engine,
            compiled_cache: RwLock::new(HashMap::new()),
            epoch_stop,
            epoch_thread: Some(epoch_thread),
        })
    }

    /// Compile or return a cached module, keyed by SHA-256 of the WASM bytes.
    fn get_or_compile(&self, wasm_bytes: &[u8]) -> Result<Arc<Module>, LimboError> {
        let hash: [u8; 32] = Sha256::digest(wasm_bytes).into();
        // Fast path: read lock
        if let Some(m) = self.compiled_cache.read().unwrap().get(&hash) {
            return Ok(m.clone());
        }
        // Slow path: write lock, double-check
        let mut cache = self.compiled_cache.write().unwrap();
        if let Some(m) = cache.get(&hash) {
            return Ok(m.clone());
        }
        let module = Arc::new(Module::new(&self.engine, wasm_bytes).map_err(|e| {
            LimboError::InternalError(format!("failed to compile WASM module: {e}"))
        })?);
        cache.insert(hash, module.clone());
        Ok(module)
    }

    /// Create a per-database runtime backed by this shared engine.
    pub fn create_runtime(self: &Arc<Self>) -> WasmtimeRuntime {
        WasmtimeRuntime {
            shared: self.clone(),
            modules: RwLock::new(HashMap::new()),
        }
    }

    /// Number of unique compiled modules in the cache (for testing).
    pub fn compiled_module_count(&self) -> usize {
        self.compiled_cache.read().unwrap().len()
    }
}

impl Drop for WasmtimeEngine {
    fn drop(&mut self) {
        self.epoch_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.epoch_thread.take() {
            let _ = handle.join();
        }
    }
}

/// Wasmtime-based WASM runtime implementing `WasmRuntimeApi`.
///
/// Uses synchronous execution with epoch-based interruption for timeout.
/// Backed by a shared `WasmtimeEngine` for cross-database module deduplication.
pub struct WasmtimeRuntime {
    shared: Arc<WasmtimeEngine>,
    modules: RwLock<HashMap<String, Arc<WasmModule>>>,
}

impl fmt::Debug for WasmtimeRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmtimeRuntime")
            .field(
                "modules",
                &self.modules.read().unwrap().keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl WasmtimeRuntime {
    /// Create a standalone runtime with its own engine.
    /// For sharing modules across databases, use `WasmtimeEngine::create_runtime`.
    pub fn new() -> Result<Self, LimboError> {
        let engine = Arc::new(WasmtimeEngine::new()?);
        Ok(engine.create_runtime())
    }
}

impl WasmRuntimeApi for WasmtimeRuntime {
    fn add_module(
        &self,
        name: &str,
        wasm_bytes: &[u8],
        export_name: &str,
    ) -> Result<(), LimboError> {
        let module = self.shared.get_or_compile(wasm_bytes)?;
        let wasm_module = WasmModule {
            module,
            export_name: export_name.to_string(),
            dispatch_info: None,
        };
        self.modules
            .write()
            .unwrap()
            .insert(name.to_string(), Arc::new(wasm_module));
        Ok(())
    }

    fn remove_module(&self, name: &str) {
        self.modules.write().unwrap().remove(name);
    }

    fn create_instance(&self, name: &str) -> Result<Box<dyn WasmInstanceApi>, LimboError> {
        let modules = self.modules.read().unwrap();
        let wasm_module = modules.get(name).ok_or_else(|| {
            LimboError::InternalError(format!("WASM module '{name}' not found in runtime"))
        })?;

        let mut store = Store::new(&self.shared.engine, ());
        // Provide initial fuel so malloc calls during argument marshaling
        // (before call_raw resets the fuel) don't trap on an empty tank.
        store.set_fuel(FUEL_QUANTUM).map_err(|e| {
            LimboError::InternalError(format!("failed to set initial WASM fuel: {e}"))
        })?;
        store.set_epoch_deadline(EPOCH_DEADLINE_TICKS);

        let mut linker = Linker::new(&self.shared.engine);
        // Satisfy any WASI imports from wasi-sdk compiled modules with no-op defaults.
        // Sqlite3 compat extensions don't do I/O, so these stubs are never called.
        linker
            .define_unknown_imports_as_default_values(&wasm_module.module)
            .map_err(|e| {
                LimboError::InternalError(format!(
                    "failed to define default imports for WASM module: {e}"
                ))
            })?;
        let instance = linker
            .instantiate(&mut store, &wasm_module.module)
            .map_err(|e| {
                LimboError::InternalError(format!("failed to instantiate WASM module: {e}"))
            })?;

        // Call _initialize if exported (WASI reactor modules need this to set up
        // function tables, global constructors, etc.). No-op for non-WASI modules.
        if let Ok(init) = instance.get_typed_func::<(), ()>(&mut store, "_initialize") {
            init.call(&mut store, ())
                .map_err(|e| LimboError::InternalError(format!("WASM _initialize failed: {e}")))?;
        }

        let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| {
            LimboError::InternalError("WASM module does not export 'memory'".to_string())
        })?;

        let malloc = instance
            .get_typed_func::<i32, i32>(&mut store, "turso_malloc")
            .map_err(|e| {
                LimboError::InternalError(format!(
                    "WASM module does not export 'turso_malloc': {e}"
                ))
            })?;

        if let Some(ref dispatch) = wasm_module.dispatch_info {
            // sqlite3-compat extensions require turso_ext_init() to be called
            // to set up the sqlite3_api global pointer. Without this, the
            // extension's sqlite3_value_* / sqlite3_result_* calls would
            // dereference a NULL pointer. The __turso_register_func import
            // is a no-op in dispatch instances.
            if let Ok(ext_init) = instance.get_typed_func::<(), i32>(&mut store, "turso_ext_init") {
                let _ = ext_init.call(&mut store, ());
            }

            // sqlite3-compat: dispatch via __turso_call(func_ptr, user_data, argc, argv)
            let turso_call = instance
                .get_typed_func::<(i32, i32, i32, i32), i64>(&mut store, &wasm_module.export_name)
                .map_err(|e| {
                    LimboError::InternalError(format!(
                        "WASM module does not export '{}': {e}",
                        wasm_module.export_name
                    ))
                })?;

            Ok(Box::new(WasmtimeDispatchInstance {
                store,
                turso_call,
                malloc,
                memory,
                func_table_idx: dispatch.func_table_idx,
                user_data_ptr: dispatch.user_data_ptr,
                last_fuel_consumed: 0,
            }))
        } else {
            // Turso-native: all exports use (argc: i32, argv: i32) -> i64 convention.
            let func = instance
                .get_typed_func::<(i32, i32), i64>(&mut store, &wasm_module.export_name)
                .map_err(|e| {
                    LimboError::InternalError(format!(
                        "WASM module does not export '{}' with (i32, i32) -> i64 signature: {e}",
                        wasm_module.export_name
                    ))
                })?;

            Ok(Box::new(WasmtimeInstance {
                store,
                instance,
                func,
                malloc,
                memory,
                export_cache: HashMap::new(),
                last_fuel_consumed: 0,
            }))
        }
    }

    fn has_module(&self, name: &str) -> bool {
        self.modules.read().unwrap().contains_key(name)
    }

    fn add_module_multi_export(
        &self,
        wasm_bytes: &[u8],
        exports: &[(&str, &str)],
    ) -> Result<(), LimboError> {
        let module = self.shared.get_or_compile(wasm_bytes)?;
        let mut modules = self.modules.write().unwrap();
        for (name, export_name) in exports {
            let wasm_module = WasmModule {
                module: module.clone(),
                export_name: export_name.to_string(),
                dispatch_info: None,
            };
            modules.insert(name.to_string(), Arc::new(wasm_module));
        }
        Ok(())
    }

    fn init_extension(&self, wasm_bytes: &[u8]) -> Result<Vec<ExtFuncRegistration>, LimboError> {
        let module = self.shared.get_or_compile(wasm_bytes)?;

        // Check if this module imports __turso_register_func (sqlite3-compat extension).
        // If not, return empty vec so the caller uses turso-native JSON manifest init.
        let has_register_import = module
            .imports()
            .any(|imp| imp.module() == "env" && imp.name() == "__turso_register_func");
        if !has_register_import {
            return Ok(Vec::new());
        }

        let mut store = Store::new(
            &self.shared.engine,
            ExtInitState {
                registrations: Vec::new(),
            },
        );
        store.set_fuel(FUEL_QUANTUM).map_err(|e| {
            LimboError::InternalError(format!("failed to set WASM fuel for init: {e}"))
        })?;
        store.set_epoch_deadline(EPOCH_DEADLINE_TICKS);

        let mut linker = Linker::new(&self.shared.engine);

        // Provide the __turso_register_func host import
        linker
            .func_wrap(
                "env",
                "__turso_register_func",
                |mut caller: wasmtime::Caller<'_, ExtInitState>,
                 name_ptr: i32,
                 name_len: i32,
                 n_arg: i32,
                 x_func: i32,
                 p_app: i32| {
                    let memory = caller.get_export("memory").and_then(|e| e.into_memory());
                    let name = if let Some(mem) = memory {
                        let data = mem.data(&caller);
                        let start = name_ptr as usize;
                        let end = start + name_len as usize;
                        if end <= data.len() {
                            String::from_utf8_lossy(&data[start..end]).to_string()
                        } else {
                            return;
                        }
                    } else {
                        return;
                    };
                    caller.data_mut().registrations.push(ExtFuncRegistration {
                        name,
                        n_arg,
                        func_table_idx: x_func,
                        user_data_ptr: p_app,
                    });
                },
            )
            .map_err(|e| {
                LimboError::InternalError(format!(
                    "failed to define __turso_register_func import: {e}"
                ))
            })?;

        // Stub out remaining unknown imports (WASI, etc.)
        linker
            .define_unknown_imports_as_default_values(&module)
            .map_err(|e| {
                LimboError::InternalError(format!(
                    "failed to define default imports for init module: {e}"
                ))
            })?;

        let instance = linker.instantiate(&mut store, &module).map_err(|e| {
            LimboError::InternalError(format!("failed to instantiate init module: {e}"))
        })?;

        // Call _initialize for WASI reactor modules
        if let Ok(init) = instance.get_typed_func::<(), ()>(&mut store, "_initialize") {
            init.call(&mut store, ()).map_err(|e| {
                LimboError::InternalError(format!("WASM _initialize failed during init: {e}"))
            })?;
        }

        // Call turso_ext_init() -> i32 (returns SQLITE_OK on success)
        let ext_init = instance
            .get_typed_func::<(), i32>(&mut store, "turso_ext_init")
            .map_err(|e| {
                LimboError::InternalError(format!(
                    "WASM module does not export 'turso_ext_init': {e}"
                ))
            })?;
        let rc = ext_init
            .call(&mut store, ())
            .map_err(|e| LimboError::InternalError(format!("turso_ext_init failed: {e}")))?;
        if rc != 0 {
            return Err(LimboError::InternalError(format!(
                "turso_ext_init returned error code {rc}"
            )));
        }

        Ok(store.into_data().registrations)
    }

    fn add_extension_func(
        &self,
        name: &str,
        wasm_bytes: &[u8],
        func_table_idx: i32,
        user_data_ptr: i32,
    ) -> Result<(), LimboError> {
        let module = self.shared.get_or_compile(wasm_bytes)?;
        let wasm_module = WasmModule {
            module,
            export_name: "__turso_call".to_string(),
            dispatch_info: Some(DispatchInfo {
                func_table_idx,
                user_data_ptr,
            }),
        };
        self.modules
            .write()
            .unwrap()
            .insert(name.to_string(), Arc::new(wasm_module));
        Ok(())
    }
}

/// A single wasmtime WASM instance.
///
/// All calls are synchronous. Epoch-based interruption provides a 2s timeout
/// for runaway UDFs.
struct WasmtimeInstance {
    store: Store<()>,
    instance: wasmtime::Instance,
    func: TypedFunc<(i32, i32), i64>,
    malloc: TypedFunc<i32, i32>,
    memory: Memory,
    export_cache: HashMap<String, TypedFunc<(i32, i32), i64>>,
    last_fuel_consumed: u64,
}

/// A wasmtime WASM instance that dispatches via `__turso_call`.
/// Used for sqlite3-compat extensions. The `func_table_idx` and `user_data_ptr`
/// are baked in at registration time and passed to `__turso_call` on every invocation.
struct WasmtimeDispatchInstance {
    store: Store<()>,
    turso_call: TypedFunc<(i32, i32, i32, i32), i64>,
    malloc: TypedFunc<i32, i32>,
    memory: Memory,
    func_table_idx: i32,
    user_data_ptr: i32,
    last_fuel_consumed: u64,
}

impl fmt::Debug for WasmtimeInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmtimeInstance").finish()
    }
}

/// Format a wasmtime error including its full cause chain.
fn format_wasmtime_error(e: &wasmtime::Error) -> String {
    let mut msg = e.to_string();
    for cause in e.chain().skip(1) {
        msg.push_str(&format!("\nCaused by: {cause}"));
    }
    msg
}

impl fmt::Debug for WasmtimeDispatchInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmtimeDispatchInstance")
            .field("func_table_idx", &self.func_table_idx)
            .field("user_data_ptr", &self.user_data_ptr)
            .finish()
    }
}

impl WasmInstanceApi for WasmtimeDispatchInstance {
    fn write_memory(&mut self, offset: usize, bytes: &[u8]) -> Result<(), LimboError> {
        let mem = self.memory.data_mut(&mut self.store);
        if offset + bytes.len() > mem.len() {
            return Err(LimboError::InternalError(
                "WASM memory write out of bounds".to_string(),
            ));
        }
        mem[offset..offset + bytes.len()].copy_from_slice(bytes);
        Ok(())
    }

    fn read_memory(&self, offset: usize, len: usize) -> Result<&[u8], LimboError> {
        let mem = self.memory.data(&self.store);
        if offset + len > mem.len() {
            return Err(LimboError::InternalError(
                "WASM memory read out of bounds".to_string(),
            ));
        }
        Ok(&mem[offset..offset + len])
    }

    fn memory_size(&self) -> usize {
        self.memory.data(&self.store).len()
    }

    fn malloc(&mut self, size: i32) -> Result<i32, LimboError> {
        self.malloc
            .call(&mut self.store, size)
            .map_err(|e| LimboError::InternalError(format!("turso_malloc failed: {e}")))
    }

    fn call_raw(&mut self, argc: i32, argv_ptr: i32) -> Result<i64, LimboError> {
        self.store
            .set_fuel(FUEL_QUANTUM)
            .map_err(|e| LimboError::InternalError(format!("failed to set WASM fuel: {e}")))?;
        self.store.set_epoch_deadline(EPOCH_DEADLINE_TICKS);

        let result = self
            .turso_call
            .call(
                &mut self.store,
                (self.func_table_idx, self.user_data_ptr, argc, argv_ptr),
            )
            .map_err(|e| LimboError::ExtensionError(format_wasmtime_error(&e)))?;

        let remaining = self.store.get_fuel().unwrap_or(FUEL_QUANTUM);
        self.last_fuel_consumed = FUEL_QUANTUM.saturating_sub(remaining);

        Ok(result)
    }

    fn last_fuel_consumed(&self) -> u64 {
        self.last_fuel_consumed
    }
}

impl WasmInstanceApi for WasmtimeInstance {
    fn write_memory(&mut self, offset: usize, bytes: &[u8]) -> Result<(), LimboError> {
        let mem = self.memory.data_mut(&mut self.store);
        if offset + bytes.len() > mem.len() {
            return Err(LimboError::InternalError(
                "WASM memory write out of bounds".to_string(),
            ));
        }
        mem[offset..offset + bytes.len()].copy_from_slice(bytes);
        Ok(())
    }

    fn read_memory(&self, offset: usize, len: usize) -> Result<&[u8], LimboError> {
        let mem = self.memory.data(&self.store);
        if offset + len > mem.len() {
            return Err(LimboError::InternalError(
                "WASM memory read out of bounds".to_string(),
            ));
        }
        Ok(&mem[offset..offset + len])
    }

    fn memory_size(&self) -> usize {
        self.memory.data(&self.store).len()
    }

    fn malloc(&mut self, size: i32) -> Result<i32, LimboError> {
        self.malloc
            .call(&mut self.store, size)
            .map_err(|e| LimboError::InternalError(format!("turso_malloc failed: {e}")))
    }

    fn call_raw(&mut self, argc: i32, argv_ptr: i32) -> Result<i64, LimboError> {
        self.store
            .set_fuel(FUEL_QUANTUM)
            .map_err(|e| LimboError::InternalError(format!("failed to set WASM fuel: {e}")))?;
        self.store.set_epoch_deadline(EPOCH_DEADLINE_TICKS);

        let result = self
            .func
            .call(&mut self.store, (argc, argv_ptr))
            .map_err(|e| LimboError::ExtensionError(format_wasmtime_error(&e)))?;

        let remaining = self.store.get_fuel().unwrap_or(FUEL_QUANTUM);
        self.last_fuel_consumed = FUEL_QUANTUM.saturating_sub(remaining);

        Ok(result)
    }

    fn last_fuel_consumed(&self) -> u64 {
        self.last_fuel_consumed
    }

    fn call_export(
        &mut self,
        export_name: &str,
        argc: i32,
        argv_ptr: i32,
    ) -> Result<i64, LimboError> {
        self.store
            .set_fuel(FUEL_QUANTUM)
            .map_err(|e| LimboError::InternalError(format!("failed to set WASM fuel: {e}")))?;
        self.store.set_epoch_deadline(EPOCH_DEADLINE_TICKS);

        if !self.export_cache.contains_key(export_name) {
            let f = self
                .instance
                .get_typed_func::<(i32, i32), i64>(&mut self.store, export_name)
                .map_err(|e| {
                    LimboError::InternalError(format!(
                        "WASM module does not export function '{export_name}': {e}"
                    ))
                })?;
            self.export_cache.insert(export_name.to_string(), f);
        }
        let func = self.export_cache.get(export_name).unwrap().clone();

        let result = func
            .call(&mut self.store, (argc, argv_ptr))
            .map_err(|e| LimboError::ExtensionError(format_wasmtime_error(&e)))?;

        let remaining = self.store.get_fuel().unwrap_or(FUEL_QUANTUM);
        self.last_fuel_consumed = FUEL_QUANTUM.saturating_sub(remaining);

        Ok(result)
    }

    // call_typed, call_yielding, call_resume, has_in_flight_call: trait defaults are correct
}

#[cfg(test)]
mod tests {
    use super::*;

    /// WAT for a function that returns a constant (i64.const 42).
    /// Minimal body: exactly one fuel-consuming instruction.
    const CONST42_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
  (func (export "turso_malloc") (param $size i32) (result i32)
    (local $ptr i32)
    global.get $bump
    local.set $ptr
    global.get $bump
    local.get $size
    i32.add
    global.set $bump
    local.get $ptr
  )
  (func (export "const42") (param $argc i32) (param $argv i32) (result i64)
    i64.const 42
  )
)
"#;

    fn compile_wat(wat: &str) -> Vec<u8> {
        wat::parse_str(wat).expect("failed to parse WAT")
    }

    /// The const42 function body is `i64.const 42` (1 instruction).
    /// Wasmtime charges 2 fuel: 1 for the function entry block + 1 for the const.
    const CONST42_EXPECTED_FUEL: u64 = 2;

    /// The add function body has 7 instructions (2x local.get, i64.load,
    /// local.get, i32.const, i32.add, i64.load, i64.add). Wasmtime charges 8 fuel.
    const ADD_EXPECTED_FUEL: u64 = 8;

    #[test]
    fn test_fuel_consumed_exact_values() {
        let runtime = WasmtimeRuntime::new().unwrap();
        let wasm_bytes = compile_wat(CONST42_WAT);
        runtime
            .add_module("const42", &wasm_bytes, "const42")
            .unwrap();
        let mut instance = runtime.create_instance("const42").unwrap();

        // First call: assert exact fuel
        let result = instance.call_raw(0, 0).unwrap();
        assert_eq!(result, 42);
        assert_eq!(
            instance.last_fuel_consumed(),
            CONST42_EXPECTED_FUEL,
            "const42 must consume exactly {CONST42_EXPECTED_FUEL} fuel"
        );

        // Second call: must consume identical fuel (deterministic)
        let result = instance.call_raw(0, 0).unwrap();
        assert_eq!(result, 42);
        assert_eq!(
            instance.last_fuel_consumed(),
            CONST42_EXPECTED_FUEL,
            "repeated calls must consume identical fuel"
        );
    }

    /// WAT for add(a, b) -> a + b, reading two i64 values from argv.
    const ADD_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
  (func (export "turso_malloc") (param $size i32) (result i32)
    (local $ptr i32)
    global.get $bump
    local.set $ptr
    global.get $bump
    local.get $size
    i32.add
    global.set $bump
    local.get $ptr
  )
  (func (export "add") (param $argc i32) (param $argv i32) (result i64)
    local.get $argv
    i64.load
    local.get $argv
    i32.const 8
    i32.add
    i64.load
    i64.add
  )
)
"#;

    #[test]
    fn test_add_function_exact_fuel() {
        let runtime = WasmtimeRuntime::new().unwrap();

        let wasm_add = compile_wat(ADD_WAT);
        runtime.add_module("add", &wasm_add, "add").unwrap();
        let mut instance = runtime.create_instance("add").unwrap();

        // Write two i64 values at argv offsets
        instance.write_memory(0, &10i64.to_le_bytes()).unwrap();
        instance.write_memory(8, &20i64.to_le_bytes()).unwrap();

        let result = instance.call_raw(2, 0).unwrap();
        assert_eq!(result, 30);
        assert_eq!(
            instance.last_fuel_consumed(),
            ADD_EXPECTED_FUEL,
            "add must consume exactly {ADD_EXPECTED_FUEL} fuel"
        );

        // Static check: more instructions = more fuel
        const _: () = assert!(ADD_EXPECTED_FUEL > CONST42_EXPECTED_FUEL);
    }

    /// Verify that the default WasmInstanceApi::last_fuel_consumed() returns 0,
    /// so runtimes that don't support fuel counting degrade gracefully.
    #[test]
    fn test_default_last_fuel_consumed_is_zero() {
        use turso_core::wasm::WasmInstanceApi;

        #[derive(Debug)]
        struct StubInstance;

        impl WasmInstanceApi for StubInstance {
            fn write_memory(&mut self, _: usize, _: &[u8]) -> Result<(), LimboError> {
                Ok(())
            }
            fn read_memory(&self, _: usize, _: usize) -> Result<&[u8], LimboError> {
                Ok(&[])
            }
            fn memory_size(&self) -> usize {
                0
            }
            fn malloc(&mut self, _: i32) -> Result<i32, LimboError> {
                Ok(0)
            }
            fn call_raw(&mut self, _: i32, _: i32) -> Result<i64, LimboError> {
                Ok(0)
            }
            // last_fuel_consumed() intentionally NOT overridden — uses default
        }

        let stub = StubInstance;
        assert_eq!(
            stub.last_fuel_consumed(),
            0,
            "default last_fuel_consumed must return 0 for runtimes without fuel support"
        );
    }

    #[test]
    fn test_call_raw_still_works() {
        let runtime = WasmtimeRuntime::new().unwrap();
        let wasm_bytes = compile_wat(CONST42_WAT);
        runtime
            .add_module("const42", &wasm_bytes, "const42")
            .unwrap();
        let mut instance = runtime.create_instance("const42").unwrap();

        let result = instance.call_raw(0, 0).unwrap();
        assert_eq!(result, 42);
        assert_eq!(instance.last_fuel_consumed(), CONST42_EXPECTED_FUEL);
    }

    /// WAT for an infinite loop function — must be interrupted by epoch timeout.
    const INFINITE_LOOP_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
  (func (export "turso_malloc") (param $size i32) (result i32)
    (local $ptr i32)
    global.get $bump
    local.set $ptr
    global.get $bump
    local.get $size
    i32.add
    global.set $bump
    local.get $ptr
  )
  (func (export "infinite") (param $argc i32) (param $argv i32) (result i64)
    (loop $loop
      (br $loop)
    )
    i64.const 0
  )
)
"#;

    #[test]
    fn test_infinite_loop_is_interrupted() {
        let runtime = WasmtimeRuntime::new().unwrap();
        let wasm_bytes = compile_wat(INFINITE_LOOP_WAT);
        runtime
            .add_module("infinite", &wasm_bytes, "infinite")
            .unwrap();
        let mut instance = runtime.create_instance("infinite").unwrap();

        let start = std::time::Instant::now();
        let result = instance.call_raw(0, 0);
        let elapsed = start.elapsed();

        assert!(result.is_err(), "infinite loop must be interrupted");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("interrupt") || err_msg.contains("epoch"),
            "error must mention interrupt or epoch, got: {err_msg}"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "epoch timeout must trigger within 5s (actual: {elapsed:?})"
        );
    }
}
