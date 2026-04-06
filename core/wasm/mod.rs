pub mod instance_cache;
#[cfg(feature = "wasm-udf")]
mod manifest;
pub mod vtab;

#[cfg(feature = "wasm-udf")]
pub use manifest::*;

use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use crate::connection::Connection;
use crate::types::Value;
use crate::{Database, LimboError, Result};

/// Default WASM instance cache capacity: 10 MiB.
pub const DEFAULT_WASM_CACHE_CAPACITY: i64 = 10 * 1024 * 1024;

/// Shared budget for WASM instance memory across all connections on a Database.
///
/// Each connection has its own local LRU cache, but they all share this budget
/// so the total WASM instance memory is bounded database-wide.
#[derive(Debug, Clone)]
pub struct WasmBudget {
    pub capacity: Arc<AtomicI64>,
    pub used: Arc<AtomicI64>,
}

impl WasmBudget {
    pub fn new(capacity: i64) -> Self {
        Self {
            capacity: Arc::new(AtomicI64::new(capacity)),
            used: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Try to reserve `bytes` from the shared budget. Returns true on success.
    pub fn try_reserve(&self, bytes: i64) -> bool {
        let capacity = self.capacity.load(Ordering::Relaxed);
        loop {
            let used = self.used.load(Ordering::Relaxed);
            if used + bytes > capacity {
                return false;
            }
            match self.used.compare_exchange_weak(
                used,
                used + bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }

    /// Release `bytes` back to the shared budget.
    pub fn release(&self, bytes: i64) {
        self.used.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn used(&self) -> i64 {
        self.used.load(Ordering::Relaxed)
    }

    pub fn capacity(&self) -> i64 {
        self.capacity.load(Ordering::Relaxed)
    }

    pub fn set_capacity(&self, capacity: i64) {
        self.capacity.store(capacity, Ordering::Relaxed);
    }
}

// ── Typed ABI types ─────────────────────────────────────────────────────────

/// Parameter type tag for the typed WASM ABI (stored in `turso_sig` section).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamType {
    I64 = 0x01,
    F64 = 0x02,
    Text = 0x03,
    Blob = 0x04,
    OptI64 = 0x05,
    OptF64 = 0x06,
    OptText = 0x07,
    OptBlob = 0x08,
}

/// Return type tag for the typed WASM ABI (stored in `turso_sig` section).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetType {
    I64 = 0x01,
    F64 = 0x02,
    Text = 0x03,
    Blob = 0x04,
    Void = 0x09,
    OptI64 = 0x05,
    OptF64 = 0x06,
    OptText = 0x07,
    OptBlob = 0x08,
}

/// A WASM-level value for typed calls (maps to WASM value types).
#[derive(Debug, Clone, Copy)]
pub enum WasmVal {
    I32(i32),
    I64(i64),
    F64(f64),
}

/// Full type signature for a typed WASM UDF, parsed from the `turso_sig` custom section.
#[derive(Debug, Clone)]
pub struct WasmFuncSig {
    pub param_types: Vec<ParamType>,
    pub ret_type: RetType,
}

/// Scratch area for text/blob arguments written by host into guest memory.
/// Starts at offset 1 (offset 0 is reserved: ptr=0 means null for Option<text/blob>).
/// Offsets 1–1015 hold argument data. Byte 1016 is the null flag for Option<i64>/Option<f64> returns.
const SCRATCH_END: usize = 1016;
const NULL_FLAG_OFFSET: usize = 1016;

// ── Cooperative yielding types ──────────────────────────────────────────────

/// Result of a WASM call that may yield (raw i64 return value).
pub enum WasmCallResult {
    /// Call completed: (raw_result, total_fuel_consumed).
    Done(i64, u64),
    /// Call suspended — resume with `call_resume()`.
    Yielded,
}

/// Result with an unmarshalled `Value` (used by VDBE).
pub enum WasmCallResultValue {
    Done(Value, u64),
    Yielded,
}

/// Default fuel quantum for cooperative yielding (instructions per yield).
pub const DEFAULT_WASM_FUEL_QUANTUM: u64 = 10_000;

// ── Traits ──────────────────────────────────────────────────────────────────

/// Low-level access to a single WASM module instance.
/// Runtime implementors provide memory read/write, malloc, and raw function call.
/// ABI marshalling is handled in core (shared across all runtimes).
pub trait WasmInstanceApi: Send + fmt::Debug {
    fn write_memory(&mut self, offset: usize, bytes: &[u8]) -> Result<(), LimboError>;
    fn read_memory(&self, offset: usize, len: usize) -> Result<&[u8], LimboError>;
    fn memory_size(&self) -> usize;
    fn malloc(&mut self, size: i32) -> Result<i32, LimboError>;
    /// Call the UDF with (argc, argv_ptr) → raw i64 return.
    fn call_raw(&mut self, argc: i32, argv_ptr: i32) -> Result<i64, LimboError>;

    /// Fuel consumed by the last `call_raw` invocation (0 if unsupported by the runtime).
    fn last_fuel_consumed(&self) -> u64 {
        0
    }

    /// Start a yielding call. The runtime will execute up to `fuel_quantum` instructions,
    /// then suspend if the function hasn't completed.
    /// Default: delegates to `call_raw` (no yielding).
    fn call_yielding(
        &mut self,
        argc: i32,
        argv_ptr: i32,
        _fuel_quantum: u64,
    ) -> Result<WasmCallResult, LimboError> {
        let result = self.call_raw(argc, argv_ptr)?;
        let fuel = self.last_fuel_consumed();
        Ok(WasmCallResult::Done(result, fuel))
    }

    /// Resume a previously yielded call. Returns `Done` when the function completes.
    /// Default: error (no yielding support).
    fn call_resume(&mut self, _fuel_quantum: u64) -> Result<WasmCallResult, LimboError> {
        Err(LimboError::InternalError(
            "WASM runtime does not support cooperative yielding".to_string(),
        ))
    }

    /// Whether there is an in-flight (suspended) call.
    fn has_in_flight_call(&self) -> bool {
        false
    }

    /// Call a named export function with (argc, argv_ptr) → raw i64 return.
    /// Used by vtab/index method cursors to call multiple exports on one instance.
    /// Default: ignores export_name and delegates to call_raw (works for single-export instances).
    fn call_export(
        &mut self,
        _export_name: &str,
        argc: i32,
        argv_ptr: i32,
    ) -> Result<i64, LimboError> {
        self.call_raw(argc, argv_ptr)
    }

    /// Call a named export with typed WASM values (i32/i64/f64).
    /// Used by the typed ABI path to bypass marshalling overhead.
    /// Default: error (runtime must implement this for typed calls to work).
    fn call_typed(
        &mut self,
        _export_name: &str,
        _args: &[WasmVal],
        _results: &mut [WasmVal],
    ) -> Result<(), LimboError> {
        Err(LimboError::InternalError(
            "WASM runtime does not support call_typed".to_string(),
        ))
    }
}

/// A function registration captured from a sqlite3-compat extension's init.
/// The extension calls sqlite3_create_function() which triggers a host import,
/// and the host captures the registration details here.
#[derive(Debug, Clone)]
pub struct ExtFuncRegistration {
    pub name: String,
    pub n_arg: i32,
    pub func_table_idx: i32,
    pub user_data_ptr: i32,
}

/// Per-database WASM runtime. Manages compiled modules and creates instances.
pub trait WasmRuntimeApi: Send + Sync + fmt::Debug {
    fn add_module(
        &self,
        name: &str,
        wasm_bytes: &[u8],
        export_name: &str,
    ) -> Result<(), LimboError>;
    fn remove_module(&self, name: &str);
    fn create_instance(&self, name: &str) -> Result<Box<dyn WasmInstanceApi>, LimboError>;
    fn has_module(&self, name: &str) -> bool;

    /// Compile a WASM module once and register multiple exports as separate named functions.
    /// Default: calls `add_module` N times (recompiles each time).
    fn add_module_multi_export(
        &self,
        wasm_bytes: &[u8],
        exports: &[(&str, &str)], // (registered_name, export_name)
    ) -> Result<(), LimboError> {
        for (name, export) in exports {
            self.add_module(name, wasm_bytes, export)?;
        }
        Ok(())
    }

    /// Initialize a sqlite3-compat extension by calling turso_ext_init with a
    /// host import that captures function registrations. Returns the list of
    /// registered functions, or an error if the module doesn't support this
    /// protocol (turso-native extensions use JSON manifest init instead).
    fn init_extension(&self, _wasm_bytes: &[u8]) -> Result<Vec<ExtFuncRegistration>, LimboError> {
        Err(LimboError::InternalError(
            "WASM runtime does not support init_extension".to_string(),
        ))
    }

    /// Register a function from a sqlite3-compat extension for dispatch via
    /// `__turso_call`. The `func_table_idx` and `user_data_ptr` are passed
    /// through to `__turso_call` at invocation time.
    fn add_extension_func(
        &self,
        _name: &str,
        _wasm_bytes: &[u8],
        _func_table_idx: i32,
        _user_data_ptr: i32,
    ) -> Result<(), LimboError> {
        Err(LimboError::InternalError(
            "WASM runtime does not support add_extension_func".to_string(),
        ))
    }
}

/// Definition for a WASM-based virtual table module, stored in SymbolTable.
#[derive(Debug, Clone)]
pub struct WasmVTabModuleDef {
    pub name: String,
    pub schema: String,
    pub open_export: String,
    pub filter_export: String,
    pub column_export: String,
    pub next_export: String,
    pub eof_export: String,
    pub rowid_export: String,
    pub close_export: Option<String>,
    pub wasm_bytes: std::sync::Arc<Vec<u8>>,
}

/// Results of compiling and registering a WASM extension.
pub struct ExtensionRegistrations {
    pub func_names: Vec<String>,
    pub type_names: Vec<String>,
    pub vtab_names: Vec<String>,
}

/// Compile a WASM extension module into the runtime and register all its
/// exports (functions, types, vtabs) in the symbol table.
///
/// Tries sqlite3-compat init (`runtime.init_extension`) first, then falls
/// back to turso-native JSON manifest init (`call_ext_init`).
pub fn compile_and_register_wasm_extension(
    runtime: &Arc<dyn WasmRuntimeApi>,
    syms: &parking_lot::RwLock<crate::connection::SymbolTable>,
    wasm_bytes: &[u8],
) -> Result<ExtensionRegistrations, LimboError> {
    let registrations = runtime.init_extension(wasm_bytes)?;
    if !registrations.is_empty() {
        // sqlite3-compat extension with host-import registrations
        // Check for conflicts before registering anything
        {
            let syms_guard = syms.read();
            for reg in &registrations {
                syms_guard.check_function_name(&reg.name)?;
            }
        }
        for reg in &registrations {
            runtime.add_extension_func(
                &reg.name,
                wasm_bytes,
                reg.func_table_idx,
                reg.user_data_ptr,
            )?;
        }
        let mut func_names = Vec::with_capacity(registrations.len());
        let mut syms_guard = syms.write();
        for reg in &registrations {
            syms_guard.register_wasm_func(reg.name.clone(), reg.n_arg, None);
            func_names.push(reg.name.clone());
        }
        drop(syms_guard);
        return Ok(ExtensionRegistrations {
            func_names,
            type_names: Vec::new(),
            vtab_names: Vec::new(),
        });
    }

    // Turso-native: Rust SDK extensions with JSON manifest
    #[cfg(feature = "wasm-udf")]
    return manifest::register_turso_native_extension(runtime, syms, wasm_bytes);

    #[cfg(not(feature = "wasm-udf"))]
    {
        let _ = (runtime, syms, wasm_bytes);
        return Err(LimboError::InternalError(
            "turso-native WASM extensions with JSON manifest require the wasm-udf feature"
                .to_string(),
        ));
    }
}

// ── Custom section parsing ──────────────────────────────────────────────────

/// Read a LEB128-encoded u32 from `bytes` starting at `pos`.
/// Returns `(value, new_pos)` or `None` if the data is truncated.
fn read_leb128_u32(bytes: &[u8], mut pos: usize) -> Option<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift = 0;
    loop {
        if pos >= bytes.len() {
            return None;
        }
        let byte = bytes[pos];
        pos += 1;
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Some((result, pos));
        }
        shift += 7;
        if shift >= 35 {
            return None; // overflow
        }
    }
}

/// Parse the `turso_sig` custom section from a WASM binary and return the
/// narg (parameter count) for the given export name, or `None` if the
/// section doesn't contain an entry for this export.
pub fn parse_narg_from_wasm(wasm_bytes: &[u8], export_name: &str) -> Option<i32> {
    let sig = parse_sig_from_wasm(wasm_bytes, export_name)?;
    Some(sig.param_types.len() as i32)
}

/// Parse the `turso_sig` custom section from a WASM binary and return the
/// full type signature for the given export name.
///
/// Section format: concatenated entries of
/// `[name_len: u8][name: bytes][narg: u8][param_types: narg bytes][ret_type: u8]`.
pub fn parse_sig_from_wasm(wasm_bytes: &[u8], export_name: &str) -> Option<WasmFuncSig> {
    if wasm_bytes.len() < 8 || &wasm_bytes[0..4] != b"\0asm" {
        return None;
    }

    let mut pos = 8;
    while pos < wasm_bytes.len() {
        let section_id = wasm_bytes[pos];
        pos += 1;

        let (section_size, new_pos) = read_leb128_u32(wasm_bytes, pos)?;
        pos = new_pos;
        let section_end = pos + section_size as usize;
        if section_end > wasm_bytes.len() {
            break;
        }

        if section_id == 0 {
            let (name_len, name_start) = read_leb128_u32(wasm_bytes, pos)?;
            let name_end = name_start + name_len as usize;
            if name_end > section_end {
                pos = section_end;
                continue;
            }
            let section_name = std::str::from_utf8(&wasm_bytes[name_start..name_end]).ok()?;
            if section_name == "turso_sig" {
                let mut entry_pos = name_end;
                while entry_pos < section_end {
                    if entry_pos >= section_end {
                        break;
                    }
                    let entry_name_len = wasm_bytes[entry_pos] as usize;
                    entry_pos += 1;
                    if entry_pos + entry_name_len > section_end {
                        break;
                    }
                    let entry_name = &wasm_bytes[entry_pos..entry_pos + entry_name_len];
                    entry_pos += entry_name_len;
                    if entry_pos >= section_end {
                        break;
                    }
                    let narg = wasm_bytes[entry_pos] as usize;
                    entry_pos += 1;
                    if entry_pos + narg >= section_end {
                        break;
                    }
                    let param_bytes = &wasm_bytes[entry_pos..entry_pos + narg];
                    entry_pos += narg;
                    if entry_pos >= section_end {
                        break;
                    }
                    let ret_byte = wasm_bytes[entry_pos];
                    entry_pos += 1;

                    if let Ok(name) = std::str::from_utf8(entry_name) {
                        if name == export_name {
                            let param_types: Option<Vec<ParamType>> =
                                param_bytes.iter().map(|&b| byte_to_param_type(b)).collect();
                            let ret_type = byte_to_ret_type(ret_byte)?;
                            return Some(WasmFuncSig {
                                param_types: param_types?,
                                ret_type,
                            });
                        }
                    }
                }
            }
        }

        pos = section_end;
    }
    None
}

/// Compile a WASM function module into the runtime and register it in the symbol table.
///
/// Used by both `op_add_function` (CREATE FUNCTION execution) and the database-open
/// bootstrap path so the compile+register logic lives in one place.
pub fn compile_and_register_wasm_func(
    runtime: &dyn WasmRuntimeApi,
    syms: &parking_lot::RwLock<crate::connection::SymbolTable>,
    def: &crate::schema::FunctionDef,
) -> Result<(), LimboError> {
    if !def.language.eq_ignore_ascii_case("wasm") {
        return Ok(());
    }

    syms.read().check_function_name(&def.name)?;

    let export = def.export_name.as_deref().unwrap_or(&def.name);
    runtime.add_module(&def.name, &def.wasm_blob, export)?;

    let sig = parse_sig_from_wasm(&def.wasm_blob, export);
    let narg = sig.as_ref().map_or_else(
        || parse_narg_from_wasm(&def.wasm_blob, export).unwrap_or(-1),
        |s| s.param_types.len() as i32,
    );
    syms.write().register_wasm_func(def.name.clone(), narg, sig);
    Ok(())
}

fn byte_to_param_type(b: u8) -> Option<ParamType> {
    match b {
        0x01 => Some(ParamType::I64),
        0x02 => Some(ParamType::F64),
        0x03 => Some(ParamType::Text),
        0x04 => Some(ParamType::Blob),
        0x05 => Some(ParamType::OptI64),
        0x06 => Some(ParamType::OptF64),
        0x07 => Some(ParamType::OptText),
        0x08 => Some(ParamType::OptBlob),
        _ => None,
    }
}

fn byte_to_ret_type(b: u8) -> Option<RetType> {
    match b {
        0x01 => Some(RetType::I64),
        0x02 => Some(RetType::F64),
        0x03 => Some(RetType::Text),
        0x04 => Some(RetType::Blob),
        0x05 => Some(RetType::OptI64),
        0x06 => Some(RetType::OptF64),
        0x07 => Some(RetType::OptText),
        0x08 => Some(RetType::OptBlob),
        0x09 => Some(RetType::Void),
        _ => None,
    }
}

/// How many i64 argv slots a parameter type consumes.
fn param_argv_slots(pt: &ParamType) -> usize {
    match pt {
        ParamType::I64 | ParamType::F64 => 1,
        ParamType::Text | ParamType::Blob => 1, // packed (len << 32) | ptr
        ParamType::OptI64 | ParamType::OptF64 => 2, // [is_null, value]
        ParamType::OptText | ParamType::OptBlob => 1, // ptr=0 means null
    }
}

/// Extract an i64 from a register value. Used by specialized fast-paths.
#[inline(always)]
fn reg_to_i64(value: &Value) -> i64 {
    match value {
        Value::Numeric(crate::numeric::Numeric::Integer(n)) => *n,
        Value::Numeric(crate::numeric::Numeric::Float(f)) => f64::from(*f) as i64,
        _ => 0,
    }
}

/// Extract an f64 from a register value. Used by specialized fast-paths.
#[inline(always)]
fn reg_to_f64(value: &Value) -> f64 {
    match value {
        Value::Numeric(crate::numeric::Numeric::Float(f)) => f64::from(*f),
        Value::Numeric(crate::numeric::Numeric::Integer(n)) => *n as f64,
        _ => 0.0,
    }
}

/// Call a WASM UDF using turso_sig knowledge.
///
/// Writes an argv array of raw typed i64 slots to the scratch area,
/// then calls via `call_raw` (TypedFunc). No tag bytes, no WasmVal boxing.
///
/// Contains specialized fast-paths for common numeric-only signatures
/// that batch all argv writes into a single `write_memory` call.
fn wasm_call_typed(
    instance: &mut dyn WasmInstanceApi,
    registers: &[crate::vdbe::Register],
    start_reg: usize,
    _export_name: &str,
    sig: &WasmFuncSig,
) -> Result<Value, LimboError> {
    // ── Specialized fast-paths for common signatures ─────────────────────
    // These avoid per-param enum matching and batch writes into one call.

    const ARGV_BASE: usize = 8;

    match (&*sig.param_types, sig.ret_type) {
        // (i64, i64) -> i64  — e.g. add(a, b)
        ([ParamType::I64, ParamType::I64], RetType::I64) => {
            let a = reg_to_i64(registers[start_reg].get_value());
            let b = reg_to_i64(registers[start_reg + 1].get_value());
            let mut buf = [0u8; 16];
            buf[0..8].copy_from_slice(&a.to_le_bytes());
            buf[8..16].copy_from_slice(&b.to_le_bytes());
            instance.write_memory(ARGV_BASE, &buf)?;
            let raw = instance.call_raw(2, ARGV_BASE as i32)?;
            return Ok(Value::from_i64(raw));
        }
        // (i64) -> i64
        ([ParamType::I64], RetType::I64) => {
            let a = reg_to_i64(registers[start_reg].get_value());
            instance.write_memory(ARGV_BASE, &a.to_le_bytes())?;
            let raw = instance.call_raw(1, ARGV_BASE as i32)?;
            return Ok(Value::from_i64(raw));
        }
        // (f64, f64) -> f64  — e.g. float_mul(a, b)
        ([ParamType::F64, ParamType::F64], RetType::F64) => {
            let a = reg_to_f64(registers[start_reg].get_value());
            let b = reg_to_f64(registers[start_reg + 1].get_value());
            let mut buf = [0u8; 16];
            buf[0..8].copy_from_slice(&a.to_bits().to_le_bytes());
            buf[8..16].copy_from_slice(&b.to_bits().to_le_bytes());
            instance.write_memory(ARGV_BASE, &buf)?;
            let raw = instance.call_raw(2, ARGV_BASE as i32)?;
            return Ok(Value::from_f64(f64::from_bits(raw as u64)));
        }
        // (f64, f64, f64, f64) -> f64  — e.g. distance_sq(x1, y1, x2, y2)
        ([ParamType::F64, ParamType::F64, ParamType::F64, ParamType::F64], RetType::F64) => {
            let mut buf = [0u8; 32];
            for i in 0..4 {
                let v = reg_to_f64(registers[start_reg + i].get_value());
                buf[i * 8..(i + 1) * 8].copy_from_slice(&v.to_bits().to_le_bytes());
            }
            instance.write_memory(ARGV_BASE, &buf)?;
            let raw = instance.call_raw(4, ARGV_BASE as i32)?;
            return Ok(Value::from_f64(f64::from_bits(raw as u64)));
        }
        _ => {} // fall through to generic path
    }

    // ── Generic path ─────────────────────────────────────────────────────
    // Calculate total argv slots
    let total_slots: usize = sig.param_types.iter().map(param_argv_slots).sum();

    let argv_end = ARGV_BASE + total_slots * 8;
    let mut data_offset = argv_end; // text/blob data goes after argv

    // Write each parameter to argv
    let mut slot = 0usize;
    for (i, param_type) in sig.param_types.iter().enumerate() {
        let value = registers[start_reg + i].get_value();
        let slot_offset = ARGV_BASE + slot * 8;

        match param_type {
            ParamType::I64 => {
                let v = match value {
                    Value::Numeric(crate::numeric::Numeric::Integer(n)) => *n,
                    Value::Numeric(crate::numeric::Numeric::Float(f)) => f64::from(*f) as i64,
                    Value::Null => 0,
                    _ => 0,
                };
                instance.write_memory(slot_offset, &v.to_le_bytes())?;
                slot += 1;
            }
            ParamType::F64 => {
                let v = match value {
                    Value::Numeric(crate::numeric::Numeric::Float(f)) => f64::from(*f),
                    Value::Numeric(crate::numeric::Numeric::Integer(n)) => *n as f64,
                    Value::Null => 0.0,
                    _ => 0.0,
                };
                instance.write_memory(slot_offset, &v.to_bits().to_le_bytes())?;
                slot += 1;
            }
            ParamType::Text => {
                let bytes = match value {
                    Value::Text(t) => t.value.as_bytes(),
                    _ => b"",
                };
                if data_offset + bytes.len() > SCRATCH_END {
                    return Err(LimboError::InternalError(
                        "WASM UDF: text argument exceeds scratch area".to_string(),
                    ));
                }
                instance.write_memory(data_offset, bytes)?;
                let packed = ((bytes.len() as u64) << 32) | (data_offset as u64);
                instance.write_memory(slot_offset, &packed.to_le_bytes())?;
                data_offset += bytes.len();
                slot += 1;
            }
            ParamType::Blob => {
                let bytes = match value {
                    Value::Blob(b) => b.as_slice(),
                    _ => b"",
                };
                if data_offset + bytes.len() > SCRATCH_END {
                    return Err(LimboError::InternalError(
                        "WASM UDF: blob argument exceeds scratch area".to_string(),
                    ));
                }
                instance.write_memory(data_offset, bytes)?;
                let packed = ((bytes.len() as u64) << 32) | (data_offset as u64);
                instance.write_memory(slot_offset, &packed.to_le_bytes())?;
                data_offset += bytes.len();
                slot += 1;
            }
            ParamType::OptI64 => {
                let (is_null, v): (i64, i64) = match value {
                    Value::Null => (1, 0),
                    Value::Numeric(crate::numeric::Numeric::Integer(n)) => (0, *n),
                    Value::Numeric(crate::numeric::Numeric::Float(f)) => (0, f64::from(*f) as i64),
                    _ => (0, 0),
                };
                instance.write_memory(slot_offset, &is_null.to_le_bytes())?;
                instance.write_memory(slot_offset + 8, &v.to_le_bytes())?;
                slot += 2;
            }
            ParamType::OptF64 => {
                let (is_null, v): (i64, f64) = match value {
                    Value::Null => (1, 0.0),
                    Value::Numeric(crate::numeric::Numeric::Float(f)) => (0, f64::from(*f)),
                    Value::Numeric(crate::numeric::Numeric::Integer(n)) => (0, *n as f64),
                    _ => (0, 0.0),
                };
                instance.write_memory(slot_offset, &is_null.to_le_bytes())?;
                instance.write_memory(slot_offset + 8, &v.to_bits().to_le_bytes())?;
                slot += 2;
            }
            ParamType::OptText => {
                match value {
                    Value::Null => {
                        instance.write_memory(slot_offset, &0u64.to_le_bytes())?;
                    }
                    Value::Text(t) => {
                        let bytes = t.value.as_bytes();
                        if data_offset + bytes.len() > SCRATCH_END {
                            return Err(LimboError::InternalError(
                                "WASM UDF: text argument exceeds scratch area".to_string(),
                            ));
                        }
                        instance.write_memory(data_offset, bytes)?;
                        let packed = ((bytes.len() as u64) << 32) | (data_offset as u64);
                        instance.write_memory(slot_offset, &packed.to_le_bytes())?;
                        data_offset += bytes.len();
                    }
                    _ => {
                        instance.write_memory(slot_offset, &0u64.to_le_bytes())?;
                    }
                }
                slot += 1;
            }
            ParamType::OptBlob => {
                match value {
                    Value::Null => {
                        instance.write_memory(slot_offset, &0u64.to_le_bytes())?;
                    }
                    Value::Blob(b) => {
                        let bytes = b.as_slice();
                        if data_offset + bytes.len() > SCRATCH_END {
                            return Err(LimboError::InternalError(
                                "WASM UDF: blob argument exceeds scratch area".to_string(),
                            ));
                        }
                        instance.write_memory(data_offset, bytes)?;
                        let packed = ((bytes.len() as u64) << 32) | (data_offset as u64);
                        instance.write_memory(slot_offset, &packed.to_le_bytes())?;
                        data_offset += bytes.len();
                    }
                    _ => {
                        instance.write_memory(slot_offset, &0u64.to_le_bytes())?;
                    }
                }
                slot += 1;
            }
        }
    }

    // Call via call_raw (uses TypedFunc<(i32, i32), i64>)
    let raw_result = instance.call_raw(total_slots as i32, ARGV_BASE as i32)?;

    // Interpret the i64 result based on return type
    match sig.ret_type {
        RetType::Void => Ok(Value::Null),
        RetType::I64 => Ok(Value::from_i64(raw_result)),
        RetType::F64 => Ok(Value::from_f64(f64::from_bits(raw_result as u64))),
        RetType::Text => {
            let ptr = (raw_result as u64 & 0xFFFF_FFFF) as usize;
            let len = ((raw_result as u64) >> 32) as usize;
            let bytes = instance.read_memory(ptr, len)?;
            let s = std::str::from_utf8(bytes)
                .map_err(|e| {
                    LimboError::InternalError(format!(
                        "WASM UDF: invalid UTF-8 in text return: {e}"
                    ))
                })?
                .to_string();
            Ok(Value::from_text(s))
        }
        RetType::Blob => {
            let ptr = (raw_result as u64 & 0xFFFF_FFFF) as usize;
            let len = ((raw_result as u64) >> 32) as usize;
            let bytes = instance.read_memory(ptr, len)?;
            Ok(Value::Blob(bytes.to_vec()))
        }
        RetType::OptI64 => {
            let null_flag = instance.read_memory(NULL_FLAG_OFFSET, 1)?;
            if null_flag[0] != 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::from_i64(raw_result))
            }
        }
        RetType::OptF64 => {
            let null_flag = instance.read_memory(NULL_FLAG_OFFSET, 1)?;
            if null_flag[0] != 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::from_f64(f64::from_bits(raw_result as u64)))
            }
        }
        RetType::OptText => {
            let ptr = (raw_result as u64 & 0xFFFF_FFFF) as usize;
            if ptr == 0 {
                Ok(Value::Null)
            } else {
                let len = ((raw_result as u64) >> 32) as usize;
                let bytes = instance.read_memory(ptr, len)?;
                let s = std::str::from_utf8(bytes)
                    .map_err(|e| {
                        LimboError::InternalError(format!("WASM UDF: invalid UTF-8: {e}"))
                    })?
                    .to_string();
                Ok(Value::from_text(s))
            }
        }
        RetType::OptBlob => {
            let ptr = (raw_result as u64 & 0xFFFF_FFFF) as usize;
            if ptr == 0 {
                Ok(Value::Null)
            } else {
                let len = ((raw_result as u64) >> 32) as usize;
                let bytes = instance.read_memory(ptr, len)?;
                Ok(Value::Blob(bytes.to_vec()))
            }
        }
    }
}

// ── ABI constants ───────────────────────────────────────────────────────────

/// Tag bytes for the WASM UDF ABI value encoding.
const TAG_INTEGER: u8 = 0x01;
const TAG_REAL: u8 = 0x02;
const TAG_TEXT: u8 = 0x03;
const TAG_BLOB: u8 = 0x04;
const TAG_NULL: u8 = 0x05;

/// Sideband address where the guest writes the return type tag.
/// The host reads this byte after each call to determine the return type,
/// eliminating ambiguity between raw integer values and tagged pointers.
const RET_TYPE_ADDR: usize = 1017;

/// Fixed argv region at offset 0 in WASM memory. 128 slots * 8 bytes = 1024 bytes,
/// fitting below the bump allocator start at offset 1024.
const ARGV_BASE: usize = 0;
const MAX_ARGV_SLOTS: usize = 128;

// ── ABI marshalling (shared across all runtimes) ────────────────────────────

/// Marshal a Value into a WASM i64 slot value.
/// For INTEGER/REAL: the value itself (as i64 bits).
/// For TEXT/BLOB/NULL: write tagged data into WASM memory via instance, return pointer.
fn marshal_arg(instance: &mut dyn WasmInstanceApi, value: &Value) -> Result<i64, LimboError> {
    match value {
        Value::Numeric(n) => match n {
            crate::numeric::Numeric::Integer(i) => Ok(*i),
            crate::numeric::Numeric::Float(f) => Ok(f64::from(*f).to_bits() as i64),
        },
        Value::Text(t) => {
            let bytes = t.value.as_bytes();
            // [TAG_TEXT][utf8 bytes][0x00]
            let total_len = 1 + bytes.len() + 1;
            let ptr = instance.malloc(total_len as i32)?;
            let offset = ptr as usize;
            let mut buf = Vec::with_capacity(total_len);
            buf.push(TAG_TEXT);
            buf.extend_from_slice(bytes);
            buf.push(0);
            instance.write_memory(offset, &buf)?;
            Ok(ptr as i64)
        }
        Value::Blob(data) => {
            // [TAG_BLOB][4-byte size][data]
            let total_len = 1 + 4 + data.len();
            let ptr = instance.malloc(total_len as i32)?;
            let offset = ptr as usize;
            let mut buf = Vec::with_capacity(total_len);
            buf.push(TAG_BLOB);
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(data);
            instance.write_memory(offset, &buf)?;
            Ok(ptr as i64)
        }
        Value::Null => {
            // [TAG_NULL]
            let ptr = instance.malloc(1)?;
            instance.write_memory(ptr as usize, &[TAG_NULL])?;
            Ok(ptr as i64)
        }
    }
}

/// Unmarshal a WASM return value into a Value.
///
/// Uses the sideband return type convention: the guest writes a type tag to
/// `RET_TYPE_ADDR` (1017) before returning. The host reads this byte to
/// determine the return type unambiguously.
///
/// If the sideband byte is 0 (not set), uses the inline-tag protocol
/// (sqlite3-compat extensions and WAT test modules). The inline-tag path
/// only checks pointers >= 1024 (the bump allocator start) to avoid
/// misinterpreting small raw integer values as tagged pointers.
fn unmarshal_result(instance: &dyn WasmInstanceApi, raw: i64) -> Result<Value, LimboError> {
    let mem_size = instance.memory_size();

    // Read the sideband return type tag.
    let ret_type = if RET_TYPE_ADDR < mem_size {
        instance.read_memory(RET_TYPE_ADDR, 1)?[0]
    } else {
        0
    };

    match ret_type {
        TAG_INTEGER => return Ok(Value::from_i64(raw)),
        TAG_REAL => {
            let bits = raw as u64;
            return Ok(Value::from_f64(f64::from_bits(bits)));
        }
        TAG_TEXT => {
            // Sideband text: ptr points to [utf8 bytes][0x00] (no tag prefix).
            let ptr = raw as usize;
            if ptr >= mem_size {
                return Err(LimboError::InternalError(
                    "WASM return: invalid text pointer".to_string(),
                ));
            }
            let remaining = mem_size - ptr;
            let mem = instance.read_memory(ptr, remaining)?;
            let end = mem.iter().position(|&b| b == 0).unwrap_or(remaining);
            let s = std::str::from_utf8(&mem[..end])
                .map_err(|e| LimboError::InternalError(format!("WASM return: invalid UTF-8: {e}")))?
                .to_string();
            return Ok(Value::from_text(s));
        }
        TAG_BLOB => {
            // Sideband blob: ptr points to [4-byte LE size][data] (no tag prefix).
            let ptr = raw as usize;
            if ptr + 4 > mem_size {
                return Err(LimboError::InternalError(
                    "WASM return: blob header truncated".to_string(),
                ));
            }
            let size_bytes = instance.read_memory(ptr, 4)?;
            let size = u32::from_le_bytes(size_bytes.try_into().unwrap()) as usize;
            if ptr + 4 + size > mem_size {
                return Err(LimboError::InternalError(
                    "WASM return: blob data truncated".to_string(),
                ));
            }
            let data = instance.read_memory(ptr + 4, size)?;
            return Ok(Value::Blob(data.to_vec()));
        }
        TAG_NULL => return Ok(Value::Null),
        _ => {}
    }

    // Inline-tag protocol: sideband not set (0). Used by sqlite3-compat
    // extensions and WAT test modules. Only check pointers >= 1024 (bump
    // allocator start) to avoid misinterpreting small raw integers as tagged
    // pointers.
    let ptr = raw as usize;
    if ptr >= 1024 && ptr < mem_size {
        let tag = instance.read_memory(ptr, 1)?;
        match tag[0] {
            TAG_INTEGER => {
                if ptr + 9 > mem_size {
                    return Err(LimboError::InternalError(
                        "WASM return: integer truncated".to_string(),
                    ));
                }
                let bytes = instance.read_memory(ptr + 1, 8)?;
                let val = i64::from_le_bytes(bytes.try_into().unwrap());
                return Ok(Value::from_i64(val));
            }
            TAG_REAL => {
                if ptr + 9 > mem_size {
                    return Err(LimboError::InternalError(
                        "WASM return: real truncated".to_string(),
                    ));
                }
                let bytes = instance.read_memory(ptr + 1, 8)?;
                let bits = u64::from_le_bytes(bytes.try_into().unwrap());
                return Ok(Value::from_f64(f64::from_bits(bits)));
            }
            TAG_TEXT => {
                let start = ptr + 1;
                let remaining = mem_size - start;
                let mem = instance.read_memory(start, remaining)?;
                let end = mem.iter().position(|&b| b == 0).unwrap_or(remaining);
                let s = std::str::from_utf8(&mem[..end])
                    .map_err(|e| {
                        LimboError::InternalError(format!("WASM return: invalid UTF-8: {e}"))
                    })?
                    .to_string();
                return Ok(Value::from_text(s));
            }
            TAG_BLOB => {
                if ptr + 5 > mem_size {
                    return Err(LimboError::InternalError(
                        "WASM return: blob header truncated".to_string(),
                    ));
                }
                let size_bytes = instance.read_memory(ptr + 1, 4)?;
                let size = u32::from_le_bytes(size_bytes.try_into().unwrap()) as usize;
                if ptr + 5 + size > mem_size {
                    return Err(LimboError::InternalError(
                        "WASM return: blob data truncated".to_string(),
                    ));
                }
                let data = instance.read_memory(ptr + 5, size)?;
                return Ok(Value::Blob(data.to_vec()));
            }
            TAG_NULL => return Ok(Value::Null),
            _ => {}
        }
    }

    // Default: treat raw value as integer.
    Ok(Value::from_i64(raw))
}

/// Call a WASM UDF reading arguments directly from VDBE registers.
/// This is the hot path — avoids allocating a Vec<Value>.
fn wasm_call_from_registers(
    instance: &mut dyn WasmInstanceApi,
    registers: &[crate::vdbe::Register],
    start_reg: usize,
    arg_count: usize,
) -> Result<Value, LimboError> {
    if arg_count > MAX_ARGV_SLOTS {
        return Err(LimboError::InternalError(format!(
            "WASM UDF: too many arguments ({arg_count}, max {MAX_ARGV_SLOTS})"
        )));
    }

    for i in 0..arg_count {
        let value = registers[start_reg + i].get_value();
        let slot_val = marshal_arg(instance, value)?;
        let offset = ARGV_BASE + i * 8;
        instance.write_memory(offset, &slot_val.to_le_bytes())?;
    }

    // Clear sideband return type tag before calling.
    instance.write_memory(RET_TYPE_ADDR, &[0x00])?;
    let result = instance.call_raw(arg_count as i32, ARGV_BASE as i32)?;

    unmarshal_result(&*instance, result)
}

/// Single entry point for calling a WASM UDF from VDBE registers.
/// Routes to the typed fast-path when a signature is available, otherwise
/// falls back to the generic marshalling path.
pub fn wasm_call_from_regs(
    instance: &mut dyn WasmInstanceApi,
    registers: &[crate::vdbe::Register],
    start_reg: usize,
    arg_count: usize,
    sig: Option<&WasmFuncSig>,
) -> Result<Value, LimboError> {
    if let Some(sig) = sig {
        wasm_call_typed(instance, registers, start_reg, "", sig)
    } else {
        wasm_call_from_registers(instance, registers, start_reg, arg_count)
    }
}

/// Call a WASM UDF reading arguments directly from VDBE registers, with cooperative yielding.
/// On first call, marshals args and starts a yielding call. Returns `Yielded` if the
/// function hasn't completed within `fuel_quantum` instructions.
pub fn wasm_call_from_registers_yielding(
    instance: &mut dyn WasmInstanceApi,
    registers: &[crate::vdbe::Register],
    start_reg: usize,
    arg_count: usize,
    fuel_quantum: u64,
) -> Result<WasmCallResultValue, LimboError> {
    if arg_count > MAX_ARGV_SLOTS {
        return Err(LimboError::InternalError(format!(
            "WASM UDF: too many arguments ({arg_count}, max {MAX_ARGV_SLOTS})"
        )));
    }

    for i in 0..arg_count {
        let value = registers[start_reg + i].get_value();
        let slot_val = marshal_arg(instance, value)?;
        let offset = ARGV_BASE + i * 8;
        instance.write_memory(offset, &slot_val.to_le_bytes())?;
    }

    // Clear sideband return type tag before calling.
    instance.write_memory(RET_TYPE_ADDR, &[0x00])?;
    let result = instance.call_yielding(arg_count as i32, ARGV_BASE as i32, fuel_quantum)?;

    match result {
        WasmCallResult::Done(raw, fuel) => {
            let value = unmarshal_result(&*instance, raw)?;
            Ok(WasmCallResultValue::Done(value, fuel))
        }
        WasmCallResult::Yielded => Ok(WasmCallResultValue::Yielded),
    }
}

/// Resume a previously yielded WASM call. Returns `Done(value, fuel)` when complete.
pub fn wasm_resume(
    instance: &mut dyn WasmInstanceApi,
    fuel_quantum: u64,
) -> Result<WasmCallResultValue, LimboError> {
    let result = instance.call_resume(fuel_quantum)?;

    match result {
        WasmCallResult::Done(raw, fuel) => {
            let value = unmarshal_result(&*instance, raw)?;
            Ok(WasmCallResultValue::Done(value, fuel))
        }
        WasmCallResult::Yielded => Ok(WasmCallResultValue::Yielded),
    }
}

/// Call a WASM UDF with a slice of Values.
pub fn wasm_call(instance: &mut dyn WasmInstanceApi, args: &[Value]) -> Result<Value, LimboError> {
    let argc = args.len();

    if argc > MAX_ARGV_SLOTS {
        return Err(LimboError::InternalError(format!(
            "WASM UDF: too many arguments ({argc}, max {MAX_ARGV_SLOTS})"
        )));
    }

    for (i, arg) in args.iter().enumerate() {
        let slot_val = marshal_arg(instance, arg)?;
        let offset = ARGV_BASE + i * 8;
        instance.write_memory(offset, &slot_val.to_le_bytes())?;
    }

    // Clear sideband return type tag before calling.
    instance.write_memory(RET_TYPE_ADDR, &[0x00])?;
    let result = instance.call_raw(argc as i32, ARGV_BASE as i32)?;

    unmarshal_result(&*instance, result)
}

/// Call a named WASM export with a slice of Values.
/// Like `wasm_call` but routes through `call_export` so one instance can call
/// multiple exports without creating a new instance per call.
pub fn wasm_call_export(
    instance: &mut dyn WasmInstanceApi,
    export_name: &str,
    args: &[Value],
) -> Result<Value, LimboError> {
    let argc = args.len();

    if argc > MAX_ARGV_SLOTS {
        return Err(LimboError::InternalError(format!(
            "WASM UDF: too many arguments ({argc}, max {MAX_ARGV_SLOTS})"
        )));
    }

    for (i, arg) in args.iter().enumerate() {
        let slot_val = marshal_arg(instance, arg)?;
        let offset = ARGV_BASE + i * 8;
        instance.write_memory(offset, &slot_val.to_le_bytes())?;
    }

    // Clear sideband return type tag before calling.
    instance.write_memory(RET_TYPE_ADDR, &[0x00])?;
    let result = instance.call_export(export_name, argc as i32, ARGV_BASE as i32)?;

    unmarshal_result(&*instance, result)
}

// ── Database-open bootstrap ─────────────────────────────────────────────────

/// Load persisted WASM functions and extensions from `__turso_internal_wasm`.
/// Called once during `Database::open` after `make_from_btree` completes.
pub fn bootstrap_from_storage(conn: &Arc<Connection>, db: &Arc<Database>) {
    if let Err(e) = load_persisted_functions(conn, db) {
        tracing::warn!("Failed to load user-defined functions during open: {e}");
    }
    if let Err(e) = load_persisted_extensions(conn, db) {
        tracing::warn!("Failed to load extensions during open: {e}");
    }
}

fn load_persisted_functions(conn: &Arc<Connection>, db: &Arc<Database>) -> Result<()> {
    conn.maybe_update_schema();
    let func_sqls = conn.query_stored_function_definitions()?;
    if func_sqls.is_empty() {
        return Ok(());
    }
    db.with_schema_mut(|schema| schema.load_function_definitions(&func_sqls))?;

    if let Some(ref runtime) = db.wasm_runtime {
        let schema = db.schema.lock();
        for def in schema.function_registry.values() {
            compile_and_register_wasm_func(runtime.as_ref(), &db.builtin_syms, def)?;
        }
    }
    Ok(())
}

#[cfg(feature = "wasm-udf")]
fn load_persisted_extensions(conn: &Arc<Connection>, db: &Arc<Database>) -> Result<()> {
    conn.maybe_update_schema();
    let ext_sqls = conn.query_stored_extension_definitions()?;
    if ext_sqls.is_empty() {
        return Ok(());
    }
    let Some(ref runtime) = db.wasm_runtime else {
        return Ok(());
    };
    for sql in &ext_sqls {
        use turso_parser::ast::{Cmd, Stmt};
        use turso_parser::parser::Parser;
        let mut parser = Parser::new(sql.as_bytes());
        let Ok(Some(Cmd::Stmt(Stmt::CreateExtension { wasm_blob, .. }))) = parser.next_cmd() else {
            continue;
        };
        let regs = compile_and_register_wasm_extension(runtime, &db.builtin_syms, &wasm_blob)?;
        db.with_schema_mut(|schema| {
            schema.add_extension_from_sql(sql, regs.func_names, regs.type_names, regs.vtab_names)
        })?;
    }
    Ok(())
}

#[cfg(not(feature = "wasm-udf"))]
fn load_persisted_extensions(_conn: &Arc<Connection>, _db: &Arc<Database>) -> Result<()> {
    Ok(())
}
