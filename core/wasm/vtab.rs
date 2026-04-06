use std::sync::Arc;

use crate::types::Value;
use crate::wasm::{wasm_call_export, WasmInstanceApi, WasmRuntimeApi, WasmVTabModuleDef};

#[derive(Debug, Clone)]
pub struct WasmVirtualTable {
    pub(crate) def: Arc<WasmVTabModuleDef>,
    pub(crate) runtime: Arc<dyn WasmRuntimeApi>,
}

impl WasmVirtualTable {
    pub fn open(&self) -> crate::Result<WasmVirtualTableCursor> {
        // Create a single WASM instance for this cursor, reused for all operations.
        let open_name = format!("__wasm_vtab_{}_open", self.def.name);
        let mut instance = self.runtime.create_instance(&open_name)?;
        let cursor_handle = instance.call_raw(0, 0)?;
        Ok(WasmVirtualTableCursor {
            instance,
            cursor_handle,
            filter_export: self.def.filter_export.clone(),
            column_export: self.def.column_export.clone(),
            next_export: self.def.next_export.clone(),
            rowid_export: self.def.rowid_export.clone(),
            def: self.def.clone(),
            runtime: self.runtime.clone(),
        })
    }
}

pub struct WasmVirtualTableCursor {
    /// Single WASM instance reused for all cursor operations (filter, next, column, etc.).
    instance: Box<dyn WasmInstanceApi>,
    cursor_handle: i64,
    filter_export: String,
    column_export: String,
    next_export: String,
    rowid_export: String,
    /// Kept for Clone support — clone needs to create a new instance.
    def: Arc<WasmVTabModuleDef>,
    runtime: Arc<dyn WasmRuntimeApi>,
}

// Safety: WasmInstanceApi is Send, WasmVTabModuleDef is Send+Sync, WasmRuntimeApi is Send+Sync
unsafe impl Send for WasmVirtualTableCursor {}
unsafe impl Sync for WasmVirtualTableCursor {}

impl std::fmt::Debug for WasmVirtualTableCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmVirtualTableCursor")
            .field("cursor_handle", &self.cursor_handle)
            .field("module", &self.def.name)
            .finish()
    }
}

impl Clone for WasmVirtualTableCursor {
    fn clone(&self) -> Self {
        // Clone creates a new WASM instance + cursor via open
        let open_name = format!("__wasm_vtab_{}_open", self.def.name);
        let mut instance = self
            .runtime
            .create_instance(&open_name)
            .expect("failed to create WASM vtab instance for clone");
        let cursor_handle = instance
            .call_raw(0, 0)
            .expect("failed to call open on WASM vtab clone");
        Self {
            instance,
            cursor_handle,
            filter_export: self.filter_export.clone(),
            column_export: self.column_export.clone(),
            next_export: self.next_export.clone(),
            rowid_export: self.rowid_export.clone(),
            def: self.def.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

impl WasmVirtualTableCursor {
    pub fn filter(&mut self, idx_num: i32, args: Vec<Value>) -> crate::Result<bool> {
        let mut all_args = vec![
            Value::from_i64(self.cursor_handle),
            Value::from_i64(idx_num as i64),
        ];
        all_args.extend(args);
        let result = wasm_call_export(&mut *self.instance, &self.filter_export, &all_args)?;
        // 0 = has rows, 1 = empty
        match result {
            Value::Numeric(n) => Ok(n.to_f64() as i64 == 0),
            _ => Ok(true),
        }
    }

    pub fn column(&mut self, idx: usize) -> crate::Result<Value> {
        let args = vec![
            Value::from_i64(self.cursor_handle),
            Value::from_i64(idx as i64),
        ];
        wasm_call_export(&mut *self.instance, &self.column_export, &args)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> crate::Result<bool> {
        let args = vec![Value::from_i64(self.cursor_handle)];
        let result = wasm_call_export(&mut *self.instance, &self.next_export, &args)?;
        // 0 = has more, 1 = EOF
        match result {
            Value::Numeric(n) => Ok(n.to_f64() as i64 == 0),
            _ => Ok(false),
        }
    }

    pub fn rowid(&mut self) -> i64 {
        let args = vec![Value::from_i64(self.cursor_handle)];
        match wasm_call_export(&mut *self.instance, &self.rowid_export, &args) {
            Ok(Value::Numeric(n)) => n.to_f64() as i64,
            _ => 0,
        }
    }
}
