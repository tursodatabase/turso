//! Turso-native WASM extension manifest types and registration logic.
//!
//! This module is only compiled when `feature = "wasm-udf"` is enabled.
//! It handles the JSON manifest protocol (`turso_ext_init` → JSON → registration).

use std::sync::Arc;

use crate::LimboError;

use super::{unmarshal_result, ExtensionRegistrations, WasmRuntimeApi, WasmVTabModuleDef};

// ── Extension manifest ──────────────────────────────────────────────────────

/// Manifest returned by an extension's `turso_ext_init` function.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionManifest {
    #[serde(default)]
    pub functions: Vec<ManifestFunction>,
    #[serde(default)]
    pub types: Vec<ManifestType>,
    #[serde(default)]
    pub vtabs: Vec<ManifestVTab>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ManifestFunction {
    pub name: String,
    pub export: String,
    /// Number of arguments, or -1 for variadic. Defaults to -1 if omitted.
    #[serde(default = "default_narg")]
    pub narg: i32,
}

fn default_narg() -> i32 {
    -1
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ManifestTypeParam {
    pub name: String,
    #[serde(default)]
    pub r#type: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ManifestTypeOperator {
    pub op: String,
    #[serde(default)]
    pub func: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ManifestType {
    pub name: String,
    pub base: String,
    #[serde(default)]
    pub params: Vec<ManifestTypeParam>,
    #[serde(default)]
    pub encode: Option<String>,
    #[serde(default)]
    pub decode: Option<String>,
    #[serde(default)]
    pub operators: Vec<ManifestTypeOperator>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ManifestVTabColumn {
    pub name: String,
    pub r#type: String,
    #[serde(default)]
    pub hidden: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ManifestVTab {
    pub name: String,
    pub columns: Vec<ManifestVTabColumn>,
    pub open: String,
    pub filter: String,
    pub column: String,
    pub next: String,
    pub eof: String,
    pub rowid: String,
    #[serde(default)]
    pub close: Option<String>,
}

/// Call `turso_ext_init` on a WASM module to get the extension manifest.
/// Creates a temporary instance, calls `turso_ext_init(0, 0)`, reads the
/// returned TAG_TEXT JSON, and deserializes it.
pub fn call_ext_init(
    runtime: &dyn WasmRuntimeApi,
    wasm_bytes: &[u8],
) -> Result<ExtensionManifest, LimboError> {
    // Register a temporary module to call turso_ext_init
    let tmp_name = "__turso_ext_init_tmp__";
    runtime.add_module(tmp_name, wasm_bytes, "turso_ext_init")?;
    let result = (|| {
        let mut instance = runtime.create_instance(tmp_name)?;
        let raw = instance.call_raw(0, 0)?;
        let value = unmarshal_result(&*instance, raw)?;
        match value {
            crate::types::Value::Text(t) => {
                let manifest: ExtensionManifest = serde_json::from_str(&t.value).map_err(|e| {
                    LimboError::InternalError(format!(
                        "failed to parse extension manifest JSON: {e}"
                    ))
                })?;
                Ok(manifest)
            }
            _ => Err(LimboError::InternalError(
                "turso_ext_init must return a TEXT value (JSON manifest)".to_string(),
            )),
        }
    })();
    runtime.remove_module(tmp_name);
    result
}

/// Register a turso-native WASM extension using the JSON manifest protocol.
///
/// Calls `turso_ext_init` to get the manifest, then registers all functions,
/// types, and vtab modules in the symbol table.
pub fn register_turso_native_extension(
    runtime: &Arc<dyn WasmRuntimeApi>,
    syms: &parking_lot::RwLock<crate::connection::SymbolTable>,
    wasm_bytes: &[u8],
) -> Result<ExtensionRegistrations, LimboError> {
    let manifest = call_ext_init(runtime.as_ref(), wasm_bytes)?;
    let wasm_bytes_arc = Arc::new(wasm_bytes.to_vec());

    // Check for conflicts before registering anything
    {
        let syms_guard = syms.read();
        for mf in &manifest.functions {
            syms_guard.check_function_name(&mf.name)?;
        }
        for vtab in &manifest.vtabs {
            syms_guard.check_vtab_module_name(&vtab.name)?;
        }
    }

    // Register function exports
    let exports: Vec<(&str, &str)> = manifest
        .functions
        .iter()
        .map(|f| (f.name.as_str(), f.export.as_str()))
        .collect();
    runtime.add_module_multi_export(wasm_bytes, &exports)?;

    let mut func_names = Vec::with_capacity(manifest.functions.len());
    {
        let mut syms_guard = syms.write();
        for mf in &manifest.functions {
            syms_guard.register_wasm_func(mf.name.clone(), mf.narg, None);
            func_names.push(mf.name.clone());
        }
    }

    // Register types
    let type_names: Vec<String> = manifest.types.iter().map(|t| t.name.clone()).collect();
    for mtype in &manifest.types {
        let valid_bases = ["TEXT", "INTEGER", "REAL", "BLOB"];
        if !valid_bases
            .iter()
            .any(|b| b.eq_ignore_ascii_case(&mtype.base))
        {
            return Err(LimboError::InternalError(format!(
                "invalid base type '{}' for custom type '{}': must be TEXT, INTEGER, REAL, or BLOB",
                mtype.base, mtype.name
            )));
        }
    }

    // Register vtab module exports and definitions
    let vtab_names: Vec<String> = manifest.vtabs.iter().map(|v| v.name.clone()).collect();
    for vtab in &manifest.vtabs {
        let valid_types = ["TEXT", "INTEGER", "REAL", "BLOB"];
        for col in &vtab.columns {
            if !valid_types
                .iter()
                .any(|t| t.eq_ignore_ascii_case(&col.r#type))
            {
                return Err(LimboError::InternalError(format!(
                    "invalid column type '{}' for vtab '{}' column '{}'",
                    col.r#type, vtab.name, col.name
                )));
            }
        }

        let vtab_exports: Vec<(String, String)> = {
            let prefix = format!("__wasm_vtab_{}", vtab.name);
            let mut exports = vec![
                (format!("{prefix}_open"), vtab.open.clone()),
                (format!("{prefix}_filter"), vtab.filter.clone()),
                (format!("{prefix}_column"), vtab.column.clone()),
                (format!("{prefix}_next"), vtab.next.clone()),
                (format!("{prefix}_eof"), vtab.eof.clone()),
                (format!("{prefix}_rowid"), vtab.rowid.clone()),
            ];
            if let Some(ref close) = vtab.close {
                exports.push((format!("{prefix}_close"), close.clone()));
            }
            exports
        };
        let export_refs: Vec<(&str, &str)> = vtab_exports
            .iter()
            .map(|(n, e)| (n.as_str(), e.as_str()))
            .collect();
        runtime.add_module_multi_export(wasm_bytes, &export_refs)?;

        let col_defs: Vec<String> = vtab
            .columns
            .iter()
            .map(|c| {
                if c.hidden {
                    format!("{} {} HIDDEN", c.name, c.r#type)
                } else {
                    format!("{} {}", c.name, c.r#type)
                }
            })
            .collect();
        let schema = format!("CREATE TABLE {}({})", vtab.name, col_defs.join(", "));

        let def = WasmVTabModuleDef {
            name: vtab.name.clone(),
            schema,
            open_export: vtab.open.clone(),
            filter_export: vtab.filter.clone(),
            column_export: vtab.column.clone(),
            next_export: vtab.next.clone(),
            eof_export: vtab.eof.clone(),
            rowid_export: vtab.rowid.clone(),
            close_export: vtab.close.clone(),
            wasm_bytes: wasm_bytes_arc.clone(),
        };
        let mut syms_guard = syms.write();
        syms_guard.vtab_modules.insert(
            vtab.name.clone(),
            Arc::new(crate::ext::VTabImpl {
                module_kind: turso_ext::VTabKind::VirtualTable,
                implementation: crate::ext::VTabImplKind::Wasm {
                    def: Arc::new(def),
                    runtime: runtime.clone(),
                },
            }),
        );
    }

    Ok(ExtensionRegistrations {
        func_names,
        type_names,
        vtab_names,
    })
}
