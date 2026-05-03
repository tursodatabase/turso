// API surface parity test for turso (local) vs turso_serverless.
//
// Reads `cargo doc --json` output from target/doc/ and asserts that every
// public method on a type in turso_serverless also exists on the corresponding
// type in turso, and vice versa.
//
// Requires nightly:
//   RUSTDOCFLAGS="-Z unstable-options --output-format json" \
//     cargo +nightly doc -p turso -p turso_serverless --no-deps
//
// Then run:
//   cargo test --test api_surface

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

/// Types that exist in both crates and should have matching APIs.
const PAIRED_TYPES: &[&str] = &[
    "Connection",
    "Statement",
    "Rows",
    "Row",
    "Column",
    "Value",
    "ValueRef",
    "Transaction",
    "Database",
    "Builder",
];

/// Methods that only make sense on the local driver (file I/O, FFI).
const LOCAL_ONLY: &[(&str, &str)] = &[
    // Builder: local-only configuration
    ("Builder", "new_local"),
    ("Builder", "with_io"),
    ("Builder", "with_io_impl"),
    ("Builder", "with_encryption"),
    ("Builder", "experimental_attach"),
    ("Builder", "experimental_custom_types"),
    ("Builder", "experimental_encryption"),
    ("Builder", "experimental_generated_columns"),
    ("Builder", "experimental_index_method"),
    ("Builder", "experimental_materialized_views"),
    ("Builder", "experimental_multiprocess_wal"),
    ("Builder", "experimental_strict"),
    ("Builder", "experimental_triggers"),
    // Connection: low-level FFI / local-only
    ("Connection", "create"),
    ("Connection", "busy_timeout"),
    ("Connection", "cacheflush"),
    ("Connection", "prepare_cached"),
    // Transaction: local-only drop behavior
    ("Transaction", "drop_behavior"),
    ("Transaction", "set_drop_behavior"),
];

/// Methods that only make sense on the remote driver.
const REMOTE_ONLY: &[(&str, &str)] = &[
    ("Builder", "new_remote"),
    ("Builder", "with_auth_token"),
    ("Column", "new"),
    ("Connection", "close"),
    ("Transaction", "conn"),
];

fn workspace_root() -> PathBuf {
    // tests/api_surface.rs -> testing/hegel/rust/ -> testing/hegel/ -> testing/ -> workspace root
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Extract type name from a rustdoc JSON `resolved_path` object.
/// The format is: `{"id": <int>, "path": "<TypeName>", "args": ...}`
/// The path may be qualified (e.g. "crate::Connection"), so we take the last segment.
fn resolve_type_name(type_for: &serde_json::Value) -> Option<String> {
    let rp = type_for.get("resolved_path")?.as_object()?;
    let path = rp.get("path")?.as_str()?;
    // Take the last segment: "crate::Connection" -> "Connection"
    Some(path.rsplit("::").next().unwrap_or(path).to_string())
}

fn parse_methods(json_path: &std::path::Path) -> BTreeMap<String, BTreeSet<String>> {
    let content = match std::fs::read_to_string(json_path) {
        Ok(c) => c,
        Err(e) => panic!(
            "Cannot read {}: {}\nRun: RUSTDOCFLAGS=\"-Z unstable-options --output-format json\" cargo +nightly doc -p turso -p turso_serverless --no-deps",
            json_path.display(), e
        ),
    };
    let doc: serde_json::Value = serde_json::from_str(&content).unwrap();
    let index = doc.get("index").and_then(|i| i.as_object()).unwrap();
    let mut result: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for item in index.values() {
        let inner = match item.get("inner").and_then(|i| i.as_object()) {
            Some(i) => i,
            None => continue,
        };
        let impl_data = match inner.get("impl").and_then(|i| i.as_object()) {
            Some(i) => i,
            None => continue,
        };
        // Skip trait impls (inherent impls have "trait": null)
        if impl_data.get("trait").is_some_and(|v| !v.is_null()) {
            continue;
        }
        // inherent impl (not trait impl)
        // Resolve type name from "for" field
        let type_for = match impl_data.get("for") {
            Some(f) => f,
            None => continue,
        };
        let type_name = match resolve_type_name(type_for) {
            Some(n) if PAIRED_TYPES.contains(&n.as_str()) => n,
            _ => continue,
        };

        let items = match impl_data.get("items").and_then(|i| i.as_array()) {
            Some(i) => i,
            None => continue,
        };

        let methods = result.entry(type_name).or_default();
        for method_id in items {
            // IDs can be integers or strings in the JSON
            let method_id_str = if let Some(n) = method_id.as_u64() {
                n.to_string()
            } else {
                method_id.to_string().replace('"', "")
            };
            let method = match index.get(&method_id_str) {
                Some(m) => m,
                None => continue,
            };
            if method.get("visibility").and_then(|v| v.as_str()) != Some("public") {
                continue;
            }
            let name = match method.get("name").and_then(|n| n.as_str()) {
                Some(n) => n,
                None => continue,
            };
            if method
                .get("inner")
                .and_then(|i| i.as_object())
                .and_then(|i| i.get("function"))
                .is_some()
            {
                methods.insert(name.to_string());
            }
        }
    }

    result
}

#[test]
fn api_surface_parity() {
    let root = workspace_root();
    let local_json = root.join("target/doc/turso.json");
    let remote_json = root.join("target/doc/turso_serverless.json");

    if !local_json.exists() || !remote_json.exists() {
        eprintln!(
            "Skipping api_surface_parity: JSON docs not found.\n\
             Run: RUSTDOCFLAGS=\"-Z unstable-options --output-format json\" \
             cargo +nightly doc -p turso -p turso_serverless --no-deps"
        );
        return;
    }

    let local = parse_methods(&local_json);
    let remote = parse_methods(&remote_json);

    let local_only_set: BTreeSet<(&str, &str)> = LOCAL_ONLY.iter().copied().collect();
    let remote_only_set: BTreeSet<(&str, &str)> = REMOTE_ONLY.iter().copied().collect();

    let mut errors = Vec::new();

    for type_name in PAIRED_TYPES {
        let local_methods = local.get(*type_name).cloned().unwrap_or_default();
        let remote_methods = remote.get(*type_name).cloned().unwrap_or_default();

        // Methods in local but not remote
        for m in &local_methods {
            if !remote_methods.contains(m) && !local_only_set.contains(&(type_name, m.as_str())) {
                errors.push(format!("  {type_name}::{m} — in local but not remote"));
            }
        }

        // Methods in remote but not local
        for m in &remote_methods {
            if !local_methods.contains(m) && !remote_only_set.contains(&(type_name, m.as_str())) {
                errors.push(format!("  {type_name}::{m} — in remote but not local"));
            }
        }
    }

    if !errors.is_empty() {
        panic!(
            "API surface mismatch between turso and turso_serverless:\n{}",
            errors.join("\n")
        );
    }
}
