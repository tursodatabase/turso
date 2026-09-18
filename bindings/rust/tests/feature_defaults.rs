//! Guard: allocator-installing features (they set a `#[global_allocator]`,
//! see `src/lib.rs`) must never be reachable from `default` — directly or
//! laundered through another feature. Allocator choice belongs to the binary.
//!
//! Reads the manifest via `cargo metadata --no-deps`: feature state is
//! irrelevant, so `--all-features` CI runs stay green.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::Command;

/// Features that install a `#[global_allocator]` in this crate.
/// Extend this list when adding another opt-in allocator.
const ALLOCATOR_FEATURES: &[&str] = &["mimalloc"];

fn package_features() -> BTreeMap<String, Vec<String>> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let out = Command::new(cargo)
        .arg("metadata")
        .arg("--format-version")
        .arg("1")
        .arg("--no-deps")
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
        .output()
        .expect("running `cargo metadata` must succeed");
    assert!(
        out.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let meta: serde_json::Value = serde_json::from_slice(&out.stdout).expect("valid metadata JSON");
    let this_pkg = env!("CARGO_PKG_NAME");
    for pkg in meta["packages"].as_array().expect("packages array") {
        if pkg["name"].as_str() == Some(this_pkg) {
            let Some(map) = pkg["features"].as_object() else {
                panic!("no features map for {this_pkg}");
            };
            return map
                .iter()
                .map(|(k, v)| {
                    let deps = v
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|x| x.as_str().map(str::to_owned))
                                .collect()
                        })
                        .unwrap_or_default();
                    (k.clone(), deps)
                })
                .collect();
        }
    }
    panic!("package {this_pkg} not found in cargo metadata");
}

/// Transitive closure of a feature through intra-package feature refs.
fn closure(features: &BTreeMap<String, Vec<String>>, start: &str) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut stack = vec![start.to_owned()];
    while let Some(f) = stack.pop() {
        if !seen.insert(f.clone()) {
            continue;
        }
        for d in features.get(&f).into_iter().flatten() {
            stack.push(d.clone());
        }
    }
    seen
}

#[test]
fn default_features_do_not_install_a_global_allocator() {
    let features = package_features();
    assert!(
        features.contains_key("default"),
        "the `turso` crate must keep an explicit default feature set"
    );
    let defaults = closure(&features, "default");
    let installed: Vec<&str> = ALLOCATOR_FEATURES
        .iter()
        .filter(|f| defaults.contains(**f))
        .copied()
        .collect();
    assert!(
        installed.is_empty(),
        "allocator feature(s) {installed:?} are reachable from `default` \
         (closure: {defaults:?}); they install a #[global_allocator] and \
         must stay opt-in so downstream binaries own the allocator slot"
    );
}
