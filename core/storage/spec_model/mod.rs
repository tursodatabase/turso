//! Spec-model storage: an in-memory implementation of the cursor contract
//! (`tests/integration/cursor_contract/SPEC.md`). It is the spec in code, an oracle for SQL
//! results, and a checker that panics with `unsupported: U<n>: ...` on an Undefined sequence.
//!
//! It exists only in debug builds. A database opened at the path `:spec-model:` keeps all its
//! trees here (`SpecModelStorage`, through the `TreeStorage` seam of `storage::tree_store`).

mod cursor;
pub mod spec;
mod store;

pub use cursor::SpecModelCursor;
pub use store::SpecModelStorage;

/// Counters that show how much of the spec-model a run used. A process that opened a
/// spec-model tree prints them to stderr when it exits.
mod stats {
    use crate::sync::{LazyLock, Mutex};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TABLES: AtomicU64 = AtomicU64::new(0);
    static INDEXES: AtomicU64 = AtomicU64::new(0);
    static DUPS: AtomicU64 = AtomicU64::new(0);
    static CURSORS: AtomicU64 = AtomicU64::new(0);
    static STEPS: LazyLock<Mutex<BTreeMap<&'static str, u64>>> =
        LazyLock::new(|| Mutex::new(BTreeMap::new()));

    pub(super) fn tree_opened(index: bool) {
        report_at_exit();
        let counter = if index { &INDEXES } else { &TABLES };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn cursor_opened() {
        report_at_exit();
        CURSORS.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn dup_opened() {
        DUPS.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn step(name: &'static str) {
        *STEPS.lock().entry(name).or_insert(0) += 1;
    }

    #[cfg_attr(not(unix), allow(dead_code))]
    fn report() -> String {
        let steps: Vec<String> = STEPS
            .lock()
            .iter()
            .map(|(name, n)| format!("{name}={n}"))
            .collect();
        format!(
            "spec-model storage: {} ephemeral table trees, {} ephemeral index trees, {} cursors on \
             database trees, {} OpenDup cursors; steps: {}",
            TABLES.load(Ordering::Relaxed),
            INDEXES.load(Ordering::Relaxed),
            CURSORS.load(Ordering::Relaxed),
            DUPS.load(Ordering::Relaxed),
            steps.join(" "),
        )
    }

    #[cfg(unix)]
    fn report_at_exit() {
        extern "C" fn print_report() {
            eprintln!("{}", report());
        }
        static ONCE: std::sync::Once = std::sync::Once::new();
        // SAFETY: `print_report` is a plain function that lives as long as the process.
        ONCE.call_once(|| unsafe {
            libc::atexit(print_report);
        });
    }

    #[cfg(not(unix))]
    fn report_at_exit() {}
}
